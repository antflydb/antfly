// Copyright 2026 Antfly, Inc.
// SPDX-License-Identifier: Apache-2.0
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

//! Native text, dense, and sparse execution over one leased remote publication. Candidate payloads
//! hydrate in physical batches through the same delete-aware Parquet cursor.
const std = @import("std");
const local = @import("antfly_local_sources");
const server_api = @import("http_server.zig");
const corpus = @import("lake_index_native_text.zig");
const search = local.storage_db_query_search_exec;
const shape = local.storage_db_query_result_shape;
const types = local.storage_db_types;
const Context = local.serverless_query_lake_read_context.Context;
const Store = @import("lake_index_store.zig").Store;
const A = std.mem.Allocator;
const overlay_api = @import("lake_search_overlay.zig");
fn retainedCancellation(context: *const Context) @import("antfly_cancellation").CancellationToken {
    const Check = struct {
        fn check(raw: *const anyopaque) !void {
            const self: *const Context = @ptrCast(@alignCast(raw));
            try self.ensureActive();
        }
        fn canceled(raw: *const anyopaque) bool {
            check(raw) catch return true;
            return false;
        }
    };
    // Artifact checkpoints preserve the precise deadline error; cloud
    // transports also need the boolean adapter during an in-flight request.
    return .{ .ptr = context, .check_fn = Check.check, .is_cancelled_fn = Check.canceled };
}

pub fn execute(a: A, server: *server_api.ApiHttpServer, table: local.common_topology_records.TableRecord, req: types.SearchRequest, request: local.api_operation.RequestContext) !?local.api_query.QueryResponse {
    return executeWithDelivery(a, server, table, req, request, null);
}
pub fn executeWithDelivery(a: A, server: *server_api.ApiHttpServer, table: local.common_topology_records.TableRecord, req: types.SearchRequest, request: local.api_operation.RequestContext, delivery: ?local.api_query_response.Delivery) !?local.api_query.QueryResponse {
    return executePinned(a, server, table, req, request, delivery, false) catch |err| switch (err) {
        error.ExternalLakeIndexNotPublished, error.ExternalLakeIndexUnavailable => error.IndexRebuilding,
        error.LakeIndexArtifactUnavailable, error.InvalidLogicalRowIdentity, error.ExternalLakeIndexDefinitionChanged, error.ExternalLakeIndexStoreChanged, error.ExternalLakeIndexCredentialsChanged, error.ExternalLakeIndexSourceChanged, error.ExternalLakeSnapshotMismatch => changed: {
            try server.requestLakePublication(table.name);
            break :changed error.CatalogGenerationChanged;
        },
        error.LakeIndexReaderLeaseExpired, error.NativeLakeTextCacheBusy, error.NativeLakeRuntimeCacheBusy, error.LakeSnapshotReadLeaseExpired, error.LakeSnapshotRetired, error.LakeOverlayCoverageUnavailable => error.StorageReadTemporarilyUnavailable,
        error.NativeLakeTextCorpusTooLarge, error.LakeOverlayTooLarge, error.ArtifactReadBudgetExceeded => error.QueryCandidateBudgetExceeded,
        error.IndexNotFound => error.InvalidQueryRequest,
        else => err,
    };
}
pub fn reconcileRecent(a: A, server: *server_api.ApiHttpServer, table: local.common_topology_records.TableRecord, request: local.api_operation.RequestContext) !void {
    if (try executePinned(a, server, table, .{}, request, null, true)) |value| {
        var response = value;
        response.deinit(a);
    }
}
fn executePinned(a: A, server: *server_api.ApiHttpServer, current_table: local.common_topology_records.TableRecord, req: types.SearchRequest, request: local.api_operation.RequestContext, delivery: ?local.api_query_response.Delivery, build_recent: bool) !?local.api_query.QueryResponse {
    const retained_api = @import("lake_retained_cut.zig");
    // The cut lifetime begins at admission, so a slow query cannot extend a
    // recent segment or lake pin beyond the retention checked when binding it.
    const cut_expires_ms = @import("antfly_platform").time.realtimeNs() / std.time.ns_per_ms +| retained_api.configuredTtl(server.cfg.node_config);
    var retained_arena = std.heap.ArenaAllocator.init(a);
    defer retained_arena.deinit();
    const ra = retained_arena.allocator();
    var table = current_table;
    var retained: ?retained_api.Descriptor = null;
    var retained_pin: ?retained_api.Pinned = null;
    defer if (retained_pin) |pin| pin.deinit();
    const normalized = try request.platformDeadline();
    var retained_context: Context = .{ .io = server.embedding_provider_runtime.io, .deadline_ns = normalized.deadline_ns, .cancellation = local.storage_object_storage.CancellationToken.fromCallback(normalized.cancellation.ptr, normalized.cancellation.is_cancelled_fn) };
    const retained_cancellation = retainedCancellation(&retained_context);
    if (req.remote_snapshot) |token| if (std.mem.startsWith(u8, token, retained_api.prefix)) {
        var cut_store = try Store.openNative(a, server.cfg.node_config, server.cfg.secret_store, true, server.cfg.deployment_mode, server.cfg.native_lake_artifact_base_dir);
        defer cut_store.deinit();
        cut_store.implementation.cloud_io = server.embedding_provider_runtime.io;
        var artifacts = cut_store.artifactStore();
        try server.prepareLakeCache();
        retained_pin = try retained_api.loadWithGroups(ra, &artifacts, cut_store.identity, token, table, @import("antfly_platform").time.realtimeNs() / std.time.ns_per_ms, retained_cancellation, &server.lake_read_cache.decoded, &server.lake_retained_groups, .{ .io = server.embedding_provider_runtime.io, .deadline_ns = normalized.deadline_ns, .cancellation = local.storage_object_storage.CancellationToken.fromCallback(normalized.cancellation.ptr, normalized.cancellation.is_cancelled_fn) });
        retained = retained_pin.?.descriptor;
        table.lake_index_catalog_json = try local.metadata_lake_index_catalog.encode(ra, .{ .namespace = retained.?.publication.namespace, .generation = retained.?.publication.generation, .published = retained.?.publication });
    };
    var schema = (try local.serverless_external_source_schema_binding.externalBindingFromSchemaJsonAlloc(a, table.schema_json)) orelse return null;
    defer schema.deinit(a);
    const total_started = @import("antfly_platform").time.monotonicNs();
    defer server.lake_query_metrics.record(.total, total_started);
    // Graph and search aggregation execution require their own native ports;
    // reject them before selecting a publication rather than dropping clauses.
    if (req.graph_queries.len != 0 or req.graph_metric_queries.len != 0 or req.aggregations_json.len != 0) return error.UnsupportedQueryRequest;
    if (req.full_text != null and req.query != .match_all) return error.UnsupportedQueryRequest;
    var context: Context = .{ .io = server.embedding_provider_runtime.io, .deadline_ns = normalized.deadline_ns, .cancellation = local.storage_object_storage.CancellationToken.fromCallback(normalized.cancellation.ptr, normalized.cancellation.is_cancelled_fn) };
    const authority = server.source.lakeIndexLifecycleAuthority(request) orelse return error.ExternalLakeIndexUnavailable;
    var state = try local.metadata_lake_index_catalog.parse(a, table.lake_index_catalog_json);
    defer state.deinit();
    const publication = state.value.published orelse return error.ExternalLakeIndexNotPublished;
    const lease = try server.lake_reader_leases.acquireRetained(server.embedding_provider_runtime.io, authority, table.table_id, publication.generation, context, if (retained) |cut| cut.reader_token else null);
    defer lease.deinit();
    context = lease.readContext();
    try server.prepareLakeCache();
    const options: @import("../serverless/configured_object_store_support.zig").BindingObjectStoreOpenOptions = .{ .retained_catalog_metadata = if (retained) |cut| cut.catalog_metadata else null, .node_config = server.cfg.node_config, .secret_store = server.cfg.secret_store, .catalog_table_id = table.table_id, .catalog_generation = table.object_storage_generation };
    var store = try Store.openNative(a, server.cfg.node_config, server.cfg.secret_store, req.remote_snapshot != null, server.cfg.deployment_mode, server.cfg.native_lake_artifact_base_dir);
    defer store.deinit();
    store.implementation.cloud_io = server.embedding_provider_runtime.io;
    var wal_cut: ?@import("lake_wal_cut_cache.zig").Pin = null;
    defer if (wal_cut) |pin| pin.deinit();
    var overlay_arena = std.heap.ArenaAllocator.init(a);
    defer overlay_arena.deinit();
    const oa = overlay_arena.allocator();
    var overlay: ?overlay_api.Overlay = null;
    var source_schema = schema;
    var retained_metadata: ?local.serverless_external_source_mod.lake_catalog.types.Table = if (retained) |cut| cut.catalog_metadata else null;
    var retained_pending: @import("../serverless/lake_ingestion.zig").Pending = .{ .lsn = 0, .key_fields = &.{}, .changes = &.{} };
    const published_only = if (req.lake_read) |read| read.visibility == .published else false;
    if (req.lake_read) |read| {
        if (schema.binding.write_policy != .iceberg_writer) return error.UnsupportedQueryRequest;
        if (read.through) |receipt| {
            if (receipt.table_id != table.table_id or receipt.object_generation != table.object_storage_generation) return error.CatalogGenerationChanged;
            if (server.cfg.node_config == null) return error.UnsupportedQueryRequest;
        }
    }
    if (retained) |cut| {
        if (cut.published_only != published_only) return error.CatalogGenerationChanged;
        retained_pending = cut.pending;
        overlay = retained_pin.?.overlay;
    }
    if (published_only or retained != null) try pinPublishedSource(&source_schema.binding, publication.base_source);
    // The configured queue supports durable standalone filesystem fallback as
    // well as cloud connections. Accepted visibility must follow that same
    // authority rather than silently ignoring locally acknowledged WAL rows.
    if (retained == null and schema.binding.write_policy == .iceberg_writer and server.cfg.node_config != null) {
        const catalog = local.serverless_external_source_mod.lake_catalog;
        var current = try @import("../serverless/configured_object_store_support.zig").executeLakeCatalogAlloc(a, schema.binding, options, context, .load);
        defer current.deinit(a);
        const root = try catalog.metadata.parse(oa, current.table.metadata_json);
        retained_metadata = try std.json.parseFromSliceLeaky(catalog.types.Table, oa, try std.json.Stringify.valueAlloc(oa, current.table, .{}), .{ .allocate = .alloc_always });
        const base_id = switch (publication.base_source) {
            .external_iceberg => |base| base.snapshot_id,
            else => return error.ExternalLakeSnapshotMismatch,
        };
        const cut = try overlay_api.snapshotCoverage(root, base_id);
        if (published_only) {
            if (req.lake_read.?.through) |receipt| if (cut < receipt.wal_lsn) return error.IndexRebuilding;
        }
        const pending = if (!published_only) shared: {
            const ingestion = @import("../serverless/lake_ingestion.zig");
            var queue = try ingestion.openQueue(oa, schema.binding, options);
            defer queue.deinit();
            const queue_prefix = try ingestion.prefix(oa, queue.prefix, schema.binding, options);
            const authority_bytes = try std.json.Stringify.valueAlloc(oa, .{ store.identity, table.table_id, table.object_storage_generation, table.schema_json, queue_prefix, publication.generation, publication.signature, publication.namespace, publication.inventory.artifact_id, if (publication.directory) |directory| directory.artifact_id else "" }, .{});
            var identity: [32]u8 = undefined;
            std.crypto.hash.Blake3.hash(authority_bytes, &identity, .{});
            wal_cut = try server.lake_wal_cuts.acquire(identity, .{ .client = queue.client, .bucket = queue.bucket, .prefix = queue_prefix, .context = context }, cut, context);
            break :shared wal_cut.?.overlay().pending;
        } else @import("../serverless/lake_ingestion.zig").Pending{ .lsn = cut, .key_fields = &.{}, .changes = &.{} };
        retained_pending = pending;
        if (req.lake_read) |read| if (read.through) |receipt| if (pending.lsn < receipt.wal_lsn) return error.IndexRebuilding;
        if (pending.count() != 0) {
            try overlay_api.requireNativeAncestry(root, base_id);
            overlay = wal_cut.?.overlay();
            // The serving index defines the archive cut; later committed and
            // uncommitted WAL rows share the same pinned suffix.
            source_schema.binding.write_policy = .read_only;
            source_schema.binding.snapshot_mode = if (std.mem.startsWith(u8, base_id, "empty:")) .current else .{ .snapshot_id = base_id };
        }
    }
    if (retained != null) if (req.lake_read) |read| if (read.through) |receipt| if (retained_pending.lsn < receipt.wal_lsn) return error.IndexRebuilding;
    var source = blk: {
        const source_started = @import("antfly_platform").time.monotonicNs();
        defer server.lake_query_metrics.record(.source, source_started);
        break :blk try @import("lake_published_navigation.zig").open(a, server, table.table_id, table.lake_index_catalog_json, .{ .storage_mode = .relational, .external_base_source = source_schema }, options.lakeOptions(), context, request, lease);
    };
    defer source.deinit();
    try source.attachCache(&server.lake_read_cache, schema.binding, context);
    context = source.protectContext(context);
    var arena = std.heap.ArenaAllocator.init(a);
    defer arena.deinit();
    const ca = arena.allocator();
    var sql_table = try server.sql_schema_cache.resolve(server.embedding_provider_runtime.io, ca, table.schema_json, table.table_id, table.name);
    sql_table.external_indexes = .{ .object_generation = table.object_storage_generation, .schema_json = table.schema_json, .catalog_json = table.lake_index_catalog_json, .indexes_json = table.indexes_json, .desired = local.metadata_lake_index_catalog.desiredFingerprint(table) };
    server.attachLakeRepairHint(&source);
    var selected = blk: {
        const publication_started = @import("antfly_platform").time.monotonicNs();
        defer server.lake_query_metrics.record(.publication, publication_started);
        break :blk (try @import("lake_index_selection.zig").selectCachedWithLease(ca, sql_table, &source, &store, context, .required, &server.lake_read_cache, lease)) orelse return error.ExternalLakeIndexUnavailable;
    };
    defer selected.deinit();
    const publication_bytes = try local.metadata_lake_index_catalog.encode(ca, .{ .namespace = selected.publication().namespace, .generation = selected.publication().generation, .published = selected.publication() });
    var snapshot_hash = std.crypto.hash.Blake3.init(.{});
    snapshot_hash.update("native-lake-search-snapshot-v1");
    var table_id: [8]u8 = undefined;
    std.mem.writeInt(u64, &table_id, table.table_id, .little);
    snapshot_hash.update(&table_id);
    snapshot_hash.update(publication_bytes);
    var archive_hash = snapshot_hash;
    archive_hash.update(table.schema_json);
    var archive_digest: [32]u8 = undefined;
    archive_hash.final(&archive_digest);
    if (overlay) |value| {
        var lsn_bytes: [8]u8 = undefined;
        std.mem.writeInt(u64, &lsn_bytes, value.pending.lsn, .little);
        snapshot_hash.update("accepted-wal-overlay-v1");
        snapshot_hash.update(&lsn_bytes);
        snapshot_hash.update(&value.contentIdentity());
    }
    snapshot_hash.update(table.schema_json);
    var snapshot_digest: [32]u8 = undefined;
    snapshot_hash.final(&snapshot_digest);
    const snapshot_token = std.fmt.bytesToHex(snapshot_digest, .lower);
    if (req.remote_snapshot) |expected| {
        if (retained == null and !std.mem.eql(u8, expected, &snapshot_token)) return error.CatalogGenerationChanged;
    } else if (req.search_after.len != 0 or req.search_before.len != 0) return error.CatalogGenerationChanged;
    const navigation_api = @import("lake_source_navigation.zig");
    var navigation_pin: ?@import("lake_index_decoded_metadata.zig").Owned(navigation_api.Root) = null;
    defer if (navigation_pin) |pin| pin.release();
    var navigation: ?navigation_api.Resolver = null;
    defer if (navigation) |*resolver| resolver.deinit();
    var metadata: ?*@import("lake_index_search_metadata.zig").Entry = null;
    defer if (metadata) |entry| entry.release();
    var owner: Execution = .{ .server = server, .table = sql_table, .source = &source, .store = &store, .domain = @import("lake_index_publication.zig").uploadDomainWithNamespace(table.table_id, store.identity, selected.publication().namespace), .declarations = selected.publication().declarations, .context = context, .request = normalized, .schema_json = table.schema_json, .arena = ca, .result_allocator = a, .overlay = if (overlay != null) &overlay.? else null, .wal_cut = if (wal_cut != null) &wal_cut.? else null };
    defer owner.deinit();
    var proof: @import("lake_index_row_source.zig").Provider = .{ .source = &source, .context = context, .expected_delete_objects = selected.delete_objects };
    if (selected.publication().source_navigation) |ref| {
        const cached: @import("lake_index_aggregate_artifact.zig").CachedRead = .{ .cache = &server.lake_read_cache, .scope = store.identity, .context = context };
        navigation_pin = try @import("lake_index_decoded_metadata.zig").acquire(navigation_api.Root, cached, store.artifactStore(), ref, retained_cancellation, navigation_api.load);
        const root = navigation_pin.?.value.*;
        if (!std.mem.eql(u8, &root.domain, &owner.domain) or !std.mem.eql(u8, root.source, source.inventory.source_id) or
            !std.mem.eql(u8, root.schema, source.inventory.schema_fingerprint) or !std.mem.eql(u8, root.snapshot, source.inventory.snapshot_id) or root.count != source.fileCount()) return error.ExternalLakeSnapshotMismatch;
        navigation = .{ .root = root, .store = store.artifactStore(), .cached = cached, .token = retained_cancellation };
        owner.navigation = &navigation.?;
    } else {
        metadata = try server.lake_search_metadata.acquire(archive_digest, &proof);
        owner.files = metadata.?.files;
        owner.private_files = metadata.?.private_files;
        owner.private_digests = metadata.?.private_digests;
    }
    const publication_finished = @import("antfly_platform").time.monotonicNs();
    var pattern_programs = local.search_query.PatternPrograms.init(@import("antfly_platform").allocator.processAllocator(std.heap.page_allocator), 2 * 1024 * 1024);
    defer pattern_programs.deinit();
    var effective = req;
    effective.text_pattern_programs = &pattern_programs;
    effective.cancellation = .{ .ptr = &owner, .is_cancelled_fn = Execution.canceled };
    const has_vectors = build_recent or effective.dense != null or effective.sparse != null or effective.dense_queries.len != 0 or effective.sparse_queries.len != 0;
    const has_text = for (owner.declarations) |declaration| {
        if (declaration.artifact.kind == .text_segment) break true;
    } else false;
    owner.cloud_generations = selected.publication().cloud_generations;
    if (has_vectors) if (owner.overlay) |pending_overlay| {
        const prepared: @import("lake_recent_vectors.zig").Prepared = if (retained) |cut| .{ .declarations = cut.recent, .cloud_generations = cut.recent_cloud, .runs = cut.recent_runs } else @import("lake_recent_vectors.zig").prepare(ca, current_table, selected.publication(), pending_overlay, owner.declarations, &store, context, .{ .antfly_provider = server.antfly_provider, .io = server.embedding_provider_runtime.io, .bounded_http_request = true, .deadline_ns = normalized.deadline_ns, .cancellation = retained_cancellation, .secret_store = server.cfg.secret_store, .remote_content = server.cfg.remote_content, .inference_api_url = server.configuredInferenceAPIURL(), .inference_api_key = server.cfg.inference_api_key, .provider_runtime = &server.embedding_provider_runtime, .source_table = table.name }, build_recent, cut_expires_ms, &server.lake_recent_vector_jobs) catch |err| {
            server.notifyLakeCommit(table.name) catch {};
            return err;
        };
        owner.recent_declarations = prepared.declarations;
        owner.recent_runs = prepared.runs;
        owner.recent_cloud = prepared.cloud_generations;
        // All declared vector recipes publish as one coherent recent cut.
        var expected: usize = 0;
        for (owner.declarations) |declaration| if (declaration.artifact.kind == .vector_segment or declaration.artifact.kind == .sparse_segment) {
            expected += 1;
        };
        for (owner.recent_runs) |run| if (run.declarations.len != expected) return error.IndexRebuilding;
        if (owner.recent_declarations.len != expected) return error.IndexRebuilding;
    };
    if (build_recent) return null;
    if (has_vectors) {
        const resolver: @import("lake_index_text_predicate.zig").PhysicalResolver = .{ .server = server, .table = sql_table, .source = &source, .context = normalized, .store = store.artifactStore(), .store_identity = store.identity, .read_context = context, .pinned = .{ .artifacts = store.artifactStore(), .store_identity = store.identity, .domain = owner.domain, .declarations = owner.declarations, .read_context = context } };
        owner.vector_filter = if (effective.filter_query_json.len != 0) try std.json.parseFromSliceLeaky(std.json.Value, ca, effective.filter_query_json, .{}) else null;
        owner.vector_exclusion = if (effective.exclusion_query_json.len != 0) try std.json.parseFromSliceLeaky(std.json.Value, ca, effective.exclusion_query_json, .{}) else null;
        if (owner.overlay) |pending_overlay| {
            _ = pending_overlay;
            try owner.resolveOverlayPhysical();
        }
        if (effective.filter_query_json.len != 0) {
            owner.vector_include_provider = try resolver.openMembership(a, effective.filter_query_json);
        }
        if (effective.exclusion_query_json.len != 0) {
            owner.vector_exclude_provider = try resolver.openMembership(a, effective.exclusion_query_json);
        }
    } else if (!has_text) try @import("lake_index_search_filter.zig").resolve(ca, sql_table, &source, request, &effective);
    owner.hydration_fields = try owner.planHydration(retrievalRequest(effective));
    owner.typed_delivery = canDeliverTypedSource(retrievalRequest(effective));
    var execution_req = effective;
    if (owner.typed_delivery and local.storage_db_query_control_contract.requestHasPostprocessPageTransforms(effective)) {
        // Candidate admission is independent of whether packing fits one native
        // window. The coordinator owns transforms and final output pagination.
        const paging = local.storage_db_query_control_contract.componentPaging(effective);
        execution_req = retrievalRequest(effective);
        execution_req.offset = paging.offset;
        execution_req.limit = paging.limit;
    }
    if (effective.hasHitEvaluation()) {
        execution_req.offset = 0;
        execution_req.limit = effective.evaluation_limit;
    }
    // Retrieval/ranking for these requests needs identities and scores only.
    // Hydrate the final page once, after all result movement, into retained column pages.
    if (owner.typed_delivery) execution_req.include_stored = false;
    const started = @import("antfly_platform").time.monotonicNs();
    var result = blk: {
        defer server.lake_query_metrics.record(.ranking, started);
        break :blk if (execution_req.full_text_queries.len != 0 or execution_req.sparse_queries.len != 0 or execution_req.dense_queries.len != 0)
            try search.searchComposed(a, execution_req, .{ .ctx = &owner, .search_components = Execution.searchComponents, .search_text_query = Execution.searchText, .search_text = Execution.dispatchText, .search_dense = Execution.searchDense, .search_sparse = Execution.searchSparse, .clone_named_set = Execution.cloneSet, .fuse_named_sets = Execution.fuseSets, .attach_graph_results = Execution.attachGraph })
        else if (execution_req.dense) |dense| try Execution.searchDense(&owner, a, execution_req, dense) else if (execution_req.sparse) |sparse| try Execution.searchSparse(&owner, a, execution_req, sparse) else if (execution_req.full_text) |text| try Execution.searchText(&owner, a, execution_req, text) else try Execution.dispatchText(&owner, a, execution_req);
    };
    defer result.deinit();
    const search_finished = @import("antfly_platform").time.monotonicNs();
    if (!owner.typed_delivery) try owner.attachHighlights(a, effective, &result);
    try context.ensureActive();
    // Retain only immutable serving metadata and recent row images. The archive
    // files/indexes remain shared, protected by their durable reader protocols.
    const serving_token: []const u8 = cursor: {
        const cursor_started = @import("antfly_platform").time.monotonicNs();
        defer server.lake_query_metrics.record(.cursor_retention, cursor_started);
        break :cursor if (retained != null) req.remote_snapshot.? else if (req.remote_snapshot != null) &snapshot_token else cut: {
            var artifacts = store.artifactStore();
            if (cut_expires_ms <= @import("antfly_platform").time.realtimeNs() / std.time.ns_per_ms) return error.DeadlineExceeded;
            break :cut try retained_api.saveCached(ra, &artifacts, store.identity, server.embedding_provider_runtime.io, .{ .expires_ms = cut_expires_ms, .table_id = table.table_id, .object_generation = table.object_storage_generation, .desired = local.metadata_lake_index_catalog.desiredFingerprint(current_table), .publication = publication, .reader_token = lease.retainedToken(), .pending = retained_pending, .catalog_metadata = retained_metadata, .published_only = published_only, .recent = owner.recent_declarations, .recent_cloud = owner.recent_cloud, .recent_runs = owner.recent_runs }, retained_cancellation, &server.lake_retained_payloads);
        };
    };
    var meta: local.api_query.QueryResponseMeta = .{ .remote_snapshot = serving_token, .shard_count = 1, .took_ms = @intCast((@import("antfly_platform").time.monotonicNs() -| total_started) / std.time.ns_per_ms) };
    defer meta.deinit(a);
    // Source-dependent transforms consume the globally selected candidate pool,
    // never every admitted segment's local pool. Preserve hidden input fields.
    const candidate_sources_loaded = owner.typed_delivery and needsCandidateSource(effective);
    const transform_plan = if (candidate_sources_loaded) try owner.planTransformInputs(effective) else null;
    if (transform_plan) |inputs| try owner.hydrateTransformCandidates(a, effective, inputs.fields, result.hits);
    try @import("query_post_processing.zig").applyQueryPostProcessing(a, effective, &result, &meta, .{ .source_table = table.name, .backend_runtime = server.cfg.backend_runtime, .secret_store = server.cfg.secret_store, .remote_content = server.cfg.remote_content });
    // Reused source is deliberately complete, including fields hidden from
    // the public page. The encoder must apply the final projection even when
    // an empty projection did not originally require deferred storage shaping.
    if (candidate_sources_loaded) effective.defer_stored_projection = true;
    var prepared_delivery = delivery;
    const lazy_hydration = owner.typed_delivery and !candidate_sources_loaded and !effective.count_only and (effective.include_stored or effective.highlight != null) and
        (if (delivery) |sink| sink.consume_columns and sink.spill_io != null else false);
    if (lazy_hydration) {
        owner.delivery_request = effective;
        prepared_delivery.?.hydrator = .{ .ptr = &owner, .load = Execution.hydrateDelivery, .release = Execution.releaseDelivery };
    } else if (owner.typed_delivery) {
        // Transforms retain full source on surviving hits. Reuse it for wire
        // projection/highlights rather than fetching the final page again.
        if ((!candidate_sources_loaded or !transform_plan.?.covers_output) and !effective.count_only and (effective.include_stored or effective.highlight != null)) try owner.hydrateTyped(a, effective, result.hits);
        try owner.attachHighlights(a, effective, &result);
    }
    if (candidate_sources_loaded and !effective.include_stored) for (result.hits) |*hit| {
        if (hit.stored_data) |bytes| a.free(bytes);
        hit.stored_data = null;
    };
    meta.took_ms = @intCast((@import("antfly_platform").time.monotonicNs() -| total_started) / std.time.ns_per_ms);
    const response = delivery_result: {
        const delivery_started = @import("antfly_platform").time.monotonicNs();
        defer server.lake_query_metrics.record(.result_delivery, delivery_started);
        break :delivery_result try local.api_query.encodeQueryResponsesWithDelivery(a, table.name, effective, meta, result, prepared_delivery);
    };
    const finished = @import("antfly_platform").time.monotonicNs();
    server.lake_read_cache.recordQuery(.{
        .total_ns = finished -| total_started,
        .publication_ns = publication_finished -| total_started,
        .search_ns = search_finished -| publication_finished,
        .hydration_ns = owner.hydration_ns,
        .delivery_ns = finished -| search_finished,
    });
    return response;
}
const Execution = struct {
    scheduled_component: bool = false,
    server: *server_api.ApiHttpServer,
    overlay: ?*overlay_api.Overlay = null,
    wal_cut: ?*const @import("lake_wal_cut_cache.zig").Pin = null,
    prefetched_archive: ?*const search.PinnedTextSource = null,
    forced_pin: ?*search.PinnedTextSource = null,
    candidate_rows: ?[]const local.storage_rowsource_types.RowRef = null,
    table: local.sql_catalog.Table,
    source: *local.serverless_query_lake_serving.ServingSource,
    store: *Store,
    domain: [32]u8,
    declarations: []const local.serverless_segment_sidecar_manifest.DeclaredArtifact,
    context: Context,
    request: local.api_operation.RequestContext,
    schema_json: []const u8,
    hydration_fields: ?[]const []const u8 = null,
    vector_filter: ?std.json.Value = null,
    vector_exclusion: ?std.json.Value = null,
    active_recent: bool = false,
    cloud_generations: ?local.metadata_lake_index_catalog.CloudGenerations = null,
    recent_cloud: ?local.metadata_lake_index_catalog.CloudGenerations = null,
    recent_runs: []const @import("lake_recent_vectors.zig").Run = &.{},
    recent_run: ?@import("lake_recent_vectors.zig").Run = null,
    unit_runtime: ?*@import("lake_index_native_runtime_cache.zig").Entry = null,
    recent_declarations: []const local.serverless_segment_sidecar_manifest.DeclaredArtifact = &.{},
    recent_dense_entries: std.StringHashMapUnmanaged(*local.storage_db_catalog_index_manager.IndexManager.DenseIndex) = .empty,
    recent_sparse_entries: std.StringHashMapUnmanaged(*local.storage_db_catalog_index_manager.IndexManager.SparseIndex) = .empty,
    vector_include_provider: ?@import("lake_index_text_predicate.zig").PhysicalResolver.Membership = null,
    vector_exclude_provider: ?@import("lake_index_text_predicate.zig").PhysicalResolver.Membership = null,
    vector_include: ?@import("lake_index_physical_set.zig").Set = null,
    vector_exclude: ?@import("lake_index_physical_set.zig").Set = null,
    typed_delivery: bool = false,
    ordinal_live_rows: @import("lake_index_text_predicate.zig").LiveRowsCache = .{},
    predicate_exclusion_json: []const u8 = "",
    predicate_allow_partial: bool = true,
    delivery_request: ?types.SearchRequest = null,
    hydration_ns: u64 = 0,
    use_stored_highlights: bool = false,
    highlight_index: ?[]const u8 = null,
    highlight_locations: std.StringHashMapUnmanaged(StoredHighlightLocation) = .empty,
    text_stores_source: std.StringHashMapUnmanaged(bool) = .empty,
    highlight_pins: std.ArrayList(search.PinnedTextSource) = .empty,
    delivery_pin: ?search.PinnedTextSource = null,
    highlight_queries: ?[]const search.HighlightQuery = null,
    arena: A,
    result_allocator: A = std.heap.page_allocator,
    navigation: ?*@import("lake_source_navigation.zig").Resolver = null,
    files: std.StringHashMapUnmanaged([]const u8) = .empty,
    private_files: std.StringHashMapUnmanaged([]const u8) = .empty,
    private_digests: std.StringHashMapUnmanaged([]const u8) = .empty,
    active_text: ?types.TextQuery = null,
    index_resolver: ?local.serverless_query_lake_serving.ServingSource.RowResolver = null,
    forced_selection: ?[]const [32]u8 = null,
    overlay_preparation: ?*@import("lake_index_text_frontier.zig") = null,
    ordered_membership: ?*OrderedMembership = null,
    recent_text_source: ?*const search.PinnedTextSource = null,
    recent_text_window: bool = false,
    text_identities: std.AutoHashMapUnmanaged(usize, @import("lake_index_text_predicate.zig").Identities) = .empty,
    sparse_entries: std.StringHashMapUnmanaged(*local.storage_db_catalog_index_manager.IndexManager.SparseIndex) = .empty,
    dense_entries: std.StringHashMapUnmanaged(*local.storage_db_catalog_index_manager.IndexManager.DenseIndex) = .empty,
    relocated_ties: std.ArrayList(*RelocatedTie) = .empty,
    runtimes: std.ArrayList(*@import("lake_index_native_runtime_cache.zig").Entry) = .empty,
    fn bindIndex(self: *Execution, name: []const u8) !void {
        self.index_resolver = try self.source.resolverForIndex(name);
    }
    fn relocated(self: *const Execution) bool {
        return if (self.index_resolver) |resolver| resolver.relocated else false;
    }
    fn fromIndex(self: *Execution, a: A, ref: local.storage_rowsource_types.RowRef) !local.storage_rowsource_types.RowRef {
        if (self.index_resolver) |resolver| return resolver.from_index(resolver.ptr, a, ref);
        return ref;
    }
    fn toIndex(self: *Execution, a: A, ref: local.storage_rowsource_types.RowRef) !local.storage_rowsource_types.RowRef {
        if (self.index_resolver) |resolver| return resolver.to_index(resolver.ptr, a, ref);
        return ref;
    }
    fn fileFor(self: *Execution, hex: []const u8, public: bool) !?[]const u8 {
        return self.fileForAlloc(self.arena, hex, public);
    }
    fn fileForAlloc(self: *Execution, a: A, hex: []const u8, public: bool) !?[]const u8 {
        if (!public) if (self.index_resolver) |resolver| return resolver.file(resolver.ptr, a, hex);
        if (self.navigation) |resolver| return resolver.file(a, hex, public);
        return if (public) self.files.get(hex) else self.private_files.get(hex);
    }
    fn privateDigest(self: *Execution, file: []const u8) ![]const u8 {
        if (self.index_resolver) |resolver| return resolver.digest(resolver.ptr, self.arena, file);
        if (self.navigation) |resolver| return resolver.digest(self.arena, file);
        return self.private_digests.get(file) orelse error.ExternalLakeSnapshotMismatch;
    }
    fn vectorRequest(self: *Execution, req: types.SearchRequest) types.SearchRequest {
        var result = req;
        if (self.overlay != null or self.vector_include != null or self.vector_exclude != null or self.vector_include_provider != null or self.vector_exclude_provider != null) {
            result.native_key_predicate = .{ .ptr = self, .allows = allowsVectorKey, .allows_many = allowsVectorKeys, .select_dense = selectDenseConstraints, .select_constraints = selectSparseConstraints, .constraint_revision = sparseConstraintRevision, .select_range = selectSparseRange };
            result.filter_query_json = "";
            result.exclusion_query_json = "";
        }
        return result;
    }
    fn selectDenseConstraints(raw: *anyopaque, a: A) !?types.DenseIdSelection {
        const self: *Execution = @ptrCast(@alignCast(raw));
        if (self.overlay != null or self.active_recent) return null;
        const runtime = self.unit_runtime orelse return null;
        const entry = runtime.dense_entry orelse return null;
        var workspace: local.sql_memory_budget = .{ .backing = a, .limit = 4 * 1024 * 1024 };
        var scratch = std.heap.ArenaAllocator.init(workspace.allocator());
        defer scratch.deinit();
        var allowance: local.serverless_query_lake_serving.ServingSource.TranslationAllowance = .{};
        return self.denseConstraintsInner(a, scratch.allocator(), entry, &allowance) catch |err| {
            if (err == error.QueryCandidateBudgetExceeded or (err == error.OutOfMemory and workspace.isLocallyExhausted())) return null;
            return err;
        };
    }
    fn denseConstraintsInner(self: *Execution, out: A, a: A, entry: *local.storage_db_catalog_index_manager.IndexManager.DenseIndex, allowance: *local.serverless_query_lake_serving.ServingSource.TranslationAllowance) !?types.DenseIdSelection {
        const need_include = self.vector_include_provider != null or self.includeSet() != null;
        const need_exclude = self.vector_exclude_provider != null or self.excludeSet() != null;
        const include = if (need_include) try self.denseIds(a, entry, if (self.vector_include_provider) |*provider| provider else null, self.includeSet(), allowance) else null;
        const exclude = if (need_exclude) try self.denseIds(a, entry, if (self.vector_exclude_provider) |*provider| provider else null, self.excludeSet(), allowance) else null;
        if (include == null and exclude == null) return null;
        const retained = if (include) |ids| try out.dupe(u64, ids) else null;
        errdefer if (retained) |ids| out.free(ids);
        return .{ .include = retained, .exclude = if (exclude) |ids| try out.dupe(u64, ids) else &.{}, .residual = (need_include and include == null) or (need_exclude and exclude == null) };
    }
    fn denseIds(self: *Execution, a: A, entry: *local.storage_db_catalog_index_manager.IndexManager.DenseIndex, provider: ?*@import("lake_index_text_predicate.zig").PhysicalResolver.Membership, selected: ?*const @import("lake_index_physical_set.zig").Set, allowance: *local.serverless_query_lake_serving.ServingSource.TranslationAllowance) !?[]const u64 {
        var converted = if (self.relocated()) try self.relocatedSelection(a, provider, selected, allowance) else null;
        defer if (converted) |*set| set.deinit();
        const selection = if (self.relocated()) (if (converted) |*set| set else return null) else selected orelse return null;
        const count = selection.boundedCardinality(4096) orelse return null;
        const Candidate = struct { id: u64, key: []const u8 };
        var candidates: std.ArrayList(Candidate) = .empty;
        try candidates.ensureTotalCapacity(a, count);
        var files = selection.files.iterator();
        while (files.next()) |file| {
            try self.context.ensureActive();
            const digest = if (self.index_resolver) |resolver| try resolver.digest(resolver.ptr, a, file.key_ptr.*) else if (self.navigation) |resolver| try resolver.digest(a, file.key_ptr.*) else self.private_digests.get(file.key_ptr.*) orelse return error.ExternalLakeSnapshotMismatch;
            var blocks = file.value_ptr.iterator();
            while (blocks.next()) |block| {
                var rows = block.value_ptr.iterator();
                while (rows.next()) |low| {
                    const row = (@as(u64, block.key_ptr.high) << 32) | low;
                    var buffer: [96]u8 = undefined;
                    const key = try std.fmt.bufPrint(&buffer, "lake2:{s}:{x:0>8}:{x:0>16}", .{ digest, block.key_ptr.group, row });
                    const id = @import("lake_index_native_dense.zig").vectorId(key);
                    candidates.appendAssumeCapacity(.{ .id = id, .key = try a.dupe(u8, key) });
                }
            }
        }
        std.mem.sort(Candidate, candidates.items, {}, struct {
            fn less(_: void, left: Candidate, right: Candidate) bool {
                return left.id < right.id;
            }
        }.less);
        const ids = try a.alloc(u64, candidates.items.len);
        const metadata = try a.alloc(?[]const u8, ids.len);
        for (candidates.items, ids) |candidate, *id| id.* = candidate.id;
        // One pinned, sorted lookup shares native navigation and returns
        // borrowed values. Exact identity verification still detects collisions.
        var read = try entry.index.beginRuntimeSearchTxn();
        defer read.abort();
        try entry.index.getMetadataManySortedInTxnUncached(&read, ids, metadata);
        var written: usize = 0;
        for (candidates.items, metadata) |candidate, value| if (value) |key| {
            if (!std.mem.eql(u8, key, candidate.key)) return error.NativeLakeVectorIdentityCollision;
            if (written == 0 or ids[written - 1] != candidate.id) {
                ids[written] = candidate.id;
                written += 1;
            }
        };
        return ids[0..written];
    }
    fn includeSet(self: *Execution) ?*const @import("lake_index_physical_set.zig").Set {
        if (self.vector_include) |*set| return set;
        if (self.vector_include_provider) |*provider| if (provider.complete) |*set| return set;
        return null;
    }
    fn excludeSet(self: *Execution) ?*const @import("lake_index_physical_set.zig").Set {
        if (self.vector_exclude) |*set| return set;
        if (self.vector_exclude_provider) |*provider| if (provider.complete) |*set| return set;
        return null;
    }
    fn sparseConstraintRevision(raw: *anyopaque) u64 {
        const self: *Execution = @ptrCast(@alignCast(raw));
        return @as(u64, @intFromBool(self.includeSet() != null)) | (@as(u64, @intFromBool(self.excludeSet() != null)) << 1);
    }
    fn selectSparseConstraints(raw: *anyopaque, a: A, lookup: types.SparseOrdinalLookup) !?types.SparseOrdinalSelection {
        const self: *Execution = @ptrCast(@alignCast(raw));
        if (self.overlay != null or self.active_recent) return null;
        if (self.relocated()) return try self.selectRelocatedConstraints(a, lookup);
        var result: types.SparseOrdinalSelection = .{};
        errdefer result.deinit();
        if (self.vector_include_provider) |provider| if (provider.complete == null) {
            result.residual = true;
            return result;
        };
        if (self.vector_exclude_provider) |provider| {
            if (provider.complete == null) result.residual = true;
        }
        // Translate compressed blocks under a live-byte and directory-work
        // budget, independently of row cardinality. The pinned sparse directory
        // proves physical-to-native identity; older generations get bounded
        // point seeks. No partial mask is ever admitted.
        result = try types.SparseOrdinalSelection.initBounded(a, 4 * 1024 * 1024);
        if (self.vector_exclude_provider) |provider| result.residual = provider.complete == null;
        self.planSparseSets(&result, lookup) catch |err| {
            if (err == error.OrdinalPlanningBudgetExceeded or (err == error.OutOfMemory and result.budget.?.exhausted)) {
                result.deinit();
                return .{ .residual = true, .deferred = lookup.native_range != null };
            }
            return err;
        };
        return result;
    }
    fn relocatedSelection(self: *Execution, a: A, provider: ?*@import("lake_index_text_predicate.zig").PhysicalResolver.Membership, selected: ?*const @import("lake_index_physical_set.zig").Set, allowance: *local.serverless_query_lake_serving.ServingSource.TranslationAllowance) !?@import("lake_index_physical_set.zig").Set {
        if (provider) |membership| if (try membership.indexSpaceSelection(a, self.index_resolver)) |set| return set;
        const current = selected orelse return null;
        return try @import("lake_index_text_predicate.zig").indexSelection(a, self.source, self.index_resolver, current, allowance);
    }
    fn selectRelocatedConstraints(self: *Execution, a: A, lookup: types.SparseOrdinalLookup) !types.SparseOrdinalSelection {
        var result = try types.SparseOrdinalSelection.initBounded(a, 4 * 1024 * 1024);
        var keep = false;
        defer if (!keep) result.deinit();
        var allowance: local.serverless_query_lake_serving.ServingSource.TranslationAllowance = .{};
        var included: ?@import("lake_index_physical_set.zig").Set = null;
        defer if (included) |*set| set.deinit();
        var excluded: ?@import("lake_index_physical_set.zig").Set = null;
        defer if (excluded) |*set| set.deinit();
        const wa = result.allocator();
        included = self.relocatedSelection(wa, if (self.vector_include_provider) |*provider| provider else null, self.includeSet(), &allowance) catch |err| {
            if (err == error.QueryCandidateBudgetExceeded or (err == error.OutOfMemory and result.budget.?.exhausted)) return .{ .residual = true, .deferred = lookup.native_range != null };
            return err;
        };
        if ((self.vector_include_provider != null or self.includeSet() != null) and included == null) return .{ .residual = true, .deferred = lookup.native_range != null };
        excluded = self.relocatedSelection(wa, if (self.vector_exclude_provider) |*provider| provider else null, self.excludeSet(), &allowance) catch |err| {
            if (err == error.QueryCandidateBudgetExceeded or (err == error.OutOfMemory and result.budget.?.exhausted)) return .{ .residual = true, .deferred = lookup.native_range != null };
            return err;
        };
        const residual = self.vector_exclude_provider != null and excluded == null;
        var direct = self.*;
        direct.vector_include = included;
        direct.vector_exclude = excluded;
        direct.vector_include_provider = null;
        direct.vector_exclude_provider = null;
        direct.planSparseSets(&result, lookup) catch |err| {
            if (err == error.OrdinalPlanningBudgetExceeded or (err == error.OutOfMemory and result.budget.?.exhausted)) return .{ .residual = true, .deferred = lookup.native_range != null };
            return err;
        };
        result.residual = residual;
        keep = true;
        return result;
    }
    fn planSparseSets(self: *Execution, result: *types.SparseOrdinalSelection, lookup: types.SparseOrdinalLookup) !void {
        // A bounded amount of eager translation preserves cheap selective seeks;
        // larger complete memberships translate only reached native windows.
        var work: types.SparseOrdinalWorkBudget = .{ .blocks = if (lookup.native_range != null) 64 else 4096 };
        var points_only = false;
        if (lookup.native_range != null and lookup.bounded_block != null) {
            const selected = self.includeSet() orelse self.excludeSet();
            if (selected) |set| switch (try self.sparseDirectoryPlan(result.allocator(), set, if (self.includeSet() != null) self.excludeSet() else null, work.blocks, work.points)) {
                .directory => {},
                .points => points_only = true,
                .deferred => return error.OrdinalPlanningBudgetExceeded,
            };
        }
        if (self.includeSet()) |include| {
            result.include = try self.selectSparseSet(result.allocator(), lookup, include, self.excludeSet(), &work, points_only);
        } else if (self.excludeSet()) |exclude| {
            result.exclude = try self.selectSparseSet(result.allocator(), lookup, exclude, null, &work, points_only);
        }
        try result.prepareRead();
    }
    /// Count physical directory windows before opening any native cursor. This
    /// uses the same effective include-minus-exclude selection as translation,
    /// and stops at the first unaffordable window, without expanding row IDs.
    const SparsePlan = enum { directory, points, deferred };
    fn sparseDirectoryPlan(self: *Execution, a: A, selected: *const @import("lake_index_physical_set.zig").Set, subtract: ?*const @import("lake_index_physical_set.zig").Set, limit: usize, point_limit: usize) !SparsePlan {
        var scratch = std.heap.ArenaAllocator.init(a);
        defer scratch.deinit();
        var windows: usize = 0;
        var points: usize = 0;
        var files = selected.files.iterator();
        while (files.next()) |file| {
            var blocks = file.value_ptr.iterator();
            while (blocks.next()) |block| {
                try self.context.ensureActive();
                _ = scratch.reset(.retain_capacity);
                var rows = try block.value_ptr.clone(scratch.allocator());
                defer rows.deinit();
                if (subtract) |exclude| if (exclude.files.getPtr(file.key_ptr.*)) |excluded_blocks| if (excluded_blocks.getPtr(block.key_ptr.*)) |excluded| rows.andNotWith(excluded);
                points = @min(point_limit + 1, points +| rows.cardinality());
                var iterator = rows.iterator();
                while (windows <= limit) {
                    const first = iterator.next() orelse break;
                    windows += 1;
                    const base = first & ~@as(u32, 1023);
                    if (base == std.math.maxInt(u32) - 1023) break;
                    iterator.seek(base + 1024);
                }
                if (windows > limit and points > point_limit) return .deferred;
            }
        }
        return if (windows > limit) .points else .directory;
    }
    fn selectSparseRange(raw: *anyopaque, a: A, lookup: types.SparseOrdinalLookup, first: u32, last: u32) !?types.SparseOrdinalSelection {
        const self: *Execution = @ptrCast(@alignCast(raw));
        if (self.overlay != null or self.active_recent) return null;
        if (!self.relocated()) {
            if (self.vector_include_provider) |provider| if (provider.complete == null) return null;
            if (self.vector_exclude_provider) |provider| if (provider.complete == null) return null;
        }
        if (lookup.native_range == null) return null;
        var result: types.SparseOrdinalSelection = .{ .include = .init(a) };
        errdefer result.deinit();
        if (!try lookup.nativeRange(a, first, last, .{ .ptr = self, .allows = allowsVectorKey }, &result.include.?)) {
            result.deinit();
            return null;
        }
        return result;
    }
    fn selectSparseSet(self: *Execution, a: A, lookup: types.SparseOrdinalLookup, selection_set: *const @import("lake_index_physical_set.zig").Set, subtract: ?*const @import("lake_index_physical_set.zig").Set, work: *types.SparseOrdinalWorkBudget, points_only: bool) !local.encoding_roaring.RoaringBitmap {
        var result = local.encoding_roaring.RoaringBitmap.init(a);
        errdefer result.deinit();
        var scratch = std.heap.ArenaAllocator.init(a);
        defer scratch.deinit();
        var files = selection_set.files.iterator();
        while (files.next()) |file| {
            const digest = try self.privateDigest(file.key_ptr.*);
            var blocks = file.value_ptr.iterator();
            while (blocks.next()) |block| {
                try self.context.ensureActive();
                var prefix: [80]u8 = undefined;
                const bytes_prefix = try std.fmt.bufPrint(&prefix, "lake2:{s}:{x:0>8}:", .{ digest, block.key_ptr.group });
                _ = scratch.reset(.retain_capacity);
                var selection = try block.value_ptr.clone(scratch.allocator());
                defer selection.deinit();
                if (subtract) |exclude| if (exclude.files.getPtr(file.key_ptr.*)) |excluded_blocks| if (excluded_blocks.getPtr(block.key_ptr.*)) |excluded| selection.andNotWith(excluded);
                if (selection.isEmpty()) continue;
                if (!points_only) if (lookup.bounded_block != null) {
                    if (try lookup.boundedBlock(a, bytes_prefix, block.key_ptr.high, &selection, &result, work)) continue;
                } else {
                    // A legacy block callback is safe only for a small mask.
                    // Broad generations require the work-accounted directory.
                    if (selection.cardinality() <= work.points) {
                        try work.takeBlock();
                        if (try lookup.lookupBlock(a, bytes_prefix, block.key_ptr.high, &selection, &result)) {
                            work.points -= selection.cardinality();
                            continue;
                        }
                    }
                };
                if (selection.cardinality() > work.points) return error.OrdinalPlanningBudgetExceeded;
                var rows = selection.iterator();
                while (rows.next()) |low| {
                    try work.takePoint();
                    if (work.points % 256 == 0) try self.context.ensureActive();
                    const row = (@as(u64, block.key_ptr.high) << 32) | low;
                    var key: [96]u8 = undefined;
                    const bytes = try std.fmt.bufPrint(&key, "lake2:{s}:{x:0>8}:{x:0>16}", .{ digest, block.key_ptr.group, row });
                    if (try lookup.lookupOne(bytes)) |num| try result.add(num);
                }
            }
        }
        return result;
    }
    /// Broad exact filters remain compressed; only the current candidate tile
    /// is translated. This avoids allocating an archive-sized dense ID array.
    fn allowsVectorKeys(raw: *anyopaque, member_keys: []const []const u8, output: []bool) !void {
        const self: *Execution = @ptrCast(@alignCast(raw));
        const RowRef = local.storage_rowsource_types.RowRef;
        if (member_keys.len != output.len) return error.InvalidSqlBackendResponse;
        var begin: usize = 0;
        while (begin < member_keys.len) {
            try self.context.ensureActive();
            const end = @min(member_keys.len, begin + 128);
            var scratch = std.heap.ArenaAllocator.init(local.sql_memory_budget.reclaimingAllocator(self.result_allocator));
            defer scratch.deinit();
            const a = scratch.allocator();
            var files: std.StringHashMapUnmanaged([]const u8) = .empty;
            var refs: [128]RowRef = undefined;
            var positions: [128]usize = undefined;
            var count: usize = 0;
            for (member_keys[begin..end], begin..) |member_key, position| {
                const key = try @import("lake_enrichment_units.zig").parent(member_key);
                if (self.overlay) |overlay| if (overlay.row(key) != null) {
                    output[position] = try allowsVectorKey(raw, member_key);
                    continue;
                };
                const coordinate = try @import("lake_index_native_state.zig").coordinates(key);
                const digest = key[6..70];
                const entry = try files.getOrPut(a, digest);
                if (!entry.found_existing) entry.value_ptr.* = (try self.fileForAlloc(a, digest, false)) orelse return error.ExternalLakeSnapshotMismatch;
                refs[count] = .{ .external = .{ .source_id = if (self.index_resolver) |resolver| resolver.source else self.source.inventory.source_id, .snapshot_id = if (self.index_resolver) |resolver| resolver.snapshot else self.source.inventory.snapshot_id, .identity_version = self.source.inventory.row_identity_version, .file_id = entry.value_ptr.*, .row_group_ordinal = coordinate.group, .row_ordinal = coordinate.row } };
                positions[count] = position;
                count += 1;
            }
            const translated = if (self.index_resolver) |resolver| mapped: {
                if (resolver.translate_many) |many| break :mapped try many(resolver.ptr, a, refs[0..count], true);
                if (resolver.translate_many_bounded) |many| {
                    var allowance: local.serverless_query_lake_serving.ServingSource.TranslationAllowance = .{};
                    break :mapped try many(resolver.ptr, a, refs[0..count], true, &allowance);
                }
                const result = try a.alloc(RowRef, count);
                for (refs[0..count], result) |ref, *out| out.* = try self.fromIndex(a, ref);
                break :mapped result;
            } else refs[0..count];
            if (translated.len != count) return error.InvalidLogicalRowIdentity;
            var candidates: [128]RowRef = undefined;
            var candidate_positions: [128]usize = undefined;
            var candidate_count: usize = 0;
            for (translated, positions[0..count]) |ref, position| {
                output[position] = false;
                if (!self.allowsCurrentVectorMask(ref)) continue;
                candidates[candidate_count] = ref;
                candidate_positions[candidate_count] = position;
                candidate_count += 1;
            }
            var allowed: [128]bool = undefined;
            @memset(allowed[0..candidate_count], true);
            if (candidate_count != 0) if (self.vector_include_provider) |*provider| try provider.allowsMany(candidates[0..candidate_count], allowed[0..candidate_count]);
            if (self.vector_exclude_provider) |*provider| {
                var remaining_refs: [128]RowRef = undefined;
                var remaining_positions: [128]usize = undefined;
                var remaining_count: usize = 0;
                for (candidates[0..candidate_count], allowed[0..candidate_count], 0..) |ref, keep, ci| if (keep) {
                    remaining_refs[remaining_count] = ref;
                    remaining_positions[remaining_count] = ci;
                    remaining_count += 1;
                };
                var excluded: [128]bool = undefined;
                if (remaining_count != 0) try provider.allowsMany(remaining_refs[0..remaining_count], excluded[0..remaining_count]);
                for (remaining_positions[0..remaining_count], excluded[0..remaining_count]) |ci, exclude| allowed[ci] = !exclude;
            }
            for (candidate_positions[0..candidate_count], allowed[0..candidate_count]) |position, keep| output[position] = keep;
            begin = end;
        }
    }
    fn allowsCurrentVectorRef(self: *Execution, ref: local.storage_rowsource_types.RowRef) !bool {
        if (!self.allowsCurrentVectorMask(ref)) return false;
        if (self.vector_include_provider) |*provider| if (!try provider.allows(ref)) return false;
        if (self.vector_exclude_provider) |*provider| if (try provider.allows(ref)) return false;
        return true;
    }
    fn allowsCurrentVectorMask(self: *Execution, ref: local.storage_rowsource_types.RowRef) bool {
        const current = ref.external;
        if (self.overlay) |overlay| if (overlay.physical) |*mask| if (mask.contains(current.file_id, current.row_group_ordinal, current.row_ordinal)) return false;
        if (self.vector_include) |*include| if (!include.contains(current.file_id, current.row_group_ordinal, current.row_ordinal)) return false;
        if (self.vector_exclude) |*exclude| if (exclude.contains(current.file_id, current.row_group_ordinal, current.row_ordinal)) return false;
        return true;
    }
    fn allowsVectorKey(raw: *anyopaque, member_key: []const u8) !bool {
        const key = try @import("lake_enrichment_units.zig").parent(member_key);
        const self: *Execution = @ptrCast(@alignCast(raw));
        try self.context.ensureActive();
        if (self.overlay) |pending_overlay| if (pending_overlay.row(key)) |row| {
            if (self.vector_filter) |query| {
                if (!try local.storage_db_query_graph_exec.jsonDocMatchesPatternFilter(self.arena, key, row, query)) return false;
            }
            if (self.vector_exclusion) |query| {
                if (try local.storage_db_query_graph_exec.jsonDocMatchesPatternFilter(self.arena, key, row, query)) return false;
            }
            return true;
        };
        const coordinate = try @import("lake_index_native_state.zig").coordinates(key);
        const file = (try self.fileFor(key[6..70], false)) orelse return error.ExternalLakeSnapshotMismatch;
        const index_ref: local.storage_rowsource_types.RowRef = .{ .external = .{ .source_id = if (self.index_resolver) |resolver| resolver.source else self.source.inventory.source_id, .snapshot_id = if (self.index_resolver) |resolver| resolver.snapshot else self.source.inventory.snapshot_id, .identity_version = self.source.inventory.row_identity_version, .file_id = file, .row_group_ordinal = coordinate.group, .row_ordinal = coordinate.row } };
        var scratch = std.heap.ArenaAllocator.init(self.result_allocator);
        defer scratch.deinit();
        const ref = try self.fromIndex(scratch.allocator(), index_ref);
        return self.allowsCurrentVectorRef(ref);
    }

    fn deinit(self: *Execution) void {
        self.ordinal_live_rows.deinit(self.result_allocator);
        if (self.vector_include_provider) |*provider| provider.deinit();
        if (self.vector_exclude_provider) |*provider| provider.deinit();
        self.highlight_locations.deinit(self.arena);
        for (self.highlight_pins.items) |*pin| pin.deinit();
        if (self.delivery_pin) |*pin| pin.deinit();
        for (self.runtimes.items) |runtime| runtime.release();
    }
    fn from(raw: ?*anyopaque) *Execution {
        return @ptrCast(@alignCast(raw.?));
    }
    fn canceled(raw: *const anyopaque) bool {
        const self: *Execution = @ptrCast(@alignCast(@constCast(raw)));
        self.context.ensureActive() catch return true;
        return false;
    }
    fn publicDocNum(raw: ?*anyopaque, a: A, snapshot: *const local.index.IndexSnapshot, doc: u32) !?[]u8 {
        const self = from(raw);
        try self.context.ensureActive();
        // Recent/enriched documents retain the ordinary stored-identity path.
        if (self.overlay != null or self.active_recent) return null;
        const identities = self.text_identities.get(@intFromPtr(snapshot)) orelse return null;
        if (identities.spanCount() == 0) return null;
        self.ordinal_live_rows.backing = self.result_allocator;
        var scratch = std.heap.ArenaAllocator.init(local.sql_memory_budget.reclaimingAllocator(a));
        defer scratch.deinit();
        var row = try identities.physicalRow(scratch.allocator(), &self.ordinal_live_rows, self.store.artifactStore(), .{ .cache = &self.server.lake_read_cache, .scope = self.domain, .context = self.context }, doc, self.source.inventory.source_id, self.source.inventory.snapshot_id);
        row.external.identity_version = self.source.inventory.row_identity_version;
        return try local.storage_rowsource_identity.allocId(a, row);
    }
    fn publicKey(raw: ?*anyopaque, a: A, member_key: []const u8) ![]u8 {
        const units = @import("lake_enrichment_units.zig");
        const key = try units.parent(member_key);
        const self = from(raw);
        if (key.len != member_key.len) {
            const public_parent = try publicKey(raw, a, key);
            defer a.free(public_parent);
            const runtime = self.unit_runtime orelse return error.InvalidChunkArtifact;
            const entry_name = if (runtime.dense_entry) |entry| entry.config.name else runtime.sparse_entry.?.config.name;
            const name = try std.fmt.allocPrint(a, "{s}_chunks", .{entry_name});
            defer a.free(name);
            return local.storage_internal_keys.chunkArtifactKeyAlloc(a, public_parent, name, try std.fmt.parseUnsigned(u32, member_key[key.len + units.marker.len ..], 16));
        }
        if (self.overlay) |overlay| if (overlay.row(key) != null) return a.dupe(u8, key);
        const position = try @import("lake_index_native_state.zig").coordinates(key);
        if (local.storage_rowsource_identity.isPublic(key)) {
            if ((try self.fileFor(try local.storage_rowsource_identity.publicFile(key), true)) == null) return error.ExternalLakeSnapshotMismatch;
            return units.rebind(a, member_key, key);
        }
        const file = (try self.fileFor(key[6..70], false)) orelse return error.ExternalLakeSnapshotMismatch;
        const index_ref: local.storage_rowsource_types.RowRef = .{ .external = .{ .source_id = if (self.index_resolver) |resolver| resolver.source else self.source.inventory.source_id, .snapshot_id = if (self.index_resolver) |resolver| resolver.snapshot else self.source.inventory.snapshot_id, .identity_version = self.source.inventory.row_identity_version, .file_id = file, .row_group_ordinal = position.group, .row_ordinal = position.row } };
        var scratch = std.heap.ArenaAllocator.init(local.sql_memory_budget.reclaimingAllocator(a));
        defer scratch.deinit();
        const parent_key = try local.storage_rowsource_identity.allocId(a, try self.fromIndex(scratch.allocator(), index_ref));
        defer a.free(parent_key);
        return units.rebind(a, member_key, parent_key);
    }
    fn nativeKey(raw: ?*anyopaque, a: A, member_key: []const u8) ![]u8 {
        const units = @import("lake_enrichment_units.zig");
        if (local.storage_internal_keys.isChunkArtifactRecordKey(member_key)) {
            var ref = (try local.storage_db_artifact_ids.decodeArtifactRefAlloc(a, member_key)) orelse return error.InvalidChunkArtifact;
            defer ref.deinit(a);
            const base = try nativeKey(raw, a, ref.document_id);
            defer a.free(base);
            return @constCast(try units.identity(a, base, .{ .ordinal = ref.chunk_id orelse return error.InvalidChunkArtifact, .chunked = true }));
        }
        const key = try units.parent(member_key);
        const self = from(raw);
        if (!local.storage_rowsource_identity.isPublic(key)) return a.dupe(u8, member_key);
        const position = try @import("lake_index_native_state.zig").coordinates(key);
        const file = (try self.fileFor(try local.storage_rowsource_identity.publicFile(key), true)) orelse return a.dupe(u8, key);
        const current_ref: local.storage_rowsource_types.RowRef = .{ .external = .{ .source_id = self.source.inventory.source_id, .snapshot_id = self.source.inventory.snapshot_id, .identity_version = self.source.inventory.row_identity_version, .file_id = file, .row_group_ordinal = position.group, .row_ordinal = position.row } };
        var scratch = std.heap.ArenaAllocator.init(local.sql_memory_budget.reclaimingAllocator(a));
        defer scratch.deinit();
        const index_ref = try self.toIndex(scratch.allocator(), current_ref);
        const digest = try self.privateDigest(index_ref.external.file_id);
        const parent_key = try std.fmt.allocPrint(a, "lake2:{s}:{x:0>8}:{x:0>16}", .{ digest, index_ref.external.row_group_ordinal, index_ref.external.row_ordinal });
        defer a.free(parent_key);
        return units.rebind(a, member_key, parent_key);
    }
    fn densePublicKey(raw: ?*anyopaque, a: A, _: *local.storage_db_catalog_index_manager.IndexManager.DenseIndex, key: []const u8) ![]u8 {
        return publicKey(raw, a, key);
    }
    fn buildOverlayPhysical(raw: *anyopaque, a: A) anyerror!@import("lake_index_physical_set.zig").Set {
        const self: *Execution = @ptrCast(@alignCast(raw));
        if (self.overlay.?.rows.image) |image| {
            var result = @import("lake_index_physical_set.zig").Set.init(a);
            errdefer result.deinit();
            for (image.parts) |part| if (part) |held| {
                var selected = try buildOverlayPhysicalFilter(raw, a, held.key_filter);
                defer selected.deinit();
                try result.orWith(&selected);
            };
            return result;
        }
        return buildOverlayPhysicalFilter(raw, a, self.overlay.?.key_filter);
    }
    fn buildOverlayPhysicalFilter(raw: *anyopaque, a: A, filter: []const u8) anyerror!@import("lake_index_physical_set.zig").Set {
        const self: *Execution = @ptrCast(@alignCast(raw));
        const resolver: @import("lake_index_text_predicate.zig").PhysicalResolver = .{ .server = self.server, .table = self.table, .source = self.source, .context = self.request, .store = self.store.artifactStore(), .store_identity = self.store.identity, .read_context = self.context, .pinned = .{ .artifacts = self.store.artifactStore(), .store_identity = self.store.identity, .domain = self.domain, .declarations = self.declarations, .read_context = self.context } };
        const excluded = (try resolver.resolve(a, filter)) orelse return error.UnsupportedQueryRequest;
        if (!excluded.exact) return error.UnsupportedQueryRequest;
        return excluded.bitmap;
    }
    fn resolveOverlayPhysical(self: *Execution) !void {
        const overlay = self.overlay orelse return;
        if (overlay.physical != null) return;
        if (self.wal_cut) |pin| if (try pin.physical(.{ .ptr = self, .build = buildOverlayPhysical, .build_filter = buildOverlayPhysicalFilter }, self.context)) |physical| {
            overlay.physical = physical.*;
            return;
        };
        overlay.physical = try buildOverlayPhysical(self, self.arena);
    }
    fn textDeclaration(self: *Execution, name: ?[]const u8) !?local.serverless_segment_sidecar_manifest.DeclaredArtifact {
        var count: usize = 0;
        for (self.declarations) |declaration| if (declaration.artifact.kind == .text_segment and corpus.supportsBuildMetadata(declaration.artifact.metadata_version)) {
            count += 1;
        };
        const selected = for (self.declarations) |declaration| {
            if (declaration.artifact.kind == .text_segment and corpus.supportsBuildMetadata(declaration.artifact.metadata_version) and (if (name) |explicit| std.mem.eql(u8, explicit, declaration.name) else count == 1 or std.mem.eql(u8, declaration.name, local.common_full_text_index_defaults.default_full_text_index_name))) break declaration;
        } else {
            // A known text index awaiting a format refresh is rebuilding,
            // rather than an invalid user-supplied index name.
            for (self.declarations) |declaration| {
                if (declaration.artifact.kind == .text_segment and !corpus.supportsBuildMetadata(declaration.artifact.metadata_version) and (name == null or std.mem.eql(u8, name.?, declaration.name))) return error.ExternalLakeIndexUnavailable;
            }
            return if (name == null) null else error.IndexNotFound;
        };
        try self.bindIndex(selected.name);
        return selected;
    }
    const HighlightPlan = struct { query: search.HighlightQuery, name: []const u8, stores_source: bool };
    fn highlightPlan(self: *Execution, name: ?[]const u8, text: types.TextQuery) !?HighlightPlan {
        const selected = (try self.textDeclaration(name)) orelse return null;
        const cached: @import("lake_index_aggregate_artifact.zig").CachedRead = .{ .cache = &self.server.lake_read_cache, .scope = self.store.identity, .context = self.context, .metrics = &self.server.lake_query_metrics };
        const token: @import("antfly_cancellation").CancellationToken = .{ .ptr = self, .is_cancelled_fn = canceled };
        const metadata = try @import("lake_index_decoded_metadata.zig").acquire(corpus.Directory, cached, self.store.artifactStore(), selected.artifact, token, corpus.loadDirectory);
        defer metadata.release();
        const root = metadata.value.root;
        if (!std.mem.eql(u8, &root.domain, &self.domain) or !@import("../serverless/build/lake_rebuild.zig").bindingsEqual(root.binding, selected.binding)) return error.InvalidNativeLakeTextCorpus;
        const config = try std.json.parseFromSliceLeaky(std.json.Value, self.arena, root.config_json, .{ .allocate = .alloc_always });
        var schema_ = try local.schema_mod.parseValidatedTableSchema(self.arena, self.schema_json);
        defer schema_.deinit(self.arena);
        const runtime = try local.schema_mod.deriveRuntimeTableSchema(self.arena, schema_);
        const field: ?[]const u8 = if (config == .object) if (config.object.get("field")) |value| if (value == .string) value.string else null else null else null;
        const stores_source = try corpus.storesSource(config) and std.mem.eql(u8, &try corpus.sourceRecipe(self.arena, .{ .table_id = self.table.id, .name = self.table.physical_name, .schema_json = self.schema_json }, root.config_json, true), &root.recipe);
        return .{ .name = selected.name, .stores_source = stores_source, .query = .{ .query = text, .text_analysis = try local.storage_db_catalog_index_manager.parseTextAnalysisForIndexConfig(self.arena, root.config_json, runtime), .runtime_schema = runtime, .selected_field = field } };
    }
    fn planHighlights(self: *Execution, req: types.SearchRequest) !void {
        if (self.highlight_queries != null or req.highlight == null) return;
        var queries: std.ArrayList(search.HighlightQuery) = .empty;
        if (req.full_text_queries.len != 0) {
            for (req.full_text_queries) |named| if (try self.highlightPlan(named.index_name, named.query)) |plan| try queries.append(self.arena, plan.query);
        } else if (req.full_text) |text| {
            if (try self.highlightPlan(req.primary_text_index_name orelse req.index_name, text)) |plan| {
                try queries.append(self.arena, plan.query);
                if (self.overlay == null and storedHighlightEligible(req, plan.query.selected_field, plan.stores_source)) {
                    self.use_stored_highlights = true;
                    self.highlight_index = plan.name;
                }
            }
        }
        self.highlight_queries = queries.items;
    }
    fn acquire(raw: ?*anyopaque, name: ?[]const u8) !?search.PinnedTextSource {
        const self = from(raw);
        if (self.forced_pin) |pin| return try pin.clone();
        const started = @import("antfly_platform").time.monotonicNs();
        defer self.server.lake_query_metrics.record(.index, started);
        try self.context.ensureActive();
        if (self.recent_text_window) {
            const preparation = self.overlay_preparation orelse return error.InvalidNativeLakeTextCorpus;
            var pin = try self.recent_text_source.?.clone();
            errdefer pin.deinit();
            var empty = local.encoding_roaring.RoaringBitmap.init(self.arena);
            defer empty.deinit();
            return try overlay_api.Overlay.window(pin, &empty, preparation.root.collection.?, preparation.plan.?, self.context);
        }
        const selected = (try self.textDeclaration(name)) orelse return null;
        const cached: @import("lake_index_aggregate_artifact.zig").CachedRead = .{ .cache = &self.server.lake_read_cache, .scope = self.store.identity, .context = self.context, .metrics = &self.server.lake_query_metrics };
        const cancellation: @import("antfly_cancellation").CancellationToken = .{ .ptr = self, .is_cancelled_fn = canceled };
        const metadata = try @import("lake_index_decoded_metadata.zig").acquire(corpus.Directory, cached, self.store.artifactStore(), selected.artifact, cancellation, corpus.loadDirectory);
        defer metadata.release();
        const root = metadata.value.root;
        if (!std.mem.eql(u8, &root.domain, &self.domain)) return error.InvalidNativeLakeTextCorpus;
        if (!@import("../serverless/build/lake_rebuild.zig").bindingsEqual(root.binding, selected.binding)) return error.InvalidNativeLakeTextCorpus;
        const config = try std.json.parseFromSliceLeaky(std.json.Value, self.arena, root.config_json, .{});
        const stores_source = try corpus.storesSource(config) and std.mem.eql(u8, &try corpus.sourceRecipe(self.arena, .{ .table_id = self.table.id, .name = self.table.physical_name, .schema_json = self.schema_json }, root.config_json, true), &root.recipe);
        try self.text_stores_source.put(self.arena, selected.name, stores_source);
        var selection = self.forced_selection;
        if (selection == null and self.overlay == null) if (self.active_text) |text| {
            selection = self.selectFiles(text, root, cached, cancellation) catch |err| switch (err) {
                error.OutOfMemory => null, // optional proof; full admission remains authoritative
                else => return err,
            };
        };
        var pin = if (self.prefetched_archive) |held| try held.clone() else self.server.lake_text_corpora.acquireSelected(self.server.embedding_provider_runtime.io, self.store.artifactStore(), selected.artifact, root, self.schema_json, cached, self.context, cancellation, selection) catch |err| switch (err) {
            // Exact segment routing is an optional admission optimization. Its
            // detailed metadata can cost more than compact full-inventory
            // routes; retry full admission when that optional budget is tight.
            error.NativeLakeTextCacheBusy, error.NativeLakeTextCorpusTooLarge, error.ArtifactReadBudgetExceeded => if (selection != null and self.forced_selection == null)
                try self.server.lake_text_corpora.acquire(self.server.embedding_provider_runtime.io, self.store.artifactStore(), selected.artifact, root, self.schema_json, cached, self.context, cancellation)
            else
                return err,
            else => return err,
        };
        errdefer pin.deinit();
        var identities = @as(*const @import("lake_index_text_predicate.zig").Identities, @ptrCast(@alignCast(pin.provider_metadata orelse return error.InvalidNativeLakeTextCorpus))).*;
        identities.row_resolver = if (self.relocated()) self.index_resolver else null;
        if (self.overlay) |overlay| {
            const joined = if (self.overlay_preparation) |preparation| bounded: {
                var excluded = try identities.bitmapForPhysical(self.arena, &overlay.physical.?, self.store.artifactStore(), cached);
                defer excluded.deinit();
                break :bounded try overlay_api.Overlay.window(pin, &excluded, preparation.root.collection.?, preparation.plan.?, self.context);
            } else full: {
                try self.resolveOverlayPhysical();
                var excluded = try identities.bitmapForPhysical(self.arena, &overlay.physical.?, self.store.artifactStore(), cached);
                defer excluded.deinit();
                var recent_pin = try self.server.lake_recent_text.acquireFamily(@import("lake_recent_text_cache.zig").Cache.key(self.store.identity, self.table.id, if (self.table.external_indexes) |indexes_| indexes_.object_generation else 0, self.source.inventory.source_id, self.source.inventory.snapshot_id, self.schema_json, root.config_json, selected.name, overlay), @import("lake_recent_text_cache.zig").Cache.family(self.store.identity, self.table.id, if (self.table.external_indexes) |indexes_| indexes_.object_generation else 0, self.source.inventory.source_id, self.source.inventory.snapshot_id, self.schema_json, root.config_json, selected.name), overlay, pin.name, pin.text_analysis, pin.runtime_schema, pin.selected_field, self.context);
                defer recent_pin.deinit();
                // Saturation can return a private pin; sharing gives the composed
                // owner the same lifetime contract as a cache hit.
                recent_pin = try recent_pin.share(std.heap.page_allocator);
                break :full try overlay_api.Overlay.composeRecent(pin, &excluded, recent_pin, self.context);
            };
            // compose consumed pin; the error cleanup must now own joined.
            pin = joined;
        }
        try self.text_identities.put(self.arena, @intFromPtr(pin.snapshot), identities);
        return pin;
    }
    fn selectFiles(self: *Execution, text: types.TextQuery, root: corpus.Root, cached: @import("lake_index_aggregate_artifact.zig").CachedRead, cancellation: @import("antfly_cancellation").CancellationToken) !?[]const [32]u8 {
        if (root.collection == null or root.inventory_version < 2 or (root.inventory_version == 2 and (root.segment_files == null or root.serving_routes == null)) or root.term_directory.version != 2) return null;
        switch (text) {
            .match_none => return &.{},
            .term, .match, .phrase, .bool_query => {},
            else => return null,
        }
        var budget: local.sql_memory_budget = .{ .backing = self.result_allocator, .limit = 8 * 1024 * 1024 };
        var temporary = std.heap.ArenaAllocator.init(budget.allocator());
        defer temporary.deinit();
        const sa = temporary.allocator();
        var schema_ = try local.schema_mod.parseValidatedTableSchema(sa, self.schema_json);
        defer schema_.deinit(sa);
        const runtime = try local.schema_mod.deriveRuntimeTableSchema(sa, schema_);
        const analysis = try local.storage_db_catalog_index_manager.parseTextAnalysisForIndexConfig(sa, root.config_json, runtime);
        return @import("lake_index_serving_selection.zig").select(self.arena, root, self.store.artifactStore(), cached, cancellation, text, analysis, runtime);
    }
    fn acquireForText(self: *Execution, name: ?[]const u8, text: types.TextQuery) !?search.PinnedTextSource {
        const previous = self.active_text;
        self.active_text = text;
        defer self.active_text = previous;
        return acquire(self, name);
    }
    fn attachHighlights(self: *Execution, a: A, req: types.SearchRequest, result: *types.SearchResult) !void {
        const options = req.highlight orelse return;
        if (req.defer_hierarchy_child_hydration or result.hits.len == 0) return;
        if (req.full_text == null and req.full_text_queries.len == 0) return;
        const started = @import("antfly_platform").time.monotonicNs();
        defer self.server.lake_query_metrics.record(.highlight, started);
        try self.context.ensureActive();
        try self.planHighlights(req);
        if (self.highlight_queries.?.len == 0) return;
        // Highlight the original source even when result shaping projected it
        // away. Hydration keeps the same snapshot, deletes and reader lease.
        var sources: ?[]?[]u8 = null;
        defer if (sources) |items| {
            for (items) |bytes| if (bytes) |owned| a.free(owned);
            a.free(items);
        };
        if (self.use_stored_highlights) {
            if (self.highlight_locations.count() != 0) {
                var prepared = try search.PreparedHighlights.init(a, options, self.highlight_queries.?);
                defer prepared.deinit();
                try self.attachLocatedHighlights(a, &prepared, result.hits);
            } else {
                var prepared = try search.PreparedHighlights.init(a, options, self.highlight_queries.?);
                defer prepared.deinit();
                try self.attachPinnedHighlights(a, &prepared, result.hits);
            }
            try self.context.ensureActive();
            return;
        } else if (!self.typed_delivery and (!req.include_stored or (!req.include_all_fields and !req.defer_stored_projection))) {
            const keys = try a.alloc([]const u8, result.hits.len);
            defer a.free(keys);
            for (result.hits, keys) |hit, *key| key.* = hit.id;
            sources = try loadManySelected(self, a, keys, self.hydration_fields);
        }
        try search.attachHighlightsWithIndexQueries(a, options, self.highlight_queries.?, result.hits, sources);
        try self.context.ensureActive();
    }
    fn attachPinnedHighlights(self: *Execution, a: A, prepared: *const search.PreparedHighlights, hits: []types.SearchHit) !void {
        if (self.highlight_pins.items.len == 0) {
            var pin = if (self.delivery_pin) |*ranked| try ranked.clone() else (try self.acquireForText(self.highlight_index, self.highlight_queries.?[0].query)) orelse return error.InvalidNativeLakeTextCorpus;
            errdefer pin.deinit();
            try self.highlight_pins.append(self.arena, pin);
        }
        if (self.highlight_pins.items.len != 1) return error.InvalidNativeLakeTextCorpus;
        const pin = self.highlight_pins.items[0];
        const identities = self.text_identities.get(@intFromPtr(pin.snapshot)) orelse return error.InvalidNativeLakeTextCorpus;
        const Winner = struct { index: usize, ordinal: u32 };
        const winners = try a.alloc(Winner, hits.len);
        defer a.free(winners);
        var live: @import("lake_index_text_predicate.zig").LiveRowsCache = .{ .backing = a };
        defer live.deinit(a);
        const cached: @import("lake_index_aggregate_artifact.zig").CachedRead = .{ .cache = &self.server.lake_read_cache, .scope = self.store.identity, .context = self.context, .metrics = &self.server.lake_query_metrics };
        var input = std.heap.ArenaAllocator.init(a);
        defer input.deinit();
        for (hits, winners, 0..) |hit, *winner, index| {
            try self.context.ensureActive();
            _ = input.reset(.retain_capacity);
            const ia = input.allocator();
            const key = try publicKey(self, ia, hit.id);
            const position = try @import("lake_index_native_state.zig").coordinates(key);
            const file = (try self.fileFor(try local.storage_rowsource_identity.publicFile(key), true)) orelse return error.ExternalLakeSnapshotMismatch;
            const row: local.storage_rowsource_types.RowRef = .{ .external = .{ .source_id = self.source.inventory.source_id, .snapshot_id = self.source.inventory.snapshot_id, .identity_version = self.source.inventory.row_identity_version, .file_id = file, .row_group_ordinal = position.group, .row_ordinal = position.row } };
            winner.* = .{ .index = index, .ordinal = (try identities.ordinal(a, &live, self.store.artifactStore(), cached, row)) orelse return error.InvalidNativeLakeTextCorpus };
        }
        std.mem.sort(Winner, winners, {}, struct {
            fn less(_: void, left: Winner, right: Winner) bool {
                return left.ordinal < right.ordinal;
            }
        }.less);
        var cursor = local.segment.SegmentReader.StoredDocCursor.init(a);
        defer cursor.deinit();
        var segment_index: usize = 0;
        var lower: u32 = 0;
        for (winners) |winner| {
            try self.context.ensureActive();
            while (segment_index < pin.snapshot.segments.len and winner.ordinal - lower >= pin.snapshot.segments[segment_index].reader.doc_count) {
                lower = try std.math.add(u32, lower, pin.snapshot.segments[segment_index].reader.doc_count);
                segment_index += 1;
            }
            if (segment_index == pin.snapshot.segments.len) return error.InvalidNativeLakeTextCorpus;
            const segment = try pin.snapshot.readSegment(segment_index);
            const doc = (try cursor.get(&segment.reader, winner.ordinal - lower)) orelse return error.InvalidNativeLakeTextCorpus;
            _ = input.reset(.retain_capacity);
            try self.attachStoredSnippet(a, input.allocator(), prepared, hits[winner.index .. winner.index + 1], doc);
        }
    }
    fn attachStoredSnippet(self: *Execution, a: A, scratch: A, prepared: *const search.PreparedHighlights, hit: []types.SearchHit, doc: local.segment.SegmentReader.StoredDocRef) !void {
        const expected = try nativeKey(self, scratch, hit[0].id);
        if (!std.mem.eql(u8, doc.id, expected)) return error.InvalidNativeLakeTextCorpus;
        const value = try std.json.parseFromSliceLeaky(std.json.Value, scratch, doc.data, .{ .parse_numbers = false });
        if (value != .object) return error.InvalidNativeLakeTextCorpus;
        try prepared.attachValue(a, scratch, &hit[0], value);
    }
    /// Sort a compact permutation, retaining result order and only one reader
    /// and decoded stored block. Hidden source is released after each snippet.
    fn attachLocatedHighlights(self: *Execution, a: A, prepared: *const search.PreparedHighlights, hits: []types.SearchHit) !void {
        const Winner = struct { index: usize, location: StoredHighlightLocation };
        const winners = try a.alloc(Winner, hits.len);
        defer a.free(winners);
        for (hits, winners, 0..) |hit, *winner, index| winner.* = .{ .index = index, .location = self.highlight_locations.get(hit.id) orelse return error.InvalidNativeLakeTextCorpus };
        std.mem.sort(Winner, winners, {}, struct {
            fn less(_: void, left: Winner, right: Winner) bool {
                const order = std.mem.order(u8, &left.location.segment, &right.location.segment);
                return if (order == .eq) left.location.document < right.location.document else order == .lt;
            }
        }.less);
        var reader_arena = std.heap.ArenaAllocator.init(a);
        defer reader_arena.deinit();
        var input = std.heap.ArenaAllocator.init(a);
        defer input.deinit();
        var begin: usize = 0;
        while (begin < winners.len) {
            try self.context.ensureActive();
            var end = begin + 1;
            while (end < winners.len and std.mem.eql(u8, &winners[begin].location.segment, &winners[end].location.segment)) : (end += 1) {}
            _ = reader_arena.reset(.retain_capacity);
            const sa = reader_arena.allocator();
            var child: Execution = .{ .server = self.server, .table = self.table, .source = self.source, .store = self.store, .domain = self.domain, .declarations = self.declarations, .context = self.context, .request = self.request, .schema_json = self.schema_json, .arena = sa, .result_allocator = sa, .navigation = self.navigation, .files = self.files, .private_files = self.private_files, .private_digests = self.private_digests, .forced_selection = &.{winners[begin].location.segment} };
            defer child.deinit();
            var pin = blk: {
                const started = @import("antfly_platform").time.monotonicNs();
                defer self.hydration_ns +|= @import("antfly_platform").time.monotonicNs() -| started;
                break :blk (try acquire(&child, self.highlight_index)) orelse return error.InvalidNativeLakeTextCorpus;
            };
            defer pin.deinit();
            if (pin.snapshot.segments.len != 1) return error.InvalidNativeLakeTextCorpus;
            const segment = try pin.snapshot.readSegment(0);
            var cursor = local.segment.SegmentReader.StoredDocCursor.init(a);
            defer cursor.deinit();
            for (winners[begin..end], begin..) |winner, position| {
                for (winners[position..@min(end, position + 4)]) |upcoming| try pin.snapshot.prefetchStoredDoc(upcoming.location.document);
                try self.context.ensureActive();
                _ = input.reset(.retain_capacity);
                const ia = input.allocator();
                const page = hits[winner.index .. winner.index + 1];
                const doc = (try cursor.get(&segment.reader, winner.location.document)) orelse return error.InvalidNativeLakeTextCorpus;
                try self.attachStoredSnippet(a, ia, prepared, page, doc);
            }
            begin = end;
        }
    }
    fn hydrateDelivery(raw: *anyopaque, a: A, hits: []types.SearchHit) !void {
        const self: *Execution = @ptrCast(@alignCast(raw));
        try self.context.ensureActive();
        try self.hydrateTyped(a, self.delivery_request.?, hits);
        var result: types.SearchResult = .{ .alloc = a, .hits = hits, .total_hits = @intCast(hits.len), .graph_results = &.{} };
        try self.attachHighlights(a, self.delivery_request.?, &result);
    }
    fn releaseDelivery(raw: *anyopaque, hits: []types.SearchHit) void {
        const self: *Execution = @ptrCast(@alignCast(raw));
        for (hits) |*hit| {
            types.freeHighlights(self.result_allocator, hit.highlights);
            hit.highlights = &.{};
            if (hit.source_value) |*source| types.deinitJsonValue(self.result_allocator, source);
            hit.source_value = null;
        }
    }
    fn hydrateTyped(self: *Execution, a: A, req: types.SearchRequest, hits: []types.SearchHit) !void {
        if (hits.len == 0) return;
        const started = @import("antfly_platform").time.monotonicNs();
        defer self.server.lake_query_metrics.record(.hydration, started);
        const keys = try a.alloc([]const u8, hits.len);
        defer a.free(keys);
        for (hits, keys) |hit, *key| key.* = hit.id;
        // Document ordinals have meaning in one text snapshot only. Composed
        // and vector results keep their existing physical hydration path.
        var pin: ?*const search.PinnedTextSource = null;
        const one_text_source = (req.full_text != null and req.full_text_queries.len == 0) or (req.full_text == null and req.full_text_queries.len == 1);
        if (one_text_source and req.dense_queries.len == 0 and req.sparse_queries.len == 0 and req.dense == null and req.sparse == null) {
            const bound = for (hits) |hit| {
                if (hit.native_text_doc_id == null) break false;
            } else true;
            if (bound) if (self.delivery_pin) |*ranked| {
                pin = ranked;
            };
        }
        try self.hydrateTypedFromSource(a, hits, keys, pin);
    }
    fn hydrateTypedFromSource(self: *Execution, a: A, hits: []types.SearchHit, keys: []const []const u8, pin: ?*const search.PinnedTextSource) !void {
        if (self.overlay == null and (pin == null or pin.?.stored_projection_fields.len == 0)) {
            const columns = try loadSelected(types.ColumnSource, self, a, keys, self.hydration_fields);
            defer {
                for (columns) |value| if (value) |owned| owned.deinit();
                a.free(columns);
            }
            for (hits, columns) |*hit, *value| {
                if (hit.stored_data) |bytes| a.free(bytes);
                hit.stored_data = null;
                if (hit.source_value) |*source| types.deinitJsonValue(a, source);
                hit.source_value = null;
                std.debug.assert(hit.column_source == null);
                hit.column_source = value.* orelse return error.StoredDocMissing;
                value.* = null;
            }
            return;
        }
        const projection = @import("lake_index_source_projection.zig");
        var fields = self.hydration_fields;
        var stored_fields: []const []const u8 = &.{};
        if (pin) |value| if (value.stored_projection_fields.len != 0) {
            const wanted = fields orelse all: {
                const all = try self.arena.alloc([]const u8, self.table.columns.len);
                for (all, self.table.columns) |*path, column| path.* = column.path;
                break :all all;
            };
            const plan = try projection.plan(self.arena, wanted, value.stored_projection_fields);
            fields = plan.parquet;
            stored_fields = plan.sidecar;
        };
        const values = if (stored_fields.len != 0 and fields.?.len == 0) empty: {
            const rows = try a.alloc(?std.json.Value, hits.len);
            @memset(rows, std.json.Value{ .object = .empty });
            break :empty rows;
        } else try loadSelected(std.json.Value, self, a, keys, fields);
        defer {
            for (values) |*value| if (value.*) |*owned| types.deinitJsonValue(a, owned);
            a.free(values);
        }
        var hydrator = projection.Hydrator.init(a);
        defer hydrator.deinit();
        if (stored_fields.len != 0) for (hits, values, 0..) |hit, *value, position| {
            try self.context.ensureActive();
            // Keep a small lookahead through the current snapshot's bounded
            // source queue. Required reads still authenticate and report errors.
            for (hits[position..@min(hits.len, position + 4)]) |upcoming| try pin.?.snapshot.prefetchStoredDoc(upcoming.native_text_doc_id.?);
            const key = try nativeKey(self, a, hit.id);
            defer a.free(key);
            if (value.*) |*row| try hydrator.append(a, pin.?.snapshot, hit.native_text_doc_id.?, key, stored_fields, row) else return error.StoredDocMissing;
        };
        for (hits, values) |*hit, *value| {
            // Residual predicate evaluation may have temporarily loaded source.
            // Final projected columns replace it after all filtering/ranking.
            if (hit.stored_data) |bytes| a.free(bytes);
            hit.stored_data = null;
            if (hit.source_value) |*source| types.deinitJsonValue(a, source);
            hit.source_value = null;
            std.debug.assert(hit.column_source == null);
            hit.source_value = value.* orelse return error.StoredDocMissing;
            value.* = null;
        }
    }
    fn planHydration(self: *Execution, req: types.SearchRequest) !?[]const []const u8 {
        const projected = (try projectionColumns(self.arena, self.table, req)) orelse return null;
        var fields: std.ArrayList([]const u8) = .empty;
        try fields.appendSlice(self.arena, projected);
        for (req.order_by) |order| try appendHydrationPath(self.arena, self.table, &fields, order.field);
        const options = req.highlight orelse return fields.items;
        if (req.full_text == null and req.full_text_queries.len == 0) return fields.items;
        try self.planHighlights(req);
        if (self.use_stored_highlights) return fields.items;
        try appendHydrationPath(self.arena, self.table, &fields, "_type");
        if (options.fields.len != 0) {
            for (options.fields) |path| try appendHydrationPath(self.arena, self.table, &fields, path);
        } else if (self.highlight_queries) |queries| {
            for (queries) |query| if (!try appendIndexHydration(self.arena, self.table, &fields, query)) return null;
        }
        return fields.items;
    }

    fn noLocal(_: ?*anyopaque, _: ?[]const u8) !?*local.storage_db_catalog_index_manager.IndexManager.TextIndex {
        return null;
    }
    fn chunkBacked(_: ?*anyopaque, _: A, _: ?[]const u8) !bool {
        return false;
    }
    fn matchAll(raw: ?*anyopaque, a: A, req: types.SearchRequest) !types.SearchResult {
        return search.searchMatchAll(a, req, .{ .ctx = raw, .collect_candidates = collectAll, .collect_candidates_stream = streamAll, .text_index_entry = noLocal, .load_projected_document = requireProjected, .load_projected_documents = loadProjected, .load_stored = loadOne, .load_many_stored = loadMany });
    }
    fn unusedStoreScan(_: ?*anyopaque, _: A, _: []const u8, _: []const u8) ![]local.storage_docstore.OwnedKVPair {
        return error.UnsupportedSqlExecution;
    }
    fn neverExpired(raw: ?*anyopaque, _: A, _: []const u8) !bool {
        try from(raw).context.ensureActive();
        return false;
    }
    fn collectAll(raw: ?*anyopaque, a: A, req: types.SearchRequest, options: search.MatchAllCandidateCollectOptions) !search.MatchAllCandidates {
        return search.collectMatchAllCandidatesWithOptions(a, req, .{ .ctx = raw, .scan_ids = scanIds, .scan_store_range = unusedStoreScan, .is_expired_key = neverExpired }, options);
    }
    fn streamAll(raw: ?*anyopaque, a: A, req: types.SearchRequest, options: search.MatchAllCandidateCollectOptions, consumer: ?*anyopaque, visit: search.MatchAllCandidateConsumer) !search.MatchAllCandidateStreamStats {
        return search.streamMatchAllCandidatesWithOptions(a, req, .{ .ctx = raw, .scan_ids = scanIds, .scan_store_range = unusedStoreScan, .is_expired_key = neverExpired }, options, consumer, visit);
    }
    fn scanCheckpoint(raw: *anyopaque) !void {
        try @as(*Execution, @ptrCast(@alignCast(raw))).context.ensureActive();
    }
    fn scanIds(raw: ?*anyopaque, a: A, options: search.MatchAllCandidateCollectOptions, target: ?*anyopaque, visit: *const fn (?*anyopaque, []const u8) anyerror!local.storage_docstore.DocStore.ScanAction) !void {
        const self = from(raw);
        try self.resolveOverlayPhysical();
        var request = self.request;
        request.cancellation = .{ .ptr = self, .is_cancelled_fn = canceled };
        const cursor = try local.sql_lake_cursor.openPinned(a, self.table, .{ .fields = &.{}, .primary_order = true, .after = options.primary_key_start_after, .before = options.primary_key_stop_before, .limit = 256 }, request, self.source);
        defer cursor.close(cursor.ptr);
        var manager: local.sql_spill.Manager = .{ .alloc = a, .io = self.context.io.?, .context = self, .checkpoint = scanCheckpoint };
        defer manager.deinit();
        var sort = local.sql_spill.Sort.init(a, &manager, &.{.{ .descending = options.primary_key_reverse }}, 512 * 1024);
        defer sort.deinit();
        var ordinal: u64 = 0;
        while (true) {
            const page = try cursor.next(cursor.ptr, a, 256);
            defer page.deinit();
            for (page.rows) |row| {
                if (self.overlay) |overlay| {
                    const position = try @import("lake_index_native_state.zig").coordinates(row.id);
                    const file = (try self.fileFor(try local.storage_rowsource_identity.publicFile(row.id), true)) orelse return error.ExternalLakeSnapshotMismatch;
                    if (overlay.physical.?.contains(file, position.group, position.row)) continue;
                }
                if (options.primary_key_reverse or self.overlay != null) {
                    try sort.add(.{ .values = &.{}, .keys = &.{local.sql_scalar.Datum.fromJson(.{ .string = row.id })}, .ordinal = ordinal });
                    ordinal += 1;
                } else if (try visit(target, row.id) == .stop) return;
            }
            if (page.after == null) break;
        }
        if (self.overlay) |overlay| {
            var ids = overlay.rows.keyIterator();
            while (ids.next()) |id| {
                try self.context.ensureActive();
                if (options.primary_key_start_after) |after| if (std.mem.order(u8, id.*, after) != .gt) continue;
                if (options.primary_key_stop_before) |before| if (std.mem.order(u8, id.*, before) != .lt) continue;
                try sort.add(.{ .values = &.{}, .keys = &.{local.sql_scalar.Datum.fromJson(.{ .string = id.* })}, .ordinal = ordinal });
                ordinal += 1;
            }
        }
        if (options.primary_key_reverse or self.overlay != null) {
            var arena = std.heap.ArenaAllocator.init(a);
            defer arena.deinit();
            while (true) {
                _ = arena.reset(.retain_capacity);
                const row = try sort.next(arena.allocator()) orelse break;
                if (try visit(target, row.keys[0].value.string) == .stop) return;
            }
        }
    }
    fn resolveIndexedFilter(raw: ?*anyopaque, a: A, snapshot: *const local.index.IndexSnapshot, json: []const u8) !?search.IndexedTextPredicate {
        const self = from(raw);
        if (self.recent_text_window) {
            var result = local.encoding_roaring.RoaringBitmap.init(a);
            errdefer result.deinit();
            var temporary = std.heap.ArenaAllocator.init(a);
            defer temporary.deinit();
            const filter = try std.json.parseFromSliceLeaky(std.json.Value, self.arena, json, .{});
            var ordinal: u32 = 0;
            for (snapshot.segments) |segment| for (0..segment.reader.doc_count) |_| {
                try self.context.ensureActive();
                _ = temporary.reset(.retain_capacity);
                const id = (try snapshot.storedIdScoped(temporary.allocator(), ordinal)) orelse return error.InvalidNativeLakeTextCorpus;
                const row = self.overlay.?.row(id) orelse return error.InvalidNativeLakeTextCorpus;
                if (try local.storage_db_query_graph_exec.jsonDocMatchesPatternFilter(temporary.allocator(), id, row, filter)) try result.add(ordinal);
                ordinal += 1;
            };
            return .{ .bitmap = result };
        }
        const identities = self.text_identities.get(@intFromPtr(snapshot)) orelse return null;
        const resolver: @import("lake_index_text_predicate.zig").Resolver = .{ .allow_partial = self.predicate_allow_partial and !std.mem.eql(u8, json, self.predicate_exclusion_json), .server = self.server, .table = self.table, .source = self.source, .context = self.request, .identities = identities, .store = self.store.artifactStore(), .store_identity = self.store.identity, .read_context = self.context, .pinned = .{ .artifacts = self.store.artifactStore(), .store_identity = self.store.identity, .domain = self.domain, .declarations = self.declarations, .read_context = self.context } };
        var result = (try resolver.resolve(a, json)) orelse return null;
        errdefer result.bitmap.deinit();
        if (self.overlay) |overlay| {
            if (!result.exact) {
                result.bitmap.deinit();
                return null;
            }
            const filter = try std.json.parseFromSliceLeaky(std.json.Value, self.arena, json, .{});
            var offset: u32 = 0;
            for (snapshot.segments, 0..) |segment, ordinal| {
                if (ordinal < identities.offsets.len - 1) {
                    offset = try std.math.add(u32, offset, segment.reader.doc_count);
                    continue;
                }
                for (0..segment.reader.doc_count) |doc| {
                    const id = try snapshot.storedIdScoped(self.arena, offset + @as(u32, @intCast(doc))) orelse continue;
                    const row = overlay.row(id) orelse continue;
                    try self.context.ensureActive();
                    if (try local.storage_db_query_graph_exec.jsonDocMatchesPatternFilter(self.arena, id, row, filter)) try result.bitmap.add(offset + @as(u32, @intCast(doc)));
                }
                offset = try std.math.add(u32, offset, segment.reader.doc_count);
            }
        }
        return result;
    }
    const Ordered = struct {
        a: A,
        execution: *Execution,
        predicate: @import("lake_index_sql_rows.zig").Predicate,
        plan_arena: std.heap.ArenaAllocator,
        identities: @import("lake_index_text_predicate.zig").Identities,
        page: []const local.storage_rowsource_types.RowRef = &.{},
        window: std.heap.ArenaAllocator,
        position: usize = 0,
        scanned: u64 = 0,
        page_size: usize = 256,
        bitmaps: @import("lake_index_text_predicate.zig").LiveRowsCache = .{},
        fn scannedCount(raw: *anyopaque) u64 {
            const self: *Ordered = @ptrCast(@alignCast(raw));
            return self.scanned;
        }
        fn close(raw: *anyopaque) void {
            const self: *Ordered = @ptrCast(@alignCast(raw));
            self.predicate.deinit();
            self.window.deinit();
            self.bitmaps.deinit(self.a);
            self.plan_arena.deinit();
            self.a.destroy(self);
        }
        fn next(raw: *anyopaque, physical_budget: usize) !?u32 {
            const self: *Ordered = @ptrCast(@alignCast(raw));
            const start = self.scanned;
            while (true) {
                if (self.scanned - start >= physical_budget) return error.OrderedCandidateBudgetExceeded;
                try self.execution.context.ensureActive();
                if (self.position == self.page.len) {
                    _ = self.window.reset(.retain_capacity);
                    self.page = try self.predicate.next(self.window.allocator(), @min(self.page_size, physical_budget - @as(usize, @intCast(self.scanned - start))));
                    self.position = 0;
                    if (self.page.len == 0) return null;
                }
                const ref = self.page[self.position];
                self.position += 1;
                self.scanned += 1;
                const execution = self.execution;
                const cached: @import("lake_index_aggregate_artifact.zig").CachedRead = .{ .cache = &execution.server.lake_read_cache, .scope = execution.store.identity, .context = execution.context };
                if (try self.identities.ordinal(self.a, &self.bitmaps, execution.store.artifactStore(), cached, ref)) |number| return number;
            }
        }
    };
    fn orderedTextScan(self: *Execution, a: A, req: types.SearchRequest) !?local.sql_catalog.Scan {
        if (req.order_by.len < 2 or !std.mem.eql(u8, req.order_by[req.order_by.len - 1].field, "_id")) return null;
        const orders = try a.alloc(local.sql_catalog.Scan.Order, req.order_by.len - 1);
        for (orders, req.order_by[0..orders.len]) |*order, field| {
            const column = for (self.table.columns) |column| {
                if (std.mem.eql(u8, column.name, field.field)) break column;
            } else return null;
            _ = column;
            order.* = .{ .column = field.field, .descending = field.desc, .nulls_first = !field.desc };
        }
        const sql_rows = @import("lake_index_sql_rows.zig");
        var conditions: std.ArrayList(local.sql_catalog.Condition) = .empty;
        if (req.filter_query_json.len != 0) {
            const parsed = try std.json.parseFromSlice(std.json.Value, a, req.filter_query_json, .{});
            const compiled = try local.storage_db_query_graph_exec.compilePatternFilter(a, parsed.value);
            var required: std.ArrayList(local.sql_catalog.Condition) = .empty;
            try @import("lake_index_text_predicate.zig").requiredConditions(a, compiled, &required, 0);
            for (required.items) |condition| {
                if (try sql_rows.compatibleSearchConditions(a, self.table, &.{condition})) try conditions.append(a, condition);
            }
        }
        // Seek inclusively on the leading field. Public IDs break ties in the
        // native collector, so an entire boundary tie group must remain visible.
        if (req.search_after.len != 0 or req.search_before.len != 0) {
            const bound = if (req.search_before.len != 0) req.search_before[0] else req.search_after[0];
            if (bound == .null) return null;
            const kind = (try self.table.column(orders[0].column)).type;
            const normalized = local.sql_lake_values.comparisonValue(a, bound, kind) catch |err| switch (err) {
                error.OutOfMemory => return err,
                else => return null,
            };
            const condition: local.sql_catalog.Condition = .{ .column = orders[0].column, .op = if (orders[0].descending != (req.search_before.len != 0)) .lte else .gte, .value = normalized };
            if (!try sql_rows.compatibleSearchConditions(a, self.table, &.{condition})) return null;
            try conditions.append(a, condition);
        }
        return .{ .fields = &.{}, .conditions = conditions.items, .order = orders, .limit = 256, .row_goal = @as(u64, req.offset) + req.limit };
    }
    fn openOrderedTextCandidates(raw: ?*anyopaque, a: A, req: types.SearchRequest, snapshot: *const local.index.IndexSnapshot) !?search.OrderedTextCandidates {
        const self = from(raw);
        // Window admission owns only a subset of native ordinals. A global
        // ordered index would rescan its prefix for each window; collect that
        // window with native doc values instead.
        if (self.forced_selection != null) return null;
        const identities = self.text_identities.get(@intFromPtr(snapshot)) orelse return null;
        var arena = std.heap.ArenaAllocator.init(a);
        var keep_arena = false;
        defer if (!keep_arena) arena.deinit();
        const scan = (try self.orderedTextScan(arena.allocator(), req)) orelse return null;
        const sql_rows = @import("lake_index_sql_rows.zig");
        var predicate = (try sql_rows.tryOpenOrderedPredicate(a, self.server, self.table, scan, self.request, self.source, req.search_before.len != 0, .{ .values = if (req.search_before.len != 0) req.search_before else req.search_after, .id_descending = req.order_by[req.order_by.len - 1].desc })) orelse return null;
        errdefer predicate.deinit();
        const owner = try a.create(Ordered);
        owner.* = .{ .a = a, .execution = self, .predicate = predicate, .plan_arena = arena, .window = .init(a), .identities = identities, .page_size = if (predicate.complete_order) @min(256, @as(usize, req.offset) + req.limit + 1) else 256 };
        keep_arena = true;
        return .{ .ptr = owner, .next = Ordered.next, .scanned_count = Ordered.scannedCount, .complete_order = predicate.complete_order, .close = Ordered.close };
    }
    const RelocatedTie = struct {
        execution: *Execution,
        identities: @import("lake_index_text_predicate.zig").Identities,
        resolver: local.serverless_query_lake_serving.ServingSource.RowResolver,
        fn fallback(_: *const anyopaque, doc: u32) u64 {
            return doc;
        }
        fn resolve(raw: *const anyopaque, a: A, doc: u32) !u64 {
            var result: [1]u64 = undefined;
            try resolveMany(raw, a, &.{doc}, &result);
            return result[0];
        }
        fn resolveMany(raw: *const anyopaque, a: A, docs: []const u32, ranks: []u64) !void {
            const self: *const RelocatedTie = @ptrCast(@alignCast(raw));
            if (docs.len != ranks.len or docs.len > 256) return error.InvalidSearchTieRank;
            try self.execution.context.ensureActive();
            var scratch = std.heap.ArenaAllocator.init(a);
            defer scratch.deinit();
            const sa = scratch.allocator();
            // Mutable navigation belongs to this tile/scoring lane. Shared
            // authenticated decoded pages remain in the publication cache.
            var live: @import("lake_index_text_predicate.zig").LiveRowsCache = .{ .backing = a };
            defer live.deinit(a);
            const cached: @import("lake_index_aggregate_artifact.zig").CachedRead = .{ .cache = &self.execution.server.lake_read_cache, .scope = self.execution.store.identity, .context = self.execution.context };
            const archive_docs = self.identities.offsets[self.identities.offsets.len - 1];
            var refs: [256]local.storage_rowsource_types.RowRef = undefined;
            var positions: [256]usize = undefined;
            var values: [256]u64 = undefined;
            var count: usize = 0;
            for (docs, 0..) |doc, i| {
                if (doc >= archive_docs) {
                    ranks[i] = std.math.add(u64, self.resolver.rows, doc - archive_docs) catch return error.InvalidSearchTieRank;
                    continue;
                }
                refs[count] = try self.identities.physicalRow(sa, &live, self.execution.store.artifactStore(), cached, doc, self.resolver.source, self.resolver.snapshot);
                positions[count] = i;
                count += 1;
            }
            if (self.resolver.tie_ranks) |batch| {
                try batch(self.resolver.ptr, sa, refs[0..count], values[0..count]);
            } else for (refs[0..count], values[0..count]) |ref, *value| value.* = try self.resolver.tie_rank(self.resolver.ptr, sa, ref);
            for (positions[0..count], values[0..count]) |i, value| ranks[i] = value;
        }
    };
    fn acquireForRanking(raw: ?*anyopaque, name: ?[]const u8) !?search.PinnedTextSource {
        const self = from(raw);
        var pin = (try acquire(raw, name)) orelse return null;
        errdefer pin.deinit();
        // Only acquire()'s registered archive snapshots carry Identities.
        // Recent windows carry Overlay.RecentMetadata in the same opaque
        // provider slot. Reinterpreting that payload as an archive tie-rank
        // callback can jump into the digest bytes during accepted-cut reads.
        if (self.text_identities.get(@intFromPtr(pin.snapshot))) |identities| {
            if (if (self.relocated()) self.index_resolver else null) |resolver| {
                const tie = try self.arena.create(RelocatedTie);
                tie.* = .{ .execution = self, .identities = identities, .resolver = resolver };
                tie.identities.row_resolver = null;
                try self.relocated_ties.append(self.arena, tie);
                pin.logical_tie_rank = .{ .ptr = tie, .rank = RelocatedTie.fallback, .resolve = RelocatedTie.resolve, .resolve_many = RelocatedTie.resolveMany };
            } else pin.logical_tie_rank = identities.tie_rank;
        }
        if (self.typed_delivery) {
            pin = try pin.share(self.result_allocator);
            const retained = try pin.clone();
            if (self.delivery_pin) |*previous| previous.deinit();
            self.delivery_pin = retained;
        }
        return pin;
    }
    // Broad score queries stream immutable routing windows. Every window uses
    // the publication's collection statistics and global term frequencies;
    // only admission/doc ordinals are local. Source hydration remains after
    // final top-K movement. Metadata filters, typed ordering/cursors and
    // highlights and text components of hybrid/composed retrieval share the same
    // bounded coordinator. The composed owner retains fusion and source leases;
    // source-dependent transforms run once on the global candidate pool.
    fn predicatePacks(self: *Execution, root: corpus.Root, cached: @import("lake_index_aggregate_artifact.zig").CachedRead, token: @import("antfly_cancellation").CancellationToken, json: []const u8) !?[]const [32]u8 {
        if (root.source_routes_version != 1) return null;
        var budget: local.sql_memory_budget = .{ .backing = std.heap.page_allocator, .limit = 16 * 1024 * 1024 };
        var arena = std.heap.ArenaAllocator.init(budget.allocator());
        defer arena.deinit();
        const a = arena.allocator();
        const resolver: @import("lake_index_text_predicate.zig").PhysicalResolver = .{ .server = self.server, .table = self.table, .source = self.source, .context = self.request, .store = self.store.artifactStore(), .store_identity = self.store.identity, .read_context = self.context, .pinned = .{ .artifacts = self.store.artifactStore(), .store_identity = self.store.identity, .domain = self.domain, .declarations = self.declarations, .read_context = self.context } };
        var allowance: local.serverless_query_lake_serving.ServingSource.TranslationAllowance = .{};
        var candidates = (try resolver.selectiveCandidates(a, json, if (self.relocated()) 4096 else std.math.maxInt(usize), &allowance)) orelse return null;
        defer candidates.deinit();
        const tree = @import("../serverless/graph_segment/page_tree.zig");
        var store = self.store.artifactStore();
        var reads: u64 = 64 * 1024 * 1024;
        var writes: u64 = 0;
        var pages: @import("../serverless/graph_segment/page_store.zig").PageStore = .{ .domain = root.domain, .artifacts = &store, .cancellation = token, .remaining_read_bytes = &reads, .remaining_write_bytes = &writes };
        var cache: tree.Cache = .{ .alloc = a, .underlying = pages.store() };
        defer cache.deinit();
        var index_candidates: ?@import("lake_index_physical_set.zig").Set = null;
        defer if (index_candidates) |*set| set.deinit();
        if (self.relocated()) index_candidates = try @import("lake_index_text_predicate.zig").indexSelection(a, self.source, self.index_resolver, &candidates, &allowance);
        const source_id = if (self.index_resolver) |resolver_| resolver_.source else self.source.inventory.source_id;
        const snapshot_id = if (self.index_resolver) |resolver_| resolver_.snapshot else self.source.inventory.snapshot_id;
        const selected = try @import("lake_index_source_routes.zig").select(a, cache.store(), root, store, cached, token, if (index_candidates) |*set| set else &candidates, source_id, snapshot_id);
        return try self.arena.dupe([32]u8, selected);
    }

    fn pagedText(self: *Execution, a: A, input: types.SearchRequest, text: types.TextQuery) anyerror!?types.SearchResult {
        var req = input;
        // Public limit=0 requests also ask for an exact count. Do not turn a
        // source transform's candidate pool into a count-only execution.
        if (req.limit == 0 and metadataCountEligible(req)) req.count_only = true;
        if (self.forced_selection != null or !self.typed_delivery or
            req.filter_text != null or req.exclusion_text != null or requiresEarlySource(retrievalRequest(req)) or
            req.filter_prefix.len != 0 or req.filter_ids.len != 0 or req.exclude_ids.len != 0 or
            req.filter_doc_ids.len != 0 or req.filter_doc_ids_positive or req.exclude_doc_ids.len != 0 or
            req.resolved_doc_filter != null or req.resolved_text_doc_filter != null or req.native_key_predicate != null or
            req.graph_queries.len != 0 or req.graph_metric_queries.len != 0 or req.graph_metric_rerank != null or
            req.aggregations_json.len != 0 or
            (!req.count_only and req.limit == 0)) return null;
        switch (text) {
            .term, .match, .phrase, .bool_query, .prefix, .wildcard, .regexp, .fuzzy => {},
            else => return null,
        }
        if (self.overlay != null and req.distributed_text_stats.len != 0) return null;
        var indexes: usize = 0;
        for (self.declarations) |declaration| if (declaration.artifact.kind == .text_segment and corpus.supportsBuildMetadata(declaration.artifact.metadata_version)) {
            indexes += 1;
        };
        const selected = for (self.declarations) |declaration| {
            if (declaration.artifact.kind == .text_segment and corpus.supportsBuildMetadata(declaration.artifact.metadata_version) and
                (if (req.index_name) |name| std.mem.eql(u8, declaration.name, name) else indexes == 1 or std.mem.eql(u8, declaration.name, local.common_full_text_index_defaults.default_full_text_index_name))) break declaration;
        } else return null;
        try self.bindIndex(selected.name);
        const cached: @import("lake_index_aggregate_artifact.zig").CachedRead = .{ .cache = &self.server.lake_read_cache, .scope = self.store.identity, .context = self.context, .metrics = &self.server.lake_query_metrics };
        const cancellation: @import("antfly_cancellation").CancellationToken = .{ .ptr = self, .is_cancelled_fn = canceled };
        const metadata = try @import("lake_index_decoded_metadata.zig").acquire(corpus.Directory, cached, self.store.artifactStore(), selected.artifact, cancellation, corpus.loadDirectory);
        defer metadata.release();
        const root = metadata.value.root;
        if (root.inventory_version != 3 or root.collection == null or root.statistics_version < 1 or root.term_statistics == null) return null;
        const window_config = if (self.server.cfg.node_config) |config| config.lake_indexes.text_cache else (local.common_config.Config.LakeIndexConfig{}).text_cache;
        if (!std.mem.eql(u8, &root.domain, &self.domain) or !@import("../serverless/build/lake_rebuild.zig").bindingsEqual(root.binding, selected.binding)) return error.InvalidNativeLakeTextCorpus;
        var plan_budget: local.sql_memory_budget = .{ .backing = a, .limit = 1024 * 1024 };
        var plan = std.heap.ArenaAllocator.init(plan_budget.allocator());
        defer plan.deinit();
        var schema_ = try local.schema_mod.parseValidatedTableSchema(self.arena, self.schema_json);
        defer schema_.deinit(self.arena);
        const runtime = try local.schema_mod.deriveRuntimeTableSchema(self.arena, schema_);
        if (req.count_only and metadataCountEligible(req) and self.overlay == null) {
            const analysis = try local.storage_db_catalog_index_manager.parseTextAnalysisForIndexConfig(plan.allocator(), root.config_json, runtime);
            const prepared_count = @import("lake_index_text_frontier.zig").prepareObserved(std.heap.page_allocator, root, self.store.artifactStore(), cached, cancellation, text, analysis, runtime, &self.server.lake_query_metrics) catch |err| switch (err) {
                error.OutOfMemory, error.ArtifactReadBudgetExceeded, error.NativeLakeTermDirectoryKeyTooLarge => null,
                else => return err,
            };
            if (prepared_count) |prepared| {
                defer prepared.deinit();
                if (prepared.plan.?.exactMetadataCount()) |count| {
                    var result: types.SearchResult = .{ .alloc = a, .hits = &.{}, .total_hits = count, .total_hits_relation = .exact };
                    if (req.profile) try result.setOwnedSortProfile(.{ .selection_reason = "native_text_metadata_count" });
                    return result;
                }
            }
        }
        const planning_bytes = if (root.serving_metadata_bytes) |bytes|
            corpus.source_spans.planningBytes(root.serving_bytes, bytes)
        else
            std.math.maxInt(u64);
        // Selective indexed metadata leads admission even for a common text
        // term. A one-row filter must not open every archive pack just because
        // its result count is smaller than K. Recent WAL rows remain separate.
        var ordering: ?local.storage_db_query_search_exec.SearchHitOrder = if (req.order_by.len != 0 or req.search_after.len != 0 or req.search_before.len != 0)
            try .init(a, req, runtime)
        else
            null;
        defer if (ordering) |*order| order.deinit(a);
        const pack_proof_mod = @import("lake_index_pack_proof.zig");
        var pack_proof: ?*pack_proof_mod = null;
        defer if (pack_proof) |proof| proof.deinit();
        const pack_probe_mod = @import("lake_index_pack_probe.zig");
        var pack_probe: ?*pack_probe_mod = null;
        defer if (pack_probe) |probe| probe.deinit();
        const stream_predicate = req.filter_query_json.len != 0 and scoreFrontierEligible(req, if (ordering) |*order| order else null);
        if (stream_predicate and !self.relocated()) {
            const resolver: @import("lake_index_text_predicate.zig").PhysicalResolver = .{ .server = self.server, .table = self.table, .source = self.source, .context = self.request, .store = self.store.artifactStore(), .store_identity = self.store.identity, .read_context = self.context, .pinned = .{ .artifacts = self.store.artifactStore(), .store_identity = self.store.identity, .domain = self.domain, .declarations = self.declarations, .read_context = self.context } };
            pack_probe = pack_probe_mod.create(root, cached, resolver, req.filter_query_json, cancellation) catch |err| switch (err) {
                error.OutOfMemory, error.ArtifactReadBudgetExceeded, error.QueryCandidateBudgetExceeded => null,
                else => return err,
            };
            if (pack_probe == null) pack_proof = pack_proof_mod.create(root, self.store.artifactStore(), cached, resolver, req.filter_query_json, cancellation) catch |err| switch (err) {
                error.OutOfMemory, error.ArtifactReadBudgetExceeded, error.QueryCandidateBudgetExceeded, error.SqlProgramLimitExceeded => null,
                else => return err,
            };
        }
        const predicate_selection = if ((!stream_predicate or self.relocated()) and req.filter_query_json.len != 0)
            self.predicatePacks(root, cached, cancellation, req.filter_query_json) catch |err| switch (err) {
                error.OutOfMemory, error.ArtifactReadBudgetExceeded, error.QueryCandidateBudgetExceeded => null,
                else => return err,
            }
        else
            null;
        if (root.segment_count == 0 or (pack_proof == null and pack_probe == null and predicate_selection == null and root.segment_count <= window_config.admission_window_segments and planning_bytes <= window_config.admission_window_bytes)) return null;
        if (ordering) |order| {
            // An ordered row index can stop after the winning prefix. Retain
            // that global seek instead of scanning every window's doc values.
            _ = plan.reset(.retain_capacity);
            const scan = self.orderedTextScan(plan.allocator(), order.effective.req) catch |err| switch (err) {
                error.OutOfMemory => return null,
                else => return err,
            };
            if (scan) |candidate| {
                const compatible = @import("lake_index_sql_rows.zig").hasOrderedPredicatePlan(plan.allocator(), self.table, candidate) catch |err| switch (err) {
                    error.OutOfMemory => return null,
                    else => return err,
                };
                if (compatible and self.overlay == null and !req.count_only and !req.evaluation_matches and root.source_routes_version == 1) {
                    const ordered = self.orderedPagedText(a, order.effective.req, text, root, selected.name, cached, cancellation, order, candidate) catch |err| switch (err) {
                        error.OutOfMemory, error.NativeLakeTextCacheBusy, error.ArtifactReadBudgetExceeded => null,
                        else => return err,
                    };
                    if (ordered) |result| return result;
                }
            }
        }
        // Highlight planning needs analysis/schema, not an admitted corpus.
        // Keep that small metadata in the request arena so final hydration never
        // reacquires every segment after the bounded search has completed.
        if (req.highlight != null and self.highlight_queries == null) {
            const config = try std.json.parseFromSliceLeaky(std.json.Value, self.arena, root.config_json, .{ .allocate = .alloc_always });
            const field: ?[]const u8 = if (config == .object) if (config.object.get("field")) |value| if (value == .string) value.string else null else null else null;
            const queries = try self.arena.alloc(search.HighlightQuery, 1);
            queries[0] = .{ .query = text, .text_analysis = try local.storage_db_catalog_index_manager.parseTextAnalysisForIndexConfig(self.arena, root.config_json, runtime), .runtime_schema = runtime, .selected_field = field };
            self.highlight_queries = queries;
        }
        var recent: ?search.PinnedTextSource = null;
        defer if (recent) |*pin| pin.deinit();
        if (self.overlay) |overlay| {
            const config = try std.json.parseFromSliceLeaky(std.json.Value, self.arena, root.config_json, .{});
            const field: ?[]const u8 = if (config == .object) if (config.object.get("field")) |value| if (value == .string) value.string else null else null else null;
            const analysis = try local.storage_db_catalog_index_manager.parseTextAnalysisForIndexConfig(self.arena, root.config_json, runtime);
            var pin = try self.server.lake_recent_text.acquireFamily(@import("lake_recent_text_cache.zig").Cache.key(self.store.identity, self.table.id, if (self.table.external_indexes) |indexes_| indexes_.object_generation else 0, self.source.inventory.source_id, self.source.inventory.snapshot_id, self.schema_json, root.config_json, selected.name, overlay), @import("lake_recent_text_cache.zig").Cache.family(self.store.identity, self.table.id, if (self.table.external_indexes) |indexes_| indexes_.object_generation else 0, self.source.inventory.source_id, self.source.inventory.snapshot_id, self.schema_json, root.config_json, selected.name), overlay, selected.name, analysis, runtime, field, self.context);
            errdefer pin.deinit();
            recent = try pin.share(std.heap.page_allocator);
        }
        const frontier_mod = @import("lake_index_text_frontier.zig");
        var frontier: ?*frontier_mod = null;
        if (scoreFrontierEligible(req, if (ordering) |*order| order else null)) {
            _ = plan.reset(.retain_capacity);
            const analysis = try local.storage_db_catalog_index_manager.parseTextAnalysisForIndexConfig(plan.allocator(), root.config_json, runtime);
            // Analysis is retained in plan until the frontier is destroyed.
            frontier = (if (recent) |pin|
                frontier_mod.createRecentObserved(std.heap.page_allocator, root, self.store.artifactStore(), cached, cancellation, text, analysis, runtime, &self.server.lake_query_metrics, pin.snapshot)
            else
                frontier_mod.createObserved(std.heap.page_allocator, root, self.store.artifactStore(), cached, cancellation, text, analysis, runtime, &self.server.lake_query_metrics)) catch |err| switch (err) {
                error.OutOfMemory, error.ArtifactReadBudgetExceeded, error.NativeLakeTermDirectoryKeyTooLarge => null,
                else => return err,
            };
        }
        defer if (frontier) |value| value.deinit();
        if (frontier) |value| {
            if (pack_probe) |probe| value.predicate_probe = .{ .ptr = probe, .allows = pack_probe_mod.allows };
            // Optional admission must not make a valid query fail on its own quota.
            // Rebuild ordinary navigation if proof attachment exhausts the frontier.
            if (pack_proof) |proof| {
                value.restrictProof(proof) catch |err| switch (err) {
                    error.OutOfMemory, error.ArtifactReadBudgetExceeded => {
                        value.deinit();
                        frontier = null;
                    },
                    else => return err,
                };
            } else if (predicate_selection) |ids_| try value.restrict(ids_);
        }
        var preparation: ?*frontier_mod = null;
        if (frontier == null and req.distributed_text_stats.len == 0) {
            _ = plan.reset(.retain_capacity);
            const analysis = try local.storage_db_catalog_index_manager.parseTextAnalysisForIndexConfig(plan.allocator(), root.config_json, runtime);
            preparation = (if (recent) |pin|
                frontier_mod.prepareRecentObserved(std.heap.page_allocator, root, self.store.artifactStore(), cached, cancellation, text, analysis, runtime, &self.server.lake_query_metrics, pin.snapshot)
            else
                frontier_mod.prepareObserved(std.heap.page_allocator, root, self.store.artifactStore(), cached, cancellation, text, analysis, runtime, &self.server.lake_query_metrics)) catch |err| switch (err) {
                error.OutOfMemory, error.ArtifactReadBudgetExceeded, error.NativeLakeTermDirectoryKeyTooLarge => null,
                else => return err,
            };
        }
        defer if (preparation) |value| value.deinit();
        if (self.overlay) |overlay| {
            // Unsupported prepared syntax retains the independent complete
            // corpus path; window-local frequencies are never a substitute.
            if (frontier == null and preparation == null) return null;
            _ = overlay;
            try self.resolveOverlayPhysical();
        }

        var reader: ?*@import("lake_index_text_directory.zig").RoutingCursor = null;
        var selected_reader: ?*@import("lake_index_text_directory.zig").SelectedCursor = null;
        defer if (selected_reader) |value| value.deinit();
        if (frontier == null) {
            var selection_scratch = std.heap.ArenaAllocator.init(plan_budget.allocator());
            defer selection_scratch.deinit();
            var planning = self.*;
            planning.arena = selection_scratch.allocator();
            const subset = predicate_selection orelse (if (self.overlay != null) null else planning.selectFiles(text, root, cached, cancellation) catch |err| switch (err) {
                error.OutOfMemory => null,
                else => return err,
            });
            if (subset) |selected_ids| {
                const owned_ids = try self.arena.dupe([32]u8, selected_ids);
                selected_reader = try @import("lake_index_text_directory.zig").SelectedCursor.create(std.heap.page_allocator, root, self.store.artifactStore(), cached, cancellation, owned_ids);
            } else reader = try @import("lake_index_text_directory.zig").RoutingCursor.create(std.heap.page_allocator, root, self.store.artifactStore(), cached, cancellation);
        }
        defer if (reader) |value| value.deinit();
        const paging = local.storage_db_query_control_contract.componentPaging(req);
        var winners = PagedWinners.initOrdered(a, if (req.count_only) 0 else @as(usize, paging.offset) + paging.limit, if (ordering) |*order| order else null);
        defer winners.deinit();
        var total: u32 = 0;
        var relation: types.TotalHitsRelation = .exact;
        var profiled: types.SearchResult = .{ .alloc = a, .hits = &.{}, .total_hits = 0 };
        defer profiled.deinit();
        var ids: [64][32]u8 = undefined;
        var batch_limit: usize = 1;
        const Navigation = struct {
            frontier: ?*frontier_mod,
            reader: ?*@import("lake_index_text_directory.zig").RoutingCursor,
            selected: ?*@import("lake_index_text_directory.zig").SelectedCursor,
            max_bytes: u64,
            execution: *Execution,
            root: corpus.Root,
            declaration: local.serverless_segment_sidecar_manifest.DeclaredArtifact,
            cached: @import("lake_index_aggregate_artifact.zig").CachedRead,
            token: @import("antfly_cancellation").CancellationToken,
            fn prepare(raw: *anyopaque, selected_ids: []const [32]u8) !?search.PinnedTextSource {
                const self_: *@This() = @ptrCast(@alignCast(raw));
                const owner = self_.execution;
                try owner.context.ensureActive();
                return try owner.server.lake_text_corpora.acquireSelected(owner.server.embedding_provider_runtime.io, owner.store.artifactStore(), self_.declaration.artifact, self_.root, owner.schema_json, self_.cached, owner.context, self_.token, selected_ids);
            }
            fn next(raw: *anyopaque, output: [][32]u8, cutoff: ?f32) !?@import("lake_index_text_directory.zig").RoutingCursor.Window {
                return nextRanked(raw, output, cutoff, null);
            }
            fn nextRanked(raw: *anyopaque, output: [][32]u8, cutoff: ?f32, position: ?@import("lake_index_text_directory.zig").Position) !?@import("lake_index_text_directory.zig").RoutingCursor.Window {
                const self_: *@This() = @ptrCast(@alignCast(raw));
                if (self_.frontier) |value| {
                    const selected_ids = (try value.nextWindowRanked(output, cutoff, position, self_.max_bytes)) orelse return null;
                    return .{ .identities = selected_ids, .doc_offset = 0 };
                }
                if (self_.selected) |value| return value.nextBounded(output, self_.max_bytes);
                return self_.reader.?.nextBounded(output, self_.max_bytes);
            }
        };
        var navigation: Navigation = .{ .frontier = frontier, .reader = reader, .selected = selected_reader, .max_bytes = window_config.admission_window_bytes, .execution = self, .root = root, .declaration = selected, .cached = cached, .token = cancellation };
        var ahead = @import("lake_text_window_ahead.zig").init(.{ .ptr = &navigation, .next = Navigation.next, .next_ranked = Navigation.nextRanked, .prepare = Navigation.prepare }, self.context.io);
        defer ahead.deinit();
        // Reuse the region even when the caller is itself an arena: freeing a
        // fresh child region per window would retain every window in its parent.
        var scratch = std.heap.ArenaAllocator.init(a);
        defer scratch.deinit();
        var recent_scored = false;
        while (true) {
            try self.context.ensureActive();
            _ = scratch.reset(.retain_capacity);
            const wa = scratch.allocator();
            // Full-match evaluation must establish complete membership before
            // evaluating any source; a competitive top-K cannot prove it.
            const competitive_cutoff = if (req.evaluation_matches) null else winners.cutoff();
            // Recent rows seed the competitive cutoff, but retain their logical
            // position after all archive rows when equal scores are compared.
            const tie_cutoff = if (req.evaluation_matches) null else winners.tieCutoff();
            const recent_window = recent != null and !recent_scored;
            const pulled = if (recent_window) null else try ahead.nextRanked(if (frontier != null) batch_limit else window_config.admission_window_segments, competitive_cutoff, tie_cutoff);
            if (pulled == null and !recent_window) break;
            var prepared = if (!recent_window) ahead.takePrepared() else null;
            defer if (prepared) |*pin| pin.deinit();
            var window: @import("lake_index_text_directory.zig").RoutingCursor.Window = .{ .identities = &.{}, .doc_offset = root.ordinalDocuments() };
            if (pulled) |value| {
                // Copy worker-owned output before the single lookahead reuses it.
                @memcpy(ids[0..value.identities.len], value.identities);
                window = .{ .identities = ids[0..value.identities.len], .doc_offset = value.doc_offset };
                ahead.prefetchRanked(if (frontier != null) batch_limit else window_config.admission_window_segments, competitive_cutoff, tie_cutoff);
            } else {
                recent_scored = true;
                ahead.prefetchRanked(if (frontier != null) batch_limit else window_config.admission_window_segments, competitive_cutoff, tie_cutoff);
            }
            var child: Execution = .{ .server = self.server, .table = self.table, .source = self.source, .store = self.store, .domain = self.domain, .declarations = self.declarations, .context = self.context, .request = self.request, .schema_json = self.schema_json, .arena = wa, .result_allocator = wa, .navigation = self.navigation, .files = self.files, .private_files = self.private_files, .private_digests = self.private_digests, .typed_delivery = true, .forced_selection = window.identities, .prefetched_archive = if (prepared != null) &prepared.? else null, .overlay = self.overlay, .overlay_preparation = if (self.overlay != null) frontier orelse preparation else null, .recent_text_source = if (recent != null) &recent.? else null, .recent_text_window = recent_window };
            defer child.deinit();
            var request = retrievalRequest(req);
            request.offset = 0;
            request.limit = if (req.count_only) req.limit else std.math.cast(u32, winners.limit) orelse return error.QueryCandidateBudgetExceeded;
            request.include_stored = false;
            request.highlight = null;
            request.text_navigation_plan = if (frontier orelse preparation) |value| value.plan else null;
            if (scoreFrontierEligible(req, if (ordering) |*order| order else null)) request.minimum_competitive_score = competitive_cutoff;
            if (!req.count_only and !req.evaluation_matches and winners.limit != 0 and winners.heap.items.len == winners.limit) {
                if (ordering) |*order| if (order.effective.req.order_by.len > 1 and
                    std.mem.eql(u8, order.effective.req.order_by[order.effective.req.order_by.len - 1].field, "_id"))
                {
                    request.competitive_sort_boundary = winners.heap.peek().?.hit.sort_values;
                };
            }
            const previous_cutoff = winners.cutoff();
            var page = postings: {
                const posting_started = @import("antfly_platform").time.monotonicNs();
                defer self.server.lake_query_metrics.record(.text_postings, posting_started);
                break :postings try searchText(&child, wa, request, text);
            };
            defer page.deinit();
            if (req.evaluation_matches and page.total_hits_relation != .exact) {
                // Local WAND may report a lower bound even when this window's
                // retained hits cover every match. Count under the same masks
                // and leased window instead of admitting the full inventory.
                var count_request = request;
                count_request.count_only = true;
                count_request.limit = 0;
                count_request.minimum_competitive_score = null;
                var counted = try searchText(&child, wa, count_request, text);
                defer counted.deinit();
                page.total_hits = counted.total_hits;
                page.total_hits_relation = counted.total_hits_relation;
            }
            if (req.evaluation_matches and (page.total_hits_relation != .exact or
                @as(u64, total) + page.total_hits > req.evaluation_limit)) return error.DecisionLimitExceeded;
            if (req.profile) if (page.sort_profile) |profile| {
                if (profiled.sort_profile == null) {
                    try profiled.setOwnedSortProfile(profile);
                } else {
                    mergeWindowProfile(&profiled.sort_profile.?, profile);
                }
            };
            if (page.total_hits_relation == .gte or page.total_hits > std.math.maxInt(u32) - total) relation = .gte;
            total +|= page.total_hits;
            for (page.hits) |hit| {
                const ordinal = std.math.add(u64, window.doc_offset, hit.native_text_doc_id orelse return error.InvalidNativeLakeTextCorpus) catch return error.InvalidNativeLakeTextCorpus;
                const location = if (self.use_stored_highlights) try storedHighlightLocation(child.delivery_pin orelse return error.InvalidNativeLakeTextCorpus, window.identities, hit.native_text_doc_id.?) else null;
                const identities: ?*const @import("lake_index_text_predicate.zig").Identities = if (ordering == null and !recent_window) archive: {
                    const pin = child.delivery_pin orelse return error.InvalidNativeLakeTextCorpus;
                    break :archive @ptrCast(@alignCast(pin.provider_metadata orelse return error.InvalidNativeLakeTextCorpus));
                } else null;
                if (if (self.relocated()) self.index_resolver else null) |resolver| {
                    if (recent_window) {
                        const rank = std.math.add(u64, resolver.rows, ordinal - root.ordinalDocuments()) catch return error.InvalidSearchTieRank;
                        try winners.addLocated(hit, rank, location);
                        continue;
                    }
                    const key = try publicKey(self, wa, hit.id);
                    const position = try @import("lake_index_native_state.zig").coordinates(key);
                    const file = (try self.fileFor(try local.storage_rowsource_identity.publicFile(key), true)) orelse return error.ExternalLakeSnapshotMismatch;
                    const current: local.storage_rowsource_types.RowRef = .{ .external = .{ .source_id = self.source.inventory.source_id, .snapshot_id = self.source.inventory.snapshot_id, .file_id = file, .row_group_ordinal = position.group, .row_ordinal = position.row } };
                    const original = try self.toIndex(wa, current);
                    try winners.addLocated(hit, try resolver.tie_rank(resolver.ptr, wa, original), location);
                } else try winners.addWindowHit(hit, ordinal, identities, recent_window, location);
            }
            // Raise cutoff promptly while winners improve; amortize setup when
            // weak/equal bounds keep admitting segments without improving K.
            if (frontier != null) batch_limit = if (req.evaluation_matches or (winners.cutoff() != null and winners.cutoff() == previous_cutoff)) @min(window_config.admission_window_segments, batch_limit * 2) else 1;
        }
        if (frontier) |value| if (value.pruned) {
            relation = .gte;
        };
        const cursor_started = @import("antfly_platform").time.monotonicNs();
        defer self.server.lake_query_metrics.record(.text_cursor, cursor_started);
        var result: types.SearchResult = .{ .alloc = a, .hits = try winners.finishLocated(paging.offset, if (self.use_stored_highlights) &self.highlight_locations else null, self.arena), .total_hits = total, .total_hits_relation = relation, .ordered_window_complete = ordering != null };
        if (req.profile) {
            result.sort_profile = profiled.sort_profile orelse .{};
            result.sort_profile_storage = profiled.sort_profile_storage;
            profiled.sort_profile = null;
            profiled.sort_profile_storage = null;
            result.sort_profile.?.selection_reason = if (predicate_selection != null or (frontier != null and (pack_proof != null or pack_probe != null))) "indexed_predicate_text_admission" else if (frontier != null) "bounded_global_text_frontier" else "bounded_text_routing_windows";
            result.sort_profile.?.selected_count = result.hits.len;
            result.sort_profile.?.window_capacity = winners.limit;
            result.sort_profile.?.collector_heap_peak = winners.peak;
        }
        return result;
    }
    /// Stream the ordered row prefix, opening only its physical serving packs.
    /// A skewed prefix abandons this optional plan and returns to bounded text
    /// windows. Full tuple order proves when later rows cannot change top-K.
    /// Query-local membership is independent of each ordered candidate page.
    /// Only immutable bitmap data survives a child execution; source pins and
    /// provider callbacks never enter this memo. Four entries share 8 MiB.
    const OrderedMembership = struct {
        const Bitmap = local.encoding_roaring.RoaringBitmap;
        const Patterns = local.search_query.PatternMembership;
        const Entry = struct { id: [32]u8, bitmap: ?Bitmap = null, patterns: *Patterns, used: u64 };
        budget: local.sql_memory_budget = .{ .backing = @import("antfly_platform").allocator.processAllocator(std.heap.page_allocator), .limit = 8 * 1024 * 1024 },
        entries: [4]?Entry = @splat(null),
        tick: u64 = 0,
        hits: usize = 0,
        builds: usize = 0,
        fn evict(self: *@This()) bool {
            return self.evictExcept(null);
        }
        fn evictExcept(self: *@This(), skip: ?usize) bool {
            var oldest: ?usize = null;
            for (self.entries, 0..) |entry, i| if (entry) |held| {
                if (skip == i) continue;
                if (oldest == null or held.used < self.entries[oldest.?].?.used) oldest = i;
            };
            const index = oldest orelse return false;
            if (self.entries[index].?.bitmap) |*bitmap| bitmap.deinit();
            self.entries[index].?.patterns.deinit();
            self.budget.allocator().destroy(self.entries[index].?.patterns);
            self.entries[index] = null;
            return true;
        }
        fn deinit(self: *@This()) void {
            while (self.evict()) {}
        }
        fn build(self: *@This(), snapshot: *const local.index.IndexSnapshot, plan: *const local.search_search.TextNavigationPlan, token: @import("antfly_cancellation").CancellationToken, patterns: *Patterns) !Bitmap {
            const a = self.budget.allocator();
            var result = Bitmap.init(a);
            errdefer result.deinit();
            var offset: u32 = 0;
            for (snapshot.segments, 0..) |_, i| {
                try token.check();
                var lease = try snapshot.leaseSegment(i);
                defer lease.deinit();
                const segment = lease.entry;
                var matches = try plan.membership.withControl(.{ .cancellation = token, .pattern_cache = patterns }).executeWithOffset(a, segment, offset);
                defer matches.deinit();
                {
                    segment.shared.lockDeletionShared();
                    defer segment.shared.unlockDeletionShared();
                    if (segment.shared.deleted) |deleted| matches.andNotWith(&deleted);
                }
                var shifted = try matches.addOffset(offset);
                defer shifted.deinit();
                try result.orWith(&shifted);
                offset = try std.math.add(u32, offset, segment.reader.doc_count);
            }
            return result;
        }
        fn get(self: *@This(), id: [32]u8, snapshot: *const local.index.IndexSnapshot, plan: *const local.search_search.TextNavigationPlan, token: @import("antfly_cancellation").CancellationToken) !?*const Bitmap {
            try token.check();
            self.tick +|= 1;
            for (&self.entries) |*entry| if (entry.*) |*held| if (std.mem.eql(u8, &held.id, &id)) {
                held.used = self.tick;
                self.hits += 1;
                return if (held.bitmap) |*bitmap| bitmap else null;
            };
            while (true) {
                var free: ?usize = null;
                for (self.entries, 0..) |entry, i| if (entry == null) {
                    free = i;
                    break;
                };
                if (free == null) {
                    _ = self.evict();
                    continue;
                }
                const patterns = self.budget.allocator().create(Patterns) catch {
                    if (!self.evict()) return null;
                    continue;
                };
                patterns.* = Patterns.init(self.budget.allocator(), 8 * 1024 * 1024);
                const index = free.?;
                self.entries[index] = .{ .id = id, .patterns = patterns, .used = self.tick };
                while (true) {
                    const bitmap = self.build(snapshot, plan, token, patterns) catch |err| {
                        if (err != error.OutOfMemory) return err;
                        if (!self.evictExcept(index)) return null;
                        continue;
                    };
                    self.entries[index].?.bitmap = bitmap;
                    self.builds += 1;
                    return &self.entries[index].?.bitmap.?;
                }
            }
        }
        fn patternCache(self: *@This(), id: [32]u8) ?*Patterns {
            for (self.entries) |entry| if (entry) |held| if (std.mem.eql(u8, &held.id, &id)) return held.patterns;
            return null;
        }
    };
    fn orderedPagedText(self: *Execution, a: A, req: types.SearchRequest, text: types.TextQuery, root: corpus.Root, name: []const u8, cached: @import("lake_index_aggregate_artifact.zig").CachedRead, token: @import("antfly_cancellation").CancellationToken, ordering: local.storage_db_query_search_exec.SearchHitOrder, scan: local.sql_catalog.Scan) !?types.SearchResult {
        var predicate = (try @import("lake_index_sql_rows.zig").tryOpenOrderedPredicate(a, self.server, self.table, scan, self.request, self.source, req.search_before.len != 0, .{ .values = if (req.search_before.len != 0) req.search_before else req.search_after, .id_descending = req.order_by[req.order_by.len - 1].desc })) orelse return null;
        defer predicate.deinit();
        if (!predicate.complete_order) return null;
        var preparation_arena = std.heap.ArenaAllocator.init(a);
        defer preparation_arena.deinit();
        const pa = preparation_arena.allocator();
        var parsed_schema = try local.schema_mod.parseValidatedTableSchema(pa, self.schema_json);
        defer parsed_schema.deinit(pa);
        const runtime = try local.schema_mod.deriveRuntimeTableSchema(pa, parsed_schema);
        const analysis = try local.storage_db_catalog_index_manager.parseTextAnalysisForIndexConfig(pa, root.config_json, runtime);
        const prepared = if (req.distributed_text_stats.len != 0) null else @import("lake_index_text_frontier.zig").prepareObserved(std.heap.page_allocator, root, self.store.artifactStore(), cached, token, text, analysis, runtime, &self.server.lake_query_metrics) catch |err| switch (err) {
            error.OutOfMemory, error.ArtifactReadBudgetExceeded, error.NativeLakeTermDirectoryKeyTooLarge => null,
            else => return err,
        };
        defer if (prepared) |held| held.deinit();
        var membership: OrderedMembership = .{};
        defer membership.deinit();
        const paging = local.storage_db_query_control_contract.componentPaging(req);
        const target = @as(usize, paging.offset) + paging.limit;
        var order = ordering;
        var winners = PagedWinners.initOrdered(a, target, &order);
        defer winners.deinit();
        var budget: local.sql_memory_budget = .{ .backing = a, .limit = 16 * 1024 * 1024 };
        var scratch = std.heap.ArenaAllocator.init(budget.allocator());
        defer scratch.deinit();
        var live: @import("lake_index_text_predicate.zig").LiveRowsCache = .{ .backing = budget.allocator() };
        defer live.deinit(budget.allocator());
        const tree = @import("../serverless/graph_segment/page_tree.zig");
        var store = self.store.artifactStore();
        var reads: u64 = 64 * 1024 * 1024;
        var writes: u64 = 0;
        var pages: @import("../serverless/graph_segment/page_store.zig").PageStore = .{ .domain = root.domain, .artifacts = &store, .cancellation = token, .remaining_read_bytes = &reads, .remaining_write_bytes = &writes };
        var page_cache: tree.Cache = .{ .alloc = budget.allocator(), .underlying = pages.store() };
        defer page_cache.deinit();
        var scanned: usize = 0;
        var total: u32 = 0;
        var complete = false;
        var exact = req.search_after.len == 0 and req.search_before.len == 0;
        const probe_limit = @max(1024, target *| 4);
        while (scanned < probe_limit) {
            try self.context.ensureActive();
            _ = scratch.reset(.retain_capacity);
            const sa = scratch.allocator();
            const refs = try predicate.next(sa, @min(@max(16, @min(256, target + 1)), probe_limit - scanned));
            if (refs.len == 0) {
                complete = true;
                break;
            }
            scanned += refs.len;
            var selected: std.AutoHashMapUnmanaged([32]u8, std.ArrayList(local.storage_rowsource_types.RowRef)) = .empty;
            for (refs) |ref| if (try @import("lake_index_source_routes.zig").lookup(sa, page_cache.store(), root, store, cached, token, &live, try self.toIndex(sa, ref))) |id| {
                const entry = try selected.getOrPut(sa, id);
                if (!entry.found_existing) entry.value_ptr.* = .empty;
                try entry.value_ptr.append(sa, ref);
            };
            var child_arena = std.heap.ArenaAllocator.init(a);
            defer child_arena.deinit();
            var segments = selected.iterator();
            while (segments.next()) |entry| {
                _ = child_arena.reset(.retain_capacity);
                const ca = child_arena.allocator();
                var child: Execution = .{ .server = self.server, .table = self.table, .source = self.source, .store = self.store, .domain = self.domain, .declarations = self.declarations, .context = self.context, .request = self.request, .schema_json = self.schema_json, .arena = ca, .result_allocator = ca, .navigation = self.navigation, .files = self.files, .private_files = self.private_files, .private_digests = self.private_digests, .typed_delivery = true, .forced_selection = &.{entry.key_ptr.*}, .candidate_rows = entry.value_ptr.items, .ordered_membership = if (prepared != null) &membership else null };
                defer child.deinit();
                var request = retrievalRequest(req);
                request.index_name = name;
                request.text_navigation_plan = if (prepared) |held| held.plan else null;
                request.offset = 0;
                request.limit = std.math.cast(u32, target) orelse return error.QueryCandidateBudgetExceeded;
                request.include_stored = false;
                request.highlight = null;
                var page = try searchText(&child, ca, request, text);
                defer page.deinit();
                total +|= page.total_hits;
                exact = exact and page.total_hits_relation == .exact;
                for (page.hits) |hit| {
                    const location = if (self.use_stored_highlights) try storedHighlightLocation(child.delivery_pin.?, &.{entry.key_ptr.*}, hit.native_text_doc_id.?) else null;
                    try winners.addLocated(hit, 0, location);
                }
            }
            if (winners.heap.items.len == target) break;
        }
        if (!complete and winners.heap.items.len < target) return null;
        var result: types.SearchResult = .{ .alloc = a, .hits = &.{}, .total_hits = total, .total_hits_relation = if (complete and exact) .exact else .gte, .ordered_window_complete = true };
        errdefer result.deinit();
        if (req.profile) try result.setOwnedSortProfile(.{ .selection_reason = "bounded_ordered_lake_index", .candidate_source = "ordered_lake_index", .ordered_scanned_count = scanned, .selected_count = winners.heap.items.len -| paging.offset });
        // Publish borrowed highlight keys only after every other allocation has
        // succeeded; an optional-plan fallback must leave no dangling keys.
        result.hits = try winners.finishLocated(paging.offset, if (self.use_stored_highlights) &self.highlight_locations else null, self.arena);
        return result;
    }
    fn searchText(raw: ?*anyopaque, a: A, req: types.SearchRequest, text: types.TextQuery) !types.SearchResult {
        const self = from(raw);
        if (try self.pagedText(a, req, text)) |result| return result;
        const previous_text = self.active_text;
        self.active_text = text;
        defer self.active_text = previous_text;
        // The engine uses one callback for includes and excludes. Require an
        // exact plan for the exclusion expression; identical include/exclude
        // expressions conservatively share that requirement.
        const previous_exclusion = self.predicate_exclusion_json;
        const previous_allow_partial = self.predicate_allow_partial;
        self.predicate_exclusion_json = req.exclusion_query_json;
        // Sort and cursor execution require the complete predicate to be
        // resolved before ranking/page boundaries, including implicit ID sort.
        self.predicate_allow_partial = req.order_by.len == 0 and req.search_after.len == 0 and req.search_before.len == 0;
        defer {
            self.predicate_exclusion_json = previous_exclusion;
            self.predicate_allow_partial = previous_allow_partial;
        }
        var effective = req;
        var candidate_pin: ?search.PinnedTextSource = null;
        const previous_pin = self.forced_pin;
        defer {
            self.forced_pin = previous_pin;
            if (candidate_pin) |*pin| pin.deinit();
        }
        var native_filter: search.ResolvedTextDocNumFilter = .{};
        var numbers: std.ArrayList(u32) = .empty;
        defer numbers.deinit(a);
        if (self.candidate_rows) |rows| {
            candidate_pin = (try acquireForRanking(raw, req.index_name)) orelse return error.InvalidNativeLakeTextCorpus;
            self.forced_pin = &candidate_pin.?;
            const identities = self.text_identities.get(@intFromPtr(candidate_pin.?.snapshot)) orelse return error.InvalidNativeLakeTextCorpus;
            var live: @import("lake_index_text_predicate.zig").LiveRowsCache = .{ .backing = a };
            defer live.deinit(a);
            const cached: @import("lake_index_aggregate_artifact.zig").CachedRead = .{ .cache = &self.server.lake_read_cache, .scope = self.store.identity, .context = self.context, .metrics = &self.server.lake_query_metrics };
            const membership = if (self.ordered_membership) |memo| if (req.text_navigation_plan) |plan| try memo.get(self.forced_selection.?[0], candidate_pin.?.snapshot, plan, .{ .ptr = self, .is_cancelled_fn = canceled }) else null else null;
            if (self.ordered_membership) |memo| effective.text_pattern_membership = memo.patternCache(self.forced_selection.?[0]);
            for (rows) |row| if (try identities.ordinal(a, &live, self.store.artifactStore(), cached, row)) |number| {
                if (membership == null or membership.?.contains(number)) try numbers.append(a, number);
            };
            std.mem.sort(u32, numbers.items, {}, struct {
                fn less(_: void, left: u32, right: u32) bool {
                    return left < right;
                }
            }.less);
            native_filter.include = if (numbers.items.len == 0) .none else .{ .doc_nums = numbers.items };
            effective.resolved_text_doc_filter = &native_filter;
        }
        return search.searchTextQuery(a, effective, text, .{ .ctx = raw, .exact_doc_id_filters = true, .acquire_text_source = acquireForRanking, .resolve_indexed_filter = resolveIndexedFilter, .open_ordered_candidates = openOrderedTextCandidates, .native_count_visibility_exact = true, .project_key = publicKey, .project_doc_num = publicDocNum, .native_key = nativeKey, .filter_candidate_presence = true, .text_index_entry = noLocal, .text_index_is_chunk_backed = chunkBacked, .search_match_all = matchAll, .project_stored_search = project, .load_stored = loadOne, .load_projected_documents = loadProjected, .postprocess = postprocess });
    }
    fn dispatchText(raw: ?*anyopaque, a: A, req: types.SearchRequest) !types.SearchResult {
        return search.searchText(a, req, .{ .ctx = raw, .func = searchText });
    }
    fn denseIndex(raw: ?*anyopaque, name: ?[]const u8) !?*local.storage_db_catalog_index_manager.IndexManager.DenseIndex {
        const self = from(raw);
        try self.context.ensureActive();
        const native = @import("lake_index_native_dense.zig");
        const declarations = if (self.active_recent) if (self.recent_run) |run| run.declarations else self.recent_declarations else self.declarations;
        const entries = if (self.active_recent) &self.recent_dense_entries else &self.dense_entries;
        var count: usize = 0;
        for (declarations) |declaration| if (declaration.artifact.kind == .vector_segment and native.supportsMetadata(declaration.artifact.metadata_version)) {
            count += 1;
        };
        const selected = for (declarations) |declaration| {
            if (declaration.artifact.kind == .vector_segment and native.supportsMetadata(declaration.artifact.metadata_version) and (if (name) |explicit| std.mem.eql(u8, explicit, declaration.name) else count == 1)) break declaration;
        } else return error.IndexNotFound;
        if (!self.active_recent) try self.bindIndex(selected.name);
        if (entries.get(if (self.active_recent) selected.artifact.artifact_id else selected.name)) |entry| {
            for (self.runtimes.items) |runtime| if (runtime.dense_entry == entry) {
                self.unit_runtime = runtime;
                break;
            };
            return entry;
        }
        try self.runtimes.ensureUnusedCapacity(self.arena, 1);
        const runtime = try self.server.lake_native_runtimes.acquire(self.server, selected, try @import("lake_recent_vectors.zig").runtimeDomain(selected), self.store.identity, self.context, if (self.active_recent) if (self.recent_run) |run| run.cloud_generations else self.recent_cloud else self.cloud_generations);
        errdefer runtime.release();
        const entry = runtime.dense_entry.?;
        try entries.put(self.arena, if (self.active_recent) selected.artifact.artifact_id else selected.name, entry);
        self.runtimes.appendAssumeCapacity(runtime);
        self.unit_runtime = runtime;
        return entry;
    }
    fn lookupDocKey(raw: ?*anyopaque, name: []const u8, id: u64) !?[]u8 {
        const self = from(raw);
        const entry = (try denseIndex(raw, name)).?;
        const metadata = (try entry.index.getMetadata(id)) orelse return null;
        defer entry.index.alloc.free(metadata);
        return try publicKey(raw, self.result_allocator, metadata);
    }
    fn lookupVectorId(raw: ?*anyopaque, name: []const u8, key: []const u8) !?u64 {
        const entry = (try denseIndex(raw, name)).?;
        const self = from(raw);
        const private = try nativeKey(raw, self.arena, key);
        const id = @import("lake_index_native_dense.zig").vectorId(private);
        const metadata = (try entry.index.getMetadata(id)) orelse return null;
        defer entry.index.alloc.free(metadata);
        return if (std.mem.eql(u8, metadata, private)) id else null;
    }
    fn denseSearch(_: ?*anyopaque, entry: *local.storage_db_catalog_index_manager.IndexManager.DenseIndex, req: local.storage_hbc_adapter.SearchRequest) !local.storage_hbc_adapter.SearchResults {
        return entry.index.searchWithRequest(req);
    }
    fn denseSearchProfiled(_: ?*anyopaque, entry: *local.storage_db_catalog_index_manager.IndexManager.DenseIndex, req: local.storage_hbc_adapter.SearchRequest) !local.storage_hbc_adapter.ProfiledSearchResults {
        return entry.index.searchProfiledRequest(req);
    }
    fn releaseRecentWindow(self: *Execution, first: usize) void {
        for (self.runtimes.items[first..]) |runtime| runtime.release();
        self.runtimes.items.len = first;
        self.recent_dense_entries.clearRetainingCapacity();
        self.recent_sparse_entries.clearRetainingCapacity();
    }
    // Independent executions own every mutable map, runtime pin and allocator.
    // Results retain scheduler admission until joined; no private pool is used.
    /// Clone immutable request inputs; mutable maps, cursors and pins start
    /// empty in every lane/partition. Source ownership is explicit at the caller.
    fn childExecution(base: *Execution, source: *local.serverless_query_lake_serving.ServingSource, scratch: A, output: A) Execution {
        return .{ .scheduled_component = base.scheduled_component, .server = base.server, .overlay = base.overlay, .wal_cut = base.wal_cut, .table = base.table, .source = source, .store = base.store, .domain = base.domain, .declarations = base.declarations, .context = base.context, .request = base.request, .schema_json = base.schema_json, .hydration_fields = base.hydration_fields, .index_resolver = base.index_resolver, .vector_filter = base.vector_filter, .vector_exclusion = base.vector_exclusion, .recent_runs = base.recent_runs, .recent_declarations = base.recent_declarations, .recent_cloud = base.recent_cloud, .cloud_generations = base.cloud_generations, .typed_delivery = base.typed_delivery, .navigation = base.navigation, .files = base.files, .private_files = base.private_files, .private_digests = base.private_digests, .arena = scratch, .result_allocator = output };
    }
    fn prepareVectorMembership(self: *Execution, a: A, req: types.SearchRequest) !void {
        if (req.filter_query_json.len == 0 and req.exclusion_query_json.len == 0) return;
        const resolver: @import("lake_index_text_predicate.zig").PhysicalResolver = .{ .server = self.server, .table = self.table, .source = self.source, .context = self.request, .store = self.store.artifactStore(), .store_identity = self.store.identity, .read_context = self.context, .pinned = .{ .artifacts = self.store.artifactStore(), .store_identity = self.store.identity, .domain = self.domain, .declarations = self.declarations, .read_context = self.context } };
        if (req.filter_query_json.len != 0) self.vector_include_provider = try resolver.openMembership(a, req.filter_query_json);
        if (req.exclusion_query_json.len != 0) self.vector_exclude_provider = try resolver.openMembership(a, req.exclusion_query_json);
    }
    fn searchComponents(raw: ?*anyopaque, a: A, components: []const local.storage_db_query_search_exec.ComposedComponent) !local.storage_db_query_search_exec.ComposedComponentBatch {
        const self = from(raw);
        const scheduler = local.sql_parallel_scheduler;
        try self.resolveOverlayPhysical();
        // Final source highlighting is coordinator-owned. Component-local
        // stored-highlight locations cannot be shared across private lanes.
        if (components.len != 0) try self.planHighlights(components[0].request);
        self.use_stored_highlights = false;
        var batch: local.storage_db_query_search_exec.ComposedComponentBatch = .{ .alloc = a, .results = try a.alloc(?types.SearchResult, components.len) };
        @memset(batch.results, null);
        errdefer batch.deinit();
        batch.execution_ns = try a.alloc(u64, components.len);
        const Worker = struct {
            budget: local.sql_memory_budget,
            arena: std.heap.ArenaAllocator = undefined,
            execution: Execution = undefined,
            source: local.serverless_query_lake_serving.ServingSource = undefined,
            parent: *local.serverless_query_lake_serving.ServingSource,
            component: local.storage_db_query_search_exec.ComposedComponent,
            elapsed_ns: u64 = 0,
            result: ?types.SearchResult = null,
            task: ?scheduler.Task(anyerror!void) = null,
            failure: ?anyerror = null,
            serial_only: bool = false,
            done: std.Io.Event = .unset,
            fn create(base: *Execution, component: local.storage_db_query_search_exec.ComposedComponent, backing: A, limit: usize) !*@This() {
                const owner = try std.heap.page_allocator.create(@This());
                owner.* = .{ .budget = .{ .backing = backing, .limit = limit }, .serial_only = limit == std.math.maxInt(usize), .parent = base.source, .component = component };
                errdefer std.heap.page_allocator.destroy(owner);
                owner.arena = .init(owner.budget.allocator());
                errdefer owner.arena.deinit();
                owner.source = base.source.parallelView(owner.budget.allocator()) catch |err| {
                    if (err == error.OutOfMemory and owner.budget.isExhausted()) return error.SqlMemoryLimitExceeded;
                    return err;
                };
                owner.execution = base.childExecution(&owner.source, owner.arena.allocator(), owner.budget.allocator());
                owner.execution.scheduled_component = true;
                return owner;
            }
            fn run(owner: *@This()) anyerror!void {
                const started = @import("antfly_platform").time.monotonicNs();
                defer {
                    owner.elapsed_ns = @import("antfly_platform").time.monotonicNs() - started;
                    if (owner.execution.context.io) |io| owner.done.set(io);
                }
                const execution = &owner.execution;
                try execution.context.ensureActive();
                const req = owner.component.request;
                if ((owner.component.query == .dense or owner.component.query == .sparse) and execution.recent_declarations.len == 0)
                    try execution.prepareVectorMembership(owner.budget.allocator(), req);
                owner.result = try switch (owner.component.query) {
                    .text => |text| searchText(execution, owner.budget.allocator(), req, text),
                    .default_text => dispatchText(execution, owner.budget.allocator(), req),
                    .dense => |dense| searchDense(execution, owner.budget.allocator(), req, dense),
                    .sparse => |sparse| searchSparse(execution, owner.budget.allocator(), req, sparse),
                };
            }
            fn close(owner: *@This()) void {
                std.debug.assert(owner.task == null);
                if (owner.result) |*result| result.deinit();
                owner.execution.deinit();
                owner.source.closeParallelView(owner.parent);
                owner.arena.deinit();
                std.debug.assert(owner.budget.live == 0);
                std.heap.page_allocator.destroy(owner);
            }
        };
        var first: usize = 0;
        while (first < components.len) {
            var workers: [3]?*Worker = @splat(null);
            defer for (&workers) |*slot| if (slot.*) |worker| {
                if (worker.task) |task| {
                    var running = task;
                    worker.task = null;
                    _ = running.cancel(self.context.io.?) catch {};
                }
                worker.close();
            };
            const count = @min(3, components.len - first);
            for (components[first..][0..count], 0..) |component, i| {
                const worker = Worker.create(self, component, @import("antfly_platform").allocator.processAllocator(std.heap.page_allocator), 16 * 1024 * 1024) catch |err| switch (err) {
                    error.SqlMemoryLimitExceeded => try Worker.create(self, component, a, std.math.maxInt(usize)),
                    else => return err,
                };
                workers[i] = worker;
                if (!worker.serial_only) if (self.context.io) |io| {
                    worker.task = scheduler.global().submitIo(io, 16 * 1024 * 1024, Worker.run, .{worker});
                };
                if (worker.task == null) Worker.run(worker) catch |err| {
                    if (err != error.OutOfMemory or !worker.budget.isExhausted()) return err;
                    worker.failure = err;
                };
            }
            for (workers[0..count], 0..) |*slot, i| {
                var worker = slot.*.?;
                if (worker.task) |task| {
                    try worker.done.wait(self.context.io.?);
                    var running = task;
                    worker.task = null;
                    running.await(self.context.io.?) catch |err| {
                        worker.failure = err;
                    };
                }
                if (worker.failure) |err| {
                    if (err != error.OutOfMemory or !worker.budget.isExhausted()) return err;
                    worker.close();
                    slot.* = null;
                    worker = try Worker.create(self, components[first + i], a, std.math.maxInt(usize));
                    slot.* = worker;
                    try Worker.run(worker);
                }
                const result = worker.result.?;
                // Component results contain hits/profile only; graph attachment
                // and fusion run after all independent components have joined.
                std.debug.assert(result.graph_results.len == 0 and result.graph_metric_results.len == 0 and result.graph_metric_rerank_status == null and result.shard_identity_read_generations.len == 0);
                var owned = try cloneSet(null, a, .{ .name = "", .hits = result.hits, .total_hits = result.total_hits, .total_hits_relation = result.total_hits_relation }, true);
                errdefer owned.deinit();
                owned.document_authority_version = result.document_authority_version;
                owned.identity_read_generation = result.identity_read_generation;
                owned.ordered_window_complete = result.ordered_window_complete;
                if (result.sort_profile) |profile| try owned.setOwnedSortProfile(profile);
                batch.results[first + i] = owned;
                batch.execution_ns[first + i] = worker.elapsed_ns;
                worker.close();
                slot.* = null;
            }
            first += count;
        }
        return batch;
    }
    fn searchRecentParts(self: *Execution, a: A, req: types.SearchRequest, query: anytype, comptime part: anytype, combined: *types.SearchResult, scheduling: *local.sql_parallel_scheduler.Scheduler) !void {
        return self.searchVectorParts(a, req, query, part, combined, scheduling, false);
    }
    fn searchVectorParts(self: *Execution, a: A, req: types.SearchRequest, query: anytype, comptime part: anytype, combined: *types.SearchResult, scheduling: *local.sql_parallel_scheduler.Scheduler, comptime include_archive: bool) !void {
        const scheduler = local.sql_parallel_scheduler;
        const workspace = 16 * 1024 * 1024;
        const Worker = struct {
            budget: local.sql_memory_budget = .{ .backing = std.heap.page_allocator, .limit = workspace },
            arena: std.heap.ArenaAllocator,
            execution: Execution,
            source_view: ?local.serverless_query_lake_serving.ServingSource = null,
            parent_source: *local.serverless_query_lake_serving.ServingSource = undefined,
            result: ?types.SearchResult = null,
            task: ?scheduler.Task(anyerror!void) = null,
            failure: ?anyerror = null,
            serial_only: bool = false,
            done: std.Io.Event = .unset,
            fn create(base: *Execution, selected_run: ?@import("lake_recent_vectors.zig").Run, limit: usize, backing: A) !*@This() {
                const owner = try std.heap.page_allocator.create(@This());
                owner.* = .{ .arena = undefined, .execution = undefined };
                owner.budget.limit = limit;
                owner.serial_only = limit == std.math.maxInt(usize);
                owner.budget.backing = backing;
                owner.arena = .init(owner.budget.allocator());
                errdefer {
                    owner.arena.deinit();
                    std.heap.page_allocator.destroy(owner);
                }
                owner.parent_source = base.source;
                if (include_archive) owner.source_view = base.source.parallelView(owner.budget.allocator()) catch |err| {
                    if (err == error.OutOfMemory and owner.budget.isExhausted()) return error.SqlMemoryLimitExceeded;
                    return err;
                };
                owner.execution = base.childExecution(if (include_archive) &owner.source_view.? else base.source, owner.arena.allocator(), owner.budget.allocator());
                owner.execution.active_recent = selected_run != null;
                owner.execution.recent_run = selected_run;
                return owner;
            }
            fn run(owner: *@This(), request: types.SearchRequest, q: @TypeOf(query)) anyerror!void {
                defer if (owner.execution.context.io) |io| owner.done.set(io);
                try owner.execution.context.ensureActive();
                if (include_archive and !owner.execution.active_recent and (request.filter_query_json.len != 0 or request.exclusion_query_json.len != 0)) {
                    // Membership cursors are mutable. Reopen them under this lane's
                    // pinned source rather than copying or sharing the parent cursor.
                    try owner.execution.prepareVectorMembership(owner.budget.allocator(), request);
                }
                owner.result = try part(&owner.execution, owner.budget.allocator(), request, q);
            }
            fn close(owner: *@This()) void {
                std.debug.assert(owner.task == null);
                if (owner.result) |*result| result.deinit();
                owner.execution.deinit();
                if (owner.source_view) |*view| view.closeParallelView(owner.parent_source);
                owner.arena.deinit();
                std.debug.assert(owner.budget.live == 0);
                std.heap.page_allocator.destroy(owner);
            }
        };
        const legacy = [_]@import("lake_recent_vectors.zig").Run{.{ .prefix = 0, .digest = @splat(0), .declarations = self.recent_declarations, .cloud_generations = self.recent_cloud }};
        const runs = if (self.recent_runs.len != 0) self.recent_runs else &legacy;
        var first: usize = 0;
        const archive_count: usize = @intFromBool(include_archive);
        if (self.scheduled_component) {
            // The enclosing component already owns scheduler admission. Keep
            // nested partition allocations inside that lane's quota rather
            // than multiplying unadmitted caller workspaces under saturation.
            for (0..runs.len + archive_count) |i| {
                try self.context.ensureActive();
                var scratch = std.heap.ArenaAllocator.init(a);
                defer scratch.deinit();
                var child = self.childExecution(self.source, scratch.allocator(), a);
                child.active_recent = !(include_archive and i == 0);
                child.recent_run = if (child.active_recent) runs[i - archive_count] else null;
                defer child.deinit();
                if (!child.active_recent) try child.prepareVectorMembership(a, req);
                var result = try part(&child, a, req, query);
                defer result.deinit();
                const merged = try local.api_query.mergeSearchResults(a, req, &.{ combined.*, result }, 0, req.limit);
                combined.deinit();
                combined.* = merged;
            }
            return;
        }
        while (first < runs.len + archive_count) {
            var workers: [3]?*Worker = @splat(null);
            defer {
                for (&workers) |*slot| if (slot.*) |worker| {
                    if (worker.task) |task| {
                        var pending = task;
                        worker.task = null;
                        _ = pending.cancel(self.context.io.?) catch {};
                    }
                    worker.close();
                };
            }
            const count = @min(workers.len, runs.len + archive_count - first);
            for (workers[0..count], 0..) |*slot, i| {
                const selected_run = if (include_archive and first + i == 0) null else runs[first + i - archive_count];
                const worker = Worker.create(self, selected_run, workspace, @import("antfly_platform").allocator.processAllocator(std.heap.page_allocator)) catch |err| switch (err) {
                    error.SqlMemoryLimitExceeded => try Worker.create(self, selected_run, std.math.maxInt(usize), a),
                    else => return err,
                };
                slot.* = worker;
                if (!worker.serial_only) if (self.context.io) |io| {
                    worker.task = scheduling.submitIo(io, workspace, Worker.run, .{ worker, req, query });
                };
                if (worker.task == null) Worker.run(worker, req, query) catch |err| {
                    // Cancellation must unwind immediately: swallowing it
                    // would consume the caller cancellation point before joins.
                    if (err != error.OutOfMemory or !worker.budget.isExhausted()) return err;
                    worker.failure = err;
                };
            }
            // Consume one joined lane immediately. Other result owners keep
            // their reservations until their own join, rather than accumulating
            // a batch of live results outside shared admission.
            for (workers[0..count], 0..) |*slot, i| {
                var worker = slot.*.?;
                if (worker.task) |task| {
                    // Future.await itself is uncancelable. Wait at an explicit
                    // cancellation point while the task still owns its lease.
                    try worker.done.wait(self.context.io.?);
                    var pending = task;
                    worker.task = null;
                    pending.await(self.context.io.?) catch |err| {
                        worker.failure = err;
                    };
                }
                if (worker.failure) |err| {
                    if (err != error.OutOfMemory or !worker.budget.isExhausted()) return err;
                    // Large individual requests retain the serial path's
                    // allocation policy instead of inheriting a lane quota.
                    worker.close();
                    slot.* = null;
                    worker = try Worker.create(self, if (include_archive and first + i == 0) null else runs[first + i - archive_count], std.math.maxInt(usize), a);
                    slot.* = worker;
                    try Worker.run(worker, req, query);
                }
                const merged = try local.api_query.mergeSearchResults(a, req, &.{ combined.*, worker.result.? }, 0, req.limit);
                combined.deinit();
                combined.* = merged;
                worker.close();
                slot.* = null;
            }
            first += count;
        }
    }
    fn searchDense(raw: ?*anyopaque, a: A, req: types.SearchRequest, query: types.DenseKnnQuery) !types.SearchResult {
        const self = from(raw);
        if (self.recent_declarations.len == 0) return searchDensePart(raw, a, req, query);
        var leaf = req;
        leaf.offset = 0;
        leaf.limit = std.math.add(u32, req.offset, req.limit) catch return error.QueryCandidateBudgetExceeded;
        var combined: types.SearchResult = .{ .alloc = a, .hits = &.{}, .total_hits = 0 };
        defer combined.deinit();
        try self.searchVectorParts(a, leaf, query, searchDensePart, &combined, local.sql_parallel_scheduler.global(), true);
        return local.api_query.mergeSearchResults(a, req, &.{combined}, req.offset, req.limit);
    }
    fn searchDensePart(raw: ?*anyopaque, a: A, req: types.SearchRequest, dense: types.DenseKnnQuery) !types.SearchResult {
        return search.searchDense(a, from(raw).vectorRequest(req), dense, .{ .ctx = raw, .exact_doc_id_filters = true, .filter_candidate_presence = true, .text_index_entry = noLocal, .dense_index = denseIndex, .lookup_doc_key = lookupDocKey, .resolve_hit_key = densePublicKey, .lookup_vector_id = lookupVectorId, .load_projected_document = requireProjected, .load_projected_documents = loadProjected, .hbc_search = denseSearch, .hbc_search_profiled = denseSearchProfiled, .postprocess = postprocessVector });
    }
    fn sparseIndex(raw: ?*anyopaque, name: ?[]const u8) !?*local.storage_db_catalog_index_manager.IndexManager.SparseIndex {
        const self = from(raw);
        try self.context.ensureActive();
        const native = @import("lake_index_native_sparse.zig");
        const declarations = if (self.active_recent) if (self.recent_run) |run| run.declarations else self.recent_declarations else self.declarations;
        const entries = if (self.active_recent) &self.recent_sparse_entries else &self.sparse_entries;
        var count: usize = 0;
        for (declarations) |declaration| if (declaration.artifact.kind == .sparse_segment and native.supportsMetadata(declaration.artifact.metadata_version)) {
            count += 1;
        };
        const selected = for (declarations) |declaration| {
            if (declaration.artifact.kind == .sparse_segment and native.supportsMetadata(declaration.artifact.metadata_version) and (if (name) |explicit| std.mem.eql(u8, explicit, declaration.name) else count == 1)) break declaration;
        } else return error.IndexNotFound;
        if (!self.active_recent) try self.bindIndex(selected.name);
        if (entries.get(if (self.active_recent) selected.artifact.artifact_id else selected.name)) |entry| {
            for (self.runtimes.items) |runtime| if (runtime.sparse_entry == entry) {
                self.unit_runtime = runtime;
                break;
            };
            return entry;
        }
        try self.runtimes.ensureUnusedCapacity(self.arena, 1);
        const runtime = try self.server.lake_native_runtimes.acquire(self.server, selected, try @import("lake_recent_vectors.zig").runtimeDomain(selected), self.store.identity, self.context, if (self.active_recent) if (self.recent_run) |run| run.cloud_generations else self.recent_cloud else self.cloud_generations);
        errdefer runtime.release();
        const entry = runtime.sparse_entry.?;
        try entries.put(self.arena, if (self.active_recent) selected.artifact.artifact_id else selected.name, entry);
        self.runtimes.appendAssumeCapacity(runtime);
        self.unit_runtime = runtime;
        return entry;
    }
    fn requireProjected(raw: ?*anyopaque, a: A, req: types.SearchRequest, key: []const u8) ![]u8 {
        return (try loadProjectedOne(raw, a, req, key)) orelse error.StoredDocMissing;
    }
    fn postprocessVector(raw: ?*anyopaque, a: A, req: types.SearchRequest, result: types.SearchResult, known_chunk_backed: bool) !types.SearchResult {
        var owned = result;
        var transferred = false;
        errdefer if (!transferred) owned.deinit();
        var chunk_backed = known_chunk_backed;
        for (owned.hits) |*hit| {
            if (!local.storage_internal_keys.isChunkArtifactRecordKey(hit.id)) continue;
            chunk_backed = true;
            var ref = (try local.storage_db_artifact_ids.decodeArtifactRefAlloc(a, hit.id)) orelse return error.InvalidChunkArtifact;
            errdefer ref.deinit(a);
            const payload = try from(raw).unitPayload(a, hit.id);
            defer a.free(payload);
            var metadata = try std.json.parseFromSlice(std.json.Value, a, payload, .{});
            defer metadata.deinit();
            const unit_id = metadata.value.object.get("_parent_unit_id").?.string;
            // Native chunk refs carry a unit_id directly. A source ref is
            // reserved for embeddings and would make the chunk ref invalid.
            if (ref.unit_id) |previous| a.free(previous);
            ref.unit_id = null;
            ref.unit_id = try a.dupe(u8, unit_id);
            if (hit.artifact_ref) |*previous| previous.deinit(a);
            hit.artifact_ref = ref;
        }
        // The shape function takes ownership, including its error paths.
        transferred = true;
        return shape.postprocessVectorSearchResult(a, req, owned, chunk_backed, .{ .ctx = raw, .is_visible = visible, .resolve_parent_id = parent, .load_parent_stored = parentStored, .load_stored = loadOne, .load_many_stored = loadMany, .load_projected_stored = loadProjectedOne, .load_many_projected_stored = loadProjected });
    }
    fn searchSparse(raw: ?*anyopaque, a: A, req: types.SearchRequest, query: types.SparseKnnQuery) !types.SearchResult {
        const self = from(raw);
        if (self.recent_declarations.len == 0) return searchSparsePart(raw, a, req, query);
        var leaf = req;
        leaf.offset = 0;
        leaf.limit = std.math.add(u32, req.offset, req.limit) catch return error.QueryCandidateBudgetExceeded;
        var combined: types.SearchResult = .{ .alloc = a, .hits = &.{}, .total_hits = 0 };
        defer combined.deinit();
        try self.searchVectorParts(a, leaf, query, searchSparsePart, &combined, local.sql_parallel_scheduler.global(), true);
        return local.api_query.mergeSearchResults(a, req, &.{combined}, req.offset, req.limit);
    }
    fn searchSparsePart(raw: ?*anyopaque, a: A, req: types.SearchRequest, sparse: types.SparseKnnQuery) !types.SearchResult {
        return search.searchSparse(a, from(raw).vectorRequest(req), sparse, .{ .score_spill = .{ .io = from(raw).context.io.?, .directory = "/tmp" }, .ctx = raw, .exact_doc_id_filters = true, .project_key = publicKey, .native_key = nativeKey, .filter_candidate_presence = true, .text_index_entry = noLocal, .sparse_index = sparseIndex, .load_projected_document = requireProjected, .load_projected_documents = loadProjected, .postprocess = postprocessVector });
    }
    fn cloneSet(_: ?*anyopaque, a: A, set: local.storage_db_query_graph_exec.NamedResultSet, stored: bool) !types.SearchResult {
        return local.storage_db_query_graph_exec.cloneNamedSetAsResult(a, set, stored);
    }
    fn fuseSets(raw: ?*anyopaque, a: A, req: types.SearchRequest, sets: []const local.storage_db_query_graph_exec.NamedResultSet) !types.SearchResult {
        return local.storage_db_query_graph_exec.fuseNamedSets(a, req, sets, .{ .ctx = raw, .load_projected_document = loadProjectedOne });
    }
    fn loadProjectedOne(raw: ?*anyopaque, a: A, req: types.SearchRequest, key: []const u8) !?[]u8 {
        const values = try loadProjected(raw, a, req, &.{key});
        defer a.free(values);
        return values[0];
    }
    fn attachGraph(_: ?*anyopaque, _: A, _: types.SearchRequest, _: *types.SearchResult, _: []const local.storage_db_query_graph_exec.NamedResultSet) !void {}
    const PayloadRuntime = struct { entry: *@import("lake_index_native_runtime_cache.zig").Entry, owned: bool };
    fn payloadRuntime(self: *Execution, name: []const u8, public_parent: []const u8) !PayloadRuntime {
        try self.context.ensureActive();
        const recent = if (self.overlay) |overlay| overlay.row(public_parent) != null else false;
        const run: ?@import("lake_recent_vectors.zig").Run = if (recent and self.recent_runs.len != 0) for (self.recent_runs) |candidate| {
            if (std.mem.startsWith(u8, public_parent, "wal1:") and public_parent.len > 5 and candidate.prefix == public_parent[5]) break candidate;
        } else return error.InvalidChunkArtifact else null;
        const declarations = if (run) |value| value.declarations else if (recent) self.recent_declarations else self.declarations;
        const declaration = for (declarations) |candidate| {
            if (std.mem.eql(u8, candidate.name, name) and (candidate.artifact.kind == .vector_segment or candidate.artifact.kind == .sparse_segment)) break candidate;
        } else return error.InvalidChunkArtifact;
        const dense_entries = if (recent) &self.recent_dense_entries else &self.dense_entries;
        const sparse_entries = if (recent) &self.recent_sparse_entries else &self.sparse_entries;
        const key = if (recent) declaration.artifact.artifact_id else declaration.name;
        for (self.runtimes.items) |runtime| {
            if (runtime.dense_entry) |entry| {
                if (dense_entries.get(key) == entry) return .{ .entry = runtime, .owned = false };
            } else if (runtime.sparse_entry) |entry| {
                if (sparse_entries.get(key) == entry) return .{ .entry = runtime, .owned = false };
            }
        }
        // Deferred fusion/delivery reopens one exact run at a time. It must not
        // pin every winning partition or alias equal index names across runs.
        const runtime = try self.server.lake_native_runtimes.acquire(self.server, declaration, try @import("lake_recent_vectors.zig").runtimeDomain(declaration), self.store.identity, self.context, if (run) |value| value.cloud_generations else if (recent) self.recent_cloud else self.cloud_generations);
        return .{ .entry = runtime, .owned = true };
    }
    fn unitPayload(self: *Execution, a: A, key: []const u8) ![]u8 {
        try self.context.ensureActive();
        const unit_key = local.storage_internal_keys.isDocumentUnitArtifactRecordKey(key);
        const chunk_key = local.storage_internal_keys.isChunkArtifactRecordKey(key);
        var artifact_ref = if (unit_key or chunk_key) (try local.storage_db_artifact_ids.decodeArtifactRefAlloc(a, key)) orelse return error.InvalidChunkArtifact else null;
        defer if (artifact_ref) |*ref| ref.deinit(a);
        const public_parent = if (artifact_ref) |ref| ref.document_id else try @import("lake_enrichment_units.zig").parent(key);
        const index_name = if (artifact_ref) |ref| named: {
            const suffix = if (unit_key) "_sources" else "_chunks";
            if (!std.mem.endsWith(u8, ref.name, suffix)) return error.InvalidChunkArtifact;
            break :named ref.name[0 .. ref.name.len - suffix.len];
        } else if (self.unit_runtime) |runtime| if (runtime.dense_entry) |entry| entry.config.name else runtime.sparse_entry.?.config.name else return error.InvalidChunkArtifact;
        const selected = try self.payloadRuntime(index_name, public_parent);
        defer if (selected.owned) selected.entry.release();
        const runtime = selected.entry;
        const entry_name = if (runtime.dense_entry) |entry| entry.config.name else runtime.sparse_entry.?.config.name;
        const source_name = try std.fmt.allocPrint(a, "{s}_sources", .{entry_name});
        defer a.free(source_name);
        const native = if (unit_key) source_key: {
            const ref = artifact_ref.?;
            const base = try nativeKey(self, a, ref.document_id);
            defer a.free(base);
            break :source_key try @import("lake_enrichment_units.zig").sourceKey(a, base, try std.fmt.parseUnsigned(u32, ref.unit_id orelse return error.InvalidChunkArtifact, 10));
        } else try nativeKey(self, a, key);
        defer a.free(native);
        const bytes = if (runtime.vectors) |vectors| dense_payload: {
            var txn = try vectors.beginRead();
            defer txn.abort();
            break :dense_payload try a.dupe(u8, try txn.get(.{ .name = "lake_units" }, native));
        } else sparse_payload: {
            var txn = try runtime.sparse_entry.?.index.beginReadTxn();
            defer txn.abort();
            const record_key = try std.fmt.allocPrint(a, "lake-unit:{s}", .{native});
            defer a.free(record_key);
            break :sparse_payload try a.dupe(u8, try txn.get(record_key));
        };
        defer a.free(bytes);
        var arena = std.heap.ArenaAllocator.init(a);
        defer arena.deinit();
        const pa = arena.allocator();
        var value = try std.json.parseFromSliceLeaky(std.json.Value, pa, bytes, .{});
        try value.object.put(pa, "_parent_doc_key", .{ .string = public_parent });
        if (!unit_key) {
            const unit_id = value.object.get("_parent_unit_id").?.string;
            try value.object.put(pa, "_parent_unit_key", .{ .string = try local.storage_internal_keys.documentUnitArtifactKeyAlloc(pa, public_parent, source_name, unit_id) });
        }
        const field_name = if (runtime.dense_entry) |entry| entry.field_name else runtime.sparse_entry.?.field_name;
        if (value.object.get("text")) |text| if (text == .string) try value.object.put(pa, field_name, text);
        try value.object.put(pa, "_source_field", .{ .string = field_name });
        if (artifact_ref) |ref| try value.object.put(pa, "_artifact_name", .{ .string = ref.name });
        try value.object.put(pa, "_source_artifact_name", .{ .string = source_name });
        try value.object.put(pa, "_id", .{ .string = key });
        try value.object.put(pa, "_type", .{ .string = if (unit_key) "unit" else "chunk" });
        return std.json.Stringify.valueAlloc(a, value, .{});
    }
    fn loadOne(raw: ?*anyopaque, a: A, key: []const u8) !?[]u8 {
        const result = try loadMany(raw, a, &.{key});
        defer a.free(result);
        return result[0];
    }
    const TransformInputs = struct { fields: ?[]const []const u8, covers_output: bool };
    fn planTransformInputs(self: *Execution, req: types.SearchRequest) !TransformInputs {
        var fields: std.ArrayList([]const u8) = .empty;
        if (req.hasHitEvaluation()) {
            const raw = try std.json.parseFromSliceLeaky(std.json.Value, self.arena, req.evaluation_json, .{});
            const plan = try local.functions_expressions.Plan.parse(self.arena, raw);
            for (plan.required_fields) |path| try appendHydrationPath(self.arena, self.table, &fields, path);
        }
        if (req.reranker) |reranker| {
            // Free-form template helpers may inspect the root or change context.
            // Without an authoritative dependency proof, retain complete input
            // under the same byte admission as direct-field templates.
            if (reranker.template.len != 0) return .{ .fields = null, .covers_output = true };
            try appendHydrationPath(self.arena, self.table, &fields, reranker.field);
        }
        var highlights = retrievalRequest(req);
        highlights.include_stored = false;
        highlights.include_all_fields = false;
        highlights.fields = &.{};
        const highlight_fields = try self.planHydration(highlights);
        if (highlight_fields) |paths| {
            for (paths) |path| try appendHydrationPath(self.arena, self.table, &fields, path);
        } else return .{ .fields = null, .covers_output = true };
        const output = try projectionColumns(self.arena, self.table, retrievalRequest(req));
        const covered = if (output) |paths| for (paths) |path| {
            const present = for (fields.items) |field| {
                if (std.mem.eql(u8, field, path)) break true;
            } else false;
            if (!present) break false;
        } else true else false;
        return .{ .fields = fields.items, .covers_output = covered };
    }
    const transform_source_bytes = 64 * 1024 * 1024;
    fn hydrateTransformCandidates(self: *Execution, a: A, req: types.SearchRequest, fields: ?[]const []const u8, hits: []types.SearchHit) !void {
        var budget: local.sql_memory_budget = .{ .backing = a, .limit = transform_source_bytes };
        self.hydrateTransformCandidatesBounded(a, &budget, fields, hits) catch |err| {
            if ((err == error.OutOfMemory and budget.isLocallyExhausted()) or err == error.TransformSourceBudgetExceeded) return if (req.hasHitEvaluation()) error.DecisionLimitExceeded else error.QueryCandidateBudgetExceeded;
            return err;
        };
    }
    fn hydrateTransformCandidatesBounded(self: *Execution, a: A, budget: *local.sql_memory_budget, fields: ?[]const []const u8, hits: []types.SearchHit) !void {
        var scratch = std.heap.ArenaAllocator.init(budget.allocator());
        defer scratch.deinit();
        var retained: usize = 0;
        var first: usize = 0;
        while (first < hits.len) {
            try self.context.ensureActive();
            _ = scratch.reset(.free_all);
            const count = @min(32, hits.len - first);
            const batch = hits[first..][0..count];
            var keys: [32][]const u8 = undefined;
            for (batch, keys[0..count]) |hit, *key| key.* = hit.id;
            const documents = try loadManySelected(self, scratch.allocator(), keys[0..count], fields);
            // Loading joins all cursor work before the budget is narrowed. The
            // retained pool and temporary cursor/serialization capacity share
            // one bound, including the copy that transfers into SearchHit's
            // ordinary allocator ownership.
            for (batch, documents) |*hit, document| {
                const bytes = document orelse return error.StoredDocMissing;
                if (bytes.len > budget.limit -| budget.live) return error.TransformSourceBudgetExceeded;
                const owned = try a.dupe(u8, bytes);
                retained += owned.len;
                budget.limit = transform_source_bytes - retained;
                if (hit.stored_data) |old| a.free(old);
                hit.stored_data = owned;
            }
            first += count;
        }
    }
    fn loadMany(raw: ?*anyopaque, a: A, keys: []const []const u8) ![]?[]u8 {
        return loadManySelected(raw, a, keys, null);
    }
    fn loadManySelected(raw: ?*anyopaque, a: A, keys: []const []const u8, selected_fields: ?[]const []const u8) ![]?[]u8 {
        const self = from(raw);
        const started = @import("antfly_platform").time.monotonicNs();
        defer self.server.lake_query_metrics.record(.hydration, started);
        return loadSelected([]u8, raw, a, keys, selected_fields);
    }
    fn loadSelected(comptime T: type, raw: ?*anyopaque, a: A, keys: []const []const u8, selected_fields: ?[]const []const u8) ![]?T {
        const self = from(raw);
        const hydration_started = @import("antfly_platform").time.monotonicNs();
        defer self.hydration_ns +|= @import("antfly_platform").time.monotonicNs() -| hydration_started;
        try self.context.ensureActive();
        const result = try a.alloc(?T, keys.len);
        @memset(result, null);
        errdefer {
            for (result) |*value| if (value.*) |*owned| {
                if (T == types.ColumnSource) owned.deinit() else if (T == std.json.Value) types.deinitJsonValue(a, owned) else a.free(owned.*);
            };
            a.free(result);
        }
        if (keys.len == 0) return result;
        var members = false;
        for (keys) |key| if ((try @import("lake_enrichment_units.zig").parent(key)).len != key.len or local.storage_internal_keys.isDocumentUnitArtifactRecordKey(key) or local.storage_internal_keys.isChunkArtifactRecordKey(key)) {
            members = true;
            break;
        };
        if (members) {
            for (keys, 0..) |key, position| {
                const base = try @import("lake_enrichment_units.zig").parent(key);
                if (base.len == key.len and !local.storage_internal_keys.isDocumentUnitArtifactRecordKey(key) and !local.storage_internal_keys.isChunkArtifactRecordKey(key)) {
                    const values = try loadSelected(T, raw, a, &.{key}, selected_fields);
                    result[position] = values[0];
                    a.free(values);
                } else {
                    if (T == types.ColumnSource) return error.UnsupportedSqlExecution;
                    const bytes = try self.unitPayload(a, key);
                    if (T == std.json.Value) {
                        result[position] = try std.json.parseFromSliceLeaky(std.json.Value, a, bytes, .{ .allocate = .alloc_always });
                        a.free(bytes);
                    } else result[position] = bytes;
                }
            }
            return result;
        }
        if (self.overlay) |overlay| {
            var archive: std.ArrayList([]const u8) = .empty;
            defer archive.deinit(a);
            var positions: std.ArrayList(usize) = .empty;
            defer positions.deinit(a);
            var recent: usize = 0;
            for (keys, 0..) |key, position| {
                if (overlay.row(key)) |row| {
                    if (T == types.ColumnSource) return error.UnsupportedSqlExecution;
                    var image: std.json.Value = .{ .object = .empty };
                    defer types.deinitJsonValue(a, &image);
                    var fields = row.object.iterator();
                    while (fields.next()) |field| {
                        const include = if (selected_fields) |selected| for (selected) |path| {
                            if (std.mem.eql(u8, path, field.key_ptr.*)) break true;
                        } else false else true;
                        if (include) try image.object.put(a, try a.dupe(u8, field.key_ptr.*), try types.cloneJsonValue(a, field.value_ptr.*));
                    }
                    try image.object.put(a, try a.dupe(u8, "_id"), .{ .string = try a.dupe(u8, key) });
                    try image.object.put(a, try a.dupe(u8, "_type"), .{ .string = try a.dupe(u8, "row") });
                    if (T == std.json.Value) result[position] = try types.cloneJsonValue(a, image) else result[position] = try std.json.Stringify.valueAlloc(a, image, .{});
                    recent += 1;
                } else {
                    try archive.append(a, key);
                    try positions.append(a, position);
                }
            }
            if (recent != 0) {
                const values = try loadSelected(T, raw, a, archive.items, selected_fields);
                defer a.free(values);
                for (values, positions.items) |value, position| result[position] = value;
                return result;
            }
        }
        // Small results keep one selection. Larger results use a bounded
        // external ordering pass, then visit physical windows in file/group/row
        // order. Window size never becomes a public result/candidate limit.
        var manager: local.sql_spill.Manager = .{ .alloc = a, .io = self.context.io.?, .context = self, .checkpoint = scanCheckpoint };
        defer manager.deinit();
        var order = local.sql_spill.Sort.init(a, &manager, &.{.{}}, 512 * 1024);
        defer order.deinit();
        const window = local.sql_lake_cursor.max_selection_rows;
        if (keys.len > window) {
            var scratch = std.heap.ArenaAllocator.init(a);
            defer scratch.deinit();
            for (keys, 0..) |key, position| {
                _ = scratch.reset(.retain_capacity);
                const canonical_key = try publicKey(raw, scratch.allocator(), key);
                try order.add(.{ .values = &.{}, .keys = &.{local.sql_scalar.Datum.fromJson(.{ .string = canonical_key })}, .ordinal = position });
            }
        }
        var first: usize = 0;
        while (first < keys.len) {
            const count = @min(window, keys.len - first);
            defer first += count;
            hydrate: {
                var arena = std.heap.ArenaAllocator.init(a);
                defer arena.deinit();
                const ca = arena.allocator();
                const refs = try ca.alloc(local.storage_rowsource_types.RowRef, count);
                const canonical = try ca.alloc([]const u8, count);
                const window_positions = try ca.alloc(usize, count);
                for (canonical, refs, window_positions, first..) |*mapped, *ref, *position, input_position| {
                    const sorted = if (keys.len > window) (try order.next(ca)) orelse return error.InvalidSqlBackendResponse else null;
                    position.* = if (sorted) |row| @intCast(row.ordinal) else input_position;
                    const key = if (sorted) |row| row.keys[0].value.string else try publicKey(raw, ca, keys[input_position]);
                    mapped.* = key;
                    if (!local.storage_rowsource_identity.isPublic(key)) return error.ExternalLakeSnapshotMismatch;
                    const parts = try local.storage_rowsource_identity.parse(key);
                    try local.storage_rowsource_identity.validateSnapshot(parts, self.source.inventory.source_id, self.source.inventory.snapshot_id);
                    if (parts.version != self.source.inventory.row_identity_version) return error.ExternalLakeSnapshotMismatch;
                    const file = (try self.fileFor(try local.storage_rowsource_identity.publicFile(key), true)) orelse return error.ExternalLakeSnapshotMismatch;
                    ref.* = .{ .external = .{ .source_id = self.source.inventory.source_id, .snapshot_id = self.source.inventory.snapshot_id, .identity_version = parts.version, .file_id = file, .row_group_ordinal = parts.group, .row_ordinal = parts.row } };
                }
                var by_key: std.StringHashMapUnmanaged(std.ArrayListUnmanaged(usize)) = .empty;
                for (canonical, window_positions) |key, position| {
                    const entry = try by_key.getOrPut(ca, key);
                    if (!entry.found_existing) entry.value_ptr.* = .empty;
                    try entry.value_ptr.append(ca, position);
                }
                const fields = if (selected_fields) |selected| selected else all: {
                    const all_fields = try ca.alloc([]const u8, self.table.columns.len);
                    for (all_fields, self.table.columns) |*field, column| field.* = column.path;
                    break :all all_fields;
                };
                var request = self.request;
                request.cancellation = .{ .ptr = self, .is_cancelled_fn = canceled };
                const covered = @import("lake_index_sql_rows.zig").gatherCovering(a, self.server, self.table, self.source, request, .{ .artifacts = self.store.artifactStore(), .store_identity = self.store.identity, .domain = self.domain, .declarations = self.declarations, .read_context = self.context }, refs, fields) catch |err| switch (err) {
                    error.SqlMemoryLimitExceeded, error.ArtifactReadBudgetExceeded => null,
                    else => return err,
                };
                if (covered) |values| {
                    defer {
                        for (values) |value| if (value) |row| row.deinit();
                        a.free(values);
                    }
                    for (values, window_positions) |value, position| {
                        const row = value orelse return error.StoredDocMissing;
                        if (T == types.ColumnSource) {
                            result[position] = row.clone();
                        } else {
                            var covered_scratch = std.heap.ArenaAllocator.init(a);
                            defer covered_scratch.deinit();
                            const json = try row.value(&covered_scratch);
                            if (T == std.json.Value) result[position] = try types.cloneJsonValue(a, json) else result[position] = try std.json.Stringify.valueAlloc(a, json, .{});
                        }
                    }
                    break :hydrate;
                }
                const cursor = try local.sql_lake_cursor.openPinned(a, self.table, .{ .fields = fields, .row_refs = refs, .limit = @intCast(count) }, request, self.source);
                defer cursor.close(cursor.ptr);
                if (T == std.json.Value or T == types.ColumnSource) if (cursor.next_columns) |next_columns| {
                    while (true) {
                        var page_arena = std.heap.ArenaAllocator.init(a);
                        defer page_arena.deinit();
                        const pa = page_arena.allocator();
                        const page = try next_columns(cursor.ptr, pa, 4096);
                        try page.validate();
                        if (T == types.ColumnSource and page.native != null) return error.UnsupportedSqlExecution;
                        const retained = if (T == types.ColumnSource) try types.ColumnSourcePage.retainOrCopy(a, page.batch, page.selection, page.retain_columns) else {};
                        defer if (T == types.ColumnSource) retained.release();
                        for (0..page.selection.len) |row_index| {
                            const identity = try page.cell(pa, row_index, "_id");
                            if (identity.value != .string) return error.InvalidSqlBackendResponse;
                            const positions = by_key.get(identity.value.string) orelse return error.InvalidSqlBackendResponse;
                            for (positions.items) |position| {
                                if (result[position] != null) return error.InvalidSqlBackendResponse;
                                if (T == types.ColumnSource) {
                                    result[position] = retained.row(row_index);
                                    continue;
                                }
                                var value: std.json.Value = .{ .object = .empty };
                                errdefer types.deinitJsonValue(a, &value);
                                for (page.batch.columns) |column| {
                                    const cell = try page.cell(pa, row_index, column.name);
                                    const name = try a.dupe(u8, column.name);
                                    errdefer a.free(name);
                                    var owned_cell = try types.cloneJsonValue(a, cell.value);
                                    errdefer types.deinitJsonValue(a, &owned_cell);
                                    try value.object.put(a, name, owned_cell);
                                }
                                result[position] = value;
                            }
                        }
                        if (page.after == null) break;
                    }
                    break :hydrate;
                };
                if (T == types.ColumnSource) return error.UnsupportedSqlExecution;
                while (true) {
                    const page = try cursor.next(cursor.ptr, a, 256);
                    defer page.deinit();
                    for (page.rows) |row| {
                        const positions = by_key.get(row.id) orelse return error.InvalidSqlBackendResponse;
                        for (positions.items) |position| {
                            if (result[position] != null) return error.InvalidSqlBackendResponse;
                            result[position] = if (T == std.json.Value) try types.cloneJsonValue(a, row.value) else try std.json.Stringify.valueAlloc(a, row.value, .{});
                        }
                    }
                    if (page.after == null) break;
                }
            }
        }
        try self.context.ensureActive();
        return result;
    }
    fn loadProjected(raw: ?*anyopaque, a: A, req: types.SearchRequest, keys: []const []const u8) ![]?[]u8 {
        const self = from(raw);
        const fields = if (requiresEncodedSource(req)) null else self.hydration_fields;
        const result = try loadManySelected(raw, a, keys, fields);
        errdefer {
            for (result) |bytes| if (bytes) |value| a.free(value);
            a.free(result);
        }
        for (result, keys) |*bytes, key| if (bytes.*) |value| {
            const projected = try project(raw, a, req, key, value);
            a.free(value);
            bytes.* = projected;
        };
        return result;
    }
    fn absent(_: ?*anyopaque, _: A, _: []const u8) !?std.json.Value {
        return null;
    }
    fn project(raw: ?*anyopaque, a: A, req: types.SearchRequest, key: []const u8, bytes: []const u8) ![]u8 {
        // Match the local DB contract: highlighting and other postprocessing
        // consume original source; the public encoder applies deferred fields.
        if (req.defer_stored_projection) return a.dupe(u8, bytes);
        return local.storage_db_query_projection.projectStoredBytesForSearch(a, req, key, bytes, .{ .ctx = raw, .load_chunks = absent, .load_embeddings = absent, .load_artifacts = absent });
    }
    fn visible(raw: ?*anyopaque, _: A, _: types.SearchHit) !bool {
        try from(raw).context.ensureActive();
        return true;
    }
    fn parent(_: ?*anyopaque, a: A, hit: types.SearchHit) ![]u8 {
        if (try local.storage_db_artifact_ids.decodeArtifactRefAlloc(a, hit.id)) |decoded| {
            var ref = decoded;
            defer ref.deinit(a);
            return a.dupe(u8, ref.document_id);
        }
        return a.dupe(u8, try @import("lake_enrichment_units.zig").parent(hit.id));
    }
    fn parentStored(raw: ?*anyopaque, a: A, _: types.SearchRequest, key: []const u8) !?[]u8 {
        return loadOne(raw, a, key);
    }
    fn postprocess(raw: ?*anyopaque, a: A, req: types.SearchRequest, result: types.SearchResult, _: bool) !types.SearchResult {
        return shape.postprocessTextSearchResult(a, req, result, false, .{ .ctx = raw, .is_visible = visible, .resolve_parent_id = parent, .load_parent_stored = parentStored, .load_stored = loadOne, .load_many_stored = loadMany, .load_projected_stored = loadProjectedOne, .load_many_projected_stored = loadProjected });
    }
};

/// Late hydration is an explicit dependency contract: source-dependent
/// operators keep the encoded provider path. Independent native retrieval
/// hands leased column pages directly to highlights and the public encoder.
fn canDeliverTypedSource(req: types.SearchRequest) bool {
    return !requiresEarlySource(req) and
        req.evaluation_limit == 0 and req.pruner == null and req.return_mode == .parent and !req.hierarchy_grouped_matches and req.hierarchy_group_level == .source and
        req.hierarchy_children == null and !req.defer_hierarchy_child_hydration and !req.hierarchy_include_source and !req.hierarchy_include_unit and
        req.hierarchy_match_include_all_fields and req.hierarchy_source_include_all_fields and req.hierarchy_unit_include_all_fields;
}

/// Coordinator-owned score transforms run once over the global candidate pool.
/// Their dependencies must not force inventory or source loading per window.
fn retrievalRequest(req: types.SearchRequest) types.SearchRequest {
    var retrieval = req;
    retrieval.reranker = null;
    retrieval.pruner = null;
    if (req.hasHitEvaluation()) {
        retrieval.evaluation_limit = 0;
        retrieval.evaluation_json = "";
    }
    return retrieval;
}

fn needsCandidateSource(req: types.SearchRequest) bool {
    return req.reranker != null or req.hasHitEvaluation();
}

fn requiresEarlySource(req: types.SearchRequest) bool {
    return req.hasHitEvaluation() or req.reranker != null or req.defer_hierarchy_child_hydration or
        req.hierarchy_children != null or req.hierarchy_include_source or req.hierarchy_include_unit or
        !req.hierarchy_match_include_all_fields or !req.hierarchy_source_include_all_fields or !req.hierarchy_unit_include_all_fields or
        req.doc_filter_bindings.len != 0 or req.query != .match_all;
}
fn requiresEncodedSource(req: types.SearchRequest) bool {
    return requiresEarlySource(req) or req.filter_query_json.len != 0 or
        req.exclusion_query_json.len != 0 or req.authorization_filter_query_json.len != 0;
}

/// Compile public include patterns to physical dependencies once per hydration
/// call. Exclusion-only projections still mean the complete source document.
fn projectionColumns(a: A, table: local.sql_catalog.Table, req: types.SearchRequest) !?[]const []const u8 {
    // Deferred wire projection does not require unrelated physical columns.
    // Consumers without an explicit dependency contract retain full source.
    if (requiresEarlySource(req)) return null;
    if (!req.include_stored) return &.{};
    if (req.fields.len == 0) return if (req.include_all_fields) null else &.{};
    var positive = false;
    for (req.fields) |field| if (field.len == 0 or field[0] != '-') {
        positive = true;
    };
    if (!positive) return null;
    var names: std.ArrayList([]const u8) = .empty;
    for (table.columns) |column| for (req.fields) |pattern| {
        if (pattern.len == 0 or pattern[0] == '-') continue;
        if (projectionMayUse(pattern, column.path)) {
            try names.append(a, column.path);
            break;
        }
    };
    return names.items;
}
fn appendHydrationPath(a: A, table: local.sql_catalog.Table, fields: *std.ArrayList([]const u8), path: []const u8) !void {
    for (table.columns) |column| {
        if (!projectionMayUse(path, column.path)) continue;
        const present = for (fields.items) |field| {
            if (std.mem.eql(u8, field, column.path)) break true;
        } else false;
        if (!present) try fields.append(a, column.path);
    }
}
fn appendIndexHydration(a: A, table: local.sql_catalog.Table, fields: *std.ArrayList([]const u8), pin: anytype) !bool {
    if (pin.selected_field) |path| {
        try appendHydrationPath(a, table, fields, path);
        return true;
    }
    if (pin.runtime_schema) |schema| if (schema.full_text_documents.len != 0) {
        if (schema.dynamic_templates.len != 0) return false;
        for (schema.full_text_documents) |document| {
            for (document.fields) |field| try appendHydrationPath(a, table, fields, field.path);
            for (document.dynamic_rules) |rule| try appendHydrationPath(a, table, fields, rule.parent_path);
            for (document.open_dynamic_paths) |path| try appendHydrationPath(a, table, fields, path);
            for (document.infer_type_dynamic_paths) |path| try appendHydrationPath(a, table, fields, path);
        }
        return true;
    };
    // Schema-less text extraction can depend on any source field.
    return false;
}

fn metadataCountEligible(req: types.SearchRequest) bool {
    return req.filter_query_json.len == 0 and req.exclusion_query_json.len == 0 and
        req.authorization_filter_query_json.len == 0 and req.distributed_text_stats.len == 0 and
        req.order_by.len == 0 and req.search_after.len == 0 and req.search_before.len == 0 and
        req.doc_filter_bindings.len == 0 and req.resolved_doc_filter == null and
        req.resolved_text_doc_filter == null and req.native_key_predicate == null and
        req.filter_prefix.len == 0 and req.filter_ids.len == 0 and req.exclude_ids.len == 0 and
        req.filter_doc_ids.len == 0 and !req.filter_doc_ids_positive and req.exclude_doc_ids.len == 0 and
        req.filter_text == null and req.exclusion_text == null and !req.hasHitEvaluation() and
        req.evaluation_limit == 0 and req.reranker == null and req.pruner == null;
}

fn storedHighlightEligible(req: types.SearchRequest, selected_field: ?[]const u8, stored: bool) bool {
    const options = req.highlight orelse return false;
    if (!stored or req.full_text == null or req.full_text_queries.len != 0 or req.dense != null or req.sparse != null or req.dense_queries.len != 0 or req.sparse_queries.len != 0) return false;
    if (selected_field) |field| for (options.fields) |path| {
        if (!std.mem.eql(u8, path, field)) return false;
    };
    return true;
}

fn projectionMayUse(pattern: []const u8, path: []const u8) bool {
    var patterns = std.mem.tokenizeScalar(u8, pattern, '.');
    var parts = std.mem.tokenizeScalar(u8, path, '.');
    while (true) {
        const token = patterns.next() orelse return true;
        const part = parts.next() orelse return true;
        if (!std.mem.eql(u8, token, "*") and !std.mem.eql(u8, token, part)) return false;
    }
}

// A retained cut exists for both raw Parquet and Iceberg. Only Iceberg
// snapshot IDs are catalog selectors; Parquet retains its object-version policy
// and the publication selection below verifies the exact resolved inventory.
fn pinPublishedSource(binding: *local.serverless_external_source_mod.ExternalTableBinding, base: @FieldType(local.metadata_lake_index_catalog.Publication, "base_source")) !void {
    switch (base) {
        .external_parquet => {
            if (binding.format != .parquet) return error.ExternalLakeSnapshotMismatch;
        },
        .external_iceberg => |source| {
            if (binding.format != .iceberg) return error.ExternalLakeSnapshotMismatch;
            binding.snapshot_mode = if (std.mem.startsWith(u8, source.snapshot_id, "empty:")) .current else .{ .snapshot_id = source.snapshot_id };
        },
        .external_lance => |source| {
            if (binding.format != .lance) return error.ExternalLakeSnapshotMismatch;
            binding.snapshot_mode = .{ .snapshot_id = source.snapshot_id };
        },
        else => return error.ExternalLakeSnapshotMismatch,
    }
    binding.write_policy = .read_only;
}

test "external lake retained source binding distinguishes Parquet inventories from Iceberg snapshot selectors" {
    const Source = @FieldType(@FieldType(local.metadata_lake_index_catalog.Publication, "base_source"), "external_parquet");
    const parquet: Source = .{ .format = .parquet_prefix, .source_uri = "s3://bucket/parquet", .snapshot_id = "inventory-digest", .schema_fingerprint = "schema" };
    var binding: local.serverless_external_source_mod.ExternalTableBinding = .{ .table_id = "table", .format = .parquet, .source_uri = parquet.source_uri, .schema_fingerprint = "schema" };
    try pinPublishedSource(&binding, .{ .external_parquet = parquet });
    try std.testing.expect(binding.snapshot_mode == .current);
    binding.snapshot_mode = .{ .object_version_digest = "explicit-digest" };
    try pinPublishedSource(&binding, .{ .external_parquet = parquet });
    try std.testing.expectEqualStrings("explicit-digest", binding.snapshot_mode.object_version_digest);
    var iceberg = parquet;
    iceberg.format = .iceberg;
    iceberg.snapshot_id = "42";
    try std.testing.expectError(error.ExternalLakeSnapshotMismatch, pinPublishedSource(&binding, .{ .external_iceberg = iceberg }));
    binding.format = .iceberg;
    binding.write_policy = .iceberg_writer;
    try pinPublishedSource(&binding, .{ .external_iceberg = iceberg });
    try std.testing.expect(binding.write_policy == .read_only);
    try std.testing.expectEqualStrings("42", binding.snapshot_mode.snapshot_id);
    iceberg.snapshot_id = "empty:table-uuid";
    try pinPublishedSource(&binding, .{ .external_iceberg = iceberg });
    try std.testing.expect(binding.snapshot_mode == .current);
    try std.testing.expectError(error.ExternalLakeSnapshotMismatch, pinPublishedSource(&binding, .{ .external_parquet = parquet }));
    try std.testing.expectError(error.ExternalLakeSnapshotMismatch, pinPublishedSource(&binding, .{ .antfly_document_segments = {} }));
}

test "external lake hydration projection narrows includes and retains exclusion semantics" {
    const a = std.testing.allocator;
    var arena = std.heap.ArenaAllocator.init(a);
    defer arena.deinit();
    const table: local.sql_catalog.Table = .{ .id = 1, .physical_name = "lake", .schema_version = 1, .columns = &.{
        .{ .name = "amount", .path = "amount", .type = .integer },
        .{ .name = "body", .path = "body", .type = .string },
        .{ .name = "nested", .path = "nested.value", .type = .string },
    } };
    const fields = (try projectionColumns(arena.allocator(), table, .{ .fields = &.{ "amount", "nested.*", "-body" } })).?;
    try std.testing.expectEqualSlices([]const u8, &.{ "amount", "nested.value" }, fields);
    try std.testing.expect((try projectionColumns(arena.allocator(), table, .{ .fields = &.{"-body"} })) == null);
    try std.testing.expect((try projectionColumns(arena.allocator(), table, .{})) == null);
    try std.testing.expectEqual(@as(usize, 0), (try projectionColumns(arena.allocator(), table, .{ .include_all_fields = false })).?.len);
    try std.testing.expectEqualSlices([]const u8, &.{"amount"}, (try projectionColumns(arena.allocator(), table, .{ .fields = &.{"amount"}, .defer_stored_projection = true })).?);
    try std.testing.expect(projectionMayUse("*", "body"));
    try std.testing.expect(projectionMayUse("nested", "nested.value"));
    try std.testing.expect(!projectionMayUse("different.*", "nested.value"));
}

test "external lake stored highlight eligibility requires explicit source and complete field coverage" {
    var req: types.SearchRequest = .{ .full_text = .{ .match = .{ .field = "body", .text = "needle" } }, .highlight = .{ .fields = &.{"body"} } };
    try std.testing.expect(!storedHighlightEligible(req, "body", false));
    try std.testing.expect(storedHighlightEligible(req, "body", true));
    req.highlight.?.fields = &.{ "body", "label" };
    try std.testing.expect(!storedHighlightEligible(req, "body", true));
    try std.testing.expect(storedHighlightEligible(req, null, true));
    req.highlight.?.fields = &.{};
    try std.testing.expect(storedHighlightEligible(req, "body", true));
    req.full_text = null;
    try std.testing.expect(!storedHighlightEligible(req, "body", true));
}

test "external lake stored highlights read authenticated native source without Parquet hydration" {
    const a = std.testing.allocator;
    var directory = try local.common_test_directory.TestDirectory.init("lake-stored-highlights");
    defer directory.cleanup();
    var fs = try local.storage_object_storage.FilesystemObjectStorage.init(a, directory.path());
    defer fs.deinit();
    var client = fs.client();
    try client.makeBucket("antfly");
    const data = try local.serverless_query_lake_parquet_rowgroup.buildTestPlainI64AndByteArrayParquetObjectAlloc(a, &.{}, &.{
        .{ .column_id = "body", .field_id = 1, .converted_type = 0, .values = &.{ "a needle in the haystack", "second needle" } },
        .{ .column_id = "label", .field_id = 2, .converted_type = 0, .values = &.{ "returned metadata", "other metadata" } },
    });
    defer a.free(data);
    var put = try client.putObject("antfly", "part.parquet", data, .{});
    put.deinit(a);
    const schema_json = try std.fmt.allocPrint(a,
        \\{{"version":1,"storage_mode":"relational","default_type":"row","base_source":{{"kind":"external","table_id":"lake","format":"parquet","uri":"file://{s}","schema_fingerprint":"schema"}},"document_schemas":{{"row":{{"schema":{{"type":"object","properties":{{"body":{{"type":"string"}},"label":{{"type":"string"}}}},"additionalProperties":false}}}}}}}}
    , .{directory.path()});
    defer a.free(schema_json);
    var binding = (try local.serverless_external_source_schema_binding.externalBindingFromSchemaJsonAlloc(a, schema_json)).?;
    defer binding.deinit(a);
    var source = try local.serverless_query_lake_serving.ServingSource.open(a, .{ .storage_mode = .relational, .external_base_source = binding }, .{});
    defer source.deinit();
    var store = try Store.openNative(a, null, null, false, .standalone, directory.path());
    defer store.deinit();
    var artifacts = store.artifactStore();
    var arena = std.heap.ArenaAllocator.init(a);
    defer arena.deinit();
    const ca = arena.allocator();
    const publication = @import("lake_index_publication.zig");
    var record: local.common_topology_records.TableRecord = .{ .table_id = 7, .name = "lake", .schema_json = schema_json, .indexes_json = "{\"text\":{\"type\":\"full_text\",\"field\":\"body\",\"store_source\":true}}" };
    record.lake_index_catalog_json = try publication.begin(ca, std.testing.io, record, &source, store.identity, .{}, 100, 20);
    const Clock = struct {
        fn now(_: *const anyopaque) !u64 {
            return 101;
        }
    };
    var clock: u8 = 0;
    record.lake_index_catalog_json = try publication.build(ca, &artifacts, record, &source, store.identity, .{ .io = std.testing.io }, .none, .{ .ptr = &clock, .now_ms = Clock.now });
    var catalog = try local.metadata_lake_index_catalog.parse(a, record.lake_index_catalog_json);
    defer catalog.deinit();
    try @import("lake_index_directory.zig").hydrate(catalog.arena.allocator(), artifacts, &catalog.value.published.?, .none, null);
    const declaration = catalog.value.published.?.declarations[0];
    const root = try corpus.loadRoot(ca, artifacts, declaration.artifact, .none, null);
    var writer = try corpus.loadWriter(a, artifacts, root, .none, null, null);
    defer writer.deinit();
    // Exercise hydration in isolation with its cache and instrumentation;
    // the source object is removed after publication construction.
    var status_token: u8 = 0;
    var server = server_api.ApiHttpServer.init(a, .{}, .{ .ptr = &status_token, .vtable = &.{ .status = undefined } }, null, null);
    defer server.deinit();
    var owner: Execution = .{ .server = &server, .table = .{ .id = 7, .physical_name = "lake", .schema_version = 1, .columns = &.{} }, .source = &source, .store = &store, .domain = root.domain, .declarations = &.{}, .context = .{}, .request = .{}, .schema_json = schema_json, .arena = ca, .use_stored_highlights = true };
    defer owner.deinit();
    owner.declarations = &.{declaration};
    owner.use_stored_highlights = false;
    const highlight_request: types.SearchRequest = .{ .index_name = "text", .full_text = .{ .match = .{ .field = "body", .text = "needle" } }, .highlight = .{ .fields = &.{"body"} }, .include_stored = false };
    try std.testing.expectEqual(@as(usize, 0), (try owner.planHydration(highlight_request)).?.len);
    try std.testing.expect(owner.use_stored_highlights);
    try std.testing.expectEqual(@as(usize, 0), owner.highlight_pins.items.len);
    try std.testing.expect(owner.delivery_pin == null);
    const planning_reads = server.lake_read_cache.snapshot().provider_reads;
    const snapshot = writer.acquireSnapshot();
    try owner.highlight_pins.append(ca, .{ .snapshot = snapshot, .name = "text", .text_analysis = .{}, .runtime_schema = null, .selected_field = "body" });
    try owner.text_identities.put(ca, @intFromPtr(snapshot), try @import("lake_index_text_predicate.zig").Identities.init(ca, root, snapshot));
    const file = source.inventory.files[0];
    const id = try local.storage_rowsource_identity.allocId(ca, .{ .external = .{ .source_id = source.inventory.source_id, .snapshot_id = source.inventory.snapshot_id, .file_id = file.file_id, .row_group_ordinal = 0, .row_ordinal = 0 } });
    try owner.files.put(ca, id[6..70], file.file_id);
    const digest = std.fmt.bytesToHex(&root.file_groups[0].file.digest, .lower);
    try owner.private_digests.put(ca, file.file_id, &digest);
    try client.deleteObject("antfly", "part.parquet", .{});
    var hits = [_]types.SearchHit{.{ .id = id, .score = 1 }};
    const stored = (try snapshot.storedDocDecompressed(a, 0)).?;
    defer a.free(stored.data);
    var parsed = try std.json.parseFromSlice(std.json.Value, a, stored.data, .{});
    defer parsed.deinit();
    try std.testing.expectEqualStrings("a needle in the haystack", parsed.value.object.get("body").?.string);
    // Selecting only body for indexing must not discard projected metadata.
    // The Parquet object is already gone, so this is durable source coverage.
    try std.testing.expectEqualStrings("returned metadata", parsed.value.object.get("label").?.string);
    const query: types.TextQuery = .{ .match = .{ .field = "body", .text = "needle" } };
    owner.highlight_queries = &.{.{ .query = query, .text_analysis = .{}, .runtime_schema = null, .selected_field = "body" }};
    var result: types.SearchResult = .{ .alloc = a, .hits = &hits, .total_hits = 1, .graph_results = &.{} };
    defer types.freeHighlights(a, hits[0].highlights);
    try owner.attachHighlights(a, .{ .full_text = query, .highlight = .{ .fields = &.{"body"} }, .include_stored = false }, &result);
    try std.testing.expectEqual(@as(usize, 1), hits[0].highlights.len);
    try std.testing.expectEqual(planning_reads, server.lake_read_cache.snapshot().provider_reads);
    // Paged winners can arrive in reverse physical order. Both documents share
    // one immutable segment; acquire it once and preserve public result order.
    const second_id = try local.storage_rowsource_identity.allocId(ca, .{ .external = .{ .source_id = source.inventory.source_id, .snapshot_id = source.inventory.snapshot_id, .file_id = file.file_id, .row_group_ordinal = 0, .row_ordinal = 1 } });
    try std.testing.expectEqual(@as(usize, 1), root.segments.len);
    const segment = corpus.term_directory.identity(root.segments[0].artifact_id);
    try owner.highlight_locations.put(ca, id, .{ .segment = segment, .document = 0 });
    try owner.highlight_locations.put(ca, second_id, .{ .segment = segment, .document = 1 });
    var grouped_hits = [_]types.SearchHit{ .{ .id = second_id, .score = 2 }, .{ .id = id, .score = 1 } };
    defer for (grouped_hits) |hit| types.freeHighlights(a, hit.highlights);
    var grouped: types.SearchResult = .{ .alloc = a, .hits = &grouped_hits, .total_hits = 2, .graph_results = &.{} };
    const index_phase = @backingInt(@import("lake_query_metrics.zig").Phase.index);
    const acquisitions = server.lake_query_metrics.snapshot().calls[index_phase];
    try owner.attachHighlights(a, .{ .full_text = query, .highlight = .{ .fields = &.{"body"} }, .include_stored = false }, &grouped);
    try std.testing.expectEqual(acquisitions + 1, server.lake_query_metrics.snapshot().calls[index_phase]);
    try std.testing.expectEqualStrings(second_id, grouped_hits[0].id);
    try std.testing.expectEqualStrings(id, grouped_hits[1].id);
    try std.testing.expectEqualStrings("second needle", grouped_hits[0].highlights[0].fragments[0].text);
    try std.testing.expectEqualStrings("a needle in the haystack", grouped_hits[1].highlights[0].fragments[0].text);
    try owner.highlight_locations.put(ca, id, .{ .segment = segment, .document = 1 });
    var invalid_hits = [_]types.SearchHit{.{ .id = id, .score = 1 }};
    var invalid: types.SearchResult = .{ .alloc = a, .hits = &invalid_hits, .total_hits = 1, .graph_results = &.{} };
    try std.testing.expectError(error.InvalidNativeLakeTextCorpus, owner.attachHighlights(a, .{ .full_text = query, .highlight = .{ .fields = &.{"body"} }, .include_stored = false }, &invalid));
    try std.testing.expectEqual(@as(usize, 0), invalid_hits[0].highlights.len);
    owner.context.deadline_ns = 0;
    try std.testing.expectError(error.DeadlineExceeded, owner.attachHighlights(a, .{ .full_text = query, .highlight = .{ .fields = &.{"body"} }, .include_stored = false }, &grouped));
    owner.highlight_locations.clearRetainingCapacity();
    owner.context.deadline_ns = 0;
    try std.testing.expectError(error.DeadlineExceeded, owner.attachHighlights(a, highlight_request, &result));
}

test "external lake deferred search projection retains highlight fields until public encoding" {
    const a = std.testing.allocator;
    const raw = "{\"body\":\"a needle in the source\",\"label\":\"row\"}";
    var req: types.SearchRequest = .{ .fields = &.{"label"}, .include_all_fields = false, .defer_stored_projection = true };
    const deferred = try Execution.project(null, a, req, "row", raw);
    defer a.free(deferred);
    try std.testing.expectEqualStrings(raw, deferred);
    req.defer_stored_projection = false;
    const projected = try Execution.project(null, a, req, "row", raw);
    defer a.free(projected);
    var parsed = try std.json.parseFromSlice(std.json.Value, a, projected, .{});
    defer parsed.deinit();
    try std.testing.expectEqualStrings("row", parsed.value.object.get("label").?.string);
    try std.testing.expect(parsed.value.object.get("body") == null);
}

test "external lake hydration unions returned and highlight fields without unrelated columns" {
    var arena = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer arena.deinit();
    var server: server_api.ApiHttpServer = undefined;
    server.lake_query_metrics = .{};
    var owner: Execution = undefined;
    owner.navigation = null;
    owner.server = &server;
    owner.arena = arena.allocator();
    owner.context = .{};
    owner.declarations = &.{};
    owner.use_stored_highlights = false;
    owner.highlight_queries = null;
    owner.table = .{ .id = 1, .physical_name = "lake", .schema_version = 1, .columns = &.{
        .{ .name = "label", .path = "label", .type = .string },
        .{ .name = "body", .path = "body", .type = .string },
        .{ .name = "unrelated", .path = "unrelated", .type = .string },
    } };
    var req: types.SearchRequest = .{ .fields = &.{"label"}, .include_all_fields = false, .defer_stored_projection = true, .full_text = .{ .match = .{ .field = "body", .text = "needle" } }, .highlight = .{ .fields = &.{"body"} } };
    try std.testing.expectEqualSlices([]const u8, &.{ "label", "body" }, (try owner.planHydration(req)).?);
    req.include_stored = false;
    try std.testing.expectEqualSlices([]const u8, &.{"body"}, (try owner.planHydration(req)).?);
    req.include_stored = true;
    req.highlight = null;
    req.order_by = &.{.{ .field = "body" }};
    try std.testing.expectEqualSlices([]const u8, &.{ "label", "body" }, (try owner.planHydration(req)).?);
    req.filter_query_json = "{}";
    try std.testing.expectEqualSlices([]const u8, &.{ "label", "body" }, (try owner.planHydration(req)).?);
    try std.testing.expect(requiresEncodedSource(req));
}

test "external lake typed sidecar hydration skips Parquet and owns final source fields" {
    const a = std.testing.allocator;
    const key = "lake2:" ++ "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa" ++ ":00000000:0000000000000000";
    const bytes = (try local.storage_db_document_mapper.buildTextSegmentFromDocuments(a, &.{.{ .key = key, .value = "{\"body\":\"a needle in the source\",\"unused\":\"not requested\"}" }}, .{}, null)).?;
    defer a.free(bytes);
    var writer = try local.index.IndexWriter.init(a);
    defer writer.deinit();
    try writer.addSegmentWithId(1, bytes);
    var arena = std.heap.ArenaAllocator.init(a);
    defer arena.deinit();
    var owner: Execution = undefined;
    owner.navigation = null;
    owner.arena = arena.allocator();
    owner.context = .{ .io = std.testing.io };
    owner.hydration_fields = &.{"body"};
    owner.result_allocator = a;
    // No public/native row mapping or source reader exists. A successful
    // result proves the covered body did not open a Parquet hydration cursor.
    owner.files = .empty;
    owner.private_files = .empty;
    owner.private_digests = .empty;
    const pin: search.PinnedTextSource = .{ .snapshot = writer.snapshot(), .name = "body", .text_analysis = .{}, .runtime_schema = null, .stored_projection_fields = &.{"body"} };
    var hits = [_]types.SearchHit{.{ .id = try a.dupe(u8, key), .native_text_doc_id = 0 }};
    defer hits[0].deinit(a);
    try owner.hydrateTypedFromSource(a, &hits, &.{key}, &pin);
    try std.testing.expectEqualStrings("a needle in the source", hits[0].source_value.?.object.get("body").?.string);
    try std.testing.expect(hits[0].source_value.?.object.get("unused") == null);
    try std.testing.expect(hits[0].stored_data == null);
    Execution.releaseDelivery(&owner, &hits);
    try std.testing.expect(hits[0].source_value == null);
}

test "external lake typed delivery separates final projection from residual predicates and pagination" {
    var req: types.SearchRequest = .{ .full_text = .{ .match = .{ .field = "body", .text = "needle" } }, .fields = &.{"label"}, .highlight = .{ .fields = &.{"body"} } };
    try std.testing.expect(canDeliverTypedSource(req));
    req.filter_query_json = "{}";
    try std.testing.expect(canDeliverTypedSource(req));
    try std.testing.expect(requiresEncodedSource(req));
    req.search_after = &.{ .{ .float = 1 }, .{ .string = "id" } };
    try std.testing.expect(canDeliverTypedSource(req));
    req.search_after = &.{};
    req.search_before = &.{ .{ .float = 1 }, .{ .string = "id" } };
    try std.testing.expect(canDeliverTypedSource(req));
    req.search_before = &.{};
    req.filter_query_json = "";
    req.order_by = &.{.{ .field = "amount" }};
    try std.testing.expect(canDeliverTypedSource(req));
    req.order_by = &.{};
    req.hierarchy_include_source = true;
    try std.testing.expect(!canDeliverTypedSource(req));
    req.hierarchy_include_source = false;
    req.evaluation_limit = 1;
    try std.testing.expect(!canDeliverTypedSource(req));
}

test "external lake bounded transforms project dependencies before candidate admission" {
    const a = std.testing.allocator;
    var arena = std.heap.ArenaAllocator.init(a);
    defer arena.deinit();
    var owner: Execution = .{ .server = undefined, .source = undefined, .store = undefined, .domain = @splat(0), .declarations = &.{}, .context = .{}, .request = .{}, .schema_json = "", .arena = arena.allocator(), .table = .{ .id = 1, .physical_name = "lake", .schema_version = 1, .columns = &.{
        .{ .name = "body", .path = "body", .type = .string },
        .{ .name = "amount", .path = "amount", .type = .integer },
        .{ .name = "unused", .path = "unused", .type = .string },
    } } };
    var req: types.SearchRequest = .{ .limit = 1, .fields = &.{"amount"}, .evaluation_limit = 201, .evaluation_json =
        \\{"scope":"candidates","candidate_count":201,"compute":{"tiny":{"field":"body"}}}
    };
    const inputs = try owner.planTransformInputs(req);
    try std.testing.expectEqual(@as(usize, 1), inputs.fields.?.len);
    try std.testing.expectEqualStrings("body", inputs.fields.?[0]);
    try std.testing.expect(!inputs.covers_output);
    req.fields = &.{"body"};
    try std.testing.expect((try owner.planTransformInputs(req)).covers_output);
    req.evaluation_limit = 0;
    req.reranker = .{ .provider = .antfly, .field = "body" };
    const direct = try owner.planTransformInputs(req);
    try std.testing.expectEqualStrings("body", direct.fields.?[0]);
    req.reranker.?.template = "{{json this}}";
    try std.testing.expect((try owner.planTransformInputs(req)).fields == null);
}

test "external lake bounded transforms retain global candidate windows and source dependencies" {
    var req: types.SearchRequest = .{ .limit = 7, .offset = 3, .reranker = .{ .provider = .antfly, .field = "hidden", .candidate_count = 31 } };
    const paging = local.storage_db_query_control_contract.componentPaging(req);
    try std.testing.expectEqual(@as(u32, 0), paging.offset);
    try std.testing.expectEqual(@as(u32, 31), paging.limit);
    try std.testing.expect(canDeliverTypedSource(retrievalRequest(req)));
    try std.testing.expect(needsCandidateSource(req));
    req.reranker = null;
    req.pruner = .{ .min_score_ratio = 0.75 };
    try std.testing.expect(canDeliverTypedSource(retrievalRequest(req)));
    try std.testing.expect(!needsCandidateSource(req));
    req.pruner = null;
    req.evaluation_limit = 31;
    try std.testing.expect(canDeliverTypedSource(retrievalRequest(req)));
    try std.testing.expect(needsCandidateSource(req));
    req.evaluation_matches = true;
    try std.testing.expect(canDeliverTypedSource(retrievalRequest(req)));
    try std.testing.expect(needsCandidateSource(req));
}

test "external lake sparse predicate planning defers broad masks and subtracts selective includes" {
    const a = std.testing.allocator;
    const Set = @import("lake_index_physical_set.zig").Set;
    // Only the predicate-planning fields are needed; no HTTP/server is opened.
    var source: local.serverless_query_lake_serving.ServingSource = undefined;
    source.row_resolver = null;
    var execution: Execution = undefined;
    execution.index_resolver = null;
    execution.source = &source;
    execution.result_allocator = a;
    execution.navigation = null;
    execution.overlay = null;
    execution.active_recent = false;
    execution.vector_include = null;
    execution.vector_include_provider = null;
    execution.vector_exclude_provider = null;
    execution.vector_exclude = Set.init(a);
    defer execution.vector_exclude.?.deinit();
    execution.private_digests = .empty;
    defer execution.private_digests.deinit(a);
    execution.context = .{ .io = std.testing.io };
    const digest: [64]u8 = @splat('0');
    try execution.private_digests.put(a, "file", &digest);
    try execution.vector_exclude.?.addBlock(.{ .file = "file", .group = 0, .base = 0, .selection = .{ .interval = .{ .lower = 0, .count = 100000 } } });
    const Lookup = struct {
        calls: usize = 0,
        fn one(raw: *anyopaque, _: []const u8) !?u32 {
            const self: *@This() = @ptrCast(@alignCast(raw));
            self.calls += 1;
            return 42;
        }
        fn block(_: *anyopaque, _: A, _: []const u8, _: u32, _: *const local.encoding_roaring.RoaringBitmap, _: *local.encoding_roaring.RoaringBitmap) !bool {
            return false;
        }
    };
    var lookup: Lookup = .{};
    const native: types.SparseOrdinalLookup = .{ .ptr = &lookup, .one = Lookup.one, .block = Lookup.block };
    var broad = (try Execution.selectSparseConstraints(&execution, a, native)).?;
    defer broad.deinit();
    try std.testing.expect(broad.residual and broad.include == null and broad.exclude == null);
    try std.testing.expectEqual(@as(usize, 0), lookup.calls);
    execution.vector_include = Set.init(a);
    defer execution.vector_include.?.deinit();
    try execution.vector_include.?.addRow("file", 0, 7);
    var empty = (try Execution.selectSparseConstraints(&execution, a, native)).?;
    defer empty.deinit();
    try std.testing.expect(!empty.residual and empty.include.?.isEmpty());
    try std.testing.expectEqual(@as(usize, 0), lookup.calls);
    try execution.vector_include.?.addRow("file", 0, 150000);
    var point = (try Execution.selectSparseConstraints(&execution, a, native)).?;
    defer point.deinit();
    try std.testing.expect(!point.residual and point.include.?.contains(42));
    try std.testing.expectEqual(@as(usize, 1), lookup.calls);
}

test "external lake sparse predicate planning translates broad blocks within work and byte caps" {
    const a = std.testing.allocator;
    const Bitmap = local.encoding_roaring.RoaringBitmap;
    const Set = @import("lake_index_physical_set.zig").Set;
    var source: local.serverless_query_lake_serving.ServingSource = undefined;
    source.row_resolver = null;
    var execution: Execution = undefined;
    execution.index_resolver = null;
    execution.source = &source;
    execution.result_allocator = a;
    execution.navigation = null;
    execution.overlay = null;
    execution.active_recent = false;
    execution.vector_include_provider = null;
    execution.vector_exclude_provider = null;
    execution.vector_include = null;
    execution.vector_exclude = Set.init(a);
    defer execution.vector_exclude.?.deinit();
    execution.private_digests = .empty;
    defer execution.private_digests.deinit(a);
    execution.context = .{ .io = std.testing.io };
    const digest: [64]u8 = @splat('0');
    try execution.private_digests.put(a, "file", &digest);
    try execution.vector_exclude.?.addBlock(.{ .file = "file", .group = 0, .base = 0, .selection = .{ .interval = .{ .lower = 0, .count = 100000 } } });
    const Lookup = struct {
        mode: enum { complete, work, memory } = .complete,
        blocks: usize = 0,
        fn one(_: *anyopaque, _: []const u8) !?u32 {
            return error.UnexpectedPointSeek;
        }
        fn legacy(_: *anyopaque, _: A, _: []const u8, _: u32, _: *const Bitmap, _: *Bitmap) !bool {
            return error.UnexpectedLegacyBlock;
        }
        fn block(raw: *anyopaque, alloc: A, _: []const u8, _: u32, rows: *const Bitmap, result: *Bitmap, work: *types.SparseOrdinalWorkBudget) !bool {
            const self: *@This() = @ptrCast(@alignCast(raw));
            self.blocks += 1;
            try work.takeBlock();
            if (self.mode != .complete) try result.add(42);
            switch (self.mode) {
                .work => return error.OrdinalPlanningBudgetExceeded,
                .memory => {
                    const oversized = try alloc.alloc(u8, 4 * 1024 * 1024 + 1);
                    defer alloc.free(oversized);
                },
                .complete => {},
            }
            try result.orWith(rows);
            return true;
        }
    };
    var lookup: Lookup = .{};
    const native: types.SparseOrdinalLookup = .{ .ptr = &lookup, .one = Lookup.one, .block = Lookup.legacy, .bounded_block = Lookup.block };
    var result = (try Execution.selectSparseConstraints(&execution, a, native)).?;
    defer result.deinit();
    try std.testing.expect(!result.residual and result.include == null);
    try std.testing.expectEqual(@as(usize, 100000), result.exclude.?.cardinality());
    try std.testing.expect(result.budget.?.live < 128 * 1024);
    try std.testing.expectEqual(@as(usize, 1), lookup.blocks);
    for ([_]@TypeOf(lookup.mode){ .work, .memory }) |mode| {
        lookup.mode = mode;
        var fallback = (try Execution.selectSparseConstraints(&execution, a, native)).?;
        defer fallback.deinit();
        try std.testing.expect(fallback.residual and fallback.include == null and fallback.exclude == null and fallback.budget == null);
    }
    lookup.mode = .complete;
    const Failure = struct {
        fn run(alloc: A, owner: *Execution, resolve: types.SparseOrdinalLookup) !void {
            var selection = (try Execution.selectSparseConstraints(owner, alloc, resolve)).?;
            defer selection.deinit();
            try std.testing.expect(!selection.residual);
            try std.testing.expectEqual(@as(usize, 100000), selection.exclude.?.cardinality());
        }
    };
    try std.testing.checkAllAllocationFailures(a, Failure.run, .{ &execution, native });
    // Subtract a broad exclusion before choosing legacy point work. A broad
    // include can have a one-row difference and must still get an exact seek.
    execution.vector_include = Set.init(a);
    defer execution.vector_include.?.deinit();
    try execution.vector_include.?.addBlock(.{ .file = "file", .group = 0, .base = 0, .selection = .{ .interval = .{ .lower = 0, .count = 100001 } } });
    var difference = (try Execution.selectSparseConstraints(&execution, a, native)).?;
    defer difference.deinit();
    try std.testing.expect(!difference.residual);
    try std.testing.expectEqual(@as(usize, 1), difference.include.?.cardinality());
    try std.testing.expect(difference.include.?.contains(100000));
}

test "external lake sparse predicate planning defers exhausted masks to exact native windows" {
    const a = std.testing.allocator;
    const Bitmap = local.encoding_roaring.RoaringBitmap;
    const Set = @import("lake_index_physical_set.zig").Set;
    var execution: Execution = undefined;
    execution.index_resolver = null;
    execution.navigation = null;
    execution.overlay = null;
    execution.active_recent = false;
    var source: local.serverless_query_lake_serving.ServingSource = undefined;
    source.row_resolver = null;
    source.inventory.row_identity_version = 1;
    execution.result_allocator = a;
    source.inventory.source_id = @constCast("source");
    source.inventory.snapshot_id = @constCast("snapshot");
    execution.source = &source;
    execution.vector_include_provider = null;
    execution.vector_exclude_provider = null;
    execution.vector_include = Set.init(a);
    defer execution.vector_include.?.deinit();
    execution.vector_exclude = Set.init(a);
    defer execution.vector_exclude.?.deinit();
    execution.private_files = .empty;
    defer execution.private_files.deinit(a);
    execution.private_digests = .empty;
    defer execution.private_digests.deinit(a);
    execution.context = .{ .io = std.testing.io };
    const digest: [64]u8 = @splat('0');
    try execution.private_files.put(a, &digest, "file");
    try execution.private_digests.put(a, "file", &digest);
    try execution.vector_include.?.addBlock(.{ .file = "file", .group = 0, .base = 0, .selection = .{ .interval = .{ .lower = 0, .count = 100000 } } });
    try execution.vector_exclude.?.addBlock(.{ .file = "file", .group = 0, .base = 0, .selection = .{ .interval = .{ .lower = 0, .count = 1001 } } });
    const Lookup = struct {
        available: bool = true,
        calls: usize = 0,
        directory_calls: usize = 0,
        fn one(_: *anyopaque, _: []const u8) !?u32 {
            return error.UnexpectedPointSeek;
        }
        fn block(_: *anyopaque, _: A, _: []const u8, _: u32, _: *const Bitmap, _: *Bitmap) !bool {
            return error.UnexpectedLegacyBlock;
        }
        fn bounded(raw: *anyopaque, _: A, _: []const u8, _: u32, _: *const Bitmap, result: *Bitmap, work: *types.SparseOrdinalWorkBudget) !bool {
            const self: *@This() = @ptrCast(@alignCast(raw));
            self.directory_calls += 1;
            try result.add(1);
            for (0..65) |_| try work.takeBlock();
            return error.ExpectedPlanningCap;
        }
        fn range(raw: *anyopaque, _: A, first: u32, last: u32, filter: types.SparseOrdinalKeyFilter, result: *Bitmap) !bool {
            const self: *@This() = @ptrCast(@alignCast(raw));
            self.calls += 1;
            if (!self.available) return false;
            var row = first;
            while (row <= last) : (row += 1) {
                var key: [96]u8 = undefined;
                const encoded = try std.fmt.bufPrint(&key, "lake2:{s}:00000000:{x:0>16}", .{ @as([64]u8, @splat('0')), row });
                if (try filter.allowsKey(encoded)) try result.add(row);
            }
            return true;
        }
    };
    var lookup: Lookup = .{};
    const native: types.SparseOrdinalLookup = .{ .ptr = &lookup, .one = Lookup.one, .block = Lookup.block, .bounded_block = Lookup.bounded, .native_range = Lookup.range };
    var deferred = (try Execution.selectSparseConstraints(&execution, a, native)).?;
    defer deferred.deinit();
    try std.testing.expect(deferred.deferred and deferred.residual and deferred.include == null and deferred.exclude == null and deferred.budget == null);
    try std.testing.expectEqual(@as(usize, 0), lookup.directory_calls);
    var window = (try Execution.selectSparseRange(&execution, a, native, 1000, 2023)).?;
    defer window.deinit();
    try window.prepareRead();
    try std.testing.expect(!window.residual and !window.deferred);
    try std.testing.expectEqual(@as(usize, 1023), window.include.?.cardinality());
    try std.testing.expect(!window.include.?.contains(1000));
    try std.testing.expect(window.include.?.contains(1001));
    const Failure = struct {
        fn run(alloc: A, owner: *Execution, resolve: types.SparseOrdinalLookup) !void {
            var selected = (try Execution.selectSparseRange(owner, alloc, resolve, 1000, 2023)).?;
            defer selected.deinit();
            try selected.prepareRead();
            try std.testing.expectEqual(@as(usize, 1023), selected.include.?.cardinality());
        }
    };
    try std.testing.checkAllAllocationFailures(a, Failure.run, .{ &execution, native });
    lookup.available = false;
    try std.testing.expect(try Execution.selectSparseRange(&execution, a, native, 1000, 2023) == null);
}

test "external lake sparse predicate planning preflights effective windows and preserves fragmented point seeks" {
    const a = std.testing.allocator;
    const Bitmap = local.encoding_roaring.RoaringBitmap;
    const Set = @import("lake_index_physical_set.zig").Set;
    var source: local.serverless_query_lake_serving.ServingSource = undefined;
    source.row_resolver = null;
    var execution: Execution = undefined;
    execution.index_resolver = null;
    execution.source = &source;
    execution.result_allocator = a;
    execution.navigation = null;
    execution.overlay = null;
    execution.active_recent = false;
    execution.vector_include_provider = null;
    execution.vector_exclude_provider = null;
    execution.vector_include = Set.init(a);
    defer execution.vector_include.?.deinit();
    execution.vector_exclude = null;
    execution.private_digests = .empty;
    defer execution.private_digests.deinit(a);
    execution.context = .{ .io = std.testing.io };
    const digest: [64]u8 = @splat('0');
    try execution.private_digests.put(a, "file", &digest);
    const Lookup = struct {
        blocks: usize = 0,
        points: usize = 0,
        fn one(raw: *anyopaque, key: []const u8) !?u32 {
            const self: *@This() = @ptrCast(@alignCast(raw));
            self.points += 1;
            return @intCast(try std.fmt.parseUnsigned(u64, key[80..], 16));
        }
        fn legacy(_: *anyopaque, _: A, _: []const u8, _: u32, _: *const Bitmap, _: *Bitmap) !bool {
            return error.UnexpectedLegacyBlock;
        }
        fn bounded(raw: *anyopaque, _: A, _: []const u8, _: u32, rows: *const Bitmap, result: *Bitmap, work: *types.SparseOrdinalWorkBudget) !bool {
            const self: *@This() = @ptrCast(@alignCast(raw));
            var it = rows.iterator();
            while (it.next()) |row| {
                try work.takeBlock();
                self.blocks += 1;
                const base = row & ~@as(u32, 1023);
                if (base == std.math.maxInt(u32) - 1023) break;
                it.seek(base + 1024);
            }
            try result.orWith(rows);
            return true;
        }
        fn range(_: *anyopaque, _: A, _: u32, _: u32, _: types.SparseOrdinalKeyFilter, _: *Bitmap) !bool {
            return error.UnexpectedRange;
        }
    };
    var lookup: Lookup = .{};
    const native: types.SparseOrdinalLookup = .{ .ptr = &lookup, .one = Lookup.one, .block = Lookup.legacy, .bounded_block = Lookup.bounded, .native_range = Lookup.range };
    try execution.vector_include.?.addBlock(.{ .file = "file", .group = 0, .base = 0, .selection = .{ .interval = .{ .lower = 0, .count = 65536 } } });
    var boundary = (try Execution.selectSparseConstraints(&execution, a, native)).?;
    defer boundary.deinit();
    try std.testing.expectEqual(@as(usize, 64), lookup.blocks);
    try std.testing.expectEqual(@as(usize, 65536), boundary.include.?.cardinality());
    try execution.vector_include.?.addRow("file", 0, 65536);
    lookup.blocks = 0;
    var broad = (try Execution.selectSparseConstraints(&execution, a, native)).?;
    defer broad.deinit();
    try std.testing.expect(broad.deferred and broad.include == null and broad.residual);
    try std.testing.expectEqual(@as(usize, 0), lookup.blocks);
    execution.vector_exclude = Set.init(a);
    defer execution.vector_exclude.?.deinit();
    try execution.vector_exclude.?.addBlock(.{ .file = "file", .group = 0, .base = 0, .selection = .{ .interval = .{ .lower = 0, .count = 65536 } } });
    var selective = (try Execution.selectSparseConstraints(&execution, a, native)).?;
    defer selective.deinit();
    try std.testing.expect(!selective.residual);
    try std.testing.expectEqual(@as(usize, 1), lookup.blocks);
    try std.testing.expectEqual(@as(usize, 1), selective.include.?.cardinality());
    try std.testing.expect(selective.include.?.contains(65536));
    // Sparse rows in many directory windows use cheap exact point seeks.
    execution.vector_include.?.deinit();
    execution.vector_include = Set.init(a);
    for (0..65) |i| try execution.vector_include.?.addRow("file", 0, 100000 + 1024 * i);
    lookup.blocks = 0;
    var fragmented = (try Execution.selectSparseConstraints(&execution, a, native)).?;
    defer fragmented.deinit();
    try std.testing.expect(!fragmented.residual);
    try std.testing.expectEqual(@as(usize, 0), lookup.blocks);
    try std.testing.expectEqual(@as(usize, 65), lookup.points);
    try std.testing.expectEqual(@as(usize, 65), fragmented.include.?.cardinality());
    // File/group/high-row coordinates each have independent directory windows.
    var coordinates = Set.init(a);
    defer coordinates.deinit();
    try coordinates.addRow("file", 0, std.math.maxInt(u64));
    try coordinates.addRow("file", 1, std.math.maxInt(u64));
    try coordinates.addRow("other", 0, std.math.maxInt(u32));
    try std.testing.expectEqual(Execution.SparsePlan.points, try execution.sparseDirectoryPlan(a, &coordinates, null, 2, 3));
    try std.testing.expectEqual(Execution.SparsePlan.directory, try execution.sparseDirectoryPlan(a, &coordinates, null, 3, 3));
    const Failure = struct {
        fn run(alloc: A, owner: *Execution, resolve: types.SparseOrdinalLookup) !void {
            var selected = (try Execution.selectSparseConstraints(owner, alloc, resolve)).?;
            defer selected.deinit();
            try std.testing.expect(!selected.residual);
            try std.testing.expectEqual(@as(usize, 65), selected.include.?.cardinality());
        }
    };
    try std.testing.checkAllAllocationFailures(a, Failure.run, .{ &execution, native });
}

/// Global score ties use the immutable corpus ordinal, never the physical file
/// name or a window-local doc number. Retains only offset + limit owned hits.
const StoredHighlightLocation = struct { segment: [32]u8, document: u32 };
fn storedHighlightLocation(pin: search.PinnedTextSource, ids: []const [32]u8, document: u32) !StoredHighlightLocation {
    if (pin.snapshot.segments.len != ids.len) return error.InvalidNativeLakeTextCorpus;
    var lower: u32 = 0;
    const admitted = if (pin.provider_metadata) |raw| @as(*const @import("lake_index_text_predicate.zig").Identities, @ptrCast(@alignCast(raw))).segment_ids else &.{};
    if (admitted.len != 0 and admitted.len != ids.len) return error.InvalidNativeLakeTextCorpus;
    for (pin.snapshot.segments, ids, 0..) |segment, supplied, ordinal| {
        const id = if (admitted.len != 0) admitted[ordinal] else supplied;
        const upper = try std.math.add(u32, lower, segment.reader.doc_count);
        if (document >= lower and document < upper) return .{ .segment = id, .document = document - lower };
        lower = upper;
    }
    return error.InvalidNativeLakeTextCorpus;
}

/// Window-local counters describe all admitted work. Capacities/peaks describe
/// the maximum concurrent window; labels retain the first executor's owned
/// contract and the coordinator supplies its admission reason separately.
fn mergeWindowProfile(target: *types.SortProfile, source: types.SortProfile) void {
    inline for (@typeInfo(types.SortProfile).@"struct".field_names, @typeInfo(types.SortProfile).@"struct".field_types) |name, T| {
        if (@typeInfo(T) == .int) {
            if (std.mem.eql(u8, name, "window_capacity") or std.mem.eql(u8, name, "window_len") or std.mem.eql(u8, name, "collector_heap_peak") or std.mem.eql(u8, name, "distributed_shard_count") or std.mem.eql(u8, name, "distributed_shard_window")) {
                @field(target, name) = @max(@field(target, name), @field(source, name));
            } else @field(target, name) +|= @field(source, name);
        } else if (T == bool) {
            @field(target, name) = if (std.mem.eql(u8, name, "selective_filter_doc_values_preferred")) @field(target, name) or @field(source, name) else @field(target, name) and @field(source, name);
        } else if (T == []const u8) {
            if (!std.mem.eql(u8, @field(target, name), @field(source, name))) @field(target, name) = "mixed_window_plans";
        }
    }
}

fn scoreFrontierEligible(req: types.SearchRequest, ordering: ?*const local.storage_db_query_search_exec.SearchHitOrder) bool {
    if (req.count_only or req.distributed_text_stats.len != 0 or req.search_before.len != 0) return false;
    const effective = if (ordering) |order| order.effective.req else return true;
    return effective.order_by.len != 0 and std.mem.eql(u8, effective.order_by[0].field, "_score") and effective.order_by[0].desc;
}

const PagedWinners = struct {
    const Winner = struct {
        hit: types.SearchHit,
        ordinal: u64,
        position: ?@import("lake_index_text_directory.zig").Position = null,
        highlight: ?StoredHighlightLocation = null,
        fn worst(ordering: ?*const local.storage_db_query_search_exec.SearchHitOrder, l: Winner, r: Winner) std.math.Order {
            if (ordering) |typed| {
                const compared = typed.compare(l.hit, r.hit);
                return if (typed.effective.req.search_before.len != 0) compared else compared.invert();
            }
            const order = std.math.order(l.hit.score orelse 0, r.hit.score orelse 0);
            if (order != .eq) return order;
            if (l.position) |left| {
                const right = r.position.?;
                if (left.recent != right.recent) return if (left.recent) .lt else .gt;
                const file = std.mem.order(u8, &right.file, &left.file);
                return if (file == .eq) std.math.order(right.rank, left.rank) else file;
            }
            return std.math.order(r.ordinal, l.ordinal);
        }
    };
    a: A,
    limit: usize,
    peak: usize = 0,
    heap: std.PriorityQueue(Winner, ?*const local.storage_db_query_search_exec.SearchHitOrder, Winner.worst) = .initContext(null),
    fn init(a: A, limit: usize) PagedWinners {
        return initOrdered(a, limit, null);
    }
    fn initOrdered(a: A, limit: usize, ordering: ?*const local.storage_db_query_search_exec.SearchHitOrder) PagedWinners {
        return .{ .a = a, .limit = limit, .heap = .initContext(ordering) };
    }
    fn cutoff(self: *const PagedWinners) ?f32 {
        if (self.limit == 0 or self.heap.items.len < self.limit) return null;
        if (self.heap.context) |order| {
            const req = order.effective.req;
            // Lower score cannot outrank a leading descending score. Keep equal
            // scores for secondary/tie ordering; backward pages have another cutoff.
            if (req.search_before.len != 0 or req.order_by.len == 0 or
                !std.mem.eql(u8, req.order_by[0].field, "_score") or !req.order_by[0].desc) return null;
        }
        return self.heap.peek().?.hit.score orelse 0;
    }
    fn tieCutoff(self: *const PagedWinners) ?@import("lake_index_text_directory.zig").Position {
        if (self.heap.context != null or self.cutoff() == null) return null;
        return self.heap.peek().?.position;
    }
    fn deinit(self: *PagedWinners) void {
        while (self.heap.pop()) |entry| {
            var hit = entry.hit;
            hit.deinit(self.a);
        }
        self.heap.deinit(self.a);
    }
    fn add(self: *PagedWinners, hit: types.SearchHit, ordinal: u64) !void {
        return self.addLocated(hit, ordinal, null);
    }
    fn addLocated(self: *PagedWinners, hit: types.SearchHit, ordinal: u64, highlight: ?StoredHighlightLocation) !void {
        return self.addWinner(hit, ordinal, null, highlight);
    }
    fn addWindowHit(self: *PagedWinners, hit: types.SearchHit, ordinal: u64, identities: ?*const @import("lake_index_text_predicate.zig").Identities, recent: bool, highlight: ?StoredHighlightLocation) !void {
        if (self.heap.context != null) return self.addLocated(hit, ordinal, highlight);
        const doc = hit.native_text_doc_id orelse return error.InvalidNativeLakeTextCorpus;
        const position: @import("lake_index_text_directory.zig").Position = if (recent)
            .{ .file = @splat(0), .rank = doc, .recent = true }
        else
            try (identities orelse return error.InvalidNativeLakeTextCorpus).logicalPosition(doc);
        return self.addPositioned(hit, position, highlight);
    }
    fn addPositioned(self: *PagedWinners, hit: types.SearchHit, position: @import("lake_index_text_directory.zig").Position, highlight: ?StoredHighlightLocation) !void {
        return self.addWinner(hit, 0, position, highlight);
    }
    fn addWinner(self: *PagedWinners, hit: types.SearchHit, ordinal: u64, position: ?@import("lake_index_text_directory.zig").Position, highlight: ?StoredHighlightLocation) !void {
        if (self.limit == 0) return;
        if (!std.math.isFinite(hit.score orelse 0)) return error.InvalidNativeLakeTextCorpus;
        if (self.heap.context) |order| try order.validate(hit);
        const candidate: Winner = .{ .hit = hit, .ordinal = ordinal, .position = position };
        if (self.heap.items.len == self.limit and Winner.worst(self.heap.context, candidate, self.heap.peek().?) != .gt) return;
        var owned = try hit.clone(self.a);
        errdefer owned.deinit(self.a);
        // Window-local identities cannot outlive the admission pin. Final
        // physical hydration uses the public key and the leased publication.
        owned.doc_ordinal = null;
        owned.native_text_doc_id = null;
        try self.heap.ensureUnusedCapacity(self.a, 1);
        if (self.heap.items.len == self.limit) {
            var removed = self.heap.pop().?.hit;
            removed.deinit(self.a);
        }
        try self.heap.push(self.a, .{ .hit = owned, .ordinal = ordinal, .position = position, .highlight = highlight });
        self.peak = @max(self.peak, self.heap.items.len);
    }
    fn finish(self: *PagedWinners, offset: usize) ![]types.SearchHit {
        return self.finishLocated(offset, null, self.a);
    }
    fn finishLocated(self: *PagedWinners, offset: usize, locations: ?*std.StringHashMapUnmanaged(StoredHighlightLocation), metadata: A) ![]types.SearchHit {
        const count = self.heap.items.len;
        const skip = @min(offset, count);
        if (locations) |map| try map.ensureUnusedCapacity(metadata, std.math.cast(u32, count - skip) orelse return error.QueryCandidateBudgetExceeded);
        const hits = try self.a.alloc(types.SearchHit, count - skip);
        var remaining = count;
        while (self.heap.pop()) |entry| {
            remaining -= 1;
            if (remaining < skip) {
                var hit = entry.hit;
                hit.deinit(self.a);
            } else {
                hits[remaining - skip] = entry.hit;
                if (locations) |map| if (entry.highlight) |location| map.putAssumeCapacity(entry.hit.id, location);
            }
        }
        if (self.heap.context) |order| if (order.effective.req.search_before.len != 0) std.mem.reverse(types.SearchHit, hits);
        return hits;
    }
};

test "external lake paged native text winners retain global ties and offset across windows" {
    const a = std.testing.allocator;
    var heap = PagedWinners.init(a, 3);
    defer heap.deinit();
    var id = [_]u8{'x'};
    // Reversed arrival and deliberately conflicting local doc IDs.
    try heap.add(.{ .id = &id, .score = 2, .native_text_doc_id = 0 }, 90);
    try heap.add(.{ .id = &id, .score = 2, .native_text_doc_id = 1 }, 50);
    id[0] = 'a';
    try heap.add(.{ .id = &id, .score = 2, .native_text_doc_id = 5 }, 5);
    id[0] = 'b';
    try heap.add(.{ .id = &id, .score = 3, .native_text_doc_id = 10 }, 100);
    id[0] = 'c';
    try heap.add(.{ .id = &id, .score = 1 }, 1);
    const hits = try heap.finish(1);
    defer a.free(hits);
    defer for (hits) |*hit| hit.deinit(a);
    try std.testing.expectEqual(@as(usize, 2), hits.len);
    try std.testing.expectEqualStrings("a", hits[0].id);
    try std.testing.expectEqualStrings("x", hits[1].id);
    try std.testing.expect(hits[0].native_text_doc_id == null);
    try std.testing.expectEqual(@as(usize, 0), heap.heap.items.len);
}

test "external lake paged native text winners share typed ordering and backward cursor direction" {
    const a = std.testing.allocator;
    const fields = [_]types.SortField{.{ .field = "_id" }};
    const bound = [_]std.json.Value{.{ .string = "d" }};
    for ([_]bool{ false, true }) |backward| {
        var order = try local.storage_db_query_search_exec.SearchHitOrder.init(a, .{ .order_by = &fields, .search_before = if (backward) &bound else &.{} }, null);
        defer order.deinit(a);
        var heap = PagedWinners.initOrdered(a, 2, &order);
        defer heap.deinit();
        for ([_][]const u8{ "b", "c", "a" }, 0..) |id, ordinal| {
            const values = [_]std.json.Value{.{ .string = id }};
            try heap.add(.{ .id = @constCast(id), .score = 1, .sort_values = @constCast(&values) }, ordinal);
        }
        const hits = try heap.finish(0);
        defer a.free(hits);
        defer for (hits) |*hit| hit.deinit(a);
        try std.testing.expectEqual(@as(usize, 2), hits.len);
        try std.testing.expectEqualStrings(if (backward) "b" else "a", hits[0].id);
        try std.testing.expectEqualStrings(if (backward) "c" else "b", hits[1].id);
        try std.testing.expect(heap.cutoff() == null);
    }
}

test "external lake global frontier explicit score cutoff preserves ties and rejects ascending or backward pages" {
    const a = std.testing.allocator;
    for ([_]bool{ true, false }) |descending| {
        const fields = [_]types.SortField{.{ .field = "_score", .desc = descending }};
        var order = try local.storage_db_query_search_exec.SearchHitOrder.init(a, .{ .order_by = &fields }, null);
        defer order.deinit(a);
        try std.testing.expectEqual(descending, scoreFrontierEligible(order.effective.req, &order));
        var heap = PagedWinners.initOrdered(a, 1, &order);
        defer heap.deinit();
        var values = try a.alloc(std.json.Value, order.effective.req.order_by.len);
        defer a.free(values);
        for (values, order.effective.req.order_by) |*value, field| value.* = if (std.mem.eql(u8, field.field, "_score")) .{ .float = 2 } else .{ .string = "z" };
        try heap.add(.{ .id = @constCast("z"), .score = 2, .sort_values = values }, 0);
        if (descending) {
            try std.testing.expectEqual(@as(?f32, 2), heap.cutoff());
            values[values.len - 1] = .{ .string = "a" };
            try heap.add(.{ .id = @constCast("a"), .score = 2, .sort_values = values }, 1);
            try std.testing.expectEqualStrings("a", heap.heap.peek().?.hit.id);
        } else try std.testing.expect(heap.cutoff() == null);
        order.effective.req.search_before = values;
        try std.testing.expect(heap.cutoff() == null);
        try std.testing.expect(!scoreFrontierEligible(order.effective.req, &order));
    }
}

test "external lake paged stored highlights retain only returned winner locations" {
    const a = std.testing.allocator;
    var heap = PagedWinners.init(a, 2);
    defer heap.deinit();
    for ([_][]const u8{ "a", "b", "c" }, 0..) |id, ordinal| try heap.addLocated(.{ .id = @constCast(id), .score = @floatFromInt(3 - ordinal) }, ordinal, .{ .segment = @splat(@intCast(ordinal)), .document = @intCast(ordinal + 7) });
    var locations: std.StringHashMapUnmanaged(StoredHighlightLocation) = .empty;
    defer locations.deinit(a);
    const hits = try heap.finishLocated(1, &locations, a);
    defer a.free(hits);
    defer for (hits) |*hit| hit.deinit(a);
    try std.testing.expectEqual(@as(usize, 1), hits.len);
    try std.testing.expectEqualStrings("b", hits[0].id);
    try std.testing.expectEqual(@as(usize, 1), locations.count());
    try std.testing.expectEqual(@as(u32, 8), locations.get("b").?.document);
    try std.testing.expect(locations.get("a") == null and locations.get("c") == null);
}

test "external lake global frontier winners keep logical file rank ties after arbitrary traversal" {
    const a = std.testing.allocator;
    var winners = PagedWinners.init(a, 3);
    defer winners.deinit();
    try winners.addPositioned(.{ .id = @constCast("late"), .score = 1 }, .{ .file = @splat(9), .rank = 0 }, null);
    try winners.addPositioned(.{ .id = @constCast("second"), .score = 1 }, .{ .file = @splat(1), .rank = 8 }, null);
    try winners.addPositioned(.{ .id = @constCast("first"), .score = 1 }, .{ .file = @splat(1), .rank = 2 }, null);
    try winners.addPositioned(.{ .id = @constCast("best"), .score = 2 }, .{ .file = @splat(255), .rank = 100 }, null);
    try std.testing.expectEqual(@as(usize, 3), winners.peak);
    const hits = try winners.finish(0);
    defer a.free(hits);
    defer for (hits) |*hit| hit.deinit(a);
    try std.testing.expectEqual(@as(usize, 3), hits.len);
    try std.testing.expectEqualStrings("best", hits[0].id);
    try std.testing.expectEqualStrings("first", hits[1].id);
    try std.testing.expectEqualStrings("second", hits[2].id);
}

test "external lake bounded WAL frontier ties retain archive order when recent is scored first" {
    const a = std.testing.allocator;
    var winners = PagedWinners.init(a, 2);
    defer winners.deinit();
    try winners.addPositioned(.{ .id = @constCast("recent"), .score = 1 }, .{ .file = @splat(0), .rank = 0, .recent = true }, null);
    try winners.addPositioned(.{ .id = @constCast("archive-late"), .score = 1 }, .{ .file = @splat(255), .rank = 2 }, null);
    try winners.addPositioned(.{ .id = @constCast("archive-first"), .score = 1 }, .{ .file = @splat(255), .rank = 1 }, null);
    const hits = try winners.finish(0);
    defer {
        for (hits) |*hit| hit.deinit(a);
        a.free(hits);
    }
    try std.testing.expectEqualStrings("archive-first", hits[0].id);
    try std.testing.expectEqualStrings("archive-late", hits[1].id);
}

test "external lake bounded window profiles sum work and preserve actual peaks" {
    var target: types.SortProfile = .{ .candidate_count = 9, .window_len = 4, .collector_heap_peak = 4, .index_sort_match = true, .native_loader = true, .native_filter_mode = "bitmap", .distributed_shard_count = 1 };
    mergeWindowProfile(&target, .{ .candidate_count = 7, .window_len = 3, .collector_heap_peak = 2, .index_sort_match = false, .native_loader = true, .native_filter_mode = "point", .distributed_shard_count = 1 });
    try std.testing.expectEqualStrings("mixed_window_plans", target.native_filter_mode);
    try std.testing.expectEqual(@as(usize, 1), target.distributed_shard_count);
    try std.testing.expectEqual(@as(u64, 16), target.candidate_count);
    try std.testing.expectEqual(@as(usize, 4), target.window_len);
    try std.testing.expectEqual(@as(usize, 4), target.collector_heap_peak);
    try std.testing.expect(!target.index_sort_match);
    try std.testing.expect(target.native_loader);
}

test "external lake global frontier fallback windows merge packed logical ties without a pruning plan" {
    const a = std.testing.allocator;
    const predicate = @import("lake_index_text_predicate.zig");
    const first_hash = corpus.term_directory.identity("source-first");
    const second_hash = corpus.term_directory.identity("source-second");
    const logical_first = if (std.mem.order(u8, &first_hash, &second_hash) == .lt) "source-first" else "source-second";
    const logical_last = if (std.mem.eql(u8, logical_first, "source-first")) "source-second" else "source-first";
    var heap = PagedWinners.init(a, 2);
    defer heap.deinit();
    // Two bounded windows arrive in physical pack order, opposite source order.
    // This is the production fallback merger; it owns no frontier or cutoff plan.
    for ([_][]const u8{ logical_last, logical_first }, 0..) |file, window| {
        const spans = [_]std.meta.Elem(@FieldType(predicate.Identities, "spans")){.{ .file = file, .span = .{ .lower = 0, .upper = 2, .rows = &.{} } }};
        const identities: predicate.Identities = .{ .offsets = &.{ 0, 2 }, .spans = &spans };
        for (0..2) |doc| {
            const id = try std.fmt.allocPrint(a, "{s}-{d}", .{ file, doc });
            defer a.free(id);
            try heap.addWindowHit(.{ .id = id, .score = 0, .native_text_doc_id = @intCast(doc) }, window * 2 + doc, &identities, false, null);
        }
    }
    const hits = try heap.finish(0);
    defer a.free(hits);
    defer for (hits) |*hit| hit.deinit(a);
    try std.testing.expectEqual(@as(usize, 2), hits.len);
    for (hits, 0..) |hit, rank| {
        const expected = try std.fmt.allocPrint(a, "{s}-{d}", .{ logical_first, rank });
        defer a.free(expected);
        try std.testing.expectEqualStrings(expected, hit.id);
    }
}

test "external lake bounded WAL unit hydration chooses exact recent partition" {
    const a = std.testing.allocator;
    var arena = std.heap.ArenaAllocator.init(a);
    defer arena.deinit();
    const sa = arena.allocator();
    const Manager = local.storage_db_catalog_index_manager.IndexManager;
    const Runtime = @import("lake_index_native_runtime_cache.zig").Entry;
    const vectors = @import("lake_recent_vectors.zig");
    var first_backend = local.storage_lsm_backend.Backend.init(a, .{});
    defer first_backend.close();
    var second_backend = local.storage_lsm_backend.Backend.init(a, .{});
    defer second_backend.close();
    const first_id = "wal1:7aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa";
    const second_id = "wal1:daaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa";
    const first_source = try @import("lake_enrichment_units.zig").sourceKey(sa, first_id, 0);
    const second_source = try @import("lake_enrichment_units.zig").sourceKey(sa, second_id, 0);
    {
        var write = try first_backend.beginWrite();
        defer write.abort();
        try write.put(.{ .name = "lake_units" }, first_source, "{\"text\":\"first\"}");
        try write.commit();
    }
    {
        var write = try second_backend.beginWrite();
        defer write.abort();
        try write.put(.{ .name = "lake_units" }, second_source, "{\"text\":\"second\"}");
        try write.commit();
    }
    var first_dense: Manager.DenseIndex = undefined;
    first_dense.config.name = @constCast("vectors");
    first_dense.field_name = @constCast("body");
    var second_dense: Manager.DenseIndex = undefined;
    second_dense.config.name = @constCast("vectors");
    second_dense.field_name = @constCast("body");
    var first_runtime: Runtime = undefined;
    first_runtime.dense_entry = &first_dense;
    first_runtime.sparse_entry = null;
    first_runtime.vectors = &first_backend;
    var second_runtime: Runtime = undefined;
    second_runtime.dense_entry = &second_dense;
    second_runtime.sparse_entry = null;
    second_runtime.vectors = &second_backend;
    var overlay: overlay_api.Overlay = .{ .pending = .{ .lsn = 2, .key_fields = &.{"id"} }, .key_filter = "" };
    try overlay.rows.put(sa, first_id, .{ .object = .empty });
    try overlay.rows.put(sa, second_id, .{ .object = .empty });
    const scope: @import("../serverless/artifacts/store.zig").UploadScope = .{ .domain = @splat(1), .attempt = @splat(2) };
    const first_artifact = try scope.artifactId(&(@as([64]u8, @splat('a'))));
    const second_artifact = try scope.artifactId(&(@as([64]u8, @splat('b'))));
    const Declared = local.serverless_segment_sidecar_manifest.DeclaredArtifact;
    const first_decl: Declared = .{ .name = "vectors", .binding = undefined, .artifact = .{ .kind = .vector_segment, .metadata_version = 2, .artifact_id = &first_artifact, .checksum = first_artifact[7..71], .byte_len = 1 } };
    const second_decl: Declared = .{ .name = "vectors", .binding = undefined, .artifact = .{ .kind = .vector_segment, .metadata_version = 2, .artifact_id = &second_artifact, .checksum = second_artifact[7..71], .byte_len = 1 } };
    const runs = [_]vectors.Run{ .{ .prefix = '7', .digest = @splat(1), .declarations = &.{first_decl} }, .{ .prefix = 'd', .digest = @splat(2), .declarations = &.{second_decl} } };
    var execution: Execution = undefined;
    execution.index_resolver = null;
    execution.navigation = null;
    execution.context = .{ .io = std.testing.io };
    execution.arena = sa;
    execution.overlay = &overlay;
    execution.active_recent = false;
    execution.recent_run = null;
    execution.recent_runs = &runs;
    execution.unit_runtime = &first_runtime;
    execution.recent_dense_entries = .empty;
    execution.recent_sparse_entries = .empty;
    execution.runtimes = .empty;
    try execution.runtimes.appendSlice(sa, &.{ &first_runtime, &second_runtime });
    try execution.recent_dense_entries.put(sa, &first_artifact, &first_dense);
    try execution.recent_dense_entries.put(sa, &second_artifact, &second_dense);
    const first_key = try local.storage_internal_keys.documentUnitArtifactKeyAlloc(sa, first_id, "vectors_sources", "0");
    const second_key = try local.storage_internal_keys.documentUnitArtifactKeyAlloc(sa, second_id, "vectors_sources", "0");
    const one = try execution.unitPayload(a, first_key);
    defer a.free(one);
    const two = try execution.unitPayload(a, second_key);
    defer a.free(two);
    try std.testing.expect(std.mem.indexOf(u8, two, "second") != null);
}

test "external lake bounded WAL retained publication cancellation reaches cloud transports" {
    const live: Context = .{};
    const active = retainedCancellation(&live);
    try active.check();
    try std.testing.expect(!local.storage_object_storage.CancellationToken.fromCallback(active.ptr, active.is_cancelled_fn).?.isCancelled());
    const expired: Context = .{ .deadline_ns = 1 };
    const token = retainedCancellation(&expired);
    try std.testing.expectError(error.DeadlineExceeded, token.check());
    try std.testing.expect(local.storage_object_storage.CancellationToken.fromCallback(token.ptr, token.is_cancelled_fn).?.isCancelled());
}

test "external lake bounded WAL vector lanes join cancel saturate and retry oversized partitions" {
    const a = std.testing.allocator;
    const io = std.testing.io;
    const State = struct {
        io: std.Io,
        gate: std.Io.Event = .unset,
        started: std.Io.Event = .unset,
        entered: std.Io.Event = .unset,
        archive_entered: std.Io.Event = .unset,
        active: std.atomic.Value(usize) = .init(0),
        peak: std.atomic.Value(usize) = .init(0),
        calls: std.atomic.Value(usize) = .init(0),
        archive_active: std.atomic.Value(bool) = .init(false),
        fn part(raw: ?*anyopaque, alloc: A, _: types.SearchRequest, oversized: bool) !types.SearchResult {
            const execution = Execution.from(raw);
            const self: *@This() = @ptrCast(@alignCast(execution.server));
            const prefix = if (execution.recent_run) |run_| run_.prefix else 0;
            if (prefix == 0) self.archive_active.store(true, .release);
            defer if (prefix == 0) {
                self.archive_active.store(false, .release);
            };
            const active = self.active.fetchAdd(1, .acq_rel) + 1;
            if (prefix == 0) self.archive_entered.set(self.io);
            defer _ = self.active.fetchSub(1, .acq_rel);
            _ = self.calls.fetchAdd(1, .monotonic);
            self.entered.set(self.io);
            _ = self.peak.fetchMax(active, .acq_rel);
            if (active >= 2) self.started.set(self.io);
            try self.gate.wait(self.io);
            if (oversized and prefix == 1) {
                const bytes = try alloc.alloc(u8, 17 * 1024 * 1024);
                alloc.free(bytes);
            }
            const hits = try alloc.alloc(types.SearchHit, 1);
            errdefer alloc.free(hits);
            hits[0] = .{ .id = try std.fmt.allocPrint(alloc, "run-{d}", .{prefix}), .score = @floatFromInt(prefix) };
            return .{ .alloc = alloc, .hits = hits, .total_hits = 1 };
        }
        fn run(execution: *Execution, alloc: A, result: *types.SearchResult, scheduling: *local.sql_parallel_scheduler.Scheduler, oversized: bool) !void {
            try execution.searchVectorParts(alloc, .{ .limit = 2, .full_text = .{ .term = .{ .field = "body", .term = "score" } } }, oversized, part, result, scheduling, true);
        }
    };
    for ([_]u8{ 0, 1, 2, 3, 4, 5, 6 }) |mode| {
        var state: State = .{ .io = io };
        var scheduling: local.sql_parallel_scheduler.Scheduler = .{};
        if (mode == 1) scheduling.max_io_workers = 0;
        if (mode == 1 or mode == 3 or mode == 4 or mode == 6) state.gate.set(io);
        var runs: [4]@import("lake_recent_vectors.zig").Run = undefined;
        for (&runs, 1..) |*run, i| run.* = .{ .prefix = @intCast(i), .digest = @splat(0), .declarations = &.{}, .cloud_generations = null };
        var source: local.serverless_query_lake_serving.ServingSource = .{ .alloc = a, .store = undefined, .inventory = undefined, .scanner = undefined };
        source.scanner.shared_reader = null;
        var execution: Execution = .{ .scheduled_component = mode >= 4, .server = @ptrCast(@alignCast(&state)), .table = undefined, .source = &source, .store = undefined, .domain = @splat(0), .declarations = &.{}, .context = .{ .io = io }, .request = .{}, .schema_json = "{}", .arena = a, .result_allocator = a, .recent_runs = &runs };
        defer execution.deinit();
        var component_budget: local.sql_memory_budget = .{ .backing = a, .limit = 16 * 1024 * 1024 };
        defer std.debug.assert(component_budget.live == 0);
        const run_alloc = if (mode == 6) component_budget.allocator() else a;
        var result: types.SearchResult = .{ .alloc = run_alloc, .hits = &.{}, .total_hits = 0 };
        defer result.deinit();
        var future = try io.concurrent(State.run, .{ &execution, run_alloc, &result, &scheduling, mode == 3 or mode == 6 });
        var joined = false;
        defer if (!joined) {
            _ = future.cancel(io) catch {};
        };
        if (mode == 0 or mode == 2) {
            try state.started.wait(io);
            try std.testing.expect(scheduling.io_workers > 0);
        }
        if (mode == 0) {
            try state.archive_entered.wait(io);
            // The gate is still closed: archive plus at least one recent lane
            // are simultaneously inside part(), independent of entry order.
            try std.testing.expect(state.archive_active.load(.acquire));
            try std.testing.expect(state.active.load(.acquire) >= 2);
        }
        if (mode == 5) try state.entered.wait(io);
        joined = true;
        if (mode == 6) {
            try std.testing.expectError(error.OutOfMemory, future.await(io));
            try std.testing.expect(component_budget.isExhausted());
        } else if (mode == 2 or mode == 5) {
            try std.testing.expectError(error.Canceled, future.cancel(io));
        } else {
            state.gate.set(io);
            try future.await(io);
            try std.testing.expectEqual(@as(u32, 5), result.total_hits);
            try std.testing.expectEqual(@as(usize, 2), result.hits.len);
            try std.testing.expectEqualStrings("run-4", result.hits[0].id);
            try std.testing.expectEqualStrings("run-3", result.hits[1].id);
            try std.testing.expectEqual(@as(usize, if (mode == 3) 6 else 5), state.calls.load(.acquire));
        }
        if (mode == 0) {
            try std.testing.expect(state.peak.load(.acquire) >= 2);
        }
        if (mode == 1 or mode >= 4) try std.testing.expectEqual(@as(usize, 1), state.peak.load(.acquire));
        try std.testing.expectEqual(@as(usize, 0), state.active.load(.acquire));
        try std.testing.expectEqual(@as(usize, 0), scheduling.bytes);
        try std.testing.expectEqual(@as(usize, 0), scheduling.io_workers);
    }
}

test "external lake global frontier large source version views avoid lane setup copies" {
    const a = std.testing.allocator;
    var source: local.serverless_query_lake_serving.ServingSource = .{ .alloc = a, .store = undefined, .inventory = undefined, .scanner = undefined };
    source.scanner.shared_reader = null;
    defer source.clearVersions();
    const value: [512]u8 = @splat('v');
    for (0..32768) |i| try source.versions.put(a, i, .{ .file_id = @constCast("file"), .object_uri = @constCast("file://file"), .byte_len = 1, .row_count = 1, .row_groups = &.{}, .etag = try a.dupe(u8, "etag"), .version_id = try a.dupe(u8, &value) });
    var tiny: local.sql_memory_budget = .{ .backing = a, .limit = 1024 };
    var view = try source.parallelView(tiny.allocator());
    defer view.closeParallelView(&source);
    try std.testing.expectEqual(@as(usize, 0), view.versions.count());
    try std.testing.expectEqualStrings(&value, view.pinnedVersion(32767).?.version_id);
    const runs = [_]@import("lake_recent_vectors.zig").Run{.{ .prefix = 1, .digest = @splat(0), .declarations = &.{}, .cloud_generations = null }};
    var execution: Execution = .{ .server = undefined, .table = undefined, .source = &source, .store = undefined, .domain = @splat(0), .declarations = &.{}, .context = .{ .io = std.testing.io }, .request = .{}, .schema_json = "{}", .arena = a, .result_allocator = a, .recent_runs = &runs };
    defer execution.deinit();
    const Part = struct {
        fn run(_: ?*anyopaque, alloc: A, _: types.SearchRequest, _: bool) !types.SearchResult {
            return .{ .alloc = alloc, .hits = &.{}, .total_hits = 0 };
        }
    };
    var combined: types.SearchResult = .{ .alloc = a, .hits = &.{}, .total_hits = 0 };
    defer combined.deinit();
    var scheduler: local.sql_parallel_scheduler.Scheduler = .{};
    try execution.searchVectorParts(a, .{ .limit = 1 }, false, Part.run, &combined, &scheduler, true);
}

test "external lake metadata navigation ordered pages share text membership and retain pressure fallback" {
    const a = std.testing.allocator;
    var arena = std.heap.ArenaAllocator.init(a);
    defer arena.deinit();
    const sa = arena.allocator();
    var overlay: overlay_api.Overlay = .{ .pending = .{ .lsn = 1, .key_fields = &.{"id"}, .changes = &.{} }, .key_filter = "" };
    try overlay.rows.put(sa, "wal1:first", try std.json.parseFromSliceLeaky(std.json.Value, sa, "{\"body\":\"rare common\"}", .{}));
    try overlay.rows.put(sa, "wal1:second", try std.json.parseFromSliceLeaky(std.json.Value, sa, "{\"body\":\"common\"}", .{}));
    var pin = try overlay.recent(a, "text", .{}, null, "body", .{});
    defer pin.deinit();
    const Stats = struct {
        fn average(raw: *anyopaque, field: []const u8) f32 {
            const snapshot: *local.index.IndexSnapshot = @ptrCast(@alignCast(raw));
            return snapshot.textAvgDocLen(field);
        }
        fn frequencies(raw: *anyopaque, alloc: A, field: []const u8, terms: []const []const u8, output: []u32) !void {
            const snapshot: *local.index.IndexSnapshot = @ptrCast(@alignCast(raw));
            try snapshot.termDocFreqs(alloc, field, terms, output);
        }
    };
    const query: local.search_search.SearchQuery = .{ .term = .{ .field = "body", .term = "rare" } };
    const prepared = (try local.search_search.TextNavigationPlan.prepare(a, query, .{ .ptr = pin.snapshot, .count = pin.snapshot.scoringDocCount(), .average = Stats.average, .frequencies = Stats.frequencies }, .{})).?;
    defer prepared.deinit();
    var memo: Execution.OrderedMembership = .{};
    defer memo.deinit();
    const first = (try memo.get(@splat(1), pin.snapshot, prepared, .none)).?;
    try std.testing.expectEqual(@as(usize, 1), first.cardinality());
    try std.testing.expect(first == (try memo.get(@splat(1), pin.snapshot, prepared, .none)).?);
    try std.testing.expectEqual(@as(usize, 1), memo.builds);
    try std.testing.expectEqual(@as(usize, 1), memo.hits);
    memo.budget.limit = 1;
    try std.testing.expect((try memo.get(@splat(2), pin.snapshot, prepared, .none)) == null);
    memo.budget.limit = 8 * 1024 * 1024;
    try std.testing.expectEqual(@as(usize, 1), (try memo.get(@splat(2), pin.snapshot, prepared, .none)).?.cardinality());
    // Membership prefiltering and ordinary mixed-Boolean scoring share the
    // same pattern leaf. Repeated ordered pages must not expand it again.
    const patterned: local.search_search.SearchQuery = .{ .bool_query = .{ .must = &.{.{ .prefix = .{ .field = "body", .prefix = "ra", .boost = 2 } }}, .should = &.{.{ .term = .{ .field = "body", .term = "common" } }} } };
    const pattern_plan = (try local.search_search.TextNavigationPlan.prepare(a, patterned, .{ .ptr = pin.snapshot, .count = pin.snapshot.scoringDocCount(), .average = Stats.average, .frequencies = Stats.frequencies }, .{})).?;
    defer pattern_plan.deinit();
    var pages: Execution.OrderedMembership = .{};
    defer pages.deinit();
    try std.testing.expectEqual(@as(usize, 1), (try pages.get(@splat(3), pin.snapshot, pattern_plan, .none)).?.cardinality());
    const patterns = pages.patternCache(@splat(3)).?;
    try std.testing.expectEqual(pin.snapshot.segments.len, patterns.builds);
    var baseline = try local.search_search.execute(a, pin.snapshot, .{ .query = patterned, .text_navigation_plan = pattern_plan, .k = 2, .include_stored = false });
    defer baseline.deinit();
    for (0..3) |_| {
        var scored = try local.search_search.execute(a, pin.snapshot, .{ .query = patterned, .text_navigation_plan = pattern_plan, .control = .{ .pattern_cache = patterns }, .k = 2, .include_stored = false, .filter_doc_nums = &.{0}, .filter_doc_nums_positive = true });
        defer scored.deinit();
        try std.testing.expectEqual(@as(usize, 1), scored.hits.len);
        try std.testing.expectEqual(baseline.hits[0].score, scored.hits[0].score);
    }
    try std.testing.expectEqual(pin.snapshot.segments.len, patterns.builds);
    try std.testing.expect(patterns.hits >= 3);
}

test "external lake covering search hydration survives missing Parquet and preserves ranked typed delivery" {
    const a = std.testing.allocator;
    var directory = try local.common_test_directory.TestDirectory.init("lake-covering-result-hydration");
    defer directory.cleanup();
    var fs = try local.storage_object_storage.FilesystemObjectStorage.init(a, directory.path());
    defer fs.deinit();
    var client = fs.client();
    try client.makeBucket("antfly");
    const data = try local.serverless_query_lake_parquet_rowgroup.buildTestPlainI64AndByteArrayParquetObjectAlloc(a, &.{.{ .column_id = "amount", .field_id = 3, .values = &.{ 9007199254740993, 9007199254740994 } }}, &.{
        .{ .column_id = "body", .field_id = 1, .converted_type = 0, .values = &.{ "a needle in the haystack", "second needle" } },
        .{ .column_id = "label", .field_id = 2, .converted_type = 0, .values = &.{ "returned metadata", "other metadata" } },
    });
    defer a.free(data);
    var put = try client.putObject("antfly", "part.parquet", data, .{});
    put.deinit(a);
    const schema_json = try std.fmt.allocPrint(a,
        \\{{"version":1,"storage_mode":"relational","default_type":"row","base_source":{{"kind":"external","table_id":"lake","format":"parquet","uri":"file://{s}","schema_fingerprint":"schema"}},"relational_indexes":[{{"name":"amount_idx","keys":[{{"column":"amount"}}],"include_columns":["body","label"]}}],"document_schemas":{{"row":{{"schema":{{"type":"object","properties":{{"amount":{{"type":"integer"}},"body":{{"type":"string"}},"label":{{"type":"string"}}}},"additionalProperties":false}}}}}}}}
    , .{directory.path()});
    defer a.free(schema_json);
    var binding = (try local.serverless_external_source_schema_binding.externalBindingFromSchemaJsonAlloc(a, schema_json)).?;
    defer binding.deinit(a);
    var source = try local.serverless_query_lake_serving.ServingSource.open(a, .{ .storage_mode = .relational, .external_base_source = binding }, .{});
    defer source.deinit();
    var store = try Store.openNative(a, null, null, false, .standalone, directory.path());
    defer store.deinit();
    var artifacts = store.artifactStore();
    var arena = std.heap.ArenaAllocator.init(a);
    defer arena.deinit();
    const ca = arena.allocator();
    const publication = @import("lake_index_publication.zig");
    var record: local.common_topology_records.TableRecord = .{ .table_id = 7, .name = "lake", .schema_json = schema_json, .indexes_json = "{}" };
    record.lake_index_catalog_json = try publication.begin(ca, std.testing.io, record, &source, store.identity, .{}, 100, 20);
    const Clock = struct {
        fn now(_: *const anyopaque) !u64 {
            return 101;
        }
    };
    var clock: u8 = 0;
    record.lake_index_catalog_json = try publication.build(ca, &artifacts, record, &source, store.identity, .{ .io = std.testing.io }, .none, .{ .ptr = &clock, .now_ms = Clock.now });
    var catalog = try local.metadata_lake_index_catalog.parse(a, record.lake_index_catalog_json);
    defer catalog.deinit();
    try @import("lake_index_directory.zig").hydrate(catalog.arena.allocator(), artifacts, &catalog.value.published.?, .none, null);
    const Hooks = struct {
        fn read(_: *anyopaque, _: A, _: u64, _: local.api_operation.RequestContext) ![]u8 {
            return error.TestUnexpectedResult;
        }
        fn mutate(_: *anyopaque, _: u64, _: u64, _: @import("../metadata/lake_index_lifecycle.zig").Mutation, _: local.api_operation.RequestContext) !void {
            return error.TestUnexpectedResult;
        }
    };
    var status_token: u8 = 0;
    var server = server_api.ApiHttpServer.init(a, .{}, .{ .ptr = &status_token, .vtable = &.{ .status = undefined, .get_lake_index_lifecycle = Hooks.read, .mutate_lake_index_lifecycle = Hooks.mutate } }, null, null);
    defer server.deinit();
    var table = try server.sql_schema_cache.resolve(std.testing.io, ca, schema_json, 7, "lake");
    table.external_indexes = .{ .schema_json = schema_json, .catalog_json = record.lake_index_catalog_json, .indexes_json = record.indexes_json, .desired = local.metadata_lake_index_catalog.desiredFingerprint(record) };
    var owner: Execution = .{ .server = &server, .table = table, .source = &source, .store = &store, .domain = publication.uploadDomainWithNamespace(7, store.identity, catalog.value.published.?.namespace), .declarations = catalog.value.published.?.declarations, .context = .{ .io = std.testing.io }, .request = .{}, .schema_json = schema_json, .arena = ca };
    defer owner.deinit();
    const first = try local.storage_rowsource_identity.allocId(ca, .{ .external = .{ .source_id = source.inventory.source_id, .snapshot_id = source.inventory.snapshot_id, .file_id = source.inventory.files[0].file_id, .row_group_ordinal = 0, .row_ordinal = 0 } });
    const second = try local.storage_rowsource_identity.allocId(ca, .{ .external = .{ .source_id = source.inventory.source_id, .snapshot_id = source.inventory.snapshot_id, .file_id = source.inventory.files[0].file_id, .row_group_ordinal = 0, .row_ordinal = 1 } });
    try owner.files.put(ca, first[6..70], source.inventory.files[0].file_id);
    try client.deleteObject("antfly", "part.parquet", .{});
    {
        const predicates = @import("lake_index_text_predicate.zig");
        const ordered_rows = @import("lake_index_sql_rows.zig");
        const resolver: predicates.PhysicalResolver = .{ .server = &server, .table = table, .source = &source, .context = .{}, .store = artifacts, .store_identity = store.identity, .read_context = owner.context, .pinned = .{ .artifacts = artifacts, .store_identity = store.identity, .domain = owner.domain, .declarations = owner.declarations, .read_context = owner.context } };
        var membership: predicates.PhysicalResolver.Membership = .{ .a = a, .resolver = resolver, .arena = .init(a), .json = "{\"amount\":9007199254740993}", .budget = std.math.maxInt(u64) };
        defer membership.deinit();
        // Exercise the tile probe even for this tiny fixture; normal planning
        // would eagerly materialize such a selective predicate. Parquet is
        // already gone, so both routes must consume authenticated native state.
        var predicate = (try ordered_rows.tryOpenPredicateWithContext(a, &server, table, &.{.{ .column = "amount", .op = .eq, .value = .{ .integer = 9007199254740993 } }}, .{}, &source, resolver.pinned)).?;
        membership.predicates.append(membership.arena.allocator(), predicate) catch |err| {
            predicate.deinit();
            return err;
        };
        const current: local.storage_rowsource_types.RowRef = .{ .external = .{ .source_id = source.inventory.source_id, .snapshot_id = source.inventory.snapshot_id, .identity_version = source.inventory.row_identity_version, .file_id = source.inventory.files[0].file_id, .row_group_ordinal = 0, .row_ordinal = 0 } };
        var other = current;
        other.external.row_ordinal = 1;
        const shuffled = [_]local.storage_rowsource_types.RowRef{ other, current, current };
        var allowed: [3]bool = undefined;
        try membership.allowsMany(&shuffled, &allowed);
        try std.testing.expectEqualSlices(bool, &.{ false, true, true }, &allowed);
        try std.testing.expect(membership.complete == null and membership.spent != 0);
        // Admission decline must discard tile work and produce the same exact
        // set through the complete native predicate path.
        membership.budget = 0;
        try membership.allowsMany(&shuffled, &allowed);
        try std.testing.expectEqualSlices(bool, &.{ false, true, true }, &allowed);
        try std.testing.expect(membership.complete != null);
    }
    var reads: ?u64 = null;
    for (0..2) |_| {
        const values = try Execution.loadSelected(types.ColumnSource, &owner, a, &.{ second, first, second }, &.{ "amount", "body" });
        defer {
            for (values) |value| if (value) |row| row.deinit();
            a.free(values);
        }
        for (values, [_]i64{ 9007199254740994, 9007199254740993, 9007199254740994 }) |value, amount| {
            var result_arena = std.heap.ArenaAllocator.init(a);
            defer result_arena.deinit();
            const json = try value.?.value(&result_arena);
            try std.testing.expectEqual(amount, json.object.get("amount").?.integer);
            try std.testing.expect(json.object.get("body").? == .string);
            try std.testing.expect(value.?.page.columns[0].values == .i64);
        }
        const current = server.lake_read_cache.snapshot().provider_reads;
        if (reads) |previous| try std.testing.expectEqual(previous, current);
        reads = current;
    }
}

test "external lake relocated sparse predicates translate selective masks and defer broad native windows" {
    const a = std.testing.allocator;
    const Set = @import("lake_index_physical_set.zig").Set;
    const RowRef = local.storage_rowsource_types.RowRef;
    const Serving = local.serverless_query_lake_serving.ServingSource;
    const Bitmap = local.encoding_roaring.RoaringBitmap;
    const Mock = struct {
        translated: usize = 0,
        batch_calls: usize = 0,
        points: usize = 0,
        fn from(_: *anyopaque, _: A, ref: RowRef) !RowRef {
            var result = ref;
            result.external.snapshot_id = "current";
            result.external.file_id = "current-file";
            result.external.row_ordinal -= 100;
            return result;
        }
        fn to(_: *anyopaque, _: A, ref: RowRef) !RowRef {
            var result = ref;
            result.external.snapshot_id = "old";
            result.external.file_id = "old-file";
            result.external.row_ordinal += 100;
            return result;
        }
        fn many(raw: *anyopaque, alloc: A, refs: []const RowRef, forward: bool, allowance: *Serving.TranslationAllowance) ![]const RowRef {
            const self: *@This() = @ptrCast(@alignCast(raw));
            if (refs.len > allowance.remaining_rows) return error.QueryCandidateBudgetExceeded;
            allowance.remaining_rows -= refs.len;
            self.translated += refs.len;
            self.batch_calls += 1;
            const result = try alloc.alloc(RowRef, refs.len);
            for (refs, result) |ref, *out| out.* = if (forward) try from(raw, alloc, ref) else try to(raw, alloc, ref);
            return result;
        }
        fn file(_: *anyopaque, _: A, _: []const u8) !?[]const u8 {
            return "old-file";
        }
        fn digest(_: *anyopaque, _: A, _: []const u8) ![]const u8 {
            return "0000000000000000000000000000000000000000000000000000000000000000";
        }
        fn rank(_: *anyopaque, _: A, _: RowRef) !u64 {
            return error.UnexpectedRankLookup;
        }
        fn close(_: *anyopaque) void {}
        fn one(raw: *anyopaque, key: []const u8) !?u32 {
            const self: *@This() = @ptrCast(@alignCast(raw));
            self.points += 1;
            const coordinate = try @import("lake_index_native_state.zig").coordinates(key);
            return @intCast(coordinate.row - 100);
        }
        fn block(_: *anyopaque, _: A, _: []const u8, _: u32, _: *const Bitmap, _: *Bitmap) !bool {
            return false;
        }
        fn range(_: *anyopaque, _: A, first: u32, last: u32, filter: types.SparseOrdinalKeyFilter, result: *Bitmap) !bool {
            var ordinal = first;
            while (ordinal <= last) : (ordinal += 1) {
                var buffer: [96]u8 = undefined;
                const key = try std.fmt.bufPrint(&buffer, "lake2:{s}:00000000:{x:0>16}", .{ try digest(undefined, undefined, ""), @as(u64, ordinal) + 100 });
                if (try filter.allowsKey(key)) try result.add(ordinal);
            }
            return true;
        }
    };
    var mock: Mock = .{};
    var source: Serving = undefined;
    source.inventory.source_id = @constCast("source");
    source.inventory.snapshot_id = @constCast("current");
    source.inventory.row_identity_version = 1;
    source.row_resolver = null;
    var execution: Execution = .{ .server = undefined, .source = &source, .store = undefined, .domain = @splat(0), .declarations = &.{}, .context = .{ .io = std.testing.io }, .request = .{}, .schema_json = "", .arena = a, .result_allocator = a, .table = undefined };
    execution.index_resolver = .{ .ptr = &mock, .source = "source", .snapshot = "old", .current_source = "source", .current_snapshot = "current", .rows = 100000, .from_index = Mock.from, .to_index = Mock.to, .file = Mock.file, .digest = Mock.digest, .tie_rank = Mock.rank, .close = Mock.close, .translate_many_bounded = Mock.many };
    execution.vector_include = Set.init(a);
    defer execution.vector_include.?.deinit();
    execution.vector_exclude = Set.init(a);
    defer execution.vector_exclude.?.deinit();
    try execution.vector_include.?.addRow("current-file", 0, 7);
    try execution.vector_include.?.addRow("current-file", 0, 9);
    try execution.vector_exclude.?.addRow("current-file", 0, 9);
    const lookup: types.SparseOrdinalLookup = .{ .ptr = &mock, .one = Mock.one, .block = Mock.block, .native_range = Mock.range };
    var selective = (try Execution.selectSparseConstraints(&execution, a, lookup)).?;
    defer selective.deinit();
    try std.testing.expect(!selective.residual and selective.include.?.contains(7) and !selective.include.?.contains(9));
    try std.testing.expectEqual(@as(usize, 1), selective.include.?.cardinality());
    try std.testing.expectEqual(@as(usize, 3), mock.translated);
    try std.testing.expectEqual(@as(usize, 1), mock.points);
    const Failure = struct {
        fn run(alloc: A, owner: *Execution, native: types.SparseOrdinalLookup) !void {
            var selected = (try Execution.selectSparseConstraints(owner, alloc, native)).?;
            defer selected.deinit();
            try std.testing.expect(selected.include.?.contains(7));
        }
    };
    try std.testing.checkAllAllocationFailures(a, Failure.run, .{ &execution, lookup });
    try execution.vector_include.?.addBlock(.{ .file = "current-file", .group = 0, .base = 0, .selection = .{ .interval = .{ .lower = 0, .count = 100000 } } });
    mock.translated = 0;
    mock.points = 0;
    var broad = (try Execution.selectSparseConstraints(&execution, a, lookup)).?;
    defer broad.deinit();
    try std.testing.expect(broad.residual and broad.deferred and broad.include == null);
    try std.testing.expectEqual(@as(usize, 0), mock.translated);
    try std.testing.expectEqual(@as(usize, 0), mock.points);
    var window = (try Execution.selectSparseRange(&execution, a, lookup, 0, 11)).?;
    defer window.deinit();
    try window.prepareRead();
    try std.testing.expectEqual(@as(usize, 11), window.include.?.cardinality());
    try std.testing.expect(window.include.?.contains(7) and !window.include.?.contains(9));
    // The broad dense path declines eager ID materialization, but translates
    // every exact residual candidate through bounded native callback tiles.
    try std.testing.expect((try Execution.selectDenseConstraints(&execution, a)) == null);
    var key_storage: [6001][96]u8 = undefined;
    var keys: [6001][]const u8 = undefined;
    var allowed: [6001]bool = undefined;
    for (&key_storage, &keys, 0..) |*buffer, *key, ordinal| key.* = try std.fmt.bufPrint(buffer, "lake2:{s}:00000000:{x:0>16}", .{ try Mock.digest(undefined, undefined, ""), @as(u64, ordinal) + 100 });
    mock.translated = 0;
    mock.batch_calls = 0;
    const predicate: types.NativeKeyPredicate = .{ .ptr = &execution, .allows = Execution.allowsVectorKey, .allows_many = Execution.allowsVectorKeys };
    const received = predicate.receiver();
    try received.allows_many.?(received.ptr, &keys, &allowed);
    try std.testing.expectEqual(keys.len, mock.translated);
    try std.testing.expectEqual(@as(usize, 47), mock.batch_calls);
    for (allowed, 0..) |keep, ordinal| try std.testing.expectEqual(ordinal != 9, keep);
}

test "external lake relocation key projection releases intermediate references" {
    const a = std.testing.allocator;
    const Source = local.serverless_query_lake_serving.ServingSource;
    const RowRef = local.storage_rowsource_types.RowRef;
    const Callbacks = struct {
        fn translate(_: *anyopaque, alloc: A, ref: RowRef) !RowRef {
            var result = ref;
            result.external.source_id = try alloc.dupe(u8, ref.external.source_id);
            errdefer alloc.free(result.external.source_id);
            result.external.snapshot_id = try alloc.dupe(u8, ref.external.snapshot_id);
            errdefer alloc.free(result.external.snapshot_id);
            result.external.file_id = try alloc.dupe(u8, "part.parquet");
            return result;
        }
        fn file(_: *anyopaque, _: A, _: []const u8) !?[]const u8 {
            return "part.parquet";
        }
        fn digest(_: *anyopaque, _: A, _: []const u8) ![]const u8 {
            return "0000000000000000000000000000000000000000000000000000000000000000";
        }
        fn rank(_: *anyopaque, _: A, _: RowRef) !u64 {
            return 0;
        }
        fn close(_: *anyopaque) void {}
    };
    var source: Source = undefined;
    source.inventory.source_id = @constCast("source");
    source.inventory.snapshot_id = @constCast("snapshot");
    source.inventory.row_identity_version = 2;
    var marker: u8 = 0;
    var execution: Execution = .{
        .server = undefined,
        .source = &source,
        .store = undefined,
        .domain = @splat(0),
        .declarations = &.{},
        .context = .{},
        .request = .{},
        .schema_json = "",
        .arena = a,
        .table = .{ .id = 1, .physical_name = "lake", .schema_version = 1, .columns = &.{} },
        .index_resolver = .{
            .ptr = &marker,
            .source = "source",
            .snapshot = "snapshot",
            .current_source = "source",
            .current_snapshot = "snapshot",
            .rows = 1,
            .from_index = Callbacks.translate,
            .to_index = Callbacks.translate,
            .file = Callbacks.file,
            .digest = Callbacks.digest,
            .tie_rank = Callbacks.rank,
            .close = Callbacks.close,
        },
    };
    const public = try Execution.publicKey(&execution, a, "lake2:0000000000000000000000000000000000000000000000000000000000000000:00000000:0000000000000000");
    defer a.free(public);
    try execution.files.put(a, try local.storage_rowsource_identity.publicFile(public), "part.parquet");
    defer execution.files.deinit(a);
    const native = try Execution.nativeKey(&execution, a, public);
    defer a.free(native);
}
