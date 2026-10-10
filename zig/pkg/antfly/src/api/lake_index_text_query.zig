// Copyright 2026 Antfly, Inc.
// SPDX-License-Identifier: Elastic-2.0
//
// Licensed under the Elastic License 2.0 (ELv2); you may not use this file
// except in compliance with the Elastic License 2.0. You may obtain a copy of
// the Elastic License 2.0 at
//
//     https://www.antfly.io/licensing/ELv2-license
//
// Unless required by applicable law or agreed to in writing, software distributed
// under the Elastic License 2.0 is distributed on an "AS IS" BASIS, WITHOUT
// WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
// Elastic License 2.0 for the specific language governing permissions and
// limitations.

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
pub fn execute(a: A, server: *server_api.ApiHttpServer, table: local.common_topology_records.TableRecord, req: types.SearchRequest, request: local.api_operation.RequestContext) !?local.api_query.QueryResponse {
    return executeWithDelivery(a, server, table, req, request, null);
}
pub fn executeWithDelivery(a: A, server: *server_api.ApiHttpServer, table: local.common_topology_records.TableRecord, req: types.SearchRequest, request: local.api_operation.RequestContext, delivery: ?local.api_query_response.Delivery) !?local.api_query.QueryResponse {
    return executePinned(a, server, table, req, request, delivery, false) catch |err| switch (err) {
        error.ExternalLakeIndexNotPublished, error.ExternalLakeIndexUnavailable => error.IndexRebuilding,
        error.ExternalLakeIndexDefinitionChanged, error.ExternalLakeIndexStoreChanged, error.ExternalLakeIndexCredentialsChanged, error.ExternalLakeIndexSourceChanged, error.ExternalLakeSnapshotMismatch => error.CatalogGenerationChanged,
        error.LakeIndexReaderLeaseExpired, error.NativeLakeTextCacheBusy, error.NativeLakeRuntimeCacheBusy, error.LakeSnapshotReadLeaseExpired, error.LakeSnapshotRetired, error.LakeOverlayCoverageUnavailable => error.StorageReadTemporarilyUnavailable,
        error.NativeLakeTextCorpusTooLarge, error.LakeOverlayTooLarge => error.QueryCandidateBudgetExceeded,
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
    const retained_cancellation: @import("antfly_cancellation").CancellationToken = .{ .ptr = request.cancellation.ptr, .is_cancelled_fn = request.cancellation.is_cancelled_fn };
    if (req.remote_snapshot) |token| if (std.mem.startsWith(u8, token, retained_api.prefix)) {
        var cut_store = try Store.openNative(a, server.cfg.node_config, server.cfg.secret_store, true, server.cfg.deployment_mode, server.cfg.native_lake_artifact_base_dir);
        defer cut_store.deinit();
        var artifacts = cut_store.artifactStore();
        retained = try retained_api.load(ra, &artifacts, cut_store.identity, token, table, @import("antfly_platform").time.realtimeNs() / std.time.ns_per_ms, retained_cancellation);
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
    const normalized = try request.platformDeadline();
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
            if (server.cfg.node_config == null or server.cfg.node_config.?.storage.artifacts.connection == null) return error.UnsupportedQueryRequest;
        }
    }
    if (retained) |cut| {
        if (cut.published_only != published_only) return error.CatalogGenerationChanged;
        retained_pending = cut.pending;
        if (cut.pending.changes.len != 0) overlay = try overlay_api.Overlay.init(oa, cut.pending);
    }
    if (published_only or retained != null) {
        const base_id = publication.base_source.external_iceberg.snapshot_id;
        source_schema.binding.write_policy = .read_only;
        source_schema.binding.snapshot_mode = if (std.mem.startsWith(u8, base_id, "empty:")) .current else .{ .snapshot_id = base_id };
    }
    if (retained == null and schema.binding.write_policy == .iceberg_writer and server.cfg.node_config != null and server.cfg.node_config.?.storage.artifacts.connection != null) {
        const catalog = local.serverless_external_source_mod.lake_catalog;
        var current = try @import("../serverless/configured_object_store_support.zig").executeLakeCatalogAlloc(a, schema.binding, options, context, .load);
        defer current.deinit(a);
        const root = try catalog.metadata.parse(oa, current.table.metadata_json);
        retained_metadata = try std.json.parseFromSliceLeaky(catalog.types.Table, oa, try std.json.Stringify.valueAlloc(oa, current.table, .{}), .{ .allocate = .alloc_always });
        const base_id = publication.base_source.external_iceberg.snapshot_id;
        const cut = try overlay_api.snapshotCoverage(root, base_id);
        if (published_only) {
            if (req.lake_read.?.through) |receipt| if (cut < receipt.wal_lsn) return error.IndexRebuilding;
        }
        const pending = if (!published_only) try @import("../serverless/lake_ingestion.zig").pending(oa, schema.binding, options, context, cut) else @import("../serverless/lake_ingestion.zig").Pending{ .lsn = cut, .key_fields = &.{}, .changes = &.{} };
        retained_pending = pending;
        if (req.lake_read) |read| if (read.through) |receipt| if (pending.lsn < receipt.wal_lsn) return error.IndexRebuilding;
        if (pending.changes.len != 0) {
            try overlay_api.requireNativeAncestry(root, base_id);
            overlay = try overlay_api.Overlay.init(oa, pending);
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
        break :blk try local.serverless_query_lake_serving.ServingSource.openCached(a, .{ .storage_mode = .relational, .external_base_source = source_schema }, options.lakeOptions(), context, &server.lake_read_cache);
    };
    defer source.deinit();
    try source.attachCache(&server.lake_read_cache, schema.binding, context);
    context = source.protectContext(context);
    var store = try Store.openNative(a, server.cfg.node_config, server.cfg.secret_store, req.remote_snapshot != null, server.cfg.deployment_mode, server.cfg.native_lake_artifact_base_dir);
    defer store.deinit();
    var arena = std.heap.ArenaAllocator.init(a);
    defer arena.deinit();
    const ca = arena.allocator();
    var sql_table = try server.sql_schema_cache.resolve(server.embedding_provider_runtime.io, ca, table.schema_json, table.table_id, table.name);
    sql_table.external_indexes = .{ .schema_json = table.schema_json, .catalog_json = table.lake_index_catalog_json, .indexes_json = table.indexes_json, .desired = local.metadata_lake_index_catalog.desiredFingerprint(table) };
    var selected = blk: {
        const publication_started = @import("antfly_platform").time.monotonicNs();
        defer server.lake_query_metrics.record(.publication, publication_started);
        break :blk (try @import("lake_index_selection.zig").selectCached(ca, sql_table, &source, &store, context, .required, &server.lake_read_cache)) orelse return error.ExternalLakeIndexUnavailable;
    };
    defer selected.deinit();
    const publication_bytes = try local.metadata_lake_index_catalog.encode(ca, .{ .namespace = selected.publication().namespace, .generation = selected.publication().generation, .published = selected.publication() });
    var snapshot_hash = std.crypto.hash.Blake3.init(.{});
    snapshot_hash.update("native-lake-search-snapshot-v1");
    var table_id: [8]u8 = undefined;
    std.mem.writeInt(u64, &table_id, table.table_id, .little);
    snapshot_hash.update(&table_id);
    snapshot_hash.update(publication_bytes);
    if (overlay) |value| {
        var lsn_bytes: [8]u8 = undefined;
        std.mem.writeInt(u64, &lsn_bytes, value.pending.lsn, .little);
        snapshot_hash.update("accepted-wal-overlay-v1");
        snapshot_hash.update(&lsn_bytes);
    }
    snapshot_hash.update(table.schema_json);
    var snapshot_digest: [32]u8 = undefined;
    snapshot_hash.final(&snapshot_digest);
    const snapshot_token = std.fmt.bytesToHex(snapshot_digest, .lower);
    if (req.remote_snapshot) |expected| {
        if (retained == null and !std.mem.eql(u8, expected, &snapshot_token)) return error.CatalogGenerationChanged;
    } else if (req.search_after.len != 0 or req.search_before.len != 0) return error.CatalogGenerationChanged;
    var owner: Execution = .{ .server = server, .table = sql_table, .source = &source, .store = &store, .domain = @import("lake_index_publication.zig").uploadDomainWithNamespace(table.table_id, store.identity, selected.publication().namespace), .declarations = selected.publication().declarations, .context = context, .request = normalized, .schema_json = table.schema_json, .arena = ca, .result_allocator = a, .overlay = if (overlay != null) &overlay.? else null };
    defer owner.deinit();
    var proof: @import("lake_index_row_source.zig").Provider = .{ .source = &source, .context = context, .expected_delete_objects = selected.delete_objects };
    const metadata = try server.lake_search_metadata.acquire(snapshot_digest, &proof);
    defer metadata.release();
    owner.files = metadata.files;
    owner.private_files = metadata.private_files;
    owner.private_digests = metadata.private_digests;
    const publication_finished = @import("antfly_platform").time.monotonicNs();
    var effective = req;
    effective.cancellation = .{ .ptr = &owner, .is_cancelled_fn = Execution.canceled };
    const has_vectors = build_recent or effective.dense != null or effective.sparse != null or effective.dense_queries.len != 0 or effective.sparse_queries.len != 0;
    const has_text = for (owner.declarations) |declaration| {
        if (declaration.artifact.kind == .text_segment) break true;
    } else false;
    if (has_vectors) if (owner.overlay) |pending_overlay| {
        owner.recent_declarations = if (retained) |cut| cut.recent else @import("lake_recent_vectors.zig").prepare(ca, current_table, selected.publication(), pending_overlay, owner.declarations, &store, context, .{ .antfly_provider = server.antfly_provider, .io = server.embedding_provider_runtime.io, .bounded_http_request = true, .deadline_ns = normalized.deadline_ns, .cancellation = retained_cancellation, .secret_store = server.cfg.secret_store, .remote_content = server.cfg.remote_content, .inference_api_url = server.configuredInferenceAPIURL(), .inference_api_key = server.cfg.inference_api_key, .provider_runtime = &server.embedding_provider_runtime, .source_table = table.name }, build_recent) catch |err| {
            server.notifyLakeCommit(table.name) catch {};
            return err;
        };
        // All declared vector recipes publish as one coherent recent cut.
        var expected: usize = 0;
        for (owner.declarations) |declaration| if (declaration.artifact.kind == .vector_segment or declaration.artifact.kind == .sparse_segment) {
            expected += 1;
        };
        if (owner.recent_declarations.len != expected) return error.IndexRebuilding;
    };
    if (build_recent) return null;
    if (has_vectors) {
        const resolver: @import("lake_index_text_predicate.zig").PhysicalResolver = .{ .server = server, .table = sql_table, .source = &source, .context = normalized, .store = store.artifactStore(), .store_identity = store.identity, .read_context = context, .pinned = .{ .artifacts = store.artifactStore(), .store_identity = store.identity, .domain = owner.domain, .declarations = owner.declarations, .read_context = context } };
        owner.vector_filter = if (effective.filter_query_json.len != 0) try std.json.parseFromSliceLeaky(std.json.Value, ca, effective.filter_query_json, .{}) else null;
        owner.vector_exclusion = if (effective.exclusion_query_json.len != 0) try std.json.parseFromSliceLeaky(std.json.Value, ca, effective.exclusion_query_json, .{}) else null;
        if (owner.overlay) |pending_overlay| {
            const replaced = (try resolver.resolve(ca, pending_overlay.key_filter)) orelse return error.UnsupportedQueryRequest;
            pending_overlay.physical = replaced.bitmap;
        }
        if (effective.filter_query_json.len != 0) {
            owner.vector_include_provider = try resolver.openMembership(a, effective.filter_query_json);
        }
        if (effective.exclusion_query_json.len != 0) {
            owner.vector_exclude_provider = try resolver.openMembership(a, effective.exclusion_query_json);
        }
    } else if (!has_text) try @import("lake_index_search_filter.zig").resolve(ca, sql_table, &source, request, &effective);
    owner.hydration_fields = try owner.planHydration(effective);
    owner.typed_delivery = owner.overlay == null and canDeliverTypedSource(effective);
    var execution_req = effective;
    // Retrieval/ranking for these requests needs identities and scores only.
    // Hydrate the final page once, after all result movement, into retained column pages.
    if (owner.typed_delivery) execution_req.include_stored = false;
    const started = @import("antfly_platform").time.monotonicNs();
    var result = blk: {
        defer server.lake_query_metrics.record(.ranking, started);
        break :blk if (execution_req.full_text_queries.len != 0 or execution_req.sparse_queries.len != 0 or execution_req.dense_queries.len != 0)
            try search.searchComposed(a, execution_req, .{ .ctx = &owner, .search_text_query = Execution.searchText, .search_text = Execution.dispatchText, .search_dense = Execution.searchDense, .search_sparse = Execution.searchSparse, .clone_named_set = Execution.cloneSet, .fuse_named_sets = Execution.fuseSets, .attach_graph_results = Execution.attachGraph })
        else if (execution_req.dense) |dense| try Execution.searchDense(&owner, a, execution_req, dense) else if (execution_req.sparse) |sparse| try Execution.searchSparse(&owner, a, execution_req, sparse) else if (execution_req.full_text) |text| try Execution.searchText(&owner, a, execution_req, text) else try Execution.dispatchText(&owner, a, execution_req);
    };
    defer result.deinit();
    const search_finished = @import("antfly_platform").time.monotonicNs();
    if (!owner.typed_delivery) try owner.attachHighlights(a, effective, &result);
    try context.ensureActive();
    // Retain only immutable serving metadata and recent row images. The archive
    // files/indexes remain shared, protected by their durable reader protocols.
    const serving_token: []const u8 = if (retained != null) req.remote_snapshot.? else if (req.remote_snapshot != null) &snapshot_token else cut: {
        var artifacts = store.artifactStore();
        if (cut_expires_ms <= @import("antfly_platform").time.realtimeNs() / std.time.ns_per_ms) return error.DeadlineExceeded;
        break :cut try retained_api.save(ra, &artifacts, store.identity, server.embedding_provider_runtime.io, .{ .expires_ms = cut_expires_ms, .table_id = table.table_id, .object_generation = table.object_storage_generation, .desired = local.metadata_lake_index_catalog.desiredFingerprint(current_table), .publication = publication, .reader_token = lease.retainedToken(), .pending = retained_pending, .catalog_metadata = retained_metadata, .published_only = published_only, .recent = owner.recent_declarations }, retained_cancellation);
    };
    var meta: local.api_query.QueryResponseMeta = .{ .remote_snapshot = serving_token, .shard_count = 1, .took_ms = @intCast((@import("antfly_platform").time.monotonicNs() -| total_started) / std.time.ns_per_ms) };
    defer meta.deinit(a);
    try @import("query_post_processing.zig").applyQueryPostProcessing(a, effective, &result, &meta, .{ .source_table = table.name, .backend_runtime = server.cfg.backend_runtime, .secret_store = server.cfg.secret_store, .remote_content = server.cfg.remote_content });
    var prepared_delivery = delivery;
    const lazy_hydration = owner.typed_delivery and !effective.count_only and (effective.include_stored or effective.highlight != null) and
        (if (delivery) |sink| sink.consume_columns and sink.spill_io != null else false);
    if (lazy_hydration) {
        owner.delivery_request = effective;
        prepared_delivery.?.hydrator = .{ .ptr = &owner, .load = Execution.hydrateDelivery, .release = Execution.releaseDelivery };
    } else if (owner.typed_delivery) {
        if (!effective.count_only and (effective.include_stored or effective.highlight != null)) try owner.hydrateTyped(a, effective, result.hits);
        try owner.attachHighlights(a, effective, &result);
    }
    meta.took_ms = @intCast((@import("antfly_platform").time.monotonicNs() -| total_started) / std.time.ns_per_ms);
    const response = try local.api_query.encodeQueryResponsesWithDelivery(a, table.name, effective, meta, result, prepared_delivery);
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
    server: *server_api.ApiHttpServer,
    overlay: ?*overlay_api.Overlay = null,
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
    unit_runtime: ?*@import("lake_index_native_runtime_cache.zig").Entry = null,
    recent_declarations: []const local.serverless_segment_sidecar_manifest.DeclaredArtifact = &.{},
    recent_dense_entries: std.StringHashMapUnmanaged(*local.storage_db_catalog_index_manager.IndexManager.DenseIndex) = .empty,
    recent_sparse_entries: std.StringHashMapUnmanaged(*local.storage_db_catalog_index_manager.IndexManager.SparseIndex) = .empty,
    vector_include_provider: ?@import("lake_index_text_predicate.zig").PhysicalResolver.Membership = null,
    vector_exclude_provider: ?@import("lake_index_text_predicate.zig").PhysicalResolver.Membership = null,
    vector_include: ?@import("lake_index_physical_set.zig").Set = null,
    vector_exclude: ?@import("lake_index_physical_set.zig").Set = null,
    typed_delivery: bool = false,
    predicate_exclusion_json: []const u8 = "",
    predicate_allow_partial: bool = true,
    delivery_request: ?types.SearchRequest = null,
    hydration_ns: u64 = 0,
    use_stored_highlights: bool = false,
    text_stores_source: std.StringHashMapUnmanaged(bool) = .empty,
    highlight_pins: std.ArrayList(search.PinnedTextSource) = .empty,
    highlight_queries: ?[]const search.HighlightQuery = null,
    arena: A,
    result_allocator: A = std.heap.page_allocator,
    files: std.StringHashMapUnmanaged([]const u8) = .empty,
    private_files: std.StringHashMapUnmanaged([]const u8) = .empty,
    private_digests: std.StringHashMapUnmanaged([]const u8) = .empty,
    text_identities: std.AutoHashMapUnmanaged(usize, @import("lake_index_text_predicate.zig").Identities) = .empty,
    sparse_entries: std.StringHashMapUnmanaged(*local.storage_db_catalog_index_manager.IndexManager.SparseIndex) = .empty,
    dense_entries: std.StringHashMapUnmanaged(*local.storage_db_catalog_index_manager.IndexManager.DenseIndex) = .empty,
    runtimes: std.ArrayList(*@import("lake_index_native_runtime_cache.zig").Entry) = .empty,
    fn vectorRequest(self: *Execution, req: types.SearchRequest) types.SearchRequest {
        var result = req;
        if (self.overlay != null or self.vector_include != null or self.vector_exclude != null or self.vector_include_provider != null or self.vector_exclude_provider != null) {
            result.native_key_predicate = .{ .ptr = self, .allows = allowsVectorKey, .select_constraints = selectSparseConstraints, .constraint_revision = sparseConstraintRevision, .select_range = selectSparseRange };
            result.filter_query_json = "";
            result.exclusion_query_json = "";
        }
        return result;
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
        if (self.vector_include_provider) |provider| if (provider.complete == null) return null;
        if (self.vector_exclude_provider) |provider| if (provider.complete == null) return null;
        const translate = lookup.native_range orelse return null;
        var result: types.SparseOrdinalSelection = .{ .include = .init(a) };
        errdefer result.deinit();
        if (!try translate(lookup.ptr, a, first, last, .{ .ptr = self, .allows = allowsVectorKey }, &result.include.?)) {
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
            const digest = self.private_digests.get(file.key_ptr.*) orelse return error.ExternalLakeSnapshotMismatch;
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
                if (!points_only) if (lookup.bounded_block) |translate| {
                    if (try translate(lookup.ptr, a, bytes_prefix, block.key_ptr.high, &selection, &result, work)) continue;
                } else {
                    // A legacy block callback is safe only for a small mask.
                    // Broad generations require the work-accounted directory.
                    if (selection.cardinality() <= work.points) {
                        try work.takeBlock();
                        if (try lookup.block(lookup.ptr, a, bytes_prefix, block.key_ptr.high, &selection, &result)) {
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
                    if (try lookup.one(lookup.ptr, bytes)) |num| try result.add(num);
                }
            }
        }
        return result;
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
        const file = self.private_files.get(key[6..70]) orelse return error.ExternalLakeSnapshotMismatch;
        if (self.overlay) |pending_overlay| if (pending_overlay.physical) |*mask| if (mask.contains(file, coordinate.group, coordinate.row)) return false;
        const ref: local.storage_rowsource_types.RowRef = .{ .external = .{ .source_id = self.source.inventory.source_id, .snapshot_id = self.source.inventory.snapshot_id, .file_id = file, .row_group_ordinal = coordinate.group, .row_ordinal = coordinate.row } };
        if (self.vector_include_provider) |*provider| if (!try provider.allows(ref)) return false;
        if (self.vector_exclude_provider) |*provider| if (try provider.allows(ref)) return false;
        if (self.vector_include) |*include| if (!include.contains(file, coordinate.group, coordinate.row)) return false;
        if (self.vector_exclude) |*exclude| if (exclude.contains(file, coordinate.group, coordinate.row)) return false;
        return true;
    }
    fn deinit(self: *Execution) void {
        if (self.vector_include_provider) |*provider| provider.deinit();
        if (self.vector_exclude_provider) |*provider| provider.deinit();
        for (self.highlight_pins.items) |*pin| pin.deinit();
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
        if (std.mem.startsWith(u8, key, "lake1:")) {
            if (!self.files.contains(key[6..70])) return error.ExternalLakeSnapshotMismatch;
            return units.rebind(a, member_key, key);
        }
        const file = self.private_files.get(key[6..70]) orelse return error.ExternalLakeSnapshotMismatch;
        const parent_key = try local.storage_rowsource_identity.allocId(a, .{ .external = .{ .source_id = self.source.inventory.source_id, .snapshot_id = self.source.inventory.snapshot_id, .file_id = file, .row_group_ordinal = position.group, .row_ordinal = position.row } });
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
        if (!std.mem.startsWith(u8, key, "lake1:")) return a.dupe(u8, member_key);
        const position = try @import("lake_index_native_state.zig").coordinates(key);
        const file = self.files.get(key[6..70]) orelse return a.dupe(u8, key);
        const digest = self.private_digests.get(file) orelse return error.ExternalLakeSnapshotMismatch;
        const parent_key = try std.fmt.allocPrint(a, "lake2:{s}:{x:0>8}:{x:0>16}", .{ digest, position.group, position.row });
        defer a.free(parent_key);
        return units.rebind(a, member_key, parent_key);
    }
    fn densePublicKey(raw: ?*anyopaque, a: A, _: *local.storage_db_catalog_index_manager.IndexManager.DenseIndex, key: []const u8) ![]u8 {
        return publicKey(raw, a, key);
    }
    fn acquire(raw: ?*anyopaque, name: ?[]const u8) !?search.PinnedTextSource {
        const self = from(raw);
        const started = @import("antfly_platform").time.monotonicNs();
        defer self.server.lake_query_metrics.record(.index, started);
        try self.context.ensureActive();
        var count: usize = 0;
        for (self.declarations) |declaration| if (declaration.artifact.kind == .text_segment and declaration.artifact.metadata_version == corpus.metadata_version) {
            count += 1;
        };
        const selected = for (self.declarations) |declaration| {
            if (declaration.artifact.kind == .text_segment and declaration.artifact.metadata_version == corpus.metadata_version and (if (name) |explicit| std.mem.eql(u8, explicit, declaration.name) else count == 1 or std.mem.eql(u8, declaration.name, local.common_full_text_index_defaults.default_full_text_index_name))) break declaration;
        } else {
            // A known text index awaiting a format refresh is rebuilding,
            // rather than an invalid user-supplied index name.
            for (self.declarations) |declaration| {
                if (declaration.artifact.kind == .text_segment and declaration.artifact.metadata_version != corpus.metadata_version and (name == null or std.mem.eql(u8, name.?, declaration.name))) return error.ExternalLakeIndexUnavailable;
            }
            return if (name == null) null else error.IndexNotFound;
        };
        const cached: @import("lake_index_aggregate_artifact.zig").CachedRead = .{ .cache = &self.server.lake_read_cache, .scope = self.store.identity, .context = self.context };
        const cancellation: @import("antfly_cancellation").CancellationToken = .{ .ptr = self, .is_cancelled_fn = canceled };
        const metadata = try @import("lake_index_decoded_metadata.zig").acquire(corpus.Root, cached, self.store.artifactStore(), selected.artifact, cancellation, corpus.loadRoot);
        defer metadata.release();
        const root = metadata.value.*;
        if (!std.mem.eql(u8, &root.domain, &self.domain)) return error.InvalidNativeLakeTextCorpus;
        if (!@import("../serverless/build/lake_rebuild.zig").bindingsEqual(root.binding, selected.binding)) return error.InvalidNativeLakeTextCorpus;
        const config = try std.json.parseFromSliceLeaky(std.json.Value, self.arena, root.config_json, .{});
        const source_recipe = corpus.sourceRecipe(.{ .table_id = self.table.id, .name = self.table.physical_name, .schema_json = self.schema_json }, root.config_json, true);
        const stores_source = try corpus.storesSource(config) and std.mem.eql(u8, &source_recipe, &root.recipe);
        try self.text_stores_source.put(self.arena, selected.name, stores_source);
        var pin = try self.server.lake_text_corpora.acquire(self.server.embedding_provider_runtime.io, self.store.artifactStore(), selected.artifact, root, self.schema_json, cached, self.context, cancellation);
        errdefer pin.deinit();
        const identities = @as(*const @import("lake_index_text_predicate.zig").Identities, @ptrCast(@alignCast(pin.provider_metadata orelse return error.InvalidNativeLakeTextCorpus))).*;
        if (self.overlay) |overlay| {
            const resolver: @import("lake_index_text_predicate.zig").Resolver = .{ .allow_partial = false, .server = self.server, .table = self.table, .source = self.source, .context = self.request, .identities = identities, .store = self.store.artifactStore(), .store_identity = self.store.identity, .read_context = self.context, .pinned = .{ .artifacts = self.store.artifactStore(), .store_identity = self.store.identity, .domain = self.domain, .declarations = self.declarations, .read_context = self.context } };
            var excluded = (try resolver.resolve(self.arena, overlay.key_filter)) orelse return error.UnsupportedQueryRequest;
            defer excluded.bitmap.deinit();
            if (!excluded.exact) return error.UnsupportedQueryRequest;
            const joined = try overlay.compose(pin, &excluded.bitmap, self.context);
            // compose consumed pin; the error cleanup must now own joined.
            pin = joined;
        }
        try self.text_identities.put(self.arena, @intFromPtr(pin.snapshot), identities);
        return pin;
    }
    fn attachHighlights(self: *Execution, a: A, req: types.SearchRequest, result: *types.SearchResult) !void {
        const options = req.highlight orelse return;
        if (req.defer_hierarchy_child_hydration or result.hits.len == 0) return;
        if (req.full_text == null and req.full_text_queries.len == 0) return;
        const started = @import("antfly_platform").time.monotonicNs();
        defer self.server.lake_query_metrics.record(.highlight, started);
        try self.context.ensureActive();
        if (self.highlight_queries == null) {
            var queries: std.ArrayList(search.HighlightQuery) = .empty;

            if (req.full_text_queries.len != 0) {
                for (req.full_text_queries) |named| {
                    var pin = (try acquire(self, named.index_name)) orelse continue;
                    self.highlight_pins.append(self.arena, pin) catch |err| {
                        pin.deinit();
                        return err;
                    };
                    try queries.append(self.arena, .{ .query = named.query, .text_analysis = pin.text_analysis, .runtime_schema = pin.runtime_schema, .selected_field = pin.selected_field });
                }
            } else if (req.full_text) |query| {
                var pin = (try acquire(self, req.primary_text_index_name orelse req.index_name)) orelse return;
                self.highlight_pins.append(self.arena, pin) catch |err| {
                    pin.deinit();
                    return err;
                };
                try queries.append(self.arena, .{ .query = query, .text_analysis = pin.text_analysis, .runtime_schema = pin.runtime_schema, .selected_field = pin.selected_field });
            }
            self.highlight_queries = queries.items;
        }
        if (self.highlight_queries.?.len == 0) return;
        // Highlight the original source even when result shaping projected it
        // away. Hydration keeps the same snapshot, deletes and reader lease.
        var sources: ?[]?[]u8 = null;
        defer if (sources) |items| {
            for (items) |bytes| if (bytes) |owned| a.free(owned);
            a.free(items);
        };
        if (self.use_stored_highlights) {
            sources = try self.loadStoredHighlights(a, result.hits);
        } else if (!self.typed_delivery and (!req.include_stored or (!req.include_all_fields and !req.defer_stored_projection))) {
            const keys = try a.alloc([]const u8, result.hits.len);
            defer a.free(keys);
            for (result.hits, keys) |hit, *key| key.* = hit.id;
            sources = try loadManySelected(self, a, keys, self.hydration_fields);
        }
        try search.attachHighlightsWithIndexQueries(a, options, self.highlight_queries.?, result.hits, sources);
        try self.context.ensureActive();
    }
    fn loadStoredHighlights(self: *Execution, a: A, hits: []const types.SearchHit) ![]?[]u8 {
        const started = @import("antfly_platform").time.monotonicNs();
        defer self.hydration_ns +|= @import("antfly_platform").time.monotonicNs() -| started;
        if (self.highlight_pins.items.len != 1) return error.InvalidNativeLakeTextCorpus;
        const pin = self.highlight_pins.items[0];
        const identities = self.text_identities.get(@intFromPtr(pin.snapshot)) orelse return error.InvalidNativeLakeTextCorpus;
        const result = try a.alloc(?[]u8, hits.len);
        @memset(result, null);
        errdefer {
            for (result) |value| if (value) |bytes| a.free(bytes);
            a.free(result);
        }
        var live: @import("lake_index_text_predicate.zig").LiveRowsCache = .{};
        defer live.deinit(a);
        const cached: @import("lake_index_aggregate_artifact.zig").CachedRead = .{ .cache = &self.server.lake_read_cache, .scope = self.store.identity, .context = self.context };
        for (hits, result) |hit, *out| {
            try self.context.ensureActive();
            var scratch = std.heap.ArenaAllocator.init(a);
            defer scratch.deinit();
            const sa = scratch.allocator();
            const key = try publicKey(self, sa, hit.id);
            const position = try @import("lake_index_native_state.zig").coordinates(key);
            const file = self.files.get(key[6..70]) orelse return error.ExternalLakeSnapshotMismatch;
            const row: local.storage_rowsource_types.RowRef = .{ .external = .{ .source_id = self.source.inventory.source_id, .snapshot_id = self.source.inventory.snapshot_id, .file_id = file, .row_group_ordinal = position.group, .row_ordinal = position.row } };
            const ordinal = (try identities.ordinal(a, &live, self.store.artifactStore(), cached, row)) orelse return error.InvalidNativeLakeTextCorpus;
            const doc = (try pin.snapshot.storedDocDecompressed(a, ordinal)) orelse return error.InvalidNativeLakeTextCorpus;
            errdefer a.free(doc.data);
            const expected = try nativeKey(self, sa, key);
            if (!std.mem.eql(u8, doc.id, expected)) return error.InvalidNativeLakeTextCorpus;
            const value = try std.json.parseFromSliceLeaky(std.json.Value, sa, doc.data, .{ .parse_numbers = false });
            if (value != .object) return error.InvalidNativeLakeTextCorpus;
            out.* = doc.data;
        }
        return result;
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
        var pin: ?search.PinnedTextSource = null;
        defer if (pin) |*value| value.deinit();
        const one_text_source = (req.full_text != null and req.full_text_queries.len == 0) or (req.full_text == null and req.full_text_queries.len == 1);
        if (one_text_source and req.dense_queries.len == 0 and req.sparse_queries.len == 0 and req.dense == null and req.sparse == null) {
            const bound = for (hits) |hit| {
                if (hit.native_text_doc_id == null) break false;
            } else true;
            if (bound) pin = try acquire(self, if (req.full_text_queries.len == 1) req.full_text_queries[0].index_name else req.primary_text_index_name orelse req.index_name);
        }
        try self.hydrateTypedFromSource(a, hits, keys, if (pin) |*value| value else null);
    }
    fn hydrateTypedFromSource(self: *Execution, a: A, hits: []types.SearchHit, keys: []const []const u8, pin: ?*const search.PinnedTextSource) !void {
        if (pin == null or pin.?.stored_projection_fields.len == 0) {
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
        if (stored_fields.len != 0) for (hits, values) |hit, *value| {
            try self.context.ensureActive();
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
        // Keep display columns on the shared Parquet projection, but do not
        // fetch a potentially large body column solely for highlighting when
        // the selected immutable text index explicitly retains that source.
        if (req.full_text != null and req.full_text_queries.len == 0) {
            if (try acquire(self, req.primary_text_index_name orelse req.index_name)) |source| {
                var pin = source;
                defer pin.deinit();
                if (storedHighlightEligible(req, pin.selected_field, self.text_stores_source.get(pin.name) orelse false)) {
                    self.use_stored_highlights = true;
                    return fields.items;
                }
            }
        }
        try appendHydrationPath(self.arena, self.table, &fields, "_type");
        if (options.fields.len != 0) {
            for (options.fields) |path| try appendHydrationPath(self.arena, self.table, &fields, path);
        } else if (req.full_text_queries.len != 0) {
            for (req.full_text_queries) |named| {
                var pin = (try acquire(self, named.index_name)) orelse continue;
                defer pin.deinit();
                if (!try appendIndexHydration(self.arena, self.table, &fields, pin)) return null;
            }
        } else {
            var pin = (try acquire(self, req.primary_text_index_name orelse req.index_name)) orelse return fields.items;
            defer pin.deinit();
            if (!try appendIndexHydration(self.arena, self.table, &fields, pin)) return null;
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
        if (self.overlay) |overlay| if (overlay.physical == null) {
            const resolver: @import("lake_index_text_predicate.zig").PhysicalResolver = .{ .allow_partial = false, .server = self.server, .table = self.table, .source = self.source, .context = self.request, .store = self.store.artifactStore(), .store_identity = self.store.identity, .read_context = self.context, .pinned = .{ .artifacts = self.store.artifactStore(), .store_identity = self.store.identity, .domain = self.domain, .declarations = self.declarations, .read_context = self.context } };
            const excluded = (try resolver.resolve(self.arena, overlay.key_filter)) orelse return error.UnsupportedQueryRequest;
            overlay.physical = excluded.bitmap;
        };
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
                    const file = self.files.get(row.id[6..70]) orelse return error.ExternalLakeSnapshotMismatch;
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
    fn openOrderedTextCandidates(raw: ?*anyopaque, a: A, req: types.SearchRequest, snapshot: *const local.index.IndexSnapshot) !?search.OrderedTextCandidates {
        const self = from(raw);
        if (req.order_by.len < 2 or !std.mem.eql(u8, req.order_by[req.order_by.len - 1].field, "_id")) return null;
        const identities = self.text_identities.get(@intFromPtr(snapshot)) orelse return null;
        var arena = std.heap.ArenaAllocator.init(a);
        var keep_arena = false;
        defer if (!keep_arena) arena.deinit();
        const ca = arena.allocator();
        const orders = try ca.alloc(local.sql_catalog.Scan.Order, req.order_by.len - 1);
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
            const parsed = try std.json.parseFromSlice(std.json.Value, ca, req.filter_query_json, .{});
            const compiled = try local.storage_db_query_graph_exec.compilePatternFilter(ca, parsed.value);
            var required: std.ArrayList(local.sql_catalog.Condition) = .empty;
            try @import("lake_index_text_predicate.zig").requiredConditions(ca, compiled, &required, 0);
            for (required.items) |condition| {
                if (try sql_rows.compatibleSearchConditions(ca, self.table, &.{condition})) try conditions.append(ca, condition);
            }
        }
        // Seek inclusively on the leading field. Public IDs break ties in the
        // native collector, so an entire boundary tie group must remain visible.
        if (req.search_after.len != 0 or req.search_before.len != 0) {
            const bound = if (req.search_before.len != 0) req.search_before[0] else req.search_after[0];
            if (bound == .null) return null;
            const kind = (try self.table.column(orders[0].column)).type;
            const normalized = local.sql_lake_values.comparisonValue(ca, bound, kind) catch |err| switch (err) {
                error.OutOfMemory => return err,
                else => return null,
            };
            const condition: local.sql_catalog.Condition = .{ .column = orders[0].column, .op = if (orders[0].descending != (req.search_before.len != 0)) .lte else .gte, .value = normalized };
            if (!try sql_rows.compatibleSearchConditions(ca, self.table, &.{condition})) return null;
            try conditions.append(ca, condition);
        }
        var predicate = (try sql_rows.tryOpenOrderedPredicate(a, self.server, self.table, .{ .fields = &.{}, .conditions = conditions.items, .order = orders, .limit = 256, .row_goal = @as(u64, req.offset) + req.limit }, self.request, self.source, req.search_before.len != 0, .{ .values = if (req.search_before.len != 0) req.search_before else req.search_after, .id_descending = req.order_by[req.order_by.len - 1].desc })) orelse return null;
        errdefer predicate.deinit();
        const owner = try a.create(Ordered);
        owner.* = .{ .a = a, .execution = self, .predicate = predicate, .plan_arena = arena, .window = .init(a), .identities = identities, .page_size = if (predicate.complete_order) @min(256, @as(usize, req.offset) + req.limit + 1) else 256 };
        keep_arena = true;
        return .{ .ptr = owner, .next = Ordered.next, .scanned_count = Ordered.scannedCount, .complete_order = predicate.complete_order, .close = Ordered.close };
    }
    fn searchText(raw: ?*anyopaque, a: A, req: types.SearchRequest, text: types.TextQuery) !types.SearchResult {
        const self = from(raw);
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
        return search.searchTextQuery(a, req, text, .{ .ctx = raw, .exact_doc_id_filters = true, .acquire_text_source = acquire, .resolve_indexed_filter = resolveIndexedFilter, .open_ordered_candidates = openOrderedTextCandidates, .native_count_visibility_exact = true, .project_key = publicKey, .native_key = nativeKey, .filter_candidate_presence = true, .text_index_entry = noLocal, .text_index_is_chunk_backed = chunkBacked, .search_match_all = matchAll, .project_stored_search = project, .load_stored = loadOne, .load_projected_documents = loadProjected, .postprocess = postprocess });
    }
    fn dispatchText(raw: ?*anyopaque, a: A, req: types.SearchRequest) !types.SearchResult {
        return search.searchText(a, req, .{ .ctx = raw, .func = searchText });
    }
    fn denseIndex(raw: ?*anyopaque, name: ?[]const u8) !?*local.storage_db_catalog_index_manager.IndexManager.DenseIndex {
        const self = from(raw);
        try self.context.ensureActive();
        const native = @import("lake_index_native_dense.zig");
        const declarations = if (self.active_recent) self.recent_declarations else self.declarations;
        const entries = if (self.active_recent) &self.recent_dense_entries else &self.dense_entries;
        var count: usize = 0;
        for (declarations) |declaration| if (declaration.artifact.kind == .vector_segment and declaration.artifact.metadata_version == native.metadata_version) {
            count += 1;
        };
        const selected = for (declarations) |declaration| {
            if (declaration.artifact.kind == .vector_segment and declaration.artifact.metadata_version == native.metadata_version and (if (name) |explicit| std.mem.eql(u8, explicit, declaration.name) else count == 1)) break declaration;
        } else return error.IndexNotFound;
        if (entries.get(selected.name)) |entry| {
            for (self.runtimes.items) |runtime| if (runtime.dense_entry == entry) {
                self.unit_runtime = runtime;
                break;
            };
            return entry;
        }
        try self.runtimes.ensureUnusedCapacity(self.arena, 1);
        const runtime = try self.server.lake_native_runtimes.acquire(self.server, selected, try @import("lake_recent_vectors.zig").runtimeDomain(selected), self.store.identity, self.context);
        errdefer runtime.release();
        const entry = runtime.dense_entry.?;
        try entries.put(self.arena, selected.name, entry);
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
    fn searchDense(raw: ?*anyopaque, a: A, req: types.SearchRequest, query: types.DenseKnnQuery) !types.SearchResult {
        const self = from(raw);
        if (self.recent_declarations.len == 0) return searchDensePart(raw, a, req, query);
        var leaf = req;
        leaf.offset = 0;
        leaf.limit = std.math.add(u32, req.offset, req.limit) catch return error.QueryCandidateBudgetExceeded;
        self.active_recent = false;
        var base = try searchDensePart(raw, a, leaf, query);
        defer base.deinit();
        self.active_recent = true;
        defer self.active_recent = false;
        var recent = try searchDensePart(raw, a, leaf, query);
        defer recent.deinit();
        return local.api_query.mergeSearchResults(a, req, &.{ base, recent }, req.offset, req.limit);
    }
    fn searchDensePart(raw: ?*anyopaque, a: A, req: types.SearchRequest, dense: types.DenseKnnQuery) !types.SearchResult {
        return search.searchDense(a, from(raw).vectorRequest(req), dense, .{ .ctx = raw, .exact_doc_id_filters = true, .filter_candidate_presence = true, .text_index_entry = noLocal, .dense_index = denseIndex, .lookup_doc_key = lookupDocKey, .resolve_hit_key = densePublicKey, .lookup_vector_id = lookupVectorId, .load_projected_document = requireProjected, .load_projected_documents = loadProjected, .hbc_search = denseSearch, .hbc_search_profiled = denseSearchProfiled, .postprocess = postprocessVector });
    }
    fn sparseIndex(raw: ?*anyopaque, name: ?[]const u8) !?*local.storage_db_catalog_index_manager.IndexManager.SparseIndex {
        const self = from(raw);
        try self.context.ensureActive();
        const native = @import("lake_index_native_sparse.zig");
        const declarations = if (self.active_recent) self.recent_declarations else self.declarations;
        const entries = if (self.active_recent) &self.recent_sparse_entries else &self.sparse_entries;
        var count: usize = 0;
        for (declarations) |declaration| if (declaration.artifact.kind == .sparse_segment and declaration.artifact.metadata_version == native.metadata_version) {
            count += 1;
        };
        const selected = for (declarations) |declaration| {
            if (declaration.artifact.kind == .sparse_segment and declaration.artifact.metadata_version == native.metadata_version and (if (name) |explicit| std.mem.eql(u8, explicit, declaration.name) else count == 1)) break declaration;
        } else return error.IndexNotFound;
        if (entries.get(selected.name)) |entry| {
            for (self.runtimes.items) |runtime| if (runtime.sparse_entry == entry) {
                self.unit_runtime = runtime;
                break;
            };
            return entry;
        }
        try self.runtimes.ensureUnusedCapacity(self.arena, 1);
        const runtime = try self.server.lake_native_runtimes.acquire(self.server, selected, try @import("lake_recent_vectors.zig").runtimeDomain(selected), self.store.identity, self.context);
        errdefer runtime.release();
        const entry = runtime.sparse_entry.?;
        try entries.put(self.arena, selected.name, entry);
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
        self.active_recent = false;
        var base = try searchSparsePart(raw, a, leaf, query);
        defer base.deinit();
        self.active_recent = true;
        defer self.active_recent = false;
        var recent = try searchSparsePart(raw, a, leaf, query);
        defer recent.deinit();
        return local.api_query.mergeSearchResults(a, req, &.{ base, recent }, req.offset, req.limit);
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
    fn unitPayload(self: *Execution, a: A, key: []const u8) ![]u8 {
        try self.context.ensureActive();
        const unit_key = local.storage_internal_keys.isDocumentUnitArtifactRecordKey(key);
        const chunk_key = local.storage_internal_keys.isChunkArtifactRecordKey(key);
        var artifact_ref = if (unit_key or chunk_key) (try local.storage_db_artifact_ids.decodeArtifactRefAlloc(a, key)) orelse return error.InvalidChunkArtifact else null;
        defer if (artifact_ref) |*ref| ref.deinit(a);
        const public_parent = if (artifact_ref) |ref| ref.document_id else try @import("lake_enrichment_units.zig").parent(key);
        var runtime = self.unit_runtime orelse return error.InvalidChunkArtifact;
        if (artifact_ref) |ref| {
            const suffix = if (unit_key) "_sources" else "_chunks";
            if (!std.mem.endsWith(u8, ref.name, suffix)) return error.InvalidChunkArtifact;
            const index_name = ref.name[0 .. ref.name.len - suffix.len];
            const recent = if (self.overlay) |overlay| overlay.row(public_parent) != null else false;
            runtime = for (self.runtimes.items) |candidate| {
                if (candidate.dense_entry) |entry| {
                    if (std.mem.eql(u8, index_name, entry.config.name) and ((self.recent_dense_entries.get(index_name) == entry) == recent)) break candidate;
                } else if (candidate.sparse_entry) |entry| {
                    if (std.mem.eql(u8, index_name, entry.config.name) and ((self.recent_sparse_entries.get(index_name) == entry) == recent)) break candidate;
                }
            } else return error.InvalidChunkArtifact;
        }
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
                    if (key.len != 96 or !std.mem.startsWith(u8, key, "lake1:") or key[70] != ':' or key[79] != ':') return error.ExternalLakeSnapshotMismatch;
                    const file = self.files.get(key[6..70]) orelse return error.ExternalLakeSnapshotMismatch;
                    ref.* = .{ .external = .{ .source_id = self.source.inventory.source_id, .snapshot_id = self.source.inventory.snapshot_id, .file_id = file, .row_group_ordinal = std.fmt.parseUnsigned(u32, key[71..79], 16) catch return error.ExternalLakeSnapshotMismatch, .row_ordinal = std.fmt.parseUnsigned(u64, key[80..96], 16) catch return error.ExternalLakeSnapshotMismatch } };
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
fn appendIndexHydration(a: A, table: local.sql_catalog.Table, fields: *std.ArrayList([]const u8), pin: search.PinnedTextSource) !bool {
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
    const data = try local.serverless_query_lake_parquet_rowgroup.buildTestPlainI64AndByteArrayParquetObjectAlloc(a, &.{}, &.{.{ .column_id = "body", .field_id = 1, .converted_type = 0, .values = &.{ "a needle in the haystack", "second needle" } }});
    defer a.free(data);
    var put = try client.putObject("antfly", "part.parquet", data, .{});
    put.deinit(a);
    const schema_json = try std.fmt.allocPrint(a,
        \\{{"version":1,"storage_mode":"relational","default_type":"row","base_source":{{"kind":"external","table_id":"lake","format":"parquet","uri":"file://{s}","schema_fingerprint":"schema"}},"document_schemas":{{"row":{{"schema":{{"type":"object","properties":{{"body":{{"type":"string"}}}},"additionalProperties":false}}}}}}}}
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
    var server: server_api.ApiHttpServer = undefined;
    server.lake_query_metrics = .{};
    server.lake_read_cache = local.serverless_query_lake_serving_cache.Cache.init(a);
    defer server.lake_read_cache.deinit();
    var owner: Execution = .{ .server = &server, .table = .{ .id = 7, .physical_name = "lake", .schema_version = 1, .columns = &.{} }, .source = &source, .store = &store, .domain = root.domain, .declarations = &.{}, .context = .{}, .request = .{}, .schema_json = schema_json, .arena = ca, .use_stored_highlights = true };
    defer owner.deinit();
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
    const values = try owner.loadStoredHighlights(a, &hits);
    defer {
        for (values) |value| if (value) |bytes| a.free(bytes);
        a.free(values);
    }
    var parsed = try std.json.parseFromSlice(std.json.Value, a, values[0].?, .{});
    defer parsed.deinit();
    try std.testing.expectEqualStrings("a needle in the haystack", parsed.value.object.get("body").?.string);
    const query: types.TextQuery = .{ .match = .{ .field = "body", .text = "needle" } };
    owner.highlight_queries = &.{.{ .query = query, .text_analysis = .{}, .runtime_schema = null, .selected_field = "body" }};
    var result: types.SearchResult = .{ .alloc = a, .hits = &hits, .total_hits = 1, .graph_results = &.{} };
    defer types.freeHighlights(a, hits[0].highlights);
    try owner.attachHighlights(a, .{ .full_text = query, .highlight = .{ .fields = &.{"body"} }, .include_stored = false }, &result);
    try std.testing.expectEqual(@as(usize, 1), hits[0].highlights.len);
    try std.testing.expectEqual(@as(u64, 0), server.lake_read_cache.snapshot().provider_reads);
    owner.context.deadline_ns = 0;
    try std.testing.expectError(error.DeadlineExceeded, owner.loadStoredHighlights(a, &hits));
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
    owner.server = &server;
    owner.arena = arena.allocator();
    owner.context = .{};
    owner.declarations = &.{};
    owner.use_stored_highlights = false;
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

test "external lake sparse predicate planning defers broad masks and subtracts selective includes" {
    const a = std.testing.allocator;
    const Set = @import("lake_index_physical_set.zig").Set;
    // Only the predicate-planning fields are needed; no HTTP/server is opened.
    var execution: Execution = undefined;
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
    var execution: Execution = undefined;
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
    execution.overlay = null;
    execution.active_recent = false;
    var source: local.serverless_query_lake_serving.ServingSource = undefined;
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
                if (try filter.allows(filter.ptr, encoded)) try result.add(row);
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
    var execution: Execution = undefined;
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
