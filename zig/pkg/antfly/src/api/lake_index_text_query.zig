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
pub fn execute(a: A, server: *server_api.ApiHttpServer, table: local.common_topology_records.TableRecord, req: types.SearchRequest, request: local.api_operation.RequestContext) !?local.api_query.QueryResponse {
    return executePinned(a, server, table, req, request) catch |err| switch (err) {
        error.ExternalLakeIndexNotPublished, error.ExternalLakeIndexUnavailable => error.IndexRebuilding,
        error.ExternalLakeIndexDefinitionChanged, error.ExternalLakeIndexStoreChanged, error.ExternalLakeIndexCredentialsChanged, error.ExternalLakeIndexSourceChanged, error.ExternalLakeSnapshotMismatch => error.CatalogGenerationChanged,
        error.LakeIndexReaderLeaseExpired, error.NativeLakeTextCacheBusy, error.NativeLakeRuntimeCacheBusy => error.StorageReadTemporarilyUnavailable,
        error.NativeLakeTextCorpusTooLarge => error.QueryCandidateBudgetExceeded,
        error.IndexNotFound => error.InvalidQueryRequest,
        else => err,
    };
}
fn executePinned(a: A, server: *server_api.ApiHttpServer, table: local.common_topology_records.TableRecord, req: types.SearchRequest, request: local.api_operation.RequestContext) !?local.api_query.QueryResponse {
    var schema = (try local.serverless_external_source_schema_binding.externalBindingFromSchemaJsonAlloc(a, table.schema_json)) orelse return null;
    defer schema.deinit(a);
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
    const lease = try server.lake_reader_leases.acquire(server.embedding_provider_runtime.io, authority, table.table_id, publication.generation, context);
    defer lease.deinit();
    context = lease.readContext();
    try server.prepareLakeCache();
    const options: @import("../serverless/configured_object_store_support.zig").BindingObjectStoreOpenOptions = .{ .node_config = server.cfg.node_config, .secret_store = server.cfg.secret_store };
    var source = try local.serverless_query_lake_serving.ServingSource.openCached(a, .{ .storage_mode = .relational, .external_base_source = schema }, options.lakeOptions(), context, &server.lake_read_cache);
    defer source.deinit();
    var store = try Store.openNative(a, server.cfg.node_config, server.cfg.secret_store, true, server.cfg.deployment_mode, server.cfg.native_lake_artifact_base_dir);
    defer store.deinit();
    var arena = std.heap.ArenaAllocator.init(a);
    defer arena.deinit();
    const ca = arena.allocator();
    var sql_table = try server.sql_schema_cache.resolve(server.embedding_provider_runtime.io, ca, table.schema_json, table.table_id, table.name);
    sql_table.external_indexes = .{ .schema_json = table.schema_json, .catalog_json = table.lake_index_catalog_json, .indexes_json = table.indexes_json, .desired = local.metadata_lake_index_catalog.desiredFingerprint(table) };
    var selected = (try @import("lake_index_selection.zig").selectCached(ca, sql_table, &source, &store, context, .required, &server.lake_read_cache)) orelse return error.ExternalLakeIndexUnavailable;
    defer selected.deinit();
    const publication_bytes = try local.metadata_lake_index_catalog.encode(ca, .{ .namespace = selected.publication().namespace, .generation = selected.publication().generation, .published = selected.publication() });
    var snapshot_hash = std.crypto.hash.Blake3.init(.{});
    snapshot_hash.update("native-lake-search-snapshot-v1");
    var table_id: [8]u8 = undefined;
    std.mem.writeInt(u64, &table_id, table.table_id, .little);
    snapshot_hash.update(&table_id);
    snapshot_hash.update(publication_bytes);
    snapshot_hash.update(table.schema_json);
    var snapshot_digest: [32]u8 = undefined;
    snapshot_hash.final(&snapshot_digest);
    const snapshot_token = std.fmt.bytesToHex(snapshot_digest, .lower);
    if (req.remote_snapshot) |expected| {
        if (!std.mem.eql(u8, expected, &snapshot_token)) return error.CatalogGenerationChanged;
    } else if (req.search_after.len != 0 or req.search_before.len != 0) return error.CatalogGenerationChanged;
    var owner: Execution = .{ .server = server, .table = sql_table, .source = &source, .store = &store, .domain = @import("lake_index_publication.zig").uploadDomainWithNamespace(table.table_id, store.identity, selected.publication().namespace), .declarations = selected.publication().declarations, .context = context, .request = normalized, .schema_json = table.schema_json, .arena = ca, .result_allocator = a };
    defer owner.deinit();
    var proof: @import("lake_index_row_source.zig").Provider = .{ .source = &source, .context = context, .expected_delete_objects = selected.delete_objects };
    const metadata = try server.lake_search_metadata.acquire(snapshot_digest, &proof);
    defer metadata.release();
    owner.files = metadata.files;
    owner.private_files = metadata.private_files;
    owner.private_digests = metadata.private_digests;
    var effective = req;
    effective.cancellation = .{ .ptr = &owner, .is_cancelled_fn = Execution.canceled };
    owner.hydration_fields = try owner.planHydration(effective);
    owner.typed_delivery = canDeliverTypedSource(effective);
    var execution_req = effective;
    // Retrieval/ranking for these requests needs identities and scores only.
    // Hydrate the final page once, after all result movement, into owned values.
    if (owner.typed_delivery) execution_req.include_stored = false;
    const started = @import("antfly_platform").time.monotonicNs();
    var result = if (execution_req.full_text_queries.len != 0 or execution_req.sparse_queries.len != 0 or execution_req.dense_queries.len != 0)
        try search.searchComposed(a, execution_req, .{ .ctx = &owner, .search_text_query = Execution.searchText, .search_text = Execution.dispatchText, .search_dense = Execution.searchDense, .search_sparse = Execution.searchSparse, .clone_named_set = Execution.cloneSet, .fuse_named_sets = Execution.fuseSets, .attach_graph_results = Execution.attachGraph })
    else if (execution_req.dense) |dense| try Execution.searchDense(&owner, a, execution_req, dense) else if (execution_req.sparse) |sparse| try Execution.searchSparse(&owner, a, execution_req, sparse) else if (execution_req.full_text) |text| try Execution.searchText(&owner, a, execution_req, text) else try Execution.dispatchText(&owner, a, execution_req);
    defer result.deinit();
    if (!owner.typed_delivery) try owner.attachHighlights(a, effective, &result);
    try context.ensureActive();
    var meta: local.api_query.QueryResponseMeta = .{ .remote_snapshot = &snapshot_token, .shard_count = 1, .took_ms = @intCast((@import("antfly_platform").time.monotonicNs() -| started) / std.time.ns_per_ms) };
    defer meta.deinit(a);
    try @import("table_reads.zig").applyQueryPostProcessing(a, effective, &result, &meta, .{ .source_table = table.name, .backend_runtime = server.cfg.backend_runtime, .secret_store = server.cfg.secret_store, .remote_content = server.cfg.remote_content });
    if (owner.typed_delivery) {
        if (!effective.count_only and (effective.include_stored or effective.highlight != null)) try owner.hydrateTyped(a, result.hits);
        try owner.attachHighlights(a, effective, &result);
    }
    meta.took_ms = @intCast((@import("antfly_platform").time.monotonicNs() -| started) / std.time.ns_per_ms);
    return try local.api_query.encodeQueryResponses(a, table.name, effective, meta, result);
}
const Execution = struct {
    server: *server_api.ApiHttpServer,
    table: local.sql_catalog.Table,
    source: *local.serverless_query_lake_serving.ServingSource,
    store: *Store,
    domain: [32]u8,
    declarations: []const local.serverless_segment_sidecar_manifest.DeclaredArtifact,
    context: Context,
    request: local.api_operation.RequestContext,
    schema_json: []const u8,
    hydration_fields: ?[]const []const u8 = null,
    typed_delivery: bool = false,
    arena: A,
    result_allocator: A = std.heap.page_allocator,
    files: std.StringHashMapUnmanaged([]const u8) = .empty,
    private_files: std.StringHashMapUnmanaged([]const u8) = .empty,
    private_digests: std.StringHashMapUnmanaged([]const u8) = .empty,
    sparse_entries: std.StringHashMapUnmanaged(*local.storage_db_catalog_index_manager.IndexManager.SparseIndex) = .empty,
    dense_entries: std.StringHashMapUnmanaged(*local.storage_db_catalog_index_manager.IndexManager.DenseIndex) = .empty,
    runtimes: std.ArrayList(*@import("lake_index_native_runtime_cache.zig").Entry) = .empty,
    fn deinit(self: *Execution) void {
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
    fn publicKey(raw: ?*anyopaque, a: A, key: []const u8) ![]u8 {
        const self = from(raw);
        const position = try @import("lake_index_native_state.zig").coordinates(key);
        if (std.mem.startsWith(u8, key, "lake1:")) {
            if (!self.files.contains(key[6..70])) return error.ExternalLakeSnapshotMismatch;
            return a.dupe(u8, key);
        }
        const file = self.private_files.get(key[6..70]) orelse return error.ExternalLakeSnapshotMismatch;
        return local.storage_rowsource_identity.allocId(a, .{ .external = .{ .source_id = self.source.inventory.source_id, .snapshot_id = self.source.inventory.snapshot_id, .file_id = file, .row_group_ordinal = position.group, .row_ordinal = position.row } });
    }
    fn nativeKey(raw: ?*anyopaque, a: A, key: []const u8) ![]u8 {
        const self = from(raw);
        if (!std.mem.startsWith(u8, key, "lake1:")) return a.dupe(u8, key);
        const position = try @import("lake_index_native_state.zig").coordinates(key);
        const file = self.files.get(key[6..70]) orelse return a.dupe(u8, key);
        const digest = self.private_digests.get(file) orelse return error.ExternalLakeSnapshotMismatch;
        return std.fmt.allocPrint(a, "lake2:{s}:{x:0>8}:{x:0>16}", .{ digest, position.group, position.row });
    }
    fn densePublicKey(raw: ?*anyopaque, a: A, _: *local.storage_db_catalog_index_manager.IndexManager.DenseIndex, key: []const u8) ![]u8 {
        return publicKey(raw, a, key);
    }
    fn acquire(raw: ?*anyopaque, name: ?[]const u8) !?search.PinnedTextSource {
        const self = from(raw);
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
        return try self.server.lake_text_corpora.acquire(self.server.embedding_provider_runtime.io, self.store.artifactStore(), selected.artifact, root, self.schema_json, cached, self.context, cancellation);
    }
    fn attachHighlights(self: *Execution, a: A, req: types.SearchRequest, result: *types.SearchResult) !void {
        const options = req.highlight orelse return;
        if (req.defer_hierarchy_child_hydration or result.hits.len == 0) return;
        if (req.full_text == null and req.full_text_queries.len == 0) return;
        try self.context.ensureActive();
        var pins: std.ArrayList(search.PinnedTextSource) = .empty;
        defer {
            for (pins.items) |*pin| pin.deinit();
            pins.deinit(a);
        }
        var queries: std.ArrayList(search.HighlightQuery) = .empty;
        defer queries.deinit(a);
        if (req.full_text_queries.len != 0) {
            for (req.full_text_queries) |named| {
                var pin = (try acquire(self, named.index_name)) orelse continue;
                pins.append(a, pin) catch |err| {
                    pin.deinit();
                    return err;
                };
                try queries.append(a, .{ .query = named.query, .text_analysis = pin.text_analysis, .runtime_schema = pin.runtime_schema, .selected_field = pin.selected_field });
            }
        } else if (req.full_text) |query| {
            var pin = (try acquire(self, req.primary_text_index_name orelse req.index_name)) orelse return;
            pins.append(a, pin) catch |err| {
                pin.deinit();
                return err;
            };
            try queries.append(a, .{ .query = query, .text_analysis = pin.text_analysis, .runtime_schema = pin.runtime_schema, .selected_field = pin.selected_field });
        }
        if (queries.items.len == 0) return;
        // Highlight the original source even when result shaping projected it
        // away. Hydration keeps the same snapshot, deletes and reader lease.
        var sources: ?[]?[]u8 = null;
        defer if (sources) |items| {
            for (items) |bytes| if (bytes) |owned| a.free(owned);
            a.free(items);
        };
        if (!self.typed_delivery and (!req.include_stored or (!req.include_all_fields and !req.defer_stored_projection))) {
            const keys = try a.alloc([]const u8, result.hits.len);
            defer a.free(keys);
            for (result.hits, keys) |hit, *key| key.* = hit.id;
            sources = try loadManySelected(self, a, keys, self.hydration_fields);
        }
        try search.attachHighlightsWithIndexQueries(a, options, queries.items, result.hits, sources);
        try self.context.ensureActive();
    }
    fn hydrateTyped(self: *Execution, a: A, hits: []types.SearchHit) !void {
        const keys = try a.alloc([]const u8, hits.len);
        defer a.free(keys);
        for (hits, keys) |hit, *key| key.* = hit.id;
        const values = try loadSelected(std.json.Value, self, a, keys, self.hydration_fields);
        defer {
            for (values) |*value| if (value.*) |*owned| types.deinitJsonValue(a, owned);
            a.free(values);
        }
        for (hits, values) |*hit, *value| {
            std.debug.assert(hit.stored_data == null and hit.source_value == null);
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
        var request = self.request;
        request.cancellation = .{ .ptr = self, .is_cancelled_fn = canceled };
        const cursor = try local.sql_lake_cursor.openPinned(a, self.table, .{ .fields = &.{}, .primary_order = true, .after = options.primary_key_start_after, .before = options.primary_key_stop_before, .limit = 256 }, request, self.source);
        defer cursor.close(cursor.ptr);
        var manager: local.sql_spill.Manager = .{ .alloc = a, .io = self.context.io.?, .context = self, .checkpoint = scanCheckpoint };
        defer manager.deinit();
        var sort = local.sql_spill.Sort.init(a, &manager, &.{.{ .descending = true }}, 512 * 1024);
        defer sort.deinit();
        var ordinal: u64 = 0;
        while (true) {
            const page = try cursor.next(cursor.ptr, a, 256);
            defer page.deinit();
            for (page.rows) |row| {
                if (options.primary_key_reverse) {
                    try sort.add(.{ .values = &.{}, .keys = &.{local.sql_scalar.Datum.fromJson(.{ .string = row.id })}, .ordinal = ordinal });
                    ordinal += 1;
                } else if (try visit(target, row.id) == .stop) return;
            }
            if (page.after == null) break;
        }
        if (options.primary_key_reverse) {
            var arena = std.heap.ArenaAllocator.init(a);
            defer arena.deinit();
            while (true) {
                _ = arena.reset(.retain_capacity);
                const row = try sort.next(arena.allocator()) orelse break;
                if (try visit(target, row.keys[0].value.string) == .stop) return;
            }
        }
    }
    fn searchText(raw: ?*anyopaque, a: A, req: types.SearchRequest, text: types.TextQuery) !types.SearchResult {
        return search.searchTextQuery(a, req, text, .{ .ctx = raw, .exact_doc_id_filters = true, .acquire_text_source = acquire, .project_key = publicKey, .native_key = nativeKey, .filter_candidate_presence = true, .text_index_entry = noLocal, .text_index_is_chunk_backed = chunkBacked, .search_match_all = matchAll, .project_stored_search = project, .load_stored = loadOne, .load_projected_documents = loadProjected, .postprocess = postprocess });
    }
    fn dispatchText(raw: ?*anyopaque, a: A, req: types.SearchRequest) !types.SearchResult {
        return search.searchText(a, req, .{ .ctx = raw, .func = searchText });
    }
    fn denseIndex(raw: ?*anyopaque, name: ?[]const u8) !?*local.storage_db_catalog_index_manager.IndexManager.DenseIndex {
        const self = from(raw);
        try self.context.ensureActive();
        const native = @import("lake_index_native_dense.zig");
        var count: usize = 0;
        for (self.declarations) |declaration| if (declaration.artifact.kind == .vector_segment and declaration.artifact.metadata_version == native.metadata_version) {
            count += 1;
        };
        const selected = for (self.declarations) |declaration| {
            if (declaration.artifact.kind == .vector_segment and declaration.artifact.metadata_version == native.metadata_version and (if (name) |explicit| std.mem.eql(u8, explicit, declaration.name) else count == 1)) break declaration;
        } else return error.IndexNotFound;
        if (self.dense_entries.get(selected.name)) |entry| return entry;
        try self.runtimes.ensureUnusedCapacity(self.arena, 1);
        const runtime = try self.server.lake_native_runtimes.acquire(self.server, selected, self.domain, self.store.identity, self.context);
        errdefer runtime.release();
        const entry = runtime.dense_entry.?;
        try self.dense_entries.put(self.arena, selected.name, entry);
        self.runtimes.appendAssumeCapacity(runtime);
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
    fn searchDense(raw: ?*anyopaque, a: A, req: types.SearchRequest, dense: types.DenseKnnQuery) !types.SearchResult {
        return search.searchDense(a, req, dense, .{ .ctx = raw, .exact_doc_id_filters = true, .filter_candidate_presence = true, .text_index_entry = noLocal, .dense_index = denseIndex, .lookup_doc_key = lookupDocKey, .resolve_hit_key = densePublicKey, .lookup_vector_id = lookupVectorId, .load_projected_document = requireProjected, .load_projected_documents = loadProjected, .hbc_search = denseSearch, .hbc_search_profiled = denseSearchProfiled, .postprocess = postprocessVector });
    }
    fn sparseIndex(raw: ?*anyopaque, name: ?[]const u8) !?*local.storage_db_catalog_index_manager.IndexManager.SparseIndex {
        const self = from(raw);
        try self.context.ensureActive();
        const native = @import("lake_index_native_sparse.zig");
        var count: usize = 0;
        for (self.declarations) |declaration| if (declaration.artifact.kind == .sparse_segment and declaration.artifact.metadata_version == native.metadata_version) {
            count += 1;
        };
        const selected = for (self.declarations) |declaration| {
            if (declaration.artifact.kind == .sparse_segment and declaration.artifact.metadata_version == native.metadata_version and (if (name) |explicit| std.mem.eql(u8, explicit, declaration.name) else count == 1)) break declaration;
        } else return error.IndexNotFound;
        if (self.sparse_entries.get(selected.name)) |entry| return entry;
        try self.runtimes.ensureUnusedCapacity(self.arena, 1);
        const runtime = try self.server.lake_native_runtimes.acquire(self.server, selected, self.domain, self.store.identity, self.context);
        errdefer runtime.release();
        const entry = runtime.sparse_entry.?;
        try self.sparse_entries.put(self.arena, selected.name, entry);
        self.runtimes.appendAssumeCapacity(runtime);
        return entry;
    }
    fn requireProjected(raw: ?*anyopaque, a: A, req: types.SearchRequest, key: []const u8) ![]u8 {
        return (try loadProjectedOne(raw, a, req, key)) orelse error.StoredDocMissing;
    }
    fn postprocessVector(raw: ?*anyopaque, a: A, req: types.SearchRequest, result: types.SearchResult, _: bool) !types.SearchResult {
        return shape.postprocessVectorSearchResult(a, req, result, false, .{ .ctx = raw, .is_visible = visible, .resolve_parent_id = parent, .load_parent_stored = parentStored, .load_stored = loadOne, .load_many_stored = loadMany, .load_projected_stored = loadProjectedOne, .load_many_projected_stored = loadProjected });
    }
    fn searchSparse(raw: ?*anyopaque, a: A, req: types.SearchRequest, sparse: types.SparseKnnQuery) !types.SearchResult {
        return search.searchSparse(a, req, sparse, .{ .ctx = raw, .exact_doc_id_filters = true, .project_key = publicKey, .native_key = nativeKey, .filter_candidate_presence = true, .text_index_entry = noLocal, .sparse_index = sparseIndex, .load_projected_document = requireProjected, .load_projected_documents = loadProjected, .postprocess = postprocessVector });
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
    fn loadOne(raw: ?*anyopaque, a: A, key: []const u8) !?[]u8 {
        const result = try loadMany(raw, a, &.{key});
        defer a.free(result);
        return result[0];
    }
    fn loadMany(raw: ?*anyopaque, a: A, keys: []const []const u8) ![]?[]u8 {
        return loadManySelected(raw, a, keys, null);
    }
    fn loadManySelected(raw: ?*anyopaque, a: A, keys: []const []const u8, selected_fields: ?[]const []const u8) ![]?[]u8 {
        return loadSelected([]u8, raw, a, keys, selected_fields);
    }
    fn loadSelected(comptime T: type, raw: ?*anyopaque, a: A, keys: []const []const u8, selected_fields: ?[]const []const u8) ![]?T {
        const self = from(raw);
        try self.context.ensureActive();
        const result = try a.alloc(?T, keys.len);
        @memset(result, null);
        errdefer {
            for (result) |*value| if (value.*) |*owned| {
                if (T == std.json.Value) types.deinitJsonValue(a, owned) else a.free(owned.*);
            };
            a.free(result);
        }
        var first: usize = 0;
        while (first < keys.len) {
            const end = @min(first + 256, keys.len);
            defer first = end;
            var arena = std.heap.ArenaAllocator.init(a);
            defer arena.deinit();
            const ca = arena.allocator();
            const refs = try ca.alloc(local.storage_rowsource_types.RowRef, end - first);
            const canonical = try ca.alloc([]const u8, end - first);
            for (keys[first..end], canonical, refs) |input_key, *mapped, *ref| {
                const key = try publicKey(raw, ca, input_key);
                mapped.* = key;
                if (key.len != 96 or !std.mem.startsWith(u8, key, "lake1:") or key[70] != ':' or key[79] != ':') return error.ExternalLakeSnapshotMismatch;
                const file = self.files.get(key[6..70]) orelse return error.ExternalLakeSnapshotMismatch;
                ref.* = .{ .external = .{ .source_id = self.source.inventory.source_id, .snapshot_id = self.source.inventory.snapshot_id, .file_id = file, .row_group_ordinal = std.fmt.parseUnsigned(u32, key[71..79], 16) catch return error.ExternalLakeSnapshotMismatch, .row_ordinal = std.fmt.parseUnsigned(u64, key[80..96], 16) catch return error.ExternalLakeSnapshotMismatch } };
            }
            var by_key: std.StringHashMapUnmanaged(std.ArrayListUnmanaged(usize)) = .empty;
            for (canonical, first..) |key, position| {
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
            const cursor = try local.sql_lake_cursor.openPinned(a, self.table, .{ .fields = fields, .row_refs = refs, .limit = 256 }, request, self.source);
            defer cursor.close(cursor.ptr);
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
        try self.context.ensureActive();
        return result;
    }
    fn loadProjected(raw: ?*anyopaque, a: A, req: types.SearchRequest, keys: []const []const u8) ![]?[]u8 {
        const self = from(raw);
        const fields = self.hydration_fields;
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
        return a.dupe(u8, hit.id);
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
/// hands owned typed sources directly to highlights and the public encoder.
fn canDeliverTypedSource(req: types.SearchRequest) bool {
    for (req.order_by) |order| if (!std.mem.eql(u8, order.field, "_score") and !std.mem.eql(u8, order.field, "_id")) return false;
    return !requiresEncodedSource(req) and req.search_after.len == 0 and req.search_before.len == 0 and
        req.evaluation_limit == 0 and req.pruner == null and req.return_mode == .parent and !req.hierarchy_grouped_matches and req.hierarchy_group_level == .source and
        req.hierarchy_children == null and !req.defer_hierarchy_child_hydration and !req.hierarchy_include_source and !req.hierarchy_include_unit and
        req.hierarchy_match_include_all_fields and req.hierarchy_source_include_all_fields and req.hierarchy_unit_include_all_fields;
}

fn requiresEncodedSource(req: types.SearchRequest) bool {
    return req.hasHitEvaluation() or req.reranker != null or req.defer_hierarchy_child_hydration or
        req.hierarchy_children != null or req.hierarchy_include_source or req.hierarchy_include_unit or
        !req.hierarchy_match_include_all_fields or !req.hierarchy_source_include_all_fields or !req.hierarchy_unit_include_all_fields or
        req.doc_filter_bindings.len != 0 or req.query != .match_all or req.filter_query_json.len != 0 or
        req.exclusion_query_json.len != 0 or req.authorization_filter_query_json.len != 0;
}

/// Compile public include patterns to physical dependencies once per hydration
/// call. Exclusion-only projections still mean the complete source document.
fn projectionColumns(a: A, table: local.sql_catalog.Table, req: types.SearchRequest) !?[]const []const u8 {
    // Deferred wire projection does not require unrelated physical columns.
    // Consumers without an explicit dependency contract retain full source.
    if (requiresEncodedSource(req)) return null;
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
    var owner: Execution = undefined;
    owner.arena = arena.allocator();
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
    try std.testing.expect((try owner.planHydration(req)) == null);
}

test "external lake typed delivery defers only source-independent native retrieval" {
    var req: types.SearchRequest = .{ .full_text = .{ .match = .{ .field = "body", .text = "needle" } }, .fields = &.{"label"}, .highlight = .{ .fields = &.{"body"} } };
    try std.testing.expect(canDeliverTypedSource(req));
    req.filter_query_json = "{}";
    try std.testing.expect(!canDeliverTypedSource(req));
    req.filter_query_json = "";
    req.order_by = &.{.{ .field = "amount" }};
    try std.testing.expect(!canDeliverTypedSource(req));
    req.order_by = &.{};
    req.hierarchy_include_source = true;
    try std.testing.expect(!canDeliverTypedSource(req));
    req.hierarchy_include_source = false;
    req.evaluation_limit = 1;
    try std.testing.expect(!canDeliverTypedSource(req));
}
