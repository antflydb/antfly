// Copyright 2026 Antfly, Inc.
// SPDX-License-Identifier: Apache-2.0

//! Cross-table promotion under the embedded database's writer fence. Resolver
//! workers are driven by the connection owner, so catalog access and the local
//! transaction coordinator share the same lifetime and serialization as SQL.
const h = @import("handles.zig");
const std = h.std;
const tables = @import("tables.zig");
const types = h.db_mod.types;
const contract = h.antfly.capi_dependencies.api_distributed_txn_contract;
const callbacks = @import("../storage/db/runtime_callbacks.zig");

pub fn attach(handle: *h.Handle, db: *h.db_mod.DB) void {
    if (!h.liteOpenModeCanWrite(handle.open_mode)) return;
    db.setResolutionCandidateSource(.{ .ptr = handle, .vtable = &.{ .get = candidateGet, .scan_prefix = candidateScan, .nearest = candidateNearest, .bound_table = boundTable, .embed_mention = embedMention } });
    db.setEntitySink(.{ .ptr = handle, .vtable = &.{ .upsert = upsert, .upsert_batch = upsertBatch } });
}

fn embedMention(ptr: *anyopaque, allocator: std.mem.Allocator, table: []const u8, embedding_name: []const u8, text: []const u8, dims: u32) anyerror![]f32 {
    const handle: *h.Handle = @ptrCast(@alignCast(ptr));
    const database = try tables.get(handle, table);
    const config = try @import("db.zig").liteMergedIndexesForDatabaseJsonAlloc(handle.alloc, database);
    defer handle.alloc.free(config);
    var embedder = try h.managed_embedder.ManagedEmbedder.initFromIndexesJsonWithOptions(handle.alloc, config, .{ .antfly_provider = handle.liteAntflyProvider(), .io = database.backend_runtime.io(), .source_table = table });
    defer embedder.deinit();
    return embedder.denseInterface().embedDense(allocator, embedding_name, text, dims);
}

fn upsert(ptr: *anyopaque, a: std.mem.Allocator, table: []const u8, key: []const u8, json: []const u8) anyerror!void {
    return upsertBatch(ptr, a, &.{.{ .table = table, .key = key, .doc_json = json }});
}

const Row = struct {
    key: []const u8,
    json: ?[]const u8,
    version: u64,
    digest: ?[32]u8,
};
const Participant = struct {
    name: []const u8,
    db: *h.db_mod.DB,
    rows: std.StringArrayHashMapUnmanaged(Row) = .empty,
};

fn merge(a: std.mem.Allocator, current: ?[]const u8, incoming: []const u8, key: []const u8) ![]const u8 {
    const document = try std.json.parseFromSliceLeaky(std.json.Value, a, incoming, .{});
    if (document != .object) return error.InvalidEntityPromotionDocument;
    var ops: std.ArrayList(types.TransformOp) = .empty;
    for ([_][]const u8{ "entity_type", "canonical_name" }) |field| {
        if (document.object.get(field)) |value| {
            if (value != .string) return error.InvalidEntityPromotionDocument;
            try ops.append(a, .{ .op = .set_on_insert, .path = field, .value_json = try std.json.Stringify.valueAlloc(a, value, .{}) });
        }
    }
    if (document.object.get("aliases")) |aliases| {
        if (aliases != .array) return error.InvalidEntityPromotionDocument;
        for (aliases.array.items) |alias| {
            if (alias != .string) return error.InvalidEntityPromotionDocument;
            try ops.append(a, .{ .op = .add_to_set, .path = "aliases", .value_json = try std.json.Stringify.valueAlloc(a, alias, .{}) });
        }
    }
    if (document.object.get("merged_into")) |redirect| {
        if (redirect != .string) return error.InvalidEntityPromotionDocument;
        try ops.append(a, .{ .op = .set, .path = "merged_into", .value_json = try std.json.Stringify.valueAlloc(a, redirect, .{}) });
        if (document.object.get("merged_into_table")) |table| {
            if (table != .string) return error.InvalidEntityPromotionDocument;
            try ops.append(a, .{ .op = .set, .path = "merged_into_table", .value_json = try std.json.Stringify.valueAlloc(a, table, .{}) });
        } else try ops.append(a, .{ .op = .unset, .path = "merged_into_table" });
    }
    if (ops.items.len == 0) return error.InvalidEntityPromotionDocument;
    return (try @import("../storage/db/transform.zig").resolveDocumentTransform(a, current, .{ .key = key, .operations = ops.items, .upsert = true })) orelse return error.InvalidEntityPromotionDocument;
}

fn upsertBatch(ptr: *anyopaque, allocator: std.mem.Allocator, entries: []const callbacks.EntityUpsert) anyerror!void {
    const handle: *h.Handle = @ptrCast(@alignCast(ptr));
    if (!h.liteOpenModeCanWrite(handle.open_mode)) return error.ReadOnly;
    if (entries.len == 0) return;
    try @import("sql_commit.zig").recover(handle);
    var arena = std.heap.ArenaAllocator.init(allocator);
    defer arena.deinit();
    const a = arena.allocator();
    var participants: std.StringArrayHashMapUnmanaged(Participant) = .empty;
    // Resolve every destination before publishing anything. Missing tables
    // leave the source promotion receipt pending and can be created later.
    for (entries) |entry| {
        if (entry.storage_table) |physical| {
            if (!std.mem.eql(u8, physical, (try boundTable(ptr, entry.table)).?)) return error.EntityPromotionUnavailable;
        }
        const slot = try participants.getOrPut(a, entry.table);
        if (!slot.found_existing) slot.value_ptr.* = .{ .name = entry.table, .db = try tables.get(handle, entry.table) };
        const participant = slot.value_ptr;
        const row = try participant.rows.getOrPut(a, entry.key);
        if (!row.found_existing) {
            const old = try participant.db.lookup(a, entry.key, .{ .include_primary_digest = true });
            row.value_ptr.* = .{ .key = entry.key, .json = if (old) |value| value.json else null, .version = if (old) |value| value.version orelse 0 else 0, .digest = if (old) |value| value.expected_content_digest else null };
        }
        row.value_ptr.json = if (entry.delete) null else try merge(a, row.value_ptr.json, entry.doc_json, entry.key);
    }
    const requests = try a.alloc(contract.TableCommitRequest, participants.count());
    for (participants.values(), requests) |*participant, *request| {
        var writes: std.ArrayList(types.TransactionWrite) = .empty;
        var deletes: std.ArrayList([]const u8) = .empty;
        var predicates: std.ArrayList(types.TransactionVersionPredicate) = .empty;
        for (participant.rows.values()) |row| {
            if (row.json) |json| try writes.append(a, .{ .key = row.key, .value = json }) else try deletes.append(a, row.key);
            try predicates.append(a, .{ .key = row.key, .expected_version = row.version, .expected_content_digest = row.digest });
        }
        request.* = .{ .table_name = participant.name, .writes = writes.items, .deletes = deletes.items, .predicates = predicates.items };
    }
    var transaction: ?types.TxnId = null;
    const outcome = try @import("sql_commit.zig").commit(handle, requests, &transaction);
    if (outcome != .committed) return error.EntityPromotionUnavailable;
}

/// Promotion can enqueue enrichments in any table, including one already
/// drained earlier in the sweep. Repeat until a complete sweep creates no
/// new replay work; a recursive taxonomy that does not converge fails boundedly.
pub fn runUntilIdle(handle: *h.Handle) !void {
    const root = handle.parent_handle orelse handle;
    if (root.embedded_path == null) return handle.database().runUntilIdle();
    try tables.load(root);
    for (0..1024) |_| {
        const before = replaySequence(root);
        try root.db.runUntilIdle();
        var iterator = root.embedded_tables.valueIterator();
        while (iterator.next()) |table| try table.*.db.runUntilIdle();
        if (before == replaySequence(root)) return;
    }
    return error.RunUntilIdleDidNotConverge;
}

fn replaySequence(handle: *h.Handle) u128 {
    var sequence: u128 = handle.db.core.nextDerivedSequence();
    sequence += handle.db.core.nextEnrichmentSequence();
    var iterator = handle.embedded_tables.valueIterator();
    while (iterator.next()) |table| {
        sequence += table.*.db.core.nextDerivedSequence();
        sequence += table.*.db.core.nextEnrichmentSequence();
    }
    return sequence;
}

fn boundTable(ptr: *anyopaque, name: []const u8) anyerror!?[]const u8 {
    const handle: *h.Handle = @ptrCast(@alignCast(ptr));
    _ = try tables.get(handle, name);
    if (std.mem.eql(u8, name, "default")) return "embedded/tables/1";
    return handle.embedded_tables.get(name).?.namespace;
}

fn candidateGet(ptr: *anyopaque, a: std.mem.Allocator, name: []const u8, key: []const u8) anyerror!?[]u8 {
    const handle: *h.Handle = @ptrCast(@alignCast(ptr));
    const db = try tables.get(handle, name);
    const row = (try db.lookup(a, key, .{})) orelse return null;
    return row.json;
}

fn candidateScan(ptr: *anyopaque, a: std.mem.Allocator, name: []const u8, prefix: []const u8, options: callbacks.CandidateSource.ScanOptions, ctx: *anyopaque, consume: callbacks.CandidateSource.Consume) anyerror!void {
    const handle: *h.Handle = @ptrCast(@alignCast(ptr));
    const db = try tables.get(handle, name);
    const upper = try a.dupe(u8, prefix);
    defer a.free(upper);
    var end = upper.len;
    while (end > 0 and upper[end - 1] == 255) end -= 1;
    if (end > 0) upper[end - 1] += 1;
    var rows = try db.scan(a, prefix, if (end == 0) "" else upper[0..end], .{ .include_documents = true, .limit = @intCast(@min(options.limit, std.math.maxInt(u32))) });
    defer rows.deinit(a);
    for (rows.documents) |row| {
        if (!std.mem.startsWith(u8, row.id, prefix)) continue;
        try consume(ctx, row.id, row.json);
    }
}

fn candidateNearest(ptr: *anyopaque, a: std.mem.Allocator, name: []const u8, query: callbacks.CandidateSource.NearestQuery, ctx: *anyopaque, consume: callbacks.CandidateSource.Consume) anyerror!void {
    const handle: *h.Handle = @ptrCast(@alignCast(ptr));
    const db = try tables.get(handle, name);
    var result = try db.search(a, .{ .index_name = query.index_name, .query = .{ .dense_knn = .{ .vector = query.embedding, .k = @intCast(query.k) } }, .limit = @intCast(query.k), .include_stored = true, .include_all_fields = true });
    defer result.deinit();
    const dense = db.core.index_manager.denseIndex(query.index_name) orelse return error.IndexNotFound;
    var txn = try dense.index.beginReadTxn();
    defer txn.abort();
    const scratch = try a.alloc(f32, query.embedding.len);
    defer a.free(scratch);
    for (result.hits) |hit| if (hit.stored_data) |json| {
        // Generated vectors live in the index, not in the primary JSON.
        // The resolver's standard name_embedding comparison must receive the
        // same canonical vector that ANN used, including after a reopen.
        const vector_id = try db.core.index_manager.lookupDenseVectorId(db.core.store, query.index_name, hit.id) orelse return error.MissingCandidateEmbedding;
        const vector = try dense.index.getVectorInto(&txn, vector_id, scratch);
        var parsed = try std.json.parseFromSlice(std.json.Value, a, json, .{});
        defer parsed.deinit();
        if (parsed.value != .object) return error.InvalidEntityPromotionDocument;
        const arena = parsed.arena.allocator();
        var values: std.array_list.Managed(std.json.Value) = .init(arena);
        for (vector) |value| try values.append(.{ .float = value });
        try parsed.value.object.put(arena, "name_embedding", .{ .array = values });
        const candidate_json = try std.json.Stringify.valueAlloc(a, parsed.value, .{});
        defer a.free(candidate_json);
        try consume(ctx, hit.id, candidate_json);
    };
}

test "capi Lite promotion atomically merges across tables and survives reopen" {
    const api = @import("db.zig");
    const a = std.testing.allocator;
    var tmp = try api.TestDirectory.init("capi");
    defer tmp.cleanup();
    const path = try std.fmt.allocPrint(a, "{s}.aflite", .{tmp.path()});
    defer a.free(path);
    var handle = try api.openLiteHandleAllocWithRuntime(std.heap.c_allocator, path, .{ .profile = .hosted, .externally_locked = true }, true, null, null);
    defer h.closeHandle(handle);
    try tables.create(handle, "entities", "{}", false);
    try tables.create(handle, "events", "{}", false);
    const sink = handle.db.entity_sink.?;
    const entries = [_]callbacks.EntityUpsert{
        .{ .table = "entities", .key = "person/ada", .doc_json = "{\"entity_type\":\"person\",\"canonical_name\":\"Ada\",\"aliases\":[\"Ada\"]}" },
        .{ .table = "events", .key = "event/write", .doc_json = "{\"entity_type\":\"event\",\"canonical_name\":\"Write\",\"aliases\":[\"Write\"]}" },
        .{ .table = "entities", .key = "person/ada", .doc_json = "{\"entity_type\":\"person\",\"canonical_name\":\"Ada Lovelace\",\"aliases\":[\"Ada Lovelace\"]}" },
    };
    try sink.upsertBatch(a, &entries);
    try sink.upsertBatch(a, &entries);
    try runUntilIdle(handle);
    const entities = try tables.get(handle, "entities");
    var row = (try entities.lookup(a, "person/ada", .{})).?;
    defer row.deinit(a);
    const parsed = try std.json.parseFromSlice(std.json.Value, a, row.json, .{});
    defer parsed.deinit();
    try std.testing.expectEqualStrings("Ada", parsed.value.object.get("canonical_name").?.string);
    try std.testing.expectEqual(@as(usize, 2), parsed.value.object.get("aliases").?.array.items.len);
    var event = (try (try tables.get(handle, "events")).lookup(a, "event/write", .{})).?;
    event.deinit(a);
    const unavailable = [_]callbacks.EntityUpsert{
        .{ .table = "entities", .key = "person/missing", .doc_json = entries[0].doc_json },
        .{ .table = "absent", .key = "event/missing", .doc_json = entries[1].doc_json },
    };
    try std.testing.expectError(error.UndefinedTable, sink.upsertBatch(a, &unavailable));
    try std.testing.expect((try entities.lookup(a, "person/missing", .{})) == null);
    h.closeHandle(handle);
    handle = try api.openLiteHandleAllocWithRuntime(std.heap.c_allocator, path, .{ .profile = .hosted, .externally_locked = true }, false, null, null);
    var reopened = (try (try tables.get(handle, "entities")).lookup(a, "person/ada", .{})).?;
    defer reopened.deinit(a);
    try std.testing.expectEqualStrings(row.json, reopened.json);
}

test "capi Lite promotion label routes extraction graphs into entity and event documents" {
    const api = @import("db.zig");
    const a = std.testing.allocator;
    var tmp = try api.TestDirectory.init("promotion-graph");
    defer tmp.cleanup();
    const path = try std.fmt.allocPrint(a, "{s}.aflite", .{tmp.path()});
    defer a.free(path);
    const handle = try api.openLiteHandleAllocWithRuntime(std.heap.c_allocator, path, .{ .profile = .native, .externally_locked = true }, true, null, null);
    try tables.create(handle, "entities", "{}", false);
    try tables.create(handle, "events", "{}", false);
    const handle_id = try api.publishHandle(handle);
    defer api.antfly_db_close(handle_id);
    const config =
        \\{"name":"knowledge","type":"graph","source":{"artifact":"kg_v1","format":"extraction_graph","mention_edge_type":"mentions"},"artifact":{"name":"kg_v1","kind":"asset","source":{"type":"field","value":"kg"},"content_type":"application/json"},"metrics":{"ppr":{"kind":"pagerank","enabled":true,"damping":0.85}},"edge_types":[{"name":"mentions"},{"name":"works_at"},{"name":"participates_in"},{"name":"uses"}],"resolvers":[{"name":"events","table":"events","source_artifact":"kg_v1","resolution_artifact":"events_resolution_v1","key_template":"event/{{ slug _entity.text }}","labels":["event"],"config_generation":1},{"name":"entities","table":"entities","source_artifact":"kg_v1","resolution_artifact":"entities_resolution_v1","key_template":"entity/{{ slug _entity.text }}","config_generation":1}]}
    ;
    try std.testing.expectEqual(h.capi.ErrorCode.ok, api.antfly_db_add_index_json(handle_id, .fromSlice(config)));
    try handle.db.reconfigureEnrichmentRuntime(.{ .enable_without_producers = true });
    try handle.db.batch(.{ .writes = &.{.{ .key = "doc:a", .value =
        \\{"kg":{"entities":[{"id":"e0","label":"person","text":"Ada Lovelace"},{"id":"e1","label":"org","text":"Antfly"},{"id":"v0","label":"event","text":"Write program","predicate":"write"}],"relations":[{"type":"works_at","source":"e0","target":"e1"},{"type":"participates_in","source":"e0","target":"v0"},{"type":"uses","source":{"text":"Ada Lovelace"},"target":{"text":"Antfly"}},{"type":"uses","source":{"text":"missing"},"target":{"text":"Antfly"}}]}}
    }}, .sync_level = .write });
    try runUntilIdle(handle);
    var entity = (try (try tables.get(handle, "entities")).lookup(a, "entity/ada_lovelace", .{})) orelse return error.TestExpectedEqual;
    defer entity.deinit(a);
    try std.testing.expect(std.mem.indexOf(u8, entity.json, "Ada Lovelace") != null);
    var event = (try (try tables.get(handle, "events")).lookup(a, "event/write_program", .{})) orelse return error.TestExpectedEqual;
    defer event.deinit(a);
    try std.testing.expect(std.mem.indexOf(u8, event.json, "Write program") != null);
    try std.testing.expect((try (try tables.get(handle, "entities")).lookup(a, "entity/write_program", .{})) == null);
    try std.testing.expect(!handle.db.pendingWorkStats().promotion.catch_up_required);
    const graph = &@constCast(handle.db.core.graphIndex("knowledge").?).index;
    const edges = try graph.getEdges(a, "entity/ada_lovelace", "uses", .out);
    defer @import("../graph/graph.zig").GraphIndex.freeEdges(a, edges);
    try std.testing.expectEqual(@as(usize, 1), edges.len);
    try std.testing.expectEqualStrings("entity/antfly", edges[0].target);
    const document_edges = try graph.getEdges(a, "doc:a", "uses", .out);
    defer @import("../graph/graph.zig").GraphIndex.freeEdges(a, document_edges);
    try std.testing.expectEqual(@as(usize, 0), document_edges.len);
    // Same numeric error code must never reuse another operation's detail.
    var child: ?*anyopaque = null;
    try std.testing.expectEqual(h.capi.ErrorCode.ok, tables.antfly_db_open_table(handle_id, .fromSlice("entities"), &child));
    defer api.antfly_db_close(child);
    try std.testing.expectEqual(h.capi.ErrorCode.invalid_argument, api.antfly_db_add_index_json(handle_id, .fromSlice("{\"name\":\"invalid\"}")));
    try std.testing.expectEqualStrings("MissingIndexType", std.mem.span(api.antfly_last_error_detail()));
    var session: u64 = 0;
    const sessions = @import("sql_session.zig");
    try std.testing.expectEqual(h.capi.ErrorCode.invalid_argument, sessions.antfly_db_sql_session_open(child, &session));
    try std.testing.expectEqual(h.capi.ErrorCode.invalid_argument, api.antfly_last_error_code());
    const detail = std.mem.span(api.antfly_last_error_detail());
    try std.testing.expect(std.mem.indexOf(u8, detail, "antfly_db_sql_session_open") != null);
    try std.testing.expect(std.mem.indexOf(u8, detail, "MissingIndexType") == null);
    try std.testing.expectEqual(h.capi.ErrorCode.ok, sessions.antfly_db_sql_session_open(handle_id, &session));
    try std.testing.expectEqual(h.capi.ErrorCode.ok, api.antfly_last_error_code());
    try std.testing.expectEqualStrings("", std.mem.span(api.antfly_last_error_detail()));
    try std.testing.expectEqual(h.capi.ErrorCode.ok, sessions.antfly_db_sql_session_close(handle_id, session));
    var named_response: h.capi.Buffer = .{};
    defer api.antfly_buffer_free(&named_response);
    try std.testing.expectEqual(h.capi.ErrorCode.ok, api.antfly_db_search_json(child, .fromSlice("{\"full_text_search\":{\"match_all\":{}},\"limit\":10}"), &named_response));
    const named_json = try std.json.parseFromSlice(std.json.Value, a, named_response.ptr.?[0..named_response.len], .{});
    defer named_json.deinit();
    try std.testing.expect(std.mem.indexOf(u8, named_response.ptr.?[0..named_response.len], "\"table\":\"entities\"") != null);
}

test "capi Lite promotion graph metrics flow through shared concepts in both directions" {
    const api = @import("db.zig");
    const a = std.testing.allocator;
    var tmp = try api.TestDirectory.init("lite-undirected-ppr");
    defer tmp.cleanup();
    const path = try std.fmt.allocPrint(a, "{s}.aflite", .{tmp.path()});
    defer a.free(path);
    const handle = try api.openLiteHandleAllocWithRuntime(std.heap.c_allocator, path, .{ .profile = .hosted, .externally_locked = true }, true, null, null);
    const handle_id = try api.publishHandle(handle);
    defer api.antfly_db_close(handle_id);
    try handle.db.addIndex(.{ .name = "knowledge", .kind = .graph, .config_json =
        \\{"metrics":{"ppr":{"kind":"pagerank","enabled":true}},"edge_types":[{"name":"about","field":"about"}]}
    });
    try handle.db.addIndex(.{ .name = "claim_text", .kind = .full_text, .config_json = "{\"field\":\"search_text\"}" });
    try handle.db.batch(.{ .writes = &.{
        .{ .key = "claim:a", .value = "{\"about\":[\"concept/shared\"],\"search_text\":\"embedder\"}" },
        .{ .key = "claim:b", .value = "{\"about\":[\"concept/shared\"]}" },
        .{ .key = "claim:unrelated", .value = "{\"about\":[\"concept/other\"]}" },
    }, .sync_level = .write });
    try runUntilIdle(handle);
    const graph = &@constCast(handle.db.core.graphIndex("knowledge").?).index;
    var directed: [3]?f64 = undefined;
    var undirected: [3]?f64 = undefined;
    const keys = &.{ "concept/shared", "claim:a", "claim:b" };
    _ = try graph.personalizedPageRankColumnIntoWithControls(a, "ppr", &.{"concept/shared"}, null, keys, &directed, .{});
    _ = try graph.personalizedPageRankColumnIntoWithControls(a, "ppr", &.{"concept/shared"}, null, keys, &undirected, .{ .direction = .both });
    try std.testing.expectEqual(@as(f64, 0), directed[1].?);
    try std.testing.expect(undirected[1].? > 0);
    try std.testing.expectApproxEqAbs(undirected[1].?, undirected[2].?, 1e-10);
    var response: h.capi.Buffer = .{};
    defer api.antfly_buffer_free(&response);
    const body =
        \\{"graph_metric":{"index":"knowledge","metric":"ppr","metric_freshness":"fresh","seed_nodes":["concept/shared"],"direction":"both","top_k":3}}
    ;
    try std.testing.expect(api.requestLooksLikePublicQueryJson(body));
    try std.testing.expectEqual(h.capi.ErrorCode.ok, api.antfly_db_search_json(handle_id, .fromSlice(body), &response));
    try std.testing.expect(std.mem.indexOf(u8, response.ptr.?[0..response.len], "claim:a") != null);
    api.antfly_buffer_free(&response);
    const traversal =
        \\{"full_text_search":{"match":{"field":"search_text","text":"embedder"}},"limit":5,"graph_queries":{"q":{"index":"knowledge","traverse":{"start":{"result_ref":"$query_results","limit":5},"direction":"both","edge_types":["about"],"max_depth":2,"include_documents":true,"include_paths":true,"deduplicate_nodes":false,"limit":20}}}}
    ;
    try std.testing.expectEqual(h.capi.ErrorCode.ok, api.antfly_db_search_json(handle_id, .fromSlice(traversal), &response));
    try std.testing.expect(std.mem.indexOf(u8, response.ptr.?[0..response.len], "claim:b") != null);
    try std.testing.expect(std.mem.indexOf(u8, response.ptr.?[0..response.len], "path") != null);
    try std.testing.expectEqual(h.capi.ErrorCode.ok, api.antfly_db_add_index_json(handle_id, .fromSlice("{\"name\":\"public_graph\",\"type\":\"graph\",\"edge_types\":[{\"name\":\"supersedes\",\"field\":\"supersedes\"}]}")));
    try std.testing.expectEqual(h.capi.ErrorCode.invalid_argument, api.antfly_db_add_index_json(handle_id, .fromSlice("{\"name\":\"invalid\"}")));
    try std.testing.expectEqualStrings("MissingIndexType", std.mem.span(api.antfly_last_error_detail()));
    try std.testing.expectEqual(h.capi.ErrorCode.not_found, api.antfly_db_lookup_json(handle_id, .fromSlice("missing-document"), &response));
    try std.testing.expectEqualStrings("NotFound", std.mem.span(api.antfly_last_error_detail()));
    try std.testing.expectEqual(h.capi.ErrorCode.not_found, api.antfly_last_error_code());
    try std.testing.expectEqual(h.capi.ErrorCode.ok, api.antfly_db_add_index_json(handle_id, .fromSlice("{\"name\":\"public_sparse\",\"type\":\"embeddings\",\"sparse\":true,\"external\":true,\"field\":\"sparse\"}")));
    try std.testing.expect(handle.database().core.index_manager.sparseIndex("public_sparse") != null);
    try std.testing.expect(handle.database().core.index_manager.denseIndex("public_sparse") == null);
    try std.testing.expectEqual(h.capi.ErrorCode.ok, api.antfly_db_add_index_json(handle_id, .fromSlice("{\"name\":\"success\",\"type\":\"graph\"}")));
    try std.testing.expectEqualStrings("", std.mem.span(api.antfly_last_error_detail()));
    try std.testing.expectEqual(h.capi.ErrorCode.invalid_argument, api.antfly_db_search_json(null, .fromSlice(body), &response));
    try std.testing.expect(std.mem.indexOf(u8, std.mem.span(api.antfly_last_error_detail()), "antfly_db_search_json") != null);
}

// The generator is deterministic so this exercises the full recursive replay
// topology without making the test depend on an external inference provider.
test "capi Lite promotion recursively generates a taxonomy in the destination table" {
    const api = @import("db.zig");
    const producer = @import("../storage/db/enrichment/asset_producer.zig");
    const a = std.testing.allocator;
    const Generator = struct {
        calls: usize = 0,
        saw_neighbors: bool = false,
        fn memory(_: *anyopaque, _: std.mem.Allocator, _: []const producer.Request) !@import("antfly_inference_work").InvocationMemoryPlan {
            return .{ .attachment_transport = .data_uri, .fixed_bytes = 16 << 20, .allocator_limit_bytes = 16 << 20, .max_result_bytes_per_item = 8 << 20, .max_result_bytes = 8 << 20 };
        }
        fn produce(ptr: *anyopaque, alloc: std.mem.Allocator, request: producer.Request) ![]u8 {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            self.calls += 1;
            self.saw_neighbors = self.saw_neighbors or std.mem.indexOf(u8, request.source_text, "neighbors") != null;
            const source = request.source_text[0 .. std.mem.indexOfScalar(u8, request.source_text, '\n') orelse request.source_text.len];
            if (std.mem.eql(u8, source, "fail")) return error.UnsupportedGlinerBoundaryRuntime;
            const parent = if (std.mem.eql(u8, source, "Compiler")) "Software" else if (std.mem.eql(u8, source, "Software")) "Computing" else return alloc.dupe(u8, "{\"entities\":[],\"relations\":[]}");
            return std.fmt.allocPrint(alloc, "{{\"entities\":[{{\"id\":\"self\",\"label\":\"concept\",\"text\":{f}}},{{\"id\":\"parent\",\"label\":\"concept\",\"text\":{f}}}],\"relations\":[{{\"type\":\"is_a\",\"source\":\"self\",\"target\":\"parent\"}}]}}", .{ std.json.fmt(source, .{}), std.json.fmt(parent, .{}) });
        }
    };
    var generator: Generator = .{};
    var tmp = try api.TestDirectory.init("recursive-taxonomy");
    defer tmp.cleanup();
    const path = try std.fmt.allocPrint(a, "{s}.aflite", .{tmp.path()});
    defer a.free(path);
    const handle = try api.openLiteHandleAllocWithRuntime(std.heap.c_allocator, path, .{ .profile = .hosted, .externally_locked = true }, true, null, null);
    defer h.closeHandle(handle);
    try tables.create(handle, "entities", "{}", false);
    const entities = try tables.get(handle, "entities");
    try entities.addIndex(.{ .name = "entity_context", .kind = .graph, .config_json = "{\"edge_types\":[{\"name\":\"context\",\"field\":\"context\"}]}" });
    try entities.addEnrichment(.{ .name = "taxonomy", .kind = .asset, .field = "canonical_name", .neighbor_context = .{ .graph_index = "entity_context", .edge_types = &.{"context"}, .direction = .out }, .content_type = "application/json", .producer_json = "{\"type\":\"generator\",\"config\":{\"provider\":\"mock\"}}" });
    try entities.addIndex(.{ .name = "taxonomy_graph", .kind = .graph, .config_json =
        \\{"sources":[{"artifact":"taxonomy","format":"extraction_graph","mention_edge_type":"mentions"}],"edge_types":[{"name":"mentions"},{"name":"is_a"},{"name":"supersedes","field":"supersedes"}]}
    });
    try entities.addResolver(.{ .name = "parents", .table = "entities", .source_artifact = "taxonomy", .resolution_artifact = "parents_resolved", .key_template = "concept/{{ slug _entity.text }}", .config_generation = 1 });
    try entities.reconfigureEnrichmentRuntime(.{ .asset_producer = .{ .ptr = &generator, .vtable = &.{ .produce = Generator.produce, .invocation_memory_for_requests = Generator.memory } } });
    try entities.batch(.{ .graph_writes = &.{.{ .index_name = "entity_context", .source = "concept/compiler", .target = "concept/programming", .edge_type = "context" }} });
    try handle.db.entity_sink.?.upsert(a, "entities", "concept/compiler", "{\"entity_type\":\"concept\",\"canonical_name\":\"Compiler\",\"aliases\":[\"Compiler\"]}");
    try runUntilIdle(handle);
    for ([_][]const u8{ "concept/compiler", "concept/software", "concept/computing" }) |key| {
        var row = (try entities.lookup(a, key, .{})) orelse return error.TestExpectedEqual;
        row.deinit(a);
    }
    try std.testing.expect(generator.calls >= 3);
    try std.testing.expect(generator.saw_neighbors);
    try std.testing.expect(!entities.pendingWorkStats().promotion.catch_up_required);
    const before = generator.calls;
    try runUntilIdle(handle);
    try std.testing.expectEqual(before, generator.calls);
    const initial_graph = &@constCast(entities.core.graphIndex("taxonomy_graph").?).index;
    const initial_taxonomy = try initial_graph.getEdges(a, "concept/compiler", "is_a", .out);
    defer @import("../graph/graph.zig").GraphIndex.freeEdges(a, initial_taxonomy);
    try std.testing.expect(initial_taxonomy.len > 0);
    // Field and artifact contributions share an index across updates/deletes.
    try entities.batch(.{ .writes = &.{.{ .key = "concept/compiler", .value = "{\"canonical_name\":\"Compiler\",\"supersedes\":[\"concept/old\"]}" }} });
    try runUntilIdle(handle);
    const graph = &@constCast(entities.core.graphIndex("taxonomy_graph").?).index;
    const field_edges = try graph.getEdges(a, "concept/compiler", "supersedes", .out);
    defer @import("../graph/graph.zig").GraphIndex.freeEdges(a, field_edges);
    try std.testing.expectEqual(@as(usize, 1), field_edges.len);
    try entities.batch(.{ .writes = &.{.{ .key = "concept/compiler", .value = "{\"canonical_name\":\"Compiler\",\"supersedes\":[]}" }} });
    try runUntilIdle(handle);
    const cleared = try graph.getEdges(a, "concept/compiler", "supersedes", .out);
    defer @import("../graph/graph.zig").GraphIndex.freeEdges(a, cleared);
    try std.testing.expectEqual(@as(usize, 0), cleared.len);
    const taxonomy = try graph.getEdges(a, "concept/compiler", "is_a", .out);
    defer @import("../graph/graph.zig").GraphIndex.freeEdges(a, taxonomy);
    try std.testing.expect(taxonomy.len > 0);

    try entities.batch(.{ .writes = &.{.{ .key = "concept/failure", .value = "{\"canonical_name\":\"fail\"}" }} });
    runUntilIdle(handle) catch {};
    var stats = try api.pendingWorkStatsJson(handle);
    defer api.antfly_buffer_free(&stats);
    const parsed = try std.json.parseFromSlice(std.json.Value, a, stats.ptr.?[0..stats.len], .{});
    defer parsed.deinit();
    try std.testing.expect(parsed.value.object.get("tables").?.object.contains("entities"));
    const enrichment = parsed.value.object.get("enrichment").?.object;
    try std.testing.expect(enrichment.get("enabled").?.bool);
    try std.testing.expect(enrichment.get("error_count").?.integer > 0);
    try std.testing.expectEqualStrings("UnsupportedGlinerBoundaryRuntime", enrichment.get("last_error_code").?.string);
}

test {
    _ = @import("../storage/db/graph_endpoint_resolutions.zig");
}

test "capi Lite promotion resolves ANN and explicit aliases through stored entity tables" {
    const api = @import("db.zig");
    const a = std.testing.allocator;
    var tmp = try api.TestDirectory.init("lite-ann-resolution");
    defer tmp.cleanup();
    const path = try std.fmt.allocPrint(a, "{s}.aflite", .{tmp.path()});
    defer a.free(path);
    const handle = try api.openLiteHandleAllocWithRuntime(std.heap.c_allocator, path, .{ .profile = .native, .externally_locked = true }, true, null, null);
    const id = try api.publishHandle(handle);
    defer api.antfly_db_close(id);
    try std.testing.expectEqual(h.capi.ErrorCode.ok, tables.antfly_db_create_table_json(id, .fromSlice("entities"), .fromSlice("{}")));
    const entities = try tables.get(handle, "entities");
    try entities.addIndex(.{ .name = "names", .kind = .dense_vector, .config_json = "{\"field\":\"canonical_vector\",\"dims\":3,\"external\":true}" });
    try entities.batch(.{ .writes = &.{.{ .key = "concept/canonical", .value = "{\"entity_type\":\"concept\",\"canonical_name\":\"Approval reviewer\",\"canonical_vector\":[1,0,0],\"_embeddings\":{\"names\":[1,0,0]}}" }}, .sync_level = .full_index });
    const config =
        \\{"name":"knowledge","type":"graph","source":{"artifact":"kg","format":"extraction_graph","mention_edge_type":"mentions"},"artifact":{"name":"kg","kind":"asset","source":{"type":"field","value":"kg"},"content_type":"application/json"},"edge_types":[{"name":"mentions"}],"resolvers":[{"name":"concepts","table":"entities","source_artifact":"kg","resolution_artifact":"resolved","key_template":"concept/{{ slug _entity.text }}","candidate_search":"ann","candidate_ann_index":"names","candidate_limit":1,"scorer_json":"{\"comparisons\":[{\"name\":\"embedding\",\"left\":\"name_embedding\",\"right\":\"name_embedding\",\"levels\":[{\"when\":\"cosine > 0.9\",\"weight\":8.0},{\"else\":true,\"weight\":-6.0}]}],\"combine\":{\"bias\":-3.0},\"decision\":{\"match\":0.9}}","config_generation":1}]}
    ;
    try std.testing.expectEqual(h.capi.ErrorCode.ok, api.antfly_db_add_index_json(id, .fromSlice(config)));
    try handle.db.reconfigureEnrichmentRuntime(.{ .enable_without_producers = true });
    try handle.db.batch(.{ .writes = &.{.{ .key = "claim:ann", .value = "{\"kg\":{\"entities\":[{\"id\":\"e0\",\"label\":\"concept\",\"text\":\"Laya reviewer\",\"embedding\":[1,0,0]}]}}" }} });
    try runUntilIdle(handle);
    const graph = &@constCast(handle.db.core.graphIndex("knowledge").?).index;
    const edges = try graph.getEdges(a, "claim:ann", "mentions", .out);
    defer @import("../graph/graph.zig").GraphIndex.freeEdges(a, edges);
    try std.testing.expectEqual(@as(usize, 1), edges.len);
    try std.testing.expectEqualStrings("concept/canonical", edges[0].target);
    try std.testing.expect((try entities.lookup(a, "concept/laya_reviewer", .{})) == null);
    // No vector on the alias row: ANN cannot return it as a candidate.
    try handle.db.entity_sink.?.upsert(a, "entities", "concept/the_decider", "{\"entity_type\":\"concept\",\"canonical_name\":\"The decider\",\"merged_into\":\"concept/canonical\"}");
    try handle.db.batch(.{ .writes = &.{.{ .key = "claim:alias", .value = "{\"kg\":{\"entities\":[{\"id\":\"e0\",\"label\":\"concept\",\"text\":\"The decider\",\"embedding\":[0,1,0]}]}}" }} });
    try runUntilIdle(handle);
    const alias_edges = try graph.getEdges(a, "claim:alias", "mentions", .out);
    defer @import("../graph/graph.zig").GraphIndex.freeEdges(a, alias_edges);
    try std.testing.expectEqual(@as(usize, 1), alias_edges.len);
    try std.testing.expectEqualStrings("concept/canonical", alias_edges[0].target);
}

test "capi Lite models_dir loads a real extractor for graph enrichment" {
    const api = @import("db.zig");
    if (!api.localInferenceRuntimeAvailable()) return error.SkipZigTest;
    const model_dir = std.mem.span(std.c.getenv("ANTFLY_LITE_EXTRACTOR_MODEL_DIR") orelse return error.SkipZigTest);
    const a = std.testing.allocator;
    var tmp = try api.TestDirectory.init("explicit-models-dir");
    defer tmp.cleanup();
    const models_dir = try std.fmt.allocPrint(a, "{s}/models", .{tmp.path()});
    defer a.free(models_dir);
    try std.Io.Dir.cwd().createDirPath(std.testing.io, models_dir);
    const link = try std.fmt.allocPrint(a, "{s}/explicit-base", .{models_dir});
    defer a.free(link);
    const encoder_dir = try std.fmt.allocPrint(a, "{s}/encoder_config", .{link});
    defer a.free(encoder_dir);
    try std.Io.Dir.cwd().createDirPath(std.testing.io, encoder_dir);
    for ([_][]const u8{ "config.json", "model.safetensors", "model_manifest.json", "tokenizer.json", "tokenizer_config.json", "encoder_config/config.json" }) |file| {
        const original = try std.fmt.allocPrint(a, "{s}/{s}", .{ model_dir, file });
        defer a.free(original);
        const destination = try std.fmt.allocPrint(a, "{s}/{s}", .{ link, file });
        defer a.free(destination);
        try std.Io.Dir.cwd().hardLink(original, .cwd(), destination, std.testing.io, .{});
    }
    const path = try std.fmt.allocPrint(a, "{s}/graph.aflite", .{tmp.path()});
    defer a.free(path);
    const handle = try api.openLiteHandleAllocWithRuntime(std.heap.c_allocator, path, .{
        .profile = .native,
        .externally_locked = true,
        .inference = .{ .models_dir = models_dir, .local_runtime_configured = true, .host_budget_mb = 16384, .backend_budget_mb = 16384, .combined_budget_mb = 24576, .scratch_budget_mb = 16384, .process_memory_budget_mb = 32768 },
    }, true, null, null);
    defer h.closeHandle(handle);
    try tables.create(handle, "entities", "{}", false);
    try handle.db.addResolver(.{ .name = "concepts", .table = "entities", .source_artifact = "kg", .resolution_artifact = "resolved", .key_template = "concept/{{ uslug _entity.text }}", .config_generation = 1 });
    try handle.db.addEnrichment(.{ .name = "kg", .kind = .asset, .field = "body", .content_type = "application/json", .producer_json = "{\"type\":\"extractor\",\"config\":{\"provider\":\"antfly\",\"model\":\"explicit-base\",\"schema\":{\"entities\":[\"company\",\"person\",\"product\",\"location\"]}}}" });
    try handle.db.addIndex(.{ .name = "knowledge", .kind = .graph, .config_json = "{\"sources\":[{\"artifact\":\"kg\",\"format\":\"extraction_graph\",\"mention_edge_type\":\"mentions\"}],\"edge_types\":[{\"name\":\"mentions\"}],\"metrics\":{\"ppr\":{\"kind\":\"pagerank\",\"enabled\":true}}}" });
    try api.refreshLiteManagedEmbeddingRuntime(handle);
    try handle.db.batch(.{ .writes = &.{.{ .key = "claim:example", .value = "{\"body\":\"Apple CEO Tim Cook announced iPhone 15 in Cupertino yesterday.\"}" }}, .sync_level = .write });
    try runUntilIdle(handle);
    const graph = &@constCast(handle.db.core.graphIndex("knowledge").?).index;
    const neighbors = try graph.getEdges(a, "claim:example", "mentions", .out);
    defer @import("../graph/graph.zig").GraphIndex.freeEdges(a, neighbors);
    try std.testing.expect(neighbors.len > 0);
    var entity = (try (try tables.get(handle, "entities")).lookup(a, neighbors[0].target, .{})) orelse return error.TestUnexpectedResult;
    defer entity.deinit(a);
    var scores: [2]?f64 = undefined;
    _ = try graph.personalizedPageRankColumnIntoWithControls(a, "ppr", &.{neighbors[0].target}, null, &.{ "claim:example", neighbors[0].target }, &scores, .{ .direction = .both });
    try std.testing.expect(scores[0].? > 0);
}
