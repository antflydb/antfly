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
    db.setResolutionCandidateSource(candidateSource(handle));
    db.setEntitySink(.{ .ptr = handle, .vtable = &.{ .upsert = upsert, .upsert_batch = upsertBatch } });
}

/// Read-only candidate seam also used by ephemeral query resolution.
pub fn candidateSource(handle: *h.Handle) callbacks.CandidateSource {
    return .{ .ptr = handle.parent_handle orelse handle, .vtable = &.{ .get = candidateGet, .scan_prefix = candidateScan, .nearest = candidateNearest, .bound_table = boundTable, .embed_mention = embedMention } };
}

fn embedMention(ptr: *anyopaque, allocator: std.mem.Allocator, table: []const u8, embedding_name: []const u8, text: []const u8, dims: u32) anyerror![]f32 {
    const handle: *h.Handle = @ptrCast(@alignCast(ptr));
    const database = try tables.get(handle, table);
    // The destination owns the canonical embedding space. Reuse its configured
    // provider for mention generation too, including injected runtime providers.
    if (database.resolution_embedder) |embedder|
        return embedder.embedDense(allocator, embedding_name, text, dims);
    if (database.enrichment_runtime) |runtime| if (runtime.config.dense_embedder) |embedder|
        return embedder.embedDense(allocator, embedding_name, text, dims);
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
    if (query.vector_fields.len == 0) {
        for (result.hits) |hit| if (hit.stored_data) |json| try consume(ctx, hit.id, json);
        return;
    }
    const dense = db.core.index_manager.denseIndex(query.index_name) orelse return error.IndexNotFound;
    var txn = try dense.index.beginReadTxn();
    defer txn.abort();
    const scratch = try a.alloc(f32, query.embedding.len);
    defer a.free(scratch);
    for (result.hits) |hit| if (hit.stored_data) |json| {
        // Generated vectors live in the index, not in the primary JSON.
        // Scorer-selected candidate fields receive the canonical vector that
        // ANN used, including after a reopen.
        const vector_id = try db.core.index_manager.lookupDenseVectorId(db.core.store, query.index_name, hit.id) orelse {
            // A candidate may disappear between ANN and hydration. Keep the
            // remaining candidates and let missing-value scoring handle it.
            try consume(ctx, hit.id, json);
            continue;
        };
        const vector = dense.index.getVectorInto(&txn, vector_id, scratch) catch |err| switch (err) {
            error.NotFound, error.VectorNotFound => {
                try consume(ctx, hit.id, json);
                continue;
            },
            else => return err,
        };
        const hydrated = try hydrateCandidate(a, json, query.vector_fields, vector);
        defer if (hydrated) |bytes| a.free(bytes);
        try consume(ctx, hit.id, hydrated orelse json);
    };
}

// Null means use the original bytes. Missing vectors remain missing values
// for the scorer; text-only scorers incur no parsing/serialization cost.
fn hydrateCandidate(a: std.mem.Allocator, json: []const u8, fields: []const []const u8, vector: ?[]const f32) !?[]u8 {
    if (fields.len == 0 or vector == null) return null;
    var parsed = try std.json.parseFromSlice(std.json.Value, a, json, .{});
    defer parsed.deinit();
    if (parsed.value != .object) return error.InvalidEntityPromotionDocument;
    const arena = parsed.arena.allocator();
    var values: std.array_list.Managed(std.json.Value) = .init(arena);
    for (vector.?) |value| try values.append(.{ .float = value });
    var changed = false;
    for (fields) |field| {
        if (parsed.value.object.contains(field)) continue;
        try parsed.value.object.put(arena, field, .{ .array = values });
        changed = true;
    }
    if (!changed) return null;
    return try std.json.Stringify.valueAlloc(a, parsed.value, .{});
}

test "capi ANN hydration keeps missing vectors local and honors scorer fields" {
    const a = std.testing.allocator;
    // Even non-JSON bytes bypass parsing when the scorer needs no vector.
    try std.testing.expect((try hydrateCandidate(a, "unparsed", &.{}, &.{ 1, 0 })) == null);
    try std.testing.expect((try hydrateCandidate(a, "unparsed", &.{"canonical_vector"}, null)) == null);
    const hydrated = (try hydrateCandidate(a, "{\"canonical_name\":\"Reviewer\"}", &.{"canonical_vector"}, &.{ 1, 0 })).?;
    defer a.free(hydrated);
    const parsed = try std.json.parseFromSlice(std.json.Value, a, hydrated, .{});
    defer parsed.deinit();
    try std.testing.expect(parsed.value.object.get("name_embedding") == null);
    try std.testing.expectEqual(@as(usize, 2), parsed.value.object.get("canonical_vector").?.array.items.len);
    const stored = "{\"canonical_vector\":[0,1],\"other_vector\":null}";
    try std.testing.expect((try hydrateCandidate(a, stored, &.{ "canonical_vector", "other_vector" }, &.{ 1, 0 })) == null);
    const mixed = (try hydrateCandidate(a, stored, &.{ "canonical_vector", "missing_vector" }, &.{ 1, 0 })).?;
    defer a.free(mixed);
    const mixed_parsed = try std.json.parseFromSlice(std.json.Value, a, mixed, .{});
    defer mixed_parsed.deinit();
    try std.testing.expectEqual(@as(i64, 0), mixed_parsed.value.object.get("canonical_vector").?.array.items[0].integer);
    try std.testing.expectEqual(@as(i64, 1), mixed_parsed.value.object.get("missing_vector").?.array.items[0].integer);
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
        \\{"full_text_search":{"match":{"field":"search_text","text":"embedder"}},"limit":5,"graph_queries":{"q":{"index":"knowledge","traverse":{"start":{"result_ref":"$query_results","limit":5},"direction":"both","edge_types":["about"],"max_depth":2,"include_documents":true,"include_paths":true,"limit":20}}}}
    ;
    try std.testing.expectEqual(h.capi.ErrorCode.ok, api.antfly_db_search_json(handle_id, .fromSlice(traversal), &response));
    try std.testing.expect(std.mem.indexOf(u8, response.ptr.?[0..response.len], "claim:b") != null);
    // Verbatim #1089 request: default deduplication, both directions, two hops,
    // document hydration and paths all survive the real C entry point.
    try std.testing.expect(std.mem.indexOf(u8, response.ptr.?[0..response.len], "\"path\"") != null);
    try std.testing.expect(std.mem.indexOf(u8, response.ptr.?[0..response.len], "\"document\"") != null);
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
    try entities.addIndex(.{ .name = "names", .kind = .dense_vector, .config_json = "{\"field\":\"canonical_vector\",\"dims\":3,\"embedding_name\":\"entity_names\",\"external\":true}" });
    // Exercise promotion -> generated embedding -> ANN visibility, with no
    // hand-written canonical row or vector. A deterministic provider makes
    // this production drain path run in ordinary CI without model downloads.
    const Embedder = struct {
        mention_calls: usize = 0,
        entity_calls: usize = 0,
        fn embed(ptr: *anyopaque, alloc: std.mem.Allocator, name: []const u8, _: []const u8, dims: u32) anyerror![]f32 {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            if (std.mem.eql(u8, name, "mention_names")) self.mention_calls += 1 else self.entity_calls += 1;
            if (dims != 3) return error.InvalidArgument;
            return try alloc.dupe(f32, &.{ 1, 0, 0 });
        }
    };
    var embed_context: Embedder = .{};
    try entities.addEnrichment(.{ .name = "entity_names", .kind = .embedding, .field = "canonical_name", .vector_space = "names", .expected_dims = 3 });
    try entities.reconfigureEnrichmentRuntime(.{ .enable_without_producers = true, .dense_embedder = .{ .ptr = &embed_context, .dense_embed_fn = Embedder.embed } });
    const config =
        \\{"name":"knowledge","type":"graph","source":{"artifact":"kg","format":"extraction_graph","mention_edge_type":"mentions"},"artifact":{"name":"kg","kind":"asset","source":{"type":"field","value":"kg"},"content_type":"application/json"},"edge_types":[{"name":"mentions"}],"resolvers":[{"name":"concepts","table":"entities","source_artifact":"kg","resolution_artifact":"resolved","key_template":"concept/{{ slug _entity.text }}","candidate_search":"ann","candidate_ann_index":"names","candidate_limit":1,"name_embedding":"mention_names","name_embedding_dims":3,"scorer_json":"{\"comparisons\":[{\"name\":\"embedding\",\"left\":\"name_embedding\",\"right\":\"canonical_embedding\",\"levels\":[{\"when\":\"cosine > 0.9\",\"weight\":8.0},{\"else\":true,\"weight\":-6.0}]}],\"combine\":{\"bias\":-3.0},\"decision\":{\"match\":0.9}}","config_generation":1}]}
    ;
    try std.testing.expectEqual(h.capi.ErrorCode.ok, api.antfly_db_add_index_json(id, .fromSlice(config)));
    const fake_embedder: @import("../storage/db/enrichment/embedder.zig").DenseEmbedder = .{ .ptr = &embed_context, .dense_embed_fn = Embedder.embed };
    try handle.db.reconfigureEnrichmentRuntime(.{ .enable_without_producers = true, .dense_embedder = fake_embedder });
    handle.db.setResolutionEmbedder(fake_embedder);
    try handle.db.batch(.{ .writes = &.{.{ .key = "claim:seed", .value = "{\"kg\":{\"entities\":[{\"id\":\"e0\",\"label\":\"concept\",\"text\":\"Approval reviewer\"}]}}" }} });
    try runUntilIdle(handle);
    var promoted = (try entities.lookup(a, "concept/approval_reviewer", .{})).?;
    defer promoted.deinit(a);
    try std.testing.expect(std.mem.indexOf(u8, promoted.json, "Approval reviewer") != null);

    try handle.db.batch(.{ .writes = &.{.{ .key = "claim:ann", .value = "{\"kg\":{\"entities\":[{\"id\":\"e0\",\"label\":\"concept\",\"text\":\"Laya reviewer\"},{\"id\":\"bad\",\"label\":\"concept\",\"text\":\"!!!\"}]}}" }} });
    try runUntilIdle(handle);
    const graph = &@constCast(handle.db.core.graphIndex("knowledge").?).index;
    const edges = try graph.getEdges(a, "claim:ann", "mentions", .out);
    defer @import("../graph/graph.zig").GraphIndex.freeEdges(a, edges);
    try std.testing.expectEqual(@as(usize, 1), edges.len);
    try std.testing.expectEqualStrings("concept/approval_reviewer", edges[0].target);
    try std.testing.expect((try entities.lookup(a, "concept/laya_reviewer", .{})) == null);
    // Public query resolution uses the stored ANN index and custom scorer
    // vector field, but must not publish a query document or any derived work.
    const sequence = handle.owned_lite_backend.?.native_docstore.?.file.activeCheckpoint().commit_sequence;
    var query_result: h.capi.Buffer = .{};
    defer api.antfly_buffer_free(&query_result);
    try std.testing.expectEqual(h.capi.ErrorCode.ok, api.antfly_db_resolve_query_json(id, .fromSlice("concepts"), .fromSlice(
        "{\"extraction\":{\"entities\":[{\"id\":\"q0\",\"label\":\"concept\",\"text\":\"Laya reviewer\"},{\"id\":\"bad\",\"label\":\"concept\",\"text\":\"!!!\"}]}}",
    ), &query_result));
    const query_json = try std.json.parseFromSlice(std.json.Value, a, query_result.ptr.?[0..query_result.len], .{});
    defer query_json.deinit();
    const query_resolution = query_json.value.object.get("resolution").?.object;
    const resolved = query_resolution.get("entities").?.array.items;
    try std.testing.expectEqual(@as(usize, 1), resolved.len);
    try std.testing.expectEqualStrings("concept/approval_reviewer", resolved[0].object.get("doc_ref").?.object.get("key").?.string);
    try std.testing.expectEqualStrings("entities", resolved[0].object.get("doc_ref").?.object.get("table").?.string);
    try std.testing.expectEqualStrings("match", resolved[0].object.get("decision").?.string);
    try std.testing.expectEqual(@as(i64, 1), query_resolution.get("skipped_mentions").?.integer);
    try std.testing.expectEqual(sequence, handle.owned_lite_backend.?.native_docstore.?.file.activeCheckpoint().commit_sequence);
    // A copy-only artifact cannot silently masquerade as a text extractor.
    var invalid_result: h.capi.Buffer = .{};
    try std.testing.expectEqual(h.capi.ErrorCode.invalid_argument, api.antfly_db_resolve_query_json(id, .fromSlice("concepts"), .fromSlice("{\"text\":\"A reviewer\"}"), &invalid_result));
    try std.testing.expectEqual(@as(usize, 0), invalid_result.len);
    try std.testing.expectEqual(h.capi.ErrorCode.invalid_argument, api.antfly_db_resolve_query_json(id, .fromSlice("concepts"), .fromSlice("{\"text\":\"x\",\"extraction\":{\"entities\":[]}}"), &invalid_result));
    try std.testing.expectEqual(h.capi.ErrorCode.not_found, api.antfly_db_resolve_query_json(id, .fromSlice("missing"), .fromSlice("{\"extraction\":{\"entities\":[]}}"), &invalid_result));
    try std.testing.expect(embed_context.mention_calls >= 3);
    try std.testing.expect(embed_context.entity_calls >= 1);
    // No vector on the alias row: ANN cannot return it as a candidate.
    try handle.db.entity_sink.?.upsert(a, "entities", "concept/the_decider", "{\"entity_type\":\"concept\",\"canonical_name\":\"The decider\",\"merged_into\":\"concept/approval_reviewer\"}");
    try handle.db.batch(.{ .writes = &.{.{ .key = "claim:alias", .value = "{\"kg\":{\"entities\":[{\"id\":\"e0\",\"label\":\"concept\",\"text\":\"The decider\"}]}}" }} });
    try runUntilIdle(handle);
    const alias_edges = try graph.getEdges(a, "claim:alias", "mentions", .out);
    defer @import("../graph/graph.zig").GraphIndex.freeEdges(a, alias_edges);
    try std.testing.expectEqual(@as(usize, 1), alias_edges.len);
    try std.testing.expectEqualStrings("concept/approval_reviewer", alias_edges[0].target);
}

test "capi Lite models_dir loads a real extractor for graph enrichment" {
    const api = @import("db.zig");
    const required = std.c.getenv("ANTFLY_LITE_REQUIRE_EXTRACTOR_TEST") != null;
    if (!api.localInferenceRuntimeAvailable()) return if (required) error.RequiredInferenceRuntimeUnavailable else error.SkipZigTest;
    const model_dir = std.mem.span(std.c.getenv("ANTFLY_LITE_EXTRACTOR_MODEL_DIR") orelse return if (required) error.RequiredExtractorModelMissing else error.SkipZigTest);
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
    const path = try std.fmt.allocPrintSentinel(a, "{s}/graph.aflite", .{tmp.path()}, 0);
    defer a.free(path);
    const options: h.capi.OpenOptions = .{
        .storage_kind = h.capi.storage_kind_lite,
        .profile = h.capi.profile_native,
        .flags = h.capi.open_flag_local_runtime_configured,
        .models_dir = .fromSlice(models_dir),
        .inference_host_budget_mb = 16384,
        .inference_backend_budget_mb = 16384,
        .inference_combined_budget_mb = 24576,
        .inference_scratch_budget_mb = 16384,
        .inference_process_memory_budget_mb = 32768,
    };
    var id: ?*anyopaque = null;
    try std.testing.expectEqual(h.capi.ErrorCode.ok, api.antfly_db_create_with_options(path, &options, &id));
    defer api.antfly_db_close(id);
    try std.testing.expectEqual(h.capi.ErrorCode.ok, tables.antfly_db_create_table_json(id, .fromSlice("entities"), .fromSlice("{}")));
    const config =
        \\{"name":"knowledge","type":"graph","source":{"artifact":"kg","format":"extraction_graph","mention_edge_type":"mentions"},"artifact":{"name":"kg","kind":"asset","source":{"type":"field","value":"body"},"content_type":"application/json","producer_json":{"type":"extractor","config":{"provider":"antfly","model":"explicit-base","schema":{"entities":["company","person","product","location"]}}}},"resolvers":[{"name":"concepts","table":"entities","source_artifact":"kg","resolution_artifact":"resolved","key_template":"concept/{{ uslug _entity.text }}","config_generation":1}],"edge_types":[{"name":"mentions"}],"metrics":{"ppr":{"kind":"pagerank","enabled":true}}}
    ;
    try std.testing.expectEqual(h.capi.ErrorCode.ok, api.antfly_db_add_index_json(id, .fromSlice(config)));
    try std.testing.expectEqual(h.capi.ErrorCode.ok, tables.antfly_db_create_table_json(id, .fromSlice("queries"), .fromSlice("{}")));
    var queries: ?*anyopaque = null;
    try std.testing.expectEqual(h.capi.ErrorCode.ok, tables.antfly_db_open_table(id, .fromSlice("queries"), &queries));
    try std.testing.expectEqual(h.capi.ErrorCode.ok, api.antfly_db_add_index_json(queries, .fromSlice(config)));
    api.antfly_db_close(queries);
    var batch_response: h.capi.Buffer = .{};
    defer api.antfly_buffer_free(&batch_response);
    try std.testing.expectEqual(h.capi.ErrorCode.ok, api.antfly_db_batch_json(id, .fromSlice("{\"inserts\":{\"claim:example\":{\"body\":\"Apple CEO Tim Cook announced iPhone 15 in Cupertino yesterday.\"}},\"sync_level\":\"write\"}"), &batch_response));
    try std.testing.expectEqual(h.capi.ErrorCode.ok, api.antfly_db_run_until_idle(id));
    var edge_response: h.capi.Buffer = .{};
    defer api.antfly_buffer_free(&edge_response);
    try std.testing.expectEqual(h.capi.ErrorCode.ok, api.antfly_db_get_edges_json(id, .fromSlice("knowledge"), .fromSlice("claim:example"), .fromSlice("mentions"), 0, &edge_response));
    const parsed_edges = try std.json.parseFromSlice(std.json.Value, a, edge_response.ptr.?[0..edge_response.len], .{});
    defer parsed_edges.deinit();
    try std.testing.expect(parsed_edges.value.array.items.len > 0);
    const encoded_target = parsed_edges.value.array.items[0].object.get("target_b64").?.string;
    const decoder = std.base64.standard.Decoder;
    const target = try a.alloc(u8, try decoder.calcSizeForSlice(encoded_target));
    defer a.free(target);
    try decoder.decode(target, encoded_target);
    var entities: ?*anyopaque = null;
    try std.testing.expectEqual(h.capi.ErrorCode.ok, tables.antfly_db_open_table(id, .fromSlice("entities"), &entities));
    defer api.antfly_db_close(entities);
    var entity_response: h.capi.Buffer = .{};
    defer api.antfly_buffer_free(&entity_response);
    try std.testing.expectEqual(h.capi.ErrorCode.ok, api.antfly_db_lookup_json(entities, .fromSlice(target), &entity_response));
    try std.testing.expect(entity_response.len > 0);
    const query = try std.fmt.allocPrint(a, "{{\"graph_metric\":{{\"index\":\"knowledge\",\"metric\":\"ppr\",\"metric_freshness\":\"fresh\",\"seed_nodes\":[{f}],\"direction\":\"both\",\"top_k\":20}}}}", .{std.json.fmt(target, .{})});
    defer a.free(query);
    var response: h.capi.Buffer = .{};
    defer api.antfly_buffer_free(&response);
    try std.testing.expectEqual(h.capi.ErrorCode.ok, api.antfly_db_search_json(id, .fromSlice(query), &response));
    const parsed_response = try std.json.parseFromSlice(std.json.Value, a, response.ptr.?[0..response.len], .{});
    defer parsed_response.deinit();
    const metrics = parsed_response.value.object.get("responses").?.array.items[0].object.get("graph_metric_results").?.object;
    var values = metrics.iterator();
    var found = false;
    while (values.next()) |metric| for (metric.value_ptr.object.get("scores").?.array.items) |score| {
        if (!std.mem.eql(u8, score.object.get("node").?.string, "claim:example")) continue;
        const rank = score.object.get("score").?;
        try std.testing.expect(if (rank == .float) rank.float > 0 else rank.integer > 0);
        found = true;
    };
    try std.testing.expect(found);
    // The ad-hoc text route runs the configured extractor and resolves its
    // real, non-empty mentions against the entities promoted above.
    var query_result: h.capi.Buffer = .{};
    defer api.antfly_buffer_free(&query_result);
    const before = api.enterHandle(id, .read).?;
    const sequence = before.handle.owned_lite_backend.?.native_docstore.?.file.activeCheckpoint().commit_sequence;
    before.leave();
    try std.testing.expectEqual(h.capi.ErrorCode.ok, api.antfly_db_resolve_query_json(id, .fromSlice("concepts"), .fromSlice("{\"text\":\"Apple CEO Tim Cook announced iPhone 15 in Cupertino yesterday.\"}"), &query_result));
    const query_json = try std.json.parseFromSlice(std.json.Value, a, query_result.ptr.?[0..query_result.len], .{});
    defer query_json.deinit();
    const resolved = query_json.value.object.get("resolution").?.object.get("entities").?.array.items;
    try std.testing.expect(resolved.len > 0);
    for (resolved) |entity| {
        try std.testing.expectEqualStrings("entities", entity.object.get("doc_ref").?.object.get("table").?.string);
        // A scorer-free resolver deterministically renders keys and reports
        // `new` on ingestion too, even when those keys already exist.
        try std.testing.expectEqualStrings("new", entity.object.get("decision").?.string);
        api.antfly_buffer_free(&entity_response);
        try std.testing.expectEqual(h.capi.ErrorCode.ok, api.antfly_db_lookup_json(entities, .fromSlice(entity.object.get("doc_ref").?.object.get("key").?.string), &entity_response));
    }
    const after = api.enterHandle(id, .read).?;
    try std.testing.expectEqual(sequence, after.handle.owned_lite_backend.?.native_docstore.?.file.activeCheckpoint().commit_sequence);
    after.leave();
    api.antfly_db_close(entities);
    entities = null;
    api.antfly_db_close(id);
    id = null;
    var readonly = options;
    readonly.open_mode = h.capi.open_mode_readonly;
    try std.testing.expectEqual(h.capi.ErrorCode.ok, api.antfly_db_open_with_options(path, &readonly, &id));
    api.antfly_buffer_free(&query_result);
    try std.testing.expectEqual(h.capi.ErrorCode.ok, api.antfly_db_resolve_query_json(id, .fromSlice("concepts"), .fromSlice("{\"text\":\"Apple CEO Tim Cook announced iPhone 15 in Cupertino yesterday.\"}"), &query_result));
    const readonly_guard = api.enterHandle(id, .read).?;
    const cached_producer = readonly_guard.handle.query_producer orelse return error.TestExpectedEqual;
    const readonly_sequence = readonly_guard.handle.owned_lite_backend.?.native_docstore.?.file.activeCheckpoint().commit_sequence;
    readonly_guard.leave();
    api.antfly_buffer_free(&query_result);
    try std.testing.expectEqual(h.capi.ErrorCode.ok, api.antfly_db_resolve_query_json(id, .fromSlice("concepts"), .fromSlice("{\"text\":\"Apple CEO Tim Cook announced iPhone 15 in Cupertino yesterday.\"}"), &query_result));
    const repeated_guard = api.enterHandle(id, .read).?;
    try std.testing.expectEqual(cached_producer, repeated_guard.handle.query_producer.?);
    try std.testing.expectEqual(readonly_sequence, repeated_guard.handle.owned_lite_backend.?.native_docstore.?.file.activeCheckpoint().commit_sequence);
    repeated_guard.leave();
    var readonly_table: ?*anyopaque = null;
    try std.testing.expectEqual(h.capi.ErrorCode.ok, tables.antfly_db_open_table(id, .fromSlice("queries"), &readonly_table));
    defer api.antfly_db_close(readonly_table);
    var table_query: h.capi.Buffer = .{};
    defer api.antfly_buffer_free(&table_query);
    const text_request = "{\"text\":\"Apple CEO Tim Cook announced iPhone 15 in Cupertino yesterday.\"}";
    try std.testing.expectEqual(h.capi.ErrorCode.ok, api.antfly_db_resolve_query_json(readonly_table, .fromSlice("concepts"), .fromSlice(text_request), &table_query));
    const child, const slot = api.handle_registry.enter(readonly_table).?;
    try std.testing.expect(child.query_producer != null);
    api.HandleRegistry.leave(slot);
    var writer: ?*anyopaque = null;
    try std.testing.expectEqual(h.capi.ErrorCode.ok, api.antfly_db_open_with_options(path, &options, &writer));
    defer api.antfly_db_close(writer);
    var write_result: h.capi.Buffer = .{};
    defer api.antfly_buffer_free(&write_result);
    try std.testing.expectEqual(h.capi.ErrorCode.ok, api.antfly_db_batch_json(writer, .fromSlice("{\"inserts\":{\"refresh_probe\":{\"body\":\"snapshot changed\"}}}"), &write_result));
    // Entering any read refreshes the owner, invalidating child caches before
    // freeing the old provider. No stale producer survives to the next query.
    const refreshed = api.enterHandle(readonly_table, .read).?;
    try std.testing.expect(refreshed.entry_error == null);
    try std.testing.expect(refreshed.handle.query_producer == null);
    refreshed.leave();
    api.antfly_buffer_free(&table_query);
    try std.testing.expectEqual(h.capi.ErrorCode.ok, api.antfly_db_resolve_query_json(readonly_table, .fromSlice("concepts"), .fromSlice(text_request), &table_query));
    try std.testing.expect(table_query.len > 0);
    const readonly_query = try std.json.parseFromSlice(std.json.Value, a, query_result.ptr.?[0..query_result.len], .{});
    defer readonly_query.deinit();
    const readonly_entities = readonly_query.value.object.get("resolution").?.object.get("entities").?.array.items;
    try std.testing.expectEqual(resolved.len, readonly_entities.len);
    for (readonly_entities) |entity| try std.testing.expectEqualStrings("new", entity.object.get("decision").?.string);
}

test "capi resolver render and merge rekey existing edges without entity promotion" {
    const api = @import("db.zig");
    const a = std.testing.allocator;
    var tmp = try api.TestDirectory.init("resolver-public-keys");
    defer tmp.cleanup();
    const path = try std.fmt.allocPrintSentinel(a, "{s}.aflite", .{tmp.path()}, 0);
    defer a.free(path);
    const options: h.capi.OpenOptions = .{ .storage_kind = h.capi.storage_kind_lite, .profile = h.capi.profile_native, .flags = h.capi.open_flag_generated_enrichment_replay };
    var id: ?*anyopaque = null;
    try std.testing.expectEqual(h.capi.ErrorCode.ok, api.antfly_db_create_with_options(path, &options, &id));
    defer api.antfly_db_close(id);
    const config =
        \\{"name":"knowledge","type":"graph","source":{"artifact":"kg","format":"extraction_graph","mention_edge_type":"mentions"},"artifact":{"name":"kg","kind":"asset","source":{"type":"field","value":"kg"},"content_type":"application/json"},"edge_types":[{"name":"mentions"},{"name":"uses"}],"resolvers":[{"name":"concepts","table":"default","promote":false,"source_artifact":"kg","resolution_artifact":"resolved","key_template":"concept/{{ uslug _entity.text }}","aliases":[{"key":"concept/qwen3","target":"concept/qwen3_embedding"}],"config_generation":1},{"name":"events","table":"default","promote":false,"source_artifact":"kg","resolution_artifact":"events_resolved","key_template":"event/{{ uslug _entity.text }}","labels":["event"],"config_generation":1}]}
    ;
    try std.testing.expectEqual(h.capi.ErrorCode.ok, api.antfly_db_add_index_json(id, .fromSlice(config)));
    var out: h.capi.Buffer = .{};
    defer api.antfly_buffer_free(&out);
    try std.testing.expectEqual(h.capi.ErrorCode.invalid_argument, api.antfly_db_render_resolver_key_json(id, .fromSlice("concepts"), .fromSlice("{\"text\":\"Launch\",\"label\":\"event\"}"), &out));
    try std.testing.expect(out.ptr == null);
    try std.testing.expectEqual(h.capi.ErrorCode.ok, api.antfly_db_render_resolver_key_json(id, .fromSlice("events"), .fromSlice("{\"text\":\"Launch\",\"label\":\"event\"}"), &out));
    try std.testing.expect(std.mem.indexOf(u8, out.ptr.?[0..out.len], "event/launch") != null);
    api.antfly_buffer_free(&out);
    try std.testing.expectEqual(h.capi.ErrorCode.ok, api.antfly_db_resolve_query_json(id, .fromSlice("concepts"), .fromSlice("{\"extraction\":{\"entities\":[{\"id\":\"e\",\"text\":\"Launch\",\"label\":\"event\"}]}}"), &out));
    const excluded_query = try std.json.parseFromSlice(std.json.Value, a, out.ptr.?[0..out.len], .{});
    defer excluded_query.deinit();
    try std.testing.expectEqual(@as(usize, 0), excluded_query.value.object.get("resolution").?.object.get("entities").?.array.items.len);
    api.antfly_buffer_free(&out);

    try std.testing.expectEqual(h.capi.ErrorCode.ok, api.antfly_db_render_resolver_key_json(id, .fromSlice("concepts"), .fromSlice("{\"text\":\"Qwen3\",\"label\":\"concept\"}"), &out));
    try std.testing.expect(std.mem.indexOf(u8, out.ptr.?[0..out.len], "concept/qwen3_embedding") != null);
    api.antfly_buffer_free(&out);
    try std.testing.expectEqual(h.capi.ErrorCode.ok, api.antfly_db_batch_json(id, .fromSlice("{\"inserts\":{\"claim:a\":{\"kg\":{\"entities\":[{\"id\":\"e0\",\"label\":\"concept\",\"text\":\"Qwen3\"},{\"id\":\"e1\",\"label\":\"concept\",\"text\":\"Antfly\"}],\"relations\":[{\"type\":\"uses\",\"source\":\"e0\",\"target\":\"e1\"}]}}}}"), &out));
    api.antfly_buffer_free(&out);
    try std.testing.expectEqual(h.capi.ErrorCode.ok, api.antfly_db_run_until_idle(id));
    try std.testing.expectEqual(h.capi.ErrorCode.not_found, api.antfly_db_lookup_json(id, .fromSlice("concept/qwen3_embedding"), &out));
    try std.testing.expectEqual(h.capi.ErrorCode.ok, api.antfly_db_merge_resolver_keys_json(id, .fromSlice("concepts"), .fromSlice("{\"key\":\"concept/qwen3_embedding\",\"target\":\"concept/qwen\"}")));
    try std.testing.expectEqual(h.capi.ErrorCode.ok, api.antfly_db_run_until_idle(id));
    try std.testing.expectEqual(h.capi.ErrorCode.ok, api.antfly_db_get_edges_json(id, .fromSlice("knowledge"), .fromSlice("claim:a"), .fromSlice("mentions"), 0, &out));
    var result = try std.json.parseFromSlice(std.json.Value, a, out.ptr.?[0..out.len], .{});
    defer result.deinit();
    const edges = result.value.array.items;
    try std.testing.expectEqual(@as(usize, 2), edges.len);
    var found_canonical = false;
    for (edges) |edge| {
        const encoded = edge.object.get("target_b64").?.string;
        const decoded = try a.alloc(u8, try std.base64.standard.Decoder.calcSizeForSlice(encoded));
        defer a.free(decoded);
        try std.base64.standard.Decoder.decode(decoded, encoded);
        if (std.mem.eql(u8, decoded, "concept/qwen")) found_canonical = true;
        try std.testing.expect(!std.mem.eql(u8, decoded, "concept/qwen3_embedding"));
    }
    try std.testing.expect(found_canonical);
    api.antfly_buffer_free(&out);
    try std.testing.expectEqual(h.capi.ErrorCode.ok, api.antfly_db_get_edges_json(id, .fromSlice("knowledge"), .fromSlice("concept/qwen"), .fromSlice("uses"), 0, &out));
    const merged_relations = try std.json.parseFromSlice(std.json.Value, a, out.ptr.?[0..out.len], .{});
    defer merged_relations.deinit();
    try std.testing.expectEqual(@as(usize, 1), merged_relations.value.array.items.len);
    // Recreate the index using the original declarative config, which omits
    // the API-created merge. Resolver identity state outlives index configs.
    var deleted = false;
    try std.testing.expectEqual(h.capi.ErrorCode.ok, api.antfly_db_delete_index(id, .fromSlice("knowledge"), &deleted));
    try std.testing.expect(deleted);
    try std.testing.expectEqual(h.capi.ErrorCode.ok, api.antfly_db_add_index_json(id, .fromSlice(config)));
    try std.testing.expectEqual(h.capi.ErrorCode.ok, api.antfly_db_run_until_idle(id));
    api.antfly_buffer_free(&out);
    try std.testing.expectEqual(h.capi.ErrorCode.ok, api.antfly_db_get_edges_json(id, .fromSlice("knowledge"), .fromSlice("concept/qwen"), .fromSlice("uses"), 0, &out));
    const relations = try std.json.parseFromSlice(std.json.Value, a, out.ptr.?[0..out.len], .{});
    defer relations.deinit();
    try std.testing.expectEqual(@as(usize, 1), relations.value.array.items.len);
    api.antfly_buffer_free(&out);
    try std.testing.expectEqual(h.capi.ErrorCode.ok, api.antfly_db_get_edges_json(id, .fromSlice("knowledge"), .fromSlice("concept/qwen3_embedding"), .fromSlice("uses"), 0, &out));
    const old_relations = try std.json.parseFromSlice(std.json.Value, a, out.ptr.?[0..out.len], .{});
    defer old_relations.deinit();
    try std.testing.expectEqual(@as(usize, 0), old_relations.value.array.items.len);

    try std.testing.expectEqual(h.capi.ErrorCode.invalid_argument, api.antfly_db_merge_resolver_keys_json(id, .fromSlice("concepts"), .fromSlice("{\"key\":\"concept/qwen\",\"target\":\"concept/qwen3\"}")));
    try std.testing.expectEqual(h.capi.ErrorCode.ok, api.antfly_db_merge_resolver_keys_json(id, .fromSlice("concepts"), .fromSlice("{\"key\":\"concept/qwen3_embedding\",\"target\":\"concept/qwen\"}")));
    api.antfly_db_close(id);
    id = null;
    try std.testing.expectEqual(h.capi.ErrorCode.ok, api.antfly_db_open_with_options(path, &options, &id));
    try std.testing.expectEqual(h.capi.ErrorCode.ok, api.antfly_db_add_index_json(id, .fromSlice(config)));
    try std.testing.expectEqual(h.capi.ErrorCode.ok, api.antfly_db_run_until_idle(id));
    api.antfly_buffer_free(&out);
    try std.testing.expectEqual(h.capi.ErrorCode.ok, api.antfly_db_get_edges_json(id, .fromSlice("knowledge"), .fromSlice("concept/qwen"), .fromSlice("uses"), 0, &out));
    const replayed_edges = try std.json.parseFromSlice(std.json.Value, a, out.ptr.?[0..out.len], .{});
    defer replayed_edges.deinit();
    try std.testing.expectEqual(@as(usize, 1), replayed_edges.value.array.items.len);
    api.antfly_buffer_free(&out);
    try std.testing.expectEqual(h.capi.ErrorCode.ok, api.antfly_db_render_resolver_key_json(id, .fromSlice("concepts"), .fromSlice("{\"text\":\"Qwen3\",\"label\":\"concept\"}"), &out));
    const rendered = try std.json.parseFromSlice(std.json.Value, a, out.ptr.?[0..out.len], .{});
    defer rendered.deinit();
    try std.testing.expectEqualStrings("concept/qwen", rendered.value.object.get("key").?.string);
    try std.testing.expectEqual(@as(i64, 2), rendered.value.object.get("config_generation").?.integer);
    // Read-only handles can query aliases without canonical documents and
    // observe the same durable generation after reopening.
    api.antfly_db_close(id);
    id = null;
    var readonly = options;
    readonly.open_mode = h.capi.open_mode_readonly;
    try std.testing.expectEqual(h.capi.ErrorCode.ok, api.antfly_db_open_with_options(path, &readonly, &id));
    api.antfly_buffer_free(&out);
    try std.testing.expectEqual(h.capi.ErrorCode.ok, api.antfly_db_resolve_query_json(id, .fromSlice("concepts"), .fromSlice("{\"extraction\":{\"entities\":[{\"id\":\"q0\",\"label\":\"concept\",\"text\":\"Qwen3\"}]}}"), &out));
    const queried = try std.json.parseFromSlice(std.json.Value, a, out.ptr.?[0..out.len], .{});
    defer queried.deinit();
    const resolution = queried.value.object.get("resolution").?.object;
    try std.testing.expectEqualStrings("concept/qwen", resolution.get("entities").?.array.items[0].object.get("doc_ref").?.object.get("key").?.string);
    try std.testing.expectEqualStrings("match", resolution.get("entities").?.array.items[0].object.get("decision").?.string);
    try std.testing.expectEqual(@as(i64, 2), resolution.get("config_generation").?.integer);
}

test "capi resolver config merge config advances revisions and protects key namespaces" {
    const api = @import("db.zig");
    const a = std.testing.allocator;
    var tmp = try api.TestDirectory.init("resolver-revision");
    defer tmp.cleanup();
    const path = try std.fmt.allocPrintSentinel(a, "{s}.aflite", .{tmp.path()}, 0);
    defer a.free(path);
    const options: h.capi.OpenOptions = .{ .storage_kind = h.capi.storage_kind_lite, .profile = h.capi.profile_native, .flags = h.capi.open_flag_generated_enrichment_replay };
    var id: ?*anyopaque = null;
    try std.testing.expectEqual(h.capi.ErrorCode.ok, api.antfly_db_create_with_options(path, &options, &id));
    defer api.antfly_db_close(id);
    const config =
        \\{"name":"knowledge","type":"graph","source":{"artifact":"kg","format":"extraction_graph","mention_edge_type":"mentions"},"artifact":{"name":"kg","kind":"asset","source":{"type":"field","value":"kg"},"content_type":"application/json"},"edge_types":[{"name":"mentions"}],"resolvers":[{"name":"concepts","table":"default","promote":false,"source_artifact":"kg","resolution_artifact":"resolved","key_template":"concept/{{ uslug _entity.text }}","config_generation":1}]}
    ;
    try std.testing.expectEqual(h.capi.ErrorCode.ok, api.antfly_db_add_index_json(id, .fromSlice(config)));
    var out: h.capi.Buffer = .{};
    defer api.antfly_buffer_free(&out);
    try std.testing.expectEqual(h.capi.ErrorCode.ok, api.antfly_db_batch_json(id, .fromSlice("{\"inserts\":{\"claim:a\":{\"kg\":{\"entities\":[{\"id\":\"e0\",\"label\":\"concept\",\"text\":\"Qwen3\"}]}}}}"), &out));
    const Check = struct {
        fn bump(handle_id: ?*anyopaque, generation: u64) !void {
            const guard = api.enterHandle(handle_id, .exclusive).?;
            defer guard.leave();
            const configs = try guard.handle.database().listResolvers(std.testing.allocator);
            defer {
                for (configs) |*cfg| cfg.deinit(std.testing.allocator);
                std.testing.allocator.free(configs);
            }
            var cfg = configs[0];
            cfg.config_generation = generation;
            _ = try guard.handle.database().upsertResolverWithResultOptions(cfg, .{ .drain_backfill = false });
        }
        fn storedRevision(handle_id: ?*anyopaque, revision: u64) !void {
            try std.testing.expectEqual(h.capi.ErrorCode.ok, api.antfly_db_run_until_idle(handle_id));
            const guard = api.enterHandle(handle_id, .read).?;
            defer guard.leave();
            const key = try @import("../storage/internal_keys.zig").resolutionArtifactKeyAlloc(std.testing.allocator, "claim:a", "resolved");
            defer std.testing.allocator.free(key);
            var txn = try guard.handle.database().core.store.beginReadTxn();
            defer txn.abort();
            const bytes = try txn.get(key);
            const expected = try std.fmt.allocPrint(std.testing.allocator, "\"config_generation\":{d}", .{revision});
            defer std.testing.allocator.free(expected);
            try std.testing.expect(std.mem.indexOf(u8, bytes, expected) != null);
        }
    };
    try Check.bump(id, 2);
    try Check.storedRevision(id, 2);
    try std.testing.expectEqual(h.capi.ErrorCode.ok, api.antfly_db_merge_resolver_keys_json(id, .fromSlice("concepts"), .fromSlice("{\"key\":\"concept/qwen3\",\"target\":\"concept/qwen\"}")));
    try Check.storedRevision(id, 3);
    try Check.bump(id, 3);
    try Check.storedRevision(id, 4);
    {
        const guard = api.enterHandle(id, .exclusive).?;
        defer guard.leave();
        const configs = try guard.handle.database().listResolvers(a);
        defer {
            for (configs) |*cfg| cfg.deinit(a);
            a.free(configs);
        }
        var metadata_only = configs[0];
        metadata_only.merge_generation += 100;
        try std.testing.expectEqual(@import("../storage/db/catalog/index_manager.zig").IndexManager.ResolverUpsertResult.updated_no_backfill, try guard.handle.database().upsertResolverWithResultOptions(metadata_only, .{ .drain_backfill = false }));
        var changed = configs[0];
        changed.table = "other";
        try std.testing.expectError(error.ResolverMergeNamespaceChange, guard.handle.database().upsertResolverWithResultOptions(changed, .{ .drain_backfill = false }));
        changed = configs[0];
        changed.key_template = "other/{{ uslug _entity.text }}";
        try std.testing.expectError(error.ResolverMergeNamespaceChange, guard.handle.database().upsertResolverWithResultOptions(changed, .{ .drain_backfill = false }));
    }
    api.antfly_buffer_free(&out);
    try std.testing.expectEqual(h.capi.ErrorCode.ok, api.antfly_db_resolve_query_json(id, .fromSlice("concepts"), .fromSlice("{\"extraction\":{\"entities\":[{\"id\":\"q\",\"label\":\"concept\",\"text\":\"Qwen3\"}]}}"), &out));
    const queried = try std.json.parseFromSlice(std.json.Value, a, out.ptr.?[0..out.len], .{});
    defer queried.deinit();
    const resolution = queried.value.object.get("resolution").?.object;
    try std.testing.expectEqual(@as(i64, 4), resolution.get("config_generation").?.integer);
    try std.testing.expectEqualStrings("concept/qwen", resolution.get("entities").?.array.items[0].object.get("doc_ref").?.object.get("key").?.string);
}
