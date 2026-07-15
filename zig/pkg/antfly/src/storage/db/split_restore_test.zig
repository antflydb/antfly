// Copyright 2026 Antfly, Inc.
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

const std = @import("std");
const platform = @import("antfly_platform");

const db_mod = @import("mod.zig");
const db_config = @import("config.zig");
const db_internal = @import("internal.zig");
const docstore_mod = @import("../docstore.zig");
const doc_identity = @import("doc_identity.zig");
const doc_set = @import("doc_set.zig");
const embedder_mod = @import("enrichment/embedder.zig");
const graph_mod = @import("../../graph/graph.zig");
const internal_keys = @import("../internal_keys.zig");
const relational_row_codec = @import("algebraic/relational_row_codec.zig");
const relational_store_mod = @import("relational_store.zig");
const schema_mod = @import("../schema.zig");
const schema_api_mod = @import("../../schema/mod.zig");
const split_restore = @import("split_restore.zig");
const TestHelpers = @import("test_support.zig");
const types = @import("types.zig");

const DB = db_mod.DB;
const PrimaryBackend = db_mod.PrimaryBackend;

fn waitForSearchResult(alloc: std.mem.Allocator, db: *DB, req: types.SearchRequest, min_hits: u32) !types.SearchResult {
    var last = try db.search(alloc, req);
    var attempts: usize = 0;
    while (last.total_hits < min_hits and attempts < 100) : (attempts += 1) {
        last.deinit();
        platform.time.sleepMs(10);
        last = try db.search(alloc, req);
    }
    if (last.total_hits < min_hits) {
        last.deinit();
        return error.Timeout;
    }
    return last;
}

fn waitForAppliedSequenceAdvance(
    alloc: std.mem.Allocator,
    db: *DB,
    index_name: []const u8,
    previous: u64,
) !u64 {
    var applied = try db.core.loadAppliedSequence(alloc, index_name);
    var attempts: usize = 0;
    while (applied <= previous and attempts < 100) : (attempts += 1) {
        platform.time.sleepMs(10);
        applied = try db.core.loadAppliedSequence(alloc, index_name);
    }
    if (applied <= previous) return error.Timeout;
    return applied;
}

fn orderedTupleIndexKeys(runtime_schema: schema_mod.TableSchema, index_name: []const u8) ![]const schema_mod.RelationalIndexKey {
    for (runtime_schema.relational_indexes) |index| {
        if (!std.mem.eql(u8, index.name, index_name)) continue;
        if (index.access_method != .ordered_tuple) return error.TestUnexpectedResult;
        return index.keys;
    }
    return error.TestUnexpectedResult;
}

fn orderedTupleValueForDocKeyAlloc(
    alloc: std.mem.Allocator,
    store: *docstore_mod.DocStore,
    runtime_schema: schema_mod.TableSchema,
    doc_key: []const u8,
) ![]u8 {
    const index_keys = try orderedTupleIndexKeys(runtime_schema, "status_amount_idx");
    const row = (try relational_store_mod.getRawAlloc(alloc, store, doc_key)) orelse return error.TestExpectedEqual;
    defer alloc.free(row);
    return try relational_store_mod.orderedTupleValueForIndexKeysAlloc(alloc, row, index_keys, runtime_schema.relational_columns);
}

fn expectSingleOrderedTupleDocKeyForTuple(
    alloc: std.mem.Allocator,
    store: *docstore_mod.DocStore,
    tuple: []const u8,
    lower_doc_key: []const u8,
    upper_doc_key: []const u8,
    expected_doc_key: []const u8,
) !void {
    const doc_keys = try relational_store_mod.scanOrderedTupleDocKeysAlloc(alloc, store, "status_amount_idx", tuple, lower_doc_key, upper_doc_key);
    defer relational_store_mod.freeDocKeys(alloc, doc_keys);
    try std.testing.expectEqual(@as(usize, 1), doc_keys.len);
    try std.testing.expectEqualStrings(expected_doc_key, doc_keys[0]);
}

fn expectSingleOrderedTupleDocKey(
    alloc: std.mem.Allocator,
    store: *docstore_mod.DocStore,
    runtime_schema: schema_mod.TableSchema,
    doc_key: []const u8,
) !void {
    const tuple = try orderedTupleValueForDocKeyAlloc(alloc, store, runtime_schema, doc_key);
    defer alloc.free(tuple);
    try expectSingleOrderedTupleDocKeyForTuple(alloc, store, tuple, doc_key, doc_key, doc_key);
}

fn expectNoOrderedTupleDocKeysForTuple(
    alloc: std.mem.Allocator,
    store: *docstore_mod.DocStore,
    tuple: []const u8,
    lower_doc_key: []const u8,
    upper_doc_key: []const u8,
) !void {
    const doc_keys = try relational_store_mod.scanOrderedTupleDocKeysAlloc(alloc, store, "status_amount_idx", tuple, lower_doc_key, upper_doc_key);
    defer relational_store_mod.freeDocKeys(alloc, doc_keys);
    try std.testing.expectEqual(@as(usize, 0), doc_keys.len);
}

fn expectNoOrderedTupleEntriesForDocKey(
    alloc: std.mem.Allocator,
    store: *docstore_mod.DocStore,
    doc_key: []const u8,
) !void {
    const lower = try internal_keys.relationalOrderedTupleIndexByDocRangeLowerAlloc(alloc, doc_key);
    defer alloc.free(lower);
    const upper = (try internal_keys.relationalOrderedTupleIndexByDocRangeUpperAlloc(alloc, doc_key)) orelse return error.TestUnexpectedResult;
    defer alloc.free(upper);
    const entries = try store.scanRange(alloc, lower, upper);
    defer docstore_mod.DocStore.freeResults(alloc, entries);
    try std.testing.expectEqual(@as(usize, 0), entries.len);
}

test "db split state and split deltas are exposed through public api" {
    const alloc = std.testing.allocator;

    var path_buf: [256]u8 = undefined;
    const path = TestHelpers.tempPath(&path_buf);
    defer TestHelpers.cleanupTempDir(path);

    var db = try DB.open(alloc, std.mem.span(path), .{});
    defer db.close();

    var split_state = types.SplitState{
        .phase = .splitting,
        .split_key = try alloc.dupe(u8, "doc:m"),
        .new_shard_id = 42,
        .started_at = 1_000,
        .original_range_end = try alloc.dupe(u8, ""),
    };
    defer split_state.deinit(alloc);

    try db.setSplitState(split_state);

    const loaded_state = (try db.getSplitState(alloc)) orelse return error.TestExpectedEqual;
    defer {
        var state = loaded_state;
        state.deinit(alloc);
    }
    try std.testing.expectEqual(types.SplitPhase.splitting, loaded_state.phase);
    try std.testing.expectEqualStrings("doc:m", loaded_state.split_key);
    try std.testing.expectEqual(@as(u64, 42), loaded_state.new_shard_id);

    try db.batch(.{
        .writes = &.{
            .{ .key = "doc:a", .value = "{\"title\":\"alpha\"}" },
            .{ .key = "doc:b", .value = "{\"title\":\"beta\"}" },
        },
        .deletes = &.{"doc:gone"},
        .timestamp_ns = 55_000,
    });

    try std.testing.expectEqual(@as(u64, 1), db.getSplitDeltaSeq());

    const entries = try db.listSplitDeltaEntriesAfter(alloc, 0);
    defer types.freeSplitDeltaEntries(alloc, entries);
    try std.testing.expectEqual(@as(usize, 1), entries.len);
    try std.testing.expectEqual(@as(u64, 55_000), entries[0].timestamp);
    try std.testing.expect(entries[0].writes.len >= 4);
    try std.testing.expect(entries[0].deletes.len >= 2);

    try db.setSplitDeltaFinalSeq(9);
    try std.testing.expectEqual(@as(u64, 9), try db.getSplitDeltaFinalSeq(alloc));
    try db.clearSplitDeltaEntries();
    try std.testing.expectEqual(@as(u64, 0), db.getSplitDeltaSeq());
    try db.clearSplitDeltaFinalSeq();
    try std.testing.expectEqual(@as(u64, 0), try db.getSplitDeltaFinalSeq(alloc));
}

test "db split finalization marks split-off document child ranges remote" {
    const alloc = std.testing.allocator;

    var path_buf: [256]u8 = undefined;
    const path = TestHelpers.tempPath(&path_buf);
    defer TestHelpers.cleanupTempDir(path);

    var db = try DB.open(alloc, std.mem.span(path), .{
        .start_index_workers = false,
        .ttl_cleanup = .{ .enabled = false },
    });
    defer db.close();

    try db.batch(.{
        .writes = &.{
            .{ .key = "doc:a", .value = "{\"title\":\"alpha\"}" },
            .{ .key = "doc:z", .value = "{\"title\":\"zeta\"}" },
        },
    });

    const manifest_key = try internal_keys.artifactNamedPrefixAlloc(alloc, "doc:a", "asset", "document_units_v1");
    defer alloc.free(manifest_key);
    const moved_child_key = try internal_keys.documentUnitArtifactKeyAlloc(alloc, "doc:z", "document_units_v1", "page:000001");
    defer alloc.free(moved_child_key);

    const ChildRangeJson = struct {
        range_id: []const u8,
        range_kind: []const u8,
        artifact_name: []const u8,
        split_boundary: []const u8,
        placement: []const u8,
        owner_group_id: u64,
        placement_generation: u64,
        route_status: []const u8,
        split_eligible: bool,
        start_key: []const u8,
        end_key_exclusive: []const u8,
        last_key: []const u8,
        child_count: usize,
    };
    const ManifestJson = struct {
        manifest_version: u64,
        generation: u64,
        artifact_name: []const u8,
        source_url: []const u8,
        source_fingerprint: []const u8,
        content_type: []const u8,
        route_type: []const u8,
        child_ranges: []const ChildRangeJson,
    };
    const child_ranges = [_]ChildRangeJson{.{
        .range_id = "range:000000",
        .range_kind = "unit",
        .artifact_name = "document_units_v1",
        .split_boundary = "unit",
        .placement = "parent",
        .owner_group_id = 0,
        .placement_generation = 4,
        .route_status = "local_committed",
        .split_eligible = true,
        .start_key = moved_child_key,
        .end_key_exclusive = "",
        .last_key = moved_child_key,
        .child_count = 1,
    }};
    const manifest = try std.json.Stringify.valueAlloc(alloc, ManifestJson{
        .manifest_version = 2,
        .generation = 1,
        .artifact_name = "document_units_v1",
        .source_url = "data:text/plain;base64,YWxwaGE=",
        .source_fingerprint = "fp",
        .content_type = "text/plain",
        .route_type = "text",
        .child_ranges = child_ranges[0..],
    }, .{});
    defer alloc.free(manifest);
    const child_value = "{\"_parent_doc_key\":\"doc:a\",\"_artifact_name\":\"document_units_v1\",\"unit_id\":\"page:000001\",\"text\":\"moved\"}";
    try db.core.store.putBatch(&.{
        .{ .key = manifest_key, .value = manifest },
        .{ .key = moved_child_key, .value = child_value },
    }, &.{});

    try db.core.shard_manager.prepareSplit("doc:m");
    try db.core.shard_manager.split(7002, "doc:m");
    db.core.index_manager.updateRange(db.core.shard_manager.getByteRange());
    try db.finalizeSplit(.{ .start = "", .end = "doc:m" });

    const updated = try db.core.store.get(alloc, manifest_key);
    defer alloc.free(updated);
    try std.testing.expect(std.mem.indexOf(u8, updated, "\"placement\":\"remote\"") != null);
    try std.testing.expect(std.mem.indexOf(u8, updated, "\"owner_group_id\":7002") != null);
    try std.testing.expect(std.mem.indexOf(u8, updated, "\"placement_generation\":5") != null);
    try std.testing.expect(std.mem.indexOf(u8, updated, "\"route_status\":\"remote_committed\"") != null);
    try std.testing.expect(std.mem.indexOf(u8, updated, "\"split_eligible\":false") != null);
    try std.testing.expectError(error.NotFound, db.core.store.get(alloc, moved_child_key));
}

test "db shadow index manager backfills split-off range and ignores parent-range live writes after split" {
    const alloc = std.testing.allocator;

    var path_buf: [256]u8 = undefined;
    const path = TestHelpers.tempPath(&path_buf);
    defer TestHelpers.cleanupTempDir(path);

    var db = try DB.open(alloc, std.mem.span(path), .{});
    defer db.close();

    try db.addIndex(.{
        .name = "ft_v1",
        .kind = .full_text,
        .config_json = "{\"field\":\"title\"}",
    });
    try db.addIndex(.{
        .name = "graph_v1",
        .kind = .graph,
        .config_json = "{}",
    });

    try db.batch(.{
        .writes = &.{
            .{ .key = "doc:a", .value = "{\"title\":\"alpha\"}" },
            .{ .key = "doc:z", .value = "{\"title\":\"zeta\"}" },
        },
        .graph_writes = &.{
            .{ .index_name = "graph_v1", .source = "doc:z", .target = "doc:y", .edge_type = "cites", .weight = 1.0 },
        },
        .sync_level = .full_index,
    });

    try db.createShadowIndexManager("doc:m", "");
    try std.testing.expect(db.shadow != null);
    try std.testing.expect(db.getShadowIndexDir().len > 0);
    try std.testing.expectEqual(@as(u32, 1), db.shadow.?.manager.textIndex("ft_v1").?.snapshot().global_doc_count);

    try db.core.shard_manager.prepareSplit("doc:m");
    try db.core.shard_manager.split(0, "doc:m");
    db.core.index_manager.updateRange(db.core.shard_manager.getByteRange());

    try db.batch(.{
        .writes = &.{.{ .key = "doc:b", .value = "{\"title\":\"bravo\"}" }},
        .sync_level = .full_index,
    });

    try std.testing.expectEqual(@as(u32, 1), db.shadow.?.manager.textIndex("ft_v1").?.snapshot().global_doc_count);

    try db.closeShadowIndexManager();
    try std.testing.expectEqualStrings("", db.getShadowIndexDir());
}

test "db split prepare and finalize produce destination shard and trim parent range" {
    const alloc = std.testing.allocator;

    var path_buf: [256]u8 = undefined;
    const path = TestHelpers.tempPath(&path_buf);
    defer TestHelpers.cleanupTempDir(path);

    var dest_buf: [256]u8 = undefined;
    const dest = TestHelpers.tempPath(&dest_buf);
    defer TestHelpers.cleanupTempDir(dest);

    const identity_namespace = doc_identity.Namespace{ .table_id = 71, .shard_id = 101, .range_id = 1001 };
    var db = try DB.open(alloc, std.mem.span(path), .{
        .identity_namespace = identity_namespace,
    });
    defer db.close();

    try db.addIndex(.{
        .name = "ft_v1",
        .kind = .full_text,
        .config_json = "{\"field\":\"title\"}",
    });
    try db.addIndex(.{
        .name = "sp_v1",
        .kind = .sparse_vector,
        .config_json = "{\"field\":\"sparse\"}",
    });
    try db.addIndex(.{
        .name = "graph_v1",
        .kind = .graph,
        .config_json = "{}",
    });

    try db.batch(.{
        .writes = &.{
            .{ .key = "doc:a", .value = "{\"title\":\"alpha\",\"sparse\":{\"indices\":[1],\"values\":[1.0]}}" },
            .{ .key = "doc:z", .value = "{\"title\":\"zeta\",\"sparse\":{\"indices\":[2],\"values\":[1.0]}}" },
        },
        .graph_writes = &.{
            .{ .index_name = "graph_v1", .source = "doc:z", .target = "doc:y", .edge_type = "cites", .weight = 1.0 },
        },
        .sync_level = .full_index,
    });
    try db.split(db.getRange(), "doc:m", "", std.mem.span(dest), true);

    var split_db = try DB.open(alloc, std.mem.span(dest), .{});
    defer split_db.close();
    try std.testing.expectEqualStrings("doc:m", split_db.getRange().start);
    try std.testing.expect((try split_db.getSplitState(alloc)) == null);
    {
        const split_stats = try split_db.diagnosticStats(alloc);
        defer types.freeDBStats(alloc, split_stats);
        try std.testing.expectEqual(identity_namespace.table_id, split_stats.doc_identity.namespace_table_id);
        try std.testing.expectEqual(identity_namespace.shard_id, split_stats.doc_identity.namespace_shard_id);
        try std.testing.expectEqual(identity_namespace.range_id, split_stats.doc_identity.namespace_range_id);
        try std.testing.expectEqual(@as(u32, 3), split_stats.doc_identity.next_ordinal);
        try std.testing.expectEqual(@as(u64, 2), split_stats.doc_identity.state_rows);
        try std.testing.expectEqual(@as(u64, 1), split_stats.doc_identity.scanned_primary_docs);
        try std.testing.expectEqual(@as(u64, 0), split_stats.doc_identity.primary_docs_missing_ordinals);
        try std.testing.expectEqual(@as(u64, 0), split_stats.doc_identity.primary_docs_missing_identity_state);
    }

    const split_doc = (try split_db.get(alloc, "doc:z")) orelse return error.TestExpectedEqual;
    defer alloc.free(split_doc);
    try std.testing.expectEqualStrings("{\"title\":\"zeta\"}", split_doc);

    var split_result = try split_db.search(alloc, .{
        .index_name = "ft_v1",
        .full_text = .{ .match = .{ .field = "title", .text = "zeta" } },
    });
    defer split_result.deinit();
    try std.testing.expectEqual(@as(u32, 1), split_result.total_hits);

    var split_sparse = try split_db.search(alloc, .{
        .index_name = "sp_v1",
        .query = .{ .sparse_knn = .{
            .indices = &.{2},
            .values = &.{1.0},
            .k = 2,
        } },
        .limit = 2,
    });
    defer split_sparse.deinit();
    try std.testing.expectEqual(@as(u32, 1), split_sparse.total_hits);
    try std.testing.expectEqualStrings("doc:z", split_sparse.hits[0].id);
    try std.testing.expectEqual(@as(?doc_set.DocOrdinal, 2), split_sparse.hits[0].doc_ordinal);

    var split_filtered = try split_db.search(alloc, .{
        .query = .{ .match_all = {} },
        .filter_doc_ids = &.{"doc:z"},
        .filter_doc_ids_positive = true,
        .limit = 10,
    });
    defer split_filtered.deinit();
    try std.testing.expectEqual(@as(u32, 1), split_filtered.total_hits);
    try std.testing.expectEqualStrings("doc:z", split_filtered.hits[0].id);
    try std.testing.expectEqual(@as(?doc_set.DocOrdinal, 2), split_filtered.hits[0].doc_ordinal);

    var split_filtered_left_doc = try split_db.search(alloc, .{
        .query = .{ .match_all = {} },
        .filter_doc_ids = &.{"doc:a"},
        .filter_doc_ids_positive = true,
        .limit = 10,
    });
    defer split_filtered_left_doc.deinit();
    try std.testing.expectEqual(@as(u32, 0), split_filtered_left_doc.total_hits);

    const split_incoming = try split_db.getEdges(alloc, "graph_v1", "doc:y", "cites", .in);
    defer graph_mod.GraphIndex.freeEdges(alloc, split_incoming);
    try std.testing.expectEqual(@as(usize, 1), split_incoming.len);
    try std.testing.expectEqualStrings("doc:z", split_incoming[0].source);

    try db.finalizeSplit(.{ .start = "", .end = "doc:m" });
    try std.testing.expectEqualStrings("doc:m", db.getRange().end);
    try std.testing.expect((try db.get(alloc, "doc:z")) == null);
    {
        const parent_stats = try db.diagnosticStats(alloc);
        defer types.freeDBStats(alloc, parent_stats);
        try std.testing.expectEqual(identity_namespace.table_id, parent_stats.doc_identity.namespace_table_id);
        try std.testing.expectEqual(identity_namespace.shard_id, parent_stats.doc_identity.namespace_shard_id);
        try std.testing.expectEqual(identity_namespace.range_id, parent_stats.doc_identity.namespace_range_id);
        try std.testing.expectEqual(@as(u32, 3), parent_stats.doc_identity.next_ordinal);
        try std.testing.expectEqual(@as(u64, 2), parent_stats.doc_identity.state_rows);
        try std.testing.expectEqual(@as(u64, 1), parent_stats.doc_identity.scanned_primary_docs);
        try std.testing.expectEqual(@as(u64, 0), parent_stats.doc_identity.primary_docs_missing_ordinals);
        try std.testing.expectEqual(@as(u64, 0), parent_stats.doc_identity.primary_docs_missing_identity_state);
    }

    var result = try db.search(alloc, .{
        .index_name = "ft_v1",
        .full_text = .{ .match = .{ .field = "title", .text = "alpha" } },
    });
    defer result.deinit();
    try std.testing.expectEqual(@as(u32, 1), result.total_hits);

    var parent_sparse = try db.search(alloc, .{
        .index_name = "sp_v1",
        .query = .{ .sparse_knn = .{
            .indices = &.{1},
            .values = &.{1.0},
            .k = 2,
        } },
        .limit = 2,
    });
    defer parent_sparse.deinit();
    try std.testing.expectEqual(@as(u32, 1), parent_sparse.total_hits);
    try std.testing.expectEqualStrings("doc:a", parent_sparse.hits[0].id);
    try std.testing.expectEqual(@as(?doc_set.DocOrdinal, 1), parent_sparse.hits[0].doc_ordinal);

    var parent_filtered = try db.search(alloc, .{
        .query = .{ .match_all = {} },
        .filter_doc_ids = &.{"doc:a"},
        .filter_doc_ids_positive = true,
        .limit = 10,
    });
    defer parent_filtered.deinit();
    try std.testing.expectEqual(@as(u32, 1), parent_filtered.total_hits);
    try std.testing.expectEqualStrings("doc:a", parent_filtered.hits[0].id);
    try std.testing.expectEqual(@as(?doc_set.DocOrdinal, 1), parent_filtered.hits[0].doc_ordinal);

    var removed = try db.search(alloc, .{
        .index_name = "ft_v1",
        .full_text = .{ .match = .{ .field = "title", .text = "zeta" } },
    });
    defer removed.deinit();
    try std.testing.expectEqual(@as(u32, 0), removed.total_hits);

    var removed_sparse = try db.search(alloc, .{
        .index_name = "sp_v1",
        .query = .{ .sparse_knn = .{
            .indices = &.{2},
            .values = &.{1.0},
            .k = 2,
        } },
        .limit = 2,
    });
    defer removed_sparse.deinit();
    try std.testing.expectEqual(@as(u32, 0), removed_sparse.total_hits);

    const parent_incoming = try db.getEdges(alloc, "graph_v1", "doc:y", "cites", .in);
    defer graph_mod.GraphIndex.freeEdges(alloc, parent_incoming);
    try std.testing.expectEqual(@as(usize, 0), parent_incoming.len);
}

test "db split moves relational rows and column entries" {
    const alloc = std.testing.allocator;

    var path_buf: [256]u8 = undefined;
    const path = TestHelpers.tempPath(&path_buf);
    defer TestHelpers.cleanupTempDir(path);

    var dest_buf: [256]u8 = undefined;
    const dest = TestHelpers.tempPath(&dest_buf);
    defer TestHelpers.cleanupTempDir(dest);

    var db = try DB.open(alloc, std.mem.span(path), .{});
    defer db.close();

    const schema_json =
        \\{"version":1,"storage_mode":"relational","default_type":"row","enforce_types":true,"document_schemas":{"row":{"schema":{"type":"object","properties":{"id":{"type":"keyword"},"title":{"type":"text"},"status":{"type":"keyword"},"amount":{"type":"numeric"}},"required":["id","title"],"additionalProperties":false}}},"primary_key":{"columns":["id"]},"relational_indexes":[{"name":"status_amount_idx","owner_kind":"relational_column","owner_name":"status","access_method":"ordered_tuple","columns":["status"],"keys":[{"column":"status"},{"column":"amount"}],"lifecycle":"ready","generation":1,"schema_fingerprint":"secondary-index-v1:status_amount_idx","generation_record":{"generation":1,"owner_ranges":[],"lifecycle":"ready","lag":0,"ready_watermark":0}},{"name":"amount","owner_kind":"relational_column","owner_name":"amount","access_method":"scalar_column","columns":["amount"]}]}
    ;
    var parsed_schema = try schema_api_mod.parseValidatedTableSchema(alloc, schema_json);
    defer parsed_schema.deinit(alloc);
    const runtime_schema = try schema_api_mod.deriveRuntimeTableSchema(alloc, parsed_schema);
    defer schema_mod.freeSchema(alloc, runtime_schema);
    try db.setSchema(runtime_schema);

    try db.addIndex(.{
        .name = "ft_v1",
        .kind = .full_text,
        .config_json = "{\"field\":\"title\"}",
    });
    try db.batch(.{
        .writes = &.{
            .{ .key = "row:a", .value = "{\"id\":\"a\",\"title\":\"alpha\",\"status\":\"open\",\"amount\":10}" },
            .{ .key = "row:z", .value = "{\"id\":\"z\",\"title\":\"zeta\",\"status\":\"closed\",\"amount\":90}" },
        },
        .sync_level = .full_index,
    });
    try expectSingleOrderedTupleDocKey(alloc, db.core.store, runtime_schema, "row:a");
    try expectSingleOrderedTupleDocKey(alloc, db.core.store, runtime_schema, "row:z");
    const source_row_z_tuple = try orderedTupleValueForDocKeyAlloc(alloc, db.core.store, runtime_schema, "row:z");
    defer alloc.free(source_row_z_tuple);

    try db.split(db.getRange(), "row:m", "", std.mem.span(dest), true);

    var split_db = try DB.open(alloc, std.mem.span(dest), .{});
    defer split_db.close();
    try std.testing.expectEqualStrings("row:m", split_db.getRange().start);

    const split_doc = (try split_db.get(alloc, "row:z")) orelse return error.TestExpectedEqual;
    defer alloc.free(split_doc);
    try std.testing.expect(std.mem.indexOf(u8, split_doc, "\"title\":\"zeta\"") != null);
    try std.testing.expect(std.mem.indexOf(u8, split_doc, "\"status\":\"closed\"") != null);
    try std.testing.expect(std.mem.indexOf(u8, split_doc, "\"amount\":90") != null);

    const split_amounts = try relational_store_mod.scanColumnAlloc(alloc, split_db.core.store, "amount", "row:z", "row:z");
    defer relational_store_mod.freeColumnValues(alloc, split_amounts);
    try std.testing.expectEqual(@as(usize, 1), split_amounts.len);
    try std.testing.expectEqualStrings("row:z", split_amounts[0].doc_key);
    try std.testing.expectEqual(.f64_val, split_amounts[0].value_type);
    try std.testing.expectEqual(@as(f64, 90), split_amounts[0].value.f64_val);
    try expectSingleOrderedTupleDocKey(alloc, split_db.core.store, runtime_schema, "row:z");

    var split_filtered = try split_db.search(alloc, .{
        .index_name = "ft_v1",
        .query = .{ .match = .{ .field = "title", .text = "zeta" } },
        .filter_query_json = "{\"term\":{\"field\":\"status\",\"term\":\"closed\"}}",
        .limit = 10,
    });
    defer split_filtered.deinit();
    try std.testing.expectEqual(@as(u32, 1), split_filtered.total_hits);
    try std.testing.expectEqualStrings("row:z", split_filtered.hits[0].id);

    try db.finalizeSplit(.{ .start = "", .end = "row:m" });
    try std.testing.expect((try db.get(alloc, "row:z")) == null);

    const parent_left_amounts = try relational_store_mod.scanColumnAlloc(alloc, db.core.store, "amount", "row:a", "row:a");
    defer relational_store_mod.freeColumnValues(alloc, parent_left_amounts);
    try std.testing.expectEqual(@as(usize, 1), parent_left_amounts.len);
    try std.testing.expectEqualStrings("row:a", parent_left_amounts[0].doc_key);
    try std.testing.expectEqual(.f64_val, parent_left_amounts[0].value_type);
    try std.testing.expectEqual(@as(f64, 10), parent_left_amounts[0].value.f64_val);
    try expectSingleOrderedTupleDocKey(alloc, db.core.store, runtime_schema, "row:a");

    var parent_filtered = try db.search(alloc, .{
        .index_name = "ft_v1",
        .query = .{ .match = .{ .field = "title", .text = "alpha" } },
        .filter_query_json = "{\"term\":{\"field\":\"status\",\"term\":\"open\"}}",
        .limit = 10,
    });
    defer parent_filtered.deinit();
    try std.testing.expectEqual(@as(u32, 1), parent_filtered.total_hits);
    try std.testing.expectEqualStrings("row:a", parent_filtered.hits[0].id);

    const parent_amounts = try relational_store_mod.scanColumnAlloc(alloc, db.core.store, "amount", "row:z", "row:z");
    defer relational_store_mod.freeColumnValues(alloc, parent_amounts);
    try std.testing.expectEqual(@as(usize, 0), parent_amounts.len);
    try expectNoOrderedTupleDocKeysForTuple(alloc, db.core.store, source_row_z_tuple, "row:z", "row:z");
    try expectNoOrderedTupleEntriesForDocKey(alloc, db.core.store, "row:z");
}

test "db split prepare and finalize work with durable lsm primary backend" {
    const alloc = std.testing.allocator;

    var path_buf: [256]u8 = undefined;
    const path = TestHelpers.tempPath(&path_buf);
    defer TestHelpers.cleanupTempDir(path);

    var dest_buf: [256]u8 = undefined;
    const dest = TestHelpers.tempPath(&dest_buf);
    defer TestHelpers.cleanupTempDir(dest);

    const primary_backend: PrimaryBackend = .{ .lsm = .{ .flush_threshold = 1 } };

    var db = try DB.open(alloc, std.mem.span(path), .{
        .primary_backend = primary_backend,
    });
    defer db.close();

    try db.addIndex(.{
        .name = "ft_v1",
        .kind = .full_text,
        .config_json = "{\"field\":\"title\"}",
    });

    try db.batch(.{
        .writes = &.{
            .{ .key = "doc:a", .value = "{\"title\":\"alpha\"}" },
            .{ .key = "doc:z", .value = "{\"title\":\"zeta\"}" },
        },
    });

    try db.split(db.getRange(), "doc:m", "", std.mem.span(dest), true);

    var split_db = try DB.open(alloc, std.mem.span(dest), .{
        .primary_backend = primary_backend,
    });
    defer split_db.close();
    try std.testing.expectEqualStrings("doc:m", split_db.getRange().start);
    {
        const split_stats = try split_db.diagnosticStats(alloc);
        defer types.freeDBStats(alloc, split_stats);
        try std.testing.expectEqual(@as(u32, 3), split_stats.doc_identity.next_ordinal);
        try std.testing.expectEqual(@as(u64, 2), split_stats.doc_identity.state_rows);
        try std.testing.expectEqual(@as(u64, 1), split_stats.doc_identity.scanned_primary_docs);
        try std.testing.expectEqual(@as(u64, 0), split_stats.doc_identity.primary_docs_missing_ordinals);
    }

    const split_doc = (try split_db.get(alloc, "doc:z")) orelse return error.TestExpectedEqual;
    defer alloc.free(split_doc);
    try std.testing.expectEqualStrings("{\"title\":\"zeta\"}", split_doc);

    var split_result = try split_db.search(alloc, .{
        .index_name = "ft_v1",
        .full_text = .{ .match = .{ .field = "title", .text = "zeta" } },
    });
    defer split_result.deinit();
    try std.testing.expectEqual(@as(u32, 1), split_result.total_hits);

    try db.finalizeSplit(.{ .start = "", .end = "doc:m" });
    try std.testing.expectEqualStrings("doc:m", db.getRange().end);
    try std.testing.expect((try db.get(alloc, "doc:z")) == null);
    {
        const parent_stats = try db.diagnosticStats(alloc);
        defer types.freeDBStats(alloc, parent_stats);
        try std.testing.expectEqual(@as(u32, 3), parent_stats.doc_identity.next_ordinal);
        try std.testing.expectEqual(@as(u64, 2), parent_stats.doc_identity.state_rows);
        try std.testing.expectEqual(@as(u64, 1), parent_stats.doc_identity.scanned_primary_docs);
        try std.testing.expectEqual(@as(u64, 0), parent_stats.doc_identity.primary_docs_missing_ordinals);
    }

    var removed = try db.search(alloc, .{
        .index_name = "ft_v1",
        .full_text = .{ .match = .{ .field = "title", .text = "zeta" } },
    });
    defer removed.deinit();
    try std.testing.expectEqual(@as(u32, 0), removed.total_hits);
}

test "db split prepare survives reopen and finalizes with durable lsm primary backend" {
    const alloc = std.testing.allocator;

    var parent_buf: [256]u8 = undefined;
    const parent_path = TestHelpers.tempPath(&parent_buf);
    defer TestHelpers.cleanupTempDir(parent_path);

    var child_buf: [256]u8 = undefined;
    const child_path = TestHelpers.tempPath(&child_buf);
    defer TestHelpers.cleanupTempDir(child_path);

    const primary_backend: PrimaryBackend = .{ .lsm = .{ .flush_threshold = 1 } };

    {
        var parent = try DB.open(alloc, std.mem.span(parent_path), .{
            .primary_backend = primary_backend,
        });
        defer parent.close();

        try parent.addIndex(.{
            .name = "ft_v1",
            .kind = .full_text,
            .config_json = "{\"field\":\"title\"}",
        });

        try parent.batch(.{
            .writes = &.{
                .{ .key = "doc:a", .value = "{\"title\":\"alpha\"}" },
                .{ .key = "doc:z", .value = "{\"title\":\"zeta\"}" },
            },
        });

        try parent.split(parent.getRange(), "doc:m", "", std.mem.span(child_path), true);
        try parent.sync(true);
    }

    var reopened_parent = try DB.open(alloc, std.mem.span(parent_path), .{
        .primary_backend = primary_backend,
    });
    defer reopened_parent.close();

    const split_state = (try reopened_parent.getSplitState(std.testing.allocator)) orelse return error.TestExpectedEqual;
    defer types.freeSplitState(std.testing.allocator, split_state);
    try std.testing.expectEqualStrings("doc:m", split_state.split_key);

    var child = try DB.open(alloc, std.mem.span(child_path), .{
        .primary_backend = primary_backend,
    });
    defer child.close();

    const split_doc = (try child.get(alloc, "doc:z")) orelse return error.TestExpectedEqual;
    defer alloc.free(split_doc);
    try std.testing.expectEqualStrings("{\"title\":\"zeta\"}", split_doc);

    try reopened_parent.finalizeSplit(.{ .start = "", .end = "doc:m" });
    try std.testing.expect((try reopened_parent.get(alloc, "doc:z")) == null);
}

test "db split prepare survives reopen and finalizes text sparse and graph indexes with durable lsm primary backend" {
    const alloc = std.testing.allocator;

    var parent_buf: [256]u8 = undefined;
    const parent_path = TestHelpers.tempPath(&parent_buf);
    defer TestHelpers.cleanupTempDir(parent_path);

    var child_buf: [256]u8 = undefined;
    const child_path = TestHelpers.tempPath(&child_buf);
    defer TestHelpers.cleanupTempDir(child_path);

    const primary_backend: PrimaryBackend = .{ .lsm = db_config.primary_lsm_options_default };

    {
        var parent = try DB.open(alloc, std.mem.span(parent_path), .{
            .primary_backend = primary_backend,
        });
        defer parent.close();

        const index_cfgs = [_]types.IndexConfig{
            .{
                .name = "ft_v1",
                .kind = .full_text,
                .config_json = "{\"field\":\"title\"}",
            },
            .{
                .name = "sp_v1",
                .kind = .sparse_vector,
                .config_json = "{\"field\":\"sparse\"}",
            },
            .{
                .name = "graph_v1",
                .kind = .graph,
                .config_json = "{}",
            },
        };
        for (index_cfgs) |cfg| try parent.addIndex(cfg);

        try parent.batch(.{
            .writes = &.{
                .{ .key = "doc:a", .value = "{\"title\":\"alpha\",\"sparse\":{\"indices\":[1],\"values\":[1.0]}}" },
                .{ .key = "doc:z", .value = "{\"title\":\"zeta\",\"sparse\":{\"indices\":[2],\"values\":[1.0]}}" },
            },
            .graph_writes = &.{
                .{ .index_name = "graph_v1", .source = "doc:z", .target = "doc:y", .edge_type = "cites", .weight = 1.0 },
            },
            .sync_level = .full_index,
        });

        try parent.split(parent.getRange(), "doc:m", "", std.mem.span(child_path), true);
        try parent.sync(true);
    }

    var reopened_parent = try DB.open(alloc, std.mem.span(parent_path), .{
        .primary_backend = primary_backend,
    });
    defer reopened_parent.close();

    const split_state = (try reopened_parent.getSplitState(alloc)) orelse return error.TestExpectedEqual;
    defer types.freeSplitState(alloc, split_state);
    try std.testing.expectEqualStrings("doc:m", split_state.split_key);

    var child = try DB.open(alloc, std.mem.span(child_path), .{
        .primary_backend = primary_backend,
    });
    defer child.close();

    const split_doc = (try child.get(alloc, "doc:z")) orelse return error.TestExpectedEqual;
    defer alloc.free(split_doc);
    try std.testing.expectEqualStrings("{\"title\":\"zeta\"}", split_doc);

    var child_text = try child.search(alloc, .{
        .index_name = "ft_v1",
        .full_text = .{ .match = .{ .field = "title", .text = "zeta" } },
    });
    defer child_text.deinit();
    try std.testing.expectEqual(@as(u32, 1), child_text.total_hits);
    try std.testing.expectEqualStrings("doc:z", child_text.hits[0].id);

    var child_sparse = try child.search(alloc, .{
        .index_name = "sp_v1",
        .query = .{ .sparse_knn = .{
            .indices = &.{2},
            .values = &.{1.0},
            .k = 2,
        } },
        .limit = 2,
    });
    defer child_sparse.deinit();
    try std.testing.expectEqual(@as(u32, 1), child_sparse.total_hits);
    try std.testing.expectEqualStrings("doc:z", child_sparse.hits[0].id);

    const child_incoming = try child.getEdges(alloc, "graph_v1", "doc:y", "cites", .in);
    defer graph_mod.GraphIndex.freeEdges(alloc, child_incoming);
    try std.testing.expectEqual(@as(usize, 1), child_incoming.len);
    try std.testing.expectEqualStrings("doc:z", child_incoming[0].source);

    try reopened_parent.finalizeSplit(.{ .start = "", .end = "doc:m" });
    try std.testing.expect((try reopened_parent.get(alloc, "doc:z")) == null);

    var parent_text = try reopened_parent.search(alloc, .{
        .index_name = "ft_v1",
        .full_text = .{ .match = .{ .field = "title", .text = "alpha" } },
    });
    defer parent_text.deinit();
    try std.testing.expectEqual(@as(u32, 1), parent_text.total_hits);
    try std.testing.expectEqualStrings("doc:a", parent_text.hits[0].id);

    var removed_text = try reopened_parent.search(alloc, .{
        .index_name = "ft_v1",
        .full_text = .{ .match = .{ .field = "title", .text = "zeta" } },
    });
    defer removed_text.deinit();
    try std.testing.expectEqual(@as(u32, 0), removed_text.total_hits);

    var parent_sparse = try reopened_parent.search(alloc, .{
        .index_name = "sp_v1",
        .query = .{ .sparse_knn = .{
            .indices = &.{1},
            .values = &.{1.0},
            .k = 2,
        } },
        .limit = 2,
    });
    defer parent_sparse.deinit();
    try std.testing.expectEqual(@as(u32, 1), parent_sparse.total_hits);
    try std.testing.expectEqualStrings("doc:a", parent_sparse.hits[0].id);

    var removed_sparse = try reopened_parent.search(alloc, .{
        .index_name = "sp_v1",
        .query = .{ .sparse_knn = .{
            .indices = &.{2},
            .values = &.{1.0},
            .k = 2,
        } },
        .limit = 2,
    });
    defer removed_sparse.deinit();
    try std.testing.expectEqual(@as(u32, 0), removed_sparse.total_hits);

    const parent_incoming = try reopened_parent.getEdges(alloc, "graph_v1", "doc:y", "cites", .in);
    defer graph_mod.GraphIndex.freeEdges(alloc, parent_incoming);
    try std.testing.expectEqual(@as(usize, 0), parent_incoming.len);
}

test "db restore snapshot recreates logical store for durable lsm primary backend" {
    const alloc = std.testing.allocator;

    var src_buf: [256]u8 = undefined;
    const src_path = TestHelpers.tempPath(&src_buf);
    defer TestHelpers.cleanupTempDir(src_path);

    var restore_buf: [256]u8 = undefined;
    const restore_path = TestHelpers.tempPath(&restore_buf);
    defer TestHelpers.cleanupTempDir(restore_path);

    const primary_backend: PrimaryBackend = .{ .lsm = .{ .flush_threshold = 1 } };

    {
        var db = try DB.open(alloc, std.mem.span(src_path), .{
            .primary_backend = primary_backend,
        });
        defer db.close();

        try db.addIndex(.{
            .name = "ft_v1",
            .kind = .full_text,
            .config_json = "{\"field\":\"title\"}",
        });
        try db.batch(.{
            .writes = &.{.{ .key = "doc:snap", .value = "{\"title\":\"snap\"}" }},
        });
        _ = try db.snapshot("snap1");
    }

    const snapshot_root = try std.fmt.allocPrint(alloc, "{s}.snapshots/snap1", .{std.mem.span(src_path)});
    defer alloc.free(snapshot_root);
    defer TestHelpers.cleanupSnapshotDirForPath(src_path);

    try DB.restoreSnapshotTo(alloc, snapshot_root, std.mem.span(restore_path), .{
        .primary_backend = primary_backend,
    });

    var restored = try DB.open(alloc, std.mem.span(restore_path), .{
        .primary_backend = primary_backend,
    });
    defer restored.close();

    const snap_doc = (try restored.get(alloc, "doc:snap")) orelse return error.TestExpectedEqual;
    defer alloc.free(snap_doc);
    try std.testing.expectEqualStrings("{\"title\":\"snap\"}", snap_doc);

    var result = try restored.search(alloc, .{
        .index_name = "ft_v1",
        .full_text = .{ .match = .{ .field = "title", .text = "snap" } },
    });
    defer result.deinit();
    try std.testing.expectEqual(@as(u32, 1), result.total_hits);
}

test "db split restore doc identity snapshot rejects invalid metadata" {
    const alloc = std.testing.allocator;

    var src_buf: [256]u8 = undefined;
    const src_path = TestHelpers.tempPath(&src_buf);
    defer TestHelpers.cleanupTempDir(src_path);

    var restore_buf: [256]u8 = undefined;
    const restore_path = TestHelpers.tempPath(&restore_buf);
    defer TestHelpers.cleanupTempDir(restore_path);

    const primary_backend: PrimaryBackend = .{ .lsm = .{ .flush_threshold = 1 } };

    {
        var db = try DB.open(alloc, std.mem.span(src_path), .{
            .primary_backend = primary_backend,
        });
        defer db.close();

        try db.batch(.{
            .writes = &.{.{ .key = "doc:bad-identity", .value = "{\"title\":\"bad\"}" }},
        });

        const state_key = internal_keys.identityOrdinalStateKey(1);
        var corrupt_state: [25]u8 = undefined;
        std.mem.writeInt(u64, corrupt_state[0..8], 0xdead_beef, .big);
        std.mem.writeInt(u64, corrupt_state[8..16], 1, .big);
        corrupt_state[16] = 0;
        @memset(corrupt_state[17..25], 0);
        try db.core.store.putBatch(&.{.{ .key = state_key[0..], .value = corrupt_state[0..] }}, &.{});

        _ = try db.snapshot("snap1");
    }

    const snapshot_root = try std.fmt.allocPrint(alloc, "{s}.snapshots/snap1", .{std.mem.span(src_path)});
    defer alloc.free(snapshot_root);
    defer TestHelpers.cleanupSnapshotDirForPath(src_path);

    try std.testing.expectError(error.InvalidDocIdentity, DB.restoreSnapshotTo(alloc, snapshot_root, std.mem.span(restore_path), .{
        .primary_backend = primary_backend,
    }));
}

test "db split restore doc identity deferred restore rejects strict namespace mismatch" {
    const alloc = std.testing.allocator;

    var src_buf: [256]u8 = undefined;
    const src_path = TestHelpers.tempPath(&src_buf);
    defer TestHelpers.cleanupTempDir(src_path);

    var restore_buf: [256]u8 = undefined;
    const restore_path = TestHelpers.tempPath(&restore_buf);
    defer TestHelpers.cleanupTempDir(restore_path);

    const primary_backend: PrimaryBackend = .{ .lsm = .{ .flush_threshold = 1 } };
    const source_namespace = doc_identity.Namespace{
        .table_id = 10,
        .shard_id = 11,
        .range_id = 12,
    };
    const target_namespace = doc_identity.Namespace{
        .table_id = 10,
        .shard_id = 99,
        .range_id = 100,
    };

    {
        var db = try DB.open(alloc, std.mem.span(src_path), .{
            .primary_backend = primary_backend,
            .identity_namespace = source_namespace,
        });
        defer db.close();

        try db.batch(.{
            .writes = &.{.{ .key = "doc:restore", .value = "{\"title\":\"restore\"}" }},
        });
        _ = try db.snapshot("snap1");
    }

    const snapshot_root = try std.fmt.allocPrint(alloc, "{s}.snapshots/snap1", .{std.mem.span(src_path)});
    defer alloc.free(snapshot_root);
    defer TestHelpers.cleanupSnapshotDirForPath(src_path);

    try std.testing.expectError(error.IdentityNamespaceMismatch, DB.restoreSnapshotToDeferredRuntimeRepair(
        alloc,
        snapshot_root,
        std.mem.span(restore_path),
        .{
            .primary_backend = primary_backend,
            .identity_namespace = target_namespace,
        },
        .{
            .backup_id = "backup-a",
            .location = "local",
            .snapshot_path = "snap1",
            .group_id = 99,
        },
    ));
}

test "db restore snapshot recreates text sparse and graph indexes for durable lsm primary backend" {
    const alloc = std.testing.allocator;

    var src_buf: [256]u8 = undefined;
    const src_path = TestHelpers.tempPath(&src_buf);
    defer TestHelpers.cleanupTempDir(src_path);

    var restore_buf: [256]u8 = undefined;
    const restore_path = TestHelpers.tempPath(&restore_buf);
    defer TestHelpers.cleanupTempDir(restore_path);

    const primary_backend: PrimaryBackend = .{ .lsm = .{ .flush_threshold = 1 } };

    {
        var db = try DB.open(alloc, std.mem.span(src_path), .{
            .primary_backend = primary_backend,
        });
        defer db.close();

        const index_cfgs = [_]types.IndexConfig{
            .{
                .name = "ft_v1",
                .kind = .full_text,
                .config_json = "{\"field\":\"title\"}",
            },
            .{
                .name = "sp_v1",
                .kind = .sparse_vector,
                .config_json = "{\"field\":\"sparse\"}",
            },
            .{
                .name = "graph_v1",
                .kind = .graph,
                .config_json = "{}",
            },
        };
        for (index_cfgs) |cfg| try db.addIndex(cfg);

        try db.batch(.{
            .writes = &.{
                .{ .key = "doc:a", .value = "{\"title\":\"alpha\",\"sparse\":{\"indices\":[1],\"values\":[1.0]}}" },
                .{ .key = "doc:z", .value = "{\"title\":\"zeta\",\"sparse\":{\"indices\":[2],\"values\":[1.0]}}" },
            },
            .graph_writes = &.{
                .{ .index_name = "graph_v1", .source = "doc:z", .target = "doc:y", .edge_type = "cites", .weight = 1.0 },
            },
        });
        _ = try db.snapshot("snap1");
    }

    const snapshot_root = try std.fmt.allocPrint(alloc, "{s}.snapshots/snap1", .{std.mem.span(src_path)});
    defer alloc.free(snapshot_root);
    defer TestHelpers.cleanupSnapshotDirForPath(src_path);

    try DB.restoreSnapshotTo(alloc, snapshot_root, std.mem.span(restore_path), .{
        .primary_backend = primary_backend,
    });

    var restored = try DB.open(alloc, std.mem.span(restore_path), .{
        .primary_backend = primary_backend,
    });
    defer restored.close();

    var text = try restored.search(alloc, .{
        .index_name = "ft_v1",
        .full_text = .{ .match = .{ .field = "title", .text = "zeta" } },
    });
    defer text.deinit();
    try std.testing.expectEqual(@as(u32, 1), text.total_hits);
    try std.testing.expectEqualStrings("doc:z", text.hits[0].id);

    var sparse = try restored.search(alloc, .{
        .index_name = "sp_v1",
        .query = .{ .sparse_knn = .{
            .indices = &.{2},
            .values = &.{1.0},
            .k = 2,
        } },
        .limit = 2,
    });
    defer sparse.deinit();
    try std.testing.expectEqual(@as(u32, 1), sparse.total_hits);
    try std.testing.expectEqualStrings("doc:z", sparse.hits[0].id);

    const incoming = try restored.getEdges(alloc, "graph_v1", "doc:y", "cites", .in);
    defer graph_mod.GraphIndex.freeEdges(alloc, incoming);
    try std.testing.expectEqual(@as(usize, 1), incoming.len);
    try std.testing.expectEqualStrings("doc:z", incoming[0].source);
}

test "db deferred restore runtime repair rebuilds graph indexes from artifacts" {
    const alloc = std.testing.allocator;

    var src_buf: [256]u8 = undefined;
    const src_path = TestHelpers.tempPath(&src_buf);
    defer TestHelpers.cleanupTempDir(src_path);

    var restore_buf: [256]u8 = undefined;
    const restore_path = TestHelpers.tempPath(&restore_buf);
    defer TestHelpers.cleanupTempDir(restore_path);

    const primary_backend: PrimaryBackend = .{ .lsm = .{ .flush_threshold = 1 } };

    {
        var db = try DB.open(alloc, std.mem.span(src_path), .{
            .primary_backend = primary_backend,
        });
        defer db.close();

        try db.addIndex(.{
            .name = "graph_v1",
            .kind = .graph,
            .config_json = "{}",
        });

        try db.batch(.{
            .writes = &.{
                .{ .key = "doc:a", .value = "{\"title\":\"alpha\",\"_edges\":{\"graph_v1\":{\"cites\":[{\"target\":\"doc:b\",\"weight\":1.0}]}}}" },
                .{ .key = "doc:b", .value = "{\"title\":\"beta\"}" },
            },
            .sync_level = .full_index,
        });
        _ = try db.snapshot("snap1");
    }

    const snapshot_root = try std.fmt.allocPrint(alloc, "{s}.snapshots/snap1", .{std.mem.span(src_path)});
    defer alloc.free(snapshot_root);
    defer TestHelpers.cleanupSnapshotDirForPath(src_path);

    try DB.restoreSnapshotToDeferredRuntimeRepair(alloc, snapshot_root, std.mem.span(restore_path), .{
        .primary_backend = primary_backend,
    }, .{
        .backup_id = "snap1",
        .location = "file:///tmp/backups",
        .snapshot_path = "snapshots/snap1",
        .group_id = 7001,
    });
    try std.testing.expect(try DB.restoreRuntimeRepairNeededForPath(alloc, std.mem.span(restore_path)));

    {
        var restored = try DB.open(alloc, std.mem.span(restore_path), .{
            .primary_backend = primary_backend,
        });
        defer restored.close();

        try std.testing.expect(try restored.repairRestoreRuntimeStateIfNeeded(alloc));

        const incoming = try restored.getEdges(alloc, "graph_v1", "doc:b", "cites", .in);
        defer graph_mod.GraphIndex.freeEdges(alloc, incoming);
        try std.testing.expectEqual(@as(usize, 1), incoming.len);
        try std.testing.expectEqualStrings("doc:a", incoming[0].source);
    }

    try std.testing.expect(!(try DB.restoreRuntimeRepairNeededForPath(alloc, std.mem.span(restore_path))));
}

test "db restore snapshot replays managed chunked dense embeddings for durable lsm primary backend" {
    const alloc = std.testing.allocator;

    var src_buf: [256]u8 = undefined;
    const src_path = TestHelpers.tempPath(&src_buf);
    defer TestHelpers.cleanupTempDir(src_path);

    var restore_buf: [256]u8 = undefined;
    const restore_path = TestHelpers.tempPath(&restore_buf);
    defer TestHelpers.cleanupTempDir(restore_path);

    const primary_backend: PrimaryBackend = .{ .lsm = .{ .flush_threshold = 1 } };

    {
        var deterministic = embedder_mod.DeterministicDenseEmbedder{};
        var db = try DB.open(alloc, std.mem.span(src_path), .{
            .primary_backend = primary_backend,
            .enrichment = .{
                .owner_id = "worker-a",
                .dense_embedder = deterministic.interface(),
            },
        });
        defer db.close();

        try db.addIndex(.{
            .name = "dv_v1",
            .kind = .dense_vector,
            .config_json = "{\"field\":\"embedding\",\"dims\":3,\"generator\":{\"kind\":\"dense_embedding\",\"source_field\":\"body\",\"chunk_name\":\"body_chunks_v1\",\"chunk_size\":8,\"chunk_overlap\":2}}",
        });

        try db.batch(.{
            .writes = &.{
                .{ .key = "doc:a", .value = "{\"body\":\"abcdefghijklmno\"}" },
            },
            .sync_level = .full_index,
        });

        const query_vec = try deterministic.interface().embedDense(alloc, "", "abcdefgh", 3);
        defer alloc.free(query_vec);
        var before = try waitForSearchResult(alloc, &db, .{
            .index_name = "dv_v1",
            .dense = .{ .vector = query_vec, .k = 3 },
            .return_mode = .parent,
        }, 1);
        defer before.deinit();
        try std.testing.expectEqualStrings("doc:a", before.hits[0].id);

        _ = try db.snapshot("snap1");
    }

    const snapshot_root = try std.fmt.allocPrint(alloc, "{s}.snapshots/snap1", .{std.mem.span(src_path)});
    defer alloc.free(snapshot_root);
    defer TestHelpers.cleanupSnapshotDirForPath(src_path);

    try DB.restoreSnapshotTo(alloc, snapshot_root, std.mem.span(restore_path), .{
        .primary_backend = primary_backend,
    });

    var deterministic = embedder_mod.DeterministicDenseEmbedder{};
    var restored = try DB.open(alloc, std.mem.span(restore_path), .{
        .primary_backend = primary_backend,
        .enrichment = .{
            .owner_id = "worker-a",
            .dense_embedder = deterministic.interface(),
        },
    });
    defer restored.close();

    const chunk_prefix = try internal_keys.artifactNamedPrefixAlloc(alloc, "doc:a", "chunk", "body_chunks_v1");
    defer alloc.free(chunk_prefix);
    const chunk_records = try restored.core.store.scanPrefix(alloc, chunk_prefix);
    defer docstore_mod.DocStore.freeResults(alloc, chunk_records);
    try std.testing.expect(chunk_records.len > 0);

    const rebuilt = try restored.rebuildDenseIndexesFromStoredEmbeddingArtifacts(alloc);
    try std.testing.expect(rebuilt > 0);
    _ = try restored.replayGeneratedEnrichmentsFromStoredDocs(alloc);
    try restored.runUntilIdle();
    const pending = restored.pendingWorkStats();
    try std.testing.expectEqual(pending.enrichment.target_sequence, pending.enrichment.applied_sequence);
    try std.testing.expect(pending.enrichment.applied_sequence >= 1);

    const query_vec = try deterministic.interface().embedDense(alloc, "", "abcdefgh", 3);
    defer alloc.free(query_vec);
    var after = try waitForSearchResult(alloc, &restored, .{
        .index_name = "dv_v1",
        .dense = .{ .vector = query_vec, .k = 3 },
        .return_mode = .parent,
    }, 1);
    defer after.deinit();
    try std.testing.expectEqualStrings("doc:a", after.hits[0].id);
}

test "db split restore doc identity runtime repair repairs managed chunked dense embeddings once for restored shard" {
    const alloc = std.testing.allocator;

    var src_buf: [256]u8 = undefined;
    const src_path = TestHelpers.tempPath(&src_buf);
    defer TestHelpers.cleanupTempDir(src_path);

    var restore_buf: [256]u8 = undefined;
    const restore_path = TestHelpers.tempPath(&restore_buf);
    defer TestHelpers.cleanupTempDir(restore_path);

    const primary_backend: PrimaryBackend = .{ .lsm = .{ .flush_threshold = 1 } };

    {
        var deterministic = embedder_mod.DeterministicDenseEmbedder{};
        var db = try DB.open(alloc, std.mem.span(src_path), .{
            .primary_backend = primary_backend,
            .enrichment = .{
                .owner_id = "worker-a",
                .dense_embedder = deterministic.interface(),
            },
        });
        defer db.close();

        try db.addIndex(.{
            .name = "dv_v1",
            .kind = .dense_vector,
            .config_json = "{\"field\":\"embedding\",\"dims\":3,\"generator\":{\"kind\":\"dense_embedding\",\"source_field\":\"body\",\"chunk_name\":\"body_chunks_v1\",\"chunk_size\":8,\"chunk_overlap\":2}}",
        });

        try db.batch(.{
            .writes = &.{
                .{ .key = "doc:a", .value = "{\"body\":\"abcdefghijklmno\"}" },
            },
            .sync_level = .full_index,
        });

        _ = try db.snapshot("snap1");
    }

    const snapshot_root = try std.fmt.allocPrint(alloc, "{s}.snapshots/snap1", .{std.mem.span(src_path)});
    defer alloc.free(snapshot_root);
    defer TestHelpers.cleanupSnapshotDirForPath(src_path);

    try DB.restoreSnapshotTo(alloc, snapshot_root, std.mem.span(restore_path), .{
        .primary_backend = primary_backend,
    });

    try DB.markRestorePrimaryRestoredForPath(
        alloc,
        std.mem.span(restore_path),
        "snap1",
        "file:///tmp/backups",
        "snapshots/snap1",
        7001,
    );
    try DB.markRestoreRuntimeRepairNeeded(alloc, std.mem.span(restore_path));
    try std.testing.expect(try DB.restoreRuntimeRepairNeededForPath(alloc, std.mem.span(restore_path)));

    {
        var deterministic = embedder_mod.DeterministicDenseEmbedder{};
        var restored = try DB.open(alloc, std.mem.span(restore_path), .{
            .primary_backend = primary_backend,
            .enrichment = .{
                .owner_id = "worker-a",
                .dense_embedder = deterministic.interface(),
            },
        });
        defer restored.close();

        try std.testing.expect(try restored.repairRestoreRuntimeStateIfNeeded(alloc));

        const query_vec = try deterministic.interface().embedDense(alloc, "", "abcdefgh", 3);
        defer alloc.free(query_vec);
        var after = try waitForSearchResult(alloc, &restored, .{
            .index_name = "dv_v1",
            .dense = .{ .vector = query_vec, .k = 3 },
            .return_mode = .parent,
        }, 1);
        defer after.deinit();
        try std.testing.expectEqualStrings("doc:a", after.hits[0].id);
    }

    const repair_marker_path = try split_restore.restoreRepairMarkerPathAlloc(alloc, std.mem.span(restore_path));
    defer alloc.free(repair_marker_path);
    try std.Io.Dir.cwd().access(std.testing.io, repair_marker_path, .{});
}

test "db split restore doc identity incomplete deferred restore import recovers before runtime repair" {
    const alloc = std.testing.allocator;

    var src_buf: [256]u8 = undefined;
    const src_path = TestHelpers.tempPath(&src_buf);
    defer TestHelpers.cleanupTempDir(src_path);

    var restore_buf: [256]u8 = undefined;
    const restore_path = TestHelpers.tempPath(&restore_buf);
    defer TestHelpers.cleanupTempDir(restore_path);

    const primary_backend: PrimaryBackend = .{ .lsm = .{ .flush_threshold = 1 } };

    {
        var db = try DB.open(alloc, std.mem.span(src_path), .{ .primary_backend = primary_backend });
        defer db.close();
        try db.batch(.{
            .writes = &.{.{ .key = "doc:a", .value = "{\"title\":\"alpha\"}" }},
            .sync_level = .write,
        });
        _ = try db.snapshot("snap1");
    }

    const snapshot_root = try std.fmt.allocPrint(alloc, "{s}.snapshots/snap1", .{std.mem.span(src_path)});
    defer alloc.free(snapshot_root);
    defer TestHelpers.cleanupSnapshotDirForPath(src_path);

    try DB.beginRestoreImport(alloc, std.mem.span(restore_path), snapshot_root, .{
        .backup_id = "snap1",
        .location = "file:///tmp/backups",
        .snapshot_path = "snapshots/snap1",
        .group_id = 7001,
    });
    try std.testing.expect(!(try DB.restoreRuntimeRepairNeededForPath(alloc, std.mem.span(restore_path))));
    try std.testing.expect(try DB.recoverIncompleteRestoreImportIfNeeded(alloc, std.mem.span(restore_path), .{ .primary_backend = primary_backend }));
    try std.testing.expect(try DB.restoreRuntimeRepairNeededForPath(alloc, std.mem.span(restore_path)));
    {
        var state = (try DB.readRestoreStateForPath(alloc, std.mem.span(restore_path))).?;
        defer state.deinit(alloc);
        try std.testing.expectEqualStrings("snap1", state.backup_id);
        try std.testing.expectEqualStrings("file:///tmp/backups", state.location);
        try std.testing.expectEqualStrings("snapshots/snap1", state.snapshot_path);
        try std.testing.expectEqual(@as(u64, 7001), state.group_id);
        try std.testing.expect(state.primary_restored);
        try std.testing.expect(!state.runtime_repair_complete);
    }

    {
        var restored = try DB.open(alloc, std.mem.span(restore_path), .{ .primary_backend = primary_backend });
        defer restored.close();
        var value = (try restored.lookup(alloc, "doc:a", .{})).?;
        defer value.deinit(alloc);
        try std.testing.expect(std.mem.indexOf(u8, value.json, "\"alpha\"") != null);
    }
}

test "db split cutover enrichment fence owns split range" {
    const alloc = std.testing.allocator;

    var path_buf: [256]u8 = undefined;
    const path = TestHelpers.tempPath(&path_buf);
    defer TestHelpers.cleanupTempDir(path);

    var dest_buf: [256]u8 = undefined;
    const dest = TestHelpers.tempPath(&dest_buf);
    defer TestHelpers.cleanupTempDir(dest);

    var deterministic_parent = embedder_mod.DeterministicDenseEmbedder{};
    var db = try DB.open(alloc, std.mem.span(path), .{
        .enrichment = .{
            .owner_id = "split-parent",
            .dense_embedder = deterministic_parent.interface(),
        },
    });
    defer db.close();

    try db.addIndex(.{
        .name = "dv_v1",
        .kind = .dense_vector,
        .config_json = "{\"field\":\"embedding\",\"dims\":3,\"generator\":{\"kind\":\"dense_embedding\",\"source_field\":\"body\",\"embedding_name\":\"body_dense_v1\"}}",
    });

    try db.batch(.{
        .writes = &.{
            .{ .key = "doc:a", .value = "{\"body\":\"left side\"}" },
            .{ .key = "doc:z", .value = "{\"body\":\"right side\"}" },
        },
        .sync_level = .write,
    });
    try db.enrichment_runtime.?.waitForApplied(1);
    try db.executor.waitForAll(2);

    try db.split(db.getRange(), "doc:m", "", std.mem.span(dest), true);
    try db.finalizeSplit(.{ .start = "", .end = "doc:m" });

    try std.testing.expectError(error.KeyOutOfRange, db.batch(.{
        .writes = &.{.{ .key = "doc:y", .value = "{\"body\":\"parent should reject\"}" }},
        .sync_level = .write,
    }));

    var deterministic_child = embedder_mod.DeterministicDenseEmbedder{};
    var child = try DB.open(alloc, std.mem.span(dest), .{
        .enrichment = .{
            .owner_id = "split-child",
            .dense_embedder = deterministic_child.interface(),
        },
    });
    defer child.close();

    const child_before = try child.stats(alloc);
    defer types.freeDBStats(alloc, child_before);
    const child_dense_before = try child.core.loadAppliedSequence(alloc, "dv_v1");

    try child.batch(.{
        .writes = &.{.{ .key = "doc:y", .value = "{\"body\":\"child owns this\"}" }},
        .sync_level = .write,
    });
    try child.runUntilIdle();
    const child_dense_after = try child.core.loadAppliedSequence(alloc, "dv_v1");
    try std.testing.expect(child_dense_after >= child_dense_before + 2);

    const query_vec = try deterministic_child.interface().embedDense(alloc, "", "child owns this", 3);
    defer alloc.free(query_vec);

    var child_result = try child.search(alloc, .{
        .index_name = "dv_v1",
        .dense = .{
            .vector = query_vec,
            .k = 1,
        },
    });
    defer child_result.deinit();
    try std.testing.expect(child_result.total_hits >= 1);
    try std.testing.expectEqualStrings("doc:y", child_result.hits[0].id);

    var parent_result = try db.search(alloc, .{
        .index_name = "dv_v1",
        .dense = .{
            .vector = query_vec,
            .k = 2,
        },
    });
    defer parent_result.deinit();
    for (parent_result.hits) |hit| {
        try std.testing.expect(!std.mem.eql(u8, hit.id, "doc:y"));
    }
}

test "db split cutover enrichment fence owns split range with durable lsm primary backend" {
    const alloc = std.testing.allocator;

    var path_buf: [256]u8 = undefined;
    const path = TestHelpers.tempPath(&path_buf);
    defer TestHelpers.cleanupTempDir(path);

    var dest_buf: [256]u8 = undefined;
    const dest = TestHelpers.tempPath(&dest_buf);
    defer TestHelpers.cleanupTempDir(dest);

    const primary_backend: PrimaryBackend = .{ .lsm = db_config.primary_lsm_options_default };

    var deterministic_parent = embedder_mod.DeterministicDenseEmbedder{};
    var db = try DB.open(alloc, std.mem.span(path), .{
        .primary_backend = primary_backend,
        .enrichment = .{
            .owner_id = "split-parent",
            .dense_embedder = deterministic_parent.interface(),
        },
    });
    defer db.close();

    try db.addIndex(.{
        .name = "dv_v1",
        .kind = .dense_vector,
        .config_json = "{\"field\":\"embedding\",\"dims\":3,\"generator\":{\"kind\":\"dense_embedding\",\"source_field\":\"body\",\"embedding_name\":\"body_dense_v1\"}}",
    });

    try db.batch(.{
        .writes = &.{
            .{ .key = "doc:a", .value = "{\"body\":\"left side\"}" },
            .{ .key = "doc:z", .value = "{\"body\":\"right side\"}" },
        },
        .sync_level = .write,
    });
    try db.enrichment_runtime.?.waitForApplied(1);
    try db.executor.waitForAll(2);

    try db.split(db.getRange(), "doc:m", "", std.mem.span(dest), true);
    try db.finalizeSplit(.{ .start = "", .end = "doc:m" });

    try std.testing.expectError(error.KeyOutOfRange, db.batch(.{
        .writes = &.{.{ .key = "doc:y", .value = "{\"body\":\"parent should reject\"}" }},
        .sync_level = .write,
    }));

    var deterministic_child = embedder_mod.DeterministicDenseEmbedder{};
    var child = try DB.open(alloc, std.mem.span(dest), .{
        .primary_backend = primary_backend,
        .enrichment = .{
            .owner_id = "split-child",
            .dense_embedder = deterministic_child.interface(),
        },
    });
    defer child.close();

    const child_before = try child.stats(alloc);
    defer types.freeDBStats(alloc, child_before);
    const child_dense_before = try child.core.loadAppliedSequence(alloc, "dv_v1");

    try child.batch(.{
        .writes = &.{.{ .key = "doc:y", .value = "{\"body\":\"child owns this\"}" }},
        .sync_level = .write,
    });
    try child.runUntilIdle();
    const child_dense_after = try child.core.loadAppliedSequence(alloc, "dv_v1");
    try std.testing.expect(child_dense_after >= child_dense_before + 2);

    const query_vec = try deterministic_child.interface().embedDense(alloc, "", "child owns this", 3);
    defer alloc.free(query_vec);

    var child_result = try child.search(alloc, .{
        .index_name = "dv_v1",
        .dense = .{
            .vector = query_vec,
            .k = 1,
        },
    });
    defer child_result.deinit();
    try std.testing.expect(child_result.total_hits >= 1);
    try std.testing.expectEqualStrings("doc:y", child_result.hits[0].id);

    var parent_result = try db.search(alloc, .{
        .index_name = "dv_v1",
        .dense = .{
            .vector = query_vec,
            .k = 2,
        },
    });
    defer parent_result.deinit();
    for (parent_result.hits) |hit| {
        try std.testing.expect(!std.mem.eql(u8, hit.id, "doc:y"));
    }
}

test "db merge-style cutover enrichment fence owns merged receiver range" {
    const alloc = std.testing.allocator;

    var receiver_buf: [256]u8 = undefined;
    const receiver_path = TestHelpers.tempPath(&receiver_buf);
    defer TestHelpers.cleanupTempDir(receiver_path);

    var donor_buf: [256]u8 = undefined;
    const donor_path = TestHelpers.tempPath(&donor_buf);
    defer TestHelpers.cleanupTempDir(donor_path);

    var deterministic_receiver = embedder_mod.DeterministicDenseEmbedder{};
    var receiver = try DB.open(alloc, std.mem.span(receiver_path), .{
        .enrichment = .{
            .owner_id = "merge-receiver",
            .dense_embedder = deterministic_receiver.interface(),
        },
    });
    defer receiver.close();
    try receiver.updateRange(.{ .start = "doc:a", .end = "doc:m" });

    var deterministic_donor = embedder_mod.DeterministicDenseEmbedder{};
    var donor = try DB.open(alloc, std.mem.span(donor_path), .{
        .enrichment = .{
            .owner_id = "merge-donor",
            .dense_embedder = deterministic_donor.interface(),
        },
    });
    defer donor.close();
    try donor.updateRange(.{ .start = "doc:m", .end = "" });

    try receiver.addIndex(.{
        .name = "dv_v1",
        .kind = .dense_vector,
        .config_json = "{\"field\":\"embedding\",\"dims\":3,\"generator\":{\"kind\":\"dense_embedding\",\"source_field\":\"body\",\"embedding_name\":\"body_dense_v1\"}}",
    });
    try donor.addIndex(.{
        .name = "dv_v1",
        .kind = .dense_vector,
        .config_json = "{\"field\":\"embedding\",\"dims\":3,\"generator\":{\"kind\":\"dense_embedding\",\"source_field\":\"body\",\"embedding_name\":\"body_dense_v1\"}}",
    });

    try donor.batch(.{
        .writes = &.{.{ .key = "doc:y", .value = "{\"body\":\"donor pre-cutover\"}" }},
        .sync_level = .write,
    });
    try donor.enrichment_runtime.?.waitForApplied(1);
    try donor.executor.waitForAll(2);

    try receiver.updateRange(.{ .start = "doc:a", .end = "" });
    try donor.updateRange(.{ .start = "doc:m", .end = "doc:m" });

    try std.testing.expectError(error.KeyOutOfRange, donor.batch(.{
        .writes = &.{.{ .key = "doc:z", .value = "{\"body\":\"donor should reject\"}" }},
        .sync_level = .write,
    }));

    try receiver.batch(.{
        .writes = &.{.{ .key = "doc:z", .value = "{\"body\":\"receiver merged this\"}" }},
        .sync_level = .write,
    });
    try receiver.enrichment_runtime.?.waitForApplied(1);
    try receiver.executor.waitForAll(2);

    const receiver_vec = try deterministic_receiver.interface().embedDense(alloc, "", "receiver merged this", 3);
    defer alloc.free(receiver_vec);

    var receiver_result = try receiver.search(alloc, .{
        .index_name = "dv_v1",
        .dense = .{
            .vector = receiver_vec,
            .k = 2,
        },
    });
    defer receiver_result.deinit();
    var found_receiver_doc = false;
    for (receiver_result.hits) |hit| {
        if (std.mem.eql(u8, hit.id, "doc:z")) found_receiver_doc = true;
    }
    try std.testing.expect(found_receiver_doc);

    var donor_result = try donor.search(alloc, .{
        .index_name = "dv_v1",
        .dense = .{
            .vector = receiver_vec,
            .k = 2,
        },
    });
    defer donor_result.deinit();
    for (donor_result.hits) |hit| {
        try std.testing.expect(!std.mem.eql(u8, hit.id, "doc:z"));
    }
}

test "db merge-style cutover enrichment fence owns merged receiver range with durable lsm primary backend" {
    const alloc = std.testing.allocator;

    var receiver_buf: [256]u8 = undefined;
    const receiver_path = TestHelpers.tempPath(&receiver_buf);
    defer TestHelpers.cleanupTempDir(receiver_path);

    var donor_buf: [256]u8 = undefined;
    const donor_path = TestHelpers.tempPath(&donor_buf);
    defer TestHelpers.cleanupTempDir(donor_path);

    const primary_backend: PrimaryBackend = .{ .lsm = .{ .flush_threshold = 1 } };

    var deterministic_receiver = embedder_mod.DeterministicDenseEmbedder{};
    var receiver = try DB.open(alloc, std.mem.span(receiver_path), .{
        .primary_backend = primary_backend,
        .enrichment = .{
            .owner_id = "merge-receiver",
            .dense_embedder = deterministic_receiver.interface(),
        },
    });
    defer receiver.close();
    try receiver.updateRange(.{ .start = "doc:a", .end = "doc:m" });

    var deterministic_donor = embedder_mod.DeterministicDenseEmbedder{};
    var donor = try DB.open(alloc, std.mem.span(donor_path), .{
        .primary_backend = primary_backend,
        .enrichment = .{
            .owner_id = "merge-donor",
            .dense_embedder = deterministic_donor.interface(),
        },
    });
    defer donor.close();
    try donor.updateRange(.{ .start = "doc:m", .end = "" });

    try receiver.addIndex(.{
        .name = "dv_v1",
        .kind = .dense_vector,
        .config_json = "{\"field\":\"embedding\",\"dims\":3,\"generator\":{\"kind\":\"dense_embedding\",\"source_field\":\"body\",\"embedding_name\":\"body_dense_v1\"}}",
    });
    try donor.addIndex(.{
        .name = "dv_v1",
        .kind = .dense_vector,
        .config_json = "{\"field\":\"embedding\",\"dims\":3,\"generator\":{\"kind\":\"dense_embedding\",\"source_field\":\"body\",\"embedding_name\":\"body_dense_v1\"}}",
    });

    try donor.batch(.{
        .writes = &.{.{ .key = "doc:y", .value = "{\"body\":\"donor pre-cutover\"}" }},
        .sync_level = .write,
    });
    try donor.enrichment_runtime.?.waitForApplied(1);
    try donor.executor.waitForAll(2);

    try receiver.updateRange(.{ .start = "doc:a", .end = "" });
    try donor.updateRange(.{ .start = "doc:m", .end = "doc:m" });

    try std.testing.expectError(error.KeyOutOfRange, donor.batch(.{
        .writes = &.{.{ .key = "doc:z", .value = "{\"body\":\"donor should reject\"}" }},
        .sync_level = .write,
    }));

    try receiver.batch(.{
        .writes = &.{.{ .key = "doc:z", .value = "{\"body\":\"receiver merged this\"}" }},
        .sync_level = .write,
    });
    try receiver.enrichment_runtime.?.waitForApplied(1);
    try receiver.executor.waitForAll(2);

    const receiver_vec = try deterministic_receiver.interface().embedDense(alloc, "", "receiver merged this", 3);
    defer alloc.free(receiver_vec);

    var receiver_result = try receiver.search(alloc, .{
        .index_name = "dv_v1",
        .dense = .{
            .vector = receiver_vec,
            .k = 2,
        },
    });
    defer receiver_result.deinit();
    var found_receiver_doc = false;
    for (receiver_result.hits) |hit| {
        if (std.mem.eql(u8, hit.id, "doc:z")) found_receiver_doc = true;
    }
    try std.testing.expect(found_receiver_doc);

    var donor_result = try donor.search(alloc, .{
        .index_name = "dv_v1",
        .dense = .{
            .vector = receiver_vec,
            .k = 2,
        },
    });
    defer donor_result.deinit();
    for (donor_result.hits) |hit| {
        try std.testing.expect(!std.mem.eql(u8, hit.id, "doc:z"));
    }
}

test "db merge-style cutover routes text sparse and graph indexes to the merged receiver range with durable lsm primary backend" {
    const alloc = std.testing.allocator;

    var receiver_buf: [256]u8 = undefined;
    const receiver_path = TestHelpers.tempPath(&receiver_buf);
    defer TestHelpers.cleanupTempDir(receiver_path);

    var donor_buf: [256]u8 = undefined;
    const donor_path = TestHelpers.tempPath(&donor_buf);
    defer TestHelpers.cleanupTempDir(donor_path);

    const primary_backend: PrimaryBackend = .{ .lsm = .{ .flush_threshold = 1 } };

    var receiver = try DB.open(alloc, std.mem.span(receiver_path), .{
        .primary_backend = primary_backend,
    });
    defer receiver.close();
    try receiver.updateRange(.{ .start = "doc:a", .end = "doc:m" });

    var donor = try DB.open(alloc, std.mem.span(donor_path), .{
        .primary_backend = primary_backend,
    });
    defer donor.close();
    try donor.updateRange(.{ .start = "doc:m", .end = "" });

    const index_cfgs = [_]types.IndexConfig{
        .{
            .name = "ft_v1",
            .kind = .full_text,
            .config_json = "{\"field\":\"title\"}",
        },
        .{
            .name = "sp_v1",
            .kind = .sparse_vector,
            .config_json = "{\"field\":\"sparse\"}",
        },
        .{
            .name = "graph_v1",
            .kind = .graph,
            .config_json = "{}",
        },
    };
    for (index_cfgs) |cfg| {
        try receiver.addIndex(cfg);
        try donor.addIndex(cfg);
    }

    try receiver.updateRange(.{ .start = "doc:a", .end = "" });
    try donor.updateRange(.{ .start = "doc:m", .end = "doc:m" });

    try std.testing.expectError(error.KeyOutOfRange, donor.batch(.{
        .writes = &.{.{ .key = "doc:z", .value = "{\"title\":\"zeta\",\"sparse\":{\"indices\":[2],\"values\":[1.0]}}" }},
    }));

    try receiver.batch(.{
        .writes = &.{
            .{ .key = "doc:a", .value = "{\"title\":\"alpha\",\"sparse\":{\"indices\":[1],\"values\":[1.0]}}" },
            .{ .key = "doc:z", .value = "{\"title\":\"zeta\",\"sparse\":{\"indices\":[2],\"values\":[1.0]}}" },
        },
        .graph_writes = &.{
            .{ .index_name = "graph_v1", .source = "doc:z", .target = "doc:y", .edge_type = "cites", .weight = 1.0 },
        },
        .sync_level = .full_index,
    });

    var receiver_text = try receiver.search(alloc, .{
        .index_name = "ft_v1",
        .full_text = .{ .match = .{ .field = "title", .text = "zeta" } },
    });
    defer receiver_text.deinit();
    try std.testing.expectEqual(@as(u32, 1), receiver_text.total_hits);
    try std.testing.expectEqualStrings("doc:z", receiver_text.hits[0].id);

    var receiver_sparse = try receiver.search(alloc, .{
        .index_name = "sp_v1",
        .query = .{ .sparse_knn = .{
            .indices = &.{2},
            .values = &.{1.0},
            .k = 2,
        } },
        .limit = 2,
    });
    defer receiver_sparse.deinit();
    try std.testing.expectEqual(@as(u32, 1), receiver_sparse.total_hits);
    try std.testing.expectEqualStrings("doc:z", receiver_sparse.hits[0].id);

    const receiver_incoming = try receiver.getEdges(alloc, "graph_v1", "doc:y", "cites", .in);
    defer graph_mod.GraphIndex.freeEdges(alloc, receiver_incoming);
    try std.testing.expectEqual(@as(usize, 1), receiver_incoming.len);
    try std.testing.expectEqualStrings("doc:z", receiver_incoming[0].source);

    var donor_text = try donor.search(alloc, .{
        .index_name = "ft_v1",
        .full_text = .{ .match = .{ .field = "title", .text = "zeta" } },
    });
    defer donor_text.deinit();
    try std.testing.expectEqual(@as(u32, 0), donor_text.total_hits);

    var donor_sparse = try donor.search(alloc, .{
        .index_name = "sp_v1",
        .query = .{ .sparse_knn = .{
            .indices = &.{2},
            .values = &.{1.0},
            .k = 2,
        } },
        .limit = 2,
    });
    defer donor_sparse.deinit();
    try std.testing.expectEqual(@as(u32, 0), donor_sparse.total_hits);

    const donor_incoming = try donor.getEdges(alloc, "graph_v1", "doc:y", "cites", .in);
    defer graph_mod.GraphIndex.freeEdges(alloc, donor_incoming);
    try std.testing.expectEqual(@as(usize, 0), donor_incoming.len);
}

test "db split cutover enrichment resume and fencing across reopen" {
    const alloc = std.testing.allocator;

    var parent_buf: [256]u8 = undefined;
    const parent_path = TestHelpers.tempPath(&parent_buf);
    defer TestHelpers.cleanupTempDir(parent_path);

    var child_buf: [256]u8 = undefined;
    const child_path = TestHelpers.tempPath(&child_buf);
    defer TestHelpers.cleanupTempDir(child_path);

    {
        var deterministic_parent = embedder_mod.DeterministicDenseEmbedder{};
        var parent = try DB.open(alloc, std.mem.span(parent_path), .{
            .enrichment = .{
                .owner_id = "split-parent",
                .dense_embedder = deterministic_parent.interface(),
            },
        });
        defer parent.close();

        try parent.addIndex(.{
            .name = "dv_v1",
            .kind = .dense_vector,
            .config_json = "{\"field\":\"embedding\",\"dims\":3,\"generator\":{\"kind\":\"dense_embedding\",\"source_field\":\"body\",\"embedding_name\":\"body_dense_v1\"}}",
        });

        try parent.batch(.{
            .writes = &.{
                .{ .key = "doc:a", .value = "{\"body\":\"left side\"}" },
                .{ .key = "doc:z", .value = "{\"body\":\"right side\"}" },
            },
            .sync_level = .write,
        });
        try parent.enrichment_runtime.?.waitForApplied(1);
        try parent.runDerivedUntil(parent.core.nextDerivedSequence());

        try parent.split(parent.getRange(), "doc:m", "", std.mem.span(child_path), true);
        try parent.finalizeSplit(.{ .start = "", .end = "doc:m" });

        var deterministic_child = embedder_mod.DeterministicDenseEmbedder{};
        var child = try DB.open(alloc, std.mem.span(child_path), .{
            .enrichment = .{
                .owner_id = "split-child",
                .dense_embedder = deterministic_child.interface(),
            },
        });
        defer child.close();

        const child_before = try child.stats(alloc);
        defer types.freeDBStats(alloc, child_before);
        const child_dense_before = try child.core.loadAppliedSequence(alloc, "dv_v1");
        try child.batch(.{
            .writes = &.{.{ .key = "doc:y", .value = "{\"body\":\"child pre-reopen\"}" }},
            .sync_level = .write,
        });
        try child.runUntilIdle();
        const child_dense_after = try child.core.loadAppliedSequence(alloc, "dv_v1");
        try std.testing.expect(child_dense_after > child_dense_before);
    }

    var deterministic_parent = embedder_mod.DeterministicDenseEmbedder{};
    var reopened_parent = try DB.open(alloc, std.mem.span(parent_path), .{
        .enrichment = .{
            .owner_id = "split-parent",
            .dense_embedder = deterministic_parent.interface(),
        },
    });
    defer reopened_parent.close();

    try std.testing.expectError(error.KeyOutOfRange, reopened_parent.batch(.{
        .writes = &.{.{ .key = "doc:y", .value = "{\"body\":\"parent still fenced\"}" }},
        .sync_level = .write,
    }));

    var deterministic_child = embedder_mod.DeterministicDenseEmbedder{};
    var reopened_child = try DB.open(alloc, std.mem.span(child_path), .{
        .enrichment = .{
            .owner_id = "split-child",
            .dense_embedder = deterministic_child.interface(),
        },
    });
    defer reopened_child.close();

    const child_before = try reopened_child.stats(alloc);
    defer types.freeDBStats(alloc, child_before);

    try reopened_child.batch(.{
        .writes = &.{.{ .key = "doc:x", .value = "{\"body\":\"child post-reopen\"}" }},
        .sync_level = .write,
    });
    try reopened_child.runUntilIdle();

    const pre_reopen_vec = try deterministic_child.interface().embedDense(alloc, "", "child pre-reopen", 3);
    defer alloc.free(pre_reopen_vec);

    var pre_reopen_result = try waitForSearchResult(alloc, &reopened_child, .{
        .index_name = "dv_v1",
        .dense = .{
            .vector = pre_reopen_vec,
            .k = 4,
        },
    }, 1);
    defer pre_reopen_result.deinit();
    var found_pre_reopen = false;
    for (pre_reopen_result.hits) |hit| {
        if (std.mem.eql(u8, hit.id, "doc:y")) found_pre_reopen = true;
    }
    try std.testing.expect(found_pre_reopen);

    const post_reopen_vec = try deterministic_child.interface().embedDense(alloc, "", "child post-reopen", 3);
    defer alloc.free(post_reopen_vec);

    var child_result = try waitForSearchResult(alloc, &reopened_child, .{
        .index_name = "dv_v1",
        .dense = .{
            .vector = post_reopen_vec,
            .k = 4,
        },
    }, 1);
    defer child_result.deinit();
    var found_post_reopen = false;
    for (child_result.hits) |hit| {
        if (std.mem.eql(u8, hit.id, "doc:x")) found_post_reopen = true;
    }
    try std.testing.expect(found_post_reopen);
}

test "db split cutover enrichment resume and fencing across reopen with durable lsm primary backend" {
    const alloc = std.testing.allocator;

    var parent_buf: [256]u8 = undefined;
    const parent_path = TestHelpers.tempPath(&parent_buf);
    defer TestHelpers.cleanupTempDir(parent_path);

    var child_buf: [256]u8 = undefined;
    const child_path = TestHelpers.tempPath(&child_buf);
    defer TestHelpers.cleanupTempDir(child_path);

    const primary_backend: PrimaryBackend = .{ .lsm = db_config.primary_lsm_options_default };

    {
        var deterministic_parent = embedder_mod.DeterministicDenseEmbedder{};
        var parent = try DB.open(alloc, std.mem.span(parent_path), .{
            .primary_backend = primary_backend,
            .enrichment = .{
                .owner_id = "split-parent",
                .dense_embedder = deterministic_parent.interface(),
            },
        });
        defer parent.close();

        try parent.addIndex(.{
            .name = "dv_v1",
            .kind = .dense_vector,
            .config_json = "{\"field\":\"embedding\",\"dims\":3,\"generator\":{\"kind\":\"dense_embedding\",\"source_field\":\"body\",\"embedding_name\":\"body_dense_v1\"}}",
        });

        try parent.batch(.{
            .writes = &.{
                .{ .key = "doc:a", .value = "{\"body\":\"left side\"}" },
                .{ .key = "doc:z", .value = "{\"body\":\"right side\"}" },
            },
            .sync_level = .write,
        });
        try parent.enrichment_runtime.?.waitForApplied(1);
        try parent.runDerivedUntil(parent.core.nextDerivedSequence());

        try parent.split(parent.getRange(), "doc:m", "", std.mem.span(child_path), true);
        try parent.finalizeSplit(.{ .start = "", .end = "doc:m" });

        var deterministic_child = embedder_mod.DeterministicDenseEmbedder{};
        var child = try DB.open(alloc, std.mem.span(child_path), .{
            .primary_backend = primary_backend,
            .enrichment = .{
                .owner_id = "split-child",
                .dense_embedder = deterministic_child.interface(),
            },
        });
        defer child.close();

        const child_before = try child.stats(alloc);
        defer types.freeDBStats(alloc, child_before);
        const child_dense_before = try child.core.loadAppliedSequence(alloc, "dv_v1");
        try child.batch(.{
            .writes = &.{.{ .key = "doc:y", .value = "{\"body\":\"child pre-reopen\"}" }},
            .sync_level = .write,
        });
        try child.runUntilIdle();
        const child_dense_after = try child.core.loadAppliedSequence(alloc, "dv_v1");
        try std.testing.expect(child_dense_after > child_dense_before);
        try parent.sync(true);
        try child.sync(true);
    }

    var deterministic_parent = embedder_mod.DeterministicDenseEmbedder{};
    var reopened_parent = try DB.open(alloc, std.mem.span(parent_path), .{
        .primary_backend = primary_backend,
        .enrichment = .{
            .owner_id = "split-parent",
            .dense_embedder = deterministic_parent.interface(),
        },
    });
    defer reopened_parent.close();

    try std.testing.expectError(error.KeyOutOfRange, reopened_parent.batch(.{
        .writes = &.{.{ .key = "doc:y", .value = "{\"body\":\"parent still fenced\"}" }},
        .sync_level = .write,
    }));

    var deterministic_child = embedder_mod.DeterministicDenseEmbedder{};
    var reopened_child = try DB.open(alloc, std.mem.span(child_path), .{
        .primary_backend = primary_backend,
        .enrichment = .{
            .owner_id = "split-child",
            .dense_embedder = deterministic_child.interface(),
        },
    });
    defer reopened_child.close();

    const child_before = try reopened_child.stats(alloc);
    defer types.freeDBStats(alloc, child_before);

    try reopened_child.batch(.{
        .writes = &.{.{ .key = "doc:x", .value = "{\"body\":\"child post-reopen\"}" }},
        .sync_level = .write,
    });
    try reopened_child.runUntilIdle();

    const pre_reopen_vec = try deterministic_child.interface().embedDense(alloc, "", "child pre-reopen", 3);
    defer alloc.free(pre_reopen_vec);

    var pre_reopen_result = try waitForSearchResult(alloc, &reopened_child, .{
        .index_name = "dv_v1",
        .dense = .{
            .vector = pre_reopen_vec,
            .k = 4,
        },
    }, 1);
    defer pre_reopen_result.deinit();
    var found_pre_reopen = false;
    for (pre_reopen_result.hits) |hit| {
        if (std.mem.eql(u8, hit.id, "doc:y")) found_pre_reopen = true;
    }
    try std.testing.expect(found_pre_reopen);

    const post_reopen_vec = try deterministic_child.interface().embedDense(alloc, "", "child post-reopen", 3);
    defer alloc.free(post_reopen_vec);

    var child_result = try waitForSearchResult(alloc, &reopened_child, .{
        .index_name = "dv_v1",
        .dense = .{
            .vector = post_reopen_vec,
            .k = 4,
        },
    }, 1);
    defer child_result.deinit();
    var found_post_reopen = false;
    for (child_result.hits) |hit| {
        if (std.mem.eql(u8, hit.id, "doc:x")) found_post_reopen = true;
    }
    try std.testing.expect(found_post_reopen);
}

test "db merge-style cutover enrichment resume and fencing across reopen" {
    const alloc = std.testing.allocator;

    var receiver_buf: [256]u8 = undefined;
    const receiver_path = TestHelpers.tempPath(&receiver_buf);
    defer TestHelpers.cleanupTempDir(receiver_path);

    var donor_buf: [256]u8 = undefined;
    const donor_path = TestHelpers.tempPath(&donor_buf);
    defer TestHelpers.cleanupTempDir(donor_path);

    {
        var deterministic_receiver = embedder_mod.DeterministicDenseEmbedder{};
        var receiver = try DB.open(alloc, std.mem.span(receiver_path), .{
            .enrichment = .{
                .owner_id = "merge-receiver",
                .dense_embedder = deterministic_receiver.interface(),
            },
        });
        defer receiver.close();
        try receiver.updateRange(.{ .start = "doc:a", .end = "doc:m" });

        var deterministic_donor = embedder_mod.DeterministicDenseEmbedder{};
        var donor = try DB.open(alloc, std.mem.span(donor_path), .{
            .enrichment = .{
                .owner_id = "merge-donor",
                .dense_embedder = deterministic_donor.interface(),
            },
        });
        defer donor.close();
        try donor.updateRange(.{ .start = "doc:m", .end = "" });

        try receiver.addIndex(.{
            .name = "dv_v1",
            .kind = .dense_vector,
            .config_json = "{\"field\":\"embedding\",\"dims\":3,\"generator\":{\"kind\":\"dense_embedding\",\"source_field\":\"body\",\"embedding_name\":\"body_dense_v1\"}}",
        });
        try donor.addIndex(.{
            .name = "dv_v1",
            .kind = .dense_vector,
            .config_json = "{\"field\":\"embedding\",\"dims\":3,\"generator\":{\"kind\":\"dense_embedding\",\"source_field\":\"body\",\"embedding_name\":\"body_dense_v1\"}}",
        });

        try donor.batch(.{
            .writes = &.{.{ .key = "doc:y", .value = "{\"body\":\"donor pre-cutover\"}" }},
            .sync_level = .write,
        });
        try donor.enrichment_runtime.?.waitForApplied(1);
        try donor.executor.waitForAll(2);

        try receiver.updateRange(.{ .start = "doc:a", .end = "" });
        try donor.updateRange(.{ .start = "doc:m", .end = "doc:m" });

        try receiver.batch(.{
            .writes = &.{.{ .key = "doc:z", .value = "{\"body\":\"receiver pre-reopen\"}" }},
            .sync_level = .write,
        });
        try receiver.enrichment_runtime.?.waitForApplied(1);
        try receiver.executor.waitForAll(2);
    }

    var deterministic_donor = embedder_mod.DeterministicDenseEmbedder{};
    var reopened_donor = try DB.open(alloc, std.mem.span(donor_path), .{
        .enrichment = .{
            .owner_id = "merge-donor",
            .dense_embedder = deterministic_donor.interface(),
        },
    });
    defer reopened_donor.close();

    try std.testing.expectError(error.KeyOutOfRange, reopened_donor.batch(.{
        .writes = &.{.{ .key = "doc:z", .value = "{\"body\":\"donor still fenced\"}" }},
        .sync_level = .write,
    }));

    var deterministic_receiver = embedder_mod.DeterministicDenseEmbedder{};
    var reopened_receiver = try DB.open(alloc, std.mem.span(receiver_path), .{
        .enrichment = .{
            .owner_id = "merge-receiver",
            .dense_embedder = deterministic_receiver.interface(),
        },
    });
    defer reopened_receiver.close();

    const receiver_before = try reopened_receiver.stats(alloc);
    defer types.freeDBStats(alloc, receiver_before);
    const receiver_dense_before = try reopened_receiver.core.loadAppliedSequence(alloc, "dv_v1");

    try reopened_receiver.batch(.{
        .writes = &.{.{ .key = "doc:w", .value = "{\"body\":\"receiver post-reopen\"}" }},
        .sync_level = .write,
    });
    try reopened_receiver.runUntilIdle();
    const receiver_dense_after = try waitForAppliedSequenceAdvance(alloc, &reopened_receiver, "dv_v1", receiver_dense_before);
    try std.testing.expect(receiver_dense_after > receiver_dense_before);

    const pre_reopen_vec = try deterministic_receiver.interface().embedDense(alloc, "", "receiver pre-reopen", 3);
    defer alloc.free(pre_reopen_vec);

    var pre_reopen_result = try waitForSearchResult(alloc, &reopened_receiver, .{
        .index_name = "dv_v1",
        .dense = .{
            .vector = pre_reopen_vec,
            .k = 3,
        },
    }, 1);
    defer pre_reopen_result.deinit();
    var found_pre_reopen = false;
    for (pre_reopen_result.hits) |hit| {
        if (std.mem.eql(u8, hit.id, "doc:z")) found_pre_reopen = true;
    }
    try std.testing.expect(found_pre_reopen);

    const receiver_vec = try deterministic_receiver.interface().embedDense(alloc, "", "receiver post-reopen", 3);
    defer alloc.free(receiver_vec);

    var receiver_result = try waitForSearchResult(alloc, &reopened_receiver, .{
        .index_name = "dv_v1",
        .dense = .{
            .vector = receiver_vec,
            .k = 3,
        },
    }, 1);
    defer receiver_result.deinit();
    var found = false;
    for (receiver_result.hits) |hit| {
        if (std.mem.eql(u8, hit.id, "doc:w")) found = true;
    }
    try std.testing.expect(found);
}

test "db merge-style cutover enrichment resume and fencing across reopen with durable lsm primary backend" {
    const alloc = std.testing.allocator;

    var receiver_buf: [256]u8 = undefined;
    const receiver_path = TestHelpers.tempPath(&receiver_buf);
    defer TestHelpers.cleanupTempDir(receiver_path);

    var donor_buf: [256]u8 = undefined;
    const donor_path = TestHelpers.tempPath(&donor_buf);
    defer TestHelpers.cleanupTempDir(donor_path);

    const primary_backend: PrimaryBackend = .{ .lsm = db_config.primary_lsm_options_default };

    {
        var deterministic_receiver = embedder_mod.DeterministicDenseEmbedder{};
        var receiver = try DB.open(alloc, std.mem.span(receiver_path), .{
            .primary_backend = primary_backend,
            .enrichment = .{
                .owner_id = "merge-receiver",
                .dense_embedder = deterministic_receiver.interface(),
            },
        });
        defer receiver.close();
        try receiver.updateRange(.{ .start = "doc:a", .end = "doc:m" });

        var deterministic_donor = embedder_mod.DeterministicDenseEmbedder{};
        var donor = try DB.open(alloc, std.mem.span(donor_path), .{
            .primary_backend = primary_backend,
            .enrichment = .{
                .owner_id = "merge-donor",
                .dense_embedder = deterministic_donor.interface(),
            },
        });
        defer donor.close();
        try donor.updateRange(.{ .start = "doc:m", .end = "" });

        try receiver.addIndex(.{
            .name = "dv_v1",
            .kind = .dense_vector,
            .config_json = "{\"field\":\"embedding\",\"dims\":3,\"generator\":{\"kind\":\"dense_embedding\",\"source_field\":\"body\",\"embedding_name\":\"body_dense_v1\"}}",
        });
        try donor.addIndex(.{
            .name = "dv_v1",
            .kind = .dense_vector,
            .config_json = "{\"field\":\"embedding\",\"dims\":3,\"generator\":{\"kind\":\"dense_embedding\",\"source_field\":\"body\",\"embedding_name\":\"body_dense_v1\"}}",
        });

        try donor.batch(.{
            .writes = &.{.{ .key = "doc:y", .value = "{\"body\":\"donor pre-cutover\"}" }},
            .sync_level = .write,
        });
        try donor.enrichment_runtime.?.waitForApplied(1);
        try donor.executor.waitForAll(2);

        try receiver.updateRange(.{ .start = "doc:a", .end = "" });
        try donor.updateRange(.{ .start = "doc:m", .end = "doc:m" });

        try receiver.batch(.{
            .writes = &.{.{ .key = "doc:z", .value = "{\"body\":\"receiver pre-reopen\"}" }},
            .sync_level = .write,
        });
        try receiver.enrichment_runtime.?.waitForApplied(1);
        try receiver.executor.waitForAll(2);
        try receiver.sync(true);
        try donor.sync(true);
    }

    var deterministic_receiver = embedder_mod.DeterministicDenseEmbedder{};
    var reopened_receiver = try DB.open(alloc, std.mem.span(receiver_path), .{
        .primary_backend = primary_backend,
        .enrichment = .{
            .owner_id = "merge-receiver",
            .dense_embedder = deterministic_receiver.interface(),
        },
    });
    defer reopened_receiver.close();

    var deterministic_donor = embedder_mod.DeterministicDenseEmbedder{};
    var reopened_donor = try DB.open(alloc, std.mem.span(donor_path), .{
        .primary_backend = primary_backend,
        .enrichment = .{
            .owner_id = "merge-donor",
            .dense_embedder = deterministic_donor.interface(),
        },
    });
    defer reopened_donor.close();

    try std.testing.expectError(error.KeyOutOfRange, reopened_donor.batch(.{
        .writes = &.{.{ .key = "doc:x", .value = "{\"body\":\"donor still fenced\"}" }},
        .sync_level = .write,
    }));

    const receiver_before = try reopened_receiver.stats(alloc);
    defer types.freeDBStats(alloc, receiver_before);
    const receiver_dense_before = try reopened_receiver.core.loadAppliedSequence(alloc, "dv_v1");

    try reopened_receiver.batch(.{
        .writes = &.{.{ .key = "doc:w", .value = "{\"body\":\"receiver post-reopen\"}" }},
        .sync_level = .write,
    });
    try reopened_receiver.runUntilIdle();
    const receiver_dense_after = try waitForAppliedSequenceAdvance(alloc, &reopened_receiver, "dv_v1", receiver_dense_before);
    try std.testing.expect(receiver_dense_after > receiver_dense_before);

    const pre_reopen_vec = try deterministic_receiver.interface().embedDense(alloc, "", "receiver pre-reopen", 3);
    defer alloc.free(pre_reopen_vec);

    var pre_reopen_result = try waitForSearchResult(alloc, &reopened_receiver, .{
        .index_name = "dv_v1",
        .dense = .{
            .vector = pre_reopen_vec,
            .k = 3,
        },
    }, 1);
    defer pre_reopen_result.deinit();
    var found_pre_reopen = false;
    for (pre_reopen_result.hits) |hit| {
        if (std.mem.eql(u8, hit.id, "doc:z")) found_pre_reopen = true;
    }
    try std.testing.expect(found_pre_reopen);

    const receiver_vec = try deterministic_receiver.interface().embedDense(alloc, "", "receiver post-reopen", 3);
    defer alloc.free(receiver_vec);

    var receiver_result = try waitForSearchResult(alloc, &reopened_receiver, .{
        .index_name = "dv_v1",
        .dense = .{
            .vector = receiver_vec,
            .k = 3,
        },
    }, 1);
    defer receiver_result.deinit();
    var found = false;
    for (receiver_result.hits) |hit| {
        if (std.mem.eql(u8, hit.id, "doc:w")) found = true;
    }
    try std.testing.expect(found);
}

test "db merge-style cutover preserves text sparse and graph indexes across reopen with durable lsm primary backend" {
    const alloc = std.testing.allocator;

    var receiver_buf: [256]u8 = undefined;
    const receiver_path = TestHelpers.tempPath(&receiver_buf);
    defer TestHelpers.cleanupTempDir(receiver_path);

    var donor_buf: [256]u8 = undefined;
    const donor_path = TestHelpers.tempPath(&donor_buf);
    defer TestHelpers.cleanupTempDir(donor_path);

    const primary_backend: PrimaryBackend = .{ .lsm = db_config.primary_lsm_options_default };

    {
        var receiver = try DB.open(alloc, std.mem.span(receiver_path), .{
            .primary_backend = primary_backend,
        });
        defer receiver.close();
        try receiver.updateRange(.{ .start = "doc:a", .end = "doc:m" });

        var donor = try DB.open(alloc, std.mem.span(donor_path), .{
            .primary_backend = primary_backend,
        });
        defer donor.close();
        try donor.updateRange(.{ .start = "doc:m", .end = "" });

        const index_cfgs = [_]types.IndexConfig{
            .{
                .name = "ft_v1",
                .kind = .full_text,
                .config_json = "{\"field\":\"title\"}",
            },
            .{
                .name = "sp_v1",
                .kind = .sparse_vector,
                .config_json = "{\"field\":\"sparse\"}",
            },
            .{
                .name = "graph_v1",
                .kind = .graph,
                .config_json = "{}",
            },
        };
        for (index_cfgs) |cfg| {
            try receiver.addIndex(cfg);
            try donor.addIndex(cfg);
        }

        try receiver.updateRange(.{ .start = "doc:a", .end = "" });
        try donor.updateRange(.{ .start = "doc:m", .end = "doc:m" });

        try receiver.batch(.{
            .writes = &.{
                .{ .key = "doc:a", .value = "{\"title\":\"alpha\",\"sparse\":{\"indices\":[1],\"values\":[1.0]}}" },
                .{ .key = "doc:z", .value = "{\"title\":\"zeta\",\"sparse\":{\"indices\":[2],\"values\":[1.0]}}" },
            },
            .graph_writes = &.{
                .{ .index_name = "graph_v1", .source = "doc:z", .target = "doc:y", .edge_type = "cites", .weight = 1.0 },
            },
        });
        try receiver.sync(true);
        try donor.sync(true);
    }

    var reopened_receiver = try DB.open(alloc, std.mem.span(receiver_path), .{
        .primary_backend = primary_backend,
    });
    defer reopened_receiver.close();

    var reopened_donor = try DB.open(alloc, std.mem.span(donor_path), .{
        .primary_backend = primary_backend,
    });
    defer reopened_donor.close();

    try std.testing.expectError(error.KeyOutOfRange, reopened_donor.batch(.{
        .writes = &.{.{ .key = "doc:x", .value = "{\"title\":\"blocked\"}" }},
    }));

    var receiver_text = try reopened_receiver.search(alloc, .{
        .index_name = "ft_v1",
        .full_text = .{ .match = .{ .field = "title", .text = "zeta" } },
    });
    defer receiver_text.deinit();
    try std.testing.expectEqual(@as(u32, 1), receiver_text.total_hits);
    try std.testing.expectEqualStrings("doc:z", receiver_text.hits[0].id);

    var receiver_sparse = try reopened_receiver.search(alloc, .{
        .index_name = "sp_v1",
        .query = .{ .sparse_knn = .{
            .indices = &.{2},
            .values = &.{1.0},
            .k = 2,
        } },
        .limit = 2,
    });
    defer receiver_sparse.deinit();
    try std.testing.expectEqual(@as(u32, 1), receiver_sparse.total_hits);
    try std.testing.expectEqualStrings("doc:z", receiver_sparse.hits[0].id);

    const receiver_incoming = try reopened_receiver.getEdges(alloc, "graph_v1", "doc:y", "cites", .in);
    defer graph_mod.GraphIndex.freeEdges(alloc, receiver_incoming);
    try std.testing.expectEqual(@as(usize, 1), receiver_incoming.len);
    try std.testing.expectEqualStrings("doc:z", receiver_incoming[0].source);

    var donor_text = try reopened_donor.search(alloc, .{
        .index_name = "ft_v1",
        .full_text = .{ .match = .{ .field = "title", .text = "zeta" } },
    });
    defer donor_text.deinit();
    try std.testing.expectEqual(@as(u32, 0), donor_text.total_hits);

    var donor_sparse = try reopened_donor.search(alloc, .{
        .index_name = "sp_v1",
        .query = .{ .sparse_knn = .{
            .indices = &.{2},
            .values = &.{1.0},
            .k = 2,
        } },
        .limit = 2,
    });
    defer donor_sparse.deinit();
    try std.testing.expectEqual(@as(u32, 0), donor_sparse.total_hits);

    const donor_incoming = try reopened_donor.getEdges(alloc, "graph_v1", "doc:y", "cites", .in);
    defer graph_mod.GraphIndex.freeEdges(alloc, donor_incoming);
    try std.testing.expectEqual(@as(usize, 0), donor_incoming.len);
}

test "db merge-style cutover routes relational rows and column scans across reopen" {
    const alloc = std.testing.allocator;
    const table_schema_api = @import("../../schema/mod.zig");

    var receiver_buf: [256]u8 = undefined;
    const receiver_path = TestHelpers.tempPath(&receiver_buf);
    defer TestHelpers.cleanupTempDir(receiver_path);

    var donor_buf: [256]u8 = undefined;
    const donor_path = TestHelpers.tempPath(&donor_buf);
    defer TestHelpers.cleanupTempDir(donor_path);

    const primary_backend: PrimaryBackend = .{ .lsm = .{ .flush_threshold = 1 } };
    const schema_json =
        \\{"version":1,"storage_mode":"relational","default_type":"row","enforce_types":true,"document_schemas":{"row":{"schema":{"type":"object","properties":{"id":{"type":"keyword"},"title":{"type":"text"},"status":{"type":"keyword"},"amount":{"type":"numeric"}},"required":["id","title"],"additionalProperties":false}}},"primary_key":{"columns":["id"]},"relational_indexes":[{"name":"status_amount_idx","owner_kind":"relational_column","owner_name":"status","access_method":"ordered_tuple","columns":["status"],"keys":[{"column":"status"},{"column":"amount"}],"lifecycle":"ready","generation":1,"schema_fingerprint":"secondary-index-v1:status_amount_idx","generation_record":{"generation":1,"owner_ranges":[],"lifecycle":"ready","lag":0,"ready_watermark":0}},{"name":"amount","owner_kind":"relational_column","owner_name":"amount","access_method":"scalar_column","columns":["amount"]}]}
    ;
    var parsed_schema = try table_schema_api.parseValidatedTableSchema(alloc, schema_json);
    defer parsed_schema.deinit(alloc);
    const runtime_schema = try table_schema_api.deriveRuntimeTableSchema(alloc, parsed_schema);
    defer schema_mod.freeSchema(alloc, runtime_schema);

    {
        var receiver = try DB.open(alloc, std.mem.span(receiver_path), .{
            .primary_backend = primary_backend,
        });
        defer receiver.close();
        try receiver.setSchema(runtime_schema);
        try receiver.updateRange(.{ .start = "row:a", .end = "row:m" });

        var donor = try DB.open(alloc, std.mem.span(donor_path), .{
            .primary_backend = primary_backend,
        });
        defer donor.close();
        try donor.setSchema(runtime_schema);
        try donor.updateRange(.{ .start = "row:m", .end = "" });

        const index_cfg = types.IndexConfig{
            .name = "ft_v1",
            .kind = .full_text,
            .config_json = "{\"field\":\"title\"}",
        };
        try receiver.addIndex(index_cfg);
        try donor.addIndex(index_cfg);

        try receiver.updateRange(.{ .start = "row:a", .end = "" });
        try donor.updateRange(.{ .start = "row:m", .end = "row:m" });

        try std.testing.expectError(error.KeyOutOfRange, donor.batch(.{
            .writes = &.{.{ .key = "row:z", .value = "{\"id\":\"z\",\"title\":\"donor rejected\",\"status\":\"closed\",\"amount\":99}" }},
            .sync_level = .full_index,
        }));

        try receiver.batch(.{
            .writes = &.{
                .{ .key = "row:b", .value = "{\"id\":\"b\",\"title\":\"receiver left\",\"status\":\"open\",\"amount\":10}" },
                .{ .key = "row:z", .value = "{\"id\":\"z\",\"title\":\"receiver merged relational\",\"status\":\"closed\",\"amount\":33}" },
            },
            .sync_level = .full_index,
        });
        try receiver.sync(true);
        try donor.sync(true);
    }

    var reopened_receiver = try DB.open(alloc, std.mem.span(receiver_path), .{
        .primary_backend = primary_backend,
    });
    defer reopened_receiver.close();

    var reopened_donor = try DB.open(alloc, std.mem.span(donor_path), .{
        .primary_backend = primary_backend,
    });
    defer reopened_donor.close();

    try std.testing.expectError(error.KeyOutOfRange, reopened_donor.batch(.{
        .writes = &.{.{ .key = "row:z", .value = "{\"id\":\"z\",\"title\":\"donor still fenced\",\"status\":\"closed\",\"amount\":100}" }},
        .sync_level = .full_index,
    }));

    const stored = (try reopened_receiver.get(alloc, "row:z")) orelse return error.TestExpectedEqual;
    defer alloc.free(stored);
    try std.testing.expect(std.mem.indexOf(u8, stored, "\"title\":\"receiver merged relational\"") != null);
    try std.testing.expect(std.mem.indexOf(u8, stored, "\"status\":\"closed\"") != null);
    try std.testing.expect(std.mem.indexOf(u8, stored, "\"amount\":33") != null);

    const amounts = try relational_store_mod.scanColumnAlloc(alloc, reopened_receiver.core.store, "amount", "row:z", "row:z");
    defer relational_store_mod.freeColumnValues(alloc, amounts);
    try std.testing.expectEqual(@as(usize, 1), amounts.len);
    try std.testing.expectEqualStrings("row:z", amounts[0].doc_key);
    try std.testing.expectEqual(.f64_val, amounts[0].value_type);
    try std.testing.expectEqual(@as(f64, 33), amounts[0].value.f64_val);
    try expectSingleOrderedTupleDocKey(alloc, reopened_receiver.core.store, runtime_schema, "row:z");

    var receiver_filtered = try reopened_receiver.search(alloc, .{
        .index_name = "ft_v1",
        .query = .{ .match = .{ .field = "title", .text = "relational" } },
        .filter_query_json = "{\"term\":{\"field\":\"status\",\"term\":\"closed\"}}",
        .include_stored = true,
        .limit = 10,
    });
    defer receiver_filtered.deinit();
    try std.testing.expectEqual(@as(u32, 1), receiver_filtered.total_hits);
    try std.testing.expectEqualStrings("row:z", receiver_filtered.hits[0].id);
    try std.testing.expect(receiver_filtered.hits[0].stored_data != null);

    var donor_filtered = try reopened_donor.search(alloc, .{
        .index_name = "ft_v1",
        .query = .{ .match = .{ .field = "title", .text = "relational" } },
        .filter_query_json = "{\"term\":{\"field\":\"status\",\"term\":\"closed\"}}",
        .limit = 10,
    });
    defer donor_filtered.deinit();
    try std.testing.expectEqual(@as(u32, 0), donor_filtered.total_hits);
    try expectNoOrderedTupleEntriesForDocKey(alloc, reopened_donor.core.store, "row:z");
}

test "db snapshot exports logical store only" {
    const alloc = std.testing.allocator;

    var path_buf: [256]u8 = undefined;
    const path = TestHelpers.tempPath(&path_buf);
    defer TestHelpers.cleanupTempDir(path);

    var db = try DB.open(alloc, std.mem.span(path), .{});
    defer db.close();

    try db.batch(.{
        .writes = &.{.{ .key = "doc:snap", .value = "{\"title\":\"snap\"}" }},
    });

    const snapshot_size = try db.snapshot("snap1");
    try std.testing.expect(snapshot_size > 0);

    const store_snapshot_path = try std.fmt.allocPrint(alloc, "{s}.snapshots/snap1/store.bin", .{std.mem.span(path)});
    defer alloc.free(store_snapshot_path);
    defer TestHelpers.cleanupSnapshotDirForPath(path);
    var io_impl = db_internal.threadedIo();
    defer io_impl.deinit();
    try std.Io.Dir.accessAbsolute(io_impl.io(), store_snapshot_path, .{});
}

test "db snapshot exports logical store only for durable lsm primary backend" {
    const alloc = std.testing.allocator;

    var path_buf: [256]u8 = undefined;
    const path = TestHelpers.tempPath(&path_buf);
    defer TestHelpers.cleanupTempDir(path);

    var db = try DB.open(alloc, std.mem.span(path), .{
        .primary_backend = .{ .lsm = .{ .flush_threshold = 1 } },
    });
    defer db.close();

    try db.batch(.{
        .writes = &.{.{ .key = "doc:snap", .value = "{\"title\":\"snap\"}" }},
    });

    const snapshot_size = try db.snapshot("snap1");
    try std.testing.expect(snapshot_size > 0);

    const store_snapshot_path = try std.fmt.allocPrint(alloc, "{s}.snapshots/snap1/store.bin", .{std.mem.span(path)});
    defer alloc.free(store_snapshot_path);
    defer TestHelpers.cleanupSnapshotDirForPath(path);
    var io_impl = db_internal.threadedIo();
    defer io_impl.deinit();
    try std.Io.Dir.accessAbsolute(io_impl.io(), store_snapshot_path, .{});
}
