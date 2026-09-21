// Copyright 2026 Antfly, Inc.
// SPDX-License-Identifier: Elastic-2.0
const std = @import("std");
const db_mod = @import("mod.zig");
const records = @import("relational_index_records.zig");
const rows = @import("relational_rows.zig");
const codec = @import("algebraic/relational_row_codec.zig");
const alloc = std.testing.allocator;
const schema_json =
    \\{"version":1,"storage_mode":"relational","default_type":"row","relational_indexes":[{"name":"by_id","keys":[{"column":"id"}],"include_columns":["label"]}],"document_schemas":{"row":{"schema":{"type":"object","properties":{"id":{"type":"integer"},"label":{"type":"keyword"},"wide":{"type":"keyword"}},"additionalProperties":false}}}}
;

fn ready(db: *db_mod.DB) !void {
    for (0..1024) |_| {
        if ((try db.relationalIndexBuildStatus("by_id")).state == .ready) return;
        _ = try db.runRelationalIndexMaintenancePass();
    }
    return error.IndexBuildDidNotConverge;
}

fn scan(db: *db_mod.DB, request: rows.Request) !rows.Page {
    var reader = try db.beginRelationalRows(alloc, request);
    defer reader.deinit();
    return reader.nextPage(alloc, std.testing.io, .{ .rows = 4096, .records = 4096, .time_ns = std.time.ns_per_s });
}

test "relational index system covering preserves row metadata, updates included values and falls back for authorization" {
    var directory = try @import("../../common/test_directory.zig").TestDirectory.init("relational-cover");
    defer directory.cleanup();
    var db = try db_mod.DB.open(alloc, directory.path(), .{ .start_optional_runtimes = false });
    defer db.close();
    try db.setSchemaJson(alloc, schema_json);
    try db.batch(.{ .timestamp_ns = 123, .writes = &.{.{ .key = "a", .value = "{\"id\":1,\"label\":\"before\",\"wide\":\"secret\"}" }} });
    try ready(&db);
    var covered = try scan(&db, .{ .index = "by_id", .fields = &.{ "id", "label" } });
    defer covered.deinit();
    var primary = try scan(&db, .{ .fields = &.{ "id", "label" } });
    defer primary.deinit();
    try std.testing.expectEqual(@as(usize, 1), covered.rows.len);
    try std.testing.expectEqual(@as(usize, 0), covered.primary_lookups);
    try std.testing.expectEqual(@as(usize, 1), covered.index_only_rows);
    try std.testing.expectEqualStrings(primary.rows[0].json, covered.rows[0].json);
    try std.testing.expectEqual(primary.rows[0].version, covered.rows[0].version);
    try std.testing.expectEqual(primary.rows[0].schema_version, covered.rows[0].schema_version);
    try std.testing.expectEqualSlices(u8, &primary.rows[0].semantic_hash, &covered.rows[0].semantic_hash);
    // Duplicate-key fallback preparation finalizes timestamps before indexing.
    try db.batch(.{ .timestamp_ns = 456, .writes = &.{
        .{ .key = "a", .value = "{\"id\":1,\"label\":\"middle\",\"wide\":\"secret\"}" },
        .{ .key = "a", .value = "{\"id\":1,\"label\":\"after\",\"wide\":\"secret\"}" },
    } });
    var updated = try scan(&db, .{ .index = "by_id", .fields = &.{"label"}, .conditions = &.{.{ .column = "label", .op = .eq, .value = .{ .string = "after" } }} });
    defer updated.deinit();
    try std.testing.expectEqual(@as(usize, 1), updated.rows.len);
    try std.testing.expectEqualStrings("{\"label\":\"after\"}", updated.rows[0].json);
    try std.testing.expectEqual(@as(u64, 456), updated.rows[0].version);
    var sentinel: u8 = 0;
    var authenticated = try scan(&db, .{ .index = "by_id", .fields = &.{"label"}, .row_filter = .{ .context = &sentinel, .matches = struct {
        fn matches(_: *anyopaque, _: std.mem.Allocator, _: []const u8, row: codec.OrdinalRowView) !bool {
            const cell = (try row.findCell(row.ordinalForName("wide").?)).?;
            return std.mem.eql(u8, cell.value.bytes_val, "secret");
        }
    }.matches } });
    defer authenticated.deinit();
    try std.testing.expectEqual(@as(usize, 1), authenticated.primary_lookups);
    try std.testing.expectEqual(@as(usize, 0), authenticated.index_only_rows);
    var digest = try scan(&db, .{ .index = "by_id", .include_primary_digest = true });
    defer digest.deinit();
    try std.testing.expect(digest.rows[0].expected_content_digest != null);
    try std.testing.expectEqual(@as(usize, 1), digest.primary_lookups);
    try db.batch(.{ .deletes = &.{"a"} });
    var deleted = try scan(&db, .{ .index = "by_id", .fields = &.{"label"} });
    defer deleted.deinit();
    try std.testing.expectEqual(@as(usize, 0), deleted.rows.len);
}

test "relational index system covering corruption fails closed and bounded ownership rebuild fills placeholders" {
    var directory = try @import("../../common/test_directory.zig").TestDirectory.init("relational-cover-repair");
    defer directory.cleanup();
    var db = try db_mod.DB.open(alloc, directory.path(), .{ .start_optional_runtimes = false });
    defer db.close();
    try db.setSchemaJson(alloc, schema_json);
    try db.batch(.{ .writes = &.{.{ .key = "b", .value = "{\"id\":1,\"label\":\"retained\"}" }} });
    try ready(&db);
    const key = blk: {
        var read = try db.core.store.beginReadTxn();
        defer read.abort();
        var cursor = try read.openCursor();
        defer cursor.close();
        const kv = (try cursor.seekAtOrAfter(records.forward_namespace)).?;
        const wrong = try alloc.dupe(u8, kv.key);
        defer alloc.free(wrong);
        wrong[wrong.len - 7] ^= 1;
        try std.testing.expectError(error.InvalidRelationalIndexForwardValue, records.forwardPayload(wrong, kv.value));
        break :blk try alloc.dupe(u8, kv.key);
    };
    defer alloc.free(key);
    try db.core.store.putBatch(&.{.{ .key = key, .value = "corrupt" }}, &.{});
    try std.testing.expectError(error.InvalidRelationalIndexForwardValue, scan(&db, .{ .index = "by_id", .fields = &.{"label"} }));
    // Transfer creates tuple-only placeholders; ownership fencing forces the
    // same primary-driven job to fill them before readiness can be published.
    try db.core.store.putBatch(&.{.{ .key = key, .value = "" }}, &.{});
    try db.updateRange(.{ .start = "a", .end = "z" });
    try std.testing.expectError(error.RelationalIndexNotReady, db.beginRelationalRows(alloc, .{ .index = "by_id" }));
    try ready(&db);
    db.close();
    db = try db_mod.DB.open(alloc, directory.path(), .{ .start_optional_runtimes = false });
    var repaired = try scan(&db, .{ .index = "by_id", .fields = &.{"label"} });
    defer repaired.deinit();
    try std.testing.expectEqualStrings("{\"label\":\"retained\"}", repaired.rows[0].json);
    try std.testing.expectEqual(@as(usize, 0), repaired.primary_lookups);
}

fn prepareCoverAllocationFailure(test_alloc: std.mem.Allocator) !void {
    const registry = @import("schema_registry.zig");
    const plans = @import("relational_index_plan.zig");
    var schemas = try registry.Registry.initCloned(test_alloc, std.testing.io, .{ .version = 1, .storage_mode = .relational, .relational_columns = &.{
        .{ .name = "id", .path = "id", .column_type = .integer },
        .{ .name = "label", .path = "label", .column_type = .string },
    } });
    defer schemas.deinit();
    var schema_view = schemas.acquire().?;
    defer schema_view.release();
    var plan = try plans.View.init(test_alloc, schema_view, &.{.{ .name = "covered", .generation = 1, .keys = &.{.{ .column = "id" }}, .include_columns = &.{"label"} }});
    defer plan.release();
    var prepared = try @import("document_mapper.zig").PreparedRelationalWrite.init(test_alloc, "a", "{\"id\":1,\"label\":\"data\"}", null, schema_view.tableSchema().*, schema_view.physicalLayout());
    defer prepared.deinit(test_alloc);
    var batch = plans.Batch.init(test_alloc, plan);
    defer batch.deinit();
    _ = try batch.appendPrepared(&prepared);
    const key = try batch.key(0, 0);
    var source = try plan.boundIndexes()[0].cover.?.projectSource(test_alloc, schema_view.tableSchema().*, schema_view.physicalLayout());
    defer source.deinit();
    const decoded = try plan.boundIndexes()[0].cover.?.decode(key.payload);
    const primary = try prepared.typedView(schema_view.tableSchema().*, schema_view.physicalLayout());
    const rebound = try plan.boundIndexes()[0].cover.?.encodeSource(test_alloc, primary, &source);
    defer test_alloc.free(rebound);
    try std.testing.expectEqualSlices(u8, key.payload, rebound);
    try std.testing.expectEqualSlices(u8, &decoded.semanticHash(), &primary.semanticHash());
}

test "relational index system covering preparation releases every allocation failure" {
    try std.testing.checkAllAllocationFailures(alloc, prepareCoverAllocationFailure, .{});
}

test "relational index system covering cold bindings reuse historical ordinals and reject incompatible layouts" {
    const schema = @import("../schema.zig");
    const mapper = @import("document_mapper.zig");
    const covering = @import("relational_index_cover.zig");
    const current: schema.TableSchema = .{ .version = 10, .storage_mode = .relational, .relational_columns = &.{
        .{ .name = "id", .path = "id", .column_type = .integer },
        .{ .name = "label", .path = "label", .column_type = .string },
    } };
    var layout = try codec.PhysicalLayout.init(alloc, current);
    defer layout.deinit();
    var plan = try covering.Plan.init(alloc, current, &layout, &.{.{ .column = "id" }}, &.{"label"});
    defer plan.deinit();
    const historical: schema.TableSchema = .{ .version = 9, .storage_mode = .relational, .relational_columns = &.{ current.relational_columns[1], current.relational_columns[0] } };
    var old_layout = try codec.PhysicalLayout.init(alloc, historical);
    defer old_layout.deinit();
    var binding = try plan.projectSource(alloc, historical, &old_layout);
    defer binding.deinit();
    try std.testing.expectEqual(@as(?usize, 1), binding.ordinals[0]);
    try std.testing.expectEqual(@as(?usize, 0), binding.ordinals[1]);
    var prepared = try mapper.PreparedRelationalWrite.init(alloc, "a", "{\"id\":7,\"label\":\"cold\"}", null, historical, &old_layout);
    defer prepared.deinit(alloc);
    const typed = try prepared.typedView(historical, &old_layout);
    try std.testing.expectError(error.RelationalRowSchemaMismatch, plan.encode(alloc, typed));
    for (0..32) |_| {
        const encoded = try plan.encodeSource(alloc, typed, &binding);
        defer alloc.free(encoded);
        try std.testing.expectEqual(@as(u32, 9), try covering.Plan.sourceVersion(encoded));
        const row = try plan.decode(encoded);
        const cell = (try row.findCell(1)).?;
        try std.testing.expectEqualStrings("cold", cell.value.bytes_val);
    }
    const invalid: schema.TableSchema = .{ .version = 8, .storage_mode = .relational, .relational_columns = &.{
        current.relational_columns[0], .{ .name = "label", .path = "label", .column_type = .integer },
    } };
    var invalid_layout = try codec.PhysicalLayout.init(alloc, invalid);
    defer invalid_layout.deinit();
    try std.testing.expectError(error.RelationalIndexColumnTypeMismatch, plan.projectSource(alloc, invalid, &invalid_layout));
}

test "relational index system covering work counts avoid primary probes for narrow projections over wide rows" {
    var directory = try @import("../../common/test_directory.zig").TestDirectory.init("relational-cover-work");
    defer directory.cleanup();
    var db = try db_mod.DB.open(alloc, directory.path(), .{ .start_optional_runtimes = false });
    defer db.close();
    try db.setSchemaJson(alloc, schema_json);
    var arena = std.heap.ArenaAllocator.init(alloc);
    defer arena.deinit();
    const owned = arena.allocator();
    const wide = try owned.alloc(u8, 4096);
    @memset(wide, 'x');
    const writes = try owned.alloc(db_mod.types.BatchWrite, 128);
    for (writes, 0..) |*write, i| write.* = .{
        .key = try std.fmt.allocPrint(owned, "doc:{d:0>5}", .{i}),
        .value = try std.json.Stringify.valueAlloc(owned, .{ .id = i, .label = "small", .wide = wide }, .{}),
    };
    try db.batch(.{ .writes = writes });
    try ready(&db);
    var covered = try scan(&db, .{ .index = "by_id", .fields = &.{"label"} });
    defer covered.deinit();
    var fallback = try scan(&db, .{ .index = "by_id", .fields = &.{"label"}, .conditions = &.{.{ .column = "wide", .op = .is_not_null }} });
    defer fallback.deinit();
    try std.testing.expectEqual(@as(usize, 128), covered.rows.len);
    try std.testing.expectEqual(covered.rows.len, fallback.rows.len);
    try std.testing.expectEqual(@as(usize, 0), covered.primary_lookups);
    try std.testing.expectEqual(@as(usize, 128), fallback.primary_lookups);
    for (covered.rows, fallback.rows) |a, b| try std.testing.expectEqualStrings(a.json, b.json);
    var forward_bytes: usize = 0;
    var read = try db.core.store.beginReadTxn();
    defer read.abort();
    var cursor = try read.openCursor();
    defer cursor.close();
    var entry = try cursor.seekAtOrAfter(records.forward_namespace);
    while (entry) |kv| : (entry = try cursor.next()) {
        if (!records.isForwardKey(kv.key)) break;
        forward_bytes += kv.value.len;
    }
    try std.testing.expect(forward_bytes < 128 * 512);
    std.debug.print("covering-index work rows=128 primary_probes=0 fallback_probes=128 unselected_json_bytes={d} forward_payload_bytes={d}\n", .{ wide.len * 128, forward_bytes });
}

test "relational index system covering TTL visibility uses full row timestamp without selecting TTL column" {
    var directory = try @import("../../common/test_directory.zig").TestDirectory.init("relational-cover-ttl");
    defer directory.cleanup();
    var db = try db_mod.DB.open(alloc, directory.path(), .{ .start_optional_runtimes = false });
    defer db.close();
    const declaration =
        \\{"version":1,"storage_mode":"relational","default_type":"row","ttl":{"duration":"1s","field":"expires"},"relational_indexes":[{"name":"by_id","keys":[{"column":"id"}],"include_columns":["label"]}],"document_schemas":{"row":{"schema":{"type":"object","properties":{"id":{"type":"integer"},"label":{"type":"keyword"},"expires":{"type":"datetime"}},"additionalProperties":false}}}}
    ;
    try db.setSchemaJson(alloc, declaration);
    try db.batch(.{ .writes = &.{
        .{ .key = "old", .value = "{\"id\":1,\"label\":\"expired\",\"expires\":\"2000-01-01T00:00:00Z\"}" },
        .{ .key = "new", .value = "{\"id\":2,\"label\":\"visible\",\"expires\":\"2099-01-01T00:00:00Z\"}" },
    } });
    try ready(&db);
    var covered = try scan(&db, .{ .index = "by_id", .fields = &.{"label"} });
    defer covered.deinit();
    var primary = try scan(&db, .{ .fields = &.{"label"} });
    defer primary.deinit();
    try std.testing.expectEqual(@as(usize, 1), covered.rows.len);
    try std.testing.expectEqualStrings("new", covered.rows[0].key);
    try std.testing.expectEqualStrings(primary.rows[0].json, covered.rows[0].json);
    try std.testing.expectEqual(@as(usize, 0), covered.primary_lookups);
}

test "relational index system covering layout identities retain unrelated epochs and rebuild changed payload dependencies" {
    var directory = try @import("../../common/test_directory.zig").TestDirectory.init("relational-cover-schema");
    defer directory.cleanup();
    var db = try db_mod.DB.open(alloc, directory.path(), .{ .start_optional_runtimes = false });
    defer db.close();
    try db.setSchemaJson(alloc, schema_json);
    try db.batch(.{ .writes = &.{.{ .key = "a", .value = "{\"id\":1,\"label\":\"original\",\"wide\":\"cold\"}" }} });
    try ready(&db);
    const original = (try db.relationalIndexBuildStatus("by_id")).generation;
    const unrelated =
        \\{"version":2,"storage_mode":"relational","default_type":"row","relational_indexes":[{"name":"by_id","keys":[{"column":"id"}],"include_columns":["label"]}],"document_schemas":{"row":{"schema":{"type":"object","properties":{"id":{"type":"integer"},"label":{"type":"keyword"},"wide":{"type":"keyword"},"added":{"type":"keyword"}},"additionalProperties":false}}}}
    ;
    try db.setSchemaJson(alloc, unrelated);
    try std.testing.expectEqual(original, (try db.relationalIndexBuildStatus("by_id")).generation);
    var historical = try scan(&db, .{ .index = "by_id", .fields = &.{"label"} });
    defer historical.deinit();
    try std.testing.expectEqual(@as(u32, 1), historical.rows[0].schema_version);
    try std.testing.expectEqualStrings("{\"label\":\"original\"}", historical.rows[0].json);
    const changed =
        \\{"version":3,"storage_mode":"relational","default_type":"row","relational_indexes":[{"name":"by_id","keys":[{"column":"id"}],"include_columns":["wide"]}],"document_schemas":{"row":{"schema":{"type":"object","properties":{"id":{"type":"integer"},"label":{"type":"keyword"},"wide":{"type":"keyword"},"added":{"type":"keyword"}},"additionalProperties":false}}}}
    ;
    try db.setSchemaJson(alloc, changed);
    const state = try db.relationalIndexBuildStatus("by_id");
    try std.testing.expect(state.generation != original);
    try std.testing.expectEqual(.building, state.state);
    try ready(&db);
    var rebuilt = try scan(&db, .{ .index = "by_id", .fields = &.{"wide"} });
    defer rebuilt.deinit();
    try std.testing.expectEqual(@as(usize, 0), rebuilt.primary_lookups);
    try std.testing.expectEqualStrings("{\"wide\":\"cold\"}", rebuilt.rows[0].json);
}
