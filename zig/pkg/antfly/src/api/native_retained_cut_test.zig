// Copyright 2026 Antfly, Inc.
// SPDX-License-Identifier: Elastic-2.0
const std = @import("std");
const local = @import("antfly_local_sources");

test "external lake composed native cut preserves primary and vector generations across writes and restart" {
    try verifyNativeCut(false);
    try verifyNativeCut(true);
}
fn verifyNativeCut(vector_store: bool) !void {
    const a = std.testing.allocator;
    var directory = try local.common_test_directory.TestDirectory.init("native-cursor-writer");
    defer directory.cleanup();
    const pins = try std.fmt.allocPrint(a, "{s}.query-pins", .{directory.path()});
    defer a.free(pins);
    defer std.Io.Dir.cwd().deleteTree(std.testing.io, pins) catch {};
    const DB = local.storage_db_db.DB;
    var db = try DB.open(a, directory.path(), .{ .identity_namespace = .{ .table_id = 7, .shard_id = 1, .range_id = 1 } });
    var opened = true;
    defer if (opened) db.close();
    if (vector_store) try db.configureTableStorage(.{ .dense_embeddings = .vector_store });
    try db.addIndex(.{ .name = "semantic", .kind = .dense_vector, .config_json = "{\"field\":\"v\",\"dims\":2,\"metric\":\"l2_squared\"}" });
    try db.batch(.{ .writes = &.{.{ .key = "doc", .value = "{\"body\":\"before\",\"v\":[1,0]}" }}, .sync_level = .full_index });
    const id: [64]u8 = @splat('a');
    const Cut = @typeInfo(@FieldType(local.storage_db_types.SearchRequest, "native_query_cut")).optional.child;
    var cut: Cut = .{ .id = &id, .table_id = 7, .expires_ms = @import("antfly_platform").time.realtimeNs() / std.time.ns_per_ms + 60_000, .create = true };
    var timed_out = cut;
    timed_out.timeout_ms = 0;
    try std.testing.expectError(error.DeadlineExceeded, db.captureQueryCut(timed_out, .none));
    try db.captureQueryCut(cut, .none);
    cut.create = false;
    try db.batch(.{ .writes = &.{.{ .key = "doc", .value = "{\"body\":\"after\",\"v\":[0,1]}" }}, .sync_level = .full_index });
    {
        var snapshot = try db.openQueryCut(cut, .none);
        defer snapshot.close();
        if (vector_store) try std.testing.expectEqual(@as(u64, 0), snapshot.local_execution.source_vectors.load(.acquire).?.stats.inventory_updates);
        const stored = (try snapshot.get(a, "doc")).?;
        defer a.free(stored);
        try std.testing.expect(std.mem.indexOf(u8, stored, "before") != null);
        var result = try snapshot.search(a, .{ .index_name = "semantic", .dense = .{ .vector = &.{ 1, 0 }, .k = 1 }, .limit = 1 });
        defer result.deinit();
        try std.testing.expectEqual(@as(usize, 1), result.hits.len);
        try std.testing.expectEqualStrings("doc", result.hits[0].id);
        try std.testing.expectEqual(@as(?f32, 0), result.hits[0].distance);
    }
    db.close();
    opened = false;
    var restarted = try DB.open(a, directory.path(), .{ .identity_namespace = .{ .table_id = 7, .shard_id = 1, .range_id = 1 } });
    defer restarted.close();
    var snapshot = try restarted.openQueryCut(cut, .none);
    defer snapshot.close();
    const stored = (try snapshot.get(a, "doc")).?;
    defer a.free(stored);
    try std.testing.expect(std.mem.indexOf(u8, stored, "before") != null);
    var missing = cut;
    const other: [64]u8 = @splat('b');
    missing.id = &other;
    try std.testing.expectError(error.CatalogGenerationChanged, restarted.openQueryCut(missing, .none));
    var wrong = cut;
    wrong.table_id = 8;
    try std.testing.expectError(error.CatalogGenerationChanged, restarted.openQueryCut(wrong, .none));
    var expired = cut;
    expired.expires_ms = 0;
    try std.testing.expectError(error.CatalogGenerationChanged, restarted.openQueryCut(expired, .none));
}

test "external lake native cursor capability fences recipe incarnation scope and expiration" {
    const a = std.testing.allocator;
    var directory = try local.common_test_directory.TestDirectory.init("native-cursor-capability");
    defer directory.cleanup();
    var fs = try @import("../serverless/artifacts/fs_store.zig").FsStore.init(a, directory.path());
    defer fs.deinit();
    var store = fs.artifactStore();
    const retained = @import("native_retained_cut.zig");
    var table: local.common_topology_records.TableRecord = .{ .table_id = 7, .name = "current", .schema_json = "{}", .indexes_json = "{}" };
    const token = try retained.save(a, &store, @splat(1), std.testing.io, table, 100, .none);
    defer a.free(token);
    var arena = std.heap.ArenaAllocator.init(a);
    defer arena.deinit();
    const descriptor = try retained.load(arena.allocator(), &store, @splat(1), token, table, 101, .none);
    try std.testing.expectEqual(@as(usize, 64), descriptor.id.len);
    try std.testing.expectError(error.CatalogGenerationChanged, retained.load(arena.allocator(), &store, @splat(2), token, table, 101, .none));
    try std.testing.expectError(error.CatalogGenerationChanged, retained.load(arena.allocator(), &store, @splat(1), token, table, 60_100, .none));
    table.table_id = 8;
    try std.testing.expectError(error.CatalogGenerationChanged, retained.load(arena.allocator(), &store, @splat(1), token, table, 101, .none));
    table.table_id = 7;
    table.indexes_json = "{\"changed\":{}}";
    try std.testing.expectError(error.CatalogGenerationChanged, retained.load(arena.allocator(), &store, @splat(1), token, table, 101, .none));
}
