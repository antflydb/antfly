// Copyright 2026 Antfly, Inc.
// SPDX-License-Identifier: Apache-2.0

//! Real primary/replay/graph owners, not a graph-only snapshot simulation.
const std = @import("std");
const db_mod = @import("db.zig");
const types = @import("types.zig");
const directory_mod = @import("../../common/test_directory.zig");

fn seed(database: *db_mod.DB, phase: []const u8, target: []const u8, sync: types.SyncLevel) !void {
    const a = std.testing.allocator;
    const root = try std.fmt.allocPrint(a, "{{\"phase\":\"{s}\",\"_edges\":{{\"g\":{{\"cites\":[{{\"target\":\"{s}\",\"weight\":2.5}}]}}}}}}", .{ phase, target });
    defer a.free(root);
    const value = try std.fmt.allocPrint(a, "{{\"phase\":\"{s}\"}}", .{phase});
    defer a.free(value);
    try database.batch(.{ .writes = &.{
        .{ .key = "root", .value = root },
        .{ .key = "old", .value = value },
        .{ .key = "new", .value = value },
    }, .sync_level = sync });
}

fn capture(database: *db_mod.DB, names: []const []const u8) !db_mod.DB.RelationalStatementSnapshot {
    var fence = (try database.tryStatementReadFence()) orelse return error.TestUnexpectedResult;
    defer fence.release();
    return database.captureRelationalStatementSnapshotWithGraphs(names);
}

test "retained graph statement cut preserves primary row and graph generation across mutation and index drop" {
    const a = std.testing.allocator;
    var directory = try directory_mod.TestDirectory.init("graph-statement-cut");
    defer directory.cleanup();
    var database = try db_mod.DB.open(a, directory.path(), .{ .start_index_workers = false, .start_optional_runtimes = false });
    defer database.close();
    try database.setSchemaJson(a,
        \\{"version":1,"storage_mode":"document","default_type":"doc","document_schemas":{"doc":{"schema":{"type":"object","properties":{"phase":{"type":"keyword"}},"additionalProperties":true}}}}
    );
    try database.addIndex(.{ .name = "g", .kind = .graph, .config_json = "{}" });
    try database.addIndex(.{ .name = "unselected", .kind = .graph, .config_json = "{}" });
    try database.batch(.{ .activate_range_tracking = true });
    try seed(&database, "before", "old", .full_index);
    var pinned = try capture(&database, &.{"g"});
    defer pinned.deinit();
    try std.testing.expectEqual(@as(usize, 1), pinned.graphs.len);
    try seed(&database, "after", "new", .full_index);
    try std.testing.expect(try database.deleteIndex("g"));
    var graph = try database.relationalGraphSnapshotPage(&pinned, a, .{ .index = "g", .key = "root", .edge_types = &.{"cites"} });
    defer graph.deinit(a);
    try graph.proof.validate();
    try std.testing.expect(pinned.read_at_ns != 0);
    try std.testing.expectEqual(pinned.read_at_ns, graph.proof.ttl_read_at_ns);
    try std.testing.expectEqual(@as(usize, 1), graph.page.edges.len);
    try std.testing.expectEqualStrings("old", graph.page.edges[0].target);
    try std.testing.expectEqual(@as(f64, 2.5), graph.page.edges[0].weight);
    try std.testing.expectError(error.SqlStatementSnapshotRequired, database.relationalGraphSnapshotPage(&pinned, a, .{ .index = "unselected", .key = "root" }));
    const rows = try database.openDocumentReadSessionAtSnapshot(a, "", "", .{ .include_range_proofs = true, .relational_query = .{ .fields = &.{"phase"} } }, &pinned);
    defer rows.deinit();
    var row_count: usize = 0;
    while (true) {
        var page = try rows.next(a, 2);
        defer page.deinit();
        for (page.rows) |row| {
            try std.testing.expectEqualStrings("before", row.value.object.get("phase").?.string);
            row_count += 1;
        }
        if (page.after == null) break;
    }
    try std.testing.expectEqual(@as(usize, 3), row_count);
}

test "retained graph statement cut rejects replay lag and busy projection before publishing any rows" {
    const a = std.testing.allocator;
    var directory = try directory_mod.TestDirectory.init("graph-statement-lag");
    defer directory.cleanup();
    var database = try db_mod.DB.open(a, directory.path(), .{ .start_index_workers = false, .start_optional_runtimes = false });
    defer database.close();
    try database.addIndex(.{ .name = "g", .kind = .graph, .config_json = "{}" });
    try seed(&database, "before", "old", .full_index);
    {
        const index = database.core.index_manager.graphIndex("g").?;
        try std.testing.expect(index.apply_mutex.tryLock());
        defer index.apply_mutex.unlock();
        try std.testing.expectError(error.ReadUnavailable, capture(&database, &.{"g"}));
    }
    try seed(&database, "after", "new", .write);
    try std.testing.expectError(error.ReadUnavailable, capture(&database, &.{"g"}));
    try database.catchUpPendingDerivedReplay();
    var recovered = try capture(&database, &.{"g"});
    defer recovered.deinit();
    var graph = try database.relationalGraphSnapshotPage(&recovered, a, .{ .index = "g", .key = "root", .edge_types = &.{"cites"} });
    defer graph.deinit(a);
    try graph.proof.validate();
    try std.testing.expectEqualStrings("new", graph.page.edges[0].target);
}

test "retained graph statement cut certifies selected empty graph durably across cold reopen" {
    const a = std.testing.allocator;
    var directory = try directory_mod.TestDirectory.init("graph-statement-empty");
    defer directory.cleanup();
    const options: db_mod.OpenOptions = .{ .start_index_workers = false, .start_optional_runtimes = false };
    {
        var database = try db_mod.DB.open(a, directory.path(), options);
        defer database.close();
        try database.addIndex(.{ .name = "empty", .kind = .graph, .config_json = "{}" });
        var pinned = try capture(&database, &.{"empty"});
        defer pinned.deinit();
        var graph = try database.relationalGraphSnapshotPage(&pinned, a, .{ .index = "empty", .key = "missing" });
        defer graph.deinit(a);
        try graph.proof.validate();
        try std.testing.expectEqual(@as(usize, 0), graph.page.edges.len);
        try std.testing.expect(graph.page.next_cursor == null);
    }
    {
        var reopened = try db_mod.DB.open(a, directory.path(), options);
        defer reopened.close();
        var pinned = try capture(&reopened, &.{"empty"});
        defer pinned.deinit();
        var graph = try reopened.relationalGraphSnapshotPage(&pinned, a, .{ .index = "empty", .key = "missing" });
        defer graph.deinit(a);
        try graph.proof.validate();
        try std.testing.expectEqual(@as(usize, 0), graph.page.edges.len);
    }
}

test "retained graph statement cut row readers use the captured TTL clock for relational and document rows" {
    const a = std.testing.allocator;
    inline for (.{ "relational", "document" }) |mode| {
        var directory = try directory_mod.TestDirectory.init("statement-ttl-clock");
        defer directory.cleanup();
        var database = try db_mod.DB.open(a, directory.path(), .{ .start_index_workers = false, .start_optional_runtimes = false });
        defer database.close();
        const schema = try std.fmt.allocPrint(a, "{{\"version\":1,\"storage_mode\":\"{s}\",\"default_type\":\"row\",\"ttl_duration_ns\":1000,\"document_schemas\":{{\"row\":{{\"schema\":{{\"type\":\"object\",\"properties\":{{\"n\":{{\"type\":\"integer\"}},\"_timestamp\":{{\"type\":\"datetime\"}}}},\"additionalProperties\":false}}}}}}}}", .{mode});
        defer a.free(schema);
        try database.setSchemaJson(a, schema);
        try database.batch(.{ .timestamp_ns = 100, .writes = &.{.{ .key = "a", .value = "{\"n\":7}" }} });
        var pinned = try capture(&database, &.{});
        defer pinned.deinit();
        // A deterministic synthetic instant before expiration. Production
        // captures set this field exactly once from the owner clock.
        pinned.read_at_ns = 200;
        const options: types.ScanOptions = .{ .relational_query = .{ .fields = &.{"n"} } };
        if (comptime std.mem.eql(u8, mode, "relational")) {
            const reader = try database.openRelationalReadSessionAtSnapshot(a, "", "", options, &pinned);
            defer reader.deinit();
            var page = try reader.nextTypedPage(a, null, .{ .rows = 8 });
            defer page.deinit();
            try std.testing.expectEqual(@as(usize, 1), page.rows.len);
        } else {
            const reader = try database.openDocumentReadSessionAtSnapshot(a, "", "", options, &pinned);
            defer reader.deinit();
            var page = try reader.next(a, 8);
            defer page.deinit();
            try std.testing.expectEqual(@as(usize, 1), page.rows.len);
        }
    }
}

test "retained graph statement cut raw admission drains existing calls and rejects new calls throughout policy activation" {
    const a = std.testing.allocator;
    var directory = try directory_mod.TestDirectory.init("graph-raw-policy-admission");
    defer directory.cleanup();
    var database = try db_mod.DB.open(a, directory.path(), .{ .start_index_workers = false, .start_optional_runtimes = false });
    defer database.close();
    var raw = try database.beginRawGraphReadLease();
    const gate = &database.local_execution.row_policy_gate;
    try gate.beginPreparing(.disabled);
    try std.testing.expect(!gate.quiesced());
    try std.testing.expectError(error.RowPolicyUnsupported, database.beginRawGraphReadLease());
    const query = @import("../query.zig");
    try std.testing.expectError(error.RowPolicyUnsupported, query.executeStorageKernelGraphExpand(a, &database, "t", .{
        .name = @constCast("g"),
        .index_name = @constCast("g"),
        .frontier = &.{},
        .exclude_nodes = &.{},
        .exclude_edges = &.{},
        .params = .{},
    }));
    try std.testing.expectError(error.RowPolicyUnsupported, query.executeStorageKernelGraphHydrate(a, &database, .{ .keys = &.{}, .include_hits = false }));
    try std.testing.expectError(error.RowPolicyUnsupported, query.executeStorageKernelGraphEdges(a, &database, .{ .index_name = @constCast("g"), .key = @constCast("root"), .direction = .out }));
    try std.testing.expectError(error.RowPolicyUnsupported, database.graphHydrateKeysForInternalRead(a, .{}, &.{}));
    try std.testing.expectError(error.RowPolicyUnsupported, database.searchGraph(a, .{}, .{ .query_type = .neighbors, .index_name = "g", .start_nodes = .{ .keys = &.{"root"} } }, null));
    try raw.check();
    raw.release();
    try std.testing.expect(gate.quiesced());
}
