// Copyright 2026 Antfly, Inc.
//
// Licensed under the Elastic License 2.0 (ELv2); you may not use this file
// except in compliance with the Elastic License 2.0. You may obtain a copy of
// the License at https://www.antfly.io/licensing/ELv2-license.

const std = @import("std");
const graph_query = @import("query.zig");
const work_budget = @import("work_budget.zig");

pub const Dimension = work_budget.Dimension;
pub const Exhaustion = work_budget.Exhaustion;

pub const Diagnostic = struct {
    operation: []const u8,
    mode: []const u8,
    dimension: Dimension,
    maximum: usize,
};

pub const Storage = struct {
    diagnostic: ?Diagnostic = null,
    operation_buf: [graph_query.max_identifier_bytes]u8 = undefined,
};

threadlocal var active_storage: ?*Storage = null;

pub const Binding = struct {
    previous: ?*Storage,

    pub fn deinit(self: Binding) void {
        active_storage = self.previous;
    }
};

pub fn bind(storage: *Storage) Binding {
    const previous = active_storage;
    active_storage = storage;
    return .{ .previous = previous };
}

pub fn reset() void {
    if (active_storage) |storage| storage.diagnostic = null;
}

pub fn take() ?Diagnostic {
    const storage = active_storage orelse return null;
    const diagnostic = storage.diagnostic;
    storage.diagnostic = null;
    return diagnostic;
}

pub fn record(operation: []const u8, query: graph_query.GraphQuery, exhaustion: Exhaustion) void {
    const storage = active_storage orelse return;
    if (operation.len == 0 or operation.len > storage.operation_buf.len) {
        storage.diagnostic = null;
        return;
    }
    @memcpy(storage.operation_buf[0..operation.len], operation);
    storage.diagnostic = .{
        .operation = storage.operation_buf[0..operation.len],
        .mode = mode(query),
        .dimension = exhaustion.dimension,
        .maximum = exhaustion.maximum,
    };
}

pub fn dimensionName(dimension: Dimension) []const u8 {
    return switch (dimension) {
        .explored_nodes => "explored_nodes",
        .explored_edges => "explored_edges",
        .explored_edge_bytes => "explored_edge_bytes",
        .scanned_anchors => "scanned_anchors",
        .intermediate_states => "intermediate_states",
        .retained_state_bytes => "retained_state_bytes",
    };
}

fn mode(query: graph_query.GraphQuery) []const u8 {
    return switch (query.query_type) {
        .neighbors => "neighbors",
        .traverse => "traverse",
        .shortest_path => "shortest_path",
        .k_shortest_paths => "k_shortest_paths",
        .pattern => if (query.match_pattern != null) "match" else "pattern",
    };
}

test "work budget diagnostic owns the operation name" {
    var storage: Storage = .{};
    const binding = bind(&storage);
    defer binding.deinit();
    var operation = [_]u8{ 'm', 'a', 't', 'c', 'h' };
    record(&operation, .{ .query_type = .pattern, .index_name = "graph", .start_nodes = .{ .keys = &.{} }, .match_pattern = .{ .nodes = &.{}, .edges = &.{} } }, .{
        .dimension = .explored_edges,
        .maximum = 100,
    });
    @memset(&operation, 'x');
    const diagnostic = take().?;
    try std.testing.expectEqualStrings("match", diagnostic.operation);
    try std.testing.expectEqualStrings("match", diagnostic.mode);
    try std.testing.expectEqual(Dimension.explored_edges, diagnostic.dimension);
    try std.testing.expectEqual(@as(usize, 100), diagnostic.maximum);
}
