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

threadlocal var last_diagnostic: ?Diagnostic = null;
threadlocal var operation_buf: [graph_query.max_identifier_bytes]u8 = undefined;

pub fn reset() void {
    last_diagnostic = null;
}

pub fn take() ?Diagnostic {
    const diagnostic = last_diagnostic;
    last_diagnostic = null;
    return diagnostic;
}

pub fn record(operation: []const u8, query: graph_query.GraphQuery, exhaustion: Exhaustion) void {
    if (operation.len == 0 or operation.len > operation_buf.len) {
        last_diagnostic = null;
        return;
    }
    @memcpy(operation_buf[0..operation.len], operation);
    last_diagnostic = .{
        .operation = operation_buf[0..operation.len],
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
