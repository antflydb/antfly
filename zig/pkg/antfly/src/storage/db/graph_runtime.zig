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
const builtin = @import("builtin");

const graph_mod = @import("../../graph/graph.zig");
const paths_mod = @import("../../graph/paths.zig");
const traversal_mod = @import("../../graph/traversal.zig");
const types = @import("types.zig");

const TestHelpers = if (builtin.is_test) @import("test_support.zig") else struct {};

test "db graph runtime helpers expose edges neighbors and shortest path" {
    const DB = @import("mod.zig").DB;
    const alloc = std.testing.allocator;

    var path_buf: [256]u8 = undefined;
    const path = TestHelpers.tempPath(&path_buf);
    defer TestHelpers.cleanupTempDir(path);

    var db = try DB.open(alloc, std.mem.span(path), .{});
    defer db.close();

    try db.addIndex(.{
        .name = "citations",
        .kind = .graph,
        .config_json = "{}",
    });
    try db.addIndex(.{
        .name = "citations_alg",
        .kind = .graph,
        .config_json = "{\"algebraic_planning\":{\"bounded_traversal\":{\"law\":\"provenance_semiring\"}}}",
    });

    try db.batch(.{
        .graph_writes = &.{
            .{ .index_name = "citations", .source = "a", .target = "b", .edge_type = "cites", .weight = 1.0 },
            .{ .index_name = "citations", .source = "a", .target = "c", .edge_type = "cites", .weight = 2.0 },
            .{ .index_name = "citations", .source = "b", .target = "d", .edge_type = "cites", .weight = 3.0 },
            .{ .index_name = "citations_alg", .source = "a", .target = "b", .edge_type = "cites", .weight = 1.0 },
            .{ .index_name = "citations_alg", .source = "a", .target = "c", .edge_type = "cites", .weight = 2.0 },
            .{ .index_name = "citations_alg", .source = "b", .target = "d", .edge_type = "cites", .weight = 3.0 },
        },
        .sync_level = .full_index,
    });

    const edges = try db.getEdges(alloc, "citations", "a", "", .out);
    defer graph_mod.GraphIndex.freeEdges(alloc, edges);
    try std.testing.expectEqual(@as(usize, 2), edges.len);

    const neighbors = try db.getNeighbors(alloc, "citations", "a", "cites", .out);
    defer traversal_mod.freeOwnedResults(alloc, neighbors);
    try std.testing.expectEqual(@as(usize, 2), neighbors.len);

    const traversed = try db.traverseEdges(alloc, "citations", "a", .{
        .direction = .out,
        .edge_types = &.{"cites"},
        .max_depth = 2,
    });
    defer traversal_mod.freeOwnedResults(alloc, traversed);
    try std.testing.expectEqual(@as(usize, 3), traversed.len);

    const shortest = (try db.findShortestPath(alloc, "citations", "a", "d", &.{"cites"}, .out, .min_hops, 8, 0, 0)).?;
    defer paths_mod.freePath(alloc, shortest);
    try std.testing.expectEqual(@as(u32, 2), shortest.length);
    try std.testing.expectEqual(@as(usize, 3), shortest.nodes.len);
    try std.testing.expectEqualStrings("a", shortest.nodes[0]);
    try std.testing.expectEqualStrings("d", shortest.nodes[2]);

    const algebraic_shortest = (try db.findShortestPath(alloc, "citations_alg", "a", "d", &.{"cites"}, .out, .min_hops, 8, 0, 0)).?;
    defer paths_mod.freePath(alloc, algebraic_shortest);
    try std.testing.expectEqual(@as(u32, 2), algebraic_shortest.length);
    try std.testing.expectEqual(@as(usize, 3), algebraic_shortest.nodes.len);
    try std.testing.expectEqualStrings("a", algebraic_shortest.nodes[0]);
    try std.testing.expectEqualStrings("b", algebraic_shortest.nodes[1]);
    try std.testing.expectEqualStrings("d", algebraic_shortest.nodes[2]);
    try std.testing.expectEqual(@as(usize, 2), algebraic_shortest.edges.len);
    try std.testing.expectEqualStrings("cites", algebraic_shortest.edges[0].edge_type);

    const algebraic_k_one = try db.findKShortestPaths(alloc, "citations_alg", "a", "d", 1, &.{"cites"}, .out, .min_hops, 8, 0, 0);
    defer paths_mod.freePaths(alloc, algebraic_k_one);
    try std.testing.expectEqual(@as(usize, 1), algebraic_k_one.len);
    try std.testing.expectEqual(@as(u32, 2), algebraic_k_one[0].length);
    try std.testing.expectEqualStrings("d", algebraic_k_one[0].nodes[2]);

    const stats = try db.stats(alloc);
    defer types.freeDBStats(alloc, stats);
    var found_alg_stats = false;
    for (stats.indexes) |item| {
        if (!std.mem.eql(u8, item.name, "citations_alg")) continue;
        found_alg_stats = true;
        try std.testing.expect(item.algebraic_graph_traversal_attempt_count > 0);
        try std.testing.expect(item.algebraic_graph_traversal_proven_count > 0);
        try std.testing.expect(item.algebraic_graph_traversal_result_node_count > 0);
    }
    try std.testing.expect(found_alg_stats);
}

test "db graph runtime algebraic shortest path applies exact min-hop edge weight filters" {
    const DB = @import("mod.zig").DB;
    const alloc = std.testing.allocator;

    var path_buf: [256]u8 = undefined;
    const path = TestHelpers.tempPath(&path_buf);
    defer TestHelpers.cleanupTempDir(path);

    var db = try DB.open(alloc, std.mem.span(path), .{});
    defer db.close();

    try db.addIndex(.{
        .name = "citations_alg",
        .kind = .graph,
        .config_json = "{\"algebraic_planning\":{\"bounded_traversal\":{\"law\":\"provenance_semiring\"}}}",
    });

    try db.batch(.{
        .graph_writes = &.{
            .{ .index_name = "citations_alg", .source = "a", .target = "b", .edge_type = "cites", .weight = 0.5 },
            .{ .index_name = "citations_alg", .source = "b", .target = "d", .edge_type = "cites", .weight = 2.0 },
            .{ .index_name = "citations_alg", .source = "a", .target = "c", .edge_type = "cites", .weight = 2.0 },
            .{ .index_name = "citations_alg", .source = "c", .target = "e", .edge_type = "cites", .weight = 2.0 },
            .{ .index_name = "citations_alg", .source = "e", .target = "d", .edge_type = "cites", .weight = 2.0 },
            .{ .index_name = "citations_alg", .source = "c", .target = "d", .edge_type = "cites", .weight = 5.0 },
        },
        .sync_level = .full_index,
    });

    const shortest = (try db.findShortestPath(alloc, "citations_alg", "a", "d", &.{"cites"}, .out, .min_hops, 4, 1.0, 3.0)).?;
    defer paths_mod.freePath(alloc, shortest);
    try std.testing.expectEqual(@as(u32, 3), shortest.length);
    try std.testing.expectEqual(@as(usize, 4), shortest.nodes.len);
    try std.testing.expectEqualStrings("a", shortest.nodes[0]);
    try std.testing.expectEqualStrings("c", shortest.nodes[1]);
    try std.testing.expectEqualStrings("e", shortest.nodes[2]);
    try std.testing.expectEqualStrings("d", shortest.nodes[3]);

    const algebraic_k_one = try db.findKShortestPaths(alloc, "citations_alg", "a", "d", 1, &.{"cites"}, .out, .min_hops, 4, 1.0, 3.0);
    defer paths_mod.freePaths(alloc, algebraic_k_one);
    try std.testing.expectEqual(@as(usize, 1), algebraic_k_one.len);
    try std.testing.expectEqual(@as(u32, 3), algebraic_k_one[0].length);
    try std.testing.expectEqualStrings("c", algebraic_k_one[0].nodes[1]);
    try std.testing.expectEqualStrings("d", algebraic_k_one[0].nodes[3]);
}
