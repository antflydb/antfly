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

//! BFS-based graph traversal engine.
//!
//! Matches Go antfly's TraverseEdges algorithm (db.go:5240-5384):
//!   - BFS with configurable depth, direction, edge type, and weight filters
//!   - Deduplication via visited set
//!   - Optional path tracking

const std = @import("std");
const Allocator = std.mem.Allocator;
const platform_time = @import("../platform/time.zig");
const graph_mod = @import("graph.zig");
const Edge = graph_mod.Edge;
const EdgeDirection = graph_mod.EdgeDirection;
const GraphIndex = graph_mod.GraphIndex;

// ============================================================================
// Traversal types
// ============================================================================

pub const TraversalRules = struct {
    edge_types: []const []const u8 = &.{}, // empty = all types
    direction: EdgeDirection = .out,
    max_depth: u32 = 3,
    min_weight: f64 = 0.0,
    max_weight: f64 = 0.0, // 0 = no upper limit
    max_results: u32 = 100,
    deduplicate: bool = true,
    include_paths: bool = false,
};

pub const TraversalResult = struct {
    key: []const u8,
    depth: u32,
    total_weight: f64,
    path: ?[]const []const u8, // if include_paths
};

// ============================================================================
// BFS traversal
// ============================================================================

const QueueEntry = struct {
    key: []const u8,
    depth: u32,
    total_weight: f64,
    path: ?std.ArrayListUnmanaged([]const u8),
};

/// Perform BFS graph traversal from start_key using the given rules.
/// Caller owns all returned memory (use freeResults to clean up).
pub fn traverse(alloc: Allocator, graph_index: *GraphIndex, start_key: []const u8, rules: TraversalRules) ![]TraversalResult {
    var results = std.ArrayListUnmanaged(TraversalResult).empty;
    errdefer {
        freeResults(alloc, results.items);
        results.deinit(alloc);
    }

    var visited = std.StringHashMapUnmanaged(void).empty;
    defer {
        var it = visited.keyIterator();
        while (it.next()) |k| alloc.free(k.*);
        visited.deinit(alloc);
    }

    // Queue
    var queue = std.ArrayListUnmanaged(QueueEntry).empty;
    defer queue.deinit(alloc);
    var queue_head: usize = 0;

    // Seed with start node
    const start_dup = try alloc.dupe(u8, start_key);
    var start_path: ?std.ArrayListUnmanaged([]const u8) = null;
    if (rules.include_paths) {
        start_path = std.ArrayListUnmanaged([]const u8).empty;
        try start_path.?.append(alloc, try alloc.dupe(u8, start_key));
    }

    try queue.append(alloc, .{
        .key = start_dup,
        .depth = 0,
        .total_weight = 1.0,
        .path = start_path,
    });

    if (rules.deduplicate) {
        try visited.put(alloc, try alloc.dupe(u8, start_key), {});
    }

    while (queue_head < queue.items.len) {
        // Dequeue from front (index-tracked)
        const current = queue.items[queue_head];
        queue_head += 1;
        defer alloc.free(current.key);

        // Add to results (skip depth 0 = start node)
        if (current.depth > 0) {
            var path_slice: ?[]const []const u8 = null;
            if (current.path) |p| {
                const duped = try alloc.alloc([]const u8, p.items.len);
                for (p.items, 0..) |s, i| {
                    duped[i] = try alloc.dupe(u8, s);
                }
                path_slice = duped;
            }
            try results.append(alloc, .{
                .key = try alloc.dupe(u8, current.key),
                .depth = current.depth,
                .total_weight = current.total_weight,
                .path = path_slice,
            });

            if (rules.max_results > 0 and results.items.len >= rules.max_results) {
                // Free remaining path from current
                if (current.path) |p| {
                    for (p.items) |s| alloc.free(s);
                    var mp = p;
                    mp.deinit(alloc);
                }
                // Free remaining queue entries (skip already-dequeued entries)
                for (queue.items[queue_head..]) |qe| {
                    alloc.free(qe.key);
                    if (qe.path) |p| {
                        for (p.items) |s| alloc.free(s);
                        var mp = p;
                        mp.deinit(alloc);
                    }
                }
                break;
            }
        }

        // Check max depth
        if (rules.max_depth > 0 and current.depth >= rules.max_depth) {
            if (current.path) |p| {
                for (p.items) |s| alloc.free(s);
                var mp = p;
                mp.deinit(alloc);
            }
            continue;
        }

        // Get edges
        const edges = try graph_index.getEdges(alloc, current.key, "", rules.direction);
        defer GraphIndex.freeEdges(alloc, edges);

        for (edges) |edge| {
            if (!shouldTraverseEdge(&rules, &edge)) continue;

            // Determine next node
            const next_key = if (std.mem.eql(u8, current.key, edge.source)) edge.target else edge.source;

            // Dedup check
            if (rules.deduplicate) {
                if (visited.contains(next_key)) continue;
                try visited.put(alloc, try alloc.dupe(u8, next_key), {});
            }

            // Build path for next node
            var next_path: ?std.ArrayListUnmanaged([]const u8) = null;
            if (rules.include_paths) {
                next_path = std.ArrayListUnmanaged([]const u8).empty;
                if (current.path) |cp| {
                    for (cp.items) |path_key| {
                        try next_path.?.append(alloc, try alloc.dupe(u8, path_key));
                    }
                }
                try next_path.?.append(alloc, try alloc.dupe(u8, next_key));
            }

            try queue.append(alloc, .{
                .key = try alloc.dupe(u8, next_key),
                .depth = current.depth + 1,
                .total_weight = current.total_weight * edge.weight,
                .path = next_path,
            });
        }

        // Free current's path
        if (current.path) |p| {
            for (p.items) |s| alloc.free(s);
            var mp = p;
            mp.deinit(alloc);
        }
    }

    const owned = try alloc.dupe(TraversalResult, results.items);
    results.deinit(alloc);
    return owned;
}

fn shouldTraverseEdge(rules: *const TraversalRules, edge: *const Edge) bool {
    // Weight filter
    if (rules.min_weight > 0 and edge.weight < rules.min_weight) return false;
    if (rules.max_weight > 0 and edge.weight > rules.max_weight) return false;

    // Edge type filter
    if (rules.edge_types.len > 0) {
        for (rules.edge_types) |et| {
            if (std.mem.eql(u8, edge.edge_type, et)) return true;
        }
        return false;
    }
    return true;
}

/// Free traversal results.
pub fn freeResults(alloc: Allocator, results: []const TraversalResult) void {
    for (results) |r| {
        alloc.free(r.key);
        if (r.path) |p| {
            for (p) |s| alloc.free(s);
            alloc.free(p);
        }
    }
}

/// Free results returned from traverse().
pub fn freeOwnedResults(alloc: Allocator, results: []TraversalResult) void {
    freeResults(alloc, results);
    alloc.free(results);
}

// ============================================================================
// Tests
// ============================================================================

const docstore = @import("../storage/docstore.zig");

fn tmpPath(buf: []u8, label: []const u8) [*:0]const u8 {
    const ns = platform_time.monotonicNs();
    const slice = std.fmt.bufPrint(buf, "/tmp/antfly-trav-{s}-{d}\x00", .{ label, ns }) catch unreachable;
    var io_impl = std.Io.Threaded.init(std.heap.page_allocator, .{});
    defer io_impl.deinit();
    std.Io.Dir.cwd().createDirPath(io_impl.io(), std.mem.span(@as([*:0]const u8, @ptrCast(slice.ptr)))) catch {};
    return @ptrCast(slice.ptr);
}

fn cleanupTmp(path: [*:0]const u8) void {
    var io_impl = std.Io.Threaded.init(std.heap.page_allocator, .{});
    defer io_impl.deinit();
    std.Io.Dir.cwd().deleteTree(io_impl.io(), std.mem.span(path)) catch {};
}

test "traversal basic BFS depth 1" {
    const alloc = std.testing.allocator;
    var sb: [256]u8 = undefined;
    const sp = tmpPath(&sb, "ts1");
    defer cleanupTmp(sp);
    var rb: [256]u8 = undefined;
    const rp = tmpPath(&rb, "tr1");
    defer cleanupTmp(rp);

    var store = try docstore.DocStore.open(alloc, sp, .{});
    defer store.close();
    var g = try GraphIndex.open(alloc, &store, rp, "test", .{});
    defer g.close();

    try g.addEdge("A", "B", "knows", 0.9, 0, 0, "");
    try g.addEdge("A", "C", "knows", 0.8, 0, 0, "");

    const results = try traverse(alloc, &g, "A", .{ .max_depth = 1 });
    defer freeOwnedResults(alloc, results);

    try std.testing.expectEqual(@as(usize, 2), results.len);
    for (results) |r| {
        try std.testing.expectEqual(@as(u32, 1), r.depth);
    }
}

test "traversal max_depth limiting" {
    const alloc = std.testing.allocator;
    var sb: [256]u8 = undefined;
    const sp = tmpPath(&sb, "ts2");
    defer cleanupTmp(sp);
    var rb: [256]u8 = undefined;
    const rp = tmpPath(&rb, "tr2");
    defer cleanupTmp(rp);

    var store = try docstore.DocStore.open(alloc, sp, .{});
    defer store.close();
    var g = try GraphIndex.open(alloc, &store, rp, "test", .{});
    defer g.close();

    // A -> B -> C -> D (chain)
    try g.addEdge("A", "B", "next", 1.0, 0, 0, "");
    try g.addEdge("B", "C", "next", 1.0, 0, 0, "");
    try g.addEdge("C", "D", "next", 1.0, 0, 0, "");

    // Depth 2: should reach B and C but not D
    const results = try traverse(alloc, &g, "A", .{ .max_depth = 2 });
    defer freeOwnedResults(alloc, results);

    try std.testing.expectEqual(@as(usize, 2), results.len);
    try std.testing.expectEqualStrings("B", results[0].key);
    try std.testing.expectEqual(@as(u32, 1), results[0].depth);
    try std.testing.expectEqualStrings("C", results[1].key);
    try std.testing.expectEqual(@as(u32, 2), results[1].depth);
}

test "traversal deduplication" {
    const alloc = std.testing.allocator;
    var sb: [256]u8 = undefined;
    const sp = tmpPath(&sb, "ts3");
    defer cleanupTmp(sp);
    var rb: [256]u8 = undefined;
    const rp = tmpPath(&rb, "tr3");
    defer cleanupTmp(rp);

    var store = try docstore.DocStore.open(alloc, sp, .{});
    defer store.close();
    var g = try GraphIndex.open(alloc, &store, rp, "test", .{});
    defer g.close();

    // Diamond: A -> B, A -> C, B -> D, C -> D
    try g.addEdge("A", "B", "e", 1.0, 0, 0, "");
    try g.addEdge("A", "C", "e", 1.0, 0, 0, "");
    try g.addEdge("B", "D", "e", 1.0, 0, 0, "");
    try g.addEdge("C", "D", "e", 1.0, 0, 0, "");

    const results = try traverse(alloc, &g, "A", .{ .max_depth = 3, .deduplicate = true });
    defer freeOwnedResults(alloc, results);

    // D should appear only once (dedup)
    try std.testing.expectEqual(@as(usize, 3), results.len); // B, C, D
}

test "traversal with path tracking" {
    const alloc = std.testing.allocator;
    var sb: [256]u8 = undefined;
    const sp = tmpPath(&sb, "ts4");
    defer cleanupTmp(sp);
    var rb: [256]u8 = undefined;
    const rp = tmpPath(&rb, "tr4");
    defer cleanupTmp(rp);

    var store = try docstore.DocStore.open(alloc, sp, .{});
    defer store.close();
    var g = try GraphIndex.open(alloc, &store, rp, "test", .{});
    defer g.close();

    try g.addEdge("A", "B", "e", 1.0, 0, 0, "");
    try g.addEdge("B", "C", "e", 1.0, 0, 0, "");

    const results = try traverse(alloc, &g, "A", .{
        .max_depth = 3,
        .include_paths = true,
    });
    defer freeOwnedResults(alloc, results);

    try std.testing.expectEqual(@as(usize, 2), results.len);

    // B's path: [A, B]
    const b_path = results[0].path.?;
    try std.testing.expectEqual(@as(usize, 2), b_path.len);
    try std.testing.expectEqualStrings("A", b_path[0]);
    try std.testing.expectEqualStrings("B", b_path[1]);

    // C's path: [A, B, C]
    const c_path = results[1].path.?;
    try std.testing.expectEqual(@as(usize, 3), c_path.len);
    try std.testing.expectEqualStrings("C", c_path[2]);
}

test "traversal weight filter" {
    const alloc = std.testing.allocator;
    var sb: [256]u8 = undefined;
    const sp = tmpPath(&sb, "ts5");
    defer cleanupTmp(sp);
    var rb: [256]u8 = undefined;
    const rp = tmpPath(&rb, "tr5");
    defer cleanupTmp(rp);

    var store = try docstore.DocStore.open(alloc, sp, .{});
    defer store.close();
    var g = try GraphIndex.open(alloc, &store, rp, "test", .{});
    defer g.close();

    try g.addEdge("A", "B", "e", 0.9, 0, 0, "");
    try g.addEdge("A", "C", "e", 0.3, 0, 0, "");

    const results = try traverse(alloc, &g, "A", .{
        .max_depth = 1,
        .min_weight = 0.5,
    });
    defer freeOwnedResults(alloc, results);

    // Only B (weight 0.9) should pass the min_weight filter
    try std.testing.expectEqual(@as(usize, 1), results.len);
    try std.testing.expectEqualStrings("B", results[0].key);
}
