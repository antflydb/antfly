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

//! Shortest path algorithms for graph indexes.
//!
//! Matches Go antfly's graph_paths.go:
//!   - BFS shortest path (min_hops)
//!   - Dijkstra's algorithm (min_weight, max_weight)
//!   - Yen's k-shortest-paths
//!
//! Three weight modes:
//!   min_hops: unweighted BFS — hop count as distance
//!   min_weight: Dijkstra sum — minimize sum of edge weights
//!   max_weight: Dijkstra log — maximize product of edge weights via -log transform

const std = @import("std");
const Allocator = std.mem.Allocator;
const platform_time = @import("../platform/time.zig");
const graph_mod = @import("graph.zig");
const Edge = graph_mod.Edge;
const EdgeDirection = graph_mod.EdgeDirection;
const GraphIndex = graph_mod.GraphIndex;

// ============================================================================
// Types
// ============================================================================

pub const PathWeightMode = enum { min_hops, min_weight, max_weight };

pub const PathFindOptions = struct {
    weight_mode: PathWeightMode = .min_hops,
    edge_types: []const []const u8 = &.{},
    direction: EdgeDirection = .out,
    max_depth: u32 = 50,
    min_weight: f64 = 0.0,
    max_weight: f64 = 0.0, // 0 = no limit
};

pub const PathEdge = struct {
    source: []const u8,
    target: []const u8,
    edge_type: []const u8,
    weight: f64,
};

pub const Path = struct {
    nodes: [][]const u8,
    edges: []PathEdge,
    total_weight: f64,
    length: u32,
};

pub fn freePath(alloc: Allocator, path: Path) void {
    for (path.nodes) |n| alloc.free(n);
    alloc.free(path.nodes);
    for (path.edges) |e| {
        alloc.free(e.source);
        alloc.free(e.target);
        alloc.free(e.edge_type);
    }
    alloc.free(path.edges);
}

pub fn freePaths(alloc: Allocator, paths: []Path) void {
    for (paths) |p| freePath(alloc, p);
    alloc.free(paths);
}

// ============================================================================
// PathNode for priority queue
// ============================================================================

const PathNode = struct {
    key: []const u8, // owned
    distance: f64,
    hops: u32,
    parent: ?*PathNode,
    parent_edge: ?OwnedEdgeInfo,
};

const OwnedEdgeInfo = struct {
    source: []const u8,
    target: []const u8,
    edge_type: []const u8,
    weight: f64,
};

fn pathNodeLessThan(_: void, a: *PathNode, b: *PathNode) std.math.Order {
    if (a.distance < b.distance) return .lt;
    if (a.distance > b.distance) return .gt;
    return std.math.order(a.hops, b.hops);
}

// ============================================================================
// Find shortest path
// ============================================================================

/// Find the shortest path between source and target.
/// Returns null if no path exists. Caller owns the result (use freePath to clean up).
pub fn findShortestPath(
    alloc: Allocator,
    graph_index: *GraphIndex,
    source: []const u8,
    target: []const u8,
    opts: PathFindOptions,
) !?Path {
    return findShortestPathWithExclusions(alloc, graph_index, source, target, opts, null, null);
}

/// Find shortest path with optional node/edge exclusions (used by Yen's algorithm).
pub fn findShortestPathWithExclusions(
    alloc: Allocator,
    graph_index: *GraphIndex,
    source: []const u8,
    target: []const u8,
    opts: PathFindOptions,
    excluded_nodes: ?*const std.StringHashMapUnmanaged(void),
    excluded_edges: ?*const std.StringHashMapUnmanaged(void),
) !?Path {
    // Same source and target — trivial path
    if (std.mem.eql(u8, source, target)) {
        const node = try alloc.dupe(u8, source);
        const nodes = try alloc.alloc([]const u8, 1);
        nodes[0] = node;
        return Path{
            .nodes = nodes,
            .edges = try alloc.alloc(PathEdge, 0),
            .total_weight = 0.0,
            .length = 0,
        };
    }

    if (opts.weight_mode == .min_hops) {
        return bfsShortestPath(alloc, graph_index, source, target, opts, excluded_nodes, excluded_edges);
    } else {
        return dijkstraPath(alloc, graph_index, source, target, opts, excluded_nodes, excluded_edges);
    }
}

// ============================================================================
// BFS shortest path (min_hops)
// ============================================================================

fn bfsShortestPath(
    alloc: Allocator,
    graph_index: *GraphIndex,
    source: []const u8,
    target: []const u8,
    opts: PathFindOptions,
    excluded_nodes: ?*const std.StringHashMapUnmanaged(void),
    excluded_edges: ?*const std.StringHashMapUnmanaged(void),
) !?Path {
    // Arena for PathNodes — freed at end
    var node_pool = std.ArrayListUnmanaged(*PathNode).empty;
    defer {
        for (node_pool.items) |n| {
            alloc.free(n.key);
            if (n.parent_edge) |e| {
                alloc.free(e.source);
                alloc.free(e.target);
                alloc.free(e.edge_type);
            }
            alloc.destroy(n);
        }
        node_pool.deinit(alloc);
    }

    var visited = std.StringHashMapUnmanaged(void).empty;
    defer {
        var it = visited.keyIterator();
        while (it.next()) |k| alloc.free(k.*);
        visited.deinit(alloc);
    }

    var queue = std.ArrayListUnmanaged(*PathNode).empty;
    defer queue.deinit(alloc);
    var queue_head: usize = 0;

    // Seed
    const start = try alloc.create(PathNode);
    start.* = .{
        .key = try alloc.dupe(u8, source),
        .distance = 0,
        .hops = 0,
        .parent = null,
        .parent_edge = null,
    };
    try node_pool.append(alloc, start);
    try queue.append(alloc, start);
    try visited.put(alloc, try alloc.dupe(u8, source), {});

    while (queue_head < queue.items.len) {
        const current = queue.items[queue_head];
        queue_head += 1;

        if (opts.max_depth > 0 and current.hops >= opts.max_depth) continue;

        const edges = try graph_index.getEdges(alloc, current.key, "", opts.direction);
        defer GraphIndex.freeEdges(alloc, edges);

        for (edges) |edge| {
            if (!shouldTraverseEdge(opts, &edge)) continue;

            const next_key = if (std.mem.eql(u8, current.key, edge.source)) edge.target else edge.source;

            // Check exclusions
            if (excluded_nodes) |en| {
                if (en.contains(next_key)) continue;
            }
            if (excluded_edges) |ee| {
                var edge_key_buf: [2048]u8 = undefined;
                const ek = makeEdgeExclusionKey(&edge_key_buf, edge.source, edge.target, edge.edge_type);
                if (ee.contains(ek)) continue;
            }

            if (visited.contains(next_key)) continue;
            try visited.put(alloc, try alloc.dupe(u8, next_key), {});

            const node = try alloc.create(PathNode);
            node.* = .{
                .key = try alloc.dupe(u8, next_key),
                .distance = @floatFromInt(current.hops + 1),
                .hops = current.hops + 1,
                .parent = current,
                .parent_edge = .{
                    .source = try alloc.dupe(u8, edge.source),
                    .target = try alloc.dupe(u8, edge.target),
                    .edge_type = try alloc.dupe(u8, edge.edge_type),
                    .weight = edge.weight,
                },
            };
            try node_pool.append(alloc, node);

            // Found target — reconstruct path
            if (std.mem.eql(u8, next_key, target)) {
                return try reconstructPath(alloc, node);
            }

            try queue.append(alloc, node);
        }
    }

    return null;
}

// ============================================================================
// Dijkstra (min_weight and max_weight)
// ============================================================================

fn dijkstraPath(
    alloc: Allocator,
    graph_index: *GraphIndex,
    source: []const u8,
    target: []const u8,
    opts: PathFindOptions,
    excluded_nodes: ?*const std.StringHashMapUnmanaged(void),
    excluded_edges: ?*const std.StringHashMapUnmanaged(void),
) !?Path {
    var node_pool = std.ArrayListUnmanaged(*PathNode).empty;
    defer {
        for (node_pool.items) |n| {
            alloc.free(n.key);
            if (n.parent_edge) |e| {
                alloc.free(e.source);
                alloc.free(e.target);
                alloc.free(e.edge_type);
            }
            alloc.destroy(n);
        }
        node_pool.deinit(alloc);
    }

    // Best distance seen per node
    var best_dist = std.StringHashMapUnmanaged(f64).empty;
    defer {
        var it = best_dist.keyIterator();
        while (it.next()) |k| alloc.free(k.*);
        best_dist.deinit(alloc);
    }

    // Priority queue
    var heap = std.PriorityQueue(*PathNode, void, pathNodeLessThan).initContext({});
    defer heap.deinit(alloc);

    const start = try alloc.create(PathNode);
    start.* = .{
        .key = try alloc.dupe(u8, source),
        .distance = 0.0,
        .hops = 0,
        .parent = null,
        .parent_edge = null,
    };
    try node_pool.append(alloc, start);
    try heap.push(alloc, start);
    try best_dist.put(alloc, try alloc.dupe(u8, source), 0.0);

    while (heap.pop()) |current| {
        // Found target
        if (std.mem.eql(u8, current.key, target)) {
            return try reconstructPath(alloc, current);
        }

        if (opts.max_depth > 0 and current.hops >= opts.max_depth) continue;

        // Skip if we've already found a better path to this node
        if (best_dist.get(current.key)) |bd| {
            if (current.distance > bd) continue;
        }

        const edges = try graph_index.getEdges(alloc, current.key, "", opts.direction);
        defer GraphIndex.freeEdges(alloc, edges);

        for (edges) |edge| {
            if (!shouldTraverseEdge(opts, &edge)) continue;

            const next_key = if (std.mem.eql(u8, current.key, edge.source)) edge.target else edge.source;

            if (excluded_nodes) |en| {
                if (en.contains(next_key)) continue;
            }
            if (excluded_edges) |ee| {
                var edge_key_buf: [2048]u8 = undefined;
                const ek = makeEdgeExclusionKey(&edge_key_buf, edge.source, edge.target, edge.edge_type);
                if (ee.contains(ek)) continue;
            }

            const new_dist = switch (opts.weight_mode) {
                .min_weight => current.distance + edge.weight,
                .max_weight => blk: {
                    if (edge.weight <= 0.0) continue; // log(0) undefined
                    break :blk current.distance + (-@log(edge.weight));
                },
                .min_hops => unreachable,
            };

            const existing = best_dist.get(next_key);
            if (existing == null or new_dist < existing.?) {
                if (existing == null) {
                    try best_dist.put(alloc, try alloc.dupe(u8, next_key), new_dist);
                } else {
                    best_dist.getPtr(next_key).?.* = new_dist;
                }

                const node = try alloc.create(PathNode);
                node.* = .{
                    .key = try alloc.dupe(u8, next_key),
                    .distance = new_dist,
                    .hops = current.hops + 1,
                    .parent = current,
                    .parent_edge = .{
                        .source = try alloc.dupe(u8, edge.source),
                        .target = try alloc.dupe(u8, edge.target),
                        .edge_type = try alloc.dupe(u8, edge.edge_type),
                        .weight = edge.weight,
                    },
                };
                try node_pool.append(alloc, node);
                try heap.push(alloc, node);
            }
        }
    }

    return null;
}

// ============================================================================
// Yen's k-shortest-paths
// ============================================================================

/// Find up to k shortest paths between source and target.
/// Caller owns the returned slice (use freePaths to clean up).
pub fn findKShortestPaths(
    alloc: Allocator,
    graph_index: *GraphIndex,
    source: []const u8,
    target: []const u8,
    k: u32,
    opts: PathFindOptions,
) ![]Path {
    if (k == 0) return try alloc.alloc(Path, 0);

    var results = std.ArrayListUnmanaged(Path).empty;
    errdefer {
        for (results.items) |p| freePath(alloc, p);
        results.deinit(alloc);
    }

    // Find first shortest path
    const first = try findShortestPath(alloc, graph_index, source, target, opts);
    if (first == null) return try alloc.alloc(Path, 0);
    try results.append(alloc, first.?);

    if (k == 1) {
        const owned = try alloc.dupe(Path, results.items);
        results.deinit(alloc);
        return owned;
    }

    // Candidates sorted by total_weight
    var candidates = std.ArrayListUnmanaged(Path).empty;
    defer {
        for (candidates.items) |p| freePath(alloc, p);
        candidates.deinit(alloc);
    }

    // Track seen paths for deduplication
    var seen_paths = std.StringHashMapUnmanaged(void).empty;
    defer {
        var it = seen_paths.keyIterator();
        while (it.next()) |key| alloc.free(key.*);
        seen_paths.deinit(alloc);
    }

    // Mark first path as seen
    const first_key = try pathToKey(alloc, &results.items[0]);
    try seen_paths.put(alloc, first_key, {});

    var ki: u32 = 1;
    while (ki < k) : (ki += 1) {
        const prev_path = &results.items[results.items.len - 1];

        // For each spur node in the previous path
        for (0..prev_path.nodes.len - 1) |spur_idx| {
            const spur_node = prev_path.nodes[spur_idx];

            // Build exclusion sets
            var excluded_edges = std.StringHashMapUnmanaged(void).empty;
            defer {
                var eit = excluded_edges.keyIterator();
                while (eit.next()) |ek| alloc.free(ek.*);
                excluded_edges.deinit(alloc);
            }

            var excluded_nodes = std.StringHashMapUnmanaged(void).empty;
            defer {
                var nit = excluded_nodes.keyIterator();
                while (nit.next()) |nk| alloc.free(nk.*);
                excluded_nodes.deinit(alloc);
            }

            // For each existing result path, if the root path matches,
            // exclude the edge from spur node to next node
            for (results.items) |result_path| {
                if (result_path.nodes.len <= spur_idx + 1) continue;
                if (!rootPathMatches(prev_path, &result_path, spur_idx)) continue;

                var edge_key_buf: [2048]u8 = undefined;
                const ek = makeEdgeExclusionKey(
                    &edge_key_buf,
                    result_path.nodes[spur_idx],
                    result_path.nodes[spur_idx + 1],
                    if (result_path.edges.len > spur_idx) result_path.edges[spur_idx].edge_type else "",
                );
                if (!excluded_edges.contains(ek)) {
                    try excluded_edges.put(alloc, try alloc.dupe(u8, ek), {});
                }
            }

            // Exclude root path nodes (except spur node)
            for (0..spur_idx) |i| {
                const node_key = prev_path.nodes[i];
                if (!excluded_nodes.contains(node_key)) {
                    try excluded_nodes.put(alloc, try alloc.dupe(u8, node_key), {});
                }
            }

            // Find spur path
            const spur_path = try findShortestPathWithExclusions(
                alloc,
                graph_index,
                spur_node,
                target,
                opts,
                &excluded_nodes,
                &excluded_edges,
            );

            if (spur_path) |sp| {
                // Build total path = root[0..spur_idx] + spur_path
                const total_path = try joinPaths(alloc, prev_path, spur_idx, &sp);
                freePath(alloc, sp);

                const pkey = try pathToKey(alloc, &total_path);
                if (!seen_paths.contains(pkey)) {
                    try seen_paths.put(alloc, pkey, {});
                    try candidates.append(alloc, total_path);
                } else {
                    alloc.free(pkey);
                    freePath(alloc, total_path);
                }
            }
        }

        if (candidates.items.len == 0) break;

        // Find best candidate (lowest total_weight)
        var best_idx: usize = 0;
        for (candidates.items[1..], 1..) |c, i| {
            if (c.total_weight < candidates.items[best_idx].total_weight) {
                best_idx = i;
            }
        }

        // Move best to results
        const best = candidates.orderedRemove(best_idx);
        try results.append(alloc, best);
    }

    const owned = try alloc.dupe(Path, results.items);
    results.deinit(alloc);
    return owned;
}

// ============================================================================
// Helpers
// ============================================================================

fn shouldTraverseEdge(opts: PathFindOptions, edge: *const Edge) bool {
    if (opts.min_weight > 0 and edge.weight < opts.min_weight) return false;
    if (opts.max_weight > 0 and edge.weight > opts.max_weight) return false;
    if (opts.edge_types.len > 0) {
        for (opts.edge_types) |et| {
            if (std.mem.eql(u8, edge.edge_type, et)) return true;
        }
        return false;
    }
    return true;
}

fn reconstructPath(alloc: Allocator, end_node: *PathNode) !Path {
    // Count path length
    var count: u32 = 0;
    var n: ?*PathNode = end_node;
    while (n) |node| : (n = node.parent) {
        count += 1;
    }

    const nodes = try alloc.alloc([]const u8, count);
    errdefer alloc.free(nodes);
    const edge_count = if (count > 0) count - 1 else 0;
    const path_edges = try alloc.alloc(PathEdge, edge_count);
    errdefer alloc.free(path_edges);

    // Fill in reverse
    var idx = count;
    n = end_node;
    var total_weight: f64 = 0.0;
    while (n) |node| : (n = node.parent) {
        idx -= 1;
        nodes[idx] = try alloc.dupe(u8, node.key);
        if (node.parent_edge) |pe| {
            // Edge array is 1 shorter than node array; idx >= 1 when parent_edge exists
            path_edges[idx - 1] = .{
                .source = try alloc.dupe(u8, pe.source),
                .target = try alloc.dupe(u8, pe.target),
                .edge_type = try alloc.dupe(u8, pe.edge_type),
                .weight = pe.weight,
            };
            total_weight += pe.weight;
        }
    }

    return Path{
        .nodes = nodes,
        .edges = path_edges,
        .total_weight = total_weight,
        .length = edge_count,
    };
}

fn makeEdgeExclusionKey(buf: []u8, src: []const u8, tgt: []const u8, edge_type: []const u8) []const u8 {
    const slice = std.fmt.bufPrint(buf, "{s}->{s}:{s}", .{ src, tgt, edge_type }) catch return "";
    return slice;
}

fn pathToKey(alloc: Allocator, path: *const Path) ![]u8 {
    var total_len: usize = 0;
    for (path.nodes) |n| total_len += n.len + 2; // "->".len
    if (total_len >= 2) total_len -= 2; // no trailing ->

    var buf = try alloc.alloc(u8, total_len);
    var pos: usize = 0;
    for (path.nodes, 0..) |n, i| {
        @memcpy(buf[pos..][0..n.len], n);
        pos += n.len;
        if (i < path.nodes.len - 1) {
            buf[pos] = '-';
            buf[pos + 1] = '>';
            pos += 2;
        }
    }
    return buf;
}

fn rootPathMatches(a: *const Path, b: *const Path, up_to: usize) bool {
    if (a.nodes.len <= up_to or b.nodes.len <= up_to) return false;
    for (0..up_to + 1) |i| {
        if (!std.mem.eql(u8, a.nodes[i], b.nodes[i])) return false;
    }
    return true;
}

fn joinPaths(alloc: Allocator, root: *const Path, spur_idx: usize, spur: *const Path) !Path {
    // root[0..spur_idx] + spur[0..]
    const root_node_count = spur_idx;
    const total_nodes = root_node_count + spur.nodes.len;
    const total_edges = root_node_count + spur.edges.len;

    const nodes = try alloc.alloc([]const u8, total_nodes);
    errdefer alloc.free(nodes);
    const edges = try alloc.alloc(PathEdge, total_edges);
    errdefer alloc.free(edges);

    // Copy root nodes and edges up to spur_idx
    for (0..root_node_count) |i| {
        nodes[i] = try alloc.dupe(u8, root.nodes[i]);
        edges[i] = .{
            .source = try alloc.dupe(u8, root.edges[i].source),
            .target = try alloc.dupe(u8, root.edges[i].target),
            .edge_type = try alloc.dupe(u8, root.edges[i].edge_type),
            .weight = root.edges[i].weight,
        };
    }

    // Copy spur path
    for (spur.nodes, 0..) |n, i| {
        nodes[root_node_count + i] = try alloc.dupe(u8, n);
    }
    for (spur.edges, 0..) |e, i| {
        edges[root_node_count + i] = .{
            .source = try alloc.dupe(u8, e.source),
            .target = try alloc.dupe(u8, e.target),
            .edge_type = try alloc.dupe(u8, e.edge_type),
            .weight = e.weight,
        };
    }

    var tw: f64 = 0.0;
    for (edges) |e| tw += e.weight;

    return Path{
        .nodes = nodes,
        .edges = edges,
        .total_weight = tw,
        .length = @intCast(total_edges),
    };
}

// ============================================================================
// Tests
// ============================================================================

const docstore = @import("../storage/docstore.zig");

fn tmpPath(buf: []u8, label: []const u8) [*:0]const u8 {
    const ns = platform_time.monotonicNs();
    const slice = std.fmt.bufPrint(buf, "/tmp/antfly-path-{s}-{d}\x00", .{ label, ns }) catch unreachable;
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

test "shortest path min_hops" {
    const alloc = std.testing.allocator;
    var sb1: [256]u8 = undefined;
    const sp = tmpPath(&sb1, "ph1s");
    defer cleanupTmp(sp);
    var rb1: [256]u8 = undefined;
    const rp = tmpPath(&rb1, "ph1r");
    defer cleanupTmp(rp);

    var store = try docstore.DocStore.open(alloc, sp, .{});
    defer store.close();
    var g = try GraphIndex.open(alloc, &store, rp, "test", .{});
    defer g.close();

    try g.addEdge("A", "B", "e", 10.0, 0, 0, "");
    try g.addEdge("B", "C", "e", 10.0, 0, 0, "");
    try g.addEdge("A", "D", "e", 1.0, 0, 0, "");
    try g.addEdge("D", "E", "e", 1.0, 0, 0, "");
    try g.addEdge("E", "C", "e", 1.0, 0, 0, "");

    const path = try findShortestPath(alloc, &g, "A", "C", .{ .weight_mode = .min_hops });
    try std.testing.expect(path != null);
    defer freePath(alloc, path.?);

    try std.testing.expectEqual(@as(u32, 2), path.?.length);
    try std.testing.expectEqual(@as(usize, 3), path.?.nodes.len);
    try std.testing.expectEqualStrings("A", path.?.nodes[0]);
    try std.testing.expectEqualStrings("B", path.?.nodes[1]);
    try std.testing.expectEqualStrings("C", path.?.nodes[2]);
}

test "shortest path min_weight" {
    const alloc = std.testing.allocator;
    var sb1: [256]u8 = undefined;
    const sp = tmpPath(&sb1, "ph2s");
    defer cleanupTmp(sp);
    var rb1: [256]u8 = undefined;
    const rp = tmpPath(&rb1, "ph2r");
    defer cleanupTmp(rp);

    var store = try docstore.DocStore.open(alloc, sp, .{});
    defer store.close();
    var g = try GraphIndex.open(alloc, &store, rp, "test", .{});
    defer g.close();

    try g.addEdge("A", "B", "e", 10.0, 0, 0, "");
    try g.addEdge("B", "C", "e", 10.0, 0, 0, "");
    try g.addEdge("A", "D", "e", 1.0, 0, 0, "");
    try g.addEdge("D", "E", "e", 1.0, 0, 0, "");
    try g.addEdge("E", "C", "e", 1.0, 0, 0, "");

    const path = try findShortestPath(alloc, &g, "A", "C", .{ .weight_mode = .min_weight });
    try std.testing.expect(path != null);
    defer freePath(alloc, path.?);

    try std.testing.expectEqual(@as(u32, 3), path.?.length);
    try std.testing.expectEqual(@as(usize, 4), path.?.nodes.len);
    try std.testing.expectEqualStrings("A", path.?.nodes[0]);
    try std.testing.expectEqualStrings("D", path.?.nodes[1]);
}

test "shortest path max_weight" {
    const alloc = std.testing.allocator;
    var sb1: [256]u8 = undefined;
    const sp = tmpPath(&sb1, "ph3s");
    defer cleanupTmp(sp);
    var rb1: [256]u8 = undefined;
    const rp = tmpPath(&rb1, "ph3r");
    defer cleanupTmp(rp);

    var store = try docstore.DocStore.open(alloc, sp, .{});
    defer store.close();
    var g = try GraphIndex.open(alloc, &store, rp, "test", .{});
    defer g.close();

    try g.addEdge("A", "B", "e", 0.9, 0, 0, "");
    try g.addEdge("B", "C", "e", 0.9, 0, 0, "");
    try g.addEdge("A", "D", "e", 0.5, 0, 0, "");
    try g.addEdge("D", "C", "e", 0.5, 0, 0, "");

    const path = try findShortestPath(alloc, &g, "A", "C", .{ .weight_mode = .max_weight });
    try std.testing.expect(path != null);
    defer freePath(alloc, path.?);

    try std.testing.expectEqual(@as(u32, 2), path.?.length);
    try std.testing.expectEqualStrings("B", path.?.nodes[1]);
}

test "shortest path no path" {
    const alloc = std.testing.allocator;
    var sb1: [256]u8 = undefined;
    const sp = tmpPath(&sb1, "ph4s");
    defer cleanupTmp(sp);
    var rb1: [256]u8 = undefined;
    const rp = tmpPath(&rb1, "ph4r");
    defer cleanupTmp(rp);

    var store = try docstore.DocStore.open(alloc, sp, .{});
    defer store.close();
    var g = try GraphIndex.open(alloc, &store, rp, "test", .{});
    defer g.close();

    try g.addEdge("A", "B", "e", 1.0, 0, 0, "");

    const path = try findShortestPath(alloc, &g, "A", "C", .{});
    try std.testing.expect(path == null);
}

test "shortest path same node" {
    const alloc = std.testing.allocator;
    var sb1: [256]u8 = undefined;
    const sp = tmpPath(&sb1, "ph5s");
    defer cleanupTmp(sp);
    var rb1: [256]u8 = undefined;
    const rp = tmpPath(&rb1, "ph5r");
    defer cleanupTmp(rp);

    var store = try docstore.DocStore.open(alloc, sp, .{});
    defer store.close();
    var g = try GraphIndex.open(alloc, &store, rp, "test", .{});
    defer g.close();

    const path = try findShortestPath(alloc, &g, "A", "A", .{});
    try std.testing.expect(path != null);
    defer freePath(alloc, path.?);

    try std.testing.expectEqual(@as(u32, 0), path.?.length);
    try std.testing.expectEqual(@as(usize, 1), path.?.nodes.len);
    try std.testing.expectEqualStrings("A", path.?.nodes[0]);
}

test "shortest path max_depth" {
    const alloc = std.testing.allocator;
    var sb1: [256]u8 = undefined;
    const sp = tmpPath(&sb1, "ph6s");
    defer cleanupTmp(sp);
    var rb1: [256]u8 = undefined;
    const rp = tmpPath(&rb1, "ph6r");
    defer cleanupTmp(rp);

    var store = try docstore.DocStore.open(alloc, sp, .{});
    defer store.close();
    var g = try GraphIndex.open(alloc, &store, rp, "test", .{});
    defer g.close();

    try g.addEdge("A", "B", "e", 1.0, 0, 0, "");
    try g.addEdge("B", "C", "e", 1.0, 0, 0, "");
    try g.addEdge("C", "D", "e", 1.0, 0, 0, "");

    const path = try findShortestPath(alloc, &g, "A", "D", .{ .max_depth = 2 });
    try std.testing.expect(path == null);

    const path2 = try findShortestPath(alloc, &g, "A", "D", .{ .max_depth = 3 });
    try std.testing.expect(path2 != null);
    defer freePath(alloc, path2.?);
    try std.testing.expectEqual(@as(u32, 3), path2.?.length);
}

test "shortest path edge type filter" {
    const alloc = std.testing.allocator;
    var sb1: [256]u8 = undefined;
    const sp = tmpPath(&sb1, "ph7s");
    defer cleanupTmp(sp);
    var rb1: [256]u8 = undefined;
    const rp = tmpPath(&rb1, "ph7r");
    defer cleanupTmp(rp);

    var store = try docstore.DocStore.open(alloc, sp, .{});
    defer store.close();
    var g = try GraphIndex.open(alloc, &store, rp, "test", .{});
    defer g.close();

    try g.addEdge("A", "B", "knows", 1.0, 0, 0, "");
    try g.addEdge("B", "C", "likes", 1.0, 0, 0, "");
    try g.addEdge("A", "D", "knows", 1.0, 0, 0, "");
    try g.addEdge("D", "C", "knows", 1.0, 0, 0, "");

    const et: []const []const u8 = &.{"knows"};
    const path = try findShortestPath(alloc, &g, "A", "C", .{
        .edge_types = et,
    });
    try std.testing.expect(path != null);
    defer freePath(alloc, path.?);

    try std.testing.expectEqual(@as(u32, 2), path.?.length);
    try std.testing.expectEqualStrings("D", path.?.nodes[1]);
}

test "k shortest paths" {
    const alloc = std.testing.allocator;
    var sb1: [256]u8 = undefined;
    const sp = tmpPath(&sb1, "ph8s");
    defer cleanupTmp(sp);
    var rb1: [256]u8 = undefined;
    const rp = tmpPath(&rb1, "ph8r");
    defer cleanupTmp(rp);

    var store = try docstore.DocStore.open(alloc, sp, .{});
    defer store.close();
    var g = try GraphIndex.open(alloc, &store, rp, "test", .{});
    defer g.close();

    try g.addEdge("A", "B", "e", 1.0, 0, 0, "");
    try g.addEdge("B", "C", "e", 1.0, 0, 0, "");
    try g.addEdge("A", "D", "e", 2.0, 0, 0, "");
    try g.addEdge("D", "C", "e", 2.0, 0, 0, "");
    try g.addEdge("A", "E", "e", 3.0, 0, 0, "");
    try g.addEdge("E", "C", "e", 3.0, 0, 0, "");

    const found_paths = try findKShortestPaths(alloc, &g, "A", "C", 3, .{
        .weight_mode = .min_weight,
    });
    defer freePaths(alloc, found_paths);

    try std.testing.expectEqual(@as(usize, 3), found_paths.len);
    try std.testing.expectEqual(@as(u32, 2), found_paths[0].length);
    try std.testing.expect(found_paths[0].total_weight <= found_paths[1].total_weight);
    try std.testing.expect(found_paths[1].total_weight <= found_paths[2].total_weight);
}
