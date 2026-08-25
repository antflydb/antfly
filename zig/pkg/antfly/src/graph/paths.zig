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
//!   - Depth-aware Dijkstra's algorithm (min_weight, max_weight)
//!   - Yen's k-shortest-paths
//!
//! Three weight modes:
//!   min_hops: unweighted BFS — hop count as distance
//!   min_weight: Dijkstra sum — minimize sum of non-negative edge weights
//!   max_weight: Dijkstra log — maximize product of edge weights in [0, 1]

const std = @import("std");
const Allocator = std.mem.Allocator;
const platform_time = @import("antfly_platform").time;
const graph_mod = @import("graph.zig");
const Edge = graph_mod.Edge;
const EdgeDirection = graph_mod.EdgeDirection;
const GraphIndex = graph_mod.GraphIndex;
const NodeAdmission = @import("node_admission.zig").NodeAdmission;
const NodeRef = @import("node_admission.zig").NodeRef;
const traversal_mod = @import("traversal.zig");
const work_budget_mod = @import("work_budget.zig");

const GraphIndexEdgeReader = struct {
    graph_index: *GraphIndex,

    pub fn getEdges(self: @This(), alloc: Allocator, key: []const u8, direction: EdgeDirection) ![]Edge {
        return try self.graph_index.getEdges(alloc, key, "", direction);
    }

    pub fn getEdgesBoundedForPath(
        self: @This(),
        alloc: Allocator,
        key: []const u8,
        edge_types: []const []const u8,
        direction: EdgeDirection,
        max_edges: usize,
        max_bytes: usize,
    ) ![]Edge {
        return try self.graph_index.getEdgesByTypesBounded(
            alloc,
            key,
            edge_types,
            direction,
            max_edges,
            max_bytes,
        );
    }

    pub fn freeEdges(_: @This(), alloc: Allocator, edges: []Edge) void {
        GraphIndex.freeEdges(alloc, edges);
    }
};

// ============================================================================
// Types
// ============================================================================

pub const PathWeightMode = enum { min_hops, min_weight, max_weight };

pub const PathFindOptions = struct {
    weight_mode: PathWeightMode = .min_hops,
    edge_types: []const []const u8 = &.{},
    direction: EdgeDirection = .out,
    max_depth: u32 = 50,
    min_weight: ?f64 = null,
    max_weight: ?f64 = null,
    node_admission: ?NodeAdmission = null,
    /// Shared by every path operation in the enclosing request. Yen spur
    /// searches deliberately reuse this pointer rather than resetting limits.
    work_budget: ?*work_budget_mod.WorkBudget = null,
};

pub const PathEdge = struct {
    source: []const u8,
    target: []const u8,
    edge_type: []const u8,
    weight: f64,
    metadata: []const u8 = "",
};

pub const Path = struct {
    nodes: [][]const u8,
    /// Internal table provenance parallel to `nodes`. An empty slice means all
    /// nodes belong to the query table.
    node_tables: []?[]const u8 = &.{},
    edges: []PathEdge,
    total_weight: f64,
    length: u32,
};

pub fn freePath(alloc: Allocator, path: Path) void {
    for (path.nodes) |n| alloc.free(n);
    alloc.free(path.nodes);
    for (path.node_tables) |table| if (table) |value| alloc.free(value);
    if (path.node_tables.len > 0) alloc.free(path.node_tables);
    for (path.edges) |e| {
        alloc.free(e.source);
        alloc.free(e.target);
        alloc.free(e.edge_type);
        if (e.metadata.len > 0) alloc.free(e.metadata);
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
    metadata: []const u8 = "",
};

const EdgeIdentity = struct {
    source: []const u8,
    target: []const u8,
    edge_type: []const u8,
};

const EdgeIdentityContext = struct {
    pub fn hash(_: @This(), key: EdgeIdentity) u64 {
        var hasher = std.hash.Wyhash.init(0x4146_4752_4150_4845);
        hashIdentityPart(&hasher, key.source);
        hashIdentityPart(&hasher, key.target);
        hashIdentityPart(&hasher, key.edge_type);
        return hasher.final();
    }

    pub fn eql(_: @This(), a: EdgeIdentity, b: EdgeIdentity) bool {
        return std.mem.eql(u8, a.source, b.source) and
            std.mem.eql(u8, a.target, b.target) and
            std.mem.eql(u8, a.edge_type, b.edge_type);
    }

    fn hashIdentityPart(hasher: *std.hash.Wyhash, value: []const u8) void {
        const len: u64 = @intCast(value.len);
        hasher.update(std.mem.asBytes(&len));
        hasher.update(value);
    }
};

const PathStateKey = struct {
    node: []const u8,
    hops: u32,
};

const PathStateKeyContext = struct {
    pub fn hash(_: @This(), key: PathStateKey) u64 {
        var hasher = std.hash.Wyhash.init(0x4146_5041_5448_5354);
        hasher.update(key.node);
        hasher.update(std.mem.asBytes(&key.hops));
        return hasher.final();
    }

    pub fn eql(_: @This(), a: PathStateKey, b: PathStateKey) bool {
        return a.hops == b.hops and std.mem.eql(u8, a.node, b.node);
    }
};

const BestDistanceMap = std.HashMapUnmanaged(PathStateKey, f64, PathStateKeyContext, 80);

const ExcludedEdgeSet = std.HashMapUnmanaged(EdgeIdentity, void, EdgeIdentityContext, 80);

fn putExcludedEdge(
    set: *ExcludedEdgeSet,
    alloc: Allocator,
    source: []const u8,
    target: []const u8,
    edge_type: []const u8,
) !void {
    const borrowed: EdgeIdentity = .{
        .source = source,
        .target = target,
        .edge_type = edge_type,
    };
    if (set.contains(borrowed)) return;

    const owned_source = try alloc.dupe(u8, source);
    errdefer alloc.free(owned_source);
    const owned_target = try alloc.dupe(u8, target);
    errdefer alloc.free(owned_target);
    const owned_edge_type = try alloc.dupe(u8, edge_type);
    errdefer alloc.free(owned_edge_type);
    try set.putNoClobber(alloc, .{
        .source = owned_source,
        .target = owned_target,
        .edge_type = owned_edge_type,
    }, {});
}

fn deinitExcludedEdges(set: *ExcludedEdgeSet, alloc: Allocator) void {
    var it = set.keyIterator();
    while (it.next()) |key| {
        alloc.free(key.source);
        alloc.free(key.target);
        alloc.free(key.edge_type);
    }
    set.deinit(alloc);
}

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
    return findShortestPathWithEdgeReader(
        alloc,
        GraphIndexEdgeReader{ .graph_index = graph_index },
        source,
        target,
        opts,
    );
}

/// Find a shortest path against an immutable edge reader. This is used by
/// serverless snapshots so they share the canonical BFS/Dijkstra semantics
/// without rebuilding a mutable GraphIndex for every request.
pub fn findShortestPathWithEdgeReader(
    alloc: Allocator,
    edge_reader: anytype,
    source: []const u8,
    target: []const u8,
    opts: PathFindOptions,
) !?Path {
    return findShortestPathWithExclusionsAndEdgeReader(alloc, edge_reader, source, target, opts, null, null);
}

/// Find shortest path with optional node/edge exclusions (used by Yen's algorithm).
pub fn findShortestPathWithExclusions(
    alloc: Allocator,
    graph_index: *GraphIndex,
    source: []const u8,
    target: []const u8,
    opts: PathFindOptions,
    excluded_nodes: ?*const std.StringHashMapUnmanaged(void),
    excluded_edges: ?*const ExcludedEdgeSet,
) !?Path {
    return findShortestPathWithExclusionsAndEdgeReader(
        alloc,
        GraphIndexEdgeReader{ .graph_index = graph_index },
        source,
        target,
        opts,
        excluded_nodes,
        excluded_edges,
    );
}

fn findShortestPathWithExclusionsAndEdgeReader(
    alloc: Allocator,
    edge_reader: anytype,
    source: []const u8,
    target: []const u8,
    opts: PathFindOptions,
    excluded_nodes: ?*const std.StringHashMapUnmanaged(void),
    excluded_edges: ?*const ExcludedEdgeSet,
) !?Path {
    var local_work_budget = work_budget_mod.WorkBudget.init(
        work_budget_mod.default_max_explored_nodes,
        work_budget_mod.default_max_explored_edges,
    );
    var effective_opts = opts;
    if (effective_opts.work_budget == null) effective_opts.work_budget = &local_work_budget;

    if (effective_opts.node_admission) |admission| {
        if (!try traversal_mod.startNodeAdmittedWithEdgeReader(alloc, edge_reader, source, effective_opts.direction, admission, effective_opts.work_budget)) {
            return null;
        }
    }

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

    if (effective_opts.weight_mode == .min_hops) {
        return bfsShortestPath(alloc, edge_reader, source, target, effective_opts, excluded_nodes, excluded_edges);
    } else {
        return dijkstraPath(alloc, edge_reader, source, target, effective_opts, excluded_nodes, excluded_edges);
    }
}

// ============================================================================
// BFS shortest path (min_hops)
// ============================================================================

fn bfsShortestPath(
    alloc: Allocator,
    edge_reader: anytype,
    source: []const u8,
    target: []const u8,
    opts: PathFindOptions,
    excluded_nodes: ?*const std.StringHashMapUnmanaged(void),
    excluded_edges: ?*const ExcludedEdgeSet,
) !?Path {
    const work_budget = opts.work_budget.?;
    // Arena for PathNodes — freed at end
    var node_pool = std.ArrayListUnmanaged(*PathNode).empty;
    defer {
        for (node_pool.items) |n| {
            alloc.free(n.key);
            if (n.parent_edge) |e| {
                alloc.free(e.source);
                alloc.free(e.target);
                alloc.free(e.edge_type);
                if (e.metadata.len > 0) alloc.free(e.metadata);
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
    try work_budget.checkIntermediateStates(queue.items.len, work_budget_mod.default_max_intermediate_states);
    try visited.put(alloc, try alloc.dupe(u8, source), {});
    try work_budget.consumeNode();

    while (queue_head < queue.items.len) {
        const current = queue.items[queue_head];
        queue_head += 1;

        if (opts.max_depth > 0 and current.hops >= opts.max_depth) continue;

        const edges = try getEdgesForPathBudget(alloc, edge_reader, current.key, opts, work_budget);
        defer edge_reader.freeEdges(alloc, edges);
        try work_budget.consumeMaterializedEdges(edges);

        const admitted_edges = if (opts.node_admission) |admission| blk: {
            const edge_mask = try alloc.alloc(bool, edges.len);
            @memset(edge_mask, false);
            errdefer alloc.free(edge_mask);
            var candidate_indexes = std.ArrayListUnmanaged(usize).empty;
            defer candidate_indexes.deinit(alloc);
            var candidate_nodes = std.ArrayListUnmanaged(NodeRef).empty;
            defer candidate_nodes.deinit(alloc);
            try candidate_indexes.ensureTotalCapacity(alloc, edges.len);
            try candidate_nodes.ensureTotalCapacity(alloc, edges.len);
            for (edges, 0..) |edge, edge_index| {
                if (!shouldTraverseEdge(opts, &edge)) continue;
                const next_key = if (std.mem.eql(u8, current.key, edge.source)) edge.target else edge.source;
                if (excluded_nodes) |en| if (en.contains(next_key)) continue;
                if (excluded_edges) |ee| {
                    if (ee.contains(.{
                        .source = edge.source,
                        .target = edge.target,
                        .edge_type = edge.edge_type,
                    })) continue;
                }
                if (visited.contains(next_key)) continue;
                const target_table = if (std.mem.eql(u8, next_key, edge.target))
                    traversal_mod.metadataTargetTable(edge.metadata)
                else
                    null;
                candidate_indexes.appendAssumeCapacity(edge_index);
                candidate_nodes.appendAssumeCapacity(.{
                    .key = next_key,
                    .table = target_table,
                    .external = std.mem.eql(u8, next_key, edge.target) and
                        (admission.external_targets or
                            target_table != null),
                });
            }
            const candidate_mask = try admission.filterAlloc(alloc, candidate_nodes.items);
            defer alloc.free(candidate_mask);
            for (candidate_indexes.items, candidate_mask) |edge_index, allowed| {
                edge_mask[edge_index] = allowed;
            }
            break :blk edge_mask;
        } else null;
        defer if (admitted_edges) |mask| alloc.free(mask);

        for (edges, 0..) |edge, edge_index| {
            const next_key = if (std.mem.eql(u8, current.key, edge.source)) edge.target else edge.source;
            if (admitted_edges) |mask| {
                if (!mask[edge_index]) continue;
            } else {
                if (!shouldTraverseEdge(opts, &edge)) continue;
                if (excluded_nodes) |en| if (en.contains(next_key)) continue;
                if (excluded_edges) |ee| {
                    if (ee.contains(.{
                        .source = edge.source,
                        .target = edge.target,
                        .edge_type = edge.edge_type,
                    })) continue;
                }
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
                    .metadata = if (edge.metadata.len > 0) try alloc.dupe(u8, edge.metadata) else "",
                },
            };
            try node_pool.append(alloc, node);
            try work_budget.consumeNode();

            // Found target — reconstruct path
            if (std.mem.eql(u8, next_key, target)) {
                return try reconstructPath(alloc, node);
            }

            try queue.append(alloc, node);
            try work_budget.checkIntermediateStates(queue.items.len, work_budget_mod.default_max_intermediate_states);
        }
    }

    return null;
}

// ============================================================================
// Dijkstra (min_weight and max_weight)
// ============================================================================

fn dijkstraPath(
    alloc: Allocator,
    edge_reader: anytype,
    source: []const u8,
    target: []const u8,
    opts: PathFindOptions,
    excluded_nodes: ?*const std.StringHashMapUnmanaged(void),
    excluded_edges: ?*const ExcludedEdgeSet,
) !?Path {
    const work_budget = opts.work_budget.?;
    var node_pool = std.ArrayListUnmanaged(*PathNode).empty;
    defer {
        for (node_pool.items) |n| {
            alloc.free(n.key);
            if (n.parent_edge) |e| {
                alloc.free(e.source);
                alloc.free(e.target);
                alloc.free(e.edge_type);
                if (e.metadata.len > 0) alloc.free(e.metadata);
            }
            alloc.destroy(n);
        }
        node_pool.deinit(alloc);
    }

    // A cheaper arrival with more hops must not suppress a shallower arrival:
    // only a label with no greater cost and no greater hop count dominates a
    // state in a bounded shortest-path query. Keys borrow node_pool storage.
    var best_dist = BestDistanceMap.empty;
    defer best_dist.deinit(alloc);

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
    try work_budget.checkIntermediateStates(heap.items.len, work_budget_mod.default_max_intermediate_states);
    try best_dist.put(alloc, .{ .node = start.key, .hops = 0 }, 0.0);
    try work_budget.consumeNode();

    while (heap.pop()) |current| {
        if (pathStateDominated(&best_dist, current.key, current.hops, current.distance, true)) continue;

        // Dijkstra may return on settlement because pathEdgeCost guarantees a
        // non-negative additive cost for every admitted edge.
        if (std.mem.eql(u8, current.key, target)) return try reconstructPath(alloc, current);

        if (opts.max_depth > 0 and current.hops >= opts.max_depth) continue;

        const edges = try getEdgesForPathBudget(alloc, edge_reader, current.key, opts, work_budget);
        defer edge_reader.freeEdges(alloc, edges);
        try work_budget.consumeMaterializedEdges(edges);

        const admitted_edges = if (opts.node_admission) |admission| blk: {
            const edge_mask = try alloc.alloc(bool, edges.len);
            @memset(edge_mask, false);
            errdefer alloc.free(edge_mask);
            var candidate_indexes = std.ArrayListUnmanaged(usize).empty;
            defer candidate_indexes.deinit(alloc);
            var candidate_nodes = std.ArrayListUnmanaged(NodeRef).empty;
            defer candidate_nodes.deinit(alloc);
            try candidate_indexes.ensureTotalCapacity(alloc, edges.len);
            try candidate_nodes.ensureTotalCapacity(alloc, edges.len);
            for (edges, 0..) |edge, edge_index| {
                if (!shouldTraverseEdge(opts, &edge)) continue;
                const next_key = if (std.mem.eql(u8, current.key, edge.source)) edge.target else edge.source;
                if (excluded_nodes) |en| if (en.contains(next_key)) continue;
                if (excluded_edges) |ee| {
                    if (ee.contains(.{
                        .source = edge.source,
                        .target = edge.target,
                        .edge_type = edge.edge_type,
                    })) continue;
                }
                const target_table = if (std.mem.eql(u8, next_key, edge.target))
                    traversal_mod.metadataTargetTable(edge.metadata)
                else
                    null;
                candidate_indexes.appendAssumeCapacity(edge_index);
                candidate_nodes.appendAssumeCapacity(.{
                    .key = next_key,
                    .table = target_table,
                    .external = std.mem.eql(u8, next_key, edge.target) and
                        (admission.external_targets or
                            target_table != null),
                });
            }
            const candidate_mask = try admission.filterAlloc(alloc, candidate_nodes.items);
            defer alloc.free(candidate_mask);
            for (candidate_indexes.items, candidate_mask) |edge_index, allowed| {
                edge_mask[edge_index] = allowed;
            }
            break :blk edge_mask;
        } else null;
        defer if (admitted_edges) |mask| alloc.free(mask);

        for (edges, 0..) |edge, edge_index| {
            const next_key = if (std.mem.eql(u8, current.key, edge.source)) edge.target else edge.source;
            if (admitted_edges) |mask| {
                if (!mask[edge_index]) continue;
            } else {
                if (!shouldTraverseEdge(opts, &edge)) continue;
                if (excluded_nodes) |en| if (en.contains(next_key)) continue;
                if (excluded_edges) |ee| {
                    if (ee.contains(.{
                        .source = edge.source,
                        .target = edge.target,
                        .edge_type = edge.edge_type,
                    })) continue;
                }
            }

            const new_dist = current.distance + try pathEdgeCost(opts.weight_mode, edge.weight);
            const next_hops = current.hops + 1;
            if (!pathStateDominated(&best_dist, next_key, next_hops, new_dist, false)) {
                const node = try alloc.create(PathNode);
                node.* = .{
                    .key = try alloc.dupe(u8, next_key),
                    .distance = new_dist,
                    .hops = next_hops,
                    .parent = current,
                    .parent_edge = .{
                        .source = try alloc.dupe(u8, edge.source),
                        .target = try alloc.dupe(u8, edge.target),
                        .edge_type = try alloc.dupe(u8, edge.edge_type),
                        .weight = edge.weight,
                        .metadata = if (edge.metadata.len > 0) try alloc.dupe(u8, edge.metadata) else "",
                    },
                };
                try node_pool.append(alloc, node);
                try work_budget.consumeNode();
                try best_dist.put(alloc, .{ .node = node.key, .hops = next_hops }, new_dist);
                try heap.push(alloc, node);
                try work_budget.checkIntermediateStates(heap.items.len, work_budget_mod.default_max_intermediate_states);
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
    return try findKShortestPathsWithEdgeReader(
        alloc,
        GraphIndexEdgeReader{ .graph_index = graph_index },
        source,
        target,
        k,
        opts,
    );
}

pub fn findKShortestPathsWithEdgeReader(
    alloc: Allocator,
    edge_reader: anytype,
    source: []const u8,
    target: []const u8,
    k: u32,
    opts: PathFindOptions,
) ![]Path {
    if (k == 0) return try alloc.alloc(Path, 0);

    var local_work_budget = work_budget_mod.WorkBudget.init(
        work_budget_mod.default_max_explored_nodes,
        work_budget_mod.default_max_explored_edges,
    );
    var effective_opts = opts;
    if (effective_opts.work_budget == null) effective_opts.work_budget = &local_work_budget;

    var results = std.ArrayListUnmanaged(Path).empty;
    errdefer {
        for (results.items) |p| freePath(alloc, p);
        results.deinit(alloc);
    }

    // Find first shortest path
    const first = try findShortestPathWithEdgeReader(alloc, edge_reader, source, target, effective_opts);
    if (first == null) return try alloc.alloc(Path, 0);
    try results.append(alloc, first.?);

    if (k == 1) {
        const owned = try alloc.dupe(Path, results.items);
        results.deinit(alloc);
        return owned;
    }

    // Candidate ordering follows the requested path objective.
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
            var excluded_edges = ExcludedEdgeSet.empty;
            defer deinitExcludedEdges(&excluded_edges, alloc);

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
                if (result_path.edges.len <= spur_idx) return error.InvalidGraphPath;
                const edge = result_path.edges[spur_idx];

                try putExcludedEdge(
                    &excluded_edges,
                    alloc,
                    edge.source,
                    edge.target,
                    edge.edge_type,
                );
            }

            // Exclude root path nodes (except spur node)
            for (0..spur_idx) |i| {
                const node_key = prev_path.nodes[i];
                if (!excluded_nodes.contains(node_key)) {
                    try excluded_nodes.put(alloc, try alloc.dupe(u8, node_key), {});
                }
            }

            var spur_opts = effective_opts;
            if (effective_opts.max_depth > 0) {
                const root_hops: u32 = @intCast(spur_idx);
                if (root_hops >= effective_opts.max_depth) continue;
                spur_opts.max_depth = effective_opts.max_depth - root_hops;
            }

            // Find a spur path within the remaining end-to-end depth budget.
            const spur_path = try findShortestPathWithExclusionsAndEdgeReader(
                alloc,
                edge_reader,
                spur_node,
                target,
                spur_opts,
                &excluded_nodes,
                &excluded_edges,
            );

            if (spur_path) |sp| {
                // Build total path = root[0..spur_idx] + spur_path
                const total_path = try joinPaths(alloc, prev_path, spur_idx, &sp);
                freePath(alloc, sp);
                if (effective_opts.max_depth > 0 and total_path.length > effective_opts.max_depth) {
                    freePath(alloc, total_path);
                    continue;
                }

                const pkey = try pathToKey(alloc, &total_path);
                if (!seen_paths.contains(pkey)) {
                    try seen_paths.put(alloc, pkey, {});
                    try candidates.append(alloc, total_path);
                    try effective_opts.work_budget.?.checkIntermediateStates(candidates.items.len, work_budget_mod.default_max_intermediate_states);
                } else {
                    alloc.free(pkey);
                    freePath(alloc, total_path);
                }
            }
        }

        if (candidates.items.len == 0) break;

        // Find the best candidate under the same objective as the spur search.
        var best_idx: usize = 0;
        for (candidates.items[1..], 1..) |c, i| {
            if (comparePathScore(&c, &candidates.items[best_idx], effective_opts.weight_mode) == .lt) {
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

fn getEdgesForPathBudget(
    alloc: Allocator,
    edge_reader: anytype,
    key: []const u8,
    opts: PathFindOptions,
    work_budget: *work_budget_mod.WorkBudget,
) ![]Edge {
    if (comptime @hasDecl(@TypeOf(edge_reader), "getEdgesBoundedForPath")) {
        return edge_reader.getEdgesBoundedForPath(
            alloc,
            key,
            opts.edge_types,
            opts.direction,
            work_budget.edgeLimit(),
            work_budget.edgeByteLimit(),
        ) catch |err| {
            const widened: anyerror = err;
            if (widened == error.GraphExploredEdgesBudgetExceeded)
                return work_budget.exhaust(.explored_edges, work_budget.max_edges);
            if (widened == error.GraphExploredEdgeBytesBudgetExceeded)
                return work_budget.exhaust(.explored_edge_bytes, work_budget.max_edge_bytes);
            if (widened == error.QueryCandidateBudgetExceeded)
                return work_budget.exhaust(.explored_edges, work_budget.max_edges);
            return err;
        };
    }
    return try edge_reader.getEdges(alloc, key, opts.direction);
}

// ============================================================================
// Helpers
// ============================================================================

fn shouldTraverseEdge(opts: PathFindOptions, edge: *const Edge) bool {
    if (opts.min_weight) |min_weight| if (edge.weight < min_weight) return false;
    if (opts.max_weight) |max_weight| if (edge.weight > max_weight) return false;
    if (opts.edge_types.len > 0) {
        for (opts.edge_types) |et| {
            if (std.mem.eql(u8, edge.edge_type, et)) return true;
        }
        return false;
    }
    return true;
}

pub fn pathEdgeCost(mode: PathWeightMode, weight: f64) !f64 {
    if (!std.math.isFinite(weight)) return switch (mode) {
        .min_weight => error.GraphMinWeightDomainViolation,
        .max_weight => error.GraphMaxWeightDomainViolation,
        .min_hops => 1.0,
    };
    return switch (mode) {
        .min_hops => 1.0,
        .min_weight => if (weight >= 0.0) weight else error.GraphMinWeightDomainViolation,
        .max_weight => if (weight < 0.0 or weight > 1.0)
            error.GraphMaxWeightDomainViolation
        else if (weight == 0.0)
            std.math.inf(f64)
        else
            -@log(weight),
    };
}

fn pathStateDominated(
    best_dist: *const BestDistanceMap,
    node: []const u8,
    hops: u32,
    distance: f64,
    exact_state_is_current: bool,
) bool {
    var candidate_hops: u32 = 0;
    while (candidate_hops <= hops) : (candidate_hops += 1) {
        const best = best_dist.get(.{ .node = node, .hops = candidate_hops }) orelse continue;
        if (candidate_hops == hops and exact_state_is_current) {
            if (best < distance) return true;
        } else if (best <= distance) {
            return true;
        }
    }
    return false;
}

fn pathScore(path: *const Path, mode: PathWeightMode) f64 {
    return switch (mode) {
        .min_hops => @floatFromInt(path.length),
        .min_weight => path.total_weight,
        .max_weight => blk: {
            var score: f64 = 0.0;
            for (path.edges) |edge| {
                score += pathEdgeCost(.max_weight, edge.weight) catch return std.math.inf(f64);
            }
            break :blk score;
        },
    };
}

fn comparePathScore(a: *const Path, b: *const Path, mode: PathWeightMode) std.math.Order {
    const score_order = std.math.order(pathScore(a, mode), pathScore(b, mode));
    if (score_order != .eq) return score_order;
    return std.math.order(a.length, b.length);
}

test "path weight filters preserve explicit zero bounds" {
    const zero = Edge{ .source = "a", .target = "b", .edge_type = "e", .weight = 0, .created_at = 0, .updated_at = 0, .metadata = "" };
    const positive = Edge{ .source = "a", .target = "b", .edge_type = "e", .weight = 0.1, .created_at = 0, .updated_at = 0, .metadata = "" };
    const negative = Edge{ .source = "a", .target = "b", .edge_type = "e", .weight = -0.1, .created_at = 0, .updated_at = 0, .metadata = "" };
    try std.testing.expect(shouldTraverseEdge(.{ .max_weight = 0 }, &zero));
    try std.testing.expect(!shouldTraverseEdge(.{ .max_weight = 0 }, &positive));
    try std.testing.expect(shouldTraverseEdge(.{ .max_weight = 0 }, &negative));
    try std.testing.expect(!shouldTraverseEdge(.{ .min_weight = 0 }, &negative));
}

test "path scoring follows the selected objective" {
    const one_edge = [_]PathEdge{.{ .source = "a", .target = "b", .edge_type = "e", .weight = 0.5 }};
    const two_edges = [_]PathEdge{
        .{ .source = "a", .target = "c", .edge_type = "e", .weight = 0.9 },
        .{ .source = "c", .target = "b", .edge_type = "e", .weight = 0.9 },
    };
    const one_hop = Path{ .nodes = &.{}, .edges = @constCast(one_edge[0..]), .total_weight = 0.5, .length = 1 };
    const two_hops = Path{ .nodes = &.{}, .edges = @constCast(two_edges[0..]), .total_weight = 1.8, .length = 2 };

    try std.testing.expectEqual(std.math.Order.lt, comparePathScore(&one_hop, &two_hops, .min_hops));
    try std.testing.expectEqual(std.math.Order.lt, comparePathScore(&one_hop, &two_hops, .min_weight));
    try std.testing.expectEqual(std.math.Order.gt, comparePathScore(&one_hop, &two_hops, .max_weight));
}

test "weighted path costs reject domains that violate Dijkstra invariants" {
    try std.testing.expectError(error.GraphMinWeightDomainViolation, pathEdgeCost(.min_weight, -0.1));
    try std.testing.expectError(error.GraphMaxWeightDomainViolation, pathEdgeCost(.max_weight, -0.1));
    try std.testing.expectError(error.GraphMaxWeightDomainViolation, pathEdgeCost(.max_weight, 1.1));
    try std.testing.expectEqual(@as(f64, 1.1), try pathEdgeCost(.min_weight, 1.1));
    try std.testing.expect(std.math.isInf(try pathEdgeCost(.max_weight, 0.0)));
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
                .metadata = if (pe.metadata.len > 0) try alloc.dupe(u8, pe.metadata) else "",
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

fn pathToKey(alloc: Allocator, path: *const Path) ![]u8 {
    var total_len: usize = 0;
    for (path.nodes, 0..) |node, i| {
        const table = if (path.node_tables.len == path.nodes.len) path.node_tables[i] else null;
        total_len = std.math.add(usize, total_len, 1 + @sizeOf(u64)) catch
            return error.PathIdentityTooLarge;
        if (table) |value| total_len = std.math.add(usize, total_len, value.len) catch
            return error.PathIdentityTooLarge;
        total_len = std.math.add(usize, total_len, @sizeOf(u64)) catch
            return error.PathIdentityTooLarge;
        total_len = std.math.add(usize, total_len, node.len) catch
            return error.PathIdentityTooLarge;
    }
    for (path.edges) |edge| {
        for ([_][]const u8{ edge.source, edge.target, edge.edge_type }) |part| {
            total_len = std.math.add(usize, total_len, @sizeOf(u64)) catch
                return error.PathIdentityTooLarge;
            total_len = std.math.add(usize, total_len, part.len) catch
                return error.PathIdentityTooLarge;
        }
    }

    const buf = try alloc.alloc(u8, total_len);
    var pos: usize = 0;
    for (path.nodes, 0..) |node, i| {
        const table = if (path.node_tables.len == path.nodes.len) path.node_tables[i] else null;
        buf[pos] = if (table == null) 0 else 1;
        pos += 1;
        const table_len: u64 = if (table) |value| @intCast(value.len) else 0;
        std.mem.writeInt(u64, buf[pos..][0..8], table_len, .little);
        pos += 8;
        if (table) |value| {
            @memcpy(buf[pos..][0..value.len], value);
            pos += value.len;
        }
        std.mem.writeInt(u64, buf[pos..][0..8], @intCast(node.len), .little);
        pos += 8;
        @memcpy(buf[pos..][0..node.len], node);
        pos += node.len;
    }
    for (path.edges) |edge| {
        for ([_][]const u8{ edge.source, edge.target, edge.edge_type }) |part| {
            std.mem.writeInt(u64, buf[pos..][0..8], @intCast(part.len), .little);
            pos += 8;
            @memcpy(buf[pos..][0..part.len], part);
            pos += part.len;
        }
    }
    return buf;
}

fn rootPathMatches(a: *const Path, b: *const Path, up_to: usize) bool {
    if (a.nodes.len <= up_to or b.nodes.len <= up_to) return false;
    for (0..up_to + 1) |i| {
        if (!std.mem.eql(u8, a.nodes[i], b.nodes[i])) return false;
        const a_table = if (a.node_tables.len == a.nodes.len) a.node_tables[i] else null;
        const b_table = if (b.node_tables.len == b.nodes.len) b.node_tables[i] else null;
        if (!optionalStringEql(a_table, b_table)) return false;
    }
    if (a.edges.len < up_to or b.edges.len < up_to) return false;
    for (0..up_to) |i| if (!pathEdgeIdentityEql(a.edges[i], b.edges[i])) return false;
    return true;
}

fn optionalStringEql(a: ?[]const u8, b: ?[]const u8) bool {
    if (a == null or b == null) return a == null and b == null;
    return std.mem.eql(u8, a.?, b.?);
}

fn pathEdgeIdentityEql(a: PathEdge, b: PathEdge) bool {
    return std.mem.eql(u8, a.source, b.source) and
        std.mem.eql(u8, a.target, b.target) and
        std.mem.eql(u8, a.edge_type, b.edge_type);
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
            .metadata = if (root.edges[i].metadata.len > 0) try alloc.dupe(u8, root.edges[i].metadata) else "",
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
            .metadata = if (e.metadata.len > 0) try alloc.dupe(u8, e.metadata) else "",
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

test "bounded weighted shortest path preserves shallower Pareto labels" {
    const alloc = std.testing.allocator;
    var sb: [256]u8 = undefined;
    const store_path = tmpPath(&sb, "weighted-depth-label-s");
    defer cleanupTmp(store_path);
    var rb: [256]u8 = undefined;
    const reverse_path = tmpPath(&rb, "weighted-depth-label-r");
    defer cleanupTmp(reverse_path);

    var store = try docstore.DocStore.open(alloc, store_path, .{});
    defer store.close();
    var graph = try GraphIndex.open(alloc, &store, reverse_path, "test", .{});
    defer graph.close();

    try graph.addEdge("A", "X", "e", 1.0, 0, 0, "");
    try graph.addEdge("A", "Y", "e", 0.0, 0, 0, "");
    try graph.addEdge("Y", "X", "e", 0.0, 0, 0, "");
    try graph.addEdge("X", "T", "e", 0.0, 0, 0, "");

    const path = try findShortestPath(alloc, &graph, "A", "T", .{
        .weight_mode = .min_weight,
        .max_depth = 2,
    });
    try std.testing.expect(path != null);
    defer freePath(alloc, path.?);
    try std.testing.expectEqual(@as(u32, 2), path.?.length);
    try std.testing.expectEqualStrings("X", path.?.nodes[1]);
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

test "k shortest paths preserve parallel typed edge identities" {
    const alloc = std.testing.allocator;
    var sb: [256]u8 = undefined;
    const store_path = tmpPath(&sb, "k-parallel-edge-s");
    defer cleanupTmp(store_path);
    var rb: [256]u8 = undefined;
    const reverse_path = tmpPath(&rb, "k-parallel-edge-r");
    defer cleanupTmp(reverse_path);

    var store = try docstore.DocStore.open(alloc, store_path, .{});
    defer store.close();
    var graph = try GraphIndex.open(alloc, &store, reverse_path, "test", .{});
    defer graph.close();

    try graph.addEdge("A", "B", "primary", 1.0, 0, 0, "");
    try graph.addEdge("A", "B", "secondary", 1.0, 0, 0, "");
    try graph.addEdge("B", "C", "finish", 1.0, 0, 0, "");

    const found_paths = try findKShortestPaths(alloc, &graph, "A", "C", 2, .{});
    defer freePaths(alloc, found_paths);

    try std.testing.expectEqual(@as(usize, 2), found_paths.len);
    try std.testing.expectEqualStrings("A", found_paths[0].nodes[0]);
    try std.testing.expectEqualStrings("B", found_paths[0].nodes[1]);
    try std.testing.expectEqualStrings("C", found_paths[0].nodes[2]);
    try std.testing.expect(!std.mem.eql(
        u8,
        found_paths[0].edges[0].edge_type,
        found_paths[1].edges[0].edge_type,
    ));
}

test "k shortest paths share one cumulative work budget across spur searches" {
    const alloc = std.testing.allocator;
    var sb: [256]u8 = undefined;
    const store_path = tmpPath(&sb, "k-shared-budget-s");
    defer cleanupTmp(store_path);
    var rb: [256]u8 = undefined;
    const reverse_path = tmpPath(&rb, "k-shared-budget-r");
    defer cleanupTmp(reverse_path);

    var store = try docstore.DocStore.open(alloc, store_path, .{});
    defer store.close();
    var graph = try GraphIndex.open(alloc, &store, reverse_path, "test", .{});
    defer graph.close();

    try graph.addEdge("A", "B", "one", 1.0, 0, 0, "");
    try graph.addEdge("A", "D", "two", 1.0, 0, 0, "");
    try graph.addEdge("B", "C", "finish", 1.0, 0, 0, "");
    try graph.addEdge("D", "C", "finish", 1.0, 0, 0, "");

    // The first BFS admits exactly A, B, D, and C. A fresh budget per Yen
    // spur would incorrectly allow the second search to proceed.
    var budget = work_budget_mod.WorkBudget.init(4, 100);
    try std.testing.expectError(
        error.GraphWorkBudgetExceeded,
        findKShortestPaths(alloc, &graph, "A", "C", 2, .{ .work_budget = &budget }),
    );
    try std.testing.expectEqual(work_budget_mod.Dimension.explored_nodes, budget.exhaustion().?.dimension);
}

test "k shortest paths apply max_depth to the complete candidate" {
    const alloc = std.testing.allocator;
    var sb: [256]u8 = undefined;
    const store_path = tmpPath(&sb, "k-path-depth-s");
    defer cleanupTmp(store_path);
    var rb: [256]u8 = undefined;
    const reverse_path = tmpPath(&rb, "k-path-depth-r");
    defer cleanupTmp(reverse_path);

    var store = try docstore.DocStore.open(alloc, store_path, .{});
    defer store.close();
    var graph = try GraphIndex.open(alloc, &store, reverse_path, "test", .{});
    defer graph.close();

    try graph.addEdge("A", "B", "e", 1.0, 0, 0, "");
    try graph.addEdge("B", "T", "e", 1.0, 0, 0, "");
    try graph.addEdge("B", "C", "e", 1.0, 0, 0, "");
    try graph.addEdge("C", "T", "e", 1.0, 0, 0, "");

    const found = try findKShortestPaths(alloc, &graph, "A", "T", 2, .{
        .weight_mode = .min_weight,
        .max_depth = 2,
    });
    defer freePaths(alloc, found);
    try std.testing.expectEqual(@as(usize, 1), found.len);
    try std.testing.expectEqual(@as(u32, 2), found[0].length);
}

test "k shortest paths exclude stored edge orientation for incoming traversal" {
    const alloc = std.testing.allocator;
    var sb: [256]u8 = undefined;
    const store_path = tmpPath(&sb, "k-path-incoming-s");
    defer cleanupTmp(store_path);
    var rb: [256]u8 = undefined;
    const reverse_path = tmpPath(&rb, "k-path-incoming-r");
    defer cleanupTmp(reverse_path);

    var store = try docstore.DocStore.open(alloc, store_path, .{});
    defer store.close();
    var graph = try GraphIndex.open(alloc, &store, reverse_path, "test", .{});
    defer graph.close();

    try graph.addEdge("A", "B", "e", 1.0, 0, 0, "");
    try graph.addEdge("B", "D", "e", 1.0, 0, 0, "");
    try graph.addEdge("A", "C", "e", 2.0, 0, 0, "");
    try graph.addEdge("C", "D", "e", 2.0, 0, 0, "");

    const found = try findKShortestPaths(alloc, &graph, "D", "A", 2, .{
        .direction = .in,
        .weight_mode = .min_weight,
    });
    defer freePaths(alloc, found);

    try std.testing.expectEqual(@as(usize, 2), found.len);
    try std.testing.expectEqualStrings("B", found[0].nodes[1]);
    try std.testing.expectEqualStrings("C", found[1].nodes[1]);
}

test "k shortest paths preserve delimiter and long node identities" {
    const alloc = std.testing.allocator;
    var sb: [256]u8 = undefined;
    const store_path = tmpPath(&sb, "k-path-identity-s");
    defer cleanupTmp(store_path);
    var rb: [256]u8 = undefined;
    const reverse_path = tmpPath(&rb, "k-path-identity-r");
    defer cleanupTmp(reverse_path);

    var store = try docstore.DocStore.open(alloc, store_path, .{});
    defer store.close();
    var graph = try GraphIndex.open(alloc, &store, reverse_path, "test", .{});
    defer graph.close();

    const long_mid = try alloc.alloc(u8, 4 * 1024);
    defer alloc.free(long_mid);
    @memset(long_mid, 'L');

    // The first two paths collide under a delimiter-concatenated identity:
    // A -> B->C -> D and A -> B -> C -> D.
    try graph.addEdge("A", "B->C", "e:->", 1.0, 0, 0, "");
    try graph.addEdge("B->C", "D", "e:->", 1.0, 0, 0, "");
    try graph.addEdge("A", "B", "e:->", 1.0, 0, 0, "");
    try graph.addEdge("B", "C", "e:->", 1.0, 0, 0, "");
    try graph.addEdge("C", "D", "e:->", 1.0, 0, 0, "");
    try graph.addEdge("A", long_mid, "e:->", 2.0, 0, 0, "");
    try graph.addEdge(long_mid, "D", "e:->", 2.0, 0, 0, "");

    const found = try findKShortestPaths(alloc, &graph, "A", "D", 3, .{
        .weight_mode = .min_weight,
    });
    defer freePaths(alloc, found);

    try std.testing.expectEqual(@as(usize, 3), found.len);
    var saw_long = false;
    for (found) |path| {
        for (path.nodes) |node| {
            if (std.mem.eql(u8, node, long_mid)) saw_long = true;
        }
    }
    try std.testing.expect(saw_long);
}
