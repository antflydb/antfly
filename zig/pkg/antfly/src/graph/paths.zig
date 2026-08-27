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
    /// Maximum number of live frontier/candidate states.
    max_intermediate_states: usize = work_budget_mod.default_max_intermediate_states,
};

pub const PathEdge = struct {
    source: []const u8,
    target: []const u8,
    edge_type: []const u8,
    weight: f64,
    metadata: []const u8 = "",
    /// Traversal orientation relative to the physical stored endpoints above.
    /// Null is retained when equal endpoint keys make the direction ambiguous;
    /// the public boundary resolves it from node-table provenance and metadata.
    traversal_direction: ?EdgeDirection = null,
};

pub const Path = struct {
    nodes: [][]const u8,
    /// Internal table provenance parallel to `nodes`. An empty slice means all
    /// nodes belong to the query table.
    node_tables: []?[]const u8 = &.{},
    edges: []PathEdge,
    total_weight: f64,
    length: u32,
    retained_budget: ?*work_budget_mod.WorkBudget = null,
    retained_state_bytes: usize = 0,
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
    if (path.retained_budget) |budget| budget.releaseStateBytes(path.retained_state_bytes);
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

fn destroyPathNode(alloc: Allocator, node: *PathNode) void {
    alloc.free(node.key);
    if (node.parent_edge) |edge| {
        alloc.free(edge.source);
        alloc.free(edge.target);
        alloc.free(edge.edge_type);
        if (edge.metadata.len > 0) alloc.free(edge.metadata);
    }
    alloc.destroy(node);
}

fn createPathNode(
    alloc: Allocator,
    key: []const u8,
    distance: f64,
    hops: u32,
    parent: ?*PathNode,
    edge: ?Edge,
) !*PathNode {
    const node = try alloc.create(PathNode);
    errdefer alloc.destroy(node);
    const owned_key = try alloc.dupe(u8, key);
    errdefer alloc.free(owned_key);
    const parent_edge: ?OwnedEdgeInfo = if (edge) |value| blk: {
        const source = try alloc.dupe(u8, value.source);
        errdefer alloc.free(source);
        const target = try alloc.dupe(u8, value.target);
        errdefer alloc.free(target);
        const edge_type = try alloc.dupe(u8, value.edge_type);
        errdefer alloc.free(edge_type);
        const metadata = if (value.metadata.len > 0) try alloc.dupe(u8, value.metadata) else "";
        errdefer if (metadata.len > 0) alloc.free(metadata);
        break :blk .{
            .source = source,
            .target = target,
            .edge_type = edge_type,
            .weight = value.weight,
            .metadata = metadata,
        };
    } else null;
    node.* = .{
        .key = owned_key,
        .distance = distance,
        .hops = hops,
        .parent = parent,
        .parent_edge = parent_edge,
    };
    return node;
}

fn retainPathNodeState(
    key: []const u8,
    edge: ?Edge,
    budget: *work_budget_mod.WorkBudget,
    retained_bytes: *usize,
) !void {
    // Account for node-pool/frontier slots and the BFS visited-key duplicate.
    const duplicated_key_bytes = std.math.mul(usize, 2, key.len) catch
        return budget.exhaust(.retained_state_bytes, budget.max_retained_state_bytes);
    var added = std.math.add(usize, @sizeOf(PathNode) + 3 * @sizeOf(*PathNode), duplicated_key_bytes) catch
        return budget.exhaust(.retained_state_bytes, budget.max_retained_state_bytes);
    if (edge) |value| {
        for ([_][]const u8{ value.source, value.target, value.edge_type, value.metadata }) |part| {
            added = std.math.add(usize, added, part.len) catch
                return budget.exhaust(.retained_state_bytes, budget.max_retained_state_bytes);
        }
    }
    try budget.retainStateBytes(added);
    retained_bytes.* += added;
}

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
    return findShortestPathWithExclusionsAndEdgeReader(alloc, edge_reader, source, target, opts, null, null, opts.work_budget);
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
        opts.work_budget,
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
    returned_state_budget: ?*work_budget_mod.WorkBudget,
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
        const retained_bytes = std.math.add(usize, @sizeOf(Path) + @sizeOf([]const u8), source.len) catch
            return effective_opts.work_budget.?.exhaust(.retained_state_bytes, effective_opts.work_budget.?.max_retained_state_bytes);
        try effective_opts.work_budget.?.retainStateBytes(retained_bytes);
        errdefer effective_opts.work_budget.?.releaseStateBytes(retained_bytes);
        const node = try alloc.dupe(u8, source);
        var node_owned = true;
        errdefer if (node_owned) alloc.free(node);
        const nodes = try alloc.alloc([]const u8, 1);
        errdefer alloc.free(nodes);
        nodes[0] = node;
        const edges = try alloc.alloc(PathEdge, 0);
        errdefer alloc.free(edges);
        const path = Path{
            .nodes = nodes,
            .edges = edges,
            .total_weight = 0.0,
            .length = 0,
            .retained_budget = returned_state_budget,
            .retained_state_bytes = retained_bytes,
        };
        node_owned = false;
        return path;
    }

    if (effective_opts.weight_mode == .min_hops) {
        return bfsShortestPath(alloc, edge_reader, source, target, effective_opts, excluded_nodes, excluded_edges, returned_state_budget);
    } else {
        return dijkstraPath(alloc, edge_reader, source, target, effective_opts, excluded_nodes, excluded_edges, returned_state_budget);
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
    returned_state_budget: ?*work_budget_mod.WorkBudget,
) !?Path {
    const work_budget = opts.work_budget.?;
    var retained_node_bytes: usize = 0;
    defer work_budget.releaseStateBytes(retained_node_bytes);
    // Arena for PathNodes — freed at end
    var node_pool = std.ArrayListUnmanaged(*PathNode).empty;
    defer {
        for (node_pool.items) |node| destroyPathNode(alloc, node);
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
    try work_budget.checkIntermediateStates(1, opts.max_intermediate_states);
    try work_budget.consumeNode();
    try retainPathNodeState(source, null, work_budget, &retained_node_bytes);
    const start = try createPathNode(alloc, source, 0, 0, null, null);
    var start_owned = true;
    errdefer if (start_owned) destroyPathNode(alloc, start);
    try node_pool.append(alloc, start);
    start_owned = false;
    try queue.append(alloc, start);
    try visited.put(alloc, try alloc.dupe(u8, source), {});

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
            const is_target = std.mem.eql(u8, next_key, target);
            if (!is_target) {
                const pending_states = queue.items.len - queue_head;
                try work_budget.checkIntermediateStates(pending_states + 1, opts.max_intermediate_states);
            }
            try work_budget.consumeNode();
            try retainPathNodeState(next_key, edge, work_budget, &retained_node_bytes);
            try visited.put(alloc, try alloc.dupe(u8, next_key), {});

            const node = try createPathNode(
                alloc,
                next_key,
                @floatFromInt(current.hops + 1),
                current.hops + 1,
                current,
                edge,
            );
            var node_owned = true;
            errdefer if (node_owned) destroyPathNode(alloc, node);
            try node_pool.append(alloc, node);
            node_owned = false;

            // Found target — reconstruct path
            if (is_target) {
                return try reconstructPath(alloc, node, work_budget, returned_state_budget);
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
    edge_reader: anytype,
    source: []const u8,
    target: []const u8,
    opts: PathFindOptions,
    excluded_nodes: ?*const std.StringHashMapUnmanaged(void),
    excluded_edges: ?*const ExcludedEdgeSet,
    returned_state_budget: ?*work_budget_mod.WorkBudget,
) !?Path {
    const work_budget = opts.work_budget.?;
    var retained_node_bytes: usize = 0;
    defer work_budget.releaseStateBytes(retained_node_bytes);
    var node_pool = std.ArrayListUnmanaged(*PathNode).empty;
    defer {
        for (node_pool.items) |node| destroyPathNode(alloc, node);
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

    try work_budget.checkIntermediateStates(1, opts.max_intermediate_states);
    try work_budget.consumeNode();
    try retainPathNodeState(source, null, work_budget, &retained_node_bytes);
    const start = try createPathNode(alloc, source, 0.0, 0, null, null);
    var start_owned = true;
    errdefer if (start_owned) destroyPathNode(alloc, start);
    try node_pool.append(alloc, start);
    start_owned = false;
    try heap.push(alloc, start);
    try best_dist.put(alloc, .{ .node = start.key, .hops = 0 }, 0.0);

    while (heap.pop()) |current| {
        if (pathStateDominated(&best_dist, current.key, current.hops, current.distance, true)) continue;

        // Dijkstra may return on settlement because pathEdgeCost guarantees a
        // non-negative additive cost for every admitted edge.
        if (std.mem.eql(u8, current.key, target))
            return try reconstructPath(alloc, current, work_budget, returned_state_budget);

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
                try work_budget.checkIntermediateStates(heap.items.len + 1, opts.max_intermediate_states);
                try work_budget.consumeNode();
                try retainPathNodeState(next_key, edge, work_budget, &retained_node_bytes);
                const node = try createPathNode(alloc, next_key, new_dist, next_hops, current, edge);
                var node_owned = true;
                errdefer if (node_owned) destroyPathNode(alloc, node);
                try node_pool.append(alloc, node);
                node_owned = false;
                try best_dist.put(alloc, .{ .node = node.key, .hops = next_hops }, new_dist);
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
    const uses_local_work_budget = effective_opts.work_budget == null;
    if (effective_opts.work_budget == null) effective_opts.work_budget = &local_work_budget;
    // Yen needs releasable leases for transient spur/candidate paths. Strip the
    // local pointer only from paths that actually escape this function.
    const transient_state_budget = effective_opts.work_budget;

    var results = std.ArrayListUnmanaged(Path).empty;
    errdefer {
        for (results.items) |p| freePath(alloc, p);
        results.deinit(alloc);
    }

    // Find first shortest path
    const first = try findShortestPathWithExclusionsAndEdgeReader(
        alloc,
        edge_reader,
        source,
        target,
        effective_opts,
        null,
        null,
        transient_state_budget,
    );
    if (first == null) return try alloc.alloc(Path, 0);
    var first_owned = true;
    errdefer if (first_owned) freePath(alloc, first.?);
    try results.append(alloc, first.?);
    first_owned = false;

    if (k == 1) {
        if (uses_local_work_budget) results.items[0].retained_budget = null;
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
    var seen_retained_bytes: usize = 0;
    defer {
        var it = seen_paths.keyIterator();
        while (it.next()) |key| alloc.free(key.*);
        seen_paths.deinit(alloc);
        effective_opts.work_budget.?.releaseStateBytes(seen_retained_bytes);
    }

    // Mark first path as seen
    const first_key = try pathToKey(alloc, &results.items[0]);
    var first_key_owned = true;
    errdefer if (first_key_owned) alloc.free(first_key);
    try retainSeenPathKey(effective_opts.work_budget.?, first_key, &seen_retained_bytes);
    try seen_paths.put(alloc, first_key, {});
    first_key_owned = false;

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
                transient_state_budget,
            );

            if (spur_path) |sp| {
                defer freePath(alloc, sp);
                // Build total path = root[0..spur_idx] + spur_path
                var total_path = try joinPathsRetained(
                    alloc,
                    prev_path,
                    spur_idx,
                    &sp,
                    effective_opts.work_budget.?,
                    transient_state_budget,
                );
                var total_path_owned = true;
                errdefer if (total_path_owned) freePath(alloc, total_path);
                if (effective_opts.max_depth > 0 and total_path.length > effective_opts.max_depth) {
                    freePath(alloc, total_path);
                    total_path_owned = false;
                    continue;
                }

                const pkey = try pathToKey(alloc, &total_path);
                if (!seen_paths.contains(pkey)) {
                    try effective_opts.work_budget.?.checkIntermediateStates(candidates.items.len + 1, effective_opts.max_intermediate_states);
                    var pkey_owned = true;
                    errdefer if (pkey_owned) alloc.free(pkey);
                    try retainSeenPathKey(effective_opts.work_budget.?, pkey, &seen_retained_bytes);
                    try seen_paths.put(alloc, pkey, {});
                    pkey_owned = false;
                    try candidates.append(alloc, total_path);
                    total_path_owned = false;
                } else {
                    alloc.free(pkey);
                    freePath(alloc, total_path);
                    total_path_owned = false;
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
        var best_owned = true;
        errdefer if (best_owned) freePath(alloc, best);
        try results.append(alloc, best);
        best_owned = false;
    }

    if (uses_local_work_budget) {
        for (results.items) |*path| path.retained_budget = null;
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

/// Compute the canonical raw edge-weight sum. This value is part of the public
/// GraphPath contract even when path ordering uses hops or a transformed
/// max-product objective, so silently publishing infinity is never valid.
pub fn sumPathEdgeWeights(edges: anytype) !f64 {
    var total: f64 = 0.0;
    for (edges) |edge| {
        if (!std.math.isFinite(edge.weight)) return error.GraphPathWeightOverflow;
        total += edge.weight;
        if (!std.math.isFinite(total)) return error.GraphPathWeightOverflow;
    }
    return total;
}

fn clonePathEdge(alloc: Allocator, edge: anytype) !PathEdge {
    const source = try alloc.dupe(u8, edge.source);
    errdefer alloc.free(source);
    const target = try alloc.dupe(u8, edge.target);
    errdefer alloc.free(target);
    const edge_type = try alloc.dupe(u8, edge.edge_type);
    errdefer alloc.free(edge_type);
    const metadata = if (edge.metadata.len > 0) try alloc.dupe(u8, edge.metadata) else "";
    errdefer if (metadata.len > 0) alloc.free(metadata);
    return .{
        .source = source,
        .target = target,
        .edge_type = edge_type,
        .weight = edge.weight,
        .metadata = metadata,
        .traversal_direction = if (@hasField(@TypeOf(edge), "traversal_direction"))
            edge.traversal_direction
        else
            null,
    };
}

test "canonical path weight sum rejects non-finite accumulation" {
    const edges = [_]PathEdge{
        .{ .source = "a", .target = "b", .edge_type = "e", .weight = std.math.floatMax(f64) },
        .{ .source = "b", .target = "c", .edge_type = "e", .weight = std.math.floatMax(f64) },
    };
    try std.testing.expectError(error.GraphPathWeightOverflow, sumPathEdgeWeights(&edges));
}

test "joining paths is allocation-failure safe" {
    var root_nodes = [_][]const u8{ "a", "b" };
    var root_edges = [_]PathEdge{.{
        .source = "a",
        .target = "b",
        .edge_type = "root",
        .weight = 1.0,
        .metadata = "{\"root\":true}",
    }};
    const root = Path{
        .nodes = &root_nodes,
        .edges = &root_edges,
        .total_weight = 1.0,
        .length = 1,
    };
    var spur_nodes = [_][]const u8{ "b", "c" };
    var spur_edges = [_]PathEdge{.{
        .source = "b",
        .target = "c",
        .edge_type = "spur",
        .weight = 2.0,
        .metadata = "{\"spur\":true}",
    }};
    const spur = Path{
        .nodes = &spur_nodes,
        .edges = &spur_edges,
        .total_weight = 2.0,
        .length = 1,
    };
    const Runner = struct {
        fn run(alloc: Allocator, root_path: Path, spur_path: Path) !void {
            const joined = try joinPaths(alloc, &root_path, 1, &spur_path);
            defer freePath(alloc, joined);
            try std.testing.expectEqual(@as(f64, 3.0), joined.total_weight);
        }
    };
    try std.testing.checkAllAllocationFailures(std.testing.allocator, Runner.run, .{ root, spur });
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

fn reconstructPath(
    alloc: Allocator,
    end_node: *PathNode,
    work_budget: *work_budget_mod.WorkBudget,
    returned_state_budget: ?*work_budget_mod.WorkBudget,
) !Path {
    const retained_bytes = reconstructedPathOwnedBytes(end_node) catch
        return work_budget.exhaust(.retained_state_bytes, work_budget.max_retained_state_bytes);
    try work_budget.retainStateBytes(retained_bytes);
    errdefer work_budget.releaseStateBytes(retained_bytes);

    // Count path length
    var count: u32 = 0;
    var n: ?*PathNode = end_node;
    while (n) |node| : (n = node.parent) {
        count += 1;
    }

    const nodes = try alloc.alloc([]const u8, count);
    var initialized_nodes: usize = 0;
    errdefer {
        for (nodes[count - initialized_nodes ..]) |node| alloc.free(node);
        alloc.free(nodes);
    }
    const edge_count = if (count > 0) count - 1 else 0;
    const path_edges = try alloc.alloc(PathEdge, edge_count);
    var initialized_edges: usize = 0;
    errdefer {
        for (path_edges[edge_count - initialized_edges ..]) |edge| {
            alloc.free(edge.source);
            alloc.free(edge.target);
            alloc.free(edge.edge_type);
            if (edge.metadata.len > 0) alloc.free(edge.metadata);
        }
        alloc.free(path_edges);
    }

    // Fill in reverse
    var idx = count;
    n = end_node;
    while (n) |node| : (n = node.parent) {
        idx -= 1;
        nodes[idx] = try alloc.dupe(u8, node.key);
        initialized_nodes += 1;
        if (node.parent_edge) |pe| {
            // Edge array is 1 shorter than node array; idx >= 1 when parent_edge exists
            path_edges[idx - 1] = try clonePathEdge(alloc, pe);
            initialized_edges += 1;
        }
    }

    const total_weight = try sumPathEdgeWeights(path_edges);

    return Path{
        .nodes = nodes,
        .edges = path_edges,
        .total_weight = total_weight,
        .length = edge_count,
        .retained_budget = returned_state_budget,
        .retained_state_bytes = retained_bytes,
    };
}

fn reconstructedPathOwnedBytes(end_node: *const PathNode) !usize {
    var total: usize = @sizeOf(Path);
    var cursor: ?*const PathNode = end_node;
    while (cursor) |node| : (cursor = node.parent) {
        total = try std.math.add(usize, total, @sizeOf([]const u8));
        total = try std.math.add(usize, total, node.key.len);
        if (node.parent_edge) |edge| {
            total = try std.math.add(usize, total, @sizeOf(PathEdge));
            for ([_][]const u8{ edge.source, edge.target, edge.edge_type, edge.metadata }) |part| {
                total = try std.math.add(usize, total, part.len);
            }
        }
    }
    return total;
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
        total_len = std.math.add(usize, total_len, 1) catch
            return error.PathIdentityTooLarge;
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
        buf[pos] = if (edge.traversal_direction) |direction| switch (direction) {
            .out => 1,
            .in => 2,
            .both => 3,
        } else 0;
        pos += 1;
        for ([_][]const u8{ edge.source, edge.target, edge.edge_type }) |part| {
            std.mem.writeInt(u64, buf[pos..][0..8], @intCast(part.len), .little);
            pos += 8;
            @memcpy(buf[pos..][0..part.len], part);
            pos += part.len;
        }
    }
    return buf;
}

fn retainSeenPathKey(
    work_budget: *work_budget_mod.WorkBudget,
    key: []const u8,
    retained_bytes: *usize,
) !void {
    const added = std.math.add(usize, @sizeOf([]const u8) + @sizeOf(void), key.len) catch
        return work_budget.exhaust(.retained_state_bytes, work_budget.max_retained_state_bytes);
    try work_budget.retainStateBytes(added);
    retained_bytes.* += added;
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
        std.mem.eql(u8, a.edge_type, b.edge_type) and
        a.traversal_direction == b.traversal_direction;
}

fn joinPathsRetained(
    alloc: Allocator,
    root: *const Path,
    spur_idx: usize,
    spur: *const Path,
    work_budget: *work_budget_mod.WorkBudget,
    returned_state_budget: ?*work_budget_mod.WorkBudget,
) !Path {
    const retained_bytes = joinedPathOwnedBytes(root, spur_idx, spur) catch
        return work_budget.exhaust(.retained_state_bytes, work_budget.max_retained_state_bytes);
    try work_budget.retainStateBytes(retained_bytes);
    errdefer work_budget.releaseStateBytes(retained_bytes);
    var path = try joinPaths(alloc, root, spur_idx, spur);
    path.retained_budget = returned_state_budget;
    path.retained_state_bytes = retained_bytes;
    return path;
}

fn joinedPathOwnedBytes(root: *const Path, spur_idx: usize, spur: *const Path) !usize {
    if (spur_idx > root.nodes.len or spur_idx > root.edges.len) return error.InvalidGraphPath;
    const node_count = try std.math.add(usize, spur_idx, spur.nodes.len);
    const edge_count = try std.math.add(usize, spur_idx, spur.edges.len);
    var total = try std.math.add(usize, @sizeOf(Path), try std.math.mul(usize, node_count, @sizeOf([]const u8)));
    total = try std.math.add(usize, total, try std.math.mul(usize, edge_count, @sizeOf(PathEdge)));
    for (root.nodes[0..spur_idx]) |node| total = try std.math.add(usize, total, node.len);
    for (spur.nodes) |node| total = try std.math.add(usize, total, node.len);
    for (root.edges[0..spur_idx]) |edge| {
        for ([_][]const u8{ edge.source, edge.target, edge.edge_type, edge.metadata }) |part|
            total = try std.math.add(usize, total, part.len);
    }
    for (spur.edges) |edge| {
        for ([_][]const u8{ edge.source, edge.target, edge.edge_type, edge.metadata }) |part|
            total = try std.math.add(usize, total, part.len);
    }
    return total;
}

fn joinPaths(alloc: Allocator, root: *const Path, spur_idx: usize, spur: *const Path) !Path {
    // root[0..spur_idx] + spur[0..]
    const root_node_count = spur_idx;
    const total_nodes = root_node_count + spur.nodes.len;
    const total_edges = root_node_count + spur.edges.len;

    const nodes = try alloc.alloc([]const u8, total_nodes);
    var initialized_nodes: usize = 0;
    errdefer {
        for (nodes[0..initialized_nodes]) |node| alloc.free(node);
        alloc.free(nodes);
    }
    const edges = try alloc.alloc(PathEdge, total_edges);
    var initialized_edges: usize = 0;
    errdefer {
        for (edges[0..initialized_edges]) |edge| {
            alloc.free(edge.source);
            alloc.free(edge.target);
            alloc.free(edge.edge_type);
            if (edge.metadata.len > 0) alloc.free(edge.metadata);
        }
        alloc.free(edges);
    }

    // Copy root nodes and edges up to spur_idx
    for (0..root_node_count) |i| {
        nodes[i] = try alloc.dupe(u8, root.nodes[i]);
        initialized_nodes += 1;
        edges[i] = try clonePathEdge(alloc, root.edges[i]);
        initialized_edges += 1;
    }

    // Copy spur path
    for (spur.nodes, 0..) |n, i| {
        nodes[root_node_count + i] = try alloc.dupe(u8, n);
        initialized_nodes += 1;
    }
    for (spur.edges, 0..) |e, i| {
        edges[root_node_count + i] = try clonePathEdge(alloc, e);
        initialized_edges += 1;
    }

    const tw = try sumPathEdgeWeights(edges);

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

    // Equal public keys can still identify reciprocal cross-table edges. The
    // internal K-path key and Yen root comparison must not collapse them.
    var nodes = [_][]const u8{ "shared", "shared" };
    var tables = [_]?[]const u8{ "authors", "entities" };
    var forward_edges = [_]PathEdge{.{
        .source = "shared",
        .target = "shared",
        .edge_type = "knows",
        .weight = 1,
        .traversal_direction = .out,
    }};
    var reverse_edges = forward_edges;
    reverse_edges[0].traversal_direction = .in;
    const forward = Path{
        .nodes = &nodes,
        .node_tables = &tables,
        .edges = &forward_edges,
        .total_weight = 1,
        .length = 1,
    };
    const reverse = Path{
        .nodes = &nodes,
        .node_tables = &tables,
        .edges = &reverse_edges,
        .total_weight = 1,
        .length = 1,
    };

    const forward_key = try pathToKey(alloc, &forward);
    defer alloc.free(forward_key);
    const reverse_key = try pathToKey(alloc, &reverse);
    defer alloc.free(reverse_key);

    try std.testing.expect(!std.mem.eql(u8, forward_key, reverse_key));
    try std.testing.expect(!rootPathMatches(&forward, &reverse, 1));
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

test "shortest path preflights live frontier admission" {
    const alloc = std.testing.allocator;
    var sb: [256]u8 = undefined;
    const store_path = tmpPath(&sb, "frontier-budget-s");
    defer cleanupTmp(store_path);
    var rb: [256]u8 = undefined;
    const reverse_path = tmpPath(&rb, "frontier-budget-r");
    defer cleanupTmp(reverse_path);

    var store = try docstore.DocStore.open(alloc, store_path, .{});
    defer store.close();
    var graph = try GraphIndex.open(alloc, &store, reverse_path, "test", .{});
    defer graph.close();
    try graph.addEdge("A", "B", "e", 1, 0, 0, "");
    try graph.addEdge("A", "C", "e", 1, 0, 0, "");

    var budget = work_budget_mod.WorkBudget.init(100, 100);
    try std.testing.expectError(error.GraphWorkBudgetExceeded, findShortestPath(alloc, &graph, "A", "missing", .{
        .max_intermediate_states = 1,
        .work_budget = &budget,
    }));
    try std.testing.expectEqual(work_budget_mod.Dimension.intermediate_states, budget.exhaustion().?.dimension);
    try std.testing.expectEqual(@as(usize, 0), budget.retained_state_bytes);
}

test "shortest path retained payloads use the shared request budget" {
    const alloc = std.testing.allocator;
    var sb: [256]u8 = undefined;
    const store_path = tmpPath(&sb, "retained-budget-s");
    defer cleanupTmp(store_path);
    var rb: [256]u8 = undefined;
    const reverse_path = tmpPath(&rb, "retained-budget-r");
    defer cleanupTmp(reverse_path);

    var store = try docstore.DocStore.open(alloc, store_path, .{});
    defer store.close();
    var graph = try GraphIndex.open(alloc, &store, reverse_path, "test", .{});
    defer graph.close();
    try graph.addEdge("source-with-long-identity", "target-with-long-identity", "edge-with-long-type", 1, 0, 0, "metadata-payload");

    var budget = work_budget_mod.WorkBudget.init(100, 100);
    budget.max_retained_state_bytes = 160;
    try std.testing.expectError(error.GraphWorkBudgetExceeded, findShortestPath(
        alloc,
        &graph,
        "source-with-long-identity",
        "target-with-long-identity",
        .{ .work_budget = &budget },
    ));
    try std.testing.expectEqual(work_budget_mod.Dimension.retained_state_bytes, budget.exhaustion().?.dimension);
    try std.testing.expectEqual(@as(usize, 0), budget.retained_state_bytes);
}
