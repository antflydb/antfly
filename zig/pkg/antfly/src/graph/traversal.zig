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
const platform_time = @import("antfly_platform").time;
const graph_mod = @import("graph.zig");
const Edge = graph_mod.Edge;
const EdgeDirection = graph_mod.EdgeDirection;
const GraphIndex = graph_mod.GraphIndex;
const NodeAdmission = @import("node_admission.zig").NodeAdmission;
const NodeRef = @import("node_admission.zig").NodeRef;
const node_identity = @import("node_identity.zig");
const work_budget_mod = @import("work_budget.zig");

// ============================================================================
// Traversal types
// ============================================================================

pub const TraversalRules = struct {
    edge_types: []const []const u8 = &.{}, // empty = all types
    direction: EdgeDirection = .out,
    max_depth: u32 = 3,
    min_weight: ?f64 = null,
    max_weight: ?f64 = null,
    max_results: u32 = 100,
    deduplicate: bool = true,
    include_paths: bool = false,
    node_admission: ?NodeAdmission = null,
    /// Optional query-scoped output admission. Rejected nodes remain eligible
    /// for expansion; only admitted nodes count toward `max_results`.
    result_admission: ?ResultAdmission = null,
    /// Shared request budget for expansion work. Omit only for internal callers
    /// that want the standard standalone graph limits.
    work_budget: ?*work_budget_mod.WorkBudget = null,
    /// Maximum number of pending traversal states. Kept configurable for
    /// request policy and deterministic low-limit testing.
    max_intermediate_states: usize = work_budget_mod.default_max_intermediate_states,
};

pub const ResultAdmission = struct {
    ctx: ?*anyopaque,
    admit_one: *const fn (ctx: ?*anyopaque, node: NodeRef) anyerror!bool,

    pub fn admit(self: ResultAdmission, node: NodeRef) !bool {
        return try self.admit_one(self.ctx, node);
    }
};

pub const TraversalResult = struct {
    key: []const u8,
    depth: u32,
    /// Hop distance from the start node. Traversal is breadth-first; callers
    /// that need an edge-weight cost must use a path query.
    distance: f64,
    /// Sum of raw edge weights along the selected BFS path. Kept separately
    /// from distance for the legacy direct traversal response.
    total_weight: f64,
    path: ?[]const []const u8, // if include_paths
    /// Table of the reached node, when the edge that reached it declared a
    /// cross-table endpoint (`target_table` in its metadata). Owned.
    target_table: ?[]const u8 = null,
    retained_budget: ?*work_budget_mod.WorkBudget = null,
    retained_state_bytes: usize = 0,
};

/// Extract `target_table` from an edge's metadata JSON
/// (`{"target_table":"entities",...}`) without a full parse. Returns a slice
/// into `metadata`; caller copies it if it must outlive the edge.
pub fn metadataTargetTable(metadata: []const u8) ?[]const u8 {
    const marker = "\"target_table\":\"";
    const start = std.mem.indexOf(u8, metadata, marker) orelse return null;
    const value_start = start + marker.len;
    const end = std.mem.indexOfScalarPos(u8, metadata, value_start, '"') orelse return null;
    if (end == value_start) return null;
    return metadata[value_start..end];
}

// ============================================================================
// BFS traversal
// ============================================================================

const TraversalAncestryNode = struct {
    key: []const u8,
    target_table: ?[]const u8,
    parent: ?*const TraversalAncestryNode,
};

/// Request-local traversal storage. Queue states borrow their identity and
/// ancestry from this arena, so a branch adds one node instead of cloning its
/// complete path prefix.
const TraversalAncestry = struct {
    arena: std.heap.ArenaAllocator,
    work_budget: *work_budget_mod.WorkBudget,
    retained_bytes: usize = 0,

    fn init(alloc: Allocator, work_budget: *work_budget_mod.WorkBudget) TraversalAncestry {
        return .{ .arena = std.heap.ArenaAllocator.init(alloc), .work_budget = work_budget };
    }

    fn deinit(self: *TraversalAncestry) void {
        self.work_budget.releaseStateBytes(self.retained_bytes);
        self.arena.deinit();
        self.* = undefined;
    }

    fn append(
        self: *TraversalAncestry,
        key: []const u8,
        target_table: ?[]const u8,
        parent: ?*const TraversalAncestryNode,
    ) !*const TraversalAncestryNode {
        var added = std.math.add(usize, @sizeOf(TraversalAncestryNode) + @sizeOf(QueueEntry), key.len) catch
            return self.work_budget.exhaust(.retained_state_bytes, self.work_budget.max_retained_state_bytes);
        if (target_table) |table| added = std.math.add(usize, added, table.len) catch
            return self.work_budget.exhaust(.retained_state_bytes, self.work_budget.max_retained_state_bytes);
        try self.work_budget.retainStateBytes(added);
        errdefer self.work_budget.releaseStateBytes(added);

        const arena_alloc = self.arena.allocator();
        const owned_key = try arena_alloc.dupe(u8, key);
        const owned_table = if (target_table) |table| try arena_alloc.dupe(u8, table) else null;
        const node = try arena_alloc.create(TraversalAncestryNode);
        node.* = .{ .key = owned_key, .target_table = owned_table, .parent = parent };
        self.retained_bytes += added;
        return node;
    }
};

const QueueEntry = struct {
    ancestry: *const TraversalAncestryNode,
    depth: u32,
    total_weight: f64,
};

/// Perform BFS graph traversal from start_key using the given rules.
/// Caller owns all returned memory (use freeResults to clean up).
pub fn traverse(alloc: Allocator, graph_index: *GraphIndex, start_key: []const u8, rules: TraversalRules) ![]TraversalResult {
    const Reader = struct {
        graph_index: *GraphIndex,

        pub fn getEdges(self: @This(), a: Allocator, key: []const u8, direction: EdgeDirection) ![]Edge {
            return try self.graph_index.getEdges(a, key, "", direction);
        }

        pub fn getEdgesBoundedForTraversal(
            self: @This(),
            a: Allocator,
            key: []const u8,
            edge_types: []const []const u8,
            direction: EdgeDirection,
            max_edges: usize,
            max_bytes: usize,
        ) ![]Edge {
            return try self.graph_index.getEdgesByTypesBounded(a, key, edge_types, direction, max_edges, max_bytes);
        }

        pub fn freeEdges(_: @This(), a: Allocator, edges: []Edge) void {
            GraphIndex.freeEdges(a, edges);
        }
    };
    return try traverseWithEdgeReader(alloc, Reader{ .graph_index = graph_index }, start_key, rules);
}

/// Reader-generic traversal over an immutable graph snapshot. The reader owns
/// the representation-specific edge lookup and cleanup while traversal keeps
/// one implementation of filtering, deduplication, and admission semantics.
pub fn traverseWithEdgeReader(
    alloc: Allocator,
    edge_reader: anytype,
    start_key: []const u8,
    rules: TraversalRules,
) ![]TraversalResult {
    var local_work_budget = work_budget_mod.WorkBudget.init(
        work_budget_mod.default_max_explored_nodes,
        work_budget_mod.default_max_explored_edges,
    );
    var effective_rules = rules;
    if (effective_rules.work_budget == null) effective_rules.work_budget = &local_work_budget;
    const work_budget = effective_rules.work_budget.?;
    const returned_state_budget = rules.work_budget;

    var results = std.ArrayListUnmanaged(TraversalResult).empty;
    errdefer {
        freeResults(alloc, results.items);
        results.deinit(alloc);
    }

    if (effective_rules.node_admission) |admission| {
        if (!try startNodeAdmittedWithEdgeReader(alloc, edge_reader, start_key, effective_rules.direction, admission, work_budget)) {
            return try results.toOwnedSlice(alloc);
        }
    }

    var visited = node_identity.Map(void){};
    defer visited.deinit(alloc);
    var visited_retained_bytes: usize = 0;
    defer work_budget.releaseStateBytes(visited_retained_bytes);

    var ancestry = TraversalAncestry.init(alloc, work_budget);
    defer ancestry.deinit();

    // Queue
    var queue = std.ArrayListUnmanaged(QueueEntry).empty;
    var queue_head: usize = 0;
    defer queue.deinit(alloc);

    // Seed with start node
    try work_budget.checkIntermediateStates(1, effective_rules.max_intermediate_states);
    try work_budget.consumeNode();
    const start_ancestry = try ancestry.append(start_key, null, null);
    try queue.append(alloc, .{
        .ancestry = start_ancestry,
        .depth = 0,
        .total_weight = 0,
    });

    if (effective_rules.deduplicate) {
        _ = try putVisitedRetained(alloc, &visited, .{ .table = null, .key = start_key }, work_budget, &visited_retained_bytes);
    }

    while (queue_head < queue.items.len) {
        // Dequeue from front (index-tracked)
        const current = queue.items[queue_head];
        queue_head += 1;

        // Add to results (skip depth 0 = start node)
        const include_in_results = current.depth > 0 and
            (effective_rules.result_admission == null or try effective_rules.result_admission.?.admit(.{
                .key = current.ancestry.key,
                .table = current.ancestry.target_table,
                .external = current.ancestry.target_table != null,
            }));
        if (include_in_results) {
            const result = try traversalResultFromQueueEntry(alloc, current, effective_rules.include_paths, work_budget, returned_state_budget);
            var result_owned = true;
            errdefer if (result_owned) freeResult(alloc, result);
            try results.append(alloc, result);
            result_owned = false;

            if (effective_rules.max_results > 0 and results.items.len >= effective_rules.max_results) {
                break;
            }
        }

        // Check max depth
        if (effective_rules.max_depth > 0 and current.depth >= effective_rules.max_depth) continue;

        // Cross-table nodes are expanded by the distributed owner router.
        // Looking them up in this source-table index aliases distinct node
        // namespaces when their keys happen to be equal.
        if (current.ancestry.target_table != null) continue;

        // Get edges
        const edges = try getEdgesForTraversalBudget(alloc, edge_reader, current.ancestry.key, effective_rules, work_budget);
        defer edge_reader.freeEdges(alloc, edges);
        try work_budget.consumeMaterializedEdges(edges);

        const admitted_edges = if (effective_rules.node_admission) |admission| blk: {
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
                if (!shouldTraverseEdge(&effective_rules, &edge)) continue;
                const next_key = if (std.mem.eql(u8, current.ancestry.key, edge.source)) edge.target else edge.source;
                const target_table = if (std.mem.eql(u8, next_key, edge.target))
                    metadataTargetTable(edge.metadata)
                else
                    null;
                if (effective_rules.deduplicate and visited.contains(.{
                    .table = target_table,
                    .key = next_key,
                })) continue;
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
            const next_key = if (std.mem.eql(u8, current.ancestry.key, edge.source)) edge.target else edge.source;

            if (admitted_edges) |mask| {
                if (!mask[edge_index]) continue;
            } else {
                if (!shouldTraverseEdge(&effective_rules, &edge)) continue;
            }
            const target_table = if (std.mem.eql(u8, next_key, edge.target))
                metadataTargetTable(edge.metadata)
            else
                null;
            if (effective_rules.deduplicate and !try putVisitedRetained(
                alloc,
                &visited,
                .{ .table = target_table, .key = next_key },
                work_budget,
                &visited_retained_bytes,
            )) continue;

            const pending_states = queue.items.len - queue_head;
            try work_budget.checkIntermediateStates(pending_states + 1, effective_rules.max_intermediate_states);
            try work_budget.consumeNode();
            const next_ancestry = try ancestry.append(next_key, target_table, current.ancestry);
            const total_weight = current.total_weight + edge.weight;
            if (!std.math.isFinite(total_weight)) return error.GraphPathWeightOverflow;
            try queue.append(alloc, .{
                .ancestry = next_ancestry,
                .depth = current.depth + 1,
                .total_weight = total_weight,
            });
        }
    }

    const owned = try alloc.dupe(TraversalResult, results.items);
    results.deinit(alloc);
    return owned;
}

fn traversalResultFromQueueEntry(
    alloc: Allocator,
    entry: QueueEntry,
    include_path: bool,
    work_budget: *work_budget_mod.WorkBudget,
    returned_state_budget: ?*work_budget_mod.WorkBudget,
) !TraversalResult {
    const retained_bytes = traversalResultRetainedBytes(entry, include_path) catch
        return work_budget.exhaust(.retained_state_bytes, work_budget.max_retained_state_bytes);
    try work_budget.retainStateBytes(retained_bytes);
    errdefer work_budget.releaseStateBytes(retained_bytes);

    const key = try alloc.dupe(u8, entry.ancestry.key);
    errdefer alloc.free(key);
    const path = if (include_path) blk: {
        const path_len: usize = @as(usize, entry.depth) + 1;
        const owned = try alloc.alloc([]const u8, path_len);
        var initialized: usize = 0;
        errdefer {
            for (owned[path_len - initialized ..]) |item| alloc.free(item);
            alloc.free(owned);
        }
        var cursor: ?*const TraversalAncestryNode = entry.ancestry;
        var i = path_len;
        while (cursor) |node| : (cursor = node.parent) {
            i -= 1;
            owned[i] = try alloc.dupe(u8, node.key);
            initialized += 1;
        }
        break :blk owned;
    } else null;
    errdefer if (path) |items| {
        for (items) |item| alloc.free(item);
        alloc.free(items);
    };
    const target_table = if (entry.ancestry.target_table) |table|
        try alloc.dupe(u8, table)
    else
        null;
    errdefer if (target_table) |table| alloc.free(table);
    return .{
        .key = key,
        .depth = entry.depth,
        .distance = @floatFromInt(entry.depth),
        .total_weight = entry.total_weight,
        .path = path,
        .target_table = target_table,
        .retained_budget = returned_state_budget,
        .retained_state_bytes = retained_bytes,
    };
}

fn traversalResultRetainedBytes(entry: QueueEntry, include_path: bool) !usize {
    var total = try std.math.add(usize, @sizeOf(TraversalResult), entry.ancestry.key.len);
    if (entry.ancestry.target_table) |table| total = try std.math.add(usize, total, table.len);
    if (include_path) {
        const path_len: usize = @as(usize, entry.depth) + 1;
        total = try std.math.add(usize, total, try std.math.mul(usize, path_len, @sizeOf([]const u8)));
        var cursor: ?*const TraversalAncestryNode = entry.ancestry;
        while (cursor) |node| : (cursor = node.parent) {
            total = try std.math.add(usize, total, node.key.len);
        }
    }
    return total;
}

fn putVisitedRetained(
    alloc: Allocator,
    visited: *node_identity.Map(void),
    identity: node_identity.Ref,
    work_budget: *work_budget_mod.WorkBudget,
    retained_bytes: *usize,
) !bool {
    if (visited.contains(identity)) return false;
    var added = std.math.add(usize, @sizeOf(node_identity.Ref), identity.key.len) catch
        return work_budget.exhaust(.retained_state_bytes, work_budget.max_retained_state_bytes);
    if (identity.table) |table| added = std.math.add(usize, added, table.len) catch
        return work_budget.exhaust(.retained_state_bytes, work_budget.max_retained_state_bytes);
    try work_budget.retainStateBytes(added);
    errdefer work_budget.releaseStateBytes(added);
    if (!try visited.putIfAbsent(alloc, identity, {})) {
        work_budget.releaseStateBytes(added);
        return false;
    }
    retained_bytes.* += added;
    return true;
}

/// Admit a traversal start according to the role it plays in this direction.
/// Resolver-produced cross-table targets are identified from incoming edge
/// metadata only after normal local admission rejects the key.
pub fn startNodeAdmitted(
    alloc: Allocator,
    graph_index: *GraphIndex,
    start_key: []const u8,
    direction: EdgeDirection,
    admission: NodeAdmission,
) !bool {
    const Reader = struct {
        graph_index: *GraphIndex,

        pub fn getEdges(self: @This(), a: Allocator, key: []const u8, edge_direction: EdgeDirection) ![]Edge {
            return try self.graph_index.getEdges(a, key, "", edge_direction);
        }

        pub fn freeEdges(_: @This(), a: Allocator, edges: []Edge) void {
            GraphIndex.freeEdges(a, edges);
        }
    };
    return try startNodeAdmittedWithEdgeReader(
        alloc,
        Reader{ .graph_index = graph_index },
        start_key,
        direction,
        admission,
        null,
    );
}

pub fn startNodeAdmittedWithEdgeReader(
    alloc: Allocator,
    edge_reader: anytype,
    start_key: []const u8,
    direction: EdgeDirection,
    admission: NodeAdmission,
    work_budget: ?*work_budget_mod.WorkBudget,
) !bool {
    const statically_external = admission.external_targets and direction == .in;
    const keys = [_][]const u8{start_key};
    const admitted = try admission.filterKeysAlloc(alloc, &keys, statically_external);
    defer alloc.free(admitted);
    if (admitted[0] or statically_external or direction == .out) return admitted[0];

    const incoming = if (work_budget) |budget|
        try getEdgesForTraversalBudget(alloc, edge_reader, start_key, .{
            .direction = .in,
            .work_budget = budget,
        }, budget)
    else
        try edge_reader.getEdges(alloc, start_key, .in);
    defer edge_reader.freeEdges(alloc, incoming);
    if (work_budget) |budget| try budget.consumeMaterializedEdges(incoming);
    for (incoming) |edge| {
        if (std.mem.eql(u8, edge.target, start_key) and
            (admission.external_targets or metadataTargetTable(edge.metadata) != null))
        {
            return true;
        }
    }
    return false;
}

fn getEdgesForTraversalBudget(
    alloc: Allocator,
    edge_reader: anytype,
    key: []const u8,
    rules: TraversalRules,
    work_budget: *work_budget_mod.WorkBudget,
) ![]Edge {
    if (comptime @hasDecl(@TypeOf(edge_reader), "getEdgesBoundedForTraversal")) {
        return edge_reader.getEdgesBoundedForTraversal(
            alloc,
            key,
            rules.edge_types,
            rules.direction,
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
    return try edge_reader.getEdges(alloc, key, rules.direction);
}

fn shouldTraverseEdge(rules: *const TraversalRules, edge: *const Edge) bool {
    // Weight filter
    if (rules.min_weight) |min_weight| if (edge.weight < min_weight) return false;
    if (rules.max_weight) |max_weight| if (edge.weight > max_weight) return false;

    // Edge type filter
    if (rules.edge_types.len > 0) {
        for (rules.edge_types) |et| {
            if (std.mem.eql(u8, edge.edge_type, et)) return true;
        }
        return false;
    }
    return true;
}

test "traversal weight filters preserve explicit zero bounds" {
    const zero = Edge{ .source = "a", .target = "b", .edge_type = "e", .weight = 0, .created_at = 0, .updated_at = 0, .metadata = "" };
    const positive = Edge{ .source = "a", .target = "b", .edge_type = "e", .weight = 0.1, .created_at = 0, .updated_at = 0, .metadata = "" };
    const negative = Edge{ .source = "a", .target = "b", .edge_type = "e", .weight = -0.1, .created_at = 0, .updated_at = 0, .metadata = "" };
    const max_zero = TraversalRules{ .max_weight = 0 };
    const min_zero = TraversalRules{ .min_weight = 0 };
    try std.testing.expect(shouldTraverseEdge(&max_zero, &zero));
    try std.testing.expect(!shouldTraverseEdge(&max_zero, &positive));
    try std.testing.expect(shouldTraverseEdge(&max_zero, &negative));
    try std.testing.expect(!shouldTraverseEdge(&min_zero, &negative));
}

/// Free traversal results.
fn freeResult(alloc: Allocator, result: TraversalResult) void {
    alloc.free(result.key);
    if (result.path) |path| {
        for (path) |key| alloc.free(key);
        alloc.free(path);
    }
    if (result.target_table) |table| alloc.free(table);
    if (result.retained_budget) |budget| budget.releaseStateBytes(result.retained_state_bytes);
}

pub fn freeResults(alloc: Allocator, results: []const TraversalResult) void {
    for (results) |result| freeResult(alloc, result);
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
    try g.addEdge("A", "B", "next", 2.0, 0, 0, "");
    try g.addEdge("B", "C", "next", 3.0, 0, 0, "");
    try g.addEdge("C", "D", "next", 1.0, 0, 0, "");

    // Depth 2: should reach B and C but not D
    const results = try traverse(alloc, &g, "A", .{ .max_depth = 2 });
    defer freeOwnedResults(alloc, results);

    try std.testing.expectEqual(@as(usize, 2), results.len);
    try std.testing.expectEqualStrings("B", results[0].key);
    try std.testing.expectEqual(@as(u32, 1), results[0].depth);
    try std.testing.expectEqual(@as(f64, 1), results[0].distance);
    try std.testing.expectEqualStrings("C", results[1].key);
    try std.testing.expectEqual(@as(u32, 2), results[1].depth);
    try std.testing.expectEqual(@as(f64, 2), results[1].distance);
    try std.testing.expectEqual(@as(f64, 5), results[1].total_weight);
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

test "traversal deduplicates by table-scoped node identity" {
    const alloc = std.testing.allocator;
    var sb: [256]u8 = undefined;
    const sp = tmpPath(&sb, "table-identity-store");
    defer cleanupTmp(sp);
    var rb: [256]u8 = undefined;
    const rp = tmpPath(&rb, "table-identity-graph");
    defer cleanupTmp(rp);

    var store = try docstore.DocStore.open(alloc, sp, .{});
    defer store.close();
    var g = try GraphIndex.open(alloc, &store, rp, "test", .{});
    defer g.close();

    try g.addEdge("A", "shared", "local", 1.0, 0, 0, "");
    try g.addEdge(
        "A",
        "shared",
        "external",
        1.0,
        0,
        0,
        "{\"target_table\":\"entities\"}",
    );

    const results = try traverse(alloc, &g, "A", .{
        .max_depth = 1,
        .deduplicate = true,
    });
    defer freeOwnedResults(alloc, results);

    try std.testing.expectEqual(@as(usize, 2), results.len);
    var local_count: usize = 0;
    var external_count: usize = 0;
    for (results) |result| {
        try std.testing.expectEqualStrings("shared", result.key);
        if (result.target_table) |table| {
            try std.testing.expectEqualStrings("entities", table);
            external_count += 1;
        } else {
            local_count += 1;
        }
    }
    try std.testing.expectEqual(@as(usize, 1), local_count);
    try std.testing.expectEqual(@as(usize, 1), external_count);
}

test "local traversal does not expand a cross-table node in the source index" {
    const alloc = std.testing.allocator;
    var sb: [256]u8 = undefined;
    const sp = tmpPath(&sb, "external-terminal-store");
    defer cleanupTmp(sp);
    var rb: [256]u8 = undefined;
    const rp = tmpPath(&rb, "external-terminal-graph");
    defer cleanupTmp(rp);

    var store = try docstore.DocStore.open(alloc, sp, .{});
    defer store.close();
    var graph = try GraphIndex.open(alloc, &store, rp, "test", .{});
    defer graph.close();

    try graph.addEdge(
        "A",
        "shared",
        "external",
        1.0,
        0,
        0,
        "{\"target_table\":\"entities\"}",
    );
    try graph.addEdge("shared", "source-only", "local", 1.0, 0, 0, "");

    const results = try traverse(alloc, &graph, "A", .{
        .max_depth = 2,
        .deduplicate = true,
    });
    defer freeOwnedResults(alloc, results);

    try std.testing.expectEqual(@as(usize, 1), results.len);
    try std.testing.expectEqualStrings("shared", results[0].key);
    try std.testing.expectEqualStrings("entities", results[0].target_table.?);
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

test "traversal preflights live frontier admission before ownership transfer" {
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
    try std.testing.expectError(error.GraphWorkBudgetExceeded, traverse(alloc, &graph, "A", .{
        .max_depth = 2,
        .max_intermediate_states = 1,
        .work_budget = &budget,
    }));
    try std.testing.expectEqual(work_budget_mod.Dimension.intermediate_states, budget.exhaustion().?.dimension);
    try std.testing.expectEqual(@as(usize, 0), budget.retained_state_bytes);
}

test "traversal ancestry and returned paths share retained state budget" {
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
    try graph.addEdge("source-with-long-identity", "target-with-long-identity", "e", 1, 0, 0, "");

    var budget = work_budget_mod.WorkBudget.init(100, 100);
    budget.max_retained_state_bytes = 128;
    try std.testing.expectError(error.GraphWorkBudgetExceeded, traverse(alloc, &graph, "source-with-long-identity", .{
        .max_depth = 1,
        .include_paths = true,
        .work_budget = &budget,
    }));
    try std.testing.expectEqual(work_budget_mod.Dimension.retained_state_bytes, budget.exhaustion().?.dimension);
    try std.testing.expectEqual(@as(usize, 0), budget.retained_state_bytes);
}
