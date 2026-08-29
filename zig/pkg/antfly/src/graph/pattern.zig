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
const Allocator = std.mem.Allocator;
const graph_mod = @import("graph.zig");
const identifier_policy = @import("identifier_policy_generated.zig");
const NodeAdmission = @import("node_admission.zig").NodeAdmission;
const NodeRef = @import("node_admission.zig").NodeRef;
const paths_mod = @import("paths.zig");
const traversal_mod = @import("traversal.zig");
const node_identity = @import("node_identity.zig");
const work_budget_diagnostic = @import("work_budget_diagnostic.zig");
const work_budget_mod = @import("work_budget.zig");

pub const max_pattern_steps: usize = 64;
pub const max_pattern_hops: u32 = 64;
pub const max_conjunctive_nodes: usize = 64;
pub const max_conjunctive_edges: usize = 64;
pub const max_optional_patterns: usize = 64;
pub const max_match_predicates: usize = 64;
pub const max_match_predicate_depth: usize = 16;
pub const max_count_aggregates: usize = 64;
pub const max_identifier_codepoints = identifier_policy.max_codepoints;
pub const max_identifier_bytes = identifier_policy.max_utf8_bytes;
pub const default_max_explored_nodes = work_budget_mod.default_max_explored_nodes;
pub const default_max_explored_edges = work_budget_mod.default_max_explored_edges;
pub const default_max_explored_edge_bytes = work_budget_mod.default_max_explored_edge_bytes;
pub const default_max_scanned_anchors = work_budget_mod.default_max_scanned_anchors;
pub const default_max_intermediate_states = work_budget_mod.default_max_intermediate_states;
pub const default_max_retained_state_bytes = work_budget_mod.default_max_retained_state_bytes;
pub const default_max_distinct_identities = work_budget_mod.default_max_distinct_identities;
pub const default_max_distinct_state_bytes = work_budget_mod.default_max_distinct_state_bytes;

pub const NodeFilter = struct {
    filter_prefix: []const u8 = "",
    filter_query_json: ?[]const u8 = null,
};

pub fn nodeFilterActive(filter: NodeFilter) bool {
    return filter.filter_prefix.len > 0 or filter.filter_query_json != null;
}

pub const FilterEvaluator = struct {
    ctx: ?*anyopaque = null,
    func: ?*const fn (?*anyopaque, node_identity.Ref, NodeFilter) anyerror!bool = null,
    batch_func: ?*const fn (?*anyopaque, Allocator, []const node_identity.Ref, NodeFilter) anyerror![]bool = null,
};

pub const PatternEdgeStep = struct {
    direction: graph_mod.EdgeDirection = .out,
    min_hops: u32 = 1,
    max_hops: u32 = 1,
    min_weight: ?f64 = null,
    max_weight: ?f64 = null,
    types: []const []const u8 = &.{},
};

/// A table declaration needs an outer optional because null is itself the
/// canonical spelling of the queried table. Keeping physical routing out of
/// PatternEdgeStep prevents planner state from leaking into the public AST.
const DeclaredTableRoute = struct {
    table: ?[]const u8,
};

/// One physically oriented adjacency read. Cross-table `both` expands to an
/// outgoing read from the bound node's table and an incoming read from the
/// opposite alias's declared source table. For an incoming read, the physical
/// source table is also the table of the reached node.
const PhysicalEdgeTraversal = struct {
    step: PatternEdgeStep,
    incoming_source: ?DeclaredTableRoute = null,
};

pub const PatternStep = struct {
    alias: []const u8 = "",
    edge: PatternEdgeStep = .{},
    node_filter: NodeFilter = .{},
};

pub const PatternBinding = struct {
    alias: []u8,
    key: []u8,
    table: ?[]u8 = null,
    depth: u32,

    pub fn deinit(self: *PatternBinding, alloc: Allocator) void {
        alloc.free(self.alias);
        alloc.free(self.key);
        if (self.table) |table| alloc.free(table);
        self.* = undefined;
    }
};

pub const PatternMatch = struct {
    bindings: []PatternBinding,
    path: []paths_mod.PathEdge,
    /// Aliases introduced by an OPTIONAL MATCH that did not match. Keeping
    /// these separate from concrete bindings preserves the existing compact
    /// binding representation while giving the public row encoder true null
    /// semantics.
    null_aliases: [][]u8 = &.{},
    retained_budget: ?*WorkBudget = null,
    retained_state_bytes: usize = 0,

    /// Transfer every owned slice out of this match and leave it safe to
    /// deinitialize. Keeping the move operation with the owning type prevents
    /// callers from overlooking newly added owned fields.
    pub fn take(self: *PatternMatch) PatternMatch {
        const owned = self.*;
        self.* = .{ .bindings = &.{}, .path = &.{}, .null_aliases = &.{} };
        return owned;
    }

    pub fn deinit(self: *PatternMatch, alloc: Allocator) void {
        for (self.bindings) |*binding| binding.deinit(alloc);
        if (self.bindings.len > 0) alloc.free(self.bindings);
        freePathEdges(alloc, self.path);
        for (self.null_aliases) |alias| alloc.free(alias);
        if (self.null_aliases.len > 0) alloc.free(self.null_aliases);
        if (self.retained_budget) |budget| budget.releaseStateBytes(self.retained_state_bytes);
        self.* = undefined;
    }
};

test "pattern match take transfers optional null alias ownership" {
    const alloc = std.testing.allocator;
    const null_aliases = try alloc.alloc([]u8, 1);
    null_aliases[0] = try alloc.dupe(u8, "optional_alias");

    var source: PatternMatch = .{
        .bindings = @constCast((&[_]PatternBinding{})[0..]),
        .path = @constCast((&[_]paths_mod.PathEdge{})[0..]),
        .null_aliases = null_aliases,
    };
    var owned = source.take();
    defer owned.deinit(alloc);

    try std.testing.expectEqual(@as(usize, 0), source.bindings.len);
    try std.testing.expectEqual(@as(usize, 0), source.path.len);
    try std.testing.expectEqual(@as(usize, 0), source.null_aliases.len);
    try std.testing.expectEqualStrings("optional_alias", owned.null_aliases[0]);
    source.deinit(alloc);
}

pub const MatchNode = struct {
    alias: []const u8,
    /// Null denotes the query table; otherwise this is the alias's owning
    /// table and participates in exact identity matching and reverse routing.
    table: ?[]const u8 = null,
    filter: NodeFilter = .{},
};

/// A structural edge between aliases. Direction is read from `from` toward
/// `to`: `in` therefore follows a stored incoming edge, and `both` accepts
/// either physical orientation.
pub const MatchEdge = struct {
    from: []const u8,
    to: []const u8,
    step: PatternEdgeStep = .{},
};

pub const MatchPredicate = union(enum) {
    not_equal: struct { left: []const u8, right: []const u8 },
    not_exists: []const MatchEdge,
};

pub const OptionalPattern = struct {
    nodes: []const MatchNode = &.{},
    edges: []const MatchEdge,
    predicates: []const MatchPredicate = &.{},
};

pub const ConjunctivePattern = struct {
    /// Alias whose bindings are enumerated from the query's source table.
    /// Making this explicit keeps cross-table patterns independent of planner
    /// heuristics: every other alias may be reached through table-qualified
    /// graph edges without changing the anchor relation's domain.
    anchor_alias: ?[]const u8 = null,
    nodes: []const MatchNode,
    edges: []const MatchEdge,
    predicates: []const MatchPredicate = &.{},
    optional: []const OptionalPattern = &.{},
};

pub const MatchPlan = enum {
    generic_expand,
    exact_two_edge_probe,
};

/// Optional per-query instrumentation for benchmarks and request diagnostics.
/// Counters describe logical matcher work rather than backend implementation
/// details, so they remain comparable across local and serverless readers.
pub const MatchStats = struct {
    plan: MatchPlan = .generic_expand,
    adjacency_reads: usize = 0,
    adjacency_edges: usize = 0,
    exact_edge_probes: usize = 0,
    exact_edge_matches: usize = 0,
};

pub fn freeMatches(alloc: Allocator, matches: []PatternMatch) void {
    for (matches) |*match| match.deinit(alloc);
    if (matches.len > 0) alloc.free(matches);
}

pub const MatchOptions = struct {
    max_results: u32 = 100,
    /// Disable row-oriented expansion windows. Safety still comes from the
    /// explicit node, edge, and intermediate-state budgets, which fail closed.
    require_complete: bool = false,
    return_aliases: []const []const u8 = &.{},
    target_nodes: []const node_identity.Ref = &.{},
    target_required: bool = false,
    include_paths: bool = true,
    evaluator: ?FilterEvaluator = null,
    node_admission: ?NodeAdmission = null,
    /// Whether the caller has already applied both node admission and the
    /// selected anchor's node filter to every supplied start identity. This is
    /// trusted execution metadata, not a query option: reached nodes always go
    /// through the normal admission and filter paths.
    start_validation: enum { required, prevalidated } = .required,
    /// The caller already charged the physical source rows inspected while
    /// producing `start_keys`. Used by runtimes that filter an in-memory source
    /// relation before handing a page to the matcher.
    anchors_precharged: bool = false,
    stats: ?*MatchStats = null,
    max_explored_nodes: usize = default_max_explored_nodes,
    max_explored_edges: usize = default_max_explored_edges,
    max_intermediate_states: usize = default_max_intermediate_states,
    /// Optional request-scoped work budget shared by cursor pages. Source rows,
    /// reached nodes, and examined edges have independent dimensions so a cheap
    /// anchor scan cannot consume the expansion allowance or run unbounded.
    work_budget: ?*WorkBudget = null,
    /// Optional request-scoped budget for the exact identity sets retained by
    /// count(distinct alias). The budget is shared across aggregate specs and
    /// cursor pages so exact aggregation either completes or fails closed with
    /// bounded memory.
    distinct_budget: ?*DistinctBudget = null,
};

pub const WorkBudget = work_budget_mod.WorkBudget;

fn edgeOwnedBytes(edges: []const graph_mod.Edge) !usize {
    var total: usize = 0;
    for (edges) |edge| {
        total = std.math.add(usize, total, @sizeOf(graph_mod.Edge)) catch
            return error.QueryCandidateBudgetExceeded;
        total = std.math.add(usize, total, edge.source.len) catch
            return error.QueryCandidateBudgetExceeded;
        total = std.math.add(usize, total, edge.target.len) catch
            return error.QueryCandidateBudgetExceeded;
        total = std.math.add(usize, total, edge.edge_type.len) catch
            return error.QueryCandidateBudgetExceeded;
        total = std.math.add(usize, total, edge.metadata.len) catch
            return error.QueryCandidateBudgetExceeded;
    }
    return total;
}

fn probedEdgeOwnedBytes(edges: []const ?graph_mod.Edge) !usize {
    var total: usize = 0;
    for (edges) |maybe_edge| {
        const edge = maybe_edge orelse continue;
        total = std.math.add(usize, total, @sizeOf(graph_mod.Edge)) catch
            return error.QueryCandidateBudgetExceeded;
        total = std.math.add(usize, total, edge.source.len) catch
            return error.QueryCandidateBudgetExceeded;
        total = std.math.add(usize, total, edge.target.len) catch
            return error.QueryCandidateBudgetExceeded;
        total = std.math.add(usize, total, edge.edge_type.len) catch
            return error.QueryCandidateBudgetExceeded;
        total = std.math.add(usize, total, edge.metadata.len) catch
            return error.QueryCandidateBudgetExceeded;
    }
    return total;
}

fn getEdgesForBudget(
    alloc: Allocator,
    edge_reader: anytype,
    table: ?[]const u8,
    key: []const u8,
    edge_types: []const []const u8,
    direction: graph_mod.EdgeDirection,
    work_budget: *WorkBudget,
    source_table_declared: bool,
) ![]graph_mod.Edge {
    if (comptime @hasDecl(@TypeOf(edge_reader), "getEdgesBoundedForPattern")) {
        return edge_reader.getEdgesBoundedForPattern(
            alloc,
            table,
            key,
            edge_types,
            direction,
            work_budget.edgeLimit(),
            work_budget.edgeByteLimit(),
            source_table_declared,
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
    if (comptime @hasDecl(@TypeOf(edge_reader), "getEdgesBounded")) {
        return edge_reader.getEdgesBounded(
            alloc,
            table,
            key,
            edge_types,
            direction,
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
    return try edge_reader.getEdges(alloc, table, key, edge_types, direction);
}

fn consumeMaterializedEdges(work_budget: *WorkBudget, edges: []const graph_mod.Edge) !void {
    try work_budget.consumeMaterializedEdges(edges);
}

pub fn isValidIdentifier(value: []const u8) bool {
    return identifier_policy.isValid(value);
}

/// Table qualifiers preserve exact identity bytes but must contain at least
/// one non-ASCII-whitespace byte. This matches the public graph schema and
/// keeps omission distinct from an invalid present-but-blank qualifier.
pub fn isValidTableQualifier(value: []const u8) bool {
    return std.mem.trim(u8, value, " \t\r\n").len > 0;
}

test "graph table qualifiers preserve exact nonblank identity bytes" {
    try std.testing.expect(isValidTableQualifier("docs"));
    try std.testing.expect(isValidTableQualifier(" docs "));
    try std.testing.expect(isValidTableQualifier("\u{00a0}"));
    try std.testing.expect(!isValidTableQualifier(""));
    try std.testing.expect(!isValidTableQualifier(" \t\r\n"));
}

test "graph identifiers match the versioned wire-policy conformance cases" {
    try std.testing.expectEqualStrings("15.0.0", identifier_policy.unicode_version);
    for (identifier_policy.conformance_cases) |case| {
        errdefer std.log.err("graph identifier conformance case failed: {s}", .{case.name});
        try std.testing.expectEqual(case.valid, isValidIdentifier(case.value));
    }
}

pub const DistinctBudget = struct {
    remaining_identities: usize,
    remaining_state_bytes: usize,

    pub fn init(max_identities: usize, max_state_bytes: usize) DistinctBudget {
        return .{
            .remaining_identities = max_identities,
            .remaining_state_bytes = max_state_bytes,
        };
    }

    pub fn consume(self: *DistinctBudget, ref: node_identity.Ref) !void {
        const identity_bytes = std.math.add(
            usize,
            if (ref.table) |table| table.len else 0,
            ref.key.len,
        ) catch return error.GraphDistinctBudgetExceeded;
        if (self.remaining_identities == 0 or identity_bytes > self.remaining_state_bytes)
            return error.GraphDistinctBudgetExceeded;
        self.remaining_identities -= 1;
        self.remaining_state_bytes -= identity_bytes;
    }

    /// Charge retained container storage used to index and return exact
    /// identities. The byte ceiling is deliberately conservative and
    /// request-consumptive: transient reallocations cannot be used to exceed
    /// admission by repeatedly growing and releasing distinct sets.
    pub fn consumeRetainedBytes(self: *DistinctBudget, bytes: usize) !void {
        if (bytes > self.remaining_state_bytes)
            return error.GraphDistinctBudgetExceeded;
        self.remaining_state_bytes -= bytes;
    }
};

pub fn ensureBorrowedDistinctMapCapacity(
    comptime Value: type,
    alloc: Allocator,
    budget: *DistinctBudget,
    map: *node_identity.BorrowedMap(Value),
    count: usize,
) !void {
    const target_capacity = work_budget_mod.hashMapCapacityForCount(
        count,
        std.hash_map.default_max_load_percentage,
    ) catch return error.GraphDistinctBudgetExceeded;
    if (target_capacity <= map.capacity()) return;
    const current_bytes = work_budget_mod.hashMapRetainedBytes(
        node_identity.Ref,
        Value,
        map.capacity(),
    ) catch return error.GraphDistinctBudgetExceeded;
    const target_bytes = work_budget_mod.hashMapRetainedBytes(
        node_identity.Ref,
        Value,
        target_capacity,
    ) catch return error.GraphDistinctBudgetExceeded;
    try budget.consumeRetainedBytes(target_bytes - current_bytes);
    try map.ensureTotalCapacity(alloc, count);
}

pub fn ensureDistinctValueCapacity(
    alloc: Allocator,
    budget: *DistinctBudget,
    values: *std.ArrayListUnmanaged(node_identity.Ref),
    count: usize,
) !void {
    if (count <= values.capacity) return;
    const doubled = std.math.mul(usize, values.capacity, 2) catch
        return error.GraphDistinctBudgetExceeded;
    const target_capacity = @max(count, @max(@as(usize, 8), doubled));
    const added_capacity = target_capacity - values.capacity;
    const added_bytes = std.math.mul(
        usize,
        added_capacity,
        @sizeOf(node_identity.Ref),
    ) catch return error.GraphDistinctBudgetExceeded;
    try budget.consumeRetainedBytes(added_bytes);
    try values.ensureTotalCapacityPrecise(alloc, target_capacity);
}

const MatchState = struct {
    bindings: []PatternBinding,
    path: []paths_mod.PathEdge,

    fn deinit(self: *MatchState, alloc: Allocator) void {
        for (self.bindings) |*binding| binding.deinit(alloc);
        if (self.bindings.len > 0) alloc.free(self.bindings);
        freePathEdges(alloc, self.path);
        self.* = undefined;
    }
};

const PathAncestry = struct {
    node: node_identity.Ref,
    parent: ?*const PathAncestry,
};

/// Request-local ancestry storage. Every expanded state owns only its newest
/// identity and a parent pointer; siblings no longer copy the complete path.
/// The arena is released when one reachability walk finishes, while the byte
/// counter prevents identifier payloads from amplifying retained memory beyond
/// the public request ceiling.
const ReachabilityAncestry = struct {
    arena: std.heap.ArenaAllocator,
    work_budget: *WorkBudget,
    retained_bytes: usize = 0,

    fn init(alloc: Allocator, work_budget: *WorkBudget) ReachabilityAncestry {
        return .{
            .arena = std.heap.ArenaAllocator.init(alloc),
            .work_budget = work_budget,
        };
    }

    fn deinit(self: *ReachabilityAncestry) void {
        self.work_budget.releaseStateBytes(self.retained_bytes);
        self.arena.deinit();
        self.* = undefined;
    }

    fn append(
        self: *ReachabilityAncestry,
        key: []const u8,
        table: ?[]const u8,
        parent: ?*const PathAncestry,
    ) !*const PathAncestry {
        var added = std.math.add(usize, @sizeOf(PathAncestry), key.len) catch
            return self.work_budget.exhaust(.retained_state_bytes, self.work_budget.max_retained_state_bytes);
        if (table) |value| added = std.math.add(usize, added, value.len) catch
            return self.work_budget.exhaust(.retained_state_bytes, self.work_budget.max_retained_state_bytes);
        try self.work_budget.retainStateBytes(added);
        errdefer self.work_budget.releaseStateBytes(added);

        const arena_alloc = self.arena.allocator();
        const owned_key = try arena_alloc.dupe(u8, key);
        const owned_table = if (table) |value| try arena_alloc.dupe(u8, value) else null;
        const ancestry = try arena_alloc.create(PathAncestry);
        ancestry.* = .{
            .node = .{ .key = owned_key, .table = owned_table },
            .parent = parent,
        };
        self.retained_bytes += added;
        return ancestry;
    }
};

const Frontier = struct {
    key: []const u8,
    table: ?[]const u8 = null,
    path: []paths_mod.PathEdge,
    ancestry: *const PathAncestry,
    hops: u32,

    fn deinit(self: *Frontier, alloc: Allocator) void {
        freePathEdges(alloc, self.path);
        self.* = undefined;
    }
};

const ReachableObserver = struct {
    ctx: *anyopaque,
    full_fn: *const fn (*anyopaque) bool,
    observe_fn: *const fn (*anyopaque, node_identity.Ref, u32, []const paths_mod.PathEdge) anyerror!void,

    fn full(self: ReachableObserver) bool {
        return self.full_fn(self.ctx);
    }

    fn observe(
        self: ReachableObserver,
        node: node_identity.Ref,
        depth: u32,
        path: []const paths_mod.PathEdge,
    ) !void {
        return self.observe_fn(self.ctx, node, depth, path);
    }
};

const ReachableNode = struct {
    key: []u8,
    table: ?[]u8 = null,
    depth: u32,
    path: []paths_mod.PathEdge,

    fn deinit(self: *ReachableNode, alloc: Allocator) void {
        alloc.free(self.key);
        if (self.table) |table| alloc.free(table);
        freePathEdges(alloc, self.path);
        self.* = undefined;
    }
};

const ReachableCollector = struct {
    alloc: Allocator,
    results: *std.ArrayListUnmanaged(ReachableNode),
    limit: usize,

    fn observer(self: *ReachableCollector) ReachableObserver {
        return .{
            .ctx = self,
            .full_fn = isFull,
            .observe_fn = observe,
        };
    }

    fn isFull(ctx: *anyopaque) bool {
        const self: *ReachableCollector = @ptrCast(@alignCast(ctx));
        return self.results.items.len >= self.limit;
    }

    fn observe(
        ctx: *anyopaque,
        node: node_identity.Ref,
        depth: u32,
        path: []const paths_mod.PathEdge,
    ) !void {
        const self: *ReachableCollector = @ptrCast(@alignCast(ctx));
        if (self.results.items.len >= self.limit) return;
        var reached = try initReachableNode(self.alloc, node.key, node.table, depth, path);
        errdefer reached.deinit(self.alloc);
        try self.results.append(self.alloc, reached);
    }
};

pub fn matchPattern(
    alloc: Allocator,
    graph_index: *graph_mod.GraphIndex,
    start_keys: []const []const u8,
    pattern: []const PatternStep,
    opts: MatchOptions,
) ![]PatternMatch {
    const GraphIndexEdgeReader = struct {
        graph_index: *graph_mod.GraphIndex,

        pub fn getEdges(
            self: @This(),
            a: Allocator,
            table: ?[]const u8,
            key: []const u8,
            edge_types: []const []const u8,
            direction: graph_mod.EdgeDirection,
        ) ![]graph_mod.Edge {
            if (table != null) return try a.alloc(graph_mod.Edge, 0);
            return try self.graph_index.getEdgesByTypes(a, key, edge_types, direction);
        }

        pub fn getEdgesBounded(
            self: @This(),
            a: Allocator,
            table: ?[]const u8,
            key: []const u8,
            edge_types: []const []const u8,
            direction: graph_mod.EdgeDirection,
            max_edges: usize,
            max_bytes: usize,
        ) ![]graph_mod.Edge {
            if (table != null) return try a.alloc(graph_mod.Edge, 0);
            return try self.graph_index.getEdgesByTypesBounded(a, key, edge_types, direction, max_edges, max_bytes);
        }

        pub fn freeEdges(_: @This(), a: Allocator, edges: []graph_mod.Edge) void {
            graph_mod.GraphIndex.freeEdges(a, edges);
        }

        pub fn probeEdgesBounded(
            self: @This(),
            a: Allocator,
            table: ?[]const u8,
            probes: []const graph_mod.EdgeProbe,
            max_owned_bytes: usize,
        ) ![]?graph_mod.Edge {
            if (table != null) {
                const empty = try a.alloc(?graph_mod.Edge, probes.len);
                @memset(empty, null);
                return empty;
            }
            return try self.graph_index.probeEdgesAllocBounded(a, probes, max_owned_bytes);
        }

        pub fn freeProbedEdges(_: @This(), a: Allocator, edges: []?graph_mod.Edge) void {
            graph_mod.GraphIndex.freeProbedEdges(a, edges);
        }
    };

    return try matchPatternWithEdgeReader(alloc, GraphIndexEdgeReader{ .graph_index = graph_index }, start_keys, pattern, opts);
}

pub fn matchPatternWithEdgeReader(
    alloc: Allocator,
    edge_reader: anytype,
    start_keys: []const []const u8,
    pattern: []const PatternStep,
    opts: MatchOptions,
) ![]PatternMatch {
    const start_nodes = try alloc.alloc(node_identity.Ref, start_keys.len);
    defer alloc.free(start_nodes);
    for (start_keys, 0..) |key, i| {
        start_nodes[i] = .{ .table = null, .key = key };
    }
    return try matchPatternFromRefsWithEdgeReader(
        alloc,
        edge_reader,
        start_nodes,
        pattern,
        opts,
    );
}

pub fn matchPatternFromRefsWithEdgeReader(
    alloc: Allocator,
    edge_reader: anytype,
    start_nodes: []const node_identity.Ref,
    pattern: []const PatternStep,
    opts: MatchOptions,
) ![]PatternMatch {
    if (pattern.len == 0 or pattern.len > max_pattern_steps) return error.InvalidArgument;
    if (opts.max_explored_nodes == 0 or
        opts.max_explored_edges == 0 or
        opts.max_intermediate_states == 0)
    {
        return error.InvalidArgument;
    }
    for (pattern[1..]) |step| {
        const min_hops = if (step.edge.min_hops == 0) @as(u32, 1) else step.edge.min_hops;
        const max_hops = if (step.edge.max_hops == 0) @as(u32, 1) else step.edge.max_hops;
        if (min_hops > max_hops or max_hops > max_pattern_hops)
            return error.InvalidArgument;
    }
    if (opts.stats) |stats| stats.* = .{};
    if (opts.target_required and opts.target_nodes.len == 0)
        return try alloc.alloc(PatternMatch, 0);
    var local_work_budget = WorkBudget.init(opts.max_explored_nodes, opts.max_explored_edges);
    const work_budget: *WorkBudget = opts.work_budget orelse &local_work_budget;
    const intermediate_limit = if (opts.max_results == 0)
        opts.max_intermediate_states
    else
        @min(
            opts.max_intermediate_states,
            @max(
                @as(usize, opts.max_results),
                std.math.mul(usize, @as(usize, opts.max_results), 10) catch
                    std.math.maxInt(usize),
            ),
        );

    // Fast-plan discovery is speculative. A reader may reveal a cross-table
    // edge that makes the plan inapplicable; never charge that abandoned work
    // to the generic fallback's public query budget.
    var exact_budget = work_budget.*;
    if (try matchExactTwoEdgePattern(
        alloc,
        edge_reader,
        start_nodes,
        pattern,
        opts,
        intermediate_limit,
        &exact_budget,
    )) |matches| {
        work_budget.* = exact_budget;
        if (opts.stats) |stats| stats.plan = .exact_two_edge_probe;
        return matches;
    }

    var current = std.ArrayListUnmanaged(MatchState).empty;
    defer {
        for (current.items) |*match| match.deinit(alloc);
        current.deinit(alloc);
    }

    var first_alias_buf: [32]u8 = undefined;
    const first_alias = effectiveAlias(pattern[0].alias, 0, &first_alias_buf);
    const start_direction = if (pattern.len > 1)
        pattern[1].edge.direction
    else
        graph_mod.EdgeDirection.out;
    const admitted_starts = if (opts.node_admission) |admission| blk: {
        const refs = try alloc.alloc(NodeRef, start_nodes.len);
        defer alloc.free(refs);
        for (start_nodes, 0..) |start, i| {
            refs[i] = .{
                .key = start.key,
                .table = start.table,
                .external = start.table != null or
                    (admission.external_targets and start_direction == .in),
            };
        }
        const mask = try admission.filterAlloc(alloc, refs);
        errdefer alloc.free(mask);
        if (start_direction == .both or
            (start_direction == .in and !admission.external_targets))
        {
            for (start_nodes, mask) |start, *allowed| {
                if (allowed.* or start.table != null) continue;
                allowed.* = try startNodeAdmitted(
                    alloc,
                    edge_reader,
                    start.key,
                    start_direction,
                    admission,
                    work_budget,
                );
            }
        }
        break :blk mask;
    } else null;
    defer if (admitted_starts) |mask| alloc.free(mask);
    for (start_nodes, 0..) |start, start_index| {
        if (admitted_starts) |mask| if (!mask[start_index]) continue;
        if (!(try passesNodeFilter(start, pattern[0].node_filter, opts.evaluator))) continue;

        const bindings = try alloc.alloc(PatternBinding, 1);
        bindings[0] = initPatternBinding(alloc, first_alias, start.key, start.table, 0) catch |err| {
            alloc.free(bindings);
            return err;
        };
        current.append(alloc, .{
            .bindings = bindings,
            .path = &.{},
        }) catch |err| {
            bindings[0].deinit(alloc);
            alloc.free(bindings);
            return err;
        };
        try work_budget.checkIntermediateStates(current.items.len, intermediate_limit);
    }

    for (1..pattern.len) |step_idx| {
        var next = std.ArrayListUnmanaged(MatchState).empty;
        errdefer {
            for (next.items) |*match| match.deinit(alloc);
            next.deinit(alloc);
        }

        const step = pattern[step_idx];
        var step_alias_buf: [32]u8 = undefined;
        const step_alias = effectiveAlias(step.alias, step_idx, &step_alias_buf);
        var prev_alias_buf: [32]u8 = undefined;
        const prev_alias = effectiveAlias(pattern[step_idx - 1].alias, step_idx - 1, &prev_alias_buf);

        for (current.items) |*match| {
            const current_binding = findBinding(match.bindings, prev_alias) orelse continue;
            const cycle_binding = findBinding(match.bindings, step_alias);
            const reachable = try findReachableNodes(
                alloc,
                edge_reader,
                current_binding.key,
                current_binding.table,
                .{ .step = step.edge },
                step.node_filter,
                if (step_idx + 1 == pattern.len) opts.target_nodes else &.{},
                step_idx + 1 == pattern.len and opts.target_required,
                opts.max_results,
                opts.require_complete,
                opts.max_intermediate_states,
                opts.evaluator,
                opts.node_admission,
                opts.include_paths,
                opts.stats,
                work_budget,
            );
            defer {
                for (reachable) |*node| node.deinit(alloc);
                if (reachable.len > 0) alloc.free(reachable);
            }

            for (reachable) |reached| {
                if (cycle_binding) |existing| {
                    if (!std.mem.eql(u8, reached.key, existing.key) or
                        !optionalTableEql(reached.table, existing.table)) continue;
                }

                var new_bindings = try cloneBindings(alloc, match.bindings);
                var state_owned = true;
                errdefer if (state_owned) {
                    for (new_bindings) |*binding| binding.deinit(alloc);
                    if (new_bindings.len > 0) alloc.free(new_bindings);
                };

                if (cycle_binding == null) {
                    var appended_binding = try initPatternBinding(
                        alloc,
                        step_alias,
                        reached.key,
                        reached.table,
                        reached.depth,
                    );
                    errdefer appended_binding.deinit(alloc);
                    const expanded = try alloc.alloc(PatternBinding, new_bindings.len + 1);
                    for (new_bindings, 0..) |binding, i| expanded[i] = binding;
                    expanded[expanded.len - 1] = appended_binding;
                    if (new_bindings.len > 0) alloc.free(new_bindings);
                    new_bindings = expanded;
                }

                const new_path = if (opts.include_paths)
                    try concatPathEdges(alloc, match.path, reached.path)
                else
                    @constCast((&[_]paths_mod.PathEdge{})[0..]);
                errdefer if (state_owned) freePathEdges(alloc, new_path);
                try next.append(alloc, .{
                    .bindings = new_bindings,
                    .path = new_path,
                });
                state_owned = false;

                try work_budget.checkIntermediateStates(next.items.len, intermediate_limit);
            }
        }

        for (current.items) |*match| match.deinit(alloc);
        current.deinit(alloc);
        current = next;

        if (current.items.len == 0) break;
    }

    const limited_len: usize = if (opts.max_results == 0) current.items.len else @min(current.items.len, opts.max_results);
    var results = try alloc.alloc(PatternMatch, limited_len);
    var initialized: usize = 0;
    errdefer {
        for (results[0..initialized]) |*match| match.deinit(alloc);
        if (results.len > 0) alloc.free(results);
    }

    for (current.items[0..limited_len], 0..) |*match, i| {
        if (opts.return_aliases.len == 0) {
            results[i] = .{
                .bindings = match.bindings,
                .path = match.path,
            };
            match.bindings = @constCast((&[_]PatternBinding{})[0..]);
            match.path = @constCast((&[_]paths_mod.PathEdge{})[0..]);
            initialized += 1;
            continue;
        }
        const filtered_bindings = try filterBindings(alloc, match.bindings, opts.return_aliases);
        errdefer {
            for (filtered_bindings) |*binding| binding.deinit(alloc);
            if (filtered_bindings.len > 0) alloc.free(filtered_bindings);
        }
        results[i] = .{
            .bindings = filtered_bindings,
            .path = if (opts.include_paths)
                try clonePathEdges(alloc, match.path)
            else
                @constCast((&[_]paths_mod.PathEdge{})[0..]),
        };
        initialized += 1;
    }

    std.log.debug(
        "antfly_graph_pattern_plan plan=generic_expand matches={d}",
        .{results.len},
    );
    return results;
}

/// For a fixed two-edge pattern with one exact start and endpoint, scan the
/// start adjacency once and batch-probe the exact second relationship for each
/// admitted middle node. The physical graph key already contains
/// (source,index,type,target), so this plan is independent of global target
/// degree and does not materialize the target's complete reverse adjacency.
fn matchExactTwoEdgePattern(
    alloc: Allocator,
    edge_reader: anytype,
    start_nodes: []const node_identity.Ref,
    pattern: []const PatternStep,
    opts: MatchOptions,
    intermediate_limit: usize,
    work_budget: *WorkBudget,
) !?[]PatternMatch {
    if (comptime !@hasDecl(@TypeOf(edge_reader), "probeEdgesBounded") or
        !@hasDecl(@TypeOf(edge_reader), "freeProbedEdges"))
    {
        return null;
    }
    if (pattern.len != 3 or start_nodes.len != 1 or !opts.target_required or opts.target_nodes.len != 1 or
        start_nodes[0].table != null or opts.target_nodes[0].table != null or
        opts.node_admission != null)
    {
        return null;
    }
    for (pattern[1..]) |step| {
        if (step.edge.types.len != 1 or step.edge.direction == .both) return null;
        const min_hops = if (step.edge.min_hops == 0) @as(u32, 1) else step.edge.min_hops;
        const max_hops = if (step.edge.max_hops == 0) @as(u32, 1) else step.edge.max_hops;
        if (min_hops != 1 or max_hops != 1) return null;
    }

    var alias_bufs: [3][32]u8 = undefined;
    const aliases = [3][]const u8{
        effectiveAlias(pattern[0].alias, 0, &alias_bufs[0]),
        effectiveAlias(pattern[1].alias, 1, &alias_bufs[1]),
        effectiveAlias(pattern[2].alias, 2, &alias_bufs[2]),
    };
    if (std.mem.eql(u8, aliases[0], aliases[1]) or
        std.mem.eql(u8, aliases[0], aliases[2]) or
        std.mem.eql(u8, aliases[1], aliases[2])) return null;

    const start_key = start_nodes[0].key;
    const target_key = opts.target_nodes[0].key;
    if (!(try passesNodeFilter(start_nodes[0], pattern[0].node_filter, opts.evaluator)) or
        !(try passesNodeFilter(opts.target_nodes[0], pattern[2].node_filter, opts.evaluator)))
    {
        return try alloc.alloc(PatternMatch, 0);
    }

    const forward_edges = try getEdgesForBudget(
        alloc,
        edge_reader,
        null,
        start_key,
        pattern[1].edge.types,
        pattern[1].edge.direction,
        work_budget,
        false,
    );
    defer edge_reader.freeEdges(alloc, forward_edges);

    const Candidate = struct {
        middle_key: []const u8,
        forward_edge_index: usize,
    };
    const expansion_limit: usize = if (opts.max_results == 0)
        1000
    else
        @min(
            std.math.mul(usize, @as(usize, opts.max_results), 10) catch std.math.maxInt(usize),
            1000,
        );
    var candidates = std.ArrayListUnmanaged(Candidate).empty;
    defer candidates.deinit(alloc);
    for (forward_edges, 0..) |graph_edge, edge_index| {
        if (!edgeMatches(graph_edge, pattern[1].edge)) continue;
        if (traversal_mod.metadataTargetTable(graph_edge.metadata) != null) return null;
        const middle_key = edgeTarget(graph_edge, start_key, pattern[1].edge.direction) orelse continue;
        if (edgeTargetTable(null, graph_edge, middle_key) != null) return null;
        if (!(try passesNodeFilter(.{ .table = null, .key = middle_key }, pattern[1].node_filter, opts.evaluator))) continue;
        try candidates.append(alloc, .{
            .middle_key = middle_key,
            .forward_edge_index = edge_index,
        });
        try work_budget.checkIntermediateStates(candidates.items.len, intermediate_limit);
        // Keep the same deterministic first-hop window as generic expansion.
        // The optimizer must not make nodes beyond that window newly visible.
        if (candidates.items.len >= expansion_limit) break;
    }

    // Match generic-expansion accounting: one expanded start node, then one
    // expanded middle node and one exact relationship probe per candidate.
    try work_budget.consumeNode();
    try consumeMaterializedEdges(work_budget, forward_edges);
    for (candidates.items) |_| try work_budget.consumeNode();
    try work_budget.consumeEdges(candidates.items.len);

    var matches = std.ArrayListUnmanaged(PatternMatch).empty;
    errdefer {
        for (matches.items) |*match| match.deinit(alloc);
        matches.deinit(alloc);
    }
    const result_limit: usize = if (opts.max_results == 0) std.math.maxInt(usize) else opts.max_results;
    var exact_edge_matches: usize = 0;
    const probe_batch_size: usize = 256;
    var candidate_offset: usize = 0;
    while (candidate_offset < candidates.items.len) {
        const batch_end = @min(candidate_offset +| probe_batch_size, candidates.items.len);
        const batch_candidates = candidates.items[candidate_offset..batch_end];
        const probes = try alloc.alloc(graph_mod.EdgeProbe, batch_candidates.len);
        defer alloc.free(probes);
        for (batch_candidates, 0..) |candidate, i| {
            probes[i] = switch (pattern[2].edge.direction) {
                .out => .{
                    .source = candidate.middle_key,
                    .target = target_key,
                    .edge_type = pattern[2].edge.types[0],
                },
                .in => .{
                    .source = target_key,
                    .target = candidate.middle_key,
                    .edge_type = pattern[2].edge.types[0],
                },
                .both => unreachable,
            };
        }
        const probed_edges = edge_reader.probeEdgesBounded(
            alloc,
            null,
            probes,
            work_budget.edgeByteLimit(),
        ) catch |err| {
            const widened: anyerror = err;
            if (widened == error.GraphExploredEdgeBytesBudgetExceeded or
                widened == error.QueryCandidateBudgetExceeded)
                return work_budget.exhaust(.explored_edge_bytes, work_budget.max_edge_bytes);
            return err;
        };
        defer edge_reader.freeProbedEdges(alloc, probed_edges);
        if (probed_edges.len != probes.len) return error.InvalidGraphEdgeProbeResult;
        const probed_bytes = probedEdgeOwnedBytes(probed_edges) catch
            return work_budget.exhaust(.explored_edge_bytes, work_budget.max_edge_bytes);
        try work_budget.consumeEdgeBytes(probed_bytes);

        for (batch_candidates, probed_edges) |candidate, maybe_backward_edge| {
            const backward_edge = maybe_backward_edge orelse continue;
            // A physical edge whose metadata changes node-table identity cannot
            // use this table-local plan. Release already-built results before
            // the successful null return hands control to generic expansion.
            if (traversal_mod.metadataTargetTable(backward_edge.metadata) != null) {
                for (matches.items) |*match| match.deinit(alloc);
                matches.deinit(alloc);
                matches = .empty;
                return null;
            }
            if (!edgeMatches(backward_edge, pattern[2].edge)) continue;
            exact_edge_matches += 1;
            if (matches.items.len >= result_limit) continue;
            const forward_edge = forward_edges[candidate.forward_edge_index];
            const middle_key = candidate.middle_key;

            var all_bindings = try alloc.alloc(PatternBinding, 3);
            var initialized_bindings: usize = 0;
            errdefer {
                for (all_bindings[0..initialized_bindings]) |*binding| binding.deinit(alloc);
                if (all_bindings.len > 0) alloc.free(all_bindings);
            }
            all_bindings[0] = try initPatternBinding(alloc, aliases[0], start_key, null, 0);
            initialized_bindings += 1;
            all_bindings[1] = try initPatternBinding(alloc, aliases[1], middle_key, null, 1);
            initialized_bindings += 1;
            all_bindings[2] = try initPatternBinding(alloc, aliases[2], target_key, null, 1);
            initialized_bindings += 1;
            const filtered_bindings = if (opts.return_aliases.len == 0) blk: {
                const owned = all_bindings;
                all_bindings = @constCast((&[_]PatternBinding{})[0..]);
                initialized_bindings = 0;
                break :blk owned;
            } else blk: {
                const filtered = try filterBindings(alloc, all_bindings, opts.return_aliases);
                for (all_bindings) |*binding| binding.deinit(alloc);
                alloc.free(all_bindings);
                all_bindings = @constCast((&[_]PatternBinding{})[0..]);
                initialized_bindings = 0;
                break :blk filtered;
            };
            errdefer {
                for (filtered_bindings) |*binding| binding.deinit(alloc);
                if (filtered_bindings.len > 0) alloc.free(filtered_bindings);
            }

            const path = if (opts.include_paths) blk: {
                const first = try appendPathEdge(alloc, &.{}, forward_edge, start_key, middle_key, pattern[1].edge.direction);
                defer freePathEdges(alloc, first);
                break :blk try appendPathEdge(alloc, first, backward_edge, middle_key, target_key, pattern[2].edge.direction);
            } else @constCast((&[_]paths_mod.PathEdge{})[0..]);
            errdefer freePathEdges(alloc, path);
            try matches.append(alloc, .{ .bindings = filtered_bindings, .path = path });
        }
        candidate_offset = batch_end;
    }
    if (opts.stats) |stats| {
        stats.adjacency_reads += 1;
        stats.adjacency_edges += forward_edges.len;
        stats.exact_edge_probes += candidates.items.len;
        stats.exact_edge_matches += exact_edge_matches;
    }
    std.log.debug(
        "antfly_graph_pattern_plan plan=exact_two_edge_probe adjacency_edges={d} probes={d} matches={d}",
        .{ forward_edges.len, candidates.items.len, matches.items.len },
    );
    return try matches.toOwnedSlice(alloc);
}

fn findReachableNodes(
    alloc: Allocator,
    edge_reader: anytype,
    start_key: []const u8,
    start_table: ?[]const u8,
    traversal: PhysicalEdgeTraversal,
    node_filter: NodeFilter,
    target_nodes: []const node_identity.Ref,
    target_required: bool,
    max_results: u32,
    require_complete: bool,
    max_intermediate_states: usize,
    evaluator: ?FilterEvaluator,
    node_admission: ?NodeAdmission,
    include_paths: bool,
    stats: ?*MatchStats,
    work_budget: *WorkBudget,
) ![]ReachableNode {
    const result_limit: usize = if (require_complete)
        std.math.maxInt(usize)
    else if (max_results == 0)
        1000
    else
        @min(
            std.math.mul(usize, @as(usize, max_results), 10) catch std.math.maxInt(usize),
            1000,
        );
    var results = std.ArrayListUnmanaged(ReachableNode).empty;
    errdefer {
        for (results.items) |*node| node.deinit(alloc);
        results.deinit(alloc);
    }
    var collector = ReachableCollector{
        .alloc = alloc,
        .results = &results,
        .limit = result_limit,
    };
    try streamReachableNodes(
        alloc,
        edge_reader,
        start_key,
        start_table,
        traversal,
        node_filter,
        target_nodes,
        target_required,
        max_intermediate_states,
        evaluator,
        node_admission,
        include_paths,
        stats,
        work_budget,
        collector.observer(),
    );
    return try results.toOwnedSlice(alloc);
}

fn streamReachableNodes(
    alloc: Allocator,
    edge_reader: anytype,
    start_key: []const u8,
    start_table: ?[]const u8,
    traversal: PhysicalEdgeTraversal,
    node_filter: NodeFilter,
    target_nodes: []const node_identity.Ref,
    target_required: bool,
    max_intermediate_states: usize,
    evaluator: ?FilterEvaluator,
    node_admission: ?NodeAdmission,
    include_paths: bool,
    stats: ?*MatchStats,
    work_budget: *WorkBudget,
    observer: ReachableObserver,
) !void {
    const edge = traversal.step;
    const min_hops = if (edge.min_hops == 0) @as(u32, 1) else edge.min_hops;
    const max_hops = if (edge.max_hops == 0) @as(u32, 1) else edge.max_hops;
    // A fixed relationship is not a variable-length path: a physical
    // self-loop may bind two distinct aliases to the same node. Multi-hop
    // expansion remains node-simple, apart from an explicit already-bound
    // cycle target.
    const fixed_single_hop = min_hops == 1 and max_hops == 1;
    // The endpoint declaration determines only the first incoming source
    // table. Unnamed intermediate sources may live anywhere in the catalog, so
    // a cross-table reverse variable path cannot be proven from this plan.
    if (traversal.incoming_source != null and max_hops > 1)
        return error.GraphReverseVariablePathUnsupported;
    var ancestry = ReachabilityAncestry.init(alloc, work_budget);
    defer ancestry.deinit();
    var current = std.ArrayListUnmanaged(Frontier).empty;
    defer {
        for (current.items) |*frontier| frontier.deinit(alloc);
        current.deinit(alloc);
    }
    {
        var initial = try initFrontier(&ancestry, start_key, start_table, &.{}, null, 0);
        errdefer initial.deinit(alloc);
        try current.append(alloc, initial);
    }
    while (current.items.len > 0 and !observer.full()) {
        var next = std.ArrayListUnmanaged(Frontier).empty;
        errdefer {
            for (next.items) |*frontier| frontier.deinit(alloc);
            next.deinit(alloc);
        }

        for (current.items) |*frontier| {
            try work_budget.consumeNode();
            if (frontier.hops >= max_hops) continue;

            const edges = try getEdgesForBudget(
                alloc,
                edge_reader,
                if (traversal.incoming_source) |source| source.table else frontier.table,
                frontier.key,
                edge.types,
                edge.direction,
                work_budget,
                traversal.incoming_source != null,
            );
            defer edge_reader.freeEdges(alloc, edges);
            if (stats) |active| {
                active.adjacency_reads += 1;
                active.adjacency_edges += edges.len;
            }
            try consumeMaterializedEdges(work_budget, edges);

            const admitted_edges = if (node_admission) |admission| blk: {
                const edge_mask = try alloc.alloc(bool, edges.len);
                @memset(edge_mask, false);
                errdefer alloc.free(edge_mask);
                var candidate_indexes = std.ArrayListUnmanaged(usize).empty;
                defer candidate_indexes.deinit(alloc);
                var candidate_nodes = std.ArrayListUnmanaged(NodeRef).empty;
                defer candidate_nodes.deinit(alloc);
                try candidate_indexes.ensureTotalCapacity(alloc, edges.len);
                try candidate_nodes.ensureTotalCapacity(alloc, edges.len);
                for (edges, 0..) |graph_edge, edge_index| {
                    if (!edgeMatches(graph_edge, edge)) continue;
                    const target_key = edgeTarget(graph_edge, frontier.key, edge.direction) orelse continue;
                    const target_table = resolvedEdgeTargetTable(
                        edge_reader,
                        frontier.table,
                        graph_edge,
                        target_key,
                        traversal,
                    );
                    if (shouldRejectPathRevisit(
                        frontierContainsNode(frontier.*, .{ .table = target_table, .key = target_key }),
                        .{ .table = target_table, .key = target_key },
                        target_nodes,
                        target_required,
                        fixed_single_hop,
                    )) continue;
                    candidate_indexes.appendAssumeCapacity(edge_index);
                    candidate_nodes.appendAssumeCapacity(.{
                        .key = target_key,
                        .table = target_table,
                        .external = std.mem.eql(u8, target_key, graph_edge.target) and
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

            const filtered_edges = if (nodeFilterActive(node_filter)) blk: {
                const edge_mask = try alloc.alloc(bool, edges.len);
                @memset(edge_mask, false);
                errdefer alloc.free(edge_mask);
                var candidate_indexes = std.ArrayListUnmanaged(usize).empty;
                defer candidate_indexes.deinit(alloc);
                var candidate_nodes = std.ArrayListUnmanaged(node_identity.Ref).empty;
                defer candidate_nodes.deinit(alloc);
                for (edges, 0..) |graph_edge, edge_index| {
                    if (admitted_edges) |mask| {
                        if (!mask[edge_index]) continue;
                    } else if (!edgeMatches(graph_edge, edge)) continue;
                    const target_key = edgeTarget(graph_edge, frontier.key, edge.direction) orelse continue;
                    const target_table = resolvedEdgeTargetTable(
                        edge_reader,
                        frontier.table,
                        graph_edge,
                        target_key,
                        traversal,
                    );
                    if (shouldRejectPathRevisit(
                        frontierContainsNode(frontier.*, .{ .table = target_table, .key = target_key }),
                        .{ .table = target_table, .key = target_key },
                        target_nodes,
                        target_required,
                        fixed_single_hop,
                    )) continue;
                    const new_hops = frontier.hops + 1;
                    if (new_hops < min_hops or
                        !targetNodeMatches(.{ .table = target_table, .key = target_key }, target_nodes, target_required)) continue;
                    try candidate_indexes.append(alloc, edge_index);
                    try candidate_nodes.append(alloc, .{ .table = target_table, .key = target_key });
                }
                const decisions = try evaluateNodeFiltersAlloc(alloc, candidate_nodes.items, node_filter, evaluator);
                defer alloc.free(decisions);
                for (candidate_indexes.items, decisions) |edge_index, allowed| edge_mask[edge_index] = allowed;
                break :blk edge_mask;
            } else null;
            defer if (filtered_edges) |mask| alloc.free(mask);

            for (edges, 0..) |graph_edge, edge_index| {
                const target_key = edgeTarget(graph_edge, frontier.key, edge.direction) orelse continue;
                const target_table = resolvedEdgeTargetTable(
                    edge_reader,
                    frontier.table,
                    graph_edge,
                    target_key,
                    traversal,
                );
                const revisits_path = frontierContainsNode(
                    frontier.*,
                    .{ .table = target_table, .key = target_key },
                );
                if (admitted_edges) |mask| {
                    if (!mask[edge_index]) continue;
                } else {
                    if (!edgeMatches(graph_edge, edge)) continue;
                    if (shouldRejectPathRevisit(
                        revisits_path,
                        .{ .table = target_table, .key = target_key },
                        target_nodes,
                        target_required,
                        fixed_single_hop,
                    )) continue;
                }
                const new_hops = frontier.hops + 1;
                const new_path = if (include_paths)
                    try appendPathEdge(alloc, frontier.path, graph_edge, frontier.key, target_key, edge.direction)
                else
                    @constCast((&[_]paths_mod.PathEdge{})[0..]);
                var new_path_owned = true;
                errdefer if (new_path_owned) freePathEdges(alloc, new_path);

                if (new_hops >= min_hops and
                    targetNodeMatches(.{ .table = target_table, .key = target_key }, target_nodes, target_required) and
                    (if (filtered_edges) |mask| mask[edge_index] else true))
                {
                    try observer.observe(
                        .{ .key = target_key, .table = target_table },
                        new_hops,
                        new_path,
                    );
                    if (observer.full()) {
                        freePathEdges(alloc, new_path);
                        new_path_owned = false;
                        break;
                    }
                }

                // A repeated node is permitted only to materialize an explicit
                // cycle-closing target. Do not expand it into non-simple paths.
                if (new_hops < max_hops and !revisits_path) {
                    var next_item = try initFrontier(
                        &ancestry,
                        target_key,
                        target_table,
                        new_path,
                        frontier.ancestry,
                        new_hops,
                    );
                    new_path_owned = false;
                    errdefer next_item.deinit(alloc);
                    try work_budget.checkIntermediateStates(next.items.len + 1, max_intermediate_states);
                    try next.append(alloc, next_item);
                } else {
                    freePathEdges(alloc, new_path);
                    new_path_owned = false;
                }
            }
            if (observer.full()) break;
        }

        for (current.items) |*frontier| frontier.deinit(alloc);
        current.deinit(alloc);
        current = next;
    }

    return;
}

fn startNodeAdmitted(
    alloc: Allocator,
    edge_reader: anytype,
    start_key: []const u8,
    direction: graph_mod.EdgeDirection,
    admission: NodeAdmission,
    work_budget: *WorkBudget,
) !bool {
    const keys = [_][]const u8{start_key};
    const admitted = try admission.filterLocalKeysAlloc(alloc, &keys);
    defer alloc.free(admitted);
    if (admitted[0] or direction == .out) return admitted[0];

    const incoming = try getEdgesForBudget(alloc, edge_reader, null, start_key, &.{}, .in, work_budget, false);
    defer edge_reader.freeEdges(alloc, incoming);
    try consumeMaterializedEdges(work_budget, incoming);
    for (incoming) |graph_edge| {
        if (std.mem.eql(u8, graph_edge.target, start_key) and
            (admission.external_targets or
                traversal_mod.metadataTargetTable(graph_edge.metadata) != null))
        {
            return true;
        }
    }
    return false;
}

fn targetNodeMatches(node: node_identity.Ref, targets: []const node_identity.Ref, target_required: bool) bool {
    if (targets.len == 0) return !target_required;
    for (targets) |target| {
        if (optionalTableEql(node.table, target.table) and std.mem.eql(u8, node.key, target.key)) return true;
    }
    return false;
}

fn edgeMatches(edge: graph_mod.Edge, step: PatternEdgeStep) bool {
    if (step.types.len > 0) {
        var matched = false;
        for (step.types) |edge_type| {
            if (std.mem.eql(u8, edge.edge_type, edge_type)) {
                matched = true;
                break;
            }
        }
        if (!matched) return false;
    }
    if (step.min_weight) |min_weight| if (edge.weight < min_weight) return false;
    if (step.max_weight) |max_weight| if (edge.weight > max_weight) return false;
    return true;
}

test "pattern edge filters preserve explicit zero bounds" {
    const zero = graph_mod.Edge{ .source = "a", .target = "b", .edge_type = "e", .weight = 0, .created_at = 0, .updated_at = 0, .metadata = "" };
    const positive = graph_mod.Edge{ .source = "a", .target = "b", .edge_type = "e", .weight = 0.1, .created_at = 0, .updated_at = 0, .metadata = "" };
    const negative = graph_mod.Edge{ .source = "a", .target = "b", .edge_type = "e", .weight = -0.1, .created_at = 0, .updated_at = 0, .metadata = "" };
    try std.testing.expect(edgeMatches(zero, .{ .max_weight = 0 }));
    try std.testing.expect(!edgeMatches(positive, .{ .max_weight = 0 }));
    try std.testing.expect(edgeMatches(negative, .{ .max_weight = 0 }));
    try std.testing.expect(!edgeMatches(negative, .{ .min_weight = 0 }));
}

fn edgeTarget(edge: graph_mod.Edge, current_key: []const u8, direction: graph_mod.EdgeDirection) ?[]const u8 {
    return switch (direction) {
        .out => if (std.mem.eql(u8, edge.source, current_key)) edge.target else null,
        .in => if (std.mem.eql(u8, edge.target, current_key)) edge.source else null,
        .both => if (std.mem.eql(u8, edge.source, current_key))
            edge.target
        else if (std.mem.eql(u8, edge.target, current_key))
            edge.source
        else
            null,
    };
}

fn edgeTargetTable(
    current_table: ?[]const u8,
    edge: graph_mod.Edge,
    target_key: []const u8,
) ?[]const u8 {
    if (std.mem.eql(u8, target_key, edge.target)) {
        return traversal_mod.metadataTargetTable(edge.metadata) orelse current_table;
    }
    return current_table;
}

/// Edge metadata names tables in the storage namespace, while match bindings
/// may use a canonical query-relative namespace (for example, null for the
/// source table). Readers that cross table boundaries can normalize the
/// derived table before it enters path-cycle checks, alias predicates, or
/// distinct-identity sets. The compile-time capability check keeps local and
/// serverless readers allocation- and dispatch-free.
fn canonicalEdgeTargetTable(
    edge_reader: anytype,
    current_table: ?[]const u8,
    edge: graph_mod.Edge,
    target_key: []const u8,
) ?[]const u8 {
    const table = edgeTargetTable(current_table, edge, target_key);
    if (comptime @hasDecl(@TypeOf(edge_reader), "canonicalizeTable")) {
        return edge_reader.canonicalizeTable(table);
    }
    return table;
}

fn canonicalizeNodeTable(edge_reader: anytype, table: ?[]const u8) ?[]const u8 {
    if (comptime @hasDecl(@TypeOf(edge_reader), "canonicalizeTable")) {
        return edge_reader.canonicalizeTable(table);
    }
    return table;
}

fn resolvedEdgeTargetTable(
    edge_reader: anytype,
    current_table: ?[]const u8,
    graph_edge: graph_mod.Edge,
    target_key: []const u8,
    traversal: PhysicalEdgeTraversal,
) ?[]const u8 {
    if (traversal.incoming_source != null and
        traversal.step.direction == .in and
        std.mem.eql(u8, target_key, graph_edge.source))
    {
        return canonicalizeNodeTable(edge_reader, traversal.incoming_source.?.table);
    }
    return canonicalEdgeTargetTable(edge_reader, current_table, graph_edge, target_key);
}

fn declaredNodeTableMatches(edge_reader: anytype, actual: ?[]const u8, declared: ?[]const u8) bool {
    return optionalTableEql(actual, canonicalizeNodeTable(edge_reader, declared));
}

fn passesPrefixFilter(key: []const u8, filter: NodeFilter) bool {
    if (filter.filter_prefix.len == 0) return true;
    return std.mem.startsWith(u8, key, filter.filter_prefix);
}

fn passesNodeFilter(node: node_identity.Ref, filter: NodeFilter, evaluator: ?FilterEvaluator) !bool {
    if (!passesPrefixFilter(node.key, filter)) return false;
    if (filter.filter_query_json == null) return true;
    const active = evaluator orelse return error.UnsupportedNodeFilterQuery;
    const eval_fn = active.func orelse return error.UnsupportedNodeFilterQuery;
    return try eval_fn(active.ctx, node, filter);
}

fn evaluateNodeFiltersAlloc(
    alloc: Allocator,
    nodes: []const node_identity.Ref,
    filter: NodeFilter,
    evaluator: ?FilterEvaluator,
) ![]bool {
    const decisions = try alloc.alloc(bool, nodes.len);
    errdefer alloc.free(decisions);
    if (!nodeFilterActive(filter)) {
        @memset(decisions, true);
        return decisions;
    }
    const active = evaluator orelse return error.UnsupportedNodeFilterQuery;
    if (active.batch_func) |batch_func| {
        var query_nodes = std.ArrayListUnmanaged(node_identity.Ref).empty;
        defer query_nodes.deinit(alloc);
        var query_indexes = std.ArrayListUnmanaged(usize).empty;
        defer query_indexes.deinit(alloc);
        @memset(decisions, false);
        for (nodes, 0..) |node, i| {
            if (!passesPrefixFilter(node.key, filter)) continue;
            try query_nodes.append(alloc, node);
            try query_indexes.append(alloc, i);
        }
        if (query_nodes.items.len == 0) return decisions;
        if (filter.filter_query_json == null) {
            for (query_indexes.items) |i| decisions[i] = true;
            return decisions;
        }
        const evaluated = try batch_func(active.ctx, alloc, query_nodes.items, filter);
        defer alloc.free(evaluated);
        if (evaluated.len != query_nodes.items.len) return error.InvalidQueryResult;
        for (query_indexes.items, evaluated) |i, allowed| decisions[i] = allowed;
        return decisions;
    }
    for (nodes, 0..) |node, i| decisions[i] = try passesNodeFilter(node, filter, active);
    return decisions;
}

fn effectiveAlias(alias: []const u8, step_idx: usize, buf: []u8) []const u8 {
    if (alias.len > 0) return alias;
    return std.fmt.bufPrint(buf, "_step{}", .{step_idx}) catch "_step";
}

const ConjunctiveState = struct {
    bindings: []PatternBinding,
    null_aliases: [][]u8 = &.{},

    fn deinit(self: *ConjunctiveState, alloc: Allocator) void {
        for (self.bindings) |*binding| binding.deinit(alloc);
        if (self.bindings.len > 0) alloc.free(self.bindings);
        for (self.null_aliases) |alias| alloc.free(alias);
        if (self.null_aliases.len > 0) alloc.free(self.null_aliases);
        self.* = undefined;
    }
};

/// Match a connected conjunctive graph pattern. Unlike the original linear
/// matcher, every edge names both endpoints, so branches and closures do not
/// require synthetic walk-back steps. Optional groups are evaluated in order
/// and preserve one null-extended row when their correlated match is empty.
pub fn matchConjunctivePattern(
    alloc: Allocator,
    graph_index: *graph_mod.GraphIndex,
    start_keys: []const []const u8,
    pattern: ConjunctivePattern,
    opts: MatchOptions,
) ![]PatternMatch {
    const Reader = struct {
        graph_index: *graph_mod.GraphIndex,

        pub fn getEdges(self: @This(), a: Allocator, table: ?[]const u8, key: []const u8, types: []const []const u8, direction: graph_mod.EdgeDirection) ![]graph_mod.Edge {
            if (table != null) return try a.alloc(graph_mod.Edge, 0);
            return try self.graph_index.getEdgesByTypes(a, key, types, direction);
        }

        pub fn getEdgesBounded(self: @This(), a: Allocator, table: ?[]const u8, key: []const u8, types: []const []const u8, direction: graph_mod.EdgeDirection, max_edges: usize, max_bytes: usize) ![]graph_mod.Edge {
            if (table != null) return try a.alloc(graph_mod.Edge, 0);
            return try self.graph_index.getEdgesByTypesBounded(a, key, types, direction, max_edges, max_bytes);
        }

        pub fn freeEdges(_: @This(), a: Allocator, edges: []graph_mod.Edge) void {
            graph_mod.GraphIndex.freeEdges(a, edges);
        }
    };
    return try matchConjunctivePatternWithEdgeReader(alloc, Reader{ .graph_index = graph_index }, start_keys, pattern, opts);
}

pub fn matchConjunctivePatternWithEdgeReader(
    alloc: Allocator,
    edge_reader: anytype,
    start_keys: []const []const u8,
    pattern: ConjunctivePattern,
    opts: MatchOptions,
) ![]PatternMatch {
    // Bounded binding queries stream complete rows instead of materializing a
    // page-wide Cartesian expansion and slicing afterward. Predicates and all
    // ordered OPTIONAL groups are applied before a row consumes the limit, so
    // this is an exact prefix rather than an unsafe intermediate-state window.
    if (opts.max_results > 0 and !opts.require_complete) {
        return try matchConjunctivePatternLimitedWithEdgeReader(
            alloc,
            edge_reader,
            start_keys,
            pattern,
            opts,
        );
    }

    var local_output_work_budget = WorkBudget.init(opts.max_explored_nodes, opts.max_explored_edges);
    const output_work_budget = opts.work_budget orelse &local_output_work_budget;
    var current = try buildConjunctiveStates(alloc, edge_reader, start_keys, pattern, opts);
    defer freeConjunctiveStates(alloc, &current);

    const result_len = if (opts.max_results == 0) current.items.len else @min(current.items.len, opts.max_results);
    const results = try alloc.alloc(PatternMatch, result_len);
    var initialized: usize = 0;
    errdefer {
        for (results[0..initialized]) |*result| result.deinit(alloc);
        if (results.len > 0) alloc.free(results);
    }
    var projection = try AliasProjection.init(alloc, opts.return_aliases);
    defer projection.deinit(alloc);
    for (current.items[0..result_len], 0..) |state, i| {
        results[i] = try projectConjunctiveState(
            alloc,
            state,
            &projection,
            output_work_budget,
            opts.work_budget,
        );
        initialized += 1;
    }
    return results;
}

fn matchConjunctivePatternLimitedWithEdgeReader(
    alloc: Allocator,
    edge_reader: anytype,
    start_keys: []const []const u8,
    pattern: ConjunctivePattern,
    opts: MatchOptions,
) ![]PatternMatch {
    if (pattern.nodes.len == 0 or pattern.nodes.len > max_pattern_steps) return error.InvalidArgument;
    try validateConjunctivePattern(pattern);

    const anchor_node = selectConjunctiveAnchor(pattern) orelse return error.InvalidArgument;
    if (!declaredNodeTableMatches(edge_reader, null, anchor_node.table)) return error.InvalidArgument;
    var local_work_budget = WorkBudget.init(opts.max_explored_nodes, opts.max_explored_edges);
    const work_budget = opts.work_budget orelse &local_work_budget;
    if (!opts.anchors_precharged) try work_budget.consumeAnchors(start_keys.len);
    const admitted_starts = if (opts.start_validation == .required and opts.node_admission != null)
        try opts.node_admission.?.filterLocalKeysAlloc(alloc, start_keys)
    else
        null;
    defer if (admitted_starts) |mask| alloc.free(mask);
    const filtered_starts = if (opts.start_validation == .required) blk: {
        const start_refs = try alloc.alloc(node_identity.Ref, start_keys.len);
        defer alloc.free(start_refs);
        for (start_keys, 0..) |key, i| start_refs[i] = .{ .table = null, .key = key };
        break :blk try evaluateNodeFiltersAlloc(alloc, start_refs, anchor_node.filter, opts.evaluator);
    } else null;
    defer if (filtered_starts) |mask| alloc.free(mask);

    var sink = try ConjunctiveBindingSink.init(
        alloc,
        opts.return_aliases,
        opts.max_results,
        work_budget,
        opts.work_budget,
    );
    defer sink.deinit(alloc);
    const processed = try alloc.alloc(bool, pattern.edges.len);
    defer alloc.free(processed);

    for (start_keys, 0..) |key, start_index| {
        if (sink.full()) break;
        if (admitted_starts) |mask| if (!mask[start_index]) continue;
        if (filtered_starts) |mask| if (!mask[start_index]) continue;

        const bindings = try alloc.alloc(PatternBinding, 1);
        bindings[0] = initPatternBinding(alloc, anchor_node.alias, key, null, 0) catch |err| {
            alloc.free(bindings);
            return err;
        };
        var state = ConjunctiveState{ .bindings = bindings };
        defer state.deinit(alloc);
        @memset(processed, false);
        try streamConjunctiveGroup(
            alloc,
            edge_reader,
            pattern,
            pattern.nodes,
            pattern.edges,
            pattern.predicates,
            state,
            processed,
            0,
            null,
            null,
            opts,
            work_budget,
            &sink,
        );
    }
    return try sink.finish(alloc);
}

fn buildConjunctiveStates(
    alloc: Allocator,
    edge_reader: anytype,
    start_keys: []const []const u8,
    pattern: ConjunctivePattern,
    opts: MatchOptions,
) !std.ArrayListUnmanaged(ConjunctiveState) {
    if (pattern.nodes.len == 0 or pattern.nodes.len > max_pattern_steps) return error.InvalidArgument;
    try validateConjunctivePattern(pattern);

    const anchor_node = selectConjunctiveAnchor(pattern) orelse return error.InvalidArgument;
    if (!declaredNodeTableMatches(edge_reader, null, anchor_node.table)) return error.InvalidArgument;
    const anchor_alias = anchor_node.alias;
    var local_work_budget = WorkBudget.init(opts.max_explored_nodes, opts.max_explored_edges);
    const work_budget: *WorkBudget = opts.work_budget orelse &local_work_budget;
    if (!opts.anchors_precharged) try work_budget.consumeAnchors(start_keys.len);

    var current = std.ArrayListUnmanaged(ConjunctiveState).empty;
    errdefer freeConjunctiveStates(alloc, &current);
    const admitted_starts = if (opts.start_validation == .required and opts.node_admission != null)
        try opts.node_admission.?.filterLocalKeysAlloc(alloc, start_keys)
    else
        null;
    defer if (admitted_starts) |mask| alloc.free(mask);
    const filtered_starts = if (opts.start_validation == .required) blk: {
        const start_refs = try alloc.alloc(node_identity.Ref, start_keys.len);
        defer alloc.free(start_refs);
        for (start_keys, 0..) |key, i| start_refs[i] = .{ .table = null, .key = key };
        break :blk try evaluateNodeFiltersAlloc(alloc, start_refs, anchor_node.filter, opts.evaluator);
    } else null;
    defer if (filtered_starts) |mask| alloc.free(mask);
    for (start_keys, 0..) |key, start_index| {
        if (admitted_starts) |mask| if (!mask[start_index]) continue;
        if (filtered_starts) |mask| if (!mask[start_index]) continue;
        const bindings = try alloc.alloc(PatternBinding, 1);
        bindings[0] = initPatternBinding(alloc, anchor_alias, key, null, 0) catch |err| {
            alloc.free(bindings);
            return err;
        };
        try current.append(alloc, .{ .bindings = bindings });
        try work_budget.checkIntermediateStates(current.items.len, opts.max_intermediate_states);
    }

    try expandConjunctiveGroup(alloc, edge_reader, pattern.nodes, pattern.edges, pattern.predicates, &current, opts, work_budget);

    for (pattern.optional) |optional_pattern| {
        var next = std.ArrayListUnmanaged(ConjunctiveState).empty;
        errdefer freeConjunctiveStates(alloc, &next);
        for (current.items) |state| {
            var correlated = std.ArrayListUnmanaged(ConjunctiveState).empty;
            errdefer freeConjunctiveStates(alloc, &correlated);
            try correlated.append(alloc, try cloneConjunctiveState(alloc, state));
            try expandConjunctiveGroup(
                alloc,
                edge_reader,
                optional_pattern.nodes,
                optional_pattern.edges,
                optional_pattern.predicates,
                &correlated,
                opts,
                work_budget,
            );
            if (correlated.items.len == 0) {
                var null_extended = try cloneConjunctiveState(alloc, state);
                errdefer null_extended.deinit(alloc);
                for (optional_pattern.nodes) |node| {
                    if (findBinding(null_extended.bindings, node.alias) != null or containsString(null_extended.null_aliases, node.alias)) continue;
                    try appendNullAlias(alloc, &null_extended, node.alias);
                }
                try next.append(alloc, null_extended);
            } else {
                try next.appendSlice(alloc, correlated.items);
                correlated.items.len = 0;
            }
            correlated.deinit(alloc);
            try work_budget.checkIntermediateStates(next.items.len, opts.max_intermediate_states);
        }
        freeConjunctiveStates(alloc, &current);
        current = next;
    }
    return current;
}

/// Selects one logical MATCH anchor independently of JSON object and edge-array
/// ordering. Filtered aliases are preferred so callers can push the same filter
/// into their identity scan, then higher-degree aliases reduce branch fan-out;
/// lexical ordering makes otherwise equivalent patterns plan identically.
pub fn selectConjunctiveAnchor(pattern: ConjunctivePattern) ?MatchNode {
    if (pattern.nodes.len == 0) return null;
    if (pattern.anchor_alias) |anchor_alias| {
        for (pattern.nodes) |node| {
            if (std.mem.eql(u8, node.alias, anchor_alias)) return node;
        }
        return null;
    }
    var selected = pattern.nodes[0];
    for (pattern.nodes[1..]) |candidate| {
        const selected_filtered = nodeFilterActive(selected.filter);
        const candidate_filtered = nodeFilterActive(candidate.filter);
        const selected_degree = aliasRequiredDegree(pattern.edges, selected.alias);
        const candidate_degree = aliasRequiredDegree(pattern.edges, candidate.alias);
        if ((candidate_filtered and !selected_filtered) or
            (candidate_filtered == selected_filtered and candidate_degree > selected_degree) or
            (candidate_filtered == selected_filtered and candidate_degree == selected_degree and std.mem.order(u8, candidate.alias, selected.alias) == .lt))
        {
            selected = candidate;
        }
    }
    return selected;
}

fn aliasRequiredDegree(edges: []const MatchEdge, alias: []const u8) usize {
    var degree: usize = 0;
    for (edges) |edge| {
        if (std.mem.eql(u8, edge.from, alias) or std.mem.eql(u8, edge.to, alias)) degree += 1;
    }
    return degree;
}

pub const CountAggregateSpec = struct {
    alias: ?[]const u8 = null,
    distinct: bool = false,
};

/// Deduplicated execution plan for a list of named aggregate expressions.
/// Aliases remain borrowed from the caller; only the slices are owned. Keeping
/// this plan in the graph layer makes duplicate-expression budget and ownership
/// semantics identical for distributed and serverless executors.
pub const CountAggregatePlan = struct {
    unique_specs: []CountAggregateSpec,
    output_indexes: []usize,

    pub fn init(alloc: Allocator, specs: []const CountAggregateSpec) !CountAggregatePlan {
        if (specs.len > max_count_aggregates) return error.InvalidArgument;
        var unique = std.ArrayListUnmanaged(CountAggregateSpec).empty;
        errdefer unique.deinit(alloc);
        const output_indexes = try alloc.alloc(usize, specs.len);
        errdefer if (output_indexes.len > 0) alloc.free(output_indexes);

        for (specs, 0..) |spec, output_index| {
            var unique_index: ?usize = null;
            for (unique.items, 0..) |prior, i| {
                if (countAggregateSpecEql(prior, spec)) {
                    unique_index = i;
                    break;
                }
            }
            if (unique_index == null) {
                unique_index = unique.items.len;
                try unique.append(alloc, spec);
            }
            output_indexes[output_index] = unique_index.?;
        }

        return .{
            .unique_specs = try unique.toOwnedSlice(alloc),
            .output_indexes = output_indexes,
        };
    }

    pub fn deinit(self: *CountAggregatePlan, alloc: Allocator) void {
        if (self.unique_specs.len > 0) alloc.free(self.unique_specs);
        if (self.output_indexes.len > 0) alloc.free(self.output_indexes);
        self.* = undefined;
    }
};

fn countAggregateSpecEql(left: CountAggregateSpec, right: CountAggregateSpec) bool {
    if (left.distinct != right.distinct) return false;
    if (left.alias == null or right.alias == null) return left.alias == null and right.alias == null;
    return std.mem.eql(u8, left.alias.?, right.alias.?);
}

pub const CountAggregateResult = struct {
    value: u128,
    distinct_values: []node_identity.Ref = &.{},

    pub fn deinit(self: *CountAggregateResult, alloc: Allocator) void {
        for (self.distinct_values) |identity| {
            if (identity.table) |table| alloc.free(table);
            alloc.free(identity.key);
        }
        if (self.distinct_values.len > 0) alloc.free(self.distinct_values);
        self.* = undefined;
    }
};

fn validateCountAggregateSpecs(specs: []const CountAggregateSpec) !void {
    if (specs.len == 0 or specs.len > max_count_aggregates) return error.InvalidArgument;
    for (specs) |spec| if (spec.alias) |alias| {
        if (!isValidIdentifier(alias)) return error.InvalidArgument;
    };
}

/// Request-scoped exact aggregate state. Callers that enumerate an anchor
/// relation in snapshot-pinned pages keep one stream for the full relation so
/// counters and exact distinct identity sets are not reset at page boundaries.
pub const ConjunctiveCountAggregateStream = struct {
    accumulators: []StreamingCountAccumulator,

    pub fn init(
        alloc: Allocator,
        specs: []const CountAggregateSpec,
        distinct_budget: *DistinctBudget,
    ) !ConjunctiveCountAggregateStream {
        try validateCountAggregateSpecs(specs);
        const accumulators = try alloc.alloc(StreamingCountAccumulator, specs.len);
        var initialized: usize = 0;
        errdefer {
            for (accumulators[0..initialized]) |*accumulator| accumulator.deinit(alloc);
            if (accumulators.len > 0) alloc.free(accumulators);
        }
        for (specs, 0..) |spec, i| {
            accumulators[i] = .{ .spec = spec, .distinct_budget = distinct_budget };
            initialized += 1;
        }
        return .{ .accumulators = accumulators };
    }

    pub fn deinit(self: *ConjunctiveCountAggregateStream, alloc: Allocator) void {
        for (self.accumulators) |*accumulator| accumulator.deinit(alloc);
        if (self.accumulators.len > 0) alloc.free(self.accumulators);
        self.* = undefined;
    }

    /// Consume one prevalidated or validation-required anchor page. Expansion
    /// work remains charged to the request-scoped budget carried by `opts`.
    pub fn consumePageWithEdgeReader(
        self: *ConjunctiveCountAggregateStream,
        alloc: Allocator,
        edge_reader: anytype,
        start_keys: []const []const u8,
        pattern: ConjunctivePattern,
        opts: MatchOptions,
    ) !void {
        var aggregate_opts = opts;
        aggregate_opts.max_results = 0;
        aggregate_opts.require_complete = true;
        if (pattern.nodes.len == 0 or pattern.nodes.len > max_pattern_steps) return error.InvalidArgument;
        try validateConjunctivePattern(pattern);

        const anchor_node = selectConjunctiveAnchor(pattern) orelse return error.InvalidArgument;
        if (!declaredNodeTableMatches(edge_reader, null, anchor_node.table)) return error.InvalidArgument;
        var local_work_budget = WorkBudget.init(aggregate_opts.max_explored_nodes, aggregate_opts.max_explored_edges);
        const work_budget = aggregate_opts.work_budget orelse &local_work_budget;
        if (!aggregate_opts.anchors_precharged) try work_budget.consumeAnchors(start_keys.len);
        const admitted_starts = if (aggregate_opts.start_validation == .required and aggregate_opts.node_admission != null)
            try aggregate_opts.node_admission.?.filterLocalKeysAlloc(alloc, start_keys)
        else
            null;
        defer if (admitted_starts) |mask| alloc.free(mask);
        const filtered_starts = if (aggregate_opts.start_validation == .required) blk: {
            const start_refs = try alloc.alloc(node_identity.Ref, start_keys.len);
            defer alloc.free(start_refs);
            for (start_keys, 0..) |key, i| start_refs[i] = .{ .table = null, .key = key };
            break :blk try evaluateNodeFiltersAlloc(alloc, start_refs, anchor_node.filter, aggregate_opts.evaluator);
        } else null;
        defer if (filtered_starts) |mask| alloc.free(mask);

        const processed = try alloc.alloc(bool, pattern.edges.len);
        defer alloc.free(processed);
        var sink = ConjunctiveAggregateSink{ .accumulators = self.accumulators };
        for (start_keys, 0..) |key, start_index| {
            if (admitted_starts) |mask| if (!mask[start_index]) continue;
            if (filtered_starts) |mask| if (!mask[start_index]) continue;

            const bindings = try alloc.alloc(PatternBinding, 1);
            bindings[0] = initPatternBinding(alloc, anchor_node.alias, key, null, 0) catch |err| {
                alloc.free(bindings);
                return err;
            };
            var state = ConjunctiveState{ .bindings = bindings };
            defer state.deinit(alloc);
            @memset(processed, false);
            try streamConjunctiveGroup(
                alloc,
                edge_reader,
                pattern,
                pattern.nodes,
                pattern.edges,
                pattern.predicates,
                state,
                processed,
                0,
                null,
                null,
                aggregate_opts,
                work_budget,
                &sink,
            );
        }
    }

    pub fn finishAlloc(self: *ConjunctiveCountAggregateStream, alloc: Allocator) ![]CountAggregateResult {
        const results = try alloc.alloc(CountAggregateResult, self.accumulators.len);
        var initialized: usize = 0;
        errdefer {
            for (results[0..initialized]) |*result| result.deinit(alloc);
            if (results.len > 0) alloc.free(results);
        }
        for (self.accumulators, 0..) |*accumulator, i| {
            results[i] = try accumulator.finish(alloc);
            initialized += 1;
        }
        return results;
    }
};

/// Stream completed graph bindings directly into aggregate accumulators. Exact
/// aggregate queries retain only counters and distinct identities, rather than
/// materializing either internal matcher states or public result rows.
pub fn aggregateConjunctivePattern(
    alloc: Allocator,
    graph_index: *graph_mod.GraphIndex,
    start_keys: []const []const u8,
    pattern: ConjunctivePattern,
    specs: []const CountAggregateSpec,
    opts: MatchOptions,
) ![]CountAggregateResult {
    const Reader = struct {
        graph_index: *graph_mod.GraphIndex,

        pub fn getEdges(self: @This(), a: Allocator, table: ?[]const u8, key: []const u8, types: []const []const u8, direction: graph_mod.EdgeDirection) ![]graph_mod.Edge {
            if (table != null) return try a.alloc(graph_mod.Edge, 0);
            return try self.graph_index.getEdgesByTypes(a, key, types, direction);
        }

        pub fn getEdgesBounded(self: @This(), a: Allocator, table: ?[]const u8, key: []const u8, types: []const []const u8, direction: graph_mod.EdgeDirection, max_edges: usize, max_bytes: usize) ![]graph_mod.Edge {
            if (table != null) return try a.alloc(graph_mod.Edge, 0);
            return try self.graph_index.getEdgesByTypesBounded(a, key, types, direction, max_edges, max_bytes);
        }

        pub fn freeEdges(_: @This(), a: Allocator, edges: []graph_mod.Edge) void {
            graph_mod.GraphIndex.freeEdges(a, edges);
        }
    };
    return try aggregateConjunctivePatternWithEdgeReader(alloc, Reader{ .graph_index = graph_index }, start_keys, pattern, specs, opts);
}

pub fn aggregateConjunctivePatternWithEdgeReader(
    alloc: Allocator,
    edge_reader: anytype,
    start_keys: []const []const u8,
    pattern: ConjunctivePattern,
    specs: []const CountAggregateSpec,
    opts: MatchOptions,
) ![]CountAggregateResult {
    var local_distinct_budget = DistinctBudget.init(
        default_max_distinct_identities,
        default_max_distinct_state_bytes,
    );
    var stream = try ConjunctiveCountAggregateStream.init(
        alloc,
        specs,
        opts.distinct_budget orelse &local_distinct_budget,
    );
    defer stream.deinit(alloc);
    try stream.consumePageWithEdgeReader(alloc, edge_reader, start_keys, pattern, opts);
    return try stream.finishAlloc(alloc);
}

const StreamingCountAccumulator = struct {
    spec: CountAggregateSpec,
    distinct_budget: *DistinctBudget,
    value: u128 = 0,
    seen: node_identity.BorrowedMap(void) = .{},
    distinct_values: std.ArrayListUnmanaged(node_identity.Ref) = .empty,

    fn observe(self: *StreamingCountAccumulator, alloc: Allocator, state: ConjunctiveState) !void {
        if (self.spec.alias == null) {
            self.value = std.math.add(u128, self.value, 1) catch return error.QueryCandidateBudgetExceeded;
            return;
        }
        const binding = findBinding(state.bindings, self.spec.alias.?) orelse return;
        if (!self.spec.distinct) {
            self.value = std.math.add(u128, self.value, 1) catch return error.QueryCandidateBudgetExceeded;
            return;
        }
        const ref = node_identity.Ref{ .table = binding.table, .key = binding.key };
        if (self.seen.contains(ref)) return;
        try self.distinct_budget.consume(ref);
        try ensureDistinctValueCapacity(
            alloc,
            self.distinct_budget,
            &self.distinct_values,
            self.distinct_values.items.len + 1,
        );
        try ensureBorrowedDistinctMapCapacity(
            void,
            alloc,
            self.distinct_budget,
            &self.seen,
            self.distinct_values.items.len + 1,
        );
        try appendOwnedIdentity(alloc, &self.distinct_values, binding.table, binding.key);
        const owned = self.distinct_values.items[self.distinct_values.items.len - 1];
        self.seen.putAssumeCapacityNoClobber(owned, {});
        self.value = @intCast(self.distinct_values.items.len);
    }

    fn finish(self: *StreamingCountAccumulator, alloc: Allocator) !CountAggregateResult {
        const output_bytes = std.math.mul(
            usize,
            self.distinct_values.items.len,
            @sizeOf(node_identity.Ref),
        ) catch return error.GraphDistinctBudgetExceeded;
        try self.distinct_budget.consumeRetainedBytes(output_bytes);
        const values = try self.distinct_values.toOwnedSlice(alloc);
        self.distinct_values = .empty;
        return .{ .value = self.value, .distinct_values = values };
    }

    fn deinit(self: *StreamingCountAccumulator, alloc: Allocator) void {
        self.seen.deinit(alloc);
        for (self.distinct_values.items) |identity| {
            if (identity.table) |table| alloc.free(table);
            alloc.free(identity.key);
        }
        self.distinct_values.deinit(alloc);
        self.* = undefined;
    }
};

const ConjunctiveAggregateSink = struct {
    accumulators: []StreamingCountAccumulator,

    fn full(_: *@This()) bool {
        return false;
    }

    fn observe(self: *@This(), alloc: Allocator, state: ConjunctiveState) !void {
        for (self.accumulators) |*accumulator| try accumulator.observe(alloc, state);
    }
};

const ConjunctiveBindingSink = struct {
    projection: AliasProjection,
    max_results: u32,
    work_budget: *WorkBudget,
    returned_state_budget: ?*WorkBudget,
    matches: std.ArrayListUnmanaged(PatternMatch) = .empty,

    fn init(
        alloc: Allocator,
        return_aliases: []const []const u8,
        max_results: u32,
        work_budget: *WorkBudget,
        returned_state_budget: ?*WorkBudget,
    ) !@This() {
        return .{
            .projection = try AliasProjection.init(alloc, return_aliases),
            .max_results = max_results,
            .work_budget = work_budget,
            .returned_state_budget = returned_state_budget,
        };
    }

    fn full(self: *@This()) bool {
        return self.matches.items.len >= self.max_results;
    }

    fn observe(self: *@This(), alloc: Allocator, state: ConjunctiveState) !void {
        if (self.full()) return;
        var projected = try projectConjunctiveState(
            alloc,
            state,
            &self.projection,
            self.work_budget,
            self.returned_state_budget,
        );
        errdefer projected.deinit(alloc);
        if (self.matches.capacity == self.matches.items.len)
            try self.matches.ensureTotalCapacityPrecise(alloc, self.matches.items.len + 1);
        self.matches.appendAssumeCapacity(projected);
    }

    fn finish(self: *@This(), alloc: Allocator) ![]PatternMatch {
        const matches = try self.matches.toOwnedSlice(alloc);
        self.matches = .empty;
        return matches;
    }

    fn deinit(self: *@This(), alloc: Allocator) void {
        for (self.matches.items) |*match| match.deinit(alloc);
        self.matches.deinit(alloc);
        self.projection.deinit(alloc);
        self.* = undefined;
    }
};

fn streamConjunctiveOptionalGroups(
    alloc: Allocator,
    edge_reader: anytype,
    pattern: ConjunctivePattern,
    optional_index: usize,
    state: ConjunctiveState,
    opts: MatchOptions,
    work_budget: *WorkBudget,
    sink: anytype,
) anyerror!void {
    if (sink.full()) return;
    if (optional_index == pattern.optional.len) {
        return try sink.observe(alloc, state);
    }

    const group = pattern.optional[optional_index];
    const processed = try alloc.alloc(bool, group.edges.len);
    defer alloc.free(processed);
    @memset(processed, false);
    var matched: usize = 0;
    try streamConjunctiveGroup(
        alloc,
        edge_reader,
        pattern,
        group.nodes,
        group.edges,
        group.predicates,
        state,
        processed,
        0,
        optional_index,
        &matched,
        opts,
        work_budget,
        sink,
    );
    if (matched != 0 or sink.full()) return;

    var null_extended = try cloneConjunctiveState(alloc, state);
    defer null_extended.deinit(alloc);
    for (group.nodes) |node| {
        if (findBinding(null_extended.bindings, node.alias) != null or containsString(null_extended.null_aliases, node.alias)) continue;
        try appendNullAlias(alloc, &null_extended, node.alias);
    }
    try streamConjunctiveOptionalGroups(
        alloc,
        edge_reader,
        pattern,
        optional_index + 1,
        null_extended,
        opts,
        work_budget,
        sink,
    );
}

fn streamConjunctiveGroup(
    alloc: Allocator,
    edge_reader: anytype,
    pattern: ConjunctivePattern,
    group_nodes: []const MatchNode,
    edges: []const MatchEdge,
    predicates: []const MatchPredicate,
    state: ConjunctiveState,
    processed: []bool,
    processed_count: usize,
    completed_optional_index: ?usize,
    optional_match_count: ?*usize,
    opts: MatchOptions,
    work_budget: *WorkBudget,
    sink: anytype,
) anyerror!void {
    if (sink.full()) return;
    if (processed_count == edges.len) {
        if (!(try conjunctivePredicatesPass(alloc, edge_reader, state, predicates, opts, work_budget))) return;
        if (completed_optional_index) |optional_index| {
            optional_match_count.?.* = std.math.add(usize, optional_match_count.?.*, 1) catch return error.QueryCandidateBudgetExceeded;
            return try streamConjunctiveOptionalGroups(
                alloc,
                edge_reader,
                pattern,
                optional_index + 1,
                state,
                opts,
                work_budget,
                sink,
            );
        }
        return try streamConjunctiveOptionalGroups(alloc, edge_reader, pattern, 0, state, opts, work_budget, sink);
    }

    var selected: ?usize = null;
    for (edges, 0..) |edge, edge_index| {
        if (processed[edge_index]) continue;
        if (bindingOrNullKnown(state, edge.from) or bindingOrNullKnown(state, edge.to)) {
            selected = edge_index;
            break;
        }
    }
    const edge_index = selected orelse return error.InvalidArgument;
    const edge = edges[edge_index];
    const from_binding = findBinding(state.bindings, edge.from);
    const to_binding = findBinding(state.bindings, edge.to);
    if ((from_binding == null and containsString(state.null_aliases, edge.from)) or
        (to_binding == null and containsString(state.null_aliases, edge.to))) return;
    if (from_binding == null and to_binding == null) return error.InvalidArgument;

    const source = from_binding orelse to_binding.?;
    const target = if (from_binding != null) to_binding else from_binding;
    const new_alias = if (from_binding != null) edge.to else edge.from;
    const new_node = findMatchNode(group_nodes, new_alias);
    const node_filter = if (new_node) |node| node.filter else NodeFilter{};
    var target_node_storage: [1]node_identity.Ref = undefined;
    const target_nodes: []const node_identity.Ref = if (target) |binding| blk: {
        target_node_storage[0] = .{ .key = binding.key, .table = binding.table };
        break :blk &target_node_storage;
    } else &.{};
    const reached_table = if (target) |binding|
        binding.table
    else if (new_node) |node|
        node.table
    else
        return error.InvalidArgument;
    var traversal_storage: [2]PhysicalEdgeTraversal = undefined;
    const traversals = try planPhysicalEdgeTraversals(
        edge_reader,
        edge.step,
        from_binding != null,
        source.table,
        reached_table,
        &traversal_storage,
    );
    processed[edge_index] = true;
    defer processed[edge_index] = false;
    for (traversals) |traversal| {
        if (sink.full()) break;
        try streamReachableIntoConjunctiveGroup(
            alloc,
            edge_reader,
            pattern,
            group_nodes,
            edges,
            predicates,
            state,
            processed,
            processed_count + 1,
            completed_optional_index,
            optional_match_count,
            opts,
            work_budget,
            sink,
            source,
            target != null,
            new_alias,
            new_node,
            traversal,
            node_filter,
            target_nodes,
        );
    }
}

fn streamReachableIntoConjunctiveGroup(
    alloc: Allocator,
    edge_reader: anytype,
    pattern: ConjunctivePattern,
    group_nodes: []const MatchNode,
    edges: []const MatchEdge,
    predicates: []const MatchPredicate,
    state: ConjunctiveState,
    processed: []bool,
    processed_count: usize,
    completed_optional_index: ?usize,
    optional_match_count: ?*usize,
    opts: MatchOptions,
    work_budget: *WorkBudget,
    sink: anytype,
    source: PatternBinding,
    target_bound: bool,
    new_alias: []const u8,
    new_node: ?MatchNode,
    traversal: PhysicalEdgeTraversal,
    node_filter: NodeFilter,
    target_nodes: []const node_identity.Ref,
) !void {
    const EdgeReader = @TypeOf(edge_reader);
    const Sink = @TypeOf(sink);
    const Context = struct {
        alloc: Allocator,
        edge_reader: EdgeReader,
        pattern: ConjunctivePattern,
        group_nodes: []const MatchNode,
        edges: []const MatchEdge,
        predicates: []const MatchPredicate,
        state: ConjunctiveState,
        processed: []bool,
        processed_count: usize,
        completed_optional_index: ?usize,
        optional_match_count: ?*usize,
        opts: MatchOptions,
        work_budget: *WorkBudget,
        sink: Sink,
        target_bound: bool,
        new_alias: []const u8,
        new_node: ?MatchNode,

        fn isFull(raw: *anyopaque) bool {
            const self: *@This() = @ptrCast(@alignCast(raw));
            return self.sink.full();
        }

        fn observe(
            raw: *anyopaque,
            reached: node_identity.Ref,
            depth: u32,
            _: []const paths_mod.PathEdge,
        ) !void {
            const self: *@This() = @ptrCast(@alignCast(raw));
            if (self.sink.full()) return;
            if (self.new_node) |node| {
                if (!declaredNodeTableMatches(self.edge_reader, reached.table, node.table)) return;
            }
            var expanded = try cloneConjunctiveState(self.alloc, self.state);
            defer expanded.deinit(self.alloc);
            if (!self.target_bound) {
                try appendConcreteBindingRef(self.alloc, &expanded, self.new_alias, reached, depth);
            }
            try streamConjunctiveGroup(
                self.alloc,
                self.edge_reader,
                self.pattern,
                self.group_nodes,
                self.edges,
                self.predicates,
                expanded,
                self.processed,
                self.processed_count,
                self.completed_optional_index,
                self.optional_match_count,
                self.opts,
                self.work_budget,
                self.sink,
            );
        }
    };

    var context = Context{
        .alloc = alloc,
        .edge_reader = edge_reader,
        .pattern = pattern,
        .group_nodes = group_nodes,
        .edges = edges,
        .predicates = predicates,
        .state = state,
        .processed = processed,
        .processed_count = processed_count,
        .completed_optional_index = completed_optional_index,
        .optional_match_count = optional_match_count,
        .opts = opts,
        .work_budget = work_budget,
        .sink = sink,
        .target_bound = target_bound,
        .new_alias = new_alias,
        .new_node = new_node,
    };
    try streamReachableNodes(
        alloc,
        edge_reader,
        source.key,
        source.table,
        traversal,
        node_filter,
        target_nodes,
        target_bound,
        opts.max_intermediate_states,
        opts.evaluator,
        opts.node_admission,
        false,
        opts.stats,
        work_budget,
        .{ .ctx = &context, .full_fn = Context.isFull, .observe_fn = Context.observe },
    );
}

fn appendOwnedIdentity(alloc: Allocator, values: *std.ArrayListUnmanaged(node_identity.Ref), table_name: ?[]const u8, key_name: []const u8) !void {
    const table = if (table_name) |name| try alloc.dupe(u8, name) else null;
    errdefer if (table) |name| alloc.free(name);
    const key = try alloc.dupe(u8, key_name);
    errdefer alloc.free(key);
    try values.append(alloc, .{ .table = table, .key = key });
}

pub fn validateConjunctivePattern(pattern: ConjunctivePattern) !void {
    if (pattern.nodes.len == 0 or
        pattern.nodes.len > max_conjunctive_nodes or
        pattern.edges.len > max_conjunctive_edges or
        pattern.optional.len > max_optional_patterns or
        pattern.predicates.len > max_match_predicates)
        return error.InvalidArgument;

    var total_nodes = pattern.nodes.len;
    var total_edges = pattern.edges.len;
    var total_predicates = pattern.predicates.len;
    try addPatternPredicateEdges(&total_edges, pattern.predicates);
    for (pattern.optional) |optional_pattern| {
        try addPatternComplexity(&total_nodes, optional_pattern.nodes.len, max_conjunctive_nodes);
        try addPatternComplexity(&total_edges, optional_pattern.edges.len, max_conjunctive_edges);
        try addPatternComplexity(&total_predicates, optional_pattern.predicates.len, max_match_predicates);
        try addPatternPredicateEdges(&total_edges, optional_pattern.predicates);
    }

    for (pattern.nodes, 0..) |node, i| {
        if (!isValidIdentifier(node.alias) or (node.table != null and !isValidTableQualifier(node.table.?))) return error.InvalidArgument;
        for (pattern.nodes[0..i]) |prior| if (std.mem.eql(u8, prior.alias, node.alias)) return error.InvalidArgument;
    }
    if (pattern.anchor_alias) |anchor_alias| {
        if (!isValidIdentifier(anchor_alias) or findMatchNode(pattern.nodes, anchor_alias) == null)
            return error.InvalidArgument;
    }
    for (pattern.edges) |edge| try validateMatchEdge(pattern.nodes, edge);
    try validateRequiredPatternConnected(pattern.nodes, pattern.edges);
    for (pattern.predicates) |predicate| try validateMatchPredicate(pattern.nodes, predicate);
    for (pattern.optional, 0..) |optional_pattern, optional_index| {
        for (optional_pattern.nodes, 0..) |node, node_index| {
            if (!isValidIdentifier(node.alias) or (node.table != null and !isValidTableQualifier(node.table.?)) or
                findMatchNode(pattern.nodes, node.alias) != null)
                return error.InvalidArgument;
            for (optional_pattern.nodes[0..node_index]) |prior| {
                if (std.mem.eql(u8, prior.alias, node.alias)) return error.InvalidArgument;
            }
            for (pattern.optional[0..optional_index]) |prior_group| {
                if (findMatchNode(prior_group.nodes, node.alias) != null) return error.InvalidArgument;
            }
        }
        for (optional_pattern.edges) |edge| {
            if (!optionalAliasVisible(pattern, optional_index, edge.from) or
                !optionalAliasVisible(pattern, optional_index, edge.to))
                return error.InvalidArgument;
            try validateEdgeHops(edge);
        }
        try validateOptionalPatternConnected(pattern, optional_index);
        for (optional_pattern.predicates) |predicate| switch (predicate) {
            .not_equal => |neq| {
                if (!optionalAliasVisible(pattern, optional_index, neq.left) or
                    !optionalAliasVisible(pattern, optional_index, neq.right))
                    return error.InvalidArgument;
            },
            .not_exists => |edges| for (edges) |edge| {
                if (!optionalAliasVisible(pattern, optional_index, edge.from) or
                    !optionalAliasVisible(pattern, optional_index, edge.to))
                    return error.InvalidArgument;
                try validateEdgeHops(edge);
            },
        };
    }
}

fn addPatternComplexity(total: *usize, amount: usize, limit: usize) !void {
    total.* = std.math.add(usize, total.*, amount) catch return error.InvalidArgument;
    if (total.* > limit) return error.InvalidArgument;
}

fn addPatternPredicateEdges(total_edges: *usize, predicates: []const MatchPredicate) !void {
    for (predicates) |predicate| switch (predicate) {
        .not_equal => {},
        .not_exists => |edges| {
            if (edges.len == 0 or edges.len > max_conjunctive_edges) return error.InvalidArgument;
            try addPatternComplexity(total_edges, edges.len, max_conjunctive_edges);
        },
    };
}

/// Required MATCH has conjunctive semantics, not an implicit Cartesian
/// product. Every declared alias must therefore be reachable from the anchor.
fn validateRequiredPatternConnected(nodes: []const MatchNode, edges: []const MatchEdge) !void {
    if (nodes.len == 1) return;
    if (edges.len == 0 or nodes.len > max_pattern_steps) return error.InvalidArgument;

    var connected: [max_pattern_steps]bool = @splat(false);
    connected[0] = true;
    var changed = true;
    while (changed) {
        changed = false;
        for (edges) |edge| {
            const from_index = matchNodeIndex(nodes, edge.from) orelse return error.InvalidArgument;
            const to_index = matchNodeIndex(nodes, edge.to) orelse return error.InvalidArgument;
            if (connected[from_index] == connected[to_index]) continue;
            connected[from_index] = true;
            connected[to_index] = true;
            changed = true;
        }
    }
    for (connected[0..nodes.len]) |is_connected| if (!is_connected) return error.InvalidArgument;
}

/// OPTIONAL groups must be correlated with aliases already visible to the
/// query, and every alias introduced by the group must participate in that
/// connected component. This prevents silently ignored declarations.
fn validateOptionalPatternConnected(pattern: ConjunctivePattern, optional_index: usize) !void {
    const group = pattern.optional[optional_index];
    if (group.nodes.len == 0) return;
    if (group.nodes.len > max_pattern_steps or group.edges.len == 0) return error.InvalidArgument;

    var connected: [max_pattern_steps]bool = @splat(false);
    var changed = true;
    while (changed) {
        changed = false;
        for (group.edges) |edge| {
            const from_index = matchNodeIndex(group.nodes, edge.from);
            const to_index = matchNodeIndex(group.nodes, edge.to);
            const from_connected = if (from_index) |index| connected[index] else optionalAliasVisibleBefore(pattern, optional_index, edge.from);
            const to_connected = if (to_index) |index| connected[index] else optionalAliasVisibleBefore(pattern, optional_index, edge.to);
            if (!from_connected and !to_connected) continue;
            if (from_index) |index| if (!connected[index]) {
                connected[index] = true;
                changed = true;
            };
            if (to_index) |index| if (!connected[index]) {
                connected[index] = true;
                changed = true;
            };
        }
    }
    for (connected[0..group.nodes.len]) |is_connected| if (!is_connected) return error.InvalidArgument;
}

fn optionalAliasVisibleBefore(pattern: ConjunctivePattern, optional_index: usize, alias: []const u8) bool {
    if (findMatchNode(pattern.nodes, alias) != null) return true;
    for (pattern.optional[0..optional_index]) |prior_group| {
        if (findMatchNode(prior_group.nodes, alias) != null) return true;
    }
    return false;
}

fn matchNodeIndex(nodes: []const MatchNode, alias: []const u8) ?usize {
    for (nodes, 0..) |node, index| if (std.mem.eql(u8, node.alias, alias)) return index;
    return null;
}

fn optionalAliasVisible(pattern: ConjunctivePattern, optional_index: usize, alias: []const u8) bool {
    if (findMatchNode(pattern.nodes, alias) != null or
        findMatchNode(pattern.optional[optional_index].nodes, alias) != null)
        return true;
    for (pattern.optional[0..optional_index]) |prior_group| {
        if (findMatchNode(prior_group.nodes, alias) != null) return true;
    }
    return false;
}

fn validateMatchEdge(nodes: []const MatchNode, edge: MatchEdge) !void {
    if (!isValidIdentifier(edge.from) or
        !isValidIdentifier(edge.to) or
        findMatchNode(nodes, edge.from) == null or
        findMatchNode(nodes, edge.to) == null) return error.InvalidArgument;
    try validateEdgeHops(edge);
}

fn validateEdgeHops(edge: MatchEdge) !void {
    const min_hops = if (edge.step.min_hops == 0) 1 else edge.step.min_hops;
    const max_hops = if (edge.step.max_hops == 0) 1 else edge.step.max_hops;
    if (min_hops > max_hops or max_hops > max_pattern_hops) return error.InvalidArgument;
}

fn validateMatchPredicate(nodes: []const MatchNode, predicate: MatchPredicate) !void {
    switch (predicate) {
        .not_equal => |neq| if (!isValidIdentifier(neq.left) or
            !isValidIdentifier(neq.right) or
            findMatchNode(nodes, neq.left) == null or
            findMatchNode(nodes, neq.right) == null) return error.InvalidArgument,
        .not_exists => |edges| for (edges) |edge| try validateMatchEdge(nodes, edge),
    }
}

fn expandConjunctiveGroup(
    alloc: Allocator,
    edge_reader: anytype,
    group_nodes: []const MatchNode,
    edges: []const MatchEdge,
    predicates: []const MatchPredicate,
    states: *std.ArrayListUnmanaged(ConjunctiveState),
    opts: MatchOptions,
    work_budget: *WorkBudget,
) !void {
    const processed = try alloc.alloc(bool, edges.len);
    defer alloc.free(processed);
    @memset(processed, false);
    var processed_count: usize = 0;

    while (processed_count < edges.len) {
        var selected: ?usize = null;
        for (edges, 0..) |edge, i| {
            if (processed[i]) continue;
            var connected = false;
            for (states.items) |state| {
                if (bindingOrNullKnown(state, edge.from) or bindingOrNullKnown(state, edge.to)) {
                    connected = true;
                    break;
                }
            }
            if (connected) {
                selected = i;
                break;
            }
        }
        const edge_index = selected orelse return error.InvalidArgument;
        const edge = edges[edge_index];
        var next = std.ArrayListUnmanaged(ConjunctiveState).empty;
        errdefer freeConjunctiveStates(alloc, &next);
        for (states.items) |state| {
            try expandConjunctiveEdge(alloc, edge_reader, group_nodes, edge, state, &next, opts, work_budget);
            try work_budget.checkIntermediateStates(next.items.len, opts.max_intermediate_states);
        }
        freeConjunctiveStates(alloc, states);
        states.* = next;
        processed[edge_index] = true;
        processed_count += 1;
        if (states.items.len == 0) return;
    }

    var write_index: usize = 0;
    for (states.items, 0..) |*state, read_index| {
        if (try conjunctivePredicatesPass(alloc, edge_reader, state.*, predicates, opts, work_budget)) {
            if (write_index != read_index) states.items[write_index] = state.*;
            write_index += 1;
        } else {
            state.deinit(alloc);
        }
    }
    states.items.len = write_index;
}

fn expandConjunctiveEdge(
    alloc: Allocator,
    edge_reader: anytype,
    group_nodes: []const MatchNode,
    edge: MatchEdge,
    state: ConjunctiveState,
    out: *std.ArrayListUnmanaged(ConjunctiveState),
    opts: MatchOptions,
    work_budget: *WorkBudget,
) !void {
    const from_binding = findBinding(state.bindings, edge.from);
    const to_binding = findBinding(state.bindings, edge.to);
    if ((from_binding == null and containsString(state.null_aliases, edge.from)) or
        (to_binding == null and containsString(state.null_aliases, edge.to))) return;
    if (from_binding == null and to_binding == null) return;

    const source = from_binding orelse to_binding.?;
    const target = if (from_binding != null) to_binding else from_binding;
    const new_alias = if (from_binding != null) edge.to else edge.from;
    const new_node = findMatchNode(group_nodes, new_alias);
    const node_filter = if (new_node) |node| node.filter else NodeFilter{};
    var target_node_storage: [1]node_identity.Ref = undefined;
    const target_nodes: []const node_identity.Ref = if (target) |binding| blk: {
        target_node_storage[0] = .{ .key = binding.key, .table = binding.table };
        break :blk &target_node_storage;
    } else &.{};
    const reached_table = if (target) |binding|
        binding.table
    else if (new_node) |node|
        node.table
    else
        return error.InvalidArgument;
    var traversal_storage: [2]PhysicalEdgeTraversal = undefined;
    const traversals = try planPhysicalEdgeTraversals(
        edge_reader,
        edge.step,
        from_binding != null,
        source.table,
        reached_table,
        &traversal_storage,
    );
    for (traversals) |traversal| {
        const reachable = try findReachableNodes(
            alloc,
            edge_reader,
            source.key,
            source.table,
            traversal,
            node_filter,
            target_nodes,
            target != null,
            0,
            opts.require_complete,
            opts.max_intermediate_states,
            opts.evaluator,
            opts.node_admission,
            false,
            opts.stats,
            work_budget,
        );
        defer {
            for (reachable) |*node| node.deinit(alloc);
            if (reachable.len > 0) alloc.free(reachable);
        }
        for (reachable) |reached| {
            if (new_node) |node| if (!declaredNodeTableMatches(edge_reader, reached.table, node.table)) continue;
            var expanded = try cloneConjunctiveState(alloc, state);
            errdefer expanded.deinit(alloc);
            if (target == null) try appendConcreteBinding(alloc, &expanded, new_alias, reached);
            try out.append(alloc, expanded);
        }
    }
}

fn conjunctivePredicatesPass(alloc: Allocator, edge_reader: anytype, state: ConjunctiveState, predicates: []const MatchPredicate, opts: MatchOptions, work_budget: *WorkBudget) !bool {
    for (predicates) |predicate| switch (predicate) {
        .not_equal => |neq| {
            const left = findBinding(state.bindings, neq.left) orelse return false;
            const right = findBinding(state.bindings, neq.right) orelse return false;
            if (std.mem.eql(u8, left.key, right.key) and optionalTableEql(left.table, right.table)) return false;
        },
        .not_exists => |negative_edges| {
            if (try correlatedEdgesExist(alloc, edge_reader, state, negative_edges, opts, work_budget)) return false;
        },
    };
    return true;
}

fn correlatedEdgesExist(alloc: Allocator, edge_reader: anytype, state: ConjunctiveState, edges: []const MatchEdge, opts: MatchOptions, work_budget: *WorkBudget) !bool {
    for (edges) |edge| {
        const from = findBinding(state.bindings, edge.from) orelse return false;
        const to = findBinding(state.bindings, edge.to) orelse return false;
        const targets = [_]node_identity.Ref{.{ .key = to.key, .table = to.table }};
        var traversal_storage: [2]PhysicalEdgeTraversal = undefined;
        const traversals = try planPhysicalEdgeTraversals(
            edge_reader,
            edge.step,
            true,
            from.table,
            to.table,
            &traversal_storage,
        );
        var exists = false;
        for (traversals) |traversal| {
            const reachable = try findReachableNodes(alloc, edge_reader, from.key, from.table, traversal, .{}, &targets, true, 1, false, opts.max_intermediate_states, opts.evaluator, opts.node_admission, false, opts.stats, work_budget);
            defer {
                for (reachable) |*node| node.deinit(alloc);
                if (reachable.len > 0) alloc.free(reachable);
            }
            if (reachable.len > 0) {
                exists = true;
                break;
            }
        }
        if (!exists) return false;
    }
    return edges.len > 0;
}

/// Compile one logical MATCH edge into one or two physically routed reads.
/// Incoming adjacency is stored with its physical source, so crossing a table
/// boundary requires the opposite alias's table. An undirected cross-table
/// edge spans two disjoint physical source domains and therefore needs both
/// oriented reads; charging each read preserves the public work budgets.
fn planPhysicalEdgeTraversals(
    edge_reader: anytype,
    declared_step: PatternEdgeStep,
    starts_at_declared_from: bool,
    start_table: ?[]const u8,
    reached_table: ?[]const u8,
    storage: *[2]PhysicalEdgeTraversal,
) ![]const PhysicalEdgeTraversal {
    var step = declared_step;
    if (!starts_at_declared_from) step.direction = reverseDirection(step.direction);

    const canonical_start = canonicalizeNodeTable(edge_reader, start_table);
    const canonical_reached = canonicalizeNodeTable(edge_reader, reached_table);
    var traversal_count: usize = 1;
    if (optionalTableEql(canonical_start, canonical_reached)) {
        storage[0] = .{ .step = step };
    } else {
        const max_hops = if (step.max_hops == 0) @as(u32, 1) else step.max_hops;
        if (max_hops > 1 and step.direction != .out)
            return error.GraphReverseVariablePathUnsupported;

        switch (step.direction) {
            .out => storage[0] = .{ .step = step },
            .in => storage[0] = .{
                .step = step,
                .incoming_source = .{ .table = reached_table },
            },
            .both => {
                var outgoing = step;
                outgoing.direction = .out;
                var incoming = step;
                incoming.direction = .in;
                storage[0] = .{ .step = outgoing };
                storage[1] = .{
                    .step = incoming,
                    .incoming_source = .{ .table = reached_table },
                };
                traversal_count = 2;
            },
        }
    }

    const traversals = storage[0..traversal_count];
    if (comptime @hasDecl(@TypeOf(edge_reader), "validatePatternSourceTable")) {
        for (traversals) |traversal| {
            try edge_reader.validatePatternSourceTable(
                if (traversal.incoming_source) |source| source.table else start_table,
                traversal.incoming_source != null,
            );
        }
    }
    return traversals;
}

fn reverseDirection(direction: graph_mod.EdgeDirection) graph_mod.EdgeDirection {
    return switch (direction) {
        .out => .in,
        .in => .out,
        .both => .both,
    };
}

fn findMatchNode(nodes: []const MatchNode, alias: []const u8) ?MatchNode {
    for (nodes) |node| if (std.mem.eql(u8, node.alias, alias)) return node;
    return null;
}

fn containsString(values: []const []const u8, needle: []const u8) bool {
    for (values) |value| if (std.mem.eql(u8, value, needle)) return true;
    return false;
}

fn bindingOrNullKnown(state: ConjunctiveState, alias: []const u8) bool {
    return findBinding(state.bindings, alias) != null or containsString(state.null_aliases, alias);
}

fn cloneConjunctiveState(alloc: Allocator, state: ConjunctiveState) !ConjunctiveState {
    const bindings = try cloneBindings(alloc, state.bindings);
    errdefer {
        for (bindings) |*binding| binding.deinit(alloc);
        if (bindings.len > 0) alloc.free(bindings);
    }
    const null_aliases = try alloc.alloc([]u8, state.null_aliases.len);
    var initialized: usize = 0;
    errdefer {
        for (null_aliases[0..initialized]) |alias| alloc.free(alias);
        if (null_aliases.len > 0) alloc.free(null_aliases);
    }
    for (state.null_aliases, 0..) |alias, i| {
        null_aliases[i] = try alloc.dupe(u8, alias);
        initialized += 1;
    }
    return .{ .bindings = bindings, .null_aliases = null_aliases };
}

fn appendConcreteBinding(alloc: Allocator, state: *ConjunctiveState, alias: []const u8, reached: ReachableNode) !void {
    return appendConcreteBindingRef(
        alloc,
        state,
        alias,
        .{ .key = reached.key, .table = reached.table },
        reached.depth,
    );
}

fn appendConcreteBindingRef(
    alloc: Allocator,
    state: *ConjunctiveState,
    alias: []const u8,
    reached: node_identity.Ref,
    depth: u32,
) !void {
    const expanded = try alloc.alloc(PatternBinding, state.bindings.len + 1);
    errdefer alloc.free(expanded);
    for (state.bindings, 0..) |binding, i| expanded[i] = binding;
    expanded[state.bindings.len] = try initPatternBinding(alloc, alias, reached.key, reached.table, depth);
    if (state.bindings.len > 0) alloc.free(state.bindings);
    state.bindings = expanded;
}

fn appendNullAlias(alloc: Allocator, state: *ConjunctiveState, alias: []const u8) !void {
    const expanded = try alloc.alloc([]u8, state.null_aliases.len + 1);
    for (state.null_aliases, 0..) |value, i| expanded[i] = value;
    expanded[state.null_aliases.len] = try alloc.dupe(u8, alias);
    if (state.null_aliases.len > 0) alloc.free(state.null_aliases);
    state.null_aliases = expanded;
}

const AliasProjection = struct {
    all: bool,
    selected: std.StringHashMapUnmanaged(void) = .empty,

    fn init(alloc: Allocator, aliases: []const []const u8) !AliasProjection {
        var projection = AliasProjection{ .all = aliases.len == 0 };
        errdefer projection.deinit(alloc);
        if (!projection.all) {
            try projection.selected.ensureTotalCapacity(alloc, @intCast(aliases.len));
            for (aliases) |alias| projection.selected.putAssumeCapacity(alias, {});
        }
        return projection;
    }

    fn contains(self: *const AliasProjection, alias: []const u8) bool {
        return self.all or self.selected.contains(alias);
    }

    fn deinit(self: *AliasProjection, alloc: Allocator) void {
        self.selected.deinit(alloc);
        self.* = undefined;
    }
};

fn projectConjunctiveState(
    alloc: Allocator,
    state: ConjunctiveState,
    projection: *const AliasProjection,
    work_budget: *WorkBudget,
    returned_state_budget: ?*WorkBudget,
) !PatternMatch {
    const retained_bytes = projectedConjunctiveStateRetainedBytes(state, projection) catch
        return work_budget.exhaust(.retained_state_bytes, work_budget.max_retained_state_bytes);
    try work_budget.retainStateBytes(retained_bytes);
    errdefer work_budget.releaseStateBytes(retained_bytes);
    var bindings = std.ArrayListUnmanaged(PatternBinding).empty;
    errdefer {
        for (bindings.items) |*binding| binding.deinit(alloc);
        bindings.deinit(alloc);
    }
    for (state.bindings) |binding| {
        if (!projection.contains(binding.alias)) continue;
        try bindings.append(alloc, try initPatternBinding(
            alloc,
            binding.alias,
            binding.key,
            binding.table,
            binding.depth,
        ));
    }
    var null_aliases = std.ArrayListUnmanaged([]u8).empty;
    errdefer {
        for (null_aliases.items) |alias| alloc.free(alias);
        null_aliases.deinit(alloc);
    }
    for (state.null_aliases) |alias| {
        if (!projection.contains(alias)) continue;
        try null_aliases.append(alloc, try alloc.dupe(u8, alias));
    }

    const owned_bindings = try bindings.toOwnedSlice(alloc);
    errdefer {
        for (owned_bindings) |*binding| binding.deinit(alloc);
        if (owned_bindings.len > 0) alloc.free(owned_bindings);
    }
    const owned_null_aliases = try null_aliases.toOwnedSlice(alloc);
    return .{
        .bindings = owned_bindings,
        .path = &.{},
        .null_aliases = owned_null_aliases,
        .retained_budget = returned_state_budget,
        .retained_state_bytes = retained_bytes,
    };
}

fn projectedConjunctiveStateRetainedBytes(
    state: ConjunctiveState,
    projection: *const AliasProjection,
) !usize {
    var total: usize = @sizeOf(PatternMatch);
    var binding_count: usize = 0;
    for (state.bindings) |binding| {
        if (!projection.contains(binding.alias)) continue;
        binding_count = try std.math.add(usize, binding_count, 1);
        total = try std.math.add(usize, total, binding.alias.len);
        total = try std.math.add(usize, total, binding.key.len);
        if (binding.table) |table| total = try std.math.add(usize, total, table.len);
    }
    total = try std.math.add(
        usize,
        total,
        try std.math.mul(usize, binding_count, @sizeOf(PatternBinding)),
    );
    var null_count: usize = 0;
    for (state.null_aliases) |alias| {
        if (!projection.contains(alias)) continue;
        null_count = try std.math.add(usize, null_count, 1);
        total = try std.math.add(usize, total, alias.len);
    }
    return try std.math.add(
        usize,
        total,
        try std.math.mul(usize, null_count, @sizeOf([]u8)),
    );
}

test "projected MATCH rows reserve and release retained output bytes" {
    const alloc = std.testing.allocator;
    var source_bindings = [_]PatternBinding{.{
        .alias = @constCast("person"),
        .key = @constCast("person:1"),
        .depth = 0,
    }};
    const state = ConjunctiveState{ .bindings = source_bindings[0..] };
    var projection = try AliasProjection.init(alloc, &.{});
    defer projection.deinit(alloc);

    var budget = WorkBudget.init(1, 1);
    budget.max_retained_state_bytes = 1;
    try std.testing.expectError(
        error.GraphWorkBudgetExceeded,
        projectConjunctiveState(alloc, state, &projection, &budget, &budget),
    );
    try std.testing.expectEqual(@as(usize, 0), budget.retained_state_bytes);

    budget.max_retained_state_bytes = default_max_retained_state_bytes;
    var result = try projectConjunctiveState(alloc, state, &projection, &budget, &budget);
    try std.testing.expect(budget.retained_state_bytes > 0);
    result.deinit(alloc);
    try std.testing.expectEqual(@as(usize, 0), budget.retained_state_bytes);
}

fn freeConjunctiveStates(alloc: Allocator, states: *std.ArrayListUnmanaged(ConjunctiveState)) void {
    for (states.items) |*state| state.deinit(alloc);
    states.deinit(alloc);
    states.* = .empty;
}

fn findBinding(bindings: []const PatternBinding, alias: []const u8) ?PatternBinding {
    for (bindings) |binding| {
        if (std.mem.eql(u8, binding.alias, alias)) return binding;
    }
    return null;
}

fn shouldReturnAlias(alias: []const u8, requested: []const []const u8) bool {
    if (requested.len == 0) return true;
    for (requested) |item| {
        if (std.mem.eql(u8, item, alias)) return true;
    }
    return false;
}

fn filterBindings(alloc: Allocator, bindings: []const PatternBinding, requested: []const []const u8) ![]PatternBinding {
    var count: usize = 0;
    for (bindings) |binding| {
        if (shouldReturnAlias(binding.alias, requested)) count += 1;
    }
    var filtered = try alloc.alloc(PatternBinding, count);
    var out_idx: usize = 0;
    errdefer {
        for (filtered[0..out_idx]) |*binding| binding.deinit(alloc);
        if (filtered.len > 0) alloc.free(filtered);
    }
    for (bindings) |binding| {
        if (!shouldReturnAlias(binding.alias, requested)) continue;
        filtered[out_idx] = try initPatternBinding(
            alloc,
            binding.alias,
            binding.key,
            binding.table,
            binding.depth,
        );
        out_idx += 1;
    }
    return filtered;
}

fn cloneBindings(alloc: Allocator, bindings: []const PatternBinding) ![]PatternBinding {
    var out = try alloc.alloc(PatternBinding, bindings.len);
    var initialized: usize = 0;
    errdefer {
        for (out[0..initialized]) |*binding| binding.deinit(alloc);
        if (out.len > 0) alloc.free(out);
    }
    for (bindings, 0..) |binding, i| {
        out[i] = try initPatternBinding(
            alloc,
            binding.alias,
            binding.key,
            binding.table,
            binding.depth,
        );
        initialized += 1;
    }
    return out;
}

fn initPatternBinding(
    alloc: Allocator,
    alias: []const u8,
    key: []const u8,
    table: ?[]const u8,
    depth: u32,
) !PatternBinding {
    const owned_alias = try alloc.dupe(u8, alias);
    errdefer alloc.free(owned_alias);
    const owned_key = try alloc.dupe(u8, key);
    errdefer alloc.free(owned_key);
    const owned_table = if (table) |table_name| try alloc.dupe(u8, table_name) else null;
    errdefer if (owned_table) |table_name| alloc.free(table_name);
    return .{
        .alias = owned_alias,
        .key = owned_key,
        .table = owned_table,
        .depth = depth,
    };
}

fn initFrontier(
    ancestry_store: *ReachabilityAncestry,
    key: []const u8,
    table: ?[]const u8,
    path: []paths_mod.PathEdge,
    parent: ?*const PathAncestry,
    hops: u32,
) !Frontier {
    const ancestry = try ancestry_store.append(key, table, parent);
    return .{
        .key = ancestry.node.key,
        .table = ancestry.node.table,
        .path = path,
        .ancestry = ancestry,
        .hops = hops,
    };
}

fn frontierContainsNode(frontier: Frontier, node: node_identity.Ref) bool {
    var ancestry: ?*const PathAncestry = frontier.ancestry;
    while (ancestry) |entry| : (ancestry = entry.parent) {
        if (optionalTableEql(entry.node.table, node.table) and
            std.mem.eql(u8, entry.node.key, node.key)) return true;
    }
    return false;
}

fn shouldRejectPathRevisit(
    revisits_path: bool,
    node: node_identity.Ref,
    target_nodes: []const node_identity.Ref,
    target_required: bool,
    fixed_single_hop: bool,
) bool {
    if (!revisits_path or fixed_single_hop) return false;
    return !(target_required and targetNodeMatches(node, target_nodes, true));
}

fn initReachableNode(
    alloc: Allocator,
    key: []const u8,
    table: ?[]const u8,
    depth: u32,
    path: []const paths_mod.PathEdge,
) !ReachableNode {
    const owned_key = try alloc.dupe(u8, key);
    errdefer alloc.free(owned_key);
    const owned_table = if (table) |table_name| try alloc.dupe(u8, table_name) else null;
    errdefer if (owned_table) |table_name| alloc.free(table_name);
    const owned_path = try clonePathEdges(alloc, path);
    errdefer freePathEdges(alloc, owned_path);
    return .{
        .key = owned_key,
        .table = owned_table,
        .depth = depth,
        .path = owned_path,
    };
}

fn optionalTableEql(left: ?[]const u8, right: ?[]const u8) bool {
    if ((left == null) != (right == null)) return false;
    if (left) |left_table| return std.mem.eql(u8, left_table, right.?);
    return true;
}

fn clonePathEdges(alloc: Allocator, edges: []const paths_mod.PathEdge) ![]paths_mod.PathEdge {
    var out = try alloc.alloc(paths_mod.PathEdge, edges.len);
    var initialized: usize = 0;
    errdefer {
        freePathEdgeItems(alloc, out[0..initialized]);
        if (out.len > 0) alloc.free(out);
    }
    for (edges, 0..) |edge, i| {
        out[i] = try dupePathEdge(alloc, edge.source, edge.target, edge.edge_type, edge.weight, edge.metadata, edge.traversal_direction);
        initialized += 1;
    }
    return out;
}

fn concatPathEdges(alloc: Allocator, left: []const paths_mod.PathEdge, right: []const paths_mod.PathEdge) ![]paths_mod.PathEdge {
    var out = try alloc.alloc(paths_mod.PathEdge, left.len + right.len);
    var initialized: usize = 0;
    errdefer {
        freePathEdgeItems(alloc, out[0..initialized]);
        if (out.len > 0) alloc.free(out);
    }
    for (left, 0..) |edge, i| {
        out[i] = try dupePathEdge(alloc, edge.source, edge.target, edge.edge_type, edge.weight, edge.metadata, edge.traversal_direction);
        initialized += 1;
    }
    for (right, 0..) |edge, i| {
        out[left.len + i] = try dupePathEdge(alloc, edge.source, edge.target, edge.edge_type, edge.weight, edge.metadata, edge.traversal_direction);
        initialized += 1;
    }
    return out;
}

fn appendPathEdge(
    alloc: Allocator,
    existing: []const paths_mod.PathEdge,
    edge: graph_mod.Edge,
    source: []const u8,
    target: []const u8,
    requested_direction: graph_mod.EdgeDirection,
) ![]paths_mod.PathEdge {
    var out = try alloc.alloc(paths_mod.PathEdge, existing.len + 1);
    var initialized: usize = 0;
    errdefer {
        freePathEdgeItems(alloc, out[0..initialized]);
        if (out.len > 0) alloc.free(out);
    }
    for (existing, 0..) |item, i| {
        out[i] = try dupePathEdge(alloc, item.source, item.target, item.edge_type, item.weight, item.metadata, item.traversal_direction);
        initialized += 1;
    }
    out[out.len - 1] = try dupePathEdge(
        alloc,
        edge.source,
        edge.target,
        edge.edge_type,
        edge.weight,
        edge.metadata,
        try traversedEdgeDirection(edge, source, target, requested_direction),
    );
    initialized += 1;
    return out;
}

fn traversedEdgeDirection(
    edge: graph_mod.Edge,
    source: []const u8,
    target: []const u8,
    requested_direction: graph_mod.EdgeDirection,
) !?graph_mod.EdgeDirection {
    const traversed_out = std.mem.eql(u8, edge.source, source) and
        std.mem.eql(u8, edge.target, target);
    const traversed_in = std.mem.eql(u8, edge.target, source) and
        std.mem.eql(u8, edge.source, target);
    return switch (requested_direction) {
        .out => if (traversed_out) .out else error.InvalidTraversedEdge,
        .in => if (traversed_in) .in else error.InvalidTraversedEdge,
        // Equal-key reciprocal edges can satisfy both comparisons. Keep the
        // direction unknown so the public boundary can resolve it from table
        // provenance and edge metadata instead of silently claiming `out`.
        .both => if (traversed_out == traversed_in)
            if (traversed_out) null else error.InvalidTraversedEdge
        else if (traversed_out)
            .out
        else
            .in,
    };
}

fn dupePathEdge(
    alloc: Allocator,
    source: []const u8,
    target: []const u8,
    edge_type: []const u8,
    weight: f64,
    metadata: []const u8,
    traversal_direction: ?graph_mod.EdgeDirection,
) !paths_mod.PathEdge {
    const owned_source = try alloc.dupe(u8, source);
    errdefer alloc.free(owned_source);
    const owned_target = try alloc.dupe(u8, target);
    errdefer alloc.free(owned_target);
    const owned_edge_type = try alloc.dupe(u8, edge_type);
    errdefer alloc.free(owned_edge_type);
    const owned_metadata = if (metadata.len > 0) try alloc.dupe(u8, metadata) else "";
    errdefer if (owned_metadata.len > 0) alloc.free(owned_metadata);
    return .{
        .source = owned_source,
        .target = owned_target,
        .edge_type = owned_edge_type,
        .weight = weight,
        .metadata = owned_metadata,
        .traversal_direction = traversal_direction,
    };
}

fn freePathEdges(alloc: Allocator, edges: []const paths_mod.PathEdge) void {
    freePathEdgeItems(alloc, edges);
    if (edges.len > 0) alloc.free(edges);
}

fn freePathEdgeItems(alloc: Allocator, edges: []const paths_mod.PathEdge) void {
    for (edges) |edge| {
        alloc.free(edge.source);
        alloc.free(edge.target);
        alloc.free(edge.edge_type);
        if (edge.metadata.len > 0) alloc.free(edge.metadata);
    }
}

test "conjunctive reverse expansion uses the declared cross-table source alias" {
    const alloc = std.testing.allocator;
    const Reader = struct {
        pub fn canonicalizeTable(_: @This(), table: ?[]const u8) ?[]const u8 {
            if (table) |name| if (std.mem.eql(u8, name, "docs")) return null;
            return table;
        }

        pub fn getEdgesBoundedForPattern(
            _: @This(),
            a: Allocator,
            table: ?[]const u8,
            key: []const u8,
            _: []const []const u8,
            direction: graph_mod.EdgeDirection,
            _: usize,
            _: usize,
            source_table_declared: bool,
        ) ![]graph_mod.Edge {
            const out = try a.alloc(graph_mod.Edge, 1);
            if (std.mem.eql(u8, key, "author")) {
                try std.testing.expect(table == null);
                try std.testing.expectEqual(graph_mod.EdgeDirection.out, direction);
                out[0] = .{ .source = "author", .target = "post", .edge_type = "AUTHORED", .weight = 1, .created_at = 0, .updated_at = 0, .metadata = "" };
                return out;
            }
            try std.testing.expectEqualStrings("post", key);
            try std.testing.expectEqualStrings("entities", table.?);
            try std.testing.expectEqual(graph_mod.EdgeDirection.in, direction);
            try std.testing.expect(source_table_declared);
            out[0] = .{ .source = "reply", .target = "post", .edge_type = "REPLIES_TO", .weight = 1, .created_at = 0, .updated_at = 0, .metadata = "{\"target_table\":\"docs\"}" };
            return out;
        }

        pub fn freeEdges(_: @This(), a: Allocator, edges: []graph_mod.Edge) void {
            a.free(edges);
        }
    };

    const nodes = [_]MatchNode{
        .{ .alias = "author" },
        .{ .alias = "post" },
        .{ .alias = "reply", .table = "entities" },
    };
    const edges = [_]MatchEdge{
        .{ .from = "author", .to = "post", .step = .{ .types = &.{"AUTHORED"} } },
        .{ .from = "reply", .to = "post", .step = .{ .types = &.{"REPLIES_TO"} } },
    };
    const matches = try matchConjunctivePatternWithEdgeReader(
        alloc,
        Reader{},
        &.{"author"},
        .{ .anchor_alias = "author", .nodes = &nodes, .edges = &edges },
        .{ .max_results = 0, .return_aliases = &.{"reply"} },
    );
    defer freeMatches(alloc, matches);
    try std.testing.expectEqual(@as(usize, 1), matches.len);
    try std.testing.expectEqualStrings("reply", matches[0].bindings[0].key);
    try std.testing.expectEqualStrings("entities", matches[0].bindings[0].table.?);
}

test "cross-table reverse variable expansion fails closed before reading adjacency" {
    const Reader = struct {
        pub fn getEdges(_: @This(), _: Allocator, _: ?[]const u8, _: []const u8, _: []const []const u8, _: graph_mod.EdgeDirection) ![]graph_mod.Edge {
            return error.TestUnexpectedResult;
        }

        pub fn freeEdges(_: @This(), _: Allocator, _: []graph_mod.Edge) void {}
    };
    var work_budget = WorkBudget.init(default_max_explored_nodes, default_max_explored_edges);
    try std.testing.expectError(
        error.GraphReverseVariablePathUnsupported,
        findReachableNodes(
            std.testing.allocator,
            Reader{},
            "bound-target",
            null,
            .{
                .step = .{
                    .types = &.{"LINKS"},
                    .direction = .in,
                    .min_hops = 1,
                    .max_hops = 2,
                },
                .incoming_source = .{ .table = "entities" },
            },
            .{},
            &.{},
            false,
            0,
            true,
            default_max_intermediate_states,
            null,
            null,
            false,
            null,
            &work_budget,
        ),
    );
}

test "cross-table both preflights every physical source before streaming" {
    const Reader = struct {
        pub fn validatePatternSourceTable(_: @This(), table: ?[]const u8, incoming: bool) !void {
            if (incoming) {
                try std.testing.expectEqualStrings("entities", table.?);
                return error.GraphExternalAliasSourceUnsupported;
            }
            try std.testing.expect(table == null);
        }
    };
    var storage: [2]PhysicalEdgeTraversal = undefined;
    try std.testing.expectError(
        error.GraphExternalAliasSourceUnsupported,
        planPhysicalEdgeTraversals(
            Reader{},
            .{ .direction = .both },
            true,
            null,
            "entities",
            &storage,
        ),
    );
}

test "conjunctive cross-table directions use physical source routing in every execution mode" {
    const alloc = std.testing.allocator;
    const Calls = struct {
        both_out: usize = 0,
        both_in: usize = 0,
    };
    const Reader = struct {
        calls: *Calls,

        pub fn canonicalizeTable(_: @This(), table: ?[]const u8) ?[]const u8 {
            if (table) |name| if (std.mem.eql(u8, name, "docs")) return null;
            return table;
        }

        pub fn getEdgesBoundedForPattern(
            self: @This(),
            a: Allocator,
            table: ?[]const u8,
            key: []const u8,
            types: []const []const u8,
            direction: graph_mod.EdgeDirection,
            _: usize,
            _: usize,
            source_table_declared: bool,
        ) ![]graph_mod.Edge {
            if (types.len != 1) return error.TestUnexpectedResult;
            const edge_type = types[0];
            var edge: graph_mod.Edge = undefined;
            if (std.mem.eql(u8, edge_type, "incoming")) {
                try std.testing.expectEqualStrings("entities", table.?);
                try std.testing.expectEqualStrings("post", key);
                try std.testing.expectEqual(graph_mod.EdgeDirection.in, direction);
                try std.testing.expect(source_table_declared);
                edge = .{ .source = "reply", .target = "post", .edge_type = "incoming", .weight = 1, .created_at = 0, .updated_at = 0, .metadata = "{\"target_table\":\"docs\"}" };
            } else if (std.mem.eql(u8, edge_type, "seed")) {
                try std.testing.expect(table == null);
                try std.testing.expectEqualStrings("root", key);
                try std.testing.expectEqual(graph_mod.EdgeDirection.out, direction);
                try std.testing.expect(!source_table_declared);
                edge = .{ .source = "root", .target = "entity", .edge_type = "seed", .weight = 1, .created_at = 0, .updated_at = 0, .metadata = "{\"target_table\":\"entities\"}" };
            } else if (std.mem.eql(u8, edge_type, "reverse_in")) {
                try std.testing.expectEqualStrings("entities", table.?);
                try std.testing.expectEqualStrings("entity", key);
                try std.testing.expectEqual(graph_mod.EdgeDirection.out, direction);
                try std.testing.expect(!source_table_declared);
                edge = .{ .source = "entity", .target = "local", .edge_type = "reverse_in", .weight = 1, .created_at = 0, .updated_at = 0, .metadata = "{\"target_table\":\"docs\"}" };
            } else if (std.mem.eql(u8, edge_type, "either")) {
                try std.testing.expectEqualStrings("post", key);
                switch (direction) {
                    .out => {
                        try std.testing.expect(table == null);
                        try std.testing.expect(!source_table_declared);
                        self.calls.both_out += 1;
                        edge = .{ .source = "post", .target = "sent", .edge_type = "either", .weight = 1, .created_at = 0, .updated_at = 0, .metadata = "{\"target_table\":\"entities\"}" };
                    },
                    .in => {
                        try std.testing.expectEqualStrings("entities", table.?);
                        try std.testing.expect(source_table_declared);
                        self.calls.both_in += 1;
                        edge = .{ .source = "received", .target = "post", .edge_type = "either", .weight = 1, .created_at = 0, .updated_at = 0, .metadata = "{\"target_table\":\"docs\"}" };
                    },
                    .both => return error.TestUnexpectedResult,
                }
            } else if (std.mem.eql(u8, edge_type, "bind")) {
                try std.testing.expect(table == null);
                try std.testing.expectEqualStrings("post", key);
                try std.testing.expectEqual(graph_mod.EdgeDirection.out, direction);
                edge = .{ .source = "post", .target = "reply", .edge_type = "bind", .weight = 1, .created_at = 0, .updated_at = 0, .metadata = "{\"target_table\":\"entities\"}" };
            } else if (std.mem.eql(u8, edge_type, "blocks")) {
                try std.testing.expectEqualStrings("entities", table.?);
                try std.testing.expectEqualStrings("post", key);
                try std.testing.expectEqual(graph_mod.EdgeDirection.in, direction);
                try std.testing.expect(source_table_declared);
                edge = .{ .source = "reply", .target = "post", .edge_type = "blocks", .weight = 1, .created_at = 0, .updated_at = 0, .metadata = "{\"target_table\":\"docs\"}" };
            } else return error.TestUnexpectedResult;

            const result = try a.alloc(graph_mod.Edge, 1);
            result[0] = edge;
            return result;
        }

        pub fn freeEdges(_: @This(), a: Allocator, edges: []graph_mod.Edge) void {
            a.free(edges);
        }
    };

    var calls = Calls{};
    const reader = Reader{ .calls = &calls };

    const incoming_nodes = [_]MatchNode{
        .{ .alias = "post" },
        .{ .alias = "reply", .table = "entities" },
    };
    const incoming_edges = [_]MatchEdge{.{
        .from = "post",
        .to = "reply",
        .step = .{ .direction = .in, .types = &.{"incoming"} },
    }};
    const incoming_pattern = ConjunctivePattern{
        .anchor_alias = "post",
        .nodes = &incoming_nodes,
        .edges = &incoming_edges,
    };
    const incoming_matches = try matchConjunctivePatternWithEdgeReader(
        alloc,
        reader,
        &.{"post"},
        incoming_pattern,
        .{ .max_results = 1, .return_aliases = &.{"reply"} },
    );
    defer freeMatches(alloc, incoming_matches);
    try std.testing.expectEqual(@as(usize, 1), incoming_matches.len);
    try std.testing.expectEqualStrings("reply", incoming_matches[0].bindings[0].key);
    try std.testing.expectEqualStrings("entities", incoming_matches[0].bindings[0].table.?);

    const count_specs = [_]CountAggregateSpec{.{}};
    const incoming_counts = try aggregateConjunctivePatternWithEdgeReader(
        alloc,
        reader,
        &.{"post"},
        incoming_pattern,
        &count_specs,
        .{},
    );
    defer {
        for (incoming_counts) |*result| result.deinit(alloc);
        alloc.free(incoming_counts);
    }
    try std.testing.expectEqual(@as(u128, 1), incoming_counts[0].value);

    const reverse_nodes = [_]MatchNode{
        .{ .alias = "root" },
        .{ .alias = "entity", .table = "entities" },
        .{ .alias = "local" },
    };
    const reverse_edges = [_]MatchEdge{
        .{ .from = "root", .to = "entity", .step = .{ .types = &.{"seed"} } },
        .{ .from = "local", .to = "entity", .step = .{ .direction = .in, .types = &.{"reverse_in"} } },
    };
    const reverse_matches = try matchConjunctivePatternWithEdgeReader(
        alloc,
        reader,
        &.{"root"},
        .{ .anchor_alias = "root", .nodes = &reverse_nodes, .edges = &reverse_edges },
        .{ .max_results = 1, .return_aliases = &.{"local"} },
    );
    defer freeMatches(alloc, reverse_matches);
    try std.testing.expectEqual(@as(usize, 1), reverse_matches.len);
    try std.testing.expectEqualStrings("local", reverse_matches[0].bindings[0].key);
    try std.testing.expect(reverse_matches[0].bindings[0].table == null);

    const both_edges = [_]MatchEdge{.{
        .from = "post",
        .to = "reply",
        .step = .{ .direction = .both, .types = &.{"either"} },
    }};
    const both_matches = try matchConjunctivePatternWithEdgeReader(
        alloc,
        reader,
        &.{"post"},
        .{ .anchor_alias = "post", .nodes = &incoming_nodes, .edges = &both_edges },
        .{ .max_results = 10, .return_aliases = &.{"reply"} },
    );
    defer freeMatches(alloc, both_matches);
    try std.testing.expectEqual(@as(usize, 2), both_matches.len);
    try std.testing.expectEqual(@as(usize, 1), calls.both_out);
    try std.testing.expectEqual(@as(usize, 1), calls.both_in);

    const required_edges = [_]MatchEdge{.{
        .from = "post",
        .to = "reply",
        .step = .{ .types = &.{"bind"} },
    }};
    const negative_edges = [_]MatchEdge{.{
        .from = "post",
        .to = "reply",
        .step = .{ .direction = .in, .types = &.{"blocks"} },
    }};
    const predicates = [_]MatchPredicate{.{ .not_exists = &negative_edges }};
    const anti_matches = try matchConjunctivePatternWithEdgeReader(
        alloc,
        reader,
        &.{"post"},
        .{ .anchor_alias = "post", .nodes = &incoming_nodes, .edges = &required_edges, .predicates = &predicates },
        .{ .max_results = 10 },
    );
    defer freeMatches(alloc, anti_matches);
    try std.testing.expectEqual(@as(usize, 0), anti_matches.len);
}

test "conjunctive fixed edges preserve self loops while variable paths remain node simple" {
    const alloc = std.testing.allocator;
    const Reader = struct {
        const stored = [_]graph_mod.Edge{.{
            .source = "same",
            .target = "same",
            .edge_type = "LOOP",
            .weight = 1,
            .created_at = 0,
            .updated_at = 0,
            .metadata = "",
        }};

        pub fn getEdges(_: @This(), _: Allocator, _: ?[]const u8, key: []const u8, types: []const []const u8, direction: graph_mod.EdgeDirection) ![]graph_mod.Edge {
            if (!std.mem.eql(u8, key, "same") or direction != .out) return @constCast((&[_]graph_mod.Edge{})[0..]);
            if (types.len > 0 and !containsString(types, "LOOP")) return @constCast((&[_]graph_mod.Edge{})[0..]);
            return @constCast(stored[0..]);
        }

        pub fn freeEdges(_: @This(), _: Allocator, _: []graph_mod.Edge) void {}
    };

    const nodes = [_]MatchNode{ .{ .alias = "a" }, .{ .alias = "b" } };
    const fixed_edges = [_]MatchEdge{.{ .from = "a", .to = "b", .step = .{ .types = &.{"LOOP"} } }};
    const fixed_pattern = ConjunctivePattern{ .anchor_alias = "a", .nodes = &nodes, .edges = &fixed_edges };

    const matches = try matchConjunctivePatternWithEdgeReader(
        alloc,
        Reader{},
        &.{"same"},
        fixed_pattern,
        .{ .max_results = 10 },
    );
    defer freeMatches(alloc, matches);
    try std.testing.expectEqual(@as(usize, 1), matches.len);
    try std.testing.expectEqualStrings("same", findBinding(matches[0].bindings, "a").?.key);
    try std.testing.expectEqualStrings("same", findBinding(matches[0].bindings, "b").?.key);

    const specs = [_]CountAggregateSpec{.{}};
    const aggregates = try aggregateConjunctivePatternWithEdgeReader(
        alloc,
        Reader{},
        &.{"same"},
        fixed_pattern,
        &specs,
        .{},
    );
    defer {
        for (aggregates) |*aggregate| aggregate.deinit(alloc);
        alloc.free(aggregates);
    }
    try std.testing.expectEqual(@as(u128, 1), aggregates[0].value);

    const variable_edges = [_]MatchEdge{.{
        .from = "a",
        .to = "b",
        .step = .{ .types = &.{"LOOP"}, .min_hops = 1, .max_hops = 2 },
    }};
    const variable_matches = try matchConjunctivePatternWithEdgeReader(
        alloc,
        Reader{},
        &.{"same"},
        .{ .anchor_alias = "a", .nodes = &nodes, .edges = &variable_edges },
        .{ .max_results = 10 },
    );
    defer freeMatches(alloc, variable_matches);
    try std.testing.expectEqual(@as(usize, 0), variable_matches.len);
}

test "conjunctive match supports branches anti joins inequality and optional nulls" {
    const alloc = std.testing.allocator;
    const Reader = struct {
        const stored = [_]graph_mod.Edge{
            .{ .source = "a", .target = "b", .edge_type = "KNOWS", .weight = 1, .created_at = 0, .updated_at = 0, .metadata = "" },
            .{ .source = "a", .target = "c", .edge_type = "KNOWS", .weight = 1, .created_at = 0, .updated_at = 0, .metadata = "" },
            .{ .source = "d", .target = "a", .edge_type = "LIKES", .weight = 1, .created_at = 0, .updated_at = 0, .metadata = "" },
            .{ .source = "e", .target = "d", .edge_type = "FOLLOWS", .weight = 1, .created_at = 0, .updated_at = 0, .metadata = "" },
            .{ .source = "x", .target = "y", .edge_type = "KNOWS", .weight = 1, .created_at = 0, .updated_at = 0, .metadata = "" },
            .{ .source = "x", .target = "z", .edge_type = "KNOWS", .weight = 1, .created_at = 0, .updated_at = 0, .metadata = "" },
        };

        pub fn getEdges(_: @This(), a: Allocator, _: ?[]const u8, key: []const u8, types: []const []const u8, direction: graph_mod.EdgeDirection) ![]graph_mod.Edge {
            var out = std.ArrayListUnmanaged(graph_mod.Edge).empty;
            defer out.deinit(a);
            for (stored) |edge| {
                const connected = switch (direction) {
                    .out => std.mem.eql(u8, edge.source, key),
                    .in => std.mem.eql(u8, edge.target, key),
                    .both => std.mem.eql(u8, edge.source, key) or std.mem.eql(u8, edge.target, key),
                };
                if (!connected) continue;
                if (types.len > 0 and !containsString(types, edge.edge_type)) continue;
                try out.append(a, edge);
            }
            return try out.toOwnedSlice(a);
        }

        pub fn freeEdges(_: @This(), a: Allocator, edges: []graph_mod.Edge) void {
            if (edges.len > 0) a.free(edges);
        }
    };

    const nodes = [_]MatchNode{ .{ .alias = "root" }, .{ .alias = "left" }, .{ .alias = "right" } };
    const required = [_]MatchEdge{
        .{ .from = "root", .to = "left", .step = .{ .types = &.{"KNOWS"} } },
        .{ .from = "root", .to = "right", .step = .{ .types = &.{"KNOWS"} } },
    };
    const missing = [_]MatchEdge{.{ .from = "left", .to = "right", .step = .{ .types = &.{"KNOWS"} } }};
    const predicates = [_]MatchPredicate{
        .{ .not_equal = .{ .left = "left", .right = "right" } },
        .{ .not_exists = &missing },
    };
    const optional_nodes = [_]MatchNode{.{ .alias = "liker" }};
    const optional_edges = [_]MatchEdge{.{ .from = "liker", .to = "root", .step = .{ .types = &.{"LIKES"} } }};
    const follower_nodes = [_]MatchNode{.{ .alias = "follower" }};
    const follower_edges = [_]MatchEdge{.{ .from = "follower", .to = "liker", .step = .{ .types = &.{"FOLLOWS"} } }};
    const optional = [_]OptionalPattern{
        .{ .nodes = &optional_nodes, .edges = &optional_edges },
        .{ .nodes = &follower_nodes, .edges = &follower_edges },
    };

    const matches = try matchConjunctivePatternWithEdgeReader(
        alloc,
        Reader{},
        &.{ "a", "x" },
        .{ .nodes = &nodes, .edges = &required, .predicates = &predicates, .optional = &optional },
        .{ .max_results = 0 },
    );
    defer freeMatches(alloc, matches);

    try std.testing.expectEqual(@as(usize, 4), matches.len);
    var saw_liker = false;
    var saw_follower = false;
    var saw_null = false;
    for (matches) |match| {
        saw_liker = saw_liker or findBinding(match.bindings, "liker") != null;
        saw_follower = saw_follower or findBinding(match.bindings, "follower") != null;
        saw_null = saw_null or containsString(match.null_aliases, "liker");
    }
    try std.testing.expect(saw_liker);
    try std.testing.expect(saw_follower);
    try std.testing.expect(saw_null);

    const specs = [_]CountAggregateSpec{
        .{},
        .{ .alias = "left", .distinct = true },
        .{ .alias = "liker" },
        .{ .alias = "liker", .distinct = true },
    };
    const aggregates = try aggregateConjunctivePatternWithEdgeReader(
        alloc,
        Reader{},
        &.{ "a", "x" },
        .{ .nodes = &nodes, .edges = &required, .predicates = &predicates, .optional = &optional },
        &specs,
        .{},
    );
    defer {
        for (aggregates) |*aggregate| aggregate.deinit(alloc);
        alloc.free(aggregates);
    }
    try std.testing.expectEqual(@as(u128, 4), aggregates[0].value);
    try std.testing.expectEqual(@as(u128, 4), aggregates[1].value);
    try std.testing.expectEqual(@as(u128, 2), aggregates[2].value);
    try std.testing.expectEqual(@as(u128, 1), aggregates[3].value);

    const streamed = try aggregateConjunctivePatternWithEdgeReader(
        alloc,
        Reader{},
        &.{ "a", "x" },
        .{ .nodes = &nodes, .edges = &required, .predicates = &predicates, .optional = &optional },
        &specs,
        .{ .max_intermediate_states = 1 },
    );
    defer {
        for (streamed) |*aggregate| aggregate.deinit(alloc);
        alloc.free(streamed);
    }
    try std.testing.expectEqual(@as(u128, 4), streamed[0].value);
    try std.testing.expectEqual(@as(u128, 4), streamed[1].value);
    try std.testing.expectEqual(@as(u128, 2), streamed[2].value);
    try std.testing.expectEqual(@as(u128, 1), streamed[3].value);

    var page_distinct_budget = DistinctBudget.init(
        default_max_distinct_identities,
        default_max_distinct_state_bytes,
    );
    var page_stream = try ConjunctiveCountAggregateStream.init(alloc, &specs, &page_distinct_budget);
    defer page_stream.deinit(alloc);
    const paged_pattern = ConjunctivePattern{
        .nodes = &nodes,
        .edges = &required,
        .predicates = &predicates,
        .optional = &optional,
    };
    try page_stream.consumePageWithEdgeReader(alloc, Reader{}, &.{"a"}, paged_pattern, .{});
    try page_stream.consumePageWithEdgeReader(alloc, Reader{}, &.{"x"}, paged_pattern, .{});
    const paged = try page_stream.finishAlloc(alloc);
    defer {
        for (paged) |*aggregate| aggregate.deinit(alloc);
        alloc.free(paged);
    }
    try std.testing.expectEqual(@as(u128, 4), paged[0].value);
    try std.testing.expectEqual(@as(u128, 4), paged[1].value);
    try std.testing.expectEqual(@as(u128, 2), paged[2].value);
    try std.testing.expectEqual(@as(u128, 1), paged[3].value);
}

test "serverless paged conjunctive aggregate preserves exact state across anchor pages" {
    const alloc = std.testing.allocator;
    const Reader = struct {
        pub fn getEdges(_: @This(), a: Allocator, _: ?[]const u8, _: []const u8, _: []const []const u8, _: graph_mod.EdgeDirection) ![]graph_mod.Edge {
            return try a.alloc(graph_mod.Edge, 0);
        }

        pub fn freeEdges(_: @This(), _: Allocator, _: []graph_mod.Edge) void {}
    };
    const nodes = [_]MatchNode{.{ .alias = "anchor" }};
    const pattern = ConjunctivePattern{
        .anchor_alias = "anchor",
        .nodes = &nodes,
        .edges = &.{},
    };
    const specs = [_]CountAggregateSpec{ .{}, .{ .alias = "anchor", .distinct = true } };
    var distinct_budget = DistinctBudget.init(default_max_distinct_identities, default_max_distinct_state_bytes);
    var stream = try ConjunctiveCountAggregateStream.init(alloc, &specs, &distinct_budget);
    defer stream.deinit(alloc);
    try stream.consumePageWithEdgeReader(alloc, Reader{}, &.{ "a", "b" }, pattern, .{ .start_validation = .prevalidated });
    try stream.consumePageWithEdgeReader(alloc, Reader{}, &.{"c"}, pattern, .{ .start_validation = .prevalidated });
    const results = try stream.finishAlloc(alloc);
    defer {
        for (results) |*result| result.deinit(alloc);
        alloc.free(results);
    }
    try std.testing.expectEqual(@as(u128, 3), results[0].value);
    try std.testing.expectEqual(@as(u128, 3), results[1].value);
}

test "anchor-only aggregate fails closed at the shared anchor scan ceiling" {
    const alloc = std.testing.allocator;
    const Reader = struct {
        pub fn getEdges(_: @This(), a: Allocator, _: ?[]const u8, _: []const u8, _: []const []const u8, _: graph_mod.EdgeDirection) ![]graph_mod.Edge {
            return try a.alloc(graph_mod.Edge, 0);
        }

        pub fn freeEdges(_: @This(), _: Allocator, _: []graph_mod.Edge) void {}
    };
    const nodes = [_]MatchNode{.{ .alias = "anchor" }};
    const pattern = ConjunctivePattern{
        .anchor_alias = "anchor",
        .nodes = &nodes,
        .edges = &.{},
    };
    const specs = [_]CountAggregateSpec{.{}};
    var distinct_budget = DistinctBudget.init(default_max_distinct_identities, default_max_distinct_state_bytes);
    var stream = try ConjunctiveCountAggregateStream.init(alloc, &specs, &distinct_budget);
    defer stream.deinit(alloc);
    var work_budget = WorkBudget.init(default_max_explored_nodes, default_max_explored_edges);
    work_budget.max_anchors = 1;
    work_budget.remaining_anchors = 1;

    try std.testing.expectError(
        error.GraphWorkBudgetExceeded,
        stream.consumePageWithEdgeReader(
            alloc,
            Reader{},
            &.{ "a", "b" },
            pattern,
            .{ .start_validation = .prevalidated, .work_budget = &work_budget },
        ),
    );
    try std.testing.expectEqual(work_budget_diagnostic.Dimension.scanned_anchors, work_budget.exhaustion().?.dimension);
}

test "conjunctive optional predicates reject aliases outside their ordered scope" {
    const base_nodes = [_]MatchNode{.{ .alias = "root" }};
    const optional_nodes = [_]MatchNode{.{ .alias = "related" }};
    const optional_edges = [_]MatchEdge{.{ .from = "root", .to = "related" }};
    const predicates = [_]MatchPredicate{.{ .not_equal = .{ .left = "related", .right = "later" } }};
    const optional = [_]OptionalPattern{.{
        .nodes = &optional_nodes,
        .edges = &optional_edges,
        .predicates = &predicates,
    }};

    try std.testing.expectError(error.InvalidArgument, validateConjunctivePattern(.{
        .nodes = &base_nodes,
        .edges = &.{},
        .optional = &optional,
    }));
}

test "conjunctive validation rejects disconnected and unused aliases" {
    const disconnected_nodes = [_]MatchNode{ .{ .alias = "a" }, .{ .alias = "b" }, .{ .alias = "unused" } };
    const disconnected_edges = [_]MatchEdge{.{ .from = "a", .to = "b" }};
    try std.testing.expectError(error.InvalidArgument, validateConjunctivePattern(.{
        .nodes = &disconnected_nodes,
        .edges = &disconnected_edges,
    }));

    const base_nodes = [_]MatchNode{.{ .alias = "a" }};
    const optional_nodes = [_]MatchNode{ .{ .alias = "b" }, .{ .alias = "unused" } };
    const optional_edges = [_]MatchEdge{.{ .from = "a", .to = "b" }};
    const optional = [_]OptionalPattern{.{ .nodes = &optional_nodes, .edges = &optional_edges }};
    try std.testing.expectError(error.InvalidArgument, validateConjunctivePattern(.{
        .nodes = &base_nodes,
        .edges = &.{},
        .optional = &optional,
    }));

    const oversized_alias = "a" ** (max_identifier_bytes + 1);
    const oversized_nodes = [_]MatchNode{.{ .alias = oversized_alias }};
    try std.testing.expectError(error.InvalidArgument, validateConjunctivePattern(.{
        .anchor_alias = oversized_alias,
        .nodes = &oversized_nodes,
        .edges = &.{},
    }));
}

test "conjunctive validation bounds total recursive pattern shape" {
    const nodes = [_]MatchNode{ .{ .alias = "a" }, .{ .alias = "b" } };
    const edge = MatchEdge{ .from = "a", .to = "b" };
    const too_many_edges = [_]MatchEdge{edge} ** (max_conjunctive_edges + 1);
    try std.testing.expectError(error.InvalidArgument, validateConjunctivePattern(.{
        .nodes = &nodes,
        .edges = &too_many_edges,
    }));

    const base_nodes = [_]MatchNode{.{ .alias = "root" }};
    const optional_node = [_]MatchNode{.{ .alias = "child" }};
    const optional_edge = [_]MatchEdge{.{ .from = "root", .to = "child" }};
    const optional_group = OptionalPattern{ .nodes = &optional_node, .edges = &optional_edge };
    const too_many_optional = [_]OptionalPattern{optional_group} ** (max_optional_patterns + 1);
    try std.testing.expectError(error.InvalidArgument, validateConjunctivePattern(.{
        .nodes = &base_nodes,
        .edges = &.{},
        .optional = &too_many_optional,
    }));
}

test "exact conjunctive aggregate does not inherit row expansion window" {
    const alloc = std.testing.allocator;
    var targets: [1001][4]u8 = undefined;
    for (&targets, 0..) |*target, i| std.mem.writeInt(u32, target, @intCast(i), .little);

    const Reader = struct {
        targets: []const [4]u8,

        pub fn getEdges(self: @This(), a: Allocator, _: ?[]const u8, key: []const u8, _: []const []const u8, _: graph_mod.EdgeDirection) ![]graph_mod.Edge {
            if (!std.mem.eql(u8, key, "anchor")) return try a.alloc(graph_mod.Edge, 0);
            const edges = try a.alloc(graph_mod.Edge, self.targets.len);
            for (edges, self.targets) |*edge, *target| edge.* = .{
                .source = "anchor",
                .target = target,
                .edge_type = "related",
                .weight = 1,
                .created_at = 0,
                .updated_at = 0,
                .metadata = "",
            };
            return edges;
        }

        pub fn freeEdges(_: @This(), a: Allocator, edges: []graph_mod.Edge) void {
            if (edges.len > 0) a.free(edges);
        }
    };

    const nodes = [_]MatchNode{ .{ .alias = "a" }, .{ .alias = "b" } };
    const edges = [_]MatchEdge{.{ .from = "a", .to = "b" }};
    const specs = [_]CountAggregateSpec{.{}};
    const aggregates = try aggregateConjunctivePatternWithEdgeReader(
        alloc,
        Reader{ .targets = &targets },
        &.{"anchor"},
        .{ .nodes = &nodes, .edges = &edges },
        &specs,
        .{ .max_explored_edges = 2_000, .max_intermediate_states = 2_000 },
    );
    defer {
        for (aggregates) |*aggregate| aggregate.deinit(alloc);
        alloc.free(aggregates);
    }
    try std.testing.expectEqual(@as(u128, targets.len), aggregates[0].value);

    const too_many_specs = [_]CountAggregateSpec{.{}} ** (max_count_aggregates + 1);
    try std.testing.expectError(error.InvalidArgument, aggregateConjunctivePatternWithEdgeReader(
        alloc,
        Reader{ .targets = &targets },
        &.{"anchor"},
        .{ .nodes = &nodes, .edges = &edges },
        &too_many_specs,
        .{ .max_explored_edges = 2_000, .max_intermediate_states = 2_000 },
    ));

    const duplicate_specs = [_]CountAggregateSpec{
        .{ .alias = "b", .distinct = true },
        .{ .alias = "b", .distinct = true },
    };
    var duplicate_plan = try CountAggregatePlan.init(alloc, &duplicate_specs);
    defer duplicate_plan.deinit(alloc);
    try std.testing.expectEqual(@as(usize, 1), duplicate_plan.unique_specs.len);
    try std.testing.expectEqualSlices(usize, &.{ 0, 0 }, duplicate_plan.output_indexes);

    const duplicates = try aggregateConjunctivePatternWithEdgeReader(
        alloc,
        Reader{ .targets = &targets },
        &.{"anchor"},
        .{ .nodes = &nodes, .edges = &edges },
        &duplicate_specs,
        .{ .max_explored_edges = 2_000, .max_intermediate_states = 2_000 },
    );
    defer {
        for (duplicates) |*aggregate| aggregate.deinit(alloc);
        alloc.free(duplicates);
    }
    try std.testing.expectEqual(@as(usize, 2), duplicates.len);
    try std.testing.expectEqual(duplicates[0].value, duplicates[1].value);
}

test "conjunctive matcher admits anchors before alias evaluation" {
    const Reader = struct {
        pub fn getEdges(_: @This(), a: Allocator, _: ?[]const u8, _: []const u8, _: []const []const u8, _: graph_mod.EdgeDirection) ![]graph_mod.Edge {
            return try a.alloc(graph_mod.Edge, 0);
        }

        pub fn freeEdges(_: @This(), a: Allocator, edges: []graph_mod.Edge) void {
            a.free(edges);
        }
    };
    const Admission = struct {
        fn filter(_: ?*anyopaque, alloc: Allocator, nodes: []const NodeRef) anyerror![]bool {
            const mask = try alloc.alloc(bool, nodes.len);
            for (nodes, 0..) |node, i| mask[i] = std.mem.eql(u8, node.key, "allowed");
            return mask;
        }
    };
    const nodes = [_]MatchNode{.{ .alias = "a" }};
    const matches = try matchConjunctivePatternWithEdgeReader(
        std.testing.allocator,
        Reader{},
        &.{ "denied", "allowed" },
        .{ .nodes = &nodes, .edges = &.{} },
        .{ .node_admission = .{ .ctx = null, .filter_many = Admission.filter } },
    );
    defer freeMatches(std.testing.allocator, matches);

    try std.testing.expectEqual(@as(usize, 1), matches.len);
    try std.testing.expectEqualStrings("allowed", matches[0].bindings[0].key);
}

test "prevalidated conjunctive anchors skip duplicate checks but reached nodes remain guarded" {
    const Reader = struct {
        const edge = [_]graph_mod.Edge{.{
            .source = "anchor",
            .target = "reached",
            .edge_type = "links",
            .weight = 1,
            .created_at = 0,
            .updated_at = 0,
            .metadata = "",
        }};

        pub fn getEdges(_: @This(), _: Allocator, _: ?[]const u8, key: []const u8, _: []const []const u8, _: graph_mod.EdgeDirection) ![]graph_mod.Edge {
            if (std.mem.eql(u8, key, "anchor")) return @constCast(edge[0..]);
            return @constCast((&[_]graph_mod.Edge{})[0..]);
        }

        pub fn freeEdges(_: @This(), _: Allocator, _: []graph_mod.Edge) void {}
    };
    const Checks = struct {
        admission_nodes: usize = 0,
        filter_nodes: usize = 0,

        fn admit(ctx: ?*anyopaque, alloc: Allocator, refs: []const NodeRef) anyerror![]bool {
            const self: *@This() = @ptrCast(@alignCast(ctx orelse return error.InvalidArgument));
            self.admission_nodes += refs.len;
            const mask = try alloc.alloc(bool, refs.len);
            @memset(mask, true);
            return mask;
        }

        fn filter(ctx: ?*anyopaque, alloc: Allocator, refs: []const node_identity.Ref, _: NodeFilter) anyerror![]bool {
            const self: *@This() = @ptrCast(@alignCast(ctx orelse return error.InvalidArgument));
            self.filter_nodes += refs.len;
            const mask = try alloc.alloc(bool, refs.len);
            for (refs, 0..) |ref, i| mask[i] = !std.mem.eql(u8, ref.key, "reached");
            return mask;
        }
    };
    const nodes = [_]MatchNode{
        .{ .alias = "a", .filter = .{ .filter_query_json = "{}" } },
        .{ .alias = "b", .filter = .{ .filter_query_json = "{}" } },
    };
    const edges = [_]MatchEdge{.{ .from = "a", .to = "b", .step = .{ .types = &.{"links"} } }};
    var checks = Checks{};
    const opts = MatchOptions{
        .node_admission = .{ .ctx = &checks, .filter_many = Checks.admit },
        .evaluator = .{ .ctx = &checks, .batch_func = Checks.filter },
        .start_validation = .prevalidated,
    };

    const matches = try matchConjunctivePatternWithEdgeReader(
        std.testing.allocator,
        Reader{},
        &.{"anchor"},
        .{ .anchor_alias = "a", .nodes = &nodes, .edges = &edges },
        opts,
    );
    defer freeMatches(std.testing.allocator, matches);
    try std.testing.expectEqual(@as(usize, 0), matches.len);
    try std.testing.expectEqual(@as(usize, 1), checks.admission_nodes);
    try std.testing.expectEqual(@as(usize, 1), checks.filter_nodes);

    checks = .{};
    const specs = [_]CountAggregateSpec{.{}};
    const aggregates = try aggregateConjunctivePatternWithEdgeReader(
        std.testing.allocator,
        Reader{},
        &.{"anchor"},
        .{ .anchor_alias = "a", .nodes = &nodes, .edges = &edges },
        &specs,
        opts,
    );
    defer {
        for (aggregates) |*aggregate| aggregate.deinit(std.testing.allocator);
        std.testing.allocator.free(aggregates);
    }
    try std.testing.expectEqual(@as(u128, 0), aggregates[0].value);
    try std.testing.expectEqual(@as(usize, 1), checks.admission_nodes);
    try std.testing.expectEqual(@as(usize, 1), checks.filter_nodes);
}

test "exact distinct aggregates share a fail-closed identity and byte budget" {
    const Reader = struct {
        pub fn getEdges(_: @This(), a: Allocator, _: ?[]const u8, _: []const u8, _: []const []const u8, _: graph_mod.EdgeDirection) ![]graph_mod.Edge {
            return try a.alloc(graph_mod.Edge, 0);
        }

        pub fn freeEdges(_: @This(), a: Allocator, edges: []graph_mod.Edge) void {
            a.free(edges);
        }
    };
    const nodes = [_]MatchNode{.{ .alias = "a" }};
    const specs = [_]CountAggregateSpec{
        .{ .alias = "a", .distinct = true },
        .{ .alias = "a", .distinct = true },
    };

    var identity_budget = DistinctBudget.init(1, 1024);
    try std.testing.expectError(
        error.GraphDistinctBudgetExceeded,
        aggregateConjunctivePatternWithEdgeReader(
            std.testing.allocator,
            Reader{},
            &.{ "one", "two" },
            .{ .anchor_alias = "a", .nodes = &nodes, .edges = &.{} },
            &specs,
            .{ .distinct_budget = &identity_budget },
        ),
    );

    // Repeated identities are retained once per distinct set, but separate
    // aggregate specs share the request budget because each owns a set.
    var shared_budget = DistinctBudget.init(2, 2048);
    const repeated = try aggregateConjunctivePatternWithEdgeReader(
        std.testing.allocator,
        Reader{},
        &.{ "same", "same" },
        .{ .anchor_alias = "a", .nodes = &nodes, .edges = &.{} },
        &specs,
        .{ .distinct_budget = &shared_budget },
    );
    defer {
        for (repeated) |*aggregate| aggregate.deinit(std.testing.allocator);
        std.testing.allocator.free(repeated);
    }
    try std.testing.expectEqual(@as(u128, 1), repeated[0].value);
    try std.testing.expectEqual(@as(u128, 1), repeated[1].value);
    try std.testing.expectEqual(@as(usize, 0), shared_budget.remaining_identities);
    try std.testing.expect(shared_budget.remaining_state_bytes < 2048 - 8);

    var byte_budget = DistinctBudget.init(10, 3);
    try std.testing.expectError(
        error.GraphDistinctBudgetExceeded,
        aggregateConjunctivePatternWithEdgeReader(
            std.testing.allocator,
            Reader{},
            &.{"four"},
            .{ .anchor_alias = "a", .nodes = &nodes, .edges = &.{} },
            specs[0..1],
            .{ .distinct_budget = &byte_budget },
        ),
    );
}

test "conjunctive anchor selection prefers filters and ignores declaration order" {
    const first = [_]MatchNode{
        .{ .alias = "z" },
        .{ .alias = "b", .filter = .{ .filter_query_json = "{\"ids\":[\"b\"]}" } },
        .{ .alias = "a", .filter = .{ .filter_query_json = "{\"ids\":[\"a\"]}" } },
    };
    const second = [_]MatchNode{
        first[2],
        first[0],
        first[1],
    };
    try std.testing.expectEqualStrings("a", selectConjunctiveAnchor(.{ .nodes = &first, .edges = &.{} }).?.alias);
    try std.testing.expectEqualStrings("a", selectConjunctiveAnchor(.{ .nodes = &second, .edges = &.{} }).?.alias);
    try std.testing.expectEqualStrings("z", selectConjunctiveAnchor(.{
        .anchor_alias = "z",
        .nodes = &first,
        .edges = &.{},
    }).?.alias);
}

test "bounded conjunctive matches stream complete rows before the intermediate-state budget" {
    const Reader = struct {
        const from_anchor = [_]graph_mod.Edge{
            .{ .source = "anchor", .target = "dead-1", .edge_type = "first", .weight = 1, .created_at = 0, .updated_at = 0, .metadata = "" },
            .{ .source = "anchor", .target = "dead-2", .edge_type = "first", .weight = 1, .created_at = 0, .updated_at = 0, .metadata = "" },
            .{ .source = "anchor", .target = "live", .edge_type = "first", .weight = 1, .created_at = 0, .updated_at = 0, .metadata = "" },
        };
        const from_live = [_]graph_mod.Edge{
            .{ .source = "live", .target = "result", .edge_type = "second", .weight = 1, .created_at = 0, .updated_at = 0, .metadata = "" },
        };

        pub fn getEdges(_: @This(), _: Allocator, _: ?[]const u8, key: []const u8, types: []const []const u8, _: graph_mod.EdgeDirection) ![]graph_mod.Edge {
            if (std.mem.eql(u8, key, "anchor") and types.len == 1 and std.mem.eql(u8, types[0], "first"))
                return @constCast(from_anchor[0..]);
            if (std.mem.eql(u8, key, "live") and types.len == 1 and std.mem.eql(u8, types[0], "second"))
                return @constCast(from_live[0..]);
            return @constCast((&[_]graph_mod.Edge{})[0..]);
        }

        pub fn freeEdges(_: @This(), _: Allocator, _: []graph_mod.Edge) void {}
    };
    const nodes = [_]MatchNode{
        .{ .alias = "a" },
        .{ .alias = "b" },
        .{ .alias = "c" },
    };
    const edges = [_]MatchEdge{
        .{ .from = "a", .to = "b", .step = .{ .types = &.{"first"} } },
        .{ .from = "b", .to = "c", .step = .{ .types = &.{"second"} } },
    };

    const matches = try matchConjunctivePatternWithEdgeReader(
        std.testing.allocator,
        Reader{},
        &.{"anchor"},
        .{ .anchor_alias = "a", .nodes = &nodes, .edges = &edges },
        .{
            .max_results = 1,
            .max_intermediate_states = 1,
            .max_explored_nodes = 32,
            .max_explored_edges = 32,
        },
    );
    defer freeMatches(std.testing.allocator, matches);

    try std.testing.expectEqual(@as(usize, 1), matches.len);
    try std.testing.expectEqualStrings("live", findBinding(matches[0].bindings, "b").?.key);
    try std.testing.expectEqualStrings("result", findBinding(matches[0].bindings, "c").?.key);
}

test "variable length conjunctive edge preserves simple path multiplicity" {
    const Reader = struct {
        const from_a = [_]graph_mod.Edge{
            .{ .source = "a", .target = "b", .edge_type = "links", .weight = 1, .created_at = 0, .updated_at = 0, .metadata = "" },
            .{ .source = "a", .target = "c", .edge_type = "links", .weight = 1, .created_at = 0, .updated_at = 0, .metadata = "" },
        };
        const from_b = [_]graph_mod.Edge{.{ .source = "b", .target = "d", .edge_type = "links", .weight = 1, .created_at = 0, .updated_at = 0, .metadata = "" }};
        const from_c = [_]graph_mod.Edge{.{ .source = "c", .target = "d", .edge_type = "links", .weight = 1, .created_at = 0, .updated_at = 0, .metadata = "" }};

        pub fn getEdges(_: @This(), _: Allocator, _: ?[]const u8, key: []const u8, _: []const []const u8, _: graph_mod.EdgeDirection) ![]graph_mod.Edge {
            if (std.mem.eql(u8, key, "a")) return @constCast(from_a[0..]);
            if (std.mem.eql(u8, key, "b")) return @constCast(from_b[0..]);
            if (std.mem.eql(u8, key, "c")) return @constCast(from_c[0..]);
            return @constCast((&[_]graph_mod.Edge{})[0..]);
        }

        pub fn freeEdges(_: @This(), _: Allocator, _: []graph_mod.Edge) void {}
    };
    const nodes = [_]MatchNode{ .{ .alias = "a" }, .{ .alias = "d" } };
    const edges = [_]MatchEdge{.{
        .from = "a",
        .to = "d",
        .step = .{ .types = &.{"links"}, .min_hops = 2, .max_hops = 2 },
    }};
    const matches = try matchConjunctivePatternWithEdgeReader(
        std.testing.allocator,
        Reader{},
        &.{"a"},
        .{ .nodes = &nodes, .edges = &edges },
        .{ .max_results = 10 },
    );
    defer freeMatches(std.testing.allocator, matches);

    try std.testing.expectEqual(@as(usize, 2), matches.len);
    try std.testing.expectEqualStrings("d", matches[0].bindings[1].key);
    try std.testing.expectEqualStrings("d", matches[1].bindings[1].key);
}

test "conjunctive cycle closure survives node admission deduplication" {
    const Reader = struct {
        const edges = [_]graph_mod.Edge{
            .{ .source = "a", .target = "b", .edge_type = "links", .weight = 1, .created_at = 0, .updated_at = 0, .metadata = "" },
            .{ .source = "b", .target = "c", .edge_type = "links", .weight = 1, .created_at = 0, .updated_at = 0, .metadata = "" },
            .{ .source = "c", .target = "a", .edge_type = "links", .weight = 1, .created_at = 0, .updated_at = 0, .metadata = "" },
        };

        pub fn getEdges(_: @This(), a: Allocator, _: ?[]const u8, key: []const u8, _: []const []const u8, _: graph_mod.EdgeDirection) ![]graph_mod.Edge {
            var out = std.ArrayListUnmanaged(graph_mod.Edge).empty;
            errdefer out.deinit(a);
            for (edges) |edge| if (std.mem.eql(u8, edge.source, key)) try out.append(a, edge);
            return try out.toOwnedSlice(a);
        }

        pub fn freeEdges(_: @This(), a: Allocator, edges_value: []graph_mod.Edge) void {
            if (edges_value.len > 0) a.free(edges_value);
        }
    };
    const Admission = struct {
        fn filter(_: ?*anyopaque, alloc: Allocator, nodes_value: []const NodeRef) anyerror![]bool {
            const mask = try alloc.alloc(bool, nodes_value.len);
            @memset(mask, true);
            return mask;
        }
    };
    const nodes = [_]MatchNode{.{ .alias = "x" }};
    const edges = [_]MatchEdge{.{
        .from = "x",
        .to = "x",
        .step = .{ .types = &.{"links"}, .min_hops = 3, .max_hops = 3 },
    }};
    const matches = try matchConjunctivePatternWithEdgeReader(
        std.testing.allocator,
        Reader{},
        &.{"a"},
        .{ .nodes = &nodes, .edges = &edges },
        .{ .node_admission = .{ .ctx = null, .filter_many = Admission.filter } },
    );
    defer freeMatches(std.testing.allocator, matches);

    try std.testing.expectEqual(@as(usize, 1), matches.len);
    try std.testing.expectEqualStrings("a", matches[0].bindings[0].key);
}

test "exact two-edge pattern uses typed batch probes without paths" {
    const Reader = struct {
        forward_calls: *usize,
        reverse_calls: *usize,
        max_probe_bytes: *usize,

        const forward = [_]graph_mod.Edge{
            .{ .source = "forum", .target = "post:1", .edge_type = "CONTAINER_OF", .weight = 1, .created_at = 0, .updated_at = 0, .metadata = "" },
            .{ .source = "forum", .target = "post:2", .edge_type = "CONTAINER_OF", .weight = 1, .created_at = 0, .updated_at = 0, .metadata = "" },
        };
        const backward = [_]graph_mod.Edge{
            .{ .source = "post:2", .target = "tag", .edge_type = "HAS_TAG", .weight = 1, .created_at = 0, .updated_at = 0, .metadata = "" },
            .{ .source = "post:3", .target = "tag", .edge_type = "HAS_TAG", .weight = 1, .created_at = 0, .updated_at = 0, .metadata = "" },
        };

        pub fn getEdges(
            self: @This(),
            _: Allocator,
            _: ?[]const u8,
            key: []const u8,
            edge_types: []const []const u8,
            direction: graph_mod.EdgeDirection,
        ) ![]graph_mod.Edge {
            try std.testing.expectEqual(@as(usize, 1), edge_types.len);
            if (std.mem.eql(u8, key, "forum")) {
                try std.testing.expectEqual(graph_mod.EdgeDirection.out, direction);
                try std.testing.expectEqualStrings("CONTAINER_OF", edge_types[0]);
                self.forward_calls.* += 1;
                return @constCast(forward[0..]);
            }
            if (std.mem.eql(u8, key, "tag")) {
                try std.testing.expectEqual(graph_mod.EdgeDirection.in, direction);
                try std.testing.expectEqualStrings("HAS_TAG", edge_types[0]);
                self.reverse_calls.* += 1;
                return @constCast(backward[0..]);
            }
            return error.TestUnexpectedResult;
        }

        pub fn freeEdges(_: @This(), _: Allocator, _: []graph_mod.Edge) void {}

        pub fn probeEdgesBounded(
            self: @This(),
            alloc: Allocator,
            _: ?[]const u8,
            probes: []const graph_mod.EdgeProbe,
            max_owned_bytes: usize,
        ) ![]?graph_mod.Edge {
            self.reverse_calls.* += 1;
            self.max_probe_bytes.* = max_owned_bytes;
            const results = try alloc.alloc(?graph_mod.Edge, probes.len);
            @memset(results, null);
            for (probes, 0..) |probe, i| {
                if (std.mem.eql(u8, probe.source, "post:2") and
                    std.mem.eql(u8, probe.target, "tag") and
                    std.mem.eql(u8, probe.edge_type, "HAS_TAG"))
                {
                    results[i] = backward[0];
                }
            }
            return results;
        }

        pub fn freeProbedEdges(_: @This(), alloc: Allocator, edges: []?graph_mod.Edge) void {
            alloc.free(edges);
        }
    };

    var forward_calls: usize = 0;
    var reverse_calls: usize = 0;
    var max_probe_bytes: usize = 0;
    var stats = MatchStats{};
    const matches = try matchPatternWithEdgeReader(
        std.testing.allocator,
        Reader{
            .forward_calls = &forward_calls,
            .reverse_calls = &reverse_calls,
            .max_probe_bytes = &max_probe_bytes,
        },
        &.{"forum"},
        &.{
            .{ .alias = "forum" },
            .{ .alias = "post", .edge = .{ .types = &.{"CONTAINER_OF"} } },
            .{ .alias = "tag", .edge = .{ .types = &.{"HAS_TAG"} } },
        },
        .{
            .target_nodes = &.{.{ .table = null, .key = "tag" }},
            .target_required = true,
            .include_paths = false,
            .stats = &stats,
        },
    );
    defer freeMatches(std.testing.allocator, matches);

    try std.testing.expectEqual(@as(usize, 1), forward_calls);
    try std.testing.expectEqual(@as(usize, 1), reverse_calls);
    try std.testing.expectEqual(
        default_max_explored_edge_bytes - try edgeOwnedBytes(&Reader.forward),
        max_probe_bytes,
    );
    try std.testing.expectEqual(@as(usize, 1), matches.len);
    try std.testing.expectEqual(MatchPlan.exact_two_edge_probe, stats.plan);
    try std.testing.expectEqual(@as(usize, 2), stats.exact_edge_probes);
    try std.testing.expectEqual(@as(usize, 0), matches[0].path.len);
    try std.testing.expectEqualStrings("post:2", matches[0].bindings[1].key);
    try std.testing.expectEqualStrings("tag", matches[0].bindings[2].key);
}

test "exact two-edge probe plan is equivalent to generic expansion" {
    const GenericReader = struct {
        const forward = [_]graph_mod.Edge{
            .{ .source = "forum", .target = "post:1", .edge_type = "CONTAINER_OF", .weight = 1, .created_at = 1, .updated_at = 2, .metadata = "{\"first\":1}" },
            .{ .source = "forum", .target = "post:2", .edge_type = "CONTAINER_OF", .weight = 1, .created_at = 1, .updated_at = 2, .metadata = "{\"first\":2}" },
        };
        const post_one = [_]graph_mod.Edge{
            .{ .source = "post:1", .target = "tag", .edge_type = "HAS_TAG", .weight = 0.5, .created_at = 3, .updated_at = 4, .metadata = "{\"second\":1}" },
        };
        const post_two = [_]graph_mod.Edge{
            .{ .source = "post:2", .target = "other", .edge_type = "HAS_TAG", .weight = 1, .created_at = 3, .updated_at = 4, .metadata = "" },
            .{ .source = "post:2", .target = "tag", .edge_type = "HAS_TAG", .weight = 0.8, .created_at = 3, .updated_at = 4, .metadata = "{\"second\":2}" },
        };

        pub fn getEdges(_: @This(), _: Allocator, _: ?[]const u8, key: []const u8, _: []const []const u8, _: graph_mod.EdgeDirection) ![]graph_mod.Edge {
            if (std.mem.eql(u8, key, "forum")) return @constCast(forward[0..]);
            if (std.mem.eql(u8, key, "post:1")) return @constCast(post_one[0..]);
            if (std.mem.eql(u8, key, "post:2")) return @constCast(post_two[0..]);
            return @constCast((&[_]graph_mod.Edge{})[0..]);
        }

        pub fn freeEdges(_: @This(), _: Allocator, _: []graph_mod.Edge) void {}
    };
    const ExactReader = struct {
        pub fn getEdges(_: @This(), alloc: Allocator, table: ?[]const u8, key: []const u8, types: []const []const u8, direction: graph_mod.EdgeDirection) ![]graph_mod.Edge {
            return (GenericReader{}).getEdges(alloc, table, key, types, direction);
        }

        pub fn freeEdges(_: @This(), _: Allocator, _: []graph_mod.Edge) void {}

        pub fn probeEdgesBounded(_: @This(), alloc: Allocator, _: ?[]const u8, probes: []const graph_mod.EdgeProbe, _: usize) ![]?graph_mod.Edge {
            const results = try alloc.alloc(?graph_mod.Edge, probes.len);
            @memset(results, null);
            for (probes, 0..) |probe, i| {
                if (std.mem.eql(u8, probe.source, "post:1") and std.mem.eql(u8, probe.target, "tag")) {
                    results[i] = GenericReader.post_one[0];
                } else if (std.mem.eql(u8, probe.source, "post:2") and std.mem.eql(u8, probe.target, "tag")) {
                    results[i] = GenericReader.post_two[1];
                }
            }
            return results;
        }

        pub fn freeProbedEdges(_: @This(), alloc: Allocator, edges: []?graph_mod.Edge) void {
            alloc.free(edges);
        }
    };

    const pattern = [_]PatternStep{
        .{ .alias = "forum" },
        .{ .alias = "post", .edge = .{ .types = &.{"CONTAINER_OF"} } },
        .{ .alias = "tag", .edge = .{ .types = &.{"HAS_TAG"}, .min_weight = 0.6 } },
    };
    const returned_aliases = [_][]const u8{ "post", "tag" };
    var exact_stats = MatchStats{};
    const exact = try matchPatternWithEdgeReader(std.testing.allocator, ExactReader{}, &.{"forum"}, &pattern, .{
        .target_nodes = &.{.{ .table = null, .key = "tag" }},
        .target_required = true,
        .return_aliases = &returned_aliases,
        .include_paths = true,
        .stats = &exact_stats,
    });
    defer freeMatches(std.testing.allocator, exact);
    var generic_stats = MatchStats{};
    const generic = try matchPatternWithEdgeReader(std.testing.allocator, GenericReader{}, &.{"forum"}, &pattern, .{
        .target_nodes = &.{.{ .table = null, .key = "tag" }},
        .target_required = true,
        .return_aliases = &returned_aliases,
        .include_paths = true,
        .stats = &generic_stats,
    });
    defer freeMatches(std.testing.allocator, generic);

    try std.testing.expectEqual(MatchPlan.exact_two_edge_probe, exact_stats.plan);
    try std.testing.expectEqual(MatchPlan.generic_expand, generic_stats.plan);
    try std.testing.expectEqual(generic.len, exact.len);
    try std.testing.expectEqual(@as(usize, 1), exact.len);
    try std.testing.expectEqual(generic[0].bindings.len, exact[0].bindings.len);
    for (generic[0].bindings, exact[0].bindings) |expected, actual| {
        try std.testing.expectEqualStrings(expected.alias, actual.alias);
        try std.testing.expectEqualStrings(expected.key, actual.key);
        try std.testing.expectEqual(expected.depth, actual.depth);
    }
    try std.testing.expectEqual(generic[0].path.len, exact[0].path.len);
    for (generic[0].path, exact[0].path) |expected, actual| {
        try std.testing.expectEqualStrings(expected.source, actual.source);
        try std.testing.expectEqualStrings(expected.target, actual.target);
        try std.testing.expectEqualStrings(expected.edge_type, actual.edge_type);
        try std.testing.expectEqual(expected.weight, actual.weight);
        try std.testing.expectEqualStrings(expected.metadata, actual.metadata);
    }
}

test "exact two-edge probe honors incoming final direction" {
    const Reader = struct {
        const first = [_]graph_mod.Edge{.{
            .source = "start",
            .target = "middle",
            .edge_type = "FIRST",
            .weight = 1,
            .created_at = 0,
            .updated_at = 0,
            .metadata = "",
        }};
        const second = graph_mod.Edge{
            .source = "target",
            .target = "middle",
            .edge_type = "SECOND",
            .weight = 1,
            .created_at = 0,
            .updated_at = 0,
            .metadata = "",
        };

        pub fn getEdges(_: @This(), _: Allocator, _: ?[]const u8, key: []const u8, _: []const []const u8, direction: graph_mod.EdgeDirection) ![]graph_mod.Edge {
            try std.testing.expectEqualStrings("start", key);
            try std.testing.expectEqual(graph_mod.EdgeDirection.out, direction);
            return @constCast(first[0..]);
        }

        pub fn freeEdges(_: @This(), _: Allocator, _: []graph_mod.Edge) void {}

        pub fn probeEdgesBounded(_: @This(), alloc: Allocator, _: ?[]const u8, probes: []const graph_mod.EdgeProbe, _: usize) ![]?graph_mod.Edge {
            try std.testing.expectEqual(@as(usize, 1), probes.len);
            try std.testing.expectEqualStrings("target", probes[0].source);
            try std.testing.expectEqualStrings("middle", probes[0].target);
            try std.testing.expectEqualStrings("SECOND", probes[0].edge_type);
            const result = try alloc.alloc(?graph_mod.Edge, 1);
            result[0] = second;
            return result;
        }

        pub fn freeProbedEdges(_: @This(), alloc: Allocator, edges: []?graph_mod.Edge) void {
            alloc.free(edges);
        }
    };

    const matches = try matchPatternWithEdgeReader(std.testing.allocator, Reader{}, &.{"start"}, &.{
        .{ .alias = "start" },
        .{ .alias = "middle", .edge = .{ .types = &.{"FIRST"} } },
        .{ .alias = "target", .edge = .{ .types = &.{"SECOND"}, .direction = .in } },
    }, .{
        .target_nodes = &.{.{ .table = null, .key = "target" }},
        .target_required = true,
        .include_paths = true,
    });
    defer freeMatches(std.testing.allocator, matches);
    try std.testing.expectEqual(@as(usize, 1), matches.len);
    try std.testing.expectEqualStrings("target", matches[0].bindings[2].key);
    try std.testing.expectEqual(@as(usize, 2), matches[0].path.len);
    try std.testing.expectEqualStrings("start", matches[0].path[0].source);
    try std.testing.expectEqualStrings("middle", matches[0].path[0].target);
    try std.testing.expectEqual(graph_mod.EdgeDirection.out, matches[0].path[0].traversal_direction.?);
    try std.testing.expectEqualStrings("target", matches[0].path[1].source);
    try std.testing.expectEqualStrings("middle", matches[0].path[1].target);
    try std.testing.expectEqual(graph_mod.EdgeDirection.in, matches[0].path[1].traversal_direction.?);

    const equal_key_edge = graph_mod.Edge{
        .source = "shared",
        .target = "shared",
        .edge_type = "CROSS_TABLE",
        .weight = 1,
        .created_at = 0,
        .updated_at = 0,
        .metadata = "",
    };
    const ambiguous = try appendPathEdge(std.testing.allocator, &.{}, equal_key_edge, "shared", "shared", .both);
    defer freePathEdges(std.testing.allocator, ambiguous);
    try std.testing.expectEqualStrings("shared", ambiguous[0].source);
    try std.testing.expectEqualStrings("shared", ambiguous[0].target);
    try std.testing.expectEqual(@as(?graph_mod.EdgeDirection, null), ambiguous[0].traversal_direction);
}

test "exact two-edge probe preserves fixed-edge self loops" {
    const Reader = struct {
        const first = [_]graph_mod.Edge{
            .{
                .source = "start",
                .target = "start",
                .edge_type = "FIRST",
                .weight = 1,
                .created_at = 0,
                .updated_at = 0,
                .metadata = "",
            },
            .{
                .source = "start",
                .target = "middle",
                .edge_type = "FIRST",
                .weight = 1,
                .created_at = 0,
                .updated_at = 0,
                .metadata = "",
            },
        };
        const cross_edge = graph_mod.Edge{
            .source = "start",
            .target = "middle",
            .edge_type = "SECOND",
            .weight = 1,
            .created_at = 0,
            .updated_at = 0,
            .metadata = "",
        };
        const self_loop = graph_mod.Edge{
            .source = "middle",
            .target = "middle",
            .edge_type = "SECOND",
            .weight = 1,
            .created_at = 0,
            .updated_at = 0,
            .metadata = "",
        };

        pub fn getEdges(_: @This(), _: Allocator, _: ?[]const u8, key: []const u8, _: []const []const u8, _: graph_mod.EdgeDirection) ![]graph_mod.Edge {
            try std.testing.expectEqualStrings("start", key);
            return @constCast(first[0..]);
        }

        pub fn freeEdges(_: @This(), _: Allocator, _: []graph_mod.Edge) void {}

        pub fn probeEdgesBounded(_: @This(), alloc: Allocator, _: ?[]const u8, probes: []const graph_mod.EdgeProbe, _: usize) ![]?graph_mod.Edge {
            try std.testing.expectEqual(@as(usize, 2), probes.len);
            const result = try alloc.alloc(?graph_mod.Edge, probes.len);
            result[0] = cross_edge;
            result[1] = self_loop;
            return result;
        }

        pub fn freeProbedEdges(_: @This(), alloc: Allocator, edges: []?graph_mod.Edge) void {
            alloc.free(edges);
        }
    };

    var stats = MatchStats{};
    const matches = try matchPatternWithEdgeReader(std.testing.allocator, Reader{}, &.{"start"}, &.{
        .{ .alias = "start" },
        .{ .alias = "middle", .edge = .{ .types = &.{"FIRST"} } },
        .{ .alias = "target", .edge = .{ .types = &.{"SECOND"} } },
    }, .{
        .target_nodes = &.{.{ .table = null, .key = "middle" }},
        .target_required = true,
        .include_paths = false,
        .stats = &stats,
    });
    defer freeMatches(std.testing.allocator, matches);

    try std.testing.expectEqual(MatchPlan.exact_two_edge_probe, stats.plan);
    try std.testing.expectEqual(@as(usize, 2), matches.len);
    try std.testing.expectEqualStrings("start", matches[0].bindings[1].key);
    try std.testing.expectEqualStrings("middle", matches[1].bindings[1].key);
}

test "exact endpoint constrains the final pattern step before limiting" {
    const Reader = struct {
        const edges = [_]graph_mod.Edge{
            .{ .source = "a", .target = "wrong", .edge_type = "link", .weight = 1, .created_at = 0, .updated_at = 0, .metadata = "" },
            .{ .source = "a", .target = "wanted", .edge_type = "link", .weight = 1, .created_at = 0, .updated_at = 0, .metadata = "" },
        };

        pub fn getEdges(_: @This(), _: Allocator, _: ?[]const u8, _: []const u8, types: []const []const u8, _: graph_mod.EdgeDirection) ![]graph_mod.Edge {
            try std.testing.expectEqualStrings("link", types[0]);
            return @constCast(edges[0..]);
        }

        pub fn freeEdges(_: @This(), _: Allocator, _: []graph_mod.Edge) void {}
    };

    const matches = try matchPatternWithEdgeReader(
        std.testing.allocator,
        Reader{},
        &.{"a"},
        &.{
            .{ .alias = "source" },
            .{ .alias = "target", .edge = .{ .types = &.{"link"} } },
        },
        .{ .max_results = 1, .target_nodes = &.{.{ .table = null, .key = "wanted" }}, .target_required = true, .include_paths = false },
    );
    defer freeMatches(std.testing.allocator, matches);
    try std.testing.expectEqual(@as(usize, 1), matches.len);
    try std.testing.expectEqualStrings("wanted", matches[0].bindings[1].key);
    try std.testing.expectEqual(@as(usize, 0), matches[0].path.len);
}

test "pattern matching rejects unbounded hops and exploration" {
    const Reader = struct {
        const edges = [_]graph_mod.Edge{
            .{ .source = "A", .target = "B", .edge_type = "e", .weight = 1, .created_at = 0, .updated_at = 0, .metadata = "" },
            .{ .source = "A", .target = "C", .edge_type = "e", .weight = 1, .created_at = 0, .updated_at = 0, .metadata = "" },
        };

        pub fn getEdges(_: @This(), _: Allocator, _: ?[]const u8, _: []const u8, _: []const []const u8, _: graph_mod.EdgeDirection) ![]graph_mod.Edge {
            return @constCast(edges[0..]);
        }

        pub fn freeEdges(_: @This(), _: Allocator, _: []graph_mod.Edge) void {}
    };
    const starts: []const []const u8 = &.{"A"};

    try std.testing.expectError(
        error.InvalidArgument,
        matchPatternWithEdgeReader(std.testing.allocator, Reader{}, starts, &.{
            .{ .alias = "a" },
            .{ .alias = "b", .edge = .{ .max_hops = max_pattern_hops + 1 } },
        }, .{}),
    );
    try std.testing.expectError(
        error.GraphWorkBudgetExceeded,
        matchPatternWithEdgeReader(std.testing.allocator, Reader{}, starts, &.{
            .{ .alias = "a" },
            .{ .alias = "b" },
        }, .{ .max_explored_edges = 1 }),
    );
}

test "pattern work budget preserves bounded adjacency exhaustion dimensions" {
    const Reader = struct {
        failure: anyerror,

        pub fn getEdgesBounded(
            self: @This(),
            _: Allocator,
            _: ?[]const u8,
            _: []const u8,
            _: []const []const u8,
            _: graph_mod.EdgeDirection,
            _: usize,
            _: usize,
        ) ![]graph_mod.Edge {
            return self.failure;
        }
    };

    var edge_budget = WorkBudget.init(3, 5);
    try std.testing.expectError(
        error.GraphWorkBudgetExceeded,
        getEdgesForBudget(
            std.testing.allocator,
            Reader{ .failure = error.GraphExploredEdgesBudgetExceeded },
            null,
            "A",
            &.{},
            .out,
            &edge_budget,
            false,
        ),
    );
    try std.testing.expectEqual(work_budget_diagnostic.Dimension.explored_edges, edge_budget.exhaustion().?.dimension);
    try std.testing.expectEqual(@as(usize, 5), edge_budget.exhaustion().?.maximum);

    var byte_budget = WorkBudget.init(3, 5);
    try std.testing.expectError(
        error.GraphWorkBudgetExceeded,
        getEdgesForBudget(
            std.testing.allocator,
            Reader{ .failure = error.GraphExploredEdgeBytesBudgetExceeded },
            null,
            "A",
            &.{},
            .out,
            &byte_budget,
            false,
        ),
    );
    try std.testing.expectEqual(work_budget_diagnostic.Dimension.explored_edge_bytes, byte_budget.exhaustion().?.dimension);
    try std.testing.expectEqual(default_max_explored_edge_bytes, byte_budget.exhaustion().?.maximum);
}

test "pattern matching keeps equal keys from distinct tables" {
    const Reader = struct {
        const edges = [_]graph_mod.Edge{
            .{
                .source = "A",
                .target = "shared",
                .edge_type = "local",
                .weight = 1,
                .created_at = 0,
                .updated_at = 0,
                .metadata = "",
            },
            .{
                .source = "A",
                .target = "shared",
                .edge_type = "external",
                .weight = 1,
                .created_at = 0,
                .updated_at = 0,
                .metadata = "{\"target_table\":\"entities\"}",
            },
        };

        pub fn getEdges(_: @This(), _: Allocator, _: ?[]const u8, _: []const u8, _: []const []const u8, _: graph_mod.EdgeDirection) ![]graph_mod.Edge {
            return @constCast(edges[0..]);
        }

        pub fn freeEdges(_: @This(), _: Allocator, _: []graph_mod.Edge) void {}
    };
    const matches = try matchPatternWithEdgeReader(
        std.testing.allocator,
        Reader{},
        &.{"A"},
        &.{
            .{ .alias = "source" },
            .{ .alias = "target" },
        },
        .{ .max_results = 10 },
    );
    defer freeMatches(std.testing.allocator, matches);

    try std.testing.expectEqual(@as(usize, 2), matches.len);
    var local_count: usize = 0;
    var external_count: usize = 0;
    for (matches) |match| {
        const target = match.bindings[1];
        try std.testing.expectEqualStrings("shared", target.key);
        if (target.table) |table| {
            try std.testing.expectEqualStrings("entities", table);
            external_count += 1;
        } else {
            local_count += 1;
        }
    }
    try std.testing.expectEqual(@as(usize, 1), local_count);
    try std.testing.expectEqual(@as(usize, 1), external_count);
}

test "exact pattern targets preserve table identity" {
    const Reader = struct {
        const edges = [_]graph_mod.Edge{
            .{ .source = "A", .target = "shared", .edge_type = "local", .weight = 1, .created_at = 0, .updated_at = 0, .metadata = "" },
            .{ .source = "A", .target = "shared", .edge_type = "external", .weight = 1, .created_at = 0, .updated_at = 0, .metadata = "{\"target_table\":\"entities\"}" },
        };

        pub fn getEdges(_: @This(), _: Allocator, _: ?[]const u8, _: []const u8, _: []const []const u8, _: graph_mod.EdgeDirection) ![]graph_mod.Edge {
            return @constCast(edges[0..]);
        }

        pub fn freeEdges(_: @This(), _: Allocator, _: []graph_mod.Edge) void {}
    };
    const matches = try matchPatternWithEdgeReader(
        std.testing.allocator,
        Reader{},
        &.{"A"},
        &.{
            .{ .alias = "source" },
            .{ .alias = "target" },
        },
        .{
            .max_results = 10,
            .target_required = true,
            .target_nodes = &.{.{ .table = "entities", .key = "shared" }},
        },
    );
    defer freeMatches(std.testing.allocator, matches);

    try std.testing.expectEqual(@as(usize, 1), matches.len);
    try std.testing.expectEqualStrings("entities", matches[0].bindings[1].table.?);
}

test "inapplicable exact plan does not consume generic fallback budget" {
    const Reader = struct {
        const start_edges = [_]graph_mod.Edge{
            .{ .source = "A", .target = "B", .edge_type = "first", .weight = 1, .created_at = 0, .updated_at = 0, .metadata = "" },
            .{ .source = "A", .target = "X", .edge_type = "first", .weight = 1, .created_at = 0, .updated_at = 0, .metadata = "{\"target_table\":\"entities\"}" },
        };
        const middle_edges = [_]graph_mod.Edge{
            .{ .source = "B", .target = "C", .edge_type = "second", .weight = 1, .created_at = 0, .updated_at = 0, .metadata = "" },
        };

        pub fn getEdges(_: @This(), _: Allocator, table: ?[]const u8, key: []const u8, _: []const []const u8, _: graph_mod.EdgeDirection) ![]graph_mod.Edge {
            if (std.mem.eql(u8, key, "A")) return @constCast(start_edges[0..]);
            if (table == null and std.mem.eql(u8, key, "B")) return @constCast(middle_edges[0..]);
            return @constCast((&[_]graph_mod.Edge{})[0..]);
        }

        pub fn freeEdges(_: @This(), _: Allocator, _: []graph_mod.Edge) void {}

        pub fn probeEdgesBounded(_: @This(), _: Allocator, _: ?[]const u8, _: []const graph_mod.EdgeProbe, _: usize) ![]?graph_mod.Edge {
            return error.TestUnexpectedResult;
        }

        pub fn freeProbedEdges(_: @This(), _: Allocator, _: []?graph_mod.Edge) void {}
    };
    const matches = try matchPatternWithEdgeReader(
        std.testing.allocator,
        Reader{},
        &.{"A"},
        &.{
            .{ .alias = "a" },
            .{ .alias = "b", .edge = .{ .types = &.{"first"} } },
            .{ .alias = "c", .edge = .{ .types = &.{"second"} } },
        },
        .{
            .target_required = true,
            .target_nodes = &.{.{ .table = null, .key = "C" }},
            .max_explored_nodes = 3,
            .max_explored_edges = 3,
        },
    );
    defer freeMatches(std.testing.allocator, matches);
    try std.testing.expectEqual(@as(usize, 1), matches.len);
}

test "pattern matching routes later hops through the bound table" {
    const Reader = struct {
        const source_edges = [_]graph_mod.Edge{.{
            .source = "A",
            .target = "shared",
            .edge_type = "external",
            .weight = 1,
            .created_at = 0,
            .updated_at = 0,
            .metadata = "{\"target_table\":\"entities\"}",
        }};
        const entity_edges = [_]graph_mod.Edge{.{
            .source = "shared",
            .target = "C",
            .edge_type = "local",
            .weight = 1,
            .created_at = 0,
            .updated_at = 0,
            .metadata = "",
        }};

        pub fn getEdges(
            _: @This(),
            _: Allocator,
            table: ?[]const u8,
            key: []const u8,
            _: []const []const u8,
            _: graph_mod.EdgeDirection,
        ) ![]graph_mod.Edge {
            if (std.mem.eql(u8, key, "A")) {
                try std.testing.expect(table == null);
                return @constCast(source_edges[0..]);
            }
            if (std.mem.eql(u8, key, "shared")) {
                try std.testing.expectEqualStrings("entities", table.?);
                return @constCast(entity_edges[0..]);
            }
            return @constCast((&[_]graph_mod.Edge{})[0..]);
        }

        pub fn freeEdges(_: @This(), _: Allocator, _: []graph_mod.Edge) void {}
    };

    const matches = try matchPatternWithEdgeReader(
        std.testing.allocator,
        Reader{},
        &.{"A"},
        &.{
            .{ .alias = "source" },
            .{ .alias = "entity" },
            .{ .alias = "related" },
        },
        .{ .max_results = 10 },
    );
    defer freeMatches(std.testing.allocator, matches);

    try std.testing.expectEqual(@as(usize, 1), matches.len);
    try std.testing.expectEqualStrings("C", matches[0].bindings[2].key);
    try std.testing.expectEqualStrings("entities", matches[0].bindings[2].table.?);
}

test "conjunctive cycle canonicalizes an explicit source table on return" {
    const Reader = struct {
        const source_edges = [_]graph_mod.Edge{.{
            .source = "A",
            .target = "X",
            .edge_type = "external",
            .weight = 1,
            .created_at = 0,
            .updated_at = 0,
            .metadata = "{\"target_table\":\"entities\"}",
        }};
        const return_edges = [_]graph_mod.Edge{.{
            .source = "X",
            .target = "A",
            .edge_type = "returns",
            .weight = 1,
            .created_at = 0,
            .updated_at = 0,
            .metadata = "{\"target_table\":\"docs\"}",
        }};

        pub fn canonicalizeTable(_: @This(), table: ?[]const u8) ?[]const u8 {
            const name = table orelse return null;
            return if (std.mem.eql(u8, name, "docs")) null else name;
        }

        pub fn getEdges(
            _: @This(),
            _: Allocator,
            table: ?[]const u8,
            key: []const u8,
            _: []const []const u8,
            _: graph_mod.EdgeDirection,
        ) ![]graph_mod.Edge {
            if (table == null and std.mem.eql(u8, key, "A"))
                return @constCast(source_edges[0..]);
            if (table != null and
                std.mem.eql(u8, table.?, "entities") and
                std.mem.eql(u8, key, "X"))
                return @constCast(return_edges[0..]);
            return @constCast((&[_]graph_mod.Edge{})[0..]);
        }

        pub fn freeEdges(_: @This(), _: Allocator, _: []graph_mod.Edge) void {}
    };
    const nodes = [_]MatchNode{
        .{ .alias = "source" },
        .{ .alias = "entity" },
    };
    const edges = [_]MatchEdge{
        .{ .from = "source", .to = "entity", .step = .{ .types = &.{"external"} } },
        .{ .from = "entity", .to = "source", .step = .{ .types = &.{"returns"} } },
    };

    const matches = try matchConjunctivePatternWithEdgeReader(
        std.testing.allocator,
        Reader{},
        &.{"A"},
        .{ .anchor_alias = "source", .nodes = &nodes, .edges = &edges },
        .{ .max_results = 10 },
    );
    defer freeMatches(std.testing.allocator, matches);

    try std.testing.expectEqual(@as(usize, 1), matches.len);
    try std.testing.expect(matches[0].bindings[0].table == null);
    try std.testing.expectEqualStrings("A", matches[0].bindings[0].key);
    try std.testing.expectEqualStrings("entities", matches[0].bindings[1].table.?);
    try std.testing.expectEqualStrings("X", matches[0].bindings[1].key);
}

test "pattern matching preserves a table-scoped start reference" {
    const Reader = struct {
        const edges = [_]graph_mod.Edge{.{
            .source = "shared",
            .target = "C",
            .edge_type = "local",
            .weight = 1,
            .created_at = 0,
            .updated_at = 0,
            .metadata = "",
        }};

        pub fn getEdges(
            _: @This(),
            _: Allocator,
            table: ?[]const u8,
            key: []const u8,
            _: []const []const u8,
            _: graph_mod.EdgeDirection,
        ) ![]graph_mod.Edge {
            try std.testing.expectEqualStrings("entities", table.?);
            try std.testing.expectEqualStrings("shared", key);
            return @constCast(edges[0..]);
        }

        pub fn freeEdges(_: @This(), _: Allocator, _: []graph_mod.Edge) void {}
    };
    const starts = [_]node_identity.Ref{.{
        .table = "entities",
        .key = "shared",
    }};
    const matches = try matchPatternFromRefsWithEdgeReader(
        std.testing.allocator,
        Reader{},
        &starts,
        &.{
            .{ .alias = "entity" },
            .{ .alias = "related" },
        },
        .{ .max_results = 10 },
    );
    defer freeMatches(std.testing.allocator, matches);

    try std.testing.expectEqual(@as(usize, 1), matches.len);
    try std.testing.expectEqualStrings("entities", matches[0].bindings[0].table.?);
    try std.testing.expectEqualStrings("entities", matches[0].bindings[1].table.?);
}

test "pattern match supports linear alias bindings and cycles" {
    const alloc = std.testing.allocator;

    var arena = std.heap.ArenaAllocator.init(alloc);
    defer arena.deinit();

    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    const dir_path = try std.fmt.allocPrint(alloc, ".zig-cache/tmp/{s}/graph-pattern", .{tmp.sub_path});
    defer alloc.free(dir_path);
    const dir = try alloc.dupeZ(u8, dir_path);
    defer alloc.free(dir);
    var doc_store = try @import("../storage/docstore.zig").DocStore.open(arena.allocator(), dir, .{});
    defer doc_store.close();

    const reverse_dir_path = try std.fmt.allocPrint(alloc, ".zig-cache/tmp/{s}/graph-pattern-rev", .{tmp.sub_path});
    defer alloc.free(reverse_dir_path);
    const reverse_dir = try alloc.dupeZ(u8, reverse_dir_path);
    defer alloc.free(reverse_dir);
    var graph_index = try graph_mod.GraphIndex.open(alloc, &doc_store, reverse_dir, "g", .{});
    defer graph_index.close();

    try graph_index.addEdge("a", "b", "cites", 1.0, 0, 0, "");
    try graph_index.addEdge("b", "c", "cites", 1.0, 0, 0, "");
    try graph_index.addEdge("c", "a", "cites", 1.0, 0, 0, "");

    const start_keys = [_][]const u8{"a"};
    const pattern = [_]PatternStep{
        .{ .alias = "src" },
        .{ .alias = "mid", .edge = .{ .types = &.{"cites"} } },
        .{ .alias = "src", .edge = .{ .types = &.{"cites"}, .min_hops = 2, .max_hops = 2 } },
    };

    const matches = try matchPattern(alloc, &graph_index, &start_keys, &pattern, .{ .max_results = 10 });
    defer freeMatches(alloc, matches);

    try std.testing.expectEqual(@as(usize, 1), matches.len);
    try std.testing.expectEqual(@as(usize, 2), matches[0].bindings.len);
    try std.testing.expectEqualStrings("src", matches[0].bindings[0].alias);
    try std.testing.expectEqualStrings("a", matches[0].bindings[0].key);
    try std.testing.expectEqual(@as(u32, 0), matches[0].bindings[0].depth);
    try std.testing.expectEqualStrings("mid", matches[0].bindings[1].alias);
    try std.testing.expectEqualStrings("b", matches[0].bindings[1].key);
    try std.testing.expectEqual(@as(u32, 1), matches[0].bindings[1].depth);
    try std.testing.expectEqual(@as(usize, 3), matches[0].path.len);
}
