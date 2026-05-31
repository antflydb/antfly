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

//! Graph Query API — structured DSL matching Go antfly's graph_searches.
//!
//! Provides a unified query interface over the graph module's traversal and
//! path-finding primitives:
//!   - traverse / neighbors: BFS via traversal.traverse()
//!   - shortest_path: via paths.findShortestPath()
//!   - k_shortest_paths: via paths.findKShortestPaths()

const std = @import("std");
const Allocator = std.mem.Allocator;
const platform_time = @import("../platform/time.zig");
const graph_mod = @import("graph.zig");
const pattern_mod = @import("pattern.zig");
const traversal_mod = @import("traversal.zig");
const paths_mod = @import("paths.zig");
const algebraic_ir = @import("../storage/db/algebraic/ir.zig");
const algebraic_planner = @import("../storage/db/algebraic/planner.zig");
const algebraic_path_mod = @import("../storage/db/algebraic/path.zig");

// ============================================================================
// Query types
// ============================================================================

pub const QueryType = enum {
    traverse,
    neighbors,
    shortest_path,
    k_shortest_paths,
    pattern,
};

pub const NodeSelector = union(enum) {
    keys: []const []const u8,
    result_ref: ResultRef,
};

pub const ResultRef = struct {
    ref: []const u8, // e.g. "$full_text_results"
    limit: u32 = 0, // 0 = use all results
};

pub const QueryParams = struct {
    edge_types: []const []const u8 = &.{},
    direction: graph_mod.EdgeDirection = .out,
    max_depth: u32 = 3,
    max_results: u32 = 100,
    min_weight: f64 = 0.0,
    max_weight: f64 = 0.0,
    deduplicate: bool = true,
    include_paths: bool = false,
    weight_mode: paths_mod.PathWeightMode = .min_hops,
    algebraic_semiring: bool = false,
};

pub const AlgebraicTraversalRejectReason = algebraic_path_mod.ExecutionRejectReason;
pub const AlgebraicTraversalProof = algebraic_path_mod.ExecutionProof;

pub fn algebraicTraversalProof(graph_index: *const graph_mod.GraphIndex, params: QueryParams) AlgebraicTraversalProof {
    return algebraic_path_mod.executionProof(.{
        .semiring_enabled = params.algebraic_semiring or graph_index.supportsAlgebraicSemiringTraversal(),
        .deduplicate = params.deduplicate,
        .max_depth = params.max_depth,
        .max_results = params.max_results,
        .min_weight = params.min_weight,
        .max_weight = params.max_weight,
        .min_hops = params.weight_mode == .min_hops,
    });
}

fn algebraicTraversalConsidered(graph_index: *const graph_mod.GraphIndex, params: QueryParams) bool {
    return params.algebraic_semiring or graph_index.supportsAlgebraicSemiringTraversal();
}

pub const GraphQuery = struct {
    query_type: QueryType,
    index_name: []const u8,
    start_nodes: NodeSelector,
    params: QueryParams = .{},
    target_nodes: ?NodeSelector = null,
    k: u32 = 1,
    pattern: []const pattern_mod.PatternStep = &.{},
    return_aliases: []const []const u8 = &.{},
    include_documents: bool = false,
    fields: []const []const u8 = &.{},
    include_all_fields: bool = true,
};

pub const ExpandStrategy = enum { @"union", intersection };

// ============================================================================
// Result types
// ============================================================================

pub const PathEdgeInfo = struct {
    source: []const u8,
    target: []const u8,
    edge_type: []const u8,
    weight: f64,
};

pub const GraphResultNode = struct {
    key: []const u8,
    depth: u32,
    distance: f64,
    path: ?[]const []const u8,
    path_edges: ?[]const PathEdgeInfo,
    provenance: ?[]const []const u8 = null,
    /// Table the node's document lives in, when an edge reaching it declared a
    /// cross-table endpoint (`target_table` in its metadata). Null means the
    /// node is same-table (hydrated locally). Lets the api hydrate a cross-table
    /// entity node from its own table instead of failing closed.
    table: ?[]const u8 = null,

    pub fn deinit(self: *GraphResultNode, alloc: Allocator) void {
        alloc.free(self.key);
        if (self.table) |t| alloc.free(t);
        if (self.path) |p| {
            for (p) |s| alloc.free(s);
            alloc.free(p);
        }
        if (self.path_edges) |pe| {
            for (pe) |e| {
                alloc.free(e.source);
                alloc.free(e.target);
                alloc.free(e.edge_type);
            }
            alloc.free(pe);
        }
        if (self.provenance) |items| {
            for (items) |item| alloc.free(item);
            alloc.free(items);
        }
        self.* = undefined;
    }
};

pub const GraphQueryResult = struct {
    nodes: []GraphResultNode,
    matches: []pattern_mod.PatternMatch = &.{},

    pub fn deinit(self: *GraphQueryResult, alloc: Allocator) void {
        for (self.nodes) |*node| node.deinit(alloc);
        alloc.free(self.nodes);
        pattern_mod.freeMatches(alloc, self.matches);
    }
};

// ============================================================================
// Graph Query Engine
// ============================================================================

pub const GraphQueryEngine = struct {
    alloc: Allocator,

    /// Execute a graph query. For result_ref node selectors, the caller must
    /// resolve refs to keys and pass them as resolved_keys.
    pub fn execute(
        self: *GraphQueryEngine,
        graph_index: *graph_mod.GraphIndex,
        gq: GraphQuery,
        resolved_keys: []const []const u8,
    ) !GraphQueryResult {
        return switch (gq.query_type) {
            .traverse => self.executeTraverse(graph_index, gq.params, resolved_keys, resolveTargetKeys(gq)),
            .neighbors => blk: {
                var params = gq.params;
                params.max_depth = 1;
                break :blk self.executeTraverse(graph_index, params, resolved_keys, resolveTargetKeys(gq));
            },
            .shortest_path => self.executeShortestPath(graph_index, gq, resolved_keys),
            .k_shortest_paths => self.executeKShortestPaths(graph_index, gq, resolved_keys),
            .pattern => self.executePattern(graph_index, gq, resolved_keys),
        };
    }

    fn executeTraverse(
        self: *GraphQueryEngine,
        graph_index: *graph_mod.GraphIndex,
        params: QueryParams,
        start_keys: []const []const u8,
        target_keys: []const []const u8,
    ) !GraphQueryResult {
        if (algebraicTraversalConsidered(graph_index, params)) {
            graph_index.noteAlgebraicTraversalAttempt();
            const proof = algebraicTraversalProof(graph_index, params);
            if (proof.safe()) {
                if (try self.executeAlgebraicTraverse(graph_index, params, start_keys, target_keys)) |result| {
                    graph_index.noteAlgebraicTraversalProven(result.nodes.len);
                    return result;
                }
                graph_index.noteAlgebraicTraversalFallback();
            } else {
                graph_index.noteAlgebraicTraversalRejected();
            }
        }

        const rules = traversal_mod.TraversalRules{
            .edge_types = params.edge_types,
            .direction = params.direction,
            .max_depth = params.max_depth,
            .min_weight = params.min_weight,
            .max_weight = params.max_weight,
            .max_results = params.max_results,
            .deduplicate = params.deduplicate,
            .include_paths = params.include_paths,
        };

        var all_results = std.ArrayListUnmanaged(GraphResultNode).empty;
        var cleanup_results = true;
        defer if (cleanup_results) {
            for (all_results.items) |node| freeResultNode(self.alloc, node);
            all_results.deinit(self.alloc);
        };

        var seen = std.StringHashMapUnmanaged(void).empty;
        defer {
            var it = seen.keyIterator();
            while (it.next()) |k| self.alloc.free(k.*);
            seen.deinit(self.alloc);
        }

        for (start_keys) |key| {
            const trav_results = try traversal_mod.traverse(self.alloc, graph_index, key, rules);
            defer traversal_mod.freeOwnedResults(self.alloc, trav_results);

            for (trav_results) |tr| {
                if (!targetKeyAllowed(target_keys, tr.key)) continue;
                if (params.deduplicate) {
                    if (seen.contains(tr.key)) continue;
                    try seen.put(self.alloc, try self.alloc.dupe(u8, tr.key), {});
                }

                var path_copy: ?[]const []const u8 = null;
                if (tr.path) |p| {
                    const duped = try self.alloc.alloc([]const u8, p.len);
                    for (p, 0..) |s, i| duped[i] = try self.alloc.dupe(u8, s);
                    path_copy = duped;
                }

                try all_results.append(self.alloc, .{
                    .key = try self.alloc.dupe(u8, tr.key),
                    .depth = tr.depth,
                    .distance = tr.total_weight,
                    .path = path_copy,
                    .path_edges = null,
                    .table = if (tr.target_table) |tt| try self.alloc.dupe(u8, tt) else null,
                });

                if (params.max_results > 0 and all_results.items.len >= params.max_results) break;
            }
            if (params.max_results > 0 and all_results.items.len >= params.max_results) break;
        }

        const owned = try self.alloc.dupe(GraphResultNode, all_results.items);
        cleanup_results = false;
        all_results.deinit(self.alloc);
        return .{ .nodes = owned };
    }

    fn executeAlgebraicTraverse(
        self: *GraphQueryEngine,
        graph_index: *graph_mod.GraphIndex,
        params: QueryParams,
        start_keys: []const []const u8,
        target_keys: []const []const u8,
    ) !?GraphQueryResult {
        if (!algebraicTraversalProof(graph_index, params).safe()) return null;
        if (!try algebraicTraversalTensorProgramAccepted(self.alloc, graph_index, params, target_keys)) return null;

        var all_results = std.ArrayListUnmanaged(GraphResultNode).empty;
        var cleanup_results = true;
        defer if (cleanup_results) {
            for (all_results.items) |node| freeResultNode(self.alloc, node);
            all_results.deinit(self.alloc);
        };

        var seen = std.StringHashMapUnmanaged(void).empty;
        defer {
            var it = seen.keyIterator();
            while (it.next()) |k| self.alloc.free(k.*);
            seen.deinit(self.alloc);
        }

        for (start_keys) |start_key| {
            var algebraic_edges = try collectAlgebraicReachabilityEdges(self.alloc, graph_index, start_key, params);
            defer algebraic_edges.deinit(self.alloc);

            const reached = try algebraic_path_mod.boundedReachabilityWithOptionsAlloc(self.alloc, start_key, algebraic_edges.items, params.max_depth, .{ .target_nodes = target_keys });
            defer algebraic_path_mod.deinitPathResults(self.alloc, reached);

            for (reached) |item| {
                if (seen.contains(item.node)) continue;
                const node = if (params.include_paths)
                    (try algebraicShortestPathResultNodeAlloc(self.alloc, graph_index, params, start_key, item.node, item)) orelse return null
                else
                    try algebraicTraversalResultNodeAlloc(self.alloc, item);
                try all_results.append(self.alloc, node);
                try seen.put(self.alloc, try self.alloc.dupe(u8, item.node), {});
                if (params.max_results > 0 and all_results.items.len >= params.max_results) break;
            }
            if (params.max_results > 0 and all_results.items.len >= params.max_results) break;
        }

        const owned = try self.alloc.dupe(GraphResultNode, all_results.items);
        cleanup_results = false;
        all_results.deinit(self.alloc);
        return .{ .nodes = owned };
    }

    fn executeShortestPath(
        self: *GraphQueryEngine,
        graph_index: *graph_mod.GraphIndex,
        gq: GraphQuery,
        start_keys: []const []const u8,
    ) !GraphQueryResult {
        const target_keys = resolveTargetKeys(gq);
        if (algebraicTraversalConsidered(graph_index, gq.params)) {
            graph_index.noteAlgebraicTraversalAttempt();
            if (try self.executeAlgebraicShortestPath(graph_index, gq.params, start_keys, target_keys)) |result| {
                graph_index.noteAlgebraicTraversalProven(result.nodes.len);
                return result;
            }
            if (algebraicTraversalProof(graph_index, gq.params).safe()) {
                graph_index.noteAlgebraicTraversalFallback();
            } else {
                graph_index.noteAlgebraicTraversalRejected();
            }
        }

        const opts = paths_mod.PathFindOptions{
            .weight_mode = gq.params.weight_mode,
            .edge_types = gq.params.edge_types,
            .direction = gq.params.direction,
            .max_depth = gq.params.max_depth,
            .min_weight = gq.params.min_weight,
            .max_weight = gq.params.max_weight,
        };

        var all_results = std.ArrayListUnmanaged(GraphResultNode).empty;
        errdefer {
            for (all_results.items) |node| freeResultNode(self.alloc, node);
            all_results.deinit(self.alloc);
        }

        for (start_keys) |sk| {
            for (target_keys) |tk| {
                const path = try paths_mod.findShortestPath(self.alloc, graph_index, sk, tk, opts);
                if (path) |p| {
                    defer paths_mod.freePath(self.alloc, p);
                    try all_results.append(self.alloc, try pathToResultNode(self.alloc, &p));
                }
            }
        }

        const owned = try self.alloc.dupe(GraphResultNode, all_results.items);
        all_results.deinit(self.alloc);
        return .{ .nodes = owned };
    }

    fn executeAlgebraicShortestPath(
        self: *GraphQueryEngine,
        graph_index: *graph_mod.GraphIndex,
        params: QueryParams,
        start_keys: []const []const u8,
        target_keys: []const []const u8,
    ) !?GraphQueryResult {
        if (!(params.algebraic_semiring or graph_index.supportsAlgebraicSemiringTraversal())) return null;
        if (params.weight_mode != .min_hops) return null;
        if (params.max_depth == 0) return null;
        if (target_keys.len == 0) return null;
        if (!try algebraicTraversalTensorProgramAccepted(self.alloc, graph_index, params, target_keys)) return null;

        var all_results = std.ArrayListUnmanaged(GraphResultNode).empty;
        errdefer {
            for (all_results.items) |node| freeResultNode(self.alloc, node);
            all_results.deinit(self.alloc);
        }

        for (start_keys) |start_key| {
            for (target_keys) |target_key| {
                if (std.mem.eql(u8, start_key, target_key)) {
                    try all_results.append(self.alloc, try trivialPathResultNode(self.alloc, start_key));
                    continue;
                }

                var algebraic_edges = try collectAlgebraicReachabilityEdges(self.alloc, graph_index, start_key, params);
                defer algebraic_edges.deinit(self.alloc);

                const reached = try algebraic_path_mod.boundedReachabilityWithOptionsAlloc(
                    self.alloc,
                    start_key,
                    algebraic_edges.items,
                    params.max_depth,
                    .{ .target_nodes = &.{target_key} },
                );
                defer algebraic_path_mod.deinitPathResults(self.alloc, reached);

                if (reached.len == 0) continue;
                if (reached.len != 1) return null;
                const node = (try algebraicShortestPathResultNodeAlloc(self.alloc, graph_index, params, start_key, target_key, reached[0])) orelse return null;
                try all_results.append(self.alloc, node);
            }
        }

        const owned = try self.alloc.dupe(GraphResultNode, all_results.items);
        all_results.deinit(self.alloc);
        return .{ .nodes = owned };
    }

    fn executeKShortestPaths(
        self: *GraphQueryEngine,
        graph_index: *graph_mod.GraphIndex,
        gq: GraphQuery,
        start_keys: []const []const u8,
    ) !GraphQueryResult {
        const target_keys = resolveTargetKeys(gq);
        if (gq.k == 1) {
            if (try self.executeAlgebraicShortestPath(graph_index, gq.params, start_keys, target_keys)) |result| return result;
        }

        const opts = paths_mod.PathFindOptions{
            .weight_mode = gq.params.weight_mode,
            .edge_types = gq.params.edge_types,
            .direction = gq.params.direction,
            .max_depth = gq.params.max_depth,
            .min_weight = gq.params.min_weight,
            .max_weight = gq.params.max_weight,
        };

        var all_results = std.ArrayListUnmanaged(GraphResultNode).empty;
        errdefer {
            for (all_results.items) |node| freeResultNode(self.alloc, node);
            all_results.deinit(self.alloc);
        }

        for (start_keys) |sk| {
            for (target_keys) |tk| {
                const found = try paths_mod.findKShortestPaths(self.alloc, graph_index, sk, tk, gq.k, opts);
                defer paths_mod.freePaths(self.alloc, found);

                for (found) |p| {
                    try all_results.append(self.alloc, try pathToResultNode(self.alloc, &p));
                }
            }
        }

        const owned = try self.alloc.dupe(GraphResultNode, all_results.items);
        all_results.deinit(self.alloc);
        return .{ .nodes = owned };
    }

    fn executePattern(
        self: *GraphQueryEngine,
        graph_index: *graph_mod.GraphIndex,
        gq: GraphQuery,
        start_keys: []const []const u8,
    ) !GraphQueryResult {
        if (algebraicPatternPlan(gq)) |plan_for_status| {
            if (algebraicTraversalConsidered(graph_index, plan_for_status.params)) {
                graph_index.noteAlgebraicTraversalAttempt();
                if (try self.executeAlgebraicPattern(graph_index, gq, start_keys)) |result| {
                    graph_index.noteAlgebraicTraversalProven(result.nodes.len);
                    return result;
                }
                if (algebraicTraversalProof(graph_index, plan_for_status.params).safe()) {
                    graph_index.noteAlgebraicTraversalFallback();
                } else {
                    graph_index.noteAlgebraicTraversalRejected();
                }
            }
        }

        const matches = try pattern_mod.matchPattern(
            self.alloc,
            graph_index,
            start_keys,
            gq.pattern,
            .{
                .max_results = gq.params.max_results,
                .return_aliases = gq.return_aliases,
            },
        );
        errdefer pattern_mod.freeMatches(self.alloc, matches);

        const owned_nodes = try collectUniqueNodesFromMatches(self.alloc, matches);
        return .{ .nodes = owned_nodes, .matches = matches };
    }

    fn executeAlgebraicPattern(
        self: *GraphQueryEngine,
        graph_index: *graph_mod.GraphIndex,
        gq: GraphQuery,
        start_keys: []const []const u8,
    ) !?GraphQueryResult {
        const plan = algebraicPatternPlan(gq) orelse return null;
        if (!algebraicTraversalProof(graph_index, plan.params).safe()) return null;
        if (!try algebraicTraversalTensorProgramAccepted(self.alloc, graph_index, plan.params, &.{})) return null;

        var matches = std.ArrayListUnmanaged(pattern_mod.PatternMatch).empty;
        var matches_owned = true;
        defer if (matches_owned) {
            for (matches.items) |*match| match.deinit(self.alloc);
            matches.deinit(self.alloc);
        };

        for (start_keys) |start_key| {
            if (!graphQueryPassesPrefixFilter(start_key, gq.pattern[0].node_filter)) continue;

            var algebraic_edges = try collectAlgebraicReachabilityEdges(self.alloc, graph_index, start_key, plan.params);
            defer algebraic_edges.deinit(self.alloc);

            const reached = try algebraic_path_mod.boundedReachabilityWithOptionsAlloc(self.alloc, start_key, algebraic_edges.items, plan.depth, .{});
            defer algebraic_path_mod.deinitPathResults(self.alloc, reached);

            for (reached) |item| {
                if (item.depth != plan.depth) continue;
                const node = (try algebraicShortestPathResultNodeAlloc(self.alloc, graph_index, plan.params, start_key, item.node, item)) orelse return null;
                defer freeResultNode(self.alloc, node);

                var match = (try algebraicPatternMatchFromNodeAlloc(self.alloc, gq.pattern, gq.return_aliases, node)) orelse return null;
                var match_owned = true;
                errdefer if (match_owned) match.deinit(self.alloc);
                try matches.append(self.alloc, match);
                match_owned = false;
                if (gq.params.max_results > 0 and matches.items.len >= gq.params.max_results) break;
            }
            if (gq.params.max_results > 0 and matches.items.len >= gq.params.max_results) break;
        }

        const owned_matches = try self.alloc.dupe(pattern_mod.PatternMatch, matches.items);
        matches_owned = false;
        matches.deinit(self.alloc);
        errdefer pattern_mod.freeMatches(self.alloc, owned_matches);
        const owned_nodes = try collectUniqueNodesFromMatches(self.alloc, owned_matches);
        return .{ .nodes = owned_nodes, .matches = owned_matches };
    }
};

/// Collect unique node keys from pattern match bindings into owned GraphResultNodes.
pub fn collectUniqueNodesFromMatches(
    alloc: Allocator,
    matches: []const pattern_mod.PatternMatch,
) ![]GraphResultNode {
    var seen = std.StringHashMapUnmanaged(void).empty;
    defer {
        var it = seen.keyIterator();
        while (it.next()) |k| alloc.free(k.*);
        seen.deinit(alloc);
    }

    var nodes = std.ArrayListUnmanaged(GraphResultNode).empty;
    errdefer {
        for (nodes.items) |*n| n.deinit(alloc);
        nodes.deinit(alloc);
    }

    for (matches) |m| {
        for (m.bindings) |binding| {
            if (seen.contains(binding.key)) continue;
            try seen.put(alloc, try alloc.dupe(u8, binding.key), {});
            try nodes.append(alloc, .{
                .key = try alloc.dupe(u8, binding.key),
                .depth = binding.depth,
                .distance = 0,
                .path = null,
                .path_edges = null,
            });
        }
    }

    const owned = try alloc.dupe(GraphResultNode, nodes.items);
    nodes.deinit(alloc);
    return owned;
}

const AlgebraicPatternPlan = struct {
    params: QueryParams,
    depth: u32,
};

fn algebraicPatternPlan(gq: GraphQuery) ?AlgebraicPatternPlan {
    if (gq.pattern.len < 2) return null;
    if (gq.target_nodes != null) return null;
    if (gq.params.weight_mode != .min_hops) return null;
    if (!gq.params.deduplicate) return null;
    if (gq.pattern[0].node_filter.filter_query_json != null) return null;
    if (!algebraicPatternAliasesUnique(gq.pattern)) return null;

    const first_edge = gq.pattern[1].edge;
    if (!algebraicPatternStepIsOneHop(first_edge)) return null;
    if (gq.pattern[1].node_filter.filter_query_json != null) return null;
    for (gq.pattern[2..]) |step| {
        if (step.node_filter.filter_query_json != null) return null;
        if (!algebraicPatternStepIsOneHop(step.edge)) return null;
        if (step.edge.direction != first_edge.direction) return null;
        if (step.edge.min_weight != first_edge.min_weight or step.edge.max_weight != first_edge.max_weight) return null;
        if (!stringSlicesEqual(step.edge.types, first_edge.types)) return null;
    }

    const depth = std.math.cast(u32, gq.pattern.len - 1) orelse return null;
    return .{
        .params = .{
            .edge_types = first_edge.types,
            .direction = first_edge.direction,
            .max_depth = depth,
            .max_results = gq.params.max_results,
            .min_weight = first_edge.min_weight,
            .max_weight = first_edge.max_weight,
            .deduplicate = true,
            .include_paths = true,
            .weight_mode = .min_hops,
            .algebraic_semiring = gq.params.algebraic_semiring,
        },
        .depth = depth,
    };
}

fn algebraicPatternStepIsOneHop(edge: pattern_mod.PatternEdgeStep) bool {
    const min_hops = if (edge.min_hops == 0) @as(u32, 1) else edge.min_hops;
    const max_hops = if (edge.max_hops == 0) @as(u32, 1) else edge.max_hops;
    return min_hops == 1 and max_hops == 1;
}

fn algebraicPatternAliasesUnique(pattern: []const pattern_mod.PatternStep) bool {
    var left_buf: [32]u8 = undefined;
    var right_buf: [32]u8 = undefined;
    for (pattern, 0..) |left, i| {
        const left_alias = graphQueryEffectiveAlias(left.alias, i, &left_buf);
        for (pattern[i + 1 ..], i + 1..) |right, j| {
            const right_alias = graphQueryEffectiveAlias(right.alias, j, &right_buf);
            if (std.mem.eql(u8, left_alias, right_alias)) return false;
        }
    }
    return true;
}

fn stringSlicesEqual(left: []const []const u8, right: []const []const u8) bool {
    if (left.len != right.len) return false;
    for (left, right) |a, b| {
        if (!std.mem.eql(u8, a, b)) return false;
    }
    return true;
}

fn graphQueryEffectiveAlias(alias: []const u8, step_idx: usize, buf: []u8) []const u8 {
    if (alias.len > 0) return alias;
    return std.fmt.bufPrint(buf, "_step{}", .{step_idx}) catch "_step";
}

fn graphQueryPassesPrefixFilter(key: []const u8, filter: pattern_mod.NodeFilter) bool {
    return filter.filter_prefix.len == 0 or std.mem.startsWith(u8, key, filter.filter_prefix);
}

fn algebraicPatternMatchFromNodeAlloc(
    alloc: Allocator,
    pattern: []const pattern_mod.PatternStep,
    return_aliases: []const []const u8,
    node: GraphResultNode,
) !?pattern_mod.PatternMatch {
    const node_path = node.path orelse return null;
    const edge_path = node.path_edges orelse return null;
    if (node_path.len != pattern.len or edge_path.len + 1 != node_path.len) return null;

    var all_bindings = try alloc.alloc(pattern_mod.PatternBinding, pattern.len);
    var initialized: usize = 0;
    defer {
        for (all_bindings[0..initialized]) |*binding| binding.deinit(alloc);
        if (all_bindings.len > 0) alloc.free(all_bindings);
    }

    for (pattern, 0..) |step, i| {
        if (!graphQueryPassesPrefixFilter(node_path[i], step.node_filter)) return null;
        var alias_buf: [32]u8 = undefined;
        const alias = graphQueryEffectiveAlias(step.alias, i, &alias_buf);
        all_bindings[i] = .{
            .alias = try alloc.dupe(u8, alias),
            .key = try alloc.dupe(u8, node_path[i]),
            .depth = std.math.cast(u32, i) orelse return null,
        };
        initialized += 1;
    }

    const filtered_bindings = try graphQueryFilterBindings(alloc, all_bindings, return_aliases);
    errdefer {
        for (filtered_bindings) |*binding| binding.deinit(alloc);
        if (filtered_bindings.len > 0) alloc.free(filtered_bindings);
    }
    return .{
        .bindings = filtered_bindings,
        .path = try clonePatternPathEdgesFromInfoAlloc(alloc, edge_path),
    };
}

fn graphQueryFilterBindings(
    alloc: Allocator,
    bindings: []const pattern_mod.PatternBinding,
    requested: []const []const u8,
) ![]pattern_mod.PatternBinding {
    var count: usize = 0;
    for (bindings) |binding| {
        if (graphQueryShouldReturnAlias(binding.alias, requested)) count += 1;
    }
    const filtered = try alloc.alloc(pattern_mod.PatternBinding, count);
    var out_idx: usize = 0;
    errdefer {
        for (filtered[0..out_idx]) |*binding| binding.deinit(alloc);
        if (filtered.len > 0) alloc.free(filtered);
    }
    for (bindings) |binding| {
        if (!graphQueryShouldReturnAlias(binding.alias, requested)) continue;
        filtered[out_idx] = .{
            .alias = try alloc.dupe(u8, binding.alias),
            .key = try alloc.dupe(u8, binding.key),
            .depth = binding.depth,
        };
        out_idx += 1;
    }
    return filtered;
}

fn graphQueryShouldReturnAlias(alias: []const u8, requested: []const []const u8) bool {
    if (requested.len == 0) return true;
    for (requested) |item| {
        if (std.mem.eql(u8, item, alias)) return true;
    }
    return false;
}

fn clonePatternPathEdgesFromInfoAlloc(alloc: Allocator, edges: []const PathEdgeInfo) ![]paths_mod.PathEdge {
    const out = try alloc.alloc(paths_mod.PathEdge, edges.len);
    var initialized: usize = 0;
    errdefer {
        freeGraphPatternPathEdgeItems(alloc, out[0..initialized]);
        if (out.len > 0) alloc.free(out);
    }
    for (edges, 0..) |edge, i| {
        out[i] = .{
            .source = try alloc.dupe(u8, edge.source),
            .target = try alloc.dupe(u8, edge.target),
            .edge_type = try alloc.dupe(u8, edge.edge_type),
            .weight = edge.weight,
        };
        initialized += 1;
    }
    return out;
}

fn freeGraphPatternPathEdgeItems(alloc: Allocator, edges: []const paths_mod.PathEdge) void {
    for (edges) |edge| {
        alloc.free(edge.source);
        alloc.free(edge.target);
        alloc.free(edge.edge_type);
    }
}

fn resolveTargetKeys(gq: GraphQuery) []const []const u8 {
    if (gq.target_nodes) |tn| {
        switch (tn) {
            .keys => |k| return k,
            .result_ref => return &.{}, // caller should have resolved
        }
    }
    return &.{};
}

fn pathToResultNode(alloc: Allocator, path: *const paths_mod.Path) !GraphResultNode {
    // Target is last node
    const target_key = if (path.nodes.len > 0) path.nodes[path.nodes.len - 1] else "";

    // Copy path nodes
    const path_nodes = try alloc.alloc([]const u8, path.nodes.len);
    errdefer alloc.free(path_nodes);
    for (path.nodes, 0..) |n, i| path_nodes[i] = try alloc.dupe(u8, n);

    // Copy path edges
    const path_edges = try alloc.alloc(PathEdgeInfo, path.edges.len);
    errdefer alloc.free(path_edges);
    for (path.edges, 0..) |e, i| {
        path_edges[i] = .{
            .source = try alloc.dupe(u8, e.source),
            .target = try alloc.dupe(u8, e.target),
            .edge_type = try alloc.dupe(u8, e.edge_type),
            .weight = e.weight,
        };
    }

    return .{
        .key = try alloc.dupe(u8, target_key),
        .depth = path.length,
        .distance = path.total_weight,
        .path = path_nodes,
        .path_edges = path_edges,
    };
}

fn trivialPathResultNode(alloc: Allocator, key: []const u8) !GraphResultNode {
    const nodes = try alloc.alloc([]const u8, 1);
    errdefer alloc.free(nodes);
    nodes[0] = try alloc.dupe(u8, key);
    errdefer alloc.free(nodes[0]);
    return .{
        .key = try alloc.dupe(u8, key),
        .depth = 0,
        .distance = 0,
        .path = nodes,
        .path_edges = try alloc.alloc(PathEdgeInfo, 0),
        .provenance = null,
    };
}

fn algebraicTraversalResultNodeAlloc(
    alloc: Allocator,
    result: algebraic_path_mod.PathResult,
) !GraphResultNode {
    const key = try alloc.dupe(u8, result.node);
    errdefer alloc.free(key);
    const provenance = try algebraic_path_mod.provenanceLabelsAlloc(alloc, result.provenance);
    errdefer freeProvenanceLabels(alloc, provenance);
    return .{
        .key = key,
        .depth = result.depth,
        .distance = @floatFromInt(result.depth),
        .path = null,
        .path_edges = null,
        .provenance = provenance,
    };
}

const ParsedProvenanceEdge = struct {
    source: []const u8,
    edge_type: []const u8,
    target: []const u8,
};

fn algebraicShortestPathResultNodeAlloc(
    alloc: Allocator,
    graph_index: *graph_mod.GraphIndex,
    params: QueryParams,
    start_key: []const u8,
    target_key: []const u8,
    result: algebraic_path_mod.PathResult,
) !?GraphResultNode {
    const labels = try algebraic_path_mod.provenanceLabelsAlloc(alloc, result.provenance);
    defer freeProvenanceLabels(alloc, labels);
    if (labels.len != result.depth) return null;

    const path_nodes = try alloc.alloc([]const u8, labels.len + 1);
    var path_node_count: usize = 0;
    var path_nodes_owned = true;
    defer if (path_nodes_owned) freePathNodeItems(alloc, path_nodes, path_node_count);

    const path_edges = try alloc.alloc(PathEdgeInfo, labels.len);
    var path_edge_count: usize = 0;
    var path_edges_owned = true;
    defer if (path_edges_owned) freePathEdgeItems(alloc, path_edges, path_edge_count);

    const used = try alloc.alloc(bool, labels.len);
    defer alloc.free(used);
    @memset(used, false);

    path_nodes[0] = try alloc.dupe(u8, start_key);
    path_node_count = 1;
    var current = path_nodes[0];

    for (0..labels.len) |_| {
        var match_index: ?usize = null;
        var match_edge: ParsedProvenanceEdge = undefined;
        for (labels, 0..) |label, i| {
            if (used[i]) continue;
            const parsed = parseProvenanceEdge(label) orelse return null;
            if (!provenanceEdgeCanAdvance(params.direction, current, parsed)) continue;
            if (match_index != null) return null;
            match_index = i;
            match_edge = parsed;
        }
        const index = match_index orelse return null;
        const next_key = provenanceEdgeNextNode(params.direction, current, match_edge) orelse return null;
        const weight = (try resolveUniqueGraphEdgeWeight(alloc, graph_index, current, match_edge, params.direction)) orelse return null;

        path_edges[path_edge_count] = .{
            .source = try alloc.dupe(u8, match_edge.source),
            .target = try alloc.dupe(u8, match_edge.target),
            .edge_type = try alloc.dupe(u8, match_edge.edge_type),
            .weight = weight,
        };
        path_edge_count += 1;

        path_nodes[path_node_count] = try alloc.dupe(u8, next_key);
        current = path_nodes[path_node_count];
        path_node_count += 1;
        used[index] = true;
    }

    if (!std.mem.eql(u8, current, target_key)) return null;

    const provenance = try cloneProvenanceLabelsAlloc(alloc, labels);
    errdefer freeProvenanceLabels(alloc, provenance);
    const key = try alloc.dupe(u8, target_key);
    errdefer alloc.free(key);

    return .{
        .key = key,
        .depth = result.depth,
        .distance = @floatFromInt(result.depth),
        .path = blk: {
            path_nodes_owned = false;
            break :blk path_nodes;
        },
        .path_edges = blk: {
            path_edges_owned = false;
            break :blk path_edges;
        },
        .provenance = provenance,
    };
}

fn parseProvenanceEdge(label: []const u8) ?ParsedProvenanceEdge {
    var it = std.mem.splitScalar(u8, label, 0x1f);
    const source = it.next() orelse return null;
    const edge_type = it.next() orelse return null;
    const target = it.next() orelse return null;
    if (it.next() != null) return null;
    return .{ .source = source, .edge_type = edge_type, .target = target };
}

fn provenanceEdgeCanAdvance(direction: graph_mod.EdgeDirection, current: []const u8, edge: ParsedProvenanceEdge) bool {
    return provenanceEdgeNextNode(direction, current, edge) != null;
}

fn provenanceEdgeNextNode(direction: graph_mod.EdgeDirection, current: []const u8, edge: ParsedProvenanceEdge) ?[]const u8 {
    return switch (direction) {
        .out => if (std.mem.eql(u8, current, edge.source)) edge.target else null,
        .in => if (std.mem.eql(u8, current, edge.target)) edge.source else null,
        .both => if (std.mem.eql(u8, current, edge.source)) edge.target else if (std.mem.eql(u8, current, edge.target)) edge.source else null,
    };
}

fn resolveUniqueGraphEdgeWeight(
    alloc: Allocator,
    graph_index: *graph_mod.GraphIndex,
    current: []const u8,
    provenance_edge: ParsedProvenanceEdge,
    direction: graph_mod.EdgeDirection,
) !?f64 {
    const edges = try graph_index.getEdges(alloc, current, "", direction);
    defer graph_mod.GraphIndex.freeEdges(alloc, edges);

    var found: ?f64 = null;
    for (edges) |edge| {
        if (!std.mem.eql(u8, edge.source, provenance_edge.source)) continue;
        if (!std.mem.eql(u8, edge.target, provenance_edge.target)) continue;
        if (!std.mem.eql(u8, edge.edge_type, provenance_edge.edge_type)) continue;
        if (found != null) return null;
        found = edge.weight;
    }
    return found;
}

fn cloneProvenanceLabelsAlloc(alloc: Allocator, labels: []const []const u8) ![][]u8 {
    const out = try alloc.alloc([]u8, labels.len);
    var initialized: usize = 0;
    errdefer {
        for (out[0..initialized]) |item| alloc.free(item);
        alloc.free(out);
    }
    for (labels, 0..) |label, i| {
        out[i] = try alloc.dupe(u8, label);
        initialized += 1;
    }
    return out;
}

fn freePathNodeItems(alloc: Allocator, nodes: []const []const u8, initialized: usize) void {
    for (nodes[0..initialized]) |node| alloc.free(node);
    alloc.free(nodes);
}

fn freePathEdgeItems(alloc: Allocator, edges: []const PathEdgeInfo, initialized: usize) void {
    for (edges[0..initialized]) |edge| {
        alloc.free(edge.source);
        alloc.free(edge.target);
        alloc.free(edge.edge_type);
    }
    alloc.free(edges);
}

fn freeResultNode(alloc: Allocator, node: GraphResultNode) void {
    alloc.free(node.key);
    if (node.table) |t| alloc.free(t);
    if (node.path) |p| {
        for (p) |s| alloc.free(s);
        alloc.free(p);
    }
    if (node.path_edges) |pe| {
        for (pe) |e| {
            alloc.free(e.source);
            alloc.free(e.target);
            alloc.free(e.edge_type);
        }
        alloc.free(pe);
    }
    if (node.provenance) |items| freeProvenanceLabels(alloc, items);
}

fn freeProvenanceLabels(alloc: Allocator, labels: []const []const u8) void {
    for (labels) |label| alloc.free(label);
    alloc.free(labels);
}

const AlgebraicReachabilityEdges = struct {
    items: []algebraic_path_mod.Edge,

    fn deinit(self: *@This(), alloc: Allocator) void {
        for (self.items) |edge| {
            alloc.free(edge.from);
            alloc.free(edge.to);
            alloc.free(edge.provenance);
        }
        if (self.items.len > 0) alloc.free(self.items);
        self.* = .{ .items = &.{} };
    }
};

const AlgebraicQueueEntry = struct {
    key: []u8,
    depth: u32,
};

fn collectAlgebraicReachabilityEdges(
    alloc: Allocator,
    graph_index: *graph_mod.GraphIndex,
    start_key: []const u8,
    params: QueryParams,
) !AlgebraicReachabilityEdges {
    var edges = std.ArrayListUnmanaged(algebraic_path_mod.Edge).empty;
    errdefer {
        for (edges.items) |edge| {
            alloc.free(edge.from);
            alloc.free(edge.to);
            alloc.free(edge.provenance);
        }
        edges.deinit(alloc);
    }

    var queue = std.ArrayListUnmanaged(AlgebraicQueueEntry).empty;
    defer {
        for (queue.items) |entry| alloc.free(entry.key);
        queue.deinit(alloc);
    }
    var queue_head: usize = 0;

    var visited = std.StringHashMapUnmanaged(void).empty;
    defer {
        var it = visited.keyIterator();
        while (it.next()) |key| alloc.free(key.*);
        visited.deinit(alloc);
    }

    try queue.append(alloc, .{
        .key = try alloc.dupe(u8, start_key),
        .depth = 0,
    });
    try visited.put(alloc, try alloc.dupe(u8, start_key), {});

    while (queue_head < queue.items.len) {
        const current = queue.items[queue_head];
        queue_head += 1;
        if (current.depth >= params.max_depth) continue;

        const graph_edges = try graph_index.getEdges(alloc, current.key, "", params.direction);
        defer graph_mod.GraphIndex.freeEdges(alloc, graph_edges);
        for (graph_edges) |edge| {
            if (!graphEdgeTypeAllowed(params.edge_types, edge.edge_type)) continue;
            if (!graphEdgeWeightAllowed(params, edge.weight)) continue;
            const next_key = if (std.mem.eql(u8, current.key, edge.source)) edge.target else edge.source;
            if (std.mem.eql(u8, next_key, start_key)) continue;
            const already_visited = visited.contains(next_key);
            if (!already_visited) try visited.put(alloc, try alloc.dupe(u8, next_key), {});

            const provenance_label = try std.fmt.allocPrint(alloc, "{s}\x1f{s}\x1f{s}", .{ edge.source, edge.edge_type, edge.target });
            defer alloc.free(provenance_label);
            const provenance = try algebraic_path_mod.provenanceTokenAlloc(alloc, &.{provenance_label});
            var provenance_owned = true;
            errdefer if (provenance_owned) alloc.free(provenance);
            const from = try alloc.dupe(u8, current.key);
            var from_owned = true;
            errdefer if (from_owned) alloc.free(from);
            const to = try alloc.dupe(u8, next_key);
            var to_owned = true;
            errdefer if (to_owned) alloc.free(to);

            try edges.append(alloc, .{
                .from = from,
                .to = to,
                .provenance = provenance,
            });
            provenance_owned = false;
            from_owned = false;
            to_owned = false;
            if (!already_visited) {
                try queue.append(alloc, .{
                    .key = try alloc.dupe(u8, next_key),
                    .depth = current.depth + 1,
                });
            }
        }
    }

    const owned = try edges.toOwnedSlice(alloc);
    return .{ .items = owned };
}

fn algebraicTraversalTensorProgramAccepted(
    alloc: Allocator,
    graph_index: *const graph_mod.GraphIndex,
    params: QueryParams,
    target_keys: []const []const u8,
) !bool {
    if (!algebraicTraversalProof(graph_index, params).safe()) return false;

    var plan = (try algebraic_planner.planGraphTraversalTensorProgramAlloc(alloc, graph_index.index_name, target_keys.len > 0)) orelse return false;
    defer plan.deinit(alloc);
    return algebraic_ir.graphTraversalProgramMatchesTarget(plan.asProgram(), graph_index.index_name, target_keys.len > 0) and
        (try algebraic_ir.tensorProgramProof(alloc, plan.access_paths, plan.asProgram())).safe();
}

fn graphEdgeTypeAllowed(allowed: []const []const u8, edge_type: []const u8) bool {
    if (allowed.len == 0) return true;
    for (allowed) |item| {
        if (std.mem.eql(u8, item, edge_type)) return true;
    }
    return false;
}

fn graphEdgeWeightAllowed(params: QueryParams, weight: f64) bool {
    if (params.min_weight > 0 and weight < params.min_weight) return false;
    if (params.max_weight > 0 and weight > params.max_weight) return false;
    return true;
}

fn targetKeyAllowed(target_keys: []const []const u8, key: []const u8) bool {
    if (target_keys.len == 0) return true;
    for (target_keys) |target_key| {
        if (std.mem.eql(u8, target_key, key)) return true;
    }
    return false;
}

// ============================================================================
// Tests
// ============================================================================

const docstore = @import("../storage/docstore.zig");

fn tmpPath(buf: []u8, label: []const u8) [*:0]const u8 {
    const ns = platform_time.monotonicNs();
    const slice = std.fmt.bufPrint(buf, "/tmp/antfly-gq-{s}-{d}\x00", .{ label, ns }) catch unreachable;
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

const TestCtx = struct {
    store: docstore.DocStore,
    graph: graph_mod.GraphIndex,
    sp: [*:0]const u8,
    rp: [*:0]const u8,

    fn deinit(self: *TestCtx) void {
        self.graph.close();
        self.store.close();
        cleanupTmp(self.sp);
        cleanupTmp(self.rp);
    }
};

fn setupGraph(alloc: Allocator, store_label: []const u8, rev_label: []const u8, sb: *[256]u8, rb: *[256]u8) !*TestCtx {
    const sp = tmpPath(sb, store_label);
    const rp = tmpPath(rb, rev_label);
    const ctx = try alloc.create(TestCtx);
    errdefer alloc.destroy(ctx);
    ctx.store = try docstore.DocStore.open(alloc, sp, .{});
    errdefer ctx.store.close();
    ctx.graph = try graph_mod.GraphIndex.open(alloc, &ctx.store, rp, "test", .{});
    ctx.sp = sp;
    ctx.rp = rp;
    return ctx;
}

fn expectAlgebraicTraversalReject(proof: AlgebraicTraversalProof, reason: AlgebraicTraversalRejectReason) !void {
    switch (proof) {
        .proven => return error.TestExpectedEqual,
        .rejected => |actual| try std.testing.expectEqual(reason, actual),
    }
}

// --- Phase 18A tests ---

test "GraphQuery struct construction" {
    const gq_traverse = GraphQuery{
        .query_type = .traverse,
        .index_name = "links",
        .start_nodes = .{ .keys = &.{ "doc1", "doc2" } },
    };
    try std.testing.expectEqual(QueryType.traverse, gq_traverse.query_type);
    try std.testing.expectEqual(@as(u32, 3), gq_traverse.params.max_depth);

    const gq_neighbors = GraphQuery{
        .query_type = .neighbors,
        .index_name = "links",
        .start_nodes = .{ .keys = &.{"doc1"} },
    };
    try std.testing.expectEqual(QueryType.neighbors, gq_neighbors.query_type);

    const gq_sp = GraphQuery{
        .query_type = .shortest_path,
        .index_name = "links",
        .start_nodes = .{ .keys = &.{"A"} },
        .target_nodes = .{ .keys = &.{"C"} },
    };
    try std.testing.expectEqual(QueryType.shortest_path, gq_sp.query_type);

    const gq_ksp = GraphQuery{
        .query_type = .k_shortest_paths,
        .index_name = "links",
        .start_nodes = .{ .keys = &.{"A"} },
        .target_nodes = .{ .keys = &.{"C"} },
        .k = 3,
    };
    try std.testing.expectEqual(@as(u32, 3), gq_ksp.k);
}

test "NodeSelector keys vs result_ref" {
    const keys_sel = NodeSelector{ .keys = &.{ "a", "b", "c" } };
    switch (keys_sel) {
        .keys => |k| try std.testing.expectEqual(@as(usize, 3), k.len),
        .result_ref => unreachable,
    }

    const ref_sel = NodeSelector{ .result_ref = .{ .ref = "$full_text_results", .limit = 10 } };
    switch (ref_sel) {
        .keys => unreachable,
        .result_ref => |r| {
            try std.testing.expectEqualStrings("$full_text_results", r.ref);
            try std.testing.expectEqual(@as(u32, 10), r.limit);
        },
    }
}

test "QueryParams defaults match TraversalRules defaults" {
    const qp = QueryParams{};
    const tr = traversal_mod.TraversalRules{};
    try std.testing.expectEqual(qp.direction, tr.direction);
    try std.testing.expectEqual(qp.max_depth, tr.max_depth);
    try std.testing.expectEqual(qp.min_weight, tr.min_weight);
    try std.testing.expectEqual(qp.max_weight, tr.max_weight);
    try std.testing.expectEqual(qp.max_results, tr.max_results);
    try std.testing.expectEqual(qp.deduplicate, tr.deduplicate);
    try std.testing.expectEqual(qp.include_paths, tr.include_paths);
}

// --- Phase 18B tests ---

test "traverse: multi-start with depth 2" {
    const alloc = std.testing.allocator;
    var sb: [256]u8 = undefined;
    var rb: [256]u8 = undefined;
    const ctx = try setupGraph(alloc, "gq1s", "gq1r", &sb, &rb);
    defer {
        ctx.deinit();
        alloc.destroy(ctx);
    }

    // A -> B -> D, A -> C, X -> Y -> Z
    try ctx.graph.addEdge("A", "B", "e", 1.0, 0, 0, "");
    try ctx.graph.addEdge("B", "D", "e", 1.0, 0, 0, "");
    try ctx.graph.addEdge("A", "C", "e", 1.0, 0, 0, "");
    try ctx.graph.addEdge("X", "Y", "e", 1.0, 0, 0, "");
    try ctx.graph.addEdge("Y", "Z", "e", 1.0, 0, 0, "");

    var engine = GraphQueryEngine{ .alloc = alloc };
    const start_keys: []const []const u8 = &.{ "A", "X" };
    var result = try engine.execute(&ctx.graph, .{
        .query_type = .traverse,
        .index_name = "test",
        .start_nodes = .{ .keys = start_keys },
        .params = .{ .max_depth = 2 },
    }, start_keys);
    defer result.deinit(alloc);

    // Should find B, C, D (from A) + Y, Z (from X) = 5 nodes
    try std.testing.expectEqual(@as(usize, 5), result.nodes.len);
}

test "traverse can execute through algebraic provenance semiring path" {
    const alloc = std.testing.allocator;
    var sb: [256]u8 = undefined;
    var rb: [256]u8 = undefined;
    const ctx = try setupGraph(alloc, "gq-alg-path-s", "gq-alg-path-r", &sb, &rb);
    defer {
        ctx.deinit();
        alloc.destroy(ctx);
    }

    try ctx.graph.addEdge("A", "B", "e", 1.0, 0, 0, "");
    try ctx.graph.addEdge("B", "D", "e", 1.0, 0, 0, "");
    try ctx.graph.addEdge("A", "C", "e", 1.0, 0, 0, "");
    try ctx.graph.addEdge("C", "D", "e", 1.0, 0, 0, "");

    var engine = GraphQueryEngine{ .alloc = alloc };
    const start_keys: []const []const u8 = &.{"A"};
    var result = try engine.execute(&ctx.graph, .{
        .query_type = .traverse,
        .index_name = "test",
        .start_nodes = .{ .keys = start_keys },
        .params = .{
            .max_depth = 2,
            .max_results = 0,
            .algebraic_semiring = true,
        },
    }, start_keys);
    defer result.deinit(alloc);

    try std.testing.expectEqual(@as(usize, 3), result.nodes.len);
    try std.testing.expectEqualStrings("B", result.nodes[0].key);
    try std.testing.expectEqual(@as(u32, 1), result.nodes[0].depth);
    try std.testing.expectEqualStrings("C", result.nodes[1].key);
    try std.testing.expectEqual(@as(u32, 1), result.nodes[1].depth);
    try std.testing.expectEqualStrings("D", result.nodes[2].key);
    try std.testing.expectEqual(@as(u32, 2), result.nodes[2].depth);
    const d_provenance = result.nodes[2].provenance orelse return error.TestExpectedEqual;
    try std.testing.expectEqual(@as(usize, 4), d_provenance.len);
    try std.testing.expectEqualStrings("A\x1fe\x1fB", d_provenance[0]);
    try std.testing.expectEqualStrings("A\x1fe\x1fC", d_provenance[1]);
    try std.testing.expectEqualStrings("B\x1fe\x1fD", d_provenance[2]);
    try std.testing.expectEqualStrings("C\x1fe\x1fD", d_provenance[3]);
}

test "traverse auto-selects algebraic semiring path from graph index config" {
    const alloc = std.testing.allocator;
    var sb: [256]u8 = undefined;
    var rb: [256]u8 = undefined;
    const ctx = try setupGraph(alloc, "gq-alg-config-s", "gq-alg-config-r", &sb, &rb);
    defer {
        ctx.deinit();
        alloc.destroy(ctx);
    }

    ctx.graph.algebraic_semiring_traversal = true;
    try ctx.graph.addEdge("A", "B", "e", 1.0, 0, 0, "");
    try ctx.graph.addEdge("B", "C", "e", 1.0, 0, 0, "");

    var engine = GraphQueryEngine{ .alloc = alloc };
    const start_keys: []const []const u8 = &.{"A"};
    var result = try engine.execute(&ctx.graph, .{
        .query_type = .traverse,
        .index_name = "test",
        .start_nodes = .{ .keys = start_keys },
        .params = .{ .max_depth = 2, .max_results = 0 },
    }, start_keys);
    defer result.deinit(alloc);

    try std.testing.expectEqual(@as(usize, 2), result.nodes.len);
    try std.testing.expectEqualStrings("B", result.nodes[0].key);
    try std.testing.expectEqualStrings("C", result.nodes[1].key);
}

test "traverse algebraic semiring path respects target nodes" {
    const alloc = std.testing.allocator;
    var sb: [256]u8 = undefined;
    var rb: [256]u8 = undefined;
    const ctx = try setupGraph(alloc, "gq-alg-target-s", "gq-alg-target-r", &sb, &rb);
    defer {
        ctx.deinit();
        alloc.destroy(ctx);
    }

    ctx.graph.algebraic_semiring_traversal = true;
    try std.testing.expect(try algebraicTraversalTensorProgramAccepted(alloc, &ctx.graph, .{ .max_depth = 2, .max_results = 0 }, &.{"C"}));
    try ctx.graph.addEdge("A", "B", "e", 1.0, 0, 0, "");
    try ctx.graph.addEdge("B", "C", "e", 1.0, 0, 0, "");
    try ctx.graph.addEdge("A", "D", "e", 1.0, 0, 0, "");

    var engine = GraphQueryEngine{ .alloc = alloc };
    const start_keys: []const []const u8 = &.{"A"};
    const target_keys: []const []const u8 = &.{"C"};
    var result = try engine.execute(&ctx.graph, .{
        .query_type = .traverse,
        .index_name = "test",
        .start_nodes = .{ .keys = start_keys },
        .target_nodes = .{ .keys = target_keys },
        .params = .{ .max_depth = 2, .max_results = 0 },
    }, start_keys);
    defer result.deinit(alloc);

    try std.testing.expectEqual(@as(usize, 1), result.nodes.len);
    try std.testing.expectEqualStrings("C", result.nodes[0].key);
    try std.testing.expectEqual(@as(u32, 2), result.nodes[0].depth);
    const provenance = result.nodes[0].provenance orelse return error.TestExpectedEqual;
    try std.testing.expectEqual(@as(usize, 2), provenance.len);
    try std.testing.expectEqualStrings("A\x1fe\x1fB", provenance[0]);
    try std.testing.expectEqualStrings("B\x1fe\x1fC", provenance[1]);
}

test "traverse algebraic semiring path supports inbound traversal" {
    const alloc = std.testing.allocator;
    var sb: [256]u8 = undefined;
    var rb: [256]u8 = undefined;
    const ctx = try setupGraph(alloc, "gq-alg-in-s", "gq-alg-in-r", &sb, &rb);
    defer {
        ctx.deinit();
        alloc.destroy(ctx);
    }

    ctx.graph.algebraic_semiring_traversal = true;
    try ctx.graph.addEdge("A", "B", "e", 1.0, 0, 0, "");
    try ctx.graph.addEdge("B", "C", "e", 1.0, 0, 0, "");
    try ctx.graph.addEdge("D", "C", "e", 1.0, 0, 0, "");

    var engine = GraphQueryEngine{ .alloc = alloc };
    const start_keys: []const []const u8 = &.{"C"};
    var result = try engine.execute(&ctx.graph, .{
        .query_type = .traverse,
        .index_name = "test",
        .start_nodes = .{ .keys = start_keys },
        .params = .{
            .direction = .in,
            .max_depth = 2,
            .max_results = 0,
        },
    }, start_keys);
    defer result.deinit(alloc);

    try std.testing.expectEqual(@as(usize, 3), result.nodes.len);
    try std.testing.expectEqualStrings("B", result.nodes[0].key);
    try std.testing.expectEqual(@as(u32, 1), result.nodes[0].depth);
    try std.testing.expectEqualStrings("D", result.nodes[1].key);
    try std.testing.expectEqual(@as(u32, 1), result.nodes[1].depth);
    try std.testing.expectEqualStrings("A", result.nodes[2].key);
    try std.testing.expectEqual(@as(u32, 2), result.nodes[2].depth);
    const provenance = result.nodes[2].provenance orelse return error.TestExpectedEqual;
    try std.testing.expectEqual(@as(usize, 2), provenance.len);
    try std.testing.expectEqualStrings("A\x1fe\x1fB", provenance[0]);
    try std.testing.expectEqualStrings("B\x1fe\x1fC", provenance[1]);
}

test "traverse algebraic semiring path supports bidirectional traversal" {
    const alloc = std.testing.allocator;
    var sb: [256]u8 = undefined;
    var rb: [256]u8 = undefined;
    const ctx = try setupGraph(alloc, "gq-alg-both-s", "gq-alg-both-r", &sb, &rb);
    defer {
        ctx.deinit();
        alloc.destroy(ctx);
    }

    ctx.graph.algebraic_semiring_traversal = true;
    try ctx.graph.addEdge("A", "B", "e", 1.0, 0, 0, "");
    try ctx.graph.addEdge("C", "A", "e", 1.0, 0, 0, "");

    var engine = GraphQueryEngine{ .alloc = alloc };
    const start_keys: []const []const u8 = &.{"A"};
    var result = try engine.execute(&ctx.graph, .{
        .query_type = .traverse,
        .index_name = "test",
        .start_nodes = .{ .keys = start_keys },
        .params = .{
            .direction = .both,
            .max_depth = 1,
            .max_results = 0,
        },
    }, start_keys);
    defer result.deinit(alloc);

    try std.testing.expectEqual(@as(usize, 2), result.nodes.len);
    var saw_b = false;
    var saw_c = false;
    for (result.nodes) |node| {
        try std.testing.expectEqual(@as(u32, 1), node.depth);
        const provenance = node.provenance orelse return error.TestExpectedEqual;
        try std.testing.expectEqual(@as(usize, 1), provenance.len);
        if (std.mem.eql(u8, node.key, "B")) {
            saw_b = true;
            try std.testing.expectEqualStrings("A\x1fe\x1fB", provenance[0]);
        } else if (std.mem.eql(u8, node.key, "C")) {
            saw_c = true;
            try std.testing.expectEqualStrings("C\x1fe\x1fA", provenance[0]);
        }
    }
    try std.testing.expect(saw_b);
    try std.testing.expect(saw_c);
    const algebraic_stats = ctx.graph.algebraicTraversalRuntimeStats();
    try std.testing.expectEqual(@as(u64, 1), algebraic_stats.attempt_count);
    try std.testing.expectEqual(@as(u64, 1), algebraic_stats.proven_count);
    try std.testing.expectEqual(@as(u64, 0), algebraic_stats.rejected_count);
    try std.testing.expectEqual(@as(u64, 0), algebraic_stats.fallback_count);
    try std.testing.expectEqual(@as(u64, 2), algebraic_stats.result_node_count);
}

test "algebraic traversal proof gates exact semiring shapes" {
    const alloc = std.testing.allocator;
    var sb: [256]u8 = undefined;
    var rb: [256]u8 = undefined;
    const ctx = try setupGraph(alloc, "gq-alg-proof-s", "gq-alg-proof-r", &sb, &rb);
    defer {
        ctx.deinit();
        alloc.destroy(ctx);
    }

    try expectAlgebraicTraversalReject(algebraicTraversalProof(&ctx.graph, .{}), .disabled);
    ctx.graph.algebraic_semiring_traversal = true;
    try std.testing.expect(algebraicTraversalProof(&ctx.graph, .{}).safe());
    try std.testing.expect(algebraicTraversalProof(&ctx.graph, .{ .max_results = 0 }).safe());
    try std.testing.expect(algebraicTraversalProof(&ctx.graph, .{ .include_paths = true, .max_results = 0 }).safe());
    try expectAlgebraicTraversalReject(algebraicTraversalProof(&ctx.graph, .{ .deduplicate = false, .max_results = 0 }), .non_deduplicated);
    try expectAlgebraicTraversalReject(algebraicTraversalProof(&ctx.graph, .{ .max_depth = 0, .max_results = 0 }), .zero_depth);
    try std.testing.expect(algebraicTraversalProof(&ctx.graph, .{ .min_weight = 0.5, .max_results = 0 }).safe());
    try std.testing.expect(algebraicTraversalProof(&ctx.graph, .{ .max_weight = 2.5, .max_results = 0 }).safe());
    try expectAlgebraicTraversalReject(algebraicTraversalProof(&ctx.graph, .{ .weight_mode = .min_weight, .max_results = 0 }), .weighted_mode);
    try std.testing.expect(algebraicTraversalProof(&ctx.graph, .{ .direction = .in, .max_results = 0 }).safe());
    try std.testing.expect(algebraicTraversalProof(&ctx.graph, .{ .direction = .both, .max_results = 0 }).safe());
}

test "traverse algebraic semiring path applies exact edge weight filters" {
    const alloc = std.testing.allocator;
    var sb: [256]u8 = undefined;
    var rb: [256]u8 = undefined;
    const ctx = try setupGraph(alloc, "gq-alg-weight-filter-s", "gq-alg-weight-filter-r", &sb, &rb);
    defer {
        ctx.deinit();
        alloc.destroy(ctx);
    }

    ctx.graph.algebraic_semiring_traversal = true;
    try ctx.graph.addEdge("A", "B", "fast", 0.5, 0, 0, "");
    try ctx.graph.addEdge("A", "C", "ok", 2.0, 0, 0, "");
    try ctx.graph.addEdge("C", "D", "ok", 2.0, 0, 0, "");
    try ctx.graph.addEdge("A", "E", "slow", 5.0, 0, 0, "");

    var engine = GraphQueryEngine{ .alloc = alloc };
    const start_keys: []const []const u8 = &.{"A"};
    var result = try engine.execute(&ctx.graph, .{
        .query_type = .traverse,
        .index_name = "test",
        .start_nodes = .{ .keys = start_keys },
        .params = .{
            .max_depth = 2,
            .max_results = 0,
            .min_weight = 1.0,
            .max_weight = 3.0,
        },
    }, start_keys);
    defer result.deinit(alloc);

    try std.testing.expectEqual(@as(usize, 2), result.nodes.len);
    try std.testing.expectEqualStrings("C", result.nodes[0].key);
    try std.testing.expectEqual(@as(u32, 1), result.nodes[0].depth);
    try std.testing.expectEqualStrings("D", result.nodes[1].key);
    try std.testing.expectEqual(@as(u32, 2), result.nodes[1].depth);
    const d_provenance = result.nodes[1].provenance orelse return error.TestExpectedEqual;
    try std.testing.expectEqual(@as(usize, 2), d_provenance.len);
    try std.testing.expectEqualStrings("A\x1fok\x1fC", d_provenance[0]);
    try std.testing.expectEqualStrings("C\x1fok\x1fD", d_provenance[1]);
}

test "traverse algebraic semiring path supports deterministic result limits" {
    const alloc = std.testing.allocator;
    var sb: [256]u8 = undefined;
    var rb: [256]u8 = undefined;
    const ctx = try setupGraph(alloc, "gq-alg-limit-s", "gq-alg-limit-r", &sb, &rb);
    defer {
        ctx.deinit();
        alloc.destroy(ctx);
    }

    ctx.graph.algebraic_semiring_traversal = true;
    try ctx.graph.addEdge("A", "C", "e", 1.0, 0, 0, "");
    try ctx.graph.addEdge("A", "B", "e", 1.0, 0, 0, "");
    try ctx.graph.addEdge("B", "D", "e", 1.0, 0, 0, "");
    try ctx.graph.addEdge("C", "E", "e", 1.0, 0, 0, "");

    var engine = GraphQueryEngine{ .alloc = alloc };
    const start_keys: []const []const u8 = &.{"A"};
    var result = try engine.execute(&ctx.graph, .{
        .query_type = .traverse,
        .index_name = "test",
        .start_nodes = .{ .keys = start_keys },
        .params = .{
            .max_depth = 2,
            .max_results = 2,
        },
    }, start_keys);
    defer result.deinit(alloc);

    try std.testing.expectEqual(@as(usize, 2), result.nodes.len);
    try std.testing.expectEqualStrings("B", result.nodes[0].key);
    try std.testing.expectEqualStrings("C", result.nodes[1].key);
    try std.testing.expect(result.nodes[0].provenance != null);
    try std.testing.expect(result.nodes[1].provenance != null);
}

test "algebraic traversal reconstructs path-returning shapes when provenance is unique" {
    const alloc = std.testing.allocator;
    var sb: [256]u8 = undefined;
    var rb: [256]u8 = undefined;
    const ctx = try setupGraph(alloc, "gq-alg-path-return-s", "gq-alg-path-return-r", &sb, &rb);
    defer {
        ctx.deinit();
        alloc.destroy(ctx);
    }

    ctx.graph.algebraic_semiring_traversal = true;
    try ctx.graph.addEdge("A", "B", "e", 1.0, 0, 0, "");
    try ctx.graph.addEdge("B", "C", "e", 1.0, 0, 0, "");

    var engine = GraphQueryEngine{ .alloc = alloc };
    const start_keys: []const []const u8 = &.{"A"};
    var result = try engine.execute(&ctx.graph, .{
        .query_type = .traverse,
        .index_name = "test",
        .start_nodes = .{ .keys = start_keys },
        .params = .{
            .max_depth = 2,
            .include_paths = true,
        },
    }, start_keys);
    defer result.deinit(alloc);

    try std.testing.expectEqual(@as(usize, 2), result.nodes.len);
    try std.testing.expectEqualStrings("B", result.nodes[0].key);
    try std.testing.expect(result.nodes[0].path != null);
    try std.testing.expect(result.nodes[0].path_edges != null);
    try std.testing.expect(result.nodes[0].provenance != null);
    try std.testing.expectEqualStrings("C", result.nodes[1].key);
    const c_path = result.nodes[1].path orelse return error.TestExpectedEqual;
    try std.testing.expectEqual(@as(usize, 3), c_path.len);
    try std.testing.expectEqualStrings("A", c_path[0]);
    try std.testing.expectEqualStrings("B", c_path[1]);
    try std.testing.expectEqualStrings("C", c_path[2]);
    const c_edges = result.nodes[1].path_edges orelse return error.TestExpectedEqual;
    try std.testing.expectEqual(@as(usize, 2), c_edges.len);
    try std.testing.expectEqualStrings("A", c_edges[0].source);
    try std.testing.expectEqualStrings("B", c_edges[0].target);
    try std.testing.expectEqualStrings("B", c_edges[1].source);
    try std.testing.expectEqualStrings("C", c_edges[1].target);
}

test "algebraic traversal falls back for ambiguous path-returning provenance" {
    const alloc = std.testing.allocator;
    var sb: [256]u8 = undefined;
    var rb: [256]u8 = undefined;
    const ctx = try setupGraph(alloc, "gq-alg-path-ambig-s", "gq-alg-path-ambig-r", &sb, &rb);
    defer {
        ctx.deinit();
        alloc.destroy(ctx);
    }

    ctx.graph.algebraic_semiring_traversal = true;
    try ctx.graph.addEdge("A", "B", "e", 1.0, 0, 0, "");
    try ctx.graph.addEdge("A", "C", "e", 1.0, 0, 0, "");
    try ctx.graph.addEdge("B", "D", "e", 1.0, 0, 0, "");
    try ctx.graph.addEdge("C", "D", "e", 1.0, 0, 0, "");

    var engine = GraphQueryEngine{ .alloc = alloc };
    const start_keys: []const []const u8 = &.{"A"};
    var result = try engine.execute(&ctx.graph, .{
        .query_type = .traverse,
        .index_name = "test",
        .start_nodes = .{ .keys = start_keys },
        .params = .{
            .max_depth = 2,
            .include_paths = true,
        },
    }, start_keys);
    defer result.deinit(alloc);

    try std.testing.expectEqual(@as(usize, 3), result.nodes.len);
    for (result.nodes) |node| {
        try std.testing.expect(node.path != null);
        try std.testing.expect(node.provenance == null);
    }
}

test "neighbors: only 1-hop" {
    const alloc = std.testing.allocator;
    var sb: [256]u8 = undefined;
    var rb: [256]u8 = undefined;
    const ctx = try setupGraph(alloc, "gq2s", "gq2r", &sb, &rb);
    defer {
        ctx.deinit();
        alloc.destroy(ctx);
    }

    try ctx.graph.addEdge("A", "B", "e", 1.0, 0, 0, "");
    try ctx.graph.addEdge("B", "C", "e", 1.0, 0, 0, "");
    try ctx.graph.addEdge("A", "D", "e", 1.0, 0, 0, "");

    var engine = GraphQueryEngine{ .alloc = alloc };
    const start_keys: []const []const u8 = &.{"A"};
    var result = try engine.execute(&ctx.graph, .{
        .query_type = .neighbors,
        .index_name = "test",
        .start_nodes = .{ .keys = start_keys },
    }, start_keys);
    defer result.deinit(alloc);

    // Only B and D (1-hop), not C (2-hop)
    try std.testing.expectEqual(@as(usize, 2), result.nodes.len);
    for (result.nodes) |node| {
        try std.testing.expectEqual(@as(u32, 1), node.depth);
    }
}

test "shortest_path via engine" {
    const alloc = std.testing.allocator;
    var sb: [256]u8 = undefined;
    var rb: [256]u8 = undefined;
    const ctx = try setupGraph(alloc, "gq3s", "gq3r", &sb, &rb);
    defer {
        ctx.deinit();
        alloc.destroy(ctx);
    }

    try ctx.graph.addEdge("A", "B", "e", 1.0, 0, 0, "");
    try ctx.graph.addEdge("B", "C", "e", 1.0, 0, 0, "");
    try ctx.graph.addEdge("A", "C", "e", 10.0, 0, 0, "");

    var engine = GraphQueryEngine{ .alloc = alloc };
    const start_keys: []const []const u8 = &.{"A"};
    var result = try engine.execute(&ctx.graph, .{
        .query_type = .shortest_path,
        .index_name = "test",
        .start_nodes = .{ .keys = start_keys },
        .target_nodes = .{ .keys = &.{"C"} },
        .params = .{ .weight_mode = .min_weight },
    }, start_keys);
    defer result.deinit(alloc);

    try std.testing.expectEqual(@as(usize, 1), result.nodes.len);
    try std.testing.expectEqualStrings("C", result.nodes[0].key);
    try std.testing.expectEqual(@as(u32, 2), result.nodes[0].depth); // A->B->C (weight 2 < 10)
    try std.testing.expect(result.nodes[0].path != null);
    try std.testing.expectEqual(@as(usize, 3), result.nodes[0].path.?.len);
}

test "shortest_path can execute through algebraic provenance semiring for unique min-hop path" {
    const alloc = std.testing.allocator;
    var sb: [256]u8 = undefined;
    var rb: [256]u8 = undefined;
    const ctx = try setupGraph(alloc, "gq-alg-shortest-s", "gq-alg-shortest-r", &sb, &rb);
    defer {
        ctx.deinit();
        alloc.destroy(ctx);
    }

    ctx.graph.algebraic_semiring_traversal = true;
    try ctx.graph.addEdge("A", "B", "e", 2.0, 0, 0, "");
    try ctx.graph.addEdge("B", "C", "e", 3.0, 0, 0, "");

    var engine = GraphQueryEngine{ .alloc = alloc };
    const start_keys: []const []const u8 = &.{"A"};
    var result = try engine.execute(&ctx.graph, .{
        .query_type = .shortest_path,
        .index_name = "test",
        .start_nodes = .{ .keys = start_keys },
        .target_nodes = .{ .keys = &.{"C"} },
        .params = .{ .max_depth = 3 },
    }, start_keys);
    defer result.deinit(alloc);

    try std.testing.expectEqual(@as(usize, 1), result.nodes.len);
    try std.testing.expectEqualStrings("C", result.nodes[0].key);
    try std.testing.expectEqual(@as(u32, 2), result.nodes[0].depth);
    try std.testing.expectEqual(@as(f64, 2.0), result.nodes[0].distance);
    const path = result.nodes[0].path orelse return error.TestExpectedEqual;
    try std.testing.expectEqual(@as(usize, 3), path.len);
    try std.testing.expectEqualStrings("A", path[0]);
    try std.testing.expectEqualStrings("B", path[1]);
    try std.testing.expectEqualStrings("C", path[2]);
    const path_edges = result.nodes[0].path_edges orelse return error.TestExpectedEqual;
    try std.testing.expectEqual(@as(usize, 2), path_edges.len);
    try std.testing.expectEqual(@as(f64, 2.0), path_edges[0].weight);
    try std.testing.expectEqual(@as(f64, 3.0), path_edges[1].weight);
    const provenance = result.nodes[0].provenance orelse return error.TestExpectedEqual;
    try std.testing.expectEqual(@as(usize, 2), provenance.len);
    try std.testing.expectEqualStrings("A\x1fe\x1fB", provenance[0]);
    try std.testing.expectEqualStrings("B\x1fe\x1fC", provenance[1]);
}

test "shortest_path falls back when algebraic provenance is ambiguous" {
    const alloc = std.testing.allocator;
    var sb: [256]u8 = undefined;
    var rb: [256]u8 = undefined;
    const ctx = try setupGraph(alloc, "gq-alg-shortest-fallback-s", "gq-alg-shortest-fallback-r", &sb, &rb);
    defer {
        ctx.deinit();
        alloc.destroy(ctx);
    }

    ctx.graph.algebraic_semiring_traversal = true;
    try ctx.graph.addEdge("A", "B", "e", 1.0, 0, 0, "");
    try ctx.graph.addEdge("B", "C", "e", 1.0, 0, 0, "");
    try ctx.graph.addEdge("A", "D", "e", 1.0, 0, 0, "");
    try ctx.graph.addEdge("D", "C", "e", 1.0, 0, 0, "");

    var engine = GraphQueryEngine{ .alloc = alloc };
    const start_keys: []const []const u8 = &.{"A"};
    var result = try engine.execute(&ctx.graph, .{
        .query_type = .shortest_path,
        .index_name = "test",
        .start_nodes = .{ .keys = start_keys },
        .target_nodes = .{ .keys = &.{"C"} },
        .params = .{ .max_depth = 3 },
    }, start_keys);
    defer result.deinit(alloc);

    try std.testing.expectEqual(@as(usize, 1), result.nodes.len);
    try std.testing.expect(result.nodes[0].path != null);
    try std.testing.expect(result.nodes[0].provenance == null);
}

test "shortest_path algebraic provenance supports inbound min-hop path" {
    const alloc = std.testing.allocator;
    var sb: [256]u8 = undefined;
    var rb: [256]u8 = undefined;
    const ctx = try setupGraph(alloc, "gq-alg-shortest-in-s", "gq-alg-shortest-in-r", &sb, &rb);
    defer {
        ctx.deinit();
        alloc.destroy(ctx);
    }

    ctx.graph.algebraic_semiring_traversal = true;
    try ctx.graph.addEdge("A", "B", "e", 2.0, 0, 0, "");
    try ctx.graph.addEdge("B", "C", "e", 3.0, 0, 0, "");

    var engine = GraphQueryEngine{ .alloc = alloc };
    const start_keys: []const []const u8 = &.{"C"};
    var result = try engine.execute(&ctx.graph, .{
        .query_type = .shortest_path,
        .index_name = "test",
        .start_nodes = .{ .keys = start_keys },
        .target_nodes = .{ .keys = &.{"A"} },
        .params = .{ .direction = .in, .max_depth = 3 },
    }, start_keys);
    defer result.deinit(alloc);

    try std.testing.expectEqual(@as(usize, 1), result.nodes.len);
    try std.testing.expectEqualStrings("A", result.nodes[0].key);
    try std.testing.expectEqual(@as(u32, 2), result.nodes[0].depth);
    const path = result.nodes[0].path orelse return error.TestExpectedEqual;
    try std.testing.expectEqualStrings("C", path[0]);
    try std.testing.expectEqualStrings("B", path[1]);
    try std.testing.expectEqualStrings("A", path[2]);
    const provenance = result.nodes[0].provenance orelse return error.TestExpectedEqual;
    try std.testing.expectEqual(@as(usize, 2), provenance.len);
    try std.testing.expectEqualStrings("A\x1fe\x1fB", provenance[0]);
    try std.testing.expectEqualStrings("B\x1fe\x1fC", provenance[1]);
}

test "shortest_path algebraic provenance supports bidirectional min-hop path" {
    const alloc = std.testing.allocator;
    var sb: [256]u8 = undefined;
    var rb: [256]u8 = undefined;
    const ctx = try setupGraph(alloc, "gq-alg-shortest-both-s", "gq-alg-shortest-both-r", &sb, &rb);
    defer {
        ctx.deinit();
        alloc.destroy(ctx);
    }

    ctx.graph.algebraic_semiring_traversal = true;
    try ctx.graph.addEdge("A", "B", "e", 2.0, 0, 0, "");
    try ctx.graph.addEdge("C", "B", "e", 3.0, 0, 0, "");

    var engine = GraphQueryEngine{ .alloc = alloc };
    const start_keys: []const []const u8 = &.{"A"};
    var result = try engine.execute(&ctx.graph, .{
        .query_type = .shortest_path,
        .index_name = "test",
        .start_nodes = .{ .keys = start_keys },
        .target_nodes = .{ .keys = &.{"C"} },
        .params = .{ .direction = .both, .max_depth = 3 },
    }, start_keys);
    defer result.deinit(alloc);

    try std.testing.expectEqual(@as(usize, 1), result.nodes.len);
    try std.testing.expectEqualStrings("C", result.nodes[0].key);
    try std.testing.expectEqual(@as(u32, 2), result.nodes[0].depth);
    const path = result.nodes[0].path orelse return error.TestExpectedEqual;
    try std.testing.expectEqualStrings("A", path[0]);
    try std.testing.expectEqualStrings("B", path[1]);
    try std.testing.expectEqualStrings("C", path[2]);
    const provenance = result.nodes[0].provenance orelse return error.TestExpectedEqual;
    try std.testing.expectEqual(@as(usize, 2), provenance.len);
    try std.testing.expectEqualStrings("A\x1fe\x1fB", provenance[0]);
    try std.testing.expectEqualStrings("C\x1fe\x1fB", provenance[1]);
}

test "shortest_path algebraic provenance applies exact edge weight filters" {
    const alloc = std.testing.allocator;
    var sb: [256]u8 = undefined;
    var rb: [256]u8 = undefined;
    const ctx = try setupGraph(alloc, "gq-alg-shortest-weight-s", "gq-alg-shortest-weight-r", &sb, &rb);
    defer {
        ctx.deinit();
        alloc.destroy(ctx);
    }

    ctx.graph.algebraic_semiring_traversal = true;
    try ctx.graph.addEdge("A", "B", "too-light", 0.5, 0, 0, "");
    try ctx.graph.addEdge("B", "D", "ok", 2.0, 0, 0, "");
    try ctx.graph.addEdge("A", "C", "ok", 2.0, 0, 0, "");
    try ctx.graph.addEdge("C", "D", "too-heavy", 5.0, 0, 0, "");
    try ctx.graph.addEdge("C", "E", "ok", 2.0, 0, 0, "");
    try ctx.graph.addEdge("E", "D", "ok", 2.0, 0, 0, "");

    var engine = GraphQueryEngine{ .alloc = alloc };
    const start_keys: []const []const u8 = &.{"A"};
    const target_keys: []const []const u8 = &.{"D"};
    var result = try engine.execute(&ctx.graph, .{
        .query_type = .shortest_path,
        .index_name = "test",
        .start_nodes = .{ .keys = start_keys },
        .target_nodes = .{ .keys = target_keys },
        .params = .{
            .max_depth = 3,
            .min_weight = 1.0,
            .max_weight = 3.0,
        },
    }, start_keys);
    defer result.deinit(alloc);

    try std.testing.expectEqual(@as(usize, 1), result.nodes.len);
    try std.testing.expectEqualStrings("D", result.nodes[0].key);
    try std.testing.expectEqual(@as(u32, 3), result.nodes[0].depth);
    const provenance = result.nodes[0].provenance orelse return error.TestExpectedEqual;
    try std.testing.expectEqual(@as(usize, 3), provenance.len);
    try std.testing.expectEqualStrings("A\x1fok\x1fC", provenance[0]);
    try std.testing.expectEqualStrings("C\x1fok\x1fE", provenance[1]);
    try std.testing.expectEqualStrings("E\x1fok\x1fD", provenance[2]);
}

test "k_shortest_paths via engine" {
    const alloc = std.testing.allocator;
    var sb: [256]u8 = undefined;
    var rb: [256]u8 = undefined;
    const ctx = try setupGraph(alloc, "gq4s", "gq4r", &sb, &rb);
    defer {
        ctx.deinit();
        alloc.destroy(ctx);
    }

    try ctx.graph.addEdge("A", "B", "e", 1.0, 0, 0, "");
    try ctx.graph.addEdge("B", "C", "e", 1.0, 0, 0, "");
    try ctx.graph.addEdge("A", "D", "e", 2.0, 0, 0, "");
    try ctx.graph.addEdge("D", "C", "e", 2.0, 0, 0, "");

    var engine = GraphQueryEngine{ .alloc = alloc };
    const start_keys: []const []const u8 = &.{"A"};
    var result = try engine.execute(&ctx.graph, .{
        .query_type = .k_shortest_paths,
        .index_name = "test",
        .start_nodes = .{ .keys = start_keys },
        .target_nodes = .{ .keys = &.{"C"} },
        .k = 2,
        .params = .{ .weight_mode = .min_weight },
    }, start_keys);
    defer result.deinit(alloc);

    try std.testing.expectEqual(@as(usize, 2), result.nodes.len);
    // First path should be shorter/lighter
    try std.testing.expect(result.nodes[0].distance <= result.nodes[1].distance);
}

test "k_shortest_paths k one can execute through algebraic shortest path proof" {
    const alloc = std.testing.allocator;
    var sb: [256]u8 = undefined;
    var rb: [256]u8 = undefined;
    const ctx = try setupGraph(alloc, "gq-alg-k1-shortest-s", "gq-alg-k1-shortest-r", &sb, &rb);
    defer {
        ctx.deinit();
        alloc.destroy(ctx);
    }

    ctx.graph.algebraic_semiring_traversal = true;
    try ctx.graph.addEdge("A", "B", "e", 2.0, 0, 0, "");
    try ctx.graph.addEdge("B", "C", "e", 3.0, 0, 0, "");

    var engine = GraphQueryEngine{ .alloc = alloc };
    const start_keys: []const []const u8 = &.{"A"};
    var result = try engine.execute(&ctx.graph, .{
        .query_type = .k_shortest_paths,
        .index_name = "test",
        .start_nodes = .{ .keys = start_keys },
        .target_nodes = .{ .keys = &.{"C"} },
        .k = 1,
        .params = .{ .max_depth = 3 },
    }, start_keys);
    defer result.deinit(alloc);

    try std.testing.expectEqual(@as(usize, 1), result.nodes.len);
    try std.testing.expectEqualStrings("C", result.nodes[0].key);
    try std.testing.expectEqual(@as(u32, 2), result.nodes[0].depth);
    try std.testing.expectEqual(@as(f64, 2.0), result.nodes[0].distance);
    const provenance = result.nodes[0].provenance orelse return error.TestExpectedEqual;
    try std.testing.expectEqual(@as(usize, 2), provenance.len);
    try std.testing.expectEqualStrings("A\x1fe\x1fB", provenance[0]);
    try std.testing.expectEqualStrings("B\x1fe\x1fC", provenance[1]);
}

test "k_shortest_paths k greater than one stays on normal pathfinder" {
    const alloc = std.testing.allocator;
    var sb: [256]u8 = undefined;
    var rb: [256]u8 = undefined;
    const ctx = try setupGraph(alloc, "gq-alg-k2-shortest-s", "gq-alg-k2-shortest-r", &sb, &rb);
    defer {
        ctx.deinit();
        alloc.destroy(ctx);
    }

    ctx.graph.algebraic_semiring_traversal = true;
    try ctx.graph.addEdge("A", "B", "e", 1.0, 0, 0, "");
    try ctx.graph.addEdge("B", "C", "e", 1.0, 0, 0, "");
    try ctx.graph.addEdge("A", "D", "e", 2.0, 0, 0, "");
    try ctx.graph.addEdge("D", "C", "e", 2.0, 0, 0, "");

    var engine = GraphQueryEngine{ .alloc = alloc };
    const start_keys: []const []const u8 = &.{"A"};
    var result = try engine.execute(&ctx.graph, .{
        .query_type = .k_shortest_paths,
        .index_name = "test",
        .start_nodes = .{ .keys = start_keys },
        .target_nodes = .{ .keys = &.{"C"} },
        .k = 2,
    }, start_keys);
    defer result.deinit(alloc);

    try std.testing.expectEqual(@as(usize, 2), result.nodes.len);
    try std.testing.expect(result.nodes[0].provenance == null);
    try std.testing.expect(result.nodes[1].provenance == null);
}

test "edge_type filtering in traverse" {
    const alloc = std.testing.allocator;
    var sb: [256]u8 = undefined;
    var rb: [256]u8 = undefined;
    const ctx = try setupGraph(alloc, "gq5s", "gq5r", &sb, &rb);
    defer {
        ctx.deinit();
        alloc.destroy(ctx);
    }

    try ctx.graph.addEdge("A", "B", "knows", 1.0, 0, 0, "");
    try ctx.graph.addEdge("A", "C", "likes", 1.0, 0, 0, "");
    try ctx.graph.addEdge("A", "D", "knows", 1.0, 0, 0, "");

    var engine = GraphQueryEngine{ .alloc = alloc };
    const start_keys: []const []const u8 = &.{"A"};
    const et: []const []const u8 = &.{"knows"};
    var result = try engine.execute(&ctx.graph, .{
        .query_type = .traverse,
        .index_name = "test",
        .start_nodes = .{ .keys = start_keys },
        .params = .{ .edge_types = et, .max_depth = 1 },
    }, start_keys);
    defer result.deinit(alloc);

    // Only "knows" edges: B and D, not C
    try std.testing.expectEqual(@as(usize, 2), result.nodes.len);
}

test "empty result: no edges from start" {
    const alloc = std.testing.allocator;
    var sb: [256]u8 = undefined;
    var rb: [256]u8 = undefined;
    const ctx = try setupGraph(alloc, "gq6s", "gq6r", &sb, &rb);
    defer {
        ctx.deinit();
        alloc.destroy(ctx);
    }

    // No edges added

    var engine = GraphQueryEngine{ .alloc = alloc };
    const start_keys: []const []const u8 = &.{"lonely"};
    var result = try engine.execute(&ctx.graph, .{
        .query_type = .traverse,
        .index_name = "test",
        .start_nodes = .{ .keys = start_keys },
    }, start_keys);
    defer result.deinit(alloc);

    try std.testing.expectEqual(@as(usize, 0), result.nodes.len);
}

test "pattern: 3-step linear chain via engine" {
    const alloc = std.testing.allocator;
    var sb: [256]u8 = undefined;
    var rb: [256]u8 = undefined;
    const ctx = try setupGraph(alloc, "gqp1s", "gqp1r", &sb, &rb);
    defer {
        ctx.deinit();
        alloc.destroy(ctx);
    }

    // A -e-> B -e-> C, A -e-> D (D has no outgoing edges)
    try ctx.graph.addEdge("A", "B", "e", 1.0, 0, 0, "");
    try ctx.graph.addEdge("B", "C", "e", 1.0, 0, 0, "");
    try ctx.graph.addEdge("A", "D", "e", 1.0, 0, 0, "");

    var engine = GraphQueryEngine{ .alloc = alloc };
    const start_keys: []const []const u8 = &.{"A"};
    // 3-step pattern: step0 binds start node, step1 hops out, step2 hops out.
    // So we're looking for start -> 1-hop -> 2-hop chains.
    const steps: []const pattern_mod.PatternStep = &.{
        .{ .alias = "start", .edge = .{ .direction = .out } },
        .{ .alias = "hop1", .edge = .{ .direction = .out } },
        .{ .alias = "hop2", .edge = .{ .direction = .out } },
    };
    var result = try engine.execute(&ctx.graph, .{
        .query_type = .pattern,
        .index_name = "test",
        .start_nodes = .{ .keys = start_keys },
        .pattern = steps,
    }, start_keys);
    defer result.deinit(alloc);

    // Only one 2-hop chain from A: A->B->C. D has no outgoing edges.
    try std.testing.expectEqual(@as(usize, 1), result.matches.len);

    // Match should bind start=A, hop1=B, hop2=C.
    const match = result.matches[0];
    try std.testing.expectEqual(@as(usize, 3), match.bindings.len);

    var found_a = false;
    var found_b = false;
    var found_c = false;
    for (match.bindings) |b| {
        if (std.mem.eql(u8, b.alias, "start") and std.mem.eql(u8, b.key, "A")) found_a = true;
        if (std.mem.eql(u8, b.alias, "hop1") and std.mem.eql(u8, b.key, "B")) found_b = true;
        if (std.mem.eql(u8, b.alias, "hop2") and std.mem.eql(u8, b.key, "C")) found_c = true;
    }
    try std.testing.expect(found_a);
    try std.testing.expect(found_b);
    try std.testing.expect(found_c);

    // Result nodes include all unique keys from match bindings.
    try std.testing.expectEqual(@as(usize, 3), result.nodes.len);
}

test "pattern can execute through algebraic provenance semiring for unique linear chain" {
    const alloc = std.testing.allocator;
    var sb: [256]u8 = undefined;
    var rb: [256]u8 = undefined;
    const ctx = try setupGraph(alloc, "gq-alg-pattern-s", "gq-alg-pattern-r", &sb, &rb);
    defer {
        ctx.deinit();
        alloc.destroy(ctx);
    }

    ctx.graph.algebraic_semiring_traversal = true;
    try ctx.graph.addEdge("A", "B", "e", 1.0, 0, 0, "");
    try ctx.graph.addEdge("B", "C", "e", 1.0, 0, 0, "");
    try ctx.graph.addEdge("A", "D", "e", 1.0, 0, 0, "");

    const start_keys: []const []const u8 = &.{"A"};
    const edge_types: []const []const u8 = &.{"e"};
    const steps: []const pattern_mod.PatternStep = &.{
        .{ .alias = "start", .edge = .{ .direction = .out } },
        .{ .alias = "hop1", .edge = .{ .direction = .out, .types = edge_types } },
        .{ .alias = "hop2", .edge = .{ .direction = .out, .types = edge_types }, .node_filter = .{ .filter_prefix = "C" } },
    };

    var engine = GraphQueryEngine{ .alloc = alloc };
    var algebraic_result = (try engine.executeAlgebraicPattern(&ctx.graph, .{
        .query_type = .pattern,
        .index_name = "test",
        .start_nodes = .{ .keys = start_keys },
        .pattern = steps,
    }, start_keys)) orelse return error.TestExpectedEqual;
    defer algebraic_result.deinit(alloc);

    try std.testing.expectEqual(@as(usize, 1), algebraic_result.matches.len);
    try std.testing.expectEqual(@as(usize, 2), algebraic_result.matches[0].path.len);
    try std.testing.expectEqualStrings("A", algebraic_result.matches[0].path[0].source);
    try std.testing.expectEqualStrings("B", algebraic_result.matches[0].path[0].target);
    try std.testing.expectEqualStrings("C", algebraic_result.matches[0].path[1].target);
    try std.testing.expectEqual(@as(u32, 0), algebraic_result.matches[0].bindings[0].depth);
    try std.testing.expectEqual(@as(u32, 1), algebraic_result.matches[0].bindings[1].depth);
    try std.testing.expectEqual(@as(u32, 2), algebraic_result.matches[0].bindings[2].depth);
    try std.testing.expectEqual(@as(usize, 3), algebraic_result.nodes.len);
}

test "pattern algebraic provenance falls back for ambiguous linear chain" {
    const alloc = std.testing.allocator;
    var sb: [256]u8 = undefined;
    var rb: [256]u8 = undefined;
    const ctx = try setupGraph(alloc, "gq-alg-pattern-amb-s", "gq-alg-pattern-amb-r", &sb, &rb);
    defer {
        ctx.deinit();
        alloc.destroy(ctx);
    }

    ctx.graph.algebraic_semiring_traversal = true;
    try ctx.graph.addEdge("A", "B", "e", 1.0, 0, 0, "");
    try ctx.graph.addEdge("B", "D", "e", 1.0, 0, 0, "");
    try ctx.graph.addEdge("A", "C", "e", 1.0, 0, 0, "");
    try ctx.graph.addEdge("C", "D", "e", 1.0, 0, 0, "");

    const start_keys: []const []const u8 = &.{"A"};
    const edge_types: []const []const u8 = &.{"e"};
    const steps: []const pattern_mod.PatternStep = &.{
        .{ .alias = "start", .edge = .{ .direction = .out } },
        .{ .alias = "mid", .edge = .{ .direction = .out, .types = edge_types } },
        .{ .alias = "end", .edge = .{ .direction = .out, .types = edge_types } },
    };

    var engine = GraphQueryEngine{ .alloc = alloc };
    try std.testing.expect((try engine.executeAlgebraicPattern(&ctx.graph, .{
        .query_type = .pattern,
        .index_name = "test",
        .start_nodes = .{ .keys = start_keys },
        .pattern = steps,
    }, start_keys)) == null);

    var fallback_result = try engine.execute(&ctx.graph, .{
        .query_type = .pattern,
        .index_name = "test",
        .start_nodes = .{ .keys = start_keys },
        .pattern = steps,
    }, start_keys);
    defer fallback_result.deinit(alloc);

    try std.testing.expectEqual(@as(usize, 2), fallback_result.matches.len);
}
