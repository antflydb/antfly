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
const NodeAdmission = @import("node_admission.zig").NodeAdmission;
const NodeRef = @import("node_admission.zig").NodeRef;
const paths_mod = @import("paths.zig");
const traversal_mod = @import("traversal.zig");
const node_identity = @import("node_identity.zig");

pub const max_pattern_steps: usize = 64;
pub const max_pattern_hops: u32 = 64;
pub const default_max_explored_nodes: usize = 100_000;
pub const default_max_explored_edges: usize = 1_000_000;
pub const default_max_intermediate_states: usize = 100_000;

pub const NodeFilter = struct {
    filter_prefix: []const u8 = "",
    filter_query_json: ?[]const u8 = null,
};

pub const FilterEvaluator = struct {
    ctx: ?*anyopaque = null,
    func: ?*const fn (?*anyopaque, []const u8, NodeFilter) anyerror!bool = null,
};

pub const PatternEdgeStep = struct {
    direction: graph_mod.EdgeDirection = .out,
    min_hops: u32 = 1,
    max_hops: u32 = 1,
    min_weight: f64 = 0.0,
    max_weight: f64 = 0.0,
    types: []const []const u8 = &.{},
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

    pub fn deinit(self: *PatternMatch, alloc: Allocator) void {
        for (self.bindings) |*binding| binding.deinit(alloc);
        if (self.bindings.len > 0) alloc.free(self.bindings);
        freePathEdges(alloc, self.path);
        self.* = undefined;
    }
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
    return_aliases: []const []const u8 = &.{},
    target_nodes: []const node_identity.Ref = &.{},
    target_required: bool = false,
    include_paths: bool = true,
    evaluator: ?FilterEvaluator = null,
    node_admission: ?NodeAdmission = null,
    stats: ?*MatchStats = null,
    max_explored_nodes: usize = default_max_explored_nodes,
    max_explored_edges: usize = default_max_explored_edges,
    max_intermediate_states: usize = default_max_intermediate_states,
};

const WorkBudget = struct {
    remaining_nodes: usize,
    remaining_edges: usize,

    fn consumeNode(self: *WorkBudget) !void {
        if (self.remaining_nodes == 0) return error.QueryCandidateBudgetExceeded;
        self.remaining_nodes -= 1;
    }

    fn consumeEdges(self: *WorkBudget, count: usize) !void {
        if (count > self.remaining_edges) return error.QueryCandidateBudgetExceeded;
        self.remaining_edges -= count;
    }
};

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

const Frontier = struct {
    key: []u8,
    table: ?[]u8 = null,
    path: []paths_mod.PathEdge,
    hops: u32,

    fn deinit(self: *Frontier, alloc: Allocator) void {
        alloc.free(self.key);
        if (self.table) |table| alloc.free(table);
        freePathEdges(alloc, self.path);
        self.* = undefined;
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

        pub fn freeEdges(_: @This(), a: Allocator, edges: []graph_mod.Edge) void {
            graph_mod.GraphIndex.freeEdges(a, edges);
        }

        pub fn probeEdges(
            self: @This(),
            a: Allocator,
            table: ?[]const u8,
            probes: []const graph_mod.EdgeProbe,
        ) ![]?graph_mod.Edge {
            if (table != null) {
                const empty = try a.alloc(?graph_mod.Edge, probes.len);
                @memset(empty, null);
                return empty;
            }
            return try self.graph_index.probeEdgesAlloc(a, probes);
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
    var work_budget = WorkBudget{
        .remaining_nodes = opts.max_explored_nodes,
        .remaining_edges = opts.max_explored_edges,
    };
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
    var exact_budget = work_budget;
    if (try matchExactTwoEdgePattern(
        alloc,
        edge_reader,
        start_nodes,
        pattern,
        opts,
        intermediate_limit,
        &exact_budget,
    )) |matches| {
        work_budget = exact_budget;
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
                );
            }
        }
        break :blk mask;
    } else null;
    defer if (admitted_starts) |mask| alloc.free(mask);
    for (start_nodes, 0..) |start, start_index| {
        if (admitted_starts) |mask| if (!mask[start_index]) continue;
        if (!(try passesNodeFilter(start.key, pattern[0].node_filter, opts.evaluator))) continue;

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
        if (current.items.len > intermediate_limit)
            return error.QueryCandidateBudgetExceeded;
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
                step.edge,
                step.node_filter,
                if (step_idx + 1 == pattern.len) opts.target_nodes else &.{},
                step_idx + 1 == pattern.len and opts.target_required,
                opts.max_results,
                opts.evaluator,
                opts.node_admission,
                opts.include_paths,
                opts.stats,
                &work_budget,
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

                if (next.items.len > intermediate_limit)
                    return error.QueryCandidateBudgetExceeded;
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
    if (comptime !@hasDecl(@TypeOf(edge_reader), "probeEdges") or
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
    if (!(try passesNodeFilter(start_key, pattern[0].node_filter, opts.evaluator)) or
        !(try passesNodeFilter(target_key, pattern[2].node_filter, opts.evaluator)))
    {
        return try alloc.alloc(PatternMatch, 0);
    }

    const forward_edges = try edge_reader.getEdges(
        alloc,
        null,
        start_key,
        pattern[1].edge.types,
        pattern[1].edge.direction,
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
        if (std.mem.eql(u8, middle_key, start_key)) continue;
        if (edgeTargetTable(null, graph_edge, middle_key) != null) return null;
        if (!(try passesNodeFilter(middle_key, pattern[1].node_filter, opts.evaluator))) continue;
        try candidates.append(alloc, .{
            .middle_key = middle_key,
            .forward_edge_index = edge_index,
        });
        if (candidates.items.len > intermediate_limit)
            return error.QueryCandidateBudgetExceeded;
        // Keep the same deterministic first-hop window as generic expansion.
        // The optimizer must not make nodes beyond that window newly visible.
        if (candidates.items.len >= expansion_limit) break;
    }

    // Match generic-expansion accounting: one expanded start node, then one
    // expanded middle node and one exact relationship probe per candidate.
    try work_budget.consumeNode();
    try work_budget.consumeEdges(forward_edges.len);
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
        const probed_edges = try edge_reader.probeEdges(alloc, null, probes);
        defer edge_reader.freeProbedEdges(alloc, probed_edges);
        if (probed_edges.len != probes.len) return error.InvalidGraphEdgeProbeResult;

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
                const first = try appendPathEdge(alloc, &.{}, forward_edge, start_key, middle_key);
                defer freePathEdges(alloc, first);
                break :blk try appendPathEdge(alloc, first, backward_edge, middle_key, target_key);
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
    edge: PatternEdgeStep,
    node_filter: NodeFilter,
    target_nodes: []const node_identity.Ref,
    target_required: bool,
    max_results: u32,
    evaluator: ?FilterEvaluator,
    node_admission: ?NodeAdmission,
    include_paths: bool,
    stats: ?*MatchStats,
    work_budget: *WorkBudget,
) ![]ReachableNode {
    const min_hops = if (edge.min_hops == 0) @as(u32, 1) else edge.min_hops;
    const max_hops = if (edge.max_hops == 0) @as(u32, 1) else edge.max_hops;
    const result_limit: usize = if (max_results == 0)
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

    var visited = node_identity.Map(void){};
    defer visited.deinit(alloc);

    var current = std.ArrayListUnmanaged(Frontier).empty;
    defer {
        for (current.items) |*frontier| frontier.deinit(alloc);
        current.deinit(alloc);
    }
    {
        var initial = try initFrontier(alloc, start_key, start_table, &.{}, 0);
        errdefer initial.deinit(alloc);
        try current.append(alloc, initial);
    }
    _ = try visited.putIfAbsent(
        alloc,
        .{ .table = start_table, .key = start_key },
        {},
    );

    while (current.items.len > 0 and results.items.len < result_limit) {
        var next = std.ArrayListUnmanaged(Frontier).empty;
        errdefer {
            for (next.items) |*frontier| frontier.deinit(alloc);
            next.deinit(alloc);
        }

        for (current.items) |*frontier| {
            try work_budget.consumeNode();
            if (frontier.hops >= max_hops) continue;

            const edges = try edge_reader.getEdges(
                alloc,
                frontier.table,
                frontier.key,
                edge.types,
                edge.direction,
            );
            defer edge_reader.freeEdges(alloc, edges);
            if (stats) |active| {
                active.adjacency_reads += 1;
                active.adjacency_edges += edges.len;
            }
            try work_budget.consumeEdges(edges.len);

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
                    if (std.mem.eql(u8, target_key, frontier.key)) continue;
                    const target_table = edgeTargetTable(
                        frontier.table,
                        graph_edge,
                        target_key,
                    );
                    if (visited.contains(.{ .table = target_table, .key = target_key })) continue;
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

            for (edges, 0..) |graph_edge, edge_index| {
                const target_key = edgeTarget(graph_edge, frontier.key, edge.direction) orelse continue;
                if (admitted_edges) |mask| {
                    if (!mask[edge_index]) continue;
                } else {
                    if (!edgeMatches(graph_edge, edge)) continue;
                    if (std.mem.eql(u8, target_key, frontier.key)) continue;
                }
                const new_hops = frontier.hops + 1;
                const target_table = edgeTargetTable(
                    frontier.table,
                    graph_edge,
                    target_key,
                );
                const new_path = if (include_paths)
                    try appendPathEdge(alloc, frontier.path, graph_edge, frontier.key, target_key)
                else
                    @constCast((&[_]paths_mod.PathEdge{})[0..]);
                var new_path_owned = true;
                errdefer if (new_path_owned) freePathEdges(alloc, new_path);

                if (new_hops >= min_hops and
                    targetNodeMatches(.{ .table = target_table, .key = target_key }, target_nodes, target_required) and
                    try passesNodeFilter(target_key, node_filter, evaluator))
                {
                    var reached = try initReachableNode(
                        alloc,
                        target_key,
                        target_table,
                        new_hops,
                        new_path,
                    );
                    errdefer reached.deinit(alloc);
                    try results.append(alloc, reached);
                    if (results.items.len >= result_limit) {
                        freePathEdges(alloc, new_path);
                        new_path_owned = false;
                        break;
                    }
                }

                if (new_hops < max_hops and try visited.putIfAbsent(
                    alloc,
                    .{ .table = target_table, .key = target_key },
                    {},
                )) {
                    var next_item = try initFrontier(
                        alloc,
                        target_key,
                        target_table,
                        new_path,
                        new_hops,
                    );
                    new_path_owned = false;
                    errdefer next_item.deinit(alloc);
                    try next.append(alloc, next_item);
                } else {
                    freePathEdges(alloc, new_path);
                    new_path_owned = false;
                }
            }
            if (results.items.len >= result_limit) break;
        }

        for (current.items) |*frontier| frontier.deinit(alloc);
        current.deinit(alloc);
        current = next;
    }

    return try results.toOwnedSlice(alloc);
}

fn startNodeAdmitted(
    alloc: Allocator,
    edge_reader: anytype,
    start_key: []const u8,
    direction: graph_mod.EdgeDirection,
    admission: NodeAdmission,
) !bool {
    const keys = [_][]const u8{start_key};
    const admitted = try admission.filterLocalKeysAlloc(alloc, &keys);
    defer alloc.free(admitted);
    if (admitted[0] or direction == .out) return admitted[0];

    const incoming = try edge_reader.getEdges(alloc, null, start_key, &.{}, .in);
    defer edge_reader.freeEdges(alloc, incoming);
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
    if (step.min_weight > 0 and edge.weight < step.min_weight) return false;
    if (step.max_weight > 0 and edge.weight > step.max_weight) return false;
    return true;
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

fn passesPrefixFilter(key: []const u8, filter: NodeFilter) bool {
    if (filter.filter_prefix.len == 0) return true;
    return std.mem.startsWith(u8, key, filter.filter_prefix);
}

fn passesNodeFilter(key: []const u8, filter: NodeFilter, evaluator: ?FilterEvaluator) !bool {
    if (!passesPrefixFilter(key, filter)) return false;
    if (filter.filter_query_json == null) return true;
    const active = evaluator orelse return error.UnsupportedNodeFilterQuery;
    const eval_fn = active.func orelse return error.UnsupportedNodeFilterQuery;
    return try eval_fn(active.ctx, key, filter);
}

fn effectiveAlias(alias: []const u8, step_idx: usize, buf: []u8) []const u8 {
    if (alias.len > 0) return alias;
    return std.fmt.bufPrint(buf, "_step{}", .{step_idx}) catch "_step";
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
    alloc: Allocator,
    key: []const u8,
    table: ?[]const u8,
    path: []paths_mod.PathEdge,
    hops: u32,
) !Frontier {
    const owned_key = try alloc.dupe(u8, key);
    errdefer alloc.free(owned_key);
    const owned_table = if (table) |table_name| try alloc.dupe(u8, table_name) else null;
    errdefer if (owned_table) |table_name| alloc.free(table_name);
    return .{
        .key = owned_key,
        .table = owned_table,
        .path = path,
        .hops = hops,
    };
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
        out[i] = try dupePathEdge(alloc, edge.source, edge.target, edge.edge_type, edge.weight, edge.metadata);
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
        out[i] = try dupePathEdge(alloc, edge.source, edge.target, edge.edge_type, edge.weight, edge.metadata);
        initialized += 1;
    }
    for (right, 0..) |edge, i| {
        out[left.len + i] = try dupePathEdge(alloc, edge.source, edge.target, edge.edge_type, edge.weight, edge.metadata);
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
) ![]paths_mod.PathEdge {
    var out = try alloc.alloc(paths_mod.PathEdge, existing.len + 1);
    var initialized: usize = 0;
    errdefer {
        freePathEdgeItems(alloc, out[0..initialized]);
        if (out.len > 0) alloc.free(out);
    }
    for (existing, 0..) |item, i| {
        out[i] = try dupePathEdge(alloc, item.source, item.target, item.edge_type, item.weight, item.metadata);
        initialized += 1;
    }
    out[out.len - 1] = try dupePathEdge(
        alloc,
        source,
        target,
        edge.edge_type,
        edge.weight,
        edge.metadata,
    );
    initialized += 1;
    return out;
}

fn dupePathEdge(
    alloc: Allocator,
    source: []const u8,
    target: []const u8,
    edge_type: []const u8,
    weight: f64,
    metadata: []const u8,
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

test "exact two-edge pattern uses typed batch probes without paths" {
    const Reader = struct {
        forward_calls: *usize,
        reverse_calls: *usize,

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

        pub fn probeEdges(
            self: @This(),
            alloc: Allocator,
            _: ?[]const u8,
            probes: []const graph_mod.EdgeProbe,
        ) ![]?graph_mod.Edge {
            self.reverse_calls.* += 1;
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
    var stats = MatchStats{};
    const matches = try matchPatternWithEdgeReader(
        std.testing.allocator,
        Reader{ .forward_calls = &forward_calls, .reverse_calls = &reverse_calls },
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

        pub fn probeEdges(_: @This(), alloc: Allocator, _: ?[]const u8, probes: []const graph_mod.EdgeProbe) ![]?graph_mod.Edge {
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

        pub fn probeEdges(_: @This(), alloc: Allocator, _: ?[]const u8, probes: []const graph_mod.EdgeProbe) ![]?graph_mod.Edge {
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
        .include_paths = false,
    });
    defer freeMatches(std.testing.allocator, matches);
    try std.testing.expectEqual(@as(usize, 1), matches.len);
    try std.testing.expectEqualStrings("target", matches[0].bindings[2].key);
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
        error.QueryCandidateBudgetExceeded,
        matchPatternWithEdgeReader(std.testing.allocator, Reader{}, starts, &.{
            .{ .alias = "a" },
            .{ .alias = "b" },
        }, .{ .max_explored_edges = 1 }),
    );
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

        pub fn probeEdges(_: @This(), _: Allocator, _: ?[]const u8, _: []const graph_mod.EdgeProbe) ![]?graph_mod.Edge {
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
