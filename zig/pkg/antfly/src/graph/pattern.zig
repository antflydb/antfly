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

pub fn freeMatches(alloc: Allocator, matches: []PatternMatch) void {
    for (matches) |*match| match.deinit(alloc);
    if (matches.len > 0) alloc.free(matches);
}

pub const MatchOptions = struct {
    max_results: u32 = 100,
    return_aliases: []const []const u8 = &.{},
    evaluator: ?FilterEvaluator = null,
    node_admission: ?NodeAdmission = null,
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
            direction: graph_mod.EdgeDirection,
        ) ![]graph_mod.Edge {
            if (table != null) return try a.alloc(graph_mod.Edge, 0);
            return try self.graph_index.getEdges(a, key, "", direction);
        }

        pub fn freeEdges(_: @This(), a: Allocator, edges: []graph_mod.Edge) void {
            graph_mod.GraphIndex.freeEdges(a, edges);
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
                opts.max_results,
                opts.evaluator,
                opts.node_admission,
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

                const new_path = try concatPathEdges(alloc, match.path, reached.path);
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

    for (current.items[0..limited_len], 0..) |match, i| {
        const filtered_bindings = try filterBindings(alloc, match.bindings, opts.return_aliases);
        errdefer {
            for (filtered_bindings) |*binding| binding.deinit(alloc);
            if (filtered_bindings.len > 0) alloc.free(filtered_bindings);
        }
        results[i] = .{
            .bindings = filtered_bindings,
            .path = try clonePathEdges(alloc, match.path),
        };
        initialized += 1;
    }

    return results;
}

fn findReachableNodes(
    alloc: Allocator,
    edge_reader: anytype,
    start_key: []const u8,
    start_table: ?[]const u8,
    edge: PatternEdgeStep,
    node_filter: NodeFilter,
    max_results: u32,
    evaluator: ?FilterEvaluator,
    node_admission: ?NodeAdmission,
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
                edge.direction,
            );
            defer edge_reader.freeEdges(alloc, edges);
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
                const new_path = try appendPathEdge(alloc, frontier.path, graph_edge, frontier.key, target_key);
                var new_path_owned = true;
                errdefer if (new_path_owned) freePathEdges(alloc, new_path);

                if (new_hops >= min_hops and try passesNodeFilter(target_key, node_filter, evaluator)) {
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

    const incoming = try edge_reader.getEdges(alloc, null, start_key, .in);
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

test "pattern matching rejects unbounded hops and exploration" {
    const Reader = struct {
        const edges = [_]graph_mod.Edge{
            .{ .source = "A", .target = "B", .edge_type = "e", .weight = 1, .created_at = 0, .updated_at = 0, .metadata = "" },
            .{ .source = "A", .target = "C", .edge_type = "e", .weight = 1, .created_at = 0, .updated_at = 0, .metadata = "" },
        };

        pub fn getEdges(_: @This(), _: Allocator, _: ?[]const u8, _: []const u8, _: graph_mod.EdgeDirection) ![]graph_mod.Edge {
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

        pub fn getEdges(_: @This(), _: Allocator, _: ?[]const u8, _: []const u8, _: graph_mod.EdgeDirection) ![]graph_mod.Edge {
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
