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
const paths_mod = @import("paths.zig");

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
    depth: u32,

    pub fn deinit(self: *PatternBinding, alloc: Allocator) void {
        alloc.free(self.alias);
        alloc.free(self.key);
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
    path: []paths_mod.PathEdge,
    hops: u32,

    fn deinit(self: *Frontier, alloc: Allocator) void {
        alloc.free(self.key);
        freePathEdges(alloc, self.path);
        self.* = undefined;
    }
};

const ReachableNode = struct {
    key: []u8,
    depth: u32,
    path: []paths_mod.PathEdge,

    fn deinit(self: *ReachableNode, alloc: Allocator) void {
        alloc.free(self.key);
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

        pub fn getEdges(self: @This(), a: Allocator, key: []const u8, direction: graph_mod.EdgeDirection) ![]graph_mod.Edge {
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
    if (pattern.len == 0) return error.InvalidArgument;

    var current = std.ArrayListUnmanaged(MatchState).empty;
    defer {
        for (current.items) |*match| match.deinit(alloc);
        current.deinit(alloc);
    }

    var first_alias_buf: [32]u8 = undefined;
    const first_alias = effectiveAlias(pattern[0].alias, 0, &first_alias_buf);
    for (start_keys) |start_key| {
        if (!(try passesNodeFilter(start_key, pattern[0].node_filter, opts.evaluator))) continue;

        var bindings = try alloc.alloc(PatternBinding, 1);
        errdefer {
            bindings[0].deinit(alloc);
            alloc.free(bindings);
        }
        bindings[0] = .{
            .alias = try alloc.dupe(u8, first_alias),
            .key = try alloc.dupe(u8, start_key),
            .depth = 0,
        };
        try current.append(alloc, .{
            .bindings = bindings,
            .path = &.{},
        });
        if (opts.max_results > 0 and current.items.len >= opts.max_results) break;
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
            const reachable = try findReachableNodes(alloc, edge_reader, current_binding.key, step.edge, step.node_filter, opts.max_results, opts.evaluator);
            defer {
                for (reachable) |*node| node.deinit(alloc);
                if (reachable.len > 0) alloc.free(reachable);
            }

            for (reachable) |reached| {
                if (cycle_binding) |existing| {
                    if (!std.mem.eql(u8, reached.key, existing.key)) continue;
                }

                var new_bindings = try cloneBindings(alloc, match.bindings);
                errdefer {
                    for (new_bindings) |*binding| binding.deinit(alloc);
                    if (new_bindings.len > 0) alloc.free(new_bindings);
                }

                if (cycle_binding == null) {
                    var expanded = try alloc.alloc(PatternBinding, new_bindings.len + 1);
                    errdefer alloc.free(expanded);
                    for (new_bindings, 0..) |binding, i| expanded[i] = binding;
                    if (new_bindings.len > 0) alloc.free(new_bindings);
                    expanded[expanded.len - 1] = .{
                        .alias = try alloc.dupe(u8, step_alias),
                        .key = try alloc.dupe(u8, reached.key),
                        .depth = reached.depth,
                    };
                    new_bindings = expanded;
                }

                const new_path = try concatPathEdges(alloc, match.path, reached.path);
                errdefer freePathEdges(alloc, new_path);
                try next.append(alloc, .{
                    .bindings = new_bindings,
                    .path = new_path,
                });

                if (opts.max_results > 0 and next.items.len >= opts.max_results * 10) break;
            }
            if (opts.max_results > 0 and next.items.len >= opts.max_results * 10) break;
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
    edge: PatternEdgeStep,
    node_filter: NodeFilter,
    max_results: u32,
    evaluator: ?FilterEvaluator,
) ![]ReachableNode {
    const min_hops = if (edge.min_hops == 0) @as(u32, 1) else edge.min_hops;
    const max_hops = if (edge.max_hops == 0) @as(u32, 1) else edge.max_hops;
    const result_limit: usize = if (max_results == 0) 1000 else @min(@as(usize, max_results) * 10, 1000);

    var results = std.ArrayListUnmanaged(ReachableNode).empty;
    errdefer {
        for (results.items) |*node| node.deinit(alloc);
        results.deinit(alloc);
    }

    var visited = std.StringHashMapUnmanaged(void).empty;
    defer {
        var it = visited.keyIterator();
        while (it.next()) |key| alloc.free(key.*);
        visited.deinit(alloc);
    }

    var current = std.ArrayListUnmanaged(Frontier).empty;
    defer {
        for (current.items) |*frontier| frontier.deinit(alloc);
        current.deinit(alloc);
    }
    try current.append(alloc, .{
        .key = try alloc.dupe(u8, start_key),
        .path = &.{},
        .hops = 0,
    });
    try visited.put(alloc, try alloc.dupe(u8, start_key), {});

    while (current.items.len > 0 and results.items.len < result_limit) {
        var next = std.ArrayListUnmanaged(Frontier).empty;
        errdefer {
            for (next.items) |*frontier| frontier.deinit(alloc);
            next.deinit(alloc);
        }

        for (current.items) |*frontier| {
            if (frontier.hops >= max_hops) continue;

            const edges = try edge_reader.getEdges(alloc, frontier.key, edge.direction);
            defer edge_reader.freeEdges(alloc, edges);

            for (edges) |graph_edge| {
                if (!edgeMatches(graph_edge, edge)) continue;
                const target_key = edgeTarget(graph_edge, frontier.key, edge.direction) orelse continue;
                if (std.mem.eql(u8, target_key, frontier.key)) continue;

                const new_hops = frontier.hops + 1;
                const new_path = try appendPathEdge(alloc, frontier.path, graph_edge, frontier.key, target_key);
                errdefer freePathEdges(alloc, new_path);

                if (new_hops >= min_hops and try passesNodeFilter(target_key, node_filter, evaluator)) {
                    try results.append(alloc, .{
                        .key = try alloc.dupe(u8, target_key),
                        .depth = new_hops,
                        .path = try clonePathEdges(alloc, new_path),
                    });
                    if (results.items.len >= result_limit) {
                        freePathEdges(alloc, new_path);
                        break;
                    }
                }

                if (new_hops < max_hops and !visited.contains(target_key)) {
                    try visited.put(alloc, try alloc.dupe(u8, target_key), {});
                    try next.append(alloc, .{
                        .key = try alloc.dupe(u8, target_key),
                        .path = new_path,
                        .hops = new_hops,
                    });
                } else {
                    freePathEdges(alloc, new_path);
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
        filtered[out_idx] = .{
            .alias = try alloc.dupe(u8, binding.alias),
            .key = try alloc.dupe(u8, binding.key),
            .depth = binding.depth,
        };
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
        out[i] = .{
            .alias = try alloc.dupe(u8, binding.alias),
            .key = try alloc.dupe(u8, binding.key),
            .depth = binding.depth,
        };
        initialized += 1;
    }
    return out;
}

fn clonePathEdges(alloc: Allocator, edges: []const paths_mod.PathEdge) ![]paths_mod.PathEdge {
    var out = try alloc.alloc(paths_mod.PathEdge, edges.len);
    var initialized: usize = 0;
    errdefer {
        freePathEdgeItems(alloc, out[0..initialized]);
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

fn concatPathEdges(alloc: Allocator, left: []const paths_mod.PathEdge, right: []const paths_mod.PathEdge) ![]paths_mod.PathEdge {
    var out = try alloc.alloc(paths_mod.PathEdge, left.len + right.len);
    var initialized: usize = 0;
    errdefer {
        freePathEdgeItems(alloc, out[0..initialized]);
        if (out.len > 0) alloc.free(out);
    }
    for (left, 0..) |edge, i| {
        out[i] = .{
            .source = try alloc.dupe(u8, edge.source),
            .target = try alloc.dupe(u8, edge.target),
            .edge_type = try alloc.dupe(u8, edge.edge_type),
            .weight = edge.weight,
        };
        initialized += 1;
    }
    for (right, 0..) |edge, i| {
        out[left.len + i] = .{
            .source = try alloc.dupe(u8, edge.source),
            .target = try alloc.dupe(u8, edge.target),
            .edge_type = try alloc.dupe(u8, edge.edge_type),
            .weight = edge.weight,
        };
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
        out[i] = .{
            .source = try alloc.dupe(u8, item.source),
            .target = try alloc.dupe(u8, item.target),
            .edge_type = try alloc.dupe(u8, item.edge_type),
            .weight = item.weight,
        };
        initialized += 1;
    }
    out[out.len - 1] = .{
        .source = try alloc.dupe(u8, source),
        .target = try alloc.dupe(u8, target),
        .edge_type = try alloc.dupe(u8, edge.edge_type),
        .weight = edge.weight,
    };
    return out;
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
    }
}

test "pattern match supports linear alias bindings and cycles" {
    const alloc = std.testing.allocator;

    var arena = std.heap.ArenaAllocator.init(alloc);
    defer arena.deinit();

    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    const dir_path = try std.fmt.allocPrint(alloc, ".zig-cache/tmp/{s}/graph-pattern", .{tmp.sub_path});
    defer alloc.free(dir_path);
    const dir = try alloc.dupeSentinel(u8, dir_path, 0);
    defer alloc.free(dir);
    var doc_store = try @import("../storage/docstore.zig").DocStore.open(arena.allocator(), dir, .{});
    defer doc_store.close();

    const reverse_dir_path = try std.fmt.allocPrint(alloc, ".zig-cache/tmp/{s}/graph-pattern-rev", .{tmp.sub_path});
    defer alloc.free(reverse_dir_path);
    const reverse_dir = try alloc.dupeSentinel(u8, reverse_dir_path, 0);
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
