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
const types = @import("../types.zig");
const graph_query_mod = @import("../../../graph/query.zig");
const graph_pattern_mod = @import("../../../graph/pattern.zig");
const fusion_mod = @import("../../../search/fusion.zig");
const geo_mod = @import("../../../search/geo.zig");
const levenshtein_mod = @import("../../../search/levenshtein.zig");
const regex_mod = @import("../../../search/regex.zig");
const doc_set = @import("../doc_set.zig");

pub const NamedResultSet = struct {
    name: []const u8,
    hits: []const types.SearchHit,
    total_hits: u32,
    resolved_doc_set: ?*const doc_set.ResolvedDocSet = null,
    resolved_doc_set_complete: bool = false,
};

pub const GraphQueryExecutor = struct {
    ctx: ?*anyopaque,
    func: *const fn (
        ctx: ?*anyopaque,
        alloc: Allocator,
        req: types.SearchRequest,
        named: *const types.NamedGraphQuery,
        named_sets: []const NamedResultSet,
    ) anyerror!types.GraphSearchResult,
    resolve_hits_to_doc_set: ?*const fn (
        ctx: ?*anyopaque,
        alloc: Allocator,
        req: types.SearchRequest,
        hits: []const types.SearchHit,
    ) anyerror!doc_set.ResolvedDocSet = null,
    resolve_nodes_to_doc_set: ?*const fn (
        ctx: ?*anyopaque,
        alloc: Allocator,
        req: types.SearchRequest,
        nodes: []const graph_query_mod.GraphResultNode,
    ) anyerror!doc_set.ResolvedDocSet = null,
};

pub const PatternQueryExecutor = struct {
    ctx: ?*anyopaque,
    match_pattern: *const fn (
        ctx: ?*anyopaque,
        alloc: Allocator,
        named: *const types.NamedGraphQuery,
        start_key_refs: []const []const u8,
    ) anyerror![]graph_pattern_mod.PatternMatch,
    load_projected_document: *const fn (
        ctx: ?*anyopaque,
        alloc: Allocator,
        query: graph_query_mod.GraphQuery,
        key: []const u8,
    ) anyerror!?[]u8,
    resolve_doc_set_doc_ids: ?*const fn (
        ctx: ?*anyopaque,
        alloc: Allocator,
        set: *const doc_set.ResolvedDocSet,
        generation: ?u64,
    ) anyerror!?[][]u8 = null,
    lookup_doc_ordinal: ?*const fn (
        ctx: ?*anyopaque,
        alloc: Allocator,
        doc_id: []const u8,
        generation: ?u64,
    ) anyerror!?doc_set.DocOrdinal = null,
};

pub const NonPatternQueryExecutor = struct {
    ctx: ?*anyopaque,
    find_shortest_path: *const fn (
        ctx: ?*anyopaque,
        alloc: Allocator,
        named: *const types.NamedGraphQuery,
        source: []const u8,
        target: []const u8,
    ) anyerror!?types.GraphPath,
    find_k_shortest_paths: *const fn (
        ctx: ?*anyopaque,
        alloc: Allocator,
        named: *const types.NamedGraphQuery,
        source: []const u8,
        target: []const u8,
    ) anyerror![]types.GraphPath,
    execute_graph_query: *const fn (
        ctx: ?*anyopaque,
        alloc: Allocator,
        named: *const types.NamedGraphQuery,
        start_key_refs: []const []const u8,
        target_keys: [][]u8,
    ) anyerror!graph_query_mod.GraphQueryResult,
    load_projected_document: *const fn (
        ctx: ?*anyopaque,
        alloc: Allocator,
        req: types.SearchRequest,
        key: []const u8,
    ) anyerror!?[]u8,
    resolve_doc_set_doc_ids: ?*const fn (
        ctx: ?*anyopaque,
        alloc: Allocator,
        set: *const doc_set.ResolvedDocSet,
        generation: ?u64,
    ) anyerror!?[][]u8 = null,
    lookup_doc_ordinal: ?*const fn (
        ctx: ?*anyopaque,
        alloc: Allocator,
        doc_id: []const u8,
        generation: ?u64,
    ) anyerror!?doc_set.DocOrdinal = null,
};

pub const FusedResultExecutor = struct {
    ctx: ?*anyopaque,
    load_projected_document: *const fn (
        ctx: ?*anyopaque,
        alloc: Allocator,
        req: types.SearchRequest,
        key: []const u8,
    ) anyerror!?[]u8,
};

pub const DocSetDocIdResolver = struct {
    ctx: ?*anyopaque = null,
    func: ?*const fn (
        ctx: ?*anyopaque,
        alloc: Allocator,
        set: *const doc_set.ResolvedDocSet,
        generation: ?u64,
    ) anyerror!?[][]u8 = null,
    identity_read_generation: ?u64 = null,
};

pub const SearchGraphExecutor = struct {
    ctx: ?*anyopaque,
    execute_graph_query: *const fn (
        ctx: ?*anyopaque,
        alloc: Allocator,
        graph_query: graph_query_mod.GraphQuery,
        start_key_refs: []const []const u8,
        target_keys: [][]u8,
    ) anyerror!graph_query_mod.GraphQueryResult,
    load_projected_document: *const fn (
        ctx: ?*anyopaque,
        alloc: Allocator,
        req: types.SearchRequest,
        key: []const u8,
    ) anyerror!?[]u8,
    resolve_doc_set_doc_ids: ?*const fn (
        ctx: ?*anyopaque,
        alloc: Allocator,
        set: *const doc_set.ResolvedDocSet,
        generation: ?u64,
    ) anyerror!?[][]u8 = null,
    lookup_doc_ordinal: ?*const fn (
        ctx: ?*anyopaque,
        alloc: Allocator,
        doc_id: []const u8,
        generation: ?u64,
    ) anyerror!?doc_set.DocOrdinal = null,
};

const VisitState = enum { unvisited, visiting, done };

pub fn sortGraphQueriesByDependencies(alloc: Allocator, queries: []const types.NamedGraphQuery) ![]usize {
    if (queries.len <= 1) {
        const indexes = try alloc.alloc(usize, queries.len);
        for (indexes, 0..) |*index, i| index.* = i;
        return indexes;
    }

    var by_name = std.StringHashMapUnmanaged(usize).empty;
    defer by_name.deinit(alloc);
    for (queries, 0..) |query, i| try by_name.put(alloc, query.name, i);

    var sorted = std.ArrayListUnmanaged(usize).empty;
    defer sorted.deinit(alloc);

    const states = try alloc.alloc(VisitState, queries.len);
    defer alloc.free(states);
    @memset(states, .unvisited);

    for (queries, 0..) |_, i| {
        try visitGraphQuery(alloc, queries, &by_name, states, &sorted, i);
    }

    return try sorted.toOwnedSlice(alloc);
}

pub fn executeGraphQueries(
    alloc: Allocator,
    req: types.SearchRequest,
    graph_queries: []const types.NamedGraphQuery,
    base_hits: []const types.SearchHit,
    base_total_hits: u32,
    executor: GraphQueryExecutor,
) ![]types.GraphSearchResult {
    var base_resolved_doc_set: ?doc_set.ResolvedDocSet = null;
    defer if (base_resolved_doc_set) |*set| set.deinit(alloc);
    var base_resolved_doc_set_ref: ?*const doc_set.ResolvedDocSet = null;
    if (graphQueriesNeedBaseResolvedDocSet(graph_queries)) if (executor.resolve_hits_to_doc_set) |resolve| {
        base_resolved_doc_set = try resolve(executor.ctx, alloc, req, base_hits);
        if (base_resolved_doc_set) |*set| base_resolved_doc_set_ref = set;
    };
    const handoff_total_hits = baseGraphHandoffTotalHits(req, base_hits, base_total_hits);
    const base_resolved_doc_set_complete = @as(u64, handoff_total_hits) <= base_hits.len;
    const named_sets = [_]NamedResultSet{
        .{ .name = "$full_text_results", .hits = base_hits, .total_hits = handoff_total_hits, .resolved_doc_set = base_resolved_doc_set_ref, .resolved_doc_set_complete = base_resolved_doc_set_complete },
        .{ .name = "$fused_results", .hits = base_hits, .total_hits = handoff_total_hits, .resolved_doc_set = base_resolved_doc_set_ref, .resolved_doc_set_complete = base_resolved_doc_set_complete },
        .{ .name = "$embeddings_results", .hits = base_hits, .total_hits = handoff_total_hits, .resolved_doc_set = base_resolved_doc_set_ref, .resolved_doc_set_complete = base_resolved_doc_set_complete },
    };
    return try executeGraphQueriesWithSets(alloc, req, graph_queries, &named_sets, executor);
}

fn baseGraphHandoffTotalHits(req: types.SearchRequest, base_hits: []const types.SearchHit, base_total_hits: u32) u32 {
    var total = @max(base_total_hits, @as(u32, @intCast(base_hits.len)));
    if (req.limit > 0 and base_hits.len >= req.limit) {
        total = @max(total, @as(u32, @intCast(@min(base_hits.len + 1, @as(usize, std.math.maxInt(u32))))));
    }
    return total;
}

fn graphQueriesNeedBaseResolvedDocSet(graph_queries: []const types.NamedGraphQuery) bool {
    for (graph_queries) |query| {
        if (selectorNeedsBaseResolvedDocSet(query.query.start_nodes)) return true;
        if (query.query.target_nodes) |target_nodes| {
            if (selectorNeedsBaseResolvedDocSet(target_nodes)) return true;
        }
    }
    return false;
}

fn selectorNeedsBaseResolvedDocSet(selector: graph_query_mod.NodeSelector) bool {
    return switch (selector) {
        .keys => false,
        .result_ref => |result_ref| result_ref.limit == 0 and isBaseResultRef(result_ref.ref),
    };
}

fn isBaseResultRef(ref: []const u8) bool {
    return std.mem.eql(u8, ref, "$full_text_results") or
        std.mem.eql(u8, ref, "$fused_results") or
        std.mem.eql(u8, ref, "$embeddings_results");
}

pub fn executeGraphQueriesWithSets(
    alloc: Allocator,
    req: types.SearchRequest,
    graph_queries: []const types.NamedGraphQuery,
    named_sets: []const NamedResultSet,
    executor: GraphQueryExecutor,
) ![]types.GraphSearchResult {
    const sorted_query_indexes = try sortGraphQueriesByDependencies(alloc, graph_queries);
    defer alloc.free(sorted_query_indexes);

    var available_sets = std.ArrayListUnmanaged(NamedResultSet).empty;
    defer available_sets.deinit(alloc);
    try available_sets.appendSlice(alloc, named_sets);

    const resolved_sets = try alloc.alloc(?doc_set.ResolvedDocSet, graph_queries.len);
    defer {
        for (resolved_sets) |*maybe_set| {
            if (maybe_set.*) |*set| set.deinit(alloc);
        }
        if (resolved_sets.len > 0) alloc.free(resolved_sets);
    }
    @memset(resolved_sets, null);

    var results = try alloc.alloc(types.GraphSearchResult, graph_queries.len);
    var initialized: usize = 0;
    errdefer {
        for (results[0..initialized]) |*result| result.deinit(alloc);
        alloc.free(results);
    }

    for (sorted_query_indexes, 0..) |query_index, i| {
        results[i] = try executor.func(executor.ctx, alloc, req, &graph_queries[query_index], available_sets.items);
        initialized += 1;
        var resolved_doc_set: ?*const doc_set.ResolvedDocSet = null;
        var resolved_doc_set_complete = false;
        if (executor.resolve_hits_to_doc_set) |resolve| {
            if (results[i].nodes.len == results[i].total_hits) {
                if (executor.resolve_nodes_to_doc_set) |resolve_nodes| {
                    resolved_sets[i] = try resolve_nodes(executor.ctx, alloc, req, results[i].nodes);
                    if (resolved_sets[i]) |*set| {
                        resolved_doc_set = set;
                        resolved_doc_set_complete = true;
                    }
                }
            }
            if (resolved_doc_set == null) {
                resolved_sets[i] = try resolve(executor.ctx, alloc, req, results[i].hits);
                if (resolved_sets[i]) |*set| {
                    resolved_doc_set = set;
                    resolved_doc_set_complete = @as(u64, results[i].total_hits) <= results[i].hits.len;
                }
            }
        }
        try available_sets.append(alloc, .{
            .name = results[i].name,
            .hits = results[i].hits,
            .total_hits = results[i].total_hits,
            .resolved_doc_set = resolved_doc_set,
            .resolved_doc_set_complete = resolved_doc_set_complete,
        });
    }

    return results;
}

fn visitGraphQuery(
    alloc: Allocator,
    queries: []const types.NamedGraphQuery,
    by_name: *std.StringHashMapUnmanaged(usize),
    states: []VisitState,
    sorted: *std.ArrayListUnmanaged(usize),
    index: usize,
) !void {
    switch (states[index]) {
        .done => return,
        .visiting => return error.GraphQueryCycle,
        .unvisited => {},
    }

    states[index] = .visiting;
    const query = queries[index];
    if (graphQueryDependencyName(query.query.start_nodes)) |dep_name| {
        if (by_name.get(dep_name)) |dep_index| try visitGraphQuery(alloc, queries, by_name, states, sorted, dep_index);
    }
    if (query.query.target_nodes) |target_nodes| {
        if (graphQueryDependencyName(target_nodes)) |dep_name| {
            if (by_name.get(dep_name)) |dep_index| try visitGraphQuery(alloc, queries, by_name, states, sorted, dep_index);
        }
    }
    states[index] = .done;
    try sorted.append(alloc, index);
}

fn graphQueryDependencyName(selector: graph_query_mod.NodeSelector) ?[]const u8 {
    return switch (selector) {
        .keys => null,
        .result_ref => |result_ref| blk: {
            if (std.mem.startsWith(u8, result_ref.ref, "$graph_results.")) {
                break :blk result_ref.ref["$graph_results.".len..];
            }
            break :blk null;
        },
    };
}

pub fn applyGraphUnion(alloc: Allocator, result: *types.SearchResult) !void {
    var ordinal_complete = true;
    for (result.hits) |hit| {
        if (hit.doc_ordinal == null) {
            ordinal_complete = false;
            break;
        }
    }
    if (ordinal_complete) {
        for (result.graph_results) |graph_result| {
            for (graph_result.hits) |hit| {
                if (hit.doc_ordinal == null) {
                    ordinal_complete = false;
                    break;
                }
            }
            if (!ordinal_complete) break;
        }
    }
    if (ordinal_complete) return try applyGraphUnionByOrdinal(alloc, result);

    var seen = std.StringHashMapUnmanaged(void).empty;
    defer seen.deinit(alloc);

    var merged = std.ArrayListUnmanaged(types.SearchHit).empty;
    errdefer {
        for (merged.items) |*hit| hit.deinit(alloc);
        merged.deinit(alloc);
    }

    for (result.hits) |hit| {
        try seen.put(alloc, hit.id, {});
        try merged.append(alloc, try hit.clone(alloc));
    }

    for (result.graph_results) |graph_result| {
        for (graph_result.hits) |hit| {
            const gop = try seen.getOrPut(alloc, hit.id);
            if (gop.found_existing) continue;
            try merged.append(alloc, try hit.clone(alloc));
        }
    }

    const owned_hits = try alloc.dupe(types.SearchHit, merged.items);
    merged.deinit(alloc);

    for (result.hits) |*hit| hit.deinit(alloc);
    if (result.hits.len > 0) alloc.free(result.hits);
    result.hits = owned_hits;
    result.total_hits = @intCast(result.hits.len);
}

fn applyGraphUnionByOrdinal(alloc: Allocator, result: *types.SearchResult) !void {
    var seen = std.AutoHashMapUnmanaged(doc_set.DocOrdinal, void).empty;
    defer seen.deinit(alloc);

    var merged = std.ArrayListUnmanaged(types.SearchHit).empty;
    errdefer {
        for (merged.items) |*hit| hit.deinit(alloc);
        merged.deinit(alloc);
    }

    for (result.hits) |hit| {
        try seen.put(alloc, hit.doc_ordinal.?, {});
        try merged.append(alloc, try hit.clone(alloc));
    }

    for (result.graph_results) |graph_result| {
        for (graph_result.hits) |hit| {
            const gop = try seen.getOrPut(alloc, hit.doc_ordinal.?);
            if (gop.found_existing) continue;
            try merged.append(alloc, try hit.clone(alloc));
        }
    }

    const owned_hits = try alloc.dupe(types.SearchHit, merged.items);
    merged.deinit(alloc);

    for (result.hits) |*hit| hit.deinit(alloc);
    if (result.hits.len > 0) alloc.free(result.hits);
    result.hits = owned_hits;
    result.total_hits = @intCast(result.hits.len);
}

pub fn applyGraphIntersection(alloc: Allocator, result: *types.SearchResult) !void {
    var ordinal_complete = true;
    for (result.hits) |hit| {
        if (hit.doc_ordinal == null) {
            ordinal_complete = false;
            break;
        }
    }
    if (ordinal_complete) {
        for (result.graph_results) |graph_result| {
            for (graph_result.hits) |hit| {
                if (hit.doc_ordinal == null) {
                    ordinal_complete = false;
                    break;
                }
            }
            if (!ordinal_complete) break;
        }
    }
    if (ordinal_complete) {
        var graph_ordinals = std.AutoHashMapUnmanaged(doc_set.DocOrdinal, void).empty;
        defer graph_ordinals.deinit(alloc);
        for (result.graph_results) |graph_result| {
            for (graph_result.hits) |hit| try graph_ordinals.put(alloc, hit.doc_ordinal.?, {});
        }
        try applyGraphIntersectionWithOrdinalSet(alloc, result, &graph_ordinals);
        return;
    }

    var graph_ids = std.StringHashMapUnmanaged(void).empty;
    defer graph_ids.deinit(alloc);
    for (result.graph_results) |graph_result| {
        for (graph_result.hits) |hit| try graph_ids.put(alloc, hit.id, {});
    }

    var filtered = std.ArrayListUnmanaged(types.SearchHit).empty;
    errdefer {
        for (filtered.items) |*hit| hit.deinit(alloc);
        filtered.deinit(alloc);
    }

    for (result.hits) |hit| {
        if (!graph_ids.contains(hit.id)) continue;
        try filtered.append(alloc, try hit.clone(alloc));
    }

    const owned_hits = try alloc.dupe(types.SearchHit, filtered.items);
    filtered.deinit(alloc);

    for (result.hits) |*hit| hit.deinit(alloc);
    if (result.hits.len > 0) alloc.free(result.hits);
    result.hits = owned_hits;
    result.total_hits = @intCast(result.hits.len);
}

fn applyGraphIntersectionWithOrdinalSet(
    alloc: Allocator,
    result: *types.SearchResult,
    graph_ordinals: *const std.AutoHashMapUnmanaged(doc_set.DocOrdinal, void),
) !void {
    var filtered = std.ArrayListUnmanaged(types.SearchHit).empty;
    errdefer {
        for (filtered.items) |*hit| hit.deinit(alloc);
        filtered.deinit(alloc);
    }

    for (result.hits) |hit| {
        if (!graph_ordinals.contains(hit.doc_ordinal.?)) continue;
        try filtered.append(alloc, try hit.clone(alloc));
    }

    const owned_hits = try alloc.dupe(types.SearchHit, filtered.items);
    filtered.deinit(alloc);

    for (result.hits) |*hit| hit.deinit(alloc);
    if (result.hits.len > 0) alloc.free(result.hits);
    result.hits = owned_hits;
    result.total_hits = @intCast(result.hits.len);
}

pub fn applyGraphExpandStrategy(
    alloc: Allocator,
    result: *types.SearchResult,
    strategy: ?graph_query_mod.ExpandStrategy,
) !void {
    const mode = strategy orelse return;
    switch (mode) {
        .@"union" => try applyGraphUnion(alloc, result),
        .intersection => try applyGraphIntersection(alloc, result),
    }
}

test "applyGraphUnion deduplicates by ordinals when hit pages are complete" {
    const alloc = std.testing.allocator;

    var result = types.SearchResult{
        .alloc = alloc,
        .hits = try alloc.alloc(types.SearchHit, 1),
        .total_hits = 1,
        .graph_results = try alloc.alloc(types.GraphSearchResult, 1),
    };
    defer result.deinit();

    result.hits[0] = .{
        .id = try alloc.dupe(u8, "doc:a"),
        .doc_ordinal = 7,
    };
    result.graph_results[0] = .{
        .name = try alloc.dupe(u8, "neighbors"),
        .hits = try alloc.alloc(types.SearchHit, 2),
        .total_hits = 2,
    };
    result.graph_results[0].hits[0] = .{
        .id = try alloc.dupe(u8, "graph-alias:a"),
        .doc_ordinal = 7,
    };
    result.graph_results[0].hits[1] = .{
        .id = try alloc.dupe(u8, "doc:b"),
        .doc_ordinal = 9,
    };

    try applyGraphUnion(alloc, &result);

    try std.testing.expectEqual(@as(u32, 2), result.total_hits);
    try std.testing.expectEqual(@as(usize, 2), result.hits.len);
    try std.testing.expectEqualStrings("doc:a", result.hits[0].id);
    try std.testing.expectEqual(@as(?doc_set.DocOrdinal, 7), result.hits[0].doc_ordinal);
    try std.testing.expectEqualStrings("doc:b", result.hits[1].id);
    try std.testing.expectEqual(@as(?doc_set.DocOrdinal, 9), result.hits[1].doc_ordinal);
}

test "applyGraphIntersection uses ordinals when hit pages are complete" {
    const alloc = std.testing.allocator;

    var result = types.SearchResult{
        .alloc = alloc,
        .hits = try alloc.alloc(types.SearchHit, 2),
        .total_hits = 2,
        .graph_results = try alloc.alloc(types.GraphSearchResult, 1),
    };
    defer result.deinit();

    result.hits[0] = .{
        .id = try alloc.dupe(u8, "doc:a"),
        .doc_ordinal = 1,
    };
    result.hits[1] = .{
        .id = try alloc.dupe(u8, "doc:b"),
        .doc_ordinal = 2,
    };
    result.graph_results[0] = .{
        .name = try alloc.dupe(u8, "neighbors"),
        .hits = try alloc.alloc(types.SearchHit, 1),
        .total_hits = 1,
    };
    result.graph_results[0].hits[0] = .{
        .id = try alloc.dupe(u8, "graph-alias:b"),
        .doc_ordinal = 2,
    };

    try applyGraphIntersection(alloc, &result);

    try std.testing.expectEqual(@as(u32, 1), result.total_hits);
    try std.testing.expectEqual(@as(usize, 1), result.hits.len);
    try std.testing.expectEqualStrings("doc:b", result.hits[0].id);
    try std.testing.expectEqual(@as(?doc_set.DocOrdinal, 2), result.hits[0].doc_ordinal);
}

pub fn cloneNamedSetAsResult(alloc: Allocator, set: NamedResultSet, include_stored: bool) !types.SearchResult {
    var hits = try alloc.alloc(types.SearchHit, set.hits.len);
    var initialized: usize = 0;
    errdefer {
        for (hits[0..initialized]) |*hit| hit.deinit(alloc);
        alloc.free(hits);
    }

    for (set.hits, 0..) |hit, i| {
        hits[i] = .{
            .id = try alloc.dupe(u8, hit.id),
            .doc_ordinal = hit.doc_ordinal,
            .score = hit.score,
            .stored_data = if (include_stored and hit.stored_data != null)
                try alloc.dupe(u8, hit.stored_data.?)
            else
                null,
        };
        initialized += 1;
    }

    return .{
        .alloc = alloc,
        .hits = hits,
        .total_hits = set.total_hits,
        .graph_results = &.{},
    };
}

pub fn fuseNamedSets(
    alloc: Allocator,
    req: types.SearchRequest,
    named_sets: []const NamedResultSet,
    executor: FusedResultExecutor,
) !types.SearchResult {
    var ranked_results = try alloc.alloc(fusion_mod.RankedResult, named_sets.len);
    defer alloc.free(ranked_results);

    const ordinal_complete = namedSetsHaveCompleteOrdinals(named_sets);
    var ordinal_fusion_keys = std.AutoHashMapUnmanaged(doc_set.DocOrdinal, OrdinalFusionEntry).empty;
    defer freeOrdinalFusionKeys(alloc, &ordinal_fusion_keys);
    var fusion_key_entries = std.StringHashMapUnmanaged(OrdinalFusionEntry).empty;
    defer fusion_key_entries.deinit(alloc);
    var ordinal_by_id = std.StringHashMapUnmanaged(?doc_set.DocOrdinal).empty;
    defer ordinal_by_id.deinit(alloc);

    for (named_sets, 0..) |set, i| {
        var ranked_hits = try alloc.alloc(fusion_mod.RankedHit, set.hits.len);
        errdefer alloc.free(ranked_hits);
        const distance_ordered = fusionUsesDistanceScore(req, set.name);
        for (set.hits, 0..) |hit, j| {
            const ranked_doc_id = if (ordinal_complete) blk: {
                const entry = try ordinalFusionEntryForHit(alloc, &ordinal_fusion_keys, &fusion_key_entries, hit);
                break :blk entry.key;
            } else blk: {
                if (hit.doc_ordinal) |ordinal| {
                    const gop = try ordinal_by_id.getOrPut(alloc, hit.id);
                    if (!gop.found_existing) {
                        gop.value_ptr.* = ordinal;
                    } else if (gop.value_ptr.*) |existing| {
                        if (existing != ordinal) gop.value_ptr.* = null;
                    }
                }
                break :blk hit.id;
            };
            const raw_score = if (hit.score) |score| score else 0.0;
            ranked_hits[j] = .{
                .doc_id = ranked_doc_id,
                .score = if (distance_ordered) -raw_score else raw_score,
            };
        }
        ranked_results[i] = .{
            .index_name = fusionWeightName(set.name),
            .hits = ranked_hits,
        };
    }
    defer for (ranked_results) |result| alloc.free(result.hits);

    const merge_config = if (req.merge_config) |config|
        fusion_mod.FusionConfig{
            .strategy = config.strategy,
            .rank_constant = config.rank_constant,
            .window_size = config.window_size,
            .weights = config.weights,
        }
    else
        fusion_mod.FusionConfig{};

    const fused = try fusion_mod.fuse(alloc, ranked_results, merge_config);
    defer fusion_mod.freeHits(alloc, fused);
    const pruned = if (req.pruner) |pruner| pruner.prune(fused) else fused;

    const limit = @min(req.limit, @as(u32, @intCast(pruned.len)));
    var hits = try alloc.alloc(types.SearchHit, limit);
    var initialized: usize = 0;
    errdefer {
        for (hits[0..initialized]) |*hit| hit.deinit(alloc);
        alloc.free(hits);
    }

    for (pruned[0..limit], 0..) |hit, i| {
        const representative = if (ordinal_complete)
            fusion_key_entries.get(hit.doc_id) orelse return error.UnsupportedQueryRequest
        else
            null;
        const output_doc_id = if (representative) |entry| entry.representative_doc_id else hit.doc_id;
        const stored_data = if (req.include_stored)
            try executor.load_projected_document(executor.ctx, alloc, req, output_doc_id)
        else
            null;
        hits[i] = .{
            .id = try alloc.dupe(u8, output_doc_id),
            .doc_ordinal = if (representative) |entry| entry.ordinal else if (ordinal_by_id.get(hit.doc_id)) |ordinal| ordinal else null,
            .score = @floatCast(hit.score),
            .stored_data = stored_data,
        };
        initialized += 1;
    }

    return .{
        .alloc = alloc,
        .hits = hits,
        .total_hits = @intCast(pruned.len),
        .graph_results = &.{},
    };
}

const OrdinalFusionEntry = struct {
    key: []const u8,
    representative_doc_id: []const u8,
    ordinal: doc_set.DocOrdinal,
};

fn namedSetsHaveCompleteOrdinals(named_sets: []const NamedResultSet) bool {
    for (named_sets, 0..) |set, set_index| {
        for (set.hits) |hit| {
            if (hit.doc_ordinal == null) return false;
            for (named_sets[set_index..]) |other_set| {
                for (other_set.hits) |other| {
                    if (!std.mem.eql(u8, hit.id, other.id)) continue;
                    if (other.doc_ordinal == null or other.doc_ordinal.? != hit.doc_ordinal.?) return false;
                }
            }
        }
    }
    return true;
}

fn ordinalFusionEntryForHit(
    alloc: Allocator,
    ordinal_entries: *std.AutoHashMapUnmanaged(doc_set.DocOrdinal, OrdinalFusionEntry),
    key_entries: *std.StringHashMapUnmanaged(OrdinalFusionEntry),
    hit: types.SearchHit,
) !OrdinalFusionEntry {
    const ordinal = hit.doc_ordinal orelse return error.UnsupportedQueryRequest;
    if (ordinal_entries.get(ordinal)) |entry| return entry;

    const key = try std.fmt.allocPrint(alloc, "__doc_ord:{d}", .{ordinal});
    errdefer alloc.free(key);
    const entry = OrdinalFusionEntry{
        .key = key,
        .representative_doc_id = hit.id,
        .ordinal = ordinal,
    };
    try ordinal_entries.put(alloc, ordinal, entry);
    errdefer _ = ordinal_entries.remove(ordinal);
    try key_entries.put(alloc, key, entry);
    return entry;
}

fn freeOrdinalFusionKeys(
    alloc: Allocator,
    ordinal_entries: *std.AutoHashMapUnmanaged(doc_set.DocOrdinal, OrdinalFusionEntry),
) void {
    var it = ordinal_entries.valueIterator();
    while (it.next()) |entry| alloc.free(@constCast(entry.key));
    ordinal_entries.deinit(alloc);
}

fn lookupDocOrdinalForGraphHit(
    alloc: Allocator,
    ctx: ?*anyopaque,
    lookup: ?*const fn (
        ctx: ?*anyopaque,
        alloc: Allocator,
        doc_id: []const u8,
        generation: ?u64,
    ) anyerror!?doc_set.DocOrdinal,
    doc_id: []const u8,
    generation: ?u64,
) !?doc_set.DocOrdinal {
    const func = lookup orelse return null;
    return try func(ctx, alloc, doc_id, generation);
}

pub fn convertPatternMatchesToGraphMatches(
    alloc: Allocator,
    raw_matches: []const graph_pattern_mod.PatternMatch,
) ![]types.GraphPatternMatch {
    const matches = try alloc.alloc(types.GraphPatternMatch, raw_matches.len);
    var initialized: usize = 0;
    errdefer {
        for (matches[0..initialized]) |*match| match.deinit(alloc);
        if (matches.len > 0) alloc.free(matches);
    }

    for (raw_matches, 0..) |raw_match, i| {
        const bindings = try alloc.alloc(types.GraphPatternBinding, raw_match.bindings.len);
        var initialized_bindings: usize = 0;
        errdefer {
            for (bindings[0..initialized_bindings]) |*binding| binding.deinit(alloc);
            if (bindings.len > 0) alloc.free(bindings);
        }
        for (raw_match.bindings, 0..) |binding, binding_index| {
            bindings[binding_index] = .{
                .alias = try alloc.dupe(u8, binding.alias),
                .node = .{
                    .key = try alloc.dupe(u8, binding.key),
                    .depth = binding.depth,
                    .distance = @floatFromInt(binding.depth),
                    .path = null,
                    .path_edges = null,
                },
            };
            initialized_bindings += 1;
        }

        const path = try alloc.alloc(graph_query_mod.PathEdgeInfo, raw_match.path.len);
        var initialized_path: usize = 0;
        errdefer {
            for (path[0..initialized_path]) |edge| {
                alloc.free(edge.source);
                alloc.free(edge.target);
                alloc.free(edge.edge_type);
            }
            if (path.len > 0) alloc.free(path);
        }
        for (raw_match.path, 0..) |edge, edge_index| {
            path[edge_index] = .{
                .source = try alloc.dupe(u8, edge.source),
                .target = try alloc.dupe(u8, edge.target),
                .edge_type = try alloc.dupe(u8, edge.edge_type),
                .weight = edge.weight,
            };
            initialized_path += 1;
        }

        matches[i] = .{
            .bindings = bindings,
            .path = path,
        };
        initialized += 1;
    }

    return matches;
}

pub fn executeSinglePatternQueryWithSets(
    alloc: Allocator,
    req: types.SearchRequest,
    named: *const types.NamedGraphQuery,
    named_sets: []const NamedResultSet,
    executor: PatternQueryExecutor,
) !types.GraphSearchResult {
    const start_keys = try resolveGraphSelectorFromSets(alloc, named.query.start_nodes, named_sets, .{
        .ctx = executor.ctx,
        .func = executor.resolve_doc_set_doc_ids,
        .identity_read_generation = req.identity_read_generation,
    });
    defer freeOwnedKeySlice(alloc, start_keys);
    const start_key_refs = try castOwnedKeysToConst(alloc, start_keys);
    defer alloc.free(start_key_refs);

    const raw_matches = try executor.match_pattern(executor.ctx, alloc, named, start_key_refs);
    defer graph_pattern_mod.freeMatches(alloc, raw_matches);

    const matches = try convertPatternMatchesToGraphMatches(alloc, raw_matches);
    errdefer {
        for (matches) |*match| match.deinit(alloc);
        if (matches.len > 0) alloc.free(matches);
    }

    const hits = try buildPatternDocumentHits(alloc, named.query, req.identity_read_generation, matches, executor);
    errdefer {
        for (hits) |*hit| hit.deinit(alloc);
        if (hits.len > 0) alloc.free(hits);
    }

    return .{
        .name = try alloc.dupe(u8, named.name),
        .nodes = &.{},
        .paths = &.{},
        .matches = matches,
        .hits = hits,
        .total_hits = @intCast(raw_matches.len),
    };
}

pub fn executeSingleNonPatternQueryWithSets(
    alloc: Allocator,
    req: types.SearchRequest,
    named: *const types.NamedGraphQuery,
    named_sets: []const NamedResultSet,
    executor: NonPatternQueryExecutor,
) !types.GraphSearchResult {
    const start_keys = try resolveGraphSelectorFromSets(alloc, named.query.start_nodes, named_sets, .{
        .ctx = executor.ctx,
        .func = executor.resolve_doc_set_doc_ids,
        .identity_read_generation = req.identity_read_generation,
    });
    defer freeOwnedKeySlice(alloc, start_keys);
    const target_keys = if (named.query.target_nodes) |target_nodes|
        try resolveGraphSelectorFromSets(alloc, target_nodes, named_sets, .{
            .ctx = executor.ctx,
            .func = executor.resolve_doc_set_doc_ids,
            .identity_read_generation = req.identity_read_generation,
        })
    else
        try alloc.alloc([]u8, 0);
    defer freeOwnedKeySlice(alloc, target_keys);

    switch (named.query.query_type) {
        .shortest_path => {
            if (start_keys.len == 0 or target_keys.len == 0) {
                return emptyGraphSearchResult(alloc, named.name);
            }
            const path = try executor.find_shortest_path(executor.ctx, alloc, named, start_keys[0], target_keys[0]);
            const paths = if (path) |owned_path| blk: {
                var items = try alloc.alloc(types.GraphPath, 1);
                items[0] = owned_path;
                break :blk items;
            } else try alloc.alloc(types.GraphPath, 0);
            return .{
                .name = try alloc.dupe(u8, named.name),
                .nodes = &.{},
                .paths = paths,
                .matches = &.{},
                .hits = try alloc.alloc(types.SearchHit, 0),
                .total_hits = @intCast(paths.len),
            };
        },
        .k_shortest_paths => {
            if (start_keys.len == 0 or target_keys.len == 0 or named.query.k == 0) {
                return emptyGraphSearchResult(alloc, named.name);
            }
            const paths = try executor.find_k_shortest_paths(executor.ctx, alloc, named, start_keys[0], target_keys[0]);
            return .{
                .name = try alloc.dupe(u8, named.name),
                .nodes = &.{},
                .paths = paths,
                .matches = &.{},
                .hits = try alloc.alloc(types.SearchHit, 0),
                .total_hits = @intCast(paths.len),
            };
        },
        .pattern => return error.UnsupportedQueryRequest,
        else => {},
    }

    const start_key_refs = try castOwnedKeysToConst(alloc, start_keys);
    defer alloc.free(start_key_refs);

    var graph_result = try executor.execute_graph_query(executor.ctx, alloc, named, start_key_refs, target_keys);
    errdefer graph_result.deinit(alloc);

    const total_hits: u32 = @intCast(graph_result.nodes.len);
    const start = @min(req.offset, total_hits);
    const end = @min(start + req.limit, total_hits);

    var hits = try alloc.alloc(types.SearchHit, end - start);
    var initialized: usize = 0;
    errdefer {
        for (hits[0..initialized]) |*hit| hit.deinit(alloc);
        alloc.free(hits);
    }

    for (graph_result.nodes[@intCast(start)..@intCast(end)], 0..) |node, i| {
        const stored_data = if (named.query.include_documents)
            try executor.load_projected_document(executor.ctx, alloc, req, node.key)
        else
            null;

        hits[i] = .{
            .id = try alloc.dupe(u8, node.key),
            .doc_ordinal = try lookupDocOrdinalForGraphHit(alloc, executor.ctx, executor.lookup_doc_ordinal, node.key, req.identity_read_generation),
            .score = @floatCast(node.distance),
            .stored_data = stored_data,
        };
        initialized += 1;
    }

    const nodes = graph_result.nodes;
    graph_result.nodes = &.{};

    return .{
        .name = try alloc.dupe(u8, named.name),
        .nodes = nodes,
        .paths = &.{},
        .matches = &.{},
        .hits = hits,
        .total_hits = total_hits,
    };
}

pub fn executeSearchGraphWithSets(
    alloc: Allocator,
    req: types.SearchRequest,
    graph_query: graph_query_mod.GraphQuery,
    named_sets: []const NamedResultSet,
    executor: SearchGraphExecutor,
) !types.SearchResult {
    const start_keys = try resolveGraphSelectorFromSets(alloc, graph_query.start_nodes, named_sets, .{
        .ctx = executor.ctx,
        .func = executor.resolve_doc_set_doc_ids,
        .identity_read_generation = req.identity_read_generation,
    });
    defer freeOwnedKeySlice(alloc, start_keys);
    const start_key_refs = try castOwnedKeysToConst(alloc, start_keys);
    defer alloc.free(start_key_refs);
    const target_keys = if (graph_query.target_nodes) |target_nodes|
        try resolveGraphSelectorFromSets(alloc, target_nodes, named_sets, .{
            .ctx = executor.ctx,
            .func = executor.resolve_doc_set_doc_ids,
            .identity_read_generation = req.identity_read_generation,
        })
    else
        try alloc.alloc([]u8, 0);
    defer freeOwnedKeySlice(alloc, target_keys);

    var result = try executor.execute_graph_query(executor.ctx, alloc, graph_query, start_key_refs, target_keys);
    defer result.deinit(alloc);

    const total_hits: u32 = @intCast(result.nodes.len);
    const start = @min(req.offset, total_hits);
    const end = @min(start + req.limit, total_hits);

    var hits = try alloc.alloc(types.SearchHit, end - start);
    var initialized: usize = 0;
    errdefer {
        for (hits[0..initialized]) |*hit| hit.deinit(alloc);
        alloc.free(hits);
    }

    for (result.nodes[@intCast(start)..@intCast(end)], 0..) |node, i| {
        const stored_data = if (graph_query.include_documents)
            try executor.load_projected_document(executor.ctx, alloc, req, node.key)
        else
            null;

        hits[i] = .{
            .id = try alloc.dupe(u8, node.key),
            .doc_ordinal = try lookupDocOrdinalForGraphHit(alloc, executor.ctx, executor.lookup_doc_ordinal, node.key, req.identity_read_generation),
            .score = @floatCast(node.distance),
            .stored_data = stored_data,
        };
        initialized += 1;
    }

    return .{
        .alloc = alloc,
        .hits = hits,
        .total_hits = total_hits,
        .graph_results = &.{},
    };
}

pub fn executeSearchGraph(
    alloc: Allocator,
    req: types.SearchRequest,
    graph_query: graph_query_mod.GraphQuery,
    base_hits: ?[]const types.SearchHit,
    executor: SearchGraphExecutor,
) !types.SearchResult {
    const start_keys = try resolveGraphSelector(alloc, graph_query.start_nodes, base_hits);
    defer freeOwnedKeySlice(alloc, start_keys);
    const start_key_refs = try castOwnedKeysToConst(alloc, start_keys);
    defer alloc.free(start_key_refs);
    const target_keys = if (graph_query.target_nodes) |target_nodes|
        try resolveGraphSelector(alloc, target_nodes, base_hits)
    else
        try alloc.alloc([]u8, 0);
    defer freeOwnedKeySlice(alloc, target_keys);

    var result = try executor.execute_graph_query(executor.ctx, alloc, graph_query, start_key_refs, target_keys);
    defer result.deinit(alloc);

    const total_hits: u32 = @intCast(result.nodes.len);
    const start = @min(req.offset, total_hits);
    const end = @min(start + req.limit, total_hits);

    var hits = try alloc.alloc(types.SearchHit, end - start);
    var initialized: usize = 0;
    errdefer {
        for (hits[0..initialized]) |*hit| hit.deinit(alloc);
        alloc.free(hits);
    }

    for (result.nodes[@intCast(start)..@intCast(end)], 0..) |node, i| {
        const stored_data = if (graph_query.include_documents)
            try executor.load_projected_document(executor.ctx, alloc, req, node.key)
        else
            null;

        hits[i] = .{
            .id = try alloc.dupe(u8, node.key),
            .doc_ordinal = try lookupDocOrdinalForGraphHit(alloc, executor.ctx, executor.lookup_doc_ordinal, node.key, req.identity_read_generation),
            .score = @floatCast(node.distance),
            .stored_data = stored_data,
        };
        initialized += 1;
    }

    return .{
        .alloc = alloc,
        .hits = hits,
        .total_hits = total_hits,
        .graph_results = &.{},
    };
}

fn buildPatternDocumentHits(
    alloc: Allocator,
    query: graph_query_mod.GraphQuery,
    identity_read_generation: ?u64,
    matches: []const types.GraphPatternMatch,
    executor: PatternQueryExecutor,
) ![]types.SearchHit {
    var hits = std.ArrayListUnmanaged(types.SearchHit).empty;
    errdefer {
        for (hits.items) |*hit| hit.deinit(alloc);
        hits.deinit(alloc);
    }

    var seen = std.StringHashMapUnmanaged(void).empty;
    defer {
        var it = seen.keyIterator();
        while (it.next()) |key| alloc.free(key.*);
        seen.deinit(alloc);
    }

    for (matches) |match| {
        for (match.bindings) |binding| {
            if (seen.contains(binding.node.key)) continue;
            try seen.put(alloc, try alloc.dupe(u8, binding.node.key), {});
            const stored_data = if (query.include_documents)
                try executor.load_projected_document(executor.ctx, alloc, query, binding.node.key)
            else
                null;
            try hits.append(alloc, .{
                .id = try alloc.dupe(u8, binding.node.key),
                .doc_ordinal = if (executor.lookup_doc_ordinal) |lookup|
                    try lookup(executor.ctx, alloc, binding.node.key, identity_read_generation)
                else
                    null,
                .score = @floatCast(binding.node.distance),
                .stored_data = stored_data,
            });
        }
    }

    return try hits.toOwnedSlice(alloc);
}

fn emptyGraphSearchResult(alloc: Allocator, name: []const u8) !types.GraphSearchResult {
    return .{
        .name = try alloc.dupe(u8, name),
        .nodes = &.{},
        .paths = &.{},
        .matches = &.{},
        .hits = try alloc.alloc(types.SearchHit, 0),
        .total_hits = 0,
    };
}

fn fusionWeightName(name: []const u8) []const u8 {
    if (std.mem.eql(u8, name, "$full_text_results")) return "full_text";
    if (std.mem.startsWith(u8, name, "$full_text_results.")) return name["$full_text_results.".len..];
    if (std.mem.startsWith(u8, name, "$aknn_results.")) return name["$aknn_results.".len..];
    return name;
}

fn fusionUsesDistanceScore(req: types.SearchRequest, name: []const u8) bool {
    if (std.mem.eql(u8, name, "$embeddings_results")) return req.dense != null;
    if (std.mem.eql(u8, name, "dense")) return req.dense != null;
    if (std.mem.startsWith(u8, name, "$aknn_results.")) return true;
    for (req.dense_queries) |dense_query| {
        if (std.mem.eql(u8, dense_query.name, name)) return true;
    }
    return false;
}

fn castOwnedKeysToConst(alloc: Allocator, keys: [][]u8) ![]const []const u8 {
    var out = try alloc.alloc([]const u8, keys.len);
    for (keys, 0..) |key, i| out[i] = key;
    return out;
}

fn freeOwnedKeySlice(alloc: Allocator, keys: [][]u8) void {
    for (keys) |key| alloc.free(key);
    if (keys.len > 0) alloc.free(keys);
}

pub fn resolveGraphSelector(alloc: Allocator, selector: graph_query_mod.NodeSelector, base_hits: ?[]const types.SearchHit) ![][]u8 {
    return switch (selector) {
        .keys => |keys| blk: {
            var duped = try alloc.alloc([]u8, keys.len);
            var initialized: usize = 0;
            errdefer {
                for (duped[0..initialized]) |key| alloc.free(key);
                alloc.free(duped);
            }
            for (keys, 0..) |key, i| {
                duped[i] = try alloc.dupe(u8, key);
                initialized += 1;
            }
            break :blk duped;
        },
        .result_ref => |result_ref| blk: {
            const hits = base_hits orelse return error.GraphResultRefNotImplemented;
            if (!std.mem.eql(u8, result_ref.ref, "$full_text_results") and
                !std.mem.eql(u8, result_ref.ref, "$fused_results") and
                !std.mem.eql(u8, result_ref.ref, "$embeddings_results"))
            {
                return error.GraphResultRefNotImplemented;
            }

            const count: usize = if (result_ref.limit == 0) hits.len else @min(hits.len, result_ref.limit);
            var duped = try alloc.alloc([]u8, count);
            var initialized: usize = 0;
            errdefer {
                for (duped[0..initialized]) |key| alloc.free(key);
                alloc.free(duped);
            }
            for (hits[0..count], 0..) |hit, i| {
                duped[i] = try alloc.dupe(u8, hit.id);
                initialized += 1;
            }
            break :blk duped;
        },
    };
}

pub fn storedDocMatchesPatternFilter(alloc: Allocator, key: []const u8, stored: []const u8, filter_query_json: []const u8) !bool {
    var parsed = try std.json.parseFromSlice(std.json.Value, alloc, stored, .{});
    defer parsed.deinit();
    var filter_query = try std.json.parseFromSlice(std.json.Value, alloc, filter_query_json, .{});
    defer filter_query.deinit();
    return try jsonDocMatchesPatternFilter(alloc, key, parsed.value, filter_query.value);
}

pub fn jsonDocMatchesPatternFilter(alloc: Allocator, key: []const u8, doc: std.json.Value, filter_query: std.json.Value) !bool {
    if (filter_query != .object) return error.InvalidArgument;

    if (filter_query.object.get("match_all") != null) return true;
    if (filter_query.object.get("match_none") != null) return false;
    if (filter_query.object.get("doc_id")) |doc_id| return docIdMatchesPatternKey(key, doc_id);

    if (filter_query.object.get("conjuncts")) |conjuncts| {
        if (conjuncts != .array) return error.InvalidArgument;
        for (conjuncts.array.items) |item| {
            if (!(try jsonDocMatchesPatternFilter(alloc, key, doc, item))) return false;
        }
        return true;
    }

    if (filter_query.object.get("disjuncts")) |disjuncts| {
        if (disjuncts != .array) return error.InvalidArgument;
        for (disjuncts.array.items) |item| {
            if (try jsonDocMatchesPatternFilter(alloc, key, doc, item)) return true;
        }
        return false;
    }

    if (filter_query.object.get("bool")) |bool_query| {
        if (bool_query != .object) return error.InvalidArgument;

        if (bool_query.object.get("must")) |must| {
            if (must != .array or must.array.items.len == 0) return error.InvalidArgument;
            for (must.array.items) |item| {
                if (!(try jsonDocMatchesPatternFilter(alloc, key, doc, item))) return false;
            }
        }

        var saw_should = false;
        if (bool_query.object.get("should")) |should| {
            if (should != .array or should.array.items.len == 0) return error.InvalidArgument;
            saw_should = true;
            var matched = false;
            for (should.array.items) |item| {
                if (try jsonDocMatchesPatternFilter(alloc, key, doc, item)) {
                    matched = true;
                    break;
                }
            }
            if (!matched) return false;
        }

        if (bool_query.object.get("must_not")) |must_not| {
            if (must_not != .array or must_not.array.items.len == 0) return error.InvalidArgument;
            for (must_not.array.items) |item| {
                if (try jsonDocMatchesPatternFilter(alloc, key, doc, item)) return false;
            }
        }

        if (bool_query.object.count() == 0) return error.InvalidArgument;
        if (!saw_should and bool_query.object.get("must") == null and bool_query.object.get("must_not") == null) {
            return error.InvalidArgument;
        }
        return true;
    }

    const field = try extractPatternField(filter_query);
    var values = std.ArrayListUnmanaged(std.json.Value).empty;
    defer values.deinit(alloc);
    if (std.mem.indexOfScalar(u8, field, '.') == null) {
        try collectJsonValuesAtSingleSegment(alloc, doc, field, &values);
    } else {
        var path_stack: [8][]const u8 = undefined;
        var path_len: usize = 0;
        var path_heap = std.ArrayListUnmanaged([]const u8).empty;
        defer path_heap.deinit(alloc);
        var parts = std.mem.splitScalar(u8, field, '.');
        while (parts.next()) |part| {
            if (path_heap.capacity > 0) {
                try path_heap.append(alloc, part);
                continue;
            }
            if (path_len < path_stack.len) {
                path_stack[path_len] = part;
                path_len += 1;
                continue;
            }
            try path_heap.ensureTotalCapacity(alloc, path_len + 1);
            for (path_stack[0..path_len]) |existing| {
                path_heap.appendAssumeCapacity(existing);
            }
            try path_heap.append(alloc, part);
        }
        const path_items = if (path_heap.capacity > 0) path_heap.items else path_stack[0..path_len];
        if (path_items.len == 0) return false;
        try collectJsonValuesAtPath(alloc, doc, path_items, 0, &values);
    }

    var predicate_arena = std.heap.ArenaAllocator.init(alloc);
    defer predicate_arena.deinit();
    return try (try compilePatternFieldPredicate(predicate_arena.allocator(), filter_query)).matches(alloc, values.items);
}

pub fn patternFilterNeedsStoredDoc(filter_query: std.json.Value) !bool {
    if (filter_query != .object) return error.InvalidArgument;

    if (filter_query.object.get("match_all") != null) return false;
    if (filter_query.object.get("match_none") != null) return false;
    if (filter_query.object.get("doc_id") != null) return false;

    if (filter_query.object.get("conjuncts")) |conjuncts| {
        if (conjuncts != .array) return error.InvalidArgument;
        for (conjuncts.array.items) |item| {
            if (try patternFilterNeedsStoredDoc(item)) return true;
        }
        return false;
    }

    if (filter_query.object.get("disjuncts")) |disjuncts| {
        if (disjuncts != .array) return error.InvalidArgument;
        for (disjuncts.array.items) |item| {
            if (try patternFilterNeedsStoredDoc(item)) return true;
        }
        return false;
    }

    if (filter_query.object.get("bool")) |bool_query| {
        if (bool_query != .object) return error.InvalidArgument;

        if (bool_query.object.get("must")) |must| {
            if (must != .array or must.array.items.len == 0) return error.InvalidArgument;
            for (must.array.items) |item| {
                if (try patternFilterNeedsStoredDoc(item)) return true;
            }
        }

        var saw_should = false;
        if (bool_query.object.get("should")) |should| {
            if (should != .array or should.array.items.len == 0) return error.InvalidArgument;
            saw_should = true;
            for (should.array.items) |item| {
                if (try patternFilterNeedsStoredDoc(item)) return true;
            }
        }

        if (bool_query.object.get("must_not")) |must_not| {
            if (must_not != .array or must_not.array.items.len == 0) return error.InvalidArgument;
            for (must_not.array.items) |item| {
                if (try patternFilterNeedsStoredDoc(item)) return true;
            }
        }

        if (bool_query.object.count() == 0) return error.InvalidArgument;
        if (!saw_should and bool_query.object.get("must") == null and bool_query.object.get("must_not") == null) {
            return error.InvalidArgument;
        }
        return false;
    }

    return true;
}

pub const CompiledPatternFilter = union(enum) {
    match_all,
    match_none,
    doc_id: []const []const u8,
    conjuncts: []CompiledPatternFilter,
    disjuncts: []CompiledPatternFilter,
    bool_query: BoolQuery,
    field_matcher: FieldMatcher,

    pub const BoolQuery = struct {
        must: []CompiledPatternFilter = &.{},
        should: []CompiledPatternFilter = &.{},
        must_not: []CompiledPatternFilter = &.{},
    };

    pub const FieldPath = union(enum) {
        single: []const u8,
        multi: []const []const u8,

        fn collectValues(self: FieldPath, alloc: Allocator, doc: std.json.Value, out: *std.ArrayListUnmanaged(std.json.Value)) !void {
            switch (self) {
                .single => |segment| try collectJsonValuesAtSingleSegment(alloc, doc, segment, out),
                .multi => |segments| try collectJsonValuesAtPath(alloc, doc, segments, 0, out),
            }
        }
    };

    pub const FieldMatcher = struct {
        path: FieldPath,
        predicate: FieldPredicate,
    };

    pub const FieldPredicate = union(enum) {
        term: []const u8,
        terms: []const []const u8,
        match: []const u8,
        prefix: []const u8,
        wildcard: []const u8,
        regexp: regex_mod.RegexAutomaton,
        fuzzy: CompiledFuzzy,
        exists,
        numeric_range: std.json.Value,
        standard_range: std.json.Value,
        date_range: std.json.Value,
        bool_field: std.json.Value,
        term_range: std.json.Value,
        ip_range: std.json.Value,
        geo_distance: std.json.Value,
        geo_bbox: std.json.Value,
        geo_shape: std.json.Value,

        fn matches(self: FieldPredicate, alloc: Allocator, values: []const std.json.Value) !bool {
            return switch (self) {
                .term => |value| jsonValuesContainTerm(values, value),
                .terms => |terms| jsonValuesContainAnyTerm(values, terms),
                .match => |value| jsonValuesContainMatch(values, value),
                .prefix => |value| jsonValuesContainPrefix(values, value),
                .wildcard => |value| jsonValuesContainWildcard(values, value),
                .regexp => |value| jsonValuesContainCompiledRegexp(values, @constCast(&value)),
                .fuzzy => |value| try jsonValuesContainFuzzy(alloc, values, value),
                .exists => values.len > 0,
                .numeric_range => |value| try jsonValuesContainNumericRange(values, value),
                .standard_range => |value| try jsonValuesContainStandardRange(values, value),
                .date_range => |value| try jsonValuesContainDateRange(values, value),
                .bool_field => |value| try jsonValuesContainBoolField(values, value),
                .term_range => |value| try jsonValuesContainTermRange(values, value),
                .ip_range => |value| try jsonValuesContainIpRange(values, value),
                .geo_distance => |value| try jsonValuesContainGeoDistance(values, value),
                .geo_bbox => |value| try jsonValuesContainGeoBBox(values, value),
                .geo_shape => |value| try jsonValuesContainGeoShape(alloc, values, value),
            };
        }
    };

    pub const CompiledFuzzy = struct {
        term: []const u8,
        folded_term: []const u8,
        max_edits: u8,
        prefix_len: u8,
    };

    pub fn needsStoredDoc(self: CompiledPatternFilter) bool {
        return switch (self) {
            .match_all, .match_none, .doc_id => false,
            .conjuncts => |items| blk: {
                for (items) |item| if (item.needsStoredDoc()) break :blk true;
                break :blk false;
            },
            .disjuncts => |items| blk: {
                for (items) |item| if (item.needsStoredDoc()) break :blk true;
                break :blk false;
            },
            .bool_query => |bool_query| blk: {
                for (bool_query.must) |item| if (item.needsStoredDoc()) break :blk true;
                for (bool_query.should) |item| if (item.needsStoredDoc()) break :blk true;
                for (bool_query.must_not) |item| if (item.needsStoredDoc()) break :blk true;
                break :blk false;
            },
            .field_matcher => true,
        };
    }

    pub fn matches(self: CompiledPatternFilter, alloc: Allocator, key: []const u8, doc: std.json.Value) !bool {
        return switch (self) {
            .match_all => true,
            .match_none => false,
            .doc_id => |ids| blk: {
                for (ids) |id| if (std.mem.eql(u8, key, id)) break :blk true;
                break :blk false;
            },
            .conjuncts => |items| blk: {
                for (items) |item| {
                    if (!(try item.matches(alloc, key, doc))) break :blk false;
                }
                break :blk true;
            },
            .disjuncts => |items| blk: {
                for (items) |item| {
                    if (try item.matches(alloc, key, doc)) break :blk true;
                }
                break :blk false;
            },
            .bool_query => |bool_query| blk: {
                for (bool_query.must) |item| {
                    if (!(try item.matches(alloc, key, doc))) break :blk false;
                }
                if (bool_query.should.len > 0) {
                    var matched = false;
                    for (bool_query.should) |item| {
                        if (try item.matches(alloc, key, doc)) {
                            matched = true;
                            break;
                        }
                    }
                    if (!matched) break :blk false;
                }
                for (bool_query.must_not) |item| {
                    if (try item.matches(alloc, key, doc)) break :blk false;
                }
                break :blk true;
            },
            .field_matcher => |matcher| blk: {
                var values = std.ArrayListUnmanaged(std.json.Value).empty;
                defer values.deinit(alloc);
                try matcher.path.collectValues(alloc, doc, &values);
                break :blk try matcher.predicate.matches(alloc, values.items);
            },
        };
    }
};

pub fn compilePatternFilter(alloc: Allocator, filter_query: std.json.Value) anyerror!CompiledPatternFilter {
    if (filter_query != .object) return error.InvalidArgument;

    if (filter_query.object.get("match_all") != null) return .match_all;
    if (filter_query.object.get("match_none") != null) return .match_none;
    if (filter_query.object.get("doc_id")) |doc_id| {
        return .{ .doc_id = try compilePatternDocIds(alloc, doc_id) };
    }
    if (filter_query.object.get("conjuncts")) |conjuncts| {
        return .{ .conjuncts = try compilePatternFilterArray(alloc, conjuncts) };
    }
    if (filter_query.object.get("disjuncts")) |disjuncts| {
        return .{ .disjuncts = try compilePatternFilterArray(alloc, disjuncts) };
    }
    if (filter_query.object.get("bool")) |bool_query| {
        if (bool_query != .object) return error.InvalidArgument;
        var compiled = CompiledPatternFilter.BoolQuery{};
        if (bool_query.object.get("must")) |must| compiled.must = try compilePatternFilterArray(alloc, must);
        if (bool_query.object.get("should")) |should| compiled.should = try compilePatternFilterArray(alloc, should);
        if (bool_query.object.get("must_not")) |must_not| compiled.must_not = try compilePatternFilterArray(alloc, must_not);
        if (bool_query.object.count() == 0) return error.InvalidArgument;
        if (compiled.should.len == 0 and compiled.must.len == 0 and compiled.must_not.len == 0) return error.InvalidArgument;
        return .{ .bool_query = compiled };
    }

    return .{
        .field_matcher = .{
            .path = try compilePatternFieldPath(alloc, try extractPatternField(filter_query)),
            .predicate = try compilePatternFieldPredicate(alloc, filter_query),
        },
    };
}

fn compilePatternDocIds(alloc: Allocator, doc_id: std.json.Value) ![]const []const u8 {
    const ids = switch (doc_id) {
        .object => (doc_id.object.get("ids") orelse return error.InvalidArgument),
        .array => doc_id,
        else => return error.InvalidArgument,
    };
    if (ids != .array or ids.array.items.len == 0) return error.InvalidArgument;
    const compiled = try alloc.alloc([]const u8, ids.array.items.len);
    for (ids.array.items, 0..) |item, i| {
        if (item != .string) return error.InvalidArgument;
        compiled[i] = item.string;
    }
    return compiled;
}

fn compilePatternFilterArray(alloc: Allocator, items: std.json.Value) anyerror![]CompiledPatternFilter {
    if (items != .array or items.array.items.len == 0) return error.InvalidArgument;
    const compiled = try alloc.alloc(CompiledPatternFilter, items.array.items.len);
    for (items.array.items, 0..) |item, i| {
        compiled[i] = try compilePatternFilter(alloc, item);
    }
    return compiled;
}

fn compilePatternFieldPath(alloc: Allocator, field: []const u8) !CompiledPatternFilter.FieldPath {
    if (std.mem.indexOfScalar(u8, field, '.') == null) {
        return .{ .single = field };
    }
    var parts = std.mem.splitScalar(u8, field, '.');
    var count: usize = 0;
    while (parts.next()) |_| count += 1;
    if (count == 0) return error.InvalidArgument;
    const compiled = try alloc.alloc([]const u8, count);
    var parts2 = std.mem.splitScalar(u8, field, '.');
    var i: usize = 0;
    while (parts2.next()) |part| : (i += 1) {
        compiled[i] = part;
    }
    return .{ .multi = compiled };
}

fn extractPatternField(filter_query: std.json.Value) ![]const u8 {
    return blk: {
        if (filter_query.object.get("term")) |term| {
            break :blk try extractPatternFieldFromStringShape(term, "term");
        }
        if (filter_query.object.get("terms")) |terms| {
            break :blk try extractPatternTermsField(terms);
        }
        if (filter_query.object.get("match")) |match| {
            break :blk try extractPatternFieldFromStringShape(match, "text");
        }
        if (filter_query.object.get("prefix")) |prefix| {
            break :blk try extractPatternFieldFromStringShape(prefix, "prefix");
        }
        if (filter_query.object.get("wildcard")) |wildcard| {
            break :blk try extractPatternFieldFromStringShape(wildcard, "pattern");
        }
        if (filter_query.object.get("regexp")) |regexp| {
            break :blk try extractPatternFieldFromStringShape(regexp, "pattern");
        }
        if (filter_query.object.get("fuzzy")) |fuzzy| {
            break :blk try extractPatternFuzzyField(fuzzy);
        }
        if (filter_query.object.get("exists")) |exists| {
            break :blk try extractPatternExistsField(exists);
        }
        if (filter_query.object.get("numeric_range")) |range_query| {
            if (range_query != .object) return error.InvalidArgument;
            const field = patternFieldOrPathValue(range_query.object) orelse return error.InvalidArgument;
            if (field != .string) return error.InvalidArgument;
            break :blk field.string;
        }
        if (filter_query.object.get("range")) |range_query| {
            break :blk try extractStandardRangeField(range_query);
        }
        if (filter_query.object.get("date_range")) |range_query| {
            if (range_query != .object) return error.InvalidArgument;
            const field = patternFieldOrPathValue(range_query.object) orelse return error.InvalidArgument;
            if (field != .string) return error.InvalidArgument;
            break :blk field.string;
        }
        if (filter_query.object.get("bool_field")) |bool_query| {
            if (bool_query != .object) return error.InvalidArgument;
            const field = patternFieldOrPathValue(bool_query.object) orelse return error.InvalidArgument;
            if (field != .string) return error.InvalidArgument;
            break :blk field.string;
        }
        if (filter_query.object.get("term_range")) |range_query| {
            if (range_query != .object) return error.InvalidArgument;
            const field = patternFieldOrPathValue(range_query.object) orelse return error.InvalidArgument;
            if (field != .string) return error.InvalidArgument;
            break :blk field.string;
        }
        if (filter_query.object.get("ip_range")) |range_query| {
            if (range_query != .object) return error.InvalidArgument;
            const field = patternFieldOrPathValue(range_query.object) orelse return error.InvalidArgument;
            if (field != .string) return error.InvalidArgument;
            break :blk field.string;
        }
        if (filter_query.object.get("geo_distance")) |geo_query| {
            if (geo_query != .object) return error.InvalidArgument;
            const field = patternFieldOrPathValue(geo_query.object) orelse return error.InvalidArgument;
            if (field != .string) return error.InvalidArgument;
            break :blk field.string;
        }
        if (filter_query.object.get("geo_bbox")) |geo_query| {
            if (geo_query != .object) return error.InvalidArgument;
            const field = patternFieldOrPathValue(geo_query.object) orelse return error.InvalidArgument;
            if (field != .string) return error.InvalidArgument;
            break :blk field.string;
        }
        if (filter_query.object.get("geo_shape")) |geo_query| {
            if (geo_query != .object) return error.InvalidArgument;
            const field = patternFieldOrPathValue(geo_query.object) orelse return error.InvalidArgument;
            if (field != .string) return error.InvalidArgument;
            break :blk field.string;
        }
        return error.InvalidArgument;
    };
}

const PatternFieldString = struct {
    field: []const u8,
    value: []const u8,
};

const PatternFieldTerms = struct {
    field: []const u8,
    terms: []const []const u8,
};

fn patternFieldOrPathValue(object: std.json.ObjectMap) ?std.json.Value {
    return object.get("field") orelse object.get("path");
}

fn extractPatternFieldFromStringShape(value: std.json.Value, value_key: []const u8) ![]const u8 {
    if (value != .object) return error.InvalidArgument;
    if (patternFieldOrPathValue(value.object)) |field_value| {
        if (field_value != .string) return error.InvalidArgument;
        _ = value.object.get(value_key) orelse value.object.get("value") orelse return error.InvalidArgument;
        return field_value.string;
    }
    if (value.object.count() != 1) return error.InvalidArgument;
    var it = value.object.iterator();
    const entry = it.next() orelse return error.InvalidArgument;
    return entry.key_ptr.*;
}

fn extractPatternFieldString(alloc: Allocator, value: std.json.Value, value_key: []const u8) !PatternFieldString {
    if (value != .object) return error.InvalidArgument;
    if (patternFieldOrPathValue(value.object)) |field_value| {
        if (field_value != .string) return error.InvalidArgument;
        const raw_value = value.object.get(value_key) orelse value.object.get("value") orelse return error.InvalidArgument;
        return .{ .field = field_value.string, .value = try jsonScalarTermAlloc(alloc, raw_value) };
    }
    if (value.object.count() != 1) return error.InvalidArgument;
    var it = value.object.iterator();
    const entry = it.next() orelse return error.InvalidArgument;
    return .{ .field = entry.key_ptr.*, .value = try jsonScalarTermAlloc(alloc, entry.value_ptr.*) };
}

fn extractPatternFieldTerms(alloc: Allocator, value: std.json.Value) !PatternFieldTerms {
    if (value != .object) return error.InvalidArgument;
    if (patternFieldOrPathValue(value.object)) |field_value| {
        if (field_value != .string) return error.InvalidArgument;
        const raw_values = value.object.get("values") orelse value.object.get("terms") orelse return error.InvalidArgument;
        return .{ .field = field_value.string, .terms = try compilePatternTerms(alloc, raw_values) };
    }
    if (value.object.count() != 1) return error.InvalidArgument;
    var it = value.object.iterator();
    const entry = it.next() orelse return error.InvalidArgument;
    return .{ .field = entry.key_ptr.*, .terms = try compilePatternTerms(alloc, entry.value_ptr.*) };
}

fn extractPatternTermsField(value: std.json.Value) ![]const u8 {
    if (value != .object) return error.InvalidArgument;
    if (patternFieldOrPathValue(value.object)) |field_value| {
        if (field_value != .string) return error.InvalidArgument;
        _ = value.object.get("values") orelse value.object.get("terms") orelse return error.InvalidArgument;
        return field_value.string;
    }
    if (value.object.count() != 1) return error.InvalidArgument;
    var it = value.object.iterator();
    const entry = it.next() orelse return error.InvalidArgument;
    if (entry.value_ptr.* != .array) return error.InvalidArgument;
    return entry.key_ptr.*;
}

fn extractPatternFuzzyField(value: std.json.Value) ![]const u8 {
    if (value != .object) return error.InvalidArgument;
    if (patternFieldOrPathValue(value.object)) |field_value| {
        if (field_value != .string) return error.InvalidArgument;
        _ = value.object.get("query") orelse value.object.get("value") orelse return error.InvalidArgument;
        return field_value.string;
    }
    if (value.object.count() != 1) return error.InvalidArgument;
    var it = value.object.iterator();
    const entry = it.next() orelse return error.InvalidArgument;
    return entry.key_ptr.*;
}

fn extractPatternFuzzyPredicate(value: std.json.Value) !std.json.Value {
    if (value != .object) return error.InvalidArgument;
    if (patternFieldOrPathValue(value.object) != null) {
        _ = value.object.get("query") orelse value.object.get("value") orelse return error.InvalidArgument;
        return value;
    }
    if (value.object.count() != 1) return error.InvalidArgument;
    var it = value.object.iterator();
    const entry = it.next() orelse return error.InvalidArgument;
    return entry.value_ptr.*;
}

fn compilePatternTerms(alloc: Allocator, value: std.json.Value) ![]const []const u8 {
    if (value != .array or value.array.items.len == 0) return error.InvalidArgument;
    const out = try alloc.alloc([]const u8, value.array.items.len);
    for (value.array.items, 0..) |item, i| {
        out[i] = try jsonScalarTermAlloc(alloc, item);
    }
    return out;
}

fn jsonScalarTermAlloc(alloc: Allocator, value: std.json.Value) ![]const u8 {
    return switch (value) {
        .string => |text| try alloc.dupe(u8, text),
        .integer => |number| try std.fmt.allocPrint(alloc, "{}", .{number}),
        .float => |number| try std.fmt.allocPrint(alloc, "{d}", .{number}),
        .number_string => |text| try alloc.dupe(u8, text),
        .bool => |boolean| try alloc.dupe(u8, if (boolean) "true" else "false"),
        .null => try alloc.dupe(u8, "null"),
        else => error.InvalidArgument,
    };
}

fn extractPatternExistsField(value: std.json.Value) ![]const u8 {
    return switch (value) {
        .string => |field| field,
        .object => |object| blk: {
            const field = patternFieldOrPathValue(object) orelse return error.InvalidArgument;
            if (field != .string) return error.InvalidArgument;
            break :blk field.string;
        },
        else => error.InvalidArgument,
    };
}

fn extractStandardRangeField(range_query: std.json.Value) ![]const u8 {
    if (range_query != .object) return error.InvalidArgument;
    if (range_query.object.get("field")) |field| {
        if (field != .string) return error.InvalidArgument;
        return field.string;
    }
    if (range_query.object.count() != 1) return error.InvalidArgument;
    var it = range_query.object.iterator();
    const entry = it.next() orelse return error.InvalidArgument;
    if (entry.value_ptr.* != .object) return error.InvalidArgument;
    return entry.key_ptr.*;
}

fn extractStandardRangePredicate(range_query: std.json.Value) !std.json.Value {
    if (range_query != .object) return error.InvalidArgument;
    if (range_query.object.get("field") != null) return range_query;
    if (range_query.object.count() != 1) return error.InvalidArgument;
    var it = range_query.object.iterator();
    const entry = it.next() orelse return error.InvalidArgument;
    if (entry.value_ptr.* != .object) return error.InvalidArgument;
    return entry.value_ptr.*;
}

fn compilePatternFieldPredicate(alloc: Allocator, filter_query: std.json.Value) !CompiledPatternFilter.FieldPredicate {
    if (filter_query.object.get("term")) |term| {
        return .{ .term = (try extractPatternFieldString(alloc, term, "term")).value };
    }
    if (filter_query.object.get("terms")) |terms| {
        return .{ .terms = (try extractPatternFieldTerms(alloc, terms)).terms };
    }
    if (filter_query.object.get("match")) |match| {
        return .{ .match = (try extractPatternFieldString(alloc, match, "text")).value };
    }
    if (filter_query.object.get("prefix")) |prefix| {
        return .{ .prefix = (try extractPatternFieldString(alloc, prefix, "prefix")).value };
    }
    if (filter_query.object.get("wildcard")) |wildcard| {
        return .{ .wildcard = (try extractPatternFieldString(alloc, wildcard, "pattern")).value };
    }
    if (filter_query.object.get("regexp")) |regexp| {
        return .{ .regexp = try regex_mod.compile(alloc, (try extractPatternFieldString(alloc, regexp, "pattern")).value) };
    }
    if (filter_query.object.get("fuzzy")) |fuzzy| {
        return .{ .fuzzy = try compileFuzzyPredicate(alloc, try extractPatternFuzzyPredicate(fuzzy)) };
    }
    if (filter_query.object.get("numeric_range")) |range_query| return .{ .numeric_range = range_query };
    if (filter_query.object.get("range")) |range_query| return .{ .standard_range = try extractStandardRangePredicate(range_query) };
    if (filter_query.object.get("date_range")) |range_query| return .{ .date_range = range_query };
    if (filter_query.object.get("bool_field")) |bool_query| return .{ .bool_field = bool_query };
    if (filter_query.object.get("term_range")) |range_query| return .{ .term_range = range_query };
    if (filter_query.object.get("ip_range")) |range_query| return .{ .ip_range = range_query };
    if (filter_query.object.get("geo_distance")) |geo_query| return .{ .geo_distance = geo_query };
    if (filter_query.object.get("geo_bbox")) |geo_query| return .{ .geo_bbox = geo_query };
    if (filter_query.object.get("geo_shape")) |geo_query| return .{ .geo_shape = geo_query };
    if (filter_query.object.get("exists") != null) return .exists;
    return error.InvalidArgument;
}

fn collectJsonValuesAtSingleSegment(
    alloc: Allocator,
    value: std.json.Value,
    segment: []const u8,
    out: *std.ArrayListUnmanaged(std.json.Value),
) !void {
    switch (value) {
        .object => |object| {
            const next = object.get(segment) orelse return;
            try out.append(alloc, next);
        },
        .array => |array| {
            if (std.fmt.parseInt(usize, segment, 10)) |index| {
                if (index >= array.items.len) return;
                try out.append(alloc, array.items[index]);
            } else |_| {
                for (array.items) |item| {
                    try collectJsonValuesAtSingleSegment(alloc, item, segment, out);
                }
            }
        },
        else => {},
    }
}

fn docIdMatchesPatternKey(key: []const u8, doc_id: std.json.Value) !bool {
    const ids = switch (doc_id) {
        .object => (doc_id.object.get("ids") orelse return error.InvalidArgument),
        .array => doc_id,
        else => return error.InvalidArgument,
    };
    if (ids != .array or ids.array.items.len == 0) return error.InvalidArgument;
    for (ids.array.items) |item| {
        if (item != .string) return error.InvalidArgument;
        if (std.mem.eql(u8, key, item.string)) return true;
    }
    return false;
}

fn collectJsonValuesAtPath(
    alloc: Allocator,
    value: std.json.Value,
    path: []const []const u8,
    depth: usize,
    out: *std.ArrayListUnmanaged(std.json.Value),
) !void {
    if (depth >= path.len) {
        try out.append(alloc, value);
        return;
    }
    const segment = path[depth];
    switch (value) {
        .object => |object| {
            const next = object.get(segment) orelse return;
            try collectJsonValuesAtPath(alloc, next, path, depth + 1, out);
        },
        .array => |array| {
            if (std.fmt.parseInt(usize, segment, 10)) |index| {
                if (index >= array.items.len) return;
                try collectJsonValuesAtPath(alloc, array.items[index], path, depth + 1, out);
            } else |_| {
                for (array.items) |item| {
                    try collectJsonValuesAtPath(alloc, item, path, depth, out);
                }
            }
        },
        else => {},
    }
}

fn jsonValuesContainTerm(values: []const std.json.Value, term: []const u8) bool {
    for (values) |value| {
        switch (value) {
            .string => |text| if (std.mem.eql(u8, text, term)) return true,
            .number_string => |text| if (std.mem.eql(u8, text, term)) return true,
            .integer => |number| {
                var buf: [32]u8 = undefined;
                const rendered = std.fmt.bufPrint(&buf, "{}", .{number}) catch continue;
                if (std.mem.eql(u8, rendered, term)) return true;
            },
            .float => |number| {
                var buf: [64]u8 = undefined;
                const rendered = std.fmt.bufPrint(&buf, "{d}", .{number}) catch continue;
                if (std.mem.eql(u8, rendered, term)) return true;
            },
            .bool => |boolean| {
                if ((boolean and std.mem.eql(u8, term, "true")) or (!boolean and std.mem.eql(u8, term, "false"))) return true;
            },
            .null => if (std.mem.eql(u8, term, "null")) return true,
            else => {},
        }
    }
    return false;
}

fn jsonValuesContainAnyTerm(values: []const std.json.Value, terms: []const []const u8) bool {
    for (terms) |term| {
        if (jsonValuesContainTerm(values, term)) return true;
    }
    return false;
}

fn jsonValuesContainMatch(values: []const std.json.Value, text: []const u8) bool {
    for (values) |value| {
        switch (value) {
            .string => |candidate| {
                if (containsCaseInsensitive(candidate, text)) return true;
            },
            else => {},
        }
    }
    return false;
}

fn jsonValuesContainPrefix(values: []const std.json.Value, prefix: []const u8) bool {
    for (values) |value| {
        switch (value) {
            .string => |candidate| {
                if (std.mem.startsWith(u8, candidate, prefix)) return true;
            },
            else => {},
        }
    }
    return false;
}

fn jsonValuesContainWildcard(values: []const std.json.Value, pattern: []const u8) bool {
    for (values) |value| {
        switch (value) {
            .string => |candidate| {
                if (wildcardMatch(pattern, candidate)) return true;
            },
            else => {},
        }
    }
    return false;
}

fn jsonValuesContainRegexp(alloc: Allocator, values: []const std.json.Value, pattern: []const u8) !bool {
    for (values) |value| {
        switch (value) {
            .string => |candidate| {
                if (try regexMatches(alloc, pattern, candidate)) return true;
            },
            else => {},
        }
    }
    return false;
}

fn jsonValuesContainCompiledRegexp(values: []const std.json.Value, compiled: *regex_mod.RegexAutomaton) bool {
    for (values) |value| {
        switch (value) {
            .string => |candidate| {
                if (regex_mod.matchesCompiled("", compiled, candidate)) return true;
            },
            else => {},
        }
    }
    return false;
}

fn jsonValuesContainFuzzy(alloc: Allocator, values: []const std.json.Value, fuzzy_query: CompiledPatternFilter.CompiledFuzzy) !bool {
    for (values) |value| {
        switch (value) {
            .string => |candidate| {
                if (!fuzzyPrefixMatches(fuzzy_query.term, candidate, fuzzy_query.prefix_len)) continue;
                if (try fuzzyMatchString(alloc, candidate, fuzzy_query.folded_term, fuzzy_query.max_edits)) return true;
            },
            else => {},
        }
    }
    return false;
}

fn compileFuzzyPredicate(alloc: Allocator, fuzzy_query: std.json.Value) !CompiledPatternFilter.CompiledFuzzy {
    var term: []const u8 = undefined;
    var max_edits: u8 = 1;
    var prefix_len: u8 = 0;
    var auto_fuzzy = false;
    switch (fuzzy_query) {
        .string => |text| term = text,
        .object => |object| {
            const query_value = object.get("query") orelse object.get("value") orelse return error.InvalidArgument;
            if (query_value != .string) return error.InvalidArgument;
            term = query_value.string;
            if (object.get("max_edits")) |edits| {
                max_edits = switch (edits) {
                    .integer => |number| std.math.cast(u8, number) orelse return error.InvalidArgument,
                    .float => |number| blk: {
                        if (!std.math.isFinite(number) or @round(number) != number) return error.InvalidArgument;
                        const parsed: i64 = @intFromFloat(number);
                        break :blk std.math.cast(u8, parsed) orelse return error.InvalidArgument;
                    },
                    else => return error.InvalidArgument,
                };
            }
            if (object.get("prefix_length")) |prefix| {
                prefix_len = switch (prefix) {
                    .integer => |number| std.math.cast(u8, number) orelse return error.InvalidArgument,
                    .float => |number| blk: {
                        if (!std.math.isFinite(number) or @round(number) != number) return error.InvalidArgument;
                        const parsed: i64 = @intFromFloat(number);
                        break :blk std.math.cast(u8, parsed) orelse return error.InvalidArgument;
                    },
                    else => return error.InvalidArgument,
                };
            }
            if (object.get("auto_fuzzy")) |auto| {
                if (auto != .bool) return error.InvalidArgument;
                auto_fuzzy = auto.bool;
            }
        },
        else => return error.InvalidArgument,
    }

    if (auto_fuzzy) {
        max_edits = if (term.len > 5) 2 else if (term.len > 2) 1 else 0;
    }
    return .{
        .term = term,
        .folded_term = try asciiLowerDup(alloc, term),
        .max_edits = max_edits,
        .prefix_len = prefix_len,
    };
}

fn jsonValuesContainNumericRange(values: []const std.json.Value, range_query: std.json.Value) !bool {
    if (range_query != .object) return error.InvalidArgument;
    const min_value = if (range_query.object.get("min")) |value|
        try jsonNumberFromValue(value)
    else
        null;
    const max_value = if (range_query.object.get("max")) |value|
        try jsonNumberFromValue(value)
    else
        null;
    if (min_value == null and max_value == null) return error.InvalidArgument;
    const inclusive_min = if (range_query.object.get("inclusive_min")) |value| blk: {
        if (value != .bool) return error.InvalidArgument;
        break :blk value.bool;
    } else true;
    const inclusive_max = if (range_query.object.get("inclusive_max")) |value| blk: {
        if (value != .bool) return error.InvalidArgument;
        break :blk value.bool;
    } else false;

    for (values) |value| {
        const candidate = jsonNumberFromValue(value) catch null orelse continue;
        if (min_value) |min| {
            if (candidate < min or (!inclusive_min and candidate == min)) continue;
        }
        if (max_value) |max| {
            if (candidate > max or (!inclusive_max and candidate == max)) continue;
        }
        return true;
    }
    return false;
}

const PatternJsonRangeBound = struct {
    value: std.json.Value,
    inclusive: bool,
};

fn jsonValuesContainStandardRange(values: []const std.json.Value, range_query: std.json.Value) !bool {
    if (range_query != .object) return error.InvalidArgument;
    const lower = try standardPatternRangeLowerBound(range_query);
    const upper = try standardPatternRangeUpperBound(range_query);
    if (lower == null and upper == null) return error.InvalidArgument;

    for (values) |value| {
        if (try jsonValueMatchesStandardRange(value, lower, upper)) return true;
    }
    return false;
}

fn jsonValueMatchesStandardRange(value: std.json.Value, lower: ?PatternJsonRangeBound, upper: ?PatternJsonRangeBound) !bool {
    if (value == .integer or value == .float) {
        const candidate = try jsonNumberFromValue(value);
        const min_value = if (lower) |bound| try jsonNumberFromValue(bound.value) else null;
        const max_value = if (upper) |bound| try jsonNumberFromValue(bound.value) else null;
        if (min_value) |min| {
            if (candidate < min or (!(lower.?.inclusive) and candidate == min)) return false;
        }
        if (max_value) |max| {
            if (candidate > max or (!(upper.?.inclusive) and candidate == max)) return false;
        }
        return true;
    }

    if (jsonDateNsFromValue(value)) |candidate| {
        const min_value = if (lower) |bound| jsonDateNsFromValue(bound.value) catch null else null;
        const max_value = if (upper) |bound| jsonDateNsFromValue(bound.value) catch null else null;
        if (min_value != null or max_value != null) {
            if (min_value) |min| {
                if (candidate < min or (!(lower.?.inclusive) and candidate == min)) return false;
            }
            if (max_value) |max| {
                if (candidate > max or (!(upper.?.inclusive) and candidate == max)) return false;
            }
            return true;
        }
    } else |_| {}

    var candidate_buf: [64]u8 = undefined;
    const candidate = jsonScalarTermSlice(value, &candidate_buf) catch return false;
    if (lower) |bound| {
        var min_buf: [64]u8 = undefined;
        const min = try jsonScalarTermSlice(bound.value, &min_buf);
        const order = std.mem.order(u8, candidate, min);
        if (order == .lt or (!bound.inclusive and order == .eq)) return false;
    }
    if (upper) |bound| {
        var max_buf: [64]u8 = undefined;
        const max = try jsonScalarTermSlice(bound.value, &max_buf);
        const order = std.mem.order(u8, candidate, max);
        if (order == .gt or (!bound.inclusive and order == .eq)) return false;
    }
    return true;
}

fn standardPatternRangeLowerBound(range_query: std.json.Value) !?PatternJsonRangeBound {
    var found: ?PatternJsonRangeBound = null;
    if (range_query.object.get("gte")) |value| try setPatternJsonRangeBound(&found, value, true);
    if (range_query.object.get("gt")) |value| try setPatternJsonRangeBound(&found, value, false);
    if (range_query.object.get("from")) |value| try setPatternJsonRangeBound(&found, value, jsonPatternBoolOrDefault(range_query.object.get("include_lower"), true));
    if (range_query.object.get("min")) |value| try setPatternJsonRangeBound(&found, value, jsonPatternBoolOrDefault(range_query.object.get("inclusive_min"), true));
    return found;
}

fn standardPatternRangeUpperBound(range_query: std.json.Value) !?PatternJsonRangeBound {
    var found: ?PatternJsonRangeBound = null;
    if (range_query.object.get("lte")) |value| try setPatternJsonRangeBound(&found, value, true);
    if (range_query.object.get("lt")) |value| try setPatternJsonRangeBound(&found, value, false);
    if (range_query.object.get("to")) |value| try setPatternJsonRangeBound(&found, value, jsonPatternBoolOrDefault(range_query.object.get("include_upper"), true));
    if (range_query.object.get("max")) |value| try setPatternJsonRangeBound(&found, value, jsonPatternBoolOrDefault(range_query.object.get("inclusive_max"), false));
    return found;
}

fn setPatternJsonRangeBound(found: *?PatternJsonRangeBound, value: std.json.Value, inclusive: bool) !void {
    if (found.* != null) return error.InvalidArgument;
    found.* = .{ .value = value, .inclusive = inclusive };
}

fn jsonPatternBoolOrDefault(value: ?std.json.Value, default_value: bool) bool {
    const actual = value orelse return default_value;
    return if (actual == .bool) actual.bool else default_value;
}

fn jsonValuesContainDateRange(values: []const std.json.Value, range_query: std.json.Value) !bool {
    if (range_query != .object) return error.InvalidArgument;
    const start_ns = if (range_query.object.get("start_ns")) |value|
        try jsonI64FromValue(value)
    else
        null;
    const end_ns = if (range_query.object.get("end_ns")) |value|
        try jsonI64FromValue(value)
    else
        null;
    if (start_ns == null and end_ns == null) return error.InvalidArgument;
    const inclusive_start = if (range_query.object.get("inclusive_start")) |value| blk: {
        if (value != .bool) return error.InvalidArgument;
        break :blk value.bool;
    } else true;
    const inclusive_end = if (range_query.object.get("inclusive_end")) |value| blk: {
        if (value != .bool) return error.InvalidArgument;
        break :blk value.bool;
    } else false;

    for (values) |value| {
        const candidate = jsonDateNsFromValue(value) catch null orelse continue;
        if (start_ns) |start| {
            if (candidate < start or (!inclusive_start and candidate == start)) continue;
        }
        if (end_ns) |end| {
            if (candidate > end or (!inclusive_end and candidate == end)) continue;
        }
        return true;
    }
    return false;
}

fn jsonValuesContainBoolField(values: []const std.json.Value, bool_query: std.json.Value) !bool {
    if (bool_query != .object) return error.InvalidArgument;
    const expected = bool_query.object.get("value") orelse return error.InvalidArgument;
    if (expected != .bool) return error.InvalidArgument;
    for (values) |value| {
        if (value == .bool and value.bool == expected.bool) return true;
    }
    return false;
}

fn jsonValuesContainTermRange(values: []const std.json.Value, range_query: std.json.Value) !bool {
    if (range_query != .object) return error.InvalidArgument;
    const min_value = range_query.object.get("min");
    const max_value = range_query.object.get("max");
    if (min_value == null and max_value == null) return error.InvalidArgument;
    const inclusive_min = if (range_query.object.get("inclusive_min")) |value| blk: {
        if (value != .bool) return error.InvalidArgument;
        break :blk value.bool;
    } else true;
    const inclusive_max = if (range_query.object.get("inclusive_max")) |value| blk: {
        if (value != .bool) return error.InvalidArgument;
        break :blk value.bool;
    } else false;

    for (values) |value| {
        var candidate_buf: [64]u8 = undefined;
        const candidate = jsonScalarTermSlice(value, &candidate_buf) catch continue;
        if (min_value) |min_raw| {
            var min_buf: [64]u8 = undefined;
            const min = try jsonScalarTermSlice(min_raw, &min_buf);
            const order = std.mem.order(u8, candidate, min);
            if (order == .lt or (!inclusive_min and order == .eq)) continue;
        }
        if (max_value) |max_raw| {
            var max_buf: [64]u8 = undefined;
            const max = try jsonScalarTermSlice(max_raw, &max_buf);
            const order = std.mem.order(u8, candidate, max);
            if (order == .gt or (!inclusive_max and order == .eq)) continue;
        }
        return true;
    }
    return false;
}

fn jsonValuesContainIpRange(values: []const std.json.Value, ip_range: std.json.Value) !bool {
    if (ip_range != .object) return error.InvalidArgument;
    const cidr_value = ip_range.object.get("cidr") orelse return error.InvalidArgument;
    if (cidr_value != .string) return error.InvalidArgument;
    const parsed = parsePatternCIDR(cidr_value.string);
    const exact_ip = if (parsed == null) parsePatternIPv4(cidr_value.string) else null;
    if (parsed == null and exact_ip == null) return error.InvalidArgument;

    for (values) |value| {
        if (value != .string) continue;
        const candidate = parsePatternIPv4(value.string) orelse continue;
        const matched = if (parsed) |cidr|
            ipInPatternRange(candidate, cidr.network, cidr.prefix_len)
        else if (exact_ip) |wanted|
            std.mem.eql(u8, wanted[0..], candidate[0..])
        else
            false;
        if (matched) return true;
    }
    return false;
}

fn jsonValuesContainGeoDistance(values: []const std.json.Value, geo_query: std.json.Value) !bool {
    if (geo_query != .object) return error.InvalidArgument;
    const lon = switch (geo_query.object.get("lon") orelse return error.InvalidArgument) {
        .integer => |value| @as(f64, @floatFromInt(value)),
        .float => |value| value,
        else => return error.InvalidArgument,
    };
    const lat = switch (geo_query.object.get("lat") orelse return error.InvalidArgument) {
        .integer => |value| @as(f64, @floatFromInt(value)),
        .float => |value| value,
        else => return error.InvalidArgument,
    };
    const radius_meters = switch (geo_query.object.get("radius_meters") orelse return error.InvalidArgument) {
        .integer => |value| @as(f64, @floatFromInt(value)),
        .float => |value| value,
        else => return error.InvalidArgument,
    };
    const center = geo_mod.GeoPoint{ .lat = lat, .lon = lon };
    for (values) |value| {
        const point = jsonGeoPointFromValue(value) catch continue;
        if (geo_mod.haversineDistance(center, point) <= radius_meters) return true;
    }
    return false;
}

fn jsonValuesContainGeoBBox(values: []const std.json.Value, geo_query: std.json.Value) !bool {
    if (geo_query != .object) return error.InvalidArgument;
    const min_lat = switch (geo_query.object.get("min_lat") orelse return error.InvalidArgument) {
        .integer => |value| @as(f64, @floatFromInt(value)),
        .float => |value| value,
        else => return error.InvalidArgument,
    };
    const min_lon = switch (geo_query.object.get("min_lon") orelse return error.InvalidArgument) {
        .integer => |value| @as(f64, @floatFromInt(value)),
        .float => |value| value,
        else => return error.InvalidArgument,
    };
    const max_lat = switch (geo_query.object.get("max_lat") orelse return error.InvalidArgument) {
        .integer => |value| @as(f64, @floatFromInt(value)),
        .float => |value| value,
        else => return error.InvalidArgument,
    };
    const max_lon = switch (geo_query.object.get("max_lon") orelse return error.InvalidArgument) {
        .integer => |value| @as(f64, @floatFromInt(value)),
        .float => |value| value,
        else => return error.InvalidArgument,
    };
    for (values) |value| {
        const point = jsonGeoPointFromValue(value) catch continue;
        if (point.lat >= min_lat and point.lat <= max_lat and
            point.lon >= min_lon and point.lon <= max_lon)
        {
            return true;
        }
    }
    return false;
}

fn jsonValuesContainGeoShape(alloc: Allocator, values: []const std.json.Value, geo_query: std.json.Value) !bool {
    if (geo_query != .object) return error.InvalidArgument;
    const relation = if (geo_query.object.get("relation")) |value|
        try parsePatternGeoShapeRelation(value)
    else
        types.GeoShapeRelation.intersects;
    const polygons = try parsePatternGeoShapePolygons(alloc, geo_query);
    defer freePatternGeoPolygons(alloc, polygons);

    for (values) |value| {
        const point = jsonGeoPointFromValue(value) catch continue;
        for (polygons) |polygon| {
            const inside = geo_mod.pointInPolygon(point, polygon);
            const matched = switch (relation) {
                .intersects, .within => inside,
                .contains => false,
            };
            if (matched) return true;
        }
    }
    return false;
}

fn jsonNumberFromValue(value: std.json.Value) !f64 {
    return switch (value) {
        .integer => |number| @floatFromInt(number),
        .float => |number| number,
        else => error.InvalidArgument,
    };
}

fn jsonI64FromValue(value: std.json.Value) !i64 {
    return switch (value) {
        .integer => |number| std.math.cast(i64, number) orelse error.InvalidArgument,
        .float => |number| blk: {
            if (!std.math.isFinite(number) or @round(number) != number) return error.InvalidArgument;
            break :blk std.math.cast(i64, @as(i128, @intFromFloat(number))) orelse error.InvalidArgument;
        },
        else => error.InvalidArgument,
    };
}

fn jsonDateNsFromValue(value: std.json.Value) !i64 {
    return switch (value) {
        .string => |text| blk: {
            const ts = (try parsePatternRfc3339ToNs(text)) orelse return error.InvalidArgument;
            break :blk @as(i64, @intCast(ts));
        },
        .integer, .float => try jsonI64FromValue(value),
        else => error.InvalidArgument,
    };
}

fn jsonScalarTermSlice(value: std.json.Value, buf: []u8) ![]const u8 {
    return switch (value) {
        .string => |text| text,
        .integer => |number| try std.fmt.bufPrint(buf, "{}", .{number}),
        .float => |number| try std.fmt.bufPrint(buf, "{d}", .{number}),
        .number_string => |text| text,
        .bool => |boolean| if (boolean) "true" else "false",
        .null => "null",
        else => error.InvalidArgument,
    };
}

const PatternCIDRParsed = struct { network: [4]u8, prefix_len: u8 };

fn parsePatternCIDR(cidr: []const u8) ?PatternCIDRParsed {
    const slash_pos = std.mem.indexOfScalar(u8, cidr, '/') orelse return null;
    const ip = parsePatternIPv4(cidr[0..slash_pos]) orelse return null;
    const prefix_len = std.fmt.parseInt(u8, cidr[slash_pos + 1 ..], 10) catch return null;
    if (prefix_len > 32) return null;
    const mask = patternIpMask(prefix_len);
    return .{
        .network = .{ ip[0] & mask[0], ip[1] & mask[1], ip[2] & mask[2], ip[3] & mask[3] },
        .prefix_len = prefix_len,
    };
}

fn parsePatternIPv4(s: []const u8) ?[4]u8 {
    var octets: [4]u8 = undefined;
    var it = std.mem.splitScalar(u8, s, '.');
    for (&octets) |*o| {
        const part = it.next() orelse return null;
        o.* = std.fmt.parseInt(u8, part, 10) catch return null;
    }
    if (it.next() != null) return null;
    return octets;
}

fn patternIpMask(prefix_len: u8) [4]u8 {
    if (prefix_len == 0) return .{ 0, 0, 0, 0 };
    if (prefix_len >= 32) return .{ 0xff, 0xff, 0xff, 0xff };
    const shift: u5 = @intCast(32 - prefix_len);
    const mask: u32 = ~@as(u32, 0) << shift;
    return .{
        @intCast((mask >> 24) & 0xff),
        @intCast((mask >> 16) & 0xff),
        @intCast((mask >> 8) & 0xff),
        @intCast(mask & 0xff),
    };
}

fn ipInPatternRange(ip: [4]u8, network: [4]u8, prefix_len: u8) bool {
    const mask = patternIpMask(prefix_len);
    return (ip[0] & mask[0]) == network[0] and
        (ip[1] & mask[1]) == network[1] and
        (ip[2] & mask[2]) == network[2] and
        (ip[3] & mask[3]) == network[3];
}

fn jsonGeoPointFromValue(value: std.json.Value) !geo_mod.GeoPoint {
    if (value != .object) return error.InvalidArgument;
    return .{
        .lon = switch (value.object.get("lon") orelse return error.InvalidArgument) {
            .integer => |v| @floatFromInt(v),
            .float => |v| v,
            else => return error.InvalidArgument,
        },
        .lat = switch (value.object.get("lat") orelse return error.InvalidArgument) {
            .integer => |v| @floatFromInt(v),
            .float => |v| v,
            else => return error.InvalidArgument,
        },
    };
}

fn parsePatternGeoShapePolygons(alloc: Allocator, value: std.json.Value) ![]const []const geo_mod.GeoPoint {
    if (value.object.get("polygons")) |polygons_value| {
        if (polygons_value != .array or polygons_value.array.items.len == 0) return error.InvalidArgument;
        var polygons = try alloc.alloc([]const geo_mod.GeoPoint, polygons_value.array.items.len);
        var initialized: usize = 0;
        errdefer {
            for (polygons[0..initialized]) |polygon| alloc.free(polygon);
            alloc.free(polygons);
        }
        for (polygons_value.array.items, 0..) |item, i| {
            polygons[i] = try parsePatternGeoPointArray(alloc, item);
            initialized += 1;
        }
        return polygons;
    }
    if (value.object.get("polygon")) |polygon_value| {
        var polygons = try alloc.alloc([]const geo_mod.GeoPoint, 1);
        errdefer alloc.free(polygons);
        polygons[0] = try parsePatternGeoPointArray(alloc, polygon_value);
        return polygons;
    }
    return error.InvalidArgument;
}

fn parsePatternGeoShapeRelation(value: std.json.Value) !types.GeoShapeRelation {
    if (value != .string) return error.InvalidArgument;
    if (std.mem.eql(u8, value.string, "intersects")) return .intersects;
    if (std.mem.eql(u8, value.string, "within")) return .within;
    if (std.mem.eql(u8, value.string, "contains")) return .contains;
    return error.InvalidArgument;
}

fn parsePatternGeoPointArray(alloc: Allocator, value: std.json.Value) ![]const geo_mod.GeoPoint {
    if (value != .array or value.array.items.len < 3) return error.InvalidArgument;
    var points = try alloc.alloc(geo_mod.GeoPoint, value.array.items.len);
    errdefer alloc.free(points);
    for (value.array.items, 0..) |item, i| {
        points[i] = try jsonGeoPointFromValue(item);
    }
    if (std.meta.eql(points[0], points[points.len - 1])) return points;
    var closed = try alloc.alloc(geo_mod.GeoPoint, points.len + 1);
    @memcpy(closed[0..points.len], points);
    closed[points.len] = points[0];
    alloc.free(points);
    return closed;
}

fn freePatternGeoPolygons(alloc: Allocator, polygons: []const []const geo_mod.GeoPoint) void {
    for (polygons) |polygon| alloc.free(polygon);
    alloc.free(polygons);
}

pub fn parsePatternRfc3339ToNs(text: []const u8) !?u64 {
    if (text.len < 20) return null;
    if (text[4] != '-' or text[7] != '-' or text[10] != 'T' or text[13] != ':' or text[16] != ':') return null;

    const year = std.fmt.parseInt(i64, text[0..4], 10) catch return null;
    const month = std.fmt.parseInt(i64, text[5..7], 10) catch return null;
    const day = std.fmt.parseInt(i64, text[8..10], 10) catch return null;
    const hour = std.fmt.parseInt(i64, text[11..13], 10) catch return null;
    const minute = std.fmt.parseInt(i64, text[14..16], 10) catch return null;
    const second = std.fmt.parseInt(i64, text[17..19], 10) catch return null;

    var idx: usize = 19;
    var nanos: u64 = 0;
    if (idx < text.len and text[idx] == '.') {
        idx += 1;
        const frac_start = idx;
        while (idx < text.len and text[idx] >= '0' and text[idx] <= '9') : (idx += 1) {}
        const frac = text[frac_start..idx];
        if (frac.len == 0 or frac.len > 9) return null;
        var frac_ns = std.fmt.parseInt(u64, frac, 10) catch return null;
        var scale: usize = frac.len;
        while (scale < 9) : (scale += 1) frac_ns *= 10;
        nanos = frac_ns;
    }
    if (idx >= text.len or text[idx] != 'Z' or idx + 1 != text.len) return null;

    const days = daysFromCivil(year, month, day);
    if (days < 0) return null;
    const secs = days * 86_400 + hour * 3_600 + minute * 60 + second;
    if (secs < 0) return null;
    return @as(u64, @intCast(secs)) * std.time.ns_per_s + nanos;
}

fn daysFromCivil(year: i64, month: i64, day: i64) i64 {
    var y = year;
    y -= if (month <= 2) @as(i64, 1) else @as(i64, 0);
    const era = @divFloor(if (y >= 0) y else y - 399, 400);
    const yoe = y - era * 400;
    const mp = month + (if (month > 2) @as(i64, -3) else @as(i64, 9));
    const doy = @divFloor(153 * mp + 2, 5) + day - 1;
    const doe = yoe * 365 + @divFloor(yoe, 4) - @divFloor(yoe, 100) + doy;
    return era * 146_097 + doe - 719_468;
}

fn wildcardMatch(pattern: []const u8, text: []const u8) bool {
    var pi: usize = 0;
    var ti: usize = 0;
    var star_pi: ?usize = null;
    var star_ti: usize = 0;

    while (ti < text.len) {
        if (pi < pattern.len and (pattern[pi] == '?' or pattern[pi] == text[ti])) {
            pi += 1;
            ti += 1;
        } else if (pi < pattern.len and pattern[pi] == '*') {
            star_pi = pi;
            star_ti = ti;
            pi += 1;
        } else if (star_pi) |sp| {
            pi = sp + 1;
            star_ti += 1;
            ti = star_ti;
        } else {
            return false;
        }
    }

    while (pi < pattern.len and pattern[pi] == '*') pi += 1;
    return pi == pattern.len;
}

fn regexMatches(alloc: Allocator, pattern: []const u8, candidate: []const u8) !bool {
    var regex = try regex_mod.compile(alloc, pattern);
    defer regex.deinit();
    return regex_mod.matchesCompiled(pattern, &regex, candidate);
}

fn fuzzyMatchString(alloc: Allocator, candidate: []const u8, folded_term: []const u8, max_edits: u8) !bool {
    if (candidate.len == 0 or folded_term.len == 0) return std.mem.eql(u8, candidate, folded_term);

    const folded_candidate = try asciiLowerDup(alloc, candidate);
    defer alloc.free(folded_candidate);

    var automaton = levenshtein_mod.LevenshteinAutomaton{
        .term = folded_term,
        .max_distance = max_edits,
        .alloc = alloc,
    };
    defer automaton.deinit();

    var state = automaton.automaton().start();
    for (folded_candidate) |b| {
        state = automaton.automaton().accept(state, b);
        if (!automaton.automaton().canMatch(state)) return false;
    }
    return automaton.automaton().isMatch(state);
}

fn fuzzyPrefixMatches(term: []const u8, candidate: []const u8, prefix_len: u8) bool {
    if (prefix_len == 0) return true;
    const prefix: usize = @intCast(prefix_len);
    if (term.len < prefix or candidate.len < prefix) return false;
    return std.mem.eql(u8, term[0..prefix], candidate[0..prefix]);
}

fn asciiLowerDup(alloc: Allocator, input: []const u8) ![]u8 {
    const out = try alloc.dupe(u8, input);
    for (out) |*b| {
        if (b.* >= 'A' and b.* <= 'Z') b.* += 'a' - 'A';
    }
    return out;
}

fn containsCaseInsensitive(haystack: []const u8, needle: []const u8) bool {
    if (needle.len == 0) return true;
    if (haystack.len < needle.len) return false;
    var i: usize = 0;
    while (i + needle.len <= haystack.len) : (i += 1) {
        if (asciiEqualIgnoreCase(haystack[i .. i + needle.len], needle)) return true;
    }
    return false;
}

fn asciiEqualIgnoreCase(left: []const u8, right: []const u8) bool {
    if (left.len != right.len) return false;
    for (left, right) |l, r| {
        if (std.ascii.toLower(l) != std.ascii.toLower(r)) return false;
    }
    return true;
}

pub fn resolveGraphSelectorFromSets(
    alloc: Allocator,
    selector: graph_query_mod.NodeSelector,
    named_sets: []const NamedResultSet,
    doc_set_resolver: DocSetDocIdResolver,
) ![][]u8 {
    return switch (selector) {
        .keys => |keys| resolveGraphSelector(alloc, .{ .keys = keys }, null),
        .result_ref => |result_ref| blk: {
            const set = findNamedSetByRef(named_sets, result_ref.ref) orelse return error.GraphResultRefNotImplemented;
            if (result_ref.limit == 0) {
                if (set.resolved_doc_set_complete) if (set.resolved_doc_set) |resolved_doc_set| {
                    const resolve = doc_set_resolver.func orelse return error.UnsupportedQueryRequest;
                    const doc_ids = (try resolve(
                        doc_set_resolver.ctx,
                        alloc,
                        resolved_doc_set,
                        doc_set_resolver.identity_read_generation,
                    )) orelse return error.UnsupportedQueryRequest;
                    break :blk doc_ids;
                };
                if (@as(u64, set.total_hits) > set.hits.len) return error.UnsupportedQueryRequest;
            }
            const count: usize = if (result_ref.limit == 0) set.hits.len else @min(set.hits.len, result_ref.limit);
            var duped = try alloc.alloc([]u8, count);
            var initialized: usize = 0;
            errdefer {
                for (duped[0..initialized]) |key| alloc.free(key);
                alloc.free(duped);
            }
            for (set.hits[0..count], 0..) |hit, i| {
                duped[i] = try alloc.dupe(u8, hit.id);
                initialized += 1;
            }
            break :blk duped;
        },
    };
}

fn findNamedSet(named_sets: []const NamedResultSet, name: []const u8) ?NamedResultSet {
    for (named_sets) |set| {
        if (std.mem.eql(u8, set.name, name)) return set;
    }
    return null;
}

fn findNamedSetByRef(named_sets: []const NamedResultSet, ref: []const u8) ?NamedResultSet {
    if (findNamedSet(named_sets, ref)) |set| return set;
    if (std.mem.eql(u8, ref, "$full_text_results")) return findNamedSet(named_sets, "full_text");
    if (std.mem.eql(u8, ref, "$embeddings_results")) return findNamedSet(named_sets, "$embeddings_results");
    if (std.mem.startsWith(u8, ref, "$full_text_results.")) {
        return findNamedSet(named_sets, ref["$full_text_results.".len..]);
    }
    if (std.mem.startsWith(u8, ref, "$aknn_results.")) {
        return findNamedSet(named_sets, ref["$aknn_results.".len..]);
    }
    if (std.mem.startsWith(u8, ref, "$graph_results.")) {
        return findNamedSet(named_sets, ref["$graph_results.".len..]);
    }
    return null;
}

test "jsonDocMatchesPatternFilter supports stored structured filters" {
    const alloc = std.testing.allocator;

    var parsed_bool = try std.json.parseFromSlice(std.json.Value, alloc,
        \\{"bool_field":{"field":"published","value":true}}
    , .{});
    defer parsed_bool.deinit();

    var parsed_term_range = try std.json.parseFromSlice(std.json.Value, alloc,
        \\{"term_range":{"field":"tag","min":"m","max":"z","inclusive_min":true,"inclusive_max":false}}
    , .{});
    defer parsed_term_range.deinit();

    var parsed_standard_range = try std.json.parseFromSlice(std.json.Value, alloc,
        \\{"range":{"score":{"gte":2.0,"lt":10.0}}}
    , .{});
    defer parsed_standard_range.deinit();

    var parsed_field_value_prefix = try std.json.parseFromSlice(std.json.Value, alloc,
        \\{"prefix":{"field":"tag","role":"measure","value":"man"}}
    , .{});
    defer parsed_field_value_prefix.deinit();

    var parsed_terms = try std.json.parseFromSlice(std.json.Value, alloc,
        \\{"terms":{"field":"tag","values":["missing","mango"]}}
    , .{});
    defer parsed_terms.deinit();

    var parsed_field_value_fuzzy = try std.json.parseFromSlice(std.json.Value, alloc,
        \\{"fuzzy":{"field":"tag","role":"measure","query":"mengo","max_edits":1,"prefix_length":1}}
    , .{});
    defer parsed_field_value_fuzzy.deinit();

    var parsed_ip_range = try std.json.parseFromSlice(std.json.Value, alloc,
        \\{"ip_range":{"field":"ip","cidr":"10.0.0.0/8"}}
    , .{});
    defer parsed_ip_range.deinit();

    var parsed_geo_distance = try std.json.parseFromSlice(std.json.Value, alloc,
        \\{"geo_distance":{"field":"location","lon":-122.4194,"lat":37.7749,"radius_meters":2000}}
    , .{});
    defer parsed_geo_distance.deinit();

    var parsed_geo_bbox = try std.json.parseFromSlice(std.json.Value, alloc,
        \\{"geo_bbox":{"field":"location","min_lat":37.70,"min_lon":-122.50,"max_lat":37.80,"max_lon":-122.40}}
    , .{});
    defer parsed_geo_bbox.deinit();

    var parsed_geo_shape = try std.json.parseFromSlice(std.json.Value, alloc,
        \\{"geo_shape":{"field":"location","polygon":[{"lon":-122.50,"lat":37.70},{"lon":-122.40,"lat":37.70},{"lon":-122.40,"lat":37.80},{"lon":-122.50,"lat":37.80}]}}
    , .{});
    defer parsed_geo_shape.deinit();

    var parsed_geo_shape_contains = try std.json.parseFromSlice(std.json.Value, alloc,
        \\{"geo_shape":{"field":"location","relation":"contains","polygon":[{"lon":-122.50,"lat":37.70},{"lon":-122.40,"lat":37.70},{"lon":-122.40,"lat":37.80},{"lon":-122.50,"lat":37.80}]}}
    , .{});
    defer parsed_geo_shape_contains.deinit();

    var parsed_doc_ids = try std.json.parseFromSlice(std.json.Value, alloc,
        \\{"doc_id":["doc:a","doc:b"]}
    , .{});
    defer parsed_doc_ids.deinit();

    var parsed_path_exists = try std.json.parseFromSlice(std.json.Value, alloc,
        \\{"exists":{"path":"meta.deleted_at"}}
    , .{});
    defer parsed_path_exists.deinit();

    var parsed_path_term_bool = try std.json.parseFromSlice(std.json.Value, alloc,
        \\{"term":{"path":"meta.archived","value":false}}
    , .{});
    defer parsed_path_term_bool.deinit();

    var parsed_terms_with_null = try std.json.parseFromSlice(std.json.Value, alloc,
        \\{"terms":{"path":"meta.optional","values":["missing",null]}}
    , .{});
    defer parsed_terms_with_null.deinit();

    var parsed_geo_doc = try std.json.parseFromSlice(std.json.Value, alloc,
        \\{"published":true,"tag":"mango","score":5.0,"ip":"10.1.2.3","location":{"lon":-122.4194,"lat":37.7749},"meta":{"deleted_at":"2026-01-01T00:00:00Z","archived":false,"optional":null}}
    , .{});
    defer parsed_geo_doc.deinit();

    try std.testing.expect(try jsonDocMatchesPatternFilter(alloc, "doc:b", parsed_geo_doc.value, parsed_bool.value));
    try std.testing.expect(try jsonDocMatchesPatternFilter(alloc, "doc:b", parsed_geo_doc.value, parsed_term_range.value));
    try std.testing.expect(try jsonDocMatchesPatternFilter(alloc, "doc:b", parsed_geo_doc.value, parsed_standard_range.value));
    try std.testing.expect(try jsonDocMatchesPatternFilter(alloc, "doc:b", parsed_geo_doc.value, parsed_field_value_prefix.value));
    try std.testing.expect(try jsonDocMatchesPatternFilter(alloc, "doc:b", parsed_geo_doc.value, parsed_terms.value));
    try std.testing.expect(try jsonDocMatchesPatternFilter(alloc, "doc:b", parsed_geo_doc.value, parsed_field_value_fuzzy.value));
    try std.testing.expect(try jsonDocMatchesPatternFilter(alloc, "doc:b", parsed_geo_doc.value, parsed_ip_range.value));
    try std.testing.expect(try jsonDocMatchesPatternFilter(alloc, "doc:b", parsed_geo_doc.value, parsed_geo_distance.value));
    try std.testing.expect(try jsonDocMatchesPatternFilter(alloc, "doc:b", parsed_geo_doc.value, parsed_geo_bbox.value));
    try std.testing.expect(try jsonDocMatchesPatternFilter(alloc, "doc:b", parsed_geo_doc.value, parsed_geo_shape.value));
    try std.testing.expect(!(try jsonDocMatchesPatternFilter(alloc, "doc:b", parsed_geo_doc.value, parsed_geo_shape_contains.value)));
    try std.testing.expect(try jsonDocMatchesPatternFilter(alloc, "doc:b", parsed_geo_doc.value, parsed_doc_ids.value));
    try std.testing.expect(try jsonDocMatchesPatternFilter(alloc, "doc:b", parsed_geo_doc.value, parsed_path_exists.value));
    try std.testing.expect(try jsonDocMatchesPatternFilter(alloc, "doc:b", parsed_geo_doc.value, parsed_path_term_bool.value));
    try std.testing.expect(try jsonDocMatchesPatternFilter(alloc, "doc:b", parsed_geo_doc.value, parsed_terms_with_null.value));
}

test "executeSingleNonPatternQueryWithSets hydrates graph documents from include_documents" {
    const alloc = std.testing.allocator;

    const Harness = struct {
        loaded: bool = false,
        seen_generation: ?u64 = null,

        fn findShortestPath(
            _: ?*anyopaque,
            _: Allocator,
            _: *const types.NamedGraphQuery,
            _: []const u8,
            _: []const u8,
        ) anyerror!?types.GraphPath {
            return null;
        }

        fn findKShortestPaths(
            _: ?*anyopaque,
            alloc_inner: Allocator,
            _: *const types.NamedGraphQuery,
            _: []const u8,
            _: []const u8,
        ) anyerror![]types.GraphPath {
            return try alloc_inner.alloc(types.GraphPath, 0);
        }

        fn executeGraphQuery(
            _: ?*anyopaque,
            alloc_inner: Allocator,
            named: *const types.NamedGraphQuery,
            start_key_refs: []const []const u8,
            target_keys: [][]u8,
        ) anyerror!graph_query_mod.GraphQueryResult {
            try std.testing.expectEqualStrings("tree_search", named.name);
            try std.testing.expectEqual(@as(usize, 1), start_key_refs.len);
            try std.testing.expectEqualStrings("doc:root", start_key_refs[0]);
            try std.testing.expectEqual(@as(usize, 0), target_keys.len);

            const nodes = try alloc_inner.alloc(graph_query_mod.GraphResultNode, 1);
            nodes[0] = .{
                .key = try alloc_inner.dupe(u8, "doc:child"),
                .depth = 1,
                .distance = 1.0,
                .path = null,
                .path_edges = null,
            };
            return .{
                .nodes = nodes,
                .matches = &.{},
            };
        }

        fn loadProjectedDocument(
            ctx: ?*anyopaque,
            alloc_inner: Allocator,
            req: types.SearchRequest,
            key: []const u8,
        ) anyerror!?[]u8 {
            const self: *@This() = @ptrCast(@alignCast(ctx.?));
            self.loaded = true;
            try std.testing.expect(!req.include_stored);
            try std.testing.expectEqualStrings("doc:child", key);
            return try alloc_inner.dupe(u8, "{\"title\":\"child\",\"body\":\"details about the architecture\"}");
        }

        fn lookupOrdinal(
            ctx: ?*anyopaque,
            _: Allocator,
            doc_id: []const u8,
            generation: ?u64,
        ) anyerror!?doc_set.DocOrdinal {
            const self: *@This() = @ptrCast(@alignCast(ctx.?));
            self.seen_generation = generation;
            if (std.mem.eql(u8, doc_id, "doc:child")) return 77;
            return null;
        }
    };

    var harness = Harness{};
    var named = types.NamedGraphQuery{
        .name = "tree_search",
        .query = .{
            .query_type = .traverse,
            .index_name = "doc_hierarchy",
            .start_nodes = .{ .keys = &.{"doc:root"} },
            .params = .{},
            .include_documents = true,
        },
    };

    var result = try executeSingleNonPatternQueryWithSets(alloc, .{
        .include_stored = false,
        .limit = 10,
        .identity_read_generation = 42,
    }, &named, &.{}, .{
        .ctx = &harness,
        .find_shortest_path = Harness.findShortestPath,
        .find_k_shortest_paths = Harness.findKShortestPaths,
        .execute_graph_query = Harness.executeGraphQuery,
        .load_projected_document = Harness.loadProjectedDocument,
        .lookup_doc_ordinal = Harness.lookupOrdinal,
    });
    defer result.deinit(alloc);

    try std.testing.expect(harness.loaded);
    try std.testing.expectEqual(@as(?u64, 42), harness.seen_generation);
    try std.testing.expectEqual(@as(usize, 1), result.hits.len);
    try std.testing.expectEqualStrings("doc:child", result.hits[0].id);
    try std.testing.expectEqual(@as(?doc_set.DocOrdinal, 77), result.hits[0].doc_ordinal);
    try std.testing.expect(result.hits[0].stored_data != null);
    try std.testing.expectEqualStrings("{\"title\":\"child\",\"body\":\"details about the architecture\"}", result.hits[0].stored_data.?);
}

test "executeSearchGraphWithSets preserves node ordinals" {
    const alloc = std.testing.allocator;

    const Harness = struct {
        seen_generation: ?u64 = null,

        fn executeGraphQuery(
            _: ?*anyopaque,
            alloc_inner: Allocator,
            graph_query: graph_query_mod.GraphQuery,
            start_key_refs: []const []const u8,
            target_keys: [][]u8,
        ) anyerror!graph_query_mod.GraphQueryResult {
            try std.testing.expectEqualStrings("doc_hierarchy", graph_query.index_name);
            try std.testing.expectEqual(@as(usize, 1), start_key_refs.len);
            try std.testing.expectEqualStrings("doc:root", start_key_refs[0]);
            try std.testing.expectEqual(@as(usize, 0), target_keys.len);

            const nodes = try alloc_inner.alloc(graph_query_mod.GraphResultNode, 1);
            nodes[0] = .{
                .key = try alloc_inner.dupe(u8, "doc:child"),
                .depth = 1,
                .distance = 1.0,
                .path = null,
                .path_edges = null,
            };
            return .{
                .nodes = nodes,
                .matches = &.{},
            };
        }

        fn loadProjectedDocument(
            _: ?*anyopaque,
            _: Allocator,
            _: types.SearchRequest,
            _: []const u8,
        ) anyerror!?[]u8 {
            return error.TestUnexpectedResult;
        }

        fn lookupOrdinal(
            ctx: ?*anyopaque,
            _: Allocator,
            doc_id: []const u8,
            generation: ?u64,
        ) anyerror!?doc_set.DocOrdinal {
            const self: *@This() = @ptrCast(@alignCast(ctx.?));
            self.seen_generation = generation;
            if (std.mem.eql(u8, doc_id, "doc:child")) return 77;
            return null;
        }
    };

    var harness = Harness{};
    var result = try executeSearchGraphWithSets(alloc, .{
        .limit = 10,
        .identity_read_generation = 42,
    }, .{
        .query_type = .traverse,
        .index_name = "doc_hierarchy",
        .start_nodes = .{ .keys = &.{"doc:root"} },
        .params = .{},
    }, &.{}, .{
        .ctx = &harness,
        .execute_graph_query = Harness.executeGraphQuery,
        .load_projected_document = Harness.loadProjectedDocument,
        .lookup_doc_ordinal = Harness.lookupOrdinal,
    });
    defer result.deinit();

    try std.testing.expectEqual(@as(?u64, 42), harness.seen_generation);
    try std.testing.expectEqual(@as(usize, 1), result.hits.len);
    try std.testing.expectEqualStrings("doc:child", result.hits[0].id);
    try std.testing.expectEqual(@as(?doc_set.DocOrdinal, 77), result.hits[0].doc_ordinal);
}

test "cloneNamedSetAsResult preserves hit ordinals" {
    const alloc = std.testing.allocator;

    const hit_id = try alloc.dupe(u8, "doc:a");
    defer alloc.free(hit_id);
    const hit_stored = try alloc.dupe(u8, "{\"title\":\"A\"}");
    defer alloc.free(hit_stored);
    const source_hits = [_]types.SearchHit{.{
        .id = hit_id,
        .doc_ordinal = 11,
        .score = 0.5,
        .stored_data = hit_stored,
    }};
    const set = NamedResultSet{
        .name = "dense",
        .hits = &source_hits,
        .total_hits = 1,
    };

    var without_stored = try cloneNamedSetAsResult(alloc, set, false);
    defer without_stored.deinit();
    try std.testing.expectEqual(@as(usize, 1), without_stored.hits.len);
    try std.testing.expectEqualStrings("doc:a", without_stored.hits[0].id);
    try std.testing.expectEqual(@as(?doc_set.DocOrdinal, 11), without_stored.hits[0].doc_ordinal);
    try std.testing.expect(without_stored.hits[0].stored_data == null);

    var with_stored = try cloneNamedSetAsResult(alloc, set, true);
    defer with_stored.deinit();
    try std.testing.expectEqual(@as(?doc_set.DocOrdinal, 11), with_stored.hits[0].doc_ordinal);
    try std.testing.expectEqualStrings("{\"title\":\"A\"}", with_stored.hits[0].stored_data.?);
}

test "buildPatternDocumentHits preserves resolved binding ordinals" {
    const alloc = std.testing.allocator;

    var bindings = try alloc.alloc(types.GraphPatternBinding, 2);
    var match = types.GraphPatternMatch{
        .bindings = bindings,
        .path = &.{},
    };
    defer match.deinit(alloc);
    bindings[0] = .{
        .alias = try alloc.dupe(u8, "root"),
        .node = .{
            .key = try alloc.dupe(u8, "doc:a"),
            .depth = 0,
            .distance = 0.0,
            .path = null,
            .path_edges = null,
        },
    };
    bindings[1] = .{
        .alias = try alloc.dupe(u8, "child"),
        .node = .{
            .key = try alloc.dupe(u8, "doc:b"),
            .depth = 1,
            .distance = 1.0,
            .path = null,
            .path_edges = null,
        },
    };

    const Harness = struct {
        seen_generation: ?u64 = null,

        fn matchPattern(
            _: ?*anyopaque,
            _: Allocator,
            _: *const types.NamedGraphQuery,
            _: []const []const u8,
        ) anyerror![]graph_pattern_mod.PatternMatch {
            return error.TestUnexpectedResult;
        }

        fn loadProjectedDocument(
            _: ?*anyopaque,
            _: Allocator,
            _: graph_query_mod.GraphQuery,
            _: []const u8,
        ) anyerror!?[]u8 {
            return error.TestUnexpectedResult;
        }

        fn lookupOrdinal(
            ctx: ?*anyopaque,
            _: Allocator,
            doc_id: []const u8,
            generation: ?u64,
        ) anyerror!?doc_set.DocOrdinal {
            const self: *@This() = @ptrCast(@alignCast(ctx orelse return error.InvalidArgument));
            self.seen_generation = generation;
            if (std.mem.eql(u8, doc_id, "doc:a")) return 11;
            if (std.mem.eql(u8, doc_id, "doc:b")) return 12;
            return null;
        }
    };

    var harness = Harness{};
    const matches = [_]types.GraphPatternMatch{match};
    const hits = try buildPatternDocumentHits(alloc, .{
        .query_type = .pattern,
        .index_name = "graph",
        .start_nodes = .{ .keys = &.{} },
        .include_documents = false,
    }, 42, &matches, .{
        .ctx = &harness,
        .match_pattern = Harness.matchPattern,
        .load_projected_document = Harness.loadProjectedDocument,
        .lookup_doc_ordinal = Harness.lookupOrdinal,
    });
    defer {
        for (hits) |*hit| hit.deinit(alloc);
        if (hits.len > 0) alloc.free(hits);
    }

    try std.testing.expectEqual(@as(?u64, 42), harness.seen_generation);
    try std.testing.expectEqual(@as(usize, 2), hits.len);
    try std.testing.expectEqualStrings("doc:a", hits[0].id);
    try std.testing.expectEqual(@as(?doc_set.DocOrdinal, 11), hits[0].doc_ordinal);
    try std.testing.expectEqualStrings("doc:b", hits[1].id);
    try std.testing.expectEqual(@as(?doc_set.DocOrdinal, 12), hits[1].doc_ordinal);
}

test "fuseNamedSets preserves source hit ordinals" {
    const alloc = std.testing.allocator;

    const id_a = try alloc.dupe(u8, "doc:a");
    defer alloc.free(id_a);
    const id_b = try alloc.dupe(u8, "doc:b");
    defer alloc.free(id_b);
    const hits = [_]types.SearchHit{
        .{ .id = id_a, .doc_ordinal = 11, .score = 1.0 },
        .{ .id = id_b, .doc_ordinal = 12, .score = 0.5 },
    };
    const named_sets = [_]NamedResultSet{.{
        .name = "dense",
        .hits = &hits,
        .total_hits = hits.len,
    }};

    const Harness = struct {
        fn loadProjectedDocument(
            _: ?*anyopaque,
            _: Allocator,
            _: types.SearchRequest,
            _: []const u8,
        ) anyerror!?[]u8 {
            return error.TestUnexpectedResult;
        }
    };

    var result = try fuseNamedSets(alloc, .{
        .limit = 2,
        .include_stored = false,
    }, &named_sets, .{
        .ctx = null,
        .load_projected_document = Harness.loadProjectedDocument,
    });
    defer result.deinit();

    try std.testing.expectEqual(@as(usize, 2), result.hits.len);
    try std.testing.expectEqualStrings("doc:a", result.hits[0].id);
    try std.testing.expectEqual(@as(?doc_set.DocOrdinal, 11), result.hits[0].doc_ordinal);
    try std.testing.expectEqualStrings("doc:b", result.hits[1].id);
    try std.testing.expectEqual(@as(?doc_set.DocOrdinal, 12), result.hits[1].doc_ordinal);
}

test "fuseNamedSets deduplicates aliases by ordinal when complete" {
    const alloc = std.testing.allocator;

    const dense_id = try alloc.dupe(u8, "doc:a");
    defer alloc.free(dense_id);
    const sparse_id = try alloc.dupe(u8, "alias:a");
    defer alloc.free(sparse_id);
    const dense_hits = [_]types.SearchHit{.{
        .id = dense_id,
        .doc_ordinal = 11,
        .score = 1.0,
    }};
    const sparse_hits = [_]types.SearchHit{.{
        .id = sparse_id,
        .doc_ordinal = 11,
        .score = 0.9,
    }};
    const named_sets = [_]NamedResultSet{
        .{
            .name = "dense",
            .hits = &dense_hits,
            .total_hits = dense_hits.len,
        },
        .{
            .name = "sparse",
            .hits = &sparse_hits,
            .total_hits = sparse_hits.len,
        },
    };

    const Harness = struct {
        fn loadProjectedDocument(
            _: ?*anyopaque,
            _: Allocator,
            _: types.SearchRequest,
            _: []const u8,
        ) anyerror!?[]u8 {
            return error.TestUnexpectedResult;
        }
    };

    var result = try fuseNamedSets(alloc, .{
        .limit = 10,
        .include_stored = false,
    }, &named_sets, .{
        .ctx = null,
        .load_projected_document = Harness.loadProjectedDocument,
    });
    defer result.deinit();

    try std.testing.expectEqual(@as(u32, 1), result.total_hits);
    try std.testing.expectEqual(@as(usize, 1), result.hits.len);
    try std.testing.expectEqualStrings("doc:a", result.hits[0].id);
    try std.testing.expectEqual(@as(?doc_set.DocOrdinal, 11), result.hits[0].doc_ordinal);
}

test "fuseNamedSets drops conflicting source hit ordinals" {
    const alloc = std.testing.allocator;

    const dense_id = try alloc.dupe(u8, "doc:a");
    defer alloc.free(dense_id);
    const sparse_id = try alloc.dupe(u8, "doc:a");
    defer alloc.free(sparse_id);
    const dense_hits = [_]types.SearchHit{.{
        .id = dense_id,
        .doc_ordinal = 11,
        .score = 1.0,
    }};
    const sparse_hits = [_]types.SearchHit{.{
        .id = sparse_id,
        .doc_ordinal = 12,
        .score = 0.9,
    }};
    const named_sets = [_]NamedResultSet{
        .{
            .name = "dense",
            .hits = &dense_hits,
            .total_hits = dense_hits.len,
        },
        .{
            .name = "sparse",
            .hits = &sparse_hits,
            .total_hits = sparse_hits.len,
        },
    };

    const Harness = struct {
        fn loadProjectedDocument(
            _: ?*anyopaque,
            _: Allocator,
            _: types.SearchRequest,
            _: []const u8,
        ) anyerror!?[]u8 {
            return error.TestUnexpectedResult;
        }
    };

    var result = try fuseNamedSets(alloc, .{
        .limit = 1,
        .include_stored = false,
    }, &named_sets, .{
        .ctx = null,
        .load_projected_document = Harness.loadProjectedDocument,
    });
    defer result.deinit();

    try std.testing.expectEqual(@as(usize, 1), result.hits.len);
    try std.testing.expectEqualStrings("doc:a", result.hits[0].id);
    try std.testing.expect(result.hits[0].doc_ordinal == null);
}

test "executeGraphQueries projects base hits to resolved doc-set for unbounded selectors" {
    const alloc = std.testing.allocator;

    const Harness = struct {
        base_resolved: bool = false,
        projected: bool = false,
        resolve_calls: usize = 0,

        fn execute(
            ctx: ?*anyopaque,
            alloc_inner: Allocator,
            req: types.SearchRequest,
            named: *const types.NamedGraphQuery,
            named_sets: []const NamedResultSet,
        ) anyerror!types.GraphSearchResult {
            return try executeSingleNonPatternQueryWithSets(alloc_inner, req, named, named_sets, .{
                .ctx = ctx,
                .find_shortest_path = findShortestPath,
                .find_k_shortest_paths = findKShortestPaths,
                .execute_graph_query = executeGraphQuery,
                .load_projected_document = loadProjectedDocument,
                .resolve_doc_set_doc_ids = resolveDocSetDocIds,
            });
        }

        fn findShortestPath(
            _: ?*anyopaque,
            _: Allocator,
            _: *const types.NamedGraphQuery,
            _: []const u8,
            _: []const u8,
        ) anyerror!?types.GraphPath {
            return error.TestUnexpectedResult;
        }

        fn findKShortestPaths(
            _: ?*anyopaque,
            _: Allocator,
            _: *const types.NamedGraphQuery,
            _: []const u8,
            _: []const u8,
        ) anyerror![]types.GraphPath {
            return error.TestUnexpectedResult;
        }

        fn executeGraphQuery(
            ctx: ?*anyopaque,
            alloc_inner: Allocator,
            named: *const types.NamedGraphQuery,
            start_key_refs: []const []const u8,
            target_keys: [][]u8,
        ) anyerror!graph_query_mod.GraphQueryResult {
            const self: *@This() = @ptrCast(@alignCast(ctx.?));
            self.projected = true;
            try std.testing.expectEqualStrings("citations", named.name);
            try std.testing.expectEqual(@as(usize, 2), start_key_refs.len);
            try std.testing.expectEqualStrings("doc:a", start_key_refs[0]);
            try std.testing.expectEqualStrings("doc:b", start_key_refs[1]);
            try std.testing.expectEqual(@as(usize, 0), target_keys.len);
            return .{
                .nodes = try alloc_inner.alloc(graph_query_mod.GraphResultNode, 0),
                .matches = &.{},
            };
        }

        fn loadProjectedDocument(
            _: ?*anyopaque,
            _: Allocator,
            _: types.SearchRequest,
            _: []const u8,
        ) anyerror!?[]u8 {
            return null;
        }

        fn resolveHits(
            ctx: ?*anyopaque,
            alloc_inner: Allocator,
            _: types.SearchRequest,
            hits: []const types.SearchHit,
        ) anyerror!doc_set.ResolvedDocSet {
            const self: *@This() = @ptrCast(@alignCast(ctx.?));
            if (self.resolve_calls == 0) {
                self.base_resolved = true;
                try std.testing.expectEqual(@as(usize, 1), hits.len);
                try std.testing.expectEqualStrings("public:seed", hits[0].id);
            } else {
                try std.testing.expectEqual(@as(usize, 0), hits.len);
            }
            self.resolve_calls += 1;
            return try doc_set.fromOrdinalsAlloc(alloc_inner, &.{ 2, 1 });
        }

        fn resolveDocSetDocIds(
            _: ?*anyopaque,
            alloc_inner: Allocator,
            set: *const doc_set.ResolvedDocSet,
            generation: ?u64,
        ) anyerror!?[][]u8 {
            try std.testing.expectEqual(@as(?u64, 42), generation);
            try std.testing.expect(set.containsOrdinal(1));
            try std.testing.expect(set.containsOrdinal(2));

            var out = try alloc_inner.alloc([]u8, 2);
            errdefer alloc_inner.free(out);
            out[0] = try alloc_inner.dupe(u8, "doc:a");
            errdefer alloc_inner.free(out[0]);
            out[1] = try alloc_inner.dupe(u8, "doc:b");
            return out;
        }
    };

    var harness = Harness{};
    const base_id = try alloc.dupe(u8, "public:seed");
    defer alloc.free(base_id);
    const base_hits = [_]types.SearchHit{.{ .id = base_id }};
    const queries = [_]types.NamedGraphQuery{.{
        .name = "citations",
        .query = .{
            .query_type = .traverse,
            .index_name = "graph",
            .start_nodes = .{ .result_ref = .{ .ref = "$full_text_results", .limit = 0 } },
            .params = .{},
        },
    }};

    const results = try executeGraphQueries(alloc, .{ .identity_read_generation = 42 }, &queries, &base_hits, @intCast(base_hits.len), .{
        .ctx = &harness,
        .func = Harness.execute,
        .resolve_hits_to_doc_set = Harness.resolveHits,
    });
    defer {
        for (results) |*result| result.deinit(alloc);
        alloc.free(results);
    }

    try std.testing.expect(harness.base_resolved);
    try std.testing.expect(harness.projected);
}

test "executeGraphQueries supports limited embeddings result_ref without base doc-set projection" {
    const alloc = std.testing.allocator;

    const Harness = struct {
        projected: bool = false,
        resolve_calls: usize = 0,

        fn execute(
            ctx: ?*anyopaque,
            alloc_inner: Allocator,
            req: types.SearchRequest,
            named: *const types.NamedGraphQuery,
            named_sets: []const NamedResultSet,
        ) anyerror!types.GraphSearchResult {
            return try executeSingleNonPatternQueryWithSets(alloc_inner, req, named, named_sets, .{
                .ctx = ctx,
                .find_shortest_path = findShortestPath,
                .find_k_shortest_paths = findKShortestPaths,
                .execute_graph_query = executeGraphQuery,
                .load_projected_document = loadProjectedDocument,
                .resolve_doc_set_doc_ids = resolveDocSetDocIds,
            });
        }

        fn findShortestPath(
            _: ?*anyopaque,
            _: Allocator,
            _: *const types.NamedGraphQuery,
            _: []const u8,
            _: []const u8,
        ) anyerror!?types.GraphPath {
            return error.TestUnexpectedResult;
        }

        fn findKShortestPaths(
            _: ?*anyopaque,
            _: Allocator,
            _: *const types.NamedGraphQuery,
            _: []const u8,
            _: []const u8,
        ) anyerror![]types.GraphPath {
            return error.TestUnexpectedResult;
        }

        fn executeGraphQuery(
            ctx: ?*anyopaque,
            alloc_inner: Allocator,
            _: *const types.NamedGraphQuery,
            start_key_refs: []const []const u8,
            target_keys: [][]u8,
        ) anyerror!graph_query_mod.GraphQueryResult {
            const self: *@This() = @ptrCast(@alignCast(ctx.?));
            self.projected = true;
            try std.testing.expectEqual(@as(usize, 1), start_key_refs.len);
            try std.testing.expectEqualStrings("doc:z", start_key_refs[0]);
            try std.testing.expectEqual(@as(usize, 0), target_keys.len);
            return .{
                .nodes = try alloc_inner.alloc(graph_query_mod.GraphResultNode, 0),
                .matches = &.{},
            };
        }

        fn loadProjectedDocument(
            _: ?*anyopaque,
            _: Allocator,
            _: types.SearchRequest,
            _: []const u8,
        ) anyerror!?[]u8 {
            return null;
        }

        fn resolveHits(
            ctx: ?*anyopaque,
            alloc_inner: Allocator,
            _: types.SearchRequest,
            hits: []const types.SearchHit,
        ) anyerror!doc_set.ResolvedDocSet {
            const self: *@This() = @ptrCast(@alignCast(ctx.?));
            self.resolve_calls += 1;
            try std.testing.expectEqual(@as(usize, 0), hits.len);
            return try doc_set.fromOrdinalsAlloc(alloc_inner, &.{});
        }

        fn resolveDocSetDocIds(
            _: ?*anyopaque,
            _: Allocator,
            _: *const doc_set.ResolvedDocSet,
            _: ?u64,
        ) anyerror!?[][]u8 {
            return error.TestUnexpectedResult;
        }
    };

    var harness = Harness{};
    const hit_z = try alloc.dupe(u8, "doc:z");
    defer alloc.free(hit_z);
    const hit_a = try alloc.dupe(u8, "doc:a");
    defer alloc.free(hit_a);
    const base_hits = [_]types.SearchHit{
        .{ .id = hit_z },
        .{ .id = hit_a },
    };
    const queries = [_]types.NamedGraphQuery{.{
        .name = "citations",
        .query = .{
            .query_type = .traverse,
            .index_name = "graph",
            .start_nodes = .{ .result_ref = .{ .ref = "$embeddings_results", .limit = 1 } },
            .params = .{},
        },
    }};

    const results = try executeGraphQueries(alloc, .{}, &queries, &base_hits, @intCast(base_hits.len), .{
        .ctx = &harness,
        .func = Harness.execute,
        .resolve_hits_to_doc_set = Harness.resolveHits,
    });
    defer {
        for (results) |*result| result.deinit(alloc);
        alloc.free(results);
    }

    try std.testing.expect(harness.projected);
    try std.testing.expectEqual(@as(usize, 1), harness.resolve_calls);
}

test "executeGraphQueries releases graph result when doc-set materialization fails" {
    const alloc = std.testing.allocator;

    const Harness = struct {
        fn execute(
            _: ?*anyopaque,
            alloc_inner: Allocator,
            _: types.SearchRequest,
            named: *const types.NamedGraphQuery,
            _: []const NamedResultSet,
        ) anyerror!types.GraphSearchResult {
            const hits = try alloc_inner.alloc(types.SearchHit, 1);
            errdefer alloc_inner.free(hits);
            hits[0] = .{ .id = try alloc_inner.dupe(u8, "doc:result") };
            return .{
                .name = try alloc_inner.dupe(u8, named.name),
                .hits = hits,
                .total_hits = 1,
            };
        }

        fn resolveHits(
            _: ?*anyopaque,
            _: Allocator,
            _: types.SearchRequest,
            hits: []const types.SearchHit,
        ) anyerror!doc_set.ResolvedDocSet {
            try std.testing.expectEqual(@as(usize, 1), hits.len);
            try std.testing.expectEqualStrings("doc:result", hits[0].id);
            return error.TestExpectedError;
        }
    };

    const queries = [_]types.NamedGraphQuery{.{
        .name = "citations",
        .query = .{
            .query_type = .traverse,
            .index_name = "graph",
            .start_nodes = .{ .keys = &.{"doc:start"} },
            .params = .{},
        },
    }};

    try std.testing.expectError(error.TestExpectedError, executeGraphQueries(alloc, .{}, &queries, &.{}, 0, .{
        .ctx = null,
        .func = Harness.execute,
        .resolve_hits_to_doc_set = Harness.resolveHits,
    }));
}

test "graph result_ref uses resolved doc-set for unbounded selectors" {
    const alloc = std.testing.allocator;

    const Harness = struct {
        projected: bool = false,
        resolved: bool = false,

        fn findShortestPath(
            _: ?*anyopaque,
            _: Allocator,
            _: *const types.NamedGraphQuery,
            _: []const u8,
            _: []const u8,
        ) anyerror!?types.GraphPath {
            return error.TestUnexpectedResult;
        }

        fn findKShortestPaths(
            _: ?*anyopaque,
            _: Allocator,
            _: *const types.NamedGraphQuery,
            _: []const u8,
            _: []const u8,
        ) anyerror![]types.GraphPath {
            return error.TestUnexpectedResult;
        }

        fn executeGraphQuery(
            ctx: ?*anyopaque,
            alloc_inner: Allocator,
            named: *const types.NamedGraphQuery,
            start_key_refs: []const []const u8,
            target_keys: [][]u8,
        ) anyerror!graph_query_mod.GraphQueryResult {
            const self: *@This() = @ptrCast(@alignCast(ctx.?));
            self.projected = true;
            try std.testing.expectEqualStrings("tree_search", named.name);
            try std.testing.expectEqual(@as(usize, 2), start_key_refs.len);
            try std.testing.expectEqualStrings("doc:a", start_key_refs[0]);
            try std.testing.expectEqualStrings("doc:b", start_key_refs[1]);
            try std.testing.expectEqual(@as(usize, 0), target_keys.len);
            return .{
                .nodes = try alloc_inner.alloc(graph_query_mod.GraphResultNode, 0),
                .matches = &.{},
            };
        }

        fn loadProjectedDocument(
            _: ?*anyopaque,
            _: Allocator,
            _: types.SearchRequest,
            _: []const u8,
        ) anyerror!?[]u8 {
            return null;
        }

        fn resolveDocSetDocIds(
            ctx: ?*anyopaque,
            alloc_inner: Allocator,
            set: *const doc_set.ResolvedDocSet,
            generation: ?u64,
        ) anyerror!?[][]u8 {
            const self: *@This() = @ptrCast(@alignCast(ctx.?));
            self.resolved = true;
            try std.testing.expectEqual(@as(?u64, 42), generation);
            try std.testing.expect(set.containsOrdinal(1));
            try std.testing.expect(set.containsOrdinal(2));

            var out = try alloc_inner.alloc([]u8, 2);
            errdefer alloc_inner.free(out);
            out[0] = try alloc_inner.dupe(u8, "doc:a");
            errdefer alloc_inner.free(out[0]);
            out[1] = try alloc_inner.dupe(u8, "doc:b");
            return out;
        }
    };

    var harness = Harness{};
    var resolved = try doc_set.fromOrdinalsAlloc(alloc, &.{ 2, 1 });
    defer resolved.deinit(alloc);
    const named_sets = [_]NamedResultSet{.{
        .name = "$full_text_results",
        .hits = &.{},
        .total_hits = 0,
        .resolved_doc_set = &resolved,
        .resolved_doc_set_complete = true,
    }};
    var named = types.NamedGraphQuery{
        .name = "tree_search",
        .query = .{
            .query_type = .traverse,
            .index_name = "doc_hierarchy",
            .start_nodes = .{ .result_ref = .{ .ref = "$full_text_results", .limit = 0 } },
            .params = .{},
        },
    };

    var result = try executeSingleNonPatternQueryWithSets(alloc, .{ .identity_read_generation = 42 }, &named, &named_sets, .{
        .ctx = &harness,
        .find_shortest_path = Harness.findShortestPath,
        .find_k_shortest_paths = Harness.findKShortestPaths,
        .execute_graph_query = Harness.executeGraphQuery,
        .load_projected_document = Harness.loadProjectedDocument,
        .resolve_doc_set_doc_ids = Harness.resolveDocSetDocIds,
    });
    defer result.deinit(alloc);

    try std.testing.expect(harness.resolved);
    try std.testing.expect(harness.projected);
}

test "graph result_ref fails closed when unbounded resolved doc-set cannot project" {
    const alloc = std.testing.allocator;

    var resolved = try doc_set.fromOrdinalsAlloc(alloc, &.{ 1, 2 });
    defer resolved.deinit(alloc);
    const hit = try alloc.dupe(u8, "doc:a");
    defer alloc.free(hit);
    const hits = [_]types.SearchHit{.{ .id = hit }};
    const named_sets = [_]NamedResultSet{.{
        .name = "$full_text_results",
        .hits = &hits,
        .total_hits = hits.len,
        .resolved_doc_set = &resolved,
        .resolved_doc_set_complete = true,
    }};
    const selector = graph_query_mod.NodeSelector{ .result_ref = .{ .ref = "$full_text_results", .limit = 0 } };

    try std.testing.expectError(error.UnsupportedQueryRequest, resolveGraphSelectorFromSets(alloc, selector, &named_sets, .{}));

    const Harness = struct {
        fn resolveDocSetDocIds(
            _: ?*anyopaque,
            _: Allocator,
            set: *const doc_set.ResolvedDocSet,
            _: ?u64,
        ) anyerror!?[][]u8 {
            try std.testing.expect(set.containsOrdinal(1));
            return null;
        }
    };
    try std.testing.expectError(error.UnsupportedQueryRequest, resolveGraphSelectorFromSets(alloc, selector, &named_sets, .{
        .func = Harness.resolveDocSetDocIds,
    }));
}

test "graph result_ref fails closed when unbounded named set is only a page" {
    const alloc = std.testing.allocator;

    var resolved = try doc_set.fromOrdinalsAlloc(alloc, &.{1});
    defer resolved.deinit(alloc);
    const hit = try alloc.dupe(u8, "doc:a");
    defer alloc.free(hit);
    const hits = [_]types.SearchHit{.{ .id = hit }};
    const named_sets = [_]NamedResultSet{.{
        .name = "$fused_results",
        .hits = &hits,
        .total_hits = 2,
        .resolved_doc_set = &resolved,
    }};
    const selector = graph_query_mod.NodeSelector{ .result_ref = .{ .ref = "$fused_results", .limit = 0 } };

    const Harness = struct {
        fn resolveDocSetDocIds(
            _: ?*anyopaque,
            _: Allocator,
            _: *const doc_set.ResolvedDocSet,
            _: ?u64,
        ) anyerror!?[][]u8 {
            return error.TestUnexpectedResult;
        }
    };
    try std.testing.expectError(error.UnsupportedQueryRequest, resolveGraphSelectorFromSets(alloc, selector, &named_sets, .{
        .func = Harness.resolveDocSetDocIds,
    }));
}

test "graph result_ref uses complete node doc-set when hits are paged" {
    const alloc = std.testing.allocator;

    const Harness = struct {
        seed_node_projection_count: usize = 0,
        dependent_key_count: usize = 0,

        fn executeGraphQuery(
            ctx: ?*anyopaque,
            alloc_inner: Allocator,
            req: types.SearchRequest,
            named: *const types.NamedGraphQuery,
            named_sets: []const NamedResultSet,
        ) anyerror!types.GraphSearchResult {
            const self: *@This() = @ptrCast(@alignCast(ctx orelse return error.InvalidArgument));
            if (std.mem.eql(u8, named.name, "seed")) {
                const nodes = try alloc_inner.alloc(graph_query_mod.GraphResultNode, 2);
                nodes[0] = .{ .key = try alloc_inner.dupe(u8, "doc:a"), .depth = 0, .distance = 1.0, .path = null, .path_edges = null };
                nodes[1] = .{ .key = try alloc_inner.dupe(u8, "doc:b"), .depth = 0, .distance = 2.0, .path = null, .path_edges = null };
                const hits = try alloc_inner.alloc(types.SearchHit, 1);
                hits[0] = .{ .id = try alloc_inner.dupe(u8, "doc:a") };
                return .{
                    .name = try alloc_inner.dupe(u8, named.name),
                    .nodes = nodes,
                    .hits = hits,
                    .total_hits = 2,
                };
            }

            const keys = try resolveGraphSelectorFromSets(alloc_inner, named.query.start_nodes, named_sets, .{
                .ctx = self,
                .func = resolveDocSetDocIds,
                .identity_read_generation = req.identity_read_generation,
            });
            defer {
                for (keys) |key| alloc_inner.free(key);
                if (keys.len > 0) alloc_inner.free(keys);
            }
            self.dependent_key_count = keys.len;
            const hits = try alloc_inner.alloc(types.SearchHit, keys.len);
            errdefer alloc_inner.free(hits);
            for (keys, 0..) |key, i| {
                hits[i] = .{ .id = try alloc_inner.dupe(u8, key) };
            }
            return .{
                .name = try alloc_inner.dupe(u8, named.name),
                .nodes = try alloc_inner.alloc(graph_query_mod.GraphResultNode, 0),
                .hits = hits,
                .total_hits = @intCast(hits.len),
            };
        }

        fn resolveNodesToDocSet(
            ctx: ?*anyopaque,
            alloc_inner: Allocator,
            _: types.SearchRequest,
            nodes: []const graph_query_mod.GraphResultNode,
        ) anyerror!doc_set.ResolvedDocSet {
            const self: *@This() = @ptrCast(@alignCast(ctx orelse return error.InvalidArgument));
            if (nodes.len == 0) return .none;
            self.seed_node_projection_count += 1;
            try std.testing.expectEqual(@as(usize, 2), nodes.len);
            try std.testing.expectEqualStrings("doc:a", nodes[0].key);
            try std.testing.expectEqualStrings("doc:b", nodes[1].key);
            return try doc_set.fromOrdinalsAlloc(alloc_inner, &.{ 1, 2 });
        }

        fn resolveHitsToDocSet(
            _: ?*anyopaque,
            _: Allocator,
            _: types.SearchRequest,
            hits: []const types.SearchHit,
        ) anyerror!doc_set.ResolvedDocSet {
            if (hits.len == 1) return error.TestUnexpectedResult;
            return .none;
        }

        fn resolveDocSetDocIds(
            _: ?*anyopaque,
            alloc_inner: Allocator,
            set: *const doc_set.ResolvedDocSet,
            generation: ?u64,
        ) anyerror!?[][]u8 {
            try std.testing.expectEqual(@as(?u64, 42), generation);
            try std.testing.expect(set.containsOrdinal(1));
            try std.testing.expect(set.containsOrdinal(2));
            const out = try alloc_inner.alloc([]u8, 2);
            errdefer alloc_inner.free(out);
            out[0] = try alloc_inner.dupe(u8, "doc:a");
            errdefer alloc_inner.free(out[0]);
            out[1] = try alloc_inner.dupe(u8, "doc:b");
            return out;
        }
    };

    var harness = Harness{};
    var queries = [_]types.NamedGraphQuery{
        .{
            .name = "seed",
            .query = .{
                .query_type = .traverse,
                .index_name = "graph",
                .start_nodes = .{ .keys = &.{"doc:root"} },
                .params = .{},
            },
        },
        .{
            .name = "dependent",
            .query = .{
                .query_type = .traverse,
                .index_name = "graph",
                .start_nodes = .{ .result_ref = .{ .ref = "$graph_results.seed", .limit = 0 } },
                .params = .{},
            },
        },
    };

    const results = try executeGraphQueriesWithSets(alloc, .{ .limit = 1, .identity_read_generation = 42 }, &queries, &.{}, .{
        .ctx = &harness,
        .func = Harness.executeGraphQuery,
        .resolve_hits_to_doc_set = Harness.resolveHitsToDocSet,
        .resolve_nodes_to_doc_set = Harness.resolveNodesToDocSet,
    });
    defer {
        for (results) |*result| result.deinit(alloc);
        alloc.free(results);
    }

    try std.testing.expectEqual(@as(usize, 1), harness.seed_node_projection_count);
    try std.testing.expectEqual(@as(usize, 2), harness.dependent_key_count);
}

test "graph result_ref with limit preserves hit order" {
    const alloc = std.testing.allocator;

    const Harness = struct {
        projected: bool = false,

        fn findShortestPath(
            _: ?*anyopaque,
            _: Allocator,
            _: *const types.NamedGraphQuery,
            _: []const u8,
            _: []const u8,
        ) anyerror!?types.GraphPath {
            return error.TestUnexpectedResult;
        }

        fn findKShortestPaths(
            _: ?*anyopaque,
            _: Allocator,
            _: *const types.NamedGraphQuery,
            _: []const u8,
            _: []const u8,
        ) anyerror![]types.GraphPath {
            return error.TestUnexpectedResult;
        }

        fn executeGraphQuery(
            ctx: ?*anyopaque,
            alloc_inner: Allocator,
            _: *const types.NamedGraphQuery,
            start_key_refs: []const []const u8,
            target_keys: [][]u8,
        ) anyerror!graph_query_mod.GraphQueryResult {
            const self: *@This() = @ptrCast(@alignCast(ctx.?));
            self.projected = true;
            try std.testing.expectEqual(@as(usize, 1), start_key_refs.len);
            try std.testing.expectEqualStrings("doc:z", start_key_refs[0]);
            try std.testing.expectEqual(@as(usize, 0), target_keys.len);
            return .{
                .nodes = try alloc_inner.alloc(graph_query_mod.GraphResultNode, 0),
                .matches = &.{},
            };
        }

        fn loadProjectedDocument(
            _: ?*anyopaque,
            _: Allocator,
            _: types.SearchRequest,
            _: []const u8,
        ) anyerror!?[]u8 {
            return null;
        }

        fn resolveDocSetDocIds(
            _: ?*anyopaque,
            _: Allocator,
            _: *const doc_set.ResolvedDocSet,
            _: ?u64,
        ) anyerror!?[][]u8 {
            return error.TestUnexpectedResult;
        }
    };

    var harness = Harness{};
    var resolved = try doc_set.fromOrdinalsAlloc(alloc, &.{ 1, 2 });
    defer resolved.deinit(alloc);
    const hit_z = try alloc.dupe(u8, "doc:z");
    defer alloc.free(hit_z);
    const hit_a = try alloc.dupe(u8, "doc:a");
    defer alloc.free(hit_a);
    const hits = [_]types.SearchHit{
        .{ .id = hit_z },
        .{ .id = hit_a },
    };
    const named_sets = [_]NamedResultSet{.{
        .name = "$full_text_results",
        .hits = &hits,
        .total_hits = hits.len,
        .resolved_doc_set = &resolved,
    }};
    var named = types.NamedGraphQuery{
        .name = "tree_search",
        .query = .{
            .query_type = .traverse,
            .index_name = "doc_hierarchy",
            .start_nodes = .{ .result_ref = .{ .ref = "$full_text_results", .limit = 1 } },
            .params = .{},
        },
    };

    var result = try executeSingleNonPatternQueryWithSets(alloc, .{}, &named, &named_sets, .{
        .ctx = &harness,
        .find_shortest_path = Harness.findShortestPath,
        .find_k_shortest_paths = Harness.findKShortestPaths,
        .execute_graph_query = Harness.executeGraphQuery,
        .load_projected_document = Harness.loadProjectedDocument,
        .resolve_doc_set_doc_ids = Harness.resolveDocSetDocIds,
    });
    defer result.deinit(alloc);

    try std.testing.expect(harness.projected);
}

test "graph query result doc-set resolution receives identity generation" {
    const alloc = std.testing.allocator;

    const Harness = struct {
        saw_generation: bool = false,

        fn execute(
            _: ?*anyopaque,
            alloc_inner: Allocator,
            req: types.SearchRequest,
            named: *const types.NamedGraphQuery,
            _: []const NamedResultSet,
        ) anyerror!types.GraphSearchResult {
            try std.testing.expectEqual(@as(?u64, 42), req.identity_read_generation);
            const hits = try alloc_inner.alloc(types.SearchHit, 1);
            hits[0] = .{ .id = try alloc_inner.dupe(u8, "doc:a") };
            return .{
                .name = try alloc_inner.dupe(u8, named.name),
                .hits = hits,
                .total_hits = 1,
            };
        }

        fn resolve(
            ctx: ?*anyopaque,
            alloc_inner: Allocator,
            req: types.SearchRequest,
            hits: []const types.SearchHit,
        ) anyerror!doc_set.ResolvedDocSet {
            const self: *@This() = @ptrCast(@alignCast(ctx.?));
            try std.testing.expectEqual(@as(?u64, 42), req.identity_read_generation);
            try std.testing.expectEqual(@as(usize, 1), hits.len);
            try std.testing.expectEqualStrings("doc:a", hits[0].id);
            self.saw_generation = true;
            return try doc_set.fromOrdinalsAlloc(alloc_inner, &.{7});
        }
    };

    var harness = Harness{};
    const queries = [_]types.NamedGraphQuery{.{
        .name = "g",
        .query = .{
            .query_type = .traverse,
            .index_name = "graph",
            .start_nodes = .{ .keys = &.{"doc:a"} },
            .params = .{},
        },
    }};

    const results = try executeGraphQueriesWithSets(alloc, .{
        .identity_read_generation = 42,
    }, &queries, &.{}, .{
        .ctx = &harness,
        .func = Harness.execute,
        .resolve_hits_to_doc_set = Harness.resolve,
    });
    defer {
        for (results) |*result| result.deinit(alloc);
        alloc.free(results);
    }

    try std.testing.expect(harness.saw_generation);
}
