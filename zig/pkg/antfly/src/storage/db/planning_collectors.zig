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
const db_core = @import("core.zig");
const docstore_mod = @import("../docstore.zig");
const internal_keys = @import("../internal_keys.zig");
const graph_exec = @import("query/graph_exec.zig");
const index_manager_mod = @import("catalog/index_manager.zig");
const planning_stats = @import("planning_stats.zig");
const query_search = @import("query/search_exec.zig");
const distributed_stats = @import("../../search/distributed_stats.zig");
const types = @import("types.zig");

pub fn resolveTextIndexEstimate(
    core: *db_core.DBCore,
    alloc: Allocator,
    index_name: ?[]const u8,
    req: types.SearchRequest,
) !?query_search.TextIndexEstimate {
    const entry = core.textIndexEntry(index_name) orelse return null;
    const chunk_backed = entry.chunk_name != null;
    const persistent = core.textIndex(entry.config.name) orelse return error.IndexNotFound;
    const indexed_doc_count = persistent.snapshot().global_doc_count;
    return .{
        .name = try alloc.dupe(u8, entry.config.name),
        .doc_count = if (indexed_doc_count > 0) indexed_doc_count else try scanPrimaryDocCount(core),
        .chunk_backed = chunk_backed,
        .group_chunk_parents = query_search.shouldGroupChunkParents(req, chunk_backed),
    };
}

fn scanPrimaryDocCount(core: *db_core.DBCore) !u64 {
    const byte_range = core.byteRange();
    const lower = try core.documentRangeLowerAlloc(byte_range.start);
    defer core.alloc.free(lower);
    const upper = if (byte_range.end.len > 0) try core.documentRangeUpperAlloc(byte_range.end) else null;
    defer if (upper) |buf| core.alloc.free(buf);

    var doc_count: u64 = 0;
    const CountState = struct {
        doc_count: *u64,

        fn scanEntry(ctx: ?*anyopaque, key: []const u8, value: []const u8) anyerror!docstore_mod.DocStore.ScanAction {
            _ = value;
            const state: *@This() = @ptrCast(@alignCast(ctx orelse return error.InvalidArgument));
            if (internal_keys.isPrimaryDocumentKey(key)) state.doc_count.* += 1;
            return .@"continue";
        }
    };

    var state = CountState{ .doc_count = &doc_count };
    try core.store.scanWithContext(lower, if (upper) |buf| buf else "", .{}, &state, CountState.scanEntry);
    return doc_count;
}

pub fn resolveDenseIndexEstimate(
    core: *db_core.DBCore,
    alloc: Allocator,
    index_name: ?[]const u8,
) !?query_search.EmbeddingIndexEstimate {
    const entry = core.denseIndex(index_name) orelse return null;
    return .{
        .name = try alloc.dupe(u8, entry.config.name),
        .sparse = false,
        .doc_count = entry.index.stats().active_count,
        .dims = entry.dims,
        .chunk_backed = entry.chunk_name != null,
    };
}

pub fn resolveSparseIndexEstimate(
    core: *db_core.DBCore,
    alloc: Allocator,
    index_name: ?[]const u8,
) !?query_search.EmbeddingIndexEstimate {
    const entry = core.sparseIndex(index_name) orelse return null;
    var sparse_index = entry.index;
    const sparse_stats = sparse_index.stats();
    return .{
        .name = try alloc.dupe(u8, entry.config.name),
        .sparse = true,
        .doc_count = sparse_stats.doc_count,
        .chunk_backed = entry.chunk_name != null,
    };
}

pub fn resolveGraphIndexEstimate(
    core: *db_core.DBCore,
    alloc: Allocator,
    index_name: []const u8,
) !?query_search.GraphIndexEstimate {
    const entry = core.graphIndex(index_name) orelse return null;
    var graph_index = entry.index;
    const graph_stats = try graph_index.stats(alloc);
    return .{
        .name = try alloc.dupe(u8, entry.config.name),
        .edge_count = graph_stats.edge_count,
        .node_count = graph_stats.node_count,
    };
}

pub fn collectTextQueryStats(
    core: *db_core.DBCore,
    alloc: Allocator,
    req: types.SearchRequest,
) ![]const distributed_stats.TextFieldStats {
    return try query_search.collectSearchRequestTextStats(alloc, req, .{
        .ctx = core,
        .text_index_entry = textIndexEntryCallback,
    });
}

pub fn appendDenseQueryCost(
    core: *db_core.DBCore,
    summary: *planning_stats.PlanningStatsSummary,
    req: types.SearchRequest,
    index_name: ?[]const u8,
    dense: types.DenseKnnQuery,
) !void {
    const entry = core.denseIndex(index_name) orelse return error.IndexNotFound;
    const chunk_backed = entry.chunk_name != null;
    const group_chunk_parents = query_search.shouldGroupChunkParents(req, chunk_backed);
    const effective_k: u64 = if (group_chunk_parents)
        entry.index.metadata.active_count
    else
        dense.k;
    const effort = query_search.resolvedSearchEffort(req.search_effort);
    const search_width = query_search.resolveSearchWidth(dense.k, effort, entry.index.stats());
    const epsilon = query_search.resolveSearchEpsilon(effort);

    summary.dense_query_count += 1;
    summary.dense_effective_k_total += effective_k;
    summary.dense_search_width_total += search_width;
    summary.dense_search_width_max = @max(summary.dense_search_width_max, search_width);
    summary.dense_epsilon_max = @max(summary.dense_epsilon_max, epsilon);
}

pub fn estimateStructuredFilterSample(
    core: *db_core.DBCore,
    alloc: Allocator,
    req: types.SearchRequest,
    sample_size: u32,
    corpus_doc_count: u64,
) !?planning_stats.PlanningStatsCollector.StructuredFilterSampleEstimate {
    if (sample_size == 0 or corpus_doc_count == 0) return null;
    if (req.filter_ids.len > 0 or req.exclude_ids.len > 0) return null;

    const filter_query_json = try buildStructuredFilterSampleQueryJsonAlloc(alloc, req) orelse return null;
    defer alloc.free(filter_query_json);

    const byte_range = core.byteRange();
    const lower = try core.documentRangeLowerAlloc(byte_range.start);
    defer alloc.free(lower);
    const upper = if (byte_range.end.len > 0) try core.documentRangeUpperAlloc(byte_range.end) else null;
    defer if (upper) |buf| alloc.free(buf);

    const docs = try core.scanStoreRange(alloc, lower, if (upper) |buf| buf else "");
    defer docstore_mod.DocStore.freeResults(alloc, docs);

    var sampled: u32 = 0;
    var matched: u64 = 0;
    for (docs) |doc| {
        if (!internal_keys.isPrimaryDocumentKey(doc.key)) continue;
        const raw_key = (try internal_keys.decodePrimaryDocumentKeyAlloc(alloc, doc.key)) orelse continue;
        defer alloc.free(raw_key);
        sampled += 1;
        if (try graph_exec.storedDocMatchesPatternFilter(alloc, raw_key, doc.value, filter_query_json)) {
            matched += 1;
        }
        if (sampled >= sample_size) break;
    }

    if (sampled == 0) return null;
    const estimate = (matched * corpus_doc_count + (sampled / 2)) / sampled;
    return .{
        .doc_count_estimate = @min(estimate, corpus_doc_count),
        .sample_size = sampled,
    };
}

fn buildStructuredFilterSampleQueryJsonAlloc(alloc: Allocator, req: types.SearchRequest) !?[]u8 {
    var must = std.ArrayListUnmanaged([]u8).empty;
    defer {
        for (must.items) |item| alloc.free(item);
        must.deinit(alloc);
    }
    var must_not = std.ArrayListUnmanaged([]u8).empty;
    defer {
        for (must_not.items) |item| alloc.free(item);
        must_not.deinit(alloc);
    }

    if (try structuredQueryJsonAlloc(alloc, req.query)) |query_json| {
        try must.append(alloc, query_json);
    }
    if (req.filter_query_json.len > 0) {
        try must.append(alloc, try alloc.dupe(u8, req.filter_query_json));
    }
    if (req.exclusion_query_json.len > 0) {
        try must_not.append(alloc, try alloc.dupe(u8, req.exclusion_query_json));
    }

    if (must.items.len == 0 and must_not.items.len == 0) return null;
    if (must.items.len == 1 and must_not.items.len == 0) return try alloc.dupe(u8, must.items[0]);

    var out = std.ArrayListUnmanaged(u8).empty;
    errdefer out.deinit(alloc);
    try out.appendSlice(alloc, "{\"bool\":{");
    var wrote_any = false;
    if (must.items.len > 0) {
        try out.appendSlice(alloc, "\"must\":[");
        for (must.items, 0..) |item, i| {
            if (i > 0) try out.append(alloc, ',');
            try out.appendSlice(alloc, item);
        }
        try out.append(alloc, ']');
        wrote_any = true;
    }
    if (must_not.items.len > 0) {
        if (wrote_any) try out.append(alloc, ',');
        try out.appendSlice(alloc, "\"must_not\":[");
        for (must_not.items, 0..) |item, i| {
            if (i > 0) try out.append(alloc, ',');
            try out.appendSlice(alloc, item);
        }
        try out.append(alloc, ']');
    }
    try out.appendSlice(alloc, "}}");
    return try out.toOwnedSlice(alloc);
}

fn structuredQueryJsonAlloc(alloc: Allocator, query: types.Query) !?[]u8 {
    return switch (query) {
        .match_all, .match_none => null,
        .doc_id,
        .numeric_range,
        .date_range,
        .bool_field,
        .geo_distance,
        .geo_bbox,
        .term_range,
        .ip_range,
        .geo_shape,
        => try jsonStringifyAlloc(alloc, query),
        else => null,
    };
}

fn jsonStringifyAlloc(alloc: Allocator, value: anytype) ![]u8 {
    return try std.fmt.allocPrint(alloc, "{f}", .{std.json.fmt(value, .{})});
}

fn textIndexEntryCallback(
    ctx: ?*anyopaque,
    index_name: ?[]const u8,
) anyerror!?*index_manager_mod.IndexManager.TextIndex {
    const core: *db_core.DBCore = @ptrCast(@alignCast(ctx orelse return error.InvalidArgument));
    return core.textIndexEntry(index_name);
}
