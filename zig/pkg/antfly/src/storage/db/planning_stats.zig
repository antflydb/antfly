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
const query_search = @import("query/search_exec.zig");
const distributed_stats = @import("../../search/distributed_stats.zig");
const types = @import("types.zig");

const preflight_structured_filter_exact_count_limit: u64 = 1024;
const bool_filter_clause_keys = [_][]const u8{ "must", "filter", "should", "must_not" };

pub const PlanningStatsSummary = query_search.RuntimePreflightSummary;

pub const PlanningStatsProvider = struct {
    ptr: *anyopaque,
    vtable: *const VTable,

    pub const VTable = struct {
        collect_search_request_stats: *const fn (
            ptr: *anyopaque,
            alloc: std.mem.Allocator,
            req: types.SearchRequest,
            max_work: u32,
        ) anyerror!PlanningStatsSummary,
    };

    pub fn collectSearchRequestStats(
        self: @This(),
        alloc: std.mem.Allocator,
        req: types.SearchRequest,
        max_work: u32,
    ) !PlanningStatsSummary {
        return try self.vtable.collect_search_request_stats(self.ptr, alloc, req, max_work);
    }

    pub fn init(
        ptr: *anyopaque,
        comptime collect_search_request_stats: *const fn (
            ptr: *anyopaque,
            alloc: std.mem.Allocator,
            req: types.SearchRequest,
            max_work: u32,
        ) anyerror!PlanningStatsSummary,
    ) @This() {
        return .{
            .ptr = ptr,
            .vtable = &.{
                .collect_search_request_stats = collect_search_request_stats,
            },
        };
    }
};

pub const PlanningStatsCollector = struct {
    pub const StructuredFilterSampleEstimate = struct {
        doc_count_estimate: u64,
        sample_size: u32,
    };

    ptr: *anyopaque,
    vtable: *const VTable,

    pub const VTable = struct {
        resolve_text_index_estimate: *const fn (
            ptr: *anyopaque,
            alloc: std.mem.Allocator,
            index_name: ?[]const u8,
            req: types.SearchRequest,
        ) anyerror!?query_search.TextIndexEstimate,
        resolve_dense_index_estimate: *const fn (
            ptr: *anyopaque,
            alloc: std.mem.Allocator,
            index_name: ?[]const u8,
        ) anyerror!?query_search.EmbeddingIndexEstimate,
        resolve_sparse_index_estimate: *const fn (
            ptr: *anyopaque,
            alloc: std.mem.Allocator,
            index_name: ?[]const u8,
        ) anyerror!?query_search.EmbeddingIndexEstimate,
        resolve_graph_index_estimate: *const fn (
            ptr: *anyopaque,
            alloc: std.mem.Allocator,
            index_name: []const u8,
        ) anyerror!?query_search.GraphIndexEstimate,
        collect_text_query_stats: *const fn (
            ptr: *anyopaque,
            alloc: std.mem.Allocator,
            req: types.SearchRequest,
        ) anyerror![]const distributed_stats.TextFieldStats,
        append_dense_query_cost: *const fn (
            ptr: *anyopaque,
            summary: *PlanningStatsSummary,
            req: types.SearchRequest,
            index_name: ?[]const u8,
            dense: types.DenseKnnQuery,
        ) anyerror!void,
        estimate_structured_filter_sample: *const fn (
            ptr: *anyopaque,
            alloc: std.mem.Allocator,
            req: types.SearchRequest,
            sample_size: u32,
            corpus_doc_count: u64,
        ) anyerror!?StructuredFilterSampleEstimate,
        search_request: *const fn (
            ptr: *anyopaque,
            alloc: std.mem.Allocator,
            req: types.SearchRequest,
        ) anyerror!types.SearchResult,
    };

    pub fn resolveTextIndexEstimate(
        self: @This(),
        alloc: std.mem.Allocator,
        index_name: ?[]const u8,
        req: types.SearchRequest,
    ) !?query_search.TextIndexEstimate {
        return try self.vtable.resolve_text_index_estimate(self.ptr, alloc, index_name, req);
    }

    pub fn resolveDenseIndexEstimate(
        self: @This(),
        alloc: std.mem.Allocator,
        index_name: ?[]const u8,
    ) !?query_search.EmbeddingIndexEstimate {
        return try self.vtable.resolve_dense_index_estimate(self.ptr, alloc, index_name);
    }

    pub fn resolveSparseIndexEstimate(
        self: @This(),
        alloc: std.mem.Allocator,
        index_name: ?[]const u8,
    ) !?query_search.EmbeddingIndexEstimate {
        return try self.vtable.resolve_sparse_index_estimate(self.ptr, alloc, index_name);
    }

    pub fn resolveGraphIndexEstimate(
        self: @This(),
        alloc: std.mem.Allocator,
        index_name: []const u8,
    ) !?query_search.GraphIndexEstimate {
        return try self.vtable.resolve_graph_index_estimate(self.ptr, alloc, index_name);
    }

    pub fn collectTextQueryStats(
        self: @This(),
        alloc: std.mem.Allocator,
        req: types.SearchRequest,
    ) ![]const distributed_stats.TextFieldStats {
        return try self.vtable.collect_text_query_stats(self.ptr, alloc, req);
    }

    pub fn appendDenseQueryCost(
        self: @This(),
        summary: *PlanningStatsSummary,
        req: types.SearchRequest,
        index_name: ?[]const u8,
        dense: types.DenseKnnQuery,
    ) !void {
        try self.vtable.append_dense_query_cost(self.ptr, summary, req, index_name, dense);
    }

    pub fn estimateStructuredFilterSample(
        self: @This(),
        alloc: std.mem.Allocator,
        req: types.SearchRequest,
        sample_size: u32,
        corpus_doc_count: u64,
    ) !?StructuredFilterSampleEstimate {
        return try self.vtable.estimate_structured_filter_sample(self.ptr, alloc, req, sample_size, corpus_doc_count);
    }

    pub fn searchRequest(
        self: @This(),
        alloc: std.mem.Allocator,
        req: types.SearchRequest,
    ) !types.SearchResult {
        return try self.vtable.search_request(self.ptr, alloc, req);
    }
};

pub fn collectSearchRequestStatsAlloc(
    alloc: std.mem.Allocator,
    collector: PlanningStatsCollector,
    req: types.SearchRequest,
    max_work: u32,
) !PlanningStatsSummary {
    var summary = try query_search.preflightSearchRequestAlloc(alloc, req);
    errdefer summary.deinit(alloc);
    summary.shard_count = 1;
    summary.text_indexes = try collectTextIndexEstimatesAlloc(alloc, collector, req);
    summary.embedding_indexes = try collectEmbeddingIndexEstimatesAlloc(alloc, collector, req);
    summary.graph_indexes = try collectGraphIndexEstimatesAlloc(alloc, collector, req);
    summary.text_query_stats = try collector.collectTextQueryStats(alloc, req);
    try fillPreflightStructuredFilterEstimate(alloc, &summary, req);
    try fillPreflightPositiveIdUpperBound(alloc, &summary, req);
    try fillPreflightStructuredFilterCountEstimate(collector, alloc, &summary, req, max_work);
    fillPreflightCostEstimate(&summary, req);
    try fillPreflightDenseCostEstimate(collector, &summary, req);
    query_search.deriveEstimateFields(&summary);
    return summary;
}

fn preflightRequestNeedsPrimaryTextIndex(req: types.SearchRequest) bool {
    return req.full_text != null or
        req.filter_query_json.len > 0 or
        req.exclusion_query_json.len > 0 or
        (!query_search.isDefaultMatchAll(req.query) and query_search.isTextQuery(req.query));
}

fn collectTextIndexEstimatesAlloc(
    alloc: std.mem.Allocator,
    collector: PlanningStatsCollector,
    req: types.SearchRequest,
) ![]const query_search.TextIndexEstimate {
    var items = std.ArrayListUnmanaged(query_search.TextIndexEstimate).empty;
    errdefer {
        for (items.items) |*item| item.deinit(alloc);
        items.deinit(alloc);
    }

    if (preflightRequestNeedsPrimaryTextIndex(req)) {
        var estimate = try collector.resolveTextIndexEstimate(alloc, req.index_name, req) orelse return error.IndexNotFound;
        errdefer estimate.deinit(alloc);
        try appendUniqueTextIndexEstimate(alloc, &items, &estimate);
    }
    for (req.full_text_queries) |query| {
        var estimate = try collector.resolveTextIndexEstimate(alloc, query.index_name, req) orelse return error.IndexNotFound;
        errdefer estimate.deinit(alloc);
        try appendUniqueTextIndexEstimate(alloc, &items, &estimate);
    }
    return if (items.items.len == 0) &.{} else try items.toOwnedSlice(alloc);
}

fn appendUniqueTextIndexEstimate(
    alloc: std.mem.Allocator,
    items: *std.ArrayListUnmanaged(query_search.TextIndexEstimate),
    estimate: *query_search.TextIndexEstimate,
) !void {
    for (items.items) |existing| {
        if (std.mem.eql(u8, existing.name, estimate.name)) {
            estimate.deinit(alloc);
            return;
        }
    }
    try items.append(alloc, estimate.*);
    estimate.* = undefined;
}

fn collectEmbeddingIndexEstimatesAlloc(
    alloc: std.mem.Allocator,
    collector: PlanningStatsCollector,
    req: types.SearchRequest,
) ![]const query_search.EmbeddingIndexEstimate {
    var items = std.ArrayListUnmanaged(query_search.EmbeddingIndexEstimate).empty;
    errdefer {
        for (items.items) |*item| item.deinit(alloc);
        items.deinit(alloc);
    }

    if (req.dense != null) {
        var estimate = try collector.resolveDenseIndexEstimate(alloc, req.index_name) orelse return error.IndexNotFound;
        errdefer estimate.deinit(alloc);
        try appendUniqueEmbeddingIndexEstimate(alloc, &items, &estimate);
    }
    for (req.dense_queries) |query| {
        var estimate = try collector.resolveDenseIndexEstimate(alloc, query.index_name) orelse return error.IndexNotFound;
        errdefer estimate.deinit(alloc);
        try appendUniqueEmbeddingIndexEstimate(alloc, &items, &estimate);
    }
    if (req.sparse != null) {
        var estimate = try collector.resolveSparseIndexEstimate(alloc, req.index_name) orelse return error.IndexNotFound;
        errdefer estimate.deinit(alloc);
        try appendUniqueEmbeddingIndexEstimate(alloc, &items, &estimate);
    }
    for (req.sparse_queries) |query| {
        var estimate = try collector.resolveSparseIndexEstimate(alloc, query.index_name) orelse return error.IndexNotFound;
        errdefer estimate.deinit(alloc);
        try appendUniqueEmbeddingIndexEstimate(alloc, &items, &estimate);
    }
    return if (items.items.len == 0) &.{} else try items.toOwnedSlice(alloc);
}

fn appendUniqueEmbeddingIndexEstimate(
    alloc: std.mem.Allocator,
    items: *std.ArrayListUnmanaged(query_search.EmbeddingIndexEstimate),
    estimate: *query_search.EmbeddingIndexEstimate,
) !void {
    for (items.items) |existing| {
        if (std.mem.eql(u8, existing.name, estimate.name) and existing.sparse == estimate.sparse) {
            estimate.deinit(alloc);
            return;
        }
    }
    try items.append(alloc, estimate.*);
    estimate.* = undefined;
}

fn collectGraphIndexEstimatesAlloc(
    alloc: std.mem.Allocator,
    collector: PlanningStatsCollector,
    req: types.SearchRequest,
) ![]const query_search.GraphIndexEstimate {
    var items = std.ArrayListUnmanaged(query_search.GraphIndexEstimate).empty;
    errdefer {
        for (items.items) |*item| item.deinit(alloc);
        items.deinit(alloc);
    }

    switch (req.query) {
        .graph => |graph_query| {
            var estimate = try collector.resolveGraphIndexEstimate(alloc, graph_query.index_name) orelse return error.IndexNotFound;
            errdefer estimate.deinit(alloc);
            try appendUniqueGraphIndexEstimate(alloc, &items, &estimate);
        },
        else => {},
    }
    for (req.graph_queries) |graph_query| {
        var estimate = try collector.resolveGraphIndexEstimate(alloc, graph_query.query.index_name) orelse return error.IndexNotFound;
        errdefer estimate.deinit(alloc);
        try appendUniqueGraphIndexEstimate(alloc, &items, &estimate);
    }
    return if (items.items.len == 0) &.{} else try items.toOwnedSlice(alloc);
}

fn appendUniqueGraphIndexEstimate(
    alloc: std.mem.Allocator,
    items: *std.ArrayListUnmanaged(query_search.GraphIndexEstimate),
    estimate: *query_search.GraphIndexEstimate,
) !void {
    for (items.items) |existing| {
        if (std.mem.eql(u8, existing.name, estimate.name)) {
            estimate.deinit(alloc);
            return;
        }
    }
    try items.append(alloc, estimate.*);
    estimate.* = undefined;
}

fn fillPreflightDenseCostEstimate(
    collector: PlanningStatsCollector,
    summary: *PlanningStatsSummary,
    req: types.SearchRequest,
) !void {
    if (req.dense) |dense| try collector.appendDenseQueryCost(summary, req, req.index_name, dense);
    for (req.dense_queries) |query| {
        try collector.appendDenseQueryCost(summary, req, query.index_name, query.query);
    }
}

fn fillPreflightStructuredFilterEstimate(
    alloc: std.mem.Allocator,
    summary: *PlanningStatsSummary,
    req: types.SearchRequest,
) !void {
    summary.filter_id_count = saturatingCastU32(req.filter_ids.len);
    summary.exclude_id_count = saturatingCastU32(req.exclude_ids.len);
    appendPreflightStructuredQueryEstimate(summary, req.query);
    if (req.filter_query_json.len > 0) try appendPreflightStructuredFilterJsonEstimate(alloc, summary, req.filter_query_json);
    if (req.exclusion_query_json.len > 0) try appendPreflightStructuredFilterJsonEstimate(alloc, summary, req.exclusion_query_json);
}

fn appendPreflightStructuredQueryEstimate(
    summary: *PlanningStatsSummary,
    query: types.Query,
) void {
    switch (query) {
        .doc_id => |doc_id| summary.doc_id_value_count +|= saturatingCastU32(doc_id.ids.len),
        .numeric_range => summary.numeric_range_clause_count +|= 1,
        .term_range => summary.term_range_clause_count +|= 1,
        .ip_range => summary.ip_range_clause_count +|= 1,
        .bool_field => summary.bool_field_clause_count +|= 1,
        .geo_distance, .geo_bbox, .geo_shape => summary.geo_filter_clause_count +|= 1,
        else => {},
    }
}

fn appendPreflightStructuredFilterJsonEstimate(
    alloc: std.mem.Allocator,
    summary: *PlanningStatsSummary,
    filter_query_json: []const u8,
) !void {
    var parsed = std.json.parseFromSlice(std.json.Value, alloc, filter_query_json, .{}) catch return;
    defer parsed.deinit();
    try appendPreflightStructuredFilterValueEstimate(summary, parsed.value);
}

fn appendPreflightStructuredFilterValueEstimate(
    summary: *PlanningStatsSummary,
    value: std.json.Value,
) !void {
    const object = switch (value) {
        .object => |object| object,
        else => return,
    };

    if (object.get("doc_id")) |doc_id_value| {
        switch (doc_id_value) {
            .array => |array| summary.doc_id_value_count +|= saturatingCastU32(array.items.len),
            else => {},
        }
    }
    if (object.get("numeric_range") != null) summary.numeric_range_clause_count +|= 1;
    if (object.get("term_range") != null) summary.term_range_clause_count +|= 1;
    if (object.get("ip_range") != null) summary.ip_range_clause_count +|= 1;

    if (object.get("bool")) |bool_value| {
        const bool_object = switch (bool_value) {
            .object => |inner| inner,
            else => return,
        };
        for (bool_filter_clause_keys) |key| {
            const items = bool_object.get(key) orelse continue;
            const array = switch (items) {
                .array => |array| array,
                else => continue,
            };
            for (array.items) |item| {
                try appendPreflightStructuredFilterValueEstimate(summary, item);
            }
        }
    }
}

fn saturatingCastU32(len: usize) u32 {
    return @intCast(@min(len, @as(usize, std.math.maxInt(u32))));
}

fn fillPreflightPositiveIdUpperBound(
    alloc: std.mem.Allocator,
    summary: *PlanningStatsSummary,
    req: types.SearchRequest,
) !void {
    var bound: ?u32 = null;
    if (req.filter_ids.len > 0) {
        bound = saturatingCastU32(req.filter_ids.len);
    }
    if (positiveIdUpperBoundForQuery(req.query)) |query_bound| {
        bound = if (bound) |existing| @min(existing, query_bound) else query_bound;
    }
    if (req.filter_query_json.len > 0) {
        if (try positiveIdUpperBoundForFilterJson(alloc, req.filter_query_json)) |filter_bound| {
            bound = if (bound) |existing| @min(existing, filter_bound) else filter_bound;
        }
    }
    summary.positive_id_result_upper_bound = bound;
}

fn fillPreflightStructuredFilterCountEstimate(
    collector: PlanningStatsCollector,
    alloc: std.mem.Allocator,
    summary: *PlanningStatsSummary,
    req: types.SearchRequest,
    max_work: u32,
) !void {
    if (!preflightRequestHasExactStructuredFilterCandidate(req)) return;
    const budget_limit: u64 = if (max_work > 0) max_work else preflight_structured_filter_exact_count_limit;
    const corpus_doc_count = preflightStructuredFilterCorpusDocCount(summary) orelse return;

    var exact_req = buildPreflightStructuredFilterProbeRequest(req);
    if (corpus_doc_count > budget_limit) {
        summary.structured_filter_count_budget_limit = budget_limit;
        exact_req.count_only = false;
        exact_req.limit = @intCast(@min(budget_limit, @as(u64, std.math.maxInt(u32))));
        var probe_result = try collector.searchRequest(alloc, exact_req);
        defer probe_result.deinit();
        switch (probe_result.total_hits_relation) {
            .exact => {
                summary.structured_filter_doc_count_estimate = probe_result.total_hits;
                summary.structured_filter_count_exact = true;
            },
            .gte => summary.structured_filter_doc_count_lower_bound = probe_result.total_hits,
        }
        if (try collector.estimateStructuredFilterSample(
            alloc,
            req,
            @intCast(@min(budget_limit, @as(u64, std.math.maxInt(u32)))),
            corpus_doc_count,
        )) |sample| {
            summary.structured_filter_doc_count_sample_estimate = sample.doc_count_estimate;
            summary.structured_filter_count_sample_size = sample.sample_size;
        }
        return;
    }

    exact_req.count_only = true;
    exact_req.limit = 0;
    var result = try collector.searchRequest(alloc, exact_req);
    defer result.deinit();
    summary.structured_filter_doc_count_estimate = result.total_hits;
    summary.structured_filter_count_exact = true;
}

fn buildPreflightStructuredFilterProbeRequest(req: types.SearchRequest) types.SearchRequest {
    var probe_req = req;
    probe_req.query = switch (req.query) {
        .doc_id,
        .numeric_range,
        .date_range,
        .bool_field,
        .geo_distance,
        .geo_bbox,
        .term_range,
        .ip_range,
        .geo_shape,
        => req.query,
        else => .{ .match_all = {} },
    };
    probe_req.offset = 0;
    probe_req.include_stored = false;
    probe_req.profile = false;
    probe_req.aggregations_json = "";
    probe_req.fields = &.{};
    probe_req.include_all_fields = true;
    probe_req.defer_stored_projection = false;
    probe_req.reranker = null;
    probe_req.reranker_query_text = "";
    probe_req.pruner = null;
    probe_req.expand_strategy = null;
    probe_req.return_mode = .parent;
    probe_req.max_chunks_per_parent = 0;
    probe_req.full_text = null;
    probe_req.dense = null;
    probe_req.sparse = null;
    probe_req.full_text_queries = &.{};
    probe_req.dense_queries = &.{};
    probe_req.sparse_queries = &.{};
    probe_req.graph_queries = &.{};
    probe_req.merge_config = null;
    probe_req.search_effort = null;
    probe_req.filter_prefix = "";
    probe_req.distance_over = null;
    probe_req.distance_under = null;
    probe_req.distributed_text_stats = &.{};
    return probe_req;
}

fn preflightRequestHasExactStructuredFilterCandidate(req: types.SearchRequest) bool {
    if (req.filter_ids.len > 0 or req.exclude_ids.len > 0) return true;
    if (preflightQuerySupportsExactStructuredFilterCount(req.query)) return true;
    if (req.filter_query_json.len > 0 and preflightFilterJsonSupportsExactStructuredFilterCount(req.filter_query_json)) return true;
    if (req.exclusion_query_json.len > 0 and preflightFilterJsonSupportsExactStructuredFilterCount(req.exclusion_query_json)) return true;
    return false;
}

fn preflightQuerySupportsExactStructuredFilterCount(query: types.Query) bool {
    return switch (query) {
        .doc_id,
        .numeric_range,
        .date_range,
        .bool_field,
        .geo_distance,
        .geo_bbox,
        .term_range,
        .ip_range,
        .geo_shape,
        => true,
        else => false,
    };
}

fn preflightFilterJsonSupportsExactStructuredFilterCount(filter_query_json: []const u8) bool {
    var arena_impl = std.heap.ArenaAllocator.init(std.heap.page_allocator);
    defer arena_impl.deinit();
    const alloc = arena_impl.allocator();
    const parsed = std.json.parseFromSlice(std.json.Value, alloc, filter_query_json, .{}) catch return false;
    return preflightFilterValueSupportsExactStructuredFilterCount(parsed.value);
}

fn preflightFilterValueSupportsExactStructuredFilterCount(value: std.json.Value) bool {
    const object = switch (value) {
        .object => |object| object,
        else => return false,
    };

    if (object.get("bool")) |bool_value| {
        const bool_object = switch (bool_value) {
            .object => |inner| inner,
            else => return false,
        };
        for (bool_filter_clause_keys) |key| {
            const items = bool_object.get(key) orelse continue;
            const array = switch (items) {
                .array => |array| array,
                else => return false,
            };
            for (array.items) |item| {
                if (!preflightFilterValueSupportsExactStructuredFilterCount(item)) return false;
            }
        }
        return true;
    }

    return object.get("doc_id") != null or
        object.get("numeric_range") != null or
        object.get("date_range") != null or
        object.get("bool_field") != null or
        object.get("geo_distance") != null or
        object.get("geo_bbox") != null or
        object.get("term_range") != null or
        object.get("ip_range") != null or
        object.get("geo_shape") != null;
}

fn preflightStructuredFilterCorpusDocCount(summary: *const PlanningStatsSummary) ?u64 {
    for (summary.text_indexes) |item| return item.doc_count;
    return null;
}

fn positiveIdUpperBoundForQuery(query: types.Query) ?u32 {
    return switch (query) {
        .doc_id => |doc_id| saturatingCastU32(doc_id.ids.len),
        else => null,
    };
}

fn positiveIdUpperBoundForFilterJson(
    alloc: std.mem.Allocator,
    filter_query_json: []const u8,
) !?u32 {
    var parsed = std.json.parseFromSlice(std.json.Value, alloc, filter_query_json, .{}) catch return null;
    defer parsed.deinit();
    return positiveIdUpperBoundForFilterValue(parsed.value);
}

fn positiveIdUpperBoundForFilterValue(value: std.json.Value) ?u32 {
    const object = switch (value) {
        .object => |object| object,
        else => return null,
    };

    if (object.get("doc_id")) |doc_id_value| {
        return switch (doc_id_value) {
            .array => |array| saturatingCastU32(array.items.len),
            else => null,
        };
    }
    if (object.get("bool")) |bool_value| {
        const bool_object = switch (bool_value) {
            .object => |inner| inner,
            else => return null,
        };

        var must_bound: ?u32 = null;
        if (bool_object.get("must")) |must_value| {
            if (positiveIdUpperBoundForFilterArray(must_value, .all)) |bound| {
                must_bound = bound;
            }
        }
        if (bool_object.get("filter")) |filter_value| {
            if (positiveIdUpperBoundForFilterArray(filter_value, .all)) |bound| {
                must_bound = if (must_bound) |existing| @min(existing, bound) else bound;
            }
        }

        var should_bound: ?u32 = null;
        if (bool_object.get("should")) |should_value| {
            if (positiveIdUpperBoundForFilterArray(should_value, .any)) |bound| {
                should_bound = bound;
            }
        }

        if (must_bound) |must_value| {
            if (should_bound) |should_value| return @min(must_value, should_value);
            return must_value;
        }
        return should_bound;
    }
    return null;
}

const FilterArrayMode = enum { all, any };

fn positiveIdUpperBoundForFilterArray(value: std.json.Value, mode: FilterArrayMode) ?u32 {
    const array = switch (value) {
        .array => |array| array,
        else => return null,
    };
    if (array.items.len == 0) return null;

    var have_bound = false;
    var result: u32 = switch (mode) {
        .all => 0,
        .any => 0,
    };

    for (array.items) |item| {
        const item_bound = positiveIdUpperBoundForFilterValue(item) orelse switch (mode) {
            .all => continue,
            .any => return null,
        };
        if (!have_bound) {
            result = item_bound;
            have_bound = true;
            continue;
        }
        switch (mode) {
            .all => result = @min(result, item_bound),
            .any => result +|= item_bound,
        }
    }
    return if (have_bound) result else null;
}

fn fillPreflightCostEstimate(
    summary: *PlanningStatsSummary,
    req: types.SearchRequest,
) void {
    const shard_result_window = computePreflightShardResultWindow(req);
    summary.shard_result_window = shard_result_window;
    summary.shard_result_window_total = shard_result_window;
    summary.stored_projection_doc_upper_bound_total = if (!req.count_only and req.include_stored) shard_result_window else 0;
    summary.rerank_doc_upper_bound = computePreflightRerankDocUpperBound(req);
    summary.aggregation_may_scan_full_results = req.aggregations_json.len > 0;
}

fn computePreflightShardResultWindow(req: types.SearchRequest) u32 {
    return req.limit +| req.offset;
}

fn computePreflightRerankDocUpperBound(req: types.SearchRequest) u32 {
    const cfg = req.reranker orelse return 0;
    return if (cfg.top_n) |top_n|
        @min(req.limit, top_n)
    else
        req.limit;
}

test {
    _ = PlanningStatsSummary;
    _ = PlanningStatsProvider;
    _ = PlanningStatsCollector;
}

test "exact structured filter counts tighten derived estimates without budget-limited metadata" {
    const alloc = std.testing.allocator;

    const MockCollector = struct {
        fn resolveTextIndexEstimate(
            _: *anyopaque,
            test_alloc: std.mem.Allocator,
            index_name: ?[]const u8,
            _: types.SearchRequest,
        ) !?query_search.TextIndexEstimate {
            return .{
                .name = try test_alloc.dupe(u8, index_name orelse "ft_v1"),
                .doc_count = 0,
            };
        }

        fn resolveEmbeddingIndexEstimate(
            _: *anyopaque,
            _: std.mem.Allocator,
            _: ?[]const u8,
        ) !?query_search.EmbeddingIndexEstimate {
            return null;
        }

        fn resolveGraphIndexEstimate(
            _: *anyopaque,
            _: std.mem.Allocator,
            _: []const u8,
        ) !?query_search.GraphIndexEstimate {
            return null;
        }

        fn collectTextQueryStats(
            _: *anyopaque,
            _: std.mem.Allocator,
            _: types.SearchRequest,
        ) ![]const distributed_stats.TextFieldStats {
            return &.{};
        }

        fn appendDenseQueryCost(
            _: *anyopaque,
            _: *PlanningStatsSummary,
            _: types.SearchRequest,
            _: ?[]const u8,
            _: types.DenseKnnQuery,
        ) !void {}

        fn estimateStructuredFilterSample(
            _: *anyopaque,
            _: std.mem.Allocator,
            _: types.SearchRequest,
            _: u32,
            _: u64,
        ) !?PlanningStatsCollector.StructuredFilterSampleEstimate {
            return null;
        }

        fn searchRequest(
            _: *anyopaque,
            test_alloc: std.mem.Allocator,
            _: types.SearchRequest,
        ) !types.SearchResult {
            return try query_search.emptySearchResult(test_alloc);
        }
    };

    const collector = PlanningStatsCollector{
        .ptr = undefined,
        .vtable = &.{
            .resolve_text_index_estimate = MockCollector.resolveTextIndexEstimate,
            .resolve_dense_index_estimate = MockCollector.resolveEmbeddingIndexEstimate,
            .resolve_sparse_index_estimate = MockCollector.resolveEmbeddingIndexEstimate,
            .resolve_graph_index_estimate = MockCollector.resolveGraphIndexEstimate,
            .collect_text_query_stats = MockCollector.collectTextQueryStats,
            .append_dense_query_cost = MockCollector.appendDenseQueryCost,
            .estimate_structured_filter_sample = MockCollector.estimateStructuredFilterSample,
            .search_request = MockCollector.searchRequest,
        },
    };

    var summary = try collectSearchRequestStatsAlloc(alloc, collector, .{
        .index_name = "ft_v1",
        .query = .{ .doc_id = .{ .ids = &.{ "doc:a", "doc:b" } } },
        .filter_ids = &.{ 1, 2, 3 },
        .exclude_ids = &.{4},
        .filter_query_json = "{\"bool\":{\"must\":[{\"numeric_range\":{\"field\":\"score\",\"min\":1}},{\"doc_id\":[\"doc:c\"]}]}}",
        .exclusion_query_json = "{\"bool\":{\"must_not\":[{\"term_range\":{\"field\":\"tag\",\"min\":\"a\",\"max\":\"m\"}},{\"ip_range\":{\"field\":\"ip\",\"cidr\":\"10.0.0.0/8\"}}]}}",
    }, 0);
    defer summary.deinit(alloc);

    try std.testing.expectEqual(@as(?u32, 1), summary.positive_id_result_upper_bound);
    try std.testing.expectEqual(@as(?u64, 0), summary.structured_filter_doc_count_estimate);
    try std.testing.expect(summary.structured_filter_count_exact);
    try std.testing.expectEqual(@as(?u64, null), summary.structured_filter_count_budget_limit);
    try std.testing.expectEqual(@as(?u32, 0), summary.result_doc_upper_bound);
    try std.testing.expectEqual(@as(?u32, 0), summary.result_doc_estimate);
    if (summary.selectivity_upper_bound_ratio) |ratio| {
        try std.testing.expectApproxEqAbs(@as(f32, 0.0), ratio, 0.0001);
    }
}

test "planning stats treats bool filter clauses as required structured filters" {
    const alloc = std.testing.allocator;

    const MockCollector = struct {
        fn resolveTextIndexEstimate(
            _: *anyopaque,
            test_alloc: std.mem.Allocator,
            _: ?[]const u8,
            _: types.SearchRequest,
        ) !?query_search.TextIndexEstimate {
            return .{
                .name = try test_alloc.dupe(u8, "ft_v1"),
                .doc_count = 100,
            };
        }

        fn resolveEmbeddingIndexEstimate(
            _: *anyopaque,
            _: std.mem.Allocator,
            _: ?[]const u8,
        ) !?query_search.EmbeddingIndexEstimate {
            return null;
        }

        fn resolveGraphIndexEstimate(
            _: *anyopaque,
            _: std.mem.Allocator,
            _: []const u8,
        ) !?query_search.GraphIndexEstimate {
            return null;
        }

        fn collectTextQueryStats(
            _: *anyopaque,
            _: std.mem.Allocator,
            _: types.SearchRequest,
        ) ![]const distributed_stats.TextFieldStats {
            return &.{};
        }

        fn appendDenseQueryCost(
            _: *anyopaque,
            _: *PlanningStatsSummary,
            _: types.SearchRequest,
            _: ?[]const u8,
            _: types.DenseKnnQuery,
        ) !void {}

        fn estimateStructuredFilterSample(
            _: *anyopaque,
            _: std.mem.Allocator,
            _: types.SearchRequest,
            _: u32,
            _: u64,
        ) !?PlanningStatsCollector.StructuredFilterSampleEstimate {
            return null;
        }

        fn searchRequest(
            _: *anyopaque,
            test_alloc: std.mem.Allocator,
            _: types.SearchRequest,
        ) !types.SearchResult {
            return try query_search.emptySearchResult(test_alloc);
        }
    };

    const collector = PlanningStatsCollector{
        .ptr = undefined,
        .vtable = &.{
            .resolve_text_index_estimate = MockCollector.resolveTextIndexEstimate,
            .resolve_dense_index_estimate = MockCollector.resolveEmbeddingIndexEstimate,
            .resolve_sparse_index_estimate = MockCollector.resolveEmbeddingIndexEstimate,
            .resolve_graph_index_estimate = MockCollector.resolveGraphIndexEstimate,
            .collect_text_query_stats = MockCollector.collectTextQueryStats,
            .append_dense_query_cost = MockCollector.appendDenseQueryCost,
            .estimate_structured_filter_sample = MockCollector.estimateStructuredFilterSample,
            .search_request = MockCollector.searchRequest,
        },
    };

    var summary = try collectSearchRequestStatsAlloc(alloc, collector, .{
        .index_name = "ft_v1",
        .filter_query_json = "{\"bool\":{\"must\":[{\"doc_id\":[\"doc:a\",\"doc:b\",\"doc:c\"]}],\"filter\":[{\"doc_id\":[\"doc:a\"]},{\"numeric_range\":{\"field\":\"score\",\"min\":1}}]}}",
    }, 0);
    defer summary.deinit(alloc);

    try std.testing.expectEqual(@as(u32, 4), summary.doc_id_value_count);
    try std.testing.expectEqual(@as(u32, 1), summary.numeric_range_clause_count);
    try std.testing.expectEqual(@as(?u32, 1), summary.positive_id_result_upper_bound);
    try std.testing.expect(summary.structured_filter_count_exact);
}
