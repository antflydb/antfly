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

//! Compiler-boundary-safe runtime preflight result types.

const std = @import("std");
const Allocator = std.mem.Allocator;
const distributed_stats_mod = @import("../../search/distributed_stats.zig");
const types = @import("types.zig");

pub const RuntimePreflight = struct {
    has_full_text_results: bool = false,
    embedding_result_names: []const []const u8 = &.{},
    graph_queries: []const types.NamedGraphQuery = &.{},
};

pub const SortRejectionDiagnostic = struct {
    field: []const u8 = "",
    reason: []const u8 = "unsupported_exact_sort",
    detail: []const u8 = "unsupported_exact_sort",
};

threadlocal var last_sort_rejection_diagnostic: ?SortRejectionDiagnostic = null;
threadlocal var last_sort_rejection_field_buf: [256]u8 = undefined;

pub fn resetLastSortRejectionDiagnostic() void {
    last_sort_rejection_diagnostic = null;
}

pub fn takeLastSortRejectionDiagnostic() ?SortRejectionDiagnostic {
    const diagnostic = last_sort_rejection_diagnostic;
    last_sort_rejection_diagnostic = null;
    return diagnostic;
}

pub fn peekLastSortRejectionDiagnostic() ?SortRejectionDiagnostic {
    return last_sort_rejection_diagnostic;
}

pub fn recordSortRejectionDiagnostic(field: []const u8, reason: []const u8, detail: []const u8) void {
    const field_len = @min(field.len, last_sort_rejection_field_buf.len);
    if (field_len > 0) @memcpy(last_sort_rejection_field_buf[0..field_len], field[0..field_len]);
    last_sort_rejection_diagnostic = .{
        .field = last_sort_rejection_field_buf[0..field_len],
        .reason = reason,
        .detail = detail,
    };
}

pub fn recordSortRejectionDiagnosticForTesting(field: []const u8, reason: []const u8, detail: []const u8) void {
    recordSortRejectionDiagnostic(field, reason, detail);
}

pub fn textQueryIsScoreBearing(query: types.TextQuery) bool {
    return switch (query) {
        .phrase,
        .multi_phrase,
        .term,
        .match,
        .multi_match_bool_prefix,
        .match_phrase,
        .fuzzy,
        .prefix,
        .wildcard,
        .regexp,
        => true,
        .bool_query => |bool_query| textBoolQueryIsScoreBearing(bool_query),
        .match_none,
        .match_all,
        .numeric_range,
        .date_range,
        .term_range,
        .doc_id,
        .bool_field,
        .geo_distance,
        .geo_bbox,
        .ip_range,
        .geo_shape,
        => false,
    };
}

fn textBoolQueryIsScoreBearing(query: types.TextBoolQuery) bool {
    for (query.must) |child| {
        if (textQueryIsScoreBearing(child)) return true;
    }
    for (query.should) |child| {
        if (textQueryIsScoreBearing(child)) return true;
    }
    return false;
}

pub fn searchRequestHasScoreBearingTextSource(req: types.SearchRequest) bool {
    if (req.full_text) |query| {
        if (textQueryIsScoreBearing(query)) return true;
    }
    for (req.full_text_queries) |query| {
        if (textQueryIsScoreBearing(query.query)) return true;
    }
    return false;
}

pub fn searchRequestHasScoreBearingVectorSource(req: types.SearchRequest) bool {
    return req.dense != null or
        req.sparse != null or
        req.dense_queries.len > 0 or
        req.sparse_queries.len > 0;
}

pub fn searchRequestHasScoreBearingSource(req: types.SearchRequest) bool {
    return searchRequestHasScoreBearingTextSource(req) or
        searchRequestHasScoreBearingVectorSource(req);
}

pub const TextIndexEstimate = struct {
    name: []const u8,
    doc_count: u64 = 0,
    chunk_backed: bool = false,
    group_chunk_parents: bool = false,

    pub fn deinit(self: *const @This(), alloc: Allocator) void {
        alloc.free(self.name);
    }
};

pub const EmbeddingIndexEstimate = struct {
    name: []const u8,
    sparse: bool = false,
    doc_count: u64 = 0,
    dims: u32 = 0,
    chunk_backed: bool = false,

    pub fn deinit(self: *const @This(), alloc: Allocator) void {
        alloc.free(self.name);
    }
};

pub const GraphIndexEstimate = struct {
    name: []const u8,
    edge_count: u64 = 0,
    node_count: u64 = 0,

    pub fn deinit(self: *const @This(), alloc: Allocator) void {
        alloc.free(self.name);
    }
};

pub const RuntimePreflightSummary = struct {
    result_refs: []const []const u8 = &.{},
    graph_query_order: []const []const u8 = &.{},
    text_indexes: []const TextIndexEstimate = &.{},
    embedding_indexes: []const EmbeddingIndexEstimate = &.{},
    graph_indexes: []const GraphIndexEstimate = &.{},
    text_query_stats: []const distributed_stats_mod.TextFieldStats = &.{},
    doc_id_value_count: u32 = 0,
    filter_id_count: u32 = 0,
    exclude_id_count: u32 = 0,
    numeric_range_clause_count: u32 = 0,
    term_range_clause_count: u32 = 0,
    ip_range_clause_count: u32 = 0,
    bool_field_clause_count: u32 = 0,
    geo_filter_clause_count: u32 = 0,
    positive_id_result_upper_bound: ?u32 = null,
    structured_filter_doc_count_estimate: ?u64 = null,
    structured_filter_doc_count_lower_bound: ?u64 = null,
    structured_filter_doc_count_sample_estimate: ?u64 = null,
    structured_filter_count_exact: bool = false,
    structured_filter_count_sample_size: u32 = 0,
    structured_filter_count_budget_limit: ?u64 = null,
    text_result_upper_bound: ?u32 = null,
    text_term_doc_freq_total: u64 = 0,
    corpus_doc_count_estimate: ?u64 = null,
    selectivity_lower_bound_ratio: ?f32 = null,
    selectivity_sample_ratio: ?f32 = null,
    selectivity_upper_bound_ratio: ?f32 = null,
    result_doc_upper_bound: ?u32 = null,
    result_doc_estimate: ?u32 = null,
    shard_result_window: u32 = 0,
    shard_result_window_total: u64 = 0,
    stored_projection_doc_upper_bound_total: u64 = 0,
    effective_stored_projection_doc_estimate_total: ?u64 = null,
    effective_stored_projection_doc_upper_bound_total: u64 = 0,
    rerank_doc_upper_bound: u32 = 0,
    effective_rerank_doc_estimate: ?u32 = null,
    effective_rerank_doc_upper_bound: u32 = 0,
    aggregation_may_scan_full_results: bool = false,
    aggregation_second_pass_doc_estimate: ?u32 = null,
    aggregation_second_pass_doc_upper_bound: ?u32 = null,
    shard_count: u32 = 0,
    remote_shard_count: u32 = 0,
    dense_query_count: u32 = 0,
    vector_worker_candidate_count: u32 = 0,
    vector_worker_fallback_count: u32 = 0,
    vector_worker_filter_constraint_count: u32 = 0,
    vector_worker_requires_algebraic_filter_resolution: bool = false,
    dense_effective_k_total: u64 = 0,
    dense_search_width_total: u64 = 0,
    dense_search_width_max: u32 = 0,
    dense_epsilon_max: f32 = 0,

    pub fn deinit(self: *const @This(), alloc: Allocator) void {
        freeOwnedStringSlice(alloc, self.result_refs);
        freeOwnedStringSlice(alloc, self.graph_query_order);
        for (self.text_indexes) |*item| item.deinit(alloc);
        if (self.text_indexes.len > 0) alloc.free(@constCast(self.text_indexes));
        for (self.embedding_indexes) |*item| item.deinit(alloc);
        if (self.embedding_indexes.len > 0) alloc.free(@constCast(self.embedding_indexes));
        for (self.graph_indexes) |*item| item.deinit(alloc);
        if (self.graph_indexes.len > 0) alloc.free(@constCast(self.graph_indexes));
        distributed_stats_mod.deinitTextFieldStats(alloc, self.text_query_stats);
    }
};

const VisitState = enum { unvisited, visiting, done };

pub fn preflightRuntimeAlloc(
    alloc: Allocator,
    runtime: RuntimePreflight,
) !RuntimePreflightSummary {
    var result_refs = std.ArrayListUnmanaged([]const u8).empty;
    errdefer freeOwnedStringItems(alloc, result_refs.items);
    errdefer result_refs.deinit(alloc);

    if (runtime.has_full_text_results) try appendUniqueOwnedString(alloc, &result_refs, "$full_text_results");
    for (runtime.embedding_result_names) |name| try appendUniqueOwnedString(alloc, &result_refs, name);
    if (runtime.embedding_result_names.len > 0) try appendUniqueOwnedString(alloc, &result_refs, "$embeddings_results");
    if (runtime.has_full_text_results and runtime.embedding_result_names.len > 0) {
        try appendUniqueOwnedString(alloc, &result_refs, "$fused_results");
    }

    const sorted_query_indexes = try sortGraphQueriesByDependencies(alloc, runtime.graph_queries);
    defer alloc.free(sorted_query_indexes);

    var graph_query_order = std.ArrayListUnmanaged([]const u8).empty;
    errdefer freeOwnedStringItems(alloc, graph_query_order.items);
    errdefer graph_query_order.deinit(alloc);
    for (sorted_query_indexes) |query_index| {
        const graph_query = runtime.graph_queries[query_index];
        try graph_query_order.append(alloc, try alloc.dupe(u8, graph_query.name));
        const graph_ref = try std.fmt.allocPrint(alloc, "$graph_results.{s}", .{graph_query.name});
        defer alloc.free(graph_ref);
        try appendUniqueOwnedString(alloc, &result_refs, graph_ref);
    }

    var summary: RuntimePreflightSummary = .{
        .result_refs = if (result_refs.items.len == 0) &.{} else try result_refs.toOwnedSlice(alloc),
        .graph_query_order = if (graph_query_order.items.len == 0) &.{} else try graph_query_order.toOwnedSlice(alloc),
    };
    deriveEstimateFields(&summary);
    return summary;
}

pub fn preflightSearchRequestAlloc(alloc: Allocator, req: types.SearchRequest) !RuntimePreflightSummary {
    var embedding_result_names = std.ArrayListUnmanaged([]const u8).empty;
    defer {
        freeOwnedStringItems(alloc, embedding_result_names.items);
        embedding_result_names.deinit(alloc);
    }

    if (req.dense_queries.len > 0 or req.sparse_queries.len > 0) {
        for (req.dense_queries) |query| try appendUniqueOwnedString(alloc, &embedding_result_names, query.name);
        for (req.sparse_queries) |query| try appendUniqueOwnedString(alloc, &embedding_result_names, query.name);
    } else if (req.dense != null and req.sparse != null) {
        try appendUniqueOwnedString(alloc, &embedding_result_names, "dense");
        try appendUniqueOwnedString(alloc, &embedding_result_names, "sparse");
    } else if (req.dense != null or req.sparse != null) {
        try appendUniqueOwnedString(alloc, &embedding_result_names, "$embeddings_results");
    }

    return try preflightRuntimeAlloc(alloc, .{
        .has_full_text_results = hasSearchRequestFullTextResults(req),
        .embedding_result_names = embedding_result_names.items,
        .graph_queries = req.graph_queries,
    });
}

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
    for (queries, 0..) |_, i| try visitGraphQuery(alloc, queries, &by_name, states, &sorted, i);
    return try sorted.toOwnedSlice(alloc);
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
    if (graphQueryDependencyName(query.query.start_nodes)) |name| {
        if (by_name.get(name)) |dependency| try visitGraphQuery(alloc, queries, by_name, states, sorted, dependency);
    }
    if (query.query.target_nodes) |target| {
        if (graphQueryDependencyName(target)) |name| {
            if (by_name.get(name)) |dependency| try visitGraphQuery(alloc, queries, by_name, states, sorted, dependency);
        }
    }
    states[index] = .done;
    try sorted.append(alloc, index);
}

fn graphQueryDependencyName(selector: anytype) ?[]const u8 {
    return switch (selector) {
        .keys, .identities => null,
        .result_ref => |result_ref| if (std.mem.startsWith(u8, result_ref.ref, "$graph_results."))
            result_ref.ref["$graph_results.".len..]
        else
            null,
    };
}

fn hasSearchRequestFullTextResults(req: types.SearchRequest) bool {
    if (req.full_text != null or req.full_text_queries.len > 0) return true;
    if (req.filter_text != null or req.exclusion_text != null) return true;
    if (req.filter_query_json.len > 0 or req.exclusion_query_json.len > 0) return true;
    return !isDefaultMatchAll(req.query) and isTextQuery(req.query);
}

fn isTextQuery(query: types.Query) bool {
    return switch (query) {
        .match_none, .match_all, .phrase, .multi_phrase, .term, .fuzzy, .numeric_range, .date_range, .doc_id, .bool_field, .geo_distance, .geo_bbox, .term_range, .ip_range, .geo_shape, .match, .match_phrase, .prefix, .wildcard, .regexp => true,
        else => false,
    };
}

fn isDefaultMatchAll(query: types.Query) bool {
    return switch (query) {
        .match_all => true,
        else => false,
    };
}

fn appendUniqueOwnedString(alloc: Allocator, values: *std.ArrayListUnmanaged([]const u8), value: []const u8) !void {
    for (values.items) |existing| if (std.mem.eql(u8, existing, value)) return;
    try values.append(alloc, try alloc.dupe(u8, value));
}

fn freeOwnedStringItems(alloc: Allocator, values: []const []const u8) void {
    for (values) |value| alloc.free(@constCast(value));
}

fn freeOwnedStringSlice(alloc: Allocator, values: []const []const u8) void {
    for (values) |value| alloc.free(value);
    if (values.len > 0) alloc.free(@constCast(values));
}

pub fn deriveEstimateFields(summary: *RuntimePreflightSummary) void {
    summary.text_result_upper_bound = textResultUpperBound(summary.*);
    summary.text_term_doc_freq_total = textTermDocFreqTotal(summary.*);
    summary.corpus_doc_count_estimate = estimatedCorpusDocCount(summary.*);
    summary.result_doc_upper_bound = resultDocUpperBound(summary.*);
    summary.result_doc_estimate = resultDocEstimate(summary.*);
    summary.selectivity_lower_bound_ratio = selectivityLowerBoundRatio(summary.*);
    summary.selectivity_sample_ratio = selectivitySampleRatio(summary.*);
    summary.selectivity_upper_bound_ratio = selectivityUpperBoundRatio(summary.*);
    summary.effective_stored_projection_doc_estimate_total = if (summary.result_doc_estimate) |estimate|
        @min(summary.stored_projection_doc_upper_bound_total, estimate)
    else
        null;
    summary.effective_stored_projection_doc_upper_bound_total = if (summary.result_doc_upper_bound) |bound|
        @min(summary.stored_projection_doc_upper_bound_total, bound)
    else
        summary.stored_projection_doc_upper_bound_total;
    summary.effective_rerank_doc_estimate = if (summary.result_doc_estimate) |estimate|
        @min(summary.rerank_doc_upper_bound, estimate)
    else
        null;
    summary.effective_rerank_doc_upper_bound = if (summary.result_doc_upper_bound) |bound|
        @min(summary.rerank_doc_upper_bound, bound)
    else
        summary.rerank_doc_upper_bound;
    summary.aggregation_second_pass_doc_estimate = if (summary.aggregation_may_scan_full_results) summary.result_doc_estimate else null;
    summary.aggregation_second_pass_doc_upper_bound = if (summary.aggregation_may_scan_full_results) summary.result_doc_upper_bound else null;
}

fn textResultUpperBound(summary: RuntimePreflightSummary) ?u32 {
    var total_bound: u64 = 0;
    var has_terms = false;
    for (summary.text_query_stats) |item| {
        var field_bound: u64 = 0;
        for (item.term_doc_freqs) |term| {
            field_bound +|= term.doc_freq;
            has_terms = true;
        }
        if (field_bound == 0) continue;
        const capped_field_bound = @min(field_bound, item.global_doc_count);
        total_bound +|= capped_field_bound;
    }
    if (!has_terms) return null;
    if (estimatedCorpusDocCount(summary)) |corpus_docs| {
        total_bound = @min(total_bound, corpus_docs);
    }
    return @intCast(@min(total_bound, @as(u64, std.math.maxInt(u32))));
}

fn textTermDocFreqTotal(summary: RuntimePreflightSummary) u64 {
    var total: u64 = 0;
    for (summary.text_query_stats) |item| {
        for (item.term_doc_freqs) |term| total +|= term.doc_freq;
    }
    return total;
}

fn estimatedCorpusDocCount(summary: RuntimePreflightSummary) ?u64 {
    var corpus_docs: u64 = 0;
    for (summary.text_query_stats) |item| corpus_docs = @max(corpus_docs, item.global_doc_count);
    for (summary.text_indexes) |item| corpus_docs = @max(corpus_docs, item.doc_count);
    for (summary.embedding_indexes) |item| corpus_docs = @max(corpus_docs, item.doc_count);
    for (summary.graph_indexes) |item| corpus_docs = @max(corpus_docs, item.node_count);
    return if (corpus_docs > 0) corpus_docs else null;
}

fn selectivityUpperBoundRatio(summary: RuntimePreflightSummary) ?f32 {
    const bound = summary.result_doc_upper_bound orelse return null;
    const corpus_docs = estimatedCorpusDocCount(summary) orelse return null;
    if (corpus_docs == 0) return null;
    return @as(f32, @floatFromInt(bound)) / @as(f32, @floatFromInt(corpus_docs));
}

fn selectivityLowerBoundRatio(summary: RuntimePreflightSummary) ?f32 {
    const lower_bound = summary.structured_filter_doc_count_lower_bound orelse return null;
    const corpus_docs = estimatedCorpusDocCount(summary) orelse return null;
    if (corpus_docs == 0) return null;
    return @as(f32, @floatFromInt(lower_bound)) / @as(f32, @floatFromInt(corpus_docs));
}

fn selectivitySampleRatio(summary: RuntimePreflightSummary) ?f32 {
    const sample_estimate = summary.structured_filter_doc_count_sample_estimate orelse return null;
    const corpus_docs = estimatedCorpusDocCount(summary) orelse return null;
    if (corpus_docs == 0) return null;
    return @as(f32, @floatFromInt(sample_estimate)) / @as(f32, @floatFromInt(corpus_docs));
}

fn resultDocUpperBound(summary: RuntimePreflightSummary) ?u32 {
    var bound = summary.positive_id_result_upper_bound;
    if (summary.structured_filter_doc_count_sample_estimate == null) if (summary.structured_filter_doc_count_estimate) |structured_count| {
        const structured_bound: u32 = @intCast(@min(structured_count, @as(u64, std.math.maxInt(u32))));
        bound = if (bound) |existing| @min(existing, structured_bound) else structured_bound;
    };
    if (summary.text_result_upper_bound) |text_bound| {
        bound = if (bound) |existing| @min(existing, text_bound) else text_bound;
    }
    return bound;
}

fn resultDocEstimate(summary: RuntimePreflightSummary) ?u32 {
    var estimate: ?u32 = null;
    if (summary.structured_filter_count_budget_limit != null) {
        if (summary.structured_filter_doc_count_sample_estimate) |structured_count| {
            estimate = @intCast(@min(structured_count, @as(u64, std.math.maxInt(u32))));
        } else if (summary.structured_filter_doc_count_estimate) |structured_count| {
            estimate = @intCast(@min(structured_count, @as(u64, std.math.maxInt(u32))));
        }
    } else if (summary.structured_filter_doc_count_estimate) |structured_count| {
        estimate = @intCast(@min(structured_count, @as(u64, std.math.maxInt(u32))));
    } else if (summary.structured_filter_doc_count_sample_estimate) |structured_count| {
        estimate = @intCast(@min(structured_count, @as(u64, std.math.maxInt(u32))));
    }
    if (estimate) |value| {
        if (summary.result_doc_upper_bound) |bound| return @min(value, bound);
        return value;
    }
    return null;
}
