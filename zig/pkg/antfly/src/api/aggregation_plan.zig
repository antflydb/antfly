// Copyright 2026 Antfly, Inc.
// SPDX-License-Identifier: Elastic-2.0

//! Shared aggregation collection and budget rules for coordinators and local owners.
const std = @import("std");
const types = @import("../storage/db/types.zig");
const aggregation_contract = @import("../storage/db/aggregations_contract.zig");
const local_query_contract = @import("local_query_contract.zig");
const checkQueryDeadline = local_query_contract.checkQueryDeadline;
const searchRequestHasResolvedDocFilter = local_query_contract.searchRequestHasResolvedDocFilter;

pub const default_aggregation_full_result_budget: u32 = 100_000;

pub fn aggregationFullResultBudgetFromRaw(raw: ?[*:0]u8) u32 {
    const value = raw orelse return default_aggregation_full_result_budget;
    const slice = std.mem.span(value);
    if (slice.len == 0) return default_aggregation_full_result_budget;
    const parsed = std.fmt.parseUnsigned(u32, slice, 10) catch return default_aggregation_full_result_budget;
    if (parsed == 0) return default_aggregation_full_result_budget;
    return @min(parsed, @as(u32, @intCast(aggregation_contract.max_aggregation_source_hits)));
}

pub fn aggregationFullResultBudget() u32 {
    return aggregationFullResultBudgetFromRaw(std.c.getenv("ANTFLY_AGGREGATION_FULL_RESULT_BUDGET\x00"));
}

pub fn identityGenerationForAggregationFullResultRerun(
    req: types.SearchRequest,
    result: types.SearchResult,
) !?u64 {
    if (aggregationCanUseCurrentResult(req, result)) return req.identity_read_generation orelse result.identity_read_generation;
    return req.identity_read_generation orelse result.identity_read_generation orelse error.UnsupportedQueryRequest;
}

pub fn aggregationCanUseCurrentResult(req: types.SearchRequest, result: types.SearchResult) bool {
    if (result.total_hits_relation != .exact) return false;
    if (result.total_hits == 0) return true;
    return !req.count_only and result.hits.len == result.total_hits;
}

pub fn aggregationFullResultLimit(req: types.SearchRequest, result: types.SearchResult, operation: []const u8) !u32 {
    try checkQueryDeadline(req);
    const budget = aggregationFullResultBudget();
    if (result.total_hits_relation == .exact and result.total_hits > budget) {
        std.log.warn("query aggregation full-result rerun budget exceeded operation={s} total_hits={d} budget={d}", .{
            operation,
            result.total_hits,
            budget,
        });
        return error.QueryCandidateBudgetExceeded;
    }
    // Even an exact first-page count only describes that execution's text
    // snapshot. Derived indexing can publish more already committed documents
    // before the rerun without advancing the primary identity generation.
    // Collect up to the budget, then prove completeness against the rerun's
    // own total instead of truncating to the earlier snapshot's count.
    return budget;
}

pub fn requireCompleteAggregationFullResult(
    req: types.SearchRequest,
    result: types.SearchResult,
    operation: []const u8,
) !void {
    try checkQueryDeadline(req);
    if (aggregationCanUseCurrentResult(req, result)) return;
    std.log.warn("query aggregation bounded full-result rerun remained incomplete operation={s} relation={s} total_hits={d} returned_hits={d} budget={d}", .{
        operation,
        @tagName(result.total_hits_relation),
        result.total_hits,
        result.hits.len,
        aggregationFullResultBudget(),
    });
    if (result.total_hits_relation == .gte or result.total_hits >= aggregationFullResultBudget()) {
        return error.QueryCandidateBudgetExceeded;
    }
    return error.UnsupportedQueryRequest;
}

pub fn aggregationFullResultRequest(req: types.SearchRequest, result: types.SearchResult, operation: []const u8) !types.SearchRequest {
    const identity_read_generation = try identityGenerationForAggregationFullResultRerun(req, result);
    return try aggregationFullResultRequestAtGeneration(req, result, operation, identity_read_generation);
}

pub fn distributedAggregationFullResultRequest(req: types.SearchRequest, result: types.SearchResult, operation: []const u8) !types.SearchRequest {
    if (result.shard_identity_read_generations.len == 0) return try aggregationFullResultRequest(req, result, operation);
    return try aggregationFullResultRequestAtGeneration(req, result, operation, null);
}

pub fn aggregationFullResultRequestAtGeneration(
    req: types.SearchRequest,
    result: types.SearchResult,
    operation: []const u8,
    identity_read_generation: ?u64,
) !types.SearchRequest {
    const full_limit = try aggregationFullResultLimit(req, result, operation);
    return aggregationCollectionRequest(req, full_limit, identity_read_generation);
}

pub fn aggregationCollectionRequest(req: types.SearchRequest, full_limit: u32, identity_read_generation: ?u64) types.SearchRequest {
    var full_req = req;
    full_req.identity_read_generation = identity_read_generation;
    full_req.offset = 0;
    full_req.limit = full_limit;
    // Exact aggregation needs exhaustive candidate coverage, not merely a larger k.
    full_req.search_effort = 1.0;
    full_req.include_stored = true;
    full_req.count_only = false;
    full_req.order_by = &.{};
    full_req.search_after = &.{};
    full_req.search_before = &.{};
    // Graph-metric reranking only orders and scores the returned hit page.
    // Aggregations consume stored fields from every match, so the internal
    // collection must not inherit its bounded candidate window. The original
    // request and its already-reranked hits remain unchanged.
    full_req.graph_metric_rerank = null;
    // Aggregations operate on the top-level result set. Canonical hierarchy
    // matches are a bounded evidence projection attached to those groups, not
    // additional aggregation rows. Disable nested expansion for the complete
    // aggregation rerun so its internal full-result limit is not mistaken for
    // a public groups-times-matches response budget.
    return types.canonicalGroupedMatchSelectionRequest(full_req);
}

pub fn canConsiderAlgebraicAggregations(req: types.SearchRequest) bool {
    return req.full_text == null and
        req.filter_text == null and
        req.exclusion_text == null and
        req.exclusion_query_json.len == 0 and
        req.full_text_queries.len == 0 and
        req.dense == null and
        req.sparse == null and
        req.dense_queries.len == 0 and
        req.sparse_queries.len == 0 and
        req.graph_queries.len == 0 and
        req.merge_config == null and
        req.reranker == null and
        req.pruner == null and
        req.filter_prefix.len == 0 and
        req.filter_ids.len == 0 and
        req.exclude_ids.len == 0 and
        req.filter_doc_ids.len == 0 and
        !req.filter_doc_ids_positive and
        req.exclude_doc_ids.len == 0 and
        !searchRequestHasResolvedDocFilter(req) and
        req.distance_over == null and
        req.distance_under == null;
}
