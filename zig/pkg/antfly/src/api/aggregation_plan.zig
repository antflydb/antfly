// Copyright 2026 Antfly, Inc.
// SPDX-License-Identifier: Elastic-2.0

//! Shared aggregation collection and budget rules for coordinators and local owners.
const std = @import("std");
const types = @import("../storage/db/types.zig");
const aggregation_contract = @import("../storage/db/aggregations_contract.zig");
const local_query_contract = @import("local_query_contract.zig");
const control_contract = @import("../storage/db/query/control_contract.zig");
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

// Aggregation domain of a composed (hybrid) request.
//
// A full-text component has a matching set: every document that matches the
// query. A vector component does not; every document has some distance, and the
// only defined set is the ranked window the caller asked for. Aggregations over
// a request with vector components therefore count every full-text match
// (bounded by the full-result budget) plus each vector component's top window,
// using the caller's k and search effort. Widening a vector component to the
// budget instead would ask approximate search for the whole index, which it can
// never certify as complete, and would not describe the caller's result set.
//
// The domain is collected as separate sub-queries and unioned:
// - the text sub-query is the collection request without vector components,
//   fusion, reranking, pruning, or graph queries, and must prove completeness;
// - each vector sub-query keeps exactly one component and runs with the window
//   that component contributed to the original request. It goes through the
//   direct single-vector path, so distributed tables merge shard windows into a
//   global top window per component.

/// Vector components that contribute a ranked window to a composed search.
/// Mirrors searchComposed: a singleton `dense` or `sparse` query is used only
/// when no named query of that kind is present.
pub fn requestVectorComponentCount(req: types.SearchRequest) usize {
    return denseComponentCount(req) + sparseComponentCount(req);
}

fn denseComponentCount(req: types.SearchRequest) usize {
    if (req.dense_queries.len > 0) return req.dense_queries.len;
    return @intFromBool(req.dense != null);
}

fn sparseComponentCount(req: types.SearchRequest) usize {
    if (req.sparse_queries.len > 0) return req.sparse_queries.len;
    return @intFromBool(req.sparse != null);
}

/// Whether the request carries a full-text matching set. The public parser
/// synthesizes a `match_all` full-text query for filter-only requests; that is a
/// filter carrier, not a match set, and contributes no aggregation rows.
pub fn requestHasAggregationTextDomain(req: types.SearchRequest) bool {
    if (req.full_text_queries.len > 0) return true;
    if (req.full_text) |text| return text != .match_all;
    return !control_contract.isDefaultMatchAll(req.query) and control_contract.isTextQuery(req.query);
}

fn withoutRetrievalComposition(req: types.SearchRequest) types.SearchRequest {
    var out = req;
    out.dense = null;
    out.sparse = null;
    out.dense_queries = &.{};
    out.sparse_queries = &.{};
    out.merge_config = null;
    out.reranker = null;
    out.reranker_query_text = "";
    out.pruner = null;
    out.clearGraphQueries();
    out.graph_metric_queries = &.{};
    out.graph_metric_rerank = null;
    out.profile = false;
    return out;
}

pub const AggregationDomainPlan = struct {
    /// The widened collection request. It still carries the vector components,
    /// so aggregation context selection (text index names, algebraic
    /// eligibility) sees the caller's retrieval shape rather than a text-only
    /// or vector-only fragment.
    aggregation_req: types.SearchRequest,
    /// Full-text matching set, or null when the request has none.
    text_req: ?types.SearchRequest,
    /// Window each vector component contributed to the original request.
    vector_window: u32,

    pub fn vectorComponentCount(self: AggregationDomainPlan) usize {
        if (self.vector_window == 0) return 0;
        return requestVectorComponentCount(self.aggregation_req);
    }

    /// One vector component, isolated, at its original window. `full_text` and
    /// `merge_config` must stay null: a full-text query would hard-filter the
    /// vector leg, and fusion would route shards back through composed search.
    pub fn vectorComponentRequest(self: AggregationDomainPlan, index: usize) types.SearchRequest {
        const source = self.aggregation_req;
        var sub = withoutRetrievalComposition(source);
        sub.full_text = null;
        sub.full_text_queries = &.{};
        sub.query = .{ .match_all = {} };
        sub.offset = 0;
        sub.limit = self.vector_window;
        const dense_count = denseComponentCount(source);
        if (index < dense_count) {
            if (source.dense_queries.len > 0) {
                sub.dense_queries = source.dense_queries[index..][0..1];
            } else {
                sub.dense = source.dense;
            }
        } else {
            const sparse_index = index - dense_count;
            if (source.sparse_queries.len > 0) {
                sub.sparse_queries = source.sparse_queries[sparse_index..][0..1];
            } else {
                sub.sparse = source.sparse;
            }
        }
        return sub;
    }
};

pub const AggregationDomainDecision = union(enum) {
    /// Keep the single full-result rerun.
    unchanged,
    decomposed: AggregationDomainPlan,
};

/// Decide how to collect the aggregation input. `original` is the caller's
/// request; `collection` is the widened request from aggregationCollectionRequest.
/// Vector windows come from `original` only: `collection.limit` is the budget,
/// and deriving a window from it would recreate the whole-index request.
pub fn aggregationDomainPlan(
    original: types.SearchRequest,
    collection: types.SearchRequest,
    budget: u32,
) !AggregationDomainDecision {
    if (requestVectorComponentCount(original) == 0) return .unchanged;
    switch (original.query) {
        // Legacy knn queries are not composed components; keep their behavior.
        .dense_knn, .sparse_knn => return .unchanged,
        else => {},
    }
    // Graph expansion adds hits after retrieval, and unit hierarchy modes are
    // hydrated and merged per unit. Neither decomposes into per-source windows.
    if (original.expand_strategy != null or original.hierarchy_children != null) return .unchanged;
    switch (original.return_mode) {
        .unit, .unit_with_chunks => return .unchanged,
        else => {},
    }
    const window = control_contract.composedVectorComponentWindow(original);
    if (window > budget) {
        std.log.warn("query aggregation vector window exceeds full-result budget window={d} budget={d}", .{ window, budget });
        return error.QueryCandidateBudgetExceeded;
    }
    return .{ .decomposed = .{
        .aggregation_req = collection,
        .text_req = if (requestHasAggregationTextDomain(collection)) withoutRetrievalComposition(collection) else null,
        .vector_window = window,
    } };
}

/// Collect the complete aggregation input for `full_req`, the collection request
/// derived from `original`. `searcher` provides
/// `fn search(self, std.mem.Allocator, types.SearchRequest) !types.SearchResult`
/// and must execute every sub-query at the same read generation as the caller's
/// first pass. The returned result is complete for the aggregation domain.
pub fn collectAggregationFullResult(
    alloc: std.mem.Allocator,
    original: types.SearchRequest,
    full_req: types.SearchRequest,
    searcher: anytype,
    operation: []const u8,
) !types.SearchResult {
    const budget = aggregationFullResultBudget();
    switch (try aggregationDomainPlan(original, full_req, budget)) {
        .unchanged => {
            var result = try searcher.search(alloc, full_req);
            errdefer result.deinit();
            try requireCompleteAggregationFullResult(full_req, result, operation);
            return result;
        },
        .decomposed => |plan| return try collectAggregationDomain(alloc, plan, budget, searcher, operation),
    }
}

pub fn collectAggregationDomain(
    alloc: std.mem.Allocator,
    plan: AggregationDomainPlan,
    budget: u32,
    searcher: anytype,
    operation: []const u8,
) !types.SearchResult {
    const component_count = plan.vectorComponentCount();
    var parts = std.ArrayListUnmanaged(types.SearchResult).empty;
    defer {
        for (parts.items) |*part| part.deinit();
        parts.deinit(alloc);
    }
    try parts.ensureTotalCapacity(alloc, component_count + 1);
    if (plan.text_req) |text_req| {
        try checkQueryDeadline(text_req);
        parts.appendAssumeCapacity(try searcher.search(alloc, text_req));
        try requireCompleteAggregationFullResult(text_req, parts.items[parts.items.len - 1], operation);
    }
    for (0..component_count) |index| {
        const component_req = plan.vectorComponentRequest(index);
        try checkQueryDeadline(component_req);
        parts.appendAssumeCapacity(try searcher.search(alloc, component_req));
        const returned = parts.items[parts.items.len - 1].hits.len;
        // The component's window is its whole matching set, so its own total
        // relation is not a completeness signal. It may never exceed the window.
        if (returned > plan.vector_window) {
            std.log.warn("query aggregation vector component exceeded its window operation={s} component={d} returned_hits={d} window={d}", .{
                operation,
                index,
                returned,
                plan.vector_window,
            });
            return error.UnsupportedQueryRequest;
        }
    }
    return try mergeAggregationDomainResults(alloc, parts.items, budget, operation);
}

var moved_hit_id: [0]u8 = .{};

fn sameAllocator(a: std.mem.Allocator, b: std.mem.Allocator) bool {
    return a.ptr == b.ptr and a.vtable == b.vtable;
}

/// Union aggregation domain parts into one complete result, deduplicated by
/// hit id in part order (the first occurrence wins, as in fusion without
/// complete ordinals). Hits are moved out of parts that share `alloc` and
/// cloned otherwise; every part remains safe to deinit afterwards.
pub fn mergeAggregationDomainResults(
    alloc: std.mem.Allocator,
    parts: []types.SearchResult,
    budget: u32,
    operation: []const u8,
) !types.SearchResult {
    var seen = std.StringHashMapUnmanaged(void).empty;
    defer seen.deinit(alloc);
    var merged = std.ArrayListUnmanaged(types.SearchHit).empty;
    errdefer {
        for (merged.items) |*hit| hit.deinit(alloc);
        merged.deinit(alloc);
    }
    for (parts) |*part| {
        const movable = sameAllocator(part.alloc, alloc);
        for (part.hits) |*hit| {
            if (seen.contains(hit.id)) continue;
            if (merged.items.len >= budget) {
                std.log.warn("query aggregation domain exceeded full-result budget operation={s} budget={d}", .{ operation, budget });
                return error.QueryCandidateBudgetExceeded;
            }
            if (movable) {
                try merged.append(alloc, hit.*);
                hit.* = .{ .id = &moved_hit_id };
            } else {
                var cloned = try hit.clone(alloc);
                merged.append(alloc, cloned) catch |err| {
                    cloned.deinit(alloc);
                    return err;
                };
            }
            try seen.put(alloc, merged.items[merged.items.len - 1].id, {});
        }
    }

    var identity_read_generation: ?u64 = null;
    var shard_generations: []types.ShardIdentityReadGeneration = &.{};
    errdefer if (shard_generations.len > 0) alloc.free(shard_generations);
    for (parts) |*part| {
        if (identity_read_generation == null) identity_read_generation = part.identity_read_generation;
        if (shard_generations.len == 0 and part.shard_identity_read_generations.len > 0) {
            // Every part was pinned to the same shard generations, so any
            // part's tokens describe the union for downstream text statistics.
            if (sameAllocator(part.alloc, alloc)) {
                shard_generations = part.shard_identity_read_generations;
                part.shard_identity_read_generations = &.{};
            } else {
                shard_generations = try alloc.dupe(types.ShardIdentityReadGeneration, part.shard_identity_read_generations);
            }
        }
    }

    const hits = try merged.toOwnedSlice(alloc);
    return .{
        .alloc = alloc,
        .hits = hits,
        .total_hits = @intCast(hits.len),
        .total_hits_relation = .exact,
        .identity_read_generation = identity_read_generation,
        .shard_identity_read_generations = shard_generations,
    };
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
