// Copyright 2026 Antfly, Inc.
// SPDX-License-Identifier: Apache-2.0
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

//! Pure aggregation adapter/collection tests, with no DB owner.
const std = @import("std");
const types = @import("storage/db/types.zig");
const implementation = @import("api/aggregation_plan.zig");
const default_aggregation_full_result_budget = implementation.default_aggregation_full_result_budget;
const aggregationFullResultBudgetFromRaw = implementation.aggregationFullResultBudgetFromRaw;
const aggregationFullResultBudget = implementation.aggregationFullResultBudget;
const identityGenerationForAggregationFullResultRerun = implementation.identityGenerationForAggregationFullResultRerun;
const aggregationCanUseCurrentResult = implementation.aggregationCanUseCurrentResult;
const aggregationFullResultLimit = implementation.aggregationFullResultLimit;
const requireCompleteAggregationFullResult = implementation.requireCompleteAggregationFullResult;
const aggregationFullResultRequest = implementation.aggregationFullResultRequest;
const distributedAggregationFullResultRequest = implementation.distributedAggregationFullResultRequest;
const aggregationFullResultRequestAtGeneration = implementation.aggregationFullResultRequestAtGeneration;
const aggregationCollectionRequest = implementation.aggregationCollectionRequest;
const requestVectorComponentCount = implementation.requestVectorComponentCount;
const requestHasAggregationTextDomain = implementation.requestHasAggregationTextDomain;
const AggregationDomainPlan = implementation.AggregationDomainPlan;
const AggregationDomainDecision = implementation.AggregationDomainDecision;
const aggregationDomainPlan = implementation.aggregationDomainPlan;
const SearchCapability = implementation.SearchCapability;
const collectAggregationFullResult = implementation.collectAggregationFullResult;
const collectFullResult = implementation.collectFullResult;
const collectAggregationDomain = implementation.collectAggregationDomain;
const collectDomain = implementation.collectDomain;
const mergeAggregationDomainResults = implementation.mergeAggregationDomainResults;
const canConsiderAlgebraicAggregations = implementation.canConsiderAlgebraicAggregations;

const aggregation_domain_test_dense = [_]types.NamedDenseQuery{
    .{ .name = "first", .index_name = "va", .query = .{ .vector = &.{ 1, 0 }, .k = 30 } },
    .{ .name = "second", .index_name = "vb", .query = .{ .vector = &.{ 0, 1 }, .k = 30 } },
};

fn aggregationDomainTestHybrid() types.SearchRequest {
    return .{
        .full_text = .{ .match = .{ .field = "body", .text = "needle" } },
        .dense_queries = &aggregation_domain_test_dense,
        .merge_config = .{ .strategy = .rrf, .rank_constant = 60 },
        .limit = 30,
        .search_effort = 0.25,
        .filter_query_json = "{\"term\":\"alpha\",\"field\":\"role\"}",
        .aggregations_json = "{\"kinds\":{\"type\":\"terms\",\"field\":\"kind\"}}",
    };
}

fn aggregationDomainTestPlan(original: types.SearchRequest) !AggregationDomainPlan {
    const collection = aggregationCollectionRequest(original, default_aggregation_full_result_budget, 7);
    return switch (try aggregationDomainPlan(original, collection, default_aggregation_full_result_budget)) {
        .decomposed => |plan| plan,
        .unchanged => error.TestUnexpectedResult,
    };
}

test "aggregation domain leaves requests without vector components unchanged" {
    const text_only: types.SearchRequest = .{
        .full_text = .{ .match = .{ .field = "body", .text = "needle" } },
        .limit = 30,
    };
    const collection = aggregationCollectionRequest(text_only, default_aggregation_full_result_budget, 7);
    try std.testing.expect((try aggregationDomainPlan(text_only, collection, default_aggregation_full_result_budget)) == .unchanged);
    try std.testing.expectEqual(@as(usize, 0), requestVectorComponentCount(text_only));
}

test "aggregation domain windows vector components by the original composed window" {
    // The collection request's limit is the budget. The window must come
    // from the caller's request, or every component becomes the whole index.
    var plan = try aggregationDomainTestPlan(aggregationDomainTestHybrid());
    try std.testing.expectEqual(default_aggregation_full_result_budget, plan.aggregation_req.limit);
    try std.testing.expectEqual(@as(u32, 30), plan.vector_window);
    try std.testing.expectEqual(@as(u32, 30), plan.vectorComponentRequest(0).limit);

    var wide = aggregationDomainTestHybrid();
    wide.merge_config = .{ .strategy = .rrf, .window_size = 50 };
    plan = try aggregationDomainTestPlan(wide);
    try std.testing.expectEqual(@as(u32, 50), plan.vector_window);

    var reranked = aggregationDomainTestHybrid();
    reranked.reranker = .{ .provider = .antfly, .field = "body", .candidate_count = 80 };
    plan = try aggregationDomainTestPlan(reranked);
    try std.testing.expectEqual(@as(u32, 80), plan.vector_window);

    // A larger internal k is preserved on the component; the executor
    // still searches max(k, window) and pages the same prefix.
    const big_k = [_]types.NamedDenseQuery{
        .{ .name = "first", .index_name = "va", .query = .{ .vector = &.{ 1, 0 }, .k = 100 } },
    };
    var internal = aggregationDomainTestHybrid();
    internal.dense_queries = &big_k;
    plan = try aggregationDomainTestPlan(internal);
    try std.testing.expectEqual(@as(u32, 30), plan.vector_window);
    try std.testing.expectEqual(@as(u32, 100), plan.vectorComponentRequest(0).dense_queries[0].query.k);
}

test "aggregation domain distinguishes explicit match_all from filter carriers" {
    var semantic = aggregationDomainTestHybrid();
    semantic.full_text = .{ .match_all = {} };
    var plan = try aggregationDomainTestPlan(semantic);
    try std.testing.expect(plan.text_req == null);
    try std.testing.expectEqual(@as(usize, 2), plan.vectorComponentCount());

    const explicit = [_]types.TextQuery{.{ .match_all = {} }};
    semantic.full_text = .{ .bool_query = .{ .must = &explicit } };
    plan = try aggregationDomainTestPlan(semantic);
    try std.testing.expect(plan.text_req != null);

    semantic.full_text = null;
    plan = try aggregationDomainTestPlan(semantic);
    try std.testing.expect(plan.text_req == null);

    plan = try aggregationDomainTestPlan(aggregationDomainTestHybrid());
    const text_req = plan.text_req orelse return error.TestUnexpectedResult;
    try std.testing.expectEqual(@as(usize, 0), requestVectorComponentCount(text_req));
    try std.testing.expect(text_req.merge_config == null);
    try std.testing.expectEqual(default_aggregation_full_result_budget, text_req.limit);
    try std.testing.expectEqualStrings(aggregationDomainTestHybrid().filter_query_json, text_req.filter_query_json);
}

test "aggregation domain isolates one retrieval source per vector sub-request" {
    var original = aggregationDomainTestHybrid();
    original.reranker = .{ .provider = .antfly, .field = "body", .candidate_count = 40 };
    original.reranker_query_text = "needle";
    original.pruner = .{ .min_score_ratio = 0.5 };
    const plan = try aggregationDomainTestPlan(original);
    try std.testing.expectEqual(@as(usize, 2), plan.vectorComponentCount());
    for (0..2) |index| {
        const sub = plan.vectorComponentRequest(index);
        try std.testing.expect(sub.full_text == null);
        try std.testing.expectEqual(@as(usize, 0), sub.full_text_queries.len);
        try std.testing.expect(sub.merge_config == null);
        try std.testing.expect(sub.reranker == null);
        try std.testing.expectEqualStrings("", sub.reranker_query_text);
        try std.testing.expect(sub.pruner == null);
        try std.testing.expectEqual(@as(u32, 0), sub.offset);
        try std.testing.expectEqual(@as(u32, 40), sub.limit);
        // The caller's effort, filters, and read generation are kept.
        try std.testing.expectEqual(@as(?f32, 0.25), sub.search_effort);
        try std.testing.expectEqualStrings(original.filter_query_json, sub.filter_query_json);
        try std.testing.expectEqual(@as(?u64, 7), sub.identity_read_generation);
        try std.testing.expectEqual(@as(usize, 1), sub.dense_queries.len);
        try std.testing.expect(sub.dense_queries.ptr == aggregation_domain_test_dense[index..].ptr);
        try std.testing.expectEqual(@as(usize, 0), sub.sparse_queries.len);
        try std.testing.expect(sub.dense == null and sub.sparse == null);
        try std.testing.expect(sub.query == .match_all);
    }

    // Singleton legacy fields are components too, dense before sparse.
    const singleton: types.SearchRequest = .{
        .full_text = .{ .match = .{ .field = "body", .text = "needle" } },
        .dense = .{ .vector = &.{ 1, 0 }, .k = 10 },
        .sparse = .{ .indices = &.{3}, .values = &.{1}, .k = 10 },
        .limit = 10,
    };
    const singleton_plan = try aggregationDomainTestPlan(singleton);
    try std.testing.expectEqual(@as(usize, 2), singleton_plan.vectorComponentCount());
    const dense_sub = singleton_plan.vectorComponentRequest(0);
    try std.testing.expect(dense_sub.dense != null and dense_sub.sparse == null);
    const sparse_sub = singleton_plan.vectorComponentRequest(1);
    try std.testing.expect(sparse_sub.sparse != null and sparse_sub.dense == null);
    try std.testing.expectEqual(@as(u32, 10), sparse_sub.limit);
}

test "aggregation domain declines expansion, unit modes, and legacy knn" {
    var expand = aggregationDomainTestHybrid();
    expand.expand_strategy = .@"union";
    try std.testing.expect((try aggregationDomainPlan(expand, expand, default_aggregation_full_result_budget)) == .unchanged);

    var unit = aggregationDomainTestHybrid();
    unit.return_mode = .unit;
    try std.testing.expect((try aggregationDomainPlan(unit, unit, default_aggregation_full_result_budget)) == .unchanged);

    var knn = aggregationDomainTestHybrid();
    knn.query = .{ .dense_knn = .{ .vector = &.{ 1, 0 }, .k = 5 } };
    try std.testing.expect((try aggregationDomainPlan(knn, knn, default_aggregation_full_result_budget)) == .unchanged);
}

test "aggregation domain rejects vector windows above the budget" {
    var original = aggregationDomainTestHybrid();
    original.merge_config = .{ .strategy = .rrf, .window_size = 500 };
    try std.testing.expectError(error.QueryCandidateBudgetExceeded, aggregationDomainPlan(original, original, 100));
}

test "aggregation domain keeps algebraic shortcuts disabled for vector requests" {
    // Aggregation context uses the collection request, which still carries
    // the vector components. A vector-only fragment would qualify for the
    // algebraic path and aggregate the whole index.
    var semantic = aggregationDomainTestHybrid();
    semantic.full_text = null;
    semantic.filter_query_json = "";
    const plan = try aggregationDomainTestPlan(semantic);
    try std.testing.expect(!canConsiderAlgebraicAggregations(plan.aggregation_req));
}

fn aggregationDomainTestResult(alloc: std.mem.Allocator, ids: []const []const u8, relation: types.TotalHitsRelation) !types.SearchResult {
    const hits = try alloc.alloc(types.SearchHit, ids.len);
    var initialized: usize = 0;
    errdefer {
        for (hits[0..initialized]) |*hit| hit.deinit(alloc);
        alloc.free(hits);
    }
    for (ids, 0..) |id, index| {
        hits[index] = .{ .id = try alloc.dupe(u8, id) };
        initialized += 1;
        hits[index].stored_data = try alloc.dupe(u8, "{}");
    }
    return .{
        .alloc = alloc,
        .hits = hits,
        .total_hits = @intCast(ids.len),
        .total_hits_relation = relation,
    };
}

test "aggregation domain merge deduplicates, moves hits, and enforces the budget" {
    const alloc = std.testing.allocator;
    var parts = [_]types.SearchResult{
        try aggregationDomainTestResult(alloc, &.{ "doc:a", "doc:b" }, .exact),
        try aggregationDomainTestResult(alloc, &.{ "doc:b", "doc:c" }, .gte),
        try aggregationDomainTestResult(alloc, &.{ "doc:d", "doc:a" }, .gte),
    };
    defer for (&parts) |*part| part.deinit();
    var merged = try mergeAggregationDomainResults(alloc, &parts, 100, "test");
    defer merged.deinit();
    try std.testing.expectEqual(types.TotalHitsRelation.exact, merged.total_hits_relation);
    try std.testing.expectEqual(@as(u32, 4), merged.total_hits);
    const want = [_][]const u8{ "doc:a", "doc:b", "doc:c", "doc:d" };
    for (want, merged.hits) |id, hit| {
        try std.testing.expectEqualStrings(id, hit.id);
        try std.testing.expect(hit.stored_data != null);
    }
    try std.testing.expect(aggregationCanUseCurrentResult(.{}, merged));

    var over = [_]types.SearchResult{
        try aggregationDomainTestResult(alloc, &.{ "doc:a", "doc:b" }, .exact),
        try aggregationDomainTestResult(alloc, &.{"doc:c"}, .gte),
    };
    defer for (&over) |*part| part.deinit();
    try std.testing.expectError(error.QueryCandidateBudgetExceeded, mergeAggregationDomainResults(alloc, &over, 2, "test"));
}

test "aggregation domain collection runs text first and requires only text completeness" {
    const alloc = std.testing.allocator;
    const Searcher = struct {
        calls: *std.ArrayListUnmanaged(types.SearchRequest),
        text_relation: types.TotalHitsRelation,

        pub fn search(self: @This(), a: std.mem.Allocator, req: types.SearchRequest) !types.SearchResult {
            try self.calls.append(a, req);
            if (req.dense_queries.len == 0) return try aggregationDomainTestResult(a, &.{ "doc:t1", "doc:shared" }, self.text_relation);
            // Vector windows report `gte` against the whole index; that is
            // their defined matching set, not an incomplete input.
            if (std.mem.eql(u8, req.dense_queries[0].index_name, "va")) return try aggregationDomainTestResult(a, &.{ "doc:shared", "doc:a1" }, .gte);
            return try aggregationDomainTestResult(a, &.{"doc:b1"}, .gte);
        }
    };
    var calls = std.ArrayListUnmanaged(types.SearchRequest).empty;
    defer calls.deinit(alloc);
    const original = aggregationDomainTestHybrid();
    const collection = aggregationCollectionRequest(original, default_aggregation_full_result_budget, 7);
    var collected = try collectAggregationFullResult(alloc, original, collection, Searcher{ .calls = &calls, .text_relation = .exact }, "test");
    defer collected.deinit();
    try std.testing.expectEqual(@as(usize, 3), calls.items.len);
    try std.testing.expectEqual(@as(usize, 0), calls.items[0].dense_queries.len);
    try std.testing.expectEqual(default_aggregation_full_result_budget, calls.items[0].limit);
    try std.testing.expectEqual(@as(u32, 30), calls.items[1].limit);
    try std.testing.expectEqual(@as(u32, 30), calls.items[2].limit);
    try std.testing.expectEqual(@as(u32, 4), collected.total_hits);
    try std.testing.expectEqual(types.TotalHitsRelation.exact, collected.total_hits_relation);

    // An incomplete text matching set is still refused.
    calls.clearRetainingCapacity();
    try std.testing.expectError(error.QueryCandidateBudgetExceeded, collectAggregationFullResult(alloc, original, collection, Searcher{ .calls = &calls, .text_relation = .gte }, "test"));
}

test "aggregation domain concrete callback failure releases earlier component results" {
    const FailingSearcher = struct {
        calls: usize = 0,

        pub fn search(self: *@This(), alloc: std.mem.Allocator, _: types.SearchRequest) !types.SearchResult {
            self.calls += 1;
            if (self.calls == 3) return error.OutOfMemory;
            return aggregationDomainTestResult(alloc, &.{"shared"}, .exact);
        }
    };
    var searcher: FailingSearcher = .{};
    const original = aggregationDomainTestHybrid();
    const collection = aggregationCollectionRequest(original, default_aggregation_full_result_budget, 7);
    try std.testing.expectError(error.OutOfMemory, implementation.collectFullResult(std.testing.allocator, original, collection, implementation.SearchCapability.borrow(&searcher), "callback failure"));
    try std.testing.expectEqual(@as(usize, 3), searcher.calls);
}
