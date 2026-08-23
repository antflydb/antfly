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
const ant_json = @import("antfly-json");
const db_mod = @import("../storage/db/mod.zig");
const db_query_search = @import("../storage/db/query/search_exec.zig");
const hierarchy_navigation = @import("../storage/hierarchy_navigation.zig");
const runtime_schema_mod = @import("../storage/schema.zig");
const graph_paths = @import("../graph/paths.zig");
const graph_mod = @import("../graph/graph.zig");
const graph_query_mod = @import("../graph/query.zig");
const query_contract = @import("query_contract.zig");

pub const QueryResponse = query_contract.QueryResponse;
pub const QueryResponseMeta = query_contract.QueryResponseMeta;
pub const OwnedQueryRequest = query_contract.OwnedQueryRequest;
pub const PublicFilterQueryErrorKind = query_contract.PublicFilterQueryErrorKind;

pub const parseQueryRequest = query_contract.parseQueryRequest;
pub const parseGraphMetricRequestsAlloc = query_contract.parseGraphMetricRequestsAlloc;
pub const parsePublicQueryRequest = query_contract.parsePublicQueryRequest;
pub const parsePublicQueryRequestWithDeadline = query_contract.parsePublicQueryRequestWithDeadline;
pub const isPublicQueryValidationError = query_contract.isPublicQueryValidationError;
pub const publicFilterQueryErrorStatus = query_contract.publicFilterQueryErrorStatus;
pub const encodePublicFilterQueryErrorBodyAlloc = query_contract.encodePublicFilterQueryErrorBodyAlloc;
pub const parseAggregationRequestsJson = query_contract.parseAggregationRequestsJson;
pub const freeAggregationRequests = query_contract.freeAggregationRequests;
pub const encodeQueryResponses = query_contract.encodeQueryResponses;

const FakeSemanticResolver = struct {
    fn iface() query_contract.SemanticResolver {
        return .{
            .ptr = undefined,
            .vtable = &.{
                .resolve_dense_query = resolveDenseQuery,
            },
        };
    }

    fn resolveDenseQuery(
        _: *anyopaque,
        alloc: std.mem.Allocator,
        table_name: []const u8,
        index_name: []const u8,
        semantic_search: []const u8,
        embedding_template: ?[]const u8,
        limit: u32,
    ) !db_mod.types.DenseKnnQuery {
        try std.testing.expectEqualStrings("docs", table_name);
        try std.testing.expectEqualStrings("semantic_idx", index_name);
        try std.testing.expectEqualStrings("alpha concept", semantic_search);
        try std.testing.expect(embedding_template == null or std.mem.eql(u8, embedding_template.?, "{{remotePDF url=this}}"));
        const vector = try alloc.alloc(f32, 3);
        vector[0] = 0.25;
        vector[1] = 0.5;
        vector[2] = 0.75;
        return .{
            .vector = vector,
            .k = limit,
        };
    }
};

fn jsonStringifyAlloc(alloc: std.mem.Allocator, value: anytype) ![]u8 {
    var out: std.Io.Writer.Allocating = .init(alloc);
    defer out.deinit();
    try std.json.Stringify.value(value, .{}, &out.writer);
    return try alloc.dupe(u8, out.written());
}

pub fn mergeSearchResults(
    alloc: std.mem.Allocator,
    req: db_mod.types.SearchRequest,
    results: []const db_mod.types.SearchResult,
    offset: u32,
    limit: u32,
) !db_mod.types.SearchResult {
    return try mergeSearchResultsWithRuntimeSchema(alloc, req, results, offset, limit, null);
}

pub fn mergeSearchResultsWithRuntimeSchema(
    alloc: std.mem.Allocator,
    req: db_mod.types.SearchRequest,
    results: []const db_mod.types.SearchResult,
    offset: u32,
    limit: u32,
    runtime_schema: ?runtime_schema_mod.TableSchema,
) !db_mod.types.SearchResult {
    if (req.hierarchy_children != null) {
        return try mergeHierarchyChildrenSearchResults(alloc, req, results, offset, limit);
    }

    if (requestReturnsHierarchyUnitGroups(req)) {
        return try mergeHierarchyUnitSearchResults(alloc, req, results, offset, limit, runtime_schema);
    }

    return try mergeGenericSearchResultsWithRuntimeSchema(alloc, req, results, offset, limit, runtime_schema);
}

fn mergeGenericSearchResultsWithRuntimeSchema(
    alloc: std.mem.Allocator,
    req: db_mod.types.SearchRequest,
    results: []const db_mod.types.SearchResult,
    offset: u32,
    limit: u32,
    runtime_schema: ?runtime_schema_mod.TableSchema,
) !db_mod.types.SearchResult {
    var total_hits: u32 = 0;
    var total_hits_relation: db_mod.types.TotalHitsRelation = .exact;
    var has_graph_results = false;
    var has_graph_metric_results = false;
    var has_graph_metric_rerank_status = false;
    for (results) |result| {
        if (result.graph_results.len > 0) has_graph_results = true;
        if (result.graph_metric_results.len > 0) has_graph_metric_results = true;
        if (result.graph_metric_rerank_status != null) has_graph_metric_rerank_status = true;
        total_hits +|= result.total_hits;
        if (result.total_hits_relation == .gte) total_hits_relation = .gte;
    }
    if (req.graph_queries.len == 0 and has_graph_results) return error.UnsupportedQueryRequest;
    if (req.graph_metric_queries.len == 0 and has_graph_metric_results) return error.UnsupportedQueryRequest;

    var graph_metric_rerank_status = if (req.graph_metric_rerank != null or has_graph_metric_rerank_status)
        try mergeGraphMetricRerankStatus(alloc, req, results)
    else
        null;
    errdefer if (graph_metric_rerank_status) |*status| status.deinit(alloc);

    if (req.order_by.len > 0 or req.search_after.len > 0 or req.search_before.len > 0) {
        var merge_req = req;
        merge_req.offset = offset;
        merge_req.limit = limit;
        const sorted_merge = try db_query_search.mergeDistributedSortedSearchResultsWithRuntimeSchemaAlloc(alloc, merge_req, results, runtime_schema);
        const final_hits = sorted_merge.hits;
        errdefer {
            for (final_hits) |*hit| hit.deinit(alloc);
            if (final_hits.len > 0) alloc.free(final_hits);
        }
        for (final_hits) |hit| try validateSearchHitGraphMetricRerankPayload(req, graph_metric_rerank_status, hit);

        const graph_results = if (req.graph_queries.len > 0 or has_graph_results)
            try mergeGraphSearchResults(alloc, req, results)
        else
            @constCast((&[_]db_mod.types.GraphSearchResult{})[0..]);
        errdefer {
            for (graph_results) |*graph_result| graph_result.deinit(alloc);
            if (graph_results.len > 0) alloc.free(graph_results);
        }
        stripUnrequestedGraphSearchMetricStatuses(alloc, req, graph_results);
        const graph_metric_results = if (req.graph_metric_queries.len > 0 or has_graph_metric_results)
            try mergeGraphMetricResults(alloc, req, results)
        else
            @constCast((&[_]db_mod.types.GraphMetricResult{})[0..]);
        errdefer {
            for (graph_metric_results) |*metric_result| metric_result.deinit(alloc);
            if (graph_metric_results.len > 0) alloc.free(graph_metric_results);
        }

        if (results.len > 1) {
            clearMergedDocOrdinals(final_hits);
            for (graph_results) |*graph_result| clearMergedDocOrdinals(graph_result.hits);
        }

        return .{
            .alloc = alloc,
            .hits = final_hits,
            .total_hits = total_hits,
            .total_hits_relation = sorted_merge.total_hits_relation,
            .identity_read_generation = mergedSearchResultIdentityReadGeneration(req, results),
            .sort_profile = sorted_merge.sort_profile,
            .graph_results = graph_results,
            .graph_metric_results = graph_metric_results,
            .graph_metric_rerank_status = graph_metric_rerank_status,
        };
    }

    // Borrow payloads and retain only the requested top-(offset+limit) window.
    // An unbounded request still needs every reference, but ordinary pages no
    // longer allocate either payload copies or an O(total candidates) pointer
    // array at the coordinator.
    var merged_hits = std.ArrayListUnmanaged(*const db_mod.types.SearchHit).empty;
    defer merged_hits.deinit(alloc);
    var bounded_hits = std.PriorityQueue(
        *const db_mod.types.SearchHit,
        ScoreMergeOrder,
        searchHitRefWorstFirst,
    ).initContext(.{ .use_score = requestUsesScoreOrderedMerge(req) });
    defer bounded_hits.deinit(alloc);

    const score_ordered_merge = requestUsesScoreOrderedMerge(req);
    var candidate_count: usize = 0;
    for (results) |result| candidate_count = std.math.add(usize, candidate_count, result.hits.len) catch std.math.maxInt(usize);
    const retained_window = std.math.add(usize, @as(usize, offset), @as(usize, limit)) catch std.math.maxInt(usize);
    const use_bounded_window = limit != 0 and retained_window < candidate_count;
    if (use_bounded_window) try bounded_hits.ensureTotalCapacity(alloc, retained_window);
    for (results) |result| {
        for (result.hits) |*hit| {
            try validateSearchHitGraphMetricRerankPayload(req, graph_metric_rerank_status, hit.*);
            if (score_ordered_merge) try validateScoreOrderedMergeHit(hit.*);
            if (!use_bounded_window) {
                try merged_hits.append(alloc, hit);
                continue;
            }
            if (bounded_hits.items.len < retained_window) {
                try bounded_hits.push(alloc, hit);
                continue;
            }
            const worst = bounded_hits.peek().?;
            if (!searchHitRefComesBefore(.{ .use_score = score_ordered_merge }, hit, worst)) continue;
            _ = bounded_hits.pop();
            try bounded_hits.push(alloc, hit);
        }
    }

    const candidate_refs = if (use_bounded_window) bounded_hits.items else merged_hits.items;
    std.sort.pdq(*const db_mod.types.SearchHit, candidate_refs, ScoreMergeOrder{
        .use_score = score_ordered_merge,
    }, searchHitRefComesBefore);

    const start: usize = @min(offset, candidate_refs.len);
    const max_count: usize = if (limit == 0) candidate_refs.len - start else @min(limit, candidate_refs.len - start);
    const end = start + max_count;

    var final_hits = try alloc.alloc(db_mod.types.SearchHit, max_count);
    var moved: usize = 0;
    errdefer {
        for (final_hits[0..moved]) |*hit| hit.deinit(alloc);
        alloc.free(final_hits);
    }

    for (candidate_refs[start..end], 0..) |hit, i| {
        final_hits[i] = try hit.clone(alloc);
        moved += 1;
    }

    const graph_results = if (req.graph_queries.len > 0 or has_graph_results)
        try mergeGraphSearchResults(alloc, req, results)
    else
        @constCast((&[_]db_mod.types.GraphSearchResult{})[0..]);
    errdefer {
        for (graph_results) |*graph_result| graph_result.deinit(alloc);
        if (graph_results.len > 0) alloc.free(graph_results);
    }
    stripUnrequestedGraphSearchMetricStatuses(alloc, req, graph_results);
    const graph_metric_results = if (req.graph_metric_queries.len > 0 or has_graph_metric_results)
        try mergeGraphMetricResults(alloc, req, results)
    else
        @constCast((&[_]db_mod.types.GraphMetricResult{})[0..]);
    errdefer {
        for (graph_metric_results) |*metric_result| metric_result.deinit(alloc);
        if (graph_metric_results.len > 0) alloc.free(graph_metric_results);
    }

    if (results.len > 1) {
        clearMergedDocOrdinals(final_hits);
        for (graph_results) |*graph_result| clearMergedDocOrdinals(graph_result.hits);
    }

    return .{
        .alloc = alloc,
        .hits = final_hits,
        .total_hits = total_hits,
        .total_hits_relation = total_hits_relation,
        .identity_read_generation = mergedSearchResultIdentityReadGeneration(req, results),
        .graph_results = graph_results,
        .graph_metric_results = graph_metric_results,
        .graph_metric_rerank_status = graph_metric_rerank_status,
    };
}

fn validateSearchHitGraphMetricRerankPayload(
    req: db_mod.types.SearchRequest,
    rerank_status: ?db_mod.types.GraphMetricStatus,
    hit: db_mod.types.SearchHit,
) !void {
    const rerank = req.graph_metric_rerank orelse {
        if (hit.score_details != null) return error.UnsupportedQueryRequest;
        return;
    };

    const hit_score = hit.score orelse return error.UnsupportedQueryRequest;
    if (!std.math.isFinite(hit_score)) return error.UnsupportedQueryRequest;
    const details = hit.score_details orelse return;
    if (!std.mem.eql(u8, details.index_name, rerank.index_name)) return error.UnsupportedQueryRequest;
    if (!std.mem.eql(u8, details.metric_name, rerank.metric_name)) return error.UnsupportedQueryRequest;
    const status = rerank_status orelse return error.UnsupportedQueryRequest;
    if (details.published_generation == 0 or details.published_generation != status.published_generation) return error.UnsupportedQueryRequest;
    if (!std.math.isFinite(details.base_score) or !std.math.isFinite(details.base_weight)) return error.UnsupportedQueryRequest;
    if (@abs(details.base_weight - rerank.base_weight) > 0.000001) return error.UnsupportedQueryRequest;
    if (details.metric_score) |score| {
        if (!std.math.isFinite(score) or details.missing_score_used) return error.UnsupportedQueryRequest;
        if (@abs(score - details.metric_score_used) > 0.000001) return error.UnsupportedQueryRequest;
    } else {
        if (!details.missing_score_used) return error.UnsupportedQueryRequest;
        if (@abs(details.metric_score_used - rerank.missing_score) > 0.000001) return error.UnsupportedQueryRequest;
    }
    if (!std.math.isFinite(details.metric_score_used) or !std.math.isFinite(details.metric_weight)) return error.UnsupportedQueryRequest;
    if (@abs(details.metric_weight - rerank.weight) > 0.000001) return error.UnsupportedQueryRequest;
    if (!std.math.isFinite(details.final_score)) return error.UnsupportedQueryRequest;
    const expected_final_score = clampF64ToF32(details.base_score * details.base_weight + details.metric_score_used * details.metric_weight);
    if (@abs(@as(f64, expected_final_score) - details.final_score) > 0.000001) return error.UnsupportedQueryRequest;
    if (@abs(@as(f64, hit_score) - details.final_score) > 0.000001) return error.UnsupportedQueryRequest;
}

fn clampF64ToF32(value: f64) f32 {
    const max = std.math.floatMax(f32);
    if (value > max) return max;
    if (value < -max) return -max;
    return @floatCast(value);
}

fn searchHitRefComesBefore(
    order: ScoreMergeOrder,
    a: *const db_mod.types.SearchHit,
    b: *const db_mod.types.SearchHit,
) bool {
    if (order.use_score) {
        const a_score = a.score.?;
        const b_score = b.score.?;
        if (a_score != b_score) return a_score > b_score;
    }
    return std.mem.order(u8, a.id, b.id) == .lt;
}

fn searchHitRefWorstFirst(
    order: ScoreMergeOrder,
    a: *const db_mod.types.SearchHit,
    b: *const db_mod.types.SearchHit,
) std.math.Order {
    if (searchHitRefComesBefore(order, a, b)) return .gt;
    if (searchHitRefComesBefore(order, b, a)) return .lt;
    return .eq;
}

fn requestReturnsHierarchyUnitGroups(req: db_mod.types.SearchRequest) bool {
    return req.hierarchy_group_level == .unit and
        (req.return_mode == .unit or req.return_mode == .unit_with_chunks);
}

fn mergeHierarchyUnitSearchResults(
    alloc: std.mem.Allocator,
    req: db_mod.types.SearchRequest,
    results: []const db_mod.types.SearchResult,
    offset: u32,
    limit: u32,
    runtime_schema: ?runtime_schema_mod.TableSchema,
) !db_mod.types.SearchResult {
    if (req.order_by.len > 0 or req.search_after.len > 0 or req.search_before.len > 0) {
        return error.UnsupportedQueryRequest;
    }
    // A single shard already owns an exact local distinct count. Coalescing is
    // only needed once multiple shard-local unit sets form a distributed union.
    if (results.len <= 1) {
        return try mergeGenericSearchResultsWithRuntimeSchema(alloc, req, results, offset, limit, runtime_schema);
    }

    const page_window_u32 = std.math.add(u32, offset, limit) catch
        return error.QueryCandidateBudgetExceeded;
    if (page_window_u32 == 0 or page_window_u32 > db_mod.types.max_canonical_hierarchy_total_matches) {
        return error.QueryCandidateBudgetExceeded;
    }
    const page_window: usize = @intCast(page_window_u32);

    var candidates = std.ArrayListUnmanaged(db_mod.types.SearchHit).empty;
    errdefer {
        for (candidates.items) |*hit| hit.deinit(alloc);
        candidates.deinit(alloc);
    }
    var by_id = std.StringHashMapUnmanaged(usize).empty;
    defer by_id.deinit(alloc);
    const CandidateOrigin = struct {
        result_index: usize,
        hit_index: usize,
    };
    var origins = std.ArrayListUnmanaged(CandidateOrigin).empty;
    defer origins.deinit(alloc);

    const cursors = try alloc.alloc(usize, results.len);
    defer alloc.free(cursors);
    @memset(cursors, 0);

    var largest_shard_total: u32 = 0;
    var every_shard_complete = true;
    for (results) |result| {
        largest_shard_total = @max(largest_shard_total, result.total_hits);
        if (result.total_hits_relation != .exact or result.total_hits != result.hits.len) {
            every_shard_complete = false;
        }
        try validateHierarchyUnitShardOrder(req, result.hits);
    }

    // Every shard has already produced its local top window. Merge those
    // sorted streams directly so coordinator memory depends on the requested
    // page, not on shard fanout. The first occurrence of an ID is its globally
    // best representative; a second bounded pass below aggregates matches for
    // only the selected IDs.
    var exhausted = false;
    while (candidates.items.len < page_window) {
        var best_result_index: ?usize = null;
        for (results, 0..) |result, result_index| {
            if (cursors[result_index] >= result.hits.len) continue;
            if (best_result_index) |current_best| {
                const candidate = result.hits[cursors[result_index]];
                const best = results[current_best].hits[cursors[current_best]];
                if (hierarchyUnitHitLessThan(req, candidate, best)) best_result_index = result_index;
            } else {
                best_result_index = result_index;
            }
        }
        const result_index = best_result_index orelse {
            exhausted = true;
            break;
        };
        const hit_index = cursors[result_index];
        cursors[result_index] += 1;
        const hit = results[result_index].hits[hit_index];
        if (by_id.contains(hit.id)) continue;

        // Reserve every fallible container operation before cloning so the
        // cloned hit has one unambiguous owner even under allocation failure.
        try candidates.ensureUnusedCapacity(alloc, 1);
        try origins.ensureUnusedCapacity(alloc, 1);
        try by_id.ensureUnusedCapacity(alloc, 1);
        const cloned = try hit.clone(alloc);
        candidates.appendAssumeCapacity(cloned);
        origins.appendAssumeCapacity(.{ .result_index = result_index, .hit_index = hit_index });
        const candidate_index = candidates.items.len - 1;
        by_id.putAssumeCapacity(candidates.items[candidate_index].id, candidate_index);
    }

    // Expansion results may contain the same selected unit on several chunk
    // owners. Aggregate only those bounded selected groups, including all of
    // their shard-local top matches, without retaining non-page candidates.
    for (results, 0..) |result, result_index| {
        for (result.hits, 0..) |hit, hit_index| {
            const candidate_index = by_id.get(hit.id) orelse continue;
            const origin = origins.items[candidate_index];
            if (origin.result_index == result_index and origin.hit_index == hit_index) continue;
            try mergeHierarchyUnitHit(alloc, req, &candidates.items[candidate_index], hit);
        }
    }

    const observed_total: u32 = @intCast(candidates.items.len);
    const exact_union_observed = exhausted and every_shard_complete;
    const total_hits = if (exact_union_observed)
        observed_total
    else
        @max(observed_total, largest_shard_total);
    const total_hits_relation: db_mod.types.TotalHitsRelation = if (exact_union_observed) .exact else .gte;
    const identity_generation = mergedSearchResultIdentityReadGeneration(req, results);

    var coalesced = db_mod.types.SearchResult{
        .alloc = alloc,
        .hits = try candidates.toOwnedSlice(alloc),
        .total_hits = total_hits,
        .total_hits_relation = total_hits_relation,
        .identity_read_generation = identity_generation,
    };
    candidates = .empty;
    defer coalesced.deinit();

    var merged = try mergeGenericSearchResultsWithRuntimeSchema(
        alloc,
        req,
        &.{coalesced},
        offset,
        limit,
        runtime_schema,
    );
    errdefer merged.deinit();
    merged.total_hits = total_hits;
    merged.total_hits_relation = total_hits_relation;
    merged.identity_read_generation = identity_generation;

    var has_graph_results = false;
    for (results) |result| {
        if (result.graph_results.len > 0) {
            has_graph_results = true;
            break;
        }
    }
    if (has_graph_results) {
        const graph_results = try mergeGraphSearchResults(alloc, req, results);
        errdefer {
            for (graph_results) |*graph_result| graph_result.deinit(alloc);
            if (graph_results.len > 0) alloc.free(graph_results);
        }
        stripUnrequestedGraphSearchMetricStatuses(alloc, req, graph_results);
        merged.graph_results = graph_results;
    }
    if (results.len > 1) {
        clearMergedDocOrdinals(merged.hits);
        for (merged.graph_results) |*graph_result| clearMergedDocOrdinals(graph_result.hits);
    }
    return merged;
}

fn validateHierarchyUnitShardOrder(
    req: db_mod.types.SearchRequest,
    hits: []const db_mod.types.SearchHit,
) !void {
    const score_ordered = requestUsesScoreOrderedMerge(req);
    for (hits, 0..) |hit, i| {
        if (score_ordered) validateScoreOrderedMergeHit(hit) catch
            return error.StorageReadTemporarilyUnavailable;
        if (i > 0 and hierarchyUnitHitLessThan(req, hit, hits[i - 1])) {
            return error.StorageReadTemporarilyUnavailable;
        }
    }
}

fn hierarchyUnitHitLessThan(
    req: db_mod.types.SearchRequest,
    left: db_mod.types.SearchHit,
    right: db_mod.types.SearchHit,
) bool {
    if (requestUsesScoreOrderedMerge(req)) {
        if (left.score.? != right.score.?) return left.score.? > right.score.?;
    }
    return std.mem.order(u8, left.id, right.id) == .lt;
}

fn mergeHierarchyUnitHit(
    alloc: std.mem.Allocator,
    req: db_mod.types.SearchRequest,
    existing: *db_mod.types.SearchHit,
    incoming: db_mod.types.SearchHit,
) !void {
    try validateMatchingHierarchyUnitIdentity(existing.*, incoming);

    if (existing.stored_data != null and incoming.stored_data != null and
        !std.mem.eql(u8, existing.stored_data.?, incoming.stored_data.?))
    {
        return error.StorageReadTemporarilyUnavailable;
    }
    if (existing.stored_data == null and incoming.stored_data != null) {
        existing.stored_data = try alloc.dupe(u8, incoming.stored_data.?);
    }

    // Merge descendant ownership first. Once ranking metadata is transferred
    // below, no later fallible operation may unwind through both owners.
    try mergeHierarchyUnitChunks(alloc, existing, incoming.chunk_hits, req.max_chunks_per_parent);

    if (requestUsesHierarchyUnitScoreOrdering(req) and hierarchyUnitHitHasBetterScore(incoming, existing.*)) {
        const index_scores = try db_mod.types.cloneIndexScores(alloc, incoming.index_scores);
        errdefer db_mod.types.freeIndexScores(alloc, index_scores);
        const sort_values: ?[]std.json.Value = if (req.order_by.len > 0)
            try db_mod.types.cloneJsonValues(alloc, incoming.sort_values)
        else
            null;
        db_mod.types.freeIndexScores(alloc, existing.index_scores);
        if (sort_values) |values| {
            db_mod.types.freeJsonValues(alloc, existing.sort_values);
            existing.sort_values = values;
        }
        existing.index_scores = index_scores;
        existing.score = incoming.score;
        existing.distance = incoming.distance;
    }
}

fn validateMatchingHierarchyUnitIdentity(
    existing: db_mod.types.SearchHit,
    incoming: db_mod.types.SearchHit,
) !void {
    // These hits were produced by independent storage placements, not supplied
    // by the user. Conflicting identities indicate a transient topology/revision
    // seam and must remain retryable instead of being exposed as a client 400.
    if (!std.mem.eql(u8, existing.id, incoming.id)) return error.StorageReadTemporarilyUnavailable;
    const left = existing.artifact_ref orelse return error.StorageReadTemporarilyUnavailable;
    const right = incoming.artifact_ref orelse return error.StorageReadTemporarilyUnavailable;
    if (left.kind != right.kind or
        !std.mem.eql(u8, left.document_id, right.document_id) or
        !std.mem.eql(u8, left.name, right.name) or
        !optionalBytesEqual(left.unit_id, right.unit_id))
    {
        return error.StorageReadTemporarilyUnavailable;
    }
}

fn optionalBytesEqual(left: ?[]u8, right: ?[]u8) bool {
    if (left == null or right == null) return left == null and right == null;
    return std.mem.eql(u8, left.?, right.?);
}

fn hierarchyUnitHitHasBetterScore(candidate: db_mod.types.SearchHit, current: db_mod.types.SearchHit) bool {
    if (candidate.score == null) return false;
    if (current.score == null) return true;
    return candidate.score.? > current.score.?;
}

fn requestUsesHierarchyUnitScoreOrdering(req: db_mod.types.SearchRequest) bool {
    return requestUsesScoreOrderedMerge(req) or
        (req.order_by.len > 0 and std.mem.eql(u8, req.order_by[0].field, "_score"));
}

fn mergeHierarchyUnitChunks(
    alloc: std.mem.Allocator,
    existing: *db_mod.types.SearchHit,
    incoming: []const db_mod.types.ChunkHit,
    requested_limit: u32,
) !void {
    const combined_len = std.math.add(usize, existing.chunk_hits.len, incoming.len) catch
        return error.QueryCandidateBudgetExceeded;
    if (combined_len > db_mod.types.max_canonical_hierarchy_total_matches) {
        return error.QueryCandidateBudgetExceeded;
    }

    const refs = try alloc.alloc(*const db_mod.types.ChunkHit, combined_len);
    defer alloc.free(refs);
    var ref_index: usize = 0;
    for (existing.chunk_hits) |*chunk| {
        refs[ref_index] = chunk;
        ref_index += 1;
    }
    for (incoming) |*chunk| {
        refs[ref_index] = chunk;
        ref_index += 1;
    }
    std.sort.pdq(*const db_mod.types.ChunkHit, refs, {}, hierarchyChunkHitPtrLessThan);

    const limit: usize = @min(@as(usize, @intCast(requested_limit)), combined_len);
    var kept = std.ArrayListUnmanaged(db_mod.types.ChunkHit).empty;
    errdefer {
        for (kept.items) |*chunk| chunk.deinit(alloc);
        kept.deinit(alloc);
    }
    try kept.ensureTotalCapacity(alloc, limit);
    for (refs) |chunk| {
        if (kept.items.len >= limit) break;
        var duplicate = false;
        for (kept.items) |prior| {
            if (std.mem.eql(u8, prior.id, chunk.id)) {
                duplicate = true;
                break;
            }
        }
        if (!duplicate) kept.appendAssumeCapacity(try chunk.clone(alloc));
    }
    const owned = try kept.toOwnedSlice(alloc);
    for (existing.chunk_hits) |*chunk| chunk.deinit(alloc);
    if (existing.chunk_hits.len > 0) alloc.free(existing.chunk_hits);
    existing.chunk_hits = owned;
}

fn hierarchyChunkHitPtrLessThan(
    _: void,
    left: *const db_mod.types.ChunkHit,
    right: *const db_mod.types.ChunkHit,
) bool {
    if (left.score != null and right.score != null and left.score.? != right.score.?) {
        return left.score.? > right.score.?;
    }
    if (left.score != null and right.score == null) return true;
    if (left.score == null and right.score != null) return false;
    return std.mem.order(u8, left.id, right.id) == .lt;
}

fn hierarchyNavigationHitTuple(hit: db_mod.types.SearchHit) !struct { position: []const u8, id: []const u8 } {
    if (hit.sort_values.len != 2 or hit.sort_values[0] != .string or hit.sort_values[1] != .string) {
        return error.StorageReadTemporarilyUnavailable;
    }
    if (!std.mem.eql(u8, hit.sort_values[1].string, hit.id)) return error.StorageReadTemporarilyUnavailable;
    return .{ .position = hit.sort_values[0].string, .id = hit.sort_values[1].string };
}

fn hierarchyNavigationTupleOrder(
    left_position: []const u8,
    left_id: []const u8,
    right_position: []const u8,
    right_id: []const u8,
) std.math.Order {
    const position_order = std.mem.order(u8, left_position, right_position);
    if (position_order != .eq) return position_order;
    return std.mem.order(u8, left_id, right_id);
}

fn hierarchyNavigationHitLessThan(_: void, left: db_mod.types.SearchHit, right: db_mod.types.SearchHit) bool {
    const left_tuple = hierarchyNavigationHitTuple(left) catch unreachable;
    const right_tuple = hierarchyNavigationHitTuple(right) catch unreachable;
    return hierarchyNavigationTupleOrder(left_tuple.position, left_tuple.id, right_tuple.position, right_tuple.id) == .lt;
}

fn mergeHierarchyChildrenSearchResults(
    alloc: std.mem.Allocator,
    req: db_mod.types.SearchRequest,
    results: []const db_mod.types.SearchResult,
    offset: u32,
    limit: u32,
) !db_mod.types.SearchResult {
    if (req.order_by.len != 2 or
        !std.mem.eql(u8, req.order_by[0].field, "_hierarchy.position") or
        req.order_by[0].desc or
        !std.mem.eql(u8, req.order_by[1].field, "_id") or
        req.order_by[1].desc or
        req.search_before.len != 0)
    {
        return error.InvalidQueryRequest;
    }
    if (req.search_after.len > 0 and
        (req.search_after.len != 2 or req.search_after[0] != .string or req.search_after[1] != .string))
    {
        return error.InvalidQueryRequest;
    }

    var candidates = std.ArrayListUnmanaged(db_mod.types.SearchHit).empty;
    errdefer {
        for (candidates.items) |*hit| hit.deinit(alloc);
        candidates.deinit(alloc);
    }
    var by_id = std.StringHashMapUnmanaged(usize).empty;
    defer by_id.deinit(alloc);
    var total_hits: u32 = 0;
    var total_hits_relation: db_mod.types.TotalHitsRelation = .exact;

    for (results) |result| {
        // Child planning is parent-owned. During a topology handoff two
        // placements may briefly expose the same plan, so use the largest
        // logical total and deduplicate identical unit IDs instead of summing.
        total_hits = @max(total_hits, result.total_hits);
        if (result.total_hits_relation == .gte) total_hits_relation = .gte;
        for (result.hits) |hit| {
            const tuple = try hierarchyNavigationHitTuple(hit);
            const gop = try by_id.getOrPut(alloc, hit.id);
            if (gop.found_existing) {
                const existing = &candidates.items[gop.value_ptr.*];
                const existing_tuple = try hierarchyNavigationHitTuple(existing.*);
                if (hierarchyNavigationTupleOrder(tuple.position, tuple.id, existing_tuple.position, existing_tuple.id) != .eq) {
                    // Duplicate plans are expected briefly during ownership
                    // handoff. Disagreement between server-generated positions
                    // means the placements observed different hierarchy revisions;
                    // retry rather than blaming the request with a 400.
                    return error.StorageReadTemporarilyUnavailable;
                }
                if (existing.stored_data == null and hit.stored_data != null) {
                    var richer = try hit.clone(alloc);
                    errdefer richer.deinit(alloc);
                    existing.deinit(alloc);
                    existing.* = richer;
                    gop.key_ptr.* = existing.id;
                }
                continue;
            }
            if (candidates.items.len >= db_mod.types.max_canonical_hierarchy_total_matches) {
                return error.QueryCandidateBudgetExceeded;
            }
            const cloned = try hit.clone(alloc);
            errdefer {
                var owned = cloned;
                owned.deinit(alloc);
            }
            try candidates.append(alloc, cloned);
            gop.key_ptr.* = candidates.items[candidates.items.len - 1].id;
            gop.value_ptr.* = candidates.items.len - 1;
        }
    }

    // Shard-local absence is expected because hierarchy planning is parent-
    // owned while distributed queries fan out to every table group. Only the
    // outer coordinator can conclude that no shard observed the parent plan.
    // Preserve direct/internal shard behavior by leaving deferred requests as
    // empty partial plans for their caller to merge.
    if (!req.defer_hierarchy_child_hydration and
        req.search_after.len > 0 and
        total_hits == 0 and
        candidates.items.len == 0)
    {
        _ = hierarchy_navigation.parsePosition(req.search_after[0].string) catch |err| switch (err) {
            error.HierarchyNavigationPositionVersionStale => return error.HierarchyCursorStale,
            error.InvalidHierarchyNavigationPosition => return error.InvalidQueryRequest,
        };
        return error.HierarchyCursorStale;
    }

    std.mem.sort(db_mod.types.SearchHit, candidates.items, {}, hierarchyNavigationHitLessThan);
    const cursor_position = if (req.search_after.len > 0) req.search_after[0].string else null;
    const cursor_id = if (req.search_after.len > 0) req.search_after[1].string else null;
    const skip: usize = if (req.search_after.len > 0) 0 else @intCast(offset);
    var admitted: usize = 0;
    var page = std.ArrayListUnmanaged(db_mod.types.SearchHit).empty;
    errdefer {
        for (page.items) |*hit| hit.deinit(alloc);
        page.deinit(alloc);
    }
    for (candidates.items) |*hit| {
        const tuple = try hierarchyNavigationHitTuple(hit.*);
        if (cursor_position) |position| {
            if (hierarchyNavigationTupleOrder(tuple.position, tuple.id, position, cursor_id.?) != .gt) continue;
        }
        if (admitted < skip) {
            admitted += 1;
            continue;
        }
        if (page.items.len >= @as(usize, @intCast(limit))) break;
        var cloned = try hit.clone(alloc);
        errdefer cloned.deinit(alloc);
        try page.append(alloc, cloned);
        admitted += 1;
    }

    // Candidate windows are bounded by the public traversal limit; cloning the
    // selected page keeps ownership simple across duplicate-placement merges.
    for (candidates.items) |*hit| {
        hit.deinit(alloc);
    }
    candidates.deinit(alloc);
    candidates = .empty;

    return .{
        .alloc = alloc,
        .hits = try page.toOwnedSlice(alloc),
        .total_hits = total_hits,
        .total_hits_relation = total_hits_relation,
    };
}

fn mergedSearchResultIdentityReadGeneration(
    req: db_mod.types.SearchRequest,
    results: []const db_mod.types.SearchResult,
) ?u64 {
    if (req.identity_read_generation) |generation| return generation;
    var common: ?u64 = null;
    for (results) |result| {
        const generation = result.identity_read_generation orelse return null;
        if (common) |existing| {
            if (existing != generation) return null;
        } else {
            common = generation;
        }
    }
    return common;
}

fn clearMergedDocOrdinals(hits: []db_mod.types.SearchHit) void {
    for (hits) |*hit| {
        hit.doc_ordinal = null;
        hit.native_text_doc_id = null;
    }
}

fn validateScoreOrderedMergeHit(hit: db_mod.types.SearchHit) !void {
    const score = hit.score orelse return error.InvalidQueryRequest;
    if (!std.math.isFinite(score)) return error.InvalidQueryRequest;
}

const ScoreMergeOrder = struct {
    use_score: bool,
};

fn requestUsesScoreOrderedMerge(req: db_mod.types.SearchRequest) bool {
    if (req.order_by.len > 0) return false;
    if (searchRequestHasScoreBearingSource(req)) return true;
    return req.dense != null or
        req.sparse != null or
        req.dense_queries.len > 0 or
        req.sparse_queries.len > 0 or
        req.merge_config != null or
        req.reranker != null or
        req.graph_metric_rerank != null;
}

fn searchRequestHasScoreBearingSource(req: db_mod.types.SearchRequest) bool {
    if (req.full_text) |query| {
        if (textQueryIsScoreBearing(query)) return true;
    }
    for (req.full_text_queries) |query| {
        if (textQueryIsScoreBearing(query.query)) return true;
    }
    return queryIsScoreBearing(req.query);
}

fn queryIsScoreBearing(query: db_mod.types.Query) bool {
    return switch (query) {
        .phrase,
        .multi_phrase,
        .term,
        .match,
        .match_phrase,
        .fuzzy,
        .prefix,
        .wildcard,
        .regexp,
        => true,
        .dense_knn,
        .sparse_knn,
        => true,
        .match_none,
        .match_all,
        .numeric_range,
        .date_range,
        .doc_id,
        .bool_field,
        .geo_distance,
        .geo_bbox,
        .term_range,
        .ip_range,
        .geo_shape,
        .graph,
        => false,
    };
}

fn textQueryIsScoreBearing(query: db_mod.types.TextQuery) bool {
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

fn textBoolQueryIsScoreBearing(query: db_mod.types.TextBoolQuery) bool {
    for (query.must) |child| {
        if (textQueryIsScoreBearing(child)) return true;
    }
    for (query.should) |child| {
        if (textQueryIsScoreBearing(child)) return true;
    }
    return false;
}

const GraphSearchResultBuilder = struct {
    name: []u8,
    nodes: std.ArrayListUnmanaged(graph_query_mod.GraphResultNode) = .empty,
    paths: std.ArrayListUnmanaged(db_mod.types.GraphPath) = .empty,
    matches: std.ArrayListUnmanaged(db_mod.types.GraphPatternMatch) = .empty,
    hits: std.ArrayListUnmanaged(db_mod.types.SearchHit) = .empty,
    metric_status: std.ArrayListUnmanaged(db_mod.types.GraphMetricStatus) = .empty,
    total_hits: u32 = 0,

    fn deinit(self: *GraphSearchResultBuilder, alloc: std.mem.Allocator) void {
        if (self.name.len > 0) alloc.free(self.name);
        for (self.nodes.items) |*node| node.deinit(alloc);
        self.nodes.deinit(alloc);
        for (self.paths.items) |path| graph_paths.freePath(alloc, path);
        self.paths.deinit(alloc);
        for (self.matches.items) |*match| match.deinit(alloc);
        self.matches.deinit(alloc);
        for (self.hits.items) |*hit| hit.deinit(alloc);
        self.hits.deinit(alloc);
        for (self.metric_status.items) |*status| status.deinit(alloc);
        self.metric_status.deinit(alloc);
        self.* = undefined;
    }

    fn toOwned(self: *GraphSearchResultBuilder, alloc: std.mem.Allocator) !db_mod.types.GraphSearchResult {
        const nodes = try self.nodes.toOwnedSlice(alloc);
        errdefer {
            for (nodes) |*node| node.deinit(alloc);
            if (nodes.len > 0) alloc.free(nodes);
        }
        const paths = try self.paths.toOwnedSlice(alloc);
        errdefer {
            for (paths) |path| graph_paths.freePath(alloc, path);
            if (paths.len > 0) alloc.free(paths);
        }
        const matches = try self.matches.toOwnedSlice(alloc);
        errdefer {
            for (matches) |*match| match.deinit(alloc);
            if (matches.len > 0) alloc.free(matches);
        }
        const hits = try self.hits.toOwnedSlice(alloc);
        errdefer {
            for (hits) |*hit| hit.deinit(alloc);
            if (hits.len > 0) alloc.free(hits);
        }

        const name = self.name;
        self.name = &.{};
        return .{
            .name = name,
            .nodes = nodes,
            .paths = paths,
            .matches = matches,
            .hits = hits,
            .total_hits = self.total_hits,
            .metric_status = try self.metric_status.toOwnedSlice(alloc),
        };
    }
};

const GraphMetricResultBuilder = struct {
    name: []u8,
    index_name: []u8,
    metric_name: []u8,
    scores: std.ArrayListUnmanaged(db_mod.types.GraphMetricScore) = .empty,
    status: ?db_mod.types.GraphMetricStatus = null,

    fn deinit(self: *GraphMetricResultBuilder, alloc: std.mem.Allocator) void {
        if (self.name.len > 0) alloc.free(self.name);
        if (self.index_name.len > 0) alloc.free(self.index_name);
        if (self.metric_name.len > 0) alloc.free(self.metric_name);
        for (self.scores.items) |*score| score.deinit(alloc);
        self.scores.deinit(alloc);
        if (self.status) |*status| status.deinit(alloc);
        self.* = undefined;
    }

    fn toOwned(self: *GraphMetricResultBuilder, alloc: std.mem.Allocator, top_k: u32) !db_mod.types.GraphMetricResult {
        std.sort.pdq(db_mod.types.GraphMetricScore, self.scores.items, {}, struct {
            fn lessThan(_: void, a: db_mod.types.GraphMetricScore, b: db_mod.types.GraphMetricScore) bool {
                if (a.score != b.score) return a.score > b.score;
                return std.mem.order(u8, a.node, b.node) == .lt;
            }
        }.lessThan);

        const count = if (top_k == 0)
            self.scores.items.len
        else
            @min(self.scores.items.len, @as(usize, @intCast(top_k)));
        const scores = try alloc.alloc(db_mod.types.GraphMetricScore, count);
        var initialized: usize = 0;
        errdefer {
            for (scores[0..initialized]) |*score| score.deinit(alloc);
            alloc.free(scores);
        }
        for (self.scores.items[0..count], 0..) |score, i| {
            scores[i] = .{
                .node = try alloc.dupe(u8, score.node),
                .score = score.score,
            };
            initialized += 1;
        }

        return .{
            .name = self.name,
            .index_name = self.index_name,
            .metric_name = self.metric_name,
            .scores = scores,
            .status = self.status orelse return error.InvalidQueryResponse,
        };
    }
};

fn mergeGraphMetricResults(
    alloc: std.mem.Allocator,
    req: db_mod.types.SearchRequest,
    results: []const db_mod.types.SearchResult,
) ![]db_mod.types.GraphMetricResult {
    try validateRequestedGraphMetricFanIn(req, results);

    var builders = std.ArrayListUnmanaged(GraphMetricResultBuilder).empty;
    defer {
        for (builders.items) |*builder| builder.deinit(alloc);
        builders.deinit(alloc);
    }

    for (results) |result| {
        for (result.graph_metric_results) |metric_result| {
            const idx = blk: {
                for (builders.items, 0..) |builder, i| {
                    if (std.mem.eql(u8, builder.name, metric_result.name)) break :blk i;
                }
                try builders.append(alloc, .{
                    .name = try alloc.dupe(u8, metric_result.name),
                    .index_name = try alloc.dupe(u8, metric_result.index_name),
                    .metric_name = try alloc.dupe(u8, metric_result.metric_name),
                });
                break :blk builders.items.len - 1;
            };
            var builder = &builders.items[idx];
            try ensureGraphMetricResultComparable(builder, metric_result);
            try validateGraphMetricScoreNodesUnique(builder.scores.items, metric_result.scores);
            for (metric_result.scores) |score| {
                try builder.scores.append(alloc, .{
                    .node = try alloc.dupe(u8, score.node),
                    .score = score.score,
                });
            }
            if (builder.status) |*status| {
                try mergeGraphMetricStatusInto(alloc, status, metric_result.status);
            } else {
                builder.status = try cloneGraphMetricStatus(alloc, metric_result.status);
            }
        }
    }

    const merged = try alloc.alloc(db_mod.types.GraphMetricResult, builders.items.len);
    var initialized: usize = 0;
    errdefer {
        for (merged[0..initialized]) |*metric_result| metric_result.deinit(alloc);
        alloc.free(merged);
    }
    for (builders.items, 0..) |*builder, i| {
        merged[i] = try builder.toOwned(alloc, graphMetricQueryTopK(req, builder.name));
        initialized += 1;
        builder.name = &.{};
        builder.index_name = &.{};
        builder.metric_name = &.{};
        builder.status = null;
    }
    return merged;
}

fn validateRequestedGraphMetricFanIn(
    req: db_mod.types.SearchRequest,
    results: []const db_mod.types.SearchResult,
) !void {
    if (req.graph_metric_queries.len == 0) return;
    try validateGraphMetricQueryNamesUnique(req.graph_metric_queries);

    for (results) |result| {
        for (result.graph_metric_results) |metric_result| {
            if (!graphMetricQueryRequested(req, metric_result.name)) return error.UnsupportedQueryRequest;
        }
        for (req.graph_metric_queries) |query| {
            var found = false;
            for (result.graph_metric_results) |metric_result| {
                if (!std.mem.eql(u8, metric_result.name, query.name)) continue;
                if (found) return error.UnsupportedQueryRequest;
                found = true;
                if (!std.mem.eql(u8, metric_result.index_name, query.query.index_name)) return error.UnsupportedQueryRequest;
                if (!std.mem.eql(u8, metric_result.metric_name, query.query.metric_name)) return error.UnsupportedQueryRequest;
                if (!std.mem.eql(u8, metric_result.status.name, query.query.metric_name)) return error.UnsupportedQueryRequest;
                if (metric_result.status.published_generation == 0) return error.UnsupportedQueryRequest;
                try validateGraphMetricPublishedStatus(metric_result.status);
                try validateGraphMetricFreshness(query.query.freshness, metric_result.status);
            }
            if (!found) return error.UnsupportedQueryRequest;
        }
        try validateRequestedHitsPairGraphMetricFanIn(req, result);
    }
}

fn validateGraphMetricQueryNamesUnique(queries: []const db_mod.types.NamedGraphMetricQuery) !void {
    for (queries, 0..) |query, i| {
        for (queries[0..i]) |previous| {
            if (std.mem.eql(u8, previous.name, query.name)) return error.UnsupportedQueryRequest;
        }
    }
}

fn graphMetricQueryRequested(req: db_mod.types.SearchRequest, query_name: []const u8) bool {
    for (req.graph_metric_queries) |query| {
        if (std.mem.eql(u8, query.name, query_name)) return true;
    }
    return false;
}

fn validateRequestedHitsPairGraphMetricFanIn(
    req: db_mod.types.SearchRequest,
    result: db_mod.types.SearchResult,
) !void {
    for (req.graph_metric_queries) |authority_query| {
        if (!isDefaultHitsAuthorityMetric(authority_query.query.metric_name)) continue;
        for (req.graph_metric_queries) |hub_query| {
            if (!isDefaultHitsHubMetric(hub_query.query.metric_name)) continue;
            if (!std.mem.eql(u8, authority_query.query.index_name, hub_query.query.index_name)) continue;
            const authority = graphMetricResultByName(result.graph_metric_results, authority_query.name) orelse return error.UnsupportedQueryRequest;
            const hub = graphMetricResultByName(result.graph_metric_results, hub_query.name) orelse return error.UnsupportedQueryRequest;
            try validateHitsPairMetricStatusesCompatible(authority.status, hub.status);
        }
    }
}

fn graphMetricResultByName(
    results: []const db_mod.types.GraphMetricResult,
    name: []const u8,
) ?db_mod.types.GraphMetricResult {
    for (results) |result| {
        if (std.mem.eql(u8, result.name, name)) return result;
    }
    return null;
}

fn isDefaultHitsAuthorityMetric(metric_name: []const u8) bool {
    return std.mem.eql(u8, metric_name, "hits_authority");
}

fn isDefaultHitsHubMetric(metric_name: []const u8) bool {
    return std.mem.eql(u8, metric_name, "hits_hub");
}

fn validateHitsPairMetricStatusesCompatible(
    authority: db_mod.types.GraphMetricStatus,
    hub: db_mod.types.GraphMetricStatus,
) !void {
    if (authority.published_generation == 0 or hub.published_generation == 0) return error.UnsupportedQueryRequest;
    if (authority.published_generation != hub.published_generation) return error.UnsupportedQueryRequest;
    try validateGraphMetricStatusSchemaAndFilterCompatible(authority, hub);
}

fn validateGraphMetricFreshness(
    freshness: db_mod.types.GraphMetricFreshness,
    status: db_mod.types.GraphMetricStatus,
) !void {
    if (freshness == .fresh and status.state != .fresh) return error.UnsupportedQueryRequest;
}

fn validateGraphMetricPublishedStatus(status: db_mod.types.GraphMetricStatus) !void {
    switch (status.state) {
        .fresh, .stale, .building, .failed => {},
        .not_ready, .disabled => return error.UnsupportedQueryRequest,
    }
    try validateGraphMetricStatusGenerationShape(status);
    if (!std.math.isFinite(status.progress)) return error.UnsupportedQueryRequest;
    if (status.progress < 0.0 or status.progress > 1.0) return error.UnsupportedQueryRequest;
    if (!std.math.isFinite(status.delta)) return error.UnsupportedQueryRequest;
}

fn validateGraphMetricUnpublishedStatus(status: db_mod.types.GraphMetricStatus) !void {
    if (status.published_generation != 0) return error.UnsupportedQueryRequest;
    switch (status.state) {
        .not_ready, .building, .failed => {},
        .fresh, .stale, .disabled => return error.UnsupportedQueryRequest,
    }
    if (!std.math.isFinite(status.progress)) return error.UnsupportedQueryRequest;
    if (status.progress < 0.0 or status.progress > 1.0) return error.UnsupportedQueryRequest;
    if (!std.math.isFinite(status.delta)) return error.UnsupportedQueryRequest;
}

fn validateGraphMetricStatusGenerationShape(status: db_mod.types.GraphMetricStatus) !void {
    const published = status.published_generation;
    if (published == 0) return error.UnsupportedQueryRequest;
    if (status.edge_generation != 0 and status.edge_generation < published) return error.UnsupportedQueryRequest;
    if (status.target_edge_generation != 0 and status.target_edge_generation < published) return error.UnsupportedQueryRequest;
    if (status.queued_generation != 0 and status.queued_generation < published) return error.UnsupportedQueryRequest;
    if (status.building_generation != 0 and status.building_generation < published) return error.UnsupportedQueryRequest;
    if (status.state == .fresh) {
        if (status.edge_generation != 0 and status.edge_generation != published) return error.UnsupportedQueryRequest;
        if (status.target_edge_generation != 0 and status.target_edge_generation != published) return error.UnsupportedQueryRequest;
    }
}

fn ensureGraphMetricResultComparable(
    builder: *const GraphMetricResultBuilder,
    metric_result: db_mod.types.GraphMetricResult,
) !void {
    if (!std.mem.eql(u8, builder.index_name, metric_result.index_name)) return error.UnsupportedQueryRequest;
    if (!std.mem.eql(u8, builder.metric_name, metric_result.metric_name)) return error.UnsupportedQueryRequest;
    const existing = builder.status orelse return;
    try validateGraphMetricStatusCompatible(existing, metric_result.status);
    if (existing.published_generation != 0 and
        metric_result.status.published_generation != 0 and
        existing.published_generation != metric_result.status.published_generation)
    {
        return error.UnsupportedQueryRequest;
    }
    if ((existing.published_generation == 0 and builder.scores.items.len > 0) or
        (metric_result.status.published_generation == 0 and metric_result.scores.len > 0))
    {
        return error.UnsupportedQueryRequest;
    }
}

fn validateGraphMetricStatusCompatible(
    existing: db_mod.types.GraphMetricStatus,
    incoming: db_mod.types.GraphMetricStatus,
) !void {
    try validateGraphMetricStatusSchemaAndFilterCompatible(existing, incoming);
    if (existing.config_fingerprint != 0 and
        incoming.config_fingerprint != 0 and
        existing.config_fingerprint != incoming.config_fingerprint)
    {
        return error.UnsupportedQueryRequest;
    }
}

fn validateGraphMetricStatusSchemaAndFilterCompatible(
    existing: db_mod.types.GraphMetricStatus,
    incoming: db_mod.types.GraphMetricStatus,
) !void {
    if (existing.metadata_version != 0 and
        incoming.metadata_version != 0 and
        existing.metadata_version != incoming.metadata_version)
    {
        return error.UnsupportedQueryRequest;
    }
    if (!existing.edge_filter.equivalent(incoming.edge_filter)) return error.UnsupportedQueryRequest;
}

fn validateGraphMetricScoreNodesUnique(
    existing_scores: []const db_mod.types.GraphMetricScore,
    incoming_scores: []const db_mod.types.GraphMetricScore,
) !void {
    for (incoming_scores, 0..) |incoming, i| {
        if (!std.math.isFinite(incoming.score)) return error.UnsupportedQueryRequest;
        for (incoming_scores[0..i]) |previous| {
            if (std.mem.eql(u8, previous.node, incoming.node)) return error.UnsupportedQueryRequest;
        }
        for (existing_scores) |existing| {
            if (std.mem.eql(u8, existing.node, incoming.node)) return error.UnsupportedQueryRequest;
        }
    }
}

fn graphMetricQueryTopK(req: db_mod.types.SearchRequest, query_name: []const u8) u32 {
    for (req.graph_metric_queries) |query| {
        if (std.mem.eql(u8, query.name, query_name)) return query.query.top_k;
    }
    return 0;
}

fn mergeGraphMetricRerankStatus(
    alloc: std.mem.Allocator,
    req: db_mod.types.SearchRequest,
    results: []const db_mod.types.SearchResult,
) !?db_mod.types.GraphMetricStatus {
    const rerank = req.graph_metric_rerank orelse {
        for (results) |result| {
            if (result.graph_metric_rerank_status != null) return error.UnsupportedQueryRequest;
        }
        return null;
    };

    var merged: ?db_mod.types.GraphMetricStatus = null;
    errdefer if (merged) |*status| status.deinit(alloc);

    for (results) |result| {
        const status = result.graph_metric_rerank_status orelse return error.UnsupportedQueryRequest;
        if (!std.mem.eql(u8, status.name, rerank.metric_name)) return error.UnsupportedQueryRequest;
        if (status.published_generation == 0) return error.UnsupportedQueryRequest;
        try validateGraphMetricPublishedStatus(status);
        try validateGraphMetricFreshness(rerank.freshness, status);
        if (merged) |*existing| {
            if (existing.published_generation != status.published_generation) return error.UnsupportedQueryRequest;
            try validateGraphMetricStatusCompatible(existing.*, status);
            try mergeGraphMetricStatusInto(alloc, existing, status);
        } else {
            merged = try cloneGraphMetricStatus(alloc, status);
        }
    }

    return merged;
}

pub fn mergeGraphMetricStatusInto(alloc: std.mem.Allocator, target: *db_mod.types.GraphMetricStatus, source: db_mod.types.GraphMetricStatus) !void {
    target.state = mergeGraphMetricState(target.state, source.state);
    target.phase = mergeGraphMetricPhase(target.phase, source.phase);
    target.metadata_version = mergeGraphMetricMetadataVersion(target.metadata_version, source.metadata_version);
    target.config_fingerprint = mergeComparableGeneration(target.config_fingerprint, source.config_fingerprint);
    target.maintenance_paused = target.maintenance_paused or source.maintenance_paused;
    target.build_queued = target.build_queued or source.build_queued;
    target.published_generation = mergeComparableGeneration(target.published_generation, source.published_generation);
    target.edge_generation = @max(target.edge_generation, source.edge_generation);
    target.target_edge_generation = @max(target.target_edge_generation, source.target_edge_generation);
    target.queued_generation = @max(target.queued_generation, source.queued_generation);
    target.building_generation = @max(target.building_generation, source.building_generation);
    target.build_job_id = if (target.build_job_id == 0) source.build_job_id else if (source.build_job_id == 0 or source.build_job_id == target.build_job_id) target.build_job_id else 0;
    target.build_started_at_ms = if (target.build_started_at_ms == 0) source.build_started_at_ms else if (source.build_started_at_ms == 0 or source.build_started_at_ms == target.build_started_at_ms) target.build_started_at_ms else @min(target.build_started_at_ms, source.build_started_at_ms);
    target.build_iteration = @max(target.build_iteration, source.build_iteration);
    target.build_lease_expires_at_ms = @max(target.build_lease_expires_at_ms, source.build_lease_expires_at_ms);
    target.build_completed_units = @max(target.build_completed_units, source.build_completed_units);
    target.build_total_units = @max(target.build_total_units, source.build_total_units);
    if (target.build_worker_id.len == 0) {
        target.build_worker_id = if (source.build_worker_id.len > 0) try alloc.dupe(u8, source.build_worker_id) else "";
    } else if (source.build_worker_id.len > 0 and !std.mem.eql(u8, target.build_worker_id, source.build_worker_id)) {
        alloc.free(target.build_worker_id);
        target.build_worker_id = try alloc.dupe(u8, "multiple");
    }
    if (target.build_cursor.len == 0 and source.build_cursor.len > 0) {
        target.build_cursor = try alloc.dupe(u8, source.build_cursor);
    }
    if (target.build_pages.len == 0 and source.build_pages.len > 0) {
        target.build_pages = try cloneGraphMetricBuildPageStatuses(alloc, source.build_pages);
    } else if (source.build_pages.len > 0) {
        target.build_pages_truncated = true;
    }
    target.build_pages_truncated = target.build_pages_truncated or source.build_pages_truncated;
    target.retry_count = @max(target.retry_count, source.retry_count);
    if (target.last_error.len == 0 and source.last_error.len > 0) {
        target.last_error = try alloc.dupe(u8, source.last_error);
    }
    target.progress = @min(target.progress, source.progress);
    target.converged = target.converged and source.converged;
    target.iterations_completed = @max(target.iterations_completed, source.iterations_completed);
    target.delta = @max(target.delta, source.delta);
    target.computed_at_ms = @max(target.computed_at_ms, source.computed_at_ms);
}

/// Operational fan-out may aggregate generations and progress that naturally
/// differ by shard, but it must never conceal divergent metric definitions.
/// Query fan-in performs the same validation before calling the lower-level
/// merge helper; control-plane callers use this checked entry point directly.
pub fn mergeCompatibleGraphMetricStatusInto(
    alloc: std.mem.Allocator,
    target: *db_mod.types.GraphMetricStatus,
    source: db_mod.types.GraphMetricStatus,
) !void {
    if (!std.mem.eql(u8, target.name, source.name)) return error.GraphMetricStatusConflict;
    if (target.metadata_version != 0 and
        source.metadata_version != 0 and
        target.metadata_version != source.metadata_version)
    {
        return error.GraphMetricStatusConflict;
    }
    if (target.config_fingerprint != 0 and
        source.config_fingerprint != 0 and
        target.config_fingerprint != source.config_fingerprint)
    {
        return error.GraphMetricStatusConflict;
    }
    if (!target.edge_filter.equivalent(source.edge_filter)) return error.GraphMetricStatusConflict;
    try mergeGraphMetricStatusInto(alloc, target, source);
}

test "graph metric operational status aggregation rejects divergent shard definitions" {
    const alloc = std.testing.allocator;
    const cites_filter = graph_mod.GraphMetricEdgeFilter{ .mode = .types, .types = &.{"cites"} };
    const related_filter = graph_mod.GraphMetricEdgeFilter{ .mode = .types, .types = &.{"related"} };

    var target = db_mod.types.GraphMetricStatus{
        .name = try alloc.dupe(u8, "pagerank"),
        .state = .fresh,
        .edge_filter = try cites_filter.cloneAlloc(alloc),
        .metadata_version = 2,
        .config_fingerprint = 11,
        .published_generation = 7,
        .progress = 1.0,
    };
    defer target.deinit(alloc);

    var metadata_mismatch = db_mod.types.GraphMetricStatus{
        .name = try alloc.dupe(u8, "pagerank"),
        .edge_filter = try cites_filter.cloneAlloc(alloc),
        .metadata_version = 3,
    };
    defer metadata_mismatch.deinit(alloc);
    try std.testing.expectError(
        error.GraphMetricStatusConflict,
        mergeCompatibleGraphMetricStatusInto(alloc, &target, metadata_mismatch),
    );

    var filter_mismatch = db_mod.types.GraphMetricStatus{
        .name = try alloc.dupe(u8, "pagerank"),
        .edge_filter = try related_filter.cloneAlloc(alloc),
        .metadata_version = 2,
    };
    defer filter_mismatch.deinit(alloc);
    try std.testing.expectError(
        error.GraphMetricStatusConflict,
        mergeCompatibleGraphMetricStatusInto(alloc, &target, filter_mismatch),
    );

    var fingerprint_mismatch = db_mod.types.GraphMetricStatus{
        .name = try alloc.dupe(u8, "pagerank"),
        .edge_filter = try cites_filter.cloneAlloc(alloc),
        .metadata_version = 2,
        .config_fingerprint = 12,
    };
    defer fingerprint_mismatch.deinit(alloc);
    try std.testing.expectError(
        error.GraphMetricStatusConflict,
        mergeCompatibleGraphMetricStatusInto(alloc, &target, fingerprint_mismatch),
    );

    var compatible = db_mod.types.GraphMetricStatus{
        .name = try alloc.dupe(u8, "pagerank"),
        .state = .stale,
        .edge_filter = try cites_filter.cloneAlloc(alloc),
        .metadata_version = 2,
        .config_fingerprint = 11,
        .published_generation = 7,
        .progress = 0.5,
    };
    defer compatible.deinit(alloc);
    try mergeCompatibleGraphMetricStatusInto(alloc, &target, compatible);
    try std.testing.expectEqual(graph_mod.GraphIndex.GraphMetricState.stale, target.state);
    try std.testing.expectEqual(@as(f64, 0.5), target.progress);
}

fn mergeGraphMetricMetadataVersion(left: u32, right: u32) u32 {
    if (left == 0) return right;
    if (right == 0) return left;
    if (left == right) return left;
    return 0;
}

fn mergeComparableGeneration(left: u64, right: u64) u64 {
    if (left == 0) return right;
    if (right == 0) return left;
    if (left == right) return left;
    return @min(left, right);
}

fn mergeGraphMetricState(left: graph_mod.GraphIndex.GraphMetricState, right: graph_mod.GraphIndex.GraphMetricState) graph_mod.GraphIndex.GraphMetricState {
    return if (graphMetricStateSeverity(right) > graphMetricStateSeverity(left)) right else left;
}

fn graphMetricStateSeverity(state: graph_mod.GraphIndex.GraphMetricState) u8 {
    return switch (state) {
        .disabled => 5,
        .failed => 4,
        .building => 3,
        .not_ready => 2,
        .stale => 1,
        .fresh => 0,
    };
}

fn mergeGraphMetricPhase(left: graph_mod.GraphIndex.GraphMetricBuildPhase, right: graph_mod.GraphIndex.GraphMetricBuildPhase) graph_mod.GraphIndex.GraphMetricBuildPhase {
    return if (graphMetricPhaseSeverity(right) > graphMetricPhaseSeverity(left)) right else left;
}

fn graphMetricPhaseSeverity(phase: graph_mod.GraphIndex.GraphMetricBuildPhase) u8 {
    return switch (phase) {
        .cleanup_old_generations => 10,
        .publish_generation, .publishing => 9,
        .check_convergence => 8,
        .hits_hub_reduce_ranks => 8,
        .hits_hub_contributions => 8,
        .reduce_ranks => 7,
        .iterate_contributions, .computing => 6,
        .initialize_ranks => 5,
        .scan_edges_and_out_degree => 4,
        .prepare_generation => 3,
        .idle => 1,
        .complete => 0,
    };
}

fn mergeGraphSearchResults(
    alloc: std.mem.Allocator,
    req: db_mod.types.SearchRequest,
    results: []const db_mod.types.SearchResult,
) ![]db_mod.types.GraphSearchResult {
    try validateRequestedGraphSearchFanIn(req, results);

    var builders = std.ArrayListUnmanaged(GraphSearchResultBuilder).empty;
    defer {
        for (builders.items) |*builder| builder.deinit(alloc);
        builders.deinit(alloc);
    }

    for (results) |result| {
        for (result.graph_results) |graph_result| {
            const idx = blk: {
                for (builders.items, 0..) |builder, i| {
                    if (std.mem.eql(u8, builder.name, graph_result.name)) break :blk i;
                }
                const name = try alloc.dupe(u8, graph_result.name);
                builders.append(alloc, .{ .name = name }) catch |err| {
                    alloc.free(name);
                    return err;
                };
                break :blk builders.items.len - 1;
            };
            var builder = &builders.items[idx];
            builder.total_hits +|= graph_result.total_hits;

            for (graph_result.nodes) |node| {
                try validateGraphResultNodePayload(node);
                try validateGraphResultNodeKeyUnique(builder.nodes.items, graph_result.nodes, node.key);
                var owned = try cloneGraphResultNode(alloc, node);
                builder.nodes.append(alloc, owned) catch |err| {
                    owned.deinit(alloc);
                    return err;
                };
            }
            for (graph_result.paths) |path| {
                try validateGraphPathPayload(path);
                const owned = try cloneGraphPath(alloc, path);
                builder.paths.append(alloc, owned) catch |err| {
                    graph_paths.freePath(alloc, owned);
                    return err;
                };
            }
            for (graph_result.matches) |match| {
                try validateGraphPatternMatchPayload(match);
                var owned = try cloneGraphPatternMatch(alloc, match);
                builder.matches.append(alloc, owned) catch |err| {
                    owned.deinit(alloc);
                    return err;
                };
            }
            for (graph_result.hits) |hit| {
                try validateGraphSearchHitPayload(hit);
                try validateGraphSearchHitIdUnique(builder.hits.items, graph_result.hits, hit.id);
                var owned = try hit.clone(alloc);
                builder.hits.append(alloc, owned) catch |err| {
                    owned.deinit(alloc);
                    return err;
                };
            }
            for (graph_result.metric_status) |status| {
                try mergeGraphSearchMetricStatus(alloc, &builder.metric_status, status);
            }
        }
    }

    const merged = try alloc.alloc(db_mod.types.GraphSearchResult, builders.items.len);
    var initialized: usize = 0;
    errdefer {
        for (merged[0..initialized]) |*graph_result| graph_result.deinit(alloc);
        alloc.free(merged);
    }
    for (builders.items, 0..) |*builder, i| {
        merged[i] = try builder.toOwned(alloc);
        initialized += 1;
    }
    return merged;
}

fn stripUnrequestedGraphSearchMetricStatuses(
    alloc: std.mem.Allocator,
    req: db_mod.types.SearchRequest,
    graph_results: []db_mod.types.GraphSearchResult,
) void {
    for (graph_results) |*graph_result| {
        if (graphSearchQueryIncludesMetricStatus(req, graph_result.name)) continue;
        freeGraphSearchMetricStatuses(alloc, graph_result);
    }
}

fn graphSearchQueryIncludesMetricStatus(req: db_mod.types.SearchRequest, name: []const u8) bool {
    for (req.graph_queries) |query| {
        if (std.mem.eql(u8, query.name, name)) return query.query.include_metric_status;
    }
    return false;
}

fn freeGraphSearchMetricStatuses(
    alloc: std.mem.Allocator,
    graph_result: *db_mod.types.GraphSearchResult,
) void {
    for (graph_result.metric_status) |*status| status.deinit(alloc);
    if (graph_result.metric_status.len > 0) alloc.free(graph_result.metric_status);
    graph_result.metric_status = &.{};
}

fn validateGraphResultNodePayload(node: graph_query_mod.GraphResultNode) !void {
    if (!std.math.isFinite(node.distance)) return error.UnsupportedQueryRequest;
    if (node.path) |path_nodes| {
        if (node.path_edges) |edges| {
            if (path_nodes.len != edges.len + 1) return error.UnsupportedQueryRequest;
            if (node.depth != edges.len) return error.UnsupportedQueryRequest;
        }
    } else if (node.path_edges != null) {
        return error.UnsupportedQueryRequest;
    }
    if (node.path_edges) |edges| {
        for (edges) |edge| {
            if (!std.math.isFinite(edge.weight)) return error.UnsupportedQueryRequest;
        }
    }
}

fn validateGraphResultNodeKeyUnique(
    existing: []const graph_query_mod.GraphResultNode,
    incoming: []const graph_query_mod.GraphResultNode,
    key: []const u8,
) !void {
    for (existing) |node| {
        if (std.mem.eql(u8, node.key, key)) return error.UnsupportedQueryRequest;
    }
    var seen: usize = 0;
    for (incoming) |node| {
        if (std.mem.eql(u8, node.key, key)) {
            seen += 1;
            if (seen > 1) return error.UnsupportedQueryRequest;
        }
    }
}

fn validateGraphPathPayload(path: db_mod.types.GraphPath) !void {
    if (path.nodes.len != path.edges.len + 1) return error.UnsupportedQueryRequest;
    if (path.length != path.edges.len) return error.UnsupportedQueryRequest;
    if (!std.math.isFinite(path.total_weight)) return error.UnsupportedQueryRequest;
    for (path.edges) |edge| {
        if (!std.math.isFinite(edge.weight)) return error.UnsupportedQueryRequest;
    }
}

fn validateGraphPatternMatchPayload(match: db_mod.types.GraphPatternMatch) !void {
    for (match.bindings) |binding| try validateGraphResultNodePayload(binding.node);
    for (match.path) |edge| {
        if (!std.math.isFinite(edge.weight)) return error.UnsupportedQueryRequest;
    }
}

fn validateGraphSearchHitPayload(hit: db_mod.types.SearchHit) !void {
    if (hit.score_details != null) return error.UnsupportedQueryRequest;
    if (hit.score) |score| {
        if (!std.math.isFinite(score)) return error.UnsupportedQueryRequest;
    }
}

fn validateGraphSearchHitIdUnique(
    existing: []const db_mod.types.SearchHit,
    incoming: []const db_mod.types.SearchHit,
    id: []const u8,
) !void {
    for (existing) |hit| {
        if (std.mem.eql(u8, hit.id, id)) return error.UnsupportedQueryRequest;
    }
    var seen: usize = 0;
    for (incoming) |hit| {
        if (std.mem.eql(u8, hit.id, id)) {
            seen += 1;
            if (seen > 1) return error.UnsupportedQueryRequest;
        }
    }
}

fn validateRequestedGraphSearchFanIn(
    req: db_mod.types.SearchRequest,
    results: []const db_mod.types.SearchResult,
) !void {
    if (req.graph_queries.len == 0) return;
    try validateGraphSearchQueryNamesUnique(req.graph_queries);

    for (results) |result| {
        for (result.graph_results) |graph_result| {
            if (!graphSearchQueryRequested(req, graph_result.name)) return error.UnsupportedQueryRequest;
        }
        for (req.graph_queries) |query| {
            var found = false;
            for (result.graph_results) |graph_result| {
                if (!std.mem.eql(u8, graph_result.name, query.name)) continue;
                if (found) return error.UnsupportedQueryRequest;
                found = true;
                try validateGraphSearchMetricStatuses(query, graph_result);
            }
            if (!found) return error.UnsupportedQueryRequest;
        }
    }
}

fn validateGraphSearchQueryNamesUnique(queries: []const db_mod.types.NamedGraphQuery) !void {
    for (queries, 0..) |query, i| {
        for (queries[0..i]) |previous| {
            if (std.mem.eql(u8, previous.name, query.name)) return error.UnsupportedQueryRequest;
        }
    }
}

fn graphSearchQueryRequested(req: db_mod.types.SearchRequest, query_name: []const u8) bool {
    for (req.graph_queries) |query| {
        if (std.mem.eql(u8, query.name, query_name)) return true;
    }
    return false;
}

fn validateGraphSearchMetricStatuses(
    query: db_mod.types.NamedGraphQuery,
    graph_result: db_mod.types.GraphSearchResult,
) !void {
    try validateGraphSearchProjectedMetricNamesUnique(query.query.metrics);
    try validateGraphSearchOrderMetricNamesUnique(query.query.order_by);
    const expects_metric_status = query.query.include_metric_status or
        query.query.metrics.len > 0 or
        query.query.order_by.len > 0 or
        query.query.where_metric.len > 0;
    if (!expects_metric_status and graph_result.metric_status.len > 0) return error.UnsupportedQueryRequest;
    try validateGraphSearchMetricStatusList(query, graph_result.metric_status);
    if (!query.query.include_metric_status) {
        try validateGraphSearchMetricStatusNamesRequested(query, graph_result.metric_status);
    }
    try validateGraphSearchMetricPayloads(query, graph_result);
    for (query.query.metrics) |metric| try validateGraphSearchMetricStatus(graph_result, metric.name, metric.freshness, false);
    for (query.query.order_by) |metric| try validateGraphSearchMetricStatus(graph_result, metric.name, metric.freshness, true);
    for (query.query.where_metric) |metric| try validateGraphSearchMetricStatus(graph_result, metric.name, metric.freshness, true);
}

fn validateGraphSearchProjectedMetricNamesUnique(metrics: []const graph_query_mod.GraphMetricRead) !void {
    for (metrics, 0..) |metric, i| {
        for (metrics[0..i]) |previous| {
            if (std.mem.eql(u8, previous.name, metric.name)) return error.UnsupportedQueryRequest;
        }
    }
}

fn validateGraphSearchOrderMetricNamesUnique(metrics: []const graph_query_mod.GraphMetricOrder) !void {
    for (metrics, 0..) |metric, i| {
        for (metrics[0..i]) |previous| {
            if (std.mem.eql(u8, previous.name, metric.name)) return error.UnsupportedQueryRequest;
        }
    }
}

fn validateGraphSearchMetricStatusList(
    query: db_mod.types.NamedGraphQuery,
    statuses: []const db_mod.types.GraphMetricStatus,
) !void {
    for (statuses, 0..) |status, i| {
        const require_published = graphSearchMetricScoreReadRequested(query, status.name);
        const allow_unpublished = !require_published and
            (query.query.include_metric_status or graphSearchMetricProjected(query, status.name));
        if (status.published_generation == 0) {
            if (!allow_unpublished) return error.UnsupportedQueryRequest;
            try validateGraphMetricUnpublishedStatus(status);
        } else {
            try validateGraphMetricPublishedStatus(status);
        }
        for (statuses[0..i]) |previous| {
            if (std.mem.eql(u8, previous.name, status.name)) return error.UnsupportedQueryRequest;
        }
    }
    try validateHitsPairMetricStatusList(statuses);
}

fn validateHitsPairMetricStatusList(statuses: []const db_mod.types.GraphMetricStatus) !void {
    var authority: ?db_mod.types.GraphMetricStatus = null;
    var hub: ?db_mod.types.GraphMetricStatus = null;
    for (statuses) |status| {
        if (isDefaultHitsAuthorityMetric(status.name)) {
            authority = status;
        } else if (isDefaultHitsHubMetric(status.name)) {
            hub = status;
        }
    }
    if (authority) |authority_status| {
        if (hub) |hub_status| try validateGraphSearchHitsPairMetricStatusesCompatible(authority_status, hub_status);
    }
}

fn validateGraphSearchHitsPairMetricStatusesCompatible(
    authority: db_mod.types.GraphMetricStatus,
    hub: db_mod.types.GraphMetricStatus,
) !void {
    if (authority.published_generation == 0 or hub.published_generation == 0) {
        if (authority.published_generation != 0 or hub.published_generation != 0) return error.UnsupportedQueryRequest;
        try validateGraphMetricStatusSchemaAndFilterCompatible(authority, hub);
        return;
    }
    try validateHitsPairMetricStatusesCompatible(authority, hub);
}

fn validateGraphSearchMetricStatusNamesRequested(
    query: db_mod.types.NamedGraphQuery,
    statuses: []const db_mod.types.GraphMetricStatus,
) !void {
    for (statuses) |status| {
        if (!graphSearchMetricNameRequested(query, status.name)) return error.UnsupportedQueryRequest;
    }
}

fn graphSearchMetricNameRequested(
    query: db_mod.types.NamedGraphQuery,
    metric_name: []const u8,
) bool {
    if (graphSearchMetricProjected(query, metric_name)) return true;
    if (graphSearchMetricScoreReadRequested(query, metric_name)) return true;
    return false;
}

fn graphSearchMetricProjected(
    query: db_mod.types.NamedGraphQuery,
    metric_name: []const u8,
) bool {
    for (query.query.metrics) |metric| {
        if (std.mem.eql(u8, metric.name, metric_name)) return true;
    }
    return false;
}

fn graphSearchMetricScoreReadRequested(
    query: db_mod.types.NamedGraphQuery,
    metric_name: []const u8,
) bool {
    for (query.query.order_by) |metric| {
        if (std.mem.eql(u8, metric.name, metric_name)) return true;
    }
    for (query.query.where_metric) |metric| {
        if (std.mem.eql(u8, metric.name, metric_name)) return true;
    }
    return false;
}

fn validateGraphSearchMetricPayloads(
    query: db_mod.types.NamedGraphQuery,
    graph_result: db_mod.types.GraphSearchResult,
) !void {
    for (graph_result.nodes) |node| try validateGraphResultNodeMetricPayload(query.query.metrics, node);
    for (graph_result.matches) |match| {
        for (match.bindings) |binding| try validateGraphResultNodeMetricPayload(query.query.metrics, binding.node);
    }
}

fn validateGraphResultNodeMetricPayload(
    projected_metrics: []const graph_query_mod.GraphMetricRead,
    node: graph_query_mod.GraphResultNode,
) !void {
    for (node.metrics, 0..) |metric, i| {
        if (!graphQueryMetricProjected(projected_metrics, metric.name)) return error.UnsupportedQueryRequest;
        for (node.metrics[0..i]) |previous| {
            if (std.mem.eql(u8, previous.name, metric.name)) return error.UnsupportedQueryRequest;
        }
        if (metric.score) |score| {
            if (!std.math.isFinite(score)) return error.UnsupportedQueryRequest;
        }
    }
    for (projected_metrics) |projected| {
        var found = false;
        for (node.metrics) |metric| {
            if (!std.mem.eql(u8, metric.name, projected.name)) continue;
            found = true;
            break;
        }
        if (!found) return error.UnsupportedQueryRequest;
    }
}

fn graphQueryMetricProjected(
    projected_metrics: []const graph_query_mod.GraphMetricRead,
    metric_name: []const u8,
) bool {
    for (projected_metrics) |metric| {
        if (std.mem.eql(u8, metric.name, metric_name)) return true;
    }
    return false;
}

fn validateGraphSearchMetricStatus(
    graph_result: db_mod.types.GraphSearchResult,
    metric_name: []const u8,
    freshness: graph_query_mod.GraphMetricFreshness,
    require_published: bool,
) !void {
    var found = false;
    for (graph_result.metric_status) |status| {
        if (!std.mem.eql(u8, status.name, metric_name)) continue;
        if (found) return error.UnsupportedQueryRequest;
        found = true;
        try validateGraphQueryMetricFreshness(freshness, status, require_published);
    }
    if (!found) return error.UnsupportedQueryRequest;
}

fn validateGraphQueryMetricFreshness(
    freshness: graph_query_mod.GraphMetricFreshness,
    status: db_mod.types.GraphMetricStatus,
    require_published: bool,
) !void {
    if (require_published and status.published_generation == 0) return error.UnsupportedQueryRequest;
    if (freshness == .fresh and status.published_generation == 0) return error.UnsupportedQueryRequest;
    if (freshness == .fresh and status.state != .fresh) return error.UnsupportedQueryRequest;
}

fn mergeGraphSearchMetricStatus(
    alloc: std.mem.Allocator,
    statuses: *std.ArrayListUnmanaged(db_mod.types.GraphMetricStatus),
    source: db_mod.types.GraphMetricStatus,
) !void {
    for (statuses.items) |*status| {
        if (!std.mem.eql(u8, status.name, source.name)) continue;
        if (status.published_generation != source.published_generation) return error.UnsupportedQueryRequest;
        try validateGraphMetricStatusCompatible(status.*, source);
        try mergeGraphMetricStatusInto(alloc, status, source);
        return;
    }
    try statuses.append(alloc, try cloneGraphMetricStatus(alloc, source));
}

fn cloneGraphSearchResult(
    alloc: std.mem.Allocator,
    source: db_mod.types.GraphSearchResult,
) !db_mod.types.GraphSearchResult {
    const GraphNode = std.meta.Child(@TypeOf(source.nodes));
    const nodes = try alloc.alloc(GraphNode, source.nodes.len);
    var initialized_nodes: usize = 0;
    errdefer {
        for (nodes[0..initialized_nodes]) |*node| node.deinit(alloc);
        if (source.nodes.len > 0) alloc.free(nodes);
    }
    for (source.nodes, 0..) |node, i| {
        nodes[i] = try cloneGraphResultNode(alloc, node);
        initialized_nodes += 1;
    }

    const GraphPath = std.meta.Child(@TypeOf(source.paths));
    const paths = try alloc.alloc(GraphPath, source.paths.len);
    var initialized_paths: usize = 0;
    errdefer {
        for (paths[0..initialized_paths]) |path| graph_paths.freePath(alloc, path);
        if (source.paths.len > 0) alloc.free(paths);
    }
    for (source.paths, 0..) |path, i| {
        paths[i] = try cloneGraphPath(alloc, path);
        initialized_paths += 1;
    }

    const hits = try alloc.alloc(db_mod.types.SearchHit, source.hits.len);
    var initialized_hits: usize = 0;
    errdefer {
        for (hits[0..initialized_hits]) |*hit| hit.deinit(alloc);
        if (source.hits.len > 0) alloc.free(hits);
    }
    for (source.hits, 0..) |hit, i| {
        hits[i] = try hit.clone(alloc);
        initialized_hits += 1;
    }

    const matches = try alloc.alloc(db_mod.types.GraphPatternMatch, source.matches.len);
    var initialized_matches: usize = 0;
    errdefer {
        for (matches[0..initialized_matches]) |*match| match.deinit(alloc);
        if (source.matches.len > 0) alloc.free(matches);
    }
    for (source.matches, 0..) |match, i| {
        matches[i] = try cloneGraphPatternMatch(alloc, match);
        initialized_matches += 1;
    }

    const metric_status = try alloc.alloc(db_mod.types.GraphMetricStatus, source.metric_status.len);
    var initialized_metric_status: usize = 0;
    errdefer {
        for (metric_status[0..initialized_metric_status]) |*status| status.deinit(alloc);
        if (source.metric_status.len > 0) alloc.free(metric_status);
    }
    for (source.metric_status, 0..) |status, i| {
        metric_status[i] = try cloneGraphMetricStatus(alloc, status);
        initialized_metric_status += 1;
    }

    return .{
        .name = try alloc.dupe(u8, source.name),
        .nodes = nodes,
        .paths = paths,
        .matches = matches,
        .hits = hits,
        .total_hits = source.total_hits,
        .metric_status = metric_status,
    };
}

pub fn cloneGraphMetricStatus(
    alloc: std.mem.Allocator,
    source: db_mod.types.GraphMetricStatus,
) !db_mod.types.GraphMetricStatus {
    const name = try alloc.dupe(u8, source.name);
    errdefer alloc.free(name);
    var edge_filter = try source.edge_filter.cloneAlloc(alloc);
    errdefer edge_filter.deinit(alloc);
    const recent_events = if (source.recent_events.len > 0)
        try alloc.dupe(graph_mod.GraphIndex.GraphMetricEvent, source.recent_events)
    else
        @constCast((&[_]graph_mod.GraphIndex.GraphMetricEvent{})[0..]);
    errdefer if (recent_events.len > 0) alloc.free(recent_events);
    const last_error = if (source.last_error.len > 0) try alloc.dupe(u8, source.last_error) else "";
    errdefer if (last_error.len > 0) alloc.free(last_error);
    const build_worker_id = if (source.build_worker_id.len > 0) try alloc.dupe(u8, source.build_worker_id) else "";
    errdefer if (build_worker_id.len > 0) alloc.free(build_worker_id);
    const build_cursor = if (source.build_cursor.len > 0) try alloc.dupe(u8, source.build_cursor) else "";
    errdefer if (build_cursor.len > 0) alloc.free(build_cursor);
    const build_pages = try cloneGraphMetricBuildPageStatuses(alloc, source.build_pages);
    errdefer {
        for (build_pages) |*page| page.deinit(alloc);
        if (build_pages.len > 0) alloc.free(build_pages);
    }
    return .{
        .name = name,
        .state = source.state,
        .phase = source.phase,
        .edge_filter = edge_filter,
        .metadata_version = source.metadata_version,
        .config_fingerprint = source.config_fingerprint,
        .maintenance_paused = source.maintenance_paused,
        .build_queued = source.build_queued,
        .published_generation = source.published_generation,
        .edge_generation = source.edge_generation,
        .target_edge_generation = source.target_edge_generation,
        .queued_generation = source.queued_generation,
        .building_generation = source.building_generation,
        .build_job_id = source.build_job_id,
        .build_started_at_ms = source.build_started_at_ms,
        .build_iteration = source.build_iteration,
        .build_lease_expires_at_ms = source.build_lease_expires_at_ms,
        .build_worker_id = build_worker_id,
        .build_cursor = build_cursor,
        .build_completed_units = source.build_completed_units,
        .build_total_units = source.build_total_units,
        .build_pages = build_pages,
        .build_pages_truncated = source.build_pages_truncated,
        .retry_count = source.retry_count,
        .last_error = last_error,
        .progress = source.progress,
        .converged = source.converged,
        .iterations_completed = source.iterations_completed,
        .delta = source.delta,
        .computed_at_ms = source.computed_at_ms,
        .last_event = source.last_event,
        .recent_events = recent_events,
    };
}

fn cloneGraphMetricBuildPageStatuses(
    alloc: std.mem.Allocator,
    source: []const db_mod.types.GraphMetricBuildPageStatus,
) ![]db_mod.types.GraphMetricBuildPageStatus {
    if (source.len == 0) return @constCast((&[_]db_mod.types.GraphMetricBuildPageStatus{})[0..]);
    const out = try alloc.alloc(db_mod.types.GraphMetricBuildPageStatus, source.len);
    var initialized: usize = 0;
    errdefer {
        for (out[0..initialized]) |*page| page.deinit(alloc);
        alloc.free(out);
    }
    for (source, 0..) |page, i| {
        const worker_id = if (page.worker_id.len > 0) try alloc.dupe(u8, page.worker_id) else "";
        errdefer if (worker_id.len > 0) alloc.free(worker_id);
        const cursor = if (page.cursor.len > 0) try alloc.dupe(u8, page.cursor) else "";
        errdefer if (cursor.len > 0) alloc.free(cursor);
        const last_error = if (page.last_error.len > 0) try alloc.dupe(u8, page.last_error) else "";
        errdefer if (last_error.len > 0) alloc.free(last_error);
        out[i] = .{
            .phase = page.phase,
            .iteration = page.iteration,
            .page_id = page.page_id,
            .state = page.state,
            .range_kind = page.range_kind,
            .worker_id = worker_id,
            .lease_expires_at_ms = page.lease_expires_at_ms,
            .attempt = page.attempt,
            .cursor = cursor,
            .completed_units = page.completed_units,
            .total_units = page.total_units,
            .last_error = last_error,
        };
        initialized += 1;
    }
    return out;
}

fn cloneGraphPatternMatch(
    alloc: std.mem.Allocator,
    source: db_mod.types.GraphPatternMatch,
) !db_mod.types.GraphPatternMatch {
    const bindings = try alloc.alloc(db_mod.types.GraphPatternBinding, source.bindings.len);
    var initialized_bindings: usize = 0;
    errdefer {
        for (bindings[0..initialized_bindings]) |*binding| binding.deinit(alloc);
        if (source.bindings.len > 0) alloc.free(bindings);
    }
    for (source.bindings, 0..) |binding, i| {
        const alias = try alloc.dupe(u8, binding.alias);
        errdefer alloc.free(alias);
        const node = try cloneGraphResultNode(alloc, binding.node);
        bindings[i] = .{ .alias = alias, .node = node };
        initialized_bindings += 1;
    }

    const path = try alloc.alloc(graph_query_mod.PathEdgeInfo, source.path.len);
    var initialized_path: usize = 0;
    errdefer {
        for (path[0..initialized_path]) |edge| {
            alloc.free(edge.source);
            alloc.free(edge.target);
            alloc.free(edge.edge_type);
            if (edge.metadata.len > 0) alloc.free(edge.metadata);
        }
        if (source.path.len > 0) alloc.free(path);
    }
    for (source.path, 0..) |edge, i| {
        path[i] = try clonePathEdge(graph_query_mod.PathEdgeInfo, alloc, edge);
        initialized_path += 1;
    }

    return .{
        .bindings = bindings,
        .path = path,
    };
}

fn cloneGraphResultNode(
    alloc: std.mem.Allocator,
    source: graph_query_mod.GraphResultNode,
) !graph_query_mod.GraphResultNode {
    const key = try alloc.dupe(u8, source.key);
    errdefer alloc.free(key);
    const table = if (source.table) |value| try alloc.dupe(u8, value) else null;
    errdefer if (table) |value| alloc.free(value);
    const path = if (source.path) |items| try cloneStringSlice(alloc, items) else null;
    errdefer if (path) |items| freeStringSlice(alloc, items);
    const path_edges = if (source.path_edges) |items|
        try clonePathEdges(graph_query_mod.PathEdgeInfo, alloc, items)
    else
        null;
    errdefer if (path_edges) |items| freePathEdges(alloc, items);
    const provenance = if (source.provenance) |items|
        try cloneStringSlice(alloc, items)
    else
        null;
    errdefer if (provenance) |items| freeStringSlice(alloc, items);

    const metrics = try alloc.alloc(graph_query_mod.GraphMetricValue, source.metrics.len);
    var initialized_metrics: usize = 0;
    errdefer {
        for (metrics[0..initialized_metrics]) |*metric| metric.deinit(alloc);
        if (source.metrics.len > 0) alloc.free(metrics);
    }
    for (source.metrics, 0..) |metric, i| {
        metrics[i] = .{
            .name = try alloc.dupe(u8, metric.name),
            .score = metric.score,
        };
        initialized_metrics += 1;
    }

    return .{
        .key = key,
        .depth = source.depth,
        .distance = source.distance,
        .path = path,
        .path_edges = path_edges,
        .provenance = provenance,
        .table = table,
        .metrics = metrics,
    };
}

fn cloneGraphPath(
    alloc: std.mem.Allocator,
    source: db_mod.types.GraphPath,
) !db_mod.types.GraphPath {
    const nodes = try cloneStringSlice(alloc, source.nodes);
    errdefer freeStringSlice(alloc, nodes);
    const node_tables = try cloneOptionalStringSlice(alloc, source.node_tables);
    errdefer freeOptionalStringSlice(alloc, node_tables);
    const edges = try clonePathEdges(graph_paths.PathEdge, alloc, source.edges);
    errdefer freePathEdges(alloc, edges);

    return .{
        .nodes = nodes,
        .node_tables = node_tables,
        .edges = edges,
        .total_weight = source.total_weight,
        .length = source.length,
    };
}

fn cloneStringSlice(
    alloc: std.mem.Allocator,
    source: []const []const u8,
) ![][]const u8 {
    const out = try alloc.alloc([]const u8, source.len);
    var initialized: usize = 0;
    errdefer {
        for (out[0..initialized]) |item| alloc.free(item);
        alloc.free(out);
    }
    for (source, 0..) |item, i| {
        out[i] = try alloc.dupe(u8, item);
        initialized += 1;
    }
    return out;
}

fn freeStringSlice(alloc: std.mem.Allocator, items: []const []const u8) void {
    for (items) |item| alloc.free(item);
    alloc.free(items);
}

fn cloneOptionalStringSlice(
    alloc: std.mem.Allocator,
    source: []const ?[]const u8,
) ![]?[]const u8 {
    if (source.len == 0) return &.{};
    const out = try alloc.alloc(?[]const u8, source.len);
    var initialized: usize = 0;
    errdefer {
        for (out[0..initialized]) |item| if (item) |value| alloc.free(value);
        alloc.free(out);
    }
    for (source, 0..) |item, i| {
        out[i] = if (item) |value| try alloc.dupe(u8, value) else null;
        initialized += 1;
    }
    return out;
}

fn freeOptionalStringSlice(
    alloc: std.mem.Allocator,
    items: []const ?[]const u8,
) void {
    for (items) |item| if (item) |value| alloc.free(value);
    if (items.len > 0) alloc.free(items);
}

fn clonePathEdges(
    comptime Edge: type,
    alloc: std.mem.Allocator,
    source: anytype,
) ![]Edge {
    const out = try alloc.alloc(Edge, source.len);
    var initialized: usize = 0;
    errdefer {
        for (out[0..initialized]) |edge| freePathEdge(alloc, edge);
        alloc.free(out);
    }
    for (source, 0..) |edge, i| {
        out[i] = try clonePathEdge(Edge, alloc, edge);
        initialized += 1;
    }
    return out;
}

fn clonePathEdge(
    comptime Edge: type,
    alloc: std.mem.Allocator,
    source: anytype,
) !Edge {
    const edge_source = try alloc.dupe(u8, source.source);
    errdefer alloc.free(edge_source);
    const target = try alloc.dupe(u8, source.target);
    errdefer alloc.free(target);
    const edge_type = try alloc.dupe(u8, source.edge_type);
    errdefer alloc.free(edge_type);
    const metadata = if (source.metadata.len > 0)
        try alloc.dupe(u8, source.metadata)
    else
        "";
    errdefer if (metadata.len > 0) alloc.free(metadata);
    return .{
        .source = edge_source,
        .target = target,
        .edge_type = edge_type,
        .weight = source.weight,
        .metadata = metadata,
    };
}

fn freePathEdges(alloc: std.mem.Allocator, edges: anytype) void {
    for (edges) |edge| freePathEdge(alloc, edge);
    alloc.free(edges);
}

fn freePathEdge(alloc: std.mem.Allocator, edge: anytype) void {
    alloc.free(edge.source);
    alloc.free(edge.target);
    alloc.free(edge.edge_type);
    if (edge.metadata.len > 0) alloc.free(edge.metadata);
}

test "query parser accepts full text request subset" {
    var owned = try parseQueryRequest(std.testing.allocator, null, "docs",
        \\{"full_text_search":{"match":{"field":"body","text":"alpha"}},"fields":["title"],"limit":5}
    );
    defer owned.deinit(std.testing.allocator);
    try std.testing.expectEqual(@as(u32, 5), owned.req.limit);
    try std.testing.expectEqual(@as(usize, 1), owned.req.fields.len);
    try std.testing.expectEqual(false, owned.req.include_all_fields);
}

test "query parser accepts generated query request shape" {
    const metadata_openapi = @import("antfly_metadata_openapi");

    var full_text = try std.json.parseFromSlice(std.json.Value, std.testing.allocator,
        \\{"match":{"field":"body","text":"alpha"}}
    , .{});
    defer full_text.deinit();

    const body = try jsonStringifyAlloc(std.testing.allocator, metadata_openapi.QueryRequest{
        .full_text_search = full_text.value,
        .fields = &.{"title"},
        .limit = 5,
        .profile = false,
    });
    defer std.testing.allocator.free(body);

    var owned = try parseQueryRequest(std.testing.allocator, null, "docs", body);
    defer owned.deinit(std.testing.allocator);
    try std.testing.expectEqual(@as(u32, 5), owned.req.limit);
    try std.testing.expectEqual(@as(usize, 1), owned.req.fields.len);
    try std.testing.expectEqualStrings("title", owned.req.fields[0]);
}

test "query parser defers ordinary stored projection to response encoding" {
    var owned = try parseQueryRequest(std.testing.allocator, null, "docs",
        \\{"fields":["id","title"],"limit":5}
    );
    defer owned.deinit(std.testing.allocator);

    try std.testing.expectEqual(@as(usize, 2), owned.req.fields.len);
    try std.testing.expect(owned.req.defer_stored_projection);
}

test "query parser defaults to key-only when fields are omitted" {
    var owned = try parseQueryRequest(std.testing.allocator, null, "docs",
        \\{"limit":5}
    );
    defer owned.deinit(std.testing.allocator);

    try std.testing.expectEqual(@as(usize, 0), owned.req.fields.len);
    try std.testing.expectEqual(false, owned.req.include_all_fields);
    try std.testing.expectEqual(false, owned.req.include_stored);
    try std.testing.expectEqual(false, owned.req.defer_stored_projection);
}

test "query parser keeps special stored projection in db layer" {
    var owned = try parseQueryRequest(std.testing.allocator, null, "docs",
        \\{"fields":["title","_chunks.*"],"limit":5}
    );
    defer owned.deinit(std.testing.allocator);

    try std.testing.expectEqual(@as(usize, 2), owned.req.fields.len);
    try std.testing.expect(!owned.req.defer_stored_projection);
}

test "query parser accepts generated count and profile flags" {
    const metadata_openapi = @import("antfly_metadata_openapi");

    var full_text = try std.json.parseFromSlice(std.json.Value, std.testing.allocator,
        \\{"match":{"field":"body","text":"alpha"}}
    , .{});
    defer full_text.deinit();

    const body = try jsonStringifyAlloc(std.testing.allocator, metadata_openapi.QueryRequest{
        .full_text_search = full_text.value,
        .count = true,
        .profile = true,
    });
    defer std.testing.allocator.free(body);

    var owned = try parseQueryRequest(std.testing.allocator, null, "docs", body);
    defer owned.deinit(std.testing.allocator);
    try std.testing.expect(owned.req.count_only);
    try std.testing.expect(owned.req.profile);
}

test "query parser accepts aggregations" {
    var owned = try parseQueryRequest(std.testing.allocator, null, "docs",
        \\{"full_text_search":{"match":{"field":"body","text":"alpha"}},"aggregations":{"price_stats":{"type":"stats","field":"price"},"categories":{"type":"terms","field":"category","size":5}}}
    );
    defer owned.deinit(std.testing.allocator);

    try std.testing.expect(owned.req.aggregations_json.len > 0);
    try std.testing.expect(std.mem.indexOf(u8, owned.req.aggregations_json, "\"price_stats\"") != null);
    try std.testing.expect(std.mem.indexOf(u8, owned.req.aggregations_json, "\"categories\"") != null);
}

test "query parser accepts bleve match query shape" {
    var owned = try parseQueryRequest(std.testing.allocator, null, "docs",
        \\{"full_text_search":{"match":"alpha","field":"body"},"limit":5}
    );
    defer owned.deinit(std.testing.allocator);
    try std.testing.expectEqual(@as(u32, 5), owned.req.limit);
    try std.testing.expect(owned.req.full_text != null);
    try std.testing.expect(owned.req.full_text.? == .match);
    try std.testing.expectEqualStrings("body", owned.req.full_text.?.match.field);
    try std.testing.expectEqualStrings("alpha", owned.req.full_text.?.match.text);
}

test "query parser accepts bleve match_all query shape" {
    var owned = try parseQueryRequest(std.testing.allocator, null, "docs",
        \\{"full_text_search":{"match_all":{}},"limit":5}
    );
    defer owned.deinit(std.testing.allocator);
    try std.testing.expectEqual(@as(u32, 5), owned.req.limit);
    try std.testing.expect(owned.req.full_text != null);
    try std.testing.expect(owned.req.full_text.? == .match_all);
}

test "query parser accepts bleve boolean filter shape" {
    var owned = try parseQueryRequest(std.testing.allocator, null, "docs",
        \\{"full_text_search":{"filter":{"match_all":{}}}}
    );
    defer owned.deinit(std.testing.allocator);
    try std.testing.expect(owned.req.full_text != null);
    try std.testing.expect(owned.req.full_text.? == .bool_query);
}

test "query parser preserves filter and exclusion request JSON" {
    var owned = try parseQueryRequest(std.testing.allocator, null, "docs",
        \\{"full_text_search":{"match":"alpha","field":"body"},"filter_query":{"term":"published","field":"status"},"exclusion_query":{"term":"draft","field":"status"}}
    );
    defer owned.deinit(std.testing.allocator);
    try std.testing.expect(owned.req.full_text != null);
    try std.testing.expect(owned.req.full_text.? == .match);
    try std.testing.expect(std.mem.indexOf(u8, owned.req.filter_query_json, "\"term\":{\"status\":\"published\"}") != null);
    try std.testing.expect(std.mem.indexOf(u8, owned.req.exclusion_query_json, "\"term\":{\"status\":\"draft\"}") != null);
}

test "query parser does not use dense fast path when public filters are present" {
    var owned = try parseQueryRequest(std.testing.allocator, null, "docs",
        \\{"embeddings":{"dense_idx":[0.1,0.2]},"indexes":["dense_idx"],"filter_query":{"term":{"status":"published"}},"exclusion_query":{"term":{"status":"draft"}},"limit":5}
    );
    defer owned.deinit(std.testing.allocator);
    try std.testing.expectEqual(@as(usize, 1), owned.req.dense_queries.len);
    try std.testing.expect(std.mem.indexOf(u8, owned.req.filter_query_json, "\"status\":\"published\"") != null);
    try std.testing.expect(std.mem.indexOf(u8, owned.req.exclusion_query_json, "\"status\":\"draft\"") != null);
}

test "query parser accepts typed bleve leaf queries through db full_text" {
    var fuzzy = try parseQueryRequest(std.testing.allocator, null, "docs",
        \\{"full_text_search":{"term":"alph","field":"body","fuzziness":1}}
    );
    defer fuzzy.deinit(std.testing.allocator);
    try std.testing.expect(fuzzy.req.full_text != null);
    try std.testing.expect(fuzzy.req.full_text.? == .fuzzy or fuzzy.req.full_text.? == .term);
    switch (fuzzy.req.full_text.?) {
        .fuzzy => |q| try std.testing.expectEqualStrings("alph", q.term),
        .term => |q| try std.testing.expectEqualStrings("alph", q.term),
        else => return error.TestUnexpectedResult,
    }

    var numeric = try parseQueryRequest(std.testing.allocator, null, "docs",
        \\{"full_text_search":{"field":"score","min":10,"max":20,"inclusive_max":true}}
    );
    defer numeric.deinit(std.testing.allocator);
    try std.testing.expect(numeric.req.full_text != null);
    try std.testing.expect(numeric.req.full_text.? == .numeric_range);
    try std.testing.expectEqual(@as(f64, 10), numeric.req.full_text.?.numeric_range.min.?);
    try std.testing.expectEqual(@as(f64, 20), numeric.req.full_text.?.numeric_range.max.?);
    try std.testing.expectEqual(true, numeric.req.full_text.?.numeric_range.inclusive_max);

    var date_range = try parseQueryRequest(std.testing.allocator, null, "docs",
        \\{"full_text_search":{"field":"created_at","start":"2026-03-01T00:00:00Z","end":"2026-03-31","inclusive_end":true}}
    );
    defer date_range.deinit(std.testing.allocator);
    try std.testing.expect(date_range.req.full_text != null);
    try std.testing.expect(date_range.req.full_text.? == .date_range);
    try std.testing.expect(date_range.req.full_text.?.date_range.start_ns != null);
    try std.testing.expect(date_range.req.full_text.?.date_range.end_ns != null);
    try std.testing.expectEqual(true, date_range.req.full_text.?.date_range.inclusive_end);
}

test "query parser accepts bleve query string queries" {
    var owned = try parseQueryRequest(std.testing.allocator, null, "docs",
        \\{"full_text_search":{"query":"body:alpha AND title:\"beta gamma\""},"limit":5}
    );
    defer owned.deinit(std.testing.allocator);

    try std.testing.expectEqual(@as(u32, 5), owned.req.limit);
    try std.testing.expect(owned.req.full_text != null);
    try std.testing.expect(owned.req.full_text.? == .bool_query);
    const root = owned.req.full_text.?.bool_query;
    try std.testing.expectEqual(@as(usize, 2), root.must.len);
    try std.testing.expect(root.must[0] == .match);
    try std.testing.expectEqualStrings("body", root.must[0].match.field);
    try std.testing.expectEqualStrings("alpha", root.must[0].match.text);
    try std.testing.expect(root.must[1] == .match_phrase);
    try std.testing.expectEqualStrings("title", root.must[1].match_phrase.field);
    try std.testing.expectEqualStrings("beta gamma", root.must[1].match_phrase.text);
}

test "query parser accepts bleve query string boosts" {
    var owned = try parseQueryRequest(std.testing.allocator, null, "docs",
        \\{"full_text_search":{"query":"body:alpha^2 AND title:\"beta gamma\"~3^4"}}
    );
    defer owned.deinit(std.testing.allocator);

    try std.testing.expect(owned.req.full_text != null);
    try std.testing.expect(owned.req.full_text.? == .bool_query);
    const root = owned.req.full_text.?.bool_query;
    try std.testing.expectEqual(@as(usize, 2), root.must.len);
    try std.testing.expect(root.must[0] == .match);
    try std.testing.expectApproxEqAbs(@as(f32, 2.0), root.must[0].match.boost, 0.0001);
    try std.testing.expect(root.must[1] == .match_phrase);
    try std.testing.expectApproxEqAbs(@as(f32, 4.0), root.must[1].match_phrase.boost, 0.0001);
}

test "query parser accepts bleve query string field groups" {
    var owned = try parseQueryRequest(std.testing.allocator, null, "docs",
        \\{"full_text_search":{"query":"title:(alpha beta)"}}
    );
    defer owned.deinit(std.testing.allocator);

    try std.testing.expect(owned.req.full_text != null);
    try std.testing.expect(owned.req.full_text.? == .bool_query);
    const root = owned.req.full_text.?.bool_query;
    try std.testing.expectEqual(@as(usize, 2), root.must.len);
    try std.testing.expect(root.must[0] == .match);
    try std.testing.expect(root.must[1] == .match);
    try std.testing.expectEqualStrings("title", root.must[0].match.field);
    try std.testing.expectEqualStrings("alpha", root.must[0].match.text);
    try std.testing.expectEqualStrings("title", root.must[1].match.field);
    try std.testing.expectEqualStrings("beta", root.must[1].match.text);
}

test "query parser accepts bleve query string inline ranges" {
    var numeric = try parseQueryRequest(std.testing.allocator, null, "docs",
        \\{"full_text_search":{"query":"score:[10 TO 20}"}}
    );
    defer numeric.deinit(std.testing.allocator);

    try std.testing.expect(numeric.req.full_text != null);
    try std.testing.expect(numeric.req.full_text.? == .numeric_range);
    try std.testing.expectEqual(@as(f64, 10), numeric.req.full_text.?.numeric_range.min.?);
    try std.testing.expectEqual(@as(f64, 20), numeric.req.full_text.?.numeric_range.max.?);
    try std.testing.expect(numeric.req.full_text.?.numeric_range.inclusive_min);
    try std.testing.expect(!numeric.req.full_text.?.numeric_range.inclusive_max);

    var date = try parseQueryRequest(std.testing.allocator, null, "docs",
        \\{"full_text_search":{"query":"created:[2024-01-01T00:00:00Z TO 2024-12-31T00:00:00Z]"}}
    );
    defer date.deinit(std.testing.allocator);

    try std.testing.expect(date.req.full_text != null);
    try std.testing.expect(date.req.full_text.? == .date_range);
    try std.testing.expect(date.req.full_text.?.date_range.start_ns != null);
    try std.testing.expect(date.req.full_text.?.date_range.end_ns != null);

    var term = try parseQueryRequest(std.testing.allocator, null, "docs",
        \\{"full_text_search":{"query":"title:[alpha TO omega]"}}
    );
    defer term.deinit(std.testing.allocator);

    try std.testing.expect(term.req.full_text != null);
    try std.testing.expect(term.req.full_text.? == .term_range);
    try std.testing.expectEqualStrings("alpha", term.req.full_text.?.term_range.min.?);
    try std.testing.expectEqualStrings("omega", term.req.full_text.?.term_range.max.?);
}

test "query parser accepts bleve query string filters" {
    var owned = try parseQueryRequest(std.testing.allocator, null, "docs",
        \\{"full_text_search":{"query":"alpha"},"filter_query":{"query":"status:published OR status:review"}}
    );
    defer owned.deinit(std.testing.allocator);

    try std.testing.expect(owned.req.full_text != null);
    try std.testing.expect(owned.req.full_text.? == .match);
    try std.testing.expect(std.mem.indexOf(u8, owned.req.filter_query_json, "\"bool\"") != null);
    try std.testing.expect(std.mem.indexOf(u8, owned.req.filter_query_json, "\"status\":\"published\"") != null);
    try std.testing.expect(std.mem.indexOf(u8, owned.req.filter_query_json, "\"status\":\"review\"") != null);
}

test "query parser rejects invalid bleve date ranges" {
    try std.testing.expectError(error.UnsupportedQueryRequest, parseQueryRequest(std.testing.allocator, null, "docs",
        \\{"full_text_search":{"field":"created_at","start":"not-a-date"}}
    ));
}

test "query parser resolves semantic search into dense query" {
    var owned = try parseQueryRequest(std.testing.allocator, FakeSemanticResolver.iface(), "docs",
        \\{"semantic_search":"alpha concept","indexes":["semantic_idx"],"limit":4}
    );
    defer owned.deinit(std.testing.allocator);

    try std.testing.expectEqual(@as(usize, 1), owned.req.dense_queries.len);
    try std.testing.expectEqualStrings("semantic_idx", owned.req.dense_queries[0].index_name);
    try std.testing.expectEqual(@as(u32, 4), owned.req.dense_queries[0].query.k);
    try std.testing.expectEqual(@as(usize, 3), owned.req.dense_queries[0].query.vector.len);
}

test "query parser preserves search effort for semantic search" {
    var owned = try parseQueryRequest(std.testing.allocator, FakeSemanticResolver.iface(), "docs",
        \\{"semantic_search":"alpha concept","indexes":["semantic_idx"],"limit":4,"search_effort":0.3}
    );
    defer owned.deinit(std.testing.allocator);

    try std.testing.expect(owned.req.search_effort != null);
    try std.testing.expectApproxEqAbs(@as(f32, 0.3), owned.req.search_effort.?, 0.0001);
}

test "query parser accepts semantic embedding template" {
    var owned = try parseQueryRequest(std.testing.allocator, FakeSemanticResolver.iface(), "docs",
        \\{"semantic_search":"alpha concept","embedding_template":"{{remotePDF url=this}}","indexes":["semantic_idx"],"limit":4}
    );
    defer owned.deinit(std.testing.allocator);

    try std.testing.expectEqual(@as(usize, 1), owned.req.dense_queries.len);
    try std.testing.expectEqualStrings("semantic_idx", owned.req.dense_queries[0].index_name);
}

test "query parser accepts precomputed embedding payload" {
    var owned = try parseQueryRequest(std.testing.allocator, null, "docs",
        \\{"embeddings":{"semantic_idx":[0.5,1.5,2.5]},"limit":6}
    );
    defer owned.deinit(std.testing.allocator);

    try std.testing.expectEqual(@as(usize, 1), owned.req.dense_queries.len);
    try std.testing.expectEqualStrings("semantic_idx", owned.req.dense_queries[0].index_name);
    try std.testing.expectEqual(@as(u32, 6), owned.req.dense_queries[0].query.k);
    try std.testing.expectEqual(@as(usize, 3), owned.req.dense_queries[0].query.vector.len);
    try std.testing.expectEqual(@as(f32, 1.5), owned.req.dense_queries[0].query.vector[1]);
}

test "query parser accepts packed dense embedding payload" {
    var owned = try parseQueryRequest(std.testing.allocator, null, "docs",
        \\{"embeddings":{"semantic_idx":"AAAAPwAAwD8AACBA"},"indexes":["semantic_idx"],"fields":["title"],"search_effort":0.3,"limit":6}
    );
    defer owned.deinit(std.testing.allocator);

    try std.testing.expectEqual(@as(usize, 1), owned.req.dense_queries.len);
    try std.testing.expectEqualStrings("semantic_idx", owned.req.dense_queries[0].index_name);
    try std.testing.expectEqual(@as(u32, 6), owned.req.dense_queries[0].query.k);
    try std.testing.expectEqual(@as(usize, 3), owned.req.dense_queries[0].query.vector.len);
    try std.testing.expectApproxEqAbs(@as(f32, 0.5), owned.req.dense_queries[0].query.vector[0], 0.0001);
    try std.testing.expectApproxEqAbs(@as(f32, 1.5), owned.req.dense_queries[0].query.vector[1], 0.0001);
    try std.testing.expectApproxEqAbs(@as(f32, 2.5), owned.req.dense_queries[0].query.vector[2], 0.0001);
    try std.testing.expectEqual(@as(?f32, 0.3), owned.req.search_effort);
    try std.testing.expect(owned.req.defer_stored_projection);
}

test "query parser rejects packed dense indexes that reference a missing embedding" {
    try std.testing.expectError(error.UnsupportedQueryRequest, parseQueryRequest(std.testing.allocator, null, "docs",
        \\{"embeddings":{"semantic_idx":"AAAAPwAAwD8AACBA"},"indexes":["missing_idx"],"limit":6}
    ));
}

test "query parser rejects invalid packed dense embedding payload" {
    try std.testing.expectError(error.InvalidQueryRequest, parseQueryRequest(std.testing.allocator, null, "docs",
        \\{"embeddings":{"semantic_idx":"not-base64"},"indexes":["semantic_idx"],"limit":6}
    ));
}

test "query parser accepts sparse embedding payload" {
    var owned = try parseQueryRequest(std.testing.allocator, null, "docs",
        \\{"embeddings":{"sparse_idx":{"indices":[1,7],"values":[0.4,0.9]}},"limit":6}
    );
    defer owned.deinit(std.testing.allocator);

    try std.testing.expectEqual(@as(usize, 1), owned.req.sparse_queries.len);
    try std.testing.expectEqualStrings("sparse_idx", owned.req.sparse_queries[0].index_name);
    try std.testing.expectEqual(@as(u32, 6), owned.req.sparse_queries[0].query.k);
    try std.testing.expectEqual(@as(usize, 2), owned.req.sparse_queries[0].query.indices.len);
    try std.testing.expectEqual(@as(u32, 7), owned.req.sparse_queries[0].query.indices[1]);
}

test "query parser accepts merge config reranker and pruner" {
    var owned = try parseQueryRequest(std.testing.allocator, FakeSemanticResolver.iface(), "docs",
        \\{"semantic_search":"alpha concept","indexes":["semantic_idx"],"full_text_search":{"match":{"field":"body","text":"alpha concept"}},"merge_config":{"strategy":"rsf","window_size":25,"rank_constant":42.0,"weights":{"full_text":0.5,"semantic_idx":1.5}},"reranker":{"provider":"antfly","model":"cross-encoder/ms-marco-MiniLM-L-6-v2","field":"body","top_n":3},"pruner":{"min_score_ratio":0.5,"require_multi_index":true},"limit":6}
    );
    defer owned.deinit(std.testing.allocator);

    try std.testing.expect(owned.req.merge_config != null);
    try std.testing.expectEqual(.rsf, owned.req.merge_config.?.strategy);
    try std.testing.expectEqual(@as(u32, 25), owned.req.merge_config.?.window_size);
    try std.testing.expectEqual(@as(usize, 2), owned.req.merge_config.?.weights.len);
    try std.testing.expect(owned.req.reranker != null);
    try std.testing.expectEqual(.antfly, owned.req.reranker.?.provider);
    try std.testing.expectEqualStrings("body", owned.req.reranker.?.field);
    try std.testing.expectEqual(@as(?u32, 3), owned.req.reranker.?.top_n);
    try std.testing.expectEqualStrings("alpha concept", owned.req.reranker_query_text);
    try std.testing.expect(owned.req.include_stored);
    try std.testing.expect(owned.req.pruner != null);
    try std.testing.expectEqual(@as(f64, 0.5), owned.req.pruner.?.min_score_ratio);
    try std.testing.expect(owned.req.pruner.?.require_multi_index);
}

test "query parser keeps stored documents for dense reranking without fields" {
    var owned = try parseQueryRequest(std.testing.allocator, null, "docs",
        \\{"embeddings":{"dense_idx":[1.0,0.0,0.0]},"indexes":["dense_idx"],"reranker":{"provider":"antfly","model":"cross-encoder/ms-marco-MiniLM-L-6-v2","field":"body","top_n":2},"limit":6}
    );
    defer owned.deinit(std.testing.allocator);

    try std.testing.expect(owned.req.reranker != null);
    try std.testing.expect(owned.req.include_stored);
}

test "query parser accepts graph searches" {
    var owned = try parseQueryRequest(std.testing.allocator, null, "docs",
        \\{"graph_searches":{"neighbors":{"type":"neighbors","index_name":"graph_idx","start_nodes":{"keys":["doc:a"]},"params":{"edge_types":["links"]}}},"limit":10}
    );
    defer owned.deinit(std.testing.allocator);

    try std.testing.expectEqual(@as(usize, 1), owned.req.graph_queries.len);
    try std.testing.expectEqualStrings("neighbors", owned.req.graph_queries[0].name);
    try std.testing.expectEqualStrings("graph_idx", owned.req.graph_queries[0].query.index_name);
    try std.testing.expect(owned.req.graph_queries[0].query.query_type == .neighbors);
    switch (owned.req.graph_queries[0].query.start_nodes) {
        .keys => |keys| try std.testing.expectEqualStrings("doc:a", keys[0]),
        else => return error.TestUnexpectedResult,
    }
}

test "query parser accepts graph pattern searches" {
    var owned = try parseQueryRequest(std.testing.allocator, null, "docs",
        \\{"graph_searches":{"pattern_walk":{"type":"pattern","index_name":"graph_idx","start_nodes":{"keys":["doc:a"]},"pattern":[{"alias":"a"},{"alias":"b","edge":{"types":["links"],"direction":"out","min_hops":1,"max_hops":2}}],"return_aliases":["b"],"include_documents":true}},"limit":10}
    );
    defer owned.deinit(std.testing.allocator);

    try std.testing.expectEqual(@as(usize, 1), owned.req.graph_queries.len);
    try std.testing.expect(owned.req.graph_queries[0].query.query_type == .pattern);
    try std.testing.expectEqual(@as(usize, 2), owned.req.graph_queries[0].query.pattern.len);
    try std.testing.expectEqual(@as(usize, 1), owned.req.graph_queries[0].query.return_aliases.len);
    try std.testing.expect(owned.req.graph_queries[0].query.include_documents);
    try std.testing.expect(owned.req.graph_queries[0].query.include_all_fields);
}

test "query parser treats explicit graph document fields as a projection" {
    var owned = try parseQueryRequest(std.testing.allocator, null, "docs",
        \\{"graph_searches":{"pattern_walk":{"type":"pattern","index_name":"graph_idx","start_nodes":{"keys":["doc:a"]},"pattern":[{"alias":"a"}],"include_documents":true,"fields":["title"]}},"limit":10}
    );
    defer owned.deinit(std.testing.allocator);

    const graph_query = owned.req.graph_queries[0].query;
    try std.testing.expect(!graph_query.include_all_fields);
    try std.testing.expectEqual(@as(usize, 1), graph_query.fields.len);
    try std.testing.expectEqualStrings("title", graph_query.fields[0]);
}

test "query parser rejects semantic search offsets" {
    try std.testing.expectError(error.UnsupportedQueryRequest, parseQueryRequest(std.testing.allocator, FakeSemanticResolver.iface(), "docs",
        \\{"semantic_search":"alpha concept","indexes":["semantic_idx"],"limit":4,"offset":1}
    ));
}

test "query parser records approximate source diagnostic for semantic exact sort" {
    db_mod.resetLastSortRejectionDiagnostic();
    try std.testing.expectError(error.UnsupportedQueryRequest, parseQueryRequest(std.testing.allocator, FakeSemanticResolver.iface(), "docs",
        \\{"semantic_search":"alpha concept","indexes":["semantic_idx"],"order_by":[{"field":"created_at","desc":true}],"limit":4}
    ));
    const diagnostic = db_mod.takeLastSortRejectionDiagnostic() orelse return error.TestUnexpectedResult;
    try std.testing.expectEqualStrings("created_at", diagnostic.field);
    try std.testing.expectEqualStrings("approximate_candidate_source", diagnostic.reason);
    try std.testing.expectEqualStrings("approximate_candidate_source", diagnostic.detail);
}

test "query parser rejects semantic cursor-only pagination as approximate source" {
    db_mod.resetLastSortRejectionDiagnostic();
    try std.testing.expectError(error.UnsupportedQueryRequest, parseQueryRequest(std.testing.allocator, FakeSemanticResolver.iface(), "docs",
        \\{"semantic_search":"alpha concept","indexes":["semantic_idx"],"search_after":["doc:a"],"limit":4}
    ));
    const diagnostic = db_mod.takeLastSortRejectionDiagnostic() orelse return error.TestUnexpectedResult;
    try std.testing.expectEqualStrings("_id", diagnostic.field);
    try std.testing.expectEqualStrings("approximate_candidate_source", diagnostic.reason);
    try std.testing.expectEqualStrings("approximate_candidate_source", diagnostic.detail);
}

test "query parser rejects semantic search_before pagination as approximate source" {
    db_mod.resetLastSortRejectionDiagnostic();
    try std.testing.expectError(error.UnsupportedQueryRequest, parseQueryRequest(std.testing.allocator, FakeSemanticResolver.iface(), "docs",
        \\{"semantic_search":"alpha concept","indexes":["semantic_idx"],"search_before":["doc:a"],"limit":4}
    ));
    const diagnostic = db_mod.takeLastSortRejectionDiagnostic() orelse return error.TestUnexpectedResult;
    try std.testing.expectEqualStrings("_id", diagnostic.field);
    try std.testing.expectEqualStrings("approximate_candidate_source", diagnostic.reason);
    try std.testing.expectEqualStrings("approximate_candidate_source", diagnostic.detail);
}

test "query parser rejects semantic score sort as approximate source" {
    const alloc = std.testing.allocator;
    db_mod.resetLastSortRejectionDiagnostic();
    try std.testing.expectError(error.UnsupportedQueryRequest, parseQueryRequest(alloc, FakeSemanticResolver.iface(), "docs",
        \\{"semantic_search":"alpha concept","indexes":["semantic_idx"],"order_by":[{"field":"_score","desc":true}],"limit":4}
    ));
    const diagnostic = db_mod.takeLastSortRejectionDiagnostic() orelse return error.TestUnexpectedResult;
    try std.testing.expectEqualStrings("_score", diagnostic.field);
    try std.testing.expectEqualStrings("approximate_candidate_source", diagnostic.reason);
    try std.testing.expectEqualStrings("approximate_candidate_source", diagnostic.detail);
}

test "query encoder emits antfly-style response envelope" {
    const alloc = std.testing.allocator;
    var hits = try alloc.alloc(db_mod.types.SearchHit, 1);
    hits[0] = .{
        .id = try alloc.dupe(u8, "doc:a"),
        .score = 1.25,
        .stored_data = try alloc.dupe(u8, "{\"title\":\"alpha\"}"),
    };
    var result = db_mod.types.SearchResult{
        .alloc = alloc,
        .hits = hits,
        .total_hits = 1,
    };
    defer result.deinit();

    var encoded = try encodeQueryResponses(alloc, "docs", .{}, .{}, result);
    defer encoded.deinit(alloc);
    try std.testing.expect(std.mem.indexOf(u8, encoded.json, "\"responses\"") != null);
    try std.testing.expect(std.mem.indexOf(u8, encoded.json, "\"_id\":\"doc:a\"") != null);
    try std.testing.expect(std.mem.indexOf(u8, encoded.json, "\"table\":\"docs\"") != null);
}

test "query encoder does not expose internal doc ordinals" {
    const alloc = std.testing.allocator;
    var hits = try alloc.alloc(db_mod.types.SearchHit, 1);
    hits[0] = .{
        .id = try alloc.dupe(u8, "doc:a"),
        .doc_ordinal = 42,
        .native_text_doc_id = 7,
        .score = 1.25,
        .stored_data = try alloc.dupe(u8, "{\"title\":\"alpha\"}"),
    };
    var result = db_mod.types.SearchResult{
        .alloc = alloc,
        .hits = hits,
        .total_hits = 1,
    };
    defer result.deinit();

    var encoded = try encodeQueryResponses(alloc, "docs", .{}, .{}, result);
    defer encoded.deinit(alloc);
    try std.testing.expect(std.mem.indexOf(u8, encoded.json, "\"_id\":\"doc:a\"") != null);
    try std.testing.expect(std.mem.indexOf(u8, encoded.json, "doc_ordinal") == null);
    try std.testing.expect(std.mem.indexOf(u8, encoded.json, "native_text_doc_id") == null);
    try std.testing.expect(std.mem.indexOf(u8, encoded.json, "ordinal") == null);
}

test "query encoder emits aggregations" {
    const alloc = std.testing.allocator;
    var hits = try alloc.alloc(db_mod.types.SearchHit, 1);
    hits[0] = .{
        .id = try alloc.dupe(u8, "doc:a"),
        .score = 1.25,
        .stored_data = try alloc.dupe(u8, "{\"title\":\"alpha\",\"price\":10,\"category\":\"books\"}"),
    };
    var result = db_mod.types.SearchResult{
        .alloc = alloc,
        .hits = hits,
        .total_hits = 1,
    };
    defer result.deinit();

    const aggregation_results = try alloc.alloc(db_mod.aggregations.SearchAggregationResult, 2);
    aggregation_results[0] = .{
        .name = "price_stats",
        .field = "price",
        .type = "stats",
        .value_json = try alloc.dupe(u8, "{\"count\":1,\"sum\":10,\"avg\":10,\"min\":10,\"max\":10,\"sum_squares\":100,\"variance\":0,\"std_dev\":0}"),
    };
    const buckets = try alloc.alloc(db_mod.aggregations.SearchAggregationBucket, 1);
    buckets[0] = .{
        .key_json = try alloc.dupe(u8, "\"books\""),
        .count = 1,
    };
    aggregation_results[1] = .{
        .name = "categories",
        .field = "category",
        .type = "terms",
        .buckets = buckets,
    };

    var meta: QueryResponseMeta = .{
        .aggregation_results = aggregation_results,
    };
    defer meta.deinit(alloc);

    var encoded = try encodeQueryResponses(alloc, "docs", .{
        .aggregations_json =
        \\{"price_stats":{"type":"stats","field":"price"},"categories":{"type":"terms","field":"category","size":5}}
        ,
    }, meta, result);
    defer encoded.deinit(alloc);

    try std.testing.expect(std.mem.indexOf(u8, encoded.json, "\"aggregations\"") != null);
    try std.testing.expect(std.mem.indexOf(u8, encoded.json, "\"price_stats\"") != null);
    try std.testing.expect(std.mem.indexOf(u8, encoded.json, "\"sum\":10") != null);
    try std.testing.expect(std.mem.indexOf(u8, encoded.json, "\"categories\"") != null);
    try std.testing.expect(std.mem.indexOf(u8, encoded.json, "\"key\":\"books\"") != null);
}

test "query encoder supports count-only and profile responses" {
    const alloc = std.testing.allocator;
    var hits = try alloc.alloc(db_mod.types.SearchHit, 1);
    hits[0] = .{
        .id = try alloc.dupe(u8, "doc:a"),
        .score = 1.25,
        .stored_data = try alloc.dupe(u8, "{\"title\":\"alpha\"}"),
    };
    var result = db_mod.types.SearchResult{
        .alloc = alloc,
        .hits = hits,
        .total_hits = 1,
    };
    defer result.deinit();

    var encoded = try encodeQueryResponses(alloc, "docs", .{ .count_only = true, .profile = true }, .{
        .took_ms = 7,
        .shard_count = 3,
        .merged = true,
        .dense_search = .{
            .resolved_search_width = 128,
            .resolved_epsilon = 0.15,
            .hbc_reranked_vectors = 42,
            .hbc_search_ns = 123456,
        },
    }, result);
    defer encoded.deinit(alloc);
    try std.testing.expect(std.mem.indexOf(u8, encoded.json, "\"total\":1") != null);
    try std.testing.expect(std.mem.indexOf(u8, encoded.json, "\"hits\":[]") != null);
    try std.testing.expect(std.mem.indexOf(u8, encoded.json, "\"profile\"") != null);
    try std.testing.expect(std.mem.indexOf(u8, encoded.json, "\"took\":7") != null);
    try std.testing.expect(std.mem.indexOf(u8, encoded.json, "\"shards\":{\"total\":3,\"successful\":3,\"failed\":0}") != null);
    try std.testing.expect(std.mem.indexOf(u8, encoded.json, "\"merge\":{\"strategy\":\"rrf\"") != null);
    try std.testing.expect(std.mem.indexOf(u8, encoded.json, "\"dense_search\":{\"total_ns\":0") != null);
    try std.testing.expect(std.mem.indexOf(u8, encoded.json, "\"resolved_search_width\":128") != null);
    try std.testing.expect(std.mem.indexOf(u8, encoded.json, "\"resolved_epsilon\":0.15") != null);
    try std.testing.expect(std.mem.indexOf(u8, encoded.json, "\"hbc_reranked_vectors\":42") != null);
}

test "query encoder projects deferred stored fields without round-tripping bytes" {
    const alloc = std.testing.allocator;
    var hits = try alloc.alloc(db_mod.types.SearchHit, 1);
    hits[0] = .{
        .id = try alloc.dupe(u8, "doc:a"),
        .score = 1.25,
        .stored_data = try alloc.dupe(u8, "{\"title\":\"alpha\",\"id\":\"stored-id\",\"body\":\"hello\"}"),
    };
    var result = db_mod.types.SearchResult{
        .alloc = alloc,
        .hits = hits,
        .total_hits = 1,
    };
    defer result.deinit();

    var encoded = try encodeQueryResponses(alloc, "docs", .{
        .fields = &.{ "id", "title" },
        .include_all_fields = false,
        .defer_stored_projection = true,
    }, .{}, result);
    defer encoded.deinit(alloc);

    try std.testing.expect(std.mem.indexOf(u8, encoded.json, "\"_id\":\"doc:a\"") != null);
    try std.testing.expect(std.mem.indexOf(u8, encoded.json, "\"_source\":{\"id\":\"stored-id\",\"title\":\"alpha\"}") != null);
    try std.testing.expect(std.mem.indexOf(u8, encoded.json, "\"body\"") == null);
}

test "query encoder omits _source for key-only hits" {
    const alloc = std.testing.allocator;
    var hits = try alloc.alloc(db_mod.types.SearchHit, 1);
    hits[0] = .{
        .id = try alloc.dupe(u8, "doc:key-only"),
        .score = 0.75,
        .stored_data = null,
    };
    var result = db_mod.types.SearchResult{
        .alloc = alloc,
        .hits = hits,
        .total_hits = 1,
    };
    defer result.deinit();

    var encoded = try encodeQueryResponses(alloc, "docs", .{
        .include_all_fields = false,
    }, .{}, result);
    defer encoded.deinit(alloc);

    try std.testing.expect(std.mem.indexOf(u8, encoded.json, "\"_id\":\"doc:key-only\"") != null);
    try std.testing.expect(std.mem.indexOf(u8, encoded.json, "\"_source\"") == null);
}

test "query encoder emits graph results" {
    const alloc = std.testing.allocator;
    const graph_nodes = try alloc.alloc(graph_query_mod.GraphResultNode, 1);
    graph_nodes[0] = .{
        .key = try alloc.dupe(u8, "doc:b"),
        .depth = 1,
        .distance = 1,
        .path = null,
        .path_edges = null,
    };
    const graph_hits = try alloc.alloc(db_mod.types.SearchHit, 1);
    graph_hits[0] = .{
        .id = try alloc.dupe(u8, "doc:b"),
        .score = 1,
        .stored_data = try alloc.dupe(u8, "{\"title\":\"beta\"}"),
    };
    const graph_results = try alloc.alloc(db_mod.types.GraphSearchResult, 1);
    graph_results[0] = .{
        .name = try alloc.dupe(u8, "neighbors"),
        .nodes = graph_nodes,
        .paths = &.{},
        .hits = graph_hits,
        .total_hits = 1,
    };
    var result = db_mod.types.SearchResult{
        .alloc = alloc,
        .hits = &.{},
        .total_hits = 0,
        .graph_results = graph_results,
    };
    defer result.deinit();

    var encoded = try encodeQueryResponses(alloc, "docs", .{
        .graph_queries = &.{
            .{
                .name = "neighbors",
                .query = .{
                    .query_type = .neighbors,
                    .index_name = "graph_idx",
                    .start_nodes = .{ .keys = &.{"doc:a"} },
                },
            },
        },
    }, .{ .took_ms = 4 }, result);
    defer encoded.deinit(alloc);
    try ant_json.testing.expectSubsetJsonText(alloc,
        \\{"responses":[{"graph_results":{"neighbors":{"type":"neighbors","nodes":[{"key":"doc:b","document":{"title":"beta"}}]}}}]}
    , encoded.json);
}

test "query merge applies global score ordering and offset" {
    const alloc = std.testing.allocator;

    var left_hits = try alloc.alloc(db_mod.types.SearchHit, 2);
    left_hits[0] = .{
        .id = try alloc.dupe(u8, "doc:b"),
        .doc_ordinal = 2,
        .score = 2.0,
        .stored_data = try alloc.dupe(u8, "{\"title\":\"beta\"}"),
    };
    left_hits[1] = .{
        .id = try alloc.dupe(u8, "doc:a"),
        .doc_ordinal = 1,
        .score = 3.0,
        .stored_data = try alloc.dupe(u8, "{\"title\":\"alpha\"}"),
    };
    var right_hits = try alloc.alloc(db_mod.types.SearchHit, 1);
    right_hits[0] = .{
        .id = try alloc.dupe(u8, "doc:c"),
        .score = 1.0,
        .stored_data = try alloc.dupe(u8, "{\"title\":\"gamma\"}"),
    };

    var left = db_mod.types.SearchResult{ .alloc = alloc, .hits = left_hits, .total_hits = 2 };
    defer left.deinit();
    var right = db_mod.types.SearchResult{ .alloc = alloc, .hits = right_hits, .total_hits = 1 };
    defer right.deinit();

    var merged = try mergeSearchResults(alloc, .{ .full_text = .{ .match = .{ .field = "body", .text = "alpha" } } }, &.{ left, right }, 1, 1);
    defer merged.deinit();

    try std.testing.expectEqual(@as(u32, 3), merged.total_hits);
    try std.testing.expectEqual(@as(usize, 1), merged.hits.len);
    try std.testing.expectEqualStrings("doc:b", merged.hits[0].id);
    try std.testing.expectEqual(@as(?u32, null), merged.hits[0].doc_ordinal);
}

test "query merge allocation scales with the selected page" {
    const large_stored = "x" ** 1024;
    var input_hits: [2048]db_mod.types.SearchHit = undefined;
    for (&input_hits) |*hit| {
        hit.* = .{
            .id = @constCast("doc:a"),
            .stored_data = @constCast(large_stored),
        };
    }
    const input = db_mod.types.SearchResult{
        .alloc = std.testing.allocator,
        .hits = &input_hits,
        .total_hits = input_hits.len,
    };

    // Enough for a bounded top-one heap plus one cloned page hit, but not for
    // an O(candidate count) pointer array or cloned stored candidates.
    var backing: [4096]u8 = undefined;
    var fba = std.heap.FixedBufferAllocator.init(&backing);
    var merged = try mergeSearchResults(fba.allocator(), .{}, &.{input}, 0, 1);
    defer merged.deinit();

    try std.testing.expectEqual(@as(usize, 1), merged.hits.len);
    try std.testing.expectEqualStrings("doc:a", merged.hits[0].id);
    try std.testing.expectEqual(@as(usize, large_stored.len), merged.hits[0].stored_data.?.len);
}

test "query merge rejects score ordered hits without finite scores" {
    const alloc = std.testing.allocator;

    var missing_score_hits = try alloc.alloc(db_mod.types.SearchHit, 1);
    missing_score_hits[0] = .{
        .id = try alloc.dupe(u8, "doc:missing"),
    };
    var missing_score = db_mod.types.SearchResult{
        .alloc = alloc,
        .hits = missing_score_hits,
        .total_hits = 1,
    };
    defer missing_score.deinit();

    const scoring_req = db_mod.types.SearchRequest{
        .full_text = .{ .match = .{ .field = "body", .text = "alpha" } },
    };
    try std.testing.expectError(error.InvalidQueryRequest, mergeSearchResults(alloc, scoring_req, &.{missing_score}, 0, 10));

    var non_finite_hits = try alloc.alloc(db_mod.types.SearchHit, 1);
    non_finite_hits[0] = .{
        .id = try alloc.dupe(u8, "doc:nan"),
        .score = std.math.nan(f32),
    };
    var non_finite = db_mod.types.SearchResult{
        .alloc = alloc,
        .hits = non_finite_hits,
        .total_hits = 1,
    };
    defer non_finite.deinit();

    try std.testing.expectError(error.InvalidQueryRequest, mergeSearchResults(alloc, scoring_req, &.{non_finite}, 0, 10));
}

test "query merge orders non score bearing hits by id without requiring scores" {
    const alloc = std.testing.allocator;

    var hits = try alloc.alloc(db_mod.types.SearchHit, 2);
    hits[0] = .{
        .id = try alloc.dupe(u8, "doc:b"),
        .score = 100.0,
    };
    hits[1] = .{
        .id = try alloc.dupe(u8, "doc:a"),
    };

    var result = db_mod.types.SearchResult{ .alloc = alloc, .hits = hits, .total_hits = 2 };
    defer result.deinit();

    var merged = try mergeSearchResults(alloc, .{ .full_text = .{ .match_all = {} } }, &.{result}, 0, 10);
    defer merged.deinit();

    try std.testing.expectEqual(@as(usize, 2), merged.hits.len);
    try std.testing.expectEqualStrings("doc:a", merged.hits[0].id);
    try std.testing.expectEqualStrings("doc:b", merged.hits[1].id);
}

fn testSortedQueryHitAlloc(alloc: std.mem.Allocator, id: []const u8, rank: i64) !db_mod.types.SearchHit {
    const sort_values = try alloc.alloc(std.json.Value, 2);
    errdefer alloc.free(sort_values);
    sort_values[0] = .{ .integer = rank };
    sort_values[1] = .{ .string = try alloc.dupe(u8, id) };
    errdefer db_mod.types.deinitJsonValue(alloc, &sort_values[1]);
    return .{
        .id = try alloc.dupe(u8, id),
        .doc_ordinal = @intCast(@max(rank, 0)),
        .sort_values = sort_values,
    };
}

fn testIdSortedQueryHitAlloc(alloc: std.mem.Allocator, id: []const u8) !db_mod.types.SearchHit {
    const sort_values = try alloc.alloc(std.json.Value, 1);
    errdefer alloc.free(sort_values);
    sort_values[0] = .{ .string = try alloc.dupe(u8, id) };
    errdefer db_mod.types.deinitJsonValue(alloc, &sort_values[0]);
    return .{
        .id = try alloc.dupe(u8, id),
        .sort_values = sort_values,
    };
}

fn testScoreSortedQueryHitAlloc(alloc: std.mem.Allocator, id: []const u8, score: f32) !db_mod.types.SearchHit {
    const sort_values = try alloc.alloc(std.json.Value, 2);
    errdefer alloc.free(sort_values);
    sort_values[0] = .{ .float = @floatCast(score) };
    sort_values[1] = .{ .string = try alloc.dupe(u8, id) };
    errdefer db_mod.types.deinitJsonValue(alloc, &sort_values[1]);
    return .{
        .id = try alloc.dupe(u8, id),
        .score = score,
        .sort_values = sort_values,
    };
}

fn testHierarchyNavigationHitAlloc(
    alloc: std.mem.Allocator,
    id: []const u8,
    position: []const u8,
) !db_mod.types.SearchHit {
    const sort_values = try alloc.alloc(std.json.Value, 2);
    errdefer alloc.free(sort_values);
    sort_values[0] = .{ .string = try alloc.dupe(u8, position) };
    errdefer db_mod.types.deinitJsonValue(alloc, &sort_values[0]);
    sort_values[1] = .{ .string = try alloc.dupe(u8, id) };
    errdefer db_mod.types.deinitJsonValue(alloc, &sort_values[1]);
    return .{
        .id = try alloc.dupe(u8, id),
        .sort_values = sort_values,
    };
}

const TestHierarchyUnitChunk = struct { id: []const u8, score: f32 };

fn testHierarchyUnitHitForIdAlloc(
    alloc: std.mem.Allocator,
    unit_id: []const u8,
    score: f32,
    chunks: []const TestHierarchyUnitChunk,
) !db_mod.types.SearchHit {
    const id = try std.fmt.allocPrint(
        alloc,
        "doc:a/_artifact/asset/document_units_v1/{s}",
        .{unit_id},
    );
    defer alloc.free(id);
    const chunk_hits = try alloc.alloc(db_mod.types.ChunkHit, chunks.len);
    var initialized: usize = 0;
    errdefer {
        for (chunk_hits[0..initialized]) |*chunk| chunk.deinit(alloc);
        alloc.free(chunk_hits);
    }
    for (chunks, 0..) |chunk, i| {
        chunk_hits[i] = .{
            .id = try alloc.dupe(u8, chunk.id),
            .score = chunk.score,
        };
        initialized += 1;
    }
    return .{
        .id = try alloc.dupe(u8, id),
        .score = score,
        .stored_data = try alloc.dupe(u8, "{\"_hierarchy_unit_revision_token\":\"revision-a\"}"),
        .artifact_ref = .{
            .document_id = try alloc.dupe(u8, "doc:a"),
            .name = try alloc.dupe(u8, "document_units_v1"),
            .kind = .asset,
            .unit_id = try alloc.dupe(u8, unit_id),
        },
        .chunk_hits = chunk_hits,
    };
}

fn testHierarchyUnitHitAlloc(
    alloc: std.mem.Allocator,
    score: f32,
    chunks: []const TestHierarchyUnitChunk,
) !db_mod.types.SearchHit {
    return testHierarchyUnitHitForIdAlloc(alloc, "unit:0", score, chunks);
}

fn testHierarchyUnitShardResultAlloc(
    alloc: std.mem.Allocator,
    shard_index: usize,
    hit_count: usize,
) !db_mod.types.SearchResult {
    const hits = try alloc.alloc(db_mod.types.SearchHit, hit_count);
    var initialized: usize = 0;
    errdefer {
        for (hits[0..initialized]) |*hit| hit.deinit(alloc);
        alloc.free(hits);
    }
    for (hits, 0..) |*hit, hit_index| {
        const unit_id = try std.fmt.allocPrint(
            alloc,
            "unit:{d:0>2}:{d:0>3}",
            .{ shard_index, hit_index },
        );
        defer alloc.free(unit_id);
        const score: f32 = @floatFromInt(hit_count - hit_index);
        hit.* = try testHierarchyUnitHitForIdAlloc(alloc, unit_id, score, &.{});
        initialized += 1;
    }
    return .{
        .alloc = alloc,
        .hits = hits,
        .total_hits = @intCast(hit_count),
    };
}

test "query merge treats hierarchy navigation positions as opaque cursor values" {
    const alloc = std.testing.allocator;
    const order_by = [_]db_mod.types.SortField{
        .{ .field = "_hierarchy.position" },
        .{ .field = "_id" },
    };
    const cursor = [_]std.json.Value{
        .{ .string = "document_units_v1/00000000000000000007/00000000000000000000" },
        .{ .string = "artifact:page:1" },
    };

    var left_hits = try alloc.alloc(db_mod.types.SearchHit, 1);
    left_hits[0] = try testHierarchyNavigationHitAlloc(
        alloc,
        "artifact:page:1",
        "document_units_v1/00000000000000000007/00000000000000000000",
    );
    var right_hits = try alloc.alloc(db_mod.types.SearchHit, 1);
    right_hits[0] = try testHierarchyNavigationHitAlloc(
        alloc,
        "artifact:page:2",
        "document_units_v1/00000000000000000007/00000000000000000001",
    );
    var left = db_mod.types.SearchResult{ .alloc = alloc, .hits = left_hits, .total_hits = 2 };
    defer left.deinit();
    var right = db_mod.types.SearchResult{ .alloc = alloc, .hits = right_hits, .total_hits = 2 };
    defer right.deinit();

    var merged = try mergeSearchResultsWithRuntimeSchema(alloc, .{
        .hierarchy_children = .{ .parent_id = "doc:a" },
        .order_by = &order_by,
        .search_after = &cursor,
        .limit = 20,
    }, &.{ left, right }, 0, 20, .{});
    defer merged.deinit();

    // Duplicate parent plans use a logical maximum rather than inflating the
    // unit count, and the coordinator applies the opaque tuple cursor.
    try std.testing.expectEqual(@as(u32, 2), merged.total_hits);
    try std.testing.expectEqual(@as(usize, 1), merged.hits.len);
    try std.testing.expectEqualStrings("artifact:page:2", merged.hits[0].id);
}

test "query merge treats conflicting hierarchy navigation plans as retryable" {
    const alloc = std.testing.allocator;
    const order_by = [_]db_mod.types.SortField{
        .{ .field = "_hierarchy.position" },
        .{ .field = "_id" },
    };

    var left_hits = try alloc.alloc(db_mod.types.SearchHit, 1);
    left_hits[0] = try testHierarchyNavigationHitAlloc(alloc, "artifact:page:1", "hn3/revision-a/page/1");
    var right_hits = try alloc.alloc(db_mod.types.SearchHit, 1);
    right_hits[0] = try testHierarchyNavigationHitAlloc(alloc, "artifact:page:1", "hn3/revision-b/page/1");
    var left = db_mod.types.SearchResult{ .alloc = alloc, .hits = left_hits, .total_hits = 1 };
    defer left.deinit();
    var right = db_mod.types.SearchResult{ .alloc = alloc, .hits = right_hits, .total_hits = 1 };
    defer right.deinit();

    try std.testing.expectError(error.StorageReadTemporarilyUnavailable, mergeSearchResultsWithRuntimeSchema(
        alloc,
        .{
            .hierarchy_children = .{ .parent_id = "doc:a" },
            .order_by = &order_by,
            .limit = 20,
        },
        &.{ left, right },
        0,
        20,
        .{},
    ));
}

test "query merge treats malformed hierarchy navigation shard tuples as retryable" {
    const alloc = std.testing.allocator;
    const order_by = [_]db_mod.types.SortField{
        .{ .field = "_hierarchy.position" },
        .{ .field = "_id" },
    };
    var hits = try alloc.alloc(db_mod.types.SearchHit, 1);
    hits[0] = try testHierarchyNavigationHitAlloc(alloc, "artifact:page:1", "hn3/revision-a/page/1");
    alloc.free(hits[0].sort_values[1].string);
    hits[0].sort_values[1] = .{ .string = try alloc.dupe(u8, "artifact:wrong-tiebreaker") };
    var result = db_mod.types.SearchResult{ .alloc = alloc, .hits = hits, .total_hits = 1 };
    defer result.deinit();

    try std.testing.expectError(error.StorageReadTemporarilyUnavailable, mergeSearchResultsWithRuntimeSchema(
        alloc,
        .{
            .hierarchy_children = .{ .parent_id = "doc:a" },
            .order_by = &order_by,
            .limit = 20,
        },
        &.{result},
        0,
        20,
        .{},
    ));
}

test "query merge releases hierarchy navigation candidates once at the global budget" {
    const alloc = std.testing.allocator;
    const order_by = [_]db_mod.types.SortField{
        .{ .field = "_hierarchy.position" },
        .{ .field = "_id" },
    };
    const hit_count = db_mod.types.max_canonical_hierarchy_total_matches + 1;
    const hits = try alloc.alloc(db_mod.types.SearchHit, hit_count);
    var initialized: usize = 0;
    errdefer {
        for (hits[0..initialized]) |*hit| hit.deinit(alloc);
        alloc.free(hits);
    }
    for (hits, 0..) |*hit, i| {
        const id = try std.fmt.allocPrint(alloc, "unit:{d}", .{i});
        defer alloc.free(id);
        const position = try std.fmt.allocPrint(alloc, "position/{d:0>8}", .{i});
        defer alloc.free(position);
        hit.* = try testHierarchyNavigationHitAlloc(alloc, id, position);
        initialized += 1;
    }
    var result = db_mod.types.SearchResult{
        .alloc = alloc,
        .hits = hits,
        .total_hits = hit_count,
    };
    defer result.deinit();

    try std.testing.expectError(error.QueryCandidateBudgetExceeded, mergeSearchResultsWithRuntimeSchema(
        alloc,
        .{
            .hierarchy_children = .{ .parent_id = "doc:a" },
            .order_by = &order_by,
            .limit = 20,
        },
        &.{result},
        0,
        20,
        .{},
    ));
}

test "query merge globally coalesces hierarchy unit groups and bounded chunks" {
    const alloc = std.testing.allocator;
    var left_hits = try alloc.alloc(db_mod.types.SearchHit, 1);
    left_hits[0] = try testHierarchyUnitHitAlloc(alloc, 0.8, &.{
        .{ .id = "chunk:a", .score = 0.8 },
        .{ .id = "chunk:shared", .score = 0.4 },
    });
    var right_hits = try alloc.alloc(db_mod.types.SearchHit, 1);
    right_hits[0] = try testHierarchyUnitHitAlloc(alloc, 0.9, &.{
        .{ .id = "chunk:b", .score = 0.9 },
        .{ .id = "chunk:shared", .score = 0.5 },
    });
    var left = db_mod.types.SearchResult{ .alloc = alloc, .hits = left_hits, .total_hits = 1 };
    defer left.deinit();
    var right = db_mod.types.SearchResult{ .alloc = alloc, .hits = right_hits, .total_hits = 1 };
    defer right.deinit();

    var merged = try mergeSearchResults(alloc, .{
        .full_text = .{ .match = .{ .field = "body", .text = "alpha" } },
        .return_mode = .unit_with_chunks,
        .hierarchy_group_level = .unit,
        .hierarchy_grouped_matches = true,
        .max_chunks_per_parent = 2,
        .limit = 10,
    }, &.{ left, right }, 0, 10);
    defer merged.deinit();

    try std.testing.expectEqual(@as(u32, 1), merged.total_hits);
    try std.testing.expectEqual(db_mod.types.TotalHitsRelation.exact, merged.total_hits_relation);
    try std.testing.expectEqual(@as(usize, 1), merged.hits.len);
    try std.testing.expectEqual(@as(?f32, 0.9), merged.hits[0].score);
    try std.testing.expectEqual(@as(usize, 2), merged.hits[0].chunk_hits.len);
    try std.testing.expectEqualStrings("chunk:b", merged.hits[0].chunk_hits[0].id);
    try std.testing.expectEqualStrings("chunk:a", merged.hits[0].chunk_hits[1].id);
}

test "query merge treats conflicting hierarchy unit identities as retryable" {
    const alloc = std.testing.allocator;
    var left_hits = try alloc.alloc(db_mod.types.SearchHit, 1);
    left_hits[0] = try testHierarchyUnitHitAlloc(alloc, 0.8, &.{});
    var right_hits = try alloc.alloc(db_mod.types.SearchHit, 1);
    right_hits[0] = try testHierarchyUnitHitAlloc(alloc, 0.9, &.{});
    const right_ref = &right_hits[0].artifact_ref.?;
    alloc.free(right_ref.name);
    right_ref.name = try alloc.dupe(u8, "document_units_v2");

    var left = db_mod.types.SearchResult{ .alloc = alloc, .hits = left_hits, .total_hits = 1 };
    defer left.deinit();
    var right = db_mod.types.SearchResult{ .alloc = alloc, .hits = right_hits, .total_hits = 1 };
    defer right.deinit();

    try std.testing.expectError(error.StorageReadTemporarilyUnavailable, mergeSearchResults(
        alloc,
        .{
            .full_text = .{ .match = .{ .field = "body", .text = "alpha" } },
            .return_mode = .unit,
            .hierarchy_group_level = .unit,
            .limit = 10,
        },
        &.{ left, right },
        0,
        10,
    ));
}

test "query merge treats malformed hierarchy unit shard ranking as retryable" {
    const alloc = std.testing.allocator;
    var left_hits = try alloc.alloc(db_mod.types.SearchHit, 1);
    left_hits[0] = try testHierarchyUnitHitForIdAlloc(alloc, "unit:0", 0.8, &.{});
    left_hits[0].score = null;
    var right_hits = try alloc.alloc(db_mod.types.SearchHit, 1);
    right_hits[0] = try testHierarchyUnitHitForIdAlloc(alloc, "unit:1", 0.7, &.{});
    var left = db_mod.types.SearchResult{ .alloc = alloc, .hits = left_hits, .total_hits = 1 };
    defer left.deinit();
    var right = db_mod.types.SearchResult{ .alloc = alloc, .hits = right_hits, .total_hits = 1 };
    defer right.deinit();

    try std.testing.expectError(error.StorageReadTemporarilyUnavailable, mergeSearchResults(
        alloc,
        .{
            .full_text = .{ .match = .{ .field = "body", .text = "alpha" } },
            .return_mode = .unit,
            .hierarchy_group_level = .unit,
            .limit = 10,
        },
        &.{ left, right },
        0,
        10,
    ));
}

test "query merge reports an honest lower bound for a partial hierarchy unit union" {
    const alloc = std.testing.allocator;
    var left_hits = try alloc.alloc(db_mod.types.SearchHit, 1);
    left_hits[0] = try testHierarchyUnitHitAlloc(alloc, 0.8, &.{});
    var right_hits = try alloc.alloc(db_mod.types.SearchHit, 1);
    right_hits[0] = try testHierarchyUnitHitAlloc(alloc, 0.9, &.{});
    var left = db_mod.types.SearchResult{ .alloc = alloc, .hits = left_hits, .total_hits = 10 };
    defer left.deinit();
    var right = db_mod.types.SearchResult{ .alloc = alloc, .hits = right_hits, .total_hits = 12 };
    defer right.deinit();

    var merged = try mergeSearchResults(alloc, .{
        .full_text = .{ .match = .{ .field = "body", .text = "alpha" } },
        .return_mode = .unit,
        .hierarchy_group_level = .unit,
        .limit = 10,
    }, &.{ left, right }, 0, 10);
    defer merged.deinit();

    try std.testing.expectEqual(@as(u32, 12), merged.total_hits);
    try std.testing.expectEqual(db_mod.types.TotalHitsRelation.gte, merged.total_hits_relation);
    try std.testing.expectEqual(@as(usize, 1), merged.hits.len);
}

test "query merge rejects exact sorting for hierarchy unit groups" {
    const alloc = std.testing.allocator;
    const order_by = [_]db_mod.types.SortField{
        .{ .field = "_score", .desc = true },
        .{ .field = "_id" },
    };
    var hits = try alloc.alloc(db_mod.types.SearchHit, 1);
    hits[0] = try testHierarchyUnitHitAlloc(alloc, 0.8, &.{});
    var result = db_mod.types.SearchResult{ .alloc = alloc, .hits = hits, .total_hits = 1 };
    defer result.deinit();

    try std.testing.expectError(error.UnsupportedQueryRequest, mergeSearchResults(alloc, .{
        .full_text = .{ .match = .{ .field = "body", .text = "alpha" } },
        .return_mode = .unit,
        .hierarchy_group_level = .unit,
        .order_by = &order_by,
        .limit = 10,
    }, &.{result}, 0, 10));
}

test "query merge bounds hierarchy unit selection by page instead of shard fanout" {
    const alloc = std.testing.allocator;
    const shard_count = 11;
    const hits_per_shard = 100;
    const results = try alloc.alloc(db_mod.types.SearchResult, shard_count);
    var initialized: usize = 0;
    defer {
        for (results[0..initialized]) |*result| result.deinit();
        alloc.free(results);
    }
    for (results, 0..) |*result, shard_index| {
        result.* = try testHierarchyUnitShardResultAlloc(alloc, shard_index, hits_per_shard);
        initialized += 1;
    }

    var merged = try mergeSearchResults(alloc, .{
        .full_text = .{ .match = .{ .field = "body", .text = "alpha" } },
        .return_mode = .unit,
        .hierarchy_group_level = .unit,
        .limit = hits_per_shard,
    }, results, 0, hits_per_shard);
    defer merged.deinit();

    try std.testing.expectEqual(@as(usize, hits_per_shard), merged.hits.len);
    try std.testing.expectEqual(@as(u32, hits_per_shard), merged.total_hits);
    try std.testing.expectEqual(db_mod.types.TotalHitsRelation.gte, merged.total_hits_relation);
    for (merged.hits, 0..) |hit, i| {
        if (i > 0) try std.testing.expect(merged.hits[i - 1].score.? >= hit.score.?);
        for (merged.hits[0..i]) |previous| {
            try std.testing.expect(!std.mem.eql(u8, previous.id, hit.id));
        }
    }
}

fn testDateSortedQueryHitAlloc(alloc: std.mem.Allocator, id: []const u8, created_at_ns: u64) !db_mod.types.SearchHit {
    const sort_values = try alloc.alloc(std.json.Value, 2);
    errdefer alloc.free(sort_values);
    sort_values[0] = .{ .string = try runtime_schema_mod.formatDateTimeNsAlloc(alloc, created_at_ns) };
    errdefer db_mod.types.deinitJsonValue(alloc, &sort_values[0]);
    sort_values[1] = .{ .string = try alloc.dupe(u8, id) };
    errdefer db_mod.types.deinitJsonValue(alloc, &sort_values[1]);
    return .{
        .id = try alloc.dupe(u8, id),
        .sort_values = sort_values,
    };
}

fn testRankRuntimeSchema() runtime_schema_mod.TableSchema {
    const templates = struct {
        const values = [_]runtime_schema_mod.DynamicTemplate{.{
            .name = "rank",
            .path_match = "rank",
            .mapping = .{
                .field_type = .numeric,
                .doc_values = true,
                .sortable = true,
                .analyzer = "keyword",
            },
        }};
    }.values;
    return .{ .dynamic_templates = &templates };
}

test "query merge applies deterministic graph metric top-k across shards" {
    const alloc = std.testing.allocator;

    const left_scores = try alloc.alloc(db_mod.types.GraphMetricScore, 2);
    left_scores[0] = .{ .node = try alloc.dupe(u8, "doc:b"), .score = 0.8 };
    left_scores[1] = .{ .node = try alloc.dupe(u8, "doc:d"), .score = 0.6 };
    const left_metrics = try alloc.alloc(db_mod.types.GraphMetricResult, 1);
    left_metrics[0] = .{
        .name = try alloc.dupe(u8, "central"),
        .index_name = try alloc.dupe(u8, "graph_idx"),
        .metric_name = try alloc.dupe(u8, "pagerank"),
        .scores = left_scores,
        .status = .{
            .name = try alloc.dupe(u8, "pagerank"),
            .state = .fresh,
            .published_generation = 5,
            .edge_generation = 5,
            .target_edge_generation = 5,
            .building_generation = 6,
            .build_job_id = 12345,
            .build_started_at_ms = 1780000000100,
            .build_iteration = 2,
            .progress = 1.0,
            .converged = true,
        },
    };
    var left = db_mod.types.SearchResult{
        .alloc = alloc,
        .hits = &.{},
        .total_hits = 0,
        .graph_metric_results = left_metrics,
    };
    defer left.deinit();

    const right_scores = try alloc.alloc(db_mod.types.GraphMetricScore, 3);
    right_scores[0] = .{ .node = try alloc.dupe(u8, "doc:c"), .score = 0.9 };
    right_scores[1] = .{ .node = try alloc.dupe(u8, "doc:a"), .score = 0.8 };
    right_scores[2] = .{ .node = try alloc.dupe(u8, "doc:e"), .score = 0.1 };
    const right_metrics = try alloc.alloc(db_mod.types.GraphMetricResult, 1);
    right_metrics[0] = .{
        .name = try alloc.dupe(u8, "central"),
        .index_name = try alloc.dupe(u8, "graph_idx"),
        .metric_name = try alloc.dupe(u8, "pagerank"),
        .scores = right_scores,
        .status = .{
            .name = try alloc.dupe(u8, "pagerank"),
            .state = .stale,
            .build_queued = true,
            .published_generation = 5,
            .edge_generation = 6,
            .target_edge_generation = 6,
            .queued_generation = 6,
            .building_generation = 6,
            .build_job_id = 12345,
            .build_started_at_ms = 1780000000100,
            .build_iteration = 3,
            .progress = 0.0,
            .converged = true,
        },
    };
    var right = db_mod.types.SearchResult{
        .alloc = alloc,
        .hits = &.{},
        .total_hits = 0,
        .graph_metric_results = right_metrics,
    };
    defer right.deinit();

    const graph_metric_queries = [_]db_mod.types.NamedGraphMetricQuery{.{
        .name = "central",
        .query = .{
            .index_name = "graph_idx",
            .metric_name = "pagerank",
            .top_k = 3,
        },
    }};
    var merged = try mergeSearchResults(alloc, .{ .graph_metric_queries = &graph_metric_queries }, &.{ left, right }, 0, 10);
    defer merged.deinit();

    try std.testing.expectEqual(@as(usize, 1), merged.graph_metric_results.len);
    const central = merged.graph_metric_results[0];
    try std.testing.expectEqualStrings("central", central.name);
    try std.testing.expectEqual(@as(usize, 3), central.scores.len);
    try std.testing.expectEqualStrings("doc:c", central.scores[0].node);
    try std.testing.expectEqualStrings("doc:a", central.scores[1].node);
    try std.testing.expectEqualStrings("doc:b", central.scores[2].node);
    try std.testing.expectEqual(graph_mod.GraphIndex.GraphMetricState.stale, central.status.state);
    try std.testing.expect(central.status.build_queued);
    try std.testing.expectEqual(@as(u64, 5), central.status.published_generation);
    try std.testing.expectEqual(@as(u64, 6), central.status.edge_generation);
    try std.testing.expectEqual(@as(u64, 6), central.status.queued_generation);
    try std.testing.expectEqual(@as(u64, 6), central.status.building_generation);
    try std.testing.expectEqual(@as(u64, 12345), central.status.build_job_id);
    try std.testing.expectEqual(@as(u64, 1780000000100), central.status.build_started_at_ms);
    try std.testing.expectEqual(@as(u32, 3), central.status.build_iteration);

    var encoded = try encodeQueryResponses(alloc, "docs", .{
        .profile = true,
        .graph_metric_queries = &graph_metric_queries,
    }, .{ .took_ms = 4, .shard_count = 2, .merged = true }, merged);
    defer encoded.deinit(alloc);
    try ant_json.testing.expectSubsetJsonText(alloc,
        \\{"responses":[{"profile":{"shards":{"total":2,"successful":2,"failed":0},"graph_metrics":[{"query_name":"central","source":"graph_metric","index_name":"graph_idx","metric_name":"pagerank","freshness":"published","status":{"state":"stale","published_generation":5,"building_generation":6}}]}}]}
    , encoded.json);

    const fresh_graph_metric_queries = [_]db_mod.types.NamedGraphMetricQuery{.{
        .name = "central",
        .query = .{
            .index_name = "graph_idx",
            .metric_name = "pagerank",
            .top_k = 3,
            .freshness = .fresh,
        },
    }};
    try std.testing.expectError(error.UnsupportedQueryRequest, mergeSearchResults(alloc, .{ .graph_metric_queries = &fresh_graph_metric_queries }, &.{ left, right }, 0, 10));

    right_metrics[0].status.published_generation = 4;
    try std.testing.expectError(error.UnsupportedQueryRequest, mergeSearchResults(alloc, .{ .graph_metric_queries = &graph_metric_queries }, &.{ left, right }, 0, 10));
}

test "query merge rejects missing or unpublished graph metric shard results" {
    const alloc = std.testing.allocator;

    const graph_metric_queries = [_]db_mod.types.NamedGraphMetricQuery{.{
        .name = "central",
        .query = .{
            .index_name = "graph_idx",
            .metric_name = "pagerank",
            .top_k = 3,
        },
    }};

    const left_scores = try alloc.alloc(db_mod.types.GraphMetricScore, 1);
    left_scores[0] = .{ .node = try alloc.dupe(u8, "doc:a"), .score = 0.8 };
    const left_metrics = try alloc.alloc(db_mod.types.GraphMetricResult, 1);
    left_metrics[0] = .{
        .name = try alloc.dupe(u8, "central"),
        .index_name = try alloc.dupe(u8, "graph_idx"),
        .metric_name = try alloc.dupe(u8, "pagerank"),
        .scores = left_scores,
        .status = .{
            .name = try alloc.dupe(u8, "pagerank"),
            .state = .fresh,
            .published_generation = 5,
            .edge_generation = 5,
            .target_edge_generation = 5,
            .progress = 1.0,
            .converged = true,
        },
    };
    var left = db_mod.types.SearchResult{
        .alloc = alloc,
        .hits = &.{},
        .total_hits = 0,
        .graph_metric_results = left_metrics,
    };
    defer left.deinit();

    var missing = db_mod.types.SearchResult{
        .alloc = alloc,
        .hits = &.{},
        .total_hits = 0,
    };
    defer missing.deinit();

    try std.testing.expectError(error.UnsupportedQueryRequest, mergeSearchResults(alloc, .{ .graph_metric_queries = &graph_metric_queries }, &.{ left, missing }, 0, 10));

    const unpublished_metrics = try alloc.alloc(db_mod.types.GraphMetricResult, 1);
    unpublished_metrics[0] = .{
        .name = try alloc.dupe(u8, "central"),
        .index_name = try alloc.dupe(u8, "graph_idx"),
        .metric_name = try alloc.dupe(u8, "pagerank"),
        .scores = &.{},
        .status = .{
            .name = try alloc.dupe(u8, "pagerank"),
            .state = .not_ready,
            .published_generation = 0,
            .edge_generation = 5,
            .target_edge_generation = 5,
        },
    };
    var unpublished = db_mod.types.SearchResult{
        .alloc = alloc,
        .hits = &.{},
        .total_hits = 0,
        .graph_metric_results = unpublished_metrics,
    };
    defer unpublished.deinit();

    try std.testing.expectError(error.UnsupportedQueryRequest, mergeSearchResults(alloc, .{ .graph_metric_queries = &graph_metric_queries }, &.{ left, unpublished }, 0, 10));
}

test "query merge rejects duplicate direct graph metric score nodes" {
    const alloc = std.testing.allocator;

    const graph_metric_queries = [_]db_mod.types.NamedGraphMetricQuery{.{
        .name = "central",
        .query = .{
            .index_name = "graph_idx",
            .metric_name = "pagerank",
            .top_k = 3,
        },
    }};

    const duplicate_scores = try alloc.alloc(db_mod.types.GraphMetricScore, 2);
    duplicate_scores[0] = .{ .node = try alloc.dupe(u8, "doc:a"), .score = 0.8 };
    duplicate_scores[1] = .{ .node = try alloc.dupe(u8, "doc:a"), .score = 0.7 };
    const duplicate_metrics = try alloc.alloc(db_mod.types.GraphMetricResult, 1);
    duplicate_metrics[0] = .{
        .name = try alloc.dupe(u8, "central"),
        .index_name = try alloc.dupe(u8, "graph_idx"),
        .metric_name = try alloc.dupe(u8, "pagerank"),
        .scores = duplicate_scores,
        .status = .{
            .name = try alloc.dupe(u8, "pagerank"),
            .state = .fresh,
            .published_generation = 5,
            .edge_generation = 5,
            .target_edge_generation = 5,
            .progress = 1.0,
            .converged = true,
        },
    };
    var duplicate = db_mod.types.SearchResult{
        .alloc = alloc,
        .hits = &.{},
        .total_hits = 0,
        .graph_metric_results = duplicate_metrics,
    };
    defer duplicate.deinit();
    try std.testing.expectError(error.UnsupportedQueryRequest, mergeSearchResults(alloc, .{ .graph_metric_queries = &graph_metric_queries }, &.{duplicate}, 0, 10));

    const left_scores = try alloc.alloc(db_mod.types.GraphMetricScore, 1);
    left_scores[0] = .{ .node = try alloc.dupe(u8, "doc:b"), .score = 0.8 };
    const left_metrics = try alloc.alloc(db_mod.types.GraphMetricResult, 1);
    left_metrics[0] = .{
        .name = try alloc.dupe(u8, "central"),
        .index_name = try alloc.dupe(u8, "graph_idx"),
        .metric_name = try alloc.dupe(u8, "pagerank"),
        .scores = left_scores,
        .status = .{
            .name = try alloc.dupe(u8, "pagerank"),
            .state = .fresh,
            .published_generation = 5,
            .edge_generation = 5,
            .target_edge_generation = 5,
            .progress = 1.0,
            .converged = true,
        },
    };
    var left = db_mod.types.SearchResult{
        .alloc = alloc,
        .hits = &.{},
        .total_hits = 0,
        .graph_metric_results = left_metrics,
    };
    defer left.deinit();

    const right_scores = try alloc.alloc(db_mod.types.GraphMetricScore, 1);
    right_scores[0] = .{ .node = try alloc.dupe(u8, "doc:b"), .score = 0.7 };
    const right_metrics = try alloc.alloc(db_mod.types.GraphMetricResult, 1);
    right_metrics[0] = .{
        .name = try alloc.dupe(u8, "central"),
        .index_name = try alloc.dupe(u8, "graph_idx"),
        .metric_name = try alloc.dupe(u8, "pagerank"),
        .scores = right_scores,
        .status = .{
            .name = try alloc.dupe(u8, "pagerank"),
            .state = .fresh,
            .published_generation = 5,
            .edge_generation = 5,
            .target_edge_generation = 5,
            .progress = 1.0,
            .converged = true,
        },
    };
    var right = db_mod.types.SearchResult{
        .alloc = alloc,
        .hits = &.{},
        .total_hits = 0,
        .graph_metric_results = right_metrics,
    };
    defer right.deinit();
    try std.testing.expectError(error.UnsupportedQueryRequest, mergeSearchResults(alloc, .{ .graph_metric_queries = &graph_metric_queries }, &.{ left, right }, 0, 10));
}

test "query merge rejects non-finite direct graph metric scores" {
    const alloc = std.testing.allocator;

    const graph_metric_queries = [_]db_mod.types.NamedGraphMetricQuery{.{
        .name = "central",
        .query = .{
            .index_name = "graph_idx",
            .metric_name = "pagerank",
            .top_k = 3,
        },
    }};

    const nan_scores = try alloc.alloc(db_mod.types.GraphMetricScore, 1);
    nan_scores[0] = .{ .node = try alloc.dupe(u8, "doc:a"), .score = std.math.nan(f64) };
    const nan_metrics = try alloc.alloc(db_mod.types.GraphMetricResult, 1);
    nan_metrics[0] = .{
        .name = try alloc.dupe(u8, "central"),
        .index_name = try alloc.dupe(u8, "graph_idx"),
        .metric_name = try alloc.dupe(u8, "pagerank"),
        .scores = nan_scores,
        .status = .{
            .name = try alloc.dupe(u8, "pagerank"),
            .state = .fresh,
            .published_generation = 5,
            .edge_generation = 5,
            .target_edge_generation = 5,
            .progress = 1.0,
            .converged = true,
        },
    };
    var nan_result = db_mod.types.SearchResult{
        .alloc = alloc,
        .hits = &.{},
        .total_hits = 0,
        .graph_metric_results = nan_metrics,
    };
    defer nan_result.deinit();
    try std.testing.expectError(error.UnsupportedQueryRequest, mergeSearchResults(alloc, .{ .graph_metric_queries = &graph_metric_queries }, &.{nan_result}, 0, 10));

    const inf_scores = try alloc.alloc(db_mod.types.GraphMetricScore, 1);
    inf_scores[0] = .{ .node = try alloc.dupe(u8, "doc:b"), .score = std.math.inf(f64) };
    const inf_metrics = try alloc.alloc(db_mod.types.GraphMetricResult, 1);
    inf_metrics[0] = .{
        .name = try alloc.dupe(u8, "central"),
        .index_name = try alloc.dupe(u8, "graph_idx"),
        .metric_name = try alloc.dupe(u8, "pagerank"),
        .scores = inf_scores,
        .status = .{
            .name = try alloc.dupe(u8, "pagerank"),
            .state = .fresh,
            .published_generation = 5,
            .edge_generation = 5,
            .target_edge_generation = 5,
            .progress = 1.0,
            .converged = true,
        },
    };
    var inf_result = db_mod.types.SearchResult{
        .alloc = alloc,
        .hits = &.{},
        .total_hits = 0,
        .graph_metric_results = inf_metrics,
    };
    defer inf_result.deinit();
    try std.testing.expectError(error.UnsupportedQueryRequest, mergeSearchResults(alloc, .{ .graph_metric_queries = &graph_metric_queries }, &.{inf_result}, 0, 10));
}

test "query merge rejects duplicate direct graph metric shard results" {
    const alloc = std.testing.allocator;

    const graph_metric_queries = [_]db_mod.types.NamedGraphMetricQuery{.{
        .name = "central",
        .query = .{
            .index_name = "graph_idx",
            .metric_name = "pagerank",
            .top_k = 3,
        },
    }};

    const scores_a = try alloc.alloc(db_mod.types.GraphMetricScore, 1);
    scores_a[0] = .{ .node = try alloc.dupe(u8, "doc:a"), .score = 0.8 };
    const scores_b = try alloc.alloc(db_mod.types.GraphMetricScore, 1);
    scores_b[0] = .{ .node = try alloc.dupe(u8, "doc:b"), .score = 0.7 };
    const duplicate_metrics = try alloc.alloc(db_mod.types.GraphMetricResult, 2);
    duplicate_metrics[0] = .{
        .name = try alloc.dupe(u8, "central"),
        .index_name = try alloc.dupe(u8, "graph_idx"),
        .metric_name = try alloc.dupe(u8, "pagerank"),
        .scores = scores_a,
        .status = .{
            .name = try alloc.dupe(u8, "pagerank"),
            .state = .fresh,
            .published_generation = 5,
            .edge_generation = 5,
            .target_edge_generation = 5,
            .progress = 1.0,
            .converged = true,
        },
    };
    duplicate_metrics[1] = .{
        .name = try alloc.dupe(u8, "central"),
        .index_name = try alloc.dupe(u8, "graph_idx"),
        .metric_name = try alloc.dupe(u8, "pagerank"),
        .scores = scores_b,
        .status = .{
            .name = try alloc.dupe(u8, "pagerank"),
            .state = .fresh,
            .published_generation = 5,
            .edge_generation = 5,
            .target_edge_generation = 5,
            .progress = 1.0,
            .converged = true,
        },
    };
    var duplicate = db_mod.types.SearchResult{
        .alloc = alloc,
        .hits = &.{},
        .total_hits = 0,
        .graph_metric_results = duplicate_metrics,
    };
    defer duplicate.deinit();

    try std.testing.expectError(error.UnsupportedQueryRequest, mergeSearchResults(alloc, .{ .graph_metric_queries = &graph_metric_queries }, &.{duplicate}, 0, 10));

    const duplicate_request_queries = [_]db_mod.types.NamedGraphMetricQuery{
        .{
            .name = "central",
            .query = .{
                .index_name = "graph_idx",
                .metric_name = "pagerank",
                .top_k = 3,
            },
        },
        .{
            .name = "central",
            .query = .{
                .index_name = "graph_idx",
                .metric_name = "degree",
                .top_k = 3,
            },
        },
    };
    var valid_scores = try alloc.alloc(db_mod.types.GraphMetricScore, 1);
    valid_scores[0] = .{ .node = try alloc.dupe(u8, "doc:a"), .score = 0.8 };
    const valid_metrics = try alloc.alloc(db_mod.types.GraphMetricResult, 1);
    valid_metrics[0] = .{
        .name = try alloc.dupe(u8, "central"),
        .index_name = try alloc.dupe(u8, "graph_idx"),
        .metric_name = try alloc.dupe(u8, "pagerank"),
        .scores = valid_scores,
        .status = .{
            .name = try alloc.dupe(u8, "pagerank"),
            .state = .fresh,
            .published_generation = 5,
            .edge_generation = 5,
            .target_edge_generation = 5,
            .progress = 1.0,
            .converged = true,
        },
    };
    var duplicate_request = db_mod.types.SearchResult{
        .alloc = alloc,
        .hits = &.{},
        .total_hits = 0,
        .graph_metric_results = valid_metrics,
    };
    defer duplicate_request.deinit();
    try std.testing.expectError(error.UnsupportedQueryRequest, mergeSearchResults(alloc, .{ .graph_metric_queries = &duplicate_request_queries }, &.{duplicate_request}, 0, 10));
}

test "query merge rejects mismatched direct graph metric shard identity" {
    const alloc = std.testing.allocator;

    const graph_metric_queries = [_]db_mod.types.NamedGraphMetricQuery{.{
        .name = "central",
        .query = .{
            .index_name = "graph_idx",
            .metric_name = "pagerank",
            .top_k = 3,
        },
    }};

    const wrong_index_metrics = try alloc.alloc(db_mod.types.GraphMetricResult, 1);
    wrong_index_metrics[0] = .{
        .name = try alloc.dupe(u8, "central"),
        .index_name = try alloc.dupe(u8, "other_graph_idx"),
        .metric_name = try alloc.dupe(u8, "pagerank"),
        .scores = &.{},
        .status = .{
            .name = try alloc.dupe(u8, "pagerank"),
            .state = .fresh,
            .published_generation = 5,
            .edge_generation = 5,
            .target_edge_generation = 5,
            .progress = 1.0,
            .converged = true,
        },
    };
    var wrong_index = db_mod.types.SearchResult{
        .alloc = alloc,
        .hits = &.{},
        .total_hits = 0,
        .graph_metric_results = wrong_index_metrics,
    };
    defer wrong_index.deinit();

    try std.testing.expectError(error.UnsupportedQueryRequest, mergeSearchResults(alloc, .{ .graph_metric_queries = &graph_metric_queries }, &.{wrong_index}, 0, 10));

    const wrong_metric_metrics = try alloc.alloc(db_mod.types.GraphMetricResult, 1);
    wrong_metric_metrics[0] = .{
        .name = try alloc.dupe(u8, "central"),
        .index_name = try alloc.dupe(u8, "graph_idx"),
        .metric_name = try alloc.dupe(u8, "degree"),
        .scores = &.{},
        .status = .{
            .name = try alloc.dupe(u8, "degree"),
            .state = .fresh,
            .published_generation = 5,
            .edge_generation = 5,
            .target_edge_generation = 5,
            .progress = 1.0,
            .converged = true,
        },
    };
    var wrong_metric = db_mod.types.SearchResult{
        .alloc = alloc,
        .hits = &.{},
        .total_hits = 0,
        .graph_metric_results = wrong_metric_metrics,
    };
    defer wrong_metric.deinit();

    try std.testing.expectError(error.UnsupportedQueryRequest, mergeSearchResults(alloc, .{ .graph_metric_queries = &graph_metric_queries }, &.{wrong_metric}, 0, 10));

    const wrong_status_metrics = try alloc.alloc(db_mod.types.GraphMetricResult, 1);
    wrong_status_metrics[0] = .{
        .name = try alloc.dupe(u8, "central"),
        .index_name = try alloc.dupe(u8, "graph_idx"),
        .metric_name = try alloc.dupe(u8, "pagerank"),
        .scores = &.{},
        .status = .{
            .name = try alloc.dupe(u8, "degree"),
            .state = .fresh,
            .published_generation = 5,
            .edge_generation = 5,
            .target_edge_generation = 5,
            .progress = 1.0,
            .converged = true,
        },
    };
    var wrong_status = db_mod.types.SearchResult{
        .alloc = alloc,
        .hits = &.{},
        .total_hits = 0,
        .graph_metric_results = wrong_status_metrics,
    };
    defer wrong_status.deinit();

    try std.testing.expectError(error.UnsupportedQueryRequest, mergeSearchResults(alloc, .{ .graph_metric_queries = &graph_metric_queries }, &.{wrong_status}, 0, 10));
}

test "query merge rejects inconsistent graph metric fan-in status state" {
    const alloc = std.testing.allocator;

    const graph_metric_queries = [_]db_mod.types.NamedGraphMetricQuery{.{
        .name = "central",
        .query = .{
            .index_name = "graph_idx",
            .metric_name = "pagerank",
            .top_k = 3,
        },
    }};
    const direct_metrics = try alloc.alloc(db_mod.types.GraphMetricResult, 1);
    direct_metrics[0] = .{
        .name = try alloc.dupe(u8, "central"),
        .index_name = try alloc.dupe(u8, "graph_idx"),
        .metric_name = try alloc.dupe(u8, "pagerank"),
        .scores = &.{},
        .status = .{
            .name = try alloc.dupe(u8, "pagerank"),
            .state = .not_ready,
            .published_generation = 5,
            .edge_generation = 5,
            .target_edge_generation = 5,
        },
    };
    var direct = db_mod.types.SearchResult{
        .alloc = alloc,
        .hits = &.{},
        .total_hits = 0,
        .graph_metric_results = direct_metrics,
    };
    defer direct.deinit();
    try std.testing.expectError(error.UnsupportedQueryRequest, mergeSearchResults(alloc, .{ .graph_metric_queries = &graph_metric_queries }, &.{direct}, 0, 10));

    const direct_future_metrics = try alloc.alloc(db_mod.types.GraphMetricResult, 1);
    direct_future_metrics[0] = .{
        .name = try alloc.dupe(u8, "central"),
        .index_name = try alloc.dupe(u8, "graph_idx"),
        .metric_name = try alloc.dupe(u8, "pagerank"),
        .scores = &.{},
        .status = .{
            .name = try alloc.dupe(u8, "pagerank"),
            .state = .fresh,
            .published_generation = 5,
            .edge_generation = 6,
            .target_edge_generation = 6,
            .progress = 1.0,
            .converged = true,
        },
    };
    var direct_future = db_mod.types.SearchResult{
        .alloc = alloc,
        .hits = &.{},
        .total_hits = 0,
        .graph_metric_results = direct_future_metrics,
    };
    defer direct_future.deinit();
    try std.testing.expectError(error.UnsupportedQueryRequest, mergeSearchResults(alloc, .{ .graph_metric_queries = &graph_metric_queries }, &.{direct_future}, 0, 10));

    const graph_metric_reads = [_]graph_query_mod.GraphMetricRead{.{
        .name = "pagerank",
        .freshness = .published,
    }};
    const graph_queries = [_]db_mod.types.NamedGraphQuery{.{
        .name = "neighbors",
        .query = .{
            .query_type = .neighbors,
            .index_name = "graph_idx",
            .start_nodes = .{ .keys = &.{"doc:a"} },
            .metrics = &graph_metric_reads,
            .include_metric_status = true,
        },
    }};
    const graph_results = try alloc.alloc(db_mod.types.GraphSearchResult, 1);
    graph_results[0] = .{
        .name = try alloc.dupe(u8, "neighbors"),
        .nodes = &.{},
        .paths = &.{},
        .hits = &.{},
        .total_hits = 0,
        .metric_status = try alloc.dupe(db_mod.types.GraphMetricStatus, &.{
            .{
                .name = try alloc.dupe(u8, "pagerank"),
                .state = .disabled,
                .published_generation = 5,
                .edge_generation = 5,
                .target_edge_generation = 5,
            },
        }),
    };
    var graph = db_mod.types.SearchResult{
        .alloc = alloc,
        .hits = &.{},
        .total_hits = 0,
        .graph_results = graph_results,
    };
    defer graph.deinit();
    try std.testing.expectError(error.UnsupportedQueryRequest, mergeSearchResults(alloc, .{ .graph_queries = &graph_queries }, &.{graph}, 0, 10));

    const graph_future_results = try alloc.alloc(db_mod.types.GraphSearchResult, 1);
    graph_future_results[0] = .{
        .name = try alloc.dupe(u8, "neighbors"),
        .nodes = &.{},
        .paths = &.{},
        .hits = &.{},
        .total_hits = 0,
        .metric_status = try alloc.dupe(db_mod.types.GraphMetricStatus, &.{
            .{
                .name = try alloc.dupe(u8, "pagerank"),
                .state = .fresh,
                .published_generation = 5,
                .edge_generation = 6,
                .target_edge_generation = 6,
                .progress = 1.0,
                .converged = true,
            },
        }),
    };
    var graph_future = db_mod.types.SearchResult{
        .alloc = alloc,
        .hits = &.{},
        .total_hits = 0,
        .graph_results = graph_future_results,
    };
    defer graph_future.deinit();
    try std.testing.expectError(error.UnsupportedQueryRequest, mergeSearchResults(alloc, .{ .graph_queries = &graph_queries }, &.{graph_future}, 0, 10));

    var rerank = db_mod.types.SearchResult{
        .alloc = alloc,
        .hits = &.{},
        .total_hits = 0,
        .graph_metric_rerank_status = .{
            .name = try alloc.dupe(u8, "pagerank"),
            .state = .not_ready,
            .published_generation = 5,
            .edge_generation = 5,
            .target_edge_generation = 5,
        },
    };
    defer rerank.deinit();
    try std.testing.expectError(error.UnsupportedQueryRequest, mergeSearchResults(alloc, .{
        .graph_metric_rerank = .{
            .index_name = "graph_idx",
            .metric_name = "pagerank",
            .freshness = .published,
            .weight = 1.0,
        },
    }, &.{rerank}, 0, 10));

    var rerank_future = db_mod.types.SearchResult{
        .alloc = alloc,
        .hits = &.{},
        .total_hits = 0,
        .graph_metric_rerank_status = .{
            .name = try alloc.dupe(u8, "pagerank"),
            .state = .fresh,
            .published_generation = 5,
            .edge_generation = 6,
            .target_edge_generation = 6,
            .progress = 1.0,
            .converged = true,
        },
    };
    defer rerank_future.deinit();
    try std.testing.expectError(error.UnsupportedQueryRequest, mergeSearchResults(alloc, .{
        .graph_metric_rerank = .{
            .index_name = "graph_idx",
            .metric_name = "pagerank",
            .freshness = .published,
            .weight = 1.0,
        },
    }, &.{rerank_future}, 0, 10));
}

test "query merge rejects non-finite graph metric fan-in status numbers" {
    const alloc = std.testing.allocator;

    const graph_metric_queries = [_]db_mod.types.NamedGraphMetricQuery{.{
        .name = "central",
        .query = .{
            .index_name = "graph_idx",
            .metric_name = "pagerank",
            .top_k = 3,
        },
    }};
    const direct_metrics = try alloc.alloc(db_mod.types.GraphMetricResult, 1);
    direct_metrics[0] = .{
        .name = try alloc.dupe(u8, "central"),
        .index_name = try alloc.dupe(u8, "graph_idx"),
        .metric_name = try alloc.dupe(u8, "pagerank"),
        .scores = &.{},
        .status = .{
            .name = try alloc.dupe(u8, "pagerank"),
            .state = .fresh,
            .published_generation = 5,
            .edge_generation = 5,
            .target_edge_generation = 5,
            .progress = std.math.nan(f64),
            .converged = true,
        },
    };
    var direct = db_mod.types.SearchResult{
        .alloc = alloc,
        .hits = &.{},
        .total_hits = 0,
        .graph_metric_results = direct_metrics,
    };
    defer direct.deinit();
    try std.testing.expectError(error.UnsupportedQueryRequest, mergeSearchResults(alloc, .{ .graph_metric_queries = &graph_metric_queries }, &.{direct}, 0, 10));

    const graph_queries = [_]db_mod.types.NamedGraphQuery{.{
        .name = "neighbors",
        .query = .{
            .query_type = .neighbors,
            .index_name = "graph_idx",
            .start_nodes = .{ .keys = &.{"doc:a"} },
            .include_metric_status = true,
        },
    }};
    const graph_results = try alloc.alloc(db_mod.types.GraphSearchResult, 1);
    graph_results[0] = .{
        .name = try alloc.dupe(u8, "neighbors"),
        .nodes = &.{},
        .paths = &.{},
        .hits = &.{},
        .total_hits = 0,
        .metric_status = try alloc.dupe(db_mod.types.GraphMetricStatus, &.{
            .{
                .name = try alloc.dupe(u8, "pagerank"),
                .state = .fresh,
                .published_generation = 5,
                .edge_generation = 5,
                .target_edge_generation = 5,
                .progress = 1.0,
                .delta = std.math.inf(f64),
                .converged = true,
            },
        }),
    };
    var graph = db_mod.types.SearchResult{
        .alloc = alloc,
        .hits = &.{},
        .total_hits = 0,
        .graph_results = graph_results,
    };
    defer graph.deinit();
    try std.testing.expectError(error.UnsupportedQueryRequest, mergeSearchResults(alloc, .{ .graph_queries = &graph_queries }, &.{graph}, 0, 10));

    var rerank = db_mod.types.SearchResult{
        .alloc = alloc,
        .hits = &.{},
        .total_hits = 0,
        .graph_metric_rerank_status = .{
            .name = try alloc.dupe(u8, "pagerank"),
            .state = .fresh,
            .published_generation = 5,
            .edge_generation = 5,
            .target_edge_generation = 5,
            .progress = std.math.inf(f64),
            .converged = true,
        },
    };
    defer rerank.deinit();
    try std.testing.expectError(error.UnsupportedQueryRequest, mergeSearchResults(alloc, .{
        .graph_metric_rerank = .{
            .index_name = "graph_idx",
            .metric_name = "pagerank",
            .freshness = .published,
            .weight = 1.0,
        },
    }, &.{rerank}, 0, 10));
}

test "query merge rejects out-of-range graph metric fan-in progress" {
    const alloc = std.testing.allocator;

    const graph_metric_queries = [_]db_mod.types.NamedGraphMetricQuery{.{
        .name = "central",
        .query = .{
            .index_name = "graph_idx",
            .metric_name = "pagerank",
            .top_k = 3,
        },
    }};

    const high_progress_metrics = try alloc.alloc(db_mod.types.GraphMetricResult, 1);
    high_progress_metrics[0] = .{
        .name = try alloc.dupe(u8, "central"),
        .index_name = try alloc.dupe(u8, "graph_idx"),
        .metric_name = try alloc.dupe(u8, "pagerank"),
        .scores = &.{},
        .status = .{
            .name = try alloc.dupe(u8, "pagerank"),
            .state = .building,
            .published_generation = 5,
            .edge_generation = 6,
            .target_edge_generation = 6,
            .progress = 1.25,
            .converged = false,
        },
    };
    var high_progress = db_mod.types.SearchResult{
        .alloc = alloc,
        .hits = &.{},
        .total_hits = 0,
        .graph_metric_results = high_progress_metrics,
    };
    defer high_progress.deinit();
    try std.testing.expectError(error.UnsupportedQueryRequest, mergeSearchResults(alloc, .{ .graph_metric_queries = &graph_metric_queries }, &.{high_progress}, 0, 10));

    var negative_progress = db_mod.types.SearchResult{
        .alloc = alloc,
        .hits = &.{},
        .total_hits = 0,
        .graph_metric_rerank_status = .{
            .name = try alloc.dupe(u8, "pagerank"),
            .state = .fresh,
            .published_generation = 5,
            .edge_generation = 5,
            .target_edge_generation = 5,
            .progress = -0.1,
            .converged = true,
        },
    };
    defer negative_progress.deinit();
    try std.testing.expectError(error.UnsupportedQueryRequest, mergeSearchResults(alloc, .{
        .graph_metric_rerank = .{
            .index_name = "graph_idx",
            .metric_name = "pagerank",
            .freshness = .published,
            .weight = 1.0,
        },
    }, &.{negative_progress}, 0, 10));
}

test "query merge rejects incompatible graph metric fan-in metadata" {
    const alloc = std.testing.allocator;

    const graph_metric_queries = [_]db_mod.types.NamedGraphMetricQuery{.{
        .name = "central",
        .query = .{
            .index_name = "graph_idx",
            .metric_name = "pagerank",
            .top_k = 3,
        },
    }};

    const left_scores = try alloc.alloc(db_mod.types.GraphMetricScore, 1);
    left_scores[0] = .{ .node = try alloc.dupe(u8, "doc:a"), .score = 0.8 };
    const left_metrics = try alloc.alloc(db_mod.types.GraphMetricResult, 1);
    left_metrics[0] = .{
        .name = try alloc.dupe(u8, "central"),
        .index_name = try alloc.dupe(u8, "graph_idx"),
        .metric_name = try alloc.dupe(u8, "pagerank"),
        .scores = left_scores,
        .status = .{
            .name = try alloc.dupe(u8, "pagerank"),
            .state = .fresh,
            .metadata_version = 1,
            .published_generation = 5,
            .edge_generation = 5,
            .target_edge_generation = 5,
            .progress = 1.0,
            .converged = true,
        },
    };
    var left = db_mod.types.SearchResult{
        .alloc = alloc,
        .hits = &.{},
        .total_hits = 0,
        .graph_metric_results = left_metrics,
    };
    defer left.deinit();

    const right_scores = try alloc.alloc(db_mod.types.GraphMetricScore, 1);
    right_scores[0] = .{ .node = try alloc.dupe(u8, "doc:b"), .score = 0.7 };
    const right_metrics = try alloc.alloc(db_mod.types.GraphMetricResult, 1);
    right_metrics[0] = .{
        .name = try alloc.dupe(u8, "central"),
        .index_name = try alloc.dupe(u8, "graph_idx"),
        .metric_name = try alloc.dupe(u8, "pagerank"),
        .scores = right_scores,
        .status = .{
            .name = try alloc.dupe(u8, "pagerank"),
            .state = .fresh,
            .metadata_version = 2,
            .published_generation = 5,
            .edge_generation = 5,
            .target_edge_generation = 5,
            .progress = 1.0,
            .converged = true,
        },
    };
    var right = db_mod.types.SearchResult{
        .alloc = alloc,
        .hits = &.{},
        .total_hits = 0,
        .graph_metric_results = right_metrics,
    };
    defer right.deinit();
    try std.testing.expectError(error.UnsupportedQueryRequest, mergeSearchResults(alloc, .{ .graph_metric_queries = &graph_metric_queries }, &.{ left, right }, 0, 10));

    const graph_metric_reads = [_]graph_query_mod.GraphMetricRead{.{
        .name = "pagerank",
        .freshness = .published,
    }};
    const graph_queries = [_]db_mod.types.NamedGraphQuery{.{
        .name = "neighbors",
        .query = .{
            .query_type = .neighbors,
            .index_name = "graph_idx",
            .start_nodes = .{ .keys = &.{"doc:a"} },
            .metrics = &graph_metric_reads,
        },
    }};
    const graph_left_results = try alloc.alloc(db_mod.types.GraphSearchResult, 1);
    graph_left_results[0] = .{
        .name = try alloc.dupe(u8, "neighbors"),
        .nodes = &.{},
        .paths = &.{},
        .hits = &.{},
        .total_hits = 0,
        .metric_status = try alloc.dupe(db_mod.types.GraphMetricStatus, &.{
            .{
                .name = try alloc.dupe(u8, "pagerank"),
                .state = .fresh,
                .published_generation = 5,
                .edge_generation = 5,
                .target_edge_generation = 5,
                .progress = 1.0,
                .converged = true,
            },
        }),
    };
    var graph_left = db_mod.types.SearchResult{
        .alloc = alloc,
        .hits = &.{},
        .total_hits = 0,
        .graph_results = graph_left_results,
    };
    defer graph_left.deinit();

    const filter_types = try alloc.alloc([]const u8, 1);
    filter_types[0] = try alloc.dupe(u8, "cites");
    const graph_right_results = try alloc.alloc(db_mod.types.GraphSearchResult, 1);
    graph_right_results[0] = .{
        .name = try alloc.dupe(u8, "neighbors"),
        .nodes = &.{},
        .paths = &.{},
        .hits = &.{},
        .total_hits = 0,
        .metric_status = try alloc.dupe(db_mod.types.GraphMetricStatus, &.{
            .{
                .name = try alloc.dupe(u8, "pagerank"),
                .state = .fresh,
                .edge_filter = .{ .mode = .types, .types = filter_types },
                .published_generation = 5,
                .edge_generation = 5,
                .target_edge_generation = 5,
                .progress = 1.0,
                .converged = true,
            },
        }),
    };
    var graph_right = db_mod.types.SearchResult{
        .alloc = alloc,
        .hits = &.{},
        .total_hits = 0,
        .graph_results = graph_right_results,
    };
    defer graph_right.deinit();
    try std.testing.expectError(error.UnsupportedQueryRequest, mergeSearchResults(alloc, .{ .graph_queries = &graph_queries }, &.{ graph_left, graph_right }, 0, 10));

    const hits_metric_queries = [_]db_mod.types.NamedGraphMetricQuery{
        .{
            .name = "authority",
            .query = .{
                .index_name = "graph_idx",
                .metric_name = "hits_authority",
                .top_k = 3,
            },
        },
        .{
            .name = "hub",
            .query = .{
                .index_name = "graph_idx",
                .metric_name = "hits_hub",
                .top_k = 3,
            },
        },
    };
    const hits_metrics = try alloc.alloc(db_mod.types.GraphMetricResult, 2);
    hits_metrics[0] = .{
        .name = try alloc.dupe(u8, "authority"),
        .index_name = try alloc.dupe(u8, "graph_idx"),
        .metric_name = try alloc.dupe(u8, "hits_authority"),
        .scores = &.{},
        .status = .{
            .name = try alloc.dupe(u8, "hits_authority"),
            .state = .fresh,
            .published_generation = 5,
            .edge_generation = 5,
            .target_edge_generation = 5,
            .progress = 1.0,
            .converged = true,
        },
    };
    hits_metrics[1] = .{
        .name = try alloc.dupe(u8, "hub"),
        .index_name = try alloc.dupe(u8, "graph_idx"),
        .metric_name = try alloc.dupe(u8, "hits_hub"),
        .scores = &.{},
        .status = .{
            .name = try alloc.dupe(u8, "hits_hub"),
            .state = .fresh,
            .published_generation = 6,
            .edge_generation = 6,
            .target_edge_generation = 6,
            .progress = 1.0,
            .converged = true,
        },
    };
    var hits_pair_mismatch = db_mod.types.SearchResult{
        .alloc = alloc,
        .hits = &.{},
        .total_hits = 0,
        .graph_metric_results = hits_metrics,
    };
    defer hits_pair_mismatch.deinit();
    try std.testing.expectError(error.UnsupportedQueryRequest, mergeSearchResults(alloc, .{ .graph_metric_queries = &hits_metric_queries }, &.{hits_pair_mismatch}, 0, 10));

    var rerank_left = db_mod.types.SearchResult{
        .alloc = alloc,
        .hits = &.{},
        .total_hits = 0,
        .graph_metric_rerank_status = .{
            .name = try alloc.dupe(u8, "pagerank"),
            .state = .fresh,
            .metadata_version = 1,
            .published_generation = 5,
            .edge_generation = 5,
            .target_edge_generation = 5,
            .progress = 1.0,
            .converged = true,
        },
    };
    defer rerank_left.deinit();
    var rerank_right = db_mod.types.SearchResult{
        .alloc = alloc,
        .hits = &.{},
        .total_hits = 0,
        .graph_metric_rerank_status = .{
            .name = try alloc.dupe(u8, "pagerank"),
            .state = .fresh,
            .metadata_version = 2,
            .published_generation = 5,
            .edge_generation = 5,
            .target_edge_generation = 5,
            .progress = 1.0,
            .converged = true,
        },
    };
    defer rerank_right.deinit();
    try std.testing.expectError(error.UnsupportedQueryRequest, mergeSearchResults(alloc, .{
        .graph_metric_rerank = .{
            .index_name = "graph_idx",
            .metric_name = "pagerank",
            .freshness = .published,
            .weight = 1.0,
        },
    }, &.{ rerank_left, rerank_right }, 0, 10));
}

test "query merge rejects unsolicited graph score surfaces" {
    const alloc = std.testing.allocator;

    const direct_metrics = try alloc.alloc(db_mod.types.GraphMetricResult, 1);
    direct_metrics[0] = .{
        .name = try alloc.dupe(u8, "central"),
        .index_name = try alloc.dupe(u8, "graph_idx"),
        .metric_name = try alloc.dupe(u8, "pagerank"),
        .scores = &.{},
        .status = .{
            .name = try alloc.dupe(u8, "pagerank"),
            .state = .fresh,
            .published_generation = 5,
            .edge_generation = 5,
            .target_edge_generation = 5,
            .progress = 1.0,
            .converged = true,
        },
    };
    var unsolicited_direct = db_mod.types.SearchResult{
        .alloc = alloc,
        .hits = &.{},
        .total_hits = 0,
        .graph_metric_results = direct_metrics,
    };
    defer unsolicited_direct.deinit();
    try std.testing.expectError(error.UnsupportedQueryRequest, mergeSearchResults(alloc, .{}, &.{unsolicited_direct}, 0, 10));

    const graph_metric_queries = [_]db_mod.types.NamedGraphMetricQuery{.{
        .name = "central",
        .query = .{
            .index_name = "graph_idx",
            .metric_name = "pagerank",
            .top_k = 3,
        },
    }};
    const extra_direct_metrics = try alloc.alloc(db_mod.types.GraphMetricResult, 2);
    extra_direct_metrics[0] = .{
        .name = try alloc.dupe(u8, "central"),
        .index_name = try alloc.dupe(u8, "graph_idx"),
        .metric_name = try alloc.dupe(u8, "pagerank"),
        .scores = &.{},
        .status = .{
            .name = try alloc.dupe(u8, "pagerank"),
            .state = .fresh,
            .published_generation = 5,
            .edge_generation = 5,
            .target_edge_generation = 5,
            .progress = 1.0,
            .converged = true,
        },
    };
    extra_direct_metrics[1] = .{
        .name = try alloc.dupe(u8, "extra"),
        .index_name = try alloc.dupe(u8, "graph_idx"),
        .metric_name = try alloc.dupe(u8, "pagerank"),
        .scores = &.{},
        .status = .{
            .name = try alloc.dupe(u8, "pagerank"),
            .state = .fresh,
            .published_generation = 5,
            .edge_generation = 5,
            .target_edge_generation = 5,
            .progress = 1.0,
            .converged = true,
        },
    };
    var extra_direct = db_mod.types.SearchResult{
        .alloc = alloc,
        .hits = &.{},
        .total_hits = 0,
        .graph_metric_results = extra_direct_metrics,
    };
    defer extra_direct.deinit();
    try std.testing.expectError(error.UnsupportedQueryRequest, mergeSearchResults(alloc, .{ .graph_metric_queries = &graph_metric_queries }, &.{extra_direct}, 0, 10));

    const graph_results = try alloc.alloc(db_mod.types.GraphSearchResult, 1);
    graph_results[0] = .{
        .name = try alloc.dupe(u8, "neighbors"),
        .nodes = &.{},
        .paths = &.{},
        .hits = &.{},
        .total_hits = 0,
        .metric_status = &.{},
    };
    var unsolicited_graph = db_mod.types.SearchResult{
        .alloc = alloc,
        .hits = &.{},
        .total_hits = 0,
        .graph_results = graph_results,
    };
    defer unsolicited_graph.deinit();
    try std.testing.expectError(error.UnsupportedQueryRequest, mergeSearchResults(alloc, .{}, &.{unsolicited_graph}, 0, 10));

    const graph_queries = [_]db_mod.types.NamedGraphQuery{.{
        .name = "neighbors",
        .query = .{
            .query_type = .neighbors,
            .index_name = "graph_idx",
            .start_nodes = .{ .keys = &.{"doc:a"} },
        },
    }};
    const extra_graph_results = try alloc.alloc(db_mod.types.GraphSearchResult, 2);
    extra_graph_results[0] = .{
        .name = try alloc.dupe(u8, "neighbors"),
        .nodes = &.{},
        .paths = &.{},
        .hits = &.{},
        .total_hits = 0,
        .metric_status = &.{},
    };
    extra_graph_results[1] = .{
        .name = try alloc.dupe(u8, "extra_neighbors"),
        .nodes = &.{},
        .paths = &.{},
        .hits = &.{},
        .total_hits = 0,
        .metric_status = &.{},
    };
    var extra_graph = db_mod.types.SearchResult{
        .alloc = alloc,
        .hits = &.{},
        .total_hits = 0,
        .graph_results = extra_graph_results,
    };
    defer extra_graph.deinit();
    try std.testing.expectError(error.UnsupportedQueryRequest, mergeSearchResults(alloc, .{ .graph_queries = &graph_queries }, &.{extra_graph}, 0, 10));
}

test "query merge rejects unsolicited graph search metric status" {
    const alloc = std.testing.allocator;

    const graph_queries = [_]db_mod.types.NamedGraphQuery{.{
        .name = "neighbors",
        .query = .{
            .query_type = .neighbors,
            .index_name = "graph_idx",
            .start_nodes = .{ .keys = &.{"doc:a"} },
        },
    }};

    const graph_results = try alloc.alloc(db_mod.types.GraphSearchResult, 1);
    graph_results[0] = .{
        .name = try alloc.dupe(u8, "neighbors"),
        .nodes = &.{},
        .paths = &.{},
        .hits = &.{},
        .total_hits = 0,
        .metric_status = try alloc.dupe(db_mod.types.GraphMetricStatus, &.{
            .{
                .name = try alloc.dupe(u8, "pagerank"),
                .state = .fresh,
                .published_generation = 5,
                .edge_generation = 5,
                .target_edge_generation = 5,
                .progress = 1.0,
                .converged = true,
            },
        }),
    };
    var unsolicited_status = db_mod.types.SearchResult{
        .alloc = alloc,
        .hits = &.{},
        .total_hits = 0,
        .graph_results = graph_results,
    };
    defer unsolicited_status.deinit();

    try std.testing.expectError(error.UnsupportedQueryRequest, mergeSearchResults(alloc, .{ .graph_queries = &graph_queries }, &.{unsolicited_status}, 0, 10));

    const graph_metric_reads = [_]graph_query_mod.GraphMetricRead{.{
        .name = "pagerank",
        .freshness = .published,
    }};
    const metric_graph_queries = [_]db_mod.types.NamedGraphQuery{.{
        .name = "neighbors",
        .query = .{
            .query_type = .neighbors,
            .index_name = "graph_idx",
            .start_nodes = .{ .keys = &.{"doc:a"} },
            .metrics = &graph_metric_reads,
        },
    }};
    const extra_status_results = try alloc.alloc(db_mod.types.GraphSearchResult, 1);
    extra_status_results[0] = .{
        .name = try alloc.dupe(u8, "neighbors"),
        .nodes = &.{},
        .paths = &.{},
        .hits = &.{},
        .total_hits = 0,
        .metric_status = try alloc.dupe(db_mod.types.GraphMetricStatus, &.{
            .{
                .name = try alloc.dupe(u8, "pagerank"),
                .state = .fresh,
                .published_generation = 5,
                .edge_generation = 5,
                .target_edge_generation = 5,
                .progress = 1.0,
                .converged = true,
            },
            .{
                .name = try alloc.dupe(u8, "degree"),
                .state = .fresh,
                .published_generation = 5,
                .edge_generation = 5,
                .target_edge_generation = 5,
                .progress = 1.0,
                .converged = true,
            },
        }),
    };
    var extra_status = db_mod.types.SearchResult{
        .alloc = alloc,
        .hits = &.{},
        .total_hits = 0,
        .graph_results = extra_status_results,
    };
    defer extra_status.deinit();
    try std.testing.expectError(error.UnsupportedQueryRequest, mergeSearchResults(alloc, .{ .graph_queries = &metric_graph_queries }, &.{extra_status}, 0, 10));
}

test "query merge validates included graph search metric status list" {
    const alloc = std.testing.allocator;

    const graph_queries = [_]db_mod.types.NamedGraphQuery{.{
        .name = "neighbors",
        .query = .{
            .query_type = .neighbors,
            .index_name = "graph_idx",
            .start_nodes = .{ .keys = &.{"doc:a"} },
            .include_metric_status = true,
        },
    }};

    const duplicate_status_results = try alloc.alloc(db_mod.types.GraphSearchResult, 1);
    duplicate_status_results[0] = .{
        .name = try alloc.dupe(u8, "neighbors"),
        .nodes = &.{},
        .paths = &.{},
        .hits = &.{},
        .total_hits = 0,
        .metric_status = try alloc.dupe(db_mod.types.GraphMetricStatus, &.{
            .{
                .name = try alloc.dupe(u8, "pagerank"),
                .state = .fresh,
                .published_generation = 5,
                .edge_generation = 5,
                .target_edge_generation = 5,
                .progress = 1.0,
                .converged = true,
            },
            .{
                .name = try alloc.dupe(u8, "pagerank"),
                .state = .fresh,
                .published_generation = 5,
                .edge_generation = 5,
                .target_edge_generation = 5,
                .progress = 1.0,
                .converged = true,
            },
        }),
    };
    var duplicate_status = db_mod.types.SearchResult{
        .alloc = alloc,
        .hits = &.{},
        .total_hits = 0,
        .graph_results = duplicate_status_results,
    };
    defer duplicate_status.deinit();
    try std.testing.expectError(error.UnsupportedQueryRequest, mergeSearchResults(alloc, .{ .graph_queries = &graph_queries }, &.{duplicate_status}, 0, 10));

    const invalid_status_results = try alloc.alloc(db_mod.types.GraphSearchResult, 1);
    invalid_status_results[0] = .{
        .name = try alloc.dupe(u8, "neighbors"),
        .nodes = &.{},
        .paths = &.{},
        .hits = &.{},
        .total_hits = 0,
        .metric_status = try alloc.dupe(db_mod.types.GraphMetricStatus, &.{
            .{
                .name = try alloc.dupe(u8, "pagerank"),
                .state = .not_ready,
                .published_generation = 5,
                .edge_generation = 5,
                .target_edge_generation = 5,
            },
        }),
    };
    var invalid_status = db_mod.types.SearchResult{
        .alloc = alloc,
        .hits = &.{},
        .total_hits = 0,
        .graph_results = invalid_status_results,
    };
    defer invalid_status.deinit();
    try std.testing.expectError(error.UnsupportedQueryRequest, mergeSearchResults(alloc, .{ .graph_queries = &graph_queries }, &.{invalid_status}, 0, 10));

    const hits_pair_status_results = try alloc.alloc(db_mod.types.GraphSearchResult, 1);
    hits_pair_status_results[0] = .{
        .name = try alloc.dupe(u8, "neighbors"),
        .nodes = &.{},
        .paths = &.{},
        .hits = &.{},
        .total_hits = 0,
        .metric_status = try alloc.dupe(db_mod.types.GraphMetricStatus, &.{
            .{
                .name = try alloc.dupe(u8, "hits_authority"),
                .state = .fresh,
                .published_generation = 5,
                .edge_generation = 5,
                .target_edge_generation = 5,
                .progress = 1.0,
                .converged = true,
            },
            .{
                .name = try alloc.dupe(u8, "hits_hub"),
                .state = .fresh,
                .published_generation = 6,
                .edge_generation = 6,
                .target_edge_generation = 6,
                .progress = 1.0,
                .converged = true,
            },
        }),
    };
    var hits_pair_status_mismatch = db_mod.types.SearchResult{
        .alloc = alloc,
        .hits = &.{},
        .total_hits = 0,
        .graph_results = hits_pair_status_results,
    };
    defer hits_pair_status_mismatch.deinit();
    try std.testing.expectError(error.UnsupportedQueryRequest, mergeSearchResults(alloc, .{ .graph_queries = &graph_queries }, &.{hits_pair_status_mismatch}, 0, 10));
}

test "query merge rejects malformed graph search metric payloads" {
    const alloc = std.testing.allocator;

    const graph_metric_reads = [_]graph_query_mod.GraphMetricRead{.{
        .name = "pagerank",
        .freshness = .published,
    }};
    const graph_queries = [_]db_mod.types.NamedGraphQuery{.{
        .name = "neighbors",
        .query = .{
            .query_type = .neighbors,
            .index_name = "graph_idx",
            .start_nodes = .{ .keys = &.{"doc:a"} },
            .metrics = &graph_metric_reads,
        },
    }};

    const missing_nodes = try alloc.alloc(graph_query_mod.GraphResultNode, 1);
    missing_nodes[0] = .{
        .key = try alloc.dupe(u8, "doc:b"),
        .depth = 1,
        .distance = 1,
        .path = null,
        .path_edges = null,
        .metrics = &.{},
    };
    const missing_results = try alloc.alloc(db_mod.types.GraphSearchResult, 1);
    missing_results[0] = .{
        .name = try alloc.dupe(u8, "neighbors"),
        .nodes = missing_nodes,
        .paths = &.{},
        .hits = &.{},
        .total_hits = 1,
        .metric_status = try alloc.dupe(db_mod.types.GraphMetricStatus, &.{
            .{
                .name = try alloc.dupe(u8, "pagerank"),
                .state = .fresh,
                .published_generation = 5,
                .edge_generation = 5,
                .target_edge_generation = 5,
                .progress = 1.0,
                .converged = true,
            },
        }),
    };
    var missing_payload = db_mod.types.SearchResult{
        .alloc = alloc,
        .hits = &.{},
        .total_hits = 0,
        .graph_results = missing_results,
    };
    defer missing_payload.deinit();
    try std.testing.expectError(error.UnsupportedQueryRequest, mergeSearchResults(alloc, .{ .graph_queries = &graph_queries }, &.{missing_payload}, 0, 10));

    const duplicate_nodes = try alloc.alloc(graph_query_mod.GraphResultNode, 1);
    duplicate_nodes[0] = .{
        .key = try alloc.dupe(u8, "doc:b"),
        .depth = 1,
        .distance = 1,
        .path = null,
        .path_edges = null,
        .metrics = try alloc.dupe(graph_query_mod.GraphMetricValue, &.{
            .{ .name = try alloc.dupe(u8, "pagerank"), .score = 0.8 },
            .{ .name = try alloc.dupe(u8, "pagerank"), .score = 0.7 },
        }),
    };
    const duplicate_results = try alloc.alloc(db_mod.types.GraphSearchResult, 1);
    duplicate_results[0] = .{
        .name = try alloc.dupe(u8, "neighbors"),
        .nodes = duplicate_nodes,
        .paths = &.{},
        .hits = &.{},
        .total_hits = 1,
        .metric_status = try alloc.dupe(db_mod.types.GraphMetricStatus, &.{
            .{
                .name = try alloc.dupe(u8, "pagerank"),
                .state = .fresh,
                .published_generation = 5,
                .edge_generation = 5,
                .target_edge_generation = 5,
                .progress = 1.0,
                .converged = true,
            },
        }),
    };
    var duplicate_payload = db_mod.types.SearchResult{
        .alloc = alloc,
        .hits = &.{},
        .total_hits = 0,
        .graph_results = duplicate_results,
    };
    defer duplicate_payload.deinit();
    try std.testing.expectError(error.UnsupportedQueryRequest, mergeSearchResults(alloc, .{ .graph_queries = &graph_queries }, &.{duplicate_payload}, 0, 10));

    const non_finite_nodes = try alloc.alloc(graph_query_mod.GraphResultNode, 1);
    non_finite_nodes[0] = .{
        .key = try alloc.dupe(u8, "doc:b"),
        .depth = 1,
        .distance = 1,
        .path = null,
        .path_edges = null,
        .metrics = try alloc.dupe(graph_query_mod.GraphMetricValue, &.{
            .{ .name = try alloc.dupe(u8, "pagerank"), .score = std.math.nan(f64) },
        }),
    };
    const non_finite_results = try alloc.alloc(db_mod.types.GraphSearchResult, 1);
    non_finite_results[0] = .{
        .name = try alloc.dupe(u8, "neighbors"),
        .nodes = non_finite_nodes,
        .paths = &.{},
        .hits = &.{},
        .total_hits = 1,
        .metric_status = try alloc.dupe(db_mod.types.GraphMetricStatus, &.{
            .{
                .name = try alloc.dupe(u8, "pagerank"),
                .state = .fresh,
                .published_generation = 5,
                .edge_generation = 5,
                .target_edge_generation = 5,
                .progress = 1.0,
                .converged = true,
            },
        }),
    };
    var non_finite_payload = db_mod.types.SearchResult{
        .alloc = alloc,
        .hits = &.{},
        .total_hits = 0,
        .graph_results = non_finite_results,
    };
    defer non_finite_payload.deinit();
    try std.testing.expectError(error.UnsupportedQueryRequest, mergeSearchResults(alloc, .{ .graph_queries = &graph_queries }, &.{non_finite_payload}, 0, 10));

    const duplicate_projected_metric_reads = [_]graph_query_mod.GraphMetricRead{
        .{
            .name = "pagerank",
            .freshness = .published,
        },
        .{
            .name = "pagerank",
            .freshness = .published,
        },
    };
    const duplicate_projected_metric_queries = [_]db_mod.types.NamedGraphQuery{.{
        .name = "neighbors",
        .query = .{
            .query_type = .neighbors,
            .index_name = "graph_idx",
            .start_nodes = .{ .keys = &.{"doc:a"} },
            .metrics = &duplicate_projected_metric_reads,
        },
    }};
    const duplicate_projected_metric_nodes = try alloc.alloc(graph_query_mod.GraphResultNode, 1);
    duplicate_projected_metric_nodes[0] = .{
        .key = try alloc.dupe(u8, "doc:b"),
        .depth = 1,
        .distance = 1,
        .path = null,
        .path_edges = null,
        .metrics = try alloc.dupe(graph_query_mod.GraphMetricValue, &.{
            .{ .name = try alloc.dupe(u8, "pagerank"), .score = 0.8 },
        }),
    };
    const duplicate_projected_metric_results = try alloc.alloc(db_mod.types.GraphSearchResult, 1);
    duplicate_projected_metric_results[0] = .{
        .name = try alloc.dupe(u8, "neighbors"),
        .nodes = duplicate_projected_metric_nodes,
        .paths = &.{},
        .hits = &.{},
        .total_hits = 1,
        .metric_status = try alloc.dupe(db_mod.types.GraphMetricStatus, &.{
            .{
                .name = try alloc.dupe(u8, "pagerank"),
                .state = .fresh,
                .published_generation = 5,
                .edge_generation = 5,
                .target_edge_generation = 5,
                .progress = 1.0,
                .converged = true,
            },
        }),
    };
    var duplicate_projected_metric_payload = db_mod.types.SearchResult{
        .alloc = alloc,
        .hits = &.{},
        .total_hits = 0,
        .graph_results = duplicate_projected_metric_results,
    };
    defer duplicate_projected_metric_payload.deinit();
    try std.testing.expectError(error.UnsupportedQueryRequest, mergeSearchResults(alloc, .{ .graph_queries = &duplicate_projected_metric_queries }, &.{duplicate_projected_metric_payload}, 0, 10));

    const status_only_queries = [_]db_mod.types.NamedGraphQuery{.{
        .name = "neighbors",
        .query = .{
            .query_type = .neighbors,
            .index_name = "graph_idx",
            .start_nodes = .{ .keys = &.{"doc:a"} },
            .include_metric_status = true,
        },
    }};
    const unsolicited_nodes = try alloc.alloc(graph_query_mod.GraphResultNode, 1);
    unsolicited_nodes[0] = .{
        .key = try alloc.dupe(u8, "doc:b"),
        .depth = 1,
        .distance = 1,
        .path = null,
        .path_edges = null,
        .metrics = try alloc.dupe(graph_query_mod.GraphMetricValue, &.{
            .{ .name = try alloc.dupe(u8, "pagerank"), .score = 0.8 },
        }),
    };
    const unsolicited_results = try alloc.alloc(db_mod.types.GraphSearchResult, 1);
    unsolicited_results[0] = .{
        .name = try alloc.dupe(u8, "neighbors"),
        .nodes = unsolicited_nodes,
        .paths = &.{},
        .hits = &.{},
        .total_hits = 1,
        .metric_status = try alloc.dupe(db_mod.types.GraphMetricStatus, &.{
            .{
                .name = try alloc.dupe(u8, "pagerank"),
                .state = .fresh,
                .published_generation = 5,
                .edge_generation = 5,
                .target_edge_generation = 5,
                .progress = 1.0,
                .converged = true,
            },
        }),
    };
    var unsolicited_payload = db_mod.types.SearchResult{
        .alloc = alloc,
        .hits = &.{},
        .total_hits = 0,
        .graph_results = unsolicited_results,
    };
    defer unsolicited_payload.deinit();
    try std.testing.expectError(error.UnsupportedQueryRequest, mergeSearchResults(alloc, .{ .graph_queries = &status_only_queries }, &.{unsolicited_payload}, 0, 10));
}

test "query merge rejects malformed graph search traversal payloads" {
    const alloc = std.testing.allocator;

    const graph_queries = [_]db_mod.types.NamedGraphQuery{.{
        .name = "neighbors",
        .query = .{
            .query_type = .neighbors,
            .index_name = "graph_idx",
            .start_nodes = .{ .keys = &.{"doc:a"} },
        },
    }};

    const non_finite_nodes = try alloc.alloc(graph_query_mod.GraphResultNode, 1);
    non_finite_nodes[0] = .{
        .key = try alloc.dupe(u8, "doc:b"),
        .depth = 1,
        .distance = std.math.inf(f64),
        .path = null,
        .path_edges = null,
        .metrics = &.{},
    };
    const non_finite_node_results = try alloc.alloc(db_mod.types.GraphSearchResult, 1);
    non_finite_node_results[0] = .{
        .name = try alloc.dupe(u8, "neighbors"),
        .nodes = non_finite_nodes,
        .paths = &.{},
        .hits = &.{},
        .total_hits = 1,
        .metric_status = &.{},
    };
    var non_finite_node = db_mod.types.SearchResult{
        .alloc = alloc,
        .hits = &.{},
        .total_hits = 0,
        .graph_results = non_finite_node_results,
    };
    defer non_finite_node.deinit();
    try std.testing.expectError(error.UnsupportedQueryRequest, mergeSearchResults(alloc, .{ .graph_queries = &graph_queries }, &.{non_finite_node}, 0, 10));

    const node_path_edges = try alloc.alloc(graph_query_mod.PathEdgeInfo, 1);
    node_path_edges[0] = .{
        .source = try alloc.dupe(u8, "doc:a"),
        .target = try alloc.dupe(u8, "doc:b"),
        .edge_type = try alloc.dupe(u8, "links"),
        .weight = std.math.nan(f64),
    };
    const non_finite_node_edge_nodes = try alloc.alloc(graph_query_mod.GraphResultNode, 1);
    non_finite_node_edge_nodes[0] = .{
        .key = try alloc.dupe(u8, "doc:b"),
        .depth = 1,
        .distance = 1,
        .path = null,
        .path_edges = node_path_edges,
        .metrics = &.{},
    };
    const non_finite_node_edge_results = try alloc.alloc(db_mod.types.GraphSearchResult, 1);
    non_finite_node_edge_results[0] = .{
        .name = try alloc.dupe(u8, "neighbors"),
        .nodes = non_finite_node_edge_nodes,
        .paths = &.{},
        .hits = &.{},
        .total_hits = 1,
        .metric_status = &.{},
    };
    var non_finite_node_edge = db_mod.types.SearchResult{
        .alloc = alloc,
        .hits = &.{},
        .total_hits = 0,
        .graph_results = non_finite_node_edge_results,
    };
    defer non_finite_node_edge.deinit();
    try std.testing.expectError(error.UnsupportedQueryRequest, mergeSearchResults(alloc, .{ .graph_queries = &graph_queries }, &.{non_finite_node_edge}, 0, 10));

    const mismatched_path_nodes = try alloc.alloc([]const u8, 1);
    mismatched_path_nodes[0] = try alloc.dupe(u8, "doc:a");
    const mismatched_path_edges = try alloc.alloc(graph_query_mod.PathEdgeInfo, 1);
    mismatched_path_edges[0] = .{
        .source = try alloc.dupe(u8, "doc:a"),
        .target = try alloc.dupe(u8, "doc:b"),
        .edge_type = try alloc.dupe(u8, "links"),
        .weight = 1.0,
    };
    const mismatched_node_path_nodes = try alloc.alloc(graph_query_mod.GraphResultNode, 1);
    mismatched_node_path_nodes[0] = .{
        .key = try alloc.dupe(u8, "doc:b"),
        .depth = 1,
        .distance = 1,
        .path = mismatched_path_nodes,
        .path_edges = mismatched_path_edges,
        .metrics = &.{},
    };
    const mismatched_node_path_results = try alloc.alloc(db_mod.types.GraphSearchResult, 1);
    mismatched_node_path_results[0] = .{
        .name = try alloc.dupe(u8, "neighbors"),
        .nodes = mismatched_node_path_nodes,
        .paths = &.{},
        .hits = &.{},
        .total_hits = 1,
        .metric_status = &.{},
    };
    var mismatched_node_path = db_mod.types.SearchResult{
        .alloc = alloc,
        .hits = &.{},
        .total_hits = 0,
        .graph_results = mismatched_node_path_results,
    };
    defer mismatched_node_path.deinit();
    try std.testing.expectError(error.UnsupportedQueryRequest, mergeSearchResults(alloc, .{ .graph_queries = &graph_queries }, &.{mismatched_node_path}, 0, 10));

    const duplicate_nodes = try alloc.alloc(graph_query_mod.GraphResultNode, 2);
    duplicate_nodes[0] = .{
        .key = try alloc.dupe(u8, "doc:b"),
        .depth = 1,
        .distance = 1,
        .path = null,
        .path_edges = null,
        .metrics = &.{},
    };
    duplicate_nodes[1] = .{
        .key = try alloc.dupe(u8, "doc:b"),
        .depth = 1,
        .distance = 1,
        .path = null,
        .path_edges = null,
        .metrics = &.{},
    };
    const duplicate_node_results = try alloc.alloc(db_mod.types.GraphSearchResult, 1);
    duplicate_node_results[0] = .{
        .name = try alloc.dupe(u8, "neighbors"),
        .nodes = duplicate_nodes,
        .paths = &.{},
        .hits = &.{},
        .total_hits = 2,
        .metric_status = &.{},
    };
    var duplicate_node_payload = db_mod.types.SearchResult{
        .alloc = alloc,
        .hits = &.{},
        .total_hits = 0,
        .graph_results = duplicate_node_results,
    };
    defer duplicate_node_payload.deinit();
    try std.testing.expectError(error.UnsupportedQueryRequest, mergeSearchResults(alloc, .{ .graph_queries = &graph_queries }, &.{duplicate_node_payload}, 0, 10));

    const duplicate_shard_left_nodes = try alloc.alloc(graph_query_mod.GraphResultNode, 1);
    duplicate_shard_left_nodes[0] = .{
        .key = try alloc.dupe(u8, "doc:c"),
        .depth = 1,
        .distance = 1,
        .path = null,
        .path_edges = null,
        .metrics = &.{},
    };
    const duplicate_shard_left_results = try alloc.alloc(db_mod.types.GraphSearchResult, 1);
    duplicate_shard_left_results[0] = .{
        .name = try alloc.dupe(u8, "neighbors"),
        .nodes = duplicate_shard_left_nodes,
        .paths = &.{},
        .hits = &.{},
        .total_hits = 1,
        .metric_status = &.{},
    };
    var duplicate_shard_left = db_mod.types.SearchResult{
        .alloc = alloc,
        .hits = &.{},
        .total_hits = 0,
        .graph_results = duplicate_shard_left_results,
    };
    defer duplicate_shard_left.deinit();

    const duplicate_shard_right_nodes = try alloc.alloc(graph_query_mod.GraphResultNode, 1);
    duplicate_shard_right_nodes[0] = .{
        .key = try alloc.dupe(u8, "doc:c"),
        .depth = 1,
        .distance = 1,
        .path = null,
        .path_edges = null,
        .metrics = &.{},
    };
    const duplicate_shard_right_results = try alloc.alloc(db_mod.types.GraphSearchResult, 1);
    duplicate_shard_right_results[0] = .{
        .name = try alloc.dupe(u8, "neighbors"),
        .nodes = duplicate_shard_right_nodes,
        .paths = &.{},
        .hits = &.{},
        .total_hits = 1,
        .metric_status = &.{},
    };
    var duplicate_shard_right = db_mod.types.SearchResult{
        .alloc = alloc,
        .hits = &.{},
        .total_hits = 0,
        .graph_results = duplicate_shard_right_results,
    };
    defer duplicate_shard_right.deinit();
    try std.testing.expectError(error.UnsupportedQueryRequest, mergeSearchResults(alloc, .{ .graph_queries = &graph_queries }, &.{ duplicate_shard_left, duplicate_shard_right }, 0, 10));

    const path_nodes = try alloc.alloc([]const u8, 2);
    path_nodes[0] = try alloc.dupe(u8, "doc:a");
    path_nodes[1] = try alloc.dupe(u8, "doc:b");
    const path_edges = try alloc.alloc(graph_paths.PathEdge, 1);
    path_edges[0] = .{
        .source = try alloc.dupe(u8, "doc:a"),
        .target = try alloc.dupe(u8, "doc:b"),
        .edge_type = try alloc.dupe(u8, "links"),
        .weight = 1.0,
    };
    const non_finite_paths = try alloc.alloc(db_mod.types.GraphPath, 1);
    non_finite_paths[0] = .{
        .nodes = path_nodes,
        .edges = path_edges,
        .total_weight = std.math.inf(f64),
        .length = 1,
    };
    const non_finite_path_results = try alloc.alloc(db_mod.types.GraphSearchResult, 1);
    non_finite_path_results[0] = .{
        .name = try alloc.dupe(u8, "neighbors"),
        .nodes = &.{},
        .paths = non_finite_paths,
        .hits = &.{},
        .total_hits = 1,
        .metric_status = &.{},
    };
    var non_finite_path = db_mod.types.SearchResult{
        .alloc = alloc,
        .hits = &.{},
        .total_hits = 0,
        .graph_results = non_finite_path_results,
    };
    defer non_finite_path.deinit();
    try std.testing.expectError(error.UnsupportedQueryRequest, mergeSearchResults(alloc, .{ .graph_queries = &graph_queries }, &.{non_finite_path}, 0, 10));

    const bad_length_path_nodes = try alloc.alloc([]const u8, 2);
    bad_length_path_nodes[0] = try alloc.dupe(u8, "doc:a");
    bad_length_path_nodes[1] = try alloc.dupe(u8, "doc:b");
    const bad_length_path_edges = try alloc.alloc(graph_paths.PathEdge, 1);
    bad_length_path_edges[0] = .{
        .source = try alloc.dupe(u8, "doc:a"),
        .target = try alloc.dupe(u8, "doc:b"),
        .edge_type = try alloc.dupe(u8, "links"),
        .weight = 1.0,
    };
    const bad_length_paths = try alloc.alloc(db_mod.types.GraphPath, 1);
    bad_length_paths[0] = .{
        .nodes = bad_length_path_nodes,
        .edges = bad_length_path_edges,
        .total_weight = 1.0,
        .length = 2,
    };
    const bad_length_results = try alloc.alloc(db_mod.types.GraphSearchResult, 1);
    bad_length_results[0] = .{
        .name = try alloc.dupe(u8, "neighbors"),
        .nodes = &.{},
        .paths = bad_length_paths,
        .hits = &.{},
        .total_hits = 1,
        .metric_status = &.{},
    };
    var bad_length = db_mod.types.SearchResult{
        .alloc = alloc,
        .hits = &.{},
        .total_hits = 0,
        .graph_results = bad_length_results,
    };
    defer bad_length.deinit();
    try std.testing.expectError(error.UnsupportedQueryRequest, mergeSearchResults(alloc, .{ .graph_queries = &graph_queries }, &.{bad_length}, 0, 10));
}

test "query merge rejects malformed graph search hit payloads" {
    const alloc = std.testing.allocator;

    const graph_queries = [_]db_mod.types.NamedGraphQuery{.{
        .name = "neighbors",
        .query = .{
            .query_type = .neighbors,
            .index_name = "graph_idx",
            .start_nodes = .{ .keys = &.{"doc:a"} },
        },
    }};

    const non_finite_hits = try alloc.alloc(db_mod.types.SearchHit, 1);
    non_finite_hits[0] = .{
        .id = try alloc.dupe(u8, "doc:b"),
        .score = std.math.inf(f32),
    };
    const non_finite_results = try alloc.alloc(db_mod.types.GraphSearchResult, 1);
    non_finite_results[0] = .{
        .name = try alloc.dupe(u8, "neighbors"),
        .nodes = &.{},
        .paths = &.{},
        .hits = non_finite_hits,
        .total_hits = 1,
        .metric_status = &.{},
    };
    var non_finite = db_mod.types.SearchResult{
        .alloc = alloc,
        .hits = &.{},
        .total_hits = 0,
        .graph_results = non_finite_results,
    };
    defer non_finite.deinit();
    try std.testing.expectError(error.UnsupportedQueryRequest, mergeSearchResults(alloc, .{ .graph_queries = &graph_queries }, &.{non_finite}, 0, 10));

    const duplicate_hits = try alloc.alloc(db_mod.types.SearchHit, 2);
    duplicate_hits[0] = .{
        .id = try alloc.dupe(u8, "doc:b"),
        .score = 1.0,
    };
    duplicate_hits[1] = .{
        .id = try alloc.dupe(u8, "doc:b"),
        .score = 0.9,
    };
    const duplicate_results = try alloc.alloc(db_mod.types.GraphSearchResult, 1);
    duplicate_results[0] = .{
        .name = try alloc.dupe(u8, "neighbors"),
        .nodes = &.{},
        .paths = &.{},
        .hits = duplicate_hits,
        .total_hits = 2,
        .metric_status = &.{},
    };
    var duplicate = db_mod.types.SearchResult{
        .alloc = alloc,
        .hits = &.{},
        .total_hits = 0,
        .graph_results = duplicate_results,
    };
    defer duplicate.deinit();
    try std.testing.expectError(error.UnsupportedQueryRequest, mergeSearchResults(alloc, .{ .graph_queries = &graph_queries }, &.{duplicate}, 0, 10));

    const duplicate_shard_left_hits = try alloc.alloc(db_mod.types.SearchHit, 1);
    duplicate_shard_left_hits[0] = .{
        .id = try alloc.dupe(u8, "doc:c"),
        .score = 1.0,
    };
    const duplicate_shard_left_results = try alloc.alloc(db_mod.types.GraphSearchResult, 1);
    duplicate_shard_left_results[0] = .{
        .name = try alloc.dupe(u8, "neighbors"),
        .nodes = &.{},
        .paths = &.{},
        .hits = duplicate_shard_left_hits,
        .total_hits = 1,
        .metric_status = &.{},
    };
    var duplicate_shard_left = db_mod.types.SearchResult{
        .alloc = alloc,
        .hits = &.{},
        .total_hits = 0,
        .graph_results = duplicate_shard_left_results,
    };
    defer duplicate_shard_left.deinit();

    const duplicate_shard_right_hits = try alloc.alloc(db_mod.types.SearchHit, 1);
    duplicate_shard_right_hits[0] = .{
        .id = try alloc.dupe(u8, "doc:c"),
        .score = 0.9,
    };
    const duplicate_shard_right_results = try alloc.alloc(db_mod.types.GraphSearchResult, 1);
    duplicate_shard_right_results[0] = .{
        .name = try alloc.dupe(u8, "neighbors"),
        .nodes = &.{},
        .paths = &.{},
        .hits = duplicate_shard_right_hits,
        .total_hits = 1,
        .metric_status = &.{},
    };
    var duplicate_shard_right = db_mod.types.SearchResult{
        .alloc = alloc,
        .hits = &.{},
        .total_hits = 0,
        .graph_results = duplicate_shard_right_results,
    };
    defer duplicate_shard_right.deinit();
    try std.testing.expectError(error.UnsupportedQueryRequest, mergeSearchResults(alloc, .{ .graph_queries = &graph_queries }, &.{ duplicate_shard_left, duplicate_shard_right }, 0, 10));

    const score_detail_hits = try alloc.alloc(db_mod.types.SearchHit, 1);
    score_detail_hits[0] = .{
        .id = try alloc.dupe(u8, "doc:b"),
        .score = 2.0,
        .score_details = .{
            .index_name = try alloc.dupe(u8, "graph_idx"),
            .metric_name = try alloc.dupe(u8, "pagerank"),
            .base_score = 1.0,
            .base_weight = 1.0,
            .metric_score = 0.5,
            .metric_score_used = 0.5,
            .metric_weight = 2.0,
            .final_score = 2.0,
            .published_generation = 8,
        },
    };
    const score_detail_results = try alloc.alloc(db_mod.types.GraphSearchResult, 1);
    score_detail_results[0] = .{
        .name = try alloc.dupe(u8, "neighbors"),
        .nodes = &.{},
        .paths = &.{},
        .hits = score_detail_hits,
        .total_hits = 1,
        .metric_status = &.{},
    };
    var score_detail = db_mod.types.SearchResult{
        .alloc = alloc,
        .hits = &.{},
        .total_hits = 0,
        .graph_results = score_detail_results,
    };
    defer score_detail.deinit();
    try std.testing.expectError(error.UnsupportedQueryRequest, mergeSearchResults(alloc, .{ .graph_queries = &graph_queries }, &.{score_detail}, 0, 10));
}

test "query merge preserves failed graph metric status across shard fan-in" {
    const alloc = std.testing.allocator;

    const left_scores = try alloc.alloc(db_mod.types.GraphMetricScore, 1);
    left_scores[0] = .{ .node = try alloc.dupe(u8, "doc:a"), .score = 0.8 };
    const left_metrics = try alloc.alloc(db_mod.types.GraphMetricResult, 1);
    left_metrics[0] = .{
        .name = try alloc.dupe(u8, "central"),
        .index_name = try alloc.dupe(u8, "graph_idx"),
        .metric_name = try alloc.dupe(u8, "pagerank"),
        .scores = left_scores,
        .status = .{
            .name = try alloc.dupe(u8, "pagerank"),
            .state = .fresh,
            .published_generation = 5,
            .edge_generation = 5,
            .target_edge_generation = 5,
            .progress = 1.0,
            .converged = true,
        },
    };
    var left = db_mod.types.SearchResult{
        .alloc = alloc,
        .hits = &.{},
        .total_hits = 0,
        .graph_metric_results = left_metrics,
    };
    defer left.deinit();

    const right_scores = try alloc.alloc(db_mod.types.GraphMetricScore, 1);
    right_scores[0] = .{ .node = try alloc.dupe(u8, "doc:b"), .score = 0.7 };
    const right_metrics = try alloc.alloc(db_mod.types.GraphMetricResult, 1);
    right_metrics[0] = .{
        .name = try alloc.dupe(u8, "central"),
        .index_name = try alloc.dupe(u8, "graph_idx"),
        .metric_name = try alloc.dupe(u8, "pagerank"),
        .scores = right_scores,
        .status = .{
            .name = try alloc.dupe(u8, "pagerank"),
            .state = .failed,
            .published_generation = 5,
            .edge_generation = 6,
            .target_edge_generation = 6,
            .building_generation = 6,
            .build_job_id = 99,
            .retry_count = 3,
            .last_error = try alloc.dupe(u8, "publish verifier rejected generation"),
            .progress = 0.4,
            .converged = true,
        },
    };
    var right = db_mod.types.SearchResult{
        .alloc = alloc,
        .hits = &.{},
        .total_hits = 0,
        .graph_metric_results = right_metrics,
    };
    defer right.deinit();

    const graph_metric_queries = [_]db_mod.types.NamedGraphMetricQuery{.{
        .name = "central",
        .query = .{
            .index_name = "graph_idx",
            .metric_name = "pagerank",
            .top_k = 2,
            .freshness = .published,
        },
    }};
    var merged = try mergeSearchResults(alloc, .{ .graph_metric_queries = &graph_metric_queries }, &.{ left, right }, 0, 10);
    defer merged.deinit();

    try std.testing.expectEqual(@as(usize, 1), merged.graph_metric_results.len);
    try std.testing.expectEqual(@as(usize, 2), merged.graph_metric_results[0].scores.len);
    try std.testing.expectEqual(graph_mod.GraphIndex.GraphMetricState.failed, merged.graph_metric_results[0].status.state);
    try std.testing.expectEqual(@as(u64, 5), merged.graph_metric_results[0].status.published_generation);
    try std.testing.expectEqual(@as(u64, 6), merged.graph_metric_results[0].status.target_edge_generation);
    try std.testing.expectEqual(@as(u64, 6), merged.graph_metric_results[0].status.building_generation);
    try std.testing.expectEqual(@as(u32, 3), merged.graph_metric_results[0].status.retry_count);
    try std.testing.expectEqualStrings("publish verifier rejected generation", merged.graph_metric_results[0].status.last_error);

    const fresh_graph_metric_queries = [_]db_mod.types.NamedGraphMetricQuery{.{
        .name = "central",
        .query = .{
            .index_name = "graph_idx",
            .metric_name = "pagerank",
            .top_k = 2,
            .freshness = .fresh,
        },
    }};
    try std.testing.expectError(error.UnsupportedQueryRequest, mergeSearchResults(alloc, .{ .graph_metric_queries = &fresh_graph_metric_queries }, &.{ left, right }, 0, 10));

    const hits_left_authority_scores = try alloc.alloc(db_mod.types.GraphMetricScore, 1);
    hits_left_authority_scores[0] = .{ .node = try alloc.dupe(u8, "doc:a"), .score = 0.9 };
    const hits_left_hub_scores = try alloc.alloc(db_mod.types.GraphMetricScore, 1);
    hits_left_hub_scores[0] = .{ .node = try alloc.dupe(u8, "doc:c"), .score = 0.75 };
    const hits_left_metrics = try alloc.alloc(db_mod.types.GraphMetricResult, 2);
    hits_left_metrics[0] = .{
        .name = try alloc.dupe(u8, "authority"),
        .index_name = try alloc.dupe(u8, "graph_idx"),
        .metric_name = try alloc.dupe(u8, "hits_authority"),
        .scores = hits_left_authority_scores,
        .status = .{
            .name = try alloc.dupe(u8, "hits_authority"),
            .state = .fresh,
            .published_generation = 8,
            .edge_generation = 8,
            .target_edge_generation = 8,
            .progress = 1.0,
            .converged = true,
        },
    };
    hits_left_metrics[1] = .{
        .name = try alloc.dupe(u8, "hub"),
        .index_name = try alloc.dupe(u8, "graph_idx"),
        .metric_name = try alloc.dupe(u8, "hits_hub"),
        .scores = hits_left_hub_scores,
        .status = .{
            .name = try alloc.dupe(u8, "hits_hub"),
            .state = .fresh,
            .published_generation = 8,
            .edge_generation = 8,
            .target_edge_generation = 8,
            .progress = 1.0,
            .converged = true,
        },
    };
    var hits_left = db_mod.types.SearchResult{
        .alloc = alloc,
        .hits = &.{},
        .total_hits = 0,
        .graph_metric_results = hits_left_metrics,
    };
    defer hits_left.deinit();

    const hits_right_authority_scores = try alloc.alloc(db_mod.types.GraphMetricScore, 1);
    hits_right_authority_scores[0] = .{ .node = try alloc.dupe(u8, "doc:b"), .score = 0.7 };
    const hits_right_hub_scores = try alloc.alloc(db_mod.types.GraphMetricScore, 1);
    hits_right_hub_scores[0] = .{ .node = try alloc.dupe(u8, "doc:d"), .score = 0.65 };
    const hits_right_metrics = try alloc.alloc(db_mod.types.GraphMetricResult, 2);
    hits_right_metrics[0] = .{
        .name = try alloc.dupe(u8, "authority"),
        .index_name = try alloc.dupe(u8, "graph_idx"),
        .metric_name = try alloc.dupe(u8, "hits_authority"),
        .scores = hits_right_authority_scores,
        .status = .{
            .name = try alloc.dupe(u8, "hits_authority"),
            .state = .failed,
            .published_generation = 8,
            .edge_generation = 9,
            .target_edge_generation = 9,
            .building_generation = 9,
            .retry_count = 2,
            .last_error = try alloc.dupe(u8, "authority shard failed"),
            .progress = 0.5,
            .converged = true,
        },
    };
    hits_right_metrics[1] = .{
        .name = try alloc.dupe(u8, "hub"),
        .index_name = try alloc.dupe(u8, "graph_idx"),
        .metric_name = try alloc.dupe(u8, "hits_hub"),
        .scores = hits_right_hub_scores,
        .status = .{
            .name = try alloc.dupe(u8, "hits_hub"),
            .state = .failed,
            .published_generation = 8,
            .edge_generation = 9,
            .target_edge_generation = 9,
            .building_generation = 9,
            .retry_count = 2,
            .last_error = try alloc.dupe(u8, "hub shard failed"),
            .progress = 0.5,
            .converged = true,
        },
    };
    var hits_right = db_mod.types.SearchResult{
        .alloc = alloc,
        .hits = &.{},
        .total_hits = 0,
        .graph_metric_results = hits_right_metrics,
    };
    defer hits_right.deinit();

    const hits_metric_queries = [_]db_mod.types.NamedGraphMetricQuery{
        .{
            .name = "authority",
            .query = .{
                .index_name = "graph_idx",
                .metric_name = "hits_authority",
                .top_k = 2,
                .freshness = .published,
            },
        },
        .{
            .name = "hub",
            .query = .{
                .index_name = "graph_idx",
                .metric_name = "hits_hub",
                .top_k = 2,
                .freshness = .published,
            },
        },
    };
    var hits_merged = try mergeSearchResults(alloc, .{ .graph_metric_queries = &hits_metric_queries }, &.{ hits_left, hits_right }, 0, 10);
    defer hits_merged.deinit();
    try std.testing.expectEqual(@as(usize, 2), hits_merged.graph_metric_results.len);
    for (hits_merged.graph_metric_results) |metric_result| {
        try std.testing.expectEqual(@as(usize, 2), metric_result.scores.len);
        try std.testing.expectEqual(graph_mod.GraphIndex.GraphMetricState.failed, metric_result.status.state);
        try std.testing.expectEqual(@as(u64, 8), metric_result.status.published_generation);
        try std.testing.expectEqual(@as(u64, 9), metric_result.status.target_edge_generation);
        try std.testing.expectEqual(@as(u64, 9), metric_result.status.building_generation);
        try std.testing.expectEqual(@as(u32, 2), metric_result.status.retry_count);
    }

    const fresh_hits_metric_queries = [_]db_mod.types.NamedGraphMetricQuery{
        .{
            .name = "authority",
            .query = .{
                .index_name = "graph_idx",
                .metric_name = "hits_authority",
                .top_k = 2,
                .freshness = .fresh,
            },
        },
        .{
            .name = "hub",
            .query = .{
                .index_name = "graph_idx",
                .metric_name = "hits_hub",
                .top_k = 2,
                .freshness = .fresh,
            },
        },
    };
    try std.testing.expectError(error.UnsupportedQueryRequest, mergeSearchResults(alloc, .{ .graph_metric_queries = &fresh_hits_metric_queries }, &.{ hits_left, hits_right }, 0, 10));
}

test "query merge requires comparable graph search metric generations across shards" {
    const alloc = std.testing.allocator;

    const graph_metric_reads = [_]graph_query_mod.GraphMetricRead{.{
        .name = "pagerank",
        .freshness = .published,
    }};
    const graph_queries = [_]db_mod.types.NamedGraphQuery{.{
        .name = "neighbors",
        .query = .{
            .query_type = .neighbors,
            .index_name = "graph_idx",
            .start_nodes = .{ .keys = &.{"doc:a"} },
            .metrics = &graph_metric_reads,
            .include_metric_status = true,
        },
    }};
    const req = db_mod.types.SearchRequest{ .graph_queries = &graph_queries };

    const left_nodes = try alloc.alloc(graph_query_mod.GraphResultNode, 1);
    left_nodes[0] = .{
        .key = try alloc.dupe(u8, "doc:b"),
        .depth = 1,
        .distance = 1,
        .path = null,
        .path_edges = null,
        .metrics = try alloc.dupe(graph_query_mod.GraphMetricValue, &.{
            .{ .name = try alloc.dupe(u8, "pagerank"), .score = 0.8 },
        }),
    };
    const left_results = try alloc.alloc(db_mod.types.GraphSearchResult, 1);
    left_results[0] = .{
        .name = try alloc.dupe(u8, "neighbors"),
        .nodes = left_nodes,
        .paths = &.{},
        .hits = &.{},
        .total_hits = 1,
        .metric_status = try alloc.dupe(db_mod.types.GraphMetricStatus, &.{
            .{
                .name = try alloc.dupe(u8, "pagerank"),
                .state = .fresh,
                .published_generation = 5,
                .edge_generation = 5,
                .target_edge_generation = 5,
                .progress = 1.0,
                .converged = true,
            },
        }),
    };
    var left = db_mod.types.SearchResult{
        .alloc = alloc,
        .hits = &.{},
        .total_hits = 0,
        .graph_results = left_results,
    };
    defer left.deinit();

    const right_nodes = try alloc.alloc(graph_query_mod.GraphResultNode, 1);
    right_nodes[0] = .{
        .key = try alloc.dupe(u8, "doc:c"),
        .depth = 1,
        .distance = 1,
        .path = null,
        .path_edges = null,
        .metrics = try alloc.dupe(graph_query_mod.GraphMetricValue, &.{
            .{ .name = try alloc.dupe(u8, "pagerank"), .score = 0.7 },
        }),
    };
    const right_results = try alloc.alloc(db_mod.types.GraphSearchResult, 1);
    right_results[0] = .{
        .name = try alloc.dupe(u8, "neighbors"),
        .nodes = right_nodes,
        .paths = &.{},
        .hits = &.{},
        .total_hits = 1,
        .metric_status = try alloc.dupe(db_mod.types.GraphMetricStatus, &.{
            .{
                .name = try alloc.dupe(u8, "pagerank"),
                .state = .stale,
                .published_generation = 5,
                .edge_generation = 6,
                .target_edge_generation = 6,
                .progress = 1.0,
                .converged = true,
            },
        }),
    };
    var right = db_mod.types.SearchResult{
        .alloc = alloc,
        .hits = &.{},
        .total_hits = 0,
        .graph_results = right_results,
    };
    defer right.deinit();

    var merged = try mergeSearchResults(alloc, req, &.{ left, right }, 0, 10);
    defer merged.deinit();
    try std.testing.expectEqual(@as(usize, 1), merged.graph_results.len);
    try std.testing.expectEqual(@as(usize, 2), merged.graph_results[0].nodes.len);
    try std.testing.expectEqual(@as(usize, 1), merged.graph_results[0].metric_status.len);
    try std.testing.expectEqual(graph_mod.GraphIndex.GraphMetricState.stale, merged.graph_results[0].metric_status[0].state);
    try std.testing.expectEqual(@as(u64, 5), merged.graph_results[0].metric_status[0].published_generation);

    const fresh_graph_metric_reads = [_]graph_query_mod.GraphMetricRead{.{
        .name = "pagerank",
        .freshness = .fresh,
    }};
    const fresh_graph_queries = [_]db_mod.types.NamedGraphQuery{.{
        .name = "neighbors",
        .query = .{
            .query_type = .neighbors,
            .index_name = "graph_idx",
            .start_nodes = .{ .keys = &.{"doc:a"} },
            .metrics = &fresh_graph_metric_reads,
        },
    }};
    try std.testing.expectError(error.UnsupportedQueryRequest, mergeSearchResults(alloc, .{ .graph_queries = &fresh_graph_queries }, &.{ left, right }, 0, 10));

    right_results[0].metric_status[0].published_generation = 4;
    try std.testing.expectError(error.UnsupportedQueryRequest, mergeSearchResults(alloc, req, &.{ left, right }, 0, 10));

    right_results[0].metric_status[0].published_generation = 0;
    right_results[0].metric_status[0].state = .not_ready;
    try std.testing.expectError(error.UnsupportedQueryRequest, mergeSearchResults(alloc, req, &.{ left, right }, 0, 10));

    var missing = db_mod.types.SearchResult{
        .alloc = alloc,
        .hits = &.{},
        .total_hits = 0,
    };
    defer missing.deinit();
    try std.testing.expectError(error.UnsupportedQueryRequest, mergeSearchResults(alloc, req, &.{ left, missing }, 0, 10));
}

test "query merge allows unpublished projected graph search metric status" {
    const alloc = std.testing.allocator;

    const graph_metric_reads = [_]graph_query_mod.GraphMetricRead{.{
        .name = "pagerank",
        .freshness = .published,
    }};
    const graph_queries = [_]db_mod.types.NamedGraphQuery{.{
        .name = "neighbors",
        .query = .{
            .query_type = .neighbors,
            .index_name = "graph_idx",
            .start_nodes = .{ .keys = &.{"doc:a"} },
            .metrics = &graph_metric_reads,
        },
    }};
    const req = db_mod.types.SearchRequest{ .graph_queries = &graph_queries };

    const nodes = try alloc.alloc(graph_query_mod.GraphResultNode, 1);
    nodes[0] = .{
        .key = try alloc.dupe(u8, "doc:b"),
        .depth = 1,
        .distance = 1,
        .path = null,
        .path_edges = null,
        .metrics = try alloc.dupe(graph_query_mod.GraphMetricValue, &.{
            .{ .name = try alloc.dupe(u8, "pagerank"), .score = null },
        }),
    };
    const graph_results = try alloc.alloc(db_mod.types.GraphSearchResult, 1);
    graph_results[0] = .{
        .name = try alloc.dupe(u8, "neighbors"),
        .nodes = nodes,
        .paths = &.{},
        .hits = &.{},
        .total_hits = 1,
        .metric_status = try alloc.dupe(db_mod.types.GraphMetricStatus, &.{
            .{
                .name = try alloc.dupe(u8, "pagerank"),
                .state = .not_ready,
                .published_generation = 0,
                .edge_generation = 3,
                .target_edge_generation = 3,
                .progress = 0.0,
                .converged = false,
            },
        }),
    };
    var result = db_mod.types.SearchResult{
        .alloc = alloc,
        .hits = &.{},
        .total_hits = 0,
        .graph_results = graph_results,
    };
    defer result.deinit();

    var merged = try mergeSearchResults(alloc, req, &.{result}, 0, 10);
    defer merged.deinit();
    try std.testing.expectEqual(@as(usize, 1), merged.graph_results.len);
    try std.testing.expectEqual(@as(usize, 1), merged.graph_results[0].nodes.len);
    try std.testing.expectEqual(@as(usize, 1), merged.graph_results[0].nodes[0].metrics.len);
    try std.testing.expect(merged.graph_results[0].nodes[0].metrics[0].score == null);
    try std.testing.expectEqual(@as(usize, 0), merged.graph_results[0].metric_status.len);

    const include_graph_queries = [_]db_mod.types.NamedGraphQuery{.{
        .name = "neighbors",
        .query = .{
            .query_type = .neighbors,
            .index_name = "graph_idx",
            .start_nodes = .{ .keys = &.{"doc:a"} },
            .metrics = &graph_metric_reads,
            .include_metric_status = true,
        },
    }};
    var included = try mergeSearchResults(alloc, .{ .graph_queries = &include_graph_queries }, &.{result}, 0, 10);
    defer included.deinit();
    try std.testing.expectEqual(@as(usize, 1), included.graph_results[0].metric_status.len);
    try std.testing.expectEqual(graph_mod.GraphIndex.GraphMetricState.not_ready, included.graph_results[0].metric_status[0].state);
    try std.testing.expectEqual(@as(u64, 0), included.graph_results[0].metric_status[0].published_generation);

    const fresh_graph_metric_reads = [_]graph_query_mod.GraphMetricRead{.{
        .name = "pagerank",
        .freshness = .fresh,
    }};
    const fresh_graph_queries = [_]db_mod.types.NamedGraphQuery{.{
        .name = "neighbors",
        .query = .{
            .query_type = .neighbors,
            .index_name = "graph_idx",
            .start_nodes = .{ .keys = &.{"doc:a"} },
            .metrics = &fresh_graph_metric_reads,
        },
    }};
    try std.testing.expectError(error.UnsupportedQueryRequest, mergeSearchResults(alloc, .{ .graph_queries = &fresh_graph_queries }, &.{result}, 0, 10));

    const graph_metric_orders = [_]graph_query_mod.GraphMetricOrder{.{
        .name = "pagerank",
        .freshness = .published,
    }};
    const order_graph_queries = [_]db_mod.types.NamedGraphQuery{.{
        .name = "neighbors",
        .query = .{
            .query_type = .neighbors,
            .index_name = "graph_idx",
            .start_nodes = .{ .keys = &.{"doc:a"} },
            .order_by = &graph_metric_orders,
        },
    }};
    const order_results = try alloc.alloc(db_mod.types.GraphSearchResult, 1);
    order_results[0] = .{
        .name = try alloc.dupe(u8, "neighbors"),
        .nodes = &.{},
        .paths = &.{},
        .hits = &.{},
        .total_hits = 0,
        .metric_status = try alloc.dupe(db_mod.types.GraphMetricStatus, &.{
            .{
                .name = try alloc.dupe(u8, "pagerank"),
                .state = .not_ready,
                .published_generation = 0,
                .edge_generation = 3,
                .target_edge_generation = 3,
                .progress = 0.0,
                .converged = false,
            },
        }),
    };
    var order_result = db_mod.types.SearchResult{
        .alloc = alloc,
        .hits = &.{},
        .total_hits = 0,
        .graph_results = order_results,
    };
    defer order_result.deinit();
    try std.testing.expectError(error.UnsupportedQueryRequest, mergeSearchResults(alloc, .{ .graph_queries = &order_graph_queries }, &.{order_result}, 0, 10));
}

test "query merge rejects ambiguous graph search fan-in metric status" {
    const alloc = std.testing.allocator;

    const graph_metric_reads = [_]graph_query_mod.GraphMetricRead{.{
        .name = "pagerank",
        .freshness = .published,
    }};
    const graph_queries = [_]db_mod.types.NamedGraphQuery{.{
        .name = "neighbors",
        .query = .{
            .query_type = .neighbors,
            .index_name = "graph_idx",
            .start_nodes = .{ .keys = &.{"doc:a"} },
            .metrics = &graph_metric_reads,
        },
    }};
    const req = db_mod.types.SearchRequest{ .graph_queries = &graph_queries };

    const duplicate_status_results = try alloc.alloc(db_mod.types.GraphSearchResult, 1);
    duplicate_status_results[0] = .{
        .name = try alloc.dupe(u8, "neighbors"),
        .nodes = &.{},
        .paths = &.{},
        .hits = &.{},
        .total_hits = 0,
        .metric_status = try alloc.dupe(db_mod.types.GraphMetricStatus, &.{
            .{
                .name = try alloc.dupe(u8, "pagerank"),
                .state = .fresh,
                .published_generation = 5,
                .edge_generation = 5,
                .target_edge_generation = 5,
                .progress = 1.0,
                .converged = true,
            },
            .{
                .name = try alloc.dupe(u8, "pagerank"),
                .state = .stale,
                .published_generation = 5,
                .edge_generation = 6,
                .target_edge_generation = 6,
                .progress = 1.0,
                .converged = true,
            },
        }),
    };
    var duplicate_status = db_mod.types.SearchResult{
        .alloc = alloc,
        .hits = &.{},
        .total_hits = 0,
        .graph_results = duplicate_status_results,
    };
    defer duplicate_status.deinit();

    try std.testing.expectError(error.UnsupportedQueryRequest, mergeSearchResults(alloc, req, &.{duplicate_status}, 0, 10));

    const duplicate_query_results = try alloc.alloc(db_mod.types.GraphSearchResult, 2);
    duplicate_query_results[0] = .{
        .name = try alloc.dupe(u8, "neighbors"),
        .nodes = &.{},
        .paths = &.{},
        .hits = &.{},
        .total_hits = 0,
        .metric_status = try alloc.dupe(db_mod.types.GraphMetricStatus, &.{
            .{
                .name = try alloc.dupe(u8, "pagerank"),
                .state = .fresh,
                .published_generation = 5,
                .edge_generation = 5,
                .target_edge_generation = 5,
                .progress = 1.0,
                .converged = true,
            },
        }),
    };
    duplicate_query_results[1] = .{
        .name = try alloc.dupe(u8, "neighbors"),
        .nodes = &.{},
        .paths = &.{},
        .hits = &.{},
        .total_hits = 0,
        .metric_status = try alloc.dupe(db_mod.types.GraphMetricStatus, &.{
            .{
                .name = try alloc.dupe(u8, "pagerank"),
                .state = .fresh,
                .published_generation = 5,
                .edge_generation = 5,
                .target_edge_generation = 5,
                .progress = 1.0,
                .converged = true,
            },
        }),
    };
    var duplicate_query = db_mod.types.SearchResult{
        .alloc = alloc,
        .hits = &.{},
        .total_hits = 0,
        .graph_results = duplicate_query_results,
    };
    defer duplicate_query.deinit();

    try std.testing.expectError(error.UnsupportedQueryRequest, mergeSearchResults(alloc, req, &.{duplicate_query}, 0, 10));

    const duplicate_request_queries = [_]db_mod.types.NamedGraphQuery{
        .{
            .name = "neighbors",
            .query = .{
                .query_type = .neighbors,
                .index_name = "graph_idx",
                .start_nodes = .{ .keys = &.{"doc:a"} },
                .metrics = &graph_metric_reads,
            },
        },
        .{
            .name = "neighbors",
            .query = .{
                .query_type = .neighbors,
                .index_name = "graph_idx",
                .start_nodes = .{ .keys = &.{"doc:b"} },
                .metrics = &graph_metric_reads,
            },
        },
    };
    const valid_query_results = try alloc.alloc(db_mod.types.GraphSearchResult, 1);
    valid_query_results[0] = .{
        .name = try alloc.dupe(u8, "neighbors"),
        .nodes = &.{},
        .paths = &.{},
        .hits = &.{},
        .total_hits = 0,
        .metric_status = try alloc.dupe(db_mod.types.GraphMetricStatus, &.{
            .{
                .name = try alloc.dupe(u8, "pagerank"),
                .state = .fresh,
                .published_generation = 5,
                .edge_generation = 5,
                .target_edge_generation = 5,
                .progress = 1.0,
                .converged = true,
            },
        }),
    };
    var duplicate_request = db_mod.types.SearchResult{
        .alloc = alloc,
        .hits = &.{},
        .total_hits = 0,
        .graph_results = valid_query_results,
    };
    defer duplicate_request.deinit();
    try std.testing.expectError(error.UnsupportedQueryRequest, mergeSearchResults(alloc, .{ .graph_queries = &duplicate_request_queries }, &.{duplicate_request}, 0, 10));

    const duplicate_order_metrics = [_]graph_query_mod.GraphMetricOrder{
        .{
            .name = "pagerank",
            .freshness = .published,
        },
        .{
            .name = "pagerank",
            .direction = .asc,
            .freshness = .published,
        },
    };
    const duplicate_order_queries = [_]db_mod.types.NamedGraphQuery{.{
        .name = "neighbors",
        .query = .{
            .query_type = .neighbors,
            .index_name = "graph_idx",
            .start_nodes = .{ .keys = &.{"doc:a"} },
            .order_by = &duplicate_order_metrics,
        },
    }};
    const duplicate_order_results = try alloc.alloc(db_mod.types.GraphSearchResult, 1);
    duplicate_order_results[0] = .{
        .name = try alloc.dupe(u8, "neighbors"),
        .nodes = &.{},
        .paths = &.{},
        .hits = &.{},
        .total_hits = 0,
        .metric_status = try alloc.dupe(db_mod.types.GraphMetricStatus, &.{
            .{
                .name = try alloc.dupe(u8, "pagerank"),
                .state = .fresh,
                .published_generation = 5,
                .edge_generation = 5,
                .target_edge_generation = 5,
                .progress = 1.0,
                .converged = true,
            },
        }),
    };
    var duplicate_order = db_mod.types.SearchResult{
        .alloc = alloc,
        .hits = &.{},
        .total_hits = 0,
        .graph_results = duplicate_order_results,
    };
    defer duplicate_order.deinit();
    try std.testing.expectError(error.UnsupportedQueryRequest, mergeSearchResults(alloc, .{ .graph_queries = &duplicate_order_queries }, &.{duplicate_order}, 0, 10));
}

test "query merge preserves failed graph search metric status across shards" {
    const alloc = std.testing.allocator;

    const graph_metric_reads = [_]graph_query_mod.GraphMetricRead{.{
        .name = "pagerank",
        .freshness = .published,
    }};
    const graph_queries = [_]db_mod.types.NamedGraphQuery{.{
        .name = "neighbors",
        .query = .{
            .query_type = .neighbors,
            .index_name = "graph_idx",
            .start_nodes = .{ .keys = &.{"doc:a"} },
            .metrics = &graph_metric_reads,
            .include_metric_status = true,
        },
    }};
    const req = db_mod.types.SearchRequest{ .graph_queries = &graph_queries };

    const left_nodes = try alloc.alloc(graph_query_mod.GraphResultNode, 1);
    left_nodes[0] = .{
        .key = try alloc.dupe(u8, "doc:b"),
        .depth = 1,
        .distance = 1,
        .path = null,
        .path_edges = null,
        .metrics = try alloc.dupe(graph_query_mod.GraphMetricValue, &.{
            .{ .name = try alloc.dupe(u8, "pagerank"), .score = 0.8 },
        }),
    };
    const left_results = try alloc.alloc(db_mod.types.GraphSearchResult, 1);
    left_results[0] = .{
        .name = try alloc.dupe(u8, "neighbors"),
        .nodes = left_nodes,
        .paths = &.{},
        .hits = &.{},
        .total_hits = 1,
        .metric_status = try alloc.dupe(db_mod.types.GraphMetricStatus, &.{
            .{
                .name = try alloc.dupe(u8, "pagerank"),
                .state = .fresh,
                .published_generation = 5,
                .edge_generation = 5,
                .target_edge_generation = 5,
                .progress = 1.0,
                .converged = true,
            },
        }),
    };
    var left = db_mod.types.SearchResult{
        .alloc = alloc,
        .hits = &.{},
        .total_hits = 0,
        .graph_results = left_results,
    };
    defer left.deinit();

    const right_nodes = try alloc.alloc(graph_query_mod.GraphResultNode, 1);
    right_nodes[0] = .{
        .key = try alloc.dupe(u8, "doc:c"),
        .depth = 1,
        .distance = 1,
        .path = null,
        .path_edges = null,
        .metrics = try alloc.dupe(graph_query_mod.GraphMetricValue, &.{
            .{ .name = try alloc.dupe(u8, "pagerank"), .score = 0.7 },
        }),
    };
    const right_results = try alloc.alloc(db_mod.types.GraphSearchResult, 1);
    right_results[0] = .{
        .name = try alloc.dupe(u8, "neighbors"),
        .nodes = right_nodes,
        .paths = &.{},
        .hits = &.{},
        .total_hits = 1,
        .metric_status = try alloc.dupe(db_mod.types.GraphMetricStatus, &.{
            .{
                .name = try alloc.dupe(u8, "pagerank"),
                .state = .failed,
                .published_generation = 5,
                .edge_generation = 6,
                .target_edge_generation = 6,
                .building_generation = 6,
                .retry_count = 2,
                .last_error = try alloc.dupe(u8, "metric rebuild failed"),
                .progress = 0.35,
                .converged = true,
            },
        }),
    };
    var right = db_mod.types.SearchResult{
        .alloc = alloc,
        .hits = &.{},
        .total_hits = 0,
        .graph_results = right_results,
    };
    defer right.deinit();

    var merged = try mergeSearchResults(alloc, req, &.{ left, right }, 0, 10);
    defer merged.deinit();
    try std.testing.expectEqual(@as(usize, 1), merged.graph_results.len);
    try std.testing.expectEqual(@as(usize, 2), merged.graph_results[0].nodes.len);
    try std.testing.expectEqual(@as(usize, 1), merged.graph_results[0].metric_status.len);
    try std.testing.expectEqual(graph_mod.GraphIndex.GraphMetricState.failed, merged.graph_results[0].metric_status[0].state);
    try std.testing.expectEqual(@as(u64, 5), merged.graph_results[0].metric_status[0].published_generation);
    try std.testing.expectEqual(@as(u64, 6), merged.graph_results[0].metric_status[0].target_edge_generation);
    try std.testing.expectEqual(@as(u64, 6), merged.graph_results[0].metric_status[0].building_generation);
    try std.testing.expectEqual(@as(u32, 2), merged.graph_results[0].metric_status[0].retry_count);
    try std.testing.expectEqualStrings("metric rebuild failed", merged.graph_results[0].metric_status[0].last_error);

    const fresh_graph_metric_reads = [_]graph_query_mod.GraphMetricRead{.{
        .name = "pagerank",
        .freshness = .fresh,
    }};
    const fresh_graph_queries = [_]db_mod.types.NamedGraphQuery{.{
        .name = "neighbors",
        .query = .{
            .query_type = .neighbors,
            .index_name = "graph_idx",
            .start_nodes = .{ .keys = &.{"doc:a"} },
            .metrics = &fresh_graph_metric_reads,
        },
    }};
    try std.testing.expectError(error.UnsupportedQueryRequest, mergeSearchResults(alloc, .{ .graph_queries = &fresh_graph_queries }, &.{ left, right }, 0, 10));
}

test "query merge enforces graph search order and filter metric generations across shards" {
    const alloc = std.testing.allocator;

    const graph_metric_orders = [_]graph_query_mod.GraphMetricOrder{.{
        .name = "pagerank",
        .freshness = .published,
    }};
    const graph_metric_filters = [_]graph_query_mod.GraphMetricFilter{.{
        .name = "pagerank",
        .op = .gte,
        .value = 0.5,
        .freshness = .published,
    }};
    const graph_queries = [_]db_mod.types.NamedGraphQuery{.{
        .name = "neighbors",
        .query = .{
            .query_type = .neighbors,
            .index_name = "graph_idx",
            .start_nodes = .{ .keys = &.{"doc:a"} },
            .order_by = &graph_metric_orders,
            .where_metric = &graph_metric_filters,
            .include_metric_status = true,
        },
    }};
    const req = db_mod.types.SearchRequest{ .graph_queries = &graph_queries };

    const left_results = try alloc.alloc(db_mod.types.GraphSearchResult, 1);
    left_results[0] = .{
        .name = try alloc.dupe(u8, "neighbors"),
        .nodes = &.{},
        .paths = &.{},
        .hits = &.{},
        .total_hits = 1,
        .metric_status = try alloc.dupe(db_mod.types.GraphMetricStatus, &.{
            .{
                .name = try alloc.dupe(u8, "pagerank"),
                .state = .fresh,
                .published_generation = 5,
                .edge_generation = 5,
                .target_edge_generation = 5,
                .progress = 1.0,
                .converged = true,
            },
        }),
    };
    var left = db_mod.types.SearchResult{
        .alloc = alloc,
        .hits = &.{},
        .total_hits = 0,
        .graph_results = left_results,
    };
    defer left.deinit();

    const right_results = try alloc.alloc(db_mod.types.GraphSearchResult, 1);
    right_results[0] = .{
        .name = try alloc.dupe(u8, "neighbors"),
        .nodes = &.{},
        .paths = &.{},
        .hits = &.{},
        .total_hits = 1,
        .metric_status = try alloc.dupe(db_mod.types.GraphMetricStatus, &.{
            .{
                .name = try alloc.dupe(u8, "pagerank"),
                .state = .stale,
                .published_generation = 5,
                .edge_generation = 6,
                .target_edge_generation = 6,
                .building_generation = 6,
                .progress = 0.25,
                .converged = true,
            },
        }),
    };
    var right = db_mod.types.SearchResult{
        .alloc = alloc,
        .hits = &.{},
        .total_hits = 0,
        .graph_results = right_results,
    };
    defer right.deinit();

    var merged = try mergeSearchResults(alloc, req, &.{ left, right }, 0, 10);
    defer merged.deinit();
    try std.testing.expectEqual(@as(usize, 1), merged.graph_results.len);
    try std.testing.expectEqual(@as(usize, 1), merged.graph_results[0].metric_status.len);
    try std.testing.expectEqual(graph_mod.GraphIndex.GraphMetricState.stale, merged.graph_results[0].metric_status[0].state);
    try std.testing.expectEqual(@as(u64, 5), merged.graph_results[0].metric_status[0].published_generation);
    try std.testing.expectEqual(@as(u64, 6), merged.graph_results[0].metric_status[0].building_generation);

    const no_status_graph_queries = [_]db_mod.types.NamedGraphQuery{.{
        .name = "neighbors",
        .query = .{
            .query_type = .neighbors,
            .index_name = "graph_idx",
            .start_nodes = .{ .keys = &.{"doc:a"} },
            .order_by = &graph_metric_orders,
            .where_metric = &graph_metric_filters,
        },
    }};
    var no_status_merged = try mergeSearchResults(alloc, .{ .graph_queries = &no_status_graph_queries }, &.{ left, right }, 0, 10);
    defer no_status_merged.deinit();
    try std.testing.expectEqual(@as(usize, 1), no_status_merged.graph_results.len);
    try std.testing.expectEqual(@as(usize, 0), no_status_merged.graph_results[0].metric_status.len);

    right_results[0].metric_status[0].state = .failed;
    right_results[0].metric_status[0].retry_count = 2;
    right_results[0].metric_status[0].last_error = try alloc.dupe(u8, "order/filter metric rebuild failed");

    var failed_merged = try mergeSearchResults(alloc, req, &.{ left, right }, 0, 10);
    defer failed_merged.deinit();
    try std.testing.expectEqual(@as(usize, 1), failed_merged.graph_results.len);
    try std.testing.expectEqual(@as(usize, 1), failed_merged.graph_results[0].metric_status.len);
    try std.testing.expectEqual(graph_mod.GraphIndex.GraphMetricState.failed, failed_merged.graph_results[0].metric_status[0].state);
    try std.testing.expectEqual(@as(u64, 5), failed_merged.graph_results[0].metric_status[0].published_generation);
    try std.testing.expectEqual(@as(u64, 6), failed_merged.graph_results[0].metric_status[0].building_generation);
    try std.testing.expectEqual(@as(u32, 2), failed_merged.graph_results[0].metric_status[0].retry_count);
    try std.testing.expectEqualStrings("order/filter metric rebuild failed", failed_merged.graph_results[0].metric_status[0].last_error);

    const fresh_graph_metric_orders = [_]graph_query_mod.GraphMetricOrder{.{
        .name = "pagerank",
        .freshness = .fresh,
    }};
    const fresh_graph_metric_filters = [_]graph_query_mod.GraphMetricFilter{.{
        .name = "pagerank",
        .op = .gte,
        .value = 0.5,
        .freshness = .fresh,
    }};
    const fresh_graph_queries = [_]db_mod.types.NamedGraphQuery{.{
        .name = "neighbors",
        .query = .{
            .query_type = .neighbors,
            .index_name = "graph_idx",
            .start_nodes = .{ .keys = &.{"doc:a"} },
            .order_by = &fresh_graph_metric_orders,
            .where_metric = &fresh_graph_metric_filters,
        },
    }};
    try std.testing.expectError(error.UnsupportedQueryRequest, mergeSearchResults(alloc, .{ .graph_queries = &fresh_graph_queries }, &.{ left, right }, 0, 10));

    right_results[0].metric_status[0].published_generation = 4;
    try std.testing.expectError(error.UnsupportedQueryRequest, mergeSearchResults(alloc, req, &.{ left, right }, 0, 10));

    right_results[0].metric_status[0].published_generation = 0;
    try std.testing.expectError(error.UnsupportedQueryRequest, mergeSearchResults(alloc, req, &.{ left, right }, 0, 10));
}

test "query profile reports merged graph search metric generation" {
    const alloc = std.testing.allocator;

    const graph_metric_reads = [_]graph_query_mod.GraphMetricRead{.{
        .name = "pagerank",
        .freshness = .published,
    }};
    const graph_queries = [_]db_mod.types.NamedGraphQuery{.{
        .name = "neighbors",
        .query = .{
            .query_type = .neighbors,
            .index_name = "graph_idx",
            .start_nodes = .{ .keys = &.{"doc:a"} },
            .metrics = &graph_metric_reads,
        },
    }};

    const graph_results = try alloc.alloc(db_mod.types.GraphSearchResult, 1);
    graph_results[0] = .{
        .name = try alloc.dupe(u8, "neighbors"),
        .nodes = &.{},
        .paths = &.{},
        .hits = &.{},
        .total_hits = 2,
        .metric_status = try alloc.dupe(db_mod.types.GraphMetricStatus, &.{
            .{
                .name = try alloc.dupe(u8, "pagerank"),
                .state = .stale,
                .published_generation = 5,
                .edge_generation = 6,
                .target_edge_generation = 6,
                .building_generation = 6,
                .progress = 0.5,
                .converged = true,
            },
        }),
    };
    var result = db_mod.types.SearchResult{
        .alloc = alloc,
        .hits = &.{},
        .total_hits = 0,
        .graph_results = graph_results,
    };
    defer result.deinit();

    var encoded = try encodeQueryResponses(alloc, "docs", .{
        .profile = true,
        .graph_queries = &graph_queries,
    }, .{ .took_ms = 2 }, result);
    defer encoded.deinit(alloc);

    try ant_json.testing.expectSubsetJsonText(alloc,
        \\{"responses":[{"profile":{"graph_metrics":[{"query_name":"neighbors","source":"graph_query","index_name":"graph_idx","metric_name":"pagerank","freshness":"published","status":{"state":"stale","published_generation":5,"building_generation":6}}]}}]}
    , encoded.json);
}

test "query merge requires comparable graph metric rerank generations across shards" {
    const alloc = std.testing.allocator;

    var left_hits = try alloc.alloc(db_mod.types.SearchHit, 1);
    left_hits[0] = .{
        .id = try alloc.dupe(u8, "doc:a"),
        .score = 3.0,
    };
    var left = db_mod.types.SearchResult{
        .alloc = alloc,
        .hits = left_hits,
        .total_hits = 1,
        .graph_metric_rerank_status = .{
            .name = try alloc.dupe(u8, "pagerank"),
            .state = .fresh,
            .published_generation = 8,
            .edge_generation = 8,
            .target_edge_generation = 8,
            .progress = 1.0,
            .converged = true,
        },
    };
    defer left.deinit();

    var right_hits = try alloc.alloc(db_mod.types.SearchHit, 1);
    right_hits[0] = .{
        .id = try alloc.dupe(u8, "doc:b"),
        .score = 2.0,
    };
    var right = db_mod.types.SearchResult{
        .alloc = alloc,
        .hits = right_hits,
        .total_hits = 1,
        .graph_metric_rerank_status = .{
            .name = try alloc.dupe(u8, "pagerank"),
            .state = .stale,
            .published_generation = 8,
            .edge_generation = 9,
            .target_edge_generation = 9,
            .progress = 1.0,
            .converged = true,
        },
    };
    defer right.deinit();

    const req = db_mod.types.SearchRequest{
        .graph_metric_rerank = .{
            .index_name = "graph_idx",
            .metric_name = "pagerank",
            .freshness = .published,
            .weight = 2.0,
        },
    };

    var merged = try mergeSearchResults(alloc, req, &.{ left, right }, 0, 10);
    defer merged.deinit();
    try std.testing.expect(merged.graph_metric_rerank_status != null);
    try std.testing.expectEqual(@as(u64, 8), merged.graph_metric_rerank_status.?.published_generation);
    try std.testing.expectEqual(graph_mod.GraphIndex.GraphMetricState.stale, merged.graph_metric_rerank_status.?.state);
    try std.testing.expectEqualStrings("doc:a", merged.hits[0].id);

    const old_right_error = right.graph_metric_rerank_status.?.last_error;
    right.graph_metric_rerank_status.?.state = .failed;
    right.graph_metric_rerank_status.?.target_edge_generation = 9;
    right.graph_metric_rerank_status.?.building_generation = 9;
    right.graph_metric_rerank_status.?.retry_count = 2;
    right.graph_metric_rerank_status.?.last_error = try alloc.dupe(u8, "metric rebuild failed");
    if (old_right_error.len > 0) alloc.free(old_right_error);

    var failed_merged = try mergeSearchResults(alloc, req, &.{ left, right }, 0, 10);
    defer failed_merged.deinit();
    try std.testing.expect(failed_merged.graph_metric_rerank_status != null);
    try std.testing.expectEqual(graph_mod.GraphIndex.GraphMetricState.failed, failed_merged.graph_metric_rerank_status.?.state);
    try std.testing.expectEqual(@as(u64, 8), failed_merged.graph_metric_rerank_status.?.published_generation);
    try std.testing.expectEqual(@as(u64, 9), failed_merged.graph_metric_rerank_status.?.building_generation);
    try std.testing.expectEqual(@as(u32, 2), failed_merged.graph_metric_rerank_status.?.retry_count);
    try std.testing.expectEqualStrings("metric rebuild failed", failed_merged.graph_metric_rerank_status.?.last_error);

    var encoded_failed = try encodeQueryResponses(alloc, "docs", .{
        .profile = true,
        .graph_metric_rerank = .{
            .index_name = "graph_idx",
            .metric_name = "pagerank",
            .freshness = .published,
            .weight = 1.0,
        },
    }, .{ .took_ms = 5, .shard_count = 2, .merged = true }, failed_merged);
    defer encoded_failed.deinit(alloc);
    try ant_json.testing.expectSubsetJsonText(alloc,
        \\{"responses":[{"profile":{"shards":{"total":2,"successful":2,"failed":0},"graph_metrics":[{"query_name":"graph_metric_rerank","source":"graph_metric_rerank","index_name":"graph_idx","metric_name":"pagerank","freshness":"published","status":{"state":"failed","published_generation":8,"building_generation":9,"retry_count":2,"last_error":"metric rebuild failed"}}]}}]}
    , encoded_failed.json);

    const fresh_req = db_mod.types.SearchRequest{
        .graph_metric_rerank = .{
            .index_name = "graph_idx",
            .metric_name = "pagerank",
            .freshness = .fresh,
            .weight = 1.0,
        },
    };
    try std.testing.expectError(error.UnsupportedQueryRequest, mergeSearchResults(alloc, fresh_req, &.{ left, right }, 0, 10));

    right.graph_metric_rerank_status.?.published_generation = 7;
    try std.testing.expectError(error.UnsupportedQueryRequest, mergeSearchResults(alloc, req, &.{ left, right }, 0, 10));
}

test "query merge rejects malformed graph metric rerank score details" {
    const alloc = std.testing.allocator;

    const Make = struct {
        fn result(
            allocator: std.mem.Allocator,
            details_index_name: []const u8,
            details_metric_name: []const u8,
            details_generation: u64,
            details_final_score: f64,
        ) !db_mod.types.SearchResult {
            var hits = try allocator.alloc(db_mod.types.SearchHit, 1);
            hits[0] = .{
                .id = try allocator.dupe(u8, "doc:a"),
                .score = 2.0,
                .score_details = .{
                    .index_name = try allocator.dupe(u8, details_index_name),
                    .metric_name = try allocator.dupe(u8, details_metric_name),
                    .base_score = 1.0,
                    .base_weight = 1.0,
                    .metric_score = 0.5,
                    .metric_score_used = 0.5,
                    .metric_weight = 2.0,
                    .final_score = details_final_score,
                    .published_generation = details_generation,
                },
            };
            return .{
                .alloc = allocator,
                .hits = hits,
                .total_hits = 1,
                .graph_metric_rerank_status = .{
                    .name = try allocator.dupe(u8, "pagerank"),
                    .state = .fresh,
                    .published_generation = 8,
                    .edge_generation = 8,
                    .target_edge_generation = 8,
                    .progress = 1.0,
                    .converged = true,
                },
            };
        }
    };

    var unsolicited_hits = try alloc.alloc(db_mod.types.SearchHit, 1);
    unsolicited_hits[0] = .{
        .id = try alloc.dupe(u8, "doc:a"),
        .score = 2.0,
        .score_details = .{
            .index_name = try alloc.dupe(u8, "graph_idx"),
            .metric_name = try alloc.dupe(u8, "pagerank"),
            .base_score = 1.0,
            .base_weight = 1.0,
            .metric_score = 0.5,
            .metric_score_used = 0.5,
            .metric_weight = 2.0,
            .final_score = 2.0,
            .published_generation = 8,
        },
    };
    var unsolicited = db_mod.types.SearchResult{
        .alloc = alloc,
        .hits = unsolicited_hits,
        .total_hits = 1,
    };
    defer unsolicited.deinit();
    try std.testing.expectError(error.UnsupportedQueryRequest, mergeSearchResults(alloc, .{}, &.{unsolicited}, 0, 10));

    const req = db_mod.types.SearchRequest{
        .graph_metric_rerank = .{
            .index_name = "graph_idx",
            .metric_name = "pagerank",
            .freshness = .published,
            .weight = 1.0,
        },
    };

    var wrong_generation = try Make.result(alloc, "graph_idx", "pagerank", 7, 2.0);
    defer wrong_generation.deinit();
    try std.testing.expectError(error.UnsupportedQueryRequest, mergeSearchResults(alloc, req, &.{wrong_generation}, 0, 10));

    var wrong_identity = try Make.result(alloc, "other_idx", "pagerank", 8, 2.0);
    defer wrong_identity.deinit();
    try std.testing.expectError(error.UnsupportedQueryRequest, mergeSearchResults(alloc, req, &.{wrong_identity}, 0, 10));

    var mismatched_final_score = try Make.result(alloc, "graph_idx", "pagerank", 8, 1.5);
    defer mismatched_final_score.deinit();
    try std.testing.expectError(error.UnsupportedQueryRequest, mergeSearchResults(alloc, req, &.{mismatched_final_score}, 0, 10));

    var mismatched_weight = try Make.result(alloc, "graph_idx", "pagerank", 8, 2.0);
    defer mismatched_weight.deinit();
    mismatched_weight.hits[0].score_details.?.metric_weight = 1.0;
    try std.testing.expectError(error.UnsupportedQueryRequest, mergeSearchResults(alloc, req, &.{mismatched_weight}, 0, 10));

    var mismatched_metric_score_used = try Make.result(alloc, "graph_idx", "pagerank", 8, 2.0);
    defer mismatched_metric_score_used.deinit();
    mismatched_metric_score_used.hits[0].score_details.?.metric_score_used = 0.25;
    try std.testing.expectError(error.UnsupportedQueryRequest, mergeSearchResults(alloc, req, &.{mismatched_metric_score_used}, 0, 10));

    var mismatched_missing_score = try Make.result(alloc, "graph_idx", "pagerank", 8, 2.0);
    defer mismatched_missing_score.deinit();
    mismatched_missing_score.hits[0].score_details.?.metric_score = null;
    mismatched_missing_score.hits[0].score_details.?.missing_score_used = true;
    try std.testing.expectError(error.UnsupportedQueryRequest, mergeSearchResults(alloc, req, &.{mismatched_missing_score}, 0, 10));

    var mismatched_formula = try Make.result(alloc, "graph_idx", "pagerank", 8, 2.0);
    defer mismatched_formula.deinit();
    mismatched_formula.hits[0].score_details.?.base_score = 0.5;
    try std.testing.expectError(error.UnsupportedQueryRequest, mergeSearchResults(alloc, req, &.{mismatched_formula}, 0, 10));

    var non_finite = try Make.result(alloc, "graph_idx", "pagerank", 8, std.math.nan(f64));
    defer non_finite.deinit();
    try std.testing.expectError(error.UnsupportedQueryRequest, mergeSearchResults(alloc, req, &.{non_finite}, 0, 10));

    var non_finite_hits = try alloc.alloc(db_mod.types.SearchHit, 1);
    non_finite_hits[0] = .{
        .id = try alloc.dupe(u8, "doc:a"),
        .score = std.math.inf(f32),
    };
    var non_finite_without_details = db_mod.types.SearchResult{
        .alloc = alloc,
        .hits = non_finite_hits,
        .total_hits = 1,
        .graph_metric_rerank_status = .{
            .name = try alloc.dupe(u8, "pagerank"),
            .state = .fresh,
            .published_generation = 8,
            .edge_generation = 8,
            .target_edge_generation = 8,
            .progress = 1.0,
            .converged = true,
        },
    };
    defer non_finite_without_details.deinit();
    try std.testing.expectError(error.UnsupportedQueryRequest, mergeSearchResults(alloc, req, &.{non_finite_without_details}, 0, 10));
}

test "query merge rejects missing or unpublished graph metric rerank shard status" {
    const alloc = std.testing.allocator;

    var left_hits = try alloc.alloc(db_mod.types.SearchHit, 1);
    left_hits[0] = .{
        .id = try alloc.dupe(u8, "doc:a"),
        .score = 3.0,
    };
    var left = db_mod.types.SearchResult{
        .alloc = alloc,
        .hits = left_hits,
        .total_hits = 1,
        .graph_metric_rerank_status = .{
            .name = try alloc.dupe(u8, "pagerank"),
            .state = .fresh,
            .published_generation = 8,
            .edge_generation = 8,
            .target_edge_generation = 8,
            .progress = 1.0,
            .converged = true,
        },
    };
    defer left.deinit();

    const req = db_mod.types.SearchRequest{
        .graph_metric_rerank = .{
            .index_name = "graph_idx",
            .metric_name = "pagerank",
            .freshness = .published,
            .weight = 1.0,
        },
    };

    var missing = db_mod.types.SearchResult{
        .alloc = alloc,
        .hits = &.{},
        .total_hits = 0,
    };
    defer missing.deinit();
    try std.testing.expectError(error.UnsupportedQueryRequest, mergeSearchResults(alloc, req, &.{ left, missing }, 0, 10));

    var unpublished = db_mod.types.SearchResult{
        .alloc = alloc,
        .hits = &.{},
        .total_hits = 0,
        .graph_metric_rerank_status = .{
            .name = try alloc.dupe(u8, "pagerank"),
            .state = .not_ready,
            .published_generation = 0,
            .edge_generation = 8,
            .target_edge_generation = 8,
        },
    };
    defer unpublished.deinit();
    try std.testing.expectError(error.UnsupportedQueryRequest, mergeSearchResults(alloc, req, &.{ left, unpublished }, 0, 10));
}

test "query parser accepts direct graph metric reads" {
    var owned = try parseQueryRequest(std.testing.allocator, null, "docs",
        \\{"graph_metric":{"name":"central","index":"graph_idx","metric":"pagerank","top_k":25,"metric_freshness":"fresh"}}
    );
    defer owned.deinit(std.testing.allocator);

    try std.testing.expectEqual(@as(usize, 1), owned.req.graph_metric_queries.len);
    try std.testing.expectEqualStrings("central", owned.req.graph_metric_queries[0].name);
    try std.testing.expectEqualStrings("graph_idx", owned.req.graph_metric_queries[0].query.index_name);
    try std.testing.expectEqualStrings("pagerank", owned.req.graph_metric_queries[0].query.metric_name);
    try std.testing.expectEqual(@as(u32, 25), owned.req.graph_metric_queries[0].query.top_k);
    try std.testing.expectEqual(db_mod.types.GraphMetricFreshness.fresh, owned.req.graph_metric_queries[0].query.freshness);
    try std.testing.expectError(error.InvalidQueryRequest, parseQueryRequest(std.testing.allocator, null, "docs",
        \\{"graph_metric":{"index":"graph_idx","metric":"pagerank","top_k":0}}
    ));
}

test "query parser accepts graph metric rerank" {
    var owned = try parseQueryRequest(std.testing.allocator, null, "docs",
        \\{"full_text_search":{"match_all":{}},"graph_metric_rerank":{"index":"graph_idx","metric":"pagerank","base_weight":0.5,"weight":2.5,"missing_score":-0.25,"metric_freshness":"fresh"},"limit":10}
    );
    defer owned.deinit(std.testing.allocator);

    try std.testing.expect(owned.req.graph_metric_rerank != null);
    try std.testing.expectEqualStrings("graph_idx", owned.req.graph_metric_rerank.?.index_name);
    try std.testing.expectEqualStrings("pagerank", owned.req.graph_metric_rerank.?.metric_name);
    try std.testing.expectEqual(db_mod.types.GraphMetricFreshness.fresh, owned.req.graph_metric_rerank.?.freshness);
    try std.testing.expectApproxEqAbs(@as(f64, 0.5), owned.req.graph_metric_rerank.?.base_weight, 0.000001);
    try std.testing.expectApproxEqAbs(@as(f64, 2.5), owned.req.graph_metric_rerank.?.weight, 0.000001);
    try std.testing.expectApproxEqAbs(@as(f64, -0.25), owned.req.graph_metric_rerank.?.missing_score, 0.000001);
}

test "query encoder emits graph metric results" {
    const alloc = std.testing.allocator;
    var scores = try alloc.alloc(db_mod.types.GraphMetricScore, 1);
    scores[0] = .{
        .node = try alloc.dupe(u8, "doc:b"),
        .score = 0.75,
    };
    var metric_results = try alloc.alloc(db_mod.types.GraphMetricResult, 1);
    metric_results[0] = .{
        .name = try alloc.dupe(u8, "pagerank"),
        .index_name = try alloc.dupe(u8, "graph_idx"),
        .metric_name = try alloc.dupe(u8, "pagerank"),
        .scores = scores,
        .status = .{
            .name = try alloc.dupe(u8, "pagerank"),
            .state = .fresh,
            .published_generation = 3,
            .edge_generation = 3,
            .converged = true,
            .iterations_completed = 12,
            .delta = 0.00001,
            .computed_at_ms = 1780000000000,
        },
    };
    var result = db_mod.types.SearchResult{
        .alloc = alloc,
        .hits = &.{},
        .total_hits = 0,
        .graph_metric_results = metric_results,
    };
    defer result.deinit();

    var encoded = try encodeQueryResponses(alloc, "docs", .{}, .{}, result);
    defer encoded.deinit(alloc);
    try ant_json.testing.expectSubsetJsonText(alloc,
        \\{"responses":[{"graph_metric_results":{"pagerank":{"metric":"pagerank","status":{"state":"fresh"},"scores":[{"node":"doc:b","score":0.75}]}}}]}
    , encoded.json);
}

test "query profile reports failed graph metric status across read surfaces" {
    const alloc = std.testing.allocator;

    const graph_metric_results = try alloc.alloc(db_mod.types.GraphMetricResult, 1);
    graph_metric_results[0] = .{
        .name = try alloc.dupe(u8, "central"),
        .index_name = try alloc.dupe(u8, "graph_idx"),
        .metric_name = try alloc.dupe(u8, "pagerank"),
        .scores = &.{},
        .status = .{
            .name = try alloc.dupe(u8, "pagerank"),
            .state = .failed,
            .published_generation = 5,
            .edge_generation = 6,
            .target_edge_generation = 6,
            .building_generation = 6,
            .retry_count = 2,
            .last_error = try alloc.dupe(u8, "direct metric rebuild failed"),
            .progress = 0.4,
            .converged = true,
        },
    };

    const graph_results = try alloc.alloc(db_mod.types.GraphSearchResult, 1);
    graph_results[0] = .{
        .name = try alloc.dupe(u8, "neighbors"),
        .nodes = &.{},
        .paths = &.{},
        .hits = &.{},
        .total_hits = 0,
        .metric_status = try alloc.dupe(db_mod.types.GraphMetricStatus, &.{
            .{
                .name = try alloc.dupe(u8, "pagerank"),
                .state = .failed,
                .published_generation = 5,
                .edge_generation = 6,
                .target_edge_generation = 6,
                .building_generation = 6,
                .retry_count = 3,
                .last_error = try alloc.dupe(u8, "graph query metric rebuild failed"),
                .progress = 0.3,
                .converged = true,
            },
        }),
    };

    var result = db_mod.types.SearchResult{
        .alloc = alloc,
        .hits = &.{},
        .total_hits = 0,
        .graph_metric_results = graph_metric_results,
        .graph_results = graph_results,
        .graph_metric_rerank_status = .{
            .name = try alloc.dupe(u8, "pagerank"),
            .state = .failed,
            .published_generation = 5,
            .edge_generation = 6,
            .target_edge_generation = 6,
            .building_generation = 6,
            .retry_count = 4,
            .last_error = try alloc.dupe(u8, "rerank metric rebuild failed"),
            .progress = 0.2,
            .converged = true,
        },
    };
    defer result.deinit();

    const graph_metric_queries = [_]db_mod.types.NamedGraphMetricQuery{.{
        .name = "central",
        .query = .{
            .index_name = "graph_idx",
            .metric_name = "pagerank",
            .freshness = .published,
        },
    }};
    const graph_metric_reads = [_]graph_query_mod.GraphMetricRead{.{
        .name = "pagerank",
        .freshness = .published,
    }};
    const graph_queries = [_]db_mod.types.NamedGraphQuery{.{
        .name = "neighbors",
        .query = .{
            .query_type = .neighbors,
            .index_name = "graph_idx",
            .start_nodes = .{ .keys = &.{"doc:a"} },
            .metrics = &graph_metric_reads,
            .include_metric_status = true,
        },
    }};

    var encoded = try encodeQueryResponses(alloc, "docs", .{
        .profile = true,
        .graph_metric_queries = &graph_metric_queries,
        .graph_queries = &graph_queries,
        .graph_metric_rerank = .{
            .index_name = "graph_idx",
            .metric_name = "pagerank",
            .freshness = .published,
            .weight = 1.0,
        },
    }, .{ .took_ms = 2 }, result);
    defer encoded.deinit(alloc);

    try ant_json.testing.expectSubsetJsonText(alloc,
        \\{"responses":[{"profile":{"graph_metrics":[{"source":"graph_metric","freshness":"published","status":{"state":"failed","published_generation":5,"building_generation":6,"retry_count":2,"last_error":"direct metric rebuild failed"}},{"source":"graph_query","freshness":"published","status":{"state":"failed","published_generation":5,"building_generation":6,"retry_count":3,"last_error":"graph query metric rebuild failed"}},{"source":"graph_metric_rerank","freshness":"published","status":{"state":"failed","published_generation":5,"building_generation":6,"retry_count":4,"last_error":"rerank metric rebuild failed"}}]}}]}
    , encoded.json);
}

test "query encoder emits graph metric rerank score details" {
    const alloc = std.testing.allocator;
    var hits = try alloc.alloc(db_mod.types.SearchHit, 1);
    hits[0] = .{
        .id = try alloc.dupe(u8, "doc:a"),
        .score = 4.0,
        .score_details = .{
            .index_name = try alloc.dupe(u8, "graph_idx"),
            .metric_name = try alloc.dupe(u8, "pagerank"),
            .base_score = 1.0,
            .base_weight = 0.5,
            .metric_score = 0.7,
            .metric_score_used = 0.7,
            .metric_weight = 5.0,
            .missing_score_used = false,
            .final_score = 4.0,
            .published_generation = 11,
        },
    };
    var result = db_mod.types.SearchResult{
        .alloc = alloc,
        .hits = hits,
        .total_hits = 1,
    };
    defer result.deinit();

    var encoded = try encodeQueryResponses(alloc, "docs", .{}, .{ .took_ms = 2 }, result);
    defer encoded.deinit(alloc);

    try ant_json.testing.expectSubsetJsonText(alloc,
        \\{"responses":[{"hits":{"hits":[{"_id":"doc:a","_score_details":{"graph_metric_rerank":{"index_name":"graph_idx","metric_name":"pagerank","base_score":1,"base_weight":0.5,"metric_score":0.7,"metric_score_used":0.7,"metric_weight":5,"missing_score_used":false,"final_score":4,"published_generation":11}}}]}}]}
    , encoded.json);
}

test "query merge rejects explicit score sort without score-bearing source" {
    const alloc = std.testing.allocator;
    const score_order = [_]db_mod.types.SortField{.{ .field = "_score", .desc = true }};

    var hits = try alloc.alloc(db_mod.types.SearchHit, 2);
    hits[0] = try testScoreSortedQueryHitAlloc(alloc, "doc:a", 1.0);
    hits[1] = try testScoreSortedQueryHitAlloc(alloc, "doc:b", 2.0);
    var result = db_mod.types.SearchResult{ .alloc = alloc, .hits = hits, .total_hits = 2 };
    defer result.deinit();

    try std.testing.expectError(error.UnsupportedQueryRequest, mergeSearchResults(alloc, .{
        .order_by = &score_order,
        .full_text = .{ .match_all = {} },
    }, &.{result}, 0, 2));

    var page = try mergeSearchResults(alloc, .{
        .order_by = &score_order,
        .full_text = .{ .match = .{ .field = "body", .text = "alpha" } },
    }, &.{result}, 0, 2);
    defer page.deinit();
    try std.testing.expectEqual(@as(usize, 2), page.hits.len);
    try std.testing.expectEqualStrings("doc:b", page.hits[0].id);
    try std.testing.expectEqualStrings("doc:a", page.hits[1].id);
}

test "query merge applies distributed typed sort ordering and cursor paging" {
    const alloc = std.testing.allocator;
    const order_by = [_]db_mod.types.SortField{
        .{ .field = "rank" },
        .{ .field = "_id" },
    };
    const schema = testRankRuntimeSchema();

    var left_hits = try alloc.alloc(db_mod.types.SearchHit, 3);
    left_hits[0] = try testSortedQueryHitAlloc(alloc, "doc:a", 1);
    left_hits[1] = try testSortedQueryHitAlloc(alloc, "doc:c", 3);
    left_hits[2] = try testSortedQueryHitAlloc(alloc, "doc:e", 5);
    var right_hits = try alloc.alloc(db_mod.types.SearchHit, 4);
    right_hits[0] = try testSortedQueryHitAlloc(alloc, "doc:b", 2);
    right_hits[1] = try testSortedQueryHitAlloc(alloc, "doc:d", 4);
    right_hits[2] = try testSortedQueryHitAlloc(alloc, "doc:f", 6);
    right_hits[3] = try testSortedQueryHitAlloc(alloc, "doc:h", 8);

    var left = db_mod.types.SearchResult{ .alloc = alloc, .hits = left_hits, .total_hits = 3 };
    defer left.deinit();
    var right = db_mod.types.SearchResult{ .alloc = alloc, .hits = right_hits, .total_hits = 4, .total_hits_relation = .gte };
    defer right.deinit();

    var first_page = try mergeSearchResultsWithRuntimeSchema(alloc, .{ .order_by = &order_by }, &.{ left, right }, 1, 3, schema);
    defer first_page.deinit();
    try std.testing.expectEqual(@as(u32, 7), first_page.total_hits);
    try std.testing.expectEqual(db_mod.types.TotalHitsRelation.gte, first_page.total_hits_relation);
    try std.testing.expectEqual(@as(usize, 3), first_page.hits.len);
    try std.testing.expectEqualStrings("doc:b", first_page.hits[0].id);
    try std.testing.expectEqualStrings("doc:c", first_page.hits[1].id);
    try std.testing.expectEqualStrings("doc:d", first_page.hits[2].id);
    try std.testing.expectEqual(@as(?u32, null), first_page.hits[0].doc_ordinal);

    const after_cursor = [_]std.json.Value{
        .{ .integer = 2 },
        .{ .string = "doc:b" },
    };
    var after_left_hits = try alloc.alloc(db_mod.types.SearchHit, 2);
    after_left_hits[0] = try testSortedQueryHitAlloc(alloc, "doc:c", 3);
    after_left_hits[1] = try testSortedQueryHitAlloc(alloc, "doc:e", 5);
    var after_right_hits = try alloc.alloc(db_mod.types.SearchHit, 2);
    after_right_hits[0] = try testSortedQueryHitAlloc(alloc, "doc:d", 4);
    after_right_hits[1] = try testSortedQueryHitAlloc(alloc, "doc:f", 6);
    var after_left = db_mod.types.SearchResult{ .alloc = alloc, .hits = after_left_hits, .total_hits = 2 };
    defer after_left.deinit();
    var after_right = db_mod.types.SearchResult{ .alloc = alloc, .hits = after_right_hits, .total_hits = 2 };
    defer after_right.deinit();
    var after_page = try mergeSearchResultsWithRuntimeSchema(alloc, .{
        .order_by = &order_by,
        .search_after = &after_cursor,
        .profile = true,
    }, &.{ after_left, after_right }, 0, 2, schema);
    defer after_page.deinit();
    try std.testing.expectEqual(@as(usize, 2), after_page.hits.len);
    try std.testing.expectEqualStrings("doc:c", after_page.hits[0].id);
    try std.testing.expectEqualStrings("doc:d", after_page.hits[1].id);
    const sort_profile = after_page.sort_profile orelse return error.TestUnexpectedResult;
    try std.testing.expectEqualStrings("distributed_k_way_merge", sort_profile.plan);
    try std.testing.expectEqualStrings("bounded_exact", sort_profile.exactness);
    try std.testing.expectEqualStrings("distributed_merge", sort_profile.source);
    try std.testing.expectEqualStrings("coordinator_merge", sort_profile.distributed_behavior);
    try std.testing.expectEqual(@as(usize, 2), sort_profile.distributed_shard_count);
    try std.testing.expectEqual(@as(usize, 2), sort_profile.distributed_shard_window);

    const before_cursor = [_]std.json.Value{
        .{ .integer = 5 },
        .{ .string = "doc:e" },
    };
    var before_left_hits = try alloc.alloc(db_mod.types.SearchHit, 2);
    before_left_hits[0] = try testSortedQueryHitAlloc(alloc, "doc:a", 1);
    before_left_hits[1] = try testSortedQueryHitAlloc(alloc, "doc:c", 3);
    var before_right_hits = try alloc.alloc(db_mod.types.SearchHit, 2);
    before_right_hits[0] = try testSortedQueryHitAlloc(alloc, "doc:b", 2);
    before_right_hits[1] = try testSortedQueryHitAlloc(alloc, "doc:d", 4);
    var before_left = db_mod.types.SearchResult{ .alloc = alloc, .hits = before_left_hits, .total_hits = 2 };
    defer before_left.deinit();
    var before_right = db_mod.types.SearchResult{ .alloc = alloc, .hits = before_right_hits, .total_hits = 2 };
    defer before_right.deinit();
    var before_page = try mergeSearchResultsWithRuntimeSchema(alloc, .{
        .order_by = &order_by,
        .search_before = &before_cursor,
    }, &.{ before_left, before_right }, 0, 2, schema);
    defer before_page.deinit();
    try std.testing.expectEqual(@as(usize, 2), before_page.hits.len);
    try std.testing.expectEqualStrings("doc:c", before_page.hits[0].id);
    try std.testing.expectEqualStrings("doc:d", before_page.hits[1].id);
}

test "query merge applies default id cursor ordering without explicit order_by" {
    const alloc = std.testing.allocator;

    var left_hits = try alloc.alloc(db_mod.types.SearchHit, 2);
    left_hits[0] = try testIdSortedQueryHitAlloc(alloc, "doc:a");
    left_hits[1] = try testIdSortedQueryHitAlloc(alloc, "doc:c");
    var right_hits = try alloc.alloc(db_mod.types.SearchHit, 2);
    right_hits[0] = try testIdSortedQueryHitAlloc(alloc, "doc:b");
    right_hits[1] = try testIdSortedQueryHitAlloc(alloc, "doc:d");

    var left = db_mod.types.SearchResult{ .alloc = alloc, .hits = left_hits, .total_hits = 2 };
    defer left.deinit();
    var right = db_mod.types.SearchResult{ .alloc = alloc, .hits = right_hits, .total_hits = 2 };
    defer right.deinit();

    const after_cursor = [_]std.json.Value{.{ .string = "doc:b" }};
    var after_page = try mergeSearchResults(alloc, .{
        .search_after = &after_cursor,
        .limit = 2,
        .profile = true,
    }, &.{ left, right }, 0, 2);
    defer after_page.deinit();

    try std.testing.expectEqual(@as(usize, 2), after_page.hits.len);
    try std.testing.expectEqualStrings("doc:c", after_page.hits[0].id);
    try std.testing.expectEqualStrings("doc:d", after_page.hits[1].id);
    const after_profile = after_page.sort_profile orelse return error.TestUnexpectedResult;
    try std.testing.expectEqualStrings("distributed_k_way_merge", after_profile.plan);
    try std.testing.expectEqualStrings("bounded_exact", after_profile.exactness);
    try std.testing.expectEqualStrings("distributed_merge", after_profile.source);

    const before_cursor = [_]std.json.Value{.{ .string = "doc:d" }};
    var before_page = try mergeSearchResults(alloc, .{
        .search_before = &before_cursor,
        .limit = 2,
    }, &.{ left, right }, 0, 2);
    defer before_page.deinit();

    try std.testing.expectEqual(@as(usize, 2), before_page.hits.len);
    try std.testing.expectEqualStrings("doc:b", before_page.hits[0].id);
    try std.testing.expectEqualStrings("doc:c", before_page.hits[1].id);
}

test "query merge sort profile does not inherit stale rejection diagnostic" {
    const alloc = std.testing.allocator;
    const order_by = [_]db_mod.types.SortField{
        .{ .field = "rank" },
        .{ .field = "_id" },
    };
    const schema = testRankRuntimeSchema();

    var left_hits = try alloc.alloc(db_mod.types.SearchHit, 1);
    left_hits[0] = try testSortedQueryHitAlloc(alloc, "doc:a", 1);
    var right_hits = try alloc.alloc(db_mod.types.SearchHit, 1);
    right_hits[0] = try testSortedQueryHitAlloc(alloc, "doc:b", 2);

    var left = db_mod.types.SearchResult{ .alloc = alloc, .hits = left_hits, .total_hits = 1 };
    defer left.deinit();
    var right = db_mod.types.SearchResult{ .alloc = alloc, .hits = right_hits, .total_hits = 1 };
    defer right.deinit();

    db_mod.recordSortRejectionDiagnostic("stale_field", "stale_reason", "stale_detail");
    var merged = try mergeSearchResultsWithRuntimeSchema(alloc, .{
        .order_by = &order_by,
        .profile = true,
    }, &.{ left, right }, 0, 2, schema);
    defer merged.deinit();

    const sort_profile = merged.sort_profile orelse return error.TestUnexpectedResult;
    try std.testing.expectEqualStrings("distributed_k_way_merge", sort_profile.plan);
    try std.testing.expectEqualStrings("bounded_exact", sort_profile.exactness);
    try std.testing.expectEqualStrings("", sort_profile.sort_rejection_reason);
    try std.testing.expectEqualStrings("", sort_profile.sort_rejection_detail);
    try std.testing.expectEqualStrings("", sort_profile.sort_rejection_field.slice());
}

test "query merge rejects distributed field sort without runtime schema" {
    const alloc = std.testing.allocator;
    const order_by = [_]db_mod.types.SortField{
        .{ .field = "rank" },
        .{ .field = "_id" },
    };

    var hits = try alloc.alloc(db_mod.types.SearchHit, 1);
    hits[0] = try testSortedQueryHitAlloc(alloc, "doc:a", 1);
    var result = db_mod.types.SearchResult{ .alloc = alloc, .hits = hits, .total_hits = 1 };
    defer result.deinit();

    try std.testing.expectError(error.UnsupportedQueryRequest, mergeSearchResults(alloc, .{
        .order_by = &order_by,
    }, &.{result}, 0, 1));
}

test "query merge applies runtime schema to distributed date cursors" {
    const alloc = std.testing.allocator;
    const mapping = runtime_schema_mod.DynamicTemplate{
        .name = "created_at",
        .path_match = "created_at",
        .mapping = .{
            .field_type = .datetime,
            .doc_values = true,
            .sortable = true,
            .analyzer = "keyword",
        },
    };
    const templates = [_]runtime_schema_mod.DynamicTemplate{mapping};
    const schema = runtime_schema_mod.TableSchema{ .dynamic_templates = &templates };
    const order_by = [_]db_mod.types.SortField{.{ .field = "created_at" }};

    const ts_a = runtime_schema_mod.parseDateTimeToNs("2026-01-01T00:00:00Z") orelse return error.TestUnexpectedResult;
    const ts_b = runtime_schema_mod.parseDateTimeToNs("2026-01-02T00:00:00Z") orelse return error.TestUnexpectedResult;
    const ts_c = runtime_schema_mod.parseDateTimeToNs("2026-01-03T00:00:00Z") orelse return error.TestUnexpectedResult;

    var left_hits = try alloc.alloc(db_mod.types.SearchHit, 2);
    left_hits[0] = try testDateSortedQueryHitAlloc(alloc, "doc:a", ts_a);
    left_hits[1] = try testDateSortedQueryHitAlloc(alloc, "doc:c", ts_c);
    var right_hits = try alloc.alloc(db_mod.types.SearchHit, 1);
    right_hits[0] = try testDateSortedQueryHitAlloc(alloc, "doc:b", ts_b);

    var left = db_mod.types.SearchResult{ .alloc = alloc, .hits = left_hits, .total_hits = 2 };
    defer left.deinit();
    var right = db_mod.types.SearchResult{ .alloc = alloc, .hits = right_hits, .total_hits = 1 };
    defer right.deinit();

    const after_cursor = [_]std.json.Value{
        .{ .number_string = try std.fmt.allocPrint(alloc, "{d}", .{ts_b}) },
        .{ .string = "doc:b" },
    };
    defer alloc.free(after_cursor[0].number_string);

    var after_left_hits = try alloc.alloc(db_mod.types.SearchHit, 1);
    after_left_hits[0] = try testDateSortedQueryHitAlloc(alloc, "doc:c", ts_c);
    const after_right_hits = try alloc.alloc(db_mod.types.SearchHit, 0);
    var after_left = db_mod.types.SearchResult{ .alloc = alloc, .hits = after_left_hits, .total_hits = 1 };
    defer after_left.deinit();
    var after_right = db_mod.types.SearchResult{ .alloc = alloc, .hits = after_right_hits, .total_hits = 0 };
    defer after_right.deinit();
    var after_page = try mergeSearchResultsWithRuntimeSchema(alloc, .{
        .order_by = &order_by,
        .search_after = &after_cursor,
        .profile = true,
    }, &.{ after_left, after_right }, 0, 1, schema);
    defer after_page.deinit();

    try std.testing.expectEqual(@as(usize, 1), after_page.hits.len);
    try std.testing.expectEqualStrings("doc:c", after_page.hits[0].id);
    const sort_profile = after_page.sort_profile orelse return error.TestUnexpectedResult;
    try std.testing.expectEqualStrings("distributed_k_way_merge", sort_profile.plan);
    try std.testing.expectEqualStrings("bounded_exact", sort_profile.exactness);
    try std.testing.expectEqualStrings("distributed_merge", sort_profile.source);
}

test "query merge rejects sorted shards without complete sort tuples" {
    const alloc = std.testing.allocator;
    const order_by = [_]db_mod.types.SortField{
        .{ .field = "rank" },
        .{ .field = "_id" },
    };
    const schema = testRankRuntimeSchema();

    var hits = try alloc.alloc(db_mod.types.SearchHit, 1);
    hits[0] = .{
        .id = try alloc.dupe(u8, "doc:a"),
        .sort_values = try alloc.alloc(std.json.Value, 1),
    };
    hits[0].sort_values[0] = .{ .integer = 1 };
    var result = db_mod.types.SearchResult{ .alloc = alloc, .hits = hits, .total_hits = 1 };
    defer result.deinit();

    try std.testing.expectError(error.InvalidQueryRequest, mergeSearchResultsWithRuntimeSchema(alloc, .{ .order_by = &order_by }, &.{result}, 0, 10, schema));
}

test "query merge rejects sorted shards whose id tiebreaker mismatches hit id" {
    const alloc = std.testing.allocator;
    const order_by = [_]db_mod.types.SortField{
        .{ .field = "rank" },
        .{ .field = "_id" },
    };
    const schema = testRankRuntimeSchema();

    var hits = try alloc.alloc(db_mod.types.SearchHit, 1);
    hits[0] = .{
        .id = try alloc.dupe(u8, "doc:a"),
        .sort_values = try alloc.alloc(std.json.Value, 2),
    };
    hits[0].sort_values[0] = .{ .integer = 1 };
    hits[0].sort_values[1] = .{ .string = try alloc.dupe(u8, "doc:b") };
    var result = db_mod.types.SearchResult{ .alloc = alloc, .hits = hits, .total_hits = 1 };
    defer result.deinit();

    try std.testing.expectError(error.InvalidQueryRequest, mergeSearchResultsWithRuntimeSchema(alloc, .{ .order_by = &order_by }, &.{result}, 0, 10, schema));
}

test "query merge rejects sorted shards with mixed sort value domains" {
    const alloc = std.testing.allocator;
    const order_by = [_]db_mod.types.SortField{
        .{ .field = "rank" },
        .{ .field = "_id" },
    };
    const schema = testRankRuntimeSchema();

    var numeric_hits = try alloc.alloc(db_mod.types.SearchHit, 1);
    numeric_hits[0] = try testSortedQueryHitAlloc(alloc, "doc:a", 1);
    var string_hits = try alloc.alloc(db_mod.types.SearchHit, 1);
    string_hits[0] = .{
        .id = try alloc.dupe(u8, "doc:b"),
        .sort_values = try alloc.alloc(std.json.Value, 2),
    };
    string_hits[0].sort_values[0] = .{ .string = try alloc.dupe(u8, "two") };
    string_hits[0].sort_values[1] = .{ .string = try alloc.dupe(u8, "doc:b") };

    var numeric = db_mod.types.SearchResult{ .alloc = alloc, .hits = numeric_hits, .total_hits = 1 };
    defer numeric.deinit();
    var string = db_mod.types.SearchResult{ .alloc = alloc, .hits = string_hits, .total_hits = 1 };
    defer string.deinit();

    try std.testing.expectError(error.InvalidQueryRequest, mergeSearchResultsWithRuntimeSchema(alloc, .{ .order_by = &order_by }, &.{ numeric, string }, 0, 10, schema));
}

test "query merge preserves single-result doc ordinals" {
    const alloc = std.testing.allocator;

    var hits = try alloc.alloc(db_mod.types.SearchHit, 1);
    hits[0] = .{
        .id = try alloc.dupe(u8, "doc:a"),
        .doc_ordinal = 9,
        .score = 1.0,
    };

    var single = db_mod.types.SearchResult{ .alloc = alloc, .hits = hits, .total_hits = 1 };
    defer single.deinit();

    var merged = try mergeSearchResults(alloc, .{}, &.{single}, 0, 1);
    defer merged.deinit();

    try std.testing.expectEqual(@as(usize, 1), merged.hits.len);
    try std.testing.expectEqualStrings("doc:a", merged.hits[0].id);
    try std.testing.expectEqual(@as(?u32, 9), merged.hits[0].doc_ordinal);
}

fn expectGraphTableProvenanceMerge(alloc: std.mem.Allocator) !void {
    var node_path = [_][]const u8{ "doc:a", "shared" };
    var node_path_edge = [_]graph_query_mod.PathEdgeInfo{.{
        .source = "doc:a",
        .target = "shared",
        .edge_type = "mentions",
        .weight = 1,
        .metadata = "{\"target_table\":\"entities\"}",
    }};
    var graph_nodes = [_]graph_query_mod.GraphResultNode{.{
        .key = "shared",
        .depth = 1,
        .distance = 1,
        .path = &node_path,
        .path_edges = &node_path_edge,
        .table = "entities",
    }};
    var graph_path_nodes = [_][]const u8{ "doc:a", "shared" };
    var graph_path_tables = [_]?[]const u8{ null, "entities" };
    var graph_path_edges = [_]graph_paths.PathEdge{.{
        .source = "doc:a",
        .target = "shared",
        .edge_type = "mentions",
        .weight = 1,
        .metadata = "{\"target_table\":\"entities\"}",
    }};
    var graph_paths_input = [_]db_mod.types.GraphPath{.{
        .nodes = &graph_path_nodes,
        .node_tables = &graph_path_tables,
        .edges = &graph_path_edges,
        .total_weight = 1,
        .length = 1,
    }};
    var match_bindings = [_]db_mod.types.GraphPatternBinding{.{
        .alias = @constCast("entity"),
        .node = .{
            .key = "shared",
            .depth = 1,
            .distance = 1,
            .path = null,
            .path_edges = null,
            .table = "entities",
        },
    }};
    var matches = [_]db_mod.types.GraphPatternMatch{.{
        .bindings = &match_bindings,
        .path = &node_path_edge,
    }};
    var graph_hits = [_]db_mod.types.SearchHit{.{
        .id = @constCast("shared"),
        .source_table = @constCast("entities"),
    }};
    var graph_results = [_]db_mod.types.GraphSearchResult{.{
        .name = @constCast("related"),
        .nodes = &graph_nodes,
        .paths = &graph_paths_input,
        .matches = &matches,
        .hits = &graph_hits,
        .total_hits = 1,
    }};
    const input = db_mod.types.SearchResult{
        .alloc = alloc,
        .hits = @constCast((&[_]db_mod.types.SearchHit{})[0..]),
        .total_hits = 0,
        .graph_results = &graph_results,
    };

    var merged = try mergeSearchResults(alloc, .{}, &.{input}, 0, 0);
    defer merged.deinit();

    const graph_result = merged.graph_results[0];
    try std.testing.expectEqualStrings("entities", graph_result.nodes[0].table.?);
    try std.testing.expectEqualStrings(
        "entities",
        graph_result.paths[0].node_tables[1].?,
    );
    try std.testing.expectEqualStrings(
        "entities",
        graph_result.matches[0].bindings[0].node.table.?,
    );
    try std.testing.expectEqualStrings(
        "entities",
        graph_result.hits[0].source_table.?,
    );
}

test "query merge preserves graph table provenance under allocation failure" {
    try std.testing.checkAllAllocationFailures(
        std.testing.allocator,
        expectGraphTableProvenanceMerge,
        .{},
    );
}

test "query merge preserves lower-bound total relation" {
    const alloc = std.testing.allocator;

    var left_hits = try alloc.alloc(db_mod.types.SearchHit, 1);
    left_hits[0] = .{
        .id = try alloc.dupe(u8, "doc:a"),
        .score = 1.0,
    };
    var right_hits = try alloc.alloc(db_mod.types.SearchHit, 1);
    right_hits[0] = .{
        .id = try alloc.dupe(u8, "doc:b"),
        .score = 0.5,
    };

    var left = db_mod.types.SearchResult{
        .alloc = alloc,
        .hits = left_hits,
        .total_hits = 1,
        .total_hits_relation = .gte,
    };
    defer left.deinit();
    var right = db_mod.types.SearchResult{ .alloc = alloc, .hits = right_hits, .total_hits = 1 };
    defer right.deinit();

    var merged = try mergeSearchResults(alloc, .{}, &.{ left, right }, 0, 10);
    defer merged.deinit();

    try std.testing.expectEqual(@as(u32, 2), merged.total_hits);
    try std.testing.expectEqual(db_mod.types.TotalHitsRelation.gte, merged.total_hits_relation);
}

test "query merge preserves common identity read generation" {
    const alloc = std.testing.allocator;

    var left_hits = try alloc.alloc(db_mod.types.SearchHit, 1);
    left_hits[0] = .{
        .id = try alloc.dupe(u8, "doc:a"),
        .score = 1.0,
    };
    var right_hits = try alloc.alloc(db_mod.types.SearchHit, 1);
    right_hits[0] = .{
        .id = try alloc.dupe(u8, "doc:b"),
        .score = 0.5,
    };

    var left = db_mod.types.SearchResult{
        .alloc = alloc,
        .hits = left_hits,
        .total_hits = 1,
        .identity_read_generation = 17,
    };
    defer left.deinit();
    var right = db_mod.types.SearchResult{
        .alloc = alloc,
        .hits = right_hits,
        .total_hits = 1,
        .identity_read_generation = 17,
    };
    defer right.deinit();

    var merged = try mergeSearchResults(alloc, .{}, &.{ left, right }, 0, 10);
    defer merged.deinit();
    try std.testing.expectEqual(@as(?u64, 17), merged.identity_read_generation);

    var stamped = try mergeSearchResults(alloc, .{ .identity_read_generation = 19 }, &.{ left, right }, 0, 10);
    defer stamped.deinit();
    try std.testing.expectEqual(@as(?u64, 19), stamped.identity_read_generation);

    right.identity_read_generation = 18;
    var mixed = try mergeSearchResults(alloc, .{}, &.{ left, right }, 0, 10);
    defer mixed.deinit();
    try std.testing.expectEqual(@as(?u64, null), mixed.identity_read_generation);
}

test "query merge orders pure dense results by descending relevance score" {
    const alloc = std.testing.allocator;

    var left_hits = try alloc.alloc(db_mod.types.SearchHit, 2);
    left_hits[0] = .{
        .id = try alloc.dupe(u8, "doc:b"),
        .score = 0.5,
        .distance = 1.0,
        .stored_data = null,
    };
    left_hits[1] = .{
        .id = try alloc.dupe(u8, "doc:c"),
        .score = 0.8,
        .distance = 0.2,
        .stored_data = null,
    };
    var right_hits = try alloc.alloc(db_mod.types.SearchHit, 1);
    right_hits[0] = .{
        .id = try alloc.dupe(u8, "doc:a"),
        .score = 1.0,
        .distance = 0.0,
        .stored_data = null,
    };

    var left = db_mod.types.SearchResult{ .alloc = alloc, .hits = left_hits, .total_hits = 2 };
    defer left.deinit();
    var right = db_mod.types.SearchResult{ .alloc = alloc, .hits = right_hits, .total_hits = 1 };
    defer right.deinit();

    var req: db_mod.types.SearchRequest = .{};
    const dense_vec = try alloc.alloc(f32, 1);
    defer alloc.free(dense_vec);
    dense_vec[0] = 1.0;
    const dense_queries = try alloc.alloc(db_mod.types.NamedDenseQuery, 1);
    defer {
        alloc.free(dense_queries[0].index_name);
        alloc.free(dense_queries[0].query.vector);
        alloc.free(dense_queries);
    }
    dense_queries[0] = .{
        .name = "",
        .index_name = try alloc.dupe(u8, "dense_idx"),
        .query = .{ .vector = try alloc.dupe(f32, dense_vec), .k = 3 },
    };
    req.dense_queries = dense_queries;

    var merged = try mergeSearchResults(alloc, req, &.{ left, right }, 0, 3);
    defer merged.deinit();

    try std.testing.expectEqual(@as(usize, 3), merged.hits.len);
    try std.testing.expectEqualStrings("doc:a", merged.hits[0].id);
    try std.testing.expectEqualStrings("doc:c", merged.hits[1].id);
    try std.testing.expectEqualStrings("doc:b", merged.hits[2].id);
    try std.testing.expectEqual(@as(?f32, 0.0), merged.hits[0].distance);
}
