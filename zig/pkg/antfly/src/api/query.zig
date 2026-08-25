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
const metadata_test_openapi = @import("antfly_metadata_openapi");
const db_mod = @import("../storage/db/mod.zig");
const db_query_search = @import("../storage/db/query/search_exec.zig");
const hierarchy_navigation = @import("../storage/hierarchy_navigation.zig");
const runtime_schema_mod = @import("../storage/schema.zig");
const graph_paths = @import("../graph/paths.zig");
const graph_query_mod = @import("../graph/query.zig");
const graph_node_identity = @import("../graph/node_identity.zig");
const query_contract = @import("query_contract.zig");

pub const QueryResponse = query_contract.QueryResponse;
pub const QueryResponseMeta = query_contract.QueryResponseMeta;
pub const OwnedQueryRequest = query_contract.OwnedQueryRequest;
pub const PublicFilterQueryErrorKind = query_contract.PublicFilterQueryErrorKind;

pub const parseQueryRequest = query_contract.parseQueryRequest;
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
    for (results) |result| {
        total_hits +|= result.total_hits;
        if (result.total_hits_relation == .gte) total_hits_relation = .gte;
    }

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

        const graph_results = if (req.graph_queries.len > 0)
            try mergeGraphSearchResults(alloc, req.graph_queries, results)
        else
            @constCast((&[_]db_mod.types.GraphSearchResult{})[0..]);
        errdefer {
            for (graph_results) |*graph_result| graph_result.deinit(alloc);
            if (graph_results.len > 0) alloc.free(graph_results);
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

    const graph_results = if (req.graph_queries.len > 0)
        try mergeGraphSearchResults(alloc, req.graph_queries, results)
    else
        @constCast((&[_]db_mod.types.GraphSearchResult{})[0..]);
    errdefer {
        for (graph_results) |*graph_result| graph_result.deinit(alloc);
        if (graph_results.len > 0) alloc.free(graph_results);
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
    };
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

    if (req.graph_queries.len > 0) {
        const graph_results = try mergeGraphSearchResults(alloc, req.graph_queries, results);
        errdefer {
            for (graph_results) |*graph_result| graph_result.deinit(alloc);
            if (graph_results.len > 0) alloc.free(graph_results);
        }
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
        req.reranker != null;
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

const GraphAggregateResultBuilder = struct {
    name: []u8,
    value: u128 = 0,
    exact: bool = true,
    distinct: bool,
    distinct_values: std.ArrayListUnmanaged(graph_node_identity.Ref) = .empty,
    distinct_seen: graph_node_identity.Map(void) = .{},

    fn appendDistinctValues(
        self: *GraphAggregateResultBuilder,
        alloc: std.mem.Allocator,
        incoming: db_mod.types.GraphAggregateResult,
    ) !void {
        if (incoming.value > 0 and incoming.distinct_values.len == 0)
            return error.InvalidQueryRequest;
        for (incoming.distinct_values) |value| {
            if (self.distinct_seen.contains(value)) continue;
            try appendClonedGraphNodeRef(alloc, &self.distinct_values, value);
            const owned = self.distinct_values.items[self.distinct_values.items.len - 1];
            errdefer {
                const removed = self.distinct_values.pop().?;
                if (removed.table) |table| alloc.free(table);
                alloc.free(removed.key);
            }
            _ = try self.distinct_seen.putIfAbsent(alloc, owned, {});
        }
        self.value = self.distinct_values.items.len;
    }

    fn deinit(self: *GraphAggregateResultBuilder, alloc: std.mem.Allocator) void {
        if (self.name.len > 0) alloc.free(self.name);
        self.distinct_seen.deinit(alloc);
        for (self.distinct_values.items) |value| {
            if (value.table) |table| alloc.free(table);
            alloc.free(value.key);
        }
        self.distinct_values.deinit(alloc);
        self.* = undefined;
    }

    fn toOwned(self: *GraphAggregateResultBuilder, alloc: std.mem.Allocator) !db_mod.types.GraphAggregateResult {
        const values = try self.distinct_values.toOwnedSlice(alloc);
        self.distinct_seen.deinit(alloc);
        self.distinct_seen = .{};
        const name = self.name;
        self.name = &.{};
        return .{
            .name = name,
            .value = self.value,
            .exact = self.exact,
            .distinct_values = values,
        };
    }
};

const GraphSearchResultBuilder = struct {
    name: []u8,
    nodes: std.ArrayListUnmanaged(graph_query_mod.GraphResultNode) = .empty,
    paths: std.ArrayListUnmanaged(db_mod.types.GraphPath) = .empty,
    matches: std.ArrayListUnmanaged(db_mod.types.GraphPatternMatch) = .empty,
    aggregates: std.ArrayListUnmanaged(GraphAggregateResultBuilder) = .empty,
    hits: std.ArrayListUnmanaged(db_mod.types.SearchHit) = .empty,
    total_hits: u32 = 0,
    truncated: bool = false,

    fn deinit(self: *GraphSearchResultBuilder, alloc: std.mem.Allocator) void {
        if (self.name.len > 0) alloc.free(self.name);
        for (self.nodes.items) |*node| node.deinit(alloc);
        self.nodes.deinit(alloc);
        for (self.paths.items) |path| graph_paths.freePath(alloc, path);
        self.paths.deinit(alloc);
        for (self.matches.items) |*match| match.deinit(alloc);
        self.matches.deinit(alloc);
        for (self.aggregates.items) |*aggregate| aggregate.deinit(alloc);
        self.aggregates.deinit(alloc);
        for (self.hits.items) |*hit| hit.deinit(alloc);
        self.hits.deinit(alloc);
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
        const aggregates = try alloc.alloc(db_mod.types.GraphAggregateResult, self.aggregates.items.len);
        var initialized_aggregates: usize = 0;
        errdefer {
            for (aggregates[0..initialized_aggregates]) |*aggregate| aggregate.deinit(alloc);
            if (aggregates.len > 0) alloc.free(aggregates);
        }
        for (self.aggregates.items, 0..) |*aggregate, i| {
            aggregates[i] = try aggregate.toOwned(alloc);
            initialized_aggregates += 1;
        }
        self.aggregates.deinit(alloc);
        self.aggregates = .empty;
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
            .aggregates = aggregates,
            .hits = hits,
            .total_hits = self.total_hits,
            .truncated = self.truncated,
        };
    }
};

fn mergeGraphSearchResults(
    alloc: std.mem.Allocator,
    queries: []const db_mod.types.NamedGraphQuery,
    results: []const db_mod.types.SearchResult,
) ![]db_mod.types.GraphSearchResult {
    var builders = std.ArrayListUnmanaged(GraphSearchResultBuilder).empty;
    defer {
        for (builders.items) |*builder| builder.deinit(alloc);
        builders.deinit(alloc);
    }

    for (results) |result| {
        try validateGraphQueriesForShard(queries, result.graph_results);
        for (result.graph_results) |graph_result| {
            try validateGraphAggregateShard(queries, graph_result);
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
            builder.truncated = builder.truncated or graph_result.truncated;

            for (graph_result.nodes) |node| {
                var owned = try cloneGraphResultNode(alloc, node);
                builder.nodes.append(alloc, owned) catch |err| {
                    owned.deinit(alloc);
                    return err;
                };
            }
            for (graph_result.paths) |path| {
                const owned = try cloneGraphPath(alloc, path);
                builder.paths.append(alloc, owned) catch |err| {
                    graph_paths.freePath(alloc, owned);
                    return err;
                };
            }
            for (graph_result.matches) |match| {
                const limit = graphQueryReturnLimit(queries, graph_result.name);
                if (limit > 0 and builder.matches.items.len >= limit) {
                    builder.truncated = true;
                    continue;
                }
                var owned = try cloneGraphPatternMatch(alloc, match);
                builder.matches.append(alloc, owned) catch |err| {
                    owned.deinit(alloc);
                    return err;
                };
            }
            for (graph_result.aggregates) |aggregate| {
                const distinct = graphAggregateIsDistinct(queries, graph_result.name, aggregate.name);
                var found = false;
                for (builder.aggregates.items) |*existing| {
                    if (!std.mem.eql(u8, existing.name, aggregate.name)) continue;
                    if (existing.distinct != distinct) return error.InvalidRemoteResponse;
                    existing.exact = existing.exact and aggregate.exact;
                    if (distinct) {
                        try existing.appendDistinctValues(alloc, aggregate);
                    } else {
                        existing.value = std.math.add(u128, existing.value, aggregate.value) catch return error.Overflow;
                    }
                    found = true;
                    break;
                }
                if (!found) {
                    const name = try alloc.dupe(u8, aggregate.name);
                    var owned = GraphAggregateResultBuilder{
                        .name = name,
                        .value = if (distinct) 0 else aggregate.value,
                        .exact = aggregate.exact,
                        .distinct = distinct,
                    };
                    var owned_active = true;
                    errdefer if (owned_active) owned.deinit(alloc);
                    if (distinct) try owned.appendDistinctValues(alloc, aggregate);
                    try builder.aggregates.append(alloc, owned);
                    owned_active = false;
                }
            }
            for (graph_result.hits) |hit| {
                var owned = try hit.clone(alloc);
                builder.hits.append(alloc, owned) catch |err| {
                    owned.deinit(alloc);
                    return err;
                };
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

/// A shard graph response is operation-keyed just like the public contract.
/// Empty operations are represented by an empty result, never by omitting the
/// operation, so version skew or a broken worker cannot silently turn an exact
/// graph request into a partial success.
fn validateGraphQueriesForShard(
    queries: []const db_mod.types.NamedGraphQuery,
    graph_results: []const db_mod.types.GraphSearchResult,
) !void {
    for (queries) |named| {
        var occurrences: usize = 0;
        for (graph_results) |graph_result| {
            if (std.mem.eql(u8, named.name, graph_result.name)) occurrences += 1;
        }
        if (occurrences != 1) return error.InvalidRemoteResponse;
    }

    for (graph_results) |graph_result| {
        var requested = false;
        for (queries) |named| {
            if (!std.mem.eql(u8, named.name, graph_result.name)) continue;
            requested = true;
            break;
        }
        if (!requested) return error.InvalidRemoteResponse;
    }
}

fn validateGraphAggregateShard(
    queries: []const db_mod.types.NamedGraphQuery,
    graph_result: db_mod.types.GraphSearchResult,
) !void {
    const query = blk: {
        for (queries) |named| {
            if (std.mem.eql(u8, named.name, graph_result.name)) break :blk named.query;
        }
        return error.InvalidRemoteResponse;
    };
    if (query.aggregates.len == 0) {
        if (graph_result.aggregates.len != 0) return error.InvalidRemoteResponse;
        return;
    }
    if (graph_result.aggregates.len != query.aggregates.len) return error.InvalidRemoteResponse;
    for (query.aggregates) |requested| {
        var occurrences: usize = 0;
        for (graph_result.aggregates) |aggregate| {
            if (!std.mem.eql(u8, requested.name, aggregate.name)) continue;
            if (!aggregate.exact) return error.QueryCandidateBudgetExceeded;
            if (requested.distinct) {
                if (aggregate.value != aggregate.distinct_values.len)
                    return error.InvalidRemoteResponse;
            } else if (aggregate.distinct_values.len != 0) {
                return error.InvalidRemoteResponse;
            }
            occurrences += 1;
        }
        if (occurrences != 1) return error.InvalidRemoteResponse;
    }
}

fn graphAggregateIsDistinct(
    queries: []const db_mod.types.NamedGraphQuery,
    query_name: []const u8,
    aggregate_name: []const u8,
) bool {
    for (queries) |named| {
        if (!std.mem.eql(u8, named.name, query_name)) continue;
        for (named.query.aggregates) |aggregate| {
            if (std.mem.eql(u8, aggregate.name, aggregate_name)) return aggregate.distinct;
        }
        return false;
    }
    return false;
}

fn graphQueryReturnLimit(
    queries: []const db_mod.types.NamedGraphQuery,
    query_name: []const u8,
) u32 {
    for (queries) |named| {
        if (std.mem.eql(u8, named.name, query_name)) return named.query.return_limit;
    }
    return 0;
}

fn cloneGraphNodeRefs(alloc: std.mem.Allocator, values: []const graph_node_identity.Ref) ![]graph_node_identity.Ref {
    var out = std.ArrayListUnmanaged(graph_node_identity.Ref).empty;
    errdefer {
        for (out.items) |value| {
            if (value.table) |table| alloc.free(table);
            alloc.free(value.key);
        }
        out.deinit(alloc);
    }
    try out.ensureTotalCapacity(alloc, values.len);
    for (values) |value| try appendClonedGraphNodeRef(alloc, &out, value);
    return try out.toOwnedSlice(alloc);
}

fn appendClonedGraphNodeRef(
    alloc: std.mem.Allocator,
    out: *std.ArrayListUnmanaged(graph_node_identity.Ref),
    value: graph_node_identity.Ref,
) !void {
    const table = if (value.table) |table_name| try alloc.dupe(u8, table_name) else null;
    errdefer if (table) |table_name| alloc.free(table_name);
    const key = try alloc.dupe(u8, value.key);
    errdefer alloc.free(key);
    try out.append(alloc, .{ .table = table, .key = key });
}

fn freeGraphNodeRefs(alloc: std.mem.Allocator, values: []const graph_node_identity.Ref) void {
    for (values) |value| {
        if (value.table) |table| alloc.free(table);
        alloc.free(value.key);
    }
    if (values.len > 0) alloc.free(values);
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

    const aggregates = try alloc.alloc(db_mod.types.GraphAggregateResult, source.aggregates.len);
    var initialized_aggregates: usize = 0;
    errdefer {
        for (aggregates[0..initialized_aggregates]) |*aggregate| aggregate.deinit(alloc);
        if (source.aggregates.len > 0) alloc.free(aggregates);
    }
    for (source.aggregates, 0..) |aggregate, i| {
        const name = try alloc.dupe(u8, aggregate.name);
        const distinct_values = cloneGraphNodeRefs(alloc, aggregate.distinct_values) catch |err| {
            alloc.free(name);
            return err;
        };
        aggregates[i] = .{ .name = name, .value = aggregate.value, .exact = aggregate.exact, .distinct_values = distinct_values };
        initialized_aggregates += 1;
    }

    return .{
        .name = try alloc.dupe(u8, source.name),
        .nodes = nodes,
        .paths = paths,
        .matches = matches,
        .aggregates = aggregates,
        .hits = hits,
        .total_hits = source.total_hits,
        .truncated = source.truncated,
    };
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

    const null_aliases = try alloc.alloc([]u8, source.null_aliases.len);
    var initialized_null_aliases: usize = 0;
    errdefer {
        for (null_aliases[0..initialized_null_aliases]) |alias| alloc.free(alias);
        if (source.null_aliases.len > 0) alloc.free(null_aliases);
    }
    for (source.null_aliases, 0..) |alias, i| {
        null_aliases[i] = try alloc.dupe(u8, alias);
        initialized_null_aliases += 1;
    }

    return .{
        .bindings = bindings,
        .path = path,
        .null_aliases = null_aliases,
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
    const path_tables = if (source.path_tables) |items| try cloneOptionalStringSlice(alloc, items) else null;
    errdefer if (path_tables) |items| freeOptionalStringSlice(alloc, items);
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

    return .{
        .key = key,
        .depth = source.depth,
        .distance = source.distance,
        .path = path,
        .path_tables = path_tables,
        .path_edges = path_edges,
        .provenance = provenance,
        .table = table,
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

test "query parser accepts graph queries" {
    var owned = try parseQueryRequest(std.testing.allocator, null, "docs",
        \\{"graph_queries":{"neighbors":{"index":"graph_idx","traverse":{"start":{"keys":["doc:a"]},"edge_types":["links"],"max_depth":1}}},"limit":10}
    );
    defer owned.deinit(std.testing.allocator);

    try std.testing.expectEqual(@as(usize, 1), owned.req.graph_queries.len);
    try std.testing.expectEqualStrings("neighbors", owned.req.graph_queries[0].name);
    try std.testing.expectEqualStrings("graph_idx", owned.req.graph_queries[0].query.index_name);
    try std.testing.expect(owned.req.graph_queries[0].query.query_type == .traverse);
    switch (owned.req.graph_queries[0].query.start_nodes) {
        .identities => |identities| {
            try std.testing.expectEqualStrings("doc:a", identities[0].key);
            try std.testing.expect(identities[0].table == null);
        },
        else => return error.TestUnexpectedResult,
    }
}

test "query parser preserves exact graph path endpoint identities" {
    var owned = try parseQueryRequest(std.testing.allocator, null, "docs",
        \\{"graph_queries":{"path":{"index":"graph_idx","shortest_path":{"from":{"key":"shared"},"to":{"key":"shared","table":"companies"}}}},"limit":10}
    );
    defer owned.deinit(std.testing.allocator);

    const graph_query = owned.req.graph_queries[0].query;
    switch (graph_query.start_nodes) {
        .identities => |identities| {
            try std.testing.expectEqual(@as(usize, 1), identities.len);
            try std.testing.expectEqualStrings("shared", identities[0].key);
            try std.testing.expect(identities[0].table == null);
        },
        else => return error.TestUnexpectedResult,
    }
    switch (graph_query.target_nodes.?) {
        .identities => |identities| {
            try std.testing.expectEqual(@as(usize, 1), identities.len);
            try std.testing.expectEqualStrings("shared", identities[0].key);
            try std.testing.expectEqualStrings("companies", identities[0].table.?);
        },
        else => return error.TestUnexpectedResult,
    }
}

test "query parser adapts deprecated graph searches" {
    var owned = try parseQueryRequest(std.testing.allocator, null, "docs",
        \\{"graph_searches":{"neighbors":{"type":"neighbors","index_name":"graph_idx","start_nodes":{"keys":["doc:a"]},"params":{"edge_types":["links"],"max_depth":1}}},"limit":10}
    );
    defer owned.deinit(std.testing.allocator);

    try std.testing.expectEqual(@as(usize, 1), owned.req.graph_queries.len);
    try std.testing.expectEqualStrings("neighbors", owned.req.graph_queries[0].name);
    try std.testing.expect(owned.req.graph_queries[0].query.query_type == .neighbors);
    try std.testing.expect(std.mem.startsWith(u8, owned.req.graph_queries_proxy_json, "{\"graph_searches\":"));
}

test "query parser rejects graph queries and graph searches together" {
    try std.testing.expectError(error.InvalidQueryRequest, parseQueryRequest(std.testing.allocator, null, "docs",
        \\{"graph_queries":{"new":{"index":"graph_idx","traverse":{"start":{"keys":["doc:a"]}}}},"graph_searches":{"old":{"type":"neighbors","index_name":"graph_idx","start_nodes":{"keys":["doc:a"]}}}}
    ));
}

test "query parser accepts graph pattern searches" {
    var owned = try parseQueryRequest(std.testing.allocator, null, "docs",
        \\{"graph_queries":{"pattern_walk":{"index":"graph_idx","match":{"anchor":"a","nodes":{"a":{"filter":{"ids":["doc:a"]}},"b":{"table":"entities"}},"edges":[{"from":"a","to":"b","types":["links"],"max_hops":2}]},"return":{"bindings":["b"],"limit":10}}},"limit":10}
    );
    defer owned.deinit(std.testing.allocator);

    try std.testing.expectEqual(@as(usize, 1), owned.req.graph_queries.len);
    try std.testing.expect(owned.req.graph_queries[0].query.query_type == .pattern);
    try std.testing.expectEqual(@as(usize, 2), owned.req.graph_queries[0].query.match_pattern.?.nodes.len);
    try std.testing.expectEqualStrings("entities", owned.req.graph_queries[0].query.match_pattern.?.nodes[1].table.?);
    try std.testing.expectEqual(@as(usize, 1), owned.req.graph_queries[0].query.return_aliases.len);
    try std.testing.expectEqual(@as(u32, 10), owned.req.graph_queries[0].query.params.max_results);
}

test "query parser owns graph match anchor through its required node alias" {
    var owned = try parseQueryRequest(std.testing.allocator, null, "docs",
        \\{"graph_queries":{"escaped":{"index":"graph_idx","match":{"anchor":"a\n","nodes":{"a\n":{}},"edges":[]},"return":{"bindings":["a\n"]}}}}
    );
    defer owned.deinit(std.testing.allocator);

    const pattern = owned.req.graph_queries[0].query.match_pattern.?;
    try std.testing.expect(pattern.anchor_alias != null);
    const anchor_alias = pattern.anchor_alias.?;
    try std.testing.expectEqualStrings("a\n", anchor_alias);
    try std.testing.expectEqual(@intFromPtr(pattern.nodes[0].alias.ptr), @intFromPtr(anchor_alias.ptr));
}

test "query parser treats explicit graph document fields as a projection" {
    var owned = try parseQueryRequest(std.testing.allocator, null, "docs",
        \\{"graph_queries":{"walk":{"index":"graph_idx","traverse":{"start":{"keys":["doc:a"]},"include_documents":true,"fields":["title"]}}},"limit":10}
    );
    defer owned.deinit(std.testing.allocator);

    const graph_query = owned.req.graph_queries[0].query;
    try std.testing.expect(graph_query.include_documents);
    try std.testing.expect(!graph_query.include_all_fields);
    try std.testing.expectEqual(@as(usize, 1), graph_query.fields.len);
    try std.testing.expectEqualStrings("title", graph_query.fields[0]);
}

test "query parser accepts exact graph count aggregates" {
    var owned = try parseQueryRequest(std.testing.allocator, null, "docs",
        \\{"graph_queries":{"pattern_count":{"index":"graph_idx","match":{"anchor":"a","nodes":{"a":{}},"edges":[]},"return":{"aggregates":{"count":{"count":"*"}}}}},"limit":10}
    );
    defer owned.deinit(std.testing.allocator);

    const graph_query = owned.req.graph_queries[0].query;
    try std.testing.expectEqual(@as(usize, 1), graph_query.aggregates.len);
    try std.testing.expectEqualStrings("count", graph_query.aggregates[0].name);
    try std.testing.expectEqualStrings("*", graph_query.aggregates[0].of);
}

test "query parser accepts duplicate graph count expressions under different names" {
    var owned = try parseQueryRequest(std.testing.allocator, null, "docs",
        \\{"graph_queries":{"pattern_count":{"index":"graph_idx","match":{"anchor":"a","nodes":{"a":{}},"edges":[]},"return":{"aggregates":{"first":{"count":"a","distinct":true},"second":{"count":"a","distinct":true}}}}},"limit":10}
    );
    defer owned.deinit(std.testing.allocator);
    try std.testing.expectEqual(@as(usize, 2), owned.req.graph_queries[0].query.aggregates.len);
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
                    .include_documents = true,
                },
            },
        },
    }, .{ .took_ms = 4 }, result);
    defer encoded.deinit(alloc);
    var parsed = try ant_json.parseFromSlice(metadata_test_openapi.QueryResponses, alloc, encoded.json, .{});
    defer parsed.deinit();
    const responses = parsed.value.responses orelse return error.TestUnexpectedResult;
    try std.testing.expectEqual(@as(usize, 1), responses.len);
    const decoded_graph_results = responses[0].graph_results orelse return error.TestUnexpectedResult;
    const result_value = decoded_graph_results.map.get("neighbors") orelse return error.TestUnexpectedResult;
    const neighbors = switch (result_value) {
        .graph_nodes_result => |value| value,
        else => return error.TestUnexpectedResult,
    };
    const nodes = neighbors.nodes;
    try std.testing.expectEqual(@as(usize, 1), nodes.len);
    try std.testing.expectEqualStrings("doc:b", nodes[0].key);
    const document = nodes[0].document orelse return error.TestUnexpectedResult;
    try std.testing.expectEqualStrings("beta", document.object.get("title").?.string);
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

    const queries = [_]db_mod.types.NamedGraphQuery{.{
        .name = "related",
        .query = .{
            .query_type = .neighbors,
            .index_name = "graph",
            .start_nodes = .{ .keys = &.{} },
        },
    }};
    var merged = try mergeSearchResults(alloc, .{ .graph_queries = &queries }, &.{input}, 0, 0);
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

fn expectGraphMergeRowLimitAndDistinctIdentity(alloc: std.mem.Allocator) !void {
    var binding_a = [_]db_mod.types.GraphPatternBinding{.{ .alias = @constCast("n"), .node = .{ .key = @constCast("shared"), .table = @constCast("people"), .depth = 0, .distance = 0, .path = &.{}, .path_edges = &.{} } }};
    var binding_b = [_]db_mod.types.GraphPatternBinding{.{ .alias = @constCast("n"), .node = .{ .key = @constCast("other"), .table = @constCast("people"), .depth = 0, .distance = 0, .path = &.{}, .path_edges = &.{} } }};
    var binding_c = [_]db_mod.types.GraphPatternBinding{.{ .alias = @constCast("n"), .node = .{ .key = @constCast("shared"), .table = @constCast("companies"), .depth = 0, .distance = 0, .path = &.{}, .path_edges = &.{} } }};
    var shard_one_matches = [_]db_mod.types.GraphPatternMatch{ .{ .bindings = &binding_a, .path = &.{} }, .{ .bindings = &binding_b, .path = &.{} } };
    var shard_two_matches = [_]db_mod.types.GraphPatternMatch{.{ .bindings = &binding_c, .path = &.{} }};
    var shard_one_distinct = [_]graph_node_identity.Ref{.{ .table = "people", .key = "shared" }};
    var shard_two_distinct = [_]graph_node_identity.Ref{ .{ .table = "people", .key = "shared" }, .{ .table = "companies", .key = "shared" } };
    var shard_one_aggregates = [_]db_mod.types.GraphAggregateResult{.{ .name = @constCast("unique"), .value = 1, .distinct_values = &shard_one_distinct }};
    var shard_two_aggregates = [_]db_mod.types.GraphAggregateResult{.{ .name = @constCast("unique"), .value = 2, .distinct_values = &shard_two_distinct }};
    var first_graph = [_]db_mod.types.GraphSearchResult{
        .{ .name = @constCast("rows"), .matches = &shard_one_matches, .hits = &.{}, .total_hits = 2 },
        .{ .name = @constCast("distinct"), .aggregates = &shard_one_aggregates, .hits = &.{}, .total_hits = 0 },
    };
    var second_graph = [_]db_mod.types.GraphSearchResult{
        .{ .name = @constCast("rows"), .matches = &shard_two_matches, .hits = &.{}, .total_hits = 1 },
        .{ .name = @constCast("distinct"), .aggregates = &shard_two_aggregates, .hits = &.{}, .total_hits = 0 },
    };
    const shard_results = [_]db_mod.types.SearchResult{
        .{ .alloc = alloc, .hits = &.{}, .total_hits = 0, .graph_results = &first_graph },
        .{ .alloc = alloc, .hits = &.{}, .total_hits = 0, .graph_results = &second_graph },
    };
    const aggregates = [_]graph_query_mod.NamedCountAggregate{.{ .name = "unique", .of = "n", .distinct = true }};
    const queries = [_]db_mod.types.NamedGraphQuery{
        .{ .name = "rows", .query = .{
            .query_type = .pattern,
            .index_name = "graph",
            .start_nodes = .{ .keys = &.{} },
            .return_limit = 2,
        } },
        .{ .name = "distinct", .query = .{
            .query_type = .pattern,
            .index_name = "graph",
            .start_nodes = .{ .keys = &.{} },
            .aggregates = &aggregates,
        } },
    };

    const merged = try mergeGraphSearchResults(alloc, &queries, &shard_results);
    defer {
        for (merged) |*result| result.deinit(alloc);
        alloc.free(merged);
    }
    try std.testing.expectEqual(@as(usize, 2), merged[0].matches.len);
    try std.testing.expect(merged[0].truncated);
    try std.testing.expectEqual(@as(u128, 2), merged[1].aggregates[0].value);
    try std.testing.expectEqual(@as(usize, 2), merged[1].aggregates[0].distinct_values.len);
}

test "graph merge enforces query-wide row limit and exact distinct identity" {
    try expectGraphMergeRowLimitAndDistinctIdentity(std.testing.allocator);
}

test "graph distinct merge preserves ownership under allocation failure" {
    try std.testing.checkAllAllocationFailures(
        std.testing.allocator,
        expectGraphMergeRowLimitAndDistinctIdentity,
        .{},
    );
}

test "graph merge rejects missing and inexact aggregate shards" {
    const alloc = std.testing.allocator;
    const aggregates = [_]graph_query_mod.NamedCountAggregate{.{ .name = "count", .of = "*" }};
    const queries = [_]db_mod.types.NamedGraphQuery{.{ .name = "counted", .query = .{
        .query_type = .pattern,
        .index_name = "graph",
        .start_nodes = .{ .keys = &.{} },
        .aggregates = &aggregates,
    } }};

    var missing_graph = [_]db_mod.types.GraphSearchResult{.{
        .name = @constCast("counted"),
        .hits = &.{},
        .total_hits = 0,
    }};
    const missing_results = [_]db_mod.types.SearchResult{.{
        .alloc = alloc,
        .hits = &.{},
        .total_hits = 0,
        .graph_results = &missing_graph,
    }};
    try std.testing.expectError(
        error.InvalidRemoteResponse,
        mergeGraphSearchResults(alloc, &queries, &missing_results),
    );

    const omitted_results = [_]db_mod.types.SearchResult{.{
        .alloc = alloc,
        .hits = &.{},
        .total_hits = 0,
        .graph_results = &.{},
    }};
    try std.testing.expectError(
        error.InvalidRemoteResponse,
        mergeGraphSearchResults(alloc, &queries, &omitted_results),
    );

    var partial = [_]db_mod.types.GraphAggregateResult{.{
        .name = @constCast("count"),
        .value = 1,
        .exact = false,
    }};
    var partial_graph = [_]db_mod.types.GraphSearchResult{.{
        .name = @constCast("counted"),
        .aggregates = &partial,
        .hits = &.{},
        .total_hits = 0,
    }};
    const partial_results = [_]db_mod.types.SearchResult{.{
        .alloc = alloc,
        .hits = &.{},
        .total_hits = 0,
        .graph_results = &partial_graph,
    }};
    try std.testing.expectError(
        error.QueryCandidateBudgetExceeded,
        mergeGraphSearchResults(alloc, &queries, &partial_results),
    );

    const distinct_aggregates = [_]graph_query_mod.NamedCountAggregate{.{
        .name = "unique",
        .of = "person",
        .distinct = true,
    }};
    const distinct_queries = [_]db_mod.types.NamedGraphQuery{.{ .name = "counted", .query = .{
        .query_type = .pattern,
        .index_name = "graph",
        .start_nodes = .{ .keys = &.{} },
        .aggregates = &distinct_aggregates,
    } }};
    var incomplete_values = [_]graph_node_identity.Ref{.{ .table = "people", .key = "one" }};
    var incomplete = [_]db_mod.types.GraphAggregateResult{.{
        .name = @constCast("unique"),
        .value = 2,
        .distinct_values = &incomplete_values,
    }};
    var incomplete_graph = [_]db_mod.types.GraphSearchResult{.{
        .name = @constCast("counted"),
        .aggregates = &incomplete,
        .hits = &.{},
        .total_hits = 0,
    }};
    const incomplete_results = [_]db_mod.types.SearchResult{.{
        .alloc = alloc,
        .hits = &.{},
        .total_hits = 0,
        .graph_results = &incomplete_graph,
    }};
    try std.testing.expectError(
        error.InvalidRemoteResponse,
        mergeGraphSearchResults(alloc, &distinct_queries, &incomplete_results),
    );
}

test "graph merge rejects missing duplicate and unknown traversal operations" {
    const alloc = std.testing.allocator;
    const queries = [_]db_mod.types.NamedGraphQuery{.{ .name = "walk", .query = .{
        .query_type = .traverse,
        .index_name = "graph",
        .start_nodes = .{ .keys = &.{"doc:a"} },
    } }};

    const omitted = [_]db_mod.types.SearchResult{.{
        .alloc = alloc,
        .hits = &.{},
        .total_hits = 0,
        .graph_results = &.{},
    }};
    try std.testing.expectError(
        error.InvalidRemoteResponse,
        mergeGraphSearchResults(alloc, &queries, &omitted),
    );

    var duplicate_graph = [_]db_mod.types.GraphSearchResult{
        .{ .name = @constCast("walk"), .hits = &.{}, .total_hits = 0 },
        .{ .name = @constCast("walk"), .hits = &.{}, .total_hits = 0 },
    };
    const duplicate = [_]db_mod.types.SearchResult{.{
        .alloc = alloc,
        .hits = &.{},
        .total_hits = 0,
        .graph_results = &duplicate_graph,
    }};
    try std.testing.expectError(
        error.InvalidRemoteResponse,
        mergeGraphSearchResults(alloc, &queries, &duplicate),
    );

    var unknown_graph = [_]db_mod.types.GraphSearchResult{.{
        .name = @constCast("other"),
        .hits = &.{},
        .total_hits = 0,
    }};
    const unknown = [_]db_mod.types.SearchResult{.{
        .alloc = alloc,
        .hits = &.{},
        .total_hits = 0,
        .graph_results = &unknown_graph,
    }};
    try std.testing.expectError(
        error.InvalidRemoteResponse,
        mergeGraphSearchResults(alloc, &queries, &unknown),
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
