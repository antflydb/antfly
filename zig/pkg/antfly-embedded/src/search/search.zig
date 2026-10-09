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

//! High-level search API.
//!
//! Ties together text analysis, query construction, scoring, and result retrieval.
//! Users interact with SearchQuery/SearchResult instead of raw WANDScorer/PostingsIterator.
//!
//! Example:
//!   const result = try search.execute(alloc, snap, .{
//!       .query = .{ .match = .{ .field = "title", .text = "running dogs" } },
//!       .k = 10,
//!   });

const std = @import("std");
const Allocator = std.mem.Allocator;
const analysis_mod = @import("analysis.zig");
const index_mod = @import("../index.zig");
const scorer_mod = @import("scorer.zig");
pub const SearchDiagnostics = scorer_mod.SearchDiagnostics;
const query_mod = @import("query.zig");
const aggregation_mod = @import("aggregation.zig");
const roaring = @import("../encoding/roaring.zig");
const typed_dv = @import("../section/typed_doc_values.zig");
const segment_mod = @import("../segment.zig");
const inverted = @import("../section/inverted.zig");
const fusion_mod = @import("fusion.zig");
const geo_mod = @import("geo.zig");
const hbc_mod = @import("../storage/hbc_adapter.zig");
const graph_query = @import("../graph/query.zig");
const graph_mod = @import("../graph/graph.zig");
const distributed_stats_mod = @import("distributed_stats.zig");

pub const GeoPoint = geo_mod.GeoPoint;
pub const TotalHitsRelation = scorer_mod.TotalHitsRelation;

// ============================================================================
// Search request / result types
// ============================================================================

pub const AggType = union(enum) {
    stats: void,
    histogram: struct { interval: f64 },
    terms: struct { top_k: u32 },
    date_histogram: struct { interval: aggregation_mod.DateInterval },
    range: struct { ranges: []const aggregation_mod.RangeSpec },
    geo_distance: struct { center: geo_mod.GeoPoint, ranges: []const aggregation_mod.GeoDistanceRange },
    geohash_grid: struct { precision: u8, top_k: u32 = 100 },
};

pub const AggSpec = struct {
    name: []const u8,
    field: []const u8,
    agg_type: AggType,
    sub_aggs: []const AggSpec = &.{},
};

pub const AggResult = union(enum) {
    stats: aggregation_mod.StatsAgg,
    histogram: HistogramResult,
    terms: []const aggregation_mod.TermsFacet.FacetEntry,
    date_histogram: DateHistogramResult,
    range: []const aggregation_mod.RangeBucket,
    geo_distance: []const aggregation_mod.GeoDistanceBand,
    geohash_grid: []const aggregation_mod.GeohashGridAgg.GridEntry,
};

pub const HistogramResult = struct {
    keys: []i64,
    counts: []u64,
};

pub const DateHistogramResult = struct {
    keys: []i128,
    counts: []u64,
};

pub const BucketKey = union(enum) {
    int: i64,
    uint: u64,
    timestamp: i128,
    range_idx: u32,
    string: []const u8,
};

pub const BucketSubResult = struct {
    bucket_key: BucketKey,
    aggs: []const NamedAggResult,
};

pub const NamedAggResult = struct {
    name: []const u8,
    result: AggResult,
    sub_results: ?[]const BucketSubResult = null,
};

pub const SortSpec = struct {
    field: []const u8,
    order: enum { asc, desc } = .desc,
};

pub const SearchCursor = struct {
    score: f32,
    doc_id: u32,
};

/// Reference to an HBC vector index, passed by the caller into the search layer.
pub const HBCIndexRef = struct {
    name: []const u8,
    index: *hbc_mod.HBCIndex,
};

pub const NamedGraphQuery = struct {
    name: []const u8,
    query: graph_query.GraphQuery,
    graph_index: *graph_mod.GraphIndex,
};

pub const NamedGraphResult = struct {
    name: []const u8,
    result: graph_query.GraphQueryResult,
};

pub const SearchRequest = struct {
    query: SearchQuery,
    k: u32 = 10,
    offset: u32 = 0,
    include_stored: bool = true,
    aggregations: []const AggSpec = &.{},
    sort: ?SortSpec = null,
    search_after: ?SearchCursor = null,
    hbc_indexes: []const HBCIndexRef = &.{},
    graph_queries: []const NamedGraphQuery = &.{},
    expand_strategy: graph_query.ExpandStrategy = .@"union",
    distributed_text_stats: []const distributed_stats_mod.TextFieldStats = &.{},
    filter_doc_bitmap: ?*const roaring.RoaringBitmap = null,
    exclude_doc_bitmap: ?*const roaring.RoaringBitmap = null,
    filter_doc_nums: []const u32 = &.{},
    filter_doc_nums_positive: bool = false,
    exclude_doc_nums: []const u32 = &.{},
    bm25_config: inverted.BM25Config = .{},
    diagnostics: ?*scorer_mod.SearchDiagnostics = null,
};

pub const SearchQuery = union(enum) {
    match_none: void,
    match: MatchQuery,
    phrase: PhraseQuery,
    term_phrase: TermPhraseQuery,
    multi_phrase: MultiPhraseQuery,
    term: TermQuery,
    fuzzy: FuzzyQuery,
    numeric_range: NumericRangeQuery,
    date_range: DateRangeQuery,
    doc_id: DocIdQuery,
    doc_num: DocNumQuery,
    bool_field: BoolFieldQuery,
    geo_distance: GeoDistanceQuery,
    geo_bbox: GeoBBoxQuery,
    term_range: TermRangeQuery,
    ip_range: IPRangeQuery,
    geo_shape: GeoShapeQuery,
    prefix: PrefixQuery,
    wildcard: WildcardQuery,
    regexp: RegexpQuery,
    bool_query: BoolQuery,
    match_all: void,
    knn: KNNQuery,
    hybrid: HybridQuery,
};

/// KNN vector search query. Returns scored results from an HBC vector index.
pub const KNNQuery = struct {
    index_name: []const u8,
    vector: []const f32,
    k: u32 = 10,
};

/// Hybrid search: fuses BM25 text results with KNN vector results.
pub const HybridQuery = struct {
    text_query: TextQueryRef,
    knn: KNNQuery,
    fusion_config: fusion_mod.FusionConfig = .{},
};

/// Reference to a text sub-query for hybrid search (avoids self-referential union).
pub const TextQueryRef = union(enum) {
    match_none: void,
    match: MatchQuery,
    phrase: PhraseQuery,
    term_phrase: TermPhraseQuery,
    multi_phrase: MultiPhraseQuery,
    term: TermQuery,
    fuzzy: FuzzyQuery,
    numeric_range: NumericRangeQuery,
    date_range: DateRangeQuery,
    doc_id: DocIdQuery,
    bool_field: BoolFieldQuery,
    geo_distance: GeoDistanceQuery,
    geo_bbox: GeoBBoxQuery,
    term_range: TermRangeQuery,
    ip_range: IPRangeQuery,
    geo_shape: GeoShapeQuery,
    prefix: PrefixQuery,
    wildcard: WildcardQuery,
    regexp: RegexpQuery,
    bool_query: BoolQuery,
};

/// Analyzes text and performs multi-term BM25 search.
pub const MatchQuery = struct {
    field: []const u8,
    text: []const u8,
    analyzer: ?*const analysis_mod.Analyzer = null,
    boost: f32 = 1.0,
};

/// Exact term match with BM25 scoring (no analysis).
pub const TermQuery = struct {
    field: []const u8,
    term: []const u8,
    boost: f32 = 1.0,
};

pub const FuzzyQuery = struct {
    field: []const u8,
    term: []const u8,
    max_edits: u8 = 1,
    prefix_len: u8 = 0,
    auto_fuzzy: bool = false,
    boost: f32 = 1.0,
};

pub const TermPhraseQuery = struct {
    field: []const u8,
    terms: []const []const u8,
    max_edits: u8 = 0,
    auto_fuzzy: bool = false,
    boost: f32 = 1.0,
};

pub const MultiPhraseQuery = struct {
    field: []const u8,
    terms: []const []const []const u8,
    max_edits: u8 = 0,
    auto_fuzzy: bool = false,
    boost: f32 = 1.0,
};

pub const NumericRangeQuery = struct {
    field: []const u8,
    min: ?f64 = null,
    max: ?f64 = null,
    inclusive_min: bool = true,
    inclusive_max: bool = false,
    boost: f32 = 1.0,
};

pub const DateRangeQuery = struct {
    field: []const u8,
    start_ns: ?i128 = null,
    end_ns: ?i128 = null,
    inclusive_start: bool = true,
    inclusive_end: bool = false,
    boost: f32 = 1.0,
};

pub const DocIdQuery = struct {
    ids: []const []const u8,
    boost: f32 = 1.0,
};

pub const DocNumQuery = struct {
    ids: []const u32,
    bitmap: ?*const roaring.RoaringBitmap = null,
    producer: ?query_mod.DocNumProducer = null,
    boost: f32 = 1.0,
};

pub const BoolFieldQuery = struct {
    field: []const u8,
    value: bool,
    boost: f32 = 1.0,
};

pub const GeoDistanceQuery = struct {
    field: []const u8,
    center: geo_mod.GeoPoint,
    radius_meters: f64,
    boost: f32 = 1.0,
};

pub const GeoBBoxQuery = struct {
    field: []const u8,
    min_lat: f64,
    min_lon: f64,
    max_lat: f64,
    max_lon: f64,
    boost: f32 = 1.0,
};

pub const TermRangeQuery = struct {
    field: []const u8,
    min: ?[]const u8 = null,
    max: ?[]const u8 = null,
    inclusive_min: bool = true,
    inclusive_max: bool = false,
    boost: f32 = 1.0,
};

pub const IPRangeQuery = struct {
    field: []const u8,
    cidr: []const u8,
    boost: f32 = 1.0,
};

pub const GeoShapeRelation = enum {
    intersects,
    within,
    contains,
};

pub const GeoShapeQuery = struct {
    field: []const u8,
    relation: GeoShapeRelation = .intersects,
    polygons: []const []const geo_mod.GeoPoint,
    boost: f32 = 1.0,
};

/// Analyzed phrase match over positional term vectors.
pub const PhraseQuery = struct {
    field: []const u8,
    text: []const u8,
    analyzer: ?*const analysis_mod.Analyzer = null,
    max_edits: u8 = 0,
    auto_fuzzy: bool = false,
    boost: f32 = 1.0,
};

/// Prefix query executed via the term dictionary.
pub const PrefixQuery = struct {
    field: []const u8,
    prefix: []const u8,
    /// Optional materialized prefix field. Execution uses an exact lookup when
    /// the field exists in a segment and transparently falls back to the source
    /// dictionary for older segments.
    indexed_field: ?[]const u8 = null,
    boost: f32 = 1.0,
};

/// Wildcard query executed via FST-backed regexp expansion.
pub const WildcardQuery = struct {
    field: []const u8,
    pattern: []const u8,
    boost: f32 = 1.0,
};

/// Regexp query executed via FST automaton traversal.
pub const RegexpQuery = struct {
    field: []const u8,
    pattern: []const u8,
    boost: f32 = 1.0,
};

/// Boolean composition of sub-queries.
pub const BoolQuery = struct {
    must: []const SearchQuery = &.{},
    should: []const SearchQuery = &.{},
    must_not: []const SearchQuery = &.{},
    min_should: u32 = 0,
    pure_should_optional: bool = false,
    boost: f32 = 1.0,
};

pub const SearchResult = struct {
    alloc: Allocator,
    hits: []ScoredHit,
    total_hits: u32,
    total_hits_relation: TotalHitsRelation = .exact,
    aggregations: []NamedAggResult = &.{},
    cursor: ?SearchCursor = null,
    graph_results: []NamedGraphResult = &.{},

    pub fn deinit(self: *SearchResult) void {
        for (self.hits) |*hit| {
            freeStoredHit(self.alloc, hit);
            freeIndexScores(self.alloc, hit.index_scores);
        }
        self.alloc.free(self.hits);
        freeAggResults(self.alloc, self.aggregations);
        for (self.graph_results) |*gr| {
            var r = gr.result;
            r.deinit(self.alloc);
        }
        if (self.graph_results.len > 0) self.alloc.free(self.graph_results);
    }
};

fn freeStoredHit(allocator: Allocator, hit: *const ScoredHit) void {
    if (hit.stored_data) |body| allocator.free(body.ptr[0 .. body.len + hit.stored_id_bytes]);
}

fn freeStoredBodies(allocator: Allocator, hits: []ScoredHit) void {
    for (hits) |*hit| {
        freeStoredHit(allocator, hit);
        hit.stored_data = null;
        hit.stored_id_bytes = 0;
        hit.id = null;
    }
}

/// Sort only temporary references, leaving score/vector/fusion order intact.
/// Normal revision-4 writers assign stored blocks in document-number order.
/// One decoded block serves neighboring hits; output bodies remain owned.
fn populateStoredHits(allocator: Allocator, snapshot: *const index_mod.IndexSnapshot, hits: []ScoredHit, diagnostics: ?*scorer_mod.SearchDiagnostics) !void {
    if (hits.len == 0) return;
    var single = [_]usize{0};
    const order = if (hits.len == 1) &single else try allocator.alloc(usize, hits.len);
    defer if (hits.len > 1) allocator.free(order);
    for (order, 0..) |*slot, i| slot.* = i;
    std.mem.sort(usize, order, hits, struct {
        fn lessThan(context: []ScoredHit, left: usize, right: usize) bool {
            return context[left].doc_id < context[right].doc_id;
        }
    }.lessThan);
    var cursor = segment_mod.SegmentReader.StoredDocCursor.init(allocator);
    defer cursor.deinit();
    defer if (diagnostics) |diag| {
        diag.stored_block_decodes +|= cursor.decode_count;
    };
    errdefer freeStoredBodies(allocator, hits);
    for (order) |position| {
        const hit = &hits[position];
        if (try snapshot.storedDocWithCursor(&cursor, hit.doc_id)) |stored| {
            const id_length = if (cursor.reader.?.native != null) stored.id.len else 0;
            const allocation = try allocator.alloc(u8, try std.math.add(usize, stored.data.len, id_length));
            const body = allocation[0..stored.data.len];
            @memcpy(body, stored.data);
            if (id_length != 0) @memcpy(allocation[body.len..], stored.id);
            hit.id = if (id_length != 0) allocation[body.len..] else stored.id;
            hit.stored_data = body;
            hit.stored_id_bytes = id_length;
            if (diagnostics) |diag| {
                diag.stored_body_copies +|= 1;
                diag.stored_body_bytes +|= body.len;
            }
        }
    }
}

fn freeIndexScores(alloc: Allocator, scores: []fusion_mod.IndexScore) void {
    for (scores) |score| alloc.free(score.index_name);
    if (scores.len > 0) alloc.free(scores);
}

fn cloneIndexScores(alloc: Allocator, scores: []const fusion_mod.IndexScore) ![]fusion_mod.IndexScore {
    if (scores.len == 0) return &.{};
    const cloned = try alloc.alloc(fusion_mod.IndexScore, scores.len);
    var initialized: usize = 0;
    errdefer {
        for (cloned[0..initialized]) |score| alloc.free(score.index_name);
        alloc.free(cloned);
    }
    for (scores, 0..) |score, i| {
        cloned[i] = .{
            .index_name = try alloc.dupe(u8, score.index_name),
            .score = score.score,
        };
        initialized += 1;
    }
    return cloned;
}

fn freeAggResults(alloc: Allocator, aggs: []const NamedAggResult) void {
    for (aggs) |agg| {
        switch (agg.result) {
            .histogram => |h| {
                alloc.free(h.keys);
                alloc.free(h.counts);
            },
            .date_histogram => |dh| {
                alloc.free(dh.keys);
                alloc.free(dh.counts);
            },
            .terms => |entries| alloc.free(entries),
            .range => |r| alloc.free(r),
            .geo_distance => |g| alloc.free(g),
            .geohash_grid => |entries| alloc.free(entries),
            .stats => {},
        }
        if (agg.sub_results) |subs| {
            for (subs) |sub| {
                freeAggResults(alloc, sub.aggs);
            }
            alloc.free(subs);
        }
    }
    if (aggs.len > 0) alloc.free(aggs);
}

pub const ScoredHit = struct {
    doc_id: u32,
    score: f32,
    id: ?[]const u8,
    stored_data: ?[]u8,
    // Native identities share the body allocation as a trailing owned slice.
    stored_id_bytes: usize = 0,
    index_scores: []fusion_mod.IndexScore = &.{},
};

// ============================================================================
// Execute
// ============================================================================

/// Compute the effective k to request from the scorer. When cursor pagination is
/// active, we need to retrieve enough results to skip past the cursor position.
fn effectiveK(request: SearchRequest, snap: *const index_mod.IndexSnapshot) u32 {
    if (request.search_after != null) {
        // Retrieve all matching results so cursor filtering works correctly
        return snap.liveDocCount();
    }
    return request.k + request.offset;
}

/// Execute a search request against an index snapshot.
pub fn execute(
    alloc: Allocator,
    snap: *const index_mod.IndexSnapshot,
    request: SearchRequest,
) !SearchResult {
    var result = if (request.sort != null)
        try executeSort(alloc, snap, request)
    else switch (request.query) {
        .match_none => try executeMatchNone(alloc, request),
        .match => |mq| try executeMatch(alloc, snap, mq, request),
        .phrase => |pq| try executePhrase(alloc, snap, pq, request),
        .term_phrase => |pq| try executeTermPhrase(alloc, snap, pq, request),
        .multi_phrase => |pq| try executeMultiPhrase(alloc, snap, pq, request),
        .term => |tq| try executeTerm(alloc, snap, tq, request),
        .fuzzy => |fq| try executeFuzzy(alloc, snap, fq, request),
        .numeric_range => |rq| try executeNumericRange(alloc, snap, rq, request),
        .date_range => |rq| try executeDateRange(alloc, snap, rq, request),
        .doc_id => |dq| try executeDocID(alloc, snap, dq, request),
        .doc_num => |dq| try executeDocNum(alloc, snap, dq, request),
        .bool_field => |bq| try executeBoolField(alloc, snap, bq, request),
        .geo_distance => |gq| try executeGeoDistance(alloc, snap, gq, request),
        .geo_bbox => |gq| try executeGeoBBox(alloc, snap, gq, request),
        .term_range => |rq| try executeTermRange(alloc, snap, rq, request),
        .ip_range => |iq| try executeIPRange(alloc, snap, iq, request),
        .geo_shape => |gq| try executeGeoShape(alloc, snap, gq, request),
        .prefix => |pq| try executePrefix(alloc, snap, pq, request),
        .wildcard => |wq| try executeWildcard(alloc, snap, wq, request),
        .regexp => |rq| try executeRegexp(alloc, snap, rq, request),
        .match_all => try executeMatchAll(alloc, snap, request),
        .bool_query => |bq| try executeBool(alloc, snap, bq, request),
        .knn => |kq| try executeKNN(alloc, snap, kq, request),
        .hybrid => |hq| try executeHybrid(alloc, snap, hq, request),
    };
    errdefer result.deinit();

    if (request.graph_queries.len > 0) {
        try executeGraphSearches(alloc, &result, request);
    }

    return result;
}

/// Execute a query as an exact bitmap/filter candidate scan. This intentionally
/// skips BM25 scoring and stored payload loading; callers that need MVCC
/// visibility or stored pattern filters can still postprocess the returned doc
/// IDs through their normal result pipeline.
/// Exact snapshot count with at most one segment's filter bitmap in memory.
/// Callers must separately prove that no primary visibility/residual check is owed.
pub fn countMatches(alloc: Allocator, snap: *const index_mod.IndexSnapshot, query: SearchQuery) !u32 {
    var arena = std.heap.ArenaAllocator.init(alloc);
    defer arena.deinit();
    const filter = try searchQueryToFilterArena(arena.allocator(), query);
    return std.math.cast(u32, try snap.countFilter(alloc, filter)) orelse error.CountOverflow;
}

pub fn executeCountCandidates(
    alloc: Allocator,
    snap: *const index_mod.IndexSnapshot,
    query: SearchQuery,
) !SearchResult {
    var arena = std.heap.ArenaAllocator.init(alloc);
    defer arena.deinit();

    const filter = try searchQueryToFilterArena(arena.allocator(), query);
    const doc_ids = try snap.executeFilter(alloc, filter);
    defer alloc.free(doc_ids);

    var hits = try alloc.alloc(ScoredHit, doc_ids.len);
    errdefer alloc.free(hits);
    for (doc_ids, 0..) |doc_id, i| {
        hits[i] = .{
            .doc_id = doc_id,
            .score = 0,
            .id = null,
            .stored_data = null,
        };
    }

    return .{ .alloc = alloc, .hits = hits, .total_hits = @intCast(doc_ids.len) };
}

fn executeGraphSearches(alloc: Allocator, result: *SearchResult, request: SearchRequest) !void {
    var graph_results = try alloc.alloc(NamedGraphResult, request.graph_queries.len);
    errdefer alloc.free(graph_results);

    for (request.graph_queries, 0..) |gs, i| {
        // Resolve start node keys
        const resolved_keys = switch (gs.query.start_nodes) {
            .keys => |k| k,
            .identities => |identities| blk: {
                const keys = try alloc.alloc([]const u8, identities.len);
                errdefer alloc.free(keys);
                for (identities, 0..) |identity, identity_index| {
                    if (identity.table != null) return error.UnsupportedQueryRequest;
                    keys[identity_index] = identity.key;
                }
                break :blk keys;
            },
            .result_ref => |ref| blk: {
                // Extract doc IDs from search hits as keys
                _ = ref;
                var key_list = std.ArrayListUnmanaged([]const u8).empty;
                defer key_list.deinit(alloc);
                for (result.hits) |hit| {
                    if (hit.id) |id| {
                        try key_list.append(alloc, id);
                    }
                }
                break :blk try alloc.dupe([]const u8, key_list.items);
            },
        };

        var engine = graph_query.GraphQueryEngine{ .alloc = alloc };
        graph_results[i] = .{
            .name = gs.name,
            .result = try engine.execute(gs.graph_index, gs.query, resolved_keys),
        };

        // Free resolved keys if they were allocated from result_ref
        switch (gs.query.start_nodes) {
            .result_ref => alloc.free(resolved_keys),
            .identities => alloc.free(resolved_keys),
            .keys => {},
        }
    }

    result.graph_results = graph_results;
}

fn executeMatch(
    alloc: Allocator,
    snap: *const index_mod.IndexSnapshot,
    mq: MatchQuery,
    request: SearchRequest,
) !SearchResult {
    if (requestHasDocNumConstraints(request)) {
        if (try executeSimpleTextBool(alloc, snap, .{ .should = &.{.{ .match = mq }} }, request)) |result| return result;
    }
    const analyzer = mq.analyzer orelse &analysis_mod.default_analyzer;
    const tokens = try analyzer.analyze(alloc, mq.text);
    defer analysis_mod.Analyzer.freeTokens(alloc, tokens);

    if (tokens.len == 0) {
        return .{ .alloc = alloc, .hits = &.{}, .total_hits = 0 };
    }

    // Extract unique terms
    var term_list = std.ArrayListUnmanaged([]const u8).empty;
    defer term_list.deinit(alloc);
    for (tokens) |tok| {
        var found = false;
        for (term_list.items) |existing| {
            if (std.mem.eql(u8, existing, tok.term)) {
                found = true;
                break;
            }
        }
        if (!found) try term_list.append(alloc, tok.term);
    }

    const results = try searchSnapshotTerms(alloc, snap, mq.field, term_list.items, request);
    defer alloc.free(results.hits);
    if (mq.boost != 1.0) {
        for (results.hits) |*hit| hit.score *= mq.boost;
    }

    return buildResult(alloc, snap, results.hits, results.total_count, results.total_relation, request);
}

fn executeTerm(
    alloc: Allocator,
    snap: *const index_mod.IndexSnapshot,
    tq: TermQuery,
    request: SearchRequest,
) !SearchResult {
    if (requestHasDocNumConstraints(request)) {
        if (try executeSimpleTextBool(alloc, snap, .{ .should = &.{.{ .term = tq }} }, request)) |result| return result;
    }
    const results = try searchSnapshotTerms(alloc, snap, tq.field, &.{tq.term}, request);
    defer alloc.free(results.hits);
    if (tq.boost != 1.0) {
        for (results.hits) |*hit| hit.score *= tq.boost;
    }

    return buildResult(alloc, snap, results.hits, results.total_count, results.total_relation, request);
}

fn searchSnapshotTerms(
    alloc: Allocator,
    snap: *const index_mod.IndexSnapshot,
    field: []const u8,
    terms: []const []const u8,
    request: SearchRequest,
) !scorer_mod.SearchResults {
    const stats = matchingFieldStats(request.distributed_text_stats, field, terms);
    const k = if (requestHasDocNumConstraints(request)) @as(u32, @intCast(@min(snap.liveDocCount(), std.math.maxInt(u32)))) else effectiveK(request, snap);
    if (request.diagnostics) |diagnostics| {
        if (stats == null) {
            return snap.searchWithConfigDiagnostics(
                alloc,
                field,
                terms,
                k,
                request.bm25_config,
                diagnostics,
            );
        }
    }
    return snap.searchWithOverrideAndConfig(
        alloc,
        field,
        terms,
        k,
        stats,
        request.bm25_config,
    );
}

fn matchingFieldStats(
    items: []const distributed_stats_mod.TextFieldStats,
    field: []const u8,
    terms: []const []const u8,
) ?distributed_stats_mod.TextFieldStats {
    for (items) |item| {
        if (!std.mem.eql(u8, item.field, field)) continue;
        for (terms) |term| {
            if (item.termDocFreq(term) == null) return null;
        }
        return item;
    }
    return null;
}

fn buildPhraseFilter(
    alloc: Allocator,
    field: []const u8,
    text: []const u8,
    analyzer: *const analysis_mod.Analyzer,
    max_edits: u8,
    auto_fuzzy: bool,
) !?query_mod.Filter {
    const tokens = try analyzer.analyze(alloc, text);
    defer analysis_mod.Analyzer.freeTokens(alloc, tokens);

    if (tokens.len == 0) return null;

    var distinct_positions: usize = 0;
    var saw_alternatives = false;
    var last_position: ?u32 = null;
    for (tokens) |tok| {
        if (last_position == null or tok.position != last_position.?) {
            distinct_positions += 1;
            last_position = tok.position;
        } else {
            saw_alternatives = true;
        }
    }

    if (saw_alternatives) {
        var grouped_terms = try alloc.alloc([]const []const u8, distinct_positions);
        var groups_initialized: usize = 0;
        errdefer {
            for (grouped_terms[0..groups_initialized]) |group| {
                for (group) |term| alloc.free(term);
                alloc.free(group);
            }
            alloc.free(grouped_terms);
        }

        var slop: u32 = 0;
        var group_start: usize = 0;
        var group_idx: usize = 0;
        var prev_position: ?u32 = null;
        while (group_start < tokens.len) {
            const position = tokens[group_start].position;
            var group_end = group_start + 1;
            while (group_end < tokens.len and tokens[group_end].position == position) : (group_end += 1) {}

            const group = try alloc.alloc([]const u8, group_end - group_start);
            var group_initialized: usize = 0;
            errdefer {
                for (group[0..group_initialized]) |term| alloc.free(term);
                alloc.free(group);
            }
            for (tokens[group_start..group_end], 0..) |tok, i| {
                group[i] = try alloc.dupe(u8, tok.term);
                group_initialized = i + 1;
            }
            grouped_terms[group_idx] = group;
            groups_initialized = group_idx + 1;
            if (prev_position) |prev| {
                if (position > prev + 1) slop += position - prev - 1;
            }
            prev_position = position;
            group_idx += 1;
            group_start = group_end;
        }

        return .{ .multi_phrase = .{
            .field = field,
            .term_alternatives = grouped_terms,
            .slop = slop,
            .max_edits = max_edits,
            .auto_fuzzy = auto_fuzzy,
        } };
    }

    var terms = try alloc.alloc([]const u8, tokens.len);
    var initialized: usize = 0;
    errdefer {
        for (terms[0..initialized]) |term| alloc.free(term);
        alloc.free(terms);
    }

    var slop: u32 = 0;
    var prev_position: ?u32 = null;
    for (tokens, 0..) |tok, i| {
        terms[i] = try alloc.dupe(u8, tok.term);
        initialized = i + 1;
        if (prev_position) |prev| {
            if (tok.position > prev + 1) slop += tok.position - prev - 1;
        }
        prev_position = tok.position;
    }

    return .{ .phrase = .{
        .field = field,
        .terms = terms,
        .slop = slop,
        .max_edits = max_edits,
        .auto_fuzzy = auto_fuzzy,
    } };
}

fn freePhraseFilterTerms(alloc: Allocator, filter: query_mod.Filter) void {
    switch (filter) {
        .phrase => |pf| {
            for (pf.terms) |term| alloc.free(term);
            alloc.free(pf.terms);
        },
        .multi_phrase => |pf| {
            for (pf.term_alternatives) |position| {
                for (position) |term| alloc.free(term);
                alloc.free(position);
            }
            alloc.free(pf.term_alternatives);
        },
        else => {},
    }
}

fn executeFilterQuery(
    alloc: Allocator,
    snap: *const index_mod.IndexSnapshot,
    filter: query_mod.Filter,
    request: SearchRequest,
    boost: f32,
) !SearchResult {
    const doc_ids = try snap.executeFilter(alloc, filter);
    defer alloc.free(doc_ids);

    var all_scored = try alloc.alloc(scorer_mod.ScoredHit, doc_ids.len);
    errdefer alloc.free(all_scored);
    defer alloc.free(all_scored);
    for (doc_ids, 0..) |doc_id, i| {
        all_scored[i] = .{
            .doc_id = doc_id,
            .score = boost,
        };
    }

    return buildResult(alloc, snap, all_scored, @intCast(all_scored.len), .exact, request);
}

/// Exact positional verification plus bounded Tantivy-compatible phrase BM25
/// top-k. Phrase frequency is the number of positional matches and phrase IDF
/// is the sum of every constituent term's IDF (including repeated terms).
fn executeScoredPhraseFilter(
    alloc: Allocator,
    snap: *const index_mod.IndexSnapshot,
    phrase_filter: query_mod.PhraseFilter,
    request: SearchRequest,
    boost: f32,
) !SearchResult {
    if (phrase_filter.auto_fuzzy or
        phrase_filter.max_edits > 0 or
        phrase_filter.slop != 0 or
        request.aggregations.len != 0 or
        request.distributed_text_stats.len != 0)
    {
        return executeFilterQuery(alloc, snap, .{ .phrase = phrase_filter }, request, boost);
    }

    const scoring_doc_count = snap.scoringDocCount();
    var phrase_idf_sum: f32 = 0;
    var frequency_stack: [16]u32 = undefined;
    const frequencies = if (phrase_filter.terms.len <= frequency_stack.len) frequency_stack[0..phrase_filter.terms.len] else try alloc.alloc(u32, phrase_filter.terms.len);
    defer if (phrase_filter.terms.len > frequency_stack.len) alloc.free(frequencies);
    try snap.termDocFreqs(alloc, phrase_filter.field, phrase_filter.terms, frequencies);
    for (frequencies) |df| {
        if (df == 0) return .{ .alloc = alloc, .hits = try alloc.alloc(ScoredHit, 0), .total_hits = 0 };
        phrase_idf_sum += inverted.bm25Idf(scoring_doc_count, df);
    }

    var collector = FastTopK{
        .alloc = alloc,
        .k = effectiveK(request, snap),
        .filter_doc_bitmap = request.filter_doc_bitmap,
        .exclude_doc_bitmap = request.exclude_doc_bitmap,
        .filter_doc_nums = request.filter_doc_nums,
        .filter_doc_nums_positive = request.filter_doc_nums_positive,
        .exclude_doc_nums = request.exclude_doc_nums,
    };
    defer collector.deinit();

    const avg_dl = snap.textAvgDocLen(phrase_filter.field);
    if (request.diagnostics) |diag| diag.segments_considered +|= @intCast(snap.segments.len);
    var doc_offset: u32 = 0;
    for (snap.segments) |*segment| {
        const segment_offset = doc_offset;
        doc_offset += segment.reader.doc_count;

        var inv_reader = (try segment.reader.invertedIndexScoped(alloc, phrase_filter.field)) orelse continue;
        defer inv_reader.deinit();
        segment.shared.lockDeletionShared();
        defer segment.shared.unlockDeletionShared();
        const PhraseScoreState = struct {
            iter: inverted.PostingsIterator,
            current: ?inverted.PostingsIterator.Hit = null,
            doc_freq: u32,
        };
        const states = try alloc.alloc(PhraseScoreState, phrase_filter.terms.len);
        var initialized: usize = 0;
        defer {
            for (states[0..initialized]) |*state| state.iter.deinit();
            alloc.free(states);
        }
        var missing_term = false;
        var lead_index: usize = 0;
        for (phrase_filter.terms, 0..) |term, i| {
            const lookup = (try inv_reader.lookup(term)) orelse {
                missing_term = true;
                break;
            };
            states[i] = .{
                .iter = try lookup.iterator(alloc),
                .doc_freq = lookup.docFreq(),
            };
            states[i].current = try states[i].iter.advanceToDeferredPositions(0);
            initialized = i + 1;
            if (states[i].current == null) {
                missing_term = true;
                break;
            }
            if (states[i].doc_freq < states[lead_index].doc_freq) lead_index = i;
        }
        if (missing_term) continue;
        if (request.diagnostics) |diag| {
            diag.segments_searched +|= 1;
            diag.postings_iterators_opened +|= @intCast(states.len);
        }

        segment_candidates: while (states[lead_index].current != null) {
            var local_doc_id = states[lead_index].current.?.doc_id;
            var needs_realign = false;
            for (states, 0..) |*state, state_index| {
                if (state_index == lead_index) continue;
                if (state.current.?.doc_id < local_doc_id) {
                    state.current = try state.iter.advanceToDeferredPositions(local_doc_id);
                    if (state.current == null) break :segment_candidates;
                }
                if (state.current.?.doc_id > local_doc_id) {
                    local_doc_id = state.current.?.doc_id;
                    needs_realign = true;
                }
            }
            if (needs_realign) {
                states[lead_index].current = try states[lead_index].iter.advanceToDeferredPositions(local_doc_id);
                continue;
            }
            if (request.diagnostics) |diag| diag.phrase_candidates_verified +|= 1;
            // The approximation is now a true document conjunction. Decode
            // positions only for this shared candidate; documents skipped
            // while realigning advanced over their position records without
            // unpacking the deltas.
            const use_packed_two_term = states.len == 2 and
                states[0].iter.canTakeDeferredPackedPositions() and
                states[1].iter.canTakeDeferredPackedPositions();
            const phrase_frequency = if (use_packed_two_term) blk: {
                const first = try states[0].iter.takeDeferredPackedPositions();
                const second = try states[1].iter.takeDeferredPackedPositions();
                if (request.diagnostics) |diag| diag.phrase_position_records_decoded +|= 2;
                break :blk try exactTwoTermPackedPhraseFrequency(first, second);
            } else blk: {
                for (states) |*state| {
                    state.current = try state.iter.decodeDeferredPositions();
                    if (request.diagnostics) |diag| diag.phrase_position_records_decoded +|= 1;
                }
                break :blk exactPhraseFrequency(states);
            };
            const deleted = if (segment.shared.deleted) |deleted_docs| deleted_docs.contains(local_doc_id) else false;
            if (phrase_frequency > 0 and !deleted) {
                const score = inverted.bm25ScoreWithIdf(
                    phrase_frequency,
                    states[0].current.?.norm,
                    avg_dl,
                    phrase_idf_sum,
                    request.bm25_config,
                );
                try collector.collect(segment_offset + local_doc_id, score * boost);
                if (request.diagnostics) |diag| diag.phrase_matches_scored +|= 1;
            }
            if (local_doc_id == std.math.maxInt(u32)) break;
            states[lead_index].current = try states[lead_index].iter.advanceToDeferredPositions(local_doc_id + 1);
        }
    }

    const scored = try collector.finish();
    defer alloc.free(scored);
    return try buildResult(alloc, snap, scored, collector.total_count, .exact, request);
}

fn exactPhraseFrequency(states: anytype) u32 {
    if (states.len == 0 or states[0].current == null) return 0;
    var frequency: u32 = 0;
    for (states[0].current.?.positions) |start_position| {
        var matches = true;
        for (states[1..], 1..) |state, term_offset| {
            const expected = start_position +| @as(u32, @intCast(term_offset));
            if (!query_mod.positionWithinSlopForScoring(state.current.?.positions, expected, 0)) {
                matches = false;
                break;
            }
        }
        if (matches) frequency +|= 1;
    }
    return frequency;
}

fn exactTwoTermPackedPhraseFrequency(
    first: inverted.PackedPositionView,
    second: inverted.PackedPositionView,
) !u32 {
    var first_cursor = try first.cursor();
    var second_cursor = try second.cursor();
    var first_position = try first_cursor.next();
    var second_position = try second_cursor.next();
    var frequency: u32 = 0;
    while (first_position != null and second_position != null) {
        const expected = first_position.? +| 1;
        if (second_position.? < expected) {
            second_position = try second_cursor.next();
        } else if (second_position.? > expected) {
            first_position = try first_cursor.next();
        } else {
            frequency +|= 1;
            first_position = try first_cursor.next();
            second_position = try second_cursor.next();
        }
    }
    return frequency;
}

fn executePhrase(
    alloc: Allocator,
    snap: *const index_mod.IndexSnapshot,
    pq: PhraseQuery,
    request: SearchRequest,
) !SearchResult {
    const analyzer = pq.analyzer orelse &analysis_mod.default_analyzer;
    const filter = (try buildPhraseFilter(alloc, pq.field, pq.text, analyzer, pq.max_edits, pq.auto_fuzzy)) orelse {
        return .{ .alloc = alloc, .hits = &.{}, .total_hits = 0 };
    };
    defer freePhraseFilterTerms(alloc, filter);
    return switch (filter) {
        .phrase => |phrase_filter| executeScoredPhraseFilter(alloc, snap, phrase_filter, request, pq.boost),
        else => executeFilterQuery(alloc, snap, filter, request, pq.boost),
    };
}

fn executeTermPhrase(
    alloc: Allocator,
    snap: *const index_mod.IndexSnapshot,
    pq: TermPhraseQuery,
    request: SearchRequest,
) !SearchResult {
    if (pq.terms.len == 0) {
        return .{ .alloc = alloc, .hits = &.{}, .total_hits = 0 };
    }
    return executeScoredPhraseFilter(alloc, snap, .{
        .field = pq.field,
        .terms = pq.terms,
        .slop = 0,
        .max_edits = pq.max_edits,
        .auto_fuzzy = pq.auto_fuzzy,
    }, request, pq.boost);
}

fn executeMultiPhrase(
    alloc: Allocator,
    snap: *const index_mod.IndexSnapshot,
    pq: MultiPhraseQuery,
    request: SearchRequest,
) !SearchResult {
    if (pq.terms.len == 0) {
        return .{ .alloc = alloc, .hits = &.{}, .total_hits = 0 };
    }
    return executeFilterQuery(alloc, snap, .{ .multi_phrase = .{
        .field = pq.field,
        .term_alternatives = pq.terms,
        .slop = 0,
        .max_edits = pq.max_edits,
        .auto_fuzzy = pq.auto_fuzzy,
    } }, request, pq.boost);
}

fn executePrefix(
    alloc: Allocator,
    snap: *const index_mod.IndexSnapshot,
    pq: PrefixQuery,
    request: SearchRequest,
) !SearchResult {
    return executeFilterQuery(alloc, snap, .{ .prefix = .{
        .field = pq.field,
        .prefix = pq.prefix,
        .indexed_field = pq.indexed_field,
    } }, request, pq.boost);
}

fn executeFuzzy(
    alloc: Allocator,
    snap: *const index_mod.IndexSnapshot,
    fq: FuzzyQuery,
    request: SearchRequest,
) !SearchResult {
    const effective_edits: u8 = if (fq.auto_fuzzy)
        (if (fq.term.len > 5) 2 else if (fq.term.len > 2) 1 else 0)
    else
        fq.max_edits;
    return executeFilterQuery(alloc, snap, .{ .fuzzy = .{
        .field = fq.field,
        .term = fq.term,
        .max_edits = effective_edits,
        .prefix_len = fq.prefix_len,
    } }, request, fq.boost);
}

fn executeNumericRange(
    alloc: Allocator,
    snap: *const index_mod.IndexSnapshot,
    rq: NumericRangeQuery,
    request: SearchRequest,
) !SearchResult {
    return executeFilterQuery(alloc, snap, .{ .range = .{
        .field = rq.field,
        .min_val = rq.min,
        .max_val = rq.max,
        .inclusive_min = rq.inclusive_min,
        .inclusive_max = rq.inclusive_max,
    } }, request, rq.boost);
}

fn executeDateRange(
    alloc: Allocator,
    snap: *const index_mod.IndexSnapshot,
    rq: DateRangeQuery,
    request: SearchRequest,
) !SearchResult {
    return executeFilterQuery(alloc, snap, .{ .date_range = .{
        .field = rq.field,
        .start_ns = rq.start_ns,
        .end_ns = rq.end_ns,
        .inclusive_start = rq.inclusive_start,
        .inclusive_end = rq.inclusive_end,
    } }, request, rq.boost);
}

fn executeDocID(
    alloc: Allocator,
    snap: *const index_mod.IndexSnapshot,
    dq: DocIdQuery,
    request: SearchRequest,
) !SearchResult {
    return executeFilterQuery(alloc, snap, .{ .doc_id = .{ .doc_ids = dq.ids } }, request, dq.boost);
}

fn executeDocNum(
    alloc: Allocator,
    snap: *const index_mod.IndexSnapshot,
    dq: DocNumQuery,
    request: SearchRequest,
) !SearchResult {
    return executeFilterQuery(alloc, snap, .{ .doc_num = .{ .doc_nums = dq.ids, .bitmap = dq.bitmap, .producer = dq.producer } }, request, dq.boost);
}

fn executeBoolField(
    alloc: Allocator,
    snap: *const index_mod.IndexSnapshot,
    bq: BoolFieldQuery,
    request: SearchRequest,
) !SearchResult {
    return executeFilterQuery(alloc, snap, .{ .bool_field = .{
        .field = bq.field,
        .value = bq.value,
    } }, request, bq.boost);
}

fn executeGeoDistance(
    alloc: Allocator,
    snap: *const index_mod.IndexSnapshot,
    gq: GeoDistanceQuery,
    request: SearchRequest,
) !SearchResult {
    return executeFilterQuery(alloc, snap, .{ .geo_distance = .{
        .field = gq.field,
        .center = gq.center,
        .radius_meters = gq.radius_meters,
    } }, request, gq.boost);
}

fn executeGeoBBox(
    alloc: Allocator,
    snap: *const index_mod.IndexSnapshot,
    gq: GeoBBoxQuery,
    request: SearchRequest,
) !SearchResult {
    return executeFilterQuery(alloc, snap, .{ .geo_bbox = .{
        .field = gq.field,
        .min_lat = gq.min_lat,
        .min_lon = gq.min_lon,
        .max_lat = gq.max_lat,
        .max_lon = gq.max_lon,
    } }, request, gq.boost);
}

fn executeTermRange(
    alloc: Allocator,
    snap: *const index_mod.IndexSnapshot,
    rq: TermRangeQuery,
    request: SearchRequest,
) !SearchResult {
    return executeFilterQuery(alloc, snap, .{ .term_range = .{
        .field = rq.field,
        .min = rq.min,
        .max = rq.max,
        .inclusive_min = rq.inclusive_min,
        .inclusive_max = rq.inclusive_max,
    } }, request, rq.boost);
}

fn executeIPRange(
    alloc: Allocator,
    snap: *const index_mod.IndexSnapshot,
    iq: IPRangeQuery,
    request: SearchRequest,
) !SearchResult {
    return executeFilterQuery(alloc, snap, .{ .ip_range = .{
        .field = iq.field,
        .cidr = iq.cidr,
    } }, request, iq.boost);
}

fn executeGeoShape(
    alloc: Allocator,
    snap: *const index_mod.IndexSnapshot,
    gq: GeoShapeQuery,
    request: SearchRequest,
) !SearchResult {
    return executeFilterQuery(alloc, snap, .{ .geo_shape = .{
        .field = gq.field,
        .relation = geoShapeFilterRelation(gq.relation),
        .polygons = gq.polygons,
    } }, request, gq.boost);
}

fn geoShapeFilterRelation(relation: GeoShapeRelation) query_mod.GeoShapeRelation {
    return switch (relation) {
        .intersects => .intersects,
        .within => .within,
        .contains => .contains,
    };
}

fn executeWildcard(
    alloc: Allocator,
    snap: *const index_mod.IndexSnapshot,
    wq: WildcardQuery,
    request: SearchRequest,
) !SearchResult {
    return executeFilterQuery(alloc, snap, .{ .wildcard = .{
        .field = wq.field,
        .pattern = wq.pattern,
    } }, request, wq.boost);
}

fn executeRegexp(
    alloc: Allocator,
    snap: *const index_mod.IndexSnapshot,
    rq: RegexpQuery,
    request: SearchRequest,
) !SearchResult {
    return executeFilterQuery(alloc, snap, .{ .regexp = .{
        .field = rq.field,
        .pattern = rq.pattern,
    } }, request, rq.boost);
}

fn executeMatchAll(
    alloc: Allocator,
    snap: *const index_mod.IndexSnapshot,
    request: SearchRequest,
) !SearchResult {
    // Use filter to get all doc IDs
    const filter = query_mod.Filter{ .match_all = {} };
    const raw_doc_ids = try snap.executeFilter(alloc, filter);
    defer alloc.free(raw_doc_ids);

    // Unlike every other query type, match_all never goes through the
    // scorer/collector path (FastTopK et al.) that consults
    // filter_doc_nums/exclude_doc_nums, and it is not always wrapped in a
    // bool_query whose executeBoolAllHit fallback re-applies those
    // constraints as a safety net. Apply them directly here so a match_all
    // query never silently ignores a filter_query/exclusion_query (or a
    // bool.must_not) that the caller resolved into native doc numbers.
    var owned_filtered: ?[]u32 = null;
    defer if (owned_filtered) |owned| alloc.free(owned);
    const doc_ids: []const u32 = if (!requestHasDocNumConstraints(request))
        raw_doc_ids
    else blk: {
        var filtered = try std.ArrayListUnmanaged(u32).initCapacity(alloc, raw_doc_ids.len);
        errdefer filtered.deinit(alloc);
        for (raw_doc_ids) |doc_id| {
            if (requestAllowsDocNum(request, doc_id)) filtered.appendAssumeCapacity(doc_id);
        }
        const owned = try filtered.toOwnedSlice(alloc);
        owned_filtered = owned;
        break :blk owned;
    };

    // Build hits (no scoring, all score 1.0)
    const total: u32 = @intCast(doc_ids.len);
    const start = @min(request.offset, total);
    const end = @min(start + request.k, total);

    var hits = try alloc.alloc(ScoredHit, end - start);
    errdefer alloc.free(hits);

    for (start..end) |i| {
        const global_id = doc_ids[i];
        const hit = ScoredHit{
            .doc_id = global_id,
            .score = 1.0,
            .id = null,
            .stored_data = null,
        };

        hits[i - start] = hit;
    }

    errdefer freeStoredBodies(alloc, hits);
    if (request.include_stored) try populateStoredHits(alloc, snap, hits, request.diagnostics);

    // Collect aggregations over ALL matching docs (not just the page)
    var agg_results: []NamedAggResult = &.{};
    if (request.aggregations.len > 0) {
        var all_scored = try alloc.alloc(scorer_mod.ScoredHit, doc_ids.len);
        defer alloc.free(all_scored);
        for (doc_ids, 0..) |did, i| {
            all_scored[i] = .{ .doc_id = did, .score = 1.0 };
        }
        agg_results = try collectAggregations(alloc, snap, all_scored, request.aggregations);
    }

    return .{ .alloc = alloc, .hits = hits, .total_hits = total, .aggregations = agg_results };
}

fn executeMatchNone(
    alloc: Allocator,
    request: SearchRequest,
) !SearchResult {
    _ = request;
    return .{
        .alloc = alloc,
        .hits = try alloc.alloc(ScoredHit, 0),
        .total_hits = 0,
    };
}

const ScoreMap = std.AutoHashMapUnmanaged(u32, f32);

fn executeQueryAllScored(
    alloc: Allocator,
    snap: *const index_mod.IndexSnapshot,
    query: SearchQuery,
    request: SearchRequest,
) ![]scorer_mod.ScoredHit {
    var sub_request = request;
    sub_request.query = query;
    sub_request.k = snap.liveDocCount();
    sub_request.offset = 0;
    sub_request.include_stored = false;
    sub_request.aggregations = &.{};
    sub_request.sort = null;
    sub_request.search_after = null;
    sub_request.graph_queries = &.{};

    var result = try execute(alloc, snap, sub_request);
    defer result.deinit();

    var scored = try alloc.alloc(scorer_mod.ScoredHit, result.hits.len);
    errdefer alloc.free(scored);
    for (result.hits, 0..) |hit, i| {
        scored[i] = .{
            .doc_id = hit.doc_id,
            .score = hit.score,
        };
    }
    return scored;
}

fn scoreMapFromHits(alloc: Allocator, hits: []const scorer_mod.ScoredHit) !ScoreMap {
    var map: ScoreMap = .{};
    errdefer map.deinit(alloc);
    for (hits) |hit| {
        const entry = try map.getOrPut(alloc, hit.doc_id);
        if (entry.found_existing) {
            entry.value_ptr.* += hit.score;
        } else {
            entry.value_ptr.* = hit.score;
        }
    }
    return map;
}

fn buildAllDocsScoreMap(alloc: Allocator, snap: *const index_mod.IndexSnapshot) !ScoreMap {
    var map: ScoreMap = .{};
    errdefer map.deinit(alloc);
    const doc_ids = try snap.executeFilter(alloc, .{ .match_all = {} });
    defer alloc.free(doc_ids);
    for (doc_ids) |doc_id| {
        try map.put(alloc, doc_id, 1.0);
    }
    return map;
}

fn buildOptionalShouldBaseScoreMap(
    alloc: Allocator,
    snap: *const index_mod.IndexSnapshot,
    request: SearchRequest,
) !ScoreMap {
    var map: ScoreMap = .{};
    errdefer map.deinit(alloc);
    const doc_ids = if (request.filter_doc_nums_positive)
        try snap.executeFilter(alloc, .{ .doc_num = .{
            .doc_nums = request.filter_doc_nums,
        } })
    else
        try snap.executeFilter(alloc, .{ .match_all = {} });
    defer alloc.free(doc_ids);
    for (doc_ids) |doc_id| {
        if (!containsSortedU32(request.exclude_doc_nums, doc_id)) {
            try map.put(alloc, doc_id, 0.0);
        }
    }
    return map;
}

fn addScoresFromHits(alloc: Allocator, map: *ScoreMap, hits: []const scorer_mod.ScoredHit) !void {
    for (hits) |hit| {
        const entry = try map.getOrPut(alloc, hit.doc_id);
        if (entry.found_existing) {
            entry.value_ptr.* += hit.score;
        } else {
            entry.value_ptr.* = hit.score;
        }
    }
}

fn intersectScoresWithHits(alloc: Allocator, map: *ScoreMap, hits: []const scorer_mod.ScoredHit) !void {
    var other = try scoreMapFromHits(alloc, hits);
    defer other.deinit(alloc);

    var to_remove = std.ArrayListUnmanaged(u32).empty;
    defer to_remove.deinit(alloc);

    var it = map.iterator();
    while (it.next()) |entry| {
        if (other.get(entry.key_ptr.*)) |score| {
            entry.value_ptr.* += score;
        } else {
            try to_remove.append(alloc, entry.key_ptr.*);
        }
    }

    for (to_remove.items) |doc_id| {
        _ = map.remove(doc_id);
    }
}

fn addOptionalScoresFromHits(alloc: Allocator, map: *ScoreMap, hits: []const scorer_mod.ScoredHit) !void {
    var other = try scoreMapFromHits(alloc, hits);
    defer other.deinit(alloc);

    var it = map.iterator();
    while (it.next()) |entry| {
        if (other.get(entry.key_ptr.*)) |score| {
            entry.value_ptr.* += score;
        }
    }
}

fn subtractScoresFromHits(alloc: Allocator, map: *ScoreMap, hits: []const scorer_mod.ScoredHit) !void {
    var other = try scoreMapFromHits(alloc, hits);
    defer other.deinit(alloc);

    var to_remove = std.ArrayListUnmanaged(u32).empty;
    defer to_remove.deinit(alloc);

    var it = map.iterator();
    while (it.next()) |entry| {
        if (other.contains(entry.key_ptr.*)) {
            try to_remove.append(alloc, entry.key_ptr.*);
        }
    }

    for (to_remove.items) |doc_id| {
        _ = map.remove(doc_id);
    }
}

fn requestHasDocNumConstraints(request: SearchRequest) bool {
    return request.filter_doc_bitmap != null or request.exclude_doc_bitmap != null or request.filter_doc_nums_positive or request.exclude_doc_nums.len > 0;
}

fn requestAllowsDocNum(request: SearchRequest, doc_id: u32) bool {
    if (request.filter_doc_bitmap) |bitmap| if (!bitmap.contains(doc_id)) return false;
    if (request.exclude_doc_bitmap) |bitmap| if (bitmap.contains(doc_id)) return false;
    if (request.filter_doc_nums_positive and !containsSortedU32(request.filter_doc_nums, doc_id)) return false;
    if (containsSortedU32(request.exclude_doc_nums, doc_id)) return false;
    return true;
}

fn applyDocNumConstraintsToScoreMap(
    alloc: Allocator,
    map: *ScoreMap,
    request: SearchRequest,
) !void {
    if (!requestHasDocNumConstraints(request)) return;

    var to_remove = std.ArrayListUnmanaged(u32).empty;
    defer to_remove.deinit(alloc);

    var it = map.iterator();
    while (it.next()) |entry| {
        if (!requestAllowsDocNum(request, entry.key_ptr.*)) {
            try to_remove.append(alloc, entry.key_ptr.*);
        }
    }
    for (to_remove.items) |doc_id| {
        _ = map.remove(doc_id);
    }
}

fn scoreMapToSortedHits(alloc: Allocator, map: ScoreMap, boost: f32) ![]scorer_mod.ScoredHit {
    var hits = try alloc.alloc(scorer_mod.ScoredHit, map.count());
    errdefer alloc.free(hits);

    var i: usize = 0;
    var it = map.iterator();
    while (it.next()) |entry| : (i += 1) {
        hits[i] = .{
            .doc_id = entry.key_ptr.*,
            .score = entry.value_ptr.* * boost,
        };
    }

    std.mem.sort(scorer_mod.ScoredHit, hits, {}, struct {
        fn cmp(_: void, a: scorer_mod.ScoredHit, b: scorer_mod.ScoredHit) bool {
            if (a.score == b.score) return a.doc_id < b.doc_id;
            return a.score > b.score;
        }
    }.cmp);

    return hits;
}

const SimpleTextTerm = struct {
    field: []const u8,
    term: []const u8,
    boost: f32,
};

const FastTermState = struct {
    iter: inverted.PostingsIterator,
    current: ?inverted.PostingsIterator.Hit = null,
    doc_freq: u32,
    idf: f32,
    boost: f32,
    block_max: ?inverted.BlockMaxInfo,
    chunk_size: u32,
    block_cursor: ?inverted.PostingsIterator.BlockCursor = null,
    exhausted: bool = false,

    pub fn deinit(self: *FastTermState) void {
        self.iter.deinit();
    }

    fn next(self: *FastTermState) !void {
        self.current = try self.iter.next();
        self.exhausted = self.current == null;
    }

    fn advanceTo(self: *FastTermState, target: u32) !void {
        if (self.exhausted or self.current.?.doc_id >= target) return;
        self.current = try self.iter.advanceTo(target);
        self.exhausted = self.current == null;
    }
};

const ProducerConstraints = struct {
    include: ?query_mod.DocNumProducer = null,
    exclude: ?query_mod.DocNumProducer = null,
    fn present(self: @This()) bool {
        return self.include != null or self.exclude != null;
    }
};

const FastTopK = struct {
    alloc: Allocator,
    k: u32,
    filter_doc_bitmap: ?*const roaring.RoaringBitmap = null,
    exclude_doc_bitmap: ?*const roaring.RoaringBitmap = null,
    filter_doc_nums: []const u32 = &.{},
    filter_doc_nums_positive: bool = false,
    exclude_doc_nums: []const u32 = &.{},
    hits: std.ArrayListUnmanaged(scorer_mod.ScoredHit) = .empty,
    total_count: u32 = 0,
    worst_index: usize = 0,
    pruned: bool = false,
    producers: ProducerConstraints = .{},
    segment_offset: u32 = 0,
    segment_count: u32 = 0,
    pending: [64]scorer_mod.ScoredHit = undefined,
    pending_count: usize = 0,

    fn complete(producer: ?query_mod.DocNumProducer) ?*const roaring.RoaringBitmap {
        const value = producer orelse return null;
        const get = value.materialized orelse return null;
        return get(value.ptr);
    }
    fn nextAllowed(self: *const FastTopK, gate: *BitmapGate, first: u32) u64 {
        var includes: [2]*const roaring.RoaringBitmap = undefined;
        var excludes: [2]*const roaring.RoaringBitmap = undefined;
        var ni: usize = 0;
        var ne: usize = 0;
        for ([_]?*const roaring.RoaringBitmap{ self.filter_doc_bitmap, complete(self.producers.include) }) |maybe| if (maybe) |bitmap| {
            includes[ni] = bitmap;
            ni += 1;
        };
        for ([_]?*const roaring.RoaringBitmap{ self.exclude_doc_bitmap, complete(self.producers.exclude) }) |maybe| if (maybe) |bitmap| {
            excludes[ne] = bitmap;
            ne += 1;
        };
        if (first >= gate.end) return 0x1_0000_0000;
        const target = roaring.RoaringBitmap.candidateLowerBound(first, gate.end, includes[0..ni], excludes[0..ne]);
        return if (target == gate.end) 0x1_0000_0000 else target;
    }

    fn beginSegment(self: *FastTopK, offset: u32, count: u32) !void {
        try self.flushPending();
        self.segment_offset = offset;
        self.segment_count = count;
    }
    fn flushPending(self: *FastTopK) !void {
        if (self.pending_count == 0) return;
        var candidates = roaring.RoaringBitmap.init(self.alloc);
        defer candidates.deinit();
        for (self.pending[0..self.pending_count]) |hit| try candidates.add(hit.doc_id - self.segment_offset);
        // Complete global membership is immutable for the request. Borrow it
        // directly instead of copying a full segment bitmap for every batch.
        const include_global = complete(self.producers.include);
        const exclude_global = complete(self.producers.exclude);
        var include = if (include_global == null) if (self.producers.include) |producer| try producer.produce(producer.ptr, self.alloc, self.segment_offset, self.segment_count, &candidates) else null else null;
        defer if (include) |*bitmap| bitmap.deinit();
        var exclude = if (exclude_global == null) if (self.producers.exclude) |producer| try producer.produce(producer.ptr, self.alloc, self.segment_offset, self.segment_count, &candidates) else null else null;
        defer if (exclude) |*bitmap| bitmap.deinit();
        for (self.pending[0..self.pending_count]) |hit| {
            const local_id = hit.doc_id - self.segment_offset;
            if (include_global) |bitmap| if (!bitmap.contains(hit.doc_id)) continue;
            if (exclude_global) |bitmap| if (bitmap.contains(hit.doc_id)) continue;
            if (include) |*bitmap| if (!bitmap.contains(local_id)) continue;
            if (exclude) |*bitmap| if (bitmap.contains(local_id)) continue;
            try self.collectAdmitted(hit);
        }
        self.pending_count = 0;
    }

    pub fn deinit(self: *FastTopK) void {
        self.hits.deinit(self.alloc);
    }

    pub fn collect(self: *FastTopK, doc_id: u32, score: f32) !void {
        if (!self.allows(doc_id)) return;
        const hit = scorer_mod.ScoredHit{ .doc_id = doc_id, .score = score };
        if (self.producers.present()) {
            self.pending[self.pending_count] = hit;
            self.pending_count += 1;
            if (self.pending_count == self.pending.len) try self.flushPending();
            return;
        }
        try self.collectAdmitted(hit);
    }
    fn collectAdmitted(self: *FastTopK, hit: scorer_mod.ScoredHit) !void {
        self.total_count += 1;
        if (self.k == 0) return;
        if (self.hits.items.len < self.k) {
            try self.hits.append(self.alloc, hit);
            if (self.hits.items.len == self.k) self.refreshWorst();
            return;
        }
        if (fastHitBetter(hit, self.hits.items[self.worst_index])) {
            self.hits.items[self.worst_index] = hit;
            self.refreshWorst();
        }
    }

    fn allows(self: *const FastTopK, doc_id: u32) bool {
        if (self.filter_doc_bitmap) |bitmap| if (!bitmap.contains(doc_id)) return false;
        if (self.exclude_doc_bitmap) |bitmap| if (bitmap.contains(doc_id)) return false;
        if (self.filter_doc_nums_positive and !containsSortedU32(self.filter_doc_nums, doc_id)) return false;
        if (containsSortedU32(self.exclude_doc_nums, doc_id)) return false;
        return true;
    }

    fn finish(self: *FastTopK) ![]scorer_mod.ScoredHit {
        try self.flushPending();
        if (self.k > 0 and self.hits.items.len < self.k) self.pruned = false;
        scorer_mod.sortScoredHits(self.hits.items);
        return try self.alloc.dupe(scorer_mod.ScoredHit, self.hits.items);
    }

    fn minCompetitiveScore(self: *const FastTopK) f32 {
        if (self.k == 0 or self.hits.items.len < self.k) return 0;
        return self.hits.items[self.worst_index].score;
    }

    fn worstCompetitiveDocId(self: *const FastTopK) ?u32 {
        if (self.k == 0 or self.hits.items.len < self.k) return null;
        return self.hits.items[self.worst_index].doc_id;
    }

    fn refreshWorst(self: *FastTopK) void {
        self.worst_index = 0;
        for (self.hits.items[1..], 1..) |candidate, i| {
            if (fastHitWorse(candidate, self.hits.items[self.worst_index])) self.worst_index = i;
        }
    }
};

fn fastHitBetter(left: scorer_mod.ScoredHit, right: scorer_mod.ScoredHit) bool {
    if (left.score != right.score) return left.score > right.score;
    return left.doc_id < right.doc_id;
}

fn fastHitWorse(left: scorer_mod.ScoredHit, right: scorer_mod.ScoredHit) bool {
    if (left.score != right.score) return left.score < right.score;
    return left.doc_id > right.doc_id;
}

fn containsSortedU32(items: []const u32, value: u32) bool {
    return std.sort.binarySearch(u32, items, value, compareU32) != null;
}

fn compareU32(expected: u32, item: u32) std.math.Order {
    return std.math.order(expected, item);
}

fn appendSimpleTextTerms(alloc: Allocator, out: *std.ArrayListUnmanaged(SimpleTextTerm), query: SearchQuery) !bool {
    switch (query) {
        .term => |tq| {
            try out.append(alloc, .{ .field = tq.field, .term = tq.term, .boost = tq.boost });
            return true;
        },
        .match => |mq| {
            const analyzer = mq.analyzer orelse &analysis_mod.default_analyzer;
            const tokens = try analyzer.analyze(alloc, mq.text);
            defer analysis_mod.Analyzer.freeTokens(alloc, tokens);
            if (tokens.len == 0) return false;

            for (tokens, 0..) |tok, i| {
                var duplicate = false;
                for (tokens[0..i]) |prev| {
                    if (std.mem.eql(u8, prev.term, tok.term)) {
                        duplicate = true;
                        break;
                    }
                }
                if (duplicate) continue;
                try out.append(alloc, .{
                    .field = mq.field,
                    .term = try alloc.dupe(u8, tok.term),
                    .boost = mq.boost,
                });
            }
            return true;
        },
        else => return false,
    }
}

fn simpleTermsField(terms: []const SimpleTextTerm, current: ?[]const u8) ?[]const u8 {
    var field = current;
    for (terms) |term| {
        if (field) |existing| {
            if (!std.mem.eql(u8, existing, term.field)) return null;
        } else {
            field = term.field;
        }
    }
    return field;
}

fn initFastTermStates(
    alloc: Allocator,
    snap: *const index_mod.IndexSnapshot,
    inv_reader: anytype,
    field: []const u8,
    terms: []const SimpleTextTerm,
    require_all_terms: bool,
    scoring_stats: ?distributed_stats_mod.TextFieldStats,
) !?[]FastTermState {
    var states = std.ArrayListUnmanaged(FastTermState).empty;
    var success = false;
    defer if (!success) {
        for (states.items) |*state| state.deinit();
        states.deinit(alloc);
    };
    const scoring_doc_count = if (scoring_stats) |stats| stats.global_doc_count else snap.scoringDocCount();

    // Reject impossible conjunctions before reading corpus-wide scoring metadata.
    // Retain lookups so accepted terms do not repeat dictionary navigation.
    const lookups = try alloc.alloc(?inverted.LookupResult, terms.len);
    defer alloc.free(lookups);
    for (terms, lookups) |term, *lookup| {
        lookup.* = try inv_reader.lookup(term.term);
        if (require_all_terms and lookup.* == null) return null;
    }
    const names = try alloc.alloc([]const u8, terms.len);
    defer alloc.free(names);
    const frequencies = try alloc.alloc(u32, terms.len);
    defer alloc.free(frequencies);
    var present: usize = 0;
    for (terms, lookups) |term, found| if (found != null) {
        names[present] = term.term;
        present += 1;
    };
    if (scoring_stats) |stats| {
        for (names[0..present], frequencies[0..present]) |name, *frequency| frequency.* = stats.termDocFreq(name) orelse return error.InvalidArgument;
    } else if (present != 0) try snap.termDocFreqs(alloc, field, names[0..present], frequencies[0..present]);
    var frequency_index: usize = 0;
    for (terms, lookups) |term, found| {
        const lookup_result = found orelse {
            if (require_all_terms) return null;
            continue;
        };
        const df = frequencies[frequency_index];
        frequency_index += 1;
        if (df == 0) {
            if (require_all_terms) return null;
            continue;
        }
        var iter = try lookup_result.iterator(alloc);
        // Simple boolean scoring uses frequency and norm only. Avoid walking
        // position varints just as the WAND scorer does for ranking-only term
        // queries.
        iter.decode_positions = false;
        var state = FastTermState{
            .iter = iter,
            .doc_freq = df,
            .idf = inverted.bm25Idf(scoring_doc_count, df),
            .boost = term.boost,
            .block_max = switch (lookup_result) {
                .postings => |postings| postings.block_max,
                .one_hit => null,
            },
            .chunk_size = switch (lookup_result) {
                .postings => |postings| postings.scoringChunkSize(),
                .one_hit => 0,
            },
        };
        states.append(alloc, state) catch |err| {
            state.deinit();
            return err;
        };
    }

    if (require_all_terms and states.items.len != terms.len) return null;
    const out = try states.toOwnedSlice(alloc);
    success = true;
    var initialized = false;
    defer if (!initialized) deinitFastTermStates(alloc, out);
    for (out) |*state| {
        try state.next();
        if (state.exhausted and require_all_terms) return null;
    }
    initialized = true;
    return out;
}

fn deinitFastTermStates(alloc: Allocator, states: []FastTermState) void {
    for (states) |*state| state.deinit();
    alloc.free(states);
}

fn scoreFastTerm(
    state: FastTermState,
    hit: inverted.PostingsIterator.Hit,
    global_doc_count: u32,
    avg_dl: f32,
    bm25_config: inverted.BM25Config,
) f32 {
    return inverted.bm25Score(hit.freq, hit.norm, global_doc_count, state.doc_freq, avg_dl, bm25_config) * state.boost;
}

fn isSegmentDocDeleted(seg: *const index_mod.SegmentEntry, doc_id: u32) bool {
    // The segment-scoring callers hold the shared deletion lock for the whole
    // postings walk, avoiding a lock/unlock for every candidate document.
    if (seg.shared.deleted) |deleted| return deleted.contains(doc_id);
    return false;
}

fn isProhibited(states: []FastTermState, doc_id: u32) !bool {
    for (states) |*state| {
        try state.advanceTo(doc_id);
        if (!state.exhausted and state.current.?.doc_id == doc_id) return true;
    }
    return false;
}

fn collectOptionalScores(
    states: []FastTermState,
    doc_id: u32,
    global_doc_count: u32,
    avg_dl: f32,
    bm25_config: inverted.BM25Config,
) !struct { count: u32, score: f32 } {
    var count: u32 = 0;
    var score: f32 = 0;
    for (states) |*state| {
        try state.advanceTo(doc_id);
        if (!state.exhausted and state.current.?.doc_id == doc_id) {
            count += 1;
            score += scoreFastTerm(state.*, state.current.?, global_doc_count, avg_dl, bm25_config);
        }
    }
    return .{ .count = count, .score = score };
}

/// A monotonic bitmap/postings intersection. Selective predicates seek postings
/// directly to the next admitted document instead of decoding every match.
const BitmapGate = struct {
    iterator: ?roaring.Iterator = null,
    next: ?u32 = null,
    end: u64,
    fn init(bitmap: ?*const roaring.RoaringBitmap, offset: u32, count: u32) BitmapGate {
        var gate: BitmapGate = .{ .end = @as(u64, offset) + count };
        if (bitmap) |set| {
            gate.iterator = set.iterator();
            gate.iterator.?.seek(offset);
            gate.next = gate.iterator.?.next();
        }
        return gate;
    }
    fn target(self: *BitmapGate, current: u32) ?u32 {
        if (self.iterator == null) return current;
        if (self.next != null and self.next.? < current) self.next = self.iterator.?.seekTo(current);
        const value = self.next orelse return null;
        return if (value < self.end) value else null;
    }
};

fn collectFastShouldSegment(
    collector: *FastTopK,
    seg: *const index_mod.SegmentEntry,
    should_states: []FastTermState,
    must_not_states: []FastTermState,
    min_should: u32,
    doc_offset: u32,
    global_doc_count: u32,
    avg_dl: f32,
    bm25_config: inverted.BM25Config,
    boost: f32,
) !void {
    var gate = BitmapGate.init(collector.filter_doc_bitmap, doc_offset, seg.reader.doc_count);
    while (true) {
        var min_doc: ?u32 = null;
        for (should_states) |state| {
            if (state.exhausted) continue;
            const doc_id = state.current.?.doc_id;
            if (min_doc == null or doc_id < min_doc.?) min_doc = doc_id;
        }
        const doc_id = min_doc orelse break;
        const admitted64 = collector.nextAllowed(&gate, doc_offset + doc_id);
        if (admitted64 >= gate.end) break;
        const admitted: u32 = @intCast(admitted64);
        if (admitted > doc_offset + doc_id) {
            for (should_states) |*state| if (!state.exhausted) {
                try state.advanceTo(admitted - doc_offset);
            };
            continue;
        }

        var should_count: u32 = 0;
        var score: f32 = 0;
        for (should_states) |*state| {
            if (state.exhausted or state.current.?.doc_id != doc_id) continue;
            should_count += 1;
            score += scoreFastTerm(state.*, state.current.?, global_doc_count, avg_dl, bm25_config);
            try state.next();
        }

        if (should_count >= min_should and
            !isSegmentDocDeleted(seg, doc_id) and
            !(try isProhibited(must_not_states, doc_id)))
        {
            try collector.collect(doc_offset + doc_id, score * boost);
        }
    }
}

/// Scan common conjunction blocks using compact metadata only. Rejected
/// blocks never decode their postings payload; all term iterators are loaded
/// together only when their summed BM25 ceiling can enter the current top-k.
fn skipNonCompetitiveConjunctionBlocks(
    collector: *FastTopK,
    must_states: []FastTermState,
    current_doc: u32,
    doc_offset: u32,
    avg_dl: f32,
    bm25_config: inverted.BM25Config,
    boost: f32,
    diagnostics: ?*scorer_mod.SearchDiagnostics,
) !bool {
    const threshold = collector.minCompetitiveScore();
    const worst_doc_id = collector.worstCompetitiveDocId() orelse return false;
    if (threshold <= 0 or must_states.len == 0) return false;

    for (must_states) |*state| {
        if (!state.iter.doc_range_aligned) {
            return skipNonCompetitiveConjunctionPostingBlocks(
                collector,
                must_states,
                current_doc,
                doc_offset,
                avg_dl,
                bm25_config,
                boost,
                diagnostics,
            );
        }
    }

    var chunk_size: u32 = 0;
    for (must_states) |*state| {
        if (state.block_max == null or state.chunk_size == 0) return false;
        if (chunk_size == 0) chunk_size = state.chunk_size else if (state.chunk_size != chunk_size) return false;
        state.block_cursor = state.iter.currentBlockCursor() orelse return false;
    }

    var rejected_blocks: u64 = 0;
    var current_block = true;
    while (true) {
        // Sparse terms need not store metadata for every logical block. Seek
        // the metadata cursors to the next block present in every required
        // term; a block absent from one term cannot contain a conjunction.
        var candidate_chunk: u32 = 0;
        for (must_states) |state| candidate_chunk = @max(candidate_chunk, state.block_cursor.?.chunk_id);
        while (true) {
            var candidate_changed = false;
            for (must_states) |*state| {
                while (state.block_cursor.?.chunk_id < candidate_chunk) {
                    if (!try state.iter.advanceBlockCursor(&state.block_cursor.?)) {
                        state.block_cursor = null;
                        for (must_states) |*other| {
                            other.current = null;
                            other.exhausted = true;
                        }
                        collector.pruned = true;
                        if (diagnostics) |diag| diag.boolean_chunks_skipped +|= rejected_blocks;
                        return true;
                    }
                }
                if (state.block_cursor.?.chunk_id > candidate_chunk) {
                    candidate_chunk = state.block_cursor.?.chunk_id;
                    candidate_changed = true;
                }
            }
            if (!candidate_changed) break;
        }

        var bound: f32 = 0;
        for (must_states) |state| {
            const cursor = state.block_cursor.?;
            bound += state.iter.blockCursorImpactWithIdf(
                state.block_max.?,
                cursor,
                avg_dl,
                state.idf,
                bm25_config,
            ) * state.boost * boost;
        }
        // Within the current block, monotonic conjunction iteration proves
        // every remaining match is at or after current_doc. For later blocks,
        // the aligned logical block start is the conservative tie boundary.
        const earliest_candidate = if (current_block)
            @as(u64, doc_offset) + current_doc
        else
            @as(u64, doc_offset) + @as(u64, candidate_chunk) * chunk_size;
        const competitive = bound > threshold or
            (bound == threshold and earliest_candidate < worst_doc_id);
        if (competitive) {
            if (rejected_blocks == 0) return false;
            for (must_states) |*state| {
                state.current = try state.iter.loadBlockCursor(state.block_cursor.?);
                state.exhausted = state.current == null;
            }
            collector.pruned = true;
            if (diagnostics) |diag| diag.boolean_chunks_skipped +|= rejected_blocks;
            return true;
        }

        rejected_blocks +|= 1;
        current_block = false;
        for (must_states) |*state| {
            if (!try state.iter.advanceBlockCursor(&state.block_cursor.?)) {
                state.block_cursor = null;
                for (must_states) |*other| {
                    other.current = null;
                    other.exhausted = true;
                }
                collector.pruned = true;
                if (diagnostics) |diag| diag.boolean_chunks_skipped +|= rejected_blocks;
                return true;
            }
        }
    }
}

/// v28 posting-count blocks are not aligned by a shared logical chunk ID.
/// Intersect their conservative [min_doc,max_doc] ranges instead. Blocks whose
/// ranges cannot overlap are skipped without payload decode; overlapping block
/// ceilings can be summed exactly as in the v27 aligned path.
fn skipNonCompetitiveConjunctionPostingBlocks(
    collector: *FastTopK,
    must_states: []FastTermState,
    current_doc: u32,
    doc_offset: u32,
    avg_dl: f32,
    bm25_config: inverted.BM25Config,
    boost: f32,
    diagnostics: ?*scorer_mod.SearchDiagnostics,
) !bool {
    for (must_states) |*state| {
        if (state.block_max == null) return false;
        state.block_cursor = state.iter.currentBlockCursor() orelse return false;
    }

    var rejected_blocks: u64 = 0;
    while (true) {
        var overlap_start: u32 = 0;
        var overlap_end: u32 = std.math.maxInt(u32);
        for (must_states) |state| {
            const cursor = state.block_cursor.?;
            overlap_start = @max(overlap_start, cursor.min_doc);
            overlap_end = @min(overlap_end, cursor.max_doc);
        }

        if (overlap_start > overlap_end) {
            var advanced = false;
            for (must_states) |*state| {
                if (state.block_cursor.?.max_doc >= overlap_start) continue;
                rejected_blocks +|= 1;
                if (!try state.iter.advanceBlockCursor(&state.block_cursor.?)) {
                    for (must_states) |*other| {
                        other.current = null;
                        other.exhausted = true;
                    }
                    collector.pruned = true;
                    if (diagnostics) |diag| diag.boolean_chunks_skipped +|= rejected_blocks;
                    return true;
                }
                advanced = true;
            }
            if (!advanced) return error.InvalidData;
            continue;
        }

        var bound: f32 = 0;
        for (must_states) |state| {
            bound += state.iter.blockCursorImpactWithIdf(
                state.block_max.?,
                state.block_cursor.?,
                avg_dl,
                state.idf,
                bm25_config,
            ) * state.boost * boost;
        }
        const earliest_candidate = @as(u64, doc_offset) + @max(current_doc, overlap_start);
        const threshold = collector.minCompetitiveScore();
        const worst_doc_id = collector.worstCompetitiveDocId() orelse return false;
        const competitive = bound > threshold or
            (bound == threshold and earliest_candidate < worst_doc_id);
        if (competitive) {
            if (rejected_blocks == 0) return false;
            const target = @max(current_doc, overlap_start);
            for (must_states) |*state| {
                state.current = try state.iter.loadBlockCursor(state.block_cursor.?);
                state.exhausted = state.current == null;
                if (!state.exhausted) try state.advanceTo(target);
            }
            collector.pruned = true;
            if (diagnostics) |diag| diag.boolean_chunks_skipped +|= rejected_blocks;
            return true;
        }

        // No conjunction in the overlapping range can enter top-k. Advancing
        // every block that ends at the overlap boundary makes progress while
        // retaining longer blocks that may overlap a later range.
        for (must_states) |*state| {
            if (state.block_cursor.?.max_doc != overlap_end) continue;
            rejected_blocks +|= 1;
            if (!try state.iter.advanceBlockCursor(&state.block_cursor.?)) {
                for (must_states) |*other| {
                    other.current = null;
                    other.exhausted = true;
                }
                collector.pruned = true;
                if (diagnostics) |diag| diag.boolean_chunks_skipped +|= rejected_blocks;
                return true;
            }
        }
    }
}

fn collectFastMustSegment(
    collector: *FastTopK,
    seg: *const index_mod.SegmentEntry,
    must_states: []FastTermState,
    should_states: []FastTermState,
    must_not_states: []FastTermState,
    min_should: u32,
    doc_offset: u32,
    global_doc_count: u32,
    avg_dl: f32,
    bm25_config: inverted.BM25Config,
    boost: f32,
    allow_block_pruning: bool,
    diagnostics: ?*scorer_mod.SearchDiagnostics,
) !void {
    var lead_idx: usize = 0;
    for (must_states[1..], 1..) |state, i| {
        if (state.doc_freq < must_states[lead_idx].doc_freq) lead_idx = i;
    }

    var gate = BitmapGate.init(collector.filter_doc_bitmap, doc_offset, seg.reader.doc_count);
    while (!must_states[lead_idx].exhausted) {
        const admitted64 = collector.nextAllowed(&gate, doc_offset + must_states[lead_idx].current.?.doc_id);
        if (admitted64 >= gate.end) return;
        const admitted: u32 = @intCast(admitted64);
        if (admitted > doc_offset + must_states[lead_idx].current.?.doc_id) {
            try must_states[lead_idx].advanceTo(admitted - doc_offset);
            if (must_states[lead_idx].exhausted) return;
        }
        var target = must_states[lead_idx].current.?.doc_id;
        var aligned = false;

        while (!aligned) {
            aligned = true;
            var next_target = target;
            for (must_states, 0..) |*state, i| {
                if (i == lead_idx) continue;
                try state.advanceTo(target);
                if (state.exhausted) return;
                const cur = state.current.?.doc_id;
                if (cur > target) {
                    aligned = false;
                    if (cur > next_target) next_target = cur;
                }
            }
            if (!aligned) {
                try must_states[lead_idx].advanceTo(next_target);
                if (must_states[lead_idx].exhausted) return;
                target = must_states[lead_idx].current.?.doc_id;
            }
        }

        if (allow_block_pruning and try skipNonCompetitiveConjunctionBlocks(
            collector,
            must_states,
            target,
            doc_offset,
            avg_dl,
            bm25_config,
            boost,
            diagnostics,
        )) continue;

        var score: f32 = 0;
        for (must_states) |state| {
            score += scoreFastTerm(state, state.current.?, global_doc_count, avg_dl, bm25_config);
        }

        const optional = try collectOptionalScores(should_states, target, global_doc_count, avg_dl, bm25_config);
        score += optional.score;

        if (optional.count >= min_should and
            !isSegmentDocDeleted(seg, target) and
            !(try isProhibited(must_not_states, target)))
        {
            try collector.collect(doc_offset + target, score * boost);
        }

        try must_states[lead_idx].next();
    }
}

/// Feed bounded batches into membership before raising the global WAND cutoff.
/// A pending batch only delays the cutoff; it can never prune a competitive hit.
fn collectFilteredWandSegment(alloc: Allocator, seg: *const index_mod.SegmentEntry, inv_reader: anytype, terms: []const SimpleTextTerm, frequencies: []const u32, offset: u32, request: SearchRequest, collector: *FastTopK, doc_count: u32, avg_dl: f32, bound_table: ?*const inverted.BM25BoundTable) !void {
    var wand = scorer_mod.WANDScorer.init(alloc, collector.k, doc_count, avg_dl, request.bm25_config);
    defer wand.deinit();
    if (bound_table) |table| wand.setBoundTable(table);
    for (terms, frequencies) |term, frequency| {
        const lookup = (try inv_reader.lookup(term.term)) orelse continue;
        const iter = try lookup.iterator(alloc);
        try wand.addTerm(iter, frequency, switch (lookup) {
            .postings => |p| p.block_max,
            .one_hit => null,
        }, switch (lookup) {
            .postings => |p| p.scoringChunkSize(),
            .one_hit => 1024,
        }, offset);
    }
    const Collector = struct {
        base: *FastTopK,
        segment: *const index_mod.SegmentEntry,
        offset: u32,
        gate: BitmapGate,
        pub fn nextCandidate(self: *@This(), first: u32) u64 {
            return self.base.nextAllowed(&self.gate, first);
        }
        pub fn topKLimit(self: *@This()) u32 {
            return self.base.k;
        }
        pub fn minCompetitiveScore(self: *@This()) f32 {
            return self.base.minCompetitiveScore();
        }
        pub fn worstCompetitiveDocId(self: *@This()) ?u32 {
            return self.base.worstCompetitiveDocId();
        }
        pub fn markLowerBound(self: *@This()) void {
            self.base.pruned = true;
        }
        pub fn collect(self: *@This(), hit: scorer_mod.ScoredHit) !void {
            if (!isSegmentDocDeleted(self.segment, hit.doc_id - self.offset)) try self.base.collect(hit.doc_id, hit.score);
        }
    };
    var live: Collector = .{ .base = collector, .segment = seg, .offset = offset, .gate = .init(collector.filter_doc_bitmap, offset, seg.reader.doc_count) };
    seg.shared.lockDeletionShared();
    defer seg.shared.unlockDeletionShared();
    try wand.executeInto(&live);
    try collector.flushPending();
    if (request.diagnostics) |diag| {
        diag.segments_searched +|= 1;
        diag.addWand(&wand);
    }
}

/// Query-wide statistics and segment planning are shared with unfiltered WAND.
fn collectFilteredWandQuery(alloc: Allocator, snap: *const index_mod.IndexSnapshot, field: []const u8, terms: []const SimpleTextTerm, request: SearchRequest, collector: *FastTopK, doc_count: u32, avg_dl: f32) !void {
    const names = try alloc.alloc([]const u8, terms.len);
    defer alloc.free(names);
    const frequencies = try alloc.alloc(u32, terms.len);
    defer alloc.free(frequencies);
    for (terms, names) |term, *name| name.* = term.term;
    try snap.termDocFreqs(alloc, field, names, frequencies);
    const bound_table = try snap.bm25BoundTable(avg_dl, request.bm25_config);
    const plans = try snap.planTextSegments(alloc, field, names, frequencies, doc_count, avg_dl, request.bm25_config);
    defer alloc.free(plans);
    if (request.diagnostics) |diag| diag.segments_considered +|= @intCast(plans.len);
    for (plans) |plan| {
        // Publish the preceding batch before observing the global cutoff.
        try collector.flushPending();
        const threshold = collector.minCompetitiveScore();
        if (threshold > 0 and plan.score_upper_bound < threshold) {
            collector.pruned = true;
            if (request.diagnostics) |diag| diag.segments_pruned +|= 1;
            continue;
        }
        const seg = &snap.segments[plan.segment_idx];
        try collector.beginSegment(plan.doc_offset, seg.reader.doc_count);
        seg.beginAccess();
        defer seg.endAccess();
        var inv_reader = (try seg.reader.invertedIndexScoped(alloc, field)) orelse continue;
        defer inv_reader.deinit();
        try collectFilteredWandSegment(alloc, seg, inv_reader, terms, frequencies, plan.doc_offset, request, collector, doc_count, avg_dl, bound_table);
    }
}

fn executeSimpleTextBool(
    alloc: Allocator,
    snap: *const index_mod.IndexSnapshot,
    bq: BoolQuery,
    request: SearchRequest,
) !?SearchResult {
    return executeSimpleTextBoolWithProducers(alloc, snap, bq, request, .{});
}

fn executeSimpleTextBoolWithProducers(
    alloc: Allocator,
    snap: *const index_mod.IndexSnapshot,
    bq: BoolQuery,
    request: SearchRequest,
    producers: ProducerConstraints,
) !?SearchResult {
    if (request.aggregations.len != 0 or
        request.search_after != null)
    {
        return null;
    }

    var arena = std.heap.ArenaAllocator.init(alloc);
    defer arena.deinit();
    const arena_alloc = arena.allocator();

    var must_terms = std.ArrayListUnmanaged(SimpleTextTerm).empty;
    var should_terms = std.ArrayListUnmanaged(SimpleTextTerm).empty;
    var must_not_terms = std.ArrayListUnmanaged(SimpleTextTerm).empty;

    for (bq.must) |sub_query| {
        if (!(try appendSimpleTextTerms(arena_alloc, &must_terms, sub_query))) return null;
    }
    for (bq.should) |sub_query| {
        if (!(try appendSimpleTextTerms(arena_alloc, &should_terms, sub_query))) return null;
    }
    for (bq.must_not) |sub_query| {
        if (!(try appendSimpleTextTerms(arena_alloc, &must_not_terms, sub_query))) return null;
    }

    if (must_terms.items.len == 0 and should_terms.items.len == 0) return null;

    var field: ?[]const u8 = null;
    if (must_terms.items.len > 0) field = simpleTermsField(must_terms.items, field) orelse return null;
    if (should_terms.items.len > 0) field = simpleTermsField(should_terms.items, field) orelse return null;
    if (must_not_terms.items.len > 0) field = simpleTermsField(must_not_terms.items, field) orelse return null;
    const text_field = field orelse return null;
    const live_doc_count = snap.liveDocCount();
    if (live_doc_count == 0) return .{ .alloc = alloc, .hits = &.{}, .total_hits = 0 };
    var term_names: std.ArrayListUnmanaged([]const u8) = .empty;
    for ([_][]const SimpleTextTerm{ must_terms.items, should_terms.items, must_not_terms.items }) |terms| for (terms) |term| try term_names.append(arena_alloc, term.term);
    const scoring_stats = matchingFieldStats(request.distributed_text_stats, text_field, term_names.items);
    const scoring_doc_count = if (scoring_stats) |stats| stats.global_doc_count else snap.scoringDocCount();

    const effective_min_should: u32 = if (should_terms.items.len > 0 and
        bq.min_should == 0 and
        must_terms.items.len == 0 and
        !bq.pure_should_optional) 1 else bq.min_should;
    if (effective_min_should > should_terms.items.len) {
        return .{ .alloc = alloc, .hits = try alloc.alloc(ScoredHit, 0), .total_hits = 0 };
    }
    // Optional pure-should queries require a zero-score candidate baseline.
    // The all-hit path builds that baseline from the already-resolved positive
    // document constraint, avoiding a full-index scan when a filter supplied it.
    if (bq.pure_should_optional and must_terms.items.len == 0) return null;

    // A pure, minimum-one disjunction is exactly the query shape handled by
    // the production Block-Max WAND scorer. Unconstrained requests use the
    // snapshot helper; constrained requests use its shared segment scorer below.
    // Prohibited, minimum-N, and per-term-boosted shapes retain boolean iterators.
    if (must_terms.items.len == 0 and
        must_not_terms.items.len == 0 and
        effective_min_should == 1 and
        !requestHasDocNumConstraints(request) and !producers.present())
    {
        var wand_compatible = true;
        for (should_terms.items) |term| {
            if (term.boost != 1.0) {
                wand_compatible = false;
                break;
            }
        }
        if (wand_compatible) {
            const terms = try arena_alloc.alloc([]const u8, should_terms.items.len);
            for (should_terms.items, 0..) |term, i| terms[i] = term.term;
            const results = if (scoring_stats) |stats|
                try snap.searchWithOverrideAndConfig(alloc, text_field, terms, effectiveK(request, snap), stats, request.bm25_config)
            else if (request.diagnostics) |diagnostics|
                try snap.searchWithConfigDiagnostics(alloc, text_field, terms, effectiveK(request, snap), request.bm25_config, diagnostics)
            else
                try snap.searchWithConfig(alloc, text_field, terms, effectiveK(request, snap), request.bm25_config);
            defer alloc.free(results.hits);
            if (bq.boost != 1.0) {
                for (results.hits) |*hit| hit.score *= bq.boost;
            }
            return try buildResult(alloc, snap, results.hits, results.total_count, results.total_relation, request);
        }
    }

    var collector = FastTopK{
        .alloc = alloc,
        .k = effectiveK(request, snap),
        .producers = producers,
        .filter_doc_bitmap = request.filter_doc_bitmap,
        .exclude_doc_bitmap = request.exclude_doc_bitmap,
        .filter_doc_nums = request.filter_doc_nums,
        .filter_doc_nums_positive = request.filter_doc_nums_positive,
        .exclude_doc_nums = request.exclude_doc_nums,
    };
    defer collector.deinit();

    const avg_dl = if (scoring_stats) |stats| stats.avgDocLen() else snap.textAvgDocLen(text_field);
    var allow_must_block_pruning = must_terms.items.len > 0 and
        should_terms.items.len == 0 and
        must_not_terms.items.len == 0 and
        bq.boost > 0;
    if (allow_must_block_pruning) {
        for (must_terms.items) |term| {
            if (term.boost <= 0) {
                allow_must_block_pruning = false;
                break;
            }
        }
    }
    var filtered_wand = must_terms.items.len == 0 and must_not_terms.items.len == 0 and effective_min_should == 1 and bq.boost == 1 and
        (producers.present() or requestHasDocNumConstraints(request));
    for (should_terms.items) |term| if (term.boost != 1) {
        filtered_wand = false;
        break;
    };
    if (filtered_wand) {
        try collectFilteredWandQuery(alloc, snap, text_field, should_terms.items, request, &collector, scoring_doc_count, avg_dl);
    } else {
        if (request.diagnostics) |diag| diag.segments_considered +|= @intCast(snap.segments.len);
        var doc_offset: u32 = 0;
        for (snap.segments) |*seg| {
            const segment_doc_offset = doc_offset;
            doc_offset += seg.reader.doc_count;

            try collector.beginSegment(segment_doc_offset, seg.reader.doc_count);
            var inv_reader = (try seg.reader.invertedIndexScoped(alloc, text_field)) orelse continue;
            defer inv_reader.deinit();
            {
                const maybe_must_states = try initFastTermStates(alloc, snap, inv_reader, text_field, must_terms.items, true, scoring_stats);
                const must_states = maybe_must_states orelse continue;
                defer deinitFastTermStates(alloc, must_states);

                const maybe_should_states = try initFastTermStates(alloc, snap, inv_reader, text_field, should_terms.items, false, scoring_stats);
                var should_states: []FastTermState = &[_]FastTermState{};
                if (maybe_should_states) |states| should_states = states;
                defer if (maybe_should_states) |states| deinitFastTermStates(alloc, states);

                const maybe_must_not_states = try initFastTermStates(alloc, snap, inv_reader, text_field, must_not_terms.items, false, scoring_stats);
                var must_not_states: []FastTermState = &[_]FastTermState{};
                if (maybe_must_not_states) |states| must_not_states = states;
                defer if (maybe_must_not_states) |states| deinitFastTermStates(alloc, states);
                if (request.diagnostics) |diag| {
                    diag.segments_searched +|= 1;
                    diag.postings_iterators_opened +|= @intCast(must_states.len + should_states.len + must_not_states.len);
                }

                seg.shared.lockDeletionShared();
                defer seg.shared.unlockDeletionShared();
                if (must_terms.items.len > 0) {
                    try collectFastMustSegment(&collector, seg, must_states, should_states, must_not_states, effective_min_should, segment_doc_offset, scoring_doc_count, avg_dl, request.bm25_config, bq.boost, allow_must_block_pruning, request.diagnostics);
                } else if (should_states.len > 0) {
                    try collectFastShouldSegment(&collector, seg, should_states, must_not_states, effective_min_should, segment_doc_offset, scoring_doc_count, avg_dl, request.bm25_config, bq.boost);
                }
            }
        }
    }

    const scored = try collector.finish();
    defer alloc.free(scored);
    if (request.diagnostics) |diag| diag.boolean_candidates_scored +|= collector.total_count;
    return try buildResult(alloc, snap, scored, collector.total_count, if (collector.pruned) .gte else .exact, request);
}

fn executeBool(
    alloc: Allocator,
    snap: *const index_mod.IndexSnapshot,
    bq: BoolQuery,
    request: SearchRequest,
) anyerror!SearchResult {
    if (bq.boost == 1 and bq.should.len == 0 and bq.must.len >= 1 and bq.must.len <= 2 and bq.must_not.len <= 1 and
        request.filter_doc_bitmap == null and request.exclude_doc_bitmap == null)
    {
        const producer_include = bq.must.len == 2 and bq.must[1] == .doc_num and bq.must[1].doc_num.producer != null and bq.must[1].doc_num.boost == 0;
        const producer_exclude = bq.must_not.len == 1 and bq.must_not[0] == .doc_num and bq.must_not[0].doc_num.producer != null;
        const native_include = bq.must.len == 1 or (bq.must[1] == .doc_num and bq.must[1].doc_num.ids.len == 0 and bq.must[1].doc_num.boost == 0 and (bq.must[1].doc_num.bitmap != null or producer_include));
        const native_exclude = bq.must_not.len == 0 or (bq.must_not[0] == .doc_num and bq.must_not[0].doc_num.ids.len == 0 and (bq.must_not[0].doc_num.bitmap != null or producer_exclude));
        if ((producer_include or producer_exclude) and native_include and native_exclude) {
            var constrained = request;
            if (bq.must.len == 2 and !producer_include) constrained.filter_doc_bitmap = bq.must[1].doc_num.bitmap;
            if (bq.must_not.len == 1 and !producer_exclude) constrained.exclude_doc_bitmap = bq.must_not[0].doc_num.bitmap;
            const producers: ProducerConstraints = .{
                .include = if (producer_include) bq.must[1].doc_num.producer else null,
                .exclude = if (producer_exclude) bq.must_not[0].doc_num.producer else null,
            };
            const base = bq.must[0];
            const simple: ?BoolQuery = switch (base) {
                .term, .match => .{ .should = &.{base} },
                .bool_query => |query| query,
                else => null,
            };
            if (simple) |query| if (try executeSimpleTextBoolWithProducers(alloc, snap, query, constrained, producers)) |result| return result;
            var arena = std.heap.ArenaAllocator.init(alloc);
            defer arena.deinit();
            const filter = try searchQueryToFilterArena(arena.allocator(), .{ .bool_query = bq });
            var membership = try snap.executeFilterBitmap(alloc, filter);
            defer membership.deinit();
            constrained = request;
            constrained.query = bq.must[0];
            constrained.filter_doc_bitmap = &membership;
            constrained.graph_queries = &.{};
            return execute(alloc, snap, constrained);
        }

        var constrained = request;
        var recognized = bq.must.len == 2 or bq.must_not.len == 1;
        if (bq.must.len == 2) {
            if (bq.must[1] == .doc_num and bq.must[1].doc_num.ids.len == 0 and bq.must[1].doc_num.boost == 0 and bq.must[1].doc_num.bitmap != null)
                constrained.filter_doc_bitmap = bq.must[1].doc_num.bitmap
            else
                recognized = false;
        }
        if (bq.must_not.len == 1) {
            if (bq.must_not[0] == .doc_num and bq.must_not[0].doc_num.ids.len == 0 and bq.must_not[0].doc_num.bitmap != null)
                constrained.exclude_doc_bitmap = bq.must_not[0].doc_num.bitmap
            else
                recognized = false;
        }
        if (recognized) {
            constrained.query = bq.must[0];
            constrained.graph_queries = &.{};
            return execute(alloc, snap, constrained);
        }
    }
    if (try executeSimpleTextBool(alloc, snap, bq, request)) |result| return result;
    return executeBoolAllHit(alloc, snap, bq, request);
}

/// Correctness reference/fallback for boolean shapes that are not yet lowered
/// to streaming scorers. Kept separate so differential tests can compare the
/// bounded iterator tree against the former all-hit/hash-map implementation.
fn executeBoolAllHit(
    alloc: Allocator,
    snap: *const index_mod.IndexSnapshot,
    bq: BoolQuery,
    request: SearchRequest,
) anyerror!SearchResult {
    var combined: ScoreMap = .{};
    var initialized = false;
    errdefer combined.deinit(alloc);
    const effective_min_should: u32 = if (bq.should.len > 0 and
        bq.min_should == 0 and
        bq.must.len == 0 and
        !bq.pure_should_optional) 1 else bq.min_should;

    for (bq.must) |sub_query| {
        const sub_hits = try executeQueryAllScored(alloc, snap, sub_query, request);
        defer alloc.free(sub_hits);
        if (!initialized) {
            combined = try scoreMapFromHits(alloc, sub_hits);
            initialized = true;
        } else {
            try intersectScoresWithHits(alloc, &combined, sub_hits);
        }
    }

    if (bq.pure_should_optional and bq.must.len == 0) {
        combined = try buildOptionalShouldBaseScoreMap(alloc, snap, request);
        initialized = true;
    }

    if (bq.should.len > 0) {
        if (!initialized) {
            combined = ScoreMap{};
            initialized = true;
            var should_counts = std.AutoHashMap(u64, u32).init(alloc);
            defer should_counts.deinit();
            for (bq.should) |sub_query| {
                const sub_hits = try executeQueryAllScored(alloc, snap, sub_query, request);
                defer alloc.free(sub_hits);
                try addScoresFromHits(alloc, &combined, sub_hits);
                if (effective_min_should > 1) {
                    for (sub_hits) |hit| {
                        const entry = try should_counts.getOrPut(hit.doc_id);
                        if (!entry.found_existing) entry.value_ptr.* = 0;
                        entry.value_ptr.* += 1;
                    }
                }
            }
            if (effective_min_should > 1) {
                var to_remove = std.ArrayListUnmanaged(u32).empty;
                defer to_remove.deinit(alloc);
                var it = combined.iterator();
                while (it.next()) |entry| {
                    const should_count = should_counts.get(entry.key_ptr.*) orelse 0;
                    if (should_count < effective_min_should) {
                        try to_remove.append(alloc, entry.key_ptr.*);
                    }
                }
                for (to_remove.items) |id| {
                    _ = combined.remove(id);
                }
            }
        } else {
            var should_counts = if (effective_min_should > 0) std.AutoHashMap(u64, u32).init(alloc) else null;
            defer if (should_counts) |*counts| counts.deinit();
            for (bq.should) |sub_query| {
                const sub_hits = try executeQueryAllScored(alloc, snap, sub_query, request);
                defer alloc.free(sub_hits);
                try addOptionalScoresFromHits(alloc, &combined, sub_hits);
                if (should_counts) |*counts| {
                    for (sub_hits) |hit| {
                        if (combined.contains(hit.doc_id)) {
                            const entry = try counts.getOrPut(hit.doc_id);
                            if (!entry.found_existing) entry.value_ptr.* = 0;
                            entry.value_ptr.* += 1;
                        }
                    }
                }
            }
            if (should_counts) |*counts| {
                var to_remove = std.ArrayListUnmanaged(u32).empty;
                defer to_remove.deinit(alloc);
                var it = combined.iterator();
                while (it.next()) |entry| {
                    const count = counts.get(entry.key_ptr.*) orelse 0;
                    if (count < effective_min_should) {
                        try to_remove.append(alloc, entry.key_ptr.*);
                    }
                }
                for (to_remove.items) |id| {
                    _ = combined.remove(id);
                }
            }
        }
    }

    if (!initialized) {
        combined = try buildAllDocsScoreMap(alloc, snap);
        initialized = true;
    }
    defer combined.deinit(alloc);

    for (bq.must_not) |sub_query| {
        const sub_hits = try executeQueryAllScored(alloc, snap, sub_query, request);
        defer alloc.free(sub_hits);
        try subtractScoresFromHits(alloc, &combined, sub_hits);
    }

    try applyDocNumConstraintsToScoreMap(alloc, &combined, request);

    const all_scored = try scoreMapToSortedHits(alloc, combined, bq.boost);
    defer alloc.free(all_scored);

    return buildResult(alloc, snap, all_scored, @intCast(all_scored.len), .exact, request);
}

pub fn searchQueryToFilterArena(alloc: Allocator, sq: SearchQuery) anyerror!query_mod.Filter {
    return switch (sq) {
        .match_none => .{ .match_none = {} },
        .match_all => .{ .match_all = {} },
        .term_phrase => |pq| .{ .phrase = .{
            .field = pq.field,
            .terms = pq.terms,
            .slop = 0,
            .max_edits = pq.max_edits,
            .auto_fuzzy = pq.auto_fuzzy,
        } },
        .multi_phrase => |pq| .{ .multi_phrase = .{
            .field = pq.field,
            .term_alternatives = pq.terms,
            .slop = 0,
            .max_edits = pq.max_edits,
            .auto_fuzzy = pq.auto_fuzzy,
        } },
        .term => |tq| .{ .term = .{ .field = tq.field, .term = tq.term } },
        .fuzzy => |fq| .{ .fuzzy = .{
            .field = fq.field,
            .term = fq.term,
            .max_edits = if (fq.auto_fuzzy)
                (if (fq.term.len > 5) 2 else if (fq.term.len > 2) 1 else 0)
            else
                fq.max_edits,
            .prefix_len = fq.prefix_len,
        } },
        .numeric_range => |rq| .{ .range = .{
            .field = rq.field,
            .min_val = rq.min,
            .max_val = rq.max,
            .inclusive_min = rq.inclusive_min,
            .inclusive_max = rq.inclusive_max,
        } },
        .date_range => |rq| .{ .date_range = .{
            .field = rq.field,
            .start_ns = rq.start_ns,
            .end_ns = rq.end_ns,
            .inclusive_start = rq.inclusive_start,
            .inclusive_end = rq.inclusive_end,
        } },
        .doc_id => |dq| .{ .doc_id = .{ .doc_ids = dq.ids } },
        .doc_num => |dq| .{ .doc_num = .{ .doc_nums = dq.ids, .bitmap = dq.bitmap, .producer = dq.producer } },
        .bool_field => |bq| .{ .bool_field = .{ .field = bq.field, .value = bq.value } },
        .geo_distance => |gq| .{ .geo_distance = .{
            .field = gq.field,
            .center = gq.center,
            .radius_meters = gq.radius_meters,
        } },
        .geo_bbox => |gq| .{ .geo_bbox = .{
            .field = gq.field,
            .min_lat = gq.min_lat,
            .min_lon = gq.min_lon,
            .max_lat = gq.max_lat,
            .max_lon = gq.max_lon,
        } },
        .term_range => |rq| .{ .term_range = .{
            .field = rq.field,
            .min = rq.min,
            .max = rq.max,
            .inclusive_min = rq.inclusive_min,
            .inclusive_max = rq.inclusive_max,
        } },
        .ip_range => |iq| .{ .ip_range = .{
            .field = iq.field,
            .cidr = iq.cidr,
        } },
        .geo_shape => |gq| .{ .geo_shape = .{
            .field = gq.field,
            .relation = geoShapeFilterRelation(gq.relation),
            .polygons = gq.polygons,
        } },
        .match => |mq| blk: {
            const analyzer = mq.analyzer orelse &analysis_mod.default_analyzer;
            const tokens = try analyzer.analyze(alloc, mq.text);
            if (tokens.len == 0) break :blk .{ .match_none = {} };
            if (tokens.len == 1) {
                break :blk .{ .term = .{
                    .field = mq.field,
                    .term = try alloc.dupe(u8, tokens[0].term),
                } };
            }
            var filters = try alloc.alloc(query_mod.Filter, tokens.len);
            for (tokens, 0..) |tok, i| {
                filters[i] = .{ .term = .{
                    .field = mq.field,
                    .term = try alloc.dupe(u8, tok.term),
                } };
            }
            break :blk .{ .bool_filter = .{ .must = &.{}, .should = filters, .must_not = &.{} } };
        },
        .phrase => |pq| (try buildPhraseFilter(alloc, pq.field, pq.text, pq.analyzer orelse &analysis_mod.default_analyzer, pq.max_edits, pq.auto_fuzzy)) orelse
            .{ .bool_filter = .{ .must = &.{}, .should = &.{}, .must_not = &.{} } },
        .prefix => |pq| .{ .prefix = .{ .field = pq.field, .prefix = pq.prefix, .indexed_field = pq.indexed_field } },
        .wildcard => |wq| .{ .wildcard = .{ .field = wq.field, .pattern = wq.pattern } },
        .regexp => |rq| .{ .regexp = .{ .field = rq.field, .pattern = rq.pattern } },
        .bool_query => |bq| blk: {
            const must = try searchQuerySliceToFilterSliceArena(alloc, bq.must);
            const should = try searchQuerySliceToFilterSliceArena(alloc, bq.should);
            const must_not = try searchQuerySliceToFilterSliceArena(alloc, bq.must_not);
            const filter_must = if (bq.pure_should_optional and must.len == 0) required: {
                const match_all = try alloc.alloc(query_mod.Filter, 1);
                match_all[0] = .{ .match_all = {} };
                break :required match_all;
            } else must;
            const effective_min_should: u32 = if (should.len > 0 and
                bq.min_should == 0 and
                filter_must.len == 0 and
                !bq.pure_should_optional) 1 else bq.min_should;
            break :blk .{
                .bool_filter = .{
                    // The bitmap engine uses a missing required branch to infer a
                    // one-clause should minimum. Preserve optional scoring should
                    // semantics with an internal match-all membership anchor.
                    .must = filter_must,
                    .should = should,
                    .must_not = must_not,
                    .min_should_match = effective_min_should,
                },
            };
        },
        else => return error.InvalidArgument,
    };
}

fn searchQuerySliceToFilterSliceArena(alloc: Allocator, items: []const SearchQuery) anyerror![]query_mod.Filter {
    if (items.len == 0) return &.{};
    var out = try alloc.alloc(query_mod.Filter, items.len);
    for (items, 0..) |item, i| {
        out[i] = try searchQueryToFilterArena(alloc, item);
    }
    return out;
}

/// Result of queryToFilter, includes allocated resources that must be freed.
const OwnedFilter = struct {
    filter: query_mod.Filter,
    /// Duped term strings that must be freed by the caller.
    duped_terms: []const []const u8,
    /// Allocated filter slice (for bool should), or empty.
    filter_slice: []query_mod.Filter,

    pub fn deinit(self: *const OwnedFilter, alloc: Allocator) void {
        for (self.duped_terms) |dt| alloc.free(dt);
        if (self.duped_terms.len > 0) alloc.free(self.duped_terms);
        if (self.filter_slice.len > 0) alloc.free(self.filter_slice);
    }
};

/// Convert a SearchQuery to a Filter for use in sort-by-field mode.
fn queryToFilter(alloc: Allocator, sq: SearchQuery) !OwnedFilter {
    return switch (sq) {
        .match_none => .{ .filter = .{ .match_none = {} }, .duped_terms = &.{}, .filter_slice = &.{} },
        .match_all => .{ .filter = .{ .match_all = {} }, .duped_terms = &.{}, .filter_slice = &.{} },
        .term_phrase => |pq| .{
            .filter = .{ .phrase = .{ .field = pq.field, .terms = pq.terms, .slop = 0, .max_edits = pq.max_edits, .auto_fuzzy = pq.auto_fuzzy } },
            .duped_terms = &.{},
            .filter_slice = &.{},
        },
        .multi_phrase => |pq| .{
            .filter = .{ .multi_phrase = .{ .field = pq.field, .term_alternatives = pq.terms, .slop = 0, .max_edits = pq.max_edits, .auto_fuzzy = pq.auto_fuzzy } },
            .duped_terms = &.{},
            .filter_slice = &.{},
        },
        .term => |tq| .{ .filter = .{ .term = .{ .field = tq.field, .term = tq.term } }, .duped_terms = &.{}, .filter_slice = &.{} },
        .fuzzy => |fq| .{
            .filter = .{ .fuzzy = .{
                .field = fq.field,
                .term = fq.term,
                .max_edits = if (fq.auto_fuzzy)
                    (if (fq.term.len > 5) 2 else if (fq.term.len > 2) 1 else 0)
                else
                    fq.max_edits,
                .prefix_len = fq.prefix_len,
            } },
            .duped_terms = &.{},
            .filter_slice = &.{},
        },
        .numeric_range => |rq| .{
            .filter = .{ .range = .{
                .field = rq.field,
                .min_val = rq.min,
                .max_val = rq.max,
                .inclusive_min = rq.inclusive_min,
                .inclusive_max = rq.inclusive_max,
            } },
            .duped_terms = &.{},
            .filter_slice = &.{},
        },
        .date_range => |rq| .{
            .filter = .{ .date_range = .{
                .field = rq.field,
                .start_ns = rq.start_ns,
                .end_ns = rq.end_ns,
                .inclusive_start = rq.inclusive_start,
                .inclusive_end = rq.inclusive_end,
            } },
            .duped_terms = &.{},
            .filter_slice = &.{},
        },
        .doc_id => |dq| .{
            .filter = .{ .doc_id = .{ .doc_ids = dq.ids } },
            .duped_terms = &.{},
            .filter_slice = &.{},
        },
        .doc_num => |dq| .{
            .filter = .{ .doc_num = .{ .doc_nums = dq.ids, .bitmap = dq.bitmap, .producer = dq.producer } },
            .duped_terms = &.{},
            .filter_slice = &.{},
        },
        .bool_field => |bq| .{
            .filter = .{ .bool_field = .{ .field = bq.field, .value = bq.value } },
            .duped_terms = &.{},
            .filter_slice = &.{},
        },
        .geo_distance => |gq| .{
            .filter = .{ .geo_distance = .{
                .field = gq.field,
                .center = gq.center,
                .radius_meters = gq.radius_meters,
            } },
            .duped_terms = &.{},
            .filter_slice = &.{},
        },
        .geo_bbox => |gq| .{
            .filter = .{ .geo_bbox = .{
                .field = gq.field,
                .min_lat = gq.min_lat,
                .min_lon = gq.min_lon,
                .max_lat = gq.max_lat,
                .max_lon = gq.max_lon,
            } },
            .duped_terms = &.{},
            .filter_slice = &.{},
        },
        .term_range => |rq| .{
            .filter = .{ .term_range = .{
                .field = rq.field,
                .min = rq.min,
                .max = rq.max,
                .inclusive_min = rq.inclusive_min,
                .inclusive_max = rq.inclusive_max,
            } },
            .duped_terms = &.{},
            .filter_slice = &.{},
        },
        .ip_range => |iq| .{
            .filter = .{ .ip_range = .{ .field = iq.field, .cidr = iq.cidr } },
            .duped_terms = &.{},
            .filter_slice = &.{},
        },
        .geo_shape => |gq| .{
            .filter = .{ .geo_shape = .{
                .field = gq.field,
                .relation = geoShapeFilterRelation(gq.relation),
                .polygons = gq.polygons,
            } },
            .duped_terms = &.{},
            .filter_slice = &.{},
        },
        .match => |mq| blk: {
            const analyzer = mq.analyzer orelse &analysis_mod.default_analyzer;
            const tokens = try analyzer.analyze(alloc, mq.text);
            defer analysis_mod.Analyzer.freeTokens(alloc, tokens);

            if (tokens.len == 0) break :blk OwnedFilter{ .filter = .{ .match_none = {} }, .duped_terms = &.{}, .filter_slice = &.{} };
            if (tokens.len == 1) {
                const duped = try alloc.dupe(u8, tokens[0].term);
                const duped_list = try alloc.alloc([]const u8, 1);
                duped_list[0] = duped;
                break :blk OwnedFilter{
                    .filter = .{ .term = .{ .field = mq.field, .term = duped } },
                    .duped_terms = duped_list,
                    .filter_slice = &.{},
                };
            }

            // Multiple terms -> bool should (OR)
            var term_filters = try alloc.alloc(query_mod.Filter, tokens.len);
            var duped_list = try alloc.alloc([]const u8, tokens.len);
            for (tokens, 0..) |tok, i| {
                const duped = try alloc.dupe(u8, tok.term);
                duped_list[i] = duped;
                term_filters[i] = .{ .term = .{ .field = mq.field, .term = duped } };
            }
            break :blk OwnedFilter{
                .filter = .{ .bool_filter = .{ .must = &.{}, .should = term_filters, .must_not = &.{} } },
                .duped_terms = duped_list,
                .filter_slice = term_filters,
            };
        },
        .phrase => |pq| blk: {
            const filter = (try buildPhraseFilter(alloc, pq.field, pq.text, pq.analyzer orelse &analysis_mod.default_analyzer, pq.max_edits, pq.auto_fuzzy)) orelse
                query_mod.Filter{ .bool_filter = .{ .must = &.{}, .should = &.{}, .must_not = &.{} } };
            const duped_terms = switch (filter) {
                .phrase => |pf| pf.terms,
                else => &.{},
            };
            break :blk OwnedFilter{
                .filter = filter,
                .duped_terms = duped_terms,
                .filter_slice = &.{},
            };
        },
        .prefix => |pq| .{
            .filter = .{ .prefix = .{ .field = pq.field, .prefix = pq.prefix, .indexed_field = pq.indexed_field } },
            .duped_terms = &.{},
            .filter_slice = &.{},
        },
        .wildcard => |wq| .{
            .filter = .{ .wildcard = .{ .field = wq.field, .pattern = wq.pattern } },
            .duped_terms = &.{},
            .filter_slice = &.{},
        },
        .regexp => |rq| .{
            .filter = .{ .regexp = .{ .field = rq.field, .pattern = rq.pattern } },
            .duped_terms = &.{},
            .filter_slice = &.{},
        },
        .bool_query => .{ .filter = .{ .match_all = {} }, .duped_terms = &.{}, .filter_slice = &.{} },
        .knn => .{ .filter = .{ .match_all = {} }, .duped_terms = &.{}, .filter_slice = &.{} },
        .hybrid => .{ .filter = .{ .match_all = {} }, .duped_terms = &.{}, .filter_slice = &.{} },
    };
}

/// Execute a search with sort-by-field (no BM25 scoring).
fn executeSort(
    alloc: Allocator,
    snap: *const index_mod.IndexSnapshot,
    request: SearchRequest,
) !SearchResult {
    var read_scope = segment_mod.TypedReadScope.init(alloc);
    defer read_scope.deinit();
    const reads = &read_scope;
    const sort_spec = request.sort.?;

    // Get matching doc IDs via filter
    const owned_filter = try queryToFilter(alloc, request.query);
    defer owned_filter.deinit(alloc);
    const doc_ids = try snap.executeFilter(alloc, owned_filter.filter);
    defer alloc.free(doc_ids);

    // Read sort field values and pair with doc IDs
    const DocVal = struct { doc_id: u32, value: f64 };
    var doc_vals = std.ArrayListUnmanaged(DocVal).empty;
    defer doc_vals.deinit(alloc);

    for (doc_ids) |did| {
        const val = try readF64ForDoc(alloc, reads, snap, did, sort_spec.field) orelse 0.0;
        try doc_vals.append(alloc, .{ .doc_id = did, .value = val });
    }

    // Sort by value
    const is_asc = sort_spec.order == .asc;
    std.mem.sort(DocVal, doc_vals.items, is_asc, struct {
        fn cmp(asc: bool, a: DocVal, b: DocVal) bool {
            if (asc) return a.value < b.value;
            return a.value > b.value;
        }
    }.cmp);

    const total: u32 = @intCast(doc_vals.items.len);
    const start = @min(request.offset, total);
    const end = @min(start + request.k, total);
    const page = doc_vals.items[start..end];

    var hits = try alloc.alloc(ScoredHit, page.len);
    errdefer alloc.free(hits);

    for (page, 0..) |dv, i| {
        const hit = ScoredHit{
            .doc_id = dv.doc_id,
            .score = @floatCast(dv.value),
            .id = null,
            .stored_data = null,
        };
        hits[i] = hit;
    }

    errdefer freeStoredBodies(alloc, hits);
    if (request.include_stored) try populateStoredHits(alloc, snap, hits, request.diagnostics);

    // Collect aggregations over all matching docs
    var agg_results: []NamedAggResult = &.{};
    if (request.aggregations.len > 0) {
        var all_scored = try alloc.alloc(scorer_mod.ScoredHit, doc_ids.len);
        defer alloc.free(all_scored);
        for (doc_ids, 0..) |did, i| {
            all_scored[i] = .{ .doc_id = did, .score = 0.0 };
        }
        agg_results = try collectAggregations(alloc, snap, all_scored, request.aggregations);
    }

    return .{ .alloc = alloc, .hits = hits, .total_hits = total, .aggregations = agg_results };
}

/// Find the named HBC index from the request's index list.
fn findHBCIndex(request: SearchRequest, name: []const u8) ?*hbc_mod.HBCIndex {
    for (request.hbc_indexes) |ref| {
        if (std.mem.eql(u8, ref.name, name)) return ref.index;
    }
    return null;
}

/// Execute a KNN vector search.
fn executeKNN(
    alloc: Allocator,
    snap: *const index_mod.IndexSnapshot,
    kq: KNNQuery,
    request: SearchRequest,
) !SearchResult {
    const hbc_index = findHBCIndex(request, kq.index_name) orelse {
        return .{ .alloc = alloc, .hits = &.{}, .total_hits = 0 };
    };

    var hbc_results = try hbc_index.search(kq.vector, kq.k);
    defer hbc_results.deinit();

    const n = hbc_results.items.items.len;
    var hits = try alloc.alloc(ScoredHit, n);
    errdefer alloc.free(hits);

    for (hbc_results.items.items, 0..) |item, i| {
        const doc_id: u32 = @intCast(item.vector_id);
        const hit = ScoredHit{
            .doc_id = doc_id,
            .score = 1.0 / (1.0 + item.distance),
            .id = null,
            .stored_data = null,
        };
        hits[i] = hit;
    }

    errdefer freeStoredBodies(alloc, hits);
    if (request.include_stored) try populateStoredHits(alloc, snap, hits, request.diagnostics);

    return .{ .alloc = alloc, .hits = hits, .total_hits = @intCast(n) };
}

/// Execute a hybrid search: BM25 text + KNN vector, fused via RRF/RSF.
fn executeHybrid(
    alloc: Allocator,
    snap: *const index_mod.IndexSnapshot,
    hq: HybridQuery,
    request: SearchRequest,
) !SearchResult {
    // Execute text search directly (avoid recursive execute → error set loop)
    // Fusion needs document numbers and scores, not candidate bodies or
    // aggregations. Materialize owned bodies once, for the fused winners.
    var candidate_request = request;
    candidate_request.include_stored = false;
    candidate_request.aggregations = &.{};
    var text_result = switch (hq.text_query) {
        .match_none => try executeMatchNone(alloc, candidate_request),
        .match => |mq| try executeMatch(alloc, snap, mq, candidate_request),
        .phrase => |pq| try executePhrase(alloc, snap, pq, candidate_request),
        .term_phrase => |pq| try executeTermPhrase(alloc, snap, pq, candidate_request),
        .multi_phrase => |pq| try executeMultiPhrase(alloc, snap, pq, candidate_request),
        .term => |tq| try executeTerm(alloc, snap, tq, candidate_request),
        .fuzzy => |fq| try executeFuzzy(alloc, snap, fq, candidate_request),
        .numeric_range => |rq| try executeNumericRange(alloc, snap, rq, candidate_request),
        .date_range => |rq| try executeDateRange(alloc, snap, rq, candidate_request),
        .doc_id => |dq| try executeDocID(alloc, snap, dq, candidate_request),
        .bool_field => |bq| try executeBoolField(alloc, snap, bq, candidate_request),
        .geo_distance => |gq| try executeGeoDistance(alloc, snap, gq, candidate_request),
        .geo_bbox => |gq| try executeGeoBBox(alloc, snap, gq, candidate_request),
        .term_range => |rq| try executeTermRange(alloc, snap, rq, candidate_request),
        .ip_range => |iq| try executeIPRange(alloc, snap, iq, candidate_request),
        .geo_shape => |gq| try executeGeoShape(alloc, snap, gq, candidate_request),
        .prefix => |pq| try executePrefix(alloc, snap, pq, candidate_request),
        .wildcard => |wq| try executeWildcard(alloc, snap, wq, candidate_request),
        .regexp => |rq| try executeRegexp(alloc, snap, rq, candidate_request),
        .bool_query => |bq| try executeBool(alloc, snap, bq, candidate_request),
    };
    defer text_result.deinit();

    // Execute KNN search directly
    var knn_result = try executeKNN(alloc, snap, hq.knn, candidate_request);
    defer knn_result.deinit();

    // Convert to fusion RankedResult format
    var text_ranked = try alloc.alloc(fusion_mod.RankedHit, text_result.hits.len);
    defer alloc.free(text_ranked);
    var text_initialized: usize = 0;
    defer for (text_ranked[0..text_initialized]) |rh| alloc.free(rh.doc_id);
    for (text_result.hits, 0..) |hit, i| {
        text_ranked[i] = .{
            .doc_id = try std.fmt.allocPrint(alloc, "{d}", .{hit.doc_id}),
            .score = @floatCast(hit.score),
        };
        text_initialized += 1;
    }

    var knn_ranked = try alloc.alloc(fusion_mod.RankedHit, knn_result.hits.len);
    defer alloc.free(knn_ranked);
    var knn_initialized: usize = 0;
    defer for (knn_ranked[0..knn_initialized]) |rh| alloc.free(rh.doc_id);
    for (knn_result.hits, 0..) |hit, i| {
        knn_ranked[i] = .{
            .doc_id = try std.fmt.allocPrint(alloc, "{d}", .{hit.doc_id}),
            .score = @floatCast(hit.score),
        };
        knn_initialized += 1;
    }

    const ranked_results = [_]fusion_mod.RankedResult{
        .{ .index_name = "text", .hits = text_ranked },
        .{ .index_name = "knn", .hits = knn_ranked },
    };

    const fused = try fusion_mod.fuse(alloc, &ranked_results, hq.fusion_config);
    defer fusion_mod.freeHits(alloc, fused);

    // Convert fused results back to ScoredHits
    const result_count = @min(fused.len, request.k);
    var hits = try alloc.alloc(ScoredHit, result_count);
    var initialized: usize = 0;
    errdefer {
        for (hits[0..initialized]) |*hit| {
            freeStoredHit(alloc, hit);
            freeIndexScores(alloc, hit.index_scores);
        }
        alloc.free(hits);
    }

    for (fused[0..result_count], 0..) |fh, i| {
        const doc_id = std.fmt.parseInt(u32, fh.doc_id, 10) catch 0;
        const hit = ScoredHit{
            .doc_id = doc_id,
            .score = @floatCast(fh.score),
            .id = null,
            .stored_data = null,
            .index_scores = try cloneIndexScores(alloc, fh.index_scores),
        };
        errdefer freeIndexScores(alloc, hit.index_scores);
        hits[i] = hit;
        initialized += 1;
    }

    if (request.include_stored) try populateStoredHits(alloc, snap, hits, request.diagnostics);

    return .{ .alloc = alloc, .hits = hits, .total_hits = @intCast(result_count) };
}

fn buildResult(
    alloc: Allocator,
    snap: *const index_mod.IndexSnapshot,
    scored: []const scorer_mod.ScoredHit,
    total_count: u32,
    total_relation: TotalHitsRelation,
    request: SearchRequest,
) !SearchResult {
    // Fallback scorers may return an unfiltered candidate set. Numeric and
    // bitmap constraints share the same boundary before final pagination.
    if (requestHasDocNumConstraints(request)) {
        var filtered: std.ArrayListUnmanaged(scorer_mod.ScoredHit) = .empty;
        defer filtered.deinit(alloc);
        for (scored) |hit| if (requestAllowsDocNum(request, hit.doc_id)) {
            try filtered.append(alloc, hit);
        };
        var next = request;
        next.filter_doc_bitmap = null;
        next.exclude_doc_bitmap = null;
        next.filter_doc_nums = &.{};
        next.filter_doc_nums_positive = false;
        next.exclude_doc_nums = &.{};
        const count = if (filtered.items.len == scored.len) total_count else @as(u32, @intCast(filtered.items.len));
        return buildResult(alloc, snap, filtered.items, count, total_relation, next);
    }
    // Apply cursor filter: skip all results at or before the cursor position.
    // Scored results are sorted by (score desc, doc_id asc).
    var filtered_start: usize = 0;
    if (request.search_after) |cursor| {
        for (scored, 0..) |sh, i| {
            if (sh.score < cursor.score or (sh.score == cursor.score and sh.doc_id > cursor.doc_id)) {
                filtered_start = i;
                break;
            }
        } else {
            filtered_start = scored.len;
        }
    }

    const after_cursor = scored[filtered_start..];
    const total: u32 = @intCast(after_cursor.len);
    const start = @min(request.offset, total);
    const end = @min(start + request.k, total);
    const result_slice = after_cursor[start..end];

    var hits = try alloc.alloc(ScoredHit, result_slice.len);
    errdefer alloc.free(hits);

    for (result_slice, 0..) |sh, i| {
        const hit = ScoredHit{
            .doc_id = sh.doc_id,
            .score = sh.score,
            .id = null,
            .stored_data = null,
        };

        hits[i] = hit;
    }

    errdefer freeStoredBodies(alloc, hits);
    if (request.include_stored) try populateStoredHits(alloc, snap, hits, request.diagnostics);

    const agg_results = try collectAggregations(alloc, snap, scored, request.aggregations);

    // Set cursor to last hit for next page
    var result_cursor: ?SearchCursor = null;
    if (hits.len > 0) {
        const last = hits[hits.len - 1];
        result_cursor = .{ .score = last.score, .doc_id = last.doc_id };
    }

    return .{ .alloc = alloc, .hits = hits, .total_hits = total_count, .total_hits_relation = total_relation, .aggregations = agg_results, .cursor = result_cursor };
}

/// Collect aggregation results by reading typed doc values for each matching doc.
fn collectAggregations(
    alloc: Allocator,
    snap: *const index_mod.IndexSnapshot,
    scored: []const scorer_mod.ScoredHit,
    agg_specs: []const AggSpec,
) ![]NamedAggResult {
    var read_scope = segment_mod.TypedReadScope.init(alloc);
    defer read_scope.deinit();
    const reads = &read_scope;
    if (agg_specs.len == 0) return &.{};

    var results = try alloc.alloc(NamedAggResult, agg_specs.len);
    errdefer alloc.free(results);

    var candidates = StatsCandidates.init(alloc, snap, scored);
    defer candidates.deinit();
    for (agg_specs, 0..) |spec, spec_idx| {
        results[spec_idx] = .{
            .name = spec.name,
            .result = try collectOneAgg(alloc, reads, snap, scored, spec, &candidates),
            .sub_results = try collectSubAggs(alloc, reads, snap, scored, spec),
        };
    }

    return results;
}

/// Collect sub-aggregation results for a bucket aggregation.
/// Groups docs by bucket key, runs sub-aggs per bucket.
fn collectSubAggs(
    alloc: Allocator,
    reads: *segment_mod.TypedReadScope,
    snap: *const index_mod.IndexSnapshot,
    scored: []const scorer_mod.ScoredHit,
    spec: AggSpec,
) !?[]const BucketSubResult {
    if (spec.sub_aggs.len == 0) return null;

    // Group scored hits by bucket key
    const BucketList = std.ArrayListUnmanaged(scorer_mod.ScoredHit);
    var i64_buckets = std.AutoHashMapUnmanaged(i64, BucketList){};
    defer {
        var it = i64_buckets.valueIterator();
        while (it.next()) |v| v.deinit(alloc);
        i64_buckets.deinit(alloc);
    }
    var timestamp_buckets = std.AutoHashMapUnmanaged(i128, BucketList){};
    defer {
        var it = timestamp_buckets.valueIterator();
        while (it.next()) |v| v.deinit(alloc);
        timestamp_buckets.deinit(alloc);
    }
    var u32_buckets = std.AutoHashMapUnmanaged(u32, BucketList){};
    defer {
        var it = u32_buckets.valueIterator();
        while (it.next()) |v| v.deinit(alloc);
        u32_buckets.deinit(alloc);
    }

    switch (spec.agg_type) {
        .histogram => |h| {
            for (scored) |hit| {
                if (try readF64ForDoc(alloc, reads, snap, hit.doc_id, spec.field)) |val| {
                    const bk: i64 = @intFromFloat(@floor(val / h.interval));
                    const gop = try i64_buckets.getOrPut(alloc, bk);
                    if (!gop.found_existing) gop.value_ptr.* = .empty;
                    try gop.value_ptr.append(alloc, hit);
                }
            }
        },
        .date_histogram => |dh| {
            for (scored) |hit| {
                if (try readTimestampForDoc(alloc, reads, snap, hit.doc_id, spec.field)) |ns| {
                    const bk = try aggregation_mod.truncateSignedToInterval(ns, dh.interval);
                    const gop = try timestamp_buckets.getOrPut(alloc, bk);
                    if (!gop.found_existing) gop.value_ptr.* = .empty;
                    try gop.value_ptr.append(alloc, hit);
                }
            }
        },
        .range => |r| {
            for (scored) |hit| {
                if (try readF64ForDoc(alloc, reads, snap, hit.doc_id, spec.field)) |val| {
                    for (r.ranges, 0..) |rng, ri| {
                        const above = if (rng.from) |f| val >= f else true;
                        const below = if (rng.to) |t| val < t else true;
                        if (above and below) {
                            const gop = try u32_buckets.getOrPut(alloc, @intCast(ri));
                            if (!gop.found_existing) gop.value_ptr.* = .empty;
                            try gop.value_ptr.append(alloc, hit);
                        }
                    }
                }
            }
        },
        .geo_distance => |gd| {
            for (scored) |hit| {
                if (try readGeoPointForDoc(alloc, reads, snap, hit.doc_id, spec.field)) |pt| {
                    const dist = geo_mod.haversineDistance(gd.center, pt);
                    for (gd.ranges, 0..) |rng, ri| {
                        const above = if (rng.from) |f| dist >= f else true;
                        const below = if (rng.to) |t| dist < t else true;
                        if (above and below) {
                            const gop = try u32_buckets.getOrPut(alloc, @intCast(ri));
                            if (!gop.found_existing) gop.value_ptr.* = .empty;
                            try gop.value_ptr.append(alloc, hit);
                        }
                    }
                }
            }
        },
        .terms => {
            // Terms sub-agg grouping would require string bucket keys;
            // for now, skip sub-aggs on terms facets (can be added later)
            return null;
        },
        .geohash_grid => {
            // Similar to terms — skip for now
            return null;
        },
        .stats => return null, // stats is a metric, not a bucket agg
    }

    // Build BucketSubResult array from whichever bucket map was used
    if (i64_buckets.count() > 0) {
        return try buildSubResultsI64(alloc, reads, snap, &i64_buckets, spec.sub_aggs);
    } else if (timestamp_buckets.count() > 0) {
        return try buildSubResultsTimestamp(alloc, reads, snap, &timestamp_buckets, spec.sub_aggs);
    } else if (u32_buckets.count() > 0) {
        return try buildSubResultsU32(alloc, reads, snap, &u32_buckets, spec.sub_aggs);
    }
    return null;
}

/// Collect leaf-level aggregations (no sub-aggs) for a subset of docs.
fn collectLeafAggs(
    alloc: Allocator,
    reads: *segment_mod.TypedReadScope,
    snap: *const index_mod.IndexSnapshot,
    scored: []const scorer_mod.ScoredHit,
    sub_specs: []const AggSpec,
) ![]NamedAggResult {
    var results = try alloc.alloc(NamedAggResult, sub_specs.len);
    errdefer alloc.free(results);
    var candidates = StatsCandidates.init(alloc, snap, scored);
    defer candidates.deinit();
    for (sub_specs, 0..) |spec, i| {
        results[i] = .{
            .name = spec.name,
            .result = try collectOneAgg(alloc, reads, snap, scored, spec, &candidates),
        };
    }
    return results;
}

fn buildSubResultsI64(
    alloc: Allocator,
    reads: *segment_mod.TypedReadScope,
    snap: *const index_mod.IndexSnapshot,
    buckets: *std.AutoHashMapUnmanaged(i64, std.ArrayListUnmanaged(scorer_mod.ScoredHit)),
    sub_specs: []const AggSpec,
) ![]const BucketSubResult {
    var results = try alloc.alloc(BucketSubResult, buckets.count());
    var idx: usize = 0;
    var it = buckets.iterator();
    while (it.next()) |entry| {
        results[idx] = .{
            .bucket_key = .{ .int = entry.key_ptr.* },
            .aggs = try collectLeafAggs(alloc, reads, snap, entry.value_ptr.items, sub_specs),
        };
        idx += 1;
    }
    std.mem.sort(BucketSubResult, results, {}, struct {
        fn cmp(_: void, a: BucketSubResult, b: BucketSubResult) bool {
            return a.bucket_key.int < b.bucket_key.int;
        }
    }.cmp);
    return results;
}

fn buildSubResultsTimestamp(
    alloc: Allocator,
    reads: *segment_mod.TypedReadScope,
    snap: *const index_mod.IndexSnapshot,
    buckets: *std.AutoHashMapUnmanaged(i128, std.ArrayListUnmanaged(scorer_mod.ScoredHit)),
    sub_specs: []const AggSpec,
) ![]const BucketSubResult {
    var results = try alloc.alloc(BucketSubResult, buckets.count());
    var idx: usize = 0;
    var it = buckets.iterator();
    while (it.next()) |entry| {
        results[idx] = .{
            .bucket_key = .{ .timestamp = entry.key_ptr.* },
            .aggs = try collectLeafAggs(alloc, reads, snap, entry.value_ptr.items, sub_specs),
        };
        idx += 1;
    }
    std.mem.sort(BucketSubResult, results, {}, struct {
        fn cmp(_: void, a: BucketSubResult, b: BucketSubResult) bool {
            return a.bucket_key.timestamp < b.bucket_key.timestamp;
        }
    }.cmp);
    return results;
}

fn buildSubResultsU32(
    alloc: Allocator,
    reads: *segment_mod.TypedReadScope,
    snap: *const index_mod.IndexSnapshot,
    buckets: *std.AutoHashMapUnmanaged(u32, std.ArrayListUnmanaged(scorer_mod.ScoredHit)),
    sub_specs: []const AggSpec,
) ![]const BucketSubResult {
    var results = try alloc.alloc(BucketSubResult, buckets.count());
    var idx: usize = 0;
    var it = buckets.iterator();
    while (it.next()) |entry| {
        results[idx] = .{
            .bucket_key = .{ .range_idx = entry.key_ptr.* },
            .aggs = try collectLeafAggs(alloc, reads, snap, entry.value_ptr.items, sub_specs),
        };
        idx += 1;
    }
    std.mem.sort(BucketSubResult, results, {}, struct {
        fn cmp(_: void, a: BucketSubResult, b: BucketSubResult) bool {
            return a.bucket_key.range_idx < b.bucket_key.range_idx;
        }
    }.cmp);
    return results;
}

fn collectOneAgg(
    alloc: Allocator,
    reads: *segment_mod.TypedReadScope,
    snap: *const index_mod.IndexSnapshot,
    scored: []const scorer_mod.ScoredHit,
    spec: AggSpec,
    candidates: *StatsCandidates,
) !AggResult {
    switch (spec.agg_type) {
        .stats => {
            // Batched path: iterate chunks per segment for SIMD-friendly bulk collection
            var stats = aggregation_mod.StatsAgg.init();
            try collectStatsGrouped(reads, snap, candidates, spec.field, &stats);
            return .{ .stats = stats };
        },
        .histogram => |h| {
            var hist = aggregation_mod.HistogramAgg.init(alloc, h.interval, 0.0);
            defer hist.deinit();

            for (scored) |hit| {
                if (try readF64ForDoc(alloc, reads, snap, hit.doc_id, spec.field)) |val| {
                    try hist.collect(val);
                }
            }

            const keys = try hist.sortedKeys(alloc);
            errdefer alloc.free(keys);
            var counts = try alloc.alloc(u64, keys.len);
            for (keys, 0..) |k, i| {
                counts[i] = hist.getCount(k);
            }

            return .{ .histogram = .{ .keys = keys, .counts = counts } };
        },
        .terms => |t| {
            var facet = aggregation_mod.TermsFacet.init(alloc);
            defer facet.deinit();

            for (scored) |hit| {
                if (try readBytesForDoc(alloc, reads, snap, hit.doc_id, spec.field)) |val| {
                    defer alloc.free(val);
                    try facet.collect(val);
                }
            }

            const entries = try facet.topK(alloc, t.top_k);
            return .{ .terms = entries };
        },
        .date_histogram => |dh| {
            var agg = aggregation_mod.SignedDateHistogramAgg.init(alloc, dh.interval);
            defer agg.deinit();

            for (scored) |hit| {
                if (try readTimestampForDoc(alloc, reads, snap, hit.doc_id, spec.field)) |ns| {
                    try agg.collect(ns);
                }
            }

            const keys = try agg.sortedKeys(alloc);
            errdefer alloc.free(keys);
            var counts = try alloc.alloc(u64, keys.len);
            for (keys, 0..) |k, i| {
                counts[i] = agg.getCount(k);
            }

            return .{ .date_histogram = .{ .keys = keys, .counts = counts } };
        },
        .range => |r| {
            var agg = try aggregation_mod.RangeAgg.init(alloc, r.ranges);
            defer agg.deinit();

            for (scored) |hit| {
                if (try readF64ForDoc(alloc, reads, snap, hit.doc_id, spec.field)) |val| {
                    agg.collect(val);
                }
            }

            return .{ .range = try alloc.dupe(aggregation_mod.RangeBucket, agg.buckets) };
        },
        .geo_distance => |gd| {
            var agg = try aggregation_mod.GeoDistanceAgg.init(alloc, gd.center, gd.ranges);
            defer agg.deinit();

            for (scored) |hit| {
                if (try readGeoPointForDoc(alloc, reads, snap, hit.doc_id, spec.field)) |pt| {
                    agg.collect(pt);
                }
            }

            return .{ .geo_distance = try alloc.dupe(aggregation_mod.GeoDistanceBand, agg.bands) };
        },
        .geohash_grid => |gg| {
            var agg = aggregation_mod.GeohashGridAgg.init(alloc, gg.precision);
            defer agg.deinit();

            for (scored) |hit| {
                if (try readGeoPointForDoc(alloc, reads, snap, hit.doc_id, spec.field)) |pt| {
                    try agg.collect(pt);
                }
            }

            const entries = try agg.topK(alloc, gg.top_k);
            return .{ .geohash_grid = entries };
        },
    }
}

/// Batched stats collection: iterates chunks per segment, decompresses each chunk
/// once, and collects all matching doc values in bulk using SIMD-friendly collectChunk.
/// Falls back to per-doc reads for non-f64/u64 types.
const StatsCandidates = struct {
    alloc: Allocator,
    snap: *const index_mod.IndexSnapshot,
    scored: []const scorer_mod.ScoredHit,
    docs: ?[]u32 = null,
    offsets: ?[]usize = null,
    count: usize = 0,

    fn init(alloc: Allocator, snap: *const index_mod.IndexSnapshot, scored: []const scorer_mod.ScoredHit) StatsCandidates {
        return .{ .alloc = alloc, .snap = snap, .scored = scored };
    }
    fn deinit(self: *StatsCandidates) void {
        if (self.docs) |docs| self.alloc.free(docs);
        if (self.offsets) |offsets| self.alloc.free(offsets);
    }
    fn prepare(self: *StatsCandidates) ![]const u32 {
        if (self.docs) |docs| return docs[0..self.count];
        // Sort global IDs once, then compact them to local IDs in place.
        // Segment offsets cost O(segments); candidate storage stays four bytes
        // per hit rather than carrying a segment index on every document.
        const docs = try self.alloc.alloc(u32, self.scored.len);
        errdefer self.alloc.free(docs);
        const offsets = try self.alloc.alloc(usize, self.snap.segments.len + 1);
        for (self.scored, docs) |hit, *id| id.* = hit.doc_id;
        std.mem.sort(u32, docs, {}, struct {
            fn less(_: void, x: u32, y: u32) bool {
                return x < y;
            }
        }.less);
        var count: usize = 0;
        var segment: usize = 0;
        offsets[0] = 0;
        var base: u64 = 0;
        for (docs) |global| {
            while (segment < self.snap.segments.len and global >= base + self.snap.segments[segment].reader.doc_count) : (segment += 1) {
                base += self.snap.segments[segment].reader.doc_count;
                offsets[segment + 1] = count;
            }
            if (segment == self.snap.segments.len) continue;
            docs[count] = @intCast(global - base);
            count += 1;
        }
        while (segment < self.snap.segments.len) : (segment += 1) offsets[segment + 1] = count;
        self.docs = docs;
        self.offsets = offsets;
        self.count = count;
        return docs[0..count];
    }
};

fn collectStatsBatched(
    alloc: Allocator,
    reads: *segment_mod.TypedReadScope,
    snap: *const index_mod.IndexSnapshot,
    scored: []const scorer_mod.ScoredHit,
    field: []const u8,
    stats: *aggregation_mod.StatsAgg,
) !void {
    var candidates = StatsCandidates.init(alloc, snap, scored);
    defer candidates.deinit();
    try collectStatsGrouped(reads, snap, &candidates, field, stats);
}

fn collectStatsGrouped(
    reads: *segment_mod.TypedReadScope,
    snap: *const index_mod.IndexSnapshot,
    candidates: *StatsCandidates,
    field: []const u8,
    stats: *aggregation_mod.StatsAgg,
) !void {
    const docs = try candidates.prepare();
    for (snap.segments, 0..) |*seg, seg_idx| {
        const local_ids = docs[candidates.offsets.?[seg_idx]..candidates.offsets.?[seg_idx + 1]];
        if (local_ids.len == 0) continue;
        const reader = (try reads.get(&seg.reader, field)) orelse continue;

        // Selective aggregations probe only their candidates. Dense scans use
        // one cursor and join sorted IDs in linear time without value arrays.
        if (local_ids.len <= reader.num_chunks) {
            var previous: ?u32 = null;
            for (local_ids) |id| {
                if (previous == id) continue;
                previous = id;
                if (try readNumericAsF64(reader, id)) |value| stats.collect(value);
            }
            continue;
        }
        var selected: usize = 0;
        // One decode per chunk, shared scratch, and no separate doc-ID/value
        // arrays. Scans and sparse point operations share the field owner.
        var cursor = typed_dv.TypedDocValuesReader.Cursor.init(reader);
        defer cursor.deinit();
        while (try cursor.next()) |entry| {
            if (entry.doc_id >= seg.reader.doc_count) return error.InvalidData;
            while (selected < local_ids.len and local_ids[selected] < entry.doc_id) : (selected += 1) {}
            if (selected == local_ids.len) break;
            if (local_ids[selected] != entry.doc_id) continue;
            const value: ?f64 = switch (entry.value) {
                .f64_val => |v| v,
                .u64_val => |v| @floatFromInt(v),
                .i64_val => |v| @floatFromInt(v),
                .numeric_val => |v| typed_dv.numericValueAsF64(v),
                else => null,
            };
            if (value) |v| stats.collect(v);
        }
    }
}

/// Read an f64 typed doc value for a global doc ID by resolving segment + field.
fn readF64ForDoc(
    alloc: Allocator,
    reads: *segment_mod.TypedReadScope,
    snap: *const index_mod.IndexSnapshot,
    global_id: u32,
    field: []const u8,
) !?f64 {
    _ = alloc;
    const resolved = snap.resolveDocId(global_id) orelse return null;
    const seg = &snap.segments[resolved.seg_idx];
    const reader = (try reads.get(&seg.reader, field)) orelse return null;
    return readNumericAsF64(reader, resolved.local_id);
}

fn readNumericAsF64(reader: *const typed_dv.TypedDocValuesReader, local_id: u32) !?f64 {
    return switch (reader.value_type) {
        .f64_val => try reader.getF64(local_id),
        .u64_val => {
            const v = try reader.getU64(local_id) orelse return null;
            return @floatFromInt(v);
        },
        .i64_val => {
            const v = try reader.getI64(local_id) orelse return null;
            return @floatFromInt(v);
        },
        .numeric_val => {
            const v = try reader.getNumeric(local_id) orelse return null;
            return typed_dv.numericValueAsF64(v);
        },
        else => null,
    };
}

/// Read a bytes typed doc value for a global doc ID. Caller owns returned slice.
fn readBytesForDoc(
    alloc: Allocator,
    reads: *segment_mod.TypedReadScope,
    snap: *const index_mod.IndexSnapshot,
    global_id: u32,
    field: []const u8,
) !?[]u8 {
    const resolved = snap.resolveDocId(global_id) orelse return null;
    const seg = &snap.segments[resolved.seg_idx];
    const reader = (try reads.get(&seg.reader, field)) orelse return null;
    if (reader.value_type != .bytes_val) return null;

    return reader.getBytesAllocWithAllocator(alloc, resolved.local_id);
}

/// Read signed timestamps, promoting legacy unsigned doc values exactly.
fn readTimestampForDoc(
    alloc: Allocator,
    reads: *segment_mod.TypedReadScope,
    snap: *const index_mod.IndexSnapshot,
    global_id: u32,
    field: []const u8,
) !?i128 {
    _ = alloc;
    const resolved = snap.resolveDocId(global_id) orelse return null;
    const seg = &snap.segments[resolved.seg_idx];
    const reader = (try reads.get(&seg.reader, field)) orelse return null;
    if (reader.value_type == .datetime_ns) return try reader.getDateTimeNs(resolved.local_id);
    if (reader.value_type != .u64_val) return null;
    return if (try reader.getU64(resolved.local_id)) |ns| @as(i128, ns) else null;
}

/// Read a geo_point typed doc value for a global doc ID.
fn readGeoPointForDoc(
    alloc: Allocator,
    reads: *segment_mod.TypedReadScope,
    snap: *const index_mod.IndexSnapshot,
    global_id: u32,
    field: []const u8,
) !?geo_mod.GeoPoint {
    _ = alloc;
    const resolved = snap.resolveDocId(global_id) orelse return null;
    const seg = &snap.segments[resolved.seg_idx];
    const reader = (try reads.get(&seg.reader, field)) orelse return null;
    if (reader.value_type != .geo_point) return null;
    const gp = try reader.getGeoPoint(resolved.local_id) orelse return null;
    return geo_mod.GeoPoint{ .lat = gp.lat, .lon = gp.lon };
}

// ============================================================================
// Tests
// ============================================================================

fn buildTestSegmentWithStoredDocs(alloc: Allocator, docs: []const struct {
    id: []const u8,
    data: []const u8,
    terms: []const inverted.InvertedIndexBuilder.TermHit,
}) ![]u8 {
    var inv_builder = inverted.InvertedIndexBuilder.init(alloc, .{});
    defer inv_builder.deinit();

    for (docs, 0..) |doc, i| {
        try inv_builder.addDocument(@intCast(i), doc.terms);
    }
    const inv_data = try inv_builder.build();
    defer alloc.free(inv_data);

    var seg_writer = segment_mod.SegmentWriter.init(alloc);
    defer seg_writer.deinit();
    const field_idx = try seg_writer.addField("title");
    try seg_writer.addSection(field_idx, .inverted_text, inv_data);

    for (docs) |doc| {
        try seg_writer.addStoredDoc(doc.id, doc.data);
    }

    return seg_writer.build();
}

test "search term query" {
    const alloc = std.testing.allocator;

    const seg_bytes = try buildTestSegmentWithStoredDocs(alloc, &.{
        .{ .id = "doc1", .data = "{\"title\":\"hello world\"}", .terms = &.{
            .{ .term = "hello", .freq = 1, .norm = 10 },
            .{ .term = "world", .freq = 1, .norm = 10 },
        } },
        .{ .id = "doc2", .data = "{\"title\":\"hello zig\"}", .terms = &.{
            .{ .term = "hello", .freq = 1, .norm = 10 },
            .{ .term = "zig", .freq = 1, .norm = 10 },
        } },
    });
    defer alloc.free(seg_bytes);

    var writer = try index_mod.IndexWriter.init(alloc);
    defer writer.deinit();
    try writer.addSegment(seg_bytes);

    const snap = writer.snapshot();
    var result = try execute(alloc, snap, .{
        .query = .{ .term = .{ .field = "title", .term = "hello" } },
        .k = 10,
    });
    defer result.deinit();

    try std.testing.expectEqual(@as(u32, 2), result.total_hits);
    try std.testing.expectEqual(@as(usize, 2), result.hits.len);
    // Both docs should have stored data
    try std.testing.expect(result.hits[0].stored_data != null);
    try std.testing.expect(result.hits[0].id != null);
}

test "external lake selected term and match scoring applies doc number predicates before top-k" {
    const a = std.testing.allocator;
    const segment = try buildTestSegmentWithStoredDocs(a, &.{
        .{ .id = "first", .data = "{}", .terms = &.{.{ .term = "hello", .freq = 1, .norm = 10 }} },
        .{ .id = "second", .data = "{}", .terms = &.{.{ .term = "hello", .freq = 1, .norm = 10 }} },
        .{ .id = "third", .data = "{}", .terms = &.{.{ .term = "hello", .freq = 1, .norm = 10 }} },
    });
    defer a.free(segment);
    var writer = try index_mod.IndexWriter.init(a);
    defer writer.deinit();
    try writer.addSegment(segment);
    const stats = [_]distributed_stats_mod.TextFieldStats{.{ .field = "title", .global_doc_count = 9, .global_total_field_len = 90, .term_doc_freqs = &.{.{ .term = "hello", .doc_freq = 3 }} }};
    for ([_]SearchQuery{ .{ .term = .{ .field = "title", .term = "hello" } }, .{ .match = .{ .field = "title", .text = "hello" } } }) |query| {
        for ([_]bool{ false, true }) |distributed| {
            var selected = try execute(a, writer.snapshot(), .{ .query = query, .k = 1, .include_stored = false, .filter_doc_nums = &.{2}, .filter_doc_nums_positive = true, .distributed_text_stats = if (distributed) &stats else &.{} });
            defer selected.deinit();
            try std.testing.expectEqual(@as(usize, 1), selected.hits.len);
            try std.testing.expectEqual(@as(u32, 2), selected.hits[0].doc_id);
            const expected_avg = if (distributed) stats[0].avgDocLen() else writer.snapshot().textAvgDocLen("title");
            try std.testing.expectApproxEqAbs(inverted.bm25Score(1, 10, if (distributed) 9 else 3, 3, expected_avg, .{}), selected.hits[0].score, 0.00001);
            var excluded = try execute(a, writer.snapshot(), .{ .query = query, .k = 1, .include_stored = false, .exclude_doc_nums = &.{ 0, 1 }, .distributed_text_stats = if (distributed) &stats else &.{} });
            defer excluded.deinit();
            try std.testing.expectEqual(@as(usize, 1), excluded.hits.len);
            try std.testing.expectEqual(@as(u32, 2), excluded.hits[0].doc_id);
        }
    }
}

test "bool fallback applies native doc number constraints" {
    const alloc = std.testing.allocator;

    const seg_bytes = try buildTestSegmentWithStoredDocs(alloc, &.{
        .{ .id = "doc1", .data = "{\"title\":\"hello one\"}", .terms = &.{
            .{ .term = "hello", .freq = 1, .norm = 10 },
        } },
        .{ .id = "doc2", .data = "{\"title\":\"hello two\"}", .terms = &.{
            .{ .term = "hello", .freq = 1, .norm = 10 },
        } },
        .{ .id = "doc3", .data = "{\"title\":\"hello three\"}", .terms = &.{
            .{ .term = "hello", .freq = 1, .norm = 10 },
        } },
    });
    defer alloc.free(seg_bytes);

    var writer = try index_mod.IndexWriter.init(alloc);
    defer writer.deinit();
    try writer.addSegment(seg_bytes);

    const stats_terms = [_]distributed_stats_mod.TermDocFreq{
        .{ .term = "hello", .doc_freq = 3 },
    };
    const distributed_stats = [_]distributed_stats_mod.TextFieldStats{
        .{
            .field = "title",
            .global_doc_count = 3,
            .global_total_field_len = 30,
            .term_doc_freqs = &stats_terms,
        },
    };
    const include_doc_nums = [_]u32{ 0, 2 };
    const exclude_doc_nums = [_]u32{2};

    const snap = writer.snapshot();
    var result = try execute(alloc, snap, .{
        .query = .{ .bool_query = .{
            .must = &.{.{ .term = .{ .field = "title", .term = "hello" } }},
        } },
        .k = 10,
        .distributed_text_stats = &distributed_stats,
        .filter_doc_nums = &include_doc_nums,
        .filter_doc_nums_positive = true,
        .exclude_doc_nums = &exclude_doc_nums,
    });
    defer result.deinit();

    try std.testing.expectEqual(@as(u32, 1), result.total_hits);
    try std.testing.expectEqual(@as(usize, 1), result.hits.len);
    try std.testing.expectEqual(@as(u32, 0), result.hits[0].doc_id);
    try std.testing.expectEqualStrings("doc1", result.hits[0].id.?);
}

test "match_all applies native doc number constraints (#931)" {
    const alloc = std.testing.allocator;

    const seg_bytes = try buildTestSegmentWithStoredDocs(alloc, &.{
        .{ .id = "doc1", .data = "{\"title\":\"hello one\"}", .terms = &.{
            .{ .term = "hello", .freq = 1, .norm = 10 },
        } },
        .{ .id = "doc2", .data = "{\"title\":\"hello two\"}", .terms = &.{
            .{ .term = "hello", .freq = 1, .norm = 10 },
        } },
        .{ .id = "doc3", .data = "{\"title\":\"hello three\"}", .terms = &.{
            .{ .term = "hello", .freq = 1, .norm = 10 },
        } },
    });
    defer alloc.free(seg_bytes);

    var writer = try index_mod.IndexWriter.init(alloc);
    defer writer.deinit();
    try writer.addSegment(seg_bytes);
    const snap = writer.snapshot();

    // Unlike every other query type (see "bool fallback applies native doc
    // number constraints" above), match_all never went through a path that
    // consulted filter_doc_nums/exclude_doc_nums, so a match_all query
    // silently ignored a resolved exclusion (exclusion_query/bool.must_not).
    const exclude_doc_nums = [_]u32{1};
    var excluded = try execute(alloc, snap, .{
        .query = .{ .match_all = {} },
        .k = 10,
        .exclude_doc_nums = &exclude_doc_nums,
    });
    defer excluded.deinit();
    try std.testing.expectEqual(@as(u32, 2), excluded.total_hits);
    try std.testing.expectEqual(@as(usize, 2), excluded.hits.len);
    for (excluded.hits) |hit| try std.testing.expect(hit.doc_id != 1);

    const include_doc_nums = [_]u32{ 0, 2 };
    var filtered = try execute(alloc, snap, .{
        .query = .{ .match_all = {} },
        .k = 10,
        .filter_doc_nums = &include_doc_nums,
        .filter_doc_nums_positive = true,
        .exclude_doc_nums = &exclude_doc_nums,
    });
    defer filtered.deinit();
    try std.testing.expectEqual(@as(u32, 2), filtered.total_hits);
    try std.testing.expectEqual(@as(usize, 2), filtered.hits.len);
    try std.testing.expectEqual(@as(u32, 0), filtered.hits[0].doc_id);
    try std.testing.expectEqual(@as(u32, 2), filtered.hits[1].doc_id);

    // Offset/limit paginate over the already-constrained set, not the raw one.
    var paged = try execute(alloc, snap, .{
        .query = .{ .match_all = {} },
        .k = 1,
        .offset = 1,
        .exclude_doc_nums = &exclude_doc_nums,
    });
    defer paged.deinit();
    try std.testing.expectEqual(@as(u32, 2), paged.total_hits);
    try std.testing.expectEqual(@as(usize, 1), paged.hits.len);
    try std.testing.expectEqual(@as(u32, 2), paged.hits[0].doc_id);
}

test "exact inclusive term range preserves prefix constant scores" {
    const alloc = std.testing.allocator;

    const seg_bytes = try buildTestSegmentWithStoredDocs(alloc, &.{
        .{ .id = "doc1", .data = "{}", .terms = &.{
            .{ .term = "app", .freq = 1, .norm = 10 },
            .{ .term = "apple", .freq = 1, .norm = 10 },
        } },
        .{ .id = "doc2", .data = "{}", .terms = &.{
            .{ .term = "app", .freq = 1, .norm = 10 },
            .{ .term = "application", .freq = 1, .norm = 10 },
        } },
        .{ .id = "doc3", .data = "{}", .terms = &.{
            .{ .term = "banana", .freq = 1, .norm = 10 },
        } },
    });
    defer alloc.free(seg_bytes);

    var writer = try index_mod.IndexWriter.init(alloc);
    defer writer.deinit();
    try writer.addSegment(seg_bytes);
    const snap = writer.snapshot();

    var prefix = try execute(alloc, snap, .{
        .query = .{ .prefix = .{ .field = "title", .prefix = "app", .boost = 2.5 } },
        .k = 10,
    });
    defer prefix.deinit();
    var exact = try execute(alloc, snap, .{
        .query = .{ .term_range = .{
            .field = "title",
            .min = "app",
            .max = "app",
            .inclusive_min = true,
            .inclusive_max = true,
            .boost = 2.5,
        } },
        .k = 10,
    });
    defer exact.deinit();

    try std.testing.expectEqual(prefix.total_hits, exact.total_hits);
    try std.testing.expectEqual(prefix.hits.len, exact.hits.len);
    for (prefix.hits, exact.hits) |prefix_hit, exact_hit| {
        try std.testing.expectEqual(prefix_hit.doc_id, exact_hit.doc_id);
        try std.testing.expectApproxEqAbs(prefix_hit.score, exact_hit.score, 0.00001);
        try std.testing.expectApproxEqAbs(@as(f32, 2.5), exact_hit.score, 0.00001);
    }
}

test "optional pure should preserves zero baseline and text scores" {
    const alloc = std.testing.allocator;
    const seg_bytes = try buildTestSegmentWithStoredDocs(alloc, &.{
        .{ .id = "doc1", .data = "{}", .terms = &.{
            .{ .term = "alpha", .freq = 1, .norm = 10 },
        } },
        .{ .id = "doc2", .data = "{}", .terms = &.{
            .{ .term = "beta", .freq = 1, .norm = 10 },
        } },
    });
    defer alloc.free(seg_bytes);

    var writer = try index_mod.IndexWriter.init(alloc);
    defer writer.deinit();
    try writer.addSegment(seg_bytes);
    const snap = writer.snapshot();
    const candidates = [_]u32{ 0, 1 };

    var direct = try execute(alloc, snap, .{
        .query = .{ .term = .{ .field = "title", .term = "alpha" } },
        .k = 10,
        .filter_doc_nums = &candidates,
        .filter_doc_nums_positive = true,
    });
    defer direct.deinit();
    const optional_query: SearchQuery = .{ .bool_query = .{
        .should = &.{.{ .term = .{ .field = "title", .term = "alpha" } }},
        .pure_should_optional = true,
    } };
    var optional = try execute(alloc, snap, .{
        .query = optional_query,
        .k = 10,
        .filter_doc_nums = &candidates,
        .filter_doc_nums_positive = true,
    });
    defer optional.deinit();

    try std.testing.expectEqual(@as(u32, 2), optional.total_hits);
    try std.testing.expectEqual(@as(usize, 2), optional.hits.len);
    try std.testing.expectEqualStrings("doc1", optional.hits[0].id.?);
    try std.testing.expectApproxEqAbs(direct.hits[0].score, optional.hits[0].score, 0.00001);
    try std.testing.expectEqual(@as(f32, 0.0), optional.hits[1].score);

    var filter_arena = std.heap.ArenaAllocator.init(alloc);
    defer filter_arena.deinit();
    const membership_filter = try searchQueryToFilterArena(
        filter_arena.allocator(),
        optional_query,
    );
    const membership = try snap.executeFilter(alloc, membership_filter);
    defer alloc.free(membership);
    try std.testing.expectEqualSlices(u32, &candidates, membership);
}

test "search match query with analysis" {
    const alloc = std.testing.allocator;

    // Pre-tokenized with stemmed terms (as if analyzer produced them)
    const seg_bytes = try buildTestSegmentWithStoredDocs(alloc, &.{
        .{ .id = "doc1", .data = "{}", .terms = &.{
            .{ .term = "run", .freq = 1, .norm = 10 },
            .{ .term = "dog", .freq = 1, .norm = 10 },
        } },
        .{ .id = "doc2", .data = "{}", .terms = &.{
            .{ .term = "walk", .freq = 1, .norm = 10 },
            .{ .term = "cat", .freq = 1, .norm = 10 },
        } },
    });
    defer alloc.free(seg_bytes);

    var writer = try index_mod.IndexWriter.init(alloc);
    defer writer.deinit();
    try writer.addSegment(seg_bytes);

    const snap = writer.snapshot();

    // Search for "running dogs" — analyzer should stem to "run" and "dog"
    var result = try execute(alloc, snap, .{
        .query = .{ .match = .{ .field = "title", .text = "running dogs" } },
        .k = 10,
    });
    defer result.deinit();

    // Should find doc1 which has "run" and "dog"
    try std.testing.expect(result.total_hits >= 1);
}

test "search match query can use distributed text stats for shard-consistent bm25" {
    const alloc = std.testing.allocator;

    const left_seg = try buildTestSegmentWithStoredDocs(alloc, &.{
        .{ .id = "doc1", .data = "{}", .terms = &.{
            .{ .term = "alpha", .freq = 1, .norm = 8 },
        } },
        .{ .id = "doc2", .data = "{}", .terms = &.{
            .{ .term = "alpha", .freq = 2, .norm = 12 },
            .{ .term = "beta", .freq = 1, .norm = 12 },
        } },
    });
    defer alloc.free(left_seg);

    const right_seg = try buildTestSegmentWithStoredDocs(alloc, &.{
        .{ .id = "doc3", .data = "{}", .terms = &.{
            .{ .term = "beta", .freq = 1, .norm = 8 },
        } },
        .{ .id = "doc4", .data = "{}", .terms = &.{
            .{ .term = "alpha", .freq = 1, .norm = 10 },
            .{ .term = "beta", .freq = 1, .norm = 10 },
        } },
    });
    defer alloc.free(right_seg);

    var combined_writer = try index_mod.IndexWriter.init(alloc);
    defer combined_writer.deinit();
    try combined_writer.addSegment(left_seg);
    try combined_writer.addSegment(right_seg);
    const combined_snap = combined_writer.snapshot();

    var left_writer = try index_mod.IndexWriter.init(alloc);
    defer left_writer.deinit();
    try left_writer.addSegment(left_seg);
    const left_snap = left_writer.snapshot();

    var right_writer = try index_mod.IndexWriter.init(alloc);
    defer right_writer.deinit();
    try right_writer.addSegment(right_seg);
    const right_snap = right_writer.snapshot();

    const distributed_stats = [_]distributed_stats_mod.TextFieldStats{.{
        .field = "title",
        .global_doc_count = combined_snap.liveDocCount(),
        .global_total_field_len = combined_snap.global_total_field_len.get("title") orelse 0,
        .term_doc_freqs = &.{
            .{ .term = "alpha", .doc_freq = try combined_snap.termDocFreq(alloc, "title", "alpha") },
            .{ .term = "beta", .doc_freq = try combined_snap.termDocFreq(alloc, "title", "beta") },
        },
    }};

    var combined = try execute(alloc, combined_snap, .{
        .query = .{ .match = .{ .field = "title", .text = "alpha beta" } },
        .k = 10,
    });
    defer combined.deinit();

    var left = try execute(alloc, left_snap, .{
        .query = .{ .match = .{ .field = "title", .text = "alpha beta" } },
        .k = 10,
        .distributed_text_stats = &distributed_stats,
    });
    defer left.deinit();

    var right = try execute(alloc, right_snap, .{
        .query = .{ .match = .{ .field = "title", .text = "alpha beta" } },
        .k = 10,
        .distributed_text_stats = &distributed_stats,
    });
    defer right.deinit();

    var merged = std.ArrayListUnmanaged(ScoredHit).empty;
    defer merged.deinit(alloc);
    try merged.appendSlice(alloc, left.hits);
    try merged.appendSlice(alloc, right.hits);
    std.mem.sort(ScoredHit, merged.items, {}, struct {
        fn lessThan(_: void, a: ScoredHit, b: ScoredHit) bool {
            if (a.score != b.score) return a.score > b.score;
            return std.mem.order(u8, a.id.?, b.id.?) == .lt;
        }
    }.lessThan);

    try std.testing.expectEqual(combined.hits.len, merged.items.len);
    for (combined.hits, merged.items) |combined_hit, merged_hit| {
        try std.testing.expectEqualStrings(combined_hit.id.?, merged_hit.id.?);
        try std.testing.expectApproxEqAbs(combined_hit.score, merged_hit.score, 0.0001);
    }
}

test "search bool conjunction query" {
    const alloc = std.testing.allocator;

    const seg_bytes = try buildTestSegmentWithStoredDocs(alloc, &.{
        .{ .id = "doc1", .data = "{}", .terms = &.{
            .{ .term = "alpha", .freq = 1, .norm = 10 },
            .{ .term = "beta", .freq = 1, .norm = 10 },
        } },
        .{ .id = "doc2", .data = "{}", .terms = &.{
            .{ .term = "alpha", .freq = 1, .norm = 10 },
        } },
        .{ .id = "doc3", .data = "{}", .terms = &.{
            .{ .term = "beta", .freq = 1, .norm = 10 },
        } },
    });
    defer alloc.free(seg_bytes);

    var writer = try index_mod.IndexWriter.init(alloc);
    defer writer.deinit();
    try writer.addSegment(seg_bytes);

    const snap = writer.snapshot();

    var result = try execute(alloc, snap, .{
        .query = .{ .bool_query = .{
            .must = &.{
                .{ .match = .{ .field = "title", .text = "alpha" } },
                .{ .match = .{ .field = "title", .text = "beta" } },
            },
        } },
        .k = 10,
    });
    defer result.deinit();

    try std.testing.expectEqual(@as(u32, 1), result.total_hits);
    try std.testing.expectEqual(@as(usize, 1), result.hits.len);
    try std.testing.expectEqualStrings("doc1", result.hits[0].id.?);

    var custom_bm25 = try execute(alloc, snap, .{
        .query = .{ .bool_query = .{
            .must = &.{
                .{ .match = .{ .field = "title", .text = "alpha" } },
                .{ .match = .{ .field = "title", .text = "beta" } },
            },
        } },
        .k = 10,
        .include_stored = false,
        .bm25_config = .{ .k1 = 2.0, .b = 0.0 },
    });
    defer custom_bm25.deinit();
    try std.testing.expectEqual(@as(usize, 1), custom_bm25.hits.len);
    try std.testing.expect(@abs(result.hits[0].score - custom_bm25.hits[0].score) > 0.0001);
}

test "search bool should-only query" {
    const alloc = std.testing.allocator;

    const seg_bytes = try buildTestSegmentWithStoredDocs(alloc, &.{
        .{ .id = "doc1", .data = "{}", .terms = &.{
            .{ .term = "alpha", .freq = 1, .norm = 10 },
        } },
        .{ .id = "doc2", .data = "{}", .terms = &.{
            .{ .term = "beta", .freq = 1, .norm = 10 },
        } },
    });
    defer alloc.free(seg_bytes);

    var writer = try index_mod.IndexWriter.init(alloc);
    defer writer.deinit();
    try writer.addSegment(seg_bytes);

    const snap = writer.snapshot();

    var result = try execute(alloc, snap, .{
        .query = .{ .bool_query = .{
            .should = &.{
                .{ .term = .{ .field = "title", .term = "alpha" } },
                .{ .term = .{ .field = "title", .term = "beta" } },
            },
        } },
        .k = 10,
    });
    defer result.deinit();

    try std.testing.expectEqual(@as(u32, 2), result.total_hits);
    try std.testing.expectEqual(@as(usize, 2), result.hits.len);
}

test "streaming boolean scorer matches all-hit reference on randomized corpus" {
    const alloc = std.testing.allocator;
    const vocabulary = [_][]const u8{ "alpha", "beta", "gamma", "delta" };
    var prng = std.Random.DefaultPrng.init(0x51a7_b001);
    const random = prng.random();

    var inv_builder = inverted.InvertedIndexBuilder.init(alloc, .{ .chunk_size = 8 });
    defer inv_builder.deinit();
    for (0..96) |doc_id| {
        var hits: [vocabulary.len]inverted.InvertedIndexBuilder.TermHit = undefined;
        var hit_count: usize = 0;
        for (vocabulary) |term| {
            if (!random.boolean()) continue;
            hits[hit_count] = .{
                .term = term,
                .freq = random.intRangeAtMost(u32, 1, 5),
                .norm = random.intRangeAtMost(u32, 4, 40),
            };
            hit_count += 1;
        }
        try inv_builder.addDocument(@intCast(doc_id), hits[0..hit_count]);
    }
    const inv_data = try inv_builder.build();
    defer alloc.free(inv_data);
    var seg_writer = segment_mod.SegmentWriter.init(alloc);
    defer seg_writer.deinit();
    const field_idx = try seg_writer.addField("title");
    try seg_writer.addSection(field_idx, .inverted_text, inv_data);
    for (0..96) |doc_id| {
        var id_buffer: [24]u8 = undefined;
        const id = try std.fmt.bufPrint(&id_buffer, "doc-{d}", .{doc_id});
        try seg_writer.addStoredDoc(id, "{}");
    }
    const segment = try seg_writer.build();
    defer alloc.free(segment);
    var writer = try index_mod.IndexWriter.init(alloc);
    defer writer.deinit();
    try writer.addSegment(segment);
    const snap = writer.snapshot();

    const term_queries = [_]SearchQuery{
        .{ .term = .{ .field = "title", .term = "alpha" } },
        .{ .term = .{ .field = "title", .term = "beta" } },
        .{ .term = .{ .field = "title", .term = "gamma" } },
        .{ .term = .{ .field = "title", .term = "delta" } },
    };
    for (0..48) |_| {
        var must: [2]SearchQuery = undefined;
        var should: [3]SearchQuery = undefined;
        var must_not: [1]SearchQuery = undefined;
        const must_len: usize = random.intRangeAtMost(usize, 0, must.len);
        const should_len: usize = random.intRangeAtMost(usize, 1, should.len);
        const must_not_len: usize = random.intRangeAtMost(usize, 0, must_not.len);
        for (must[0..must_len]) |*item| item.* = term_queries[random.uintLessThan(usize, term_queries.len)];
        for (should[0..should_len]) |*item| item.* = term_queries[random.uintLessThan(usize, term_queries.len)];
        for (must_not[0..must_not_len]) |*item| item.* = term_queries[random.uintLessThan(usize, term_queries.len)];
        const min_should: u32 = if (must_len == 0)
            random.intRangeAtMost(u32, 1, @intCast(should_len))
        else
            random.intRangeAtMost(u32, 0, @intCast(should_len));
        const bool_query = BoolQuery{
            .must = must[0..must_len],
            .should = should[0..should_len],
            .must_not = must_not[0..must_not_len],
            .min_should = min_should,
        };
        const request = SearchRequest{
            .query = .{ .bool_query = bool_query },
            .k = snap.liveDocCount(),
            .include_stored = false,
        };
        var streaming = (try executeSimpleTextBool(alloc, snap, bool_query, request)) orelse return error.TestExpectedEqual;
        defer streaming.deinit();
        var reference = try executeBoolAllHit(alloc, snap, bool_query, request);
        defer reference.deinit();
        try std.testing.expectEqual(reference.total_hits, streaming.total_hits);
        try std.testing.expectEqual(reference.hits.len, streaming.hits.len);
        for (reference.hits, streaming.hits) |expected, actual| {
            try std.testing.expectEqual(expected.doc_id, actual.doc_id);
            try std.testing.expectApproxEqAbs(expected.score, actual.score, 0.00001);
        }
    }
}

test "search pure bool should uses WAND top-k" {
    const alloc = std.testing.allocator;

    const seg_bytes = try buildTestSegmentWithStoredDocs(alloc, &.{
        .{ .id = "doc1", .data = "{}", .terms = &.{
            .{ .term = "alpha", .freq = 1, .norm = 10 },
        } },
        .{ .id = "doc2", .data = "{}", .terms = &.{
            .{ .term = "beta", .freq = 1, .norm = 10 },
        } },
        .{ .id = "doc3", .data = "{}", .terms = &.{
            .{ .term = "alpha", .freq = 2, .norm = 8 },
            .{ .term = "beta", .freq = 2, .norm = 8 },
        } },
    });
    defer alloc.free(seg_bytes);

    var writer = try index_mod.IndexWriter.init(alloc);
    defer writer.deinit();
    try writer.addSegment(seg_bytes);

    var result = try execute(alloc, writer.snapshot(), .{
        .query = .{ .bool_query = .{
            .should = &.{
                .{ .term = .{ .field = "title", .term = "alpha" } },
                .{ .term = .{ .field = "title", .term = "beta" } },
            },
        } },
        .k = 1,
        .include_stored = false,
    });
    defer result.deinit();

    try std.testing.expectEqual(@as(usize, 1), result.hits.len);
    try std.testing.expectEqual(@as(u32, 2), result.hits[0].doc_id);
    try std.testing.expectEqual(@as(u32, 3), result.total_hits);
    try std.testing.expectEqual(TotalHitsRelation.exact, result.total_hits_relation);
}

test "pure conjunction block pruning retains earliest cutoff ties" {
    const alloc = std.testing.allocator;
    var inv_builder = inverted.InvertedIndexBuilder.init(alloc, .{ .chunk_size = 4 });
    defer inv_builder.deinit();
    for (0..40) |doc_id| {
        try inv_builder.addDocument(@intCast(doc_id), &.{
            .{ .term = "alpha", .freq = 1, .norm = 10 },
            .{ .term = "beta", .freq = 1, .norm = 10 },
        });
    }
    const inv_data = try inv_builder.build();
    defer alloc.free(inv_data);
    var seg_writer = segment_mod.SegmentWriter.init(alloc);
    defer seg_writer.deinit();
    const field_idx = try seg_writer.addField("title");
    try seg_writer.addSection(field_idx, .inverted_text, inv_data);
    for (0..40) |doc_id| {
        var id_buf: [16]u8 = undefined;
        const id = try std.fmt.bufPrint(&id_buf, "doc-{d}", .{doc_id});
        try seg_writer.addStoredDoc(id, "{}");
    }
    const segment = try seg_writer.build();
    defer alloc.free(segment);
    var writer = try index_mod.IndexWriter.init(alloc);
    defer writer.deinit();
    try writer.addSegment(segment);
    var diagnostics: scorer_mod.SearchDiagnostics = .{};
    var result = try execute(alloc, writer.snapshot(), .{
        .query = .{ .bool_query = .{ .must = &.{
            .{ .term = .{ .field = "title", .term = "alpha" } },
            .{ .term = .{ .field = "title", .term = "beta" } },
        } } },
        .k = 2,
        .include_stored = false,
        .diagnostics = &diagnostics,
    });
    defer result.deinit();

    try std.testing.expectEqual(@as(usize, 2), result.hits.len);
    try std.testing.expectEqual(@as(u32, 0), result.hits[0].doc_id);
    try std.testing.expectEqual(@as(u32, 1), result.hits[1].doc_id);
    try std.testing.expectEqual(TotalHitsRelation.gte, result.total_hits_relation);
    try std.testing.expect(diagnostics.boolean_chunks_skipped > 0);
}

test "pure conjunction metadata scan preserves later competitive block" {
    const alloc = std.testing.allocator;
    var inv_builder = inverted.InvertedIndexBuilder.init(alloc, .{ .chunk_size = 2 });
    defer inv_builder.deinit();
    try inv_builder.addDocument(0, &.{
        .{ .term = "alpha", .freq = 1, .norm = 100 },
        .{ .term = "beta", .freq = 1, .norm = 100 },
    });
    try inv_builder.addDocument(1, &.{
        .{ .term = "alpha", .freq = 1, .norm = 100 },
        .{ .term = "beta", .freq = 1, .norm = 100 },
    });
    var empty_doc: u32 = 2;
    while (empty_doc < 1024) : (empty_doc += 1) try inv_builder.addDocument(empty_doc, &.{});
    try inv_builder.addDocument(1024, &.{
        .{ .term = "alpha", .freq = 100, .norm = 1 },
        .{ .term = "beta", .freq = 100, .norm = 1 },
    });
    const inv_data = try inv_builder.build();
    defer alloc.free(inv_data);
    var seg_writer = segment_mod.SegmentWriter.init(alloc);
    defer seg_writer.deinit();
    const field_idx = try seg_writer.addField("title");
    try seg_writer.addSection(field_idx, .inverted_text, inv_data);
    for (0..1025) |doc_id| {
        var id_buf: [16]u8 = undefined;
        const id = try std.fmt.bufPrint(&id_buf, "doc-{d}", .{doc_id});
        try seg_writer.addStoredDoc(id, "{}");
    }
    const segment = try seg_writer.build();
    defer alloc.free(segment);
    var writer = try index_mod.IndexWriter.init(alloc);
    defer writer.deinit();
    try writer.addSegment(segment);
    var diagnostics: scorer_mod.SearchDiagnostics = .{};
    var result = try execute(alloc, writer.snapshot(), .{
        .query = .{ .bool_query = .{ .must = &.{
            .{ .term = .{ .field = "title", .term = "alpha" } },
            .{ .term = .{ .field = "title", .term = "beta" } },
        } } },
        .k = 1,
        .include_stored = false,
        .diagnostics = &diagnostics,
    });
    defer result.deinit();

    try std.testing.expectEqual(@as(usize, 1), result.hits.len);
    try std.testing.expectEqual(@as(u32, 1024), result.hits[0].doc_id);
    try std.testing.expect(diagnostics.boolean_chunks_skipped > 0);
}

test "search bool query general path respects doc number constraints" {
    const alloc = std.testing.allocator;

    const seg_bytes = try buildTestSegmentWithStoredDocs(alloc, &.{
        .{ .id = "doc1", .data = "{}", .terms = &.{
            .{ .term = "alpha", .freq = 1, .norm = 10 },
        } },
        .{ .id = "doc2", .data = "{}", .terms = &.{
            .{ .term = "alpha", .freq = 1, .norm = 10 },
        } },
        .{ .id = "doc3", .data = "{}", .terms = &.{
            .{ .term = "alpha", .freq = 1, .norm = 10 },
            .{ .term = "blocked", .freq = 1, .norm = 10 },
        } },
    });
    defer alloc.free(seg_bytes);

    var writer = try index_mod.IndexWriter.init(alloc);
    defer writer.deinit();
    try writer.addSegment(seg_bytes);

    const snap = writer.snapshot();
    const filter_doc_nums = [_]u32{ 0, 1, 2 };
    const exclude_doc_nums = [_]u32{0};

    var result = try execute(alloc, snap, .{
        .query = .{ .bool_query = .{
            .must = &.{.{ .term = .{ .field = "title", .term = "alpha" } }},
            .must_not = &.{.{ .prefix = .{ .field = "title", .prefix = "blo" } }},
        } },
        .k = 10,
        .filter_doc_nums = &filter_doc_nums,
        .filter_doc_nums_positive = true,
        .exclude_doc_nums = &exclude_doc_nums,
    });
    defer result.deinit();

    try std.testing.expectEqual(@as(u32, 1), result.total_hits);
    try std.testing.expectEqual(@as(usize, 1), result.hits.len);
    try std.testing.expectEqual(@as(u32, 1), result.hits[0].doc_id);
    try std.testing.expectEqualStrings("doc2", result.hits[0].id.?);
}

test "count candidate scan matches empty analyzed query semantics" {
    const alloc = std.testing.allocator;

    const seg_bytes = try buildTestSegmentWithStoredDocs(alloc, &.{
        .{ .id = "doc1", .data = "{}", .terms = &.{
            .{ .term = "alpha", .freq = 1, .norm = 10 },
        } },
    });
    defer alloc.free(seg_bytes);

    var writer = try index_mod.IndexWriter.init(alloc);
    defer writer.deinit();
    try writer.addSegment(seg_bytes);

    const snap = writer.snapshot();

    var result = try executeCountCandidates(alloc, snap, .{
        .match = .{ .field = "title", .text = "" },
    });
    defer result.deinit();

    try std.testing.expectEqual(@as(u32, 0), result.total_hits);
    try std.testing.expectEqual(@as(usize, 0), result.hits.len);
}

test "count candidate scan respects bool min_should" {
    const alloc = std.testing.allocator;

    const seg_bytes = try buildTestSegmentWithStoredDocs(alloc, &.{
        .{ .id = "doc1", .data = "{}", .terms = &.{
            .{ .term = "alpha", .freq = 1, .norm = 10 },
            .{ .term = "beta", .freq = 1, .norm = 10 },
        } },
        .{ .id = "doc2", .data = "{}", .terms = &.{
            .{ .term = "alpha", .freq = 1, .norm = 10 },
        } },
    });
    defer alloc.free(seg_bytes);

    var writer = try index_mod.IndexWriter.init(alloc);
    defer writer.deinit();
    try writer.addSegment(seg_bytes);

    const snap = writer.snapshot();

    var result = try executeCountCandidates(alloc, snap, .{
        .bool_query = .{
            .should = &.{
                .{ .term = .{ .field = "title", .term = "alpha" } },
                .{ .term = .{ .field = "title", .term = "beta" } },
            },
            .min_should = 2,
        },
    });
    defer result.deinit();

    try std.testing.expectEqual(@as(u32, 1), result.total_hits);
    try std.testing.expectEqual(@as(usize, 1), result.hits.len);
    try std.testing.expectEqual(@as(u32, 0), result.hits[0].doc_id);
}

test "search match query with custom analyzer" {
    const alloc = std.testing.allocator;
    const tri_html_analyzer = analysis_mod.Analyzer{
        .char_filters = &.{.html_strip},
        .tokenizer = .whitespace,
        .filters = &.{ .lowercase, .{ .ngram = .{ .min = 3, .max = 3 } } },
    };

    const seg_bytes = try buildTestSegmentWithStoredDocs(alloc, &.{
        .{ .id = "doc1", .data = "{\"title\":\"<b>Hello</b>\"}", .terms = &.{
            .{ .term = "hel", .freq = 1, .norm = 3, .positions = &.{0} },
            .{ .term = "ell", .freq = 1, .norm = 3, .positions = &.{0} },
            .{ .term = "llo", .freq = 1, .norm = 3, .positions = &.{0} },
        } },
    });
    defer alloc.free(seg_bytes);

    var writer = try index_mod.IndexWriter.init(alloc);
    defer writer.deinit();
    try writer.addSegment(seg_bytes);

    const snap = writer.snapshot();

    var result = try execute(alloc, snap, .{
        .query = .{ .match = .{
            .field = "title",
            .text = "hello",
            .analyzer = &tri_html_analyzer,
        } },
        .k = 10,
    });
    defer result.deinit();

    try std.testing.expectEqual(@as(u32, 1), result.total_hits);
    try std.testing.expectEqual(@as(usize, 1), result.hits.len);
    try std.testing.expectEqualStrings("doc1", result.hits[0].id.?);
}

test "search phrase query with custom analyzer and shared positions" {
    const alloc = std.testing.allocator;
    const tri_html_analyzer = analysis_mod.Analyzer{
        .char_filters = &.{.html_strip},
        .tokenizer = .whitespace,
        .filters = &.{ .lowercase, .{ .ngram = .{ .min = 3, .max = 3 } } },
    };

    const seg_bytes = try buildTestSegmentWithStoredDocs(alloc, &.{
        .{ .id = "doc1", .data = "{\"title\":\"<b>Hello world</b>\"}", .terms = &.{
            .{ .term = "hel", .freq = 1, .norm = 6, .positions = &.{0} },
            .{ .term = "ell", .freq = 1, .norm = 6, .positions = &.{0} },
            .{ .term = "llo", .freq = 1, .norm = 6, .positions = &.{0} },
            .{ .term = "wor", .freq = 1, .norm = 6, .positions = &.{1} },
            .{ .term = "orl", .freq = 1, .norm = 6, .positions = &.{1} },
            .{ .term = "rld", .freq = 1, .norm = 6, .positions = &.{1} },
        } },
    });
    defer alloc.free(seg_bytes);

    var writer = try index_mod.IndexWriter.init(alloc);
    defer writer.deinit();
    try writer.addSegment(seg_bytes);

    const snap = writer.snapshot();

    var result = try execute(alloc, snap, .{
        .query = .{ .phrase = .{
            .field = "title",
            .text = "hello world",
            .analyzer = &tri_html_analyzer,
        } },
        .k = 10,
    });
    defer result.deinit();

    try std.testing.expectEqual(@as(u32, 1), result.total_hits);
    try std.testing.expectEqual(@as(usize, 1), result.hits.len);
    try std.testing.expectEqualStrings("doc1", result.hits[0].id.?);
}

test "search term phrase uses exact positional BM25 top-k" {
    const alloc = std.testing.allocator;
    const seg_bytes = try buildTestSegmentWithStoredDocs(alloc, &.{
        .{ .id = "doc1", .data = "{}", .terms = &.{
            .{ .term = "hello", .freq = 1, .norm = 10, .positions = &.{0} },
            .{ .term = "world", .freq = 1, .norm = 10, .positions = &.{1} },
        } },
        .{ .id = "doc2", .data = "{}", .terms = &.{
            .{ .term = "hello", .freq = 3, .norm = 5, .positions = &.{ 0, 2, 4 } },
            .{ .term = "world", .freq = 2, .norm = 5, .positions = &.{ 1, 3 } },
        } },
        .{ .id = "doc3", .data = "{}", .terms = &.{
            .{ .term = "hello", .freq = 1, .norm = 10, .positions = &.{1} },
            .{ .term = "world", .freq = 1, .norm = 10, .positions = &.{0} },
        } },
    });
    defer alloc.free(seg_bytes);

    var writer = try index_mod.IndexWriter.init(alloc);
    defer writer.deinit();
    try writer.addSegment(seg_bytes);

    var result = try execute(alloc, writer.snapshot(), .{
        .query = .{ .term_phrase = .{ .field = "title", .terms = &.{ "hello", "world" } } },
        .k = 1,
        .include_stored = false,
    });
    defer result.deinit();
    try std.testing.expectEqual(@as(u32, 2), result.total_hits);
    try std.testing.expectEqual(TotalHitsRelation.exact, result.total_hits_relation);
    try std.testing.expectEqual(@as(usize, 1), result.hits.len);
    try std.testing.expectEqual(@as(u32, 1), result.hits[0].doc_id);
    try std.testing.expect(result.hits[0].score > 0);
    const snap = writer.snapshot();
    const phrase_idf = inverted.bm25Idf(3, 3) * 2.0;
    const expected_score = inverted.bm25ScoreWithIdf(2, 5, snap.textAvgDocLen("title"), phrase_idf, .{});
    try std.testing.expectApproxEqAbs(expected_score, result.hits[0].score, 0.00001);

    var custom_bm25 = try execute(alloc, writer.snapshot(), .{
        .query = .{ .term_phrase = .{ .field = "title", .terms = &.{ "hello", "world" } } },
        .k = 1,
        .include_stored = false,
        .bm25_config = .{ .k1 = 2.0, .b = 0.0 },
    });
    defer custom_bm25.deinit();
    try std.testing.expectEqual(@as(u32, 1), custom_bm25.hits[0].doc_id);
    try std.testing.expect(@abs(result.hits[0].score - custom_bm25.hits[0].score) > 0.0001);
}

test "streaming phrase scorer matches randomized positional reference" {
    const alloc = std.testing.allocator;
    const document_count = 64;
    const document_length = 16;
    var prng = std.Random.DefaultPrng.init(0xface_5102);
    const random = prng.random();
    var expected_frequency: [document_count]u32 = @splat(0);
    var alpha_df: u32 = 0;
    var beta_df: u32 = 0;

    var inv_builder = inverted.InvertedIndexBuilder.init(alloc, .{ .chunk_size = 8 });
    defer inv_builder.deinit();
    for (0..document_count) |doc_id| {
        var alpha_positions: [document_length]u32 = undefined;
        var beta_positions: [document_length]u32 = undefined;
        var alpha_len: usize = 0;
        var beta_len: usize = 0;
        var previous_alpha = false;
        for (0..document_length) |position| {
            const token = random.uintLessThan(u8, 3);
            const is_alpha = token == 0;
            const is_beta = token == 1;
            if (is_alpha) {
                alpha_positions[alpha_len] = @intCast(position);
                alpha_len += 1;
            } else if (is_beta) {
                beta_positions[beta_len] = @intCast(position);
                beta_len += 1;
            }
            if (previous_alpha and is_beta) expected_frequency[doc_id] += 1;
            previous_alpha = is_alpha;
        }
        var hits: [2]inverted.InvertedIndexBuilder.TermHit = undefined;
        var hit_len: usize = 0;
        if (alpha_len > 0) {
            alpha_df += 1;
            hits[hit_len] = .{ .term = "alpha", .freq = @intCast(alpha_len), .norm = document_length, .positions = alpha_positions[0..alpha_len] };
            hit_len += 1;
        }
        if (beta_len > 0) {
            beta_df += 1;
            hits[hit_len] = .{ .term = "beta", .freq = @intCast(beta_len), .norm = document_length, .positions = beta_positions[0..beta_len] };
            hit_len += 1;
        }
        try inv_builder.addDocument(@intCast(doc_id), hits[0..hit_len]);
    }
    const inv_data = try inv_builder.build();
    defer alloc.free(inv_data);
    var segment_writer = segment_mod.SegmentWriter.init(alloc);
    defer segment_writer.deinit();
    const field_index = try segment_writer.addField("title");
    try segment_writer.addSection(field_index, .inverted_text, inv_data);
    for (0..document_count) |doc_id| {
        var id_buffer: [24]u8 = undefined;
        const id = try std.fmt.bufPrint(&id_buffer, "phrase-{d}", .{doc_id});
        try segment_writer.addStoredDoc(id, "{}");
    }
    const segment = try segment_writer.build();
    defer alloc.free(segment);
    var writer = try index_mod.IndexWriter.init(alloc);
    defer writer.deinit();
    try writer.addSegment(segment);

    var diagnostics: scorer_mod.SearchDiagnostics = .{};
    var result = try execute(alloc, writer.snapshot(), .{
        .query = .{ .term_phrase = .{ .field = "title", .terms = &.{ "alpha", "beta" } } },
        .k = document_count,
        .include_stored = false,
        .diagnostics = &diagnostics,
    });
    defer result.deinit();

    var expected = std.ArrayListUnmanaged(scorer_mod.ScoredHit).empty;
    defer expected.deinit(alloc);
    const idf_sum = inverted.bm25Idf(document_count, alpha_df) + inverted.bm25Idf(document_count, beta_df);
    const average_document_length = writer.snapshot().textAvgDocLen("title");
    for (expected_frequency, 0..) |frequency, doc_id| {
        if (frequency == 0) continue;
        try expected.append(alloc, .{
            .doc_id = @intCast(doc_id),
            .score = inverted.bm25ScoreWithIdf(frequency, document_length, average_document_length, idf_sum, .{}),
        });
    }
    scorer_mod.sortScoredHits(expected.items);
    try std.testing.expectEqual(@as(u32, @intCast(expected.items.len)), result.total_hits);
    try std.testing.expectEqual(expected.items.len, result.hits.len);
    for (expected.items, result.hits) |reference, actual| {
        try std.testing.expectEqual(reference.doc_id, actual.doc_id);
        try std.testing.expectApproxEqAbs(reference.score, actual.score, 0.00001);
    }
    try std.testing.expectEqual(
        diagnostics.phrase_candidates_verified * 2,
        diagnostics.phrase_position_records_decoded,
    );
}

test "search with pagination offset" {
    const alloc = std.testing.allocator;

    const seg_bytes = try buildTestSegmentWithStoredDocs(alloc, &.{
        .{ .id = "a", .data = "{}", .terms = &.{.{ .term = "x", .freq = 3, .norm = 10 }} },
        .{ .id = "b", .data = "{}", .terms = &.{.{ .term = "x", .freq = 2, .norm = 10 }} },
        .{ .id = "c", .data = "{}", .terms = &.{.{ .term = "x", .freq = 1, .norm = 10 }} },
    });
    defer alloc.free(seg_bytes);

    var writer = try index_mod.IndexWriter.init(alloc);
    defer writer.deinit();
    try writer.addSegment(seg_bytes);

    const snap = writer.snapshot();

    // Page 1: first result
    var r1 = try execute(alloc, snap, .{
        .query = .{ .term = .{ .field = "title", .term = "x" } },
        .k = 1,
        .offset = 0,
    });
    defer r1.deinit();
    try std.testing.expectEqual(@as(usize, 1), r1.hits.len);
    try std.testing.expect(r1.total_hits >= 1);

    // Page 2: second result
    var r2 = try execute(alloc, snap, .{
        .query = .{ .term = .{ .field = "title", .term = "x" } },
        .k = 1,
        .offset = 1,
    });
    defer r2.deinit();
    try std.testing.expectEqual(@as(usize, 1), r2.hits.len);
    // Second page should have a different doc
    try std.testing.expect(r2.hits[0].doc_id != r1.hits[0].doc_id);
}

test "resolveDocId across segments" {
    const alloc = std.testing.allocator;

    const seg1 = try buildTestSegmentWithStoredDocs(alloc, &.{
        .{ .id = "a", .data = "data_a", .terms = &.{.{ .term = "x", .freq = 1, .norm = 10 }} },
        .{ .id = "b", .data = "data_b", .terms = &.{.{ .term = "x", .freq = 1, .norm = 10 }} },
    });
    defer alloc.free(seg1);

    const seg2 = try buildTestSegmentWithStoredDocs(alloc, &.{
        .{ .id = "c", .data = "data_c", .terms = &.{.{ .term = "x", .freq = 1, .norm = 10 }} },
    });
    defer alloc.free(seg2);

    var writer = try index_mod.IndexWriter.init(alloc);
    defer writer.deinit();
    try writer.addSegment(seg1);
    try writer.addSegment(seg2);

    const snap = writer.snapshot();

    // Doc 0 → seg 0, local 0
    const r0 = snap.resolveDocId(0).?;
    try std.testing.expectEqual(@as(usize, 0), r0.seg_idx);
    try std.testing.expectEqual(@as(u32, 0), r0.local_id);

    // Doc 1 → seg 0, local 1
    const r1 = snap.resolveDocId(1).?;
    try std.testing.expectEqual(@as(usize, 0), r1.seg_idx);
    try std.testing.expectEqual(@as(u32, 1), r1.local_id);

    // Doc 2 → seg 1, local 0
    const r2 = snap.resolveDocId(2).?;
    try std.testing.expectEqual(@as(usize, 1), r2.seg_idx);
    try std.testing.expectEqual(@as(u32, 0), r2.local_id);

    // Doc 3 → out of range
    try std.testing.expect(snap.resolveDocId(3) == null);

    // storedDoc by global ID
    const stored = (try snap.storedDoc(2)).?;
    try std.testing.expectEqualStrings("c", stored.id);
}

test "search with stats aggregation" {
    const alloc = std.testing.allocator;

    // Build inverted index
    var inv_builder = inverted.InvertedIndexBuilder.init(alloc, .{});
    defer inv_builder.deinit();
    try inv_builder.addDocument(0, &.{.{ .term = "hello", .freq = 1, .norm = 10 }});
    try inv_builder.addDocument(1, &.{.{ .term = "hello", .freq = 1, .norm = 10 }});
    try inv_builder.addDocument(2, &.{.{ .term = "hello", .freq = 1, .norm = 10 }});
    const inv_data = try inv_builder.build();
    defer alloc.free(inv_data);

    // Build typed doc values for a "price" field
    var dv_writer = typed_dv.TypedDocValuesWriter.init(alloc, .numeric_val, 1024);
    defer dv_writer.deinit();
    try dv_writer.add(0, .{ .numeric_val = .{ .i64_val = 10 } });
    try dv_writer.add(1, .{ .numeric_val = .{ .f64_val = 20.0 } });
    try dv_writer.add(2, .{ .numeric_val = .{ .u64_val = 30 } });
    const dv_data = try dv_writer.build();
    defer alloc.free(dv_data);

    // Build segment with both inverted + typed doc values
    var seg_writer = segment_mod.SegmentWriter.init(alloc);
    defer seg_writer.deinit();
    const title_idx = try seg_writer.addField("title");
    try seg_writer.addSection(title_idx, .inverted_text, inv_data);
    const price_idx = try seg_writer.addField("price");
    try seg_writer.addSection(price_idx, .typed_doc_values, dv_data);
    try seg_writer.addStoredDoc("d1", "{}");
    try seg_writer.addStoredDoc("d2", "{}");
    try seg_writer.addStoredDoc("d3", "{}");
    const seg_bytes = try seg_writer.build();
    defer alloc.free(seg_bytes);

    var writer = try index_mod.IndexWriter.init(alloc);
    defer writer.deinit();
    try writer.addSegment(seg_bytes);

    const snap = writer.snapshot();
    var result = try execute(alloc, snap, .{
        .query = .{ .term = .{ .field = "title", .term = "hello" } },
        .k = 10,
        .aggregations = &.{
            .{ .name = "price_stats", .field = "price", .agg_type = .{ .stats = {} } },
        },
    });
    defer result.deinit();

    try std.testing.expectEqual(@as(u32, 3), result.total_hits);
    try std.testing.expectEqual(@as(usize, 1), result.aggregations.len);
    try std.testing.expectEqualStrings("price_stats", result.aggregations[0].name);

    const stats = result.aggregations[0].result.stats;
    try std.testing.expectEqual(@as(u64, 3), stats.count);
    try std.testing.expectApproxEqAbs(@as(f64, 10.0), stats.min, 0.001);
    try std.testing.expectApproxEqAbs(@as(f64, 30.0), stats.max, 0.001);
    try std.testing.expectApproxEqAbs(@as(f64, 60.0), stats.sum, 0.001);
    try std.testing.expectApproxEqAbs(@as(f64, 20.0), stats.avg(), 0.001);
}

test "search with histogram aggregation" {
    const alloc = std.testing.allocator;

    var inv_builder = inverted.InvertedIndexBuilder.init(alloc, .{});
    defer inv_builder.deinit();
    try inv_builder.addDocument(0, &.{.{ .term = "x", .freq = 1, .norm = 10 }});
    try inv_builder.addDocument(1, &.{.{ .term = "x", .freq = 1, .norm = 10 }});
    try inv_builder.addDocument(2, &.{.{ .term = "x", .freq = 1, .norm = 10 }});
    try inv_builder.addDocument(3, &.{.{ .term = "x", .freq = 1, .norm = 10 }});
    const inv_data = try inv_builder.build();
    defer alloc.free(inv_data);

    var dv_writer = typed_dv.TypedDocValuesWriter.init(alloc, .f64_val, 1024);
    defer dv_writer.deinit();
    try dv_writer.add(0, .{ .f64_val = 5.0 });
    try dv_writer.add(1, .{ .f64_val = 15.0 });
    try dv_writer.add(2, .{ .f64_val = 7.0 });
    try dv_writer.add(3, .{ .f64_val = 25.0 });
    const dv_data = try dv_writer.build();
    defer alloc.free(dv_data);

    var seg_writer = segment_mod.SegmentWriter.init(alloc);
    defer seg_writer.deinit();
    const title_idx = try seg_writer.addField("title");
    try seg_writer.addSection(title_idx, .inverted_text, inv_data);
    const price_idx = try seg_writer.addField("price");
    try seg_writer.addSection(price_idx, .typed_doc_values, dv_data);
    for (0..4) |_| try seg_writer.addStoredDoc("d", "{}");
    const seg_bytes = try seg_writer.build();
    defer alloc.free(seg_bytes);

    var writer = try index_mod.IndexWriter.init(alloc);
    defer writer.deinit();
    try writer.addSegment(seg_bytes);

    const snap = writer.snapshot();
    var result = try execute(alloc, snap, .{
        .query = .{ .term = .{ .field = "title", .term = "x" } },
        .k = 10,
        .aggregations = &.{
            .{ .name = "price_hist", .field = "price", .agg_type = .{ .histogram = .{ .interval = 10.0 } } },
        },
    });
    defer result.deinit();

    try std.testing.expectEqual(@as(usize, 1), result.aggregations.len);
    const hist = result.aggregations[0].result.histogram;
    // Buckets: 0 (5.0, 7.0), 1 (15.0), 2 (25.0)
    try std.testing.expectEqual(@as(usize, 3), hist.keys.len);
    try std.testing.expectEqual(@as(i64, 0), hist.keys[0]);
    try std.testing.expectEqual(@as(u64, 2), hist.counts[0]);
    try std.testing.expectEqual(@as(i64, 1), hist.keys[1]);
    try std.testing.expectEqual(@as(u64, 1), hist.counts[1]);
    try std.testing.expectEqual(@as(i64, 2), hist.keys[2]);
    try std.testing.expectEqual(@as(u64, 1), hist.counts[2]);
}

test "search with sort-by-field" {
    const alloc = std.testing.allocator;

    var inv_builder = inverted.InvertedIndexBuilder.init(alloc, .{});
    defer inv_builder.deinit();
    try inv_builder.addDocument(0, &.{.{ .term = "x", .freq = 1, .norm = 10 }});
    try inv_builder.addDocument(1, &.{.{ .term = "x", .freq = 1, .norm = 10 }});
    try inv_builder.addDocument(2, &.{.{ .term = "x", .freq = 1, .norm = 10 }});
    const inv_data = try inv_builder.build();
    defer alloc.free(inv_data);

    var dv_writer = typed_dv.TypedDocValuesWriter.init(alloc, .f64_val, 1024);
    defer dv_writer.deinit();
    try dv_writer.add(0, .{ .f64_val = 30.0 });
    try dv_writer.add(1, .{ .f64_val = 10.0 });
    try dv_writer.add(2, .{ .f64_val = 20.0 });
    const dv_data = try dv_writer.build();
    defer alloc.free(dv_data);

    var seg_writer = segment_mod.SegmentWriter.init(alloc);
    defer seg_writer.deinit();
    const title_idx = try seg_writer.addField("title");
    try seg_writer.addSection(title_idx, .inverted_text, inv_data);
    const price_idx = try seg_writer.addField("price");
    try seg_writer.addSection(price_idx, .typed_doc_values, dv_data);
    try seg_writer.addStoredDoc("expensive", "{}");
    try seg_writer.addStoredDoc("cheap", "{}");
    try seg_writer.addStoredDoc("mid", "{}");
    const seg_bytes = try seg_writer.build();
    defer alloc.free(seg_bytes);

    var writer = try index_mod.IndexWriter.init(alloc);
    defer writer.deinit();
    try writer.addSegment(seg_bytes);

    const snap = writer.snapshot();

    // Sort ascending by price
    var result_asc = try execute(alloc, snap, .{
        .query = .{ .term = .{ .field = "title", .term = "x" } },
        .k = 10,
        .sort = .{ .field = "price", .order = .asc },
    });
    defer result_asc.deinit();

    try std.testing.expectEqual(@as(u32, 3), result_asc.total_hits);
    // Ascending: 10.0, 20.0, 30.0 → doc 1, doc 2, doc 0
    try std.testing.expectApproxEqAbs(@as(f32, 10.0), result_asc.hits[0].score, 0.001);
    try std.testing.expectApproxEqAbs(@as(f32, 20.0), result_asc.hits[1].score, 0.001);
    try std.testing.expectApproxEqAbs(@as(f32, 30.0), result_asc.hits[2].score, 0.001);

    // Sort descending by price
    var result_desc = try execute(alloc, snap, .{
        .query = .{ .term = .{ .field = "title", .term = "x" } },
        .k = 10,
        .sort = .{ .field = "price", .order = .desc },
    });
    defer result_desc.deinit();

    try std.testing.expectApproxEqAbs(@as(f32, 30.0), result_desc.hits[0].score, 0.001);
    try std.testing.expectApproxEqAbs(@as(f32, 20.0), result_desc.hits[1].score, 0.001);
    try std.testing.expectApproxEqAbs(@as(f32, 10.0), result_desc.hits[2].score, 0.001);
}

test "search with cursor pagination" {
    const alloc = std.testing.allocator;

    const seg_bytes = try buildTestSegmentWithStoredDocs(alloc, &.{
        .{ .id = "a", .data = "{}", .terms = &.{.{ .term = "x", .freq = 3, .norm = 10 }} },
        .{ .id = "b", .data = "{}", .terms = &.{.{ .term = "x", .freq = 2, .norm = 10 }} },
        .{ .id = "c", .data = "{}", .terms = &.{.{ .term = "x", .freq = 1, .norm = 10 }} },
    });
    defer alloc.free(seg_bytes);

    var writer = try index_mod.IndexWriter.init(alloc);
    defer writer.deinit();
    try writer.addSegment(seg_bytes);

    const snap = writer.snapshot();

    // Page 1: get first result
    var r1 = try execute(alloc, snap, .{
        .query = .{ .term = .{ .field = "title", .term = "x" } },
        .k = 1,
    });
    defer r1.deinit();
    try std.testing.expectEqual(@as(usize, 1), r1.hits.len);
    try std.testing.expect(r1.cursor != null);

    // Page 2: use cursor from page 1
    var r2 = try execute(alloc, snap, .{
        .query = .{ .term = .{ .field = "title", .term = "x" } },
        .k = 1,
        .search_after = r1.cursor,
    });
    defer r2.deinit();
    try std.testing.expectEqual(@as(usize, 1), r2.hits.len);
    // Should be a different doc
    try std.testing.expect(r2.hits[0].doc_id != r1.hits[0].doc_id);
    try std.testing.expect(r2.cursor != null);

    // Page 3: use cursor from page 2
    var r3 = try execute(alloc, snap, .{
        .query = .{ .term = .{ .field = "title", .term = "x" } },
        .k = 1,
        .search_after = r2.cursor,
    });
    defer r3.deinit();
    try std.testing.expectEqual(@as(usize, 1), r3.hits.len);
    try std.testing.expect(r3.hits[0].doc_id != r2.hits[0].doc_id);
    try std.testing.expect(r3.hits[0].doc_id != r1.hits[0].doc_id);

    // Page 4: should be empty
    var r4 = try execute(alloc, snap, .{
        .query = .{ .term = .{ .field = "title", .term = "x" } },
        .k = 1,
        .search_after = r3.cursor,
    });
    defer r4.deinit();
    try std.testing.expectEqual(@as(usize, 0), r4.hits.len);
    try std.testing.expect(r4.cursor == null);
}

test "KNN search missing index returns empty" {
    const alloc = std.testing.allocator;

    const seg_bytes = try buildTestSegmentWithStoredDocs(alloc, &.{
        .{ .id = "doc1", .data = "{}", .terms = &.{.{ .term = "x", .freq = 1, .norm = 10 }} },
    });
    defer alloc.free(seg_bytes);

    var writer = try index_mod.IndexWriter.init(alloc);
    defer writer.deinit();
    try writer.addSegment(seg_bytes);

    const snap = writer.snapshot();
    var result = try execute(alloc, snap, .{
        .query = .{ .knn = .{ .index_name = "nonexistent", .vector = &.{ 1.0, 2.0, 3.0 }, .k = 5 } },
        .k = 10,
    });
    defer result.deinit();

    try std.testing.expectEqual(@as(u32, 0), result.total_hits);
    try std.testing.expectEqual(@as(usize, 0), result.hits.len);
}

test "KNN score conversion distance to score" {
    // Verify the score formula: score = 1.0 / (1.0 + distance)
    // distance=0 → score=1.0, distance=1 → score=0.5, distance=3 → score=0.25
    const score0: f32 = 1.0 / (1.0 + 0.0);
    try std.testing.expectApproxEqAbs(@as(f32, 1.0), score0, 0.001);

    const score1: f32 = 1.0 / (1.0 + 1.0);
    try std.testing.expectApproxEqAbs(@as(f32, 0.5), score1, 0.001);

    const score3: f32 = 1.0 / (1.0 + 3.0);
    try std.testing.expectApproxEqAbs(@as(f32, 0.25), score3, 0.001);
}

test "search with date histogram aggregation" {
    const alloc = std.testing.allocator;

    // Build inverted index (all docs match "x")
    var inv_builder = inverted.InvertedIndexBuilder.init(alloc, .{});
    defer inv_builder.deinit();
    try inv_builder.addDocument(0, &.{.{ .term = "x", .freq = 1, .norm = 10 }});
    try inv_builder.addDocument(1, &.{.{ .term = "x", .freq = 1, .norm = 10 }});
    try inv_builder.addDocument(2, &.{.{ .term = "x", .freq = 1, .norm = 10 }});
    const inv_data = try inv_builder.build();
    defer alloc.free(inv_data);

    // Build u64 typed doc values for "timestamp" field (nanoseconds)
    const ns_per_s: u64 = 1_000_000_000;
    var dv_writer = typed_dv.TypedDocValuesWriter.init(alloc, .u64_val, 1024);
    defer dv_writer.deinit();
    // Two timestamps in hour 0, one in hour 1
    const base_day: u64 = 19737 * 86400;
    try dv_writer.add(0, .{ .u64_val = (base_day + 1800) * ns_per_s }); // 00:30
    try dv_writer.add(1, .{ .u64_val = (base_day + 1200) * ns_per_s }); // 00:20
    try dv_writer.add(2, .{ .u64_val = (base_day + 5400) * ns_per_s }); // 01:30
    const dv_data = try dv_writer.build();
    defer alloc.free(dv_data);

    var seg_writer = segment_mod.SegmentWriter.init(alloc);
    defer seg_writer.deinit();
    const title_idx = try seg_writer.addField("title");
    try seg_writer.addSection(title_idx, .inverted_text, inv_data);
    const ts_idx = try seg_writer.addField("timestamp");
    try seg_writer.addSection(ts_idx, .typed_doc_values, dv_data);
    try seg_writer.addStoredDoc("d1", "{}");
    try seg_writer.addStoredDoc("d2", "{}");
    try seg_writer.addStoredDoc("d3", "{}");
    const seg_bytes = try seg_writer.build();
    defer alloc.free(seg_bytes);

    var writer = try index_mod.IndexWriter.init(alloc);
    defer writer.deinit();
    try writer.addSegment(seg_bytes);

    const snap = writer.snapshot();
    var result = try execute(alloc, snap, .{
        .query = .{ .term = .{ .field = "title", .term = "x" } },
        .k = 10,
        .aggregations = &.{
            .{ .name = "by_hour", .field = "timestamp", .agg_type = .{ .date_histogram = .{ .interval = .hour } } },
        },
    });
    defer result.deinit();

    try std.testing.expectEqual(@as(usize, 1), result.aggregations.len);
    const dh = result.aggregations[0].result.date_histogram;
    try std.testing.expectEqual(@as(usize, 2), dh.keys.len);
    // Hour 0: 2 docs, hour 1: 1 doc
    try std.testing.expectEqual(@as(u64, 2), dh.counts[0]);
    try std.testing.expectEqual(@as(u64, 1), dh.counts[1]);
}

test "search with range aggregation" {
    const alloc = std.testing.allocator;

    var inv_builder = inverted.InvertedIndexBuilder.init(alloc, .{});
    defer inv_builder.deinit();
    try inv_builder.addDocument(0, &.{.{ .term = "x", .freq = 1, .norm = 10 }});
    try inv_builder.addDocument(1, &.{.{ .term = "x", .freq = 1, .norm = 10 }});
    try inv_builder.addDocument(2, &.{.{ .term = "x", .freq = 1, .norm = 10 }});
    const inv_data = try inv_builder.build();
    defer alloc.free(inv_data);

    var dv_writer = typed_dv.TypedDocValuesWriter.init(alloc, .f64_val, 1024);
    defer dv_writer.deinit();
    try dv_writer.add(0, .{ .f64_val = 15.0 });
    try dv_writer.add(1, .{ .f64_val = 50.0 });
    try dv_writer.add(2, .{ .f64_val = 150.0 });
    const dv_data = try dv_writer.build();
    defer alloc.free(dv_data);

    var seg_writer = segment_mod.SegmentWriter.init(alloc);
    defer seg_writer.deinit();
    const title_idx = try seg_writer.addField("title");
    try seg_writer.addSection(title_idx, .inverted_text, inv_data);
    const price_idx = try seg_writer.addField("price");
    try seg_writer.addSection(price_idx, .typed_doc_values, dv_data);
    try seg_writer.addStoredDoc("d1", "{}");
    try seg_writer.addStoredDoc("d2", "{}");
    try seg_writer.addStoredDoc("d3", "{}");
    const seg_bytes = try seg_writer.build();
    defer alloc.free(seg_bytes);

    var writer = try index_mod.IndexWriter.init(alloc);
    defer writer.deinit();
    try writer.addSegment(seg_bytes);

    const snap = writer.snapshot();
    var result = try execute(alloc, snap, .{
        .query = .{ .term = .{ .field = "title", .term = "x" } },
        .k = 10,
        .aggregations = &.{
            .{ .name = "price_ranges", .field = "price", .agg_type = .{ .range = .{
                .ranges = &.{
                    .{ .from = null, .to = 100 },
                    .{ .from = 100, .to = null },
                },
            } } },
        },
    });
    defer result.deinit();

    try std.testing.expectEqual(@as(usize, 1), result.aggregations.len);
    const range_result = result.aggregations[0].result.range;
    try std.testing.expectEqual(@as(usize, 2), range_result.len);
    try std.testing.expectEqual(@as(u64, 2), range_result[0].count); // < 100: 15, 50
    try std.testing.expectEqual(@as(u64, 1), range_result[1].count); // >= 100: 150
}

test "search with geo distance aggregation" {
    const alloc = std.testing.allocator;

    var inv_builder = inverted.InvertedIndexBuilder.init(alloc, .{});
    defer inv_builder.deinit();
    try inv_builder.addDocument(0, &.{.{ .term = "x", .freq = 1, .norm = 10 }});
    try inv_builder.addDocument(1, &.{.{ .term = "x", .freq = 1, .norm = 10 }});
    const inv_data = try inv_builder.build();
    defer alloc.free(inv_data);

    // Two geo points: one ~1km from SF center, one ~100km
    var dv_writer = typed_dv.TypedDocValuesWriter.init(alloc, .geo_point, 1024);
    defer dv_writer.deinit();
    try dv_writer.add(0, .{ .geo_point = .{ .lat = 37.7839, .lon = -122.4194 } }); // ~1km
    try dv_writer.add(1, .{ .geo_point = .{ .lat = 38.5816, .lon = -121.4944 } }); // ~100km
    const dv_data = try dv_writer.build();
    defer alloc.free(dv_data);

    var seg_writer = segment_mod.SegmentWriter.init(alloc);
    defer seg_writer.deinit();
    const title_idx = try seg_writer.addField("title");
    try seg_writer.addSection(title_idx, .inverted_text, inv_data);
    const loc_idx = try seg_writer.addField("location");
    try seg_writer.addSection(loc_idx, .typed_doc_values, dv_data);
    try seg_writer.addStoredDoc("d1", "{}");
    try seg_writer.addStoredDoc("d2", "{}");
    const seg_bytes = try seg_writer.build();
    defer alloc.free(seg_bytes);

    var writer = try index_mod.IndexWriter.init(alloc);
    defer writer.deinit();
    try writer.addSegment(seg_bytes);

    const snap = writer.snapshot();
    var result = try execute(alloc, snap, .{
        .query = .{ .term = .{ .field = "title", .term = "x" } },
        .k = 10,
        .aggregations = &.{
            .{
                .name = "by_distance",
                .field = "location",
                .agg_type = .{
                    .geo_distance = .{
                        .center = .{ .lat = 37.7749, .lon = -122.4194 },
                        .ranges = &.{
                            .{ .from = null, .to = 5000 }, // < 5km
                            .{ .from = 5000, .to = null }, // >= 5km
                        },
                    },
                },
            },
        },
    });
    defer result.deinit();

    try std.testing.expectEqual(@as(usize, 1), result.aggregations.len);
    const gd = result.aggregations[0].result.geo_distance;
    try std.testing.expectEqual(@as(usize, 2), gd.len);
    try std.testing.expectEqual(@as(u64, 1), gd[0].count); // < 5km
    try std.testing.expectEqual(@as(u64, 1), gd[1].count); // >= 5km
}

test "search with geohash grid aggregation" {
    const alloc = std.testing.allocator;

    var inv_builder = inverted.InvertedIndexBuilder.init(alloc, .{});
    defer inv_builder.deinit();
    try inv_builder.addDocument(0, &.{.{ .term = "x", .freq = 1, .norm = 10 }});
    try inv_builder.addDocument(1, &.{.{ .term = "x", .freq = 1, .norm = 10 }});
    try inv_builder.addDocument(2, &.{.{ .term = "x", .freq = 1, .norm = 10 }});
    const inv_data = try inv_builder.build();
    defer alloc.free(inv_data);

    // Two points in same cell, one far away
    var dv_writer = typed_dv.TypedDocValuesWriter.init(alloc, .geo_point, 1024);
    defer dv_writer.deinit();
    try dv_writer.add(0, .{ .geo_point = .{ .lat = 37.7749, .lon = -122.4194 } });
    try dv_writer.add(1, .{ .geo_point = .{ .lat = 37.7750, .lon = -122.4195 } });
    try dv_writer.add(2, .{ .geo_point = .{ .lat = 40.7128, .lon = -74.0060 } });
    const dv_data = try dv_writer.build();
    defer alloc.free(dv_data);

    var seg_writer = segment_mod.SegmentWriter.init(alloc);
    defer seg_writer.deinit();
    const title_idx = try seg_writer.addField("title");
    try seg_writer.addSection(title_idx, .inverted_text, inv_data);
    const loc_idx = try seg_writer.addField("location");
    try seg_writer.addSection(loc_idx, .typed_doc_values, dv_data);
    try seg_writer.addStoredDoc("d1", "{}");
    try seg_writer.addStoredDoc("d2", "{}");
    try seg_writer.addStoredDoc("d3", "{}");
    const seg_bytes = try seg_writer.build();
    defer alloc.free(seg_bytes);

    var writer = try index_mod.IndexWriter.init(alloc);
    defer writer.deinit();
    try writer.addSegment(seg_bytes);

    const snap = writer.snapshot();
    var result = try execute(alloc, snap, .{
        .query = .{ .term = .{ .field = "title", .term = "x" } },
        .k = 10,
        .aggregations = &.{
            .{ .name = "geo_grid", .field = "location", .agg_type = .{ .geohash_grid = .{ .precision = 5 } } },
        },
    });
    defer result.deinit();

    try std.testing.expectEqual(@as(usize, 1), result.aggregations.len);
    const grid = result.aggregations[0].result.geohash_grid;
    try std.testing.expectEqual(@as(usize, 2), grid.len);
    // Top cell has 2 docs (SF), second has 1 (NYC)
    try std.testing.expectEqual(@as(u64, 2), grid[0].count);
    try std.testing.expectEqual(@as(u64, 1), grid[1].count);
}

test "search with date histogram + stats sub-aggregation" {
    const alloc = std.testing.allocator;

    var inv_builder = inverted.InvertedIndexBuilder.init(alloc, .{});
    defer inv_builder.deinit();
    try inv_builder.addDocument(0, &.{.{ .term = "x", .freq = 1, .norm = 10 }});
    try inv_builder.addDocument(1, &.{.{ .term = "x", .freq = 1, .norm = 10 }});
    try inv_builder.addDocument(2, &.{.{ .term = "x", .freq = 1, .norm = 10 }});
    const inv_data = try inv_builder.build();
    defer alloc.free(inv_data);

    // Timestamps: two in hour 0, one in hour 1
    const ns_per_s: u64 = 1_000_000_000;
    const base_day: u64 = 19737 * 86400;
    var ts_writer = typed_dv.TypedDocValuesWriter.init(alloc, .u64_val, 1024);
    defer ts_writer.deinit();
    try ts_writer.add(0, .{ .u64_val = (base_day + 1800) * ns_per_s });
    try ts_writer.add(1, .{ .u64_val = (base_day + 1200) * ns_per_s });
    try ts_writer.add(2, .{ .u64_val = (base_day + 5400) * ns_per_s });
    const ts_data = try ts_writer.build();
    defer alloc.free(ts_data);

    // Prices: 10, 20, 30
    var price_writer = typed_dv.TypedDocValuesWriter.init(alloc, .f64_val, 1024);
    defer price_writer.deinit();
    try price_writer.add(0, .{ .f64_val = 10.0 });
    try price_writer.add(1, .{ .f64_val = 20.0 });
    try price_writer.add(2, .{ .f64_val = 30.0 });
    const price_data = try price_writer.build();
    defer alloc.free(price_data);

    var seg_writer = segment_mod.SegmentWriter.init(alloc);
    defer seg_writer.deinit();
    const title_idx = try seg_writer.addField("title");
    try seg_writer.addSection(title_idx, .inverted_text, inv_data);
    const ts_idx = try seg_writer.addField("timestamp");
    try seg_writer.addSection(ts_idx, .typed_doc_values, ts_data);
    const price_idx = try seg_writer.addField("price");
    try seg_writer.addSection(price_idx, .typed_doc_values, price_data);
    try seg_writer.addStoredDoc("d1", "{}");
    try seg_writer.addStoredDoc("d2", "{}");
    try seg_writer.addStoredDoc("d3", "{}");
    const seg_bytes = try seg_writer.build();
    defer alloc.free(seg_bytes);

    var writer = try index_mod.IndexWriter.init(alloc);
    defer writer.deinit();
    try writer.addSegment(seg_bytes);

    const snap = writer.snapshot();
    var result = try execute(alloc, snap, .{
        .query = .{ .term = .{ .field = "title", .term = "x" } },
        .k = 10,
        .aggregations = &.{
            .{
                .name = "by_hour",
                .field = "timestamp",
                .agg_type = .{ .date_histogram = .{ .interval = .hour } },
                .sub_aggs = &.{
                    .{ .name = "price_stats", .field = "price", .agg_type = .{ .stats = {} } },
                },
            },
        },
    });
    defer result.deinit();

    // Should have sub-results for each bucket
    try std.testing.expectEqual(@as(usize, 1), result.aggregations.len);
    const subs = result.aggregations[0].sub_results orelse return error.TestExpectedEqual;
    try std.testing.expectEqual(@as(usize, 2), subs.len); // 2 hourly buckets

    // Bucket 0 (hour 0): docs 0,1 → prices 10, 20
    const hour0_stats = subs[0].aggs[0].result.stats;
    try std.testing.expectEqual(@as(u64, 2), hour0_stats.count);
    try std.testing.expectApproxEqAbs(@as(f64, 10.0), hour0_stats.min, 0.001);
    try std.testing.expectApproxEqAbs(@as(f64, 20.0), hour0_stats.max, 0.001);

    // Bucket 1 (hour 1): doc 2 → price 30
    const hour1_stats = subs[1].aggs[0].result.stats;
    try std.testing.expectEqual(@as(u64, 1), hour1_stats.count);
    try std.testing.expectApproxEqAbs(@as(f64, 30.0), hour1_stats.min, 0.001);
}

test "search with range + stats sub-aggregation" {
    const alloc = std.testing.allocator;

    var inv_builder = inverted.InvertedIndexBuilder.init(alloc, .{});
    defer inv_builder.deinit();
    try inv_builder.addDocument(0, &.{.{ .term = "x", .freq = 1, .norm = 10 }});
    try inv_builder.addDocument(1, &.{.{ .term = "x", .freq = 1, .norm = 10 }});
    try inv_builder.addDocument(2, &.{.{ .term = "x", .freq = 1, .norm = 10 }});
    const inv_data = try inv_builder.build();
    defer alloc.free(inv_data);

    // Prices: 15, 50, 150
    var price_writer = typed_dv.TypedDocValuesWriter.init(alloc, .f64_val, 1024);
    defer price_writer.deinit();
    try price_writer.add(0, .{ .f64_val = 15.0 });
    try price_writer.add(1, .{ .f64_val = 50.0 });
    try price_writer.add(2, .{ .f64_val = 150.0 });
    const price_data = try price_writer.build();
    defer alloc.free(price_data);

    var seg_writer = segment_mod.SegmentWriter.init(alloc);
    defer seg_writer.deinit();
    const title_idx = try seg_writer.addField("title");
    try seg_writer.addSection(title_idx, .inverted_text, inv_data);
    const price_idx = try seg_writer.addField("price");
    try seg_writer.addSection(price_idx, .typed_doc_values, price_data);
    try seg_writer.addStoredDoc("d1", "{}");
    try seg_writer.addStoredDoc("d2", "{}");
    try seg_writer.addStoredDoc("d3", "{}");
    const seg_bytes = try seg_writer.build();
    defer alloc.free(seg_bytes);

    var writer = try index_mod.IndexWriter.init(alloc);
    defer writer.deinit();
    try writer.addSegment(seg_bytes);

    const snap = writer.snapshot();
    var result = try execute(alloc, snap, .{
        .query = .{ .term = .{ .field = "title", .term = "x" } },
        .k = 10,
        .aggregations = &.{
            .{
                .name = "price_ranges",
                .field = "price",
                .agg_type = .{ .range = .{
                    .ranges = &.{
                        .{ .from = null, .to = 100 },
                        .{ .from = 100, .to = null },
                    },
                } },
                .sub_aggs = &.{
                    .{ .name = "price_stats", .field = "price", .agg_type = .{ .stats = {} } },
                },
            },
        },
    });
    defer result.deinit();

    const subs = result.aggregations[0].sub_results orelse return error.TestExpectedEqual;
    try std.testing.expectEqual(@as(usize, 2), subs.len);

    // Range 0 (< 100): docs 0,1 → prices 15, 50
    const r0_stats = subs[0].aggs[0].result.stats;
    try std.testing.expectEqual(@as(u64, 2), r0_stats.count);
    try std.testing.expectApproxEqAbs(@as(f64, 15.0), r0_stats.min, 0.001);
    try std.testing.expectApproxEqAbs(@as(f64, 50.0), r0_stats.max, 0.001);

    // Range 1 (>= 100): doc 2 → price 150
    const r1_stats = subs[1].aggs[0].result.stats;
    try std.testing.expectEqual(@as(u64, 1), r1_stats.count);
    try std.testing.expectApproxEqAbs(@as(f64, 150.0), r1_stats.min, 0.001);
}

test "search result has empty graph_results by default" {
    const alloc = std.testing.allocator;

    const seg_bytes = try buildTestSegmentWithStoredDocs(alloc, &.{
        .{ .id = "doc1", .data = "{}", .terms = &.{
            .{ .term = "hello", .freq = 1, .norm = 10 },
        } },
    });
    defer alloc.free(seg_bytes);

    var writer = try index_mod.IndexWriter.init(alloc);
    defer writer.deinit();
    try writer.addSegment(seg_bytes);

    const snap = writer.snapshot();
    var result = try execute(alloc, snap, .{
        .query = .{ .term = .{ .field = "title", .term = "hello" } },
        .k = 10,
    });
    defer result.deinit();

    try std.testing.expectEqual(@as(usize, 0), result.graph_results.len);
    try std.testing.expectEqual(@as(u32, 1), result.total_hits);
}

test "search stored hit cursor preserves order ownership and allocation failure cleanup" {
    const a = std.testing.allocator;
    const bytes = try buildTestSegmentWithStoredDocs(a, &.{
        .{ .id = "a", .data = "first", .terms = &.{.{ .term = "hello", .freq = 1, .norm = 10 }} },
        .{ .id = "b", .data = "second", .terms = &.{.{ .term = "hello", .freq = 1, .norm = 10 }} },
        .{ .id = "c", .data = "third", .terms = &.{.{ .term = "hello", .freq = 1, .norm = 10 }} },
    });
    defer a.free(bytes);
    var writer = try index_mod.IndexWriter.init(a);
    defer writer.deinit();
    try writer.addSegment(bytes);
    const snapshot = writer.snapshot();
    const Scenario = struct {
        fn run(allocator: Allocator, snap: *const index_mod.IndexSnapshot) !void {
            var hits = [_]ScoredHit{
                .{ .doc_id = 2, .score = 3, .id = null, .stored_data = null },
                .{ .doc_id = 0, .score = 2, .id = null, .stored_data = null },
                .{ .doc_id = 1, .score = 1, .id = null, .stored_data = null },
            };
            defer freeStoredBodies(allocator, &hits);
            try populateStoredHits(allocator, snap, &hits, null);
            try std.testing.expectEqualStrings("c", hits[0].id.?);
            try std.testing.expectEqualStrings("third", hits[0].stored_data.?);
            try std.testing.expectEqualStrings("a", hits[1].id.?);
            try std.testing.expectEqualStrings("first", hits[1].stored_data.?);
            try std.testing.expectEqualStrings("b", hits[2].id.?);
            try std.testing.expectEqualStrings("second", hits[2].stored_data.?);
            try std.testing.expectEqual(@as(f32, 3), hits[0].score);
        }
    };
    try std.testing.checkAllAllocationFailures(a, Scenario.run, .{snapshot});
    var native = try index_mod.IndexWriter.init(a);
    defer native.deinit();
    try native.addSegmentWithIdData(1, .fromNative(.{ .contiguous = bytes }));
    try Scenario.run(a, native.snapshot());
    try std.testing.checkAllAllocationFailures(a, Scenario.run, .{native.snapshot()});
    var native_single = [_]ScoredHit{.{ .doc_id = 1, .score = 1, .id = null, .stored_data = null }};
    defer freeStoredBodies(a, &native_single);
    try populateStoredHits(a, native.snapshot(), &native_single, null);
    try std.testing.expectEqualStrings("b", native_single[0].id.?);
    try std.testing.expectEqualStrings("second", native_single[0].stored_data.?);
    try std.testing.expectEqual(@as(usize, 0), native.snapshot().segments[0].reader.native.?.identity_bytes);
    var singleton = [_]ScoredHit{.{ .doc_id = 0, .score = 1, .id = null, .stored_data = null }};
    defer freeStoredBodies(a, &singleton);
    try populateStoredHits(a, snapshot, &singleton, null);
    try std.testing.expectEqualStrings("first", singleton[0].stored_data.?);
}

test "search hybrid materializes only fused winners and cleans allocation failures" {
    const a = std.testing.allocator;
    const bytes = try buildTestSegmentWithStoredDocs(a, &.{
        .{ .id = "a", .data = "first", .terms = &.{.{ .term = "hello", .freq = 1, .norm = 10 }} },
        .{ .id = "b", .data = "second", .terms = &.{.{ .term = "hello", .freq = 1, .norm = 10 }} },
    });
    defer a.free(bytes);
    var writer = try index_mod.IndexWriter.init(a);
    defer writer.deinit();
    try writer.addSegment(bytes);
    const Scenario = struct {
        fn run(allocator: Allocator, snap: *const index_mod.IndexSnapshot) !void {
            var diagnostics = scorer_mod.SearchDiagnostics{};
            var result = try executeHybrid(allocator, snap, .{
                .text_query = .{ .term = .{ .field = "title", .term = "hello" } },
                .knn = .{ .index_name = "missing", .vector = &.{ 1, 2 }, .k = 2 },
            }, .{ .query = .{ .match_none = {} }, .k = 2, .diagnostics = &diagnostics });
            defer result.deinit();
            try std.testing.expectEqual(@as(usize, 2), result.hits.len);
            try std.testing.expectEqualStrings("first", result.hits[0].stored_data.?);
            try std.testing.expectEqualStrings("second", result.hits[1].stored_data.?);
            try std.testing.expectEqual(@as(u64, 2), diagnostics.stored_body_copies);
            try std.testing.expectEqual(@as(u64, 1), diagnostics.stored_block_decodes);
            try std.testing.expectEqual(@as(u64, 11), diagnostics.stored_body_bytes);
        }
    };
    try std.testing.checkAllAllocationFailures(a, Scenario.run, .{writer.snapshot()});
}

test "native stats aggregate dense and selective candidates with scoped ownership" {
    const a = std.testing.allocator;
    var values = typed_dv.TypedDocValuesWriter.init(a, .u64_val, 128);
    defer values.deinit();
    var segment = segment_mod.SegmentWriter.init(a);
    defer segment.deinit();
    for (0..1000) |doc| {
        var id: [32]u8 = undefined;
        try segment.addStoredDoc(try std.fmt.bufPrint(&id, "doc-{d}", .{doc}), "");
        if (doc % 2 == 0) try values.add(@intCast(doc), .{ .u64_val = doc + 1 });
    }
    const column = try values.build();
    defer a.free(column);
    try segment.addSection(try segment.addField("rank"), .typed_doc_values, column);
    const bytes = try segment.build();
    defer a.free(bytes);
    var writer = try index_mod.IndexWriter.init(a);
    defer writer.deinit();
    try writer.addSegmentWithIdData(1, .fromNative(.{ .contiguous = bytes }));
    const Harness = struct {
        fn run(alloc: Allocator, snapshot: *const index_mod.IndexSnapshot) !void {
            var reads = segment_mod.TypedReadScope.init(alloc);
            defer reads.deinit();
            const candidates = try alloc.alloc(scorer_mod.ScoredHit, 1000);
            defer alloc.free(candidates);
            for (candidates, 0..) |*candidate, doc| candidate.* = .{ .doc_id = @intCast(doc), .score = 0 };
            var dense = aggregation_mod.StatsAgg.init();
            try collectStatsBatched(alloc, &reads, snapshot, candidates, "rank", &dense);
            try std.testing.expectEqual(@as(u64, 500), dense.count);
            try std.testing.expectEqual(@as(f64, 250_000), dense.sum);
            try std.testing.expectEqual(@as(usize, 0), reads.cache.?.decode_count);
            var grouped = StatsCandidates.init(alloc, snapshot, candidates);
            defer grouped.deinit();
            var first = aggregation_mod.StatsAgg.init();
            try collectStatsGrouped(&reads, snapshot, &grouped, "rank", &first);
            const directory = (try grouped.prepare()).ptr;
            var repeated = aggregation_mod.StatsAgg.init();
            try collectStatsGrouped(&reads, snapshot, &grouped, "rank", &repeated);
            try std.testing.expectEqual(first.count, repeated.count);
            try std.testing.expectEqual(first.sum, repeated.sum);
            try std.testing.expectEqual(directory, (try grouped.prepare()).ptr);

            const selective = [_]scorer_mod.ScoredHit{ .{ .doc_id = 0, .score = 0 }, .{ .doc_id = 0, .score = 0 }, .{ .doc_id = 100, .score = 0 } };
            var sparse = aggregation_mod.StatsAgg.init();
            try collectStatsBatched(alloc, &reads, snapshot, &selective, "rank", &sparse);
            try std.testing.expectEqual(@as(u64, 2), sparse.count);
            try std.testing.expectEqual(@as(f64, 102), sparse.sum);
            try std.testing.expect(reads.cache.?.decode_count <= 4);
            try std.testing.expectEqual(@as(usize, 1), reads.entries.items.len);
        }
    };
    try Harness.run(a, writer.snapshot());
    try std.testing.checkAllAllocationFailures(a, Harness.run, .{writer.snapshot()});
    try writer.addSegmentWithIdData(2, .fromNative(.{ .contiguous = bytes }));
    var reads = segment_mod.TypedReadScope.init(a);
    defer reads.deinit();
    const mixed = [_]scorer_mod.ScoredHit{
        .{ .doc_id = 1500, .score = 0 },                 .{ .doc_id = 0, .score = 0 },
        .{ .doc_id = 1000, .score = 0 },                 .{ .doc_id = 0, .score = 0 },
        .{ .doc_id = std.math.maxInt(u32), .score = 0 }, .{ .doc_id = 100, .score = 0 },
        .{ .doc_id = 1800, .score = 0 },
    };
    var grouped = StatsCandidates.init(a, writer.snapshot(), &mixed);
    defer grouped.deinit();
    var stats = aggregation_mod.StatsAgg.init();
    try collectStatsGrouped(&reads, writer.snapshot(), &grouped, "rank", &stats);
    try std.testing.expectEqual(@as(u64, 5), stats.count);
    try std.testing.expectEqual(@as(f64, 1405), stats.sum);
    try std.testing.expectEqualSlices(usize, &.{ 0, 3, 6 }, grouped.offsets.?);
}

test "native search identities and bodies outlive the source owner" {
    const a = std.testing.allocator;
    const bytes = try buildTestSegmentWithStoredDocs(a, &.{
        .{ .id = "aaa", .data = "first", .terms = &.{.{ .term = "hello", .freq = 1, .norm = 10 }} },
        .{ .id = "bbb", .data = "second", .terms = &.{.{ .term = "hello", .freq = 1, .norm = 10 }} },
    });
    var writer = try index_mod.IndexWriter.init(a);
    var owner_open = true;
    defer if (owner_open) writer.deinit();
    defer a.free(bytes);
    try writer.addSegmentWithIdData(1, .fromNative(.{ .contiguous = bytes }));
    var result = try execute(a, writer.snapshot(), .{ .query = .match_all, .k = 2, .include_stored = true });
    defer result.deinit();
    writer.deinit();
    owner_open = false;
    try std.testing.expectEqualStrings("aaa", result.hits[0].id.?);
    try std.testing.expectEqualStrings("bbb", result.hits[1].id.?);
    try std.testing.expectEqualStrings("first", result.hits[0].stored_data.?);
    try std.testing.expectEqualStrings("second", result.hits[1].stored_data.?);
}

test "native ID filters and deletes never retain stable identity pages" {
    const a = std.testing.allocator;
    var builder = segment_mod.SegmentWriter.init(a);
    defer builder.deinit();
    for (0..2048) |i| {
        var id: [1024]u8 = @splat('x');
        std.mem.writeInt(u64, id[0..8], i, .little);
        try builder.addStoredDoc(&id, "");
    }
    const bytes = try builder.build();
    defer a.free(bytes);
    var writer = try index_mod.IndexWriter.init(a);
    defer writer.deinit();
    try writer.addSegmentWithIdData(1, .fromNative(.{ .contiguous = bytes }));
    const entry = &writer.snapshot().segments[0];
    const before = entry.reader.nativeNavigationBytes();
    var matches = try (query_mod.DocIdFilter{ .doc_ids = &.{"absent"} }).execute(a, entry);
    defer matches.deinit();
    const after = entry.reader.nativeNavigationBytes();
    try std.testing.expectEqual(@as(usize, 0), matches.cardinality());
    try std.testing.expectEqual(@as(usize, 0), entry.reader.native.?.identity_bytes);
    var id: [1024]u8 = @splat('x');
    std.mem.writeInt(u64, id[0..8], 1023, .little);
    const deletes = try writer.deleteAllByIdsTracked(a, &.{ &id, "absent" });
    defer index_mod.IndexWriter.freeDeleteInfos(a, deletes);
    try std.testing.expectEqual(@as(usize, 1), deletes.len);
    try std.testing.expectEqual(@as(usize, 1), deletes[0].local_ids.len);
    try std.testing.expectEqual(@as(usize, 0), entry.reader.native.?.identity_bytes);
    std.debug.print("LITE_ID_SCAN rows=2048 retained_growth={d} retained_ids=0\n", .{after - before});
}

test "external lake impossible Boolean conjunction skips global scoring reads" {
    const a = std.testing.allocator;
    const bytes = try buildTestSegmentWithStoredDocs(a, &.{
        .{ .id = "one", .data = "{}", .terms = &.{.{ .term = "common", .freq = 1, .norm = 1 }} },
        .{ .id = "two", .data = "{}", .terms = &.{.{ .term = "common", .freq = 1, .norm = 1 }} },
    });
    defer a.free(bytes);
    var writer = try index_mod.IndexWriter.init(a);
    defer writer.deinit();
    try writer.addSegment(bytes);
    try writer.addSegment(bytes);
    const snap = writer.snapshot();
    var reader = (try snap.segments[0].reader.invertedIndexScoped(a, "title")).?;
    defer reader.deinit();
    const terms = [_]SimpleTextTerm{
        .{ .field = "title", .term = "absent", .boost = 1 },
        .{ .field = "title", .term = "common", .boost = 1 },
    };
    try std.testing.expect((try initFastTermStates(a, snap, &reader, "title", &terms, true, null)) == null);
    try std.testing.expectEqual(@as(u64, 0), snap.term_doc_freq_cache_misses);
    const states = (try initFastTermStates(a, snap, &reader, "title", terms[1..], true, null)).?;
    defer deinitFastTermStates(a, states);
    try std.testing.expectEqual(@as(u32, 4), states[0].doc_freq);
}

test "external lake indexed bitmap filters preserve ranking disjunction and exact counts" {
    const a = std.testing.allocator;
    const bytes = try buildTestSegmentWithStoredDocs(a, &.{
        .{ .id = "zero", .data = "{}", .terms = &.{.{ .term = "alpha", .freq = 4, .norm = 10 }} },
        .{ .id = "one", .data = "{}", .terms = &.{.{ .term = "beta", .freq = 2, .norm = 10 }} },
        .{ .id = "two", .data = "{}", .terms = &.{.{ .term = "alpha", .freq = 1, .norm = 10 }} },
    });
    defer a.free(bytes);
    var writer = try index_mod.IndexWriter.init(a);
    defer writer.deinit();
    try writer.addSegment(bytes);
    var bitmap = roaring.RoaringBitmap.init(a);
    defer bitmap.deinit();
    try bitmap.add(1);
    try bitmap.add(2);
    const base: SearchQuery = .{ .match = .{ .field = "title", .text = "alpha beta" } };
    const query: SearchQuery = .{ .bool_query = .{ .must = &.{ base, .{ .doc_num = .{ .ids = &.{}, .bitmap = &bitmap, .boost = 0 } } } } };
    var original = try execute(a, writer.snapshot(), .{ .query = base, .k = 3, .include_stored = false });
    defer original.deinit();
    var filtered = try execute(a, writer.snapshot(), .{ .query = query, .k = 3, .include_stored = false });
    defer filtered.deinit();
    try std.testing.expectEqual(@as(usize, 2), filtered.hits.len);
    for (filtered.hits) |hit| {
        try std.testing.expect(bitmap.contains(hit.doc_id));
        const score = for (original.hits) |before| {
            if (before.doc_id == hit.doc_id) break before.score;
        } else return error.TestUnexpectedResult;
        try std.testing.expectEqual(score, hit.score);
    }
    var count = try executeCountCandidates(a, writer.snapshot(), query);
    defer count.deinit();
    try std.testing.expectEqual(@as(u32, 2), count.total_hits);
    try std.testing.expectEqual(@as(u32, 2), try countMatches(a, writer.snapshot(), query));
    const excluded: SearchQuery = .{ .bool_query = .{ .must = &.{base}, .must_not = &.{.{ .doc_num = .{ .ids = &.{}, .bitmap = &bitmap } }} } };
    var remaining = try execute(a, writer.snapshot(), .{ .query = excluded, .k = 3, .include_stored = false });
    defer remaining.deinit();
    try std.testing.expectEqual(@as(usize, 1), remaining.hits.len);
    try std.testing.expectEqual(@as(u32, 0), remaining.hits[0].doc_id);
}

test "search signed date histogram retains negative wide and nested buckets" {
    const alloc = std.testing.allocator;

    // Build inverted index (all docs match "x")
    var inv_builder = inverted.InvertedIndexBuilder.init(alloc, .{});
    defer inv_builder.deinit();
    try inv_builder.addDocument(0, &.{.{ .term = "x", .freq = 1, .norm = 10 }});
    try inv_builder.addDocument(1, &.{.{ .term = "x", .freq = 1, .norm = 10 }});
    try inv_builder.addDocument(2, &.{.{ .term = "x", .freq = 1, .norm = 10 }});
    const inv_data = try inv_builder.build();
    defer alloc.free(inv_data);

    var dv_writer = typed_dv.TypedDocValuesWriter.init(alloc, .datetime_ns, 1024);
    defer dv_writer.deinit();
    try dv_writer.add(0, .{ .datetime_ns = -1 });
    try dv_writer.add(1, .{ .datetime_ns = 0 });
    try dv_writer.add(2, .{ .datetime_ns = 253402300799999999999 });
    const dv_data = try dv_writer.build();
    defer alloc.free(dv_data);

    var seg_writer = segment_mod.SegmentWriter.init(alloc);
    defer seg_writer.deinit();
    const title_idx = try seg_writer.addField("title");
    try seg_writer.addSection(title_idx, .inverted_text, inv_data);
    const ts_idx = try seg_writer.addField("timestamp");
    try seg_writer.addSection(ts_idx, .typed_doc_values, dv_data);
    try seg_writer.addStoredDoc("d1", "{}");
    try seg_writer.addStoredDoc("d2", "{}");
    try seg_writer.addStoredDoc("d3", "{}");
    const seg_bytes = try seg_writer.build();
    defer alloc.free(seg_bytes);

    var writer = try index_mod.IndexWriter.init(alloc);
    defer writer.deinit();
    try writer.addSegment(seg_bytes);

    const snap = writer.snapshot();
    var result = try execute(alloc, snap, .{
        .query = .{ .term = .{ .field = "title", .term = "x" } },
        .k = 10,
        .aggregations = &.{
            .{ .name = "by_hour", .field = "timestamp", .agg_type = .{ .date_histogram = .{ .interval = .hour } }, .sub_aggs = &.{.{ .name = "nested", .field = "timestamp", .agg_type = .{ .date_histogram = .{ .interval = .day } } }} },
        },
    });
    defer result.deinit();

    try std.testing.expectEqual(@as(usize, 1), result.aggregations.len);
    const dh = result.aggregations[0].result.date_histogram;
    try std.testing.expectEqual(@as(usize, 3), dh.keys.len);
    try std.testing.expectEqual(@as(i128, -std.time.ns_per_hour), dh.keys[0]);
    try std.testing.expectEqual(@as(i128, 0), dh.keys[1]);
    try std.testing.expect(dh.keys[2] > std.math.maxInt(u64));
    for (dh.counts) |count| try std.testing.expectEqual(@as(u64, 1), count);
    const nested = result.aggregations[0].sub_results.?;
    try std.testing.expectEqual(@as(usize, 3), nested.len);
    try std.testing.expectEqual(dh.keys[0], nested[0].bucket_key.timestamp);
    try std.testing.expectEqual(@as(i128, -std.time.ns_per_day), nested[0].aggs[0].result.date_histogram.keys[0]);
}

test "external lake deferred membership refines text candidates with exact scores counts and exclusions" {
    const a = std.testing.allocator;
    const bytes = try buildTestSegmentWithStoredDocs(a, &.{
        .{ .id = "zero", .data = "{}", .terms = &.{.{ .term = "rare", .freq = 4, .norm = 10 }} },
        .{ .id = "one", .data = "{}", .terms = &.{.{ .term = "common", .freq = 2, .norm = 10 }} },
        .{ .id = "two", .data = "{}", .terms = &.{.{ .term = "rare", .freq = 1, .norm = 10 }} },
    });
    defer a.free(bytes);
    var writer = try index_mod.IndexWriter.init(a);
    defer writer.deinit();
    try writer.addSegment(bytes);
    try writer.addSegment(bytes);
    const Producer = struct {
        calls: usize = 0,
        probed: usize = 0,
        fail: bool = false,
        fn produce(raw: *anyopaque, alloc: Allocator, offset: u32, count: u32, candidates: ?*const roaring.RoaringBitmap) !roaring.RoaringBitmap {
            const self: *@This() = @ptrCast(@alignCast(raw));
            if (self.fail) return error.InjectedMembershipFailure;
            self.calls += 1;
            var result = roaring.RoaringBitmap.init(alloc);
            errdefer result.deinit();
            const selected = candidates orelse return error.ExpectedTextCandidates;
            self.probed += selected.cardinality();
            var iterator = selected.iterator();
            while (iterator.next()) |doc| {
                if (doc >= count) return error.InvalidArgument;
                if ((offset + doc) % 2 == 0) try result.add(doc);
            }
            return result;
        }
    };
    var producer: Producer = .{};
    const deferred: SearchQuery = .{ .doc_num = .{ .ids = &.{}, .producer = .{ .ptr = &producer, .produce = Producer.produce }, .boost = 0 } };
    const base: SearchQuery = .{ .match = .{ .field = "title", .text = "rare" } };
    const included: SearchQuery = .{ .bool_query = .{ .must = &.{ base, deferred } } };
    const excluded: SearchQuery = .{ .bool_query = .{ .must = &.{base}, .must_not = &.{deferred} } };
    var original = try execute(a, writer.snapshot(), .{ .query = base, .k = 6, .include_stored = false });
    defer original.deinit();
    var bitmap = roaring.RoaringBitmap.init(a);
    defer bitmap.deinit();
    for ([_]u32{ 0, 2, 4 }) |doc| try bitmap.add(doc);
    const bitmap_clause: SearchQuery = .{ .doc_num = .{ .ids = &.{}, .bitmap = &bitmap, .boost = 0 } };
    for ([_]SearchQuery{ included, excluded }) |query| {
        const reference_query: SearchQuery = if (query.bool_query.must.len == 2)
            .{ .bool_query = .{ .must = &.{ base, bitmap_clause } } }
        else
            .{ .bool_query = .{ .must = &.{base}, .must_not = &.{bitmap_clause} } };
        var reference = try execute(a, writer.snapshot(), .{ .query = reference_query, .k = 6, .include_stored = false });
        defer reference.deinit();
        var result = try execute(a, writer.snapshot(), .{ .query = query, .k = 6, .include_stored = false });
        defer result.deinit();
        try std.testing.expectEqual(@as(usize, 2), result.hits.len);
        try std.testing.expectEqual(@as(u32, 2), try countMatches(a, writer.snapshot(), query));
        var count = try executeCountCandidates(a, writer.snapshot(), query);
        defer count.deinit();
        try std.testing.expectEqual(@as(u32, 2), count.total_hits);
        for (result.hits) |hit| {
            try std.testing.expectEqual(query.bool_query.must.len == 2, hit.doc_id % 2 == 0);
            const expected = for (original.hits) |before| {
                if (before.doc_id == hit.doc_id) break before.score;
            } else return error.TestUnexpectedResult;
            // Existing filtered/unfiltered scorer kernels can differ by an
            // f32 ULP; deferred membership must exactly match bitmap scoring.
            try std.testing.expectApproxEqAbs(expected, hit.score, 0.000001);
            const bitmap_score = for (reference.hits) |before| {
                if (before.doc_id == hit.doc_id) break before.score;
            } else return error.TestUnexpectedResult;
            try std.testing.expectEqual(bitmap_score, hit.score);
        }
    }
    try std.testing.expectEqual(@as(usize, 12), producer.calls);
    try std.testing.expectEqual(@as(usize, 24), producer.probed);
    producer.fail = true;
    try std.testing.expectError(error.InjectedMembershipFailure, execute(a, writer.snapshot(), .{ .query = included, .k = 2 }));
}

test "external lake producer top k prunes common text without a complete membership pass" {
    const a = std.testing.allocator;
    var builder = inverted.InvertedIndexBuilder.init(a, .{ .chunk_size = 128 });
    defer builder.deinit();
    for (0..8192) |i| try builder.addDocument(@intCast(i), &.{
        .{ .term = "common", .freq = if (i < 128) 100 else 1, .norm = 100 },
        .{ .term = "other", .freq = if (i < 128) 30 else 1, .norm = 100 },
    });
    const section = try builder.build();
    defer a.free(section);
    var segment = segment_mod.SegmentWriter.init(a);
    defer segment.deinit();
    for (0..8192) |i| {
        var id: [32]u8 = undefined;
        try segment.addStoredDoc(try std.fmt.bufPrint(&id, "doc-{d}", .{i}), "{}");
    }
    const field = try segment.addField("title");
    try segment.addSection(field, .inverted_text, section);
    const bytes = try segment.build();
    defer a.free(bytes);
    var writer = try index_mod.IndexWriter.init(a);
    defer writer.deinit();
    try writer.addSegment(bytes);
    try writer.addSegment(bytes);
    const Producer = struct {
        probed: usize = 0,
        max_batch: usize = 0,
        fn produce(raw: *anyopaque, alloc: Allocator, offset: u32, count: u32, candidates: ?*const roaring.RoaringBitmap) !roaring.RoaringBitmap {
            const self: *@This() = @ptrCast(@alignCast(raw));
            const selected = candidates orelse return error.ExpectedBoundedCandidates;
            self.probed += selected.cardinality();
            self.max_batch = @max(self.max_batch, selected.cardinality());
            var result = roaring.RoaringBitmap.init(alloc);
            errdefer result.deinit();
            var it = selected.iterator();
            while (it.next()) |doc| {
                if (doc >= count) return error.InvalidArgument;
                if ((offset + doc) % 2 == 0) try result.add(doc);
            }
            return result;
        }
    };
    var producer: Producer = .{};
    const deferred: SearchQuery = .{ .doc_num = .{ .ids = &.{}, .producer = .{ .ptr = &producer, .produce = Producer.produce }, .boost = 0 } };
    const term: SearchQuery = .{ .term = .{ .field = "title", .term = "common" } };
    const other: SearchQuery = .{ .term = .{ .field = "title", .term = "other" } };
    var bitmap = roaring.RoaringBitmap.init(a);
    defer bitmap.deinit();
    for (0..8192) |i| try bitmap.add(@intCast(i * 2));
    const fixed: SearchQuery = .{ .doc_num = .{ .ids = &.{}, .bitmap = &bitmap, .boost = 0 } };
    for ([_]SearchQuery{ term, .{ .match = .{ .field = "title", .text = "common other" } }, .{ .bool_query = .{ .must = &.{ term, other } } } }) |base| {
        for ([_]bool{ false, true }) |exclude| {
            const query: SearchQuery = if (exclude) .{ .bool_query = .{ .must = &.{base}, .must_not = &.{deferred} } } else .{ .bool_query = .{ .must = &.{ base, deferred } } };
            const reference_query: SearchQuery = if (exclude) .{ .bool_query = .{ .must = &.{base}, .must_not = &.{fixed} } } else .{ .bool_query = .{ .must = &.{ base, fixed } } };
            var reference = try execute(a, writer.snapshot(), .{ .query = reference_query, .k = 16384, .include_stored = false });
            defer reference.deinit();
            producer = .{};
            var diagnostics: SearchDiagnostics = .{};
            var result = try execute(a, writer.snapshot(), .{ .query = query, .k = 3, .include_stored = false, .diagnostics = &diagnostics });
            defer result.deinit();
            try std.testing.expectEqual(@as(usize, 3), result.hits.len);
            for (result.hits, reference.hits[0..3]) |hit, expected| {
                try std.testing.expectEqual(expected.doc_id, hit.doc_id);
                try std.testing.expectEqual(expected.score, hit.score);
            }
            try std.testing.expect(producer.max_batch <= 64);
            try std.testing.expect(producer.probed < 8192);
            try std.testing.expect(diagnostics.wand_chunks_skipped + diagnostics.boolean_chunks_skipped > 0);
            try std.testing.expectEqual(TotalHitsRelation.gte, result.total_hits_relation);
            try std.testing.expectEqual(@as(u32, 8192), try countMatches(a, writer.snapshot(), query));
        }
    }
    // Once adaptive probing switches to complete membership, the same scorer
    // must seek to the sparse answer in a later segment instead of scanning onward.
    var point = roaring.RoaringBitmap.init(a);
    defer point.deinit();
    try point.add(15193);
    try point.prepareRead();
    const Materialized = struct {
        bitmap: *const roaring.RoaringBitmap,
        ready: bool = false,
        probed: usize = 0,
        fn complete(raw: *anyopaque) ?*const roaring.RoaringBitmap {
            const self: *@This() = @ptrCast(@alignCast(raw));
            return if (self.ready) self.bitmap else null;
        }
        fn produce(raw: *anyopaque, alloc: Allocator, offset: u32, count: u32, candidates: ?*const roaring.RoaringBitmap) !roaring.RoaringBitmap {
            const self: *@This() = @ptrCast(@alignCast(raw));
            self.probed += (candidates orelse return error.ExpectedBoundedCandidates).cardinality();
            self.ready = true;
            return self.bitmap.sliceRebased(alloc, offset, @as(u64, offset) + count);
        }
    };
    var materialized: Materialized = .{ .bitmap = &point };
    const adaptive: SearchQuery = .{ .doc_num = .{ .ids = &.{}, .boost = 0, .producer = .{ .ptr = &materialized, .produce = Materialized.produce, .materialized = Materialized.complete } } };
    var sparse_answer = try execute(a, writer.snapshot(), .{ .query = .{ .bool_query = .{ .must = &.{ term, adaptive } } }, .k = 3, .include_stored = false });
    defer sparse_answer.deinit();
    try std.testing.expectEqual(@as(usize, 1), sparse_answer.hits.len);
    try std.testing.expectEqual(@as(u32, 15193), sparse_answer.hits[0].doc_id);
    try std.testing.expectEqual(TotalHitsRelation.exact, sparse_answer.total_hits_relation);
    try std.testing.expectEqual(@as(usize, 64), materialized.probed);
}

test "external lake producer top k overlapping masks bound navigation to a posting window" {
    const a = std.testing.allocator;
    var bitmap = roaring.RoaringBitmap.init(a);
    defer bitmap.deinit();
    for (0..500000) |i| try bitmap.add(@intCast(i * 2));
    try bitmap.prepareRead();
    var collector: FastTopK = .{ .alloc = a, .k = 1, .filter_doc_bitmap = &bitmap, .exclude_doc_bitmap = &bitmap };
    defer collector.deinit();
    var gate = BitmapGate.init(&bitmap, 0, 1000000);
    try std.testing.expectEqual(@as(u64, 4096), collector.nextAllowed(&gate, 0));
    try std.testing.expectEqual(@as(u64, 0x1_0000_0000), collector.nextAllowed(&gate, 999999));
}

test "external lake producer top k shares segment bound planning across fragmented snapshots" {
    const a = std.testing.allocator;
    var arena = std.heap.ArenaAllocator.init(a);
    defer arena.deinit();
    const ca = arena.allocator();
    var writer = try index_mod.IndexWriter.init(a);
    defer writer.deinit();
    for (0..20) |i| {
        const id = try std.fmt.allocPrint(ca, "segment-{d}", .{i});
        const bytes = try buildTestSegmentWithStoredDocs(ca, &.{.{ .id = id, .data = "{}", .terms = &.{.{ .term = "ranked", .freq = if (i == 19) 100 else 1, .norm = 10 }} }});
        try writer.addSegment(bytes);
    }
    var included = roaring.RoaringBitmap.init(a);
    defer included.deinit();
    try included.addRange(0, 20);
    try included.prepareRead();
    var diagnostics: SearchDiagnostics = .{};
    var result = try execute(a, writer.snapshot(), .{ .query = .{ .term = .{ .field = "title", .term = "ranked" } }, .filter_doc_bitmap = &included, .k = 1, .include_stored = false, .diagnostics = &diagnostics });
    defer result.deinit();
    try std.testing.expectEqual(@as(usize, 1), result.hits.len);
    try std.testing.expectEqual(@as(u32, 19), result.hits[0].doc_id);
    try std.testing.expectEqual(@as(u64, 20), diagnostics.segments_considered);
    try std.testing.expect(diagnostics.segments_pruned > 0);
    try std.testing.expect(diagnostics.segments_searched < 20);
}
