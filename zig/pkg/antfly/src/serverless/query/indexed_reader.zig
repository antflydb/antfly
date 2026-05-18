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
const document_segment_mod = @import("../document_segment/mod.zig");
const segment_mod = @import("../segment/mod.zig");
const text_segment_mod = @import("../text_segment/mod.zig");
const sparse_segment_mod = @import("../sparse_segment/mod.zig");
const vector_segment_mod = @import("../vector_segment/mod.zig");
const query_request = @import("request.zig");
const query_plan = @import("plan.zig");
const runtime_mod = @import("runtime.zig");
const materializer_mod = @import("materializer.zig");
const vector_proto = @import("antfly_vector").proto;
const vector_quantizer = @import("antfly_vector").quantizer;
const vector_types = @import("antfly_vector").vector;

const ScoredDoc = struct {
    doc_id: []const u8,
    score: u32,
};

const ScoredCluster = struct {
    cluster_index: usize,
    score: f32,
};

const ApproxCandidate = struct {
    cluster_index: usize,
    local_index: usize,
    distance: f32,
    error_bound: f32,
};

pub const SearchExecutionStats = struct {
    actual_probe_count: usize = 0,
    actual_shortlist_count: usize = 0,
    quantized_candidate_count: usize = 0,
    exact_rerank_count: usize = 0,
    cluster_prune_count: usize = 0,
};

pub fn searchAlloc(alloc: Allocator, session: *runtime_mod.QuerySession, req: query_request.QueryRequest) ![]query_request.SearchHit {
    var stats: SearchExecutionStats = .{};
    return try searchAllocWithStats(alloc, session, req, &stats);
}

pub fn searchPlanAlloc(alloc: Allocator, session: *runtime_mod.QuerySession, plan: query_plan.SearchPlan) ![]query_request.SearchHit {
    var stats: SearchExecutionStats = .{};
    return try searchPlanAllocWithStats(alloc, session, plan, &stats);
}

pub fn searchAllocWithStats(
    alloc: Allocator,
    session: *runtime_mod.QuerySession,
    req: query_request.QueryRequest,
    stats: *SearchExecutionStats,
) ![]query_request.SearchHit {
    return try searchResolvedAllocWithStats(alloc, session, .{
        .mode = req.mode,
        .use_text = switch (req.mode) {
            .text => true,
            .hybrid => std.mem.trim(u8, req.text, &std.ascii.whitespace).len != 0,
            else => false,
        },
        .use_vector = switch (req.mode) {
            .vector => true,
            .hybrid => req.vector != null and req.vector.?.len != 0,
            else => false,
        },
        .use_sparse = switch (req.mode) {
            .sparse => true,
            .hybrid => req.sparse != null and req.sparse.?.len != 0,
            else => false,
        },
    }, req, stats);
}

pub fn searchPlanAllocWithStats(
    alloc: Allocator,
    session: *runtime_mod.QuerySession,
    plan: query_plan.SearchPlan,
    stats: *SearchExecutionStats,
) ![]query_request.SearchHit {
    return try searchResolvedAllocWithStats(alloc, session, .{
        .mode = plan.request.mode,
        .use_text = plan.usesTextLane(),
        .use_vector = plan.usesVectorLane(),
        .use_sparse = plan.usesSparseLane(),
    }, plan.request, stats);
}

const ResolvedSearchLanes = struct {
    mode: query_request.QueryMode,
    use_text: bool,
    use_vector: bool,
    use_sparse: bool,
};

fn searchResolvedAllocWithStats(
    alloc: Allocator,
    session: *runtime_mod.QuerySession,
    lanes: ResolvedSearchLanes,
    req: query_request.QueryRequest,
    stats: *SearchExecutionStats,
) ![]query_request.SearchHit {
    const docs = try loadPublishedDocumentsAlloc(alloc, session);
    defer materializer_mod.freeDocuments(alloc, docs);

    const scored_docs = switch (lanes.mode) {
        .text => try searchTextAlloc(alloc, session, req),
        .vector => try searchVectorAlloc(alloc, session, req, stats),
        .hybrid => try searchHybridAllocResolved(alloc, session, req, lanes, stats),
        .sparse => try searchSparseAlloc(alloc, session, req),
    };
    defer freeScoredDocs(alloc, scored_docs);

    const final_scored_docs = if (req.filter_text != null or req.exclusion_text != null)
        try applyTextFilterSetsAlloc(alloc, session, scored_docs, req)
    else
        scored_docs;
    defer if (final_scored_docs.ptr != scored_docs.ptr) freeScoredDocs(alloc, final_scored_docs);

    const hits = try alloc.alloc(query_request.SearchHit, final_scored_docs.len);
    var initialized_hits: usize = 0;
    errdefer query_request.freeHits(alloc, hits[0..initialized_hits]);
    for (final_scored_docs, 0..) |scored, idx| {
        const body = findBody(docs, scored.doc_id) orelse return error.DocumentBodyNotFound;
        hits[idx] = .{
            .doc_id = try alloc.dupe(u8, scored.doc_id),
            .body = try alloc.dupe(u8, body),
            .score = scored.score,
        };
        initialized_hits += 1;
    }
    return hits;
}

fn loadPublishedDocumentsAlloc(alloc: Allocator, session: *runtime_mod.QuerySession) ![]materializer_mod.Document {
    const doc_index = session.findArtifactIndex(.document_segment) orelse return error.DocumentSegmentNotFound;
    const doc_payload = try session.fetchArtifactAlloc(doc_index);
    defer alloc.free(doc_payload);
    const doc_entries = try document_segment_mod.decodeAlloc(alloc, doc_payload);
    defer document_segment_mod.freeEntries(alloc, doc_entries);

    const base_docs = try allocMaterializedDocumentsFromEntries(alloc, doc_entries);
    errdefer materializer_mod.freeDocuments(alloc, base_docs);

    const mutation_index = session.findArtifactIndex(.mutation_segment) orelse return base_docs;
    const mutation_payload = try session.fetchArtifactAlloc(mutation_index);
    defer alloc.free(mutation_payload);
    const mutation_entries = try segment_mod.decodeAlloc(alloc, mutation_payload);
    defer segment_mod.freeEntries(alloc, mutation_entries);
    const overlay = try allocMaterializerMutationsFromEntries(alloc, mutation_entries);
    defer freeMaterializerMutations(alloc, overlay);
    const docs = try materializer_mod.materializeOverBaseAlloc(alloc, base_docs, overlay);
    materializer_mod.freeDocuments(alloc, base_docs);
    return docs;
}

fn allocMaterializedDocumentsFromEntries(alloc: Allocator, entries: []const document_segment_mod.Entry) ![]materializer_mod.Document {
    const docs = try alloc.alloc(materializer_mod.Document, entries.len);
    errdefer alloc.free(docs);
    var initialized: usize = 0;
    errdefer {
        for (docs[0..initialized]) |*doc| doc.deinit(alloc);
    }
    for (entries, 0..) |entry, idx| {
        docs[idx] = .{
            .doc_id = try alloc.dupe(u8, entry.doc_id),
            .body = try alloc.dupe(u8, entry.body),
            .last_lsn = entry.last_lsn,
            .last_timestamp_ns = entry.last_timestamp_ns,
        };
        initialized += 1;
    }
    return docs;
}

fn allocMaterializerMutationsFromEntries(alloc: Allocator, entries: []const segment_mod.Entry) ![]materializer_mod.Mutation {
    const mutations = try alloc.alloc(materializer_mod.Mutation, entries.len);
    errdefer alloc.free(mutations);
    var initialized: usize = 0;
    errdefer freeMaterializerMutations(alloc, mutations[0..initialized]);
    for (entries, 0..) |entry, idx| {
        mutations[idx] = .{
            .lsn = entry.lsn,
            .timestamp_ns = entry.timestamp_ns,
            .kind = entry.kind,
            .doc_id = try alloc.dupe(u8, entry.doc_id),
            .body = if (entry.body) |body| try alloc.dupe(u8, body) else null,
        };
        initialized += 1;
    }
    return mutations;
}

fn freeMaterializerMutations(alloc: Allocator, mutations: []materializer_mod.Mutation) void {
    for (mutations) |mutation| {
        alloc.free(mutation.doc_id);
        if (mutation.body) |body| alloc.free(body);
    }
    alloc.free(mutations);
}

pub fn warmSearchPath(session: *runtime_mod.QuerySession, req: query_request.QueryRequest) !void {
    return try warmResolvedSearchPath(session, .{
        .mode = req.mode,
        .use_text = switch (req.mode) {
            .text => true,
            .hybrid => std.mem.trim(u8, req.text, &std.ascii.whitespace).len != 0,
            else => false,
        },
        .use_vector = switch (req.mode) {
            .vector => true,
            .hybrid => req.vector != null and req.vector.?.len != 0,
            else => false,
        },
        .use_sparse = switch (req.mode) {
            .sparse => true,
            .hybrid => req.sparse != null and req.sparse.?.len != 0,
            else => false,
        },
    }, req);
}

pub fn warmSearchPlanPath(session: *runtime_mod.QuerySession, plan: query_plan.SearchPlan) !void {
    return try warmResolvedSearchPath(session, .{
        .mode = plan.request.mode,
        .use_text = plan.usesTextLane(),
        .use_vector = plan.usesVectorLane(),
        .use_sparse = plan.usesSparseLane(),
    }, plan.request);
}

fn warmResolvedSearchPath(session: *runtime_mod.QuerySession, lanes: ResolvedSearchLanes, req: query_request.QueryRequest) !void {
    try session.warmArtifactKind(.document_segment);
    switch (lanes.mode) {
        .text => {
            const text_index = try resolveTextArtifactIndex(session, req);
            try session.warmArtifact(text_index);
        },
        .vector => {
            const vector_index = try resolveVectorArtifactIndex(session, req);
            const query_vector = req.vector orelse return error.VectorQueryRequired;
            try warmVectorArtifact(session, vector_index, req, query_vector);
        },
        .hybrid => {
            if (lanes.use_text) {
                const text_index = try resolveTextArtifactIndex(session, req);
                try session.warmArtifact(text_index);
            }
            if (lanes.use_vector) {
                const query_vector = req.vector orelse return error.VectorQueryRequired;
                const vector_index = try resolveVectorArtifactIndex(session, req);
                try warmVectorArtifact(session, vector_index, req, query_vector);
            }
            if (lanes.use_sparse) {
                const sparse_index = try resolveSparseArtifactIndex(session, req);
                try warmSparseArtifact(session, sparse_index, req);
            }
        },
        .sparse => {
            const sparse_index = try resolveSparseArtifactIndex(session, req);
            try warmSparseArtifact(session, sparse_index, req);
        },
    }
}

fn searchTextAlloc(alloc: Allocator, session: *runtime_mod.QuerySession, req: query_request.QueryRequest) ![]ScoredDoc {
    const text_index = try resolveTextArtifactIndex(session, req);
    const text_payload = try session.fetchArtifactAlloc(text_index);
    defer alloc.free(text_payload);
    var text_segment = try text_segment_mod.decodeAlloc(alloc, text_payload);
    defer text_segment_mod.freeSegment(alloc, &text_segment);
    return try searchTextSegmentAlloc(alloc, text_segment, req);
}

fn searchVectorAlloc(alloc: Allocator, session: *runtime_mod.QuerySession, req: query_request.QueryRequest, stats: *SearchExecutionStats) ![]ScoredDoc {
    const query_vector = req.vector orelse return error.VectorQueryRequired;
    const vector_index = try resolveVectorArtifactIndex(session, req);
    return try searchVectorArtifactAlloc(alloc, session, vector_index, req, query_vector, stats);
}

fn searchSparseAlloc(alloc: Allocator, session: *runtime_mod.QuerySession, req: query_request.QueryRequest) ![]ScoredDoc {
    const sparse_index = try resolveSparseArtifactIndex(session, req);
    return try searchSparseArtifactAlloc(alloc, session, sparse_index, req);
}

fn searchHybridAlloc(alloc: Allocator, session: *runtime_mod.QuerySession, req: query_request.QueryRequest, stats: *SearchExecutionStats) ![]ScoredDoc {
    return try searchHybridAllocResolved(alloc, session, req, .{
        .mode = .hybrid,
        .use_text = std.mem.trim(u8, req.text, &std.ascii.whitespace).len != 0,
        .use_vector = req.vector != null and req.vector.?.len != 0,
        .use_sparse = req.sparse != null and req.sparse.?.len != 0,
    }, stats);
}

fn searchHybridAllocResolved(
    alloc: Allocator,
    session: *runtime_mod.QuerySession,
    req: query_request.QueryRequest,
    lanes: ResolvedSearchLanes,
    stats: *SearchExecutionStats,
) ![]ScoredDoc {
    var merged = std.StringArrayHashMapUnmanaged(f32).empty;
    defer freeScoreMap(alloc, &merged);

    if (lanes.use_text) {
        const text_hits = try searchTextAlloc(alloc, session, req);
        defer freeScoredDocs(alloc, text_hits);
        try fuseHitsAlloc(alloc, &merged, text_hits, req.text_weight, req.fusion_strategy);
    }
    if (lanes.use_vector) {
        const vector_hits = try searchVectorAlloc(alloc, session, req, stats);
        defer freeScoredDocs(alloc, vector_hits);
        try fuseHitsAlloc(alloc, &merged, vector_hits, req.vector_weight, req.fusion_strategy);
    }
    if (lanes.use_sparse) {
        const sparse_hits = try searchSparseAlloc(alloc, session, req);
        defer freeScoredDocs(alloc, sparse_hits);
        try fuseHitsAlloc(alloc, &merged, sparse_hits, req.sparse_weight, req.fusion_strategy);
    }

    return try ownedScoredDocsFromFloatMap(alloc, merged, req.offset, req.limit);
}

fn searchTextSegmentAlloc(alloc: Allocator, text_segment: text_segment_mod.Segment, req: query_request.QueryRequest) ![]ScoredDoc {
    return try searchTextSegmentSpecAlloc(alloc, text_segment, req.text, req.operator, req.offset, req.limit, req.min_score);
}

fn searchTextSegmentSpecAlloc(
    alloc: Allocator,
    text_segment: text_segment_mod.Segment,
    text: []const u8,
    operator: query_request.QueryOperator,
    offset: usize,
    limit: usize,
    min_score: u32,
) ![]ScoredDoc {
    const normalized_query = try normalizeAlloc(alloc, text);
    defer alloc.free(normalized_query);
    if (normalized_query.len == 0) return try alloc.alloc(ScoredDoc, 0);

    const query_terms = try tokenizeAlloc(alloc, normalized_query);
    defer freeTokenSlice(alloc, query_terms);
    if (query_terms.len == 0) return try alloc.alloc(ScoredDoc, 0);

    const scores = try alloc.alloc(u32, text_segment.docs.len);
    defer alloc.free(scores);
    @memset(scores, 0);

    const matched_terms = try alloc.alloc(u16, text_segment.docs.len);
    defer alloc.free(matched_terms);
    @memset(matched_terms, 0);

    switch (operator) {
        .any_terms => accumulateAnyTerms(text_segment, query_terms, scores),
        .all_terms => accumulateAllTerms(text_segment, query_terms, scores, matched_terms),
        .phrase => accumulatePhrase(text_segment, normalized_query, query_terms[0], scores),
        .prefix_any_term => accumulatePrefixAnyTerms(text_segment, query_terms, scores),
    }

    var scored = std.ArrayListUnmanaged(ScoredDoc).empty;
    defer scored.deinit(alloc);
    for (scores, 0..) |score, doc_index| {
        if (score == 0) continue;
        if (operator == .all_terms and matched_terms[doc_index] != @as(u16, @intCast(query_terms.len))) continue;
        try scored.append(alloc, .{
            .doc_id = try alloc.dupe(u8, text_segment.docs[doc_index].doc_id),
            .score = score,
        });
    }

    std.mem.sort(ScoredDoc, scored.items, {}, lessScoredDoc);
    return try clipScoredDocsAlloc(alloc, scored.items, offset, limit, min_score);
}

fn applyTextFilterSetsAlloc(
    alloc: Allocator,
    session: *runtime_mod.QuerySession,
    hits: []const ScoredDoc,
    req: query_request.QueryRequest,
) ![]ScoredDoc {
    const text_index = try resolveTextArtifactIndex(session, req);
    const text_payload = try session.fetchArtifactAlloc(text_index);
    defer alloc.free(text_payload);
    var text_segment = try text_segment_mod.decodeAlloc(alloc, text_payload);
    defer text_segment_mod.freeSegment(alloc, &text_segment);

    const filter_hits = if (req.filter_text) |text|
        try searchTextSegmentSpecAlloc(alloc, text_segment, text, req.filter_operator, 0, text_segment.docs.len, 0)
    else
        try alloc.alloc(ScoredDoc, 0);
    defer freeScoredDocs(alloc, filter_hits);

    const exclusion_hits = if (req.exclusion_text) |text|
        try searchTextSegmentSpecAlloc(alloc, text_segment, text, req.exclusion_operator, 0, text_segment.docs.len, 0)
    else
        try alloc.alloc(ScoredDoc, 0);
    defer freeScoredDocs(alloc, exclusion_hits);

    var allowed = std.StringHashMapUnmanaged(void).empty;
    defer allowed.deinit(alloc);
    if (req.filter_text != null) {
        for (filter_hits) |hit| try allowed.put(alloc, hit.doc_id, {});
    }

    var excluded = std.StringHashMapUnmanaged(void).empty;
    defer excluded.deinit(alloc);
    if (req.exclusion_text != null) {
        for (exclusion_hits) |hit| try excluded.put(alloc, hit.doc_id, {});
    }

    var filtered = std.ArrayListUnmanaged(ScoredDoc).empty;
    errdefer {
        for (filtered.items) |hit| alloc.free(hit.doc_id);
        filtered.deinit(alloc);
    }
    for (hits) |hit| {
        if (req.filter_prefix) |filter_prefix| {
            if (!std.mem.startsWith(u8, hit.doc_id, filter_prefix)) continue;
        }
        if (req.filter_text != null and !allowed.contains(hit.doc_id)) continue;
        if (req.exclusion_text != null and excluded.contains(hit.doc_id)) continue;
        try filtered.append(alloc, .{
            .doc_id = try alloc.dupe(u8, hit.doc_id),
            .score = hit.score,
        });
    }
    return try filtered.toOwnedSlice(alloc);
}

fn resolveTextArtifactIndex(
    session: *runtime_mod.QuerySession,
    req: query_request.QueryRequest,
) !usize {
    if (req.indexes) |indexes| {
        for (indexes) |index_name| {
            if (session.findNamedArtifactIndex(.text_segment, index_name)) |artifact_index| return artifact_index;
        }
    }
    if (session.manifest.stats.published_search_sources.findText()) |text_source| {
        if (session.findNamedArtifactIndex(.text_segment, text_source.index_name)) |artifact_index| return artifact_index;
    }
    return session.findArtifactIndex(.text_segment) orelse error.TextSegmentNotFound;
}

fn resolveVectorArtifactIndex(
    session: *runtime_mod.QuerySession,
    req: query_request.QueryRequest,
) !usize {
    if (req.indexes) |indexes| {
        for (indexes) |index_name| {
            if (session.findNamedArtifactIndex(.vector_segment, index_name)) |artifact_index| return artifact_index;
        }
    }
    if (session.manifest.stats.published_search_sources.findVector()) |vector_source| {
        if (session.findNamedArtifactIndex(.vector_segment, vector_source.index_name)) |artifact_index| return artifact_index;
    }
    return session.findArtifactIndex(.vector_segment) orelse error.VectorSegmentNotFound;
}

fn resolveSparseArtifactIndex(
    session: *runtime_mod.QuerySession,
    req: query_request.QueryRequest,
) !usize {
    if (req.indexes) |indexes| {
        for (indexes) |index_name| {
            if (session.findNamedArtifactIndex(.sparse_segment, index_name)) |artifact_index| return artifact_index;
        }
    }
    if (session.manifest.stats.published_search_sources.findSparse()) |sparse_source| {
        if (session.findNamedArtifactIndex(.sparse_segment, sparse_source.index_name)) |artifact_index| return artifact_index;
    }
    return session.findArtifactIndex(.sparse_segment) orelse error.SparseSegmentNotFound;
}

fn searchSparseSegmentAlloc(alloc: Allocator, sparse_segment: sparse_segment_mod.Segment, req: query_request.QueryRequest) ![]ScoredDoc {
    const sparse_query = req.sparse orelse return error.SparseQueryRequired;
    if (sparse_query.len == 0) return try alloc.alloc(ScoredDoc, 0);

    const scores = try alloc.alloc(f32, sparse_segment.docs.len);
    defer alloc.free(scores);
    @memset(scores, 0);

    for (sparse_query) |feature| {
        const normalized_term = try normalizeAlloc(alloc, feature.term);
        defer alloc.free(normalized_term);
        if (normalized_term.len == 0) continue;
        const maybe_term = findSparseTermEntry(sparse_segment, normalized_term);
        const term = maybe_term orelse continue;
        for (term.postings) |posting| {
            scores[posting.doc_index] += posting.weight * feature.weight;
        }
    }

    var scored = std.ArrayListUnmanaged(ScoredDoc).empty;
    defer scored.deinit(alloc);
    for (scores, 0..) |score, doc_index| {
        if (score <= 0) continue;
        try scored.append(alloc, .{
            .doc_id = try alloc.dupe(u8, sparse_segment.docs[doc_index].doc_id),
            .score = @intFromFloat(score * 1000.0),
        });
    }
    std.mem.sort(ScoredDoc, scored.items, {}, lessScoredDoc);
    return try clipScoredDocsAlloc(alloc, scored.items, req.offset, req.limit, req.min_score);
}

fn searchSparseArtifactAlloc(
    alloc: Allocator,
    session: *runtime_mod.QuerySession,
    sparse_index: usize,
    req: query_request.QueryRequest,
) ![]ScoredDoc {
    const sparse_query = req.sparse orelse return error.SparseQueryRequired;
    if (sparse_query.len == 0) return try alloc.alloc(ScoredDoc, 0);

    const header_block_id = try sparseBlockIdAlloc(alloc, .header, 0);
    defer alloc.free(header_block_id);
    const header_bytes = try session.fetchArtifactBlockRangeAlloc(sparse_index, header_block_id, 0, 16);
    defer alloc.free(header_bytes);
    const header = try sparse_segment_mod.decodeHeader(header_bytes);

    const docs_offset: u64 = 16;
    const docs_block_id = try sparseBlockIdAlloc(alloc, .docs, 0);
    defer alloc.free(docs_block_id);
    const docs_bytes = try session.fetchArtifactBlockRangeAlloc(sparse_index, docs_block_id, docs_offset, header.docs_len);
    defer alloc.free(docs_bytes);
    const docs = try sparse_segment_mod.decodeDocsAlloc(alloc, header.doc_count, docs_bytes);
    defer {
        for (docs) |*doc| doc.deinit(alloc);
        alloc.free(docs);
    }

    const table_offset = docs_offset + header.docs_len;
    const table_len = sparse_segment_mod.termRecordLen() * @as(usize, @intCast(header.term_count));
    const table_block_id = try sparseBlockIdAlloc(alloc, .table, 0);
    defer alloc.free(table_block_id);
    const table_bytes = try session.fetchArtifactBlockRangeAlloc(sparse_index, table_block_id, table_offset, table_len);
    defer alloc.free(table_bytes);
    const term_records = try sparse_segment_mod.decodeTermTableAlloc(alloc, header.term_count, table_bytes);
    defer alloc.free(term_records);

    const terms_blob_offset = table_offset + table_len;
    const terms_block_id = try sparseBlockIdAlloc(alloc, .terms, 0);
    defer alloc.free(terms_block_id);
    const terms_bytes = try session.fetchArtifactBlockRangeAlloc(sparse_index, terms_block_id, terms_blob_offset, header.terms_blob_len);
    defer alloc.free(terms_bytes);

    const scores = try alloc.alloc(f32, docs.len);
    defer alloc.free(scores);
    @memset(scores, 0);

    for (sparse_query) |feature| {
        const normalized_term = try normalizeAlloc(alloc, feature.term);
        defer alloc.free(normalized_term);
        if (normalized_term.len == 0) continue;
        const maybe_match = try findSparseTermRecord(term_records, terms_bytes, normalized_term);
        const match = maybe_match orelse continue;
        const postings_block_id = try sparseBlockIdAlloc(alloc, .postings, match.term_index);
        defer alloc.free(postings_block_id);
        const postings_bytes = try session.fetchArtifactBlockRangeAlloc(
            sparse_index,
            postings_block_id,
            match.record.postings_offset,
            match.record.postings_len,
        );
        defer alloc.free(postings_bytes);
        const postings = try sparse_segment_mod.decodePostingBlockAlloc(alloc, match.record.doc_freq, postings_bytes);
        defer alloc.free(postings);
        for (postings) |posting| {
            scores[posting.doc_index] += posting.weight * feature.weight;
        }
    }

    var scored = std.ArrayListUnmanaged(ScoredDoc).empty;
    defer scored.deinit(alloc);
    for (scores, 0..) |score, doc_index| {
        if (score <= 0) continue;
        try scored.append(alloc, .{
            .doc_id = try alloc.dupe(u8, docs[doc_index].doc_id),
            .score = @intFromFloat(score * 1000.0),
        });
    }
    std.mem.sort(ScoredDoc, scored.items, {}, lessScoredDoc);
    return try clipScoredDocsAlloc(alloc, scored.items, req.offset, req.limit, req.min_score);
}

fn searchVectorSegmentAlloc(
    alloc: Allocator,
    vector_segment: vector_segment_mod.Segment,
    req: query_request.QueryRequest,
    query_vector: []const f32,
    stats: *SearchExecutionStats,
) ![]ScoredDoc {
    if (query_vector.len != vector_segment.dims) return error.VectorDimsMismatch;
    if (vector_segment.clusters.len == 0) return try alloc.alloc(ScoredDoc, 0);
    const effective_query = try normalizedCosineQueryAlloc(alloc, vector_segment.metric, query_vector);
    defer if (vector_segment.metric == .cosine) alloc.free(@constCast(effective_query));
    const query_measure = vectorQueryMeasure(effective_query, vector_segment.metric);

    var quantizer = try vector_quantizer.RaBitQuantizer.init(alloc, @intCast(vector_segment.dims), 42, vector_segment.metric);
    defer quantizer.deinit();

    var clusters = try alloc.alloc(ScoredCluster, vector_segment.clusters.len);
    defer alloc.free(clusters);
    for (vector_segment.clusters, 0..) |cluster, idx| {
        clusters[idx] = .{
            .cluster_index = idx,
            .score = routingScoreForQuery(effective_query, query_measure, cluster.centroid, vector_segment.metric),
        };
    }
    std.mem.sort(ScoredCluster, clusters, {}, lessScoredCluster);
    const probes = effectiveProbeCount(req, vector_segment.base_probe_count, clusters);
    stats.actual_probe_count = probes;

    var candidates = std.ArrayListUnmanaged(ApproxCandidate).empty;
    defer candidates.deinit(alloc);
    const needed = req.offset + req.limit;
    for (clusters[0..probes], 0..) |cluster_hit, probe_rank| {
        const cluster = vector_segment.clusters[cluster_hit.cluster_index];
        const start: usize = cluster.start_index;
        const end: usize = start + cluster.entry_count;
        if (start >= end) continue;

        if (cluster.quantized_set.len > 0) {
            var quantized = try vector_proto.RaBitQuantizedVectorSet.decode(alloc, cluster.quantized_set);
            defer quantized.deinit(alloc);
            const count = end - start;
            const distances = try alloc.alloc(f32, count);
            defer alloc.free(distances);
            const error_bounds = try alloc.alloc(f32, count);
            defer alloc.free(error_bounds);
            try quantizer.estimateDistances(&quantized, effective_query, distances, error_bounds);
            var local_candidates = std.ArrayListUnmanaged(ApproxCandidate).empty;
            defer local_candidates.deinit(alloc);
            for (distances, error_bounds, 0..) |distance, error_bound, idx| {
                try local_candidates.append(alloc, .{
                    .cluster_index = cluster_hit.cluster_index,
                    .local_index = idx,
                    .distance = distance,
                    .error_bound = error_bound,
                });
            }
            std.mem.sort(ApproxCandidate, local_candidates.items, {}, lessApproxCandidate);
            const local_cap = clusterCandidateCap(local_candidates.items, cluster, needed, probes, probe_rank);
            stats.quantized_candidate_count += local_candidates.items.len;
            if (local_cap < local_candidates.items.len) stats.cluster_prune_count += 1;
            for (local_candidates.items[0..@min(local_candidates.items.len, local_cap)]) |candidate| {
                try candidates.append(alloc, candidate);
            }
            continue;
        }

        for (start..end) |entry_index| {
            const entry = vector_segment.entries[entry_index];
            try candidates.append(alloc, .{
                .cluster_index = cluster_hit.cluster_index,
                .local_index = entry_index - start,
                .distance = vector_types.distanceToQuery(effective_query, query_measure, entry.vector, vector_segment.metric),
                .error_bound = 0,
            });
        }
    }

    if (candidates.items.len == 0) return try alloc.alloc(ScoredDoc, 0);

    std.mem.sort(ApproxCandidate, candidates.items, {}, lessApproxCandidate);
    const shortlist_count = vectorShortlistCount(candidates.items.len, probes, req.limit, req.offset, vector_segment.shortlist_multiplier);
    stats.actual_shortlist_count = shortlist_count;

    var scored = std.ArrayListUnmanaged(ScoredDoc).empty;
    defer scored.deinit(alloc);
    var exact_blocks = try alloc.alloc(?[]vector_segment_mod.Entry, vector_segment.clusters.len);
    defer {
        for (exact_blocks) |maybe_block| {
            if (maybe_block) |block| {
                for (block) |*entry| entry.deinit(alloc);
                alloc.free(block);
            }
        }
        alloc.free(exact_blocks);
    }
    @memset(exact_blocks, null);

    for (candidates.items[0..shortlist_count]) |candidate| {
        if (optimisticScoreCannotBeatFloor(candidate, vector_segment.metric, scored.items, needed)) break;
        if (exact_blocks[candidate.cluster_index] == null) {
            const cluster = vector_segment.clusters[candidate.cluster_index];
            exact_blocks[candidate.cluster_index] = try vector_segment_mod.decodeExactEntriesAlloc(
                alloc,
                vector_segment.dims,
                @intCast(cluster.entry_count),
                cluster.exact_entries,
            );
        }
        const block = exact_blocks[candidate.cluster_index].?;
        const entry = block[candidate.local_index];
        const similarity = similarityForQuery(effective_query, query_measure, entry.vector, vector_segment.metric);
        if (similarity <= 0) continue;
        stats.exact_rerank_count += 1;
        try scored.append(alloc, .{
            .doc_id = try alloc.dupe(u8, entry.doc_id),
            .score = @intFromFloat(similarity * 1000.0),
        });
        std.mem.sort(ScoredDoc, scored.items, {}, lessScoredDoc);
        trimScoredDocsToNeeded(alloc, &scored, needed);
    }
    return try clipScoredDocsAlloc(alloc, scored.items, req.offset, req.limit, req.min_score);
}

fn searchVectorArtifactAlloc(
    alloc: Allocator,
    session: *runtime_mod.QuerySession,
    vector_index: usize,
    req: query_request.QueryRequest,
    query_vector: []const f32,
    stats: *SearchExecutionStats,
) ![]ScoredDoc {
    const header_block_id = try vectorBlockIdAlloc(alloc, .header, 0);
    defer alloc.free(header_block_id);
    const header_bytes = try session.fetchArtifactBlockRangeAlloc(vector_index, header_block_id, 0, vector_segment_mod.header_len);
    defer alloc.free(header_bytes);
    const header = try vector_segment_mod.decodeHeader(header_bytes);
    if (query_vector.len != header.dims) return error.VectorDimsMismatch;
    if (header.cluster_count == 0) return try alloc.alloc(ScoredDoc, 0);
    const effective_query = try normalizedCosineQueryAlloc(alloc, header.metric, query_vector);
    defer if (header.metric == .cosine) alloc.free(@constCast(effective_query));
    const query_measure = vectorQueryMeasure(effective_query, header.metric);

    const table_len = vector_segment_mod.clusterRecordLen(header.dims) * @as(usize, @intCast(header.cluster_count));
    const table_block_id = try vectorBlockIdAlloc(alloc, .table, 0);
    defer alloc.free(table_block_id);
    const table_bytes = try session.fetchArtifactBlockRangeAlloc(vector_index, table_block_id, vector_segment_mod.header_len, table_len);
    defer alloc.free(table_bytes);
    const clusters = try vector_segment_mod.decodeClusterTableAlloc(alloc, header.dims, header.cluster_count, table_bytes);
    defer {
        for (clusters) |*cluster| cluster.deinit(alloc);
        alloc.free(clusters);
    }

    var quantizer = try vector_quantizer.RaBitQuantizer.init(alloc, @intCast(header.dims), 42, header.metric);
    defer quantizer.deinit();

    var ranked_clusters = try alloc.alloc(ScoredCluster, clusters.len);
    defer alloc.free(ranked_clusters);
    for (clusters, 0..) |cluster, idx| {
        ranked_clusters[idx] = .{
            .cluster_index = idx,
            .score = routingScoreForQuery(effective_query, query_measure, cluster.centroid, header.metric),
        };
    }
    std.mem.sort(ScoredCluster, ranked_clusters, {}, lessScoredCluster);
    const probes = effectiveProbeCount(req, header.base_probe_count, ranked_clusters);
    stats.actual_probe_count = probes;

    var candidates = std.ArrayListUnmanaged(ApproxCandidate).empty;
    defer candidates.deinit(alloc);
    const needed = req.offset + req.limit;
    for (ranked_clusters[0..probes], 0..) |cluster_hit, probe_rank| {
        const cluster = clusters[cluster_hit.cluster_index];
        if (cluster.entry_count == 0) continue;

        if (cluster.quantized_len > 0) {
            const quantized_block_id = try vectorBlockIdAlloc(alloc, .quantized, cluster_hit.cluster_index);
            defer alloc.free(quantized_block_id);
            const quantized_bytes = try session.fetchArtifactBlockRangeAlloc(
                vector_index,
                quantized_block_id,
                cluster.quantized_offset,
                cluster.quantized_len,
            );
            defer alloc.free(quantized_bytes);
            var quantized = try vector_proto.RaBitQuantizedVectorSet.decode(alloc, quantized_bytes);
            defer quantized.deinit(alloc);
            const count: usize = @intCast(cluster.entry_count);
            const distances = try alloc.alloc(f32, count);
            defer alloc.free(distances);
            const error_bounds = try alloc.alloc(f32, count);
            defer alloc.free(error_bounds);
            try quantizer.estimateDistances(&quantized, effective_query, distances, error_bounds);
            var local_candidates = std.ArrayListUnmanaged(ApproxCandidate).empty;
            defer local_candidates.deinit(alloc);
            for (distances, error_bounds, 0..) |distance, error_bound, idx| {
                try local_candidates.append(alloc, .{
                    .cluster_index = cluster_hit.cluster_index,
                    .local_index = idx,
                    .distance = distance,
                    .error_bound = error_bound,
                });
            }
            std.mem.sort(ApproxCandidate, local_candidates.items, {}, lessApproxCandidate);
            const local_cap = clusterCandidateCap(local_candidates.items, cluster, needed, probes, probe_rank);
            stats.quantized_candidate_count += local_candidates.items.len;
            if (local_cap < local_candidates.items.len) stats.cluster_prune_count += 1;
            for (local_candidates.items[0..@min(local_candidates.items.len, local_cap)]) |candidate| {
                try candidates.append(alloc, candidate);
            }
            continue;
        }

        const exact_block_id = try vectorBlockIdAlloc(alloc, .exact, cluster_hit.cluster_index);
        defer alloc.free(exact_block_id);
        const exact_bytes = try session.fetchArtifactBlockRangeAlloc(
            vector_index,
            exact_block_id,
            cluster.exact_entries_offset,
            cluster.exact_entries_len,
        );
        defer alloc.free(exact_bytes);
        const exact_entries = try vector_segment_mod.decodeExactEntriesAlloc(
            alloc,
            header.dims,
            @intCast(cluster.entry_count),
            exact_bytes,
        );
        defer {
            for (exact_entries) |*entry| entry.deinit(alloc);
            alloc.free(exact_entries);
        }
        for (exact_entries, 0..) |entry, idx| {
            try candidates.append(alloc, .{
                .cluster_index = cluster_hit.cluster_index,
                .local_index = idx,
                .distance = vector_types.distanceToQuery(effective_query, query_measure, entry.vector, header.metric),
                .error_bound = 0,
            });
        }
    }

    if (candidates.items.len == 0) return try alloc.alloc(ScoredDoc, 0);

    std.mem.sort(ApproxCandidate, candidates.items, {}, lessApproxCandidate);
    const shortlist_count = vectorShortlistCount(candidates.items.len, probes, req.limit, req.offset, header.shortlist_multiplier);
    stats.actual_shortlist_count = shortlist_count;

    var scored = std.ArrayListUnmanaged(ScoredDoc).empty;
    defer {
        for (scored.items) |hit| alloc.free(hit.doc_id);
        scored.deinit(alloc);
    }
    var exact_blocks = try alloc.alloc(?[]vector_segment_mod.Entry, clusters.len);
    defer {
        for (exact_blocks) |maybe_block| {
            if (maybe_block) |block| {
                for (block) |*entry| entry.deinit(alloc);
                alloc.free(block);
            }
        }
        alloc.free(exact_blocks);
    }
    @memset(exact_blocks, null);

    for (candidates.items[0..shortlist_count]) |candidate| {
        if (optimisticScoreCannotBeatFloor(candidate, header.metric, scored.items, needed)) break;
        if (exact_blocks[candidate.cluster_index] == null) {
            const cluster = clusters[candidate.cluster_index];
            const exact_block_id = try vectorBlockIdAlloc(alloc, .exact, candidate.cluster_index);
            defer alloc.free(exact_block_id);
            const exact_bytes = try session.fetchArtifactBlockRangeAlloc(vector_index, exact_block_id, cluster.exact_entries_offset, cluster.exact_entries_len);
            defer alloc.free(exact_bytes);
            exact_blocks[candidate.cluster_index] = try vector_segment_mod.decodeExactEntriesAlloc(
                alloc,
                header.dims,
                @intCast(cluster.entry_count),
                exact_bytes,
            );
        }
        const block = exact_blocks[candidate.cluster_index].?;
        const entry = block[candidate.local_index];
        const similarity = similarityForQuery(effective_query, query_measure, entry.vector, header.metric);
        if (similarity <= 0) continue;
        stats.exact_rerank_count += 1;
        try scored.append(alloc, .{
            .doc_id = try alloc.dupe(u8, entry.doc_id),
            .score = @intFromFloat(similarity * 1000.0),
        });
        std.mem.sort(ScoredDoc, scored.items, {}, lessScoredDoc);
        trimScoredDocsToNeeded(alloc, &scored, needed);
    }
    var merged = std.StringArrayHashMapUnmanaged(u32).empty;
    defer freeScoreMapU32(alloc, &merged);
    for (scored.items) |hit| {
        const gop = try merged.getOrPut(alloc, hit.doc_id);
        if (!gop.found_existing) {
            gop.key_ptr.* = try alloc.dupe(u8, hit.doc_id);
            gop.value_ptr.* = hit.score;
        } else if (hit.score > gop.value_ptr.*) {
            gop.value_ptr.* = hit.score;
        }
    }
    const merged_owned = try ownedScoredDocsFromMap(alloc, merged, 0, merged.count());
    defer alloc.free(merged_owned);
    return try clipScoredDocsAlloc(alloc, merged_owned, req.offset, req.limit, req.min_score);
}

fn warmVectorArtifact(
    session: *runtime_mod.QuerySession,
    vector_index: usize,
    req: query_request.QueryRequest,
    query_vector: []const f32,
) !void {
    const alloc = session.alloc;
    const header_block_id = try vectorBlockIdAlloc(alloc, .header, 0);
    defer alloc.free(header_block_id);
    const header_bytes = try session.fetchArtifactBlockRangeAlloc(vector_index, header_block_id, 0, vector_segment_mod.header_len);
    defer alloc.free(header_bytes);
    const header = try vector_segment_mod.decodeHeader(header_bytes);
    if (query_vector.len != header.dims) return error.VectorDimsMismatch;
    if (header.cluster_count == 0) return;
    const effective_query = try normalizedCosineQueryAlloc(alloc, header.metric, query_vector);
    defer if (header.metric == .cosine) alloc.free(@constCast(effective_query));
    const query_measure = vectorQueryMeasure(effective_query, header.metric);

    const table_len = vector_segment_mod.clusterRecordLen(header.dims) * @as(usize, @intCast(header.cluster_count));
    const table_block_id = try vectorBlockIdAlloc(alloc, .table, 0);
    defer alloc.free(table_block_id);
    const table_bytes = try session.fetchArtifactBlockRangeAlloc(vector_index, table_block_id, vector_segment_mod.header_len, table_len);
    defer alloc.free(table_bytes);
    const clusters = try vector_segment_mod.decodeClusterTableAlloc(alloc, header.dims, header.cluster_count, table_bytes);
    defer {
        for (clusters) |*cluster| cluster.deinit(alloc);
        alloc.free(clusters);
    }

    var ranked_clusters = try alloc.alloc(ScoredCluster, clusters.len);
    defer alloc.free(ranked_clusters);
    for (clusters, 0..) |cluster, idx| {
        ranked_clusters[idx] = .{
            .cluster_index = idx,
            .score = routingScoreForQuery(effective_query, query_measure, cluster.centroid, header.metric),
        };
    }
    std.mem.sort(ScoredCluster, ranked_clusters, {}, lessScoredCluster);
    const probes = effectiveProbeCount(req, header.base_probe_count, ranked_clusters);

    for (ranked_clusters[0..probes]) |cluster_hit| {
        const cluster = clusters[cluster_hit.cluster_index];
        if (cluster.quantized_len > 0) {
            const quantized_block_id = try vectorBlockIdAlloc(alloc, .quantized, cluster_hit.cluster_index);
            defer alloc.free(quantized_block_id);
            const quantized = try session.fetchArtifactBlockRangeAlloc(
                vector_index,
                quantized_block_id,
                cluster.quantized_offset,
                cluster.quantized_len,
            );
            alloc.free(quantized);
        }
        if (cluster.exact_entries_len > 0) {
            const exact_block_id = try vectorBlockIdAlloc(alloc, .exact, cluster_hit.cluster_index);
            defer alloc.free(exact_block_id);
            const exact = try session.fetchArtifactBlockRangeAlloc(
                vector_index,
                exact_block_id,
                cluster.exact_entries_offset,
                cluster.exact_entries_len,
            );
            alloc.free(exact);
        }
    }
}

fn warmSparseArtifact(session: *runtime_mod.QuerySession, sparse_index: usize, req: query_request.QueryRequest) !void {
    const alloc = session.alloc;
    const sparse_query = req.sparse orelse return error.SparseQueryRequired;
    if (sparse_query.len == 0) return;

    const header_block_id = try sparseBlockIdAlloc(alloc, .header, 0);
    defer alloc.free(header_block_id);
    const header_bytes = try session.fetchArtifactBlockRangeAlloc(sparse_index, header_block_id, 0, 16);
    defer alloc.free(header_bytes);
    const header = try sparse_segment_mod.decodeHeader(header_bytes);

    const docs_offset: u64 = 16;
    const docs_block_id = try sparseBlockIdAlloc(alloc, .docs, 0);
    defer alloc.free(docs_block_id);
    const docs = try session.fetchArtifactBlockRangeAlloc(sparse_index, docs_block_id, docs_offset, header.docs_len);
    alloc.free(docs);

    const table_offset = docs_offset + header.docs_len;
    const table_len = sparse_segment_mod.termRecordLen() * @as(usize, @intCast(header.term_count));
    const table_block_id = try sparseBlockIdAlloc(alloc, .table, 0);
    defer alloc.free(table_block_id);
    const table_bytes = try session.fetchArtifactBlockRangeAlloc(sparse_index, table_block_id, table_offset, table_len);
    defer alloc.free(table_bytes);
    const term_records = try sparse_segment_mod.decodeTermTableAlloc(alloc, header.term_count, table_bytes);
    defer alloc.free(term_records);

    const terms_blob_offset = table_offset + table_len;
    const terms_block_id = try sparseBlockIdAlloc(alloc, .terms, 0);
    defer alloc.free(terms_block_id);
    const terms_bytes = try session.fetchArtifactBlockRangeAlloc(sparse_index, terms_block_id, terms_blob_offset, header.terms_blob_len);
    defer alloc.free(terms_bytes);

    for (sparse_query) |feature| {
        const normalized_term = try normalizeAlloc(alloc, feature.term);
        defer alloc.free(normalized_term);
        if (normalized_term.len == 0) continue;
        const maybe_match = try findSparseTermRecord(term_records, terms_bytes, normalized_term);
        const match = maybe_match orelse continue;
        const postings_block_id = try sparseBlockIdAlloc(alloc, .postings, match.term_index);
        defer alloc.free(postings_block_id);
        const postings = try session.fetchArtifactBlockRangeAlloc(
            sparse_index,
            postings_block_id,
            match.record.postings_offset,
            match.record.postings_len,
        );
        alloc.free(postings);
    }
}

const VectorBlockKind = enum {
    header,
    table,
    quantized,
    exact,
};

fn vectorBlockIdAlloc(alloc: Allocator, kind: VectorBlockKind, cluster_index: usize) ![]u8 {
    return switch (kind) {
        .header => try alloc.dupe(u8, "vector-header"),
        .table => try alloc.dupe(u8, "vector-table"),
        .quantized => try std.fmt.allocPrint(alloc, "vector-cluster-{d}-quantized", .{cluster_index}),
        .exact => try std.fmt.allocPrint(alloc, "vector-cluster-{d}-exact", .{cluster_index}),
    };
}

const SparseBlockKind = enum {
    header,
    docs,
    table,
    terms,
    postings,
};

fn sparseBlockIdAlloc(alloc: Allocator, kind: SparseBlockKind, term_index: usize) ![]u8 {
    return switch (kind) {
        .header => try alloc.dupe(u8, "sparse-header"),
        .docs => try alloc.dupe(u8, "sparse-docs"),
        .table => try alloc.dupe(u8, "sparse-table"),
        .terms => try alloc.dupe(u8, "sparse-terms"),
        .postings => try std.fmt.allocPrint(alloc, "sparse-term-{d}-postings", .{term_index}),
    };
}

const SparseTermMatch = struct {
    term_index: usize,
    record: sparse_segment_mod.TermRecord,
};

fn findSparseTermRecord(
    term_records: []const sparse_segment_mod.TermRecord,
    terms_blob: []const u8,
    term: []const u8,
) !?SparseTermMatch {
    for (term_records, 0..) |record, idx| {
        const candidate = try sparse_segment_mod.termBytes(terms_blob, record);
        const order = std.mem.order(u8, candidate, term);
        if (order == .eq) return .{ .term_index = idx, .record = record };
        if (order == .gt) break;
    }
    return null;
}

fn metricScore(lhs: []const f32, rhs: []const f32, metric: vector_types.DistanceMetric) f32 {
    return switch (metric) {
        .l2_squared => -vector_types.distance(lhs, rhs, .l2_squared),
        .inner_product => vector_types.dot(lhs, rhs),
        .cosine => vector_types.cosineSimilarity(lhs, rhs),
    };
}

fn normalizedCosineQueryAlloc(
    alloc: Allocator,
    metric: vector_types.DistanceMetric,
    query: []const f32,
) ![]const f32 {
    if (metric != .cosine) return query;
    const normalized = try alloc.dupe(f32, query);
    _ = vector_types.normalize(normalized);
    return normalized;
}

fn vectorQueryMeasure(query: []const f32, metric: vector_types.DistanceMetric) f32 {
    return switch (metric) {
        .l2_squared => vector_types.dot(query, query),
        .inner_product => 0,
        .cosine => vector_types.norm(query),
    };
}

fn routingScoreForQuery(
    query: []const f32,
    query_measure: f32,
    candidate: []const f32,
    metric: vector_types.DistanceMetric,
) f32 {
    return switch (metric) {
        .l2_squared => -vector_types.distanceToQuery(query, query_measure, candidate, .l2_squared),
        .inner_product => vector_types.dot(query, candidate),
        .cosine => 1.0 - vector_types.distanceToQuery(query, query_measure, candidate, .cosine),
    };
}

fn similarityForMetric(lhs: []const f32, rhs: []const f32, metric: vector_types.DistanceMetric) f32 {
    return switch (metric) {
        .l2_squared => 1.0 / (1.0 + vector_types.distance(lhs, rhs, .l2_squared)),
        .inner_product => vector_types.dot(lhs, rhs),
        .cosine => vector_types.cosineSimilarity(lhs, rhs),
    };
}

fn similarityForQuery(
    query: []const f32,
    query_measure: f32,
    candidate: []const f32,
    metric: vector_types.DistanceMetric,
) f32 {
    return switch (metric) {
        .l2_squared => 1.0 / (1.0 + vector_types.distanceToQuery(query, query_measure, candidate, .l2_squared)),
        .inner_product => vector_types.dot(query, candidate),
        .cosine => 1.0 - vector_types.distanceToQuery(query, query_measure, candidate, .cosine),
    };
}

fn vectorShortlistCount(candidate_count: usize, probes: usize, limit: usize, offset: usize, shortlist_multiplier: u32) usize {
    if (candidate_count == 0) return 0;
    const needed = offset + limit;
    const multiplier: usize = @max(@as(usize, 2), shortlist_multiplier);
    const probe_budget = @max(probes * multiplier, probes + needed);
    const shortlist = @max(needed + 4, @max(needed * multiplier, probe_budget));
    return @min(candidate_count, shortlist);
}

fn clusterCandidateCap(
    candidates: []const ApproxCandidate,
    cluster: vector_segment_mod.Cluster,
    needed: usize,
    probes: usize,
    probe_rank: usize,
) usize {
    if (candidates.len == 0) return 0;
    const base_needed = @max(@as(usize, 1), needed);
    const base = std.math.divCeil(usize, base_needed * 2, @max(@as(usize, 1), probes)) catch base_needed;
    const rank_bonus: usize = if (probe_rank < 2) 4 else if (probe_rank < 4) 2 else 0;
    var cap = @min(candidates.len, @max(@as(usize, 4), base + rank_bonus));
    if (cap >= candidates.len) return candidates.len;

    const best_distance = optimisticDistance(candidates[0]);
    const boundary_distance = optimisticDistance(candidates[cap - 1]);
    const tail_distance = optimisticDistance(candidates[candidates.len - 1]);
    const total_spread = @max(@as(f32, 0.0001), tail_distance - best_distance);
    const kept_spread = @max(@as(f32, 0), boundary_distance - best_distance);
    const spread_ratio = kept_spread / total_spread;
    const cluster_spread = @max(@as(f32, 0), cluster.routing_distance_max - cluster.routing_distance_min);
    const avg_distance_bias = @max(@as(f32, 0), cluster.routing_distance_avg - cluster.routing_distance_min);

    if (spread_ratio < 0.25 or cluster_spread < 0.08 or avg_distance_bias < 0.05) {
        cap += @max(@as(usize, 2), cap / 2);
    } else if ((spread_ratio > 0.75 or avg_distance_bias > 0.25) and cap > 4) {
        cap -= @max(@as(usize, 1), cap / 4);
    }
    return @min(candidates.len, @max(@as(usize, 4), cap));
}

fn optimisticDistance(candidate: ApproxCandidate) f32 {
    return @max(@as(f32, 0), candidate.distance - candidate.error_bound);
}

fn effectiveProbeCount(req: query_request.QueryRequest, minimum_hint: u32, ranked_clusters: []const ScoredCluster) usize {
    const cluster_count = ranked_clusters.len;
    if (cluster_count == 0) return 0;
    const auto = autoProbeCount(cluster_count);
    const hinted = @max(@as(usize, minimum_hint), auto);
    const requested = resolveRequestedProbeCount(req, cluster_count);
    var probes = if (requested == 0) hinted else @max(@as(usize, requested), hinted);
    probes = @min(cluster_count, @max(@as(usize, 1), probes));
    const max_probe_budget = @min(cluster_count, @max(probes, probes * 2));
    while (probes < max_probe_budget) : (probes += 1) {
        const prev = ranked_clusters[probes - 1].score;
        const next = ranked_clusters[probes].score;
        if ((prev - next) > probeScoreGapTolerance(ranked_clusters[0].score, prev)) break;
    }
    return probes;
}

fn resolveRequestedProbeCount(req: query_request.QueryRequest, cluster_count: usize) u32 {
    const effort = normalizedSearchEffort(req.search_effort) orelse return req.num_probes;
    if (cluster_count == 0) return 0;
    if (effort == 0) return 1;
    if (effort >= 1) return std.math.cast(u32, cluster_count) orelse std.math.maxInt(u32);
    const balanced: f32 = 0.5;
    const min_probes: u32 = 1;
    const balanced_probes: u32 = @max(min_probes, req.num_probes);
    const max_probes: u32 = std.math.cast(u32, cluster_count) orelse std.math.maxInt(u32);
    if (effort <= balanced) {
        const ratio = effort / balanced;
        return min_probes + @as(u32, @intFromFloat(@as(f32, @floatFromInt(balanced_probes - min_probes)) * ratio));
    }
    const ratio = (effort - balanced) / (1 - balanced);
    const span = max_probes -| balanced_probes;
    return @min(max_probes, balanced_probes + @as(u32, @intFromFloat(@as(f32, @floatFromInt(span)) * ratio)));
}

fn normalizedSearchEffort(effort: ?f32) ?f32 {
    const value = effort orelse return null;
    if (std.math.isNan(value)) return null;
    if (value < 0) return 0;
    if (value > 1) return 1;
    return value;
}

fn autoProbeCount(cluster_count: usize) usize {
    if (cluster_count <= 2) return cluster_count;
    const logish: usize = @intFromFloat(@ceil(std.math.log2(@as(f64, @floatFromInt(cluster_count)))));
    return @min(cluster_count, @max(@as(usize, 2), logish + 1));
}

fn probeScoreGapTolerance(best: f32, current: f32) f32 {
    const scale = @max(@abs(best), @abs(current));
    return @max(@as(f32, 0.02), scale * 0.08);
}

fn optimisticScoreCannotBeatFloor(
    candidate: ApproxCandidate,
    metric: vector_types.DistanceMetric,
    scored: []const ScoredDoc,
    needed: usize,
) bool {
    if (needed == 0 or scored.len < needed) return false;
    const optimistic_distance = @max(@as(f32, 0), candidate.distance - candidate.error_bound);
    return scoreFromSimilarity(similarityFromDistance(optimistic_distance, metric)) <= scored[needed - 1].score;
}

fn similarityFromDistance(distance: f32, metric: vector_types.DistanceMetric) f32 {
    return switch (metric) {
        .l2_squared => 1.0 / (1.0 + distance),
        .inner_product => -distance,
        .cosine => 1.0 - distance,
    };
}

fn scoreFromSimilarity(similarity: f32) u32 {
    if (similarity <= 0) return 0;
    const scaled = @min(similarity * 1000.0, @as(f32, @floatFromInt(std.math.maxInt(u32))));
    return @intFromFloat(scaled);
}

fn trimScoredDocsToNeeded(alloc: Allocator, scored: *std.ArrayListUnmanaged(ScoredDoc), needed: usize) void {
    if (needed == 0 or scored.items.len <= needed) return;
    for (scored.items[needed..]) |*entry| {
        alloc.free(entry.doc_id);
    }
    scored.shrinkRetainingCapacity(needed);
}

const reciprocal_rank_k: f32 = 60.0;

fn fuseHitsAlloc(
    alloc: Allocator,
    merged: *std.StringArrayHashMapUnmanaged(f32),
    hits: []const ScoredDoc,
    weight: f32,
    strategy: query_request.QueryFusionStrategy,
) !void {
    if (weight <= 0 or hits.len == 0) return;
    for (hits, 0..) |hit, rank| {
        const contribution = switch (strategy) {
            .weighted_rrf => weight / (reciprocal_rank_k + @as(f32, @floatFromInt(rank + 1))),
            .weighted_sum => weight * @as(f32, @floatFromInt(hit.score)),
        };
        const gop = try merged.getOrPut(alloc, hit.doc_id);
        if (!gop.found_existing) {
            gop.key_ptr.* = try alloc.dupe(u8, hit.doc_id);
            gop.value_ptr.* = 0;
        }
        gop.value_ptr.* += contribution;
    }
}

fn freeScoreMap(alloc: Allocator, merged: *std.StringArrayHashMapUnmanaged(f32)) void {
    for (merged.keys()) |key| alloc.free(key);
    merged.deinit(alloc);
}

fn freeScoreMapU32(alloc: Allocator, merged: *std.StringArrayHashMapUnmanaged(u32)) void {
    for (merged.keys()) |key| alloc.free(key);
    merged.deinit(alloc);
}

fn ownedScoredDocsFromFloatMap(
    alloc: Allocator,
    merged: std.StringArrayHashMapUnmanaged(f32),
    offset: usize,
    limit: usize,
) ![]ScoredDoc {
    var scored = try alloc.alloc(ScoredDoc, merged.count());
    errdefer alloc.free(scored);
    var initialized: usize = 0;
    errdefer {
        for (scored[0..initialized]) |hit| alloc.free(hit.doc_id);
    }
    for (merged.keys(), merged.values(), 0..) |doc_id, score, idx| {
        const scaled = @min(score * 1_000.0, @as(f32, @floatFromInt(std.math.maxInt(u32))));
        scored[idx] = .{
            .doc_id = try alloc.dupe(u8, doc_id),
            .score = @intFromFloat(scaled),
        };
        initialized += 1;
    }
    std.mem.sort(ScoredDoc, scored, {}, lessScoredDoc);
    const out = try clipScoredDocsAlloc(alloc, scored, offset, limit, 0);
    alloc.free(scored);
    return out;
}

fn ownedScoredDocsFromMap(
    alloc: Allocator,
    merged: std.StringArrayHashMapUnmanaged(u32),
    offset: usize,
    limit: usize,
) ![]ScoredDoc {
    var scored = try alloc.alloc(ScoredDoc, merged.count());
    errdefer alloc.free(scored);
    var initialized: usize = 0;
    errdefer {
        for (scored[0..initialized]) |hit| alloc.free(hit.doc_id);
    }
    for (merged.keys(), merged.values(), 0..) |doc_id, score, idx| {
        scored[idx] = .{
            .doc_id = try alloc.dupe(u8, doc_id),
            .score = score,
        };
        initialized += 1;
    }
    std.mem.sort(ScoredDoc, scored, {}, lessScoredDoc);
    const out = try clipScoredDocsAlloc(alloc, scored, offset, limit, 0);
    alloc.free(scored);
    return out;
}

fn clipScoredDocsAlloc(alloc: Allocator, scored: []ScoredDoc, offset: usize, limit: usize, min_score: u32) ![]ScoredDoc {
    var filtered_count: usize = 0;
    for (scored) |hit| {
        if (hit.score >= min_score) filtered_count += 1;
    }
    if (offset >= filtered_count) {
        for (scored) |hit| alloc.free(hit.doc_id);
        return try alloc.alloc(ScoredDoc, 0);
    }

    const visible = @min(filtered_count - offset, limit);
    const out = try alloc.alloc(ScoredDoc, visible);
    var filtered_index: usize = 0;
    var out_index: usize = 0;
    for (scored) |hit| {
        if (hit.score < min_score) {
            alloc.free(hit.doc_id);
            continue;
        }
        if (filtered_index < offset) {
            alloc.free(hit.doc_id);
            filtered_index += 1;
            continue;
        }
        if (out_index < visible) {
            out[out_index] = hit;
            out_index += 1;
            filtered_index += 1;
            continue;
        }
        alloc.free(hit.doc_id);
    }
    return out;
}

fn freeScoredDocs(alloc: Allocator, hits: []ScoredDoc) void {
    for (hits) |hit| alloc.free(hit.doc_id);
    alloc.free(hits);
}

test "indexed reader merges duplicate vector doc hits by best score" {
    const alloc = std.testing.allocator;
    var merged = std.StringArrayHashMapUnmanaged(u32).empty;
    defer freeScoreMapU32(alloc, &merged);

    {
        const gop = try merged.getOrPut(alloc, "doc-a");
        gop.key_ptr.* = try alloc.dupe(u8, "doc-a");
        gop.value_ptr.* = 400;
    }
    {
        const gop = try merged.getOrPut(alloc, "doc-a");
        try std.testing.expect(gop.found_existing);
        gop.value_ptr.* = @max(gop.value_ptr.*, 900);
    }
    {
        const gop = try merged.getOrPut(alloc, "doc-b");
        gop.key_ptr.* = try alloc.dupe(u8, "doc-b");
        gop.value_ptr.* = 700;
    }

    const merged_owned = try ownedScoredDocsFromMap(alloc, merged, 0, merged.count());
    defer freeScoredDocs(alloc, merged_owned);
    try std.testing.expectEqual(@as(usize, 2), merged_owned.len);
    try std.testing.expectEqualStrings("doc-a", merged_owned[0].doc_id);
    try std.testing.expectEqual(@as(u32, 900), merged_owned[0].score);
}

fn accumulateAnyTerms(text_segment: text_segment_mod.Segment, query_terms: []const []const u8, scores: []u32) void {
    for (query_terms) |term| {
        const term_entry = findTerm(text_segment.terms, term) orelse continue;
        for (term_entry.postings) |posting| scores[posting.doc_index] += posting.term_freq;
    }
}

fn accumulateAllTerms(
    text_segment: text_segment_mod.Segment,
    query_terms: []const []const u8,
    scores: []u32,
    matched_terms: []u16,
) void {
    for (query_terms) |term| {
        const term_entry = findTerm(text_segment.terms, term) orelse return;
        for (term_entry.postings) |posting| {
            scores[posting.doc_index] += posting.term_freq;
            matched_terms[posting.doc_index] += 1;
        }
    }
    for (scores, 0..) |score, doc_index| {
        if (score == 0) continue;
        if (matched_terms[doc_index] == @as(u16, @intCast(query_terms.len))) {
            scores[doc_index] = score + @as(u32, @intCast(query_terms.len * 10));
        } else {
            scores[doc_index] = 0;
        }
    }
}

fn accumulatePhrase(
    text_segment: text_segment_mod.Segment,
    normalized_query: []const u8,
    seed_term: []const u8,
    scores: []u32,
) void {
    const seed = findTerm(text_segment.terms, seed_term) orelse return;
    for (seed.postings) |posting| {
        const doc = text_segment.docs[posting.doc_index];
        if (std.mem.indexOf(u8, doc.normalized_text, normalized_query) == null) continue;
        scores[posting.doc_index] = 100 + posting.term_freq;
    }
}

fn accumulatePrefixAnyTerms(text_segment: text_segment_mod.Segment, query_terms: []const []const u8, scores: []u32) void {
    for (text_segment.terms) |term_entry| {
        var matched = false;
        for (query_terms) |prefix| {
            if (std.mem.startsWith(u8, term_entry.term, prefix)) {
                matched = true;
                break;
            }
        }
        if (!matched) continue;
        for (term_entry.postings) |posting| scores[posting.doc_index] += posting.term_freq;
    }
}

fn findTerm(terms: []const text_segment_mod.TermEntry, needle: []const u8) ?text_segment_mod.TermEntry {
    var lo: usize = 0;
    var hi: usize = terms.len;
    while (lo < hi) {
        const mid = lo + ((hi - lo) / 2);
        switch (std.mem.order(u8, terms[mid].term, needle)) {
            .eq => return terms[mid],
            .lt => lo = mid + 1,
            .gt => hi = mid,
        }
    }
    return null;
}

fn findBody(entries: []const materializer_mod.Document, doc_id: []const u8) ?[]const u8 {
    for (entries) |entry| {
        if (std.mem.eql(u8, entry.doc_id, doc_id)) return entry.body;
    }
    return null;
}

fn lessScoredDoc(_: void, lhs: ScoredDoc, rhs: ScoredDoc) bool {
    if (lhs.score != rhs.score) return lhs.score > rhs.score;
    return std.mem.order(u8, lhs.doc_id, rhs.doc_id) == .lt;
}

fn lessScoredCluster(_: void, lhs: ScoredCluster, rhs: ScoredCluster) bool {
    if (lhs.score != rhs.score) return lhs.score > rhs.score;
    return lhs.cluster_index < rhs.cluster_index;
}

fn lessApproxCandidate(_: void, lhs: ApproxCandidate, rhs: ApproxCandidate) bool {
    const lhs_best = lhs.distance - lhs.error_bound;
    const rhs_best = rhs.distance - rhs.error_bound;
    if (lhs_best != rhs_best) return lhs_best < rhs_best;
    if (lhs.distance != rhs.distance) return lhs.distance < rhs.distance;
    if (lhs.cluster_index != rhs.cluster_index) return lhs.cluster_index < rhs.cluster_index;
    return lhs.local_index < rhs.local_index;
}

fn findSparseTermEntry(segment: sparse_segment_mod.Segment, term: []const u8) ?sparse_segment_mod.TermEntry {
    for (segment.terms) |entry| {
        const order = std.mem.order(u8, entry.term, term);
        if (order == .eq) return entry;
        if (order == .gt) break;
    }
    return null;
}

pub fn normalizeAlloc(alloc: Allocator, value: []const u8) ![]u8 {
    var out = std.ArrayListUnmanaged(u8).empty;
    errdefer out.deinit(alloc);
    var pending_space = false;
    for (value) |byte| {
        const lowered = std.ascii.toLower(byte);
        if (std.ascii.isWhitespace(lowered) or std.ascii.isControl(lowered)) {
            pending_space = out.items.len > 0;
            continue;
        }
        if (pending_space) {
            try out.append(alloc, ' ');
            pending_space = false;
        }
        try out.append(alloc, lowered);
    }
    return try out.toOwnedSlice(alloc);
}

fn tokenizeAlloc(alloc: Allocator, normalized: []const u8) ![]const []const u8 {
    var list = std.ArrayListUnmanaged([]const u8).empty;
    errdefer list.deinit(alloc);
    var iter = std.mem.tokenizeAny(u8, normalized, " ");
    while (iter.next()) |token| {
        if (token.len == 0) continue;
        try list.append(alloc, try alloc.dupe(u8, token));
    }
    return try list.toOwnedSlice(alloc);
}

fn freeTokenSlice(alloc: Allocator, tokens: []const []const u8) void {
    for (tokens) |token| alloc.free(token);
    alloc.free(tokens);
}

test "normalizeAlloc lowercases and compresses whitespace" {
    const alloc = std.testing.allocator;
    const normalized = try normalizeAlloc(alloc, " Alpha\tBravo \n  Charlie ");
    defer alloc.free(normalized);
    try std.testing.expectEqualStrings("alpha bravo charlie", normalized);
}

test "indexed reader uses text postings for all-term and prefix search" {
    const alloc = std.testing.allocator;
    var text_segment = text_segment_mod.Segment{
        .docs = try alloc.alloc(text_segment_mod.DocumentEntry, 3),
        .terms = try alloc.alloc(text_segment_mod.TermEntry, 4),
    };
    defer text_segment_mod.freeSegment(alloc, &text_segment);

    text_segment.docs[0] = .{ .doc_id = try alloc.dupe(u8, "doc-a"), .normalized_text = try alloc.dupe(u8, "alpha bravo bravo"), .token_count = 3 };
    text_segment.docs[1] = .{ .doc_id = try alloc.dupe(u8, "doc-b"), .normalized_text = try alloc.dupe(u8, "alpha charlie"), .token_count = 2 };
    text_segment.docs[2] = .{ .doc_id = try alloc.dupe(u8, "doc-c"), .normalized_text = try alloc.dupe(u8, "alpine beta"), .token_count = 2 };
    text_segment.terms[0] = .{ .term = try alloc.dupe(u8, "alpha"), .postings = try alloc.dupe(text_segment_mod.Posting, &.{ .{ .doc_index = 0, .term_freq = 1 }, .{ .doc_index = 1, .term_freq = 1 } }) };
    text_segment.terms[1] = .{ .term = try alloc.dupe(u8, "alpine"), .postings = try alloc.dupe(text_segment_mod.Posting, &.{.{ .doc_index = 2, .term_freq = 1 }}) };
    text_segment.terms[2] = .{ .term = try alloc.dupe(u8, "bravo"), .postings = try alloc.dupe(text_segment_mod.Posting, &.{.{ .doc_index = 0, .term_freq = 2 }}) };
    text_segment.terms[3] = .{ .term = try alloc.dupe(u8, "charlie"), .postings = try alloc.dupe(text_segment_mod.Posting, &.{.{ .doc_index = 1, .term_freq = 1 }}) };

    var req = query_request.QueryRequest{
        .text = try alloc.dupe(u8, "alpha bravo"),
        .operator = .all_terms,
    };
    defer req.deinit(alloc);
    const all_hits = try searchTextSegmentAlloc(alloc, text_segment, req);
    defer freeScoredDocs(alloc, all_hits);
    try std.testing.expectEqual(@as(usize, 1), all_hits.len);
    try std.testing.expectEqualStrings("doc-a", all_hits[0].doc_id);
}

test "indexed reader honors min_score when clipping results" {
    const alloc = std.testing.allocator;
    const scored = try alloc.dupe(ScoredDoc, &.{
        .{ .doc_id = try alloc.dupe(u8, "doc-a"), .score = 120 },
        .{ .doc_id = try alloc.dupe(u8, "doc-b"), .score = 80 },
        .{ .doc_id = try alloc.dupe(u8, "doc-c"), .score = 40 },
    });
    defer alloc.free(scored);
    const out = try clipScoredDocsAlloc(alloc, scored, 0, 10, 81);
    defer freeScoredDocs(alloc, out);
    try std.testing.expectEqual(@as(usize, 1), out.len);
    try std.testing.expectEqualStrings("doc-a", out[0].doc_id);
}

test "indexed reader scores vector segment by cosine similarity" {
    const alloc = std.testing.allocator;
    var quantizer = try vector_quantizer.RaBitQuantizer.init(alloc, 2, 42, vector_types.DistanceMetric.cosine);
    defer quantizer.deinit();

    const cluster0_vectors = [_]f32{ 1.0, 0.0 };
    var cluster0_quantized = try quantizer.quantize(&.{ 1.0, 0.0 }, &cluster0_vectors, 1);
    defer cluster0_quantized.deinit(alloc);
    const cluster0_blob = try cluster0_quantized.encode(alloc);
    defer alloc.free(cluster0_blob);
    var cluster0_entry = vector_segment_mod.Entry{
        .doc_id = try alloc.dupe(u8, "doc-a"),
        .vector = try alloc.dupe(f32, &.{ 1.0, 0.0 }),
    };
    defer cluster0_entry.deinit(alloc);
    const cluster0_exact = try vector_segment_mod.encodeExactEntriesAlloc(alloc, &.{cluster0_entry});
    defer alloc.free(cluster0_exact);

    const cluster1_vectors = [_]f32{ 0.5, 0.5 };
    var cluster1_quantized = try quantizer.quantize(&.{ 0.5, 0.5 }, &cluster1_vectors, 1);
    defer cluster1_quantized.deinit(alloc);
    const cluster1_blob = try cluster1_quantized.encode(alloc);
    defer alloc.free(cluster1_blob);
    var cluster1_entry = vector_segment_mod.Entry{
        .doc_id = try alloc.dupe(u8, "doc-b"),
        .vector = try alloc.dupe(f32, &.{ 0.5, 0.5 }),
    };
    defer cluster1_entry.deinit(alloc);
    const cluster1_exact = try vector_segment_mod.encodeExactEntriesAlloc(alloc, &.{cluster1_entry});
    defer alloc.free(cluster1_exact);

    var vector_segment = vector_segment_mod.Segment{
        .dims = 2,
        .metric = .cosine,
        .clusters = try alloc.alloc(vector_segment_mod.Cluster, 2),
        .entries = try alloc.alloc(vector_segment_mod.Entry, 2),
    };
    defer vector_segment_mod.freeSegment(alloc, &vector_segment);
    vector_segment.clusters[0] = .{
        .centroid = try alloc.dupe(f32, &.{ 1.0, 0.0 }),
        .start_index = 0,
        .entry_count = 1,
        .quantized_set = try alloc.dupe(u8, cluster0_blob),
        .exact_entries = try alloc.dupe(u8, cluster0_exact),
    };
    vector_segment.clusters[1] = .{
        .centroid = try alloc.dupe(f32, &.{ 0.5, 0.5 }),
        .start_index = 1,
        .entry_count = 1,
        .quantized_set = try alloc.dupe(u8, cluster1_blob),
        .exact_entries = try alloc.dupe(u8, cluster1_exact),
    };
    vector_segment.entries[0] = .{ .doc_id = try alloc.dupe(u8, "doc-a"), .vector = try alloc.dupe(f32, &.{ 1.0, 0.0 }) };
    vector_segment.entries[1] = .{ .doc_id = try alloc.dupe(u8, "doc-b"), .vector = try alloc.dupe(f32, &.{ 0.5, 0.5 }) };

    var req = query_request.QueryRequest{
        .text = try alloc.dupe(u8, ""),
        .vector = try alloc.dupe(f32, &.{ 1.0, 0.0 }),
        .mode = .vector,
        .num_probes = 1,
    };
    defer req.deinit(alloc);
    var stats: SearchExecutionStats = .{};
    const hits = try searchVectorSegmentAlloc(alloc, vector_segment, req, req.vector.?, &stats);
    defer freeScoredDocs(alloc, hits);
    try std.testing.expectEqual(@as(usize, 2), hits.len);
    try std.testing.expectEqualStrings("doc-a", hits[0].doc_id);
}

test "indexed reader scores vector segment by inner product" {
    const alloc = std.testing.allocator;
    var quantizer = try vector_quantizer.RaBitQuantizer.init(alloc, 2, 42, vector_types.DistanceMetric.inner_product);
    defer quantizer.deinit();

    const cluster0_vectors = [_]f32{ 2.0, 0.0 };
    var cluster0_quantized = try quantizer.quantize(&.{ 2.0, 0.0 }, &cluster0_vectors, 1);
    defer cluster0_quantized.deinit(alloc);
    const cluster0_blob = try cluster0_quantized.encode(alloc);
    defer alloc.free(cluster0_blob);
    var cluster0_entry = vector_segment_mod.Entry{
        .doc_id = try alloc.dupe(u8, "doc-dot"),
        .vector = try alloc.dupe(f32, &.{ 2.0, 0.0 }),
    };
    defer cluster0_entry.deinit(alloc);
    const cluster0_exact = try vector_segment_mod.encodeExactEntriesAlloc(alloc, &.{cluster0_entry});
    defer alloc.free(cluster0_exact);

    const cluster1_vectors = [_]f32{ 0.75, 0.75 };
    var cluster1_quantized = try quantizer.quantize(&.{ 0.75, 0.75 }, &cluster1_vectors, 1);
    defer cluster1_quantized.deinit(alloc);
    const cluster1_blob = try cluster1_quantized.encode(alloc);
    defer alloc.free(cluster1_blob);
    var cluster1_entry = vector_segment_mod.Entry{
        .doc_id = try alloc.dupe(u8, "doc-cos"),
        .vector = try alloc.dupe(f32, &.{ 0.75, 0.75 }),
    };
    defer cluster1_entry.deinit(alloc);
    const cluster1_exact = try vector_segment_mod.encodeExactEntriesAlloc(alloc, &.{cluster1_entry});
    defer alloc.free(cluster1_exact);

    var vector_segment = vector_segment_mod.Segment{
        .dims = 2,
        .metric = .inner_product,
        .clusters = try alloc.alloc(vector_segment_mod.Cluster, 2),
        .entries = try alloc.alloc(vector_segment_mod.Entry, 2),
    };
    defer vector_segment_mod.freeSegment(alloc, &vector_segment);
    vector_segment.clusters[0] = .{
        .centroid = try alloc.dupe(f32, &.{ 2.0, 0.0 }),
        .start_index = 0,
        .entry_count = 1,
        .quantized_set = try alloc.dupe(u8, cluster0_blob),
        .exact_entries = try alloc.dupe(u8, cluster0_exact),
    };
    vector_segment.clusters[1] = .{
        .centroid = try alloc.dupe(f32, &.{ 0.75, 0.75 }),
        .start_index = 1,
        .entry_count = 1,
        .quantized_set = try alloc.dupe(u8, cluster1_blob),
        .exact_entries = try alloc.dupe(u8, cluster1_exact),
    };
    vector_segment.entries[0] = .{ .doc_id = try alloc.dupe(u8, "doc-dot"), .vector = try alloc.dupe(f32, &.{ 2.0, 0.0 }) };
    vector_segment.entries[1] = .{ .doc_id = try alloc.dupe(u8, "doc-cos"), .vector = try alloc.dupe(f32, &.{ 0.75, 0.75 }) };

    var req = query_request.QueryRequest{
        .text = try alloc.dupe(u8, ""),
        .vector = try alloc.dupe(f32, &.{ 1.0, 1.0 }),
        .mode = .vector,
        .num_probes = 2,
    };
    defer req.deinit(alloc);
    var stats: SearchExecutionStats = .{};
    const hits = try searchVectorSegmentAlloc(alloc, vector_segment, req, req.vector.?, &stats);
    defer freeScoredDocs(alloc, hits);
    try std.testing.expectEqual(@as(usize, 2), hits.len);
    try std.testing.expectEqualStrings("doc-dot", hits[0].doc_id);
}

test "indexed reader scores vector segment by l2 distance" {
    const alloc = std.testing.allocator;
    var quantizer = try vector_quantizer.RaBitQuantizer.init(alloc, 2, 42, vector_types.DistanceMetric.l2_squared);
    defer quantizer.deinit();

    const cluster0_vectors = [_]f32{ 1.0, 1.0 };
    var cluster0_quantized = try quantizer.quantize(&.{ 1.0, 1.0 }, &cluster0_vectors, 1);
    defer cluster0_quantized.deinit(alloc);
    const cluster0_blob = try cluster0_quantized.encode(alloc);
    defer alloc.free(cluster0_blob);
    var cluster0_entry = vector_segment_mod.Entry{
        .doc_id = try alloc.dupe(u8, "doc-near"),
        .vector = try alloc.dupe(f32, &.{ 1.0, 1.0 }),
    };
    defer cluster0_entry.deinit(alloc);
    const cluster0_exact = try vector_segment_mod.encodeExactEntriesAlloc(alloc, &.{cluster0_entry});
    defer alloc.free(cluster0_exact);

    const cluster1_vectors = [_]f32{ 5.0, 5.0 };
    var cluster1_quantized = try quantizer.quantize(&.{ 5.0, 5.0 }, &cluster1_vectors, 1);
    defer cluster1_quantized.deinit(alloc);
    const cluster1_blob = try cluster1_quantized.encode(alloc);
    defer alloc.free(cluster1_blob);
    var cluster1_entry = vector_segment_mod.Entry{
        .doc_id = try alloc.dupe(u8, "doc-far"),
        .vector = try alloc.dupe(f32, &.{ 5.0, 5.0 }),
    };
    defer cluster1_entry.deinit(alloc);
    const cluster1_exact = try vector_segment_mod.encodeExactEntriesAlloc(alloc, &.{cluster1_entry});
    defer alloc.free(cluster1_exact);

    var vector_segment = vector_segment_mod.Segment{
        .dims = 2,
        .metric = .l2_squared,
        .clusters = try alloc.alloc(vector_segment_mod.Cluster, 2),
        .entries = try alloc.alloc(vector_segment_mod.Entry, 2),
    };
    defer vector_segment_mod.freeSegment(alloc, &vector_segment);
    vector_segment.clusters[0] = .{
        .centroid = try alloc.dupe(f32, &.{ 1.0, 1.0 }),
        .start_index = 0,
        .entry_count = 1,
        .quantized_set = try alloc.dupe(u8, cluster0_blob),
        .exact_entries = try alloc.dupe(u8, cluster0_exact),
    };
    vector_segment.clusters[1] = .{
        .centroid = try alloc.dupe(f32, &.{ 5.0, 5.0 }),
        .start_index = 1,
        .entry_count = 1,
        .quantized_set = try alloc.dupe(u8, cluster1_blob),
        .exact_entries = try alloc.dupe(u8, cluster1_exact),
    };
    vector_segment.entries[0] = .{ .doc_id = try alloc.dupe(u8, "doc-near"), .vector = try alloc.dupe(f32, &.{ 1.0, 1.0 }) };
    vector_segment.entries[1] = .{ .doc_id = try alloc.dupe(u8, "doc-far"), .vector = try alloc.dupe(f32, &.{ 5.0, 5.0 }) };

    var req = query_request.QueryRequest{
        .text = try alloc.dupe(u8, ""),
        .vector = try alloc.dupe(f32, &.{ 0.9, 1.1 }),
        .mode = .vector,
        .num_probes = 2,
    };
    defer req.deinit(alloc);
    var stats: SearchExecutionStats = .{};
    const hits = try searchVectorSegmentAlloc(alloc, vector_segment, req, req.vector.?, &stats);
    defer freeScoredDocs(alloc, hits);
    try std.testing.expectEqual(@as(usize, 2), hits.len);
    try std.testing.expectEqualStrings("doc-near", hits[0].doc_id);
}

test "indexed reader scores sparse segment by weighted postings" {
    const alloc = std.testing.allocator;
    var sparse_segment = sparse_segment_mod.Segment{
        .docs = try alloc.alloc(sparse_segment_mod.DocumentEntry, 2),
        .terms = try alloc.alloc(sparse_segment_mod.TermEntry, 2),
    };
    defer sparse_segment_mod.freeSegment(alloc, &sparse_segment);

    sparse_segment.docs[0] = .{ .doc_id = try alloc.dupe(u8, "doc-a"), .feature_count = 2 };
    sparse_segment.docs[1] = .{ .doc_id = try alloc.dupe(u8, "doc-b"), .feature_count = 1 };
    sparse_segment.terms[0] = .{
        .term = try alloc.dupe(u8, "alpha"),
        .postings = try alloc.dupe(sparse_segment_mod.Posting, &.{
            .{ .doc_index = 0, .weight = 1.0 },
            .{ .doc_index = 1, .weight = 0.25 },
        }),
    };
    sparse_segment.terms[1] = .{
        .term = try alloc.dupe(u8, "bravo"),
        .postings = try alloc.dupe(sparse_segment_mod.Posting, &.{
            .{ .doc_index = 0, .weight = 0.5 },
        }),
    };

    const sparse_query = try alloc.dupe(query_request.SparseTermWeight, &.{
        .{ .term = try alloc.dupe(u8, "alpha"), .weight = 1.0 },
        .{ .term = try alloc.dupe(u8, "bravo"), .weight = 0.5 },
    });
    var req = query_request.QueryRequest{
        .text = try alloc.dupe(u8, ""),
        .sparse = sparse_query,
        .mode = .sparse,
    };
    defer req.deinit(alloc);

    const hits = try searchSparseSegmentAlloc(alloc, sparse_segment, req);
    defer freeScoredDocs(alloc, hits);
    try std.testing.expectEqual(@as(usize, 2), hits.len);
    try std.testing.expectEqualStrings("doc-a", hits[0].doc_id);
}

test "indexed reader hybrid mode merges text and vector hits" {
    const alloc = std.testing.allocator;
    var merged = std.StringArrayHashMapUnmanaged(f32).empty;
    defer freeScoreMap(alloc, &merged);
    const text_hits = try alloc.dupe(ScoredDoc, &.{
        .{ .doc_id = "doc-a", .score = 300 },
        .{ .doc_id = "doc-b", .score = 200 },
    });
    defer alloc.free(text_hits);
    const vector_hits = try alloc.dupe(ScoredDoc, &.{
        .{ .doc_id = "doc-b", .score = 900 },
        .{ .doc_id = "doc-a", .score = 100 },
    });
    defer alloc.free(vector_hits);

    try fuseHitsAlloc(alloc, &merged, text_hits, 1.0, .weighted_rrf);
    try fuseHitsAlloc(alloc, &merged, vector_hits, 8.0, .weighted_rrf);

    const hits = try ownedScoredDocsFromFloatMap(alloc, merged, 0, 10);
    defer freeScoredDocs(alloc, hits);
    try std.testing.expectEqual(@as(usize, 2), hits.len);
    try std.testing.expectEqualStrings("doc-b", hits[0].doc_id);
}

test "vector shortlist grows with probes and stays bounded by candidates" {
    try std.testing.expectEqual(@as(usize, 10), vectorShortlistCount(10, 1, 10, 0, 2));
    try std.testing.expectEqual(@as(usize, 32), vectorShortlistCount(100, 4, 8, 0, 4));
    try std.testing.expectEqual(@as(usize, 16), vectorShortlistCount(20, 8, 5, 0, 2));
}

test "cluster candidate cap favors earlier probes but stays bounded" {
    const candidates = [_]ApproxCandidate{
        .{ .cluster_index = 0, .local_index = 0, .distance = 0.10, .error_bound = 0.01 },
        .{ .cluster_index = 0, .local_index = 1, .distance = 0.11, .error_bound = 0.01 },
        .{ .cluster_index = 0, .local_index = 2, .distance = 0.12, .error_bound = 0.01 },
        .{ .cluster_index = 0, .local_index = 3, .distance = 0.13, .error_bound = 0.01 },
        .{ .cluster_index = 0, .local_index = 4, .distance = 0.14, .error_bound = 0.01 },
        .{ .cluster_index = 0, .local_index = 5, .distance = 0.30, .error_bound = 0.01 },
        .{ .cluster_index = 0, .local_index = 6, .distance = 0.45, .error_bound = 0.01 },
        .{ .cluster_index = 0, .local_index = 7, .distance = 0.60, .error_bound = 0.01 },
    };
    const tight_cluster = vector_segment_mod.Cluster{
        .centroid = &.{},
        .start_index = 0,
        .entry_count = candidates.len,
        .routing_distance_min = 0.01,
        .routing_distance_max = 0.05,
        .quantized_set = &.{},
        .exact_entries = &.{},
    };
    const wide_cluster = vector_segment_mod.Cluster{
        .centroid = &.{},
        .start_index = 0,
        .entry_count = candidates.len,
        .routing_distance_min = 0.01,
        .routing_distance_max = 0.50,
        .quantized_set = &.{},
        .exact_entries = &.{},
    };

    try std.testing.expectEqual(@as(usize, 8), clusterCandidateCap(&candidates, tight_cluster, 4, 4, 0));
    try std.testing.expectEqual(@as(usize, 6), clusterCandidateCap(&candidates, wide_cluster, 4, 4, 5));
    try std.testing.expectEqual(@as(usize, 3), clusterCandidateCap(candidates[0..3], wide_cluster, 10, 2, 0));
}

test "adaptive probe count expands when cluster scores are tightly grouped" {
    const ranked = [_]ScoredCluster{
        .{ .cluster_index = 0, .score = 1.0 },
        .{ .cluster_index = 1, .score = 0.97 },
        .{ .cluster_index = 2, .score = 0.95 },
        .{ .cluster_index = 3, .score = 0.2 },
    };
    try std.testing.expectEqual(@as(usize, 3), effectiveProbeCount(.{ .text = @constCast(""), .num_probes = 1 }, 2, &ranked));
}

test "search effort controls serverless vector probe count" {
    try std.testing.expectEqual(@as(u32, 1), resolveRequestedProbeCount(.{ .text = @constCast(""), .search_effort = 0.0 }, 8));
    try std.testing.expectEqual(@as(u32, 2), resolveRequestedProbeCount(.{ .text = @constCast(""), .num_probes = 2, .search_effort = 0.5 }, 8));
    try std.testing.expectEqual(@as(u32, 8), resolveRequestedProbeCount(.{ .text = @constCast(""), .num_probes = 2, .search_effort = 1.0 }, 8));
}

test "optimistic bound pruning rejects hopeless candidates once floor exists" {
    const scored = [_]ScoredDoc{
        .{ .doc_id = "doc-a", .score = 800 },
        .{ .doc_id = "doc-b", .score = 700 },
    };
    try std.testing.expect(optimisticScoreCannotBeatFloor(.{
        .cluster_index = 0,
        .local_index = 0,
        .distance = 0.6,
        .error_bound = 0.05,
    }, .cosine, &scored, 2));
    try std.testing.expect(!optimisticScoreCannotBeatFloor(.{
        .cluster_index = 0,
        .local_index = 0,
        .distance = 0.1,
        .error_bound = 0.05,
    }, .cosine, &scored, 2));
}

test "hybrid weighted sum favors higher raw modality score" {
    const alloc = std.testing.allocator;
    var merged = std.StringArrayHashMapUnmanaged(f32).empty;
    defer freeScoreMap(alloc, &merged);
    const text_hits = try alloc.dupe(ScoredDoc, &.{
        .{ .doc_id = "doc-a", .score = 5000 },
        .{ .doc_id = "doc-b", .score = 100 },
    });
    defer alloc.free(text_hits);
    const vector_hits = try alloc.dupe(ScoredDoc, &.{
        .{ .doc_id = "doc-b", .score = 3000 },
    });
    defer alloc.free(vector_hits);

    try fuseHitsAlloc(alloc, &merged, text_hits, 1.0, .weighted_sum);
    try fuseHitsAlloc(alloc, &merged, vector_hits, 0.25, .weighted_sum);
    const hits = try ownedScoredDocsFromFloatMap(alloc, merged, 0, 10);
    defer freeScoredDocs(alloc, hits);
    try std.testing.expectEqualStrings("doc-a", hits[0].doc_id);
}
