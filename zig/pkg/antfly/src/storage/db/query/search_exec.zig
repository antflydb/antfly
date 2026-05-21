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
const types = @import("../types.zig");
const aggregations_mod = @import("../aggregations.zig");
const index_manager_mod = @import("../catalog/index_manager.zig");
const runtime_schema_mod = @import("../../schema.zig");
const docstore_mod = @import("../../docstore.zig");
const internal_keys = @import("../../internal_keys.zig");
const doc_set = @import("../doc_set.zig");
const graph_exec = @import("graph_exec.zig");
const search_mod = @import("../../../search/search.zig");
const index_mod = @import("../../../index.zig");
const segment_mod = @import("../../../segment.zig");
const distributed_stats_mod = @import("../../../search/distributed_stats.zig");
const analysis_mod = @import("../../../search/analysis.zig");
const introducer_mod = @import("../../../introducer.zig");
const platform_time = @import("../../../platform/time.zig");
const platform = @import("antfly_platform");
const vectorindex_mod = @import("antfly_vectorindex");
const builtin = @import("builtin");
const sparse_mod = if (builtin.os.tag == .freestanding)
    @import("../sparse_stub.zig")
else
    @import("../../../sparse/sparse.zig");

fn getenv(name: [*:0]const u8) ?[]const u8 {
    return platform.env.getenv(name);
}

const default_balanced_search_effort: f32 = 0.5;
var bench_query_profile_counter: std.atomic.Value(u64) = .init(0);
const bench_query_profile_unknown = std.math.maxInt(u64);
const bench_query_profile_disabled = std.math.maxInt(u64) - 1;
var bench_query_profile_every_cache: std.atomic.Value(u64) = .init(bench_query_profile_unknown);

pub const SearchTextDispatcher = struct {
    ctx: ?*anyopaque,
    func: *const fn (
        ctx: ?*anyopaque,
        alloc: Allocator,
        req: types.SearchRequest,
        text_query: types.TextQuery,
    ) anyerror!types.SearchResult,
};

pub const SearchTextQueryExecutor = struct {
    ctx: ?*anyopaque,
    text_index_entry: *const fn (
        ctx: ?*anyopaque,
        index_name: ?[]const u8,
    ) anyerror!?*index_manager_mod.IndexManager.TextIndex,
    text_index_is_chunk_backed: *const fn (
        ctx: ?*anyopaque,
        alloc: Allocator,
        index_name: ?[]const u8,
    ) anyerror!bool,
    search_match_all: *const fn (
        ctx: ?*anyopaque,
        alloc: Allocator,
        req: types.SearchRequest,
    ) anyerror!types.SearchResult,
    project_stored_search: *const fn (
        ctx: ?*anyopaque,
        alloc: Allocator,
        req: types.SearchRequest,
        doc_key: []const u8,
        raw: []const u8,
    ) anyerror![]u8,
    resolve_doc_set_doc_ids: ?*const fn (
        ctx: ?*anyopaque,
        alloc: Allocator,
        set: *const doc_set.ResolvedDocSet,
        generation: ?u64,
    ) anyerror!?[]const []const u8 = null,
    resolve_doc_ids_to_doc_set: ?*const fn (
        ctx: ?*anyopaque,
        alloc: Allocator,
        doc_ids: []const []const u8,
        generation: ?u64,
    ) anyerror!doc_set.ResolvedDocSet = null,
    live_filter_doc_set: ?*const fn (
        ctx: ?*anyopaque,
        alloc: Allocator,
        set: *const doc_set.ResolvedDocSet,
        generation: ?u64,
    ) anyerror!doc_set.ResolvedDocSet = null,
    postprocess: *const fn (
        ctx: ?*anyopaque,
        alloc: Allocator,
        req: types.SearchRequest,
        raw: types.SearchResult,
        chunk_backed: bool,
    ) anyerror!types.SearchResult,
};

pub const SearchTextStatsExecutor = struct {
    ctx: ?*anyopaque,
    text_index_entry: *const fn (
        ctx: ?*anyopaque,
        index_name: ?[]const u8,
    ) anyerror!?*index_manager_mod.IndexManager.TextIndex,
};

pub const ExplicitTextStatRequest = struct {
    index_name: ?[]const u8 = null,
    field: []const u8,
    terms: []const []const u8 = &.{},
    resolved_doc_filter: ?*const doc_set.ResolvedDocFilter = null,
};

pub const ExplicitBackgroundTextStatRequest = struct {
    aggregation_name: []const u8,
    index_name: ?[]const u8 = null,
    field: []const u8,
    terms: []const []const u8 = &.{},
    background_query: aggregations_mod.BackgroundQuery,
    resolved_doc_filter: ?*const doc_set.ResolvedDocFilter = null,
};

const SearchRequestTextStatEntry = struct {
    field: []const u8 = "",
    index_name: ?[]const u8 = null,
    terms: std.StringHashMapUnmanaged(void) = .{},
};

pub const RuntimePreflight = struct {
    has_full_text_results: bool = false,
    embedding_result_names: []const []const u8 = &.{},
    graph_queries: []const types.NamedGraphQuery = &.{},
};

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

pub const DenseSearchExecutor = struct {
    ctx: ?*anyopaque,
    text_index_entry: *const fn (
        ctx: ?*anyopaque,
        index_name: ?[]const u8,
    ) anyerror!?*index_manager_mod.IndexManager.TextIndex,
    dense_index: *const fn (
        ctx: ?*anyopaque,
        index_name: ?[]const u8,
    ) anyerror!?*index_manager_mod.IndexManager.DenseIndex,
    lookup_doc_key: *const fn (
        ctx: ?*anyopaque,
        index_name: []const u8,
        vector_id: u64,
    ) anyerror!?[]u8,
    lookup_vector_id: *const fn (
        ctx: ?*anyopaque,
        index_name: []const u8,
        doc_key: []const u8,
    ) anyerror!?u64,
    lookup_vector_ids_for_ordinals: ?*const fn (
        ctx: ?*anyopaque,
        alloc: Allocator,
        index_name: []const u8,
        ordinals: []const u32,
    ) anyerror![]u64 = null,
    lookup_doc_ordinal: ?*const fn (
        ctx: ?*anyopaque,
        alloc: Allocator,
        doc_id: []const u8,
        generation: ?u64,
    ) anyerror!?doc_set.DocOrdinal = null,
    resolve_doc_set_doc_ids: ?*const fn (
        ctx: ?*anyopaque,
        alloc: Allocator,
        set: *const doc_set.ResolvedDocSet,
        generation: ?u64,
    ) anyerror!?[]const []const u8 = null,
    resolve_doc_ids_to_doc_set: ?*const fn (
        ctx: ?*anyopaque,
        alloc: Allocator,
        doc_ids: []const []const u8,
        generation: ?u64,
    ) anyerror!doc_set.ResolvedDocSet = null,
    live_filter_doc_set: ?*const fn (
        ctx: ?*anyopaque,
        alloc: Allocator,
        set: *const doc_set.ResolvedDocSet,
        generation: ?u64,
    ) anyerror!doc_set.ResolvedDocSet = null,
    load_projected_document: *const fn (
        ctx: ?*anyopaque,
        alloc: Allocator,
        req: types.SearchRequest,
        key: []const u8,
    ) anyerror![]u8,
    hbc_search: *const fn (
        ctx: ?*anyopaque,
        entry: *index_manager_mod.IndexManager.DenseIndex,
        req: vectorindex_mod.SearchRequest,
    ) anyerror!vectorindex_mod.SearchResults,
    hbc_search_profiled: *const fn (
        ctx: ?*anyopaque,
        entry: *index_manager_mod.IndexManager.DenseIndex,
        req: vectorindex_mod.SearchRequest,
    ) anyerror!vectorindex_mod.ProfiledSearchResults,
    postprocess: *const fn (
        ctx: ?*anyopaque,
        alloc: Allocator,
        req: types.SearchRequest,
        raw: types.SearchResult,
        chunk_backed: bool,
    ) anyerror!types.SearchResult,
};

pub const DenseSearchProfile = struct {
    pub const DebugHit = struct {
        id: u64 = 0,
        distance: f32 = 0,
        error_bound: f32 = 0,
        lower_bound: f32 = 0,
        upper_bound: f32 = 0,
    };

    pub const DebugPair = struct {
        left: DebugHit = .{},
        right: DebugHit = .{},
        distance_gap: f32 = 0,
        interval_gap: f32 = 0,
        overlaps: bool = false,
    };

    total_ns: u64 = 0,
    index_lookup_ns: u64 = 0,
    hbc_search_ns: u64 = 0,
    hbc_runtime_txn_ns: u64 = 0,
    hbc_scratch_acquire_ns: u64 = 0,
    hbc_node_cache_lookup_ns: u64 = 0,
    hbc_quantized_cache_lookup_ns: u64 = 0,
    resolved_search_width: u32 = 0,
    resolved_epsilon: f32 = 0,
    hbc_nodes_visited: u64 = 0,
    hbc_leaves_explored: u64 = 0,
    hbc_approx_vectors_scored: u64 = 0,
    hbc_exact_vectors_scored: u64 = 0,
    hbc_reranked_vectors: u64 = 0,
    hbc_approx_candidate_count: u64 = 0,
    hbc_rerank_candidate_count: u64 = 0,
    hbc_ambiguous_top_k_pairs: u64 = 0,
    hbc_ambiguous_boundary_pairs: u64 = 0,
    hbc_ambiguous_distance_over_hits: u64 = 0,
    hbc_ambiguous_distance_under_hits: u64 = 0,
    hbc_full_rerank_due_to_threshold: bool = false,
    hbc_top_k_count: u64 = 0,
    hbc_min_distance_gap_top_k: f32 = 0,
    hbc_min_interval_gap_top_k: f32 = 0,
    hbc_closest_pair_top_k: ?DebugPair = null,
    hbc_boundary_pair: ?DebugPair = null,
    hbc_boundary_tail_error_avg: f32 = 0,
    hbc_boundary_tail_error_max: f32 = 0,
    hbc_boundary_tail_distance_gap_avg: f32 = 0,
    hbc_boundary_tail_distance_gap_min: f32 = 0,
    hbc_boundary_tail_distance_gap_max: f32 = 0,
    hbc_boundary_tail_interval_gap_avg: f32 = 0,
    hbc_boundary_tail_interval_gap_min: f32 = 0,
    hbc_boundary_tail_interval_gap_max: f32 = 0,
    hbc_approx_top_count: u64 = 0,
    hbc_approx_top: [5]DebugHit = .{ .{}, .{}, .{}, .{}, .{} },
    hbc_rerank_external_score_ns: u64 = 0,
    hbc_rerank_vector_load_ns: u64 = 0,
    hbc_rerank_metadata_lookup_ns: u64 = 0,
    hbc_rerank_artifact_key_ns: u64 = 0,
    hbc_rerank_artifact_read_ns: u64 = 0,
    hbc_rerank_artifact_decode_ns: u64 = 0,
    hbc_rerank_artifact_distance_ns: u64 = 0,
    hbc_rerank_lsm_cache_hits: u64 = 0,
    hbc_rerank_lsm_cache_misses: u64 = 0,
    hbc_rerank_distance_ns: u64 = 0,
    doc_key_resolve_ns: u64 = 0,
    load_projected_document_ns: u64 = 0,
    postprocess_ns: u64 = 0,
    raw_hit_count: u32 = 0,
    returned_hit_count: u32 = 0,
    inline_metadata_hits: u32 = 0,
    fetched_metadata_hits: u32 = 0,
    lookup_doc_key_hits: u32 = 0,
};

pub const ProfiledDenseSearchResult = struct {
    result: types.SearchResult,
    profile: DenseSearchProfile,
};

pub const SparseSearchExecutor = struct {
    ctx: ?*anyopaque,
    text_index_entry: *const fn (
        ctx: ?*anyopaque,
        index_name: ?[]const u8,
    ) anyerror!?*index_manager_mod.IndexManager.TextIndex,
    sparse_index: *const fn (
        ctx: ?*anyopaque,
        index_name: ?[]const u8,
    ) anyerror!?*index_manager_mod.IndexManager.SparseIndex,
    resolve_doc_set_doc_ids: ?*const fn (
        ctx: ?*anyopaque,
        alloc: Allocator,
        set: *const doc_set.ResolvedDocSet,
        generation: ?u64,
    ) anyerror!?[]const []const u8 = null,
    resolve_doc_ids_to_doc_set: ?*const fn (
        ctx: ?*anyopaque,
        alloc: Allocator,
        doc_ids: []const []const u8,
        generation: ?u64,
    ) anyerror!doc_set.ResolvedDocSet = null,
    live_filter_doc_set: ?*const fn (
        ctx: ?*anyopaque,
        alloc: Allocator,
        set: *const doc_set.ResolvedDocSet,
        generation: ?u64,
    ) anyerror!doc_set.ResolvedDocSet = null,
    lookup_doc_nums_for_ordinals: ?*const fn (
        ctx: ?*anyopaque,
        alloc: Allocator,
        index_name: []const u8,
        ordinals: []const u32,
    ) anyerror![]const u32 = null,
    lookup_doc_ordinal: ?*const fn (
        ctx: ?*anyopaque,
        alloc: Allocator,
        doc_id: []const u8,
        generation: ?u64,
    ) anyerror!?doc_set.DocOrdinal = null,
    load_projected_document: *const fn (
        ctx: ?*anyopaque,
        alloc: Allocator,
        req: types.SearchRequest,
        key: []const u8,
    ) anyerror![]u8,
    postprocess: *const fn (
        ctx: ?*anyopaque,
        alloc: Allocator,
        req: types.SearchRequest,
        raw: types.SearchResult,
        chunk_backed: bool,
    ) anyerror!types.SearchResult,
};

pub const MatchAllCandidate = struct {
    id: []u8,
    ordinal: ?doc_set.DocOrdinal = null,

    pub fn deinit(self: *MatchAllCandidate, alloc: Allocator) void {
        if (self.id.len > 0) alloc.free(self.id);
        self.* = undefined;
    }
};

pub const MatchAllCandidates = struct {
    items: []MatchAllCandidate,

    pub fn deinit(self: *MatchAllCandidates, alloc: Allocator) void {
        for (self.items) |*item| item.deinit(alloc);
        alloc.free(self.items);
        self.* = undefined;
    }
};

pub const MatchAllExecutor = struct {
    ctx: ?*anyopaque,
    collect_candidates: *const fn (
        ctx: ?*anyopaque,
        alloc: Allocator,
        req: types.SearchRequest,
    ) anyerror!MatchAllCandidates,
    text_index_entry: *const fn (
        ctx: ?*anyopaque,
        index_name: ?[]const u8,
    ) anyerror!?*index_manager_mod.IndexManager.TextIndex,
    resolve_doc_set_doc_ids: ?*const fn (
        ctx: ?*anyopaque,
        alloc: Allocator,
        set: *const doc_set.ResolvedDocSet,
        generation: ?u64,
    ) anyerror!?[]const []const u8 = null,
    resolve_doc_ids_to_doc_set: ?*const fn (
        ctx: ?*anyopaque,
        alloc: Allocator,
        doc_ids: []const []const u8,
        generation: ?u64,
    ) anyerror!doc_set.ResolvedDocSet = null,
    live_filter_doc_set: ?*const fn (
        ctx: ?*anyopaque,
        alloc: Allocator,
        set: *const doc_set.ResolvedDocSet,
        generation: ?u64,
    ) anyerror!doc_set.ResolvedDocSet = null,
    load_projected_document: *const fn (
        ctx: ?*anyopaque,
        alloc: Allocator,
        req: types.SearchRequest,
        key: []const u8,
    ) anyerror![]u8,
};

pub const MatchAllCandidateCollector = struct {
    ctx: ?*anyopaque,
    scan_store_range: *const fn (
        ctx: ?*anyopaque,
        alloc: Allocator,
        lower: []const u8,
        upper: []const u8,
    ) anyerror![]docstore_mod.OwnedKVPair,
    is_expired_key: *const fn (
        ctx: ?*anyopaque,
        alloc: Allocator,
        key: []const u8,
    ) anyerror!bool,
    lookup_doc_ordinal: ?*const fn (
        ctx: ?*anyopaque,
        alloc: Allocator,
        key: []const u8,
        generation: ?u64,
    ) anyerror!?doc_set.DocOrdinal = null,
};

pub const ComposedSearchExecutor = struct {
    ctx: ?*anyopaque,
    search_text_query: *const fn (
        ctx: ?*anyopaque,
        alloc: Allocator,
        req: types.SearchRequest,
        text_query: types.TextQuery,
    ) anyerror!types.SearchResult,
    search_text: *const fn (
        ctx: ?*anyopaque,
        alloc: Allocator,
        req: types.SearchRequest,
    ) anyerror!types.SearchResult,
    search_dense: *const fn (
        ctx: ?*anyopaque,
        alloc: Allocator,
        req: types.SearchRequest,
        dense: types.DenseKnnQuery,
    ) anyerror!types.SearchResult,
    search_sparse: *const fn (
        ctx: ?*anyopaque,
        alloc: Allocator,
        req: types.SearchRequest,
        sparse: types.SparseKnnQuery,
    ) anyerror!types.SearchResult,
    clone_named_set: *const fn (
        ctx: ?*anyopaque,
        alloc: Allocator,
        set: graph_exec.NamedResultSet,
        include_stored: bool,
    ) anyerror!types.SearchResult,
    fuse_named_sets: *const fn (
        ctx: ?*anyopaque,
        alloc: Allocator,
        req: types.SearchRequest,
        named_sets: []const graph_exec.NamedResultSet,
    ) anyerror!types.SearchResult,
    resolve_hits_to_doc_set: ?*const fn (
        ctx: ?*anyopaque,
        alloc: Allocator,
        req: types.SearchRequest,
        hits: []const types.SearchHit,
    ) anyerror!doc_set.ResolvedDocSet = null,
    attach_graph_results: *const fn (
        ctx: ?*anyopaque,
        alloc: Allocator,
        req: types.SearchRequest,
        base: *types.SearchResult,
        named_sets: []const graph_exec.NamedResultSet,
    ) anyerror!void,
};

pub fn searchLookupOptions(req: types.SearchRequest) types.LookupOptions {
    return .{
        .fields = req.fields,
        .include_all_fields = req.include_all_fields,
    };
}

pub fn preflightRuntimeAlloc(
    alloc: Allocator,
    runtime: RuntimePreflight,
) !RuntimePreflightSummary {
    var result_refs = std.ArrayListUnmanaged([]const u8).empty;
    errdefer freeOwnedStringItems(alloc, result_refs.items);
    errdefer result_refs.deinit(alloc);

    if (runtime.has_full_text_results) {
        try appendUniqueOwnedString(alloc, &result_refs, "$full_text_results");
    }
    for (runtime.embedding_result_names) |name| {
        try appendUniqueOwnedString(alloc, &result_refs, name);
    }
    if (runtime.embedding_result_names.len > 0) {
        try appendUniqueOwnedString(alloc, &result_refs, "$embeddings_results");
    }
    if (runtime.has_full_text_results and runtime.embedding_result_names.len > 0) {
        try appendUniqueOwnedString(alloc, &result_refs, "$fused_results");
    }

    const sorted_query_indexes = try graph_exec.sortGraphQueriesByDependencies(alloc, runtime.graph_queries);
    defer alloc.free(sorted_query_indexes);

    var graph_query_order = std.ArrayListUnmanaged([]const u8).empty;
    errdefer freeOwnedStringItems(alloc, graph_query_order.items);
    errdefer graph_query_order.deinit(alloc);
    for (sorted_query_indexes) |query_index| {
        const graph_query = runtime.graph_queries[query_index];
        try graph_query_order.append(alloc, try alloc.dupe(u8, graph_query.name));
        const graph_ref = try std.fmt.allocPrint(alloc, "$graph_results.{s}", .{graph_query.name});
        errdefer alloc.free(graph_ref);
        try appendUniqueOwnedString(alloc, &result_refs, graph_ref);
        alloc.free(graph_ref);
    }

    var summary: RuntimePreflightSummary = .{
        .result_refs = if (result_refs.items.len == 0) &.{} else try result_refs.toOwnedSlice(alloc),
        .graph_query_order = if (graph_query_order.items.len == 0) &.{} else try graph_query_order.toOwnedSlice(alloc),
    };
    deriveEstimateFields(&summary);
    return summary;
}

pub fn preflightSearchRequestAlloc(
    alloc: Allocator,
    req: types.SearchRequest,
) !RuntimePreflightSummary {
    var embedding_result_names = std.ArrayListUnmanaged([]const u8).empty;
    defer {
        freeOwnedStringItems(alloc, embedding_result_names.items);
        embedding_result_names.deinit(alloc);
    }

    if (req.dense_queries.len > 0 or req.sparse_queries.len > 0) {
        for (req.dense_queries) |dense_query| try appendUniqueOwnedString(alloc, &embedding_result_names, dense_query.name);
        for (req.sparse_queries) |sparse_query| try appendUniqueOwnedString(alloc, &embedding_result_names, sparse_query.name);
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

pub fn emptySearchResult(alloc: Allocator) !types.SearchResult {
    return .{
        .alloc = alloc,
        .hits = try alloc.alloc(types.SearchHit, 0),
        .total_hits = 0,
        .graph_results = &.{},
    };
}

fn appendUniqueOwnedString(
    alloc: Allocator,
    values: *std.ArrayListUnmanaged([]const u8),
    value: []const u8,
) !void {
    for (values.items) |existing| {
        if (std.mem.eql(u8, existing, value)) return;
    }
    try values.append(alloc, try alloc.dupe(u8, value));
}

fn freeOwnedStringItems(alloc: Allocator, values: []const []const u8) void {
    for (values) |value| alloc.free(@constCast(value));
}

fn freeOwnedStringSlice(alloc: Allocator, values: []const []const u8) void {
    freeOwnedStringItems(alloc, values);
    if (values.len > 0) alloc.free(@constCast(values));
}

pub fn isTextQuery(query: types.Query) bool {
    return switch (query) {
        .match_none, .match_all, .phrase, .multi_phrase, .term, .fuzzy, .numeric_range, .date_range, .doc_id, .bool_field, .geo_distance, .geo_bbox, .term_range, .ip_range, .geo_shape, .match, .match_phrase, .prefix, .wildcard, .regexp => true,
        else => false,
    };
}

pub fn searchComposed(
    alloc: Allocator,
    req: types.SearchRequest,
    executor: ComposedSearchExecutor,
) !types.SearchResult {
    var named_sets = std.ArrayListUnmanaged(graph_exec.NamedResultSet).empty;
    defer named_sets.deinit(alloc);
    var owned_results = std.ArrayListUnmanaged(types.SearchResult).empty;
    defer {
        for (owned_results.items) |*item| item.deinit();
        owned_results.deinit(alloc);
    }
    var owned_resolved_sets = std.ArrayListUnmanaged(*doc_set.ResolvedDocSet).empty;
    defer {
        for (owned_resolved_sets.items) |set| {
            set.deinit(alloc);
            alloc.destroy(set);
        }
        owned_resolved_sets.deinit(alloc);
    }

    if (req.full_text_queries.len == 0) {
        if (req.full_text) |text| {
            const text_result = try executor.search_text_query(executor.ctx, alloc, req, text);
            const resolved_doc_set = try resolveComposedHitsToDocSet(alloc, req, executor, &owned_resolved_sets, text_result.hits);
            try named_sets.append(alloc, .{
                .name = "$full_text_results",
                .hits = text_result.hits,
                .total_hits = text_result.total_hits,
                .resolved_doc_set = resolved_doc_set,
            });
            try owned_results.append(alloc, text_result);
        } else if (!isDefaultMatchAll(req.query) and isTextQuery(req.query)) {
            const text_result = try executor.search_text(executor.ctx, alloc, req);
            const resolved_doc_set = try resolveComposedHitsToDocSet(alloc, req, executor, &owned_resolved_sets, text_result.hits);
            try named_sets.append(alloc, .{
                .name = "$full_text_results",
                .hits = text_result.hits,
                .total_hits = text_result.total_hits,
                .resolved_doc_set = resolved_doc_set,
            });
            try owned_results.append(alloc, text_result);
        }
    } else {
        for (req.full_text_queries) |full_text_query| {
            var text_req = req;
            text_req.index_name = full_text_query.index_name;
            const text_result = try executor.search_text_query(executor.ctx, alloc, text_req, full_text_query.query);
            const resolved_doc_set = try resolveComposedHitsToDocSet(alloc, req, executor, &owned_resolved_sets, text_result.hits);
            try named_sets.append(alloc, .{
                .name = full_text_query.name,
                .hits = text_result.hits,
                .total_hits = text_result.total_hits,
                .resolved_doc_set = resolved_doc_set,
            });
            try owned_results.append(alloc, text_result);
        }
    }

    if (req.dense_queries.len == 0 and req.dense != null) {
        const dense_result = try executor.search_dense(executor.ctx, alloc, req, req.dense.?);
        const resolved_doc_set = try resolveComposedHitsToDocSet(alloc, req, executor, &owned_resolved_sets, dense_result.hits);
        try named_sets.append(alloc, .{
            .name = if (req.sparse == null) "$embeddings_results" else "dense",
            .hits = dense_result.hits,
            .total_hits = dense_result.total_hits,
            .resolved_doc_set = resolved_doc_set,
        });
        try owned_results.append(alloc, dense_result);
    } else {
        for (req.dense_queries) |dense_query| {
            var dense_req = req;
            dense_req.index_name = dense_query.index_name;
            const dense_result = try executor.search_dense(executor.ctx, alloc, dense_req, dense_query.query);
            const resolved_doc_set = try resolveComposedHitsToDocSet(alloc, req, executor, &owned_resolved_sets, dense_result.hits);
            try named_sets.append(alloc, .{
                .name = dense_query.name,
                .hits = dense_result.hits,
                .total_hits = dense_result.total_hits,
                .resolved_doc_set = resolved_doc_set,
            });
            try owned_results.append(alloc, dense_result);
        }
    }

    if (req.sparse_queries.len == 0 and req.sparse != null) {
        const sparse_result = try executor.search_sparse(executor.ctx, alloc, req, req.sparse.?);
        const resolved_doc_set = try resolveComposedHitsToDocSet(alloc, req, executor, &owned_resolved_sets, sparse_result.hits);
        try named_sets.append(alloc, .{
            .name = if (req.dense == null) "$embeddings_results" else "sparse",
            .hits = sparse_result.hits,
            .total_hits = sparse_result.total_hits,
            .resolved_doc_set = resolved_doc_set,
        });
        try owned_results.append(alloc, sparse_result);
    } else {
        for (req.sparse_queries) |sparse_query| {
            var sparse_req = req;
            sparse_req.index_name = sparse_query.index_name;
            const sparse_result = try executor.search_sparse(executor.ctx, alloc, sparse_req, sparse_query.query);
            const resolved_doc_set = try resolveComposedHitsToDocSet(alloc, req, executor, &owned_resolved_sets, sparse_result.hits);
            try named_sets.append(alloc, .{
                .name = sparse_query.name,
                .hits = sparse_result.hits,
                .total_hits = sparse_result.total_hits,
                .resolved_doc_set = resolved_doc_set,
            });
            try owned_results.append(alloc, sparse_result);
        }
    }

    var base = if (named_sets.items.len == 0)
        try emptySearchResult(alloc)
    else if (named_sets.items.len == 1 and req.merge_config == null)
        try executor.clone_named_set(executor.ctx, alloc, named_sets.items[0], req.include_stored)
    else
        try executor.fuse_named_sets(executor.ctx, alloc, req, named_sets.items);
    errdefer base.deinit();

    const fused_resolved_doc_set = try resolveComposedHitsToDocSet(alloc, req, executor, &owned_resolved_sets, base.hits);
    try named_sets.append(alloc, .{
        .name = "$fused_results",
        .hits = base.hits,
        .total_hits = base.total_hits,
        .resolved_doc_set = fused_resolved_doc_set,
    });

    try appendEmbeddingsResultAlias(alloc, req, executor, &named_sets, &owned_results, &owned_resolved_sets);

    if (req.graph_queries.len > 0) {
        try executor.attach_graph_results(executor.ctx, alloc, req, &base, named_sets.items);
    }
    return base;
}

fn resolveComposedHitsToDocSet(
    alloc: Allocator,
    req: types.SearchRequest,
    executor: ComposedSearchExecutor,
    owned_resolved_sets: *std.ArrayListUnmanaged(*doc_set.ResolvedDocSet),
    hits: []const types.SearchHit,
) !?*const doc_set.ResolvedDocSet {
    if (req.graph_queries.len == 0) return null;
    const resolve = executor.resolve_hits_to_doc_set orelse return null;
    const set = try alloc.create(doc_set.ResolvedDocSet);
    errdefer alloc.destroy(set);
    set.* = try resolve(executor.ctx, alloc, req, hits);
    errdefer set.deinit(alloc);
    try owned_resolved_sets.append(alloc, set);
    return set;
}

fn appendEmbeddingsResultAlias(
    alloc: Allocator,
    req: types.SearchRequest,
    executor: ComposedSearchExecutor,
    named_sets: *std.ArrayListUnmanaged(graph_exec.NamedResultSet),
    owned_results: *std.ArrayListUnmanaged(types.SearchResult),
    owned_resolved_sets: *std.ArrayListUnmanaged(*doc_set.ResolvedDocSet),
) !void {
    if (findComposedNamedSet(named_sets.items, "$embeddings_results") != null) return;

    var embedding_sets = std.ArrayListUnmanaged(graph_exec.NamedResultSet).empty;
    defer embedding_sets.deinit(alloc);
    if (req.dense_queries.len == 0 and req.dense != null) try appendComposedNamedSetIfPresent(alloc, &embedding_sets, named_sets.items, "dense");
    if (req.sparse_queries.len == 0 and req.sparse != null) try appendComposedNamedSetIfPresent(alloc, &embedding_sets, named_sets.items, "sparse");
    for (req.dense_queries) |dense_query| try appendComposedNamedSetIfPresent(alloc, &embedding_sets, named_sets.items, dense_query.name);
    for (req.sparse_queries) |sparse_query| try appendComposedNamedSetIfPresent(alloc, &embedding_sets, named_sets.items, sparse_query.name);

    if (embedding_sets.items.len == 0) return;
    if (embedding_sets.items.len == 1) {
        try named_sets.append(alloc, .{
            .name = "$embeddings_results",
            .hits = embedding_sets.items[0].hits,
            .total_hits = embedding_sets.items[0].total_hits,
            .resolved_doc_set = embedding_sets.items[0].resolved_doc_set,
        });
        return;
    }

    var embeddings_result = try executor.fuse_named_sets(executor.ctx, alloc, req, embedding_sets.items);
    errdefer embeddings_result.deinit();
    const resolved_doc_set = try resolveComposedHitsToDocSet(alloc, req, executor, owned_resolved_sets, embeddings_result.hits);
    try named_sets.append(alloc, .{
        .name = "$embeddings_results",
        .hits = embeddings_result.hits,
        .total_hits = embeddings_result.total_hits,
        .resolved_doc_set = resolved_doc_set,
    });
    try owned_results.append(alloc, embeddings_result);
}

fn appendComposedNamedSetIfPresent(
    alloc: Allocator,
    out: *std.ArrayListUnmanaged(graph_exec.NamedResultSet),
    named_sets: []const graph_exec.NamedResultSet,
    name: []const u8,
) !void {
    if (findComposedNamedSet(named_sets, name)) |set| try out.append(alloc, set);
}

fn findComposedNamedSet(named_sets: []const graph_exec.NamedResultSet, name: []const u8) ?graph_exec.NamedResultSet {
    for (named_sets) |set| {
        if (std.mem.eql(u8, set.name, name)) return set;
    }
    return null;
}

pub fn isDefaultMatchAll(query: types.Query) bool {
    return switch (query) {
        .match_all => true,
        else => false,
    };
}

fn hasSearchRequestFullTextResults(req: types.SearchRequest) bool {
    if (req.full_text != null) return true;
    if (req.full_text_queries.len > 0) return true;
    if (req.filter_query_json.len > 0 or req.exclusion_query_json.len > 0) return true;
    return !isDefaultMatchAll(req.query) and isTextQuery(req.query);
}

const ComponentPaging = struct {
    offset: u32,
    limit: u32,
};

fn componentPaging(req: types.SearchRequest) ComponentPaging {
    var limit = req.limit +| req.offset;
    const needs_component_window =
        req.merge_config != null or
        req.pruner != null or
        req.reranker != null;

    if (!needs_component_window) {
        return .{
            .offset = req.offset,
            .limit = req.limit,
        };
    }

    if (req.merge_config) |merge_config| {
        if (merge_config.window_size > limit) limit = merge_config.window_size;
    }
    if (req.reranker) |reranker| {
        if (reranker.top_n) |top_n| {
            if (top_n > limit) limit = top_n;
        }
    }

    return .{
        .offset = 0,
        .limit = limit,
    };
}

fn hasStoredPatternFilters(req: types.SearchRequest) bool {
    return req.filter_query_json.len > 0 or req.exclusion_query_json.len > 0;
}

const NativeDenseConstraints = struct {
    positive_filter: bool = false,
    filter_ids: []const u64 = &.{},
    filter_ids_owned: bool = false,
    exclude_ids: []const u64 = &.{},
    exclude_ids_owned: bool = false,
    resolved_stored_filters: bool = false,

    fn deinit(self: *NativeDenseConstraints, alloc: Allocator) void {
        if (self.filter_ids_owned and self.filter_ids.len > 0) alloc.free(@constCast(self.filter_ids));
        if (self.exclude_ids_owned and self.exclude_ids.len > 0) alloc.free(@constCast(self.exclude_ids));
        self.* = undefined;
    }
};

const NativeDocIdConstraints = struct {
    positive_filter: bool = false,
    filter_doc_ids: []const []const u8 = &.{},
    exclude_doc_ids: []const []const u8 = &.{},
    filter_doc_nums: []const u32 = &.{},
    exclude_doc_nums: []const u32 = &.{},
    filter_doc_ids_owned: bool = false,
    exclude_doc_ids_owned: bool = false,
    filter_doc_nums_owned: bool = false,
    exclude_doc_nums_owned: bool = false,
    resolved_stored_filters: bool = false,

    fn deinit(self: *NativeDocIdConstraints, alloc: Allocator) void {
        if (self.filter_doc_ids_owned) freeDocIdSlice(alloc, self.filter_doc_ids);
        if (self.exclude_doc_ids_owned) freeDocIdSlice(alloc, self.exclude_doc_ids);
        if (self.filter_doc_nums_owned and self.filter_doc_nums.len > 0) alloc.free(@constCast(self.filter_doc_nums));
        if (self.exclude_doc_nums_owned and self.exclude_doc_nums.len > 0) alloc.free(@constCast(self.exclude_doc_nums));
        self.* = undefined;
    }
};

const StructuredFilterResolverExecutor = struct {
    ctx: ?*anyopaque,
    text_index_entry: *const fn (
        ctx: ?*anyopaque,
        index_name: ?[]const u8,
    ) anyerror!?*index_manager_mod.IndexManager.TextIndex,
    resolve_doc_set_doc_ids: ?*const fn (
        ctx: ?*anyopaque,
        alloc: Allocator,
        set: *const doc_set.ResolvedDocSet,
        generation: ?u64,
    ) anyerror!?[]const []const u8 = null,
    resolve_doc_ids_to_doc_set: ?*const fn (
        ctx: ?*anyopaque,
        alloc: Allocator,
        doc_ids: []const []const u8,
        generation: ?u64,
    ) anyerror!doc_set.ResolvedDocSet = null,
    live_filter_doc_set: ?*const fn (
        ctx: ?*anyopaque,
        alloc: Allocator,
        set: *const doc_set.ResolvedDocSet,
        generation: ?u64,
    ) anyerror!doc_set.ResolvedDocSet = null,
    lookup_doc_nums_for_ordinals: ?*const fn (
        ctx: ?*anyopaque,
        alloc: Allocator,
        index_name: []const u8,
        ordinals: []const u32,
    ) anyerror![]const u32 = null,
    doc_num_index_name: ?[]const u8 = null,
    require_doc_num_projection_mapper: bool = false,
    project_ordinals_to_doc_ids: bool = true,
    text_snapshot_for_doc_num_projection: ?*const index_mod.IndexSnapshot = null,
    apply_live_all_docs: bool = false,
    identity_read_generation: ?u64 = null,
};

const StructuredFilterDocSetCache = struct {
    const Entry = struct {
        filter_query_json: []u8,
        identity_namespace_tag: ?u64,
        identity_read_generation: ?u64,
        set: doc_set.ResolvedDocSet,
    };

    entries: std.ArrayListUnmanaged(Entry) = .empty,

    fn deinit(self: *StructuredFilterDocSetCache, alloc: Allocator) void {
        for (self.entries.items) |*entry| {
            alloc.free(entry.filter_query_json);
            entry.set.deinit(alloc);
        }
        self.entries.deinit(alloc);
        self.* = .{};
    }

    fn get(
        self: *const StructuredFilterDocSetCache,
        filter_query_json: []const u8,
        identity_read_generation: ?u64,
    ) ?*const doc_set.ResolvedDocSet {
        return self.getWithNamespace(filter_query_json, null, identity_read_generation);
    }

    fn getShared(
        self: *const StructuredFilterDocSetCache,
        filter_query_json: []const u8,
        identity_namespace_tag: u64,
        identity_read_generation: u64,
    ) ?*const doc_set.ResolvedDocSet {
        return self.getWithNamespace(filter_query_json, identity_namespace_tag, identity_read_generation);
    }

    fn getWithNamespace(
        self: *const StructuredFilterDocSetCache,
        filter_query_json: []const u8,
        identity_namespace_tag: ?u64,
        identity_read_generation: ?u64,
    ) ?*const doc_set.ResolvedDocSet {
        for (self.entries.items) |*entry| {
            if (entry.identity_namespace_tag == identity_namespace_tag and
                entry.identity_read_generation == identity_read_generation and
                std.mem.eql(u8, entry.filter_query_json, filter_query_json))
            {
                return &entry.set;
            }
        }
        return null;
    }

    fn putCloneAlloc(
        self: *StructuredFilterDocSetCache,
        alloc: Allocator,
        filter_query_json: []const u8,
        identity_read_generation: ?u64,
        set: *const doc_set.ResolvedDocSet,
    ) !void {
        return try self.putCloneWithNamespaceAlloc(alloc, filter_query_json, null, identity_read_generation, set);
    }

    fn putSharedCloneAlloc(
        self: *StructuredFilterDocSetCache,
        alloc: Allocator,
        filter_query_json: []const u8,
        identity_namespace_tag: u64,
        identity_read_generation: u64,
        set: *const doc_set.ResolvedDocSet,
    ) !void {
        return try self.putCloneWithNamespaceAlloc(alloc, filter_query_json, identity_namespace_tag, identity_read_generation, set);
    }

    fn putCloneWithNamespaceAlloc(
        self: *StructuredFilterDocSetCache,
        alloc: Allocator,
        filter_query_json: []const u8,
        identity_namespace_tag: ?u64,
        identity_read_generation: ?u64,
        set: *const doc_set.ResolvedDocSet,
    ) !void {
        const owned_query = try alloc.dupe(u8, filter_query_json);
        errdefer alloc.free(owned_query);
        var owned_set = try doc_set.cloneAlloc(alloc, set);
        errdefer owned_set.deinit(alloc);
        try self.entries.append(alloc, .{
            .filter_query_json = owned_query,
            .identity_namespace_tag = identity_namespace_tag,
            .identity_read_generation = identity_read_generation,
            .set = owned_set,
        });
        owned_set = .none;
    }
};

fn deriveNativeDocIdConstraintsArena(
    alloc: Allocator,
    req: types.SearchRequest,
) !NativeDocIdConstraints {
    var out = NativeDocIdConstraints{};

    if (req.filter_query_json.len > 0) {
        const parsed = try std.json.parseFromSlice(std.json.Value, alloc, req.filter_query_json, .{});
        if (try compilePatternFilterOptional(alloc, parsed.value)) |compiled| {
            var doc_ids = std.ArrayListUnmanaged([]const u8).empty;
            if (try collectPositiveDocIdSuperset(alloc, compiled, &doc_ids)) {
                out.filter_doc_ids = try doc_ids.toOwnedSlice(alloc);
                out.positive_filter = true;
            }

            var excluded_doc_ids = std.ArrayListUnmanaged([]const u8).empty;
            try collectBoolMustNotExactDocIds(alloc, compiled, &excluded_doc_ids);
            if (excluded_doc_ids.items.len > 0) out.exclude_doc_ids = try excluded_doc_ids.toOwnedSlice(alloc);
        }
    }

    if (req.exclusion_query_json.len > 0) {
        const parsed = try std.json.parseFromSlice(std.json.Value, alloc, req.exclusion_query_json, .{});
        if (try compilePatternFilterOptional(alloc, parsed.value)) |compiled| {
            var doc_ids = std.ArrayListUnmanaged([]const u8).empty;
            if (try collectExactDocIds(alloc, compiled, &doc_ids)) {
                const exclusion_doc_ids = try doc_ids.toOwnedSlice(alloc);
                out.exclude_doc_ids = if (out.exclude_doc_ids.len > 0)
                    try unionDocIdsArena(alloc, out.exclude_doc_ids, exclusion_doc_ids)
                else
                    exclusion_doc_ids;
            }
        }
    }

    return out;
}

fn compilePatternFilterOptional(alloc: Allocator, value: std.json.Value) !?graph_exec.CompiledPatternFilter {
    return graph_exec.compilePatternFilter(alloc, value) catch |err| switch (err) {
        error.InvalidArgument => null,
        else => return err,
    };
}

fn unionDocIdsArena(
    alloc: Allocator,
    left: []const []const u8,
    right: []const []const u8,
) ![]const []const u8 {
    var out = std.ArrayListUnmanaged([]const u8).empty;
    for (left) |id| try appendDocIds(alloc, &out, &.{id});
    for (right) |id| try appendDocIds(alloc, &out, &.{id});
    return try out.toOwnedSlice(alloc);
}

fn deriveNativeDocIdConstraintsAlloc(
    alloc: Allocator,
    req: types.SearchRequest,
    executor: StructuredFilterResolverExecutor,
) !NativeDocIdConstraints {
    var active_executor = executor;
    if (active_executor.identity_read_generation == null) active_executor.identity_read_generation = req.identity_read_generation;
    var out = NativeDocIdConstraints{};
    errdefer out.deinit(alloc);
    var structured_filter_doc_sets = StructuredFilterDocSetCache{};
    defer structured_filter_doc_sets.deinit(alloc);

    if (active_executor.apply_live_all_docs) {
        try applyLiveAllDocFilterToNativeConstraintsAlloc(alloc, &out, active_executor);
    }

    if (resolvedDocFilterFromRequest(req)) |filter| {
        try applyResolvedDocFilterToNativeConstraintsAlloc(alloc, &out, filter, active_executor);
    }

    const resolved_request_doc_ids = !active_executor.project_ordinals_to_doc_ids and active_executor.resolve_doc_ids_to_doc_set != null;
    if (req.filter_doc_ids_positive or req.filter_doc_ids.len > 0) {
        if (resolved_request_doc_ids) {
            var resolved = try active_executor.resolve_doc_ids_to_doc_set.?(
                active_executor.ctx,
                alloc,
                req.filter_doc_ids,
                active_executor.identity_read_generation,
            );
            defer resolved.deinit(alloc);
            const filter = doc_set.ResolvedDocFilter{
                .include = resolved,
                .exclude = .none,
            };
            try applyResolvedDocFilterToNativeConstraintsAlloc(alloc, &out, &filter, active_executor);
        } else {
            if (out.positive_filter) {
                const intersected = try intersectDocIdsAlloc(alloc, out.filter_doc_ids, req.filter_doc_ids);
                if (out.filter_doc_ids_owned) freeDocIdSlice(alloc, out.filter_doc_ids);
                out.filter_doc_ids = intersected;
                out.filter_doc_ids_owned = true;
            } else {
                out.filter_doc_ids = req.filter_doc_ids;
            }
            out.positive_filter = true;
        }
        out.resolved_stored_filters = true;
    }
    if (req.exclude_doc_ids.len > 0) {
        if (resolved_request_doc_ids) {
            var resolved = try active_executor.resolve_doc_ids_to_doc_set.?(
                active_executor.ctx,
                alloc,
                req.exclude_doc_ids,
                active_executor.identity_read_generation,
            );
            defer resolved.deinit(alloc);
            const filter = doc_set.ResolvedDocFilter{
                .include = .all,
                .exclude = resolved,
            };
            try applyResolvedDocFilterToNativeConstraintsAlloc(alloc, &out, &filter, active_executor);
        } else {
            if (out.exclude_doc_ids.len > 0) {
                const merged = try unionDocIdsAlloc(alloc, out.exclude_doc_ids, req.exclude_doc_ids);
                if (out.exclude_doc_ids_owned) freeDocIdSlice(alloc, out.exclude_doc_ids);
                out.exclude_doc_ids = merged;
                out.exclude_doc_ids_owned = true;
            } else {
                out.exclude_doc_ids = req.exclude_doc_ids;
            }
        }
        out.resolved_stored_filters = true;
    }

    if (req.filter_query_json.len > 0) {
        if (try collectStructuredFilterResolvedDocSetCachedAlloc(alloc, req, active_executor, &structured_filter_doc_sets, req.filter_query_json)) |resolved| {
            var owned_resolved = resolved;
            defer owned_resolved.deinit(alloc);
            const filter = doc_set.ResolvedDocFilter{
                .include = owned_resolved,
                .exclude = .none,
            };
            try applyResolvedDocFilterToNativeConstraintsAlloc(alloc, &out, &filter, active_executor);
        } else if (try collectStructuredFilterDocIdsAlloc(alloc, req, active_executor, req.filter_query_json)) |doc_ids| {
            if (out.positive_filter) {
                const intersected = try intersectDocIdsAlloc(alloc, out.filter_doc_ids, doc_ids);
                if (out.filter_doc_ids_owned) freeDocIdSlice(alloc, out.filter_doc_ids);
                freeDocIdSlice(alloc, doc_ids);
                out.filter_doc_ids = intersected;
            } else {
                out.filter_doc_ids = doc_ids;
            }
            out.filter_doc_ids_owned = true;
            out.positive_filter = true;
            out.resolved_stored_filters = true;
        } else {
            var arena = std.heap.ArenaAllocator.init(alloc);
            defer arena.deinit();
            const arena_alloc = arena.allocator();
            const parsed = try std.json.parseFromSlice(std.json.Value, arena_alloc, req.filter_query_json, .{});
            if (try compilePatternFilterOptional(arena_alloc, parsed.value)) |compiled| {
                var doc_ids = std.ArrayListUnmanaged([]const u8).empty;
                defer doc_ids.deinit(arena_alloc);
                if (try collectPositiveDocIdSuperset(arena_alloc, compiled, &doc_ids)) {
                    const owned_doc_ids = try dupeDocIdSliceAlloc(alloc, doc_ids.items);
                    if (out.positive_filter) {
                        const intersected = try intersectDocIdsAlloc(alloc, out.filter_doc_ids, owned_doc_ids);
                        if (out.filter_doc_ids_owned) freeDocIdSlice(alloc, out.filter_doc_ids);
                        freeDocIdSlice(alloc, owned_doc_ids);
                        out.filter_doc_ids = intersected;
                    } else {
                        out.filter_doc_ids = owned_doc_ids;
                    }
                    out.filter_doc_ids_owned = true;
                    out.positive_filter = true;
                }
            }
        }

        var arena = std.heap.ArenaAllocator.init(alloc);
        defer arena.deinit();
        const arena_alloc = arena.allocator();
        const parsed = try std.json.parseFromSlice(std.json.Value, arena_alloc, req.filter_query_json, .{});
        if (try compilePatternFilterOptional(arena_alloc, parsed.value)) |compiled| {
            var excluded_doc_ids = std.ArrayListUnmanaged([]const u8).empty;
            defer excluded_doc_ids.deinit(arena_alloc);
            try collectBoolMustNotExactDocIds(arena_alloc, compiled, &excluded_doc_ids);
            if (excluded_doc_ids.items.len > 0) {
                const owned_excludes = try dupeDocIdSliceAlloc(alloc, excluded_doc_ids.items);
                if (out.exclude_doc_ids.len > 0) {
                    const merged = try unionDocIdsAlloc(alloc, out.exclude_doc_ids, owned_excludes);
                    if (out.exclude_doc_ids_owned) freeDocIdSlice(alloc, out.exclude_doc_ids);
                    freeDocIdSlice(alloc, owned_excludes);
                    out.exclude_doc_ids = merged;
                } else {
                    out.exclude_doc_ids = owned_excludes;
                }
                out.exclude_doc_ids_owned = true;
            }
        }
    }

    if (req.exclusion_query_json.len > 0) {
        if (try collectStructuredFilterResolvedDocSetCachedAlloc(alloc, req, active_executor, &structured_filter_doc_sets, req.exclusion_query_json)) |resolved| {
            var owned_resolved = resolved;
            defer owned_resolved.deinit(alloc);
            const filter = doc_set.ResolvedDocFilter{
                .include = .all,
                .exclude = owned_resolved,
            };
            try applyResolvedDocFilterToNativeConstraintsAlloc(alloc, &out, &filter, active_executor);
        } else if (try collectStructuredFilterDocIdsAlloc(alloc, req, active_executor, req.exclusion_query_json)) |doc_ids| {
            if (out.exclude_doc_ids.len > 0) {
                const merged = try unionDocIdsAlloc(alloc, out.exclude_doc_ids, doc_ids);
                if (out.exclude_doc_ids_owned) freeDocIdSlice(alloc, out.exclude_doc_ids);
                freeDocIdSlice(alloc, doc_ids);
                out.exclude_doc_ids = merged;
            } else {
                out.exclude_doc_ids = doc_ids;
            }
            out.exclude_doc_ids_owned = true;
            out.resolved_stored_filters = true;
        } else {
            var arena = std.heap.ArenaAllocator.init(alloc);
            defer arena.deinit();
            const arena_alloc = arena.allocator();
            const parsed = try std.json.parseFromSlice(std.json.Value, arena_alloc, req.exclusion_query_json, .{});
            if (try compilePatternFilterOptional(arena_alloc, parsed.value)) |compiled| {
                var doc_ids = std.ArrayListUnmanaged([]const u8).empty;
                defer doc_ids.deinit(arena_alloc);
                if (try collectExactDocIds(arena_alloc, compiled, &doc_ids)) {
                    const owned_excludes = try dupeDocIdSliceAlloc(alloc, doc_ids.items);
                    if (out.exclude_doc_ids.len > 0) {
                        const merged = try unionDocIdsAlloc(alloc, out.exclude_doc_ids, owned_excludes);
                        if (out.exclude_doc_ids_owned) freeDocIdSlice(alloc, out.exclude_doc_ids);
                        freeDocIdSlice(alloc, owned_excludes);
                        out.exclude_doc_ids = merged;
                    } else {
                        out.exclude_doc_ids = owned_excludes;
                    }
                    out.exclude_doc_ids_owned = true;
                }
            }
        }
    }

    return out;
}

fn resolvedDocFilterFromRequest(req: types.SearchRequest) ?*const doc_set.ResolvedDocFilter {
    const ptr = req.resolved_doc_filter orelse return null;
    return @ptrCast(@alignCast(ptr));
}

fn applyResolvedDocFilterAlloc(
    alloc: Allocator,
    out: *NativeDocIdConstraints,
    filter: *const doc_set.ResolvedDocFilter,
    executor: StructuredFilterResolverExecutor,
) !void {
    var live_include: ?doc_set.ResolvedDocSet = null;
    defer if (live_include) |*set| set.deinit(alloc);
    const include_set = try maybeLiveFilterResolvedDocSetAlloc(alloc, &filter.include, executor, &live_include);

    var live_exclude: ?doc_set.ResolvedDocSet = null;
    defer if (live_exclude) |*set| set.deinit(alloc);
    const exclude_set = try maybeLiveFilterResolvedDocSetAlloc(alloc, &filter.exclude, executor, &live_exclude);

    if (exclude_set.* == .all) {
        markNativeDocIdConstraintsEmpty(out, alloc);
        return;
    }

    var include_represented = include_set.* == .all;
    if (executor.project_ordinals_to_doc_ids or resolvedSetHasDocKeys(include_set)) {
        if (try docIdsForResolvedDocSetAlloc(alloc, include_set, executor)) |ids| {
            if (out.positive_filter) {
                const intersected = try intersectDocIdsAlloc(alloc, out.filter_doc_ids, ids);
                if (out.filter_doc_ids_owned) freeDocIdSlice(alloc, out.filter_doc_ids);
                freeDocIdSlice(alloc, ids);
                out.filter_doc_ids = intersected;
            } else {
                out.filter_doc_ids = ids;
            }
            out.filter_doc_ids_owned = true;
            out.positive_filter = true;
            out.resolved_stored_filters = true;
            include_represented = true;
        }
    }
    if (!executor.project_ordinals_to_doc_ids) {
        if (try docNumsForResolvedDocSetWithExecutorAlloc(alloc, include_set, executor)) |doc_nums| {
            if (out.positive_filter and out.filter_doc_nums.len > 0) {
                const intersected = try intersectDocNumsAlloc(alloc, out.filter_doc_nums, doc_nums);
                if (out.filter_doc_nums_owned and out.filter_doc_nums.len > 0) alloc.free(@constCast(out.filter_doc_nums));
                alloc.free(doc_nums);
                out.filter_doc_nums = intersected;
            } else {
                out.filter_doc_nums = doc_nums;
            }
            out.filter_doc_nums_owned = true;
            out.positive_filter = true;
            out.resolved_stored_filters = true;
            include_represented = true;
        }
    }
    if (!include_represented) return error.UnsupportedQueryRequest;

    var exclude_represented = exclude_set.* == .none;
    if (!executor.project_ordinals_to_doc_ids) {
        if (try docNumsForResolvedDocSetWithExecutorAlloc(alloc, exclude_set, executor)) |doc_nums| {
            if (out.exclude_doc_nums.len > 0) {
                const merged = try unionDocNumsAlloc(alloc, out.exclude_doc_nums, doc_nums);
                if (out.exclude_doc_nums_owned and out.exclude_doc_nums.len > 0) alloc.free(@constCast(out.exclude_doc_nums));
                alloc.free(doc_nums);
                out.exclude_doc_nums = merged;
            } else {
                out.exclude_doc_nums = doc_nums;
            }
            out.exclude_doc_nums_owned = true;
            out.resolved_stored_filters = true;
            exclude_represented = true;
        }
    }
    if (executor.project_ordinals_to_doc_ids or resolvedSetHasDocKeys(exclude_set)) {
        if (try docIdsForResolvedDocSetAlloc(alloc, exclude_set, executor)) |ids| {
            if (out.exclude_doc_ids.len > 0) {
                const merged = try unionDocIdsAlloc(alloc, out.exclude_doc_ids, ids);
                if (out.exclude_doc_ids_owned) freeDocIdSlice(alloc, out.exclude_doc_ids);
                freeDocIdSlice(alloc, ids);
                out.exclude_doc_ids = merged;
            } else {
                out.exclude_doc_ids = ids;
            }
            out.exclude_doc_ids_owned = true;
            out.resolved_stored_filters = true;
            exclude_represented = true;
        }
    }
    if (!exclude_represented) return error.UnsupportedQueryRequest;
}

fn applyLiveAllDocFilterToNativeConstraintsAlloc(
    alloc: Allocator,
    out: *NativeDocIdConstraints,
    executor: StructuredFilterResolverExecutor,
) !void {
    const live_filter = executor.live_filter_doc_set orelse return;
    const all: doc_set.ResolvedDocSet = .all;
    var live = try live_filter(executor.ctx, alloc, &all, executor.identity_read_generation);
    defer live.deinit(alloc);
    switch (live) {
        .all => return,
        else => {},
    }
    const filter = doc_set.ResolvedDocFilter{
        .include = try doc_set.cloneAlloc(alloc, &live),
        .exclude = .none,
    };
    var owned_filter = filter;
    defer owned_filter.deinit(alloc);

    const resolved_stored_filters_before_live_filter = out.resolved_stored_filters;
    try applyResolvedDocFilterToNativeConstraintsAlloc(alloc, out, &owned_filter, executor);
    out.resolved_stored_filters = resolved_stored_filters_before_live_filter;
}

fn applyResolvedDocFilterToNativeConstraintsAlloc(
    alloc: Allocator,
    out: *NativeDocIdConstraints,
    filter: *const doc_set.ResolvedDocFilter,
    executor: StructuredFilterResolverExecutor,
) !void {
    if (executor.text_snapshot_for_doc_num_projection) |snapshot| {
        return try applyResolvedDocFilterToTextDocNumsAlloc(alloc, snapshot, out, filter, executor);
    }
    return try applyResolvedDocFilterAlloc(alloc, out, filter, executor);
}

fn markNativeDocIdConstraintsEmpty(out: *NativeDocIdConstraints, alloc: Allocator) void {
    if (out.filter_doc_ids_owned) freeDocIdSlice(alloc, out.filter_doc_ids);
    if (out.exclude_doc_ids_owned) freeDocIdSlice(alloc, out.exclude_doc_ids);
    if (out.filter_doc_nums_owned and out.filter_doc_nums.len > 0) alloc.free(@constCast(out.filter_doc_nums));
    if (out.exclude_doc_nums_owned and out.exclude_doc_nums.len > 0) alloc.free(@constCast(out.exclude_doc_nums));
    out.filter_doc_ids = &.{};
    out.exclude_doc_ids = &.{};
    out.filter_doc_nums = &.{};
    out.exclude_doc_nums = &.{};
    out.filter_doc_ids_owned = false;
    out.exclude_doc_ids_owned = false;
    out.filter_doc_nums_owned = false;
    out.exclude_doc_nums_owned = false;
    out.positive_filter = true;
    out.resolved_stored_filters = true;
}

fn applyResolvedDocFilterToTextDocNumsAlloc(
    alloc: Allocator,
    snapshot: *const index_mod.IndexSnapshot,
    out: *NativeDocIdConstraints,
    filter: *const doc_set.ResolvedDocFilter,
    executor: StructuredFilterResolverExecutor,
) !void {
    var live_include: ?doc_set.ResolvedDocSet = null;
    defer if (live_include) |*set| set.deinit(alloc);
    const include_set = try maybeLiveFilterResolvedDocSetAlloc(alloc, &filter.include, executor, &live_include);

    var live_exclude: ?doc_set.ResolvedDocSet = null;
    defer if (live_exclude) |*set| set.deinit(alloc);
    const exclude_set = try maybeLiveFilterResolvedDocSetAlloc(alloc, &filter.exclude, executor, &live_exclude);

    if (exclude_set.* == .all) {
        markNativeDocIdConstraintsEmpty(out, alloc);
        return;
    }

    const can_project_ordinals = snapshot.hasDocOrdinalCoverage();
    var include_represented = include_set.* == .all;
    if (can_project_ordinals) {
        if (try textDocNumsForResolvedDocSetAlloc(alloc, snapshot, include_set)) |doc_nums| {
            if (out.positive_filter and out.filter_doc_nums.len > 0) {
                const intersected = try intersectDocNumsAlloc(alloc, out.filter_doc_nums, doc_nums);
                if (out.filter_doc_nums_owned and out.filter_doc_nums.len > 0) alloc.free(@constCast(out.filter_doc_nums));
                alloc.free(doc_nums);
                out.filter_doc_nums = intersected;
            } else {
                out.filter_doc_nums = doc_nums;
            }
            out.filter_doc_nums_owned = true;
            out.positive_filter = true;
            out.resolved_stored_filters = true;
            include_represented = true;
        }
    }

    if (!include_represented) {
        if (try docIdsForResolvedDocSetAlloc(alloc, include_set, executor)) |ids| {
            if (out.positive_filter) {
                const intersected = try intersectDocIdsAlloc(alloc, out.filter_doc_ids, ids);
                if (out.filter_doc_ids_owned) freeDocIdSlice(alloc, out.filter_doc_ids);
                freeDocIdSlice(alloc, ids);
                out.filter_doc_ids = intersected;
            } else {
                out.filter_doc_ids = ids;
            }
            out.filter_doc_ids_owned = true;
            out.positive_filter = true;
            out.resolved_stored_filters = true;
            include_represented = true;
        }
    }
    if (!include_represented) return error.UnsupportedQueryRequest;

    var exclude_represented = exclude_set.* == .none;
    if (can_project_ordinals) {
        if (try textDocNumsForResolvedDocSetAlloc(alloc, snapshot, exclude_set)) |doc_nums| {
            if (out.exclude_doc_nums.len > 0) {
                const merged = try unionDocNumsAlloc(alloc, out.exclude_doc_nums, doc_nums);
                if (out.exclude_doc_nums_owned and out.exclude_doc_nums.len > 0) alloc.free(@constCast(out.exclude_doc_nums));
                alloc.free(doc_nums);
                out.exclude_doc_nums = merged;
            } else {
                out.exclude_doc_nums = doc_nums;
            }
            out.exclude_doc_nums_owned = true;
            out.resolved_stored_filters = true;
            exclude_represented = true;
        }
    }

    if (!exclude_represented) {
        if (try docIdsForResolvedDocSetAlloc(alloc, exclude_set, executor)) |ids| {
            if (out.exclude_doc_ids.len > 0) {
                const merged = try unionDocIdsAlloc(alloc, out.exclude_doc_ids, ids);
                if (out.exclude_doc_ids_owned) freeDocIdSlice(alloc, out.exclude_doc_ids);
                freeDocIdSlice(alloc, ids);
                out.exclude_doc_ids = merged;
            } else {
                out.exclude_doc_ids = ids;
            }
            out.exclude_doc_ids_owned = true;
            out.resolved_stored_filters = true;
            exclude_represented = true;
        }
    }
    if (!exclude_represented) return error.UnsupportedQueryRequest;
}

fn textDocNumsForResolvedDocSetAlloc(
    alloc: Allocator,
    snapshot: *const index_mod.IndexSnapshot,
    set: *const doc_set.ResolvedDocSet,
) !?[]const u32 {
    const ordinals = (try docNumsForResolvedDocSetAlloc(alloc, set)) orelse return null;
    defer alloc.free(ordinals);
    return try snapshot.docNumsForOrdinalsAlloc(alloc, ordinals);
}

fn maybeLiveFilterResolvedDocSetAlloc(
    alloc: Allocator,
    set: *const doc_set.ResolvedDocSet,
    executor: StructuredFilterResolverExecutor,
    owned: *?doc_set.ResolvedDocSet,
) !*const doc_set.ResolvedDocSet {
    switch (set.*) {
        .doc_keys, .ordinals, .ordinal_bitmap => {},
        .all, .none => return set,
    }
    const live_filter = executor.live_filter_doc_set orelse return set;
    owned.* = try live_filter(executor.ctx, alloc, set, executor.identity_read_generation);
    return &owned.*.?;
}

fn resolvedSetHasDocKeys(set: *const doc_set.ResolvedDocSet) bool {
    return switch (set.*) {
        .doc_keys => true,
        else => false,
    };
}

fn docIdsForResolvedDocSetAlloc(
    alloc: Allocator,
    set: *const doc_set.ResolvedDocSet,
    executor: StructuredFilterResolverExecutor,
) !?[]const []const u8 {
    return switch (set.*) {
        .all => null,
        .none => try alloc.alloc([]const u8, 0),
        .doc_keys => |keys| try dupeDocIdSliceAlloc(alloc, keys),
        .ordinals, .ordinal_bitmap => if (executor.resolve_doc_set_doc_ids) |resolve|
            try resolve(executor.ctx, alloc, set, executor.identity_read_generation)
        else
            null,
    };
}

fn docNumsForResolvedDocSetWithExecutorAlloc(
    alloc: Allocator,
    set: *const doc_set.ResolvedDocSet,
    executor: StructuredFilterResolverExecutor,
) !?[]const u32 {
    switch (set.*) {
        .ordinals, .ordinal_bitmap => {
            const lookup = executor.lookup_doc_nums_for_ordinals orelse {
                if (executor.require_doc_num_projection_mapper) return error.UnsupportedQueryRequest;
                return try docNumsForResolvedDocSetAlloc(alloc, set);
            };
            const index_name = executor.doc_num_index_name orelse {
                if (executor.require_doc_num_projection_mapper) return error.UnsupportedQueryRequest;
                return try docNumsForResolvedDocSetAlloc(alloc, set);
            };
            const ordinals = (try docNumsForResolvedDocSetAlloc(alloc, set)) orelse return null;
            defer alloc.free(ordinals);
            return try lookup(executor.ctx, alloc, index_name, ordinals);
        },
        else => return try docNumsForResolvedDocSetAlloc(alloc, set),
    }
}

fn docNumsForResolvedDocSetAlloc(
    alloc: Allocator,
    set: *const doc_set.ResolvedDocSet,
) !?[]const u32 {
    return switch (set.*) {
        .all, .doc_keys => null,
        .none => try alloc.alloc(u32, 0),
        .ordinals => |ordinals| try alloc.dupe(u32, ordinals),
        .ordinal_bitmap => |*bitmap| blk: {
            var out = std.ArrayListUnmanaged(u32).empty;
            errdefer out.deinit(alloc);
            var iter = bitmap.iterator();
            while (iter.next()) |ordinal| try out.append(alloc, ordinal);
            break :blk try out.toOwnedSlice(alloc);
        },
    };
}

fn collectStructuredFilterResolvedDocSetAlloc(
    alloc: Allocator,
    req: types.SearchRequest,
    executor: StructuredFilterResolverExecutor,
    filter_query_json: []const u8,
) !?doc_set.ResolvedDocSet {
    const text_entry = try resolveFilterTextIndexEntry(executor, req.primary_text_index_name, req.index_name) orelse return null;

    var arena = std.heap.ArenaAllocator.init(alloc);
    defer arena.deinit();
    const arena_alloc = arena.allocator();
    const parsed = std.json.parseFromSlice(std.json.Value, arena_alloc, filter_query_json, .{}) catch return null;
    const search_query = patternFilterValueToSearchQuery(arena_alloc, parsed.value, text_entry.text_analysis, text_entry.runtime_schema) catch return null;

    const snapshot = text_entry.persistent.snapshot();
    const k: u32 = @intCast(@min(snapshot.global_doc_count, @as(u64, std.math.maxInt(u32))));
    var result = try search_mod.execute(alloc, snapshot, .{
        .query = search_query,
        .k = k,
        .offset = 0,
        .include_stored = false,
        .distributed_text_stats = req.distributed_text_stats,
    });
    defer result.deinit();
    if (result.hits.len == 0) return .none;

    if (snapshot.hasDocOrdinalCoverage()) {
        if (try resolvedDocSetForTextHitsFromOrdinalSidecarAlloc(alloc, snapshot, result.hits, executor)) |resolved| {
            return resolved;
        }
    }

    const resolve = executor.resolve_doc_ids_to_doc_set orelse return null;
    var doc_ids = std.ArrayListUnmanaged([]const u8).empty;
    defer freeDocIdArrayList(alloc, &doc_ids);
    for (result.hits) |hit| {
        const id = hit.id orelse blk: {
            const stored = snapshot.storedDoc(hit.doc_id) orelse continue;
            break :blk stored.id;
        };
        try appendOwnedDocId(alloc, &doc_ids, id);
    }
    return try resolve(executor.ctx, alloc, doc_ids.items, executor.identity_read_generation);
}

fn resolvedDocSetForTextHitsFromOrdinalSidecarAlloc(
    alloc: Allocator,
    snapshot: *const index_mod.IndexSnapshot,
    hits: []const search_mod.ScoredHit,
    executor: StructuredFilterResolverExecutor,
) !?doc_set.ResolvedDocSet {
    var doc_nums = std.ArrayListUnmanaged(u32).empty;
    defer doc_nums.deinit(alloc);
    for (hits) |hit| try appendDocNum(alloc, &doc_nums, hit.doc_id);

    const ordinals = (try snapshot.docOrdinalsForDocNumsAlloc(alloc, doc_nums.items)) orelse return null;
    defer alloc.free(ordinals);
    if (ordinals.len == 0) return null;

    var resolved = try doc_set.fromOrdinalsAlloc(alloc, ordinals);
    errdefer resolved.deinit(alloc);
    if (executor.live_filter_doc_set) |live_filter| {
        defer resolved.deinit(alloc);
        return try live_filter(executor.ctx, alloc, &resolved, executor.identity_read_generation);
    }
    return resolved;
}

fn collectStructuredFilterResolvedDocSetCachedAlloc(
    alloc: Allocator,
    req: types.SearchRequest,
    executor: StructuredFilterResolverExecutor,
    cache: *StructuredFilterDocSetCache,
    filter_query_json: []const u8,
) !?doc_set.ResolvedDocSet {
    if (cache.get(filter_query_json, executor.identity_read_generation)) |cached| {
        return try doc_set.cloneAlloc(alloc, cached);
    }
    var resolved = (try collectStructuredFilterResolvedDocSetAlloc(alloc, req, executor, filter_query_json)) orelse return null;
    errdefer resolved.deinit(alloc);
    try cache.putCloneAlloc(alloc, filter_query_json, executor.identity_read_generation, &resolved);
    return resolved;
}

fn collectStructuredFilterDocIdsAlloc(
    alloc: Allocator,
    req: types.SearchRequest,
    executor: StructuredFilterResolverExecutor,
    filter_query_json: []const u8,
) !?[]const []const u8 {
    const text_entry = try resolveFilterTextIndexEntry(executor, req.primary_text_index_name, req.index_name) orelse return null;

    var arena = std.heap.ArenaAllocator.init(alloc);
    defer arena.deinit();
    const arena_alloc = arena.allocator();
    const parsed = std.json.parseFromSlice(std.json.Value, arena_alloc, filter_query_json, .{}) catch return null;
    const search_query = patternFilterValueToSearchQuery(arena_alloc, parsed.value, text_entry.text_analysis, text_entry.runtime_schema) catch return null;

    const snapshot = text_entry.persistent.snapshot();
    const k: u32 = @intCast(@min(snapshot.global_doc_count, @as(u64, std.math.maxInt(u32))));
    var result = try search_mod.execute(alloc, snapshot, .{
        .query = search_query,
        .k = k,
        .offset = 0,
        .include_stored = false,
        .distributed_text_stats = req.distributed_text_stats,
    });
    defer result.deinit();
    if (result.hits.len == 0) return try alloc.alloc([]const u8, 0);

    var out = std.ArrayListUnmanaged([]const u8).empty;
    errdefer freeDocIdArrayList(alloc, &out);
    for (result.hits) |hit| {
        const id = hit.id orelse blk: {
            const stored = snapshot.storedDoc(hit.doc_id) orelse continue;
            break :blk stored.id;
        };
        try appendOwnedDocId(alloc, &out, id);
    }
    return try out.toOwnedSlice(alloc);
}

fn resolveFilterTextIndexEntry(
    executor: StructuredFilterResolverExecutor,
    primary_text_index_name: ?[]const u8,
    index_name: ?[]const u8,
) !?*index_manager_mod.IndexManager.TextIndex {
    if (primary_text_index_name) |name| {
        if (try executor.text_index_entry(executor.ctx, name)) |entry| return entry;
    }
    if (index_name) |name| {
        if (try executor.text_index_entry(executor.ctx, name)) |entry| return entry;
    }
    return try executor.text_index_entry(executor.ctx, null);
}

fn dupeDocIdSliceAlloc(alloc: Allocator, doc_ids: []const []const u8) ![]const []const u8 {
    var out = std.ArrayListUnmanaged([]const u8).empty;
    errdefer freeDocIdArrayList(alloc, &out);
    for (doc_ids) |doc_id| try appendOwnedDocId(alloc, &out, doc_id);
    return try out.toOwnedSlice(alloc);
}

fn unionDocIdsAlloc(alloc: Allocator, left: []const []const u8, right: []const []const u8) ![]const []const u8 {
    var out = std.ArrayListUnmanaged([]const u8).empty;
    errdefer freeDocIdArrayList(alloc, &out);
    for (left) |id| try appendOwnedDocId(alloc, &out, id);
    for (right) |id| try appendOwnedDocId(alloc, &out, id);
    return try out.toOwnedSlice(alloc);
}

fn intersectDocIdsAlloc(alloc: Allocator, left: []const []const u8, right: []const []const u8) ![]const []const u8 {
    var out = std.ArrayListUnmanaged([]const u8).empty;
    errdefer freeDocIdArrayList(alloc, &out);
    for (left) |id| {
        if (!containsDocId(right, id)) continue;
        try appendOwnedDocId(alloc, &out, id);
    }
    return try out.toOwnedSlice(alloc);
}

fn unionDocNumsAlloc(alloc: Allocator, left: []const u32, right: []const u32) ![]const u32 {
    var out = std.ArrayListUnmanaged(u32).empty;
    errdefer out.deinit(alloc);
    for (left) |doc_num| try appendDocNum(alloc, &out, doc_num);
    for (right) |doc_num| try appendDocNum(alloc, &out, doc_num);
    return try out.toOwnedSlice(alloc);
}

fn intersectDocNumsAlloc(alloc: Allocator, left: []const u32, right: []const u32) ![]const u32 {
    var out = std.ArrayListUnmanaged(u32).empty;
    errdefer out.deinit(alloc);
    for (left) |doc_num| {
        if (!containsDocNum(right, doc_num)) continue;
        try appendDocNum(alloc, &out, doc_num);
    }
    return try out.toOwnedSlice(alloc);
}

fn appendDocNum(alloc: Allocator, out: *std.ArrayListUnmanaged(u32), doc_num: u32) !void {
    if (containsDocNum(out.items, doc_num)) return;
    try out.append(alloc, doc_num);
}

fn appendOwnedDocId(alloc: Allocator, out: *std.ArrayListUnmanaged([]const u8), id: []const u8) !void {
    for (out.items) |existing| {
        if (std.mem.eql(u8, existing, id)) return;
    }
    try out.append(alloc, try alloc.dupe(u8, id));
}

fn freeDocIdArrayList(alloc: Allocator, out: *std.ArrayListUnmanaged([]const u8)) void {
    for (out.items) |id| alloc.free(@constCast(id));
    out.deinit(alloc);
}

fn freeDocIdSlice(alloc: Allocator, doc_ids: []const []const u8) void {
    for (doc_ids) |id| alloc.free(@constCast(id));
    if (doc_ids.len > 0) alloc.free(@constCast(doc_ids));
}

fn containsDocId(doc_ids: []const []const u8, expected: []const u8) bool {
    for (doc_ids) |doc_id| {
        if (std.mem.eql(u8, doc_id, expected)) return true;
    }
    return false;
}

fn containsDocNum(doc_nums: []const u32, expected: u32) bool {
    for (doc_nums) |doc_num| {
        if (doc_num == expected) return true;
    }
    return false;
}

fn patternFilterValueToSearchQuery(
    alloc: Allocator,
    value: std.json.Value,
    text_analysis: introducer_mod.TextAnalysisConfig,
    runtime_schema: ?runtime_schema_mod.TableSchema,
) anyerror!search_mod.SearchQuery {
    if (value != .object) return error.InvalidArgument;
    if (value.object.get("match_all") != null) return .{ .match_all = {} };
    if (value.object.get("match_none") != null) return .{ .match_none = {} };
    if (value.object.get("doc_id")) |doc_id| return .{ .doc_id = .{
        .ids = try parsePatternDocIdsForSearch(alloc, doc_id),
    } };
    if (value.object.get("conjuncts")) |conjuncts| return .{ .bool_query = .{
        .must = try patternFilterArrayToSearchQueries(alloc, conjuncts, text_analysis, runtime_schema),
    } };
    if (value.object.get("disjuncts")) |disjuncts| return .{ .bool_query = .{
        .should = try patternFilterArrayToSearchQueries(alloc, disjuncts, text_analysis, runtime_schema),
        .min_should = 1,
    } };
    if (value.object.get("bool")) |bool_query| return try patternBoolFilterToSearchQuery(alloc, bool_query, text_analysis, runtime_schema);

    if (value.object.get("term")) |term| {
        const field_value = try singleFieldString(term, "term");
        return .{ .term = .{ .field = field_value.field, .term = field_value.value } };
    }
    if (value.object.get("terms")) |terms| {
        const field_terms = try singleFieldTerms(alloc, terms);
        const should = try alloc.alloc(search_mod.SearchQuery, field_terms.terms.len);
        for (field_terms.terms, 0..) |term, i| {
            should[i] = .{ .term = .{ .field = field_terms.field, .term = term } };
        }
        return .{ .bool_query = .{ .should = should, .min_should = 1 } };
    }
    if (value.object.get("match")) |match| {
        const field_value = try singleFieldString(match, "text");
        return .{ .match = .{
            .field = field_value.field,
            .text = field_value.value,
            .analyzer = try resolveQueryAnalyzer(field_value.field, null, text_analysis, runtime_schema),
        } };
    }
    if (value.object.get("prefix")) |prefix| {
        const field_value = try singleFieldString(prefix, "prefix");
        return .{ .prefix = .{ .field = field_value.field, .prefix = field_value.value } };
    }
    if (value.object.get("wildcard")) |wildcard| {
        const field_value = try singleFieldString(wildcard, "pattern");
        return .{ .wildcard = .{ .field = field_value.field, .pattern = field_value.value } };
    }
    if (value.object.get("regexp")) |regexp| {
        const field_value = try singleFieldString(regexp, "pattern");
        return .{ .regexp = .{ .field = field_value.field, .pattern = field_value.value } };
    }
    if (value.object.get("fuzzy")) |fuzzy| {
        const field_fuzzy = try singleFieldFuzzy(fuzzy);
        return .{ .fuzzy = .{
            .field = field_fuzzy.field,
            .term = field_fuzzy.term,
            .max_edits = field_fuzzy.max_edits,
            .prefix_len = field_fuzzy.prefix_len,
            .auto_fuzzy = field_fuzzy.auto_fuzzy,
        } };
    }
    if (value.object.get("numeric_range")) |range_query| return .{ .numeric_range = try parseNumericRangeQuery(range_query) };
    if (value.object.get("bool_field")) |bool_query| return .{ .bool_field = try parseBoolFieldQuery(bool_query) };
    if (value.object.get("term_range")) |range_query| return .{ .term_range = try parseTermRangeQuery(range_query) };
    if (value.object.get("ip_range")) |range_query| return .{ .ip_range = try parseIpRangeQuery(range_query) };
    if (value.object.get("geo_distance")) |geo_query| return .{ .geo_distance = try parseGeoDistanceQuery(geo_query) };
    if (value.object.get("geo_bbox")) |geo_query| return .{ .geo_bbox = try parseGeoBBoxQuery(geo_query) };
    return error.UnsupportedQueryRequest;
}

fn patternBoolFilterToSearchQuery(
    alloc: Allocator,
    value: std.json.Value,
    text_analysis: introducer_mod.TextAnalysisConfig,
    runtime_schema: ?runtime_schema_mod.TableSchema,
) !search_mod.SearchQuery {
    if (value != .object) return error.InvalidArgument;
    return .{ .bool_query = .{
        .must = if (value.object.get("must")) |must|
            try patternFilterArrayToSearchQueries(alloc, must, text_analysis, runtime_schema)
        else
            &.{},
        .should = if (value.object.get("should")) |should|
            try patternFilterArrayToSearchQueries(alloc, should, text_analysis, runtime_schema)
        else
            &.{},
        .must_not = if (value.object.get("must_not")) |must_not|
            try patternFilterArrayToSearchQueries(alloc, must_not, text_analysis, runtime_schema)
        else
            &.{},
        .min_should = if (value.object.get("should") != null) 1 else 0,
    } };
}

fn patternFilterArrayToSearchQueries(
    alloc: Allocator,
    value: std.json.Value,
    text_analysis: introducer_mod.TextAnalysisConfig,
    runtime_schema: ?runtime_schema_mod.TableSchema,
) ![]const search_mod.SearchQuery {
    if (value != .array or value.array.items.len == 0) return error.InvalidArgument;
    const out = try alloc.alloc(search_mod.SearchQuery, value.array.items.len);
    for (value.array.items, 0..) |item, i| {
        out[i] = try patternFilterValueToSearchQuery(alloc, item, text_analysis, runtime_schema);
    }
    return out;
}

fn parsePatternDocIdsForSearch(alloc: Allocator, value: std.json.Value) ![]const []const u8 {
    const ids = switch (value) {
        .object => value.object.get("ids") orelse return error.InvalidArgument,
        .array => value,
        else => return error.InvalidArgument,
    };
    if (ids != .array or ids.array.items.len == 0) return error.InvalidArgument;
    const out = try alloc.alloc([]const u8, ids.array.items.len);
    for (ids.array.items, 0..) |item, i| {
        if (item != .string) return error.InvalidArgument;
        out[i] = item.string;
    }
    return out;
}

const FieldString = struct {
    field: []const u8,
    value: []const u8,
};

const FieldTerms = struct {
    field: []const u8,
    terms: []const []const u8,
};

const FieldFuzzy = struct {
    field: []const u8,
    term: []const u8,
    max_edits: u8 = 1,
    prefix_len: u8 = 0,
    auto_fuzzy: bool = false,
};

fn singleFieldString(value: std.json.Value, value_key: []const u8) !FieldString {
    if (value != .object) return error.InvalidArgument;
    if (value.object.get("field") orelse value.object.get("path")) |field_value| {
        if (field_value != .string) return error.InvalidArgument;
        const raw_value = value.object.get(value_key) orelse value.object.get("value") orelse return error.InvalidArgument;
        if (raw_value != .string) return error.InvalidArgument;
        return .{ .field = field_value.string, .value = raw_value.string };
    }
    if (value.object.count() != 1) return error.InvalidArgument;
    var it = value.object.iterator();
    const entry = it.next() orelse return error.InvalidArgument;
    if (entry.value_ptr.* != .string) return error.InvalidArgument;
    return .{ .field = entry.key_ptr.*, .value = entry.value_ptr.string };
}

fn singleFieldTerms(alloc: Allocator, value: std.json.Value) !FieldTerms {
    if (value != .object) return error.InvalidArgument;
    if (value.object.get("field") orelse value.object.get("path")) |field_value| {
        if (field_value != .string) return error.InvalidArgument;
        const raw_values = value.object.get("values") orelse value.object.get("terms") orelse return error.InvalidArgument;
        return .{ .field = field_value.string, .terms = try parseScalarTerms(alloc, raw_values) };
    }
    if (value.object.count() != 1) return error.InvalidArgument;
    var it = value.object.iterator();
    const entry = it.next() orelse return error.InvalidArgument;
    return .{ .field = entry.key_ptr.*, .terms = try parseScalarTerms(alloc, entry.value_ptr.*) };
}

fn singleFieldFuzzy(value: std.json.Value) !FieldFuzzy {
    if (value != .object) return error.InvalidArgument;
    if (value.object.get("field") orelse value.object.get("path")) |field_value| {
        if (field_value != .string) return error.InvalidArgument;
        var out = FieldFuzzy{
            .field = field_value.string,
            .term = jsonString(value.object.get("query") orelse value.object.get("value") orelse return error.InvalidArgument) orelse return error.InvalidArgument,
        };
        try parseFuzzyOptions(value.object, &out);
        return out;
    }
    if (value.object.count() != 1) return error.InvalidArgument;
    var it = value.object.iterator();
    const entry = it.next() orelse return error.InvalidArgument;
    var out = switch (entry.value_ptr.*) {
        .string => |term| FieldFuzzy{ .field = entry.key_ptr.*, .term = term },
        .object => |object| blk: {
            var parsed = FieldFuzzy{
                .field = entry.key_ptr.*,
                .term = jsonString(object.get("query") orelse object.get("value") orelse return error.InvalidArgument) orelse return error.InvalidArgument,
            };
            try parseFuzzyOptions(object, &parsed);
            break :blk parsed;
        },
        else => return error.InvalidArgument,
    };
    if (out.auto_fuzzy) out.max_edits = autoFuzzyEdits(out.term);
    return out;
}

test "pattern filter single-field helpers accept explicit path alias" {
    const alloc = std.testing.allocator;

    var term_json = try std.json.parseFromSlice(std.json.Value, alloc,
        \\{"path":"/tier","term":"gold"}
    , .{});
    defer term_json.deinit();
    const term = try singleFieldString(term_json.value, "term");
    try std.testing.expectEqualStrings("/tier", term.field);
    try std.testing.expectEqualStrings("gold", term.value);

    var terms_json = try std.json.parseFromSlice(std.json.Value, alloc,
        \\{"path":"/tier","values":["gold","silver"]}
    , .{});
    defer terms_json.deinit();
    const terms = try singleFieldTerms(alloc, terms_json.value);
    defer {
        for (terms.terms) |item| alloc.free(@constCast(item));
        alloc.free(terms.terms);
    }
    try std.testing.expectEqualStrings("/tier", terms.field);
    try std.testing.expectEqual(@as(usize, 2), terms.terms.len);
    try std.testing.expectEqualStrings("gold", terms.terms[0]);
    try std.testing.expectEqualStrings("silver", terms.terms[1]);

    var fuzzy_json = try std.json.parseFromSlice(std.json.Value, alloc,
        \\{"path":"/tier","value":"gild","prefix_length":1}
    , .{});
    defer fuzzy_json.deinit();
    const fuzzy = try singleFieldFuzzy(fuzzy_json.value);
    try std.testing.expectEqualStrings("/tier", fuzzy.field);
    try std.testing.expectEqualStrings("gild", fuzzy.term);
    try std.testing.expectEqual(@as(u8, 1), fuzzy.prefix_len);
}

fn parseFuzzyOptions(object: anytype, out: *FieldFuzzy) !void {
    if (object.get("max_edits")) |edits| out.max_edits = jsonU8(edits) orelse return error.InvalidArgument;
    if (object.get("prefix_length")) |prefix| out.prefix_len = jsonU8(prefix) orelse return error.InvalidArgument;
    if (object.get("auto_fuzzy")) |auto| {
        if (auto != .bool) return error.InvalidArgument;
        out.auto_fuzzy = auto.bool;
        if (auto.bool) out.max_edits = autoFuzzyEdits(out.term);
    }
}

fn autoFuzzyEdits(term: []const u8) u8 {
    return if (term.len > 5) 2 else if (term.len > 2) 1 else 0;
}

fn parseScalarTerms(alloc: Allocator, value: std.json.Value) ![]const []const u8 {
    if (value != .array or value.array.items.len == 0) return error.InvalidArgument;
    const out = try alloc.alloc([]const u8, value.array.items.len);
    for (value.array.items, 0..) |item, i| {
        out[i] = try jsonScalarTermAlloc(alloc, item);
    }
    return out;
}

fn jsonScalarTermAlloc(alloc: Allocator, value: std.json.Value) ![]const u8 {
    return switch (value) {
        .string => |text| try alloc.dupe(u8, text),
        .integer => |number| try std.fmt.allocPrint(alloc, "{}", .{number}),
        .float => |number| try std.fmt.allocPrint(alloc, "{d}", .{number}),
        .number_string => |text| try alloc.dupe(u8, text),
        .bool => |boolean| try alloc.dupe(u8, if (boolean) "true" else "false"),
        .null => try alloc.dupe(u8, "null"),
        else => error.InvalidArgument,
    };
}

fn parseNumericRangeQuery(value: std.json.Value) !search_mod.NumericRangeQuery {
    if (value != .object) return error.InvalidArgument;
    const field = jsonString(value.object.get("field") orelse return error.InvalidArgument) orelse return error.InvalidArgument;
    return .{
        .field = field,
        .min = jsonOptionalF64(value.object.get("min")),
        .max = jsonOptionalF64(value.object.get("max")),
        .inclusive_min = jsonOptionalBool(value.object.get("inclusive_min")) orelse true,
        .inclusive_max = jsonOptionalBool(value.object.get("inclusive_max")) orelse false,
    };
}

fn parseBoolFieldQuery(value: std.json.Value) !search_mod.BoolFieldQuery {
    if (value != .object) return error.InvalidArgument;
    const field = jsonString(value.object.get("field") orelse return error.InvalidArgument) orelse return error.InvalidArgument;
    const bool_value = jsonOptionalBool(value.object.get("value")) orelse return error.InvalidArgument;
    return .{ .field = field, .value = bool_value };
}

fn parseTermRangeQuery(value: std.json.Value) !search_mod.TermRangeQuery {
    if (value != .object) return error.InvalidArgument;
    const field = jsonString(value.object.get("field") orelse return error.InvalidArgument) orelse return error.InvalidArgument;
    return .{
        .field = field,
        .min = jsonString(value.object.get("min") orelse .null),
        .max = jsonString(value.object.get("max") orelse .null),
        .inclusive_min = jsonOptionalBool(value.object.get("inclusive_min")) orelse true,
        .inclusive_max = jsonOptionalBool(value.object.get("inclusive_max")) orelse false,
    };
}

fn parseIpRangeQuery(value: std.json.Value) !search_mod.IPRangeQuery {
    if (value != .object) return error.InvalidArgument;
    const field = jsonString(value.object.get("field") orelse return error.InvalidArgument) orelse return error.InvalidArgument;
    const cidr = jsonString(value.object.get("cidr") orelse return error.InvalidArgument) orelse return error.InvalidArgument;
    return .{ .field = field, .cidr = cidr };
}

fn parseGeoDistanceQuery(value: std.json.Value) !search_mod.GeoDistanceQuery {
    if (value != .object) return error.InvalidArgument;
    const field = jsonString(value.object.get("field") orelse return error.InvalidArgument) orelse return error.InvalidArgument;
    const lon = jsonOptionalF64(value.object.get("lon")) orelse return error.InvalidArgument;
    const lat = jsonOptionalF64(value.object.get("lat")) orelse return error.InvalidArgument;
    const radius_meters = jsonOptionalF64(value.object.get("radius_meters")) orelse return error.InvalidArgument;
    return .{ .field = field, .center = .{ .lon = lon, .lat = lat }, .radius_meters = radius_meters };
}

fn parseGeoBBoxQuery(value: std.json.Value) !search_mod.GeoBBoxQuery {
    if (value != .object) return error.InvalidArgument;
    const field = jsonString(value.object.get("field") orelse return error.InvalidArgument) orelse return error.InvalidArgument;
    return .{
        .field = field,
        .min_lat = jsonOptionalF64(value.object.get("min_lat")) orelse return error.InvalidArgument,
        .min_lon = jsonOptionalF64(value.object.get("min_lon")) orelse return error.InvalidArgument,
        .max_lat = jsonOptionalF64(value.object.get("max_lat")) orelse return error.InvalidArgument,
        .max_lon = jsonOptionalF64(value.object.get("max_lon")) orelse return error.InvalidArgument,
    };
}

fn jsonString(value: std.json.Value) ?[]const u8 {
    return switch (value) {
        .string => |text| text,
        else => null,
    };
}

fn jsonOptionalF64(value: ?std.json.Value) ?f64 {
    const actual = value orelse return null;
    return switch (actual) {
        .integer => |number| @floatFromInt(number),
        .float => |number| number,
        else => null,
    };
}

fn jsonOptionalBool(value: ?std.json.Value) ?bool {
    const actual = value orelse return null;
    return switch (actual) {
        .bool => |boolean| boolean,
        else => null,
    };
}

fn jsonU8(value: std.json.Value) ?u8 {
    return switch (value) {
        .integer => |number| std.math.cast(u8, number),
        .float => |number| blk: {
            if (!std.math.isFinite(number) or @round(number) != number) break :blk null;
            const parsed: i64 = @intFromFloat(number);
            break :blk std.math.cast(u8, parsed);
        },
        else => null,
    };
}

fn deriveNativeDenseConstraintsAlloc(
    alloc: Allocator,
    req: types.SearchRequest,
    executor: DenseSearchExecutor,
    index_name: []const u8,
    apply_live_all_docs: bool,
) !NativeDenseConstraints {
    var out = NativeDenseConstraints{};
    errdefer out.deinit(alloc);

    if (req.filter_ids.len > 0) {
        out.positive_filter = true;
        out.filter_ids = req.filter_ids;
    }

    var doc_constraints = try deriveNativeDocIdConstraintsAlloc(alloc, req, .{
        .ctx = executor.ctx,
        .text_index_entry = executor.text_index_entry,
        .resolve_doc_set_doc_ids = executor.resolve_doc_set_doc_ids,
        .resolve_doc_ids_to_doc_set = executor.resolve_doc_ids_to_doc_set,
        .live_filter_doc_set = executor.live_filter_doc_set,
        .project_ordinals_to_doc_ids = false,
        .apply_live_all_docs = apply_live_all_docs,
    });
    defer doc_constraints.deinit(alloc);

    if (doc_constraints.positive_filter) {
        var mapped: []u64 = &.{};
        var mapped_owned = false;
        if (doc_constraints.filter_doc_nums.len > 0) {
            mapped = try denseVectorIdsForDocNumsAlloc(alloc, doc_constraints.filter_doc_nums, executor, index_name);
            mapped_owned = true;
        }
        if (doc_constraints.filter_doc_ids.len > 0) {
            const mapped_doc_ids = try denseVectorIdsForDocIdsAlloc(alloc, doc_constraints.filter_doc_ids, executor, index_name);
            if (mapped_owned) {
                const intersected = try intersectVectorIdsAlloc(alloc, mapped, mapped_doc_ids);
                alloc.free(mapped);
                alloc.free(mapped_doc_ids);
                mapped = intersected;
            } else {
                mapped = mapped_doc_ids;
            }
            mapped_owned = true;
        }
        if (out.positive_filter) {
            const intersected = try intersectVectorIdsAlloc(alloc, out.filter_ids, mapped);
            if (mapped_owned) alloc.free(mapped);
            if (out.filter_ids_owned and out.filter_ids.len > 0) alloc.free(@constCast(out.filter_ids));
            out.filter_ids = intersected;
            out.filter_ids_owned = true;
        } else {
            out.filter_ids = mapped;
            out.filter_ids_owned = mapped_owned;
            out.positive_filter = true;
        }
    }

    if (doc_constraints.exclude_doc_nums.len > 0) {
        const mapped_excludes = try denseVectorIdsForDocNumsAlloc(alloc, doc_constraints.exclude_doc_nums, executor, index_name);
        try mergeNativeExcludeIds(alloc, &out, mapped_excludes, req.exclude_ids);
    }
    if (doc_constraints.exclude_doc_ids.len > 0) {
        const mapped_excludes = try denseVectorIdsForDocIdsAlloc(alloc, doc_constraints.exclude_doc_ids, executor, index_name);
        try mergeNativeExcludeIds(alloc, &out, mapped_excludes, req.exclude_ids);
    }
    if (out.exclude_ids.len == 0 and req.exclude_ids.len > 0) {
        out.exclude_ids = req.exclude_ids;
    }
    out.resolved_stored_filters = doc_constraints.resolved_stored_filters;
    return out;
}

fn mergeNativeExcludeIds(
    alloc: Allocator,
    out: *NativeDenseConstraints,
    mapped_excludes: []u64,
    request_excludes: []const u64,
) !void {
    defer alloc.free(mapped_excludes);
    const base = if (out.exclude_ids.len > 0) out.exclude_ids else request_excludes;
    const merged = try unionVectorIdsAlloc(alloc, base, mapped_excludes);
    if (out.exclude_ids_owned and out.exclude_ids.len > 0) alloc.free(@constCast(out.exclude_ids));
    out.exclude_ids = merged;
    out.exclude_ids_owned = true;
}

fn collectPositiveDocIdSuperset(
    alloc: Allocator,
    filter: graph_exec.CompiledPatternFilter,
    out: *std.ArrayListUnmanaged([]const u8),
) !bool {
    return switch (filter) {
        .match_none => true,
        .doc_id => |ids| {
            try appendDocIds(alloc, out, ids);
            return true;
        },
        .conjuncts => |items| blk: {
            for (items) |item| {
                if (try collectPositiveDocIdSuperset(alloc, item, out)) break :blk true;
            }
            break :blk false;
        },
        .disjuncts => try collectExactDocIds(alloc, filter, out),
        .bool_query => |bool_query| blk: {
            for (bool_query.must) |item| {
                if (try collectPositiveDocIdSuperset(alloc, item, out)) break :blk true;
            }
            if (bool_query.must.len == 0 and bool_query.must_not.len == 0 and bool_query.should.len > 0) {
                break :blk try collectAllExactDocIds(alloc, bool_query.should, out);
            }
            break :blk false;
        },
        else => false,
    };
}

fn collectExactDocIds(
    alloc: Allocator,
    filter: graph_exec.CompiledPatternFilter,
    out: *std.ArrayListUnmanaged([]const u8),
) anyerror!bool {
    return switch (filter) {
        .match_none => true,
        .doc_id => |ids| {
            try appendDocIds(alloc, out, ids);
            return true;
        },
        .disjuncts => |items| try collectAllExactDocIds(alloc, items, out),
        else => false,
    };
}

fn collectAllExactDocIds(
    alloc: Allocator,
    items: []const graph_exec.CompiledPatternFilter,
    out: *std.ArrayListUnmanaged([]const u8),
) anyerror!bool {
    const start = out.items.len;
    for (items) |item| {
        if (!(try collectExactDocIds(alloc, item, out))) {
            out.shrinkRetainingCapacity(start);
            return false;
        }
    }
    return true;
}

fn collectBoolMustNotExactDocIds(
    alloc: Allocator,
    filter: graph_exec.CompiledPatternFilter,
    out: *std.ArrayListUnmanaged([]const u8),
) !void {
    switch (filter) {
        .bool_query => |bool_query| {
            for (bool_query.must_not) |item| {
                _ = try collectExactDocIds(alloc, item, out);
            }
            for (bool_query.must) |item| try collectBoolMustNotExactDocIds(alloc, item, out);
        },
        .conjuncts => |items| for (items) |item| try collectBoolMustNotExactDocIds(alloc, item, out),
        else => {},
    }
}

fn appendDocIds(
    alloc: Allocator,
    out: *std.ArrayListUnmanaged([]const u8),
    ids: []const []const u8,
) !void {
    for (ids) |id| {
        var exists = false;
        for (out.items) |existing| {
            if (std.mem.eql(u8, existing, id)) {
                exists = true;
                break;
            }
        }
        if (!exists) try out.append(alloc, id);
    }
}

fn denseVectorIdsForDocIdsAlloc(
    alloc: Allocator,
    doc_ids: []const []const u8,
    executor: DenseSearchExecutor,
    index_name: []const u8,
) ![]u64 {
    var out = std.ArrayListUnmanaged(u64).empty;
    errdefer out.deinit(alloc);
    for (doc_ids) |doc_id| {
        const vector_id = (try executor.lookup_vector_id(executor.ctx, index_name, doc_id)) orelse continue;
        if (!containsVectorId(out.items, vector_id)) try out.append(alloc, vector_id);
    }
    return try out.toOwnedSlice(alloc);
}

fn denseVectorIdsForDocNumsAlloc(
    alloc: Allocator,
    doc_nums: []const u32,
    executor: DenseSearchExecutor,
    index_name: []const u8,
) ![]u64 {
    if (executor.lookup_vector_ids_for_ordinals) |lookup| {
        return try lookup(executor.ctx, alloc, index_name, doc_nums);
    }
    return error.UnsupportedQueryRequest;
}

fn intersectVectorIdsAlloc(alloc: Allocator, left: []const u64, right: []const u64) ![]u64 {
    var out = std.ArrayListUnmanaged(u64).empty;
    errdefer out.deinit(alloc);
    for (left) |id| {
        if (containsVectorId(right, id) and !containsVectorId(out.items, id)) try out.append(alloc, id);
    }
    return try out.toOwnedSlice(alloc);
}

fn unionVectorIdsAlloc(alloc: Allocator, left: []const u64, right: []const u64) ![]u64 {
    var out = std.ArrayListUnmanaged(u64).empty;
    errdefer out.deinit(alloc);
    for (left) |id| if (!containsVectorId(out.items, id)) try out.append(alloc, id);
    for (right) |id| if (!containsVectorId(out.items, id)) try out.append(alloc, id);
    return try out.toOwnedSlice(alloc);
}

fn containsVectorId(items: []const u64, id: u64) bool {
    for (items) |item| if (item == id) return true;
    return false;
}

pub fn shouldGroupChunkParents(req: types.SearchRequest, is_chunk_backed: bool) bool {
    return is_chunk_backed and req.return_mode != .chunk;
}

pub fn searchText(
    alloc: Allocator,
    req: types.SearchRequest,
    dispatcher: SearchTextDispatcher,
) !types.SearchResult {
    return switch (req.query) {
        .match_none => try dispatcher.func(dispatcher.ctx, alloc, req, .{ .match_none = {} }),
        .match_all => try dispatcher.func(dispatcher.ctx, alloc, req, .{ .match_all = {} }),
        .phrase => |phrase| try dispatcher.func(dispatcher.ctx, alloc, req, .{ .phrase = .{
            .field = phrase.field,
            .terms = phrase.terms,
            .max_edits = phrase.max_edits,
            .auto_fuzzy = phrase.auto_fuzzy,
            .boost = phrase.boost,
        } }),
        .multi_phrase => |phrase| try dispatcher.func(dispatcher.ctx, alloc, req, .{ .multi_phrase = .{
            .field = phrase.field,
            .terms = phrase.terms,
            .max_edits = phrase.max_edits,
            .auto_fuzzy = phrase.auto_fuzzy,
            .boost = phrase.boost,
        } }),
        .term => |term| try dispatcher.func(dispatcher.ctx, alloc, req, .{ .term = .{
            .field = term.field,
            .term = term.term,
            .boost = term.boost,
        } }),
        .fuzzy => |fuzzy| try dispatcher.func(dispatcher.ctx, alloc, req, .{ .fuzzy = .{
            .field = fuzzy.field,
            .term = fuzzy.term,
            .max_edits = fuzzy.max_edits,
            .prefix_len = fuzzy.prefix_len,
            .auto_fuzzy = fuzzy.auto_fuzzy,
            .boost = fuzzy.boost,
        } }),
        .numeric_range => |range_query| try dispatcher.func(dispatcher.ctx, alloc, req, .{ .numeric_range = .{
            .field = range_query.field,
            .min = range_query.min,
            .max = range_query.max,
            .inclusive_min = range_query.inclusive_min,
            .inclusive_max = range_query.inclusive_max,
            .boost = range_query.boost,
        } }),
        .date_range => |range_query| try dispatcher.func(dispatcher.ctx, alloc, req, .{ .date_range = .{
            .field = range_query.field,
            .start_ns = range_query.start_ns,
            .end_ns = range_query.end_ns,
            .inclusive_start = range_query.inclusive_start,
            .inclusive_end = range_query.inclusive_end,
            .boost = range_query.boost,
        } }),
        .doc_id => |doc_id| try dispatcher.func(dispatcher.ctx, alloc, req, .{ .doc_id = .{
            .ids = doc_id.ids,
            .boost = doc_id.boost,
        } }),
        .bool_field => |bool_field| try dispatcher.func(dispatcher.ctx, alloc, req, .{ .bool_field = .{
            .field = bool_field.field,
            .value = bool_field.value,
            .boost = bool_field.boost,
        } }),
        .geo_distance => |geo_distance| try dispatcher.func(dispatcher.ctx, alloc, req, .{ .geo_distance = .{
            .field = geo_distance.field,
            .lon = geo_distance.lon,
            .lat = geo_distance.lat,
            .radius_meters = geo_distance.radius_meters,
            .boost = geo_distance.boost,
        } }),
        .geo_bbox => |geo_bbox| try dispatcher.func(dispatcher.ctx, alloc, req, .{ .geo_bbox = .{
            .field = geo_bbox.field,
            .min_lat = geo_bbox.min_lat,
            .min_lon = geo_bbox.min_lon,
            .max_lat = geo_bbox.max_lat,
            .max_lon = geo_bbox.max_lon,
            .boost = geo_bbox.boost,
        } }),
        .term_range => |range_query| try dispatcher.func(dispatcher.ctx, alloc, req, .{ .term_range = .{
            .field = range_query.field,
            .min = range_query.min,
            .max = range_query.max,
            .inclusive_min = range_query.inclusive_min,
            .inclusive_max = range_query.inclusive_max,
            .boost = range_query.boost,
        } }),
        .ip_range => |ip_range| try dispatcher.func(dispatcher.ctx, alloc, req, .{ .ip_range = .{
            .field = ip_range.field,
            .cidr = ip_range.cidr,
            .boost = ip_range.boost,
        } }),
        .geo_shape => |geo_shape| try dispatcher.func(dispatcher.ctx, alloc, req, .{ .geo_shape = .{
            .field = geo_shape.field,
            .relation = geo_shape.relation,
            .polygons = geo_shape.polygons,
            .boost = geo_shape.boost,
        } }),
        .match => |match| try dispatcher.func(dispatcher.ctx, alloc, req, .{ .match = .{
            .field = match.field,
            .text = match.text,
            .analyzer = match.analyzer,
            .boost = match.boost,
        } }),
        .match_phrase => |phrase| try dispatcher.func(dispatcher.ctx, alloc, req, .{ .match_phrase = .{
            .field = phrase.field,
            .text = phrase.text,
            .analyzer = phrase.analyzer,
            .max_edits = phrase.max_edits,
            .auto_fuzzy = phrase.auto_fuzzy,
            .boost = phrase.boost,
        } }),
        .prefix => |prefix| try dispatcher.func(dispatcher.ctx, alloc, req, .{ .prefix = .{
            .field = prefix.field,
            .prefix = prefix.prefix,
            .boost = prefix.boost,
        } }),
        .wildcard => |wildcard| try dispatcher.func(dispatcher.ctx, alloc, req, .{ .wildcard = .{
            .field = wildcard.field,
            .pattern = wildcard.pattern,
            .boost = wildcard.boost,
        } }),
        .regexp => |regexp| try dispatcher.func(dispatcher.ctx, alloc, req, .{ .regexp = .{
            .field = regexp.field,
            .pattern = regexp.pattern,
            .boost = regexp.boost,
        } }),
        else => unreachable,
    };
}

pub fn searchTextQuery(
    alloc: Allocator,
    req: types.SearchRequest,
    text_query: types.TextQuery,
    executor: SearchTextQueryExecutor,
) !types.SearchResult {
    const text_entry = (try executor.text_index_entry(executor.ctx, req.index_name)) orelse return switch (text_query) {
        .match_all => executor.search_match_all(executor.ctx, alloc, req),
        else => error.IndexNotFound,
    };
    const text_index = &text_entry.persistent;
    const chunk_backed = try executor.text_index_is_chunk_backed(executor.ctx, alloc, req.index_name);
    const group_chunk_parents = shouldGroupChunkParents(req, chunk_backed);
    const paging = componentPaging(req);

    var arena = std.heap.ArenaAllocator.init(alloc);
    defer arena.deinit();
    const arena_alloc = arena.allocator();
    const base_search_query = try textQueryToSearchQuery(arena_alloc, text_query, text_entry.text_analysis, text_entry.runtime_schema);
    const snapshot = text_index.snapshot();
    var constraint_req = req;
    constraint_req.resolved_doc_filter = null;
    var native_constraints = try deriveNativeDocIdConstraintsAlloc(alloc, constraint_req, .{
        .ctx = executor.ctx,
        .text_index_entry = executor.text_index_entry,
        .resolve_doc_set_doc_ids = executor.resolve_doc_set_doc_ids,
        .resolve_doc_ids_to_doc_set = executor.resolve_doc_ids_to_doc_set,
        .live_filter_doc_set = executor.live_filter_doc_set,
        .project_ordinals_to_doc_ids = false,
        .text_snapshot_for_doc_num_projection = snapshot,
        .apply_live_all_docs = !chunk_backed or snapshot.hasDocOrdinalCoverage(),
    });
    defer native_constraints.deinit(alloc);

    const collect_all_hits = group_chunk_parents;
    if (resolvedDocFilterFromRequest(req)) |filter| {
        try applyResolvedDocFilterToTextDocNumsAlloc(alloc, snapshot, &native_constraints, filter, .{
            .ctx = executor.ctx,
            .text_index_entry = executor.text_index_entry,
            .resolve_doc_set_doc_ids = executor.resolve_doc_set_doc_ids,
            .resolve_doc_ids_to_doc_set = executor.resolve_doc_ids_to_doc_set,
            .live_filter_doc_set = executor.live_filter_doc_set,
            .project_ordinals_to_doc_ids = false,
            .identity_read_generation = req.identity_read_generation,
        });
    }
    try convertNativeDocIdsToTextDocNumsAlloc(alloc, snapshot, &native_constraints);

    if (native_constraints.positive_filter and native_constraints.filter_doc_ids.len == 0 and native_constraints.filter_doc_nums.len == 0) {
        return executor.postprocess(executor.ctx, alloc, req, .{
            .alloc = alloc,
            .hits = &.{},
            .total_hits = 0,
            .graph_results = &.{},
        }, chunk_backed);
    }
    const search_query = try textSearchQueryWithNativeDocIdsAlloc(arena_alloc, base_search_query, native_constraints);

    var result = if (req.count_only)
        try search_mod.executeCountCandidates(alloc, snapshot, search_query)
    else
        try search_mod.execute(alloc, snapshot, .{
            .query = search_query,
            .k = if (collect_all_hits) @intCast(snapshot.global_doc_count) else paging.limit,
            .offset = if (collect_all_hits) 0 else paging.offset,
            .include_stored = req.include_stored,
            .distributed_text_stats = req.distributed_text_stats,
        });
    defer result.deinit();

    var hits = try alloc.alloc(types.SearchHit, result.hits.len);
    var initialized: usize = 0;
    var owns_hits = true;
    errdefer {
        if (owns_hits) {
            for (hits[0..initialized]) |*hit| hit.deinit(alloc);
            alloc.free(hits);
        }
    }

    for (result.hits, 0..) |hit, i| {
        const doc_ordinal = try snapshot.docOrdinal(hit.doc_id);
        const id = hit.id orelse {
            const stored = snapshot.storedDoc(hit.doc_id) orelse return error.StoredDocMissing;
            hits[i] = .{
                .id = try alloc.dupe(u8, stored.id),
                .doc_ordinal = doc_ordinal,
                .score = hit.score,
                .stored_data = null,
            };
            initialized += 1;
            continue;
        };

        hits[i] = .{
            .id = try alloc.dupe(u8, id),
            .doc_ordinal = doc_ordinal,
            .score = hit.score,
            .stored_data = if (req.include_stored and hit.stored_data != null)
                try executor.project_stored_search(executor.ctx, alloc, req, id, hit.stored_data.?)
            else
                null,
        };
        initialized += 1;
    }

    owns_hits = false;
    return try executor.postprocess(executor.ctx, alloc, req, .{
        .alloc = alloc,
        .hits = hits,
        .total_hits = result.total_hits,
        .total_hits_relation = switch (result.total_hits_relation) {
            .exact => .exact,
            .gte => .gte,
        },
        .graph_results = &.{},
    }, chunk_backed);
}

fn textSearchQueryWithNativeDocIdsAlloc(
    alloc: Allocator,
    base: search_mod.SearchQuery,
    constraints: NativeDocIdConstraints,
) !search_mod.SearchQuery {
    const has_include_doc_ids = constraints.positive_filter and constraints.filter_doc_ids.len > 0;
    const has_include_doc_nums = constraints.positive_filter and constraints.filter_doc_nums.len > 0;
    const has_exclude_doc_ids = constraints.exclude_doc_ids.len > 0;
    const has_exclude_doc_nums = constraints.exclude_doc_nums.len > 0;
    const has_include = has_include_doc_ids or has_include_doc_nums;
    const has_exclude = has_exclude_doc_ids or has_exclude_doc_nums;
    if (!has_include and !has_exclude) return base;

    const must_len: usize = 1 +
        @as(usize, if (has_include_doc_ids) 1 else 0) +
        @as(usize, if (has_include_doc_nums) 1 else 0);
    const must = try alloc.alloc(search_mod.SearchQuery, must_len);
    must[0] = base;
    var must_i: usize = 1;
    if (has_include_doc_ids) {
        must[must_i] = .{ .doc_id = .{ .ids = constraints.filter_doc_ids } };
        must_i += 1;
    }
    if (has_include_doc_nums) {
        must[must_i] = .{ .doc_num = .{ .ids = constraints.filter_doc_nums } };
        must_i += 1;
    }

    const must_not = if (has_exclude) blk: {
        const items_len: usize =
            @as(usize, if (has_exclude_doc_ids) 1 else 0) +
            @as(usize, if (has_exclude_doc_nums) 1 else 0);
        const items = try alloc.alloc(search_mod.SearchQuery, items_len);
        var i: usize = 0;
        if (has_exclude_doc_ids) {
            items[i] = .{ .doc_id = .{ .ids = constraints.exclude_doc_ids } };
            i += 1;
        }
        if (has_exclude_doc_nums) {
            items[i] = .{ .doc_num = .{ .ids = constraints.exclude_doc_nums } };
            i += 1;
        }
        break :blk items;
    } else &.{};

    return .{ .bool_query = .{
        .must = must,
        .must_not = must_not,
        .min_should = 0,
    } };
}

fn convertNativeDocIdsToTextDocNumsAlloc(
    alloc: Allocator,
    snapshot: *const index_mod.IndexSnapshot,
    constraints: *NativeDocIdConstraints,
) !void {
    if (constraints.filter_doc_ids.len > 0) {
        const mapped = try textDocNumsForDocIdsAlloc(alloc, snapshot, constraints.filter_doc_ids);
        if (constraints.filter_doc_nums.len > 0) {
            const intersected = try intersectDocNumsAlloc(alloc, constraints.filter_doc_nums, mapped);
            if (constraints.filter_doc_nums_owned and constraints.filter_doc_nums.len > 0) alloc.free(@constCast(constraints.filter_doc_nums));
            alloc.free(mapped);
            constraints.filter_doc_nums = intersected;
        } else {
            constraints.filter_doc_nums = mapped;
        }
        constraints.filter_doc_nums_owned = true;
        if (constraints.filter_doc_ids_owned) freeDocIdSlice(alloc, constraints.filter_doc_ids);
        constraints.filter_doc_ids = &.{};
        constraints.filter_doc_ids_owned = false;
    }

    if (constraints.exclude_doc_ids.len > 0) {
        const mapped = try textDocNumsForDocIdsAlloc(alloc, snapshot, constraints.exclude_doc_ids);
        if (constraints.exclude_doc_nums.len > 0) {
            const merged = try unionDocNumsAlloc(alloc, constraints.exclude_doc_nums, mapped);
            if (constraints.exclude_doc_nums_owned and constraints.exclude_doc_nums.len > 0) alloc.free(@constCast(constraints.exclude_doc_nums));
            alloc.free(mapped);
            constraints.exclude_doc_nums = merged;
        } else {
            constraints.exclude_doc_nums = mapped;
        }
        constraints.exclude_doc_nums_owned = true;
        if (constraints.exclude_doc_ids_owned) freeDocIdSlice(alloc, constraints.exclude_doc_ids);
        constraints.exclude_doc_ids = &.{};
        constraints.exclude_doc_ids_owned = false;
    }
}

fn textDocNumsForDocIdsAlloc(
    alloc: Allocator,
    snapshot: *const index_mod.IndexSnapshot,
    doc_ids: []const []const u8,
) ![]const u32 {
    var out = std.ArrayListUnmanaged(u32).empty;
    errdefer out.deinit(alloc);
    var doc_offset: u32 = 0;
    for (snapshot.segments) |*seg| {
        for (0..seg.reader.doc_count) |local_doc_usize| {
            const local_doc: u32 = @intCast(local_doc_usize);
            if (seg.deleted) |deleted| {
                if (deleted.contains(local_doc)) continue;
            }
            const stored = seg.reader.storedDoc(local_doc) orelse continue;
            if (!containsDocId(doc_ids, stored.id)) continue;
            try appendDocNum(alloc, &out, doc_offset + local_doc);
        }
        doc_offset += seg.reader.doc_count;
    }
    return try out.toOwnedSlice(alloc);
}

pub fn collectSearchRequestTextStats(
    alloc: Allocator,
    req: types.SearchRequest,
    executor: SearchTextStatsExecutor,
) ![]const distributed_stats_mod.TextFieldStats {
    var stats_map = std.StringHashMapUnmanaged(SearchRequestTextStatEntry){};
    defer {
        var it = stats_map.iterator();
        while (it.next()) |entry| {
            var term_it = entry.value_ptr.terms.keyIterator();
            while (term_it.next()) |term| alloc.free(term.*);
            entry.value_ptr.terms.deinit(alloc);
            alloc.free(entry.value_ptr.field);
            alloc.free(entry.key_ptr.*);
        }
        stats_map.deinit(alloc);
    }

    if (req.full_text_queries.len == 0) {
        if (req.full_text) |text_query| {
            const text_entry = (try executor.text_index_entry(executor.ctx, req.index_name)) orelse return &.{};
            try collectTextQueryTerms(alloc, &stats_map, req.index_name, text_query, text_entry.text_analysis, text_entry.runtime_schema);
        } else if (isTextQuery(req.query) and !isDefaultMatchAll(req.query)) {
            const text_entry = (try executor.text_index_entry(executor.ctx, req.index_name)) orelse return &.{};
            try collectQueryTerms(alloc, &stats_map, req.index_name, req.query, text_entry.text_analysis, text_entry.runtime_schema);
        }
    } else {
        for (req.full_text_queries) |item| {
            const text_entry = (try executor.text_index_entry(executor.ctx, item.index_name)) orelse continue;
            try collectTextQueryTerms(alloc, &stats_map, item.index_name, item.query, text_entry.text_analysis, text_entry.runtime_schema);
        }
    }

    if (req.filter_query_json.len > 0 or req.exclusion_query_json.len > 0) {
        const filter_index_name = req.primary_text_index_name orelse req.index_name;
        const text_entry = (try executor.text_index_entry(executor.ctx, filter_index_name)) orelse return &.{};
        if (req.filter_query_json.len > 0) {
            try collectPatternFilterQueryTerms(alloc, &stats_map, filter_index_name, req.filter_query_json, text_entry.text_analysis, text_entry.runtime_schema);
        }
        if (req.exclusion_query_json.len > 0) {
            try collectPatternFilterQueryTerms(alloc, &stats_map, filter_index_name, req.exclusion_query_json, text_entry.text_analysis, text_entry.runtime_schema);
        }
    }

    if (stats_map.count() == 0) return &.{};

    var requests = try alloc.alloc(ExplicitTextStatRequest, stats_map.count());
    defer {
        for (requests) |request| {
            for (request.terms) |term| alloc.free(term);
            if (request.terms.len > 0) alloc.free(request.terms);
        }
        alloc.free(requests);
    }

    var request_index: usize = 0;
    var it = stats_map.iterator();
    while (it.next()) |entry| {
        const terms = try alloc.alloc([]const u8, entry.value_ptr.terms.count());
        var term_index: usize = 0;
        var term_it = entry.value_ptr.terms.keyIterator();
        while (term_it.next()) |term| {
            terms[term_index] = try alloc.dupe(u8, term.*);
            term_index += 1;
        }
        requests[request_index] = .{
            .index_name = entry.value_ptr.index_name,
            .field = entry.value_ptr.field,
            .terms = terms,
        };
        request_index += 1;
    }

    return try collectExplicitTextStats(alloc, requests, executor);
}

pub fn collectExplicitTextStats(
    alloc: Allocator,
    requests: []const ExplicitTextStatRequest,
    executor: SearchTextStatsExecutor,
) ![]const distributed_stats_mod.TextFieldStats {
    if (requests.len == 0) return &.{};
    const out = try alloc.alloc(distributed_stats_mod.TextFieldStats, requests.len);
    var initialized: usize = 0;
    errdefer {
        for (out[0..initialized]) |*item| item.deinit(alloc);
        if (out.len > 0) alloc.free(out);
    }

    for (requests, 0..) |request, i| {
        const text_entry = (try executor.text_index_entry(executor.ctx, request.index_name)) orelse return error.IndexNotFound;
        const snapshot = text_entry.persistent.snapshot();
        if (request.resolved_doc_filter) |filter| {
            out[i] = try collectFilteredExplicitTextStats(alloc, snapshot, request, filter);
            initialized += 1;
            continue;
        }
        const term_doc_freqs = try alloc.alloc(distributed_stats_mod.TermDocFreq, request.terms.len);
        var initialized_terms: usize = 0;
        errdefer {
            for (term_doc_freqs[0..initialized_terms]) |*item| item.deinit(alloc);
            if (term_doc_freqs.len > 0) alloc.free(term_doc_freqs);
        }
        for (request.terms, 0..) |term, term_index| {
            term_doc_freqs[term_index] = .{
                .term = try alloc.dupe(u8, term),
                .doc_freq = try snapshot.termDocFreq(alloc, request.field, term),
            };
            initialized_terms += 1;
        }
        out[i] = .{
            .field = try alloc.dupe(u8, request.field),
            .global_doc_count = snapshot.global_doc_count,
            .global_total_field_len = snapshot.global_total_field_len.get(request.field) orelse 0,
            .term_doc_freqs = term_doc_freqs,
        };
        initialized += 1;
    }
    return out;
}

fn collectFilteredExplicitTextStats(
    alloc: Allocator,
    snapshot: *const index_mod.IndexSnapshot,
    request: ExplicitTextStatRequest,
    filter: *const doc_set.ResolvedDocFilter,
) !distributed_stats_mod.TextFieldStats {
    const term_doc_freqs = try alloc.alloc(distributed_stats_mod.TermDocFreq, request.terms.len);
    var initialized_terms: usize = 0;
    errdefer {
        for (term_doc_freqs[0..initialized_terms]) |*item| item.deinit(alloc);
        if (term_doc_freqs.len > 0) alloc.free(term_doc_freqs);
    }
    for (request.terms, 0..) |term, term_index| {
        term_doc_freqs[term_index] = .{
            .term = try alloc.dupe(u8, term),
            .doc_freq = 0,
        };
        initialized_terms += 1;
    }

    var global_doc_count: u32 = 0;
    var global_total_field_len: u64 = 0;
    var doc_offset: u32 = 0;
    for (snapshot.segments) |*seg| {
        for (0..seg.reader.doc_count) |local_doc_usize| {
            const local_doc: u32 = @intCast(local_doc_usize);
            if (seg.deleted) |deleted| {
                if (deleted.contains(local_doc)) continue;
            }
            const doc_id = doc_offset + local_doc;
            if (!(try docAllowedByResolvedFilter(snapshot, doc_id, filter))) continue;
            global_doc_count += 1;
            const stored = (try snapshot.storedDocDecompressed(doc_id)) orelse continue;
            defer alloc.free(stored.data);
            var parsed = std.json.parseFromSlice(std.json.Value, alloc, stored.data, .{}) catch continue;
            defer parsed.deinit();
            const value = extractJsonValueAtPath(parsed.value, request.field) orelse continue;
            global_total_field_len += try countAnalyzedTokensInJsonValue(alloc, value);

            var seen_terms = std.StringHashMap(void).init(alloc);
            defer {
                var it = seen_terms.keyIterator();
                while (it.next()) |key| alloc.free(key.*);
                seen_terms.deinit();
            }
            try collectSignificantTermsFromJsonValue(alloc, value, &seen_terms);
            for (term_doc_freqs) |*item| {
                if (seen_terms.contains(item.term)) item.doc_freq +|= 1;
            }
        }
        doc_offset += seg.reader.doc_count;
    }

    return .{
        .field = try alloc.dupe(u8, request.field),
        .global_doc_count = global_doc_count,
        .global_total_field_len = global_total_field_len,
        .term_doc_freqs = term_doc_freqs,
    };
}

fn docAllowedByResolvedFilter(
    snapshot: *const index_mod.IndexSnapshot,
    doc_id: u32,
    filter: *const doc_set.ResolvedDocFilter,
) !bool {
    return (try docSetContainsSnapshotDoc(snapshot, &filter.include, doc_id)) and
        !(try docSetContainsSnapshotDoc(snapshot, &filter.exclude, doc_id));
}

fn docSetContainsSnapshotDoc(
    snapshot: *const index_mod.IndexSnapshot,
    set: *const doc_set.ResolvedDocSet,
    doc_id: u32,
) !bool {
    return switch (set.*) {
        .all => true,
        .none => false,
        .ordinals, .ordinal_bitmap => blk: {
            const ordinal = (try snapshot.docOrdinal(doc_id)) orelse break :blk false;
            break :blk set.containsOrdinal(ordinal);
        },
        .doc_keys => |keys| blk: {
            const stored = snapshot.storedDoc(doc_id) orelse break :blk false;
            for (keys) |key| {
                if (std.mem.eql(u8, key, stored.id)) break :blk true;
            }
            break :blk false;
        },
    };
}

pub fn collectExplicitBackgroundTextStats(
    alloc: Allocator,
    requests: []const ExplicitBackgroundTextStatRequest,
    executor: SearchTextStatsExecutor,
) ![]const aggregations_mod.DistributedBackgroundTextStats {
    if (requests.len == 0) return &.{};
    const out = try alloc.alloc(aggregations_mod.DistributedBackgroundTextStats, requests.len);
    var initialized: usize = 0;
    errdefer {
        for (out[0..initialized]) |*item| item.deinit(alloc);
        if (out.len > 0) alloc.free(out);
    }

    for (requests, 0..) |request, i| {
        const text_entry = (try executor.text_index_entry(executor.ctx, request.index_name)) orelse return error.IndexNotFound;
        const snapshot = text_entry.persistent.snapshot();
        var background_result = try executeBackgroundQuery(alloc, snapshot, request.background_query);
        defer background_result.deinit();

        const term_doc_freqs = try alloc.alloc(distributed_stats_mod.TermDocFreq, request.terms.len);
        var initialized_terms: usize = 0;
        errdefer {
            for (term_doc_freqs[0..initialized_terms]) |*item| item.deinit(alloc);
            if (term_doc_freqs.len > 0) alloc.free(term_doc_freqs);
        }
        for (request.terms, 0..) |term, term_index| {
            term_doc_freqs[term_index] = .{
                .term = try alloc.dupe(u8, term),
                .doc_freq = 0,
            };
            initialized_terms += 1;
        }

        var background_doc_count: u32 = 0;
        for (background_result.hits) |hit| {
            if (request.resolved_doc_filter) |filter| {
                if (!(try docAllowedByResolvedFilter(snapshot, hit.doc_id, filter))) continue;
            }
            background_doc_count += 1;
            const stored = hit.stored_data orelse continue;
            var parsed = std.json.parseFromSlice(std.json.Value, alloc, stored, .{}) catch continue;
            defer parsed.deinit();
            const value = extractJsonValueAtPath(parsed.value, request.field) orelse continue;

            var seen_terms = std.StringHashMap(void).init(alloc);
            defer {
                var it = seen_terms.keyIterator();
                while (it.next()) |key| alloc.free(key.*);
                seen_terms.deinit();
            }
            try collectSignificantTermsFromJsonValue(alloc, value, &seen_terms);

            for (term_doc_freqs) |*item| {
                if (seen_terms.contains(item.term)) item.doc_freq +|= 1;
            }
        }

        out[i] = .{
            .aggregation_name = try alloc.dupe(u8, request.aggregation_name),
            .field = try alloc.dupe(u8, request.field),
            .background_doc_count = if (request.resolved_doc_filter != null) background_doc_count else background_result.total_hits,
            .term_doc_freqs = term_doc_freqs,
        };
        initialized += 1;
    }
    return out;
}

pub fn executeBackgroundQuery(
    alloc: Allocator,
    snapshot: *const @import("../../../index.zig").IndexSnapshot,
    query: aggregations_mod.BackgroundQuery,
) !search_mod.SearchResult {
    const request: search_mod.SearchRequest = .{
        .query = switch (query) {
            .match_all => .{ .match_all = {} },
            .match => |match| .{ .match = .{
                .field = match.field,
                .text = match.text,
            } },
            .term => |term| .{ .term = .{
                .field = term.field,
                .term = term.term,
            } },
        },
        .k = snapshot.global_doc_count,
        .include_stored = true,
    };
    return search_mod.execute(alloc, snapshot, request);
}

fn collectQueryTerms(
    alloc: Allocator,
    stats_map: *std.StringHashMapUnmanaged(SearchRequestTextStatEntry),
    index_name: ?[]const u8,
    query: types.Query,
    text_analysis: introducer_mod.TextAnalysisConfig,
    runtime_schema: ?runtime_schema_mod.TableSchema,
) !void {
    return switch (query) {
        .term => |term| try appendFieldTerm(alloc, stats_map, index_name, term.field, term.term),
        .match => |match| try appendAnalyzedTerms(alloc, stats_map, index_name, match.field, match.text, match.analyzer, text_analysis, runtime_schema),
        else => {},
    };
}

fn collectTextQueryTerms(
    alloc: Allocator,
    stats_map: *std.StringHashMapUnmanaged(SearchRequestTextStatEntry),
    index_name: ?[]const u8,
    query: types.TextQuery,
    text_analysis: introducer_mod.TextAnalysisConfig,
    runtime_schema: ?runtime_schema_mod.TableSchema,
) !void {
    return switch (query) {
        .term => |term| try appendFieldTerm(alloc, stats_map, index_name, term.field, term.term),
        .match => |match| try appendAnalyzedTerms(alloc, stats_map, index_name, match.field, match.text, match.analyzer, text_analysis, runtime_schema),
        .bool_query => |bool_query| {
            for (bool_query.must) |child| try collectTextQueryTerms(alloc, stats_map, index_name, child, text_analysis, runtime_schema);
            for (bool_query.should) |child| try collectTextQueryTerms(alloc, stats_map, index_name, child, text_analysis, runtime_schema);
            for (bool_query.must_not) |child| try collectTextQueryTerms(alloc, stats_map, index_name, child, text_analysis, runtime_schema);
        },
        else => {},
    };
}

fn appendAnalyzedTerms(
    alloc: Allocator,
    stats_map: *std.StringHashMapUnmanaged(SearchRequestTextStatEntry),
    index_name: ?[]const u8,
    field: []const u8,
    text: []const u8,
    analyzer_name: ?[]const u8,
    text_analysis: introducer_mod.TextAnalysisConfig,
    runtime_schema: ?runtime_schema_mod.TableSchema,
) !void {
    const analyzer = (try resolveQueryAnalyzer(field, analyzer_name, text_analysis, runtime_schema)) orelse &analysis_mod.default_analyzer;
    const tokens = try analyzer.analyze(alloc, text);
    defer analysis_mod.Analyzer.freeTokens(alloc, tokens);
    for (tokens) |token| {
        try appendFieldTerm(alloc, stats_map, index_name, field, token.term);
    }
}

fn appendFieldTerm(
    alloc: Allocator,
    stats_map: *std.StringHashMapUnmanaged(SearchRequestTextStatEntry),
    index_name: ?[]const u8,
    field: []const u8,
    term: []const u8,
) !void {
    const map_key = try textStatsMapKeyAlloc(alloc, index_name, field);
    errdefer alloc.free(map_key);

    const gop = try stats_map.getOrPut(alloc, map_key);
    if (!gop.found_existing) {
        gop.key_ptr.* = map_key;
        gop.value_ptr.* = .{
            .field = try alloc.dupe(u8, field),
            .index_name = index_name,
        };
    } else {
        alloc.free(map_key);
    }
    const term_gop = try gop.value_ptr.terms.getOrPut(alloc, term);
    if (!term_gop.found_existing) {
        term_gop.key_ptr.* = try alloc.dupe(u8, term);
    }
}

fn textStatsMapKeyAlloc(alloc: Allocator, index_name: ?[]const u8, field: []const u8) ![]u8 {
    if (index_name) |bound_index_name| {
        return try textStatsTupleKeyAlloc(alloc, &.{ "index", bound_index_name, field });
    }
    return try textStatsTupleKeyAlloc(alloc, &.{ "field", field });
}

fn textStatsTupleKeyAlloc(alloc: Allocator, components: []const []const u8) ![]u8 {
    var out = std.ArrayListUnmanaged(u8).empty;
    errdefer out.deinit(alloc);

    for (components) |component| {
        if (component.len > std.math.maxInt(u32)) return error.KeyComponentTooLarge;
        var len_buf: [@sizeOf(u32)]u8 = undefined;
        std.mem.writeInt(u32, &len_buf, @intCast(component.len), .big);
        try out.appendSlice(alloc, &len_buf);
        try out.appendSlice(alloc, component);
    }

    return try out.toOwnedSlice(alloc);
}

test "search request text stats keys preserve embedded separators" {
    const alloc = std.testing.allocator;

    var stats_map = std.StringHashMapUnmanaged(SearchRequestTextStatEntry){};
    defer {
        var it = stats_map.iterator();
        while (it.next()) |entry| {
            alloc.free(entry.key_ptr.*);
            alloc.free(entry.value_ptr.field);
            var term_it = entry.value_ptr.terms.keyIterator();
            while (term_it.next()) |term| alloc.free(term.*);
            entry.value_ptr.terms.deinit(alloc);
        }
        stats_map.deinit(alloc);
    }

    try appendFieldTerm(alloc, &stats_map, "idx\x1ffield", "name", "alpha");
    try appendFieldTerm(alloc, &stats_map, "idx", "field\x1fname", "beta");

    try std.testing.expectEqual(@as(u32, 2), stats_map.count());
}

fn collectPatternFilterQueryTerms(
    alloc: Allocator,
    stats_map: *std.StringHashMapUnmanaged(SearchRequestTextStatEntry),
    index_name: ?[]const u8,
    filter_query_json: []const u8,
    text_analysis: introducer_mod.TextAnalysisConfig,
    runtime_schema: ?runtime_schema_mod.TableSchema,
) !void {
    var parsed = std.json.parseFromSlice(std.json.Value, alloc, filter_query_json, .{}) catch return;
    defer parsed.deinit();
    try collectPatternFilterValueTerms(alloc, stats_map, index_name, parsed.value, text_analysis, runtime_schema);
}

fn collectPatternFilterValueTerms(
    alloc: Allocator,
    stats_map: *std.StringHashMapUnmanaged(SearchRequestTextStatEntry),
    index_name: ?[]const u8,
    value: std.json.Value,
    text_analysis: introducer_mod.TextAnalysisConfig,
    runtime_schema: ?runtime_schema_mod.TableSchema,
) !void {
    const object = switch (value) {
        .object => |object| object,
        else => return,
    };

    if (object.get("term")) |term_value| {
        try collectPatternFilterFieldStringTerms(alloc, stats_map, index_name, term_value, false, text_analysis, runtime_schema);
    }
    if (object.get("match")) |match_value| {
        try collectPatternFilterFieldStringTerms(alloc, stats_map, index_name, match_value, true, text_analysis, runtime_schema);
    }
    if (object.get("bool")) |bool_value| {
        const bool_object = switch (bool_value) {
            .object => |inner| inner,
            else => return,
        };
        for ([_][]const u8{ "must", "should", "must_not" }) |key| {
            const items = bool_object.get(key) orelse continue;
            const array = switch (items) {
                .array => |array| array,
                else => continue,
            };
            for (array.items) |item| {
                try collectPatternFilterValueTerms(alloc, stats_map, index_name, item, text_analysis, runtime_schema);
            }
        }
    }
}

fn collectPatternFilterFieldStringTerms(
    alloc: Allocator,
    stats_map: *std.StringHashMapUnmanaged(SearchRequestTextStatEntry),
    index_name: ?[]const u8,
    value: std.json.Value,
    analyze: bool,
    text_analysis: introducer_mod.TextAnalysisConfig,
    runtime_schema: ?runtime_schema_mod.TableSchema,
) !void {
    const object = switch (value) {
        .object => |object| object,
        else => return,
    };
    if (object.count() != 1) return;
    var it = object.iterator();
    const entry = it.next() orelse return;
    const text = switch (entry.value_ptr.*) {
        .string => |text| text,
        else => return,
    };
    if (analyze) {
        try appendAnalyzedTerms(alloc, stats_map, index_name, entry.key_ptr.*, text, null, text_analysis, runtime_schema);
    } else {
        try appendFieldTerm(alloc, stats_map, index_name, entry.key_ptr.*, text);
    }
}

fn extractJsonValueAtPath(root: std.json.Value, field_path: []const u8) ?std.json.Value {
    var current = root;
    var parts = std.mem.splitScalar(u8, field_path, '.');
    while (parts.next()) |part| {
        switch (current) {
            .object => |obj| current = obj.get(part) orelse return null,
            else => return null,
        }
    }
    return current;
}

fn collectSignificantTermsFromJsonValue(
    alloc: Allocator,
    value: std.json.Value,
    seen_terms: *std.StringHashMap(void),
) !void {
    switch (value) {
        .array => |arr| for (arr.items) |item| try collectSignificantTermsFromJsonValue(alloc, item, seen_terms),
        .string => {
            const tokens = try analysis_mod.default_analyzer.analyze(alloc, value.string);
            defer analysis_mod.Analyzer.freeTokens(alloc, tokens);
            for (tokens) |tok| {
                const entry = try seen_terms.getOrPut(tok.term);
                if (entry.found_existing) continue;
                entry.key_ptr.* = try alloc.dupe(u8, tok.term);
                entry.value_ptr.* = {};
            }
        },
        else => {},
    }
}

fn countAnalyzedTokensInJsonValue(alloc: Allocator, value: std.json.Value) !u64 {
    return switch (value) {
        .array => |arr| blk: {
            var total: u64 = 0;
            for (arr.items) |item| total += try countAnalyzedTokensInJsonValue(alloc, item);
            break :blk total;
        },
        .string => blk: {
            const tokens = try analysis_mod.default_analyzer.analyze(alloc, value.string);
            defer analysis_mod.Analyzer.freeTokens(alloc, tokens);
            break :blk @as(u64, @intCast(tokens.len));
        },
        else => 0,
    };
}

pub fn searchDense(
    alloc: Allocator,
    req: types.SearchRequest,
    dense: types.DenseKnnQuery,
    executor: DenseSearchExecutor,
) !types.SearchResult {
    var profile = DenseSearchProfile{};
    return try searchDenseInternal(alloc, req, dense, executor, &profile, false);
}

pub fn searchDenseProfiled(
    alloc: Allocator,
    req: types.SearchRequest,
    dense: types.DenseKnnQuery,
    executor: DenseSearchExecutor,
) !ProfiledDenseSearchResult {
    var profile = DenseSearchProfile{};
    return .{
        .result = try searchDenseInternal(alloc, req, dense, executor, &profile, true),
        .profile = profile,
    };
}

fn searchDenseInternal(
    alloc: Allocator,
    req: types.SearchRequest,
    dense: types.DenseKnnQuery,
    executor: DenseSearchExecutor,
    profile: *DenseSearchProfile,
    include_hbc_profile: bool,
) !types.SearchResult {
    const total_start = platform_time.monotonicNs();

    const index_lookup_start = total_start;
    const entry = (try executor.dense_index(executor.ctx, req.index_name)) orelse return error.IndexNotFound;
    profile.index_lookup_ns = platform_time.monotonicNs() - index_lookup_start;

    const chunk_backed = entry.chunk_name != null;
    const group_chunk_parents = shouldGroupChunkParents(req, chunk_backed);
    const paging = componentPaging(req);
    const index_stats = entry.index.stats();
    var native_constraints = try deriveNativeDenseConstraintsAlloc(alloc, req, executor, req.index_name orelse entry.config.name, true);
    defer native_constraints.deinit(alloc);
    const unresolved_stored_filters = hasStoredPatternFilters(req) and !native_constraints.resolved_stored_filters;
    const full_candidate_window = group_chunk_parents or unresolved_stored_filters;
    const effective_k: u32 = if (full_candidate_window)
        @intCast(index_stats.active_count)
    else
        @max(dense.k, paging.limit);
    const effort = resolvedSearchEffort(req.search_effort);
    const resolved_search_width = resolveSearchWidth(dense.k, effort, index_stats);
    const resolved_epsilon = resolveSearchEpsilon(effort);
    profile.resolved_search_width = resolved_search_width;
    profile.resolved_epsilon = resolved_epsilon;
    const bench_query_profile = shouldLogBenchQueryProfile();
    const collect_hbc_profile = include_hbc_profile or bench_query_profile;

    if (native_constraints.positive_filter and native_constraints.filter_ids.len == 0) {
        profile.returned_hit_count = 0;
        profile.total_ns = platform_time.monotonicNs() - total_start;
        return .{
            .alloc = alloc,
            .hits = &.{},
            .total_hits = 0,
            .graph_results = &.{},
        };
    }

    const effective_filter_ids = if (native_constraints.positive_filter) native_constraints.filter_ids else req.filter_ids;
    const effective_exclude_ids = if (native_constraints.exclude_ids.len > 0) native_constraints.exclude_ids else req.exclude_ids;
    const bounded_full_candidate_count: u32 = if (native_constraints.positive_filter)
        @intCast(@min(native_constraints.filter_ids.len, std.math.maxInt(u32)))
    else
        @intCast(index_stats.active_count);
    const hbc_effective_k: u32 = if (full_candidate_window) bounded_full_candidate_count else effective_k;

    const hbc_req: vectorindex_mod.SearchRequest = .{
        .query = dense.vector,
        .k = hbc_effective_k,
        .search_width = resolved_search_width,
        .epsilon = resolved_epsilon,
        .filter_prefix = req.filter_prefix,
        .distance_over = req.distance_over,
        .distance_under = req.distance_under,
        .filter_ids = effective_filter_ids,
        .exclude_ids = effective_exclude_ids,
    };

    const hbc_search_start = platform_time.monotonicNs();
    var results = if (collect_hbc_profile) blk: {
        const profiled = executor.hbc_search_profiled(executor.ctx, entry, hbc_req) catch |err| switch (err) {
            error.NotFound => {
                profile.returned_hit_count = 0;
                profile.total_ns = platform_time.monotonicNs() - total_start;
                return .{
                    .alloc = alloc,
                    .hits = &.{},
                    .total_hits = 0,
                    .graph_results = &.{},
                };
            },
            else => return err,
        };
        profile.hbc_runtime_txn_ns = profiled.profile.runtime_txn_ns;
        profile.hbc_scratch_acquire_ns = profiled.profile.scratch_acquire_ns;
        profile.hbc_node_cache_lookup_ns = profiled.profile.node_cache_lookup_ns;
        profile.hbc_quantized_cache_lookup_ns = profiled.profile.quantized_cache_lookup_ns;
        profile.hbc_nodes_visited = profiled.profile.nodes_visited;
        profile.hbc_leaves_explored = profiled.profile.leaves_explored;
        profile.hbc_approx_vectors_scored = profiled.profile.approx_vectors_scored;
        profile.hbc_exact_vectors_scored = profiled.profile.exact_vectors_scored;
        profile.hbc_reranked_vectors = profiled.profile.reranked_vectors;
        profile.hbc_approx_candidate_count = profiled.profile.approx_candidate_count;
        profile.hbc_rerank_candidate_count = profiled.profile.rerank_candidate_count;
        profile.hbc_ambiguous_top_k_pairs = profiled.profile.ambiguous_top_k_pairs;
        profile.hbc_ambiguous_boundary_pairs = profiled.profile.ambiguous_boundary_pairs;
        profile.hbc_ambiguous_distance_over_hits = profiled.profile.ambiguous_distance_over_hits;
        profile.hbc_ambiguous_distance_under_hits = profiled.profile.ambiguous_distance_under_hits;
        profile.hbc_full_rerank_due_to_threshold = profiled.profile.full_rerank_due_to_threshold;
        profile.hbc_top_k_count = profiled.profile.top_k_count;
        profile.hbc_min_distance_gap_top_k = profiled.profile.min_distance_gap_top_k;
        profile.hbc_min_interval_gap_top_k = profiled.profile.min_interval_gap_top_k;
        profile.hbc_closest_pair_top_k = if (profiled.profile.closest_pair_top_k) |pair| mapDebugPair(pair) else null;
        profile.hbc_boundary_pair = if (profiled.profile.boundary_pair) |pair| mapDebugPair(pair) else null;
        profile.hbc_boundary_tail_error_avg = profiled.profile.boundary_tail_error_avg;
        profile.hbc_boundary_tail_error_max = profiled.profile.boundary_tail_error_max;
        profile.hbc_boundary_tail_distance_gap_avg = profiled.profile.boundary_tail_distance_gap_avg;
        profile.hbc_boundary_tail_distance_gap_min = profiled.profile.boundary_tail_distance_gap_min;
        profile.hbc_boundary_tail_distance_gap_max = profiled.profile.boundary_tail_distance_gap_max;
        profile.hbc_boundary_tail_interval_gap_avg = profiled.profile.boundary_tail_interval_gap_avg;
        profile.hbc_boundary_tail_interval_gap_min = profiled.profile.boundary_tail_interval_gap_min;
        profile.hbc_boundary_tail_interval_gap_max = profiled.profile.boundary_tail_interval_gap_max;
        profile.hbc_approx_top_count = profiled.profile.approx_top_count;
        for (profiled.profile.approx_top, 0..) |hit, i| {
            profile.hbc_approx_top[i] = mapDebugHit(hit);
        }
        profile.hbc_rerank_external_score_ns = profiled.profile.rerank_vector_load_ns;
        profile.hbc_rerank_vector_load_ns = profiled.profile.rerank_vector_load_ns;
        profile.hbc_rerank_metadata_lookup_ns = profiled.profile.rerank_metadata_lookup_ns;
        profile.hbc_rerank_artifact_key_ns = profiled.profile.rerank_artifact_key_ns;
        profile.hbc_rerank_artifact_read_ns = profiled.profile.rerank_artifact_read_ns;
        profile.hbc_rerank_artifact_decode_ns = profiled.profile.rerank_artifact_decode_ns;
        profile.hbc_rerank_artifact_distance_ns = profiled.profile.rerank_artifact_distance_ns;
        profile.hbc_rerank_lsm_cache_hits = profiled.profile.rerank_lsm_cache_hits;
        profile.hbc_rerank_lsm_cache_misses = profiled.profile.rerank_lsm_cache_misses;
        profile.hbc_rerank_distance_ns = profiled.profile.rerank_distance_ns;
        break :blk profiled.results;
    } else executor.hbc_search(executor.ctx, entry, hbc_req) catch |err| switch (err) {
        error.NotFound => {
            profile.returned_hit_count = 0;
            profile.total_ns = platform_time.monotonicNs() - total_start;
            return .{
                .alloc = alloc,
                .hits = &.{},
                .total_hits = 0,
                .graph_results = &.{},
            };
        },
        else => return err,
    };
    profile.hbc_search_ns = platform_time.monotonicNs() - hbc_search_start;
    defer results.deinit();

    const raw_hits = results.getHits();
    profile.raw_hit_count = @intCast(raw_hits.len);
    const start: u32 = if (full_candidate_window) 0 else @min(paging.offset, @as(u32, @intCast(raw_hits.len)));
    const end: u32 = if (full_candidate_window) @intCast(raw_hits.len) else @min(start + paging.limit, @as(u32, @intCast(raw_hits.len)));

    var hits = std.ArrayListUnmanaged(types.SearchHit).empty;
    errdefer {
        for (hits.items) |*hit| hit.deinit(alloc);
        hits.deinit(alloc);
    }

    for (raw_hits[@intCast(start)..@intCast(end)], 0..) |hit, i| {
        const result_index: usize = @as(usize, @intCast(start)) + i;
        const resolve_start = platform_time.monotonicNs();
        const doc_key = if (results.takeMetadata(result_index)) |metadata| blk: {
            profile.inline_metadata_hits += 1;
            break :blk metadata;
        } else blk: {
            if (try entry.index.getMetadata(hit.vector_id)) |metadata| {
                profile.fetched_metadata_hits += 1;
                break :blk metadata;
            }
            const looked_up = (try executor.lookup_doc_key(
                executor.ctx,
                req.index_name orelse entry.config.name,
                hit.vector_id,
            )) orelse {
                profile.doc_key_resolve_ns += platform_time.monotonicNs() - resolve_start;
                continue;
            };
            profile.lookup_doc_key_hits += 1;
            break :blk looked_up;
        };
        profile.doc_key_resolve_ns += platform_time.monotonicNs() - resolve_start;
        var doc_key_owned = true;
        errdefer if (doc_key_owned) alloc.free(doc_key);

        var stored_data: ?[]u8 = null;
        var stored_data_owned = false;
        errdefer if (stored_data_owned) {
            if (stored_data) |data| alloc.free(data);
        };
        if (req.include_stored and !(chunk_backed and group_chunk_parents)) {
            const load_start = platform_time.monotonicNs();
            stored_data = try executor.load_projected_document(executor.ctx, alloc, req, doc_key);
            stored_data_owned = true;
            profile.load_projected_document_ns += platform_time.monotonicNs() - load_start;
        }
        try hits.append(alloc, .{
            .id = doc_key,
            .doc_ordinal = try lookupDenseHitDocOrdinal(alloc, req, executor, doc_key),
            .score = hit.distance,
            .stored_data = stored_data,
        });
        doc_key_owned = false;
        stored_data_owned = false;
    }

    const postprocess_start = platform_time.monotonicNs();
    var result = try executor.postprocess(executor.ctx, alloc, req, .{
        .alloc = alloc,
        .hits = try hits.toOwnedSlice(alloc),
        .total_hits = @intCast(hits.items.len),
        .graph_results = &.{},
    }, chunk_backed);
    errdefer result.deinit();
    if (hasStoredPatternFilters(req)) {
        result = try pageSearchResultInPlace(alloc, result, paging);
    }
    profile.postprocess_ns = platform_time.monotonicNs() - postprocess_start;
    profile.returned_hit_count = result.total_hits;
    profile.total_ns = platform_time.monotonicNs() - total_start;
    if (bench_query_profile) logBenchDenseQueryProfile(req, dense, index_stats, profile);
    return result;
}

fn shouldLogBenchQueryProfile() bool {
    const every = benchQueryProfileEvery() orelse return false;
    if (every == 0) return false;
    const current = bench_query_profile_counter.fetchAdd(1, .monotonic) + 1;
    return current % every == 0;
}

fn benchQueryProfileEvery() ?u64 {
    const cached = bench_query_profile_every_cache.load(.monotonic);
    if (cached != bench_query_profile_unknown) {
        return if (cached == bench_query_profile_disabled) null else cached;
    }
    const every = benchQueryProfileEveryUncached() orelse {
        bench_query_profile_every_cache.store(bench_query_profile_disabled, .monotonic);
        return null;
    };
    if (every >= bench_query_profile_disabled) {
        bench_query_profile_every_cache.store(bench_query_profile_disabled, .monotonic);
        return null;
    }
    bench_query_profile_every_cache.store(every, .monotonic);
    return every;
}

fn benchQueryProfileEveryUncached() ?u64 {
    const raw_z = getenv("ANTFLY_BENCH_QUERY_PROFILE_EVERY") orelse {
        const enabled = getenv("ANTFLY_BENCH_QUERY_PROFILE") orelse return null;
        if (std.mem.eql(u8, enabled, "0") or
            std.ascii.eqlIgnoreCase(enabled, "false") or
            std.ascii.eqlIgnoreCase(enabled, "no"))
        {
            return null;
        }
        return 100;
    };
    const raw = raw_z;
    if (raw.len == 0) return null;
    return std.fmt.parseUnsigned(u64, raw, 10) catch null;
}

fn lookupDenseHitDocOrdinal(
    alloc: Allocator,
    req: types.SearchRequest,
    executor: DenseSearchExecutor,
    doc_key: []const u8,
) !?doc_set.DocOrdinal {
    if (!denseHitPageNeedsDocOrdinals(req)) return null;
    const lookup = executor.lookup_doc_ordinal orelse return null;
    return try lookup(executor.ctx, alloc, doc_key, req.identity_read_generation);
}

fn denseHitPageNeedsDocOrdinals(req: types.SearchRequest) bool {
    return req.resolved_doc_filter != null or
        req.graph_queries.len != 0 or
        req.filter_doc_ids_positive or
        req.filter_doc_ids.len != 0 or
        req.exclude_doc_ids.len != 0 or
        hasStoredPatternFilters(req);
}

fn nsToUs(ns: u64) u64 {
    return ns / std.time.ns_per_us;
}

fn logBenchDenseQueryProfile(
    req: types.SearchRequest,
    dense: types.DenseKnnQuery,
    index_stats: vectorindex_mod.IndexStats,
    profile: *const DenseSearchProfile,
) void {
    const estimated_leaves = estimateLeafCount(index_stats);
    std.log.info(
        "antfly_bench_dense_query index={s} k={d} limit={d} offset={d} effort={d:.3} nodes={d} active={d} estimated_leaves={d} leaf_size={d} branching={d} search_width={d} epsilon={d:.3} total_us={d} index_lookup_us={d} hbc_us={d} doc_key_us={d} load_projected_us={d} postprocess_us={d} raw_hits={d} returned_hits={d}",
        .{
            req.index_name orelse "",
            dense.k,
            req.limit,
            req.offset,
            req.search_effort orelse @as(f32, -1.0),
            index_stats.node_count,
            index_stats.active_count,
            estimated_leaves,
            index_stats.leaf_size,
            index_stats.branching_factor,
            profile.resolved_search_width,
            profile.resolved_epsilon,
            nsToUs(profile.total_ns),
            nsToUs(profile.index_lookup_ns),
            nsToUs(profile.hbc_search_ns),
            nsToUs(profile.doc_key_resolve_ns),
            nsToUs(profile.load_projected_document_ns),
            nsToUs(profile.postprocess_ns),
            profile.raw_hit_count,
            profile.returned_hit_count,
        },
    );
    std.log.info(
        "antfly_bench_dense_query_hbc index={s} nodes_visited={d} leaves={d} approx_vectors={d} exact_vectors={d} reranked={d} approx_candidates={d} rerank_candidates={d} ambiguous_top_k={d} ambiguous_boundary={d} distance_over_hits={d} distance_under_hits={d} full_rerank={any} top_k_count={d} min_distance_gap={d:.6} min_interval_gap={d:.6} rerank_vector_load_us={d} rerank_metadata_us={d} rerank_artifact_key_us={d} rerank_artifact_read_us={d} rerank_artifact_decode_us={d} rerank_artifact_distance_us={d} rerank_lsm_cache_hits={d} rerank_lsm_cache_misses={d} rerank_distance_us={d} inline_meta={d} fetched_meta={d} lookup_doc_key={d}",
        .{
            req.index_name orelse "",
            profile.hbc_nodes_visited,
            profile.hbc_leaves_explored,
            profile.hbc_approx_vectors_scored,
            profile.hbc_exact_vectors_scored,
            profile.hbc_reranked_vectors,
            profile.hbc_approx_candidate_count,
            profile.hbc_rerank_candidate_count,
            profile.hbc_ambiguous_top_k_pairs,
            profile.hbc_ambiguous_boundary_pairs,
            profile.hbc_ambiguous_distance_over_hits,
            profile.hbc_ambiguous_distance_under_hits,
            profile.hbc_full_rerank_due_to_threshold,
            profile.hbc_top_k_count,
            profile.hbc_min_distance_gap_top_k,
            profile.hbc_min_interval_gap_top_k,
            nsToUs(profile.hbc_rerank_vector_load_ns),
            nsToUs(profile.hbc_rerank_metadata_lookup_ns),
            nsToUs(profile.hbc_rerank_artifact_key_ns),
            nsToUs(profile.hbc_rerank_artifact_read_ns),
            nsToUs(profile.hbc_rerank_artifact_decode_ns),
            nsToUs(profile.hbc_rerank_artifact_distance_ns),
            profile.hbc_rerank_lsm_cache_hits,
            profile.hbc_rerank_lsm_cache_misses,
            nsToUs(profile.hbc_rerank_distance_ns),
            profile.inline_metadata_hits,
            profile.fetched_metadata_hits,
            profile.lookup_doc_key_hits,
        },
    );
}

fn mapDebugHit(hit: vectorindex_mod.DebugHit) DenseSearchProfile.DebugHit {
    return .{
        .id = hit.id,
        .distance = hit.distance,
        .error_bound = hit.error_bound,
        .lower_bound = hit.lower_bound,
        .upper_bound = hit.upper_bound,
    };
}

fn mapDebugPair(pair: vectorindex_mod.DebugPair) DenseSearchProfile.DebugPair {
    return .{
        .left = mapDebugHit(pair.left),
        .right = mapDebugHit(pair.right),
        .distance_gap = pair.distance_gap,
        .interval_gap = pair.interval_gap,
        .overlaps = pair.overlaps,
    };
}

pub fn normalizedSearchEffort(effort: ?f32) ?f32 {
    const value = effort orelse return null;
    if (std.math.isNan(value)) return null;
    if (value < 0) return 0;
    if (value > 1) return 1;
    return value;
}

pub fn resolvedSearchEffort(effort: ?f32) f32 {
    return normalizedSearchEffort(effort) orelse default_balanced_search_effort;
}

fn estimateLeafCount(stats: vectorindex_mod.IndexStats) u32 {
    if (stats.active_count == 0) return 0;
    const leaf_size = @max(stats.leaf_size, 1);
    const estimated = (stats.active_count + leaf_size - 1) / leaf_size;
    return @intCast(@min(estimated, @as(u64, std.math.maxInt(u32))));
}

pub fn resolveSearchWidth(k: u32, effort: f32, stats: vectorindex_mod.IndexStats) u32 {
    const min_width = @max(k, @as(u32, 64));
    const legacy_max_width = @max(min_width * 20, @as(u32, 4096));
    const legacy_balanced_width = min_width + @as(u32, @intFromFloat(@as(f32, @floatFromInt(legacy_max_width - min_width)) * default_balanced_search_effort));
    const estimated_leaf_count = estimateLeafCount(stats);
    const max_width = if (estimated_leaf_count > 0)
        @max(min_width, @max(estimated_leaf_count, if (stats.node_count > 0 and stats.node_count <= std.math.maxInt(u32)) @as(u32, @intCast(stats.node_count)) else estimated_leaf_count))
    else if (stats.node_count > legacy_max_width and stats.node_count <= std.math.maxInt(u32))
        @as(u32, @intCast(stats.node_count))
    else
        legacy_max_width;
    const balanced_cap = if (estimated_leaf_count > 0) @max(min_width, estimated_leaf_count) else max_width;
    const leaf_balanced_width = min_width + @as(u32, @intFromFloat(@as(f32, @floatFromInt(balanced_cap - min_width)) * default_balanced_search_effort));
    const balanced_width = @min(legacy_balanced_width, leaf_balanced_width);

    if (effort <= default_balanced_search_effort) {
        if (balanced_width <= min_width) return min_width;
        const ratio = effort / default_balanced_search_effort;
        return min_width + @as(u32, @intFromFloat(@as(f32, @floatFromInt(balanced_width - min_width)) * ratio));
    }

    if (max_width <= balanced_width) return max_width;
    const ratio = (effort - default_balanced_search_effort) / (1 - default_balanced_search_effort);
    const width = balanced_width + @as(u32, @intFromFloat(@as(f32, @floatFromInt(max_width - balanced_width)) * ratio));
    return @min(width, max_width);
}

pub fn resolveSearchEpsilon(effort: f32) f32 {
    if (effort < default_balanced_search_effort) {
        return 1.0 + (effort * 12.0);
    }
    return 7.0 + ((effort - default_balanced_search_effort) * 186.0);
}

fn sparseHitParentOrdinal(
    alloc: Allocator,
    executor: SparseSearchExecutor,
    hit_doc_id: []const u8,
    generation: ?u64,
) !?doc_set.DocOrdinal {
    const lookup = executor.lookup_doc_ordinal orelse return null;
    if (!internal_keys.isChunkArtifactRecordKey(hit_doc_id)) return null;
    const parent_doc_id = (try internal_keys.decodeDocumentComponentAlloc(alloc, hit_doc_id)) orelse return null;
    defer alloc.free(parent_doc_id);
    return try lookup(executor.ctx, alloc, parent_doc_id, generation);
}

fn sparseHitOrdinal(
    alloc: Allocator,
    executor: SparseSearchExecutor,
    hit_doc_id: []const u8,
    generation: ?u64,
) !?doc_set.DocOrdinal {
    const lookup = executor.lookup_doc_ordinal orelse return null;
    return try lookup(executor.ctx, alloc, hit_doc_id, generation);
}

pub fn searchSparse(
    alloc: Allocator,
    req: types.SearchRequest,
    sparse: types.SparseKnnQuery,
    executor: SparseSearchExecutor,
) !types.SearchResult {
    const entry = (try executor.sparse_index(executor.ctx, req.index_name)) orelse return error.IndexNotFound;
    const chunk_backed = entry.chunk_name != null;
    const group_chunk_parents = shouldGroupChunkParents(req, chunk_backed);
    const paging = componentPaging(req);
    var native_constraints = try deriveNativeDocIdConstraintsAlloc(alloc, req, .{
        .ctx = executor.ctx,
        .text_index_entry = executor.text_index_entry,
        .resolve_doc_set_doc_ids = executor.resolve_doc_set_doc_ids,
        .resolve_doc_ids_to_doc_set = executor.resolve_doc_ids_to_doc_set,
        .live_filter_doc_set = executor.live_filter_doc_set,
        .lookup_doc_nums_for_ordinals = executor.lookup_doc_nums_for_ordinals,
        .doc_num_index_name = req.index_name orelse entry.config.name,
        .require_doc_num_projection_mapper = true,
        .project_ordinals_to_doc_ids = false,
        .apply_live_all_docs = true,
    });
    defer native_constraints.deinit(alloc);
    const unresolved_stored_filters = hasStoredPatternFilters(req) and !native_constraints.resolved_stored_filters;
    const full_candidate_window = group_chunk_parents or unresolved_stored_filters;
    const effective_k: u32 = if (full_candidate_window)
        @intCast(entry.index.next_doc_num)
    else
        @max(sparse.k, paging.limit);
    const query = sparse_mod.SparseVector{
        .indices = sparse.indices,
        .values = sparse.values,
    };
    if (native_constraints.positive_filter and native_constraints.filter_doc_ids.len == 0 and native_constraints.filter_doc_nums.len == 0) {
        return .{
            .alloc = alloc,
            .hits = &.{},
            .total_hits = 0,
            .graph_results = &.{},
        };
    }
    const raw_hits = try entry.index.searchConstrained(alloc, &query, effective_k, .{
        .filter_doc_ids = native_constraints.filter_doc_ids,
        .exclude_doc_ids = native_constraints.exclude_doc_ids,
        .filter_doc_nums = native_constraints.filter_doc_nums,
        .exclude_doc_nums = native_constraints.exclude_doc_nums,
    });
    defer sparse_mod.SparseIndex.freeResults(alloc, raw_hits);

    const start: u32 = if (full_candidate_window) 0 else @min(paging.offset, @as(u32, @intCast(raw_hits.len)));
    const end: u32 = if (full_candidate_window) @intCast(raw_hits.len) else @min(start + paging.limit, @as(u32, @intCast(raw_hits.len)));

    var hits = try alloc.alloc(types.SearchHit, end - start);
    var initialized: usize = 0;
    var owns_hits = true;
    errdefer {
        if (owns_hits) {
            for (hits[0..initialized]) |*hit| hit.deinit(alloc);
            alloc.free(hits);
        }
    }

    for (raw_hits[@intCast(start)..@intCast(end)], 0..) |hit, i| {
        hits[i] = .{
            .id = try alloc.dupe(u8, hit.doc_id),
            .doc_ordinal = if (chunk_backed)
                try sparseHitParentOrdinal(alloc, executor, hit.doc_id, req.identity_read_generation)
            else
                try sparseHitOrdinal(alloc, executor, hit.doc_id, req.identity_read_generation),
            .score = hit.score,
            .stored_data = if (req.include_stored and !(chunk_backed and group_chunk_parents))
                try executor.load_projected_document(executor.ctx, alloc, req, hit.doc_id)
            else
                null,
        };
        initialized += 1;
    }

    owns_hits = false;
    var result = try executor.postprocess(executor.ctx, alloc, req, .{
        .alloc = alloc,
        .hits = hits,
        .total_hits = @intCast(raw_hits.len),
        .graph_results = &.{},
    }, chunk_backed);
    errdefer result.deinit();
    if (hasStoredPatternFilters(req)) {
        result = try pageSearchResultInPlace(alloc, result, paging);
    }
    return result;
}

fn pageSearchResultInPlace(
    alloc: Allocator,
    result: types.SearchResult,
    paging: ComponentPaging,
) !types.SearchResult {
    var owned = result;
    const total: u32 = @intCast(owned.hits.len);
    const start = @min(paging.offset, total);
    const end = @min(start + paging.limit, total);
    const start_usize: usize = @intCast(start);
    const end_usize: usize = @intCast(end);
    const page_len = end_usize - start_usize;

    var paged_hits: []types.SearchHit = if (page_len > 0)
        try alloc.alloc(types.SearchHit, page_len)
    else
        &[_]types.SearchHit{};
    var initialized: usize = 0;
    errdefer {
        for (paged_hits[0..initialized]) |*hit| hit.deinit(alloc);
        if (paged_hits.len > 0) alloc.free(paged_hits);
    }

    for (owned.hits, 0..) |*hit, i| {
        if (i >= start_usize and i < end_usize) {
            paged_hits[initialized] = hit.*;
            hit.* = undefined;
            initialized += 1;
        } else {
            hit.deinit(alloc);
        }
    }

    if (owned.hits.len > 0) alloc.free(owned.hits);
    owned.hits = paged_hits;
    owned.total_hits = total;
    return owned;
}

pub fn searchMatchAll(
    alloc: Allocator,
    req: types.SearchRequest,
    executor: MatchAllExecutor,
) !types.SearchResult {
    var candidates = try executor.collect_candidates(executor.ctx, alloc, req);
    defer candidates.deinit(alloc);

    var native_constraints = try deriveNativeDocIdConstraintsAlloc(alloc, req, .{
        .ctx = executor.ctx,
        .text_index_entry = executor.text_index_entry,
        .resolve_doc_set_doc_ids = executor.resolve_doc_set_doc_ids,
        .resolve_doc_ids_to_doc_set = executor.resolve_doc_ids_to_doc_set,
        .live_filter_doc_set = executor.live_filter_doc_set,
        .project_ordinals_to_doc_ids = false,
        .apply_live_all_docs = true,
    });
    defer native_constraints.deinit(alloc);
    try applyMatchAllDocIdConstraintsAlloc(alloc, &candidates, &native_constraints);

    const paging = componentPaging(req);

    const total_hits: u32 = @intCast(candidates.items.len);
    const start = @min(paging.offset, total_hits);
    const end = @min(start + paging.limit, total_hits);
    const start_usize: usize = @intCast(start);
    const end_usize: usize = @intCast(end);

    var hits = try alloc.alloc(types.SearchHit, end_usize - start_usize);
    errdefer alloc.free(hits);

    for (candidates.items, 0..) |*candidate, i| {
        if (i < start_usize or i >= end_usize) continue;
        hits[i - start_usize] = .{
            .id = candidate.id,
            .doc_ordinal = candidate.ordinal,
            .score = 1.0,
            .stored_data = if (req.include_stored)
                try executor.load_projected_document(executor.ctx, alloc, req, candidate.id)
            else
                null,
        };
        candidate.id = @constCast(&[_]u8{});
    }

    return .{
        .alloc = alloc,
        .hits = hits,
        .total_hits = total_hits,
        .graph_results = &.{},
    };
}

fn applyMatchAllDocIdConstraintsAlloc(
    alloc: Allocator,
    candidates: *MatchAllCandidates,
    constraints: *const NativeDocIdConstraints,
) !void {
    if (!constraints.positive_filter and constraints.exclude_doc_ids.len == 0 and constraints.exclude_doc_nums.len == 0) return;

    var keep_count: usize = 0;
    for (candidates.items) |candidate| {
        if (matchAllCandidateAllowed(candidate, constraints)) keep_count += 1;
    }
    if (keep_count == candidates.items.len) return;

    const filtered = try alloc.alloc(MatchAllCandidate, keep_count);
    var initialized: usize = 0;
    errdefer {
        for (filtered[0..initialized]) |*candidate| candidate.deinit(alloc);
        if (filtered.len > 0) alloc.free(filtered);
    }

    for (candidates.items) |*candidate| {
        if (matchAllCandidateAllowed(candidate.*, constraints)) {
            filtered[initialized] = candidate.*;
            candidate.id = @constCast(&[_]u8{});
            initialized += 1;
        } else {
            candidate.deinit(alloc);
        }
    }

    alloc.free(candidates.items);
    candidates.items = filtered;
}

fn matchAllCandidateAllowed(
    candidate: MatchAllCandidate,
    constraints: *const NativeDocIdConstraints,
) bool {
    if (constraints.positive_filter) {
        if (constraints.filter_doc_ids.len == 0 and constraints.filter_doc_nums.len == 0) return false;
        if (constraints.filter_doc_ids.len > 0 and !containsDocId(constraints.filter_doc_ids, candidate.id)) return false;
        if (constraints.filter_doc_nums.len > 0) {
            const ordinal = candidate.ordinal orelse return false;
            if (!containsDocNum(constraints.filter_doc_nums, ordinal)) return false;
        }
    }
    if (containsDocId(constraints.exclude_doc_ids, candidate.id)) return false;
    if (candidate.ordinal) |ordinal| {
        if (containsDocNum(constraints.exclude_doc_nums, ordinal)) return false;
    }
    return true;
}

pub fn collectMatchAllCandidates(
    alloc: Allocator,
    req: types.SearchRequest,
    collector: MatchAllCandidateCollector,
) !MatchAllCandidates {
    _ = req.index_name;

    const lower = try internal_keys.documentRangeLowerAlloc(alloc, "");
    defer alloc.free(lower);

    const docs = try collector.scan_store_range(collector.ctx, alloc, lower, "");
    defer docstore_mod.DocStore.freeResults(alloc, docs);

    var candidates = std.ArrayListUnmanaged(MatchAllCandidate).empty;
    var seen = std.StringHashMapUnmanaged(void){};
    defer seen.deinit(alloc);
    errdefer {
        for (candidates.items) |*item| item.deinit(alloc);
        candidates.deinit(alloc);
    }

    for (docs) |doc| {
        if (!internal_keys.isPrimaryDocumentKey(doc.key)) continue;
        const raw_key = (try internal_keys.decodePrimaryDocumentKeyAlloc(alloc, doc.key)) orelse continue;
        errdefer alloc.free(raw_key);
        if (try collector.is_expired_key(collector.ctx, alloc, raw_key)) {
            alloc.free(raw_key);
            continue;
        }
        if (seen.contains(raw_key)) {
            alloc.free(raw_key);
            continue;
        }
        const ordinal = if (collector.lookup_doc_ordinal) |lookup|
            try lookup(collector.ctx, alloc, raw_key, req.identity_read_generation)
        else
            null;
        try seen.put(alloc, raw_key, {});
        try candidates.append(alloc, .{
            .id = raw_key,
            .ordinal = ordinal,
        });
    }

    return .{ .items = try candidates.toOwnedSlice(alloc) };
}

pub fn textQueryToSearchQuery(
    alloc: Allocator,
    text_query: types.TextQuery,
    text_analysis: introducer_mod.TextAnalysisConfig,
    runtime_schema: ?runtime_schema_mod.TableSchema,
) anyerror!search_mod.SearchQuery {
    return switch (text_query) {
        .match_none => .{ .match_none = {} },
        .match_all => .{ .match_all = {} },
        .phrase => |phrase| .{ .term_phrase = .{
            .field = phrase.field,
            .terms = phrase.terms,
            .max_edits = phrase.max_edits,
            .auto_fuzzy = phrase.auto_fuzzy,
            .boost = phrase.boost,
        } },
        .multi_phrase => |phrase| .{ .multi_phrase = .{
            .field = phrase.field,
            .terms = phrase.terms,
            .max_edits = phrase.max_edits,
            .auto_fuzzy = phrase.auto_fuzzy,
            .boost = phrase.boost,
        } },
        .term => |term| .{ .term = .{
            .field = term.field,
            .term = term.term,
            .boost = term.boost,
        } },
        .fuzzy => |fuzzy| .{ .fuzzy = .{
            .field = fuzzy.field,
            .term = fuzzy.term,
            .max_edits = fuzzy.max_edits,
            .prefix_len = fuzzy.prefix_len,
            .auto_fuzzy = fuzzy.auto_fuzzy,
            .boost = fuzzy.boost,
        } },
        .numeric_range => |range_query| .{ .numeric_range = .{
            .field = range_query.field,
            .min = range_query.min,
            .max = range_query.max,
            .inclusive_min = range_query.inclusive_min,
            .inclusive_max = range_query.inclusive_max,
            .boost = range_query.boost,
        } },
        .date_range => |range_query| .{ .date_range = .{
            .field = range_query.field,
            .start_ns = range_query.start_ns,
            .end_ns = range_query.end_ns,
            .inclusive_start = range_query.inclusive_start,
            .inclusive_end = range_query.inclusive_end,
            .boost = range_query.boost,
        } },
        .doc_id => |doc_id| .{ .doc_id = .{
            .ids = doc_id.ids,
            .boost = doc_id.boost,
        } },
        .bool_field => |bool_field| .{ .bool_field = .{
            .field = bool_field.field,
            .value = bool_field.value,
            .boost = bool_field.boost,
        } },
        .geo_distance => |geo_distance| .{ .geo_distance = .{
            .field = geo_distance.field,
            .center = .{ .lon = geo_distance.lon, .lat = geo_distance.lat },
            .radius_meters = geo_distance.radius_meters,
            .boost = geo_distance.boost,
        } },
        .geo_bbox => |geo_bbox| .{ .geo_bbox = .{
            .field = geo_bbox.field,
            .min_lat = geo_bbox.min_lat,
            .min_lon = geo_bbox.min_lon,
            .max_lat = geo_bbox.max_lat,
            .max_lon = geo_bbox.max_lon,
            .boost = geo_bbox.boost,
        } },
        .term_range => |range_query| .{ .term_range = .{
            .field = range_query.field,
            .min = range_query.min,
            .max = range_query.max,
            .inclusive_min = range_query.inclusive_min,
            .inclusive_max = range_query.inclusive_max,
            .boost = range_query.boost,
        } },
        .ip_range => |ip_range| .{ .ip_range = .{
            .field = ip_range.field,
            .cidr = ip_range.cidr,
            .boost = ip_range.boost,
        } },
        .geo_shape => |geo_shape| .{ .geo_shape = .{
            .field = geo_shape.field,
            .relation = switch (geo_shape.relation) {
                .intersects => .intersects,
                .within => .within,
                .contains => .contains,
            },
            .polygons = try geoPointPolygonsToSearchPolygons(alloc, geo_shape.polygons),
            .boost = geo_shape.boost,
        } },
        .match => |match| .{ .match = .{
            .field = match.field,
            .text = match.text,
            .analyzer = try resolveQueryAnalyzer(match.field, match.analyzer, text_analysis, runtime_schema),
            .boost = match.boost,
        } },
        .match_phrase => |phrase| .{ .phrase = .{
            .field = phrase.field,
            .text = phrase.text,
            .analyzer = try resolveQueryAnalyzer(phrase.field, phrase.analyzer, text_analysis, runtime_schema),
            .max_edits = phrase.max_edits,
            .auto_fuzzy = phrase.auto_fuzzy,
            .boost = phrase.boost,
        } },
        .prefix => |prefix| .{ .prefix = .{
            .field = prefix.field,
            .prefix = prefix.prefix,
            .boost = prefix.boost,
        } },
        .wildcard => |wildcard| .{ .wildcard = .{
            .field = wildcard.field,
            .pattern = wildcard.pattern,
            .boost = wildcard.boost,
        } },
        .regexp => |regexp| .{ .regexp = .{
            .field = regexp.field,
            .pattern = regexp.pattern,
            .boost = regexp.boost,
        } },
        .bool_query => |bool_query| .{ .bool_query = .{
            .must = try textQuerySliceToSearchQuerySlice(alloc, bool_query.must, text_analysis, runtime_schema),
            .should = try textQuerySliceToSearchQuerySlice(alloc, bool_query.should, text_analysis, runtime_schema),
            .must_not = try textQuerySliceToSearchQuerySlice(alloc, bool_query.must_not, text_analysis, runtime_schema),
            .min_should = bool_query.min_should,
            .boost = bool_query.boost,
        } },
    };
}

fn textQuerySliceToSearchQuerySlice(
    alloc: Allocator,
    items: []const types.TextQuery,
    text_analysis: introducer_mod.TextAnalysisConfig,
    runtime_schema: ?runtime_schema_mod.TableSchema,
) anyerror![]search_mod.SearchQuery {
    if (items.len == 0) return &.{};
    var out = try alloc.alloc(search_mod.SearchQuery, items.len);
    errdefer alloc.free(out);
    for (items, 0..) |item, i| {
        out[i] = try textQueryToSearchQuery(alloc, item, text_analysis, runtime_schema);
    }
    return out;
}

fn resolveQueryAnalyzer(
    field: []const u8,
    analyzer_name: ?[]const u8,
    text_analysis: introducer_mod.TextAnalysisConfig,
    runtime_schema: ?runtime_schema_mod.TableSchema,
) !?*const analysis_mod.Analyzer {
    if (analyzer_name) |name| return try resolveAnalyzerName(name, text_analysis);
    if (resolveConfiguredFieldAnalyzerName(text_analysis, field)) |configured_analyzer| {
        return try resolveAnalyzerName(configured_analyzer, text_analysis);
    }
    if (runtime_schema) |schema| {
        if (resolveIndexedFieldAnalyzer(schema, field)) |schema_analyzer| {
            return try resolveAnalyzerName(schema_analyzer, text_analysis);
        }
    }
    return null;
}

fn resolveConfiguredFieldAnalyzerName(text_analysis: introducer_mod.TextAnalysisConfig, field: []const u8) ?[]const u8 {
    var resolved: ?[]const u8 = null;
    for (text_analysis.field_analyzers) |item| {
        if (!std.mem.eql(u8, item.field_name, field)) continue;
        if (resolved == null) {
            resolved = item.analyzer_name;
        } else if (!std.mem.eql(u8, resolved.?, item.analyzer_name)) {
            return null;
        }
    }
    return resolved;
}

fn resolveIndexedFieldAnalyzer(schema: runtime_schema_mod.TableSchema, field: []const u8) ?[]const u8 {
    if (resolveExplicitFieldAnalyzer(schema, field)) |analyzer| return analyzer;
    if (resolveDynamicRuleFieldAnalyzer(schema, field)) |analyzer| return analyzer;
    if (resolveDynamicTemplateFieldAnalyzer(schema, field)) |analyzer| return analyzer;
    if (fallsUnderDynamicTextPath(schema, field)) return "standard";
    return null;
}

fn resolveExplicitFieldAnalyzer(schema: runtime_schema_mod.TableSchema, field: []const u8) ?[]const u8 {
    var resolved: ?[]const u8 = null;
    for (schema.full_text_documents) |document_schema| {
        for (document_schema.fields) |runtime_field| {
            if (!std.mem.eql(u8, runtime_field.emitted_name, field)) continue;
            if (resolved == null) {
                resolved = runtime_field.analyzer;
            } else if (!std.mem.eql(u8, resolved.?, runtime_field.analyzer)) {
                return null;
            }
        }
    }
    return resolved;
}

fn resolveDynamicRuleFieldAnalyzer(schema: runtime_schema_mod.TableSchema, field: []const u8) ?[]const u8 {
    var resolved: ?[]const u8 = null;
    for (schema.full_text_documents) |document_schema| {
        for (document_schema.dynamic_rules) |rule| {
            for (rule.variants) |variant| {
                const source_path = if (variant.suffix.len == 0)
                    field
                else if (std.mem.endsWith(u8, field, variant.suffix))
                    field[0 .. field.len - variant.suffix.len]
                else
                    continue;
                if (!pathMatchesDynamicRule(source_path, rule)) continue;
                if (resolved == null) {
                    resolved = variant.analyzer;
                } else if (!std.mem.eql(u8, resolved.?, variant.analyzer)) {
                    return null;
                }
            }
        }
    }
    return resolved;
}

fn resolveDynamicTemplateFieldAnalyzer(schema: runtime_schema_mod.TableSchema, field: []const u8) ?[]const u8 {
    if (runtime_schema_mod.resolveFieldType(schema, field)) |mapping| {
        if (isTextFieldType(mapping.field_type)) return mapping.analyzer;
    }
    const field_name = fieldNameFromPath(field);
    if (!std.mem.eql(u8, field_name, field)) {
        if (runtime_schema_mod.resolveFieldType(schema, field_name)) |mapping| {
            if (isTextFieldType(mapping.field_type)) return mapping.analyzer;
        }
    }
    return null;
}

fn fallsUnderDynamicTextPath(schema: runtime_schema_mod.TableSchema, field: []const u8) bool {
    for (schema.full_text_documents) |document_schema| {
        for (document_schema.open_dynamic_paths) |open_path| {
            if (open_path.len == 0) return true;
            if (!std.mem.startsWith(u8, field, open_path)) continue;
            if (field.len == open_path.len) return true;
            if (field.len > open_path.len and field[open_path.len] == '.') return true;
        }
        for (document_schema.infer_type_dynamic_paths) |infer_path| {
            if (infer_path.len == 0) return true;
            if (!std.mem.startsWith(u8, field, infer_path)) continue;
            if (field.len == infer_path.len) return true;
            if (field.len > infer_path.len and field[infer_path.len] == '.') return true;
        }
    }
    return false;
}

fn pathMatchesDynamicRule(path: []const u8, rule: runtime_schema_mod.FullTextDynamicRule) bool {
    if (rule.parent_path.len == 0) {
        const first_dot = std.mem.indexOfScalar(u8, path, '.');
        const dynamic_segment = if (first_dot) |idx| path[0..idx] else path;
        const remainder = if (first_dot) |idx| path[idx + 1 ..] else "";
        if (!segmentMatchesPattern(dynamic_segment, rule.segment_pattern)) return false;
        return std.mem.eql(u8, remainder, rule.relative_path);
    }

    if (!std.mem.startsWith(u8, path, rule.parent_path)) return false;
    if (path.len <= rule.parent_path.len or path[rule.parent_path.len] != '.') return false;

    const after_parent = path[rule.parent_path.len + 1 ..];
    const dynamic_end = std.mem.indexOfScalar(u8, after_parent, '.');
    const dynamic_segment = if (dynamic_end) |idx| after_parent[0..idx] else after_parent;
    const remainder = if (dynamic_end) |idx| after_parent[idx + 1 ..] else "";
    if (!segmentMatchesPattern(dynamic_segment, rule.segment_pattern)) return false;
    return std.mem.eql(u8, remainder, rule.relative_path);
}

fn segmentMatchesPattern(segment: []const u8, pattern: ?[]const u8) bool {
    if (segment.len == 0) return false;
    if (pattern) |compiled| {
        return @import("../../../search/regex.zig").matches(std.heap.page_allocator, compiled, segment) catch false;
    }
    return true;
}

fn isTextFieldType(field_type: runtime_schema_mod.AntflyType) bool {
    return switch (field_type) {
        .text, .html, .keyword, .link, .search_as_you_type => true,
        else => false,
    };
}

fn fieldNameFromPath(path: []const u8) []const u8 {
    const last_dot = std.mem.lastIndexOfScalar(u8, path, '.') orelse return path;
    return path[last_dot + 1 ..];
}

pub fn resolveAnalyzerName(name: []const u8, text_analysis: introducer_mod.TextAnalysisConfig) !*const analysis_mod.Analyzer {
    if (introducer_mod.resolveAnalyzerName(name, text_analysis)) |analyzer| return analyzer;
    return error.InvalidArgument;
}

test "resolveIndexedFieldAnalyzer uses compiled explicit and dynamic mappings" {
    const schema: runtime_schema_mod.TableSchema = .{
        .dynamic_templates = &.{
            .{
                .name = "meta_keywords",
                .path_match = "meta.*",
                .mapping = .{
                    .field_type = .keyword,
                    .analyzer = "keyword",
                },
            },
        },
        .full_text_documents = &.{
            .{
                .name = "doc",
                .fields = &.{
                    .{
                        .path = "title",
                        .emitted_name = "title",
                        .analyzer = "french",
                    },
                    .{
                        .path = "title",
                        .emitted_name = "title__keyword",
                        .analyzer = "keyword",
                    },
                },
                .dynamic_rules = &.{
                    .{
                        .parent_path = "meta",
                        .relative_path = "title",
                        .variants = &.{
                            .{
                                .suffix = "",
                                .analyzer = "standard",
                            },
                            .{
                                .suffix = "__2gram",
                                .analyzer = "search_as_you_type",
                            },
                        },
                    },
                },
                .open_dynamic_paths = &.{""},
                .infer_type_dynamic_paths = &.{"attributes"},
            },
        },
    };

    try std.testing.expectEqualStrings("french", resolveIndexedFieldAnalyzer(schema, "title").?);
    try std.testing.expectEqualStrings("keyword", resolveIndexedFieldAnalyzer(schema, "title__keyword").?);
    try std.testing.expectEqualStrings("search_as_you_type", resolveIndexedFieldAnalyzer(schema, "meta.tag_blue.title__2gram").?);
    try std.testing.expectEqualStrings("keyword", resolveIndexedFieldAnalyzer(schema, "meta.created_at").?);
    try std.testing.expectEqualStrings("standard", resolveIndexedFieldAnalyzer(schema, "body").?);
    try std.testing.expectEqualStrings("standard", resolveIndexedFieldAnalyzer(schema, "attributes.color").?);
}

test "resolveConfiguredFieldAnalyzerName returns unique configured analyzer and drops conflicts" {
    const cfg: introducer_mod.TextAnalysisConfig = .{
        .field_analyzers = &.{
            .{ .field_name = "meta.body", .analyzer_name = "french" },
            .{ .field_name = "meta.body", .analyzer_name = "french" },
            .{ .field_name = "meta.created_at", .analyzer_name = "keyword" },
            .{ .field_name = "meta.created_at", .analyzer_name = "standard" },
        },
    };

    try std.testing.expectEqualStrings("french", resolveConfiguredFieldAnalyzerName(cfg, "meta.body").?);
    try std.testing.expectEqual(@as(?[]const u8, null), resolveConfiguredFieldAnalyzerName(cfg, "meta.created_at"));
    try std.testing.expectEqual(@as(?[]const u8, null), resolveConfiguredFieldAnalyzerName(cfg, "missing"));
}

fn testIndexStats(active_count: u64, node_count: u64, leaf_size: u32) vectorindex_mod.IndexStats {
    return .{
        .dims = 128,
        .active_count = active_count,
        .node_count = node_count,
        .root_node = 1,
        .branching_factor = 128,
        .leaf_size = leaf_size,
    };
}

test "resolveSearchEffort maps effort to leaf-aware HBC width" {
    try std.testing.expectEqual(@as(?f32, null), normalizedSearchEffort(null));
    try std.testing.expectEqual(@as(f32, 0), normalizedSearchEffort(-0.5).?);
    try std.testing.expectEqual(@as(f32, 1), normalizedSearchEffort(5.0).?);
    try std.testing.expectEqual(@as(?f32, null), normalizedSearchEffort(std.math.nan(f32)));
    try std.testing.expectApproxEqAbs(@as(f32, default_balanced_search_effort), resolvedSearchEffort(null), 0.0001);
    try std.testing.expectApproxEqAbs(@as(f32, 0.5), resolvedSearchEffort(0.5), 0.0001);

    try std.testing.expectEqual(@as(u32, 2080), resolveSearchWidth(10, default_balanced_search_effort, testIndexStats(0, 0, 128)));
    try std.testing.expectEqual(@as(u32, 64), resolveSearchWidth(10, 0.0, testIndexStats(0, 0, 128)));
    try std.testing.expectEqual(@as(u32, 4096), resolveSearchWidth(10, 1.0, testIndexStats(0, 0, 128)));
    try std.testing.expectEqual(@as(u32, 17_591), resolveSearchWidth(10, 1.0, testIndexStats(0, 17_591, 128)));
    try std.testing.expectEqual(@as(u32, 2080), resolveSearchWidth(10, default_balanced_search_effort, testIndexStats(0, 17_591, 128)));
    try std.testing.expectEqual(@as(u32, 879), resolveSearchWidth(10, 1.0, testIndexStats(879, 879, 128)));
    try std.testing.expectEqual(@as(u32, 500), resolveSearchWidth(500, 0.0, testIndexStats(0, 0, 128)));

    try std.testing.expectEqual(@as(u32, 391), estimateLeafCount(testIndexStats(50_000, 879, 128)));
    try std.testing.expectEqual(@as(u32, 161), resolveSearchWidth(10, 0.3, testIndexStats(50_000, 879, 128)));
    try std.testing.expectEqual(@as(u32, 227), resolveSearchWidth(10, default_balanced_search_effort, testIndexStats(50_000, 879, 128)));
    try std.testing.expectEqual(@as(u32, 879), resolveSearchWidth(10, 1.0, testIndexStats(50_000, 879, 128)));
    try std.testing.expectEqual(@as(u32, 2080), resolveSearchWidth(10, default_balanced_search_effort, testIndexStats(1_000_000, 17_591, 128)));
    try std.testing.expectEqual(@as(u32, 17_591), resolveSearchWidth(10, 1.0, testIndexStats(1_000_000, 17_591, 128)));

    try std.testing.expectApproxEqAbs(@as(f32, 1.0), resolveSearchEpsilon(0.0), 0.01);
    try std.testing.expectApproxEqAbs(@as(f32, 7.0), resolveSearchEpsilon(default_balanced_search_effort), 0.01);
    try std.testing.expectApproxEqAbs(@as(f32, 100.0), resolveSearchEpsilon(1.0), 0.01);
}

fn testDenseIndexCallback(_: ?*anyopaque, _: ?[]const u8) anyerror!?*index_manager_mod.IndexManager.DenseIndex {
    return error.UnexpectedTestCall;
}

fn testTextIndexEntryCallback(_: ?*anyopaque, _: ?[]const u8) anyerror!?*index_manager_mod.IndexManager.TextIndex {
    return null;
}

fn testDenseDocKeyCallback(_: ?*anyopaque, _: []const u8, _: u64) anyerror!?[]u8 {
    return error.UnexpectedTestCall;
}

fn testDenseVectorIdCallback(_: ?*anyopaque, _: []const u8, doc_key: []const u8) anyerror!?u64 {
    if (std.mem.eql(u8, doc_key, "doc:a")) return 11;
    if (std.mem.eql(u8, doc_key, "doc:b")) return 22;
    if (std.mem.eql(u8, doc_key, "doc:c")) return 33;
    if (std.mem.eql(u8, doc_key, "doc:d")) return 44;
    return null;
}

fn testDenseLoadProjectedCallback(_: ?*anyopaque, _: Allocator, _: types.SearchRequest, _: []const u8) anyerror![]u8 {
    return error.UnexpectedTestCall;
}

fn testDenseHbcSearchCallback(_: ?*anyopaque, _: *index_manager_mod.IndexManager.DenseIndex, _: vectorindex_mod.SearchRequest) anyerror!vectorindex_mod.SearchResults {
    return error.UnexpectedTestCall;
}

fn testDenseHbcSearchProfiledCallback(_: ?*anyopaque, _: *index_manager_mod.IndexManager.DenseIndex, _: vectorindex_mod.SearchRequest) anyerror!vectorindex_mod.ProfiledSearchResults {
    return error.UnexpectedTestCall;
}

fn testDensePostprocessCallback(_: ?*anyopaque, _: Allocator, _: types.SearchRequest, _: types.SearchResult, _: bool) anyerror!types.SearchResult {
    return error.UnexpectedTestCall;
}

fn testDenseConstraintExecutor() DenseSearchExecutor {
    return .{
        .ctx = null,
        .text_index_entry = testTextIndexEntryCallback,
        .dense_index = testDenseIndexCallback,
        .lookup_doc_key = testDenseDocKeyCallback,
        .lookup_vector_id = testDenseVectorIdCallback,
        .load_projected_document = testDenseLoadProjectedCallback,
        .hbc_search = testDenseHbcSearchCallback,
        .hbc_search_profiled = testDenseHbcSearchProfiledCallback,
        .postprocess = testDensePostprocessCallback,
    };
}

test "native dense constraints derive safe doc-id filter and exclusion ids" {
    const alloc = std.testing.allocator;
    var constraints = try deriveNativeDenseConstraintsAlloc(alloc, .{
        .filter_doc_ids = &.{ "doc:a", "doc:b", "doc:missing" },
        .filter_doc_ids_positive = true,
        .exclude_doc_ids = &.{"doc:d"},
        .filter_ids = &.{ 22, 99 },
        .exclude_ids = &.{55},
        .filter_query_json =
        \\{"bool":{"must":[{"doc_id":["doc:a","doc:b"]},{"term":{"category":"keep"}}],"must_not":[{"doc_id":["doc:c"]}]}}
        ,
    }, testDenseConstraintExecutor(), "dv_v1", false);
    defer constraints.deinit(alloc);

    try std.testing.expect(constraints.positive_filter);
    try std.testing.expectEqual(@as(usize, 1), constraints.filter_ids.len);
    try std.testing.expectEqual(@as(u64, 22), constraints.filter_ids[0]);
    try std.testing.expect(containsVectorId(constraints.exclude_ids, 33));
    try std.testing.expect(containsVectorId(constraints.exclude_ids, 44));
    try std.testing.expect(containsVectorId(constraints.exclude_ids, 55));
}

test "native dense constraints preserve empty positive algebraic candidate sets" {
    const alloc = std.testing.allocator;
    var constraints = try deriveNativeDenseConstraintsAlloc(alloc, .{
        .filter_doc_ids = &.{},
        .filter_doc_ids_positive = true,
    }, testDenseConstraintExecutor(), "dv_v1", false);
    defer constraints.deinit(alloc);

    try std.testing.expect(constraints.positive_filter);
    try std.testing.expectEqual(@as(usize, 0), constraints.filter_ids.len);

    var intersected = try deriveNativeDenseConstraintsAlloc(alloc, .{
        .filter_doc_ids = &.{},
        .filter_doc_ids_positive = true,
        .filter_ids = &.{ 11, 22 },
    }, testDenseConstraintExecutor(), "dv_v1", false);
    defer intersected.deinit(alloc);

    try std.testing.expect(intersected.positive_filter);
    try std.testing.expectEqual(@as(usize, 0), intersected.filter_ids.len);
}

test "native dense constraints fail closed without ordinal vector mapping" {
    const alloc = std.testing.allocator;
    var filter = doc_set.ResolvedDocFilter{
        .include = try doc_set.fromOrdinalsAlloc(alloc, &.{2}),
    };
    defer filter.deinit(alloc);

    try std.testing.expectError(error.UnsupportedQueryRequest, deriveNativeDenseConstraintsAlloc(alloc, .{
        .resolved_doc_filter = &filter,
    }, testDenseConstraintExecutor(), "dv_v1", false));
}

test "native sparse constraints accept algebraic doc id candidate sets" {
    const alloc = std.testing.allocator;
    var constraints = try deriveNativeDocIdConstraintsAlloc(alloc, .{
        .filter_doc_ids = &.{ "doc:a", "doc:b" },
        .filter_doc_ids_positive = true,
        .exclude_doc_ids = &.{"doc:c"},
        .filter_query_json =
        \\{"doc_id":["doc:b","doc:d"]}
        ,
        .exclusion_query_json =
        \\{"doc_id":["doc:e"]}
        ,
    }, .{
        .ctx = null,
        .text_index_entry = testTextIndexEntryCallback,
    });
    defer constraints.deinit(alloc);

    try std.testing.expect(constraints.positive_filter);
    try std.testing.expectEqual(@as(usize, 1), constraints.filter_doc_ids.len);
    try std.testing.expectEqualStrings("doc:b", constraints.filter_doc_ids[0]);
    try std.testing.expect(containsDocId(constraints.exclude_doc_ids, "doc:c"));
    try std.testing.expect(containsDocId(constraints.exclude_doc_ids, "doc:e"));
}

test "native sparse constraints preserve empty positive algebraic candidate sets" {
    const alloc = std.testing.allocator;
    var constraints = try deriveNativeDocIdConstraintsAlloc(alloc, .{
        .filter_doc_ids = &.{},
        .filter_doc_ids_positive = true,
    }, .{
        .ctx = null,
        .text_index_entry = testTextIndexEntryCallback,
    });
    defer constraints.deinit(alloc);

    try std.testing.expect(constraints.positive_filter);
    try std.testing.expectEqual(@as(usize, 0), constraints.filter_doc_ids.len);
}

const TestMatchAllCtx = struct {
    ids: []const []const u8,
    ordinals: []const ?doc_set.DocOrdinal = &.{},
};

fn testCollectMatchAllCandidatesCallback(
    ctx: ?*anyopaque,
    alloc: Allocator,
    _: types.SearchRequest,
) anyerror!MatchAllCandidates {
    const test_ctx: *const TestMatchAllCtx = @ptrCast(@alignCast(ctx orelse return error.InvalidArgument));
    var out = try alloc.alloc(MatchAllCandidate, test_ctx.ids.len);
    var initialized: usize = 0;
    errdefer {
        for (out[0..initialized]) |*candidate| candidate.deinit(alloc);
        if (out.len > 0) alloc.free(out);
    }
    for (test_ctx.ids, 0..) |id, i| {
        const ordinal = if (i < test_ctx.ordinals.len) test_ctx.ordinals[i] else null;
        out[i] = .{ .id = try alloc.dupe(u8, id), .ordinal = ordinal };
        initialized += 1;
    }
    return .{ .items = out };
}

fn testMatchAllLoadProjectedCallback(
    _: ?*anyopaque,
    _: Allocator,
    _: types.SearchRequest,
    _: []const u8,
) anyerror![]u8 {
    return error.UnexpectedTestCall;
}

fn testMatchAllExecutor(ctx: *const TestMatchAllCtx) MatchAllExecutor {
    return .{
        .ctx = @constCast(ctx),
        .collect_candidates = testCollectMatchAllCandidatesCallback,
        .text_index_entry = testTextIndexEntryCallback,
        .resolve_doc_set_doc_ids = testResolveDocSetDocIdsCallback,
        .resolve_doc_ids_to_doc_set = testResolveDocIdsToDocSetCallback,
        .live_filter_doc_set = testLiveFilterDocSetCallback,
        .load_projected_document = testMatchAllLoadProjectedCallback,
    };
}

test "match_all applies explicit doc id constraints before paging" {
    const alloc = std.testing.allocator;
    const ctx = TestMatchAllCtx{
        .ids = &.{ "doc:a", "doc:b", "doc:c" },
        .ordinals = &.{ 1, 2, 3 },
    };

    var executor = testMatchAllExecutor(&ctx);
    executor.live_filter_doc_set = null;
    var result = try searchMatchAll(alloc, .{
        .filter_doc_ids = &.{ "doc:a", "doc:c" },
        .filter_doc_ids_positive = true,
        .exclude_doc_ids = &.{"doc:c"},
        .include_stored = false,
        .limit = 10,
    }, executor);
    defer result.deinit();

    try std.testing.expectEqual(@as(u32, 1), result.total_hits);
    try std.testing.expectEqual(@as(usize, 1), result.hits.len);
    try std.testing.expectEqualStrings("doc:a", result.hits[0].id);
}

test "match_all candidate ordinal lookup uses identity read generation" {
    const alloc = std.testing.allocator;
    const Harness = struct {
        seen_generation: ?u64 = null,

        fn scanStoreRange(
            _: ?*anyopaque,
            scan_alloc: Allocator,
            _: []const u8,
            _: []const u8,
        ) anyerror![]docstore_mod.OwnedKVPair {
            const out = try scan_alloc.alloc(docstore_mod.OwnedKVPair, 1);
            errdefer scan_alloc.free(out);
            out[0] = .{
                .key = try internal_keys.documentKeyAlloc(scan_alloc, "doc:a"),
                .value = try scan_alloc.dupe(u8, "{}"),
            };
            return out;
        }

        fn isExpiredKey(_: ?*anyopaque, _: Allocator, _: []const u8) anyerror!bool {
            return false;
        }

        fn lookupDocOrdinal(
            ctx: ?*anyopaque,
            _: Allocator,
            doc_id: []const u8,
            generation: ?u64,
        ) anyerror!?doc_set.DocOrdinal {
            const self: *@This() = @ptrCast(@alignCast(ctx orelse return error.InvalidArgument));
            try std.testing.expectEqualStrings("doc:a", doc_id);
            self.seen_generation = generation;
            return 9;
        }
    };

    var harness = Harness{};
    var candidates = try collectMatchAllCandidates(alloc, .{
        .identity_read_generation = 77,
    }, .{
        .ctx = &harness,
        .scan_store_range = Harness.scanStoreRange,
        .is_expired_key = Harness.isExpiredKey,
        .lookup_doc_ordinal = Harness.lookupDocOrdinal,
    });
    defer candidates.deinit(alloc);

    try std.testing.expectEqual(@as(?u64, 77), harness.seen_generation);
    try std.testing.expectEqual(@as(usize, 1), candidates.items.len);
    try std.testing.expectEqual(@as(?doc_set.DocOrdinal, 9), candidates.items[0].ordinal);
}

test "match_all consumes resolved ordinal filters without doc id projection" {
    const alloc = std.testing.allocator;
    const ctx = TestMatchAllCtx{
        .ids = &.{ "doc:a", "doc:b", "doc:c" },
        .ordinals = &.{ 1, 2, 3 },
    };
    var filter = doc_set.ResolvedDocFilter{
        .include = try doc_set.fromOrdinalsAlloc(alloc, &.{ 1, 2, 3 }),
        .exclude = try doc_set.fromOrdinalsAlloc(alloc, &.{3}),
    };
    defer filter.deinit(alloc);

    var executor = testMatchAllExecutor(&ctx);
    executor.resolve_doc_set_doc_ids = null;
    var result = try searchMatchAll(alloc, .{
        .resolved_doc_filter = &filter,
        .include_stored = false,
        .limit = 10,
    }, executor);
    defer result.deinit();

    try std.testing.expectEqual(@as(u32, 1), result.total_hits);
    try std.testing.expectEqual(@as(usize, 1), result.hits.len);
    try std.testing.expectEqualStrings("doc:b", result.hits[0].id);
}

fn testResolveDocSetDocIdsCallback(
    _: ?*anyopaque,
    alloc: Allocator,
    set: *const doc_set.ResolvedDocSet,
    _: ?u64,
) anyerror!?[]const []const u8 {
    var out = std.ArrayListUnmanaged([]const u8).empty;
    errdefer freeDocIdArrayList(alloc, &out);
    switch (set.*) {
        .ordinals => |ordinals| {
            for (ordinals) |ordinal| {
                const id: []const u8 = switch (ordinal) {
                    1 => "doc:a",
                    2 => "doc:b",
                    3 => "doc:c",
                    else => return error.NotFound,
                };
                try appendOwnedDocId(alloc, &out, id);
            }
            return try out.toOwnedSlice(alloc);
        },
        .ordinal_bitmap => |*bitmap| {
            var iter = bitmap.iterator();
            while (iter.next()) |ordinal| {
                const id: []const u8 = switch (ordinal) {
                    1 => "doc:a",
                    2 => "doc:b",
                    3 => "doc:c",
                    else => return error.NotFound,
                };
                try appendOwnedDocId(alloc, &out, id);
            }
            return try out.toOwnedSlice(alloc);
        },
        else => return null,
    }
}

fn testResolveDocIdsToDocSetCallback(
    _: ?*anyopaque,
    alloc: Allocator,
    doc_ids: []const []const u8,
    _: ?u64,
) anyerror!doc_set.ResolvedDocSet {
    var ordinals = std.ArrayListUnmanaged(doc_set.DocOrdinal).empty;
    defer ordinals.deinit(alloc);
    for (doc_ids) |doc_id| {
        const ordinal: doc_set.DocOrdinal = if (std.mem.eql(u8, doc_id, "doc:a"))
            1
        else if (std.mem.eql(u8, doc_id, "doc:b"))
            2
        else if (std.mem.eql(u8, doc_id, "doc:c"))
            3
        else
            return try doc_set.cloneDocKeysAlloc(alloc, doc_ids);
        try ordinals.append(alloc, ordinal);
    }
    if (ordinals.items.len == 0) return .none;
    return try doc_set.fromOrdinalsAlloc(alloc, ordinals.items);
}

fn testLookupDocNumsForOrdinalsCallback(
    _: ?*anyopaque,
    alloc: Allocator,
    index_name: []const u8,
    ordinals: []const u32,
) anyerror![]const u32 {
    try std.testing.expectEqualStrings("sp_v1", index_name);
    const out = try alloc.alloc(u32, ordinals.len);
    errdefer alloc.free(out);
    for (ordinals, 0..) |ordinal, i| {
        out[i] = switch (ordinal) {
            1 => 101,
            2 => 102,
            3 => 103,
            else => return error.NotFound,
        };
    }
    return out;
}

fn testLiveFilterDocSetCallback(
    _: ?*anyopaque,
    alloc: Allocator,
    set: *const doc_set.ResolvedDocSet,
    _: ?u64,
) anyerror!doc_set.ResolvedDocSet {
    switch (set.*) {
        .all => return .all,
        .none => return .none,
        .doc_keys => |keys| {
            var out = std.ArrayListUnmanaged([]const u8).empty;
            errdefer freeDocIdArrayList(alloc, &out);
            for (keys) |key| {
                if (std.mem.eql(u8, key, "doc:a")) continue;
                try appendOwnedDocId(alloc, &out, key);
            }
            return .{ .doc_keys = try out.toOwnedSlice(alloc) };
        },
        .ordinals => |ordinals| {
            var out = std.ArrayListUnmanaged(doc_set.DocOrdinal).empty;
            defer out.deinit(alloc);
            for (ordinals) |ordinal| {
                if (ordinal == 1) continue;
                try out.append(alloc, ordinal);
            }
            return try doc_set.fromOrdinalsAlloc(alloc, out.items);
        },
        .ordinal_bitmap => |*bitmap| {
            var out = std.ArrayListUnmanaged(doc_set.DocOrdinal).empty;
            defer out.deinit(alloc);
            var iter = bitmap.iterator();
            while (iter.next()) |ordinal| {
                if (ordinal == 1) continue;
                try out.append(alloc, ordinal);
            }
            return try doc_set.fromOrdinalsAlloc(alloc, out.items);
        },
    }
}

fn testLiveAllDocSetCallback(
    _: ?*anyopaque,
    alloc: Allocator,
    set: *const doc_set.ResolvedDocSet,
    _: ?u64,
) anyerror!doc_set.ResolvedDocSet {
    return switch (set.*) {
        .all => try doc_set.fromOrdinalsAlloc(alloc, &.{ 2, 3 }),
        else => try doc_set.cloneAlloc(alloc, set),
    };
}

const TestGenerationLiveFilterProbe = struct {
    seen_generation: ?u64 = null,
};

fn testGenerationLiveFilterDocSetCallback(
    ctx: ?*anyopaque,
    alloc: Allocator,
    set: *const doc_set.ResolvedDocSet,
    generation: ?u64,
) anyerror!doc_set.ResolvedDocSet {
    const probe: *TestGenerationLiveFilterProbe = @ptrCast(@alignCast(ctx orelse return error.InvalidArgument));
    probe.seen_generation = generation;
    switch (set.*) {
        .ordinals => |ordinals| {
            var out = std.ArrayListUnmanaged(doc_set.DocOrdinal).empty;
            defer out.deinit(alloc);
            for (ordinals) |ordinal| {
                if (generation == null and ordinal == 1) continue;
                try out.append(alloc, ordinal);
            }
            return try doc_set.fromOrdinalsAlloc(alloc, out.items);
        },
        else => return try doc_set.cloneAlloc(alloc, set),
    }
}

test "native sparse constraints consume resolved doc sets before vector filtering" {
    const alloc = std.testing.allocator;
    var filter = doc_set.ResolvedDocFilter{
        .include = try doc_set.fromOrdinalsAlloc(alloc, &.{ 2, 1 }),
        .exclude = try doc_set.cloneDocKeysAlloc(alloc, &.{"doc:c"}),
    };
    defer filter.deinit(alloc);

    var constraints = try deriveNativeDocIdConstraintsAlloc(alloc, .{
        .resolved_doc_filter = &filter,
        .filter_doc_ids = &.{"doc:b"},
        .filter_doc_ids_positive = true,
        .exclude_doc_ids = &.{"doc:d"},
    }, .{
        .ctx = null,
        .text_index_entry = testTextIndexEntryCallback,
        .resolve_doc_set_doc_ids = testResolveDocSetDocIdsCallback,
    });
    defer constraints.deinit(alloc);

    try std.testing.expect(constraints.positive_filter);
    try std.testing.expectEqual(@as(usize, 1), constraints.filter_doc_ids.len);
    try std.testing.expectEqualStrings("doc:b", constraints.filter_doc_ids[0]);
    try std.testing.expect(containsDocId(constraints.exclude_doc_ids, "doc:c"));
    try std.testing.expect(containsDocId(constraints.exclude_doc_ids, "doc:d"));
}

test "native constraints pass identity generation to doc-set id projection" {
    const alloc = std.testing.allocator;
    var filter = doc_set.ResolvedDocFilter{
        .include = try doc_set.fromOrdinalsAlloc(alloc, &.{1}),
    };
    defer filter.deinit(alloc);

    const Harness = struct {
        seen_generation: ?u64 = null,

        fn resolveDocSetDocIds(
            ctx: ?*anyopaque,
            alloc_inner: Allocator,
            set: *const doc_set.ResolvedDocSet,
            generation: ?u64,
        ) anyerror!?[]const []const u8 {
            const self: *@This() = @ptrCast(@alignCast(ctx orelse return error.InvalidArgument));
            self.seen_generation = generation;
            try std.testing.expect(set.containsOrdinal(1));
            return try dupeDocIdSliceAlloc(alloc_inner, &.{"doc:a"});
        }
    };

    var harness = Harness{};
    var constraints = try deriveNativeDocIdConstraintsAlloc(alloc, .{
        .resolved_doc_filter = &filter,
        .identity_read_generation = 7,
    }, .{
        .ctx = &harness,
        .text_index_entry = testTextIndexEntryCallback,
        .resolve_doc_set_doc_ids = Harness.resolveDocSetDocIds,
    });
    defer constraints.deinit(alloc);

    try std.testing.expectEqual(@as(?u64, 7), harness.seen_generation);
    try std.testing.expect(constraints.positive_filter);
    try std.testing.expectEqual(@as(usize, 1), constraints.filter_doc_ids.len);
    try std.testing.expectEqualStrings("doc:a", constraints.filter_doc_ids[0]);
}

test "native constraints fail closed when resolved ordinals cannot be represented" {
    const alloc = std.testing.allocator;
    var filter = doc_set.ResolvedDocFilter{
        .include = try doc_set.fromOrdinalsAlloc(alloc, &.{1}),
    };
    defer filter.deinit(alloc);

    try std.testing.expectError(error.UnsupportedQueryRequest, deriveNativeDocIdConstraintsAlloc(alloc, .{
        .resolved_doc_filter = &filter,
    }, .{
        .ctx = null,
        .text_index_entry = testTextIndexEntryCallback,
    }));

    var exclude_filter = doc_set.ResolvedDocFilter{
        .include = .all,
        .exclude = try doc_set.fromOrdinalsAlloc(alloc, &.{2}),
    };
    defer exclude_filter.deinit(alloc);

    try std.testing.expectError(error.UnsupportedQueryRequest, deriveNativeDocIdConstraintsAlloc(alloc, .{
        .resolved_doc_filter = &exclude_filter,
    }, .{
        .ctx = null,
        .text_index_entry = testTextIndexEntryCallback,
    }));
}

test "native sparse constraints fail closed without ordinal doc num mapper" {
    const alloc = std.testing.allocator;
    var filter = doc_set.ResolvedDocFilter{
        .include = try doc_set.fromOrdinalsAlloc(alloc, &.{ 3, 1 }),
        .exclude = try doc_set.fromOrdinalsAlloc(alloc, &.{2}),
    };
    defer filter.deinit(alloc);

    try std.testing.expectError(error.UnsupportedQueryRequest, deriveNativeDocIdConstraintsAlloc(alloc, .{
        .resolved_doc_filter = &filter,
    }, .{
        .ctx = null,
        .text_index_entry = testTextIndexEntryCallback,
        .resolve_doc_set_doc_ids = testResolveDocSetDocIdsCallback,
        .project_ordinals_to_doc_ids = false,
        .require_doc_num_projection_mapper = true,
    }));
}

test "native sparse constraints map resolved ordinals to physical doc nums" {
    const alloc = std.testing.allocator;
    var filter = doc_set.ResolvedDocFilter{
        .include = try doc_set.fromOrdinalsAlloc(alloc, &.{ 3, 1 }),
        .exclude = try doc_set.fromOrdinalsAlloc(alloc, &.{2}),
    };
    defer filter.deinit(alloc);

    var constraints = try deriveNativeDocIdConstraintsAlloc(alloc, .{
        .resolved_doc_filter = &filter,
    }, .{
        .ctx = null,
        .text_index_entry = testTextIndexEntryCallback,
        .resolve_doc_set_doc_ids = testResolveDocSetDocIdsCallback,
        .lookup_doc_nums_for_ordinals = testLookupDocNumsForOrdinalsCallback,
        .doc_num_index_name = "sp_v1",
        .project_ordinals_to_doc_ids = false,
        .require_doc_num_projection_mapper = true,
    });
    defer constraints.deinit(alloc);

    try std.testing.expect(constraints.positive_filter);
    try std.testing.expectEqual(@as(usize, 2), constraints.filter_doc_nums.len);
    try std.testing.expect(containsDocNum(constraints.filter_doc_nums, 101));
    try std.testing.expect(containsDocNum(constraints.filter_doc_nums, 103));
    try std.testing.expectEqual(@as(usize, 0), constraints.filter_doc_ids.len);
    try std.testing.expectEqual(@as(usize, 1), constraints.exclude_doc_nums.len);
    try std.testing.expectEqual(@as(u32, 102), constraints.exclude_doc_nums[0]);
    try std.testing.expectEqual(@as(usize, 0), constraints.exclude_doc_ids.len);
}

test "native constraints treat resolved all-doc exclusion as empty candidates" {
    const alloc = std.testing.allocator;
    var filter = doc_set.ResolvedDocFilter{
        .include = try doc_set.fromOrdinalsAlloc(alloc, &.{ 1, 2 }),
        .exclude = .all,
    };
    defer filter.deinit(alloc);

    var constraints = try deriveNativeDocIdConstraintsAlloc(alloc, .{
        .resolved_doc_filter = &filter,
    }, .{
        .ctx = null,
        .text_index_entry = testTextIndexEntryCallback,
        .resolve_doc_set_doc_ids = testResolveDocSetDocIdsCallback,
        .project_ordinals_to_doc_ids = false,
    });
    defer constraints.deinit(alloc);

    try std.testing.expect(constraints.positive_filter);
    try std.testing.expectEqual(@as(usize, 0), constraints.filter_doc_nums.len);
    try std.testing.expectEqual(@as(usize, 0), constraints.filter_doc_ids.len);
    try std.testing.expectEqual(@as(usize, 0), constraints.exclude_doc_nums.len);
    try std.testing.expectEqual(@as(usize, 0), constraints.exclude_doc_ids.len);
    try std.testing.expect(constraints.resolved_stored_filters);
}

test "native sparse constraints resolve explicit request doc ids to doc nums" {
    const alloc = std.testing.allocator;
    var constraints = try deriveNativeDocIdConstraintsAlloc(alloc, .{
        .filter_doc_ids = &.{ "doc:b", "doc:c" },
        .filter_doc_ids_positive = true,
        .exclude_doc_ids = &.{"doc:a"},
    }, .{
        .ctx = null,
        .text_index_entry = testTextIndexEntryCallback,
        .resolve_doc_ids_to_doc_set = testResolveDocIdsToDocSetCallback,
        .project_ordinals_to_doc_ids = false,
    });
    defer constraints.deinit(alloc);

    try std.testing.expect(constraints.positive_filter);
    try std.testing.expectEqual(@as(usize, 2), constraints.filter_doc_nums.len);
    try std.testing.expect(containsDocNum(constraints.filter_doc_nums, 2));
    try std.testing.expect(containsDocNum(constraints.filter_doc_nums, 3));
    try std.testing.expectEqual(@as(usize, 0), constraints.filter_doc_ids.len);
    try std.testing.expectEqual(@as(usize, 1), constraints.exclude_doc_nums.len);
    try std.testing.expectEqual(@as(u32, 1), constraints.exclude_doc_nums[0]);
    try std.testing.expectEqual(@as(usize, 0), constraints.exclude_doc_ids.len);
}

test "native sparse constraints keep explicit doc ids when identity coverage is incomplete" {
    const alloc = std.testing.allocator;
    var constraints = try deriveNativeDocIdConstraintsAlloc(alloc, .{
        .filter_doc_ids = &.{"doc:z"},
        .filter_doc_ids_positive = true,
        .exclude_doc_ids = &.{"doc:missing"},
    }, .{
        .ctx = null,
        .text_index_entry = testTextIndexEntryCallback,
        .resolve_doc_ids_to_doc_set = testResolveDocIdsToDocSetCallback,
        .project_ordinals_to_doc_ids = false,
    });
    defer constraints.deinit(alloc);

    try std.testing.expect(constraints.positive_filter);
    try std.testing.expectEqual(@as(usize, 0), constraints.filter_doc_nums.len);
    try std.testing.expectEqual(@as(usize, 1), constraints.filter_doc_ids.len);
    try std.testing.expectEqualStrings("doc:z", constraints.filter_doc_ids[0]);
    try std.testing.expectEqual(@as(usize, 0), constraints.exclude_doc_nums.len);
    try std.testing.expectEqual(@as(usize, 1), constraints.exclude_doc_ids.len);
    try std.testing.expectEqualStrings("doc:missing", constraints.exclude_doc_ids[0]);
}

test "native sparse constraints can apply broad live doc filter" {
    const alloc = std.testing.allocator;
    var constraints = try deriveNativeDocIdConstraintsAlloc(alloc, .{}, .{
        .ctx = null,
        .text_index_entry = testTextIndexEntryCallback,
        .live_filter_doc_set = testLiveAllDocSetCallback,
        .project_ordinals_to_doc_ids = false,
        .apply_live_all_docs = true,
    });
    defer constraints.deinit(alloc);

    try std.testing.expect(constraints.positive_filter);
    try std.testing.expectEqual(@as(usize, 2), constraints.filter_doc_nums.len);
    try std.testing.expect(!containsDocNum(constraints.filter_doc_nums, 1));
    try std.testing.expect(containsDocNum(constraints.filter_doc_nums, 2));
    try std.testing.expect(containsDocNum(constraints.filter_doc_nums, 3));
}

test "native sparse constraints live-filter resolved ordinal sets" {
    const alloc = std.testing.allocator;
    var filter = doc_set.ResolvedDocFilter{
        .include = try doc_set.fromOrdinalsAlloc(alloc, &.{ 3, 1, 2 }),
        .exclude = try doc_set.fromOrdinalsAlloc(alloc, &.{ 1, 3 }),
    };
    defer filter.deinit(alloc);

    var constraints = try deriveNativeDocIdConstraintsAlloc(alloc, .{
        .resolved_doc_filter = &filter,
    }, .{
        .ctx = null,
        .text_index_entry = testTextIndexEntryCallback,
        .resolve_doc_set_doc_ids = testResolveDocSetDocIdsCallback,
        .live_filter_doc_set = testLiveFilterDocSetCallback,
        .project_ordinals_to_doc_ids = false,
    });
    defer constraints.deinit(alloc);

    try std.testing.expect(constraints.positive_filter);
    try std.testing.expectEqual(@as(usize, 2), constraints.filter_doc_nums.len);
    try std.testing.expect(!containsDocNum(constraints.filter_doc_nums, 1));
    try std.testing.expect(containsDocNum(constraints.filter_doc_nums, 2));
    try std.testing.expect(containsDocNum(constraints.filter_doc_nums, 3));
    try std.testing.expectEqual(@as(usize, 1), constraints.exclude_doc_nums.len);
    try std.testing.expectEqual(@as(u32, 3), constraints.exclude_doc_nums[0]);
}

test "native constraints pass identity read generation to live doc filtering" {
    const alloc = std.testing.allocator;
    var filter = doc_set.ResolvedDocFilter{
        .include = try doc_set.fromOrdinalsAlloc(alloc, &.{ 1, 2 }),
    };
    defer filter.deinit(alloc);

    var current_probe = TestGenerationLiveFilterProbe{};
    var current_constraints = try deriveNativeDocIdConstraintsAlloc(alloc, .{
        .resolved_doc_filter = &filter,
    }, .{
        .ctx = &current_probe,
        .text_index_entry = testTextIndexEntryCallback,
        .live_filter_doc_set = testGenerationLiveFilterDocSetCallback,
        .project_ordinals_to_doc_ids = false,
    });
    defer current_constraints.deinit(alloc);
    try std.testing.expectEqual(@as(?u64, null), current_probe.seen_generation);
    try std.testing.expectEqual(@as(usize, 1), current_constraints.filter_doc_nums.len);
    try std.testing.expectEqual(@as(u32, 2), current_constraints.filter_doc_nums[0]);

    var snapshot_probe = TestGenerationLiveFilterProbe{};
    var snapshot_constraints = try deriveNativeDocIdConstraintsAlloc(alloc, .{
        .resolved_doc_filter = &filter,
        .identity_read_generation = 7,
    }, .{
        .ctx = &snapshot_probe,
        .text_index_entry = testTextIndexEntryCallback,
        .live_filter_doc_set = testGenerationLiveFilterDocSetCallback,
        .project_ordinals_to_doc_ids = false,
    });
    defer snapshot_constraints.deinit(alloc);
    try std.testing.expectEqual(@as(?u64, 7), snapshot_probe.seen_generation);
    try std.testing.expectEqual(@as(usize, 2), snapshot_constraints.filter_doc_nums.len);
    try std.testing.expect(containsDocNum(snapshot_constraints.filter_doc_nums, 1));
    try std.testing.expect(containsDocNum(snapshot_constraints.filter_doc_nums, 2));
}

test "text search query pushes native doc id constraints into bool query" {
    const alloc = std.testing.allocator;
    var arena = std.heap.ArenaAllocator.init(alloc);
    defer arena.deinit();

    const query = try textSearchQueryWithNativeDocIdsAlloc(arena.allocator(), .{
        .match = .{ .field = "body", .text = "alpha" },
    }, .{
        .positive_filter = true,
        .filter_doc_ids = &.{ "doc:b", "doc:c" },
        .exclude_doc_ids = &.{"doc:c"},
    });

    switch (query) {
        .bool_query => |bool_query| {
            try std.testing.expectEqual(@as(usize, 2), bool_query.must.len);
            try std.testing.expectEqual(@as(usize, 1), bool_query.must_not.len);
            try std.testing.expect(std.meta.activeTag(bool_query.must[0]) == .match);
            try std.testing.expect(std.meta.activeTag(bool_query.must[1]) == .doc_id);
            try std.testing.expectEqualStrings("doc:b", bool_query.must[1].doc_id.ids[0]);
            try std.testing.expect(std.meta.activeTag(bool_query.must_not[0]) == .doc_id);
            try std.testing.expectEqualStrings("doc:c", bool_query.must_not[0].doc_id.ids[0]);
        },
        else => return error.TestUnexpectedResult,
    }
}

test "text search query pushes native doc num constraints into bool query" {
    const alloc = std.testing.allocator;
    var arena = std.heap.ArenaAllocator.init(alloc);
    defer arena.deinit();

    const query = try textSearchQueryWithNativeDocIdsAlloc(arena.allocator(), .{
        .match = .{ .field = "body", .text = "alpha" },
    }, .{
        .positive_filter = true,
        .filter_doc_nums = &.{ 1, 3 },
        .exclude_doc_nums = &.{2},
    });

    switch (query) {
        .bool_query => |bool_query| {
            try std.testing.expectEqual(@as(usize, 2), bool_query.must.len);
            try std.testing.expectEqual(@as(usize, 1), bool_query.must_not.len);
            try std.testing.expect(std.meta.activeTag(bool_query.must[0]) == .match);
            try std.testing.expect(std.meta.activeTag(bool_query.must[1]) == .doc_num);
            try std.testing.expectEqual(@as(u32, 1), bool_query.must[1].doc_num.ids[0]);
            try std.testing.expect(std.meta.activeTag(bool_query.must_not[0]) == .doc_num);
            try std.testing.expectEqual(@as(u32, 2), bool_query.must_not[0].doc_num.ids[0]);
        },
        else => return error.TestUnexpectedResult,
    }
}

test "text native constraints project resolved ordinals through segment sidecar" {
    const alloc = std.testing.allocator;

    var seg_writer = segment_mod.SegmentWriter.init(alloc);
    defer seg_writer.deinit();
    try seg_writer.addStoredDoc("doc:a", "{}");
    try seg_writer.addStoredDoc("doc:b", "{}");
    try seg_writer.addDocOrdinals(&.{ 42, 7 });
    const segment_bytes = try seg_writer.build();
    defer alloc.free(segment_bytes);

    var writer = try index_mod.IndexWriter.init(alloc);
    defer writer.deinit();
    try writer.addSegment(segment_bytes);

    var filter = doc_set.ResolvedDocFilter{
        .include = try doc_set.fromOrdinalsAlloc(alloc, &.{7}),
        .exclude = try doc_set.fromOrdinalsAlloc(alloc, &.{42}),
    };
    defer filter.deinit(alloc);

    var constraints = try deriveNativeDocIdConstraintsAlloc(alloc, .{
        .resolved_doc_filter = &filter,
    }, .{
        .ctx = null,
        .text_index_entry = testTextIndexEntryCallback,
        .text_snapshot_for_doc_num_projection = writer.snapshot(),
    });
    defer constraints.deinit(alloc);

    try std.testing.expect(constraints.positive_filter);
    try std.testing.expectEqual(@as(usize, 1), constraints.filter_doc_nums.len);
    try std.testing.expectEqual(@as(u32, 1), constraints.filter_doc_nums[0]);
    try std.testing.expectEqual(@as(usize, 1), constraints.exclude_doc_nums.len);
    try std.testing.expectEqual(@as(u32, 0), constraints.exclude_doc_nums[0]);
    try std.testing.expectEqual(@as(usize, 0), constraints.filter_doc_ids.len);
    try std.testing.expectEqual(@as(usize, 0), constraints.exclude_doc_ids.len);
}

test "text resolved doc filter projection passes identity generation to live filtering" {
    const alloc = std.testing.allocator;

    var seg_writer = segment_mod.SegmentWriter.init(alloc);
    defer seg_writer.deinit();
    try seg_writer.addStoredDoc("doc:a", "{}");
    try seg_writer.addStoredDoc("doc:b", "{}");
    try seg_writer.addDocOrdinals(&.{ 1, 2 });
    const segment_bytes = try seg_writer.build();
    defer alloc.free(segment_bytes);

    var writer = try index_mod.IndexWriter.init(alloc);
    defer writer.deinit();
    try writer.addSegment(segment_bytes);

    var filter = doc_set.ResolvedDocFilter{
        .include = try doc_set.fromOrdinalsAlloc(alloc, &.{ 1, 2 }),
    };
    defer filter.deinit(alloc);

    var constraints = NativeDocIdConstraints{};
    defer constraints.deinit(alloc);
    var probe = TestGenerationLiveFilterProbe{};
    try applyResolvedDocFilterToTextDocNumsAlloc(alloc, writer.snapshot(), &constraints, &filter, .{
        .ctx = &probe,
        .text_index_entry = testTextIndexEntryCallback,
        .live_filter_doc_set = testGenerationLiveFilterDocSetCallback,
        .project_ordinals_to_doc_ids = false,
        .identity_read_generation = 7,
    });

    try std.testing.expectEqual(@as(?u64, 7), probe.seen_generation);
    try std.testing.expect(constraints.positive_filter);
    try std.testing.expectEqual(@as(usize, 2), constraints.filter_doc_nums.len);
    try std.testing.expect(containsDocNum(constraints.filter_doc_nums, 0));
    try std.testing.expect(containsDocNum(constraints.filter_doc_nums, 1));
    try std.testing.expectEqual(@as(usize, 0), constraints.filter_doc_ids.len);
}

test "text native constraints resolve explicit request doc ids through ordinal sidecar" {
    const alloc = std.testing.allocator;

    var seg_writer = segment_mod.SegmentWriter.init(alloc);
    defer seg_writer.deinit();
    try seg_writer.addStoredDoc("doc:a", "{}");
    try seg_writer.addStoredDoc("doc:b", "{}");
    try seg_writer.addStoredDoc("doc:c", "{}");
    try seg_writer.addDocOrdinals(&.{ 1, 2, 3 });
    const segment_bytes = try seg_writer.build();
    defer alloc.free(segment_bytes);

    var writer = try index_mod.IndexWriter.init(alloc);
    defer writer.deinit();
    try writer.addSegment(segment_bytes);

    const Harness = struct {
        fn resolveDocSetDocIds(
            _: ?*anyopaque,
            _: Allocator,
            _: *const doc_set.ResolvedDocSet,
            _: ?u64,
        ) anyerror!?[]const []const u8 {
            return error.UnexpectedTestCall;
        }
    };

    var constraints = try deriveNativeDocIdConstraintsAlloc(alloc, .{
        .filter_doc_ids = &.{ "doc:b", "doc:c" },
        .filter_doc_ids_positive = true,
        .exclude_doc_ids = &.{"doc:a"},
    }, .{
        .ctx = null,
        .text_index_entry = testTextIndexEntryCallback,
        .resolve_doc_ids_to_doc_set = testResolveDocIdsToDocSetCallback,
        .resolve_doc_set_doc_ids = Harness.resolveDocSetDocIds,
        .text_snapshot_for_doc_num_projection = writer.snapshot(),
        .project_ordinals_to_doc_ids = false,
    });
    defer constraints.deinit(alloc);

    try std.testing.expect(writer.snapshot().hasDocOrdinalCoverage());
    try std.testing.expect(constraints.positive_filter);
    try std.testing.expectEqual(@as(usize, 2), constraints.filter_doc_nums.len);
    try std.testing.expect(containsDocNum(constraints.filter_doc_nums, 1));
    try std.testing.expect(containsDocNum(constraints.filter_doc_nums, 2));
    try std.testing.expectEqual(@as(usize, 1), constraints.exclude_doc_nums.len);
    try std.testing.expectEqual(@as(u32, 0), constraints.exclude_doc_nums[0]);
    try std.testing.expectEqual(@as(usize, 0), constraints.filter_doc_ids.len);
    try std.testing.expectEqual(@as(usize, 0), constraints.exclude_doc_ids.len);
}

test "text native constraints fall back for mixed ordinal sidecar coverage" {
    const alloc = std.testing.allocator;

    var seg_with_ordinals = segment_mod.SegmentWriter.init(alloc);
    defer seg_with_ordinals.deinit();
    try seg_with_ordinals.addStoredDoc("doc:a", "{}");
    try seg_with_ordinals.addDocOrdinals(&.{1});
    const covered_segment = try seg_with_ordinals.build();
    defer alloc.free(covered_segment);

    var legacy_seg = segment_mod.SegmentWriter.init(alloc);
    defer legacy_seg.deinit();
    try legacy_seg.addStoredDoc("doc:b", "{}");
    const legacy_segment = try legacy_seg.build();
    defer alloc.free(legacy_segment);

    var writer = try index_mod.IndexWriter.init(alloc);
    defer writer.deinit();
    try writer.addSegment(covered_segment);
    try writer.addSegment(legacy_segment);

    var filter = doc_set.ResolvedDocFilter{
        .include = try doc_set.fromOrdinalsAlloc(alloc, &.{1}),
        .exclude = try doc_set.fromOrdinalsAlloc(alloc, &.{2}),
    };
    defer filter.deinit(alloc);

    var constraints = try deriveNativeDocIdConstraintsAlloc(alloc, .{
        .resolved_doc_filter = &filter,
    }, .{
        .ctx = null,
        .text_index_entry = testTextIndexEntryCallback,
        .resolve_doc_set_doc_ids = testResolveDocSetDocIdsCallback,
        .text_snapshot_for_doc_num_projection = writer.snapshot(),
    });
    defer constraints.deinit(alloc);

    try std.testing.expect(!writer.snapshot().hasDocOrdinalCoverage());
    try std.testing.expect(constraints.positive_filter);
    try std.testing.expectEqual(@as(usize, 0), constraints.filter_doc_nums.len);
    try std.testing.expectEqual(@as(usize, 0), constraints.exclude_doc_nums.len);
    try std.testing.expectEqual(@as(usize, 1), constraints.filter_doc_ids.len);
    try std.testing.expectEqualStrings("doc:a", constraints.filter_doc_ids[0]);
    try std.testing.expectEqual(@as(usize, 1), constraints.exclude_doc_ids.len);
    try std.testing.expectEqualStrings("doc:b", constraints.exclude_doc_ids[0]);
}

test "text native constraints fail closed when resolved ordinals cannot be projected" {
    const alloc = std.testing.allocator;

    var legacy_seg = segment_mod.SegmentWriter.init(alloc);
    defer legacy_seg.deinit();
    try legacy_seg.addStoredDoc("doc:a", "{}");
    const legacy_segment = try legacy_seg.build();
    defer alloc.free(legacy_segment);

    var writer = try index_mod.IndexWriter.init(alloc);
    defer writer.deinit();
    try writer.addSegment(legacy_segment);

    var filter = doc_set.ResolvedDocFilter{
        .include = try doc_set.fromOrdinalsAlloc(alloc, &.{1}),
    };
    defer filter.deinit(alloc);

    try std.testing.expect(!writer.snapshot().hasDocOrdinalCoverage());
    try std.testing.expectError(error.UnsupportedQueryRequest, deriveNativeDocIdConstraintsAlloc(alloc, .{
        .resolved_doc_filter = &filter,
    }, .{
        .ctx = null,
        .text_index_entry = testTextIndexEntryCallback,
        .text_snapshot_for_doc_num_projection = writer.snapshot(),
    }));
}

test "text native constraints treat resolved all-doc exclusion as empty candidates" {
    const alloc = std.testing.allocator;

    var seg_writer = segment_mod.SegmentWriter.init(alloc);
    defer seg_writer.deinit();
    try seg_writer.addStoredDoc("doc:a", "{}");
    try seg_writer.addDocOrdinals(&.{1});
    const segment_bytes = try seg_writer.build();
    defer alloc.free(segment_bytes);

    var writer = try index_mod.IndexWriter.init(alloc);
    defer writer.deinit();
    try writer.addSegment(segment_bytes);

    var filter = doc_set.ResolvedDocFilter{
        .include = try doc_set.fromOrdinalsAlloc(alloc, &.{1}),
        .exclude = .all,
    };
    defer filter.deinit(alloc);

    var constraints = try deriveNativeDocIdConstraintsAlloc(alloc, .{
        .resolved_doc_filter = &filter,
    }, .{
        .ctx = null,
        .text_index_entry = testTextIndexEntryCallback,
        .resolve_doc_set_doc_ids = testResolveDocSetDocIdsCallback,
        .text_snapshot_for_doc_num_projection = writer.snapshot(),
    });
    defer constraints.deinit(alloc);

    try std.testing.expect(constraints.positive_filter);
    try std.testing.expectEqual(@as(usize, 0), constraints.filter_doc_nums.len);
    try std.testing.expectEqual(@as(usize, 0), constraints.filter_doc_ids.len);
    try std.testing.expectEqual(@as(usize, 0), constraints.exclude_doc_nums.len);
    try std.testing.expectEqual(@as(usize, 0), constraints.exclude_doc_ids.len);
    try std.testing.expect(constraints.resolved_stored_filters);
}

test "structured text filter hit resolution uses ordinal sidecar" {
    const alloc = std.testing.allocator;

    var seg_writer = segment_mod.SegmentWriter.init(alloc);
    defer seg_writer.deinit();
    try seg_writer.addStoredDoc("doc:a", "{}");
    try seg_writer.addStoredDoc("doc:b", "{}");
    try seg_writer.addDocOrdinals(&.{ 1, 2 });
    const segment_bytes = try seg_writer.build();
    defer alloc.free(segment_bytes);

    var writer = try index_mod.IndexWriter.init(alloc);
    defer writer.deinit();
    try writer.addSegment(segment_bytes);

    const hits = [_]search_mod.ScoredHit{
        .{ .doc_id = 0, .score = 1.0, .id = null, .stored_data = null },
        .{ .doc_id = 1, .score = 1.0, .id = null, .stored_data = null },
    };
    var resolved = (try resolvedDocSetForTextHitsFromOrdinalSidecarAlloc(alloc, writer.snapshot(), hits[0..], .{
        .ctx = null,
        .text_index_entry = testTextIndexEntryCallback,
        .live_filter_doc_set = testLiveFilterDocSetCallback,
    })) orelse return error.TestUnexpectedResult;
    defer resolved.deinit(alloc);

    switch (resolved) {
        .ordinals => |ordinals| {
            try std.testing.expectEqual(@as(usize, 1), ordinals.len);
            try std.testing.expectEqual(@as(doc_set.DocOrdinal, 2), ordinals[0]);
        },
        else => return error.TestUnexpectedResult,
    }
}

test "structured filter doc set cache returns owned clones" {
    const alloc = std.testing.allocator;
    var cache = StructuredFilterDocSetCache{};
    defer cache.deinit(alloc);

    var source = try doc_set.fromOrdinalsAlloc(alloc, &.{ 4, 2, 4 });
    defer source.deinit(alloc);
    try cache.putCloneAlloc(alloc, "{\"term\":{\"field\":\"status\",\"value\":\"open\"}}", 7, &source);

    try std.testing.expect(cache.get("{\"term\":{\"field\":\"status\",\"value\":\"open\"}}", null) == null);
    try std.testing.expect(cache.get("{\"term\":{\"field\":\"status\",\"value\":\"open\"}}", 8) == null);
    const cached = cache.get("{\"term\":{\"field\":\"status\",\"value\":\"open\"}}", 7) orelse return error.TestUnexpectedResult;
    var clone = try doc_set.cloneAlloc(alloc, cached);
    defer clone.deinit(alloc);

    try std.testing.expect(clone.containsOrdinal(2));
    try std.testing.expect(clone.containsOrdinal(4));
    try std.testing.expect(!clone.containsOrdinal(3));

    var current_source = try doc_set.fromOrdinalsAlloc(alloc, &.{3});
    defer current_source.deinit(alloc);
    try cache.putCloneAlloc(alloc, "{\"term\":{\"field\":\"status\",\"value\":\"open\"}}", null, &current_source);
    const current_cached = cache.get("{\"term\":{\"field\":\"status\",\"value\":\"open\"}}", null) orelse return error.TestUnexpectedResult;
    try std.testing.expect(current_cached.containsOrdinal(3));
    try std.testing.expect(!current_cached.containsOrdinal(2));
}

test "structured filter doc set cache separates shared namespace generation keys" {
    const alloc = std.testing.allocator;
    var cache = StructuredFilterDocSetCache{};
    defer cache.deinit(alloc);

    var source = try doc_set.fromOrdinalsAlloc(alloc, &.{ 9, 10 });
    defer source.deinit(alloc);
    try cache.putSharedCloneAlloc(alloc, "{\"term\":{\"field\":\"status\",\"value\":\"open\"}}", 111, 7, &source);

    try std.testing.expect(cache.get("{\"term\":{\"field\":\"status\",\"value\":\"open\"}}", 7) == null);
    try std.testing.expect(cache.getShared("{\"term\":{\"field\":\"status\",\"value\":\"open\"}}", 112, 7) == null);
    try std.testing.expect(cache.getShared("{\"term\":{\"field\":\"status\",\"value\":\"open\"}}", 111, 8) == null);
    const cached = cache.getShared("{\"term\":{\"field\":\"status\",\"value\":\"open\"}}", 111, 7) orelse return error.TestUnexpectedResult;
    try std.testing.expect(cached.containsOrdinal(9));
    try std.testing.expect(cached.containsOrdinal(10));
}

test "composed search carries resolved doc sets for graph attachment" {
    const alloc = std.testing.allocator;

    const Harness = struct {
        saw_attach: bool = false,

        fn makeResult(alloc_inner: Allocator, doc_id: []const u8) !types.SearchResult {
            const hits = try alloc_inner.alloc(types.SearchHit, 1);
            hits[0] = .{ .id = try alloc_inner.dupe(u8, doc_id) };
            return .{
                .alloc = alloc_inner,
                .hits = hits,
                .total_hits = 1,
            };
        }

        fn searchTextQuery(
            _: ?*anyopaque,
            alloc_inner: Allocator,
            req: types.SearchRequest,
            _: types.TextQuery,
        ) anyerror!types.SearchResult {
            try std.testing.expectEqual(@as(?u64, 77), req.identity_read_generation);
            return try makeResult(alloc_inner, "doc:a");
        }

        fn searchText(
            _: ?*anyopaque,
            _: Allocator,
            _: types.SearchRequest,
        ) anyerror!types.SearchResult {
            return error.TestUnexpectedResult;
        }

        fn searchDense(
            _: ?*anyopaque,
            alloc_inner: Allocator,
            req: types.SearchRequest,
            _: types.DenseKnnQuery,
        ) anyerror!types.SearchResult {
            try std.testing.expectEqual(@as(?u64, 77), req.identity_read_generation);
            return try makeResult(alloc_inner, "doc:b");
        }

        fn searchSparse(
            _: ?*anyopaque,
            alloc_inner: Allocator,
            req: types.SearchRequest,
            _: types.SparseKnnQuery,
        ) anyerror!types.SearchResult {
            try std.testing.expectEqual(@as(?u64, 77), req.identity_read_generation);
            return try makeResult(alloc_inner, "doc:c");
        }

        fn cloneNamedSet(
            _: ?*anyopaque,
            alloc_inner: Allocator,
            set: graph_exec.NamedResultSet,
            include_stored: bool,
        ) anyerror!types.SearchResult {
            return try graph_exec.cloneNamedSetAsResult(alloc_inner, set, include_stored);
        }

        fn fuseNamedSets(
            _: ?*anyopaque,
            alloc_inner: Allocator,
            req: types.SearchRequest,
            named_sets: []const graph_exec.NamedResultSet,
        ) anyerror!types.SearchResult {
            try std.testing.expectEqual(@as(?u64, 77), req.identity_read_generation);
            const doc_id = if (named_sets.len == 2 and
                findComposedNamedSet(named_sets, "dense") != null and
                findComposedNamedSet(named_sets, "sparse") != null)
                "doc:embeddings"
            else
                "doc:fused";
            return try makeResult(alloc_inner, doc_id);
        }

        fn resolveHits(
            _: ?*anyopaque,
            alloc_inner: Allocator,
            req: types.SearchRequest,
            hits: []const types.SearchHit,
        ) anyerror!doc_set.ResolvedDocSet {
            try std.testing.expectEqual(@as(?u64, 77), req.identity_read_generation);
            var ordinals = std.ArrayListUnmanaged(doc_set.DocOrdinal).empty;
            defer ordinals.deinit(alloc_inner);
            for (hits) |hit| {
                const ordinal: doc_set.DocOrdinal = if (std.mem.eql(u8, hit.id, "doc:a"))
                    1
                else if (std.mem.eql(u8, hit.id, "doc:b"))
                    2
                else if (std.mem.eql(u8, hit.id, "doc:c"))
                    3
                else if (std.mem.eql(u8, hit.id, "doc:embeddings"))
                    8
                else if (std.mem.eql(u8, hit.id, "doc:fused"))
                    9
                else
                    return error.TestUnexpectedResult;
                try ordinals.append(alloc_inner, ordinal);
            }
            return try doc_set.fromOrdinalsAlloc(alloc_inner, ordinals.items);
        }

        fn expectNamedSetOrdinal(named_sets: []const graph_exec.NamedResultSet, name: []const u8, ordinal: doc_set.DocOrdinal) !void {
            const set = findComposedNamedSet(named_sets, name) orelse return error.TestUnexpectedResult;
            const resolved = set.resolved_doc_set orelse return error.TestUnexpectedResult;
            try std.testing.expect(resolved.containsOrdinal(ordinal));
        }

        fn attachGraphResults(
            ctx: ?*anyopaque,
            _: Allocator,
            req: types.SearchRequest,
            _: *types.SearchResult,
            named_sets: []const graph_exec.NamedResultSet,
        ) anyerror!void {
            const self: *@This() = @ptrCast(@alignCast(ctx.?));
            try std.testing.expectEqual(@as(?u64, 77), req.identity_read_generation);
            try expectNamedSetOrdinal(named_sets, "$full_text_results", 1);
            try expectNamedSetOrdinal(named_sets, "dense", 2);
            try expectNamedSetOrdinal(named_sets, "sparse", 3);
            try expectNamedSetOrdinal(named_sets, "$embeddings_results", 8);
            try expectNamedSetOrdinal(named_sets, "$fused_results", 9);
            self.saw_attach = true;
        }
    };

    const dense_vector = [_]f32{1.0};
    const sparse_indices = [_]u32{1};
    const sparse_values = [_]f32{1.0};
    const graph_queries = [_]types.NamedGraphQuery{.{
        .name = "g",
        .query = .{
            .query_type = .traverse,
            .index_name = "graph",
            .start_nodes = .{ .keys = &.{"doc:a"} },
            .params = .{},
        },
    }};

    var harness = Harness{};
    var result = try searchComposed(alloc, .{
        .full_text = .{ .match = .{ .field = "body", .text = "alpha" } },
        .dense = .{ .vector = &dense_vector, .k = 1 },
        .sparse = .{ .indices = &sparse_indices, .values = &sparse_values, .k = 1 },
        .graph_queries = &graph_queries,
        .include_stored = false,
        .identity_read_generation = 77,
    }, .{
        .ctx = &harness,
        .search_text_query = Harness.searchTextQuery,
        .search_text = Harness.searchText,
        .search_dense = Harness.searchDense,
        .search_sparse = Harness.searchSparse,
        .clone_named_set = Harness.cloneNamedSet,
        .fuse_named_sets = Harness.fuseNamedSets,
        .resolve_hits_to_doc_set = Harness.resolveHits,
        .attach_graph_results = Harness.attachGraphResults,
    });
    defer result.deinit();

    try std.testing.expect(harness.saw_attach);
    try std.testing.expectEqual(@as(usize, 1), result.hits.len);
    try std.testing.expectEqualStrings("doc:fused", result.hits[0].id);
}

test "composed search skips resolved doc-set materialization without graph queries" {
    const alloc = std.testing.allocator;

    const Harness = struct {
        fn makeResult(alloc_inner: Allocator, doc_id: []const u8) !types.SearchResult {
            const hits = try alloc_inner.alloc(types.SearchHit, 1);
            hits[0] = .{ .id = try alloc_inner.dupe(u8, doc_id) };
            return .{
                .alloc = alloc_inner,
                .hits = hits,
                .total_hits = 1,
            };
        }

        fn searchTextQuery(
            _: ?*anyopaque,
            _: Allocator,
            _: types.SearchRequest,
            _: types.TextQuery,
        ) anyerror!types.SearchResult {
            return error.TestUnexpectedResult;
        }

        fn searchText(
            _: ?*anyopaque,
            _: Allocator,
            _: types.SearchRequest,
        ) anyerror!types.SearchResult {
            return error.TestUnexpectedResult;
        }

        fn searchDense(
            _: ?*anyopaque,
            alloc_inner: Allocator,
            _: types.SearchRequest,
            _: types.DenseKnnQuery,
        ) anyerror!types.SearchResult {
            return try makeResult(alloc_inner, "doc:a");
        }

        fn searchSparse(
            _: ?*anyopaque,
            _: Allocator,
            _: types.SearchRequest,
            _: types.SparseKnnQuery,
        ) anyerror!types.SearchResult {
            return error.TestUnexpectedResult;
        }

        fn cloneNamedSet(
            _: ?*anyopaque,
            alloc_inner: Allocator,
            set: graph_exec.NamedResultSet,
            include_stored: bool,
        ) anyerror!types.SearchResult {
            return try graph_exec.cloneNamedSetAsResult(alloc_inner, set, include_stored);
        }

        fn fuseNamedSets(
            _: ?*anyopaque,
            _: Allocator,
            _: types.SearchRequest,
            _: []const graph_exec.NamedResultSet,
        ) anyerror!types.SearchResult {
            return error.TestUnexpectedResult;
        }

        fn resolveHits(
            _: ?*anyopaque,
            _: Allocator,
            _: types.SearchRequest,
            _: []const types.SearchHit,
        ) anyerror!doc_set.ResolvedDocSet {
            return error.TestUnexpectedResult;
        }

        fn attachGraphResults(
            _: ?*anyopaque,
            _: Allocator,
            _: types.SearchRequest,
            _: *types.SearchResult,
            _: []const graph_exec.NamedResultSet,
        ) anyerror!void {
            return error.TestUnexpectedResult;
        }
    };

    const dense_vector = [_]f32{1.0};
    var result = try searchComposed(alloc, .{
        .dense = .{ .vector = &dense_vector, .k = 1 },
        .include_stored = false,
    }, .{
        .ctx = null,
        .search_text_query = Harness.searchTextQuery,
        .search_text = Harness.searchText,
        .search_dense = Harness.searchDense,
        .search_sparse = Harness.searchSparse,
        .clone_named_set = Harness.cloneNamedSet,
        .fuse_named_sets = Harness.fuseNamedSets,
        .resolve_hits_to_doc_set = Harness.resolveHits,
        .attach_graph_results = Harness.attachGraphResults,
    });
    defer result.deinit();

    try std.testing.expectEqual(@as(usize, 1), result.hits.len);
    try std.testing.expectEqualStrings("doc:a", result.hits[0].id);
}

test "preflightSearchRequestAlloc summarizes search request result refs" {
    var summary = try preflightSearchRequestAlloc(std.testing.allocator, .{
        .full_text = .{ .match_all = {} },
        .dense_queries = &.{
            .{
                .name = "dense_primary",
                .index_name = "dense_primary",
                .query = .{ .vector = &.{ 0.25, 0.5 }, .k = 10 },
            },
        },
        .sparse_queries = &.{
            .{
                .name = "sparse_primary",
                .index_name = "sparse_primary",
                .query = .{ .indices = &.{ 1, 2 }, .values = &.{ 0.5, 0.25 }, .k = 10 },
            },
        },
    });
    defer summary.deinit(std.testing.allocator);

    try std.testing.expectEqual(@as(usize, 5), summary.result_refs.len);
    try std.testing.expectEqualStrings("$full_text_results", summary.result_refs[0]);
    try std.testing.expectEqualStrings("dense_primary", summary.result_refs[1]);
    try std.testing.expectEqualStrings("sparse_primary", summary.result_refs[2]);
    try std.testing.expectEqualStrings("$embeddings_results", summary.result_refs[3]);
    try std.testing.expectEqualStrings("$fused_results", summary.result_refs[4]);
}

pub fn geoPointPolygonsToSearchPolygons(
    alloc: Allocator,
    polygons: []const []const types.GeoPoint,
) ![]const []const search_mod.GeoPoint {
    if (polygons.len == 0) return &.{};
    var out = try alloc.alloc([]const search_mod.GeoPoint, polygons.len);
    var initialized: usize = 0;
    errdefer {
        for (out[0..initialized]) |polygon| alloc.free(polygon);
        alloc.free(out);
    }
    for (polygons, 0..) |polygon, i| {
        var converted = try alloc.alloc(search_mod.GeoPoint, polygon.len);
        for (polygon, 0..) |point, j| {
            converted[j] = .{ .lon = point.lon, .lat = point.lat };
        }
        out[i] = converted;
        initialized += 1;
    }
    return out;
}
