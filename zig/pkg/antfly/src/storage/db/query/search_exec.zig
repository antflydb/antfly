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
const doc_identity = @import("../doc_identity.zig");
const graph_exec = @import("graph_exec.zig");
const result_shape = @import("result_shape.zig");
const search_mod = @import("../../../search/search.zig");
const index_mod = @import("../../../index.zig");
const segment_mod = @import("../../../segment.zig");
const typed_dv = @import("../../../section/typed_doc_values.zig");
const distributed_stats_mod = @import("../../../search/distributed_stats.zig");
const analysis_mod = @import("../../../search/analysis.zig");
const introducer_mod = @import("../../../introducer.zig");
const mapper_mod = @import("../document_mapper.zig");
const platform_time = @import("../../../platform/time.zig");
const platform = @import("antfly_platform");
const vectorindex_mod = @import("antfly_vectorindex");
const vector_mod = @import("antfly_vector").vector;
const builtin = @import("builtin");
const sparse_mod = if (builtin.os.tag == .freestanding)
    @import("../sparse_stub.zig")
else
    @import("../../../sparse/sparse.zig");

fn getenv(name: [*:0]const u8) ?[]const u8 {
    return platform.env.getenv(name);
}

const default_balanced_search_effort: f32 = 0.5;
const default_late_visibility_exact_candidate_budget: u32 = 100_000;
const default_exact_native_filter_candidate_budget: u32 = 1024;
const default_match_all_primary_key_scan_batch_size: usize = 4096;
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
    load_stored: *const fn (
        ctx: ?*anyopaque,
        alloc: Allocator,
        key: []const u8,
    ) anyerror!?[]u8,
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
    all_docs_visible: ?*const fn (
        ctx: ?*anyopaque,
        generation: ?u64,
    ) anyerror!bool = null,
    requires_full_candidate_visibility_filter: ?*const fn (
        ctx: ?*anyopaque,
        generation: ?u64,
    ) anyerror!bool = null,
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
    all_docs_visible_fast: ?*const fn (
        ctx: ?*anyopaque,
        generation: ?u64,
    ) anyerror!bool = null,
    lookup_doc_ordinal: ?*const fn (
        ctx: ?*anyopaque,
        alloc: Allocator,
        doc_id: []const u8,
        generation: ?u64,
    ) anyerror!?doc_set.DocOrdinal = null,
    lookup_doc_ordinals: ?*const fn (
        ctx: ?*anyopaque,
        alloc: Allocator,
        doc_ids: []const []const u8,
        generation: ?u64,
    ) anyerror![]?doc_set.DocOrdinal = null,
    lookup_doc_ordinals_for_vector_ids: ?*const fn (
        ctx: ?*anyopaque,
        alloc: Allocator,
        index_name: []const u8,
        vector_ids: []const u64,
        generation: ?u64,
    ) anyerror![]?doc_set.DocOrdinal = null,
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
    exact_dense_search: ?*const fn (
        ctx: ?*anyopaque,
        entry: *index_manager_mod.IndexManager.DenseIndex,
        req: vectorindex_mod.SearchRequest,
    ) anyerror!vectorindex_mod.SearchResults = null,
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
    constraint_ns: u64 = 0,
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
    hbc_leaf_payload_stale: u64 = 0,
    hbc_leaf_payload_missing: u64 = 0,
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
    doc_ordinal_lookup_ns: u64 = 0,
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
    lookup_doc_ordinals: ?*const fn (
        ctx: ?*anyopaque,
        alloc: Allocator,
        doc_ids: []const []const u8,
        generation: ?u64,
    ) anyerror![]?doc_set.DocOrdinal = null,
    load_projected_document: *const fn (
        ctx: ?*anyopaque,
        alloc: Allocator,
        req: types.SearchRequest,
        key: []const u8,
    ) anyerror![]u8,
    load_projected_documents: ?*const fn (
        ctx: ?*anyopaque,
        alloc: Allocator,
        req: types.SearchRequest,
        keys: []const []const u8,
    ) anyerror![]?[]u8 = null,
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

pub const MatchAllCandidateCollectOptions = struct {
    candidate_limit: ?u32 = null,
    constraints: ?*const NativeDocIdConstraints = null,
    scan_batch_size: ?usize = null,
    primary_key_start_after: ?[]const u8 = null,
    primary_key_stop_before: ?[]const u8 = null,
    stop_after_accepted: ?usize = null,
};

pub const MatchAllCandidateConsumer = *const fn (
    ctx: ?*anyopaque,
    candidate: MatchAllCandidate,
) anyerror!void;

pub const MatchAllCandidateStreamStats = struct {
    accepted_count: usize = 0,
    stopped_early: bool = false,
};

pub const MatchAllExecutor = struct {
    ctx: ?*anyopaque,
    collect_candidates: *const fn (
        ctx: ?*anyopaque,
        alloc: Allocator,
        req: types.SearchRequest,
        options: MatchAllCandidateCollectOptions,
    ) anyerror!MatchAllCandidates,
    collect_candidates_stream: ?*const fn (
        ctx: ?*anyopaque,
        alloc: Allocator,
        req: types.SearchRequest,
        options: MatchAllCandidateCollectOptions,
        consumer_ctx: ?*anyopaque,
        consumer: MatchAllCandidateConsumer,
    ) anyerror!MatchAllCandidateStreamStats = null,
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
    load_projected_documents: ?*const fn (
        ctx: ?*anyopaque,
        alloc: Allocator,
        req: types.SearchRequest,
        keys: []const []const u8,
    ) anyerror![]?[]u8 = null,
    load_stored: *const fn (
        ctx: ?*anyopaque,
        alloc: Allocator,
        key: []const u8,
    ) anyerror!?[]u8,
    load_many_stored: ?*const fn (
        ctx: ?*anyopaque,
        alloc: Allocator,
        keys: []const []const u8,
    ) anyerror![]?[]u8 = null,
};

pub const MatchAllCandidateCollector = struct {
    ctx: ?*anyopaque,
    scan_store_range: *const fn (
        ctx: ?*anyopaque,
        alloc: Allocator,
        lower: []const u8,
        upper: []const u8,
    ) anyerror![]docstore_mod.OwnedKVPair,
    scan_store_range_with_context: ?*const fn (
        ctx: ?*anyopaque,
        lower: []const u8,
        upper: []const u8,
        scan_ctx: ?*anyopaque,
        callback: docstore_mod.DocStore.ScanWithContextCallback,
    ) anyerror!void = null,
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
    resolve_structured_doc_filter: ?*const fn (
        ctx: ?*anyopaque,
        alloc: Allocator,
        req: types.SearchRequest,
    ) anyerror!?doc_set.ResolvedDocFilter = null,
    resolve_structured_text_doc_filter: ?*const fn (
        ctx: ?*anyopaque,
        alloc: Allocator,
        req: types.SearchRequest,
    ) anyerror!?ResolvedTextDocNumFilter = null,
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

const TextDocNumSet = union(enum) {
    all,
    none,
    doc_nums: []const u32,

    fn deinit(self: *TextDocNumSet, alloc: Allocator) void {
        switch (self.*) {
            .doc_nums => |items| if (items.len > 0) alloc.free(@constCast(items)),
            .all, .none => {},
        }
        self.* = .none;
    }
};

pub const ResolvedTextDocNumFilter = struct {
    include: TextDocNumSet = .all,
    exclude: TextDocNumSet = .none,

    pub fn deinit(self: *ResolvedTextDocNumFilter, alloc: Allocator) void {
        self.include.deinit(alloc);
        self.exclude.deinit(alloc);
        self.* = undefined;
    }
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
    try validateComposedSortPageOptions(req);
    const bench_query_profile = shouldLogBenchQueryProfile();
    const composed_start_ns = if (bench_query_profile) platform_time.monotonicNs() else 0;
    var text_ns: u64 = 0;
    var dense_ns: u64 = 0;
    var sparse_ns: u64 = 0;
    var fuse_ns: u64 = 0;
    var graph_ns: u64 = 0;
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
    var shared_filter: ?doc_set.ResolvedDocFilter = if (executor.resolve_structured_doc_filter) |resolve|
        try resolve(executor.ctx, alloc, req)
    else
        null;
    defer if (shared_filter) |*filter| filter.deinit(alloc);
    var shared_text_filter: ?ResolvedTextDocNumFilter = if (executor.resolve_structured_text_doc_filter) |resolve|
        try resolve(executor.ctx, alloc, req)
    else
        null;
    defer if (shared_text_filter) |*filter| filter.deinit(alloc);

    var shared_req = req;
    if (shared_filter) |*filter| {
        shared_req.resolved_doc_filter = filter;
        shared_req.resolved_doc_filter_owned = false;
        shared_req.filter_query_json = "";
        shared_req.exclusion_query_json = "";
    }
    if (shared_text_filter) |*filter| {
        shared_req.resolved_text_doc_filter = filter;
    }

    if (shared_req.full_text_queries.len == 0) {
        if (shared_req.full_text) |text| {
            const phase_start_ns = if (bench_query_profile) platform_time.monotonicNs() else 0;
            const text_result = try executor.search_text_query(executor.ctx, alloc, shared_req, text);
            if (bench_query_profile) text_ns += platform_time.monotonicNs() - phase_start_ns;
            const resolved_doc_set = try resolveComposedHitsToDocSet(alloc, shared_req, executor, &owned_resolved_sets, text_result.hits);
            try named_sets.append(alloc, .{
                .name = "$full_text_results",
                .hits = text_result.hits,
                .total_hits = text_result.total_hits,
                .resolved_doc_set = resolved_doc_set,
            });
            try owned_results.append(alloc, text_result);
        } else if (!isDefaultMatchAll(shared_req.query) and isTextQuery(shared_req.query)) {
            const phase_start_ns = if (bench_query_profile) platform_time.monotonicNs() else 0;
            const text_result = try executor.search_text(executor.ctx, alloc, shared_req);
            if (bench_query_profile) text_ns += platform_time.monotonicNs() - phase_start_ns;
            const resolved_doc_set = try resolveComposedHitsToDocSet(alloc, shared_req, executor, &owned_resolved_sets, text_result.hits);
            try named_sets.append(alloc, .{
                .name = "$full_text_results",
                .hits = text_result.hits,
                .total_hits = text_result.total_hits,
                .resolved_doc_set = resolved_doc_set,
            });
            try owned_results.append(alloc, text_result);
        }
    } else {
        for (shared_req.full_text_queries) |full_text_query| {
            var text_req = shared_req;
            text_req.index_name = full_text_query.index_name;
            const phase_start_ns = if (bench_query_profile) platform_time.monotonicNs() else 0;
            const text_result = try executor.search_text_query(executor.ctx, alloc, text_req, full_text_query.query);
            if (bench_query_profile) text_ns += platform_time.monotonicNs() - phase_start_ns;
            const resolved_doc_set = try resolveComposedHitsToDocSet(alloc, shared_req, executor, &owned_resolved_sets, text_result.hits);
            try named_sets.append(alloc, .{
                .name = full_text_query.name,
                .hits = text_result.hits,
                .total_hits = text_result.total_hits,
                .resolved_doc_set = resolved_doc_set,
            });
            try owned_results.append(alloc, text_result);
        }
    }

    var vector_req = shared_req;
    vector_req.full_text = null;

    if (vector_req.dense_queries.len == 0 and vector_req.dense != null) {
        const phase_start_ns = if (bench_query_profile) platform_time.monotonicNs() else 0;
        const dense_result = try executor.search_dense(executor.ctx, alloc, vector_req, vector_req.dense.?);
        if (bench_query_profile) dense_ns += platform_time.monotonicNs() - phase_start_ns;
        const resolved_doc_set = try resolveComposedHitsToDocSet(alloc, shared_req, executor, &owned_resolved_sets, dense_result.hits);
        try named_sets.append(alloc, .{
            .name = if (vector_req.sparse == null) "$embeddings_results" else "dense",
            .hits = dense_result.hits,
            .total_hits = dense_result.total_hits,
            .resolved_doc_set = resolved_doc_set,
        });
        try owned_results.append(alloc, dense_result);
    } else {
        for (vector_req.dense_queries) |dense_query| {
            var dense_req = vector_req;
            dense_req.index_name = dense_query.index_name;
            const phase_start_ns = if (bench_query_profile) platform_time.monotonicNs() else 0;
            const dense_result = try executor.search_dense(executor.ctx, alloc, dense_req, dense_query.query);
            if (bench_query_profile) dense_ns += platform_time.monotonicNs() - phase_start_ns;
            const resolved_doc_set = try resolveComposedHitsToDocSet(alloc, shared_req, executor, &owned_resolved_sets, dense_result.hits);
            try named_sets.append(alloc, .{
                .name = dense_query.name,
                .hits = dense_result.hits,
                .total_hits = dense_result.total_hits,
                .resolved_doc_set = resolved_doc_set,
            });
            try owned_results.append(alloc, dense_result);
        }
    }

    if (vector_req.sparse_queries.len == 0 and vector_req.sparse != null) {
        const phase_start_ns = if (bench_query_profile) platform_time.monotonicNs() else 0;
        const sparse_result = try executor.search_sparse(executor.ctx, alloc, vector_req, vector_req.sparse.?);
        if (bench_query_profile) sparse_ns += platform_time.monotonicNs() - phase_start_ns;
        const resolved_doc_set = try resolveComposedHitsToDocSet(alloc, shared_req, executor, &owned_resolved_sets, sparse_result.hits);
        try named_sets.append(alloc, .{
            .name = if (vector_req.dense == null) "$embeddings_results" else "sparse",
            .hits = sparse_result.hits,
            .total_hits = sparse_result.total_hits,
            .resolved_doc_set = resolved_doc_set,
        });
        try owned_results.append(alloc, sparse_result);
    } else {
        for (vector_req.sparse_queries) |sparse_query| {
            var sparse_req = vector_req;
            sparse_req.index_name = sparse_query.index_name;
            const phase_start_ns = if (bench_query_profile) platform_time.monotonicNs() else 0;
            const sparse_result = try executor.search_sparse(executor.ctx, alloc, sparse_req, sparse_query.query);
            if (bench_query_profile) sparse_ns += platform_time.monotonicNs() - phase_start_ns;
            const resolved_doc_set = try resolveComposedHitsToDocSet(alloc, shared_req, executor, &owned_resolved_sets, sparse_result.hits);
            try named_sets.append(alloc, .{
                .name = sparse_query.name,
                .hits = sparse_result.hits,
                .total_hits = sparse_result.total_hits,
                .resolved_doc_set = resolved_doc_set,
            });
            try owned_results.append(alloc, sparse_result);
        }
    }

    const fuse_start_ns = if (bench_query_profile) platform_time.monotonicNs() else 0;
    var base = if (named_sets.items.len == 0)
        try emptySearchResult(alloc)
    else if (named_sets.items.len == 1)
        try executor.clone_named_set(executor.ctx, alloc, named_sets.items[0], shared_req.include_stored)
    else
        try executor.fuse_named_sets(executor.ctx, alloc, shared_req, named_sets.items);
    if (bench_query_profile) fuse_ns = platform_time.monotonicNs() - fuse_start_ns;
    errdefer base.deinit();

    const fused_resolved_doc_set = try resolveComposedHitsToDocSet(alloc, shared_req, executor, &owned_resolved_sets, base.hits);
    try named_sets.append(alloc, .{
        .name = "$fused_results",
        .hits = base.hits,
        .total_hits = base.total_hits,
        .resolved_doc_set = fused_resolved_doc_set,
    });

    try appendEmbeddingsResultAlias(alloc, shared_req, executor, &named_sets, &owned_results, &owned_resolved_sets);

    if (shared_req.graph_queries.len > 0) {
        const graph_start_ns = if (bench_query_profile) platform_time.monotonicNs() else 0;
        try executor.attach_graph_results(executor.ctx, alloc, shared_req, &base, named_sets.items);
        if (bench_query_profile) graph_ns = platform_time.monotonicNs() - graph_start_ns;
    }
    if (bench_query_profile) {
        std.log.info(
            "antfly_bench_composed_query text_us={d} dense_us={d} sparse_us={d} fuse_us={d} graph_us={d} total_us={d} named_sets={d} hits={d} total_hits={d}",
            .{
                nsToUs(text_ns),
                nsToUs(dense_ns),
                nsToUs(sparse_ns),
                nsToUs(fuse_ns),
                nsToUs(graph_ns),
                nsToUs(platform_time.monotonicNs() - composed_start_ns),
                named_sets.items.len,
                base.hits.len,
                base.total_hits,
            },
        );
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

    var embeddings_req = req;
    embeddings_req.merge_config = null;
    var embeddings_result = try executor.fuse_named_sets(executor.ctx, alloc, embeddings_req, embedding_sets.items);
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

fn requestHasSortPageOptions(req: types.SearchRequest) bool {
    return req.order_by.len > 0 or req.search_after.len > 0 or req.search_before.len > 0;
}

fn sortFieldIsId(field: types.SortField) bool {
    return std.mem.eql(u8, field.field, "_id");
}

fn sortFieldIsScore(field: types.SortField) bool {
    return std.mem.eql(u8, field.field, "_score");
}

fn sortScoreNumber(hit: types.SearchHit) f64 {
    const score = hit.score orelse return 0;
    return if (std.math.isFinite(score)) @floatCast(score) else 0;
}

fn sortFieldNeedsNativeValue(field: types.SortField) bool {
    return !sortFieldIsId(field) and !sortFieldIsScore(field);
}

fn validateSortIdTiebreaker(order_by: []const types.SortField) !void {
    for (order_by, 0..) |field, i| {
        if (!sortFieldIsId(field)) continue;
        if (i + 1 != order_by.len or field.desc) return error.InvalidQueryRequest;
    }
}

fn sortFieldsNeedImplicitIdTiebreaker(order_by: []const types.SortField) bool {
    if (order_by.len == 0) return false;
    return !sortFieldIsId(order_by[order_by.len - 1]);
}

fn effectiveSortFieldCount(order_by: []const types.SortField) usize {
    return order_by.len + @as(usize, if (sortFieldsNeedImplicitIdTiebreaker(order_by)) 1 else 0);
}

fn effectiveSortFieldAt(order_by: []const types.SortField, index: usize) types.SortField {
    if (index < order_by.len) return order_by[index];
    return .{ .field = "_id", .desc = false };
}

const EffectiveSortRequest = struct {
    req: types.SearchRequest,
    owned_order_by: []types.SortField = &.{},

    fn deinit(self: *EffectiveSortRequest, alloc: Allocator) void {
        if (self.owned_order_by.len > 0) alloc.free(self.owned_order_by);
    }
};

fn effectiveSortRequestAlloc(alloc: Allocator, req: types.SearchRequest) !EffectiveSortRequest {
    try validateSortIdTiebreaker(req.order_by);
    if (!sortFieldsNeedImplicitIdTiebreaker(req.order_by)) return .{ .req = req };

    const order_by = try alloc.alloc(types.SortField, req.order_by.len + 1);
    @memcpy(order_by[0..req.order_by.len], req.order_by);
    order_by[req.order_by.len] = .{ .field = "_id", .desc = false };
    var effective_req = req;
    effective_req.order_by = order_by;
    return .{ .req = effective_req, .owned_order_by = order_by };
}

fn rejectApproximateSortPageOptions(req: types.SearchRequest) !void {
    if (requestHasSortPageOptions(req)) return error.UnsupportedQueryRequest;
}

fn validateSortPageOptions(req: types.SearchRequest) !void {
    if (!requestHasSortPageOptions(req)) return;
    if (req.count_only) return error.UnsupportedQueryRequest;
    try validateSortCursorContract(req);
}

fn composedTextSourceCount(req: types.SearchRequest) usize {
    if (req.full_text_queries.len > 0) return req.full_text_queries.len;
    if (req.full_text != null) return 1;
    if (!isDefaultMatchAll(req.query) and isTextQuery(req.query)) return 1;
    return 0;
}

fn composedEmbeddingSourceCount(req: types.SearchRequest) usize {
    var count: usize = 0;
    count += if (req.dense_queries.len > 0) req.dense_queries.len else if (req.dense != null) @as(usize, 1) else 0;
    count += if (req.sparse_queries.len > 0) req.sparse_queries.len else if (req.sparse != null) @as(usize, 1) else 0;
    return count;
}

fn validateComposedSortPageOptions(req: types.SearchRequest) !void {
    try validateSortPageOptions(req);
    if (!requestHasSortPageOptions(req)) return;
    if (composedEmbeddingSourceCount(req) > 0) return error.UnsupportedQueryRequest;
    if (composedTextSourceCount(req) != 1) return error.UnsupportedQueryRequest;
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

fn requestWithoutResolvedStoredFilters(req: types.SearchRequest, filter_query_json_resolved: bool, exclusion_query_json_resolved: bool) types.SearchRequest {
    if (!filter_query_json_resolved and !exclusion_query_json_resolved) return req;
    var next = req;
    if (filter_query_json_resolved) next.filter_query_json = "";
    if (exclusion_query_json_resolved) next.exclusion_query_json = "";
    return next;
}

const NativeDenseConstraints = struct {
    positive_filter: bool = false,
    filter_ids: []const u64 = &.{},
    filter_ids_owned: bool = false,
    exclude_ids: []const u64 = &.{},
    exclude_ids_owned: bool = false,
    resolved_stored_filters: bool = false,
    filter_query_json_resolved: bool = false,
    exclusion_query_json_resolved: bool = false,

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
    filter_query_json_resolved: bool = false,
    exclusion_query_json_resolved: bool = false,

    fn deinit(self: *NativeDocIdConstraints, alloc: Allocator) void {
        if (self.filter_doc_ids_owned) freeDocIdSlice(alloc, self.filter_doc_ids);
        if (self.exclude_doc_ids_owned) freeDocIdSlice(alloc, self.exclude_doc_ids);
        if (self.filter_doc_nums_owned and self.filter_doc_nums.len > 0) alloc.free(@constCast(self.filter_doc_nums));
        if (self.exclude_doc_nums_owned and self.exclude_doc_nums.len > 0) alloc.free(@constCast(self.exclude_doc_nums));
        self.* = undefined;
    }
};

pub const StructuredFilterResolverExecutor = struct {
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
    all_docs_visible: ?*const fn (
        ctx: ?*anyopaque,
        generation: ?u64,
    ) anyerror!bool = null,
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

pub fn resolveStructuredDocFilterForComposedAlloc(
    alloc: Allocator,
    req: types.SearchRequest,
    executor: StructuredFilterResolverExecutor,
) !?doc_set.ResolvedDocFilter {
    if (req.resolved_doc_filter != null) return null;
    if (req.filter_query_json.len == 0 and req.exclusion_query_json.len == 0) return null;

    var active_executor = executor;
    if (active_executor.identity_read_generation == null) active_executor.identity_read_generation = req.identity_read_generation;
    var cache = StructuredFilterDocSetCache{};
    defer cache.deinit(alloc);

    var out = doc_set.ResolvedDocFilter{};
    errdefer out.deinit(alloc);
    var changed = false;

    if (req.filter_query_json.len > 0) {
        var include = (try collectStructuredFilterResolvedDocSetCachedAlloc(alloc, req, active_executor, &cache, req.filter_query_json)) orelse return null;
        defer include.deinit(alloc);
        var next_include = (try doc_set.intersectAlloc(alloc, &out.include, &include)) orelse return null;
        errdefer next_include.deinit(alloc);
        out.include.deinit(alloc);
        out.include = next_include;
        changed = true;
    }

    if (req.exclusion_query_json.len > 0) {
        var exclude = (try collectStructuredFilterResolvedDocSetCachedAlloc(alloc, req, active_executor, &cache, req.exclusion_query_json)) orelse return null;
        defer exclude.deinit(alloc);
        var next_exclude = (try doc_set.unionAlloc(alloc, &out.exclude, &exclude)) orelse return null;
        errdefer next_exclude.deinit(alloc);
        out.exclude.deinit(alloc);
        out.exclude = next_exclude;
        changed = true;
    }

    return if (changed) out else null;
}

pub fn resolveStructuredTextDocNumFilterForComposedAlloc(
    alloc: Allocator,
    req: types.SearchRequest,
    executor: StructuredFilterResolverExecutor,
) !?ResolvedTextDocNumFilter {
    if (req.resolved_doc_filter != null) return null;
    if (req.filter_query_json.len == 0 and req.exclusion_query_json.len == 0) return null;

    var active_executor = executor;
    if (active_executor.identity_read_generation == null) active_executor.identity_read_generation = req.identity_read_generation;
    const all_visible = if (active_executor.all_docs_visible) |visible|
        try visible(active_executor.ctx, active_executor.identity_read_generation)
    else
        false;
    if (!all_visible) return null;

    var out = ResolvedTextDocNumFilter{};
    errdefer out.deinit(alloc);
    var changed = false;

    if (req.filter_query_json.len > 0) {
        const include = (try collectStructuredFilterTextDocNumsAlloc(alloc, req, active_executor, req.filter_query_json)) orelse return null;
        out.include.deinit(alloc);
        out.include = include;
        changed = true;
    }

    if (req.exclusion_query_json.len > 0) {
        const exclude = (try collectStructuredFilterTextDocNumsAlloc(alloc, req, active_executor, req.exclusion_query_json)) orelse return null;
        out.exclude.deinit(alloc);
        out.exclude = exclude;
        changed = true;
    }

    return if (changed) out else null;
}

const StructuredFilterDocSetCache = struct {
    const Entry = struct {
        filter_query_json: []u8,
        identity_namespace: ?doc_identity.Namespace,
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
        identity_namespace: doc_identity.Namespace,
        identity_read_generation: u64,
    ) ?*const doc_set.ResolvedDocSet {
        return self.getWithNamespace(filter_query_json, identity_namespace, identity_read_generation);
    }

    fn getWithNamespace(
        self: *const StructuredFilterDocSetCache,
        filter_query_json: []const u8,
        identity_namespace: ?doc_identity.Namespace,
        identity_read_generation: ?u64,
    ) ?*const doc_set.ResolvedDocSet {
        for (self.entries.items) |*entry| {
            if (optionalIdentityNamespaceEql(entry.identity_namespace, identity_namespace) and
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
        identity_namespace: doc_identity.Namespace,
        identity_read_generation: u64,
        set: *const doc_set.ResolvedDocSet,
    ) !void {
        return try self.putCloneWithNamespaceAlloc(alloc, filter_query_json, identity_namespace, identity_read_generation, set);
    }

    fn putCloneWithNamespaceAlloc(
        self: *StructuredFilterDocSetCache,
        alloc: Allocator,
        filter_query_json: []const u8,
        identity_namespace: ?doc_identity.Namespace,
        identity_read_generation: ?u64,
        set: *const doc_set.ResolvedDocSet,
    ) !void {
        const owned_query = try alloc.dupe(u8, filter_query_json);
        errdefer alloc.free(owned_query);
        var owned_set = try doc_set.cloneAlloc(alloc, set);
        errdefer owned_set.deinit(alloc);
        try self.entries.append(alloc, .{
            .filter_query_json = owned_query,
            .identity_namespace = identity_namespace,
            .identity_read_generation = identity_read_generation,
            .set = owned_set,
        });
        owned_set = .none;
    }
};

fn optionalIdentityNamespaceEql(a: ?doc_identity.Namespace, b: ?doc_identity.Namespace) bool {
    if (a) |left| {
        if (b) |right| return left.eql(right);
        return false;
    }
    return b == null;
}

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
    const bench_query_profile = shouldLogBenchQueryProfile();
    const total_start_ns = if (bench_query_profile) platform_time.monotonicNs() else 0;
    var filter_json_ns: u64 = 0;
    var exclusion_json_ns: u64 = 0;
    var active_executor = executor;
    if (active_executor.identity_read_generation == null) active_executor.identity_read_generation = req.identity_read_generation;
    var out = NativeDocIdConstraints{};
    errdefer out.deinit(alloc);
    var structured_filter_doc_sets = StructuredFilterDocSetCache{};
    defer structured_filter_doc_sets.deinit(alloc);

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

    if (req.full_text) |text_query| {
        if (try collectFullTextResolvedDocSetAlloc(alloc, req, active_executor, text_query)) |resolved| {
            var owned_resolved = resolved;
            defer owned_resolved.deinit(alloc);
            const filter = doc_set.ResolvedDocFilter{
                .include = owned_resolved,
                .exclude = .none,
            };
            try applyResolvedDocFilterToNativeConstraintsAlloc(alloc, &out, &filter, active_executor);
        }
    }

    if (req.filter_query_json.len > 0) {
        const phase_start_ns = if (bench_query_profile) platform_time.monotonicNs() else 0;
        if (try collectStructuredFilterResolvedDocSetCachedAlloc(alloc, req, active_executor, &structured_filter_doc_sets, req.filter_query_json)) |resolved| {
            var owned_resolved = resolved;
            defer owned_resolved.deinit(alloc);
            const filter = doc_set.ResolvedDocFilter{
                .include = owned_resolved,
                .exclude = .none,
            };
            try applyResolvedDocFilterToNativeConstraintsAlloc(alloc, &out, &filter, active_executor);
            out.filter_query_json_resolved = true;
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
            out.filter_query_json_resolved = true;
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
        if (bench_query_profile) filter_json_ns += platform_time.monotonicNs() - phase_start_ns;
    }

    if (req.exclusion_query_json.len > 0) {
        const phase_start_ns = if (bench_query_profile) platform_time.monotonicNs() else 0;
        if (try collectStructuredFilterResolvedDocSetCachedAlloc(alloc, req, active_executor, &structured_filter_doc_sets, req.exclusion_query_json)) |resolved| {
            var owned_resolved = resolved;
            defer owned_resolved.deinit(alloc);
            const filter = doc_set.ResolvedDocFilter{
                .include = .all,
                .exclude = owned_resolved,
            };
            try applyResolvedDocFilterToNativeConstraintsAlloc(alloc, &out, &filter, active_executor);
            out.exclusion_query_json_resolved = true;
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
            out.exclusion_query_json_resolved = true;
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
                    out.exclusion_query_json_resolved = true;
                    out.resolved_stored_filters = true;
                }
            }
        }
        if (bench_query_profile) exclusion_json_ns += platform_time.monotonicNs() - phase_start_ns;
    }

    if (active_executor.apply_live_all_docs and !out.positive_filter) {
        try applyLiveAllDocFilterToNativeConstraintsAlloc(alloc, &out, active_executor);
    }

    if (bench_query_profile) {
        std.log.info(
            "antfly_bench_doc_constraints total_us={d} filter_json_us={d} exclusion_json_us={d} positive_filter={} filter_doc_nums={d} filter_doc_ids={d} exclude_doc_nums={d} exclude_doc_ids={d}",
            .{
                nsToUs(platform_time.monotonicNs() - total_start_ns),
                nsToUs(filter_json_ns),
                nsToUs(exclusion_json_ns),
                out.positive_filter,
                out.filter_doc_nums.len,
                out.filter_doc_ids.len,
                out.exclude_doc_nums.len,
                out.exclude_doc_ids.len,
            },
        );
    }
    return out;
}

fn collectFullTextResolvedDocSetAlloc(
    alloc: Allocator,
    req: types.SearchRequest,
    executor: StructuredFilterResolverExecutor,
    text_query: types.TextQuery,
) !?doc_set.ResolvedDocSet {
    const text_entry = try resolveFilterTextIndexEntry(executor, req.primary_text_index_name, req.index_name) orelse return null;

    var arena = std.heap.ArenaAllocator.init(alloc);
    defer arena.deinit();
    const arena_alloc = arena.allocator();
    const search_query = try textQueryToSearchQuery(arena_alloc, text_query, text_entry.text_analysis, text_entry.runtime_schema);

    return try collectSearchQueryResolvedDocSetAlloc(alloc, arena_alloc, executor, text_entry, search_query);
}

fn resolvedDocFilterFromRequest(req: types.SearchRequest) ?*const doc_set.ResolvedDocFilter {
    const ptr = req.resolved_doc_filter orelse return null;
    return @ptrCast(@alignCast(ptr));
}

fn resolvedTextDocNumFilterFromRequest(req: types.SearchRequest) ?*const ResolvedTextDocNumFilter {
    const ptr = req.resolved_text_doc_filter orelse return null;
    return @ptrCast(@alignCast(ptr));
}

fn applyResolvedTextDocNumFilterAlloc(
    alloc: Allocator,
    out: *NativeDocIdConstraints,
    filter: *const ResolvedTextDocNumFilter,
) !void {
    switch (filter.exclude) {
        .all => {
            markNativeDocIdConstraintsEmpty(out, alloc);
            return;
        },
        .none => {},
        .doc_nums => {},
    }
    switch (filter.include) {
        .none => {
            markNativeDocIdConstraintsEmpty(out, alloc);
            return;
        },
        .all => {},
        .doc_nums => |doc_nums| {
            const owned = try alloc.dupe(u32, doc_nums);
            if (out.positive_filter and out.filter_doc_nums.len > 0) {
                const intersected = try intersectDocNumsAlloc(alloc, out.filter_doc_nums, owned);
                if (out.filter_doc_nums_owned and out.filter_doc_nums.len > 0) alloc.free(@constCast(out.filter_doc_nums));
                alloc.free(owned);
                out.filter_doc_nums = intersected;
            } else {
                out.filter_doc_nums = owned;
            }
            out.filter_doc_nums_owned = true;
            out.positive_filter = true;
            out.resolved_stored_filters = true;
        },
    }

    switch (filter.exclude) {
        .all, .none => {},
        .doc_nums => |doc_nums| {
            const owned = try alloc.dupe(u32, doc_nums);
            if (out.exclude_doc_nums.len > 0) {
                const merged = try unionDocNumsAlloc(alloc, out.exclude_doc_nums, owned);
                if (out.exclude_doc_nums_owned and out.exclude_doc_nums.len > 0) alloc.free(@constCast(out.exclude_doc_nums));
                alloc.free(owned);
                out.exclude_doc_nums = merged;
            } else {
                out.exclude_doc_nums = owned;
            }
            out.exclude_doc_nums_owned = true;
            out.resolved_stored_filters = true;
        },
    }
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
    if (include_set.* == .none) {
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

    // The include set came from the live filter itself; re-running the live
    // filter over it would redo one visibility probe per document.
    var already_filtered_executor = executor;
    already_filtered_executor.live_filter_doc_set = null;

    const resolved_stored_filters_before_live_filter = out.resolved_stored_filters;
    try applyResolvedDocFilterToNativeConstraintsAlloc(alloc, out, &owned_filter, already_filtered_executor);
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
    if (include_set.* == .none) {
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

fn searchTextNeedsLateVisibilityFilter(
    executor: SearchTextQueryExecutor,
    can_apply_live_all_docs: bool,
    has_native_positive_filter: bool,
    generation: ?u64,
) !bool {
    if (executor.requires_full_candidate_visibility_filter) |requires| {
        if (try requires(executor.ctx, generation)) return true;
    }
    if (has_native_positive_filter) return false;
    if (can_apply_live_all_docs) return false;
    if (executor.live_filter_doc_set == null) return false;
    if (executor.all_docs_visible) |all_visible| {
        return !(try all_visible(executor.ctx, generation));
    }
    return true;
}

fn lateVisibilityExactCandidateBudgetFromRaw(raw: ?[]const u8) u32 {
    const value = raw orelse return default_late_visibility_exact_candidate_budget;
    if (value.len == 0) return default_late_visibility_exact_candidate_budget;
    const parsed = std.fmt.parseUnsigned(u32, value, 10) catch return default_late_visibility_exact_candidate_budget;
    return if (parsed == 0) std.math.maxInt(u32) else parsed;
}

fn lateVisibilityExactCandidateBudget() u32 {
    return lateVisibilityExactCandidateBudgetFromRaw(getenv("ANTFLY_TEXT_LATE_VISIBILITY_EXACT_CANDIDATE_BUDGET"));
}

fn boundedU32(count: anytype) u32 {
    return @intCast(@min(count, @as(@TypeOf(count), std.math.maxInt(u32))));
}

fn effectiveTextCandidateLimit(snapshot_doc_count: u64, constraints: NativeDocIdConstraints) u32 {
    if (constraints.positive_filter) return boundedU32(constraints.filter_doc_nums.len);
    return boundedU32(snapshot_doc_count);
}

fn enforceLateVisibilityExactCandidateBudget(candidate_limit: u32, budget: u32) !void {
    if (candidate_limit <= budget) return;
    return error.QueryCandidateBudgetExceeded;
}

const ExactSortBudgetRejectionReason = enum {
    text_exact_late_visibility_totals,
    text_field_sort_candidate_window,
    match_all_candidate_collect_limit,
    match_all_exact_candidate_window,
};

fn exactSortBudgetRejectionReasonName(reason: ExactSortBudgetRejectionReason) []const u8 {
    return switch (reason) {
        .text_exact_late_visibility_totals => "text_exact_late_visibility_totals",
        .text_field_sort_candidate_window => "text_field_sort_candidate_window",
        .match_all_candidate_collect_limit => "match_all_candidate_collect_limit",
        .match_all_exact_candidate_window => "match_all_exact_candidate_window",
    };
}

fn logExactSortBudgetRejection(
    source: []const u8,
    reason: ExactSortBudgetRejectionReason,
    index_name: ?[]const u8,
    candidates: u32,
    budget: u32,
    plan: ?SortExecutionPlan,
) void {
    std.log.warn(
        "antfly_exact_sort_budget_reject source={s} reason={s} index={s} candidates={d} budget={d} plan={s} exactness={s}",
        .{
            source,
            exactSortBudgetRejectionReasonName(reason),
            index_name orelse "",
            candidates,
            budget,
            if (plan) |p| sortExecutionPlanKindName(p.kind) else "none",
            if (plan) |p| sortPlanExactnessName(sortExecutionPlanExactness(p)) else "unspecified",
        },
    );
}

fn checkSearchRequestDeadline(req: types.SearchRequest) !void {
    const deadline_ns = req.execution_deadline_ns orelse return;
    if (platform_time.monotonicNs() >= deadline_ns) return error.Timeout;
}

test "exact sort budget rejection reason names are stable for diagnostics" {
    try std.testing.expectEqualStrings("text_exact_late_visibility_totals", exactSortBudgetRejectionReasonName(.text_exact_late_visibility_totals));
    try std.testing.expectEqualStrings("text_field_sort_candidate_window", exactSortBudgetRejectionReasonName(.text_field_sort_candidate_window));
    try std.testing.expectEqualStrings("match_all_candidate_collect_limit", exactSortBudgetRejectionReasonName(.match_all_candidate_collect_limit));
    try std.testing.expectEqualStrings("match_all_exact_candidate_window", exactSortBudgetRejectionReasonName(.match_all_exact_candidate_window));
}

test "text late visibility requirement overrides positive native filter" {
    const callbacks = struct {
        fn requiresFullCandidateVisibilityFilter(_: ?*anyopaque, _: ?u64) anyerror!bool {
            return true;
        }
    };

    const needs_late_filter = try searchTextNeedsLateVisibilityFilter(.{
        .ctx = null,
        .text_index_entry = undefined,
        .text_index_is_chunk_backed = undefined,
        .search_match_all = undefined,
        .project_stored_search = undefined,
        .load_stored = undefined,
        .requires_full_candidate_visibility_filter = callbacks.requiresFullCandidateVisibilityFilter,
        .postprocess = undefined,
    }, true, true, null);
    try std.testing.expect(needs_late_filter);
}

test "text late visibility candidate limit uses positive native filter bound" {
    try std.testing.expectEqual(@as(u32, 2), effectiveTextCandidateLimit(1_000_000, .{
        .positive_filter = true,
        .filter_doc_nums = &.{ 10, 20 },
    }));
    try std.testing.expectEqual(std.math.maxInt(u32), effectiveTextCandidateLimit(@as(u64, std.math.maxInt(u32)) + 99, .{}));
}

test "text late visibility exact candidate budget parses disabled and fallback values" {
    try std.testing.expectEqual(default_late_visibility_exact_candidate_budget, lateVisibilityExactCandidateBudgetFromRaw(null));
    try std.testing.expectEqual(default_late_visibility_exact_candidate_budget, lateVisibilityExactCandidateBudgetFromRaw(""));
    try std.testing.expectEqual(default_late_visibility_exact_candidate_budget, lateVisibilityExactCandidateBudgetFromRaw("bad"));
    try std.testing.expectEqual(@as(u32, 42), lateVisibilityExactCandidateBudgetFromRaw("42"));
    try std.testing.expectEqual(std.math.maxInt(u32), lateVisibilityExactCandidateBudgetFromRaw("0"));
}

test "text late visibility exact candidate budget rejects oversized exact windows" {
    try enforceLateVisibilityExactCandidateBudget(100, 100);
    try std.testing.expectError(error.QueryCandidateBudgetExceeded, enforceLateVisibilityExactCandidateBudget(101, 100));
}

fn paginateSearchResultInPlace(result: *types.SearchResult, offset: u32, limit: u32) !void {
    const alloc = result.alloc;
    const available: u32 = @intCast(@min(result.hits.len, @as(usize, std.math.maxInt(u32))));
    const start = @min(offset, available);
    const end_u64 = @min(@as(u64, start) + @as(u64, limit), @as(u64, available));
    const start_usize: usize = @intCast(start);
    const end_usize: usize = @intCast(end_u64);
    if (start_usize == 0 and end_usize == result.hits.len) return;

    const selected = try alloc.alloc(types.SearchHit, end_usize - start_usize);
    errdefer alloc.free(selected);
    for (result.hits, 0..) |*hit, i| {
        if (i >= start_usize and i < end_usize) {
            selected[i - start_usize] = hit.*;
            hit.* = undefined;
        } else {
            hit.deinit(alloc);
        }
    }
    if (result.hits.len > 0) alloc.free(result.hits);
    result.hits = selected;
}

const SortValue = union(enum) {
    null_value,
    bool_value: bool,
    integer: i64,
    number: f64,
    number_string: []const u8,
    string: []const u8,

    fn deinit(self: @This(), alloc: Allocator) void {
        switch (self) {
            .string, .number_string => |text| alloc.free(@constCast(text)),
            else => {},
        }
    }
};

const NativeSortValueLoader = struct {
    ctx: ?*anyopaque,
    require_native: bool = false,
    load: *const fn (
        ctx: ?*anyopaque,
        alloc: Allocator,
        hit: types.SearchHit,
        field: []const u8,
    ) anyerror!?SortValue,
};

const SortExecutionPlanKind = enum {
    none,
    id_only,
    id_seek,
    sorted_segment_seek,
    score_top_k,
    native_doc_values_top_n,
    distributed_k_way_merge,
    stored_json_debug,
    unsupported_exact_sort,
};

fn sortExecutionPlanKindName(kind: SortExecutionPlanKind) []const u8 {
    return switch (kind) {
        .none => "none",
        .id_only => "id_only",
        .id_seek => "id_seek",
        .sorted_segment_seek => "sorted_segment_seek",
        .score_top_k => "score_top_k",
        .native_doc_values_top_n => "native_doc_values_top_n",
        .distributed_k_way_merge => "distributed_k_way_merge",
        .stored_json_debug => "stored_json_debug",
        .unsupported_exact_sort => "unsupported_exact_sort",
    };
}

const SortPlanExactness = enum {
    unspecified,
    none,
    exact,
    bounded_exact,
    unsupported,
};

fn sortPlanExactnessName(exactness: SortPlanExactness) []const u8 {
    return switch (exactness) {
        .unspecified => "unspecified",
        .none => "none",
        .exact => "exact",
        .bounded_exact => "bounded_exact",
        .unsupported => "unsupported",
    };
}

const SortPlanSource = enum {
    unspecified,
    none,
    candidate_collector,
    primary_key_scan,
    sorted_segment_scan,
    score_top_k,
    doc_values_collector,
    distributed_merge,
    stored_json_debug,
    unsupported,
};

fn sortPlanSourceName(source: SortPlanSource) []const u8 {
    return switch (source) {
        .unspecified => "unspecified",
        .none => "none",
        .candidate_collector => "candidate_collector",
        .primary_key_scan => "primary_key_scan",
        .sorted_segment_scan => "sorted_segment_scan",
        .score_top_k => "score_top_k",
        .doc_values_collector => "doc_values_collector",
        .distributed_merge => "distributed_merge",
        .stored_json_debug => "stored_json_debug",
        .unsupported => "unsupported",
    };
}

const SortPlanCursorSupport = enum {
    unspecified,
    none,
    comparator,
    segment_seek,
    distributed_seek,
    unsupported,
};

fn sortPlanCursorSupportName(cursor_support: SortPlanCursorSupport) []const u8 {
    return switch (cursor_support) {
        .unspecified => "unspecified",
        .none => "none",
        .comparator => "comparator",
        .segment_seek => "segment_seek",
        .distributed_seek => "distributed_seek",
        .unsupported => "unsupported",
    };
}

const SortPlanSourceLoad = enum {
    unspecified,
    none,
    source_free,
    projected_source_after_page,
    stored_source_required,
    unsupported,
};

fn sortPlanSourceLoadName(source_load: SortPlanSourceLoad) []const u8 {
    return switch (source_load) {
        .unspecified => "unspecified",
        .none => "none",
        .source_free => "source_free",
        .projected_source_after_page => "projected_source_after_page",
        .stored_source_required => "stored_source_required",
        .unsupported => "unsupported",
    };
}

const SortPlanDistributedBehavior = enum {
    unspecified,
    none,
    shard_local_only,
    coordinator_merge,
    unsupported,
};

fn sortPlanDistributedBehaviorName(distributed_behavior: SortPlanDistributedBehavior) []const u8 {
    return switch (distributed_behavior) {
        .unspecified => "unspecified",
        .none => "none",
        .shard_local_only => "shard_local_only",
        .coordinator_merge => "coordinator_merge",
        .unsupported => "unsupported",
    };
}

test "sort execution plan kind names are stable for profiles" {
    try std.testing.expectEqualStrings("none", sortExecutionPlanKindName(.none));
    try std.testing.expectEqualStrings("id_only", sortExecutionPlanKindName(.id_only));
    try std.testing.expectEqualStrings("id_seek", sortExecutionPlanKindName(.id_seek));
    try std.testing.expectEqualStrings("sorted_segment_seek", sortExecutionPlanKindName(.sorted_segment_seek));
    try std.testing.expectEqualStrings("score_top_k", sortExecutionPlanKindName(.score_top_k));
    try std.testing.expectEqualStrings("native_doc_values_top_n", sortExecutionPlanKindName(.native_doc_values_top_n));
    try std.testing.expectEqualStrings("distributed_k_way_merge", sortExecutionPlanKindName(.distributed_k_way_merge));
    try std.testing.expectEqualStrings("stored_json_debug", sortExecutionPlanKindName(.stored_json_debug));
    try std.testing.expectEqualStrings("unsupported_exact_sort", sortExecutionPlanKindName(.unsupported_exact_sort));
}

test "sort execution plan dimension names are stable for profiles" {
    try std.testing.expectEqualStrings("exact", sortPlanExactnessName(.exact));
    try std.testing.expectEqualStrings("bounded_exact", sortPlanExactnessName(.bounded_exact));
    try std.testing.expectEqualStrings("candidate_collector", sortPlanSourceName(.candidate_collector));
    try std.testing.expectEqualStrings("score_top_k", sortPlanSourceName(.score_top_k));
    try std.testing.expectEqualStrings("doc_values_collector", sortPlanSourceName(.doc_values_collector));
    try std.testing.expectEqualStrings("primary_key_scan", sortPlanSourceName(.primary_key_scan));
    try std.testing.expectEqualStrings("sorted_segment_scan", sortPlanSourceName(.sorted_segment_scan));
    try std.testing.expectEqualStrings("distributed_merge", sortPlanSourceName(.distributed_merge));
    try std.testing.expectEqualStrings("comparator", sortPlanCursorSupportName(.comparator));
    try std.testing.expectEqualStrings("segment_seek", sortPlanCursorSupportName(.segment_seek));
    try std.testing.expectEqualStrings("distributed_seek", sortPlanCursorSupportName(.distributed_seek));
    try std.testing.expectEqualStrings("projected_source_after_page", sortPlanSourceLoadName(.projected_source_after_page));
    try std.testing.expectEqualStrings("stored_source_required", sortPlanSourceLoadName(.stored_source_required));
    try std.testing.expectEqualStrings("shard_local_only", sortPlanDistributedBehaviorName(.shard_local_only));
    try std.testing.expectEqualStrings("coordinator_merge", sortPlanDistributedBehaviorName(.coordinator_merge));
}

const SortExecutionPlan = struct {
    kind: SortExecutionPlanKind,
    require_native: bool = false,
    exactness: SortPlanExactness = .unspecified,
    source: SortPlanSource = .unspecified,
    cursor_support: SortPlanCursorSupport = .unspecified,
    source_load: SortPlanSourceLoad = .unspecified,
    distributed_behavior: SortPlanDistributedBehavior = .unspecified,
    runtime_schema: ?runtime_schema_mod.TableSchema = null,
    index_sort_match: bool = false,
    sorted_segment_executor_available: bool = false,
};

fn defaultSortPlanExactness(kind: SortExecutionPlanKind) SortPlanExactness {
    return switch (kind) {
        .none => .none,
        .id_only, .id_seek, .sorted_segment_seek, .score_top_k, .native_doc_values_top_n, .distributed_k_way_merge => .exact,
        .stored_json_debug => .bounded_exact,
        .unsupported_exact_sort => .unsupported,
    };
}

fn defaultSortPlanSource(kind: SortExecutionPlanKind) SortPlanSource {
    return switch (kind) {
        .none => .none,
        .id_only => .candidate_collector,
        .id_seek => .primary_key_scan,
        .sorted_segment_seek => .sorted_segment_scan,
        .score_top_k => .score_top_k,
        .native_doc_values_top_n => .doc_values_collector,
        .distributed_k_way_merge => .distributed_merge,
        .stored_json_debug => .stored_json_debug,
        .unsupported_exact_sort => .unsupported,
    };
}

fn defaultSortPlanCursorSupport(kind: SortExecutionPlanKind) SortPlanCursorSupport {
    return switch (kind) {
        .none => .none,
        .id_only, .score_top_k => .comparator,
        .id_seek, .sorted_segment_seek => .segment_seek,
        .native_doc_values_top_n, .stored_json_debug => .comparator,
        .distributed_k_way_merge => .distributed_seek,
        .unsupported_exact_sort => .unsupported,
    };
}

fn defaultSortPlanSourceLoad(kind: SortExecutionPlanKind) SortPlanSourceLoad {
    return switch (kind) {
        .none => .none,
        .id_only, .id_seek, .sorted_segment_seek, .score_top_k, .native_doc_values_top_n, .distributed_k_way_merge => .projected_source_after_page,
        .stored_json_debug => .stored_source_required,
        .unsupported_exact_sort => .unsupported,
    };
}

fn defaultSortPlanDistributedBehavior(kind: SortExecutionPlanKind) SortPlanDistributedBehavior {
    return switch (kind) {
        .none => .none,
        .id_only, .id_seek, .sorted_segment_seek, .score_top_k, .native_doc_values_top_n, .stored_json_debug => .shard_local_only,
        .distributed_k_way_merge => .coordinator_merge,
        .unsupported_exact_sort => .unsupported,
    };
}

fn sortExecutionPlanExactness(plan: SortExecutionPlan) SortPlanExactness {
    return if (plan.exactness == .unspecified) defaultSortPlanExactness(plan.kind) else plan.exactness;
}

fn sortExecutionPlanSource(plan: SortExecutionPlan) SortPlanSource {
    return if (plan.source == .unspecified) defaultSortPlanSource(plan.kind) else plan.source;
}

fn sortExecutionPlanCursorSupport(plan: SortExecutionPlan) SortPlanCursorSupport {
    return if (plan.cursor_support == .unspecified) defaultSortPlanCursorSupport(plan.kind) else plan.cursor_support;
}

fn sortExecutionPlanSourceLoad(plan: SortExecutionPlan) SortPlanSourceLoad {
    return if (plan.source_load == .unspecified) defaultSortPlanSourceLoad(plan.kind) else plan.source_load;
}

fn sortExecutionPlanSourceLoadForRequest(plan: SortExecutionPlan, req: types.SearchRequest) SortPlanSourceLoad {
    const source_load = sortExecutionPlanSourceLoad(plan);
    return switch (source_load) {
        .projected_source_after_page => if (req.include_stored) .projected_source_after_page else .source_free,
        else => source_load,
    };
}

fn sortExecutionPlanDistributedBehavior(plan: SortExecutionPlan) SortPlanDistributedBehavior {
    return if (plan.distributed_behavior == .unspecified) defaultSortPlanDistributedBehavior(plan.kind) else plan.distributed_behavior;
}

test "sort execution plan dimensions default from kind unless explicit" {
    const text_id_plan = SortExecutionPlan{ .kind = .id_only };
    try std.testing.expectEqual(SortPlanExactness.exact, sortExecutionPlanExactness(text_id_plan));
    try std.testing.expectEqual(SortPlanSource.candidate_collector, sortExecutionPlanSource(text_id_plan));
    try std.testing.expectEqual(SortPlanCursorSupport.comparator, sortExecutionPlanCursorSupport(text_id_plan));

    const match_all_id_plan = SortExecutionPlan{ .kind = .id_seek };
    try std.testing.expectEqual(SortPlanSource.primary_key_scan, sortExecutionPlanSource(match_all_id_plan));
    try std.testing.expectEqual(SortPlanCursorSupport.segment_seek, sortExecutionPlanCursorSupport(match_all_id_plan));

    const sorted_seek_plan = SortExecutionPlan{ .kind = .sorted_segment_seek };
    try std.testing.expectEqual(SortPlanSource.sorted_segment_scan, sortExecutionPlanSource(sorted_seek_plan));
    try std.testing.expectEqual(SortPlanCursorSupport.segment_seek, sortExecutionPlanCursorSupport(sorted_seek_plan));
    try std.testing.expectEqual(SortPlanDistributedBehavior.shard_local_only, sortExecutionPlanDistributedBehavior(sorted_seek_plan));

    const native_plan = SortExecutionPlan{ .kind = .native_doc_values_top_n };
    try std.testing.expectEqual(SortPlanSource.doc_values_collector, sortExecutionPlanSource(native_plan));
    try std.testing.expectEqual(SortPlanSourceLoad.projected_source_after_page, sortExecutionPlanSourceLoad(native_plan));
    try std.testing.expectEqual(SortPlanSourceLoad.source_free, sortExecutionPlanSourceLoadForRequest(native_plan, .{
        .include_stored = false,
    }));
    try std.testing.expectEqual(SortPlanSourceLoad.projected_source_after_page, sortExecutionPlanSourceLoadForRequest(native_plan, .{
        .include_stored = true,
    }));

    const debug_plan = SortExecutionPlan{ .kind = .stored_json_debug };
    try std.testing.expectEqual(SortPlanSourceLoad.stored_source_required, sortExecutionPlanSourceLoadForRequest(debug_plan, .{
        .include_stored = false,
    }));

    const score_plan = SortExecutionPlan{ .kind = .score_top_k };
    try std.testing.expectEqual(SortPlanExactness.exact, sortExecutionPlanExactness(score_plan));
    try std.testing.expectEqual(SortPlanSource.score_top_k, sortExecutionPlanSource(score_plan));
    try std.testing.expectEqual(SortPlanCursorSupport.comparator, sortExecutionPlanCursorSupport(score_plan));

    const distributed_plan = SortExecutionPlan{ .kind = .distributed_k_way_merge };
    try std.testing.expectEqual(SortPlanSource.distributed_merge, sortExecutionPlanSource(distributed_plan));
    try std.testing.expectEqual(SortPlanCursorSupport.distributed_seek, sortExecutionPlanCursorSupport(distributed_plan));
    try std.testing.expectEqual(SortPlanDistributedBehavior.coordinator_merge, sortExecutionPlanDistributedBehavior(distributed_plan));
}

const DecoratedSortHit = struct {
    hit: types.SearchHit,
    keys: []SortValue,

    fn deinit(self: *@This(), alloc: Allocator) void {
        self.hit.deinit(alloc);
        freeSortValues(alloc, self.keys);
        self.* = undefined;
    }
};

const SortCollectorProfile = struct {
    candidate_count: u64 = 0,
    cursor_rejected_count: u64 = 0,
    admitted_count: u64 = 0,
    replaced_count: u64 = 0,
    discarded_count: u64 = 0,
    selected_count: u64 = 0,
    decorate_ns: u64 = 0,
    native_doc_value_load_ns: u64 = 0,
    native_doc_value_hit_count: u64 = 0,
    native_doc_value_miss_count: u64 = 0,
    stored_json_load_ns: u64 = 0,
    stored_json_load_count: u64 = 0,
    final_sort_ns: u64 = 0,
    total_ns: u64 = 0,
    window_capacity: usize = 0,
    window_len: usize = 0,
};

fn validateSortExecutionPlanForRuntimeMode(
    plan: SortExecutionPlan,
    native_loader: ?NativeSortValueLoader,
    is_test_runtime: bool,
) !void {
    switch (plan.kind) {
        .none => return error.InvalidQueryRequest,
        .sorted_segment_seek, .distributed_k_way_merge, .unsupported_exact_sort => return error.UnsupportedQueryRequest,
        .native_doc_values_top_n => {
            if (native_loader == null or plan.runtime_schema == null) return error.UnsupportedQueryRequest;
        },
        .stored_json_debug => if (!is_test_runtime) return error.UnsupportedQueryRequest,
        .id_only, .id_seek, .score_top_k => {},
    }
}

fn sortRequestIsOnlyIdSeek(req: types.SearchRequest) bool {
    return req.order_by.len == 1 and sortFieldIsId(req.order_by[0]) and !req.order_by[0].desc;
}

fn validateSortExecutionPlanMatchesRequest(plan: SortExecutionPlan, req: types.SearchRequest) !void {
    switch (plan.kind) {
        .none, .sorted_segment_seek, .distributed_k_way_merge, .stored_json_debug, .unsupported_exact_sort => {},
        .id_only => if (requestHasScoreSort(req) or requestNeedsNativeSortValues(req)) return error.UnsupportedQueryRequest,
        .id_seek => if (!sortRequestIsOnlyIdSeek(req)) return error.UnsupportedQueryRequest,
        .score_top_k => if (!requestHasScoreSort(req) or requestNeedsNativeSortValues(req)) return error.UnsupportedQueryRequest,
        .native_doc_values_top_n => if (!requestNeedsNativeSortValues(req)) return error.UnsupportedQueryRequest,
    }
}

fn validateNativeDocValuesRuntimeMappings(plan: SortExecutionPlan, req: types.SearchRequest) !void {
    if (plan.kind != .native_doc_values_top_n) return;
    const schema = plan.runtime_schema orelse return error.UnsupportedQueryRequest;
    const cursor = activeSortCursor(req);
    for (req.order_by, 0..) |field, i| {
        if (!sortFieldNeedsNativeValue(field)) continue;
        const mapping = sortFieldMapping(schema, field.field) orelse return error.UnsupportedQueryRequest;
        if (mappedSortFieldRejectionReason(mapping) != null) return error.UnsupportedQueryRequest;
        if (cursor.len > 0 and !mappedSortCursorValueIsValid(mapping, cursor[i])) return error.InvalidQueryRequest;
    }
}

fn validateSortExecutionPlanForRuntime(req: types.SearchRequest, plan: SortExecutionPlan, native_loader: ?NativeSortValueLoader) !void {
    try validateSortExecutionPlanMatchesRequest(plan, req);
    try validateSortExecutionPlanForRuntimeMode(plan, native_loader, builtin.is_test);
    try validateNativeDocValuesRuntimeMappings(plan, req);
}

fn freeSortValues(alloc: Allocator, values: []SortValue) void {
    for (values) |value| value.deinit(alloc);
    if (values.len > 0) alloc.free(values);
}

fn deinitSortValues(values: []SortValue, alloc: Allocator) void {
    for (values) |value| value.deinit(alloc);
}

fn sortValueRank(value: SortValue) u8 {
    return switch (value) {
        .null_value => 0,
        .bool_value => 1,
        .integer, .number, .number_string => 2,
        .string => 3,
    };
}

fn sortValueAsFloat(value: SortValue) f64 {
    return switch (value) {
        .integer => |v| @floatFromInt(v),
        .number => |v| v,
        .number_string => |v| std.fmt.parseFloat(f64, v) catch std.math.nan(f64),
        else => std.math.nan(f64),
    };
}

fn compareFloatSortValues(a: f64, b: f64) std.math.Order {
    const a_nan = std.math.isNan(a);
    const b_nan = std.math.isNan(b);
    if (a_nan or b_nan) {
        if (a_nan and b_nan) return .eq;
        return if (a_nan) .gt else .lt;
    }
    return std.math.order(a, b);
}

fn compareIntToFloatSortValue(a: i64, b: f64) std.math.Order {
    if (std.math.isNan(b)) return .lt;

    const min_i64_f = -9223372036854775808.0;
    const max_i64_plus_one_f = 9223372036854775808.0;
    if (b < min_i64_f) return .gt;
    if (b >= max_i64_plus_one_f) return .lt;
    if (b == min_i64_f) return std.math.order(a, std.math.minInt(i64));

    const truncated: i64 = @intFromFloat(b);
    const truncated_f: f64 = @floatFromInt(truncated);
    if (b == truncated_f) return std.math.order(a, truncated);
    if (b > 0) return if (a <= truncated) .lt else .gt;
    return if (a < truncated) .lt else .gt;
}

fn compareNumberSortValues(a: SortValue, b: SortValue) std.math.Order {
    if (a == .integer and b == .integer) return std.math.order(a.integer, b.integer);
    if (a == .integer) return compareIntToFloatSortValue(a.integer, sortValueAsFloat(b));
    if (b == .integer) {
        return switch (compareIntToFloatSortValue(b.integer, sortValueAsFloat(a))) {
            .lt => .gt,
            .eq => .eq,
            .gt => .lt,
        };
    }
    return compareFloatSortValues(sortValueAsFloat(a), sortValueAsFloat(b));
}

fn compareSortValues(a: SortValue, b: SortValue) std.math.Order {
    const ar = sortValueRank(a);
    const br = sortValueRank(b);
    if (ar != br) return std.math.order(ar, br);
    return switch (a) {
        .null_value => .eq,
        .bool_value => |av| std.math.order(@intFromBool(av), @intFromBool(b.bool_value)),
        .integer, .number, .number_string => compareNumberSortValues(a, b),
        .string => |av| std.mem.order(u8, av, b.string),
    };
}

test "sort value numeric comparison preserves integer precision and nan policy" {
    try std.testing.expectEqual(std.math.Order.lt, compareSortValues(
        .{ .integer = std.math.maxInt(i64) },
        .{ .number = 9223372036854775808.0 },
    ));
    try std.testing.expectEqual(std.math.Order.gt, compareSortValues(
        .{ .integer = std.math.minInt(i64) + 1 },
        .{ .number = -9223372036854775808.0 },
    ));
    try std.testing.expectEqual(std.math.Order.lt, compareSortValues(
        .{ .integer = 1 },
        .{ .number = 1.25 },
    ));
    try std.testing.expectEqual(std.math.Order.gt, compareSortValues(
        .{ .integer = -1 },
        .{ .number = -1.25 },
    ));
    try std.testing.expectEqual(std.math.Order.gt, compareSortValues(
        .{ .number = std.math.nan(f64) },
        .{ .number = 1.0 },
    ));
    try std.testing.expectEqual(std.math.Order.eq, compareSortValues(
        .{ .number = std.math.nan(f64) },
        .{ .number = std.math.nan(f64) },
    ));
}

test "stored json debug sort plan is rejected outside test runtime" {
    try validateSortExecutionPlanForRuntimeMode(.{ .kind = .stored_json_debug }, null, true);
    try std.testing.expectError(
        error.UnsupportedQueryRequest,
        validateSortExecutionPlanForRuntimeMode(.{ .kind = .stored_json_debug }, null, false),
    );
}

test "future sort execution plans fail closed until executors exist" {
    try std.testing.expectError(
        error.UnsupportedQueryRequest,
        validateSortExecutionPlanForRuntimeMode(.{ .kind = .sorted_segment_seek }, null, true),
    );
    try std.testing.expectError(
        error.UnsupportedQueryRequest,
        validateSortExecutionPlanForRuntimeMode(.{ .kind = .distributed_k_way_merge }, null, true),
    );
}

test "sort execution plans reject incompatible order_by shapes" {
    const id_order = [_]types.SortField{.{ .field = "_id" }};
    const score_order = [_]types.SortField{.{ .field = "_score", .desc = true }};
    const field_order = [_]types.SortField{.{ .field = "rank" }};

    try validateSortExecutionPlanMatchesRequest(.{ .kind = .id_seek }, .{ .order_by = &id_order });
    try validateSortExecutionPlanMatchesRequest(.{ .kind = .score_top_k }, .{ .order_by = &score_order });

    try std.testing.expectError(error.UnsupportedQueryRequest, validateSortExecutionPlanMatchesRequest(.{ .kind = .id_seek }, .{ .order_by = &score_order }));
    try std.testing.expectError(error.UnsupportedQueryRequest, validateSortExecutionPlanMatchesRequest(.{ .kind = .score_top_k }, .{ .order_by = &field_order }));
    try std.testing.expectError(error.UnsupportedQueryRequest, validateSortExecutionPlanMatchesRequest(.{ .kind = .id_only }, .{ .order_by = &score_order }));
    try std.testing.expectError(error.UnsupportedQueryRequest, validateSortExecutionPlanMatchesRequest(.{ .kind = .native_doc_values_top_n }, .{ .order_by = &id_order }));
}

test "native doc values runtime plan validates mappings" {
    const rank_order = [_]types.SortField{.{ .field = "rank" }};
    const cursor = [_]std.json.Value{ .{ .string = "wrong" }, .{ .string = "doc:a" } };
    const Loader = struct {
        fn load(_: ?*anyopaque, _: Allocator, _: types.SearchHit, _: []const u8) anyerror!?SortValue {
            return .{ .integer = 1 };
        }
    };
    const loader = NativeSortValueLoader{ .require_native = true, .load = Loader.load };

    const non_sortable_templates = [_]runtime_schema_mod.DynamicTemplate{.{
        .name = "rank",
        .path_match = "rank",
        .mapping = .{
            .field_type = .numeric,
            .doc_values = true,
            .sortable = false,
            .analyzer = "keyword",
        },
    }};
    const non_sortable_schema = runtime_schema_mod.TableSchema{ .dynamic_templates = &non_sortable_templates };
    try std.testing.expectError(error.UnsupportedQueryRequest, validateSortExecutionPlanForRuntime(.{
        .order_by = &rank_order,
    }, .{
        .kind = .native_doc_values_top_n,
        .require_native = true,
        .runtime_schema = non_sortable_schema,
    }, loader));

    const sortable_templates = [_]runtime_schema_mod.DynamicTemplate{.{
        .name = "rank",
        .path_match = "rank",
        .mapping = .{
            .field_type = .numeric,
            .doc_values = true,
            .sortable = true,
            .analyzer = "keyword",
        },
    }};
    const sortable_schema = runtime_schema_mod.TableSchema{ .dynamic_templates = &sortable_templates };
    try std.testing.expectError(error.InvalidQueryRequest, validateSortExecutionPlanForRuntime(.{
        .order_by = &rank_order,
        .search_after = &cursor,
    }, .{
        .kind = .native_doc_values_top_n,
        .require_native = true,
        .runtime_schema = sortable_schema,
    }, loader));

    try std.testing.expectError(error.UnsupportedQueryRequest, validateSortExecutionPlanForRuntime(.{
        .order_by = &rank_order,
    }, .{
        .kind = .native_doc_values_top_n,
        .require_native = true,
        .runtime_schema = .{},
    }, loader));
}

fn compareDecoratedSortHits(req: types.SearchRequest, a: DecoratedSortHit, b: DecoratedSortHit) std.math.Order {
    for (req.order_by, 0..) |field, i| {
        const order = compareSortValues(a.keys[i], b.keys[i]);
        if (order != .eq) {
            if (field.desc) {
                return switch (order) {
                    .lt => .gt,
                    .eq => .eq,
                    .gt => .lt,
                };
            }
            return order;
        }
    }
    return .eq;
}

fn decoratedLessThan(req: types.SearchRequest, a: DecoratedSortHit, b: DecoratedSortHit) bool {
    return compareDecoratedSortHits(req, a, b) == .lt;
}

fn sortValueFromSortJson(plan: SortExecutionPlan, field: types.SortField, value: std.json.Value) !SortValue {
    if (!sortCursorValueIsScalar(value)) return error.InvalidQueryRequest;
    if (sortFieldIsScore(field) and !jsonValueIsNumeric(value)) return error.InvalidQueryRequest;
    if (sortFieldIsId(field) and value != .string) return error.InvalidQueryRequest;
    return try sortValueFromCursorJson(plan, field.field, value);
}

fn compareSearchHitSortValues(req: types.SearchRequest, plan: SortExecutionPlan, a: types.SearchHit, b: types.SearchHit) !std.math.Order {
    const field_count = effectiveSortFieldCount(req.order_by);
    if (a.sort_values.len != field_count or b.sort_values.len != field_count) return error.InvalidQueryRequest;
    for (0..field_count) |i| {
        const field = effectiveSortFieldAt(req.order_by, i);
        const a_value = try sortValueFromSortJson(plan, field, a.sort_values[i]);
        const b_value = try sortValueFromSortJson(plan, field, b.sort_values[i]);
        const order = compareSortValues(a_value, b_value);
        if (order != .eq) {
            if (field.desc) {
                return switch (order) {
                    .lt => .gt,
                    .eq => .eq,
                    .gt => .lt,
                };
            }
            return order;
        }
    }
    return .eq;
}

fn searchHitAllowedByCursor(req: types.SearchRequest, plan: SortExecutionPlan, hit: types.SearchHit) !bool {
    const field_count = effectiveSortFieldCount(req.order_by);
    if (hit.sort_values.len != field_count) return error.InvalidQueryRequest;
    const cursor = activeSortCursor(req);
    if (cursor.len == 0) return true;
    for (0..field_count) |i| {
        const field = effectiveSortFieldAt(req.order_by, i);
        const hit_value = try sortValueFromSortJson(plan, field, hit.sort_values[i]);
        const cursor_value = try sortValueFromCursorJson(plan, field.field, cursor[i]);
        const order = compareSortValues(hit_value, cursor_value);
        if (order != .eq) {
            const effective_order = if (field.desc) switch (order) {
                .lt => std.math.Order.gt,
                .eq => std.math.Order.eq,
                .gt => std.math.Order.lt,
            } else order;
            if (req.search_after.len > 0) return effective_order == .gt;
            if (req.search_before.len > 0) return effective_order == .lt;
            return true;
        }
    }
    return false;
}

const DistributedSortedShard = struct {
    hits: []const types.SearchHit = &.{},
};

const DistributedMergeHeapEntry = struct {
    shard_index: usize,
    hit_index: usize,
};

fn distributedMergeEntryLess(
    req: types.SearchRequest,
    plan: SortExecutionPlan,
    shards: []const DistributedSortedShard,
    a: DistributedMergeHeapEntry,
    b: DistributedMergeHeapEntry,
) !bool {
    const order = try compareSearchHitSortValues(req, plan, shards[a.shard_index].hits[a.hit_index], shards[b.shard_index].hits[b.hit_index]);
    if (order != .eq) return order == .lt;
    if (a.shard_index != b.shard_index) return a.shard_index < b.shard_index;
    return a.hit_index < b.hit_index;
}

fn distributedMergeHeapSwap(heap: []DistributedMergeHeapEntry, a: usize, b: usize) void {
    const tmp = heap[a];
    heap[a] = heap[b];
    heap[b] = tmp;
}

fn distributedMergeHeapSiftUp(
    req: types.SearchRequest,
    plan: SortExecutionPlan,
    shards: []const DistributedSortedShard,
    heap: []DistributedMergeHeapEntry,
    start_index: usize,
) !void {
    var index = start_index;
    while (index > 0) {
        const parent = (index - 1) / 2;
        if (!(try distributedMergeEntryLess(req, plan, shards, heap[index], heap[parent]))) break;
        distributedMergeHeapSwap(heap, index, parent);
        index = parent;
    }
}

fn distributedMergeHeapSiftDown(
    req: types.SearchRequest,
    plan: SortExecutionPlan,
    shards: []const DistributedSortedShard,
    heap: []DistributedMergeHeapEntry,
) !void {
    var index: usize = 0;
    while (true) {
        const left = index * 2 + 1;
        if (left >= heap.len) break;
        const right = left + 1;
        var best = left;
        if (right < heap.len and try distributedMergeEntryLess(req, plan, shards, heap[right], heap[left])) {
            best = right;
        }
        if (!(try distributedMergeEntryLess(req, plan, shards, heap[best], heap[index]))) break;
        distributedMergeHeapSwap(heap, index, best);
        index = best;
    }
}

fn distributedMergeHeapPush(
    req: types.SearchRequest,
    plan: SortExecutionPlan,
    shards: []const DistributedSortedShard,
    heap: []DistributedMergeHeapEntry,
    heap_len: *usize,
    entry: DistributedMergeHeapEntry,
) !void {
    heap[heap_len.*] = entry;
    heap_len.* += 1;
    try distributedMergeHeapSiftUp(req, plan, shards, heap[0..heap_len.*], heap_len.* - 1);
}

fn distributedMergeHeapPop(
    req: types.SearchRequest,
    plan: SortExecutionPlan,
    shards: []const DistributedSortedShard,
    heap: []DistributedMergeHeapEntry,
    heap_len: *usize,
) !DistributedMergeHeapEntry {
    const out = heap[0];
    heap_len.* -= 1;
    if (heap_len.* > 0) {
        heap[0] = heap[heap_len.*];
        try distributedMergeHeapSiftDown(req, plan, shards, heap[0..heap_len.*]);
    }
    return out;
}

fn appendDistributedMergedHit(
    alloc: Allocator,
    out: *std.ArrayListUnmanaged(types.SearchHit),
    source: types.SearchHit,
    selected_count: *usize,
    skip_count: usize,
    limit: usize,
) !bool {
    if (selected_count.* < skip_count) {
        selected_count.* += 1;
        return false;
    }
    if (out.items.len >= limit) return true;
    try out.append(alloc, try source.clone(alloc));
    selected_count.* += 1;
    return out.items.len >= limit;
}

fn retainDistributedPreviousHit(
    alloc: Allocator,
    ring: []types.SearchHit,
    ring_start: *usize,
    ring_len: *usize,
    source: types.SearchHit,
) !void {
    if (ring.len == 0) return;
    const cloned = try source.clone(alloc);
    errdefer {
        var owned = cloned;
        owned.deinit(alloc);
    }
    if (ring_len.* < ring.len) {
        ring[(ring_start.* + ring_len.*) % ring.len] = cloned;
        ring_len.* += 1;
        return;
    }
    ring[ring_start.*].deinit(alloc);
    ring[ring_start.*] = cloned;
    ring_start.* = (ring_start.* + 1) % ring.len;
}

fn mergeDistributedSortedHitsAlloc(
    alloc: Allocator,
    req: types.SearchRequest,
    plan: SortExecutionPlan,
    shards: []const DistributedSortedShard,
) ![]types.SearchHit {
    var effective = try effectiveSortRequestAlloc(alloc, req);
    defer effective.deinit(alloc);
    const effective_req = effective.req;

    try validateSortPageOptions(effective_req);
    try validateSortCursorContract(effective_req);
    if (effective_req.order_by.len == 0) return error.InvalidQueryRequest;
    if (effective_req.search_before.len > 0 and effective_req.offset != 0) return error.InvalidQueryRequest;

    var heap: []DistributedMergeHeapEntry = if (shards.len > 0)
        try alloc.alloc(DistributedMergeHeapEntry, shards.len)
    else
        &.{};
    defer if (heap.len > 0) alloc.free(heap);
    var heap_len: usize = 0;
    for (shards, 0..) |shard, shard_index| {
        if (shard.hits.len == 0) continue;
        try distributedMergeHeapPush(effective_req, plan, shards, heap, &heap_len, .{
            .shard_index = shard_index,
            .hit_index = 0,
        });
    }

    const limit: usize = @intCast(effective_req.limit);
    if (effective_req.search_before.len > 0) {
        var ring: []types.SearchHit = if (limit > 0) try alloc.alloc(types.SearchHit, limit) else &.{};
        var ring_start: usize = 0;
        var ring_len: usize = 0;
        errdefer {
            for (0..ring_len) |i| ring[(ring_start + i) % ring.len].deinit(alloc);
            if (ring.len > 0) alloc.free(ring);
        }
        while (heap_len > 0) {
            const entry = try distributedMergeHeapPop(effective_req, plan, shards, heap, &heap_len);
            const hit = shards[entry.shard_index].hits[entry.hit_index];
            if (try searchHitAllowedByCursor(effective_req, plan, hit)) {
                try retainDistributedPreviousHit(alloc, ring, &ring_start, &ring_len, hit);
            }
            const next_index = entry.hit_index + 1;
            if (next_index < shards[entry.shard_index].hits.len) {
                try distributedMergeHeapPush(effective_req, plan, shards, heap, &heap_len, .{
                    .shard_index = entry.shard_index,
                    .hit_index = next_index,
                });
            }
        }
        const out = try alloc.alloc(types.SearchHit, ring_len);
        var initialized: usize = 0;
        errdefer {
            for (out[0..initialized]) |*hit| hit.deinit(alloc);
            if (out.len > 0) alloc.free(out);
        }
        for (0..ring_len) |i| {
            const ring_index = (ring_start + i) % ring.len;
            out[i] = ring[ring_index];
            ring[ring_index] = undefined;
            initialized += 1;
        }
        if (ring.len > 0) alloc.free(ring);
        return out;
    }

    var out = std.ArrayListUnmanaged(types.SearchHit).empty;
    errdefer {
        for (out.items) |*hit| hit.deinit(alloc);
        out.deinit(alloc);
    }
    const skip_count: usize = if (effective_req.search_after.len > 0) 0 else @intCast(effective_req.offset);
    var selected_count: usize = 0;
    while (heap_len > 0 and out.items.len < limit) {
        const entry = try distributedMergeHeapPop(effective_req, plan, shards, heap, &heap_len);
        const hit = shards[entry.shard_index].hits[entry.hit_index];
        if (try searchHitAllowedByCursor(effective_req, plan, hit)) {
            if (try appendDistributedMergedHit(alloc, &out, hit, &selected_count, skip_count, limit)) break;
        }
        const next_index = entry.hit_index + 1;
        if (next_index < shards[entry.shard_index].hits.len) {
            try distributedMergeHeapPush(effective_req, plan, shards, heap, &heap_len, .{
                .shard_index = entry.shard_index,
                .hit_index = next_index,
            });
        }
    }
    return try out.toOwnedSlice(alloc);
}

fn jsonFieldValue(value: std.json.Value, field: []const u8) ?std.json.Value {
    var current = value;
    var parts = std.mem.splitScalar(u8, field, '.');
    while (parts.next()) |part| {
        if (part.len == 0) return null;
        if (current != .object) return null;
        current = current.object.get(part) orelse return null;
    }
    return current;
}

fn sortValueFromJson(value: ?std.json.Value) !SortValue {
    const actual = value orelse return .null_value;
    return switch (actual) {
        .null => .null_value,
        .bool => |v| .{ .bool_value = v },
        .integer => |v| .{ .integer = v },
        .float => |v| .{ .number = v },
        .number_string => |v| .{ .number_string = v },
        .string => |v| .{ .string = v },
        .array, .object => error.UnsupportedQueryRequest,
    };
}

fn ownedSortValueFromJson(alloc: Allocator, value: ?std.json.Value) !SortValue {
    const sort_value = try sortValueFromJson(value);
    return switch (sort_value) {
        .string => |text| .{ .string = try alloc.dupe(u8, text) },
        .number_string => |text| .{ .number_string = try alloc.dupe(u8, text) },
        else => sort_value,
    };
}

fn nativeSortValueMatchesMappedField(value: SortValue, mapping: runtime_schema_mod.FieldMapping) bool {
    if (value == .null_value) return true;
    return switch (mapping.field_type) {
        .keyword, .link => value == .string,
        .numeric => value == .integer or value == .number,
        .boolean => value == .bool_value,
        .datetime => value == .integer or value == .number,
        else => false,
    };
}

fn nativeSortValueMatchesPlanField(plan: SortExecutionPlan, field: []const u8, value: SortValue) bool {
    const schema = plan.runtime_schema orelse return true;
    const mapping = sortFieldMapping(schema, field) orelse return false;
    return nativeSortValueMatchesMappedField(value, mapping);
}

fn sortValueAsU64(value: SortValue) ?u64 {
    return switch (value) {
        .integer => |v| if (v >= 0) @intCast(v) else null,
        .number => |v| blk: {
            if (std.math.isNan(v) or v < 0) break :blk null;
            if (v >= 18446744073709551616.0) break :blk null;
            const int_value: u64 = @intFromFloat(v);
            if (@as(f64, @floatFromInt(int_value)) != v) break :blk null;
            break :blk int_value;
        },
        .number_string => |v| std.fmt.parseInt(u64, v, 10) catch null,
        else => null,
    };
}

fn sortValueJsonForFieldAlloc(alloc: Allocator, plan: SortExecutionPlan, field: []const u8, value: SortValue) !std.json.Value {
    if (plan.runtime_schema) |schema| {
        if (sortFieldMapping(schema, field)) |mapping| {
            if (mapping.field_type == .datetime) {
                if (sortValueAsU64(value)) |ns| {
                    return .{ .string = try runtime_schema_mod.formatDateTimeNsAlloc(alloc, ns) };
                }
                return error.UnsupportedQueryRequest;
            }
        }
    }
    return switch (value) {
        .null_value => .null,
        .bool_value => |v| .{ .bool = v },
        .integer => |v| .{ .integer = v },
        .number => |v| .{ .float = v },
        .number_string => |v| .{ .number_string = try alloc.dupe(u8, v) },
        .string => |v| .{ .string = try alloc.dupe(u8, v) },
    };
}

fn appendSortValueJson(alloc: Allocator, values: *std.ArrayListUnmanaged(std.json.Value), plan: SortExecutionPlan, field: []const u8, value: SortValue) !void {
    const json_value = try sortValueJsonForFieldAlloc(alloc, plan, field, value);
    errdefer {
        var owned = json_value;
        types.deinitJsonValue(alloc, &owned);
    }
    try values.append(alloc, json_value);
}

fn sortValueFromCursorJson(plan: SortExecutionPlan, field: []const u8, value: std.json.Value) !SortValue {
    if (plan.runtime_schema) |schema| {
        if (sortFieldMapping(schema, field)) |mapping| {
            if (!mappedSortCursorValueIsValid(mapping, value)) return error.InvalidQueryRequest;
            if (mapping.field_type == .datetime and value == .string) {
                if (runtime_schema_mod.parseDateTimeToNs(value.string)) |ns| {
                    if (ns <= @as(u64, @intCast(std.math.maxInt(i64)))) {
                        return .{ .integer = @intCast(ns) };
                    }
                    return .{ .number = @floatFromInt(ns) };
                }
            }
        }
    }
    return sortValueFromJson(value) catch error.InvalidQueryRequest;
}

fn compareDecoratedHitToCursor(req: types.SearchRequest, plan: SortExecutionPlan, hit: DecoratedSortHit, cursor: []const std.json.Value) !std.math.Order {
    for (req.order_by, 0..) |field, i| {
        const cursor_value = try sortValueFromCursorJson(plan, field.field, cursor[i]);
        const order = compareSortValues(hit.keys[i], cursor_value);
        if (order != .eq) {
            if (field.desc) {
                return switch (order) {
                    .lt => .gt,
                    .eq => .eq,
                    .gt => .lt,
                };
            }
            return order;
        }
    }
    return .eq;
}

fn sortCursorValueIsScalar(value: std.json.Value) bool {
    return switch (value) {
        .null, .bool, .integer, .float, .number_string, .string => true,
        .array, .object => false,
    };
}

fn validateSortCursorContract(req: types.SearchRequest) !void {
    try validateSortIdTiebreaker(req.order_by);
    const field_count = effectiveSortFieldCount(req.order_by);
    if (req.search_after.len > 0 and req.search_before.len > 0) return error.InvalidQueryRequest;
    if (req.search_after.len > 0 and req.search_after.len != field_count) return error.InvalidQueryRequest;
    if (req.search_before.len > 0 and req.search_before.len != field_count) return error.InvalidQueryRequest;
    const cursor = activeSortCursor(req);
    for (0..field_count) |i| {
        const field = effectiveSortFieldAt(req.order_by, i);
        if (cursor.len > 0 and !sortCursorValueIsScalar(cursor[i])) {
            return error.InvalidQueryRequest;
        }
        if (sortFieldIsScore(field) and cursor.len > 0 and !jsonValueIsNumeric(cursor[i])) {
            return error.InvalidQueryRequest;
        }
        if (std.mem.eql(u8, field.field, "_id") and cursor.len > 0 and cursor[i] != .string) {
            return error.InvalidQueryRequest;
        }
    }
}

fn activeSortCursor(req: types.SearchRequest) []const std.json.Value {
    if (req.search_after.len > 0) return req.search_after;
    if (req.search_before.len > 0) return req.search_before;
    return &.{};
}

fn sortWindowCapacity(req: types.SearchRequest) usize {
    if (req.search_after.len > 0 or req.search_before.len > 0) return @intCast(req.limit);
    const wanted = @as(u64, req.offset) +| @as(u64, req.limit);
    return @intCast(@min(wanted, @as(u64, std.math.maxInt(usize))));
}

fn decoratedHitAllowedByCursor(req: types.SearchRequest, plan: SortExecutionPlan, hit: DecoratedSortHit) !bool {
    if (req.search_after.len > 0) return (try compareDecoratedHitToCursor(req, plan, hit, req.search_after)) == .gt;
    if (req.search_before.len > 0) return (try compareDecoratedHitToCursor(req, plan, hit, req.search_before)) == .lt;
    return true;
}

fn decoratedWindowWorstIndex(req: types.SearchRequest, window: []const DecoratedSortHit, keep_previous_page: bool) usize {
    var worst: usize = 0;
    for (window[1..], 1..) |item, i| {
        const order = compareDecoratedSortHits(req, item, window[worst]);
        if (keep_previous_page) {
            if (order == .lt) worst = i;
        } else {
            if (order == .gt) worst = i;
        }
    }
    return worst;
}

fn decoratedHitBetterThanWorst(req: types.SearchRequest, candidate: DecoratedSortHit, worst: DecoratedSortHit, keep_previous_page: bool) bool {
    const order = compareDecoratedSortHits(req, candidate, worst);
    if (keep_previous_page) return order == .gt;
    return order == .lt;
}

fn admitDecoratedSortHitIntoWindow(
    alloc: Allocator,
    req: types.SearchRequest,
    window: []DecoratedSortHit,
    window_len: *usize,
    keep_previous_page: bool,
    profile: ?*SortCollectorProfile,
    decorated: DecoratedSortHit,
    allowed_by_cursor: bool,
) void {
    if (!allowed_by_cursor or window.len == 0) {
        if (profile) |p| {
            if (!allowed_by_cursor) {
                p.cursor_rejected_count += 1;
            } else {
                p.discarded_count += 1;
            }
        }
        var owned = decorated;
        owned.deinit(alloc);
        return;
    }
    if (window_len.* < window.len) {
        window[window_len.*] = decorated;
        window_len.* += 1;
        if (profile) |p| p.admitted_count += 1;
        return;
    }

    const worst_index = decoratedWindowWorstIndex(req, window[0..window_len.*], keep_previous_page);
    if (decoratedHitBetterThanWorst(req, decorated, window[worst_index], keep_previous_page)) {
        window[worst_index].deinit(alloc);
        window[worst_index] = decorated;
        if (profile) |p| {
            p.replaced_count += 1;
            p.admitted_count += 1;
        }
    } else {
        if (profile) |p| p.discarded_count += 1;
        var owned = decorated;
        owned.deinit(alloc);
    }
}

fn searchHitSortStoredJsonAlloc(
    alloc: Allocator,
    hit: *types.SearchHit,
    load_ctx: ?*anyopaque,
    load_stored: *const fn (?*anyopaque, Allocator, []const u8) anyerror!?[]u8,
) !std.json.Parsed(std.json.Value) {
    if (hit.stored_data == null) {
        hit.stored_data = (try load_stored(load_ctx, alloc, hit.id)) orelse return error.StoredDocMissing;
    }
    return try std.json.parseFromSlice(std.json.Value, alloc, hit.stored_data.?, .{});
}

const TextDocValueSortContext = struct {
    snapshot: *const index_mod.IndexSnapshot,
    ordinal_to_text_doc_id: ?*const std.AutoHashMapUnmanaged(doc_set.DocOrdinal, u32) = null,
};

const TextDocValueSortPlanContext = struct {
    plan: SortExecutionPlan,
    ctx: TextDocValueSortContext,
};

fn readBytesDocValueAlloc(
    alloc: Allocator,
    reader: *const typed_dv.TypedDocValuesReader,
    local_doc_id: u32,
) !?[]u8 {
    const found = try reader.findDoc(local_doc_id) orelse return null;
    defer alloc.free(found.chunk_data);

    const num_docs = std.mem.readInt(u32, found.chunk_data[0..4], .little);
    var cursor: usize = 4 + @as(usize, num_docs) * 4;
    for (0..found.pos) |_| {
        const value_len = std.mem.readInt(u32, found.chunk_data[cursor..][0..4], .little);
        cursor += 4 + value_len;
    }
    const value_len = std.mem.readInt(u32, found.chunk_data[cursor..][0..4], .little);
    cursor += 4;
    return try alloc.dupe(u8, found.chunk_data[cursor..][0..value_len]);
}

fn nativeSortValueFromTextDocValuesAlloc(
    alloc: Allocator,
    snapshot: *const index_mod.IndexSnapshot,
    native_text_doc_id: u32,
    field: []const u8,
) !?SortValue {
    const resolved = snapshot.resolveDocId(native_text_doc_id) orelse return null;
    const segment = &snapshot.segments[resolved.seg_idx];
    const section_data = segment.reader.getSection(field, .typed_doc_values) orelse return error.UnsupportedQueryRequest;
    var reader = try typed_dv.TypedDocValuesReader.init(alloc, section_data);
    return switch (reader.value_type) {
        .u64_val => {
            const value = try reader.getU64(resolved.local_id) orelse return error.UnsupportedQueryRequest;
            if (value <= @as(u64, @intCast(std.math.maxInt(i64)))) {
                return .{ .integer = @intCast(value) };
            }
            return .{ .number = @floatFromInt(value) };
        },
        .f64_val => {
            const value = try reader.getF64(resolved.local_id) orelse return error.UnsupportedQueryRequest;
            return .{ .number = value };
        },
        .bool_val => {
            const value = try reader.getBool(resolved.local_id) orelse return error.UnsupportedQueryRequest;
            return .{ .bool_value = value };
        },
        .bytes_val => {
            const value = try readBytesDocValueAlloc(alloc, &reader, resolved.local_id) orelse return error.UnsupportedQueryRequest;
            return .{ .string = value };
        },
        .geo_point => error.UnsupportedQueryRequest,
    };
}

fn loadTextDocValueSortValue(
    ctx: ?*anyopaque,
    alloc: Allocator,
    hit: types.SearchHit,
    field: []const u8,
) anyerror!?SortValue {
    const sort_ctx: *const TextDocValueSortContext = @ptrCast(@alignCast(ctx orelse return error.InvalidArgument));
    const native_text_doc_id = hit.native_text_doc_id orelse blk: {
        const ordinal = hit.doc_ordinal orelse return null;
        const ordinal_map = sort_ctx.ordinal_to_text_doc_id orelse return null;
        break :blk ordinal_map.get(ordinal) orelse return null;
    };
    return try nativeSortValueFromTextDocValuesAlloc(alloc, sort_ctx.snapshot, native_text_doc_id, field);
}

fn decorateSortHitAlloc(
    alloc: Allocator,
    req: types.SearchRequest,
    plan: SortExecutionPlan,
    hit: types.SearchHit,
    load_ctx: ?*anyopaque,
    load_stored: *const fn (?*anyopaque, Allocator, []const u8) anyerror!?[]u8,
    native_loader: ?NativeSortValueLoader,
    profile: ?*SortCollectorProfile,
) !DecoratedSortHit {
    var owned_hit = hit;
    errdefer owned_hit.deinit(alloc);

    const keys = try alloc.alloc(SortValue, req.order_by.len);
    var keys_initialized: usize = 0;
    var values = std.ArrayListUnmanaged(std.json.Value).empty;
    errdefer {
        for (values.items) |*value| types.deinitJsonValue(alloc, value);
        values.deinit(alloc);
        deinitSortValues(keys[0..keys_initialized], alloc);
        alloc.free(keys);
    }

    var parsed_doc: ?std.json.Parsed(std.json.Value) = null;
    defer if (parsed_doc) |*parsed| parsed.deinit();
    for (req.order_by, 0..) |field, i| {
        const sort_value: SortValue = if (std.mem.eql(u8, field.field, "_id"))
            .{ .string = try alloc.dupe(u8, owned_hit.id) }
        else if (std.mem.eql(u8, field.field, "_score"))
            .{ .number = sortScoreNumber(owned_hit) }
        else blk: {
            if (native_loader) |loader| {
                const native_start_ns = if (profile != null) platform_time.monotonicNs() else 0;
                const loaded_native = try loader.load(loader.ctx, alloc, owned_hit, field.field);
                if (profile) |p| {
                    p.native_doc_value_load_ns += platform_time.monotonicNs() - native_start_ns;
                    if (loaded_native != null) {
                        p.native_doc_value_hit_count += 1;
                    } else {
                        p.native_doc_value_miss_count += 1;
                    }
                }
                if (loaded_native) |native_value| {
                    if (!nativeSortValueMatchesPlanField(plan, field.field, native_value)) {
                        var owned_native = native_value;
                        owned_native.deinit(alloc);
                        return error.UnsupportedQueryRequest;
                    }
                    break :blk native_value;
                }
                if (plan.kind == .native_doc_values_top_n or plan.require_native or loader.require_native) {
                    return error.UnsupportedQueryRequest;
                }
            }
            if (parsed_doc == null) {
                const stored_start_ns = if (profile != null) platform_time.monotonicNs() else 0;
                parsed_doc = try searchHitSortStoredJsonAlloc(alloc, &owned_hit, load_ctx, load_stored);
                if (profile) |p| {
                    p.stored_json_load_ns += platform_time.monotonicNs() - stored_start_ns;
                    p.stored_json_load_count += 1;
                }
            }
            break :blk try ownedSortValueFromJson(alloc, jsonFieldValue(parsed_doc.?.value, field.field));
        };
        keys[i] = sort_value;
        keys_initialized += 1;
        try appendSortValueJson(alloc, &values, plan, field.field, sort_value);
    }
    owned_hit.sort_values = try values.toOwnedSlice(alloc);
    return .{ .hit = owned_hit, .keys = keys };
}

fn sortAndPageSearchResultInPlace(
    result: *types.SearchResult,
    req: types.SearchRequest,
    load_ctx: ?*anyopaque,
    load_stored: *const fn (?*anyopaque, Allocator, []const u8) anyerror!?[]u8,
    plan: SortExecutionPlan,
    native_loader: ?NativeSortValueLoader,
) !void {
    var effective = try effectiveSortRequestAlloc(result.alloc, req);
    defer effective.deinit(result.alloc);
    const effective_req = effective.req;

    try validateSortPageOptions(effective_req);
    if (effective_req.order_by.len == 0) return;
    if (plan.kind == .none) return;
    try validateSortExecutionPlanForRuntime(effective_req, plan, native_loader);
    try checkSearchRequestDeadline(effective_req);

    const bench_query_profile = shouldLogBenchQueryProfile();
    const sort_start_ns = if (bench_query_profile) platform_time.monotonicNs() else 0;
    var profile = SortCollectorProfile{};
    const alloc = result.alloc;
    const input_hits = result.hits;
    result.hits = &.{};
    const window_capacity = sortWindowCapacity(effective_req);
    if (bench_query_profile) profile.window_capacity = window_capacity;
    const keep_previous_page = effective_req.search_before.len > 0;
    var window: []DecoratedSortHit = if (window_capacity > 0)
        try alloc.alloc(DecoratedSortHit, window_capacity)
    else
        &.{};
    var window_len: usize = 0;
    var processed_hits: usize = 0;
    var input_owned = true;
    var window_drained_prefix: usize = 0;
    errdefer {
        for (window[window_drained_prefix..window_len]) |*item| item.deinit(alloc);
        if (window.len > 0) alloc.free(window);
        if (input_owned) {
            for (input_hits[processed_hits..]) |*hit| hit.deinit(alloc);
            if (input_hits.len > 0) alloc.free(input_hits);
        }
    }
    for (input_hits, 0..) |*hit, i| {
        if (i % 1024 == 0) try checkSearchRequestDeadline(effective_req);
        if (bench_query_profile) profile.candidate_count += 1;
        const raw_hit = hit.*;
        hit.* = undefined;
        processed_hits = i + 1;
        const decorate_start_ns = if (bench_query_profile) platform_time.monotonicNs() else 0;
        const decorated = try decorateSortHitAlloc(
            alloc,
            effective_req,
            plan,
            raw_hit,
            load_ctx,
            load_stored,
            native_loader,
            if (bench_query_profile) &profile else null,
        );
        if (bench_query_profile) profile.decorate_ns += platform_time.monotonicNs() - decorate_start_ns;

        const allowed_by_cursor = try decoratedHitAllowedByCursor(effective_req, plan, decorated);
        admitDecoratedSortHitIntoWindow(
            alloc,
            effective_req,
            window,
            &window_len,
            keep_previous_page,
            if (bench_query_profile) &profile else null,
            decorated,
            allowed_by_cursor,
        );
    }
    if (input_hits.len > 0) alloc.free(input_hits);
    input_owned = false;

    try checkSearchRequestDeadline(effective_req);
    const final_sort_start_ns = if (bench_query_profile) platform_time.monotonicNs() else 0;
    std.sort.pdq(DecoratedSortHit, window[0..window_len], effective_req, decoratedLessThan);
    if (bench_query_profile) profile.final_sort_ns = platform_time.monotonicNs() - final_sort_start_ns;
    try checkSearchRequestDeadline(effective_req);

    var start: usize = 0;
    var end: usize = window_len;
    if (effective_req.search_after.len == 0 and effective_req.search_before.len == 0) {
        start = @min(@as(usize, @intCast(effective_req.offset)), window_len);
        end = @min(start + @as(usize, @intCast(effective_req.limit)), window_len);
    }

    const selected = try alloc.alloc(types.SearchHit, end - start);
    var selected_initialized: usize = 0;
    errdefer {
        for (selected[0..selected_initialized]) |*hit| hit.deinit(alloc);
        if (selected.len > 0) alloc.free(selected);
    }
    for (window[0..window_len], 0..) |*item, i| {
        if (i % 1024 == 0) try checkSearchRequestDeadline(effective_req);
        if (i >= start and i < end) {
            selected[selected_initialized] = item.hit;
            item.hit = undefined;
            selected_initialized += 1;
            if (bench_query_profile) profile.selected_count += 1;
        } else {
            item.hit.deinit(alloc);
        }
        freeSortValues(alloc, item.keys);
        window_drained_prefix = i + 1;
    }
    if (window.len > 0) alloc.free(window);
    result.hits = selected;
    if (bench_query_profile) {
        profile.window_len = window_len;
        profile.total_ns = platform_time.monotonicNs() - sort_start_ns;
        logBenchSortCollectorProfile(effective_req, plan, native_loader != null, profile);
    }
}

fn sortAndPageMatchAllCandidatesAlloc(
    alloc: Allocator,
    req: types.SearchRequest,
    candidates: *MatchAllCandidates,
    load_ctx: ?*anyopaque,
    load_stored: *const fn (?*anyopaque, Allocator, []const u8) anyerror!?[]u8,
    plan: SortExecutionPlan,
    native_loader: ?NativeSortValueLoader,
) !types.SearchResult {
    var effective = try effectiveSortRequestAlloc(alloc, req);
    defer effective.deinit(alloc);
    const effective_req = effective.req;

    try validateSortPageOptions(effective_req);
    if (effective_req.order_by.len == 0) return error.InvalidQueryRequest;
    try validateSortExecutionPlanForRuntime(effective_req, plan, native_loader);
    try checkSearchRequestDeadline(effective_req);

    const bench_query_profile = shouldLogBenchQueryProfile();
    const sort_start_ns = if (bench_query_profile) platform_time.monotonicNs() else 0;
    var profile = SortCollectorProfile{};
    const window_capacity = sortWindowCapacity(effective_req);
    if (bench_query_profile) profile.window_capacity = window_capacity;
    const keep_previous_page = effective_req.search_before.len > 0;
    var window: []DecoratedSortHit = if (window_capacity > 0)
        try alloc.alloc(DecoratedSortHit, window_capacity)
    else
        &.{};
    var window_len: usize = 0;
    var window_drained_prefix: usize = 0;
    errdefer {
        for (window[window_drained_prefix..window_len]) |*item| item.deinit(alloc);
        if (window.len > 0) alloc.free(window);
    }

    for (candidates.items, 0..) |*candidate, i| {
        if (i % 1024 == 0) try checkSearchRequestDeadline(effective_req);
        if (bench_query_profile) profile.candidate_count += 1;
        const raw_hit = types.SearchHit{
            .id = candidate.id,
            .doc_ordinal = candidate.ordinal,
            .score = 1.0,
            .stored_data = null,
        };
        candidate.id = @constCast(&[_]u8{});

        const decorate_start_ns = if (bench_query_profile) platform_time.monotonicNs() else 0;
        const decorated = try decorateSortHitAlloc(
            alloc,
            effective_req,
            plan,
            raw_hit,
            load_ctx,
            load_stored,
            native_loader,
            if (bench_query_profile) &profile else null,
        );
        if (bench_query_profile) profile.decorate_ns += platform_time.monotonicNs() - decorate_start_ns;

        const allowed_by_cursor = try decoratedHitAllowedByCursor(effective_req, plan, decorated);
        admitDecoratedSortHitIntoWindow(
            alloc,
            effective_req,
            window,
            &window_len,
            keep_previous_page,
            if (bench_query_profile) &profile else null,
            decorated,
            allowed_by_cursor,
        );
    }

    try checkSearchRequestDeadline(effective_req);
    const final_sort_start_ns = if (bench_query_profile) platform_time.monotonicNs() else 0;
    std.sort.pdq(DecoratedSortHit, window[0..window_len], effective_req, decoratedLessThan);
    if (bench_query_profile) profile.final_sort_ns = platform_time.monotonicNs() - final_sort_start_ns;
    try checkSearchRequestDeadline(effective_req);

    var start: usize = 0;
    var end: usize = window_len;
    if (effective_req.search_after.len == 0 and effective_req.search_before.len == 0) {
        start = @min(@as(usize, @intCast(effective_req.offset)), window_len);
        end = @min(start + @as(usize, @intCast(effective_req.limit)), window_len);
    }

    const selected = try alloc.alloc(types.SearchHit, end - start);
    var selected_initialized: usize = 0;
    errdefer {
        for (selected[0..selected_initialized]) |*hit| hit.deinit(alloc);
        if (selected.len > 0) alloc.free(selected);
    }
    for (window[0..window_len], 0..) |*item, i| {
        if (i % 1024 == 0) try checkSearchRequestDeadline(effective_req);
        if (i >= start and i < end) {
            selected[selected_initialized] = item.hit;
            item.hit = undefined;
            selected_initialized += 1;
            if (bench_query_profile) profile.selected_count += 1;
        } else {
            item.hit.deinit(alloc);
        }
        freeSortValues(alloc, item.keys);
        window_drained_prefix = i + 1;
    }
    if (window.len > 0) alloc.free(window);
    if (bench_query_profile) {
        profile.window_len = window_len;
        profile.total_ns = platform_time.monotonicNs() - sort_start_ns;
        logBenchSortCollectorProfile(effective_req, plan, native_loader != null, profile);
    }

    return .{
        .alloc = alloc,
        .hits = selected,
        .total_hits = @intCast(@min(candidates.items.len, @as(usize, std.math.maxInt(u32)))),
        .graph_results = &.{},
    };
}

const MatchAllSortStreamContext = struct {
    alloc: Allocator,
    req: types.SearchRequest,
    plan: SortExecutionPlan,
    load_ctx: ?*anyopaque,
    load_stored: *const fn (?*anyopaque, Allocator, []const u8) anyerror!?[]u8,
    native_loader: ?NativeSortValueLoader,
    window: []DecoratedSortHit,
    window_len: usize = 0,
    keep_previous_page: bool,
    profile: ?*SortCollectorProfile,
    total_hits: u32 = 0,
};

fn consumeMatchAllSortCandidate(ctx: ?*anyopaque, candidate: MatchAllCandidate) !void {
    const stream_ctx: *MatchAllSortStreamContext = @ptrCast(@alignCast(ctx orelse return error.InvalidArgument));
    const alloc = stream_ctx.alloc;
    if (stream_ctx.profile) |profile| profile.candidate_count += 1;
    stream_ctx.total_hits +|= 1;

    var owned_candidate = candidate;
    const raw_hit = types.SearchHit{
        .id = owned_candidate.id,
        .doc_ordinal = owned_candidate.ordinal,
        .score = 1.0,
        .stored_data = null,
    };
    owned_candidate.id = @constCast(&[_]u8{});

    const decorate_start_ns = if (stream_ctx.profile != null) platform_time.monotonicNs() else 0;
    const decorated = try decorateSortHitAlloc(
        alloc,
        stream_ctx.req,
        stream_ctx.plan,
        raw_hit,
        stream_ctx.load_ctx,
        stream_ctx.load_stored,
        stream_ctx.native_loader,
        stream_ctx.profile,
    );
    if (stream_ctx.profile) |profile| profile.decorate_ns += platform_time.monotonicNs() - decorate_start_ns;

    const allowed_by_cursor = try decoratedHitAllowedByCursor(stream_ctx.req, stream_ctx.plan, decorated);
    admitDecoratedSortHitIntoWindow(
        alloc,
        stream_ctx.req,
        stream_ctx.window,
        &stream_ctx.window_len,
        stream_ctx.keep_previous_page,
        stream_ctx.profile,
        decorated,
        allowed_by_cursor,
    );
}

fn sortAndPageMatchAllCandidateStreamAlloc(
    alloc: Allocator,
    req: types.SearchRequest,
    executor: MatchAllExecutor,
    options: MatchAllCandidateCollectOptions,
    plan: SortExecutionPlan,
    native_loader: ?NativeSortValueLoader,
) !types.SearchResult {
    var effective = try effectiveSortRequestAlloc(alloc, req);
    defer effective.deinit(alloc);
    const effective_req = effective.req;

    try validateSortPageOptions(effective_req);
    if (effective_req.order_by.len == 0) return error.InvalidQueryRequest;
    try validateSortExecutionPlanForRuntime(effective_req, plan, native_loader);
    try checkSearchRequestDeadline(effective_req);

    const collect_stream = executor.collect_candidates_stream orelse return error.UnsupportedQueryRequest;
    const bench_query_profile = shouldLogBenchQueryProfile();
    const sort_start_ns = if (bench_query_profile) platform_time.monotonicNs() else 0;
    var profile = SortCollectorProfile{};
    const window_capacity = sortWindowCapacity(effective_req);
    if (bench_query_profile) profile.window_capacity = window_capacity;
    const keep_previous_page = effective_req.search_before.len > 0;
    var window: []DecoratedSortHit = if (window_capacity > 0)
        try alloc.alloc(DecoratedSortHit, window_capacity)
    else
        &.{};
    var stream_ctx = MatchAllSortStreamContext{
        .alloc = alloc,
        .req = effective_req,
        .plan = plan,
        .load_ctx = executor.ctx,
        .load_stored = executor.load_stored,
        .native_loader = native_loader,
        .window = window,
        .keep_previous_page = keep_previous_page,
        .profile = if (bench_query_profile) &profile else null,
    };
    var window_drained_prefix: usize = 0;
    errdefer {
        for (window[window_drained_prefix..stream_ctx.window_len]) |*item| item.deinit(alloc);
        if (window.len > 0) alloc.free(window);
    }

    const stream_stats = try collect_stream(executor.ctx, alloc, req, options, &stream_ctx, consumeMatchAllSortCandidate);

    try checkSearchRequestDeadline(effective_req);
    const final_sort_start_ns = if (bench_query_profile) platform_time.monotonicNs() else 0;
    std.sort.pdq(DecoratedSortHit, stream_ctx.window[0..stream_ctx.window_len], effective_req, decoratedLessThan);
    if (bench_query_profile) profile.final_sort_ns = platform_time.monotonicNs() - final_sort_start_ns;
    try checkSearchRequestDeadline(effective_req);

    var start: usize = 0;
    var end: usize = stream_ctx.window_len;
    if (effective_req.search_after.len == 0 and effective_req.search_before.len == 0) {
        start = @min(@as(usize, @intCast(effective_req.offset)), stream_ctx.window_len);
        end = @min(start + @as(usize, @intCast(effective_req.limit)), stream_ctx.window_len);
    }

    const selected = try alloc.alloc(types.SearchHit, end - start);
    var selected_initialized: usize = 0;
    errdefer {
        for (selected[0..selected_initialized]) |*hit| hit.deinit(alloc);
        if (selected.len > 0) alloc.free(selected);
    }
    for (stream_ctx.window[0..stream_ctx.window_len], 0..) |*item, i| {
        if (i % 1024 == 0) try checkSearchRequestDeadline(effective_req);
        if (i >= start and i < end) {
            selected[selected_initialized] = item.hit;
            item.hit = undefined;
            selected_initialized += 1;
            if (bench_query_profile) profile.selected_count += 1;
        } else {
            item.hit.deinit(alloc);
        }
        freeSortValues(alloc, item.keys);
        window_drained_prefix = i + 1;
    }
    if (window.len > 0) alloc.free(window);
    if (bench_query_profile) {
        profile.window_len = stream_ctx.window_len;
        profile.total_ns = platform_time.monotonicNs() - sort_start_ns;
        logBenchSortCollectorProfile(effective_req, plan, native_loader != null, profile);
    }

    return .{
        .alloc = alloc,
        .hits = selected,
        .total_hits = stream_ctx.total_hits,
        .total_hits_relation = if (stream_stats.stopped_early or options.primary_key_stop_before != null) .gte else .exact,
        .graph_results = &.{},
    };
}

const MatchAllIdSeekContext = struct {
    alloc: Allocator,
    req: types.SearchRequest,
    skip_count: usize,
    limit: usize,
    hits: std.ArrayListUnmanaged(types.SearchHit) = .empty,
    accepted_count: usize = 0,

    fn deinitHits(self: *@This()) void {
        for (self.hits.items) |*hit| hit.deinit(self.alloc);
        self.hits.deinit(self.alloc);
    }
};

fn consumeMatchAllIdSeekCandidate(ctx: ?*anyopaque, candidate: MatchAllCandidate) !void {
    const seek_ctx: *MatchAllIdSeekContext = @ptrCast(@alignCast(ctx orelse return error.InvalidArgument));
    const alloc = seek_ctx.alloc;

    var owned_candidate = candidate;
    errdefer owned_candidate.deinit(alloc);

    seek_ctx.accepted_count += 1;
    if (seek_ctx.accepted_count <= seek_ctx.skip_count) {
        owned_candidate.deinit(alloc);
        return;
    }
    if (seek_ctx.hits.items.len >= seek_ctx.limit) {
        owned_candidate.deinit(alloc);
        return;
    }

    const sort_values = try alloc.alloc(std.json.Value, 1);
    errdefer alloc.free(sort_values);
    sort_values[0] = .{ .string = try alloc.dupe(u8, owned_candidate.id) };
    errdefer types.deinitJsonValue(alloc, &sort_values[0]);

    try seek_ctx.hits.append(alloc, .{
        .id = owned_candidate.id,
        .doc_ordinal = owned_candidate.ordinal,
        .score = 1.0,
        .sort_values = sort_values,
        .stored_data = null,
    });
    owned_candidate.id = @constCast(&[_]u8{});
}

fn idOnlySearchAfterCursor(req: types.SearchRequest) ?[]const u8 {
    if (req.search_after.len == 0) return null;
    return req.search_after[0].string;
}

fn idOnlySearchBeforeCursor(req: types.SearchRequest) ?[]const u8 {
    if (req.search_before.len == 0) return null;
    return req.search_before[0].string;
}

fn sortAndPageMatchAllIdSeekAlloc(
    alloc: Allocator,
    req: types.SearchRequest,
    executor: MatchAllExecutor,
    constraints: *const NativeDocIdConstraints,
) !types.SearchResult {
    var effective = try effectiveSortRequestAlloc(alloc, req);
    defer effective.deinit(alloc);
    const effective_req = effective.req;

    try validateSortPageOptions(effective_req);
    if (effective_req.order_by.len != 1 or !sortFieldIsId(effective_req.order_by[0])) {
        return error.InvalidQueryRequest;
    }
    if (effective_req.search_before.len > 0) return error.UnsupportedQueryRequest;

    const collect_stream = executor.collect_candidates_stream orelse return error.UnsupportedQueryRequest;
    const skip_count: usize = if (effective_req.search_after.len == 0)
        @intCast(effective_req.offset)
    else
        0;
    const requested_limit: usize = @intCast(effective_req.limit);
    if (requested_limit == 0) {
        return .{
            .alloc = alloc,
            .hits = &.{},
            .total_hits = 0,
            .total_hits_relation = .gte,
            .graph_results = &.{},
        };
    }

    const stop_after = skip_count +| requested_limit;
    var seek_ctx = MatchAllIdSeekContext{
        .alloc = alloc,
        .req = effective_req,
        .skip_count = skip_count,
        .limit = requested_limit,
    };
    errdefer seek_ctx.deinitHits();

    const stats = try collect_stream(executor.ctx, alloc, req, .{
        .constraints = constraints,
        .primary_key_start_after = idOnlySearchAfterCursor(effective_req),
        .stop_after_accepted = stop_after,
        .scan_batch_size = @max(@min(stop_after, default_match_all_primary_key_scan_batch_size), 1),
    }, &seek_ctx, consumeMatchAllIdSeekCandidate);

    const hits = try seek_ctx.hits.toOwnedSlice(alloc);
    return .{
        .alloc = alloc,
        .hits = hits,
        .total_hits = @intCast(@min(stats.accepted_count, @as(usize, std.math.maxInt(u32)))),
        .total_hits_relation = if (stats.stopped_early) .gte else .exact,
        .graph_results = &.{},
    };
}

fn sortCursorMode(req: types.SearchRequest) []const u8 {
    if (req.search_after.len > 0) return "after";
    if (req.search_before.len > 0) return "before";
    return "none";
}

fn logBenchSortCollectorProfile(
    req: types.SearchRequest,
    plan: SortExecutionPlan,
    native_loader_enabled: bool,
    profile: SortCollectorProfile,
) void {
    std.log.info(
        "antfly_bench_sort_collector total_us={d} decorate_us={d} native_doc_value_load_us={d} native_doc_value_hits={d} native_doc_value_misses={d} stored_json_load_us={d} stored_json_loads={d} final_sort_us={d} candidates={d} cursor_rejected={d} admitted={d} replaced={d} discarded={d} selected={d} window_capacity={d} window_len={d} order_fields={d} cursor={s} plan={s} exactness={s} source={s} cursor_support={s} source_load={s} distributed={s} require_native={} native_loader={} index_sort_match={} sorted_segment_executor_available={}",
        .{
            nsToUs(profile.total_ns),
            nsToUs(profile.decorate_ns),
            nsToUs(profile.native_doc_value_load_ns),
            profile.native_doc_value_hit_count,
            profile.native_doc_value_miss_count,
            nsToUs(profile.stored_json_load_ns),
            profile.stored_json_load_count,
            nsToUs(profile.final_sort_ns),
            profile.candidate_count,
            profile.cursor_rejected_count,
            profile.admitted_count,
            profile.replaced_count,
            profile.discarded_count,
            profile.selected_count,
            profile.window_capacity,
            profile.window_len,
            req.order_by.len,
            sortCursorMode(req),
            sortExecutionPlanKindName(plan.kind),
            sortPlanExactnessName(sortExecutionPlanExactness(plan)),
            sortPlanSourceName(sortExecutionPlanSource(plan)),
            sortPlanCursorSupportName(sortExecutionPlanCursorSupport(plan)),
            sortPlanSourceLoadName(sortExecutionPlanSourceLoadForRequest(plan, req)),
            sortPlanDistributedBehaviorName(sortExecutionPlanDistributedBehavior(plan)),
            plan.require_native,
            native_loader_enabled,
            plan.index_sort_match,
            plan.sorted_segment_executor_available,
        },
    );
    if (profile.native_doc_value_hit_count > 0 or profile.native_doc_value_miss_count > 0 or profile.stored_json_load_count > 0) {
        std.log.info(
            "antfly_bench_sort_values native_doc_value_hits={d} native_doc_value_misses={d} stored_json_loads={d}",
            .{
                profile.native_doc_value_hit_count,
                profile.native_doc_value_miss_count,
                profile.stored_json_load_count,
            },
        );
    }
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
    if (patternFilterValueHasRole(parsed.value)) return null;
    const search_query = patternFilterValueToSearchQuery(arena_alloc, parsed.value, text_entry.text_analysis, text_entry.runtime_schema) catch return null;

    return try collectSearchQueryResolvedDocSetAlloc(alloc, arena_alloc, executor, text_entry, search_query);
}

fn collectSearchQueryResolvedDocSetAlloc(
    alloc: Allocator,
    arena_alloc: Allocator,
    executor: StructuredFilterResolverExecutor,
    text_entry: *index_manager_mod.IndexManager.TextIndex,
    search_query: search_mod.SearchQuery,
) !?doc_set.ResolvedDocSet {
    const bench_profile = getenv("ANTFLY_BENCH_QUERY_PROFILE") != null;
    const total_start_ns = if (bench_profile) platform_time.monotonicNs() else 0;
    var snapshot_ns: u64 = 0;
    var capability_ns: u64 = 0;
    var filter_compile_ns: u64 = 0;
    var execute_ns: u64 = 0;
    var ordinal_ns: u64 = 0;
    const snapshot_start_ns = if (bench_profile) platform_time.monotonicNs() else 0;
    const snapshot = text_entry.persistent.snapshot();
    if (bench_profile) snapshot_ns = platform_time.monotonicNs() - snapshot_start_ns;
    const capability_start_ns = if (bench_profile) platform_time.monotonicNs() else 0;
    if (!(try searchQueryCanUseSnapshot(
        snapshot,
        search_query,
        text_entry.text_analysis,
        text_entry.runtime_schema,
    ))) return null;
    if (bench_profile) capability_ns = platform_time.monotonicNs() - capability_start_ns;

    const filter_compile_start_ns = if (bench_profile) platform_time.monotonicNs() else 0;
    const filter = search_mod.searchQueryToFilterArena(arena_alloc, search_query) catch return null;
    if (bench_profile) filter_compile_ns = platform_time.monotonicNs() - filter_compile_start_ns;
    const execute_start_ns = if (bench_profile) platform_time.monotonicNs() else 0;
    const doc_nums = try snapshot.executeFilter(alloc, filter);
    if (bench_profile) execute_ns = platform_time.monotonicNs() - execute_start_ns;
    defer alloc.free(doc_nums);
    if (doc_nums.len == 0) {
        const empty: doc_set.ResolvedDocSet = .none;
        if (bench_profile) {
            std.log.info(
                "antfly_bench_structured_filter total_us={d} snapshot_us={d} capability_us={d} filter_compile_us={d} execute_us={d} ordinal_us={d} doc_nums={d} result=none",
                .{ nsToUs(platform_time.monotonicNs() - total_start_ns), nsToUs(snapshot_ns), nsToUs(capability_ns), nsToUs(filter_compile_ns), nsToUs(execute_ns), nsToUs(ordinal_ns), doc_nums.len },
            );
        }
        return empty;
    }

    if (snapshot.hasDocOrdinalCoverage()) {
        const ordinal_start_ns = if (bench_profile) platform_time.monotonicNs() else 0;
        if (try resolvedDocSetForTextDocNumsFromOrdinalSidecarAlloc(alloc, snapshot, doc_nums, executor)) |resolved| {
            if (bench_profile) {
                ordinal_ns = platform_time.monotonicNs() - ordinal_start_ns;
                std.log.info(
                    "antfly_bench_structured_filter total_us={d} snapshot_us={d} capability_us={d} filter_compile_us={d} execute_us={d} ordinal_us={d} doc_nums={d} result=ordinals",
                    .{ nsToUs(platform_time.monotonicNs() - total_start_ns), nsToUs(snapshot_ns), nsToUs(capability_ns), nsToUs(filter_compile_ns), nsToUs(execute_ns), nsToUs(ordinal_ns), doc_nums.len },
                );
            }
            return resolved;
        }
        if (bench_profile) ordinal_ns = platform_time.monotonicNs() - ordinal_start_ns;
    }

    const resolve = executor.resolve_doc_ids_to_doc_set orelse return null;
    var doc_ids = std.ArrayListUnmanaged([]const u8).empty;
    defer freeDocIdArrayList(alloc, &doc_ids);
    for (doc_nums) |doc_num| {
        const stored = snapshot.storedDoc(doc_num) orelse continue;
        try doc_ids.append(alloc, try alloc.dupe(u8, stored.id));
    }
    const resolved = try resolve(executor.ctx, alloc, doc_ids.items, executor.identity_read_generation);
    if (bench_profile) {
        std.log.info(
            "antfly_bench_structured_filter total_us={d} snapshot_us={d} capability_us={d} filter_compile_us={d} execute_us={d} ordinal_us={d} doc_nums={d} result=doc_ids",
            .{ nsToUs(platform_time.monotonicNs() - total_start_ns), nsToUs(snapshot_ns), nsToUs(capability_ns), nsToUs(filter_compile_ns), nsToUs(execute_ns), nsToUs(ordinal_ns), doc_nums.len },
        );
    }
    return resolved;
}

fn collectStructuredFilterTextDocNumsAlloc(
    alloc: Allocator,
    req: types.SearchRequest,
    executor: StructuredFilterResolverExecutor,
    filter_query_json: []const u8,
) !?TextDocNumSet {
    const text_entry = try resolveFilterTextIndexEntry(executor, req.primary_text_index_name, req.index_name) orelse return null;

    var arena = std.heap.ArenaAllocator.init(alloc);
    defer arena.deinit();
    const arena_alloc = arena.allocator();
    const parsed = std.json.parseFromSlice(std.json.Value, arena_alloc, filter_query_json, .{}) catch return null;
    if (patternFilterValueHasRole(parsed.value)) return null;
    const search_query = patternFilterValueToSearchQuery(arena_alloc, parsed.value, text_entry.text_analysis, text_entry.runtime_schema) catch return null;

    const snapshot = text_entry.persistent.snapshot();
    if (!(try searchQueryCanUseSnapshot(
        snapshot,
        search_query,
        text_entry.text_analysis,
        text_entry.runtime_schema,
    ))) return null;

    const filter = search_mod.searchQueryToFilterArena(arena_alloc, search_query) catch return null;
    const doc_nums = try snapshot.executeFilter(alloc, filter);
    if (doc_nums.len == 0) {
        alloc.free(doc_nums);
        return .none;
    }
    return .{ .doc_nums = doc_nums };
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

    return try resolvedDocSetForTextDocNumsFromOrdinalSidecarAlloc(alloc, snapshot, doc_nums.items, executor);
}

fn resolvedDocSetForTextDocNumsFromOrdinalSidecarAlloc(
    alloc: Allocator,
    snapshot: *const index_mod.IndexSnapshot,
    doc_nums: []const u32,
    executor: StructuredFilterResolverExecutor,
) !?doc_set.ResolvedDocSet {
    const ordinals = (try snapshot.docOrdinalsForDocNumsAlloc(alloc, doc_nums)) orelse return null;
    defer alloc.free(ordinals);
    if (ordinals.len == 0) return null;
    var resolved = try doc_set.fromOrdinalsAlloc(alloc, ordinals);
    errdefer resolved.deinit(alloc);

    var live_owned: ?doc_set.ResolvedDocSet = null;
    defer if (live_owned) |*owned| owned.deinit(alloc);
    const live_set = try maybeLiveFilterResolvedDocSetAlloc(alloc, &resolved, executor, &live_owned);
    if (live_set != &resolved) {
        resolved.deinit(alloc);
        return try doc_set.cloneAlloc(alloc, live_set);
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
    if (!(try searchQueryCanUseSnapshot(
        snapshot,
        search_query,
        text_entry.text_analysis,
        text_entry.runtime_schema,
    ))) return null;
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

fn searchQueryCanUseSnapshot(
    snapshot: *const index_mod.IndexSnapshot,
    query: search_mod.SearchQuery,
    text_analysis: introducer_mod.TextAnalysisConfig,
    runtime_schema: ?runtime_schema_mod.TableSchema,
) !bool {
    return switch (query) {
        .match_none,
        .match_all,
        .doc_id,
        .doc_num,
        => true,
        .knn => false,
        .hybrid => false,
        .match => |item| try snapshot.hasInvertedField(item.field),
        .phrase => |item| try snapshot.hasInvertedField(item.field),
        .term_phrase => |item| try snapshot.hasInvertedField(item.field),
        .multi_phrase => |item| try snapshot.hasInvertedField(item.field),
        .term => |item| (try queryFieldUsesKeywordAnalyzer(item.field, text_analysis, runtime_schema)) and
            try snapshot.hasInvertedField(item.field),
        .fuzzy => |item| try snapshot.hasInvertedField(item.field),
        .numeric_range => |item| try snapshot.hasInvertedField(item.field),
        .date_range => |item| try snapshot.hasInvertedField(item.field),
        .bool_field => |item| try snapshot.hasInvertedField(item.field),
        .geo_distance => |item| try snapshot.hasInvertedField(item.field),
        .geo_bbox => |item| try snapshot.hasInvertedField(item.field),
        .term_range => |item| try snapshot.hasInvertedField(item.field),
        .ip_range => |item| try snapshot.hasInvertedField(item.field),
        .geo_shape => |item| try snapshot.hasInvertedField(item.field),
        .prefix => |item| try snapshot.hasInvertedField(item.field),
        .wildcard => |item| try snapshot.hasInvertedField(item.field),
        .regexp => |item| try snapshot.hasInvertedField(item.field),
        .bool_query => |item| {
            for (item.must) |child| {
                if (!(try searchQueryCanUseSnapshot(snapshot, child, text_analysis, runtime_schema))) return false;
            }
            for (item.should) |child| {
                if (!(try searchQueryCanUseSnapshot(snapshot, child, text_analysis, runtime_schema))) return false;
            }
            for (item.must_not) |child| {
                if (!(try searchQueryCanUseSnapshot(snapshot, child, text_analysis, runtime_schema))) return false;
            }
            return true;
        },
    };
}

fn queryFieldUsesKeywordAnalyzer(
    field: []const u8,
    text_analysis: introducer_mod.TextAnalysisConfig,
    runtime_schema: ?runtime_schema_mod.TableSchema,
) !bool {
    if (std.mem.endsWith(u8, field, mapper_mod.schema_less_exact_field_suffix)) return true;
    const analyzer = (try resolveQueryAnalyzer(field, null, text_analysis, runtime_schema)) orelse return false;
    return analyzer.tokenizer == .keyword and analyzer.filters.len == 0 and analyzer.char_filters.len == 0;
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
    var out = try alloc.alloc(u32, left.len + right.len);
    errdefer alloc.free(out);
    @memcpy(out[0..left.len], left);
    @memcpy(out[left.len..], right);
    std.mem.sort(u32, out, {}, u32LessThan);
    const unique_len = uniqueSortedU32(out);
    return try alloc.realloc(out, unique_len);
}

fn intersectDocNumsAlloc(alloc: Allocator, left: []const u32, right: []const u32) ![]const u32 {
    if (left.len == 0 or right.len == 0) return try alloc.alloc(u32, 0);
    var sorted_right = try alloc.dupe(u32, right);
    defer alloc.free(sorted_right);
    std.mem.sort(u32, sorted_right, {}, u32LessThan);
    const unique_right = sorted_right[0..uniqueSortedU32(sorted_right)];

    var sorted_left = try alloc.dupe(u32, left);
    defer alloc.free(sorted_left);
    std.mem.sort(u32, sorted_left, {}, u32LessThan);
    const unique_left = sorted_left[0..uniqueSortedU32(sorted_left)];

    var out = std.ArrayListUnmanaged(u32).empty;
    errdefer out.deinit(alloc);
    for (unique_left) |doc_num| {
        if (containsSortedU32(unique_right, doc_num)) try out.append(alloc, doc_num);
    }
    return try out.toOwnedSlice(alloc);
}

fn u32LessThan(_: void, left: u32, right: u32) bool {
    return left < right;
}

fn uniqueSortedU32(values: []u32) usize {
    if (values.len == 0) return 0;
    var out: usize = 1;
    for (values[1..]) |value| {
        if (value == values[out - 1]) continue;
        values[out] = value;
        out += 1;
    }
    return out;
}

fn containsSortedU32(values: []const u32, expected: u32) bool {
    return std.sort.binarySearch(u32, values, expected, compareU32) != null;
}

fn compareU32(expected: u32, item: u32) std.math.Order {
    return std.math.order(expected, item);
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
        return .{ .term = .{
            .field = try exactTermFilterFieldAlloc(alloc, field_value.field, text_analysis, runtime_schema),
            .term = field_value.value,
        } };
    }
    if (value.object.get("terms")) |terms| {
        const field_terms = try singleFieldTerms(alloc, terms);
        const exact_field = try exactTermFilterFieldAlloc(alloc, field_terms.field, text_analysis, runtime_schema);
        const should = try alloc.alloc(search_mod.SearchQuery, field_terms.terms.len);
        for (field_terms.terms, 0..) |term, i| {
            should[i] = .{ .term = .{ .field = exact_field, .term = term } };
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
    var must_out = std.ArrayListUnmanaged(search_mod.SearchQuery).empty;
    errdefer must_out.deinit(alloc);
    if (value.object.get("filter")) |filter| {
        try appendPatternFilterArrayToSearchQueries(alloc, &must_out, filter, text_analysis, runtime_schema);
    }
    if (value.object.get("must")) |must| {
        try appendPatternFilterArrayToSearchQueries(alloc, &must_out, must, text_analysis, runtime_schema);
    }
    const has_must = value.object.get("must") != null or value.object.get("filter") != null;
    const has_should = value.object.get("should") != null;
    const only_must_not = !has_must and !has_should and value.object.get("must_not") != null;
    return .{ .bool_query = .{
        .must = if (must_out.items.len > 0)
            try must_out.toOwnedSlice(alloc)
        else if (only_must_not)
            &.{.{ .match_all = {} }}
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
        .min_should = if (!has_must and has_should) 1 else 0,
    } };
}

fn appendPatternFilterArrayToSearchQueries(
    alloc: Allocator,
    out: *std.ArrayListUnmanaged(search_mod.SearchQuery),
    value: std.json.Value,
    text_analysis: introducer_mod.TextAnalysisConfig,
    runtime_schema: ?runtime_schema_mod.TableSchema,
) !void {
    if (value != .array or value.array.items.len == 0) return error.InvalidArgument;
    try out.ensureUnusedCapacity(alloc, value.array.items.len);
    for (value.array.items) |item| {
        out.appendAssumeCapacity(try patternFilterValueToSearchQuery(alloc, item, text_analysis, runtime_schema));
    }
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

fn patternFilterValueHasRole(value: std.json.Value) bool {
    return switch (value) {
        .object => |object| {
            if (object.get("role") != null) return true;
            var it = object.iterator();
            while (it.next()) |entry| {
                if (patternFilterValueHasRole(entry.value_ptr.*)) return true;
            }
            return false;
        },
        .array => |array| {
            for (array.items) |item| {
                if (patternFilterValueHasRole(item)) return true;
            }
            return false;
        },
        else => false,
    };
}

fn exactTermFilterFieldAlloc(
    alloc: Allocator,
    field: []const u8,
    text_analysis: introducer_mod.TextAnalysisConfig,
    runtime_schema: ?runtime_schema_mod.TableSchema,
) ![]const u8 {
    if (try queryFieldUsesKeywordAnalyzer(field, text_analysis, runtime_schema)) return field;
    if (std.mem.endsWith(u8, field, mapper_mod.schema_less_exact_field_suffix)) return field;
    return try mapper_mod.schemaLessExactFieldNameAlloc(alloc, field);
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

test "pattern bool filter clauses are merged into required search clauses" {
    var arena = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer arena.deinit();
    const alloc = arena.allocator();

    const parsed = try std.json.parseFromSlice(std.json.Value, alloc,
        \\{"bool":{"must":[{"doc_id":["doc:must"]}],"filter":[{"doc_id":["doc:filter"]}]}}
    , .{});
    const query = try patternFilterValueToSearchQuery(alloc, parsed.value, .{}, null);

    try std.testing.expect(query == .bool_query);
    try std.testing.expectEqual(@as(usize, 2), query.bool_query.must.len);
    try std.testing.expect(query.bool_query.must[0] == .doc_id);
    try std.testing.expectEqualStrings("doc:filter", query.bool_query.must[0].doc_id.ids[0]);
    try std.testing.expect(query.bool_query.must[1] == .doc_id);
    try std.testing.expectEqualStrings("doc:must", query.bool_query.must[1].doc_id.ids[0]);
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
    const bench_profile = getenv("ANTFLY_BENCH_QUERY_PROFILE") != null;
    const total_start_ns = if (bench_profile) platform_time.monotonicNs() else 0;
    var doc_constraints_ns: u64 = 0;
    var filter_doc_nums_ns: u64 = 0;
    var filter_doc_ids_ns: u64 = 0;
    var filter_intersect_ns: u64 = 0;
    var exclude_doc_nums_ns: u64 = 0;
    var exclude_doc_ids_ns: u64 = 0;
    var out = NativeDenseConstraints{};
    errdefer out.deinit(alloc);

    if (req.filter_ids.len > 0) {
        out.positive_filter = true;
        out.filter_ids = req.filter_ids;
    }

    const apply_broad_live_docs = apply_live_all_docs and
        !(try canSkipBroadLiveDenseConstraint(req, executor));
    const doc_constraints_start_ns = if (bench_profile) platform_time.monotonicNs() else 0;
    var doc_constraints = try deriveNativeDocIdConstraintsAlloc(alloc, req, .{
        .ctx = executor.ctx,
        .text_index_entry = executor.text_index_entry,
        .resolve_doc_set_doc_ids = executor.resolve_doc_set_doc_ids,
        .resolve_doc_ids_to_doc_set = executor.resolve_doc_ids_to_doc_set,
        .live_filter_doc_set = executor.live_filter_doc_set,
        .project_ordinals_to_doc_ids = false,
        .apply_live_all_docs = apply_broad_live_docs,
    });
    if (bench_profile) doc_constraints_ns = platform_time.monotonicNs() - doc_constraints_start_ns;
    defer doc_constraints.deinit(alloc);

    if (doc_constraints.positive_filter) {
        var mapped: []u64 = &.{};
        var mapped_owned = false;
        if (doc_constraints.filter_doc_nums.len > 0) {
            const phase_start_ns = if (bench_profile) platform_time.monotonicNs() else 0;
            mapped = try denseVectorIdsForDocNumsAlloc(alloc, doc_constraints.filter_doc_nums, executor, index_name);
            if (bench_profile) filter_doc_nums_ns += platform_time.monotonicNs() - phase_start_ns;
            mapped_owned = true;
        }
        if (doc_constraints.filter_doc_ids.len > 0) {
            const phase_start_ns = if (bench_profile) platform_time.monotonicNs() else 0;
            const mapped_doc_ids = try denseVectorIdsForDocIdsAlloc(alloc, doc_constraints.filter_doc_ids, executor, index_name);
            if (bench_profile) filter_doc_ids_ns += platform_time.monotonicNs() - phase_start_ns;
            if (mapped_owned) {
                const intersect_start_ns = if (bench_profile) platform_time.monotonicNs() else 0;
                const intersected = try intersectVectorIdsAlloc(alloc, mapped, mapped_doc_ids);
                if (bench_profile) filter_intersect_ns += platform_time.monotonicNs() - intersect_start_ns;
                alloc.free(mapped);
                alloc.free(mapped_doc_ids);
                mapped = intersected;
            } else {
                mapped = mapped_doc_ids;
            }
            mapped_owned = true;
        }
        if (out.positive_filter) {
            const intersect_start_ns = if (bench_profile) platform_time.monotonicNs() else 0;
            const intersected = try intersectVectorIdsAlloc(alloc, out.filter_ids, mapped);
            if (bench_profile) filter_intersect_ns += platform_time.monotonicNs() - intersect_start_ns;
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
        const phase_start_ns = if (bench_profile) platform_time.monotonicNs() else 0;
        const mapped_excludes = try denseVectorIdsForDocNumsAlloc(alloc, doc_constraints.exclude_doc_nums, executor, index_name);
        if (bench_profile) exclude_doc_nums_ns += platform_time.monotonicNs() - phase_start_ns;
        try mergeNativeExcludeIds(alloc, &out, mapped_excludes, req.exclude_ids);
    }
    if (doc_constraints.exclude_doc_ids.len > 0) {
        const phase_start_ns = if (bench_profile) platform_time.monotonicNs() else 0;
        const mapped_excludes = try denseVectorIdsForDocIdsAlloc(alloc, doc_constraints.exclude_doc_ids, executor, index_name);
        if (bench_profile) exclude_doc_ids_ns += platform_time.monotonicNs() - phase_start_ns;
        try mergeNativeExcludeIds(alloc, &out, mapped_excludes, req.exclude_ids);
    }
    if (out.exclude_ids.len == 0 and req.exclude_ids.len > 0) {
        out.exclude_ids = req.exclude_ids;
    }
    out.resolved_stored_filters = doc_constraints.resolved_stored_filters;
    out.filter_query_json_resolved = doc_constraints.filter_query_json_resolved;
    out.exclusion_query_json_resolved = doc_constraints.exclusion_query_json_resolved;
    if (bench_profile) {
        std.log.info(
            "antfly_bench_dense_constraints total_us={d} doc_constraints_us={d} filter_doc_nums_us={d} filter_doc_ids_us={d} filter_intersect_us={d} exclude_doc_nums_us={d} exclude_doc_ids_us={d} positive_filter={} filter_doc_nums={d} filter_doc_ids={d} out_filter_ids={d} exclude_doc_nums={d} exclude_doc_ids={d} out_exclude_ids={d}",
            .{
                nsToUs(platform_time.monotonicNs() - total_start_ns),
                nsToUs(doc_constraints_ns),
                nsToUs(filter_doc_nums_ns),
                nsToUs(filter_doc_ids_ns),
                nsToUs(filter_intersect_ns),
                nsToUs(exclude_doc_nums_ns),
                nsToUs(exclude_doc_ids_ns),
                doc_constraints.positive_filter,
                doc_constraints.filter_doc_nums.len,
                doc_constraints.filter_doc_ids.len,
                out.filter_ids.len,
                doc_constraints.exclude_doc_nums.len,
                doc_constraints.exclude_doc_ids.len,
                out.exclude_ids.len,
            },
        );
    }
    return out;
}

fn canSkipBroadLiveDenseConstraint(req: types.SearchRequest, executor: DenseSearchExecutor) !bool {
    if (req.filter_query_json.len != 0 or req.exclusion_query_json.len != 0) return false;
    if (req.resolved_doc_filter != null) return false;
    if (req.filter_doc_ids.len != 0 or req.exclude_doc_ids.len != 0) return false;
    if (req.filter_ids.len != 0 or req.exclude_ids.len != 0 or req.filter_doc_ids_positive) return false;
    const visible = executor.all_docs_visible_fast orelse return false;
    return try visible(executor.ctx, req.identity_read_generation);
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
    if (left.len == 0 or right.len == 0) return try alloc.alloc(u64, 0);
    var sorted_right = try alloc.dupe(u64, right);
    defer alloc.free(sorted_right);
    std.mem.sort(u64, sorted_right, {}, u64LessThan);
    const unique_right = sorted_right[0..uniqueSortedU64(sorted_right)];

    var sorted_left = try alloc.dupe(u64, left);
    defer alloc.free(sorted_left);
    std.mem.sort(u64, sorted_left, {}, u64LessThan);
    const unique_left = sorted_left[0..uniqueSortedU64(sorted_left)];

    var out = std.ArrayListUnmanaged(u64).empty;
    errdefer out.deinit(alloc);
    for (unique_left) |id| {
        if (containsSortedU64(unique_right, id)) try out.append(alloc, id);
    }
    return try out.toOwnedSlice(alloc);
}

fn unionVectorIdsAlloc(alloc: Allocator, left: []const u64, right: []const u64) ![]u64 {
    var out = try alloc.alloc(u64, left.len + right.len);
    errdefer alloc.free(out);
    @memcpy(out[0..left.len], left);
    @memcpy(out[left.len..], right);
    std.mem.sort(u64, out, {}, u64LessThan);
    const unique_len = uniqueSortedU64(out);
    return try alloc.realloc(out, unique_len);
}

fn containsVectorId(items: []const u64, id: u64) bool {
    for (items) |item| if (item == id) return true;
    return false;
}

fn u64LessThan(_: void, left: u64, right: u64) bool {
    return left < right;
}

fn uniqueSortedU64(values: []u64) usize {
    if (values.len == 0) return 0;
    var out: usize = 1;
    for (values[1..]) |value| {
        if (value == values[out - 1]) continue;
        values[out] = value;
        out += 1;
    }
    return out;
}

fn containsSortedU64(values: []const u64, expected: u64) bool {
    return std.sort.binarySearch(u64, values, expected, compareU64) != null;
}

fn compareU64(expected: u64, item: u64) std.math.Order {
    return std.math.order(expected, item);
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
    try checkSearchRequestDeadline(req);
    const bench_query_profile = shouldLogBenchQueryProfile();
    const total_start_ns = if (bench_query_profile) platform_time.monotonicNs() else 0;
    const text_entry = (try executor.text_index_entry(executor.ctx, req.index_name)) orelse return switch (text_query) {
        .match_all => executor.search_match_all(executor.ctx, alloc, req),
        else => error.IndexNotFound,
    };
    var effective_req = req;
    if (effective_req.index_name == null) effective_req.index_name = text_entry.config.name;
    const text_index = &text_entry.persistent;
    const chunk_backed = try executor.text_index_is_chunk_backed(executor.ctx, alloc, effective_req.index_name);
    const group_chunk_parents = shouldGroupChunkParents(effective_req, chunk_backed);
    const paging = componentPaging(effective_req);

    var arena = std.heap.ArenaAllocator.init(alloc);
    defer arena.deinit();
    const arena_alloc = arena.allocator();
    const base_search_query = try textQueryToSearchQuery(arena_alloc, text_query, text_entry.text_analysis, text_entry.runtime_schema);
    const snapshot = text_index.snapshot();
    const can_apply_live_all_docs = !chunk_backed or snapshot.hasDocOrdinalCoverage();
    const constraints_start_ns = if (bench_query_profile) platform_time.monotonicNs() else 0;
    var constraint_req = effective_req;
    constraint_req.resolved_doc_filter = null;
    constraint_req.full_text = null;
    var native_constraints = try deriveNativeDocIdConstraintsAlloc(alloc, constraint_req, .{
        .ctx = executor.ctx,
        .text_index_entry = executor.text_index_entry,
        .resolve_doc_set_doc_ids = executor.resolve_doc_set_doc_ids,
        .resolve_doc_ids_to_doc_set = executor.resolve_doc_ids_to_doc_set,
        .live_filter_doc_set = executor.live_filter_doc_set,
        .project_ordinals_to_doc_ids = false,
        .text_snapshot_for_doc_num_projection = snapshot,
        .apply_live_all_docs = can_apply_live_all_docs,
    });
    defer native_constraints.deinit(alloc);
    const derive_constraints_ns = if (bench_query_profile) platform_time.monotonicNs() - constraints_start_ns else 0;

    const resolved_filter_start_ns = if (bench_query_profile) platform_time.monotonicNs() else 0;
    if (resolvedTextDocNumFilterFromRequest(effective_req)) |filter| {
        try applyResolvedTextDocNumFilterAlloc(alloc, &native_constraints, filter);
    } else if (resolvedDocFilterFromRequest(effective_req)) |filter| {
        try applyResolvedDocFilterToTextDocNumsAlloc(alloc, snapshot, &native_constraints, filter, .{
            .ctx = executor.ctx,
            .text_index_entry = executor.text_index_entry,
            .resolve_doc_set_doc_ids = executor.resolve_doc_set_doc_ids,
            .resolve_doc_ids_to_doc_set = executor.resolve_doc_ids_to_doc_set,
            .live_filter_doc_set = executor.live_filter_doc_set,
            .project_ordinals_to_doc_ids = false,
            .identity_read_generation = effective_req.identity_read_generation,
        });
    }
    const resolved_filter_ns = if (bench_query_profile) platform_time.monotonicNs() - resolved_filter_start_ns else 0;
    const convert_start_ns = if (bench_query_profile) platform_time.monotonicNs() else 0;
    try convertNativeDocIdsToTextDocNumsAlloc(alloc, snapshot, &native_constraints);
    try normalizeNativeDocNumConstraintsAlloc(alloc, &native_constraints);
    const convert_constraints_ns = if (bench_query_profile) platform_time.monotonicNs() - convert_start_ns else 0;

    if (native_constraints.positive_filter and native_constraints.filter_doc_ids.len == 0 and native_constraints.filter_doc_nums.len == 0) {
        return executor.postprocess(executor.ctx, alloc, effective_req, .{
            .alloc = alloc,
            .hits = &.{},
            .total_hits = 0,
            .graph_results = &.{},
        }, chunk_backed);
    }
    const late_visibility_paginate = try searchTextNeedsLateVisibilityFilter(
        executor,
        can_apply_live_all_docs,
        native_constraints.positive_filter,
        effective_req.identity_read_generation,
    );
    const full_candidate_limit = effectiveTextCandidateLimit(snapshot.global_doc_count, native_constraints);
    const requires_field_sort = effective_req.order_by.len > 0;
    const search_query = try textSearchQueryWithNativeDocIdsAlloc(arena_alloc, base_search_query, native_constraints, effective_req.count_only);
    const load_stored_in_search_engine = effective_req.include_stored and !chunk_backed and !requires_field_sort;
    var field_sort_plan = SortExecutionPlan{ .kind = .none };
    if (requires_field_sort) field_sort_plan = try planTextNativeSortFields(effective_req, snapshot, text_entry.runtime_schema);
    const exact_late_visibility_totals = late_visibility_paginate and
        (effective_req.count_only or
            effective_req.limit == 0 or
            effective_req.aggregations_json.len != 0 or
            effective_req.graph_queries.len != 0 or
            group_chunk_parents);
    const exact_candidate_budget = lateVisibilityExactCandidateBudget();
    if (exact_late_visibility_totals) {
        enforceLateVisibilityExactCandidateBudget(full_candidate_limit, exact_candidate_budget) catch |err| {
            logExactSortBudgetRejection(
                "text",
                .text_exact_late_visibility_totals,
                effective_req.index_name,
                full_candidate_limit,
                exact_candidate_budget,
                if (requires_field_sort) field_sort_plan else null,
            );
            return err;
        };
    }
    const adaptive_late_visibility = late_visibility_paginate and !exact_late_visibility_totals;
    const requested_visible_end = effective_req.offset +| effective_req.limit;
    const collect_window_candidates = group_chunk_parents or late_visibility_paginate or requires_field_sort;
    var candidate_limit: u32 = if (collect_window_candidates)
        if (requires_field_sort)
            @min(full_candidate_limit, exact_candidate_budget)
        else if (adaptive_late_visibility)
            @min(full_candidate_limit, @max(@as(u32, 1), @max(paging.limit, requested_visible_end)))
        else
            full_candidate_limit
    else
        paging.limit;
    var candidate_iterations: u32 = 0;
    var execute_ns: u64 = 0;
    var hits_ns: u64 = 0;
    var postprocess_ns: u64 = 0;

    while (true) {
        try checkSearchRequestDeadline(effective_req);
        candidate_iterations += 1;
        var postprocess_req = effective_req;
        if (late_visibility_paginate or requires_field_sort) {
            postprocess_req.offset = 0;
            postprocess_req.limit = candidate_limit;
        }

        const execute_start_ns = if (bench_query_profile) platform_time.monotonicNs() else 0;
        var result = if (effective_req.count_only)
            try search_mod.executeCountCandidates(alloc, snapshot, search_query)
        else
            try search_mod.execute(alloc, snapshot, .{
                .query = search_query,
                .k = if (collect_window_candidates) candidate_limit else paging.limit,
                .offset = if (collect_window_candidates) 0 else paging.offset,
                .include_stored = load_stored_in_search_engine,
                .distributed_text_stats = effective_req.distributed_text_stats,
                .filter_doc_nums = native_constraints.filter_doc_nums,
                .filter_doc_nums_positive = native_constraints.positive_filter,
                .exclude_doc_nums = native_constraints.exclude_doc_nums,
            });
        defer result.deinit();
        if (bench_query_profile) execute_ns += platform_time.monotonicNs() - execute_start_ns;

        const candidates_exhausted = !collect_window_candidates or
            candidate_limit >= full_candidate_limit or
            (result.total_hits_relation == .exact and result.total_hits <= candidate_limit);

        const hits_start_ns = if (bench_query_profile) platform_time.monotonicNs() else 0;
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
            if (i % 1024 == 0) try checkSearchRequestDeadline(effective_req);
            const doc_ordinal = try snapshot.docOrdinal(hit.doc_id);
            const id = hit.id orelse {
                const stored = snapshot.storedDoc(hit.doc_id) orelse return error.StoredDocMissing;
                var materialized = types.SearchHit{
                    .id = try alloc.dupe(u8, stored.id),
                    .doc_ordinal = doc_ordinal,
                    .native_text_doc_id = hit.doc_id,
                    .score = hit.score,
                    .stored_data = null,
                };
                var assigned = false;
                errdefer if (!assigned) materialized.deinit(alloc);
                materialized.index_scores = try types.cloneIndexScores(alloc, hit.index_scores);
                hits[i] = materialized;
                assigned = true;
                initialized += 1;
                continue;
            };

            var materialized = types.SearchHit{
                .id = try alloc.dupe(u8, id),
                .doc_ordinal = doc_ordinal,
                .native_text_doc_id = hit.doc_id,
                .score = hit.score,
                .stored_data = null,
            };
            var assigned = false;
            errdefer if (!assigned) materialized.deinit(alloc);
            materialized.index_scores = try types.cloneIndexScores(alloc, hit.index_scores);
            materialized.stored_data = if (load_stored_in_search_engine and hit.stored_data != null)
                try executor.project_stored_search(executor.ctx, alloc, effective_req, id, hit.stored_data.?)
            else
                null;
            hits[i] = materialized;
            assigned = true;
            initialized += 1;
        }
        if (bench_query_profile) hits_ns += platform_time.monotonicNs() - hits_start_ns;

        owns_hits = false;
        const postprocess_start_ns = if (bench_query_profile) platform_time.monotonicNs() else 0;
        var out = try executor.postprocess(executor.ctx, alloc, postprocess_req, .{
            .alloc = alloc,
            .hits = hits,
            .total_hits = result.total_hits,
            .total_hits_relation = switch (result.total_hits_relation) {
                .exact => .exact,
                .gte => .gte,
            },
            .graph_results = &.{},
        }, chunk_backed);
        errdefer out.deinit();
        if (bench_query_profile) postprocess_ns += platform_time.monotonicNs() - postprocess_start_ns;

        const visible_candidate_count: u32 = @intCast(@min(out.hits.len, @as(usize, std.math.maxInt(u32))));
        if (adaptive_late_visibility and !candidates_exhausted and visible_candidate_count < requested_visible_end) {
            out.deinit();
            const grown_limit = @min(full_candidate_limit, @max(candidate_limit +| 1, candidate_limit *| 2));
            if (grown_limit == candidate_limit) return error.InvalidQueryRequest;
            candidate_limit = grown_limit;
            continue;
        }
        if (adaptive_late_visibility and !candidates_exhausted) {
            out.total_hits = visible_candidate_count;
            out.total_hits_relation = .gte;
        }
        if (requires_field_sort and !candidates_exhausted) {
            logExactSortBudgetRejection(
                "text",
                .text_field_sort_candidate_window,
                effective_req.index_name,
                result.total_hits,
                candidate_limit,
                field_sort_plan,
            );
            return error.QueryCandidateBudgetExceeded;
        }
        try checkSearchRequestDeadline(effective_req);
        if (requires_field_sort) {
            if (field_sort_plan.kind == .native_doc_values_top_n) {
                const native_sort_ctx = TextDocValueSortContext{ .snapshot = snapshot };
                try sortAndPageSearchResultInPlace(&out, effective_req, executor.ctx, executor.load_stored, field_sort_plan, .{
                    .ctx = @constCast(&native_sort_ctx),
                    .require_native = field_sort_plan.require_native,
                    .load = loadTextDocValueSortValue,
                });
            } else {
                try sortAndPageSearchResultInPlace(&out, effective_req, executor.ctx, executor.load_stored, field_sort_plan, null);
            }
            if (effective_req.include_stored and !chunk_backed) {
                const source_profile = try loadMissingProjectedTextHitDocuments(alloc, effective_req, executor, out.hits);
                logBenchProjectedSourceLoadProfile(effective_req, field_sort_plan, "text", source_profile);
            }
        } else if (late_visibility_paginate and !effective_req.count_only) {
            try paginateSearchResultInPlace(&out, effective_req.offset, effective_req.limit);
        }
        if (bench_query_profile) {
            std.log.info(
                "antfly_bench_text_query index={s} total_us={d} derive_constraints_us={d} apply_resolved_us={d} convert_constraints_us={d} execute_us={d} hits_us={d} postprocess_us={d} positive_filter={} filter_doc_nums={d} filter_doc_ids={d} exclude_doc_nums={d} exclude_doc_ids={d} snapshot_docs={d} candidate_window={d} candidate_iterations={d} late_visibility={} hits={d} total_hits={d}",
                .{
                    effective_req.index_name orelse "",
                    nsToUs(platform_time.monotonicNs() - total_start_ns),
                    nsToUs(derive_constraints_ns),
                    nsToUs(resolved_filter_ns),
                    nsToUs(convert_constraints_ns),
                    nsToUs(execute_ns),
                    nsToUs(hits_ns),
                    nsToUs(postprocess_ns),
                    native_constraints.positive_filter,
                    native_constraints.filter_doc_nums.len,
                    native_constraints.filter_doc_ids.len,
                    native_constraints.exclude_doc_nums.len,
                    native_constraints.exclude_doc_ids.len,
                    snapshot.global_doc_count,
                    candidate_limit,
                    candidate_iterations,
                    late_visibility_paginate,
                    out.hits.len,
                    out.total_hits,
                },
            );
        }
        return out;
    }
}

fn textSearchQueryWithNativeDocIdsAlloc(
    alloc: Allocator,
    base: search_mod.SearchQuery,
    constraints: NativeDocIdConstraints,
    include_doc_nums: bool,
) !search_mod.SearchQuery {
    const has_include_doc_ids = constraints.positive_filter and constraints.filter_doc_ids.len > 0;
    const has_include_doc_nums = include_doc_nums and constraints.positive_filter and constraints.filter_doc_nums.len > 0;
    const has_exclude_doc_ids = constraints.exclude_doc_ids.len > 0;
    const has_exclude_doc_nums = include_doc_nums and constraints.exclude_doc_nums.len > 0;
    const has_native_doc_nums = !include_doc_nums and
        ((constraints.positive_filter and constraints.filter_doc_nums.len > 0) or constraints.exclude_doc_nums.len > 0);
    const has_include = has_include_doc_ids or has_include_doc_nums;
    const has_exclude = has_exclude_doc_ids or has_exclude_doc_nums;
    if (!has_include and !has_exclude and !has_native_doc_nums) return base;

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

fn normalizeNativeDocNumConstraintsAlloc(
    alloc: Allocator,
    constraints: *NativeDocIdConstraints,
) !void {
    try normalizeNativeDocNumSliceAlloc(alloc, &constraints.filter_doc_nums, &constraints.filter_doc_nums_owned);
    try normalizeNativeDocNumSliceAlloc(alloc, &constraints.exclude_doc_nums, &constraints.exclude_doc_nums_owned);
}

fn normalizeNativeDocNumSliceAlloc(
    alloc: Allocator,
    items: *[]const u32,
    owned: *bool,
) !void {
    if (items.*.len == 0) return;
    const mutable = if (owned.*)
        @constCast(items.*)
    else blk: {
        const copy = try alloc.dupe(u32, items.*);
        items.* = copy;
        owned.* = true;
        break :blk copy;
    };
    std.mem.sort(u32, mutable, {}, u32LessThan);
    const unique_len = uniqueSortedU32(mutable);
    if (unique_len != mutable.len) {
        items.* = try alloc.realloc(mutable, unique_len);
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
            if (seg.shared.deleted) |deleted| {
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
            if (seg.shared.deleted) |deleted| {
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
        for ([_][]const u8{ "must", "filter", "should", "must_not" }) |key| {
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
    try rejectApproximateSortPageOptions(req);
    const total_start = platform_time.monotonicNs();

    const index_lookup_start = total_start;
    const entry = (try executor.dense_index(executor.ctx, req.index_name)) orelse return error.IndexNotFound;
    profile.index_lookup_ns = platform_time.monotonicNs() - index_lookup_start;

    const chunk_backed = entry.chunk_name != null;
    const group_chunk_parents = shouldGroupChunkParents(req, chunk_backed);
    const paging = componentPaging(req);
    const index_stats = entry.index.stats();
    const constraint_start = platform_time.monotonicNs();
    var native_constraints = try deriveNativeDenseConstraintsAlloc(alloc, req, executor, req.index_name orelse entry.config.name, true);
    profile.constraint_ns = platform_time.monotonicNs() - constraint_start;
    defer native_constraints.deinit(alloc);
    const unresolved_stored_filters =
        (req.filter_query_json.len > 0 and !native_constraints.filter_query_json_resolved) or
        (req.exclusion_query_json.len > 0 and !native_constraints.exclusion_query_json_resolved);
    const postprocess_req = requestWithoutResolvedStoredFilters(
        req,
        native_constraints.filter_query_json_resolved,
        native_constraints.exclusion_query_json_resolved,
    );
    const full_candidate_window = group_chunk_parents or unresolved_stored_filters;
    const page_candidate_window = pagingCandidateWindow(paging);
    const effective_k: u32 = if (full_candidate_window)
        @intCast(index_stats.active_count)
    else
        @max(dense.k, page_candidate_window);
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
    var candidate_window: u32 = if (full_candidate_window)
        initialDenseFullCandidateWindow(bounded_full_candidate_count, paging)
    else
        effective_k;

    while (true) {
        const hbc_effective_k: u32 = if (full_candidate_window) candidate_window else effective_k;
        const hbc_req: vectorindex_mod.SearchRequest = .{
            .query = dense.vector,
            .k = hbc_effective_k,
            .rerank_k = if (full_candidate_window) @as(usize, @intCast(@min(paging.offset +| paging.limit, hbc_effective_k))) else null,
            .search_width = resolved_search_width,
            .epsilon = resolved_epsilon,
            .rerank_factor = resolveRerankFactor(effort),
            .filter_prefix = req.filter_prefix,
            .distance_over = req.distance_over,
            .distance_under = req.distance_under,
            .filter_ids = effective_filter_ids,
            .exclude_ids = effective_exclude_ids,
        };

        const hbc_search_start = platform_time.monotonicNs();
        const use_exact_native_filter = shouldExactScoreNativeDenseFilter(native_constraints, paging);
        var results = if (use_exact_native_filter) blk: {
            const exact = if (executor.exact_dense_search) |exact_search|
                try exact_search(executor.ctx, entry, hbc_req)
            else
                try exactScoreNativeDenseFilter(alloc, entry, hbc_req);
            profile.hbc_exact_vectors_scored = @intCast(native_constraints.filter_ids.len);
            break :blk exact;
        } else if (collect_hbc_profile) blk: {
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
            profile.hbc_leaf_payload_stale = profiled.profile.leaf_payload_stale;
            profile.hbc_leaf_payload_missing = profiled.profile.leaf_payload_missing;
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
        profile.hbc_search_ns += platform_time.monotonicNs() - hbc_search_start;
        defer results.deinit();

        const raw_hits = results.getHits();
        profile.raw_hit_count = @intCast(raw_hits.len);
        const candidate_window_incomplete = denseCandidateWindowIncomplete(hbc_effective_k, bounded_full_candidate_count, raw_hits.len);
        const start: u32 = if (full_candidate_window) 0 else @min(paging.offset, @as(u32, @intCast(raw_hits.len)));
        const end: u32 = if (full_candidate_window) @intCast(raw_hits.len) else @min(start + paging.limit, @as(u32, @intCast(raw_hits.len)));

        var hits = std.ArrayListUnmanaged(types.SearchHit).empty;
        var hit_vector_ids = std.ArrayListUnmanaged(u64).empty;
        defer hit_vector_ids.deinit(alloc);
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
            const load_stored_before_postprocess = postprocess_req.include_stored and
                !(chunk_backed and group_chunk_parents) and
                !unresolved_stored_filters;
            if (load_stored_before_postprocess) {
                const load_start = platform_time.monotonicNs();
                stored_data = try executor.load_projected_document(executor.ctx, alloc, postprocess_req, doc_key);
                stored_data_owned = true;
                profile.load_projected_document_ns += platform_time.monotonicNs() - load_start;
            }
            try hit_vector_ids.append(alloc, hit.vector_id);
            try hits.append(alloc, .{
                .id = doc_key,
                .doc_ordinal = null,
                .score = hit.distance,
                .stored_data = stored_data,
            });
            doc_key_owned = false;
            stored_data_owned = false;
        }
        const ordinal_lookup_start = platform_time.monotonicNs();
        try lookupDenseHitDocOrdinals(alloc, postprocess_req, executor, hit_vector_ids.items, hits.items);
        profile.doc_ordinal_lookup_ns += platform_time.monotonicNs() - ordinal_lookup_start;

        const postprocess_start = platform_time.monotonicNs();
        const dense_hits_total: u32 = @intCast(hits.items.len);
        const dense_hits = try hits.toOwnedSlice(alloc);
        var result = try executor.postprocess(executor.ctx, alloc, postprocess_req, .{
            .alloc = alloc,
            .hits = dense_hits,
            .total_hits = dense_hits_total,
            .total_hits_relation = if (candidate_window_incomplete) .gte else .exact,
            .graph_results = &.{},
        }, chunk_backed);
        errdefer result.deinit();

        const visible_candidate_count = result.total_hits;
        if (full_candidate_window and candidate_window_incomplete and visible_candidate_count < page_candidate_window) {
            result.deinit();
            const grown_window = growDenseFullCandidateWindow(candidate_window, bounded_full_candidate_count, page_candidate_window);
            if (grown_window == candidate_window) return error.InvalidQueryRequest;
            candidate_window = grown_window;
            continue;
        }
        if (candidate_window_incomplete) result.total_hits_relation = .gte;
        if (unresolved_stored_filters) {
            result = try pageSearchResultInPlace(alloc, result, paging);
        }
        profile.postprocess_ns += platform_time.monotonicNs() - postprocess_start;
        if (postprocess_req.include_stored and !(chunk_backed and group_chunk_parents)) {
            const load_start = platform_time.monotonicNs();
            try loadMissingProjectedDenseHitDocuments(alloc, postprocess_req, executor, result.hits);
            profile.load_projected_document_ns += platform_time.monotonicNs() - load_start;
        }
        profile.returned_hit_count = result.total_hits;
        profile.total_ns = platform_time.monotonicNs() - total_start;
        if (bench_query_profile) logBenchDenseQueryProfile(req, dense, index_stats, profile);
        return result;
    }
}

fn shouldLogBenchQueryProfile() bool {
    const every = benchQueryProfileEvery() orelse return false;
    if (every == 0) return false;
    const current = bench_query_profile_counter.fetchAdd(1, .monotonic) + 1;
    return current % every == 0;
}

fn shouldExactScoreNativeDenseFilter(
    native_constraints: NativeDenseConstraints,
    paging: ComponentPaging,
) bool {
    if (!native_constraints.positive_filter) return false;
    if (native_constraints.filter_ids.len == 0) return false;
    const paging_budget = pagingCandidateWindow(paging) *| 32;
    const budget = @max(paging_budget, default_exact_native_filter_candidate_budget);
    return native_constraints.filter_ids.len <= budget;
}

fn pagingCandidateWindow(paging: ComponentPaging) u32 {
    return paging.offset +| paging.limit;
}

fn initialDenseFullCandidateWindow(bounded_full_candidate_count: u32, paging: ComponentPaging) u32 {
    if (bounded_full_candidate_count == 0) return 0;
    const overfetch_window = @max(paging.limit *| 32, @as(u32, 1024));
    return @min(bounded_full_candidate_count, @max(overfetch_window, pagingCandidateWindow(paging)));
}

fn growDenseFullCandidateWindow(current: u32, bounded_full_candidate_count: u32, requested_visible_end: u32) u32 {
    if (current >= bounded_full_candidate_count) return current;
    const grown = @max(current +| 1, @max(current *| 2, requested_visible_end));
    return @min(bounded_full_candidate_count, grown);
}

fn denseCandidateWindowIncomplete(candidate_window: u32, bounded_full_candidate_count: u32, raw_hits_len: usize) bool {
    _ = raw_hits_len;
    return candidate_window < bounded_full_candidate_count;
}

test "dense full candidate window covers requested offset page and grows bounded" {
    const paging = ComponentPaging{ .offset = 1024, .limit = 1 };
    try std.testing.expectEqual(@as(u32, 1025), initialDenseFullCandidateWindow(2000, paging));
    try std.testing.expectEqual(@as(u32, 2000), growDenseFullCandidateWindow(1025, 2000, 1025));
    try std.testing.expect(denseCandidateWindowIncomplete(1025, 2000, 1025));
    try std.testing.expect(!denseCandidateWindowIncomplete(2000, 2000, 1025));
    try std.testing.expectEqual(@as(u32, 7), initialDenseFullCandidateWindow(7, paging));
}

fn exactScoreNativeDenseFilter(
    alloc: Allocator,
    entry: *index_manager_mod.IndexManager.DenseIndex,
    req: vectorindex_mod.SearchRequest,
) !vectorindex_mod.SearchResults {
    var results = try vectorindex_mod.SearchResults.initCapacity(
        alloc,
        req.k,
        req.k,
        @min(req.k, req.filter_ids.len),
    );
    errdefer results.deinit();

    var txn = try entry.index.beginReadTxn();
    defer txn.abort();

    const vector_scratch = try alloc.alloc(f32, entry.dims);
    defer alloc.free(vector_scratch);
    const query_measure = vector_mod.norm(req.query);

    for (req.filter_ids) |vector_id| {
        if (containsVectorId(req.exclude_ids, vector_id)) continue;
        const vector = entry.index.getVectorViewOrScratch(&txn, vector_id, vector_scratch) catch |err| switch (err) {
            error.NotFound => continue,
            else => return err,
        };
        if (vector.len != req.query.len) return error.DimensionMismatch;

        const distance = vector_mod.distanceToQuery(req.query, query_measure, vector, entry.metric);
        if (!std.math.isFinite(distance)) continue;
        if (req.distance_over) |threshold| {
            if (distance <= threshold) continue;
        }
        if (req.distance_under) |threshold| {
            if (distance >= threshold) continue;
        }
        if (req.filter_prefix.len > 0) {
            const metadata = (try entry.index.getMetadataInTxn(&txn, vector_id)) orelse continue;
            if (!std.mem.startsWith(u8, metadata, req.filter_prefix)) continue;
        }
        results.addResult(vector_id, distance, 0);
    }
    results.sort();
    return results;
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

fn loadMissingProjectedDenseHitDocuments(
    alloc: Allocator,
    req: types.SearchRequest,
    executor: DenseSearchExecutor,
    hits: []types.SearchHit,
) !void {
    for (hits) |*hit| {
        if (hit.stored_data != null) continue;
        hit.stored_data = try executor.load_projected_document(executor.ctx, alloc, req, hit.id);
    }
}

fn lookupDenseHitDocOrdinals(
    alloc: Allocator,
    req: types.SearchRequest,
    executor: DenseSearchExecutor,
    vector_ids: []const u64,
    hits: []types.SearchHit,
) !void {
    if (!denseHitPageNeedsDocOrdinals(req) or hits.len == 0) return;
    if (executor.lookup_doc_ordinals_for_vector_ids) |lookup_for_vector_ids| {
        if (req.index_name) |index_name| {
            const ordinals = try lookup_for_vector_ids(executor.ctx, alloc, index_name, vector_ids, req.identity_read_generation);
            defer alloc.free(ordinals);
            if (ordinals.len != hits.len) return error.InvalidDocIdentity;
            var missing_count: usize = 0;
            for (hits, 0..) |*hit, i| {
                hit.doc_ordinal = ordinals[i];
                if (ordinals[i] == null) missing_count += 1;
            }
            if (missing_count == 0) return;
        }
    }
    if (executor.lookup_doc_ordinals) |lookup_many| {
        var doc_ids = try std.ArrayListUnmanaged([]const u8).initCapacity(alloc, hits.len);
        defer doc_ids.deinit(alloc);
        var hit_indexes = try std.ArrayListUnmanaged(usize).initCapacity(alloc, hits.len);
        defer hit_indexes.deinit(alloc);
        for (hits, 0..) |hit, i| {
            if (hit.doc_ordinal != null) continue;
            doc_ids.appendAssumeCapacity(hit.id);
            hit_indexes.appendAssumeCapacity(i);
        }
        if (doc_ids.items.len == 0) return;
        const ordinals = try lookup_many(executor.ctx, alloc, doc_ids.items, req.identity_read_generation);
        defer alloc.free(ordinals);
        if (ordinals.len != doc_ids.items.len) return error.InvalidDocIdentity;
        for (hit_indexes.items, 0..) |hit_index, i| hits[hit_index].doc_ordinal = ordinals[i];
        return;
    }
    for (hits) |*hit| {
        if (hit.doc_ordinal != null) continue;
        hit.doc_ordinal = try lookupDenseHitDocOrdinal(alloc, req, executor, hit.id);
    }
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
        "antfly_bench_dense_query index={s} primary_text={s} has_filter={} has_exclusion={} k={d} limit={d} offset={d} effort={d:.3} nodes={d} active={d} estimated_leaves={d} leaf_size={d} branching={d} search_width={d} epsilon={d:.3} total_us={d} index_lookup_us={d} constraint_us={d} hbc_us={d} doc_key_us={d} doc_ordinal_us={d} load_projected_us={d} postprocess_us={d} raw_hits={d} returned_hits={d}",
        .{
            req.index_name orelse "",
            req.primary_text_index_name orelse "",
            req.filter_query_json.len > 0,
            req.exclusion_query_json.len > 0,
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
            nsToUs(profile.constraint_ns),
            nsToUs(profile.hbc_search_ns),
            nsToUs(profile.doc_key_resolve_ns),
            nsToUs(profile.doc_ordinal_lookup_ns),
            nsToUs(profile.load_projected_document_ns),
            nsToUs(profile.postprocess_ns),
            profile.raw_hit_count,
            profile.returned_hit_count,
        },
    );
    std.log.info(
        "antfly_bench_dense_query_hbc index={s} nodes_visited={d} leaves={d} approx_vectors={d} exact_vectors={d} payload_stale={d} payload_missing={d} reranked={d} approx_candidates={d} rerank_candidates={d} ambiguous_top_k={d} ambiguous_boundary={d} distance_over_hits={d} distance_under_hits={d} full_rerank={any} top_k_count={d} min_distance_gap={d:.6} min_interval_gap={d:.6} rerank_vector_load_us={d} rerank_metadata_us={d} rerank_artifact_key_us={d} rerank_artifact_read_us={d} rerank_artifact_decode_us={d} rerank_artifact_distance_us={d} rerank_lsm_cache_hits={d} rerank_lsm_cache_misses={d} rerank_distance_us={d} inline_meta={d} fetched_meta={d} lookup_doc_key={d}",
        .{
            req.index_name orelse "",
            profile.hbc_nodes_visited,
            profile.hbc_leaves_explored,
            profile.hbc_approx_vectors_scored,
            profile.hbc_exact_vectors_scored,
            profile.hbc_leaf_payload_stale,
            profile.hbc_leaf_payload_missing,
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

// Search policy: effort 0..1 maps to a leaf-visit budget (search width), a
// dynamic-pruning epsilon, and a rerank candidate multiplier. The width floor
// scales with how many leaves are needed to cover k oversampled results, not
// with k itself, and the ramp between floor and ceiling is geometric so the
// knob behaves sensibly across index sizes. Epsilon stays small enough that
// the SPANN-style "skip leaves whose centroid is (1+eps) x the best" pruning
// actually fires. The constants can be overridden through environment
// variables (read once per process) for offline tuning.
const SearchPolicy = struct {
    epsilon_base: f32 = 0.90,
    epsilon_span: f32 = 1.10,
    width_k_factor: f32 = 4.0,
    width_min_leaves: u32 = 4,
    // Fraction of the leaf ceiling visited at balanced (0.5) effort. On the
    // 50K OpenAI case, recall at the default effort tracks this anchor; the
    // current HBC tree needs most of its leaves for ~0.98 recall, so the
    // default anchor sits high and the low-effort range is where the policy
    // saves work.
    width_mid_fraction: f32 = 0.70,
    // Absolute ceiling on the balanced-effort anchor so the default leaf
    // budget does not grow linearly with corpus size (matches the legacy
    // behavior at the 1M scale; no effect at 50K-scale trees).
    width_mid_cap: f32 = 2048.0,
    rerank_factor_base: f32 = 6.0,
    rerank_factor_span: f32 = 6.0,
};

var search_policy: SearchPolicy = .{};
var search_policy_loaded = std.atomic.Value(bool).init(false);

fn ensureSearchPolicyLoaded() void {
    if (search_policy_loaded.load(.acquire)) return;
    // Benign race: concurrent first callers recompute identical values from
    // the environment before either publishes the flag.
    loadSearchPolicyEnv();
    search_policy_loaded.store(true, .release);
}

fn searchPolicyEnvF32(name: [*:0]const u8, default_value: f32) f32 {
    const raw = getenv(name) orelse return default_value;
    return std.fmt.parseFloat(f32, raw) catch default_value;
}

fn loadSearchPolicyEnv() void {
    search_policy.epsilon_base = searchPolicyEnvF32("ANTFLY_SEARCH_EPSILON_BASE", search_policy.epsilon_base);
    search_policy.epsilon_span = searchPolicyEnvF32("ANTFLY_SEARCH_EPSILON_SPAN", search_policy.epsilon_span);
    search_policy.width_k_factor = searchPolicyEnvF32("ANTFLY_SEARCH_WIDTH_K_FACTOR", search_policy.width_k_factor);
    search_policy.width_mid_fraction = searchPolicyEnvF32("ANTFLY_SEARCH_WIDTH_MID_FRACTION", search_policy.width_mid_fraction);
    search_policy.width_mid_cap = searchPolicyEnvF32("ANTFLY_SEARCH_WIDTH_MID_CAP", search_policy.width_mid_cap);
    search_policy.rerank_factor_base = searchPolicyEnvF32("ANTFLY_SEARCH_RERANK_BASE", search_policy.rerank_factor_base);
    search_policy.rerank_factor_span = searchPolicyEnvF32("ANTFLY_SEARCH_RERANK_SPAN", search_policy.rerank_factor_span);
}

pub fn resolveSearchWidth(k: u32, effort: f32, stats: vectorindex_mod.IndexStats) u32 {
    ensureSearchPolicyLoaded();
    const leaf_size = @max(stats.leaf_size, 1);
    const estimated_leaf_count = estimateLeafCount(stats);
    const legacy_max_width = @max(@max(k, @as(u32, 64)) * 20, @as(u32, 4096));
    const max_width = if (estimated_leaf_count > 0)
        estimated_leaf_count
    else if (stats.node_count > legacy_max_width and stats.node_count <= std.math.maxInt(u32))
        @as(u32, @intCast(stats.node_count))
    else
        legacy_max_width;

    const k_leaves_u64 = std.math.divCeil(u64, @as(u64, @intFromFloat(@max(1.0, @ceil(@as(f32, @floatFromInt(k)) * search_policy.width_k_factor)))), leaf_size) catch 1;
    const k_leaves: u32 = @intCast(@min(k_leaves_u64, @as(u64, std.math.maxInt(u32))));
    const min_width = @min(max_width, @max(k_leaves, search_policy.width_min_leaves));
    if (min_width >= max_width) return max_width;
    if (effort <= 0) return min_width;
    if (effort >= 1) return max_width;

    // Two geometric segments anchored at width_mid_fraction x ceiling for the
    // balanced effort, so the default recall stays calibrated while the floor
    // and the low-effort range scale with k instead of the corpus.
    const min_f = @as(f32, @floatFromInt(min_width));
    const max_f = @as(f32, @floatFromInt(max_width));
    const mid_f = @min(@min(max_f, search_policy.width_mid_cap), @max(min_f, max_f * search_policy.width_mid_fraction));
    const width_f = if (effort <= 0.5)
        min_f * std.math.pow(f32, mid_f / min_f, effort * 2.0)
    else
        mid_f * std.math.pow(f32, max_f / mid_f, (effort - 0.5) * 2.0);
    const width: u32 = @intFromFloat(@min(width_f, max_f));
    return @max(min_width, @min(width, max_width));
}

pub fn resolveSearchEpsilon(effort: f32) f32 {
    ensureSearchPolicyLoaded();
    return search_policy.epsilon_base + (effort * search_policy.epsilon_span);
}

pub fn resolveRerankFactor(effort: f32) usize {
    ensureSearchPolicyLoaded();
    const factor = search_policy.rerank_factor_base + (effort * search_policy.rerank_factor_span);
    return @max(1, @as(usize, @intFromFloat(@round(factor))));
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
    try rejectApproximateSortPageOptions(req);
    const bench_query_profile = shouldLogBenchQueryProfile();
    const total_start_ns = if (bench_query_profile) platform_time.monotonicNs() else 0;
    var constraint_ns: u64 = 0;
    var index_search_ns: u64 = 0;
    var hit_build_ns: u64 = 0;
    var postprocess_ns: u64 = 0;
    var page_ns: u64 = 0;
    const entry = (try executor.sparse_index(executor.ctx, req.index_name)) orelse return error.IndexNotFound;
    const chunk_backed = entry.chunk_name != null;
    const group_chunk_parents = shouldGroupChunkParents(req, chunk_backed);
    const paging = componentPaging(req);
    const constraint_start_ns = if (bench_query_profile) platform_time.monotonicNs() else 0;
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
    if (bench_query_profile) constraint_ns = platform_time.monotonicNs() - constraint_start_ns;
    defer native_constraints.deinit(alloc);
    const unresolved_stored_filters =
        (req.filter_query_json.len > 0 and !native_constraints.filter_query_json_resolved) or
        (req.exclusion_query_json.len > 0 and !native_constraints.exclusion_query_json_resolved);
    const postprocess_req = requestWithoutResolvedStoredFilters(
        req,
        native_constraints.filter_query_json_resolved,
        native_constraints.exclusion_query_json_resolved,
    );
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
    const index_search_start_ns = if (bench_query_profile) platform_time.monotonicNs() else 0;
    const raw_hits = try entry.index.searchConstrained(alloc, &query, effective_k, .{
        .filter_doc_ids = native_constraints.filter_doc_ids,
        .exclude_doc_ids = native_constraints.exclude_doc_ids,
        .filter_doc_nums = native_constraints.filter_doc_nums,
        .exclude_doc_nums = native_constraints.exclude_doc_nums,
    });
    if (bench_query_profile) index_search_ns = platform_time.monotonicNs() - index_search_start_ns;
    defer sparse_mod.SparseIndex.freeResults(alloc, raw_hits);

    const start: u32 = if (full_candidate_window) 0 else @min(paging.offset, @as(u32, @intCast(raw_hits.len)));
    const end: u32 = if (full_candidate_window) @intCast(raw_hits.len) else @min(start + paging.limit, @as(u32, @intCast(raw_hits.len)));
    const sparse_doc_nums_are_ordinals =
        !chunk_backed and
        native_constraints.positive_filter and
        native_constraints.filter_doc_nums.len > 0 and
        native_constraints.filter_doc_ids.len == 0;

    var hits = try alloc.alloc(types.SearchHit, end - start);
    var initialized: usize = 0;
    var owns_hits = true;
    errdefer {
        if (owns_hits) {
            for (hits[0..initialized]) |*hit| hit.deinit(alloc);
            alloc.free(hits);
        }
    }

    var batch_doc_ordinals: []?doc_set.DocOrdinal = &.{};
    defer if (batch_doc_ordinals.len > 0) alloc.free(batch_doc_ordinals);
    if (!chunk_backed and !sparse_doc_nums_are_ordinals) {
        if (executor.lookup_doc_ordinals) |lookup_many| {
            const selected = raw_hits[@intCast(start)..@intCast(end)];
            if (selected.len > 0) {
                const doc_ids = try alloc.alloc([]const u8, selected.len);
                defer alloc.free(doc_ids);
                for (selected, 0..) |hit, i| doc_ids[i] = hit.doc_id;
                batch_doc_ordinals = try lookup_many(executor.ctx, alloc, doc_ids, req.identity_read_generation);
                if (batch_doc_ordinals.len != selected.len) return error.InvalidDocIdentity;
            }
        }
    }

    const hit_build_start_ns = if (bench_query_profile) platform_time.monotonicNs() else 0;
    for (raw_hits[@intCast(start)..@intCast(end)], 0..) |hit, i| {
        hits[i] = .{
            .id = try alloc.dupe(u8, hit.doc_id),
            .doc_ordinal = if (chunk_backed)
                try sparseHitParentOrdinal(alloc, executor, hit.doc_id, req.identity_read_generation)
            else if (sparse_doc_nums_are_ordinals and hit.doc_num != null)
                hit.doc_num.?
            else if (batch_doc_ordinals.len > 0)
                batch_doc_ordinals[i]
            else
                try sparseHitOrdinal(alloc, executor, hit.doc_id, req.identity_read_generation),
            .score = hit.score,
            .stored_data = null,
        };
        initialized += 1;
    }
    if (bench_query_profile) hit_build_ns = platform_time.monotonicNs() - hit_build_start_ns;

    owns_hits = false;
    const postprocess_start_ns = if (bench_query_profile) platform_time.monotonicNs() else 0;
    var result = try executor.postprocess(executor.ctx, alloc, postprocess_req, .{
        .alloc = alloc,
        .hits = hits,
        .total_hits = @intCast(raw_hits.len),
        .graph_results = &.{},
    }, chunk_backed);
    if (bench_query_profile) postprocess_ns = platform_time.monotonicNs() - postprocess_start_ns;
    errdefer result.deinit();
    if (unresolved_stored_filters) {
        const page_start_ns = if (bench_query_profile) platform_time.monotonicNs() else 0;
        result = try pageSearchResultInPlace(alloc, result, paging);
        if (bench_query_profile) page_ns = platform_time.monotonicNs() - page_start_ns;
    }
    if (postprocess_req.include_stored and !(chunk_backed and group_chunk_parents)) {
        const load_start_ns = if (bench_query_profile) platform_time.monotonicNs() else 0;
        try loadMissingProjectedSparseHitDocuments(alloc, postprocess_req, executor, result.hits);
        if (bench_query_profile) hit_build_ns += platform_time.monotonicNs() - load_start_ns;
    }
    if (bench_query_profile) {
        std.log.info(
            "antfly_bench_sparse_query total_us={d} constraint_us={d} index_search_us={d} hit_build_us={d} postprocess_us={d} page_us={d} raw_hits={d} returned_hits={d} filter_doc_nums={d} exclude_doc_nums={d}",
            .{
                nsToUs(platform_time.monotonicNs() - total_start_ns),
                nsToUs(constraint_ns),
                nsToUs(index_search_ns),
                nsToUs(hit_build_ns),
                nsToUs(postprocess_ns),
                nsToUs(page_ns),
                raw_hits.len,
                result.hits.len,
                native_constraints.filter_doc_nums.len,
                native_constraints.exclude_doc_nums.len,
            },
        );
    }
    return result;
}

fn loadMissingProjectedSparseHitDocuments(
    alloc: Allocator,
    req: types.SearchRequest,
    executor: SparseSearchExecutor,
    hits: []types.SearchHit,
) !void {
    var missing_count: usize = 0;
    for (hits) |hit| {
        if (hit.stored_data == null) missing_count += 1;
    }
    if (missing_count == 0) return;

    if (executor.load_projected_documents) |load_many| {
        const keys = try alloc.alloc([]const u8, missing_count);
        defer alloc.free(keys);
        var key_count: usize = 0;
        for (hits) |hit| {
            if (hit.stored_data != null) continue;
            keys[key_count] = hit.id;
            key_count += 1;
        }

        var loaded = try load_many(executor.ctx, alloc, req, keys);
        defer freeOptionalOwnedBytes(alloc, loaded);
        if (loaded.len != keys.len) return error.InvalidSearchResult;

        var loaded_index: usize = 0;
        for (hits) |*hit| {
            if (hit.stored_data != null) continue;
            const stored = loaded[loaded_index] orelse return error.StoredDocMissing;
            hit.stored_data = stored;
            loaded[loaded_index] = null;
            loaded_index += 1;
        }
        return;
    }

    for (hits) |*hit| {
        if (hit.stored_data != null) continue;
        hit.stored_data = try executor.load_projected_document(executor.ctx, alloc, req, hit.id);
    }
}

fn freeOptionalOwnedBytes(alloc: Allocator, values: []?[]u8) void {
    for (values) |value| {
        if (value) |bytes| alloc.free(bytes);
    }
    alloc.free(values);
}

const ProjectedSourceLoadProfile = struct {
    requested_count: usize = 0,
    loaded_count: usize = 0,
    batch_count: usize = 0,
    total_ns: u64 = 0,
};

fn logBenchProjectedSourceLoadProfile(
    req: types.SearchRequest,
    plan: SortExecutionPlan,
    source: []const u8,
    profile: ProjectedSourceLoadProfile,
) void {
    if (!shouldLogBenchQueryProfile()) return;
    std.log.info(
        "antfly_bench_projected_source_load source={s} total_us={d} requested={d} loaded={d} batches={d} order_fields={d} cursor={s} plan={s} source_load={s}",
        .{
            source,
            nsToUs(profile.total_ns),
            profile.requested_count,
            profile.loaded_count,
            profile.batch_count,
            req.order_by.len,
            sortCursorMode(req),
            sortExecutionPlanKindName(plan.kind),
            sortPlanSourceLoadName(sortExecutionPlanSourceLoadForRequest(plan, req)),
        },
    );
}

fn loadMissingProjectedMatchAllHitDocuments(
    alloc: Allocator,
    req: types.SearchRequest,
    executor: MatchAllExecutor,
    hits: []types.SearchHit,
) !ProjectedSourceLoadProfile {
    const start_ns = platform_time.monotonicNs();
    var profile = ProjectedSourceLoadProfile{};
    errdefer profile.total_ns = platform_time.monotonicNs() - start_ns;

    var missing_count: usize = 0;
    for (hits) |hit| {
        if (hit.stored_data == null) missing_count += 1;
    }
    profile.requested_count = missing_count;
    if (missing_count == 0) {
        profile.total_ns = platform_time.monotonicNs() - start_ns;
        return profile;
    }

    if (executor.load_projected_documents) |load_many| {
        const keys = try alloc.alloc([]const u8, missing_count);
        defer alloc.free(keys);
        var key_count: usize = 0;
        for (hits) |hit| {
            if (hit.stored_data != null) continue;
            keys[key_count] = hit.id;
            key_count += 1;
        }

        var loaded = try load_many(executor.ctx, alloc, req, keys);
        profile.batch_count += 1;
        defer freeOptionalOwnedBytes(alloc, loaded);
        if (loaded.len != keys.len) return error.InvalidSearchResult;

        var loaded_index: usize = 0;
        for (hits) |*hit| {
            if (hit.stored_data != null) continue;
            const stored = loaded[loaded_index] orelse return error.StoredDocMissing;
            hit.stored_data = stored;
            loaded[loaded_index] = null;
            profile.loaded_count += 1;
            loaded_index += 1;
        }
        profile.total_ns = platform_time.monotonicNs() - start_ns;
        return profile;
    }

    for (hits) |*hit| {
        if (hit.stored_data != null) continue;
        hit.stored_data = try executor.load_projected_document(executor.ctx, alloc, req, hit.id);
        profile.loaded_count += 1;
        profile.batch_count += 1;
    }
    profile.total_ns = platform_time.monotonicNs() - start_ns;
    return profile;
}

fn loadMissingProjectedTextHitDocuments(
    alloc: Allocator,
    req: types.SearchRequest,
    executor: SearchTextQueryExecutor,
    hits: []types.SearchHit,
) !ProjectedSourceLoadProfile {
    const start_ns = platform_time.monotonicNs();
    var profile = ProjectedSourceLoadProfile{};
    for (hits) |*hit| {
        if (hit.stored_data != null) continue;
        profile.requested_count += 1;
        const stored = (try executor.load_stored(executor.ctx, alloc, hit.id)) orelse return error.StoredDocMissing;
        defer alloc.free(stored);
        hit.stored_data = try executor.project_stored_search(executor.ctx, alloc, req, hit.id, stored);
        profile.loaded_count += 1;
        profile.batch_count += 1;
    }
    profile.total_ns = platform_time.monotonicNs() - start_ns;
    return profile;
}

fn requestNeedsNativeSortValues(req: types.SearchRequest) bool {
    for (req.order_by) |field| {
        if (sortFieldNeedsNativeValue(field)) return true;
    }
    return false;
}

fn requestHasScoreSort(req: types.SearchRequest) bool {
    for (req.order_by) |field| {
        if (sortFieldIsScore(field)) return true;
    }
    return false;
}

fn typedDocValuesTypeMatchesMappedSortField(
    value_type: typed_dv.ValueType,
    mapping: runtime_schema_mod.FieldMapping,
) bool {
    return switch (mapping.field_type) {
        .keyword, .link => value_type == .bytes_val,
        .numeric => value_type == .u64_val or value_type == .f64_val,
        .boolean => value_type == .bool_val,
        .datetime => value_type == .u64_val,
        else => false,
    };
}

const TypedDocValuesCoverageStatus = enum {
    covered,
    missing_doc_values_section,
    doc_values_kind_mismatch,
    sparse_live_doc_values,
    invalid_doc_value_doc_id,
    duplicate_doc_value_doc_id,
};

fn typedDocValuesCoverageStatusName(status: TypedDocValuesCoverageStatus) []const u8 {
    return switch (status) {
        .covered => "covered",
        .missing_doc_values_section => "missing_doc_values_section",
        .doc_values_kind_mismatch => "doc_values_kind_mismatch",
        .sparse_live_doc_values => "sparse_live_doc_values",
        .invalid_doc_value_doc_id => "invalid_doc_value_doc_id",
        .duplicate_doc_value_doc_id => "duplicate_doc_value_doc_id",
    };
}

test "typed doc values coverage status names are stable for diagnostics" {
    try std.testing.expectEqualStrings("covered", typedDocValuesCoverageStatusName(.covered));
    try std.testing.expectEqualStrings("missing_doc_values_section", typedDocValuesCoverageStatusName(.missing_doc_values_section));
    try std.testing.expectEqualStrings("doc_values_kind_mismatch", typedDocValuesCoverageStatusName(.doc_values_kind_mismatch));
    try std.testing.expectEqualStrings("sparse_live_doc_values", typedDocValuesCoverageStatusName(.sparse_live_doc_values));
    try std.testing.expectEqualStrings("invalid_doc_value_doc_id", typedDocValuesCoverageStatusName(.invalid_doc_value_doc_id));
    try std.testing.expectEqualStrings("duplicate_doc_value_doc_id", typedDocValuesCoverageStatusName(.duplicate_doc_value_doc_id));
}

fn logNativeSortPlanRejection(field: []const u8, reason: []const u8) void {
    if (!shouldLogBenchQueryProfile()) return;
    std.log.info(
        "antfly_bench_sort_plan_reject field={s} reason={s}",
        .{ field, reason },
    );
}

fn snapshotTypedDocValuesCoverageForMapping(
    snapshot: *const index_mod.IndexSnapshot,
    field: []const u8,
    mapping: runtime_schema_mod.FieldMapping,
) !TypedDocValuesCoverageStatus {
    for (snapshot.segments) |*segment| {
        if (segment.liveDocCount() == 0) continue;
        const section_data = segment.reader.getSection(field, .typed_doc_values) orelse return .missing_doc_values_section;
        const reader = try typed_dv.TypedDocValuesReader.init(snapshot.alloc, section_data);
        if (!typedDocValuesTypeMatchesMappedSortField(reader.value_type, mapping)) return .doc_values_kind_mismatch;
        const live_status = try typedDocValuesCoverLiveDocsAlloc(snapshot.alloc, segment, &reader);
        if (live_status != .covered) return live_status;
    }
    return .covered;
}

fn typedDocValuesCoverLiveDocsAlloc(
    alloc: Allocator,
    segment: *const index_mod.SegmentEntry,
    reader: *const typed_dv.TypedDocValuesReader,
) !TypedDocValuesCoverageStatus {
    const doc_count = segment.reader.doc_count;
    if (doc_count == 0) return .covered;

    var present = try std.DynamicBitSetUnmanaged.initEmpty(alloc, doc_count);
    defer present.deinit(alloc);

    for (0..reader.num_chunks) |chunk_idx| {
        const doc_ids = try reader.readChunkDocIds(@intCast(chunk_idx));
        defer alloc.free(doc_ids);
        for (doc_ids) |doc_id| {
            if (doc_id >= doc_count) return .invalid_doc_value_doc_id;
            if (present.isSet(doc_id)) return .duplicate_doc_value_doc_id;
            present.set(doc_id);
        }
    }

    for (0..doc_count) |local_doc_id| {
        const local_doc_u32: u32 = @intCast(local_doc_id);
        if (segment.shared.deleted) |deleted| {
            if (deleted.contains(local_doc_u32)) continue;
        }
        if (!present.isSet(local_doc_id)) return .sparse_live_doc_values;
    }
    return .covered;
}

fn sortFieldMapping(schema: runtime_schema_mod.TableSchema, field: []const u8) ?runtime_schema_mod.FieldMapping {
    return runtime_schema_mod.resolveFieldType(schema, field);
}

fn sortFieldTypeIsScalar(field_type: runtime_schema_mod.AntflyType) bool {
    return switch (field_type) {
        .keyword, .numeric, .boolean, .datetime, .link => true,
        else => false,
    };
}

const NativeSortPlanRejectionReason = enum {
    missing_runtime_mapping,
    unmapped_field,
    non_scalar_field,
    non_sortable_field,
    missing_doc_values_capability,
    invalid_cursor_type,
};

fn nativeSortPlanRejectionReasonName(reason: NativeSortPlanRejectionReason) []const u8 {
    return switch (reason) {
        .missing_runtime_mapping => "missing_runtime_mapping",
        .unmapped_field => "unmapped_field",
        .non_scalar_field => "non_scalar_field",
        .non_sortable_field => "non_sortable_field",
        .missing_doc_values_capability => "missing_doc_values_capability",
        .invalid_cursor_type => "invalid_cursor_type",
    };
}

test "native sort plan rejection reason names are stable for diagnostics" {
    try std.testing.expectEqualStrings("missing_runtime_mapping", nativeSortPlanRejectionReasonName(.missing_runtime_mapping));
    try std.testing.expectEqualStrings("unmapped_field", nativeSortPlanRejectionReasonName(.unmapped_field));
    try std.testing.expectEqualStrings("non_scalar_field", nativeSortPlanRejectionReasonName(.non_scalar_field));
    try std.testing.expectEqualStrings("non_sortable_field", nativeSortPlanRejectionReasonName(.non_sortable_field));
    try std.testing.expectEqualStrings("missing_doc_values_capability", nativeSortPlanRejectionReasonName(.missing_doc_values_capability));
    try std.testing.expectEqualStrings("invalid_cursor_type", nativeSortPlanRejectionReasonName(.invalid_cursor_type));
}

fn mappedSortFieldRejectionReason(mapping: runtime_schema_mod.FieldMapping) ?NativeSortPlanRejectionReason {
    if (!sortFieldTypeIsScalar(mapping.field_type)) return .non_scalar_field;
    if (!mapping.sortable) return .non_sortable_field;
    if (!mapping.doc_values) return .missing_doc_values_capability;
    return null;
}

fn jsonValueIsNumeric(value: std.json.Value) bool {
    return switch (value) {
        .integer, .float, .number_string => true,
        else => false,
    };
}

fn mappedSortCursorValueIsValid(mapping: runtime_schema_mod.FieldMapping, value: std.json.Value) bool {
    if (value == .null) return true;
    return switch (mapping.field_type) {
        .keyword, .link => value == .string,
        .numeric => jsonValueIsNumeric(value),
        .datetime => jsonValueIsNumeric(value) or (value == .string and runtime_schema_mod.parseDateTimeToNs(value.string) != null),
        .boolean => value == .bool,
        else => false,
    };
}

fn sortFieldMatchesIndexSortField(requested: types.SortField, configured: runtime_schema_mod.IndexSortField) bool {
    return requested.desc == configured.desc and std.mem.eql(u8, requested.field, configured.field);
}

fn sortRequestMatchesIndexSort(req: types.SearchRequest, schema: runtime_schema_mod.TableSchema) bool {
    if (schema.index_sort.len == 0 or req.order_by.len == 0) return false;
    const field_count = effectiveSortFieldCount(req.order_by);
    if (field_count != schema.index_sort.len) return false;
    for (schema.index_sort, 0..) |configured, i| {
        const requested = effectiveSortFieldAt(req.order_by, i);
        if (!sortFieldMatchesIndexSortField(requested, configured)) return false;
    }
    return true;
}

fn planTextNativeSortFields(
    req: types.SearchRequest,
    snapshot: *const index_mod.IndexSnapshot,
    runtime_schema: ?runtime_schema_mod.TableSchema,
) !SortExecutionPlan {
    try validateSortPageOptions(req);
    if (req.order_by.len == 0) return .{ .kind = .none };
    const cursor = activeSortCursor(req);
    if (!requestNeedsNativeSortValues(req)) return .{
        .kind = if (requestHasScoreSort(req)) .score_top_k else .id_only,
        .source = .candidate_collector,
        .cursor_support = .comparator,
    };
    const schema = runtime_schema orelse {
        logNativeSortPlanRejection("*", nativeSortPlanRejectionReasonName(.missing_runtime_mapping));
        return error.UnsupportedQueryRequest;
    };
    for (req.order_by, 0..) |field, i| {
        if (std.mem.eql(u8, field.field, "_id") or std.mem.eql(u8, field.field, "_score")) continue;
        const mapping = sortFieldMapping(schema, field.field) orelse {
            logNativeSortPlanRejection(field.field, nativeSortPlanRejectionReasonName(.unmapped_field));
            return error.UnsupportedQueryRequest;
        };
        if (mappedSortFieldRejectionReason(mapping)) |reason| {
            logNativeSortPlanRejection(field.field, nativeSortPlanRejectionReasonName(reason));
            return error.UnsupportedQueryRequest;
        }
        if (cursor.len > 0 and !mappedSortCursorValueIsValid(mapping, cursor[i])) {
            logNativeSortPlanRejection(field.field, nativeSortPlanRejectionReasonName(.invalid_cursor_type));
            return error.InvalidQueryRequest;
        }
        if (snapshot.global_doc_count == 0) continue;
        const coverage_status = try snapshotTypedDocValuesCoverageForMapping(snapshot, field.field, mapping);
        if (coverage_status != .covered) {
            logNativeSortPlanRejection(field.field, typedDocValuesCoverageStatusName(coverage_status));
            return error.UnsupportedQueryRequest;
        }
    }
    const exact_index_sort_match = sortRequestMatchesIndexSort(req, schema);
    return .{
        .kind = .native_doc_values_top_n,
        .require_native = true,
        .exactness = .exact,
        .source = .doc_values_collector,
        .cursor_support = .comparator,
        .source_load = .projected_source_after_page,
        .distributed_behavior = .shard_local_only,
        .runtime_schema = runtime_schema,
        .index_sort_match = exact_index_sort_match,
        .sorted_segment_executor_available = false,
    };
}

fn validateTextNativeSortFields(
    req: types.SearchRequest,
    snapshot: *const index_mod.IndexSnapshot,
    runtime_schema: ?runtime_schema_mod.TableSchema,
) !void {
    _ = try planTextNativeSortFields(req, snapshot, runtime_schema);
}

fn buildOrdinalTextDocIdMapAlloc(
    alloc: Allocator,
    snapshot: *const index_mod.IndexSnapshot,
    candidates: []const MatchAllCandidate,
    ordinal_to_text_doc_id: *std.AutoHashMapUnmanaged(doc_set.DocOrdinal, u32),
) !bool {
    if (!snapshot.hasDocOrdinalCoverage()) return false;

    var ordinals = std.ArrayListUnmanaged(doc_set.DocOrdinal).empty;
    defer ordinals.deinit(alloc);
    for (candidates) |candidate| {
        const ordinal = candidate.ordinal orelse continue;
        try ordinals.append(alloc, ordinal);
    }
    if (ordinals.items.len == 0) return false;

    const doc_nums = try snapshot.docNumsForOrdinalsAlloc(alloc, ordinals.items);
    defer alloc.free(doc_nums);
    for (doc_nums) |doc_num| {
        const ordinal = (try snapshot.docOrdinal(doc_num)) orelse continue;
        try ordinal_to_text_doc_id.put(alloc, ordinal, doc_num);
    }
    return ordinal_to_text_doc_id.count() > 0;
}

fn planMatchAllSortBeforeCandidatesAlloc(
    alloc: Allocator,
    req: types.SearchRequest,
    executor: MatchAllExecutor,
) !SortExecutionPlan {
    var effective = try effectiveSortRequestAlloc(alloc, req);
    defer effective.deinit(alloc);
    const effective_req = effective.req;

    try validateSortPageOptions(effective_req);
    if (effective_req.order_by.len == 0) return .{ .kind = .none };
    if (!requestNeedsNativeSortValues(effective_req)) return .{
        .kind = if (requestHasScoreSort(effective_req)) .score_top_k else .id_seek,
    };

    const text_entry = (try executor.text_index_entry(executor.ctx, effective_req.index_name)) orelse {
        return error.UnsupportedQueryRequest;
    };
    return try planTextNativeSortFields(effective_req, text_entry.persistent.snapshot(), text_entry.runtime_schema);
}

fn buildMatchAllNativeSortContextAlloc(
    alloc: Allocator,
    req: types.SearchRequest,
    executor: MatchAllExecutor,
    candidates: []const MatchAllCandidate,
    ordinal_to_text_doc_id: *std.AutoHashMapUnmanaged(doc_set.DocOrdinal, u32),
) !?TextDocValueSortPlanContext {
    if (req.order_by.len == 0 or !requestNeedsNativeSortValues(req)) return null;
    const text_entry = (try executor.text_index_entry(executor.ctx, req.index_name)) orelse return null;
    const snapshot = text_entry.persistent.snapshot();
    const plan = try planTextNativeSortFields(req, snapshot, text_entry.runtime_schema);
    if (plan.kind != .native_doc_values_top_n) return null;
    if (candidates.len > 0 and !(try buildOrdinalTextDocIdMapAlloc(alloc, snapshot, candidates, ordinal_to_text_doc_id))) {
        return error.UnsupportedQueryRequest;
    }
    return .{
        .plan = plan,
        .ctx = .{
            .snapshot = snapshot,
            .ordinal_to_text_doc_id = ordinal_to_text_doc_id,
        },
    };
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
    try checkSearchRequestDeadline(req);
    const planned_sort = if (requestHasSortPageOptions(req))
        try planMatchAllSortBeforeCandidatesAlloc(alloc, req, executor)
    else
        SortExecutionPlan{ .kind = .none };

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

    const exact_sort_budget = lateVisibilityExactCandidateBudget();
    const needs_stored_pattern_filter =
        req.filter_query_json.len > 0 or
        req.exclusion_query_json.len > 0 or
        req.resolved_doc_filter != null;

    if (req.order_by.len > 0 and planned_sort.kind == .id_seek and !needs_stored_pattern_filter and req.search_before.len == 0 and executor.collect_candidates_stream != null) {
        var out = try sortAndPageMatchAllIdSeekAlloc(alloc, req, executor, &native_constraints);
        errdefer out.deinit();
        if (req.include_stored) {
            const source_profile = try loadMissingProjectedMatchAllHitDocuments(alloc, req, executor, out.hits);
            logBenchProjectedSourceLoadProfile(req, planned_sort, "match_all", source_profile);
        }
        return out;
    }

    if (req.order_by.len > 0 and planned_sort.kind == .id_seek and !needs_stored_pattern_filter and executor.collect_candidates_stream != null) {
        var out = try sortAndPageMatchAllCandidateStreamAlloc(alloc, req, executor, .{
            .candidate_limit = exact_sort_budget,
            .constraints = &native_constraints,
            .primary_key_stop_before = idOnlySearchBeforeCursor(req),
        }, planned_sort, null);
        errdefer out.deinit();
        if (req.include_stored) {
            const source_profile = try loadMissingProjectedMatchAllHitDocuments(alloc, req, executor, out.hits);
            logBenchProjectedSourceLoadProfile(req, planned_sort, "match_all", source_profile);
        }
        return out;
    }

    var candidates = executor.collect_candidates(executor.ctx, alloc, req, if (req.order_by.len > 0) .{
        .candidate_limit = exact_sort_budget,
        .constraints = &native_constraints,
    } else .{}) catch |err| {
        if (req.order_by.len > 0 and err == error.QueryCandidateBudgetExceeded) {
            logExactSortBudgetRejection(
                "match_all",
                .match_all_candidate_collect_limit,
                req.index_name,
                exact_sort_budget +| 1,
                exact_sort_budget,
                planned_sort,
            );
        }
        return err;
    };
    defer candidates.deinit(alloc);
    try applyMatchAllDocIdConstraintsAlloc(alloc, &candidates, &native_constraints);
    try checkSearchRequestDeadline(req);
    if (req.order_by.len > 0) {
        const candidate_count: u32 = @intCast(@min(candidates.items.len, @as(usize, std.math.maxInt(u32))));
        enforceLateVisibilityExactCandidateBudget(candidate_count, exact_sort_budget) catch |err| {
            logExactSortBudgetRejection(
                "match_all",
                .match_all_exact_candidate_window,
                req.index_name,
                candidate_count,
                exact_sort_budget,
                planned_sort,
            );
            return err;
        };
    }

    var ordinal_to_text_doc_id = std.AutoHashMapUnmanaged(doc_set.DocOrdinal, u32).empty;
    defer ordinal_to_text_doc_id.deinit(alloc);
    var native_sort_ctx: TextDocValueSortContext = undefined;
    var native_sort_loader: ?NativeSortValueLoader = null;
    var sort_plan = planned_sort;
    if (try buildMatchAllNativeSortContextAlloc(alloc, req, executor, candidates.items, &ordinal_to_text_doc_id)) |planned| {
        native_sort_ctx = planned.ctx;
        sort_plan = planned.plan;
        native_sort_loader = .{
            .ctx = &native_sort_ctx,
            .require_native = planned.plan.require_native,
            .load = loadTextDocValueSortValue,
        };
    }
    if (sort_plan.kind == .stored_json_debug and candidates.items.len > 0) {
        return error.UnsupportedQueryRequest;
    }

    if (needs_stored_pattern_filter) {
        var hits = try alloc.alloc(types.SearchHit, candidates.items.len);
        var initialized: usize = 0;
        errdefer {
            for (hits[0..initialized]) |*hit| hit.deinit(alloc);
            if (hits.len > 0) alloc.free(hits);
        }
        for (candidates.items, 0..) |*candidate, i| {
            if (i % 1024 == 0) try checkSearchRequestDeadline(req);
            hits[i] = .{
                .id = candidate.id,
                .doc_ordinal = candidate.ordinal,
                .score = 1.0,
                .stored_data = null,
            };
            candidate.id = @constCast(&[_]u8{});
            initialized += 1;
        }

        var filtered = try result_shape.applyStoredSearchPatternFilters(alloc, req, .{
            .alloc = alloc,
            .hits = hits,
            .total_hits = @intCast(hits.len),
            .graph_results = &.{},
        }, .{
            .ctx = executor.ctx,
            .load_stored = executor.load_stored,
            .load_many_stored = executor.load_many_stored,
            .resolve_doc_set_doc_ids = executor.resolve_doc_set_doc_ids,
            .resolve_doc_ids_to_doc_set = executor.resolve_doc_ids_to_doc_set,
        });
        var filtered_owned = true;
        errdefer if (filtered_owned) filtered.deinit();

        if (req.order_by.len > 0) {
            try sortAndPageSearchResultInPlace(&filtered, req, executor.ctx, executor.load_stored, sort_plan, native_sort_loader);
            filtered_owned = false;
            errdefer filtered.deinit();
            if (req.include_stored) {
                const source_profile = try loadMissingProjectedMatchAllHitDocuments(alloc, req, executor, filtered.hits);
                logBenchProjectedSourceLoadProfile(req, sort_plan, "match_all", source_profile);
            }
            return filtered;
        }

        var paged = try pageSearchResultInPlace(alloc, filtered, componentPaging(req));
        filtered_owned = false;
        errdefer paged.deinit();
        if (req.include_stored) {
            _ = try loadMissingProjectedMatchAllHitDocuments(alloc, req, executor, paged.hits);
        }
        return paged;
    }

    const paging = componentPaging(req);

    const total_hits: u32 = @intCast(candidates.items.len);
    if (req.order_by.len > 0) {
        var out = try sortAndPageMatchAllCandidatesAlloc(
            alloc,
            req,
            &candidates,
            executor.ctx,
            executor.load_stored,
            sort_plan,
            native_sort_loader,
        );
        errdefer out.deinit();
        if (req.include_stored) {
            const source_profile = try loadMissingProjectedMatchAllHitDocuments(alloc, req, executor, out.hits);
            logBenchProjectedSourceLoadProfile(req, sort_plan, "match_all", source_profile);
        }
        return out;
    }

    const start = if (req.order_by.len > 0) 0 else @min(paging.offset, total_hits);
    const end = if (req.order_by.len > 0) total_hits else @min(start + paging.limit, total_hits);
    const start_usize: usize = @intCast(start);
    const end_usize: usize = @intCast(end);

    var hits = try alloc.alloc(types.SearchHit, end_usize - start_usize);
    errdefer alloc.free(hits);

    for (candidates.items, 0..) |*candidate, i| {
        if (i % 1024 == 0) try checkSearchRequestDeadline(req);
        if (i < start_usize or i >= end_usize) continue;
        hits[i - start_usize] = .{
            .id = candidate.id,
            .doc_ordinal = candidate.ordinal,
            .score = 1.0,
            .stored_data = if (req.include_stored) try executor.load_projected_document(executor.ctx, alloc, req, candidate.id) else null,
        };
        candidate.id = @constCast(&[_]u8{});
    }

    var out = types.SearchResult{
        .alloc = alloc,
        .hits = hits,
        .total_hits = total_hits,
        .graph_results = &.{},
    };
    errdefer out.deinit();
    return out;
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
    return try collectMatchAllCandidatesWithOptions(alloc, req, collector, .{});
}

pub fn collectMatchAllCandidatesWithOptions(
    alloc: Allocator,
    req: types.SearchRequest,
    collector: MatchAllCandidateCollector,
    options: MatchAllCandidateCollectOptions,
) !MatchAllCandidates {
    var state = try collectMatchAllCandidateStateWithOptions(alloc, req, collector, options, null, null);
    defer state.seen.deinit(alloc);
    defer state.deinitSeenKeyStorage();
    errdefer state.deinitCandidates();
    return .{ .items = try state.candidates.toOwnedSlice(alloc) };
}

pub fn streamMatchAllCandidatesWithOptions(
    alloc: Allocator,
    req: types.SearchRequest,
    collector: MatchAllCandidateCollector,
    options: MatchAllCandidateCollectOptions,
    consumer_ctx: ?*anyopaque,
    consumer: MatchAllCandidateConsumer,
) !MatchAllCandidateStreamStats {
    var state = try collectMatchAllCandidateStateWithOptions(alloc, req, collector, options, consumer_ctx, consumer);
    defer state.seen.deinit(alloc);
    defer state.deinitSeenKeyStorage();
    state.deinitCandidates();
    return .{
        .accepted_count = state.accepted_count,
        .stopped_early = state.stopped_early,
    };
}

fn collectMatchAllCandidateStateWithOptions(
    alloc: Allocator,
    req: types.SearchRequest,
    collector: MatchAllCandidateCollector,
    options: MatchAllCandidateCollectOptions,
    consumer_ctx: ?*anyopaque,
    consumer: ?MatchAllCandidateConsumer,
) !MatchAllCandidateCollectState {
    _ = req.index_name;

    const lower = if (options.primary_key_start_after) |start_after|
        (try internal_keys.documentRangeUpperAlloc(alloc, start_after)) orelse return error.InvalidInternalUserKey
    else
        try internal_keys.documentRangeLowerAlloc(alloc, "");
    defer alloc.free(lower);
    const upper = if (options.primary_key_stop_before) |stop_before|
        try internal_keys.documentRangeLowerAlloc(alloc, stop_before)
    else
        null;
    defer if (upper) |buf| alloc.free(buf);

    var state = MatchAllCandidateCollectState{
        .alloc = alloc,
        .req = req,
        .collector = collector,
        .options = options,
        .consumer_ctx = consumer_ctx,
        .consumer = consumer,
    };
    errdefer {
        state.seen.deinit(alloc);
        state.deinitSeenKeyStorage();
        state.deinitCandidates();
    }

    if (collector.scan_store_range_with_context) |scan| {
        var current_lower = try alloc.dupe(u8, lower);
        defer alloc.free(current_lower);
        const batch_size = @max(options.scan_batch_size orelse default_match_all_primary_key_scan_batch_size, 1);

        while (true) {
            var batch = MatchAllPrimaryKeyScanBatch{
                .alloc = alloc,
                .req = req,
                .max_keys = batch_size,
            };
            errdefer batch.deinit();

            try scan(collector.ctx, current_lower, if (upper) |buf| buf else "", &batch, MatchAllPrimaryKeyScanBatch.scanEntry);
            for (batch.raw_keys.items) |*raw_key| {
                const owned = raw_key.*;
                raw_key.* = @constCast(&[_]u8{});
                try state.consumeRawKey(owned);
                if (state.shouldStopAfterAccepted()) {
                    state.stopped_early = true;
                    break;
                }
            }

            const maybe_next_lower = batch.next_lower;
            batch.next_lower = null;
            batch.deinit();
            if (state.stopped_early) break;
            const next_lower = maybe_next_lower orelse break;
            alloc.free(current_lower);
            current_lower = next_lower;
        }
    } else {
        const docs = try collector.scan_store_range(collector.ctx, alloc, lower, if (upper) |buf| buf else "");
        defer docstore_mod.DocStore.freeResults(alloc, docs);
        for (docs) |doc| {
            try state.consumeStoreKey(doc.key);
            if (state.shouldStopAfterAccepted()) {
                state.stopped_early = true;
                break;
            }
        }
    }

    return state;
}

const MatchAllPrimaryKeyScanBatch = struct {
    alloc: Allocator,
    req: types.SearchRequest,
    max_keys: usize,
    raw_keys: std.ArrayListUnmanaged([]u8) = .empty,
    next_lower: ?[]u8 = null,
    scanned: usize = 0,

    fn deinit(self: *@This()) void {
        for (self.raw_keys.items) |key| {
            if (key.len > 0) self.alloc.free(key);
        }
        self.raw_keys.deinit(self.alloc);
        if (self.next_lower) |next| self.alloc.free(next);
        self.* = undefined;
    }

    fn scanEntry(ctx: ?*anyopaque, key: []const u8, _: []const u8) anyerror!docstore_mod.DocStore.ScanAction {
        const self: *@This() = @ptrCast(@alignCast(ctx orelse return error.InvalidArgument));
        self.scanned += 1;
        if (self.scanned % 1024 == 0) try checkSearchRequestDeadline(self.req);

        if (!internal_keys.isPrimaryDocumentKey(key)) return .@"continue";
        var raw_key = (try internal_keys.decodePrimaryDocumentKeyAlloc(self.alloc, key)) orelse return .@"continue";
        errdefer self.alloc.free(raw_key);

        try self.raw_keys.append(self.alloc, raw_key);
        raw_key = @constCast(&[_]u8{});

        if (self.raw_keys.items.len >= self.max_keys) {
            self.next_lower = try exclusiveStoreScanResumeLowerAlloc(self.alloc, key);
            return .stop;
        }
        return .@"continue";
    }
};

fn exclusiveStoreScanResumeLowerAlloc(alloc: Allocator, key: []const u8) ![]u8 {
    const out = try alloc.alloc(u8, key.len + 1);
    @memcpy(out[0..key.len], key);
    out[key.len] = 0;
    return out;
}

const MatchAllCandidateCollectState = struct {
    alloc: Allocator,
    req: types.SearchRequest,
    collector: MatchAllCandidateCollector,
    options: MatchAllCandidateCollectOptions,
    candidates: std.ArrayListUnmanaged(MatchAllCandidate) = .empty,
    seen: std.StringHashMapUnmanaged(void) = .{},
    seen_key_storage: std.ArrayListUnmanaged([]u8) = .empty,
    consumer_ctx: ?*anyopaque = null,
    consumer: ?MatchAllCandidateConsumer = null,
    processed: usize = 0,
    accepted_count: usize = 0,
    stopped_early: bool = false,

    fn deinitCandidates(self: *@This()) void {
        for (self.candidates.items) |*item| item.deinit(self.alloc);
        self.candidates.deinit(self.alloc);
    }

    fn deinitSeenKeyStorage(self: *@This()) void {
        for (self.seen_key_storage.items) |key| self.alloc.free(key);
        self.seen_key_storage.deinit(self.alloc);
    }

    fn shouldStopAfterAccepted(self: *const @This()) bool {
        const limit = self.options.stop_after_accepted orelse return false;
        return self.accepted_count >= limit;
    }

    fn consumeStoreKey(self: *@This(), store_key: []const u8) !void {
        self.processed += 1;
        if (self.processed % 1024 == 0) try checkSearchRequestDeadline(self.req);

        if (!internal_keys.isPrimaryDocumentKey(store_key)) return;
        var raw_key = (try internal_keys.decodePrimaryDocumentKeyAlloc(self.alloc, store_key)) orelse return;
        errdefer self.alloc.free(raw_key);

        const owned = raw_key;
        raw_key = @constCast(&[_]u8{});
        try self.consumeRawKey(owned);
    }

    fn consumeRawKey(self: *@This(), raw_key_owned: []u8) !void {
        var raw_key = raw_key_owned;
        errdefer self.alloc.free(raw_key);

        if (try self.collector.is_expired_key(self.collector.ctx, self.alloc, raw_key)) {
            self.alloc.free(raw_key);
            return;
        }

        if (self.seen.contains(raw_key)) {
            self.alloc.free(raw_key);
            return;
        }

        const ordinal = if (self.collector.lookup_doc_ordinal) |lookup|
            try lookup(self.collector.ctx, self.alloc, raw_key, self.req.identity_read_generation)
        else
            null;

        var candidate = MatchAllCandidate{
            .id = raw_key,
            .ordinal = ordinal,
        };
        raw_key = @constCast(&[_]u8{});

        if (self.options.constraints) |constraints| {
            if (!matchAllCandidateAllowed(candidate, constraints)) {
                candidate.deinit(self.alloc);
                return;
            }
        }

        if (self.options.candidate_limit) |limit| {
            if (self.accepted_count >= limit) {
                candidate.deinit(self.alloc);
                return error.QueryCandidateBudgetExceeded;
            }
        }

        errdefer candidate.deinit(self.alloc);
        if (self.consumer) |consumer| {
            const seen_key = try self.alloc.dupe(u8, candidate.id);
            errdefer self.alloc.free(seen_key);
            try self.seen.put(self.alloc, seen_key, {});
            try self.seen_key_storage.append(self.alloc, seen_key);

            try consumer(self.consumer_ctx, candidate);
            candidate.id = @constCast(&[_]u8{});
            self.accepted_count += 1;
        } else {
            try self.seen.put(self.alloc, candidate.id, {});
            try self.candidates.append(self.alloc, candidate);
            candidate.id = @constCast(&[_]u8{});
            self.accepted_count += 1;
        }
    }
};

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
        .multi_match_bool_prefix => |multi_match| try multiMatchBoolPrefixToSearchQuery(alloc, multi_match, text_analysis, runtime_schema),
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

const ResolvedMultiMatchField = struct {
    field: []const u8,
    boost: f32,
};

const search_as_you_type_max_shingle_size: usize = 3;

fn multiMatchBoolPrefixToSearchQuery(
    alloc: Allocator,
    multi_match: anytype,
    text_analysis: introducer_mod.TextAnalysisConfig,
    runtime_schema: ?runtime_schema_mod.TableSchema,
) !search_mod.SearchQuery {
    var fields = std.ArrayListUnmanaged(ResolvedMultiMatchField).empty;
    defer fields.deinit(alloc);
    for (multi_match.fields) |field| {
        try appendResolvedMultiMatchFields(alloc, &fields, field, text_analysis, runtime_schema);
    }

    var should = std.ArrayListUnmanaged(search_mod.SearchQuery).empty;
    errdefer should.deinit(alloc);
    for (fields.items) |field| {
        if (try fieldBoolPrefixSearchQuery(alloc, field, multi_match.query, text_analysis, runtime_schema)) |query| {
            try should.append(alloc, query);
        }
    }

    if (should.items.len == 0) return .{ .match_none = {} };
    return .{ .bool_query = .{
        .should = try should.toOwnedSlice(alloc),
        .min_should = 1,
        .boost = multi_match.boost,
    } };
}

fn appendResolvedMultiMatchFields(
    alloc: Allocator,
    fields: *std.ArrayListUnmanaged(ResolvedMultiMatchField),
    requested: types.TextMultiMatchField,
    text_analysis: introducer_mod.TextAnalysisConfig,
    runtime_schema: ?runtime_schema_mod.TableSchema,
) !void {
    try appendUniqueResolvedMultiMatchField(alloc, fields, requested.field, requested.boost);
    if (isSearchAsYouTypeGeneratedField(requested.field)) return;
    if (!try fieldHasSearchAsYouTypeSubfields(alloc, text_analysis, runtime_schema, requested.field)) return;

    const two_gram = try std.fmt.allocPrint(alloc, "{s}._2gram", .{requested.field});
    try appendUniqueResolvedMultiMatchField(alloc, fields, two_gram, requested.boost);
    const three_gram = try std.fmt.allocPrint(alloc, "{s}._3gram", .{requested.field});
    try appendUniqueResolvedMultiMatchField(alloc, fields, three_gram, requested.boost);
    const index_prefix = try std.fmt.allocPrint(alloc, "{s}._index_prefix", .{requested.field});
    try appendUniqueResolvedMultiMatchField(alloc, fields, index_prefix, requested.boost);
}

fn appendUniqueResolvedMultiMatchField(
    alloc: Allocator,
    fields: *std.ArrayListUnmanaged(ResolvedMultiMatchField),
    field: []const u8,
    boost: f32,
) !void {
    for (fields.items) |item| {
        if (std.mem.eql(u8, item.field, field)) return;
    }
    try fields.append(alloc, .{ .field = field, .boost = boost });
}

fn fieldHasSearchAsYouTypeSubfields(
    alloc: Allocator,
    text_analysis: introducer_mod.TextAnalysisConfig,
    runtime_schema: ?runtime_schema_mod.TableSchema,
    field: []const u8,
) !bool {
    const two_gram = try std.fmt.allocPrint(alloc, "{s}._2gram", .{field});
    defer alloc.free(two_gram);
    if (resolveConfiguredFieldAnalyzerName(text_analysis, two_gram)) |analyzer| {
        if (std.mem.eql(u8, analyzer, "search_as_you_type_2gram")) return true;
    }
    if (runtime_schema) |schema| {
        if (resolveIndexedFieldAnalyzer(schema, two_gram)) |analyzer| {
            if (std.mem.eql(u8, analyzer, "search_as_you_type_2gram")) return true;
        }
    }
    return false;
}

fn fieldBoolPrefixSearchQuery(
    alloc: Allocator,
    field: ResolvedMultiMatchField,
    text: []const u8,
    text_analysis: introducer_mod.TextAnalysisConfig,
    runtime_schema: ?runtime_schema_mod.TableSchema,
) !?search_mod.SearchQuery {
    if (std.mem.endsWith(u8, field.field, "._index_prefix")) {
        const tokens = try analysis_mod.simple_analyzer.analyze(alloc, text);
        defer analysis_mod.Analyzer.freeTokens(alloc, tokens);
        if (tokens.len == 0) return null;
        const start = if (tokens.len > search_as_you_type_max_shingle_size)
            tokens.len - search_as_you_type_max_shingle_size
        else
            0;
        return .{ .term = .{
            .field = field.field,
            .term = try joinTokenTermsAlloc(alloc, tokens[start..]),
            .boost = field.boost,
        } };
    }

    const analyzer = (try resolveQueryAnalyzer(field.field, null, text_analysis, runtime_schema)) orelse &analysis_mod.default_analyzer;
    const tokens = try analyzer.analyze(alloc, text);
    defer analysis_mod.Analyzer.freeTokens(alloc, tokens);
    if (tokens.len == 0) return null;

    var should = std.ArrayListUnmanaged(search_mod.SearchQuery).empty;
    errdefer should.deinit(alloc);
    if (tokens.len > 1) {
        for (tokens[0 .. tokens.len - 1]) |token| {
            try should.append(alloc, .{ .term = .{
                .field = field.field,
                .term = try alloc.dupe(u8, token.term),
            } });
        }
    }
    try should.append(alloc, .{ .prefix = .{
        .field = field.field,
        .prefix = try alloc.dupe(u8, tokens[tokens.len - 1].term),
    } });
    return .{ .bool_query = .{
        .should = try should.toOwnedSlice(alloc),
        .min_should = 1,
        .boost = field.boost,
    } };
}

fn joinTokenTermsAlloc(alloc: Allocator, tokens: []const analysis_mod.Token) ![]u8 {
    var out = std.ArrayListUnmanaged(u8).empty;
    errdefer out.deinit(alloc);
    for (tokens, 0..) |token, idx| {
        if (idx > 0) try out.append(alloc, ' ');
        try out.appendSlice(alloc, token.term);
    }
    return try out.toOwnedSlice(alloc);
}

fn isSearchAsYouTypeGeneratedField(field: []const u8) bool {
    return std.mem.endsWith(u8, field, "._2gram") or
        std.mem.endsWith(u8, field, "._3gram") or
        std.mem.endsWith(u8, field, "._index_prefix");
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
    if (std.mem.endsWith(u8, field, mapper_mod.schema_less_exact_field_suffix)) return "keyword";
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
                        .emitted_name = "title.keyword",
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
                                .suffix = "._2gram",
                                .analyzer = "search_as_you_type_2gram",
                            },
                            .{
                                .suffix = "._3gram",
                                .analyzer = "search_as_you_type_3gram",
                            },
                            .{
                                .suffix = "._index_prefix",
                                .analyzer = "search_as_you_type_index_prefix",
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
    try std.testing.expectEqualStrings("keyword", resolveIndexedFieldAnalyzer(schema, "title.keyword").?);
    try std.testing.expectEqualStrings("search_as_you_type_2gram", resolveIndexedFieldAnalyzer(schema, "meta.tag_blue.title._2gram").?);
    try std.testing.expectEqualStrings("search_as_you_type_3gram", resolveIndexedFieldAnalyzer(schema, "meta.tag_blue.title._3gram").?);
    try std.testing.expectEqualStrings("search_as_you_type_index_prefix", resolveIndexedFieldAnalyzer(schema, "meta.tag_blue.title._index_prefix").?);
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

    // Width floor scales with leaves needed for oversampled k, not with k.
    try std.testing.expectEqual(@as(u32, 4), resolveSearchWidth(10, 0.0, testIndexStats(0, 0, 128)));
    try std.testing.expectEqual(@as(u32, 16), resolveSearchWidth(500, 0.0, testIndexStats(0, 0, 128)));
    // Effort 1.0 reaches the ceiling: all leaves when stats are known,
    // node count or the legacy cap otherwise.
    try std.testing.expectEqual(@as(u32, 4096), resolveSearchWidth(10, 1.0, testIndexStats(0, 0, 128)));
    try std.testing.expectEqual(@as(u32, 17_591), resolveSearchWidth(10, 1.0, testIndexStats(0, 17_591, 128)));
    try std.testing.expectEqual(@as(u32, 7), resolveSearchWidth(10, 1.0, testIndexStats(879, 879, 128)));

    try std.testing.expectEqual(@as(u32, 391), estimateLeafCount(testIndexStats(50_000, 879, 128)));
    try std.testing.expectEqual(@as(u32, 391), resolveSearchWidth(10, 1.0, testIndexStats(50_000, 879, 128)));

    // The ramp is geometric on both sides of the balanced anchor, monotonic,
    // and the balanced effort lands at the calibrated mid fraction (~50% of
    // the leaf ceiling).
    const w03 = resolveSearchWidth(10, 0.3, testIndexStats(50_000, 879, 128));
    const w05 = resolveSearchWidth(10, default_balanced_search_effort, testIndexStats(50_000, 879, 128));
    const w07 = resolveSearchWidth(10, 0.7, testIndexStats(50_000, 879, 128));
    try std.testing.expect(4 < w03 and w03 < w05 and w05 < w07 and w07 < 391);
    try std.testing.expect(w05 >= 391 * 3 / 5 and w05 <= 391 * 4 / 5);

    // At large corpora the balanced anchor is capped instead of growing
    // linearly with the leaf count, while effort 1.0 still reaches them all.
    const w05_1m = resolveSearchWidth(10, default_balanced_search_effort, testIndexStats(1_000_000, 17_591, 128));
    try std.testing.expect(w05_1m <= 2048);
    try std.testing.expectEqual(@as(u32, 7813), resolveSearchWidth(10, 1.0, testIndexStats(1_000_000, 17_591, 128)));

    // Epsilon ramps from mild pruning toward effectively unpruned.
    try std.testing.expectApproxEqAbs(@as(f32, 0.90), resolveSearchEpsilon(0.0), 0.01);
    try std.testing.expectApproxEqAbs(@as(f32, 1.45), resolveSearchEpsilon(default_balanced_search_effort), 0.01);
    try std.testing.expectApproxEqAbs(@as(f32, 2.00), resolveSearchEpsilon(1.0), 0.01);

    try std.testing.expectEqual(@as(usize, 6), resolveRerankFactor(0.0));
    try std.testing.expectEqual(@as(usize, 9), resolveRerankFactor(default_balanced_search_effort));
    try std.testing.expectEqual(@as(usize, 12), resolveRerankFactor(1.0));
}

test "multi_match bool_prefix index prefix uses trailing shingle window" {
    var arena = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer arena.deinit();
    const alloc = arena.allocator();

    const fields = [_]types.TextMultiMatchField{
        .{ .field = "name._index_prefix" },
    };
    const query = try textQueryToSearchQuery(alloc, .{ .multi_match_bool_prefix = .{
        .query = "premium smartphone apple ip",
        .fields = &fields,
    } }, .{}, null);

    try std.testing.expect(query == .bool_query);
    try std.testing.expectEqual(@as(usize, 1), query.bool_query.should.len);
    try std.testing.expect(query.bool_query.should[0] == .term);
    try std.testing.expectEqualStrings("name._index_prefix", query.bool_query.should[0].term.field);
    try std.testing.expectEqualStrings("smartphone apple ip", query.bool_query.should[0].term.term);
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

test "dense and sparse search reject unsupported exact sort page options" {
    const alloc = std.testing.allocator;
    const order_by = [_]types.SortField{
        .{ .field = "created_at", .desc = true },
        .{ .field = "_id" },
    };
    const dense_vector = [_]f32{1.0};
    const sparse_indices = [_]u32{1};
    const sparse_values = [_]f32{1.0};

    try std.testing.expectError(error.UnsupportedQueryRequest, searchDense(alloc, .{
        .order_by = &order_by,
        .include_stored = false,
        .limit = 10,
    }, .{
        .vector = &dense_vector,
        .k = 10,
    }, testDenseConstraintExecutor()));

    try std.testing.expectError(error.UnsupportedQueryRequest, searchSparse(alloc, .{
        .search_after = &.{.{ .integer = 1 }},
        .include_stored = false,
        .limit = 10,
    }, .{
        .indices = &sparse_indices,
        .values = &sparse_values,
        .k = 10,
    }, undefined));
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
    collect_count: ?*usize = null,
    stream_collect_count: ?*usize = null,
    stream_accepted_count: ?*usize = null,
    projected_load_count: ?*usize = null,
    projected_batch_count: ?*usize = null,
    projected_batch_doc_count: ?*usize = null,
};

fn testCollectMatchAllCandidatesCallback(
    ctx: ?*anyopaque,
    alloc: Allocator,
    _: types.SearchRequest,
    options: MatchAllCandidateCollectOptions,
) anyerror!MatchAllCandidates {
    const test_ctx: *const TestMatchAllCtx = @ptrCast(@alignCast(ctx orelse return error.InvalidArgument));
    if (test_ctx.collect_count) |counter| counter.* += 1;
    var out = std.ArrayListUnmanaged(MatchAllCandidate).empty;
    var initialized: usize = 0;
    errdefer {
        for (out.items[0..initialized]) |*candidate| candidate.deinit(alloc);
        out.deinit(alloc);
    }
    for (test_ctx.ids, 0..) |id, i| {
        const ordinal = if (i < test_ctx.ordinals.len) test_ctx.ordinals[i] else null;
        var candidate = MatchAllCandidate{ .id = try alloc.dupe(u8, id), .ordinal = ordinal };
        if (options.constraints) |constraints| {
            if (!matchAllCandidateAllowed(candidate, constraints)) {
                candidate.deinit(alloc);
                continue;
            }
        }
        if (options.candidate_limit) |limit| {
            if (out.items.len >= limit) {
                candidate.deinit(alloc);
                return error.QueryCandidateBudgetExceeded;
            }
        }
        try out.append(alloc, candidate);
        candidate.id = @constCast(&[_]u8{});
        initialized += 1;
    }
    return .{ .items = try out.toOwnedSlice(alloc) };
}

fn testStreamMatchAllCandidatesCallback(
    ctx: ?*anyopaque,
    alloc: Allocator,
    req: types.SearchRequest,
    options: MatchAllCandidateCollectOptions,
    consumer_ctx: ?*anyopaque,
    consumer: MatchAllCandidateConsumer,
) anyerror!MatchAllCandidateStreamStats {
    const test_ctx: *const TestMatchAllCtx = @ptrCast(@alignCast(ctx orelse return error.InvalidArgument));
    if (test_ctx.stream_collect_count) |counter| counter.* += 1;
    var accepted: usize = 0;
    for (test_ctx.ids, 0..) |id, i| {
        if (options.primary_key_start_after) |start_after| {
            if (std.mem.order(u8, id, start_after) != .gt) continue;
        }
        if (options.primary_key_stop_before) |stop_before| {
            if (std.mem.order(u8, id, stop_before) != .lt) break;
        }
        const ordinal = if (i < test_ctx.ordinals.len) test_ctx.ordinals[i] else null;
        var candidate = MatchAllCandidate{ .id = try alloc.dupe(u8, id), .ordinal = ordinal };
        errdefer candidate.deinit(alloc);
        if (options.constraints) |constraints| {
            if (!matchAllCandidateAllowed(candidate, constraints)) {
                candidate.deinit(alloc);
                continue;
            }
        }
        if (options.candidate_limit) |limit| {
            if (accepted >= limit) return error.QueryCandidateBudgetExceeded;
        }
        try consumer(consumer_ctx, candidate);
        candidate.id = @constCast(&[_]u8{});
        accepted += 1;
        if (test_ctx.stream_accepted_count) |counter| counter.* += 1;
        if (options.stop_after_accepted) |limit| {
            if (accepted >= limit) return .{ .accepted_count = accepted, .stopped_early = true };
        }
    }
    _ = req;
    return .{ .accepted_count = accepted, .stopped_early = false };
}

fn testMatchAllLoadProjectedCallback(
    ctx: ?*anyopaque,
    alloc: Allocator,
    _: types.SearchRequest,
    key: []const u8,
) anyerror![]u8 {
    const test_ctx: *const TestMatchAllCtx = @ptrCast(@alignCast(ctx orelse return error.InvalidArgument));
    const counter = test_ctx.projected_load_count orelse return error.UnexpectedTestCall;
    counter.* += 1;
    return try std.fmt.allocPrint(alloc, "{{\"id\":\"{s}\"}}", .{key});
}

fn testMatchAllLoadProjectedManyCallback(
    ctx: ?*anyopaque,
    alloc: Allocator,
    req: types.SearchRequest,
    keys: []const []const u8,
) anyerror![]?[]u8 {
    const test_ctx: *const TestMatchAllCtx = @ptrCast(@alignCast(ctx orelse return error.InvalidArgument));
    const batch_counter = test_ctx.projected_batch_count orelse return error.UnexpectedTestCall;
    const doc_counter = test_ctx.projected_batch_doc_count orelse return error.UnexpectedTestCall;
    batch_counter.* += 1;
    doc_counter.* += keys.len;

    const out = try alloc.alloc(?[]u8, keys.len);
    var initialized: usize = 0;
    errdefer {
        for (out[0..initialized]) |value| if (value) |bytes| alloc.free(bytes);
        alloc.free(out);
    }
    _ = req;
    for (keys, 0..) |key, i| {
        out[i] = try std.fmt.allocPrint(alloc, "{{\"id\":\"{s}\"}}", .{key});
        initialized += 1;
    }
    return out;
}

fn testMatchAllLoadStoredCallback(
    _: ?*anyopaque,
    alloc: Allocator,
    key: []const u8,
) anyerror!?[]u8 {
    const json = if (std.mem.eql(u8, key, "doc:a"))
        "{\"tier\":\"gold\",\"rank\":2,\"nested\":{\"created_at\":\"2026-01-02\"}}"
    else if (std.mem.eql(u8, key, "doc:b"))
        "{\"tier\":\"silver\",\"rank\":1,\"nested\":{\"created_at\":\"2026-01-01\"}}"
    else if (std.mem.eql(u8, key, "doc:c"))
        "{\"tier\":\"gold\",\"rank\":1,\"nested\":{\"created_at\":\"2026-01-03\"}}"
    else
        return null;
    return try alloc.dupe(u8, json);
}

fn testUnexpectedLoadStoredCallback(
    _: ?*anyopaque,
    _: Allocator,
    _: []const u8,
) anyerror!?[]u8 {
    return error.UnexpectedTestCall;
}

test "sort uses native text doc values before stored json fallback" {
    const alloc = std.testing.allocator;

    var dv_writer = typed_dv.TypedDocValuesWriter.init(alloc, .f64_val, 1024);
    defer dv_writer.deinit();
    try dv_writer.add(0, .{ .f64_val = 30.0 });
    try dv_writer.add(1, .{ .f64_val = 10.0 });
    try dv_writer.add(2, .{ .f64_val = 20.0 });
    const dv_data = try dv_writer.build();
    defer alloc.free(dv_data);

    var seg_writer = segment_mod.SegmentWriter.init(alloc);
    defer seg_writer.deinit();
    const price_idx = try seg_writer.addField("price");
    try seg_writer.addSection(price_idx, .typed_doc_values, dv_data);
    try seg_writer.addStoredDoc("doc:a", "{\"price\":30}");
    try seg_writer.addStoredDoc("doc:b", "{\"price\":10}");
    try seg_writer.addStoredDoc("doc:c", "{\"price\":20}");
    const seg_bytes = try seg_writer.build();
    defer alloc.free(seg_bytes);

    var writer = try index_mod.IndexWriter.init(alloc);
    defer writer.deinit();
    try writer.addSegment(seg_bytes);
    const snapshot = writer.snapshot();
    const native_sort_ctx = TextDocValueSortContext{ .snapshot = snapshot };
    const templates = [_]runtime_schema_mod.DynamicTemplate{.{
        .name = "price",
        .path_match = "price",
        .mapping = .{
            .field_type = .numeric,
            .doc_values = true,
            .sortable = true,
            .analyzer = "keyword",
        },
    }};
    const schema = runtime_schema_mod.TableSchema{ .dynamic_templates = &templates };

    var hits = try alloc.alloc(types.SearchHit, 3);
    hits[0] = .{ .id = try alloc.dupe(u8, "doc:a"), .native_text_doc_id = 0, .score = 1.0 };
    hits[1] = .{ .id = try alloc.dupe(u8, "doc:b"), .native_text_doc_id = 1, .score = 1.0 };
    hits[2] = .{ .id = try alloc.dupe(u8, "doc:c"), .native_text_doc_id = 2, .score = 1.0 };
    var result = types.SearchResult{
        .alloc = alloc,
        .hits = hits,
        .total_hits = 3,
        .graph_results = &.{},
    };
    defer result.deinit();

    const order_by = [_]types.SortField{
        .{ .field = "price", .desc = false },
        .{ .field = "_id", .desc = false },
    };
    try sortAndPageSearchResultInPlace(&result, .{
        .order_by = &order_by,
        .include_stored = false,
        .limit = 2,
    }, null, testUnexpectedLoadStoredCallback, .{
        .kind = .native_doc_values_top_n,
        .require_native = false,
        .runtime_schema = schema,
    }, .{
        .ctx = @constCast(&native_sort_ctx),
        .load = loadTextDocValueSortValue,
    });

    try std.testing.expectEqual(@as(usize, 2), result.hits.len);
    try std.testing.expectEqualStrings("doc:b", result.hits[0].id);
    try std.testing.expectEqualStrings("doc:c", result.hits[1].id);
    try std.testing.expectApproxEqAbs(@as(f64, 10.0), result.hits[0].sort_values[0].float, 0.001);
    try std.testing.expectEqualStrings("doc:b", result.hits[0].sort_values[1].string);
}

test "sort resolves match_all doc ordinals to native text doc values" {
    const alloc = std.testing.allocator;

    var dv_writer = typed_dv.TypedDocValuesWriter.init(alloc, .f64_val, 1024);
    defer dv_writer.deinit();
    try dv_writer.add(0, .{ .f64_val = 30.0 });
    try dv_writer.add(1, .{ .f64_val = 10.0 });
    try dv_writer.add(2, .{ .f64_val = 20.0 });
    const dv_data = try dv_writer.build();
    defer alloc.free(dv_data);

    var seg_writer = segment_mod.SegmentWriter.init(alloc);
    defer seg_writer.deinit();
    const price_idx = try seg_writer.addField("price");
    try seg_writer.addSection(price_idx, .typed_doc_values, dv_data);
    try seg_writer.addStoredDoc("doc:a", "{\"price\":30}");
    try seg_writer.addStoredDoc("doc:b", "{\"price\":10}");
    try seg_writer.addStoredDoc("doc:c", "{\"price\":20}");
    try seg_writer.addDocOrdinals(&.{ 101, 102, 103 });
    const seg_bytes = try seg_writer.build();
    defer alloc.free(seg_bytes);

    var writer = try index_mod.IndexWriter.init(alloc);
    defer writer.deinit();
    try writer.addSegment(seg_bytes);
    const snapshot = writer.snapshot();

    var ordinal_to_text_doc_id = std.AutoHashMapUnmanaged(doc_set.DocOrdinal, u32).empty;
    defer ordinal_to_text_doc_id.deinit(alloc);
    const candidates = [_]MatchAllCandidate{
        .{ .id = @constCast("doc:a"), .ordinal = 101 },
        .{ .id = @constCast("doc:b"), .ordinal = 102 },
        .{ .id = @constCast("doc:c"), .ordinal = 103 },
    };
    try std.testing.expect(try buildOrdinalTextDocIdMapAlloc(alloc, snapshot, &candidates, &ordinal_to_text_doc_id));
    const native_sort_ctx = TextDocValueSortContext{
        .snapshot = snapshot,
        .ordinal_to_text_doc_id = &ordinal_to_text_doc_id,
    };
    const templates = [_]runtime_schema_mod.DynamicTemplate{.{
        .name = "price",
        .path_match = "price",
        .mapping = .{
            .field_type = .numeric,
            .doc_values = true,
            .sortable = true,
            .analyzer = "keyword",
        },
    }};
    const schema = runtime_schema_mod.TableSchema{ .dynamic_templates = &templates };

    var hits = try alloc.alloc(types.SearchHit, 3);
    hits[0] = .{ .id = try alloc.dupe(u8, "doc:a"), .doc_ordinal = 101, .score = 1.0 };
    hits[1] = .{ .id = try alloc.dupe(u8, "doc:b"), .doc_ordinal = 102, .score = 1.0 };
    hits[2] = .{ .id = try alloc.dupe(u8, "doc:c"), .doc_ordinal = 103, .score = 1.0 };
    var result = types.SearchResult{
        .alloc = alloc,
        .hits = hits,
        .total_hits = 3,
        .graph_results = &.{},
    };
    defer result.deinit();

    const order_by = [_]types.SortField{.{ .field = "price", .desc = false }};
    try sortAndPageSearchResultInPlace(&result, .{
        .order_by = &order_by,
        .include_stored = false,
        .limit = 3,
    }, null, testUnexpectedLoadStoredCallback, .{
        .kind = .native_doc_values_top_n,
        .require_native = false,
        .runtime_schema = schema,
    }, .{
        .ctx = @constCast(&native_sort_ctx),
        .load = loadTextDocValueSortValue,
    });

    try std.testing.expectEqual(@as(usize, 3), result.hits.len);
    try std.testing.expectEqualStrings("doc:b", result.hits[0].id);
    try std.testing.expectEqualStrings("doc:c", result.hits[1].id);
    try std.testing.expectEqualStrings("doc:a", result.hits[2].id);
}

test "required native sort does not fall back to stored json on doc value miss" {
    const alloc = std.testing.allocator;

    var seg_writer = segment_mod.SegmentWriter.init(alloc);
    defer seg_writer.deinit();
    try seg_writer.addStoredDoc("doc:a", "{\"price\":30}");
    const seg_bytes = try seg_writer.build();
    defer alloc.free(seg_bytes);

    var writer = try index_mod.IndexWriter.init(alloc);
    defer writer.deinit();
    try writer.addSegment(seg_bytes);
    const snapshot = writer.snapshot();
    const native_sort_ctx = TextDocValueSortContext{ .snapshot = snapshot };

    var hits = try alloc.alloc(types.SearchHit, 1);
    hits[0] = .{ .id = try alloc.dupe(u8, "doc:a"), .score = 1.0 };
    var result = types.SearchResult{
        .alloc = alloc,
        .hits = hits,
        .total_hits = 1,
        .graph_results = &.{},
    };
    defer result.deinit();

    const order_by = [_]types.SortField{.{ .field = "price", .desc = false }};
    try std.testing.expectError(error.UnsupportedQueryRequest, sortAndPageSearchResultInPlace(&result, .{
        .order_by = &order_by,
        .include_stored = false,
        .limit = 1,
    }, null, testUnexpectedLoadStoredCallback, .{ .kind = .native_doc_values_top_n, .require_native = true }, .{
        .ctx = @constCast(&native_sort_ctx),
        .require_native = true,
        .load = loadTextDocValueSortValue,
    }));
}

test "native doc values plan enforces native values even with non-requiring loader" {
    const alloc = std.testing.allocator;

    var seg_writer = segment_mod.SegmentWriter.init(alloc);
    defer seg_writer.deinit();
    try seg_writer.addStoredDoc("doc:a", "{\"price\":30}");
    const seg_bytes = try seg_writer.build();
    defer alloc.free(seg_bytes);

    var writer = try index_mod.IndexWriter.init(alloc);
    defer writer.deinit();
    try writer.addSegment(seg_bytes);
    const snapshot = writer.snapshot();
    const native_sort_ctx = TextDocValueSortContext{ .snapshot = snapshot };

    var hits = try alloc.alloc(types.SearchHit, 1);
    hits[0] = .{ .id = try alloc.dupe(u8, "doc:a"), .score = 1.0 };
    var result = types.SearchResult{
        .alloc = alloc,
        .hits = hits,
        .total_hits = 1,
        .graph_results = &.{},
    };
    defer result.deinit();

    const order_by = [_]types.SortField{.{ .field = "price", .desc = false }};
    try std.testing.expectError(error.UnsupportedQueryRequest, sortAndPageSearchResultInPlace(&result, .{
        .order_by = &order_by,
        .include_stored = false,
        .limit = 1,
    }, null, testUnexpectedLoadStoredCallback, .{ .kind = .native_doc_values_top_n, .require_native = false }, .{
        .ctx = @constCast(&native_sort_ctx),
        .require_native = false,
        .load = loadTextDocValueSortValue,
    }));
}

test "native doc values plan rejects runtime value kind mismatch" {
    const alloc = std.testing.allocator;

    const templates = [_]runtime_schema_mod.DynamicTemplate{.{
        .name = "title",
        .path_match = "title",
        .mapping = .{
            .field_type = .keyword,
            .doc_values = true,
            .sortable = true,
            .analyzer = "keyword",
        },
    }};
    const schema = runtime_schema_mod.TableSchema{ .dynamic_templates = &templates };

    var hits = try alloc.alloc(types.SearchHit, 1);
    hits[0] = .{ .id = try alloc.dupe(u8, "doc:a"), .score = 1.0 };
    var result = types.SearchResult{
        .alloc = alloc,
        .hits = hits,
        .total_hits = 1,
        .graph_results = &.{},
    };
    defer result.deinit();

    const Loader = struct {
        fn load(_: ?*anyopaque, _: Allocator, _: types.SearchHit, field: []const u8) anyerror!?SortValue {
            try std.testing.expectEqualStrings("title", field);
            return .{ .number = 42.0 };
        }
    };

    const order_by = [_]types.SortField{.{ .field = "title", .desc = false }};
    try std.testing.expectError(error.UnsupportedQueryRequest, sortAndPageSearchResultInPlace(&result, .{
        .order_by = &order_by,
        .include_stored = false,
        .limit = 1,
    }, null, testUnexpectedLoadStoredCallback, .{
        .kind = .native_doc_values_top_n,
        .require_native = true,
        .runtime_schema = schema,
    }, .{
        .require_native = true,
        .load = Loader.load,
    }));
}

test "required native sort fails on absent physical doc value section" {
    const alloc = std.testing.allocator;

    var seg_writer = segment_mod.SegmentWriter.init(alloc);
    defer seg_writer.deinit();
    try seg_writer.addStoredDoc("doc:a", "{\"price\":30}");
    const seg_bytes = try seg_writer.build();
    defer alloc.free(seg_bytes);

    var writer = try index_mod.IndexWriter.init(alloc);
    defer writer.deinit();
    try writer.addSegment(seg_bytes);
    const snapshot = writer.snapshot();
    const native_sort_ctx = TextDocValueSortContext{ .snapshot = snapshot };

    var hits = try alloc.alloc(types.SearchHit, 1);
    hits[0] = .{ .id = try alloc.dupe(u8, "doc:a"), .native_text_doc_id = 0, .score = 1.0 };
    var result = types.SearchResult{
        .alloc = alloc,
        .hits = hits,
        .total_hits = 1,
        .graph_results = &.{},
    };
    defer result.deinit();

    const order_by = [_]types.SortField{.{ .field = "price", .desc = false }};
    try std.testing.expectError(error.UnsupportedQueryRequest, sortAndPageSearchResultInPlace(&result, .{
        .order_by = &order_by,
        .include_stored = false,
        .limit = 1,
    }, null, testUnexpectedLoadStoredCallback, .{ .kind = .native_doc_values_top_n, .require_native = true }, .{
        .ctx = @constCast(&native_sort_ctx),
        .require_native = true,
        .load = loadTextDocValueSortValue,
    }));
}

test "required native sort fails on sparse doc value entry miss" {
    const alloc = std.testing.allocator;

    var dv_writer = typed_dv.TypedDocValuesWriter.init(alloc, .f64_val, 1024);
    defer dv_writer.deinit();
    try dv_writer.add(0, .{ .f64_val = 30.0 });
    const dv_data = try dv_writer.build();
    defer alloc.free(dv_data);

    var seg_writer = segment_mod.SegmentWriter.init(alloc);
    defer seg_writer.deinit();
    const price_idx = try seg_writer.addField("price");
    try seg_writer.addSection(price_idx, .typed_doc_values, dv_data);
    try seg_writer.addStoredDoc("doc:a", "{\"price\":30}");
    try seg_writer.addStoredDoc("doc:b", "{\"price\":20}");
    const seg_bytes = try seg_writer.build();
    defer alloc.free(seg_bytes);

    var writer = try index_mod.IndexWriter.init(alloc);
    defer writer.deinit();
    try writer.addSegment(seg_bytes);
    const snapshot = writer.snapshot();
    const native_sort_ctx = TextDocValueSortContext{ .snapshot = snapshot };

    var hits = try alloc.alloc(types.SearchHit, 2);
    hits[0] = .{ .id = try alloc.dupe(u8, "doc:a"), .native_text_doc_id = 0, .score = 1.0 };
    hits[1] = .{ .id = try alloc.dupe(u8, "doc:b"), .native_text_doc_id = 1, .score = 1.0 };
    var result = types.SearchResult{
        .alloc = alloc,
        .hits = hits,
        .total_hits = 2,
        .graph_results = &.{},
    };
    defer result.deinit();

    const order_by = [_]types.SortField{.{ .field = "price", .desc = false }};
    try std.testing.expectError(error.UnsupportedQueryRequest, sortAndPageSearchResultInPlace(&result, .{
        .order_by = &order_by,
        .include_stored = false,
        .limit = 2,
    }, null, testUnexpectedLoadStoredCallback, .{ .kind = .native_doc_values_top_n, .require_native = true }, .{
        .ctx = @constCast(&native_sort_ctx),
        .require_native = true,
        .load = loadTextDocValueSortValue,
    }));
}

test "sort paging rejects count-only ordered requests" {
    const alloc = std.testing.allocator;

    var hits = try alloc.alloc(types.SearchHit, 1);
    hits[0] = .{ .id = try alloc.dupe(u8, "doc:a"), .score = 1.0 };
    var result = types.SearchResult{
        .alloc = alloc,
        .hits = hits,
        .total_hits = 1,
        .graph_results = &.{},
    };
    defer result.deinit();

    const order_by = [_]types.SortField{.{ .field = "_id", .desc = false }};
    try std.testing.expectError(error.UnsupportedQueryRequest, sortAndPageSearchResultInPlace(&result, .{
        .order_by = &order_by,
        .count_only = true,
        .limit = 1,
    }, null, testUnexpectedLoadStoredCallback, .{ .kind = .id_only }, null));
}

test "native text sort validation rejects fields without typed doc values" {
    const alloc = std.testing.allocator;

    var seg_writer = segment_mod.SegmentWriter.init(alloc);
    defer seg_writer.deinit();
    try seg_writer.addStoredDoc("doc:a", "{\"title\":\"alpha\"}");
    const seg_bytes = try seg_writer.build();
    defer alloc.free(seg_bytes);

    var writer = try index_mod.IndexWriter.init(alloc);
    defer writer.deinit();
    try writer.addSegment(seg_bytes);
    const snapshot = writer.snapshot();

    const invalid_order_by = [_]types.SortField{.{ .field = "title", .desc = false }};
    try std.testing.expectError(error.UnsupportedQueryRequest, validateTextNativeSortFields(.{
        .order_by = &invalid_order_by,
        .limit = 10,
    }, snapshot, null));

    const id_order_by = [_]types.SortField{.{ .field = "_id", .desc = false }};
    try validateTextNativeSortFields(.{
        .order_by = &id_order_by,
        .limit = 10,
    }, snapshot, null);

    const id_plan = try planTextNativeSortFields(.{
        .order_by = &id_order_by,
        .limit = 10,
    }, snapshot, null);
    try std.testing.expectEqual(SortExecutionPlanKind.id_only, id_plan.kind);
    try std.testing.expect(!id_plan.require_native);

    const score_order_by = [_]types.SortField{.{ .field = "_score", .desc = true }};
    const score_plan = try planTextNativeSortFields(.{
        .order_by = &score_order_by,
        .limit = 10,
    }, snapshot, null);
    try std.testing.expectEqual(SortExecutionPlanKind.score_top_k, score_plan.kind);
    try std.testing.expect(!score_plan.require_native);

    try std.testing.expectError(error.UnsupportedQueryRequest, validateTextNativeSortFields(.{
        .order_by = &id_order_by,
        .count_only = true,
        .limit = 10,
    }, snapshot, null));
}

test "native text sort validation uses runtime sortable mappings" {
    const alloc = std.testing.allocator;

    var dv_writer = typed_dv.TypedDocValuesWriter.init(alloc, .u64_val, 1024);
    defer dv_writer.deinit();
    try dv_writer.add(0, .{ .u64_val = 1 });
    const dv_data = try dv_writer.build();
    defer alloc.free(dv_data);

    var seg_writer = segment_mod.SegmentWriter.init(alloc);
    defer seg_writer.deinit();
    const created_idx = try seg_writer.addField("created_at");
    try seg_writer.addSection(created_idx, .typed_doc_values, dv_data);
    try seg_writer.addStoredDoc("doc:a", "{\"created_at\":\"1970-01-01T00:00:00.000000001Z\",\"title\":\"alpha\"}");
    const seg_bytes = try seg_writer.build();
    defer alloc.free(seg_bytes);

    var writer = try index_mod.IndexWriter.init(alloc);
    defer writer.deinit();
    try writer.addSegment(seg_bytes);
    const snapshot = writer.snapshot();

    const templates = [_]runtime_schema_mod.DynamicTemplate{
        .{
            .name = "created_at",
            .path_match = "created_at",
            .mapping = .{
                .field_type = .datetime,
                .doc_values = true,
                .sortable = true,
                .analyzer = "keyword",
            },
        },
        .{
            .name = "title",
            .path_match = "title",
            .mapping = .{
                .field_type = .keyword,
                .doc_values = true,
                .sortable = false,
                .analyzer = "keyword",
            },
        },
    };
    const index_sort = [_]runtime_schema_mod.IndexSortField{
        .{ .field = "created_at", .desc = true },
        .{ .field = "_id", .desc = false },
    };
    const schema = runtime_schema_mod.TableSchema{
        .dynamic_templates = &templates,
        .index_sort = &index_sort,
    };

    const valid_order_by = [_]types.SortField{.{ .field = "created_at", .desc = true }};
    try validateTextNativeSortFields(.{
        .order_by = &valid_order_by,
        .limit = 10,
    }, snapshot, schema);
    const native_plan = try planTextNativeSortFields(.{
        .order_by = &valid_order_by,
        .limit = 10,
    }, snapshot, schema);
    try std.testing.expectEqual(SortExecutionPlanKind.native_doc_values_top_n, native_plan.kind);
    try std.testing.expect(native_plan.require_native);
    try std.testing.expect(native_plan.index_sort_match);
    try std.testing.expect(!native_plan.sorted_segment_executor_available);

    const valid_cursor = [_]std.json.Value{
        .{ .integer = 123 },
        .{ .string = "doc:a" },
    };
    try validateTextNativeSortFields(.{
        .order_by = &valid_order_by,
        .search_after = &valid_cursor,
        .limit = 10,
    }, snapshot, schema);

    const valid_date_cursor = [_]std.json.Value{
        .{ .string = "2026-01-01T00:00:00Z" },
        .{ .string = "doc:a" },
    };
    try validateTextNativeSortFields(.{
        .order_by = &valid_order_by,
        .search_after = &valid_date_cursor,
        .limit = 10,
    }, snapshot, schema);

    const invalid_cursor = [_]std.json.Value{
        .{ .string = "not-a-date" },
        .{ .string = "doc:a" },
    };
    try std.testing.expectError(error.InvalidQueryRequest, validateTextNativeSortFields(.{
        .order_by = &valid_order_by,
        .search_after = &invalid_cursor,
        .limit = 10,
    }, snapshot, schema));

    const invalid_order_by = [_]types.SortField{.{ .field = "title", .desc = false }};
    try std.testing.expectError(error.UnsupportedQueryRequest, validateTextNativeSortFields(.{
        .order_by = &invalid_order_by,
        .limit = 10,
    }, snapshot, schema));

    const unknown_order_by = [_]types.SortField{.{ .field = "unknown", .desc = false }};
    try std.testing.expectError(error.UnsupportedQueryRequest, validateTextNativeSortFields(.{
        .order_by = &unknown_order_by,
        .limit = 10,
    }, snapshot, schema));
}

test "sort planner detects exact index sort eligibility after implicit id normalization" {
    const templates = [_]runtime_schema_mod.DynamicTemplate{.{
        .name = "created_at",
        .path_match = "created_at",
        .mapping = .{
            .field_type = .datetime,
            .doc_values = true,
            .sortable = true,
            .analyzer = "keyword",
        },
    }};
    const exact_index_sort = [_]runtime_schema_mod.IndexSortField{
        .{ .field = "created_at", .desc = true },
        .{ .field = "_id", .desc = false },
    };
    const exact_schema = runtime_schema_mod.TableSchema{
        .dynamic_templates = &templates,
        .index_sort = &exact_index_sort,
    };

    const created_desc = [_]types.SortField{.{ .field = "created_at", .desc = true }};
    try std.testing.expect(sortRequestMatchesIndexSort(.{
        .order_by = &created_desc,
    }, exact_schema));

    const created_desc_with_id = [_]types.SortField{
        .{ .field = "created_at", .desc = true },
        .{ .field = "_id", .desc = false },
    };
    try std.testing.expect(sortRequestMatchesIndexSort(.{
        .order_by = &created_desc_with_id,
    }, exact_schema));

    const wrong_direction = [_]types.SortField{.{ .field = "created_at", .desc = false }};
    try std.testing.expect(!sortRequestMatchesIndexSort(.{
        .order_by = &wrong_direction,
    }, exact_schema));

    const category_middle_index_sort = [_]runtime_schema_mod.IndexSortField{
        .{ .field = "created_at", .desc = true },
        .{ .field = "category", .desc = false },
        .{ .field = "_id", .desc = false },
    };
    const category_schema = runtime_schema_mod.TableSchema{
        .dynamic_templates = &templates,
        .index_sort = &category_middle_index_sort,
    };
    try std.testing.expect(!sortRequestMatchesIndexSort(.{
        .order_by = &created_desc,
    }, category_schema));
}

test "native text sort validation requires runtime mapping for non-id fields" {
    const alloc = std.testing.allocator;

    var dv_writer = typed_dv.TypedDocValuesWriter.init(alloc, .u64_val, 1024);
    defer dv_writer.deinit();
    try dv_writer.add(0, .{ .u64_val = 1 });
    const dv_data = try dv_writer.build();
    defer alloc.free(dv_data);

    var seg_writer = segment_mod.SegmentWriter.init(alloc);
    defer seg_writer.deinit();
    const created_idx = try seg_writer.addField("created_at");
    try seg_writer.addSection(created_idx, .typed_doc_values, dv_data);
    try seg_writer.addStoredDoc("doc:a", "{\"created_at\":\"1970-01-01T00:00:00.000000001Z\"}");
    const seg_bytes = try seg_writer.build();
    defer alloc.free(seg_bytes);

    var writer = try index_mod.IndexWriter.init(alloc);
    defer writer.deinit();
    try writer.addSegment(seg_bytes);
    const snapshot = writer.snapshot();

    const order_by = [_]types.SortField{.{ .field = "created_at", .desc = false }};
    try std.testing.expectError(error.UnsupportedQueryRequest, validateTextNativeSortFields(.{
        .order_by = &order_by,
        .limit = 10,
    }, snapshot, null));
}

test "native text sort validation rejects typed doc value kind mismatch" {
    const alloc = std.testing.allocator;

    var dv_writer = typed_dv.TypedDocValuesWriter.init(alloc, .f64_val, 1024);
    defer dv_writer.deinit();
    try dv_writer.add(0, .{ .f64_val = 1.0 });
    const dv_data = try dv_writer.build();
    defer alloc.free(dv_data);

    var seg_writer = segment_mod.SegmentWriter.init(alloc);
    defer seg_writer.deinit();
    const created_idx = try seg_writer.addField("created_at");
    try seg_writer.addSection(created_idx, .typed_doc_values, dv_data);
    try seg_writer.addStoredDoc("doc:a", "{\"created_at\":\"1970-01-01T00:00:00.000000001Z\"}");
    const seg_bytes = try seg_writer.build();
    defer alloc.free(seg_bytes);

    var writer = try index_mod.IndexWriter.init(alloc);
    defer writer.deinit();
    try writer.addSegment(seg_bytes);
    const snapshot = writer.snapshot();

    const templates = [_]runtime_schema_mod.DynamicTemplate{.{
        .name = "created_at",
        .path_match = "created_at",
        .mapping = .{
            .field_type = .datetime,
            .doc_values = true,
            .sortable = true,
            .analyzer = "keyword",
        },
    }};
    const schema = runtime_schema_mod.TableSchema{ .dynamic_templates = &templates };

    const order_by = [_]types.SortField{.{ .field = "created_at", .desc = false }};
    try std.testing.expectError(error.UnsupportedQueryRequest, validateTextNativeSortFields(.{
        .order_by = &order_by,
        .limit = 10,
    }, snapshot, schema));
}

test "native sort planner classifies mapping and cursor rejection reasons" {
    const keyword_mapping = runtime_schema_mod.FieldMapping{
        .field_type = .keyword,
        .doc_values = true,
        .sortable = true,
        .analyzer = "keyword",
    };
    const text_mapping = runtime_schema_mod.FieldMapping{
        .field_type = .text,
        .doc_values = true,
        .sortable = true,
        .analyzer = "standard",
    };
    const non_sortable_mapping = runtime_schema_mod.FieldMapping{
        .field_type = .keyword,
        .doc_values = true,
        .sortable = false,
        .analyzer = "keyword",
    };
    const non_doc_valued_mapping = runtime_schema_mod.FieldMapping{
        .field_type = .keyword,
        .doc_values = false,
        .sortable = true,
        .analyzer = "keyword",
    };
    const datetime_mapping = runtime_schema_mod.FieldMapping{
        .field_type = .datetime,
        .doc_values = true,
        .sortable = true,
        .analyzer = "keyword",
    };

    try std.testing.expect(mappedSortFieldRejectionReason(keyword_mapping) == null);
    try std.testing.expectEqual(NativeSortPlanRejectionReason.non_scalar_field, mappedSortFieldRejectionReason(text_mapping).?);
    try std.testing.expectEqual(NativeSortPlanRejectionReason.non_sortable_field, mappedSortFieldRejectionReason(non_sortable_mapping).?);
    try std.testing.expectEqual(NativeSortPlanRejectionReason.missing_doc_values_capability, mappedSortFieldRejectionReason(non_doc_valued_mapping).?);

    try std.testing.expect(mappedSortCursorValueIsValid(keyword_mapping, .{ .string = "doc:a" }));
    try std.testing.expect(!mappedSortCursorValueIsValid(keyword_mapping, .{ .integer = 42 }));
    try std.testing.expect(mappedSortCursorValueIsValid(datetime_mapping, .{ .string = "1970-01-01T00:00:00Z" }));
    try std.testing.expect(!mappedSortCursorValueIsValid(datetime_mapping, .{ .string = "not-a-date" }));
}

test "native sort coverage diagnostics classify physical doc value failures" {
    const alloc = std.testing.allocator;
    const numeric_mapping = runtime_schema_mod.FieldMapping{
        .field_type = .numeric,
        .doc_values = true,
        .sortable = true,
        .analyzer = "keyword",
    };
    const boolean_mapping = runtime_schema_mod.FieldMapping{
        .field_type = .boolean,
        .doc_values = true,
        .sortable = true,
        .analyzer = "keyword",
    };

    {
        var seg_writer = segment_mod.SegmentWriter.init(alloc);
        defer seg_writer.deinit();
        try seg_writer.addStoredDoc("doc:a", "{\"price\":1}");
        const seg_bytes = try seg_writer.build();
        defer alloc.free(seg_bytes);

        var writer = try index_mod.IndexWriter.init(alloc);
        defer writer.deinit();
        try writer.addSegment(seg_bytes);
        const snapshot = writer.snapshot();

        try std.testing.expectEqual(
            TypedDocValuesCoverageStatus.missing_doc_values_section,
            try snapshotTypedDocValuesCoverageForMapping(snapshot, "price", numeric_mapping),
        );
    }

    {
        var dv_writer = typed_dv.TypedDocValuesWriter.init(alloc, .f64_val, 1024);
        defer dv_writer.deinit();
        try dv_writer.add(0, .{ .f64_val = 1.0 });
        const dv_data = try dv_writer.build();
        defer alloc.free(dv_data);

        var seg_writer = segment_mod.SegmentWriter.init(alloc);
        defer seg_writer.deinit();
        const price_idx = try seg_writer.addField("price");
        try seg_writer.addSection(price_idx, .typed_doc_values, dv_data);
        try seg_writer.addStoredDoc("doc:a", "{\"price\":1}");
        const seg_bytes = try seg_writer.build();
        defer alloc.free(seg_bytes);

        var writer = try index_mod.IndexWriter.init(alloc);
        defer writer.deinit();
        try writer.addSegment(seg_bytes);
        const snapshot = writer.snapshot();

        try std.testing.expectEqual(
            TypedDocValuesCoverageStatus.doc_values_kind_mismatch,
            try snapshotTypedDocValuesCoverageForMapping(snapshot, "price", boolean_mapping),
        );
    }

    {
        var dv_writer = typed_dv.TypedDocValuesWriter.init(alloc, .f64_val, 1024);
        defer dv_writer.deinit();
        try dv_writer.add(0, .{ .f64_val = 1.0 });
        const dv_data = try dv_writer.build();
        defer alloc.free(dv_data);

        var seg_writer = segment_mod.SegmentWriter.init(alloc);
        defer seg_writer.deinit();
        const price_idx = try seg_writer.addField("price");
        try seg_writer.addSection(price_idx, .typed_doc_values, dv_data);
        try seg_writer.addStoredDoc("doc:a", "{\"price\":1}");
        try seg_writer.addStoredDoc("doc:b", "{\"price\":2}");
        const seg_bytes = try seg_writer.build();
        defer alloc.free(seg_bytes);

        var writer = try index_mod.IndexWriter.init(alloc);
        defer writer.deinit();
        try writer.addSegment(seg_bytes);
        const snapshot = writer.snapshot();

        try std.testing.expectEqual(
            TypedDocValuesCoverageStatus.sparse_live_doc_values,
            try snapshotTypedDocValuesCoverageForMapping(snapshot, "price", numeric_mapping),
        );
    }

    {
        var dv_writer = typed_dv.TypedDocValuesWriter.init(alloc, .f64_val, 1024);
        defer dv_writer.deinit();
        try dv_writer.add(7, .{ .f64_val = 1.0 });
        const dv_data = try dv_writer.build();
        defer alloc.free(dv_data);

        var seg_writer = segment_mod.SegmentWriter.init(alloc);
        defer seg_writer.deinit();
        const price_idx = try seg_writer.addField("price");
        try seg_writer.addSection(price_idx, .typed_doc_values, dv_data);
        try seg_writer.addStoredDoc("doc:a", "{\"price\":1}");
        const seg_bytes = try seg_writer.build();
        defer alloc.free(seg_bytes);

        var writer = try index_mod.IndexWriter.init(alloc);
        defer writer.deinit();
        try writer.addSegment(seg_bytes);
        const snapshot = writer.snapshot();

        try std.testing.expectEqual(
            TypedDocValuesCoverageStatus.invalid_doc_value_doc_id,
            try snapshotTypedDocValuesCoverageForMapping(snapshot, "price", numeric_mapping),
        );
    }

    {
        var dv_writer = typed_dv.TypedDocValuesWriter.init(alloc, .f64_val, 1024);
        defer dv_writer.deinit();
        try dv_writer.add(0, .{ .f64_val = 1.0 });
        try dv_writer.add(0, .{ .f64_val = 2.0 });
        const dv_data = try dv_writer.build();
        defer alloc.free(dv_data);

        var seg_writer = segment_mod.SegmentWriter.init(alloc);
        defer seg_writer.deinit();
        const price_idx = try seg_writer.addField("price");
        try seg_writer.addSection(price_idx, .typed_doc_values, dv_data);
        try seg_writer.addStoredDoc("doc:a", "{\"price\":1}");
        const seg_bytes = try seg_writer.build();
        defer alloc.free(seg_bytes);

        var writer = try index_mod.IndexWriter.init(alloc);
        defer writer.deinit();
        try writer.addSegment(seg_bytes);
        const snapshot = writer.snapshot();

        try std.testing.expectEqual(
            TypedDocValuesCoverageStatus.duplicate_doc_value_doc_id,
            try snapshotTypedDocValuesCoverageForMapping(snapshot, "price", numeric_mapping),
        );
    }
}

test "native text sort validation rejects sparse typed doc values for live docs" {
    const alloc = std.testing.allocator;

    var dv_writer = typed_dv.TypedDocValuesWriter.init(alloc, .f64_val, 1024);
    defer dv_writer.deinit();
    try dv_writer.add(0, .{ .f64_val = 1.0 });
    const dv_data = try dv_writer.build();
    defer alloc.free(dv_data);

    var seg_writer = segment_mod.SegmentWriter.init(alloc);
    defer seg_writer.deinit();
    const price_idx = try seg_writer.addField("price");
    try seg_writer.addSection(price_idx, .typed_doc_values, dv_data);
    try seg_writer.addStoredDoc("doc:a", "{\"price\":1}");
    try seg_writer.addStoredDoc("doc:b", "{\"price\":2}");
    const seg_bytes = try seg_writer.build();
    defer alloc.free(seg_bytes);

    var writer = try index_mod.IndexWriter.init(alloc);
    defer writer.deinit();
    try writer.addSegment(seg_bytes);
    const snapshot = writer.snapshot();

    const templates = [_]runtime_schema_mod.DynamicTemplate{.{
        .name = "price",
        .path_match = "price",
        .mapping = .{
            .field_type = .numeric,
            .doc_values = true,
            .sortable = true,
            .analyzer = "keyword",
        },
    }};
    const schema = runtime_schema_mod.TableSchema{ .dynamic_templates = &templates };

    const order_by = [_]types.SortField{.{ .field = "price", .desc = false }};
    try std.testing.expectError(error.UnsupportedQueryRequest, validateTextNativeSortFields(.{
        .order_by = &order_by,
        .limit = 10,
    }, snapshot, schema));
}

test "native text sort validation honors full path mapping constraints" {
    const alloc = std.testing.allocator;

    var dv_writer = typed_dv.TypedDocValuesWriter.init(alloc, .u64_val, 1024);
    defer dv_writer.deinit();
    try dv_writer.add(0, .{ .u64_val = 1 });
    const dv_data = try dv_writer.build();
    defer alloc.free(dv_data);

    var seg_writer = segment_mod.SegmentWriter.init(alloc);
    defer seg_writer.deinit();
    const created_idx = try seg_writer.addField("secret.created_at");
    try seg_writer.addSection(created_idx, .typed_doc_values, dv_data);
    try seg_writer.addStoredDoc("doc:a", "{\"secret\":{\"created_at\":\"1970-01-01T00:00:00.000000001Z\"}}");
    const seg_bytes = try seg_writer.build();
    defer alloc.free(seg_bytes);

    var writer = try index_mod.IndexWriter.init(alloc);
    defer writer.deinit();
    try writer.addSegment(seg_bytes);
    const snapshot = writer.snapshot();

    const templates = [_]runtime_schema_mod.DynamicTemplate{.{
        .name = "dates",
        .match_pattern = "*_at",
        .path_unmatch = "secret.*",
        .mapping = .{
            .field_type = .datetime,
            .doc_values = true,
            .sortable = true,
            .analyzer = "keyword",
        },
    }};
    const schema = runtime_schema_mod.TableSchema{ .dynamic_templates = &templates };

    const order_by = [_]types.SortField{.{ .field = "secret.created_at", .desc = false }};
    try std.testing.expectError(error.UnsupportedQueryRequest, validateTextNativeSortFields(.{
        .order_by = &order_by,
        .limit = 10,
    }, snapshot, schema));
}

test "native text sort validation requires schema-backed physical doc values" {
    const alloc = std.testing.allocator;

    var seg_writer = segment_mod.SegmentWriter.init(alloc);
    defer seg_writer.deinit();
    try seg_writer.addStoredDoc("doc:a", "{\"created_at\":\"1970-01-01T00:00:00.000000001Z\"}");
    const seg_bytes = try seg_writer.build();
    defer alloc.free(seg_bytes);

    var writer = try index_mod.IndexWriter.init(alloc);
    defer writer.deinit();
    try writer.addSegment(seg_bytes);
    const snapshot = writer.snapshot();

    const templates = [_]runtime_schema_mod.DynamicTemplate{.{
        .name = "created_at",
        .path_match = "created_at",
        .mapping = .{
            .field_type = .datetime,
            .doc_values = true,
            .sortable = true,
            .analyzer = "keyword",
        },
    }};
    const schema = runtime_schema_mod.TableSchema{
        .dynamic_templates = &templates,
    };

    const order_by = [_]types.SortField{.{ .field = "created_at", .desc = false }};
    try std.testing.expectError(error.UnsupportedQueryRequest, validateTextNativeSortFields(.{
        .order_by = &order_by,
        .limit = 10,
    }, snapshot, schema));
}

test "native text sort validation requires typed doc values on every live segment" {
    const alloc = std.testing.allocator;

    var dv_writer = typed_dv.TypedDocValuesWriter.init(alloc, .f64_val, 1024);
    defer dv_writer.deinit();
    try dv_writer.add(0, .{ .f64_val = 10.0 });
    const dv_data = try dv_writer.build();
    defer alloc.free(dv_data);

    var covered_seg_writer = segment_mod.SegmentWriter.init(alloc);
    defer covered_seg_writer.deinit();
    const covered_price_idx = try covered_seg_writer.addField("price");
    try covered_seg_writer.addSection(covered_price_idx, .typed_doc_values, dv_data);
    try covered_seg_writer.addStoredDoc("doc:a", "{\"price\":10}");
    const covered_seg_bytes = try covered_seg_writer.build();
    defer alloc.free(covered_seg_bytes);

    var missing_seg_writer = segment_mod.SegmentWriter.init(alloc);
    defer missing_seg_writer.deinit();
    try missing_seg_writer.addStoredDoc("doc:b", "{\"price\":20}");
    const missing_seg_bytes = try missing_seg_writer.build();
    defer alloc.free(missing_seg_bytes);

    var writer = try index_mod.IndexWriter.init(alloc);
    defer writer.deinit();
    try writer.addSegment(covered_seg_bytes);
    try writer.addSegment(missing_seg_bytes);
    const snapshot = writer.snapshot();

    const order_by = [_]types.SortField{.{ .field = "price", .desc = false }};
    try std.testing.expectError(error.UnsupportedQueryRequest, validateTextNativeSortFields(.{
        .order_by = &order_by,
        .limit = 10,
    }, snapshot, null));
}

test "native text sort validation ignores fully deleted segments without typed doc values" {
    const alloc = std.testing.allocator;

    var dv_writer = typed_dv.TypedDocValuesWriter.init(alloc, .f64_val, 1024);
    defer dv_writer.deinit();
    try dv_writer.add(0, .{ .f64_val = 10.0 });
    const dv_data = try dv_writer.build();
    defer alloc.free(dv_data);

    var covered_seg_writer = segment_mod.SegmentWriter.init(alloc);
    defer covered_seg_writer.deinit();
    const covered_price_idx = try covered_seg_writer.addField("price");
    try covered_seg_writer.addSection(covered_price_idx, .typed_doc_values, dv_data);
    try covered_seg_writer.addStoredDoc("doc:live", "{\"price\":10}");
    const covered_seg_bytes = try covered_seg_writer.build();
    defer alloc.free(covered_seg_bytes);

    var deleted_seg_writer = segment_mod.SegmentWriter.init(alloc);
    defer deleted_seg_writer.deinit();
    try deleted_seg_writer.addStoredDoc("doc:deleted", "{\"price\":20}");
    const deleted_seg_bytes = try deleted_seg_writer.build();
    defer alloc.free(deleted_seg_bytes);

    var writer = try index_mod.IndexWriter.init(alloc);
    defer writer.deinit();
    try writer.addSegment(covered_seg_bytes);
    try writer.addSegment(deleted_seg_bytes);
    try std.testing.expect(try writer.deleteById("doc:deleted"));
    const snapshot = writer.snapshot();

    const templates = [_]runtime_schema_mod.DynamicTemplate{.{
        .name = "price",
        .path_match = "price",
        .mapping = .{
            .field_type = .numeric,
            .doc_values = true,
            .sortable = true,
            .analyzer = "keyword",
        },
    }};
    const schema = runtime_schema_mod.TableSchema{ .dynamic_templates = &templates };
    const order_by = [_]types.SortField{.{ .field = "price", .desc = false }};
    try validateTextNativeSortFields(.{
        .order_by = &order_by,
        .limit = 10,
    }, snapshot, schema);
}

test "stored json debug sort can decorate bounded test results" {
    const alloc = std.testing.allocator;

    var hits = try alloc.alloc(types.SearchHit, 2);
    hits[0] = .{ .id = try alloc.dupe(u8, "doc:a"), .score = 1.0 };
    hits[1] = .{ .id = try alloc.dupe(u8, "doc:b"), .score = 1.0 };
    var result = types.SearchResult{
        .alloc = alloc,
        .hits = hits,
        .total_hits = 2,
        .graph_results = &.{},
    };
    defer result.deinit();

    const order_by = [_]types.SortField{.{ .field = "rank", .desc = false }};
    try sortAndPageSearchResultInPlace(&result, .{
        .order_by = &order_by,
        .include_stored = false,
        .limit = 2,
    }, null, testMatchAllLoadStoredCallback, .{ .kind = .stored_json_debug }, null);

    try std.testing.expectEqualStrings("doc:b", result.hits[0].id);
    try std.testing.expectEqual(@as(i64, 1), result.hits[0].sort_values[0].integer);
    try std.testing.expectEqualStrings("doc:a", result.hits[1].id);
}

test "stored json debug sort rejects non-scalar sort values" {
    const alloc = std.testing.allocator;

    const callbacks = struct {
        fn loadStored(_: ?*anyopaque, load_alloc: Allocator, key: []const u8) anyerror!?[]u8 {
            const json = if (std.mem.eql(u8, key, "doc:a"))
                "{\"rank\":{\"nested\":1}}"
            else if (std.mem.eql(u8, key, "doc:b"))
                "{\"rank\":1}"
            else
                return null;
            return try load_alloc.dupe(u8, json);
        }
    };

    var hits = try alloc.alloc(types.SearchHit, 2);
    hits[0] = .{ .id = try alloc.dupe(u8, "doc:a"), .score = 1.0 };
    hits[1] = .{ .id = try alloc.dupe(u8, "doc:b"), .score = 1.0 };
    var result = types.SearchResult{
        .alloc = alloc,
        .hits = hits,
        .total_hits = 2,
        .graph_results = &.{},
    };
    defer result.deinit();

    const order_by = [_]types.SortField{.{ .field = "rank", .desc = false }};
    try std.testing.expectError(error.UnsupportedQueryRequest, sortAndPageSearchResultInPlace(&result, .{
        .order_by = &order_by,
        .include_stored = false,
        .limit = 2,
    }, null, callbacks.loadStored, .{ .kind = .stored_json_debug }, null));
}

test "score sort uses hit score and implicit id cursor" {
    const alloc = std.testing.allocator;

    var hits = try alloc.alloc(types.SearchHit, 3);
    hits[0] = .{ .id = try alloc.dupe(u8, "doc:a"), .score = 0.25 };
    hits[1] = .{ .id = try alloc.dupe(u8, "doc:b"), .score = 2.0 };
    hits[2] = .{ .id = try alloc.dupe(u8, "doc:c"), .score = 1.0 };
    var first_page = types.SearchResult{
        .alloc = alloc,
        .hits = hits,
        .total_hits = 3,
        .graph_results = &.{},
    };
    defer first_page.deinit();

    const order_by = [_]types.SortField{.{ .field = "_score", .desc = true }};
    try sortAndPageSearchResultInPlace(&first_page, .{
        .order_by = &order_by,
        .include_stored = false,
        .limit = 2,
    }, null, testUnexpectedLoadStoredCallback, .{ .kind = .score_top_k }, null);

    try std.testing.expectEqual(@as(usize, 2), first_page.hits.len);
    try std.testing.expectEqualStrings("doc:b", first_page.hits[0].id);
    try std.testing.expectEqualStrings("doc:c", first_page.hits[1].id);
    try std.testing.expectEqual(@as(usize, 2), first_page.hits[0].sort_values.len);
    try std.testing.expectApproxEqAbs(@as(f64, 2.0), first_page.hits[0].sort_values[0].float, 0.001);
    try std.testing.expectEqualStrings("doc:b", first_page.hits[0].sort_values[1].string);

    var next_hits = try alloc.alloc(types.SearchHit, 3);
    next_hits[0] = .{ .id = try alloc.dupe(u8, "doc:a"), .score = 0.25 };
    next_hits[1] = .{ .id = try alloc.dupe(u8, "doc:b"), .score = 2.0 };
    next_hits[2] = .{ .id = try alloc.dupe(u8, "doc:c"), .score = 1.0 };
    var second_page = types.SearchResult{
        .alloc = alloc,
        .hits = next_hits,
        .total_hits = 3,
        .graph_results = &.{},
    };
    defer second_page.deinit();

    try sortAndPageSearchResultInPlace(&second_page, .{
        .order_by = &order_by,
        .search_after = first_page.hits[1].sort_values,
        .include_stored = false,
        .limit = 2,
    }, null, testUnexpectedLoadStoredCallback, .{ .kind = .score_top_k }, null);

    try std.testing.expectEqual(@as(usize, 1), second_page.hits.len);
    try std.testing.expectEqualStrings("doc:a", second_page.hits[0].id);
    try std.testing.expectApproxEqAbs(@as(f64, 0.25), second_page.hits[0].sort_values[0].float, 0.001);
    try std.testing.expectEqualStrings("doc:a", second_page.hits[0].sort_values[1].string);
}

test "stored sort appends implicit id tiebreaker for stable cursors" {
    const alloc = std.testing.allocator;

    var hits = try alloc.alloc(types.SearchHit, 3);
    hits[0] = .{ .id = try alloc.dupe(u8, "doc:a"), .score = 1.0 };
    hits[1] = .{ .id = try alloc.dupe(u8, "doc:b"), .score = 1.0 };
    hits[2] = .{ .id = try alloc.dupe(u8, "doc:c"), .score = 1.0 };
    var first_page = types.SearchResult{
        .alloc = alloc,
        .hits = hits,
        .total_hits = 3,
        .graph_results = &.{},
    };
    defer first_page.deinit();

    const order_by = [_]types.SortField{.{ .field = "rank", .desc = false }};
    try sortAndPageSearchResultInPlace(&first_page, .{
        .order_by = &order_by,
        .include_stored = false,
        .limit = 2,
    }, null, testMatchAllLoadStoredCallback, .{ .kind = .stored_json_debug }, null);

    try std.testing.expectEqual(@as(usize, 2), first_page.hits.len);
    try std.testing.expectEqualStrings("doc:b", first_page.hits[0].id);
    try std.testing.expectEqualStrings("doc:c", first_page.hits[1].id);
    try std.testing.expectEqual(@as(usize, 2), first_page.hits[0].sort_values.len);
    try std.testing.expectEqual(@as(i64, 1), first_page.hits[0].sort_values[0].integer);
    try std.testing.expectEqualStrings("doc:b", first_page.hits[0].sort_values[1].string);

    var next_hits = try alloc.alloc(types.SearchHit, 3);
    next_hits[0] = .{ .id = try alloc.dupe(u8, "doc:a"), .score = 1.0 };
    next_hits[1] = .{ .id = try alloc.dupe(u8, "doc:b"), .score = 1.0 };
    next_hits[2] = .{ .id = try alloc.dupe(u8, "doc:c"), .score = 1.0 };
    var second_page = types.SearchResult{
        .alloc = alloc,
        .hits = next_hits,
        .total_hits = 3,
        .graph_results = &.{},
    };
    defer second_page.deinit();

    try sortAndPageSearchResultInPlace(&second_page, .{
        .order_by = &order_by,
        .search_after = first_page.hits[1].sort_values,
        .include_stored = false,
        .limit = 2,
    }, null, testMatchAllLoadStoredCallback, .{ .kind = .stored_json_debug }, null);

    try std.testing.expectEqual(@as(usize, 1), second_page.hits.len);
    try std.testing.expectEqualStrings("doc:a", second_page.hits[0].id);
    try std.testing.expectEqual(@as(i64, 2), second_page.hits[0].sort_values[0].integer);
    try std.testing.expectEqualStrings("doc:a", second_page.hits[0].sort_values[1].string);
}

test "native datetime sort accepts iso string cursor" {
    const alloc = std.testing.allocator;

    var dv_writer = typed_dv.TypedDocValuesWriter.init(alloc, .u64_val, 1024);
    defer dv_writer.deinit();
    try dv_writer.add(0, .{ .u64_val = 10 });
    try dv_writer.add(1, .{ .u64_val = 20 });
    try dv_writer.add(2, .{ .u64_val = 30 });
    const dv_data = try dv_writer.build();
    defer alloc.free(dv_data);

    var seg_writer = segment_mod.SegmentWriter.init(alloc);
    defer seg_writer.deinit();
    const created_idx = try seg_writer.addField("created_at");
    try seg_writer.addSection(created_idx, .typed_doc_values, dv_data);
    try seg_writer.addStoredDoc("doc:a", "{\"created_at\":\"1970-01-01T00:00:00.000000010Z\"}");
    try seg_writer.addStoredDoc("doc:b", "{\"created_at\":\"1970-01-01T00:00:00.000000020Z\"}");
    try seg_writer.addStoredDoc("doc:c", "{\"created_at\":\"1970-01-01T00:00:00.000000030Z\"}");
    const seg_bytes = try seg_writer.build();
    defer alloc.free(seg_bytes);

    var writer = try index_mod.IndexWriter.init(alloc);
    defer writer.deinit();
    try writer.addSegment(seg_bytes);
    const snapshot = writer.snapshot();
    const native_sort_ctx = TextDocValueSortContext{ .snapshot = snapshot };
    const templates = [_]runtime_schema_mod.DynamicTemplate{.{
        .name = "created_at",
        .path_match = "created_at",
        .mapping = .{
            .field_type = .datetime,
            .doc_values = true,
            .sortable = true,
            .analyzer = "keyword",
        },
    }};
    const schema = runtime_schema_mod.TableSchema{ .dynamic_templates = &templates };

    var hits = try alloc.alloc(types.SearchHit, 3);
    hits[0] = .{ .id = try alloc.dupe(u8, "doc:a"), .native_text_doc_id = 0, .score = 1.0 };
    hits[1] = .{ .id = try alloc.dupe(u8, "doc:b"), .native_text_doc_id = 1, .score = 1.0 };
    hits[2] = .{ .id = try alloc.dupe(u8, "doc:c"), .native_text_doc_id = 2, .score = 1.0 };
    var result = types.SearchResult{
        .alloc = alloc,
        .hits = hits,
        .total_hits = 3,
        .graph_results = &.{},
    };
    defer result.deinit();

    const order_by = [_]types.SortField{
        .{ .field = "created_at", .desc = false },
        .{ .field = "_id", .desc = false },
    };
    const cursor = [_]std.json.Value{
        .{ .string = "1970-01-01T00:00:00.000000015Z" },
        .{ .string = "doc:a" },
    };
    try sortAndPageSearchResultInPlace(&result, .{
        .order_by = &order_by,
        .search_after = &cursor,
        .include_stored = false,
        .limit = 10,
    }, null, testUnexpectedLoadStoredCallback, .{
        .kind = .native_doc_values_top_n,
        .require_native = true,
        .runtime_schema = schema,
    }, .{
        .ctx = @constCast(&native_sort_ctx),
        .require_native = true,
        .load = loadTextDocValueSortValue,
    });

    try std.testing.expectEqual(@as(usize, 2), result.hits.len);
    try std.testing.expectEqualStrings("doc:b", result.hits[0].id);
    try std.testing.expectEqualStrings("doc:c", result.hits[1].id);
    try std.testing.expectEqualStrings("1970-01-01T00:00:00.000000020Z", result.hits[0].sort_values[0].string);
    try std.testing.expectEqualStrings("doc:b", result.hits[0].sort_values[1].string);
    try std.testing.expectEqualStrings("1970-01-01T00:00:00.000000030Z", result.hits[1].sort_values[0].string);
    try std.testing.expectEqualStrings("doc:c", result.hits[1].sort_values[1].string);
}

test "native sort runtime rejects cursor values that do not match mapping" {
    const alloc = std.testing.allocator;

    const templates = [_]runtime_schema_mod.DynamicTemplate{.{
        .name = "created_at",
        .path_match = "created_at",
        .mapping = .{
            .field_type = .datetime,
            .doc_values = true,
            .sortable = true,
            .analyzer = "keyword",
        },
    }};
    const schema = runtime_schema_mod.TableSchema{ .dynamic_templates = &templates };

    var hits = try alloc.alloc(types.SearchHit, 1);
    hits[0] = .{ .id = try alloc.dupe(u8, "doc:a"), .score = 1.0 };
    var result = types.SearchResult{
        .alloc = alloc,
        .hits = hits,
        .total_hits = 1,
        .graph_results = &.{},
    };
    defer result.deinit();

    const Loader = struct {
        fn load(_: ?*anyopaque, _: Allocator, _: types.SearchHit, field: []const u8) anyerror!?SortValue {
            try std.testing.expectEqualStrings("created_at", field);
            return .{ .integer = 20 };
        }
    };

    const order_by = [_]types.SortField{.{ .field = "created_at", .desc = false }};
    const cursor = [_]std.json.Value{ .{ .string = "not-a-date" }, .{ .string = "doc:a" } };
    try std.testing.expectError(error.InvalidQueryRequest, sortAndPageSearchResultInPlace(&result, .{
        .order_by = &order_by,
        .search_after = &cursor,
        .include_stored = false,
        .limit = 10,
    }, null, testUnexpectedLoadStoredCallback, .{
        .kind = .native_doc_values_top_n,
        .require_native = true,
        .runtime_schema = schema,
    }, .{
        .require_native = true,
        .load = Loader.load,
    }));
}

test "native datetime sort rejects values that cannot serialize as timestamps" {
    const alloc = std.testing.allocator;

    const templates = [_]runtime_schema_mod.DynamicTemplate{.{
        .name = "created_at",
        .path_match = "created_at",
        .mapping = .{
            .field_type = .datetime,
            .doc_values = true,
            .sortable = true,
            .analyzer = "keyword",
        },
    }};
    const schema = runtime_schema_mod.TableSchema{ .dynamic_templates = &templates };

    var hits = try alloc.alloc(types.SearchHit, 1);
    hits[0] = .{ .id = try alloc.dupe(u8, "doc:a"), .score = 1.0 };
    var result = types.SearchResult{
        .alloc = alloc,
        .hits = hits,
        .total_hits = 1,
        .graph_results = &.{},
    };
    defer result.deinit();

    const Loader = struct {
        fn load(_: ?*anyopaque, _: Allocator, _: types.SearchHit, field: []const u8) anyerror!?SortValue {
            try std.testing.expectEqualStrings("created_at", field);
            return .{ .number = 20.5 };
        }
    };

    const order_by = [_]types.SortField{.{ .field = "created_at", .desc = false }};
    try std.testing.expectError(error.UnsupportedQueryRequest, sortAndPageSearchResultInPlace(&result, .{
        .order_by = &order_by,
        .include_stored = false,
        .limit = 10,
    }, null, testUnexpectedLoadStoredCallback, .{
        .kind = .native_doc_values_top_n,
        .require_native = true,
        .runtime_schema = schema,
    }, .{
        .require_native = true,
        .load = Loader.load,
    }));
}

test "native doc values sort plan requires runtime schema" {
    const alloc = std.testing.allocator;

    var hits = try alloc.alloc(types.SearchHit, 1);
    hits[0] = .{ .id = try alloc.dupe(u8, "doc:a"), .score = 1.0 };
    var result = types.SearchResult{
        .alloc = alloc,
        .hits = hits,
        .total_hits = 1,
        .graph_results = &.{},
    };
    defer result.deinit();

    const Loader = struct {
        fn load(_: ?*anyopaque, _: Allocator, _: types.SearchHit, field: []const u8) anyerror!?SortValue {
            try std.testing.expectEqualStrings("rank", field);
            return .{ .integer = 1 };
        }
    };

    const order_by = [_]types.SortField{.{ .field = "rank", .desc = false }};
    try std.testing.expectError(error.UnsupportedQueryRequest, sortAndPageSearchResultInPlace(&result, .{
        .order_by = &order_by,
        .include_stored = false,
        .limit = 1,
    }, null, testUnexpectedLoadStoredCallback, .{
        .kind = .native_doc_values_top_n,
        .require_native = true,
    }, .{
        .require_native = true,
        .load = Loader.load,
    }));
}

test "native doc values sort plan requires a native loader" {
    const alloc = std.testing.allocator;

    var hits = try alloc.alloc(types.SearchHit, 1);
    hits[0] = .{ .id = try alloc.dupe(u8, "doc:a"), .score = 1.0 };
    var result = types.SearchResult{
        .alloc = alloc,
        .hits = hits,
        .total_hits = 1,
        .graph_results = &.{},
    };
    defer result.deinit();

    const order_by = [_]types.SortField{.{ .field = "rank", .desc = false }};
    try std.testing.expectError(error.UnsupportedQueryRequest, sortAndPageSearchResultInPlace(&result, .{
        .order_by = &order_by,
        .include_stored = false,
        .limit = 1,
    }, null, testUnexpectedLoadStoredCallback, .{ .kind = .native_doc_values_top_n, .require_native = true }, null));
}

test "text field sort source loading happens only for selected missing hits" {
    const alloc = std.testing.allocator;

    const Harness = struct {
        load_count: usize = 0,
        project_count: usize = 0,

        fn loadStored(ctx: ?*anyopaque, load_alloc: Allocator, key: []const u8) anyerror!?[]u8 {
            const self: *@This() = @ptrCast(@alignCast(ctx orelse return error.InvalidArgument));
            self.load_count += 1;
            return try std.fmt.allocPrint(load_alloc, "{{\"id\":\"{s}\"}}", .{key});
        }

        fn projectStored(
            ctx: ?*anyopaque,
            project_alloc: Allocator,
            _: types.SearchRequest,
            doc_key: []const u8,
            raw: []const u8,
        ) anyerror![]u8 {
            const self: *@This() = @ptrCast(@alignCast(ctx orelse return error.InvalidArgument));
            self.project_count += 1;
            try std.testing.expect(std.mem.indexOf(u8, raw, doc_key) != null);
            return try project_alloc.dupe(u8, raw);
        }
    };

    var harness = Harness{};
    var hits = try alloc.alloc(types.SearchHit, 2);
    hits[0] = .{
        .id = try alloc.dupe(u8, "doc:a"),
        .stored_data = try alloc.dupe(u8, "{\"id\":\"doc:a\"}"),
    };
    hits[1] = .{ .id = try alloc.dupe(u8, "doc:b") };
    var result = types.SearchResult{
        .alloc = alloc,
        .hits = hits,
        .total_hits = 2,
        .graph_results = &.{},
    };
    defer result.deinit();

    const profile = try loadMissingProjectedTextHitDocuments(alloc, .{}, .{
        .ctx = &harness,
        .text_index_entry = undefined,
        .text_index_is_chunk_backed = undefined,
        .search_match_all = undefined,
        .project_stored_search = Harness.projectStored,
        .load_stored = Harness.loadStored,
        .postprocess = undefined,
    }, result.hits);

    try std.testing.expectEqual(@as(usize, 1), harness.load_count);
    try std.testing.expectEqual(@as(usize, 1), harness.project_count);
    try std.testing.expectEqual(@as(usize, 1), profile.requested_count);
    try std.testing.expectEqual(@as(usize, 1), profile.loaded_count);
    try std.testing.expectEqual(@as(usize, 1), profile.batch_count);
    try std.testing.expect(result.hits[0].stored_data != null);
    try std.testing.expect(result.hits[1].stored_data != null);
    try std.testing.expectEqualStrings("{\"id\":\"doc:b\"}", result.hits[1].stored_data.?);
}

fn testMatchAllExecutor(ctx: *const TestMatchAllCtx) MatchAllExecutor {
    return .{
        .ctx = @constCast(ctx),
        .collect_candidates = testCollectMatchAllCandidatesCallback,
        .collect_candidates_stream = testStreamMatchAllCandidatesCallback,
        .text_index_entry = testTextIndexEntryCallback,
        .resolve_doc_set_doc_ids = testResolveDocSetDocIdsCallback,
        .resolve_doc_ids_to_doc_set = testResolveDocIdsToDocSetCallback,
        .live_filter_doc_set = testLiveFilterDocSetCallback,
        .load_projected_document = testMatchAllLoadProjectedCallback,
        .load_projected_documents = testMatchAllLoadProjectedManyCallback,
        .load_stored = testMatchAllLoadStoredCallback,
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

test "match_all streaming collection stops after exact sort budget" {
    const alloc = std.testing.allocator;
    const encoded = [_][]u8{
        try internal_keys.documentKeyAlloc(alloc, "doc:a"),
        try internal_keys.documentKeyAlloc(alloc, "doc:b"),
        try internal_keys.documentKeyAlloc(alloc, "doc:c"),
    };
    defer for (encoded) |key| alloc.free(key);

    const Harness = struct {
        keys: []const []const u8,
        visited: usize = 0,
        in_scan_callback: bool = false,
        expired_checks: usize = 0,
        ordinal_lookups: usize = 0,

        fn scanStoreRange(
            _: ?*anyopaque,
            _: Allocator,
            _: []const u8,
            _: []const u8,
        ) anyerror![]docstore_mod.OwnedKVPair {
            return error.UnexpectedTestCall;
        }

        fn scanStoreRangeWithContext(
            ctx: ?*anyopaque,
            _: []const u8,
            _: []const u8,
            scan_ctx: ?*anyopaque,
            callback: docstore_mod.DocStore.ScanWithContextCallback,
        ) anyerror!void {
            const self: *@This() = @ptrCast(@alignCast(ctx orelse return error.InvalidArgument));
            self.in_scan_callback = true;
            defer self.in_scan_callback = false;
            for (self.keys) |key| {
                self.visited += 1;
                if (try callback(scan_ctx, key, "{}") == .stop) return;
            }
        }

        fn isExpiredKey(ctx: ?*anyopaque, _: Allocator, _: []const u8) anyerror!bool {
            const self: *@This() = @ptrCast(@alignCast(ctx orelse return error.InvalidArgument));
            try std.testing.expect(!self.in_scan_callback);
            self.expired_checks += 1;
            return false;
        }

        fn lookupDocOrdinal(ctx: ?*anyopaque, _: Allocator, _: []const u8, _: ?u64) anyerror!?doc_set.DocOrdinal {
            const self: *@This() = @ptrCast(@alignCast(ctx orelse return error.InvalidArgument));
            try std.testing.expect(!self.in_scan_callback);
            self.ordinal_lookups += 1;
            return @intCast(self.ordinal_lookups);
        }
    };

    var harness = Harness{ .keys = &encoded };
    try std.testing.expectError(error.QueryCandidateBudgetExceeded, collectMatchAllCandidatesWithOptions(alloc, .{}, .{
        .ctx = &harness,
        .scan_store_range = Harness.scanStoreRange,
        .scan_store_range_with_context = Harness.scanStoreRangeWithContext,
        .is_expired_key = Harness.isExpiredKey,
        .lookup_doc_ordinal = Harness.lookupDocOrdinal,
    }, .{
        .candidate_limit = 1,
        .scan_batch_size = 2,
    }));
    try std.testing.expectEqual(@as(usize, 2), harness.visited);
    try std.testing.expectEqual(@as(usize, 2), harness.expired_checks);
    try std.testing.expectEqual(@as(usize, 2), harness.ordinal_lookups);
}

test "match_all id stream search_after skips cursor document child records" {
    const alloc = std.testing.allocator;
    const encoded = [_][]u8{
        try internal_keys.documentKeyAlloc(alloc, "doc:a"),
        try internal_keys.ttlKeyAlloc(alloc, "doc:a"),
        try internal_keys.documentKeyAlloc(alloc, "doc:b"),
    };
    defer for (encoded) |key| alloc.free(key);

    const Harness = struct {
        keys: []const []const u8,
        visited: usize = 0,

        fn scanStoreRange(
            _: ?*anyopaque,
            _: Allocator,
            _: []const u8,
            _: []const u8,
        ) anyerror![]docstore_mod.OwnedKVPair {
            return error.UnexpectedTestCall;
        }

        fn scanStoreRangeWithContext(
            ctx: ?*anyopaque,
            lower: []const u8,
            upper: []const u8,
            scan_ctx: ?*anyopaque,
            callback: docstore_mod.DocStore.ScanWithContextCallback,
        ) anyerror!void {
            const self: *@This() = @ptrCast(@alignCast(ctx orelse return error.InvalidArgument));
            for (self.keys) |key| {
                if (std.mem.order(u8, key, lower) == .lt) continue;
                if (upper.len > 0 and std.mem.order(u8, key, upper) != .lt) continue;
                self.visited += 1;
                if (try callback(scan_ctx, key, "{}") == .stop) return;
            }
        }

        fn isExpiredKey(_: ?*anyopaque, _: Allocator, _: []const u8) anyerror!bool {
            return false;
        }

        fn lookupDocOrdinal(_: ?*anyopaque, _: Allocator, _: []const u8, _: ?u64) anyerror!?doc_set.DocOrdinal {
            return null;
        }
    };

    const ConsumerCtx = struct {
        alloc: Allocator,
        seen: usize = 0,
    };
    const Consumer = struct {
        fn consume(ctx: ?*anyopaque, candidate: MatchAllCandidate) anyerror!void {
            const consumer_ctx: *ConsumerCtx = @ptrCast(@alignCast(ctx orelse return error.InvalidArgument));
            var owned = candidate;
            defer owned.deinit(consumer_ctx.alloc);
            try std.testing.expectEqualStrings("doc:b", owned.id);
            consumer_ctx.seen += 1;
        }
    };

    var harness = Harness{ .keys = &encoded };
    var consumer_ctx = ConsumerCtx{ .alloc = alloc };
    const stats = try streamMatchAllCandidatesWithOptions(alloc, .{}, .{
        .ctx = &harness,
        .scan_store_range = Harness.scanStoreRange,
        .scan_store_range_with_context = Harness.scanStoreRangeWithContext,
        .is_expired_key = Harness.isExpiredKey,
        .lookup_doc_ordinal = Harness.lookupDocOrdinal,
    }, .{
        .primary_key_start_after = "doc:a",
        .stop_after_accepted = 1,
        .scan_batch_size = 4,
    }, &consumer_ctx, Consumer.consume);

    try std.testing.expectEqual(@as(usize, 1), harness.visited);
    try std.testing.expectEqual(@as(usize, 1), consumer_ctx.seen);
    try std.testing.expectEqual(@as(usize, 1), stats.accepted_count);
    try std.testing.expect(stats.stopped_early);
}

test "match_all id stream search_before excludes cursor document child records" {
    const alloc = std.testing.allocator;
    const encoded = [_][]u8{
        try internal_keys.documentKeyAlloc(alloc, "doc:a"),
        try internal_keys.documentKeyAlloc(alloc, "doc:b"),
        try internal_keys.ttlKeyAlloc(alloc, "doc:b"),
        try internal_keys.documentKeyAlloc(alloc, "doc:c"),
    };
    defer for (encoded) |key| alloc.free(key);

    const Harness = struct {
        keys: []const []const u8,
        visited: usize = 0,

        fn scanStoreRange(
            _: ?*anyopaque,
            _: Allocator,
            _: []const u8,
            _: []const u8,
        ) anyerror![]docstore_mod.OwnedKVPair {
            return error.UnexpectedTestCall;
        }

        fn scanStoreRangeWithContext(
            ctx: ?*anyopaque,
            lower: []const u8,
            upper: []const u8,
            scan_ctx: ?*anyopaque,
            callback: docstore_mod.DocStore.ScanWithContextCallback,
        ) anyerror!void {
            const self: *@This() = @ptrCast(@alignCast(ctx orelse return error.InvalidArgument));
            for (self.keys) |key| {
                if (std.mem.order(u8, key, lower) == .lt) continue;
                if (upper.len > 0 and std.mem.order(u8, key, upper) != .lt) continue;
                self.visited += 1;
                if (try callback(scan_ctx, key, "{}") == .stop) return;
            }
        }

        fn isExpiredKey(_: ?*anyopaque, _: Allocator, _: []const u8) anyerror!bool {
            return false;
        }

        fn lookupDocOrdinal(_: ?*anyopaque, _: Allocator, _: []const u8, _: ?u64) anyerror!?doc_set.DocOrdinal {
            return null;
        }
    };

    const ConsumerCtx = struct {
        alloc: Allocator,
        seen: usize = 0,
    };
    const Consumer = struct {
        fn consume(ctx: ?*anyopaque, candidate: MatchAllCandidate) anyerror!void {
            const consumer_ctx: *ConsumerCtx = @ptrCast(@alignCast(ctx orelse return error.InvalidArgument));
            var owned = candidate;
            defer owned.deinit(consumer_ctx.alloc);
            try std.testing.expectEqualStrings("doc:a", owned.id);
            consumer_ctx.seen += 1;
        }
    };

    var harness = Harness{ .keys = &encoded };
    var consumer_ctx = ConsumerCtx{ .alloc = alloc };
    const stats = try streamMatchAllCandidatesWithOptions(alloc, .{}, .{
        .ctx = &harness,
        .scan_store_range = Harness.scanStoreRange,
        .scan_store_range_with_context = Harness.scanStoreRangeWithContext,
        .is_expired_key = Harness.isExpiredKey,
        .lookup_doc_ordinal = Harness.lookupDocOrdinal,
    }, .{
        .primary_key_stop_before = "doc:b",
        .scan_batch_size = 4,
    }, &consumer_ctx, Consumer.consume);

    try std.testing.expectEqual(@as(usize, 1), harness.visited);
    try std.testing.expectEqual(@as(usize, 1), consumer_ctx.seen);
    try std.testing.expectEqual(@as(usize, 1), stats.accepted_count);
    try std.testing.expect(!stats.stopped_early);
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

test "match_all rejects expired execution deadline" {
    const alloc = std.testing.allocator;
    const ctx = TestMatchAllCtx{
        .ids = &.{ "doc:a", "doc:b", "doc:c" },
        .ordinals = &.{ 1, 2, 3 },
    };

    try std.testing.expectError(error.Timeout, searchMatchAll(alloc, .{
        .include_stored = false,
        .limit = 10,
        .execution_deadline_ns = platform_time.monotonicNs(),
    }, testMatchAllExecutor(&ctx)));
}

test "match_all applies stored pattern filters before paging" {
    const alloc = std.testing.allocator;
    const ctx = TestMatchAllCtx{
        .ids = &.{ "doc:a", "doc:b", "doc:c" },
        .ordinals = &.{ 1, 2, 3 },
    };

    var executor = testMatchAllExecutor(&ctx);
    executor.live_filter_doc_set = null;
    var result = try searchMatchAll(alloc, .{
        .filter_query_json = "{\"term\":{\"tier\":\"gold\"}}",
        .include_stored = false,
        .limit = 1,
        .offset = 1,
    }, executor);
    defer result.deinit();

    try std.testing.expectEqual(@as(u32, 2), result.total_hits);
    try std.testing.expectEqual(@as(usize, 1), result.hits.len);
    try std.testing.expectEqualStrings("doc:c", result.hits[0].id);
}

test "match_all rejects field sort without native doc values" {
    const alloc = std.testing.allocator;
    var collect_count: usize = 0;
    const ctx = TestMatchAllCtx{
        .ids = &.{ "doc:a", "doc:b", "doc:c" },
        .ordinals = &.{ 1, 2, 3 },
        .collect_count = &collect_count,
    };
    const order_by = [_]types.SortField{
        .{ .field = "rank" },
        .{ .field = "_id" },
    };

    var executor = testMatchAllExecutor(&ctx);
    executor.live_filter_doc_set = null;
    try std.testing.expectError(error.UnsupportedQueryRequest, searchMatchAll(alloc, .{
        .order_by = &order_by,
        .include_stored = false,
        .limit = 2,
    }, executor));
    try std.testing.expectEqual(@as(usize, 0), collect_count);
}

test "match_all supports id-only sort without native doc values" {
    const alloc = std.testing.allocator;
    var collect_count: usize = 0;
    var stream_collect_count: usize = 0;
    var stream_accepted_count: usize = 0;
    const ctx = TestMatchAllCtx{
        .ids = &.{ "doc:a", "doc:b", "doc:c" },
        .ordinals = &.{ 1, 2, 3 },
        .collect_count = &collect_count,
        .stream_collect_count = &stream_collect_count,
        .stream_accepted_count = &stream_accepted_count,
    };
    const order_by = [_]types.SortField{.{ .field = "_id" }};

    var executor = testMatchAllExecutor(&ctx);
    executor.live_filter_doc_set = null;
    const req = types.SearchRequest{
        .order_by = &order_by,
        .include_stored = false,
        .offset = 1,
        .limit = 1,
    };
    const plan = try planMatchAllSortBeforeCandidatesAlloc(alloc, req, executor);
    try std.testing.expectEqual(SortExecutionPlanKind.id_seek, plan.kind);
    try std.testing.expectEqual(SortPlanSource.primary_key_scan, sortExecutionPlanSource(plan));
    try std.testing.expectEqual(SortPlanCursorSupport.segment_seek, sortExecutionPlanCursorSupport(plan));

    var result = try searchMatchAll(alloc, req, executor);
    defer result.deinit();

    try std.testing.expectEqual(@as(u32, 2), result.total_hits);
    try std.testing.expectEqual(types.TotalHitsRelation.gte, result.total_hits_relation);
    try std.testing.expectEqual(@as(usize, 1), result.hits.len);
    try std.testing.expectEqualStrings("doc:b", result.hits[0].id);
    try std.testing.expectEqualStrings("doc:b", result.hits[0].sort_values[0].string);
    try std.testing.expectEqual(@as(usize, 0), collect_count);
    try std.testing.expectEqual(@as(usize, 1), stream_collect_count);
    try std.testing.expectEqual(@as(usize, 2), stream_accepted_count);
}

test "match_all id-only sort seeks after cursor without scanning prior ids" {
    const alloc = std.testing.allocator;
    var collect_count: usize = 0;
    var stream_collect_count: usize = 0;
    var stream_accepted_count: usize = 0;
    const ctx = TestMatchAllCtx{
        .ids = &.{ "doc:a", "doc:b", "doc:c" },
        .ordinals = &.{ 1, 2, 3 },
        .collect_count = &collect_count,
        .stream_collect_count = &stream_collect_count,
        .stream_accepted_count = &stream_accepted_count,
    };
    const order_by = [_]types.SortField{.{ .field = "_id" }};
    const cursor = [_]std.json.Value{.{ .string = "doc:a" }};

    var executor = testMatchAllExecutor(&ctx);
    executor.live_filter_doc_set = null;
    const req = types.SearchRequest{
        .order_by = &order_by,
        .search_after = &cursor,
        .include_stored = false,
        .limit = 1,
    };
    const plan = try planMatchAllSortBeforeCandidatesAlloc(alloc, req, executor);
    try std.testing.expectEqual(SortExecutionPlanKind.id_seek, plan.kind);
    try std.testing.expectEqual(SortPlanSource.primary_key_scan, sortExecutionPlanSource(plan));
    try std.testing.expectEqual(SortPlanCursorSupport.segment_seek, sortExecutionPlanCursorSupport(plan));

    var result = try searchMatchAll(alloc, req, executor);
    defer result.deinit();

    try std.testing.expectEqual(@as(u32, 1), result.total_hits);
    try std.testing.expectEqual(types.TotalHitsRelation.gte, result.total_hits_relation);
    try std.testing.expectEqual(@as(usize, 1), result.hits.len);
    try std.testing.expectEqualStrings("doc:b", result.hits[0].id);
    try std.testing.expectEqualStrings("doc:b", result.hits[0].sort_values[0].string);
    try std.testing.expectEqual(@as(usize, 0), collect_count);
    try std.testing.expectEqual(@as(usize, 1), stream_collect_count);
    try std.testing.expectEqual(@as(usize, 1), stream_accepted_count);
}

test "match_all id-only search_before bounds primary key stream at cursor" {
    const alloc = std.testing.allocator;
    var collect_count: usize = 0;
    var stream_collect_count: usize = 0;
    var stream_accepted_count: usize = 0;
    const ctx = TestMatchAllCtx{
        .ids = &.{ "doc:a", "doc:b", "doc:c" },
        .ordinals = &.{ 1, 2, 3 },
        .collect_count = &collect_count,
        .stream_collect_count = &stream_collect_count,
        .stream_accepted_count = &stream_accepted_count,
    };
    const order_by = [_]types.SortField{.{ .field = "_id" }};
    const cursor = [_]std.json.Value{.{ .string = "doc:c" }};

    var executor = testMatchAllExecutor(&ctx);
    executor.live_filter_doc_set = null;
    var result = try searchMatchAll(alloc, .{
        .order_by = &order_by,
        .search_before = &cursor,
        .include_stored = false,
        .limit = 1,
    }, executor);
    defer result.deinit();

    try std.testing.expectEqual(@as(u32, 2), result.total_hits);
    try std.testing.expectEqual(types.TotalHitsRelation.gte, result.total_hits_relation);
    try std.testing.expectEqual(@as(usize, 1), result.hits.len);
    try std.testing.expectEqualStrings("doc:b", result.hits[0].id);
    try std.testing.expectEqualStrings("doc:b", result.hits[0].sort_values[0].string);
    try std.testing.expectEqual(@as(usize, 0), collect_count);
    try std.testing.expectEqual(@as(usize, 1), stream_collect_count);
    try std.testing.expectEqual(@as(usize, 2), stream_accepted_count);
}

test "match_all ordered source loads only selected hits" {
    const alloc = std.testing.allocator;
    var collect_count: usize = 0;
    var stream_collect_count: usize = 0;
    var stream_accepted_count: usize = 0;
    var projected_load_count: usize = 0;
    var projected_batch_count: usize = 0;
    var projected_batch_doc_count: usize = 0;
    const ctx = TestMatchAllCtx{
        .ids = &.{ "doc:a", "doc:b", "doc:c" },
        .ordinals = &.{ 1, 2, 3 },
        .collect_count = &collect_count,
        .stream_collect_count = &stream_collect_count,
        .stream_accepted_count = &stream_accepted_count,
        .projected_load_count = &projected_load_count,
        .projected_batch_count = &projected_batch_count,
        .projected_batch_doc_count = &projected_batch_doc_count,
    };
    const order_by = [_]types.SortField{.{ .field = "_id" }};

    var executor = testMatchAllExecutor(&ctx);
    executor.live_filter_doc_set = null;
    var result = try searchMatchAll(alloc, .{
        .order_by = &order_by,
        .include_stored = true,
        .offset = 1,
        .limit = 1,
    }, executor);
    defer result.deinit();

    try std.testing.expectEqual(@as(usize, 1), result.hits.len);
    try std.testing.expectEqualStrings("doc:b", result.hits[0].id);
    try std.testing.expect(result.hits[0].stored_data != null);
    try std.testing.expectEqual(@as(usize, 0), projected_load_count);
    try std.testing.expectEqual(@as(usize, 1), projected_batch_count);
    try std.testing.expectEqual(@as(usize, 1), projected_batch_doc_count);
    try std.testing.expectEqual(@as(usize, 0), collect_count);
    try std.testing.expectEqual(@as(usize, 1), stream_collect_count);
    try std.testing.expectEqual(@as(usize, 2), stream_accepted_count);
}

test "match_all rejects invalid sort cursor contract" {
    const alloc = std.testing.allocator;
    const ctx = TestMatchAllCtx{
        .ids = &.{ "doc:a", "doc:b", "doc:c" },
        .ordinals = &.{ 1, 2, 3 },
    };
    const order_by = [_]types.SortField{
        .{ .field = "rank" },
        .{ .field = "_id" },
    };
    const short_cursor = [_]std.json.Value{.{ .integer = 1 }};
    const full_cursor = [_]std.json.Value{
        .{ .integer = 1 },
        .{ .string = "doc:b" },
    };
    const bad_id_cursor = [_]std.json.Value{
        .{ .integer = 1 },
        .{ .integer = 9 },
    };
    var bad_array_value = std.json.Array.init(alloc);
    defer bad_array_value.deinit();
    const bad_array_cursor = [_]std.json.Value{
        .{ .array = bad_array_value },
        .{ .string = "doc:b" },
    };
    var bad_object_value = std.json.ObjectMap.init(alloc);
    defer bad_object_value.deinit();
    const bad_object_cursor = [_]std.json.Value{
        .{ .object = bad_object_value },
        .{ .string = "doc:b" },
    };

    var executor = testMatchAllExecutor(&ctx);
    executor.live_filter_doc_set = null;
    try std.testing.expectError(error.InvalidQueryRequest, searchMatchAll(alloc, .{
        .order_by = &order_by,
        .search_after = &short_cursor,
        .include_stored = false,
        .limit = 2,
    }, executor));
    try std.testing.expectError(error.InvalidQueryRequest, searchMatchAll(alloc, .{
        .order_by = &order_by,
        .search_after = &full_cursor,
        .search_before = &full_cursor,
        .include_stored = false,
        .limit = 2,
    }, executor));
    try std.testing.expectError(error.InvalidQueryRequest, searchMatchAll(alloc, .{
        .order_by = &order_by,
        .search_after = &bad_id_cursor,
        .include_stored = false,
        .limit = 2,
    }, executor));
    try std.testing.expectError(error.InvalidQueryRequest, searchMatchAll(alloc, .{
        .order_by = &order_by,
        .search_after = &bad_array_cursor,
        .include_stored = false,
        .limit = 2,
    }, executor));
    try std.testing.expectError(error.InvalidQueryRequest, searchMatchAll(alloc, .{
        .order_by = &order_by,
        .search_before = &bad_object_cursor,
        .include_stored = false,
        .limit = 2,
    }, executor));
}

test "match_all rejects dotted field sort without native doc values" {
    const alloc = std.testing.allocator;
    const ctx = TestMatchAllCtx{
        .ids = &.{ "doc:a", "doc:b", "doc:c" },
        .ordinals = &.{ 1, 2, 3 },
    };
    const order_by = [_]types.SortField{
        .{ .field = "nested.created_at", .desc = true },
        .{ .field = "_id" },
    };

    var executor = testMatchAllExecutor(&ctx);
    executor.live_filter_doc_set = null;
    try std.testing.expectError(error.UnsupportedQueryRequest, searchMatchAll(alloc, .{
        .order_by = &order_by,
        .include_stored = false,
        .limit = 2,
    }, executor));
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
    }, false);

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
    }, true);

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
    const namespace_a = doc_identity.Namespace{ .table_id = 1, .shard_id = 11, .range_id = 111 };
    const namespace_b = doc_identity.Namespace{ .table_id = 1, .shard_id = 11, .range_id = 112 };
    const namespace_same_tag_different_tuple = doc_identity.Namespace{ .table_id = 111, .shard_id = 0, .range_id = 0 };
    try cache.putSharedCloneAlloc(alloc, "{\"term\":{\"field\":\"status\",\"value\":\"open\"}}", namespace_a, 7, &source);

    try std.testing.expect(cache.get("{\"term\":{\"field\":\"status\",\"value\":\"open\"}}", 7) == null);
    try std.testing.expect(cache.getShared("{\"term\":{\"field\":\"status\",\"value\":\"open\"}}", namespace_b, 7) == null);
    try std.testing.expect(cache.getShared("{\"term\":{\"field\":\"status\",\"value\":\"open\"}}", namespace_same_tag_different_tuple, 7) == null);
    try std.testing.expect(cache.getShared("{\"term\":{\"field\":\"status\",\"value\":\"open\"}}", namespace_a, 8) == null);
    const cached = cache.getShared("{\"term\":{\"field\":\"status\",\"value\":\"open\"}}", namespace_a, 7) orelse return error.TestUnexpectedResult;
    try std.testing.expect(cached.containsOrdinal(9));
    try std.testing.expect(cached.containsOrdinal(10));

    var next_generation_source = try doc_set.fromOrdinalsAlloc(alloc, &.{11});
    defer next_generation_source.deinit(alloc);
    try cache.putSharedCloneAlloc(alloc, "{\"term\":{\"field\":\"status\",\"value\":\"open\"}}", namespace_a, 8, &next_generation_source);
    const cached_generation_7 = cache.getShared("{\"term\":{\"field\":\"status\",\"value\":\"open\"}}", namespace_a, 7) orelse return error.TestUnexpectedResult;
    try std.testing.expect(cached_generation_7.containsOrdinal(9));
    try std.testing.expect(!cached_generation_7.containsOrdinal(11));
    const cached_generation_8 = cache.getShared("{\"term\":{\"field\":\"status\",\"value\":\"open\"}}", namespace_a, 8) orelse return error.TestUnexpectedResult;
    try std.testing.expect(cached_generation_8.containsOrdinal(11));
    try std.testing.expect(!cached_generation_8.containsOrdinal(9));
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
        .merge_config = .{ .strategy = .rsf },
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

test "composed search rejects exact field sort across embedding sources" {
    const alloc = std.testing.allocator;
    const dense_vector = [_]f32{1.0};
    const order_by = [_]types.SortField{
        .{ .field = "created_at", .desc = true },
        .{ .field = "_id" },
    };

    try std.testing.expectError(error.UnsupportedQueryRequest, searchComposed(alloc, .{
        .dense = .{ .vector = &dense_vector, .k = 1 },
        .order_by = &order_by,
        .include_stored = false,
    }, undefined));
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
