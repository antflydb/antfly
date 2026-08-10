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

//! Query values shared by distributed control and the physical query engine.
//! Keep this module free of DB, index-manager, backend, and cache imports.

const types = @import("../types.zig");
const aggregations = @import("../aggregations.zig");
const doc_set = @import("../doc_set.zig");

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
    background_query: aggregations.BackgroundQuery,
    resolved_doc_filter: ?*const doc_set.ResolvedDocFilter = null,
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
    native_filter_candidate_count: u64 = 0,
    search_route: []const u8 = "",
    route_reason: []const u8 = "",
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

pub fn isTextQuery(query: types.Query) bool {
    return switch (query) {
        .match_none, .match_all, .phrase, .multi_phrase, .term, .fuzzy, .numeric_range, .date_range, .doc_id, .bool_field, .geo_distance, .geo_bbox, .term_range, .ip_range, .geo_shape, .match, .match_phrase, .prefix, .wildcard, .regexp => true,
        else => false,
    };
}

pub fn isDefaultMatchAll(query: types.Query) bool {
    return switch (query) {
        .match_all => true,
        else => false,
    };
}
