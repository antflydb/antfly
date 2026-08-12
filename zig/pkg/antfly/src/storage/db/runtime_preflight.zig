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

//! Compiler-boundary-safe runtime preflight result types.

const std = @import("std");
const Allocator = std.mem.Allocator;
const distributed_stats_mod = @import("../../search/distributed_stats.zig");

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

fn freeOwnedStringSlice(alloc: Allocator, values: []const []const u8) void {
    for (values) |value| alloc.free(value);
    if (values.len > 0) alloc.free(@constCast(values));
}
