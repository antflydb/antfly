// Copyright 2026 Antfly, Inc.
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

const std = @import("std");
const Allocator = std.mem.Allocator;
const types = @import("types.zig");
const search_results = @import("search_results.zig");

pub const SearchResult = search_results.SearchResult;
pub const SearchResults = search_results.SearchResults;

pub const SearchRequest = struct {
    query: []const f32,
    k: usize,
    rerank_k: ?usize = null,
    search_width: ?u32 = null,
    epsilon: ?f32 = null,
    load_metadata: bool = true,
    filter_prefix: []const u8 = "",
    distance_over: ?f32 = null,
    distance_under: ?f32 = null,
    filter_ids: []const u64 = &.{},
    exclude_ids: []const u64 = &.{},
};

pub const SearchProfile = struct {
    total_ns: u64 = 0,
    setup_ns: u64 = 0,
    runtime_txn_ns: u64 = 0,
    scratch_acquire_ns: u64 = 0,
    root_load_ns: u64 = 0,
    node_cache_lookup_ns: u64 = 0,
    node_cache_miss_ns: u64 = 0,
    node_cache_misses: u64 = 0,
    quantized_cache_lookup_ns: u64 = 0,
    quantized_cache_miss_ns: u64 = 0,
    quantized_cache_misses: u64 = 0,
    child_expand_ns: u64 = 0,
    leaf_score_ns: u64 = 0,
    rerank_ns: u64 = 0,
    rerank_prepare_ns: u64 = 0,
    rerank_select_positions_ns: u64 = 0,
    rerank_vector_load_ns: u64 = 0,
    rerank_prefetch_ns: u64 = 0,
    rerank_metadata_lookup_ns: u64 = 0,
    rerank_artifact_key_ns: u64 = 0,
    rerank_artifact_read_ns: u64 = 0,
    rerank_artifact_decode_ns: u64 = 0,
    rerank_artifact_distance_ns: u64 = 0,
    rerank_lsm_cache_hits: u64 = 0,
    rerank_lsm_cache_misses: u64 = 0,
    rerank_vector_view_ns: u64 = 0,
    rerank_distance_ns: u64 = 0,
    rerank_apply_ns: u64 = 0,
    rerank_resort_ns: u64 = 0,
    rerank_finalize_ns: u64 = 0,
    rerank_metadata_ns: u64 = 0,
    nodes_visited: u64 = 0,
    leaves_explored: u64 = 0,
    approx_nodes_expanded: u64 = 0,
    approx_leaves_scored: u64 = 0,
    approx_vectors_scored: u64 = 0,
    exact_vectors_scored: u64 = 0,
    reranked_vectors: u64 = 0,
    approx_candidate_count: u64 = 0,
    rerank_candidate_count: u64 = 0,
    ambiguous_top_k_pairs: u64 = 0,
    ambiguous_boundary_pairs: u64 = 0,
    ambiguous_distance_over_hits: u64 = 0,
    ambiguous_distance_under_hits: u64 = 0,
    full_rerank_due_to_threshold: bool = false,
    top_k_count: u64 = 0,
    min_distance_gap_top_k: f32 = 0,
    min_interval_gap_top_k: f32 = 0,
    closest_pair_top_k: ?DebugPair = null,
    boundary_pair: ?DebugPair = null,
    boundary_tail_error_avg: f32 = 0,
    boundary_tail_error_max: f32 = 0,
    boundary_tail_distance_gap_avg: f32 = 0,
    boundary_tail_distance_gap_min: f32 = 0,
    boundary_tail_distance_gap_max: f32 = 0,
    boundary_tail_interval_gap_avg: f32 = 0,
    boundary_tail_interval_gap_min: f32 = 0,
    boundary_tail_interval_gap_max: f32 = 0,
    approx_top_count: u64 = 0,
    approx_top: [5]DebugHit = .{ .{}, .{}, .{}, .{}, .{} },
};

pub const ProfiledSearchResults = struct {
    results: SearchResults,
    profile: SearchProfile,
};

pub const DebugLeafScore = struct {
    vector_id: u64,
    approx_distance: f32,
    error_bound: f32,
    exact_distance: f32,
};

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

pub const DebugNodeDistance = struct {
    node_id: u64,
    distance: f32,
};

pub const IndexStats = struct {
    dims: u32,
    active_count: u64,
    node_count: u64,
    root_node: u64,
    branching_factor: u32,
    leaf_size: u32,
};

pub const HBCDebugNode = struct {
    id: u64,
    is_leaf: bool,
    parent: u64,
    level: u16,
    children: []u64,
    members: []u64,

    pub fn deinit(self: *HBCDebugNode, alloc: Allocator) void {
        alloc.free(self.children);
        alloc.free(self.members);
    }
};

pub const RequestFilterState = struct {
    include: std.AutoHashMapUnmanaged(u64, void) = .empty,
    exclude: std.AutoHashMapUnmanaged(u64, void) = .empty,

    pub fn init(alloc: Allocator, req: SearchRequest) !RequestFilterState {
        var state = RequestFilterState{};
        errdefer state.deinit(alloc);

        if (req.filter_ids.len > 0) {
            try state.include.ensureTotalCapacity(alloc, @intCast(req.filter_ids.len));
            for (req.filter_ids) |id| try state.include.put(alloc, id, {});
        }
        if (req.exclude_ids.len > 0) {
            try state.exclude.ensureTotalCapacity(alloc, @intCast(req.exclude_ids.len));
            for (req.exclude_ids) |id| try state.exclude.put(alloc, id, {});
        }
        return state;
    }

    pub fn deinit(self: *RequestFilterState, alloc: Allocator) void {
        self.include.deinit(alloc);
        self.exclude.deinit(alloc);
        self.* = .{};
    }

    pub fn rejects(self: *const RequestFilterState, vector_id: u64) bool {
        if (self.exclude.count() > 0 and self.exclude.contains(vector_id)) return true;
        if (self.include.count() > 0 and !self.include.contains(vector_id)) return true;
        return false;
    }

    pub fn isTrivial(self: *const RequestFilterState) bool {
        return self.include.count() == 0 and self.exclude.count() == 0;
    }
};

pub fn candidateLessThan(_: void, a: types.PriorityItem, b: types.PriorityItem) std.math.Order {
    const a_score = candidatePriorityScore(a);
    const b_score = candidatePriorityScore(b);
    if (std.math.isNan(a_score)) {
        if (std.math.isNan(b_score)) return std.math.order(a.id, b.id);
        return .gt;
    }
    if (std.math.isNan(b_score)) return .lt;
    const order = std.math.order(a_score, b_score);
    if (order != .eq) return order;
    return std.math.order(a.id, b.id);
}

fn candidatePriorityScore(item: types.PriorityItem) f32 {
    return item.distance - item.error_bound;
}

test "candidateLessThan gives NaN scores deterministic lowest priority" {
    const finite = types.PriorityItem{ .id = 10, .distance = 1, .error_bound = 0 };
    const nan_distance = types.PriorityItem{ .id = 20, .distance = std.math.nan(f32), .error_bound = 0 };
    const nan_bound = types.PriorityItem{ .id = 30, .distance = 1, .error_bound = std.math.nan(f32) };

    try std.testing.expectEqual(std.math.Order.lt, candidateLessThan({}, finite, nan_distance));
    try std.testing.expectEqual(std.math.Order.gt, candidateLessThan({}, nan_distance, finite));
    try std.testing.expectEqual(std.math.Order.lt, candidateLessThan({}, nan_distance, nan_bound));
}
