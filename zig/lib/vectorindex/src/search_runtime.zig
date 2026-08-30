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
const vec = @import("antfly_vector").vector;
const quantizer = @import("antfly_vector").quantizer;
const types = @import("types.zig");
const search_types = @import("search_types.zig");

pub const RerankLookup = struct {
    item_index: usize,
    vector_id: u64,
    key: [10]u8,
    // Captured before the authoritative read. Adapters with generation-safe
    // vector caching use it to reject a fill that crossed a write boundary.
    vector_cache_fill_epoch: ?u64 = null,
};

pub const CoverageMember = struct {
    vector_id: u64,
    leaf_id: u64,
};

pub const SearchScratch = struct {
    dims: usize,
    estimate: quantizer.RaBitQuantizer.EstimateScratch,
    transformed_query: []f32,
    centroid: []f32,
    vector: []f32,
    vector_batch: []f32,
    member_ids: []u64,
    vector_ids: []u64,
    metadata: []?[]const u8,
    flags: []bool,
    positions: []usize,
    lookups: []RerankLookup,
    key_views: [][]const u8,
    values: []?[]const u8,
    vector_views: [][]const f32,
    distances: []f32,
    error_bounds: []f32,
    score_bounds: []f32,
    flat_probes: []search_types.FlatCentroidProbe,
    flat_probe_merge: []search_types.FlatCentroidProbe,
    coverage_members: []CoverageMember,
    coverage_visited_words: []usize,

    pub fn init(alloc: Allocator, dims: usize, max_branching: usize, max_leaf: usize) !SearchScratch {
        const max_candidates = @max(max_branching, max_leaf);
        const vector_value_count = std.math.mul(usize, dims, max_candidates) catch return error.OutOfMemory;
        const estimate = try quantizer.RaBitQuantizer.EstimateScratch.init(alloc, dims);
        errdefer {
            var tmp = estimate;
            tmp.deinit(alloc);
        }
        const transformed_query = try alloc.alloc(f32, dims);
        errdefer alloc.free(transformed_query);
        const centroid = try alloc.alloc(f32, dims);
        errdefer alloc.free(centroid);
        const vector = try alloc.alloc(f32, dims);
        errdefer alloc.free(vector);
        const vector_batch = try alloc.alloc(f32, vector_value_count);
        errdefer alloc.free(vector_batch);
        const member_ids = try alloc.alloc(u64, max_leaf);
        errdefer alloc.free(member_ids);
        const vector_ids = try alloc.alloc(u64, max_candidates);
        errdefer alloc.free(vector_ids);
        const metadata = try alloc.alloc(?[]const u8, max_candidates);
        errdefer alloc.free(metadata);
        const flags = try alloc.alloc(bool, max_candidates);
        errdefer alloc.free(flags);
        const positions = try alloc.alloc(usize, max_candidates);
        errdefer alloc.free(positions);
        const lookups = try alloc.alloc(RerankLookup, max_candidates);
        errdefer alloc.free(lookups);
        const key_views = try alloc.alloc([]const u8, max_candidates);
        errdefer alloc.free(key_views);
        const values = try alloc.alloc(?[]const u8, max_candidates);
        errdefer alloc.free(values);
        const vector_views = try alloc.alloc([]const f32, max_candidates);
        errdefer alloc.free(vector_views);
        const distances = try alloc.alloc(f32, max_candidates);
        errdefer alloc.free(distances);
        const error_bounds = try alloc.alloc(f32, max_candidates);
        errdefer alloc.free(error_bounds);
        const score_bounds = try alloc.alloc(f32, max_candidates * 2);
        errdefer alloc.free(score_bounds);
        const flat_probes = try alloc.alloc(search_types.FlatCentroidProbe, 0);
        errdefer alloc.free(flat_probes);
        const flat_probe_merge = try alloc.alloc(search_types.FlatCentroidProbe, 0);
        errdefer alloc.free(flat_probe_merge);
        const coverage_members = try alloc.alloc(CoverageMember, 0);
        errdefer alloc.free(coverage_members);
        const coverage_visited_words = try alloc.alloc(usize, 0);
        return .{
            .dims = dims,
            .estimate = estimate,
            .transformed_query = transformed_query,
            .centroid = centroid,
            .vector = vector,
            .vector_batch = vector_batch,
            .member_ids = member_ids,
            .vector_ids = vector_ids,
            .metadata = metadata,
            .flags = flags,
            .positions = positions,
            .lookups = lookups,
            .key_views = key_views,
            .values = values,
            .vector_views = vector_views,
            .distances = distances,
            .error_bounds = error_bounds,
            .score_bounds = score_bounds,
            .flat_probes = flat_probes,
            .flat_probe_merge = flat_probe_merge,
            .coverage_members = coverage_members,
            .coverage_visited_words = coverage_visited_words,
        };
    }

    pub fn ensureLookupCapacity(self: *SearchScratch, alloc: Allocator, needed: usize) !void {
        if (self.lookups.len < needed) self.lookups = try alloc.realloc(self.lookups, needed);
        if (self.key_views.len < needed) self.key_views = try alloc.realloc(self.key_views, needed);
        if (self.values.len < needed) self.values = try alloc.realloc(self.values, needed);
    }

    pub fn ensureVectorFetchCapacity(self: *SearchScratch, alloc: Allocator, needed: usize) !void {
        const vector_value_count = std.math.mul(usize, self.dims, needed) catch return error.OutOfMemory;
        if (self.positions.len < needed) self.positions = try alloc.realloc(self.positions, needed);
        if (self.vector_ids.len < needed) self.vector_ids = try alloc.realloc(self.vector_ids, needed);
        if (self.metadata.len < needed) self.metadata = try alloc.realloc(self.metadata, needed);
        try self.ensureLookupCapacity(alloc, needed);
        if (self.vector_views.len < needed) self.vector_views = try alloc.realloc(self.vector_views, needed);
        if (self.distances.len < needed) self.distances = try alloc.realloc(self.distances, needed);
        if (self.error_bounds.len < needed) self.error_bounds = try alloc.realloc(self.error_bounds, needed);
        const score_scratch_needed = std.math.mul(usize, needed, 2) catch return error.OutOfMemory;
        if (self.score_bounds.len < score_scratch_needed) self.score_bounds = try alloc.realloc(self.score_bounds, score_scratch_needed);
        if (self.vector_batch.len < vector_value_count) self.vector_batch = try alloc.realloc(self.vector_batch, vector_value_count);
    }

    pub fn ensureRerankCapacity(self: *SearchScratch, alloc: Allocator, needed: usize) !void {
        if (self.flags.len < needed) self.flags = try alloc.realloc(self.flags, needed);
        try self.ensureVectorFetchCapacity(alloc, needed);
    }

    pub fn ensureMemberIdCapacity(self: *SearchScratch, alloc: Allocator, needed: usize) !void {
        if (self.member_ids.len < needed) self.member_ids = try alloc.realloc(self.member_ids, needed);
    }

    pub fn ensureVectorIdCapacity(self: *SearchScratch, alloc: Allocator, needed: usize) !void {
        if (self.vector_ids.len < needed) self.vector_ids = try alloc.realloc(self.vector_ids, needed);
    }

    pub fn ensureCoverageMemberCapacity(self: *SearchScratch, alloc: Allocator, needed: usize) !void {
        if (self.coverage_members.len < needed) self.coverage_members = try alloc.realloc(self.coverage_members, needed);
    }

    pub fn ensureFlatProbeCapacity(self: *SearchScratch, alloc: Allocator, needed: usize, needs_merge: bool) !void {
        if (self.flat_probes.len < needed) self.flat_probes = try alloc.realloc(self.flat_probes, needed);
        if (needs_merge and self.flat_probe_merge.len < needed) {
            self.flat_probe_merge = try alloc.realloc(self.flat_probe_merge, needed);
        }
    }

    pub fn projectedBytesWithFlatProbeCapacity(
        self: *const SearchScratch,
        needed: usize,
        needs_merge: bool,
        vector_fetch_needed: usize,
    ) !u64 {
        var projected = self.bytes();
        projected = try addSliceGrowthBytes(search_types.FlatCentroidProbe, projected, self.flat_probes.len, needed);
        if (needs_merge) {
            projected = try addSliceGrowthBytes(search_types.FlatCentroidProbe, projected, self.flat_probe_merge.len, needed);
        }
        projected = try addSliceGrowthBytes(usize, projected, self.positions.len, vector_fetch_needed);
        projected = try addSliceGrowthBytes(u64, projected, self.vector_ids.len, vector_fetch_needed);
        projected = try addSliceGrowthBytes(?[]const u8, projected, self.metadata.len, vector_fetch_needed);
        projected = try addSliceGrowthBytes(RerankLookup, projected, self.lookups.len, vector_fetch_needed);
        projected = try addSliceGrowthBytes([]const u8, projected, self.key_views.len, vector_fetch_needed);
        projected = try addSliceGrowthBytes(?[]const u8, projected, self.values.len, vector_fetch_needed);
        projected = try addSliceGrowthBytes([]const f32, projected, self.vector_views.len, vector_fetch_needed);
        projected = try addSliceGrowthBytes(f32, projected, self.distances.len, vector_fetch_needed);
        projected = try addSliceGrowthBytes(f32, projected, self.error_bounds.len, vector_fetch_needed);
        projected = try addSliceGrowthBytes(f32, projected, self.score_bounds.len, try std.math.mul(usize, vector_fetch_needed, 2));
        const vector_value_count = std.math.mul(usize, self.dims, vector_fetch_needed) catch return error.OutOfMemory;
        projected = try addSliceGrowthBytes(f32, projected, self.vector_batch.len, vector_value_count);
        return projected;
    }

    /// Release index-sized buffers retained by a completed search. The normal
    /// bounded scratch stays hot; buffers that grew beyond the configured tree
    /// fanout are disposable and can be rebuilt on the next flat scan.
    pub fn reclaimRetainedWorkspace(
        self: *SearchScratch,
        alloc: Allocator,
        target_bytes: u64,
        max_candidates: usize,
    ) u64 {
        if (target_bytes == 0) return 0;
        var reclaimed: u64 = 0;
        reclaimed +|= freeSliceAboveRetainedCapacity(search_types.FlatCentroidProbe, alloc, &self.flat_probes, 0);
        if (reclaimed >= target_bytes) return reclaimed;
        reclaimed +|= freeSliceAboveRetainedCapacity(search_types.FlatCentroidProbe, alloc, &self.flat_probe_merge, 0);
        if (reclaimed >= target_bytes) return reclaimed;
        reclaimed +|= freeSliceAboveRetainedCapacity(CoverageMember, alloc, &self.coverage_members, 0);
        if (reclaimed >= target_bytes) return reclaimed;
        reclaimed +|= freeSliceAboveRetainedCapacity(usize, alloc, &self.coverage_visited_words, 0);
        if (reclaimed >= target_bytes) return reclaimed;

        const baseline_vector_values = std.math.mul(usize, self.dims, max_candidates) catch std.math.maxInt(usize);
        reclaimed +|= freeSliceAboveRetainedCapacity(f32, alloc, &self.vector_batch, baseline_vector_values);
        if (reclaimed >= target_bytes) return reclaimed;
        reclaimed +|= freeSliceAboveRetainedCapacity(usize, alloc, &self.positions, max_candidates);
        if (reclaimed >= target_bytes) return reclaimed;
        reclaimed +|= freeSliceAboveRetainedCapacity(u64, alloc, &self.vector_ids, max_candidates);
        if (reclaimed >= target_bytes) return reclaimed;
        reclaimed +|= freeSliceAboveRetainedCapacity(?[]const u8, alloc, &self.metadata, max_candidates);
        if (reclaimed >= target_bytes) return reclaimed;
        reclaimed +|= freeSliceAboveRetainedCapacity(RerankLookup, alloc, &self.lookups, max_candidates);
        if (reclaimed >= target_bytes) return reclaimed;
        reclaimed +|= freeSliceAboveRetainedCapacity([]const u8, alloc, &self.key_views, max_candidates);
        if (reclaimed >= target_bytes) return reclaimed;
        reclaimed +|= freeSliceAboveRetainedCapacity(?[]const u8, alloc, &self.values, max_candidates);
        if (reclaimed >= target_bytes) return reclaimed;
        reclaimed +|= freeSliceAboveRetainedCapacity([]const f32, alloc, &self.vector_views, max_candidates);
        if (reclaimed >= target_bytes) return reclaimed;
        reclaimed +|= freeSliceAboveRetainedCapacity(f32, alloc, &self.distances, max_candidates);
        if (reclaimed >= target_bytes) return reclaimed;
        reclaimed +|= freeSliceAboveRetainedCapacity(f32, alloc, &self.error_bounds, max_candidates);
        reclaimed +|= freeSliceAboveRetainedCapacity(f32, alloc, &self.score_bounds, max_candidates * 2);
        return reclaimed;
    }

    pub fn projectedBytesForExhaustiveCoverage(
        self: *const SearchScratch,
        node_count: u64,
        assignment_capacity: usize,
    ) !u64 {
        var projected = self.bytes();
        if (assignment_capacity != 0) {
            projected = try addSliceGrowthBytes(CoverageMember, projected, self.coverage_members.len, assignment_capacity);
            projected = try addSliceGrowthBytes(RerankLookup, projected, self.lookups.len, assignment_capacity);
            projected = try addSliceGrowthBytes([]const u8, projected, self.key_views.len, assignment_capacity);
            projected = try addSliceGrowthBytes(?[]const u8, projected, self.values.len, assignment_capacity);
        }
        const word_bits = @bitSizeOf(usize);
        const words_u64 = std.math.divCeil(u64, node_count, word_bits) catch return error.OutOfMemory;
        const words = std.math.cast(usize, words_u64) orelse return error.OutOfMemory;
        return try addSliceGrowthBytes(usize, projected, self.coverage_visited_words.len, words);
    }

    /// Exhaustive coverage scales with the published index, unlike the normal
    /// bounded request workspace. Do not pin those buffers in the per-index
    /// scratch cache after the request has released its resource reservation.
    pub fn clearExhaustiveWorkspace(self: *SearchScratch, alloc: Allocator) void {
        alloc.free(self.flat_probes);
        alloc.free(self.flat_probe_merge);
        alloc.free(self.coverage_members);
        alloc.free(self.coverage_visited_words);
        alloc.free(self.lookups);
        alloc.free(self.key_views);
        alloc.free(self.values);
        self.flat_probes = &.{};
        self.flat_probe_merge = &.{};
        self.coverage_members = &.{};
        self.coverage_visited_words = &.{};
        self.lookups = &.{};
        self.key_views = &.{};
        self.values = &.{};
    }

    pub fn resetCoverageVisited(self: *SearchScratch, alloc: Allocator, node_count: u64) !void {
        const word_bits = @bitSizeOf(usize);
        const words_u64 = std.math.divCeil(u64, node_count, word_bits) catch return error.OutOfMemory;
        const words = std.math.cast(usize, words_u64) orelse return error.OutOfMemory;
        if (self.coverage_visited_words.len < words) {
            self.coverage_visited_words = try alloc.realloc(self.coverage_visited_words, words);
        }
        @memset(self.coverage_visited_words[0..words], 0);
    }

    pub fn markCoverageNodeVisited(self: *SearchScratch, node_id: u64, node_count: u64) bool {
        if (node_id == 0 or node_id > node_count) return false;
        const zero_based = node_id - 1;
        const word_bits = @bitSizeOf(usize);
        const word_index: usize = @intCast(zero_based / word_bits);
        const bit_index: std.math.Log2Int(usize) = @intCast(zero_based % word_bits);
        const mask = @as(usize, 1) << bit_index;
        if (self.coverage_visited_words[word_index] & mask != 0) return false;
        self.coverage_visited_words[word_index] |= mask;
        return true;
    }

    pub fn bytes(self: *const SearchScratch) u64 {
        return estimateScratchBytes(&self.estimate) +
            byteLen(self.transformed_query) +
            byteLen(self.centroid) +
            byteLen(self.vector) +
            byteLen(self.vector_batch) +
            byteLen(self.member_ids) +
            byteLen(self.vector_ids) +
            byteLen(self.metadata) +
            byteLen(self.flags) +
            byteLen(self.positions) +
            byteLen(self.lookups) +
            byteLen(self.key_views) +
            byteLen(self.values) +
            byteLen(self.vector_views) +
            byteLen(self.distances) +
            byteLen(self.error_bounds) +
            byteLen(self.score_bounds) +
            byteLen(self.flat_probes) +
            byteLen(self.flat_probe_merge) +
            byteLen(self.coverage_members) +
            byteLen(self.coverage_visited_words);
    }

    pub fn deinit(self: *SearchScratch, alloc: Allocator) void {
        self.estimate.deinit(alloc);
        alloc.free(self.transformed_query);
        alloc.free(self.centroid);
        alloc.free(self.vector);
        alloc.free(self.vector_batch);
        alloc.free(self.member_ids);
        alloc.free(self.vector_ids);
        alloc.free(self.metadata);
        alloc.free(self.flags);
        alloc.free(self.positions);
        alloc.free(self.lookups);
        alloc.free(self.key_views);
        alloc.free(self.values);
        alloc.free(self.vector_views);
        alloc.free(self.distances);
        alloc.free(self.error_bounds);
        alloc.free(self.score_bounds);
        alloc.free(self.flat_probes);
        alloc.free(self.flat_probe_merge);
        alloc.free(self.coverage_members);
        alloc.free(self.coverage_visited_words);
        self.* = undefined;
    }
};

fn byteLen(values: anytype) u64 {
    return @as(u64, @intCast(values.len * @sizeOf(std.meta.Child(@TypeOf(values)))));
}

fn addSliceGrowthBytes(comptime T: type, current_bytes: u64, current_len: usize, needed: usize) !u64 {
    if (needed <= current_len) return current_bytes;
    const growth_len = needed - current_len;
    const growth_bytes = std.math.mul(u64, @intCast(growth_len), @sizeOf(T)) catch return error.OutOfMemory;
    return std.math.add(u64, current_bytes, growth_bytes) catch return error.OutOfMemory;
}

fn freeSliceAboveRetainedCapacity(
    comptime T: type,
    alloc: Allocator,
    values: *[]T,
    retained_capacity: usize,
) u64 {
    if (values.len <= retained_capacity) return 0;
    const reclaimed = byteLen(values.*);
    alloc.free(values.*);
    values.* = &.{};
    return reclaimed;
}

test "SearchScratch grows error bounds with vector fetch capacity" {
    const alloc = std.testing.allocator;
    var scratch = try SearchScratch.init(alloc, 4, 2, 2);
    defer scratch.deinit(alloc);

    try scratch.ensureVectorFetchCapacity(alloc, 5);

    try std.testing.expect(scratch.distances.len >= 5);
    try std.testing.expect(scratch.error_bounds.len >= 5);
    try std.testing.expect(scratch.vector_batch.len >= 4 * 5);
}

test "SearchScratch accounts coverage buffers and detects repeated nodes" {
    const alloc = std.testing.allocator;
    var scratch = try SearchScratch.init(alloc, 4, 2, 2);
    defer scratch.deinit(alloc);

    const bytes_before = scratch.bytes();
    try scratch.ensureCoverageMemberCapacity(alloc, 8);
    try scratch.resetCoverageVisited(alloc, 130);

    try std.testing.expect(scratch.bytes() >= bytes_before +
        8 * @sizeOf(CoverageMember) +
        3 * @sizeOf(usize));
    try std.testing.expect(scratch.markCoverageNodeVisited(130, 130));
    try std.testing.expect(!scratch.markCoverageNodeVisited(130, 130));
    try std.testing.expect(!scratch.markCoverageNodeVisited(131, 130));

    scratch.clearExhaustiveWorkspace(alloc);
    try std.testing.expectEqual(@as(usize, 0), scratch.coverage_members.len);
    try std.testing.expectEqual(@as(usize, 0), scratch.lookups.len);
    try scratch.ensureVectorFetchCapacity(alloc, 5);
    try std.testing.expect(scratch.lookups.len >= 5);
}

test "SearchScratch accounts reusable flat frontier workspace" {
    const alloc = std.testing.allocator;
    var scratch = try SearchScratch.init(alloc, 4, 2, 2);
    defer scratch.deinit(alloc);

    const bytes_before = scratch.bytes();
    try scratch.ensureFlatProbeCapacity(alloc, 4_097, true);

    try std.testing.expect(scratch.bytes() >= bytes_before +
        2 * 4_097 * @sizeOf(search_types.FlatCentroidProbe));
}

test "SearchScratch flat projection includes block scoring workspace" {
    const alloc = std.testing.allocator;
    var scratch = try SearchScratch.init(alloc, 1_536, 16, 64);
    defer scratch.deinit(alloc);

    const frontier_only = try scratch.projectedBytesWithFlatProbeCapacity(10_000, false, 0);
    const with_block_scoring = try scratch.projectedBytesWithFlatProbeCapacity(10_000, false, 8_192);
    try std.testing.expect(with_block_scoring > frontier_only + 48 * 1024 * 1024);

    try scratch.ensureFlatProbeCapacity(alloc, 10_000, false);
    try scratch.ensureVectorFetchCapacity(alloc, 8_192);
    try std.testing.expectEqual(with_block_scoring, scratch.bytes());
}

test "SearchScratch vector fetch rejects dimension overflow before growth" {
    const alloc = std.testing.allocator;
    var scratch = try SearchScratch.init(alloc, 2, 2, 2);
    defer scratch.deinit(alloc);
    const before = scratch.bytes();
    scratch.dims = std.math.maxInt(usize);

    try std.testing.expectError(error.OutOfMemory, scratch.ensureVectorFetchCapacity(alloc, 2));
    try std.testing.expectEqual(before, scratch.bytes());
}

test "SearchScratch pressure reclaim preserves bounded hot workspace" {
    const alloc = std.testing.allocator;
    var scratch = try SearchScratch.init(alloc, 8, 4, 4);
    defer scratch.deinit(alloc);
    try scratch.ensureFlatProbeCapacity(alloc, 1_024, true);
    try scratch.ensureVectorFetchCapacity(alloc, 128);

    const reclaimed = scratch.reclaimRetainedWorkspace(alloc, std.math.maxInt(u64), 4);
    try std.testing.expect(reclaimed > 0);
    try std.testing.expectEqual(@as(usize, 0), scratch.flat_probes.len);
    try std.testing.expectEqual(@as(usize, 0), scratch.flat_probe_merge.len);
    try std.testing.expectEqual(@as(usize, 8), scratch.transformed_query.len);
    try std.testing.expectEqual(@as(usize, 4), scratch.member_ids.len);
    try std.testing.expectEqual(@as(usize, 4), scratch.flags.len);
}

fn estimateScratchBytes(scratch: *const quantizer.RaBitQuantizer.EstimateScratch) u64 {
    return byteLen(scratch.query_diff) +
        byteLen(scratch.q1) +
        byteLen(scratch.q2) +
        byteLen(scratch.q3) +
        byteLen(scratch.q4);
}

pub fn requestHasExtraFilters(
    req: search_types.SearchRequest,
    filter_state: *const search_types.RequestFilterState,
) bool {
    return req.filter_prefix.len > 0 or
        req.distance_over != null or
        req.distance_under != null or
        !filter_state.isTrivial();
}

pub fn exactDistanceToStoredVector(
    metric: types.HBCConfig,
    query: []const f32,
    query_measure: f32,
    candidate: []const f32,
) f32 {
    return vec.distanceToQuery(query, query_measure, candidate, metric.metric);
}

pub fn exactDistancesToStoredVectors(
    metric: types.HBCConfig,
    query: []const f32,
    query_measure: f32,
    candidates: []const []const f32,
    distances: []f32,
) void {
    std.debug.assert(candidates.len <= distances.len);
    switch (metric.metric) {
        .l2_squared => vec.batchL2SquaredDistance(query, candidates, distances),
        .inner_product => {
            vec.batchDot(query, candidates, distances);
            for (distances[0..candidates.len]) |*distance| distance.* = -distance.*;
        },
        .cosine => {
            if (query_measure == 0) {
                @memset(distances[0..candidates.len], 1.0);
                return;
            }
            vec.batchDot(query, candidates, distances);
            for (distances[0..candidates.len], candidates[0..candidates.len]) |*distance, candidate| {
                const candidate_norm = vec.norm(candidate);
                distance.* = if (candidate_norm == 0.0) 1.0 else 1.0 - (distance.* / (query_measure * candidate_norm));
            }
        },
    }
}

/// Request-path variant. Keep cancellation latency bounded even when a leaf
/// or rerank window is large; maintenance/debug callers retain the vectorized
/// helper above.
pub fn exactDistancesToStoredVectorsCancellable(
    metric: types.HBCConfig,
    query: []const f32,
    query_measure: f32,
    candidates: []const []const f32,
    distances: []f32,
    cancellation: ?@import("search_types.zig").CancellationToken,
) !void {
    return exactDistancesToStoredVectorsWithCancellationCheck(
        metric,
        query,
        query_measure,
        candidates,
        distances,
        CancellationCheck{ .token = cancellation },
    );
}

const exact_distance_cancellation_stride = 64;

const CancellationCheck = struct {
    token: ?@import("search_types.zig").CancellationToken,

    inline fn check(self: @This()) !void {
        if (self.token) |token| {
            if (token.isCancelled()) return error.Cancelled;
        }
    }
};

fn exactDistancesToStoredVectorsWithCancellationCheck(
    metric: types.HBCConfig,
    query: []const f32,
    query_measure: f32,
    candidates: []const []const f32,
    distances: []f32,
    cancellation_check: anytype,
) !void {
    std.debug.assert(candidates.len <= distances.len);
    for (candidates, 0..) |candidate, i| {
        if (i % exact_distance_cancellation_stride == 0) try cancellation_check.check();
        distances[i] = exactDistanceToStoredVector(metric, query, query_measure, candidate);
    }
}

test "exact cosine distance includes candidate norm" {
    const metric = types.HBCConfig{ .dims = 2, .metric = .cosine };
    const query = [_]f32{ 1.0, 0.0 };
    const same_direction_large = [_]f32{ 10.0, 0.0 };
    const orthogonal = [_]f32{ 0.0, 2.0 };
    const query_measure = vec.norm(&query);

    try std.testing.expectApproxEqAbs(
        @as(f32, 0.0),
        exactDistanceToStoredVector(metric, &query, query_measure, &same_direction_large),
        1e-6,
    );
    try std.testing.expectApproxEqAbs(
        @as(f32, 1.0),
        exactDistanceToStoredVector(metric, &query, query_measure, &orthogonal),
        1e-6,
    );

    var distances: [2]f32 = undefined;
    exactDistancesToStoredVectors(metric, &query, query_measure, &.{ &same_direction_large, &orthogonal }, &distances);
    try std.testing.expectApproxEqAbs(@as(f32, 0.0), distances[0], 1e-6);
    try std.testing.expectApproxEqAbs(@as(f32, 1.0), distances[1], 1e-6);
}

test "cancellable exact distances honor preexisting cancellation" {
    const metric = types.HBCConfig{ .dims = 2, .metric = .l2_squared };
    const query = [_]f32{ 0.0, 0.0 };
    const candidate = [_]f32{ 1.0, 1.0 };
    var candidates: [128][]const f32 = undefined;
    for (&candidates) |*slot| slot.* = &candidate;
    var distances: [128]f32 = undefined;
    var cancelled = std.atomic.Value(bool).init(true);
    try std.testing.expectError(
        error.Cancelled,
        exactDistancesToStoredVectorsCancellable(metric, &query, 0, &candidates, &distances, .fromAtomic(&cancelled)),
    );
}

test "cancellable exact distances stop at a mid-batch checkpoint" {
    const CancelAtSecondCheckpoint = struct {
        calls: usize = 0,

        fn check(self: *@This()) !void {
            self.calls += 1;
            if (self.calls == 2) return error.Cancelled;
        }
    };

    const metric = types.HBCConfig{ .dims = 2, .metric = .l2_squared };
    const query = [_]f32{ 0.0, 0.0 };
    const candidate = [_]f32{ 1.0, 1.0 };
    var candidates: [exact_distance_cancellation_stride * 2][]const f32 = undefined;
    for (&candidates) |*slot| slot.* = &candidate;
    var distances: [candidates.len]f32 = undefined;
    @memset(&distances, std.math.nan(f32));
    var cancellation_check = CancelAtSecondCheckpoint{};

    try std.testing.expectError(
        error.Cancelled,
        exactDistancesToStoredVectorsWithCancellationCheck(
            metric,
            &query,
            0,
            &candidates,
            &distances,
            &cancellation_check,
        ),
    );
    try std.testing.expectEqual(@as(usize, 2), cancellation_check.calls);
    for (distances[0..exact_distance_cancellation_stride]) |distance| {
        try std.testing.expectEqual(@as(f32, 2.0), distance);
    }
    for (distances[exact_distance_cancellation_stride..]) |distance| {
        try std.testing.expect(std.math.isNan(distance));
    }
}
