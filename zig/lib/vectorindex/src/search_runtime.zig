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

    pub fn init(alloc: Allocator, dims: usize, max_branching: usize, max_leaf: usize) !SearchScratch {
        const max_candidates = @max(max_branching, max_leaf);
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
        const vector_batch = try alloc.alloc(f32, dims * max_candidates);
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
        };
    }

    pub fn ensureVectorFetchCapacity(self: *SearchScratch, alloc: Allocator, needed: usize) !void {
        if (self.positions.len < needed) self.positions = try alloc.realloc(self.positions, needed);
        if (self.vector_ids.len < needed) self.vector_ids = try alloc.realloc(self.vector_ids, needed);
        if (self.metadata.len < needed) self.metadata = try alloc.realloc(self.metadata, needed);
        if (self.lookups.len < needed) self.lookups = try alloc.realloc(self.lookups, needed);
        if (self.key_views.len < needed) self.key_views = try alloc.realloc(self.key_views, needed);
        if (self.values.len < needed) self.values = try alloc.realloc(self.values, needed);
        if (self.vector_views.len < needed) self.vector_views = try alloc.realloc(self.vector_views, needed);
        if (self.distances.len < needed) self.distances = try alloc.realloc(self.distances, needed);
        if (self.error_bounds.len < needed) self.error_bounds = try alloc.realloc(self.error_bounds, needed);
        if (self.vector_batch.len < self.dims * needed) self.vector_batch = try alloc.realloc(self.vector_batch, self.dims * needed);
    }

    pub fn ensureRerankCapacity(self: *SearchScratch, alloc: Allocator, needed: usize) !void {
        if (self.flags.len < needed) self.flags = try alloc.realloc(self.flags, needed);
        try self.ensureVectorFetchCapacity(alloc, needed);
    }

    pub fn ensureMemberIdCapacity(self: *SearchScratch, alloc: Allocator, needed: usize) !void {
        if (self.member_ids.len < needed) self.member_ids = try alloc.realloc(self.member_ids, needed);
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
            byteLen(self.error_bounds);
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
        self.* = undefined;
    }
};

fn byteLen(values: anytype) u64 {
    return @as(u64, @intCast(values.len * @sizeOf(std.meta.Child(@TypeOf(values)))));
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
