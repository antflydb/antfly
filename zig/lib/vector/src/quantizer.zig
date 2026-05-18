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

//! RaBitQ Quantizer - quantizes vectors into 1 bit per dimension.
//!
//! Port of antfly/lib/vector/quantize/rabitquantizer.go.
//!
//! Reference: "RaBitQ: Quantizing High-Dimensional Vectors with a Theoretical Error Bound
//! for Approximate Nearest Neighbor Search" by Jianyang Gao & Cheng Long.

const std = @import("std");
const math = std.math;
const Allocator = std.mem.Allocator;
const rabitq = @import("rabitq.zig");
const vec = @import("vector.zig");
const proto = @import("proto.zig");
const go_rand = @import("go_rand.zig");

/// RaBitQuantizer quantizes vectors into 1 bit per dimension.
///
/// Thread-safe: can be cached and reused across threads.
pub const RaBitQuantizer = struct {
    dims: usize,
    sqrt_dims: f32,
    sqrt_dims_inv: f32,
    /// Random offsets in [0, 1) to remove bias when quantizing query vectors.
    unbias: []f32,
    distance_metric: vec.DistanceMetric,
    alloc: Allocator,

    /// Creates a new RaBitQ quantizer.
    ///
    /// The seed is used to generate pseudo-random values for the algorithm.
    /// Quantizers must be recreated with the same seed to search existing quantized sets.
    pub fn init(alloc: Allocator, dims: usize, seed: u64, distance_metric: vec.DistanceMetric) !RaBitQuantizer {
        std.debug.assert(dims > 0);

        // Match Go's math/rand/v2 rand.New(rand.NewPCG(seed, 1048)).
        var rng = go_rand.GoPcg.init(seed, 1048);

        const unbias = try alloc.alloc(f32, dims);
        for (unbias) |*u| {
            u.* = rng.float32();
        }

        const sqrt_dims: f32 = @sqrt(@as(f32, @floatFromInt(dims)));
        return .{
            .dims = dims,
            .sqrt_dims = sqrt_dims,
            .sqrt_dims_inv = 1.0 / sqrt_dims,
            .unbias = unbias,
            .distance_metric = distance_metric,
            .alloc = alloc,
        };
    }

    pub fn deinit(self: *RaBitQuantizer) void {
        self.alloc.free(self.unbias);
        self.* = undefined;
    }

    pub const EstimateScratch = struct {
        query_diff: []f32,
        q1: []u64,
        q2: []u64,
        q3: []u64,
        q4: []u64,

        pub fn init(alloc: Allocator, dims: usize) !EstimateScratch {
            const width = rabitq.codeWidth(dims);
            const query_diff = try alloc.alloc(f32, dims);
            errdefer alloc.free(query_diff);
            const q1 = try alloc.alloc(u64, width);
            errdefer alloc.free(q1);
            const q2 = try alloc.alloc(u64, width);
            errdefer alloc.free(q2);
            const q3 = try alloc.alloc(u64, width);
            errdefer alloc.free(q3);
            const q4 = try alloc.alloc(u64, width);
            errdefer alloc.free(q4);
            return .{
                .query_diff = query_diff,
                .q1 = q1,
                .q2 = q2,
                .q3 = q3,
                .q4 = q4,
            };
        }

        pub fn deinit(self: *EstimateScratch, alloc: Allocator) void {
            alloc.free(self.query_diff);
            alloc.free(self.q1);
            alloc.free(self.q2);
            alloc.free(self.q3);
            alloc.free(self.q4);
            self.* = undefined;
        }
    };

    /// Quantizes a set of vectors relative to a centroid.
    /// Returns a new RaBitQuantizedVectorSet.
    pub fn quantize(
        self: *const RaBitQuantizer,
        centroid: []const f32,
        vectors: []const f32,
        count: usize,
    ) !proto.RaBitQuantizedVectorSet {
        const width = rabitq.codeWidth(self.dims);

        // Allocate output buffers.
        const codes = try self.alloc.alloc(u64, count * width);
        errdefer self.alloc.free(codes);
        const code_counts = try self.alloc.alloc(u32, count);
        errdefer self.alloc.free(code_counts);
        const centroid_distances = try self.alloc.alloc(f32, count);
        errdefer self.alloc.free(centroid_distances);
        const quantized_dot_products = try self.alloc.alloc(f32, count);
        errdefer self.alloc.free(quantized_dot_products);

        // Allocate temp space for normalized vectors.
        const temp_diffs = try self.alloc.alloc(f32, count * self.dims);
        defer self.alloc.free(temp_diffs);

        // Step 1: Compute differences from centroid and normalize.
        for (0..count) |i| {
            const v = vectors[i * self.dims ..][0..self.dims];
            const diff = temp_diffs[i * self.dims ..][0..self.dims];

            // diff = v - centroid
            vec.subTo(diff, v, centroid);

            // Compute ||v - centroid||
            const dist = vec.norm(diff);
            centroid_distances[i] = dist;

            // Normalize to unit vector: diff /= ||diff||
            if (dist != 0) {
                vec.scale(1.0 / dist, diff);
            }
        }

        // Step 3 & 4: Quantize unit vectors into codes and compute dot products.
        rabitq.quantizeVectors(
            temp_diffs,
            codes,
            quantized_dot_products,
            code_counts,
            self.sqrt_dims_inv,
            count,
            self.dims,
            width,
        );

        // Compute centroid dot products (for InnerProduct/Cosine).
        var centroid_dot_products: []f32 = &.{};
        var centroid_norm: f32 = 0;
        if (self.distance_metric != .l2_squared) {
            centroid_dot_products = try self.alloc.alloc(f32, count);
            for (0..count) |i| {
                const v = vectors[i * self.dims ..][0..self.dims];
                centroid_dot_products[i] = vec.dot(v, centroid);
            }
            centroid_norm = vec.norm(centroid);
        }

        const centroid_copy = try self.alloc.dupe(f32, centroid);

        return .{
            .metric = self.distance_metric,
            .centroid = centroid_copy,
            .codes = .{
                .count = @intCast(count),
                .width = @intCast(width),
                .data = codes,
            },
            .code_counts = code_counts,
            .centroid_distances = centroid_distances,
            .quantized_dot_products = quantized_dot_products,
            .centroid_dot_products = centroid_dot_products,
            .centroid_norm = centroid_norm,
        };
    }

    pub fn quantizeInto(
        self: *const RaBitQuantizer,
        qs: *proto.RaBitQuantizedVectorSet,
        centroid: []const f32,
        vectors: []const f32,
        count: usize,
    ) !void {
        const width = rabitq.codeWidth(self.dims);
        qs.metric = self.distance_metric;
        qs.centroid = try resizeSlice(f32, self.alloc, qs.centroid, centroid.len);
        @memcpy(qs.centroid, centroid);

        qs.codes.data = try resizeSlice(u64, self.alloc, qs.codes.data, count * width);
        qs.codes.count = @intCast(count);
        qs.codes.width = @intCast(width);
        qs.code_counts = try resizeSlice(u32, self.alloc, qs.code_counts, count);
        qs.centroid_distances = try resizeSlice(f32, self.alloc, qs.centroid_distances, count);
        qs.quantized_dot_products = try resizeSlice(f32, self.alloc, qs.quantized_dot_products, count);

        const temp_diffs = try self.alloc.alloc(f32, count * self.dims);
        defer self.alloc.free(temp_diffs);

        for (0..count) |i| {
            const v = vectors[i * self.dims ..][0..self.dims];
            const diff = temp_diffs[i * self.dims ..][0..self.dims];
            vec.subTo(diff, v, centroid);
            const dist = vec.norm(diff);
            qs.centroid_distances[i] = dist;
            if (dist != 0) vec.scale(1.0 / dist, diff);
        }

        rabitq.quantizeVectors(
            temp_diffs,
            qs.codes.data,
            qs.quantized_dot_products,
            qs.code_counts,
            self.sqrt_dims_inv,
            count,
            self.dims,
            width,
        );

        if (self.distance_metric != .l2_squared) {
            qs.centroid_dot_products = try resizeSlice(f32, self.alloc, qs.centroid_dot_products, count);
            for (0..count) |i| {
                const v = vectors[i * self.dims ..][0..self.dims];
                qs.centroid_dot_products[i] = vec.dot(v, centroid);
            }
            qs.centroid_norm = vec.norm(centroid);
        } else {
            if (qs.centroid_dot_products.len > 0) {
                self.alloc.free(qs.centroid_dot_products);
                qs.centroid_dot_products = &.{};
            }
            qs.centroid_norm = 0;
        }
    }

    pub fn quantizeWithSet(
        self: *const RaBitQuantizer,
        qs: *proto.RaBitQuantizedVectorSet,
        vectors: []const f32,
        count: usize,
    ) !void {
        const width = rabitq.codeWidth(self.dims);
        const old_count = qs.getCount();
        const new_count = old_count + count;

        qs.codes.data = try resizeSlice(u64, self.alloc, qs.codes.data, new_count * width);
        qs.codes.count = @intCast(new_count);
        qs.codes.width = @intCast(width);
        qs.code_counts = try resizeSlice(u32, self.alloc, qs.code_counts, new_count);
        qs.centroid_distances = try resizeSlice(f32, self.alloc, qs.centroid_distances, new_count);
        qs.quantized_dot_products = try resizeSlice(f32, self.alloc, qs.quantized_dot_products, new_count);

        if (self.distance_metric != .l2_squared) {
            qs.centroid_dot_products = try resizeSlice(f32, self.alloc, qs.centroid_dot_products, new_count);
            for (0..count) |i| {
                const v = vectors[i * self.dims ..][0..self.dims];
                qs.centroid_dot_products[old_count + i] = vec.dot(v, qs.centroid);
            }
        }

        const temp_diffs = try self.alloc.alloc(f32, count * self.dims);
        defer self.alloc.free(temp_diffs);

        for (0..count) |i| {
            const v = vectors[i * self.dims ..][0..self.dims];
            const diff = temp_diffs[i * self.dims ..][0..self.dims];
            vec.subTo(diff, v, qs.centroid);
            const dist = vec.norm(diff);
            qs.centroid_distances[old_count + i] = dist;
            if (dist != 0) vec.scale(1.0 / dist, diff);
        }

        rabitq.quantizeVectors(
            temp_diffs,
            qs.codes.data[old_count * width ..],
            qs.quantized_dot_products[old_count..],
            qs.code_counts[old_count..],
            self.sqrt_dims_inv,
            count,
            self.dims,
            width,
        );
    }

    /// Estimates distances from a query vector to all vectors in the quantized set.
    ///
    /// Fills `distances` and `error_bounds` slices (caller-allocated, length = set.getCount()).
    pub fn estimateDistances(
        self: *const RaBitQuantizer,
        qs: *const proto.RaBitQuantizedVectorSet,
        query_vector: []const f32,
        distances: []f32,
        error_bounds: []f32,
    ) !void {
        var scratch = try EstimateScratch.init(self.alloc, self.dims);
        defer scratch.deinit(self.alloc);
        try self.estimateDistancesWithScratch(qs, query_vector, distances, error_bounds, &scratch);
    }

    pub fn estimateDistancesWithScratch(
        self: *const RaBitQuantizer,
        qs: *const proto.RaBitQuantizedVectorSet,
        query_vector: []const f32,
        distances: []f32,
        error_bounds: []f32,
        scratch: *EstimateScratch,
    ) !void {
        const count = qs.getCount();
        const width: usize = @intCast(qs.codes.width);
        const temp_query_diff = scratch.query_diff[0..self.dims];
        const temp_q1 = scratch.q1[0..width];
        const temp_q2 = scratch.q2[0..width];
        const temp_q3 = scratch.q3[0..width];
        const temp_q4 = scratch.q4[0..width];

        // Normalize query vector relative to centroid.
        vec.subTo(temp_query_diff, query_vector, qs.centroid);
        const query_centroid_distance = vec.norm(temp_query_diff);

        if (query_centroid_distance == 0) {
            self.calcCentroidDistances(qs, distances);
            @memset(error_bounds[0..count], 0);
            return;
        }

        var squared_centroid_norm: f32 = 0;
        var query_centroid_dot_product: f32 = 0;
        if (self.distance_metric != .l2_squared) {
            query_centroid_dot_product = vec.dot(query_vector, qs.centroid);
            squared_centroid_norm = qs.centroid_norm * qs.centroid_norm;
        }

        // Normalize query diff to unit vector.
        vec.scale(1.0 / query_centroid_distance, temp_query_diff);

        // Find min/max for 4-bit quantization.
        const mm = vec.minMax(temp_query_diff);
        const min_val = mm.min;
        const max_val = mm.max;

        const quantized_range: f32 = 15.0;
        const delta = (max_val - min_val) / quantized_range;

        // Quantize query to 4-bit sub-codes.
        @memset(temp_q1, 0);
        @memset(temp_q2, 0);
        @memset(temp_q3, 0);
        @memset(temp_q4, 0);

        var quantized_sum: u64 = 0;
        var quantized1: u64 = 0;
        var quantized2: u64 = 0;
        var quantized3: u64 = 0;
        var quantized4: u64 = 0;

        for (0..self.dims) |d| {
            if (delta != 0) {
                var q_val: u64 = @intFromFloat(@floor((temp_query_diff[d] - min_val) / delta + self.unbias[d]));
                q_val = @min(q_val, @as(u64, @intFromFloat(quantized_range)));
                quantized_sum += q_val;
                quantized1 = (quantized1 << 1) | (q_val & 1);
                quantized2 = (quantized2 << 1) | ((q_val & 2) >> 1);
                quantized3 = (quantized3 << 1) | ((q_val & 4) >> 2);
                quantized4 = (quantized4 << 1) | ((q_val & 8) >> 3);
            } else {
                quantized1 <<= 1;
                quantized2 <<= 1;
                quantized3 <<= 1;
                quantized4 <<= 1;
            }

            if ((d + 1) % 64 == 0) {
                const offset = d / 64;
                temp_q1[offset] = quantized1;
                temp_q2[offset] = quantized2;
                temp_q3[offset] = quantized3;
                temp_q4[offset] = quantized4;
            }
        }

        // Set leftover bits.
        if (self.dims % 64 != 0) {
            const offset = self.dims / 64;
            const shift: u6 = @intCast(64 - (self.dims % 64));
            temp_q1[offset] = quantized1 << shift;
            temp_q2[offset] = quantized2 << shift;
            temp_q3[offset] = quantized3 << shift;
            temp_q4[offset] = quantized4 << shift;
        }

        const delta_scale = delta * self.sqrt_dims_inv;
        const term1_scale = 2.0 * delta_scale;
        const term2_scale = 2.0 * min_val * self.sqrt_dims_inv;
        const term34 = delta_scale * @as(f32, @floatFromInt(quantized_sum)) + self.sqrt_dims * min_val;

        switch (self.distance_metric) {
            .l2_squared => {
                const query_centroid_distance_sq = query_centroid_distance * query_centroid_distance;
                for (0..count) |i| {
                    const code = qs.codes.atConst(i);
                    const bit_product: f32 = @floatFromInt(rabitq.bitProduct(
                        code,
                        temp_q1,
                        temp_q2,
                        temp_q3,
                        temp_q4,
                    ));
                    const estimator = (term1_scale * bit_product +
                        term2_scale * @as(f32, @floatFromInt(qs.code_counts[i])) -
                        term34) * qs.quantized_dot_products[i];
                    const data_centroid_distance = qs.centroid_distances[i];
                    const multiplier = 2.0 * data_centroid_distance * query_centroid_distance;
                    var distance = data_centroid_distance * data_centroid_distance +
                        query_centroid_distance_sq -
                        multiplier * estimator;
                    var error_bound = multiplier / self.sqrt_dims;

                    if (distance < 0) {
                        error_bound = @max(error_bound + distance, 0);
                        distance = 0;
                    }

                    distances[i] = distance;
                    error_bounds[i] = error_bound;
                }
            },
            .inner_product => {
                for (0..count) |i| {
                    const code = qs.codes.atConst(i);
                    const bit_product: f32 = @floatFromInt(rabitq.bitProduct(
                        code,
                        temp_q1,
                        temp_q2,
                        temp_q3,
                        temp_q4,
                    ));
                    const estimator = (term1_scale * bit_product +
                        term2_scale * @as(f32, @floatFromInt(qs.code_counts[i])) -
                        term34) * qs.quantized_dot_products[i];
                    const data_centroid_distance = qs.centroid_distances[i];
                    const multiplier = data_centroid_distance * query_centroid_distance;
                    const inner_product = multiplier * estimator +
                        qs.centroid_dot_products[i] + query_centroid_dot_product - squared_centroid_norm;
                    distances[i] = -inner_product;
                    error_bounds[i] = multiplier / self.sqrt_dims;
                }
            },
            .cosine => {
                for (0..count) |i| {
                    const code = qs.codes.atConst(i);
                    const bit_product: f32 = @floatFromInt(rabitq.bitProduct(
                        code,
                        temp_q1,
                        temp_q2,
                        temp_q3,
                        temp_q4,
                    ));
                    const estimator = (term1_scale * bit_product +
                        term2_scale * @as(f32, @floatFromInt(qs.code_counts[i])) -
                        term34) * qs.quantized_dot_products[i];
                    const data_centroid_distance = qs.centroid_distances[i];
                    const multiplier = data_centroid_distance * query_centroid_distance;
                    const inner_product = multiplier * estimator +
                        qs.centroid_dot_products[i] + query_centroid_dot_product - squared_centroid_norm;
                    var distance = 1.0 - inner_product;
                    var eb = multiplier / self.sqrt_dims;
                    if (distance < 0) {
                        eb = @max(eb + distance, 0);
                        distance = 0;
                    } else if (distance > 2) {
                        eb = @max(@min(eb - (distance - 2), 2), 0);
                        distance = 2;
                    }
                    distances[i] = distance;
                    error_bounds[i] = eb;
                }
            },
        }
    }

    fn calcCentroidDistances(
        self: *const RaBitQuantizer,
        qs: *const proto.RaBitQuantizedVectorSet,
        distances: []f32,
    ) void {
        switch (self.distance_metric) {
            .l2_squared => {
                for (qs.centroid_distances, 0..) |cd, i| {
                    distances[i] = cd * cd;
                }
            },
            .inner_product => {
                for (qs.centroid_dot_products, 0..) |cdp, i| {
                    distances[i] = -cdp;
                }
            },
            .cosine => {
                const inv_centroid_norm: f32 = if (qs.centroid_norm != 0) 1.0 / qs.centroid_norm else 0.0;
                for (qs.centroid_dot_products, 0..) |cdp, i| {
                    distances[i] = 1.0 - cdp * inv_centroid_norm;
                }
            },
        }
    }
};

fn resizeSlice(comptime T: type, alloc: Allocator, slice: []T, new_len: usize) ![]T {
    if (slice.len == 0) return try alloc.alloc(T, new_len);
    return try alloc.realloc(slice, new_len);
}

// --- Tests ---

test "RaBitQuantizer basic L2Squared" {
    const alloc = std.testing.allocator;

    var q = try RaBitQuantizer.init(alloc, 64, 42, .l2_squared);
    defer q.deinit();

    // Create a simple centroid and a set of vectors.
    var centroid: [64]f32 = undefined;
    @memset(&centroid, 0.0);

    // Single vector: all positive, magnitude 1.
    var vector_data: [64]f32 = undefined;
    const val: f32 = 1.0 / @sqrt(@as(f32, 64.0));
    @memset(&vector_data, val);

    var qs = try q.quantize(&centroid, &vector_data, 1);
    defer qs.deinit(alloc);

    try std.testing.expectEqual(@as(usize, 1), qs.getCount());

    // Estimate distance from the vector to itself (should be ~0).
    var distances: [1]f32 = undefined;
    var error_bounds: [1]f32 = undefined;
    try q.estimateDistances(&qs, &vector_data, &distances, &error_bounds);

    // Distance should be very small (it's an approximation).
    try std.testing.expect(distances[0] < 0.1);
}

test "RaBitQuantizer distinct vectors" {
    const alloc = std.testing.allocator;

    var q = try RaBitQuantizer.init(alloc, 64, 42, .l2_squared);
    defer q.deinit();

    var centroid: [64]f32 = undefined;
    @memset(&centroid, 0.0);

    // Two vectors: one positive, one negative.
    var vectors: [128]f32 = undefined;
    const mag: f32 = 1.0 / @sqrt(@as(f32, 64.0));
    for (0..64) |j| {
        vectors[j] = mag; // vector 0: all positive
        vectors[64 + j] = -mag; // vector 1: all negative
    }

    var qs = try q.quantize(&centroid, &vectors, 2);
    defer qs.deinit(alloc);

    try std.testing.expectEqual(@as(usize, 2), qs.getCount());

    // Query with the first vector: should be closer to vector 0 than vector 1.
    var distances: [2]f32 = undefined;
    var error_bounds: [2]f32 = undefined;
    try q.estimateDistances(&qs, vectors[0..64], &distances, &error_bounds);

    try std.testing.expect(distances[0] < distances[1]);
}

test "RaBitQuantizer centroid query matches inner product centroid distances" {
    const alloc = std.testing.allocator;

    var q = try RaBitQuantizer.init(alloc, 2, 42, .inner_product);
    defer q.deinit();

    const centroid = [_]f32{ 4.0, 3.0 };
    const vectors = [_]f32{
        5.0, 2.0,
        1.0, 2.0,
        6.0, 5.0,
    };

    var qs = try q.quantize(&centroid, &vectors, 3);
    defer qs.deinit(alloc);

    var distances: [3]f32 = undefined;
    var error_bounds: [3]f32 = undefined;
    try q.estimateDistances(&qs, &centroid, &distances, &error_bounds);

    try std.testing.expectApproxEqAbs(@as(f32, -26.0), distances[0], 1e-5);
    try std.testing.expectApproxEqAbs(@as(f32, -10.0), distances[1], 1e-5);
    try std.testing.expectApproxEqAbs(@as(f32, -39.0), distances[2], 1e-5);
    try std.testing.expectApproxEqAbs(@as(f32, 0.0), error_bounds[0], 1e-6);
    try std.testing.expectApproxEqAbs(@as(f32, 0.0), error_bounds[1], 1e-6);
    try std.testing.expectApproxEqAbs(@as(f32, 0.0), error_bounds[2], 1e-6);
}

test "RaBitQuantizer centroid query matches cosine centroid distances" {
    const alloc = std.testing.allocator;

    var q = try RaBitQuantizer.init(alloc, 2, 42, .cosine);
    defer q.deinit();

    var centroid = [_]f32{ 1.0, 1.0 };
    _ = vec.normalize(&centroid);

    const vectors = [_]f32{
        1.0,        0.0,
        0.0,        1.0,
        0.70710677, 0.70710677,
    };

    var qs = try q.quantize(&centroid, &vectors, 3);
    defer qs.deinit(alloc);

    var distances: [3]f32 = undefined;
    var error_bounds: [3]f32 = undefined;
    try q.estimateDistances(&qs, &centroid, &distances, &error_bounds);

    try std.testing.expectApproxEqAbs(@as(f32, 0.29289323), distances[0], 1e-5);
    try std.testing.expectApproxEqAbs(@as(f32, 0.29289323), distances[1], 1e-5);
    try std.testing.expectApproxEqAbs(@as(f32, 0.0), distances[2], 1e-5);
    try std.testing.expectApproxEqAbs(@as(f32, 0.0), error_bounds[0], 1e-6);
    try std.testing.expectApproxEqAbs(@as(f32, 0.0), error_bounds[1], 1e-6);
    try std.testing.expectApproxEqAbs(@as(f32, 0.0), error_bounds[2], 1e-6);
}
