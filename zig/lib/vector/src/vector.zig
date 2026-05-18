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

//! Vector types and distance metrics matching antfly/lib/vector/vector.proto.

const std = @import("std");
const math = std.math;
const go_rand = @import("go_rand.zig");

/// Distance metric for computing similarity between vectors.
/// Wire-compatible with antfly.lib.vector.DistanceMetric protobuf enum.
pub const DistanceMetric = enum(i32) {
    /// Squared Euclidean distance: ||vec1 - vec2||²
    l2_squared = 0,
    /// Negative dot product: -(vec1 · vec2)
    inner_product = 1,
    /// 1 - cosine similarity (assumes pre-normalized vectors)
    cosine = 2,
};

pub const RotAlgorithm = enum(i32) {
    none = 0,
    givens = 1,
};

pub const ClustAlgorithm = enum(i32) {
    kmeans = 0,
    hilbert = 1,
};

pub const Hilbert = struct {
    alloc: std.mem.Allocator,
    bits: u32 = 32,
    dimension: u32,
    length: u32,

    pub fn init(alloc: std.mem.Allocator, dimension: u32) !Hilbert {
        if (dimension == 0) return error.DimensionNotPositive;
        return .{
            .alloc = alloc,
            .dimension = dimension,
            .length = 32 * dimension,
        };
    }

    pub fn deinit(self: *Hilbert) void {
        self.* = undefined;
    }

    pub fn byteLen(self: *const Hilbert) usize {
        return @intCast((self.length + 7) / 8);
    }

    pub fn encodeVecBytes(self: *const Hilbert, input: []const f32) ![]u8 {
        if (input.len != self.dimension) return error.DimensionMismatch;
        const out = try self.alloc.alloc(u8, self.byteLen());
        errdefer self.alloc.free(out);

        const coords = try self.alloc.alloc(u32, self.dimension);
        defer self.alloc.free(coords);

        try self.encodeVecBytesInto(input, coords, out);
        return out;
    }

    pub fn encodeVecBytesInto(self: *const Hilbert, input: []const f32, coords: []u32, out: []u8) !void {
        if (input.len != self.dimension) return error.DimensionMismatch;
        if (coords.len < self.dimension) return error.ScratchTooSmall;
        if (out.len != self.byteLen()) return error.OutputSizeMismatch;

        const active_coords = coords[0..self.dimension];
        for (input, 0..) |val, i| {
            active_coords[i] = @bitCast(val);
        }
        self.axesToTranspose(active_coords);
        self.untransposeBytesInto(active_coords, out);
    }

    fn untransposeBytes(self: *const Hilbert, x: []const u32) ![]u8 {
        const out = try self.alloc.alloc(u8, self.byteLen());
        self.untransposeBytesInto(x, out);
        return out;
    }

    fn untransposeBytesInto(self: *const Hilbert, x: []const u32, out: []u8) void {
        @memset(out, 0);

        var b_index: usize = self.length;
        var mask: u32 = @as(u32, 1) << @intCast(self.bits - 1);
        const byte_len = self.byteLen();

        var bit: u32 = 0;
        while (bit < self.bits) : (bit += 1) {
            for (x) |coord| {
                b_index -= 1;
                if ((coord & mask) != 0) {
                    const byte_index = byte_len - 1 - (b_index / 8);
                    out[byte_index] |= @as(u8, 1) << @intCast(b_index % 8);
                }
            }
            mask >>= 1;
        }
    }

    fn axesToTranspose(self: *const Hilbert, x: []u32) void {
        const m: u32 = @as(u32, 1) << @intCast(self.bits - 1);
        const n = x.len;

        var q = m;
        while (q > 1) : (q >>= 1) {
            const p = q - 1;
            for (0..n) |i| {
                if ((x[i] & q) != 0) {
                    x[0] ^= p;
                } else {
                    const t = (x[0] ^ x[i]) & p;
                    x[0] ^= t;
                    x[i] ^= t;
                }
            }
        }

        var i: usize = 1;
        while (i < n) : (i += 1) {
            x[i] ^= x[i - 1];
        }

        var t: u32 = 0;
        q = m;
        while (q > 1) : (q >>= 1) {
            if ((x[n - 1] & q) != 0) t ^= q - 1;
        }
        for (0..n) |idx| x[idx] ^= t;
    }
};

/// A single vector (slice of f32).
pub const T = []const f32;

/// Set of float32 vectors of equal dimension, stored contiguously.
/// Wire-compatible with antfly.lib.vector.Set protobuf message.
pub const Set = struct {
    dims: usize,
    count: usize,
    data: []f32,

    pub fn at(self: *const Set, index: usize) []f32 {
        const start = index * self.dims;
        return self.data[start .. start + self.dims];
    }

    pub fn atConst(self: *const Set, index: usize) []const f32 {
        const start = index * self.dims;
        return self.data[start .. start + self.dims];
    }
};

const GivensRotation = struct {
    offset1: usize,
    offset2: usize,
    cos: f32,
    sin: f32,
};

pub const RandomOrthogonalTransformer = struct {
    alloc: std.mem.Allocator,
    algo: RotAlgorithm,
    dims: usize,
    seed: u64,
    rotations: []GivensRotation,

    pub fn init(alloc: std.mem.Allocator, algo: RotAlgorithm, dims: usize, seed: u64) !RandomOrthogonalTransformer {
        var out = RandomOrthogonalTransformer{
            .alloc = alloc,
            .algo = algo,
            .dims = dims,
            .seed = seed,
            .rotations = &.{},
        };

        if (algo == .none) return out;

        var rng = go_rand.GoPcg.init(seed, 1048);

        switch (algo) {
            .none => {},
            .givens => {
                const num_rotations: usize = @intFromFloat(@ceil(@as(f64, @floatFromInt(dims)) * std.math.log2(@as(f64, @floatFromInt(dims)))));
                out.rotations = try alloc.alloc(GivensRotation, num_rotations);
                for (out.rotations) |*rot| {
                    const offset1 = rng.intN(dims);
                    var offset2 = rng.intN(dims - 1);
                    if (offset2 >= offset1) offset2 += 1;
                    const theta = rng.float32() * 2.0 * std.math.pi;
                    rot.* = .{
                        .offset1 = offset1,
                        .offset2 = offset2,
                        .cos = @cos(theta),
                        .sin = @sin(theta),
                    };
                }
            },
        }
        return out;
    }

    pub fn deinit(self: *RandomOrthogonalTransformer) void {
        if (self.rotations.len > 0) self.alloc.free(self.rotations);
        self.* = undefined;
    }

    pub fn transform(self: *const RandomOrthogonalTransformer, original: []const f32, transformed: []f32) []f32 {
        switch (self.algo) {
            .none => @memcpy(transformed, original),
            .givens => {
                @memcpy(transformed, original);
                for (self.rotations) |rot| {
                    const left = transformed[rot.offset1];
                    const right = transformed[rot.offset2];
                    transformed[rot.offset1] = rot.cos * left + rot.sin * right;
                    transformed[rot.offset2] = -rot.sin * left + rot.cos * right;
                }
            },
        }
        return transformed;
    }

    pub fn untransform(self: *const RandomOrthogonalTransformer, transformed: []const f32, original: []f32) []f32 {
        switch (self.algo) {
            .none => @memcpy(original, transformed),
            .givens => {
                @memcpy(original, transformed);
                var i = self.rotations.len;
                while (i > 0) {
                    i -= 1;
                    const rot = self.rotations[i];
                    const left = original[rot.offset1];
                    const right = original[rot.offset2];
                    original[rot.offset1] = rot.cos * left - rot.sin * right;
                    original[rot.offset2] = rot.sin * left + rot.cos * right;
                }
            },
        }
        return original;
    }
};

// --- SIMD vector operations ---

/// Compute the L2 (Euclidean) norm of a vector.
pub fn norm(v: []const f32) f32 {
    return @sqrt(dot(v, v));
}

/// Compute the dot product of two vectors.
pub fn dot(a: []const f32, b: []const f32) f32 {
    std.debug.assert(a.len == b.len);
    return dotGeneric(a, b);
}

fn dotGeneric(a: []const f32, b: []const f32) f32 {
    const n = a.len;
    const simd_width = 8;
    const SimdF32 = @Vector(simd_width, f32);

    var sum0: SimdF32 = @splat(0.0);
    var sum1: SimdF32 = @splat(0.0);
    var sum2: SimdF32 = @splat(0.0);
    var sum3: SimdF32 = @splat(0.0);
    var i: usize = 0;

    while (i + simd_width * 4 <= n) : (i += simd_width * 4) {
        const av: SimdF32 = a[i..][0..simd_width].*;
        const bv: SimdF32 = b[i..][0..simd_width].*;
        const av1: SimdF32 = a[i + simd_width ..][0..simd_width].*;
        const bv1: SimdF32 = b[i + simd_width ..][0..simd_width].*;
        const av2: SimdF32 = a[i + simd_width * 2 ..][0..simd_width].*;
        const bv2: SimdF32 = b[i + simd_width * 2 ..][0..simd_width].*;
        const av3: SimdF32 = a[i + simd_width * 3 ..][0..simd_width].*;
        const bv3: SimdF32 = b[i + simd_width * 3 ..][0..simd_width].*;
        sum0 += av * bv;
        sum1 += av1 * bv1;
        sum2 += av2 * bv2;
        sum3 += av3 * bv3;
    }

    var sum = sum0 + sum1 + sum2 + sum3;
    while (i + simd_width <= n) : (i += simd_width) {
        const av: SimdF32 = a[i..][0..simd_width].*;
        const bv: SimdF32 = b[i..][0..simd_width].*;
        sum += av * bv;
    }

    var result: f32 = @reduce(.Add, sum);

    // Scalar tail.
    while (i < n) : (i += 1) {
        result += a[i] * b[i];
    }

    return result;
}

pub fn batchDot(query: []const f32, candidates: []const []const f32, dots: []f32) void {
    std.debug.assert(candidates.len <= dots.len);
    for (candidates, 0..) |candidate, i| {
        dots[i] = dot(query, candidate);
    }
}

/// Compute dst = a - b.
pub fn subTo(dst: []f32, a: []const f32, b: []const f32) void {
    const n = dst.len;
    const simd_width = 8;
    const SimdF32 = @Vector(simd_width, f32);

    var i: usize = 0;
    while (i + simd_width <= n) : (i += simd_width) {
        const av: SimdF32 = a[i..][0..simd_width].*;
        const bv: SimdF32 = b[i..][0..simd_width].*;
        dst[i..][0..simd_width].* = av - bv;
    }

    while (i < n) : (i += 1) {
        dst[i] = a[i] - b[i];
    }
}

/// Scale vector in place: v *= scalar.
pub fn scale(scalar: f32, v: []f32) void {
    const n = v.len;
    const simd_width = 8;
    const SimdF32 = @Vector(simd_width, f32);
    const s: SimdF32 = @splat(scalar);

    var i: usize = 0;
    while (i + simd_width <= n) : (i += simd_width) {
        const vv: SimdF32 = v[i..][0..simd_width].*;
        v[i..][0..simd_width].* = vv * s;
    }

    while (i < n) : (i += 1) {
        v[i] *= scalar;
    }
}

/// Compute distance between two vectors using the given metric.
pub fn distance(a: []const f32, b: []const f32, metric: DistanceMetric) f32 {
    return switch (metric) {
        .l2_squared => blk: {
            // ||a - b||^2 = ||a||^2 + ||b||^2 - 2*(a·b)
            const d = dot(a, a) + dot(b, b) - 2.0 * dot(a, b);
            break :blk @max(0.0, d);
        },
        .inner_product => -dot(a, b),
        .cosine => blk: {
            const d = dot(a, b);
            const na = norm(a);
            const nb = norm(b);
            if (na == 0.0 or nb == 0.0) break :blk 1.0;
            break :blk 1.0 - d / (na * nb);
        },
    };
}

/// Compute distance from a fixed query using a precomputed query magnitude.
/// For `.l2_squared`, `query_measure` must be `dot(query, query)`.
/// For `.cosine`, `query_measure` must be `norm(query)`.
/// For `.inner_product`, `query_measure` is ignored.
pub fn distanceToQuery(query: []const f32, query_measure: f32, candidate: []const f32, metric: DistanceMetric) f32 {
    return switch (metric) {
        .l2_squared => l2SquaredDistanceToQuery(query, candidate),
        .inner_product => -dot(query, candidate),
        .cosine => blk: {
            const d = dot(query, candidate);
            const nb = norm(candidate);
            if (query_measure == 0.0 or nb == 0.0) break :blk 1.0;
            break :blk 1.0 - d / (query_measure * nb);
        },
    };
}

/// Compute cosine similarity between two vectors.
pub fn cosineSimilarity(a: []const f32, b: []const f32) f32 {
    const na = norm(a);
    const nb = norm(b);
    if (na == 0.0 or nb == 0.0) return 0.0;
    return dot(a, b) / (na * nb);
}

/// Add vector b into a in place: a[i] += b[i].
pub fn add(a: []f32, b: []const f32) void {
    const n = a.len;
    const simd_width = 8;
    const SimdF32 = @Vector(simd_width, f32);

    var i: usize = 0;
    while (i + simd_width <= n) : (i += simd_width) {
        const av: SimdF32 = a[i..][0..simd_width].*;
        const bv: SimdF32 = b[i..][0..simd_width].*;
        a[i..][0..simd_width].* = av + bv;
    }
    while (i < n) : (i += 1) {
        a[i] += b[i];
    }
}

/// Normalize a vector to unit length in place. Returns the original norm.
pub fn normalize(v: []f32) f32 {
    const n = norm(v);
    if (n != 0) {
        scale(1.0 / n, v);
    }
    return n;
}

pub fn validateUnitVectorSet(vectors: *const Set) !void {
    for (0..vectors.count) |i| {
        const n = norm(vectors.atConst(i));
        if (@abs(n - 1.0) > 1e-3) return error.NonUnitVector;
    }
}

/// L2 squared distance between two vectors.
pub fn l2SquaredDistance(a: []const f32, b: []const f32) f32 {
    std.debug.assert(a.len == b.len);
    return l2SquaredDistanceGeneric(a, b);
}

pub fn l2SquaredDistanceToQuery(query: []const f32, candidate: []const f32) f32 {
    return l2SquaredDistance(query, candidate);
}

fn l2SquaredDistanceGeneric(a: []const f32, b: []const f32) f32 {
    const n = a.len;
    const simd_width = 8;
    const SimdF32 = @Vector(simd_width, f32);

    var sum0: SimdF32 = @splat(0.0);
    var sum1: SimdF32 = @splat(0.0);
    var sum2: SimdF32 = @splat(0.0);
    var sum3: SimdF32 = @splat(0.0);
    var i: usize = 0;

    while (i + simd_width * 4 <= n) : (i += simd_width * 4) {
        const av0: SimdF32 = a[i..][0..simd_width].*;
        const bv0: SimdF32 = b[i..][0..simd_width].*;
        const av1: SimdF32 = a[i + simd_width ..][0..simd_width].*;
        const bv1: SimdF32 = b[i + simd_width ..][0..simd_width].*;
        const av2: SimdF32 = a[i + simd_width * 2 ..][0..simd_width].*;
        const bv2: SimdF32 = b[i + simd_width * 2 ..][0..simd_width].*;
        const av3: SimdF32 = a[i + simd_width * 3 ..][0..simd_width].*;
        const bv3: SimdF32 = b[i + simd_width * 3 ..][0..simd_width].*;
        const da0 = av0 - bv0;
        const da1 = av1 - bv1;
        const da2 = av2 - bv2;
        const da3 = av3 - bv3;
        sum0 += da0 * da0;
        sum1 += da1 * da1;
        sum2 += da2 * da2;
        sum3 += da3 * da3;
    }

    var sum = sum0 + sum1 + sum2 + sum3;
    while (i + simd_width <= n) : (i += simd_width) {
        const av: SimdF32 = a[i..][0..simd_width].*;
        const bv: SimdF32 = b[i..][0..simd_width].*;
        const diff = av - bv;
        sum += diff * diff;
    }

    var result: f32 = @reduce(.Add, sum);
    while (i < n) : (i += 1) {
        const diff = a[i] - b[i];
        result += diff * diff;
    }
    return @max(0.0, result);
}

pub fn batchL2SquaredDistance(query: []const f32, candidates: []const []const f32, distances: []f32) void {
    std.debug.assert(candidates.len <= distances.len);
    for (candidates, 0..) |candidate, i| {
        distances[i] = l2SquaredDistanceToQuery(query, candidate);
    }
}

/// Find min and max values in a vector.
pub fn minMax(v: []const f32) struct { min: f32, max: f32 } {
    if (v.len == 0) return .{ .min = 0, .max = 0 };

    var min_val: f32 = v[0];
    var max_val: f32 = v[0];

    for (v[1..]) |val| {
        if (val < min_val) min_val = val;
        if (val > max_val) max_val = val;
    }

    return .{ .min = min_val, .max = max_val };
}

// --- Tests ---

test "dot product" {
    const a = [_]f32{ 1.0, 2.0, 3.0, 4.0, 5.0, 6.0, 7.0, 8.0, 9.0 };
    const b = [_]f32{ 9.0, 8.0, 7.0, 6.0, 5.0, 4.0, 3.0, 2.0, 1.0 };
    // 9+16+21+24+25+24+21+16+9 = 165
    try std.testing.expectApproxEqAbs(dot(&a, &b), 165.0, 1e-4);
}

test "norm" {
    const v = [_]f32{ 3.0, 4.0 };
    try std.testing.expectApproxEqAbs(norm(&v), 5.0, 1e-6);
}

test "subTo" {
    const a = [_]f32{ 5.0, 3.0, 1.0 };
    const b = [_]f32{ 1.0, 2.0, 3.0 };
    var dst: [3]f32 = undefined;
    subTo(&dst, &a, &b);
    try std.testing.expectApproxEqAbs(dst[0], 4.0, 1e-6);
    try std.testing.expectApproxEqAbs(dst[1], 1.0, 1e-6);
    try std.testing.expectApproxEqAbs(dst[2], -2.0, 1e-6);
}

test "random orthogonal transformer round trips" {
    const alloc = std.testing.allocator;
    var transformer = try RandomOrthogonalTransformer.init(alloc, .givens, 4, 42);
    defer transformer.deinit();

    const original = [_]f32{ 1.0, 2.0, 3.0, 4.0 };
    var transformed: [4]f32 = undefined;
    var restored: [4]f32 = undefined;

    _ = transformer.transform(&original, &transformed);
    _ = transformer.untransform(&transformed, &restored);

    for (original, restored) |expected, actual| {
        try std.testing.expectApproxEqAbs(expected, actual, 1e-4);
    }
}
