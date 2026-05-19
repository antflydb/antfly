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

//! SIMD-accelerated primitives for RaBitQ vector quantization.
//!
//! RaBitQ is a 1-bit quantization algorithm for high-dimensional vectors that provides:
//!   - Compact representation (1 bit per dimension)
//!   - Theoretical error bounds for approximate nearest neighbor search
//!   - Fast distance estimation via weighted popcount operations
//!
//! Reference: "RaBitQ: Quantizing High-Dimensional Vectors with a Theoretical Error Bound
//! for Approximate Nearest Neighbor Search" by Jianyang Gao & Cheng Long.
//! URL: https://arxiv.org/pdf/2405.12497

const std = @import("std");
const math = std.math;

// SIMD vector width: 4 u64s = 256 bits (matches AVX2 / 2x NEON).
const simd_width = 4;
const SimdU64 = @Vector(simd_width, u64);
const SimdF32x8 = @Vector(8, f32);

/// Computes the weighted bit product used in RaBitQ distance estimation.
///
/// result = 1*popcount(code & q1) + 2*popcount(code & q2) +
///          4*popcount(code & q3) + 8*popcount(code & q4)
///
/// This is the hot-path operation in RaBitQ search, called for every candidate vector.
pub fn bitProduct(code: []const u64, q1: []const u64, q2: []const u64, q3: []const u64, q4: []const u64) u32 {
    const n = code.len;
    if (n == 0) return 0;

    var sum1: u64 = 0;
    var sum2: u64 = 0;
    var sum4: u64 = 0;
    var sum8: u64 = 0;

    // Process 4x SIMD vectors at a time (16 u64s per iteration on 256-bit SIMD).
    const stride = simd_width * 4;
    var i: usize = 0;

    while (i + stride <= n) : (i += stride) {
        inline for (0..4) |k| {
            const off = i + k * simd_width;
            const code_vec: SimdU64 = code[off..][0..simd_width].*;
            const q1_vec: SimdU64 = q1[off..][0..simd_width].*;
            const q2_vec: SimdU64 = q2[off..][0..simd_width].*;
            const q3_vec: SimdU64 = q3[off..][0..simd_width].*;
            const q4_vec: SimdU64 = q4[off..][0..simd_width].*;

            sum1 += reduceSum(popCount(code_vec & q1_vec));
            sum2 += reduceSum(popCount(code_vec & q2_vec));
            sum4 += reduceSum(popCount(code_vec & q3_vec));
            sum8 += reduceSum(popCount(code_vec & q4_vec));
        }
    }

    // Process remaining full SIMD vectors one at a time.
    while (i + simd_width <= n) : (i += simd_width) {
        const code_vec: SimdU64 = code[i..][0..simd_width].*;
        const q1_vec: SimdU64 = q1[i..][0..simd_width].*;
        const q2_vec: SimdU64 = q2[i..][0..simd_width].*;
        const q3_vec: SimdU64 = q3[i..][0..simd_width].*;
        const q4_vec: SimdU64 = q4[i..][0..simd_width].*;

        sum1 += reduceSum(popCount(code_vec & q1_vec));
        sum2 += reduceSum(popCount(code_vec & q2_vec));
        sum4 += reduceSum(popCount(code_vec & q3_vec));
        sum8 += reduceSum(popCount(code_vec & q4_vec));
    }

    // Scalar tail.
    while (i < n) : (i += 1) {
        sum1 += @popCount(code[i] & q1[i]);
        sum2 += @popCount(code[i] & q2[i]);
        sum4 += @popCount(code[i] & q3[i]);
        sum8 += @popCount(code[i] & q4[i]);
    }

    return @intCast(sum1 + (sum2 << 1) + (sum4 << 2) + (sum8 << 3));
}

/// Quantizes unit vectors into 1-bit codes.
///
/// For each input unit vector, this function:
///  1. Extracts sign bits (1 for positive/zero, 0 for negative)
///  2. Packs bits into uint64 codes (MSB-first within each uint64)
///  3. Computes the dot product between the unit vector and its quantized form
///  4. Counts the number of 1-bits in the code
///
/// The dot_products output contains 1/<o̅,o> (inverted) for use in distance estimation.
/// If the dot product is zero (vector equals centroid), dot_products[i] is set to 0.
pub fn quantizeVectors(
    unit_vectors: []const f32,
    codes: []u64,
    dot_products: []f32,
    code_counts: []u32,
    sqrt_dims_inv: f32,
    count: usize,
    dims: usize,
    width: usize,
) void {
    const neg_sqrt_dims_inv = -sqrt_dims_inv;

    for (0..count) |vi| {
        const vec = unit_vectors[vi * dims .. (vi + 1) * dims];
        const code = codes[vi * width .. (vi + 1) * width];

        var dot_product: f64 = 0;
        var code_bits: u64 = 0;
        var code_count: u32 = 0;
        var code_idx: usize = 0;
        var bit_pos: usize = 0;

        // Process SIMD-width f32 chunks.
        var dim: usize = 0;
        while (dim + 8 <= dims) : (dim += 8) {
            const chunk = vec[dim..][0..8];
            const vec_data: SimdF32x8 = chunk.*;
            const zero_vec: SimdF32x8 = @splat(0.0);
            const pos_mult: SimdF32x8 = @splat(sqrt_dims_inv);
            const neg_mult: SimdF32x8 = @splat(neg_sqrt_dims_inv);

            // Select multiplier based on sign: negative elements get negated multiplier.
            const neg_mask = vec_data < zero_vec;
            const mult_vec = @select(f32, neg_mask, neg_mult, pos_mult);
            const prod_vec = vec_data * mult_vec;
            dot_product += @as(f64, @reduce(.Add, prod_vec));

            // Extract sign bits and pack into code.
            inline for (0..8) |j| {
                const sign_bit = getSignBit(vec_data[j]);
                code_bits = (code_bits << 1) | @as(u64, 1 - sign_bit);
                bit_pos += 1;

                if (bit_pos == 64) {
                    code[code_idx] = code_bits;
                    code_count += @popCount(code_bits);
                    code_idx += 1;
                    code_bits = 0;
                    bit_pos = 0;
                }
            }
        }

        // Scalar tail for remaining dimensions.
        while (dim < dims) : (dim += 1) {
            const element = vec[dim];
            const sign_bit = getSignBit(element);

            const mult: f32 = if (sign_bit == 1) neg_sqrt_dims_inv else sqrt_dims_inv;
            dot_product += @as(f64, element) * @as(f64, mult);

            code_bits = (code_bits << 1) | @as(u64, 1 - sign_bit);
            bit_pos += 1;

            if (bit_pos == 64) {
                code[code_idx] = code_bits;
                code_count += @popCount(code_bits);
                code_idx += 1;
                code_bits = 0;
                bit_pos = 0;
            }
        }

        // Handle remaining bits - shift to MSB positions.
        if (bit_pos > 0) {
            const shift: u6 = @intCast(64 - bit_pos);
            code_bits = code_bits << shift;
            code[code_idx] = code_bits;
            code_count += @popCount(code_bits);
        }

        // Store results.
        code_counts[vi] = code_count;
        dot_products[vi] = if (dot_product != 0) @floatCast(1.0 / dot_product) else 0;
    }
}

/// Returns the number of uint64s needed to store a code for the given dimensions.
/// This is ⌈dims/64⌉.
pub fn codeWidth(dims: usize) usize {
    return (dims + 63) / 64;
}

/// Returns a float32 with the magnitude of x and a sign that is the product
/// of the signs of x and y.
pub fn multiplySigns(x: f32, y: f32) f32 {
    const sign_mask: u32 = 1 << 31;
    const x_bits = @as(u32, @bitCast(x));
    const y_bits = @as(u32, @bitCast(y));
    return @bitCast(x_bits ^ (y_bits & sign_mask));
}

// --- Internal helpers ---

/// Returns 1 if the float is negative (including -0), 0 otherwise.
inline fn getSignBit(f: f32) u32 {
    return @as(u32, @bitCast(f)) >> 31;
}

/// SIMD popcount on a vector of u64.
inline fn popCount(v: SimdU64) SimdU64 {
    return @popCount(v);
}

/// Horizontal sum of a SIMD u64 vector.
inline fn reduceSum(v: SimdU64) u64 {
    return @reduce(.Add, v);
}

// --- Tests ---

test "codeWidth" {
    const expect = std.testing.expect;
    try expect(codeWidth(64) == 1);
    try expect(codeWidth(128) == 2);
    try expect(codeWidth(384) == 6);
    try expect(codeWidth(512) == 8);
    try expect(codeWidth(1) == 1);
    try expect(codeWidth(65) == 2);
}

test "getSignBit" {
    const expect = std.testing.expect;
    try expect(getSignBit(1.0) == 0);
    try expect(getSignBit(0.0) == 0);
    try expect(getSignBit(-1.0) == 1);
    try expect(getSignBit(-0.0) == 1);
}

test "multiplySigns" {
    const expectApprox = std.testing.expectApproxEqAbs;
    try expectApprox(multiplySigns(3.0, 1.0), 3.0, 1e-6);
    try expectApprox(multiplySigns(3.0, -1.0), -3.0, 1e-6);
    try expectApprox(multiplySigns(-3.0, -1.0), 3.0, 1e-6);
    try expectApprox(multiplySigns(-3.0, 1.0), -3.0, 1e-6);
}

test "bitProduct basic" {
    const expect = std.testing.expect;

    // All zeros.
    {
        const code = [_]u64{0};
        const q1 = [_]u64{0};
        const q2 = [_]u64{0};
        const q3 = [_]u64{0};
        const q4 = [_]u64{0};
        try expect(bitProduct(&code, &q1, &q2, &q3, &q4) == 0);
    }

    // All ones, single element.
    {
        const all_ones = ~@as(u64, 0);
        const code = [_]u64{all_ones};
        const q1 = [_]u64{all_ones};
        const q2 = [_]u64{all_ones};
        const q3 = [_]u64{all_ones};
        const q4 = [_]u64{all_ones};
        // 1*64 + 2*64 + 4*64 + 8*64 = 64*(1+2+4+8) = 64*15 = 960
        try expect(bitProduct(&code, &q1, &q2, &q3, &q4) == 960);
    }

    // Only q1 has bits set.
    {
        const all_ones = ~@as(u64, 0);
        const code = [_]u64{all_ones};
        const q1 = [_]u64{all_ones};
        const q2 = [_]u64{0};
        const q3 = [_]u64{0};
        const q4 = [_]u64{0};
        // 1*64 = 64
        try expect(bitProduct(&code, &q1, &q2, &q3, &q4) == 64);
    }

    // Empty slices.
    {
        const empty: []const u64 = &.{};
        try expect(bitProduct(empty, empty, empty, empty, empty) == 0);
    }
}

test "bitProduct multi-element with SIMD" {
    const expect = std.testing.expect;

    // 8 elements (exercises the SIMD path for width=4).
    const all_ones = ~@as(u64, 0);
    var code: [8]u64 = undefined;
    var q1: [8]u64 = undefined;
    var q2: [8]u64 = undefined;
    var q3: [8]u64 = undefined;
    var q4: [8]u64 = undefined;

    for (0..8) |j| {
        code[j] = all_ones;
        q1[j] = all_ones;
        q2[j] = all_ones;
        q3[j] = all_ones;
        q4[j] = all_ones;
    }

    // 8 * 64 * 15 = 7680
    try expect(bitProduct(&code, &q1, &q2, &q3, &q4) == 7680);
}

test "bitProduct selective bits" {
    const expect = std.testing.expect;

    // Only bottom bit set in code and q1.
    const code = [_]u64{1};
    const q1 = [_]u64{1};
    const q2 = [_]u64{0};
    const q3 = [_]u64{0};
    const q4 = [_]u64{0};
    // popcount(1 & 1) = 1, weight 1 => 1
    try expect(bitProduct(&code, &q1, &q2, &q3, &q4) == 1);
}

test "quantizeVectors single positive vector" {
    const expectApprox = std.testing.expectApproxEqAbs;
    const expect = std.testing.expect;

    const dims = 64;
    const width = comptime codeWidth(dims);
    const count = 1;
    const sqrt_dims_inv: f32 = 1.0 / @sqrt(@as(f32, @floatFromInt(dims)));

    // All-positive unit vector (uniform).
    var unit_vec: [dims]f32 = undefined;
    const val: f32 = 1.0 / @sqrt(@as(f32, @floatFromInt(dims)));
    for (&unit_vec) |*v| v.* = val;

    var codes: [width]u64 = undefined;
    var dot_products: [count]f32 = undefined;
    var code_counts: [count]u32 = undefined;

    quantizeVectors(&unit_vec, &codes, &dot_products, &code_counts, sqrt_dims_inv, count, dims, width);

    // All positive => all bits should be 1.
    try expect(codes[0] == ~@as(u64, 0));
    // Code count should be 64.
    try expect(code_counts[0] == 64);
    // Dot product should be 1/dot where dot = sum(val * sqrt_dims_inv) = dims * val * sqrt_dims_inv
    // = 64 * (1/8) * (1/8) = 1.0, so inverted = 1.0
    try expectApprox(dot_products[0], 1.0, 0.01);
}

test "quantizeVectors single negative vector" {
    const expect = std.testing.expect;

    const dims = 64;
    const width = comptime codeWidth(dims);
    const count = 1;
    const sqrt_dims_inv: f32 = 1.0 / @sqrt(@as(f32, @floatFromInt(dims)));

    // All-negative unit vector.
    var unit_vec: [dims]f32 = undefined;
    const val: f32 = -1.0 / @sqrt(@as(f32, @floatFromInt(dims)));
    for (&unit_vec) |*v| v.* = val;

    var codes: [width]u64 = undefined;
    var dot_products: [count]f32 = undefined;
    var code_counts: [count]u32 = undefined;

    quantizeVectors(&unit_vec, &codes, &dot_products, &code_counts, sqrt_dims_inv, count, dims, width);

    // All negative => all bits should be 0.
    try expect(codes[0] == 0);
    // Code count should be 0.
    try expect(code_counts[0] == 0);
}

test "quantizeVectors 384 dims" {
    // Exercises SIMD + scalar tail with typical embedding dimensions.
    const dims = 384;
    const width = comptime codeWidth(dims);
    const count = 1;
    const sqrt_dims_inv: f32 = 1.0 / @sqrt(@as(f32, @floatFromInt(dims)));

    // Alternating positive/negative.
    var unit_vec: [dims]f32 = undefined;
    const mag: f32 = 1.0 / @sqrt(@as(f32, @floatFromInt(dims)));
    for (0..dims) |j| {
        unit_vec[j] = if (j % 2 == 0) mag else -mag;
    }

    var codes: [width]u64 = undefined;
    var dot_products: [count]f32 = undefined;
    var code_counts: [count]u32 = undefined;

    quantizeVectors(&unit_vec, &codes, &dot_products, &code_counts, sqrt_dims_inv, count, dims, width);

    // Half positive, half negative => 192 bits set.
    const expect = std.testing.expect;
    try expect(code_counts[0] == 192);
}
