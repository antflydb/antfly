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
const builtin = @import("builtin");

// SIMD width (f32 lanes), comptime-selected from the build target:
//   - WASM (32 or 64): 128-bit SIMD = 4 lanes
//   - x86_64 with AVX-512F: 512-bit zmm = 16 lanes
//   - else (AVX/AVX2, NEON): 256-bit = 8 lanes
// Mirrors pkg/inference/src/web/profile.zig::simd_f32_lanes; kept here so
// lib/linalg has no termite import dependency.  termite's activations.zig
// re-exports `vec_len` and `expVec` from this file so the two sides cannot
// diverge.
pub const vec_len = blk: {
    if (builtin.cpu.arch == .wasm32 or builtin.cpu.arch == .wasm64) break :blk 4;
    if (builtin.cpu.arch == .x86_64 and std.Target.x86.featureSetHas(builtin.cpu.features, .avx512f)) break :blk 16;
    break :blk 8;
};
const F32xN = @Vector(vec_len, f32);

pub fn dot(a: []const f32, b: []const f32) f32 {
    std.debug.assert(a.len == b.len);
    return dotPtrs(a.ptr, b.ptr, a.len);
}

pub fn axpy(alpha: f32, x: []const f32, y: []f32) void {
    std.debug.assert(x.len == y.len);
    axpyPtrs(alpha, x.ptr, y.ptr, x.len);
}

pub fn dotPtrs(a: [*]const f32, b: [*]const f32, len: usize) f32 {
    var acc: F32xN = @splat(0.0);
    var i: usize = 0;
    while (i + vec_len <= len) : (i += vec_len) {
        const va: F32xN = a[i..][0..vec_len].*;
        const vb: F32xN = b[i..][0..vec_len].*;
        acc += va * vb;
    }
    var sum: f32 = @reduce(.Add, acc);
    while (i < len) : (i += 1) {
        sum += a[i] * b[i];
    }
    return sum;
}

pub fn axpyPtrs(alpha: f32, x: [*]const f32, y: [*]f32, len: usize) void {
    const a_splat: F32xN = @splat(alpha);
    var i: usize = 0;
    while (i + vec_len <= len) : (i += vec_len) {
        const xv: F32xN = x[i..][0..vec_len].*;
        const yv: F32xN = y[i..][0..vec_len].*;
        y[i..][0..vec_len].* = yv + a_splat * xv;
    }
    while (i < len) : (i += 1) {
        y[i] += alpha * x[i];
    }
}

// Vectorized single-precision exp via Cephes-style range reduction.
// exp(x) = 2^k * exp(r), |r| <= ln(2)/2, with a degree-6 Horner polynomial
// for exp(r).  Saturates: x > 88.7 -> +inf, x < -87.34 -> 0 (so masked
// attention scores at -inf underflow cleanly to 0 instead of producing NaN).
//
// Mirrors pkg/inference/src/backends/activations.zig::expVec; copied here so
// lib/linalg stays free of termite imports.  Used by softmaxRow and
// expSubtractAndSum on the attention hot path where @exp(@Vector) currently
// lowers to per-lane libm expf.
pub inline fn expVec(x_in: F32xN) F32xN {
    const I32xN = @Vector(vec_len, i32);
    const ln2: F32xN = @splat(0.69314718055994530942);
    const inv_ln2: F32xN = @splat(1.4426950408889634074);
    const exp_hi: F32xN = @splat(88.7228);
    const zero_threshold: F32xN = @splat(-87.34);
    const zero_v: F32xN = @splat(0.0);
    const inf_v: F32xN = @splat(std.math.inf(f32));
    const nan_v: F32xN = @splat(std.math.nan(f32));

    const nan_mask = x_in != x_in;
    const overflow_mask = x_in > exp_hi;
    const underflow_mask = x_in < zero_threshold;
    const clamped_hi = @select(f32, overflow_mask, exp_hi, x_in);
    const clamped = @select(f32, underflow_mask, zero_threshold, clamped_hi);
    const x = @select(f32, nan_mask, zero_v, clamped);

    const half: F32xN = @splat(0.5);
    const ki: I32xN = @intFromFloat(x * inv_ln2 + @select(f32, x < zero_v, -half, half));
    const k: F32xN = @floatFromInt(ki);
    const r = x - k * ln2;

    const c1: F32xN = @splat(1.0);
    const c2: F32xN = @splat(0.5);
    const c3: F32xN = @splat(0.16666667);
    const c4: F32xN = @splat(0.041666668);
    const c5: F32xN = @splat(0.008333334);
    const c6: F32xN = @splat(0.0013888889);
    var p: F32xN = c6;
    p = @mulAdd(F32xN, p, r, c5);
    p = @mulAdd(F32xN, p, r, c4);
    p = @mulAdd(F32xN, p, r, c3);
    p = @mulAdd(F32xN, p, r, c2);
    p = @mulAdd(F32xN, p, r, c1);
    p = @mulAdd(F32xN, p, r, c1);

    const bias: I32xN = @splat(127);
    const pow2_bits: I32xN = (ki + bias) << @splat(23);
    const pow2: F32xN = @bitCast(pow2_bits);

    const finite_result = p * pow2;
    const saturated = @select(f32, underflow_mask, zero_v, finite_result);
    const overflowed = @select(f32, overflow_mask, inf_v, saturated);
    return @select(f32, nan_mask, nan_v, overflowed);
}

/// Numerically-stable in-place softmax over `row` using vectorized exp.
/// -inf inputs (masked positions) collapse to 0 cleanly.
pub fn softmaxRow(row: []f32) void {
    if (row.len == 0) return;

    var max_vec: F32xN = @splat(row[0]);
    var i: usize = 0;
    while (i + vec_len <= row.len) : (i += vec_len) {
        const v: F32xN = row[i..][0..vec_len].*;
        max_vec = @max(max_vec, v);
    }
    var max_val: f32 = @reduce(.Max, max_vec);
    while (i < row.len) : (i += 1) {
        if (row[i] > max_val) max_val = row[i];
    }
    if (max_val == -std.math.inf(f32)) {
        @memset(row, 0.0);
        return;
    }

    const max_splat: F32xN = @splat(max_val);
    var sum_acc: F32xN = @splat(0.0);
    i = 0;
    while (i + vec_len <= row.len) : (i += vec_len) {
        const v: F32xN = row[i..][0..vec_len].*;
        const e = expVec(v - max_splat);
        row[i..][0..vec_len].* = e;
        sum_acc += e;
    }
    var sum: f32 = @reduce(.Add, sum_acc);
    while (i < row.len) : (i += 1) {
        const e = @exp(row[i] - max_val);
        row[i] = e;
        sum += e;
    }
    if (sum == 0.0) return;

    const inv = 1.0 / sum;
    const inv_splat: F32xN = @splat(inv);
    i = 0;
    while (i + vec_len <= row.len) : (i += vec_len) {
        const rv: F32xN = row[i..][0..vec_len].*;
        row[i..][0..vec_len].* = rv * inv_splat;
    }
    while (i < row.len) : (i += 1) {
        row[i] *= inv;
    }
}

/// Block step of a streaming flash-attention softmax: overwrites
/// `scores[0..len]` with `exp(score - new_max)` and returns their sum.
/// Replaces the per-element scalar `@exp` loop in flashAttentionHost /
/// flashCausalAttentionHost — that loop runs cur_bkv times per Q-row per
/// KV-block per head per batch, which is the dominant cost of attention.
pub fn expSubtractAndSum(scores: []f32, new_max: f32) f32 {
    if (new_max == -std.math.inf(f32)) {
        @memset(scores, 0.0);
        return 0.0;
    }
    const max_splat: F32xN = @splat(new_max);
    var sum_acc: F32xN = @splat(0.0);
    var i: usize = 0;
    while (i + vec_len <= scores.len) : (i += vec_len) {
        const sv: F32xN = scores[i..][0..vec_len].*;
        const ev = expVec(sv - max_splat);
        scores[i..][0..vec_len].* = ev;
        sum_acc += ev;
    }
    var sum: f32 = @reduce(.Add, sum_acc);
    while (i < scores.len) : (i += 1) {
        const e = @exp(scores[i] - new_max);
        scores[i] = e;
        sum += e;
    }
    return sum;
}

test "dot and axpy match expected results" {
    const a = [_]f32{ 1, 2, 3, 4 };
    const b = [_]f32{ 5, 6, 7, 8 };
    try std.testing.expectApproxEqAbs(@as(f32, 70.0), dot(&a, &b), 1e-5);

    var y = [_]f32{ 1, 1, 1, 1 };
    axpy(0.5, &a, &y);
    try std.testing.expectEqualSlices(f32, &[_]f32{ 1.5, 2.0, 2.5, 3.0 }, &y);
}

test "softmaxRow matches scalar reference" {
    const allocator = std.testing.allocator;
    const len: usize = 257; // CLIP-ViT seq length, exercises SIMD tail
    const a = try allocator.alloc(f32, len);
    defer allocator.free(a);
    const b = try allocator.alloc(f32, len);
    defer allocator.free(b);
    var prng = std.Random.DefaultPrng.init(0x57F7_AAAA);
    for (a) |*v| v.* = (prng.random().float(f32) - 0.5) * 8.0;
    @memcpy(b, a);

    // Reference: scalar exp.
    var max_val: f32 = -std.math.inf(f32);
    for (a) |v| max_val = @max(max_val, v);
    var sum: f32 = 0.0;
    for (a) |*v| {
        v.* = @exp(v.* - max_val);
        sum += v.*;
    }
    const inv = 1.0 / sum;
    for (a) |*v| v.* *= inv;

    softmaxRow(b);
    for (a, b) |sc, vc| try std.testing.expect(@abs(sc - vc) < 5e-6);
}

test "softmaxRow handles -inf saturation (masked attention)" {
    var row = [_]f32{ -std.math.inf(f32), 1.0, 2.0, -std.math.inf(f32) };
    softmaxRow(&row);
    try std.testing.expectApproxEqAbs(@as(f32, 0.0), row[0], 1e-6);
    try std.testing.expectApproxEqAbs(@as(f32, 0.0), row[3], 1e-6);
    var s: f32 = 0;
    for (row) |v| s += v;
    try std.testing.expectApproxEqAbs(@as(f32, 1.0), s, 1e-6);
}

test "expSubtractAndSum matches scalar reference for flash softmax block" {
    const allocator = std.testing.allocator;
    const len: usize = 256; // BLOCK_KV size
    const a = try allocator.alloc(f32, len);
    defer allocator.free(a);
    const b = try allocator.alloc(f32, len);
    defer allocator.free(b);
    var prng = std.Random.DefaultPrng.init(0xBE57_F00D);
    for (a) |*v| v.* = (prng.random().float(f32) - 0.5) * 6.0;
    @memcpy(b, a);
    // Mask out a few positions to exercise the -inf path.
    a[0] = -std.math.inf(f32);
    b[0] = -std.math.inf(f32);
    a[100] = -std.math.inf(f32);
    b[100] = -std.math.inf(f32);

    // Pick a max that's slightly above the data so exp gets values <= 0
    // (matches how flash attention uses new_max).
    var new_max: f32 = -std.math.inf(f32);
    for (a) |v| new_max = @max(new_max, v);

    var ref_sum: f32 = 0;
    for (a) |*v| {
        if (v.* == -std.math.inf(f32)) {
            v.* = 0;
        } else {
            v.* = @exp(v.* - new_max);
            ref_sum += v.*;
        }
    }
    const got_sum = expSubtractAndSum(b, new_max);
    try std.testing.expect(@abs(ref_sum - got_sum) < 1e-3);
    for (a, b) |sc, vc| try std.testing.expect(@abs(sc - vc) < 5e-6);
}
