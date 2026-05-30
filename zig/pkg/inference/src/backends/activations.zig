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

// Activation functions for neural network inference.
// Uses Zig's @Vector for vectorized operations on supported platforms.

const std = @import("std");
const linalg_primitives = @import("inference_linalg").primitives;

// Comptime-selected SIMD width and vectorized exp helper, sourced from
// `lib/linalg/src/primitives.zig` so the two sides cannot diverge.  The
// previous incarnation copied expVec into this file with a "Mirrors..."
// comment; that's a duplication tax (any bug fix had to land twice) and
// risked drifting whenever someone touched only one of them.
const VEC_LEN = linalg_primitives.vec_len;
const F32xN = @Vector(VEC_LEN, f32);

pub const expVec = linalg_primitives.expVec;

// tanh(x) = 1 - 2 / (exp(2x) + 1).  Clamp 2x to [-30, 30] so exp doesn't
// overflow; tanh saturates to ±1 well before then anyway.
inline fn tanhVec(x: F32xN) F32xN {
    const lo: F32xN = @splat(-30.0);
    const hi: F32xN = @splat(30.0);
    const two: F32xN = @splat(2.0);
    const one: F32xN = @splat(1.0);
    const x2 = @min(@max(two * x, lo), hi);
    return one - two / (expVec(x2) + one);
}

/// GELU activation: x * 0.5 * (1 + tanh(sqrt(2/pi) * (x + 0.044715 * x^3)))
pub fn gelu(data: []f32) void {
    const sqrt_2_over_pi_s: f32 = 0.7978845608028654;
    const sqrt_2_over_pi: F32xN = @splat(sqrt_2_over_pi_s);
    const c044715: F32xN = @splat(0.044715);
    const half: F32xN = @splat(0.5);
    const one: F32xN = @splat(1.0);
    var i: usize = 0;
    while (i + VEC_LEN <= data.len) : (i += VEC_LEN) {
        const v: F32xN = data[i..][0..VEC_LEN].*;
        const v3 = v * v * v;
        const inner = sqrt_2_over_pi * (v + c044715 * v3);
        data[i..][0..VEC_LEN].* = half * v * (one + tanhVec(inner));
    }
    while (i < data.len) : (i += 1) {
        const val = data[i];
        const inner = sqrt_2_over_pi_s * (val + 0.044715 * val * val * val);
        data[i] = 0.5 * val * (1.0 + std.math.tanh(inner));
    }
}

/// ReLU activation: max(0, x)
pub fn relu(data: []f32) void {
    const zero: F32xN = @splat(0.0);
    var i: usize = 0;

    // Vectorized path
    while (i + VEC_LEN <= data.len) : (i += VEC_LEN) {
        const v: F32xN = data[i..][0..VEC_LEN].*;
        data[i..][0..VEC_LEN].* = @max(v, zero);
    }

    // Scalar remainder
    while (i < data.len) : (i += 1) {
        if (data[i] < 0.0) data[i] = 0.0;
    }
}

/// Sigmoid activation: 1 / (1 + exp(-x))
pub fn sigmoid(data: []f32) void {
    const one: F32xN = @splat(1.0);
    var i: usize = 0;
    while (i + VEC_LEN <= data.len) : (i += VEC_LEN) {
        const v: F32xN = data[i..][0..VEC_LEN].*;
        data[i..][0..VEC_LEN].* = one / (one + expVec(-v));
    }
    while (i < data.len) : (i += 1) {
        data[i] = 1.0 / (1.0 + @exp(-data[i]));
    }
}

/// SiLU/Swish activation: x * sigmoid(x) = x / (1 + exp(-x))
pub fn silu(data: []f32) void {
    const one: F32xN = @splat(1.0);
    var i: usize = 0;
    while (i + VEC_LEN <= data.len) : (i += VEC_LEN) {
        const v: F32xN = data[i..][0..VEC_LEN].*;
        data[i..][0..VEC_LEN].* = v / (one + expVec(-v));
    }
    while (i < data.len) : (i += 1) {
        const val = data[i];
        data[i] = val / (1.0 + @exp(-val));
    }
}

/// Quick GELU activation: x * sigmoid(1.702 * x)
/// Faster approximation used by CLIP and some other models.
pub fn quickGelu(data: []f32) void {
    const neg1702: F32xN = @splat(-1.702);
    const one: F32xN = @splat(1.0);
    var i: usize = 0;
    while (i + VEC_LEN <= data.len) : (i += VEC_LEN) {
        const v: F32xN = data[i..][0..VEC_LEN].*;
        data[i..][0..VEC_LEN].* = v / (one + expVec(neg1702 * v));
    }
    while (i < data.len) : (i += 1) {
        const val = data[i];
        data[i] = val / (1.0 + @exp(-1.702 * val));
    }
}

/// RMS normalization: x * rsqrt(mean(x^2) + eps) * weight. No bias, no mean subtraction.
pub fn rmsNorm(data: []f32, weight: []const f32, dim: usize, eps: f32) void {
    const batch = data.len / dim;
    for (0..batch) |b| {
        const row = data[b * dim .. (b + 1) * dim];

        const sum_sq = vectorDotSelf(row);
        const rms = @sqrt(sum_sq / @as(f32, @floatFromInt(dim)) + eps);
        const inv_rms = 1.0 / rms;

        const scale: F32xN = @splat(inv_rms);
        var i: usize = 0;
        while (i + VEC_LEN <= dim) : (i += VEC_LEN) {
            const v: F32xN = row[i..][0..VEC_LEN].*;
            const w: F32xN = weight[i..][0..VEC_LEN].*;
            row[i..][0..VEC_LEN].* = v * scale * w;
        }
        while (i < dim) : (i += 1) {
            row[i] = row[i] * inv_rms * weight[i];
        }
    }
}

/// Softmax over the last dimension. data is [batch, dim], applied per row.
///
/// Uses the vectorized expVec instead of @exp(@Vector) so the inner loop runs
/// as native SIMD FMAs rather than per-lane libm expf calls. Attention scores
/// are softmaxed once per head per layer in CLIP/CLAP/CLIPCLAP, so this is on
/// every multimodal forward pass.
pub fn softmax(data: []f32, dim: usize) void {
    const batch = data.len / dim;
    for (0..batch) |b| {
        const row = data[b * dim .. (b + 1) * dim];

        const max_val = vectorMax(row);
        if (max_val == -std.math.inf(f32)) {
            @memset(row, 0.0);
            continue;
        }

        // exp(x - max) and sum, fused into a single vectorized pass.
        const max_splat: F32xN = @splat(max_val);
        var sum_acc: F32xN = @splat(0.0);
        var i: usize = 0;
        while (i + VEC_LEN <= dim) : (i += VEC_LEN) {
            const v: F32xN = row[i..][0..VEC_LEN].*;
            const e = expVec(v - max_splat);
            row[i..][0..VEC_LEN].* = e;
            sum_acc += e;
        }
        var sum: f32 = @reduce(.Add, sum_acc);
        while (i < dim) : (i += 1) {
            row[i] = @exp(row[i] - max_val);
            sum += row[i];
        }

        // Normalize.  Multiply by reciprocal once instead of dividing per lane.
        if (sum > 0.0) {
            const inv_sum_scalar = 1.0 / sum;
            const inv_sum: F32xN = @splat(inv_sum_scalar);
            i = 0;
            while (i + VEC_LEN <= dim) : (i += VEC_LEN) {
                const v: F32xN = row[i..][0..VEC_LEN].*;
                row[i..][0..VEC_LEN].* = v * inv_sum;
            }
            while (i < dim) : (i += 1) {
                row[i] *= inv_sum_scalar;
            }
        }
    }
}

/// Layer normalization: y = gamma * (x - mean) / sqrt(var + eps) + beta
///
/// Fuses the mean and variance reductions into a single sweep over the row
/// using var = E[x^2] - E[x]^2.  The previous two-pass version touched each
/// row three times (sum -> sum-of-squared-deviations -> normalize); CLIP
/// has 24 LayerNorms per text+vision encoder pair, and CLAP has another
/// pair per audio block, so halving the read traffic of the reduction
/// stage matters across the multimodal forward pass.
pub fn layerNorm(
    data: []f32,
    gamma: []const f32,
    beta: []const f32,
    dim: usize,
    eps: f32,
) void {
    const batch = data.len / dim;
    const dim_f: f32 = @floatFromInt(dim);
    const inv_dim: f32 = 1.0 / dim_f;

    for (0..batch) |b| {
        const row = data[b * dim .. (b + 1) * dim];

        // Single-pass sum and sum-of-squares.  Numerically this is fine for
        // typical f32 activations in [-10, 10]; the inputs to LayerNorm in a
        // transformer are always residual-stream values that have already been
        // bounded by previous norms.
        var sum_acc: F32xN = @splat(0.0);
        var sumsq_acc: F32xN = @splat(0.0);
        var i: usize = 0;
        while (i + VEC_LEN <= dim) : (i += VEC_LEN) {
            const v: F32xN = row[i..][0..VEC_LEN].*;
            sum_acc += v;
            sumsq_acc = @mulAdd(F32xN, v, v, sumsq_acc);
        }
        var sum: f32 = @reduce(.Add, sum_acc);
        var sumsq: f32 = @reduce(.Add, sumsq_acc);
        while (i < dim) : (i += 1) {
            const v = row[i];
            sum += v;
            sumsq += v * v;
        }

        const mean = sum * inv_dim;
        const variance = @max(sumsq * inv_dim - mean * mean, 0.0);
        const inv_std = 1.0 / @sqrt(variance + eps);

        // Fold the affine transform into a single FMA: y = x*scale + bias_eff
        // where scale = gamma * inv_std and bias_eff = beta - mean*scale.
        const mean_splat: F32xN = @splat(mean);
        const inv_std_splat: F32xN = @splat(inv_std);
        i = 0;
        while (i + VEC_LEN <= dim) : (i += VEC_LEN) {
            const v: F32xN = row[i..][0..VEC_LEN].*;
            const g: F32xN = gamma[i..][0..VEC_LEN].*;
            const bt: F32xN = beta[i..][0..VEC_LEN].*;
            const scale = g * inv_std_splat;
            row[i..][0..VEC_LEN].* = @mulAdd(F32xN, v - mean_splat, scale, bt);
        }
        while (i < dim) : (i += 1) {
            row[i] = gamma[i] * (row[i] - mean) * inv_std + beta[i];
        }
    }
}

/// Log-softmax over the last dimension.  data is [batch, dim], applied per row.
///
/// Numerically stable: log_softmax(x) = (x - max) - log(sum(exp(x - max))).
/// Uses the vectorized expVec for the sum-exp pass; the previous scalar
/// implementation in native_compute.zig went through libm expf per element.
pub fn logSoftmaxInPlace(data: []f32, dim: usize) void {
    const batch = data.len / dim;
    for (0..batch) |b| {
        const row = data[b * dim .. (b + 1) * dim];

        const max_val = vectorMax(row);
        const max_splat: F32xN = @splat(max_val);

        // Subtract max in place (so we only do the offset computation once),
        // then sum(exp(...)) via expVec.
        var sum_acc: F32xN = @splat(0.0);
        var i: usize = 0;
        while (i + VEC_LEN <= dim) : (i += VEC_LEN) {
            const v: F32xN = row[i..][0..VEC_LEN].*;
            const shifted = v - max_splat;
            row[i..][0..VEC_LEN].* = shifted;
            sum_acc += expVec(shifted);
        }
        var sum_exp: f32 = @reduce(.Add, sum_acc);
        while (i < dim) : (i += 1) {
            row[i] -= max_val;
            sum_exp += @exp(row[i]);
        }

        // Subtract log(sum_exp) in place.
        const lse = @log(sum_exp);
        const lse_splat: F32xN = @splat(lse);
        i = 0;
        while (i + VEC_LEN <= dim) : (i += VEC_LEN) {
            const v: F32xN = row[i..][0..VEC_LEN].*;
            row[i..][0..VEC_LEN].* = v - lse_splat;
        }
        while (i < dim) : (i += 1) {
            row[i] -= lse;
        }
    }
}

// --- Vectorized helpers ---

fn vectorSum(data: []const f32) f32 {
    var acc: F32xN = @splat(0.0);
    var i: usize = 0;
    while (i + VEC_LEN <= data.len) : (i += VEC_LEN) {
        const v: F32xN = data[i..][0..VEC_LEN].*;
        acc += v;
    }
    var sum = @reduce(.Add, acc);
    while (i < data.len) : (i += 1) {
        sum += data[i];
    }
    return sum;
}

fn vectorDotSelf(data: []const f32) f32 {
    var acc: F32xN = @splat(0.0);
    var i: usize = 0;
    while (i + VEC_LEN <= data.len) : (i += VEC_LEN) {
        const v: F32xN = data[i..][0..VEC_LEN].*;
        acc += v * v;
    }
    var sum = @reduce(.Add, acc);
    while (i < data.len) : (i += 1) {
        sum += data[i] * data[i];
    }
    return sum;
}

fn vectorMax(data: []const f32) f32 {
    if (data.len == 0) return -std.math.inf(f32);
    var max_vec: F32xN = @splat(data[0]);
    var i: usize = 0;
    while (i + VEC_LEN <= data.len) : (i += VEC_LEN) {
        const v: F32xN = data[i..][0..VEC_LEN].*;
        max_vec = @max(max_vec, v);
    }
    var max_val = @reduce(.Max, max_vec);
    while (i < data.len) : (i += 1) {
        if (data[i] > max_val) max_val = data[i];
    }
    return max_val;
}

// --- Sampling utilities (shared by generation pipelines) ---

/// Return the index of the largest value.
pub fn argmax(data: []const f32) usize {
    if (data.len == 0) return 0;
    const max_val = vectorMax(data);
    for (data, 0..) |v, i| {
        if (v == max_val) return i;
    }
    return 0;
}

/// Top-k filtering: zero out everything below the k-th largest probability, then renormalize.
pub fn topK(probs: []f32, k: usize, allocator: std.mem.Allocator) void {
    if (k >= probs.len) return;

    // Copy and partial-sort to find the k-th largest value.
    // Use a scratch buffer so we don't disturb the original until we threshold.
    const scratch = allocator.alloc(f32, probs.len) catch {
        // Fallback: greedy keep-top-k without allocation
        topKScalar(probs, k);
        return;
    };
    defer allocator.free(scratch);
    @memcpy(scratch, probs);

    // Partial sort: repeatedly find max and zero to get the k-th value.
    var threshold: f32 = 0.0;
    for (0..k) |_| {
        const m = vectorMax(scratch);
        if (m <= 0 or m == -std.math.inf(f32)) break;
        threshold = m;
        // Zero out all instances of max so next iteration finds the next value
        replaceVal(scratch, m, 0.0);
    }

    // Threshold + renormalize
    thresholdAndNormalize(probs, threshold);
}

/// Scalar fallback for topK when allocation fails.
fn topKScalar(probs: []f32, k: usize) void {
    var threshold: f32 = 0.0;
    var prev_max: f32 = std.math.inf(f32);
    for (0..k) |_| {
        var cur_max: f32 = -std.math.inf(f32);
        for (probs) |v| {
            if (v > cur_max and v < prev_max) cur_max = v;
        }
        if (cur_max == -std.math.inf(f32)) break;
        threshold = cur_max;
        prev_max = cur_max;
        var count: usize = 0;
        for (probs) |v| {
            if (v >= threshold) count += 1;
        }
        if (count >= k) break;
    }
    thresholdAndNormalize(probs, threshold);
}

/// Top-p (nucleus) filtering: keep the smallest set of tokens whose cumulative
/// probability >= p, zero out the rest, then renormalize.
pub fn topP(probs: []f32, p: f32, allocator: std.mem.Allocator) void {
    if (p >= 1.0) return;

    // Work on a scratch copy to walk from largest to smallest
    const scratch = allocator.alloc(f32, probs.len) catch {
        topPScalar(probs, p);
        return;
    };
    defer allocator.free(scratch);
    @memcpy(scratch, probs);

    var cumsum: f32 = 0.0;
    var cutoff: f32 = 0.0;

    while (cumsum < p) {
        const m = vectorMax(scratch);
        if (m <= 0 or m == -std.math.inf(f32)) break;
        cutoff = m;
        // Sum all instances of this value
        cumsum += sumEqual(scratch, m);
        // Zero them out so next iteration finds the next largest
        replaceVal(scratch, m, 0.0);
    }

    thresholdAndNormalize(probs, cutoff);
}

/// Scalar fallback for topP when allocation fails.
fn topPScalar(probs: []f32, p: f32) void {
    var cumsum: f32 = 0.0;
    var cutoff: f32 = std.math.inf(f32);
    while (cumsum < p) {
        var max_val: f32 = -std.math.inf(f32);
        for (probs) |v| {
            if (v > max_val and v < cutoff) max_val = v;
        }
        if (max_val <= 0 or max_val == -std.math.inf(f32)) break;
        for (probs) |v| {
            if (v == max_val) cumsum += v;
        }
        cutoff = max_val;
    }
    thresholdAndNormalize(probs, cutoff);
}

/// Sample from a probability distribution using cumulative sum.
/// Accumulates partial sums in chunks for fast scanning.
pub fn sampleFromProbs(probs: []const f32) usize {
    // Use pointer address as entropy source — each call gets a different stack address
    const seed: u64 = @as(u64, @intFromPtr(probs.ptr)) *% 0x9E3779B97F4A7C15 +% @as(u64, @intCast(probs.len));
    var rng = std.Random.DefaultPrng.init(seed);
    const r = rng.random().float(f32);

    var cumsum: f32 = 0.0;
    var i: usize = 0;
    while (i + VEC_LEN <= probs.len) : (i += VEC_LEN) {
        const v: F32xN = probs[i..][0..VEC_LEN].*;
        const chunk_sum = @reduce(.Add, v);
        if (cumsum + chunk_sum >= r) {
            // Target is in this chunk — scan scalar
            for (0..VEC_LEN) |j| {
                cumsum += probs[i + j];
                if (cumsum >= r) return i + j;
            }
        }
        cumsum += chunk_sum;
    }
    // Scalar remainder
    while (i < probs.len) : (i += 1) {
        cumsum += probs[i];
        if (cumsum >= r) return i;
    }
    return probs.len - 1;
}

// --- Vectorized helpers for sampling ---

/// Replace all occurrences of `val` with `replacement`.
fn replaceVal(data: []f32, val: f32, replacement: f32) void {
    const val_vec: F32xN = @splat(val);
    const rep_vec: F32xN = @splat(replacement);
    var i: usize = 0;
    while (i + VEC_LEN <= data.len) : (i += VEC_LEN) {
        const v: F32xN = data[i..][0..VEC_LEN].*;
        const mask = v == val_vec;
        data[i..][0..VEC_LEN].* = @select(f32, mask, rep_vec, v);
    }
    while (i < data.len) : (i += 1) {
        if (data[i] == val) data[i] = replacement;
    }
}

/// Sum all elements equal to `val`.
fn sumEqual(data: []const f32, val: f32) f32 {
    const val_vec: F32xN = @splat(val);
    const zero_vec: F32xN = @splat(0.0);
    var acc: F32xN = @splat(0.0);
    var i: usize = 0;
    while (i + VEC_LEN <= data.len) : (i += VEC_LEN) {
        const v: F32xN = data[i..][0..VEC_LEN].*;
        const mask = v == val_vec;
        acc += @select(f32, mask, v, zero_vec);
    }
    var sum = @reduce(.Add, acc);
    while (i < data.len) : (i += 1) {
        if (data[i] == val) sum += data[i];
    }
    return sum;
}

/// Zero values below `threshold`, then renormalize so sum = 1.
fn thresholdAndNormalize(data: []f32, threshold: f32) void {
    const thresh_vec: F32xN = @splat(threshold);
    const zero_vec: F32xN = @splat(0.0);
    var sum_acc: F32xN = @splat(0.0);
    var i: usize = 0;

    // Zero out below threshold and accumulate sum
    while (i + VEC_LEN <= data.len) : (i += VEC_LEN) {
        const v: F32xN = data[i..][0..VEC_LEN].*;
        const keep = v >= thresh_vec;
        const filtered = @select(f32, keep, v, zero_vec);
        data[i..][0..VEC_LEN].* = filtered;
        sum_acc += filtered;
    }
    var sum = @reduce(.Add, sum_acc);
    while (i < data.len) : (i += 1) {
        if (data[i] < threshold) {
            data[i] = 0.0;
        } else {
            sum += data[i];
        }
    }

    // Renormalize
    if (sum > 0) {
        const inv: F32xN = @splat(1.0 / sum);
        const inv_scalar = 1.0 / sum;
        i = 0;
        while (i + VEC_LEN <= data.len) : (i += VEC_LEN) {
            const v: F32xN = data[i..][0..VEC_LEN].*;
            data[i..][0..VEC_LEN].* = v * inv;
        }
        while (i < data.len) : (i += 1) {
            data[i] *= inv_scalar;
        }
    }
}

// -- Tests --

test "gelu" {
    var data = [_]f32{ 0.0, 1.0, -1.0, 2.0 };
    gelu(&data);
    try std.testing.expectApproxEqAbs(@as(f32, 0.0), data[0], 1e-5);
    try std.testing.expectApproxEqAbs(@as(f32, 0.8412), data[1], 1e-3);
    try std.testing.expectApproxEqAbs(@as(f32, -0.1588), data[2], 1e-3);
}

// Vector vs scalar parity for gelu/silu/sigmoid/quickGelu across full activation range,
// including the SIMD tail.  Locks in tanhVec accuracy.
test "vectorized activations match libm scalar reference" {
    const allocator = std.testing.allocator;
    const len = 521; // odd length to exercise vector tail
    const buf_a = try allocator.alloc(f32, len);
    defer allocator.free(buf_a);
    const buf_b = try allocator.alloc(f32, len);
    defer allocator.free(buf_b);

    // Range [-8, 8] covers normal activation outputs plus saturation regions
    for (buf_a, 0..) |*v, i| {
        const t: f32 = @as(f32, @floatFromInt(i)) / @as(f32, @floatFromInt(len - 1));
        v.* = -8.0 + 16.0 * t;
    }
    @memcpy(buf_b, buf_a);

    // GELU
    geluScalarRef(buf_a);
    gelu(buf_b);
    for (buf_a, buf_b) |sc, vc| {
        try std.testing.expect(@abs(sc - vc) < 5e-6);
    }

    // SiLU
    for (buf_a, 0..) |*v, i| {
        const t: f32 = @as(f32, @floatFromInt(i)) / @as(f32, @floatFromInt(len - 1));
        v.* = -8.0 + 16.0 * t;
    }
    @memcpy(buf_b, buf_a);
    siluScalarRef(buf_a);
    silu(buf_b);
    for (buf_a, buf_b) |sc, vc| {
        try std.testing.expect(@abs(sc - vc) < 5e-6);
    }

    // Sigmoid
    for (buf_a, 0..) |*v, i| {
        const t: f32 = @as(f32, @floatFromInt(i)) / @as(f32, @floatFromInt(len - 1));
        v.* = -8.0 + 16.0 * t;
    }
    @memcpy(buf_b, buf_a);
    sigmoidScalarRef(buf_a);
    sigmoid(buf_b);
    for (buf_a, buf_b) |sc, vc| {
        try std.testing.expect(@abs(sc - vc) < 5e-6);
    }

    // QuickGELU
    for (buf_a, 0..) |*v, i| {
        const t: f32 = @as(f32, @floatFromInt(i)) / @as(f32, @floatFromInt(len - 1));
        v.* = -8.0 + 16.0 * t;
    }
    @memcpy(buf_b, buf_a);
    quickGeluScalarRef(buf_a);
    quickGelu(buf_b);
    for (buf_a, buf_b) |sc, vc| {
        try std.testing.expect(@abs(sc - vc) < 5e-6);
    }
}

test "vectorized activations handle non-finite lanes" {
    var sigmoid_data = @as([VEC_LEN]f32, @splat(0.0));
    sigmoid_data[0] = std.math.inf(f32);
    sigmoid_data[1] = -std.math.inf(f32);
    sigmoid_data[2] = std.math.nan(f32);
    sigmoid(&sigmoid_data);
    try std.testing.expectEqual(@as(f32, 1.0), sigmoid_data[0]);
    try std.testing.expectEqual(@as(f32, 0.0), sigmoid_data[1]);
    try std.testing.expect(std.math.isNan(sigmoid_data[2]));

    var silu_data = @as([VEC_LEN]f32, @splat(0.0));
    silu_data[0] = std.math.inf(f32);
    silu_data[1] = -std.math.inf(f32);
    silu_data[2] = std.math.nan(f32);
    silu(&silu_data);
    try std.testing.expect(silu_data[0] == std.math.inf(f32));
    try std.testing.expect(std.math.isNan(silu_data[1]));
    try std.testing.expect(std.math.isNan(silu_data[2]));

    var quick_gelu_data = @as([VEC_LEN]f32, @splat(0.0));
    quick_gelu_data[0] = std.math.inf(f32);
    quick_gelu_data[1] = -std.math.inf(f32);
    quick_gelu_data[2] = std.math.nan(f32);
    quickGelu(&quick_gelu_data);
    try std.testing.expect(quick_gelu_data[0] == std.math.inf(f32));
    try std.testing.expect(std.math.isNan(quick_gelu_data[1]));
    try std.testing.expect(std.math.isNan(quick_gelu_data[2]));
}

fn geluScalarRef(data: []f32) void {
    const sqrt_2_over_pi: f32 = 0.7978845608028654;
    for (data) |*x| {
        const val = x.*;
        const inner = sqrt_2_over_pi * (val + 0.044715 * val * val * val);
        x.* = 0.5 * val * (1.0 + std.math.tanh(inner));
    }
}

fn sigmoidScalarRef(data: []f32) void {
    for (data) |*x| x.* = 1.0 / (1.0 + @exp(-x.*));
}

fn siluScalarRef(data: []f32) void {
    for (data) |*x| {
        const v = x.*;
        x.* = v / (1.0 + @exp(-v));
    }
}

fn quickGeluScalarRef(data: []f32) void {
    for (data) |*x| {
        const v = x.*;
        x.* = v * (1.0 / (1.0 + @exp(-1.702 * v)));
    }
}

test "relu" {
    var data = [_]f32{ -2.0, -1.0, 0.0, 1.0, 2.0, -0.5, 0.5, -3.0, 4.0 };
    relu(&data);
    try std.testing.expectApproxEqAbs(@as(f32, 0.0), data[0], 1e-6);
    try std.testing.expectApproxEqAbs(@as(f32, 0.0), data[1], 1e-6);
    try std.testing.expectApproxEqAbs(@as(f32, 0.0), data[2], 1e-6);
    try std.testing.expectApproxEqAbs(@as(f32, 1.0), data[3], 1e-6);
    try std.testing.expectApproxEqAbs(@as(f32, 4.0), data[8], 1e-6);
}

test "silu" {
    var data = [_]f32{ 0.0, 1.0 };
    silu(&data);
    try std.testing.expectApproxEqAbs(@as(f32, 0.0), data[0], 1e-5);
    // silu(1) = 1 * sigmoid(1) ≈ 0.7311
    try std.testing.expectApproxEqAbs(@as(f32, 0.7311), data[1], 1e-3);
}

test "silu remains finite for larger positive activation range" {
    var data = @as([(VEC_LEN * 2)]f32, @splat(0.0));
    data[0] = 11.367456;
    data[VEC_LEN] = 11.367456;
    silu(&data);
    try std.testing.expect(std.math.isFinite(data[0]));
    try std.testing.expect(std.math.isFinite(data[VEC_LEN]));
    try std.testing.expectApproxEqAbs(@as(f32, 11.367327), data[0], 1e-3);
    try std.testing.expectApproxEqAbs(@as(f32, 11.367327), data[VEC_LEN], 1e-3);
}

test "rmsNorm" {
    var data = [_]f32{ 1.0, 2.0, 3.0, 4.0 };
    const weight = [_]f32{ 1.0, 1.0 };
    rmsNorm(&data, &weight, 2, 1e-6);
    // rms([1,2]) = sqrt((1+4)/2) = sqrt(2.5) ≈ 1.5811
    // [1/1.5811, 2/1.5811] ≈ [0.6325, 1.2649]
    try std.testing.expectApproxEqAbs(@as(f32, 0.6325), data[0], 1e-3);
    try std.testing.expectApproxEqAbs(@as(f32, 1.2649), data[1], 1e-3);
}

test "softmax" {
    var data = [_]f32{ 1.0, 2.0, 3.0 };
    softmax(&data, 3);
    try std.testing.expectApproxEqAbs(@as(f32, 0.0900), data[0], 1e-3);
    try std.testing.expectApproxEqAbs(@as(f32, 0.2447), data[1], 1e-3);
    try std.testing.expectApproxEqAbs(@as(f32, 0.6652), data[2], 1e-3);
}

test "softmax matches scalar reference across SIMD tail" {
    // Length deliberately straddles a vector boundary so we exercise both
    // the wide and the scalar tail.
    const allocator = std.testing.allocator;
    const dim: usize = 77; // CLIP attention tail length, has the SIMD tail
    const buf_a = try allocator.alloc(f32, dim);
    defer allocator.free(buf_a);
    const buf_b = try allocator.alloc(f32, dim);
    defer allocator.free(buf_b);
    var rng = std.Random.DefaultPrng.init(0xC0DEBABE);
    for (buf_a) |*v| v.* = (rng.random().float(f32) - 0.5) * 12.0;
    @memcpy(buf_b, buf_a);

    softmaxScalarRef(buf_a, dim);
    softmax(buf_b, dim);
    for (buf_a, buf_b) |sc, vc| try std.testing.expect(@abs(sc - vc) < 5e-6);
    var sum: f32 = 0;
    for (buf_b) |v| sum += v;
    try std.testing.expectApproxEqAbs(@as(f32, 1.0), sum, 1e-5);
}

test "softmax zeroes fully masked rows" {
    var data = [_]f32{
        -std.math.inf(f32), -std.math.inf(f32), -std.math.inf(f32),
        1.0,                -std.math.inf(f32), 2.0,
    };
    softmax(&data, 3);

    try std.testing.expectEqual(@as(f32, 0.0), data[0]);
    try std.testing.expectEqual(@as(f32, 0.0), data[1]);
    try std.testing.expectEqual(@as(f32, 0.0), data[2]);
    try std.testing.expect(std.math.isFinite(data[3]));
    try std.testing.expect(std.math.isFinite(data[4]));
    try std.testing.expect(std.math.isFinite(data[5]));
    try std.testing.expectApproxEqAbs(@as(f32, 1.0), data[3] + data[4] + data[5], 1e-6);
}

test "logSoftmaxInPlace matches scalar reference" {
    const allocator = std.testing.allocator;
    const dim: usize = 257; // CLIP ViT seq length (256 patches + CLS)
    const buf_a = try allocator.alloc(f32, dim);
    defer allocator.free(buf_a);
    const buf_b = try allocator.alloc(f32, dim);
    defer allocator.free(buf_b);
    var rng = std.Random.DefaultPrng.init(0xCAFEF00D);
    for (buf_a) |*v| v.* = (rng.random().float(f32) - 0.5) * 8.0;
    @memcpy(buf_b, buf_a);

    logSoftmaxScalarRef(buf_a, dim);
    logSoftmaxInPlace(buf_b, dim);
    for (buf_a, buf_b) |sc, vc| try std.testing.expect(@abs(sc - vc) < 1e-5);
}

fn softmaxScalarRef(data: []f32, dim: usize) void {
    const batch = data.len / dim;
    for (0..batch) |b| {
        const row = data[b * dim .. (b + 1) * dim];
        var max_val: f32 = -std.math.inf(f32);
        for (row) |v| max_val = @max(max_val, v);
        var sum: f32 = 0;
        for (row) |*v| {
            v.* = @exp(v.* - max_val);
            sum += v.*;
        }
        const inv = 1.0 / sum;
        for (row) |*v| v.* *= inv;
    }
}

fn logSoftmaxScalarRef(data: []f32, dim: usize) void {
    const batch = data.len / dim;
    for (0..batch) |b| {
        const row = data[b * dim .. (b + 1) * dim];
        var max_val: f32 = -std.math.inf(f32);
        for (row) |v| max_val = @max(max_val, v);
        var sum_exp: f32 = 0;
        for (row) |v| sum_exp += @exp(v - max_val);
        const lse = @log(sum_exp);
        for (row) |*v| v.* = (v.* - max_val) - lse;
    }
}

test "layer norm" {
    var data = [_]f32{ 1.0, 2.0, 3.0, 4.0 };
    const gamma = [_]f32{ 1.0, 1.0 };
    const beta = [_]f32{ 0.0, 0.0 };
    layerNorm(&data, &gamma, &beta, 2, 1e-5);
    try std.testing.expectApproxEqAbs(@as(f32, -1.0), data[0], 1e-3);
    try std.testing.expectApproxEqAbs(@as(f32, 1.0), data[1], 1e-3);
}

test "layer norm matches two-pass reference" {
    // Lock in fused single-pass parity with the textbook two-pass formulation
    // across CLIP-sized hidden dims.  Uses a width that hits the SIMD tail.
    const allocator = std.testing.allocator;
    const dim: usize = 768; // CLIP ViT-B hidden size
    const batch: usize = 3;
    const total = dim * batch;
    const a = try allocator.alloc(f32, total);
    defer allocator.free(a);
    const b = try allocator.alloc(f32, total);
    defer allocator.free(b);
    const gamma = try allocator.alloc(f32, dim);
    defer allocator.free(gamma);
    const beta = try allocator.alloc(f32, dim);
    defer allocator.free(beta);

    var rng = std.Random.DefaultPrng.init(0xBEEFFACE);
    for (a) |*v| v.* = (rng.random().float(f32) - 0.5) * 6.0;
    @memcpy(b, a);
    for (gamma) |*g| g.* = 0.5 + rng.random().float(f32);
    for (beta) |*bt| bt.* = (rng.random().float(f32) - 0.5) * 2.0;

    layerNormTwoPassRef(a, gamma, beta, dim, 1e-5);
    layerNorm(b, gamma, beta, dim, 1e-5);
    for (a, b) |sc, vc| try std.testing.expect(@abs(sc - vc) < 5e-4);
}

fn layerNormTwoPassRef(
    data: []f32,
    gamma: []const f32,
    beta: []const f32,
    dim: usize,
    eps: f32,
) void {
    const batch = data.len / dim;
    const dim_f: f32 = @floatFromInt(dim);
    for (0..batch) |b| {
        const row = data[b * dim .. (b + 1) * dim];
        var sum: f32 = 0;
        for (row) |v| sum += v;
        const mean = sum / dim_f;
        var var_sum: f32 = 0;
        for (row) |v| {
            const d = v - mean;
            var_sum += d * d;
        }
        const variance = var_sum / dim_f;
        const inv_std = 1.0 / @sqrt(variance + eps);
        for (row, 0..) |*v, i| {
            v.* = gamma[i] * (v.* - mean) * inv_std + beta[i];
        }
    }
}

test "argmax" {
    const data = [_]f32{ 0.1, 0.3, 0.9, 0.2, 0.5 };
    try std.testing.expectEqual(@as(usize, 2), argmax(&data));
}

test "argmax single element" {
    const data = [_]f32{42.0};
    try std.testing.expectEqual(@as(usize, 0), argmax(&data));
}

test "topK basic" {
    const allocator = std.testing.allocator;
    var probs = [_]f32{ 0.1, 0.4, 0.05, 0.3, 0.15 };
    topK(&probs, 2, allocator);
    // Only indices 1 (0.4) and 3 (0.3) should be nonzero
    try std.testing.expectApproxEqAbs(@as(f32, 0.0), probs[0], 1e-5);
    try std.testing.expect(probs[1] > 0.5); // 0.4/0.7 ≈ 0.571
    try std.testing.expectApproxEqAbs(@as(f32, 0.0), probs[2], 1e-5);
    try std.testing.expect(probs[3] > 0.3); // 0.3/0.7 ≈ 0.429
    try std.testing.expectApproxEqAbs(@as(f32, 0.0), probs[4], 1e-5);
    // Sum should be ~1.0
    var sum: f32 = 0.0;
    for (probs) |v| sum += v;
    try std.testing.expectApproxEqAbs(@as(f32, 1.0), sum, 1e-5);
}

test "topP basic" {
    const allocator = std.testing.allocator;
    var probs = [_]f32{ 0.05, 0.6, 0.1, 0.2, 0.05 };
    topP(&probs, 0.8, allocator);
    // 0.6 alone < 0.8, so include 0.6 + 0.2 = 0.8 >= 0.8
    // Keep indices 1 (0.6) and 3 (0.2), zero out rest
    try std.testing.expectApproxEqAbs(@as(f32, 0.0), probs[0], 1e-5);
    try std.testing.expect(probs[1] > 0.7); // 0.6/0.8 = 0.75
    try std.testing.expectApproxEqAbs(@as(f32, 0.0), probs[2], 1e-5);
    try std.testing.expect(probs[3] > 0.2); // 0.2/0.8 = 0.25
    try std.testing.expectApproxEqAbs(@as(f32, 0.0), probs[4], 1e-5);
    var sum: f32 = 0.0;
    for (probs) |v| sum += v;
    try std.testing.expectApproxEqAbs(@as(f32, 1.0), sum, 1e-5);
}

test "sampleFromProbs distribution" {
    // Deterministic-ish: a probability of 1.0 at one index should always return that index
    const probs = [_]f32{ 0.0, 0.0, 1.0, 0.0 };
    const idx = sampleFromProbs(&probs);
    try std.testing.expectEqual(@as(usize, 2), idx);
}
