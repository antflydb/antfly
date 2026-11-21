// Copyright 2026 Antfly, Inc.
// SPDX-License-Identifier: Apache-2.0
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

//! Tile LayerNorm: out[r, :] = (x[r, :] - mean) / sqrt(var + eps) * gamma + beta.
//! Two passes per row (mean, then centered variance) on every backend.

const std = @import("std");
const fexpr = @import("fexpr.zig");
const native_math = @import("native_math.zig");
const parallel = @import("parallel.zig");

pub const LayerNorm = struct {
    name: []const u8,
    eps: f32 = 1e-5,
    /// GPU threads per row.
    threads: u32 = 128,
};

pub const Args = struct {
    x: []const f32,
    gamma: []const f32,
    beta: []const f32,
    out: []f32,
    rows: usize,
    dim: usize,
};

pub fn reference(comptime ln: LayerNorm, a: Args) void {
    for (0..a.rows) |r| {
        const xs = a.x[r * a.dim ..][0..a.dim];
        var mean: f64 = 0;
        for (xs) |v| mean += v;
        mean /= @floatFromInt(a.dim);
        var variance: f64 = 0;
        for (xs) |v| variance += (v - mean) * (v - mean);
        variance /= @floatFromInt(a.dim);
        const inv = 1 / @sqrt(variance + ln.eps);
        for (xs, 0..) |v, c| a.out[r * a.dim + c] = @floatCast((v - mean) * inv * a.gamma[c] + a.beta[c]);
    }
}

const lanes = 16;
const V = @Vector(lanes, f32);

pub fn run(comptime ln: LayerNorm, a: Args) void {
    const Ctx = struct {
        a: Args,
        fn body(ctx: @This(), begin: usize, end: usize) void {
            for (begin..end) |r| row(ln, ctx.a, r);
        }
    };
    parallel.forRanges(a.rows, @max(@as(usize, 1), 8192 / @max(a.dim, 1)), Ctx{ .a = a }, Ctx.body);
}

fn row(comptime ln: LayerNorm, a: Args, r: usize) void {
    const x = a.x[r * a.dim ..][0..a.dim];
    const out = a.out[r * a.dim ..][0..a.dim];
    const full = a.dim / lanes * lanes;
    var acc: V = @splat(0);
    var c: usize = 0;
    while (c < full) : (c += lanes) acc += x[c..][0..lanes].*;
    var sum = @reduce(.Add, acc);
    for (x[full..]) |v| sum += v;
    const mean = sum / @as(f32, @floatFromInt(a.dim));
    acc = @splat(0);
    c = 0;
    while (c < full) : (c += lanes) {
        const d = @as(V, x[c..][0..lanes].*) - @as(V, @splat(mean));
        acc = @mulAdd(V, d, d, acc);
    }
    var sq = @reduce(.Add, acc);
    for (x[full..]) |v| sq += (v - mean) * (v - mean);
    const inv = 1 / @sqrt(sq / @as(f32, @floatFromInt(a.dim)) + ln.eps);
    c = 0;
    while (c < full) : (c += lanes) {
        const d = (@as(V, x[c..][0..lanes].*) - @as(V, @splat(mean))) * @as(V, @splat(inv));
        out[c..][0..lanes].* = (comptime fexpr.acc.mulAdd(.bias, .residual)).evaluate(struct {}, fexpr.Inputs(V){ .value = d, .bias = a.gamma[c..][0..lanes].*, .residual = a.beta[c..][0..lanes].*, .gate = @splat(0) });
    }
    while (c < a.dim) : (c += 1) out[c] = (x[c] - mean) * inv * a.gamma[c] + a.beta[c];
}

/// RMS keeps a separate uncentered, single-accumulator reduction. It must not
/// be implemented as LayerNorm with a disabled mean (which changes the tree).
pub const RmsNorm = struct { name: []const u8, eps: f32 = 1e-5, threads: u32 = 128 };
pub const RmsArgs = struct {
    x: []const f32,
    gamma: []const f32,
    out: []f32,
    dim: usize,
    eps: f32,
    residual: ?[]const f32 = null,
    norm_scale: f32 = 1,
    output_scale: f32 = 1,
};
/// Exact CPU production schedule; lane width is owned by the caller's target.
/// Input/output may alias. The residual path has explicit strict rounding.
pub fn runRms(comptime width: comptime_int, comptime residual_scaled: bool, a: RmsArgs) void {
    @setFloatMode(.strict);
    const Vec = @Vector(width, f32);
    for (0..a.x.len / a.dim) |r| {
        const x = a.x[r * a.dim ..][0..a.dim];
        const out = a.out[r * a.dim ..][0..a.dim];
        const inv = 1.0 / @sqrt(@import("rows.zig").sumSquares(width, x) / @as(f32, @floatFromInt(a.dim)) + a.eps);
        var c: usize = 0;
        while (c + width <= a.dim) : (c += width) {
            const v: Vec = x[c..][0..width].*;
            const w: Vec = a.gamma[c..][0..width].*;
            const normalized = (comptime native_math.rms(.scale_then_weight)).evaluate(struct {}, .{ .value = v, .inverse = @as(Vec, @splat(inv)), .weight = w });
            out[c..][0..width].* = if (residual_scaled)
                (@as(Vec, a.residual.?[r * a.dim + c ..][0..width].*) + normalized * @as(Vec, @splat(a.norm_scale))) * @as(Vec, @splat(a.output_scale))
            else
                normalized;
        }
        while (c < a.dim) : (c += 1) {
            const normalized = (comptime native_math.rms(.scale_then_weight)).evaluate(struct {}, .{ .value = x[c], .inverse = inv, .weight = a.gamma[c] });
            out[c] = if (residual_scaled) (a.residual.?[r * a.dim + c] + normalized * a.norm_scale) * a.output_scale else normalized;
        }
    }
}

pub fn referenceRms(a: RmsArgs) void {
    for (0..a.x.len / a.dim) |r| {
        const x = a.x[r * a.dim ..][0..a.dim];
        var sq: f64 = 0;
        for (x) |v| sq += @as(f64, v) * v;
        const inv = 1 / @sqrt(sq / @as(f64, @floatFromInt(a.dim)) + a.eps);
        for (x, 0..) |v, c| {
            var value = @as(f64, v) * inv * a.gamma[c];
            if (a.residual) |res| value = (res[r * a.dim + c] + value * a.norm_scale) * a.output_scale;
            a.out[r * a.dim + c] = @floatCast(value);
        }
    }
}

/// Token-major masked mean. A mask excludes a token before any arithmetic,
/// so padding NaNs cannot contaminate the output. Empty selections yield zero.
pub fn maskedMean(comptime width: comptime_int, x: []const f32, mask: ?[]const bool, out: []f32, rows: usize, dim: usize) void {
    maskedMeanTyped(width, bool, x, mask, out, rows, dim);
}
pub fn maskedMeanTyped(comptime width: comptime_int, comptime Mask: type, x: []const f32, mask: ?[]const Mask, out: []f32, rows: usize, dim: usize) void {
    const Vec = @Vector(width, f32);
    @memset(out[0..dim], 0);
    // Preserve the production F32 count/divide contract.
    var count: f32 = 0;
    for (0..rows) |r| {
        if (mask) |m| {
            if (comptime Mask == bool) {
                if (!m[r]) continue;
            } else {
                if (m[r] <= 0) continue;
            }
        }
        count += 1;
        const src = x[r * dim ..][0..dim];
        var c: usize = 0;
        while (c + width <= dim) : (c += width) out[c..][0..width].* = @as(Vec, out[c..][0..width].*) + @as(Vec, src[c..][0..width].*);
        while (c < dim) : (c += 1) out[c] += src[c];
    }
    if (count == 0) return;
    for (out[0..dim]) |*value| value.* /= count;
}

pub const MaskedMean = struct { name: []const u8, threads: u32 = 256 };
