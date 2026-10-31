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
        out[c..][0..lanes].* = @mulAdd(V, d, a.gamma[c..][0..lanes].*, a.beta[c..][0..lanes].*);
    }
    while (c < a.dim) : (c += 1) out[c] = (x[c] - mean) * inv * a.gamma[c] + a.beta[c];
}
