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

//! Tile attention: for each head h and query i,
//!   out[i, h] = softmax_j(scale * q[i, h] · k[j, h]) · v[j, h]
//! over keys j < tk, and with `causal`, j <= q_offset + i. Rows are token
//! rows with heads side by side (`ld_*` floats per row, head h at column
//! h * head_dim), which is the layout the projections produce.
//!
//! Flash-style: keys are visited in tiles with an online softmax (running
//! max and sum, the same state as the RowReduce logsumexp channel), so the
//! score matrix is never stored.

const std = @import("std");
const vmath = @import("vmath.zig");
const parallel = @import("parallel.zig");

pub const Attention = struct {
    name: []const u8,
    head_dim: u32,
    /// GPU: SIMD-groups (one query row each) per block.
    queries_per_block: u32 = 4,
};

pub const Args = struct {
    q: []const f32,
    k: []const f32,
    v: []const f32,
    out: []f32,
    tq: usize,
    tk: usize,
    heads: usize,
    ld_q: usize,
    ld_k: usize,
    ld_v: usize,
    ld_o: usize,
    scale: f32,
    causal: bool = false,
    /// Absolute position of query 0 (causal masking).
    q_offset: usize = 0,
};

fn keyLimit(a: Args, i: usize) usize {
    return if (a.causal) @min(a.tk, a.q_offset + i + 1) else a.tk;
}

pub fn reference(comptime att: Attention, a: Args) !void {
    const d = att.head_dim;
    const scores = try std.testing.allocator.alloc(f64, a.tk);
    defer std.testing.allocator.free(scores);
    for (0..a.heads) |h| for (0..a.tq) |i| {
        const limit = keyLimit(a, i);
        var hi: f64 = -std.math.inf(f64);
        for (0..limit) |j| {
            var s: f64 = 0;
            for (0..d) |e| s += @as(f64, a.q[i * a.ld_q + h * d + e]) * a.k[j * a.ld_k + h * d + e];
            scores[j] = s * a.scale;
            hi = @max(hi, scores[j]);
        }
        var total: f64 = 0;
        for (scores[0..limit]) |*s| {
            s.* = @exp(s.* - hi);
            total += s.*;
        }
        for (0..d) |e| {
            var o: f64 = 0;
            for (0..limit) |j| o += scores[j] * a.v[j * a.ld_v + h * d + e];
            a.out[i * a.ld_o + h * d + e] = @floatCast(o / total);
        }
    };
}

const key_tile = 16;
const lanes = 16;
const V = @Vector(lanes, f32);
const S = @Vector(key_tile, f32);

pub fn run(comptime att: Attention, a: Args) void {
    comptime std.debug.assert(att.head_dim % lanes == 0);
    const Ctx = struct {
        a: Args,
        fn body(ctx: @This(), begin: usize, end: usize) void {
            for (begin..end) |item| query(att, ctx.a, item % ctx.a.heads, item / ctx.a.heads);
        }
    };
    const min_items = @max(@as(usize, 1), 4096 / @max(a.tk, 1));
    parallel.forRanges(a.tq * a.heads, min_items, Ctx{ .a = a }, Ctx.body);
}

fn query(comptime att: Attention, a: Args, h: usize, i: usize) void {
    const chunks = att.head_dim / lanes;
    const limit = keyLimit(a, i);
    var q: [chunks]V = undefined;
    inline for (0..chunks) |c| q[c] = @as(V, a.q[i * a.ld_q + h * att.head_dim + c * lanes ..][0..lanes].*) * @as(V, @splat(a.scale));
    var o: [chunks]V = @splat(@splat(0));
    var m: f32 = -std.math.inf(f32);
    var l: f32 = 0;
    var j0: usize = 0;
    while (j0 < limit) : (j0 += key_tile) {
        var scores: [key_tile]f32 = @splat(-std.math.inf(f32));
        const n = @min(key_tile, limit - j0);
        for (0..n) |t| {
            const krow = a.k[(j0 + t) * a.ld_k + h * att.head_dim ..];
            var dot: V = @splat(0);
            inline for (0..chunks) |c| dot = @mulAdd(V, q[c], krow[c * lanes ..][0..lanes].*, dot);
            scores[t] = @reduce(.Add, dot);
        }
        const s: S = scores;
        const m_new = @max(m, @reduce(.Max, s));
        const corr = vmath.vexp(1, @splat(m - m_new))[0];
        const p = vmath.vexp(key_tile, s - @as(S, @splat(m_new)));
        l = l * corr + @reduce(.Add, p);
        const probs: [key_tile]f32 = p;
        inline for (0..chunks) |c| o[c] *= @as(V, @splat(corr));
        for (0..n) |t| {
            const vrow = a.v[(j0 + t) * a.ld_v + h * att.head_dim ..];
            inline for (0..chunks) |c| o[c] = @mulAdd(V, @splat(probs[t]), vrow[c * lanes ..][0..lanes].*, o[c]);
        }
        m = m_new;
    }
    const inv: f32 = if (l > 0) 1 / l else 0;
    inline for (0..chunks) |c| a.out[i * a.ld_o + h * att.head_dim + c * lanes ..][0..lanes].* = o[c] * @as(V, @splat(inv));
}
