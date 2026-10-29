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

//! Data-movement tiles: one output element per GPU thread.
//!
//! - `Im2col` unrolls a 1-D convolution so it runs as a GEMM:
//!   out[t, c * kernel + j] = x(c, t * stride + j - pad), zero outside.
//!   The input is channel-major ([C, T], the mel layout) or time-major
//!   ([T, C], a previous GEMM's output); the weight [C_out, C, kernel]
//!   flattens to the matching [C_out, C * kernel].
//! - `Embed` gathers token rows and adds positional rows:
//!   out[t, :] = table[ids[t], :] + pos[pos_offset + t, :].

const std = @import("std");
const parallel = @import("parallel.zig");

pub const InputLayout = enum { channel_major, time_major };

pub const Im2col = struct {
    name: []const u8,
    kernel: u32,
    stride: u32,
    pad: u32,
    layout: InputLayout,
    threads: u32 = 256,

    pub fn outLen(comptime c: Im2col, t_in: usize) usize {
        return (t_in + 2 * c.pad - c.kernel) / c.stride + 1;
    }
};

pub const Im2colArgs = struct {
    x: []const f32,
    out: []f32,
    channels: usize,
    t_in: usize,
};

/// Also the oracle: there is no arithmetic to round differently.
pub fn im2col(comptime c: Im2col, a: Im2colArgs) void {
    const t_out = c.outLen(a.t_in);
    const width = a.channels * c.kernel;
    const Ctx = struct {
        a: Im2colArgs,
        fn body(ctx: @This(), begin: usize, end: usize) void {
            const aa = ctx.a;
            const w = aa.channels * c.kernel;
            for (begin..end) |t| for (0..aa.channels) |ch| for (0..c.kernel) |j| {
                const src = @as(isize, @intCast(t * c.stride + j)) - @as(isize, c.pad);
                aa.out[t * w + ch * c.kernel + j] = if (src < 0 or src >= aa.t_in) 0 else switch (c.layout) {
                    .channel_major => aa.x[ch * aa.t_in + @as(usize, @intCast(src))],
                    .time_major => aa.x[@as(usize, @intCast(src)) * aa.channels + ch],
                };
            };
        }
    };
    _ = width;
    parallel.forRanges(t_out, 64, Ctx{ .a = a }, Ctx.body);
}

pub const Embed = struct {
    name: []const u8,
    threads: u32 = 256,
};

pub const EmbedArgs = struct {
    ids: []const u32,
    table: []const f32,
    pos: []const f32,
    out: []f32,
    dim: usize,
    pos_offset: usize,
};

pub fn embed(comptime _: Embed, a: EmbedArgs) void {
    for (a.ids, 0..) |id, t| {
        const src = a.table[@as(usize, id) * a.dim ..][0..a.dim];
        const p = a.pos[(a.pos_offset + t) * a.dim ..][0..a.dim];
        for (a.out[t * a.dim ..][0..a.dim], src, p) |*o, s, q| o.* = s + q;
    }
}
