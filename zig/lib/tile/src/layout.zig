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

/// Element offsets, not byte offsets. A view can describe slices, transpose,
/// split projections, and padded packing without operation-specific loops.
pub const Rows = struct {
    rows: usize,
    cols: usize,
    row_stride: usize,
    col_stride: usize = 1,
    offset: usize = 0,
    pub fn validate(v: Rows, len: usize) error{InvalidLayout}!void {
        if (v.rows == 0 or v.cols == 0) {
            if (v.offset > len) return error.InvalidLayout;
            return;
        }
        const row = std.math.mul(usize, v.rows - 1, v.row_stride) catch return error.InvalidLayout;
        const col = std.math.mul(usize, v.cols - 1, v.col_stride) catch return error.InvalidLayout;
        const last = std.math.add(usize, v.offset, std.math.add(usize, row, col) catch return error.InvalidLayout) catch return error.InvalidLayout;
        if (last >= len) return error.InvalidLayout;
    }
};
/// Source and destination must not overlap. All validation happens before the
/// first store; contiguous rows lower to memcpy, including typed f16/u32 data.
pub fn copyRows(comptime T: type, src: []const T, sv: Rows, dst: []T, dv: Rows) error{InvalidLayout}!void {
    if (sv.rows != dv.rows or sv.cols != dv.cols) return error.InvalidLayout;
    try sv.validate(src.len);
    try dv.validate(dst.len);
    for (0..sv.rows) |r| copyRow(T, src, sv, r, dst, dv, r);
}
fn copyRow(comptime T: type, src: []const T, sv: Rows, sr: usize, dst: []T, dv: Rows, dr: usize) void {
    if (sv.cols == 0) return;
    const si = sv.offset + sr * sv.row_stride;
    const di = dv.offset + dr * dv.row_stride;
    if (sv.col_stride == 1 and dv.col_stride == 1) {
        @memcpy(dst[di..][0..dv.cols], src[si..][0..sv.cols]);
    } else {
        for (0..sv.cols) |c| dst[di + c * dv.col_stride] = src[si + c * sv.col_stride];
    }
}
pub fn gatherRows(comptime T: type, src: []const T, sv: Rows, ids: []const u32, dst: []T, dv: Rows) error{InvalidLayout}!void {
    if (ids.len != dv.rows or sv.cols != dv.cols) return error.InvalidLayout;
    try sv.validate(src.len);
    try dv.validate(dst.len);
    for (ids) |id| {
        if (id >= sv.rows) return error.InvalidLayout;
    }
    for (ids, 0..) |id, r| copyRow(T, src, sv, id, dst, dv, r);
}

pub const Element = enum { f32, f16, u32, i32 };
pub const Copy = struct { name: []const u8, element: Element = .f32, gather: bool = false, threads: u32 = 256 };

/// Shared host ABI for generated copies. Counts/strides are elements. This
/// prepared launch validates every index and catches flattened-size overflow
/// before a caller allocates buffers or submits GPU work.
pub const CopyParams = extern struct {
    rows: u64,
    cols: u64,
    src_offset: u64,
    src_row_stride: u64,
    src_col_stride: u64,
    dst_offset: u64,
    dst_row_stride: u64,
    dst_col_stride: u64,
};
pub const CopyLaunch = struct { params: CopyParams, groups: usize };
pub fn prepareCopy(comptime c: Copy, src: Rows, src_len: usize, dst: Rows, dst_len: usize, ids: ?[]const u32) error{InvalidLayout}!CopyLaunch {
    if (c.threads == 0 or c.threads > 1024) @compileError("copy threads must be between 1 and 1024");
    if (src.cols != dst.cols or (!c.gather and src.rows != dst.rows)) return error.InvalidLayout;
    try src.validate(src_len);
    try dst.validate(dst_len);
    if (c.gather) {
        const indices = ids orelse return error.InvalidLayout;
        if (indices.len != dst.rows) return error.InvalidLayout;
        for (indices) |id| {
            if (id >= src.rows) return error.InvalidLayout;
        }
    } else if (ids != null) return error.InvalidLayout;
    const count = std.math.mul(usize, dst.rows, dst.cols) catch return error.InvalidLayout;
    const groups = count / c.threads + @intFromBool(count % c.threads != 0);
    // Metal's threadgroup position is uint; individual element offsets remain u64.
    if (groups > std.math.maxInt(u32)) return error.InvalidLayout;
    return .{ .params = .{ .rows = dst.rows, .cols = dst.cols, .src_offset = src.offset, .src_row_stride = src.row_stride, .src_col_stride = src.col_stride, .dst_offset = dst.offset, .dst_row_stride = dst.row_stride, .dst_col_stride = dst.col_stride }, .groups = groups };
}
