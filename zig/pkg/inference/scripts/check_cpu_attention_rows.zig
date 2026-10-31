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

//! Frozen-original/candidate CPU attention parity and fixed AA/AB/BA timing.
//! Build with `baseline` and `candidate` modules rooted at their linalg mod.zig.
//! --bench: 31 pairs, one complete call/sample, median <=1.05, p95 <=1.10.
const std = @import("std");
const baseline = @import("baseline");
const candidate = @import("candidate");
const Mode = enum { causal, flash, segment, dense };
const Gemm = struct {
    pub const query_block = 64;
    pub const key_block = 256;
    pub const dense_query_block = 128;
    pub const dense_key_block = 256;
    pub const sgemmSync = candidate.sgemmSequential;
    pub const sgemmTransBSync = candidate.sgemmTransBSequential;
};
const Case = struct {
    mode: Mode,
    rows: usize,
    keys: usize,
    dim: usize,
    heads: usize = 2,
    kv_heads: usize,
    q: []f32,
    k: []f32,
    v: []f32,
    mask: []i64,
    bias: []f32,
    ranges: []u32,
    query_positions: []i32,
    key_positions: []i32,

    fn init(a: std.mem.Allocator, mode: Mode, rows: usize, keys: usize, dim: usize) !Case {
        const grouped = mode == .causal or mode == .segment;
        const count = if (grouped) keys else rows;
        var c: Case = .{
            .mode = mode,
            .rows = rows,
            .keys = count,
            .dim = dim,
            .kv_heads = if (grouped) 1 else 2,
            .q = try a.alloc(f32, rows * 2 * dim),
            .k = try a.alloc(f32, count * (if (grouped) @as(usize, 1) else 2) * dim),
            .v = undefined,
            .mask = try a.alloc(i64, count),
            .bias = try a.alloc(f32, rows * count),
            .ranges = try a.alloc(u32, rows * 6),
            .query_positions = try a.alloc(i32, rows),
            .key_positions = try a.alloc(i32, count),
        };
        c.v = try a.alloc(f32, c.k.len);
        for (c.q, 0..) |*x, i| x.* = @sin(@as(f32, @floatFromInt(i)) * 0.113) * 0.7;
        for (c.k, c.v, 0..) |*x, *y, i| {
            x.* = @cos(@as(f32, @floatFromInt(i)) * 0.173) * (if (i >= c.k.len / 2) @as(f32, 1.3) else 0.4);
            y.* = @sin(@as(f32, @floatFromInt(i)) * 0.271);
        }
        for (c.mask, 0..) |*x, i| x.* = @intFromBool(i % 7 != 3 and (count < 513 or i >= 256));
        for (c.bias, 0..) |*x, i| x.* = @as(f32, @floatFromInt(i % 17)) / 23;
        for (0..rows) |row| {
            c.ranges[row * 6 ..][0..6].* = if (row % 7 == 3) .{ 0, 0, 0, 0, 0, 0 } else .{ 0, @intCast(count / 4), @intCast(count / 2), @intCast(count * 3 / 4), @intCast(count - 1), @intCast(count) };
            c.query_positions[row] = @as(i32, @intCast(row * 3)) - 17;
        }
        for (c.key_positions, 0..) |*x, i| x.* = @as(i32, @intCast(i * 2)) - 11;
        return c;
    }
    fn deinit(c: Case, a: std.mem.Allocator) void {
        inline for (.{ "q", "k", "v", "mask", "bias", "ranges", "query_positions", "key_positions" }) |field| a.free(@field(c, field));
    }
    fn run(c: Case, comptime impl: type, a: std.mem.Allocator) ![]f32 {
        return switch (c.mode) {
            .causal => impl.flashCausalAttentionHost(a, c.q, c.k, c.v, c.bias, null, 31, 1, c.rows, c.keys, c.keys / 2, 5, c.heads, c.kv_heads, c.dim),
            .flash => impl.flashAttentionHost(a, c.q, c.k, c.v, c.bias, c.mask, 1, c.rows, c.heads, c.dim),
            .segment => impl.segmentAttentionGroupedHostWithGemmIo(Gemm, a, null, c.q, c.k, c.v, c.ranges, c.query_positions, c.key_positions, if (c.dim % 2 == 0) std.math.maxInt(u32) else 13, c.rows, c.keys, c.heads, c.kv_heads, c.dim, 1 / @sqrt(@as(f32, @floatFromInt(c.dim)))),
            .dense => impl.denseAttentionHostWithGemmIo(Gemm, a, null, c.q, c.k, c.v, 1, c.rows, c.heads, c.dim),
        };
    }
};
fn elapsed(io: std.Io, c: Case, comptime impl: type, a: std.mem.Allocator) !f64 {
    const start = std.Io.Clock.awake.now(io).nanoseconds;
    const output = try c.run(impl, a);
    std.mem.doNotOptimizeAway(output);
    a.free(output);
    const end = std.Io.Clock.awake.now(io).nanoseconds;
    return @floatFromInt(end - start);
}
fn less(_: void, x: f64, y: f64) bool {
    return x < y;
}
fn compare(c: Case, a: std.mem.Allocator) !void {
    const old = try c.run(baseline, a);
    defer a.free(old);
    const new = try c.run(candidate, a);
    defer a.free(new);
    if (!std.mem.eql(u8, std.mem.sliceAsBytes(old), std.mem.sliceAsBytes(new))) return error.OriginalBitwiseParity;
    std.debug.print("{{\"kind\":\"correctness\",\"mode\":\"{s}\",\"rows\":{d},\"keys\":{d},\"dim\":{d},\"pass\":true}}\n", .{ @tagName(c.mode), c.rows, c.keys, c.dim });
}
fn timing(io: std.Io, c: Case, a: std.mem.Allocator) !void {
    for (0..8) |_| {
        _ = try elapsed(io, c, baseline, a);
        _ = try elapsed(io, c, candidate, a);
    }
    for (0..3) |order| {
        var left: [31]f64 = undefined;
        var right: [31]f64 = undefined;
        for (&left, &right, 0..) |*x, *y, i| {
            if (order == 2) {
                x.* = try elapsed(io, c, candidate, a);
                y.* = try elapsed(io, c, baseline, a);
            } else {
                x.* = try elapsed(io, c, baseline, a);
                y.* = if (order == 0) try elapsed(io, c, baseline, a) else try elapsed(io, c, candidate, a);
            }
            std.debug.print("{{\"kind\":\"sample\",\"mode\":\"{s}\",\"order\":{d},\"i\":{d},\"left_ns\":{d},\"right_ns\":{d}}}\n", .{ @tagName(c.mode), order, i, x.*, y.* });
        }
        std.mem.sort(f64, &left, {}, less);
        std.mem.sort(f64, &right, {}, less);
        const median = if (order == 2) left[15] / right[15] else right[15] / left[15];
        const tail = if (order == 2) left[29] / right[29] else right[29] / left[29];
        std.debug.print("{{\"kind\":\"timing\",\"mode\":\"{s}\",\"order\":{d},\"median_ratio\":{d},\"p95_ratio\":{d},\"pass\":{s}}}\n", .{ @tagName(c.mode), order, median, tail, if (median <= 1.05 and tail <= 1.10) "true" else "false" });
    }
}
pub fn main(init: std.process.Init) !void {
    const a = init.gpa;
    var args = std.process.Args.Iterator.init(init.minimal.args);
    _ = args.next();
    const flag = args.next();
    const bench = if (flag) |v| std.mem.eql(u8, v, "--bench") else false;
    if ((flag != null and !bench) or args.next() != null) return error.InvalidArguments;
    inline for (.{ Mode.causal, Mode.flash, Mode.segment, Mode.dense }) |mode| {
        for ([_]usize{ 3, 7, 8, 9, 31, 32, 33, 64, 65, 128 }) |dim| for (0..3) |shape| {
            const rows = if (mode == .dense) ([_]usize{ 33, 257, 513 })[shape] else ([_]usize{ 1, 17, 65 })[shape];
            const c = try Case.init(a, mode, rows, ([_]usize{ 33, 257, 513 })[shape], dim);
            defer c.deinit(a);
            try compare(c, a);
        };
        if (mode == .flash) {
            const c = try Case.init(a, mode, 513, 513, 65);
            defer c.deinit(a);
            try compare(c, a);
        }
        if (bench) {
            const c = try Case.init(a, mode, if (mode == .dense or mode == .flash) 257 else 129, 513, 64);
            defer c.deinit(a);
            try compare(c, a);
            try timing(init.io, c, a);
        }
    }
}
