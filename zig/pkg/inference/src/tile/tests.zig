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

//! Tile kernel tests: every lowering against the scalar reference.
//! Run with `zig build test-tile-kernels`.

const std = @import("std");
const tile = @import("tile.zig");
const cpu = @import("cpu.zig");
const gpu = @import("gpu.zig");
const whisper = @import("whisper_logits.zig");

const k = whisper.kernel;

const Params = struct {
    out_dim: u32,
    suppress_count: u32 = 0,
    ts_begin: u32,
    text_allowed: u32 = 1,
    ts_min: u32,
    ts_max: u32,
    eot: u32,
    probe_id: u32 = 0,
    mode: u32 = 0,
    token_slot: u32 = 0,
    stats_slot: u32 = 0,
    reserved: u32 = 0,
};

fn expectStates(expected: k.Result(), actual: k.Result()) !void {
    inline for (k.channels, 0..) |c, ci| {
        const e = expected[ci];
        const a = actual[ci];
        switch (c.reduce) {
            .argmax => {
                if (e.id() != a.id() or !(e.a == a.a or (std.math.isNan(e.a) and std.math.isNan(a.a)))) {
                    std.debug.print("channel {s}: expected ({d}, {d}) got ({d}, {d})\n", .{ c.name, e.a, e.id(), a.a, a.id() });
                    return error.TestExpectedEqual;
                }
            },
            .logsumexp => {
                try std.testing.expectEqual(e.a, a.a);
                try std.testing.expectApproxEqRel(e.b, a.b, 1e-4);
            },
        }
    }
}

const Case = struct {
    values: []f32,
    params: Params,
    suppress: []i32,
    grammar: [8]u32,
};

fn randomCase(allocator: std.mem.Allocator, rng: std.Random) !Case {
    const sizes = [_]usize{ 0, 1, 7, 33, 1024, 1500, 4099, 51_865 };
    const n = sizes[rng.uintLessThan(usize, sizes.len)];
    const values = try allocator.alloc(f32, n + rng.uintLessThan(usize, 3));
    const coarse = rng.boolean(); // coarse values produce exact ties
    for (values) |*v| {
        v.* = if (coarse) @floatFromInt(rng.intRangeAtMost(i32, -4, 4)) else (rng.float(f32) - 0.5) * 40;
        if (rng.uintLessThan(u32, 50) == 0) v.* = tile.neg_inf;
    }
    const dim: u32 = @intCast(n);
    const ts_begin: u32 = if (dim == 0) 0 else rng.uintAtMost(u32, dim);
    const ts_min = ts_begin + (if (dim > ts_begin) rng.uintAtMost(u32, dim - ts_begin) else 0);
    const ts_max = ts_min + (if (dim > ts_min) rng.uintAtMost(u32, dim - ts_min) else 0);
    const suppress = try allocator.alloc(i32, rng.uintLessThan(usize, 100));
    for (suppress) |*s| s.* = rng.intRangeAtMost(i32, -2, @intCast(dim + 2));
    var grammar: [8]u32 = @splat(0);
    grammar[0] = rng.uintLessThan(u32, 2);
    grammar[1] = ts_begin;
    grammar[2] = ts_max;
    return .{
        .values = values,
        .params = .{
            .out_dim = dim,
            .suppress_count = @intCast(suppress.len),
            .ts_begin = ts_begin,
            .text_allowed = rng.uintLessThan(u32, 2),
            .ts_min = ts_min,
            .ts_max = ts_max,
            .eot = if (dim == 0) 0 else rng.uintLessThan(u32, dim + 1),
            .mode = rng.uintLessThan(u32, 16),
        },
        .suppress = suppress,
        .grammar = grammar,
    };
}

test "tile whisper logits: CPU vector lowering matches the scalar reference" {
    var prng = std.Random.DefaultPrng.init(0x7117_e5ee);
    const rng = prng.random();
    for (0..200) |_| {
        var arena = std.heap.ArenaAllocator.init(std.testing.allocator);
        defer arena.deinit();
        const case = try randomCase(arena.allocator(), rng);
        const args = whisper.args(case.values, case.params, case.suppress, &case.grammar);
        const expected = tile.reference(k, args);
        try expectStates(expected, cpu.RowReduce(k, 8).run(&args));
        try expectStates(expected, cpu.RowReduce(k, 16).run(&args));
    }
}

test "tile whisper logits: block partials combine to the full reduction" {
    var prng = std.Random.DefaultPrng.init(0x9a27_1a15);
    const rng = prng.random();
    for (0..100) |_| {
        var arena = std.heap.ArenaAllocator.init(std.testing.allocator);
        defer arena.deinit();
        const case = try randomCase(arena.allocator(), rng);
        const args = whisper.args(case.values, case.params, case.suppress, &case.grammar);
        const blocks = (case.values.len + k.block - 1) / k.block;
        const records = try arena.allocator().alloc(f32, @max(blocks, 1) * k.partial_stride);
        const written = cpu.RowReduce(k, 8).partials(&args, records);
        try expectStates(tile.reference(k, args), tile.combineRecords(k, records[0 .. written * k.partial_stride]));
    }
}

test "tile whisper logits: grammar window overrides params in device-window mode" {
    var values: [64]f32 = undefined;
    for (&values, 0..) |*v, i| v.* = @floatFromInt(i);
    const base: Params = .{ .out_dim = 64, .ts_begin = 32, .text_allowed = 1, .ts_min = 32, .ts_max = 64, .eot = 31 };
    const grammar = [8]u32{ 0, 40, 48, 0, 0, 0, 0, 0 };

    const plain = tile.reference(k, whisper.args(&values, base, &.{}, &grammar));
    try std.testing.expectEqual(@as(u32, 63), plain[0].id()); // best
    try std.testing.expectEqual(@as(u32, 31), plain[1].id()); // text

    var windowed = base;
    windowed.mode = 1;
    const out = tile.reference(k, whisper.args(&values, windowed, &.{}, &grammar));
    try std.testing.expectEqual(@as(u32, 47), out[0].id()); // best: ts window [40, 48)
    try std.testing.expectEqual(@as(u32, 31), out[1].id()); // text: only eot survives
    try std.testing.expectEqual(@as(u32, 47), out[2].id()); // ts
    try std.testing.expectEqual(@as(u32, 63), out[3].id()); // raw ignores masks
    try expectStates(out, cpu.RowReduce(k, 8).run(&whisper.args(&values, windowed, &.{}, &grammar)));
}

test "tile whisper logits: suppressed eot and -inf logits" {
    var values = [_]f32{ tile.neg_inf, 1, 5, tile.neg_inf, 2 };
    const params: Params = .{ .out_dim = 5, .ts_begin = 3, .ts_min = 3, .ts_max = 5, .eot = 2 };
    const out = tile.reference(k, whisper.args(&values, params, &.{ 2, -1 }, &@as([8]u32, @splat(0))));
    try std.testing.expectEqual(@as(u32, 4), out[0].id()); // eot (index 2) suppressed
    try std.testing.expectEqual(@as(u32, 1), out[1].id());
    // -inf contributes nothing; the hand-written kernel's push would turn a
    // leading -inf into NaN.
    try std.testing.expect(!std.math.isNan(out[4].b));
    try std.testing.expectApproxEqRel(@exp(@as(f32, -1)) + 1, out[4].b, 1e-6);
}

test "tile whisper logits: rendered sources keep the hand-written ABI" {
    const msl = comptime gpu.renderMetal(k);
    try std.testing.expect(std.mem.indexOf(u8, msl, "kernel void antfly_tile_whisper_logits_partials(device const float *logits [[buffer(0)]], device const int *suppress_ids [[buffer(1)]], device float *partials [[buffer(2)]], constant antfly_tile_whisper_logits_params &p [[buffer(3)]], device const uint *grammar [[buffer(4)]], threadgroup float *sh [[threadgroup(0)]]") != null);
    try std.testing.expect(std.mem.indexOf(u8, msl, "struct antfly_tile_whisper_logits_params { uint out_dim; uint suppress_count; uint ts_begin; uint text_allowed; uint ts_min; uint ts_max; uint eot; uint probe_id; uint mode; uint token_slot; uint stats_slot; uint reserved; };") != null);
    const cu = comptime gpu.renderCuda(k);
    try std.testing.expect(std.mem.indexOf(u8, cu, "extern \"C\" __global__ void __launch_bounds__(256) antfly_tile_whisper_logits_partials(") != null);
    try std.testing.expect(std.mem.indexOf(u8, cu, "antfly_tile_whisper_logits_partials_combine(") != null);
}

test "tile cpu vexp matches @exp" {
    var x: f32 = -90;
    while (x < 88) : (x += 0.0137) {
        const v: @Vector(8, f32) = @splat(x);
        const got = cpu.vexp(8, v)[0];
        const want = @exp(x);
        if (x < -87.3) {
            try std.testing.expectEqual(@as(f32, 0), got);
        } else {
            try std.testing.expectApproxEqRel(want, got, 4e-7);
        }
    }
    try std.testing.expectEqual(@as(f32, 0), cpu.vexp(8, @splat(tile.neg_inf))[0]);
    try std.testing.expectEqual(@as(f32, 1), cpu.vexp(8, @splat(0))[0]);
}

// ---- Model primitives (GEMM, LayerNorm, attention, data movement) ----

const fexpr = @import("fexpr.zig");
const vmath = @import("vmath.zig");
const gemm = @import("gemm.zig");
const norm = @import("norm.zig");
const attention = @import("attention.zig");
const layout = @import("layout.zig");

fn fill(rng: std.Random, xs: []f32, scale: f32) void {
    for (xs) |*x| x.* = (rng.float(f32) - 0.5) * 2 * scale;
}

fn expectClose(expected: []const f32, actual: []const f32, rel: f32, abs: f32) !void {
    for (expected, actual, 0..) |e, a, i| {
        if (@abs(e - a) > abs + rel * @abs(e)) {
            std.debug.print("index {d}: expected {d}, got {d}\n", .{ i, e, a });
            return error.TestExpectedApproxEq;
        }
    }
}

test "tile vmath erf and gelu track the f64 references" {
    var x: f32 = -6;
    while (x < 6) : (x += 0.00731) {
        try std.testing.expectApproxEqAbs(@as(f32, @floatCast(vmath.erfF64(x))), vmath.verf(4, @splat(x))[0], 6e-7);
        const g: f32 = @floatCast(vmath.geluF64(x));
        try std.testing.expectApproxEqAbs(g, vmath.vgelu(4, @splat(x))[0], 1e-6 + 1e-6 * @abs(g));
    }
}

test "tile gemm matches the f64 reference across shapes and epilogues" {
    const kernels = comptime .{
        gemm.Gemm{ .name = "plain" },
        gemm.Gemm{ .name = "bias_gelu", .epilogue = fexpr.acc.plus(.bias).geluOf() },
        gemm.Gemm{ .name = "bias_res", .epilogue = fexpr.acc.plus(.bias).plus(.residual) },
        gemm.Gemm{ .name = "scaled", .epilogue = fexpr.acc.plus(.bias).times(fexpr.lit(0.125)) },
    };
    var prng = std.Random.DefaultPrng.init(0x6e33);
    const rng = prng.random();
    const shapes = [_][3]usize{ .{ 1, 1, 1 }, .{ 1, 37, 16 }, .{ 5, 7, 33 }, .{ 33, 18, 64 }, .{ 70, 65, 240 }, .{ 1, 300, 384 } };
    inline for (kernels) |g| for (shapes) |s| {
        const m, const n, const kd = s;
        const alloc = std.testing.allocator;
        const x = try alloc.alloc(f32, m * kd);
        defer alloc.free(x);
        const w = try alloc.alloc(f32, n * kd);
        defer alloc.free(w);
        const bias = try alloc.alloc(f32, n);
        defer alloc.free(bias);
        const res = try alloc.alloc(f32, m * n);
        defer alloc.free(res);
        const want = try alloc.alloc(f32, m * n);
        defer alloc.free(want);
        const got = try alloc.alloc(f32, m * n);
        defer alloc.free(got);
        fill(rng, x, 1);
        fill(rng, w, 1);
        fill(rng, bias, 1);
        fill(rng, res, 1);
        gemm.reference(g, .{ .x = x, .w = w, .bias = bias, .residual = res.ptr, .out = want.ptr, .m = m, .n = n, .k = kd });
        // In place: the residual is the output buffer.
        @memcpy(got, res);
        gemm.run(g, .{ .x = x, .w = w, .bias = bias, .residual = got.ptr, .out = got.ptr, .m = m, .n = n, .k = kd });
        try expectClose(want, got, 1e-5, 1e-5 * @as(f32, @floatFromInt(kd)));
    };
}

test "tile layernorm matches the f64 reference" {
    var prng = std.Random.DefaultPrng.init(0x1a7e);
    const rng = prng.random();
    const ln = norm.LayerNorm{ .name = "ln" };
    for ([_][2]usize{ .{ 1, 384 }, .{ 9, 384 }, .{ 3, 17 }, .{ 40, 64 } }) |s| {
        const rows, const dim = s;
        const alloc = std.testing.allocator;
        const x = try alloc.alloc(f32, rows * dim);
        defer alloc.free(x);
        const gamma = try alloc.alloc(f32, dim);
        defer alloc.free(gamma);
        const beta = try alloc.alloc(f32, dim);
        defer alloc.free(beta);
        const want = try alloc.alloc(f32, rows * dim);
        defer alloc.free(want);
        const got = try alloc.alloc(f32, rows * dim);
        defer alloc.free(got);
        fill(rng, x, 4);
        for (x, 0..) |*v, i| v.* += @floatFromInt(i % 3); // nonzero mean
        fill(rng, gamma, 1);
        fill(rng, beta, 1);
        norm.reference(ln, .{ .x = x, .gamma = gamma, .beta = beta, .out = want, .rows = rows, .dim = dim });
        norm.run(ln, .{ .x = x, .gamma = gamma, .beta = beta, .out = got, .rows = rows, .dim = dim });
        try expectClose(want, got, 1e-5, 1e-5);
    }
}

test "tile attention matches the f64 reference (self, causal, cross)" {
    var prng = std.Random.DefaultPrng.init(0xa77e);
    const rng = prng.random();
    const att = attention.Attention{ .name = "att", .head_dim = 64 };
    const AttCase = struct { tq: usize, tk: usize, heads: usize, causal: bool, q_offset: usize };
    const cases = [_]AttCase{
        .{ .tq = 7, .tk = 7, .heads = 2, .causal = false, .q_offset = 0 },
        .{ .tq = 5, .tk = 5, .heads = 3, .causal = true, .q_offset = 0 },
        .{ .tq = 1, .tk = 9, .heads = 6, .causal = true, .q_offset = 8 },
        .{ .tq = 3, .tk = 37, .heads = 2, .causal = true, .q_offset = 34 },
        .{ .tq = 4, .tk = 150, .heads = 6, .causal = false, .q_offset = 0 },
    };
    for (cases) |c| {
        const ld = c.heads * 64;
        const alloc = std.testing.allocator;
        const q = try alloc.alloc(f32, c.tq * ld);
        defer alloc.free(q);
        const kk = try alloc.alloc(f32, c.tk * ld);
        defer alloc.free(kk);
        const v = try alloc.alloc(f32, c.tk * ld);
        defer alloc.free(v);
        const want = try alloc.alloc(f32, c.tq * ld);
        defer alloc.free(want);
        const got = try alloc.alloc(f32, c.tq * ld);
        defer alloc.free(got);
        fill(rng, q, 1);
        fill(rng, kk, 1);
        fill(rng, v, 1);
        const args: attention.Args = .{ .q = q, .k = kk, .v = v, .out = want, .tq = c.tq, .tk = c.tk, .heads = c.heads, .ld_q = ld, .ld_k = ld, .ld_v = ld, .ld_o = ld, .scale = 0.125, .causal = c.causal, .q_offset = c.q_offset };
        try attention.reference(att, args);
        var a2 = args;
        a2.out = got;
        attention.run(att, a2);
        try expectClose(want, got, 1e-5, 2e-6);
    }
}

test "tile im2col and embed lay data out as conv and lookup expect" {
    // channel-major, stride 1, pad 1: out[t, c*3 + j] = x[c, t + j - 1]
    const x = [_]f32{ 1, 2, 3, 10, 20, 30 }; // C=2, T=3
    var out: [3 * 6]f32 = undefined;
    layout.im2col(.{ .name = "a", .kernel = 3, .stride = 1, .pad = 1, .layout = .channel_major }, .{ .x = &x, .out = &out, .channels = 2, .t_in = 3 });
    try std.testing.expectEqualSlices(f32, &.{ 0, 1, 2, 0, 10, 20, 1, 2, 3, 10, 20, 30, 2, 3, 0, 20, 30, 0 }, &out);
    // time-major, stride 2: x[t, c]; T=4 -> 2 outputs
    const y = [_]f32{ 1, 10, 2, 20, 3, 30, 4, 40 };
    var out2: [2 * 6]f32 = undefined;
    layout.im2col(.{ .name = "b", .kernel = 3, .stride = 2, .pad = 1, .layout = .time_major }, .{ .x = &y, .out = &out2, .channels = 2, .t_in = 4 });
    try std.testing.expectEqualSlices(f32, &.{ 0, 1, 2, 0, 10, 20, 2, 3, 4, 20, 30, 40 }, &out2);
    const table = [_]f32{ 0, 0, 1, 1, 2, 2 };
    const pos = [_]f32{ 0.5, 0.5, 0.25, 0.25, 0.125, 0.125 };
    var e: [4]f32 = undefined;
    layout.embed(.{ .name = "e" }, .{ .ids = &.{ 2, 1 }, .table = &table, .pos = &pos, .out = &e, .dim = 2, .pos_offset = 1 });
    try std.testing.expectEqualSlices(f32, &.{ 2.25, 2.25, 1.125, 1.125 }, &e);
}
