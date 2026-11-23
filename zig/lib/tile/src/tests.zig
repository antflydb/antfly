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

const std = @import("std");
const core = @import("mod.zig");

test "shared page mapping preserves entry units and rejects wide overflow" {
    const blocks: core.layout.PageTable = .{ .unit = .blocks };
    const bases: core.layout.PageTable = .{ .unit = .token_bases };
    try std.testing.expectEqual(@as(?u32, 19), blocks.map(7, &.{ 9, 4 }, 4, 64));
    try std.testing.expectEqual(@as(?u32, 7), bases.map(7, &.{ 9, 4 }, 4, 64));
    try std.testing.expectEqual(@as(?u32, null), blocks.map(8, &.{ 9, 4 }, 4, 64));
    try std.testing.expectEqual(@as(?u32, null), blocks.map(0, &.{std.math.maxInt(u32)}, 4, std.math.maxInt(u32)));
    try std.testing.expectEqual(@as(?u32, null), bases.map(3, &.{std.math.maxInt(u32)}, 4, std.math.maxInt(u32)));
    try std.testing.expectEqual(@as(?u32, null), blocks.map(0, &.{0}, 0, 64));
}

test "transpose launch contract covers tails and keeps backend schedules" {
    try std.testing.expectEqual([3]usize{ 3, 2, 7 }, try core.native_layout.Transpose.grid(33, 65, 7));
    try std.testing.expectEqual(@as(u32, 256), core.native_layout.cuda_transpose.threads());
    try std.testing.expectEqual(@as(u32, 1024), core.native_layout.metal_transpose.threads());
    try std.testing.expectError(error.InvalidLayout, core.native_layout.Transpose.grid(1, std.math.maxInt(usize), 1));
}

test "complex product keeps scalar and SIMD FFT butterfly association" {
    const values = [_]f32{ 0, 1, -2, 1e-8, 1e8, -3.25 };
    for (values, 0..) |r, i| {
        const im = values[(i + 1) % values.len];
        const wr: f32 = 0.37;
        const wi: f32 = -0.83;
        const actual = core.native_audio.product(r, im, wr, wi);
        const expected_r = r * wr - im * wi;
        const expected_i = r * wi + im * wr;
        try std.testing.expectEqual(@as(u32, @bitCast(expected_r)), @as(u32, @bitCast(actual.real)));
        try std.testing.expectEqual(@as(u32, @bitCast(expected_i)), @as(u32, @bitCast(actual.imag)));
        const vector = core.native_audio.product(@as(@Vector(4, f32), @splat(r)), @as(@Vector(4, f32), @splat(im)), @as(@Vector(4, f32), @splat(wr)), @as(@Vector(4, f32), @splat(wi)));
        try std.testing.expectEqual(@as(u32, @bitCast(expected_r)), @as(u32, @bitCast(vector.real[3])));
    }
    const rendered = comptime core.native_audio.real_product.renderNative(.{ .mul = "MUL", .sub = "SUB" }, .{ .real = "r", .imag = "i", .wr = "wr", .wi = "wi" }, null);
    try std.testing.expectEqualStrings("SUB(MUL(r, wr), MUL(i, wi))", rendered);
}

test "online attention state agrees with independent stable softmax" {
    const scores = [_]f32{ -100, 2, 3, -7, 9, 8, 30, -50 };
    var old_max: f32 = -std.math.inf(f32);
    var sum: f32 = 0;
    var weighted: f32 = 0;
    for (0..4) |tile| {
        const new_max = @max(old_max, @max(scores[tile * 2], scores[tile * 2 + 1]));
        const correction: f32 = if (std.math.isFinite(old_max)) @exp(old_max - new_max) else 0;
        var tile_sum: f32 = 0;
        var tile_weighted: f32 = 0;
        for (0..2) |col| {
            const i = tile * 2 + col;
            const probability = core.native_attention.Online.probability.evaluate(core.native_math.ScalarMath, .{ .value = scores[i], .score = scores[i], .new_max = new_max });
            tile_sum += probability;
            tile_weighted += probability * @as(f32, @floatFromInt(i + 1));
        }
        sum = core.native_attention.Online.updateMass(sum, correction, tile_sum);
        weighted = core.native_attention.Online.updateMass(weighted, correction, tile_weighted);
        old_max = new_max;
    }
    var reference_sum: f64 = 0;
    var reference_value: f64 = 0;
    for (scores, 0..) |score, i| {
        const p = @exp(@as(f64, score) - 30);
        reference_sum += p;
        reference_value += p * @as(f64, @floatFromInt(i + 1));
    }
    try std.testing.expectApproxEqAbs(@as(f32, @floatCast(reference_value / reference_sum)), weighted / sum, 1e-6);
    try std.testing.expect(core.native_attention.CausalWindow.contains(7, 4, 4));
    try std.testing.expect(!core.native_attention.CausalWindow.contains(7, 3, 4));
    try std.testing.expect(!core.native_attention.CausalWindow.contains(7, 8, 0));
}

test "shared GRU cell preserves independent reset update and candidate stages" {
    for (0..32) |i| {
        const x = @as(f32, @floatFromInt(i)) / 7 - 2;
        const gi = [3]f32{ x, x * -0.7, x + 0.2 };
        const gh = [3]f32{ x * 0.3, x - 0.4, x * -1.2 };
        const previous = x / 3;
        const r = 1 / (1 + @exp(-(gi[0] + gh[0])));
        const z = 1 / (1 + @exp(-(gi[1] + gh[1])));
        const n = std.math.tanh(gi[2] + r * gh[2]);
        const expected = (1 - z) * n + z * previous;
        try std.testing.expectEqual(@as(u32, @bitCast(expected)), @as(u32, @bitCast(core.native_math.Gru.cell(gi, gh, previous))));
    }
}

test "first matching row program keeps earliest token missing words and batches" {
    var output: [12]f32 = @splat(-1);
    var seen: [6]u8 = undefined;
    try (core.native_layout.Program{ .operation = .first_match }).run(.{ .input = &.{ 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12 }, .output = &output, .rows = 6, .cols = 2, .mask = &.{ 2, 2, -1, 1, 3, 1 }, .sequence = 3, .words = 3, .seen = &seen });
    try std.testing.expectEqualSlices(f32, &.{ 0, 0, 1, 2, 0, 0, 7, 8, 0, 0, 9, 10 }, &output);
    const before = output;
    try std.testing.expectError(error.InvalidLayout, (core.native_layout.Program{ .operation = .first_match }).run(.{ .input = &.{}, .output = &output, .rows = 6, .cols = 2, .mask = &.{1}, .sequence = 3, .words = 3 }));
    try std.testing.expectEqualSlices(f32, &before, &output);
}

test "shared native tree preserves cancellation association and interleaved planes" {
    for ([_]usize{ 1, 2, 8, 32, 64, 256 }) |width| {
        var a: [256]f32 = undefined;
        var b: [256]f32 = undefined;
        for (0..width) |lane| {
            a[lane] = @as(f32, @floatFromInt(@as(i32, @intCast(lane % 13)) - 6)) * (if (lane % 3 == 0) @as(f32, 0x1p20) else @as(f32, 0x1p-8));
            b[lane] = @as(f32, @floatFromInt(lane)) + 0.25;
        }
        var expected_a = a;
        var expected_b = b;
        var stride = width >> 1;
        while (stride != 0) : (stride >>= 1) for (0..stride) |lane| {
            expected_a[lane] += expected_a[lane + stride];
            expected_b[lane] += expected_b[lane + stride];
        };
        try core.native_statistics.Tree.reduce(&.{ a[0..width], b[0..width] });
        try std.testing.expectEqual(@as(u32, @bitCast(expected_a[0])), @as(u32, @bitCast(a[0])));
        try std.testing.expectEqual(@as(u32, @bitCast(expected_b[0])), @as(u32, @bitCast(b[0])));
    }
    var invalid = [_]f32{ 1, 2, 3 };
    try std.testing.expectError(error.InvalidLayout, core.native_statistics.Tree.reduce(&.{&invalid}));
    try std.testing.expectEqualSlices(f32, &.{ 1, 2, 3 }, &invalid);
}

test "paired native tree chooses global finite maximum then lowest index" {
    const Tree = core.native_statistics.Tree;
    for ([_]usize{ 1, 2, 8, 32, 64, 256 }) |width| {
        var values: [256]f32 = undefined;
        var indices: [256]u32 = undefined;
        var expected_value: f32 = -std.math.inf(f32);
        var expected_index: u32 = std.math.maxInt(u32);
        for (0..width) |lane| {
            values[lane] = @as(f32, @floatFromInt(@as(i32, @intCast((lane * 17) % 31)) - 15));
            indices[lane] = @intCast(width - lane);
            // Independent serial selection versus the halving native tree.
            if (values[lane] > expected_value or (values[lane] == expected_value and indices[lane] < expected_index)) {
                expected_value = values[lane];
                expected_index = indices[lane];
            }
        }
        try Tree.reduceIndexed(values[0..width], indices[0..width]);
        try std.testing.expectEqual(expected_value, values[0]);
        try std.testing.expectEqual(expected_index, indices[0]);
    }
}

test "paired native tree preserves unordered NaNs infinities and signed-zero identity" {
    const nan: f32 = @bitCast(@as(u32, 0x7fc01234));
    const inf = std.math.inf(f32);
    const cases = [_]struct { values: [2]f32, ids: [2]u32, winner: usize }{
        .{ .values = .{ nan, 4 }, .ids = .{ 9, 1 }, .winner = 0 },
        .{ .values = .{ 4, nan }, .ids = .{ 9, 1 }, .winner = 0 },
        .{ .values = .{ -inf, -inf }, .ids = .{ 9, 1 }, .winner = 1 },
        .{ .values = .{ inf, inf }, .ids = .{ 9, 1 }, .winner = 1 },
        .{ .values = .{ -0.0, 0.0 }, .ids = .{ 9, 1 }, .winner = 1 },
        .{ .values = .{ -0.0, 0.0 }, .ids = .{ 1, 1 }, .winner = 0 },
    };
    for (cases) |c| {
        var values = c.values;
        var ids = c.ids;
        try core.native_statistics.Tree.reduceIndexed(&values, &ids);
        try std.testing.expectEqual(@as(u32, @bitCast(c.values[c.winner])), @as(u32, @bitCast(values[0])));
        try std.testing.expectEqual(c.ids[c.winner], ids[0]);
    }
    var invalid = [_]f32{ 1, 2, 3 };
    var ids = [_]u32{ 3, 2, 1 };
    try std.testing.expectError(error.InvalidLayout, core.native_statistics.Tree.reduceIndexed(&invalid, &ids));
    try std.testing.expectEqualSlices(f32, &.{ 1, 2, 3 }, &invalid);
    try std.testing.expectEqualSlices(u32, &.{ 3, 2, 1 }, &ids);
    try std.testing.expectError(error.InvalidLayout, core.native_statistics.Tree.reduceIndexed(invalid[0..2], ids[0..1]));
}

test "ordered suppression retains ties, first allowed NaN, infinities and empty identity" {
    const nan = std.math.nan(f32);
    const inf = std.math.inf(f32);
    const cases = [_]struct { values: []const f32, ids: []const i32 = &.{}, index: u32, found: bool = true }{
        .{ .values = &.{}, .index = 0, .found = false },
        .{ .values = &.{ 4, 4, 2 }, .index = 0 },
        .{ .values = &.{ -inf, -inf }, .index = 0 },
        .{ .values = &.{ nan, inf, 9 }, .index = 0 },
        .{ .values = &.{ 1, nan, inf, inf }, .index = 2 },
        .{ .values = &.{ nan, 2, 2 }, .ids = &.{ -1, 0, 99 }, .index = 1 },
        .{ .values = &.{ 2, nan, inf }, .ids = &.{0}, .index = 1 },
        .{ .values = &.{ 9, 8 }, .ids = &.{ 0, 1, 1 }, .index = 0, .found = false },
    };
    for (cases) |c| {
        const result = core.rows.argmaxSuppressed(c.values, c.ids);
        try std.testing.expectEqual(c.index, result.index);
        try std.testing.expectEqual(c.found, result.found);
    }
}

test "masked statistics exclude padding NaNs and define nonfinite logsumexp" {
    const s = core.rows.statistics(&.{ 1, std.math.nan(f32), 3 }, &.{ true, false, true });
    try std.testing.expectEqual(@as(usize, 2), s.count);
    try std.testing.expectEqual(@as(f64, 2), s.mean);
    try std.testing.expectEqual(@as(f64, 1), s.variance);
    try std.testing.expectApproxEqAbs(@as(f64, 3) + @log(1 + @exp(@as(f64, -2))), s.logsumexp, 1e-14);
    try std.testing.expectEqual(-std.math.inf(f64), core.rows.statistics(&.{}, null).logsumexp);
    try std.testing.expectEqual(-std.math.inf(f64), core.rows.statistics(&.{-std.math.inf(f32)}, null).logsumexp);
    try std.testing.expectEqual(std.math.inf(f64), core.rows.statistics(&.{ std.math.inf(f32), 2 }, null).logsumexp);
    try std.testing.expect(std.math.isNan(core.rows.statistics(&.{ std.math.nan(f32), 2 }, null).logsumexp));
}

test "typed views split, transpose, gather and validate before any store" {
    const src = [_]u32{ 0x7fc01234, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11 };
    var out: [6]u32 = @splat(0);
    try core.layout.copyRows(u32, &src, .{ .rows = 2, .cols = 3, .row_stride = 6, .offset = 3 }, &out, .{ .rows = 2, .cols = 3, .row_stride = 3 });
    try std.testing.expectEqualSlices(u32, &.{ 3, 4, 5, 9, 10, 11 }, &out);
    try core.layout.copyRows(u32, src[0..6], .{ .rows = 3, .cols = 2, .row_stride = 1, .col_stride = 3 }, &out, .{ .rows = 3, .cols = 2, .row_stride = 2 });
    try std.testing.expectEqualSlices(u32, &.{ 0x7fc01234, 3, 1, 4, 2, 5 }, &out);
    try core.layout.gatherRows(u32, &src, .{ .rows = 4, .cols = 3, .row_stride = 3 }, &.{ 3, 0 }, &out, .{ .rows = 2, .cols = 3, .row_stride = 3 });
    try std.testing.expectEqualSlices(u32, &.{ 9, 10, 11, 0x7fc01234, 1, 2 }, &out);
    const before = out;
    try std.testing.expectError(error.InvalidLayout, core.layout.gatherRows(u32, &src, .{ .rows = 4, .cols = 3, .row_stride = 3 }, &.{ 0, 4 }, &out, .{ .rows = 2, .cols = 3, .row_stride = 3 }));
    try std.testing.expectEqualSlices(u32, &before, &out);
    try std.testing.expectError(error.InvalidLayout, (core.layout.Rows{ .rows = 3, .cols = 1, .row_stride = std.math.maxInt(usize) }).validate(src.len));
    try core.layout.copyRows(f16, &.{ 1, 2, 3 }, .{ .rows = 0, .cols = 3, .row_stride = 3 }, &.{}, .{ .rows = 0, .cols = 3, .row_stride = 3 });
}

test "RMS and fused residual match independent f64 across SIMD tails and aliasing" {
    var rng = std.Random.DefaultPrng.init(991);
    var x: [258]f32 = undefined;
    var weights: [129]f32 = undefined;
    var residual: [258]f32 = undefined;
    var actual: [258]f32 = undefined;
    var expected: [258]f32 = undefined;
    for (&x, &residual) |*v, *r| {
        v.* = (rng.random().float(f32) - 0.5) * 8;
        r.* = rng.random().float(f32);
    }
    for (&weights) |*v| v.* = rng.random().float(f32);
    inline for (.{ 1, 4, 8, 16 }) |width| inline for (.{ false, true }) |fused| {
        for ([_]usize{ 1, 7, 8, 15, 16, 33, 129 }) |dim| {
            const a: core.norm.RmsArgs = .{ .x = x[0 .. 2 * dim], .gamma = weights[0..dim], .out = expected[0 .. 2 * dim], .dim = dim, .eps = 1e-5, .residual = if (fused) residual[0 .. 2 * dim] else null, .norm_scale = 0.75, .output_scale = -0.5 };
            core.norm.referenceRms(a);
            @memcpy(actual[0 .. 2 * dim], x[0 .. 2 * dim]);
            var b = a;
            b.x = actual[0 .. 2 * dim];
            b.out = actual[0 .. 2 * dim];
            core.norm.runRms(width, fused, b);
            for (expected[0 .. 2 * dim], actual[0 .. 2 * dim]) |e, v| try std.testing.expectApproxEqAbs(e, v, 3e-6);
        }
    };
    var pooled: [3]f32 = undefined;
    core.norm.maskedMean(4, &.{ 2, 4, 6, std.math.nan(f32), 3, 2, 4, 6, 8 }, &.{ true, false, true }, &pooled, 3, 3);
    try std.testing.expectEqualSlices(f32, &.{ 3, 5, 7 }, &pooled);
    core.norm.maskedMean(4, &.{ 1, 2, 3 }, &.{false}, &pooled, 1, 3);
    try std.testing.expectEqualSlices(f32, &.{ 0, 0, 0 }, &pooled);
}

test "projection epilogue precision boundaries, SiLU and gating" {
    const e = core.conformance.Programs.projection.epilogue;
    try std.testing.expect(e.uses(.silu) and e.uses(.mul) and e.uses(.round_f16) and e.uses(.round_bf16));
    const native = comptime e.renderInputs(.{ .silu = "qualified_native_silu" }, .{ .value = "v", .bias = "b", .gate = "up", .residual = "skip" });
    try std.testing.expect(std.mem.indexOf(u8, native, "qualified_native_silu") != null);
    const v: @Vector(4, f32) = .{ -3, -0.125, 0.25, 5 };
    const actual = e.evalVecInputs(4, core.fexpr.DefaultMath, .{ .value = v, .bias = @splat(0.125), .gate = @splat(0.5), .residual = @splat(0.25) });
    inline for (0..4) |i| try std.testing.expectApproxEqAbs(@as(f32, @floatCast(e.evalF64Inputs(.{ .value = v[i], .bias = 0.125, .gate = 0.5, .residual = 0.25 }))), actual[i], 0.008);
    try std.testing.expectEqual(@as(f32, 1), core.fexpr.roundBf16Value(@bitCast(@as(u32, 0x3f808000))));
    try std.testing.expectEqual(@as(u32, 0x3f820000), @as(u32, @bitCast(core.fexpr.roundBf16Value(@bitCast(@as(u32, 0x3f818000))))));
    try std.testing.expect(std.math.isNan(core.fexpr.roundBf16Value(@bitCast(@as(u32, 0x7f800001)))));
    try std.testing.expectEqual(std.math.inf(f32), core.fexpr.roundBf16Value(std.math.inf(f32)));
    inline for (.{ .metal, .cuda, .opencl }) |lang| {
        const Source = core.conformance.Programs;
        const source = comptime core.gpu_model.renderModule(Source, lang, "Tile contracts");
        try std.testing.expect(std.mem.indexOf(u8, source, "antfly_tile_round_bf16") != null);
        try std.testing.expect(std.mem.indexOf(u8, source, "contract_gather") != null);
        try std.testing.expect(std.mem.indexOf(u8, source, "contract_rms") != null);
    }
}

test "vector scoring matches f64 for every SIMD boundary" {
    var prng = std.Random.DefaultPrng.init(752);
    var x: [257]f32 = undefined;
    var y: [257]f32 = undefined;
    for (&x, &y) |*a, *b| {
        a.* = prng.random().float(f32) - 0.5;
        b.* = prng.random().float(f32) - 0.5;
    }
    for (0..258) |n| {
        var ref: f64 = 0;
        for (x[0..n], y[0..n]) |a, b| ref += @as(f64, a) * b;
        var scores: [2]f32 = undefined;
        core.scoring.batchDot(x[0..n], &.{ x[0..n], y[0..n] }, &scores);
        try std.testing.expectApproxEqAbs(@as(f32, @floatCast(ref)), scores[1], 2e-6);
    }
}

test "GPU layout preparation checks flattened overflow, ids and the 64-bit ABI" {
    const view: core.layout.Rows = .{ .rows = 4, .cols = 3, .row_stride = 3 };
    const launch = try core.layout.prepareCopy(.{ .name = "gather", .gather = true }, view, 12, .{ .rows = 2, .cols = 3, .row_stride = 4, .offset = 1 }, 8, &.{ 3, 0 });
    try std.testing.expectEqual(@as(usize, 1), launch.groups);
    try std.testing.expectEqual(@as(usize, 64), @sizeOf(core.layout.CopyParams));
    try std.testing.expectEqual(@as(u64, 4), launch.params.dst_row_stride);
    try std.testing.expectError(error.InvalidLayout, core.layout.prepareCopy(.{ .name = "gather", .gather = true }, view, 12, view, 12, &.{ 0, 1, 2, 4 }));
    const overflow: core.layout.Rows = .{ .rows = std.math.maxInt(usize), .cols = 2, .row_stride = 0, .col_stride = 0 };
    try std.testing.expectError(error.InvalidLayout, core.layout.prepareCopy(.{ .name = "copy" }, overflow, 1, overflow, 1, null));
}

test "GEMM gate/up input and residual are independent in the CPU lowering" {
    const g = core.conformance.Programs.projection;
    const x = [_]f32{ 1, 2, 3, 4, 5, 6 };
    const w = [_]f32{ 1, 0, 0, 0, 1, 0 };
    const bias = [_]f32{ 0.125, -0.125 };
    const gate = [_]f32{ 0.5, 0.75, 1, -0.5 };
    const residual = [_]f32{ -0.25, 0.25, 0.125, -0.125 };
    var got: [4]f32 = undefined;
    var ref: [4]f32 = undefined;
    const args: core.gemm.Args = .{ .x = &x, .w = &w, .bias = &bias, .gate = &gate, .residual = &residual, .out = &got, .m = 2, .n = 2, .k = 3 };
    var oracle = args;
    oracle.out = &ref;
    core.gemm.reference(g, oracle);
    core.gemm.run(g, args);
    for (got, ref) |a, b| try std.testing.expectApproxEqAbs(b, a, 0.008);
}

test {
    _ = @import("native_math.zig");
}

test "normalization profiles preserve affine association, explicit FMA and algorithm ownership" {
    const f = core.fexpr;
    const input = .{ .value = @as(f32, 1e20), .inverse = @as(f32, 1e20), .weight = @as(f32, 1e-20) };
    try std.testing.expect(std.math.isInf((comptime core.native_math.rms(.scale_then_weight)).evaluate(struct {}, input)));
    try std.testing.expect(std.math.isFinite((comptime core.native_math.rms(.weighted_inverse)).evaluate(struct {}, input)));
    const fused = comptime f.acc.mulAdd(.bias, .residual);
    const operands: f.Inputs(f32) = .{ .value = 1.0000001, .bias = 0.9999999, .residual = -1, .gate = 0 };
    try std.testing.expectEqual(@as(u32, @bitCast(@mulAdd(f32, operands.value, operands.bias, operands.residual))), @as(u32, @bitCast(fused.evaluate(struct {}, operands))));
    const serial = comptime core.native_normalization.metalLayer(.{ .name = "serial", .statistics = .centered_two_pass }, "Params", "family");
    const rows = comptime core.native_normalization.metalLayer(.{ .name = "rows", .statistics = .raw_moments_tree }, "Params", "family");
    try std.testing.expect(std.mem.indexOf(u8, serial, "family<0u, false, false>") != null);
    try std.testing.expect(std.mem.indexOf(u8, rows, "family<0u, false, true>") != null);
    const split: core.layout.InterleavedRows = .{ .parts = 3 };
    try std.testing.expectEqual(@as(usize, 21), (try split.view(2, 7, 1)).row_stride);
    try std.testing.expectEqual(@as(usize, 7), (try split.view(2, 7, 1)).offset);
    try std.testing.expectError(error.InvalidLayout, split.view(2, 7, 3));
}

test "native literal emission preserves negative zero" {
    const e = comptime core.fexpr.lit(-0.0);
    const source = comptime e.renderNative(.{}, .{ .value = "", .bias = "", .gate = "", .residual = "" }, null);
    try std.testing.expectEqualStrings("-0.0f", source);
}

test "native unary emission separates nested minus tokens" {
    const e = comptime core.fexpr.acc.negated().negated();
    try std.testing.expectEqualStrings("-(-x)", comptime e.renderNative(.{}, .{ .value = "x", .bias = "", .gate = "", .residual = "" }, null));
}

test "default vector math evaluates primitive tanh" {
    const input: @Vector(4, f32) = .{ -3, -0.0, 0.25, 5 };
    const e = comptime core.fexpr.acc.tanhOf();
    const result = e.evalVec(4, input, @splat(0), @splat(0));
    inline for (0..4) |i| try std.testing.expectEqual(@as(u32, @bitCast(std.math.tanh(input[i]))), @as(u32, @bitCast(result[i])));
}

test "named adjoints preserve the original ordered activation formula and subtraction" {
    const f = core.fexpr;
    for (0..4097) |i| {
        const x: f32 = (@as(f32, @floatFromInt(i)) - 2048) / 256;
        const x2 = x * x;
        const t = std.math.tanh(@as(f32, 0.7978845608028654) * (x + @as(f32, 0.044715) * x * x2));
        const sech2 = 1 - t * t;
        const expected = @as(f32, 0.5) * (1 + t) + @as(f32, 0.5) * x * sech2 * @as(f32, 0.7978845608028654) * (1 + @as(f32, 0.134145) * x2);
        const actual = (comptime core.native_math.geluDerivative(f.operand("sech2"))).evaluate(struct {}, .{ .value = x, .t = t, .x2 = x2, .sech2 = sech2 });
        try std.testing.expectEqual(@as(u32, @bitCast(expected)), @as(u32, @bitCast(actual)));
    }
    const e = comptime f.operand("left").minus(f.operand("right").minus(f.operand("tail")));
    try std.testing.expectEqualStrings("a - (b - c)", comptime e.renderNative(.{}, .{ .left = "a", .right = "b", .tail = "c" }, null));
}

test "relative row launch contract covers query tails and rejects overflow" {
    const S = core.native_attention.RelativeRow64;
    try std.testing.expectEqual(@as(usize, 8), try S.blocks(2, 9, 2));
    try std.testing.expectEqual(@as(usize, 132), try S.blocks(2, 257, 2));
    try std.testing.expect(!S.supports(1, 513, 2, 64));
    try std.testing.expect(!S.supports(1, 7, 2, 32));
    try std.testing.expectError(error.InvalidLayout, S.blocks(std.math.maxInt(usize), 512, 2));
    try std.testing.expectError(error.InvalidLayout, S.blocks(1, 0, 1));
}

test "BF16 softmax schedule admission retains unqualified device baselines" {
    const schedule = core.native_statistics.Bf16ScoreSoftmax;
    // The serving optimization is physically qualified on GB10. In particular,
    // A100 and L4 keep the original shared-tree occupancy/register contract.
    for ([_][2]i32{ .{ 7, 5 }, .{ 8, 0 }, .{ 8, 9 }, .{ 9, 0 }, .{ 12, 0 }, .{ 12, 2 } }) |device| {
        for ([_]usize{ 1, 31, 270, 511, 512, 513, 8192 }) |columns| {
            try std.testing.expect(!schedule.supportsNarrow(columns, device[0], device[1]));
        }
    }
    for ([_]usize{ 1, 31, 270, 511, 512 }) |columns| try std.testing.expect(schedule.supportsNarrow(columns, 12, 1));
    for ([_]usize{ 0, 513, 8192, std.math.maxInt(usize) }) |columns| try std.testing.expect(!schedule.supportsNarrow(columns, 12, 1));
}

// Check ordered optimizer expressions against independently written F32
// formulas across small/large gradients, zero decay and distinct beta values.
test "shared AdamW stages retain moment and decay association" {
    const AdamW = core.native_math.AdamW;
    inline for (.{ @as(f32, 0), @as(f32, 0.9), @as(f32, 0.999) }) |beta| {
        for (0..2048) |i| {
            const g: f32 = @as(f32, @floatFromInt(i)) / 31 - 33;
            const m: f32 = g / 11;
            const v: f32 = @abs(g) + 0.001;
            const got_m = AdamW.first.evaluate(struct {}, .{ .value = g, .beta1 = beta, .m = m, .g = g });
            const got_v = AdamW.second.evaluate(struct {}, .{ .value = g, .beta2 = beta, .v = v, .g = g });
            const want_m: f32 = beta * m + (1 - beta) * g;
            const want_v: f32 = beta * v + (1 - beta) * g * g;
            try std.testing.expectEqual(@as(u32, @bitCast(want_m)), @as(u32, @bitCast(got_m)));
            try std.testing.expectEqual(@as(u32, @bitCast(want_v)), @as(u32, @bitCast(got_v)));
            const root = @sqrt(got_v / 0.01);
            const mh = got_m / 0.1;
            const got_update = AdamW.update.evaluate(struct {}, .{ .value = g, .m_hat = mh, .root_v = root, .eps = @as(f32, 1e-8), .decay = beta, .w = m });
            const want_update: f32 = mh / (root + @as(f32, 1e-8)) + beta * m;
            try std.testing.expectEqual(@as(u32, @bitCast(want_update)), @as(u32, @bitCast(got_update)));
        }
    }
}
