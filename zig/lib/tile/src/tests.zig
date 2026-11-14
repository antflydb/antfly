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
