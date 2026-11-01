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

//! Native activation profiles over the common Tile expression representation.
const std = @import("std");
const f = @import("fexpr.zig");
pub const Activation = enum { gelu, sigmoid, silu, quick_gelu, relu, relu_squared };
pub const CubicOrder = enum { left_associated, cube_first };
pub const Recipe = struct { expr: f.FExpr, gelu_inner: ?f.FExpr = null };
pub fn activation(comptime kind: Activation, comptime order: CubicOrder) Recipe {
    const x = f.acc;
    const positive = comptime x.maxWith(f.lit(0));
    const inner = comptime f.lit(0.7978845608028654).times(x.plus(if (order == .cube_first) f.lit(0.044715).times(x.times(x).times(x)) else f.lit(0.044715).times(x).times(x).times(x)));
    return .{ .expr = switch (kind) {
        .gelu => f.lit(0.5).times(x).times(f.lit(1).plus(inner.tanhOf())),
        .silu => x.siluOf(),
        .sigmoid => f.lit(1).dividedBy(f.lit(1).plus(x.negated().expOf())),
        .quick_gelu => x.dividedBy(f.lit(1).plus(f.lit(-1.702).times(x).expOf())),
        .relu => positive,
        .relu_squared => positive.times(positive),
    }, .gelu_inner = if (kind == .gelu) inner else null };
}
pub inline fn evaluate(comptime recipe: Recipe, comptime Math: type, input: anytype) @TypeOf(input) {
    const T = @TypeOf(input);
    const zero: T = if (@typeInfo(T) == .vector) @splat(0) else 0;
    return recipe.expr.evaluate(Math, f.Inputs(T){ .value = input, .bias = zero, .gate = zero, .residual = zero });
}
pub const ScalarMath = struct {
    pub inline fn exp(x: f32) f32 {
        return @exp(x);
    }
    pub inline fn tanh(x: f32) f32 {
        return std.math.tanh(x);
    }
};
pub const Source = f.FExpr.Source;
pub const metal = Source{ .tanh = "tanh", .exp = "exp", .maximum = "max" };
pub const cuda = Source{ .tanh = "tanhf", .exp = "expf", .maximum = "fmaxf", .fma = "fmaf" };
pub const cuda_fast = Source{ .tanh = "tanhf", .exp = "__expf", .maximum = "fmaxf", .fma = "fmaf" };
fn names(comptime input: []const u8) f.Inputs([]const u8) {
    return .{ .value = input, .bias = "", .gate = "", .residual = "" };
}
pub fn expression(comptime r: Recipe, comptime source: Source, comptime input: []const u8) []const u8 {
    @setEvalBranchQuota(100_000);
    return r.expr.renderNative(source, names(input), null);
}

test "kernel math recipes preserve scalar and vector operation order against independent formulas" {
    const Math = struct {
        pub fn exp(x: @Vector(4, f32)) @Vector(4, f32) {
            return @exp(x);
        }
        pub fn tanh(x: @Vector(4, f32)) @Vector(4, f32) {
            var result: @Vector(4, f32) = undefined;
            inline for (0..4) |i| result[i] = std.math.tanh(x[i]);
            return result;
        }
    };
    inline for (comptime std.meta.tags(Activation)) |kind| {
        inline for (comptime std.meta.tags(CubicOrder)) |order| {
            const r = comptime activation(kind, order);
            for (0..1024) |index| {
                const x: f32 = @as(f32, @floatFromInt(index)) / 32 - 16;
                const cubic = if (order == .cube_first) @as(f32, 0.044715) * ((x * x) * x) else ((@as(f32, 0.044715) * x) * x) * x;
                const positive = @max(x, 0);
                const want: f32 = switch (kind) {
                    .gelu => (0.5 * x) * (1 + std.math.tanh(@as(f32, 0.7978845608028654) * (x + cubic))),
                    .sigmoid => 1 / (1 + @exp(-x)),
                    .silu => x / (1 + @exp(-x)),
                    .quick_gelu => x / (1 + @exp(-1.702 * x)),
                    .relu => positive,
                    .relu_squared => positive * positive,
                };
                try std.testing.expectEqual(@as(u32, @bitCast(want)), @as(u32, @bitCast(evaluate(r, ScalarMath, x))));
                const result = evaluate(r, Math, @as(@Vector(4, f32), @splat(x)));
                inline for (0..4) |lane| try std.testing.expectApproxEqAbs(want, result[lane], 2e-6);
            }
            inline for (.{ metal, cuda, cuda_fast }) |source| {
                const text = comptime expression(r, source, "x");
                try std.testing.expect(text.len > 0);
            }
        }
    }
}

/// Guarded GELU is an explicit numerical policy used by the existing packed
/// projection epilogues. It deliberately differs from ordinary activations.
pub fn guardedGeluSource(comptime source: Source, comptime qualifier: []const u8, comptime name: []const u8, comptime single_line: bool) []const u8 {
    @setEvalBranchQuota(100000);
    const r = comptime activation(.gelu, .left_associated);
    const inner = comptime r.gelu_inner.?.renderNative(source, names("x"), null);
    const result = comptime r.expr.renderNative(source, names("x"), .{ .expr = r.gelu_inner.?, .text = "inner" });
    const gap = if (single_line) " " else "\n    ";
    return qualifier ++ " float " ++ name ++ "(float x) {" ++ gap ++
        "if (!isfinite(x)) return 0.0f;" ++ gap ++
        (if (single_line) "float inner = " else "const float inner = ") ++ inner ++ ";" ++ gap ++
        "if (inner > 10.0f) return x;" ++ gap ++
        "if (inner < -10.0f) return 0.0f;" ++ gap ++
        (if (single_line) "float y = " else "const float y = ") ++ result ++ ";" ++ gap ++
        "return isfinite(y) ? y : 0.0f;" ++ (if (single_line) " }" else "\n}");
}

/// Decoder ABI: GELU=0/1, SiLU=2, ReLU=3, QuickGELU=4, and the legacy default
/// ReLU-squared. The target's exp/fast-exp choice remains part of the lowering.
pub fn decoderActivationSource(comptime source: Source, comptime qualifier: []const u8, comptime name: []const u8) []const u8 {
    @setEvalBranchQuota(100000);
    const gelu = comptime activation(.gelu, .left_associated);
    const inner = comptime gelu.gelu_inner.?.renderNative(source, names("x"), null);
    const output = comptime gelu.expr.renderNative(source, names("x"), .{ .expr = gelu.gelu_inner.?, .text = "inner" });
    const fast = comptime std.mem.eql(u8, source.exp, "__expf");
    const gelu_branch = if (fast)
        "    if (activation <= 1u) {\n        const float inner = " ++ inner ++ ";\n        return " ++ output ++ ";\n    }\n"
    else
        "    if (activation <= 1u) return " ++ expression(gelu, source, "x") ++ ";\n";
    return qualifier ++ " float " ++ name ++ "(float x, unsigned int activation) {\n" ++ gelu_branch ++
        "    if (activation == 2u) return " ++ expression(activation(.silu, .left_associated), source, "x") ++ ";\n" ++
        "    if (activation == 3u) return " ++ expression(activation(.relu, .left_associated), source, "x") ++ ";\n" ++
        "    if (activation == 4u) return " ++ expression(activation(.quick_gelu, .left_associated), source, "x") ++ ";\n" ++
        "    const float r = " ++ expression(activation(.relu, .left_associated), source, "x") ++ ";\n" ++
        "    return r * r;\n}";
}

/// Statistics algorithms are separate contracts: none may substitute another
/// solely because the final affine expression is shared.
pub const Statistics = enum { rms_uncentered, centered_two_pass, raw_moments_tree, welford };
pub const RmsOrder = enum { scale_then_weight, weighted_inverse };
pub fn rms(comptime order: RmsOrder) f.FExpr {
    return if (order == .scale_then_weight) f.acc.times(.gate).times(.bias) else f.acc.times((@as(f.FExpr, .gate)).times(.bias));
}
pub const layer_affine = f.acc.times(.gate).times(.bias).plus(.residual);

pub fn cudaExpression(comptime expr: f.FExpr, comptime value: []const u8, comptime bias: []const u8, comptime gate: []const u8, comptime residual: []const u8) f.Emission {
    return .{ .expr = expr, .source = cuda, .inputs = .{ .value = value, .bias = bias, .gate = gate, .residual = residual } };
}
