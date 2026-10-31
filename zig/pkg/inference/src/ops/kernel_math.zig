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

//! Shared scalar mathematics; CPU evaluation expands at compilation and GPU
//! lowering happens during source generation. Schedules retain vector widths,
//! transcendental implementations, finite guards and floating-point order.
const std = @import("std");
pub const Activation = enum { gelu, sigmoid, silu, quick_gelu, relu, relu_squared };
pub const CubicOrder = enum { left_associated, cube_first };
const Id = u8;
const Pair = struct { a: Id, b: Id };
const Node = union(enum) {
    input,
    constant: f32,
    add: Pair,
    multiply: Pair,
    divide: Pair,
    negate: Id,
    exponential: Id,
    hyperbolic_tangent: Id,
    maximum: Pair,
};
pub const Recipe = struct {
    nodes: [24]Node = undefined,
    count: Id = 0,
    result: Id = 0,
    gelu_inner: ?Id = null,
    fn put(r: *Recipe, node: Node) Id {
        const id = r.count;
        r.nodes[id] = node;
        r.count += 1;
        return id;
    }
    fn number(r: *Recipe, value: f32) Id {
        return r.put(.{ .constant = value });
    }
    fn binary(r: *Recipe, comptime tag: std.meta.Tag(Node), a: Id, b: Id) Id {
        return r.put(@unionInit(Node, @tagName(tag), Pair{ .a = a, .b = b }));
    }
};
pub fn activation(kind: Activation, order: CubicOrder) Recipe {
    var r = Recipe{};
    const x = r.put(.input);
    r.result = switch (kind) {
        .gelu => blk: {
            const c = r.number(0.044715);
            const cubic = switch (order) {
                .left_associated => r.binary(.multiply, r.binary(.multiply, r.binary(.multiply, c, x), x), x),
                .cube_first => r.binary(.multiply, c, r.binary(.multiply, r.binary(.multiply, x, x), x)),
            };
            const inner = r.binary(.multiply, r.number(0.7978845608028654), r.binary(.add, x, cubic));
            r.gelu_inner = inner;
            break :blk r.binary(.multiply, r.binary(.multiply, r.number(0.5), x), r.binary(.add, r.number(1), r.put(.{ .hyperbolic_tangent = inner })));
        },
        .sigmoid, .silu, .quick_gelu => blk: {
            const negative = if (kind == .quick_gelu) r.binary(.multiply, r.number(-1.702), x) else r.put(.{ .negate = x });
            const denominator = r.binary(.add, r.number(1), r.put(.{ .exponential = negative }));
            break :blk r.binary(.divide, if (kind == .sigmoid) r.number(1) else x, denominator);
        },
        .relu, .relu_squared => blk: {
            const positive = r.binary(.maximum, x, r.number(0));
            break :blk if (kind == .relu_squared) r.binary(.multiply, positive, positive) else positive;
        },
    };
    return r;
}
/// Math supplies the selected scalar or SIMD exp/tanh implementation. There
/// is no runtime interpreter, node dispatch, allocation or recipe storage.
pub inline fn evaluate(comptime recipe: Recipe, comptime Math: type, input: anytype) @TypeOf(input) {
    const T = @TypeOf(input);
    var values: [recipe.count]T = undefined;
    inline for (recipe.nodes[0..recipe.count], 0..) |node, index| {
        values[index] = switch (node) {
            .input => input,
            .constant => |v| if (@typeInfo(T) == .vector) @splat(v) else v,
            .add => |p| values[p.a] + values[p.b],
            .multiply => |p| values[p.a] * values[p.b],
            .divide => |p| values[p.a] / values[p.b],
            .negate => |p| -values[p],
            .exponential => |p| Math.exp(values[p]),
            .hyperbolic_tangent => |p| Math.tanh(values[p]),
            .maximum => |p| @max(values[p.a], values[p.b]),
        };
    }
    return values[recipe.result];
}
pub const ScalarMath = struct {
    pub inline fn exp(x: f32) f32 {
        return @exp(x);
    }
    pub inline fn tanh(x: f32) f32 {
        return std.math.tanh(x);
    }
};
pub const Source = struct { tanh: []const u8, exp: []const u8, maximum: []const u8 };
pub const metal = Source{ .tanh = "tanh", .exp = "exp", .maximum = "max" };
pub const cuda = Source{ .tanh = "tanhf", .exp = "expf", .maximum = "fmaxf" };
pub const cuda_fast = Source{ .tanh = "tanhf", .exp = "__expf", .maximum = "fmaxf" };

/// Explicit parentheses preserve the arithmetic tree, including the distinct
/// scalar and SIMD GELU cubic orders. Contraction policy remains target-owned.
pub fn expression(comptime r: Recipe, comptime source: Source, comptime input: []const u8) []const u8 {
    @setEvalBranchQuota(100000);
    return expressionAt(r, r.result, source, input, null);
}
fn precedence(node: Node) u8 {
    return switch (node) {
        .add => 1,
        .multiply, .divide => 2,
        .negate => 3,
        else => 4,
    };
}
fn operand(comptime r: Recipe, comptime id: Id, comptime parent: Node, comptime right: bool, comptime source: Source, comptime input: []const u8, comptime substitute_inner: ?Id) []const u8 {
    const text = expressionAt(r, id, source, input, substitute_inner);
    // Equal-precedence right operands require parentheses: floating arithmetic
    // is not associative, including the native cube-first GELU schedule.
    return if (precedence(r.nodes[id]) < precedence(parent) or (right and precedence(r.nodes[id]) == precedence(parent))) "(" ++ text ++ ")" else text;
}
fn expressionAt(comptime r: Recipe, comptime id: Id, comptime source: Source, comptime input: []const u8, comptime substitute_inner: ?Id) []const u8 {
    if (substitute_inner == id) return "inner";
    return switch (r.nodes[id]) {
        .input => input,
        .constant => |v| if (v == 0) "0.0f" else if (v == 1) "1.0f" else if (v == 0.5) "0.5f" else if (v == 0.044715) "0.044715f" else if (v == 0.7978845608028654) "0.7978845608028654f" else if (v == -1.702) "-1.702f" else unreachable,
        .add, .multiply, .divide => |p| std.fmt.comptimePrint("{s} {s} {s}", .{ operand(r, p.a, r.nodes[id], false, source, input, substitute_inner), switch (r.nodes[id]) {
            .add => "+",
            .multiply => "*",
            .divide => "/",
            else => unreachable,
        }, operand(r, p.b, r.nodes[id], true, source, input, substitute_inner) }),
        .negate => |p| "-" ++ operand(r, p, r.nodes[id], false, source, input, substitute_inner),
        .exponential => |p| std.fmt.comptimePrint("{s}({s})", .{ source.exp, expressionAt(r, p, source, input, substitute_inner) }),
        .hyperbolic_tangent => |p| std.fmt.comptimePrint("{s}({s})", .{ source.tanh, expressionAt(r, p, source, input, substitute_inner) }),
        .maximum => |p| std.fmt.comptimePrint("{s}({s}, {s})", .{ source.maximum, expressionAt(r, p.a, source, input, substitute_inner), expressionAt(r, p.b, source, input, substitute_inner) }),
    };
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
    const inner = comptime expressionAt(r, r.gelu_inner.?, source, "x", null);
    const result = comptime expressionAt(r, r.result, source, "x", r.gelu_inner);
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
    const inner = comptime expressionAt(gelu, gelu.gelu_inner.?, source, "x", null);
    const output = comptime expressionAt(gelu, gelu.result, source, "x", gelu.gelu_inner);
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
