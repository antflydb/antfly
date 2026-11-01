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

//! Float epilogue expressions: what a kernel does with each computed value
//! before storing it. `acc` is the value (a GEMM dot product), `bias` is
//! indexed by output column, `residual` by output element.
//!
//! Example: `fexpr.acc.plus(.bias).geluOf().plus(.residual)`.

const std = @import("std");
const vmath = @import("vmath.zig");

/// Named operands let native schedules feed registers or buffer loads into
/// the same expression without conflating a gate/up value and skip residual.
pub fn Inputs(comptime T: type) type {
    return struct { value: T, bias: T, gate: T, residual: T };
}

pub const FExpr = union(enum) {
    acc,
    bias,
    /// Elementwise gate/up-projection input, independent of the skip residual.
    gate,
    residual,
    lit: f32,
    add: [2]*const FExpr,
    mul: [2]*const FExpr,
    div: [2]*const FExpr,
    maximum: [2]*const FExpr,
    fma: [3]*const FExpr,
    neg: *const FExpr,
    exp: *const FExpr,
    tanh: *const FExpr,
    gelu: *const FExpr,
    silu: *const FExpr,
    round_f16: *const FExpr,
    round_bf16: *const FExpr,

    fn box(comptime e: FExpr) *const FExpr {
        return &struct {
            const value = e;
        }.value;
    }

    pub fn plus(comptime a: FExpr, comptime b: FExpr) FExpr {
        return .{ .add = .{ box(a), box(b) } };
    }
    pub fn times(comptime a: FExpr, comptime b: FExpr) FExpr {
        return .{ .mul = .{ box(a), box(b) } };
    }
    pub fn mulAdd(comptime a: FExpr, comptime b: FExpr, comptime c: FExpr) FExpr {
        return .{ .fma = .{ box(a), box(b), box(c) } };
    }
    pub fn dividedBy(comptime a: FExpr, comptime b: FExpr) FExpr {
        return .{ .div = .{ box(a), box(b) } };
    }
    pub fn maxWith(comptime a: FExpr, comptime b: FExpr) FExpr {
        return .{ .maximum = .{ box(a), box(b) } };
    }
    pub fn negated(comptime a: FExpr) FExpr {
        return .{ .neg = box(a) };
    }
    pub fn expOf(comptime a: FExpr) FExpr {
        return .{ .exp = box(a) };
    }
    pub fn tanhOf(comptime a: FExpr) FExpr {
        return .{ .tanh = box(a) };
    }
    /// A semantic activation lowers to the same arithmetic for CPU evaluation
    /// and native source emission. Target helpers may retain qualified math.
    pub fn expanded(comptime e: FExpr) FExpr {
        return switch (e) {
            .silu => |x| x.*.dividedBy(lit(1).plus(x.*.negated().expOf())),
            else => e,
        };
    }

    pub fn geluOf(comptime a: FExpr) FExpr {
        return .{ .gelu = box(a) };
    }

    pub fn siluOf(comptime a: FExpr) FExpr {
        return .{ .silu = box(a) };
    }
    /// Explicit storage rounding boundaries, including intermediates between
    /// bias/activation/gating/residual stages. All other arithmetic is f32.
    pub fn roundF16(comptime a: FExpr) FExpr {
        return .{ .round_f16 = box(a) };
    }
    pub fn roundBf16(comptime a: FExpr) FExpr {
        return .{ .round_bf16 = box(a) };
    }

    pub fn uses(comptime e: FExpr, comptime tag: std.meta.Tag(FExpr)) bool {
        return switch (e) {
            .acc, .bias, .gate, .residual, .lit => e == tag,
            .add, .mul, .div, .maximum => |xs| e == tag or xs[0].uses(tag) or xs[1].uses(tag),
            .fma => |x| e == tag or x[0].uses(tag) or x[1].uses(tag) or x[2].uses(tag),
            .gelu, .silu, .round_f16, .round_bf16, .neg, .exp, .tanh => |x| e == tag or x.uses(tag),
        };
    }

    /// f64 reference evaluation.
    pub fn evalF64(comptime e: FExpr, value: f64, bias: f64, res: f64) f64 {
        if (comptime e.uses(.gate)) @compileError("use evalF64Inputs to supply a separate gate");
        return e.evalF64Inputs(.{ .value = value, .bias = bias, .gate = 0, .residual = res });
    }
    pub fn evalF64Inputs(comptime e: FExpr, inputs: Inputs(f64)) f64 {
        return e.evaluate(ReferenceMath, inputs);
    }

    pub fn evalVec(comptime e: FExpr, comptime n: comptime_int, value: @Vector(n, f32), bias: @Vector(n, f32), res: @Vector(n, f32)) @Vector(n, f32) {
        return e.evalVecWith(n, DefaultMath, value, bias, res);
    }

    /// The consumer supplies exp/GELU implementations so a native schedule can
    /// retain its existing approximation and contraction policy.
    pub fn evalVecWith(comptime e: FExpr, comptime n: comptime_int, comptime Math: type, value: @Vector(n, f32), bias: @Vector(n, f32), res: @Vector(n, f32)) @Vector(n, f32) {
        if (comptime e.uses(.gate)) @compileError("use evalVecInputs to supply a separate gate");
        return e.evalVecInputs(n, Math, .{ .value = value, .bias = bias, .gate = @splat(0), .residual = res });
    }
    pub fn evalVecInputs(comptime e: FExpr, comptime n: comptime_int, comptime Math: type, inputs: Inputs(@Vector(n, f32))) @Vector(n, f32) {
        const Adapter = struct {
            pub fn exp(x: @Vector(n, f32)) @Vector(n, f32) {
                return Math.exp(n, x);
            }
            pub fn gelu(x: @Vector(n, f32)) @Vector(n, f32) {
                return Math.gelu(n, x);
            }
            pub fn tanh(x: @Vector(n, f32)) @Vector(n, f32) {
                return Math.tanh(n, x);
            }
        };
        return e.evaluate(Adapter, inputs);
    }

    /// One arithmetic interpreter expanded at comptime for scalars and SIMD.
    /// The calling schedule owns transcendental approximations and contraction.
    pub inline fn evaluate(comptime e: FExpr, comptime Math: type, inputs: anytype) @TypeOf(inputs.value) {
        const T = @TypeOf(inputs.value);
        return switch (e) {
            .acc => inputs.value,
            .bias => inputs.bias,
            .gate => inputs.gate,
            .residual => inputs.residual,
            .lit => |v| if (@typeInfo(T) == .vector) @splat(v) else v,
            .add => |x| x[0].evaluate(Math, inputs) + x[1].evaluate(Math, inputs),
            .mul => |x| x[0].evaluate(Math, inputs) * x[1].evaluate(Math, inputs),
            .div => |x| x[0].evaluate(Math, inputs) / x[1].evaluate(Math, inputs),
            .fma => |x| @mulAdd(T, x[0].evaluate(Math, inputs), x[1].evaluate(Math, inputs), x[2].evaluate(Math, inputs)),
            .maximum => |x| @max(x[0].evaluate(Math, inputs), x[1].evaluate(Math, inputs)),
            .neg => |x| -x.evaluate(Math, inputs),
            .exp => |x| Math.exp(x.evaluate(Math, inputs)),
            .tanh => |x| Math.tanh(x.evaluate(Math, inputs)),
            .gelu => |x| Math.gelu(x.evaluate(Math, inputs)),
            .silu => blk: {
                const primitive = comptime e.expanded();
                break :blk primitive.evaluate(Math, inputs);
            },
            .round_f16 => |x| @floatCast(@as(if (@typeInfo(T) == .vector) @Vector(@typeInfo(T).vector.len, f16) else f16, @floatCast(x.evaluate(Math, inputs)))),
            .round_bf16 => |x| blk: {
                const v = x.evaluate(Math, inputs);
                if (@typeInfo(T) != .vector) break :blk @as(T, roundBf16Value(@floatCast(v)));
                var out: T = undefined;
                inline for (0..@typeInfo(T).vector.len) |i| out[i] = roundBf16Value(v[i]);
                break :blk out;
            },
        };
    }

    /// C source over the given variable names.
    pub fn render(comptime e: FExpr, comptime value: []const u8, comptime bias: []const u8, comptime res: []const u8) []const u8 {
        return e.renderWith(.{}, value, bias, res);
    }

    /// GPU schedules supply their qualified activation and rounding helpers.
    /// The expression tree never chooses a GEMM or transcendental schedule.
    pub const Source = struct {
        fma: []const u8 = "fma",
        exp: []const u8 = "exp",
        tanh: []const u8 = "tanh",
        maximum: []const u8 = "max",
        native_syntax: bool = false,
        compact: bool = false,
        expand_silu: bool = true,
        gelu: []const u8 = "antfly_tile_gelu",
        silu: []const u8 = "antfly_tile_silu",
        round_f16: []const u8 = "antfly_tile_round_f16",
        round_bf16: []const u8 = "antfly_tile_round_bf16",
    };
    pub fn renderWith(comptime e: FExpr, comptime source: Source, comptime value: []const u8, comptime bias: []const u8, comptime res: []const u8) []const u8 {
        if (comptime e.uses(.gate)) @compileError("use renderInputs to supply a separate gate");
        return e.renderInputs(source, .{ .value = value, .bias = bias, .gate = "", .residual = res });
    }
    pub fn renderInputs(comptime e: FExpr, comptime source: Source, comptime inputs: Inputs([]const u8)) []const u8 {
        return e.renderAt(source, inputs, null, false, null);
    }
    pub const Substitute = struct { expr: FExpr, text: []const u8 };
    pub fn renderNative(comptime e: FExpr, comptime source: Source, comptime inputs: Inputs([]const u8), comptime substitute: ?Substitute) []const u8 {
        var native = source;
        native.native_syntax = true;
        return e.renderAt(native, inputs, null, false, substitute);
    }
    fn precedence(comptime e: FExpr) u8 {
        return switch (e) {
            .add => 1,
            .mul, .div => 2,
            .neg => 3,
            else => 4,
        };
    }
    fn renderAt(comptime e: FExpr, comptime source: Source, comptime inputs: Inputs([]const u8), comptime parent: ?FExpr, comptime right: bool, comptime sub: ?Substitute) []const u8 {
        @setEvalBranchQuota(100_000);
        if (sub) |replacement| if (std.meta.eql(e, replacement.expr)) return replacement.text;
        if (source.native_syntax and source.expand_silu and e == .silu) return e.expanded().renderAt(source, inputs, parent, right, sub);
        const text = switch (e) {
            .acc => inputs.value,
            .bias => inputs.bias,
            .gate => inputs.gate,
            .residual => inputs.residual,
            .lit => |v| if (source.native_syntax) nativeLiteral(v) else std.fmt.comptimePrint("{e}f", .{v}),
            .add, .mul, .div => |x| blk: {
                const body = x[0].renderAt(source, inputs, e, false, sub) ++ (if (source.compact) (if (e == .add) "+" else if (e == .mul) "*" else "/") else (if (e == .add) " + " else if (e == .mul) " * " else " / ")) ++ x[1].renderAt(source, inputs, e, true, sub);
                break :blk if (source.native_syntax) body else "(" ++ body ++ ")";
            },
            .fma => |x| source.fma ++ "(" ++ x[0].renderAt(source, inputs, null, false, sub) ++ ", " ++ x[1].renderAt(source, inputs, null, false, sub) ++ ", " ++ x[2].renderAt(source, inputs, null, false, sub) ++ ")",
            .maximum => |x| source.maximum ++ "(" ++ x[0].renderAt(source, inputs, null, false, sub) ++ ", " ++ x[1].renderAt(source, inputs, null, false, sub) ++ ")",
            .neg => |x| blk: {
                const child = x.renderAt(source, inputs, e, false, sub);
                break :blk if (std.mem.startsWith(u8, child, "-")) "-(" ++ child ++ ")" else "-" ++ child;
            },
            .exp => |x| source.exp ++ "(" ++ x.renderAt(source, inputs, null, false, sub) ++ ")",
            .tanh => |x| source.tanh ++ "(" ++ x.renderAt(source, inputs, null, false, sub) ++ ")",
            .gelu => |x| source.gelu ++ "(" ++ x.renderAt(source, inputs, null, false, sub) ++ ")",
            .silu => |x| source.silu ++ "(" ++ x.renderAt(source, inputs, null, false, sub) ++ ")",
            .round_f16 => |x| source.round_f16 ++ "(" ++ x.renderAt(source, inputs, null, false, sub) ++ ")",
            .round_bf16 => |x| source.round_bf16 ++ "(" ++ x.renderAt(source, inputs, null, false, sub) ++ ")",
        };
        return if (source.native_syntax and parent != null and (e.precedence() < parent.?.precedence() or (right and e.precedence() == parent.?.precedence()))) "(" ++ text ++ ")" else text;
    }
};

pub const acc: FExpr = .acc;
pub fn lit(comptime v: f32) FExpr {
    return .{ .lit = v };
}

pub const DefaultMath = struct {
    pub const exp = vmath.vexp;
    pub const gelu = vmath.vgelu;
    pub fn tanh(comptime n: comptime_int, x: @Vector(n, f32)) @Vector(n, f32) {
        var result: @Vector(n, f32) = undefined;
        inline for (0..n) |i| result[i] = std.math.tanh(x[i]);
        return result;
    }
};
/// IEEE bfloat16 round-to-nearest-even, preserving infinities and quieting NaNs
/// rather than allowing discarded payload bits to turn a NaN into infinity.
pub fn roundBf16Value(value: f32) f32 {
    const bits: u32 = @bitCast(value);
    if (bits & 0x7fffffff > 0x7f800000) return @bitCast((bits & 0xffff0000) | 0x00400000);
    return @bitCast((bits +% 0x7fff +% ((bits >> 16) & 1)) & 0xffff0000);
}

const ReferenceMath = struct {
    pub const gelu = vmath.geluF64;
    pub fn exp(x: f64) f64 {
        return @exp(x);
    }
    pub fn tanh(x: f64) f64 {
        return std.math.tanh(x);
    }
};
fn nativeLiteral(comptime v: f32) []const u8 {
    return if (@as(u32, @bitCast(v)) == 0x80000000) "-0.0f" else if (v == 0) "0.0f" else if (v == 1) "1.0f" else if (v == 0.5) "0.5f" else if (v == 0.044715) "0.044715f" else if (v == 0.7978845608028654) "0.7978845608028654f" else if (v == -1.702) "-1.702f" else std.fmt.comptimePrint("{e}f", .{v});
}

/// A source-template binding. Native schedules retain temporaries and barriers;
/// this object supplies only the arithmetic for a selected store/phase.
pub const Emission = struct {
    expr: FExpr,
    source: FExpr.Source,
    inputs: Inputs([]const u8),
    pub fn render(comptime e: Emission) []const u8 {
        return e.expr.renderNative(e.source, e.inputs, null);
    }
};
