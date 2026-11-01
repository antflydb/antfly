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
            .add, .mul => |xs| e == tag or xs[0].uses(tag) or xs[1].uses(tag),
            .gelu, .silu, .round_f16, .round_bf16 => |x| e == tag or x.uses(tag),
        };
    }

    /// f64 reference evaluation.
    pub fn evalF64(comptime e: FExpr, value: f64, bias: f64, res: f64) f64 {
        if (comptime e.uses(.gate)) @compileError("use evalF64Inputs to supply a separate gate");
        return e.evalF64Inputs(.{ .value = value, .bias = bias, .gate = 0, .residual = res });
    }
    pub fn evalF64Inputs(comptime e: FExpr, inputs: Inputs(f64)) f64 {
        return switch (e) {
            .acc => inputs.value,
            .bias => inputs.bias,
            .gate => inputs.gate,
            .residual => inputs.residual,
            .lit => |v| v,
            .add => |xs| xs[0].evalF64Inputs(inputs) + xs[1].evalF64Inputs(inputs),
            .mul => |xs| xs[0].evalF64Inputs(inputs) * xs[1].evalF64Inputs(inputs),
            .gelu => |x| vmath.geluF64(x.evalF64Inputs(inputs)),
            .silu => |x| blk: {
                const v = x.evalF64Inputs(inputs);
                break :blk v / (1 + @exp(-v));
            },
            .round_f16 => |x| @as(f16, @floatCast(x.evalF64Inputs(inputs))),
            .round_bf16 => |x| roundBf16Value(@floatCast(x.evalF64Inputs(inputs))),
        };
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
        return switch (e) {
            .acc => inputs.value,
            .bias => inputs.bias,
            .gate => inputs.gate,
            .residual => inputs.residual,
            .lit => |v| @splat(v),
            .add => |xs| xs[0].evalVecInputs(n, Math, inputs) + xs[1].evalVecInputs(n, Math, inputs),
            .mul => |xs| xs[0].evalVecInputs(n, Math, inputs) * xs[1].evalVecInputs(n, Math, inputs),
            .gelu => |x| Math.gelu(n, x.evalVecInputs(n, Math, inputs)),
            .silu => |x| blk: {
                const v = x.evalVecInputs(n, Math, inputs);
                break :blk v / (@as(@Vector(n, f32), @splat(1)) + Math.exp(n, -v));
            },
            .round_f16 => |x| @floatCast(@as(@Vector(n, f16), @floatCast(x.evalVecInputs(n, Math, inputs)))),
            .round_bf16 => |x| blk: {
                const v = x.evalVecInputs(n, Math, inputs);
                var out: @Vector(n, f32) = undefined;
                inline for (0..n) |i| out[i] = roundBf16Value(v[i]);
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
        return switch (e) {
            .acc => inputs.value,
            .bias => inputs.bias,
            .gate => inputs.gate,
            .residual => inputs.residual,
            .lit => |v| std.fmt.comptimePrint("{e}f", .{v}),
            .add => |xs| "(" ++ xs[0].renderInputs(source, inputs) ++ " + " ++ xs[1].renderInputs(source, inputs) ++ ")",
            .mul => |xs| "(" ++ xs[0].renderInputs(source, inputs) ++ " * " ++ xs[1].renderInputs(source, inputs) ++ ")",
            .gelu => |x| source.gelu ++ "(" ++ x.renderInputs(source, inputs) ++ ")",
            .silu => |x| source.silu ++ "(" ++ x.renderInputs(source, inputs) ++ ")",
            .round_f16 => |x| source.round_f16 ++ "(" ++ x.renderInputs(source, inputs) ++ ")",
            .round_bf16 => |x| source.round_bf16 ++ "(" ++ x.renderInputs(source, inputs) ++ ")",
        };
    }
};

pub const acc: FExpr = .acc;
pub fn lit(comptime v: f32) FExpr {
    return .{ .lit = v };
}

pub const DefaultMath = struct {
    pub const exp = vmath.vexp;
    pub const gelu = vmath.vgelu;
};
/// IEEE bfloat16 round-to-nearest-even, preserving infinities and quieting NaNs
/// rather than allowing discarded payload bits to turn a NaN into infinity.
pub fn roundBf16Value(value: f32) f32 {
    const bits: u32 = @bitCast(value);
    if (bits & 0x7fffffff > 0x7f800000) return @bitCast((bits & 0xffff0000) | 0x00400000);
    return @bitCast((bits +% 0x7fff +% ((bits >> 16) & 1)) & 0xffff0000);
}
