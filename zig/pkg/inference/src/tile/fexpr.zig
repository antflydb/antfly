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
//! Example: `fexpr.acc.plus(.bias).gelu().plus(.residual)`.

const std = @import("std");
const vmath = @import("vmath.zig");

pub const FExpr = union(enum) {
    acc,
    bias,
    residual,
    lit: f32,
    add: [2]*const FExpr,
    mul: [2]*const FExpr,
    gelu: *const FExpr,

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

    pub fn uses(comptime e: FExpr, comptime tag: std.meta.Tag(FExpr)) bool {
        return switch (e) {
            .acc, .bias, .residual, .lit => e == tag,
            .add, .mul => |xs| xs[0].uses(tag) or xs[1].uses(tag),
            .gelu => |x| x.uses(tag),
        };
    }

    /// f64 reference evaluation.
    pub fn evalF64(comptime e: FExpr, value: f64, bias: f64, res: f64) f64 {
        return switch (e) {
            .acc => value,
            .bias => bias,
            .residual => res,
            .lit => |v| v,
            .add => |xs| xs[0].evalF64(value, bias, res) + xs[1].evalF64(value, bias, res),
            .mul => |xs| xs[0].evalF64(value, bias, res) * xs[1].evalF64(value, bias, res),
            .gelu => |x| vmath.geluF64(x.evalF64(value, bias, res)),
        };
    }

    pub fn evalVec(comptime e: FExpr, comptime n: comptime_int, value: @Vector(n, f32), bias: @Vector(n, f32), res: @Vector(n, f32)) @Vector(n, f32) {
        return switch (e) {
            .acc => value,
            .bias => bias,
            .residual => res,
            .lit => |v| @splat(v),
            .add => |xs| xs[0].evalVec(n, value, bias, res) + xs[1].evalVec(n, value, bias, res),
            .mul => |xs| xs[0].evalVec(n, value, bias, res) * xs[1].evalVec(n, value, bias, res),
            .gelu => |x| vmath.vgelu(n, x.evalVec(n, value, bias, res)),
        };
    }

    /// C source over the given variable names.
    pub fn render(comptime e: FExpr, comptime value: []const u8, comptime bias: []const u8, comptime res: []const u8) []const u8 {
        return switch (e) {
            .acc => value,
            .bias => bias,
            .residual => res,
            .lit => |v| std.fmt.comptimePrint("{e}f", .{v}),
            .add => |xs| "(" ++ xs[0].render(value, bias, res) ++ " + " ++ xs[1].render(value, bias, res) ++ ")",
            .mul => |xs| "(" ++ xs[0].render(value, bias, res) ++ " * " ++ xs[1].render(value, bias, res) ++ ")",
            .gelu => |x| "antfly_tile_gelu(" ++ x.render(value, bias, res) ++ ")",
        };
    }
};

pub const acc: FExpr = .acc;
pub fn lit(comptime v: f32) FExpr {
    return .{ .lit = v };
}
