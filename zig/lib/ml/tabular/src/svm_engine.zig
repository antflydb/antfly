// Copyright 2026 Antfly, Inc.
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

//! SVM decision function:
//!   f_o(x) = Σ_i α_{o,i} K(x, sv_i) + b_o
//! Kernels: linear, RBF, polynomial, sigmoid (tanh).
//! Multi-class is one-vs-rest, one score per output.

const std = @import("std");
const ir = @import("ir.zig");

pub const Error = error{
    OutOfMemory,
    FeatureCountMismatch,
    OutputMismatch,
    UnsupportedKernel,
};

pub const SvmEngine = struct {
    alloc: std.mem.Allocator,
    kernel: ir.SvmKernel,
    num_features: u32,
    num_vectors: u32,
    num_outputs: u32,
    gamma: f32,
    coef0: f32,
    degree: u32,

    /// row-major [num_vectors * num_features], pre-cast f32.
    support_vectors32: []const f32,
    /// row-major [num_outputs * num_vectors], pre-cast f32.
    dual32: []const f32,
    intercept32: []const f32,

    pub fn init(alloc: std.mem.Allocator, sm: ir.SVMModel) Error!SvmEngine {
        if (sm.support_vectors.len == 0) return Error.OutputMismatch;
        const num_vectors: u32 = @intCast(sm.support_vectors.len);
        const num_features: u32 = @intCast(sm.support_vectors[0].len);
        for (sm.support_vectors) |sv| if (sv.len != num_features) return Error.FeatureCountMismatch;
        if (sm.dual_coefficients.len == 0) return Error.OutputMismatch;
        const num_outputs: u32 = @intCast(sm.dual_coefficients.len);
        for (sm.dual_coefficients) |row| if (row.len != num_vectors) return Error.OutputMismatch;
        if (sm.intercept.len != num_outputs) return Error.OutputMismatch;

        const sv = try alloc.alloc(f32, @as(usize, num_vectors) * num_features);
        const du = try alloc.alloc(f32, @as(usize, num_outputs) * num_vectors);
        const ic = try alloc.alloc(f32, num_outputs);
        for (sm.support_vectors, 0..) |row, vi| {
            for (row, 0..) |v, fi| sv[vi * num_features + fi] = @floatCast(v);
        }
        for (sm.dual_coefficients, 0..) |row, oi| {
            for (row, 0..) |v, vi| du[oi * num_vectors + vi] = @floatCast(v);
        }
        for (sm.intercept, 0..) |v, i| ic[i] = @floatCast(v);

        return .{
            .alloc = alloc,
            .kernel = sm.kernel,
            .num_features = num_features,
            .num_vectors = num_vectors,
            .num_outputs = num_outputs,
            .gamma = @floatCast(sm.gamma),
            .coef0 = @floatCast(sm.coef0),
            .degree = sm.degree,
            .support_vectors32 = sv,
            .dual32 = du,
            .intercept32 = ic,
        };
    }

    pub fn deinit(self: *SvmEngine) void {
        self.alloc.free(self.support_vectors32);
        self.alloc.free(self.dual32);
        self.alloc.free(self.intercept32);
    }

    pub fn predictSingle(self: *const SvmEngine, feats: []const f32, out: []f32) void {
        if (feats.len != self.num_features or out.len != self.num_outputs) return;

        // Compute K(x, sv_i) for every support vector once, then dot with each
        // output's dual-coefficient row.
        var k_buf: [256]f32 = undefined;
        var k_dyn: ?[]f32 = null;
        defer if (k_dyn) |k| self.alloc.free(k);

        const k_slice: []f32 = blk: {
            if (self.num_vectors <= k_buf.len) break :blk k_buf[0..self.num_vectors];
            const k = self.alloc.alloc(f32, self.num_vectors) catch return;
            k_dyn = k;
            break :blk k;
        };

        var vi: u32 = 0;
        while (vi < self.num_vectors) : (vi += 1) {
            const sv = self.support_vectors32[vi * self.num_features .. (vi + 1) * self.num_features];
            k_slice[vi] = self.kernelValue(feats, sv);
        }

        var oi: u32 = 0;
        while (oi < self.num_outputs) : (oi += 1) {
            const row = self.dual32[oi * self.num_vectors .. (oi + 1) * self.num_vectors];
            var acc: f32 = self.intercept32[oi];
            for (row, 0..) |a, j| acc += a * k_slice[j];
            out[oi] = acc;
        }
    }

    inline fn kernelValue(self: *const SvmEngine, x: []const f32, sv: []const f32) f32 {
        return switch (self.kernel) {
            .linear => dot(x, sv),
            .rbf => @exp(-self.gamma * sqDist(x, sv)),
            .poly => intPow(self.gamma * dot(x, sv) + self.coef0, self.degree),
            .sigmoid => std.math.tanh(self.gamma * dot(x, sv) + self.coef0),
        };
    }
};

inline fn dot(a: []const f32, b: []const f32) f32 {
    var s: f32 = 0;
    for (a, 0..) |v, i| s += v * b[i];
    return s;
}

inline fn sqDist(a: []const f32, b: []const f32) f32 {
    var s: f32 = 0;
    for (a, 0..) |v, i| {
        const d = v - b[i];
        s += d * d;
    }
    return s;
}

inline fn intPow(base: f32, exp: u32) f32 {
    var result: f32 = 1.0;
    var b: f32 = base;
    var e = exp;
    while (e > 0) : (e >>= 1) {
        if ((e & 1) != 0) result *= b;
        b *= b;
    }
    return result;
}

test "linear-kernel SVM is a dot product plus bias" {
    const sm: ir.SVMModel = .{
        .kernel = .linear,
        .support_vectors = &[_][]const f64{&[_]f64{ 1, 0 }},
        .dual_coefficients = &[_][]const f64{&[_]f64{1.0}},
        .intercept = &[_]f64{0.0},
        .gamma = 0,
        .coef0 = 0,
        .degree = 1,
    };
    var eng = try SvmEngine.init(std.testing.allocator, sm);
    defer eng.deinit();
    var out: [1]f32 = undefined;
    eng.predictSingle(&[_]f32{ 3, 4 }, &out);
    try std.testing.expectApproxEqAbs(@as(f32, 3), out[0], 1e-6);
}

test "RBF kernel evaluates to 1 at the support vector itself" {
    const sm: ir.SVMModel = .{
        .kernel = .rbf,
        .support_vectors = &[_][]const f64{&[_]f64{ 0, 0 }},
        .dual_coefficients = &[_][]const f64{&[_]f64{1.0}},
        .intercept = &[_]f64{0.0},
        .gamma = 1.0,
        .coef0 = 0,
        .degree = 1,
    };
    var eng = try SvmEngine.init(std.testing.allocator, sm);
    defer eng.deinit();
    var out: [1]f32 = undefined;
    eng.predictSingle(&[_]f32{ 0, 0 }, &out);
    try std.testing.expectApproxEqAbs(@as(f32, 1.0), out[0], 1e-6);
}

test "poly kernel raises to degree" {
    const sm: ir.SVMModel = .{
        .kernel = .poly,
        .support_vectors = &[_][]const f64{&[_]f64{ 1, 1 }},
        .dual_coefficients = &[_][]const f64{&[_]f64{1.0}},
        .intercept = &[_]f64{0.0},
        .gamma = 1.0,
        .coef0 = 0.0,
        .degree = 3,
    };
    var eng = try SvmEngine.init(std.testing.allocator, sm);
    defer eng.deinit();
    var out: [1]f32 = undefined;
    eng.predictSingle(&[_]f32{ 1, 1 }, &out); // dot = 2, (1*2+0)^3 = 8
    try std.testing.expectApproxEqAbs(@as(f32, 8.0), out[0], 1e-5);
}
