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

//! Feature scaling and missing-value imputation. Both operate in-place on
//! f32 feature vectors.

const std = @import("std");
const ir = @import("ir.zig");

pub const Error = error{
    OutOfMemory,
    FeatureCountMismatch,
};

pub const ScalerEngine = struct {
    alloc: std.mem.Allocator,
    method: ir.ScalerMethod,
    /// Per-feature parameters pre-cast to f32. Length = num_features.
    a: []const f32, // standard/robust: center;   minmax: min;      maxabs: unused
    b: []const f32, // standard/robust: scale;    minmax: max-min;  maxabs: abs_scale

    pub fn init(alloc: std.mem.Allocator, sc: ir.Scaler) Error!ScalerEngine {
        const n: usize = switch (sc.method) {
            .standard, .robust => sc.center.len,
            .minmax => sc.min.len,
            .maxabs => sc.scale.len,
        };
        if (n == 0) return Error.FeatureCountMismatch;

        const a = try alloc.alloc(f32, n);
        const b = try alloc.alloc(f32, n);
        switch (sc.method) {
            .standard, .robust => {
                for (sc.center, 0..) |v, i| a[i] = @floatCast(v);
                for (sc.scale, 0..) |v, i| b[i] = @floatCast(v);
            },
            .minmax => {
                if (sc.max.len != n) return Error.FeatureCountMismatch;
                for (sc.min, 0..) |v, i| a[i] = @floatCast(v);
                for (sc.max, 0..) |v, i| {
                    const mx: f32 = @floatCast(v);
                    b[i] = mx - a[i]; // pre-compute range
                }
            },
            .maxabs => {
                for (sc.scale, 0..) |v, i| {
                    a[i] = 0;
                    b[i] = @floatCast(v);
                }
            },
        }
        return .{ .alloc = alloc, .method = sc.method, .a = a, .b = b };
    }

    pub fn deinit(self: *ScalerEngine) void {
        self.alloc.free(self.a);
        self.alloc.free(self.b);
    }

    pub fn apply(self: *const ScalerEngine, x: []f32) void {
        if (x.len != self.a.len) return;
        switch (self.method) {
            .standard, .robust => {
                for (x, 0..) |*v, i| {
                    const s = self.b[i];
                    v.* = if (s == 0) 0 else (v.* - self.a[i]) / s;
                }
            },
            .minmax => {
                for (x, 0..) |*v, i| {
                    const range = self.b[i];
                    v.* = if (range == 0) 0 else (v.* - self.a[i]) / range;
                }
            },
            .maxabs => {
                for (x, 0..) |*v, i| {
                    const s = self.b[i];
                    v.* = if (s == 0) 0 else v.* / s;
                }
            },
        }
    }
};

pub const ImputerEngine = struct {
    alloc: std.mem.Allocator,
    fill32: []const f32,

    pub fn init(alloc: std.mem.Allocator, im: ir.Imputer) Error!ImputerEngine {
        if (im.fill_values.len == 0) return Error.FeatureCountMismatch;
        const f = try alloc.alloc(f32, im.fill_values.len);
        for (im.fill_values, 0..) |v, i| f[i] = @floatCast(v);
        return .{ .alloc = alloc, .fill32 = f };
    }

    pub fn deinit(self: *ImputerEngine) void {
        self.alloc.free(self.fill32);
    }

    pub fn apply(self: *const ImputerEngine, x: []f32) void {
        if (x.len != self.fill32.len) return;
        for (x, 0..) |*v, i| if (v.* != v.*) {
            v.* = self.fill32[i];
        };
    }
};

test "standard scaler centers and scales" {
    const sc: ir.Scaler = .{
        .method = .standard,
        .center = &[_]f64{ 5.0, 10.0 },
        .scale = &[_]f64{ 2.0, 5.0 },
    };
    var eng = try ScalerEngine.init(std.testing.allocator, sc);
    defer eng.deinit();
    var x = [_]f32{ 7.0, 20.0 };
    eng.apply(&x);
    try std.testing.expectApproxEqAbs(@as(f32, 1.0), x[0], 1e-6); // (7-5)/2
    try std.testing.expectApproxEqAbs(@as(f32, 2.0), x[1], 1e-6); // (20-10)/5
}

test "minmax scaler maps to [0,1]" {
    const sc: ir.Scaler = .{
        .method = .minmax,
        .min = &[_]f64{ 0.0, -1.0 },
        .max = &[_]f64{ 10.0, 1.0 },
    };
    var eng = try ScalerEngine.init(std.testing.allocator, sc);
    defer eng.deinit();
    var x = [_]f32{ 5.0, 0.0 };
    eng.apply(&x);
    try std.testing.expectApproxEqAbs(@as(f32, 0.5), x[0], 1e-6);
    try std.testing.expectApproxEqAbs(@as(f32, 0.5), x[1], 1e-6);
}

test "imputer fills NaN only" {
    const im: ir.Imputer = .{
        .strategy = .mean,
        .fill_values = &[_]f64{ 0.0, 99.0 },
    };
    var eng = try ImputerEngine.init(std.testing.allocator, im);
    defer eng.deinit();
    var x = [_]f32{ 3.0, std.math.nan(f32) };
    eng.apply(&x);
    try std.testing.expectApproxEqAbs(@as(f32, 3.0), x[0], 1e-6);
    try std.testing.expectApproxEqAbs(@as(f32, 99.0), x[1], 1e-6);
}
