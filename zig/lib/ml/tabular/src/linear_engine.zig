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

//! Linear / logistic regression. y = W·x + b.
//!
//! Activations (sigmoid for logistic, softmax for multinomial) are applied
//! by the output stage, not here.

const std = @import("std");
const ir = @import("ir.zig");

pub const Error = error{
    OutOfMemory,
    FeatureCountMismatch,
    OutputMismatch,
};

pub const LinearEngine = struct {
    alloc: std.mem.Allocator,
    num_features: u32,
    num_outputs: u32,
    /// row-major [num_outputs * num_features], pre-cast to f32.
    weights32: []const f32,
    biases32: []const f32,

    pub fn init(alloc: std.mem.Allocator, lm: ir.LinearModel) Error!LinearEngine {
        if (lm.weights.len == 0) return Error.OutputMismatch;
        const num_outputs: u32 = @intCast(lm.weights.len);
        const num_features: u32 = @intCast(lm.weights[0].len);
        for (lm.weights) |row| if (row.len != num_features) return Error.FeatureCountMismatch;
        if (lm.biases.len != num_outputs) return Error.OutputMismatch;

        const w = try alloc.alloc(f32, @as(usize, num_outputs) * num_features);
        const b = try alloc.alloc(f32, num_outputs);
        for (lm.weights, 0..) |row, oi| {
            for (row, 0..) |v, fi| w[oi * num_features + fi] = @floatCast(v);
        }
        for (lm.biases, 0..) |v, i| b[i] = @floatCast(v);

        return .{
            .alloc = alloc,
            .num_features = num_features,
            .num_outputs = num_outputs,
            .weights32 = w,
            .biases32 = b,
        };
    }

    pub fn deinit(self: *LinearEngine) void {
        self.alloc.free(self.weights32);
        self.alloc.free(self.biases32);
    }

    pub fn predictSingle(self: *const LinearEngine, feats: []const f32, out: []f32) void {
        if (feats.len != self.num_features or out.len != self.num_outputs) return;
        var oi: u32 = 0;
        while (oi < self.num_outputs) : (oi += 1) {
            const row = self.weights32[oi * self.num_features .. (oi + 1) * self.num_features];
            var acc: f32 = self.biases32[oi];
            for (row, 0..) |w, fi| acc += w * feats[fi];
            out[oi] = acc;
        }
    }
};

test "logistic stump: w=[2], b=[-1]" {
    const lm: ir.LinearModel = .{
        .weights = &[_][]const f64{&[_]f64{2.0}},
        .biases = &[_]f64{-1.0},
    };
    var eng = try LinearEngine.init(std.testing.allocator, lm);
    defer eng.deinit();

    var out: [1]f32 = undefined;
    eng.predictSingle(&[_]f32{0.5}, &out);
    try std.testing.expectApproxEqAbs(@as(f32, 0.0), out[0], 1e-6); // 2*0.5 - 1 = 0
    eng.predictSingle(&[_]f32{1.0}, &out);
    try std.testing.expectApproxEqAbs(@as(f32, 1.0), out[0], 1e-6);
}

test "multi-output linear" {
    const lm: ir.LinearModel = .{
        .weights = &[_][]const f64{
            &[_]f64{ 1, 0 },
            &[_]f64{ 0, 2 },
        },
        .biases = &[_]f64{ 0.0, 0.5 },
    };
    var eng = try LinearEngine.init(std.testing.allocator, lm);
    defer eng.deinit();

    var out: [2]f32 = undefined;
    eng.predictSingle(&[_]f32{ 3, 4 }, &out);
    try std.testing.expectApproxEqAbs(@as(f32, 3.0), out[0], 1e-6);
    try std.testing.expectApproxEqAbs(@as(f32, 8.5), out[1], 1e-6); // 2*4 + 0.5
}
