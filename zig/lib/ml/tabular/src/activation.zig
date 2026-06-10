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

//! Output activations applied once per sample after the model stage.

const std = @import("std");
const ir = @import("ir.zig");

pub fn apply(act: ir.Activation, x: []f32) void {
    switch (act) {
        .identity => {},
        .sigmoid => sigmoid(x),
        .softmax => softmax(x),
        .exp => expOnly(x),
    }
}

fn sigmoid(x: []f32) void {
    for (x) |*v| v.* = 1.0 / (1.0 + @exp(-v.*));
}

fn expOnly(x: []f32) void {
    for (x) |*v| v.* = @exp(v.*);
}

/// Numerically-stable softmax: subtract max before exp.
fn softmax(x: []f32) void {
    if (x.len == 0) return;
    var mx: f32 = x[0];
    for (x[1..]) |v| if (v > mx) {
        mx = v;
    };
    var sum: f32 = 0;
    for (x) |*v| {
        v.* = @exp(v.* - mx);
        sum += v.*;
    }
    if (sum > 0) for (x) |*v| {
        v.* /= sum;
    };
}

test "identity is a no-op" {
    var v = [_]f32{ 1, 2, 3 };
    apply(.identity, &v);
    try std.testing.expectEqualSlices(f32, &[_]f32{ 1, 2, 3 }, &v);
}

test "sigmoid maps 0 to 0.5" {
    var v = [_]f32{0};
    apply(.sigmoid, &v);
    try std.testing.expectApproxEqAbs(@as(f32, 0.5), v[0], 1e-6);
}

test "softmax sums to 1 and is stable for large inputs" {
    var v = [_]f32{ 1000, 1001, 1002 };
    apply(.softmax, &v);
    const sum = v[0] + v[1] + v[2];
    try std.testing.expectApproxEqAbs(@as(f32, 1.0), sum, 1e-5);
    try std.testing.expect(v[0] < v[1]);
    try std.testing.expect(v[1] < v[2]);
}

test "softmax handles empty slice" {
    var v: [0]f32 = .{};
    apply(.softmax, &v);
}
