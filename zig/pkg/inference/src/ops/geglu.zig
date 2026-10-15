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

//! Reference for `fused_geglu` and its backward (`ml.graph.GegluAttrs`): for
//! each row of `x` = [gate | value], each `width` wide, the output is
//! gelu_exact(gate) * value. The Metal kernels (`gliner_boundary_device`
//! kinds `laya_geglu` and `geglu_backward`) use the same arithmetic,
//! including exact GELU's clamp to x (above 10) and 0 (below -10).
const std = @import("std");
const erf = @import("../backends/activations.zig").erfApproxF32;

fn gelu(x: f32) f32 {
    if (!std.math.isFinite(x)) return 0;
    if (x > 10) return x;
    if (x < -10) return 0;
    return 0.5 * x * (1 + erf(x * 0.7071067811865476));
}

fn geluDerivative(x: f32) f32 {
    if (!std.math.isFinite(x)) return 0;
    if (x > 10) return 1;
    if (x < -10) return 0;
    return 0.5 * (1 + erf(x * 0.7071067811865476)) + x * 0.3989422804014327 * @exp(-0.5 * x * x);
}

pub fn forward(out: []f32, x: []const f32, width: usize) !void {
    if (width == 0 or x.len % (2 * width) != 0 or out.len * 2 != x.len) return error.InvalidGeglu;
    for (0..x.len / (2 * width)) |row| for (0..width) |i| {
        const gate = x[row * 2 * width + i];
        out[row * width + i] = gelu(gate) * x[row * 2 * width + width + i];
    };
}

/// dx = [dout * value * gelu'(gate) | dout * gelu(gate)].
pub fn backward(dx: []f32, x: []const f32, dout: []const f32, width: usize) !void {
    if (width == 0 or x.len % (2 * width) != 0 or dx.len != x.len or dout.len * 2 != x.len) return error.InvalidGeglu;
    for (0..x.len / (2 * width)) |row| for (0..width) |i| {
        const gate = x[row * 2 * width + i];
        const value = x[row * 2 * width + width + i];
        const g = dout[row * width + i];
        dx[row * 2 * width + i] = g * value * geluDerivative(gate);
        dx[row * 2 * width + width + i] = g * gelu(gate);
    };
}

test "geglu backward matches finite differences" {
    const x = [_]f32{ 0.3, -1.2, 2.5, 0.7, -0.4, 1.1 };
    const dout = [_]f32{ 1.5, -0.5, 2.0 };
    var dx: [6]f32 = undefined;
    try backward(&dx, &x, &dout, 3);
    for (0..x.len) |i| {
        var plus = x;
        var minus = x;
        plus[i] += 1e-3;
        minus[i] -= 1e-3;
        var out_plus: [3]f32 = undefined;
        var out_minus: [3]f32 = undefined;
        try forward(&out_plus, &plus, 3);
        try forward(&out_minus, &minus, 3);
        var numeric: f32 = 0;
        for (dout, out_plus, out_minus) |g, p, m| numeric += g * (p - m) / 2e-3;
        try std.testing.expectApproxEqAbs(numeric, dx[i], 2e-3);
    }
}
