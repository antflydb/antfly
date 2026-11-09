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

//! Vector math for the CPU lowering, plus the matching C source the GPU
//! lowerings emit, so every backend evaluates the same polynomials.
//!
//! `@exp` on a `@Vector` lowers to one libm call per lane, and Zig has no
//! `erf`, so both are polynomials here. The f64 versions (`expF64`, `erfF64`,
//! `geluF64`) are accurate references for tests.

const std = @import("std");

/// exp, Cephes-style (about 1 ulp). Inputs below -87.3 (including -inf)
/// return 0.
pub fn vexp(comptime width: comptime_int, x: @Vector(width, f32)) @Vector(width, f32) {
    const V = @Vector(width, f32);
    const I = @Vector(width, i32);
    const xc = @min(@max(x, @as(V, @splat(-87.3))), @as(V, @splat(88.37)));
    const n = @round(xc * @as(V, @splat(1.44269504088896341)));
    const r = xc - n * @as(V, @splat(0.693359375)) - n * @as(V, @splat(-2.12194440e-4));
    var p: V = @splat(1.9875691500e-4);
    p = p * r + @as(V, @splat(1.3981999507e-3));
    p = p * r + @as(V, @splat(8.3334519073e-3));
    p = p * r + @as(V, @splat(4.1665795894e-2));
    p = p * r + @as(V, @splat(1.6666665459e-1));
    p = p * r + @as(V, @splat(5.0000001201e-1));
    const y = p * r * r + r + @as(V, @splat(1));
    const ni: I = @intFromFloat(n);
    const scale: V = @bitCast((ni + @as(I, @splat(127))) << @splat(23));
    return @select(f32, x < @as(V, @splat(-87.3)), @as(V, @splat(0)), y * scale);
}

// Abramowitz & Stegun 7.1.26: |error| <= 1.5e-7 in exact arithmetic,
// about 6e-7 in f32.
const erf_p = 0.3275911;
const erf_a = [5]f32{ 0.254829592, -0.284496736, 1.421413741, -1.453152027, 1.061405429 };

pub fn verf(comptime width: comptime_int, x: @Vector(width, f32)) @Vector(width, f32) {
    const V = @Vector(width, f32);
    const ax = @abs(x);
    const t = @as(V, @splat(1)) / (@as(V, @splat(1)) + @as(V, @splat(erf_p)) * ax);
    var poly: V = @splat(erf_a[4]);
    inline for (0..4) |i| poly = poly * t + @as(V, @splat(erf_a[3 - i]));
    const y = @as(V, @splat(1)) - poly * t * vexp(width, -ax * ax);
    return @select(f32, x < @as(V, @splat(0)), -y, y);
}

/// GELU with the erf form, as PyTorch's `nn.GELU()` / Whisper's "gelu".
pub fn vgelu(comptime width: comptime_int, x: @Vector(width, f32)) @Vector(width, f32) {
    const V = @Vector(width, f32);
    return @as(V, @splat(0.5)) * x * (@as(V, @splat(1)) + verf(width, x * @as(V, @splat(std.math.sqrt1_2))));
}

pub fn erfF64(x: f64) f64 {
    const ax = @abs(x);
    if (ax > 5) return std.math.sign(x);
    // Taylor series; at |x| <= 5 the cancellation stays below ~1e-9.
    var term = x;
    var sum = x;
    var n: f64 = 0;
    while (@abs(term) > 1e-17 * @abs(sum) or n < 4) {
        n += 1;
        term *= -x * x / n;
        sum += term / (2 * n + 1);
        if (n > 400) break;
    }
    return sum * 2 / std.math.sqrt(std.math.pi);
}

pub fn geluF64(x: f64) f64 {
    return 0.5 * x * (1 + erfF64(x * std.math.sqrt1_2));
}

/// C helpers for the GPU lowerings (`func` is the function qualifier,
/// `exp_fn` and `abs_fn` the dialect's exp and fabs).
pub fn cHelpers(comptime func: []const u8, comptime exp_fn: []const u8, comptime abs_fn: []const u8) []const u8 {
    return std.fmt.comptimePrint(
        \\{[f]s} float antfly_tile_erf(float x) {{
        \\    const float ax = {[abs]s}(x);
        \\    const float t = 1.0f / (1.0f + {[p]e}f * ax);
        \\    const float poly = (((({[a4]e}f * t + {[a3]e}f) * t + {[a2]e}f) * t + {[a1]e}f) * t + {[a0]e}f) * t;
        \\    const float y = 1.0f - poly * {[exp]s}(-ax * ax);
        \\    return x < 0.0f ? -y : y;
        \\}}
        \\{[f]s} float antfly_tile_gelu(float x) {{
        \\    return 0.5f * x * (1.0f + antfly_tile_erf(x * 0.70710678118654752f));
        \\}}
        \\
    , .{ .f = func, .exp = exp_fn, .abs = abs_fn, .p = erf_p, .a0 = erf_a[0], .a1 = erf_a[1], .a2 = erf_a[2], .a3 = erf_a[3], .a4 = erf_a[4] });
}
