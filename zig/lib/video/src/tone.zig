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

//! Pinned inference rendering: BT.2100 EOTF, D65 gamut conversion, fixed
//! 100-nit exposure, global luminance Reinhard shoulder, then sRGB encoding.
//! HLG assumes a 1000-nit display, gamma 1.2 and zero black. This is an explicit
//! reproducible policy, not a mastering-metadata-dependent television renderer.
const std = @import("std");
pub const Policy = enum(u32) { none, pq_reinhard_100, hlg_reinhard_100 };
fn pq64(n: f64) f64 {
    const v = std.math.pow(f64, std.math.clamp(n, 0, 1), 32.0 / 2523.0);
    return 100.0 * std.math.pow(f64, @max(v - 3424.0 / 4096.0, 0) / (2413.0 / 128.0 - 2392.0 / 128.0 * v), 16384.0 / 2610.0);
}
fn hlg64(n: f64) f64 {
    const v = std.math.clamp(n, 0, 1);
    return if (v <= 0.5) v * v / 3 else (@exp((v - 0.55991073) / 0.17883277) + 0.28466892) / 12;
}
pub fn pq(n: f32) f32 {
    return @floatCast(pq64(n));
}
pub fn hlgScene(n: f32) f32 {
    return @floatCast(hlg64(n));
}
pub fn render(encoded: [3]f32, wide: bool, policy: Policy) [3]u8 {
    return render64(.{ encoded[0], encoded[1], encoded[2] }, wide, policy);
}
pub fn render64(encoded: [3]f64, wide: bool, policy: Policy) [3]u8 {
    std.debug.assert(policy != .none);
    var linear: [3]f64 = undefined;
    for (&linear, encoded) |*channel, n| channel.* = if (policy == .pq_reinhard_100) pq64(n / 255) else hlg64(n / 255);
    if (policy == .hlg_reinhard_100) {
        const l = if (wide) linear[0] * 0.2627 + linear[1] * 0.6780 + linear[2] * 0.0593 else linear[0] * 0.2126 + linear[1] * 0.7152 + linear[2] * 0.0722;
        const scale = 10 * std.math.pow(f64, @max(l, 0), 0.2);
        for (&linear) |*channel| channel.* *= scale;
    }
    if (wide) {
        const r = linear;
        linear = .{
            1.660491 * r[0] - 0.587641 * r[1] - 0.072850 * r[2],
            -0.124550 * r[0] + 1.132900 * r[1] - 0.008350 * r[2],
            -0.018151 * r[0] - 0.100579 * r[1] + 1.118730 * r[2],
        };
    }
    const scale = 1 / (1 + @max(linear[0] * 0.2126 + linear[1] * 0.7152 + linear[2] * 0.0722, 0));
    var out: [3]u8 = undefined;
    for (&out, linear) |*channel, c| {
        const v = std.math.clamp(c * scale, 0, 1);
        const encoded_srgb = if (v <= 0.0031308) 12.92 * v else 1.055 * std.math.pow(f64, v, 1.0 / 2.4) - 0.055;
        channel.* = @intFromFloat(std.math.clamp(@floor(encoded_srgb * 255 + 0.5), 0, 255));
    }
    return out;
}
test "HDR transfer anchors and fixed exposure preserve neutral channels" {
    try std.testing.expectApproxEqAbs(@as(f32, 0), pq(0), 0.000001);
    try std.testing.expectApproxEqAbs(@as(f32, 100), pq(1), 0.01);
    try std.testing.expectApproxEqAbs(@as(f32, 1), pq(0.508078421517399), 0.0001);
    try std.testing.expectApproxEqAbs(@as(f32, 1.0 / 12.0), hlgScene(0.5), 0.000001);
    try std.testing.expectApproxEqAbs(@as(f32, 1), hlgScene(1), 0.00001);
    try std.testing.expectEqual([3]u8{ 0, 0, 0 }, render(.{ 0, 0, 0 }, true, .pq_reinhard_100));
    try std.testing.expectEqual([3]u8{ 188, 188, 188 }, render(@splat(0.508078421517399 * 255), true, .pq_reinhard_100));
    try std.testing.expectEqual([3]u8{ 254, 254, 254 }, render(@splat(255), true, .pq_reinhard_100));
}
