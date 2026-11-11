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

//! Exact retractable binary64 sum, in units of the smallest subnormal.
//! 2176 signed bits cover every finite binary64 times an i64 row count.
//! The persisted odd-significand/shift string is compact for ordinary inputs;
//! fixed-width arithmetic bounds work and avoids per-observation allocations.
const std = @import("std");
pub const Int = i2176;
const UInt = u2176;
pub const Exact = struct {
    units: Int = 0,
    pub fn input(value: f64) !Exact {
        if (!std.math.isFinite(value)) return error.RetainedSeriesNumericOverflow;
        const bits: u64 = @bitCast(value);
        const exponent: u11 = @truncate(bits >> 52);
        const mantissa: UInt = (bits & 0xfffffffffffff) | (if (exponent == 0) @as(u64, 0) else @as(u64, 1) << 52);
        const magnitude: Int = @intCast(mantissa << @as(u12, if (exponent == 0) 0 else exponent - 1));
        return .{ .units = if (bits >> 63 != 0) -magnitude else magnitude };
    }
    pub fn add(self: Exact, other: Exact) !Exact {
        return .{ .units = std.math.add(Int, self.units, other.units) catch return error.RetainedSeriesNumericOverflow };
    }
    pub fn parse(bytes: []const u8) !Exact {
        if (bytes.len == 0 or bytes.len > 550) return error.RetainedSeriesCorrupt;
        const colon = std.mem.indexOfScalar(u8, bytes, ':') orelse return error.RetainedSeriesCorrupt;
        const odd = std.fmt.parseInt(Int, bytes[0..colon], 16) catch return error.RetainedSeriesCorrupt;
        const shift = std.fmt.parseInt(u12, bytes[colon + 1 ..], 16) catch return error.RetainedSeriesCorrupt;
        if (shift >= 2175 or (odd == 0 and shift != 0) or (odd != 0 and @mod(odd, 2) == 0)) return error.RetainedSeriesCorrupt;
        const value = std.math.shlExact(Int, odd, shift) catch return error.RetainedSeriesCorrupt;
        if (value == std.math.minInt(Int)) return error.RetainedSeriesCorrupt;
        return .{ .units = value };
    }
    pub fn encode(self: Exact, a: std.mem.Allocator) ![]const u8 {
        const shift: u12 = if (self.units == 0) 0 else @intCast(@ctz(@as(UInt, @bitCast(self.units))));
        return std.fmt.allocPrint(a, "{x}:{x}", .{ self.units >> shift, shift });
    }
    /// Round the exact rational once, including subnormals and AVG ties.
    pub fn number(self: Exact, divisor: u64) f64 {
        std.debug.assert(divisor != 0);
        const negative = self.units < 0;
        const magnitude: UInt = @intCast(if (negative) -self.units else self.units);
        const quotient = magnitude / divisor;
        const remainder = magnitude % divisor;
        const highest: u12 = if (quotient == 0) 0 else @intCast(2175 - @clz(quotient));
        var shift: u12 = highest -| 52;
        var kept: u64 = @intCast(quotient >> shift);
        const round_up = if (shift == 0)
            remainder * 2 > divisor or (remainder * 2 == divisor and kept & 1 != 0)
        else round: {
            const half = @as(UInt, 1) << (shift - 1);
            const discarded = quotient & ((@as(UInt, 1) << shift) - 1);
            break :round discarded > half or (discarded == half and (remainder != 0 or kept & 1 != 0));
        };
        if (round_up) kept += 1;
        if (kept == @as(u64, 1) << 53) {
            kept >>= 1;
            shift += 1;
        }
        if (shift > 2045) return if (negative) -std.math.inf(f64) else std.math.inf(f64);
        const exponent: u64 = if (kept < @as(u64, 1) << 52) 0 else shift + 1;
        const bits = (@as(u64, @intFromBool(negative)) << 63) | (exponent << 52) | (kept & 0xfffffffffffff);
        return @bitCast(bits);
    }
};

test "retained exact sum survives cancellation subnormals and rational rounding" {
    const a = std.testing.allocator;
    const large = try Exact.input(0x1p120);
    const one = try Exact.input(1);
    const total = try large.add(one);
    const result = try total.add(.{ .units = -large.units });
    try std.testing.expectEqual(@as(f64, 1), result.number(1));
    const encoded = try total.encode(a);
    defer a.free(encoded);
    try std.testing.expectEqual(total.units, (try Exact.parse(encoded)).units);
    const tiny = try Exact.input(0x1p-1074);
    try std.testing.expectEqual(@as(f64, 0), tiny.number(2));
    try std.testing.expectEqual(@as(f64, 0x1p-1073), (try tiny.add(try tiny.add(tiny))).number(2));
    const maximum = try Exact.input(std.math.floatMax(f64));
    try std.testing.expectEqual(std.math.floatMax(f64), (try maximum.add(maximum)).number(2));
    try std.testing.expect(std.math.isInf((try maximum.add(maximum)).number(1)));
    for ([_]f64{ 0, -0.0, 1, -1, 0.1, -0.1, 0x1p-1074, 0x1p-1022, 0x1p120, std.math.floatMax(f64) }) |n|
        try std.testing.expectEqual(n, (try Exact.input(n)).number(1));
}
