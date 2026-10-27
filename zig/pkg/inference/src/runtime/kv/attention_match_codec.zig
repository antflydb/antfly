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

//! Portable E4M3FN checkpoint codec. Power-of-two scales make accepted decoded
//! values exactly representable in f16; no device-specific FP8 ABI is used.
const std = @import("std");

pub const Encoding = enum {
    f32,
    f16,
    fp8,
    rotated4,

    pub fn parse(text: []const u8) !Encoding {
        return std.meta.stringToEnum(Encoding, text) orelse error.UnsupportedKvCheckpointEncoding;
    }

    pub fn wireVersion(self: Encoding) u32 {
        return switch (self) {
            .f32 => 2,
            .f16, .fp8 => 3,
            .rotated4 => 4,
        };
    }
};

pub const Fp8 = struct {
    first_row: u32 = 0,
    rows: u32 = 0,
    key_scale: f32 = 1,
    value_scale: f32 = 1,
};

pub fn halfExact(value: f32) bool {
    const half: f16 = @floatCast(value);
    return std.math.isFinite(value) and std.math.isFinite(half) and
        @as(u32, @bitCast(value)) == @as(u32, @bitCast(@as(f32, half)));
}

pub fn validScale(scale: f32) bool {
    if (!std.math.isFinite(scale) or scale < 1.0 / 256.0 or scale > 256) return false;
    return @as(u32, @bitCast(scale)) & 0x7fffff == 0;
}

pub fn scaleFor(values: []const f64) f32 {
    var maximum: f64 = 0;
    for (values) |value| maximum = @max(maximum, @abs(value));
    var scale: f32 = 1.0 / 256.0;
    while (maximum > 448.0 * scale and scale < 256) scale *= 2;
    return scale;
}

pub fn decode(bits: u8, scale: f32) !f32 {
    if (!validScale(scale) or bits & 0x7f == 0x7f) return error.InvalidKvCheckpoint;
    const exponent = (bits >> 3) & 15;
    const mantissa = bits & 7;
    // E4M3FN normals map directly to IEEE f32 exponent/mantissa fields.
    // Subnormals are exact multiples of 2^-9. Avoid libm in the scalar hot path.
    const magnitude: f32 = if (exponent == 0)
        @as(f32, @floatFromInt(mantissa)) * (1.0 / 512.0)
    else
        @bitCast(((@as(u32, exponent) + 120) << 23) | (@as(u32, mantissa) << 20));
    const value = magnitude * scale;
    const signed = if (bits & 128 != 0) -value else value;
    if (!halfExact(signed)) return error.InvalidKvCheckpoint;
    return signed;
}

/// Round to nearest, ties to even. Overflow is rejected, never saturated.
pub fn encode(value: f32, scale: f32) !u8 {
    if (!std.math.isFinite(value) or !validScale(scale)) return error.InvalidKvCheckpoint;
    const magnitude = @abs(value) / scale;
    if (magnitude > 448) return error.InvalidKvCheckpoint;
    const code: u8 = if (magnitude < 1.0 / 64.0) subnormal: {
        const units = magnitude * 512;
        const integer: u8 = @intFromFloat(units);
        const remainder = units - @as(f32, @floatFromInt(integer));
        break :subnormal integer + @as(u8, @intFromBool(remainder > 0.5 or (remainder == 0.5 and integer & 1 != 0)));
    } else normal: {
        const bits: u32 = @bitCast(magnitude);
        // Drop 20 mantissa bits, with ties going to the even retained bit.
        // Carry naturally crosses exponent boundaries, including subnormal 8.
        const rounded = bits + 0x7ffff + ((bits >> 20) & 1);
        break :normal @intCast((rounded >> 20) - 120 * 8);
    };
    return code | @as(u8, @intCast((@as(u32, @bitCast(value)) >> 24) & 128));
}

pub fn round(values: []f64, scale: f32) !void {
    for (values) |*value| value.* = try decode(try encode(@floatCast(value.*), scale), scale);
}

test "FP8 E4M3FN exhaustive canonical codes and ties preserve signed zero" {
    for (0..256) |i| {
        const code: u8 = @intCast(i);
        if (code & 127 == 127) {
            try std.testing.expectError(error.InvalidKvCheckpoint, decode(code, 1));
        } else {
            const value = try decode(code, 1);
            try std.testing.expectEqual(code, try encode(value, 1));
        }
    }
    try std.testing.expectEqual(@as(f32, 448), try decode(126, 1));
    try std.testing.expectEqual(@as(u8, 56), try encode(1.0625, 1));
    try std.testing.expectEqual(@as(u8, 58), try encode(1.1875, 1));
    try std.testing.expectError(error.InvalidKvCheckpoint, encode(449, 1));
    try std.testing.expectError(error.InvalidKvCheckpoint, decode(126, 256));
    for ([_]f32{ 0, -1, 0.3, std.math.inf(f32), std.math.nan(f32) }) |scale|
        try std.testing.expectError(error.InvalidKvCheckpoint, decode(0, scale));
}

// Independent arithmetic oracle: enumerate the format, then search adjacent
// values. Keep this deliberately separate from the production bit conversion.
fn referenceMagnitude(code: u8) f32 {
    const exponent = (code >> 3) & 15;
    const mantissa = code & 7;
    if (exponent == 0) return @as(f32, @floatFromInt(mantissa)) / 512;
    var value: f32 = @as(f32, @floatFromInt(8 + @as(u16, mantissa))) / 512;
    for (1..exponent) |_| value *= 2;
    return value;
}

fn referenceEncode(value: f32, scale: f32) !u8 {
    const magnitude = @abs(value) / scale;
    if (!std.math.isFinite(value) or magnitude > 448) return error.InvalidKvCheckpoint;
    var low: u8 = 0;
    var high: u8 = 126;
    while (low < high) {
        const mid = low + (high - low) / 2;
        if (referenceMagnitude(mid) < magnitude) low = mid + 1 else high = mid;
    }
    if (low > 0) {
        const above = referenceMagnitude(low) - magnitude;
        const below = magnitude - referenceMagnitude(low - 1);
        if (below < above or (below == above and low & 1 != 0)) low -= 1;
    }
    return low | @as(u8, @intCast((@as(u32, @bitCast(value)) >> 24) & 128));
}

test "FP8 conversion matches independent oracle for every half and supported scale" {
    var scale: f32 = 1.0 / 256.0;
    while (scale <= 256) : (scale *= 2) {
        for (0..65536) |bits| {
            const value: f32 = @as(f16, @bitCast(@as(u16, @intCast(bits))));
            const expected = referenceEncode(value, scale) catch {
                try std.testing.expectError(error.InvalidKvCheckpoint, encode(value, scale));
                continue;
            };
            try std.testing.expectEqual(expected, try encode(value, scale));
        }
        for (0..256) |bits| {
            const code: u8 = @intCast(bits);
            const magnitude = referenceMagnitude(code & 127) * scale;
            const expected = if (code & 128 != 0) -magnitude else magnitude;
            if (code & 127 == 127 or !halfExact(expected)) {
                try std.testing.expectError(error.InvalidKvCheckpoint, decode(code, scale));
            } else {
                try std.testing.expectEqual(@as(u32, @bitCast(expected)), @as(u32, @bitCast(try decode(code, scale))));
            }
        }
        // FP32 values just below/at/above every rounding midpoint catch errors
        // that would be invisible when testing already-rounded FP16 inputs.
        for (0..126) |i| {
            const low = referenceMagnitude(@intCast(i));
            const high = referenceMagnitude(@intCast(i + 1));
            const midpoint: u32 = @bitCast((low + high) * 0.5 * scale);
            for ([_]u32{ midpoint - 1, midpoint, midpoint + 1 }) |bits| {
                const value: f32 = @bitCast(bits);
                try std.testing.expectEqual(try referenceEncode(value, scale), try encode(value, scale));
                try std.testing.expectEqual(try referenceEncode(-value, scale), try encode(-value, scale));
            }
        }
    }
}
