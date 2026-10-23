// Copyright 2026 Antfly, Inc.
// SPDX-License-Identifier: Apache-2.0
//! Integer conversion for already-rounded IEEE binary16 checkpoint values.
//! Avoids software floating-point conversion calls in baseline CPU builds.
const std = @import("std");

pub inline fn isRepresentable(value: f32) bool {
    const magnitude = @as(u32, @bitCast(value)) & 0x7fffffff;
    if (magnitude == 0) return true;
    const exponent = magnitude >> 23;
    if (exponent < 103 or exponent > 142) return false;
    const shift: u5 = @intCast(if (exponent < 113) 126 - exponent else 13);
    return magnitude & ((@as(u32, 1) << shift) - 1) == 0;
}

/// The caller validates finite exact representability before conversion.
pub inline fn encode(value: f32) f16 {
    std.debug.assert(isRepresentable(value));
    const bits: u32 = @bitCast(value);
    const sign: u16 = @intCast((bits >> 16) & 0x8000);
    const magnitude = bits & 0x7fffffff;
    if (magnitude == 0) return @bitCast(sign);
    const exponent = magnitude >> 23;
    if (exponent >= 113) return @bitCast(sign | @as(u16, @intCast((magnitude - 0x38000000) >> 13)));
    const shift: u5 = @intCast(126 - exponent);
    return @bitCast(sign | @as(u16, @intCast(((magnitude & 0x7fffff) | 0x800000) >> shift)));
}

pub inline fn decode(value: f16) f32 {
    const bits: u16 = @bitCast(value);
    const sign: u32 = @as(u32, bits & 0x8000) << 16;
    const exponent: u32 = (bits >> 10) & 31;
    const fraction: u32 = bits & 1023;
    if (exponent == 31) return @bitCast(sign | 0x7f800000 | (fraction << 13));
    if (exponent != 0) return @bitCast(sign | ((exponent + 112) << 23) | (fraction << 13));
    if (fraction == 0) return @bitCast(sign);
    const shift: u5 = @intCast(@clz(fraction) - 21);
    return @bitCast(sign | ((113 - @as(u32, shift)) << 23) | ((fraction << (shift + 13)) & 0x7fffff));
}

test "exact half conversion preserves all finite half bit patterns and signed zero" {
    for (0..65536) |i| {
        const bits: u16 = @intCast(i);
        const half: f16 = @bitCast(bits);
        const expected: f32 = @floatCast(half);
        const actual = decode(half);
        if (!std.math.isFinite(expected)) {
            try std.testing.expect(!isRepresentable(actual));
            try std.testing.expectEqual(std.math.isNan(expected), std.math.isNan(actual));
            continue;
        }
        try std.testing.expectEqual(@as(u32, @bitCast(expected)), @as(u32, @bitCast(actual)));
        try std.testing.expect(isRepresentable(actual));
        try std.testing.expectEqual(bits, @as(u16, @bitCast(encode(actual))));
    }
}

test "exact half admission matches rounded IEEE conversion on float boundaries and random bits" {
    var bits: u32 = 0;
    for (0..131072) |_| {
        bits = bits *% 1664525 +% 1013904223;
        const value: f32 = @bitCast(bits);
        const rounded: f32 = @floatCast(@as(f16, @floatCast(value)));
        try std.testing.expectEqual(std.math.isFinite(value) and rounded == value, isRepresentable(value));
    }
    for ([_]u32{ 0, 0x80000000, 1, 0x33000000, 0x33800000, 0x387fc000, 0x38800000, 0x477fe000, 0x47800000, 0x7f800000, 0xff800000, 0x7fc00000 }) |boundary| {
        for ([_]u32{ boundary -% 1, boundary, boundary +% 1 }) |candidate| {
            const value: f32 = @bitCast(candidate);
            const rounded: f32 = @floatCast(@as(f16, @floatCast(value)));
            try std.testing.expectEqual(std.math.isFinite(value) and rounded == value, isRepresentable(value));
        }
    }
}

/// The first 2*N bytes contain little-endian CUDA half values. Expand from
/// the end so writes never overwrite half values still awaiting conversion.
pub fn expandReadback(output: []f32) void {
    const bytes = std.mem.sliceAsBytes(output);
    var i = output.len;
    while (i > 0) {
        i -= 1;
        const bits = std.mem.readInt(u16, bytes[i * 2 ..][0..2], .little);
        output[i] = decode(@bitCast(bits));
    }
}

test "in-place CUDA half readback preserves every half pattern and boundaries" {
    const a = std.testing.allocator;
    const buffer = try a.alloc(f32, 65536 + 2);
    defer a.free(buffer);
    const sentinel: f32 = @bitCast(@as(u32, 0xdeadbeef));
    for ([_]usize{ 0, 1, 2, 3, 17, 4096, 8193, 65536 }) |n| {
        @memset(buffer, sentinel);
        const out = buffer[1..][0..n];
        const bytes = std.mem.sliceAsBytes(out);
        for (0..n) |i| std.mem.writeInt(u16, bytes[i * 2 ..][0..2], @intCast(i), .little);
        expandReadback(out);
        for (out, 0..) |actual, i| {
            const expected = decode(@bitCast(@as(u16, @intCast(i))));
            try std.testing.expectEqual(@as(u32, @bitCast(expected)), @as(u32, @bitCast(actual)));
        }
        try std.testing.expectEqual(@as(u32, 0xdeadbeef), @as(u32, @bitCast(buffer[0])));
        try std.testing.expectEqual(@as(u32, 0xdeadbeef), @as(u32, @bitCast(buffer[n + 1])));
    }
}
