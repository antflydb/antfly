// Copyright 2026 Antfly, Inc.
// SPDX-License-Identifier: Apache-2.0
//! Portable SIMD semiplanar packing. Samples above 8 bits stay little-endian.
const std = @import("std");
const little = @import("builtin").target.cpu.arch.endian() == .little;
pub fn row(comptime Sample: type, out: []u8, samples: []const Sample) void {
    std.debug.assert(out.len == samples.len * @sizeOf(Sample));
    if (little or Sample == u8) @memcpy(out, std.mem.sliceAsBytes(samples)) else {
        for (samples, 0..) |value, i| std.mem.writeInt(u16, out[i * 2 ..][0..2], value, .little);
    }
}
pub fn scalar(comptime Sample: type, out: []u8, u: []const Sample, v: []const Sample) void {
    std.debug.assert(u.len == v.len and out.len == u.len * 2 * @sizeOf(Sample));
    for (u, v, 0..) |a, b, i| {
        if (Sample == u8) {
            out[i * 2] = a;
            out[i * 2 + 1] = b;
        } else {
            std.mem.writeInt(u16, out[i * 4 ..][0..2], a, .little);
            std.mem.writeInt(u16, out[i * 4 + 2 ..][0..2], b, .little);
        }
    }
}
pub fn interleave(comptime Sample: type, out: []u8, u: []const Sample, v: []const Sample) void {
    std.debug.assert(u.len == v.len and out.len == u.len * 2 * @sizeOf(Sample));
    if ((!little and Sample != u8) or @import("builtin").zig_backend != .stage2_llvm) return scalar(Sample, out, u, v);
    const lanes = 8;
    const mask: @Vector(lanes * 2, i32) = comptime blk: {
        var result: [lanes * 2]i32 = undefined;
        for (0..lanes) |i| {
            result[i * 2] = @intCast(i);
            result[i * 2 + 1] = ~@as(i32, @intCast(i));
        }
        break :blk result;
    };
    var cursor: usize = 0;
    while (u.len - cursor >= lanes) : (cursor += lanes) {
        const a: @Vector(lanes, Sample) = u[cursor..][0..lanes].*;
        const b: @Vector(lanes, Sample) = v[cursor..][0..lanes].*;
        const interleaved: [lanes * 2 * @sizeOf(Sample)]u8 = @bitCast(@shuffle(Sample, a, b, mask));
        @memcpy(out[cursor * 2 * @sizeOf(Sample) ..][0..interleaved.len], &interleaved);
    }
    scalar(Sample, out[cursor * 2 * @sizeOf(Sample) ..], u[cursor..], v[cursor..]);
}
test "video SIMD plane packing matches scalar across depths tails and misaligned output" {
    inline for (.{ u8, u16 }) |Sample| {
        var u: [97]Sample = undefined;
        var v: [97]Sample = undefined;
        for (&u, &v, 0..) |*a, *b, i| {
            a.* = @truncate(i * 113);
            b.* = @truncate(i * 79 + 17);
        }
        for (0..98) |length| {
            var want: [400]u8 = @splat(0);
            var got: [400]u8 = @splat(0);
            const size = length * 2 * @sizeOf(Sample);
            scalar(Sample, want[1..][0..size], u[0..length], v[0..length]);
            interleave(Sample, got[1..][0..size], u[0..length], v[0..length]);
            try std.testing.expectEqualSlices(u8, &want, &got);
        }
    }
}
