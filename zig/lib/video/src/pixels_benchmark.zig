// Copyright 2026 Antfly, Inc.
// SPDX-License-Identifier: Apache-2.0
//! 1080p 4:2:2 high-depth chroma packing, scalar and portable SIMD.
const std = @import("std");
const pixels = @import("h264_pixels.zig");
fn run(comptime vector: bool, out: []u8, u: []const u16, v: []const u16) void {
    for (0..32) |_| {
        for (0..1080) |y| {
            const start = y * 960;
            const target = out[start * 4 ..][0 .. 960 * 4];
            if (vector) pixels.interleave(u16, target, u[start..][0..960], v[start..][0..960]) else pixels.scalar(u16, target, u[start..][0..960], v[start..][0..960]);
        }
        std.mem.doNotOptimizeAway(out.ptr);
    }
}
pub fn main(init: std.process.Init) !void {
    const a = init.gpa;
    const u = try a.alloc(u16, 960 * 1080);
    defer a.free(u);
    const v = try a.alloc(u16, u.len);
    defer a.free(v);
    const scalar = try a.alloc(u8, u.len * 4);
    defer a.free(scalar);
    const simd = try a.alloc(u8, scalar.len);
    defer a.free(simd);
    for (u, v, 0..) |*first, *second, i| {
        first.* = @truncate(i * 113);
        second.* = @truncate(i * 79 + 17);
    }
    var output_buffer: [4096]u8 = undefined;
    var writer = std.Io.File.stdout().writer(init.io, &output_buffer);
    for (0..10) |sample| {
        const first = std.Io.Clock.awake.now(init.io).nanoseconds;
        run(false, scalar, u, v);
        const middle = std.Io.Clock.awake.now(init.io).nanoseconds;
        run(true, simd, u, v);
        const end = std.Io.Clock.awake.now(init.io).nanoseconds;
        if (!std.mem.eql(u8, scalar, simd)) return error.KernelMismatch;
        try writer.interface.print("{{\"sample\":{d},\"frames\":32,\"width\":1920,\"height\":1080,\"chroma\":422,\"storage_bits\":16,\"output_bytes\":{d},\"scalar_ns\":{d},\"simd_ns\":{d}}}\n", .{ sample, scalar.len * 32, middle - first, end - middle });
    }
    try writer.interface.flush();
}
