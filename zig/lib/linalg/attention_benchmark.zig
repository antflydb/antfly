// Copyright 2026 Antfly, Inc.
// SPDX-License-Identifier: Apache-2.0
const std = @import("std");
const attention = @import("src/attention.zig");
pub fn main(init: std.process.Init) !void {
    const a = init.gpa;
    var buffer: [8192]u8 = undefined;
    var writer = std.Io.File.stdout().writer(init.io, &buffer);
    for ([_]usize{ 257, 882, 1260 }) |seq| {
        const batch = 2;
        const heads = 16;
        const dim = 64;
        const count = batch * heads * seq * dim;
        const q = try a.alloc(f32, count);
        defer a.free(q);
        const k = try a.alloc(f32, count);
        defer a.free(k);
        const v = try a.alloc(f32, count);
        defer a.free(v);
        const mask = try a.alloc(i64, batch * seq);
        defer a.free(mask);
        @memset(mask, 1);
        var random = std.Random.DefaultPrng.init(80155);
        for (q, k, v) |*x, *y, *z| {
            x.* = random.random().float(f32) * 2 - 1;
            y.* = random.random().float(f32) * 2 - 1;
            z.* = random.random().float(f32) * 2 - 1;
        }
        for (0..5) |sample| {
            const first = std.Io.Clock.awake.now(init.io).nanoseconds;
            const serial = try attention.flashAttentionHost(a, q, k, v, null, mask, batch, seq, heads, dim);
            defer a.free(serial);
            const middle = std.Io.Clock.awake.now(init.io).nanoseconds;
            const parallel = try attention.flashAttentionHostWithIo(a, init.io, q, k, v, null, mask, batch, seq, heads, dim);
            defer a.free(parallel);
            const last = std.Io.Clock.awake.now(init.io).nanoseconds;
            if (!std.mem.eql(f32, serial, parallel)) return error.AttentionMismatch;
            try writer.interface.print("{{\"batch\":2,\"heads\":16,\"dim\":64,\"seq\":{d},\"sample\":{d},\"serial_ns\":{d},\"parallel_ns\":{d},\"exact\":true}}\n", .{ seq, sample, middle - first, last - middle });
            try writer.interface.flush();
        }
    }
}
