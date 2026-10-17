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

const std = @import("std");
const core = @import("src/gemm.zig");
const prepared = @import("src/packed_gemm.zig");
fn direct(m: usize, n: usize, k: usize, x: []const f32, w: []const f32, y: []f32, repeats: usize) void {
    for (0..repeats) |_| {
        core.sgemmTransBSequential(m, n, k, 1, x, w, 0, y);
        std.mem.doNotOptimizeAway(y.ptr);
    }
}
fn panels(comptime kc: usize, comptime mc: usize, m: usize, p: prepared.Panels, x: []const f32, y: []f32, repeats: usize) void {
    for (0..repeats) |_| {
        prepared.rowsTiled(kc, mc, p, 0, m, x, y);
        std.mem.doNotOptimizeAway(y.ptr);
    }
}
pub fn main(init: std.process.Init) !void {
    var buffer: [8192]u8 = undefined;
    var output = std.Io.File.stdout().writer(init.io, &buffer);
    const a = init.gpa;
    for ([_][3]usize{ .{ 32, 512, 512 }, .{ 128, 512, 512 }, .{ 512, 512, 512 }, .{ 128, 4096, 512 }, .{ 128, 512, 2048 }, .{ 513, 1024, 512 }, .{ 127, 131, 65 } }) |shape| {
        const m, const n, const k = .{ shape[0], shape[1], shape[2] };
        const x = try a.alloc(f32, m * k);
        defer a.free(x);
        const w = try a.alloc(f32, n * k);
        defer a.free(w);
        const y = try a.alloc(f32, m * n);
        defer a.free(y);
        const expected = try a.alloc(f32, y.len);
        defer a.free(expected);
        for (x, 0..) |*v, i| v.* = @as(f32, @floatFromInt(@as(i32, @intCast(i % 31)) - 15)) / 31;
        for (w, 0..) |*v, i| v.* = @as(f32, @floatFromInt(@as(i32, @intCast(i % 37)) - 18)) / 37;
        const cold_start = std.Io.Clock.awake.now(init.io).nanoseconds;
        const p = try prepared.Panels.init(a, w, n, k);
        defer p.deinit(a);
        const setup_ns = std.Io.Clock.awake.now(init.io).nanoseconds - cold_start;
        direct(m, n, k, x, w, expected, 1);
        panels(128, 32, m, p, x, y, 1);
        var delta: f32 = 0;
        for (y, expected) |v, e| delta = @max(delta, @abs(v - e));
        if (delta > 2e-5) return error.KernelMismatch;
        const repeats = @max(2, 100_000_000 / (m * n * k));
        inline for (.{ .{ 128, 32 }, .{ 256, 32 }, .{ 512, 32 }, .{ 512, 8 } }) |tile| for (0..5) |sample| {
            const t0 = std.Io.Clock.awake.now(init.io).nanoseconds;
            direct(m, n, k, x, w, y, repeats);
            const t1 = std.Io.Clock.awake.now(init.io).nanoseconds;
            panels(tile[0], tile[1], m, p, x, y, repeats);
            const t2 = std.Io.Clock.awake.now(init.io).nanoseconds;
            delta = 0;
            for (y, expected) |v, e| delta = @max(delta, @abs(v - e));
            if (delta > 2e-5) return error.KernelMismatch;
            direct(m, n, k, x, w, y, repeats);
            const t3 = std.Io.Clock.awake.now(init.io).nanoseconds;
            try output.interface.print("{{\"m\":{d},\"n\":{d},\"k\":{d},\"kc\":{d},\"mc\":{d},\"sample\":{d},\"repeats\":{d},\"setup_ns\":{d},\"panel_bytes\":{d},\"before_ns\":{d},\"candidate_ns\":{d},\"after_ns\":{d},\"max_delta\":{d}}}\n", .{ m, n, k, tile[0], tile[1], sample, repeats, setup_ns, p.data.len * 4, t1 - t0, t2 - t1, t3 - t2, delta });
            try output.interface.flush();
        };
    }
}
