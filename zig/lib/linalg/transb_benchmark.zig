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
const variants = [_][3]usize{ .{ 8, 6, 2 }, .{ 8, 4, 2 }, .{ 4, 4, 4 }, .{ 4, 6, 3 }, .{ 4, 4, 3 } };
fn run(comptime tile: [3]usize, m: usize, n: usize, k: usize, a: []const f32, b: []const f32, c: []f32, repeats: usize) void {
    for (0..repeats) |_| {
        @memset(c, 0);
        core.sgemmTransBAddSliceTiled(f32, tile[0], tile[1], tile[2], 0, m, n, k, 0.125, a, b, c);
        std.mem.doNotOptimizeAway(c.ptr);
    }
}
pub fn main(init: std.process.Init) !void {
    var buffer: [8192]u8 = undefined;
    var out = std.Io.File.stdout().writer(init.io, &buffer);
    const alloc = init.gpa;
    for ([_][3]usize{ .{ 64, 256, 64 }, .{ 64, 118, 64 }, .{ 64, 256, 256 }, .{ 127, 131, 65 }, .{ 256, 512, 512 } }) |shape| {
        const m, const n, const k = .{ shape[0], shape[1], shape[2] };
        const a = try alloc.alloc(f32, m * k);
        defer alloc.free(a);
        const b = try alloc.alloc(f32, n * k);
        defer alloc.free(b);
        const c = try alloc.alloc(f32, m * n);
        defer alloc.free(c);
        const expected = try alloc.alloc(f32, c.len);
        defer alloc.free(expected);
        for (a, 0..) |*x, i| x.* = @as(f32, @floatFromInt(@as(i32, @intCast(i % 47)) - 23)) / 29;
        for (b, 0..) |*x, i| x.* = @as(f32, @floatFromInt(@as(i32, @intCast(i % 53)) - 26)) / 31;
        run(variants[0], m, n, k, a, b, expected, 1);
        const repeats = @max(8, 500_000_000 / (m * n * k));
        for (0..7) |sample| {
            inline for (variants[1..]) |tile| {
                const first = std.Io.Clock.awake.now(init.io).nanoseconds;
                run(variants[0], m, n, k, a, b, c, repeats);
                const second = std.Io.Clock.awake.now(init.io).nanoseconds;
                run(tile, m, n, k, a, b, c, repeats);
                const third = std.Io.Clock.awake.now(init.io).nanoseconds;
                var error_max: f32 = 0;
                for (c, expected) |x, y| error_max = @max(error_max, @abs(x - y));
                run(variants[0], m, n, k, a, b, c, repeats);
                const last = std.Io.Clock.awake.now(init.io).nanoseconds;
                if (error_max > 2e-5) return error.KernelMismatch;
                try out.interface.print("{{\"m\":{d},\"n\":{d},\"k\":{d},\"width\":{d},\"mr\":{d},\"nr\":{d},\"sample\":{d},\"repeats\":{d},\"before_ns\":{d},\"candidate_ns\":{d},\"after_ns\":{d},\"max_delta\":{d}}}\n", .{ m, n, k, tile[0], tile[1], tile[2], sample, repeats, second - first, third - second, last - third, error_max });
                try out.interface.flush();
            }
        }
    }
    try out.interface.flush();
}
