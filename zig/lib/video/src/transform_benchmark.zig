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

//! Standalone scalar/SIMD kernel measurement; excludes entropy, decode and I/O.
const std = @import("std");
const transform = @import("h264_transform4.zig");
fn run(comptime vector: bool, input: []const [16]i64, iterations: usize) u64 {
    var checksum: u64 = 0;
    for (0..iterations) |i| {
        const result = if (vector) transform.simd(input[i % input.len]) else transform.scalar(input[i % input.len]);
        for (result) |value| checksum +%= @bitCast(value);
        std.mem.doNotOptimizeAway(result);
    }
    return checksum;
}
pub fn main(init: std.process.Init) !void {
    var buffer: [4096]u8 = undefined;
    var writer = std.Io.File.stdout().writer(init.io, &buffer);
    var input: [1024][16]i64 = undefined;
    var random = std.Random.DefaultPrng.init(0x1345);
    for (&input) |*block| for (block) |*value| {
        value.* = random.random().intRangeAtMost(i64, -100_000, 100_000);
    };
    const iterations = 1_000_000;
    for (0..10) |sample| {
        const first = std.Io.Clock.awake.now(init.io).nanoseconds;
        const a = run(false, &input, iterations);
        const middle = std.Io.Clock.awake.now(init.io).nanoseconds;
        const b = run(true, &input, iterations);
        const end = std.Io.Clock.awake.now(init.io).nanoseconds;
        if (a != b) return error.KernelMismatch;
        try writer.interface.print("{{\"sample\":{d},\"blocks\":{d},\"scalar_ns\":{d},\"simd_ns\":{d},\"checksum\":{d}}}\n", .{ sample, iterations, middle - first, end - middle, a });
    }
    try writer.interface.flush();
}
