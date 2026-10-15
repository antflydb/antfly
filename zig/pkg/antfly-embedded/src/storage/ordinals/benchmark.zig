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

//! Compare bulk native calls with the same inline algorithms. Alternating
//! rounds include the same input reset, and publish a checksum to retain work.
const std = @import("std");
const core = @import("storage_ordinals");
const operations = core.operations;
const Timing = struct { ns: i128, checksum: u64 };
noinline fn sortRun(comptime shared: bool, io: std.Io, input: []const u32, scratch: []u32, repetitions: usize) Timing {
    var checksum: u64 = 0;
    const start = std.Io.Clock.awake.now(io).nanoseconds;
    for (0..repetitions) |_| {
        @memcpy(scratch, input);
        if (shared) core.sort(u32, scratch) else operations.sort(u32, scratch);
        checksum +%= scratch[0] +% scratch[scratch.len - 1];
    }
    return .{ .ns = std.Io.Clock.awake.now(io).nanoseconds - start, .checksum = checksum };
}
noinline fn mergeRun(comptime shared: bool, comptime operation: core.Operation, io: std.Io, left: []const u32, right: []const u32, out: []u32, repetitions: usize) Timing {
    var checksum: u64 = 0;
    const start = std.Io.Clock.awake.now(io).nanoseconds;
    for (0..repetitions) |_| {
        const count = if (shared) core.merge(operation, left, right, out) else operations.merge(operation, left, right, out);
        checksum +%= count;
        if (count != 0) checksum +%= out[count - 1];
    }
    return .{ .ns = std.Io.Clock.awake.now(io).nanoseconds - start, .checksum = checksum };
}
fn median(values: *[7]i128) i128 {
    std.mem.sortUnstable(i128, values, {}, std.sort.asc(i128));
    return values[3];
}
pub fn main(init: std.process.Init) !void {
    for ([_]usize{ 8, 64, 1024, 65536 }) |n| {
        const input = try init.gpa.alloc(u32, n);
        defer init.gpa.free(input);
        const scratch = try init.gpa.alloc(u32, n);
        defer init.gpa.free(scratch);
        var state: u32 = 0x12345678;
        for (input) |*value| {
            state ^= state << 13;
            state ^= state >> 17;
            state ^= state << 5;
            value.* = state;
        }
        const repetitions = @max(32, 16777216 / n);
        var inline_ns: [7]i128 = undefined;
        var native_ns: [7]i128 = undefined;
        var checksum: u64 = 0;
        for (0..7) |round| {
            const first = if (round % 2 == 0) sortRun(false, init.io, input, scratch, repetitions) else sortRun(true, init.io, input, scratch, repetitions);
            const second = if (round % 2 == 0) sortRun(true, init.io, input, scratch, repetitions) else sortRun(false, init.io, input, scratch, repetitions);
            inline_ns[round] = if (round % 2 == 0) first.ns else second.ns;
            native_ns[round] = if (round % 2 == 0) second.ns else first.ns;
            std.debug.assert(first.checksum == second.checksum);
            checksum +%= first.checksum;
        }
        std.debug.print("sort n={d} inline_ns={d} shared_ns={d} checksum={d}\n", .{ n, median(&inline_ns), median(&native_ns), checksum });
        const left = try init.gpa.alloc(u32, n);
        defer init.gpa.free(left);
        const right = try init.gpa.alloc(u32, n);
        defer init.gpa.free(right);
        const out = try init.gpa.alloc(u32, 2 * n);
        defer init.gpa.free(out);
        for (left, right, 0..) |*a, *b, i| {
            a.* = @intCast(i * 2);
            b.* = @intCast(i * 3);
        }
        inline for (.{ core.Operation.union_set, core.Operation.intersection, core.Operation.difference }) |operation| {
            for (0..7) |round| {
                const first = if (round % 2 == 0) mergeRun(false, operation, init.io, left, right, out, repetitions) else mergeRun(true, operation, init.io, left, right, out, repetitions);
                const second = if (round % 2 == 0) mergeRun(true, operation, init.io, left, right, out, repetitions) else mergeRun(false, operation, init.io, left, right, out, repetitions);
                inline_ns[round] = if (round % 2 == 0) first.ns else second.ns;
                native_ns[round] = if (round % 2 == 0) second.ns else first.ns;
                std.debug.assert(first.checksum == second.checksum);
                checksum +%= first.checksum;
            }
            std.debug.print("{s} n={d} inline_ns={d} shared_ns={d} checksum={d}\n", .{ @tagName(operation), n, median(&inline_ns), median(&native_ns), checksum });
        }
    }
}
