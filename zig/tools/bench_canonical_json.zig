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

//! Native canonical digest comparison. Build with --dep identity, pointing
//! identity at pkg/antfly-embedded/src/common/value_identity.zig. JSON lines go
//! to stderr; synthetic corpus generation is outside the measured allocator.
const std = @import("std");
const identity = @import("identity");
fn reference(a: std.mem.Allocator, raw: []const u8) ![32]u8 {
    var parsed = try std.json.parseFromSlice(std.json.Value, a, raw, .{});
    defer parsed.deinit();
    return identity.digestValue(parsed.value);
}
pub fn main(init: std.process.Init) !void {
    const a = init.arena.allocator();
    const patterns = [_][]const u8{ "x", "line\nquote\"\\", "café λ 🚀", "tiny" };
    const names = [_][]const u8{ "opaque_ascii", "opaque_escaped", "opaque_unicode", "opaque_small" };
    for (patterns, names, 0..) |pattern, name, case| {
        const size = if (case == 3) pattern.len else (256 * 1024 / pattern.len) * pattern.len;
        const text = try a.alloc(u8, size);
        for (0..size / pattern.len) |i| @memcpy(text[i * pattern.len ..][0..pattern.len], pattern);
        const raw = try std.json.Stringify.valueAlloc(a, .{.{ .text = text, .n = @as(f64, 1.23456789) }}, .{});
        const expected = try reference(std.heap.smp_allocator, raw);
        for (0..5) |round| {
            const order: [2]bool = if (round % 2 == 0) .{ false, true } else .{ true, false };
            for (order) |stream| {
                var measured = std.testing.FailingAllocator.init(std.heap.smp_allocator, .{});
                const alloc = measured.allocator();
                const started = std.Io.Clock.awake.now(init.io).nanoseconds;
                const iterations = 32;
                for (0..iterations) |_| {
                    const digest = if (stream) (try identity.validatedScalarDigest(alloc, raw)).? else try reference(alloc, raw);
                    if (!std.mem.eql(u8, &expected, &digest)) return error.DigestMismatch;
                    std.mem.doNotOptimizeAway(&digest);
                }
                const elapsed = std.Io.Clock.awake.now(init.io).nanoseconds - started;
                if (measured.allocated_bytes != measured.freed_bytes) return error.LeakedAllocation;
                std.debug.print("{{\"workload\":\"{s}\",\"mode\":\"{s}\",\"round\":{d},\"iterations\":{d},\"bytes\":{d},\"elapsed_ns\":{d},\"allocated_bytes\":{d},\"allocations\":{d}}}\n", .{ name, if (stream) "stream" else "dom", round, iterations, raw.len, elapsed, measured.allocated_bytes, measured.allocations });
            }
        }
    }
}
