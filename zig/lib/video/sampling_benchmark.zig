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

//! Metadata-only long-clip benchmark; excludes decoder and model execution.
const std = @import("std");
const sampling = @import("src/sampling.zig");
fn now() i96 {
    return std.Io.Clock.awake.now(std.Options.debug_io).nanoseconds;
}
pub fn main() !void {
    const a = std.heap.smp_allocator;
    const count = 108000; // One hour at 30 fps.
    const pictures = try a.alloc(sampling.TimedPicture, count);
    defer a.free(pictures);
    for (pictures, 0..) |*picture, i| picture.* = .{ .pts = @intCast(i), .duration = 1 };
    const begin = now();
    var timeline = try sampling.Timeline.init(a, pictures, .{});
    defer timeline.deinit();
    const built = now();
    var checksum: usize = 0;
    const windows = 256;
    for (0..windows) |i| {
        const policy = sampling.TimestampPolicy{ .start_ns = @intCast(i * 8 * std.time.ns_per_s), .end_ns = @intCast((i * 8 + 16) * std.time.ns_per_s), .step_ns = std.time.ns_per_s, .hold = true, .overflow = .truncate };
        const selected = try timeline.select(a, 30, policy, .{}, true);
        defer a.free(selected);
        for (selected) |id| checksum +%= id;
    }
    const indexed = now();
    var reference_sum: usize = 0;
    for (0..windows) |i| {
        const policy = sampling.TimestampPolicy{ .start_ns = @intCast(i * 8 * std.time.ns_per_s), .end_ns = @intCast((i * 8 + 16) * std.time.ns_per_s), .step_ns = std.time.ns_per_s, .hold = true, .overflow = .truncate };
        const selected = try sampling.cappedTimestamps(a, pictures, 30, policy, .{});
        defer a.free(selected);
        for (selected) |id| reference_sum +%= id;
    }
    const finished = now();
    if (checksum != reference_sum) return error.IndexSamplingMismatch;
    std.debug.print("{{\"pictures\":{d},\"windows\":{d},\"build_ns\":{d},\"indexed_ns\":{d},\"linear_ns\":{d},\"checksum\":{d}}}\n", .{ count, windows, built - begin, indexed - built, finished - indexed, checksum });
}
