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

//! Feedback for bounded immutable work. Progress count is not a resource cap:
//! callers still enforce byte, allocator, operation and deadline admission.
const std = @import("std");
pub const Sample = struct {
    completed: usize,
    elapsed_ns: u64,
    read_bytes: u64,
    write_bytes: u64,
};
pub const Policy = struct {
    minimum: usize = 1,
    maximum: usize = 4096,
    target_ns: u64 = 2 * std.time.ns_per_s,
    read_bytes: u64 = 8 * 1024 * 1024,
    write_bytes: u64 = 8 * 1024 * 1024,

    pub fn next(self: Policy, current: usize, sample: Sample) !usize {
        if (self.minimum == 0 or self.maximum < self.minimum or self.target_ns == 0 or self.read_bytes == 0 or self.write_bytes == 0 or current < self.minimum or current > self.maximum or sample.completed > current) return error.InvalidWorkQuantum;
        // Shrink on pressure even for a partial prefix. A short tail alone is
        // not evidence of pressure and never teaches a smaller quantum.
        const load = @max(@as(f64, @floatFromInt(sample.elapsed_ns)) / @as(f64, @floatFromInt(self.target_ns)), @max(@as(f64, @floatFromInt(sample.read_bytes)) / @as(f64, @floatFromInt(self.read_bytes)), @as(f64, @floatFromInt(sample.write_bytes)) / @as(f64, @floatFromInt(self.write_bytes))));
        if (load > 1) return @max(self.minimum, @min(current, @as(usize, @intFromFloat(@as(f64, @floatFromInt(sample.completed)) / load * 0.75))));
        if (sample.completed == current and load < 0.5) return @min(self.maximum, current *| 2);
        return current;
    }
};

test "publication quantum adapts completed work within independent resource limits" {
    const policy = Policy{};
    const fast = Sample{ .completed = 128, .elapsed_ns = std.time.ns_per_ms, .read_bytes = 1024, .write_bytes = 4096 };
    try std.testing.expectEqual(@as(usize, 256), try policy.next(128, fast));
    try std.testing.expectEqual(@as(usize, 128), try policy.next(128, .{ .completed = 3, .elapsed_ns = 1, .read_bytes = 0, .write_bytes = 0 }));
    try std.testing.expectEqual(@as(usize, 48), try policy.next(128, .{ .completed = 128, .elapsed_ns = 4 * std.time.ns_per_s, .read_bytes = 0, .write_bytes = 0 }));
    try std.testing.expectEqual(@as(usize, 48), try policy.next(128, .{ .completed = 128, .elapsed_ns = 1, .read_bytes = 0, .write_bytes = 16 * 1024 * 1024 }));
    try std.testing.expectEqual(@as(usize, 4096), try policy.next(4096, .{ .completed = 4096, .elapsed_ns = 1, .read_bytes = 0, .write_bytes = 0 }));
    try std.testing.expectError(error.InvalidWorkQuantum, policy.next(128, .{ .completed = 129, .elapsed_ns = 1, .read_bytes = 0, .write_bytes = 0 }));
}
