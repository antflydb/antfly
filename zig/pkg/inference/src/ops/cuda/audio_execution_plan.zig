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

//! Warm allocation/lifetime trace. Stable offsets reuse only storage whose
//! last consumer has been submitted on the same stream. No request is rerun
//! for planning; capture validates the complete allocation/free event sequence.
const std = @import("std");
const arena = @import("execution_arena.zig");
pub const limit = arena.limit;
pub const Plan = arena.Plan;
pub const Scope = arena.Scope;
pub const Entry = struct { key: u64 = 0, stamp: u64 = 0, warmed: bool = false, disabled: bool = false, plan: ?*Plan = null };
pub const Cache = struct {
    entries: [8]Entry = @splat(.{}),
    clock: u64 = 0,
    pub fn get(self: *Cache, key: u64) *Entry {
        self.clock +|= 1;
        var oldest: usize = 0;
        for (&self.entries, 0..) |*e, i| {
            if (e.stamp != 0 and e.key == key) {
                e.stamp = self.clock;
                return e;
            }
            if (e.stamp < self.entries[oldest].stamp) oldest = i;
        }
        const e = &self.entries[oldest];
        if (e.plan) |p| std.heap.c_allocator.destroy(p);
        e.* = .{ .key = key, .stamp = self.clock };
        return e;
    }
    pub fn clear(self: *Cache) void {
        for (self.entries) |e| if (e.plan) |p| std.heap.c_allocator.destroy(p);
        self.* = .{};
    }
};
test "embeddinggemma2 CUDA audio lifetime plan reuses dead regions and rejects allocation drift" {
    const a = std.testing.allocator;
    const p = try a.create(Plan);
    defer a.destroy(p);
    p.* = .{};
    p.allocate(256, 1000);
    p.allocate(512, 2000);
    p.release(1000);
    p.allocate(128, 3000);
    p.release(2000);
    try std.testing.expect(p.finish(3000));
    try std.testing.expectEqual(@as(usize, 768), p.peak);
    var s = Scope{ .plan = p, .base = 10000 };
    try std.testing.expectEqual(@as(usize, 10000), try s.allocate(256));
    try std.testing.expectEqual(@as(usize, 10256), try s.allocate(512));
    s.release(10000);
    try std.testing.expectEqual(@as(usize, 10000), try s.allocate(128));
    s.release(10256);
    try std.testing.expect(s.valid and s.cursor == p.event_count);
    var wrong = Scope{ .plan = p, .base = 10000 };
    try std.testing.expectError(error.AudioPlanChanged, wrong.allocate(128));
    p.* = .{};
    p.allocate(limit + 1, 1);
    try std.testing.expect(!p.valid);
    p.* = .{};
    p.allocate(256, 1);
    p.allocate(256, 2);
    try std.testing.expect(!p.finish(2));
}
