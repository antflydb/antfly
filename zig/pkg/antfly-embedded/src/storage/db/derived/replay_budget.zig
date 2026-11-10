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

//! Feedback controller for detached replay. A turn retains one primary window;
//! sizing targets a short elapsed quantum while hard limits bound payload memory.
//! Per-kind lanes survive runtime refresh through the namespace scheduling owner.
const std = @import("std");
pub const Budget = struct { records: usize = 64, bytes: u64 = 1024 * 1024 };
pub const Snapshot = struct {
    records: [5]usize = @splat(64),
    bytes: [5]u64 = @splat(1024 * 1024),
    observations: u64 = 0,
    overruns: u64 = 0,
    retention_pressure: bool = false,
    target_quantum_ns: u64 = 25 * std.time.ns_per_ms,
};
pub const Controller = struct {
    mutex: std.atomic.Mutex = .unlocked,
    lanes: [5]Budget = @splat(.{}),
    pressure: bool = false,
    blocked_appends: u64 = 0,
    observations: u64 = 0,
    overruns: u64 = 0,
    pub const quantum_ns = 25 * std.time.ns_per_ms;
    fn lock(self: *Controller) void {
        while (!self.mutex.tryLock()) std.atomic.spinLoopHint();
    }
    pub fn updatePressure(self: *Controller, pending: u64, reusable: u64, blocked: u64) void {
        self.lock();
        defer self.mutex.unlock();
        self.pressure = pending > 128 and reusable < 128 and blocked > self.blocked_appends;
        self.blocked_appends = blocked;
    }
    pub fn snapshot(self: *Controller) Snapshot {
        self.lock();
        defer self.mutex.unlock();
        var result = Snapshot{ .observations = self.observations, .overruns = self.overruns, .retention_pressure = self.pressure };
        for (self.lanes, 0..) |lane, i| {
            result.records[i] = if (self.pressure) @min(lane.records, 32) else lane.records;
            result.bytes[i] = if (self.pressure) @min(lane.bytes, 512 * 1024) else lane.bytes;
        }
        return result;
    }
    pub fn budget(self: *Controller, lane: usize) Budget {
        self.lock();
        defer self.mutex.unlock();
        var result = self.lanes[lane];
        if (self.pressure) {
            result.records = @min(result.records, 32);
            result.bytes = @min(result.bytes, 512 * 1024);
        }
        return result;
    }
    pub fn observe(self: *Controller, lane: usize, used: Budget, elapsed_ns: u64, scanned: usize, failed: bool) void {
        self.lock();
        defer self.mutex.unlock();
        self.observations +|= 1;
        if (failed or elapsed_ns > quantum_ns) {
            self.overruns +|= @intFromBool(!failed);
            self.lanes[lane] = .{ .records = @max(8, used.records / 2), .bytes = @max(128 * 1024, used.bytes / 2) };
        } else if (!self.pressure and elapsed_ns < quantum_ns / 2 and scanned >= used.records) {
            self.lanes[lane] = .{ .records = @min(2048, used.records * 2), .bytes = @min(16 * 1024 * 1024, used.bytes * 2) };
        }
    }
};

test "replay budget grows only on full cheap windows and shrinks expensive work" {
    var state: Controller = .{};
    const initial = state.budget(0);
    state.observe(0, initial, 1, initial.records - 1, false);
    try std.testing.expectEqualDeep(initial, state.budget(0));
    state.observe(0, initial, 1, initial.records, false);
    try std.testing.expectEqual(@as(usize, 128), state.budget(0).records);
    const grown = state.budget(0);
    state.observe(0, grown, Controller.quantum_ns + 1, grown.records, false);
    try std.testing.expectEqualDeep(initial, state.budget(0));
    try std.testing.expectEqualDeep(initial, state.budget(1));
}

test "replay budget bounds memory and retains a minimum under pressure and failures" {
    var state: Controller = .{};
    for (0..20) |_| {
        const used = state.budget(0);
        state.observe(0, used, 1, used.records, false);
    }
    try std.testing.expectEqual(@as(usize, 2048), state.budget(0).records);
    try std.testing.expectEqual(@as(u64, 16 * 1024 * 1024), state.budget(0).bytes);
    state.updatePressure(1000, 0, 1);
    try std.testing.expectEqual(@as(usize, 32), state.budget(0).records);
    for (0..20) |_| {
        const used = state.budget(0);
        state.observe(0, used, 1, 0, true);
    }
    try std.testing.expectEqual(@as(usize, 8), state.budget(0).records);
    try std.testing.expectEqual(@as(u64, 128 * 1024), state.budget(0).bytes);
    state.updatePressure(0, 1000, 1);
    try std.testing.expect(!state.pressure);
}
