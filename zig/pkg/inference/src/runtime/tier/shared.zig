// Copyright 2026 Antfly, Inc.
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

const Stats = struct {
    requests: u64 = 0,
    completions: u64 = 0,
    hint_total: u64 = 0,
    last_request_hint: u32 = 0,
    last_request_epoch: u64 = 0,
    last_complete_epoch: u64 = 0,
};

pub const SharedPrefetchState = struct {
    allocator: std.mem.Allocator,
    mutex: std.atomic.Mutex = .unlocked,
    epoch: u64 = 0,
    stats: std.StringHashMapUnmanaged(Stats) = .empty,

    pub fn init(allocator: std.mem.Allocator) SharedPrefetchState {
        return .{ .allocator = allocator };
    }

    pub fn deinit(self: *SharedPrefetchState) void {
        var it = self.stats.iterator();
        while (it.next()) |entry| self.allocator.free(entry.key_ptr.*);
        self.stats.deinit(self.allocator);
    }

    pub fn noteRequest(self: *SharedPrefetchState, name: []const u8, hint: u32) !u64 {
        self.lock();
        defer self.unlock();

        self.epoch += 1;
        const stats = try self.getOrPutStats(name);
        stats.requests += 1;
        stats.hint_total +|= hint;
        stats.last_request_hint = hint;
        stats.last_request_epoch = self.epoch;
        return priorityFor(self.epoch, stats.*);
    }

    pub fn noteComplete(self: *SharedPrefetchState, name: []const u8) !u64 {
        self.lock();
        defer self.unlock();

        self.epoch += 1;
        const stats = try self.getOrPutStats(name);
        stats.completions += 1;
        stats.last_complete_epoch = self.epoch;
        return priorityFor(self.epoch, stats.*);
    }

    pub fn pendingEstimate(self: *SharedPrefetchState, name: []const u8) u64 {
        self.lock();
        defer self.unlock();

        const stats = self.stats.get(name) orelse return 0;
        return stats.requests -| stats.completions;
    }

    pub fn priorityEstimate(self: *SharedPrefetchState, name: []const u8) u64 {
        self.lock();
        defer self.unlock();

        const stats = self.stats.get(name) orelse return 0;
        return priorityFor(self.epoch, stats);
    }

    fn lock(self: *SharedPrefetchState) void {
        while (!self.mutex.tryLock()) {
            std.Thread.yield() catch {};
        }
    }

    fn unlock(self: *SharedPrefetchState) void {
        self.mutex.unlock();
    }

    fn getOrPutStats(self: *SharedPrefetchState, name: []const u8) !*Stats {
        const entry = try self.stats.getOrPut(self.allocator, name);
        if (!entry.found_existing) {
            entry.key_ptr.* = try self.allocator.dupe(u8, name);
            entry.value_ptr.* = .{};
        }
        return entry.value_ptr;
    }
};

fn priorityFor(now_epoch: u64, stats: Stats) u64 {
    const pending = stats.requests -| stats.completions;
    const recency_window: u64 = 64;
    const request_age = now_epoch -| stats.last_request_epoch;
    const recentness = if (stats.last_request_epoch == 0 or request_age >= recency_window)
        0
    else
        (recency_window - request_age);
    const completion_age = now_epoch -| stats.last_complete_epoch;
    const unfinished_bias = if (stats.last_request_epoch > stats.last_complete_epoch)
        @min(completion_age + 1, recency_window)
    else
        0;
    const recent_hint = @as(u64, stats.last_request_hint) * 8;
    const learned_hint = @min(stats.hint_total, 4096);
    var priority: u64 = 0;
    priority +|= pending *| 4096;
    priority +|= recentness *| 64;
    priority +|= unfinished_bias *| 16;
    priority +|= recent_hint;
    priority +|= learned_hint;
    priority +|= @min(stats.requests, 1023);
    return priority;
}

test "shared prefetch state tracks requests and completions" {
    var state = SharedPrefetchState.init(std.testing.allocator);
    defer state.deinit();

    const first = try state.noteRequest("a", 1);
    const second = try state.noteRequest("a", 8);
    const after_complete = try state.noteComplete("a");

    try std.testing.expect(second > first);
    try std.testing.expect(after_complete < second);
    try std.testing.expectEqual(@as(u64, 1), state.pendingEstimate("a"));
    try std.testing.expect(state.priorityEstimate("a") > 0);
}
