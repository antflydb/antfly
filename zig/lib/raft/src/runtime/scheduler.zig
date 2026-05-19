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
const core = @import("../core/mod.zig");

pub const SchedulerConfig = struct {
    tick_interval_ms: u32 = 100,
    max_tick_batch: usize = 128,
    priority_boost: u8 = 2,
};

pub const VirtualTime = struct {
    round: u64 = 0,
    now_ms: u64 = 0,
};

pub const Scheduler = struct {
    alloc: std.mem.Allocator,
    cfg: SchedulerConfig,
    time: VirtualTime = .{},
    group_ids: std.ArrayListUnmanaged(core.types.GroupId) = .empty,
    quiesced: std.AutoHashMapUnmanaged(core.types.GroupId, void) = .empty,
    priority: std.AutoHashMapUnmanaged(core.types.GroupId, u8) = .empty,
    cursor: usize = 0,
    ready_cursor: usize = 0,

    pub fn init(alloc: std.mem.Allocator, cfg: SchedulerConfig) Scheduler {
        return .{
            .alloc = alloc,
            .cfg = cfg,
        };
    }

    pub fn deinit(self: *Scheduler) void {
        self.group_ids.deinit(self.alloc);
        self.quiesced.deinit(self.alloc);
        self.priority.deinit(self.alloc);
        self.* = undefined;
    }

    pub fn registerGroup(self: *Scheduler, group_id: core.types.GroupId) !void {
        for (self.group_ids.items) |existing| {
            if (existing == group_id) return error.GroupAlreadyRegistered;
        }
        try self.group_ids.append(self.alloc, group_id);
    }

    pub fn unregisterGroup(self: *Scheduler, group_id: core.types.GroupId) bool {
        for (self.group_ids.items, 0..) |existing, i| {
            if (existing != group_id) continue;
            _ = self.group_ids.orderedRemove(i);
            _ = self.quiesced.remove(group_id);
            if (self.group_ids.items.len == 0) {
                self.cursor = 0;
                self.ready_cursor = 0;
            } else {
                if (self.cursor >= self.group_ids.items.len) {
                    self.cursor %= self.group_ids.items.len;
                }
                if (self.ready_cursor >= self.group_ids.items.len) {
                    self.ready_cursor %= self.group_ids.items.len;
                }
            }
            return true;
        }
        return false;
    }

    pub fn nextTickGroup(self: *Scheduler) ?core.types.GroupId {
        return self.nextGroup(&self.cursor);
    }

    pub fn nextReadyGroup(self: *Scheduler) ?core.types.GroupId {
        return self.nextGroup(&self.ready_cursor);
    }

    pub fn quiesceGroup(self: *Scheduler, group_id: core.types.GroupId) !void {
        if (!self.isRegistered(group_id)) return error.UnknownGroup;
        try self.quiesced.put(self.alloc, group_id, {});
    }

    pub fn resumeGroup(self: *Scheduler, group_id: core.types.GroupId) bool {
        return self.quiesced.remove(group_id);
    }

    pub fn isQuiesced(self: *const Scheduler, group_id: core.types.GroupId) bool {
        return self.quiesced.contains(group_id);
    }

    pub fn activeGroupCount(self: *const Scheduler) usize {
        return self.group_ids.items.len - self.quiesced.count();
    }

    pub fn nowMs(self: *const Scheduler) u64 {
        return self.time.now_ms;
    }

    pub fn round(self: *const Scheduler) u64 {
        return self.time.round;
    }

    pub fn snapshotTime(self: *const Scheduler) VirtualTime {
        return self.time;
    }

    pub fn advanceVirtualTime(self: *Scheduler) VirtualTime {
        self.time.round +|= 1;
        self.time.now_ms +|= self.cfg.tick_interval_ms;
        return self.time;
    }

    pub fn tickBatch(self: *Scheduler, alloc: std.mem.Allocator) ![]core.types.GroupId {
        var out = std.ArrayListUnmanaged(core.types.GroupId).empty;
        errdefer out.deinit(alloc);
        const batch_limit = @min(self.cfg.max_tick_batch, self.activeGroupCount());

        while (out.items.len < batch_limit) {
            const group_id = self.nextTickGroup() orelse break;
            try out.append(alloc, group_id);
        }
        return try out.toOwnedSlice(alloc);
    }

    pub fn noteReady(self: *Scheduler, group_id: core.types.GroupId) void {
        self.boostGroup(group_id, self.cfg.priority_boost);
    }

    pub fn noteActivity(self: *Scheduler, group_id: core.types.GroupId) void {
        self.boostGroup(group_id, self.cfg.priority_boost);
    }

    fn isRegistered(self: *const Scheduler, group_id: core.types.GroupId) bool {
        for (self.group_ids.items) |existing| {
            if (existing == group_id) return true;
        }
        return false;
    }

    fn boostGroup(self: *Scheduler, group_id: core.types.GroupId, amount: u8) void {
        if (!self.isRegistered(group_id) or self.isQuiesced(group_id)) return;
        const gop = self.priority.getOrPut(self.alloc, group_id) catch return;
        if (!gop.found_existing) gop.value_ptr.* = 0;
        const current = gop.value_ptr.*;
        gop.value_ptr.* = std.math.add(u8, current, amount) catch std.math.maxInt(u8);
    }

    fn nextGroup(self: *Scheduler, cursor: *usize) ?core.types.GroupId {
        if (self.group_ids.items.len == 0) return null;

        var checked: usize = 0;
        while (checked < self.group_ids.items.len) : (checked += 1) {
            const group_id = self.group_ids.items[cursor.*];
            cursor.* = (cursor.* + 1) % self.group_ids.items.len;
            if (self.isQuiesced(group_id)) continue;
            if (self.priority.get(group_id)) |score| {
                if (score > 0) {
                    if (score == 1) {
                        _ = self.priority.remove(group_id);
                    } else {
                        self.priority.put(self.alloc, group_id, score - 1) catch {};
                    }
                    return group_id;
                }
            }
        }

        checked = 0;
        while (checked < self.group_ids.items.len) : (checked += 1) {
            const group_id = self.group_ids.items[cursor.*];
            cursor.* = (cursor.* + 1) % self.group_ids.items.len;
            if (!self.isQuiesced(group_id)) return group_id;
        }
        return null;
    }
};

test "scheduler round-robins groups" {
    var scheduler = Scheduler.init(std.testing.allocator, .{ .max_tick_batch = 8 });
    defer scheduler.deinit();

    try scheduler.registerGroup(1);
    try scheduler.registerGroup(2);
    try scheduler.registerGroup(3);

    try std.testing.expectEqual(@as(?core.types.GroupId, 1), scheduler.nextTickGroup());
    try std.testing.expectEqual(@as(?core.types.GroupId, 2), scheduler.nextTickGroup());
    try std.testing.expectEqual(@as(?core.types.GroupId, 3), scheduler.nextTickGroup());
    try std.testing.expectEqual(@as(?core.types.GroupId, 1), scheduler.nextTickGroup());
}

test "scheduler skips quiesced groups" {
    var scheduler = Scheduler.init(std.testing.allocator, .{ .max_tick_batch = 8 });
    defer scheduler.deinit();

    try scheduler.registerGroup(1);
    try scheduler.registerGroup(2);
    try scheduler.registerGroup(3);
    try scheduler.quiesceGroup(2);

    try std.testing.expectEqual(@as(?core.types.GroupId, 1), scheduler.nextTickGroup());
    try std.testing.expectEqual(@as(?core.types.GroupId, 3), scheduler.nextTickGroup());
    try std.testing.expectEqual(@as(?core.types.GroupId, 1), scheduler.nextReadyGroup());
    try std.testing.expectEqual(@as(?core.types.GroupId, 3), scheduler.nextReadyGroup());

    try std.testing.expect(scheduler.resumeGroup(2));
    try std.testing.expectEqual(@as(?core.types.GroupId, 1), scheduler.nextTickGroup());
    try std.testing.expectEqual(@as(?core.types.GroupId, 2), scheduler.nextTickGroup());
}

test "scheduler boosts active groups ahead of plain round robin" {
    var scheduler = Scheduler.init(std.testing.allocator, .{ .priority_boost = 2 });
    defer scheduler.deinit();

    try scheduler.registerGroup(1);
    try scheduler.registerGroup(2);
    try scheduler.registerGroup(3);

    scheduler.noteActivity(3);
    try std.testing.expectEqual(@as(?core.types.GroupId, 3), scheduler.nextReadyGroup());
    try std.testing.expectEqual(@as(?core.types.GroupId, 3), scheduler.nextReadyGroup());
    try std.testing.expectEqual(@as(?core.types.GroupId, 1), scheduler.nextReadyGroup());
}

test "scheduler unregister normalizes ready cursor independently" {
    var scheduler = Scheduler.init(std.testing.allocator, .{});
    defer scheduler.deinit();

    try scheduler.registerGroup(1);
    try scheduler.registerGroup(2);
    try scheduler.registerGroup(3);

    try std.testing.expectEqual(@as(?core.types.GroupId, 1), scheduler.nextReadyGroup());
    try std.testing.expectEqual(@as(?core.types.GroupId, 2), scheduler.nextReadyGroup());
    try std.testing.expectEqual(@as(?core.types.GroupId, 3), scheduler.nextReadyGroup());

    try std.testing.expect(scheduler.unregisterGroup(1));
    try std.testing.expectEqual(@as(?core.types.GroupId, 2), scheduler.nextReadyGroup());
    try std.testing.expectEqual(@as(?core.types.GroupId, 3), scheduler.nextReadyGroup());
}

test "scheduler advances virtual time explicitly" {
    var scheduler = Scheduler.init(std.testing.allocator, .{ .tick_interval_ms = 25 });
    defer scheduler.deinit();

    try std.testing.expectEqual(@as(u64, 0), scheduler.nowMs());
    try std.testing.expectEqual(@as(u64, 0), scheduler.round());

    const first = scheduler.advanceVirtualTime();
    try std.testing.expectEqual(@as(u64, 1), first.round);
    try std.testing.expectEqual(@as(u64, 25), first.now_ms);

    const second = scheduler.advanceVirtualTime();
    try std.testing.expectEqual(@as(u64, 2), second.round);
    try std.testing.expectEqual(@as(u64, 50), second.now_ms);
}
