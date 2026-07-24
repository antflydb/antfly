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
};

pub const VirtualTime = struct {
    round: u64 = 0,
    now_ms: u64 = 0,
};

pub const Scheduler = struct {
    const QueueLinks = struct {
        prev: ?core.types.GroupId = null,
        next: ?core.types.GroupId = null,
        queued: bool = false,
    };

    const GroupState = struct {
        quiesced: bool = false,
        ready_visit_epoch: u64 = 0,
        fair_ready: QueueLinks = .{},
        continuation_ready: QueueLinks = .{},
    };

    const ReadyQueue = struct {
        head: ?core.types.GroupId = null,
        tail: ?core.types.GroupId = null,
        len: usize = 0,
    };

    pub const ReadyPassKind = enum {
        /// Prioritize queued hints, then audit every registered group once.
        fair,
        /// Consume only productive continuation hints present at pass start.
        continuation,
    };

    pub const ReadyPass = struct {
        kind: ReadyPassKind,
        epoch: u64,
        priority_remaining: usize,
        fallback_checked: usize = 0,
    };

    alloc: std.mem.Allocator,
    cfg: SchedulerConfig,
    time: VirtualTime = .{},
    group_ids: std.ArrayListUnmanaged(core.types.GroupId) = .empty,
    groups: std.AutoHashMapUnmanaged(core.types.GroupId, GroupState) = .empty,
    fair_ready: ReadyQueue = .{},
    continuation_ready: ReadyQueue = .{},
    active_group_count: usize = 0,
    ready_epoch: u64 = 0,
    ready_pass_active: bool = false,
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
        self.groups.deinit(self.alloc);
        self.* = undefined;
    }

    pub fn registerGroup(self: *Scheduler, group_id: core.types.GroupId) !void {
        std.debug.assert(!self.ready_pass_active);
        if (self.groups.contains(group_id)) return error.GroupAlreadyRegistered;

        try self.group_ids.ensureUnusedCapacity(self.alloc, 1);
        try self.groups.putNoClobber(self.alloc, group_id, .{});
        self.group_ids.appendAssumeCapacity(group_id);
        self.active_group_count += 1;
    }

    pub fn unregisterGroup(self: *Scheduler, group_id: core.types.GroupId) bool {
        std.debug.assert(!self.ready_pass_active);
        const state = self.groups.get(group_id) orelse return false;
        for (self.group_ids.items, 0..) |existing, i| {
            if (existing != group_id) continue;
            self.removeQueuedGroup(group_id);
            _ = self.group_ids.orderedRemove(i);
            _ = self.groups.remove(group_id);
            if (!state.quiesced) self.active_group_count -= 1;
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
        // Consensus timeouts are wall-clock obligations. Ready-work priority
        // must not let a busy group consume another group's election or
        // heartbeat tick in the same bounded host round.
        return self.nextRoundRobinGroup(&self.cursor);
    }

    pub fn beginReadyPass(self: *Scheduler, kind: ReadyPassKind) ReadyPass {
        std.debug.assert(!self.ready_pass_active);
        self.ready_pass_active = true;
        self.ready_epoch +%= 1;
        if (self.ready_epoch == 0) {
            var states = self.groups.valueIterator();
            while (states.next()) |state| state.ready_visit_epoch = 0;
            self.ready_epoch = 1;
        }
        // Hints appended while this pass runs belong to the next pass.
        return .{
            .kind = kind,
            .epoch = self.ready_epoch,
            .priority_remaining = self.queueFor(kind).len,
        };
    }

    pub fn nextReadyGroup(self: *Scheduler, pass: *ReadyPass) ?core.types.GroupId {
        std.debug.assert(self.ready_pass_active);
        std.debug.assert(pass.epoch == self.ready_epoch);

        while (pass.priority_remaining > 0) {
            pass.priority_remaining -= 1;
            const group_id = self.popReady(pass.kind) orelse break;
            const state = self.groups.getPtr(group_id) orelse continue;
            if (state.quiesced or state.ready_visit_epoch == pass.epoch) continue;
            state.ready_visit_epoch = pass.epoch;
            return group_id;
        }

        if (pass.kind == .fair) {
            // Hints are an optimization, not a correctness requirement. Probe
            // every group once so a dropped wakeup cannot strand Raft work.
            while (pass.fallback_checked < self.group_ids.items.len) {
                const group_id = self.group_ids.items[self.ready_cursor];
                self.ready_cursor = (self.ready_cursor + 1) % self.group_ids.items.len;
                pass.fallback_checked += 1;
                const state = self.groups.getPtr(group_id) orelse continue;
                if (state.quiesced or state.ready_visit_epoch == pass.epoch) continue;
                state.ready_visit_epoch = pass.epoch;
                return group_id;
            }
        }
        return null;
    }

    pub fn finishReadyPass(self: *Scheduler, pass: *ReadyPass) void {
        std.debug.assert(self.ready_pass_active);
        std.debug.assert(pass.epoch == self.ready_epoch);
        self.ready_pass_active = false;
        pass.* = undefined;
    }

    pub fn hasQueuedContinuation(self: *const Scheduler) bool {
        std.debug.assert(!self.ready_pass_active);
        return self.continuation_ready.len > 0;
    }

    pub fn quiesceGroup(self: *Scheduler, group_id: core.types.GroupId) !void {
        std.debug.assert(!self.ready_pass_active);
        const state = self.groups.getPtr(group_id) orelse return error.UnknownGroup;
        if (state.quiesced) return;
        state.quiesced = true;
        self.active_group_count -= 1;
        self.removeQueuedGroup(group_id);
    }

    pub fn resumeGroup(self: *Scheduler, group_id: core.types.GroupId) bool {
        std.debug.assert(!self.ready_pass_active);
        const state = self.groups.getPtr(group_id) orelse return false;
        if (!state.quiesced) return false;
        state.quiesced = false;
        self.active_group_count += 1;
        return true;
    }

    pub fn isQuiesced(self: *const Scheduler, group_id: core.types.GroupId) bool {
        const state = self.groups.get(group_id) orelse return false;
        return state.quiesced;
    }

    pub fn activeGroupCount(self: *const Scheduler) usize {
        return self.active_group_count;
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
        self.enqueueReady(group_id, .fair);
    }

    pub fn noteActivity(self: *Scheduler, group_id: core.types.GroupId) void {
        self.enqueueReady(group_id, .fair);
    }

    /// Defers a denied Ready frontier until the next fair host round.
    pub fn deferReady(self: *Scheduler, group_id: core.types.GroupId) void {
        self.unlinkReady(group_id, .continuation);
        self.enqueueReady(group_id, .fair);
    }

    /// Publishes the result of one successful Ready step. Only a frontier
    /// proven to have advanced may enter the same-round continuation queue.
    pub fn completeReady(self: *Scheduler, group_id: core.types.GroupId, has_more: bool) void {
        self.unlinkReady(group_id, .fair);
        self.unlinkReady(group_id, .continuation);
        if (has_more) self.enqueueReady(group_id, .continuation);
    }

    fn enqueueReady(self: *Scheduler, group_id: core.types.GroupId, kind: ReadyPassKind) void {
        const state = self.groups.getPtr(group_id) orelse return;
        if (state.quiesced) return;
        const links = linksFor(state, kind);
        if (links.queued) return;

        const queue = self.queueFor(kind);
        links.* = .{
            .prev = queue.tail,
            .queued = true,
        };
        if (queue.tail) |tail_id| {
            const tail_state = self.groups.getPtr(tail_id) orelse unreachable;
            linksFor(tail_state, kind).next = group_id;
        } else {
            queue.head = group_id;
        }
        queue.tail = group_id;
        queue.len += 1;
    }

    fn removeQueuedGroup(self: *Scheduler, group_id: core.types.GroupId) void {
        std.debug.assert(!self.ready_pass_active);
        self.unlinkReady(group_id, .fair);
        self.unlinkReady(group_id, .continuation);
    }

    fn popReady(self: *Scheduler, kind: ReadyPassKind) ?core.types.GroupId {
        const group_id = self.queueFor(kind).head orelse return null;
        self.unlinkReady(group_id, kind);
        return group_id;
    }

    fn unlinkReady(self: *Scheduler, group_id: core.types.GroupId, kind: ReadyPassKind) void {
        const state = self.groups.getPtr(group_id) orelse return;
        const links = linksFor(state, kind);
        if (!links.queued) return;

        const prev = links.prev;
        const next = links.next;
        const queue = self.queueFor(kind);
        if (prev) |prev_id| {
            const prev_state = self.groups.getPtr(prev_id) orelse unreachable;
            linksFor(prev_state, kind).next = next;
        } else {
            queue.head = next;
        }
        if (next) |next_id| {
            const next_state = self.groups.getPtr(next_id) orelse unreachable;
            linksFor(next_state, kind).prev = prev;
        } else {
            queue.tail = prev;
        }
        queue.len -= 1;
        links.* = .{};
    }

    fn queueFor(self: *Scheduler, kind: ReadyPassKind) *ReadyQueue {
        return switch (kind) {
            .fair => &self.fair_ready,
            .continuation => &self.continuation_ready,
        };
    }

    fn linksFor(state: *GroupState, kind: ReadyPassKind) *QueueLinks {
        return switch (kind) {
            .fair => &state.fair_ready,
            .continuation => &state.continuation_ready,
        };
    }

    fn nextRoundRobinGroup(self: *Scheduler, cursor: *usize) ?core.types.GroupId {
        if (self.group_ids.items.len == 0) return null;

        var checked: usize = 0;
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
    {
        var pass = scheduler.beginReadyPass(.fair);
        defer scheduler.finishReadyPass(&pass);
        try std.testing.expectEqual(@as(?core.types.GroupId, 1), scheduler.nextReadyGroup(&pass));
        try std.testing.expectEqual(@as(?core.types.GroupId, 3), scheduler.nextReadyGroup(&pass));
        try std.testing.expectEqual(@as(?core.types.GroupId, null), scheduler.nextReadyGroup(&pass));
    }

    try std.testing.expect(scheduler.resumeGroup(2));
    try std.testing.expectEqual(@as(?core.types.GroupId, 1), scheduler.nextTickGroup());
    try std.testing.expectEqual(@as(?core.types.GroupId, 2), scheduler.nextTickGroup());
}

test "scheduler prioritizes each ready group once per pass" {
    var scheduler = Scheduler.init(std.testing.allocator, .{});
    defer scheduler.deinit();

    try scheduler.registerGroup(1);
    try scheduler.registerGroup(2);
    try scheduler.registerGroup(3);

    for (0..8) |_| scheduler.noteActivity(3);
    var first_pass = scheduler.beginReadyPass(.fair);
    try std.testing.expectEqual(@as(?core.types.GroupId, 3), scheduler.nextReadyGroup(&first_pass));
    scheduler.completeReady(3, true);
    try std.testing.expectEqual(@as(?core.types.GroupId, 1), scheduler.nextReadyGroup(&first_pass));
    try std.testing.expectEqual(@as(?core.types.GroupId, 2), scheduler.nextReadyGroup(&first_pass));
    try std.testing.expectEqual(@as(?core.types.GroupId, null), scheduler.nextReadyGroup(&first_pass));
    scheduler.finishReadyPass(&first_pass);

    var second_pass = scheduler.beginReadyPass(.continuation);
    defer scheduler.finishReadyPass(&second_pass);
    try std.testing.expectEqual(@as(?core.types.GroupId, 3), scheduler.nextReadyGroup(&second_pass));
}

test "scheduler unregister normalizes ready cursor independently" {
    var scheduler = Scheduler.init(std.testing.allocator, .{});
    defer scheduler.deinit();

    try scheduler.registerGroup(1);
    try scheduler.registerGroup(2);
    try scheduler.registerGroup(3);

    {
        var pass = scheduler.beginReadyPass(.fair);
        defer scheduler.finishReadyPass(&pass);
        try std.testing.expectEqual(@as(?core.types.GroupId, 1), scheduler.nextReadyGroup(&pass));
        try std.testing.expectEqual(@as(?core.types.GroupId, 2), scheduler.nextReadyGroup(&pass));
        try std.testing.expectEqual(@as(?core.types.GroupId, 3), scheduler.nextReadyGroup(&pass));
    }

    try std.testing.expect(scheduler.unregisterGroup(1));
    var pass = scheduler.beginReadyPass(.fair);
    defer scheduler.finishReadyPass(&pass);
    try std.testing.expectEqual(@as(?core.types.GroupId, 2), scheduler.nextReadyGroup(&pass));
    try std.testing.expectEqual(@as(?core.types.GroupId, 3), scheduler.nextReadyGroup(&pass));
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
