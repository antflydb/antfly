// Copyright 2026 Antfly, Inc.
//
// Licensed under the Elastic License 2.0 (ELv2); you may not use this file
// except in compliance with the Elastic License 2.0. You may obtain a copy of
// the Elastic License 2.0 at
//
//     https://www.antfly.io/licensing/ELv2-license
//
// Unless required by applicable law or agreed to in writing, software distributed
// under the Elastic License 2.0 is distributed on an "AS IS" BASIS, WITHOUT
// WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
// Elastic License 2.0 for the specific language governing permissions and
// limitations.

const std = @import("std");
const Allocator = std.mem.Allocator;
const resource_manager_mod = @import("../../resource_manager.zig");

pub const Tracker = struct {
    const Entry = struct {
        sequence: u64,
        bytes: u64,
    };

    resource_manager: ?*resource_manager_mod.ResourceManager = null,
    entries: std.ArrayListUnmanaged(Entry) = .empty,
    accounted_bytes: u64 = 0,
    // Once entry allocation fails, retain exact aggregate accounting and force
    // producers to drain through the newest affected sequence. We deliberately
    // do not attempt to allocate again until that range is released: memory
    // pressure must make admission more conservative, never silently disable it.
    overflow_first_sequence: ?u64 = null,
    overflow_last_sequence: u64 = 0,
    overflow_bytes: u64 = 0,

    pub fn init(resource_manager: ?*resource_manager_mod.ResourceManager) Tracker {
        return .{ .resource_manager = resource_manager };
    }

    pub fn deinit(self: *Tracker, alloc: Allocator) void {
        self.observe(0);
        self.entries.deinit(alloc);
        self.* = undefined;
    }

    pub fn track(self: *Tracker, alloc: Allocator, sequence: u64, bytes: u64) !void {
        if (self.resource_manager == null or bytes == 0) return;
        if (self.overflow_first_sequence != null) {
            self.overflow_last_sequence = @max(self.overflow_last_sequence, sequence);
            self.overflow_bytes +|= bytes;
            self.observe(self.accounted_bytes +| bytes);
            return;
        }
        self.entries.append(alloc, .{
            .sequence = sequence,
            .bytes = bytes,
        }) catch {
            self.overflow_first_sequence = sequence;
            self.overflow_last_sequence = sequence;
            self.overflow_bytes = bytes;
            self.observe(self.accounted_bytes +| bytes);
            return;
        };
        self.observe(self.accounted_bytes +| bytes);
    }

    pub fn releaseThrough(self: *Tracker, sequence: u64) void {
        if (self.resource_manager == null) return;
        var write_index: usize = 0;
        var released: u64 = 0;
        for (self.entries.items) |entry| {
            if (entry.sequence <= sequence) {
                released +|= entry.bytes;
                continue;
            }
            self.entries.items[write_index] = entry;
            write_index += 1;
        }
        self.entries.items.len = write_index;
        if (self.overflow_first_sequence != null and sequence >= self.overflow_last_sequence) {
            released +|= self.overflow_bytes;
            self.overflow_first_sequence = null;
            self.overflow_last_sequence = 0;
            self.overflow_bytes = 0;
        }
        if (released == 0) return;
        self.observe(self.accounted_bytes -| released);
    }

    /// Returns the oldest target that drains debt below both the sequence and
    /// byte low-water marks. Producers wait for this bounded target rather
    /// than draining through the newest write, preserving useful pipelining.
    pub fn throttleTargetSequence(self: *Tracker) ?u64 {
        const manager = self.resource_manager orelse return null;
        // Without per-sequence allocation we cannot safely calculate a partial
        // low-water target. Draining the aggregate range is bounded by the
        // committed replay head and restores precise accounting.
        if (self.overflow_first_sequence != null) return self.overflow_last_sequence;
        if (self.entries.items.len == 0) return null;

        var release_count: usize = 0;
        const limits = manager.derivedBacklogLimits();
        if (limits.high_sequences > 0 and self.entries.items.len > limits.high_sequences) {
            release_count = self.entries.items.len - @min(limits.resume_sequences, self.entries.items.len);
        }

        const stats = manager.sliceStats(.derived_backlog);
        const throttle_bytes = pressureRequestsProducerThrottle(stats);
        if (throttle_bytes) {
            const low_water_bytes = if (stats.soft_limit_bytes > 0)
                stats.soft_limit_bytes * 3 / 4
            else
                stats.hard_limit_bytes * 3 / 4;
            var remaining_bytes = self.accounted_bytes;
            var byte_release_count: usize = 0;
            while (byte_release_count < self.entries.items.len and remaining_bytes > low_water_bytes) : (byte_release_count += 1) {
                remaining_bytes -|= self.entries.items[byte_release_count].bytes;
            }
            release_count = @max(release_count, byte_release_count);
        }

        // A replay cursor temporarily pins primary LSM read state while a
        // bounded window is being applied. When aggregate LSM state reaches
        // its policy threshold, wait for at least the oldest pending sequence;
        // completion closes the window/cursor and releases that pinned state.
        // This is adaptive to actual bytes and complements the sequence cap.
        if (pressureRequestsProducerThrottle(manager.sliceStats(.lsm_in_memory_state))) {
            release_count = @max(release_count, 1);
        }

        if (release_count == 0) return null;
        return self.entries.items[release_count - 1].sequence;
    }

    fn pressureRequestsProducerThrottle(stats: resource_manager_mod.SliceStats) bool {
        return switch (stats.pressure) {
            .normal => false,
            .soft => stats.soft_action == .throttle_writes or stats.soft_action == .reject_work,
            .hard => stats.hard_action == .throttle_writes or stats.hard_action == .reject_work,
        };
    }

    fn observe(self: *Tracker, bytes: u64) void {
        const manager = self.resource_manager orelse return;
        manager.observeUsage(.derived_backlog, &self.accounted_bytes, bytes);
    }
};

test "derived backlog tracker accounts and releases payload bytes" {
    var budgets = resource_manager_mod.Options.defaultBudgets();
    budgets[@intFromEnum(resource_manager_mod.Slice.derived_backlog)] = .{
        .soft_limit_bytes = 10,
        .hard_limit_bytes = 20,
    };
    var manager = resource_manager_mod.ResourceManager.init(.{ .budgets = budgets });
    var tracker = Tracker.init(&manager);
    defer tracker.deinit(std.testing.allocator);

    try tracker.track(std.testing.allocator, 1, 8);
    try tracker.track(std.testing.allocator, 2, 15);
    var stats = manager.snapshot();
    try std.testing.expectEqual(@as(u64, 23), stats.slices[@intFromEnum(resource_manager_mod.Slice.derived_backlog)].used_bytes);
    try std.testing.expectEqual(@as(u64, 1), stats.slices[@intFromEnum(resource_manager_mod.Slice.derived_backlog)].soft_limit_events);
    try std.testing.expectEqual(@as(u64, 1), stats.slices[@intFromEnum(resource_manager_mod.Slice.derived_backlog)].hard_limit_rejections);

    tracker.releaseThrough(1);
    stats = manager.snapshot();
    try std.testing.expectEqual(@as(u64, 15), stats.slices[@intFromEnum(resource_manager_mod.Slice.derived_backlog)].used_bytes);

    tracker.releaseThrough(2);
    stats = manager.snapshot();
    try std.testing.expectEqual(@as(u64, 0), stats.slices[@intFromEnum(resource_manager_mod.Slice.derived_backlog)].used_bytes);
}

test "derived backlog tracker reports throttle pressure" {
    var budgets = resource_manager_mod.Options.defaultBudgets();
    budgets[@intFromEnum(resource_manager_mod.Slice.derived_backlog)] = .{
        .soft_limit_bytes = 10,
        .hard_limit_bytes = 20,
    };
    var manager = resource_manager_mod.ResourceManager.init(.{ .budgets = budgets });
    var tracker = Tracker.init(&manager);
    defer tracker.deinit(std.testing.allocator);

    try std.testing.expectEqual(@as(?u64, null), tracker.throttleTargetSequence());
    try tracker.track(std.testing.allocator, 1, 11);
    try std.testing.expectEqual(@as(?u64, 1), tracker.throttleTargetSequence());
    tracker.releaseThrough(1);
    try std.testing.expectEqual(@as(?u64, null), tracker.throttleTargetSequence());
}

test "derived backlog tracker fails closed when sequence accounting allocation fails" {
    var budgets = resource_manager_mod.Options.defaultBudgets();
    budgets[@intFromEnum(resource_manager_mod.Slice.derived_backlog)] = .{
        .soft_limit_bytes = 10,
        .hard_limit_bytes = 20,
    };
    var manager = resource_manager_mod.ResourceManager.init(.{ .budgets = budgets });
    var tracker = Tracker.init(&manager);
    defer tracker.deinit(std.testing.allocator);

    var failing = std.testing.FailingAllocator.init(std.testing.allocator, .{ .fail_index = 0 });
    try tracker.track(failing.allocator(), 7, 12);
    try tracker.track(failing.allocator(), 8, 13);

    try std.testing.expectEqual(@as(?u64, 8), tracker.throttleTargetSequence());
    try std.testing.expectEqual(@as(u64, 25), manager.sliceStats(.derived_backlog).used_bytes);

    tracker.releaseThrough(7);
    try std.testing.expectEqual(@as(?u64, 8), tracker.throttleTargetSequence());
    try std.testing.expectEqual(@as(u64, 25), manager.sliceStats(.derived_backlog).used_bytes);

    tracker.releaseThrough(8);
    try std.testing.expectEqual(@as(?u64, null), tracker.throttleTargetSequence());
    try std.testing.expectEqual(@as(u64, 0), manager.sliceStats(.derived_backlog).used_bytes);
}

test "derived backlog tracker applies sequence high and low water marks" {
    var manager = resource_manager_mod.ResourceManager.init(.{
        .derived_backlog_high_sequences = 4,
        .derived_backlog_resume_sequences = 2,
    });
    var tracker = Tracker.init(&manager);
    defer tracker.deinit(std.testing.allocator);

    for (1..6) |sequence| try tracker.track(std.testing.allocator, sequence, 1);
    try std.testing.expectEqual(@as(?u64, 3), tracker.throttleTargetSequence());
    tracker.releaseThrough(3);
    try std.testing.expectEqual(@as(?u64, null), tracker.throttleTargetSequence());
}

test "derived backlog tracker reacts to aggregate lsm state pressure" {
    var budgets = resource_manager_mod.Options.defaultBudgets();
    budgets[@intFromEnum(resource_manager_mod.Slice.lsm_in_memory_state)] = .{
        .soft_limit_bytes = 10,
        .hard_limit_bytes = 20,
    };
    var manager = resource_manager_mod.ResourceManager.init(.{
        .budgets = budgets,
        .derived_backlog_high_sequences = 0,
    });
    var tracker = Tracker.init(&manager);
    defer tracker.deinit(std.testing.allocator);

    try tracker.track(std.testing.allocator, 7, 1);
    var lsm_bytes: u64 = 0;
    manager.observeUsage(.lsm_in_memory_state, &lsm_bytes, 21);
    try std.testing.expectEqual(@as(?u64, 7), tracker.throttleTargetSequence());
    manager.observeUsage(.lsm_in_memory_state, &lsm_bytes, 0);
    try std.testing.expectEqual(@as(?u64, null), tracker.throttleTargetSequence());
}
