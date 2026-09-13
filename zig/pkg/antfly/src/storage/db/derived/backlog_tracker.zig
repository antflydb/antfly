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
        retained_bytes: u64,
        reservation: ?resource_manager_mod.Reservation = null,
    };

    /// Account the durable payload and the per-sequence ownership needed to
    /// release it independently. This makes the byte budget a cardinality
    /// bound too: a stream of empty or tiny replay records cannot grow the
    /// tracker indefinitely while reporting negligible retained memory.
    fn retainedCharge(payload_bytes: u64) u64 {
        return payload_bytes +| @sizeOf(Entry);
    }

    /// A source mutation reserves its replay-retention bytes before the
    /// primary WAL commit. The credit is then transferred to the tracker; it
    /// must never be copied and independently released by a caller.
    pub const Admission = struct {
        reservation: ?resource_manager_mod.Reservation = null,
        retained_bytes: u64 = 0,

        pub fn cancel(self: *Admission) void {
            if (self.reservation) |*reservation| reservation.release();
            self.* = .{};
        }
    };

    resource_manager: ?*resource_manager_mod.ResourceManager = null,
    entries: std.ArrayListUnmanaged(Entry) = .empty,
    retained_bytes: u64 = 0,
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
        for (self.entries.items) |*entry| {
            if (entry.reservation) |*reservation| reservation.release();
        }
        self.observe(0);
        self.entries.deinit(alloc);
        self.* = undefined;
    }

    pub fn track(self: *Tracker, alloc: Allocator, sequence: u64, bytes: u64) !void {
        if (self.resource_manager == null) return;
        const retained_bytes = retainedCharge(bytes);
        if (self.overflow_first_sequence != null) {
            self.overflow_last_sequence = @max(self.overflow_last_sequence, sequence);
            self.overflow_bytes +|= retained_bytes;
            self.retained_bytes +|= retained_bytes;
            self.observe(self.accounted_bytes +| retained_bytes);
            return;
        }
        self.entries.append(alloc, .{
            .sequence = sequence,
            .retained_bytes = retained_bytes,
        }) catch {
            self.overflow_first_sequence = sequence;
            self.overflow_last_sequence = sequence;
            self.overflow_bytes = retained_bytes;
            self.retained_bytes +|= retained_bytes;
            self.observe(self.accounted_bytes +| retained_bytes);
            return;
        };
        self.retained_bytes +|= retained_bytes;
        self.observe(self.accounted_bytes +| retained_bytes);
    }

    /// Reserves exact node-wide replay capacity at the last fallible boundary
    /// before a source mutation becomes durable. Source mutations for one DB
    /// are serialized, so reserving list capacity here guarantees the later
    /// post-commit transfer is allocation-free.
    pub fn admit(self: *Tracker, alloc: Allocator, bytes: u64) !Admission {
        const manager = self.resource_manager orelse return .{};
        try self.entries.ensureUnusedCapacity(alloc, 1);
        const retained_bytes = retainedCharge(bytes);
        return .{
            .reservation = try manager.reserveImmediate(.derived_backlog, retained_bytes),
            .retained_bytes = retained_bytes,
        };
    }

    /// Transfers an admitted credit after the primary WAL and replay intent
    /// commit atomically. This operation is deliberately allocation-free and
    /// cannot fail after the source mutation is durable.
    pub fn commitAdmission(self: *Tracker, sequence: u64, admission: *Admission) void {
        if (admission.reservation == null) {
            admission.* = .{};
            return;
        }
        self.entries.appendAssumeCapacity(.{
            .sequence = sequence,
            .retained_bytes = admission.retained_bytes,
            .reservation = admission.reservation,
        });
        self.retained_bytes +|= admission.retained_bytes;
        admission.reservation = null;
        admission.retained_bytes = 0;
    }

    pub fn releaseThrough(self: *Tracker, sequence: u64) void {
        if (self.resource_manager == null) return;
        var write_index: usize = 0;
        var released_observed: u64 = 0;
        var released_total: u64 = 0;
        for (self.entries.items) |*entry| {
            if (entry.sequence <= sequence) {
                released_total +|= entry.retained_bytes;
                if (entry.reservation) |*reservation|
                    reservation.release()
                else
                    released_observed +|= entry.retained_bytes;
                continue;
            }
            self.entries.items[write_index] = entry.*;
            write_index += 1;
        }
        self.entries.items.len = write_index;
        if (self.overflow_first_sequence != null and sequence >= self.overflow_last_sequence) {
            released_total +|= self.overflow_bytes;
            released_observed +|= self.overflow_bytes;
            self.overflow_first_sequence = null;
            self.overflow_last_sequence = 0;
            self.overflow_bytes = 0;
        }
        if (released_total == 0) return;
        self.retained_bytes -|= released_total;
        if (released_observed != 0) self.observe(self.accounted_bytes -| released_observed);
    }

    /// Returns the oldest bounded target that reduces admission debt. Sequence
    /// pressure is drained in latency-sized windows; byte pressure may request
    /// a larger target to restore its low-water mark. Producers never drain
    /// through the newest write merely because the sequence count is high.
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
            const to_resume = self.entries.items.len - @min(limits.resume_sequences, self.entries.items.len);
            release_count = if (limits.throttle_window_sequences == 0)
                to_resume
            else
                @min(to_resume, limits.throttle_window_sequences);
        }

        const stats = manager.sliceStats(.derived_backlog);
        const throttle_bytes = pressureRequestsProducerThrottle(stats);
        if (throttle_bytes) {
            const low_water_bytes = if (stats.soft_limit_bytes > 0)
                stats.soft_limit_bytes * 3 / 4
            else
                stats.hard_limit_bytes * 3 / 4;
            var remaining_bytes = self.retained_bytes;
            var byte_release_count: usize = 0;
            while (byte_release_count < self.entries.items.len and remaining_bytes > low_water_bytes) : (byte_release_count += 1) {
                remaining_bytes -|= self.entries.items[byte_release_count].retained_bytes;
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

test "derived backlog tracker accounts payload and sequence ownership" {
    const first_charge = Tracker.retainedCharge(8);
    const second_charge = Tracker.retainedCharge(15);
    var budgets = resource_manager_mod.Options.defaultBudgets();
    budgets[@intFromEnum(resource_manager_mod.Slice.derived_backlog)] = .{
        .soft_limit_bytes = first_charge,
        .hard_limit_bytes = first_charge + second_charge - 1,
    };
    var manager = resource_manager_mod.ResourceManager.init(.{ .budgets = budgets });
    var tracker = Tracker.init(&manager);
    defer tracker.deinit(std.testing.allocator);

    try tracker.track(std.testing.allocator, 1, 8);
    try tracker.track(std.testing.allocator, 2, 15);
    var stats = manager.snapshot();
    try std.testing.expectEqual(first_charge + second_charge, stats.slices[@intFromEnum(resource_manager_mod.Slice.derived_backlog)].used_bytes);
    try std.testing.expectEqual(@as(u64, 1), stats.slices[@intFromEnum(resource_manager_mod.Slice.derived_backlog)].soft_limit_events);
    try std.testing.expectEqual(@as(u64, 1), stats.slices[@intFromEnum(resource_manager_mod.Slice.derived_backlog)].hard_limit_rejections);

    tracker.releaseThrough(1);
    stats = manager.snapshot();
    try std.testing.expectEqual(second_charge, stats.slices[@intFromEnum(resource_manager_mod.Slice.derived_backlog)].used_bytes);

    tracker.releaseThrough(2);
    stats = manager.snapshot();
    try std.testing.expectEqual(@as(u64, 0), stats.slices[@intFromEnum(resource_manager_mod.Slice.derived_backlog)].used_bytes);
}

test "derived backlog tracker reports throttle pressure" {
    const charge = Tracker.retainedCharge(11);
    var budgets = resource_manager_mod.Options.defaultBudgets();
    budgets[@intFromEnum(resource_manager_mod.Slice.derived_backlog)] = .{
        .soft_limit_bytes = charge - 1,
        .hard_limit_bytes = charge * 2,
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

test "derived backlog admission transfers exact precommit capacity" {
    const admitted_charge = Tracker.retainedCharge(9);
    var budgets = resource_manager_mod.Options.defaultBudgets();
    budgets[@intFromEnum(resource_manager_mod.Slice.derived_backlog)] = .{
        .soft_limit_bytes = admitted_charge - 1,
        .hard_limit_bytes = admitted_charge + Tracker.retainedCharge(4) - 1,
    };
    var manager = resource_manager_mod.ResourceManager.init(.{ .budgets = budgets });
    var tracker = Tracker.init(&manager);
    defer tracker.deinit(std.testing.allocator);

    var admission = try tracker.admit(std.testing.allocator, 9);
    defer admission.cancel();
    try std.testing.expectEqual(admitted_charge, manager.sliceStats(.derived_backlog).used_bytes);
    try std.testing.expectError(error.ResourceBudgetExceeded, tracker.admit(std.testing.allocator, 4));

    tracker.commitAdmission(7, &admission);
    try std.testing.expect(admission.reservation == null);
    tracker.releaseThrough(6);
    try std.testing.expectEqual(admitted_charge, manager.sliceStats(.derived_backlog).used_bytes);
    tracker.releaseThrough(7);
    try std.testing.expectEqual(@as(u64, 0), manager.sliceStats(.derived_backlog).used_bytes);
}

test "derived backlog admission bounds empty record cardinality" {
    const record_charge = Tracker.retainedCharge(0);
    var budgets = resource_manager_mod.Options.defaultBudgets();
    budgets[@intFromEnum(resource_manager_mod.Slice.derived_backlog)] = .{
        .hard_limit_bytes = record_charge,
    };
    var manager = resource_manager_mod.ResourceManager.init(.{ .budgets = budgets });
    var tracker = Tracker.init(&manager);
    defer tracker.deinit(std.testing.allocator);

    var first = try tracker.admit(std.testing.allocator, 0);
    defer first.cancel();
    tracker.commitAdmission(1, &first);
    try std.testing.expectError(error.ResourceBudgetExceeded, tracker.admit(std.testing.allocator, 0));
    tracker.releaseThrough(1);
    try std.testing.expectEqual(@as(u64, 0), manager.sliceStats(.derived_backlog).used_bytes);
}

test "derived backlog tracker fails closed when sequence accounting allocation fails" {
    const first_charge = Tracker.retainedCharge(12);
    const second_charge = Tracker.retainedCharge(13);
    var budgets = resource_manager_mod.Options.defaultBudgets();
    budgets[@intFromEnum(resource_manager_mod.Slice.derived_backlog)] = .{
        .soft_limit_bytes = first_charge - 1,
        .hard_limit_bytes = first_charge + second_charge - 1,
    };
    var manager = resource_manager_mod.ResourceManager.init(.{ .budgets = budgets });
    var tracker = Tracker.init(&manager);
    defer tracker.deinit(std.testing.allocator);

    var failing = std.testing.FailingAllocator.init(std.testing.allocator, .{ .fail_index = 0 });
    try tracker.track(failing.allocator(), 7, 12);
    try tracker.track(failing.allocator(), 8, 13);

    try std.testing.expectEqual(@as(?u64, 8), tracker.throttleTargetSequence());
    try std.testing.expectEqual(first_charge + second_charge, manager.sliceStats(.derived_backlog).used_bytes);

    tracker.releaseThrough(7);
    try std.testing.expectEqual(@as(?u64, 8), tracker.throttleTargetSequence());
    try std.testing.expectEqual(first_charge + second_charge, manager.sliceStats(.derived_backlog).used_bytes);

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

test "derived backlog tracker bounds sequence-only admission drain window" {
    var manager = resource_manager_mod.ResourceManager.init(.{
        .derived_backlog_high_sequences = 8,
        .derived_backlog_resume_sequences = 4,
        .derived_backlog_throttle_window_sequences = 2,
    });
    var tracker = Tracker.init(&manager);
    defer tracker.deinit(std.testing.allocator);

    for (1..11) |sequence| try tracker.track(std.testing.allocator, sequence, 1);
    try std.testing.expectEqual(@as(?u64, 2), tracker.throttleTargetSequence());
    tracker.releaseThrough(2);
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
