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

//! Per-DB admission barrier for revision-consistent native snapshots.
//!
//! Ownership belongs to an operation, never to an OS thread. Independent Io
//! tasks acquire independent leases. Nested helpers explicitly retain their
//! caller's lease, even when a waiting capture has closed reader admission.

const std = @import("std");
const builtin = @import("builtin");
const apply_rw_lock_mod = @import("apply_rw_lock.zig");

pub const SnapshotAdmission = struct {
    lock: apply_rw_lock_mod.ApplyRwLock = .{},

    pub const MutationLease = struct {
        admission: *SnapshotAdmission,
        // Capture-owned helpers are scoped borrows: they must finish before
        // the capture releases. Ordinary shared retains have independent life.
        capture: ?*const CaptureLease = null,
        active: bool = true,

        pub fn retain(self: *const @This()) @This() {
            std.debug.assert(self.active);
            if (self.capture) |capture| {
                std.debug.assert(capture.active);
            } else {
                self.admission.lock.retainShared();
            }
            return .{ .admission = self.admission, .capture = self.capture };
        }

        pub fn release(self: *@This()) void {
            if (!self.active) return;
            if (self.capture) |capture| {
                std.debug.assert(capture.active);
            } else {
                self.admission.lock.unlockShared();
            }
            self.active = false;
        }

        pub fn deinit(self: *@This()) void {
            self.release();
        }
    };

    pub const CaptureLease = struct {
        admission: *SnapshotAdmission,
        active: bool = true,

        pub fn borrowMutation(self: *const @This()) MutationLease {
            std.debug.assert(self.active);
            return .{ .admission = self.admission, .capture = self };
        }

        pub fn release(self: *@This()) void {
            if (!self.active) return;
            self.admission.lock.unlockExclusive();
            self.active = false;
        }

        pub fn deinit(self: *@This()) void {
            self.release();
        }
    };

    pub fn acquireMutation(self: *@This()) MutationLease {
        self.lock.lockShared();
        return .{ .admission = self };
    }

    pub fn acquireMutationIo(self: *@This(), io: std.Io, cancellation: anytype) !MutationLease {
        try self.lock.lockSharedIo(io, cancellation);
        return .{ .admission = self };
    }

    /// Retaining an existing operation lease bypasses reader admission. A
    /// pending capture must not deadlock a nested helper behind its own lease.
    pub fn acquireMutationControlledIo(self: *@This(), io: std.Io, acquisition: apply_rw_lock_mod.ApplyRwLock.Acquisition, retained: ?*const MutationLease) !MutationLease {
        if (retained) |lease| {
            std.debug.assert(lease.active and lease.admission == self);
            try acquisition.lifetime.check();
            return lease.retain();
        }
        try self.lock.lockSharedControlledIo(io, acquisition);
        return .{ .admission = self };
    }

    /// Foreground, pre-decision acquisition only. A caller retaining other
    /// service leases may not lend CPU. It must reject contention rather than
    /// occupy capacity needed by the capture holder's original continuation.
    /// Mandatory apply/resolution callers retain acquireMutationControlledIo.
    pub fn acquireMutationPreDecisionControlledIo(self: *@This(), io: std.Io, acquisition: apply_rw_lock_mod.ApplyRwLock.Acquisition, retained: ?*const MutationLease) !MutationLease {
        if (retained != null or acquisition.execution_wait != null)
            return self.acquireMutationControlledIo(io, acquisition, retained);
        try acquisition.lifetime.check();
        if (!self.lock.tryLockShared()) return error.Backpressured;
        var lease: MutationLease = .{ .admission = self };
        errdefer lease.release();
        try acquisition.lifetime.check();
        return lease;
    }

    pub fn acquireCaptureControlledIo(self: *@This(), io: std.Io, acquisition: apply_rw_lock_mod.ApplyRwLock.Acquisition) !CaptureLease {
        try self.lock.lockExclusiveControlledIo(io, acquisition);
        return .{ .admission = self };
    }

    pub fn acquireCapture(self: *@This()) CaptureLease {
        self.lock.lockExclusive();
        return .{ .admission = self };
    }

    /// Distributed capture never waits for another owner while retaining a
    /// partial set of fences. The coordinator releases all acquired leases
    /// before parking/retrying when any owner cannot admit capture immediately.
    pub fn tryAcquireCapture(self: *@This()) ?CaptureLease {
        if (!self.lock.tryLockExclusive()) return null;
        return .{ .admission = self };
    }

    pub fn acquireCaptureIo(self: *@This(), io: std.Io, cancellation: anytype) !CaptureLease {
        try self.lock.lockExclusiveIo(io, cancellation);
        return .{ .admission = self };
    }
};

test "storage.db snapshot admission retained mutation survives its original lease" {
    var admission: SnapshotAdmission = .{};
    var outer = admission.acquireMutation();
    var inner = outer.retain();
    outer.release();
    try std.testing.expect(!admission.lock.tryLockExclusive());
    inner.release();
    try std.testing.expect(admission.lock.tryLockExclusive());
    admission.lock.unlockExclusive();
}

test "storage.db snapshot admission capture explicitly lends maintenance permission" {
    var admission: SnapshotAdmission = .{};
    var capture = admission.acquireCapture();
    defer capture.release();
    var maintenance = capture.borrowMutation();
    defer maintenance.release();
    var nested = maintenance.retain();
    defer nested.release();
    try std.testing.expect(!admission.lock.tryLockShared());
}

const AdmissionVoprHarness = if (builtin.is_test) struct {
    const vopr = @import("vopr");
    runtime: *vopr.vopr_io.VoprIo,

    fn resumeTask(self: @This(), future: std.Io.Future(anyerror!void)) !void {
        const actor = self.runtime.futureTaskSnapshot(future.any_future.?).?.id;
        var enabled: vopr.transition.List = .{};
        defer enabled.deinit(std.testing.allocator);
        var events: vopr.event.Sink = .{};
        defer events.deinit(std.testing.allocator);
        try self.runtime.scheduler().enumerateReady(&enabled, std.testing.allocator);
        for (enabled.items.items) |candidate| {
            if (candidate.actor_id == actor and std.mem.eql(u8, candidate.name, "vopr-io.task_resume")) {
                try self.runtime.scheduler().executeReady(candidate.id, &events, std.testing.allocator);
                return;
            }
        }
        return error.AdmissionTaskNotReady;
    }

    fn drain(self: @This()) !void {
        var enabled: vopr.transition.List = .{};
        defer enabled.deinit(std.testing.allocator);
        var events: vopr.event.Sink = .{};
        defer events.deinit(std.testing.allocator);
        for (0..64) |_| {
            if (self.runtime.scheduler().quiescent()) return;
            enabled.items.clearRetainingCapacity();
            try self.runtime.scheduler().enumerateReady(&enabled, std.testing.allocator);
            try enabled.canonicalize();
            if (enabled.items.items.len == 0) return error.AdmissionTasksBlocked;
            try self.runtime.scheduler().executeReady(enabled.items.items[0].id, &events, std.testing.allocator);
        }
        return error.AdmissionTasksDidNotComplete;
    }
} else struct {};

test "storage.db snapshot admission VOPR capture excludes another task and cancellation retires its waiter" {
    const Work = struct {
        admission: *SnapshotAdmission,
        io: std.Io,
        capture_active: bool = false,
        mutation_done: bool = false,
        mutation_overlapped: bool = false,

        fn capture(self: *@This()) anyerror!void {
            var lease = try self.admission.acquireCaptureIo(self.io, null);
            defer lease.release();
            self.capture_active = true;
            defer self.capture_active = false;
            // Both tasks run on this OS thread while capture owns admission.
            try self.io.sleep(.fromMilliseconds(10), .awake);
        }

        fn mutate(self: *@This()) anyerror!void {
            var lease = try self.admission.acquireMutationIo(self.io, null);
            defer lease.release();
            self.mutation_overlapped = self.capture_active;
            self.mutation_done = true;
        }
    };
    for ([_]bool{ false, true }) |cancel| {
        var runtime = try AdmissionVoprHarness.vopr.vopr_io.VoprIo.init(.{});
        defer runtime.deinit();
        const io = runtime.io();
        const harness: AdmissionVoprHarness = .{ .runtime = &runtime };
        var admission: SnapshotAdmission = .{ .lock = .{ .io = io } };
        var work: Work = .{ .admission = &admission, .io = io };
        var capture = io.async(Work.capture, .{&work});
        defer {
            _ = runtime.cancelAndDrainTasksForTeardown(std.testing.allocator, 64) catch @panic("admission cleanup failed");
            _ = capture.cancel(io) catch {};
        }
        try harness.resumeTask(capture);
        try std.testing.expect(work.capture_active);
        var mutation = io.async(Work.mutate, .{&work});
        defer {
            _ = runtime.cancelAndDrainTasksForTeardown(std.testing.allocator, 64) catch @panic("admission cleanup failed");
            _ = mutation.cancel(io) catch {};
        }
        try harness.resumeTask(mutation);
        try std.testing.expect(!work.mutation_done);
        try std.testing.expect(runtime.futureTaskSnapshot(mutation.any_future.?).?.waiting_on_futex);
        if (cancel) runtime.tasks.requestCancelAll();
        try harness.drain();
        if (cancel) {
            try std.testing.expectError(error.Canceled, capture.await(io));
            try std.testing.expectError(error.Canceled, mutation.await(io));
        } else {
            try capture.await(io);
            try mutation.await(io);
        }
        try std.testing.expectEqual(!cancel, work.mutation_done);
        try std.testing.expect(!work.mutation_overlapped);
        try std.testing.expectEqual(@as(u32, 0), admission.lock.parked_waiters.load(.acquire));
        try std.testing.expect(admission.lock.tryLockExclusive());
        admission.lock.unlockExclusive();
        try runtime.ensureNoCapabilityViolation();
    }
}

test "storage.db snapshot admission VOPR explicit retain passes a queued capture without admitting unrelated mutations" {
    const Work = struct {
        fn capture(admission: *SnapshotAdmission, io: std.Io, done: *bool) anyerror!void {
            var lease = try admission.acquireCaptureIo(io, null);
            defer lease.release();
            done.* = true;
        }
    };
    var runtime = try AdmissionVoprHarness.vopr.vopr_io.VoprIo.init(.{});
    defer runtime.deinit();
    const io = runtime.io();
    const harness: AdmissionVoprHarness = .{ .runtime = &runtime };
    var admission: SnapshotAdmission = .{ .lock = .{ .io = io } };
    var outer = try admission.acquireMutationIo(io, null);
    defer outer.release();
    var done = false;
    var capture = io.async(Work.capture, .{ &admission, io, &done });
    defer {
        outer.release();
        _ = runtime.cancelAndDrainTasksForTeardown(std.testing.allocator, 64) catch @panic("admission cleanup failed");
        _ = capture.cancel(io) catch {};
    }
    try harness.resumeTask(capture);
    try std.testing.expect(runtime.futureTaskSnapshot(capture.any_future.?).?.waiting_on_futex);
    // Retain does not re-enter the closed gate, so this cannot deadlock behind
    // the capture waiting for our original operation to complete.
    var inner = outer.retain();
    defer inner.release();
    outer.release();
    try std.testing.expect(!done);
    try std.testing.expect(!admission.lock.tryLockShared());
    inner.release();
    try harness.drain();
    try capture.await(io);
    try std.testing.expect(done);
    try std.testing.expectEqual(@as(i96, 0), std.Io.Clock.awake.now(io).nanoseconds);
}

test "storage.db snapshot admission lease can be released by another Io worker" {
    const Work = struct {
        fn release(lease: *SnapshotAdmission.MutationLease) void {
            lease.release();
        }
    };
    var admission: SnapshotAdmission = .{ .lock = .{ .io = std.testing.io } };
    var lease = admission.acquireMutation();
    defer lease.release();
    var task = try std.testing.io.concurrent(Work.release, .{&lease});
    task.await(std.testing.io);
    try std.testing.expect(admission.lock.tryLockExclusive());
    admission.lock.unlockExclusive();
}

test "storage.db snapshot admission controlled waits resume the original fence holder" {
    const native = @import("../../common/runnable_scheduler.zig");
    for ([_]bool{ false, true }) |capture_waits| {
        var scheduler = try native.Scheduler.init(.{ .slots = 2, .control_slots = 1 });
        defer scheduler.close();
        var holder: native.Execution = .{};
        defer holder.finish();
        var waiter: native.Execution = .{};
        defer waiter.finish();
        try holder.start(&scheduler, .{ .io = std.testing.io, .retained_bytes = 17 }, .query);
        var admission: SnapshotAdmission = .{};
        var mutation: ?SnapshotAdmission.MutationLease = if (capture_waits) admission.acquireMutation() else null;
        defer if (mutation) |*lease| lease.release();
        var capture: ?SnapshotAdmission.CaptureLease = if (capture_waits) null else admission.acquireCapture();
        defer if (capture) |*lease| lease.release();
        try holder.park(.resource_blocked);
        try waiter.start(&scheduler, .{ .io = std.testing.io, .retained_bytes = 23 }, .write);
        const State = struct {
            scheduler: *native.Scheduler,
            holder: *native.Execution,
            waiter: *native.Execution,
            mutation: *?SnapshotAdmission.MutationLease,
            capture: *?SnapshotAdmission.CaptureLease,
            begins: usize = 0,
            ends: usize = 0,
            fn check(raw: *const anyopaque) !void {
                const self: *const @This() = @ptrCast(@alignCast(raw));
                try self.waiter.checkLifetime();
            }
            fn begin(raw: *anyopaque) !void {
                const self: *@This() = @ptrCast(@alignCast(raw));
                self.begins += 1;
                try self.waiter.park(.resource_blocked);
                try std.testing.expectEqual(@as(usize, 0), self.scheduler.stats().runnable_slots);
                try std.testing.expectEqual(@as(usize, 2), self.scheduler.stats().operations);
                try std.testing.expectEqual(@as(u64, 40), self.scheduler.stats().retained_bytes);
                try self.holder.enter(.query);
                if (self.mutation.*) |*lease| lease.release();
                if (self.capture.*) |*lease| lease.release();
                self.holder.finish();
            }
            fn end(raw: *anyopaque) !void {
                const self: *@This() = @ptrCast(@alignCast(raw));
                self.ends += 1;
                try self.waiter.enter(.write);
            }
        };
        var state: State = .{ .scheduler = &scheduler, .holder = &holder, .waiter = &waiter, .mutation = &mutation, .capture = &capture };
        const controls: apply_rw_lock_mod.ApplyRwLock.Acquisition = .{
            .lifetime = .{ .context = &state, .check_fn = State.check },
            .execution_wait = .{ .ptr = &state, .begin = State.begin, .end = State.end },
        };
        if (capture_waits) {
            var lease = try admission.acquireCaptureControlledIo(std.testing.io, controls);
            try std.testing.expect(!admission.lock.tryLockShared());
            lease.release();
            lease.release();
        } else {
            var lease = try admission.acquireMutationControlledIo(std.testing.io, controls, null);
            try std.testing.expect(!admission.lock.tryLockExclusive());
            lease.release();
            lease.release();
        }
        try std.testing.expectEqual(@as(usize, 1), state.begins);
        try std.testing.expectEqual(state.begins, state.ends);
        waiter.finish();
        try std.testing.expectEqual(@as(usize, 0), scheduler.stats().operations);
        try std.testing.expectEqual(@as(u64, 0), scheduler.stats().retained_bytes);
        try std.testing.expectEqual(@as(usize, 0), scheduler.stats().runnable_slots);
        try std.testing.expect(admission.lock.tryLockExclusive());
        admission.lock.unlockExclusive();
    }
}

test "storage.db snapshot admission controlled nested retain keeps the original lease and lifetime" {
    const State = struct {
        checks: usize = 0,
        failure: ?anyerror = null,
        fn check(raw: *const anyopaque) !void {
            const self: *@This() = @ptrCast(@alignCast(@constCast(raw)));
            self.checks += 1;
            if (self.failure) |err| return err;
        }
        fn unexpectedWait(_: *anyopaque) !void {
            return error.UnexpectedNestedAdmissionWait;
        }
    };
    for ([_]bool{ false, true }) |capture_owned| {
        var admission: SnapshotAdmission = .{};
        var capture: ?SnapshotAdmission.CaptureLease = if (capture_owned) admission.acquireCapture() else null;
        defer if (capture) |*lease| lease.release();
        var outer = if (capture) |*lease| lease.borrowMutation() else admission.acquireMutation();
        defer outer.release();
        // A queued capture closes unrelated shared admission. Retaining the
        // active mutation must bypass it without acquiring another CPU owner.
        if (!capture_owned) admission.lock.exclusive_waiters.store(1, .release);
        defer if (!capture_owned) admission.lock.exclusive_waiters.store(0, .release);
        var state: State = .{};
        const controls: apply_rw_lock_mod.ApplyRwLock.Acquisition = .{
            .lifetime = .{ .context = &state, .check_fn = State.check },
            .execution_wait = .{ .ptr = &state, .begin = State.unexpectedWait, .end = State.unexpectedWait },
        };
        var inner = try admission.acquireMutationControlledIo(std.testing.io, controls, &outer);
        try std.testing.expectEqual(@as(usize, 1), state.checks);
        inner.release();
        for ([_]anyerror{ error.Timeout, error.Canceled, error.SchedulerClosed }) |cause| {
            state.failure = cause;
            try std.testing.expectError(cause, admission.acquireMutationControlledIo(std.testing.io, controls, &outer));
            try std.testing.expect(outer.active);
        }
        if (!capture_owned) admission.lock.exclusive_waiters.store(0, .release);
        outer.release();
        if (capture) |*lease| lease.release();
        try std.testing.expect(admission.lock.tryLockExclusive());
        admission.lock.unlockExclusive();
    }
}

pub fn qualifyPreDecisionCapacityCycle() !void {
    if (!builtin.is_test) @compileError("qualification requires a test build");
    const native = @import("../../common/runnable_scheduler.zig");
    const Mode = enum { reject_busy, can_wait, canceled, expired, closed, late_rejection, retained, mandatory };
    const Fixture = struct {
        capture: *SnapshotAdmission.CaptureLease,
        execution: *native.Execution,
        scheduler: *native.Scheduler,
        mode: Mode,
        checks: usize = 0,
        parks: usize = 0,
        fn check(raw: *const anyopaque) !void {
            const self: *@This() = @ptrCast(@alignCast(@constCast(raw)));
            self.checks += 1;
            switch (self.mode) {
                .canceled => return error.Canceled,
                .expired => return error.DeadlineExceeded,
                .closed => return error.SchedulerClosed,
                .late_rejection => if (self.checks >= 2) return error.Canceled,
                .mandatory => if (self.checks >= 2) self.capture.release(),
                else => {},
            }
            try self.execution.checkLifetime();
        }
        fn begin(raw: *anyopaque) !void {
            const self: *@This() = @ptrCast(@alignCast(raw));
            try std.testing.expectEqual(Mode.can_wait, self.mode);
            self.parks += 1;
            try self.execution.park(.resource_blocked);
            try std.testing.expectEqual(@as(usize, 0), self.scheduler.stats().runnable_slots);
            try std.testing.expectEqual(@as(usize, 2), self.scheduler.stats().operations);
            try std.testing.expectEqual(@as(u64, 40), self.scheduler.stats().retained_bytes);
            var health: native.Execution = .{};
            try health.start(self.scheduler, .{ .io = std.testing.io, .allow_wait = false }, .control);
            health.finish();
            self.capture.release();
        }
        fn end(raw: *anyopaque) !void {
            const self: *@This() = @ptrCast(@alignCast(raw));
            try self.execution.enter(.write);
        }
    };
    for ([_]Mode{ .reject_busy, .can_wait, .canceled, .expired, .closed, .late_rejection, .retained, .mandatory }) |mode| {
        var scheduler = try native.Scheduler.init(.{ .slots = 2 });
        var reader: native.Execution = .{};
        try reader.start(&scheduler, .{ .io = std.testing.io, .allow_wait = false, .retained_bytes = 17 }, .query);
        defer reader.finish();
        var admission: SnapshotAdmission = .{ .lock = .{ .io = std.testing.io } };
        var capture = admission.tryAcquireCapture() orelse return error.TestCaptureUnavailable;
        defer capture.release();
        try reader.park(.resource_blocked);
        var writer: native.Execution = .{};
        try writer.start(&scheduler, .{ .io = std.testing.io, .allow_wait = false, .retained_bytes = 23 }, .write);
        defer writer.finish();
        var fixture: Fixture = .{ .capture = &capture, .execution = &writer, .scheduler = &scheduler, .mode = mode };
        const controls: apply_rw_lock_mod.ApplyRwLock.Acquisition = .{ .lifetime = .{ .context = &fixture, .check_fn = Fixture.check }, .execution_wait = if (mode == .can_wait) .{ .ptr = &fixture, .begin = Fixture.begin, .end = Fixture.end } else null };
        var retained: ?SnapshotAdmission.MutationLease = if (mode == .retained) capture.borrowMutation() else null;
        defer if (retained) |*lease| lease.release();
        if (mode == .late_rejection) capture.release();
        const result = if (mode == .mandatory)
            admission.acquireMutationControlledIo(std.testing.io, controls, null)
        else
            admission.acquireMutationPreDecisionControlledIo(std.testing.io, controls, if (retained) |*lease| lease else null);
        if (mode == .can_wait or mode == .retained or mode == .mandatory) {
            var lease = try result;
            lease.release();
            if (mode == .retained) try std.testing.expect(capture.active);
        } else {
            try std.testing.expectError(switch (mode) {
                .reject_busy => error.Backpressured,
                .expired => error.DeadlineExceeded,
                .closed => error.SchedulerClosed,
                else => error.Canceled,
            }, result);
            if (mode != .late_rejection) try std.testing.expect(capture.active);
        }
        try std.testing.expectEqual(@as(usize, if (mode == .can_wait) 1 else 0), fixture.parks);
        try std.testing.expectEqual(@as(u64, 0), admission.lock.shared_waiters.load(.acquire));
        writer.finish();
        // Returning the non-lending writer's CPU lets the original capture
        // continuation resume without a fresh operation or extended lifetime.
        try reader.enter(.query);
        if (retained) |*lease| {
            lease.release();
            retained = null;
        }
        capture.release();
        var next = admission.tryAcquireCapture() orelse return error.TestLeakedMutationLease;
        next.release();
        reader.finish();
        try std.testing.expectEqual(@as(usize, 0), scheduler.stats().operations);
        try std.testing.expectEqual(@as(u64, 0), scheduler.stats().retained_bytes);
    }
}
