// Copyright 2026 Antfly, Inc.
// SPDX-License-Identifier: Elastic-2.0

//! Per-DB admission barrier for revision-consistent native snapshots.
//!
//! Ownership belongs to an operation, never to an OS thread. Independent Io
//! tasks acquire independent leases. Nested helpers explicitly retain their
//! caller's lease, even when a waiting capture has closed reader admission.

const std = @import("std");
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

    pub fn acquireCapture(self: *@This()) CaptureLease {
        self.lock.lockExclusive();
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

const AdmissionVoprHarness = struct {
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
};

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
