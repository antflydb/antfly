// Copyright 2026 Antfly, Inc.
// SPDX-License-Identifier: Apache-2.0

//! Pre-decision apply admission shared by batches, BEGIN and intent prepare.
//! Mandatory decision/resolution paths retain their separate completion owner.
const lock = @import("apply_rw_lock.zig");

pub fn lockApply(owner: anytype, controls: ?lock.ApplyRwLock.Acquisition) !void {
    if (controls) |original| {
        try owner.enforcePortableRuntimeGate();
        try owner.core.apply_mutex.lockExclusiveControlledIo(
            owner.core.apply_mutex.io orelse return error.MissingBackendRuntimeIo,
            original,
        );
        errdefer owner.core.unlockApply();
        // Activation may close admission while the operation is parked.
        try owner.enforcePortableRuntimeGate();
    } else {
        try owner.lockApplyForPortableRuntime();
    }
}

test "mutation apply admission rechecks activation after original CPU resumption" {
    const std = @import("std");
    const native = @import("../../common/runnable_scheduler.zig");
    const Core = struct {
        apply_mutex: *lock.ApplyRwLock,
        pub fn unlockApply(self: *@This()) void {
            self.apply_mutex.unlockExclusive();
        }
    };
    const Owner = struct {
        core: Core,
        closed: bool = false,
        gates: usize = 0,
        pub fn enforcePortableRuntimeGate(self: *@This()) !void {
            self.gates += 1;
            if (self.closed) return error.PortableRuntimeActivationInProgress;
        }
        pub fn lockApplyForPortableRuntime(self: *@This()) !void {
            try self.enforcePortableRuntimeGate();
            self.core.apply_mutex.lockExclusive();
            errdefer self.core.unlockApply();
            try self.enforcePortableRuntimeGate();
        }
    };
    var scheduler = try native.Scheduler.init(.{ .slots = 2, .control_slots = 1 });
    defer scheduler.close();
    var holder: native.Execution = .{};
    defer holder.finish();
    var writer: native.Execution = .{};
    defer writer.finish();
    try holder.start(&scheduler, .{ .io = std.testing.io, .retained_bytes = 17 }, .query);
    var mutex: lock.ApplyRwLock = .{ .io = std.testing.io };
    mutex.lockShared();
    var held = true;
    defer if (held) mutex.unlockShared();
    try holder.park(.resource_blocked);
    try writer.start(&scheduler, .{ .io = std.testing.io, .retained_bytes = 23 }, .write);
    var owner: Owner = .{ .core = .{ .apply_mutex = &mutex } };
    const State = struct {
        owner: *Owner,
        scheduler: *native.Scheduler,
        holder: *native.Execution,
        writer: *native.Execution,
        held: *bool,
        parks: usize = 0,
        resumes: usize = 0,
        fn check(raw: *const anyopaque) !void {
            const self: *const @This() = @ptrCast(@alignCast(raw));
            try self.writer.checkLifetime();
        }
        fn begin(raw: *anyopaque) !void {
            const self: *@This() = @ptrCast(@alignCast(raw));
            self.parks += 1;
            try self.writer.park(.resource_blocked);
            try std.testing.expectEqual(@as(usize, 0), self.scheduler.stats().runnable_slots);
            try std.testing.expectEqual(@as(usize, 2), self.scheduler.stats().operations);
            try std.testing.expectEqual(@as(u64, 40), self.scheduler.stats().retained_bytes);
            try self.holder.enter(.query);
            self.owner.closed = true;
            self.owner.core.apply_mutex.unlockShared();
            self.held.* = false;
            self.holder.finish();
        }
        fn end(raw: *anyopaque) !void {
            const self: *@This() = @ptrCast(@alignCast(raw));
            self.resumes += 1;
            try self.writer.enter(.write);
        }
    };
    var state: State = .{ .owner = &owner, .scheduler = &scheduler, .holder = &holder, .writer = &writer, .held = &held };
    try std.testing.expectError(error.PortableRuntimeActivationInProgress, lockApply(&owner, .{
        .lifetime = .{ .context = &state, .check_fn = State.check },
        .execution_wait = .{ .ptr = &state, .begin = State.begin, .end = State.end },
    }));
    try std.testing.expectEqual(@as(usize, 2), owner.gates);
    try std.testing.expectEqual(@as(usize, 1), state.parks);
    try std.testing.expectEqual(state.parks, state.resumes);
    try std.testing.expect(mutex.tryLockExclusive());
    mutex.unlockExclusive();
    writer.finish();
    try std.testing.expectEqual(@as(usize, 0), scheduler.stats().operations);
    try std.testing.expectEqual(@as(usize, 0), scheduler.stats().runnable_slots);
    try std.testing.expectEqual(@as(u64, 0), scheduler.stats().retained_bytes);
}

test "mutation apply admission original rejection and missing Io leave legacy fences reusable" {
    const std = @import("std");
    const Core = struct {
        apply_mutex: *lock.ApplyRwLock,
        pub fn unlockApply(self: *@This()) void {
            self.apply_mutex.unlockExclusive();
        }
    };
    const Owner = struct {
        core: Core,
        pub fn enforcePortableRuntimeGate(_: *@This()) !void {}
        pub fn lockApplyForPortableRuntime(self: *@This()) !void {
            self.core.apply_mutex.lockExclusive();
        }
    };
    const State = struct {
        checks: usize = 0,
        fail_at: usize,
        cause: anyerror,
        fn check(raw: *const anyopaque) !void {
            const self: *@This() = @ptrCast(@alignCast(@constCast(raw)));
            self.checks += 1;
            if (self.checks >= self.fail_at) return self.cause;
        }
    };
    var mutex: lock.ApplyRwLock = .{ .io = std.testing.io };
    var owner: Owner = .{ .core = .{ .apply_mutex = &mutex } };
    for ([_]anyerror{ error.Canceled, error.DeadlineExceeded, error.SchedulerClosed }) |cause| {
        for (1..4) |fail_at| {
            var state: State = .{ .fail_at = fail_at, .cause = cause };
            try std.testing.expectError(cause, lockApply(&owner, .{ .lifetime = .{ .context = &state, .check_fn = State.check } }));
            try std.testing.expect(mutex.tryLockExclusive());
            mutex.unlockExclusive();
        }
    }
    var state: State = .{ .fail_at = 99, .cause = error.Canceled };
    mutex.io = null;
    try std.testing.expectError(error.MissingBackendRuntimeIo, lockApply(&owner, .{ .lifetime = .{ .context = &state, .check_fn = State.check } }));
    try std.testing.expectEqual(@as(usize, 0), state.checks);
    try lockApply(&owner, null);
    owner.core.unlockApply();
    try std.testing.expect(mutex.tryLockExclusive());
    mutex.unlockExclusive();
}
