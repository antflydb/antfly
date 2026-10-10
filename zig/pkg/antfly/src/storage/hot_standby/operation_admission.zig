// Copyright 2026 Antfly, Inc.
// SPDX-License-Identifier: Elastic-2.0

//! One synchronous HA operation borrows its authenticated ingress execution.
//! Receivers and budget stay at stable addresses until all physical work returns.
const std = @import("std");
const runtime = @import("antfly_runtime_abi");
const sources = @import("antfly_local_sources");

pub const Control = struct {
    execution: runtime.runnable_abi.Borrowed,
    io: runtime.io_abi.Borrow,
};

const Controls = struct {
    original: runtime.runnable_abi.Borrowed,
    execution_runnable: ?runtime.runnable_abi.Borrowed,
};
fn check(controls: Controls) !void {
    try controls.original.checkLifetime();
}
const Budget = sources.common_read_execution_budget.Budget(Controls, check);

pub const Session = struct {
    receiver: runtime.io_abi.Receiver = undefined,
    budget: Budget = undefined,

    pub fn bind(self: *Session, control: Control) !void {
        try control.execution.validate();
        self.receiver = try control.io.receive();
        self.budget = .{ .controls = .{ .original = control.execution, .execution_runnable = control.execution }, .io = self.receiver.io() };
        try self.budget.checkpoint();
    }
    pub fn checkpoint(self: *Session) !void {
        try self.budget.checkpoint();
    }
    pub fn lifetime(self: *Session) runtime.runnable_abi.Reentry {
        return .{ .context = self, .check = checkLifetime };
    }
    fn checkLifetime(raw: *anyopaque) callconv(.c) runtime.error_abi.Status {
        const self: *Session = @ptrCast(@alignCast(raw));
        self.checkpoint() catch |err| return runtime.error_abi.statusFromError(err);
        return .ok;
    }
    pub fn lockActivity(self: *Session, io: std.Io, mutex: *std.Io.Mutex) !void {
        try self.budget.checkpoint();
        while (!mutex.tryLock()) try self.budget.waitForProgress();
        errdefer mutex.unlock(io);
        try self.budget.checkpoint();
    }
    pub fn waitActivity(self: *Session, io: std.Io, mutex: *std.Io.Mutex, condition: *std.Io.Condition) !void {
        try @import("../../api/group_admission_wait.zig").wait(io, mutex, condition, &self.budget);
    }
    pub fn acquireExclusive(self: *Session, barrier: *runtime.mutation_barrier.MutationBarrier) !runtime.mutation_barrier.MutationBarrier.ExclusiveLease {
        // This authenticated HA request is a fresh capture operation. A
        // different Io operation may own a legacy lease on this same thread.
        return barrier.acquireExclusiveRootControlled(self.budget.io, &self.budget);
    }
    /// Other HA state holders do not yet carry CPU reentry. Preserve the
    /// original lifetime while retaining CPU inside the exclusive-lease graph.
    pub fn lockRetained(self: *Session, mutex: *std.atomic.Mutex) !void {
        try self.budget.checkpoint();
        while (!mutex.tryLock()) {
            try self.budget.io.sleep(.fromMilliseconds(1), .awake);
            try self.budget.checkpoint();
        }
        errdefer mutex.unlock();
        try self.budget.checkpoint();
    }
};

test "HA original admission parks capture CPU and retains ownership through C1 holder and health progress" {
    const native = sources.common_request_admission.runnable_scheduler;
    var scheduler = try native.Scheduler.init(.{ .slots = 2, .control_slots = 1 });
    defer scheduler.close();
    var holder: native.Execution = .{};
    defer holder.finish();
    var capture: native.Execution = .{};
    defer capture.finish();
    try holder.start(&scheduler, .{ .io = std.testing.io, .retained_bytes = 17 }, .background);
    var barrier: runtime.mutation_barrier.MutationBarrier = .{};
    var lease = barrier.acquireExclusive();
    var held = true;
    defer if (held) lease.release();
    try holder.park(.resource_blocked);
    try capture.start(&scheduler, .{ .io = std.testing.io, .retained_bytes = 23 }, .background);
    const State = struct {
        scheduler: *native.Scheduler,
        holder: *native.Execution,
        capture: *native.Execution,
        lease: *runtime.mutation_barrier.MutationBarrier.ExclusiveLease,
        held: *bool,
        parks: usize = 0,
        resumes: usize = 0,
        fn life(raw: *anyopaque) callconv(.c) runtime.error_abi.Status {
            const self: *@This() = @ptrCast(@alignCast(raw));
            self.capture.checkLifetime() catch |err| return runtime.error_abi.statusFromError(err);
            return .ok;
        }
        fn check(_: *anyopaque) callconv(.c) runtime.error_abi.Status {
            @panic("lifetime admission must not use a yielding checkpoint");
        }
        fn current(_: *anyopaque, out: *c_int) callconv(.c) runtime.error_abi.Status {
            out.* = @backingInt(runtime.runnable_abi.Class.background);
            return .ok;
        }
        fn park(raw: *anyopaque, reason: c_int) callconv(.c) runtime.error_abi.Status {
            const self: *@This() = @ptrCast(@alignCast(raw));
            std.debug.assert(reason == @backingInt(runtime.runnable_abi.Wait.resource));
            self.parks += 1;
            self.capture.park(.resource_blocked) catch |err| return runtime.error_abi.statusFromError(err);
            const parked = self.scheduler.stats();
            std.debug.assert(parked.runnable_slots == 0 and parked.operations == 2 and parked.retained_bytes == 40);
            var health: native.Execution = .{};
            defer health.finish();
            health.start(self.scheduler, .{ .io = std.testing.io, .retained_bytes = 5, .allow_wait = false }, .control) catch |err| return runtime.error_abi.statusFromError(err);
            self.holder.enter(.background) catch |err| return runtime.error_abi.statusFromError(err);
            std.debug.assert(self.scheduler.stats().runnable_slots == 2);
            self.lease.release();
            self.held.* = false;
            self.holder.finish();
            return .ok;
        }
        fn enter(raw: *anyopaque, class: c_int) callconv(.c) runtime.error_abi.Status {
            const self: *@This() = @ptrCast(@alignCast(raw));
            std.debug.assert(class == @backingInt(runtime.runnable_abi.Class.background));
            self.resumes += 1;
            self.capture.enter(.background) catch |err| return runtime.error_abi.statusFromError(err);
            return .ok;
        }
    };
    var state: State = .{ .scheduler = &scheduler, .holder = &holder, .capture = &capture, .lease = &lease, .held = &held };
    const io = std.testing.io;
    var admission: Session = .{};
    try admission.bind(.{ .io = runtime.io_abi.Borrow.init(&io), .execution = .{
        .context = &state,
        .cpu_slots = 1,
        .parent_class = @backingInt(runtime.runnable_abi.Class.background),
        .checkpoint = State.check,
        .check_lifetime = State.life,
        .current_class = State.current,
        .park = State.park,
        .enter = State.enter,
    } });
    var captured = try admission.acquireExclusive(&barrier);
    try std.testing.expectEqual(@as(usize, 1), state.parks);
    try std.testing.expectEqual(state.parks, state.resumes);
    try std.testing.expectEqual(@as(usize, 1), scheduler.stats().operations);
    try std.testing.expectEqual(@as(u64, 23), scheduler.stats().retained_bytes);
    try std.testing.expectEqual(@as(usize, 1), scheduler.stats().runnable_slots);
    captured.release();
    capture.finish();
    try std.testing.expectEqual(@as(usize, 0), scheduler.stats().operations);
    try std.testing.expectEqual(@as(u64, 0), scheduler.stats().retained_bytes);
    try std.testing.expectEqual(@as(u64, 0), barrier.pendingExclusiveAcquisitions());
    var reusable = barrier.tryAcquireShared() orelse return error.LeakedCaptureBarrier;
    reusable.release();
}

test "HA original admission rejects without releasing another holder or refreshing lifetime" {
    const State = struct {
        checks: usize = 0,
        fail_at: usize = 2,
        cause: anyerror,
        fn life(raw: *anyopaque) callconv(.c) runtime.error_abi.Status {
            const self: *@This() = @ptrCast(@alignCast(raw));
            self.checks += 1;
            return if (self.checks >= self.fail_at) runtime.error_abi.statusFromError(self.cause) else .ok;
        }
        fn check(_: *anyopaque) callconv(.c) runtime.error_abi.Status {
            @panic("unexpected yielding checkpoint");
        }
        fn phase(_: *anyopaque, _: c_int) callconv(.c) runtime.error_abi.Status {
            @panic("retained CPU must not transfer");
        }
    };
    const io = std.testing.io;
    for ([_]anyerror{ error.Canceled, error.DeadlineExceeded, error.SchedulerClosed }) |cause| {
        var state: State = .{ .cause = cause };
        const control: Control = .{ .io = runtime.io_abi.Borrow.init(&io), .execution = .{
            .context = &state,
            .cpu_slots = 1,
            .parent_class = @backingInt(runtime.runnable_abi.Class.background),
            .checkpoint = State.check,
            .check_lifetime = State.life,
            .park = State.phase,
            .enter = State.phase,
        } };
        var admission: Session = .{};
        try admission.bind(control);
        var barrier: runtime.mutation_barrier.MutationBarrier = .{};
        var held = barrier.acquireExclusive();
        try std.testing.expectError(cause, admission.acquireExclusive(&barrier));
        try std.testing.expectEqual(@as(usize, 2), state.checks);
        try std.testing.expectEqual(@as(u64, 0), barrier.pendingExclusiveAcquisitions());
        try std.testing.expect(barrier.tryAcquireExclusive() == null);
        held.release();
        try std.testing.expectError(cause, admission.acquireExclusive(&barrier));
        try std.testing.expectEqual(@as(usize, 2), state.checks);
        var reusable = barrier.tryAcquireExclusive() orelse return error.LeakedCaptureBarrier;
        reusable.release();
        state = .{ .cause = cause, .fail_at = 4 };
        var retained: Session = .{};
        try retained.bind(control);
        var mutex: std.atomic.Mutex = .unlocked;
        try std.testing.expect(mutex.tryLock());
        defer mutex.unlock();
        try std.testing.expectError(cause, retained.lockRetained(&mutex));
        try std.testing.expectEqual(@as(usize, 4), state.checks);
        try std.testing.expect(!mutex.tryLock());
    }
}

test "HA original admission separates same-thread mutation from capture ownership at C1" {
    const native = sources.common_request_admission.runnable_scheduler;
    var scheduler = try native.Scheduler.init(.{ .slots = 2, .control_slots = 1 });
    defer scheduler.close();
    var holder: native.Execution = .{};
    defer holder.finish();
    var capture: native.Execution = .{};
    defer capture.finish();
    try holder.start(&scheduler, .{ .io = std.testing.io, .retained_bytes = 17 }, .background);
    var barrier: runtime.mutation_barrier.MutationBarrier = .{};
    var lease = barrier.acquireShared();
    var held = true;
    defer if (held) lease.release();
    try holder.park(.resource_blocked);
    try capture.start(&scheduler, .{ .io = std.testing.io, .retained_bytes = 23 }, .background);
    const State = struct {
        scheduler: *native.Scheduler,
        holder: *native.Execution,
        capture: *native.Execution,
        lease: *runtime.mutation_barrier.MutationBarrier.SharedLease,
        held: *bool,
        parks: usize = 0,
        resumes: usize = 0,
        fn life(raw: *anyopaque) callconv(.c) runtime.error_abi.Status {
            const self: *@This() = @ptrCast(@alignCast(raw));
            self.capture.checkLifetime() catch |err| return runtime.error_abi.statusFromError(err);
            return .ok;
        }
        fn check(_: *anyopaque) callconv(.c) runtime.error_abi.Status {
            @panic("lifetime admission must not use a yielding checkpoint");
        }
        fn current(_: *anyopaque, out: *c_int) callconv(.c) runtime.error_abi.Status {
            out.* = @backingInt(runtime.runnable_abi.Class.background);
            return .ok;
        }
        fn park(raw: *anyopaque, reason: c_int) callconv(.c) runtime.error_abi.Status {
            const self: *@This() = @ptrCast(@alignCast(raw));
            std.debug.assert(reason == @backingInt(runtime.runnable_abi.Wait.resource));
            self.parks += 1;
            self.capture.park(.resource_blocked) catch |err| return runtime.error_abi.statusFromError(err);
            const parked = self.scheduler.stats();
            std.debug.assert(parked.runnable_slots == 0 and parked.operations == 2 and parked.retained_bytes == 40);
            var health: native.Execution = .{};
            defer health.finish();
            health.start(self.scheduler, .{ .io = std.testing.io, .retained_bytes = 5, .allow_wait = false }, .control) catch |err| return runtime.error_abi.statusFromError(err);
            self.holder.enter(.background) catch |err| return runtime.error_abi.statusFromError(err);
            std.debug.assert(self.scheduler.stats().runnable_slots == 2);
            self.lease.release();
            self.held.* = false;
            self.holder.finish();
            return .ok;
        }
        fn enter(raw: *anyopaque, class: c_int) callconv(.c) runtime.error_abi.Status {
            const self: *@This() = @ptrCast(@alignCast(raw));
            std.debug.assert(class == @backingInt(runtime.runnable_abi.Class.background));
            self.resumes += 1;
            self.capture.enter(.background) catch |err| return runtime.error_abi.statusFromError(err);
            return .ok;
        }
    };
    var state: State = .{ .scheduler = &scheduler, .holder = &holder, .capture = &capture, .lease = &lease, .held = &held };
    const io = std.testing.io;
    var admission: Session = .{};
    try admission.bind(.{ .io = runtime.io_abi.Borrow.init(&io), .execution = .{
        .context = &state,
        .cpu_slots = 1,
        .parent_class = @backingInt(runtime.runnable_abi.Class.background),
        .checkpoint = State.check,
        .check_lifetime = State.life,
        .current_class = State.current,
        .park = State.park,
        .enter = State.enter,
    } });
    var captured = try admission.acquireExclusive(&barrier);
    try std.testing.expectEqual(@as(usize, 1), state.parks);
    try std.testing.expectEqual(state.parks, state.resumes);
    try std.testing.expectEqual(@as(usize, 1), scheduler.stats().operations);
    try std.testing.expectEqual(@as(u64, 23), scheduler.stats().retained_bytes);
    try std.testing.expectEqual(@as(usize, 1), scheduler.stats().runnable_slots);
    captured.release();
    capture.finish();
    try std.testing.expectEqual(@as(usize, 0), scheduler.stats().operations);
    try std.testing.expectEqual(@as(u64, 0), scheduler.stats().retained_bytes);
    try std.testing.expectEqual(@as(u64, 0), barrier.pendingExclusiveAcquisitions());
    var reusable = barrier.tryAcquireShared() orelse return error.LeakedCaptureBarrier;
    reusable.release();
}
