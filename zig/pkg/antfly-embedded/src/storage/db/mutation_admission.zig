// Copyright 2026 Antfly, Inc.
// SPDX-License-Identifier: Apache-2.0

//! Pre-decision apply admission shared by batches, BEGIN and intent prepare.
//! Mandatory decision/resolution paths retain their separate completion owner.
const lock = @import("apply_rw_lock.zig");

/// Retire nested mutation readers in acquisition order reversed, after the
/// local commit and replication tails are ordered. Mandatory retirement must
/// not consult cancellation or acquire a new execution owner.
pub const MutationRetirement = struct {
    current: *?@import("antfly_runtime_abi").mutation_barrier.MutationBarrier.SharedLease,
    enclosing: ?*?@import("antfly_runtime_abi").mutation_barrier.MutationBarrier.SharedLease = null,

    pub fn retire(self: @This()) void {
        release(self.current);
        if (self.enclosing) |outer| release(outer);
    }

    fn release(slot: *?@import("antfly_runtime_abi").mutation_barrier.MutationBarrier.SharedLease) void {
        if (slot.*) |*lease| lease.release();
        slot.* = null;
    }
};

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

/// Pass the same original admission through the process-wide mutation/capture
/// gate before acquiring snapshot/apply service fences. No new owner or time.
pub fn acquireReplicationMutationShared(owner: anytype, controls: ?lock.ApplyRwLock.Acquisition) !?@import("antfly_runtime_abi").mutation_barrier.MutationBarrier.SharedLease {
    return acquireReplicationMutationSharedScoped(owner, controls, receiverMutationRoot(owner));
}

/// Explicit enclosing ownership belongs to an invocation, never to shared DB
/// state or an OS thread. Legacy callers keep their existing nesting behavior.
pub fn receiverMutationRoot(owner: anytype) ?*@import("antfly_runtime_abi").mutation_barrier.MutationBarrier.RootSharedLease {
    if (@hasField(@TypeOf(owner.*), "mutation_root")) return owner.mutation_root;
    return null;
}

pub fn acquireReplicationMutationSharedScoped(
    owner: anytype,
    controls: ?lock.ApplyRwLock.Acquisition,
    root: ?*@import("antfly_runtime_abi").mutation_barrier.MutationBarrier.RootSharedLease,
) !?@import("antfly_runtime_abi").mutation_barrier.MutationBarrier.SharedLease {
    const barrier = owner.replicationMutationBarrier() orelse {
        if (root != null) return error.InvalidArgument;
        return null;
    };
    if (root) |enclosing| {
        if (enclosing.barrier != barrier) return error.InvalidArgument;
        const Check = struct {
            original: ?lock.ApplyRwLock.Acquisition,
            pub fn checkpoint(self: *@This()) !void {
                try checkLifetime(self.original);
            }
            pub fn executionBoundary(_: *@This()) ?@import("../../common/execution_wait.zig").Boundary {
                return null;
            }
        };
        var check: Check = .{ .original = controls };
        if (enclosing.held) return try enclosing.borrowControlled(&check);
        return try enclosing.borrowPhaseControlled(owner.core.apply_mutex.io orelse return error.MissingBackendRuntimeIo, &check);
    }
    if (controls) |original| {
        const Adapter = struct {
            original: lock.ApplyRwLock.Acquisition,
            pub fn checkpoint(self: *@This()) !void {
                try self.original.lifetime.check();
            }
            pub fn executionBoundary(self: *@This()) @TypeOf(self.original.execution_wait) {
                return self.original.execution_wait;
            }
        };
        var adapter: Adapter = .{ .original = original };
        return try barrier.acquireSharedControlled(owner.core.apply_mutex.io orelse return error.MissingBackendRuntimeIo, &adapter);
    }
    return barrier.acquireShared();
}

/// While native mutation or resolution retains replication/snapshot service leases,
/// dependent capture holders have not all adopted mandatory CPU reentry yet.
/// Keep the original lifetime, but prohibit CPU transfer inside that graph.
pub fn retainedLeaseControls(controls: ?lock.ApplyRwLock.Acquisition) ?lock.ApplyRwLock.Acquisition {
    var result = controls orelse return null;
    result.execution_wait = null;
    return result;
}

pub fn checkLifetime(controls: ?lock.ApplyRwLock.Acquisition) !void {
    if (controls) |original| try original.lifetime.check();
}

pub const IntentCollection = struct {
    const read = @import("../../common/read_admission.zig");
    original: ?lock.ApplyRwLock.Acquisition = null,
    io: ?@import("std").Io = null,
    payload: read.Payload = undefined,
    pub fn bind(self: *@This(), controls: ?lock.ApplyRwLock.Acquisition, io: ?@import("std").Io) ?read.Port {
        self.original = retainedLeaseControls(controls);
        self.io = io;
        if (self.original == null) return null;
        self.payload = .{ .ptr = self, .vtable = &.{ .checkpoint = check, .wait_for_progress = wait } };
        return read.Port.init(&self.payload);
    }
    fn check(raw: *anyopaque) !void {
        const self: *@This() = @ptrCast(@alignCast(raw));
        try checkLifetime(self.original);
    }
    fn wait(raw: *anyopaque) !void {
        const self: *@This() = @ptrCast(@alignCast(raw));
        try checkLifetime(self.original);
        const io = self.io orelse return error.MissingBackendRuntimeIo;
        try io.sleep(.fromMilliseconds(1), .awake);
        try checkLifetime(self.original);
    }
};

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

test "mutation apply admission replication barrier preserves original CPU lifetime and capture intent" {
    const std = @import("std");
    const runtime = @import("antfly_runtime_abi");
    const native = @import("../../common/runnable_scheduler.zig");
    const budgets = @import("../../api/mutation_admission_controls.zig");
    const Barrier = runtime.mutation_barrier.MutationBarrier;
    const Clock = struct {
        var current: std.atomic.Value(u64) = .init(0);
        fn now() u64 {
            return current.load(.acquire);
        }
        fn canceled(raw: *const anyopaque) bool {
            return @as(*const std.atomic.Value(bool), @ptrCast(@alignCast(raw))).load(.acquire);
        }
    };
    const Bridge = struct {
        fn from(raw: *anyopaque) *native.Execution {
            return @ptrCast(@alignCast(raw));
        }
        fn life(raw: *anyopaque) callconv(.c) runtime.error_abi.Status {
            from(raw).checkLifetime() catch |err| return runtime.error_abi.statusFromError(err);
            return .ok;
        }
        fn check(_: *anyopaque) callconv(.c) runtime.error_abi.Status {
            @panic("lifetime checks must not yield under a mutation lease");
        }
        fn current(raw: *anyopaque, out: *c_int) callconv(.c) runtime.error_abi.Status {
            out.* = @backingInt(from(raw).retainedClass() catch |err| return runtime.error_abi.statusFromError(err));
            return .ok;
        }
        fn park(raw: *anyopaque, reason: c_int) callconv(.c) runtime.error_abi.Status {
            if (reason != @backingInt(runtime.runnable_abi.Wait.resource)) return runtime.error_abi.statusFromError(error.InvalidArgument);
            from(raw).park(.resource_blocked) catch |err| return runtime.error_abi.statusFromError(err);
            return .ok;
        }
        fn enter(raw: *anyopaque, class: c_int) callconv(.c) runtime.error_abi.Status {
            const captured = std.enums.fromInt(native.Class, class) orelse return runtime.error_abi.statusFromError(error.InvalidArgument);
            from(raw).enter(captured) catch |err| return runtime.error_abi.statusFromError(err);
            return .ok;
        }
    };
    const Owner = struct {
        barrier: *Barrier,
        core: struct { apply_mutex: lock.ApplyRwLock },
        pub fn replicationMutationBarrier(self: *@This()) ?*Barrier {
            return self.barrier;
        }
    };
    const Worker = struct {
        scheduler: *native.Scheduler,
        owner: *Owner,
        io: std.Io,
        canceled: *std.atomic.Value(bool),
        exclusive: bool,
        execution: native.Execution = .{},
        ready: std.Io.Event = .unset,
        finished: std.Io.Event = .unset,
        err: ?anyerror = null,
        acquired: bool = false,
        fn run(self: *@This()) void {
            self.execute() catch |err| {
                self.err = err;
                self.ready.set(self.io);
            };
            self.finished.set(self.io);
        }
        fn execute(self: *@This()) !void {
            try self.execution.start(self.scheduler, .{ .io = self.io, .retained_bytes = 256, .native_now_ns = Clock.now, .deadline_ns = 20 * std.time.ns_per_s, .cancellation = .{ .ptr = self.canceled, .requested = Clock.canceled } }, .write);
            const borrow: runtime.runnable_abi.Borrowed = .{ .context = &self.execution, .cpu_slots = 1, .parent_class = @backingInt(runtime.runnable_abi.Class.query), .checkpoint = Bridge.check, .check_lifetime = Bridge.life, .current_class = Bridge.current, .park = Bridge.park, .enter = Bridge.enter };
            var budget = budgets.init(.{}, borrow, self.io);
            self.ready.set(self.io);
            if (self.exclusive) {
                var lease = try self.owner.barrier.acquireExclusiveControlled(self.io, &budget);
                defer lease.release();
                self.acquired = true;
                try std.testing.expectEqual(native.Class.write, try self.execution.retainedClass());
            } else {
                var lease = (try acquireReplicationMutationShared(self.owner, budgets.acquisition(&budget))).?;
                defer lease.release();
                self.acquired = true;
                // Reentrant metadata/transaction work retains the same outer
                // physical lease and performs no new wait or CPU transition.
                var nested = (try acquireReplicationMutationShared(self.owner, budgets.acquisition(&budget))).?;
                try std.testing.expect(nested.nested);
                nested.release();
                try std.testing.expectEqual(native.Class.write, try self.execution.retainedClass());
            }
        }
    };
    const Case = enum { success, canceled, expired, closed };
    const io = std.testing.io;
    for ([_]bool{ false, true }) |exclusive| {
        for (std.enums.values(Case)) |case| {
            Clock.current.store(0, .release);
            defer Clock.current.store(0, .release);
            var barrier: Barrier = .{};
            var owner: Owner = .{ .barrier = &barrier, .core = .{ .apply_mutex = .{ .io = io } } };
            var shared: ?Barrier.SharedLease = if (exclusive) barrier.acquireShared() else null;
            var captured: ?Barrier.ExclusiveLease = if (!exclusive) barrier.acquireExclusive() else null;
            var scheduler = try native.Scheduler.init(.{ .slots = 2, .control_slots = 1 });
            defer scheduler.close();
            var canceled: std.atomic.Value(bool) = .init(false);
            var worker: Worker = .{ .scheduler = &scheduler, .owner = &owner, .io = io, .canceled = &canceled, .exclusive = exclusive };
            defer worker.execution.finish();
            var group: std.Io.Group = .init;
            defer {
                if (shared) |*lease| {
                    lease.release();
                    shared = null;
                }
                if (captured) |*lease| {
                    lease.release();
                    captured = null;
                }
                group.await(io) catch {};
            }
            group.async(io, Worker.run, .{&worker});
            try worker.ready.waitTimeout(io, .{ .duration = .{ .raw = .fromSeconds(5), .clock = .awake } });
            var observed = false;
            for (0..1000) |_| {
                if (scheduler.stats().resource_blocked == 1) {
                    observed = true;
                    break;
                }
                try io.sleep(.fromMilliseconds(1), .awake);
            }
            try std.testing.expect(observed);
            try std.testing.expectEqual(@as(usize, 0), scheduler.stats().runnable_slots);
            try std.testing.expectEqual(@as(usize, 1), scheduler.stats().operations);
            try std.testing.expectEqual(@as(u64, 256), scheduler.stats().retained_bytes);
            if (exclusive) {
                try std.testing.expectEqual(@as(u64, 1), barrier.pendingExclusiveAcquisitions());
                // Writer intent closes new reader admission while CPU is free.
                const unexpectedly_open = barrier.reader_gate.tryLock();
                if (unexpectedly_open) barrier.reader_gate.unlock();
                try std.testing.expect(!unexpectedly_open);
            } else try std.testing.expectEqual(@as(u64, 1), barrier.pendingSharedAcquisitions());
            var health: native.Execution = .{};
            defer health.finish();
            try health.start(&scheduler, .{ .io = io, .allow_wait = false }, .control);
            try health.checkpoint();
            health.finish();
            if (case == .canceled) canceled.store(true, .release);
            if (case == .expired) Clock.current.store(20 * std.time.ns_per_s, .release);
            if (case == .closed) scheduler.close();
            if (shared) |*lease| {
                lease.release();
                shared = null;
            }
            if (captured) |*lease| {
                lease.release();
                captured = null;
            }
            try worker.finished.waitTimeout(io, .{ .duration = .{ .raw = .fromSeconds(5), .clock = .awake } });
            group.await(io) catch {};
            if (case == .success) {
                try std.testing.expect(worker.err == null and worker.acquired);
            } else {
                const expected: anyerror = switch (case) {
                    .canceled => error.Canceled,
                    .expired => error.DeadlineExceeded,
                    .closed => error.SchedulerClosed,
                    .success => unreachable,
                };
                try std.testing.expectEqual(@as(?anyerror, expected), worker.err);
                try std.testing.expect(!worker.acquired);
            }
            try std.testing.expectEqual(@as(u64, 0), barrier.pendingSharedAcquisitions());
            try std.testing.expectEqual(@as(u64, 0), barrier.pendingExclusiveAcquisitions());
            var reusable = barrier.tryAcquireExclusive() orelse return error.TestExpectedReleasedBarrier;
            reusable.release();
            try std.testing.expectEqual(@as(usize, 1), scheduler.stats().operations);
            worker.execution.finish();
            try std.testing.expectEqual(@as(usize, 0), scheduler.stats().operations);
            try std.testing.expectEqual(@as(usize, 0), scheduler.stats().runnable_slots);
            try std.testing.expectEqual(@as(u64, 0), scheduler.stats().retained_bytes);
        }
    }
}

test "mutation apply admission retained resolution lease checks original lifetime without CPU transfer" {
    const std = @import("std");
    const Owner = struct {
        core: struct {
            apply_mutex: lock.ApplyRwLock,
            pub fn unlockApply(self: *@This()) void {
                self.apply_mutex.unlockExclusive();
            }
        },
        pub fn enforcePortableRuntimeGate(_: *@This()) !void {}
        pub fn lockApplyForPortableRuntime(_: *@This()) !void {
            @panic("controlled resolution cannot fall back to legacy admission");
        }
    };
    const State = struct {
        checks: usize = 0,
        fn life(raw: *const anyopaque) !void {
            const self: *@This() = @ptrCast(@alignCast(@constCast(raw)));
            self.checks += 1;
            if (self.checks >= 3) return error.DeadlineExceeded;
        }
        fn transition(_: *anyopaque) !void {
            @panic("retained lease admission must not transfer CPU");
        }
    };
    var owner: Owner = .{ .core = .{ .apply_mutex = .{ .io = std.testing.io } } };
    var state: State = .{};
    const original: lock.ApplyRwLock.Acquisition = .{
        .lifetime = .{ .context = &state, .check_fn = State.life },
        .execution_wait = .{ .ptr = &state, .begin = State.transition, .end = State.transition },
    };
    owner.core.apply_mutex.lockShared();
    var held = true;
    defer if (held) owner.core.apply_mutex.unlockShared();
    try std.testing.expectError(error.DeadlineExceeded, lockApply(&owner, retainedLeaseControls(original)));
    try std.testing.expect(original.execution_wait != null);
    try std.testing.expectEqual(@as(usize, 3), state.checks);
    try std.testing.expectError(error.DeadlineExceeded, checkLifetime(retainedLeaseControls(original)));
    owner.core.apply_mutex.unlockShared();
    held = false;
    try std.testing.expect(owner.core.apply_mutex.tryLockExclusive());
    owner.core.apply_mutex.unlockExclusive();
}

test "mutation apply admission intent collection borrows original lifetime and prohibits CPU transfer" {
    const std = @import("std");
    const State = struct {
        checks: usize = 0,
        fn life(raw: *const anyopaque) !void {
            const self: *@This() = @ptrCast(@alignCast(@constCast(raw)));
            self.checks += 1;
            if (self.checks >= 3) return error.DeadlineExceeded;
        }
        fn transfer(_: *anyopaque) !void {
            @panic("intent collection retains its service lease and CPU class");
        }
    };
    var state: State = .{};
    const original: lock.ApplyRwLock.Acquisition = .{ .lifetime = .{ .context = &state, .check_fn = State.life }, .execution_wait = .{ .ptr = &state, .begin = State.transfer, .end = State.transfer } };
    var collection: IntentCollection = .{};
    const scope = collection.bind(original, std.testing.io).?;
    try scope.checkpoint();
    try std.testing.expect(try scope.executionWait() == null);
    try std.testing.expect(original.execution_wait != null);
    try std.testing.expectError(error.DeadlineExceeded, scope.waitForProgress());
    try std.testing.expectEqual(@as(usize, 3), state.checks);
}
