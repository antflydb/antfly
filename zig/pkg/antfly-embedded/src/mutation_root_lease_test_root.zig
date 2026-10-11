// Copyright 2026 Antfly, Inc.
// SPDX-License-Identifier: Apache-2.0
pub const antfly_sources = struct {};
pub const consumer_tests_only = true;
const std = @import("std");
const Barrier = @import("antfly_runtime_abi").mutation_barrier.MutationBarrier;
const native = @import("common/runnable_scheduler.zig");
const Boundary = @import("common/execution_wait.zig").Boundary;

const Lease = union(enum) {
    root: Barrier.RootSharedLease,
    legacy: Barrier.SharedLease,
    fn release(self: *@This()) void {
        switch (self.*) {
            inline else => |*value| value.release(),
        }
    }
};

test "mutation root lease counts distinct C1 owners and releases on another worker" {
    var scheduler = try native.Scheduler.init(.{ .slots = 2, .control_slots = 1 });
    defer scheduler.close();
    var holder: native.Execution = .{};
    defer holder.finish();
    try holder.start(&scheduler, .{ .io = std.testing.io, .retained_bytes = 17 }, .background);
    var barrier: Barrier = .{};
    var old = barrier.acquireShared();
    var old_held = true;
    defer if (old_held) old.release();
    try holder.park(.resource_blocked);
    var fresh: native.Execution = .{};
    defer fresh.finish();
    try fresh.start(&scheduler, .{ .io = std.testing.io, .retained_bytes = 23 }, .write);
    const Controls = struct {
        execution: *native.Execution,
        pub fn checkpoint(self: *@This()) !void {
            try self.execution.checkLifetime();
        }
        pub fn executionBoundary(_: *@This()) ?Boundary {
            return null;
        }
    };
    var controls: Controls = .{ .execution = &fresh };
    var lease: Lease = .{ .root = try barrier.acquireSharedRootControlled(std.testing.io, &controls) };
    var held = true;
    defer if (held) lease.release();
    try std.testing.expectEqual(@as(usize, 2), barrier.reader_count);
    try std.testing.expectEqual(@as(u64, 0), barrier.pendingSharedAcquisitions());
    var health: native.Execution = .{};
    defer health.finish();
    try health.start(&scheduler, .{ .io = std.testing.io, .retained_bytes = 1 }, .control);
    health.finish();
    const worker = try std.Thread.spawn(.{}, Lease.release, .{&lease});
    worker.join();
    held = false;
    try std.testing.expectEqual(@as(usize, 1), barrier.reader_count);
    try std.testing.expectEqual(@as(usize, 2), scheduler.stats().operations);
    try std.testing.expectEqual(@as(u64, 40), scheduler.stats().retained_bytes);
    try std.testing.expect(barrier.tryAcquireExclusive() == null);
    // Releasing the independent root cannot change genuine legacy nesting.
    var nested = barrier.acquireShared();
    try std.testing.expect(nested.nested);
    nested.release();
    old.release();
    old_held = false;
    var capture = barrier.tryAcquireExclusive() orelse return error.LeakedMutationRoot;
    capture.release();
    fresh.finish();
    try std.testing.expectEqual(@as(u64, 17), scheduler.stats().retained_bytes);
    holder.finish();
    try std.testing.expectEqual(@as(usize, 0), scheduler.stats().operations);
}

test "mutation root lease respects writer intent and original expiry across C1 parking" {
    const Clock = struct {
        var value: u64 = 0;
        fn now() u64 {
            return value;
        }
    };
    Clock.value = 0;
    var scheduler = try native.Scheduler.init(.{ .slots = 2, .control_slots = 1 });
    defer scheduler.close();
    var holder: native.Execution = .{};
    defer holder.finish();
    try holder.start(&scheduler, .{ .io = std.testing.io, .retained_bytes = 17 }, .background);
    var barrier: Barrier = .{};
    var old = barrier.acquireShared();
    defer old.release();
    try holder.park(.resource_blocked);
    try std.testing.expect(barrier.reader_gate.tryLock());
    defer barrier.reader_gate.unlock();
    var fresh: native.Execution = .{};
    defer fresh.finish();
    try fresh.start(&scheduler, .{ .io = std.testing.io, .retained_bytes = 23, .native_now_ns = Clock.now, .deadline_ns = 10 }, .write);
    const Controls = struct {
        scheduler: *native.Scheduler,
        holder: *native.Execution,
        execution: *native.Execution,
        parks: usize = 0,
        resumes: usize = 0,
        pub fn checkpoint(self: *@This()) !void {
            try self.execution.checkLifetime();
        }
        pub fn executionBoundary(self: *@This()) ?Boundary {
            return .{ .ptr = self, .begin = begin, .end = end };
        }
        fn begin(raw: *anyopaque) !void {
            const self: *@This() = @ptrCast(@alignCast(raw));
            try self.execution.park(.resource_blocked);
            self.parks += 1;
            try std.testing.expectEqual(@as(usize, 0), self.scheduler.stats().runnable_slots);
            try std.testing.expectEqual(@as(u64, 40), self.scheduler.stats().retained_bytes);
            try self.holder.enter(.background);
            var health: native.Execution = .{};
            defer health.finish();
            try health.start(self.scheduler, .{ .io = std.testing.io, .retained_bytes = 1 }, .control);
            try self.holder.park(.resource_blocked);
            Clock.value = 10;
        }
        fn end(raw: *anyopaque) !void {
            const self: *@This() = @ptrCast(@alignCast(raw));
            self.resumes += 1;
            try self.execution.enter(.write);
        }
    };
    var controls: Controls = .{ .scheduler = &scheduler, .holder = &holder, .execution = &fresh };
    try std.testing.expectError(error.DeadlineExceeded, barrier.acquireSharedRootControlled(std.testing.io, &controls));
    try std.testing.expectEqual(@as(usize, 1), controls.parks);
    try std.testing.expectEqual(@as(usize, 1), controls.resumes);
    try std.testing.expectEqual(@as(usize, 1), barrier.reader_count);
    try std.testing.expectEqual(@as(u64, 0), barrier.pendingSharedAcquisitions());
    try std.testing.expect(!barrier.reader_gate.tryLock());
    try std.testing.expectEqual(@as(usize, 2), scheduler.stats().operations);
    try std.testing.expectEqual(@as(u64, 40), scheduler.stats().retained_bytes);
    fresh.finish();
    try std.testing.expectEqual(@as(u64, 17), scheduler.stats().retained_bytes);
}

test "mutation root lease explicit nested admission bypasses writer gate and joins cross worker" {
    const admission = @import("storage/db/mutation_admission.zig");
    const Apply = @import("storage/db/apply_rw_lock.zig").ApplyRwLock;
    var barrier: Barrier = .{};
    const Controls = struct {
        pub fn checkpoint(_: *@This()) !void {}
        pub fn executionBoundary(_: *@This()) ?Boundary {
            return null;
        }
    };
    var controls: Controls = .{};
    var root = try barrier.acquireSharedRootControlled(std.testing.io, &controls);
    var held = true;
    defer if (held) root.release();
    const Owner = struct {
        barrier: *Barrier,
        mutation_root: ?*Barrier.RootSharedLease,
        core: struct { apply_mutex: Apply = .{} } = .{},
        pub fn replicationMutationBarrier(self: *@This()) ?*Barrier {
            return self.barrier;
        }
    };
    var owner: Owner = .{ .barrier = &barrier, .mutation_root = &root };
    try std.testing.expect(barrier.reader_gate.tryLock());
    var gate_held = true;
    defer if (gate_held) barrier.reader_gate.unlock();
    const Original = struct {
        fn check(_: *const anyopaque) !void {}
        fn rejectWait(_: *anyopaque) !void {
            return error.UnexpectedMutationRootWait;
        }
    };
    const original: Apply.Acquisition = .{
        .lifetime = .{ .context = &controls, .check_fn = Original.check },
        .execution_wait = .{ .ptr = &controls, .begin = Original.rejectWait, .end = Original.rejectWait },
    };
    var first = (try admission.acquireReplicationMutationShared(&owner, original)).?;
    var first_held = true;
    defer if (first_held) first.release();
    var second = (try admission.acquireReplicationMutationSharedScoped(&owner, original, &root)).?;
    var second_held = true;
    defer if (second_held) second.release();
    try std.testing.expectEqual(@as(usize, 1), barrier.reader_count);
    try std.testing.expectEqual(@as(usize, 2), root.borrowers.load(.acquire));
    try std.testing.expectEqual(@as(u64, 0), barrier.pendingSharedAcquisitions());
    const worker = try std.Thread.spawn(.{}, Barrier.SharedLease.release, .{&second});
    worker.join();
    second_held = false;
    try std.testing.expectEqual(@as(usize, 1), root.borrowers.load(.acquire));
    try std.testing.expectEqual(@as(usize, 1), barrier.reader_count);
    // Root cannot retire until the remaining synchronous consumer joins.
    barrier.reader_gate.unlock();
    gate_held = false;
    try std.testing.expect(barrier.tryAcquireExclusive() == null);
    // Release before the root, without allowing the deferred borrow to linger.
    first.release();
    first_held = false;
    root.release();
    held = false;
    var capture = barrier.tryAcquireExclusive() orelse return error.LeakedMutationRoot;
    capture.release();
}

test "mutation root lease explicit nested admission preserves rejection and rolls back borrow" {
    const admission = @import("storage/db/mutation_admission.zig");
    const Apply = @import("storage/db/apply_rw_lock.zig").ApplyRwLock;
    const Controls = struct {
        pub fn checkpoint(_: *@This()) !void {}
        pub fn executionBoundary(_: *@This()) ?Boundary {
            return null;
        }
    };
    var controls: Controls = .{};
    var barrier: Barrier = .{};
    var root = try barrier.acquireSharedRootControlled(std.testing.io, &controls);
    defer root.release();
    const Owner = struct {
        barrier: *Barrier,
        core: struct { apply_mutex: Apply = .{} } = .{},
        pub fn replicationMutationBarrier(self: *@This()) ?*Barrier {
            return self.barrier;
        }
    };
    var owner: Owner = .{ .barrier = &barrier };
    const Lifetime = struct {
        calls: usize = 0,
        reject_at: usize,
        failure: anyerror,
        fn check(raw: *const anyopaque) !void {
            const self: *@This() = @ptrCast(@alignCast(@constCast(raw)));
            self.calls += 1;
            if (self.calls >= self.reject_at) return self.failure;
        }
        fn forbiddenWait(_: *anyopaque) !void {
            @panic("nested borrowing cannot park CPU");
        }
    };
    var old = root.borrow();
    defer old.release();
    try std.testing.expect(barrier.reader_gate.tryLock());
    defer barrier.reader_gate.unlock();
    for ([_]anyerror{ error.Canceled, error.DeadlineExceeded, error.SchedulerClosed, error.Forbidden }) |failure| {
        for ([_]usize{ 1, 2 }) |reject_at| {
            var original: Lifetime = .{ .reject_at = reject_at, .failure = failure };
            const acquisition: Apply.Acquisition = .{
                .lifetime = .{ .context = &original, .check_fn = Lifetime.check },
                .execution_wait = .{ .ptr = &original, .begin = Lifetime.forbiddenWait, .end = Lifetime.forbiddenWait },
            };
            try std.testing.expectError(failure, admission.acquireReplicationMutationSharedScoped(&owner, acquisition, &root));
            try std.testing.expectEqual(reject_at, original.calls);
            try std.testing.expectEqual(@as(usize, 1), root.borrowers.load(.acquire));
            try std.testing.expectEqual(@as(usize, 1), barrier.reader_count);
            try std.testing.expectEqual(@as(u64, 0), barrier.pendingSharedAcquisitions());
        }
    }
    var other: Barrier = .{};
    owner.barrier = &other;
    try std.testing.expectError(error.InvalidArgument, admission.acquireReplicationMutationSharedScoped(&owner, null, &root));
    owner.barrier = &barrier;
    var healthy = (try admission.acquireReplicationMutationSharedScoped(&owner, null, &root)).?;
    healthy.release();
    try std.testing.expectEqual(@as(usize, 1), root.borrowers.load(.acquire));
}

test "mutation root lease retires resolver and nested batch before C1 capture" {
    const admission = @import("storage/db/mutation_admission.zig");
    var scheduler = try native.Scheduler.init(.{ .slots = 2, .control_slots = 1 });
    defer scheduler.close();
    var original: native.Execution = .{};
    defer original.finish();
    try original.start(&scheduler, .{ .io = std.testing.io, .retained_bytes = 23 }, .write);
    var barrier: Barrier = .{};
    var resolver: ?Barrier.SharedLease = barrier.acquireShared();
    var batch: ?Barrier.SharedLease = barrier.acquireShared();
    const retirement: admission.MutationRetirement = .{ .current = &batch, .enclosing = &resolver };
    defer retirement.retire();
    try std.testing.expect(batch.?.nested);
    try std.testing.expect(barrier.tryAcquireExclusive() == null);
    retirement.retire();
    retirement.retire();
    try std.testing.expect(batch == null and resolver == null);
    try original.park(.resource_blocked);
    var capture_owner: native.Execution = .{};
    defer capture_owner.finish();
    try capture_owner.start(&scheduler, .{ .io = std.testing.io }, .control);
    var capture = barrier.tryAcquireExclusive() orelse return error.ResolverReaderSurvivedBatch;
    capture.release();
    try std.testing.expectEqual(@as(usize, 2), scheduler.stats().operations);
    try std.testing.expectEqual(@as(u64, 23), scheduler.stats().retained_bytes);
    capture_owner.finish();
    try original.enter(.write);
    var cleanup = barrier.acquireShared();
    cleanup.release();
}

test "mutation root lease graph migration admission preserves original pass rejection" {
    const admission = @import("storage/db/mutation_admission.zig");
    const Pass = @import("storage/db/graph_cleanup_owner.zig").Pass;
    const Apply = @import("storage/db/apply_rw_lock.zig").ApplyRwLock;
    const Owner = struct {
        barrier: *Barrier,
        core: struct { apply_mutex: struct { io: ?std.Io = std.testing.io } = .{} } = .{},
        pub fn replicationMutationBarrier(self: *@This()) ?*Barrier {
            return self.barrier;
        }
    };
    const Clock = struct {
        var barrier: *Barrier = undefined;
        var stopping: *std.atomic.Value(bool) = undefined;
        var value: u64 = 0;
        var cancel: bool = false;
        fn now() u64 {
            if (barrier.pendingSharedAcquisitions() != 0) {
                if (cancel) stopping.store(true, .release) else value = 10;
            }
            return value;
        }
    };
    for ([_]bool{ false, true }) |cancel| {
        var barrier: Barrier = .{};
        var owner: Owner = .{ .barrier = &barrier };
        var stopping: std.atomic.Value(bool) = .init(false);
        Clock.barrier = &barrier;
        Clock.stopping = &stopping;
        Clock.value = 0;
        Clock.cancel = cancel;
        const original: Pass = .{ .stopping = &stopping, .deadline_ns = 10, .now_ns = Clock.now };
        const controls: Apply.Acquisition = .{ .lifetime = original.lifetime() };
        var capture = barrier.tryAcquireExclusive() orelse return error.TestUnexpectedResult;
        var held = true;
        defer if (held) capture.release();
        try std.testing.expectError(if (cancel) error.Canceled else error.DeadlineExceeded, admission.acquireReplicationMutationShared(&owner, controls));
        try std.testing.expectEqual(@as(u64, 10), original.deadline_ns);
        try std.testing.expectEqual(@as(u64, 0), barrier.pendingSharedAcquisitions());
        try std.testing.expectEqual(@as(usize, 0), barrier.reader_count);
        capture.release();
        held = false;
        var healthy_stopping: std.atomic.Value(bool) = .init(false);
        const healthy: Pass = .{ .stopping = &healthy_stopping, .deadline_ns = 20, .now_ns = Clock.now };
        var admitted = (try admission.acquireReplicationMutationShared(&owner, .{ .lifetime = healthy.lifetime() })).?;
        admitted.release();
        var after = barrier.tryAcquireExclusive() orelse return error.MigrationAdmissionLeakedReader;
        after.release();
    }
}

test "mutation root lease phase retirement preserves C1 owner through capture and fresh reentry" {
    var scheduler = try native.Scheduler.init(.{ .slots = 2, .control_slots = 1 });
    defer scheduler.close();
    var original: native.Execution = .{};
    defer original.finish();
    try original.start(&scheduler, .{ .io = std.testing.io, .retained_bytes = 23 }, .write);
    const Controls = struct {
        execution: *native.Execution,
        pub fn checkpoint(self: *@This()) !void {
            try self.execution.checkLifetime();
        }
        pub fn executionBoundary(_: *@This()) ?Boundary {
            return null;
        }
    };
    var controls: Controls = .{ .execution = &original };
    var barrier: Barrier = .{};
    var root = try barrier.acquireSharedRootControlled(std.testing.io, &controls);
    defer root.release();
    var child = root.borrow();
    try std.testing.expectError(error.InvalidRunnableTransition, root.retirePhase());
    try std.testing.expect(root.held);
    try std.testing.expectEqual(@as(usize, 1), barrier.reader_count);
    const joined = try std.Thread.spawn(.{}, Barrier.SharedLease.release, .{&child});
    joined.join();
    try root.retirePhase();
    try root.retirePhase();
    try std.testing.expect(!root.held);
    try std.testing.expect(root.barrier == &barrier);
    try std.testing.expectEqual(@as(usize, 0), barrier.reader_count);
    try std.testing.expectError(error.InvalidRunnableTransition, root.borrowControlled(&controls));
    try original.park(.resource_blocked);
    const Capture = struct {
        scheduler: *native.Scheduler,
        barrier: *Barrier,
        failure: ?anyerror = null,
        fn run(self: *@This()) void {
            self.capture() catch |err| {
                self.failure = err;
            };
        }
        fn capture(self: *@This()) !void {
            var control: native.Execution = .{};
            defer control.finish();
            try control.start(self.scheduler, .{ .io = std.testing.io }, .control);
            var physical = self.barrier.tryAcquireExclusive() orelse return error.RetiredRootBlockedCapture;
            physical.release();
        }
    };
    var capture: Capture = .{ .scheduler = &scheduler, .barrier = &barrier };
    const worker = try std.Thread.spawn(.{}, Capture.run, .{&capture});
    worker.join();
    if (capture.failure) |err| return err;
    try std.testing.expectEqual(@as(usize, 1), scheduler.stats().operations);
    try std.testing.expectEqual(@as(u64, 23), scheduler.stats().retained_bytes);
    try original.enter(.write);
    // Another operation's legacy reader on this OS worker cannot turn a new
    // physical phase into thread-local nesting of that unrelated operation.
    var other = barrier.acquireShared();
    var other_held = true;
    defer if (other_held) other.release();
    try root.reenterPhaseControlled(std.testing.io, &controls);
    try std.testing.expect(root.held);
    try std.testing.expectEqual(@as(usize, 2), barrier.reader_count);
    var nested = try root.borrowControlled(&controls);
    nested.release();
    try root.retirePhase();
    try std.testing.expectEqual(@as(usize, 1), barrier.reader_count);
    other.release();
    other_held = false;
    var after = barrier.tryAcquireExclusive() orelse return error.PhaseReentryLeakedReader;
    after.release();
    try std.testing.expectEqual(@as(usize, 1), scheduler.stats().operations);
    try std.testing.expectEqual(@as(u64, 23), scheduler.stats().retained_bytes);
}

test "mutation root lease phase reentry retains original rejection and rolls back post grant" {
    const Controls = struct {
        calls: usize = 0,
        reject_at: usize = 0,
        failure: anyerror = error.Canceled,
        pub fn checkpoint(self: *@This()) !void {
            self.calls += 1;
            if (self.reject_at != 0 and self.calls == self.reject_at) return self.failure;
        }
        pub fn executionBoundary(_: *@This()) ?Boundary {
            return null;
        }
    };
    var controls: Controls = .{};
    var barrier: Barrier = .{};
    var root = try barrier.acquireSharedRootControlled(std.testing.io, &controls);
    defer root.release();
    try std.testing.expectError(error.InvalidRunnableTransition, root.reenterPhaseControlled(std.testing.io, &controls));
    try root.retirePhase();
    for ([_]anyerror{ error.Canceled, error.DeadlineExceeded, error.Forbidden, error.SchedulerClosed }) |failure| {
        for ([_]usize{ 1, 2 }) |reject_at| {
            controls = .{ .reject_at = reject_at, .failure = failure };
            try std.testing.expectError(failure, root.reenterPhaseControlled(std.testing.io, &controls));
            try std.testing.expectEqual(reject_at, controls.calls);
            try std.testing.expect(!root.held);
            try std.testing.expect(root.barrier == &barrier);
            try std.testing.expectEqual(@as(usize, 0), root.borrowers.load(.acquire));
            try std.testing.expectEqual(@as(usize, 0), barrier.reader_count);
            try std.testing.expectEqual(@as(u64, 0), barrier.pendingSharedAcquisitions());
            var capture = barrier.tryAcquireExclusive() orelse return error.RejectedPhaseLeakedReader;
            capture.release();
        }
    }
    controls = .{};
    try root.reenterPhaseControlled(std.testing.io, &controls);
    try std.testing.expect(root.held);
    try std.testing.expectEqual(@as(usize, 1), barrier.reader_count);
    try root.retirePhase();
}

test "mutation root lease scoped admission owns only the phase it opens" {
    const admission = @import("storage/db/mutation_admission.zig");
    const Apply = @import("storage/db/apply_rw_lock.zig").ApplyRwLock;
    const Owner = struct {
        barrier: *Barrier,
        core: struct { apply_mutex: Apply = .{ .io = std.testing.io } } = .{},
        pub fn replicationMutationBarrier(self: *@This()) ?*Barrier {
            return self.barrier;
        }
    };
    var barrier: Barrier = .{};
    var root = Barrier.RootSharedLease.initRetired(&barrier);
    defer root.release();
    var owner: Owner = .{ .barrier = &barrier };
    for (0..2) |_| {
        var guard = (try admission.acquireReplicationMutationSharedScoped(&owner, null, &root)).?;
        try std.testing.expect(guard.phase_owner and root.held);
        var nested = (try admission.acquireReplicationMutationSharedScoped(&owner, null, &root)).?;
        try std.testing.expect(!nested.phase_owner);
        const worker = try std.Thread.spawn(.{}, Barrier.SharedLease.release, .{&nested});
        worker.join();
        try std.testing.expect(root.held);
        try std.testing.expectEqual(@as(usize, 1), barrier.reader_count);
        guard.release();
        try std.testing.expect(!root.held);
        try std.testing.expect(root.barrier == &barrier);
        var capture = barrier.tryAcquireExclusive() orelse return error.ScopedPhaseBlockedCapture;
        capture.release();
    }
    // An already-held caller owns retirement; a local guard only borrows it.
    var held = root.borrowPhase();
    var borrowed = (try admission.acquireReplicationMutationSharedScoped(&owner, null, &root)).?;
    borrowed.release();
    try std.testing.expect(root.held and held.phase_owner);
    held.release();
    try std.testing.expect(!root.held);
}

test "mutation root lease scoped phase original rejection rolls back every acquisition check" {
    const admission = @import("storage/db/mutation_admission.zig");
    const Apply = @import("storage/db/apply_rw_lock.zig").ApplyRwLock;
    const Owner = struct {
        barrier: *Barrier,
        core: struct { apply_mutex: Apply = .{ .io = std.testing.io } } = .{},
        pub fn replicationMutationBarrier(self: *@This()) ?*Barrier {
            return self.barrier;
        }
    };
    const Original = struct {
        calls: usize = 0,
        reject_at: usize,
        failure: anyerror,
        fn check(raw: *const anyopaque) !void {
            const self: *@This() = @ptrCast(@alignCast(@constCast(raw)));
            self.calls += 1;
            if (self.calls == self.reject_at) return self.failure;
        }
    };
    var barrier: Barrier = .{};
    var root = Barrier.RootSharedLease.initRetired(&barrier);
    defer root.release();
    var owner: Owner = .{ .barrier = &barrier };
    for ([_]anyerror{ error.Canceled, error.DeadlineExceeded, error.Forbidden, error.SchedulerClosed }) |failure| {
        for (1..5) |reject_at| {
            var original: Original = .{ .reject_at = reject_at, .failure = failure };
            const controls: Apply.Acquisition = .{ .lifetime = .{ .context = &original, .check_fn = Original.check } };
            try std.testing.expectError(failure, admission.acquireReplicationMutationSharedScoped(&owner, controls, &root));
            try std.testing.expectEqual(reject_at, original.calls);
            try std.testing.expect(!root.held);
            try std.testing.expectEqual(@as(usize, 0), root.borrowers.load(.acquire));
            try std.testing.expectEqual(@as(usize, 0), barrier.reader_count);
            try std.testing.expectEqual(@as(u64, 0), barrier.pendingSharedAcquisitions());
            var capture = barrier.tryAcquireExclusive() orelse return error.RejectedScopedPhaseBlockedCapture;
            capture.release();
        }
    }
    var healthy = (try admission.acquireReplicationMutationSharedScoped(&owner, null, &root)).?;
    healthy.release();
    try std.testing.expect(!root.held);
}

test "mutation root lease mandatory phase remains independent of another legacy operation" {
    var barrier: Barrier = .{};
    var root = Barrier.RootSharedLease.initRetired(&barrier);
    defer root.release();
    var other = barrier.acquireShared();
    var phase = root.borrowPhase();
    try std.testing.expectEqual(@as(usize, 2), barrier.reader_count);
    phase.release();
    try std.testing.expect(!root.held);
    try std.testing.expectEqual(@as(usize, 1), barrier.reader_count);
    other.release();
    var capture = barrier.tryAcquireExclusive() orelse return error.MandatoryPhaseBlockedCapture;
    capture.release();
}
