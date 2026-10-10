// Copyright 2026 Antfly, Inc.
// SPDX-License-Identifier: Apache-2.0
//! Pre-decision startup recovery preserves the original request lifetime.
//! Recovery retains service leases: CPU transfer awaits full holder adoption.
const std = @import("std");
const admission = @import("mutation_admission.zig");
const Controls = @import("apply_rw_lock.zig").ApplyRwLock.Acquisition;

pub fn lockFlush(mutex: *std.Io.Mutex, io: std.Io, controls: ?Controls) !void {
    if (controls == null) {
        mutex.lockUncancelable(io);
        return;
    }
    try admission.checkLifetime(controls);
    while (!mutex.tryLock()) {
        try admission.checkLifetime(controls);
        try io.sleep(.fromMilliseconds(1), .awake);
    }
    errdefer mutex.unlock(io);
    try admission.checkLifetime(controls);
}

pub fn ensure(owner: anytype, original: ?Controls) !void {
    return ensureImpl(owner, original, Legacy{});
}

/// The borrowed root is invocation state, never installed on the resident DB.
pub fn ensureWithRoot(owner: anytype, original: ?Controls, root: ?*@import("antfly_runtime_abi").mutation_barrier.MutationBarrier.RootSharedLease) !void {
    return ensureImpl(owner, original, Rooted{ .root = root });
}

const Legacy = struct {
    fn flush(_: @This(), owner: anytype, controls: ?Controls) !void {
        try owner.flushDurableReplicationOutboxesWithAdmission(controls);
    }
    fn flushLocked(_: @This(), owner: anytype, controls: ?Controls) !void {
        try owner.flushDurableReplicationOutboxesLockedWithAdmission(controls);
    }
};
const Rooted = struct {
    root: ?*@import("antfly_runtime_abi").mutation_barrier.MutationBarrier.RootSharedLease,
    fn flush(self: @This(), owner: anytype, controls: ?Controls) !void {
        try owner.flushDurableReplicationOutboxesWithAdmissionAndRoot(controls, self.root);
    }
    fn flushLocked(self: @This(), owner: anytype, controls: ?Controls) !void {
        try owner.flushDurableReplicationOutboxesLockedWithAdmissionAndRoot(controls, self.root);
    }
};

fn ensureImpl(owner: anytype, original: ?Controls, scope: anytype) !void {
    const controls = admission.retainedLeaseControls(original);
    try admission.checkLifetime(controls);
    // A failed primary append must complete before later mutations overtake
    // its durable afterimage. Rejection leaves that obligation for recovery.
    while (owner.async_context.primary_replication_append_pending.load(.acquire)) {
        try admission.checkLifetime(controls);
        try scope.flush(owner, controls);
    }
    if (!owner.local_execution.durable_replication_startup_barrier_pending.load(.acquire) and
        !owner.local_execution.row_policy_replication_outbox_pending.load(.acquire)) return;
    const io = owner.backend_runtime.io() orelse std.Options.debug_io;
    try lockFlush(&owner.local_execution.durable_replication_flush_mutex, io, controls);
    defer owner.local_execution.durable_replication_flush_mutex.unlock(io);
    if (!owner.local_execution.durable_replication_startup_barrier_pending.load(.acquire) and
        !owner.local_execution.row_policy_replication_outbox_pending.load(.acquire)) return;
    while (true) {
        while (owner.local_execution.durable_replication_outbox_maybe.load(.acquire)) {
            try admission.checkLifetime(controls);
            try scope.flushLocked(owner, controls);
        }
        // The apply fence closes the race with a policy commit publishing its
        // outbox flag after the scan. Never clear flags without that fence.
        try admission.lockApply(owner, controls);
        const empty = !owner.local_execution.durable_replication_outbox_maybe.load(.acquire);
        if (empty) {
            owner.local_execution.durable_replication_startup_barrier_pending.store(false, .release);
            owner.local_execution.row_policy_replication_outbox_pending.store(false, .release);
        }
        owner.core.unlockApply();
        if (empty) return;
    }
}

test "replication startup original lifetime rejects held flush and apply fences without retiring obligations" {
    const Atomic = std.atomic.Value(bool);
    const Apply = @import("apply_rw_lock.zig").ApplyRwLock;
    const Owner = struct {
        const Core = struct {
            apply_mutex: *Apply,
            pub fn unlockApply(self: *@This()) void {
                self.apply_mutex.unlockExclusive();
            }
        };
        core: Core,
        backend_runtime: struct {
            pub fn io(_: *const @This()) ?std.Io {
                return std.testing.io;
            }
        } = .{},
        async_context: struct { primary_replication_append_pending: Atomic = .init(false) } = .{},
        local_execution: struct {
            durable_replication_flush_mutex: std.Io.Mutex = .init,
            durable_replication_startup_barrier_pending: Atomic = .init(true),
            row_policy_replication_outbox_pending: Atomic = .init(true),
            durable_replication_outbox_maybe: Atomic = .init(false),
        } = .{},
        flushes: usize = 0,
        pub fn enforcePortableRuntimeGate(_: *@This()) !void {}
        pub fn lockApplyForPortableRuntime(self: *@This()) !void {
            self.core.apply_mutex.lockExclusive();
        }
        pub fn flushDurableReplicationOutboxesWithAdmission(self: *@This(), controls: ?Controls) !void {
            try admission.checkLifetime(controls);
            self.async_context.primary_replication_append_pending.store(false, .release);
            self.flushes += 1;
        }
        pub fn flushDurableReplicationOutboxesLockedWithAdmission(self: *@This(), controls: ?Controls) !void {
            try admission.checkLifetime(controls);
            self.local_execution.durable_replication_outbox_maybe.store(false, .release);
            self.flushes += 1;
        }
    };
    const State = struct {
        checks: usize = 0,
        transitions: usize = 0,
        rejected: ?anyerror = null,
        fn check(raw: *const anyopaque) !void {
            const self: *@This() = @ptrCast(@alignCast(@constCast(raw)));
            self.checks += 1;
            if (self.checks >= 8) if (self.rejected) |err| return err;
        }
        fn transition(raw: *anyopaque) !void {
            const self: *@This() = @ptrCast(@alignCast(raw));
            self.transitions += 1;
            return error.InvalidRunnableTransition;
        }
    };
    for ([_]anyerror{ error.DeadlineExceeded, error.Canceled, error.SchedulerClosed }) |rejection| {
        for (0..2) |held_fence| {
            var apply: Apply = .{ .io = std.testing.io };
            var owner: Owner = .{ .core = .{ .apply_mutex = &apply } };
            if (held_fence == 0) owner.local_execution.durable_replication_flush_mutex.lockUncancelable(std.testing.io) else apply.lockExclusive();
            var held = true;
            defer if (held) {
                if (held_fence == 0) owner.local_execution.durable_replication_flush_mutex.unlock(std.testing.io) else apply.unlockExclusive();
            };
            var state: State = .{ .rejected = rejection };
            const original: Controls = .{
                .lifetime = .{ .context = &state, .check_fn = State.check },
                .execution_wait = .{ .ptr = &state, .begin = State.transition, .end = State.transition },
            };
            try std.testing.expectError(rejection, ensure(&owner, original));
            try std.testing.expectEqual(@as(usize, 8), state.checks);
            try std.testing.expectEqual(@as(usize, 0), state.transitions);
            try std.testing.expect(owner.local_execution.durable_replication_startup_barrier_pending.load(.acquire));
            try std.testing.expect(owner.local_execution.row_policy_replication_outbox_pending.load(.acquire));
            try std.testing.expectEqual(@as(usize, 0), owner.flushes);
            if (held_fence == 0) owner.local_execution.durable_replication_flush_mutex.unlock(std.testing.io) else apply.unlockExclusive();
            held = false;
            // Rejected request does not poison either fence or clear flags.
            // A later healthy owner performs recovery and clears them exactly
            // under the same ordered apply fence.
            state.rejected = null;
            owner.async_context.primary_replication_append_pending.store(true, .release);
            owner.local_execution.durable_replication_outbox_maybe.store(true, .release);
            try ensure(&owner, original);
            try std.testing.expectEqual(@as(usize, 2), owner.flushes);
            try std.testing.expect(!owner.local_execution.durable_replication_startup_barrier_pending.load(.acquire));
            try std.testing.expect(!owner.local_execution.row_policy_replication_outbox_pending.load(.acquire));
            try std.testing.expect(!owner.async_context.primary_replication_append_pending.load(.acquire));
            try std.testing.expect(owner.local_execution.durable_replication_flush_mutex.tryLock());
            owner.local_execution.durable_replication_flush_mutex.unlock(std.testing.io);
            try std.testing.expect(apply.tryLockExclusive());
            apply.unlockExclusive();
            try std.testing.expectEqual(@as(usize, 0), state.transitions);
        }
    }
}

test "replication startup original lifetime preserves explicit root across flush phases under writer intent" {
    const Barrier = @import("antfly_runtime_abi").mutation_barrier.MutationBarrier;
    const Apply = @import("apply_rw_lock.zig").ApplyRwLock;
    const Atomic = std.atomic.Value(bool);
    const Lifetime = struct {
        barrier: *Barrier,
        checks: usize = 0,
        fn check(raw: *const anyopaque) !void {
            const self: *@This() = @ptrCast(@alignCast(@constCast(raw)));
            self.checks += 1;
            // A lost enclosing borrow must reject promptly instead of hanging
            // while this invocation's physical reader excludes a waiting writer.
            if (self.barrier.pendingSharedAcquisitions() != 0) return error.DeadlineExceeded;
        }
        fn forbiddenWait(_: *anyopaque) !void {
            return error.InvalidRunnableTransition;
        }
    };
    const Owner = struct {
        const Core = struct {
            apply_mutex: *Apply,
            pub fn unlockApply(self: *@This()) void {
                self.apply_mutex.unlockExclusive();
            }
        };
        core: Core,
        barrier: *Barrier,
        expected_root: *Barrier.RootSharedLease,
        backend_runtime: struct {
            pub fn io(_: *const @This()) ?std.Io {
                return std.testing.io;
            }
        } = .{},
        async_context: struct { primary_replication_append_pending: Atomic = .init(true) } = .{},
        local_execution: struct {
            durable_replication_flush_mutex: std.Io.Mutex = .init,
            durable_replication_startup_barrier_pending: Atomic = .init(true),
            row_policy_replication_outbox_pending: Atomic = .init(true),
            durable_replication_outbox_maybe: Atomic = .init(true),
        } = .{},
        flushes: usize = 0,
        pub fn replicationMutationBarrier(self: *@This()) ?*Barrier {
            return self.barrier;
        }
        pub fn enforcePortableRuntimeGate(_: *@This()) !void {}
        pub fn lockApplyForPortableRuntime(self: *@This()) !void {
            self.core.apply_mutex.lockExclusive();
        }
        fn borrow(self: *@This(), controls: ?Controls, root: ?*Barrier.RootSharedLease) !void {
            try std.testing.expect(root == self.expected_root);
            try std.testing.expect(controls.?.execution_wait == null);
            var lease = (try admission.acquireReplicationMutationSharedScoped(self, controls, root)).?;
            defer lease.release();
            try std.testing.expectEqual(@as(usize, 1), self.barrier.reader_count);
            try std.testing.expectEqual(@as(usize, 1), self.expected_root.borrowers.load(.acquire));
            try std.testing.expectEqual(@as(u64, 0), self.barrier.pendingSharedAcquisitions());
            self.flushes += 1;
        }
        pub fn flushDurableReplicationOutboxesWithAdmissionAndRoot(self: *@This(), controls: ?Controls, root: ?*Barrier.RootSharedLease) !void {
            try self.borrow(controls, root);
            self.async_context.primary_replication_append_pending.store(false, .release);
        }
        pub fn flushDurableReplicationOutboxesLockedWithAdmissionAndRoot(self: *@This(), controls: ?Controls, root: ?*Barrier.RootSharedLease) !void {
            try self.borrow(controls, root);
            self.local_execution.durable_replication_outbox_maybe.store(false, .release);
        }
    };
    var barrier: Barrier = .{};
    var lifetime: Lifetime = .{ .barrier = &barrier };
    const controls: Controls = .{ .lifetime = .{ .context = &lifetime, .check_fn = Lifetime.check }, .execution_wait = .{ .ptr = &lifetime, .begin = Lifetime.forbiddenWait, .end = Lifetime.forbiddenWait } };
    const RootControls = struct {
        pub fn checkpoint(_: *@This()) !void {}
        pub fn executionBoundary(_: *@This()) ?@import("../../common/execution_wait.zig").Boundary {
            return null;
        }
    };
    var root_controls: RootControls = .{};
    var root = try barrier.acquireSharedRootControlled(std.testing.io, &root_controls);
    var root_held = true;
    defer if (root_held) root.release();
    try std.testing.expect(barrier.reader_gate.tryLock());
    var gate_held = true;
    defer if (gate_held) barrier.reader_gate.unlock();
    var apply: Apply = .{ .io = std.testing.io };
    var owner: Owner = .{ .core = .{ .apply_mutex = &apply }, .barrier = &barrier, .expected_root = &root };
    try ensureWithRoot(&owner, controls, &root);
    try std.testing.expectEqual(@as(usize, 2), owner.flushes);
    try std.testing.expect(lifetime.checks > 0);
    try std.testing.expectEqual(@as(usize, 0), root.borrowers.load(.acquire));
    try std.testing.expect(!owner.local_execution.durable_replication_startup_barrier_pending.load(.acquire));
    try std.testing.expect(!owner.local_execution.row_policy_replication_outbox_pending.load(.acquire));
    try std.testing.expectEqual(@as(usize, 1), barrier.reader_count);
    barrier.reader_gate.unlock();
    gate_held = false;
    root.release();
    root_held = false;
    var capture = barrier.tryAcquireExclusive() orelse return error.StartupScopeLeakedMutationBorrow;
    capture.release();
}
