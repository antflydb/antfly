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
        try waitForProgress(io, controls);
    }
    errdefer mutex.unlock(io);
    try admission.checkLifetime(controls);
}

fn waitForProgress(io: std.Io, controls: ?Controls) !void {
    if (controls) |original| if (original.execution_wait) |boundary| {
        return boundary.run(waitTick, .{io});
    };
    try waitTick(io);
}

fn waitTick(io: std.Io) !void {
    try io.sleep(.fromMilliseconds(1), .awake);
}

/// Only an explicitly retired root proves no mutation reader is retained.
/// Snapshot/policy callers still suppress their original boundary themselves.
pub fn entryControls(original: ?Controls, root: ?*@import("antfly_runtime_abi").mutation_barrier.MutationBarrier.RootSharedLease) ?Controls {
    if (root) |value| if (!value.held and value.borrowers.load(.acquire) == 0) return original;
    return admission.retainedLeaseControls(original);
}

pub fn ensure(owner: anytype, original: ?Controls) !void {
    return ensureImpl(owner, original, Legacy{});
}

/// The borrowed root is invocation state, never installed on the resident DB.
pub fn ensureWithRoot(owner: anytype, original: ?Controls, root: ?*@import("antfly_runtime_abi").mutation_barrier.MutationBarrier.RootSharedLease) !void {
    return ensureImpl(owner, original, Rooted{ .root = root });
}

/// Called before this invocation takes policy or snapshot leases. A missing
/// root here represents no configured global barrier, not a retained reader.
pub fn ensurePreDecisionBeforePolicyWithRoot(owner: anytype, original: ?Controls, root: ?*@import("antfly_runtime_abi").mutation_barrier.MutationBarrier.RootSharedLease) !void {
    return ensurePreDecisionImpl(owner, original, Rooted{ .root = root, .before_policy = true });
}

/// After local service acquisition, only an explicitly retired root and the
/// caller's unsuppressed boundary can authorize lending. Mandatory callers keep
/// ensureWithRoot and its existing completion/recovery protocol.
pub fn ensurePreDecisionWithRoot(owner: anytype, original: ?Controls, root: ?*@import("antfly_runtime_abi").mutation_barrier.MutationBarrier.RootSharedLease) !void {
    return ensurePreDecisionImpl(owner, original, Rooted{ .root = root });
}

fn ensurePreDecisionImpl(owner: anytype, original: ?Controls, scope: anytype) !void {
    try admission.checkLifetime(original);
    if (original == null) return ensureImpl(owner, original, scope);
    if (scope.entry(original).?.execution_wait != null) return ensureImpl(owner, original, scope);
    if (!admission.ownerField(owner, "async_context").*.primary_replication_append_pending.load(.acquire) and
        !admission.ownerField(owner, "async_context").*.primary_replication_outbox_pending.load(.acquire) and
        !admission.ownerField(owner, "local_execution").*.durable_replication_startup_barrier_pending.load(.acquire) and
        !admission.ownerField(owner, "local_execution").*.row_policy_replication_outbox_pending.load(.acquire) and
        !admission.ownerField(owner, "local_execution").*.durable_replication_outbox_maybe.load(.acquire)) return;
    var schedule_recovery = false;
    // Enqueue only after this probe has retired its local service fences.
    defer if (schedule_recovery) owner.scheduleDurableReplicationOutboxRecovery();
    if (admission.ownerField(owner, "async_context").*.primary_replication_append_pending.load(.acquire)) {
        admission.ownerField(owner, "local_execution").*.durable_replication_outbox_maybe.store(true, .release);
        schedule_recovery = true;
        try admission.checkLifetime(original);
        return error.Backpressured;
    }
    const io = admission.ownerField(owner, "backend_runtime").*.io() orelse std.Options.debug_io;
    if (!admission.ownerField(owner, "local_execution").*.durable_replication_flush_mutex.tryLock()) {
        try admission.checkLifetime(original);
        return error.Backpressured;
    }
    defer admission.ownerField(owner, "local_execution").*.durable_replication_flush_mutex.unlock(io);
    const retained = admission.retainedLeaseControls(original);
    try admission.lockApplyPreDecision(owner, retained);
    defer admission.ownerField(owner, "core").*.unlockApply();
    if (admission.ownerField(owner, "async_context").*.primary_replication_append_pending.load(.acquire)) {
        admission.ownerField(owner, "local_execution").*.durable_replication_outbox_maybe.store(true, .release);
        schedule_recovery = true;
        try admission.checkLifetime(original);
        return error.Backpressured;
    }
    var scan: admission.IntentCollection = .{};
    const control = scan.bindPreDecisionBeforeMutation(retained, io, null).?;
    const pending = try @import("durable_outbox_store.zig").hasPendingWithAdmission(admission.ownerField(owner, "core").*.store, control);
    if (pending) {
        // Conservatively retain a discovered obligation even if this foreground
        // lifetime rejects after the scan. No delivery/clear authority is added.
        admission.ownerField(owner, "local_execution").*.durable_replication_outbox_maybe.store(true, .release);
        schedule_recovery = true;
        try admission.checkLifetime(original);
        return error.Backpressured;
    }
    try admission.checkLifetime(original);
    // Flush excludes another recovery pass; apply excludes a new publication.
    // A fenced empty snapshot is required before clearing optimistic hints.
    admission.ownerField(owner, "local_execution").*.durable_replication_outbox_maybe.store(false, .release);
    admission.ownerField(owner, "async_context").*.primary_replication_outbox_pending.store(false, .release);
    admission.ownerField(owner, "local_execution").*.durable_replication_startup_barrier_pending.store(false, .release);
    admission.ownerField(owner, "local_execution").*.row_policy_replication_outbox_pending.store(false, .release);
}

const Legacy = struct {
    fn entry(_: @This(), original: ?Controls) ?Controls {
        return admission.retainedLeaseControls(original);
    }
    fn flush(_: @This(), owner: anytype, controls: ?Controls) !void {
        try owner.flushDurableReplicationOutboxesWithAdmission(controls);
    }
    fn flushLocked(_: @This(), owner: anytype, controls: ?Controls) !void {
        try owner.flushDurableReplicationOutboxesLockedWithAdmission(controls);
    }
};
const Rooted = struct {
    root: ?*@import("antfly_runtime_abi").mutation_barrier.MutationBarrier.RootSharedLease,
    before_policy: bool = false,
    fn entry(self: @This(), original: ?Controls) ?Controls {
        if (self.before_policy and self.root == null) return original;
        return entryControls(original, self.root);
    }
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
    while (admission.ownerField(owner, "async_context").*.primary_replication_append_pending.load(.acquire)) {
        try admission.checkLifetime(controls);
        try scope.flush(owner, scope.entry(original));
        if (admission.ownerField(owner, "async_context").*.primary_replication_append_pending.load(.acquire)) {
            // An append may still be in flight before an outbox is published.
            // Do not spin on repeated empty flushes or occupy its CPU slot.
            const io = admission.ownerField(owner, "backend_runtime").*.io() orelse std.Options.debug_io;
            try waitForProgress(io, scope.entry(original));
            try admission.checkLifetime(controls);
        }
    }
    if (!admission.ownerField(owner, "local_execution").*.durable_replication_startup_barrier_pending.load(.acquire) and
        !admission.ownerField(owner, "local_execution").*.row_policy_replication_outbox_pending.load(.acquire)) return;
    const io = admission.ownerField(owner, "backend_runtime").*.io() orelse std.Options.debug_io;
    try lockFlush(&admission.ownerField(owner, "local_execution").*.durable_replication_flush_mutex, io, scope.entry(original));
    defer admission.ownerField(owner, "local_execution").*.durable_replication_flush_mutex.unlock(io);
    if (!admission.ownerField(owner, "local_execution").*.durable_replication_startup_barrier_pending.load(.acquire) and
        !admission.ownerField(owner, "local_execution").*.row_policy_replication_outbox_pending.load(.acquire)) return;
    while (true) {
        while (admission.ownerField(owner, "local_execution").*.durable_replication_outbox_maybe.load(.acquire)) {
            try admission.checkLifetime(controls);
            try scope.flushLocked(owner, controls);
        }
        // The apply fence closes the race with a policy commit publishing its
        // outbox flag after the scan. Never clear flags without that fence.
        try admission.lockApply(owner, controls);
        const empty = !admission.ownerField(owner, "local_execution").*.durable_replication_outbox_maybe.load(.acquire);
        if (empty) {
            admission.ownerField(owner, "local_execution").*.durable_replication_startup_barrier_pending.store(false, .release);
            admission.ownerField(owner, "local_execution").*.row_policy_replication_outbox_pending.store(false, .release);
        }
        admission.ownerField(owner, "core").*.unlockApply();
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
            if (held_fence == 0) admission.ownerField(owner, "local_execution").*.durable_replication_flush_mutex.lockUncancelable(std.testing.io) else apply.lockExclusive();
            var held = true;
            defer if (held) {
                if (held_fence == 0) admission.ownerField(owner, "local_execution").*.durable_replication_flush_mutex.unlock(std.testing.io) else apply.unlockExclusive();
            };
            var state: State = .{ .rejected = rejection };
            const original: Controls = .{
                .lifetime = .{ .context = &state, .check_fn = State.check },
                .execution_wait = .{ .ptr = &state, .begin = State.transition, .end = State.transition },
            };
            try std.testing.expectError(rejection, ensure(&owner, original));
            try std.testing.expectEqual(@as(usize, 8), state.checks);
            try std.testing.expectEqual(@as(usize, 0), state.transitions);
            try std.testing.expect(admission.ownerField(owner, "local_execution").*.durable_replication_startup_barrier_pending.load(.acquire));
            try std.testing.expect(admission.ownerField(owner, "local_execution").*.row_policy_replication_outbox_pending.load(.acquire));
            try std.testing.expectEqual(@as(usize, 0), owner.flushes);
            if (held_fence == 0) admission.ownerField(owner, "local_execution").*.durable_replication_flush_mutex.unlock(std.testing.io) else apply.unlockExclusive();
            held = false;
            // Rejected request does not poison either fence or clear flags.
            // A later healthy owner performs recovery and clears them exactly
            // under the same ordered apply fence.
            state.rejected = null;
            admission.ownerField(owner, "async_context").*.primary_replication_append_pending.store(true, .release);
            admission.ownerField(owner, "local_execution").*.durable_replication_outbox_maybe.store(true, .release);
            try ensure(&owner, original);
            try std.testing.expectEqual(@as(usize, 2), owner.flushes);
            try std.testing.expect(!admission.ownerField(owner, "local_execution").*.durable_replication_startup_barrier_pending.load(.acquire));
            try std.testing.expect(!admission.ownerField(owner, "local_execution").*.row_policy_replication_outbox_pending.load(.acquire));
            try std.testing.expect(!admission.ownerField(owner, "async_context").*.primary_replication_append_pending.load(.acquire));
            try std.testing.expect(admission.ownerField(owner, "local_execution").*.durable_replication_flush_mutex.tryLock());
            admission.ownerField(owner, "local_execution").*.durable_replication_flush_mutex.unlock(std.testing.io);
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
    try std.testing.expect(!admission.ownerField(owner, "local_execution").*.durable_replication_startup_barrier_pending.load(.acquire));
    try std.testing.expect(!admission.ownerField(owner, "local_execution").*.row_policy_replication_outbox_pending.load(.acquire));
    try std.testing.expectEqual(@as(usize, 1), barrier.reader_count);
    barrier.reader_gate.unlock();
    gate_held = false;
    root.release();
    root_held = false;
    var capture = barrier.tryAcquireExclusive() orelse return error.StartupScopeLeakedMutationBorrow;
    capture.release();
}

test "replication startup original lifetime retired flush wait lends CPU then retains it inside recovery" {
    const native = @import("../../common/runnable_scheduler.zig");
    const Barrier = @import("antfly_runtime_abi").mutation_barrier.MutationBarrier;
    const Atomic = std.atomic.Value(bool);
    const Apply = @import("apply_rw_lock.zig").ApplyRwLock;
    const Owner = struct {
        core: struct {
            apply_mutex: *Apply,
            pub fn unlockApply(self: *@This()) void {
                self.apply_mutex.unlockExclusive();
            }
        },
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
            durable_replication_outbox_maybe: Atomic = .init(true),
        } = .{},
        expected_root: *Barrier.RootSharedLease,
        flushes: usize = 0,
        pub fn enforcePortableRuntimeGate(_: *@This()) !void {}
        pub fn lockApplyForPortableRuntime(_: *@This()) !void {
            return error.UnexpectedLegacyAdmission;
        }
        pub fn flushDurableReplicationOutboxesWithAdmissionAndRoot(_: *@This(), _: ?Controls, _: ?*Barrier.RootSharedLease) !void {
            return error.UnexpectedUnlockedFlush;
        }
        pub fn flushDurableReplicationOutboxesLockedWithAdmissionAndRoot(self: *@This(), controls: ?Controls, root: ?*Barrier.RootSharedLease) !void {
            try std.testing.expect(root == self.expected_root);
            try std.testing.expect(controls.?.execution_wait == null);
            try admission.checkLifetime(controls);
            if (self.local_execution.durable_replication_flush_mutex.tryLock()) {
                self.local_execution.durable_replication_flush_mutex.unlock(std.testing.io);
                return error.FlushMutexNotHeld;
            }
            self.flushes += 1;
            self.local_execution.durable_replication_outbox_maybe.store(false, .release);
        }
    };
    var scheduler = try native.Scheduler.init(.{ .slots = 2, .control_slots = 1 });
    defer scheduler.close();
    var holder: native.Execution = .{};
    defer holder.finish();
    var requester: native.Execution = .{};
    defer requester.finish();
    var barrier: Barrier = .{};
    var root = Barrier.RootSharedLease.initRetired(&barrier);
    defer root.release();
    var apply: Apply = .{ .io = std.testing.io };
    var owner: Owner = .{ .core = .{ .apply_mutex = &apply }, .expected_root = &root };
    try holder.start(&scheduler, .{ .io = std.testing.io, .retained_bytes = 17 }, .background);
    admission.ownerField(owner, "local_execution").*.durable_replication_flush_mutex.lockUncancelable(std.testing.io);
    var held = true;
    defer if (held) admission.ownerField(owner, "local_execution").*.durable_replication_flush_mutex.unlock(std.testing.io);
    try holder.park(.resource_blocked);
    try requester.start(&scheduler, .{ .io = std.testing.io, .retained_bytes = 23 }, .write);
    const entry = &requester.entry.?;
    const State = struct {
        scheduler: *native.Scheduler,
        holder: *native.Execution,
        requester: *native.Execution,
        mutex: *std.Io.Mutex,
        held: *bool,
        parks: usize = 0,
        resumes: usize = 0,
        fn check(raw: *const anyopaque) !void {
            try @as(*const @This(), @ptrCast(@alignCast(raw))).requester.checkLifetime();
        }
        fn begin(raw: *anyopaque) !void {
            const self: *@This() = @ptrCast(@alignCast(raw));
            self.parks += 1;
            try self.requester.park(.resource_blocked);
            try std.testing.expectEqual(@as(usize, 0), self.scheduler.stats().runnable_slots);
            try std.testing.expectEqual(@as(u64, 40), self.scheduler.stats().retained_bytes);
            var health: native.Execution = .{};
            try health.start(self.scheduler, .{ .io = std.testing.io, .allow_wait = false }, .control);
            health.finish();
            try self.holder.enter(.background);
            self.mutex.unlock(std.testing.io);
            self.held.* = false;
            self.holder.finish();
        }
        fn end(raw: *anyopaque) !void {
            const self: *@This() = @ptrCast(@alignCast(raw));
            self.resumes += 1;
            try self.requester.enter(.write);
        }
    };
    var state: State = .{ .scheduler = &scheduler, .holder = &holder, .requester = &requester, .mutex = &admission.ownerField(owner, "local_execution").*.durable_replication_flush_mutex, .held = &held };
    try ensureWithRoot(&owner, .{ .lifetime = .{ .context = &state, .check_fn = State.check }, .execution_wait = .{ .ptr = &state, .begin = State.begin, .end = State.end } }, &root);
    try std.testing.expectEqual(@as(usize, 1), state.parks);
    try std.testing.expectEqual(state.parks, state.resumes);
    try std.testing.expectEqual(@as(usize, 1), owner.flushes);
    try std.testing.expect(!admission.ownerField(owner, "local_execution").*.durable_replication_startup_barrier_pending.load(.acquire));
    try std.testing.expect(!admission.ownerField(owner, "local_execution").*.row_policy_replication_outbox_pending.load(.acquire));
    try std.testing.expect(entry == &requester.entry.?);
    try std.testing.expectEqual(@as(usize, 1), scheduler.stats().operations);
    try std.testing.expectEqual(@as(u64, 23), scheduler.stats().retained_bytes);
    try std.testing.expect(admission.ownerField(owner, "local_execution").*.durable_replication_flush_mutex.tryLock());
    admission.ownerField(owner, "local_execution").*.durable_replication_flush_mutex.unlock(std.testing.io);
    try std.testing.expect(!root.held and root.borrowers.load(.acquire) == 0);
}

test "replication startup original lifetime flush entry policy retains legacy held and caller suppressed boundaries" {
    const Barrier = @import("antfly_runtime_abi").mutation_barrier.MutationBarrier;
    const Check = struct {
        fn check(_: *const anyopaque) !void {}
        fn forbidden(_: *anyopaque) !void {
            @panic("policy test must not execute a boundary");
        }
    };
    var context: u8 = 0;
    const original: Controls = .{ .lifetime = .{ .context = &context, .check_fn = Check.check }, .execution_wait = .{ .ptr = &context, .begin = Check.forbidden, .end = Check.forbidden } };
    var barrier: Barrier = .{};
    var root = Barrier.RootSharedLease.initRetired(&barrier);
    defer root.release();
    try std.testing.expect(entryControls(original, &root).?.execution_wait != null);
    try std.testing.expect(entryControls(original, null).?.execution_wait == null);
    try std.testing.expect(entryControls(admission.retainedLeaseControls(original), &root).?.execution_wait == null);
    // Only inspect policy here: no physical lease is fabricated for release.
    root.held = true;
    try std.testing.expect(entryControls(original, &root).?.execution_wait == null);
    root.held = false;
}

test "replication startup original lifetime rejected flush CPU reentry never unlocks another holder" {
    const native = @import("../../common/runnable_scheduler.zig");
    const Barrier = @import("antfly_runtime_abi").mutation_barrier.MutationBarrier;
    const Signal = struct {
        canceled: bool = false,
        fn requested(raw: *const anyopaque) bool {
            return @as(*const @This(), @ptrCast(@alignCast(raw))).canceled;
        }
    };
    const Clock = struct {
        var now_ns: u64 = 0;
        fn now() u64 {
            return now_ns;
        }
    };
    for ([_]anyerror{ error.Canceled, error.DeadlineExceeded, error.SchedulerClosed }) |cause| {
        var scheduler = try native.Scheduler.init(.{ .slots = 2, .control_slots = 1 });
        defer scheduler.close();
        var requester: native.Execution = .{};
        defer requester.finish();
        var signal: Signal = .{};
        Clock.now_ns = 0;
        try requester.start(&scheduler, .{ .io = std.testing.io, .cancellation = .{ .ptr = &signal, .requested = Signal.requested }, .native_now_ns = Clock.now, .deadline_ns = if (cause == error.DeadlineExceeded) 100 else null, .retained_bytes = 23 }, .write);
        const entry = &requester.entry.?;
        var barrier: Barrier = .{};
        var root = Barrier.RootSharedLease.initRetired(&barrier);
        defer root.release();
        var mutex: std.Io.Mutex = .init;
        mutex.lockUncancelable(std.testing.io);
        defer mutex.unlock(std.testing.io);
        const State = struct {
            requester: *native.Execution,
            scheduler: *native.Scheduler,
            signal: *Signal,
            cause: anyerror,
            parks: usize = 0,
            resumes: usize = 0,
            fn check(raw: *const anyopaque) !void {
                try @as(*const @This(), @ptrCast(@alignCast(raw))).requester.checkLifetime();
            }
            fn begin(raw: *anyopaque) !void {
                const self: *@This() = @ptrCast(@alignCast(raw));
                self.parks += 1;
                try self.requester.park(.resource_blocked);
                switch (self.cause) {
                    error.Canceled => self.signal.canceled = true,
                    error.DeadlineExceeded => Clock.now_ns = 101,
                    error.SchedulerClosed => self.scheduler.close(),
                    else => unreachable,
                }
            }
            fn end(raw: *anyopaque) !void {
                const self: *@This() = @ptrCast(@alignCast(raw));
                self.resumes += 1;
                try self.requester.enter(.write);
            }
        };
        var state: State = .{ .requester = &requester, .scheduler = &scheduler, .signal = &signal, .cause = cause };
        const original: Controls = .{ .lifetime = .{ .context = &state, .check_fn = State.check }, .execution_wait = .{ .ptr = &state, .begin = State.begin, .end = State.end } };
        try std.testing.expectError(cause, lockFlush(&mutex, std.testing.io, entryControls(original, &root)));
        try std.testing.expectEqual(@as(usize, 1), state.parks);
        try std.testing.expectEqual(@as(usize, 1), state.resumes);
        try std.testing.expect(!mutex.tryLock());
        try std.testing.expect(!root.held and root.borrowers.load(.acquire) == 0);
        try std.testing.expect(entry == &requester.entry.?);
        try std.testing.expectEqual(@as(usize, 0), scheduler.stats().runnable_slots);
        try std.testing.expectEqual(@as(usize, 1), scheduler.stats().operations);
        try std.testing.expectEqual(@as(u64, 23), scheduler.stats().retained_bytes);
    }
}

test "replication startup original lifetime pending append yields empty flush and preserves debt on rejection" {
    const native = @import("../../common/runnable_scheduler.zig");
    const Barrier = @import("antfly_runtime_abi").mutation_barrier.MutationBarrier;
    const Atomic = std.atomic.Value(bool);
    const Apply = @import("apply_rw_lock.zig").ApplyRwLock;
    const Signal = struct {
        canceled: bool = false,
        fn requested(raw: *const anyopaque) bool {
            return @as(*const @This(), @ptrCast(@alignCast(raw))).canceled;
        }
    };
    const Clock = struct {
        var now_ns: u64 = 0;
        fn now() u64 {
            return now_ns;
        }
    };
    const Owner = struct {
        core: struct {
            apply_mutex: *Apply,
            pub fn unlockApply(self: *@This()) void {
                self.apply_mutex.unlockExclusive();
            }
        },
        backend_runtime: struct {
            pub fn io(_: *const @This()) ?std.Io {
                return std.testing.io;
            }
        } = .{},
        async_context: struct { primary_replication_append_pending: Atomic = .init(true) } = .{},
        local_execution: struct {
            durable_replication_flush_mutex: std.Io.Mutex = .init,
            durable_replication_startup_barrier_pending: Atomic = .init(false),
            row_policy_replication_outbox_pending: Atomic = .init(false),
            durable_replication_outbox_maybe: Atomic = .init(false),
        } = .{},
        expected_root: *Barrier.RootSharedLease,
        flushes: usize = 0,
        pub fn enforcePortableRuntimeGate(_: *@This()) !void {}
        pub fn lockApplyForPortableRuntime(_: *@This()) !void {
            return error.UnexpectedLegacyAdmission;
        }
        pub fn flushDurableReplicationOutboxesWithAdmissionAndRoot(self: *@This(), controls: ?Controls, root: ?*Barrier.RootSharedLease) !void {
            try admission.checkLifetime(controls);
            try std.testing.expect(root == self.expected_root);
            self.flushes += 1;
            // The append has not published an outbox yet; this successful flush
            // cannot retire the pending append. Its actual owner must progress.
        }
        pub fn flushDurableReplicationOutboxesLockedWithAdmissionAndRoot(_: *@This(), _: ?Controls, _: ?*Barrier.RootSharedLease) !void {
            return error.UnexpectedLockedFlush;
        }
    };
    for ([_]?anyerror{ null, error.Canceled, error.DeadlineExceeded, error.SchedulerClosed }) |cause| {
        var scheduler = try native.Scheduler.init(.{ .slots = 2, .control_slots = 1 });
        defer scheduler.close();
        var append: native.Execution = .{};
        defer append.finish();
        var requester: native.Execution = .{};
        defer requester.finish();
        var barrier: Barrier = .{};
        var root = Barrier.RootSharedLease.initRetired(&barrier);
        defer root.release();
        var apply: Apply = .{ .io = std.testing.io };
        var owner: Owner = .{ .core = .{ .apply_mutex = &apply }, .expected_root = &root };
        try append.start(&scheduler, .{ .io = std.testing.io, .retained_bytes = 17 }, .background);
        try append.park(.resource_blocked);
        var signal: Signal = .{};
        Clock.now_ns = 0;
        try requester.start(&scheduler, .{ .io = std.testing.io, .retained_bytes = 23, .cancellation = .{ .ptr = &signal, .requested = Signal.requested }, .native_now_ns = Clock.now, .deadline_ns = if (cause) |rejection| (if (rejection == error.DeadlineExceeded) 100 else null) else null }, .write);
        const entry = &requester.entry.?;
        const State = struct {
            scheduler: *native.Scheduler,
            append: *native.Execution,
            requester: *native.Execution,
            pending: *Atomic,
            signal: *Signal,
            cause: ?anyerror,
            parks: usize = 0,
            resumes: usize = 0,
            fn check(raw: *const anyopaque) !void {
                try @as(*const @This(), @ptrCast(@alignCast(raw))).requester.checkLifetime();
            }
            fn begin(raw: *anyopaque) !void {
                const self: *@This() = @ptrCast(@alignCast(raw));
                self.parks += 1;
                try self.requester.park(.resource_blocked);
                try std.testing.expectEqual(@as(usize, 0), self.scheduler.stats().runnable_slots);
                try std.testing.expectEqual(@as(u64, 40), self.scheduler.stats().retained_bytes);
                if (self.cause) |rejection| {
                    switch (rejection) {
                        error.Canceled => self.signal.canceled = true,
                        error.DeadlineExceeded => Clock.now_ns = 101,
                        error.SchedulerClosed => self.scheduler.close(),
                        else => unreachable,
                    }
                } else {
                    var health: native.Execution = .{};
                    try health.start(self.scheduler, .{ .io = std.testing.io, .allow_wait = false }, .control);
                    health.finish();
                    try self.append.enter(.background);
                    self.pending.store(false, .release);
                    self.append.finish();
                }
            }
            fn end(raw: *anyopaque) !void {
                const self: *@This() = @ptrCast(@alignCast(raw));
                self.resumes += 1;
                try self.requester.enter(.write);
            }
        };
        var state: State = .{ .scheduler = &scheduler, .append = &append, .requester = &requester, .pending = &admission.ownerField(owner, "async_context").*.primary_replication_append_pending, .signal = &signal, .cause = cause };
        const result = ensureWithRoot(&owner, .{ .lifetime = .{ .context = &state, .check_fn = State.check }, .execution_wait = .{ .ptr = &state, .begin = State.begin, .end = State.end } }, &root);
        if (cause) |rejection| {
            try std.testing.expectError(rejection, result);
            try std.testing.expect(admission.ownerField(owner, "async_context").*.primary_replication_append_pending.load(.acquire));
            try std.testing.expectEqual(@as(usize, 0), scheduler.stats().runnable_slots);
            try std.testing.expectEqual(@as(usize, 2), scheduler.stats().operations);
            try std.testing.expectEqual(@as(u64, 40), scheduler.stats().retained_bytes);
        } else {
            try result;
            try std.testing.expect(!admission.ownerField(owner, "async_context").*.primary_replication_append_pending.load(.acquire));
            try std.testing.expectEqual(@as(usize, 1), scheduler.stats().operations);
            try std.testing.expectEqual(@as(u64, 23), scheduler.stats().retained_bytes);
        }
        try std.testing.expectEqual(@as(usize, 1), owner.flushes);
        try std.testing.expectEqual(@as(usize, 1), state.parks);
        try std.testing.expectEqual(@as(usize, 1), state.resumes);
        try std.testing.expect(entry == &requester.entry.?);
        try std.testing.expect(!root.held and root.borrowers.load(.acquire) == 0);
    }
}
