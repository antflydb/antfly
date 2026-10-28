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

//! Pre-decision apply admission shared by batches, BEGIN and intent prepare.
//! Mandatory decision/resolution paths retain their separate completion owner.
const lock = @import("apply_rw_lock.zig");

/// Retire nested mutation readers in acquisition order reversed, after the
/// local commit and replication tails are ordered. Mandatory retirement must
/// not consult cancellation or acquire a new execution owner.
pub const MutationRetirement = struct {
    current: *?@import("antfly_runtime_abi").mutation_barrier.MutationBarrier.SharedLease,
    enclosing: ?*?@import("antfly_runtime_abi").mutation_barrier.MutationBarrier.SharedLease = null,

    snapshot: ?*@import("snapshot_admission.zig").SnapshotAdmission.MutationLease = null,
    row_policy: ?*@import("row_policy_gate.zig").Gate.Lease = null,

    pub fn retire(self: @This()) void {
        if (self.snapshot) |lease| lease.release();
        release(self.current);
        if (self.enclosing) |outer| release(outer);
        if (self.row_policy) |lease| lease.release();
    }

    fn release(slot: *?@import("antfly_runtime_abi").mutation_barrier.MutationBarrier.SharedLease) void {
        if (slot.*) |*lease| lease.release();
        slot.* = null;
    }
};

/// Pre-apply generated computation may retire its owned policy and capture
/// admissions. The caller retains CPU for now; this scope never lends it while
/// a service holder is live. Snapshot-bearing callers use their retained path.
pub const SpeculativePreparation = struct {
    const Barrier = @import("antfly_runtime_abi").mutation_barrier.MutationBarrier;
    current: *?Barrier.SharedLease,
    enclosing: ?*?Barrier.SharedLease,
    root: ?*Barrier.RootSharedLease,
    controls: ?lock.ApplyRwLock.Acquisition,
    policy: ?@import("row_policy_gate.zig").Gate.Preparation = null,
    restore_current: bool,
    restore_enclosing: bool,
    active: bool = true,
    producer_active: bool = false,

    pub fn validateRetired(self: *const @This()) !void {
        if (!self.active or self.producer_active or self.current.* != null) return error.InvalidRunnableTransition;
        if (self.enclosing) |outer| if (outer.* != null) return error.InvalidRunnableTransition;
        if (self.root) |root| if (root.held or root.borrowers.load(.acquire) != 0) return error.InvalidRunnableTransition;
        if (self.policy) |policy| if (!policy.lease.released) return error.InvalidRunnableTransition;
    }

    pub fn begin(current: *?Barrier.SharedLease, enclosing: ?*?Barrier.SharedLease, root: ?*Barrier.RootSharedLease, policy: ?*@import("row_policy_gate.zig").Gate.Lease, controls: ?lock.ApplyRwLock.Acquisition) !@This() {
        try checkLifetime(controls);
        var readers: usize = 0;
        for ([_]?*?Barrier.SharedLease{ current, enclosing }) |slot| if (slot) |owned| {
            if (owned.*) |lease| {
                if (root == null or lease.root != root or lease.barrier != root.?.barrier) return error.InvalidRunnableTransition;
                readers += 1;
            }
        };
        if (enclosing) |outer| if (outer == current) return error.InvalidRunnableTransition;
        if (current.*) |lease| if (lease.phase_owner and readers != 1) return error.InvalidRunnableTransition;
        if (root) |shared| if (shared.borrowers.load(.acquire) != readers) return error.InvalidRunnableTransition;
        const self: @This() = .{
            .current = current,
            .enclosing = enclosing,
            .root = root,
            .controls = controls,
            .restore_current = current.* != null,
            .restore_enclosing = if (enclosing) |outer| outer.* != null else false,
            .policy = if (policy) |lease| try lease.retireForPreparation(nowSeconds()) else null,
        };
        (MutationRetirement{ .current = current, .enclosing = enclosing }).retire();
        if (root) |shared| try shared.retirePhase();
        return self;
    }

    pub fn reenter(self: *@This(), owner: anytype) !void {
        const root = self.root;
        const controls = self.controls;
        if (!self.active or self.producer_active) return error.InvalidRunnableTransition;
        self.active = false;
        try checkLifetime(controls);
        // Reacquire capture without waiting while the original CPU is held.
        // Every partial admission is retired if policy or lifetime changed.
        errdefer (MutationRetirement{ .current = self.current, .enclosing = self.enclosing }).retire();
        if (self.restore_enclosing) self.enclosing.?.* = try acquireReplicationMutationSharedPreDecisionScoped(owner, retainedLeaseControls(controls), root);
        if (self.restore_current) self.current.* = try acquireReplicationMutationSharedPreDecisionScoped(owner, retainedLeaseControls(controls), root);
        try checkLifetime(controls);
        if (self.policy) |*policy| try policy.reenter(nowSeconds());
    }

    fn nowSeconds() i64 {
        return @intCast(@divFloor(@import("antfly_platform").time.realtimeNs(), @import("std").time.ns_per_s));
    }
};

/// Synchronous authority for one resident publication attempt. The DB and
/// retry owner outlive it because teardown joins every claimed job. No request
/// borrow, fresh deadline, CPU transfer, or separately allocated owner is used.
pub const BackgroundPublication = struct {
    closing: *const @import("std").atomic.Value(bool),
    stopping: *const @import("std").atomic.Value(bool),

    pub fn controls(self: *const @This()) lock.ApplyRwLock.Acquisition {
        return .{ .lifetime = .{ .context = self, .check_fn = check } };
    }
    fn check(raw: *const anyopaque) !void {
        const self: *const @This() = @ptrCast(@alignCast(raw));
        if (self.closing.load(.acquire) or self.stopping.load(.acquire)) return error.Canceled;
    }
};

pub inline fn ownerField(owner: anytype, comptime name: []const u8) if (@hasDecl(@TypeOf(owner.*), "admissionOwnerField")) @TypeOf(owner.admissionOwnerField(name)) else @TypeOf(&@field(owner, name)) {
    if (comptime @hasDecl(@TypeOf(owner.*), "admissionOwnerField")) return owner.admissionOwnerField(name);
    return &@field(owner, name);
}

pub fn lockApply(owner: anytype, controls: ?lock.ApplyRwLock.Acquisition) !void {
    if (controls) |original| {
        try owner.enforcePortableRuntimeGate();
        try ownerField(owner, "core").*.apply_mutex.lockExclusiveControlledIo(
            ownerField(owner, "core").*.apply_mutex.io orelse return error.MissingBackendRuntimeIo,
            original,
        );
        errdefer ownerField(owner, "core").*.unlockApply();
        // Activation may close admission while the operation is parked.
        try owner.enforcePortableRuntimeGate();
    } else {
        try owner.lockApplyForPortableRuntime();
    }
}

/// One recovery attempt never parks behind an apply holder. The durable
/// marker remains authoritative when the caller returns pending.
pub fn tryLockApplyForRecovery(owner: anytype) !void {
    try owner.enforcePortableRuntimeGate();
    if (!ownerField(owner, "core").*.apply_mutex.tryLockExclusiveForAdmission()) return error.CommitCompletionPending;
    errdefer ownerField(owner, "core").*.unlockApply();
    try owner.enforcePortableRuntimeGate();
}

pub fn tryAcquireRecoveryMutationShared(owner: anytype, root: ?*@import("antfly_runtime_abi").mutation_barrier.MutationBarrier.RootSharedLease) !?@import("antfly_runtime_abi").mutation_barrier.MutationBarrier.SharedLease {
    const barrier = owner.replicationMutationBarrier() orelse {
        if (root != null) return error.InvalidArgument;
        return null;
    };
    if (root) |enclosing| {
        if (enclosing.barrier != barrier) return error.InvalidArgument;
        return (try enclosing.tryBorrowPhase()) orelse error.CommitCompletionPending;
    }
    return barrier.tryAcquireShared() orelse error.CommitCompletionPending;
}

/// Only pre-decision entrypoints may use retryable contention here. A caller
/// retaining snapshot/policy/replication leases cannot surrender CPU, so it
/// must not wait on an apply holder that may need that capacity to resume.
pub fn lockApplyPreDecision(owner: anytype, controls: ?lock.ApplyRwLock.Acquisition) !void {
    const original = controls orelse return lockApply(owner, null);
    if (original.execution_wait != null) return lockApply(owner, controls);
    try original.lifetime.check();
    try owner.enforcePortableRuntimeGate();
    if (!ownerField(owner, "core").*.apply_mutex.tryLockExclusiveForAdmission()) {
        try original.lifetime.check();
        return error.Backpressured;
    }
    errdefer ownerField(owner, "core").*.unlockApply();
    try original.lifetime.check();
    // Gate closure racing acquisition must release the acquired apply lock.
    try owner.enforcePortableRuntimeGate();
}

/// Pass the same original admission through the process-wide mutation/capture
/// gate before acquiring snapshot/apply service fences. No new owner or time.
pub fn acquireReplicationMutationShared(owner: anytype, controls: ?lock.ApplyRwLock.Acquisition) !?@import("antfly_runtime_abi").mutation_barrier.MutationBarrier.SharedLease {
    return acquireReplicationMutationSharedScoped(owner, controls, receiverMutationRoot(owner));
}

/// A completion attempt retaining mutation ownership cannot lend CPU to an
/// apply holder. Return to the durable retry owner rather than wait in a cycle.
pub fn lockApplyForCompletion(owner: anytype, controls: ?lock.ApplyRwLock.Acquisition) !void {
    const original = controls orelse return lockApply(owner, null);
    if (original.execution_wait != null) return lockApply(owner, controls);
    try original.lifetime.check();
    try owner.enforcePortableRuntimeGate();
    if (!ownerField(owner, "core").*.apply_mutex.tryLockExclusiveForAdmission()) {
        try original.lifetime.check();
        return error.CommitCompletionPending;
    }
    errdefer ownerField(owner, "core").*.unlockApply();
    try original.lifetime.check();
    try owner.enforcePortableRuntimeGate();
}

/// BEGIN has not acquired a local policy/apply lease at this point. Preserve
/// the caller's explicit wait policy while acquiring replication admission;
/// enterRaw must recheck policy after this returns. Callers with other holders
/// must already suppress CPU transfer in their supplied controls.
pub fn acquireBeginReplicationMutationShared(owner: anytype, controls: ?lock.ApplyRwLock.Acquisition) !?@import("antfly_runtime_abi").mutation_barrier.MutationBarrier.SharedLease {
    try checkLifetime(controls);
    if (ownerField(owner, "local_execution").*.row_policy_gate.currentPhase() != .disabled) return error.RowPolicyAuthenticationRequired;
    return acquireReplicationMutationSharedPreDecisionScoped(owner, controls, receiverMutationRoot(owner));
}

/// Explicit enclosing ownership belongs to an invocation, never to shared DB
/// state or an OS thread. Legacy callers keep their existing nesting behavior.
pub fn receiverMutationRoot(owner: anytype) ?*@import("antfly_runtime_abi").mutation_barrier.MutationBarrier.RootSharedLease {
    if (@hasField(@TypeOf(owner.*), "mutation_root")) return owner.mutation_root;
    return null;
}

/// A synchronous borrowed mutation receiver carries one invocation root into
/// helpers whose signatures do not have BatchExecutionOptions. Never write this
/// pointer into a shared DB or change an already established root identity.
pub const ReceiverRootBinding = struct {
    const Root = @import("antfly_runtime_abi").mutation_barrier.MutationBarrier.RootSharedLease;
    slot: ?*?*Root = null,
    previous: ?*Root = null,
    installed: ?*Root = null,
    pub fn release(self: *@This()) void {
        const slot = self.slot orelse return;
        @import("std").debug.assert(slot.* == self.installed);
        slot.* = self.previous;
        self.slot = null;
    }
};

pub fn bindReceiverMutationRoot(owner: anytype, requested: ?*@import("antfly_runtime_abi").mutation_barrier.MutationBarrier.RootSharedLease) !ReceiverRootBinding {
    if (comptime @hasField(@TypeOf(owner.*), "mutation_root")) {
        const previous = owner.mutation_root;
        const selected = requested orelse previous orelse return .{};
        if (previous) |current| if (current != selected) return error.InvalidArgument;
        // deinitScratch identifies the invocation-owned mutation receiver.
        // Guard inherited/matching roots too, so scope exit catches replacement
        // throughout retries and completion instead of only after installation.
        if (comptime @hasDecl(@TypeOf(owner.*), "deinitScratch") or @hasDecl(@TypeOf(owner.*), "admissionOwnerField")) {
            owner.mutation_root = selected;
            return .{ .slot = &owner.mutation_root, .previous = previous, .installed = selected };
        }
    }
    return .{};
}

/// Capture ownership for one synchronous controlled DB invocation. Keep this
/// stack slot stable through preparation retries, local commit and joined waits.
/// An explicit enclosing root is borrowed, never retired by scope destruction.
pub const InvocationRoot = struct {
    const Barrier = @import("antfly_runtime_abi").mutation_barrier.MutationBarrier;
    owned: ?Barrier.RootSharedLease = null,

    pub fn bind(self: *@This(), barrier: ?*Barrier, enclosing: ?*Barrier.RootSharedLease) !?*Barrier.RootSharedLease {
        @import("std").debug.assert(self.owned == null);
        if (enclosing) |root| {
            if (barrier == null or root.barrier != barrier.?) return error.InvalidArgument;
            return root;
        }
        const gate = barrier orelse return null;
        self.owned = Barrier.RootSharedLease.initRetired(gate);
        return &self.owned.?;
    }

    pub fn deinit(self: *@This()) void {
        if (self.owned) |*root| root.release();
        self.owned = null;
    }
};

pub fn acquireReplicationMutationSharedScoped(
    owner: anytype,
    controls: ?lock.ApplyRwLock.Acquisition,
    root: ?*@import("antfly_runtime_abi").mutation_barrier.MutationBarrier.RootSharedLease,
) !?@import("antfly_runtime_abi").mutation_barrier.MutationBarrier.SharedLease {
    return acquireReplicationMutationSharedWithPolicy(owner, controls, root, false);
}

/// Explicit foreground entry only. Mandatory recovery/reentry keeps the scoped
/// helper above, even when its controls cannot lend CPU.
pub fn acquireReplicationMutationSharedPreDecisionScoped(
    owner: anytype,
    controls: ?lock.ApplyRwLock.Acquisition,
    root: ?*@import("antfly_runtime_abi").mutation_barrier.MutationBarrier.RootSharedLease,
) !?@import("antfly_runtime_abi").mutation_barrier.MutationBarrier.SharedLease {
    return acquireReplicationMutationSharedWithPolicy(owner, controls, root, true);
}

fn acquireReplicationMutationSharedWithPolicy(
    owner: anytype,
    controls: ?lock.ApplyRwLock.Acquisition,
    root: ?*@import("antfly_runtime_abi").mutation_barrier.MutationBarrier.RootSharedLease,
    comptime pre_decision: bool,
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
            pub fn executionBoundary(self: *@This()) ?@import("../../common/execution_wait.zig").Boundary {
                // An already-held root only borrows and never waits here.
                // A retired phase owns no reader; retain the caller's explicit
                // wait policy. Snapshot/policy holders suppress it at ingress
                // with retainedLeaseControls rather than losing it globally.
                return if (self.original) |original| original.execution_wait else null;
            }
        };
        var check: Check = .{ .original = controls };
        if (enclosing.held) return try enclosing.borrowControlled(&check);
        if (pre_decision and controls != null) return try enclosing.borrowPhasePreDecisionControlled(ownerField(owner, "core").*.apply_mutex.io orelse return error.MissingBackendRuntimeIo, &check);
        return try enclosing.borrowPhaseControlled(ownerField(owner, "core").*.apply_mutex.io orelse return error.MissingBackendRuntimeIo, &check);
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
        if (pre_decision) return try barrier.acquireSharedPreDecisionControlled(ownerField(owner, "core").*.apply_mutex.io orelse return error.MissingBackendRuntimeIo, &adapter);
        return try barrier.acquireSharedControlled(ownerField(owner, "core").*.apply_mutex.io orelse return error.MissingBackendRuntimeIo, &adapter);
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

/// The caller has finished apply, graph publication and local shadow effects.
/// Remaining immutable plans/owned request buffers are memory, not service locks.
pub const CommittedVisibilityHolders = struct {
    snapshot: ?*const @import("snapshot_admission.zig").SnapshotAdmission.MutationLease = null,
    row_policy: ?*const @import("row_policy_gate.zig").Gate.Lease = null,
    root: ?*const @import("antfly_runtime_abi").mutation_barrier.MutationBarrier.RootSharedLease = null,
    capture_barrier_present: bool = false,
    mutation_reader_active: bool = false,
};

pub fn committedVisibilityBoundary(controls: ?lock.ApplyRwLock.Acquisition, holders: CommittedVisibilityHolders) ?@import("../../common/execution_wait.zig").Boundary {
    const original = controls orelse return null;
    if (holders.mutation_reader_active) return null;
    if (holders.snapshot) |lease| if (lease.active) return null;
    if (holders.row_policy) |lease| if (!lease.released) return null;
    if (holders.root) |root| {
        if (root.held or root.borrowers.load(.acquire) != 0) return null;
    } else if (holders.capture_barrier_present) return null;
    return original.execution_wait;
}

pub fn checkLifetime(controls: ?lock.ApplyRwLock.Acquisition) !void {
    if (controls) |original| try original.lifetime.check();
}

pub const IntentCollection = struct {
    const read = @import("../../common/read_admission.zig");
    original: ?lock.ApplyRwLock.Acquisition = null,
    io: ?@import("std").Io = null,
    payload: read.Payload = undefined,
    retained_physical_wait: bool = false,
    /// Publication retains the original finite authority and CPU while native
    /// mailbox I/O runs independently of apply/snapshot/policy service fences.
    pub fn bindPublication(self: *@This(), controls: ?lock.ApplyRwLock.Acquisition, io: ?@import("std").Io) ?read.Port {
        const port = self.bind(controls, io) orelse return null;
        self.retained_physical_wait = self.original.?.retained_physical_wait;
        return port;
    }
    pub fn bind(self: *@This(), controls: ?lock.ApplyRwLock.Acquisition, io: ?@import("std").Io) ?read.Port {
        return self.bindBeforeMutation(controls, io, null);
    }
    /// Only an explicit retired root proves this invocation owns no physical
    /// reader. Legacy entry and existing holder scopes retain their CPU policy.
    pub fn bindBeforeMutation(self: *@This(), controls: ?lock.ApplyRwLock.Acquisition, io: ?@import("std").Io, root: ?*@import("antfly_runtime_abi").mutation_barrier.MutationBarrier.RootSharedLease) ?read.Port {
        return self.bindWithPolicy(controls, io, root, false);
    }
    /// Advisory foreground pressure admission only, before this write has a
    /// decision. Non-lending ledger/progress contention is retryable rejection.
    pub fn bindPreDecisionBeforeMutation(self: *@This(), controls: ?lock.ApplyRwLock.Acquisition, io: ?@import("std").Io, root: ?*@import("antfly_runtime_abi").mutation_barrier.MutationBarrier.RootSharedLease) ?read.Port {
        return self.bindWithPolicy(controls, io, root, true);
    }
    fn bindWithPolicy(self: *@This(), controls: ?lock.ApplyRwLock.Acquisition, io: ?@import("std").Io, root: ?*@import("antfly_runtime_abi").mutation_barrier.MutationBarrier.RootSharedLease, comptime pre_decision: bool) ?read.Port {
        self.original = if (root) |enclosing|
            if (!enclosing.held and enclosing.borrowers.load(.acquire) == 0) controls else retainedLeaseControls(controls)
        else
            retainedLeaseControls(controls);
        self.io = io;
        self.retained_physical_wait = false;
        if (self.original == null) return null;
        self.payload = .{ .ptr = self, .vtable = &.{ .checkpoint = check, .wait_for_progress = if (pre_decision) waitPreDecision else wait, .check_lifetime = check, .retained_physical_wait = retainedPhysicalWait }, .execution_wait = if (pre_decision) self.original.?.execution_wait else null };
        return read.Port.init(&self.payload);
    }
    fn retainedPhysicalWait(raw: *anyopaque) !bool {
        const self: *@This() = @ptrCast(@alignCast(raw));
        return self.retained_physical_wait;
    }
    fn check(raw: *anyopaque) !void {
        const self: *@This() = @ptrCast(@alignCast(raw));
        try checkLifetime(self.original);
    }
    fn waitPreDecision(raw: *anyopaque) !void {
        const self: *@This() = @ptrCast(@alignCast(raw));
        try checkLifetime(self.original);
        const original = self.original orelse return error.ExecutionWaitUnsupported;
        if (original.execution_wait == null) return error.Backpressured;
        try wait(raw);
    }
    fn wait(raw: *anyopaque) !void {
        const self: *@This() = @ptrCast(@alignCast(raw));
        try checkLifetime(self.original);
        const io = self.io orelse return error.MissingBackendRuntimeIo;
        if (self.original) |original| {
            if (original.execution_wait) |boundary| {
                try boundary.run(waitTick, .{io});
            } else try waitTick(io);
        } else try waitTick(io);
        try checkLifetime(self.original);
    }
    fn waitTick(io: @import("std").Io) !void {
        try io.sleep(.fromMilliseconds(1), .awake);
    }
};

test "mutation apply admission pressure before retired mutation phase lends original CPU and preserves rejection" {
    const std = @import("std");
    const native = @import("../../common/runnable_scheduler.zig");
    const Barrier = @import("antfly_runtime_abi").mutation_barrier.MutationBarrier;
    const Manager = struct {
        blocked: bool = true,
        pub fn admissionDecisionWithAdmission(self: *@This(), _: void, _: u64, _: @import("../../common/read_admission.zig").Port) !struct {
            pressure: enum { hard, normal, soft },
            action: enum { throttle_writes, report, shrink_cache, defer_background_work, reject_work },
            hard_limit_bytes: u64,
        } {
            return .{ .pressure = if (self.blocked) .hard else .normal, .action = if (self.blocked) .throttle_writes else .report, .hard_limit_bytes = 100 };
        }
        pub fn canWaitForPressureChange(_: *@This()) bool {
            return true;
        }
    };
    for ([_]?anyerror{ null, error.Canceled, error.DeadlineExceeded, error.SchedulerClosed }) |cause| {
        var scheduler = try native.Scheduler.init(.{ .slots = 2, .control_slots = 1 });
        defer scheduler.close();
        var holder: native.Execution = .{};
        defer holder.finish();
        var writer: native.Execution = .{};
        defer writer.finish();
        try holder.start(&scheduler, .{ .io = std.testing.io, .retained_bytes = 17 }, .query);
        try holder.park(.resource_blocked);
        try writer.start(&scheduler, .{ .io = std.testing.io, .retained_bytes = 23 }, .write);
        const entry = &writer.entry.?;
        var barrier: Barrier = .{};
        var root = Barrier.RootSharedLease.initRetired(&barrier);
        defer root.release();
        var manager: Manager = .{};
        const State = struct {
            scheduler: *native.Scheduler,
            holder: *native.Execution,
            writer: *native.Execution,
            root: *Barrier.RootSharedLease,
            manager: *Manager,
            cause: ?anyerror,
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
                try std.testing.expect(!self.root.held and self.root.borrowers.load(.acquire) == 0);
                // Capture is no longer blocked by a preparation reader.
                var capture = self.root.barrier.acquireExclusive();
                capture.release();
                var health: native.Execution = .{};
                defer health.finish();
                try health.start(self.scheduler, .{ .io = std.testing.io, .allow_wait = false }, .control);
                try self.holder.enter(.query);
                if (self.cause == null) self.manager.blocked = false;
                try self.holder.park(.resource_blocked);
            }
            fn end(raw: *anyopaque) !void {
                const self: *@This() = @ptrCast(@alignCast(raw));
                self.resumes += 1;
                if (self.cause) |err| return err;
                try self.writer.enter(.write);
            }
        };
        var state: State = .{ .scheduler = &scheduler, .holder = &holder, .writer = &writer, .root = &root, .manager = &manager, .cause = cause };
        const controls: lock.ApplyRwLock.Acquisition = .{ .lifetime = .{ .context = &state, .check_fn = State.check }, .execution_wait = .{ .ptr = &state, .begin = State.begin, .end = State.end } };
        var adapter: IntentCollection = .{};
        const control = adapter.bindBeforeMutation(controls, std.testing.io, &root).?;
        const result = @import("../pressure_admission.zig").awaitAdmission(&manager, {}, 1, control);
        if (cause) |err| {
            try std.testing.expectError(err, result);
            try std.testing.expect(manager.blocked);
        } else {
            try result;
            try std.testing.expect(!manager.blocked);
        }
        try std.testing.expectEqual(@as(usize, 1), state.parks);
        try std.testing.expectEqual(@as(usize, 1), state.resumes);
        try std.testing.expect(entry == &writer.entry.?);
        try std.testing.expectEqual(@as(u64, 23), writer.entry.?.bytes);
        try std.testing.expectEqual(@as(usize, 2), scheduler.stats().operations);
        try std.testing.expectEqual(@as(u64, 40), scheduler.stats().retained_bytes);
        try std.testing.expect(!root.held and root.borrowers.load(.acquire) == 0);
        // Legacy and explicitly suppressed holder policies never acquire the
        // new wait boundary, even when the root is retired.
        _ = adapter.bindBeforeMutation(controls, std.testing.io, null);
        try std.testing.expect(adapter.original.?.execution_wait == null);
        _ = adapter.bindBeforeMutation(retainedLeaseControls(controls), std.testing.io, &root);
        try std.testing.expect(adapter.original.?.execution_wait == null);
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
    ownerField(&owner, "core").*.unlockApply();
    try std.testing.expect(mutex.tryLockExclusive());
    mutex.unlockExclusive();
}

test "mutation apply admission replication barrier preserves original CPU lifetime and capture intent" {
    const std = @import("std");
    const runtime = @import("antfly_runtime_abi");
    const native = @import("../../common/runnable_scheduler.zig");
    const budgets = @import("../../api/mutation_admission_controls.zig");
    const Barrier = runtime.mutation_barrier.MutationBarrier;
    const PolicyGate = @import("row_policy_gate.zig").Gate;
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
        local_execution: struct { row_policy_gate: PolicyGate = .{} } = .{},
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
        begin: bool = false,
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
                var lease = (try if (self.begin) acquireBeginReplicationMutationShared(self.owner, budgets.acquisition(&budget)) else acquireReplicationMutationShared(self.owner, budgets.acquisition(&budget))).?;
                defer lease.release();
                var policy: ?PolicyGate.Lease = if (self.begin) try self.owner.local_execution.row_policy_gate.enterRaw() else null;
                defer if (policy) |*held| held.release();
                if (self.begin) {
                    try std.testing.expectEqual(@as(usize, 1), self.owner.local_execution.row_policy_gate.readers.load(.acquire));
                    try std.testing.expect(retainedLeaseControls(budgets.acquisition(&budget)).?.execution_wait == null);
                }
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
    const Case = enum { success, canceled, expired, closed, policy_changed };
    const io = std.testing.io;
    for ([_]bool{ false, true }) |begin| {
        for ([_]bool{ false, true }) |exclusive| {
            if (begin and exclusive) continue;
            for (std.enums.values(Case)) |case| {
                if (case == .policy_changed and !begin) continue;
                Clock.current.store(0, .release);
                defer Clock.current.store(0, .release);
                var barrier: Barrier = .{};
                var owner: Owner = .{ .barrier = &barrier, .core = .{ .apply_mutex = .{ .io = io } } };
                var shared: ?Barrier.SharedLease = if (exclusive) barrier.acquireShared() else null;
                var captured: ?Barrier.ExclusiveLease = if (!exclusive) barrier.acquireExclusive() else null;
                var scheduler = try native.Scheduler.init(.{ .slots = 2, .control_slots = 1 });
                defer scheduler.close();
                var canceled: std.atomic.Value(bool) = .init(false);
                var worker: Worker = .{ .scheduler = &scheduler, .owner = &owner, .io = io, .canceled = &canceled, .exclusive = exclusive, .begin = begin };
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
                try std.testing.expectEqual(@as(usize, 0), ownerField(&owner, "local_execution").*.row_policy_gate.readers.load(.acquire));
                // A waiting BEGIN frees the sole ordinary slot as well as allowing
                // reserved control work. No policy lease is held while it waits.
                var ordinary: native.Execution = .{};
                defer ordinary.finish();
                try ordinary.start(&scheduler, .{ .io = io, .allow_wait = false }, .query);
                try ordinary.checkpoint();
                ordinary.finish();
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
                if (case == .policy_changed) try ownerField(&owner, "local_execution").*.row_policy_gate.beginPreparing(.disabled);
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
                        .policy_changed => error.RowPolicyAuthenticationRequired,
                        .success => unreachable,
                    };
                    try std.testing.expectEqual(@as(?anyerror, expected), worker.err);
                    try std.testing.expect(!worker.acquired);
                }
                try std.testing.expectEqual(@as(usize, 0), ownerField(&owner, "local_execution").*.row_policy_gate.readers.load(.acquire));
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
    ownerField(&owner, "core").*.apply_mutex.lockShared();
    var held = true;
    defer if (held) ownerField(&owner, "core").*.apply_mutex.unlockShared();
    try std.testing.expectError(error.DeadlineExceeded, lockApply(&owner, retainedLeaseControls(original)));
    try std.testing.expect(original.execution_wait != null);
    try std.testing.expectEqual(@as(usize, 3), state.checks);
    try std.testing.expectError(error.DeadlineExceeded, checkLifetime(retainedLeaseControls(original)));
    ownerField(&owner, "core").*.apply_mutex.unlockShared();
    held = false;
    try std.testing.expect(ownerField(&owner, "core").*.apply_mutex.tryLockExclusive());
    ownerField(&owner, "core").*.apply_mutex.unlockExclusive();
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

test "mutation apply admission retired root phase lends original CPU for capture and protected progress" {
    const std = @import("std");
    const native = @import("../../common/runnable_scheduler.zig");
    const Barrier = @import("antfly_runtime_abi").mutation_barrier.MutationBarrier;
    var scheduler = try native.Scheduler.init(.{ .slots = 2, .control_slots = 1 });
    defer scheduler.close();
    var holder: native.Execution = .{};
    defer holder.finish();
    var writer: native.Execution = .{};
    defer writer.finish();
    var barrier: Barrier = .{};
    try holder.start(&scheduler, .{ .io = std.testing.io, .retained_bytes = 17 }, .background);
    var exclusive = barrier.acquireExclusive();
    var held = true;
    defer if (held) exclusive.release();
    try holder.park(.resource_blocked);
    try writer.start(&scheduler, .{ .io = std.testing.io, .retained_bytes = 23 }, .write);
    const entry = &writer.entry.?;
    var root = Barrier.RootSharedLease.initRetired(&barrier);
    defer root.release();
    const Owner = struct {
        core: struct { apply_mutex: *lock.ApplyRwLock },
        barrier: *Barrier,
        pub fn replicationMutationBarrier(self: *@This()) ?*Barrier {
            return self.barrier;
        }
    };
    var mutex: lock.ApplyRwLock = .{ .io = std.testing.io };
    var owner: Owner = .{ .core = .{ .apply_mutex = &mutex }, .barrier = &barrier };
    const State = struct {
        scheduler: *native.Scheduler,
        holder: *native.Execution,
        writer: *native.Execution,
        exclusive: *Barrier.ExclusiveLease,
        held: *bool,
        parks: usize = 0,
        resumes: usize = 0,
        fn check(raw: *const anyopaque) !void {
            try @as(*const @This(), @ptrCast(@alignCast(raw))).writer.checkLifetime();
        }
        fn begin(raw: *anyopaque) !void {
            const self: *@This() = @ptrCast(@alignCast(raw));
            self.parks += 1;
            try self.writer.park(.resource_blocked);
            try std.testing.expectEqual(@as(usize, 0), self.scheduler.stats().runnable_slots);
            try std.testing.expectEqual(@as(usize, 2), self.scheduler.stats().operations);
            try std.testing.expectEqual(@as(u64, 40), self.scheduler.stats().retained_bytes);
            var control: native.Execution = .{};
            try control.start(self.scheduler, .{ .io = std.testing.io, .allow_wait = false }, .control);
            control.finish();
            try self.holder.enter(.background);
            self.exclusive.release();
            self.held.* = false;
            self.holder.finish();
        }
        fn end(raw: *anyopaque) !void {
            const self: *@This() = @ptrCast(@alignCast(raw));
            self.resumes += 1;
            try self.writer.enter(.write);
        }
    };
    var state: State = .{ .scheduler = &scheduler, .holder = &holder, .writer = &writer, .exclusive = &exclusive, .held = &held };
    const controls: lock.ApplyRwLock.Acquisition = .{ .lifetime = .{ .context = &state, .check_fn = State.check }, .execution_wait = .{ .ptr = &state, .begin = State.begin, .end = State.end } };
    var acquired = (try acquireReplicationMutationSharedScoped(&owner, controls, &root)).?;
    try std.testing.expect(acquired.phase_owner and root.held);
    try std.testing.expectEqual(@as(usize, 1), state.parks);
    try std.testing.expectEqual(state.parks, state.resumes);
    try std.testing.expect(entry == &writer.entry.?);
    try std.testing.expectEqual(@as(usize, 1), scheduler.stats().operations);
    try std.testing.expectEqual(@as(u64, 23), scheduler.stats().retained_bytes);
    // Nested holders borrow the same reader without another wait/CPU grant.
    var nested = (try acquireReplicationMutationSharedScoped(&owner, controls, &root)).?;
    try std.testing.expect(!nested.phase_owner);
    try std.testing.expectEqual(@as(usize, 1), state.parks);
    nested.release();
    acquired.release();
    try std.testing.expect(!root.held);
    try std.testing.expectEqual(@as(usize, 0), root.borrowers.load(.acquire));
    var capture = barrier.tryAcquireExclusive() orelse return error.LeakedMutationReader;
    capture.release();
}

test "mutation apply admission retired phase rejected reentry preserves other holder and original charge" {
    const std = @import("std");
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
    const Owner = struct {
        core: struct { apply_mutex: *lock.ApplyRwLock },
        barrier: *Barrier,
        pub fn replicationMutationBarrier(self: *@This()) ?*Barrier {
            return self.barrier;
        }
    };
    for ([_]anyerror{ error.Canceled, error.DeadlineExceeded, error.SchedulerClosed }) |cause| {
        var scheduler = try native.Scheduler.init(.{ .slots = 2, .control_slots = 1 });
        defer scheduler.close();
        var writer: native.Execution = .{};
        defer writer.finish();
        var signal: Signal = .{};
        Clock.now_ns = 0;
        try writer.start(&scheduler, .{ .io = std.testing.io, .cancellation = .{ .ptr = &signal, .requested = Signal.requested }, .native_now_ns = Clock.now, .deadline_ns = if (cause == error.DeadlineExceeded) 100 else null, .retained_bytes = 23 }, .write);
        const entry = &writer.entry.?;
        var barrier: Barrier = .{};
        var exclusive = barrier.acquireExclusive();
        defer exclusive.release();
        var root = Barrier.RootSharedLease.initRetired(&barrier);
        defer root.release();
        var mutex: lock.ApplyRwLock = .{ .io = std.testing.io };
        var owner: Owner = .{ .core = .{ .apply_mutex = &mutex }, .barrier = &barrier };
        const State = struct {
            writer: *native.Execution,
            scheduler: *native.Scheduler,
            signal: *Signal,
            cause: anyerror,
            parks: usize = 0,
            resumes: usize = 0,
            fn check(raw: *const anyopaque) !void {
                try @as(*const @This(), @ptrCast(@alignCast(raw))).writer.checkLifetime();
            }
            fn begin(raw: *anyopaque) !void {
                const self: *@This() = @ptrCast(@alignCast(raw));
                self.parks += 1;
                try self.writer.park(.resource_blocked);
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
                try self.writer.enter(.write);
            }
        };
        var state: State = .{ .writer = &writer, .scheduler = &scheduler, .signal = &signal, .cause = cause };
        try std.testing.expectError(cause, acquireReplicationMutationSharedScoped(&owner, .{ .lifetime = .{ .context = &state, .check_fn = State.check }, .execution_wait = .{ .ptr = &state, .begin = State.begin, .end = State.end } }, &root));
        try std.testing.expectEqual(@as(usize, 1), state.parks);
        try std.testing.expectEqual(@as(usize, 1), state.resumes);
        try std.testing.expect(!root.held and root.borrowers.load(.acquire) == 0);
        try std.testing.expect(barrier.tryAcquireExclusive() == null);
        try std.testing.expectEqual(@as(u64, 0), barrier.pendingSharedAcquisitions());
        try std.testing.expectEqual(@as(usize, 0), scheduler.stats().runnable_slots);
        try std.testing.expectEqual(@as(usize, 1), scheduler.stats().operations);
        try std.testing.expectEqual(@as(u64, 23), scheduler.stats().retained_bytes);
        try std.testing.expect(entry == &writer.entry.?);
    }
}

test "mutation apply admission retired phase retains caller suppression inside unconverted holder graph" {
    const std = @import("std");
    const native = @import("../../common/runnable_scheduler.zig");
    const Barrier = @import("antfly_runtime_abi").mutation_barrier.MutationBarrier;
    var scheduler = try native.Scheduler.init(.{ .slots = 2, .control_slots = 1 });
    defer scheduler.close();
    var writer: native.Execution = .{};
    defer writer.finish();
    try writer.start(&scheduler, .{ .io = std.testing.io, .retained_bytes = 23 }, .write);
    const entry = &writer.entry.?;
    var barrier: Barrier = .{};
    var exclusive = barrier.acquireExclusive();
    defer exclusive.release();
    var root = Barrier.RootSharedLease.initRetired(&barrier);
    defer root.release();
    const Owner = struct {
        core: struct { apply_mutex: *lock.ApplyRwLock },
        barrier: *Barrier,
        pub fn replicationMutationBarrier(self: *@This()) ?*Barrier {
            return self.barrier;
        }
    };
    var mutex: lock.ApplyRwLock = .{ .io = std.testing.io };
    var owner: Owner = .{ .core = .{ .apply_mutex = &mutex }, .barrier = &barrier };
    const State = struct {
        writer: *native.Execution,
        checks: usize = 0,
        fn check(raw: *const anyopaque) !void {
            const self: *@This() = @constCast(@as(*const @This(), @ptrCast(@alignCast(raw))));
            try self.writer.checkLifetime();
            self.checks += 1;
            if (self.checks == 3) return error.DeadlineExceeded;
        }
        fn forbidden(_: *anyopaque) !void {
            @panic("caller retained controls must not transfer CPU");
        }
    };
    var state: State = .{ .writer = &writer };
    const original: lock.ApplyRwLock.Acquisition = .{ .lifetime = .{ .context = &state, .check_fn = State.check }, .execution_wait = .{ .ptr = &state, .begin = State.forbidden, .end = State.forbidden } };
    try std.testing.expectError(error.DeadlineExceeded, acquireReplicationMutationSharedScoped(&owner, retainedLeaseControls(original), &root));
    try std.testing.expectEqual(@as(usize, 3), state.checks);
    try std.testing.expect(!root.held and root.borrowers.load(.acquire) == 0);
    try std.testing.expectEqual(@as(usize, 1), scheduler.stats().runnable_slots);
    try std.testing.expectEqual(@as(usize, 1), scheduler.stats().operations);
    try std.testing.expectEqual(@as(u64, 23), scheduler.stats().retained_bytes);
    try std.testing.expect(entry == &writer.entry.?);
    try std.testing.expect(barrier.tryAcquireExclusive() == null);
    try std.testing.expectEqual(@as(u64, 0), barrier.pendingSharedAcquisitions());
}

test "mutation apply admission scoped receiver carries explicit root into nested helpers behind queued capture" {
    const std = @import("std");
    const Barrier = @import("antfly_runtime_abi").mutation_barrier.MutationBarrier;
    const Owner = struct {
        mutation_root: ?*Barrier.RootSharedLease = null,
        core: struct { apply_mutex: *lock.ApplyRwLock },
        barrier: *Barrier,
        pub fn replicationMutationBarrier(self: *@This()) ?*Barrier {
            return self.barrier;
        }
        pub fn deinitScratch(_: *@This()) void {}
        fn nested(self: *@This()) !void {
            var lease = (try acquireReplicationMutationShared(self, null)).?;
            defer lease.release();
            try std.testing.expect(lease.root == self.mutation_root);
            try std.testing.expect(!lease.phase_owner);
        }
        fn invoke(self: *@This(), root: *Barrier.RootSharedLease, reject: bool) !void {
            var binding = try bindReceiverMutationRoot(self, root);
            defer binding.release();
            try self.nested();
            // A nested batch binding accepts the same root without resetting it.
            var inner = try bindReceiverMutationRoot(self, root);
            defer inner.release();
            try self.nested();
            if (reject) return error.Canceled;
        }
    };
    const Check = struct {
        pub fn checkpoint(_: *@This()) !void {}
        pub fn executionBoundary(_: *@This()) ?@import("../../common/execution_wait.zig").Boundary {
            return null;
        }
    };
    var check: Check = .{};
    var barrier: Barrier = .{};
    var root = try barrier.acquireSharedRootControlled(std.testing.io, &check);
    defer root.release();
    var mutex: lock.ApplyRwLock = .{ .io = std.testing.io };
    var owner: Owner = .{ .core = .{ .apply_mutex = &mutex }, .barrier = &barrier };
    // A queued capture closes the reader gate. Nested acquisition must borrow
    // this operation's existing reader rather than enter behind that writer.
    try std.testing.expect(barrier.reader_gate.tryLock());
    defer barrier.reader_gate.unlock();
    try owner.invoke(&root, false);
    try std.testing.expect(owner.mutation_root == null);
    try std.testing.expectEqual(@as(usize, 0), root.borrowers.load(.acquire));
    try std.testing.expectError(error.Canceled, owner.invoke(&root, true));
    try std.testing.expect(owner.mutation_root == null);
    try std.testing.expect(root.held and root.borrowers.load(.acquire) == 0);
}

test "mutation apply admission receiver binding rejects root replacement and never mutates shared DB descriptors" {
    const std = @import("std");
    const Barrier = @import("antfly_runtime_abi").mutation_barrier.MutationBarrier;
    const Receiver = struct {
        mutation_root: ?*Barrier.RootSharedLease = null,
        pub fn deinitScratch(_: *@This()) void {}
    };
    const Descriptor = struct { mutation_root: ?*Barrier.RootSharedLease = null };
    var barrier: Barrier = .{};
    var first = Barrier.RootSharedLease.initRetired(&barrier);
    defer first.release();
    var second = Barrier.RootSharedLease.initRetired(&barrier);
    defer second.release();
    var receiver: Receiver = .{};
    var binding = try bindReceiverMutationRoot(&receiver, &first);
    defer binding.release();
    try std.testing.expect(receiverMutationRoot(&receiver) == &first);
    try std.testing.expectError(error.InvalidArgument, bindReceiverMutationRoot(&receiver, &second));
    try std.testing.expect(receiver.mutation_root == &first);
    var inherited = try bindReceiverMutationRoot(&receiver, null);
    inherited.release();
    try std.testing.expect(receiver.mutation_root == &first);
    binding.release();
    try std.testing.expect(receiver.mutation_root == null);
    binding.release();
    var descriptor: Descriptor = .{};
    var ignored = try bindReceiverMutationRoot(&descriptor, &first);
    ignored.release();
    try std.testing.expect(descriptor.mutation_root == null);
    var shared: struct {} = .{};
    var absent = try bindReceiverMutationRoot(&shared, &first);
    absent.release();
}

test "mutation apply admission committed holder retirement releases owned policy and snapshot without touching borrowed readers" {
    const std = @import("std");
    const PolicyGate = @import("row_policy_gate.zig").Gate;
    const Snapshot = @import("snapshot_admission.zig").SnapshotAdmission;
    const Barrier = @import("antfly_runtime_abi").mutation_barrier.MutationBarrier;
    const native = @import("../../common/runnable_scheduler.zig");
    for ([_]bool{ false, true }) |retain_other_readers| {
        var scheduler = try native.Scheduler.init(.{ .slots = 2 });
        defer scheduler.close();
        var execution: native.Execution = .{};
        try execution.start(&scheduler, .{ .io = std.testing.io, .retained_bytes = 23 }, .write);
        defer execution.finish();
        var policy = PolicyGate.init(.{ .mode_initialized = true, .storage_mode = .relational, .active_schema_version = 3 });
        var owned_policy = try policy.enterRaw();
        defer owned_policy.release();
        var other_policy: ?PolicyGate.Lease = if (retain_other_readers) owned_policy.clone() else null;
        defer if (other_policy) |*lease| lease.release();
        try policy.beginPreparing(.disabled);
        var snapshot_admission: Snapshot = .{};
        var owned_snapshot = snapshot_admission.acquireMutation();
        defer owned_snapshot.release();
        var other_snapshot: ?Snapshot.MutationLease = if (retain_other_readers) owned_snapshot.retain() else null;
        defer if (other_snapshot) |*lease| lease.release();
        var barrier: Barrier = .{};
        var outer: ?Barrier.SharedLease = barrier.acquireShared();
        defer if (outer) |*lease| lease.release();
        var inner: ?Barrier.SharedLease = barrier.acquireShared();
        defer if (inner) |*lease| lease.release();
        try std.testing.expect(!policy.quiesced());
        try std.testing.expect(!snapshot_admission.lock.tryLockExclusive());
        try std.testing.expect(barrier.tryAcquireExclusive() == null);
        // Mandatory post-commit retirement cannot consult an expired request
        // or ask for fresh CPU. The runnable/memory owner remains unchanged.
        execution.entry.?.options.deadline_ns = 0;
        const retirement: MutationRetirement = .{ .current = &inner, .enclosing = &outer, .snapshot = &owned_snapshot, .row_policy = &owned_policy };
        retirement.retire();
        retirement.retire();
        try std.testing.expect(owned_policy.released and !owned_snapshot.active);
        try std.testing.expect(inner == null and outer == null);
        var capture = barrier.tryAcquireExclusive() orelse return error.TestUnexpectedResult;
        capture.release();
        try std.testing.expectEqual(!retain_other_readers, policy.quiesced());
        try std.testing.expectEqual(@as(usize, 1), scheduler.stats().runnable_slots);
        try std.testing.expectEqual(@as(usize, 1), scheduler.stats().operations);
        try std.testing.expectEqual(@as(u64, 23), scheduler.stats().retained_bytes);
        if (retain_other_readers) {
            try std.testing.expect(!other_policy.?.released and other_snapshot.?.active);
            try std.testing.expect(!snapshot_admission.lock.tryLockExclusive());
            other_policy.?.release();
            other_snapshot.?.release();
        }
        try std.testing.expect(policy.quiesced());
        try std.testing.expect(snapshot_admission.lock.tryLockExclusive());
        snapshot_admission.lock.unlockExclusive();
        try policy.publishCommitted(.{ .mode_initialized = true, .storage_mode = .relational, .active_schema_version = 3, .row_policy_phase = .active, .row_policy_generation = 7, .row_policy_catalog_epoch = 9 });
    }
}

test "mutation apply admission committed visibility lends only after explicit holder retirement" {
    const std = @import("std");
    const Barrier = @import("antfly_runtime_abi").mutation_barrier.MutationBarrier;
    const Policy = @import("row_policy_gate.zig").Gate;
    const Snapshot = @import("snapshot_admission.zig").SnapshotAdmission;
    const Check = struct {
        fn lifetime(_: *const anyopaque) !void {}
        fn wait(_: *anyopaque) !void {
            return error.TestUnexpectedResult;
        }
        pub fn checkpoint(_: *@This()) !void {}
        pub fn executionBoundary(_: *@This()) ?@import("../../common/execution_wait.zig").Boundary {
            return null;
        }
    };
    var check: Check = .{};
    const controls: lock.ApplyRwLock.Acquisition = .{ .lifetime = .{ .context = &check, .check_fn = Check.lifetime }, .execution_wait = .{ .ptr = &check, .begin = Check.wait, .end = Check.wait } };
    var barrier: Barrier = .{};
    var root = Barrier.RootSharedLease.initRetired(&barrier);
    defer root.release();
    var policy = Policy.init(.{ .mode_initialized = true, .storage_mode = .relational, .active_schema_version = 3 });
    var lease = try policy.enterRaw();
    defer lease.release();
    var snapshots: Snapshot = .{};
    var snapshot = snapshots.acquireMutation();
    defer snapshot.release();
    var holders: CommittedVisibilityHolders = .{ .root = &root, .capture_barrier_present = true, .row_policy = &lease, .snapshot = &snapshot };
    try std.testing.expect(committedVisibilityBoundary(controls, holders) == null);
    lease.release();
    try std.testing.expect(committedVisibilityBoundary(controls, holders) == null);
    snapshot.release();
    try std.testing.expect(committedVisibilityBoundary(controls, holders).?.ptr == @as(*anyopaque, @ptrCast(&check)));
    try root.reenterPhaseControlled(std.testing.io, &check);
    try std.testing.expect(committedVisibilityBoundary(controls, holders) == null);
    try root.retirePhase();
    holders.mutation_reader_active = true;
    try std.testing.expect(committedVisibilityBoundary(controls, holders) == null);
    holders.mutation_reader_active = false;
    holders.root = null;
    try std.testing.expect(committedVisibilityBoundary(controls, holders) == null);
    holders.capture_barrier_present = false;
    try std.testing.expect(committedVisibilityBoundary(controls, holders).?.ptr == @as(*anyopaque, @ptrCast(&check)));
    try std.testing.expect(committedVisibilityBoundary(null, holders) == null);
    var suppressed = controls;
    suppressed.execution_wait = null;
    try std.testing.expect(committedVisibilityBoundary(suppressed, holders) == null);
}

test "mutation apply admission invocation root stays independent of same-thread legacy ownership" {
    const std = @import("std");
    const Barrier = @import("antfly_runtime_abi").mutation_barrier.MutationBarrier;
    const Owner = struct {
        barrier: *Barrier,
        core: struct { apply_mutex: struct { io: ?std.Io } },
        pub fn replicationMutationBarrier(self: *@This()) ?*Barrier {
            return self.barrier;
        }
    };
    const Check = struct {
        cause: ?anyerror = null,
        fn check(raw: *const anyopaque) !void {
            const self: *const @This() = @ptrCast(@alignCast(raw));
            if (self.cause) |cause| return cause;
        }
    };
    var barrier: Barrier = .{};
    var legacy = barrier.acquireShared();
    defer legacy.release();
    var invocation: InvocationRoot = .{};
    defer invocation.deinit();
    const root = (try invocation.bind(&barrier, null)).?;
    try std.testing.expect(!root.held and root.borrowers.load(.acquire) == 0);
    var owner: Owner = .{ .barrier = &barrier, .core = .{ .apply_mutex = .{ .io = std.testing.io } } };
    var check: Check = .{};
    const controls: lock.ApplyRwLock.Acquisition = .{ .lifetime = .{ .context = &check, .check_fn = Check.check } };
    // Physical counts distinguish this operation from TLS nesting, which would
    // merely increment the other operation's same-thread depth.
    for (0..2) |_| {
        var borrow = (try acquireReplicationMutationSharedScoped(&owner, controls, root)).?;
        try std.testing.expect(root.held and root.borrowers.load(.acquire) == 1);
        try std.testing.expectEqual(@as(usize, 2), barrier.reader_count);
        borrow.release();
        try std.testing.expect(!root.held and root.borrowers.load(.acquire) == 0);
        try std.testing.expectEqual(@as(usize, 1), barrier.reader_count);
        try std.testing.expect(barrier.tryAcquireExclusive() == null);
    }
    for ([_]anyerror{ error.Canceled, error.DeadlineExceeded, error.SchedulerClosed, error.CatalogGenerationChanged }) |cause| {
        check.cause = cause;
        try std.testing.expectError(cause, acquireReplicationMutationSharedScoped(&owner, controls, root));
        try std.testing.expect(!root.held and root.borrowers.load(.acquire) == 0);
        try std.testing.expectEqual(@as(usize, 1), barrier.reader_count);
    }
    invocation.deinit();
    try std.testing.expectEqual(@as(usize, 1), barrier.reader_count);
}

test "mutation apply admission invocation root borrows explicit enclosing identity and rejects foreign barrier" {
    const std = @import("std");
    const Barrier = @import("antfly_runtime_abi").mutation_barrier.MutationBarrier;
    const Check = struct {
        pub fn checkpoint(_: *@This()) !void {}
        pub fn executionBoundary(_: *@This()) ?@import("../../common/execution_wait.zig").Boundary {
            return null;
        }
    };
    var check: Check = .{};
    var barrier: Barrier = .{};
    var other_barrier: Barrier = .{};
    var enclosing = try barrier.acquireSharedRootControlled(std.testing.io, &check);
    defer enclosing.release();
    var invocation: InvocationRoot = .{};
    try std.testing.expect((try invocation.bind(&barrier, &enclosing)).? == &enclosing);
    invocation.deinit();
    try std.testing.expect(enclosing.held and enclosing.borrowers.load(.acquire) == 0);
    try std.testing.expectEqual(@as(usize, 1), barrier.reader_count);
    try std.testing.expectError(error.InvalidArgument, invocation.bind(&other_barrier, &enclosing));
    try std.testing.expectError(error.InvalidArgument, invocation.bind(null, &enclosing));
    try std.testing.expect((try invocation.bind(null, null)) == null);
    try std.testing.expect(enclosing.held);
}
