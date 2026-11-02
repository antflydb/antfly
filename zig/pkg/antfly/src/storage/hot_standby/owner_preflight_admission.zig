// Copyright 2026 Antfly, Inc.
// SPDX-License-Identifier: Elastic-2.0
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
const sources = @import("antfly_local_sources");
const read_control = sources.common_read_admission;
const Context = sources.api_operation.RequestContext;
const Mirror = sources.storage_db_replication_contract.AsyncEffectMirror;
const Barrier = @import("antfly_runtime_abi").mutation_barrier.MutationBarrier;

const Runnable = @import("antfly_runtime_abi").runnable_abi.Borrowed;
pub fn checkOriginal(context: Context, runnable: ?Runnable) !void {
    if (runnable) |original| {
        try original.checkLifetime();
    }
    var checked_context = context;
    if (context.cancellation.check_fn != null) {
        try context.cancellation.check();
        // ensureActive's legacy boolean adapter must not recheck and collapse
        // a scoped one-shot cause. Keep its original clock/deadline unchanged.
        checked_context.cancellation = .none;
    }
    try checked_context.ensureActive();
}

const Budget = struct {
    context: Context,
    runnable: ?Runnable = null,
    io: std.Io,
    pub fn checkpoint(scope: *@This()) !void {
        try checkOriginal(scope.context, scope.runnable);
    }
    pub fn waitForProgress(scope: *@This()) !void {
        try scope.checkpoint();
        try scope.io.sleep(.fromMilliseconds(1), .awake);
        try scope.checkpoint();
    }
    pub fn executionBoundary(_: *@This()) ?sources.common_execution_wait.Boundary {
        return null;
    }
};

/// Keep this session at a stable address through the synchronous C owner call.
pub const Session = struct {
    budget: Budget = undefined,
    visibility_budget: TransitionBudget = undefined,
    adapter: read_control.Adapter(*Budget) = undefined,
    payload: read_control.Payload = undefined,
    pub fn bind(scope: *@This(), context: Context, io: std.Io) !read_control.Port {
        return scope.bindWithRunnable(context, io, null);
    }
    pub fn bindWithRunnable(scope: *@This(), context: Context, io: std.Io, runnable: ?Runnable) !read_control.Port {
        try checkOriginal(context, runnable);
        scope.budget = .{ .context = context, .io = io, .runnable = runnable };
        scope.visibility_budget = .{ .controls = .{ .context = context, .execution_runnable = runnable }, .io = io };
        scope.adapter = .{ .budget = &scope.budget };
        scope.payload = scope.adapter.payload();
        return read_control.Port.init(&scope.payload);
    }
    /// Native commit must retire capture, snapshot, apply and policy holders
    /// before selecting this boundary. It never changes pre-decision admission.
    pub fn committedVisibilityBoundary(scope: *@This()) ?sources.common_execution_wait.Boundary {
        return scope.visibility_budget.executionBoundary();
    }
};

const TransitionControls = struct {
    execution_runnable: ?Runnable = null,
    context: Context,
    require_checked_reentry: bool = true,
    wait_deadline_ns: ?u64 = null,
};
fn checkTransition(controls: TransitionControls) !void {
    try checkOriginal(controls.context, controls.execution_runnable);
    if (controls.wait_deadline_ns) |deadline|
        if (@import("antfly_platform").time.monotonicNs() >= deadline) return error.StorageBusy;
}
const TransitionBudget = sources.common_read_execution_budget.Budget(TransitionControls, checkTransition);

/// One attempt has returned without an owner lease. Retire capture before the
/// transition sleep, reenter original CPU, then restore capture before retrying
/// physical open/configuration. The caller keeps this scope and slots stable.
pub const OwnerTransition = struct {
    budget: TransitionBudget,
    root: ?*Barrier.RootSharedLease,
    parent: *?Barrier.SharedLease,
    gate: ?sources.storage_db_replication_contract.WriteGate = null,

    pub fn init(context: Context, runnable: ?Runnable, io: std.Io, root: ?*Barrier.RootSharedLease, parent: *?Barrier.SharedLease, gate: ?sources.storage_db_replication_contract.WriteGate) @This() {
        return .{ .budget = .{ .controls = .{ .context = context, .execution_runnable = runnable }, .io = io }, .root = root, .parent = parent, .gate = gate };
    }
    pub fn executionIo(self: *const @This()) std.Io {
        return self.budget.io;
    }
    pub fn wait(self: *@This(), duration: std.Io.Duration, deadline_ns: u64) !void {
        self.budget.controls.wait_deadline_ns = deadline_ns;
        return self.boundary().run(std.Io.sleep, .{ self.budget.io, duration, std.Io.Clock.awake });
    }
    pub fn boundary(self: *@This()) sources.common_execution_wait.Boundary {
        return .{ .ptr = self, .begin = begin, .end = end };
    }
    fn begin(raw: *anyopaque) !void {
        const self: *@This() = @ptrCast(@alignCast(raw));
        try self.budget.checkpoint();
        if (self.parent.*) |*lease| {
            const root = self.root orelse return error.InvalidRunnableTransition;
            if (lease.root != root or root.borrowers.load(.acquire) != 1) return error.InvalidRunnableTransition;
            lease.release();
            self.parent.* = null;
        }
        if (self.root) |root| if (root.held or root.borrowers.load(.acquire) != 0) return error.InvalidRunnableTransition;
        if (self.budget.executionBoundary()) |original| try original.beginWait();
    }
    fn end(raw: *anyopaque) !void {
        const self: *@This() = @ptrCast(@alignCast(raw));
        if (self.budget.executionBoundary()) |original| try original.endWait();
        try self.budget.checkpoint();
        if (self.gate) |gate| try gate.check();
        if (self.root) |root| {
            if (self.parent.* != null) return error.InvalidRunnableTransition;
            self.parent.* = try root.borrowPhaseControlled(self.budget.io, &self.budget);
        }
        try self.budget.checkpoint();
        if (self.gate) |gate| try gate.check();
    }
};

/// Caller retains its legacy mutation/service holder and CPU policy. Borrow
/// the original clock and cancellation for this pre-decision scope only.
pub fn acquireMutation(mirror: ?Mirror, context: Context, io: std.Io) !?Barrier.SharedLease {
    return acquireMutationWithRunnable(mirror, context, io, null);
}
pub fn acquireMutationWithRunnable(mirror: ?Mirror, context: Context, io: std.Io, runnable: ?Runnable) !?Barrier.SharedLease {
    try checkOriginal(context, runnable);
    const configured = mirror orelse return null;
    const barrier = configured.mutation_barrier orelse return null;
    var budget: Budget = .{ .context = context, .io = io, .runnable = runnable };
    return try barrier.acquireSharedControlled(io, &budget);
}

pub fn acquireMutationWithRoot(mirror: ?Mirror, context: Context, io: std.Io, runnable: ?Runnable, root: *Barrier.RootSharedLease) !?Barrier.SharedLease {
    try checkOriginal(context, runnable);
    const barrier = if (mirror) |configured| configured.mutation_barrier else null;
    if (barrier == null or root.barrier != barrier.?) return error.InvalidArgument;
    var budget: Budget = .{ .context = context, .io = io, .runnable = runnable };
    return try root.borrowPhaseControlled(io, &budget);
}

pub fn preflight(mirror: ?Mirror, context: Context, io: std.Io) !void {
    return preflightWithRunnable(mirror, context, io, null);
}
pub fn preflightWithRunnable(mirror: ?Mirror, context: Context, io: std.Io, runnable: ?Runnable) !void {
    try checkOriginal(context, runnable);
    const configured = mirror orelse return;
    var budget: Budget = .{ .context = context, .io = io, .runnable = runnable };
    var adapter: read_control.Adapter(*Budget) = .{ .budget = &budget };
    const payload = adapter.payload();
    try configured.publisher.preflightWithAdmission(configured, true, read_control.Port.init(&payload));
}

test "controlled owner preflight checks runnable lifetime without yielding its held CPU grant" {
    const runtime = @import("antfly_runtime_abi");
    const State = struct {
        lifetimes: usize = 0,
        quantum: usize = 0,
        phases: usize = 0,
        failure: ?anyerror = null,
        fn lifetime(raw: *anyopaque) callconv(.c) runtime.error_abi.Status {
            const scope: *@This() = @ptrCast(@alignCast(raw));
            scope.lifetimes += 1;
            return if (scope.failure) |err| runtime.error_abi.statusFromError(err) else .ok;
        }
        fn checkpoint(raw: *anyopaque) callconv(.c) runtime.error_abi.Status {
            const scope: *@This() = @ptrCast(@alignCast(raw));
            scope.quantum += 1;
            return .ok;
        }
        fn phase(raw: *anyopaque, _: c_int) callconv(.c) runtime.error_abi.Status {
            const scope: *@This() = @ptrCast(@alignCast(raw));
            scope.phases += 1;
            return .ok;
        }
    };
    var state: State = .{};
    var runnable: Runnable = .{ .context = &state, .cpu_slots = 1, .parent_class = 1, .checkpoint = State.checkpoint, .park = State.phase, .enter = State.phase, .check_lifetime = State.lifetime };
    var session: Session = .{};
    const port = try session.bindWithRunnable(.{}, std.testing.io, runnable);
    var native_borrow: sources.api_batch_execution_admission.Borrow = .{};
    const controls = try native_borrow.bind(&port);
    try controls.lifetime.check();
    try std.testing.expect(state.lifetimes >= 3);
    for ([_]anyerror{ error.Canceled, error.DeadlineExceeded, error.SchedulerClosed }) |cause| {
        state.failure = cause;
        try std.testing.expectError(cause, controls.lifetime.check());
    }
    try std.testing.expectEqual(@as(usize, 0), state.quantum);
    try std.testing.expectEqual(@as(usize, 0), state.phases);
    runnable.check_lifetime = null;
    try std.testing.expectError(error.UnsupportedOperation, session.bindWithRunnable(.{}, std.testing.io, runnable));
    try std.testing.expectEqual(@as(usize, 0), state.quantum);
}

test "controlled owner preflight retains original cancellation and records decisions without new authority" {
    const State = struct {
        checks: usize = 0,
        fail_at: usize = std.math.maxInt(usize),
        cause: anyerror = error.Canceled,
        calls: usize = 0,
        recorded: bool = false,
        fn check(raw: *const anyopaque) !void {
            const scope: *@This() = @ptrCast(@alignCast(@constCast(raw)));
            scope.checks += 1;
            if (scope.checks >= scope.fail_at) return scope.cause;
        }
        fn next(_: *anyopaque) u64 {
            return 1;
        }
        fn identity(_: *anyopaque) sources.storage_db_replication_contract.Publisher.Identity {
            return .{ .table_id = 1, .shard_id = 2, .timeline_id = 3, .epoch = 4 };
        }
        fn publish(_: Mirror, _: sources.storage_db_durable_outbox.Kind, _: []const u8, _: sources.storage_db_doc_identity_namespace.Namespace) !u64 {
            return error.UnexpectedPublication;
        }
        fn recover(_: Mirror, _: sources.storage_db_durable_outbox.Kind, _: sources.storage_db_durable_outbox.DurableReplicationOutbox, _: sources.storage_db_doc_identity_namespace.Namespace) !u64 {
            return error.UnexpectedPublication;
        }
        fn legacy(_: Mirror, _: bool) !void {
            return error.UnexpectedLegacyPreflight;
        }
        fn complete(_: Mirror, _: u64) !void {
            return error.UnexpectedCompletion;
        }
        fn admitted(mirror: Mirror, record: bool, control: read_control.Port) !void {
            const scope: *@This() = @ptrCast(@alignCast(mirror.publisher.ptr));
            scope.calls += 1;
            scope.recorded = record;
            try std.testing.expect((try control.executionWait()) == null);
            try control.checkpoint();
        }
    };
    var state: State = .{};
    var barrier: Barrier = .{};
    const mirror: Mirror = .{ .publisher = .{ .ptr = &state, .vtable = &.{ .next_lsn = State.next, .identity = State.identity, .publish = State.publish, .recover = State.recover, .preflight = State.legacy, .preflight_with_admission = State.admitted, .complete = State.complete } }, .mutation_barrier = &barrier };
    const context: Context = .{ .cancellation = .{ .ptr = &state, .check_fn = State.check } };
    var lease = (try acquireMutation(mirror, context, std.testing.io)).?;
    try preflight(mirror, context, std.testing.io);
    try std.testing.expect(state.recorded and state.calls == 1);
    lease.release();
    const checks = state.checks;
    for ([_]anyerror{error.Canceled}) |cause| {
        for (1..checks + 1) |fail_at| {
            state = .{ .fail_at = fail_at, .cause = cause };
            const result = acquireMutation(mirror, context, std.testing.io);
            if (result) |acquired| {
                var held = acquired.?;
                defer held.release();
                try std.testing.expectError(cause, preflight(mirror, context, std.testing.io));
            } else |err| try std.testing.expectEqual(cause, err);
            try std.testing.expectEqual(@as(usize, 0), barrier.reader_count);
        }
        try std.testing.expect(barrier.reader_gate.tryLock());
        defer barrier.reader_gate.unlock();
        state = .{ .fail_at = 4, .cause = cause };
        try std.testing.expectError(cause, acquireMutation(mirror, context, std.testing.io));
        try std.testing.expect(!barrier.reader_gate.tryLock());
        try std.testing.expectEqual(@as(usize, 0), barrier.reader_count);
    }
    state = .{};
    var execution: Session = .{};
    const execution_port = try execution.bind(context, std.testing.io);
    var native_borrow: sources.api_batch_execution_admission.Borrow = .{};
    const native_controls = try native_borrow.bind(&execution_port);
    try std.testing.expect(native_controls.execution_wait == null);
    state.fail_at = state.checks + 1;
    try std.testing.expectError(error.Canceled, native_controls.lifetime.check());
    state = .{ .fail_at = 1, .cause = error.CatalogGenerationChanged };
    // A fallible scoped checkpoint preserves catalog rejection rather than
    // collapsing it through RequestContext's legacy boolean adapter.
    try std.testing.expectError(error.CatalogGenerationChanged, preflight(null, context, std.testing.io));
    try std.testing.expectError(error.DeadlineExceeded, preflight(mirror, .{ .deadline_ns = 0 }, std.testing.io));
    try std.testing.expectError(error.DeadlineExceeded, acquireMutation(mirror, .{ .deadline_ns = 0 }, std.testing.io));
    try std.testing.expectEqual(@as(usize, 0), state.calls);
}

test "controlled owner preflight transition retires capture and lends original CPU before retry" {
    const native = sources.common_request_admission.runnable_scheduler;
    const Case = enum { success, cancel, deadline, shutdown, generation, sleep_failure };
    const Fixture = struct {
        scheduler: *native.Scheduler,
        root: *Barrier.RootSharedLease,
        parent: *?Barrier.SharedLease,
        case: Case,
        cause: ?anyerror = null,
        calls: usize = 0,
        fn check(raw: *const anyopaque) !void {
            const self: *const @This() = @ptrCast(@alignCast(raw));
            if (self.cause) |cause| return cause;
        }
        fn sleep(raw: ?*anyopaque, _: std.Io.Timeout) std.Io.Cancelable!void {
            const self: *@This() = @ptrCast(@alignCast(raw.?));
            self.calls += 1;
            std.debug.assert(!self.root.held and self.root.borrowers.load(.acquire) == 0 and self.parent.* == null);
            std.debug.assert(self.scheduler.stats().runnable_slots == 0 and self.scheduler.stats().retained_bytes == 23);
            var capture = self.root.barrier.tryAcquireExclusive() orelse unreachable;
            defer capture.release();
            var peer: native.Execution = .{};
            peer.start(self.scheduler, .{ .io = std.testing.io, .retained_bytes = 17, .allow_wait = false }, .background) catch unreachable;
            defer peer.finish();
            var health: native.Execution = .{};
            health.start(self.scheduler, .{ .io = std.testing.io, .retained_bytes = 5, .allow_wait = false }, .control) catch unreachable;
            defer health.finish();
            std.debug.assert(self.scheduler.stats().runnable_slots == 2 and self.scheduler.stats().retained_bytes == 45);
            switch (self.case) {
                .success => {},
                .cancel => self.cause = error.Canceled,
                .deadline => self.cause = error.DeadlineExceeded,
                .shutdown => self.scheduler.close(),
                .generation => self.cause = error.CatalogGenerationChanged,
                .sleep_failure => return error.Canceled,
            }
        }
    };
    const Check = struct {
        pub fn checkpoint(_: *@This()) !void {}
        pub fn executionBoundary(_: *@This()) ?sources.common_execution_wait.Boundary {
            return null;
        }
    };
    for ([_]Case{ .success, .cancel, .deadline, .shutdown, .generation, .sleep_failure }) |case| {
        var scheduler = try native.Scheduler.init(.{ .slots = 2 });
        defer scheduler.close();
        var execution: native.Execution = .{};
        try execution.start(&scheduler, .{ .io = std.testing.io, .retained_bytes = 23 }, .write);
        defer execution.finish();
        var read = @import("../../api/runnable_local_read.zig").Read{ .execution = &execution, .allocator = std.testing.allocator };
        const team = try read.team();
        var barrier: Barrier = .{};
        var root = Barrier.RootSharedLease.initRetired(&barrier);
        defer root.release();
        var check: Check = .{};
        var parent: ?Barrier.SharedLease = try root.borrowPhaseControlled(std.testing.io, &check);
        defer if (parent) |*lease| lease.release();
        var fixture: Fixture = .{ .scheduler = &scheduler, .root = &root, .parent = &parent, .case = case };
        var vtable = std.testing.io.vtable.*;
        vtable.sleep = Fixture.sleep;
        const io: std.Io = .{ .userdata = &fixture, .vtable = &vtable };
        var transition = OwnerTransition.init(.{ .cancellation = .{ .ptr = &fixture, .check_fn = Fixture.check } }, team.borrow, io, &root, &parent, null);
        try std.testing.expect(transition.executionIo().userdata == io.userdata);
        const result = transition.wait(.fromMilliseconds(1), std.math.maxInt(u64));
        switch (case) {
            .success => try result,
            .cancel, .sleep_failure => try std.testing.expectError(error.Canceled, result),
            .deadline => try std.testing.expectError(error.DeadlineExceeded, result),
            .shutdown => try std.testing.expectError(error.SchedulerClosed, result),
            .generation => try std.testing.expectError(error.CatalogGenerationChanged, result),
        }
        try std.testing.expectEqual(@as(usize, 1), fixture.calls);
        const restored = case == .success or case == .sleep_failure;
        try std.testing.expectEqual(restored, root.held);
        try std.testing.expectEqual(restored, parent != null);
        try std.testing.expectEqual(@as(usize, if (restored) 1 else 0), scheduler.stats().runnable_slots);
        try std.testing.expectEqual(@as(usize, 1), scheduler.stats().operations);
        try std.testing.expectEqual(@as(u64, 23), scheduler.stats().retained_bytes);
        try std.testing.expectEqual(native.Class.write, try execution.retainedClass());
    }
}

test "controlled owner preflight preserves one-shot scoped rejection without boolean recheck" {
    const Probe = struct {
        calls: usize = 0,
        cause: anyerror,
        reject_at: usize,
        fn check(raw: *const anyopaque) !void {
            const self: *@This() = @ptrCast(@alignCast(@constCast(raw)));
            self.calls += 1;
            if (self.calls == self.reject_at) return self.cause;
        }
    };
    for ([_]anyerror{ error.Canceled, error.DeadlineExceeded, error.SchedulerClosed, error.CatalogGenerationChanged }) |cause| {
        var probe: Probe = .{ .cause = cause, .reject_at = 1 };
        try std.testing.expectError(cause, checkOriginal(.{ .cancellation = .{ .ptr = &probe, .check_fn = Probe.check } }, null));
        try std.testing.expectEqual(@as(usize, 1), probe.calls);
        probe = .{ .cause = cause, .reject_at = 2 };
        const context: Context = .{ .cancellation = .{ .ptr = &probe, .check_fn = Probe.check } };
        try checkOriginal(context, null);
        try std.testing.expectEqual(@as(usize, 1), probe.calls);
        try std.testing.expectError(cause, checkOriginal(context, null));
        try std.testing.expectEqual(@as(usize, 2), probe.calls);
    }
}

test "controlled owner preflight transition refuses retirement with another borrower or expired local wait" {
    const Check = struct {
        pub fn checkpoint(_: *@This()) !void {}
        pub fn executionBoundary(_: *@This()) ?sources.common_execution_wait.Boundary {
            return null;
        }
    };
    var check: Check = .{};
    var barrier: Barrier = .{};
    var root = Barrier.RootSharedLease.initRetired(&barrier);
    defer root.release();
    var parent: ?Barrier.SharedLease = try root.borrowPhaseControlled(std.testing.io, &check);
    defer if (parent) |*lease| lease.release();
    var child = try root.borrowControlled(&check);
    var transition = OwnerTransition.init(.{}, null, std.testing.io, &root, &parent, null);
    try std.testing.expectError(error.InvalidRunnableTransition, transition.boundary().beginWait());
    try std.testing.expect(root.held and parent != null and root.borrowers.load(.acquire) == 2);
    child.release();
    try std.testing.expectError(error.StorageBusy, transition.wait(.zero, 0));
    try std.testing.expect(root.held and parent != null and root.borrowers.load(.acquire) == 1);
}

test "controlled owner preflight compiled batch visibility lends original CPU only after capture retirement" {
    const native = sources.common_request_admission.runnable_scheduler;
    const Case = enum { success, canceled, deadline, closed, generation, io_failure };
    const Fixture = struct {
        scheduler: *native.Scheduler,
        root: *Barrier.RootSharedLease,
        case: Case,
        cause: ?anyerror = null,
        calls: usize = 0,
        fn check(raw: *const anyopaque) !void {
            const self: *const @This() = @ptrCast(@alignCast(raw));
            if (self.cause) |cause| return cause;
        }
        fn sleep(raw: ?*anyopaque, _: std.Io.Timeout) std.Io.Cancelable!void {
            const self: *@This() = @ptrCast(@alignCast(raw.?));
            self.calls += 1;
            std.debug.assert(!self.root.held and self.root.borrowers.load(.acquire) == 0);
            std.debug.assert(self.scheduler.stats().runnable_slots == 0 and self.scheduler.stats().retained_bytes == 23);
            var capture = self.root.barrier.tryAcquireExclusive() orelse unreachable;
            defer capture.release();
            var peer: native.Execution = .{};
            peer.start(self.scheduler, .{ .io = std.testing.io, .retained_bytes = 17, .allow_wait = false }, .background) catch unreachable;
            defer peer.finish();
            var health: native.Execution = .{};
            health.start(self.scheduler, .{ .io = std.testing.io, .retained_bytes = 5, .allow_wait = false }, .control) catch unreachable;
            defer health.finish();
            std.debug.assert(self.scheduler.stats().runnable_slots == 2);
            switch (self.case) {
                .success => {},
                .canceled => self.cause = error.Canceled,
                .deadline => self.cause = error.DeadlineExceeded,
                .closed => self.scheduler.close(),
                .generation => self.cause = error.CatalogGenerationChanged,
                .io_failure => return error.Canceled,
            }
        }
    };
    const Check = struct {
        pub fn checkpoint(_: *@This()) !void {}
        pub fn executionBoundary(_: *@This()) ?sources.common_execution_wait.Boundary {
            return null;
        }
    };
    for (std.enums.values(Case)) |case| {
        var scheduler = try native.Scheduler.init(.{ .slots = 2 });
        defer scheduler.close();
        var execution: native.Execution = .{};
        try execution.start(&scheduler, .{ .io = std.testing.io, .retained_bytes = 23 }, .write);
        defer execution.finish();
        var read = @import("../../api/runnable_local_read.zig").Read{ .execution = &execution, .allocator = std.testing.allocator };
        const team = try read.team();
        var barrier: Barrier = .{};
        var root = Barrier.RootSharedLease.initRetired(&barrier);
        defer root.release();
        var check: Check = .{};
        var parent: ?Barrier.SharedLease = try root.borrowPhaseControlled(std.testing.io, &check);
        defer if (parent) |*lease| lease.release();
        var inner: ?Barrier.SharedLease = try root.borrowControlled(&check);
        defer if (inner) |*lease| lease.release();
        var fixture: Fixture = .{ .scheduler = &scheduler, .root = &root, .case = case };
        var vtable = std.testing.io.vtable.*;
        vtable.sleep = Fixture.sleep;
        const io: std.Io = .{ .userdata = &fixture, .vtable = &vtable };
        var session: Session = .{};
        const original = try session.bindWithRunnable(.{ .cancellation = .{ .ptr = &fixture, .check_fn = Fixture.check } }, io, team.borrow);
        const batch = sources.api_batch_execution_admission;
        const payload: batch.Payload = .{ .original = original, .roots = .{ .root = &root, .retirement = &parent, .committed_visibility = session.committedVisibilityBoundary() } };
        const port = batch.Port.init(&payload);
        var borrow: batch.Borrow = .{};
        var controls = try borrow.bind(&port);
        try std.testing.expect(controls.execution_wait == null);
        controls.execution_wait = borrow.roots.committed_visibility;
        const admission = sources.storage_db_mutation_admission;
        try std.testing.expect(admission.committedVisibilityBoundary(controls, .{ .root = &root, .capture_barrier_present = true, .mutation_reader_active = true }) == null);
        const retirement: admission.MutationRetirement = .{ .current = &inner, .enclosing = borrow.roots.retirement };
        retirement.retire();
        const boundary = admission.committedVisibilityBoundary(controls, .{ .root = &root, .capture_barrier_present = true, .mutation_reader_active = parent != null or inner != null }) orelse return error.TestUnexpectedResult;
        const result = boundary.run(std.Io.sleep, .{ io, std.Io.Duration.fromMilliseconds(1), std.Io.Clock.awake });
        switch (case) {
            .success => try result,
            .canceled, .io_failure => try std.testing.expectError(error.Canceled, result),
            .deadline => try std.testing.expectError(error.DeadlineExceeded, result),
            .closed => try std.testing.expectError(error.SchedulerClosed, result),
            .generation => try std.testing.expectError(error.CatalogGenerationChanged, result),
        }
        try std.testing.expectEqual(@as(usize, 1), fixture.calls);
        try std.testing.expect(!root.held and parent == null and inner == null);
        const resumed = case == .success or case == .io_failure;
        try std.testing.expectEqual(@as(usize, if (resumed) 1 else 0), scheduler.stats().runnable_slots);
        if (resumed) try std.testing.expectEqual(@import("antfly_runtime_abi").runnable_abi.Class.write, try team.borrow.currentClass());
        try std.testing.expectEqual(@as(usize, 23), scheduler.stats().retained_bytes);
        execution.finish();
        try std.testing.expectEqual(@as(usize, 0), scheduler.stats().operations);
        try std.testing.expectEqual(@as(usize, 0), scheduler.stats().retained_bytes);
    }
}

extern fn antfly_batch_visibility_archive_run(*const anyopaque, *anyopaque, *const fn (*anyopaque) callconv(.c) @import("antfly_runtime_abi").error_abi.Status, *u32) callconv(.c) @import("antfly_runtime_abi").error_abi.Status;

test "controlled owner preflight independently linked batch visibility preserves original execution and capture retirement" {
    const native = sources.common_request_admission.runnable_scheduler;
    const Case = enum { success, canceled, deadline, closed, generation, io_failure };
    const Fixture = struct {
        scheduler: *native.Scheduler,
        root: *Barrier.RootSharedLease,
        case: Case,
        cause: ?anyerror = null,
        wait_io: std.Io = undefined,
        calls: usize = 0,
        fn check(raw: *const anyopaque) !void {
            const self: *const @This() = @ptrCast(@alignCast(raw));
            if (self.cause) |cause| return cause;
        }
        fn archiveWait(raw: *anyopaque) callconv(.c) @import("antfly_runtime_abi").error_abi.Status {
            const self: *@This() = @ptrCast(@alignCast(raw));
            std.Io.sleep(self.wait_io, .fromMilliseconds(1), .awake) catch |err| return @import("antfly_runtime_abi").error_abi.statusFromError(err);
            return .ok;
        }
        fn sleep(raw: ?*anyopaque, _: std.Io.Timeout) std.Io.Cancelable!void {
            const self: *@This() = @ptrCast(@alignCast(raw.?));
            self.calls += 1;
            std.debug.assert(!self.root.held and self.root.borrowers.load(.acquire) == 0);
            std.debug.assert(self.scheduler.stats().runnable_slots == 0 and self.scheduler.stats().retained_bytes == 23);
            var capture = self.root.barrier.tryAcquireExclusive() orelse unreachable;
            defer capture.release();
            var peer: native.Execution = .{};
            peer.start(self.scheduler, .{ .io = std.testing.io, .retained_bytes = 17, .allow_wait = false }, .background) catch unreachable;
            defer peer.finish();
            var health: native.Execution = .{};
            health.start(self.scheduler, .{ .io = std.testing.io, .retained_bytes = 5, .allow_wait = false }, .control) catch unreachable;
            defer health.finish();
            std.debug.assert(self.scheduler.stats().runnable_slots == 2);
            switch (self.case) {
                .success => {},
                .canceled => self.cause = error.Canceled,
                .deadline => self.cause = error.DeadlineExceeded,
                .closed => self.scheduler.close(),
                .generation => self.cause = error.CatalogGenerationChanged,
                .io_failure => return error.Canceled,
            }
        }
    };
    const Check = struct {
        pub fn checkpoint(_: *@This()) !void {}
        pub fn executionBoundary(_: *@This()) ?sources.common_execution_wait.Boundary {
            return null;
        }
    };
    for (std.enums.values(Case)) |case| {
        var scheduler = try native.Scheduler.init(.{ .slots = 2 });
        defer scheduler.close();
        var execution: native.Execution = .{};
        try execution.start(&scheduler, .{ .io = std.testing.io, .retained_bytes = 23 }, .write);
        defer execution.finish();
        var read = @import("../../api/runnable_local_read.zig").Read{ .execution = &execution, .allocator = std.testing.allocator };
        const team = try read.team();
        var barrier: Barrier = .{};
        var root = Barrier.RootSharedLease.initRetired(&barrier);
        defer root.release();
        var check: Check = .{};
        var parent: ?Barrier.SharedLease = try root.borrowPhaseControlled(std.testing.io, &check);
        defer if (parent) |*lease| lease.release();
        var fixture: Fixture = .{ .scheduler = &scheduler, .root = &root, .case = case };
        var vtable = std.testing.io.vtable.*;
        vtable.sleep = Fixture.sleep;
        const io: std.Io = .{ .userdata = &fixture, .vtable = &vtable };
        fixture.wait_io = io;
        var session: Session = .{};
        const original = try session.bindWithRunnable(.{ .cancellation = .{ .ptr = &fixture, .check_fn = Fixture.check } }, io, team.borrow);
        const batch = sources.api_batch_execution_admission;
        var payload: batch.Payload = .{ .original = original, .roots = .{ .root = &root, .retirement = &parent, .committed_visibility = session.committedVisibilityBoundary() } };
        const port = batch.Port.init(&payload);
        var foreign_dispatches: u32 = 0;
        const error_abi = @import("antfly_runtime_abi").error_abi;
        payload.wait_contract.version += 1;
        try std.testing.expectEqual(error.UnsupportedVersion, error_abi.errorFromStatus(antfly_batch_visibility_archive_run(&port, &fixture, Fixture.archiveWait, &foreign_dispatches)));
        payload.wait_contract.version -= 1;
        try std.testing.expect(root.held and parent != null and fixture.calls == 0 and foreign_dispatches == 0);
        for ([_]anyerror{ error.Canceled, error.DeadlineExceeded, error.SchedulerClosed, error.CatalogGenerationChanged }) |cause| {
            fixture.cause = cause;
            try std.testing.expectEqual(cause, error_abi.errorFromStatus(antfly_batch_visibility_archive_run(&port, &fixture, Fixture.archiveWait, &foreign_dispatches)));
            try std.testing.expect(root.held and parent != null and fixture.calls == 0 and foreign_dispatches == 0);
        }
        fixture.cause = null;
        const status = antfly_batch_visibility_archive_run(&port, &fixture, Fixture.archiveWait, &foreign_dispatches);
        const result: anyerror!void = if (status.isOk()) {} else error_abi.errorFromStatus(status);
        try std.testing.expectEqual(@as(u32, 2), foreign_dispatches);
        switch (case) {
            .success => try result,
            .canceled, .io_failure => try std.testing.expectError(error.Canceled, result),
            .deadline => try std.testing.expectError(error.DeadlineExceeded, result),
            .closed => try std.testing.expectError(error.SchedulerClosed, result),
            .generation => try std.testing.expectError(error.CatalogGenerationChanged, result),
        }
        try std.testing.expectEqual(@as(usize, 1), fixture.calls);
        try std.testing.expect(!root.held and parent == null);
        const resumed = case == .success or case == .io_failure;
        try std.testing.expectEqual(@as(usize, if (resumed) 1 else 0), scheduler.stats().runnable_slots);
        if (resumed) try std.testing.expectEqual(@import("antfly_runtime_abi").runnable_abi.Class.write, try team.borrow.currentClass());
        try std.testing.expectEqual(@as(usize, 23), scheduler.stats().retained_bytes);
        execution.finish();
        try std.testing.expectEqual(@as(usize, 0), scheduler.stats().operations);
        try std.testing.expectEqual(@as(usize, 0), scheduler.stats().retained_bytes);
    }
}
