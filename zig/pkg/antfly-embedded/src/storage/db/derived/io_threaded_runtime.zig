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

const std = @import("std");
const builtin = @import("builtin");
const Io = std.Io;
const Allocator = std.mem.Allocator;
const replay_source_mod = @import("replay_source.zig");
const derived_worker = @import("derived_worker.zig");
const catch_up_policy = @import("catch_up_policy.zig");
const backlog_tracker_mod = @import("backlog_tracker.zig");
const resource_manager_mod = @import("../../resource_manager.zig");
const index_manager_mod = @import("../catalog/index_manager.zig");
const types = @import("../types.zig");
const runtime_types = @import("runtime_types.zig");
const change_journal_mod = @import("change_journal.zig");
const derived_types = @import("derived_types.zig");
const threaded_io_limits = @import("antfly_runtime_fs").threaded_io_limits;
const platform_time = @import("antfly_platform").time;
const Scheduler = @import("../../../common/maintenance_scheduler.zig").Scheduler;

pub const RuntimeError = runtime_types.RuntimeError;
pub const ApplyFn = runtime_types.ApplyFn;
pub const PersistFn = runtime_types.PersistFn;
pub const TruncateFn = runtime_types.TruncateFn;
pub const BeginCatchUpFn = runtime_types.BeginCatchUpFn;
pub const FinishCatchUpFn = runtime_types.FinishCatchUpFn;
pub const CanAdvanceToTargetFn = runtime_types.CanAdvanceToTargetFn;
pub const AppliedSequenceAdvancedFn = runtime_types.AppliedSequenceAdvancedFn;
pub const CatchUpSessionToken = runtime_types.CatchUpSessionToken;
pub const CatchUpFinishResult = runtime_types.CatchUpFinishResult;

const Worker = struct {
    runtime: *DerivedRuntime,
    name: []u8,
    kind: index_manager_mod.ManagedIndexRef,
    applied_sequence: u64,
    persisted_sequence: u64,
    target_sequence: u64,
    stop: bool = false,
    paused: bool = false,
    dispatch_active: bool = false,
    future: ?Scheduler.Handle = null,
    join_mutex: std.atomic.Mutex = .unlocked,
    join_finisher: bool = false,
    joined: Io.Event = .unset,
    join_changed: Io.Event = .unset,
    join_generation: std.atomic.Value(u64) = .init(0),
    join_waiters: std.atomic.Value(u32) = .init(0),
    finalization: struct {
        started: bool = false,
        cursor: ?replay_source_mod.MatchingCursor = null,
        had_session: bool = false,
        token: CatchUpSessionToken = .{},
        finish_done: bool = false,
        finish_failure: ?anyerror = null,
        durable: bool = false,
        done: bool = false,
    } = .{},
    next_delay_ms: ?u64 = 0,
    idle_since_ns: ?u64 = null,
    catch_up_open: bool = false,
    catch_up_token: CatchUpSessionToken = .{},
    catch_up_close_requested: bool = false,
    catch_up_close_active: bool = false,
    catch_up_close_failed: bool = false,
    replay_cursor: ?replay_source_mod.MatchingCursor = null,
    replay_cursor_open_sequence: u64 = 0,
    catch_up_active: bool = false,
    last_replay_tail_records: u64 = 0,
    recoverable_retry_backoff: catch_up_policy.RecoverableRetryBackoff = .{},
    retry_not_before_ns: u64 = 0,
    last_error_name: ?[]const u8 = null,
};

const PersistSnapshot = struct {
    name: []u8,
    sequence: u64,
};

fn freePersistSnapshots(alloc: Allocator, snapshots: []PersistSnapshot) void {
    for (snapshots) |snapshot| alloc.free(snapshot.name);
    if (snapshots.len > 0) alloc.free(snapshots);
}

fn appendPersistSnapshot(alloc: Allocator, snapshots: *std.ArrayListUnmanaged(PersistSnapshot), worker: *const Worker) !void {
    const name = try alloc.dupe(u8, worker.name);
    errdefer alloc.free(name);
    try snapshots.append(alloc, .{
        .name = name,
        .sequence = worker.applied_sequence,
    });
}

fn forcePersistAppliedSequence(worker: *const Worker) bool {
    return catch_up_policy.forIndex(worker.kind, worker.runtime.backlog.resource_manager).force_persist_applied_sequence;
}

fn canAdvanceToTarget(runtime: *DerivedRuntime, worker: *Worker, from_sequence: u64, target_sequence: u64) !bool {
    if (runtime.can_advance_to_target_fn) |callback| {
        return try callback(runtime.ctx, worker.kind, from_sequence, target_sequence);
    }
    return true;
}

fn indexNameInList(name: []const u8, index_names: []const []const u8) bool {
    for (index_names) |candidate| {
        if (std.mem.eql(u8, name, candidate)) return true;
    }
    return false;
}

pub const DerivedRuntime = if (builtin.os.tag == .freestanding) struct {
    pub fn init(
        alloc: Allocator,
        replay_source: replay_source_mod.Source,
        ctx: *anyopaque,
        apply_fn: ApplyFn,
        persist_fn: PersistFn,
        truncate_fn: TruncateFn,
        begin_catch_up_fn: ?BeginCatchUpFn,
        finish_catch_up_fn: ?FinishCatchUpFn,
        can_advance_to_target_fn: ?CanAdvanceToTargetFn,
        applied_sequence_advanced_fn: ?AppliedSequenceAdvancedFn,
        resource_manager: ?*resource_manager_mod.ResourceManager,
    ) @This() {
        _ = alloc;
        _ = replay_source;
        _ = ctx;
        _ = apply_fn;
        _ = persist_fn;
        _ = truncate_fn;
        _ = begin_catch_up_fn;
        _ = finish_catch_up_fn;
        _ = can_advance_to_target_fn;
        _ = applied_sequence_advanced_fn;
        _ = resource_manager;
        return .{};
    }

    pub fn deinit(self: *@This()) void {
        self.* = undefined;
    }

    pub fn beginShutdown(_: *@This()) void {}
    pub fn requestShutdown(_: *@This()) void {}
    pub fn quiesceWithAdmission(self: *@This(), original: @import("../../../common/read_admission.zig").Port) !void {
        const Ready = struct {
            fn get(_: *anyopaque) anyerror!bool {
                return true;
            }
        };
        try @import("../../../common/cleanup_wait.zig").until(null, original, self, Ready.get, null);
    }

    pub fn finalizeWithAdmission(self: *@This(), original: @import("../../../common/read_admission.zig").Port) !void {
        try self.quiesceWithAdmission(original);
    }

    pub fn hasWorkers(_: *@This()) bool {
        return false;
    }

    pub fn failIfUnhealthy(_: *@This()) !void {}

    pub fn pauseWorker(_: *@This(), _: []const u8) !bool {
        return false;
    }

    pub fn resumeWorker(_: *@This(), _: []const u8) void {}

    pub fn addWorker(self: *@This(), name: []const u8, kind: index_manager_mod.ManagedIndexRef, applied_sequence: u64) !void {
        _ = self;
        _ = name;
        _ = kind;
        _ = applied_sequence;
        return error.UnsupportedPlatform;
    }

    pub fn removeWorker(self: *@This(), name: []const u8) void {
        _ = self;
        _ = name;
    }

    pub fn appliedSequence(self: *@This(), name: []const u8) ?u64 {
        _ = self;
        _ = name;
        return null;
    }

    pub fn snapshotStats(_: *@This()) types.DerivedWorkerStats {
        return .{};
    }

    pub fn notifySequence(self: *@This(), sequence: u64) void {
        _ = self;
        _ = sequence;
    }

    pub fn notifyIndexes(self: *@This(), sequence: u64, index_names: []const []const u8) void {
        _ = self;
        _ = sequence;
        _ = index_names;
    }

    pub fn forceSequence(self: *@This(), sequence: u64) void {
        _ = self;
        _ = sequence;
    }

    pub fn trackBacklogBytes(self: *@This(), sequence: u64, bytes: u64) !void {
        _ = self;
        _ = sequence;
        _ = bytes;
    }

    pub fn backlogThrottleTargetSequence(_: *@This()) ?u64 {
        return null;
    }

    pub fn releaseBacklogThrough(self: *@This(), sequence: u64) void {
        _ = self;
        _ = sequence;
    }

    pub fn waitForAll(self: *@This(), sequence: u64) !void {
        _ = self;
        _ = sequence;
        return error.UnsupportedPlatform;
    }

    pub fn waitForAllWithVisibilityWait(
        self: *@This(),
        sequence: u64,
        wait: runtime_types.VisibilityWait,
    ) !void {
        _ = wait;
        return try self.waitForAll(sequence);
    }

    pub fn waitForIndexes(self: *@This(), sequence: u64, index_names: []const []const u8) !void {
        _ = self;
        _ = sequence;
        _ = index_names;
        return error.UnsupportedPlatform;
    }

    pub fn waitForIndexesWithVisibilityWait(
        self: *@This(),
        sequence: u64,
        index_names: []const []const u8,
        wait: runtime_types.VisibilityWait,
    ) !void {
        _ = wait;
        return try self.waitForIndexes(sequence, index_names);
    }
} else struct {
    const IoOwner = enum {
        owned,
        borrowed,
    };

    alloc: Allocator,
    threaded: *Io.Threaded,
    threaded_owner: IoOwner,
    scheduler: ?*Scheduler = null,
    owns_scheduler: bool = false,
    replay_source: replay_source_mod.Source,
    ctx: *anyopaque,
    apply_fn: ApplyFn,
    persist_fn: PersistFn,
    truncate_fn: TruncateFn,
    begin_catch_up_fn: ?BeginCatchUpFn,
    finish_catch_up_fn: ?FinishCatchUpFn,
    finish_with_admission: ?runtime_types.FinishWithAdmissionFn = null,
    can_advance_to_target_fn: ?CanAdvanceToTargetFn,
    applied_sequence_advanced_fn: ?AppliedSequenceAdvancedFn,
    mutex: Io.Mutex = .init,
    cond: Io.Condition = .init,
    workers: std.ArrayListUnmanaged(*Worker) = .empty,
    shutdown: bool = false,
    closing: std.atomic.Value(bool) = .init(false),
    finalizer_active: std.atomic.Value(bool) = .init(false),
    active_borrows: std.atomic.Value(u32) = .init(0),
    last_error_name: ?[]const u8 = null,
    // Claims exclude duplicate cleanup; completion alone releases backlog credit.
    last_claimed_truncate_sequence: u64 = 0,
    last_truncated_sequence: u64 = 0,
    // Shared cleanup backoff survives worker yields and foreground wakes.
    truncate_retry_not_before_ns: u64 = 0,
    truncate_retry_backoff: catch_up_policy.RecoverableRetryBackoff = .{},
    force_catch_up_sequence: u64 = 0,
    last_notified_sequence: u64 = 0,
    truncates_in_flight: usize = 0,
    backlog: backlog_tracker_mod.Tracker,
    recoverable_retry_counters: catch_up_policy.RecoverableRetryCounters = .{},

    pub fn init(
        alloc: Allocator,
        replay_source: replay_source_mod.Source,
        ctx: *anyopaque,
        apply_fn: ApplyFn,
        persist_fn: PersistFn,
        truncate_fn: TruncateFn,
        begin_catch_up_fn: ?BeginCatchUpFn,
        finish_catch_up_fn: ?FinishCatchUpFn,
        can_advance_to_target_fn: ?CanAdvanceToTargetFn,
        applied_sequence_advanced_fn: ?AppliedSequenceAdvancedFn,
        resource_manager: ?*resource_manager_mod.ResourceManager,
    ) !DerivedRuntime {
        const threaded = try alloc.create(Io.Threaded);
        errdefer alloc.destroy(threaded);
        // Standalone users own a bounded I/O lane and lazily create a shared
        // scheduler. Database-backed runtimes borrow both from BackendRuntime.
        threaded.* = threaded_io_limits.initService(alloc);
        return initWithIo(
            alloc,
            threaded,
            .owned,
            replay_source,
            ctx,
            apply_fn,
            persist_fn,
            truncate_fn,
            begin_catch_up_fn,
            finish_catch_up_fn,
            can_advance_to_target_fn,
            applied_sequence_advanced_fn,
            resource_manager,
        );
    }

    pub fn initBorrowed(
        alloc: Allocator,
        threaded: *Io.Threaded,
        replay_source: replay_source_mod.Source,
        ctx: *anyopaque,
        apply_fn: ApplyFn,
        persist_fn: PersistFn,
        truncate_fn: TruncateFn,
        begin_catch_up_fn: ?BeginCatchUpFn,
        finish_catch_up_fn: ?FinishCatchUpFn,
        can_advance_to_target_fn: ?CanAdvanceToTargetFn,
        applied_sequence_advanced_fn: ?AppliedSequenceAdvancedFn,
        resource_manager: ?*resource_manager_mod.ResourceManager,
    ) DerivedRuntime {
        return initWithIo(
            alloc,
            threaded,
            .borrowed,
            replay_source,
            ctx,
            apply_fn,
            persist_fn,
            truncate_fn,
            begin_catch_up_fn,
            finish_catch_up_fn,
            can_advance_to_target_fn,
            applied_sequence_advanced_fn,
            resource_manager,
        );
    }

    fn initWithIo(
        alloc: Allocator,
        threaded: *Io.Threaded,
        threaded_owner: IoOwner,
        replay_source: replay_source_mod.Source,
        ctx: *anyopaque,
        apply_fn: ApplyFn,
        persist_fn: PersistFn,
        truncate_fn: TruncateFn,
        begin_catch_up_fn: ?BeginCatchUpFn,
        finish_catch_up_fn: ?FinishCatchUpFn,
        can_advance_to_target_fn: ?CanAdvanceToTargetFn,
        applied_sequence_advanced_fn: ?AppliedSequenceAdvancedFn,
        resource_manager: ?*resource_manager_mod.ResourceManager,
    ) DerivedRuntime {
        return .{
            .alloc = alloc,
            .threaded = threaded,
            .threaded_owner = threaded_owner,
            .replay_source = replay_source,
            .ctx = ctx,
            .apply_fn = apply_fn,
            .persist_fn = persist_fn,
            .truncate_fn = truncate_fn,
            .begin_catch_up_fn = begin_catch_up_fn,
            .finish_catch_up_fn = finish_catch_up_fn,
            .can_advance_to_target_fn = can_advance_to_target_fn,
            .applied_sequence_advanced_fn = applied_sequence_advanced_fn,
            .backlog = backlog_tracker_mod.Tracker.init(resource_manager),
        };
    }

    fn ioContext(self: *DerivedRuntime) Io {
        return self.threaded.io();
    }

    fn signalWorkers(self: *DerivedRuntime, io: Io) void {
        self.cond.broadcast(io);
        if (self.scheduler) |scheduler| for (self.workers.items) |worker| {
            if (worker.last_error_name == null) scheduler.wake(worker);
        };
    }

    pub fn deinit(self: *DerivedRuntime) void {
        const io = self.ioContext();
        while (self.finalizer_active.cmpxchgWeak(false, true, .acquire, .monotonic) != null)
            io.sleep(.fromMilliseconds(1), .awake) catch {};
        self.beginShutdown();
        while (self.active_borrows.load(.acquire) != 0) io.sleep(.fromMilliseconds(1), .awake) catch {};
        self.mutex.lockUncancelable(io);
        self.mutex.unlock(io);

        for (self.workers.items) |worker| {
            if (worker.future) |*future| _ = future.await(io);
            self.finalizeWorker(worker, null) catch |err| {
                std.log.warn("derived worker finalization deferred worker={s}: {s}", .{ worker.name, @errorName(err) });
            };
            // A failed controlled attempt may still own an unclosed cursor.
            if (worker.finalization.cursor) |*cursor| cursor.deinit(self.alloc);
            if (worker.replay_cursor) |*cursor| cursor.deinit(self.alloc);
            self.alloc.free(worker.name);
            self.alloc.destroy(worker);
        }
        self.workers.deinit(self.alloc);
        self.backlog.deinit(self.alloc);
        if (self.owns_scheduler) if (self.scheduler) |scheduler| scheduler.destroy();
        if (self.threaded_owner == .owned) {
            self.threaded.deinit();
            self.alloc.destroy(self.threaded);
        }
        self.* = undefined;
    }

    pub fn requestShutdown(self: *DerivedRuntime) void {
        self.closing.store(true, .release);
    }

    fn admitBorrow(self: *DerivedRuntime) !void {
        if (self.closing.load(.acquire)) return error.WorkerStopping;
        const io = self.ioContext();
        self.mutex.lockUncancelable(io);
        defer self.mutex.unlock(io);
        if (self.closing.load(.acquire)) return error.WorkerStopping;
        if (self.active_borrows.load(.monotonic) == std.math.maxInt(u32)) return error.ResourceLimitExceeded;
        _ = self.active_borrows.fetchAdd(1, .acquire);
    }

    fn noBorrows(raw: *anyopaque) anyerror!bool {
        const self: *DerivedRuntime = @ptrCast(@alignCast(raw));
        return self.active_borrows.load(.acquire) == 0;
    }

    /// The coordinator closes producer dependencies before this join. Retain
    /// worker/session arrays and idle finalization for physical teardown.
    pub fn quiesceWithAdmission(self: *DerivedRuntime, original: @import("../../../common/read_admission.zig").Port) !void {
        self.requestShutdown();
        const io = self.ioContext();
        const cleanup = @import("../../../common/cleanup_wait.zig");
        try cleanup.lockIo(&self.mutex, io, original);
        self.shutdown = true;
        self.cond.broadcast(io);
        self.mutex.unlock(io);
        try cleanup.until(io, original, self, noBorrows, null);
        // Every admitted add/remove/pause or foreground persistence callback
        // has exited. New topology operations reject before touching arrays.
        for (self.workers.items) |worker| {
            const join: @import("../maintenance_stop.zig").Borrowed = .{
                .mutex = &worker.join_mutex,
                .registration = &worker.future,
                .stopping = &self.closing,
                .finisher = &worker.join_finisher,
                .joined = &worker.joined,
                .changed = &worker.join_changed,
                .generation = &worker.join_generation,
                .waiters = &worker.join_waiters,
            };
            try join.stopWithAdmission(io, original);
        }
        try cleanup.lockIo(&self.mutex, io, original);
        self.mutex.unlock(io);
    }

    fn finalizerReady(raw: *anyopaque) anyerror!bool {
        const self: *DerivedRuntime = @ptrCast(@alignCast(raw));
        return !self.finalizer_active.load(.acquire);
    }

    fn finalizationCheckpoint(original: ?@import("../../../common/read_admission.zig").Port) !void {
        if (original) |control| try control.checkpoint();
    }

    fn lockFinalizationMetadata(self: *DerivedRuntime, original: ?@import("../../../common/read_admission.zig").Port) !void {
        const io = self.ioContext();
        if (original) |control| {
            try @import("../../../common/cleanup_wait.zig").lockIo(&self.mutex, io, control);
        } else self.mutex.lockUncancelable(io);
    }

    /// No original Port escapes this synchronous call. Callback work runs
    /// with the caller's execution, while only contended guards lend it.
    /// Callback I/O/cost contracts remain a separate qualification obligation.
    pub fn finalizeWithAdmission(self: *DerivedRuntime, original: @import("../../../common/read_admission.zig").Port) !void {
        self.requestShutdown();
        const io = self.ioContext();
        const cleanup = @import("../../../common/cleanup_wait.zig");
        while (true) {
            try cleanup.until(io, original, self, finalizerReady, null);
            if (self.finalizer_active.cmpxchgWeak(false, true, .acquire, .monotonic) == null) break;
        }
        defer self.finalizer_active.store(false, .release);
        try self.quiesceWithAdmission(original);
        for (self.workers.items) |worker| try self.finalizeWorker(worker, original);
    }

    fn finalizeWorker(self: *DerivedRuntime, worker: *Worker, original: ?@import("../../../common/read_admission.zig").Port) !void {
        try finalizationCheckpoint(original);
        const final = &worker.finalization;
        if (final.done) return;
        const io = self.ioContext();
        if (!final.started) {
            try self.lockFinalizationMetadata(original);
            if (worker.catch_up_close_failed) {
                self.mutex.unlock(io);
                return RuntimeError.AsyncWorkerFailed;
            }
            // Transfer into runtime-owned retry state before any subsequent
            // checkpoint can reject. Never leave a cursor/token on the stack.
            final.cursor = worker.replay_cursor;
            final.had_session = worker.catch_up_open;
            final.token = worker.catch_up_token;
            final.started = true;
            worker.replay_cursor = null;
            worker.replay_cursor_open_sequence = 0;
            worker.catch_up_open = false;
            worker.catch_up_token = .{};
            worker.catch_up_close_requested = false;
            worker.catch_up_close_active = final.had_session;
            worker.last_replay_tail_records = 0;
            self.mutex.unlock(io);
        }
        if (final.cursor) |*cursor| {
            try finalizationCheckpoint(original);
            cursor.deinit(self.alloc);
            final.cursor = null;
        }
        if (!final.finish_done) {
            try finalizationCheckpoint(original);
            if (final.had_session) {
                if (self.finish_catch_up_fn) |finish| {
                    var attempt: runtime_types.FinishAttempt = .{};
                    const call = if (original != null and self.finish_with_admission != null)
                        self.finish_with_admission.?(self.ctx, worker.kind, final.token, worker.applied_sequence, original.?, &attempt)
                    else legacy: {
                        attempt.consumed = true;
                        break :legacy finish(self.ctx, worker.kind, final.token, worker.applied_sequence, true);
                    };
                    const result = call catch |cause| failed: {
                        if (!attempt.consumed) return cause;
                        final.finish_failure = cause;
                        break :failed CatchUpFinishResult{};
                    };
                    if (!attempt.consumed) final.finish_failure = RuntimeError.AsyncWorkerFailed;
                    // Record authoritative publication before any fallible
                    // reentry/guard step. Retrying cannot repeat the callback.
                    final.durable = result.applied_sequence_persisted;
                }
            }
            final.finish_done = true;
        }
        try self.lockFinalizationMetadata(original);
        worker.catch_up_close_active = false;
        worker.catch_up_close_failed = final.finish_failure != null;
        self.cond.broadcast(io);
        self.mutex.unlock(io);
        if (final.finish_failure) |cause| return cause;
        if (worker.applied_sequence > worker.persisted_sequence and !final.durable) {
            try finalizationCheckpoint(original);
            if (!try self.persist_fn(self.ctx, worker.name, worker.applied_sequence, true))
                return RuntimeError.AsyncWorkerFailed;
            final.durable = true;
        }
        try self.lockFinalizationMetadata(original);
        if (final.durable) worker.persisted_sequence = @max(worker.persisted_sequence, worker.applied_sequence);
        final.done = true;
        self.mutex.unlock(io);
    }

    pub fn beginShutdown(self: *DerivedRuntime) void {
        self.requestShutdown();
        const io = self.ioContext();
        self.mutex.lockUncancelable(io);
        self.shutdown = true;
        for (self.workers.items) |worker| worker.stop = true;
        self.signalWorkers(io);
        self.mutex.unlock(io);
    }

    pub fn hasWorkers(self: *DerivedRuntime) bool {
        const io = self.ioContext();
        self.mutex.lockUncancelable(io);
        defer self.mutex.unlock(io);
        return self.workers.items.len > 0;
    }

    pub fn failIfUnhealthy(self: *DerivedRuntime) !void {
        const io = self.ioContext();
        self.mutex.lockUncancelable(io);
        defer self.mutex.unlock(io);
        if (self.last_error_name != null) return RuntimeError.AsyncWorkerFailed;
        for (self.workers.items) |worker| if (worker.last_error_name != null) return RuntimeError.AsyncWorkerFailed;
    }

    pub fn addWorker(self: *DerivedRuntime, name: []const u8, kind: index_manager_mod.ManagedIndexRef, applied_sequence: u64) !void {
        try self.admitBorrow();
        defer _ = self.active_borrows.fetchSub(1, .release);
        const io = self.ioContext();

        const worker = try self.alloc.create(Worker);
        errdefer self.alloc.destroy(worker);
        worker.* = .{
            .runtime = self,
            .name = try self.alloc.dupe(u8, name),
            .kind = .{
                .name = undefined,
                .kind = kind.kind,
                .estimated_dense_vector_bytes = kind.estimated_dense_vector_bytes,
                .dense_replay_working_set_factor = kind.dense_replay_working_set_factor,
            },
            .applied_sequence = applied_sequence,
            .persisted_sequence = applied_sequence,
            .target_sequence = applied_sequence,
        };
        errdefer self.alloc.free(worker.name);
        worker.kind.name = worker.name;

        self.mutex.lockUncancelable(io);
        worker.target_sequence = @max(worker.target_sequence, self.last_notified_sequence);
        self.workers.append(self.alloc, worker) catch |err| {
            self.mutex.unlock(io);
            return err;
        };
        self.mutex.unlock(io);
        errdefer {
            self.mutex.lockUncancelable(io);
            const idx = for (self.workers.items, 0..) |candidate, i| {
                if (candidate == worker) break i;
            } else unreachable;
            _ = self.workers.orderedRemove(idx);
            self.mutex.unlock(io);
        }

        self.mutex.lockUncancelable(io);
        defer self.mutex.unlock(io);
        if (self.scheduler == null) {
            self.scheduler = try Scheduler.create(self.alloc, io, 8);
            self.owns_scheduler = true;
        }
        worker.future = try self.scheduler.?.registerClass(.derived, worker, workerStep);
    }

    /// Keep the worker in the retention set while its scheduler callback drains.
    /// Dispatch admission and pause share the runtime mutex; after the drain
    /// only this structural owner may touch the worker's retained session.
    pub fn pauseWorker(self: *DerivedRuntime, name: []const u8) !bool {
        try self.admitBorrow();
        defer _ = self.active_borrows.fetchSub(1, .release);
        const io = self.ioContext();
        self.mutex.lockUncancelable(io);
        const worker = for (self.workers.items) |candidate| {
            if (std.mem.eql(u8, candidate.name, name)) break candidate;
        } else {
            self.mutex.unlock(io);
            return false;
        };
        if (worker.paused) {
            self.mutex.unlock(io);
            return error.DerivedWorkerAlreadyPaused;
        }
        worker.paused = true;
        while (worker.dispatch_active) self.cond.waitUncancelable(io, &self.mutex);
        const close_failed = worker.catch_up_close_failed;
        self.mutex.unlock(io);
        errdefer self.resumeWorker(name);
        if (close_failed) return RuntimeError.AsyncWorkerFailed;
        // Structural callers drain cleanup without occupying a shared scheduler
        // slot. Keep this paused worker in the retention set until completion.
        while (true) {
            self.mutex.lockUncancelable(io);
            const stopping = self.closing.load(.acquire) or self.shutdown or worker.stop;
            const pending = self.computeMinPersistedLocked() > self.last_truncated_sequence;
            const delay = self.truncate_retry_not_before_ns -| platform_time.monotonicNs();
            self.mutex.unlock(io);
            if (stopping) return error.WorkerStopping;
            if (!pending) break;
            if (delay > 0) {
                io.sleep(Io.Duration.fromNanoseconds(@intCast(delay)), .awake) catch {};
            } else {
                attemptPendingTruncate(self, io) catch |err| {
                    self.recordError(io, worker, "pause_truncate", err);
                    return err;
                };
                io.sleep(.fromMilliseconds(1), .awake) catch {};
            }
        }
        _ = closeWorkerCatchUpState(self, worker, worker.applied_sequence, true) catch |err| {
            self.recordError(io, worker, "pause_close_session", err);
            return err;
        };
        return true;
    }

    pub fn resumeWorker(self: *DerivedRuntime, name: []const u8) void {
        self.admitBorrow() catch return;
        defer _ = self.active_borrows.fetchSub(1, .release);
        const io = self.ioContext();
        self.mutex.lockUncancelable(io);
        defer self.mutex.unlock(io);
        for (self.workers.items) |worker| {
            if (!std.mem.eql(u8, worker.name, name)) continue;
            worker.paused = false;
            // Wake the original scheduler registration; no restart allocation.
            if (self.scheduler) |scheduler| scheduler.wake(worker);
            self.cond.broadcast(io);
            return;
        }
    }

    pub fn removeWorker(self: *DerivedRuntime, name: []const u8) void {
        self.admitBorrow() catch return;
        defer _ = self.active_borrows.fetchSub(1, .release);
        const io = self.ioContext();

        self.mutex.lockUncancelable(io);
        const idx = for (self.workers.items, 0..) |worker, i| {
            if (std.mem.eql(u8, worker.name, name)) break i;
        } else {
            self.mutex.unlock(io);
            return;
        };
        const worker = self.workers.orderedRemove(idx);
        worker.stop = true;
        self.signalWorkers(io);
        self.mutex.unlock(io);

        if (worker.future) |*future| _ = future.await(io);
        _ = closeWorkerCatchUpState(self, worker, worker.applied_sequence, true) catch |err| {
            std.log.warn("derived worker final catch-up close failed worker={s}: {s}", .{ worker.name, @errorName(err) });
        };
        self.alloc.free(worker.name);
        self.alloc.destroy(worker);
    }

    pub fn appliedSequence(self: *DerivedRuntime, name: []const u8) ?u64 {
        const io = self.ioContext();
        self.mutex.lockUncancelable(io);
        defer self.mutex.unlock(io);
        for (self.workers.items) |worker| {
            if (std.mem.eql(u8, worker.name, name)) return worker.applied_sequence;
        }
        return null;
    }

    pub fn snapshotStats(self: *DerivedRuntime) types.DerivedWorkerStats {
        const io = self.ioContext();
        self.mutex.lockUncancelable(io);
        defer self.mutex.unlock(io);
        var stats = types.DerivedWorkerStats{
            .workers = @intCast(self.workers.items.len),
        };
        for (self.workers.items) |worker| {
            if (worker.last_error_name != null) stats.failed_workers += 1;
            const lag = worker.target_sequence -| worker.applied_sequence;
            if (lag > 0) stats.workers_with_replay_debt += 1;
            stats.max_replay_lag_sequences = @max(stats.max_replay_lag_sequences, lag);
        }
        const retries = self.recoverable_retry_counters.snapshot();
        stats.recoverable_retries = retries.total;
        stats.writer_locked_retries = retries.writer_locked;
        stats.resource_budget_retries = retries.resource_budget;
        stats.replay_document_not_visible_retries = retries.replay_document_not_visible;
        stats.artifact_repair_required_retries = retries.artifact_repair_required;
        stats.not_found_retries = retries.not_found;
        return stats;
    }

    pub fn notifySequence(self: *DerivedRuntime, sequence: u64) void {
        const io = self.ioContext();
        self.mutex.lockUncancelable(io);
        defer self.mutex.unlock(io);

        self.last_notified_sequence = @max(self.last_notified_sequence, sequence);
        var changed = false;
        for (self.workers.items) |worker| {
            const next = @max(worker.target_sequence, sequence);
            changed = changed or next != worker.target_sequence;
            worker.target_sequence = next;
        }
        if (changed) self.signalWorkers(io);
    }

    pub fn notifyIndexes(self: *DerivedRuntime, sequence: u64, index_names: []const []const u8) void {
        if (index_names.len == 0) return;
        const io = self.ioContext();
        self.mutex.lockUncancelable(io);
        defer self.mutex.unlock(io);

        var changed = false;
        for (self.workers.items) |worker| {
            if (!indexNameInList(worker.name, index_names)) continue;
            const next = @max(worker.target_sequence, sequence);
            changed = changed or next != worker.target_sequence;
            worker.target_sequence = next;
        }
        if (changed) self.signalWorkers(io);
    }

    pub fn notifyExceptKind(self: *DerivedRuntime, sequence: u64, excluded_kind: types.IndexKind) void {
        const io = self.ioContext();
        self.mutex.lockUncancelable(io);
        defer self.mutex.unlock(io);

        self.last_notified_sequence = @max(self.last_notified_sequence, sequence);
        var changed = false;
        for (self.workers.items) |worker| {
            if (worker.kind.kind == excluded_kind) continue;
            const next = @max(worker.target_sequence, sequence);
            changed = changed or next != worker.target_sequence;
            worker.target_sequence = next;
        }
        if (changed) self.signalWorkers(io);
    }

    pub fn forceSequence(self: *DerivedRuntime, sequence: u64) void {
        const io = self.ioContext();
        self.mutex.lockUncancelable(io);
        defer self.mutex.unlock(io);

        self.last_notified_sequence = @max(self.last_notified_sequence, sequence);
        self.force_catch_up_sequence = @max(self.force_catch_up_sequence, sequence);
        var changed = false;
        for (self.workers.items) |worker| {
            const next = @max(worker.target_sequence, sequence);
            changed = changed or next != worker.target_sequence;
            worker.target_sequence = next;
        }
        if (changed) self.signalWorkers(io);
    }

    pub fn trackBacklogBytes(self: *DerivedRuntime, sequence: u64, bytes: u64) !void {
        const io = self.ioContext();
        self.mutex.lockUncancelable(io);
        defer self.mutex.unlock(io);
        return try self.backlog.track(self.alloc, sequence, bytes);
    }

    pub fn admitBacklogBytes(self: *DerivedRuntime, bytes: u64) !backlog_tracker_mod.Tracker.Admission {
        const io = self.ioContext();
        self.mutex.lockUncancelable(io);
        defer self.mutex.unlock(io);
        return try self.backlog.admit(self.alloc, bytes);
    }

    pub fn commitBacklogAdmission(
        self: *DerivedRuntime,
        sequence: u64,
        admission: *backlog_tracker_mod.Tracker.Admission,
    ) void {
        const io = self.ioContext();
        self.mutex.lockUncancelable(io);
        defer self.mutex.unlock(io);
        self.backlog.commitAdmission(sequence, admission);
    }

    pub fn backlogThrottleTargetSequence(self: *DerivedRuntime) ?u64 {
        const io = self.ioContext();
        self.mutex.lockUncancelable(io);
        defer self.mutex.unlock(io);
        return self.backlog.throttleTargetSequence();
    }

    pub fn releaseBacklogThrough(self: *DerivedRuntime, sequence: u64) void {
        const io = self.ioContext();
        self.mutex.lockUncancelable(io);
        defer self.mutex.unlock(io);
        self.backlog.releaseThrough(sequence);
    }

    pub fn waitForAll(self: *DerivedRuntime, sequence: u64) !void {
        return try self.waitForAllWithVisibilityWait(sequence, .{});
    }

    /// Caller owns the worker mutex on entry and on every return. Original
    /// authority callbacks may inspect runtime state and must run unlocked.
    fn checkVisibilityWaitUnlocked(self: *DerivedRuntime, io: Io, wait: runtime_types.VisibilityWait) !void {
        self.mutex.unlock(io);
        const result = wait.check();
        self.mutex.lockUncancelable(io);
        try result;
    }

    fn waitVisibilityProgressUnlocked(self: *DerivedRuntime, io: Io, wait: runtime_types.VisibilityWait, duration: Io.Duration) !void {
        self.mutex.unlock(io);
        const result = wait.waitForProgress(io, duration);
        self.mutex.lockUncancelable(io);
        try result;
    }

    pub fn waitForAllWithVisibilityWait(
        self: *DerivedRuntime,
        sequence: u64,
        wait: runtime_types.VisibilityWait,
    ) !void {
        try wait.check();
        try self.admitBorrow();
        defer _ = self.active_borrows.fetchSub(1, .release);
        const io = self.ioContext();
        self.mutex.lockUncancelable(io);
        defer self.mutex.unlock(io);
        self.force_catch_up_sequence = @max(self.force_catch_up_sequence, sequence);
        for (self.workers.items) |worker| {
            worker.target_sequence = @max(worker.target_sequence, sequence);
        }
        self.signalWorkers(io);

        while (true) {
            if (self.closing.load(.acquire)) {
                try self.checkVisibilityWaitUnlocked(io, wait);
                return error.WorkerStopping;
            }
            if (self.last_error_name != null) return RuntimeError.AsyncWorkerFailed;

            var all_applied = true;
            for (self.workers.items) |worker| {
                if (worker.last_error_name != null) return RuntimeError.AsyncWorkerFailed;
                if (worker.catch_up_open) {
                    worker.catch_up_close_requested = true;
                }
                if (worker.paused or worker.applied_sequence < sequence or worker.catch_up_active or worker.catch_up_open or worker.catch_up_close_active or worker.catch_up_close_failed) {
                    all_applied = false;
                }
            }
            if (all_applied and self.truncates_in_flight == 0) {
                var all_persisted = true;
                var snapshots = std.ArrayListUnmanaged(PersistSnapshot).empty;
                defer snapshots.deinit(self.alloc);
                errdefer {
                    for (snapshots.items) |snapshot| self.alloc.free(snapshot.name);
                }
                for (self.workers.items) |worker| {
                    if (worker.applied_sequence == 0) continue;
                    try appendPersistSnapshot(self.alloc, &snapshots, worker);
                }
                const persist_snapshots = try snapshots.toOwnedSlice(self.alloc);
                snapshots = .empty;
                defer freePersistSnapshots(self.alloc, persist_snapshots);
                self.mutex.unlock(io);
                for (persist_snapshots) |snapshot| {
                    const persisted = self.persist_fn(self.ctx, snapshot.name, snapshot.sequence, true) catch |err| {
                        self.mutex.lockUncancelable(io);
                        return err;
                    };
                    self.mutex.lockUncancelable(io);
                    if (persisted) {
                        for (self.workers.items) |worker| {
                            if (std.mem.eql(u8, worker.name, snapshot.name)) {
                                worker.persisted_sequence = @max(worker.persisted_sequence, snapshot.sequence);
                                break;
                            }
                        }
                    } else {
                        all_persisted = false;
                    }
                    self.mutex.unlock(io);
                }
                self.mutex.lockUncancelable(io);
                if (!all_persisted) {
                    try self.waitVisibilityProgressUnlocked(io, wait, Io.Duration.zero);
                    continue;
                }
                if (self.truncates_in_flight != 0) {
                    try self.waitVisibilityProgressUnlocked(io, wait, Io.Duration.fromMilliseconds(1));
                    continue;
                }
                const truncate_sequence = truncate: {
                    const min_persisted = self.computeMinPersistedLocked();
                    if (min_persisted > self.last_claimed_truncate_sequence) {
                        self.last_claimed_truncate_sequence = min_persisted;
                        break :truncate min_persisted;
                    }
                    break :truncate 0;
                };
                if (truncate_sequence > 0) {
                    self.truncates_in_flight += 1;
                    self.mutex.unlock(io);
                    truncateWithVisibilityWait(self, truncate_sequence, wait, io) catch |err| {
                        self.mutex.lockUncancelable(io);
                        self.truncates_in_flight -= 1;
                        self.last_claimed_truncate_sequence = self.last_truncated_sequence;
                        self.signalWorkers(io);
                        return err;
                    };
                    self.mutex.lockUncancelable(io);
                    self.last_truncated_sequence = @max(self.last_truncated_sequence, truncate_sequence);
                    self.backlog.releaseThrough(truncate_sequence);
                    self.truncates_in_flight -= 1;
                    self.truncate_retry_not_before_ns = 0;
                    self.truncate_retry_backoff.reset();
                    self.cond.broadcast(io);
                }
                try self.checkVisibilityWaitUnlocked(io, wait);
                return;
            }
            try self.checkVisibilityWaitUnlocked(io, wait);
            try self.waitVisibilityProgressUnlocked(io, wait, Io.Duration.fromNanoseconds(std.time.ns_per_ms));
        }
    }

    pub fn waitForIndexes(self: *DerivedRuntime, sequence: u64, index_names: []const []const u8) !void {
        return try self.waitForIndexesWithVisibilityWait(sequence, index_names, .{});
    }

    pub fn waitForIndexesWithVisibilityWait(
        self: *DerivedRuntime,
        sequence: u64,
        index_names: []const []const u8,
        wait: runtime_types.VisibilityWait,
    ) !void {
        if (index_names.len == 0) return;
        try wait.check();
        try self.admitBorrow();
        defer _ = self.active_borrows.fetchSub(1, .release);
        const io = self.ioContext();
        self.mutex.lockUncancelable(io);
        defer self.mutex.unlock(io);
        var changed = false;
        for (self.workers.items) |worker| {
            if (!indexNameInList(worker.name, index_names)) continue;
            const next = @max(worker.target_sequence, sequence);
            changed = changed or next != worker.target_sequence;
            worker.target_sequence = next;
        }
        if (changed) self.signalWorkers(io);

        while (true) {
            if (self.closing.load(.acquire)) {
                try self.checkVisibilityWaitUnlocked(io, wait);
                return error.WorkerStopping;
            }
            if (self.last_error_name != null) return RuntimeError.AsyncWorkerFailed;

            var all_applied = true;
            for (self.workers.items) |worker| {
                if (!indexNameInList(worker.name, index_names)) continue;
                if (worker.last_error_name != null) return RuntimeError.AsyncWorkerFailed;
                if (worker.catch_up_open) {
                    worker.catch_up_close_requested = true;
                }
                if (worker.paused or worker.applied_sequence < sequence or worker.catch_up_active or worker.catch_up_open or worker.catch_up_close_active or worker.catch_up_close_failed) {
                    all_applied = false;
                }
            }
            if (all_applied and self.truncates_in_flight == 0) {
                var all_persisted = true;
                var snapshots = std.ArrayListUnmanaged(PersistSnapshot).empty;
                defer snapshots.deinit(self.alloc);
                errdefer {
                    for (snapshots.items) |snapshot| self.alloc.free(snapshot.name);
                }
                for (self.workers.items) |worker| {
                    if (!indexNameInList(worker.name, index_names)) continue;
                    if (worker.applied_sequence == 0) continue;
                    try appendPersistSnapshot(self.alloc, &snapshots, worker);
                }
                const persist_snapshots = try snapshots.toOwnedSlice(self.alloc);
                snapshots = .empty;
                defer freePersistSnapshots(self.alloc, persist_snapshots);
                self.mutex.unlock(io);
                for (persist_snapshots) |snapshot| {
                    const persisted = self.persist_fn(self.ctx, snapshot.name, snapshot.sequence, true) catch |err| {
                        self.mutex.lockUncancelable(io);
                        return err;
                    };
                    self.mutex.lockUncancelable(io);
                    if (persisted) {
                        for (self.workers.items) |worker| {
                            if (std.mem.eql(u8, worker.name, snapshot.name)) {
                                worker.persisted_sequence = @max(worker.persisted_sequence, snapshot.sequence);
                                break;
                            }
                        }
                    } else {
                        all_persisted = false;
                    }
                    self.mutex.unlock(io);
                }
                self.mutex.lockUncancelable(io);
                if (!all_persisted) {
                    try self.waitVisibilityProgressUnlocked(io, wait, Io.Duration.zero);
                    continue;
                }
                if (self.truncates_in_flight != 0) {
                    try self.waitVisibilityProgressUnlocked(io, wait, Io.Duration.fromMilliseconds(1));
                    continue;
                }
                const truncate_sequence = truncate: {
                    const min_persisted = self.computeMinPersistedLocked();
                    if (min_persisted > self.last_claimed_truncate_sequence) {
                        self.last_claimed_truncate_sequence = min_persisted;
                        break :truncate min_persisted;
                    }
                    break :truncate 0;
                };
                if (truncate_sequence > 0) {
                    self.truncates_in_flight += 1;
                    self.mutex.unlock(io);
                    truncateWithVisibilityWait(self, truncate_sequence, wait, io) catch |err| {
                        self.mutex.lockUncancelable(io);
                        self.truncates_in_flight -= 1;
                        self.last_claimed_truncate_sequence = self.last_truncated_sequence;
                        self.signalWorkers(io);
                        return err;
                    };
                    self.mutex.lockUncancelable(io);
                    self.last_truncated_sequence = @max(self.last_truncated_sequence, truncate_sequence);
                    self.backlog.releaseThrough(truncate_sequence);
                    self.truncates_in_flight -= 1;
                    self.truncate_retry_not_before_ns = 0;
                    self.truncate_retry_backoff.reset();
                    self.cond.broadcast(io);
                }
                try self.checkVisibilityWaitUnlocked(io, wait);
                return;
            }
            try self.checkVisibilityWaitUnlocked(io, wait);
            try self.waitVisibilityProgressUnlocked(io, wait, Io.Duration.fromNanoseconds(std.time.ns_per_ms));
        }
    }

    fn recordError(self: *DerivedRuntime, io: Io, worker: *Worker, stage: []const u8, err: anyerror) void {
        std.log.err("derived worker failed worker={s} stage={s}: {s}", .{ worker.name, stage, @errorName(err) });
        self.mutex.lockUncancelable(io);
        defer self.mutex.unlock(io);
        // Attribute to the registration itself, never a name lookup that
        // could accidentally poison a newer incarnation with the same name.
        if (worker.last_error_name == null) worker.last_error_name = @errorName(err);
        self.signalWorkers(io);
    }

    fn computeMinPersistedLocked(self: *DerivedRuntime) u64 {
        if (self.workers.items.len == 0) return 0;
        var min_persisted: u64 = std.math.maxInt(u64);
        for (self.workers.items) |worker| {
            min_persisted = @min(min_persisted, worker.persisted_sequence);
        }
        return min_persisted;
    }
};

test "derived enrichment visibility guard observes cancellation and deadline" {
    var cancelled = std.atomic.Value(bool).init(true);
    try std.testing.expectError(
        error.EnrichmentWaitCanceled,
        (runtime_types.VisibilityWait{ .cancellation = types.CancellationToken.fromAtomic(&cancelled) }).check(),
    );
    cancelled.store(false, .release);
    try std.testing.expectError(
        error.EnrichmentWaitTimeout,
        (runtime_types.VisibilityWait{ .deadline_ns = platform_time.monotonicNs() }).check(),
    );
    var clock = @import("antfly_platform").clock.ManualClock{};
    clock.setRealtimeNs(100);
    const wait = runtime_types.VisibilityWait{ .clock = clock.clock(), .deadline_ns = 200 };
    try wait.check();
    clock.setRealtimeNs(200);
    try std.testing.expectError(error.EnrichmentWaitTimeout, wait.check());
}

fn workerStep(worker: *Worker) ?u64 {
    const runtime = worker.runtime;
    const io = runtime.ioContext();
    runtime.mutex.lockUncancelable(io);
    if (worker.paused or runtime.closing.load(.acquire)) {
        runtime.mutex.unlock(io);
        return null;
    }
    worker.dispatch_active = true;
    runtime.mutex.unlock(io);
    defer {
        runtime.mutex.lockUncancelable(io);
        worker.dispatch_active = false;
        runtime.cond.broadcast(io);
        runtime.mutex.unlock(io);
    }
    worker.next_delay_ms = 0;
    workerMain(worker);
    if (workerIsStopping(worker.runtime, worker, worker.runtime.ioContext())) return null;
    return worker.next_delay_ms;
}

fn workerMain(worker: *Worker) void {
    const runtime = worker.runtime;
    const io = runtime.ioContext();
    var close_success = true;
    defer if (!close_success or workerIsStopping(runtime, worker, io)) {
        _ = closeWorkerCatchUpState(runtime, worker, worker.applied_sequence, close_success) catch |err| {
            runtime.recordError(io, worker, "close_session", err);
        };
    };

    // One replay window per dispatch. Session state belongs to the worker
    // registration and survives yields; no physical thread is pinned at idle.
    for (0..1) |_| {
        runtime.mutex.lockUncancelable(io);
        const retry_remaining = if (worker.retry_not_before_ns == 0) 0 else worker.retry_not_before_ns -| platform_time.monotonicNs();
        if (!runtime.closing.load(.acquire) and !runtime.shutdown and !worker.stop and !worker.paused and runtime.last_error_name == null and worker.last_error_name == null and retry_remaining > 0) {
            // Foreground notifications may wake a delayed registration. Preserve
            // its retry deadline instead of letting hot writers defeat backoff.
            worker.next_delay_ms = @max(1, (retry_remaining +| (std.time.ns_per_ms - 1)) / std.time.ns_per_ms);
            runtime.mutex.unlock(io);
            return;
        }
        if (!runtime.closing.load(.acquire) and !runtime.shutdown and !worker.stop and !worker.paused and runtime.last_error_name == null and worker.last_error_name == null and worker.target_sequence <= worker.applied_sequence) {
            if (worker.applied_sequence > worker.persisted_sequence or
                (runtime.truncates_in_flight == 0 and runtime.computeMinPersistedLocked() > runtime.last_claimed_truncate_sequence))
            {
                const sequence = worker.applied_sequence;
                const needs_persistence = sequence > worker.persisted_sequence;
                const cleanup_delay = runtime.truncate_retry_not_before_ns -| platform_time.monotonicNs();
                if (worker.applied_sequence <= worker.persisted_sequence and cleanup_delay > 0) {
                    worker.next_delay_ms = delayMilliseconds(cleanup_delay);
                    runtime.mutex.unlock(io);
                    return;
                }
                runtime.mutex.unlock(io);
                const persisted = (if (needs_persistence)
                    persistIdleAppliedSequence(runtime, worker, sequence, io)
                else
                    cleanupIdleAppliedSequence(runtime, io)) catch |err| {
                    if (err == error.WorkerStopping) return;
                    if (catch_up_policy.isRecoverableAdmissionError(err)) {
                        scheduleRecoverableCatchUpRetry(worker, err);
                        return;
                    }
                    runtime.recordError(io, worker, "idle_persist", err);
                    return;
                };
                worker.recoverable_retry_backoff.reset();
                worker.retry_not_before_ns = 0;
                runtime.mutex.lockUncancelable(io);
                const remaining = runtime.truncate_retry_not_before_ns -| platform_time.monotonicNs();
                runtime.mutex.unlock(io);
                worker.next_delay_ms = if (remaining > 0) delayMilliseconds(remaining) else if (persisted) 0 else 50;
                return;
            }
            if (!worker.catch_up_open) {
                runtime.mutex.unlock(io);
                worker.next_delay_ms = null;
                return;
            }
            const close_requested = worker.catch_up_close_requested;
            runtime.mutex.unlock(io);
            const timestamp = platform_time.monotonicNs();
            if (worker.idle_since_ns == null) worker.idle_since_ns = timestamp;
            const policy = catch_up_policy.forIndex(worker.kind, runtime.backlog.resource_manager);
            const idle_ns = catch_up_policy.sessionIdleMaxWaitNs(policy, worker.last_replay_tail_records);
            const remaining = idle_ns -| (timestamp -| worker.idle_since_ns.?);
            if (!close_requested and remaining > 0) {
                worker.next_delay_ms = @max(1, remaining / std.time.ns_per_ms);
                return;
            }
            _ = closeWorkerCatchUpState(runtime, worker, worker.applied_sequence, true) catch |err| {
                close_success = false;
                runtime.recordError(io, worker, "idle_close", err);
                return;
            };
            worker.idle_since_ns = null;
            worker.next_delay_ms = null;
            return;
        }
        if (runtime.closing.load(.acquire) or runtime.shutdown or worker.stop or worker.paused or runtime.last_error_name != null or worker.last_error_name != null) {
            runtime.mutex.unlock(io);
            return;
        }
        const from_sequence = worker.applied_sequence;
        worker.idle_since_ns = null;
        const target_sequence = worker.target_sequence;
        const replay_tail_records = target_sequence -| from_sequence;
        if (replay_tail_records > 0) worker.last_replay_tail_records = replay_tail_records;
        worker.catch_up_active = true;
        runtime.mutex.unlock(io);

        // Do not pin the primary replay generation until the coalescing wait
        // has collected its batch. Otherwise a stable cursor can only observe
        // the pre-wait tail and every newly arrived record forces another
        // publication cycle.
        waitForReplayWindow(runtime, worker, from_sequence, io);
        ensureWorkerCatchUpState(runtime, worker, from_sequence) catch |err| {
            runtime.mutex.lockUncancelable(io);
            worker.catch_up_active = false;
            runtime.cond.broadcast(io);
            runtime.mutex.unlock(io);
            if (workerIsStopping(runtime, worker, io)) return;
            if (isRecoverableCatchUpError(worker, err)) {
                _ = closeWorkerCatchUpState(runtime, worker, worker.applied_sequence, false) catch |close_err| {
                    close_success = false;
                    runtime.recordError(io, worker, "recoverable_begin_catch_up_close", close_err);
                    return;
                };
                scheduleRecoverableCatchUpRetry(worker, err);
                continue;
            }
            close_success = false;
            runtime.recordError(io, worker, "begin_catch_up_session", err);
            return;
        };
        var stats = catchUpWorker(runtime, worker) catch |err| {
            runtime.mutex.lockUncancelable(io);
            worker.catch_up_active = false;
            runtime.cond.broadcast(io);
            runtime.mutex.unlock(io);
            if (workerIsStopping(runtime, worker, io)) return;
            if (isRecoverableCatchUpError(worker, err)) {
                _ = closeWorkerCatchUpState(runtime, worker, worker.applied_sequence, false) catch |close_err| {
                    close_success = false;
                    runtime.recordError(io, worker, "recoverable_catch_up_close", close_err);
                    return;
                };
                scheduleRecoverableCatchUpRetry(worker, err);
                continue;
            }
            close_success = false;
            runtime.recordError(io, worker, "catch_up", err);
            return;
        };
        if (stats.last_sequence == 0 and target_sequence > from_sequence) {
            if (worker.replay_cursor != null) {
                if (worker.replay_cursor.?.canFollowTail()) {
                    const target_visible = runtime.replay_source.isSequenceVisible(target_sequence) catch |err| {
                        runtime.mutex.lockUncancelable(io);
                        worker.catch_up_active = false;
                        runtime.cond.broadcast(io);
                        runtime.mutex.unlock(io);
                        close_success = false;
                        runtime.recordError(io, worker, "target_visibility", err);
                        return;
                    };
                    if (!target_visible) {
                        runtime.mutex.lockUncancelable(io);
                        worker.catch_up_active = false;
                        runtime.cond.broadcast(io);
                        runtime.mutex.unlock(io);
                        io.sleep(Io.Duration.zero, .awake) catch {};
                        continue;
                    }
                }
                closeWorkerReplayCursor(runtime, worker);
                ensureWorkerCatchUpState(runtime, worker, from_sequence) catch |err| {
                    runtime.mutex.lockUncancelable(io);
                    worker.catch_up_active = false;
                    runtime.cond.broadcast(io);
                    runtime.mutex.unlock(io);
                    if (workerIsStopping(runtime, worker, io)) return;
                    if (isRecoverableCatchUpError(worker, err)) {
                        _ = closeWorkerCatchUpState(runtime, worker, worker.applied_sequence, false) catch |close_err| {
                            close_success = false;
                            runtime.recordError(io, worker, "recoverable_refresh_replay_cursor_close", close_err);
                            return;
                        };
                        scheduleRecoverableCatchUpRetry(worker, err);
                        continue;
                    }
                    close_success = false;
                    runtime.recordError(io, worker, "refresh_replay_cursor", err);
                    return;
                };
                stats = catchUpWorker(runtime, worker) catch |err| {
                    runtime.mutex.lockUncancelable(io);
                    worker.catch_up_active = false;
                    runtime.cond.broadcast(io);
                    runtime.mutex.unlock(io);
                    if (workerIsStopping(runtime, worker, io)) return;
                    if (isRecoverableCatchUpError(worker, err)) {
                        _ = closeWorkerCatchUpState(runtime, worker, worker.applied_sequence, false) catch |close_err| {
                            close_success = false;
                            runtime.recordError(io, worker, "recoverable_refreshed_catch_up_close", close_err);
                            return;
                        };
                        scheduleRecoverableCatchUpRetry(worker, err);
                        continue;
                    }
                    close_success = false;
                    runtime.recordError(io, worker, "catch_up_refreshed", err);
                    return;
                };
            }
        }
        runtime.mutex.lockUncancelable(io);
        worker.catch_up_active = false;
        runtime.cond.broadcast(io);
        runtime.mutex.unlock(io);
        if (stats.last_sequence == 0 and worker.replay_cursor != null and !worker.replay_cursor.?.canFollowTail()) {
            closeWorkerReplayCursor(runtime, worker);
        }

        const target_advance_allowed = if (stats.shouldTryTargetAdvance(from_sequence, target_sequence))
            canAdvanceToTarget(runtime, worker, from_sequence, target_sequence) catch |err| {
                close_success = false;
                runtime.recordError(io, worker, "target_advance", err);
                return;
            }
        else
            false;
        const caught_up_sequence = if (stats.appliedSequenceAdvance(from_sequence)) |sequence|
            sequence
        else if (target_advance_allowed)
            target_sequence
        else
            from_sequence;
        if (caught_up_sequence == from_sequence and stats.shouldTryTargetAdvance(from_sequence, target_sequence)) {
            _ = closeWorkerCatchUpState(runtime, worker, worker.applied_sequence, false) catch |err| {
                close_success = false;
                runtime.recordError(io, worker, "coverage_gap_close", err);
                return;
            };
            io.sleep(Io.Duration.fromNanoseconds(50 * std.time.ns_per_ms), .awake) catch {};
            continue;
        }

        var persisted = false;
        if (caught_up_sequence > from_sequence) {
            const finish_result = closeWorkerCatchUpState(runtime, worker, caught_up_sequence, true) catch |err| {
                if (isRecoverablePublishError(worker, err)) {
                    scheduleRecoverableCatchUpRetry(worker, err);
                    continue;
                }
                close_success = false;
                runtime.recordError(io, worker, "publish_catch_up", err);
                return;
            };
            persisted = finish_result.applied_sequence_persisted;
        }

        if (caught_up_sequence > from_sequence and !persisted) {
            persisted = runtime.persist_fn(runtime.ctx, worker.name, caught_up_sequence, forcePersistAppliedSequence(worker)) catch |err| {
                if (catch_up_policy.isRecoverableAdmissionError(err)) {
                    scheduleRecoverableCatchUpRetry(worker, err);
                    continue;
                }
                runtime.recordError(io, worker, "persist", err);
                return;
            };
        }

        var truncate_sequence: u64 = 0;
        runtime.mutex.lockUncancelable(io);
        const applied_sequence_advanced = caught_up_sequence > worker.applied_sequence;
        if (applied_sequence_advanced) {
            worker.applied_sequence = caught_up_sequence;
        }
        if (persisted and caught_up_sequence > worker.persisted_sequence) {
            worker.persisted_sequence = caught_up_sequence;
        }
        if (runtime.truncates_in_flight == 0 and platform_time.monotonicNs() >= runtime.truncate_retry_not_before_ns and worker.persisted_sequence > runtime.last_claimed_truncate_sequence) {
            const min_persisted = runtime.computeMinPersistedLocked();
            if (min_persisted > runtime.last_claimed_truncate_sequence) {
                runtime.last_claimed_truncate_sequence = min_persisted;
                truncate_sequence = min_persisted;
            }
        }
        if (truncate_sequence > 0) {
            runtime.truncates_in_flight += 1;
        } else {
            runtime.cond.broadcast(io);
        }
        runtime.mutex.unlock(io);
        if (applied_sequence_advanced) if (runtime.applied_sequence_advanced_fn) |callback| {
            callback(runtime.ctx, worker.name, caught_up_sequence);
        };

        if (shouldRefreshReplayCursor(worker, caught_up_sequence)) {
            closeWorkerReplayCursor(runtime, worker);
        }

        if (truncate_sequence > 0) {
            completeWorkerTruncateAttempt(runtime, truncate_sequence, io) catch |err| {
                runtime.recordError(io, worker, "truncate", err);
                return;
            };
        }
        worker.recoverable_retry_backoff.reset();
        worker.retry_not_before_ns = 0;
    }
}

fn cleanupIdleAppliedSequence(runtime: *DerivedRuntime, io: Io) !bool {
    try attemptPendingTruncate(runtime, io);
    return true;
}

fn persistIdleAppliedSequence(runtime: *DerivedRuntime, worker: *Worker, sequence: u64, io: Io) !bool {
    const persisted = try runtime.persist_fn(runtime.ctx, worker.name, sequence, false);
    runtime.mutex.lockUncancelable(io);
    if (persisted and sequence > worker.persisted_sequence) {
        worker.persisted_sequence = sequence;
    }
    const truncate_sequence = claimPendingTruncateLocked(runtime);
    if (truncate_sequence == 0) runtime.cond.broadcast(io);
    runtime.mutex.unlock(io);
    if (truncate_sequence != 0) try completeWorkerTruncateAttempt(runtime, truncate_sequence, io);
    return persisted;
}

/// Caller holds the runtime mutex. Durable publication and cleanup admission
/// use the same critical section without an additional lock per idle dispatch.
fn claimPendingTruncateLocked(runtime: *DerivedRuntime) u64 {
    if (runtime.truncates_in_flight != 0 or platform_time.monotonicNs() < runtime.truncate_retry_not_before_ns) return 0;
    const minimum = runtime.computeMinPersistedLocked();
    if (minimum <= runtime.last_claimed_truncate_sequence) return 0;
    runtime.last_claimed_truncate_sequence = minimum;
    runtime.truncates_in_flight += 1;
    return minimum;
}

/// Cleanup does not need to re-persist an already durable worker watermark.
/// All callers share one exclusive claim and one contention backoff.
fn attemptPendingTruncate(runtime: *DerivedRuntime, io: Io) !void {
    runtime.mutex.lockUncancelable(io);
    const sequence = claimPendingTruncateLocked(runtime);
    if (sequence == 0) runtime.cond.broadcast(io);
    runtime.mutex.unlock(io);
    if (sequence != 0) try completeWorkerTruncateAttempt(runtime, sequence, io);
}

fn truncateWithVisibilityWait(runtime: *DerivedRuntime, sequence: u64, wait: runtime_types.VisibilityWait, io: Io) !void {
    while (true) {
        try wait.check();
        runtime.mutex.lockUncancelable(io);
        const remaining = runtime.truncate_retry_not_before_ns -| platform_time.monotonicNs();
        runtime.mutex.unlock(io);
        if (remaining != 0) {
            var sleep_ns = @min(remaining, 10 * std.time.ns_per_ms);
            if (wait.deadline_ns) |deadline| {
                const now = if (wait.clock) |clock| clock.nowRealtimeNs() else platform_time.monotonicNs();
                sleep_ns = @min(sleep_ns, deadline -| now);
            }
            io.sleep(Io.Duration.fromNanoseconds(@intCast(sleep_ns)), .awake) catch {};
            continue;
        }
        runtime.truncate_fn(runtime.ctx, sequence) catch |err| {
            if (!catch_up_policy.isRecoverableAdmissionError(err)) return err;
            runtime.mutex.lockUncancelable(io);
            const delay = catch_up_policy.recordRecoverableRetry(&runtime.recoverable_retry_counters, runtime.backlog.resource_manager, &runtime.truncate_retry_backoff, err);
            runtime.truncate_retry_not_before_ns = platform_time.monotonicNs() +| delay;
            runtime.mutex.unlock(io);
            continue;
        };
        return;
    }
}

fn delayMilliseconds(ns: u64) u64 {
    return @max(1, (ns +| (std.time.ns_per_ms - 1)) / std.time.ns_per_ms);
}

/// A scheduler dispatch attempts cleanup once. Credit and the completion
/// watermark remain unchanged on failure; another worker can retry the claim
/// only after the shared backoff expires. No sleep holds a derived-work slot.
fn completeWorkerTruncateAttempt(runtime: *DerivedRuntime, sequence: u64, io: Io) !void {
    runtime.truncate_fn(runtime.ctx, sequence) catch |err| {
        runtime.mutex.lockUncancelable(io);
        defer runtime.mutex.unlock(io);
        runtime.truncates_in_flight -= 1;
        runtime.last_claimed_truncate_sequence = runtime.last_truncated_sequence;
        if (catch_up_policy.isRecoverableAdmissionError(err)) {
            const delay = catch_up_policy.recordRecoverableRetry(&runtime.recoverable_retry_counters, runtime.backlog.resource_manager, &runtime.truncate_retry_backoff, err);
            runtime.truncate_retry_not_before_ns = platform_time.monotonicNs() +| delay;
            runtime.signalWorkers(io);
            runtime.cond.broadcast(io);
            return;
        }
        runtime.cond.broadcast(io);
        return err;
    };
    runtime.mutex.lockUncancelable(io);
    defer runtime.mutex.unlock(io);
    runtime.last_truncated_sequence = @max(runtime.last_truncated_sequence, sequence);
    runtime.backlog.releaseThrough(sequence);
    runtime.truncates_in_flight -= 1;
    runtime.truncate_retry_not_before_ns = 0;
    runtime.truncate_retry_backoff.reset();
    runtime.cond.broadcast(io);
}

fn ensureWorkerCatchUpState(runtime: *DerivedRuntime, worker: *Worker, from_sequence: u64) !void {
    if (!catch_up_policy.deferSourceCapture(worker.kind, runtime.backlog.resource_manager))
        try ensureWorkerSourceCapture(runtime, worker);
    if (worker.replay_cursor == null) {
        worker.replay_cursor = try runtime.replay_source.openMatchingCursor(
            runtime.alloc,
            from_sequence,
            derived_worker.targetHintForManagedIndex(worker.kind),
        );
        worker.replay_cursor_open_sequence = from_sequence;
    }
}

fn ensureWorkerSourceCapture(runtime: *DerivedRuntime, worker: *Worker) !void {
    if (!worker.catch_up_open) {
        const io = runtime.ioContext();
        runtime.mutex.lockUncancelable(io);
        worker.catch_up_close_failed = false;
        runtime.mutex.unlock(io);
        worker.catch_up_token = if (runtime.begin_catch_up_fn) |begin_catch_up|
            try begin_catch_up(runtime.ctx, worker.kind)
        else
            .{};
        runtime.mutex.lockUncancelable(io);
        worker.catch_up_open = true;
        runtime.mutex.unlock(io);
    }
}

fn beginCollectedWindowCapture(ctx: *anyopaque, _: index_manager_mod.ManagedIndexRef) !void {
    const worker: *Worker = @ptrCast(@alignCast(ctx));
    // Called after collection and before any apply callback. Subsequent chunks
    // borrow this same token; no source record or coalesced transaction is split.
    try ensureWorkerSourceCapture(worker.runtime, worker);
}

fn closeWorkerReplayCursor(runtime: *DerivedRuntime, worker: *Worker) void {
    const io = runtime.ioContext();
    runtime.mutex.lockUncancelable(io);
    var replay_cursor = worker.replay_cursor;
    worker.replay_cursor = null;
    worker.replay_cursor_open_sequence = 0;
    runtime.mutex.unlock(io);

    if (replay_cursor) |*cursor| cursor.deinit(runtime.alloc);
}

fn closeWorkerCatchUpState(
    runtime: *DerivedRuntime,
    worker: *Worker,
    applied_sequence: u64,
    success: bool,
) !CatchUpFinishResult {
    const io = runtime.ioContext();
    runtime.mutex.lockUncancelable(io);
    var replay_cursor = worker.replay_cursor;
    const catch_up_open = worker.catch_up_open;
    const token = worker.catch_up_token;
    worker.replay_cursor = null;
    worker.replay_cursor_open_sequence = 0;
    worker.catch_up_open = false;
    worker.catch_up_token = .{};
    worker.catch_up_close_requested = false;
    if (catch_up_open) {
        worker.catch_up_close_active = true;
        worker.catch_up_close_failed = false;
    }
    worker.last_replay_tail_records = 0;
    runtime.mutex.unlock(io);

    if (replay_cursor) |*cursor| cursor.deinit(runtime.alloc);
    if (!catch_up_open) return .{};

    var finish_result: CatchUpFinishResult = .{};
    if (runtime.finish_catch_up_fn) |finish_catch_up| {
        finish_result = finish_catch_up(runtime.ctx, worker.kind, token, applied_sequence, success) catch |err| {
            runtime.mutex.lockUncancelable(io);
            worker.catch_up_close_active = false;
            worker.catch_up_close_failed = true;
            runtime.cond.broadcast(io);
            runtime.mutex.unlock(io);
            return err;
        };
    }
    {
        runtime.mutex.lockUncancelable(io);
        worker.catch_up_close_active = false;
        worker.catch_up_close_failed = false;
        runtime.cond.broadcast(io);
        runtime.mutex.unlock(io);
    }
    if (worker.kind.kind == .dense_vector and @import("../../dense_perf_experiments.zig").enabled("ANTFLY_EXPERIMENT_CAPTURE_STAGES"))
        std.log.info("dense replay capture finish token={} sequence={} success={} applied_sequence_persisted={}", .{
            token.value, applied_sequence, success, finish_result.applied_sequence_persisted,
        });
    return finish_result;
}

fn isRecoverablePublishError(worker: *const Worker, err: anyerror) bool {
    if (catch_up_policy.isRecoverableAdmissionError(err)) return true;
    return switch (err) {
        // Structural reconciliation can retire the old HBC streaming session
        // after replay work completes but before this worker publishes it.
        // The applied checkpoint is advanced only after a successful close,
        // so reopening and replaying is idempotent and preserves visibility.
        error.NoActiveWriteSession => true,
        error.NotFound => catch_up_policy.forIndex(worker.kind, worker.runtime.backlog.resource_manager).not_found_is_recoverable,
        error.ReplayDocumentNotVisible,
        error.ArtifactRepairRequired,
        => true,
        else => false,
    };
}

fn isRecoverableCatchUpError(worker: *const Worker, err: anyerror) bool {
    if (catch_up_policy.isRecoverableAdmissionError(err)) return true;
    return switch (err) {
        error.ReplayDocumentNotVisible,
        error.PostingWalCaptureOwnershipConflict,
        error.ArtifactRepairRequired,
        => true,
        error.NotFound => catch_up_policy.forIndex(worker.kind, worker.runtime.backlog.resource_manager).not_found_is_recoverable,
        else => false,
    };
}

fn workerIsStopping(runtime: *DerivedRuntime, worker: *const Worker, io: Io) bool {
    runtime.mutex.lockUncancelable(io);
    defer runtime.mutex.unlock(io);
    return runtime.closing.load(.acquire) or runtime.shutdown or worker.stop or worker.paused or runtime.last_error_name != null or worker.last_error_name != null;
}

fn scheduleRecoverableCatchUpRetry(worker: *Worker, err: anyerror) void {
    const delay_ns = catch_up_policy.recordRecoverableRetry(
        &worker.runtime.recoverable_retry_counters,
        worker.runtime.backlog.resource_manager,
        &worker.recoverable_retry_backoff,
        err,
    );
    if (worker.recoverable_retry_backoff.shouldLog()) std.log.warn(
        "derived worker retrying recoverable failure worker={s} error={s} failures={} retry_ms={}",
        .{ worker.name, @errorName(err), worker.recoverable_retry_backoff.failures, delay_ns / std.time.ns_per_ms },
    );
    worker.retry_not_before_ns = platform_time.monotonicNs() +| delay_ns;
    worker.next_delay_ms = @max(1, delay_ns / std.time.ns_per_ms);
}

fn waitForReplayWindow(runtime: *DerivedRuntime, worker: *Worker, from_sequence: u64, io: Io) void {
    const policy = catch_up_policy.forIndex(worker.kind, runtime.backlog.resource_manager);
    const delay_ns = policy.coalesce_delay_ns;
    if (delay_ns == 0) return;

    var waited_ns: u64 = 0;
    while (true) {
        runtime.mutex.lockUncancelable(io);
        const shutdown = runtime.closing.load(.acquire) or runtime.shutdown or worker.stop or worker.paused or runtime.last_error_name != null or worker.last_error_name != null;
        const target = worker.target_sequence;
        const pending_records = target -| from_sequence;
        const force_sequence = runtime.force_catch_up_sequence;
        runtime.mutex.unlock(io);

        const max_wait_ns = catch_up_policy.replayWindowMaxWaitNs(policy, pending_records);
        if (shutdown or pending_records == 0 or max_wait_ns == 0 or force_sequence > from_sequence or waited_ns >= max_wait_ns) return;

        const sleep_ns = @min(delay_ns, max_wait_ns - waited_ns);
        io.sleep(Io.Duration.fromNanoseconds(@intCast(sleep_ns)), .awake) catch {};
        waited_ns +|= sleep_ns;

        runtime.mutex.lockUncancelable(io);
        const target_advanced = worker.target_sequence > target;
        runtime.mutex.unlock(io);
        if (!target_advanced) return;
    }
}

fn catchUpWorker(runtime: *DerivedRuntime, worker: *Worker) !derived_worker.CatchUpStats {
    const policy = catch_up_policy.forIndex(worker.kind, runtime.backlog.resource_manager);
    const deferred_capture = catch_up_policy.deferSourceCapture(worker.kind, runtime.backlog.resource_manager);
    if (worker.replay_cursor == null) {
        try ensureWorkerCatchUpState(runtime, worker, worker.applied_sequence);
    }
    const max_windows_per_call: usize = blk: {
        const io = runtime.ioContext();
        runtime.mutex.lockUncancelable(io);
        defer runtime.mutex.unlock(io);
        if (runtime.force_catch_up_sequence >= worker.target_sequence) break :blk 0;
        break :blk policy.max_windows_per_publish;
    };
    const ApplySession = struct {
        runtime: *DerivedRuntime,
        worker: *Worker,

        fn apply(ptr: *anyopaque, batch: derived_types.DerivedBatch, index_ref: index_manager_mod.ManagedIndexRef) anyerror!bool {
            const session: *@This() = @ptrCast(@alignCast(ptr));
            // Deferred capture can open during collection. Read the token at
            // callback time, after beginCollectedWindowCapture has installed it.
            return session.runtime.apply_fn(session.runtime.ctx, batch, index_ref, session.worker.catch_up_token);
        }
    };
    var session = ApplySession{ .runtime = runtime, .worker = worker };
    const capture_before_collection = worker.catch_up_open;
    const stats = try derived_worker.catchUpIndexFromMatchingCursor(
        runtime.alloc,
        &worker.replay_cursor.?,
        worker.kind,
        &session,
        ApplySession.apply,
        .{
            .resource_manager = runtime.backlog.resource_manager,
            .window_ctx = worker,
            .begin_window_fn = if (deferred_capture) beginCollectedWindowCapture else null,
            .max_windows_per_call = max_windows_per_call,
            .max_call_ns = policy.max_call_ns,
            .max_call_bytes = policy.max_call_bytes,
            .max_items_per_window = policy.max_items_per_window,
            .max_chunk_bytes = policy.max_chunk_bytes,
            .estimated_dense_vector_bytes = policy.estimated_dense_vector_bytes,
            .max_work_chunk_bytes = policy.max_work_chunk_bytes,
            .dense_replay_working_set_factor = policy.dense_replay_working_set_factor,
            .target_sequence = worker.target_sequence,
        },
    );
    if (worker.kind.kind == .dense_vector and @import("../../dense_perf_experiments.zig").enabled("ANTFLY_EXPERIMENT_CAPTURE_STAGES"))
        std.log.info("dense replay collection token={} sequence={} records={} applied_windows={} deferred_capture={} capture_before_collection={} collect_ns={} apply_ns={}", .{
            worker.catch_up_token.value, stats.last_sequence,     stats.scanned_entries, stats.applied_entries, deferred_capture,
            capture_before_collection,   stats.window_collect_ns, stats.apply_ns,
        });
    return stats;
}

fn shouldRefreshReplayCursor(worker: *const Worker, caught_up_sequence: u64) bool {
    const cursor = worker.replay_cursor orelse return false;
    if (cursor.canFollowTail()) return false;
    if (caught_up_sequence <= worker.replay_cursor_open_sequence) return false;
    // Primary-store replay cursors pin an LSM read snapshot. Refresh them
    // after each successful catch-up window so hot ingest does not hold a
    // cloned mutable memtable open across unrelated writes.
    return true;
}

fn stopAndJoinWorker(runtime: *DerivedRuntime, worker: *Worker, io: Io) void {
    runtime.mutex.lockUncancelable(io);
    worker.stop = true;
    runtime.signalWorkers(io);
    runtime.mutex.unlock(io);
    if (worker.future) |*future| _ = future.await(io);
}

const TestThreadedRuntimeCapture = struct {
    require_capture_worker: ?*Worker = null,
    fail_next_begin: bool = false,
    empty_coverage_checks: @import("antfly_platform").atomic.Value(u64) = .init(0),
    runtime: ?*DerivedRuntime = null,
    apply_calls: @import("antfly_platform").atomic.Value(u64) = .init(0),
    begin_calls: @import("antfly_platform").atomic.Value(u64) = .init(0),
    finish_calls: @import("antfly_platform").atomic.Value(u64) = .init(0),
    publish_failures: @import("antfly_platform").atomic.Value(u64) = .init(0),
    apply_not_found_failures: @import("antfly_platform").atomic.Value(u64) = .init(0),
    resource_budget_failures: @import("antfly_platform").atomic.Value(u64) = .init(0),
    persisted_sequence: @import("antfly_platform").atomic.Value(u64) = .init(0),
    truncate_calls: @import("antfly_platform").atomic.Value(u64) = .init(0),
    truncated_sequence: @import("antfly_platform").atomic.Value(u64) = .init(0),
    advanced_sequence: @import("antfly_platform").atomic.Value(u64) = .init(0),
    callback_observed_applied_sequence: @import("antfly_platform").atomic.Value(u64) = .init(0),
    fail_next_forced_persist: std.atomic.Value(bool) = .init(false),
    fail_next_dense_apply_not_found: std.atomic.Value(bool) = .init(false),
    fail_next_apply_resource_budget: std.atomic.Value(bool) = .init(false),
    fail_next_apply_would_block: std.atomic.Value(bool) = .init(false),
    would_block_failures: std.atomic.Value(u64) = .init(0),
    fail_next_publish: std.atomic.Value(bool) = .init(false),
    fail_next_truncate_writer_locked: std.atomic.Value(bool) = .init(false),
    block_finish: std.atomic.Value(bool) = .init(false),
    finish_entered: std.atomic.Value(bool) = .init(false),
    release_finish: std.atomic.Value(bool) = .init(false),
};

fn testThreadedRuntimeAppliedSequenceAdvanced(ctx: *anyopaque, index_name: []const u8, sequence: u64) void {
    const capture: *TestThreadedRuntimeCapture = @ptrCast(@alignCast(ctx));
    capture.advanced_sequence.store(sequence, .release);
    const runtime = capture.runtime orelse return;
    capture.callback_observed_applied_sequence.store(runtime.appliedSequence(index_name) orelse 0, .release);
}

fn testThreadedRuntimeApply(ctx: *anyopaque, batch: derived_types.DerivedBatch, index_ref: index_manager_mod.ManagedIndexRef, token: CatchUpSessionToken) !bool {
    _ = batch;
    const capture: *TestThreadedRuntimeCapture = @ptrCast(@alignCast(ctx));
    if (capture.require_capture_worker) |worker| {
        try std.testing.expect(worker.catch_up_open);
        try std.testing.expect(!worker.catch_up_token.isNone());
        try std.testing.expectEqual(worker.catch_up_token, token);
        try std.testing.expect(worker.replay_cursor != null);
    }
    _ = capture.apply_calls.fetchAdd(1, .monotonic);
    if (capture.fail_next_apply_would_block.swap(false, .monotonic)) {
        _ = capture.would_block_failures.fetchAdd(1, .monotonic);
        return error.WouldBlock;
    }
    if (capture.fail_next_apply_resource_budget.swap(false, .monotonic)) {
        _ = capture.resource_budget_failures.fetchAdd(1, .monotonic);
        return error.ResourceBudgetExceeded;
    }
    if (index_ref.kind == .dense_vector and capture.fail_next_dense_apply_not_found.swap(false, .monotonic)) {
        _ = capture.apply_not_found_failures.fetchAdd(1, .monotonic);
        return error.NotFound;
    }
    return true;
}

fn testThreadedRuntimePersist(ctx: *anyopaque, index_name: []const u8, sequence: u64, force: bool) !bool {
    _ = index_name;
    const capture: *TestThreadedRuntimeCapture = @ptrCast(@alignCast(ctx));
    if (force and capture.fail_next_forced_persist.swap(false, .monotonic)) return error.Canceled;
    capture.persisted_sequence.store(sequence, .monotonic);
    return true;
}

fn testThreadedRuntimeTruncate(ctx: *anyopaque, sequence: u64) !void {
    const capture: *TestThreadedRuntimeCapture = @ptrCast(@alignCast(ctx));
    _ = capture.truncate_calls.fetchAdd(1, .monotonic);
    if (capture.fail_next_truncate_writer_locked.swap(false, .monotonic)) return error.WriterLocked;
    capture.truncated_sequence.store(sequence, .monotonic);
}

fn testThreadedRuntimeBeginCatchUp(ctx: *anyopaque, index_ref: index_manager_mod.ManagedIndexRef) !CatchUpSessionToken {
    _ = index_ref;
    const capture: *TestThreadedRuntimeCapture = @ptrCast(@alignCast(ctx));
    if (capture.fail_next_begin) {
        capture.fail_next_begin = false;
        return error.ResourceBudgetExceeded;
    }
    return .{ .value = capture.begin_calls.fetchAdd(1, .monotonic) + 1 };
}

fn testThreadedRuntimeFinishCatchUp(
    ctx: *anyopaque,
    index_ref: index_manager_mod.ManagedIndexRef,
    token: CatchUpSessionToken,
    applied_sequence: u64,
    success: bool,
) !CatchUpFinishResult {
    _ = index_ref;
    _ = token;
    _ = applied_sequence;
    const capture: *TestThreadedRuntimeCapture = @ptrCast(@alignCast(ctx));
    _ = capture.finish_calls.fetchAdd(1, .monotonic);
    if (capture.block_finish.load(.acquire)) {
        capture.finish_entered.store(true, .release);
        while (!capture.release_finish.load(.acquire)) std.atomic.spinLoopHint();
    }
    if (success and capture.fail_next_publish.swap(false, .monotonic)) {
        _ = capture.publish_failures.fetchAdd(1, .monotonic);
        return error.NotFound;
    }
    return .{};
}

fn testThreadedRuntimeJournalOpenOptions() change_journal_mod.OpenOptions {
    return .{
        .backend = .lsm_memory,
        .lsm_options = .{
            .flush_threshold = 512,
            .compact_threshold_runs = 256,
            .wal_enabled = false,
            .obsolete_retention_ns = 0,
        },
    };
}

fn appendTestThreadedRuntimeRecord(log: *change_journal_mod.Journal, alloc: Allocator, record: change_journal_mod.Record) !void {
    const payload = try change_journal_mod.encodeRecord(alloc, record);
    defer alloc.free(payload);
    _ = try log.appendOpaque(payload);
}

test "io threaded deferred source capture excludes preparation and preserves failure ownership" {
    const alloc = std.testing.allocator;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    const path = try std.fmt.allocPrintSentinel(alloc, ".zig-cache/tmp/{s}/late-capture", .{tmp.sub_path}, 0);
    defer alloc.free(path);
    var journal = try change_journal_mod.Journal.open(path, testThreadedRuntimeJournalOpenOptions());
    defer journal.close();
    try appendTestThreadedRuntimeRecord(&journal, alloc, .{
        .sequence = 1,
        .changed_doc_keys = &.{"doc:a"},
        .target_hints = &.{ .dense_vector, .full_text },
    });
    for ([_]bool{ false, true }) |enabled| {
        var manager = resource_manager_mod.ResourceManager.init(.{});
        defer manager.deinit(alloc);
        manager.dense_deferred_source_capture = enabled;
        var capture: TestThreadedRuntimeCapture = .{};
        var runtime = try DerivedRuntime.init(alloc, replay_source_mod.Source.fromJournal(&journal), &capture, testThreadedRuntimeApply, testThreadedRuntimePersist, testThreadedRuntimeTruncate, testThreadedRuntimeBeginCatchUp, testThreadedRuntimeFinishCatchUp, null, null, &manager);
        defer runtime.deinit();
        var name = "dense".*;
        var worker: Worker = .{ .runtime = &runtime, .name = &name, .kind = .{ .name = &name, .kind = .dense_vector }, .applied_sequence = 0, .persisted_sequence = 0, .target_sequence = 1 };
        defer _ = closeWorkerCatchUpState(&runtime, &worker, 0, false) catch {};
        capture.require_capture_worker = &worker;

        try ensureWorkerCatchUpState(&runtime, &worker, 0);
        try std.testing.expect(worker.replay_cursor != null);
        try std.testing.expectEqual(!enabled, worker.catch_up_open);
        const stats = try catchUpWorker(&runtime, &worker);
        try std.testing.expectEqual(@as(u64, 1), stats.last_sequence);
        try std.testing.expectEqual(@as(u64, 1), capture.begin_calls.load(.monotonic));
        try std.testing.expectEqual(@as(u64, 1), capture.apply_calls.load(.monotonic));
        const first_token = worker.catch_up_token;
        // More collected windows retain the exact owner, never mint a borrower
        // capable of closing a later transaction or publish before finish.
        try beginCollectedWindowCapture(&worker, worker.kind);
        try std.testing.expectEqual(first_token, worker.catch_up_token);
        try std.testing.expectEqual(@as(u64, 0), capture.finish_calls.load(.monotonic));
        _ = try closeWorkerCatchUpState(&runtime, &worker, 1, true);
        try std.testing.expect(worker.replay_cursor == null and !worker.catch_up_open);
        try std.testing.expectEqual(@as(u64, 1), capture.finish_calls.load(.monotonic));

        // An empty cursor does not need a mutation lease in the deferred path.
        try ensureWorkerCatchUpState(&runtime, &worker, 1);
        const empty = try catchUpWorker(&runtime, &worker);
        try std.testing.expectEqual(@as(u64, 0), empty.last_sequence);
        try std.testing.expectEqual(!enabled, worker.catch_up_open);
        _ = try closeWorkerCatchUpState(&runtime, &worker, 1, true);

        if (enabled) {
            capture.fail_next_begin = true;
            try ensureWorkerCatchUpState(&runtime, &worker, 0);
            try std.testing.expectError(error.ResourceBudgetExceeded, catchUpWorker(&runtime, &worker));
            try std.testing.expect(!worker.catch_up_open and worker.catch_up_token.isNone());
            try std.testing.expectEqual(@as(u64, 1), capture.apply_calls.load(.monotonic));
            _ = try closeWorkerCatchUpState(&runtime, &worker, 0, false);
            try std.testing.expect(worker.replay_cursor == null);
            // Retry starts from the persisted boundary, not the consumed cursor.
            try ensureWorkerCatchUpState(&runtime, &worker, 0);
            _ = try catchUpWorker(&runtime, &worker);
            try std.testing.expect(worker.catch_up_token.value > first_token.value);
            _ = try closeWorkerCatchUpState(&runtime, &worker, 1, true);
        }
        worker.kind.kind = .full_text;
        try ensureWorkerCatchUpState(&runtime, &worker, 0);
        try std.testing.expect(worker.catch_up_open); // unchanged non-dense policy
        _ = try closeWorkerCatchUpState(&runtime, &worker, 0, false);
    }
}

test "io threaded deferred source capture advances empty targets only through coverage guard" {
    const alloc = std.testing.allocator;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    const path = try std.fmt.allocPrintSentinel(alloc, ".zig-cache/tmp/{s}/empty-late-capture", .{tmp.sub_path}, 0);
    defer alloc.free(path);
    var journal = try change_journal_mod.Journal.open(path, testThreadedRuntimeJournalOpenOptions());
    defer journal.close();
    try appendTestThreadedRuntimeRecord(&journal, alloc, .{
        .sequence = 1,
        .changed_doc_keys = &.{"text:only"},
        .target_hints = &.{.full_text},
    });
    const Coverage = struct {
        fn allow(ctx: *anyopaque, _: index_manager_mod.ManagedIndexRef, from: u64, target: u64) !bool {
            const capture: *TestThreadedRuntimeCapture = @ptrCast(@alignCast(ctx));
            try std.testing.expectEqual(@as(u64, 0), from);
            try std.testing.expectEqual(@as(u64, 1), target);
            // The first guard refusal must leave the durable boundary at 0.
            return capture.empty_coverage_checks.fetchAdd(1, .monotonic) != 0;
        }
    };
    var manager = resource_manager_mod.ResourceManager.init(.{});
    defer manager.deinit(alloc);
    manager.dense_deferred_source_capture = true;
    var capture: TestThreadedRuntimeCapture = .{};
    var runtime = try DerivedRuntime.init(alloc, replay_source_mod.Source.fromJournal(&journal), &capture, testThreadedRuntimeApply, testThreadedRuntimePersist, testThreadedRuntimeTruncate, testThreadedRuntimeBeginCatchUp, testThreadedRuntimeFinishCatchUp, Coverage.allow, null, &manager);
    defer runtime.deinit();
    try runtime.addWorker("dense", .{ .name = "dense", .kind = .dense_vector }, 0);
    try runtime.waitForAllWithVisibilityWait(1, .{ .deadline_ns = platform_time.monotonicNs() + 5 * std.time.ns_per_s });
    try std.testing.expectEqual(@as(u64, 2), capture.empty_coverage_checks.load(.monotonic));
    try std.testing.expectEqual(@as(u64, 0), capture.begin_calls.load(.monotonic));
    try std.testing.expectEqual(@as(u64, 0), capture.apply_calls.load(.monotonic));
    try std.testing.expectEqual(@as(u64, 0), capture.finish_calls.load(.monotonic));
    try std.testing.expectEqual(@as(u64, 1), capture.persisted_sequence.load(.monotonic));
}

test "io threaded worker keeps the dense replay working-set factor" {
    const alloc = std.testing.allocator;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    const path = try std.fmt.allocPrintSentinel(alloc, ".zig-cache/tmp/{s}/working-set-factor", .{tmp.sub_path}, 0);
    defer alloc.free(path);
    var journal = try change_journal_mod.Journal.open(path, testThreadedRuntimeJournalOpenOptions());
    defer journal.close();
    var manager = resource_manager_mod.ResourceManager.init(.{});
    defer manager.deinit(alloc);
    var capture: TestThreadedRuntimeCapture = .{};
    var runtime = try DerivedRuntime.init(alloc, replay_source_mod.Source.fromJournal(&journal), &capture, testThreadedRuntimeApply, testThreadedRuntimePersist, testThreadedRuntimeTruncate, testThreadedRuntimeBeginCatchUp, testThreadedRuntimeFinishCatchUp, null, null, &manager);
    defer runtime.deinit();
    const kind: index_manager_mod.ManagedIndexRef = .{
        .name = "dense",
        .kind = .dense_vector,
        .estimated_dense_vector_bytes = 8 * 1536 * @sizeOf(f32),
        .dense_replay_working_set_factor = 8,
    };
    try runtime.addWorker("dense", kind, 0);
    const worker = runtime.workers.items[0];
    try std.testing.expectEqual(@as(u64, 8), worker.kind.dense_replay_working_set_factor);
    // The policy the worker replays under pairs the scaled estimate with the
    // scaled ceiling, exactly as the catalog's index reference does.
    const from_worker = catch_up_policy.forIndex(worker.kind, null);
    const from_catalog = catch_up_policy.forIndex(kind, null);
    try std.testing.expectEqual(from_catalog.max_chunk_bytes, from_worker.max_chunk_bytes);
    try std.testing.expectEqual(from_catalog.estimated_dense_vector_bytes, from_worker.estimated_dense_vector_bytes);
}

test "io threaded scheduled terminal pass releases its retained session" {
    var capture = TestThreadedRuntimeCapture{};
    var runtime = try DerivedRuntime.init(
        std.testing.allocator,
        undefined,
        &capture,
        testThreadedRuntimeApply,
        testThreadedRuntimePersist,
        testThreadedRuntimeTruncate,
        null,
        testThreadedRuntimeFinishCatchUp,
        null,
        null,
        null,
    );
    defer runtime.deinit();
    var worker = Worker{
        .runtime = &runtime,
        .name = @constCast("terminal"),
        .kind = .{ .name = "terminal", .kind = .full_text },
        .applied_sequence = 0,
        .persisted_sequence = 0,
        .target_sequence = 0,
        .catch_up_open = true,
    };
    runtime.last_error_name = "TerminalProbe";
    try std.testing.expectEqual(@as(?u64, null), workerStep(&worker));
    try std.testing.expect(!worker.catch_up_open);
    try std.testing.expectEqual(@as(u64, 1), capture.finish_calls.load(.monotonic));
}

test "io threaded forced persist errors unwind snapshot ownership safely" {
    const alloc = std.testing.allocator;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    const journal_path = try std.fmt.allocPrint(alloc, ".zig-cache/tmp/{s}/io-threaded-forced-persist-error-journal", .{tmp.sub_path});
    defer alloc.free(journal_path);
    const journal_path_z = try alloc.dupeSentinel(u8, journal_path, 0);
    defer alloc.free(journal_path_z);

    var journal = try change_journal_mod.Journal.open(journal_path_z, testThreadedRuntimeJournalOpenOptions());
    defer journal.close();

    var capture = TestThreadedRuntimeCapture{};
    var runtime = try DerivedRuntime.init(
        alloc,
        replay_source_mod.Source.fromJournal(&journal),
        &capture,
        testThreadedRuntimeApply,
        testThreadedRuntimePersist,
        testThreadedRuntimeTruncate,
        null,
        null,
        null,
        null,
        null,
    );
    defer runtime.deinit();

    try runtime.addWorker("text_idx", .{ .name = "text_idx", .kind = .full_text }, 1);

    capture.fail_next_forced_persist.store(true, .monotonic);
    try std.testing.expectError(error.Canceled, runtime.waitForAll(1));

    capture.fail_next_forced_persist.store(true, .monotonic);
    try std.testing.expectError(error.Canceled, runtime.waitForIndexes(1, &.{"text_idx"}));
}

test "io threaded applied callback observes published watermark outside runtime lock" {
    const alloc = std.testing.allocator;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    const journal_path = try std.fmt.allocPrint(alloc, ".zig-cache/tmp/{s}/io-threaded-applied-callback-journal", .{tmp.sub_path});
    defer alloc.free(journal_path);
    const journal_path_z = try alloc.dupeSentinel(u8, journal_path, 0);
    defer alloc.free(journal_path_z);

    var journal = try change_journal_mod.Journal.open(journal_path_z, testThreadedRuntimeJournalOpenOptions());
    defer journal.close();
    try appendTestThreadedRuntimeRecord(&journal, alloc, .{
        .sequence = 1,
        .changed_doc_keys = &.{"doc:a"},
        .target_hints = &.{.full_text},
    });

    var capture = TestThreadedRuntimeCapture{};
    var runtime = try DerivedRuntime.init(
        alloc,
        replay_source_mod.Source.fromJournal(&journal),
        &capture,
        testThreadedRuntimeApply,
        testThreadedRuntimePersist,
        testThreadedRuntimeTruncate,
        null,
        null,
        null,
        testThreadedRuntimeAppliedSequenceAdvanced,
        null,
    );
    capture.runtime = &runtime;
    defer runtime.deinit();

    try std.testing.expectEqual(
        Io.Limit.limited(threaded_io_limits.service),
        runtime.threaded.concurrent_limit,
    );

    try runtime.addWorker("text_idx", .{ .name = "text_idx", .kind = .full_text }, 0);
    runtime.notifySequence(1);
    try runtime.waitForAll(1);
    try runtime.failIfUnhealthy();

    try std.testing.expectEqual(@as(u64, 1), capture.advanced_sequence.load(.acquire));
    try std.testing.expectEqual(@as(u64, 1), capture.callback_observed_applied_sequence.load(.acquire));
}

test "io threaded wait observes worker-owned catch-up close" {
    const alloc = std.testing.allocator;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    const journal_path = try std.fmt.allocPrint(alloc, ".zig-cache/tmp/{s}/io-threaded-worker-lifetime-journal", .{tmp.sub_path});
    defer alloc.free(journal_path);
    const journal_path_z = try alloc.dupeSentinel(u8, journal_path, 0);
    defer alloc.free(journal_path_z);

    var journal = try change_journal_mod.Journal.open(journal_path_z, testThreadedRuntimeJournalOpenOptions());
    defer journal.close();
    try appendTestThreadedRuntimeRecord(&journal, alloc, .{
        .sequence = 1,
        .changed_doc_keys = &.{"doc:a"},
        .target_hints = &.{.dense_vector},
    });

    var capture = TestThreadedRuntimeCapture{};
    capture.block_finish.store(true, .release);
    var runtime = try DerivedRuntime.init(
        alloc,
        replay_source_mod.Source.fromJournal(&journal),
        &capture,
        testThreadedRuntimeApply,
        testThreadedRuntimePersist,
        testThreadedRuntimeTruncate,
        testThreadedRuntimeBeginCatchUp,
        testThreadedRuntimeFinishCatchUp,
        null,
        null,
        null,
    );
    defer runtime.deinit();

    try runtime.addWorker("dense_idx", .{ .name = "dense_idx", .kind = .dense_vector }, 0);
    const io = runtime.ioContext();

    const Race = struct {
        runtime: *DerivedRuntime,
        wait_started: std.atomic.Value(bool) = .init(false),
        wait_failed: std.atomic.Value(bool) = .init(false),
        wait_done: std.atomic.Value(bool) = .init(false),

        fn wait(self: *@This()) void {
            self.wait_started.store(true, .release);
            self.runtime.waitForAll(1) catch {
                self.wait_failed.store(true, .release);
            };
            self.wait_done.store(true, .release);
        }
    };
    var race = Race{ .runtime = &runtime };
    var wait_thread = try std.testing.io.concurrent(Race.wait, .{&race});
    var wait_joined = false;
    defer if (!wait_joined) {
        capture.release_finish.store(true, .release);
        wait_thread.await(std.testing.io);
    };

    for (0..5_000) |_| {
        if (capture.finish_entered.load(.acquire)) break;
        io.sleep(Io.Duration.fromMilliseconds(1), .awake) catch {};
    } else return error.TestTimeout;
    for (0..5_000) |_| {
        if (race.wait_started.load(.acquire)) break;
        io.sleep(Io.Duration.fromMilliseconds(1), .awake) catch {};
    } else return error.TestTimeout;

    // A waiter may observe the applied watermark while its worker still owns
    // the corresponding publish callback. It must not steal that session or
    // report completion until the worker finishes closing it. It also must not
    // leave a close request behind for the next session while this one closes.
    io.sleep(Io.Duration.fromMilliseconds(25), .awake) catch {};
    try std.testing.expect(!race.wait_done.load(.acquire));
    {
        runtime.mutex.lockUncancelable(io);
        defer runtime.mutex.unlock(io);
        try std.testing.expect(runtime.workers.items[0].catch_up_close_active);
        try std.testing.expect(!runtime.workers.items[0].catch_up_close_requested);
    }

    capture.release_finish.store(true, .release);
    wait_thread.await(std.testing.io);
    wait_joined = true;

    try std.testing.expect(!race.wait_failed.load(.acquire));
    try std.testing.expect(race.wait_done.load(.acquire));
    {
        runtime.mutex.lockUncancelable(io);
        defer runtime.mutex.unlock(io);
        try std.testing.expect(!runtime.workers.items[0].catch_up_close_failed);
        try std.testing.expect(!runtime.workers.items[0].catch_up_close_requested);
    }
}

test "io threaded wait requests prompt worker catch-up close" {
    const alloc = std.testing.allocator;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    const journal_path = try std.fmt.allocPrint(alloc, ".zig-cache/tmp/{s}/io-threaded-worker-close-request-journal", .{tmp.sub_path});
    defer alloc.free(journal_path);
    const journal_path_z = try alloc.dupeSentinel(u8, journal_path, 0);
    defer alloc.free(journal_path_z);

    var journal = try change_journal_mod.Journal.open(journal_path_z, testThreadedRuntimeJournalOpenOptions());
    defer journal.close();

    var capture = TestThreadedRuntimeCapture{};
    capture.block_finish.store(true, .release);
    var runtime = try DerivedRuntime.init(
        alloc,
        replay_source_mod.Source.fromJournal(&journal),
        &capture,
        testThreadedRuntimeApply,
        testThreadedRuntimePersist,
        testThreadedRuntimeTruncate,
        testThreadedRuntimeBeginCatchUp,
        testThreadedRuntimeFinishCatchUp,
        null,
        null,
        null,
    );
    defer runtime.deinit();

    try runtime.addWorker("dense_idx", .{ .name = "dense_idx", .kind = .dense_vector }, 1);
    const io = runtime.ioContext();
    runtime.mutex.lockUncancelable(io);
    runtime.workers.items[0].catch_up_open = true;
    runtime.signalWorkers(io);
    runtime.mutex.unlock(io);

    const Wait = struct {
        runtime: *DerivedRuntime,
        failed: std.atomic.Value(bool) = .init(false),
        done: std.atomic.Value(bool) = .init(false),

        fn run(self: *@This()) void {
            self.runtime.waitForAll(1) catch {
                self.failed.store(true, .release);
            };
            self.done.store(true, .release);
        }
    };
    var wait = Wait{ .runtime = &runtime };
    var wait_thread = try std.testing.io.concurrent(Wait.run, .{&wait});
    var wait_joined = false;
    defer if (!wait_joined) {
        capture.release_finish.store(true, .release);
        wait_thread.await(std.testing.io);
    };

    // Dense workers normally retain an idle session for reuse. A synchronous
    // wait must ask the owner to publish promptly instead of inheriting that
    // multi-second idle window.
    for (0..1_000) |_| {
        if (capture.finish_entered.load(.acquire)) break;
        io.sleep(Io.Duration.fromMilliseconds(1), .awake) catch {};
    } else return error.TestTimeout;
    try std.testing.expect(!wait.done.load(.acquire));

    capture.release_finish.store(true, .release);
    wait_thread.await(std.testing.io);
    wait_joined = true;
    try std.testing.expect(!wait.failed.load(.acquire));
    try std.testing.expect(wait.done.load(.acquire));
}

test "io threaded wait observes failed worker-owned catch-up close" {
    const alloc = std.testing.allocator;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    const journal_path = try std.fmt.allocPrint(alloc, ".zig-cache/tmp/{s}/io-threaded-worker-close-failure-journal", .{tmp.sub_path});
    defer alloc.free(journal_path);
    const journal_path_z = try alloc.dupeSentinel(u8, journal_path, 0);
    defer alloc.free(journal_path_z);

    var journal = try change_journal_mod.Journal.open(journal_path_z, testThreadedRuntimeJournalOpenOptions());
    defer journal.close();

    var capture = TestThreadedRuntimeCapture{};
    var runtime = try DerivedRuntime.init(
        alloc,
        replay_source_mod.Source.fromJournal(&journal),
        &capture,
        testThreadedRuntimeApply,
        testThreadedRuntimePersist,
        testThreadedRuntimeTruncate,
        testThreadedRuntimeBeginCatchUp,
        testThreadedRuntimeFinishCatchUp,
        null,
        null,
        null,
    );
    defer runtime.deinit();

    try runtime.addWorker("dense_idx", .{ .name = "dense_idx", .kind = .dense_vector }, 1);
    const io = runtime.ioContext();
    runtime.mutex.lockUncancelable(io);
    const worker = runtime.workers.items[0];
    worker.catch_up_open = true;
    runtime.mutex.unlock(io);
    capture.fail_next_publish.store(true, .release);

    try std.testing.expectError(error.NotFound, closeWorkerCatchUpState(&runtime, worker, worker.applied_sequence, true));
    {
        runtime.mutex.lockUncancelable(io);
        defer runtime.mutex.unlock(io);
        try std.testing.expect(!worker.catch_up_close_active);
        try std.testing.expect(worker.catch_up_close_failed);
    }

    const Wait = struct {
        runtime: *DerivedRuntime,
        saw_expected_error: std.atomic.Value(bool) = .init(false),
        done: std.atomic.Value(bool) = .init(false),

        fn run(self: *@This()) void {
            self.runtime.waitForAll(1) catch |err| {
                self.saw_expected_error.store(err == RuntimeError.AsyncWorkerFailed, .release);
            };
            self.done.store(true, .release);
        }

        fn failRuntime(self: *@This()) void {
            const runtime_io = self.runtime.ioContext();
            self.runtime.mutex.lockUncancelable(runtime_io);
            if (self.runtime.last_error_name == null) self.runtime.last_error_name = @errorName(error.NotFound);
            self.runtime.cond.broadcast(runtime_io);
            self.runtime.mutex.unlock(runtime_io);
        }
    };
    var wait = Wait{ .runtime = &runtime };
    var wait_thread = try std.testing.io.concurrent(Wait.run, .{&wait});
    var wait_joined = false;
    defer if (!wait_joined) {
        wait.failRuntime();
        wait_thread.await(std.testing.io);
    };

    io.sleep(Io.Duration.fromMilliseconds(25), .awake) catch {};
    try std.testing.expect(!wait.done.load(.acquire));

    wait.failRuntime();
    wait_thread.await(std.testing.io);
    wait_joined = true;
    try std.testing.expect(wait.done.load(.acquire));
    try std.testing.expect(wait.saw_expected_error.load(.acquire));
}

test "io threaded worker backoffs and retries replay truncation writer lock" {
    const alloc = std.testing.allocator;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    const journal_path = try std.fmt.allocPrint(alloc, ".zig-cache/tmp/{s}/io-threaded-truncate-writer-lock-retry-journal", .{tmp.sub_path});
    defer alloc.free(journal_path);
    const journal_path_z = try alloc.dupeSentinel(u8, journal_path, 0);
    defer alloc.free(journal_path_z);

    var journal = try change_journal_mod.Journal.open(journal_path_z, testThreadedRuntimeJournalOpenOptions());
    defer journal.close();
    try appendTestThreadedRuntimeRecord(&journal, alloc, .{
        .sequence = 1,
        .changed_doc_keys = &.{"doc:a"},
        .target_hints = &.{.full_text},
    });

    var manager = resource_manager_mod.ResourceManager.init(.{});
    defer manager.deinit(alloc);
    var capture = TestThreadedRuntimeCapture{};
    capture.fail_next_truncate_writer_locked.store(true, .monotonic);
    var runtime = try DerivedRuntime.init(
        alloc,
        replay_source_mod.Source.fromJournal(&journal),
        &capture,
        testThreadedRuntimeApply,
        testThreadedRuntimePersist,
        testThreadedRuntimeTruncate,
        testThreadedRuntimeBeginCatchUp,
        testThreadedRuntimeFinishCatchUp,
        null,
        null,
        &manager,
    );
    defer runtime.deinit();

    try runtime.addWorker("text_idx", .{ .name = "text_idx", .kind = .full_text }, 0);
    runtime.notifySequence(1);
    try runtime.waitForAll(1);
    try runtime.failIfUnhealthy();

    try std.testing.expectEqual(@as(u64, 2), capture.truncate_calls.load(.monotonic));
    try std.testing.expectEqual(@as(u64, 1), capture.truncated_sequence.load(.monotonic));
    try std.testing.expectEqual(@as(u64, 1), runtime.snapshotStats().writer_locked_retries);
    try std.testing.expectEqual(@as(u64, 1), manager.derivedRecoverableRetryStats().writer_locked);
    const io = runtime.ioContext();
    runtime.mutex.lockUncancelable(io);
    defer runtime.mutex.unlock(io);
    try std.testing.expectEqual(@as(u8, 0), runtime.workers.items[0].recoverable_retry_backoff.failures);
}

test "io threaded dense catch-up NotFound closes session before retry" {
    const alloc = std.testing.allocator;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    const journal_path = try std.fmt.allocPrint(alloc, ".zig-cache/tmp/{s}/io-threaded-dense-catch-up-retry-journal", .{tmp.sub_path});
    defer alloc.free(journal_path);
    const journal_path_z = try alloc.dupeSentinel(u8, journal_path, 0);
    defer alloc.free(journal_path_z);

    var journal = try change_journal_mod.Journal.open(journal_path_z, testThreadedRuntimeJournalOpenOptions());
    defer journal.close();
    try appendTestThreadedRuntimeRecord(&journal, alloc, .{
        .sequence = 1,
        .changed_doc_keys = &.{"doc:a"},
        .target_hints = &.{.dense_vector},
    });

    var capture = TestThreadedRuntimeCapture{};
    capture.fail_next_dense_apply_not_found.store(true, .monotonic);
    var runtime = try DerivedRuntime.init(
        alloc,
        replay_source_mod.Source.fromJournal(&journal),
        &capture,
        testThreadedRuntimeApply,
        testThreadedRuntimePersist,
        testThreadedRuntimeTruncate,
        testThreadedRuntimeBeginCatchUp,
        testThreadedRuntimeFinishCatchUp,
        null,
        null,
        null,
    );
    defer runtime.deinit();

    try runtime.addWorker("dense_idx", .{ .name = "dense_idx", .kind = .dense_vector }, 0);
    runtime.notifySequence(1);
    try runtime.waitForAll(1);
    try runtime.failIfUnhealthy();

    try std.testing.expectEqual(@as(u64, 1), capture.apply_not_found_failures.load(.monotonic));
    try std.testing.expect(capture.apply_calls.load(.monotonic) >= 2);
    try std.testing.expect(capture.begin_calls.load(.monotonic) >= 2);
    try std.testing.expect(capture.finish_calls.load(.monotonic) >= 2);
    try std.testing.expectEqual(@as(u64, 1), runtime.appliedSequence("dense_idx").?);
    try std.testing.expectEqual(@as(u64, 1), capture.persisted_sequence.load(.monotonic));
}

test "io threaded dense publish NotFound retries with a fresh session" {
    const alloc = std.testing.allocator;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    const journal_path = try std.fmt.allocPrint(alloc, ".zig-cache/tmp/{s}/io-threaded-dense-publish-retry-journal", .{tmp.sub_path});
    defer alloc.free(journal_path);
    const journal_path_z = try alloc.dupeSentinel(u8, journal_path, 0);
    defer alloc.free(journal_path_z);

    var journal = try change_journal_mod.Journal.open(journal_path_z, testThreadedRuntimeJournalOpenOptions());
    defer journal.close();
    try appendTestThreadedRuntimeRecord(&journal, alloc, .{
        .sequence = 1,
        .changed_doc_keys = &.{"doc:a"},
        .target_hints = &.{.dense_vector},
    });

    var capture = TestThreadedRuntimeCapture{};
    capture.fail_next_publish.store(true, .monotonic);
    var runtime = try DerivedRuntime.init(
        alloc,
        replay_source_mod.Source.fromJournal(&journal),
        &capture,
        testThreadedRuntimeApply,
        testThreadedRuntimePersist,
        testThreadedRuntimeTruncate,
        testThreadedRuntimeBeginCatchUp,
        testThreadedRuntimeFinishCatchUp,
        null,
        null,
        null,
    );
    defer runtime.deinit();

    try runtime.addWorker("dense_idx", .{ .name = "dense_idx", .kind = .dense_vector }, 0);
    runtime.notifySequence(1);
    try runtime.waitForAll(1);
    try runtime.failIfUnhealthy();

    try std.testing.expectEqual(@as(u64, 1), capture.publish_failures.load(.monotonic));
    try std.testing.expect(capture.apply_calls.load(.monotonic) >= 2);
    try std.testing.expect(capture.begin_calls.load(.monotonic) >= 2);
    try std.testing.expect(capture.finish_calls.load(.monotonic) >= 2);
    try std.testing.expectEqual(@as(u64, 1), runtime.appliedSequence("dense_idx").?);
    try std.testing.expectEqual(@as(u64, 1), capture.persisted_sequence.load(.monotonic));
}

test "io threaded full-text resource pressure retries without poisoning runtime" {
    const alloc = std.testing.allocator;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    const journal_path = try std.fmt.allocPrint(alloc, ".zig-cache/tmp/{s}/io-threaded-full-text-resource-retry-journal", .{tmp.sub_path});
    defer alloc.free(journal_path);
    const journal_path_z = try alloc.dupeSentinel(u8, journal_path, 0);
    defer alloc.free(journal_path_z);

    var journal = try change_journal_mod.Journal.open(journal_path_z, testThreadedRuntimeJournalOpenOptions());
    defer journal.close();
    try appendTestThreadedRuntimeRecord(&journal, alloc, .{
        .sequence = 1,
        .changed_doc_keys = &.{"doc:a"},
        .target_hints = &.{.full_text},
    });

    var capture = TestThreadedRuntimeCapture{};
    capture.fail_next_apply_resource_budget.store(true, .monotonic);
    var runtime = try DerivedRuntime.init(
        alloc,
        replay_source_mod.Source.fromJournal(&journal),
        &capture,
        testThreadedRuntimeApply,
        testThreadedRuntimePersist,
        testThreadedRuntimeTruncate,
        testThreadedRuntimeBeginCatchUp,
        testThreadedRuntimeFinishCatchUp,
        null,
        null,
        null,
    );
    defer runtime.deinit();

    try runtime.addWorker("text_idx", .{ .name = "text_idx", .kind = .full_text }, 0);
    runtime.notifySequence(1);
    try runtime.waitForAll(1);
    try runtime.failIfUnhealthy();

    try std.testing.expectEqual(@as(u64, 1), capture.resource_budget_failures.load(.monotonic));
    try std.testing.expect(capture.apply_calls.load(.monotonic) >= 2);
    try std.testing.expect(capture.begin_calls.load(.monotonic) >= 2);
    try std.testing.expect(capture.finish_calls.load(.monotonic) >= 2);
    try std.testing.expectEqual(@as(u64, 1), runtime.appliedSequence("text_idx").?);
    try std.testing.expectEqual(@as(u64, 1), capture.persisted_sequence.load(.monotonic));
}

test "derived worker pause retains registration watermark and allocation-free resume" {
    const alloc = std.testing.allocator;
    var capture = TestThreadedRuntimeCapture{};
    var runtime = try DerivedRuntime.init(alloc, undefined, &capture, testThreadedRuntimeApply, testThreadedRuntimePersist, testThreadedRuntimeTruncate, null, null, null, null, null);
    defer runtime.deinit();
    try runtime.addWorker("paused", .{ .name = "paused", .kind = .graph }, 1);
    try runtime.addWorker("other", .{ .name = "other", .kind = .graph }, 4);
    const worker = runtime.workers.items[0];
    const registration = worker.future.?.task;
    try std.testing.expect(try runtime.pauseWorker("paused"));
    try std.testing.expectEqual(@as(u64, 1), runtime.computeMinPersistedLocked());
    runtime.notifyIndexes(1, &.{"paused"});
    try std.testing.expectError(error.EnrichmentWaitTimeout, runtime.waitForIndexesWithVisibilityWait(4, &.{"paused"}, .{ .deadline_ns = 0 }));
    // The visibility wait retains its target. Reset it only because this unit
    // test has no replay source; the DB regressions exercise that pending debt.
    const io = runtime.ioContext();
    runtime.mutex.lockUncancelable(io);
    worker.target_sequence = 1;
    runtime.mutex.unlock(io);
    var failing = std.testing.FailingAllocator.init(alloc, .{ .fail_index = 0 });
    runtime.alloc = failing.allocator();
    runtime.resumeWorker("paused");
    // Pause once more before examining fields that the resumed dispatch owns.
    try std.testing.expect(try runtime.pauseWorker("paused"));
    runtime.alloc = alloc;
    try std.testing.expect(!failing.has_induced_failure);
    try std.testing.expectEqual(registration, worker.future.?.task);
    try std.testing.expectEqual(@as(u64, 1), worker.target_sequence);
    runtime.removeWorker("paused");
    try std.testing.expectEqual(@as(u64, 4), runtime.computeMinPersistedLocked());
}

test "derived worker pause session close failure keeps retention and fails health closed" {
    const alloc = std.testing.allocator;
    const Failure = struct {
        fn finish(_: *anyopaque, _: index_manager_mod.ManagedIndexRef, _: CatchUpSessionToken, _: u64, _: bool) !CatchUpFinishResult {
            return error.OutOfMemory;
        }
    };
    var capture = TestThreadedRuntimeCapture{};
    var runtime = try DerivedRuntime.init(alloc, undefined, &capture, testThreadedRuntimeApply, testThreadedRuntimePersist, testThreadedRuntimeTruncate, null, Failure.finish, null, null, null);
    defer runtime.deinit();
    const worker = try alloc.create(Worker);
    worker.* = .{ .runtime = &runtime, .name = try alloc.dupe(u8, "paused"), .kind = .{ .name = "paused", .kind = .graph }, .applied_sequence = 1, .persisted_sequence = 1, .target_sequence = 1, .catch_up_open = true };
    try runtime.workers.append(alloc, worker);
    @import("antfly_test_error_logs").expectErrorLogs(1);
    try std.testing.expectError(error.OutOfMemory, runtime.pauseWorker("paused"));
    try std.testing.expectError(RuntimeError.AsyncWorkerFailed, runtime.failIfUnhealthy());
    try std.testing.expectEqual(@as(?u64, 1), runtime.appliedSequence("paused"));
    try std.testing.expectEqual(@as(u64, 1), runtime.computeMinPersistedLocked());
    try std.testing.expect(!worker.paused);
}

test "derived worker pause drains active truncation retries and releases backlog" {
    const alloc = std.testing.allocator;
    const Probe = struct {
        io: std.Io,
        entered: std.Io.Event = .unset,
        release: std.Io.Event = .unset,
        paused: std.Io.Event = .unset,
        pause_error: ?anyerror = null,
        truncate_calls: usize = 0,
        fn persist(_: *anyopaque, _: []const u8, _: u64, _: bool) !bool {
            return true;
        }
        fn truncate(ptr: *anyopaque, _: u64) !void {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            self.entered.set(self.io);
            self.release.waitUncancelable(self.io);
            self.truncate_calls += 1;
            if (self.truncate_calls == 1) return error.WriterLocked;
        }
        fn pause(self: *@This(), runtime: *DerivedRuntime) void {
            _ = runtime.pauseWorker("active") catch |err| {
                self.pause_error = err;
                self.paused.set(self.io);
                return;
            };
            self.paused.set(self.io);
        }
    };
    var manager = resource_manager_mod.ResourceManager.init(.{});
    defer manager.deinit(alloc);
    var probe: Probe = undefined;
    var runtime = try DerivedRuntime.init(alloc, undefined, &probe, testThreadedRuntimeApply, Probe.persist, Probe.truncate, null, null, null, null, &manager);
    defer runtime.deinit();
    const io = runtime.ioContext();
    probe = .{ .io = io };
    try runtime.trackBacklogBytes(1, 64);
    try runtime.addWorker("active", .{ .name = "active", .kind = .graph }, 0);
    const worker = runtime.workers.items[0];
    runtime.mutex.lockUncancelable(io);
    worker.applied_sequence = 1;
    worker.target_sequence = 1;
    runtime.signalWorkers(io);
    runtime.mutex.unlock(io);
    const timeout: std.Io.Timeout = .{ .duration = .{ .raw = std.Io.Duration.fromSeconds(10), .clock = .awake } };
    // Ensure test failure can never strand a callback or its pause joiner.
    defer probe.release.set(io);
    try probe.entered.waitTimeout(io, timeout);
    var pause_task = try io.concurrent(Probe.pause, .{ &probe, &runtime });
    defer {
        probe.release.set(io);
        pause_task.await(io);
    }
    const deadline = platform_time.monotonicNs() + 10 * std.time.ns_per_s;
    while (true) {
        runtime.mutex.lockUncancelable(io);
        const admitted = worker.paused;
        const active = worker.dispatch_active;
        runtime.mutex.unlock(io);
        if (admitted) {
            try std.testing.expect(active);
            break;
        }
        if (platform_time.monotonicNs() >= deadline) return error.TestUnexpectedResult;
        try io.sleep(std.Io.Duration.fromMilliseconds(1), .awake);
    }
    probe.release.set(io);
    try probe.paused.waitTimeout(io, timeout);
    try std.testing.expect(probe.pause_error == null);
    try std.testing.expect(!worker.dispatch_active);
    try std.testing.expect(worker.paused);
    try std.testing.expectEqual(@as(usize, 2), probe.truncate_calls);
    try std.testing.expectEqual(@as(u64, 0), runtime.backlog.retained_bytes);
    runtime.removeWorker("active");
}

test "issue1015 full-text catch-up WouldBlock retries without poisoning worker" {
    const alloc = std.testing.allocator;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    const journal_path = try std.fmt.allocPrint(alloc, ".zig-cache/tmp/{s}/issue1015-would-block-journal", .{tmp.sub_path});
    defer alloc.free(journal_path);
    const journal_path_z = try alloc.dupeSentinel(u8, journal_path, 0);
    defer alloc.free(journal_path_z);

    var journal = try change_journal_mod.Journal.open(journal_path_z, testThreadedRuntimeJournalOpenOptions());
    defer journal.close();
    try appendTestThreadedRuntimeRecord(&journal, alloc, .{
        .sequence = 1,
        .changed_doc_keys = &.{"doc:a"},
        .target_hints = &.{.full_text},
    });

    var capture = TestThreadedRuntimeCapture{};
    capture.fail_next_apply_would_block.store(true, .monotonic);
    var runtime = try DerivedRuntime.init(
        alloc,
        replay_source_mod.Source.fromJournal(&journal),
        &capture,
        testThreadedRuntimeApply,
        testThreadedRuntimePersist,
        testThreadedRuntimeTruncate,
        testThreadedRuntimeBeginCatchUp,
        testThreadedRuntimeFinishCatchUp,
        null,
        null,
        null,
    );
    defer runtime.deinit();

    try runtime.addWorker("text_idx", .{ .name = "text_idx", .kind = .full_text }, 0);
    runtime.notifySequence(1);
    try runtime.waitForAllWithVisibilityWait(1, .{ .deadline_ns = platform_time.monotonicNs() + 10 * std.time.ns_per_s });
    try runtime.failIfUnhealthy();

    try std.testing.expectEqual(@as(u64, 1), capture.would_block_failures.load(.monotonic));
    try std.testing.expect(capture.apply_calls.load(.monotonic) >= 2);
    try std.testing.expect(capture.begin_calls.load(.monotonic) >= 2);
    try std.testing.expect(capture.finish_calls.load(.monotonic) >= 2);
    try std.testing.expectEqual(@as(u64, 1), runtime.appliedSequence("text_idx").?);
    try std.testing.expectEqual(@as(u64, 1), capture.persisted_sequence.load(.monotonic));
}

test "issue1015 terminal worker failure leaves unrelated worker operational and retains journal" {
    const alloc = std.testing.allocator;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    const path = try std.fmt.allocPrint(alloc, ".zig-cache/tmp/{s}/isolated-worker-journal", .{tmp.sub_path});
    defer alloc.free(path);
    const path_z = try alloc.dupeSentinel(u8, path, 0);
    defer alloc.free(path_z);
    var journal = try change_journal_mod.Journal.open(path_z, testThreadedRuntimeJournalOpenOptions());
    defer journal.close();
    try appendTestThreadedRuntimeRecord(&journal, alloc, .{ .sequence = 1, .changed_doc_keys = &.{"doc:a"}, .target_hints = &.{.full_text} });
    const Apply = struct {
        fn apply(ctx: *anyopaque, batch: derived_types.DerivedBatch, index: index_manager_mod.ManagedIndexRef, token: CatchUpSessionToken) !bool {
            if (std.mem.eql(u8, index.name, "failed")) return error.InvalidData;
            return testThreadedRuntimeApply(ctx, batch, index, token);
        }
    };
    var capture = TestThreadedRuntimeCapture{};
    var runtime = try DerivedRuntime.init(alloc, replay_source_mod.Source.fromJournal(&journal), &capture, Apply.apply, testThreadedRuntimePersist, testThreadedRuntimeTruncate, testThreadedRuntimeBeginCatchUp, testThreadedRuntimeFinishCatchUp, null, null, null);
    defer runtime.deinit();
    @import("antfly_test_error_logs").expectErrorLogs(2);
    try runtime.addWorker("failed", .{ .name = "failed", .kind = .full_text }, 0);
    try runtime.addWorker("healthy", .{ .name = "healthy", .kind = .full_text }, 0);
    runtime.notifySequence(1);
    try runtime.waitForIndexesWithVisibilityWait(1, &.{"healthy"}, .{ .deadline_ns = platform_time.monotonicNs() + 10 * std.time.ns_per_s });
    // Wait for the failing worker too, with a deadline so regressions cannot hang.
    try std.testing.expectError(RuntimeError.AsyncWorkerFailed, runtime.waitForIndexesWithVisibilityWait(1, &.{"failed"}, .{ .deadline_ns = platform_time.monotonicNs() + 10 * std.time.ns_per_s }));
    try std.testing.expectError(RuntimeError.AsyncWorkerFailed, runtime.failIfUnhealthy());
    try std.testing.expectEqual(@as(?u64, 0), runtime.appliedSequence("failed"));
    try std.testing.expectEqual(@as(?u64, 1), runtime.appliedSequence("healthy"));
    try std.testing.expectEqual(@as(u64, 1), runtime.snapshotStats().failed_workers);
    try std.testing.expectEqual(@as(u64, 0), capture.truncated_sequence.load(.monotonic));
}

test "issue1015 foreground notifications cannot defeat worker contention backoff" {
    const alloc = std.testing.allocator;
    var capture = TestThreadedRuntimeCapture{};
    var runtime = try DerivedRuntime.init(alloc, undefined, &capture, testThreadedRuntimeApply, testThreadedRuntimePersist, testThreadedRuntimeTruncate, null, null, null, null, null);
    defer runtime.deinit();
    var worker = Worker{ .runtime = &runtime, .name = @constCast("delayed"), .kind = .{ .name = "delayed", .kind = .full_text }, .applied_sequence = 0, .persisted_sequence = 0, .target_sequence = 1 };
    worker.retry_not_before_ns = platform_time.monotonicNs() + std.time.ns_per_s;
    // No replay source exists: every early wake must yield before opening it.
    for (0..100) |_| try std.testing.expect((workerStep(&worker) orelse 0) > 0);
    try std.testing.expectEqual(@as(u64, 0), capture.apply_calls.load(.monotonic));
    try std.testing.expectEqual(@as(u64, 0), capture.begin_calls.load(.monotonic));
}

test "issue1015 truncation contention recovers credit and failed claims remain retryable" {
    const alloc = std.testing.allocator;
    const Probe = struct {
        failure: anyerror,
        calls: usize = 0,
        fn persist(_: *anyopaque, _: []const u8, _: u64, _: bool) !bool {
            return true;
        }
        fn truncate(ptr: *anyopaque, _: u64) !void {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            self.calls += 1;
            if (self.calls == 1) return self.failure;
        }
    };
    for ([_]anyerror{ error.WouldBlock, error.FileBusy, error.InvalidData }) |failure| {
        var manager = resource_manager_mod.ResourceManager.init(.{});
        defer manager.deinit(alloc);
        var probe = Probe{ .failure = failure };
        var runtime = try DerivedRuntime.init(alloc, undefined, &probe, testThreadedRuntimeApply, Probe.persist, Probe.truncate, null, null, null, null, &manager);
        defer runtime.deinit();
        const worker = try alloc.create(Worker);
        worker.* = .{ .runtime = &runtime, .name = try alloc.dupe(u8, "cleanup"), .kind = .{ .name = "cleanup", .kind = .graph }, .applied_sequence = 10, .persisted_sequence = 0, .target_sequence = 10 };
        try runtime.workers.append(alloc, worker);
        try runtime.trackBacklogBytes(10, 64);
        const retained = runtime.backlog.retained_bytes;
        try std.testing.expect(retained >= 64);
        const io = runtime.ioContext();
        if (failure == error.InvalidData) {
            try std.testing.expectError(failure, persistIdleAppliedSequence(&runtime, worker, 10, io));
        } else {
            try std.testing.expect(try persistIdleAppliedSequence(&runtime, worker, 10, io));
            // Notifications and other workers must not defeat runtime backoff.
            runtime.truncate_retry_not_before_ns = platform_time.monotonicNs() + std.time.ns_per_s;
            for (0..100) |_| _ = try persistIdleAppliedSequence(&runtime, worker, 10, io);
            try std.testing.expectEqual(@as(usize, 1), probe.calls);
            runtime.truncate_retry_not_before_ns = 0;
        }
        try std.testing.expectEqual(@as(u64, 0), runtime.last_truncated_sequence);
        try std.testing.expectEqual(@as(u64, 0), runtime.last_claimed_truncate_sequence);
        try std.testing.expectEqual(retained, runtime.backlog.retained_bytes);
        try std.testing.expect(try persistIdleAppliedSequence(&runtime, worker, 10, io));
        try std.testing.expectEqual(@as(usize, 2), probe.calls);
        try std.testing.expectEqual(@as(u64, 10), runtime.last_truncated_sequence);
        try std.testing.expectEqual(@as(u64, 0), runtime.backlog.retained_bytes);
        try runtime.failIfUnhealthy();
    }
}

test "issue1015 failed registrations stay parked during primary notifications" {
    const alloc = std.testing.allocator;
    var capture = TestThreadedRuntimeCapture{};
    var runtime = try DerivedRuntime.init(alloc, undefined, &capture, testThreadedRuntimeApply, testThreadedRuntimePersist, testThreadedRuntimeTruncate, null, null, null, null, null);
    defer runtime.deinit();
    const io = runtime.ioContext();
    runtime.scheduler = try Scheduler.create(alloc, io, 8);
    runtime.owns_scheduler = true;
    const worker = try alloc.create(Worker);
    worker.* = .{ .runtime = &runtime, .name = try alloc.dupe(u8, "failed"), .kind = .{ .name = "failed", .kind = .full_text }, .applied_sequence = 1, .persisted_sequence = 1, .target_sequence = 1, .last_error_name = "InvalidData" };
    try runtime.workers.append(alloc, worker);
    worker.future = try runtime.scheduler.?.registerClass(.derived, worker, workerStep);
    const deadline = platform_time.monotonicNs() + 5 * std.time.ns_per_s;
    while (true) {
        const stats = runtime.scheduler.?.snapshot();
        if (stats.dispatches == 1 and stats.active == 0) break;
        if (platform_time.monotonicNs() >= deadline) return error.TestUnexpectedResult;
        try io.sleep(.fromMilliseconds(1), .awake);
    }
    for (2..100) |sequence| runtime.notifySequence(sequence);
    try io.sleep(.fromMilliseconds(20), .awake);
    try std.testing.expectEqual(@as(u64, 1), runtime.scheduler.?.snapshot().dispatches);
    try std.testing.expectEqual(@as(u64, 0), capture.apply_calls.load(.monotonic));
}

test "issue1015 visibility truncation timeouts release claims and retain cleanup credit" {
    const alloc = std.testing.allocator;
    const Probe = struct {
        fail: bool = true,
        fn persist(_: *anyopaque, _: []const u8, _: u64, _: bool) !bool {
            return true;
        }
        fn truncate(ptr: *anyopaque, _: u64) !void {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            if (self.fail) return error.WouldBlock;
        }
    };
    for ([_]bool{ false, true }) |selected| {
        var manager = resource_manager_mod.ResourceManager.init(.{});
        defer manager.deinit(alloc);
        var probe = Probe{};
        var runtime = try DerivedRuntime.init(alloc, undefined, &probe, testThreadedRuntimeApply, Probe.persist, Probe.truncate, null, null, null, null, &manager);
        defer runtime.deinit();
        const worker = try alloc.create(Worker);
        worker.* = .{ .runtime = &runtime, .name = try alloc.dupe(u8, "cleanup"), .kind = .{ .name = "cleanup", .kind = .graph }, .applied_sequence = 10, .persisted_sequence = 10, .target_sequence = 10 };
        try runtime.workers.append(alloc, worker);
        try runtime.trackBacklogBytes(10, 64);
        const retained = runtime.backlog.retained_bytes;
        const wait = runtime_types.VisibilityWait{ .deadline_ns = platform_time.monotonicNs() + 2 * std.time.ns_per_ms };
        if (selected) {
            try std.testing.expectError(error.EnrichmentWaitTimeout, runtime.waitForIndexesWithVisibilityWait(10, &.{"cleanup"}, wait));
        } else {
            try std.testing.expectError(error.EnrichmentWaitTimeout, runtime.waitForAllWithVisibilityWait(10, wait));
        }
        try std.testing.expectEqual(@as(usize, 0), runtime.truncates_in_flight);
        try std.testing.expectEqual(@as(u64, 0), runtime.last_claimed_truncate_sequence);
        try std.testing.expectEqual(@as(u64, 0), runtime.last_truncated_sequence);
        try std.testing.expectEqual(retained, runtime.backlog.retained_bytes);
        probe.fail = false;
        if (selected) try runtime.waitForIndexes(10, &.{"cleanup"}) else try runtime.waitForAll(10);
        try std.testing.expectEqual(@as(u64, 10), runtime.last_truncated_sequence);
        try std.testing.expectEqual(@as(u64, 0), runtime.backlog.retained_bytes);
    }
}

test "issue1015 cleanup contention yields the shared derived scheduler slot" {
    const alloc = std.testing.allocator;
    const Probe = struct {
        allow_cleanup: std.atomic.Value(bool) = .init(false),
        cleanup_calls: std.atomic.Value(u32) = .init(0),
        peer_calls: std.atomic.Value(u32) = .init(0),
        fn persist(_: *anyopaque, _: []const u8, _: u64, _: bool) !bool {
            return true;
        }
        fn truncate(raw: *anyopaque, _: u64) !void {
            const self: *@This() = @ptrCast(@alignCast(raw));
            _ = self.cleanup_calls.fetchAdd(1, .monotonic);
            if (!self.allow_cleanup.load(.acquire)) return error.WouldBlock;
        }
        fn peer(self: *@This()) ?u64 {
            _ = self.peer_calls.fetchAdd(1, .monotonic);
            return null;
        }
    };
    var probe = Probe{};
    var runtime = try DerivedRuntime.init(alloc, undefined, &probe, testThreadedRuntimeApply, Probe.persist, Probe.truncate, null, null, null, null, null);
    const io = runtime.ioContext();
    // Four slots is the CPU-limited production scheduler minimum: one per class.
    const scheduler = try Scheduler.create(alloc, io, 4);
    runtime.scheduler = scheduler;
    runtime.owns_scheduler = true;
    defer runtime.deinit();
    defer probe.allow_cleanup.store(true, .release);
    const worker = try alloc.create(Worker);
    worker.* = .{ .runtime = &runtime, .name = try alloc.dupe(u8, "cleanup"), .kind = .{ .name = "cleanup", .kind = .graph }, .applied_sequence = 1, .persisted_sequence = 0, .target_sequence = 1 };
    try runtime.workers.append(alloc, worker);
    worker.future = try scheduler.registerClass(.derived, worker, workerStep);
    var deadline = platform_time.monotonicNs() + std.time.ns_per_s;
    while (probe.cleanup_calls.load(.monotonic) == 0) {
        if (platform_time.monotonicNs() >= deadline) return error.TestUnexpectedResult;
        try io.sleep(.fromMilliseconds(1), .awake);
    }
    var peer = try scheduler.registerClass(.derived, &probe, Probe.peer);
    defer peer.await(io);
    // Release cleanup before joining the peer, including on assertion failure.
    defer probe.allow_cleanup.store(true, .release);
    deadline = platform_time.monotonicNs() + 200 * std.time.ns_per_ms;
    while (probe.peer_calls.load(.monotonic) == 0 and platform_time.monotonicNs() < deadline) try io.sleep(.fromMilliseconds(1), .awake);
    std.debug.print("FRESH_CLEANUP_FAIRNESS cleanup_retries={d} peer_dispatches={d} active_derived={d}\n", .{ probe.cleanup_calls.load(.monotonic), probe.peer_calls.load(.monotonic), scheduler.snapshot().active_by_class[@backingInt(Scheduler.Class.derived)] });
    try std.testing.expect(probe.peer_calls.load(.monotonic) != 0);
    probe.allow_cleanup.store(true, .release);
    deadline = platform_time.monotonicNs() + 10 * std.time.ns_per_s;
    while (true) {
        runtime.mutex.lockUncancelable(io);
        const complete = runtime.last_truncated_sequence == 1;
        runtime.mutex.unlock(io);
        if (complete) break;
        if (platform_time.monotonicNs() >= deadline) return error.TestUnexpectedResult;
        try io.sleep(.fromMilliseconds(1), .awake);
    }
}

test "issue1015 failed registration can still pause and retire retained cleanup" {
    const alloc = std.testing.allocator;
    var capture = TestThreadedRuntimeCapture{};
    var runtime = try DerivedRuntime.init(alloc, undefined, &capture, testThreadedRuntimeApply, testThreadedRuntimePersist, testThreadedRuntimeTruncate, null, null, null, null, null);
    defer runtime.deinit();
    const worker = try alloc.create(Worker);
    worker.* = .{ .runtime = &runtime, .name = try alloc.dupe(u8, "failed"), .kind = .{ .name = "failed", .kind = .graph }, .applied_sequence = 1, .persisted_sequence = 1, .target_sequence = 1, .last_error_name = "InvalidData" };
    try runtime.workers.append(alloc, worker);
    try std.testing.expectError(RuntimeError.AsyncWorkerFailed, runtime.failIfUnhealthy());
    try std.testing.expect(try runtime.pauseWorker("failed"));
    try std.testing.expectEqual(@as(u64, 1), runtime.last_truncated_sequence);
    runtime.removeWorker("failed");
    try runtime.failIfUnhealthy();
}

test "issue1015 pause cleanup bypasses persistence for a durable worker" {
    const alloc = std.testing.allocator;
    const Probe = struct {
        persist_calls: usize = 0,
        fn persist(raw: *anyopaque, _: []const u8, _: u64, _: bool) !bool {
            const self: *@This() = @ptrCast(@alignCast(raw));
            self.persist_calls += 1;
            return error.WouldBlock;
        }
        fn truncate(_: *anyopaque, _: u64) !void {}
    };
    var probe = Probe{};
    var runtime = try DerivedRuntime.init(alloc, undefined, &probe, testThreadedRuntimeApply, Probe.persist, Probe.truncate, null, null, null, null, null);
    defer runtime.deinit();
    const worker = try alloc.create(Worker);
    worker.* = .{ .runtime = &runtime, .name = try alloc.dupe(u8, "healthy"), .kind = .{ .name = "healthy", .kind = .full_text }, .applied_sequence = 1, .persisted_sequence = 1, .target_sequence = 1 };
    try runtime.workers.append(alloc, worker);
    try std.testing.expect(try runtime.pauseWorker("healthy"));
    try std.testing.expectEqual(@as(usize, 0), probe.persist_calls);
    try std.testing.expectEqual(@as(u64, 1), runtime.last_truncated_sequence);
    std.debug.print("FRESH_PAUSE_HEALTH durable_sequence={d} persist_calls={d} worker_error={s} paused={any}\n", .{ worker.persisted_sequence, probe.persist_calls, worker.last_error_name orelse "none", worker.paused });
    // A transient race must not permanently disable an already-durable index.
    try runtime.failIfUnhealthy();
}

test "issue1015 foreground visibility cleanup preserves runtime backoff" {
    const alloc = std.testing.allocator;
    const Probe = struct {
        calls: usize = 0,
        fn persist(_: *anyopaque, _: []const u8, _: u64, _: bool) !bool {
            return true;
        }
        fn truncate(raw: *anyopaque, _: u64) !void {
            const self: *@This() = @ptrCast(@alignCast(raw));
            self.calls += 1;
            return error.WouldBlock;
        }
    };
    var probe = Probe{};
    var runtime = try DerivedRuntime.init(alloc, undefined, &probe, testThreadedRuntimeApply, Probe.persist, Probe.truncate, null, null, null, null, null);
    defer runtime.deinit();
    const worker = try alloc.create(Worker);
    worker.* = .{ .runtime = &runtime, .name = try alloc.dupe(u8, "cleanup"), .kind = .{ .name = "cleanup", .kind = .graph }, .applied_sequence = 1, .persisted_sequence = 1, .target_sequence = 1 };
    try runtime.workers.append(alloc, worker);
    runtime.truncate_retry_not_before_ns = platform_time.monotonicNs() + 10 * std.time.ns_per_s;
    for (0..20) |_| {
        try std.testing.expectError(error.EnrichmentWaitTimeout, runtime.waitForAllWithVisibilityWait(1, .{ .deadline_ns = platform_time.monotonicNs() + std.time.ns_per_ms }));
    }
    for (0..20) |_| {
        try std.testing.expectError(error.EnrichmentWaitTimeout, runtime.waitForIndexesWithVisibilityWait(1, &.{"cleanup"}, .{ .deadline_ns = platform_time.monotonicNs() + std.time.ns_per_ms }));
    }
    std.debug.print("FRESH_FOREGROUND_BACKOFF requests=20 truncate_calls={d} claim={d} complete={d}\n", .{ probe.calls, runtime.last_claimed_truncate_sequence, runtime.last_truncated_sequence });
    try std.testing.expectEqual(@as(usize, 0), probe.calls);
}

test "issue1015 durable idle cleanup skips persistence" {
    const a = std.testing.allocator;
    const Probe = struct {
        persist_calls: usize = 0,
        truncate_calls: usize = 0,
        fn persist(raw: *anyopaque, _: []const u8, _: u64, _: bool) !bool {
            const self: *@This() = @ptrCast(@alignCast(raw));
            self.persist_calls += 1;
            return true;
        }
        fn truncate(raw: *anyopaque, _: u64) !void {
            const self: *@This() = @ptrCast(@alignCast(raw));
            self.truncate_calls += 1;
            if (self.truncate_calls < 4) return error.WouldBlock;
        }
    };
    var probe = Probe{};
    var runtime = try DerivedRuntime.init(a, undefined, &probe, testThreadedRuntimeApply, Probe.persist, Probe.truncate, null, null, null, null, null);
    defer runtime.deinit();
    const worker = try a.create(Worker);
    worker.* = .{ .runtime = &runtime, .name = try a.dupe(u8, "durable"), .kind = .{ .name = "durable", .kind = .graph }, .applied_sequence = 1, .persisted_sequence = 1, .target_sequence = 1 };
    try runtime.workers.append(a, worker);
    // Simulate dispatches after each shared cleanup deadline expires.
    for (0..4) |dispatch| {
        runtime.truncate_retry_not_before_ns = 0;
        workerMain(worker);
        if (dispatch < 3) {
            runtime.truncate_retry_not_before_ns = platform_time.monotonicNs() + 10 * std.time.ns_per_s;
            workerMain(worker);
            try std.testing.expectEqual(dispatch + 1, probe.truncate_calls);
        }
    }
    try runtime.failIfUnhealthy();
    try std.testing.expectEqual(@as(usize, 4), probe.truncate_calls);
    try std.testing.expectEqual(@as(u64, 1), runtime.last_truncated_sequence);
    std.debug.print("LITE_IDLE_CLEANUP durable_sequence=1 dispatches=4 persist_calls={d} truncate_calls={d}\n", .{ probe.persist_calls, probe.truncate_calls });
    try std.testing.expectEqual(@as(usize, 0), probe.persist_calls);
}

test "issue1015 idle cleanup persists a new watermark once" {
    const a = std.testing.allocator;
    const Probe = struct {
        persist_calls: usize = 0,
        truncate_calls: usize = 0,
        fn persist(raw: *anyopaque, _: []const u8, _: u64, _: bool) !bool {
            const self: *@This() = @ptrCast(@alignCast(raw));
            self.persist_calls += 1;
            return true;
        }
        fn truncate(raw: *anyopaque, _: u64) !void {
            const self: *@This() = @ptrCast(@alignCast(raw));
            self.truncate_calls += 1;
            if (self.truncate_calls < 4) return error.WouldBlock;
        }
    };
    var probe = Probe{};
    var runtime = try DerivedRuntime.init(a, undefined, &probe, testThreadedRuntimeApply, Probe.persist, Probe.truncate, null, null, null, null, null);
    defer runtime.deinit();
    const worker = try a.create(Worker);
    worker.* = .{ .runtime = &runtime, .name = try a.dupe(u8, "durable"), .kind = .{ .name = "durable", .kind = .graph }, .applied_sequence = 1, .persisted_sequence = 0, .target_sequence = 1 };
    try runtime.workers.append(a, worker);
    // Simulate dispatches after each shared cleanup deadline expires.
    for (0..4) |dispatch| {
        runtime.truncate_retry_not_before_ns = 0;
        workerMain(worker);
        if (dispatch < 3) {
            runtime.truncate_retry_not_before_ns = platform_time.monotonicNs() + 10 * std.time.ns_per_s;
            workerMain(worker);
            try std.testing.expectEqual(dispatch + 1, probe.truncate_calls);
        }
    }
    try runtime.failIfUnhealthy();
    try std.testing.expectEqual(@as(usize, 4), probe.truncate_calls);
    try std.testing.expectEqual(@as(u64, 1), runtime.last_truncated_sequence);
    std.debug.print("LITE_IDLE_CLEANUP durable_sequence=1 dispatches=4 persist_calls={d} truncate_calls={d}\n", .{ probe.persist_calls, probe.truncate_calls });
    try std.testing.expectEqual(@as(usize, 1), probe.persist_calls);
}

test "controlled derived visibility callbacks run outside worker mutex and preserve original rejection" {
    const alloc = std.testing.allocator;
    const Probe = struct {
        runtime: *DerivedRuntime,
        calls: usize = 0,
        mutex_was_free: bool = true,
        cause: anyerror,
        fn check(raw: *const anyopaque) !void {
            const self: *@This() = @ptrCast(@alignCast(@constCast(raw)));
            self.calls += 1;
            if (!self.runtime.mutex.tryLock()) {
                self.mutex_was_free = false;
                return error.TestUnexpectedResult;
            }
            self.runtime.mutex.unlock(self.runtime.ioContext());
            if (self.calls >= 2) return self.cause;
        }
    };
    for ([_]bool{ false, true }) |selected| for ([_]anyerror{ error.Canceled, error.DeadlineExceeded, error.SchedulerClosed, error.CatalogGenerationChanged }) |cause| {
        var capture: TestThreadedRuntimeCapture = .{};
        var runtime = try DerivedRuntime.init(alloc, undefined, &capture, testThreadedRuntimeApply, testThreadedRuntimePersist, testThreadedRuntimeTruncate, null, null, null, null, null);
        defer runtime.deinit();
        const worker = try alloc.create(Worker);
        worker.* = .{ .runtime = &runtime, .name = try alloc.dupe(u8, "held"), .kind = .{ .name = "held", .kind = .full_text }, .applied_sequence = 0, .persisted_sequence = 0, .target_sequence = 0, .paused = true };
        try runtime.workers.append(alloc, worker);
        var probe: Probe = .{ .runtime = &runtime, .cause = cause };
        const wait: runtime_types.VisibilityWait = .{ .cancellation = .{ .ptr = &probe, .check_fn = Probe.check } };
        if (selected)
            try std.testing.expectError(cause, runtime.waitForIndexesWithVisibilityWait(1, &.{"held"}, wait))
        else
            try std.testing.expectError(cause, runtime.waitForAllWithVisibilityWait(1, wait));
        try std.testing.expect(probe.mutex_was_free);
        try std.testing.expectEqual(@as(usize, 2), probe.calls);
        try std.testing.expect(runtime.mutex.tryLock());
        runtime.mutex.unlock(runtime.ioContext());
        try std.testing.expect(worker.paused and !worker.dispatch_active and worker.future == null);
    };
}

test "controlled derived visibility ready completion rechecks authority outside worker mutex" {
    const alloc = std.testing.allocator;
    const Probe = struct {
        runtime: *DerivedRuntime,
        calls: usize = 0,
        fn check(raw: *const anyopaque) !void {
            const self: *@This() = @ptrCast(@alignCast(@constCast(raw)));
            self.calls += 1;
            if (!self.runtime.mutex.tryLock()) return error.TestUnexpectedResult;
            self.runtime.mutex.unlock(self.runtime.ioContext());
            if (self.calls == 2) return error.DeadlineExceeded;
        }
    };
    for ([_]bool{ false, true }) |selected| {
        var capture: TestThreadedRuntimeCapture = .{};
        var runtime = try DerivedRuntime.init(alloc, undefined, &capture, testThreadedRuntimeApply, testThreadedRuntimePersist, testThreadedRuntimeTruncate, null, null, null, null, null);
        defer runtime.deinit();
        var probe: Probe = .{ .runtime = &runtime };
        const wait: runtime_types.VisibilityWait = .{ .cancellation = .{ .ptr = &probe, .check_fn = Probe.check } };
        if (selected)
            try std.testing.expectError(error.DeadlineExceeded, runtime.waitForIndexesWithVisibilityWait(1, &.{"ready"}, wait))
        else
            try std.testing.expectError(error.DeadlineExceeded, runtime.waitForAllWithVisibilityWait(1, wait));
        try std.testing.expectEqual(@as(usize, 2), probe.calls);
        try std.testing.expect(runtime.mutex.tryLock());
        runtime.mutex.unlock(runtime.ioContext());
        try std.testing.expectEqual(@as(usize, 0), runtime.workers.items.len);
    }
}

test "controlled derived visibility polling lends CPU and fences original owner reentry" {
    const native = @import("../../../common/runnable_scheduler.zig");
    const Probe = struct {
        runtime: *DerivedRuntime,
        execution: *native.Execution,
        scheduler: *native.Scheduler,
        cause: ?anyerror,
        begins: usize = 0,
        ends: usize = 0,
        fn authority(raw: *const anyopaque) !void {
            const self: *const @This() = @ptrCast(@alignCast(raw));
            try self.execution.checkLifetime();
        }
        fn fence(raw: *anyopaque) !void {
            const self: *@This() = @ptrCast(@alignCast(raw));
            if (self.cause) |cause| return cause;
        }
        fn begin(raw: *anyopaque) !void {
            const self: *@This() = @ptrCast(@alignCast(raw));
            self.begins += 1;
            try std.testing.expect(self.runtime.mutex.tryLock());
            self.runtime.mutex.unlock(self.runtime.ioContext());
            try self.execution.park(.resource_blocked);
            try std.testing.expectEqual(@as(usize, 0), self.scheduler.stats().runnable_slots);
            try std.testing.expectEqual(@as(u64, 23), self.scheduler.stats().retained_bytes);
            var peer: native.Execution = .{};
            try peer.start(self.scheduler, .{ .io = std.testing.io, .retained_bytes = 17, .allow_wait = false }, .background);
            defer peer.finish();
            var health: native.Execution = .{};
            try health.start(self.scheduler, .{ .io = std.testing.io, .retained_bytes = 5, .allow_wait = false }, .control);
            defer health.finish();
            try std.testing.expectEqual(@as(usize, 2), self.scheduler.stats().runnable_slots);
            try std.testing.expectEqual(@as(usize, 3), self.scheduler.stats().operations);
            try std.testing.expectEqual(@as(u64, 45), self.scheduler.stats().retained_bytes);
            // Simulated worker advancement; no physical backend apply is claimed.
            try std.testing.expect(self.runtime.mutex.tryLock());
            for (self.runtime.workers.items) |worker| {
                worker.paused = false;
                worker.applied_sequence = 1;
                worker.persisted_sequence = 1;
            }
            self.runtime.mutex.unlock(self.runtime.ioContext());
        }
        fn end(raw: *anyopaque) !void {
            const self: *@This() = @ptrCast(@alignCast(raw));
            self.ends += 1;
            try std.testing.expect(self.runtime.mutex.tryLock());
            self.runtime.mutex.unlock(self.runtime.ioContext());
            try self.execution.enterWithCheck(.query, .{ .context = self, .check = fence });
        }
    };
    const alloc = std.testing.allocator;
    for ([_]bool{ false, true }) |selected| for ([_]?anyerror{ null, error.Canceled, error.DeadlineExceeded, error.SchedulerClosed, error.CatalogGenerationChanged }) |cause| {
        var scheduler = try native.Scheduler.init(.{ .slots = 2 });
        defer scheduler.close();
        var execution: native.Execution = .{};
        try execution.start(&scheduler, .{ .io = std.testing.io, .retained_bytes = 23 }, .query);
        defer execution.finish();
        var capture: TestThreadedRuntimeCapture = .{};
        var runtime = try DerivedRuntime.init(alloc, undefined, &capture, testThreadedRuntimeApply, testThreadedRuntimePersist, testThreadedRuntimeTruncate, null, null, null, null, null);
        defer runtime.deinit();
        const worker = try alloc.create(Worker);
        worker.* = .{ .runtime = &runtime, .name = try alloc.dupe(u8, "held"), .kind = .{ .name = "held", .kind = .full_text }, .applied_sequence = 0, .persisted_sequence = 0, .target_sequence = 0, .paused = true };
        try runtime.workers.append(alloc, worker);
        var probe: Probe = .{ .runtime = &runtime, .execution = &execution, .scheduler = &scheduler, .cause = cause };
        const controls: @import("../apply_rw_lock.zig").ApplyRwLock.Acquisition = .{
            .lifetime = .{ .context = &probe, .check_fn = Probe.authority },
            .execution_wait = .{ .ptr = &probe, .begin = Probe.begin, .end = Probe.end },
        };
        const wait: runtime_types.VisibilityWait = .{ .execution_wait = @import("../mutation_admission.zig").committedVisibilityBoundary(controls, .{}) };
        const result = if (selected) runtime.waitForIndexesWithVisibilityWait(1, &.{"held"}, wait) else runtime.waitForAllWithVisibilityWait(1, wait);
        if (cause) |err| {
            try std.testing.expectError(err, result);
            try std.testing.expectEqual(@as(usize, 0), scheduler.stats().runnable_slots);
        } else {
            try result;
            try std.testing.expectEqual(@as(usize, 1), scheduler.stats().runnable_slots);
        }
        try std.testing.expectEqual(@as(usize, 1), probe.begins);
        try std.testing.expectEqual(@as(usize, 1), probe.ends);
        try std.testing.expectEqual(@as(usize, 1), scheduler.stats().operations);
        try std.testing.expectEqual(@as(u64, 23), scheduler.stats().retained_bytes);
        try std.testing.expect(runtime.mutex.tryLock());
        runtime.mutex.unlock(runtime.ioContext());
        execution.finish();
        try std.testing.expectEqual(@as(usize, 0), scheduler.stats().operations);
        try std.testing.expectEqual(@as(u64, 0), scheduler.stats().retained_bytes);
    };
}

test "controlled derived quiescence retains callbacks foreground persistence and detached removals across original failures" {
    const Case = enum { success, begin_failure, end_failure, lifetime_in_wait };
    const Mode = enum { scheduled, foreground_persist, detached_remove };
    for (std.enums.values(Case)) |case| for (std.enums.values(Mode)) |mode| {
        const F = struct {
            runtime: *DerivedRuntime = undefined,
            mode: Mode,
            io: Io = undefined,
            entered: Io.Event = .unset,
            release: Io.Event = .unset,
            fn block(self: *@This()) void {
                std.debug.assert(self.runtime.mutex.tryLock());
                self.runtime.mutex.unlock(self.io);
                self.entered.set(self.io);
                self.release.waitUncancelable(self.io);
            }
            fn step(self: *@This()) ?u64 {
                self.block();
                return null;
            }
            fn apply(_: *anyopaque, _: derived_types.DerivedBatch, _: index_manager_mod.ManagedIndexRef, _: CatchUpSessionToken) !bool {
                return true;
            }
            fn persist(raw: *anyopaque, _: []const u8, _: u64, _: bool) !bool {
                const self: *@This() = @ptrCast(@alignCast(raw));
                if (self.mode == .foreground_persist) self.block();
                return true;
            }
            fn truncate(_: *anyopaque, _: u64) !void {}
            fn finish(raw: *anyopaque, _: index_manager_mod.ManagedIndexRef, _: CatchUpSessionToken, _: u64, _: bool) !CatchUpFinishResult {
                const self: *@This() = @ptrCast(@alignCast(raw));
                if (self.mode == .detached_remove) self.block();
                return .{};
            }
            fn direct(self: *@This()) void {
                if (self.mode == .detached_remove) self.runtime.removeWorker("worker") else self.runtime.waitForAll(1) catch |cause| {
                    if (cause != error.WorkerStopping) @panic("unexpected visibility failure");
                };
            }
        };
        var fixture: F = .{ .mode = mode };
        var runtime = try DerivedRuntime.init(std.testing.allocator, undefined, &fixture, F.apply, F.persist, F.truncate, null, F.finish, null, null, null);
        defer runtime.deinit();
        const io = runtime.ioContext();
        fixture.runtime = &runtime;
        fixture.io = io;
        const worker = try std.testing.allocator.create(Worker);
        const name = try std.testing.allocator.dupe(u8, "worker");
        worker.* = .{ .runtime = &runtime, .name = name, .kind = .{ .name = name, .kind = .full_text }, .applied_sequence = 1, .persisted_sequence = 0, .target_sequence = 1, .catch_up_open = mode == .detached_remove, .catch_up_token = .{ .value = 7 } };
        try runtime.workers.append(std.testing.allocator, worker);
        var direct: ?Io.Future(void) = null;
        defer {
            fixture.release.set(io);
            if (direct) |*future| future.await(io);
        }
        if (mode == .scheduled) {
            runtime.scheduler = try Scheduler.create(std.testing.allocator, io, 4);
            runtime.owns_scheduler = true;
            worker.future = try runtime.scheduler.?.registerClass(.derived, &fixture, F.step);
        } else direct = try io.concurrent(F.direct, .{&fixture});
        try fixture.entered.wait(io);
        const Control = struct {
            fixture: *F,
            case: Case,
            begins: usize = 0,
            ends: usize = 0,
            fn check(raw: *anyopaque) !void {
                const self: *@This() = @ptrCast(@alignCast(raw));
                if (self.case == .lifetime_in_wait and self.begins != 0) return error.DeadlineExceeded;
            }
            fn deadline(_: *anyopaque) !?u64 {
                return 1000;
            }
            fn begin(raw: *anyopaque) !void {
                const self: *@This() = @ptrCast(@alignCast(raw));
                self.begins += 1;
                if (self.case == .begin_failure) return error.NotFound;
                if (self.case != .lifetime_in_wait) self.fixture.release.set(self.fixture.io);
            }
            fn end(raw: *anyopaque) !void {
                const self: *@This() = @ptrCast(@alignCast(raw));
                self.ends += 1;
                self.fixture.release.set(self.fixture.io);
                if (self.case == .end_failure) return error.Canceled;
            }
        };
        const admission = @import("../../../common/read_admission.zig");
        var control: Control = .{ .fixture = &fixture, .case = case };
        var payload: admission.Payload = .{ .ptr = &control, .vtable = &.{ .checkpoint = Control.check, .check_lifetime = Control.check, .deadline_ns = Control.deadline }, .execution_wait = .{ .ptr = &control, .begin = Control.begin, .end = Control.end } };
        const result = runtime.quiesceWithAdmission(.init(&payload));
        if (case == .success) try result else {
            try std.testing.expectError(switch (case) {
                .begin_failure => error.NotFound,
                .end_failure => error.Canceled,
                .lifetime_in_wait => error.DeadlineExceeded,
                .success => unreachable,
            }, result);
            try std.testing.expect(runtime.closing.load(.acquire));
            if (mode == .scheduled) try std.testing.expect(worker.future != null);
            fixture.release.set(io);
            control.case = .success;
            try runtime.quiesceWithAdmission(.init(&payload));
        }
        try std.testing.expectEqual(@as(u32, 0), runtime.active_borrows.load(.acquire));
        try std.testing.expectError(error.WorkerStopping, runtime.addWorker("late", .{ .name = "late", .kind = .full_text }, 0));
        try std.testing.expectError(error.WorkerStopping, runtime.waitForAll(1));
        if (mode == .detached_remove) try std.testing.expectEqual(@as(usize, 0), runtime.workers.items.len) else try std.testing.expect(worker.future == null and worker.joined.isSet());
        try std.testing.expectEqual(control.begins - @intFromBool(case == .begin_failure), control.ends);
    };
}

test "controlled derived quiescence preserves idle finalization until physical deinit" {
    const F = struct {
        finishes: usize = 0,
        persists: usize = 0,
        fn apply(_: *anyopaque, _: derived_types.DerivedBatch, _: index_manager_mod.ManagedIndexRef, _: CatchUpSessionToken) !bool {
            return true;
        }
        fn persist(raw: *anyopaque, _: []const u8, _: u64, _: bool) !bool {
            const self: *@This() = @ptrCast(@alignCast(raw));
            self.persists += 1;
            return true;
        }
        fn truncate(_: *anyopaque, _: u64) !void {}
        fn finish(raw: *anyopaque, _: index_manager_mod.ManagedIndexRef, token: CatchUpSessionToken, _: u64, _: bool) !CatchUpFinishResult {
            const self: *@This() = @ptrCast(@alignCast(raw));
            std.debug.assert(token.value == 7);
            self.finishes += 1;
            return .{};
        }
        fn check(_: *anyopaque) !void {}
        fn deadline(_: *anyopaque) !?u64 {
            return 1000;
        }
    };
    var f: F = .{};
    var runtime = try DerivedRuntime.init(std.testing.allocator, undefined, &f, F.apply, F.persist, F.truncate, null, F.finish, null, null, null);
    var live = true;
    defer if (live) runtime.deinit();
    const worker = try std.testing.allocator.create(Worker);
    const name = try std.testing.allocator.dupe(u8, "idle");
    worker.* = .{ .runtime = &runtime, .name = name, .kind = .{ .name = name, .kind = .full_text }, .applied_sequence = 1, .persisted_sequence = 0, .target_sequence = 1, .catch_up_open = true, .catch_up_token = .{ .value = 7 } };
    try runtime.workers.append(std.testing.allocator, worker);
    const admission = @import("../../../common/read_admission.zig");
    var payload: admission.Payload = .{ .ptr = &f, .vtable = &.{ .checkpoint = F.check, .check_lifetime = F.check, .deadline_ns = F.deadline }, .execution_wait = .{ .ptr = &f, .begin = F.check, .end = F.check } };
    try runtime.quiesceWithAdmission(.init(&payload));
    try std.testing.expectEqual(@as(usize, 0), f.finishes);
    try std.testing.expectEqual(@as(usize, 0), f.persists);
    try std.testing.expect(worker.catch_up_open and worker.catch_up_token.value == 7);
    runtime.deinit();
    live = false;
    try std.testing.expectEqual(@as(usize, 1), f.finishes);
    try std.testing.expectEqual(@as(usize, 1), f.persists);
}

const FinalizationProbe = struct {
    runtime: *DerivedRuntime = undefined,
    hold: enum { none, finish, persist } = .none,
    held: bool = false,
    finishes: usize = 0,
    persists: usize = 0,
    durable_finish: bool = false,
    fail_finish: bool = false,
    defer_persist_once: bool = false,
    controlled_mode: enum { success, reject_before, fail_after, invalid_success } = .success,
    controlled_calls: usize = 0,
    block_finish: bool = false,
    finish_entered: Io.Event = .unset,
    finish_release: Io.Event = .unset,
    fn release(self: *@This()) void {
        if (!self.held) return;
        self.runtime.mutex.unlock(self.runtime.ioContext());
        self.held = false;
    }
    fn takeGuard(self: *@This()) void {
        // User callbacks run outside the metadata guard. Keep a peer guard
        // held on return to force the following publication/reentry wait.
        std.debug.assert(self.runtime.mutex.tryLock());
        self.held = true;
    }
    fn apply(_: *anyopaque, _: derived_types.DerivedBatch, _: index_manager_mod.ManagedIndexRef, _: CatchUpSessionToken) !bool {
        return true;
    }
    fn truncate(_: *anyopaque, _: u64) !void {}
    fn finish(raw: *anyopaque, _: index_manager_mod.ManagedIndexRef, token: CatchUpSessionToken, _: u64, _: bool) !CatchUpFinishResult {
        const self: *@This() = @ptrCast(@alignCast(raw));
        std.debug.assert(token.value == 7);
        self.finishes += 1;
        if (self.block_finish) {
            self.finish_entered.set(self.runtime.ioContext());
            self.finish_release.waitUncancelable(self.runtime.ioContext());
        }
        if (self.hold == .finish) self.takeGuard();
        if (self.fail_finish) return error.OutOfMemory;
        return .{ .applied_sequence_persisted = self.durable_finish };
    }
    fn controlledFinish(raw: *anyopaque, _: index_manager_mod.ManagedIndexRef, token: CatchUpSessionToken, _: u64, original: @import("../../../common/read_admission.zig").Port, attempt: *runtime_types.FinishAttempt) !CatchUpFinishResult {
        const self: *@This() = @ptrCast(@alignCast(raw));
        std.debug.assert(token.value == 7);
        self.controlled_calls += 1;
        try original.checkpoint();
        if (self.controlled_mode == .reject_before) return error.NotFound;
        if (self.controlled_mode != .invalid_success) attempt.consumed = true;
        if (self.controlled_mode == .fail_after) return error.OutOfMemory;
        return .{ .applied_sequence_persisted = true };
    }
    fn persist(raw: *anyopaque, _: []const u8, _: u64, force: bool) !bool {
        const self: *@This() = @ptrCast(@alignCast(raw));
        std.debug.assert(force);
        self.persists += 1;
        if (self.hold == .persist) self.takeGuard();
        if (self.defer_persist_once and self.persists == 1) return false;
        return true;
    }
    fn install(self: *@This(), runtime: *DerivedRuntime) !*Worker {
        self.runtime = runtime;
        const alloc = std.testing.allocator;
        const worker = try alloc.create(Worker);
        errdefer alloc.destroy(worker);
        const name = try alloc.dupe(u8, "finalize");
        errdefer alloc.free(name);
        worker.* = .{ .runtime = runtime, .name = name, .kind = .{ .name = name, .kind = .dense_vector }, .applied_sequence = 1, .persisted_sequence = 0, .target_sequence = 1, .catch_up_open = true, .catch_up_token = .{ .value = 7 } };
        try runtime.workers.append(alloc, worker);
        return worker;
    }
};

test "controlled derived finalization retains completed finish and persistence across original guard failures" {
    const admission = @import("../../../common/read_admission.zig");
    const Case = enum { success, begin_failure, end_failure, lifetime_in_wait };
    for ([_]@TypeOf(@as(FinalizationProbe, .{}).hold){ .finish, .persist }) |stage| {
        for (std.enums.values(Case)) |case| {
            var probe: FinalizationProbe = .{ .hold = stage };
            var runtime = try DerivedRuntime.init(std.testing.allocator, undefined, &probe, FinalizationProbe.apply, FinalizationProbe.persist, FinalizationProbe.truncate, null, FinalizationProbe.finish, null, null, null);
            var live = true;
            defer if (live) {
                probe.release();
                runtime.deinit();
            };
            const worker = try probe.install(&runtime);
            const Control = struct {
                probe: *FinalizationProbe,
                case: Case,
                begins: usize = 0,
                ends: usize = 0,
                fn check(raw: *anyopaque) !void {
                    const self: *@This() = @ptrCast(@alignCast(raw));
                    if (self.case == .lifetime_in_wait and self.begins != 0) return error.DeadlineExceeded;
                }
                fn deadline(_: *anyopaque) !?u64 {
                    return 1000;
                }
                fn begin(raw: *anyopaque) !void {
                    const self: *@This() = @ptrCast(@alignCast(raw));
                    self.begins += 1;
                    if (self.case == .begin_failure) return error.NotFound;
                    if (self.case != .lifetime_in_wait) self.probe.release();
                }
                fn end(raw: *anyopaque) !void {
                    const self: *@This() = @ptrCast(@alignCast(raw));
                    self.ends += 1;
                    self.probe.release();
                    if (self.case == .end_failure) return error.Canceled;
                }
            };
            var control: Control = .{ .probe = &probe, .case = case };
            var payload: admission.Payload = .{ .ptr = &control, .vtable = &.{ .checkpoint = Control.check, .check_lifetime = Control.check, .deadline_ns = Control.deadline }, .execution_wait = .{ .ptr = &control, .begin = Control.begin, .end = Control.end } };
            const result = runtime.finalizeWithAdmission(.init(&payload));
            if (case == .success) try result else {
                try std.testing.expectError(switch (case) {
                    .begin_failure => error.NotFound,
                    .end_failure => error.Canceled,
                    .lifetime_in_wait => error.DeadlineExceeded,
                    .success => unreachable,
                }, result);
                try std.testing.expect(worker.finalization.finish_done and !worker.finalization.done);
                try std.testing.expectEqual(stage == .persist, worker.finalization.durable);
                try std.testing.expectEqual(@as(u64, 0), worker.persisted_sequence);
                try std.testing.expect(!runtime.finalizer_active.load(.acquire));
                probe.release();
                control.case = .success;
                try runtime.finalizeWithAdmission(.init(&payload));
            }
            try std.testing.expect(worker.finalization.done);
            try std.testing.expectEqual(@as(u64, 1), worker.persisted_sequence);
            try runtime.finalizeWithAdmission(.init(&payload));
            try std.testing.expectEqual(@as(usize, 1), probe.finishes);
            try std.testing.expectEqual(@as(usize, 1), probe.persists);
            try std.testing.expectEqual(control.begins - @intFromBool(case == .begin_failure), control.ends);
            runtime.deinit();
            live = false;
            try std.testing.expectEqual(@as(usize, 1), probe.finishes);
            try std.testing.expectEqual(@as(usize, 1), probe.persists);
        }
    }
}

test "controlled derived finalization preserves authoritative durability and failed or deferred publication" {
    const admission = @import("../../../common/read_admission.zig");
    const Case = enum { durable, finish_failure, deferred_persist };
    for (std.enums.values(Case)) |case| {
        var probe: FinalizationProbe = .{ .durable_finish = case == .durable, .fail_finish = case == .finish_failure, .defer_persist_once = case == .deferred_persist };
        var runtime = try DerivedRuntime.init(std.testing.allocator, undefined, &probe, FinalizationProbe.apply, FinalizationProbe.persist, FinalizationProbe.truncate, null, FinalizationProbe.finish, null, null, null);
        var live = true;
        defer if (live) runtime.deinit();
        const worker = try probe.install(&runtime);
        const Control = struct {
            fn check(_: *anyopaque) !void {}
            fn deadline(_: *anyopaque) !?u64 {
                return 1000;
            }
        };
        var payload: admission.Payload = .{ .ptr = &probe, .vtable = &.{ .checkpoint = Control.check, .check_lifetime = Control.check, .deadline_ns = Control.deadline }, .execution_wait = .{ .ptr = &probe, .begin = Control.check, .end = Control.check } };
        const result = runtime.finalizeWithAdmission(.init(&payload));
        switch (case) {
            .durable => {
                try result;
                try std.testing.expectEqual(@as(usize, 0), probe.persists);
            },
            .finish_failure => {
                try std.testing.expectError(error.OutOfMemory, result);
                try std.testing.expectError(error.OutOfMemory, runtime.finalizeWithAdmission(.init(&payload)));
                try std.testing.expect(worker.catch_up_close_failed);
                try std.testing.expectEqual(@as(u64, 0), worker.persisted_sequence);
                try std.testing.expectEqual(@as(usize, 0), probe.persists);
            },
            .deferred_persist => {
                try std.testing.expectError(RuntimeError.AsyncWorkerFailed, result);
                try std.testing.expectEqual(@as(u64, 0), worker.persisted_sequence);
                try runtime.finalizeWithAdmission(.init(&payload));
                try std.testing.expectEqual(@as(usize, 2), probe.persists);
            },
        }
        try std.testing.expectEqual(@as(usize, 1), probe.finishes);
        if (case != .finish_failure) try std.testing.expectEqual(@as(u64, 1), worker.persisted_sequence);
        const persists = probe.persists;
        runtime.deinit();
        live = false;
        try std.testing.expectEqual(@as(usize, 1), probe.finishes);
        try std.testing.expectEqual(persists, probe.persists);
    }
}

test "controlled derived finalization serializes competing closers without lending around callbacks" {
    const admission = @import("../../../common/read_admission.zig");
    var probe: FinalizationProbe = .{ .block_finish = true };
    var runtime = try DerivedRuntime.init(std.testing.allocator, undefined, &probe, FinalizationProbe.apply, FinalizationProbe.persist, FinalizationProbe.truncate, null, FinalizationProbe.finish, null, null, null);
    defer runtime.deinit();
    _ = try probe.install(&runtime);
    const Control = struct {
        reject_begin: bool,
        begins: usize = 0,
        ends: usize = 0,
        fn check(_: *anyopaque) !void {}
        fn deadline(_: *anyopaque) !?u64 {
            return 1000;
        }
        fn begin(raw: *anyopaque) !void {
            const self: *@This() = @ptrCast(@alignCast(raw));
            self.begins += 1;
            if (self.reject_begin) return error.NotFound;
        }
        fn end(raw: *anyopaque) !void {
            const self: *@This() = @ptrCast(@alignCast(raw));
            self.ends += 1;
        }
        fn payload(self: *@This()) admission.Payload {
            return .{ .ptr = self, .vtable = &.{ .checkpoint = check, .check_lifetime = check, .deadline_ns = deadline }, .execution_wait = .{ .ptr = self, .begin = begin, .end = end } };
        }
        fn run(value: *DerivedRuntime, original: admission.Port) !void {
            try value.finalizeWithAdmission(original);
        }
    };
    var first: Control = .{ .reject_begin = false };
    var first_payload = first.payload();
    const io = runtime.ioContext();
    var future = try io.concurrent(Control.run, .{ &runtime, admission.Port.init(&first_payload) });
    defer {
        probe.finish_release.set(io);
        _ = future.await(io) catch {};
    }
    try probe.finish_entered.waitTimeout(io, .{ .duration = .{ .raw = .fromSeconds(5), .clock = .awake } });
    try std.testing.expect(runtime.mutex.tryLock());
    runtime.mutex.unlock(io);
    var second: Control = .{ .reject_begin = true };
    var second_payload = second.payload();
    try std.testing.expectError(error.NotFound, runtime.finalizeWithAdmission(.init(&second_payload)));
    try std.testing.expect(runtime.finalizer_active.load(.acquire));
    try std.testing.expectEqual(@as(usize, 1), second.begins);
    try std.testing.expectEqual(@as(usize, 0), second.ends);
    probe.finish_release.set(io);
    try future.await(io);
    try std.testing.expectEqual(@as(usize, 0), first.begins);
    try std.testing.expectEqual(@as(usize, 0), first.ends);
    second.reject_begin = false;
    try runtime.finalizeWithAdmission(.init(&second_payload));
    try std.testing.expectEqual(@as(usize, 1), probe.finishes);
    try std.testing.expectEqual(@as(usize, 1), probe.persists);
}

test "controlled dense finalization callback consumption separates retry from committed failure" {
    const admission = @import("../../../common/read_admission.zig");
    for (std.enums.values(@FieldType(FinalizationProbe, "controlled_mode"))) |mode| {
        var probe: FinalizationProbe = .{ .controlled_mode = mode };
        var runtime = try DerivedRuntime.init(std.testing.allocator, undefined, &probe, FinalizationProbe.apply, FinalizationProbe.persist, FinalizationProbe.truncate, null, FinalizationProbe.finish, null, null, null);
        var live = true;
        defer if (live) runtime.deinit();
        const worker = try probe.install(&runtime);
        runtime.finish_with_admission = FinalizationProbe.controlledFinish;
        const Control = struct {
            fn check(raw: *anyopaque) !void {
                const self: *FinalizationProbe = @ptrCast(@alignCast(raw));
                if (self.controlled_calls != 0 and self.controlled_mode == .reject_before) return error.NotFound;
            }
            fn deadline(_: *anyopaque) !?u64 {
                return 1000;
            }
        };
        var payload: admission.Payload = .{ .ptr = &probe, .vtable = &.{ .checkpoint = Control.check, .check_lifetime = Control.check, .deadline_ns = Control.deadline }, .execution_wait = .{ .ptr = &probe, .begin = Control.check, .end = Control.check } };
        const result = runtime.finalizeWithAdmission(.init(&payload));
        switch (mode) {
            .success => try result,
            .reject_before => {
                try std.testing.expectError(error.NotFound, result);
                try std.testing.expect(!worker.finalization.finish_done and worker.finalization.finish_failure == null);
                try std.testing.expect(worker.finalization.token.value == 7 and worker.finalization.had_session);
                probe.controlled_mode = .success;
                try runtime.finalizeWithAdmission(.init(&payload));
            },
            .fail_after, .invalid_success => {
                const cause = if (mode == .fail_after) error.OutOfMemory else RuntimeError.AsyncWorkerFailed;
                try std.testing.expectError(cause, result);
                try std.testing.expect(worker.finalization.finish_done and worker.finalization.finish_failure != null);
                try std.testing.expectError(cause, runtime.finalizeWithAdmission(.init(&payload)));
                try std.testing.expectEqual(@as(u64, 0), worker.persisted_sequence);
            },
        }
        if (mode == .success or mode == .reject_before) try std.testing.expectEqual(@as(u64, 1), worker.persisted_sequence);
        try std.testing.expectEqual(@as(usize, if (mode == .reject_before) 2 else 1), probe.controlled_calls);
        try std.testing.expectEqual(@as(usize, 0), probe.finishes);
        try std.testing.expectEqual(@as(usize, 0), probe.persists);
        runtime.deinit();
        live = false;
        try std.testing.expectEqual(@as(usize, 0), probe.finishes);
        try std.testing.expectEqual(@as(usize, 0), probe.persists);
    }
}
