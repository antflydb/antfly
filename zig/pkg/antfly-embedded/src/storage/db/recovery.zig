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
const Allocator = std.mem.Allocator;
const db_mod = @import("db.zig");
const DB = db_mod.DB;

/// Recovery state owned by `DB`: transaction recovery, publication recovery, portable runtime activation, restore caches and source pin cleanup.
pub const Recovery = struct {
    source_pin_cleanup: @import("source_pin_cleanup_owner.zig").Owner = .{},
    transactions: Transactions = .{},
    publication: Publication = .{},
    portable_activation: PortableActivation = .{},
    restore: Restore = .{},

    /// Transaction recovery contexts and the runtime that resolves in-doubt transactions.
    pub const Transactions = struct {
        identity_context: ?*db_mod.db_core.TransactionRecoveryIdentityContext = null,
        local_context: ?*db_mod.TransactionRecoveryLocalContext = null,
        owner: @import("local_runtime_owner.zig").OwningBundle(db_mod.TransactionRecoveryContexts, db_mod.transaction_runtime_mod.Runtime, db_mod.TransactionRecoveryContexts.release) = .{},
        runtime: ?*db_mod.transaction_runtime_mod.Runtime = null,
    };

    /// Publication recovery. `owner` is always set by `DB.open`.
    pub const Publication = struct {
        /// Owner-scoped recovery keeps remote acknowledgement off the open/read
        /// path. The startup barrier orders crash-left obligations before new WAL
        /// records; steady-state mutations then wait on independent LSN watermarks.
        owner: @import("publication_recovery_owner.zig").Owner = undefined,
        snapshot_mutex: std.atomic.Mutex = .unlocked,
    };

    /// Retries activating a portable runtime until it succeeds.
    pub const PortableActivation = struct {
        attempts: db_mod.AtomicU64 = db_mod.AtomicU64.init(0),
        mutex: std.atomic.Mutex = .unlocked,
        owner: db_mod.portable_activation_recovery.Owner = .{},
    };

    /// Caches reused across restore pages.
    pub const Restore = struct {
        decoder_cache: @import("../restore_decoder_cache.zig").Cache = .{},
        graph_parse_cache: ?db_mod.GraphRestoreParseCache = null,
        graph_parse_count_for_test: usize = 0,
    };

    fn dbPtr(self: *Recovery) *DB {
        return @alignCast(@fieldParentPtr("recovery", self));
    }

    pub fn initTransactionRuntime(self: *Recovery, cfg: db_mod.transaction_runtime_mod.Config) !void {
        const db = self.dbPtr();
        if (comptime db_mod.storage_engine.enabled) return db_mod.storage_engine.call("initTransactionRuntime", .{ db, cfg });
        var contexts: db_mod.TransactionRecoveryContexts = .{
            .identity = try db_mod.db_core.TransactionRecoveryIdentityContext.init(
                db.runtime_alloc,
                db.core.store,
                db.core.identity_namespace,
                db_mod.relationalColumns(db),
                if (db.core.schema) |schema| schema.version else 0,
            ),
        };
        errdefer contexts.release();
        contexts.identity.resource_manager = db.core.index_manager.resource_manager;
        contexts.identity.io = db.backend_runtime.io() orelse std.Options.debug_io;
        self.transactions.owner = try @TypeOf(self.transactions.owner).create(db.runtime_alloc, contexts, .{ db, cfg }, constructTransactionRuntime);
        self.transactions.identity_context = &self.transactions.owner.context.?.identity;
        self.transactions.local_context = &self.transactions.owner.context.?.local;
        self.transactions.runtime = self.transactions.owner.runtime.?;
    }

    pub fn prepareTransactionRecoveryOwner(self: *Recovery) !void {
        const db = self.dbPtr();
        const ctx = self.transactions.local_context orelse return;
        if (ctx.execution != null) return;
        ctx.split_shadow = db.indexes.split_shadow;
        ctx.execution = db_mod.LocalExecutionContext.borrow(db);
    }

    fn constructTransactionRuntime(alloc: Allocator, contexts: *db_mod.TransactionRecoveryContexts, db: *DB, cfg: db_mod.transaction_runtime_mod.Config) !db_mod.transaction_runtime_mod.Runtime {
        var effective_cfg = cfg;
        effective_cfg.resolution_extra_hooks = db_mod.db_core.transactionRecoveryIdentityHooks(&contexts.identity);
        effective_cfg.local_resolution_ctx = &contexts.local;
        effective_cfg.resolve_local_fn = db_mod.resolveRecoveredLocalTransaction;
        return try db_mod.transaction_runtime_mod.Runtime.init(
            alloc,
            db.core.batchExecutionResources().store,
            db.backend_runtime,
            effective_cfg,
        );
    }

    pub fn ensureTransactionRecoveryRuntime(self: *Recovery, cfg: db_mod.transaction_runtime_mod.Config) !void {
        const db = self.dbPtr();
        if (comptime db_mod.storage_engine.enabled) return db_mod.storage_engine.call("ensureTransactionRecoveryRuntime", .{ db, cfg });
        if (!cfg.enabled or self.transactions.runtime != null) return;
        try db.lockApplyForPortableRuntime();
        defer db.core.unlockApply();
        if (self.transactions.runtime != null) return;
        try self.initTransactionRuntime(cfg);
        if (db.local_execution.optional_runtime_workers_enabled) {
            const runtime = self.transactions.runtime.?;
            try self.prepareTransactionRecoveryOwner();
            try runtime.start();
        }
    }

    /// Admit an external recovery pass without exposing write-gate policy.
    pub fn admitTransactionRecovery(self: *Recovery) !?db_mod.MutationBarrier.SharedLease {
        const db = self.dbPtr();
        if (comptime db_mod.storage_engine.enabled) return db_mod.storage_engine.call("admitTransactionRecovery", .{db});
        var lease = db.acquireReplicationMutationShared();
        errdefer if (lease) |*held| held.release();
        try db.enforceReplicationWriteGate();
        return lease;
    }

    /// Source-pin deletion shares the LSM maintenance lane; it never creates a
    /// dedicated thread or monopolizes compaction while the corpus is large.
    pub fn runSourcePinCleanupStep(self: *Recovery) !bool {
        const db = self.dbPtr();
        if (comptime db_mod.storage_engine.enabled) return db_mod.storage_engine.call("runSourcePinCleanupStep", .{db});
        if (db_mod.openModeRequiresReadOnlyBackends(db.open_mode) or !self.source_pin_cleanup.due(db_mod.monotonicTimeNs())) return false;
        const units_before = self.source_pin_cleanup.work_units.load(.acquire);
        const work = @import("source_pin.zig").reconcileReleasedWithBudget(db, .{}) catch |err| {
            const progressed = self.source_pin_cleanup.work_units.load(.acquire) != units_before;
            self.recordSourcePinCleanupFailureWithProgress(err, progressed);
            return progressed;
        };
        const progressed = work.completed_units != 0;
        self.source_pin_cleanup.succeeded(progressed, db_mod.monotonicTimeNs());
        return progressed;
    }

    pub fn recordSourcePinCleanupFailure(self: *Recovery, err: anyerror) void {
        self.recordSourcePinCleanupFailureWithProgress(err, false);
    }

    fn recordSourcePinCleanupFailureWithProgress(self: *Recovery, err: anyerror, progressed: bool) void {
        const db = self.dbPtr();
        self.source_pin_cleanup.failed(err, progressed, db_mod.monotonicTimeNs(), db.core.path);
    }

    pub fn childGenerationSourcePinsSchema(core: *db_mod.db_core.DBCore, candidate_version: u32) !bool {
        if (!try DB.childGenerationSourceFence(core)) return false;
        const durable = core.schema orelse return error.IntegrityCatalogChanged;
        if (candidate_version <= durable.version) return false;
        if (candidate_version != (std.math.add(u32, durable.version, 1) catch return error.GenerationAdmissionChanged))
            return error.GenerationAdmissionChanged;
        return true;
    }

    /// The metadata successor is visible before its exact Raft schema install.
    /// Callers configuring a reopened owner must retain its old schema and
    /// indexes while the verified child-source fence remains active.
    pub fn childGenerationSourcePinsSchemaJson(self: *Recovery, schema_json: []const u8) !bool {
        const db = self.dbPtr();
        var parsed = try db_mod.public_table_schema.parseValidatedTableSchema(db.alloc, schema_json);
        defer parsed.deinit(db.alloc);
        db.core.lockApplyShared();
        defer db.core.unlockApplyShared();
        return childGenerationSourcePinsSchema(db.core, parsed.version);
    }

    /// Writer-open recovery runs before any optional worker or catalog replay.
    /// A durable prepared admission cannot be hidden by its applied watermark.
    pub fn resumePreparedSourcePin(self: *Recovery) !void {
        const db = self.dbPtr();
        const pending = blk: {
            var read = try db.core.store.beginReadTxn();
            defer read.abort();
            break :blk try @import("../source_pin_state.zig").load(&read);
        };
        if (pending) |prepared| {
            var namespace: [24]u8 = undefined;
            db_mod.doc_identity.encodeNamespace(&namespace, db.core.identity_namespace);
            if (!std.mem.eql(u8, &prepared.namespace, &namespace)) return;
            const scope = try @import("online_source_contract.zig").Scope.decode(&prepared.scope_bytes);
            try db.lockApplyForPortableRuntime();
            defer db.core.unlockApply();
            try @import("source_pin.zig").ensureAssumeApply(db, scope);
        }
    }

    /// Retries a top-level portable catalog activation that failed after the
    /// durable generation commit. The restore loader rolls partial runtime
    /// state back on error. Recovery is single-flight: callers never queue a
    /// second full catalog reload behind one already in progress.
    pub fn retryPortableRuntimeActivationIfNeeded(self: *Recovery) !bool {
        const db = self.dbPtr();
        if (!db.async_context.portable_runtime_activation_pending.load(.acquire)) return false;
        if (!self.portable_activation.mutex.tryLock()) return error.PortableRuntimeActivationPending;
        defer self.portable_activation.mutex.unlock();
        if (!db.async_context.portable_runtime_activation_pending.load(.acquire)) return false;

        _ = self.portable_activation.attempts.fetchAdd(1, .monotonic);
        db.loadPortableRuntimeAfterPublication() catch |err| {
            db.async_context.portable_runtime_activation_pending.store(true, .release);
            return err;
        };
        db.async_context.portable_runtime_activation_pending.store(false, .release);
        // Session cleanup intentionally leaves maintenance deferred while the
        // catalog is degraded. Release that deferral only after the restored
        // runtime is fully usable.
        db_mod.resumeDeferredBackgroundMaintenanceIfIdle(db.async_context);
        db.maintenance.startQuarantineRetryWorkerIfNeeded();
        std.log.info("portable runtime activation recovered path={s}", .{db.core.path});
        return true;
    }

    pub fn retryPortableActivationFromOwner(ptr: *anyopaque) !bool {
        const db: *DB = @ptrCast(@alignCast(ptr));
        return db.recovery.retryPortableRuntimeActivationIfNeeded();
    }

    pub fn startPortableActivationRetryWorkerIfNeeded(self: *Recovery) void {
        self.startPortableActivationRetryWorkerIfNeededInner(false);
    }

    pub fn startPortableActivationRetryWorkerIfNeededInner(self: *Recovery, allow_test_background: bool) void {
        const db = self.dbPtr();
        if (comptime builtin.single_threaded or builtin.os.tag == .freestanding) return;
        if (comptime builtin.is_test) {
            if (!allow_test_background) return;
        }
        self.portable_activation.owner.start(db.portableActivationRecoveryPort());
    }

    pub fn launchPortableActivationRetryWorkerLocked(self: *Recovery) !db_mod.portable_activation_recovery.Owner.PortableActivationRetryLaunch {
        const db = self.dbPtr();
        self.portable_activation.owner.port = db.portableActivationRecoveryPort();
        self.portable_activation.owner.bound = true;
        return self.portable_activation.owner.launchPortableActivationRetryWorkerLocked();
    }

    pub fn stopPortableActivationRetryWorker(self: *Recovery) void {
        const db = self.dbPtr();
        self.portable_activation.owner.stop(db.portableActivationRecoveryPort());
    }
};

test "portable activation retry runtime job can be restarted" {
    if (builtin.single_threaded or builtin.os.tag == .freestanding) return error.SkipZigTest;

    const alloc = std.testing.allocator;
    var path_tmp = try db_mod.TestDirectory.init("db");
    defer path_tmp.cleanup();
    const path = path_tmp.path().ptr;
    defer db_mod.cleanupTempDir(path);

    var db = try DB.open(alloc, std.mem.span(path), .{
        .start_index_workers = false,
        .start_optional_runtimes = false,
        .start_optional_runtime_workers = false,
        .ttl_cleanup = .{ .enabled = false },
    });
    defer db.close();

    // A worker with no pending activation exits immediately. Exercise two full
    // runtime-owned generations to prove completion releases launch admission.
    for (0..2) |_| {
        _ = db_mod.lockAtomic(&db.recovery.portable_activation.owner.lifecycle_mutex);
        db.recovery.portable_activation.owner.stopping.store(false, .release);
        db.recovery.portable_activation.owner.worker_running.store(true, .release);
        const launch = db.recovery.launchPortableActivationRetryWorkerLocked() catch |err| {
            db.recovery.portable_activation.owner.worker_running.store(false, .release);
            db.recovery.portable_activation.owner.lifecycle_mutex.unlock();
            return err;
        };
        db.recovery.portable_activation.owner.lifecycle_mutex.unlock();
        try std.testing.expectEqual(db_mod.portable_activation_recovery.Owner.PortableActivationRetryLaunch.backend_runtime, launch);

        const exit_deadline = db_mod.monotonicTimeNs() + std.time.ns_per_s;
        while (db.recovery.portable_activation.owner.worker_running.load(.acquire) and db_mod.monotonicTimeNs() < exit_deadline) {
            @import("antfly_platform").time.yieldNow();
        }
        try std.testing.expect(!db.recovery.portable_activation.owner.worker_running.load(.acquire));
    }
}

test "portable activation retry stop joins the runtime worker final handshake" {
    if (builtin.single_threaded or builtin.os.tag == .freestanding) return error.SkipZigTest;

    const alloc = std.testing.allocator;
    var path_tmp = try db_mod.TestDirectory.init("db");
    defer path_tmp.cleanup();
    const path = path_tmp.path().ptr;
    defer db_mod.cleanupTempDir(path);

    var db = try DB.open(alloc, std.mem.span(path), .{
        .start_index_workers = false,
        .start_optional_runtimes = false,
        .start_optional_runtime_workers = false,
        .ttl_cleanup = .{ .enabled = false },
    });
    defer db.close();

    const ExitCtx = struct {
        db: *DB,
        holding_final_lock: *std.atomic.Value(bool),
        release_final_lock: *std.atomic.Value(bool),

        fn run(ctx: @This()) void {
            while (!ctx.db.recovery.portable_activation.owner.stopping.load(.acquire)) @import("antfly_platform").time.yieldNow();
            _ = db_mod.lockAtomic(&ctx.db.recovery.portable_activation.owner.lifecycle_mutex);
            ctx.db.recovery.portable_activation.owner.worker_running.store(false, .release);
            ctx.holding_final_lock.store(true, .release);
            while (!ctx.release_final_lock.load(.acquire)) @import("antfly_platform").time.yieldNow();
            ctx.db.recovery.portable_activation.owner.lifecycle_mutex.unlock();
        }
    };
    const StopCtx = struct {
        db: *DB,
        returned: *std.atomic.Value(bool),

        fn run(ctx: @This()) void {
            ctx.db.recovery.stopPortableActivationRetryWorker();
            ctx.returned.store(true, .release);
        }
    };

    var holding_final_lock = std.atomic.Value(bool).init(false);
    var release_final_lock = std.atomic.Value(bool).init(false);
    var stop_returned = std.atomic.Value(bool).init(false);
    db.recovery.portable_activation.owner.stopping.store(false, .release);
    db.recovery.portable_activation.owner.worker_running.store(true, .release);

    var worker = try std.Thread.spawn(.{}, ExitCtx.run, .{ExitCtx{
        .db = &db,
        .holding_final_lock = &holding_final_lock,
        .release_final_lock = &release_final_lock,
    }});
    var worker_joined = false;
    defer if (!worker_joined) {
        release_final_lock.store(true, .release);
        worker.join();
    };
    var stopper = try std.Thread.spawn(.{}, StopCtx.run, .{StopCtx{
        .db = &db,
        .returned = &stop_returned,
    }});
    var stopper_joined = false;
    defer if (!stopper_joined) stopper.join();

    const hold_deadline = db_mod.monotonicTimeNs() + std.time.ns_per_s;
    while (!holding_final_lock.load(.acquire) and db_mod.monotonicTimeNs() < hold_deadline) {
        @import("antfly_platform").time.yieldNow();
    }
    try std.testing.expect(holding_final_lock.load(.acquire));
    // Give the stopper ample time to observe the published false state. It
    // must still wait for the lifecycle lock that fences the worker's return.
    db_mod.sleepNs(10 * std.time.ns_per_ms);
    try std.testing.expect(!stop_returned.load(.acquire));

    release_final_lock.store(true, .release);
    worker.join();
    worker_joined = true;
    stopper.join();
    stopper_joined = true;
    try std.testing.expect(stop_returned.load(.acquire));
}

test "portable activation retry shutdown rejects an already claimed maintenance probe" {
    if (builtin.single_threaded or builtin.os.tag == .freestanding) return error.SkipZigTest;

    const alloc = std.testing.allocator;
    var path_tmp = try db_mod.TestDirectory.init("db");
    defer path_tmp.cleanup();
    const path = path_tmp.path().ptr;
    defer db_mod.cleanupTempDir(path);

    var db = try DB.open(alloc, std.mem.span(path), .{
        .start_index_workers = false,
        .start_optional_runtimes = false,
        .start_optional_runtime_workers = false,
        .ttl_cleanup = .{ .enabled = false },
    });

    db_mod.test_fail_portable_activation_retry_fallback_submit.store(true, .release);
    defer db_mod.test_fail_portable_activation_retry_fallback_submit.store(false, .release);
    defer db_mod.test_pause_portable_activation_retry_probe_before_lifecycle_lock.store(false, .release);
    defer db_mod.test_portable_activation_retry_probe_paused.store(false, .release);
    defer db_mod.test_release_portable_activation_retry_probe.store(true, .release);

    db.async_context.portable_runtime_activation_pending.store(true, .release);
    db.recovery.startPortableActivationRetryWorkerIfNeededInner(true);
    try std.testing.expect(db.recovery.portable_activation.owner.next_launch_ns > 0);

    // Rearm the injected launch failures. The claimed probe must leave these
    // untouched after close publishes the permanent stop state.
    db_mod.test_fail_portable_activation_retry_fallback_submit.store(true, .release);
    db.recovery.portable_activation.owner.next_launch_ns = 0;
    db_mod.test_release_portable_activation_retry_probe.store(false, .release);
    db_mod.test_portable_activation_retry_probe_paused.store(false, .release);
    db_mod.test_pause_portable_activation_retry_probe_before_lifecycle_lock.store(true, .release);

    const PollCtx = struct {
        db: *DB,
        completed: *std.atomic.Value(bool),

        fn run(ctx: @This()) void {
            // The runtime reaper and this explicit poll legitimately race to
            // claim the probe. A zero count means the reaper won; the paused
            // hook below proves that one of them did claim it.
            _ = ctx.db.backend_runtime.durable_jobs.poll(1) catch return;
            ctx.completed.store(true, .release);
        }
    };
    const CloseCtx = struct {
        db: *DB,
        returned: *std.atomic.Value(bool),

        fn run(ctx: @This()) void {
            ctx.db.close();
            ctx.returned.store(true, .release);
        }
    };

    var poll_completed = std.atomic.Value(bool).init(false);
    var close_returned = std.atomic.Value(bool).init(false);
    var poller = try std.Thread.spawn(.{}, PollCtx.run, .{PollCtx{
        .db = &db,
        .completed = &poll_completed,
    }});
    var poller_joined = false;
    defer if (!poller_joined) {
        db_mod.test_release_portable_activation_retry_probe.store(true, .release);
        poller.join();
    };

    const pause_deadline = db_mod.monotonicTimeNs() + std.time.ns_per_s;
    while (!db_mod.test_portable_activation_retry_probe_paused.load(.acquire) and db_mod.monotonicTimeNs() < pause_deadline) {
        @import("antfly_platform").time.yieldNow();
    }
    try std.testing.expect(db_mod.test_portable_activation_retry_probe_paused.load(.acquire));

    var closer = try std.Thread.spawn(.{}, CloseCtx.run, .{CloseCtx{
        .db = &db,
        .returned = &close_returned,
    }});
    var closer_joined = false;
    defer if (!closer_joined) {
        db_mod.test_release_portable_activation_retry_probe.store(true, .release);
        closer.join();
    };

    const stop_deadline = db_mod.monotonicTimeNs() + std.time.ns_per_s;
    while (!db.recovery.portable_activation.owner.stopping.load(.acquire) and db_mod.monotonicTimeNs() < stop_deadline) {
        @import("antfly_platform").time.yieldNow();
    }
    try std.testing.expect(db.recovery.portable_activation.owner.stopping.load(.acquire));
    try std.testing.expect(!close_returned.load(.acquire));

    db_mod.test_release_portable_activation_retry_probe.store(true, .release);
    poller.join();
    poller_joined = true;
    closer.join();
    closer_joined = true;

    try std.testing.expect(poll_completed.load(.acquire));
    try std.testing.expect(close_returned.load(.acquire));
    try std.testing.expect(db_mod.test_fail_portable_activation_retry_fallback_submit.load(.acquire));
}

test "portable activation retry uses owner scoped runtime" {
    if (builtin.single_threaded or builtin.os.tag == .freestanding) return error.SkipZigTest;

    const alloc = std.testing.allocator;
    var path_tmp = try db_mod.TestDirectory.init("db");
    defer path_tmp.cleanup();
    const path = path_tmp.path().ptr;
    defer db_mod.cleanupTempDir(path);

    var db = try DB.open(alloc, std.mem.span(path), .{
        .start_index_workers = false,
        .start_optional_runtimes = false,
        .start_optional_runtime_workers = false,
        .ttl_cleanup = .{ .enabled = false },
    });
    defer db.close();

    db.async_context.portable_runtime_activation_pending.store(true, .release);
    _ = db_mod.lockAtomic(&db.recovery.portable_activation.owner.lifecycle_mutex);
    db.recovery.portable_activation.owner.stopping.store(false, .release);
    db.recovery.portable_activation.owner.worker_running.store(true, .release);
    const launch = db.recovery.launchPortableActivationRetryWorkerLocked() catch |err| {
        db.recovery.portable_activation.owner.worker_running.store(false, .release);
        db.recovery.portable_activation.owner.lifecycle_mutex.unlock();
        return err;
    };
    db.recovery.portable_activation.owner.lifecycle_mutex.unlock();
    try std.testing.expectEqual(db_mod.portable_activation_recovery.Owner.PortableActivationRetryLaunch.backend_runtime, launch);

    const recovery_deadline = db_mod.monotonicTimeNs() + 5 * std.time.ns_per_s;
    while ((db.async_context.portable_runtime_activation_pending.load(.acquire) or
        db.recovery.portable_activation.owner.worker_running.load(.acquire)) and
        db_mod.monotonicTimeNs() < recovery_deadline)
    {
        @import("antfly_platform").time.yieldNow();
    }
    try std.testing.expect(!db.async_context.portable_runtime_activation_pending.load(.acquire));
    try std.testing.expect(!db.recovery.portable_activation.owner.worker_running.load(.acquire));
    try std.testing.expectEqual(@as(u64, 1), db.recovery.portable_activation.attempts.load(.monotonic));
}

test "portable activation retry automatically rearms after runtime submission fails" {
    if (builtin.single_threaded or builtin.os.tag == .freestanding) return error.SkipZigTest;

    const alloc = std.testing.allocator;
    var path_tmp = try db_mod.TestDirectory.init("db");
    defer path_tmp.cleanup();
    const path = path_tmp.path().ptr;
    defer db_mod.cleanupTempDir(path);

    var db = try DB.open(alloc, std.mem.span(path), .{
        .start_index_workers = false,
        .start_optional_runtimes = false,
        .start_optional_runtime_workers = false,
        .ttl_cleanup = .{ .enabled = false },
    });
    defer db.close();

    db.async_context.portable_runtime_activation_pending.store(true, .release);
    db_mod.test_fail_portable_activation_retry_fallback_submit.store(true, .release);
    defer db_mod.test_fail_portable_activation_retry_fallback_submit.store(false, .release);

    db.recovery.startPortableActivationRetryWorkerIfNeededInner(true);
    try std.testing.expect(!db.recovery.portable_activation.owner.worker_running.load(.acquire));
    try std.testing.expectEqual(@as(u32, 1), db.recovery.portable_activation.owner.launch_failure_streak);
    try std.testing.expect(db.recovery.portable_activation.owner.next_launch_ns > 0);

    const recovery_deadline = db_mod.monotonicTimeNs() + 5 * std.time.ns_per_s;
    while ((db.async_context.portable_runtime_activation_pending.load(.acquire) or
        db.recovery.portable_activation.owner.worker_running.load(.acquire)) and
        db_mod.monotonicTimeNs() < recovery_deadline)
    {
        db_mod.sleepNs(std.time.ns_per_ms);
    }
    try std.testing.expect(!db.async_context.portable_runtime_activation_pending.load(.acquire));
    try std.testing.expect(!db.recovery.portable_activation.owner.worker_running.load(.acquire));
    try std.testing.expectEqual(@as(u32, 0), db.recovery.portable_activation.owner.launch_failure_streak);
    try std.testing.expectEqual(@as(u64, 0), db.recovery.portable_activation.owner.next_launch_ns);
    try std.testing.expectEqual(@as(u64, 1), db.recovery.portable_activation.attempts.load(.monotonic));
}

test "db searches fail fast without joining portable activation recovery" {
    if (builtin.single_threaded or builtin.os.tag == .freestanding) return error.SkipZigTest;

    const alloc = std.testing.allocator;
    var io_impl = std.Io.Threaded.init(alloc, .{});
    defer io_impl.deinit();
    const io = io_impl.io();
    var path_tmp = try db_mod.TestDirectory.init("db");
    defer path_tmp.cleanup();
    const path = path_tmp.path().ptr;
    defer db_mod.cleanupTempDir(path);

    var db = try DB.open(alloc, std.mem.span(path), .{
        .start_index_workers = false,
        .start_optional_runtimes = false,
        .start_optional_runtime_workers = false,
        .ttl_cleanup = .{ .enabled = false },
    });
    defer db.close();
    try db.addIndex(.{
        .name = "dense_idx",
        .kind = .dense_vector,
        .config_json = "{\"field\":\"embedding\",\"dims\":2,\"metric\":\"l2_squared\"}",
    });

    db.async_context.portable_runtime_activation_pending.store(true, .release);
    var lookup_hook = db_mod.PublishedDenseCatalogLookupTestHook{ .io = io };
    lookup_hook.release.set(io);
    db_mod.test_published_dense_catalog_lookup_hook = &lookup_hook;
    defer {
        db_mod.test_published_dense_catalog_lookup_hook = null;
        db.async_context.portable_runtime_activation_pending.store(false, .release);
    }
    const attempts_before = db.recovery.portable_activation.attempts.load(.monotonic);

    try std.testing.expectError(error.PortableRuntimeActivationPending, db.search(alloc, .{
        .index_name = "dense_idx",
        .limit = 1,
        .include_stored = false,
        .query = .{ .dense_knn = .{ .vector = &.{ 0.0, 0.0 }, .k = 1 } },
    }));
    try std.testing.expect(!lookup_hook.entered.isSet());
    try std.testing.expectEqual(attempts_before, db.recovery.portable_activation.attempts.load(.monotonic));

    try std.testing.expectError(error.PortableRuntimeActivationPending, db.searchDenseProfiled(alloc, .{
        .index_name = "dense_idx",
        .limit = 1,
        .include_stored = false,
        .query = .{ .dense_knn = .{ .vector = &.{ 0.0, 0.0 }, .k = 1 } },
    }, .{ .vector = &.{ 0.0, 0.0 }, .k = 1 }));
    try std.testing.expect(!lookup_hook.entered.isSet());
    try std.testing.expectEqual(attempts_before, db.recovery.portable_activation.attempts.load(.monotonic));

    try std.testing.expect(db.recovery.portable_activation.mutex.tryLock());
    defer db.recovery.portable_activation.mutex.unlock();
    try std.testing.expectError(error.PortableRuntimeActivationPending, db.recovery.retryPortableRuntimeActivationIfNeeded());
    try std.testing.expectEqual(attempts_before, db.recovery.portable_activation.attempts.load(.monotonic));
}

test "db graph ownership restore materializes large artifacts in published segments" {
    // Keep leak checks and failure injection; allocation backtraces are opt-in.
    var allocator_state: std.heap.SafeAllocator = .init(std.heap.page_allocator, .{ .stack_trace_frames = 0 });
    defer std.debug.assert(allocator_state.deinit() == 0);
    const alloc = if (@import("antfly_platform").env.getenvBool("ANTFLY_TEST_ALLOCATOR_TRACES")) std.testing.allocator else allocator_state.allocator();
    var path_tmp = try db_mod.TestDirectory.init("db");
    defer path_tmp.cleanup();
    const path = path_tmp.path().ptr;
    defer db_mod.cleanupTempDir(path);

    var db = try DB.open(alloc, std.mem.span(path), .{ .start_index_workers = false });
    defer db.close();
    try db.addIndex(.{
        .name = "relations_graph",
        .kind = .graph,
        .config_json =
        \\{"source":{"artifact":"relations_v1","path":"$.relations[*]","format":"extraction_relation"},"artifact":{"name":"relations_v1","kind":"asset","source":{"type":"field","value":"relations"},"content_type":"application/json"}}
        ,
    });

    var raw = std.ArrayListUnmanaged(u8).empty;
    defer raw.deinit(alloc);
    try raw.appendSlice(alloc, "{\"relations\":[");
    for (0..2050) |i| {
        if (i > 0) try raw.append(alloc, ',');
        const item = try std.fmt.allocPrint(alloc, "{{\"type\":\"mentions\",\"target\":\"doc:{d}\"}}", .{i});
        defer alloc.free(item);
        try raw.appendSlice(alloc, item);
    }
    try raw.appendSlice(alloc, "]}");
    const artifact_key = try db_mod.internal_keys.artifactNamedPrefixAlloc(alloc, "doc:a", "asset", "relations_v1");
    defer alloc.free(artifact_key);
    try db.core.store.put(artifact_key, raw.items);

    const first = try db_mod.materializeGraphSourceArtifactRestorePage(
        &db,
        alloc,
        artifact_key,
        "relations_graph",
        0,
        0,
        0,
        0,
    );
    try std.testing.expectEqual(@as(?usize, 2048), first.next_item_offset);
    try std.testing.expectEqual(@as(usize, 1), first.next_segment_index);
    try std.testing.expectEqual(@as(usize, 2048), first.key_count);

    // A failed later page rolls back one committed segment atomically. With a
    // single segment there is no continuation, and neither the visible edge
    // nor its ownership manifest may survive.
    var partial_ref = (try db_mod.decodeArtifactRefIfKnownAlloc(alloc, artifact_key)) orelse return error.TestUnexpectedResult;
    defer partial_ref.deinit(alloc);
    try std.testing.expect(!try db_mod.rollbackGraphSourceArtifactRestoreStatePage(&db, alloc, "relations_graph", partial_ref));
    const partial_state_name = try db_mod.graphArtifactStateNameAlloc(alloc, partial_ref);
    defer alloc.free(partial_state_name);
    const partial_state_key = try db_mod.graphAssetStateKeyAlloc(alloc, "doc:a", "relations_graph", partial_state_name);
    defer alloc.free(partial_state_key);
    const partial_segment_key = try db_mod.internal_keys.graphAssetStateSegmentKeyAlloc(alloc, partial_state_key, 0);
    defer alloc.free(partial_segment_key);
    try std.testing.expectError(error.NotFound, db.core.store.get(alloc, partial_segment_key));
    const partial_edge_key = try db_mod.internal_keys.graphEdgeArtifactKeyAlloc(alloc, "doc:a", "relations_graph", "mentions", "doc:0");
    defer alloc.free(partial_edge_key);
    try std.testing.expectError(error.NotFound, db.core.store.get(alloc, partial_edge_key));

    const restarted_first = try db_mod.materializeGraphSourceArtifactRestorePage(
        &db,
        alloc,
        artifact_key,
        "relations_graph",
        0,
        0,
        0,
        0,
    );

    const second = try db_mod.materializeGraphSourceArtifactRestorePage(
        &db,
        alloc,
        artifact_key,
        "relations_graph",
        restarted_first.next_item_offset.?,
        restarted_first.next_segment_index,
        restarted_first.key_count,
        restarted_first.manifest_bytes,
    );
    try std.testing.expectEqual(@as(?usize, null), second.next_item_offset);
    try std.testing.expectEqual(@as(usize, 2), second.next_segment_index);
    try std.testing.expectEqual(@as(usize, 2050), second.key_count);
    try std.testing.expectEqual(@as(usize, 1), db.recovery.restore.graph_parse_count_for_test);

    var artifact_ref = (try db_mod.decodeArtifactRefIfKnownAlloc(alloc, artifact_key)) orelse return error.TestUnexpectedResult;
    defer artifact_ref.deinit(alloc);
    const state_name = try db_mod.graphArtifactStateNameAlloc(alloc, artifact_ref);
    defer alloc.free(state_name);
    const state_key = try db_mod.graphAssetStateKeyAlloc(alloc, "doc:a", "relations_graph", state_name);
    defer alloc.free(state_key);
    const root_raw = try db.core.store.get(alloc, state_key);
    defer alloc.free(root_raw);
    try std.testing.expectEqual(db_mod.graph_asset_state.Format.v5, try db_mod.graph_asset_state.format(root_raw));
    const restored_keys = (try db_mod.loadGraphAssetStateKeysAlloc(alloc, db.core.store, state_key, db.core.index_manager.graphIndex("relations_graph").?.config.coverage_generation)).?;
    defer db_mod.freeOwnedConstKeySlice(alloc, restored_keys);
    try std.testing.expectEqual(@as(usize, 2050), restored_keys.len);

    try db.core.store.put(artifact_key, "{\"relations\":[{\"type\":\"mentions\",\"target\":\"doc:replacement\"}]}");
    const changed = try db_mod.materializeGraphSourceArtifactsForIndex(
        alloc,
        db.core.store,
        db.core.index_manager,
        &.{artifact_key},
        "relations_graph",
        .{},
    );
    defer db_mod.freeOwnedKeySlice(alloc, changed);
    const first_segment_key = try db_mod.internal_keys.graphAssetStateSegmentKeyAlloc(alloc, state_key, 0);
    defer alloc.free(first_segment_key);
    const second_segment_key = try db_mod.internal_keys.graphAssetStateSegmentKeyAlloc(alloc, state_key, 1);
    defer alloc.free(second_segment_key);
    const replacement_segment = try db.core.store.get(alloc, first_segment_key);
    defer alloc.free(replacement_segment);
    try std.testing.expectError(error.NotFound, db.core.store.get(alloc, second_segment_key));
    const replacement_root = try db.core.store.get(alloc, state_key);
    defer alloc.free(replacement_root);
    try std.testing.expectEqual(db_mod.graph_asset_state.Format.v5, try db_mod.graph_asset_state.format(replacement_root));
    const replacement_keys = (try db_mod.loadGraphAssetStateKeysAlloc(
        alloc,
        db.core.store,
        state_key,
        db.core.index_manager.graphIndex("relations_graph").?.config.coverage_generation,
    )).?;
    defer db_mod.freeOwnedConstKeySlice(alloc, replacement_keys);
    try std.testing.expectEqual(@as(usize, 1), replacement_keys.len);
}
