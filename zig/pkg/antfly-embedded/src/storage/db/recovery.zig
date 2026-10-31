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
