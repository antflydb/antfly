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
const db_mod = @import("../db.zig");
const DB = db_mod.DB;

/// Enrichment state owned by `DB`: the enrichment runtime, artifact production and repair, entity resolution and promotion.
pub const Enrichment = struct {
    owner: DB.EnrichmentOwner = .{},
    /// Borrowed views for existing local consumers. The bundle owns destruction;
    /// replacement publishes these views while holding the lifecycle fence.
    append_context: ?*db_mod.EnrichmentAppendContext = null,
    runtime: ?*db_mod.enrichment_runtime_mod.EnrichmentRuntime = null,
    embedding_activity: @import("../embedding_activity_cache.zig").Owner = .{},
    managed_admission: @import("../managed_admission_owner.zig").Owner = .{},
    artifacts: Artifacts = .{},
    resolution: Resolution = .{},
    promotion: Promotion = .{},

    /// Artifact production and repair metadata maintenance.
    pub const Artifacts = struct {
        repair_metadata_due_ns: u64 = 0,
        repair_metadata_pending: bool = true,
        footprint_pending: std.atomic.Value(bool) = .init(true),
        producer_baseline_pending: std.atomic.Value(bool) = .init(true),
        producer_scheduler: @import("../artifact_producer_scheduler.zig").Scheduler = .{},
    };

    /// Entity resolution: its append context, runtime, candidate source and embedder.
    pub const Resolution = struct {
        append_context: ?*db_mod.EnrichmentAppendContext = null,
        owner: @import("../local_runtime_owner.zig").Bundle(db_mod.EnrichmentAppendContext, db_mod.resolution_runtime_mod.ResolutionRuntime) = .{},
        runtime: ?*db_mod.resolution_runtime_mod.ResolutionRuntime = null,
        candidate_source: ?db_mod.resolution_runtime_mod.CandidateSource = null,
        embedder: ?db_mod.embedder_mod.DenseEmbedder = null,
    };

    /// Entity promotion: its runtime and the sink that receives promoted entities.
    pub const Promotion = struct {
        owner_bundle: @import("../local_runtime_owner.zig").RuntimeOwner(db_mod.promotion_runtime_mod.PromotionRuntime) = .{},
        runtime: ?*db_mod.promotion_runtime_mod.PromotionRuntime = null,
        entity_sink: ?db_mod.promotion_runtime_mod.EntitySink = null,
        owner: ?db_mod.promotion_runtime_mod.PromotionOwner = null,
        entity_sink_missing_policy: db_mod.promotion_runtime_mod.MissingSinkPolicy = .wait,
    };

    fn dbPtr(self: *Enrichment) *DB {
        return @alignCast(@fieldParentPtr("enrichment", self));
    }

    pub fn createDetachedRuntime(self: *Enrichment, enrichment_cfg: *db_mod.enrichment_runtime_mod.Config) !?DB.DetachedEnrichmentRuntime {
        const db = self.dbPtr();
        if (comptime db_mod.storage_engine.enabled) return db_mod.storage_engine.call("createDetachedRuntime", .{ db, enrichment_cfg });
        if (enrichment_cfg.dense_embedder == null and enrichment_cfg.sparse_embedder == null and enrichment_cfg.asset_producer == null and !enrichment_cfg.enable_without_producers and !db.core.index_manager.hasGroupSources()) return null;
        // Producer ownership moves through reconfiguration, but execution and
        // security capabilities belong to the resident DB. Hydrate every
        // replacement at this single construction boundary so live index DDL
        // cannot silently fall back to default remote-content policy or an
        // unrelated executor when it supplies only a new producer set.
        var runtime_cfg = enrichment_cfg.*;
        if (db.core.index_manager.hasGroupSources()) runtime_cfg.enable_without_producers = true;
        runtime_cfg.root_incarnation = db.root_incarnation;
        if (runtime_cfg.secret_store == null) runtime_cfg.secret_store = db.secret_store;
        if (runtime_cfg.remote_content == null) runtime_cfg.remote_content = db.remote_content;
        if (runtime_cfg.resource_manager == null) runtime_cfg.resource_manager = db.core.index_manager.resource_manager;
        if (runtime_cfg.io == null) runtime_cfg.io = db.backend_runtime.inferenceIo();
        runtime_cfg.relational_base_rows = db_mod.relationalColumns(db) != null;

        const resources = db.core.batchExecutionResources();
        const context: db_mod.EnrichmentAppendContext = .{
            .alloc = db.runtime_alloc,
            .root_incarnation = db.root_incarnation,
            .read_only = db_mod.openModeRequiresReadOnlyBackends(db.open_mode),
            .clock = runtime_cfg.clock orelse db.backend_runtime.clock(),
            .store = resources.store,
            .applied_sequence_checkpoint_path = resources.applied_sequence_checkpoint_path,
            .index_repair_checkpoint = resources.index_repair_checkpoint,
            .shard_manager = resources.shard_manager,
            .index_manager = resources.index_manager,
            .apply_mutex = resources.apply_mutex,
            .portable_runtime_activation_pending = &db.async_context.portable_runtime_activation_pending,
            .snapshot_admission = resources.snapshot_admission,
            .snapshot_replay_admission = resources.snapshot_replay_admission,
            .repair_replay_mutex = resources.repair_replay_mutex,
            .change_journal = resources.change_journal,
            .replay_source = resources.replay_source,
            .executor = db.executor,
            .async_context = db.async_context,
            .log_mutex = resources.log_mutex,
            .replication_async_effect_mirror = db.local_execution.replication_async_effect_mirror,
            .replication_async_batch_mirror = db.local_execution.replication_async_batch_mirror,
            .replication_async_metadata_mirror = db.local_execution.replication_async_metadata_mirror,
            .replication_write_gate = db.local_execution.replication_write_gate,
            .resolution_runtime = self.resolution.runtime,
            .promotion_runtime = self.promotion.runtime,
            .artifact_publication_dispatcher = db.local_execution.artifact_publication_dispatcher,
        };
        var bundle = try DB.DetachedEnrichmentRuntime.create(db.runtime_alloc, enrichment_cfg, runtime_cfg, context, .{
            .backend_runtime = db.backend_runtime,
            .neighbor_acquire = db_mod.acquireNeighborContextGraphSource,
            .write = db_mod.appendGeneratedBatchFromEnrichment,
            .failure = db_mod.recordEnrichmentRequestFailure,
            .failure_pending = db_mod.enrichmentRequestFailurePending,
            .failure_range_pending = db_mod.enrichmentRequestFailureRangePending,
            .failure_lock = db_mod.lockEnrichmentFailurePendingFence,
            .failure_unlock = db_mod.unlockEnrichmentFailurePendingFence,
            .notify_ctx = db.executor,
            .notify = db_mod.notifyDerivedExecutorSequence,
            .commit = db_mod.commitArtifactUnitTurnFromEnrichment,
        });
        errdefer bundle.deinit(db.runtime_alloc);
        try mergeTerminalFailureEnvelope(resources.store, bundle.runtime.?);
        return bundle;
    }

    fn mergeTerminalFailureEnvelope(
        store: *db_mod.docstore_mod.DocStore,
        runtime: *db_mod.enrichment_runtime_mod.EnrichmentRuntime,
    ) !void {
        const marker_envelope = try db_mod.enrichmentTerminalFailureSequenceEnvelope(store);
        if (marker_envelope.min != 0) {
            runtime.terminal_failure_min_sequence = if (runtime.terminal_failure_min_sequence == 0)
                marker_envelope.min
            else
                @min(runtime.terminal_failure_min_sequence, marker_envelope.min);
            runtime.terminal_failure_max_sequence = @max(runtime.terminal_failure_max_sequence, marker_envelope.max);
        }
    }

    pub fn publishOwner(ptr: *anyopaque) void {
        const db: *DB = @ptrCast(@alignCast(ptr));
        db.enrichment.append_context = db.enrichment.owner.bundle.append_ctx;
        db.enrichment.runtime = db.enrichment.owner.bundle.runtime;
        db.async_context.enrichment_runtime = db.enrichment.owner.bundle.runtime;
    }

    pub fn initRuntime(self: *Enrichment, enrichment_cfg: *db_mod.enrichment_runtime_mod.Config) !void {
        const db = self.dbPtr();
        self.owner.bundle = (try self.createDetachedRuntime(enrichment_cfg)) orelse return;
        publishOwner(db);
    }

    pub fn reconfigureRuntime(self: *Enrichment, cfg: db_mod.enrichment_runtime_mod.Config) !void {
        const db = self.dbPtr();
        if (comptime db_mod.storage_engine.enabled) return db_mod.storage_engine.call("reconfigureRuntime", .{ db, cfg });
        try self.reconfigureRuntimeWithOptions(cfg, .{});
    }

    /// Install a replacement producer set without starting it. Structural
    /// activation uses this form to quiesce the old owner once, mutate the
    /// index catalog, and then start the exact replacement after the durable
    /// admission is established. Starting here would make addIndex stop the
    /// new worker a second time and let corpus work delay control-plane DDL.
    pub fn reconfigureRuntimePaused(self: *Enrichment, cfg: db_mod.enrichment_runtime_mod.Config) !void {
        const db = self.dbPtr();
        if (comptime db_mod.storage_engine.enabled) return db_mod.storage_engine.call("reconfigureRuntimePaused", .{ db, cfg });
        try self.reconfigureRuntimeWithOptions(cfg, .{ .start_replacement = false });
    }

    fn reconfigureRuntimeWithOptions(
        self: *Enrichment,
        cfg: db_mod.enrichment_runtime_mod.Config,
        options: DB.EnrichmentReconfigureOptions,
    ) !void {
        const db = self.dbPtr();
        if (db_mod.openModeRequiresReadOnlyBackends(db.open_mode)) return error.ReadOnly;

        const recovery = db.recovery.transactions.local_context;
        const recovery_io = db.backend_runtime.io();
        if (recovery) |ctx| ctx.provider_mutex.lockUncancelable(recovery_io.?);
        defer if (recovery) |ctx| ctx.provider_mutex.unlock(recovery_io.?);

        const hook_present = DB.hasQueryVisibilityHook(db.async_context);
        try self.owner.replace(.{
            .ptr = db,
            .alloc = db.runtime_alloc,
            .mutex = &db.async_context.enrichment_lifecycle_mutex,
            .desired_running = &db.async_context.enrichment_desired_running,
            .allows_running = db.open_mode.allowsOptionalRuntimes(),
            .status_hook = if (hook_present) .{ .ptr = db.async_context, .on_change = DB.notifyAsyncContextVisibilityHook, .on_activity = DB.notifyAsyncContextActivityHook } else null,
            .create = createReplacement,
            .after_create = replacementCreated,
            .prepare = prepareReplacement,
            .restore = restorePrevious,
            .publish = publishOwner,
        }, cfg, options.start_replacement and db.local_execution.optional_runtime_workers_enabled);
        if (!hook_present) db.setQueryVisibilityHook(null);
    }

    fn createReplacement(ptr: *anyopaque, cfg: *db_mod.enrichment_runtime_mod.Config) !?DB.DetachedEnrichmentRuntime {
        const db: *DB = @ptrCast(@alignCast(ptr));
        return db.enrichment.createDetachedRuntime(cfg);
    }

    fn replacementCreated(ptr: *anyopaque) !void {
        const db: *DB = @ptrCast(@alignCast(ptr));
        if (builtin.is_test) if (DB.test_enrichment_reconfigure_after_detached_hook) |hook| try hook(db);
    }

    pub fn prepareReplacement(ptr: *anyopaque, runtime: *db_mod.enrichment_runtime_mod.EnrichmentRuntime, telemetry: ?db_mod.types.EnrichmentStats) !void {
        const db: *DB = @ptrCast(@alignCast(ptr));
        // The previous worker has joined. Rehydrate its final durable debt;
        // volatile telemetry never substitutes for those persisted markers.
        try runtime.reloadDurableState();
        if (telemetry) |previous| runtime.inheritProcessTelemetry(previous);
        try mergeTerminalFailureEnvelope(db.core.batchExecutionResources().store, runtime);
        if (db.core.hasGeneratedEnrichmentTargets()) {
            const target = db.core.nextEnrichmentSequence();
            if (target != 0) runtime.resumeTargetPreservingRetryDebt(target);
        }
    }

    fn restorePrevious(ptr: *anyopaque) !void {
        const db: *DB = @ptrCast(@alignCast(ptr));
        try db.enrichment.restartAfterStructuralMutation("failed enrichment reconfiguration", "");
    }

    pub fn resumeRuntimeAfterReconfigure(self: *Enrichment, operation: []const u8, index_name: []const u8) !void {
        const db = self.dbPtr();
        if (comptime db_mod.storage_engine.enabled) return db_mod.storage_engine.call("resumeRuntimeAfterReconfigure", .{ db, operation, index_name });
        if (self.runtime == null) return;
        try self.restartAfterStructuralMutation(operation, index_name);
    }

    fn startRuntimeForLifecycle(runtime: *db_mod.enrichment_runtime_mod.EnrichmentRuntime) !void {
        if (builtin.is_test) {
            var remaining = DB.test_enrichment_restart_failures_remaining.load(.acquire);
            while (remaining != 0) {
                if (DB.test_enrichment_restart_failures_remaining.cmpxchgWeak(
                    remaining,
                    remaining - 1,
                    .acq_rel,
                    .acquire,
                )) |actual| {
                    remaining = actual;
                    continue;
                }
                return error.TestTransientEnrichmentRestart;
            }
        }
        try runtime.start();
    }

    fn scheduleRestartContext(ctx: *db_mod.AsyncContext, lane: db_mod.background_runtime_mod.DurableJobLane, owner_id: u64) void {
        ctx.enrichment_restart_owner.schedule(.{ .ptr = ctx, .lane = lane, .owner_id = owner_id, .io = ctx.io, .closing = &ctx.background_closing, .wanted = restartWanted, .attempt = attemptRestart, .name = "enrichment", .unavailable = error.EnrichmentRuntimeUnavailable });
    }

    pub fn restartWanted(ptr: *anyopaque) bool {
        const ctx: *db_mod.AsyncContext = @ptrCast(@alignCast(ptr));
        return ctx.enrichment_desired_running.load(.acquire);
    }

    fn attemptRestart(ptr: *anyopaque) !bool {
        const ctx: *db_mod.AsyncContext = @ptrCast(@alignCast(ptr));
        db_mod.lockAtomicWithBackoff(&ctx.enrichment_lifecycle_mutex);
        const runtime = ctx.enrichment_runtime;
        const should_start = runtime != null and ctx.enrichment_desired_running.load(.acquire) and !ctx.background_closing.load(.acquire) and !runtime.?.isStarted();
        const start_error: ?anyerror = if (should_start) blk: {
            startRuntimeForLifecycle(runtime.?) catch |err| break :blk err;
            break :blk null;
        } else null;
        const started = runtime == null or runtime.?.isStarted();
        ctx.enrichment_lifecycle_mutex.unlock();
        if (started) {
            DB.notifyQueryVisibilityHook(ctx, .status);
            return true;
        }
        if (start_error) |err| return err;
        return false;
    }

    pub fn quiesceForStructuralMutation(self: *Enrichment) bool {
        const db = self.dbPtr();
        const desired = db.async_context.enrichment_desired_running.swap(false, .acq_rel);
        db_mod.lockAtomicWithBackoff(&db.async_context.enrichment_lifecycle_mutex);
        defer db.async_context.enrichment_lifecycle_mutex.unlock();
        const runtime = db.async_context.enrichment_runtime orelse return desired;
        const started = runtime.isStarted();
        if (started) runtime.stop();
        return desired or started;
    }

    pub fn restartAfterStructuralMutation(self: *Enrichment, operation: []const u8, index_name: []const u8) !void {
        const db = self.dbPtr();
        // Manual hosts drive enrichment while holding their publication lease.
        // Catalog changes must preserve the worker policy chosen at open.
        if (!db.local_execution.optional_runtime_workers_enabled) return;
        db.async_context.enrichment_desired_running.store(true, .release);
        db_mod.lockAtomicWithBackoff(&db.async_context.enrichment_lifecycle_mutex);
        const runtime = db.async_context.enrichment_runtime orelse {
            db.async_context.enrichment_lifecycle_mutex.unlock();
            return;
        };
        const start_result = if (!runtime.isStarted()) startRuntimeForLifecycle(runtime) else {};
        db.async_context.enrichment_lifecycle_mutex.unlock();
        start_result catch |err| {
            // The durable restart supervisor owns recovery from this point;
            // preserve the diagnostic without classifying a retryable outage
            // as an unrecoverable process failure.
            std.log.warn("failed to restart enrichment runtime after {s} index={s} err={s}", .{ operation, index_name, @errorName(err) });
            scheduleRestartContext(
                db.async_context,
                db.backend_runtime.durable_jobs,
                db.indexes.repair.cleanup_owner_id,
            );
            DB.notifyQueryVisibilityHook(db.async_context, .status);
            return err;
        };
    }

    pub fn failureCoverageMarkerKeyAlloc(
        self: *Enrichment,
        alloc: Allocator,
        issue: db_mod.types.ArtifactRepairIssue,
    ) !?[]u8 {
        const db = self.dbPtr();
        if (issue.reason != .enrichment_failed or issue.index_name.len == 0) return null;
        if (db.core.index_manager.denseIndex(issue.index_name) == null and
            db.core.index_manager.sparseIndex(issue.index_name) == null)
        {
            return null;
        }
        const generation = db.core.index_manager.coverageGenerationForIndex(issue.index_name) orelse return null;
        return try db_mod.internal_keys.derivedCoverageOutcomeKeyAlloc(alloc, issue.index_name, generation, issue.doc_key);
    }

    fn persistedStats(self: *Enrichment) !db_mod.types.EnrichmentStats {
        const db = self.dbPtr();
        if (!db.core.hasGeneratedEnrichmentTargets() and db.core.index_manager.enrichments.items.len == 0) return .{};
        const resources = db.core.batchExecutionResources();
        const checkpoint = try db_mod.enrichment_state.loadProjectionCheckpoint(db.alloc, resources.store, db_mod.enrichment_runtime_mod.scope_name);
        const applied = checkpoint.applied_sequence;
        const status = try db_mod.enrichment_state.loadRuntimeStatus(db.alloc, resources.store, db_mod.enrichment_runtime_mod.scope_name);
        const target = @max(applied, status.target_sequence);
        return .{
            .enabled = target > 0 or status.error_count > 0 or status.retrying or status.worker_failed,
            .target_sequence = target,
            .applied_sequence = applied,
            .projection_checkpoint_status = db_mod.enrichment_state.projectionStatusName(checkpoint.status),
            .projection_checkpoint_applied_sequence = checkpoint.applied_sequence,
            .projection_checkpoint_generation = checkpoint.generation,
            .projection_checkpoint_config_hash = checkpoint.config_hash,
            .checkpoint_replay_tail_sequence_count = target -| checkpoint.applied_sequence,
            .last_error_code = status.last_error_code,
            .error_count = status.error_count,
            .retryable_error_count = status.retryable_error_count,
            .fatal_error_count = status.fatal_error_count,
            .output_counts = status.output_counts,
            .consecutive_retry_count = status.consecutive_retry_count,
            .next_retry_at_ms = status.next_retry_at_ms,
            .retrying = status.retrying,
            .worker_failed = status.worker_failed,
        };
    }

    pub fn runUntil(self: *Enrichment, sequence: u64) !void {
        try self.runUntilWithCancellation(sequence, .none);
    }

    fn runUntilWithCancellation(self: *Enrichment, sequence: u64, cancellation: db_mod.types.CancellationToken) !void {
        try self.runUntilWithVisibilityDeadline(sequence, cancellation, null);
    }

    pub fn runUntilWithVisibilityDeadline(
        self: *Enrichment,
        sequence: u64,
        cancellation: db_mod.types.CancellationToken,
        deadline_ns: ?u64,
    ) !void {
        const db = self.dbPtr();
        if (sequence == 0) return;
        if (self.runtime) |runtime| {
            if (!db.local_execution.optional_runtime_workers_enabled) {
                if (deadline_ns == null and cancellation.ptr == null) {
                    // Explicit maintenance/drain callers historically execute
                    // custom providers inline. They are not request waiters and
                    // retain that source-compatible behavior; API visibility
                    // always supplies the shared finite deadline below.
                    try runtime.catchUpUntil(sequence);
                    return;
                }
                // Foreground ownership is still a post-commit visibility
                // barrier. Honor the borrowed request token and the runtime's
                // hard wait deadline, but never reinterpret cancellation as a
                // rollback of the durable primary mutation.
                try runtime.catchUpUntilWithVisibilityDeadline(sequence, cancellation, deadline_ns);
                return;
            }
            runtime.notifySequence(sequence);
            try runtime.waitForAppliedWithVisibilityDeadline(sequence, cancellation, deadline_ns);
        }
    }

    /// Unlike `runEnrichmentUntilWithVisibilityDeadline`, never substitutes
    /// the request-visibility default (`sync_wait_timeout_ms`, 5 minutes)
    /// when no deadline is supplied, even when `optional_runtime_workers_enabled`
    /// -- worker-mode's `waitForAppliedWithVisibilityDeadline` only passively
    /// waits for a separate background worker to make progress and bounds
    /// that wait unconditionally when `deadline_ns` is null. `runUntilIdle`
    /// (Lite's synchronous ingest drain, in particular) never supplies a
    /// deadline and must run the foreground catch-up pass itself until the
    /// full enrichment backlog clears, however long that legitimately takes,
    /// not silently truncate at 5 minutes and surface a slow-but-progressing
    /// drain as a hard failure. Scoped to `runUntilIdle`'s own drain call
    /// (via `ReplayDrainOptions.unbounded_enrichment_wait`) rather than
    /// `runEnrichmentUntilWithVisibilityDeadline` generally: other callers
    /// (plain `runEnrichmentUntil`, `runMaintenanceUntil`, request-visibility
    /// barriers) still rely on the passive bounded wait deferring to that
    /// background worker, and forcing them through inline foreground
    /// execution instead changed observable enrichment-worker ownership
    /// behavior (see the `TestLiteHostedPauseResumeGeneratedEnrichment`
    /// regression this scoping fixes).
    fn runUntilForDrainUnbounded(self: *Enrichment, sequence: u64) !void {
        if (sequence == 0) return;
        const runtime = self.runtime orelse return;
        runtime.notifySequence(sequence);
        try runtime.catchUpUntilForDrain(sequence);
    }

    pub fn runUntilForDrain(self: *Enrichment, sequence: u64, options: DB.ReplayDrainOptions) !void {
        const db = self.dbPtr();
        while (true) {
            const outcome = if (options.unbounded_enrichment_wait and options.cancellation.ptr == null)
                self.runUntilForDrainUnbounded(sequence)
            else
                self.runUntilWithCancellation(sequence, options.cancellation);
            outcome catch |err| switch (err) {
                error.EnrichmentRetryInProgress => {
                    if (!options.wait_for_enrichment_retries) return err;
                    const io = db.backend_runtime.io() orelse return error.MissingBackendRuntimeIo;
                    try io.sleep(.fromMilliseconds(25), .awake);
                    continue;
                },
                else => return err,
            };
            return;
        }
    }

    pub fn markAppliedIfNoPendingThrough(self: *Enrichment, sequence: u64) !void {
        const db = self.dbPtr();
        if (sequence == 0) return;
        const runtime = self.runtime orelse return;
        const runtime_stats = runtime.stats();
        if (runtime_stats.applied_sequence >= sequence) return;
        if (try db.noPendingEnrichmentReplayThrough(runtime_stats.applied_sequence, sequence)) {
            try runtime.markAppliedThrough(sequence);
        }
    }

    pub fn throughputSnapshot(self: *Enrichment) DB.EnrichmentThroughputSnapshot {
        const runtime = self.runtime orelse return .{};
        return .{
            .embed_batches = runtime.embed_batches_completed,
            .embed_items = runtime.embed_items_completed,
            .embed_ns = runtime.total_embed_ns,
            .extract_batches = runtime.extract_batches_completed,
            .extract_items = runtime.extract_items_completed,
            .extract_ns = runtime.total_extract_ns,
        };
    }

    pub fn statsWithSupervisorState(self: *Enrichment, fallback: db_mod.types.EnrichmentStats) db_mod.types.EnrichmentStats {
        const db = self.dbPtr();
        // Status is called while the DB apply lock is held. Structural
        // mutations hold lifecycle while stopping and joining the enrichment
        // worker, and that worker can be waiting for apply. Never close the
        // lifecycle -> worker join -> apply -> lifecycle cycle here.
        const lifecycle_locked = db.async_context.enrichment_lifecycle_mutex.tryLock();
        defer if (lifecycle_locked) db.async_context.enrichment_lifecycle_mutex.unlock();
        var enrichment_status = if (lifecycle_locked)
            if (db.async_context.enrichment_runtime) |runtime|
                runtime.stats()
            else
                self.persistedStats() catch fallback
        else
            fallback;
        const desired = db.async_context.enrichment_desired_running.load(.acquire);
        // `desired` is the lock-free source of truth for whether this DB owns
        // an enrichment producer. A retained query-only snapshot may have
        // `enabled=false`, and lifecycle contention prevents us from replacing
        // it with runtime.stats(). Never let that observation gap turn a live
        // or restarting producer into a disabled one: public readiness treats
        // enabled + retrying as transient debt and remains fail-closed until a
        // later poll can observe the worker directly.
        if (desired) enrichment_status.enabled = true;
        const started = if (lifecycle_locked)
            if (db.async_context.enrichment_runtime) |runtime| runtime.isStarted() else false
        else
            false;
        if (desired and !started) {
            // While lifecycle owns the runtime pointer, expose the persisted
            // snapshot as a supervised transition rather than a fatal worker.
            const supervised = !lifecycle_locked or db.async_context.enrichment_restart_owner.state.load(.acquire) != 0;
            enrichment_status.retrying = supervised;
            enrichment_status.worker_failed = !supervised;
            enrichment_status.projection_checkpoint_status = if (supervised) "retrying" else "failed";
        }
        return enrichment_status;
    }

    pub fn clearEmbeddingActivityCache(self: *Enrichment) void {
        const db = self.dbPtr();
        self.embedding_activity.clear(db.alloc);
    }

    /// Atomic managed admission normally defers the canonical derived worker
    /// until shadow activation. The canonical generation can nevertheless
    /// become independently serviceable while enrichment drains its durable
    /// requests. Before retiring that admission intent, attach the ordinary
    /// worker at the durable publication cursor so future writes continue to
    /// advance the generation. Intent removal is deliberately ordered after
    /// this fallible operation: a failed attachment remains durable repair
    /// debt and is retried by the same single owner.
    pub fn ensureManagedAdmissionCanonicalWorker(
        self: *Enrichment,
        alloc: Allocator,
        intent: db_mod.index_repair_state.IndexRepairIntent,
    ) !void {
        const db = self.dbPtr();
        if (!db.start_index_workers or db.executor.appliedSequence(intent.index_name) != null) return;

        const configs = try db.core.listIndexes(alloc);
        defer db_mod.types.freeIndexConfigs(alloc, configs);
        const cfg = for (configs) |candidate| {
            if (std.mem.eql(u8, candidate.name, intent.index_name)) break candidate;
        } else return error.IndexNotFound;
        if (cfg.kind != intent.kind or db_mod.types.indexConfigHash(cfg) != intent.config_hash) {
            return error.IndexConfigMismatch;
        }

        const applied = try db.core.loadAppliedSequence(alloc, intent.index_name);
        try db.executor.addWorker(intent.index_name, .{
            .name = intent.index_name,
            .kind = intent.kind,
        }, applied);
        const current_target = db.core.nextDerivedSequence();
        if (current_target > applied) db.executor.notifyIndexes(current_target, &.{intent.index_name});
    }

    pub fn requestManagedAdmissionMaterialization(self: *Enrichment) void {
        self.managed_admission.request();
    }

    pub fn reconcileArtifactDeleteEnrichment(self: *Enrichment, kind: db_mod.types.EnrichmentKind, name: []const u8, context: @import("../artifact_reconcile_intent.zig").Context) !bool {
        return self.deleteEnrichmentForReconciliation(kind, name, context);
    }

    pub fn reconcileArtifactAddEnrichment(self: *Enrichment, cfg: db_mod.types.EnrichmentConfig, context: @import("../artifact_reconcile_intent.zig").Context) !void {
        const db = self.dbPtr();
        if (comptime db_mod.storage_engine.enabled) return db_mod.storage_engine.call("reconcileArtifactAddEnrichment", .{ db, cfg, context });
        if (db_mod.openModeRequiresReadOnlyBackends(db.open_mode)) return error.ReadOnly;
        var replication_mutation = db.acquireReplicationMutationShared();
        defer if (replication_mutation) |*lease| lease.release();
        // Exact committed reconciliation provenance authorizes standby
        // materialization; no public/local configuration bypass is exposed.
        try db.lockApplyForPortableRuntime();
        defer db.core.unlockApply();
        {
            var probe = try db.core.store.beginProbeTxn();
            defer probe.abort();
            try db.requireOnlineArtifactCatalogMutableWithContextLocked(&probe, context);
        }
        try db.enforceVectorMigrationConfigurationGate();
        _ = try db.core.upsertEnrichment(cfg);
    }

    pub fn addEnrichment(self: *Enrichment, cfg: db_mod.types.EnrichmentConfig) !void {
        const db = self.dbPtr();
        if (comptime db_mod.storage_engine.enabled) return db_mod.storage_engine.call("addEnrichment", .{ db, cfg });
        if (db_mod.openModeRequiresReadOnlyBackends(db.open_mode)) return error.ReadOnly;
        var replication_mutation = db.acquireReplicationMutationShared();
        defer if (replication_mutation) |*lease| lease.release();
        try db.enforceReplicationWriteGate();
        try db.lockApplyForPortableRuntime();
        defer db.core.unlockApply();
        {
            var probe = try db.core.store.beginProbeTxn();
            defer probe.abort();
            try db.requireOnlineArtifactCatalogMutableLocked(&probe);
        }
        try db.enforceVectorMigrationConfigurationGate();
        try db.enforceReplicationWriteGate();
        try db.core.addEnrichment(cfg);
    }

    pub fn upsertEnrichment(self: *Enrichment, cfg: db_mod.types.EnrichmentConfig) !db_mod.index_manager_mod.IndexManager.EnrichmentUpsertResult {
        const db = self.dbPtr();
        if (comptime db_mod.storage_engine.enabled) return db_mod.storage_engine.call("upsertEnrichment", .{ db, cfg });
        if (db_mod.openModeRequiresReadOnlyBackends(db.open_mode)) return error.ReadOnly;
        var replication_mutation = db.acquireReplicationMutationShared();
        defer if (replication_mutation) |*lease| lease.release();
        try db.enforceReplicationWriteGate();
        try db.lockApplyForPortableRuntime();
        defer db.core.unlockApply();
        {
            var probe = try db.core.store.beginProbeTxn();
            defer probe.abort();
            try db.requireOnlineArtifactCatalogMutableLocked(&probe);
        }
        try db.enforceVectorMigrationConfigurationGate();
        try db.enforceReplicationWriteGate();
        return try db.core.upsertEnrichment(cfg);
    }

    pub fn getEnrichment(self: *Enrichment, alloc: Allocator, kind: db_mod.types.EnrichmentKind, name: []const u8) !?db_mod.types.EnrichmentConfig {
        const db = self.dbPtr();
        if (comptime db_mod.storage_engine.enabled) return db_mod.storage_engine.call("getEnrichment", .{ db, alloc, kind, name });
        try db.lockApplySharedForPortableRuntime();
        defer db.core.unlockApplyShared();
        return try db.core.getEnrichment(alloc, kind, name);
    }

    pub fn listEnrichments(self: *Enrichment, alloc: Allocator) ![]db_mod.types.EnrichmentConfig {
        const db = self.dbPtr();
        if (comptime db_mod.storage_engine.enabled) return db_mod.storage_engine.call("listEnrichments", .{ db, alloc });
        try db.lockApplySharedForPortableRuntime();
        defer db.core.unlockApplyShared();
        return try db.core.listEnrichments(alloc);
    }

    pub fn extractEnrichments(self: *Enrichment, alloc: Allocator, writes: []const db_mod.types.BatchWrite) !db_mod.types.ExtractEnrichmentsResult {
        const db = self.dbPtr();
        try db.lockApplySharedForPortableRuntime();
        defer db.core.unlockApplyShared();

        var collector: db_mod.result_collectors.Extraction = .{ .alloc = alloc };
        defer collector.deinit();
        for (writes) |write| {
            var extracted = try db_mod.mapper.extractWrite(alloc, write.key, write.value);
            defer extracted.deinit(alloc);
            try db_mod.augmentExtractedWriteWithGraphFieldEdges(db, alloc, write.key, write.value, &extracted);
            try collector.append(write.key, extracted);
        }
        return collector.finish();
    }

    pub fn computeEnrichments(self: *Enrichment, alloc: Allocator, writes: []const db_mod.types.BatchWrite) !db_mod.types.ComputeEnrichmentsResult {
        const db = self.dbPtr();
        try db.lockApplySharedForPortableRuntime();
        defer db.core.unlockApplyShared();

        var collector: db_mod.result_collectors.Enrichments = .{ .alloc = alloc };
        defer collector.deinit();

        for (writes) |write| {
            var extracted = try db_mod.mapper.extractWrite(alloc, write.key, write.value);
            defer extracted.deinit(alloc);
            try db_mod.augmentExtractedWriteWithGraphFieldEdges(db, alloc, write.key, write.value, &extracted);
            const cleaned = extracted.cleaned_value orelse continue;

            var explicit_dense = std.ArrayListUnmanaged(db_mod.types.EnrichmentDenseEmbeddingWrite).empty;
            defer {
                for (explicit_dense.items) |*embedding| embedding.deinit(alloc);
                explicit_dense.deinit(alloc);
            }
            for (extracted.dense_embeddings) |embedding| {
                try db_mod.appendClonedExplicitDenseEmbedding(
                    alloc,
                    &explicit_dense,
                    embedding.index_name,
                    embedding.doc_key,
                    embedding.vector,
                );
            }
            var explicit_sparse = std.ArrayListUnmanaged(db_mod.types.EnrichmentSparseEmbeddingWrite).empty;
            defer {
                for (explicit_sparse.items) |*embedding| embedding.deinit(alloc);
                explicit_sparse.deinit(alloc);
            }
            for (extracted.sparse_embeddings) |embedding| {
                try db_mod.appendClonedExplicitSparseEmbedding(
                    alloc,
                    &explicit_sparse,
                    embedding.index_name,
                    embedding.doc_key,
                    embedding.indices,
                    embedding.values,
                );
            }

            const generated = try db.core.planGeneratedEnrichments(
                alloc,
                write.key,
                cleaned,
                explicit_dense.items,
                explicit_sparse.items,
            );
            defer db_mod.enrichment_types.deinitGeneratedRequests(alloc, generated);

            var chunk_cache = std.ArrayListUnmanaged(db_mod.ChunkCacheEntry).empty;
            defer {
                for (chunk_cache.items) |entry| {
                    alloc.free(entry.key);
                    db_mod.chunker_mod.freeChunks(alloc, entry.chunks);
                }
                chunk_cache.deinit(alloc);
            }

            for (generated) |request| {
                switch (request.kind) {
                    .chunk_text => try db_mod.computeChunkRequest(alloc, db, cleaned, request, &collector.artifacts, &collector.documents, &chunk_cache),
                    .dense_embedding => db_mod.computeDenseRequest(alloc, db, cleaned, request, &collector.artifacts, &collector.dense, &chunk_cache) catch |err| switch (err) {
                        error.MissingDenseEmbedder => try db_mod.appendUniqueOwnedKey(alloc, &collector.failed, write.key),
                        else => return err,
                    },
                    else => {},
                }
            }
        }

        return try collector.finish();
    }

    pub fn deleteEnrichment(self: *Enrichment, kind: db_mod.types.EnrichmentKind, name: []const u8) !bool {
        const db = self.dbPtr();
        if (comptime db_mod.storage_engine.enabled) return db_mod.storage_engine.call("deleteEnrichment", .{ db, kind, name });
        return self.deleteEnrichmentForReconciliation(kind, name, null);
    }

    fn deleteEnrichmentForReconciliation(self: *Enrichment, kind: db_mod.types.EnrichmentKind, name: []const u8, context: ?@import("../artifact_reconcile_intent.zig").Context) !bool {
        const db = self.dbPtr();
        if (db_mod.openModeRequiresReadOnlyBackends(db.open_mode)) return error.ReadOnly;
        var replication_mutation = db.acquireReplicationMutationShared();
        defer if (replication_mutation) |*lease| lease.release();
        if (context == null) try db.enforceReplicationWriteGate();
        try db.lockApplyForPortableRuntime();
        defer db.core.unlockApply();
        {
            var probe = try db.core.store.beginProbeTxn();
            defer probe.abort();
            try db.requireOnlineArtifactCatalogMutableWithContextLocked(&probe, context);
        }
        try db.enforceVectorMigrationConfigurationGate();
        return try db.core.deleteEnrichment(kind, name);
    }

    pub fn runArtifactRepairMetadataMaintenancePass(self: *Enrichment) !bool {
        const db = self.dbPtr();
        if (db_mod.openModeRequiresReadOnlyBackends(db.open_mode)) return false;
        // The mark owns immutable primary/ANN/source leases. Scan before
        // taking apply; only setup, planning, and publication need that fence.
        if (db.local_execution.source_vectors.load(.acquire)) |source| try source.advanceMarkingSnapshot();
        return self.runArtifactRepairMetadataMaintenanceAfterScan();
    }

    pub fn runArtifactRepairMetadataMaintenanceAfterScan(self: *Enrichment) !bool {
        const db = self.dbPtr();
        try db.lockApplyForPortableRuntime();
        defer db.core.unlockApply();

        var more = try db.core.index_manager.runGraphOwnershipCleanupStep();
        more = (try db.rebuildArtifactRepairSummaryIfMissing(db.alloc)) or more;
        more = (try db.rebuildArtifactRepairKindIndexIfMissing(db.alloc)) or more;
        if (db.local_execution.source_vectors.load(.acquire)) |source| {
            const step_bytes = source.backgroundCollectionStepBytes();
            if (step_bytes != 0) {
                try db.refreshSourceVectorOwnershipScopes();
                const prior_generation = source.currentGeneration();
                if (try source.collectBackgroundStepDeferredMark(&db.core.store.runtime_store, step_bytes)) {
                    if (source.currentGeneration() != prior_generation) try db.core.index_manager.refreshSourcePayloadGeneration();
                }
                if (source.collectionPending()) more = true;
            }
        }
        return more;
    }

    pub fn runArtifactRepairMetadataMaintenanceUntilIdle(self: *Enrichment) !void {
        const db = self.dbPtr();
        if (comptime db_mod.storage_engine.enabled) return db_mod.storage_engine.call("runArtifactRepairMetadataMaintenanceUntilIdle", .{db});
        while (try self.runArtifactRepairMetadataMaintenancePass()) {}
    }

    /// Start only after the DB has reached its final address. DB.open returns
    /// by value, so cache owners invoke this after installing that value in a
    /// stable heap entry rather than letting an async task capture the open
    /// function's temporary stack address.
    pub fn startArtifactRepairMetadataWorkerIfNeeded(self: *Enrichment) void {
        const db = self.dbPtr();
        if (!db.start_independent_maintenance) return;
        if (comptime builtin.single_threaded or builtin.os.tag == .freestanding) return;
        // Ordinary unit tests drive maintenance explicitly. Borrowed runtimes
        // schedule the production owner deterministically, including VOPR's
        // test-artifact runner; disabling it strands durable graph cleanup.
        if (comptime builtin.is_test) {
            if (!db.backend_runtime.usesBorrowedIo()) return;
        }
        if (!db.start_index_workers) return;
        if (db_mod.openModeRequiresReadOnlyBackends(db.open_mode)) return;
        db.maintenance.independent.start(db.independentMaintenancePort());
    }

    pub fn stopArtifactRepairMetadataWorker(self: *Enrichment) void {
        const db = self.dbPtr();
        db.maintenance.independent.stop(db.backend_runtime);
    }

    /// One bounded physical-key page, never under the DB apply lock. Both
    /// discovery and the ordinary maintenance scheduler advance the same
    /// durable cursor, so large old roots do not require repeated client calls.
    pub fn advanceArtifactFootprintPage(self: *Enrichment) !bool {
        const db = self.dbPtr();
        if (comptime db_mod.storage_engine.enabled) return db_mod.storage_engine.call("advanceArtifactFootprintPage", .{db});
        if (db_mod.openModeRequiresReadOnlyBackends(db.open_mode)) return true;
        var budget = if (db.core.index_manager.resource_manager) |manager|
            db_mod.resource_manager_mod.BudgetedAllocator.init(manager, .relational_preparation_working_set, db.alloc, 1)
        else
            null;
        defer if (budget) |*value| value.deinit();
        const scratch = if (budget) |*value| value.allocator() else db.alloc;
        const complete = @import("../../artifact_footprint.zig").reconcilePage(scratch, db.core.store) catch |err| {
            if (err == error.OutOfMemory) if (budget) |*value| if (value.denied()) return error.ResourceBudgetExceeded;
            return err;
        };
        self.artifacts.footprint_pending.store(!complete, .release);
        return complete;
    }

    /// Append root-producer requests using the existing durable replay journal.
    /// The cursor is only a fairness hint: acceptance and sealing remain ordered
    /// operations, and restart can safely rediscover every unscheduled row.
    pub fn advanceArtifactProducerWorkPage(self: *Enrichment) !bool {
        const db = self.dbPtr();
        if (db_mod.openModeRequiresReadOnlyBackends(db.open_mode)) return false;
        return self.artifacts.producer_scheduler.advance(.{ .ptr = db, .now = DB.producerMaintenanceNow, .page = advanceArtifactProducerBudgetedPage });
    }

    fn advanceArtifactProducerBudgetedPage(ptr: *anyopaque) !bool {
        const db: *DB = @ptrCast(@alignCast(ptr));
        var budget = if (db.core.index_manager.resource_manager) |manager|
            db_mod.resource_manager_mod.BudgetedAllocator.init(manager, .relational_preparation_working_set, db.alloc, 1)
        else
            null;
        defer if (budget) |*value| value.deinit();
        const scratch = if (budget) |*value| value.allocator() else db.alloc;
        return db.enrichment.advanceArtifactProducerWorkPageWithAllocator(scratch) catch |err| {
            if (err == error.OutOfMemory) if (budget) |*value| if (value.denied()) return error.ResourceBudgetExceeded;
            return err;
        };
    }

    fn advanceArtifactProducerWorkPageWithAllocator(self: *Enrichment, scratch: Allocator) !bool {
        const db = self.dbPtr();
        const has_ordered_authority = blk: {
            var read = try db.core.store.beginReadTxn();
            defer read.abort();
            break :blk try @import("../artifact_publication.zig").authority(&read) != null;
        };
        if (!has_ordered_authority) {
            if (db.maintenance.local_source.completion_audit_pending) {
                _ = try db.ensureGeneratedCoverageRecoveryIntents(scratch);
                db.maintenance.local_source.completion_audit_pending = false;
            }
            return false;
        }
        var plan = try db.core.index_manager.acquireWritePlanSnapshot();
        defer plan.release();
        const readiness_progress = try @import("../artifact_producer_readiness.zig").advance(scratch, db.core.store, db.root_incarnation, plan.plan());
        const obligations = @import("../artifact_producer_obligations.zig");
        var page = blk: {
            var read = try db.core.store.beginReadTxnWithBlockCacheAdmission(.transient);
            defer read.abort();
            const authority = (try @import("../artifact_publication.zig").authority(&read)) orelse return false;
            // Protocol-14 owners have no required-stream work registry.
            const state = (try obligations.load(&read)) orelse {
                var ordered = (try @import("../artifact_inventory.zig").load(scratch, &read)) orelse return error.ArtifactCatalogDrift;
                defer ordered.deinit();
                if (ordered.value.command.binding.effect_protocol >= 15) return error.ArtifactCatalogCorrupt;
                const pending = try @import("../artifact_producer_readiness.zig").pending(scratch, db.core.store, db.root_incarnation);
                self.artifacts.producer_scheduler.completeReadinessPage(db.alloc, pending, readiness_progress, db.independentMaintenanceNowNs());
                return readiness_progress;
            };
            try state.requireAuthority(authority);
            if (state.sealed_attempt != null) return false;
            const catalogs = try @import("../artifact_inventory.zig").catalogs(&read);
            if (!plan.plan().matchesArtifactInventory(catalogs) or !std.mem.eql(u8, &catalogs.digest(), &authority.catalog_digest)) return error.ArtifactCatalogDrift;
            const cursor = self.artifacts.producer_scheduler.cursorForAuthority(db.alloc, authority);
            break :blk try obligations.scanWork(scratch, &read, authority, cursor);
        };
        defer page.deinit();
        const dispatch = @import("../artifact_producer_dispatch.zig");
        const retry = @import("../artifact_producer_retry.zig");
        const retry_now = db.independentMaintenanceNowNs();
        self.artifacts.producer_scheduler.observeAuthority(page.authority);
        const retry_real_now: u64 = @intCast(@max(0, std.Io.Timestamp.now(db.core.index_manager.checkpointIo(), .real).toNanoseconds()));
        // Persisted wall time selects retries only. It prevents cold-owner
        // churn from restarting the grace period forever; runtime deadlines
        // still use the owner's awake clock and cannot discharge any work.
        if (self.artifacts.producer_scheduler.needsRetryDelay()) {
            var read = try db.core.store.beginReadTxn();
            defer read.abort();
            self.artifacts.producer_scheduler.setRetryDelay(retry_now, try obligations.retryDelay(&read, page.authority, retry_real_now, retry.interval_ns));
        }
        if (self.artifacts.producer_scheduler.needsRetryRound(retry_now, page.items.len != 0, plan.plan().generated_templates.len != 0)) {
            try db.lockApplyForPortableRuntime();
            defer db.core.unlockApply();
            var txn = try db.core.store.beginWriteTxn();
            errdefer txn.abort();
            const round = try obligations.beginRetryRoundAt(&txn, page.authority, retry_real_now);
            try txn.commit();
            self.artifacts.producer_scheduler.startRound(page.authority, round);
        }
        var refs: dispatch.Buffer = undefined;
        const deadline = db_mod.platform_time.monotonicNs() +| @import("../artifact_producer_scheduler.zig").Scheduler.quantum_ns;
        var processed: usize = 0;
        var more_dispatch = false;
        for (page.items) |item| {
            if (processed != 0 and db_mod.platform_time.monotonicNs() >= deadline) break;
            // Allocate the next fairness cursor before committing, so allocation
            // failure cannot strand an admitted request behind a stale cursor.
            const next = try db.alloc.dupe(u8, item.document);
            errdefer db.alloc.free(next);
            // Native verification is local, resumable evidence, not discharge.
            // It also runs for catalogs with no generated provider templates.
            const native_progress = try @import("../artifact_native_stream.zig").advance(scratch, db.core.store, db.root_incarnation, item.document, plan.plan());
            more_dispatch = more_dispatch or native_progress == .progress;
            if (native_progress == .closed) {
                _ = try db.advanceArtifactUnitReceiptPage(scratch, item.document, plan.plan());
                try db.enqueueArtifactCompletion(scratch, item.document, plan.plan());
            }
            if (!item.dispatch_complete and plan.plan().generated_templates.len != 0) {
                const requests = try dispatch.prepare(plan.plan().generated_templates, item.next_template, item.document, &refs);
                var ctx = db.batchContext();
                _ = try db_mod.appendDerivedBatchRecordContextWithWork(&ctx, .{ .generated_enrichment_refs = requests.items }, false, .{
                    .authority = page.authority,
                    .item = item,
                    .plan_generation = plan.generation(),
                    .page = requests.progress,
                });
                more_dispatch = more_dispatch or !requests.progress.complete;
            }
            if (self.artifacts.producer_scheduler.retryPage(item, plan.plan().generated_templates.len != 0)) |round| {
                const requests = blk: {
                    var read = try db.core.store.beginReadTxnWithBlockCacheAdmission(.transient);
                    defer read.abort();
                    break :blk try retry.prepare(scratch, &read, db.root_incarnation, plan.plan(), round.first_template, item.document, &refs);
                };
                const required: db_mod.RequiredProducerDispatch = .{ .authority = page.authority, .item = item, .plan_generation = plan.generation(), .page = requests.progress, .retry_round = round.number };
                if (requests.items.len != 0) {
                    var ctx = db.batchContext();
                    _ = try db_mod.appendDerivedBatchRecordContextWithWork(&ctx, .{ .generated_enrichment_refs = requests.items }, false, required);
                } else {
                    // No empty replay record for an already accepted page.
                    // This advances scheduling only, not completion credit.
                    try db.lockApplyForPortableRuntime();
                    defer db.core.unlockApply();
                    if (db.core.index_manager.writePlanGeneration() != required.plan_generation) return error.EnrichmentSourceChanged;
                    var txn = try db.core.store.beginWriteTxn();
                    errdefer txn.abort();
                    if (!try obligations.stageRetry(scratch, &txn, page.authority, item, round.number, null, requests.progress)) return error.EnrichmentSourceChanged;
                    try txn.commit();
                }
                self.artifacts.producer_scheduler.noteRetryProgress(requests.progress.complete);
            }
            self.artifacts.producer_scheduler.commitCursor(db.alloc, page.authority, next);
            processed += 1;
        }
        const readiness_pending = try @import("../artifact_producer_readiness.zig").pending(scratch, db.core.store, db.root_incarnation);
        self.artifacts.producer_scheduler.completeWorkPage(db.alloc, processed == page.items.len and page.at_end, more_dispatch, .{ .pending = readiness_pending, .progressed = readiness_progress }, db.independentMaintenanceNowNs());
        return processed != 0 or readiness_progress;
    }

    pub fn advanceArtifactProducerBaselinePage(self: *Enrichment) !bool {
        const db = self.dbPtr();
        if (db_mod.openModeRequiresReadOnlyBackends(db.open_mode)) return true;
        var budget = if (db.core.index_manager.resource_manager) |manager|
            db_mod.resource_manager_mod.BudgetedAllocator.init(manager, .relational_preparation_working_set, db.alloc, 1)
        else
            null;
        defer if (budget) |*value| value.deinit();
        const scratch = if (budget) |*value| value.allocator() else db.alloc;
        return self.advanceArtifactProducerBaselinePageWithAllocator(scratch) catch |err| {
            if (err == error.OutOfMemory) if (budget) |*value| if (value.denied()) return error.ResourceBudgetExceeded;
            return err;
        };
    }

    fn advanceArtifactProducerBaselinePageWithAllocator(self: *Enrichment, scratch: Allocator) !bool {
        const db = self.dbPtr();
        const publication_mode = blk: {
            var read = try db.core.store.beginReadTxn();
            defer read.abort();
            if (try @import("../artifact_publication.zig").authority(&read) == null) {
                self.artifacts.producer_baseline_pending.store(false, .release);
                return true;
            }
            const owner = (try @import("../../source_authority.zig").publicationOwner(&read)) orelse return error.ArtifactCatalogDrift;
            break :blk owner.mode;
        };
        const retired_work = try @import("../artifact_producer_obligations.zig").collectObsoleteWorkPage(scratch, db.core.store);
        const retired_proofs = try @import("../artifact_producer_provenance.zig").collectObsoletePage(scratch, db.core.store);
        const retired_checkpoints = try @import("../artifact_stream_checkpoint.zig").collectObsoletePage(scratch, db.core.store, db.root_incarnation);
        const retired_streams = try @import("../artifact_stream_progress.zig").collectObsoletePage(scratch, db.core.store, db.root_incarnation);
        const retired_completions = try @import("../artifact_completion_progress.zig").collectObsoletePage(scratch, db.core.store, db.root_incarnation);
        const retired_native = try @import("../artifact_native_stream.zig").collectObsoletePage(scratch, db.core.store, db.root_incarnation);
        const retired_inventories = try @import("../artifact_chunk_inventory_progress.zig").collectObsoletePage(scratch, db.core.store, db.root_incarnation);
        const retired_units = try @import("../artifact_unit_progress.zig").collectObsoletePage(scratch, db.core.store, db.root_incarnation);
        const retired_unit_jobs = try @import("../artifact_unit_jobs.zig").collectObsoletePage(scratch, db.core.store, db.root_incarnation);
        const retired_authored = try @import("../artifact_authored_acceptance.zig").collectObsoletePage(scratch, db.core.store, db.root_incarnation);
        const swept_authored = try @import("../artifact_authored_acceptance.zig").collectCurrentPage(scratch, db.core.store, db.root_incarnation);
        const retired = retired_work and retired_proofs and retired_checkpoints and retired_streams and retired_completions and retired_native and retired_inventories and retired_units and retired_unit_jobs and retired_authored and swept_authored;
        if (publication_mode == .ordered) {
            var prepared = (try @import("../artifact_producer_baseline.zig").prepareRaft(scratch, db.core.store)) orelse {
                var validation = (try @import("../artifact_producer_validation.zig").prepareRaft(scratch, db.core.store)) orelse {
                    self.artifacts.producer_baseline_pending.store(!retired, .release);
                    return retired;
                };
                defer validation.deinit();
                const dispatcher = db.local_execution.artifact_publication_dispatcher orelse {
                    self.artifacts.producer_baseline_pending.store(!retired, .release);
                    return false;
                };
                const encoded = try @import("../artifact_publication_transport_codec.zig").encodeAlloc(scratch, validation.command);
                defer scratch.free(encoded);
                try dispatcher.enqueue(dispatcher.ptr, validation.command.namespace, encoded);
                self.artifacts.producer_baseline_pending.store(true, .release);
                return false;
            };
            defer prepared.deinit();
            const dispatcher = db.local_execution.artifact_publication_dispatcher orelse {
                self.artifacts.producer_baseline_pending.store(!retired, .release);
                return false;
            };
            const encoded = try @import("../artifact_publication_transport_codec.zig").encodeAlloc(scratch, prepared.command);
            defer scratch.free(encoded);
            try dispatcher.enqueue(dispatcher.ptr, prepared.command.namespace, encoded);
            // Queue admission is not a baseline certificate. The next pass
            // observes only progress installed by the replicated transaction.
            self.artifacts.producer_baseline_pending.store(true, .release);
            return false;
        }
        const complete = try @import("../artifact_producer_baseline.zig").page(scratch, db.core.store);
        self.artifacts.producer_baseline_pending.store(!complete or !retired, .release);
        return complete and retired;
    }

    pub fn appendGeneratedEnrichmentsFromStoredDocsPage(
        self: *Enrichment,
        alloc: Allocator,
        force_generated_artifact_names: []const []const u8,
        skip_terminally_covered: bool,
        resume_key: ?[]const u8,
        limit: usize,
        options: DB.GeneratedEnrichmentReplayScanOptions,
    ) !DB.GeneratedEnrichmentReplayPageResult {
        const db = self.dbPtr();
        if (self.runtime == null) return .{ .complete = true };
        if (limit == 0) return error.InvalidArgument;

        const initial_lower = if (resume_key == null) try db.core.documentRangeLowerAlloc("") else null;
        defer if (initial_lower) |key| db.core.alloc.free(key);
        const lower = resume_key orelse initial_lower.?;
        var replay_batch = try db.collectStoredGeneratedReplayBatch(alloc, lower, resume_key != null, limit, options);
        defer replay_batch.deinit(alloc);

        var pending_batch = db_mod.derived_types.DerivedBatch{};
        defer db_mod.derived_types.deinitDerivedBatch(db.alloc, &pending_batch);
        try db_mod.appendGeneratedEnrichments(db, &pending_batch, .{
            .writes = replay_batch.writes,
            .sync_level = .write,
        }, replay_batch.extracted, force_generated_artifact_names, skip_terminally_covered);
        var target_sequence: u64 = 0;
        if (pending_batch.generated_enrichment_refs.len != 0) {
            target_sequence = try db_mod.appendDerivedBatchRecord(db, pending_batch);
            db.executor.notifySequence(target_sequence);
            if (self.runtime) |runtime| runtime.notifySequence(target_sequence);
            db.notifyResolverReplayRuntimes(target_sequence);
        }

        const complete = replay_batch.complete;
        const next_resume_key = if (complete)
            null
        else blk: {
            const key = replay_batch.next_lower orelse break :blk if (resume_key) |previous| try alloc.dupe(u8, previous) else null;
            replay_batch.next_lower = null;
            break :blk key;
        };
        return .{
            .generated_ref_count = pending_batch.generated_enrichment_refs.len,
            .target_sequence = target_sequence,
            .documents_scanned = replay_batch.writes.len,
            .next_resume_key = next_resume_key,
            .complete = complete,
        };
    }

    fn appendGeneratedEnrichmentsFromStoredDocs(
        self: *Enrichment,
        alloc: Allocator,
        force_generated_artifact_names: []const []const u8,
        skip_terminally_covered: bool,
    ) !DB.GeneratedEnrichmentReplayAppendResult {
        const db = self.dbPtr();
        if (self.runtime == null) return .{};

        const chunk_size: usize = 128;
        const initial_lower = try db.core.documentRangeLowerAlloc("");
        defer db.core.alloc.free(initial_lower);
        var lower = try alloc.dupe(u8, initial_lower);
        defer alloc.free(lower);
        var lower_exclusive = false;

        var generated_ref_count: usize = 0;
        var latest_sequence: u64 = 0;
        while (true) {
            var replay_batch = try db.collectStoredGeneratedReplayBatch(alloc, lower, lower_exclusive, chunk_size, .{
                .max_records = std.math.maxInt(usize),
                .max_bytes = std.math.maxInt(usize),
            });
            defer replay_batch.deinit(alloc);
            if (replay_batch.writes.len == 0) break;

            var pending_batch = db_mod.derived_types.DerivedBatch{};
            defer db_mod.derived_types.deinitDerivedBatch(db.alloc, &pending_batch);
            try db_mod.appendGeneratedEnrichments(db, &pending_batch, .{
                .writes = replay_batch.writes,
                .sync_level = .write,
            }, replay_batch.extracted, force_generated_artifact_names, skip_terminally_covered);
            if (pending_batch.generated_enrichment_refs.len != 0) {
                generated_ref_count += pending_batch.generated_enrichment_refs.len;
                const sequence = try db_mod.appendDerivedBatchRecord(db, pending_batch);
                latest_sequence = @max(latest_sequence, sequence);
            }

            const next_lower = replay_batch.next_lower orelse break;
            replay_batch.next_lower = null;
            alloc.free(lower);
            lower = next_lower;
            lower_exclusive = true;
        }
        if (latest_sequence != 0) {
            db.executor.notifySequence(latest_sequence);
            if (self.runtime) |runtime| runtime.notifySequence(latest_sequence);
            db.notifyResolverReplayRuntimes(latest_sequence);
        }
        return .{
            .generated_ref_count = generated_ref_count,
            .target_sequence = latest_sequence,
        };
    }

    pub fn replayGeneratedEnrichmentsFromStoredDocs(self: *Enrichment, alloc: Allocator) !usize {
        const db = self.dbPtr();
        if (comptime db_mod.storage_engine.enabled) return db_mod.storage_engine.call("replayGeneratedEnrichmentsFromStoredDocs", .{ db, alloc });
        return (try self.appendGeneratedEnrichmentsFromStoredDocs(alloc, &.{}, true)).generated_ref_count;
    }

    pub fn reprocessGeneratedEnrichmentFromStoredDocs(
        self: *Enrichment,
        alloc: Allocator,
        artifact_name: ?[]const u8,
    ) !usize {
        return (try self.reprocessGeneratedEnrichmentFromStoredDocsWithProgress(alloc, artifact_name)).generated_ref_count;
    }

    pub fn reprocessGeneratedEnrichmentFromStoredDocsWithProgress(
        self: *Enrichment,
        alloc: Allocator,
        artifact_name: ?[]const u8,
    ) !DB.GeneratedEnrichmentReplayAppendResult {
        const db = self.dbPtr();
        if (comptime db_mod.storage_engine.enabled) return db_mod.storage_engine.call("reprocessGeneratedEnrichmentFromStoredDocsWithProgress", .{ db, alloc, artifact_name });
        if (artifact_name) |name| {
            const force_artifacts = [_][]const u8{name};
            return try self.appendGeneratedEnrichmentsFromStoredDocs(alloc, &force_artifacts, false);
        }
        return try self.appendGeneratedEnrichmentsFromStoredDocs(alloc, &.{}, false);
    }

    pub fn clearEmbeddingActivityObservations(indexes: []db_mod.types.DBIndexStats) void {
        for (indexes) |*item| {
            if (item.kind != .dense_vector and item.kind != .sparse_vector) continue;
            item.embedding_activity_observed = false;
            item.embedding_activity_sample_fresh = false;
            item.embedding_activity = .{};
        }
    }

    fn constructResolutionRuntime(alloc: Allocator, append_ctx: *db_mod.EnrichmentAppendContext, db: *DB) !db_mod.resolution_runtime_mod.ResolutionRuntime {
        return try db_mod.resolution_runtime_mod.ResolutionRuntime.init(
            alloc,
            db.core.batchExecutionResources().store,
            db.core.replaySource(),
            db.core.batchExecutionResources().index_manager,
            append_ctx,
            db_mod.appendResolutionRecord,
            db.backend_runtime,
            db.enrichment.resolution.candidate_source,
            db.enrichment.resolution.embedder,
        );
    }

    pub fn initResolutionRuntime(self: *Enrichment) !void {
        const db = self.dbPtr();
        if (comptime db_mod.storage_engine.enabled) return db_mod.storage_engine.call("initResolutionRuntime", .{db});
        // Always constructed so catalog/status/runUntilIdle APIs can observe
        // and drain replay. The background worker is started lazily only while
        // the resolver catalog is non-empty.
        const resources = db.core.batchExecutionResources();
        const context_value: db_mod.EnrichmentAppendContext = .{
            .cache_source = self.resolution.candidate_source,
            .alloc = db.runtime_alloc,
            .root_incarnation = db.root_incarnation,
            .artifact_publication_dispatcher = db.local_execution.artifact_publication_dispatcher,
            .store = resources.store,
            .applied_sequence_checkpoint_path = resources.applied_sequence_checkpoint_path,
            .index_repair_checkpoint = resources.index_repair_checkpoint,
            .shard_manager = resources.shard_manager,
            .index_manager = resources.index_manager,
            .apply_mutex = resources.apply_mutex,
            .portable_runtime_activation_pending = &db.async_context.portable_runtime_activation_pending,
            .snapshot_admission = resources.snapshot_admission,
            .snapshot_replay_admission = resources.snapshot_replay_admission,
            .repair_replay_mutex = resources.repair_replay_mutex,
            .change_journal = resources.change_journal,
            .replay_source = resources.replay_source,
            .executor = db.executor,
            .async_context = db.async_context,
            .log_mutex = resources.log_mutex,
            .replication_async_effect_mirror = db.local_execution.replication_async_effect_mirror,
            .replication_async_batch_mirror = db.local_execution.replication_async_batch_mirror,
            .replication_async_metadata_mirror = db.local_execution.replication_async_metadata_mirror,
            .replication_write_gate = db.local_execution.replication_write_gate,
        };

        self.resolution.owner = try @TypeOf(self.resolution.owner).create(db.runtime_alloc, context_value, .{db}, constructResolutionRuntime);
        const append_ctx = self.resolution.owner.context.?;
        const runtime = self.resolution.owner.runtime.?;
        runtime.ordered_writer = .{ .ptr = append_ctx, .process = db_mod.processOrderedResolutions };
        append_ctx.resolution_runtime = runtime;
        self.resolution.append_context = append_ctx;
        self.resolution.runtime = runtime;
        db.async_context.resolution_runtime = runtime;
        if (self.append_context) |ctx| ctx.resolution_runtime = runtime;
    }

    pub fn startResolverReplayRuntimesIfConfigured(self: *Enrichment) !void {
        const db = self.dbPtr();
        if (!db.hasConfiguredResolvers()) return;
        try self.startResolverReplayRuntimes();
    }

    /// Activate only after the serving owner installs candidate/sink hooks.
    pub fn activateResolverReplayRuntimes(self: *Enrichment) !void {
        const db = self.dbPtr();
        if (comptime db_mod.storage_engine.enabled) return db_mod.storage_engine.call("activateResolverReplayRuntimes", .{db});
        db.resolver_workers_enabled = true;
        if (db.local_execution.optional_runtime_workers_enabled) try self.startResolverReplayRuntimesIfConfigured();
    }

    /// Join callbacks that borrow a serving owner's cross-table catalog before
    /// that owner destroys any namespace or shared inference provider.
    pub fn deactivateResolverReplayRuntimes(self: *Enrichment) void {
        const db = self.dbPtr();
        if (comptime db_mod.storage_engine.enabled) return db_mod.storage_engine.call("deactivateResolverReplayRuntimes", .{db}) catch @panic("incompatible storage owner");
        db.resolver_workers_enabled = false;
        self.stopResolverReplayRuntimes();
    }

    pub fn startResolverReplayRuntimes(self: *Enrichment) !void {
        const db = self.dbPtr();
        if (!db.resolver_workers_enabled or !db.local_execution.optional_runtime_workers_enabled) return;
        var resolution_started = false;
        errdefer if (resolution_started) if (self.resolution.runtime) |runtime| runtime.stop();
        if (self.resolution.runtime) |runtime| {
            try runtime.start();
            resolution_started = true;
        }
        if (builtin.is_test and db_mod.test_fail_portable_promotion_runtime_start) {
            db_mod.test_fail_portable_promotion_runtime_start = false;
            return error.InjectedPortablePromotionRuntimeStartFailure;
        }
        if (self.promotion.runtime) |runtime| try runtime.start();
    }

    pub fn stopResolverReplayRuntimes(self: *Enrichment) void {
        if (self.resolution.runtime) |runtime| runtime.stop();
        if (self.promotion.runtime) |runtime| runtime.stop();
    }

    pub fn stopResolverReplayRuntimesIfUnconfigured(self: *Enrichment) void {
        const db = self.dbPtr();
        if (db.hasConfiguredResolvers()) return;
        if (self.resolution.runtime) |runtime| runtime.stop();
        if (self.promotion.runtime) |runtime| runtime.stop();
    }

    /// Inject (or clear) the cross-shard entity-resolution candidate source on
    /// an already-open DB. The serving layer (managed write cache) calls this
    /// right after a DB is opened, since managed DBs open lazily and cannot
    /// thread the source through `OpenOptions`. No-op when this DB has no
    /// resolution runtime (e.g. read-only / status opens).
    /// The embedder must outlive all resolution work. Call under the owner fence
    /// when replacing a managed enrichment runtime.
    pub fn setResolutionEmbedder(self: *Enrichment, embedder: ?db_mod.embedder_mod.DenseEmbedder) void {
        if (self.resolution.runtime) |runtime| {
            while (!runtime.catch_up_mutex.tryLock()) std.atomic.spinLoopHint();
            defer runtime.catch_up_mutex.unlock();
            runtime.embedder = embedder;
        }
        self.resolution.embedder = embedder;
    }

    pub fn setResolutionCandidateSource(self: *Enrichment, src: ?db_mod.resolution_runtime_mod.CandidateSource) void {
        const db = self.dbPtr();
        if (comptime db_mod.storage_engine.enabled) return db_mod.storage_engine.call("setResolutionCandidateSource", .{ db, src }) catch @panic("incompatible storage owner");
        self.resolution.candidate_source = src;
        if (self.resolution.append_context) |context| context.cache_source = src;
        if (self.resolution.runtime) |runtime| runtime.setCandidateSource(src);
    }

    pub fn acquireResolverCatalogActivity(self: *Enrichment, wait: bool) !DB.ResolverCatalogActivity {
        const db = self.dbPtr();
        const resolution = if (self.resolution.runtime) |runtime| &runtime.catch_up_mutex else null;
        const promotion = if (self.promotion.runtime) |runtime| &runtime.catch_up_mutex else null;
        // Schema calls must not wait indefinitely for model inference/replay.
        var attempts: usize = 0;
        while (true) : (attempts += 1) {
            if (resolution == null or resolution.?.tryLock()) {
                if (promotion == null or promotion.?.tryLock())
                    return .{ .resolution = resolution, .promotion = promotion };
                if (resolution) |mutex| mutex.unlock();
            }
            // Managed refresh/apply callers hold a group activity. They must
            // release it so a worker already inside a Raft callback can finish.
            if (!wait) return error.WriterLocked;
            if (attempts >= 100) return error.Busy;
            const io = db.backend_runtime.controlIo() orelse db.backend_runtime.io() orelse
                return error.BackendRuntimeIoUnavailable;
            try io.sleep(.fromMilliseconds(1), .awake);
        }
    }

    /// Add or replace a resolver. Inserts and material config changes re-resolve
    /// the existing corpus so the new resolver/scorer behavior applies to
    /// documents already ingested (the extraction artifacts did not change, so
    /// the incremental hint would not fire on its own).
    pub fn upsertResolverWithResultOptions(
        self: *Enrichment,
        cfg: db_mod.index_manager_mod.ResolverConfig,
        options: DB.ResolverUpsertOptions,
    ) !db_mod.index_manager_mod.IndexManager.ResolverUpsertResult {
        const db = self.dbPtr();
        if (db_mod.openModeRequiresReadOnlyBackends(db.open_mode)) return error.ReadOnly;
        var replication_mutation = db.acquireReplicationMutationShared();
        defer if (replication_mutation) |*lease| lease.release();
        try db.enforceReplicationWriteGate();
        const upsert_result = blk: {
            try cfg.validate();
            // An unchanged catalog observation does not mutate resolver state.
            // Do not wait for distributed callbacks, rewrite the catalog, or
            // retire a serving owner just because replay is currently active.
            if (!options.migrate_namespace and db.core.index_manager.resolverConfigMatches(cfg)) break :blk .updated_no_backfill;
            var activity = try self.acquireResolverCatalogActivity(options.drain_backfill or options.wait_for_activity);
            defer activity.deinit();
            try db.lockApplyForPortableRuntime();
            defer db.core.unlockApply();
            {
                var probe = try db.core.store.beginProbeTxn();
                defer probe.abort();
                try db.requireOnlineArtifactCatalogMutableLocked(&probe);
            }
            const result = if (options.migrate_namespace)
                try db.core.index_manager.upsertResolverWithNamespaceMigration(db.core.store, cfg, true)
            else
                try db.core.upsertResolver(cfg);
            if (result != .updated_no_backfill) {
                if (self.resolution.runtime) |runtime| runtime.notifyBackfill();
            }
            break :blk result;
        };
        switch (upsert_result) {
            .inserted, .updated_backfill_required => {
                try self.startResolverReplayRuntimes();
                if (options.drain_backfill) {
                    try self.backfillResolverCorpus();
                }
            },
            .updated_no_backfill => {
                try self.startResolverReplayRuntimesIfConfigured();
                if (options.drain_backfill and self.resolution.runtime != null) {
                    const runtime = self.resolution.runtime.?;
                    if (try runtime.hasReresolveBacklog()) {
                        try self.backfillResolverCorpus();
                    } else {
                        // The worker may have cleared the enqueue cursor while
                        // the resulting replay and downstream work is pending.
                        try db.runUntilIdle();
                    }
                }
            },
        }
        return upsert_result;
    }

    pub fn completeResolverProjectionWork(self: *Enrichment, key: []const u8) !void {
        const db = self.dbPtr();
        if (db_mod.openModeRequiresReadOnlyBackends(db.open_mode)) return error.ReadOnly;
        var replication_mutation = db.acquireReplicationMutationShared();
        defer if (replication_mutation) |*lease| lease.release();
        try db.enforceReplicationWriteGate();
        var activity = try self.acquireResolverCatalogActivity(true);
        defer activity.deinit();
        try db.lockApplyForPortableRuntime();
        defer db.core.unlockApply();
        var probe = try db.core.store.beginProbeTxn();
        defer probe.abort();
        try db.requireOnlineArtifactCatalogMutableLocked(&probe);
        try db.core.index_manager.completeResolverProjectionWork(db.core.store, key);
    }

    pub fn publishResolverNamespaceProjection(self: *Enrichment, table: []const u8, table_id: u64, namespace: @import("../resolver_namespace.zig").Namespace) !bool {
        return self.publishResolverNamespaceProjectionWork(table, table_id, namespace, null);
    }

    pub fn publishResolverNamespaceProjectionWork(self: *Enrichment, table: []const u8, table_id: u64, namespace: @import("../resolver_namespace.zig").Namespace, completion: ?@import("../../lite/resolver_consumers.zig").Completion) !bool {
        const db = self.dbPtr();
        if (db_mod.openModeRequiresReadOnlyBackends(db.open_mode)) return error.ReadOnly;
        var replication_mutation = db.acquireReplicationMutationShared();
        defer if (replication_mutation) |*lease| lease.release();
        try db.enforceReplicationWriteGate();
        const changed = blk: {
            var activity = try self.acquireResolverCatalogActivity(true);
            defer activity.deinit();
            try db.lockApplyForPortableRuntime();
            defer db.core.unlockApply();
            var probe = try db.core.store.beginProbeTxn();
            defer probe.abort();
            try db.requireOnlineArtifactCatalogMutableLocked(&probe);
            break :blk try db.core.index_manager.publishResolverNamespaceProjectionWork(db.core.store, table, table_id, namespace, completion);
        };
        if (changed) {
            if (self.resolution.runtime) |runtime| runtime.notifyBackfill();
            try self.startResolverReplayRuntimes();
        }
        return changed;
    }

    pub fn upsertResolverWithResult(self: *Enrichment, cfg: db_mod.index_manager_mod.ResolverConfig) !db_mod.index_manager_mod.IndexManager.ResolverUpsertResult {
        return try self.upsertResolverWithResultOptions(cfg, .{});
    }

    pub fn curateResolverJsonAlloc(self: *Enrichment, alloc: Allocator, name: []const u8, request_json: []const u8) ![]u8 {
        const db = self.dbPtr();
        if (comptime db_mod.storage_engine.enabled) return db_mod.storage_engine.call("curateResolverJsonAlloc", .{ db, alloc, name, request_json });
        return @import("../resolver_curation.zig").mutate(db, alloc, name, request_json);
    }

    pub fn listResolverStaticDescriptors(self: *Enrichment, alloc: Allocator) ![]db_mod.index_manager_mod.ResolverConfig {
        const db = self.dbPtr();
        if (comptime db_mod.storage_engine.enabled) return db_mod.storage_engine.call("listResolverStaticDescriptors", .{ db, alloc });
        return db.core.index_manager.listResolverStaticDescriptors(alloc);
    }

    pub fn listResolverIdentities(self: *Enrichment, alloc: Allocator) ![]db_mod.index_manager_mod.ResolverConfig {
        const db = self.dbPtr();
        if (comptime db_mod.storage_engine.enabled) return db_mod.storage_engine.call("listResolverIdentities", .{ db, alloc });
        return db.core.index_manager.listResolverIdentities(alloc);
    }

    pub fn consumeResolverMetadataDrop(self: *Enrichment, alloc: Allocator, identity: @import("../resolver_curation.zig").MetadataIdentity) !void {
        const db = self.dbPtr();
        if (comptime db_mod.storage_engine.enabled) return db_mod.storage_engine.call("consumeResolverMetadataDrop", .{ db, alloc, identity });
        return @import("../resolver_curation.zig").consumeMetadataDrop(db, alloc, identity);
    }

    pub fn drainResolverBackfill(self: *Enrichment) !void {
        try self.startResolverReplayRuntimesIfConfigured();
        try self.backfillResolverCorpus();
    }

    pub fn backfillResolverCorpus(self: *Enrichment) !void {
        const db = self.dbPtr();
        if (!db.hasConfiguredResolvers()) return;
        if (self.resolution.runtime) |runtime| {
            try runtime.requestReresolveBacklog();
            while (true) {
                var tick = try runtime.runReresolveBacklogWindow();
                const queued = tick.queued;
                const complete = tick.complete;
                tick.deinit(db.runtime_alloc);
                // Drive the enqueued extraction artifacts through resolution,
                // promotion, and graph materialization before queuing more.
                if (queued > 0) try db.runUntilIdle();
                if (complete) break;
            }
            // A background window may have enqueued the final records, so this
            // caller's last window can report complete with zero queued work.
            // Synchronous backfill promises applied output, not just an empty
            // corpus cursor; drain the shared replay target before returning.
            try db.runUntilIdle();
        }
    }

    pub fn dropRetiredResolverIdentity(self: *Enrichment, name: []const u8) !bool {
        const db = self.dbPtr();
        if (db_mod.openModeRequiresReadOnlyBackends(db.open_mode)) return error.ReadOnly;
        var replication_mutation = db.acquireReplicationMutationShared();
        defer if (replication_mutation) |*lease| lease.release();
        try db.enforceReplicationWriteGate();
        var activity = try self.acquireResolverCatalogActivity(true);
        defer activity.deinit();
        try db.lockApplyForPortableRuntime();
        defer db.core.unlockApply();
        var probe = try db.core.store.beginProbeTxn();
        defer probe.abort();
        try db.requireOnlineArtifactCatalogMutableLocked(&probe);
        return try db.core.index_manager.dropRetiredResolverIdentity(db.core.store, name);
    }

    pub fn restoreResolverIdentityWithoutDrain(self: *Enrichment, name: []const u8, cfg: ?db_mod.index_manager_mod.ResolverConfig) !void {
        const db = self.dbPtr();
        if (db_mod.openModeRequiresReadOnlyBackends(db.open_mode)) return error.ReadOnly;
        var replication_mutation = db.acquireReplicationMutationShared();
        defer if (replication_mutation) |*lease| lease.release();
        try db.enforceReplicationWriteGate();
        var activity = try self.acquireResolverCatalogActivity(true);
        defer activity.deinit();
        try db.lockApplyForPortableRuntime();
        defer db.core.unlockApply();
        var probe = try db.core.store.beginProbeTxn();
        defer probe.abort();
        try db.requireOnlineArtifactCatalogMutableLocked(&probe);
        if (cfg) |previous| {
            try db.core.index_manager.restoreResolverIdentity(db.core.store, previous);
        } else {
            _ = try db.core.index_manager.removeResolver(db.core.store, name);
        }
        if (self.resolution.runtime) |runtime| runtime.notifyBackfill();
    }

    pub fn removeResolverWithoutDrain(self: *Enrichment, name: []const u8) !bool {
        const db = self.dbPtr();
        if (comptime db_mod.storage_engine.enabled) return db_mod.storage_engine.call("removeResolverWithoutDrain", .{ db, name });
        return self.removeResolverForReconciliation(name, null);
    }

    pub fn removeResolverForReconciliation(self: *Enrichment, name: []const u8, context: ?@import("../artifact_reconcile_intent.zig").Context) !bool {
        const db = self.dbPtr();
        if (context) |authority| return self.removeResolverReconciliationPage(name, authority);
        if (db_mod.openModeRequiresReadOnlyBackends(db.open_mode)) return error.ReadOnly;
        var replication_mutation = db.acquireReplicationMutationShared();
        defer if (replication_mutation) |*lease| lease.release();
        if (context == null) try db.enforceReplicationWriteGate();
        {
            var activity = try self.acquireResolverCatalogActivity(false);
            defer activity.deinit();
            {
                db.core.index_manager.catalog_mutex.lockShared();
                defer db.core.index_manager.catalog_mutex.unlockShared();
                try db.core.index_manager.requireResolverNotReplacing(name);
            }
            // No resolver can be removed while an old decision or promotion is
            // in flight. Wake the existing owner and retry on a later refresh.
            const promotion_ready = if (self.promotion.runtime) |runtime|
                runtime.retirementReadinessLocked() != .pending
            else blk: {
                const persisted = self.promotionStageStats();
                break :blk !persisted.catch_up_required and !persisted.blocked;
            };
            if (self.resolutionStageStats().catch_up_required or !promotion_ready)
                return error.WriterLocked;
            try db.lockApplyForPortableRuntime();
            defer db.core.unlockApply();
            {
                var probe = try db.core.store.beginProbeTxn();
                defer probe.abort();
                try db.requireOnlineArtifactCatalogMutableWithContextLocked(&probe, context);
            }
            const cfg = (try db.resolverConfigByNameAlloc(name)) orelse return false;
            defer {
                var owned = cfg;
                owned.deinit(db.alloc);
            }
            if (try self.retireResolverArtifactsLocked(cfg)) |sequence| {
                DB.notifyQueryVisibilityTargetAdvanced(db.async_context, sequence);
                db.executor.notifySequence(sequence);
                db.notifyResolverReplayRuntimesForced(sequence);
                return error.WriterLocked;
            }
            // Graph replay still needs the resolver contract to interpret
            // retirement records, including records whose artifact is absent.
            // Its persisted/live watermark also makes this safe after reopen.
            for (db.core.index_manager.graph_indexes.items) |entry| {
                const applied = try db.managedIndexAppliedSequence(db.alloc, entry.config.name);
                const target = try db.managedIndexReplayTargetSequence(db.alloc, entry.config.name, .graph, applied);
                if (applied < target)
                    return error.WriterLocked;
            }
            if (!try db.core.removeResolver(name)) return false;
        }
        self.stopResolverReplayRuntimesIfUnconfigured();
        return true;
    }

    fn removeResolverReconciliationPage(self: *Enrichment, name: []const u8, context: @import("../artifact_reconcile_intent.zig").Context) !bool {
        const db = self.dbPtr();
        const intent = @import("../artifact_reconcile_intent.zig");
        var activity = try self.acquireResolverCatalogActivity(false);
        defer activity.deinit();
        try db.lockApplyForPortableRuntime();
        defer db.core.unlockApply();
        {
            var read = try db.core.store.beginReadTxn();
            defer read.abort();
            try db.requireOnlineArtifactCatalogMutableWithContextLocked(&read, context);
            try db.requireArtifactReconcileUnfenced(&read, context);
        }
        // Graph indexes were detached by preceding reconciliation steps.
        // catch_up_mutex covers resolver/promotion jobs through publication;
        // holding both activity locks drains any prior callback. Between
        // pages, the durable intent rejects producer and handoff publication
        // under the apply mutex, so nothing can appear behind this cursor.
        // Final config removal uses these same locks before stopping workers.
        if (db.core.graphIndexes().len != 0) return error.ArtifactCatalogDrift;
        const config = (try db.resolverConfigByNameAlloc(name)) orelse return false;
        defer {
            var owned = config;
            owned.deinit(db.alloc);
        }
        const raw = try db.core.getStoreValue(db.alloc, intent.resolver_cursor_key);
        defer if (raw) |bytes| db.alloc.free(bytes);
        var parsed = if (raw) |bytes| std.json.parseFromSlice(intent.ResolverCursor, db.alloc, bytes, .{}) catch |err| switch (err) {
            error.OutOfMemory => return err,
            else => return error.ArtifactCatalogCorrupt,
        } else null;
        defer if (parsed) |*value| value.deinit();
        const prior: ?intent.ResolverCursor = if (parsed) |value| if (std.mem.eql(u8, value.value.name, name) and std.mem.eql(u8, &value.value.token, &context.token)) value.value else null else null;
        if (prior != null and prior.?.complete) return db.core.removeResolver(name);
        const prefix = [_]u8{db_mod.internal_keys.user_namespace};
        const keys = try db.core.store.scanPrefixKeysPage(db.alloc, &prefix, if (prior) |value| value.after else null, 128);
        defer {
            for (keys) |key| db.alloc.free(key);
            db.alloc.free(keys);
        }
        var deletes: std.ArrayListUnmanaged([]const u8) = .empty;
        defer {
            for (deletes.items) |key| db.alloc.free(@constCast(key));
            deletes.deinit(db.alloc);
        }
        for (keys) |key| {
            const resolution = (try db_mod.internal_keys.parseResolutionArtifactKeyAlloc(db.alloc, key)) orelse continue;
            defer {
                db.alloc.free(resolution.doc_key);
                db.alloc.free(resolution.artifact_name);
            }
            if (!std.mem.eql(u8, resolution.artifact_name, config.resolution_artifact)) continue;
            try deletes.append(db.alloc, try db.alloc.dupe(u8, key));
            try db_mod.appendResolutionHandoffDeleteKey(db.alloc, &deletes, key);
        }
        const cursor = try std.json.Stringify.valueAlloc(db.alloc, intent.ResolverCursor{ .token = context.token, .name = name, .after = if (keys.len == 0) "" else keys[keys.len - 1], .complete = keys.len == 0 }, .{});
        defer db.alloc.free(cursor);
        try db.core.store.putBatch(&.{.{ .key = intent.resolver_cursor_key, .value = cursor }}, deletes.items);
        return false;
    }

    fn retireResolverArtifactsLocked(self: *Enrichment, cfg: db_mod.index_manager_mod.ResolverConfig) !?u64 {
        const db = self.dbPtr();
        var deletes = std.ArrayListUnmanaged([]const u8).empty;
        defer {
            for (deletes.items) |key| db.alloc.free(@constCast(key));
            deletes.deinit(db.alloc);
        }
        var changed = std.ArrayListUnmanaged([]u8).empty;
        defer {
            for (changed.items) |key| db.alloc.free(key);
            changed.deinit(db.alloc);
        }

        const marker_prefix = try db_mod.internal_keys.assetArtifactSourceIndexPrefixAlloc(db.alloc, cfg.source_artifact);
        defer db.alloc.free(marker_prefix);
        const markers = try db.core.store.scanPrefix(db.alloc, marker_prefix);
        defer db_mod.docstore_mod.DocStore.freeResults(db.alloc, markers);

        for (markers) |marker| {
            const parsed = (try db_mod.internal_keys.parseAssetArtifactKeyAlloc(db.alloc, marker.value)) orelse continue;
            defer {
                db.alloc.free(parsed.doc_key);
                db.alloc.free(parsed.artifact_name);
            }
            if (!std.mem.eql(u8, parsed.artifact_name, cfg.source_artifact)) continue;

            const resolution_key = try db_mod.internal_keys.resolutionArtifactKeyAlloc(db.alloc, parsed.doc_key, cfg.resolution_artifact);
            var resolution_key_owned = true;
            errdefer if (resolution_key_owned) db.alloc.free(resolution_key);
            const exists = if (db.core.store.get(db.alloc, resolution_key)) |raw| exists: {
                db.alloc.free(raw);
                break :exists true;
            } else |err| switch (err) {
                error.NotFound => false,
                else => return err,
            };
            if (exists and !db_mod.containsDeleteKey(deletes.items, resolution_key)) {
                try deletes.append(db.alloc, resolution_key);
                resolution_key_owned = false;
                try db_mod.appendUniqueOwnedKey(db.alloc, &changed, resolution_key);
                try db_mod.appendResolutionHandoffDeleteKey(db.alloc, &deletes, resolution_key);
            } else {
                db.alloc.free(resolution_key);
                resolution_key_owned = false;
            }

            try self.collectResolverMentionArtifactsForDocLocked(cfg, parsed.doc_key, &deletes, &changed);
        }

        const root_lower = [_]u8{db_mod.internal_keys.user_namespace};
        const root_upper = [_]u8{db_mod.internal_keys.user_namespace + 1};
        const resolution_rows = try db.core.store.scanRange(db.alloc, root_lower[0..], root_upper[0..]);
        defer db_mod.docstore_mod.DocStore.freeResults(db.alloc, resolution_rows);
        for (resolution_rows) |row| {
            const parsed = (try db_mod.internal_keys.parseResolutionArtifactKeyAlloc(db.alloc, row.key)) orelse continue;
            defer {
                db.alloc.free(parsed.doc_key);
                db.alloc.free(parsed.artifact_name);
            }
            if (!std.mem.eql(u8, parsed.artifact_name, cfg.resolution_artifact)) continue;
            if (!db_mod.containsDeleteKey(deletes.items, row.key)) {
                try deletes.append(db.alloc, try db.alloc.dupe(u8, row.key));
                try db_mod.appendUniqueOwnedKey(db.alloc, &changed, row.key);
                try db_mod.appendResolutionHandoffDeleteKey(db.alloc, &deletes, row.key);
            }
            try self.collectResolverMentionArtifactsForDocLocked(cfg, parsed.doc_key, &deletes, &changed);
        }

        if (deletes.items.len == 0 and changed.items.len == 0) return null;

        const sequence = db.core.reserveDerivedAppendSequence();
        const replay_payload = try db_mod.encodeChangeRecordPayload(&db.batchContext(), .{
            .sequence = sequence,
            .changed_artifact_keys = changed.items,
        }, sequence);
        defer db.alloc.free(replay_payload);

        var backlog_admission = try db.executor.admitBacklogBytes(@intCast(replay_payload.len));
        defer backlog_admission.cancel();
        try db.core.store.putBatchWithReplay(db.backend_runtime.io(), &.{}, deletes.items, .{
            .sequence = sequence,
            .payload = replay_payload,
        });
        db.mirrorReplicationReplayPayloadBestEffort(replay_payload);
        db.executor.commitBacklogAdmission(sequence, &backlog_admission);
        return sequence;
    }

    fn collectResolverMentionArtifactsForDocLocked(
        self: *Enrichment,
        cfg: db_mod.index_manager_mod.ResolverConfig,
        doc_key: []const u8,
        deletes: *std.ArrayListUnmanaged([]const u8),
        changed: *std.ArrayListUnmanaged([]u8),
    ) !void {
        const db = self.dbPtr();
        for (db.core.graphIndexes()) |graph_entry| {
            for (graph_entry.artifact_sources) |source| {
                if (source.mention_edge_type.len == 0) continue;
                if (!std.mem.eql(u8, source.artifact_name, cfg.source_artifact)) continue;

                const state_name = try db_mod.mentionGraphStateNameAlloc(db.alloc, cfg.source_artifact, cfg.resolution_artifact);
                defer db.alloc.free(state_name);
                try db.collectGraphStateForDeleteLocked(doc_key, graph_entry.config.name, state_name, graph_entry.config.coverage_generation, deletes, changed);

                const mention_state_name = try db_mod.mentionArtifactStateNameAlloc(db.alloc, cfg.source_artifact, cfg.resolution_artifact);
                defer db.alloc.free(mention_state_name);
                try db.collectGraphStateForDeleteLocked(doc_key, graph_entry.config.name, mention_state_name, graph_entry.config.coverage_generation, deletes, changed);
            }
        }
    }

    pub fn retireResolverReplayBeforeCatalogRemoval(self: *Enrichment) !void {
        const db = self.dbPtr();
        try db.runUntilIdle();
        const resolution_stats = self.resolutionStageStats();
        if (resolution_stats.catch_up_required or resolution_stats.blocked) return error.ResolverReplayPending;
        const promotion_stats = self.promotionStageStats();
        if (promotion_stats.catch_up_required or promotion_stats.blocked) return error.ResolverReplayPending;
    }

    pub fn extractResolverQueryAlloc(self: *Enrichment, alloc: Allocator, cfg: db_mod.index_manager_mod.ResolverConfig, text: []const u8, producer_override: ?db_mod.asset_producer_mod.Producer) ![]u8 {
        const db = self.dbPtr();
        var plan = try db.resolverQueryPlanAlloc(alloc, cfg);
        defer plan.deinit(alloc);
        const producer = producer_override orelse if (self.runtime) |runtime|
            if (runtime.config.asset_producer) |configured| configured.withInvocationContext(.{ .io = runtime.config.io, .cancellation = runtime.config.cancellation }) else return error.SqlFeatureNotSupported
        else
            return error.SqlFeatureNotSupported;
        return plan.produce(alloc, text, producer);
    }

    pub fn resolutionStageStats(self: *Enrichment) db_mod.types.ReplayStageStats {
        const db = self.dbPtr();
        if (!db.hasConfiguredResolvers()) return .{};
        if (self.resolution.runtime) |runtime| return runtime.stats();
        return self.persistedResolutionStageStats() catch .{
            .enabled = true,
            .catch_up_required = true,
            .blocked = true,
            .blocked_reason = "resolver_recovery_state_unavailable",
        };
    }

    fn persistedResolutionStageStats(self: *Enrichment) !db_mod.types.ReplayStageStats {
        const db = self.dbPtr();
        var persisted_stage = try db.persistedReplayStageStats(db_mod.resolution_runtime_mod.scope_name, false);
        const resources = db.core.batchExecutionResources();
        if (try db_mod.resolution_runtime_mod.hasDurableReresolveBacklog(resources.store)) {
            persisted_stage.enabled = true;
            persisted_stage.catch_up_required = true;
        }
        return persisted_stage;
    }

    pub fn initPromotionRuntime(self: *Enrichment) !void {
        const db = self.dbPtr();
        if (comptime db_mod.storage_engine.enabled) return db_mod.storage_engine.call("initPromotionRuntime", .{db});
        // Always constructed (like the resolution runtime) so synchronous
        // drains and status APIs remain available. The background worker is
        // started lazily only while resolver replay can produce promotion work.
        self.promotion.owner_bundle = try @TypeOf(self.promotion.owner_bundle).create(db.runtime_alloc, .{
            db.runtime_alloc,
            db.core.batchExecutionResources().store,
            db.core.replaySource(),
            db.backend_runtime,
            self.promotion.owner,
            // Cross-shard entity sink when the serving layer injected one (via
            // OpenOptions or setEntitySink); null follows the configured missing-sink policy.
            self.promotion.entity_sink,
            self.promotion.entity_sink_missing_policy,
        });
        const runtime = self.promotion.owner_bundle.runtime.?;
        runtime.catalog = db.core.index_manager;
        self.promotion.runtime = runtime;
        db.async_context.promotion_runtime = runtime;
        // Patch the resolution stage's append context so journaling a resolution
        // artifact (tagged with the promotion hint) wakes the promoter.
        if (self.resolution.append_context) |ctx| ctx.promotion_runtime = runtime;
    }

    /// Inject (or clear) the cross-shard entity sink on an already-open DB. The
    /// serving layer (managed write cache) calls this right after a DB is opened,
    /// since managed DBs open lazily and cannot thread the sink through
    /// `OpenOptions`. No-op when this DB has no promotion runtime.
    pub fn setEntitySink(self: *Enrichment, sink: ?db_mod.promotion_runtime_mod.EntitySink) void {
        const db = self.dbPtr();
        if (comptime db_mod.storage_engine.enabled) return db_mod.storage_engine.call("setEntitySink", .{ db, sink }) catch @panic("incompatible storage owner");
        self.promotion.entity_sink = sink;
        if (self.promotion.runtime) |runtime| runtime.setSink(sink);
    }

    /// Inject (or clear) the source-shard promotion owner on an already-open DB.
    /// Serving-layer managed DBs use this to keep follower raft apply from
    /// turning replay into public entity-table writes.
    pub fn setPromotionOwner(self: *Enrichment, owner: ?db_mod.promotion_runtime_mod.PromotionOwner) void {
        const db = self.dbPtr();
        if (comptime db_mod.storage_engine.enabled) return db_mod.storage_engine.call("setPromotionOwner", .{ db, owner }) catch @panic("incompatible storage owner");
        self.promotion.owner = owner;
        if (self.promotion.runtime) |runtime| runtime.setOwner(owner);
    }

    pub fn promotionStageStats(self: *Enrichment) db_mod.types.ReplayStageStats {
        const db = self.dbPtr();
        if (!db.hasConfiguredResolvers()) return .{};
        if (self.promotion.runtime) |runtime| return runtime.stats();
        return db.persistedReplayStageStats(db_mod.promotion_runtime_mod.scope_name, false) catch .{};
    }
};
