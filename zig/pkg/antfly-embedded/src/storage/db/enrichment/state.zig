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

test "document extraction templated inline source size is rejected before persistence" {
    const alloc = std.testing.allocator;

    var path_tmp = try db_mod.TestDirectory.init("db");
    defer path_tmp.cleanup();
    const path = path_tmp.path().ptr;
    defer db_mod.cleanupTempDir(path);

    const security = db_mod.scraping.ContentSecurityConfig{ .max_download_size_bytes = 4 };
    var remote_content = db_mod.scraping.RemoteContentConfig{ .security = security };
    var db = try DB.open(alloc, std.mem.span(path), .{
        .remote_content = &remote_content,
        .start_index_workers = false,
        .ttl_cleanup = .{ .enabled = false },
    });
    defer db.close();

    try db.enrichment.addEnrichment(.{
        .name = "document_units_v1",
        .kind = .asset,
        .template = "{{url}}",
        .content_type = "application/json",
        .producer_json = "{\"type\":\"document_extraction\",\"config\":{}}",
    });
    try std.testing.expectError(error.StreamTooLong, db.batch(.{
        .writes = &.{.{
            .key = "doc:templated-too-large",
            .value = "{\"url\":\"data:text/plain;base64,aGVsbG8=\"}",
        }},
        .sync_level = .write,
    }));

    const doc_key = try db_mod.internal_keys.documentKeyAlloc(alloc, "doc:templated-too-large");
    defer alloc.free(doc_key);
    try std.testing.expectError(error.NotFound, db.core.store.get(alloc, doc_key));
}

test "stale multi-source vector replay writes are source fenced" {
    const alloc = std.testing.allocator;
    var path_tmp = try db_mod.TestDirectory.init("db");
    defer path_tmp.cleanup();
    const path = path_tmp.path().ptr;
    defer db_mod.cleanupTempDir(path);

    var db = try DB.open(alloc, std.mem.span(path), .{});
    defer db.close();
    try db.enrichment.addEnrichment(.{
        .name = "dense_source_v1",
        .kind = .embedding,
        .field = "body",
        .expected_dims = 2,
        .producer_json = "{\"version\":1,\"provider\":\"antfly\",\"model\":\"dense-test\",\"dimensions\":2}",
    });
    try db.enrichment.addEnrichment(.{
        .name = "sparse_source_v1",
        .kind = .embedding,
        .field = "body",
        .producer_json = "{\"version\":1,\"provider\":\"antfly\",\"model\":\"sparse-test\"}",
    });
    try db.addIndex(.{
        .name = "dense_union",
        .kind = .dense_vector,
        .config_json = "{\"field\":\"embedding\",\"dims\":2,\"sources\":[{\"artifact\":\"dense_source_v1\"}]}",
    });
    try db.addIndex(.{
        .name = "sparse_union",
        .kind = .sparse_vector,
        .config_json = "{\"field\":\"sparse_embedding\",\"sources\":[{\"artifact\":\"sparse_source_v1\"}]}",
    });

    const dense_artifact = try db_mod.internal_keys.embeddingArtifactKeyForDocumentAlloc(alloc, "doc:authoritative", "dense_source_v1");
    defer alloc.free(dense_artifact);
    const sparse_artifact = try db_mod.internal_keys.embeddingArtifactKeyForDocumentAlloc(alloc, "doc:authoritative", "sparse_source_v1");
    defer alloc.free(sparse_artifact);
    const foreign_artifact = try db_mod.internal_keys.embeddingArtifactKeyForDocumentAlloc(alloc, "doc:stale", "retired_source_v1");
    defer alloc.free(foreign_artifact);

    const dense_stale = [_]db_mod.derived_types.DerivedDenseEmbeddingWrite{
        .{ .index_name = "dense_union", .doc_key = "doc:stale", .vector = &.{ 1, 0 } },
        .{ .index_name = "dense_union", .doc_key = "doc:stale", .artifact_key = foreign_artifact, .vector = &.{ 1, 0 } },
    };
    try std.testing.expectEqual(db_mod.ManagedIndexBatchApplicability.irrelevant, db_mod.managedIndexBatchApplicability(
        db.core.index_manager,
        .{ .dense_embeddings = &dense_stale },
        .{ .name = "dense_union", .kind = .dense_vector },
    ));
    const dense_writes = [_]db_mod.derived_types.DerivedDenseEmbeddingWrite{
        dense_stale[0],
        dense_stale[1],
        .{ .index_name = "dense_union", .doc_key = "doc:untrusted", .artifact_key = dense_artifact, .vector = &.{ 0, 1 } },
    };
    var collected_dense = try db_mod.collectDenseEmbeddingWritesForBatch(alloc, db.core.index_manager, &dense_writes, &.{}, "dense_union");
    defer collected_dense.deinit();
    try std.testing.expectEqual(@as(usize, 1), collected_dense.writes.len);
    try std.testing.expectEqualStrings("doc:authoritative", collected_dense.writes[0].doc_key);
    try std.testing.expectEqual(@as(usize, 0), collected_dense.writes[0].vector.len);

    const sparse_stale = [_]db_mod.derived_types.DerivedSparseEmbeddingWrite{
        .{ .index_name = "sparse_union", .doc_key = "doc:stale", .indices = &.{1}, .values = &.{1} },
        .{ .index_name = "sparse_union", .doc_key = "doc:stale", .artifact_key = foreign_artifact, .indices = &.{1}, .values = &.{1} },
    };
    try std.testing.expectEqual(db_mod.ManagedIndexBatchApplicability.irrelevant, db_mod.managedIndexBatchApplicability(
        db.core.index_manager,
        .{ .sparse_embeddings = &sparse_stale },
        .{ .name = "sparse_union", .kind = .sparse_vector },
    ));
    const sparse_writes = [_]db_mod.derived_types.DerivedSparseEmbeddingWrite{
        sparse_stale[0],
        sparse_stale[1],
        .{ .index_name = "sparse_union", .doc_key = "doc:untrusted", .artifact_key = sparse_artifact, .indices = &.{1}, .values = &.{1} },
    };
    var collected_sparse = try db_mod.collectSparseEmbeddingWritesForBatch(alloc, db.core.index_manager, &sparse_writes, &.{}, "sparse_union");
    defer collected_sparse.deinit();
    try std.testing.expectEqual(@as(usize, 1), collected_sparse.writes.len);
    try std.testing.expectEqualStrings("doc:authoritative", collected_sparse.writes[0].doc_key);
    try std.testing.expectEqual(@as(usize, 0), collected_sparse.writes[0].indices.len);
}

test "db vector indexes combine direct document and chunk-backed artifact sources" {
    const alloc = std.testing.allocator;

    var path_tmp = try db_mod.TestDirectory.init("db");
    defer path_tmp.cleanup();
    const path = path_tmp.path().ptr;
    defer db_mod.cleanupTempDir(path);

    var db = try DB.open(alloc, std.mem.span(path), .{
        .start_index_workers = false,
        .start_optional_runtimes = false,
    });
    defer db.close();

    try db.enrichment.addEnrichment(.{
        .name = "body_chunks_v1",
        .kind = .chunk,
        .field = "body",
        .chunk_size = 64,
    });
    for ([_]db_mod.types.EnrichmentConfig{
        .{ .name = "title_dense_v1", .kind = .embedding, .field = "title", .expected_dims = 2, .vector_space = "test:mixed-dense-v1" },
        .{ .name = "summary_dense_v1", .kind = .embedding, .field = "summary", .expected_dims = 2, .vector_space = "test:mixed-dense-v1" },
        .{ .name = "body_dense_v1", .kind = .embedding, .field = "text", .source_artifact_name = "body_chunks_v1", .expected_dims = 2, .vector_space = "test:mixed-dense-v1" },
        .{ .name = "title_sparse_v1", .kind = .embedding, .field = "title", .vector_space = "test:mixed-sparse-v1" },
        .{ .name = "summary_sparse_v1", .kind = .embedding, .field = "summary", .vector_space = "test:mixed-sparse-v1" },
        .{ .name = "body_sparse_v1", .kind = .embedding, .field = "text", .source_artifact_name = "body_chunks_v1", .vector_space = "test:mixed-sparse-v1" },
    }) |cfg| try db.enrichment.addEnrichment(cfg);

    try db.addIndex(.{
        .name = "mixed_dense",
        .kind = .dense_vector,
        .config_json = "{\"field\":\"embedding\",\"dims\":2,\"metric\":\"l2_squared\",\"sources\":[{\"artifact\":\"title_dense_v1\"},{\"artifact\":\"summary_dense_v1\"},{\"artifact\":\"body_dense_v1\"}]}",
    });
    try db.addIndex(.{
        .name = "mixed_sparse",
        .kind = .sparse_vector,
        .config_json = "{\"field\":\"embedding\",\"sources\":[{\"artifact\":\"title_sparse_v1\"},{\"artifact\":\"summary_sparse_v1\"},{\"artifact\":\"body_sparse_v1\"}]}",
    });
    try db.addIndex(.{
        .name = "primary_text",
        .kind = .full_text,
        .config_json = "{}",
    });

    try db.batch(.{
        .writes = &.{
            .{ .key = "doc:direct", .value = "{\"title\":\"direct\",\"summary\":\"second direct member\",\"body\":\"unused\"}" },
            .{ .key = "doc:chunk", .value = "{\"title\":\"unused\",\"body\":\"chunk\"}" },
        },
        .sync_level = .full_index,
    });

    const chunk_key = try db_mod.internal_keys.chunkArtifactKeyAlloc(alloc, "doc:chunk", "body_chunks_v1", 0);
    defer alloc.free(chunk_key);
    try db.core.store.put(chunk_key, "{\"_parent_doc_key\":\"doc:chunk\",\"text\":\"chunk\"}");

    const direct_dense = try db_mod.expectedDocumentEmbeddingArtifactKeyAlloc(alloc, "doc:direct", "title_dense_v1");
    defer alloc.free(direct_dense);
    const second_direct_dense = try db_mod.expectedDocumentEmbeddingArtifactKeyAlloc(alloc, "doc:direct", "summary_dense_v1");
    defer alloc.free(second_direct_dense);
    const chunk_dense = try db_mod.internal_keys.derivedEmbeddingArtifactKeyAlloc(alloc, chunk_key, "body_dense_v1");
    defer alloc.free(chunk_dense);
    try db_mod.putDenseEmbeddingArtifactWithCounterForTest(&db, alloc, direct_dense, null, &.{ 0.0, 0.0 });
    try db_mod.putDenseEmbeddingArtifactWithCounterForTest(&db, alloc, second_direct_dense, null, &.{ 0.1, 0.0 });
    try db_mod.putDenseEmbeddingArtifactWithCounterForTest(&db, alloc, chunk_dense, null, &.{ 0.25, 0.0 });
    try std.testing.expectEqual(
        @as(?u64, 3),
        try DB.loadDenseArtifactTargetCounter(alloc, db.core.store, "mixed_dense"),
    );

    const direct_sparse = try db_mod.expectedDocumentEmbeddingArtifactKeyAlloc(alloc, "doc:direct", "title_sparse_v1");
    defer alloc.free(direct_sparse);
    const second_direct_sparse = try db_mod.expectedDocumentEmbeddingArtifactKeyAlloc(alloc, "doc:direct", "summary_sparse_v1");
    defer alloc.free(second_direct_sparse);
    const chunk_sparse = try db_mod.internal_keys.derivedEmbeddingArtifactKeyAlloc(alloc, chunk_key, "body_sparse_v1");
    defer alloc.free(chunk_sparse);
    try db_mod.putSparseEmbeddingArtifactForTest(&db, alloc, direct_sparse, null, &.{1}, &.{1.0});
    try db_mod.putSparseEmbeddingArtifactForTest(&db, alloc, second_direct_sparse, null, &.{1}, &.{0.9});
    try db_mod.putSparseEmbeddingArtifactForTest(&db, alloc, chunk_sparse, null, &.{1}, &.{0.75});

    try std.testing.expectEqual(@as(usize, 3), try db_mod.rebuildDenseIndexFromStoredEmbeddingArtifactsContext(db.async_context, "mixed_dense", 16));
    try std.testing.expectEqual(@as(usize, 3), try db_mod.rebuildSparseIndexFromStoredEmbeddingArtifactsContext(db.async_context, "mixed_sparse", 16));

    var dense = try db.search(alloc, .{
        .index_name = "mixed_dense",
        .query = .{ .dense_knn = .{ .vector = &.{ 0.0, 0.0 }, .k = 2 } },
        .limit = 2,
        .search_effort = 1.0,
    });
    defer dense.deinit();
    try std.testing.expectEqual(@as(usize, 2), dense.hits.len);
    var dense_has_direct = false;
    var dense_has_chunk = false;
    for (dense.hits) |hit| {
        dense_has_direct = dense_has_direct or std.mem.eql(u8, hit.id, "doc:direct");
        dense_has_chunk = dense_has_chunk or std.mem.eql(u8, hit.id, "doc:chunk");
    }
    try std.testing.expect(dense_has_direct and dense_has_chunk);
    for (dense.hits) |hit| try std.testing.expect(hit.artifact_ref != null);

    var dense_prefix = try db.searchDenseProfiled(alloc, .{
        .index_name = "mixed_dense",
        .filter_prefix = "doc:chunk",
        .limit = 2,
        .include_stored = false,
    }, .{ .vector = &.{ 0.0, 0.0 }, .k = 2 });
    defer dense_prefix.result.deinit();
    try std.testing.expectEqual(@as(u32, 1), dense_prefix.result.total_hits);
    try std.testing.expectEqual(@as(usize, 1), dense_prefix.result.hits.len);
    try std.testing.expectEqualStrings("doc:chunk", dense_prefix.result.hits[0].id);
    try std.testing.expectEqual(@as(u64, 1), dense_prefix.profile.native_filter_candidate_count);
    try std.testing.expectEqual(@as(u64, 1), dense_prefix.profile.hbc_exact_vectors_scored);

    var dense_members = try db.search(alloc, .{
        .index_name = "mixed_dense",
        .query = .{ .dense_knn = .{ .vector = &.{ 0.0, 0.0 }, .k = 3 } },
        .limit = 3,
        .search_effort = 1.0,
        .return_mode = .member,
    });
    defer dense_members.deinit();
    try std.testing.expectEqual(@as(usize, 3), dense_members.hits.len);
    for (dense_members.hits) |hit| try std.testing.expect(hit.artifact_ref != null);

    var composed_members = try db.search(alloc, .{
        .dense_queries = &.{
            .{ .name = "first", .index_name = "mixed_dense", .query = .{ .vector = &.{ 0.0, 0.0 }, .k = 3 } },
            .{ .name = "second", .index_name = "mixed_dense", .query = .{ .vector = &.{ 0.0, 0.0 }, .k = 3 } },
        },
        .limit = 3,
        .search_effort = 1.0,
        .return_mode = .member,
    });
    defer composed_members.deinit();
    try std.testing.expectEqual(@as(usize, 3), composed_members.hits.len);
    try std.testing.expectEqual(@as(u32, 3), composed_members.total_hits);
    for (composed_members.hits) |hit| {
        try std.testing.expect(hit.artifact_ref != null);
        try std.testing.expectEqual(@as(usize, 2), hit.index_scores.len);
    }

    var dense_chunk_alias = try db.search(alloc, .{
        .index_name = "mixed_dense",
        .query = .{ .dense_knn = .{ .vector = &.{ 0.0, 0.0 }, .k = 3 } },
        .limit = 3,
        .search_effort = 1.0,
        .return_mode = .chunk,
    });
    defer dense_chunk_alias.deinit();
    try std.testing.expectEqual(dense_members.hits.len, dense_chunk_alias.hits.len);

    try std.testing.expectError(error.UnsupportedHierarchyGrouping, db.search(alloc, .{
        .index_name = "mixed_dense",
        .query = .{ .dense_knn = .{ .vector = &.{ 0.0, 0.0 }, .k = 2 } },
        .limit = 2,
        .search_effort = 1.0,
        .return_mode = .unit,
    }));
    var dense_with_chunks = try db.search(alloc, .{
        .index_name = "mixed_dense",
        .query = .{ .dense_knn = .{ .vector = &.{ 0.0, 0.0 }, .k = 2 } },
        .limit = 2,
        .search_effort = 1.0,
        .return_mode = .parent_with_chunks,
    });
    defer dense_with_chunks.deinit();
    try std.testing.expectEqual(@as(usize, 2), dense_with_chunks.hits.len);
    for (dense_with_chunks.hits) |hit| {
        if (std.mem.eql(u8, hit.id, "doc:direct")) {
            try std.testing.expectEqual(@as(usize, 0), hit.chunk_hits.len);
        } else if (std.mem.eql(u8, hit.id, "doc:chunk")) {
            try std.testing.expectEqual(@as(usize, 1), hit.chunk_hits.len);
            try std.testing.expect(hit.chunk_hits[0].artifact_ref != null);
        }
    }

    var sparse = try db.search(alloc, .{
        .index_name = "mixed_sparse",
        .query = .{ .sparse_knn = .{ .indices = &.{1}, .values = &.{1.0}, .k = 2 } },
        .limit = 2,
        .search_effort = 1.0,
    });
    defer sparse.deinit();
    try std.testing.expectEqual(@as(usize, 2), sparse.hits.len);
    var sparse_has_direct = false;
    var sparse_has_chunk = false;
    for (sparse.hits) |hit| {
        sparse_has_direct = sparse_has_direct or std.mem.eql(u8, hit.id, "doc:direct");
        sparse_has_chunk = sparse_has_chunk or std.mem.eql(u8, hit.id, "doc:chunk");
    }
    try std.testing.expect(sparse_has_direct and sparse_has_chunk);

    var sparse_members = try db.search(alloc, .{
        .index_name = "mixed_sparse",
        .query = .{ .sparse_knn = .{ .indices = &.{1}, .values = &.{1.0}, .k = 3 } },
        .limit = 3,
        .search_effort = 1.0,
        .return_mode = .member,
    });
    defer sparse_members.deinit();
    try std.testing.expectEqual(@as(usize, 3), sparse_members.hits.len);
    for (sparse_members.hits) |hit| try std.testing.expect(hit.artifact_ref != null);

    var composed_sparse_members = try db.search(alloc, .{
        .sparse_queries = &.{
            .{ .name = "first", .index_name = "mixed_sparse", .query = .{ .indices = &.{1}, .values = &.{1.0}, .k = 3 } },
            .{ .name = "second", .index_name = "mixed_sparse", .query = .{ .indices = &.{1}, .values = &.{1.0}, .k = 3 } },
        },
        .limit = 3,
        .search_effort = 1.0,
        .return_mode = .member,
    });
    defer composed_sparse_members.deinit();
    try std.testing.expectEqual(@as(usize, 3), composed_sparse_members.hits.len);
    try std.testing.expectEqual(@as(u32, 3), composed_sparse_members.total_hits);
    for (composed_sparse_members.hits) |hit| {
        try std.testing.expect(hit.artifact_ref != null);
        try std.testing.expectEqual(@as(usize, 2), hit.index_scores.len);
    }

    var sparse_prefix = try db.search(alloc, .{
        .index_name = "mixed_sparse",
        .query = .{ .sparse_knn = .{ .indices = &.{1}, .values = &.{1.0}, .k = 2 } },
        .filter_prefix = "doc:chunk",
        .limit = 2,
        .search_effort = 1.0,
    });
    defer sparse_prefix.deinit();
    try std.testing.expectEqual(@as(u32, 1), sparse_prefix.total_hits);
    try std.testing.expectEqual(@as(usize, 1), sparse_prefix.hits.len);
    try std.testing.expectEqualStrings("doc:chunk", sparse_prefix.hits[0].id);

    var hybrid_prefix = try db.search(alloc, .{
        .full_text_queries = &.{.{
            .name = "text",
            .index_name = "primary_text",
            .query = .{ .match_all = {} },
        }},
        .dense_queries = &.{.{
            .name = "dense",
            .index_name = "mixed_dense",
            .query = .{ .vector = &.{ 0.0, 0.0 }, .k = 2 },
        }},
        .sparse_queries = &.{.{
            .name = "sparse",
            .index_name = "mixed_sparse",
            .query = .{ .indices = &.{1}, .values = &.{1.0}, .k = 2 },
        }},
        .merge_config = .{
            .strategy = .rrf,
            .weights = &.{
                .{ .name = "text", .weight = 1.0 },
                .{ .name = "dense", .weight = 1.0 },
                .{ .name = "sparse", .weight = 1.0 },
            },
        },
        .filter_prefix = "doc:chunk",
        .limit = 10,
    });
    defer hybrid_prefix.deinit();
    try std.testing.expectEqual(@as(u32, 1), hybrid_prefix.total_hits);
    try std.testing.expectEqual(@as(usize, 1), hybrid_prefix.hits.len);
    try std.testing.expectEqualStrings("doc:chunk", hybrid_prefix.hits[0].id);
    try std.testing.expectError(error.UnsupportedHierarchyGrouping, db.search(alloc, .{
        .index_name = "mixed_sparse",
        .query = .{ .sparse_knn = .{ .indices = &.{1}, .values = &.{1.0}, .k = 2 } },
        .limit = 2,
        .search_effort = 1.0,
        .return_mode = .unit_with_chunks,
    }));

    // Removing an embedding row leaves its ANN/posting member behind until
    // retirement catches up. The source still exists and must not establish
    // presence for that member or merge its score into the surviving one.
    try db.core.store.delete(direct_dense);
    try db.core.store.delete(direct_sparse);
    for ([_]bool{ false, true }) |include_stored| {
        for ([_]bool{ false, true }) |is_sparse| {
            const index_name = if (is_sparse) "mixed_sparse" else "mixed_dense";
            const missing_name = if (is_sparse) "title_sparse_v1" else "title_dense_v1";
            const surviving_name = if (is_sparse) "summary_sparse_v1" else "summary_dense_v1";
            const query: db_mod.types.Query = if (is_sparse)
                .{ .sparse_knn = .{ .indices = &.{1}, .values = &.{1.0}, .k = 3 } }
            else
                .{ .dense_knn = .{ .vector = &.{ 0.0, 0.0 }, .k = 3 } };
            var req = db_mod.types.SearchRequest{ .index_name = index_name, .query = query, .limit = 3, .return_mode = .member, .include_stored = include_stored, .search_effort = 1.0 };
            var members = try db.search(alloc, req);
            defer members.deinit();
            try std.testing.expectEqual(@as(usize, 2), members.hits.len);
            try std.testing.expectEqual(@as(u32, 2), members.total_hits);
            for (members.hits) |hit| try std.testing.expect(!std.mem.eql(u8, hit.artifact_ref.?.name, missing_name));

            req.limit = 1;
            var page = try db.search(alloc, req);
            defer page.deinit();
            try std.testing.expectEqual(@as(usize, 1), page.hits.len);
            try std.testing.expectEqualStrings(surviving_name, page.hits[0].artifact_ref.?.name);
            req.offset = 1;
            var tail = try db.search(alloc, req);
            defer tail.deinit();
            try std.testing.expectEqual(@as(usize, 1), tail.hits.len);
            try std.testing.expect(!std.mem.eql(u8, tail.hits[0].artifact_ref.?.name, missing_name));

            var composed_req = db_mod.types.SearchRequest{ .limit = 3, .return_mode = .member, .include_stored = include_stored, .search_effort = 1.0 };
            if (is_sparse) {
                composed_req.sparse_queries = &.{
                    .{ .name = "first", .index_name = index_name, .query = query.sparse_knn },
                    .{ .name = "second", .index_name = index_name, .query = query.sparse_knn },
                };
            } else {
                composed_req.dense_queries = &.{
                    .{ .name = "first", .index_name = index_name, .query = query.dense_knn },
                    .{ .name = "second", .index_name = index_name, .query = query.dense_knn },
                };
            }
            var composed = try db.search(alloc, composed_req);
            defer composed.deinit();
            try std.testing.expectEqual(@as(usize, 2), composed.hits.len);
            for (composed.hits) |hit| {
                try std.testing.expect(!std.mem.eql(u8, hit.artifact_ref.?.name, missing_name));
                try std.testing.expectEqual(@as(usize, 2), hit.index_scores.len);
            }
        }
    }
}

test "db apply fences wait through their borrowed runtime" {
    const vopr = @import("vopr");
    const alloc = std.testing.allocator;
    var runtime_io = try vopr.vopr_io.VoprIo.init(.{ .monotonic_ns = 100 * std.time.ns_per_s });
    defer runtime_io.deinit();
    const io = runtime_io.io();
    var runtime = try db_mod.background_runtime_mod.BackendRuntimeHandle.init(alloc, .{
        .backend = .manual,
        .borrowed_io = .{ .general = io },
    });
    defer runtime.deinit();
    var db = try DB.open(alloc, "/apply-fence-vopr", .{
        .backend_runtime = runtime.ptr(),
        .executor = .{ .backend = .manual },
        .primary_backend = .{ .mem = .{} },
        .physical_root_mode = .external_backend,
        .start_index_workers = false,
        .start_optional_runtimes = false,
        .ttl_cleanup = .{ .enabled = false },
    });
    defer db.close();
    // Fail before attempting a blocking acquisition if DB.open loses ownership.
    try std.testing.expect(db.core.apply_mutex.io != null);
    try std.testing.expectEqual(io.userdata, db.core.apply_mutex.io.?.userdata);
    try std.testing.expectEqual(io.userdata, db.core.snapshot_admission.lock.io.?.userdata);
    try std.testing.expectEqual(io.userdata, db.core.snapshot_replay_admission.lock.io.?.userdata);
    const Work = struct {
        fn run(database: *DB, shared: bool, completed: *bool) !void {
            if (shared) {
                try database.lockApplySharedForPortableRuntime();
                database.core.unlockApplyShared();
            } else {
                // Exercise the maintenance entrypoint from the topology-churn hang.
                _ = try database.enrichment.runArtifactRepairMetadataMaintenanceAfterScan();
            }
            completed.* = true;
        }
    };
    var enabled: vopr.transition.List = .{};
    defer enabled.deinit(alloc);
    var events: vopr.event.Sink = .{};
    defer events.deinit(alloc);
    for ([_]bool{ false, true }) |shared| {
        const mutex = db.core.apply_mutex;
        if (shared) mutex.lockExclusive() else mutex.lockShared();
        var held = true;
        var completed = false;
        var future = io.async(Work.run, .{ &db, shared, &completed });
        defer {
            if (held) {
                if (shared) mutex.unlockExclusive() else mutex.unlockShared();
            }
            _ = runtime_io.cancelAndDrainTasksForTeardown(alloc, 64) catch @panic("apply fence test cleanup failed");
            _ = future.cancel(io) catch {};
        }
        const started_ns = std.Io.Clock.awake.now(io).nanoseconds;
        const scheduler = runtime_io.scheduler();
        for (0..16) |_| {
            if (runtime_io.futureTaskSnapshot(future.any_future.?).?.waiting_on_futex) break;
            enabled.items.clearRetainingCapacity();
            try scheduler.enumerateReady(&enabled, alloc);
            try enabled.canonicalize();
            try std.testing.expect(enabled.items.items.len != 0);
            try scheduler.executeReady(enabled.items.items[0].id, &events, alloc);
        }
        try std.testing.expect(!completed);
        const parked = runtime_io.futureTaskSnapshot(future.any_future.?).?;
        try std.testing.expect(parked.waiting_on_futex);
        try std.testing.expectEqual(null, parked.sleep_deadline_ns);
        if (shared) mutex.unlockExclusive() else mutex.unlockShared();
        held = false;
        for (0..32) |_| {
            if (scheduler.quiescent()) break;
            enabled.items.clearRetainingCapacity();
            try scheduler.enumerateReady(&enabled, alloc);
            try enabled.canonicalize();
            try std.testing.expect(enabled.items.items.len != 0);
            try scheduler.executeReady(enabled.items.items[0].id, &events, alloc);
        }
        try std.testing.expect(scheduler.quiescent());
        try future.await(io);
        try std.testing.expect(completed);
        try std.testing.expectEqual(started_ns, std.Io.Clock.awake.now(io).nanoseconds);
        try std.testing.expectEqual(@as(u64, 0), mutex.exclusive_waiters.load(.acquire));
        try std.testing.expectEqual(@as(u64, 0), mutex.shared_waiters.load(.acquire));
    }
    try runtime_io.ensureNoCapabilityViolation();
}

test "background maintenance services lifecycle runs on borrowed VoprIo" {
    const vopr = @import("vopr");
    const alloc = std.testing.allocator;
    var vopr_io = try vopr.vopr_io.VoprIo.init(.{
        .required = .of(&.{ .clock_read, .task_scheduling, .synchronization, .sleep }),
    });
    defer vopr_io.deinit();
    var runtime_owners_closed = false;
    var backend_runtime = try db_mod.background_runtime_mod.BackendRuntimeHandle.init(alloc, .{
        .backend = .manual,
        .borrowed_io = .{ .general = vopr_io.io() },
    });
    defer if (!runtime_owners_closed) backend_runtime.deinit();

    var path_tmp = try db_mod.TestDirectory.init("db");
    defer path_tmp.cleanup();
    const path = path_tmp.path().ptr;
    var db = try DB.open(alloc, std.mem.span(path), .{
        .backend_runtime = backend_runtime.ptr(),
        .executor = .{ .backend = .manual },
        .primary_backend = .{ .mem = .{} },
        .physical_root_mode = .external_backend,
        .start_index_workers = false,
        .start_optional_runtimes = true,
        .start_optional_runtime_workers = false,
        .enrichment = .{ .enable_without_producers = true },
    });
    defer if (!runtime_owners_closed) db.close();

    const resources = db.core.batchExecutionResources();
    var text_merge = try db_mod.text_merge_runtime_mod.TextMergeRuntime.init(
        alloc,
        resources.index_manager,
        resources.apply_mutex,
        backend_runtime.ptr(),
        .{ .enabled = true, .idle_interval_ms = 1 },
    );
    defer if (!runtime_owners_closed) text_merge.deinit();
    var sparse_compaction = try db_mod.sparse_compaction_runtime_mod.SparseCompactionRuntime.init(
        alloc,
        resources.index_manager,
        resources.apply_mutex,
        backend_runtime.ptr(),
        .{ .enabled = true, .idle_interval_ms = 1 },
    );
    defer if (!runtime_owners_closed) sparse_compaction.deinit();

    var lifecycle_ok = false;
    const Lifecycle = struct {
        fn cycle(runtime: anytype) bool {
            runtime.start() catch return false;
            if (!runtime.isStarted()) return false;
            if (!runtime.pause()) return false;
            if (runtime.isStarted()) return false;
            runtime.resumeAfterPause() catch return false;
            if (!runtime.isStarted()) return false;
            if (!runtime.stop()) return false;
            return !runtime.isStarted();
        }

        fn run(
            database: *DB,
            text: *db_mod.text_merge_runtime_mod.TextMergeRuntime,
            sparse: *db_mod.sparse_compaction_runtime_mod.SparseCompactionRuntime,
            backend_owner: *db_mod.background_runtime_mod.BackendRuntimeHandle,
            closed: *bool,
            passed: *bool,
        ) void {
            // Close consumers before their shared scheduler, on the injected
            // executor, so quiescence includes service-owner teardown.
            defer {
                sparse.deinit();
                text.deinit();
                database.close();
                backend_owner.deinit();
                closed.* = true;
            }
            const enrichment = database.enrichment.runtime orelse return;
            enrichment.start() catch return;
            if (!enrichment.isStarted()) return;
            enrichment.stop();
            if (enrichment.isStarted()) return;

            const resolution = database.enrichment.resolution.runtime orelse return;
            resolution.start() catch return;
            if (!resolution.worker_started.load(.acquire)) return;
            resolution.stop();
            if (resolution.worker_started.load(.acquire)) return;

            const promotion = database.enrichment.promotion.runtime orelse return;
            promotion.start() catch return;
            if (!promotion.worker_started.load(.acquire)) return;
            promotion.stop();
            if (promotion.worker_started.load(.acquire)) return;

            if (!cycle(text)) return;
            if (!cycle(sparse)) return;
            passed.* = true;
        }
    };
    _ = vopr_io.io().async(Lifecycle.run, .{ &db, &text_merge, &sparse_compaction, &backend_runtime, &runtime_owners_closed, &lifecycle_ok });
    const scheduler = vopr_io.scheduler();
    var enabled: vopr.transition.List = .{};
    defer enabled.deinit(alloc);
    var events: vopr.event.Sink = .{};
    defer events.deinit(alloc);
    while (!scheduler.quiescent()) {
        enabled.items.clearRetainingCapacity();
        try scheduler.enumerateReady(&enabled, alloc);
        try enabled.canonicalize();
        try std.testing.expect(enabled.items.items.len != 0);
        try scheduler.executeReady(enabled.items.items[0].id, &events, alloc);
    }
    try std.testing.expect(lifecycle_ok);
    try vopr_io.ensureNoCapabilityViolation();
}

test "db enrichment reconfigure preserves active runtime when replacement cannot initialize" {
    const alloc = std.testing.allocator;

    var deterministic = db_mod.embedder_mod.DeterministicDenseEmbedder{};
    var path_tmp = try db_mod.TestDirectory.init("db");
    defer path_tmp.cleanup();
    const path = path_tmp.path().ptr;
    defer db_mod.cleanupTempDir(path);

    var db = try DB.open(alloc, std.mem.span(path), .{
        .enrichment = .{ .dense_embedder = deterministic.interface() },
    });
    defer db.close();
    const original_backend_runtime = db.backend_runtime;
    defer db.backend_runtime = original_backend_runtime;

    const original_runtime = db.enrichment.runtime orelse return error.TestUnexpectedResult;
    var manual_runtime = try db_mod.background_runtime_mod.BackendRuntimeHandle.init(alloc, .{ .backend = .manual });
    defer manual_runtime.deinit();

    db.backend_runtime = manual_runtime.ptr();
    var replacement_embedder = db_mod.embedder_mod.DeterministicDenseEmbedder{};
    try std.testing.expectError(error.MissingBackendRuntimeIo, db.enrichment.reconfigureRuntime(.{
        .dense_embedder = replacement_embedder.interface(),
    }));
    try std.testing.expectEqual(original_runtime, db.enrichment.runtime.?);
}

test "db enrichment reconfigure inherits resident execution and security capabilities" {
    const alloc = std.testing.allocator;
    var path_tmp = try db_mod.TestDirectory.init("db");
    defer path_tmp.cleanup();
    const path = path_tmp.path().ptr;
    defer db_mod.cleanupTempDir(path);

    var remote_content = db_mod.scraping.RemoteContentConfig{
        .security = .{ .block_private_ips = false },
    };
    var db = try DB.open(alloc, std.mem.span(path), .{
        .remote_content = &remote_content,
    });
    defer db.close();

    var replacement_embedder = db_mod.embedder_mod.DeterministicDenseEmbedder{};
    try db.enrichment.reconfigureRuntime(.{
        .dense_embedder = replacement_embedder.interface(),
    });

    const runtime = db.enrichment.runtime orelse return error.TestUnexpectedResult;
    try std.testing.expectEqual(&remote_content, runtime.config.remote_content.?);
    try std.testing.expectEqual(db.core.index_manager.resource_manager, runtime.config.resource_manager);
    try std.testing.expect(runtime.config.io != null);
}

test "db enrichment reconfigure refreshes durable state after old worker joins" {
    const alloc = std.testing.allocator;
    var path_tmp = try db_mod.TestDirectory.init("db");
    defer path_tmp.cleanup();
    const path = path_tmp.path().ptr;
    defer db_mod.cleanupTempDir(path);

    var original_embedder = db_mod.embedder_mod.DeterministicDenseEmbedder{};
    var db = try DB.open(alloc, std.mem.span(path), .{
        .start_optional_runtime_workers = false,
        .enrichment = .{ .dense_embedder = original_embedder.interface() },
    });
    defer db.close();
    try db.addIndex(.{
        .name = "semantic",
        .kind = .dense_vector,
        .config_json = "{\"field\":\"embedding\",\"dims\":3,\"generator\":{\"kind\":\"dense_embedding\",\"source_field\":\"body\"}}",
    });
    try db.batch(.{
        .writes = &.{.{ .key = "doc:a", .value = "{\"body\":\"alpha\"}" }},
        .sync_level = .write,
    });
    try std.testing.expect(db.core.hasGeneratedEnrichmentTargets());
    try std.testing.expect(db.core.nextEnrichmentSequence() != 0);
    const original_runtime = db.enrichment.runtime.?;
    original_runtime.embed_batches_started = 2;
    original_runtime.embed_batches_completed = 2;
    original_runtime.embed_items_started = 3;
    original_runtime.embed_items_completed = 3;
    original_runtime.last_embed_batch_items = 1;
    original_runtime.last_embed_batch_bytes = 42;
    original_runtime.last_embed_batch_max_bytes = 42;
    original_runtime.last_embed_batch_completed_ms = 1234;
    original_runtime.last_embed_batch_ns = 99;
    original_runtime.total_embed_ns = 199;

    const LateOldWorkerPublish = struct {
        fn run(target_db: *DB) !void {
            var status = try db_mod.enrichment_state.loadRuntimeStatus(
                target_db.runtime_alloc,
                target_db.core.store,
                db_mod.enrichment_runtime_mod.scope_name,
            );
            status.error_count = 9;
            status.retryable_error_count = 7;
            status.fatal_error_count = 2;
            status.consecutive_retry_count = 3;
            status.retry_failure_fingerprint = 0xfeed;
            status.retry_failure_count = 2;
            status.worker_failed = true;
            status.terminal_failure_min_sequence = 41;
            status.terminal_failure_max_sequence = 43;
            try db_mod.enrichment_state.saveRuntimeStatus(
                target_db.core.store,
                db_mod.enrichment_runtime_mod.scope_name,
                status,
            );
        }
    };
    DB.test_enrichment_reconfigure_after_detached_hook = LateOldWorkerPublish.run;
    defer DB.test_enrichment_reconfigure_after_detached_hook = null;

    var replacement_embedder = db_mod.embedder_mod.DeterministicDenseEmbedder{};
    try db.enrichment.reconfigureRuntime(.{ .dense_embedder = replacement_embedder.interface() });
    const replacement = db.enrichment.runtime.?;
    try std.testing.expect(replacement != original_runtime);
    replacement.stop();
    const stats = replacement.stats();
    try std.testing.expectEqual(@as(u64, 9), stats.error_count);
    try std.testing.expectEqual(@as(u64, 7), stats.retryable_error_count);
    try std.testing.expectEqual(@as(u64, 2), stats.fatal_error_count);
    try std.testing.expectEqual(@as(u64, 2), stats.embed_batches_started);
    try std.testing.expectEqual(@as(u64, 2), stats.embed_batches_completed);
    try std.testing.expectEqual(@as(u64, 3), stats.embed_items_started);
    try std.testing.expectEqual(@as(u64, 3), stats.embed_items_completed);
    try std.testing.expectEqual(@as(u64, 42), stats.last_embed_batch_bytes);
    try std.testing.expectEqual(@as(u64, 1234), stats.last_embed_batch_completed_ms);
    try std.testing.expectEqual(@as(u64, 99), stats.last_embed_batch_ns);
    try std.testing.expectEqual(@as(u64, 199), stats.total_embed_ns);
    try std.testing.expectEqual(@as(u32, 3), stats.consecutive_retry_count);
    try std.testing.expectEqual(@as(u64, 0xfeed), replacement.retry_failure_fingerprint);
    try std.testing.expectEqual(@as(u32, 2), replacement.retry_failure_count);
    try std.testing.expect(stats.worker_failed);
    try std.testing.expectEqual(@as(u64, 41), replacement.terminal_failure_min_sequence);
    try std.testing.expectEqual(@as(u64, 43), replacement.terminal_failure_max_sequence);

    const persisted = try db_mod.enrichment_state.loadRuntimeStatus(
        alloc,
        db.core.store,
        db_mod.enrichment_runtime_mod.scope_name,
    );
    try std.testing.expectEqual(@as(u32, 3), persisted.consecutive_retry_count);
    try std.testing.expectEqual(@as(u64, 0xfeed), persisted.retry_failure_fingerprint);
    try std.testing.expectEqual(@as(u32, 2), persisted.retry_failure_count);
    try std.testing.expect(persisted.worker_failed);
}

test "db enrichment status does not wait for lifecycle mutation" {
    const alloc = std.testing.allocator;
    var deterministic = db_mod.embedder_mod.DeterministicDenseEmbedder{};
    var path_tmp = try db_mod.TestDirectory.init("db");
    defer path_tmp.cleanup();
    const path = path_tmp.path().ptr;
    defer db_mod.cleanupTempDir(path);

    var db = try DB.open(alloc, std.mem.span(path), .{
        .enrichment = .{ .dense_embedder = deterministic.interface() },
    });
    defer db.close();

    db_mod.lockAtomicWithBackoff(&db.async_context.enrichment_lifecycle_mutex);
    const fallback = db_mod.types.EnrichmentStats{
        .target_sequence = 41,
        .applied_sequence = 37,
    };
    const status = db.enrichment.statsWithSupervisorState(fallback);
    db.async_context.enrichment_lifecycle_mutex.unlock();

    try std.testing.expect(status.enabled);
    try std.testing.expectEqual(@as(u64, 41), status.target_sequence);
    try std.testing.expectEqual(@as(u64, 37), status.applied_sequence);
    try std.testing.expect(status.retrying);
    try std.testing.expect(!status.worker_failed);
}

test "db runtime-only status overlay preserves a resident enrichment worker" {
    const alloc = std.testing.allocator;
    var deterministic = db_mod.embedder_mod.DeterministicDenseEmbedder{};
    var path_tmp = try db_mod.TestDirectory.init("db");
    defer path_tmp.cleanup();
    const path = path_tmp.path().ptr;
    defer db_mod.cleanupTempDir(path);

    var db = try DB.open(alloc, std.mem.span(path), .{
        .enrichment = .{ .dense_embedder = deterministic.interface() },
    });
    defer db.close();
    try db.addIndex(.{
        .name = "semantic",
        .kind = .dense_vector,
        .config_json =
        \\{"field":"embedding","dims":3,"generator":{"kind":"dense_embedding","source_field":"body","embedding_name":"semantic"}}
        ,
    });

    // Every fresh status constructor applies the runtime-only overlay after it
    // releases the index apply lock. Otherwise callers that do not start from
    // a cached snapshot lose per-index activity even while the worker is live.
    const consistent = try db.runtimeStatusStatsConsistent(alloc);
    defer db_mod.types.freeDBStats(alloc, consistent);
    try std.testing.expectEqual(@as(usize, 1), consistent.indexes.len);
    try std.testing.expect(consistent.indexes[0].embedding_activity_observed);
    try std.testing.expect(consistent.indexes[0].embedding_activity_sample_fresh);
    try std.testing.expect(consistent.indexes[0].embedding_activity.epoch != 0);

    // Model a durable/query-only recovery snapshot. A resident writer overlay
    // must replace its absent producer diagnostics immediately rather than
    // waiting for the periodic full status refresh.
    const coverage_generation = db.core.index_manager.coverageGenerationForIndex("semantic") orelse 0;
    var indexes = [_]db_mod.types.DBIndexStats{.{
        .name = "semantic",
        .kind = .dense_vector,
        .coverage_generation = coverage_generation,
    }};
    var recovered: db_mod.types.DBStats = .{ .indexes = indexes[0..] };
    db.overlayRuntimeStatusRuntimeBestEffort(&recovered);
    try std.testing.expect(recovered.enrichment.enabled);
    try std.testing.expect(recovered.enrichment.worker_started);
    try std.testing.expect(indexes[0].embedding_activity_observed);
    try std.testing.expect(indexes[0].embedding_activity_sample_fresh);
    try std.testing.expect(indexes[0].embedding_activity.epoch != 0);

    // A lifecycle lock miss is an observation gap, not evidence that the
    // worker disappeared. Preserve the exact last owner sample within the
    // shared heartbeat TTL even though the durable status projection is
    // reconstructed for the next poll.
    const observed_epoch = indexes[0].embedding_activity.epoch;
    const observed_sequence = indexes[0].embedding_activity.sample_sequence;
    indexes[0] = .{
        .name = "semantic",
        .kind = .dense_vector,
        .coverage_generation = coverage_generation,
    };
    db_mod.lockAtomicWithBackoff(&db.async_context.enrichment_lifecycle_mutex);
    db.overlayRuntimeStatusRuntimeBestEffort(&recovered);
    db.async_context.enrichment_lifecycle_mutex.unlock();
    try std.testing.expect(indexes[0].embedding_activity_observed);
    try std.testing.expect(!indexes[0].embedding_activity_sample_fresh);
    try std.testing.expectEqual(observed_epoch, indexes[0].embedding_activity.epoch);
    try std.testing.expectEqual(observed_sequence, indexes[0].embedding_activity.sample_sequence);

    // A stale receipt cannot survive indefinitely if the owner is wedged or
    // gone while lifecycle access remains contended.
    db_mod.lockAtomic(&db.enrichment.embedding_activity.mutex);
    const cached = db.enrichment.embedding_activity.cache.getPtr("semantic") orelse return error.TestUnexpectedResult;
    cached.observed_at_ms -|= db_mod.types.embedding_activity_retention_ms;
    db.enrichment.embedding_activity.mutex.unlock();
    indexes[0] = .{
        .name = "semantic",
        .kind = .dense_vector,
        .coverage_generation = coverage_generation,
    };
    db_mod.lockAtomicWithBackoff(&db.async_context.enrichment_lifecycle_mutex);
    db.overlayRuntimeStatusRuntimeBestEffort(&recovered);
    db.async_context.enrichment_lifecycle_mutex.unlock();
    try std.testing.expect(!indexes[0].embedding_activity_observed);
    try std.testing.expect(!indexes[0].embedding_activity_sample_fresh);
    try std.testing.expectEqual(@as(u64, 0), indexes[0].embedding_activity.epoch);

    // A new incarnation under the same index name must never inherit the old
    // worker's retained activity.
    db.overlayRuntimeStatusRuntimeBestEffort(&recovered);
    indexes[0] = .{
        .name = "semantic",
        .kind = .dense_vector,
        .coverage_generation = coverage_generation +% 1,
    };
    db_mod.lockAtomicWithBackoff(&db.async_context.enrichment_lifecycle_mutex);
    db.overlayRuntimeStatusRuntimeBestEffort(&recovered);
    db.async_context.enrichment_lifecycle_mutex.unlock();
    try std.testing.expect(!indexes[0].embedding_activity_observed);
    try std.testing.expectEqual(@as(u64, 0), indexes[0].embedding_activity.epoch);
}

test "document extraction template prompt failure is rejected before persistence" {
    const alloc = std.testing.allocator;

    var path_tmp = try db_mod.TestDirectory.init("db");
    defer path_tmp.cleanup();
    const path = path_tmp.path().ptr;
    defer db_mod.cleanupTempDir(path);

    var db = try DB.open(alloc, std.mem.span(path), .{
        .start_index_workers = false,
        .ttl_cleanup = .{ .enabled = false },
    });
    defer db.close();

    try db.enrichment.addEnrichment(.{
        .name = "document_units_v1",
        .kind = .asset,
        .template = "<<<error:status=413 message=StreamTooLong>>> fallback text",
        .content_type = "application/json",
        .producer_json = "{\"type\":\"document_extraction\",\"config\":{}}",
    });

    try std.testing.expectError(error.PermanentPromptFailure, db.batch(.{
        .writes = &.{.{
            .key = "doc:templated-prompt-failure",
            .value = "{\"url\":\"data:text/plain;base64,Zm9v\"}",
        }},
        .sync_level = .write,
    }));
}

test "db enrichment status changes notify query visibility hook" {
    const alloc = std.testing.allocator;

    var path_tmp = try db_mod.TestDirectory.init("db");
    defer path_tmp.cleanup();
    const path = path_tmp.path().ptr;
    defer db_mod.cleanupTempDir(path);

    var deterministic = db_mod.embedder_mod.DeterministicDenseEmbedder{};
    var db = try DB.open(alloc, std.mem.span(path), .{
        .enrichment = .{ .dense_embedder = deterministic.interface() },
        // Assert the explicit status edge without racing startup/replay
        // notifications from independently scheduled workers.
        .start_index_workers = false,
        .start_optional_runtime_workers = false,
    });
    defer db.close();

    const HookCtx = struct {
        status_calls: u64 = 0,
        saw_db: bool = false,
        change: ?db_mod.QueryVisibilityChange = null,

        fn onChange(ptr: *anyopaque, event: db_mod.QueryVisibilityEvent) void {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            // Attachment also replays existing repair debt. That is a valid
            // visibility edge, independent of the enrichment status contract.
            if (event.change != .status) return;
            self.status_calls += 1;
            self.change = event.change;
        }
    };
    var hook_ctx = HookCtx{};
    db.setQueryVisibilityHook(.{
        .ptr = &hook_ctx,
        .on_change = HookCtx.onChange,
    });

    defer db.setQueryVisibilityHook(null);

    try std.testing.expectEqual(@as(u64, 0), hook_ctx.status_calls);
    try db.enrichment.runtime.?.markAppliedThrough(1);

    try std.testing.expectEqual(@as(u64, 1), hook_ctx.status_calls);
    try std.testing.expectEqual(db_mod.QueryVisibilityChange.status, hook_ctx.change.?);

    try db.enrichment.runtime.?.markAppliedThrough(2);
    try std.testing.expectEqual(@as(u64, 2), hook_ctx.status_calls);
    try std.testing.expectEqual(db_mod.QueryVisibilityChange.status, hook_ctx.change.?);
}

test "db generated downstream indexes are exact convergence targets at source commit including implicit chunk deletion" {
    const alloc = std.testing.allocator;
    var path_tmp = try db_mod.TestDirectory.init("db");
    defer path_tmp.cleanup();
    const path = path_tmp.path().ptr;
    defer db_mod.cleanupTempDir(path);
    var db = try DB.open(alloc, std.mem.span(path), .{
        .start_index_workers = false,
        .executor = .{ .backend = .manual },
    });
    defer db.close();
    try db.addIndex(.{ .name = "default", .kind = .full_text, .config_json = "{}" });
    try db.enrichment.addEnrichment(.{
        .name = "body_chunks",
        .kind = .chunk,
        .field = "body",
        .chunk_size = 64,
        .full_text_index = true,
    });
    const key = try db_mod.internal_keys.chunkArtifactKeyAlloc(alloc, "doc:a", "body_chunks", 0);
    defer alloc.free(key);
    var context = try db.core.index_manager.acquireTextPublicationContext(alloc, "default");
    defer context.deinit();
    try std.testing.expect(try db.core.index_manager.textPublicationContextConsumesKeyAssumeCatalogLocked("default", context, key));
    const batch: db_mod.derived_types.DerivedBatch = .{ .deleted_keys = &.{key} };
    var targets = try db_mod.collectManagedSyncTargets(alloc, db.core.index_manager, batch);
    defer targets.deinit(alloc);
    try std.testing.expectEqual(@as(usize, 1), targets.all_indexes.len);
    try std.testing.expectEqualStrings("default", targets.all_indexes[0]);
    try std.testing.expectEqual(db_mod.IndexTargetVisibility.ServingSetEffect.may_reduce, targets.target_identities[0].serving_set_effect);
    var replay_targets = try db_mod.collectManagedSyncTargetsForRecord(alloc, db.core.index_manager, .{ .sequence = 7, .deleted_doc_keys = &.{key} });
    defer replay_targets.deinit(alloc);
    try std.testing.expectEqual(@as(usize, 1), replay_targets.all_indexes.len);
    const deletes = try db_mod.collectTextReplayDeleteKeys(alloc, db.core.index_manager, batch, "default", context);
    defer alloc.free(deletes);
    try std.testing.expectEqual(@as(usize, 1), deletes.len);
    try std.testing.expectEqualStrings(key, deletes[0]);
}

test "db starts resolver replay workers only while resolver catalog is configured" {
    const alloc = std.testing.allocator;

    var path_tmp = try db_mod.TestDirectory.init("db");
    defer path_tmp.cleanup();
    const path = path_tmp.path().ptr;
    defer db_mod.cleanupTempDir(path);

    var db = try DB.open(alloc, std.mem.span(path), .{});
    defer db.close();

    try std.testing.expect(db.enrichment.resolution.runtime != null);
    try std.testing.expect(db.enrichment.promotion.runtime != null);
    try std.testing.expect(!db.enrichment.resolution.runtime.?.worker_started.load(.acquire));
    try std.testing.expect(!db.enrichment.promotion.runtime.?.worker_started.load(.acquire));

    try db.addIndex(.{
        .name = "relations_graph",
        .kind = .graph,
        .config_json =
        \\{
        \\  "source":{"artifact":"relations_v1","path":"$.relations[*]","format":"extraction_relation"},
        \\  "artifact":{"name":"relations_v1","kind":"asset","source":{"type":"field","value":"relations"},"content_type":"application/json"}
        \\}
        ,
    });
    try std.testing.expect(!db.enrichment.resolution.runtime.?.worker_started.load(.acquire));
    try std.testing.expect(!db.enrichment.promotion.runtime.?.worker_started.load(.acquire));

    try db.batch(.{
        .writes = &.{.{
            .key = "doc:no-resolver",
            .value = "{\"title\":\"ordinary write\"}",
        }},
        .sync_level = .write,
    });
    try db.runUntilIdle();
    const no_resolver_pending = db.pendingWorkStats();
    try std.testing.expect(!no_resolver_pending.resolution.enabled);
    try std.testing.expect(!no_resolver_pending.resolution.catch_up_required);
    try std.testing.expect(!no_resolver_pending.promotion.enabled);
    try std.testing.expect(!no_resolver_pending.promotion.catch_up_required);
    try std.testing.expect(!db.enrichment.resolution.runtime.?.worker_started.load(.acquire));
    try std.testing.expect(!db.enrichment.promotion.runtime.?.worker_started.load(.acquire));
    try db.enrichment.drainResolverBackfill();
    try std.testing.expectError(error.NotFound, db.core.store.get(alloc, db_mod.resolver_catalog_mod.reresolve_resume_key));
    try std.testing.expectError(error.NotFound, db.core.store.get(alloc, db_mod.resolver_catalog_mod.reresolve_repair_resume_key));

    try db.addResolver(.{
        .name = "kg_lifecycle",
        .table = "entities",
        .source_artifact = "relations_v1",
        .resolution_artifact = "resolution_lifecycle_v1",
        .key_template = "{{ lower _entity.label }}/{{ slug _entity.text }}",
        .config_generation = 1,
    });
    try std.testing.expect(db.enrichment.resolution.runtime.?.worker_started.load(.acquire));
    try std.testing.expect(db.enrichment.promotion.runtime.?.worker_started.load(.acquire));

    try std.testing.expect(try db.removeResolver("kg_lifecycle"));
    try std.testing.expect(!db.enrichment.resolution.runtime.?.worker_started.load(.acquire));
    try std.testing.expect(!db.enrichment.promotion.runtime.?.worker_started.load(.acquire));
}

test "db runUntilIdle catches resolution up to the committed tail after a missed notification" {
    const alloc = std.testing.allocator;

    var path_tmp = try db_mod.TestDirectory.init("db");
    defer path_tmp.cleanup();
    const path = path_tmp.path().ptr;
    defer db_mod.cleanupTempDir(path);

    var db = try DB.open(alloc, std.mem.span(path), .{
        .start_index_workers = false,
        .start_optional_runtime_workers = false,
    });
    defer db.close();

    // A graph index drives production of the relations_v1 asset (extraction)
    // artifact; the resolver consumes the same artifact.
    try db.addIndex(.{
        .name = "relations_graph",
        .kind = .graph,
        .config_json =
        \\{
        \\  "source":{"artifact":"relations_v1","path":"$.relations[*]","format":"extraction_relation"},
        \\  "artifact":{"name":"relations_v1","kind":"asset","source":{"type":"field","value":"relations"},"content_type":"application/json"}
        \\}
        ,
    });
    try db.addResolver(.{
        .name = "knowledge_graph",
        .table = "entities",
        .source_artifact = "relations_v1",
        .resolution_artifact = "resolution_v1",
        .key_template = "{{ lower _entity.label }}/{{ slug _entity.text }}",
        .config_generation = 1,
    });

    try db.batch(.{
        .writes = &.{
            .{
                .key = "doc:a",
                .value =
                \\{"relations":{"entities":[{"id":"e0","label":"person","text":"Ada Lovelace"},{"id":"e1","label":"org","text":"Antfly"}]}}
                ,
            },
        },
        .sync_level = .enrichments,
    });

    // Runtime notifications are wake-up optimizations, not correctness
    // state. Simulate losing the notification after the source artifact was
    // committed; runUntilIdle must recover from the durable replay tail.
    const resolution_runtime = db.enrichment.resolution.runtime.?;
    resolution_runtime.target_sequence.store(
        resolution_runtime.applied_sequence.load(.acquire),
        .release,
    );
    try db.runUntilIdle();

    const resolution_key = try db_mod.internal_keys.resolutionArtifactKeyAlloc(alloc, "doc:a", "resolution_v1");
    defer alloc.free(resolution_key);
    const raw = try db.core.store.get(alloc, resolution_key);
    defer alloc.free(raw);

    var parsed = try std.json.parseFromSlice(std.json.Value, alloc, raw, .{});
    defer parsed.deinit();
    try std.testing.expectEqual(@as(i64, 1), parsed.value.object.get("config_generation").?.integer);
    const entities = parsed.value.object.get("entities").?.array.items;
    try std.testing.expectEqual(@as(usize, 2), entities.len);
    try std.testing.expectEqualStrings("person/ada_lovelace", entities[0].object.get("doc_ref").?.object.get("key").?.string);
    try std.testing.expectEqualStrings("org/antfly", entities[1].object.get("doc_ref").?.object.get("key").?.string);
}

test "db resolver workers recover pending journal targets after reopen without new writes" {
    for ([_]bool{ false, true }) |resolved_before_close| {
        const alloc = std.testing.allocator;
        var path_tmp = try db_mod.TestDirectory.init("db");
        defer path_tmp.cleanup();
        const path = path_tmp.path().ptr;
        defer db_mod.cleanupTempDir(path);
        var sink = db_mod.FakePromotionSink{ .alloc = alloc };
        defer sink.deinit();
        var db = try DB.open(alloc, std.mem.span(path), .{
            .start_index_workers = false,
            .start_optional_runtime_workers = false,
            .start_resolver_workers = false,
            .entity_sink = sink.sink(),
            .enrichment = .{ .enable_without_producers = true },
        });
        defer db.close();
        try db.addIndex(.{
            .name = "relations_graph",
            .kind = .graph,
            .config_json =
            \\{"source":{"artifact":"relations_v1","path":"$.relations[*]","format":"extraction_relation"},"artifact":{"name":"relations_v1","kind":"asset","source":{"type":"field","value":"relations"},"content_type":"application/json"}}
            ,
        });
        try db.addResolver(.{
            .name = "kg",
            .table = "entities",
            .source_artifact = "relations_v1",
            .resolution_artifact = "resolution_v1",
            .key_template = "{{ lower _entity.label }}/{{ slug _entity.text }}",
            .config_generation = 1,
        });
        try db.batch(.{
            .writes = &.{.{ .key = "doc:a", .value =
            \\{"relations":{"entities":[{"id":"e0","label":"person","text":"Ada Lovelace"},{"id":"e1","label":"org","text":"Antfly"}]}}
            }},
            .sync_level = .write,
        });
        try db.enrichment.runUntil(db.core.nextDerivedSequence());
        if (resolved_before_close) try db.enrichment.resolution.runtime.?.catchUp();
        const pending_hint: db_mod.change_journal_mod.TargetHint = if (resolved_before_close) .promotion else .resolution;
        try std.testing.expect(try db.core.store.latestReplaySequenceForHint(pending_hint, 0) > 0);
        // Graph replay can finish while the independent cross-table promotion
        // is still pending. Startup must not infer its debt from graph debt.
        try db.runDerivedUntil(db.core.nextDerivedSequence());
        try std.testing.expectEqual(@as(usize, 0), sink.count());
        db.close();
        db = try DB.open(alloc, std.mem.span(path), .{
            .start_resolver_workers = false,
            .entity_sink = sink.sink(),
        });
        if (resolved_before_close) {
            try std.testing.expect(db.enrichment.promotionStageStats().catch_up_required);
        } else {
            try std.testing.expect(db.enrichment.resolutionStageStats().catch_up_required);
        }
        try db.enrichment.activateResolverReplayRuntimes();
        const io = db.backend_runtime.controlIo() orelse db.backend_runtime.io().?;
        for (0..1000) |_| {
            // Publishing promotion output does not certify the independent
            // resolution checkpoint/backfill worker has finished its turn.
            if (sink.count() == 2 and
                !db.enrichment.resolutionStageStats().catch_up_required and
                !db.enrichment.promotionStageStats().catch_up_required) break;
            try io.sleep(.fromMilliseconds(5), .awake);
        }
        try std.testing.expectEqual(@as(usize, 2), sink.count());
        try std.testing.expect(!db.enrichment.resolutionStageStats().catch_up_required);
        try std.testing.expect(!db.enrichment.promotionStageStats().catch_up_required);
    }
}

test "db resolver worker resumes durable backfill after deferred activation and reopen" {
    for ([_]bool{ false, true }) |reopen| {
        const alloc = std.testing.allocator;

        var path_tmp = try db_mod.TestDirectory.init("db");
        defer path_tmp.cleanup();
        const path = path_tmp.path().ptr;
        defer db_mod.cleanupTempDir(path);

        var db = try DB.open(alloc, std.mem.span(path), .{ .start_resolver_workers = false });
        defer db.close();

        try db.addIndex(.{
            .name = "relations_graph",
            .kind = .graph,
            .config_json =
            \\{
            \\  "source":{"artifact":"relations_v1","path":"$.relations[*]","format":"extraction_relation"},
            \\  "artifact":{"name":"relations_v1","kind":"asset","source":{"type":"field","value":"relations"},"content_type":"application/json"}
            \\}
            ,
        });

        try db.batch(.{
            .writes = &.{.{
                .key = "doc:a",
                .value =
                \\{"relations":{"entities":[{"id":"e0","label":"person","text":"Ada Lovelace"}]}}
                ,
            }},
            .sync_level = .enrichments,
        });
        try db.runUntilIdle();

        const cfg: db_mod.index_manager_mod.ResolverConfig = .{
            .name = "kg_background",
            .table = "entities",
            .source_artifact = "relations_v1",
            .resolution_artifact = "resolution_background_v1",
            .key_template = "{{ lower _entity.label }}/{{ slug _entity.text }}",
            .config_generation = 1,
        };
        _ = try db.enrichment.upsertResolverWithResultOptions(cfg, .{ .drain_backfill = false });
        try std.testing.expect(!db.enrichment.resolution.runtime.?.worker_started.load(.acquire));
        try std.testing.expect(db.enrichment.resolutionStageStats().catch_up_required);
        if (reopen) {
            db.close();
            db = try DB.open(alloc, std.mem.span(path), .{ .start_resolver_workers = false });
            try std.testing.expect(!db.enrichment.resolution.runtime.?.worker_started.load(.acquire));
            try std.testing.expect(db.enrichment.resolutionStageStats().catch_up_required);
        }
        // No synchronous catchUp/runUntilIdle or new write may drive this work.
        try db.enrichment.activateResolverReplayRuntimes();
        const resolution_key = try db_mod.internal_keys.resolutionArtifactKeyAlloc(alloc, "doc:a", cfg.resolution_artifact);
        defer alloc.free(resolution_key);
        const io = db.backend_runtime.controlIo() orelse db.backend_runtime.io().?;
        var completed = false;
        for (0..1000) |_| {
            if (db.core.store.get(alloc, resolution_key)) |raw| {
                defer alloc.free(raw);
                if (std.mem.indexOf(u8, raw, "person/ada_lovelace") != null and !db.enrichment.resolutionStageStats().catch_up_required) {
                    completed = true;
                    break;
                }
            } else |err| if (err != error.NotFound) return err;
            try io.sleep(.fromMilliseconds(5), .awake);
        }
        try std.testing.expect(completed);
        try std.testing.expectError(error.NotFound, db.core.store.get(alloc, db_mod.resolver_catalog_mod.reresolve_resume_key));
        try std.testing.expectError(error.NotFound, db.core.store.get(alloc, db_mod.resolver_catalog_mod.reresolve_repair_resume_key));
    }
}

test "db managed resolver changes fence in-flight replay and reset durable cursors" {
    for ([_]bool{ false, true }) |follower| {
        const alloc = std.testing.allocator;

        var path_tmp = try db_mod.TestDirectory.init("db");
        defer path_tmp.cleanup();
        const path = path_tmp.path().ptr;
        defer db_mod.cleanupTempDir(path);

        var sink = db_mod.FakePromotionSink{ .alloc = alloc };
        defer sink.deinit();
        var db = try DB.open(alloc, std.mem.span(path), .{ .start_resolver_workers = false, .entity_sink = sink.sink() });
        defer db.close();

        try db.addIndex(.{
            .name = "relations_graph",
            .kind = .graph,
            .config_json =
            \\{
            \\  "source":{"artifact":"relations_v1","path":"$.relations[*]","format":"extraction_relation"},
            \\  "artifact":{"name":"relations_v1","kind":"asset","source":{"type":"field","value":"relations"},"content_type":"application/json"}
            \\}
            ,
        });

        try db.batch(.{
            .writes = &.{.{
                .key = "doc:a",
                .value =
                \\{"relations":{"entities":[{"id":"e0","label":"person","text":"Ada Lovelace"}]}}
                ,
            }},
            .sync_level = .enrichments,
        });
        try db.runUntilIdle();

        const cfg: db_mod.index_manager_mod.ResolverConfig = .{
            .name = "kg_background",
            .table = "entities",
            .source_artifact = "relations_v1",
            .resolution_artifact = "resolution_background_v1",
            .key_template = "{{ lower _entity.label }}/{{ slug _entity.text }}",
            .config_generation = 1,
        };
        _ = try db.enrichment.upsertResolverWithResultOptions(cfg, .{ .drain_backfill = false });
        // Leave generation-one decisions queued for promotion across DDL.
        try db.enrichment.resolution.runtime.?.catchUp();
        try std.testing.expectEqual(@as(usize, 0), sink.count());
        var replacement = cfg;
        replacement.config_generation = 2;
        for ([_]*std.atomic.Mutex{ &db.enrichment.resolution.runtime.?.catch_up_mutex, &db.enrichment.promotion.runtime.?.catch_up_mutex }) |mutex| {
            try std.testing.expect(mutex.tryLock());
            defer mutex.unlock();
            try std.testing.expectEqual(db_mod.index_manager_mod.IndexManager.ResolverUpsertResult.updated_no_backfill, try db.enrichment.upsertResolverWithResultOptions(cfg, .{ .drain_backfill = false }));
            try std.testing.expectError(error.WriterLocked, db.enrichment.upsertResolverWithResultOptions(replacement, .{ .drain_backfill = false }));
            try std.testing.expectError(error.WriterLocked, db.enrichment.removeResolverWithoutDrain(cfg.name));
            var saved = (try db.resolverConfigByNameAlloc(cfg.name)).?;
            defer saved.deinit(alloc);
            try std.testing.expectEqual(@as(u64, 1), saved.config_generation);
        }
        {
            var txn = try db.core.store.beginWriteTxn();
            errdefer txn.abort();
            try txn.put(db_mod.resolver_catalog_mod.reresolve_resume_key, "past-doc-a");
            try txn.put(db_mod.resolver_catalog_mod.reresolve_repair_resume_key, "past-doc-a");
            try txn.commit();
        }
        _ = try db.enrichment.upsertResolverWithResultOptions(replacement, .{ .drain_backfill = false });
        for ([_][]const u8{ db_mod.resolver_catalog_mod.reresolve_resume_key, db_mod.resolver_catalog_mod.reresolve_repair_resume_key }) |key| {
            const cursor = try db.core.store.get(alloc, key);
            defer alloc.free(cursor);
            try std.testing.expectEqualStrings("", cursor);
        }
        try db.enrichment.promotion.runtime.?.catchUp();
        try std.testing.expectEqual(@as(usize, 0), sink.count());
        try db.runUntilIdle();
        try std.testing.expectEqual(@as(usize, 1), sink.count());
        const resolution_key = try db_mod.internal_keys.resolutionArtifactKeyAlloc(alloc, "doc:a", cfg.resolution_artifact);
        defer alloc.free(resolution_key);
        const raw = try db.core.store.get(alloc, resolution_key);
        defer alloc.free(raw);
        try std.testing.expect(std.mem.indexOf(u8, raw, "\"config_generation\":2") != null);
        const FollowerOwner = struct {
            fn isLocal(_: *anyopaque) bool {
                return false;
            }
        };
        if (follower) db.enrichment.setPromotionOwner(.{ .ptr = undefined, .vtable = &.{ .is_local_owner = FollowerOwner.isLocal } });
        // Retirement is durable replay work; keep the catalog until it finishes.
        try std.testing.expectError(error.WriterLocked, db.enrichment.removeResolverWithoutDrain(cfg.name));
        try db.runUntilIdle();
        try std.testing.expect(try db.enrichment.removeResolverWithoutDrain(cfg.name));
        try std.testing.expectError(error.NotFound, db.core.store.get(alloc, resolution_key));
    }
}

test "db drains pending resolver backfill when retrying a no-op upsertResolver" {
    for ([_]bool{ false, true }) |already_enqueued| {
        const alloc = std.testing.allocator;

        var path_tmp = try db_mod.TestDirectory.init("db");
        defer path_tmp.cleanup();
        const path = path_tmp.path().ptr;
        defer db_mod.cleanupTempDir(path);

        var db = try DB.open(alloc, std.mem.span(path), .{ .start_resolver_workers = !already_enqueued });
        defer db.close();

        try db.addIndex(.{
            .name = "relations_graph",
            .kind = .graph,
            .config_json =
            \\{
            \\  "source":{"artifact":"relations_v1","path":"$.relations[*]","format":"extraction_relation"},
            \\  "artifact":{"name":"relations_v1","kind":"asset","source":{"type":"field","value":"relations"},"content_type":"application/json"}
            \\}
            ,
        });

        try db.batch(.{
            .writes = &.{.{
                .key = "doc:a",
                .value =
                \\{"relations":{"entities":[{"id":"e0","label":"person","text":"Ada Lovelace"}]}}
                ,
            }},
            .sync_level = .enrichments,
        });
        try db.runUntilIdle();

        const resolution_key = try db_mod.internal_keys.resolutionArtifactKeyAlloc(alloc, "doc:a", "resolution_retry_v1");
        defer alloc.free(resolution_key);
        try std.testing.expectError(error.NotFound, db.core.store.get(alloc, resolution_key));

        const cfg: db_mod.index_manager_mod.ResolverConfig = .{
            .name = "kg_retry",
            .table = "entities",
            .source_artifact = "relations_v1",
            .resolution_artifact = "resolution_retry_v1",
            .key_template = "{{ lower _entity.label }}/{{ slug _entity.text }}",
            .config_generation = 1,
        };

        {
            db_mod.lockApply(&db);
            defer db.core.unlockApply();
            try std.testing.expectEqual(db_mod.index_manager_mod.IndexManager.ResolverUpsertResult.inserted, try db.core.upsertResolver(cfg));
        }
        try std.testing.expectError(error.NotFound, db.core.store.get(alloc, resolution_key));
        try std.testing.expect(try db.enrichment.resolution.runtime.?.hasReresolveBacklog());

        if (already_enqueued) {
            // Model the worker clearing the dirty cursor after enqueueing the
            // replay record, before resolution has materialized its output.
            while (try db.enrichment.resolution.runtime.?.hasReresolveBacklog()) {
                var tick = try db.enrichment.resolution.runtime.?.runReresolveBacklogWindow();
                tick.deinit(db.runtime_alloc);
            }
            try std.testing.expectError(error.NotFound, db.core.store.get(alloc, resolution_key));
            try std.testing.expect(db.enrichment.resolution.runtime.?.target_sequence.load(.acquire) >
                db.enrichment.resolution.runtime.?.applied_sequence.load(.acquire));
        }

        // Retrying the same catalog config is a material no-op, but the durable
        // dirty cursor from the first attempt must still be drained.
        try db.upsertResolver(cfg);

        const raw = try db.core.store.get(alloc, resolution_key);
        defer alloc.free(raw);
        try std.testing.expect(std.mem.indexOf(u8, raw, "\"config_generation\":1") != null);
        try std.testing.expect(std.mem.indexOf(u8, raw, "person/ada_lovelace") != null);
        try std.testing.expect(!try db.enrichment.resolution.runtime.?.hasReresolveBacklog());
    }
}

test "db refuses resolver removal while resolution or promotion replay is pending" {
    const alloc = std.testing.allocator;

    var path_tmp = try db_mod.TestDirectory.init("db");
    defer path_tmp.cleanup();
    const path = path_tmp.path().ptr;
    defer db_mod.cleanupTempDir(path);

    var db = try DB.open(alloc, std.mem.span(path), .{});
    defer db.close();

    try db.addIndex(.{
        .name = "relations_graph",
        .kind = .graph,
        .config_json =
        \\{
        \\  "source":{"artifact":"relations_v1","path":"$.relations[*]","format":"extraction_relation","mention_edge_type":"mentions"},
        \\  "artifact":{"name":"relations_v1","kind":"asset","source":{"type":"field","value":"relations"},"content_type":"application/json"}
        \\}
        ,
    });
    try db.addResolver(.{
        .name = "kg_pending_remove",
        .table = "entities",
        .source_artifact = "relations_v1",
        .resolution_artifact = "resolution_pending_remove_v1",
        .key_template = "{{ lower _entity.label }}/{{ slug _entity.text }}",
        .config_generation = 1,
    });

    try db.batch(.{
        .writes = &.{.{
            .key = "doc:a",
            .value =
            \\{"relations":{"entities":[{"id":"e0","label":"person","text":"Ada Lovelace"}]}}
            ,
        }},
        .sync_level = .enrichments,
    });

    // The default standalone DB has no entity sink and waits rather than
    // advancing promotion replay. Removing the resolver here would otherwise
    // erase the catalog signal that older retention logic used.
    try std.testing.expectError(error.ResolverReplayPending, db.removeResolver("kg_pending_remove"));
    try std.testing.expect(db.enrichment.promotionStageStats().catch_up_required);
}

test "db graph index materializes unit-derived chunk artifacts into graph edge artifacts" {
    const alloc = std.testing.allocator;

    var path_tmp = try db_mod.TestDirectory.init("db");
    defer path_tmp.cleanup();
    const path = path_tmp.path().ptr;
    defer db_mod.cleanupTempDir(path);

    var db = try DB.open(alloc, std.mem.span(path), .{});
    defer db.close();

    try db.enrichment.addEnrichment(.{
        .name = "document_units_v1",
        .kind = .asset,
        .field = "url",
        .content_type = "application/json",
        .producer_json = "{\"type\":\"document_extraction\",\"config\":{}}",
    });
    try db.enrichment.addEnrichment(.{
        .name = "document_chunks_v1",
        .kind = .chunk,
        .field = "text",
        .source_artifact_name = "document_units_v1",
        .chunk_size = 64,
    });
    try db.addIndex(.{
        .name = "chunk_graph",
        .kind = .graph,
        .config_json =
        \\{
        \\  "source":{"artifact":"document_chunks_v1","format":"extraction_relation",
        \\    "nodes":{"target":"{{ _artifact.value.text }}"},
        \\    "edge":{"type":"mentions","metadata":{"unit":"{{ _artifact.value._parent_unit_id }}","artifact":"{{ _artifact.name }}"}}}
        \\}
        ,
    });

    try db.batch(.{
        .writes = &.{.{
            .key = "doc:a",
            .value = "{\"url\":\"data:text/plain;base64,ZG9jOmI=\"}",
        }},
        .sync_level = .enrichments,
    });
    try db.runUntilIdle();

    const unit_key = try db_mod.internal_keys.documentUnitArtifactKeyAlloc(alloc, "doc:a", "document_units_v1", "document:000001");
    defer alloc.free(unit_key);
    const edges = try db.getEdges(alloc, "chunk_graph", "doc:a", "mentions", .out);
    defer db_mod.graph_mod.GraphIndex.freeEdges(alloc, edges);
    try std.testing.expectEqual(@as(usize, 1), edges.len);
    try std.testing.expectEqualStrings("doc:b", edges[0].target);
    try std.testing.expect(std.mem.indexOf(u8, edges[0].metadata, "\"unit\":\"document:000001\"") != null);
    try std.testing.expect(std.mem.indexOf(u8, edges[0].metadata, "\"artifact\":\"document_chunks_v1\"") != null);
}

test "db direct generated chunks feed multi-source text and graph indexes" {
    const alloc = std.testing.allocator;

    var path_tmp = try db_mod.TestDirectory.init("db");
    defer path_tmp.cleanup();
    const path = path_tmp.path().ptr;
    defer db_mod.cleanupTempDir(path);

    var db = try DB.open(alloc, std.mem.span(path), .{
        .enrichment = .{
            .owner_id = "worker-a",
            .enable_without_producers = true,
        },
    });
    defer db.close();

    const ephemeral_chunker =
        "{\"provider\":\"antfly\",\"store_chunks\":false,\"text\":{\"target_tokens\":32,\"overlap_tokens\":0}}";
    try db.enrichment.addEnrichment(.{
        .name = "search_chunks_v1",
        .kind = .chunk,
        .field = "body",
        .chunker_json = ephemeral_chunker,
    });
    try db.enrichment.addEnrichment(.{
        .name = "graph_chunks_v1",
        .kind = .chunk,
        .field = "relation",
        .chunker_json = ephemeral_chunker,
    });
    try db.addIndex(.{
        .name = "selected_text",
        .kind = .full_text,
        .config_json = "{\"sources\":[{\"artifact\":\"search_chunks_v1\"}]}",
    });
    try db.addIndex(.{
        .name = "selected_graph",
        .kind = .graph,
        .config_json =
        \\{"sources":[{"artifact":"graph_chunks_v1","format":"extraction_relation","nodes":{"target":"{{ _artifact.value.relation }}"},"edge":{"type":"mentions"}}]}
        ,
    });

    try db.batch(.{
        .writes = &.{.{
            .key = "doc:a",
            .value = "{\"body\":\"alpha searchable text\",\"relation\":\"doc:b\"}",
        }},
        .sync_level = .enrichments,
    });
    try db.runUntilIdle();

    var text_result = try db.search(alloc, .{
        .index_name = "selected_text",
        .full_text = .{ .match = .{ .field = "body", .text = "alpha" } },
    });
    defer text_result.deinit();
    try std.testing.expectEqual(@as(u32, 1), text_result.total_hits);

    const graph_chunk_key = try db_mod.internal_keys.chunkArtifactKeyAlloc(alloc, "doc:a", "graph_chunks_v1", 0);
    defer alloc.free(graph_chunk_key);
    const graph_chunk = try db.core.store.get(alloc, graph_chunk_key);
    defer alloc.free(graph_chunk);

    const edges = try db.getEdges(alloc, "selected_graph", "doc:a", "mentions", .out);
    defer db_mod.graph_mod.GraphIndex.freeEdges(alloc, edges);
    try std.testing.expectEqual(@as(usize, 1), edges.len);
    try std.testing.expectEqualStrings("doc:b", edges[0].target);

    try db.batch(.{
        .writes = &.{.{
            .key = "doc:a",
            .value = "{\"body\":\"alpha searchable text\",\"relation\":\"\"}",
        }},
        .sync_level = .enrichments,
    });
    try db.runUntilIdle();
    try std.testing.expectError(error.NotFound, db.core.store.get(alloc, graph_chunk_key));
    const after_delete = try db.getEdges(alloc, "selected_graph", "doc:a", "mentions", .out);
    defer db_mod.graph_mod.GraphIndex.freeEdges(alloc, after_delete);
    try std.testing.expectEqual(@as(usize, 0), after_delete.len);
}

test "db member-mode chunk hits filter through the parent row on the full-text arm" {
    const alloc = std.testing.allocator;

    var path_tmp = try db_mod.TestDirectory.init("db");
    defer path_tmp.cleanup();
    const path = path_tmp.path().ptr;
    defer db_mod.cleanupTempDir(path);

    var db = try DB.open(alloc, std.mem.span(path), .{
        .enrichment = .{
            .owner_id = "worker-a",
            .enable_without_producers = true,
        },
    });
    defer db.close();

    const ephemeral_chunker =
        "{\"provider\":\"antfly\",\"store_chunks\":false,\"text\":{\"target_tokens\":32,\"overlap_tokens\":0}}";
    try db.enrichment.addEnrichment(.{
        .name = "body_chunks_v1",
        .kind = .chunk,
        .field = "body",
        .chunker_json = ephemeral_chunker,
    });
    try db.addIndex(.{
        .name = "selected_text",
        .kind = .full_text,
        .config_json = "{\"sources\":[{\"artifact\":\"body_chunks_v1\"}]}",
    });

    // Chunk members are a separate full-text document from their parent row
    // and never carry the parent's own fields (issue #931). `category` only
    // exists on the parent row, never on a chunk's own stored payload.
    try db.batch(.{
        .writes = &.{
            .{ .key = "doc:keep", .value = "{\"body\":\"alpha searchable text\",\"category\":\"keep\"}" },
            .{ .key = "doc:drop", .value = "{\"body\":\"alpha also appears here\",\"category\":\"drop\"}" },
        },
        .sync_level = .enrichments,
    });
    try db.runUntilIdle();

    var unfiltered = try db.search(alloc, .{
        .index_name = "selected_text",
        .full_text = .{ .match = .{ .field = "body", .text = "alpha" } },
        .return_mode = .member,
    });
    defer unfiltered.deinit();
    try std.testing.expectEqual(@as(u32, 2), unfiltered.total_hits);

    var included = try db.search(alloc, .{
        .index_name = "selected_text",
        .full_text = .{ .match = .{ .field = "body", .text = "alpha" } },
        .filter_query_json = "{\"term\":{\"category\":\"keep\"}}",
        .return_mode = .member,
    });
    defer included.deinit();
    try std.testing.expectEqual(@as(u32, 1), included.total_hits);
    try std.testing.expectEqual(@as(usize, 1), included.hits.len);
    try std.testing.expectEqualStrings("doc:keep", included.hits[0].artifact_ref.?.document_id);

    var excluded = try db.search(alloc, .{
        .index_name = "selected_text",
        .full_text = .{ .match = .{ .field = "body", .text = "alpha" } },
        .exclusion_query_json = "{\"term\":{\"category\":\"keep\"}}",
        .return_mode = .member,
    });
    defer excluded.deinit();
    try std.testing.expectEqual(@as(u32, 1), excluded.total_hits);
    try std.testing.expectEqual(@as(usize, 1), excluded.hits.len);
}

test "db member-mode chunk hits apply query.bool.filter/must_not against the parent row" {
    const alloc = std.testing.allocator;

    var path_tmp = try db_mod.TestDirectory.init("db");
    defer path_tmp.cleanup();
    const path = path_tmp.path().ptr;
    defer db_mod.cleanupTempDir(path);

    var db = try DB.open(alloc, std.mem.span(path), .{
        .enrichment = .{
            .owner_id = "worker-a",
            .enable_without_producers = true,
        },
    });
    defer db.close();

    const ephemeral_chunker =
        "{\"provider\":\"antfly\",\"store_chunks\":false,\"text\":{\"target_tokens\":32,\"overlap_tokens\":0}}";
    try db.enrichment.addEnrichment(.{
        .name = "body_chunks_v1",
        .kind = .chunk,
        .field = "body",
        .chunker_json = ephemeral_chunker,
    });
    try db.addIndex(.{
        .name = "selected_text",
        .kind = .full_text,
        .config_json = "{\"sources\":[{\"artifact\":\"body_chunks_v1\"}]}",
    });

    // `filter_text`/`exclusion_text` are the native TextQuery form
    // query.bool.filter/must_not lowers to at the API layer. `category`
    // only exists on the parent row, never on a chunk's own stored payload.
    try db.batch(.{
        .writes = &.{
            .{ .key = "doc:keep", .value = "{\"body\":\"alpha searchable text\",\"category\":\"keep\"}" },
            .{ .key = "doc:drop", .value = "{\"body\":\"alpha also appears here\",\"category\":\"drop\"}" },
        },
        .sync_level = .enrichments,
    });
    try db.runUntilIdle();

    var included = try db.search(alloc, .{
        .index_name = "selected_text",
        .full_text = .{ .match = .{ .field = "body", .text = "alpha" } },
        .filter_text = .{ .term = .{ .field = "category", .term = "keep" } },
        .return_mode = .member,
    });
    defer included.deinit();
    try std.testing.expectEqual(@as(u32, 1), included.total_hits);
    try std.testing.expectEqual(@as(usize, 1), included.hits.len);
    try std.testing.expectEqualStrings("doc:keep", included.hits[0].artifact_ref.?.document_id);

    var excluded = try db.search(alloc, .{
        .index_name = "selected_text",
        .full_text = .{ .match = .{ .field = "body", .text = "alpha" } },
        .exclusion_text = .{ .term = .{ .field = "category", .term = "keep" } },
        .return_mode = .member,
    });
    defer excluded.deinit();
    try std.testing.expectEqual(@as(u32, 1), excluded.total_hits);
    try std.testing.expectEqual(@as(usize, 1), excluded.hits.len);
    try std.testing.expectEqualStrings("doc:drop", excluded.hits[0].artifact_ref.?.document_id);

    // An analyzed/tokenized clause (`match`) has no stored-pattern
    // equivalent; chunk-backed member mode must reject it rather than
    // silently resolve it against the chunk's own (wrong) doc-number space.
    try std.testing.expectError(error.UnsupportedQueryRequest, db.search(alloc, .{
        .index_name = "selected_text",
        .full_text = .{ .match = .{ .field = "body", .text = "alpha" } },
        .filter_text = .{ .match = .{ .field = "category", .text = "keep" } },
        .return_mode = .member,
    }));
}

// PR #957 review blocker 6: a doc_id clause inside query.bool.filter/
// must_not (native TextQuery filter_text/exclusion_text) lowers through
// liftChunkBoolFilterClausesAlloc / lowerTextQueryToStoredPatternValueAlloc
// into a stored-pattern "doc_id" predicate. applyStoredSearchPatternFilters
// used to match that predicate against the chunk hit's own id instead of its
// resolved parent id, so a doc_id filter silently matched nothing and a
// doc_id exclusion silently excluded nothing.
test "db member-mode chunk hits apply query.bool.filter/must_not doc_id against the resolved parent id on the full-text arm (PR #957)" {
    const alloc = std.testing.allocator;

    var path_tmp = try db_mod.TestDirectory.init("db");
    defer path_tmp.cleanup();
    const path = path_tmp.path().ptr;
    defer db_mod.cleanupTempDir(path);

    var db = try DB.open(alloc, std.mem.span(path), .{
        .enrichment = .{
            .owner_id = "worker-a",
            .enable_without_producers = true,
        },
    });
    defer db.close();

    const ephemeral_chunker =
        "{\"provider\":\"antfly\",\"store_chunks\":false,\"text\":{\"target_tokens\":32,\"overlap_tokens\":0}}";
    try db.enrichment.addEnrichment(.{
        .name = "body_chunks_v1",
        .kind = .chunk,
        .field = "body",
        .chunker_json = ephemeral_chunker,
    });
    try db.addIndex(.{
        .name = "selected_text",
        .kind = .full_text,
        .config_json = "{\"sources\":[{\"artifact\":\"body_chunks_v1\"}]}",
    });

    try db.batch(.{
        .writes = &.{
            .{ .key = "doc:keep", .value = "{\"body\":\"alpha searchable text\",\"category\":\"keep\"}" },
            .{ .key = "doc:drop", .value = "{\"body\":\"alpha also appears here\",\"category\":\"drop\"}" },
        },
        .sync_level = .enrichments,
    });
    try db.runUntilIdle();

    var included = try db.search(alloc, .{
        .index_name = "selected_text",
        .full_text = .{ .match = .{ .field = "body", .text = "alpha" } },
        .filter_text = .{ .doc_id = .{ .ids = &.{"doc:keep"} } },
        .return_mode = .member,
    });
    defer included.deinit();
    try std.testing.expectEqual(@as(u32, 1), included.total_hits);
    try std.testing.expectEqual(@as(usize, 1), included.hits.len);
    try std.testing.expectEqualStrings("doc:keep", included.hits[0].artifact_ref.?.document_id);

    var excluded = try db.search(alloc, .{
        .index_name = "selected_text",
        .full_text = .{ .match = .{ .field = "body", .text = "alpha" } },
        .exclusion_text = .{ .doc_id = .{ .ids = &.{"doc:keep"} } },
        .return_mode = .member,
    });
    defer excluded.deinit();
    try std.testing.expectEqual(@as(u32, 1), excluded.total_hits);
    try std.testing.expectEqual(@as(usize, 1), excluded.hits.len);
    try std.testing.expectEqualStrings("doc:drop", excluded.hits[0].artifact_ref.?.document_id);
}

test "db asset consumes another asset artifact and re-derives on upstream change" {
    const alloc = std.testing.allocator;

    var path_tmp = try db_mod.TestDirectory.init("db");
    defer path_tmp.cleanup();
    const path = path_tmp.path().ptr;
    defer db_mod.cleanupTempDir(path);

    var db = try DB.open(alloc, std.mem.span(path), .{
        .enrichment = .{
            .owner_id = "worker-a",
            .enable_without_producers = true,
        },
    });
    defer db.close();

    try db.enrichment.addEnrichment(.{
        .name = "summary_text_v1",
        .kind = .asset,
        .field = "summary",
        .content_type = "text/plain",
        .producer_json = "{\"type\":\"copy\"}",
    });
    try db.enrichment.addEnrichment(.{
        .name = "summary_echo_v1",
        .kind = .asset,
        .source_artifact_name = "summary_text_v1",
        .content_type = "text/plain",
        .producer_json = "{\"type\":\"copy\"}",
    });
    try db.addIndex(.{
        .name = "echo_text",
        .kind = .full_text,
        .config_json = "{\"sources\":[{\"artifact\":\"summary_echo_v1\"}]}",
    });

    try db.batch(.{
        .writes = &.{.{
            .key = "doc:a",
            .value = "{\"summary\":\"firstupstreamtoken concise summary\"}",
        }},
        .sync_level = .enrichments,
    });
    try db.runUntilIdle();

    const echo_key = try db_mod.internal_keys.artifactNamedPrefixAlloc(alloc, "doc:a", "asset", "summary_echo_v1");
    defer alloc.free(echo_key);
    const first = try db.core.store.get(alloc, echo_key);
    defer alloc.free(first);
    try std.testing.expectEqualStrings("firstupstreamtoken concise summary", first);

    try db.batch(.{
        .writes = &.{.{
            .key = "doc:a",
            .value = "{\"summary\":\"secondupstreamtoken concise summary\"}",
        }},
        .sync_level = .enrichments,
    });
    try db.runUntilIdle();
    const second = try db.core.store.get(alloc, echo_key);
    defer alloc.free(second);
    try std.testing.expectEqualStrings("secondupstreamtoken concise summary", second);

    // Removing the upstream source retires the producer and, through the
    // same batch's delete overlay, the consumer.
    try db.batch(.{
        .writes = &.{.{
            .key = "doc:a",
            .value = "{}",
        }},
        .sync_level = .enrichments,
    });
    try db.runUntilIdle();
    try std.testing.expectError(error.NotFound, db.core.store.get(alloc, echo_key));
}

test "db asset consuming asset validates upstream references at admission" {
    const alloc = std.testing.allocator;

    var path_tmp = try db_mod.TestDirectory.init("db");
    defer path_tmp.cleanup();
    const path = path_tmp.path().ptr;
    defer db_mod.cleanupTempDir(path);

    var db = try DB.open(alloc, std.mem.span(path), .{
        .enrichment = .{
            .owner_id = "worker-a",
            .enable_without_producers = true,
        },
    });
    defer db.close();

    try db.enrichment.addEnrichment(.{
        .name = "a_v1",
        .kind = .asset,
        .field = "summary",
        .producer_json = "{\"type\":\"copy\"}",
    });
    // Self-reference is the one-hop cycle.
    try std.testing.expectError(error.InvalidEnrichmentConfig, db.enrichment.addEnrichment(.{
        .name = "self_v1",
        .kind = .asset,
        .source_artifact_name = "self_v1",
        .producer_json = "{\"type\":\"copy\"}",
    }));
    // The upstream must already be an admitted asset.
    try std.testing.expectError(error.InvalidEnrichmentConfig, db.enrichment.addEnrichment(.{
        .name = "b_v1",
        .kind = .asset,
        .source_artifact_name = "missing_v1",
        .producer_json = "{\"type\":\"copy\"}",
    }));
    // Consuming assets read produced bytes; a document field cannot also be
    // the source.
    try std.testing.expectError(error.InvalidEnrichmentConfig, db.enrichment.addEnrichment(.{
        .name = "c_v1",
        .kind = .asset,
        .field = "summary",
        .source_artifact_name = "a_v1",
        .producer_json = "{\"type\":\"copy\"}",
    }));
    // Media-locator producers dereference the source as a URL and stay closed.
    try std.testing.expectError(error.InvalidEnrichmentConfig, db.enrichment.addEnrichment(.{
        .name = "r_v1",
        .kind = .asset,
        .source_artifact_name = "a_v1",
        .producer_json = "{\"type\":\"reader\"}",
    }));
    // A valid chain admits.
    try db.enrichment.addEnrichment(.{
        .name = "d_v1",
        .kind = .asset,
        .source_artifact_name = "a_v1",
        .producer_json = "{\"type\":\"copy\"}",
    });
}

test "db multi-source full text unions chunk and textual asset streams across deletion and reopen" {
    const alloc = std.testing.allocator;

    var path_tmp = try db_mod.TestDirectory.init("db");
    defer path_tmp.cleanup();
    const path = path_tmp.path().ptr;
    defer db_mod.cleanupTempDir(path);

    {
        var db = try DB.open(alloc, std.mem.span(path), .{
            .enrichment = .{
                .owner_id = "worker-a",
                .enable_without_producers = true,
            },
        });
        defer db.close();

        try db.enrichment.addEnrichment(.{
            .name = "body_chunks_v1",
            .kind = .chunk,
            .field = "body",
            .chunker_json =
            \\{"provider":"antfly","store_chunks":false,"text":{"target_tokens":32,"overlap_tokens":0}}
            ,
        });
        try db.enrichment.addEnrichment(.{
            .name = "summary_text_v1",
            .kind = .asset,
            .field = "summary",
            .content_type = "text/plain",
            .producer_json = "{\"type\":\"copy\"}",
        });
        try db.addIndex(.{
            .name = "document_text",
            .kind = .full_text,
            .config_json = "{\"sources\":[{\"artifact\":\"body_chunks_v1\"},{\"artifact\":\"summary_text_v1\"}]}",
        });

        try db.batch(.{
            .writes = &.{.{
                .key = "doc:a",
                .value = "{\"body\":\"chunkonlytoken searchable body\",\"summary\":\"assetonlytoken concise summary\"}",
            }},
            .sync_level = .enrichments,
        });
        try db.runUntilIdle();

        var chunk_match = try db_mod.waitForSearchResult(alloc, &db, .{
            .index_name = "document_text",
            .full_text = .{ .match = .{ .field = "body", .text = "chunkonlytoken" } },
            .return_mode = .member,
        }, 1);
        defer chunk_match.deinit();
        try std.testing.expectEqual(@as(usize, 1), chunk_match.hits.len);
        const chunk_match_ref = chunk_match.hits[0].artifact_ref orelse return error.TestUnexpectedResult;
        try std.testing.expectEqualStrings("body_chunks_v1", chunk_match_ref.name);

        var asset_match = try db_mod.waitForSearchResult(alloc, &db, .{
            .index_name = "document_text",
            .full_text = .{ .match = .{ .field = "summary", .text = "assetonlytoken" } },
            .return_mode = .member,
        }, 1);
        defer asset_match.deinit();
        try std.testing.expectEqual(@as(usize, 1), asset_match.hits.len);
        const asset_match_ref = asset_match.hits[0].artifact_ref orelse return error.TestUnexpectedResult;
        try std.testing.expectEqualStrings("summary_text_v1", asset_match_ref.name);

        var members = try db_mod.waitForSearchResult(alloc, &db, .{
            .index_name = "document_text",
            .full_text = .{ .match_all = {} },
            .limit = 10,
            .return_mode = .member,
        }, 2);
        defer members.deinit();
        try std.testing.expectEqual(@as(usize, 2), members.hits.len);
        var saw_chunk = false;
        var saw_asset = false;
        for (members.hits) |hit| {
            const artifact_ref = hit.artifact_ref orelse return error.TestUnexpectedResult;
            try std.testing.expectEqualStrings("doc:a", artifact_ref.document_id);
            if (std.mem.eql(u8, artifact_ref.name, "body_chunks_v1")) {
                try std.testing.expectEqual(db_mod.types.ArtifactKind.chunk, artifact_ref.kind);
                saw_chunk = true;
            } else if (std.mem.eql(u8, artifact_ref.name, "summary_text_v1")) {
                try std.testing.expectEqual(db_mod.types.ArtifactKind.asset, artifact_ref.kind);
                saw_asset = true;
            }
        }
        try std.testing.expect(saw_chunk and saw_asset);

        var parents = try db_mod.waitForSearchResult(alloc, &db, .{
            .index_name = "document_text",
            .full_text = .{ .match_all = {} },
            .limit = 10,
        }, 1);
        defer parents.deinit();
        try std.testing.expectEqual(@as(usize, 1), parents.hits.len);
        try std.testing.expectEqualStrings("doc:a", parents.hits[0].id);

        // Prefix filtering is defined over the source document identity, not
        // the encoded artifact member key. Exercise both the direct text lane
        // and the named/composed lane while an out-of-prefix artifact has the
        // same searchable content.
        try db.batch(.{
            .writes = &.{.{
                .key = "doc:b",
                .value = "{\"body\":\"chunkonlytoken searchable body\",\"summary\":\"assetonlytoken concise summary\"}",
            }},
            .sync_level = .enrichments,
        });
        try db.runUntilIdle();

        var prefixed_text = try db_mod.waitForSearchResult(alloc, &db, .{
            .index_name = "document_text",
            .full_text = .{ .match = .{ .field = "body", .text = "chunkonlytoken" } },
            .filter_prefix = "doc:a",
            .limit = 10,
            .return_mode = .member,
        }, 1);
        defer prefixed_text.deinit();
        try std.testing.expectEqual(@as(u32, 1), prefixed_text.total_hits);
        try std.testing.expectEqualStrings("doc:a", prefixed_text.hits[0].artifact_ref.?.document_id);

        var prefixed_composed_text = try db_mod.waitForSearchResult(alloc, &db, .{
            .full_text_queries = &.{.{
                .name = "artifact_body",
                .index_name = "document_text",
                .query = .{ .match = .{ .field = "body", .text = "chunkonlytoken" } },
            }},
            .filter_prefix = "doc:a",
            .limit = 10,
        }, 1);
        defer prefixed_composed_text.deinit();
        try std.testing.expectEqual(@as(u32, 1), prefixed_composed_text.total_hits);
        try std.testing.expectEqualStrings("doc:a", prefixed_composed_text.hits[0].artifact_ref.?.document_id);

        try db.batch(.{ .deletes = &.{"doc:b"}, .sync_level = .enrichments });
        try db.runUntilIdle();

        // Removing one producer input must delete only that source member;
        // the other source remains searchable and retains its provenance.
        try db.batch(.{
            .writes = &.{.{
                .key = "doc:a",
                .value = "{\"body\":\"chunkonlytoken searchable body\"}",
            }},
            .sync_level = .enrichments,
        });
        try db.runUntilIdle();

        const removed_asset_key = try db_mod.internal_keys.artifactNamedPrefixAlloc(alloc, "doc:a", "asset", "summary_text_v1");
        defer alloc.free(removed_asset_key);
        try std.testing.expectError(error.NotFound, db.core.store.get(alloc, removed_asset_key));

        var removed_match = try db.search(alloc, .{
            .index_name = "document_text",
            .full_text = .{ .match = .{ .field = "summary", .text = "assetonlytoken" } },
        });
        defer removed_match.deinit();
        try std.testing.expectEqual(@as(u32, 0), removed_match.total_hits);

        var after_delete = try db_mod.waitForSearchResult(alloc, &db, .{
            .index_name = "document_text",
            .full_text = .{ .match_all = {} },
            .limit = 10,
            .return_mode = .member,
        }, 1);
        defer after_delete.deinit();
        try std.testing.expectEqual(@as(usize, 1), after_delete.hits.len);
        const remaining_ref = after_delete.hits[0].artifact_ref orelse return error.TestUnexpectedResult;
        try std.testing.expectEqualStrings("body_chunks_v1", remaining_ref.name);
        try db.sync(true);
    }

    var reopened = try DB.open(alloc, std.mem.span(path), .{});
    defer reopened.close();
    var recovered = try db_mod.waitForSearchResult(alloc, &reopened, .{
        .index_name = "document_text",
        .full_text = .{ .match_all = {} },
        .limit = 10,
        .return_mode = .member,
    }, 1);
    defer recovered.deinit();
    try std.testing.expectEqual(@as(usize, 1), recovered.hits.len);
    const recovered_ref = recovered.hits[0].artifact_ref orelse return error.TestUnexpectedResult;
    try std.testing.expectEqualStrings("body_chunks_v1", recovered_ref.name);
}

test "db mention edge weight is fused from extractor trust and mention confidence" {
    const alloc = std.testing.allocator;

    var path_tmp = try db_mod.TestDirectory.init("db");
    defer path_tmp.cleanup();
    const path = path_tmp.path().ptr;
    defer db_mod.cleanupTempDir(path);

    var db = try DB.open(alloc, std.mem.span(path), .{
        .start_index_workers = false,
        .start_optional_runtime_workers = false,
    });
    defer db.close();
    var promotion_sink = db_mod.FakePromotionSink{ .alloc = alloc };
    defer promotion_sink.deinit();
    db.enrichment.setEntitySink(promotion_sink.sink());

    try db.addIndex(.{
        .name = "prov_graph",
        .kind = .graph,
        .config_json =
        \\{
        \\  "source":{"artifact":"relations_v1","mention_edge_type":"mentions",
        \\    "format":"extraction_relation","path":"$.relations[*]"},
        \\  "artifact":{"name":"relations_v1","kind":"asset","source":{"type":"field","value":"relations"},"content_type":"application/json"}
        \\}
        ,
    });
    // A resolver declaring noisy_or fusion: this extractor is trusted 0.9, no
    // graph prior. The mention asserts confidence 0.8, so the edge weight is
    // fuse(noisy_or, [{0.8, 0.9}], prior=0, prior_weight=0) = 1-(1-0.72) = 0.72.
    try db.addResolver(.{
        .name = "kg",
        .table = "entities",
        .source_artifact = "relations_v1",
        .resolution_artifact = "resolution_v1",
        .key_template = "{{ lower _entity.label }}/{{ slug _entity.text }}",
        .fusion_combine = "noisy_or",
        .fusion_trust = 0.9,
        .config_generation = 1,
    });

    try db.batch(.{
        .writes = &.{.{
            .key = "doc:a",
            .value =
            \\{"relations":{"entities":[{"id":"e0","label":"person","text":"Ada Lovelace","confidence":0.8}]}}
            ,
        }},
        .sync_level = .enrichments,
    });
    try db.runUntilIdle();

    const resolution_key = try db_mod.internal_keys.resolutionArtifactKeyAlloc(alloc, "doc:a", "resolution_v1");
    defer alloc.free(resolution_key);
    const initial_resolution = try db.core.store.get(alloc, resolution_key);
    defer alloc.free(initial_resolution);

    const out = try db.getEdges(alloc, "prov_graph", "doc:a", "mentions", .out);
    defer db_mod.graph_mod.GraphIndex.freeEdges(alloc, out);
    try std.testing.expectEqual(@as(usize, 1), out.len);
    try std.testing.expectEqualStrings("person/ada_lovelace", out[0].target);
    // Calibrated weight, not the legacy 1.0.
    try std.testing.expectApproxEqAbs(@as(f64, 0.72), out[0].weight, 1e-9);
    try std.testing.expectEqual(@as(usize, 1), promotion_sink.count());

    // Mention confidence belongs to the source extraction, not the resolution
    // decision. The resolution bytes therefore stay unchanged, but replay must
    // still reconcile the source-dependent graph weight.
    try db.batch(.{
        .writes = &.{.{
            .key = "doc:a",
            .value =
            \\{"relations":{"entities":[{"id":"e0","label":"person","text":"Ada Lovelace","confidence":0.4}]}}
            ,
        }},
        .sync_level = .enrichments,
    });
    try db.runUntilIdle();

    const updated_resolution = try db.core.store.get(alloc, resolution_key);
    defer alloc.free(updated_resolution);
    try std.testing.expectEqualStrings(initial_resolution, updated_resolution);

    const updated = try db.getEdges(alloc, "prov_graph", "doc:a", "mentions", .out);
    defer db_mod.graph_mod.GraphIndex.freeEdges(alloc, updated);
    try std.testing.expectEqual(@as(usize, 1), updated.len);
    try std.testing.expectApproxEqAbs(@as(f64, 0.36), updated[0].weight, 1e-9);
    try std.testing.expectEqual(@as(usize, 1), promotion_sink.count());
    try std.testing.expect(!db.pendingWorkStats().promotion.blocked);
}

test "db multi-source graph precedence falls back after winner deletion and reopen" {
    const alloc = std.testing.allocator;

    var path_tmp = try db_mod.TestDirectory.init("db");
    defer path_tmp.cleanup();
    const path = path_tmp.path().ptr;
    defer db_mod.cleanupTempDir(path);

    {
        var db = try DB.open(alloc, std.mem.span(path), .{});
        defer db.close();

        try db.enrichment.addEnrichment(.{
            .name = "primary_relations_v1",
            .kind = .asset,
            .field = "primary_relations",
            .content_type = "application/json",
        });
        try db.enrichment.addEnrichment(.{
            .name = "fallback_relations_v1",
            .kind = .asset,
            .field = "fallback_relations",
            .content_type = "application/json",
        });
        try db.addIndex(.{
            .name = "relations_graph",
            .kind = .graph,
            .config_json =
            \\{"sources":[{"artifact":"primary_relations_v1"},{"artifact":"fallback_relations_v1"}]}
            ,
        });

        try db.batch(.{
            .writes = &.{.{
                .key = "doc:a",
                .value =
                \\{"primary_relations":{"type":"mentions","target":{"document_id":"doc:b"},"weight":2,"winner":"primary"},"fallback_relations":{"type":"mentions","target":{"document_id":"doc:b"},"weight":1,"winner":"fallback"}}
                ,
            }},
            .sync_level = .enrichments,
        });
        try db.runUntilIdle();

        {
            const edges = try db.getEdges(alloc, "relations_graph", "doc:a", "mentions", .out);
            defer db_mod.graph_mod.GraphIndex.freeEdges(alloc, edges);
            try std.testing.expectEqual(@as(usize, 1), edges.len);
            try std.testing.expectEqual(@as(f64, 2), edges[0].weight);
            try std.testing.expect(std.mem.indexOf(u8, edges[0].metadata, "\"winner\":\"primary\"") != null);
        }
        const generation = (db.core.index_manager.graphIndex("relations_graph") orelse return error.TestUnexpectedResult).config.coverage_generation;
        const contender_count_key = try db_mod.internal_keys.graphEdgeContenderCountKeyAlloc(alloc, "doc:a", "relations_graph");
        defer alloc.free(contender_count_key);
        {
            const raw_count = try db.core.store.get(alloc, contender_count_key);
            defer alloc.free(raw_count);
            try std.testing.expectEqual(@as(?usize, 1), try db_mod.graph_edge_contender.decodeVisibleCount(raw_count, generation));
        }
        const logical_edge_key = try db_mod.internal_keys.graphEdgeArtifactKeyAlloc(alloc, "doc:a", "relations_graph", "mentions", "doc:b");
        defer alloc.free(logical_edge_key);
        const contender_prefix = try db_mod.internal_keys.graphEdgeContenderEdgePrefixAlloc(alloc, "doc:a", "relations_graph", logical_edge_key);
        defer alloc.free(contender_prefix);
        {
            const contenders = try db.core.store.scanPrefix(alloc, contender_prefix);
            defer db_mod.docstore_mod.DocStore.freeResults(alloc, contenders);
            try std.testing.expectEqual(@as(usize, 2), contenders.len);
        }

        // Removing the higher-precedence artifact must reveal the retained
        // lower-precedence payload instead of deleting the logical edge.
        try db.batch(.{
            .writes = &.{.{
                .key = "doc:a",
                .value =
                \\{"fallback_relations":{"type":"mentions","target":{"document_id":"doc:b"},"weight":1,"winner":"fallback"}}
                ,
            }},
            .sync_level = .enrichments,
        });
        try db.runUntilIdle();

        const edges = try db.getEdges(alloc, "relations_graph", "doc:a", "mentions", .out);
        defer db_mod.graph_mod.GraphIndex.freeEdges(alloc, edges);
        try std.testing.expectEqual(@as(usize, 1), edges.len);
        try std.testing.expectEqual(@as(f64, 1), edges[0].weight);
        try std.testing.expect(std.mem.indexOf(u8, edges[0].metadata, "\"winner\":\"fallback\"") != null);
        {
            const contenders = try db.core.store.scanPrefix(alloc, contender_prefix);
            defer db_mod.docstore_mod.DocStore.freeResults(alloc, contenders);
            try std.testing.expectEqual(@as(usize, 1), contenders.len);
        }
        try db.sync(true);
    }

    var reopened = try DB.open(alloc, std.mem.span(path), .{});
    defer reopened.close();
    const reopened_edges = try reopened.getEdges(alloc, "relations_graph", "doc:a", "mentions", .out);
    defer db_mod.graph_mod.GraphIndex.freeEdges(alloc, reopened_edges);
    try std.testing.expectEqual(@as(usize, 1), reopened_edges.len);
    try std.testing.expectEqual(@as(f64, 1), reopened_edges[0].weight);
    try std.testing.expect(std.mem.indexOf(u8, reopened_edges[0].metadata, "\"winner\":\"fallback\"") != null);
}

// Regression for PR #957's review of "fix(storage): stop a surviving asset
// contender from resurrecting an explicitly deleted graph edge": removing an
// ordinary (non-winning, non-direct) contender must still leave the winning
// contender's edge in place. The sibling precedence test above exercises the
// opposite edge of this same reconcileGlobalGraphEdgeWinner path (removing
// the winner reveals the fallback); this one removes the fallback while the
// winner survives untouched, which the unconditional winner-removal this
// fixes used to wipe out alongside it.
test "db multi-source graph precedence keeps the primary edge after removing the fallback" {
    const alloc = std.testing.allocator;

    var path_tmp = try db_mod.TestDirectory.init("db");
    defer path_tmp.cleanup();
    const path = path_tmp.path().ptr;
    defer db_mod.cleanupTempDir(path);

    {
        var db = try DB.open(alloc, std.mem.span(path), .{});
        defer db.close();

        try db.enrichment.addEnrichment(.{
            .name = "primary_relations_v1",
            .kind = .asset,
            .field = "primary_relations",
            .content_type = "application/json",
        });
        try db.enrichment.addEnrichment(.{
            .name = "fallback_relations_v1",
            .kind = .asset,
            .field = "fallback_relations",
            .content_type = "application/json",
        });
        try db.addIndex(.{
            .name = "relations_graph",
            .kind = .graph,
            .config_json =
            \\{"sources":[{"artifact":"primary_relations_v1"},{"artifact":"fallback_relations_v1"}]}
            ,
        });

        try db.batch(.{
            .writes = &.{.{
                .key = "doc:a",
                .value =
                \\{"primary_relations":{"type":"mentions","target":{"document_id":"doc:b"},"weight":2,"winner":"primary"},"fallback_relations":{"type":"mentions","target":{"document_id":"doc:b"},"weight":1,"winner":"fallback"}}
                ,
            }},
            .sync_level = .enrichments,
        });
        try db.runUntilIdle();

        {
            const edges = try db.getEdges(alloc, "relations_graph", "doc:a", "mentions", .out);
            defer db_mod.graph_mod.GraphIndex.freeEdges(alloc, edges);
            try std.testing.expectEqual(@as(usize, 1), edges.len);
            try std.testing.expectEqual(@as(f64, 2), edges[0].weight);
            try std.testing.expect(std.mem.indexOf(u8, edges[0].metadata, "\"winner\":\"primary\"") != null);
        }
        const logical_edge_key = try db_mod.internal_keys.graphEdgeArtifactKeyAlloc(alloc, "doc:a", "relations_graph", "mentions", "doc:b");
        defer alloc.free(logical_edge_key);
        const contender_prefix = try db_mod.internal_keys.graphEdgeContenderEdgePrefixAlloc(alloc, "doc:a", "relations_graph", logical_edge_key);
        defer alloc.free(contender_prefix);
        {
            const contenders = try db.core.store.scanPrefix(alloc, contender_prefix);
            defer db_mod.docstore_mod.DocStore.freeResults(alloc, contenders);
            try std.testing.expectEqual(@as(usize, 2), contenders.len);
        }

        // Removing the lower-precedence (non-winning) artifact must leave
        // the still-winning primary edge exactly as it was, not delete it.
        try db.batch(.{
            .writes = &.{.{
                .key = "doc:a",
                .value =
                \\{"primary_relations":{"type":"mentions","target":{"document_id":"doc:b"},"weight":2,"winner":"primary"}}
                ,
            }},
            .sync_level = .enrichments,
        });
        try db.runUntilIdle();

        const edges = try db.getEdges(alloc, "relations_graph", "doc:a", "mentions", .out);
        defer db_mod.graph_mod.GraphIndex.freeEdges(alloc, edges);
        try std.testing.expectEqual(@as(usize, 1), edges.len);
        try std.testing.expectEqual(@as(f64, 2), edges[0].weight);
        try std.testing.expect(std.mem.indexOf(u8, edges[0].metadata, "\"winner\":\"primary\"") != null);
        {
            const contenders = try db.core.store.scanPrefix(alloc, contender_prefix);
            defer db_mod.docstore_mod.DocStore.freeResults(alloc, contenders);
            try std.testing.expectEqual(@as(usize, 1), contenders.len);
        }
        try db.sync(true);
    }

    var reopened = try DB.open(alloc, std.mem.span(path), .{});
    defer reopened.close();
    const reopened_edges = try reopened.getEdges(alloc, "relations_graph", "doc:a", "mentions", .out);
    defer db_mod.graph_mod.GraphIndex.freeEdges(alloc, reopened_edges);
    try std.testing.expectEqual(@as(usize, 1), reopened_edges.len);
    try std.testing.expectEqual(@as(f64, 2), reopened_edges[0].weight);
    try std.testing.expect(std.mem.indexOf(u8, reopened_edges[0].metadata, "\"winner\":\"primary\"") != null);
}

test "db portable restore rebuilds multi-source graph contender provenance" {
    const alloc = std.testing.allocator;

    var source_tmp = try db_mod.TestDirectory.init("db");
    defer source_tmp.cleanup();
    const source_path = source_tmp.path().ptr;
    defer db_mod.cleanupTempDir(source_path);
    var restore_tmp = try db_mod.TestDirectory.init("db");
    defer restore_tmp.cleanup();
    const restore_path = restore_tmp.path().ptr;
    defer db_mod.cleanupTempDir(restore_path);

    const primary_backend: db_mod.PrimaryBackend = .{ .lsm = .{ .flush_threshold = 1 } };
    var portable = std.ArrayList(u8).empty;
    defer portable.deinit(alloc);

    {
        var source = try DB.open(alloc, std.mem.span(source_path), .{ .primary_backend = primary_backend });
        defer source.close();
        try source.enrichment.addEnrichment(.{
            .name = "primary_relations_v1",
            .kind = .asset,
            .field = "primary_relations",
            .content_type = "application/json",
        });
        try source.enrichment.addEnrichment(.{
            .name = "fallback_relations_v1",
            .kind = .asset,
            .field = "fallback_relations",
            .content_type = "application/json",
        });
        try source.addIndex(.{
            .name = "relations_graph",
            .kind = .graph,
            .config_json =
            \\{"ttl":{"duration":"1h"},"sources":[{"artifact":"primary_relations_v1"},{"artifact":"fallback_relations_v1"}]}
            ,
        });
        try source.batch(.{
            .writes = &.{.{
                .key = "doc:a",
                .value =
                \\{"primary_relations":{"type":"mentions","target":{"document_id":"doc:b"},"weight":2,"winner":"primary"},"fallback_relations":{"type":"mentions","target":{"document_id":"doc:b"},"weight":1,"winner":"fallback"}}
                ,
            }},
            .sync_level = .enrichments,
        });
        try source.runUntilIdle();
        try db_mod.portable_backup.exportPortable(alloc, source.core.store, &portable);
    }

    {
        var restored = try DB.open(alloc, std.mem.span(restore_path), .{
            .primary_backend = primary_backend,
            .open_mode = .writer_no_replay,
            .start_index_workers = false,
            .start_optional_runtimes = false,
        });
        defer restored.close();
        try db_mod.portable_backup.importPortable(alloc, restored.core.store, portable.items);
    }

    try DB.markRestorePrimaryRestoredForPathWithArtifact(
        alloc,
        std.mem.span(restore_path),
        "graph-snapshot",
        "file:///tmp/backups",
        "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855",
        "snapshots/graph-snapshot",
        7002,
    );
    try DB.markRestoreRuntimeRepairNeeded(alloc, std.mem.span(restore_path));
    while (try DB.restoreRuntimeRepairNeededForPath(alloc, std.mem.span(restore_path))) {
        var restored = try DB.open(alloc, std.mem.span(restore_path), .{
            .primary_backend = primary_backend,
            .open_mode = .writer_no_replay,
            .start_index_workers = false,
            .enrichment = .{
                .owner_id = "restore-worker",
                .enable_without_producers = true,
            },
        });
        defer restored.close();
        try std.testing.expect(try restored.repairRestoreRuntimeStateStepIfNeeded(alloc));
    }

    var restored = try DB.open(alloc, std.mem.span(restore_path), .{
        .primary_backend = primary_backend,
        .enrichment = .{
            .owner_id = "restore-worker",
            .enable_without_producers = true,
        },
    });
    defer restored.close();
    try restored.batch(.{
        .writes = &.{.{
            .key = "doc:a",
            .value =
            \\{"fallback_relations":{"type":"mentions","target":{"document_id":"doc:b"},"weight":1,"winner":"fallback"}}
            ,
        }},
        .sync_level = .enrichments,
    });
    try restored.runUntilIdle();

    const edges = try restored.getEdges(alloc, "relations_graph", "doc:a", "mentions", .out);
    defer db_mod.graph_mod.GraphIndex.freeEdges(alloc, edges);
    try std.testing.expectEqual(@as(usize, 1), edges.len);
    try std.testing.expectEqual(@as(f64, 1), edges[0].weight);
    try std.testing.expect(std.mem.indexOf(u8, edges[0].metadata, "\"winner\":\"fallback\"") != null);
    const due_rows = try restored.core.store.scanPrefix(alloc, &db_mod.internal_keys.graph_edge_expiration_index_prefix);
    defer db_mod.docstore_mod.DocStore.freeResults(alloc, due_rows);
    try std.testing.expectEqual(@as(usize, 1), due_rows.len);
    const due = (try db_mod.graph_edge_ttl_expiration.decodeDue(due_rows[0].value)).source;
    try std.testing.expectEqualStrings("relations_graph", due.index_name);
}

test "db graph artifact source lifecycle reuses and protects asset enrichments" {
    const alloc = std.testing.allocator;

    var path_tmp = try db_mod.TestDirectory.init("db");
    defer path_tmp.cleanup();
    const path = path_tmp.path().ptr;
    defer db_mod.cleanupTempDir(path);

    var db = try DB.open(alloc, std.mem.span(path), .{});
    defer db.close();

    try db.addIndex(.{
        .name = "relations_graph_a",
        .kind = .graph,
        .config_json =
        \\{
        \\  "source":{"artifact":"relations_v1","path":"$.relations[*]","format":"extraction_relation"},
        \\  "artifact":{"name":"relations_v1","kind":"asset","source":{"type":"field","value":"relations"},"content_type":"application/json"}
        \\}
        ,
    });

    const created = (try db.enrichment.getEnrichment(alloc, .asset, "relations_v1")) orelse return error.TestUnexpectedResult;
    defer {
        var tmp = created;
        tmp.deinit(alloc);
    }
    try std.testing.expectEqualStrings("relations", created.field);
    try std.testing.expectEqualStrings("application/json", created.content_type);

    try db.addIndex(.{
        .name = "relations_graph_b",
        .kind = .graph,
        .config_json =
        \\{
        \\  "source":{"artifact":"relations_v1","path":"$.relations[*]","format":"extraction_relation"},
        \\  "artifact":{"name":"relations_v1","kind":"asset","source":{"type":"field","value":"relations"},"content_type":"application/json"}
        \\}
        ,
    });

    try std.testing.expectError(error.EnrichmentInUse, db.enrichment.deleteEnrichment(.asset, "relations_v1"));
    try std.testing.expect(try db.deleteIndex("relations_graph_a"));
    try std.testing.expectError(error.EnrichmentInUse, db.enrichment.deleteEnrichment(.asset, "relations_v1"));

    const still_present = (try db.enrichment.getEnrichment(alloc, .asset, "relations_v1")) orelse return error.TestUnexpectedResult;
    defer {
        var tmp = still_present;
        tmp.deinit(alloc);
    }
    try std.testing.expectEqualStrings("relations", still_present.field);
}

test "db graph artifact source reuses user enrichment and rejects incompatible shorthand" {
    const alloc = std.testing.allocator;

    var path_tmp = try db_mod.TestDirectory.init("db");
    defer path_tmp.cleanup();
    const path = path_tmp.path().ptr;
    defer db_mod.cleanupTempDir(path);

    var db = try DB.open(alloc, std.mem.span(path), .{});
    defer db.close();

    try db.enrichment.addEnrichment(.{
        .name = "relations_v1",
        .kind = .asset,
        .field = "relations",
        .content_type = "application/json",
    });

    try db.addIndex(.{
        .name = "relations_graph",
        .kind = .graph,
        .config_json =
        \\{
        \\  "source":{"artifact":"relations_v1","path":"$.relations[*]","format":"extraction_relation"}
        \\}
        ,
    });

    try std.testing.expectError(error.ConflictingEnrichmentConfig, db.addIndex(.{
        .name = "conflicting_graph",
        .kind = .graph,
        .config_json =
        \\{
        \\  "source":{"artifact":"relations_v1","path":"$.relations[*]","format":"extraction_relation"},
        \\  "artifact":{"name":"relations_v1","kind":"asset","source":{"type":"field","value":"body"},"content_type":"application/json"}
        \\}
        ,
    }));
}

test "db full_index precomputes generated enrichments into the committed batch" {
    const alloc = std.testing.allocator;
    var manager = db_mod.resource_manager_mod.ResourceManager.init(.{ .identity_allocator = alloc });
    defer manager.deinit(alloc);

    var path_tmp = try db_mod.TestDirectory.init("db");
    defer path_tmp.cleanup();
    const path = path_tmp.path().ptr;
    defer db_mod.cleanupTempDir(path);

    var deterministic_dense = db_mod.embedder_mod.DeterministicDenseEmbedder{};
    var deterministic_sparse = db_mod.embedder_mod.DeterministicSparseEmbedder{};
    var db = try DB.open(alloc, std.mem.span(path), .{
        .resource_manager = &manager,
        .enrichment = .{
            .owner_id = "worker-a",
            .dense_embedder = deterministic_dense.interface(),
            .sparse_embedder = deterministic_sparse.interface(),
        },
    });
    defer db.close();

    try db.addIndex(.{
        .name = "dv_v1",
        .kind = .dense_vector,
        .config_json = "{\"field\":\"embedding\",\"dims\":3,\"generator\":{\"kind\":\"dense_embedding\",\"source_field\":\"body\",\"artifact_name\":\"body_chunks_v1\",\"chunk_size\":8,\"chunk_overlap\":2,\"embedding_name\":\"chunk_dense_v1\"}}",
    });
    try db.addIndex(.{
        .name = "sp_v1",
        .kind = .sparse_vector,
        .config_json = "{\"field\":\"sparse_embedding\",\"generator\":{\"kind\":\"sparse_embedding\",\"source_field\":\"body\",\"artifact_name\":\"body_chunks_v1\",\"chunk_size\":8,\"chunk_overlap\":2}}",
    });
    try db.addIndex(.{
        .name = "ft_chunks",
        .kind = .full_text,
        .config_json = "{\"chunk_name\":\"body_chunks_v1\"}",
    });

    try db.batch(.{
        .writes = &.{
            .{ .key = "doc:a", .value = "{\"body\":\"abcdefghijklmno\"}" },
        },
        .sync_level = .full_index,
    });

    try std.testing.expectEqual(@as(u64, 0), manager.sliceStats(.relational_preparation_working_set).used_bytes);

    var text_result = try db.search(alloc, .{
        .index_name = "ft_chunks",
        .full_text = .{ .match = .{ .field = "body", .text = "abcdefgh" } },
    });
    defer text_result.deinit();
    try std.testing.expect(text_result.total_hits > 0);

    var dense_result = try db.search(alloc, .{
        .index_name = "dv_v1",
        .dense = .{ .vector = &.{ 1.0, 0.0, 0.0 }, .k = 1 },
    });
    defer dense_result.deinit();
    try std.testing.expect(dense_result.total_hits > 0);

    const sparse_applied = try db.core.loadAppliedSequence(alloc, "sp_v1");
    try std.testing.expect(sparse_applied > 0);

    const runtime = db.enrichment.runtime.?;
    const dense_activity = runtime.indexEmbeddingActivity("dv_v1");
    const sparse_activity = runtime.indexEmbeddingActivity("sp_v1");
    for ([_]db_mod.types.EmbeddingActivityStats{ dense_activity, sparse_activity }) |activity| {
        try std.testing.expect(activity.chunks_created > 0);
        try std.testing.expect(activity.embedding_batches_completed > 0);
        try std.testing.expect(activity.embeddings_computed > 0);
        try std.testing.expectEqual(@as(u64, 0), activity.active_batch_size);
        try std.testing.expect(!activity.retrying);
    }
}

test "db enrichments precomputes generated enrichments into the committed batch" {
    const alloc = std.testing.allocator;

    var path_tmp = try db_mod.TestDirectory.init("db");
    defer path_tmp.cleanup();
    const path = path_tmp.path().ptr;
    defer db_mod.cleanupTempDir(path);

    var deterministic_dense = db_mod.embedder_mod.DeterministicDenseEmbedder{};
    var deterministic_sparse = db_mod.embedder_mod.DeterministicSparseEmbedder{};
    var db = try DB.open(alloc, std.mem.span(path), .{
        // Inspect the foreground transaction and its exact first watermark.
        // Independent replay may legitimately publish another observation.
        .start_index_workers = false,
        .start_optional_runtime_workers = false,
        .enrichment = .{
            .owner_id = "worker-a",
            .dense_embedder = deterministic_dense.interface(),
            .sparse_embedder = deterministic_sparse.interface(),
        },
    });
    defer db.close();

    try db.addIndex(.{
        .name = "dv_v1",
        .kind = .dense_vector,
        .config_json = "{\"field\":\"embedding\",\"dims\":3,\"generator\":{\"kind\":\"dense_embedding\",\"source_field\":\"body\",\"artifact_name\":\"body_chunks_v1\",\"chunk_size\":8,\"chunk_overlap\":2,\"embedding_name\":\"chunk_dense_v1\"}}",
    });
    try db.addIndex(.{
        .name = "sp_v1",
        .kind = .sparse_vector,
        .config_json = "{\"field\":\"sparse_embedding\",\"generator\":{\"kind\":\"sparse_embedding\",\"source_field\":\"body\",\"artifact_name\":\"body_chunks_v1\",\"chunk_size\":8,\"chunk_overlap\":2}}",
    });
    try db.addIndex(.{
        .name = "ft_chunks",
        .kind = .full_text,
        .config_json = "{\"chunk_name\":\"body_chunks_v1\"}",
    });

    try db.batch(.{
        .writes = &.{
            .{ .key = "doc:a", .value = "{\"body\":\"abcdefghijklmno\"}" },
        },
        .sync_level = .enrichments,
    });
    try std.testing.expectEqual(@as(u64, 1), db.enrichment.runtime.?.stats().applied_sequence);

    const journal_entries = try db_mod.replay_stream_mod.iterateFrom(alloc, db.core.store, 1);
    defer {
        for (journal_entries) |*entry| entry.deinit(alloc);
        alloc.free(journal_entries);
    }
    try std.testing.expectEqual(@as(usize, 1), journal_entries.len);

    var journal_record = try db_mod.change_journal_mod.decodeRecord(alloc, journal_entries[0].payload);
    defer journal_record.deinit();

    try std.testing.expect(journal_record.record.changed_doc_keys.len >= 1);
    try std.testing.expectEqualStrings("doc:a", journal_record.record.changed_doc_keys[0]);
    try std.testing.expect(db_mod.journalRecordHasHint(journal_record.record, .full_text));
    try std.testing.expect(db_mod.journalRecordHasHint(journal_record.record, .dense_vector));
    try std.testing.expect(db_mod.journalRecordHasHint(journal_record.record, .sparse_vector));
    try std.testing.expect(!db_mod.journalRecordHasHint(journal_record.record, .enrichment));
    try std.testing.expect(journal_record.record.changed_artifact_keys.len > 0);
}

test "db enrichments precomputed watermark advances across replay entries without enrichment debt" {
    const alloc = std.testing.allocator;

    var path_tmp = try db_mod.TestDirectory.init("db");
    defer path_tmp.cleanup();
    const path = path_tmp.path().ptr;
    defer db_mod.cleanupTempDir(path);

    var deterministic_dense = db_mod.embedder_mod.DeterministicDenseEmbedder{};
    // This probes foreground precomputed advancement; a concurrent replay
    // worker could consume the inert entry before the initial observation.
    var db = try DB.open(alloc, std.mem.span(path), .{
        .start_optional_runtime_workers = false,
        .enrichment = .{
            .owner_id = "worker-a",
            .dense_embedder = deterministic_dense.interface(),
        },
    });
    defer db.close();

    const inert_payload = try db_mod.change_journal_mod.encodeRecord(alloc, .{
        .sequence = 1,
        .changed_doc_keys = &.{"doc:before"},
        .target_hints = &.{.full_text},
    });
    defer alloc.free(inert_payload);
    try db.core.store.appendReplayOpaque(alloc, 1, inert_payload);
    try db.enrichment.runtime.?.resumeFrom(0, 0);
    try std.testing.expectEqual(@as(u64, 0), db.enrichment.runtime.?.stats().applied_sequence);

    try db.addIndex(.{
        .name = "dv_v1",
        .kind = .dense_vector,
        .config_json = "{\"field\":\"embedding\",\"dims\":3,\"generator\":{\"kind\":\"dense_embedding\",\"source_field\":\"body\",\"embedding_name\":\"body_dense_v1\"}}",
    });

    try db.batch(.{
        .writes = &.{
            .{ .key = "doc:a", .value = "{\"body\":\"precomputed dense text\"}" },
        },
        .sync_level = .enrichments,
    });

    try std.testing.expectEqual(db.core.nextDerivedSequence(), db.enrichment.runtime.?.stats().applied_sequence);
}

test "db asset producer enrichments execute fake providers and skip unchanged state" {
    const alloc = std.testing.allocator;

    var path_tmp = try db_mod.TestDirectory.init("db");
    defer path_tmp.cleanup();
    const path = path_tmp.path().ptr;
    defer db_mod.cleanupTempDir(path);

    var fake = db_mod.TestAssetProducer{};
    var db = try DB.open(alloc, std.mem.span(path), .{
        .enrichment = .{
            .owner_id = "worker-a",
            .asset_producer = fake.producer(),
        },
    });
    defer db.close();

    try db.enrichment.addEnrichment(.{
        .name = "generated_title_v1",
        .kind = .asset,
        .field = "body",
        .content_type = "text/plain",
        .producer_json = "{\"type\":\"generator\",\"config\":{\"provider\":\"antfly\",\"model\":\"test-generator\"}}",
    });
    try db.enrichment.addEnrichment(.{
        .name = "image_text_v1",
        .kind = .asset,
        .field = "image",
        .content_type = "text/plain",
        .producer_json = "{\"type\":\"reader\",\"config\":{\"provider\":\"mock\"}}",
    });
    try db.enrichment.addEnrichment(.{
        .name = "audio_text_v1",
        .kind = .asset,
        .field = "audio",
        .content_type = "text/plain",
        .producer_json = "{\"type\":\"transcriber\",\"config\":{\"provider\":\"mock\"}}",
    });
    try db.enrichment.addEnrichment(.{
        .name = "relations_v1",
        .kind = .asset,
        .field = "body",
        .content_type = "application/json",
        .producer_json = "{\"type\":\"extractor\",\"config\":{\"provider\":\"mock\"}}",
    });
    try db.enrichment.addEnrichment(.{
        .name = "body_copy_v1",
        .kind = .asset,
        .field = "body",
        .content_type = "text/plain",
        .producer_json = "{\"type\":\"copy\"}",
    });

    try db.batch(.{
        .writes = &.{.{
            .key = "doc:a",
            .value = "{\"body\":\"hello\",\"image\":\"data:image/png;base64,aaa\",\"audio\":\"https://example.test/a.wav\"}",
        }},
        .sync_level = .enrichments,
    });

    try std.testing.expectEqual(@as(usize, 4), fake.calls);
    try std.testing.expectEqual(@as(usize, 1), fake.generator_calls);
    try std.testing.expectEqual(@as(usize, 1), fake.reader_calls);
    try std.testing.expectEqual(@as(usize, 1), fake.transcriber_calls);
    try std.testing.expectEqual(@as(usize, 1), fake.extractor_calls);

    var first = (try db.lookup(alloc, "doc:a", .{
        .fields = &.{"_artifacts"},
        .include_all_fields = false,
    })).?;
    defer first.deinit(alloc);
    var parsed_first = try std.json.parseFromSlice(std.json.Value, alloc, first.json, .{});
    defer parsed_first.deinit();
    const first_artifacts = parsed_first.value.object.get("_artifacts").?.object;
    try std.testing.expectEqualStrings("generator:hello", first_artifacts.get("generated_title_v1").?.object.get("value").?.string);
    try std.testing.expectEqualStrings("reader:data:image/png;base64,aaa", first_artifacts.get("image_text_v1").?.object.get("value").?.string);
    try std.testing.expectEqualStrings("transcriber:https://example.test/a.wav", first_artifacts.get("audio_text_v1").?.object.get("value").?.string);

    try db.batch(.{
        .writes = &.{.{
            .key = "doc:a",
            .value = "{\"body\":\"hello\",\"image\":\"data:image/png;base64,aaa\",\"audio\":\"https://example.test/a.wav\"}",
        }},
        .sync_level = .enrichments,
    });
    try std.testing.expectEqual(@as(usize, 4), fake.calls);

    try db.batch(.{
        .writes = &.{.{
            .key = "doc:a",
            .value = "{\"body\":\"goodbye\",\"image\":\"data:image/png;base64,aaa\",\"audio\":\"https://example.test/a.wav\"}",
        }},
        .sync_level = .enrichments,
    });
    try std.testing.expectEqual(@as(usize, 6), fake.calls);
    try std.testing.expectEqual(@as(usize, 2), fake.generator_calls);
    try std.testing.expectEqual(@as(usize, 1), fake.reader_calls);
    try std.testing.expectEqual(@as(usize, 1), fake.transcriber_calls);
    try std.testing.expectEqual(@as(usize, 2), fake.extractor_calls);

    // Explicit regeneration bypasses unchanged-state checks for every asset
    // producer family. Copy is handled locally; the other four invoke the
    // configured producer again.
    for ([_][]const u8{ "generated_title_v1", "image_text_v1", "audio_text_v1", "relations_v1", "body_copy_v1" }) |artifact_name| {
        try std.testing.expect(try db.reprocessDocumentArtifact(alloc, "doc:a", artifact_name));
    }
    try std.testing.expectEqual(@as(usize, 10), fake.calls);
    try std.testing.expectEqual(@as(usize, 3), fake.generator_calls);
    try std.testing.expectEqual(@as(usize, 2), fake.reader_calls);
    try std.testing.expectEqual(@as(usize, 2), fake.transcriber_calls);
    try std.testing.expectEqual(@as(usize, 3), fake.extractor_calls);

    var range_reprocess = try db.reprocessDocumentArtifactRange(alloc, "generated_title_v1", .{ .limit = 10 });
    defer range_reprocess.deinit(alloc);
    try std.testing.expectEqual(@as(usize, 1), range_reprocess.reprocessed);
    try std.testing.expectEqual(@as(usize, 11), fake.calls);
    try std.testing.expectEqual(@as(usize, 4), fake.generator_calls);

    const asset_key = try db_mod.internal_keys.artifactNamedPrefixAlloc(alloc, "doc:a", "asset", "generated_title_v1");
    defer alloc.free(asset_key);
    const marker_key = try db_mod.internal_keys.assetArtifactSourceIndexKeyAlloc(alloc, "generated_title_v1", "doc:a");
    defer alloc.free(marker_key);
    const state_key = try db_mod.assetStateKeyAlloc(alloc, "doc:a", "generated_title_v1");
    defer alloc.free(state_key);
    const stored_asset = try db.core.store.get(alloc, asset_key);
    alloc.free(stored_asset);
    const stored_marker = try db.core.store.get(alloc, marker_key);
    defer alloc.free(stored_marker);
    try std.testing.expectEqualStrings(asset_key, stored_marker);
    const stored_state = try db.core.store.get(alloc, state_key);
    alloc.free(stored_state);

    try db.batch(.{
        .deletes = &.{"doc:a"},
        .sync_level = .write,
    });
    try std.testing.expectError(error.NotFound, db.core.store.get(alloc, asset_key));
    try std.testing.expectError(error.NotFound, db.core.store.get(alloc, marker_key));
    try std.testing.expectError(error.NotFound, db.core.store.get(alloc, state_key));
}

test "db asset producer enrichments batch compatible generated assets" {
    const alloc = std.testing.allocator;

    const BatchProducer = struct {
        batch_calls: usize = 0,
        batch_lengths: [4]usize = .{ 0, 0, 0, 0 },
        single_calls: usize = 0,

        fn producer(self: *@This()) db_mod.asset_producer_mod.Producer {
            return .{
                .ptr = self,
                .vtable = &.{
                    .produce = produce,
                    .produce_batch = produceBatch,
                    .invocation_memory_for_requests = db_mod.TestAssetProducer.invocationMemory,
                },
            };
        }

        fn produce(ptr: *anyopaque, alloc_inner: Allocator, request: db_mod.asset_producer_mod.Request) ![]u8 {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            self.single_calls += 1;
            return try std.fmt.allocPrint(alloc_inner, "single:{s}", .{request.source_text});
        }

        fn produceBatch(ptr: *anyopaque, alloc_inner: Allocator, requests: []const db_mod.asset_producer_mod.Request) ![][]u8 {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            self.batch_lengths[self.batch_calls] = requests.len;
            self.batch_calls += 1;
            const out = try alloc_inner.alloc([]u8, requests.len);
            errdefer {
                for (out) |item| {
                    if (item.len > 0) alloc_inner.free(item);
                }
                alloc_inner.free(out);
            }
            for (out, requests) |*item, request| {
                item.* = try std.fmt.allocPrint(alloc_inner, "batch:{s}", .{request.source_text});
            }
            return out;
        }
    };

    var path_tmp = try db_mod.TestDirectory.init("db");
    defer path_tmp.cleanup();
    const path = path_tmp.path().ptr;
    defer db_mod.cleanupTempDir(path);

    var fake = BatchProducer{};
    var db = try DB.open(alloc, std.mem.span(path), .{
        .enrichment = .{
            .owner_id = "worker-a",
            .asset_producer = fake.producer(),
        },
    });
    defer db.close();

    try db.enrichment.addEnrichment(.{
        .name = "summary_v1",
        .kind = .asset,
        .field = "body",
        .content_type = "text/plain",
        .producer_json = "{\"type\":\"generator\",\"config\":{\"provider\":\"antfly\",\"model\":\"test-generator\"}}",
        .execution = .{ .batch_items = 2, .batch_bytes = 1024 },
    });

    try db.batch(.{
        .writes = &.{
            .{ .key = "doc:a", .value = "{\"body\":\"alpha\"}" },
            .{ .key = "doc:b", .value = "{\"body\":\"beta\"}" },
        },
        .sync_level = .enrichments,
    });

    try std.testing.expectEqual(@as(usize, 1), fake.batch_calls);
    try std.testing.expectEqual(@as(usize, 2), fake.batch_lengths[0]);
    try std.testing.expectEqual(@as(usize, 0), fake.single_calls);

    var first = (try db.lookup(alloc, "doc:a", .{
        .fields = &.{"_artifacts"},
        .include_all_fields = false,
    })).?;
    defer first.deinit(alloc);
    var parsed_first = try std.json.parseFromSlice(std.json.Value, alloc, first.json, .{});
    defer parsed_first.deinit();
    try std.testing.expectEqualStrings("batch:alpha", parsed_first.value.object.get("_artifacts").?.object.get("summary_v1").?.object.get("value").?.string);

    var second = (try db.lookup(alloc, "doc:b", .{
        .fields = &.{"_artifacts"},
        .include_all_fields = false,
    })).?;
    defer second.deinit(alloc);
    var parsed_second = try std.json.parseFromSlice(std.json.Value, alloc, second.json, .{});
    defer parsed_second.deinit();
    try std.testing.expectEqualStrings("batch:beta", parsed_second.value.object.get("_artifacts").?.object.get("summary_v1").?.object.get("value").?.string);
}

test "db asset producer sync precompute fails closed on permanent item failure" {
    const alloc = std.testing.allocator;

    const FallbackProducer = struct {
        batch_calls: usize = 0,
        single_calls: usize = 0,

        fn producer(self: *@This()) db_mod.asset_producer_mod.Producer {
            return .{
                .ptr = self,
                .vtable = &.{
                    .produce = produce,
                    .produce_batch = produceBatch,
                    .invocation_memory_for_requests = db_mod.TestAssetProducer.invocationMemory,
                },
            };
        }

        fn produce(ptr: *anyopaque, alloc_inner: Allocator, request: db_mod.asset_producer_mod.Request) ![]u8 {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            self.single_calls += 1;
            if (std.mem.eql(u8, request.source_text, "bad")) return error.BadAssetInput;
            return try std.fmt.allocPrint(alloc_inner, "ok:{s}", .{request.source_text});
        }

        fn produceBatch(ptr: *anyopaque, _: Allocator, _: []const db_mod.asset_producer_mod.Request) ![][]u8 {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            self.batch_calls += 1;
            return error.BatchItemFailed;
        }
    };

    var path_tmp = try db_mod.TestDirectory.init("db");
    defer path_tmp.cleanup();
    const path = path_tmp.path().ptr;
    defer db_mod.cleanupTempDir(path);

    var fake = FallbackProducer{};
    var db = try DB.open(alloc, std.mem.span(path), .{
        .enrichment = .{
            .owner_id = "worker-a",
            .asset_producer = fake.producer(),
        },
    });
    defer db.close();

    try db.enrichment.addEnrichment(.{
        .name = "summary_v1",
        .kind = .asset,
        .field = "body",
        .content_type = "text/plain",
        .producer_json = "{\"type\":\"generator\",\"config\":{\"provider\":\"antfly\",\"model\":\"test-generator\"}}",
        .execution = .{ .batch_items = 2, .batch_bytes = 1024 },
    });

    try db.batch(.{
        .writes = &.{
            .{ .key = "doc:good", .value = "{\"body\":\"good\"}" },
        },
        .sync_level = .enrichments,
    });

    try std.testing.expectEqual(@as(usize, 1), fake.batch_calls);
    try std.testing.expectEqual(@as(usize, 1), fake.single_calls);

    const good_key = try db_mod.internal_keys.artifactNamedPrefixAlloc(alloc, "doc:good", "asset", "summary_v1");
    defer alloc.free(good_key);
    const good_value = try db.core.store.get(alloc, good_key);
    defer alloc.free(good_value);
    try std.testing.expectEqualStrings("ok:good", good_value);

    try std.testing.expectError(error.BadAssetInput, db.batch(.{
        .writes = &.{
            .{ .key = "doc:good", .value = "{\"body\":\"bad\"}" },
        },
        .sync_level = .enrichments,
    }));

    try std.testing.expectEqual(@as(usize, 2), fake.batch_calls);
    try std.testing.expectEqual(@as(usize, 2), fake.single_calls);

    const committed_doc = (try db.get(alloc, "doc:good")) orelse return error.TestExpectedEqual;
    defer alloc.free(committed_doc);
    try std.testing.expectEqualStrings("{\"body\":\"good\"}", committed_doc);

    const unchanged_good_value = try db.core.store.get(alloc, good_key);
    defer alloc.free(unchanged_good_value);
    try std.testing.expectEqualStrings("ok:good", unchanged_good_value);

    try std.testing.expectError(error.BadAssetInput, db.batch(.{
        .writes = &.{
            .{ .key = "doc:bad", .value = "{\"body\":\"bad\"}" },
        },
        .sync_level = .enrichments,
    }));

    try std.testing.expectEqual(@as(usize, 3), fake.batch_calls);
    try std.testing.expectEqual(@as(usize, 3), fake.single_calls);

    const rejected_doc = try db.get(alloc, "doc:bad");
    defer if (rejected_doc) |value| alloc.free(value);
    try std.testing.expect(rejected_doc == null);

    const bad_key = try db_mod.internal_keys.artifactNamedPrefixAlloc(alloc, "doc:bad", "asset", "summary_v1");
    defer alloc.free(bad_key);
    try std.testing.expectError(error.NotFound, db.core.store.get(alloc, bad_key));
}

test "db document extraction asset materializes unit artifacts from data url" {
    const alloc = std.testing.allocator;

    var path_tmp = try db_mod.TestDirectory.init("db");
    defer path_tmp.cleanup();
    const path = path_tmp.path().ptr;
    defer db_mod.cleanupTempDir(path);

    var fake = db_mod.TestAssetProducer{};
    var db = try DB.open(alloc, std.mem.span(path), .{
        .enrichment = .{
            .owner_id = "worker-a",
            .asset_producer = fake.producer(),
        },
    });
    defer db.close();

    try db.enrichment.addEnrichment(.{
        .name = "document_units_v1",
        .kind = .asset,
        .field = "url",
        .content_type = "application/json",
        .producer_json = "{\"type\":\"document_extraction\",\"config\":{}}",
    });
    // A source may have multiple asset enrichments. Opaque non-document
    // payloads must never be sniffed as extraction manifests by traversal.
    try db.enrichment.addEnrichment(.{
        .name = "source_copy_v1",
        .kind = .asset,
        .field = "url",
        .content_type = "text/plain",
        .producer_json = "{\"type\":\"copy\"}",
    });

    try db.batch(.{
        .writes = &.{.{
            .key = "doc:a",
            .value = "{\"url\":\"data:text/plain;base64,YWxwaGEgYmV0YQ==\"}",
        }},
        .sync_level = .enrichments,
    });

    try std.testing.expectEqual(@as(usize, 0), fake.calls);

    const manifest_key = try db_mod.internal_keys.artifactNamedPrefixAlloc(alloc, "doc:a", "asset", "document_units_v1");
    defer alloc.free(manifest_key);
    const unit_key = try db_mod.internal_keys.documentUnitArtifactKeyAlloc(alloc, "doc:a", "document_units_v1", "document:000001");
    defer alloc.free(unit_key);
    const state_key = try db_mod.assetStateKeyAlloc(alloc, "doc:a", "document_units_v1");
    defer alloc.free(state_key);
    const navigation_summary_key = try db_mod.internal_keys.documentUnitNavigationSummaryKeyAlloc(alloc, "doc:a", "document_units_v1");
    defer alloc.free(navigation_summary_key);
    const navigation_block_key = try db_mod.internal_keys.documentUnitNavigationBlockKeyAlloc(alloc, "doc:a", "document_units_v1", 0);
    defer alloc.free(navigation_block_key);

    const manifest = try db.core.store.get(alloc, manifest_key);
    defer alloc.free(manifest);
    try std.testing.expect(std.mem.indexOf(u8, manifest, "\"artifact_type\":\"document_units\"") != null);
    try std.testing.expect(std.mem.indexOf(u8, manifest, "\"route_type\":\"text\"") != null);
    try std.testing.expect(std.mem.indexOf(u8, manifest, "\"unit_count\":1") != null);

    const unit_payload = try db.core.store.get(alloc, unit_key);
    defer alloc.free(unit_payload);
    try std.testing.expect(std.mem.indexOf(u8, unit_payload, "\"_parent_doc_key\":\"doc:a\"") != null);
    try std.testing.expect(std.mem.indexOf(u8, unit_payload, "\"_artifact_name\":\"document_units_v1\"") != null);
    try std.testing.expect(std.mem.indexOf(u8, unit_payload, "\"unit_id\":\"document:000001\"") != null);
    try std.testing.expect(std.mem.indexOf(u8, unit_payload, "\"text\":\"alpha beta\"") != null);
    try std.testing.expect(std.mem.indexOf(u8, unit_payload, "\"method\":\"text\"") != null);
    try std.testing.expect(std.mem.indexOf(u8, unit_payload, "\"_artifact_unit_fingerprint\"") != null);
    try std.testing.expect(std.mem.indexOf(u8, unit_payload, "\"_hierarchy\"") == null);

    const state = try db.core.store.get(alloc, state_key);
    defer alloc.free(state);
    try std.testing.expect(std.mem.indexOf(u8, state, "\"kind\":\"document_extraction_state_v1\"") != null);
    try std.testing.expect(std.mem.indexOf(u8, state, "\"unit_descriptors\"") != null);
    try std.testing.expect(std.mem.indexOf(u8, state, "document:000001") != null);

    const navigation_summary = try db.core.store.get(alloc, navigation_summary_key);
    defer alloc.free(navigation_summary);
    try db_mod.ant_json.testing.expectSubsetJsonText(
        alloc,
        "{\"kind\":\"document_unit_navigation_summary_v1\",\"generation\":1,\"unit_count\":1,\"block_count\":1}",
        navigation_summary,
    );

    const navigation_block = try db.core.store.get(alloc, navigation_block_key);
    defer alloc.free(navigation_block);
    try std.testing.expect(std.mem.indexOf(u8, navigation_block, "\"kind\":\"document_unit_navigation_block_v1\"") != null);
    try std.testing.expect(std.mem.indexOf(u8, navigation_block, "document:000001") != null);

    // Simulate an artifact retained from before compact navigation metadata.
    // An unchanged-source enrichment pass must backfill from descriptors while
    // preserving the manifest generation, rather than downloading/extracting.
    var legacy_state_parsed = try std.json.parseFromSlice(std.json.Value, alloc, state, .{});
    defer legacy_state_parsed.deinit();
    _ = legacy_state_parsed.value.object.orderedRemove("navigation_digest");
    _ = legacy_state_parsed.value.object.orderedRemove("navigation_block_count");
    _ = legacy_state_parsed.value.object.orderedRemove("navigation_block_size");
    const legacy_state = try std.json.Stringify.valueAlloc(alloc, legacy_state_parsed.value, .{});
    defer alloc.free(legacy_state);
    try db.core.store.put(state_key, legacy_state);
    try db.core.store.delete(navigation_summary_key);
    try db.core.store.delete(navigation_block_key);
    var backfill_writes = std.ArrayListUnmanaged(db_mod.types.BatchWrite).empty;
    defer {
        for (backfill_writes.items) |write| {
            alloc.free(@constCast(write.key));
            alloc.free(@constCast(write.value));
        }
        backfill_writes.deinit(alloc);
    }
    var backfill_deletes = std.ArrayListUnmanaged([]const u8).empty;
    defer {
        for (backfill_deletes.items) |key| alloc.free(@constCast(key));
        backfill_deletes.deinit(alloc);
    }
    try db_mod.appendDocumentExtractionNavigationBackfill(
        alloc,
        &db,
        "doc:a",
        "document_units_v1",
        "6c1146dc037bb3ca77d4248754322b14697b236ddaddf5203b13ab97f133fb78",
        legacy_state,
        (try db_mod.documentUnitHierarchyManifestInfo(alloc, manifest)).published_generation.?,
        &backfill_writes,
        &backfill_deletes,
    );
    for (backfill_writes.items) |write| try db.core.store.put(write.key, write.value);
    for (backfill_deletes.items) |key| try db.core.store.delete(key);
    const backfilled_manifest = try db.core.store.get(alloc, manifest_key);
    defer alloc.free(backfilled_manifest);
    try std.testing.expectEqualStrings(manifest, backfilled_manifest);
    const backfilled_state = try db.core.store.get(alloc, state_key);
    defer alloc.free(backfilled_state);
    const backfilled_summary = try db.core.store.get(alloc, navigation_summary_key);
    defer alloc.free(backfilled_summary);
    const backfilled_block = try db.core.store.get(alloc, navigation_block_key);
    defer alloc.free(backfilled_block);
    try std.testing.expect(try db_mod.hierarchy_navigation.indexMetadataMatches(alloc, backfilled_state, backfilled_summary, 1));

    const navigation_order = [_]db_mod.types.SortField{
        .{ .field = "_hierarchy.position" },
        .{ .field = "_id" },
    };

    // A missing summary is only eligible for legacy fallback when the state
    // predates all navigation metadata. Current-format state must never be
    // traversed through that fallback during a partial publication.
    try db.core.store.delete(navigation_summary_key);
    try std.testing.expectError(error.StorageReadTemporarilyUnavailable, db.search(alloc, .{
        .hierarchy_children = .{ .parent_id = "doc:a" },
        .return_mode = .unit,
        .order_by = &navigation_order,
        .limit = 20,
        .include_all_fields = false,
    }));
    try db.core.store.put(navigation_summary_key, backfilled_summary);

    try std.testing.expectError(error.InvalidQueryRequest, db.search(alloc, .{
        .query = .{ .match_none = {} },
        .hierarchy_children = .{ .parent_id = "doc:a" },
        .return_mode = .unit,
        .order_by = &navigation_order,
        .include_all_fields = false,
    }));
    var navigation = try db.search(alloc, .{
        .hierarchy_children = .{ .parent_id = "doc:a" },
        .return_mode = .unit,
        .order_by = &navigation_order,
        .limit = 20,
        .include_all_fields = false,
        .fields = &.{ "unit_id", "unit_type", "text", db_mod.hierarchy_navigation_unit_fingerprint_field },
    });
    defer navigation.deinit();
    try std.testing.expectEqual(@as(u32, 1), navigation.total_hits);
    try std.testing.expectEqual(@as(usize, 1), navigation.hits.len);
    try std.testing.expectEqual(@as(usize, 2), navigation.hits[0].sort_values.len);
    try std.testing.expect(std.mem.startsWith(u8, navigation.hits[0].sort_values[0].string, "hn3/"));
    try std.testing.expect(std.mem.indexOf(u8, navigation.hits[0].sort_values[0].string, "/646f63756d656e745f756e6974735f7631/") != null);
    try std.testing.expectEqualStrings("document:000001", navigation.hits[0].artifact_ref.?.unit_id.?);
    var projected_unit = try std.json.parseFromSlice(std.json.Value, alloc, navigation.hits[0].stored_data.?, .{});
    defer projected_unit.deinit();
    try db_mod.ant_json.testing.expectSubsetJsonValue(
        alloc,
        "{\"unit_id\":\"document:000001\",\"text\":\"alpha beta\"}",
        projected_unit.value,
    );
    try std.testing.expect(projected_unit.value.object.get("_hierarchy") == null);
    try std.testing.expect(projected_unit.value.object.get(db_mod.hierarchy_navigation_unit_fingerprint_field) == null);

    // A reader that races an extraction must never combine the old summary
    // with artifacts being rewritten under an in-progress manifest.
    try db.core.store.put(
        manifest_key,
        "{\"artifact_type\":\"document_units\",\"generation\":1," ++
            "\"merge_plan\":{\"from_generation\":1,\"to_generation\":2,\"status\":\"in_progress\"}}",
    );
    try std.testing.expectError(error.StorageReadTemporarilyUnavailable, db.search(alloc, .{
        .hierarchy_children = .{ .parent_id = "doc:a" },
        .return_mode = .unit,
        .order_by = &navigation_order,
        .limit = 20,
        .include_all_fields = false,
    }));
    try db.core.store.put(
        manifest_key,
        "{\"artifact_type\":\"document_units\",\"generation\":2," ++
            "\"merge_plan\":{\"from_generation\":1,\"to_generation\":2,\"status\":\"failed\"}}",
    );
    try std.testing.expectError(error.DocumentArtifactPublicationFailed, db.search(alloc, .{
        .hierarchy_children = .{ .parent_id = "doc:a" },
        .return_mode = .unit,
        .order_by = &navigation_order,
        .limit = 20,
        .include_all_fields = false,
    }));
    try db.core.store.put(manifest_key, manifest);

    // Independently read records are cross-checked by revision so even an
    // unexpected non-atomic store implementation fails closed.
    var mismatched_summary_parsed = try std.json.parseFromSlice(std.json.Value, alloc, backfilled_summary, .{});
    defer mismatched_summary_parsed.deinit();
    mismatched_summary_parsed.value.object.getPtr("generation").?.* = .{ .integer = 2 };
    const mismatched_summary = try std.json.Stringify.valueAlloc(alloc, mismatched_summary_parsed.value, .{});
    defer alloc.free(mismatched_summary);
    try db.core.store.put(navigation_summary_key, mismatched_summary);
    try std.testing.expectError(error.StorageReadTemporarilyUnavailable, db.search(alloc, .{
        .hierarchy_children = .{ .parent_id = "doc:a" },
        .return_mode = .unit,
        .order_by = &navigation_order,
        .limit = 20,
        .include_all_fields = false,
    }));
    try db.core.store.put(navigation_summary_key, backfilled_summary);

    var identity_navigation = try db.search(alloc, .{
        .hierarchy_children = .{ .parent_id = "doc:a" },
        .return_mode = .unit,
        .order_by = &navigation_order,
        .limit = 20,
        .include_stored = false,
        .include_all_fields = false,
        .fields = &.{},
    });
    defer identity_navigation.deinit();
    try std.testing.expectEqual(@as(usize, 1), identity_navigation.hits.len);
    try std.testing.expect(identity_navigation.hits[0].stored_data == null);
    try std.testing.expectEqualStrings(
        navigation.hits[0].sort_values[0].string,
        identity_navigation.hits[0].sort_values[0].string,
    );

    var cancelled = std.atomic.Value(bool).init(true);
    try std.testing.expectError(error.Cancelled, db.search(alloc, .{
        .hierarchy_children = .{ .parent_id = "doc:a" },
        .return_mode = .unit,
        .order_by = &navigation_order,
        .limit = 20,
        .include_all_fields = false,
        .cancellation = db_mod.types.CancellationToken.fromAtomic(&cancelled),
    }));

    // The parent-owned state remains authoritative when a unit payload is on a
    // child-range shard. The local planner must retain the unit and let the
    // table coordinator hydrate the selected page through routed lookups.
    try db.core.store.delete(unit_key);
    var remote_unit_page = try db.search(alloc, .{
        .hierarchy_children = .{ .parent_id = "doc:a" },
        .defer_hierarchy_child_hydration = true,
        .return_mode = .unit,
        .order_by = &navigation_order,
        .limit = 20,
        .include_all_fields = false,
    });
    defer remote_unit_page.deinit();
    try std.testing.expectEqual(@as(u32, 1), remote_unit_page.total_hits);
    try std.testing.expectEqual(@as(usize, 1), remote_unit_page.hits.len);
    try std.testing.expect(remote_unit_page.hits[0].stored_data == null);
    try db.core.store.put(unit_key, unit_payload);

    const stale_cursor = [_]std.json.Value{
        .{ .string = navigation.hits[0].sort_values[0].string },
        .{ .string = navigation.hits[0].sort_values[1].string },
    };
    const malformed_cursor = [_]std.json.Value{
        .{ .string = "hn1/not-a-source-revision/document_units_v1/00000000000000000001/00000000000000000000/fingerprint" },
        .{ .string = navigation.hits[0].sort_values[1].string },
    };
    const legacy_cursor = [_]std.json.Value{
        .{ .string = "hn2/retired" },
        .{ .string = navigation.hits[0].sort_values[1].string },
    };
    const short_cursor = [_]std.json.Value{.{ .string = navigation.hits[0].sort_values[0].string }};
    const non_string_cursor = [_]std.json.Value{
        .{ .integer = 1 },
        .{ .string = navigation.hits[0].sort_values[1].string },
    };
    for ([_][]const std.json.Value{ &short_cursor, &non_string_cursor }) |invalid_cursor| {
        try std.testing.expectError(error.InvalidQueryRequest, db.search(alloc, .{
            .hierarchy_children = .{ .parent_id = "doc:a" },
            .return_mode = .unit,
            .order_by = &navigation_order,
            .search_after = invalid_cursor,
            .limit = 20,
            .include_all_fields = false,
        }));
    }
    for ([_]u32{ 0, db_mod.types.max_canonical_hierarchy_groups + 1 }) |invalid_limit| {
        try std.testing.expectError(error.InvalidQueryRequest, db.search(alloc, .{
            .hierarchy_children = .{ .parent_id = "doc:a" },
            .return_mode = .unit,
            .order_by = &navigation_order,
            .limit = invalid_limit,
            .include_all_fields = false,
        }));
    }
    try std.testing.expectError(error.InvalidQueryRequest, db.search(alloc, .{
        .hierarchy_children = .{ .parent_id = "doc:a" },
        .return_mode = .unit,
        .order_by = &navigation_order,
        .search_after = &malformed_cursor,
        .limit = 20,
        .include_all_fields = false,
    }));
    try std.testing.expectError(error.HierarchyCursorStale, db.search(alloc, .{
        .hierarchy_children = .{ .parent_id = "doc:a" },
        .return_mode = .unit,
        .order_by = &navigation_order,
        .search_after = &legacy_cursor,
        .limit = 20,
        .include_all_fields = false,
    }));
    try db.batch(.{
        .writes = &.{.{
            .key = "doc:a",
            .value = "{\"url\":\"data:text/plain;base64,Z2FtbWEgZGVsdGE=\"}",
        }},
        .sync_level = .enrichments,
    });
    try std.testing.expectError(error.HierarchyCursorStale, db.search(alloc, .{
        .hierarchy_children = .{ .parent_id = "doc:a" },
        .return_mode = .unit,
        .order_by = &navigation_order,
        .search_after = &stale_cursor,
        .limit = 20,
        .include_all_fields = false,
    }));

    try db.batch(.{
        .writes = &.{.{
            .key = "doc:a",
            .value = "{\"url\":\"\"}",
        }},
        .sync_level = .enrichments,
    });
    try std.testing.expectError(error.NotFound, db.core.store.get(alloc, manifest_key));
    try std.testing.expectError(error.NotFound, db.core.store.get(alloc, unit_key));
    try std.testing.expectError(error.NotFound, db.core.store.get(alloc, state_key));
    try std.testing.expectError(error.NotFound, db.core.store.get(alloc, navigation_summary_key));
    try std.testing.expectError(error.NotFound, db.core.store.get(alloc, navigation_block_key));

    // Row policy must not turn parent existence into a cursor-response oracle.
    // Exercise the same initial/valid/malformed cursor matrix below while the
    // source still exists but an internal authorization filter excludes it.
    const hidden_parent_request = db_mod.types.SearchRequest{
        .hierarchy_children = .{ .parent_id = "doc:a" },
        .return_mode = .unit,
        .order_by = &navigation_order,
        .limit = 20,
        .include_all_fields = false,
        .filter_doc_ids = &.{"doc:other"},
        .filter_doc_ids_positive = true,
    };
    var hidden_source = try db.search(alloc, hidden_parent_request);
    defer hidden_source.deinit();
    try std.testing.expectEqual(@as(u32, 0), hidden_source.total_hits);
    try std.testing.expectEqual(@as(usize, 0), hidden_source.hits.len);
    var hidden_valid_cursor = hidden_parent_request;
    hidden_valid_cursor.search_after = &stale_cursor;
    try std.testing.expectError(error.HierarchyCursorStale, db.search(alloc, hidden_valid_cursor));
    var hidden_malformed_cursor = hidden_parent_request;
    hidden_malformed_cursor.search_after = &malformed_cursor;
    try std.testing.expectError(error.InvalidQueryRequest, db.search(alloc, hidden_malformed_cursor));

    // Deleting the parent after issuing a page cursor invalidates the complete
    // hierarchy revision. Initial traversal remains an empty collection, while
    // continuation returns exactly the same cursor classifications as the
    // authorization-hidden source above.
    try db.batch(.{
        .deletes = &.{"doc:a"},
        .sync_level = .enrichments,
    });
    var missing_source = try db.search(alloc, .{
        .hierarchy_children = .{ .parent_id = "doc:a" },
        .return_mode = .unit,
        .order_by = &navigation_order,
        .limit = 20,
        .include_all_fields = false,
    });
    defer missing_source.deinit();
    try std.testing.expectEqual(@as(u32, 0), missing_source.total_hits);
    try std.testing.expectEqual(@as(usize, 0), missing_source.hits.len);
    const missing_shard_request = db_mod.types.SearchRequest{
        .hierarchy_children = .{ .parent_id = "doc:a" },
        .return_mode = .unit,
        .order_by = &navigation_order,
        .search_after = &stale_cursor,
        .limit = 20,
        .include_all_fields = false,
        .defer_hierarchy_child_hydration = true,
    };
    var missing_shard = try db.search(alloc, missing_shard_request);
    defer missing_shard.deinit();
    try std.testing.expectEqual(@as(u32, 0), missing_shard.total_hits);
    try std.testing.expectEqual(@as(usize, 0), missing_shard.hits.len);
    try std.testing.expectError(error.HierarchyCursorStale, db.search(alloc, .{
        .hierarchy_children = .{ .parent_id = "doc:a" },
        .return_mode = .unit,
        .order_by = &navigation_order,
        .search_after = &stale_cursor,
        .limit = 20,
        .include_all_fields = false,
    }));
    try std.testing.expectError(error.InvalidQueryRequest, db.search(alloc, .{
        .hierarchy_children = .{ .parent_id = "doc:a" },
        .return_mode = .unit,
        .order_by = &navigation_order,
        .search_after = &malformed_cursor,
        .limit = 20,
        .include_all_fields = false,
    }));
}

test "db hierarchy navigation seeks only the descriptor blocks needed by the page" {
    const alloc = std.testing.allocator;

    var path_tmp = try db_mod.TestDirectory.init("db");
    defer path_tmp.cleanup();
    const path = path_tmp.path().ptr;
    defer db_mod.cleanupTempDir(path);

    var db = try DB.open(alloc, std.mem.span(path), .{});
    defer db.close();
    try db.enrichment.addEnrichment(.{
        .name = "document_units_v1",
        .kind = .asset,
        .field = "url",
        .content_type = "application/json",
        .producer_json = "{\"type\":\"document_extraction\",\"config\":{}}",
    });
    try db.batch(.{
        .writes = &.{.{ .key = "doc:blocked", .value = "{\"title\":\"blocked traversal\"}" }},
        .sync_level = .write,
    });

    const descriptors = try alloc.alloc(db_mod.DocumentExtractionUnitDescriptor, db_mod.hierarchy_navigation_block_size + 1);
    var initialized: usize = 0;
    defer {
        for (descriptors[0..initialized]) |descriptor| {
            alloc.free(@constCast(descriptor.key));
            alloc.free(@constCast(descriptor.fingerprint));
        }
        alloc.free(descriptors);
    }
    for (descriptors, 0..) |*descriptor, ordinal| {
        const unit_id = try std.fmt.allocPrint(alloc, "page:{d:0>6}", .{ordinal + 1});
        defer alloc.free(unit_id);
        descriptor.* = .{
            .key = try db_mod.internal_keys.documentUnitArtifactKeyAlloc(alloc, "doc:blocked", "document_units_v1", unit_id),
            .fingerprint = try std.fmt.allocPrint(alloc, "fingerprint-{d}", .{ordinal + 1}),
        };
        initialized += 1;
    }

    const manifest_key = try db_mod.internal_keys.artifactNamedPrefixAlloc(alloc, "doc:blocked", "asset", "document_units_v1");
    defer alloc.free(manifest_key);
    try db.core.store.put(
        manifest_key,
        "{\"artifact_type\":\"document_units\",\"generation\":1," ++
            "\"merge_plan\":{\"from_generation\":0,\"to_generation\":1,\"status\":\"converged\"}}",
    );
    const digest = try db_mod.hierarchyNavigationArtifactDigestAlloc(alloc, descriptors);
    defer alloc.free(digest);
    const summary_key = try db_mod.internal_keys.documentUnitNavigationSummaryKeyAlloc(alloc, "doc:blocked", "document_units_v1");
    defer alloc.free(summary_key);
    const summary = try db_mod.hierarchyNavigationSummaryValueAlloc(alloc, 1, digest, @intCast(descriptors.len), 2);
    defer alloc.free(summary);
    try db.core.store.put(summary_key, summary);

    const first_block_key = try db_mod.internal_keys.documentUnitNavigationBlockKeyAlloc(alloc, "doc:blocked", "document_units_v1", 0);
    defer alloc.free(first_block_key);
    const first_block = try db_mod.hierarchyNavigationBlockValueAlloc(alloc, 0, descriptors[0..db_mod.hierarchy_navigation_block_size]);
    defer alloc.free(first_block);
    try db.core.store.put(first_block_key, first_block);
    try db.core.store.put(descriptors[0].key, "{\"_artifact_unit_fingerprint\":\"fingerprint-1\"}");
    const second_block_key = try db_mod.internal_keys.documentUnitNavigationBlockKeyAlloc(alloc, "doc:blocked", "document_units_v1", 1);
    defer alloc.free(second_block_key);
    // A corrupt later block is intentional: the first page must not touch it.
    try db.core.store.put(second_block_key, "{}");

    const order = [_]db_mod.types.SortField{
        .{ .field = "_hierarchy.position" },
        .{ .field = "_id" },
    };
    var first_page = try db.search(alloc, .{
        .hierarchy_children = .{ .parent_id = "doc:blocked" },
        .return_mode = .unit,
        .order_by = &order,
        .limit = 1,
        .include_all_fields = false,
        .fields = &.{},
    });
    defer first_page.deinit();
    try std.testing.expectEqual(@as(u32, db_mod.hierarchy_navigation_block_size + 1), first_page.total_hits);
    try std.testing.expectEqual(@as(usize, 1), first_page.hits.len);
    try std.testing.expectEqualStrings("page:000001", first_page.hits[0].artifact_ref.?.unit_id.?);

    try std.testing.expectError(error.InvalidQueryRequest, db.search(alloc, .{
        .hierarchy_children = .{ .parent_id = "doc:blocked" },
        .return_mode = .unit,
        .order_by = &order,
        .offset = db_mod.hierarchy_navigation_block_size,
        .limit = 1,
        .include_all_fields = false,
        .fields = &.{},
    }));

    try db.core.store.delete(descriptors[0].key);
    try std.testing.expectError(error.InvalidDocumentExtractionState, db.search(alloc, .{
        .hierarchy_children = .{ .parent_id = "doc:blocked" },
        .return_mode = .unit,
        .order_by = &order,
        .limit = 1,
        .include_all_fields = false,
        .fields = &.{},
    }));
}

test "db hierarchy cursor binds every unit artifact revision under its source" {
    const alloc = std.testing.allocator;

    var path_tmp = try db_mod.TestDirectory.init("db");
    defer path_tmp.cleanup();
    const path = path_tmp.path().ptr;
    defer db_mod.cleanupTempDir(path);

    var fake = db_mod.TestAssetProducer{};
    var db = try DB.open(alloc, std.mem.span(path), .{
        .enrichment = .{
            .owner_id = "worker-a",
            .asset_producer = fake.producer(),
        },
    });
    defer db.close();

    for ([_][]const u8{ "a/units_v1", "z_units_v1" }) |name| {
        try db.enrichment.addEnrichment(.{
            .name = name,
            .kind = .asset,
            .field = "url",
            .content_type = "application/json",
            .producer_json = "{\"type\":\"document_extraction\",\"config\":{}}",
        });
    }
    try db.batch(.{
        .writes = &.{.{
            .key = "doc:a",
            .value = "{\"url\":\"data:text/plain;base64,YWxwaGE=\"}",
        }},
        .sync_level = .enrichments,
    });

    const order = [_]db_mod.types.SortField{
        .{ .field = "_hierarchy.position" },
        .{ .field = "_id" },
    };
    var first_page = try db.search(alloc, .{
        .hierarchy_children = .{ .parent_id = "doc:a" },
        .return_mode = .unit,
        .order_by = &order,
        .limit = 1,
        .include_all_fields = false,
    });
    defer first_page.deinit();
    try std.testing.expectEqual(@as(u32, 2), first_page.total_hits);
    try std.testing.expectEqualStrings("a/units_v1", first_page.hits[0].artifact_ref.?.name);
    const cursor = [_]std.json.Value{
        .{ .string = first_page.hits[0].sort_values[0].string },
        .{ .string = first_page.hits[0].sort_values[1].string },
    };

    var replay = try db.search(alloc, .{
        .hierarchy_children = .{ .parent_id = "doc:a" },
        .return_mode = .unit,
        .order_by = &order,
        .search_after = &cursor,
        .limit = 1,
        .include_all_fields = false,
    });
    defer replay.deinit();
    try std.testing.expectEqual(@as(usize, 1), replay.hits.len);
    try std.testing.expectEqualStrings("z_units_v1", replay.hits[0].artifact_ref.?.name);

    // Reprocess only the later-sorting sibling artifact. A cursor located in
    // the unchanged first artifact must still be rejected because traversal is
    // bound to the complete source hierarchy revision.
    try std.testing.expect(try db.reprocessDocumentArtifact(alloc, "doc:a", "z_units_v1"));
    try std.testing.expectError(error.HierarchyCursorStale, db.search(alloc, .{
        .hierarchy_children = .{ .parent_id = "doc:a" },
        .return_mode = .unit,
        .order_by = &order,
        .search_after = &cursor,
        .limit = 1,
        .include_all_fields = false,
    }));

    const corrupt_manifest_key = try db_mod.internal_keys.artifactNamedPrefixAlloc(alloc, "doc:a", "asset", "z_units_v1");
    defer alloc.free(corrupt_manifest_key);
    try db.core.store.put(corrupt_manifest_key, "{\"artifact_type\":\"document_units\"}");
    try std.testing.expectError(error.InvalidDocumentExtractionManifest, db.search(alloc, .{
        .hierarchy_children = .{ .parent_id = "doc:a" },
        .return_mode = .unit,
        .order_by = &order,
        .limit = 1,
        .include_all_fields = false,
    }));
}

test "db async document extraction accounts resource manager working set" {
    const alloc = std.testing.allocator;

    var path_tmp = try db_mod.TestDirectory.init("db");
    defer path_tmp.cleanup();
    const path = path_tmp.path().ptr;
    defer db_mod.cleanupTempDir(path);

    var resource_manager = db_mod.resource_manager_mod.ResourceManager.init(.{});
    var db = try DB.open(alloc, std.mem.span(path), .{
        .resource_manager = &resource_manager,
        .enrichment = .{
            .owner_id = "worker-a",
            .enable_without_producers = true,
        },
    });
    defer db.close();

    try db.enrichment.addEnrichment(.{
        .name = "document_units_v1",
        .kind = .asset,
        .field = "url",
        .content_type = "application/json",
        .producer_json = "{\"type\":\"document_extraction\",\"config\":{}}",
    });

    try db.batch(.{
        .writes = &.{.{
            .key = "doc:a",
            .value = "{\"url\":\"data:text/plain;base64,YWxwaGEgYmV0YQ==\"}",
        }},
        .sync_level = .write,
    });
    try db.runUntilIdle();

    const manifest_key = try db_mod.internal_keys.artifactNamedPrefixAlloc(alloc, "doc:a", "asset", "document_units_v1");
    defer alloc.free(manifest_key);
    const manifest = try db.core.store.get(alloc, manifest_key);
    defer alloc.free(manifest);
    try std.testing.expect(std.mem.indexOf(u8, manifest, "\"status\":\"converged\"") != null);

    const stats = resource_manager.snapshot().slices[@backingInt(db_mod.resource_manager_mod.Slice.document_extraction_working_set)];
    try std.testing.expect(stats.peak_bytes > 0);
    try std.testing.expectEqual(@as(u64, 0), stats.used_bytes);
}

test "db async document extraction deletes artifacts with corrupt previous extraction state" {
    const alloc = std.testing.allocator;

    var path_tmp = try db_mod.TestDirectory.init("db");
    defer path_tmp.cleanup();
    const path = path_tmp.path().ptr;
    defer db_mod.cleanupTempDir(path);

    var db = try DB.open(alloc, std.mem.span(path), .{
        .enrichment = .{
            .owner_id = "worker-a",
            .enable_without_producers = true,
        },
    });
    defer db.close();

    try db.enrichment.addEnrichment(.{
        .name = "document_units_v1",
        .kind = .asset,
        .field = "url",
        .content_type = "application/json",
        .producer_json = "{\"type\":\"document_extraction\",\"config\":{}}",
    });
    try db.enrichment.addEnrichment(.{
        .name = "document_chunks_v1",
        .kind = .chunk,
        .field = "text",
        .source_artifact_name = "document_units_v1",
        .chunk_size = 16,
        .chunk_overlap = 0,
        .full_text_index = true,
    });
    try db.addIndex(.{
        .name = "ft_document_chunks",
        .kind = .full_text,
        .config_json = "{\"chunk_name\":\"document_chunks_v1\"}",
    });

    try db.batch(.{
        .writes = &.{.{
            .key = "doc:async-delete",
            .value = "{\"url\":\"data:text/plain;base64,YWxwaGEgYmV0YQ==\"}",
        }},
        .sync_level = .write,
    });
    try db.runUntilIdle();

    const manifest_key = try db_mod.internal_keys.artifactNamedPrefixAlloc(alloc, "doc:async-delete", "asset", "document_units_v1");
    defer alloc.free(manifest_key);
    const unit_key = try db_mod.internal_keys.documentUnitArtifactKeyAlloc(alloc, "doc:async-delete", "document_units_v1", "document:000001");
    defer alloc.free(unit_key);
    const chunk_key = try db_mod.internal_keys.documentUnitChunkArtifactKeyAlloc(alloc, "doc:async-delete", "document_chunks_v1", "document:000001", 0);
    defer alloc.free(chunk_key);
    const state_key = try db_mod.assetStateKeyAlloc(alloc, "doc:async-delete", "document_units_v1");
    defer alloc.free(state_key);

    const initial_unit_payload = try db.core.store.get(alloc, unit_key);
    alloc.free(initial_unit_payload);
    const initial_chunk_payload = try db.core.store.get(alloc, chunk_key);
    alloc.free(initial_chunk_payload);
    var before_delete = try db.search(alloc, .{
        .index_name = "ft_document_chunks",
        .full_text = .{ .match = .{ .field = "text", .text = "alpha" } },
        .return_mode = .chunk,
    });
    defer before_delete.deinit();
    try std.testing.expect(before_delete.total_hits > 0);
    try db.core.store.put(state_key, "{");

    try db.batch(.{
        .writes = &.{.{
            .key = "doc:async-delete",
            .value = "{\"url\":\"\"}",
        }},
        .sync_level = .write,
    });
    try db.runUntilIdle();

    try std.testing.expectError(error.NotFound, db.core.store.get(alloc, manifest_key));
    try std.testing.expectError(error.NotFound, db.core.store.get(alloc, unit_key));
    try std.testing.expectError(error.NotFound, db.core.store.get(alloc, chunk_key));
    try std.testing.expectError(error.NotFound, db.core.store.get(alloc, state_key));
    var after_delete = try db.search(alloc, .{
        .index_name = "ft_document_chunks",
        .full_text = .{ .match = .{ .field = "text", .text = "alpha" } },
        .return_mode = .chunk,
    });
    defer after_delete.deinit();
    try std.testing.expectEqual(@as(u32, 0), after_delete.total_hits);
}

test "db document extraction routes mixed files using source metadata fields" {
    const alloc = std.testing.allocator;

    var path_tmp = try db_mod.TestDirectory.init("db");
    defer path_tmp.cleanup();
    const path = path_tmp.path().ptr;
    defer db_mod.cleanupTempDir(path);

    var db = try DB.open(alloc, std.mem.span(path), .{});
    defer db.close();

    try db.enrichment.addEnrichment(.{
        .name = "document_units_v1",
        .kind = .asset,
        .field = "url",
        .content_type = "application/json",
        .producer_json =
        \\{"type":"document_extraction","config":{"source":{"filename_field":"filename"},"routes":[{"match":{"extension":["md"]},"extractor":{"type":"text","unit":"note"}}]}}
        ,
    });

    try db.batch(.{
        .writes = &.{.{
            .key = "doc:a",
            .value = "{\"url\":\"data:application/octet-stream;base64,YWxwaGEgYmV0YQ==\",\"filename\":\"notes.md\"}",
        }},
        .sync_level = .enrichments,
    });

    const manifest_key = try db_mod.internal_keys.artifactNamedPrefixAlloc(alloc, "doc:a", "asset", "document_units_v1");
    defer alloc.free(manifest_key);
    const unit_key = try db_mod.internal_keys.documentUnitArtifactKeyAlloc(alloc, "doc:a", "document_units_v1", "note:000001");
    defer alloc.free(unit_key);

    const manifest = try db.core.store.get(alloc, manifest_key);
    defer alloc.free(manifest);
    try std.testing.expect(std.mem.indexOf(u8, manifest, "\"route_type\":\"text\"") != null);

    const unit_payload = try db.core.store.get(alloc, unit_key);
    defer alloc.free(unit_payload);
    try std.testing.expect(std.mem.indexOf(u8, unit_payload, "\"unit_id\":\"note:000001\"") != null);
    try std.testing.expect(std.mem.indexOf(u8, unit_payload, "\"unit_type\":\"note\"") != null);
    try std.testing.expect(std.mem.indexOf(u8, unit_payload, "\"text\":\"alpha beta\"") != null);
}

test "db document extraction stores docx section units" {
    const alloc = std.testing.allocator;

    var path_tmp = try db_mod.TestDirectory.init("db");
    defer path_tmp.cleanup();
    const path = path_tmp.path().ptr;
    defer db_mod.cleanupTempDir(path);

    var db = try DB.open(alloc, std.mem.span(path), .{
        .start_index_workers = false,
        .ttl_cleanup = .{ .enabled = false },
    });
    defer db.close();

    try db.enrichment.addEnrichment(.{
        .name = "document_units_v1",
        .kind = .asset,
        .field = "url",
        .content_type = "application/json",
        .producer_json = "{\"type\":\"document_extraction\",\"config\":{\"source\":{\"filename_field\":\"filename\",\"content_type_field\":\"mime_type\"}}}",
    });

    try db.batch(.{
        .writes = &.{.{
            .key = "doc:docx",
            .value = "{\"filename\":\"report.docx\",\"mime_type\":\"application/vnd.openxmlformats-officedocument.wordprocessingml.document\",\"url\":\"data:application/vnd.openxmlformats-officedocument.wordprocessingml.document;base64,UEsDBBQAAAAAAAAAAABUVz0vhwAAAIcAAAARAAAAd29yZC9kb2N1bWVudC54bWw8dzpkb2N1bWVudCB4bWxuczp3PSJ3Ij48dzpib2R5Pjx3OnA+PHc6cj48dzp0PkFscGhhIERCPC93OnQ+PC93OnI+PC93OnA+PHc6cD48dzpyPjx3OnQ+QmV0YSBEQjwvdzp0PjwvdzpyPjwvdzpwPjwvdzpib2R5Pjwvdzpkb2N1bWVudD5QSwECFAAUAAAAAAAAAAAAVFc9L4cAAACHAAAAEQAAAAAAAAAAAAAAAAAAAAAAd29yZC9kb2N1bWVudC54bWxQSwUGAAAAAAEAAQA/AAAAtgAAAAAA\"}",
        }},
        .sync_level = .enrichments,
    });

    const manifest_key = try db_mod.internal_keys.artifactNamedPrefixAlloc(alloc, "doc:docx", "asset", "document_units_v1");
    defer alloc.free(manifest_key);
    const section_key = try db_mod.internal_keys.documentUnitArtifactKeyAlloc(alloc, "doc:docx", "document_units_v1", "section:000001");
    defer alloc.free(section_key);

    const manifest = try db.core.store.get(alloc, manifest_key);
    defer alloc.free(manifest);
    try std.testing.expect(std.mem.indexOf(u8, manifest, "\"route_type\":\"docx\"") != null);
    try std.testing.expect(std.mem.indexOf(u8, manifest, "\"unit_count\":1") != null);

    const section_payload = try db.core.store.get(alloc, section_key);
    defer alloc.free(section_payload);
    try std.testing.expect(std.mem.indexOf(u8, section_payload, "\"unit_type\":\"section\"") != null);
    try std.testing.expect(std.mem.indexOf(u8, section_payload, "\"method\":\"docx_text\"") != null);
    try std.testing.expect(std.mem.indexOf(u8, section_payload, "\"text\":\"Alpha DB\\nBeta DB\"") != null);
    try std.testing.expect(std.mem.indexOf(u8, section_payload, "\"source_content_type\":\"application/vnd.openxmlformats-officedocument.wordprocessingml.document\"") != null);
}

test "db document extraction stores zip archive entry units" {
    const alloc = std.testing.allocator;

    var path_tmp = try db_mod.TestDirectory.init("db");
    defer path_tmp.cleanup();
    const path = path_tmp.path().ptr;
    defer db_mod.cleanupTempDir(path);

    var db = try DB.open(alloc, std.mem.span(path), .{
        .start_index_workers = false,
        .ttl_cleanup = .{ .enabled = false },
    });
    defer db.close();

    try db.enrichment.addEnrichment(.{
        .name = "document_units_v1",
        .kind = .asset,
        .field = "url",
        .content_type = "application/json",
        .producer_json = "{\"type\":\"document_extraction\",\"config\":{\"source\":{\"filename_field\":\"filename\",\"content_type_field\":\"mime_type\"}}}",
    });

    try db.batch(.{
        .writes = &.{.{
            .key = "doc:archive",
            .value = "{\"filename\":\"bundle.zip\",\"mime_type\":\"application/zip\",\"url\":\"data:application/zip;base64,UEsDBBQAAAAAAAAAAADxLiMkDwAAAA8AAAAPAAAAZG9jcy9yZWFkbWUudHh0QXJjaGl2ZSBEQiB0ZXh0UEsBAhQAFAAAAAAAAAAAAPEuIyQPAAAADwAAAA8AAAAAAAAAAAAAAAAAAAAAAGRvY3MvcmVhZG1lLnR4dFBLBQYAAAAAAQABAD0AAAA8AAAAAAA=\"}",
        }},
        .sync_level = .enrichments,
    });

    const manifest_key = try db_mod.internal_keys.artifactNamedPrefixAlloc(alloc, "doc:archive", "asset", "document_units_v1");
    defer alloc.free(manifest_key);
    const entry_key = try db_mod.internal_keys.documentUnitArtifactKeyAlloc(alloc, "doc:archive", "document_units_v1", "archive:entry:000001");
    defer alloc.free(entry_key);

    const manifest = try db.core.store.get(alloc, manifest_key);
    defer alloc.free(manifest);
    try std.testing.expect(std.mem.indexOf(u8, manifest, "\"route_type\":\"archive\"") != null);
    try std.testing.expect(std.mem.indexOf(u8, manifest, "\"unit_count\":1") != null);

    const entry_payload = try db.core.store.get(alloc, entry_key);
    defer alloc.free(entry_payload);
    try std.testing.expect(std.mem.indexOf(u8, entry_payload, "\"unit_type\":\"archive_entry\"") != null);
    try std.testing.expect(std.mem.indexOf(u8, entry_payload, "\"method\":\"zip_text\"") != null);
    try std.testing.expect(std.mem.indexOf(u8, entry_payload, "\"source_path\":\"docs/readme.txt\"") != null);
    try std.testing.expect(std.mem.indexOf(u8, entry_payload, "\"text\":\"Archive DB text\"") != null);
    try std.testing.expect(std.mem.indexOf(u8, entry_payload, "\"source_content_type\":\"application/zip\"") != null);
}

test "db document extraction marks image OCR not configured without a reader" {
    const alloc = std.testing.allocator;

    var path_tmp = try db_mod.TestDirectory.init("db");
    defer path_tmp.cleanup();
    const path = path_tmp.path().ptr;
    defer db_mod.cleanupTempDir(path);

    var db = try DB.open(alloc, std.mem.span(path), .{
        .start_index_workers = false,
        .ttl_cleanup = .{ .enabled = false },
    });
    defer db.close();

    try db.enrichment.addEnrichment(.{
        .name = "document_units_v1",
        .kind = .asset,
        .field = "url",
        .content_type = "application/json",
        .producer_json = "{\"type\":\"document_extraction\",\"config\":{\"source\":{\"filename_field\":\"filename\",\"content_type_field\":\"mime_type\"}}}",
    });

    try db.batch(.{
        .writes = &.{.{
            .key = "doc:image",
            .value = "{\"filename\":\"scan.png\",\"mime_type\":\"image/png\",\"url\":\"data:image/png;base64,iVBORw0KGgppbWFnZSBieXRlcw==\"}",
        }},
        .sync_level = .enrichments,
    });

    const manifest_key = try db_mod.internal_keys.artifactNamedPrefixAlloc(alloc, "doc:image", "asset", "document_units_v1");
    defer alloc.free(manifest_key);
    const unit_key = try db_mod.internal_keys.documentUnitArtifactKeyAlloc(alloc, "doc:image", "document_units_v1", "image:000001");
    defer alloc.free(unit_key);

    const manifest = try db.core.store.get(alloc, manifest_key);
    defer alloc.free(manifest);
    try std.testing.expect(std.mem.indexOf(u8, manifest, "\"route_type\":\"image\"") != null);
    try std.testing.expect(std.mem.indexOf(u8, manifest, "\"unit_count\":1") != null);

    const unit_payload = try db.core.store.get(alloc, unit_key);
    defer alloc.free(unit_payload);
    try std.testing.expect(std.mem.indexOf(u8, unit_payload, "\"unit_type\":\"image\"") != null);
    try std.testing.expect(std.mem.indexOf(u8, unit_payload, "\"method\":\"ocr_pending\"") != null);
    try std.testing.expect(std.mem.indexOf(u8, unit_payload, "\"extraction_status\":\"ocr_not_configured\"") != null);
    try std.testing.expect(std.mem.indexOf(u8, unit_payload, "\"byte_length\":19") != null);
    try std.testing.expect(std.mem.indexOf(u8, unit_payload, "\"source_sha256\"") != null);
    try std.testing.expect(std.mem.indexOf(u8, unit_payload, "\"ocr_used\":false") != null);
    try std.testing.expect(std.mem.indexOf(u8, unit_payload, "\"transcript_used\":false") != null);
    try std.testing.expect(std.mem.indexOf(u8, unit_payload, "\"source_content_type\":\"image/png\"") != null);
}

test "db document extraction completes image OCR with reader producer" {
    const alloc = std.testing.allocator;

    var path_tmp = try db_mod.TestDirectory.init("db");
    defer path_tmp.cleanup();
    const path = path_tmp.path().ptr;
    defer db_mod.cleanupTempDir(path);

    var fake = db_mod.TestAssetProducer{};
    var db = try DB.open(alloc, std.mem.span(path), .{
        .start_index_workers = false,
        .ttl_cleanup = .{ .enabled = false },
        .enrichment = .{
            .owner_id = "worker-a",
            .asset_producer = fake.producer(),
        },
    });
    defer db.close();

    try db.enrichment.addEnrichment(.{
        .name = "document_units_v1",
        .kind = .asset,
        .field = "url",
        .content_type = "application/json",
        .producer_json = "{\"type\":\"document_extraction\",\"config\":{\"source\":{\"filename_field\":\"filename\",\"content_type_field\":\"mime_type\"},\"ocr\":{\"reader\":{\"provider\":\"antfly\",\"model\":\"test-reader\"}}}}",
    });

    try db.batch(.{
        .writes = &.{.{
            .key = "doc:image-ocr",
            .value = "{\"filename\":\"scan.png\",\"mime_type\":\"image/png\",\"url\":\"data:image/png;base64,iVBORw0KGgppbWFnZSBieXRlcw==\"}",
        }},
        .sync_level = .enrichments,
    });

    try std.testing.expectEqual(@as(usize, 1), fake.calls);
    try std.testing.expectEqual(@as(usize, 1), fake.reader_calls);

    const unit_key = try db_mod.internal_keys.documentUnitArtifactKeyAlloc(alloc, "doc:image-ocr", "document_units_v1", "image:000001");
    defer alloc.free(unit_key);
    const unit_payload = try db.core.store.get(alloc, unit_key);
    defer alloc.free(unit_payload);
    try std.testing.expect(std.mem.indexOf(u8, unit_payload, "\"method\":\"ocr_text\"") != null);
    try std.testing.expect(std.mem.indexOf(u8, unit_payload, "\"extraction_status\":\"completed\"") != null);
    try std.testing.expect(std.mem.indexOf(u8, unit_payload, "\"ocr_used\":true") != null);
    try std.testing.expect(std.mem.indexOf(u8, unit_payload, "\"text\":\"reader:data:image/png;base64,iVBORw0KGgppbWFnZSBieXRlcw==\"") != null);
}

test "db document extraction attempts forced OCR for a scanned PDF" {
    // The real PDF implementation is covered by lib-pdf-test and the
    // production-binary PDF/OCR E2E suite. The aggregate Antfly test root uses
    // the lightweight PDF stub to stay below constrained CI memory limits.
    if (!db_mod.document_extraction_mod.pdf_runtime_available) return error.SkipZigTest;

    const alloc = std.testing.allocator;

    var path_tmp = try db_mod.TestDirectory.init("db");
    defer path_tmp.cleanup();
    const path = path_tmp.path().ptr;
    defer db_mod.cleanupTempDir(path);

    var fake = db_mod.TestAssetProducer{ .reader_output = "[{\"text\":\"scanned PDF transcription with enough text to remain selected\"}]" };
    var db = try DB.open(alloc, std.mem.span(path), .{
        .start_index_workers = false,
        .ttl_cleanup = .{ .enabled = false },
        .enrichment = .{
            .owner_id = "worker-a",
            .asset_producer = fake.producer(),
        },
    });
    defer db.close();

    try db.enrichment.addEnrichment(.{
        .name = "document_units_v1",
        .kind = .asset,
        .field = "url",
        .content_type = "application/json",
        .producer_json = "{\"type\":\"document_extraction\",\"config\":{\"source\":{\"filename_field\":\"filename\",\"content_type_field\":\"mime_type\"},\"ocr\":{\"mode\":\"always\",\"reader\":{\"provider\":\"antfly\",\"model\":\"test-reader\"}}}}",
    });

    const pdf_bytes = try std.Io.Dir.cwd().readFileAlloc(
        std.testing.io,
        "lib/pdf/testdata/scanned_table_fixture.pdf",
        alloc,
        .limited(4 * 1024 * 1024),
    );
    defer alloc.free(pdf_bytes);
    const encoded_len = std.base64.standard.Encoder.calcSize(pdf_bytes.len);
    const encoded = try alloc.alloc(u8, encoded_len);
    defer alloc.free(encoded);
    _ = std.base64.standard.Encoder.encode(encoded, pdf_bytes);
    const value = try std.fmt.allocPrint(
        alloc,
        "{{\"filename\":\"scan.pdf\",\"mime_type\":\"application/pdf\",\"url\":\"data:application/pdf;base64,{s}\"}}",
        .{encoded},
    );
    defer alloc.free(value);

    try db.batch(.{
        .writes = &.{.{ .key = "doc:pdf-ocr", .value = value }},
        .sync_level = .enrichments,
    });

    try std.testing.expectEqual(@as(usize, 1), fake.reader_calls);
    const unit_key = try db_mod.internal_keys.documentUnitArtifactKeyAlloc(alloc, "doc:pdf-ocr", "document_units_v1", "page:000001");
    defer alloc.free(unit_key);
    const unit_payload = try db.core.store.get(alloc, unit_key);
    defer alloc.free(unit_payload);
    try std.testing.expect(std.mem.indexOf(u8, unit_payload, "\"ocr_attempted\":true") != null);
    try std.testing.expect(std.mem.indexOf(u8, unit_payload, "\"ocr_used\":true") != null);
    try std.testing.expect(std.mem.indexOf(u8, unit_payload, "\"ocr_render_dpi\":150") != null);
    try std.testing.expect(std.mem.indexOf(u8, unit_payload, "scanned PDF transcription") != null);

    const manifest_key = try db_mod.internal_keys.artifactNamedPrefixAlloc(alloc, "doc:pdf-ocr", "asset", "document_units_v1");
    defer alloc.free(manifest_key);
    const manifest = try db.core.store.get(alloc, manifest_key);
    defer alloc.free(manifest);
    try std.testing.expect(std.mem.indexOf(u8, manifest, "\"ocr_attempted_count\":1") != null);
    try std.testing.expect(std.mem.indexOf(u8, manifest, "\"ocr_selected_count\":1") != null);
    try std.testing.expect(std.mem.indexOf(u8, manifest, "\"ocr_failed_count\":0") != null);
}

test "db async document extraction reuses generated OCR text across streaming passes" {
    const alloc = std.testing.allocator;

    var path_tmp = try db_mod.TestDirectory.init("db");
    defer path_tmp.cleanup();
    const path = path_tmp.path().ptr;
    defer db_mod.cleanupTempDir(path);

    var fake = db_mod.TestAssetProducer{};
    var db = try DB.open(alloc, std.mem.span(path), .{
        .ttl_cleanup = .{ .enabled = false },
        .enrichment = .{
            .owner_id = "worker-a",
            .asset_producer = fake.producer(),
        },
    });
    defer db.close();

    try db.enrichment.addEnrichment(.{
        .name = "document_units_v1",
        .kind = .asset,
        .field = "url",
        .content_type = "application/json",
        .producer_json = "{\"type\":\"document_extraction\",\"config\":{\"source\":{\"filename_field\":\"filename\",\"content_type_field\":\"mime_type\"},\"ocr\":{\"reader\":{\"provider\":\"antfly\",\"model\":\"test-reader\"}}}}",
    });

    try db.batch(.{
        .writes = &.{.{
            .key = "doc:image-ocr-async",
            .value = "{\"filename\":\"scan.png\",\"mime_type\":\"image/png\",\"url\":\"data:image/png;base64,iVBORw0KGgppbWFnZSBieXRlcw==\"}",
        }},
        .sync_level = .write,
    });
    try db.runUntilIdle();

    try std.testing.expectEqual(@as(usize, 1), fake.calls);
    try std.testing.expectEqual(@as(usize, 1), fake.reader_calls);

    const unit_key = try db_mod.internal_keys.documentUnitArtifactKeyAlloc(alloc, "doc:image-ocr-async", "document_units_v1", "image:000001");
    defer alloc.free(unit_key);
    const unit_payload = try db.core.store.get(alloc, unit_key);
    defer alloc.free(unit_payload);
    try std.testing.expect(std.mem.indexOf(u8, unit_payload, "\"method\":\"ocr_text\"") != null);
    try std.testing.expect(std.mem.indexOf(u8, unit_payload, "\"extraction_status\":\"completed\"") != null);
    try std.testing.expect(std.mem.indexOf(u8, unit_payload, "\"text\":\"reader:data:image/png;base64,iVBORw0KGgppbWFnZSBieXRlcw==\"") != null);
}

test "db document extraction stores structured OCR confidence and coordinates" {
    const alloc = std.testing.allocator;

    var path_tmp = try db_mod.TestDirectory.init("db");
    defer path_tmp.cleanup();
    const path = path_tmp.path().ptr;
    defer db_mod.cleanupTempDir(path);

    var fake = db_mod.TestAssetProducer{
        .reader_output = "[{\"text\":\"invoice total\",\"confidence\":0.92,\"bbox\":[1,2,101,42],\"warning\":\"low contrast\"}]",
    };
    var db = try DB.open(alloc, std.mem.span(path), .{
        .start_index_workers = false,
        .ttl_cleanup = .{ .enabled = false },
        .enrichment = .{
            .owner_id = "worker-a",
            .asset_producer = fake.producer(),
        },
    });
    defer db.close();

    try db.enrichment.addEnrichment(.{
        .name = "document_units_v1",
        .kind = .asset,
        .field = "url",
        .content_type = "application/json",
        .producer_json = "{\"type\":\"document_extraction\",\"config\":{\"source\":{\"filename_field\":\"filename\",\"content_type_field\":\"mime_type\"},\"ocr\":{\"reader\":{\"provider\":\"antfly\",\"model\":\"test-reader\"}}}}",
    });

    try db.batch(.{
        .writes = &.{.{
            .key = "doc:image-ocr-structured",
            .value = "{\"filename\":\"scan.png\",\"mime_type\":\"image/png\",\"url\":\"data:image/png;base64,iVBORw0KGgppbWFnZSBieXRlcw==\"}",
        }},
        .sync_level = .enrichments,
    });

    const unit_key = try db_mod.internal_keys.documentUnitArtifactKeyAlloc(alloc, "doc:image-ocr-structured", "document_units_v1", "image:000001");
    defer alloc.free(unit_key);
    const unit_payload = try db.core.store.get(alloc, unit_key);
    defer alloc.free(unit_payload);
    var parsed = try std.json.parseFromSlice(std.json.Value, alloc, unit_payload, .{});
    defer parsed.deinit();
    try std.testing.expectEqualStrings("invoice total", parsed.value.object.get("text").?.string);
    try std.testing.expectApproxEqAbs(@as(f64, 0.92), parsed.value.object.get("confidence").?.float, 0.0001);
    try std.testing.expectApproxEqAbs(@as(f64, 0.92), parsed.value.object.get("ocr_confidence").?.float, 0.0001);
    const bbox = parsed.value.object.get("ocr_bbox").?.array.items;
    try std.testing.expectEqual(@as(usize, 4), bbox.len);
    try std.testing.expectApproxEqAbs(@as(f64, 1), db_mod.jsonTestNumber(bbox[0]), 0.0001);
    try std.testing.expectApproxEqAbs(@as(f64, 42), db_mod.jsonTestNumber(bbox[3]), 0.0001);
    try std.testing.expectEqualStrings("low contrast", parsed.value.object.get("extraction_warning").?.string);
    const provenance = parsed.value.object.get("provenance").?.object;
    try std.testing.expectApproxEqAbs(@as(f64, 0.92), provenance.get("confidence").?.float, 0.0001);
    try std.testing.expectApproxEqAbs(@as(f64, 0.92), provenance.get("ocr_confidence").?.float, 0.0001);
    try std.testing.expectEqualStrings("low contrast", provenance.get("extraction_warning").?.string);
}

test "db document extraction completes audio transcription with transcriber producer" {
    const alloc = std.testing.allocator;

    var path_tmp = try db_mod.TestDirectory.init("db");
    defer path_tmp.cleanup();
    const path = path_tmp.path().ptr;
    defer db_mod.cleanupTempDir(path);

    var fake = db_mod.TestAssetProducer{
        .transcriber_output = "{\"text\":\"spoken words\",\"confidence\":0.81}",
    };
    var db = try DB.open(alloc, std.mem.span(path), .{
        .start_index_workers = false,
        .ttl_cleanup = .{ .enabled = false },
        .enrichment = .{
            .owner_id = "worker-a",
            .asset_producer = fake.producer(),
        },
    });
    defer db.close();

    try db.enrichment.addEnrichment(.{
        .name = "document_units_v1",
        .kind = .asset,
        .field = "url",
        .content_type = "application/json",
        .producer_json = "{\"type\":\"document_extraction\",\"config\":{\"source\":{\"filename_field\":\"filename\",\"content_type_field\":\"mime_type\"},\"transcription\":{\"transcriber\":{\"provider\":\"mock-transcriber\",\"model\":\"test-transcriber\"}}}}",
    });

    try db.batch(.{
        .writes = &.{.{
            .key = "doc:audio-transcript",
            .value = "{\"filename\":\"audio.mp3\",\"mime_type\":\"audio/mpeg\",\"url\":\"data:audio/mpeg;base64,SUQzYXVkaW8gYnl0ZXM=\"}",
        }},
        .sync_level = .enrichments,
    });

    try std.testing.expectEqual(@as(usize, 1), fake.calls);
    try std.testing.expectEqual(@as(usize, 1), fake.transcriber_calls);

    const unit_key = try db_mod.internal_keys.documentUnitArtifactKeyAlloc(alloc, "doc:audio-transcript", "document_units_v1", "audio:000001");
    defer alloc.free(unit_key);
    const unit_payload = try db.core.store.get(alloc, unit_key);
    defer alloc.free(unit_payload);
    try std.testing.expect(std.mem.indexOf(u8, unit_payload, "\"method\":\"transcript_text\"") != null);
    try std.testing.expect(std.mem.indexOf(u8, unit_payload, "\"extraction_status\":\"completed\"") != null);
    try std.testing.expect(std.mem.indexOf(u8, unit_payload, "\"transcript_used\":true") != null);
    var parsed = try std.json.parseFromSlice(std.json.Value, alloc, unit_payload, .{});
    defer parsed.deinit();
    try std.testing.expectEqualStrings("spoken words", parsed.value.object.get("text").?.string);
    try std.testing.expectApproxEqAbs(@as(f64, 0.81), parsed.value.object.get("confidence").?.float, 0.0001);
    try std.testing.expectApproxEqAbs(@as(f64, 0.81), parsed.value.object.get("transcript_confidence").?.float, 0.0001);
    const provenance = parsed.value.object.get("provenance").?.object;
    try std.testing.expectApproxEqAbs(@as(f64, 0.81), provenance.get("confidence").?.float, 0.0001);
    try std.testing.expectApproxEqAbs(@as(f64, 0.81), provenance.get("transcript_confidence").?.float, 0.0001);
}

test "db document extraction transcript segments time chunk artifacts" {
    const alloc = std.testing.allocator;

    var path_tmp = try db_mod.TestDirectory.init("db");
    defer path_tmp.cleanup();
    const path = path_tmp.path().ptr;
    defer db_mod.cleanupTempDir(path);

    var fake = db_mod.TestAssetProducer{
        .transcriber_output = "{\"text\":\"first phrase here. second phrase there.\",\"confidence\":0.9,\"duration_ms\":2500,\"segments\":[{\"text\":\"first phrase here.\",\"start_ms\":0,\"end_ms\":1200},{\"text\":\"second phrase there.\",\"start_ms\":1300,\"end_ms\":2500}]}",
    };
    var db = try DB.open(alloc, std.mem.span(path), .{
        .ttl_cleanup = .{ .enabled = false },
        .enrichment = .{
            .owner_id = "worker-a",
            .asset_producer = fake.producer(),
        },
    });
    defer db.close();

    try db.enrichment.addEnrichment(.{
        .name = "document_units_v1",
        .kind = .asset,
        .field = "url",
        .content_type = "application/json",
        .producer_json = "{\"type\":\"document_extraction\",\"config\":{\"source\":{\"filename_field\":\"filename\",\"content_type_field\":\"mime_type\"},\"transcription\":{\"transcriber\":{\"provider\":\"mock-transcriber\",\"model\":\"test-transcriber\"}}}}",
    });
    try db.enrichment.addEnrichment(.{
        .name = "document_chunks_v1",
        .kind = .chunk,
        .field = "text",
        .source_artifact_name = "document_units_v1",
        .chunk_size = 20,
    });

    try db.batch(.{
        .writes = &.{.{
            .key = "doc:timed-audio",
            .value = "{\"filename\":\"audio.mp3\",\"mime_type\":\"audio/mpeg\",\"url\":\"data:audio/mpeg;base64,SUQzYXVkaW8gYnl0ZXM=\"}",
        }},
        .sync_level = .full_index,
    });
    try std.testing.expectEqual(@as(usize, 1), fake.transcriber_calls);

    const unit_key = try db_mod.internal_keys.documentUnitArtifactKeyAlloc(alloc, "doc:timed-audio", "document_units_v1", "audio:000001");
    defer alloc.free(unit_key);
    const unit_payload = try db.core.store.get(alloc, unit_key);
    defer alloc.free(unit_payload);
    var parsed_unit = try std.json.parseFromSlice(std.json.Value, alloc, unit_payload, .{});
    defer parsed_unit.deinit();
    try std.testing.expectEqualStrings("first phrase here. second phrase there.", parsed_unit.value.object.get("text").?.string);
    const spans = parsed_unit.value.object.get("provenance").?.object.get("transcript_spans").?.array;
    try std.testing.expectEqual(@as(usize, 2), spans.items.len);
    try std.testing.expectEqual(@as(i64, 0), spans.items[0].object.get("char_start").?.integer);
    try std.testing.expectEqual(@as(i64, 18), spans.items[0].object.get("char_end").?.integer);
    try std.testing.expectEqual(@as(i64, 1200), spans.items[0].object.get("end_ms").?.integer);
    try std.testing.expectEqual(@as(i64, 19), spans.items[1].object.get("char_start").?.integer);
    try std.testing.expectEqual(@as(i64, 1300), spans.items[1].object.get("start_ms").?.integer);
    try std.testing.expectEqual(@as(i64, 2500), spans.items[1].object.get("end_ms").?.integer);

    // Every chunk cut from the transcript carries the offsets of the phrases
    // it overlaps, and the first chunk starts where the recording does.
    var chunk_index: u32 = 0;
    var timed_chunks: usize = 0;
    var last_end_ms: f64 = 0;
    while (true) : (chunk_index += 1) {
        const chunk_key = try db_mod.internal_keys.documentUnitChunkArtifactKeyAlloc(alloc, "doc:timed-audio", "document_chunks_v1", "audio:000001", chunk_index);
        defer alloc.free(chunk_key);
        const chunk_payload = db.core.store.get(alloc, chunk_key) catch |err| switch (err) {
            error.NotFound => break,
            else => return err,
        };
        defer alloc.free(chunk_payload);
        var parsed_chunk = try std.json.parseFromSlice(std.json.Value, alloc, chunk_payload, .{});
        defer parsed_chunk.deinit();
        const start_ms = db_mod.jsonTestNumber(parsed_chunk.value.object.get("_start_time_ms").?);
        const end_ms = db_mod.jsonTestNumber(parsed_chunk.value.object.get("_end_time_ms").?);
        if (chunk_index == 0) try std.testing.expectEqual(@as(f64, 0), start_ms);
        try std.testing.expect(end_ms >= start_ms);
        try std.testing.expect(start_ms >= last_end_ms or start_ms == 0 or start_ms == 1300);
        last_end_ms = end_ms;
        timed_chunks += 1;
    }
    try std.testing.expect(timed_chunks >= 2);
    try std.testing.expectEqual(@as(f64, 2500), last_end_ms);
}

test "db document extraction keeps diarized speakers through reopen" {
    const alloc = std.testing.allocator;

    var path_tmp = try db_mod.TestDirectory.init("db");
    defer path_tmp.cleanup();
    const path = path_tmp.path().ptr;
    defer db_mod.cleanupTempDir(path);

    var fake = db_mod.TestAssetProducer{
        .transcriber_output = "{\"text\":\"alpha alpha alpha alpha alpha. beta beta beta beta beta.\",\"confidence\":0.9,\"duration_ms\":6000,\"speakers\":[\"SPEAKER_00\",\"SPEAKER_01\"],\"segments\":[{\"text\":\"alpha alpha alpha alpha alpha.\",\"start_ms\":0,\"end_ms\":3000,\"speaker\":\"SPEAKER_00\"},{\"text\":\"beta beta beta beta beta.\",\"start_ms\":3100,\"end_ms\":6000,\"speaker\":\"SPEAKER_01\"}]}",
    };

    {
        var db = try DB.open(alloc, std.mem.span(path), .{
            .ttl_cleanup = .{ .enabled = false },
            .enrichment = .{
                .owner_id = "worker-a",
                .asset_producer = fake.producer(),
            },
        });
        defer db.close();

        try db.enrichment.addEnrichment(.{
            .name = "document_units_v1",
            .kind = .asset,
            .field = "url",
            .content_type = "application/json",
            .producer_json = "{\"type\":\"document_extraction\",\"config\":{\"source\":{\"filename_field\":\"filename\",\"content_type_field\":\"mime_type\"},\"transcription\":{\"transcriber\":{\"provider\":\"mock-transcriber\",\"model\":\"test-transcriber\",\"diarization\":true}}}}",
        });
        try db.enrichment.addEnrichment(.{
            .name = "document_chunks_v1",
            .kind = .chunk,
            .field = "text",
            .source_artifact_name = "document_units_v1",
            .chunk_size = 20,
        });

        try db.batch(.{
            .writes = &.{.{
                .key = "doc:diarized-audio",
                .value = "{\"filename\":\"call.webm\",\"mime_type\":\"video/webm\",\"url\":\"data:video/webm;base64,SUQzYXVkaW8gYnl0ZXM=\"}",
            }},
            .sync_level = .full_index,
        });
        try std.testing.expectEqual(@as(usize, 1), fake.transcriber_calls);
    }

    // Reopen: the speaker attribution has to survive as durable artifact
    // state, not just as something the enrichment pass held in memory.
    var reopened = try DB.open(alloc, std.mem.span(path), .{
        .ttl_cleanup = .{ .enabled = false },
        .start_index_workers = false,
    });
    defer reopened.close();

    const unit_key = try db_mod.internal_keys.documentUnitArtifactKeyAlloc(alloc, "doc:diarized-audio", "document_units_v1", "audio:000001");
    defer alloc.free(unit_key);
    const unit_payload = try reopened.core.store.get(alloc, unit_key);
    defer alloc.free(unit_payload);
    var parsed_unit = try std.json.parseFromSlice(std.json.Value, alloc, unit_payload, .{});
    defer parsed_unit.deinit();

    // Every phrase kept the speaker who said it, numbered in the order they
    // first spoke.
    const spans = parsed_unit.value.object.get("provenance").?.object.get("transcript_spans").?.array;
    try std.testing.expect(spans.items.len >= 2);
    try std.testing.expectEqual(@as(i64, 0), spans.items[0].object.get("speaker_index").?.integer);
    try std.testing.expectEqual(@as(i64, 1), spans.items[spans.items.len - 1].object.get("speaker_index").?.integer);

    // Chunks cut from the transcript carry the speaker's label, and a chunk
    // that straddles the turn carries none.
    var chunk_index: u32 = 0;
    var first_speaker_chunks: usize = 0;
    var second_speaker_chunks: usize = 0;
    while (true) : (chunk_index += 1) {
        const chunk_key = try db_mod.internal_keys.documentUnitChunkArtifactKeyAlloc(alloc, "doc:diarized-audio", "document_chunks_v1", "audio:000001", chunk_index);
        defer alloc.free(chunk_key);
        const chunk_payload = reopened.core.store.get(alloc, chunk_key) catch |err| switch (err) {
            error.NotFound => break,
            else => return err,
        };
        defer alloc.free(chunk_payload);
        var parsed_chunk = try std.json.parseFromSlice(std.json.Value, alloc, chunk_payload, .{});
        defer parsed_chunk.deinit();
        const speaker = parsed_chunk.value.object.get("_speaker") orelse continue;
        if (std.mem.eql(u8, speaker.string, "SPEAKER_00")) {
            first_speaker_chunks += 1;
        } else if (std.mem.eql(u8, speaker.string, "SPEAKER_01")) {
            second_speaker_chunks += 1;
        } else {
            return error.UnexpectedSpeakerLabel;
        }
    }
    try std.testing.expect(first_speaker_chunks >= 1);
    try std.testing.expect(second_speaker_chunks >= 1);
}

test "db document extraction stores rfc822 email units" {
    const alloc = std.testing.allocator;

    var path_tmp = try db_mod.TestDirectory.init("db");
    defer path_tmp.cleanup();
    const path = path_tmp.path().ptr;
    defer db_mod.cleanupTempDir(path);

    var db = try DB.open(alloc, std.mem.span(path), .{
        .start_index_workers = false,
        .ttl_cleanup = .{ .enabled = false },
    });
    defer db.close();

    try db.enrichment.addEnrichment(.{
        .name = "document_units_v1",
        .kind = .asset,
        .field = "url",
        .content_type = "application/json",
        .producer_json = "{\"type\":\"document_extraction\",\"config\":{\"source\":{\"filename_field\":\"filename\",\"content_type_field\":\"mime_type\"}}}",
    });

    try db.batch(.{
        .writes = &.{.{
            .key = "doc:email",
            .value = "{\"filename\":\"message.eml\",\"mime_type\":\"message/rfc822\",\"url\":\"data:message/rfc822;base64,U3ViamVjdDogQWxwaGENCkZyb206IGFAZXhhbXBsZS50ZXN0DQoNCkhlbGxvIGVtYWls\"}",
        }},
        .sync_level = .enrichments,
    });

    const manifest_key = try db_mod.internal_keys.artifactNamedPrefixAlloc(alloc, "doc:email", "asset", "document_units_v1");
    defer alloc.free(manifest_key);
    const headers_key = try db_mod.internal_keys.documentUnitArtifactKeyAlloc(alloc, "doc:email", "document_units_v1", "email:headers");
    defer alloc.free(headers_key);
    const body_key = try db_mod.internal_keys.documentUnitArtifactKeyAlloc(alloc, "doc:email", "document_units_v1", "email:body");
    defer alloc.free(body_key);

    const manifest = try db.core.store.get(alloc, manifest_key);
    defer alloc.free(manifest);
    try std.testing.expect(std.mem.indexOf(u8, manifest, "\"route_type\":\"email\"") != null);
    try std.testing.expect(std.mem.indexOf(u8, manifest, "\"unit_count\":2") != null);

    const headers_payload = try db.core.store.get(alloc, headers_key);
    defer alloc.free(headers_payload);
    try std.testing.expect(std.mem.indexOf(u8, headers_payload, "\"unit_type\":\"email_headers\"") != null);
    try std.testing.expect(std.mem.indexOf(u8, headers_payload, "Subject: Alpha") != null);
    try std.testing.expect(std.mem.indexOf(u8, headers_payload, "\"method\":\"email_rfc822\"") != null);

    const body_payload = try db.core.store.get(alloc, body_key);
    defer alloc.free(body_payload);
    try std.testing.expect(std.mem.indexOf(u8, body_payload, "\"unit_type\":\"email_body\"") != null);
    try std.testing.expect(std.mem.indexOf(u8, body_payload, "\"text\":\"Hello email\"") != null);
    try std.testing.expect(std.mem.indexOf(u8, body_payload, "\"source_content_type\":\"message/rfc822\"") != null);
}

test "db document extraction stores multipart rfc822 text parts" {
    const alloc = std.testing.allocator;

    var path_tmp = try db_mod.TestDirectory.init("db");
    defer path_tmp.cleanup();
    const path = path_tmp.path().ptr;
    defer db_mod.cleanupTempDir(path);

    var db = try DB.open(alloc, std.mem.span(path), .{
        .start_index_workers = false,
        .ttl_cleanup = .{ .enabled = false },
    });
    defer db.close();

    try db.enrichment.addEnrichment(.{
        .name = "document_units_v1",
        .kind = .asset,
        .field = "url",
        .content_type = "application/json",
        .producer_json = "{\"type\":\"document_extraction\",\"config\":{\"source\":{\"filename_field\":\"filename\",\"content_type_field\":\"mime_type\"}}}",
    });

    try db.batch(.{
        .writes = &.{.{
            .key = "doc:multipart-email",
            .value = "{\"filename\":\"message.eml\",\"mime_type\":\"message/rfc822\",\"url\":\"data:message/rfc822;base64,U3ViamVjdDogQWxwaGENCkNvbnRlbnQtVHlwZTogbXVsdGlwYXJ0L2FsdGVybmF0aXZlOyBib3VuZGFyeT0iYjEiDQoNCi0tYjENCkNvbnRlbnQtVHlwZTogdGV4dC9wbGFpbg0KDQpQbGFpbiBib2R5DQotLWIxDQpDb250ZW50LVR5cGU6IHRleHQvaHRtbA0KDQo8cD5IVE1MIGJvZHk8L3A+DQotLWIxLS0NCg==\"}",
        }},
        .sync_level = .enrichments,
    });

    const manifest_key = try db_mod.internal_keys.artifactNamedPrefixAlloc(alloc, "doc:multipart-email", "asset", "document_units_v1");
    defer alloc.free(manifest_key);
    const plain_key = try db_mod.internal_keys.documentUnitArtifactKeyAlloc(alloc, "doc:multipart-email", "document_units_v1", "email:part:000001");
    defer alloc.free(plain_key);
    const html_key = try db_mod.internal_keys.documentUnitArtifactKeyAlloc(alloc, "doc:multipart-email", "document_units_v1", "email:part:000002");
    defer alloc.free(html_key);

    const manifest = try db.core.store.get(alloc, manifest_key);
    defer alloc.free(manifest);
    try std.testing.expect(std.mem.indexOf(u8, manifest, "\"route_type\":\"email\"") != null);
    try std.testing.expect(std.mem.indexOf(u8, manifest, "\"unit_count\":3") != null);

    const plain_payload = try db.core.store.get(alloc, plain_key);
    defer alloc.free(plain_payload);
    try std.testing.expect(std.mem.indexOf(u8, plain_payload, "\"unit_type\":\"email_part\"") != null);
    try std.testing.expect(std.mem.indexOf(u8, plain_payload, "\"method\":\"email_text\"") != null);
    try std.testing.expect(std.mem.indexOf(u8, plain_payload, "Plain body") != null);

    const html_payload = try db.core.store.get(alloc, html_key);
    defer alloc.free(html_payload);
    try std.testing.expect(std.mem.indexOf(u8, html_payload, "\"method\":\"email_html\"") != null);
    try std.testing.expect(std.mem.indexOf(u8, html_payload, "\"text\":\"HTML body\"") != null);
}

test "db document extraction stores unsupported file manifest without searchable units" {
    const alloc = std.testing.allocator;

    var path_tmp = try db_mod.TestDirectory.init("db");
    defer path_tmp.cleanup();
    const path = path_tmp.path().ptr;
    defer db_mod.cleanupTempDir(path);

    var db = try DB.open(alloc, std.mem.span(path), .{});
    defer db.close();

    try db.enrichment.addEnrichment(.{
        .name = "document_units_v1",
        .kind = .asset,
        .field = "url",
        .content_type = "application/json",
        .producer_json = "{\"type\":\"document_extraction\",\"config\":{}}",
    });
    try db.addIndex(.{
        .name = "ft_document_units",
        .kind = .full_text,
        .config_json = "{\"artifact_name\":\"document_units_v1\"}",
    });

    try db.batch(.{
        .writes = &.{.{
            .key = "doc:bin",
            .value = "{\"url\":\"data:application/octet-stream;base64,AAEC\"}",
        }},
        .sync_level = .full_index,
    });

    const manifest_key = try db_mod.internal_keys.artifactNamedPrefixAlloc(alloc, "doc:bin", "asset", "document_units_v1");
    defer alloc.free(manifest_key);
    const state_key = try db_mod.assetStateKeyAlloc(alloc, "doc:bin", "document_units_v1");
    defer alloc.free(state_key);

    const manifest = try db.core.store.get(alloc, manifest_key);
    defer alloc.free(manifest);
    try std.testing.expect(std.mem.indexOf(u8, manifest, "\"route_type\":\"unsupported\"") != null);
    try std.testing.expect(std.mem.indexOf(u8, manifest, "\"unsupported_reason\":\"unsupported_content_type\"") != null);
    try std.testing.expect(std.mem.indexOf(u8, manifest, "\"unit_count\":0") != null);
    try std.testing.expect(std.mem.indexOf(u8, manifest, "\"chunk_count\":0") != null);

    const state = try db.core.store.get(alloc, state_key);
    defer alloc.free(state);
    try std.testing.expect(std.mem.indexOf(u8, state, "\"unit_keys\":[]") != null);
    try std.testing.expect(std.mem.indexOf(u8, state, "\"chunk_keys\":[]") != null);

    var result = try db.search(alloc, .{
        .index_name = "ft_document_units",
        .full_text = .{ .match_all = {} },
    });
    defer result.deinit();
    try std.testing.expectEqual(@as(u32, 0), result.total_hits);
}

test "db document extraction skips stable unit local rewrites without text consumers" {
    const alloc = std.testing.allocator;

    var path_tmp = try db_mod.TestDirectory.init("db");
    defer path_tmp.cleanup();
    const path = path_tmp.path().ptr;
    defer db_mod.cleanupTempDir(path);

    var counting = db_mod.CountingDenseEmbedder{};
    var db = try DB.open(alloc, std.mem.span(path), .{
        .enrichment = .{
            .owner_id = "worker-a",
            .dense_embedder = counting.interface(),
        },
    });
    defer db.close();

    try db.enrichment.addEnrichment(.{
        .name = "document_units_v1",
        .kind = .asset,
        .field = "url",
        .content_type = "application/json",
        .producer_json = "{\"type\":\"document_extraction\",\"config\":{}}",
    });
    try db.enrichment.addEnrichment(.{
        .name = "document_chunks_v1",
        .kind = .chunk,
        .field = "text",
        .source_artifact_name = "document_units_v1",
        .chunk_size = 256,
    });
    try db.enrichment.addEnrichment(.{
        .name = "document_chunk_dense_v1",
        .kind = .embedding,
        .field = "text",
        .source_artifact_name = "document_chunks_v1",
        .expected_dims = 3,
    });
    try db.addIndex(.{
        .name = "dv_document_chunks",
        .kind = .dense_vector,
        .config_json = "{\"field\":\"embedding\",\"dims\":3,\"embedding_name\":\"document_chunk_dense_v1\"}",
    });
    try std.testing.expect(db.core.index_manager.denseIndex("dv_document_chunks").?.supports_unit_grouping);

    try db.batch(.{
        .writes = &.{.{
            .key = "doc:a",
            .value = "{\"url\":\"data:text/plain;base64,YWxwaGEgYmV0YSBnYW1tYQ==\"}",
        }},
        .sync_level = .full_index,
    });
    try db.runUntilIdle();
    const first_calls = counting.calls;
    try std.testing.expectEqual(@as(usize, 1), first_calls);

    const manifest_key = try db_mod.internal_keys.artifactNamedPrefixAlloc(alloc, "doc:a", "asset", "document_units_v1");
    defer alloc.free(manifest_key);

    try db.batch(.{
        .writes = &.{.{
            .key = "doc:a",
            .value = "{\"url\":\"data:text/plain,alpha%20beta%20gamma\"}",
        }},
        .sync_level = .full_index,
    });
    try std.testing.expectEqual(first_calls, counting.calls);

    const manifest = try db.core.store.get(alloc, manifest_key);
    defer alloc.free(manifest);
    try std.testing.expect(std.mem.indexOf(u8, manifest, "\"generation\":2") != null);
    try std.testing.expect(std.mem.indexOf(u8, manifest, "\"op\":\"keep\"") != null);
    try std.testing.expect(std.mem.indexOf(u8, manifest, "\"fingerprint_match\":true") != null);
}

test "db dispatches generated document child range artifacts to remote owner" {
    const alloc = std.testing.allocator;

    var path_tmp = try db_mod.TestDirectory.init("db");
    defer path_tmp.cleanup();
    const path = path_tmp.path().ptr;
    defer db_mod.cleanupTempDir(path);

    var db = try DB.open(alloc, std.mem.span(path), .{
        .start_index_workers = false,
        .ttl_cleanup = .{ .enabled = false },
    });
    defer db.close();

    try db.enrichment.addEnrichment(.{
        .name = "document_units_v1",
        .kind = .asset,
        .field = "url",
        .content_type = "application/json",
        .producer_json = "{\"type\":\"document_extraction\",\"config\":{}}",
    });

    try db.batch(.{
        .writes = &.{.{
            .key = "doc:a",
            .value = "{\"url\":\"data:text/plain;base64,YWxwaGE=\"}",
        }},
        .sync_level = .full_index,
    });

    try std.testing.expect(try db.updateDocumentArtifactChildRangePlacement(alloc, "doc:a", "document_units_v1", .{
        .range_id = "range:000000",
        .placement = "remote",
        .owner_group_id = 7002,
        .placement_generation = 7,
        .route_status = "remote_committed",
        .split_eligible = true,
    }));

    var moved = (try db.getDocumentArtifactManifest(alloc, "doc:a", "document_units_v1")) orelse return error.TestUnexpectedResult;
    defer moved.deinit(alloc);
    try std.testing.expectEqual(@as(usize, 1), moved.child_ranges.len);
    const remote_unit_key = try alloc.dupe(u8, moved.child_ranges[0].start_key);
    defer alloc.free(remote_unit_key);

    const local_deletes = [_][]const u8{remote_unit_key};
    _ = try db.applyDocumentArtifactChildRangeBatch(.{
        .artifact_delete_keys = local_deletes[0..],
        .sync_level = .write,
    });
    try std.testing.expectError(error.NotFound, db.core.store.get(alloc, remote_unit_key));

    const Capture = struct {
        calls: usize = 0,
        owner_group_id: u64 = 0,
        artifact_writes: usize = 0,
        artifact_delete_keys: usize = 0,
        documents: usize = 0,
        dense_embeddings: usize = 0,
        sparse_embeddings: usize = 0,
        first_key: ?[]u8 = null,
        first_value: ?[]u8 = null,

        pub fn deinit(self: *@This(), allocator: Allocator) void {
            if (self.first_key) |key| allocator.free(key);
            if (self.first_value) |value| allocator.free(value);
        }

        fn dispatcher(self: *@This()) db_mod.DocumentArtifactChildRangeDispatcher {
            return .{ .ptr = self, .select_destination = db_mod.testChildRangeDestination, .apply = apply };
        }

        fn apply(ptr: *anyopaque, allocator: Allocator, dispatch: db_mod.DocumentArtifactChildRangeDispatch) !void {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            self.calls += 1;
            self.owner_group_id = dispatch.owner_group_id;
            self.artifact_writes += dispatch.child_batch.artifact_writes.len;
            self.artifact_delete_keys += dispatch.child_batch.artifact_delete_keys.len;
            self.documents += dispatch.child_batch.documents.len;
            self.dense_embeddings += dispatch.child_batch.dense_embeddings.len;
            self.sparse_embeddings += dispatch.child_batch.sparse_embeddings.len;
            if (self.first_key == null and dispatch.child_batch.artifact_writes.len > 0) {
                self.first_key = try allocator.dupe(u8, dispatch.child_batch.artifact_writes[0].key);
                errdefer {
                    allocator.free(self.first_key.?);
                    self.first_key = null;
                }
                self.first_value = try allocator.dupe(u8, dispatch.child_batch.artifact_writes[0].value);
            }
        }
    };

    var capture = Capture{};
    defer capture.deinit(alloc);

    try db.batchWithDocumentArtifactChildRangeDispatcher(.{
        .writes = &.{.{
            .key = "doc:a",
            .value = "{\"url\":\"data:text/plain;base64,YmV0YQ==\"}",
        }},
        .sync_level = .full_index,
    }, capture.dispatcher());

    try std.testing.expectEqual(@as(usize, 1), capture.calls);
    try std.testing.expectEqual(@as(u64, 7002), capture.owner_group_id);
    try std.testing.expectEqual(@as(usize, 1), capture.artifact_writes);
    try std.testing.expectEqual(@as(usize, 0), capture.artifact_delete_keys);
    try std.testing.expectEqualStrings(remote_unit_key, capture.first_key.?);
    try std.testing.expect(std.mem.indexOf(u8, capture.first_value.?, "\"_artifact_route_status\":\"remote_committed\"") != null);
    try std.testing.expect(std.mem.indexOf(u8, capture.first_value.?, "\"_artifact_owner_group_id\":7002") != null);
    try std.testing.expectError(error.NotFound, db.core.store.get(alloc, remote_unit_key));
}

test "db retries remote document child range dispatch from durable outbox" {
    const alloc = std.testing.allocator;

    var path_tmp = try db_mod.TestDirectory.init("db");
    defer path_tmp.cleanup();
    const path = path_tmp.path().ptr;
    defer db_mod.cleanupTempDir(path);

    var db = try DB.open(alloc, std.mem.span(path), .{
        .start_index_workers = false,
        .ttl_cleanup = .{ .enabled = false },
    });
    defer db.close();

    try db.enrichment.addEnrichment(.{
        .name = "document_units_v1",
        .kind = .asset,
        .field = "url",
        .content_type = "application/json",
        .producer_json = "{\"type\":\"document_extraction\",\"config\":{}}",
    });

    try db.batch(.{
        .writes = &.{.{
            .key = "doc:a",
            .value = "{\"url\":\"data:text/plain;base64,YWxwaGE=\"}",
        }},
        .sync_level = .full_index,
    });

    try std.testing.expect(try db.updateDocumentArtifactChildRangePlacement(alloc, "doc:a", "document_units_v1", .{
        .range_id = "range:000000",
        .placement = "remote",
        .owner_group_id = 7002,
        .placement_generation = 7,
        .route_status = "remote_committed",
        .split_eligible = true,
    }));

    var moved = (try db.getDocumentArtifactManifest(alloc, "doc:a", "document_units_v1")) orelse return error.TestUnexpectedResult;
    defer moved.deinit(alloc);
    const remote_unit_key = try alloc.dupe(u8, moved.child_ranges[0].start_key);
    defer alloc.free(remote_unit_key);

    const local_deletes = [_][]const u8{remote_unit_key};
    _ = try db.applyDocumentArtifactChildRangeBatch(.{
        .artifact_delete_keys = local_deletes[0..],
        .sync_level = .write,
    });

    const FlakyCapture = struct {
        fail_next: bool = true,
        calls: usize = 0,
        owner_group_id: u64 = 0,
        artifact_writes: usize = 0,
        first_key: ?[]u8 = null,

        pub fn deinit(self: *@This(), allocator: Allocator) void {
            if (self.first_key) |key| allocator.free(key);
        }

        fn dispatcher(self: *@This()) db_mod.DocumentArtifactChildRangeDispatcher {
            return .{ .ptr = self, .select_destination = db_mod.testChildRangeDestination, .apply = apply };
        }

        fn apply(ptr: *anyopaque, allocator: Allocator, dispatch: db_mod.DocumentArtifactChildRangeDispatch) !void {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            self.calls += 1;
            if (self.fail_next) {
                self.fail_next = false;
                return error.IntentionalDispatchFailure;
            }
            self.owner_group_id = dispatch.owner_group_id;
            self.artifact_writes += dispatch.child_batch.artifact_writes.len;
            if (self.first_key == null and dispatch.child_batch.artifact_writes.len > 0) {
                self.first_key = try allocator.dupe(u8, dispatch.child_batch.artifact_writes[0].key);
            }
        }
    };

    var capture = FlakyCapture{};
    defer capture.deinit(alloc);

    try std.testing.expectError(error.IntentionalDispatchFailure, db.batchWithDocumentArtifactChildRangeDispatcher(.{
        .writes = &.{.{
            .key = "doc:a",
            .value = "{\"url\":\"data:text/plain;base64,YmV0YQ==\"}",
        }},
        .sync_level = .full_index,
    }, capture.dispatcher()));

    const outbox_prefix = try db_mod.internal_keys.documentChildRangeOutboxRootPrefixAlloc(alloc);
    defer alloc.free(outbox_prefix);
    const pending = try db.core.scanStorePrefix(alloc, outbox_prefix);
    defer db_mod.docstore_mod.DocStore.freeResults(alloc, pending);
    try std.testing.expectEqual(@as(usize, 1), pending.len);
    try std.testing.expectError(error.NotFound, db.core.store.get(alloc, remote_unit_key));

    const drained = try db.drainDocumentArtifactChildRangeOutbox(capture.dispatcher(), 0);
    try std.testing.expectEqual(@as(usize, 1), drained.scanned);
    try std.testing.expectEqual(@as(usize, 1), drained.dispatched);
    try std.testing.expectEqual(@as(usize, 1), drained.deleted);
    try std.testing.expectEqual(@as(usize, 2), capture.calls);
    try std.testing.expectEqual(@as(u64, 7002), capture.owner_group_id);
    try std.testing.expectEqual(@as(usize, 1), capture.artifact_writes);
    try std.testing.expectEqualStrings(remote_unit_key, capture.first_key.?);

    const after = try db.core.scanStorePrefix(alloc, outbox_prefix);
    defer db_mod.docstore_mod.DocStore.freeResults(alloc, after);
    try std.testing.expectEqual(@as(usize, 0), after.len);
}

test "db public extraction budget rejection publishes a failure manifest" {
    const alloc = std.testing.allocator;
    var path_tmp = try db_mod.TestDirectory.init("db");
    defer path_tmp.cleanup();
    const resources = @import("../../resource_manager.zig");
    var budgets = resources.Options.defaultBudgets();
    budgets[@backingInt(resources.Slice.document_extraction_working_set)] = .{ .soft_limit_bytes = 3072, .hard_limit_bytes = 4096 };
    var manager = resources.ResourceManager.init(.{ .budgets = budgets });
    defer manager.deinit(alloc);
    var db = try DB.open(alloc, path_tmp.path(), .{
        .start_optional_runtime_workers = false,
        .start_index_workers = false,
        .resource_manager = &manager,
        .enrichment = .{ .resource_manager = &manager },
    });
    defer db.close();
    try db.enrichment.addEnrichment(.{ .name = "units", .kind = .asset, .field = "url", .content_type = "application/json", .producer_json = "{\"type\":\"document_extraction\",\"config\":{}}" });
    const body = try alloc.alloc(u8, 64 * 1024);
    defer alloc.free(body);
    @memset(body, 'x');
    const encoded = try alloc.alloc(u8, std.base64.standard.Encoder.calcSize(body.len));
    defer alloc.free(encoded);
    _ = std.base64.standard.Encoder.encode(encoded, body);
    const document = try std.fmt.allocPrint(alloc, "{{\"url\":\"data:text/plain;base64,{s}\"}}", .{encoded});
    defer alloc.free(document);
    try db.batch(.{ .writes = &.{.{ .key = "doc", .value = document }}, .sync_level = .write });
    try std.testing.expect(try db.reprocessDocumentArtifact(alloc, "doc", "units"));
    var manifest = (try db.getDocumentArtifactManifest(alloc, "doc", "units")) orelse return error.TestUnexpectedResult;
    defer manifest.deinit(alloc);
    try std.testing.expectEqualStrings("failed", manifest.merge_status);
    try std.testing.expectEqualStrings("DocumentExtractionWorkingSetTooLarge", manifest.last_error_code.?);
    try std.testing.expectEqual(@as(usize, 0), manifest.unit_count);
}

test "db public artifact reprocess accepts durable work despite unrelated provider retries" {
    const alloc = std.testing.allocator;
    var path_tmp = try db_mod.TestDirectory.init("db");
    defer path_tmp.cleanup();
    const path = path_tmp.path().ptr;
    defer db_mod.cleanupTempDir(path);

    var gated = db_mod.GateDenseEmbedder{};
    gated.allowed_successes.store(100, .release);
    var db = try DB.open(alloc, std.mem.span(path), .{
        .start_optional_runtime_workers = false,
        .enrichment = .{
            .owner_id = "worker-a",
            .dense_embedder = gated.interface(),
            .inline_retry_max_attempts = 1,
            .worker_retry_max_attempts = 10,
        },
    });
    defer db.close();
    try db.enrichment.addEnrichment(.{
        .name = "document_units_v1",
        .kind = .asset,
        .field = "url",
        .content_type = "application/json",
        .producer_json = "{\"type\":\"document_extraction\",\"config\":{}}",
    });
    try db.addIndex(.{
        .name = "semantic",
        .kind = .dense_vector,
        .config_json = "{\"field\":\"embedding\",\"dims\":3,\"generator\":{\"kind\":\"dense_embedding\",\"source_field\":\"body\",\"embedding_name\":\"dense_v1\"}}",
    });
    try db.batch(.{
        .writes = &.{
            .{ .key = "doc:repair", .value = "{\"url\":\"data:text/plain;base64,YWxwaGE=\"}" },
            .{ .key = "doc:embedding", .value = "{\"body\":\"healthy cached output\"}" },
        },
        .sync_level = .full_index,
    });
    var before = (try db.getDocumentArtifactManifest(alloc, "doc:repair", "document_units_v1")) orelse return error.TestUnexpectedResult;
    defer before.deinit(alloc);
    gated.allowed_successes.store(0, .release);
    try db.batch(.{
        .writes = &.{.{ .key = "doc:retry", .value = "{\"body\":\"provider unavailable\"}" }},
        .sync_level = .write,
    });
    try std.testing.expectError(error.EnrichmentRetryInProgress, db.enrichment.runUntil(db.core.nextEnrichmentSequence()));

    // Both public endpoints acknowledge the committed request without waiting
    // for the unrelated retry debt. The repair queue still uses full_index.
    try std.testing.expect(try db.reprocessDocumentArtifact(alloc, "doc:repair", "document_units_v1"));
    // Public acceptance commits the request without entering inference. Neither
    // unrelated retries nor rejection by the requested provider can erase it.
    const calls_before_acceptance = gated.successful_requests.load(.acquire);
    try std.testing.expect(try db.reprocessDocumentArtifact(alloc, "doc:embedding", "dense_v1"));
    try std.testing.expectEqual(calls_before_acceptance, gated.successful_requests.load(.acquire));
    try std.testing.expect(try db.reprocessDocumentEmbeddingArtifact(alloc, "doc:embedding", "dense_v1"));
    try std.testing.expectEqual(calls_before_acceptance, gated.successful_requests.load(.acquire));
    try std.testing.expectError(error.NotFound, db.reprocessDocumentArtifact(alloc, "missing", "document_units_v1"));
    try std.testing.expect(!try db.reprocessDocumentArtifact(alloc, "doc:repair", "missing"));
    try std.testing.expect(!try db.reprocessDocumentEmbeddingArtifact(alloc, "missing", "dense_v1"));

    // Provider recovery lets the queued work finish; success did not discard it.
    gated.allowed_successes.store(100, .release);
    db_mod.sleepNs(550 * std.time.ns_per_ms);
    try db.runUntilIdle();
    var after = (try db.getDocumentArtifactManifest(alloc, "doc:repair", "document_units_v1")) orelse return error.TestUnexpectedResult;
    defer after.deinit(alloc);
    try std.testing.expectEqual(before.generation + 1, after.generation);
    const embedding_key = try db_mod.internal_keys.embeddingArtifactKeyForDocumentAlloc(alloc, "doc:retry", "dense_v1");
    defer alloc.free(embedding_key);
    const embedding = try db.core.store.get(alloc, embedding_key);
    defer alloc.free(embedding);
    try std.testing.expect(embedding.len > 0);
}

test "db document extraction manifest inspection and reprocess API" {
    const alloc = std.testing.allocator;

    var path_tmp = try db_mod.TestDirectory.init("db");
    defer path_tmp.cleanup();
    const path = path_tmp.path().ptr;
    defer db_mod.cleanupTempDir(path);

    var counting = db_mod.CountingDenseEmbedder{};
    var db = try DB.open(alloc, std.mem.span(path), .{
        .enrichment = .{
            .owner_id = "worker-a",
            .dense_embedder = counting.interface(),
        },
    });
    defer db.close();

    try db.enrichment.addEnrichment(.{
        .name = "document_units_v1",
        .kind = .asset,
        .field = "url",
        .content_type = "application/json",
        .producer_json = "{\"type\":\"document_extraction\",\"config\":{}}",
    });
    try db.enrichment.addEnrichment(.{
        .name = "document_chunks_v1",
        .kind = .chunk,
        .field = "text",
        .source_artifact_name = "document_units_v1",
        .chunk_size = 256,
    });
    try db.enrichment.addEnrichment(.{
        .name = "document_chunk_dense_v1",
        .kind = .embedding,
        .field = "text",
        .source_artifact_name = "document_chunks_v1",
        .expected_dims = 3,
    });
    try db.addIndex(.{
        .name = "dv_document_chunks",
        .kind = .dense_vector,
        .config_json = "{\"field\":\"embedding\",\"dims\":3,\"embedding_name\":\"document_chunk_dense_v1\"}",
    });

    try db.batch(.{
        .writes = &.{.{
            .key = "doc:a",
            .value = "{\"url\":\"data:text/plain;base64,YWxwaGEgYmV0YSBnYW1tYQ==\"}",
        }},
        .sync_level = .full_index,
    });
    try db.runUntilIdle();
    const first_calls = counting.calls;
    try std.testing.expectEqual(@as(usize, 1), first_calls);

    var inspected = (try db.getDocumentArtifactManifest(alloc, "doc:a", "document_units_v1")) orelse return error.TestUnexpectedResult;
    defer inspected.deinit(alloc);
    try std.testing.expectEqualStrings("doc:a", inspected.document_id);
    try std.testing.expectEqualStrings("document_units_v1", inspected.artifact_name);
    try std.testing.expect(std.mem.startsWith(u8, inspected.artifact_id, "af1:asset:"));
    try std.testing.expectEqual(@as(u64, 2), inspected.manifest_version);
    try std.testing.expectEqual(@as(u64, 1), inspected.generation);
    try std.testing.expectEqualStrings("data:text/plain;base64,YWxwaGEgYmV0YSBnYW1tYQ==", inspected.source_url);
    try std.testing.expectEqual(@as(usize, 64), inspected.source_fingerprint.len);
    try std.testing.expectEqualStrings("text/plain", inspected.content_type);
    try std.testing.expectEqualStrings("text", inspected.route_type);
    try std.testing.expectEqual(@as(usize, 1), inspected.unit_count);
    try std.testing.expectEqual(@as(usize, 1), inspected.chunk_count);
    try std.testing.expectEqual(@as(usize, 2), inspected.child_range_count);
    try std.testing.expectEqual(@as(usize, 2), inspected.child_ranges.len);
    try std.testing.expectEqualStrings("range:000000", inspected.child_ranges[0].range_id);
    try std.testing.expectEqualStrings("unit", inspected.child_ranges[0].range_kind);
    try std.testing.expectEqualStrings("document_units_v1", inspected.child_ranges[0].artifact_name);
    try std.testing.expectEqual(@as(?u64, 0), inspected.child_ranges[0].owner_group_id);
    try std.testing.expectEqual(@as(?u64, 0), inspected.child_ranges[0].placement_generation);
    try std.testing.expectEqualStrings("local_committed", inspected.child_ranges[0].route_status.?);
    try std.testing.expectEqual(@as(?bool, false), inspected.child_ranges[0].split_eligible);
    try std.testing.expectEqual(@as(usize, 1), inspected.child_ranges[0].child_count);
    try std.testing.expect(inspected.child_ranges[0].text_bytes != null);
    try std.testing.expectEqualStrings("chunk", inspected.child_ranges[1].range_kind);
    try std.testing.expectEqualStrings("chunk", inspected.child_ranges[1].split_boundary);
    try std.testing.expect(std.mem.indexOf(u8, inspected.manifest_json, "\"range_policy\"") != null);
    try std.testing.expect(std.mem.indexOf(u8, inspected.manifest_json, "\"unit_target_children\":256") != null);
    try std.testing.expect(std.mem.indexOf(u8, inspected.manifest_json, "\"unit_target_text_bytes\":1048576") != null);
    try std.testing.expect(std.mem.indexOf(u8, inspected.manifest_json, "\"oversized_unit_policy\":\"single_unit_range\"") != null);
    try std.testing.expectEqualStrings("converged", inspected.merge_status);
    try std.testing.expectEqual(@as(u64, 0), inspected.merge_from_generation);
    try std.testing.expectEqual(@as(u64, 1), inspected.merge_to_generation);
    try std.testing.expectEqualStrings("unit_fingerprint", inspected.merge_operation_granularity);
    try std.testing.expect(inspected.merge_operation_count > 0);
    try std.testing.expect(inspected.state_json != null);

    try std.testing.expect(try db.updateDocumentArtifactChildRangePlacement(alloc, "doc:a", "document_units_v1", .{
        .range_id = "range:000000",
        .placement = "remote",
        .owner_group_id = 7001,
        .placement_generation = 2,
        .route_status = "remote_committed",
        .split_eligible = true,
    }));
    var moved = (try db.getDocumentArtifactManifest(alloc, "doc:a", "document_units_v1")) orelse return error.TestUnexpectedResult;
    defer moved.deinit(alloc);
    try std.testing.expectEqualStrings("remote", moved.child_ranges[0].placement);
    try std.testing.expectEqual(@as(?u64, 7001), moved.child_ranges[0].owner_group_id);
    try std.testing.expectEqual(@as(?u64, 2), moved.child_ranges[0].placement_generation);
    try std.testing.expectEqualStrings("remote_committed", moved.child_ranges[0].route_status.?);
    try std.testing.expectEqual(@as(?bool, true), moved.child_ranges[0].split_eligible);
    try std.testing.expect(std.mem.indexOf(u8, moved.manifest_json, "\"owner_group_id\":7001") != null);
    try std.testing.expect(!try db.updateDocumentArtifactChildRangePlacement(alloc, "doc:a", "document_units_v1", .{
        .range_id = "range:missing",
        .placement = "remote",
    }));
    try std.testing.expect(!try db.updateDocumentArtifactChildRangePlacement(alloc, "doc:missing", "document_units_v1", .{
        .range_id = "range:000000",
        .placement = "remote",
    }));

    var artifact_list = try db.listDocumentArtifactManifests(alloc, "doc:a");
    defer artifact_list.deinit(alloc);
    try std.testing.expectEqualStrings("doc:a", artifact_list.document_id);
    try std.testing.expectEqual(@as(usize, 1), artifact_list.artifacts.len);
    try std.testing.expectEqualStrings("document_units_v1", artifact_list.artifacts[0].artifact_name);
    try std.testing.expectEqual(@as(u64, 1), artifact_list.artifacts[0].generation);
    try std.testing.expect(artifact_list.artifacts[0].state_json != null);

    try std.testing.expect(try db.reprocessDocumentArtifact(alloc, "doc:a", "document_units_v1"));
    try std.testing.expectEqual(first_calls, counting.calls);

    var after = (try db.getDocumentArtifactManifest(alloc, "doc:a", "document_units_v1")) orelse return error.TestUnexpectedResult;
    defer after.deinit(alloc);
    try std.testing.expectEqual(@as(u64, 2), after.generation);
    try std.testing.expectEqualStrings("remote", after.child_ranges[0].placement);
    try std.testing.expectEqual(@as(?u64, 7001), after.child_ranges[0].owner_group_id);
    try std.testing.expectEqual(@as(?u64, 2), after.child_ranges[0].placement_generation);
    try std.testing.expectEqualStrings("remote_committed", after.child_ranges[0].route_status.?);
    try std.testing.expect(std.mem.indexOf(u8, after.manifest_json, "\"op\":\"keep\"") != null);
    const routed_unit_payload = try db.core.store.get(alloc, after.child_ranges[0].start_key);
    defer alloc.free(routed_unit_payload);
    try std.testing.expect(std.mem.indexOf(u8, routed_unit_payload, "\"_artifact_route_status\":\"remote_committed\"") != null);
    try std.testing.expect(std.mem.indexOf(u8, routed_unit_payload, "\"_artifact_owner_group_id\":7001") != null);

    try std.testing.expectError(error.NotFound, db.reprocessDocumentArtifact(alloc, "doc:missing", "document_units_v1"));
    try std.testing.expect((try db.getDocumentArtifactManifest(alloc, "doc:missing", "document_units_v1")) == null);
    var missing_list = try db.listDocumentArtifactManifests(alloc, "doc:missing");
    defer missing_list.deinit(alloc);
    try std.testing.expectEqualStrings("doc:missing", missing_list.document_id);
    try std.testing.expectEqual(@as(usize, 0), missing_list.artifacts.len);

    try db.batch(.{
        .writes = &.{.{
            .key = "doc:b",
            .value = "{\"url\":\"data:text/plain;base64,ZGVsdGEgZXBzaWxvbg==\"}",
        }},
        .sync_level = .full_index,
    });

    var range_first = try db.reprocessDocumentArtifactRange(alloc, "document_units_v1", .{ .limit = 1 });
    defer range_first.deinit(alloc);
    try std.testing.expectEqual(@as(usize, 1), range_first.scanned);
    try std.testing.expectEqual(@as(usize, 1), range_first.reprocessed);
    try std.testing.expectEqual(@as(usize, 0), range_first.failed);
    try std.testing.expectEqual(@as(usize, 0), range_first.failures.len);
    try std.testing.expect(range_first.next_key != null);
    try std.testing.expectEqualStrings("doc:a", range_first.next_key.?);

    var range_after_a = (try db.getDocumentArtifactManifest(alloc, "doc:a", "document_units_v1")) orelse return error.TestUnexpectedResult;
    defer range_after_a.deinit(alloc);
    try std.testing.expectEqual(@as(u64, 3), range_after_a.generation);

    var range_second = try db.reprocessDocumentArtifactRange(alloc, "document_units_v1", .{ .from_key = range_first.next_key.? });
    defer range_second.deinit(alloc);
    try std.testing.expectEqual(@as(usize, 1), range_second.scanned);
    try std.testing.expectEqual(@as(usize, 1), range_second.reprocessed);
    try std.testing.expect(range_second.next_key == null);

    var range_after_b = (try db.getDocumentArtifactManifest(alloc, "doc:b", "document_units_v1")) orelse return error.TestUnexpectedResult;
    defer range_after_b.deinit(alloc);
    try std.testing.expectEqual(@as(u64, 2), range_after_b.generation);
}

test "db generic artifact repair queue reprocesses document asset artifacts" {
    const alloc = std.testing.allocator;

    var path_tmp = try db_mod.TestDirectory.init("db");
    defer path_tmp.cleanup();
    const path = path_tmp.path().ptr;
    defer db_mod.cleanupTempDir(path);

    var db = try DB.open(alloc, std.mem.span(path), .{});
    defer db.close();

    try db.enrichment.addEnrichment(.{
        .name = "document_units_v1",
        .kind = .asset,
        .field = "url",
        .content_type = "application/json",
        .producer_json = "{\"type\":\"document_extraction\",\"config\":{}}",
    });
    try db.batch(.{
        .writes = &.{.{
            .key = "doc:a",
            .value = "{\"url\":\"data:text/plain;base64,YWxwaGE=\"}",
        }},
        .sync_level = .full_index,
    });

    var before = (try db.getDocumentArtifactManifest(alloc, "doc:a", "document_units_v1")) orelse return error.TestUnexpectedResult;
    defer before.deinit(alloc);
    try std.testing.expectEqual(@as(u64, 1), before.generation);

    const manifest_key = try db_mod.internal_keys.artifactNamedPrefixAlloc(alloc, "doc:a", "asset", "document_units_v1");
    defer alloc.free(manifest_key);
    const artifact_key_hex = try db_mod.bytesToHexAlloc(alloc, manifest_key);
    var issue = db_mod.types.ArtifactRepairIssue{
        .artifact_kind = .asset,
        .index_name = try alloc.dupe(u8, "document_units_v1"),
        .doc_key = try alloc.dupe(u8, "doc:a"),
        .artifact_name = try alloc.dupe(u8, "document_units_v1"),
        .artifact_key = artifact_key_hex,
        .reason = .corrupt_artifact,
        .sequence = 1,
    };
    defer issue.deinit(alloc);

    try db.recordArtifactRepairIssue(alloc, issue);

    {
        const issues = try db.listArtifactRepairIssues(alloc, .asset, "document_units_v1", 0);
        defer db_mod.types.freeArtifactRepairIssues(alloc, issues);
        try std.testing.expectEqual(@as(usize, 1), issues.len);
        try std.testing.expectEqual(.asset, issues[0].artifact_kind);
        try std.testing.expectEqual(.corrupt_artifact, issues[0].reason);
        try std.testing.expectEqualStrings("doc:a", issues[0].doc_key);
        try std.testing.expectEqualStrings(artifact_key_hex, issues[0].artifact_key);
    }

    const degraded_stats = try db.stats(alloc);
    defer db_mod.types.freeDBStats(alloc, degraded_stats);
    try std.testing.expect(degraded_stats.repair_degraded);
    try std.testing.expectEqual(@as(u64, 1), degraded_stats.repair_issue_count);
    try std.testing.expectEqual(@as(u32, 0), degraded_stats.index_count);

    try db.core.store.put(manifest_key, "bad-artifact");

    const repair = try db.repairArtifactIssues(alloc, .asset, 10);
    try std.testing.expectEqual(@as(u64, 1), repair.scanned);
    try std.testing.expectEqual(@as(u64, 1), repair.reprocessed);
    try std.testing.expectEqual(@as(u64, 1), repair.repaired);
    try std.testing.expectEqual(@as(u64, 0), repair.failed);

    const issues_after = try db.listArtifactRepairIssues(alloc, .asset, "document_units_v1", 0);
    defer db_mod.types.freeArtifactRepairIssues(alloc, issues_after);
    try std.testing.expectEqual(@as(usize, 0), issues_after.len);

    const repaired_stats = try db.stats(alloc);
    defer db_mod.types.freeDBStats(alloc, repaired_stats);
    try std.testing.expect(!repaired_stats.repair_degraded);
    try std.testing.expectEqual(@as(u64, 0), repaired_stats.repair_issue_count);

    var after = (try db.getDocumentArtifactManifest(alloc, "doc:a", "document_units_v1")) orelse return error.TestUnexpectedResult;
    defer after.deinit(alloc);
    try std.testing.expectEqual(@as(u64, 1), after.generation);
    try std.testing.expect(after.manifest_json.len > 0);
}

test "db generic artifact repair queue regenerates set-valued chunk artifacts" {
    const alloc = std.testing.allocator;

    var path_tmp = try db_mod.TestDirectory.init("db");
    defer path_tmp.cleanup();
    const path = path_tmp.path().ptr;
    defer db_mod.cleanupTempDir(path);

    var db = try DB.open(alloc, std.mem.span(path), .{});
    defer db.close();

    try db.enrichment.addEnrichment(.{
        .name = "body_chunks_v1",
        .kind = .chunk,
        .field = "body",
        .chunk_size = 8,
    });
    try db.batch(.{
        .writes = &.{.{
            .key = "doc:a",
            // Empty input intentionally produces no physical chunk. Successful
            // regeneration of this empty set must still retire durable debt.
            .value = "{\"body\":\"\"}",
        }},
        .sync_level = .full_index,
    });

    const chunk_prefix = try db_mod.internal_keys.artifactNamedPrefixAlloc(alloc, "doc:a", "chunk", "body_chunks_v1");
    defer alloc.free(chunk_prefix);
    const artifact_key_hex = try db_mod.bytesToHexAlloc(alloc, chunk_prefix);
    var issue = db_mod.types.ArtifactRepairIssue{
        .artifact_kind = .chunk,
        .doc_key = try alloc.dupe(u8, "doc:a"),
        .artifact_name = try alloc.dupe(u8, "body_chunks_v1"),
        .artifact_key = artifact_key_hex,
        .reason = .enrichment_failed,
        .sequence = 1,
    };
    defer issue.deinit(alloc);
    try db.recordArtifactRepairIssue(alloc, issue);

    const repair = try db.repairArtifactIssues(alloc, .chunk, 10);
    try std.testing.expectEqual(@as(u64, 1), repair.scanned);
    try std.testing.expectEqual(@as(u64, 1), repair.reprocessed);
    try std.testing.expectEqual(@as(u64, 1), repair.repaired);
    try std.testing.expectEqual(@as(u64, 0), repair.failed);
    try std.testing.expect(!repair.debt_remaining);

    const issues_after = try db.listArtifactRepairIssues(alloc, .chunk, null, 0);
    defer db_mod.types.freeArtifactRepairIssues(alloc, issues_after);
    try std.testing.expectEqual(@as(usize, 0), issues_after.len);
}

test "db index repair replacement loads named artifact producer dependencies" {
    const alloc = std.testing.allocator;
    var path_tmp = try db_mod.TestDirectory.init("db");
    defer path_tmp.cleanup();
    const path = path_tmp.path().ptr;
    defer db_mod.cleanupTempDir(path);
    var db = try DB.open(alloc, std.mem.span(path), .{
        .start_index_workers = false,
        .start_optional_runtime_workers = false,
        .ttl_cleanup = .{ .enabled = false },
    });
    defer db.close();
    try db.enrichment.addEnrichment(.{
        .name = "body_chunks",
        .kind = .chunk,
        .field = "body",
        .chunk_size = 64,
    });
    for ([_]db_mod.types.IndexKind{ .dense_vector, .sparse_vector }) |kind| {
        const dense = kind == .dense_vector;
        const source_name = if (dense) "dense_source" else "sparse_source";
        try db.enrichment.addEnrichment(.{
            .name = source_name,
            .kind = .embedding,
            .field = "text",
            .source_artifact_name = "body_chunks",
            .expected_dims = if (dense) 3 else 0,
            .producer_json = if (dense)
                "{\"version\":1,\"provider\":\"antfly\",\"model\":\"dense-test\",\"dimensions\":3}"
            else
                "{\"version\":1,\"provider\":\"antfly\",\"model\":\"sparse-test\"}",
        });
        const cfg = db_mod.types.IndexConfig{
            .name = if (dense) "dense_idx" else "sparse_idx",
            .kind = kind,
            .config_json = if (dense)
                "{\"field\":\"embedding\",\"dims\":3,\"embedding_name\":\"dense_source\"}"
            else
                "{\"field\":\"sparse_embedding\",\"embedding_name\":\"sparse_source\"}",
        };
        try db.addIndex(cfg);
        const canonical = db.core.index_manager.get(cfg.name).?;
        const shadow_path = try std.fmt.allocPrint(alloc, "{s}/replacement-{s}", .{ std.mem.span(path), cfg.name });
        defer alloc.free(shadow_path);
        var shadow = try db_mod.index_manager_mod.IndexManager.initWithOptions(alloc, shadow_path, db.index_backends);
        defer shadow.deinit();
        shadow.setIo(db.backend_runtime.io());
        // A replacement shares the durable producer contracts, but must not
        // reopen the canonical indexes or recreate/persist their producers.
        var missing_source = canonical.*;
        missing_source.config_json = if (dense)
            "{\"field\":\"embedding\",\"dims\":3,\"embedding_name\":\"missing_source\"}"
        else
            "{\"field\":\"sparse_embedding\",\"embedding_name\":\"missing_source\"}";
        try std.testing.expectError(error.InvalidIndexConfig, shadow.registerReplacementIndex(db.core.store, missing_source));
        try shadow.registerReplacementIndex(db.core.store, canonical.*);
        const producer = shadow.getEnrichment(.embedding, source_name) orelse return error.TestUnexpectedResult;
        try std.testing.expectEqualStrings("body_chunks", producer.source_artifact_name);
        try std.testing.expectEqualStrings("text", producer.source_field);
        const chunk = shadow.getEnrichment(.chunk, "body_chunks") orelse return error.TestUnexpectedResult;
        try std.testing.expectEqualStrings("body", chunk.source_field);
        try std.testing.expectEqual(@as(u32, 64), chunk.chunk_size);
    }
}

test "db document extraction failure preserves last-known-good children and recovers" {
    const alloc = std.testing.allocator;

    var path_tmp = try db_mod.TestDirectory.init("db");
    defer path_tmp.cleanup();
    const path = path_tmp.path().ptr;
    defer db_mod.cleanupTempDir(path);

    var db = try DB.open(alloc, std.mem.span(path), .{});
    defer db.close();

    try db.enrichment.addEnrichment(.{
        .name = "document_units_v1",
        .kind = .asset,
        .field = "url",
        .content_type = "application/json",
        .producer_json = "{\"type\":\"document_extraction\",\"config\":{}}",
    });

    try db.batch(.{
        .writes = &.{.{
            .key = "doc:a",
            .value = "{\"url\":\"data:text/plain;base64,YWxwaGE=\"}",
        }},
        .sync_level = .full_index,
    });

    var before = (try db.getDocumentArtifactManifest(alloc, "doc:a", "document_units_v1")) orelse return error.TestUnexpectedResult;
    defer before.deinit(alloc);
    try std.testing.expectEqualStrings("text", before.route_type);
    try std.testing.expect(before.last_error_code == null);
    try std.testing.expectEqual(@as(usize, 1), before.child_ranges.len);
    const stale_unit_key = try alloc.dupe(u8, before.child_ranges[0].start_key);
    defer alloc.free(stale_unit_key);
    {
        const stale_unit = try db.core.store.get(alloc, stale_unit_key);
        defer alloc.free(stale_unit);
    }

    try db.batch(.{
        .writes = &.{.{
            .key = "doc:a",
            .value = "{\"url\":\"data:text/plain;base64\"}",
        }},
        .sync_level = .full_index,
    });

    var after = (try db.getDocumentArtifactManifest(alloc, "doc:a", "document_units_v1")) orelse return error.TestUnexpectedResult;
    defer after.deinit(alloc);
    try std.testing.expectEqual(@as(u64, before.generation + 1), after.generation);
    try std.testing.expectEqualStrings("error", after.route_type);
    try std.testing.expectEqualStrings("failed", after.merge_status);
    try std.testing.expectEqual(@as(usize, 1), after.unit_count);
    try std.testing.expectEqual(@as(usize, 1), after.child_range_count);
    try std.testing.expect(after.state_json != null);
    try std.testing.expect(after.last_error_code != null);
    try std.testing.expectEqualStrings("InvalidDataUri", after.last_error_code.?);
    try std.testing.expect(after.last_error_message != null);
    try std.testing.expectEqualStrings("remote content download failed", after.last_error_message.?);
    try std.testing.expect(std.mem.indexOf(u8, after.manifest_json, "\"last_error\"") != null);
    {
        const retained_unit = try db.core.store.get(alloc, stale_unit_key);
        defer alloc.free(retained_unit);
    }

    var list = try db.listDocumentArtifactManifests(alloc, "doc:a");
    defer list.deinit(alloc);
    try std.testing.expectEqual(@as(usize, 1), list.artifacts.len);
    try std.testing.expectEqualStrings("InvalidDataUri", list.artifacts[0].last_error_code.?);

    // A failure manifest must remain eligible for repair even when the restored
    // source matches the retained state fingerprint.
    try db.batch(.{
        .writes = &.{.{
            .key = "doc:a",
            .value = "{\"url\":\"data:text/plain;base64,YWxwaGE=\"}",
        }},
        .sync_level = .full_index,
    });

    var repaired = (try db.getDocumentArtifactManifest(alloc, "doc:a", "document_units_v1")) orelse return error.TestUnexpectedResult;
    defer repaired.deinit(alloc);
    try std.testing.expectEqual(@as(u64, after.generation + 1), repaired.generation);
    try std.testing.expectEqualStrings("text", repaired.route_type);
    try std.testing.expectEqualStrings("converged", repaired.merge_status);
    try std.testing.expectEqual(@as(usize, 1), repaired.unit_count);
    try std.testing.expect(repaired.last_error_code == null);
}

test "db document extraction skips stable unit local rewrites while replaying full text from stored artifacts" {
    const alloc = std.testing.allocator;

    var path_tmp = try db_mod.TestDirectory.init("db");
    defer path_tmp.cleanup();
    const path = path_tmp.path().ptr;
    defer db_mod.cleanupTempDir(path);

    var counting = db_mod.CountingDenseEmbedder{};
    var db = try DB.open(alloc, std.mem.span(path), .{
        .enrichment = .{
            .owner_id = "worker-a",
            .dense_embedder = counting.interface(),
        },
    });
    defer db.close();

    try db.enrichment.addEnrichment(.{
        .name = "document_units_v1",
        .kind = .asset,
        .field = "url",
        .content_type = "application/json",
        .producer_json = "{\"type\":\"document_extraction\",\"config\":{}}",
    });
    try db.enrichment.addEnrichment(.{
        .name = "document_chunks_v1",
        .kind = .chunk,
        .field = "text",
        .source_artifact_name = "document_units_v1",
        .chunk_size = 256,
    });
    try db.enrichment.addEnrichment(.{
        .name = "document_chunk_dense_v1",
        .kind = .embedding,
        .field = "text",
        .source_artifact_name = "document_chunks_v1",
        .expected_dims = 3,
    });
    try db.addIndex(.{
        .name = "ft_document_units",
        .kind = .full_text,
        .config_json = "{\"field\":\"text\",\"sources\":[{\"artifact\":\"document_units_v1\"}]}",
    });
    try db.addIndex(.{
        .name = "ft_document_chunks",
        .kind = .full_text,
        .config_json = "{\"chunk_name\":\"document_chunks_v1\"}",
    });
    try db.addIndex(.{
        .name = "dv_document_chunks",
        .kind = .dense_vector,
        .config_json = "{\"field\":\"embedding\",\"dims\":3,\"embedding_name\":\"document_chunk_dense_v1\"}",
    });
    const chunk_vector_indexes = try db.core.index_manager.vectorIndexesForChunk(alloc, "document_chunks_v1");
    defer {
        for (chunk_vector_indexes) |index_name| alloc.free(index_name);
        alloc.free(chunk_vector_indexes);
    }
    try std.testing.expectEqual(@as(usize, 1), chunk_vector_indexes.len);
    try std.testing.expectEqualStrings("dv_document_chunks", chunk_vector_indexes[0]);
    const asset_vector_indexes = try db.core.index_manager.vectorIndexesDependingOnArtifact(alloc, "document_units_v1");
    defer {
        for (asset_vector_indexes) |index_name| alloc.free(index_name);
        alloc.free(asset_vector_indexes);
    }
    try std.testing.expectEqual(@as(usize, 1), asset_vector_indexes.len);
    try std.testing.expectEqualStrings("dv_document_chunks", asset_vector_indexes[0]);
    const asset_consumers = try db.core.index_manager.indexesDependingOnArtifact(alloc, "document_units_v1");
    defer {
        for (asset_consumers) |index_name| alloc.free(index_name);
        alloc.free(asset_consumers);
    }
    try std.testing.expectEqual(@as(usize, 3), asset_consumers.len);
    var found_units_text = false;
    var found_chunks_text = false;
    var found_chunks_dense = false;
    for (asset_consumers) |index_name| {
        found_units_text = found_units_text or std.mem.eql(u8, index_name, "ft_document_units");
        found_chunks_text = found_chunks_text or std.mem.eql(u8, index_name, "ft_document_chunks");
        found_chunks_dense = found_chunks_dense or std.mem.eql(u8, index_name, "dv_document_chunks");
    }
    try std.testing.expect(found_units_text and found_chunks_text and found_chunks_dense);

    const first_value = "{\"url\":\"data:text/plain;base64,YWxwaGEgYmV0YSBnYW1tYQ==\"}";
    try db.batch(.{
        .writes = &.{.{ .key = "doc:a", .value = first_value }},
        .sync_level = .full_index,
    });
    try db.runUntilIdle();
    const first_calls = counting.calls;
    try std.testing.expectEqual(@as(usize, 1), first_calls);

    const second_value = "{\"url\":\"data:text/plain,alpha%20beta%20gamma\"}";
    var extracted = [_]db_mod.mapper.ExtractedWrite{try db_mod.mapper.extractWrite(alloc, "doc:a", second_value)};
    defer extracted[0].deinit(alloc);
    var precomputed = try db_mod.prepareGeneratedEnrichments(&db, alloc, .{
        .writes = &.{.{ .key = "doc:a", .value = second_value }},
    }, &extracted, .all, &.{}, null, null);
    defer precomputed.deinit(alloc);

    const unit_key = try db_mod.internal_keys.documentUnitArtifactKeyAlloc(alloc, "doc:a", "document_units_v1", "document:000001");
    defer alloc.free(unit_key);
    const chunk_key = try db_mod.internal_keys.documentUnitChunkArtifactKeyAlloc(alloc, "doc:a", "document_chunks_v1", "document:000001", 0);
    defer alloc.free(chunk_key);

    try std.testing.expectEqual(first_calls, counting.calls);
    try std.testing.expectEqual(@as(usize, 2), precomputed.documents.len);
    var saw_unit_doc = false;
    var saw_chunk_doc = false;
    for (precomputed.documents) |doc| {
        try std.testing.expectEqual(@as(?[]const u8, null), doc.cleaned_value);
        if (std.mem.eql(u8, doc.key, unit_key)) saw_unit_doc = true;
        if (std.mem.eql(u8, doc.key, chunk_key)) saw_chunk_doc = true;
    }
    try std.testing.expect(saw_unit_doc);
    try std.testing.expect(saw_chunk_doc);

    try db.batch(.{
        .writes = &.{.{ .key = "doc:a", .value = second_value }},
        .sync_level = .full_index,
    });
    try std.testing.expectEqual(first_calls, counting.calls);

    // A full-index merge precomputes this successful empty extraction before
    // generated replay. The replay's stable-state shortcut must still account
    // for the source document so artifact-backed vector coverage can converge.
    try db.batch(.{
        .writes = &.{.{ .key = "doc:empty", .value = "{\"url\":\"data:text/plain,\"}" }},
        .sync_level = .full_index,
    });
    try db.runUntilIdle();
    const coverage_stats = try db.stats(alloc);
    defer db_mod.types.freeDBStats(alloc, coverage_stats);
    var saw_vector_coverage = false;
    for (coverage_stats.indexes) |index_stats| {
        if (!std.mem.eql(u8, index_stats.name, "dv_document_chunks")) continue;
        saw_vector_coverage = true;
        try std.testing.expect(index_stats.coverage_summary_ready);
        try std.testing.expectEqual(@as(u64, 1), index_stats.coverage_produced_count);
        try std.testing.expectEqual(@as(u64, 1), index_stats.coverage_skipped_count);
        try std.testing.expectEqual(@as(u64, 0), index_stats.coverage_terminal_failed_count);
    }
    try std.testing.expect(saw_vector_coverage);

    var unit_result = try db.search(alloc, .{
        .index_name = "ft_document_units",
        .full_text = .{ .match = .{ .field = "text", .text = "gamma" } },
        .return_mode = .unit,
    });
    defer unit_result.deinit();
    try std.testing.expectEqual(@as(u32, 1), unit_result.total_hits);
    try std.testing.expectEqualStrings("doc:a", unit_result.hits[0].artifact_ref.?.document_id);
    try std.testing.expectEqualStrings("document_units_v1", unit_result.hits[0].artifact_ref.?.name);
    try std.testing.expectEqualStrings("document:000001", unit_result.hits[0].artifact_ref.?.unit_id.?);

    var all_unit_members = try db.search(alloc, .{
        .index_name = "ft_document_units",
        .full_text = .{ .match_all = {} },
        .return_mode = .chunk,
    });
    defer all_unit_members.deinit();
    try std.testing.expectEqual(@as(u32, 2), all_unit_members.total_hits);
    for (all_unit_members.hits) |hit| {
        try std.testing.expectEqual(db_mod.types.ArtifactKind.asset, hit.artifact_ref.?.kind);
        try std.testing.expect(hit.artifact_ref.?.unit_id != null);
    }

    // The artifact contains operational strings such as unit_type in addition
    // to text. A selective artifact index must not leak those fields into its
    // postings, including through the schema-less _all projection.
    var excluded_metadata = try db.search(alloc, .{
        .index_name = "ft_document_units",
        .full_text = .{ .match = .{ .field = "unit_type", .text = "document" } },
        .return_mode = .chunk,
    });
    defer excluded_metadata.deinit();
    try std.testing.expectEqual(@as(u32, 0), excluded_metadata.total_hits);

    var chunk_result = try db.search(alloc, .{
        .index_name = "ft_document_chunks",
        .full_text = .{ .match = .{ .field = "text", .text = "gamma" } },
        .return_mode = .chunk,
    });
    defer chunk_result.deinit();
    try std.testing.expectEqual(@as(u32, 1), chunk_result.total_hits);

    var chunk_unit_result = try db.search(alloc, .{
        .index_name = "ft_document_chunks",
        .full_text = .{ .match = .{ .field = "text", .text = "gamma" } },
        .return_mode = .unit,
    });
    defer chunk_unit_result.deinit();
    try std.testing.expectEqual(@as(u32, 1), chunk_unit_result.total_hits);
    try std.testing.expectEqualStrings("doc:a", chunk_unit_result.hits[0].artifact_ref.?.document_id);
    try std.testing.expectEqualStrings("document_units_v1", chunk_unit_result.hits[0].artifact_ref.?.name);
    try std.testing.expectEqualStrings("document:000001", chunk_unit_result.hits[0].artifact_ref.?.unit_id.?);
}

test "db document extraction chunks units through source artifact enrichment" {
    const alloc = std.testing.allocator;

    var path_tmp = try db_mod.TestDirectory.init("db");
    defer path_tmp.cleanup();
    const path = path_tmp.path().ptr;
    defer db_mod.cleanupTempDir(path);

    var deterministic_dense = db_mod.embedder_mod.DeterministicDenseEmbedder{};
    var deterministic_sparse = db_mod.embedder_mod.DeterministicSparseEmbedder{};
    var db = try DB.open(alloc, std.mem.span(path), .{
        .enrichment = .{
            .owner_id = "worker-a",
            .dense_embedder = deterministic_dense.interface(),
            .sparse_embedder = deterministic_sparse.interface(),
        },
    });
    defer db.close();

    try db.enrichment.addEnrichment(.{
        .name = "document_units_v1",
        .kind = .asset,
        .field = "url",
        .content_type = "application/json",
        .producer_json = "{\"type\":\"document_extraction\",\"config\":{}}",
    });
    try db.enrichment.addEnrichment(.{
        .name = "document_chunks_v1",
        .kind = .chunk,
        .field = "text",
        .source_artifact_name = "document_units_v1",
        .chunk_size = 256,
    });
    try db.enrichment.addEnrichment(.{
        .name = "document_chunk_dense_v1",
        .kind = .embedding,
        .field = "text",
        .source_artifact_name = "document_chunks_v1",
        .expected_dims = 3,
    });
    try db.enrichment.addEnrichment(.{
        .name = "document_chunk_caption_dense_v1",
        .kind = .embedding,
        .field = "caption",
        .source_artifact_name = "document_chunks_v1",
        .expected_dims = 3,
    });
    try db.enrichment.addEnrichment(.{
        .name = "document_chunk_sparse_v1",
        .kind = .embedding,
        .field = "text",
        .source_artifact_name = "document_chunks_v1",
    });
    try db.addIndex(.{
        .name = "ft_document_chunks",
        .kind = .full_text,
        .config_json = "{\"chunk_name\":\"document_chunks_v1\"}",
    });
    try db.addIndex(.{
        .name = "document_vectors",
        .kind = .dense_vector,
        .config_json = "{\"field\":\"embedding\",\"dims\":3,\"embedding_name\":\"document_chunk_dense_v1\"}",
    });
    try db.addIndex(.{
        .name = "document_caption_vectors",
        .kind = .dense_vector,
        .config_json = "{\"field\":\"embedding\",\"dims\":3,\"embedding_name\":\"document_chunk_caption_dense_v1\"}",
    });
    try db.addIndex(.{
        .name = "document_chunk_sparse_v1",
        .kind = .sparse_vector,
        .config_json = "{\"field\":\"sparse_embedding\",\"embedding_name\":\"document_chunk_sparse_v1\"}",
    });
    const embedding_cfg = db.core.index_manager.getEnrichment(.embedding, "document_chunk_dense_v1") orelse return error.TestUnexpectedResult;
    try std.testing.expectEqualStrings("document_chunks_v1", embedding_cfg.source_artifact_name);
    try std.testing.expectEqual(@as(u32, 3), embedding_cfg.expected_dims);
    const dense_consumers = try db.core.index_manager.denseIndexesForEmbedding(alloc, "document_chunk_dense_v1", 3);
    defer {
        for (dense_consumers) |name| alloc.free(name);
        alloc.free(dense_consumers);
    }
    try std.testing.expectEqual(@as(usize, 1), dense_consumers.len);
    try std.testing.expectEqualStrings("document_vectors", dense_consumers[0]);
    const sparse_consumers = try db.core.index_manager.sparseIndexesForEmbedding(alloc, "document_chunk_sparse_v1");
    defer {
        for (sparse_consumers) |name| alloc.free(name);
        alloc.free(sparse_consumers);
    }
    try std.testing.expectEqual(@as(usize, 1), sparse_consumers.len);
    try std.testing.expectEqualStrings("document_chunk_sparse_v1", sparse_consumers[0]);

    const planned = try db.core.planGeneratedEnrichments(
        alloc,
        "doc:planned",
        "{\"url\":\"data:text/plain;base64,YWxwaGEgYmV0YSBnYW1tYQ==\",\"text\":\"source document decoy\"}",
        &.{},
        &.{},
    );
    defer db_mod.enrichment_types.deinitGeneratedRequests(alloc, planned);
    var saw_document_asset = false;
    var saw_document_chunk_dense = false;
    var saw_document_chunk_caption_dense = false;
    var saw_document_chunk_sparse = false;
    for (planned) |request| {
        if (request.kind == .asset and std.mem.eql(u8, request.artifact_name, "document_units_v1")) saw_document_asset = true;
        if (request.kind == .dense_embedding and
            std.mem.eql(u8, request.artifact_name, "document_chunks_v1") and
            std.mem.eql(u8, request.embedding_name, "document_chunk_dense_v1") and
            request.chunk_size == 256)
        {
            saw_document_chunk_dense = true;
        }
        if (request.kind == .dense_embedding and
            std.mem.eql(u8, request.artifact_name, "document_chunks_v1") and
            std.mem.eql(u8, request.embedding_name, "document_chunk_caption_dense_v1") and
            request.chunk_size == 256)
        {
            saw_document_chunk_caption_dense = true;
        }
        if (request.kind == .sparse_embedding and
            std.mem.eql(u8, request.artifact_name, "document_chunks_v1") and
            std.mem.eql(u8, request.embedding_name, "document_chunk_sparse_v1") and
            request.chunk_size == 256)
        {
            saw_document_chunk_sparse = true;
        }
    }
    try std.testing.expect(saw_document_asset);
    try std.testing.expect(saw_document_chunk_dense);
    try std.testing.expect(saw_document_chunk_caption_dense);
    try std.testing.expect(saw_document_chunk_sparse);

    try db.batch(.{
        .writes = &.{.{
            .key = "doc:a",
            .value = "{\"url\":\"data:text/plain;base64,YWxwaGEgYmV0YSBnYW1tYQ==\",\"text\":\"source document decoy\"}",
        }},
        .sync_level = .full_index,
    });

    const unit_key = try db_mod.internal_keys.documentUnitArtifactKeyAlloc(alloc, "doc:a", "document_units_v1", "document:000001");
    defer alloc.free(unit_key);
    const chunk_key = try db_mod.internal_keys.documentUnitChunkArtifactKeyAlloc(alloc, "doc:a", "document_chunks_v1", "document:000001", 0);
    defer alloc.free(chunk_key);
    const state_key = try db_mod.assetStateKeyAlloc(alloc, "doc:a", "document_units_v1");
    defer alloc.free(state_key);
    const manifest_key = try db_mod.internal_keys.artifactNamedPrefixAlloc(alloc, "doc:a", "asset", "document_units_v1");
    defer alloc.free(manifest_key);

    const chunk_payload = try db.core.store.get(alloc, chunk_key);
    defer alloc.free(chunk_payload);
    const unit_payload = try db.core.store.get(alloc, unit_key);
    defer alloc.free(unit_payload);
    var parsed_unit_payload = try std.json.parseFromSlice(std.json.Value, alloc, unit_payload, .{});
    defer parsed_unit_payload.deinit();
    try std.testing.expect(std.mem.indexOf(u8, chunk_payload, "\"_parent_doc_key\":\"doc:a\"") != null);
    try std.testing.expect(std.mem.indexOf(u8, chunk_payload, "\"_parent_unit_id\":\"document:000001\"") != null);
    try std.testing.expect(std.mem.indexOf(u8, chunk_payload, "\"_artifact_name\":\"document_chunks_v1\"") != null);
    try std.testing.expect(std.mem.indexOf(u8, chunk_payload, "\"_source_artifact_name\":\"document_units_v1\"") != null);
    try std.testing.expect(std.mem.indexOf(u8, chunk_payload, "\"text\":\"alpha beta gamma\"") != null);
    var parsed_chunk_payload = try std.json.parseFromSlice(std.json.Value, alloc, chunk_payload, .{});
    defer parsed_chunk_payload.deinit();
    try std.testing.expectEqualStrings("range:000001", parsed_chunk_payload.value.object.get("_artifact_range_id").?.string);
    try std.testing.expectEqualStrings("chunk", parsed_chunk_payload.value.object.get("_artifact_range_kind").?.string);
    try std.testing.expectEqualStrings("local_committed", parsed_chunk_payload.value.object.get("_artifact_route_status").?.string);
    try std.testing.expectEqual(@as(i64, 0), parsed_chunk_payload.value.object.get("_artifact_owner_group_id").?.integer);
    try std.testing.expectEqualStrings(
        parsed_unit_payload.value.object.get(db_mod.hierarchy_navigation_unit_fingerprint_field).?.string,
        parsed_chunk_payload.value.object.get(db_mod.hierarchy_navigation_unit_fingerprint_field).?.string,
    );
    const chunk_provenance = parsed_chunk_payload.value.object.get("provenance").?.object;
    try std.testing.expectEqualStrings("unit", chunk_provenance.get("offset_basis").?.string);
    try std.testing.expectEqualStrings("document:000001", chunk_provenance.get("parent_unit_id").?.string);
    try std.testing.expectEqualStrings("document_units_v1", chunk_provenance.get("source_artifact_name").?.string);
    try std.testing.expectEqual(@as(i64, 0), chunk_provenance.get("unit_char_start").?.integer);
    try std.testing.expectEqual(@as(i64, 16), chunk_provenance.get("unit_char_end").?.integer);
    try std.testing.expectEqual(@as(i64, 0), chunk_provenance.get("document_char_start").?.integer);
    try std.testing.expectEqual(@as(i64, 16), chunk_provenance.get("document_char_end").?.integer);

    const dense_artifact_key = try db_mod.internal_keys.derivedEmbeddingArtifactKeyAlloc(alloc, chunk_key, "document_chunk_dense_v1");
    defer alloc.free(dense_artifact_key);
    const dense_artifact_payload = try db.core.store.get(alloc, dense_artifact_key);
    defer alloc.free(dense_artifact_payload);
    const sparse_artifact_key = try db_mod.internal_keys.derivedEmbeddingArtifactKeyAlloc(alloc, chunk_key, "document_chunk_sparse_v1");
    defer alloc.free(sparse_artifact_key);
    const sparse_artifact_payload = try db.core.store.get(alloc, sparse_artifact_key);
    defer alloc.free(sparse_artifact_payload);

    const state = try db.core.store.get(alloc, state_key);
    defer alloc.free(state);
    try std.testing.expect(std.mem.indexOf(u8, state, "\"chunk_keys\"") != null);
    try std.testing.expect(std.mem.indexOf(u8, state, "\"unit_descriptors\"") != null);
    try std.testing.expect(std.mem.indexOf(u8, state, "\"fingerprint\"") != null);
    try db_mod.ant_json.testing.expectSubsetJsonText(
        alloc,
        "{\"chunk_unit_fingerprint_version\":2}",
        state,
    );

    const manifest = try db.core.store.get(alloc, manifest_key);
    defer alloc.free(manifest);
    try std.testing.expect(std.mem.indexOf(u8, manifest, "\"manifest_version\":2") != null);
    try std.testing.expect(std.mem.indexOf(u8, manifest, "\"generation\":1") != null);
    try std.testing.expect(std.mem.indexOf(u8, manifest, "\"child_ranges\"") != null);
    try std.testing.expect(std.mem.indexOf(u8, manifest, "\"range_kind\":\"unit\"") != null);
    try std.testing.expect(std.mem.indexOf(u8, manifest, "\"range_kind\":\"chunk\"") != null);
    try std.testing.expect(std.mem.indexOf(u8, manifest, "\"merge_plan\"") != null);
    try std.testing.expect(std.mem.indexOf(u8, manifest, "\"from_generation\":0") != null);
    try std.testing.expect(std.mem.indexOf(u8, manifest, "\"to_generation\":1") != null);
    try std.testing.expect(std.mem.indexOf(u8, manifest, "\"operation_granularity\":\"unit_fingerprint\"") != null);

    var result = try db.search(alloc, .{
        .full_text = .{ .match = .{ .field = "text", .text = "gamma" } },
    });
    defer result.deinit();
    try std.testing.expectEqual(@as(u32, 1), result.total_hits);
    try std.testing.expectEqualStrings("doc:a", result.hits[0].id);

    var parent_with_chunks = try db.search(alloc, .{
        .index_name = "ft_document_chunks",
        .full_text = .{ .match = .{ .field = "text", .text = "gamma" } },
        .return_mode = .parent_with_chunks,
        .include_stored = false,
    });
    defer parent_with_chunks.deinit();
    try std.testing.expectEqual(@as(u32, 1), parent_with_chunks.total_hits);
    try std.testing.expectEqualStrings("doc:a", parent_with_chunks.hits[0].id);
    try std.testing.expectEqual(@as(usize, 1), parent_with_chunks.hits[0].chunk_hits.len);
    var chunk_public_id_for_rollup = try db_mod.artifact_ids.resolvePublicHitIdentityAlloc(alloc, chunk_key);
    defer chunk_public_id_for_rollup.deinit(alloc);
    try std.testing.expectEqualStrings(chunk_public_id_for_rollup.id, parent_with_chunks.hits[0].chunk_hits[0].id);
    const rollup_chunk_ref = parent_with_chunks.hits[0].chunk_hits[0].artifact_ref orelse return error.TestUnexpectedResult;
    try std.testing.expectEqualStrings("doc:a", rollup_chunk_ref.document_id);
    try std.testing.expectEqualStrings("document_chunks_v1", rollup_chunk_ref.name);
    try std.testing.expectEqual(db_mod.types.ArtifactKind.chunk, rollup_chunk_ref.kind);
    try std.testing.expectEqual(@as(?u32, 0), rollup_chunk_ref.chunk_id);
    try std.testing.expectEqualStrings("document:000001", rollup_chunk_ref.unit_id.?);

    const query_vec = try deterministic_dense.interface().embedDense(alloc, "document_chunk_dense_v1", "alpha beta gamma", 3);
    defer alloc.free(query_vec);
    const dense_index = db.core.index_manager.denseIndex("document_vectors") orelse return error.IndexNotFound;
    var direct = try db_mod.waitForDenseIndexResultsWithAttempts(dense_index.index, query_vec, 3, 1, db_mod.slow_test_wait_attempts);
    defer direct.deinit();
    const dense_internal_id = if (direct.takeMetadata(0)) |metadata|
        metadata
    else blk: {
        const hit = direct.getHits()[0];
        break :blk (try dense_index.index.getMetadata(hit.vector_id)) orelse return error.TestUnexpectedResult;
    };
    defer alloc.free(dense_internal_id);
    try std.testing.expectEqualStrings(chunk_key, dense_internal_id);

    var semantic_rollup = try db.search(alloc, .{
        .index_name = "document_vectors",
        .dense = .{ .vector = query_vec, .k = 3 },
        .return_mode = .parent_with_chunks,
        .limit = 1,
        .include_stored = false,
    });
    defer semantic_rollup.deinit();
    try std.testing.expectEqual(@as(u32, 1), semantic_rollup.total_hits);
    try std.testing.expectEqualStrings("doc:a", semantic_rollup.hits[0].id);
    try std.testing.expectEqual(@as(usize, 1), semantic_rollup.hits[0].chunk_hits.len);
    try std.testing.expectEqualStrings(chunk_public_id_for_rollup.id, semantic_rollup.hits[0].chunk_hits[0].id);
    const semantic_chunk_ref = semantic_rollup.hits[0].chunk_hits[0].artifact_ref orelse return error.TestUnexpectedResult;
    try std.testing.expectEqualStrings("doc:a", semantic_chunk_ref.document_id);
    try std.testing.expectEqualStrings("document_chunks_v1", semantic_chunk_ref.name);
    try std.testing.expectEqual(db_mod.types.ArtifactKind.chunk, semantic_chunk_ref.kind);
    try std.testing.expectEqual(@as(?u32, 0), semantic_chunk_ref.chunk_id);
    try std.testing.expectEqualStrings("document:000001", semantic_chunk_ref.unit_id.?);

    var sparse_query = try deterministic_sparse.interface().embedSparse(alloc, "document_chunk_sparse_v1", "alpha beta gamma");
    defer sparse_query.deinit(alloc);
    var sparse_result = try db.search(alloc, .{
        .index_name = "document_chunk_sparse_v1",
        .query = .{ .sparse_knn = .{
            .indices = sparse_query.indices,
            .values = sparse_query.values,
            .k = 3,
        } },
        .return_mode = .chunk,
        .limit = 1,
        .include_stored = false,
        .hierarchy_include_source = true,
        .hierarchy_include_unit = true,
    });
    defer sparse_result.deinit();
    try std.testing.expectEqual(@as(u32, 1), sparse_result.total_hits);
    var chunk_public_id = try db_mod.artifact_ids.resolvePublicHitIdentityAlloc(alloc, chunk_key);
    defer chunk_public_id.deinit(alloc);
    try std.testing.expectEqualStrings(chunk_public_id.id, sparse_result.hits[0].id);
    try std.testing.expect(sparse_result.hits[0].stored_data == null);
    try std.testing.expect(sparse_result.hits[0].ancestor_source_data != null);
    try std.testing.expect(std.mem.indexOf(u8, sparse_result.hits[0].ancestor_source_data.?, "\"url\"") != null);
    try std.testing.expect(sparse_result.hits[0].ancestor_unit_data != null);
    try std.testing.expect(std.mem.indexOf(u8, sparse_result.hits[0].ancestor_unit_data.?, "\"unit_id\":\"document:000001\"") != null);
    try std.testing.expect(std.mem.indexOf(u8, sparse_result.hits[0].ancestor_unit_data.?, "\"text\":\"alpha beta gamma\"") != null);

    var composed_ancestors = try db.search(alloc, .{
        .full_text_queries = &.{.{ .name = "only", .index_name = "ft_document_chunks", .query = .{ .match = .{ .field = "text", .text = "gamma" } } }},
        .return_mode = .member,
        .limit = 1,
        .include_stored = false,
        .hierarchy_include_source = true,
        .hierarchy_include_unit = true,
    });
    defer composed_ancestors.deinit();
    try std.testing.expectEqual(@as(usize, 1), composed_ancestors.hits.len);
    try std.testing.expect(composed_ancestors.hits[0].ancestor_source_data != null);
    try std.testing.expect(composed_ancestors.hits[0].ancestor_unit_data != null);
    try std.testing.expectEqualStrings(sparse_result.hits[0].ancestor_source_data.?, composed_ancestors.hits[0].ancestor_source_data.?);
    try std.testing.expectEqualStrings(sparse_result.hits[0].ancestor_unit_data.?, composed_ancestors.hits[0].ancestor_unit_data.?);

    try db.batch(.{
        .writes = &.{.{
            .key = "doc:a",
            .value = "{\"url\":\"data:text/plain;base64,YWxwaGEgYmV0YSBkZWx0YQ==\",\"text\":\"updated source document decoy\"}",
        }},
        .sync_level = .write,
    });
    try db.waitForCurrentSyncLevel(.full_index);
    const updated_manifest = try db.core.store.get(alloc, manifest_key);
    defer alloc.free(updated_manifest);
    try std.testing.expect(std.mem.indexOf(u8, updated_manifest, "\"generation\":2") != null);
    try std.testing.expect(std.mem.indexOf(u8, updated_manifest, "\"from_generation\":1") != null);
    try std.testing.expect(std.mem.indexOf(u8, updated_manifest, "\"to_generation\":2") != null);
    const updated_chunk_payload = try db.core.store.get(alloc, chunk_key);
    defer alloc.free(updated_chunk_payload);
    try std.testing.expect(std.mem.indexOf(u8, updated_chunk_payload, "\"text\":\"alpha beta delta\"") != null);
    const refreshed_dense_artifact_payload = try db.core.store.get(alloc, dense_artifact_key);
    defer alloc.free(refreshed_dense_artifact_payload);
    const refreshed_sparse_artifact_payload = try db.core.store.get(alloc, sparse_artifact_key);
    defer alloc.free(refreshed_sparse_artifact_payload);

    // A live source/unit must not resurrect an orphaned member when grouped.
    // Presence applies to the raw member before either grouping or hydration.
    try db.core.store.delete(chunk_key);
    for (std.enums.values(db_mod.types.ReturnMode)) |mode| {
        for ([_]bool{ false, true }) |include_stored| {
            var missing = try db.search(alloc, .{
                .index_name = "ft_document_chunks",
                .full_text = .{ .match = .{ .field = "text", .text = "delta" } },
                .return_mode = mode,
                .include_stored = include_stored,
                .limit = 1,
            });
            defer missing.deinit();
            try std.testing.expectEqual(@as(usize, 0), missing.hits.len);
            try std.testing.expectEqual(@as(u32, 0), missing.total_hits);
        }
    }
    var missing_dense = try db.search(alloc, .{ .index_name = "document_vectors", .dense = .{ .vector = query_vec, .k = 1 }, .limit = 1, .include_stored = false });
    defer missing_dense.deinit();
    // Vectors are backed by their embedding artifacts, independently of the
    // optional stored chunk payload used by the full-text index above.
    try std.testing.expectEqual(@as(usize, 1), missing_dense.hits.len);
    var updated_sparse_query = try deterministic_sparse.interface().embedSparse(alloc, "document_chunk_sparse_v1", "alpha beta delta");
    defer updated_sparse_query.deinit(alloc);
    var missing_sparse = try db.search(alloc, .{ .index_name = "document_chunk_sparse_v1", .query = .{ .sparse_knn = .{ .indices = updated_sparse_query.indices, .values = updated_sparse_query.values, .k = 1 } }, .limit = 1, .include_stored = false });
    defer missing_sparse.deinit();
    try std.testing.expectEqual(@as(usize, 1), missing_sparse.hits.len);
    try db.core.store.delete(dense_artifact_key);
    try db.core.store.delete(sparse_artifact_key);
    var orphaned_dense = try db.search(alloc, .{ .index_name = "document_vectors", .dense = .{ .vector = query_vec, .k = 1 }, .limit = 1, .include_stored = false });
    defer orphaned_dense.deinit();
    try std.testing.expectEqual(@as(usize, 0), orphaned_dense.hits.len);
    var orphaned_sparse = try db.search(alloc, .{ .index_name = "document_chunk_sparse_v1", .query = .{ .sparse_knn = .{ .indices = updated_sparse_query.indices, .values = updated_sparse_query.values, .k = 1 } }, .limit = 1, .include_stored = false });
    defer orphaned_sparse.deinit();
    try std.testing.expectEqual(@as(usize, 0), orphaned_sparse.hits.len);

    try db.batch(.{
        .writes = &.{.{
            .key = "doc:a",
            .value = "{\"url\":\"\"}",
        }},
        .sync_level = .full_index,
    });
    try std.testing.expectError(error.NotFound, db.core.store.get(alloc, unit_key));
    try std.testing.expectError(error.NotFound, db.core.store.get(alloc, chunk_key));
    try std.testing.expectError(error.NotFound, db.core.store.get(alloc, dense_artifact_key));
    try std.testing.expectError(error.NotFound, db.core.store.get(alloc, sparse_artifact_key));
    try std.testing.expectEqual(@as(u64, 0), db.core.index_manager.denseIndex("document_vectors").?.index.metadata.active_count);
}

test "db sync_level enrichments precompute for document-mode writes does not hold the apply lock" {
    if (builtin.single_threaded or builtin.os.tag == .freestanding) return error.SkipZigTest;
    const alloc = std.testing.allocator;

    var path_tmp = try db_mod.TestDirectory.init("db");
    defer path_tmp.cleanup();
    const path = path_tmp.path().ptr;
    defer db_mod.cleanupTempDir(path);

    var slow_asset = db_mod.SlowGatedAssetProducer{};
    var db = try DB.open(alloc, std.mem.span(path), .{
        .enrichment = .{
            .owner_id = "worker-a",
            .asset_producer = slow_asset.interface(),
            .inline_retry_max_attempts = 1,
        },
        .ttl_cleanup = .{ .enabled = false },
    });
    defer db.close();
    defer slow_asset.allow();

    try db.enrichment.addEnrichment(.{
        .name = "summary_v1",
        .kind = .asset,
        .field = "body",
        .content_type = "text/plain",
        .producer_json = "{\"type\":\"generator\",\"config\":{\"provider\":\"antfly\",\"model\":\"test-generator\"}}",
    });

    const Writer = struct {
        db: *DB,
        err: ?anyerror = null,

        fn run(self: *@This()) void {
            self.db.batch(.{
                .writes = &.{.{ .key = "doc:a", .value = "{\"body\":\"alpha summary source\"}" }},
                .sync_level = .enrichments,
            }) catch |err| {
                self.err = err;
            };
        }
    };
    var writer = Writer{ .db = &db };
    const writer_thread = try std.Thread.spawn(.{}, Writer.run, .{&writer});
    var joined = false;
    defer if (!joined) {
        slow_asset.allow();
        writer_thread.join();
    };

    // Wait until the asset producer is actually invoked: this batch's
    // generated-enrichment precompute is in flight, ahead of the apply lock.
    var attempts: usize = 0;
    while (attempts < db_mod.slow_test_wait_attempts) : (attempts += 1) {
        if (slow_asset.started.load(.acquire)) break;
        db_mod.sleepPollInterval();
    }
    try std.testing.expect(slow_asset.started.load(.acquire));
    try std.testing.expect(!slow_asset.finished.load(.acquire));

    // A concurrent reader must not queue behind the in-flight precompute.
    // Before this fix, sync_level: .enrichments precompute for document-mode
    // writes ran entirely under the exclusive apply lock, so this probe
    // (which only takes the shared apply lock) would have blocked for as
    // long as the asset producer stayed parked.
    const probe_start_ns = db_mod.monotonicTimeNs();
    _ = db.lsmMaintenanceScore();
    const probe_elapsed_ns = db_mod.monotonicTimeNs() - probe_start_ns;
    try std.testing.expect(!slow_asset.finished.load(.acquire));
    try std.testing.expect(probe_elapsed_ns < 500 * std.time.ns_per_ms);

    slow_asset.allow();
    writer_thread.join();
    joined = true;
    try std.testing.expectEqual(@as(?anyerror, null), writer.err);

    const artifact_key = try db_mod.internal_keys.artifactNamedPrefixAlloc(alloc, "doc:a", "asset", "summary_v1");
    defer alloc.free(artifact_key);
    const artifact_value = try db.core.store.get(alloc, artifact_key);
    defer alloc.free(artifact_value);
    try std.testing.expectEqualStrings("asset:alpha summary source", artifact_value);
}

test "db document extraction concurrent same-key writes converge without stale precompute" {
    if (builtin.single_threaded or builtin.os.tag == .freestanding) return error.SkipZigTest;
    const alloc = std.testing.allocator;

    var path_tmp = try db_mod.TestDirectory.init("db");
    defer path_tmp.cleanup();
    const path = path_tmp.path().ptr;
    defer db_mod.cleanupTempDir(path);

    var deterministic_dense = db_mod.embedder_mod.DeterministicDenseEmbedder{};
    var deterministic_sparse = db_mod.embedder_mod.DeterministicSparseEmbedder{};
    var db = try DB.open(alloc, std.mem.span(path), .{
        .enrichment = .{
            .owner_id = "worker-a",
            .dense_embedder = deterministic_dense.interface(),
            .sparse_embedder = deterministic_sparse.interface(),
        },
    });
    defer db.close();

    try db.enrichment.addEnrichment(.{
        .name = "document_units_v1",
        .kind = .asset,
        .field = "url",
        .content_type = "application/json",
        .producer_json = "{\"type\":\"document_extraction\",\"config\":{}}",
    });
    try db.enrichment.addEnrichment(.{
        .name = "document_chunks_v1",
        .kind = .chunk,
        .field = "text",
        .source_artifact_name = "document_units_v1",
        .chunk_size = 256,
    });
    try db.enrichment.addEnrichment(.{
        .name = "document_chunk_dense_v1",
        .kind = .embedding,
        .field = "text",
        .source_artifact_name = "document_chunks_v1",
        .expected_dims = 3,
    });
    try db.addIndex(.{
        .name = "document_vectors",
        .kind = .dense_vector,
        .config_json = "{\"field\":\"embedding\",\"dims\":3,\"embedding_name\":\"document_chunk_dense_v1\"}",
    });

    var hook = db_mod.PortableRuntimeBatchPrelockTestHook{};
    db_mod.test_portable_runtime_batch_prelock_hook = &hook;
    defer db_mod.test_portable_runtime_batch_prelock_hook = null;

    // doc:a does not exist yet: thread_a's pre-lock precompute (hoisted by
    // this change) reads that absence and plans a from_generation: 0 commit,
    // then parks at the prelock test hook, just before the apply lock.
    const Writer = struct {
        db: *DB,
        value: []const u8,
        err: ?anyerror = null,

        fn run(self: *@This()) void {
            self.db.batch(.{
                .writes = &.{.{ .key = "doc:a", .value = self.value }},
                .sync_level = .enrichments,
            }) catch |err| {
                self.err = err;
            };
        }
    };
    var writer_a = Writer{
        .db = &db,
        .value = "{\"url\":\"data:text/plain;base64,YWxwaGEgYmV0YSBnYW1tYQ==\",\"text\":\"decoy\"}",
    };
    const thread_a = try std.Thread.spawn(.{}, Writer.run, .{&writer_a});
    var joined_a = false;
    defer if (!joined_a) {
        hook.release.store(true, .release);
        thread_a.join();
    };

    const prelock_deadline = db_mod.monotonicTimeNs() + 5 * std.time.ns_per_s;
    while (!hook.entered.load(.acquire) and db_mod.monotonicTimeNs() < prelock_deadline) {
        @import("antfly_platform").time.yieldNow();
    }
    try std.testing.expect(hook.entered.load(.acquire));

    // Commit different content for the same key while thread_a is parked.
    // Clear the hook first so this write is not itself gated by it.
    db_mod.test_portable_runtime_batch_prelock_hook = null;
    try db.batch(.{
        .writes = &.{.{
            .key = "doc:a",
            .value = "{\"url\":\"data:text/plain;base64,YmV0YSBnYW1tYSBkZWx0YQ==\",\"text\":\"decoy\"}",
        }},
        .sync_level = .full_index,
    });

    // Release thread_a. Its stale pre-lock precompute (built against a
    // nonexistent prior document) must be detected and retried against the
    // now-current state, not silently committed on top of it -- which would
    // either corrupt the generation counter or leave orphaned chunk/embedding
    // artifacts from the intermediate commit.
    hook.release.store(true, .release);
    thread_a.join();
    joined_a = true;
    try std.testing.expectEqual(@as(?anyerror, null), writer_a.err);

    const manifest_key = try db_mod.internal_keys.artifactNamedPrefixAlloc(alloc, "doc:a", "asset", "document_units_v1");
    defer alloc.free(manifest_key);
    const manifest = try db.core.store.get(alloc, manifest_key);
    defer alloc.free(manifest);
    try std.testing.expect(std.mem.indexOf(u8, manifest, "\"generation\":2") != null);
    try std.testing.expect(std.mem.indexOf(u8, manifest, "\"from_generation\":1") != null);
    try std.testing.expect(std.mem.indexOf(u8, manifest, "\"to_generation\":2") != null);

    const chunk_prefix = try db_mod.internal_keys.artifactNamedPrefixAlloc(alloc, "doc:a", "chunk", "document_chunks_v1");
    defer alloc.free(chunk_prefix);
    const chunk_entries = try db.core.store.scanPrefix(alloc, chunk_prefix);
    defer db_mod.docstore_mod.DocStore.freeResults(alloc, chunk_entries);
    var chunk_count: usize = 0;
    var last_chunk_payload: []const u8 = "";
    for (chunk_entries) |entry| if (db_mod.internal_keys.isChunkArtifactRecordKey(entry.key)) {
        chunk_count += 1;
        last_chunk_payload = entry.value;
    };
    // Exactly one surviving chunk, matching thread_a's final ("alpha beta
    // gamma") content -- no orphaned chunk from the intermediate ("beta
    // gamma delta") generation this write raced against.
    try std.testing.expectEqual(@as(usize, 1), chunk_count);
    try std.testing.expect(std.mem.indexOf(u8, last_chunk_payload, "alpha beta gamma") != null);
}

test "db document extraction pre-lock precompute survives a concurrent catalog mutation without touching freed memory" {
    if (builtin.single_threaded or builtin.os.tag == .freestanding) return error.SkipZigTest;
    const alloc = std.testing.allocator;

    var path_tmp = try db_mod.TestDirectory.init("db");
    defer path_tmp.cleanup();
    const path = path_tmp.path().ptr;
    defer db_mod.cleanupTempDir(path);

    var deterministic_dense = db_mod.embedder_mod.DeterministicDenseEmbedder{};
    var deterministic_sparse = db_mod.embedder_mod.DeterministicSparseEmbedder{};
    var db = try DB.open(alloc, std.mem.span(path), .{
        .enrichment = .{
            .owner_id = "worker-a",
            .dense_embedder = deterministic_dense.interface(),
            .sparse_embedder = deterministic_sparse.interface(),
        },
    });
    defer db.close();

    try db.enrichment.addEnrichment(.{
        .name = "document_units_v1",
        .kind = .asset,
        .field = "url",
        .content_type = "application/json",
        .producer_json = "{\"type\":\"document_extraction\",\"config\":{}}",
    });
    try db.enrichment.addEnrichment(.{
        .name = "document_chunks_v1",
        .kind = .chunk,
        .field = "text",
        .source_artifact_name = "document_units_v1",
        .chunk_size = 256,
    });
    try db.enrichment.addEnrichment(.{
        .name = "document_chunk_dense_v1",
        .kind = .embedding,
        .field = "text",
        .source_artifact_name = "document_chunks_v1",
        .expected_dims = 3,
    });
    try db.addIndex(.{
        .name = "document_vectors",
        .kind = .dense_vector,
        .config_json = "{\"field\":\"embedding\",\"dims\":3,\"embedding_name\":\"document_chunk_dense_v1\"}",
    });

    var hook = db_mod.PortableRuntimeBatchPrelockTestHook{};
    db_mod.test_portable_runtime_batch_prelock_hook = &hook;
    defer db_mod.test_portable_runtime_batch_prelock_hook = null;

    // thread_a's pre-lock precompute (extraction against the owned write-plan
    // snapshot, then chunk/embedding derivation) runs to completion and
    // parks at the prelock test hook, just before the apply lock.
    const Writer = struct {
        db: *DB,
        err: ?anyerror = null,

        fn run(self: *@This()) void {
            self.db.batch(.{
                .writes = &.{.{
                    .key = "doc:a",
                    .value = "{\"url\":\"data:text/plain;base64,YWxwaGEgYmV0YSBnYW1tYQ==\"}",
                }},
                .sync_level = .enrichments,
            }) catch |err| {
                self.err = err;
            };
        }
    };
    var writer_a = Writer{ .db = &db };
    const thread_a = try std.Thread.spawn(.{}, Writer.run, .{&writer_a});
    var joined_a = false;
    defer if (!joined_a) {
        hook.release.store(true, .release);
        thread_a.join();
    };

    const prelock_deadline = db_mod.monotonicTimeNs() + 5 * std.time.ns_per_s;
    while (!hook.entered.load(.acquire) and db_mod.monotonicTimeNs() < prelock_deadline) {
        @import("antfly_platform").time.yieldNow();
    }
    try std.testing.expect(hook.entered.load(.acquire));

    // Replace the CHUNK enrichment doc:a's own pre-lock extraction actually
    // used -- with a different chunk_size -- while thread_a is parked just
    // before the apply lock. By this point thread_a's
    // DocumentExtractionCatalogView (built once, under
    // catalog_mutex.lockShared(), in computeAssetRequestDerived) has
    // already been fully read -- its chunk/embedding derivation runs
    // entirely pre-lock, before this hook -- so this exercises the same
    // class of bug the review blocker identified: upsertEnrichment frees
    // the live "document_chunks_v1" entry's strings (chunker_json,
    // source_field, ...) and replaces them in place; thread_a's view, and
    // everything it derived from "document_chunks_v1" and
    // "document_chunk_dense_v1" (which consumes it), must be fully owned,
    // not a borrowed pointer into what this upsert frees. Deleting the
    // chunk enrichment outright is rejected with error.EnrichmentInUse
    // while the embedding still names it as source_artifact_name, so this
    // uses the allowed in-place replace instead. A catalog publish bumps
    // the write-plan generation thread_a pinned, so this also forces the
    // existing PreparedGenerationChanged retry path to run against the
    // replaced catalog entry.
    db_mod.test_portable_runtime_batch_prelock_hook = null;
    _ = try db.enrichment.upsertEnrichment(.{
        .name = "document_chunks_v1",
        .kind = .chunk,
        .field = "text",
        .source_artifact_name = "document_units_v1",
        .chunk_size = 4,
    });

    hook.release.store(true, .release);
    thread_a.join();
    joined_a = true;
    try std.testing.expectEqual(@as(?anyerror, null), writer_a.err);

    // Whether this landed from thread_a's own pre-lock result (if its
    // pinned generation happened to still match) or a generation-changed
    // retry against the replaced chunk_size, the testing allocator above
    // would already have caught any read of, or double free of, memory the
    // upsert's replace freed. Every surviving chunk still gets its
    // "document_chunk_dense_v1" embedding artifact -- the replace never
    // touched that enrichment, so this is the same convergence check as
    // the "concurrent same-key writes" test above, just with a catalog
    // replace as the interference instead of another write.
    const chunk_prefix = try db_mod.internal_keys.artifactNamedPrefixAlloc(alloc, "doc:a", "chunk", "document_chunks_v1");
    defer alloc.free(chunk_prefix);
    const chunk_entries = try db.core.store.scanPrefix(alloc, chunk_prefix);
    defer db_mod.docstore_mod.DocStore.freeResults(alloc, chunk_entries);
    var chunk_count: usize = 0;
    var dense_artifact_count: usize = 0;
    for (chunk_entries) |entry| {
        if (!db_mod.internal_keys.isChunkArtifactRecordKey(entry.key)) continue;
        chunk_count += 1;
        const dense_artifact_key = try db_mod.internal_keys.derivedEmbeddingArtifactKeyAlloc(alloc, entry.key, "document_chunk_dense_v1");
        defer alloc.free(dense_artifact_key);
        const dense_artifact_payload = try db.core.store.get(alloc, dense_artifact_key);
        alloc.free(dense_artifact_payload);
        dense_artifact_count += 1;
    }
    try std.testing.expect(chunk_count > 0);
    try std.testing.expectEqual(chunk_count, dense_artifact_count);
}

test "db document extraction unit chunks embed in provider batches, not per chunk" {
    const alloc = std.testing.allocator;
    const BatchProbe = struct {
        deterministic: db_mod.embedder_mod.DeterministicDenseEmbedder = .{},
        batch_calls: usize = 0,
        items: usize = 0,

        fn embedDense(_: *anyopaque, _: Allocator, _: []const u8, _: []const u8, _: u32) ![]f32 {
            return error.TestUnexpectedResult;
        }

        fn embedDenseBatch(
            raw: *anyopaque,
            batch_alloc: Allocator,
            embedding_name: []const u8,
            texts: []const []const u8,
            dims: u32,
        ) ![]const []const f32 {
            const self: *@This() = @ptrCast(@alignCast(raw));
            self.batch_calls += 1;
            self.items += texts.len;
            const vectors = try batch_alloc.alloc([]const f32, texts.len);
            var initialized: usize = 0;
            errdefer {
                for (vectors[0..initialized]) |vector| batch_alloc.free(@constCast(vector));
                batch_alloc.free(vectors);
            }
            for (texts, vectors) |text, *vector| {
                vector.* = try db_mod.embedder_mod.DeterministicDenseEmbedder.embedDense(
                    &self.deterministic,
                    batch_alloc,
                    embedding_name,
                    text,
                    dims,
                );
                initialized += 1;
            }
            return vectors;
        }

        fn interface(self: *@This()) db_mod.embedder_mod.DenseEmbedder {
            return .{
                .ptr = self,
                .dense_embed_fn = embedDense,
                .dense_embed_batch_fn = embedDenseBatch,
                .deinit_fn = null,
            };
        }
    };

    const c = struct {
        extern fn setenv(name: [*:0]const u8, value: [*:0]const u8, overwrite: c_int) c_int;
        extern fn unsetenv(name: [*:0]const u8) c_int;
    };
    const batch_items_env = "ANTFLY_ENRICHMENT_EMBED_BATCH_ITEMS";
    try std.testing.expectEqual(@as(c_int, 0), c.setenv(batch_items_env, "3", 1));
    defer _ = c.unsetenv(batch_items_env);

    var path_tmp = try db_mod.TestDirectory.init("db");
    defer path_tmp.cleanup();
    const path = path_tmp.path().ptr;
    defer db_mod.cleanupTempDir(path);
    var probe = BatchProbe{};
    var db = try DB.open(alloc, std.mem.span(path), .{
        .enrichment = .{ .owner_id = "worker-a", .dense_embedder = probe.interface() },
    });
    defer db.close();

    try db.enrichment.addEnrichment(.{
        .name = "document_units_v1",
        .kind = .asset,
        .field = "url",
        .content_type = "application/json",
        .producer_json = "{\"type\":\"document_extraction\",\"config\":{}}",
    });
    try db.enrichment.addEnrichment(.{
        .name = "document_chunks_v1",
        .kind = .chunk,
        .field = "text",
        .source_artifact_name = "document_units_v1",
        .chunk_size = 256,
    });
    try db.enrichment.addEnrichment(.{
        .name = "document_chunk_dense_v1",
        .kind = .embedding,
        .field = "text",
        .source_artifact_name = "document_chunks_v1",
        .expected_dims = 3,
    });
    try db.addIndex(.{
        .name = "document_vectors",
        .kind = .dense_vector,
        .config_json = "{\"field\":\"embedding\",\"dims\":3,\"embedding_name\":\"document_chunk_dense_v1\"}",
    });

    // ~2.5 KB of text, base64-encoded, so chunk_size: 256 produces well over
    // one provider batch (batch items pinned to 3 above) from a single
    // document_extraction unit.
    const doc_value = "{\"url\":\"data:text/plain;base64," ++
        "YWxwaGEgYmV0YSBnYW1tYSBkZWx0YSBlcHNpbG9uIHpldGEgZXRhIHRoZXRhIGlvdGEga2FwcGEgbGFtYmRhIG11IG51IHhpIG9taWNyb24gcGkgcmhvIHNpZ21hIHRhdSB1cHNpbG9uIHBoaSBjaGkgcHNpIG9tZWdhLiBhbHBoYSBiZXRhIGdhbW1hIGRlbHRhIGVwc2lsb24gemV0YSBldGEgdGhldGEgaW90YSBrYXBwYSBsYW1iZGEgbXUgbnUgeGkgb21pY3JvbiBwaSByaG8gc2lnbWEgdGF1IHVwc2lsb24gcGhpIGNoaSBwc2kgb21lZ2EuIGFscGhhIGJldGEgZ2FtbWEgZGVsdGEgZXBzaWxvbiB6ZXRhIGV0YSB0aGV0YSBpb3RhIGthcHBhIGxhbWJkYSBtdSBudSB4aSBvbWljcm9uIHBpIHJobyBzaWdtYSB0YXUgdXBzaWxvbiBwaGkgY2hpIHBzaSBvbWVnYS4gYWxwaGEgYmV0YSBnYW1tYSBkZWx0YSBlcHNpbG9uIHpldGEgZXRhIHRoZXRhIGlvdGEga2FwcGEgbGFtYmRhIG11IG51IHhpIG9taWNyb24gcGkgcmhvIHNpZ21hIHRhdSB1cHNpbG9uIHBoaSBjaGkgcHNpIG9tZWdhLiBhbHBoYSBiZXRhIGdhbW1hIGRlbHRhIGVwc2lsb24gemV0YSBldGEgdGhldGEgaW90YSBrYXBwYSBsYW1iZGEgbXUgbnUgeGkgb21pY3JvbiBwaSByaG8gc2lnbWEgdGF1IHVwc2lsb24gcGhpIGNoaSBwc2kgb21lZ2EuIGFscGhhIGJldGEgZ2FtbWEgZGVsdGEgZXBzaWxvbiB6ZXRhIGV0YSB0aGV0YSBpb3RhIGthcHBhIGxhbWJkYSBtdSBudSB4aSBvbWljcm9uIHBpIHJobyBzaWdtYSB0YXUgdXBzaWxvbiBwaGkgY2hpIHBzaSBvbWVnYS4gYWxwaGEgYmV0YSBnYW1tYSBkZWx0YSBlcHNpbG9uIHpldGEgZXRhIHRoZXRhIGlvdGEga2FwcGEgbGFtYmRhIG11IG51IHhpIG9taWNyb24gcGkgcmhvIHNpZ21hIHRhdSB1cHNpbG9uIHBoaSBjaGkgcHNpIG9tZWdhLiBhbHBoYSBiZXRhIGdhbW1hIGRlbHRhIGVwc2lsb24gemV0YSBldGEgdGhldGEgaW90YSBrYXBwYSBsYW1iZGEgbXUgbnUgeGkgb21pY3JvbiBwaSByaG8gc2lnbWEgdGF1IHVwc2lsb24gcGhpIGNoaSBwc2kgb21lZ2EuIGFscGhhIGJldGEgZ2FtbWEgZGVsdGEgZXBzaWxvbiB6ZXRhIGV0YSB0aGV0YSBpb3RhIGthcHBhIGxhbWJkYSBtdSBudSB4aSBvbWljcm9uIHBpIHJobyBzaWdtYSB0YXUgdXBzaWxvbiBwaGkgY2hpIHBzaSBvbWVnYS4gYWxwaGEgYmV0YSBnYW1tYSBkZWx0YSBlcHNpbG9uIHpldGEgZXRhIHRoZXRhIGlvdGEga2FwcGEgbGFtYmRhIG11IG51IHhpIG9taWNyb24gcGkgcmhvIHNpZ21hIHRhdSB1cHNpbG9uIHBoaSBjaGkgcHNpIG9tZWdhLiBhbHBoYSBiZXRhIGdhbW1hIGRlbHRhIGVwc2lsb24gemV0YSBldGEgdGhldGEgaW90YSBrYXBwYSBsYW1iZGEgbXUgbnUgeGkgb21pY3JvbiBwaSByaG8gc2lnbWEgdGF1IHVwc2lsb24gcGhpIGNoaSBwc2kgb21lZ2EuIGFscGhhIGJldGEgZ2FtbWEgZGVsdGEgZXBzaWxvbiB6ZXRhIGV0YSB0aGV0YSBpb3RhIGthcHBhIGxhbWJkYSBtdSBudSB4aSBvbWljcm9uIHBpIHJobyBzaWdtYSB0YXUgdXBzaWxvbiBwaGkgY2hpIHBzaSBvbWVnYS4gYWxwaGEgYmV0YSBnYW1tYSBkZWx0YSBlcHNpbG9uIHpldGEgZXRhIHRoZXRhIGlvdGEga2FwcGEgbGFtYmRhIG11IG51IHhpIG9taWNyb24gcGkgcmhvIHNpZ21hIHRhdSB1cHNpbG9uIHBoaSBjaGkgcHNpIG9tZWdhLiBhbHBoYSBiZXRhIGdhbW1hIGRlbHRhIGVwc2lsb24gemV0YSBldGEgdGhldGEgaW90YSBrYXBwYSBsYW1iZGEgbXUgbnUgeGkgb21pY3JvbiBwaSByaG8gc2lnbWEgdGF1IHVwc2lsb24gcGhpIGNoaSBwc2kgb21lZ2EuIGFscGhhIGJldGEgZ2FtbWEgZGVsdGEgZXBzaWxvbiB6ZXRhIGV0YSB0aGV0YSBpb3RhIGthcHBhIGxhbWJkYSBtdSBudSB4aSBvbWljcm9uIHBpIHJobyBzaWdtYSB0YXUgdXBzaWxvbiBwaGkgY2hpIHBzaSBvbWVnYS4gYWxwaGEgYmV0YSBnYW1tYSBkZWx0YSBlcHNpbG9uIHpldGEgZXRhIHRoZXRhIGlvdGEga2FwcGEgbGFtYmRhIG11IG51IHhpIG9taWNyb24gcGkgcmhvIHNpZ21hIHRhdSB1cHNpbG9uIHBoaSBjaGkgcHNpIG9tZWdhLiBhbHBoYSBiZXRhIGdhbW1hIGRlbHRhIGVwc2lsb24gemV0YSBldGEgdGhldGEgaW90YSBrYXBwYSBsYW1iZGEgbXUgbnUgeGkgb21pY3JvbiBwaSByaG8gc2lnbWEgdGF1IHVwc2lsb24gcGhpIGNoaSBwc2kgb21lZ2EuIGFscGhhIGJldGEgZ2FtbWEgZGVsdGEgZXBzaWxvbiB6ZXRhIGV0YSB0aGV0YSBpb3RhIGthcHBhIGxhbWJkYSBtdSBudSB4aSBvbWljcm9uIHBpIHJobyBzaWdtYSB0YXUgdXBzaWxvbiBwaGkgY2hpIHBzaSBvbWVnYS4gYWxwaGEgYmV0YSBnYW1tYSBkZWx0YSBlcHNpbG9uIHpldGEgZXRhIHRoZXRhIGlvdGEga2FwcGEgbGFtYmRhIG11IG51IHhpIG9taWNyb24gcGkgcmhvIHNpZ21hIHRhdSB1cHNpbG9uIHBoaSBjaGkgcHNpIG9tZWdhLiBhbHBoYSBiZXRhIGdhbW1hIGRlbHRhIGVwc2lsb24gemV0YSBldGEgdGhldGEgaW90YSBrYXBwYSBsYW1iZGEgbXUgbnUgeGkgb21pY3JvbiBwaSByaG8gc2lnbWEgdGF1IHVwc2lsb24gcGhpIGNoaSBwc2kgb21lZ2EuIGFscGhhIGJldGEgZ2FtbWEgZGVsdGEgZXBzaWxvbiB6ZXRhIGV0YSB0aGV0YSBpb3RhIGthcHBhIGxhbWJkYSBtdSBudSB4aSBvbWljcm9uIHBpIHJobyBzaWdtYSB0YXUgdXBzaWxvbiBwaGkgY2hpIHBzaSBvbWVnYS4g" ++
        "\"}";

    try db.batch(.{
        .writes = &.{.{ .key = "doc:a", .value = doc_value }},
        .sync_level = .enrichments,
    });

    // Well more than one provider batch's worth of chunks, but far fewer
    // batch calls than chunks: the old per-chunk path would have called the
    // single-item embedDense (which always errors here) once per chunk.
    try std.testing.expect(probe.items >= 8);
    try std.testing.expect(probe.batch_calls < probe.items);
    const max_batch_items = db_mod.generatedEmbedBatchItems();
    const expected_batch_calls = (probe.items + max_batch_items - 1) / max_batch_items;
    try std.testing.expectEqual(expected_batch_calls, probe.batch_calls);

    // Every chunk this document produced has its own derived dense embedding
    // artifact -- batching the provider calls did not drop or misattribute
    // any chunk's embedding.
    const chunk_prefix = try db_mod.internal_keys.artifactNamedPrefixAlloc(alloc, "doc:a", "chunk", "document_chunks_v1");
    defer alloc.free(chunk_prefix);
    const chunk_entries = try db.core.store.scanPrefix(alloc, chunk_prefix);
    defer db_mod.docstore_mod.DocStore.freeResults(alloc, chunk_entries);
    var chunk_count: usize = 0;
    for (chunk_entries) |entry| {
        if (!db_mod.internal_keys.isChunkArtifactRecordKey(entry.key)) continue;
        chunk_count += 1;
        const dense_artifact_key = try db_mod.internal_keys.derivedEmbeddingArtifactKeyAlloc(alloc, entry.key, "document_chunk_dense_v1");
        defer alloc.free(dense_artifact_key);
        const dense_artifact_payload = try db.core.store.get(alloc, dense_artifact_key);
        alloc.free(dense_artifact_payload);
    }
    try std.testing.expectEqual(probe.items, chunk_count);
}

test "db sync_level enrichments returns without waiting on an unrelated enrichment backlog" {
    if (builtin.single_threaded or builtin.os.tag == .freestanding) return error.SkipZigTest;
    const alloc = std.testing.allocator;

    var path_tmp = try db_mod.TestDirectory.init("db");
    defer path_tmp.cleanup();
    const path = path_tmp.path().ptr;
    defer db_mod.cleanupTempDir(path);

    var slow_asset = db_mod.SlowGatedAssetProducer{};
    var db = try DB.open(alloc, std.mem.span(path), .{
        .enrichment = .{
            .owner_id = "worker-a",
            .asset_producer = slow_asset.interface(),
            .inline_retry_max_attempts = 1,
        },
        .ttl_cleanup = .{ .enabled = false },
    });
    defer db.close();
    defer slow_asset.allow();

    try db.enrichment.addEnrichment(.{
        .name = "summary_v1",
        .kind = .asset,
        .field = "body",
        .content_type = "text/plain",
        .producer_json = "{\"type\":\"generator\",\"config\":{\"provider\":\"antfly\",\"model\":\"test-generator\"}}",
    });

    // sync_level: .write defers this document's asset enrichment to the
    // background runtime, which picks it up and parks in the producer.
    try db.batch(.{
        .writes = &.{.{ .key = "doc:backlog", .value = "{\"body\":\"alpha summary source\"}" }},
        .sync_level = .write,
    });

    var attempts: usize = 0;
    while (attempts < db_mod.slow_test_wait_attempts) : (attempts += 1) {
        if (slow_asset.started.load(.acquire)) break;
        db_mod.sleepPollInterval();
    }
    try std.testing.expect(slow_asset.started.load(.acquire));
    try std.testing.expect(!slow_asset.finished.load(.acquire));

    // This row has no value in the enrichment's source field, so its own
    // sync_level: .enrichments precompute needs nothing from the producer
    // (generated_enrichment_refs stays empty) and must not wait for
    // db.enrichment.runtime's applied_sequence to catch up to -- let alone
    // past -- the backlogged "doc:backlog" commit still parked ahead of it.
    const wait_start_ns = db_mod.monotonicTimeNs();
    try db.batch(.{
        .writes = &.{.{ .key = "doc:tiny", .value = "{\"other\":\"no body field\"}" }},
        .sync_level = .enrichments,
    });
    const wait_elapsed_ns = db_mod.monotonicTimeNs() - wait_start_ns;
    try std.testing.expect(!slow_asset.finished.load(.acquire));
    try std.testing.expect(wait_elapsed_ns < 5 * std.time.ns_per_s);

    slow_asset.allow();
}

test "db document extraction changed version updates large chunked source document" {
    const alloc = std.testing.allocator;

    var path_tmp = try db_mod.TestDirectory.init("db");
    defer path_tmp.cleanup();
    const path = path_tmp.path().ptr;
    defer db_mod.cleanupTempDir(path);

    var db = try DB.open(alloc, std.mem.span(path), .{});
    defer db.close();

    try db.enrichment.addEnrichment(.{
        .name = "document_units_v1",
        .kind = .asset,
        .field = "url",
        .content_type = "application/json",
        .producer_json = "{\"type\":\"document_extraction\",\"config\":{\"source\":{\"etag_field\":\"sha256\",\"version_field\":\"sha256\",\"content_type_field\":\"mime_type\",\"filename_field\":\"filename\"}}}",
    });
    try db.enrichment.addEnrichment(.{
        .name = "document_chunks_v1",
        .kind = .chunk,
        .field = "text",
        .source_artifact_name = "document_units_v1",
        .chunk_size = 96,
        .chunk_overlap = 0,
        .full_text_index = true,
    });
    try db.addIndex(.{
        .name = "ft_document_chunks",
        .kind = .full_text,
        .config_json = "{\"chunk_name\":\"document_chunks_v1\"}",
    });

    const url_v1 = try db_mod.testLargeHtmlDataUrlAlloc(alloc, "v1", "firstversiontoken", 360);
    defer alloc.free(url_v1);
    const doc_v1 = try db_mod.testSourceDocumentJsonAlloc(alloc, url_v1, "sha-v1");
    defer alloc.free(doc_v1);
    try db.batch(.{
        .writes = &.{.{ .key = "doc:large", .value = doc_v1 }},
        .sync_level = .full_index,
    });

    const state_key = try db_mod.assetStateKeyAlloc(alloc, "doc:large", "document_units_v1");
    defer alloc.free(state_key);
    const first_state = try db.core.store.get(alloc, state_key);
    defer alloc.free(first_state);
    const first_chunk_keys = try db_mod.documentExtractionStateChunkKeysAlloc(alloc, first_state);
    defer db_mod.freeOwnedConstKeySlice(alloc, first_chunk_keys);
    try std.testing.expect(first_chunk_keys.len > 300);

    const url_v2 = try db_mod.testLargeHtmlDataUrlAlloc(alloc, "v2", "secondversiontoken", 360);
    defer alloc.free(url_v2);
    const doc_v2 = try db_mod.testSourceDocumentJsonAlloc(alloc, url_v2, "sha-v2");
    defer alloc.free(doc_v2);
    try db.batch(.{
        .writes = &.{.{ .key = "doc:large", .value = doc_v2 }},
        .sync_level = .full_index,
    });

    const manifest_key = try db_mod.internal_keys.artifactNamedPrefixAlloc(alloc, "doc:large", "asset", "document_units_v1");
    defer alloc.free(manifest_key);
    const manifest = try db.core.store.get(alloc, manifest_key);
    defer alloc.free(manifest);
    try std.testing.expect(std.mem.indexOf(u8, manifest, "\"generation\":2") != null);

    const second_state = try db.core.store.get(alloc, state_key);
    defer alloc.free(second_state);
    const second_chunk_keys = try db_mod.documentExtractionStateChunkKeysAlloc(alloc, second_state);
    defer db_mod.freeOwnedConstKeySlice(alloc, second_chunk_keys);
    try std.testing.expect(second_chunk_keys.len > 300);

    var second_result = try db.search(alloc, .{
        .index_name = "ft_document_chunks",
        .full_text = .{ .match = .{ .field = "text", .text = "secondversiontoken" } },
        .return_mode = .chunk,
    });
    defer second_result.deinit();
    try std.testing.expect(second_result.total_hits > 0);

    var first_result = try db.search(alloc, .{
        .index_name = "ft_document_chunks",
        .full_text = .{ .match = .{ .field = "text", .text = "firstversiontoken" } },
        .return_mode = .chunk,
    });
    defer first_result.deinit();
    try std.testing.expectEqual(@as(u32, 0), first_result.total_hits);
}

// Chunk artifacts routed into the default full-text index must follow their
// enrichment's lifecycle: a chunker config change rechunks on reprocess
// (issue 927), a newly registered chunk artifact materializes without wedging
// full-text replay (issue 928), and a deleted one has its members retired
// from the index when reprocess purges its rows (issue 929).
test "db document extraction chunk enrichment lifecycle rechunks adds and retires chunk artifacts" {
    const alloc = std.testing.allocator;

    var path_tmp = try db_mod.TestDirectory.init("db");
    defer path_tmp.cleanup();
    const path = path_tmp.path().ptr;
    defer db_mod.cleanupTempDir(path);

    var db = try DB.open(alloc, std.mem.span(path), .{});
    defer db.close();

    try db.enrichment.addEnrichment(.{
        .name = "document_units_v1",
        .kind = .asset,
        .field = "url",
        .content_type = "application/json",
        .producer_json = "{\"type\":\"document_extraction\",\"config\":{}}",
    });
    try db.enrichment.addEnrichment(.{
        .name = "document_chunks_v1",
        .kind = .chunk,
        .field = "text",
        .source_artifact_name = "document_units_v1",
        .chunk_size = 64,
        .chunk_overlap = 0,
        .full_text_index = true,
    });
    try db.addIndex(.{ .name = "ft_default", .kind = .full_text, .config_json = "{}" });

    var url = std.ArrayListUnmanaged(u8).empty;
    defer url.deinit(alloc);
    try url.appendSlice(alloc, "data:text/plain,");
    for (0..40) |i| {
        const word = try std.fmt.allocPrint(alloc, "harbor{d}%20ledger%20window%20", .{i});
        defer alloc.free(word);
        try url.appendSlice(alloc, word);
    }
    try url.appendSlice(alloc, "zygomorphic");
    const doc_value = try std.fmt.allocPrint(alloc, "{{\"url\":\"{s}\"}}", .{url.items});
    defer alloc.free(doc_value);

    try db.batch(.{ .writes = &.{.{ .key = "doc:a", .value = doc_value }}, .sync_level = .full_index });
    try db.runUntilIdle();
    const small_chunks = try db_mod.testChunkArtifactCount(alloc, &db, "doc:a", "document_chunks_v1");
    try std.testing.expect(small_chunks > 4);
    try std.testing.expectEqual(@as(u32, 1), try db_mod.testDefaultTextHitCount(alloc, &db, "zygomorphic"));

    // Growing the chunk size keeps the old chunk ids 0..n but must replace
    // their text, so the tail term moves into the new last chunk.
    _ = try db.enrichment.upsertEnrichment(.{
        .name = "document_chunks_v1",
        .kind = .chunk,
        .field = "text",
        .source_artifact_name = "document_units_v1",
        .chunk_size = 4096,
        .chunk_overlap = 0,
        .full_text_index = true,
    });
    try std.testing.expect(try db.reprocessDocumentArtifact(alloc, "doc:a", "document_units_v1"));
    try db.runUntilIdle();
    try std.testing.expectEqual(@as(usize, 1), try db_mod.testChunkArtifactCount(alloc, &db, "doc:a", "document_chunks_v1"));
    try std.testing.expectEqual(@as(u32, 1), try db_mod.testDefaultTextHitCount(alloc, &db, "zygomorphic"));
    try std.testing.expectEqual(@as(u32, 1), try db_mod.testDefaultTextHitCount(alloc, &db, "harbor0"));

    try db.enrichment.addEnrichment(.{
        .name = "document_chunks_v2",
        .kind = .chunk,
        .field = "text",
        .source_artifact_name = "document_units_v1",
        .chunk_size = 4096,
        .chunk_overlap = 0,
        .full_text_index = true,
    });
    try std.testing.expect(try db.reprocessDocumentArtifact(alloc, "doc:a", "document_units_v1"));
    try db.runUntilIdle();
    try std.testing.expectEqual(@as(usize, 1), try db_mod.testChunkArtifactCount(alloc, &db, "doc:a", "document_chunks_v2"));
    try std.testing.expectEqual(@as(u32, 2), try db_mod.testDefaultTextHitCount(alloc, &db, "zygomorphic"));

    try std.testing.expect(try db.enrichment.deleteEnrichment(.chunk, "document_chunks_v2"));
    try std.testing.expect(try db.reprocessDocumentArtifact(alloc, "doc:a", "document_units_v1"));
    try db.runUntilIdle();
    try std.testing.expectEqual(@as(usize, 0), try db_mod.testChunkArtifactCount(alloc, &db, "doc:a", "document_chunks_v2"));
    try std.testing.expectEqual(@as(u32, 1), try db_mod.testDefaultTextHitCount(alloc, &db, "zygomorphic"));
}

test "db document extraction update recovers corrupt previous extraction state" {
    const alloc = std.testing.allocator;

    var path_tmp = try db_mod.TestDirectory.init("db");
    defer path_tmp.cleanup();
    const path = path_tmp.path().ptr;
    defer db_mod.cleanupTempDir(path);

    var db = try DB.open(alloc, std.mem.span(path), .{});
    defer db.close();

    try db.enrichment.addEnrichment(.{
        .name = "document_units_v1",
        .kind = .asset,
        .field = "url",
        .content_type = "application/json",
        .producer_json = "{\"type\":\"document_extraction\",\"config\":{\"source\":{\"etag_field\":\"sha256\",\"version_field\":\"sha256\",\"content_type_field\":\"mime_type\",\"filename_field\":\"filename\"}}}",
    });
    try db.enrichment.addEnrichment(.{
        .name = "document_chunks_v1",
        .kind = .chunk,
        .field = "text",
        .source_artifact_name = "document_units_v1",
        .chunk_size = 96,
        .chunk_overlap = 0,
        .full_text_index = true,
    });

    const url_v1 = try db_mod.testLargeHtmlDataUrlAlloc(alloc, "v1", "corruptoldtoken", 80);
    defer alloc.free(url_v1);
    const doc_v1 = try db_mod.testSourceDocumentJsonAlloc(alloc, url_v1, "sha-v1");
    defer alloc.free(doc_v1);
    try db.batch(.{
        .writes = &.{.{ .key = "doc:corrupt", .value = doc_v1 }},
        .sync_level = .full_index,
    });

    const state_key = try db_mod.assetStateKeyAlloc(alloc, "doc:corrupt", "document_units_v1");
    defer alloc.free(state_key);
    try db.core.store.put(state_key, "{\"kind\":\"document_extraction_state_v1\",\"unit_keys\":[7],\"unit_descriptors\":{},\"chunk_keys\":[false]}");

    const url_v2 = try db_mod.testLargeHtmlDataUrlAlloc(alloc, "v2", "corruptnewtoken", 80);
    defer alloc.free(url_v2);
    const doc_v2 = try db_mod.testSourceDocumentJsonAlloc(alloc, url_v2, "sha-v2");
    defer alloc.free(doc_v2);
    try db.batch(.{
        .writes = &.{.{ .key = "doc:corrupt", .value = doc_v2 }},
        .sync_level = .full_index,
    });

    const recovered_state = try db.core.store.get(alloc, state_key);
    defer alloc.free(recovered_state);
    const recovered_chunk_keys = try db_mod.documentExtractionStateChunkKeysAlloc(alloc, recovered_state);
    defer db_mod.freeOwnedConstKeySlice(alloc, recovered_chunk_keys);
    try std.testing.expect(recovered_chunk_keys.len > 0);

    try db.core.store.put(state_key, "{");

    const url_v3 = try db_mod.testLargeHtmlDataUrlAlloc(alloc, "v3", "corruptthirdtoken", 80);
    defer alloc.free(url_v3);
    const doc_v3 = try db_mod.testSourceDocumentJsonAlloc(alloc, url_v3, "sha-v3");
    defer alloc.free(doc_v3);
    try db.batch(.{
        .writes = &.{.{ .key = "doc:corrupt", .value = doc_v3 }},
        .sync_level = .full_index,
    });

    const recovered_truncated_state = try db.core.store.get(alloc, state_key);
    defer alloc.free(recovered_truncated_state);
    const recovered_truncated_chunk_keys = try db_mod.documentExtractionStateChunkKeysAlloc(alloc, recovered_truncated_state);
    defer db_mod.freeOwnedConstKeySlice(alloc, recovered_truncated_chunk_keys);
    try std.testing.expect(recovered_truncated_chunk_keys.len > 0);
}

test "db extractEnrichments exposes cleaned writes and special fields" {
    const alloc = std.testing.allocator;

    var path_tmp = try db_mod.TestDirectory.init("db");
    defer path_tmp.cleanup();
    const path = path_tmp.path().ptr;
    defer db_mod.cleanupTempDir(path);

    var db = try DB.open(alloc, std.mem.span(path), .{});
    defer db.close();

    var result = try db.enrichment.extractEnrichments(alloc, &.{
        .{
            .key = "doc:a",
            .value = "{\"title\":\"alpha\",\"_embeddings\":{\"dense_idx\":[1,2,3],\"sparse_idx\":{\"indices\":[1,5],\"values\":[0.5,0.75]}},\"_edges\":{\"graph_v1\":{\"cites\":[{\"target\":\"doc:b\",\"edge_id\":\"relationship-1\",\"metadata\":{\"source\":\"fixture\"},\"weight\":2.0}]}}}",
        },
    });
    defer result.deinit(alloc);

    try std.testing.expectEqual(@as(usize, 1), result.cleaned_writes.len);
    try std.testing.expectEqualStrings("doc:a", result.cleaned_writes[0].key);
    try std.testing.expect(std.mem.indexOf(u8, result.cleaned_writes[0].value, "\"title\":\"alpha\"") != null);
    try std.testing.expect(std.mem.indexOf(u8, result.cleaned_writes[0].value, "_embeddings") == null);
    try std.testing.expect(std.mem.indexOf(u8, result.cleaned_writes[0].value, "_summaries") == null);
    try std.testing.expect(std.mem.indexOf(u8, result.cleaned_writes[0].value, "_edges") == null);

    try std.testing.expectEqual(@as(usize, 1), result.dense_embeddings.len);
    try std.testing.expectEqualStrings("dense_idx", result.dense_embeddings[0].index_name);
    try std.testing.expectEqual(@as(usize, 3), result.dense_embeddings[0].vector.len);

    try std.testing.expectEqual(@as(usize, 1), result.sparse_embeddings.len);
    try std.testing.expectEqualStrings("sparse_idx", result.sparse_embeddings[0].index_name);
    try std.testing.expectEqual(@as(usize, 2), result.sparse_embeddings[0].indices.len);

    try std.testing.expectEqual(@as(usize, 1), result.graph_writes.len);
    try std.testing.expectEqualStrings("graph_v1", result.graph_writes[0].index_name);
    try std.testing.expectEqualStrings("doc:b", result.graph_writes[0].target);
    try std.testing.expectEqualStrings("relationship-1", result.graph_writes[0].edge_id);

    const Failure = struct {
        fn run(failure_alloc: Allocator, target: *DB) !void {
            var extracted = try target.enrichment.extractEnrichments(failure_alloc, &.{
                .{
                    .key = "doc:a",
                    .value = "{\"title\":\"alpha\",\"_embeddings\":{\"dense_idx\":[1,2,3],\"sparse_idx\":{\"indices\":[1,5],\"values\":[0.5,0.75]}},\"_edges\":{\"graph_v1\":{\"cites\":[{\"target\":\"doc:b\",\"edge_id\":\"relationship-1\",\"metadata\":{\"source\":\"fixture\"},\"weight\":2.0}]}}}",
                },
            });
            defer extracted.deinit(failure_alloc);
            try std.testing.expectEqual(@as(usize, 1), extracted.graph_writes.len);
            var packed_result = try target.enrichment.extractEnrichments(failure_alloc, &.{.{ .key = "packed", .value = "{\"title\":\"packed\",\"_embeddings\":{\"dense_idx\":\"AACAPw==\"}}" }});
            defer packed_result.deinit(failure_alloc);
            try std.testing.expectEqual(@as(f32, 1), packed_result.dense_embeddings[0].vector[0]);
        }
    };
    try @import("antfly_platform").allocator.checkAllAllocationFailures(alloc, Failure.run, .{&db});
}

test "db extractEnrichments rejects unsupported legacy summaries field" {
    const alloc = std.testing.allocator;

    var path_tmp = try db_mod.TestDirectory.init("db");
    defer path_tmp.cleanup();
    const path = path_tmp.path().ptr;
    defer db_mod.cleanupTempDir(path);

    var db = try DB.open(alloc, std.mem.span(path), .{});
    defer db.close();

    try std.testing.expectError(error.UnsupportedReservedField, db.enrichment.extractEnrichments(alloc, &.{
        .{
            .key = "doc:a",
            .value = "{\"title\":\"alpha\",\"_summaries\":{\"sum_idx\":\"brief\"}}",
        },
    }));
}

test "db computeEnrichments synchronously builds chunk and embedding outputs" {
    const alloc = std.testing.allocator;

    var path_tmp = try db_mod.TestDirectory.init("db");
    defer path_tmp.cleanup();
    const path = path_tmp.path().ptr;
    defer db_mod.cleanupTempDir(path);

    var deterministic = db_mod.embedder_mod.DeterministicDenseEmbedder{};
    var db = try DB.open(alloc, std.mem.span(path), .{
        .enrichment = .{
            .owner_id = "worker-a",
            .dense_embedder = deterministic.interface(),
        },
    });
    defer db.close();

    try db.addIndex(.{
        .name = "dv_v1",
        .kind = .dense_vector,
        .config_json = "{\"field\":\"embedding\",\"dims\":3,\"generator\":{\"kind\":\"dense_embedding\",\"source_field\":\"body\",\"chunk_name\":\"body_chunks_v1\",\"chunk_size\":8,\"chunk_overlap\":2,\"embedding_name\":\"chunk_dense_v1\"}}",
    });
    try db.addIndex(.{
        .name = "ft_chunks",
        .kind = .full_text,
        .config_json = "{\"chunk_name\":\"body_chunks_v1\"}",
    });

    var result = try db.enrichment.computeEnrichments(alloc, &.{
        .{
            .key = "doc:a",
            .value = "{\"body\":\"abcdefghijklmno\"}",
        },
    });
    defer result.deinit(alloc);

    var chunk_artifacts: usize = 0;
    var embedding_artifacts: usize = 0;
    for (result.artifact_writes) |write| {
        if (write.artifact_ref.kind == .chunk) {
            chunk_artifacts += 1;
        } else if (write.artifact_ref.kind == .embedding) {
            embedding_artifacts += 1;
        }
    }

    try std.testing.expectEqual(@as(usize, 3), chunk_artifacts);
    try std.testing.expectEqual(@as(usize, 3), embedding_artifacts);
    try std.testing.expectEqual(@as(usize, 3), result.documents.len);
    try std.testing.expectEqual(@as(usize, 3), result.dense_embeddings.len);
    try std.testing.expectEqual(@as(usize, 0), result.failed_keys.len);
    try std.testing.expectEqualStrings("ft_chunks", result.documents[0].target_index_names[0]);
    try std.testing.expectEqualStrings("dv_v1", result.dense_embeddings[0].index_name);

    const runtime_stats = db.enrichment.runtime.?.stats();
    try std.testing.expectEqual(@as(u64, 1), runtime_stats.embed_batches_started);
    try std.testing.expectEqual(@as(u64, 1), runtime_stats.embed_batches_completed);
    try std.testing.expectEqual(@as(u64, 3), runtime_stats.embed_items_started);
    try std.testing.expectEqual(@as(u64, 3), runtime_stats.embed_items_completed);

    const AllocationSweep = struct {
        fn run(failing: Allocator, target: *DB) !void {
            var computed = try target.enrichment.computeEnrichments(failing, &.{.{ .key = "doc:a", .value = "{\"body\":\"abcdefghijklmno\"}" }});
            defer computed.deinit(failing);
            try std.testing.expect(computed.artifact_writes.len != 0);
            try std.testing.expect(computed.documents.len != 0);
            try std.testing.expect(computed.dense_embeddings.len != 0);
        }
    };
    try @import("antfly_platform").allocator.checkAllAllocationFailures(alloc, AllocationSweep.run, .{&db});
}

test "db leased enrichment worker renews text replay tenure across provider waits" {
    const alloc = std.testing.allocator;
    const Slow = struct {
        calls: usize = 0,
        fn embed(ptr: *anyopaque, a: Allocator, name: []const u8, text: []const u8, dims: u32) ![]f32 {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            self.calls += 1;
            // Plain text uses no document-transform heartbeat guard. The
            // replay tenure must survive several renewal intervals itself.
            db_mod.sleepNs(2500 * std.time.ns_per_ms);
            var deterministic = db_mod.embedder_mod.DeterministicDenseEmbedder{};
            return deterministic.interface().embedDense(a, name, text, dims);
        }
    };
    var path_tmp = try db_mod.TestDirectory.init("db");
    defer path_tmp.cleanup();
    var slow = Slow{};
    var db = try DB.open(alloc, path_tmp.path(), .{
        .start_optional_runtime_workers = false,
        .enrichment = .{
            .owner_id = "text-replay-owner",
            .lease_ttl_ms = 1000,
            .dense_embedder = .{ .ptr = &slow, .dense_embed_fn = Slow.embed },
        },
    });
    defer db.close();
    try db.addIndex(.{
        .name = "semantic",
        .kind = .dense_vector,
        .config_json = "{\"field\":\"embedding\",\"dims\":3,\"generator\":{\"kind\":\"dense_embedding\",\"source_field\":\"body\",\"embedding_name\":\"body_dense\"}}",
    });
    try db.batch(.{ .writes = &.{.{ .key = "doc", .value = "{\"body\":\"slow text provider\"}" }}, .sync_level = .write });
    try db.runUntilIdle();
    const stats = db.enrichment.runtime.?.stats();
    try std.testing.expectEqual(@as(usize, 1), slow.calls);
    try std.testing.expectEqual(@as(u64, 0), stats.lost_leases);
    try std.testing.expectEqual(db.core.nextEnrichmentSequence(), stats.applied_sequence);
    try std.testing.expect(stats.has_lease);
    try std.testing.expect(db.enrichment.runtime.?.ownership.renewal_count > 0);
}

test "db leased enrichment worker backs off while a stale owner holds the lease" {
    const alloc = std.testing.allocator;

    var path_tmp = try db_mod.TestDirectory.init("db");
    defer path_tmp.cleanup();
    const path = path_tmp.path().ptr;
    defer db_mod.cleanupTempDir(path);

    var deterministic = db_mod.embedder_mod.DeterministicDenseEmbedder{};
    var db = try DB.open(alloc, std.mem.span(path), .{
        .start_optional_runtime_workers = false,
        .enrichment = .{
            .owner_id = "worker-b",
            .dense_embedder = deterministic.interface(),
        },
        .ttl_cleanup = .{ .enabled = false },
    });
    defer db.close();

    var stale_lease = try db_mod.enrichment_lease.EnrichmentLease.init(
        alloc,
        db.core.store,
        db_mod.enrichment_lease.default_lease_key,
    );
    defer stale_lease.deinit();
    try db.addIndex(.{
        .name = "dv_v1",
        .kind = .dense_vector,
        .config_json = "{\"field\":\"embedding\",\"dims\":3,\"generator\":{\"kind\":\"dense_embedding\",\"source_field\":\"body\",\"embedding_name\":\"body_dense_v1\"}}",
    });

    // Installing the producer may replace its runtime and lease state. Pin
    // the competing owner only after admission and before starting the worker.
    const now_ms = db_mod.platform_time.realtimeNs() / std.time.ns_per_ms;
    try std.testing.expect(try stale_lease.tryAcquire("worker-a", now_ms, 30_000));
    try db.enrichment.runtime.?.start();

    try db.batch(.{
        .writes = &.{.{ .key = "doc:a", .value = "{\"body\":\"blocked enrichment\"}" }},
        .sync_level = .write,
    });

    var first_failures: u64 = 0;
    var attempts: usize = 0;
    while (attempts < db_mod.default_test_wait_attempts) : (attempts += 1) {
        first_failures = db.enrichment.runtime.?.stats().lease_acquire_failures;
        if (first_failures > 0) break;
        db_mod.sleepPollInterval();
    }
    try std.testing.expect(first_failures > 0);

    db_mod.sleepNs(250 * std.time.ns_per_ms);
    const later_failures = db.enrichment.runtime.?.stats().lease_acquire_failures;
    // A 100ms denial delay permits at most a handful of retries in this
    // observation window. The former zero-duration yield produces thousands.
    try std.testing.expect(later_failures - first_failures <= 5);
}

test "db leased enrichment worker generates dense embeddings with durable lsm primary backend" {
    const alloc = std.testing.allocator;

    var path_tmp = try db_mod.TestDirectory.init("db");
    defer path_tmp.cleanup();
    const path = path_tmp.path().ptr;
    defer db_mod.cleanupTempDir(path);

    var deterministic = db_mod.embedder_mod.DeterministicDenseEmbedder{};
    var db = try DB.open(alloc, std.mem.span(path), .{
        .primary_backend = .{ .lsm = .{ .flush_threshold = 1 } },
        .enrichment = .{
            .owner_id = "worker-a",
            .dense_embedder = deterministic.interface(),
        },
    });
    defer db.close();

    try db.addIndex(.{
        .name = "dv_v1",
        .kind = .dense_vector,
        .config_json = "{\"field\":\"embedding\",\"dims\":3,\"generator\":{\"kind\":\"dense_embedding\",\"source_field\":\"body\",\"embedding_name\":\"body_dense_v1\"}}",
    });

    try db.batch(.{
        .writes = &.{
            .{ .key = "doc:a", .value = "{\"title\":\"alpha\",\"body\":\"generated vector text\"}" },
        },
        .sync_level = .write,
    });

    try db.enrichment.runtime.?.waitForApplied(1);
    try db.executor.waitForAll(2);

    const stats = try db.stats(alloc);
    defer db_mod.types.freeDBStats(alloc, stats);
    try std.testing.expect(stats.enrichment.enabled);
    try std.testing.expect(stats.enrichment.lease_owned);
    try std.testing.expect(stats.enrichment.has_lease);
    try std.testing.expect(stats.enrichment.acquisition_count > 0);
    try std.testing.expectEqual(@as(u64, 1), stats.enrichment.applied_sequence);

    const query_vec = try deterministic.interface().embedDense(alloc, "", "generated vector text", 3);
    defer alloc.free(query_vec);

    var result = try db.search(alloc, .{
        .index_name = "dv_v1",
        .dense = .{
            .vector = query_vec,
            .k = 1,
        },
    });
    defer result.deinit();
    try std.testing.expectEqual(@as(u32, 1), result.total_hits);
    try std.testing.expectEqualStrings("doc:a", result.hits[0].id);

    const artifact_key = try db_mod.expectedDocumentEmbeddingArtifactKeyAlloc(alloc, "doc:a", "body_dense_v1");
    defer alloc.free(artifact_key);
    const artifacts = try db.core.store.scanPrefix(alloc, artifact_key);
    defer db_mod.docstore_mod.DocStore.freeResults(alloc, artifacts);
    try std.testing.expectEqual(@as(usize, 1), artifacts.len);
    try std.testing.expectEqualStrings(artifact_key, artifacts[0].key);
    try db_mod.expectDenseEmbeddingArtifactValue(alloc, artifacts[0].value, db_mod.enrichment_artifact_codec.hashSource("generated vector text"), 3);
}

test "db retryable asset producer batches do not block independent dense publication" {
    const alloc = std.testing.allocator;

    var path_tmp = try db_mod.TestDirectory.init("db");
    defer path_tmp.cleanup();
    const path = path_tmp.path().ptr;
    defer db_mod.cleanupTempDir(path);

    var gated_asset = db_mod.GateAssetProducer{};
    var dense = db_mod.embedder_mod.DeterministicDenseEmbedder{};
    var db = try DB.open(alloc, std.mem.span(path), .{
        .enrichment = .{
            .owner_id = "worker-a",
            .asset_producer = gated_asset.interface(),
            .dense_embedder = dense.interface(),
            .inline_retry_max_attempts = 1,
        },
        .ttl_cleanup = .{ .enabled = false },
    });
    defer db.close();

    // Register the unavailable asset producer first so request ordering cannot
    // be the mechanism that lets the dense producer make progress.
    try db.enrichment.addEnrichment(.{
        .name = "summary_v1",
        .kind = .asset,
        .field = "body",
        .content_type = "text/plain",
        .producer_json = "{\"type\":\"generator\",\"config\":{\"provider\":\"antfly\",\"model\":\"test-generator\"}}",
    });
    // A distinct producer configuration forces a second native batch. Batch
    // partitioning must remain deferred so the first outage cannot perform
    // provider I/O while later producer classes are still being planned.
    try db.enrichment.addEnrichment(.{
        .name = "summary_v2",
        .kind = .asset,
        .field = "body",
        .content_type = "text/plain",
        .producer_json = "{\"type\":\"generator\",\"config\":{\"provider\":\"antfly\",\"model\":\"test-generator-secondary\"}}",
    });
    try db.addIndex(.{
        .name = "title_dense",
        .kind = .dense_vector,
        .config_json =
        \\{"field":"embedding","dims":3,"publication_policy":"progressive","generator":{"kind":"dense_embedding","source_field":"title","embedding_name":"title_dense"}}
        ,
    });
    try db.batch(.{
        .writes = &.{.{
            .key = "doc:a",
            .value = "{\"title\":\"independent title\",\"body\":\"unavailable summary source\"}",
        }},
        .sync_level = .write,
    });

    var sibling_published = false;
    var attempts: usize = 0;
    while (attempts < db_mod.default_test_wait_attempts) : (attempts += 1) {
        const stats = try db.stats(alloc);
        defer db_mod.types.freeDBStats(alloc, stats);
        if (gated_asset.blocked_requests.load(.acquire) >= 2) {
            for (stats.indexes) |index_stats| {
                if (!std.mem.eql(u8, index_stats.name, "title_dense")) continue;
                sibling_published = index_stats.doc_count == 1 and
                    index_stats.coverage_produced_count == 1 and
                    index_stats.replay_applied_sequence != 0;
                break;
            }
        }
        if (sibling_published)
            sibling_published = try db_mod.independentDensePublicationVisibleForTest(&db, alloc);
        if (sibling_published) break;
        db_mod.sleepPollInterval();
    }
    try std.testing.expect(gated_asset.blocked_requests.load(.acquire) >= 2);
    try std.testing.expect(sibling_published);

    var result = try db.search(alloc, .{
        .index_name = "title_dense",
        .dense = .{
            .vector = &.{ 1.0, 0.0, 0.0 },
            .k = 1,
        },
    });
    defer result.deinit();
    try std.testing.expectEqual(@as(u32, 1), result.total_hits);
    try std.testing.expectEqualStrings("doc:a", result.hits[0].id);

    gated_asset.allowAll();
    try db.runUntilIdle();
    try std.testing.expect(gated_asset.successful_requests.load(.acquire) != 0);
}

test "db blocked dense embedding lane does not force the independent asset lane to redo checkpointed work" {
    const alloc = std.testing.allocator;

    var path_tmp = try db_mod.TestDirectory.init("db");
    defer path_tmp.cleanup();
    const path = path_tmp.path().ptr;
    defer db_mod.cleanupTempDir(path);

    var gated_dense = db_mod.GateDenseEmbedder{};
    gated_dense.allowed_successes.store(0, .release);
    var gated_asset = db_mod.GateAssetProducer{};
    gated_asset.allowAll();
    var db = try DB.open(alloc, std.mem.span(path), .{
        .enrichment = .{
            .owner_id = "worker-a",
            .asset_producer = gated_asset.interface(),
            .dense_embedder = gated_dense.interface(),
            .inline_retry_max_attempts = 1,
        },
        .ttl_cleanup = .{ .enabled = false },
    });
    defer db.close();

    try db.enrichment.addEnrichment(.{
        .name = "summary_v1",
        .kind = .asset,
        .field = "body",
        .content_type = "text/plain",
        .producer_json = "{\"type\":\"generator\",\"config\":{\"provider\":\"antfly\",\"model\":\"test-generator\"}}",
    });
    try db.addIndex(.{
        .name = "title_dense",
        .kind = .dense_vector,
        .config_json =
        \\{"field":"embedding","dims":3,"publication_policy":"progressive","generator":{"kind":"dense_embedding","source_field":"title","embedding_name":"title_dense"}}
        ,
    });
    try db.batch(.{
        .writes = &.{.{
            .key = "doc:a",
            .value = "{\"title\":\"independent title\",\"body\":\"available summary source\"}",
        }},
        .sync_level = .write,
    });

    // The dense lane is blocked from the first attempt onward. The
    // asset-producer lane is an independent execution lane with its own
    // window and replay cursor (scope "generated.assets"), so it must
    // publish and checkpoint without waiting on the still-failing dense
    // lane (scope "generated.dense"). See "Two-Stream Execution Model" in
    // ENRICHMENTS.md.
    // The asset lane's first publish is a real provider round trip plus a
    // durable checkpoint on a background worker; on a loaded host that can
    // take longer than the default one-second poll budget, so use the slow
    // budget here (the assertion is unchanged).
    var asset_published = false;
    var attempts: usize = 0;
    while (attempts < db_mod.slow_test_wait_attempts) : (attempts += 1) {
        if (gated_dense.blocked_requests.load(.acquire) != 0 and
            gated_asset.successful_requests.load(.acquire) != 0)
        {
            asset_published = true;
            break;
        }
        db_mod.sleepPollInterval();
    }
    try std.testing.expect(asset_published);
    const first_asset_successes = gated_asset.successful_requests.load(.acquire);

    // The asset lane's own replay cursor is durable even though the dense
    // lane, which never completed a batch, has none: this is only true of
    // the per-stream design. Under the historical single shared cursor, no
    // cursor would exist under either scope until *both* lanes published.
    // The producer's success is observed before the lane has flushed its
    // window and saved the cursor, so wait for the checkpoint itself.
    var assets_cursor_seen = false;
    attempts = 0;
    while (attempts < db_mod.slow_test_wait_attempts) : (attempts += 1) {
        if (try db_mod.enrichment_state.loadReplayCursor(alloc, db.enrichment.runtime.?.store, "generated.assets")) |cursor| {
            var owned = cursor;
            owned.deinit(alloc);
            assets_cursor_seen = true;
            break;
        }
        db_mod.sleepPollInterval();
    }
    try std.testing.expect(assets_cursor_seen);

    gated_dense.allowAll();
    try db.runUntilIdle();

    var result = try db.search(alloc, .{
        .index_name = "title_dense",
        .dense = .{ .vector = &.{ 1.0, 0.0, 0.0 }, .k = 1 },
    });
    defer result.deinit();
    try std.testing.expectEqual(@as(u32, 1), result.total_hits);
    try std.testing.expectEqualStrings("doc:a", result.hits[0].id);

    // The already-checkpointed asset lane is not redone once the sibling
    // dense lane finally publishes.
    try std.testing.expectEqual(first_asset_successes, gated_asset.successful_requests.load(.acquire));
}

test "cross-quantum lane pipelining publishes a fast dense quantum while a slower asset quantum is still in flight" {
    if (builtin.single_threaded or builtin.os.tag == .freestanding) return error.SkipZigTest;
    const alloc = std.testing.allocator;

    var path_tmp = try db_mod.TestDirectory.init("db");
    defer path_tmp.cleanup();
    const path = path_tmp.path().ptr;
    defer db_mod.cleanupTempDir(path);

    const c = struct {
        extern fn setenv(name: [*:0]const u8, value: [*:0]const u8, overwrite: c_int) c_int;
        extern fn unsetenv(name: [*:0]const u8) c_int;
    };
    const window_env = "ANTFLY_ENRICHMENT_PREPARATION_WINDOW_ITEMS";
    try std.testing.expectEqual(@as(c_int, 0), c.setenv(window_env, "1", 1));
    defer _ = c.unsetenv(window_env);

    var slow_asset = db_mod.SlowGatedAssetProducer{};
    var deterministic = db_mod.embedder_mod.DeterministicDenseEmbedder{};
    var db = try DB.open(alloc, std.mem.span(path), .{
        .enrichment = .{
            .owner_id = "worker-a",
            .asset_producer = slow_asset.interface(),
            .dense_embedder = deterministic.interface(),
            .inline_retry_max_attempts = 1,
        },
        .ttl_cleanup = .{ .enabled = false },
    });
    defer db.close();
    // Safety net, evaluated (LIFO) before the `db.close()` defer above: if
    // any assertion below fails before the explicit `slow_asset.allow()`
    // call, this still releases the parked producer so `db.close()`'s
    // shutdown drain cannot hang the whole test binary waiting on a gate
    // nothing will ever open. Calling `allow()` twice is harmless (it is a
    // one-way latch).
    defer slow_asset.allow();

    try db.enrichment.addEnrichment(.{
        .name = "summary_v1",
        .kind = .asset,
        .field = "body",
        .content_type = "text/plain",
        .producer_json = "{\"type\":\"generator\",\"config\":{\"provider\":\"antfly\",\"model\":\"test-generator\"}}",
    });
    try db.addIndex(.{
        .name = "title_dense",
        .kind = .dense_vector,
        .config_json =
        \\{"field":"embedding","dims":3,"publication_policy":"progressive","generator":{"kind":"dense_embedding","source_field":"title","embedding_name":"title_dense"}}
        ,
    });

    // doc:a has only "body" (queues asset-lane work -- quantum 1); doc:b has
    // only "title" (queues dense-lane work -- quantum 2, scanned strictly
    // after quantum 1). The preparation window is forced to 1 item, so these
    // become two separate preparation quanta instead of one shared quantum
    // (within-quantum concurrency already worked before this change; the
    // bug this test targets is specifically about a *later* quantum's
    // dispatch waiting on an *earlier* quantum's still-running lane).
    try db.batch(.{
        .writes = &.{
            .{ .key = "doc:a", .value = "{\"body\":\"alpha summary source\"}" },
            .{ .key = "doc:b", .value = "{\"title\":\"independent title\"}" },
        },
        .sync_level = .write,
    });

    // Poll until the asset lane's producer call has started but not yet
    // returned. The historical bug dispatched quantum 1's asset lane
    // synchronously-to-the-caller: the scan (and therefore quantum 2's
    // dispatch) could not proceed until this call returned.
    var attempts: usize = 0;
    var caught_overlap = false;
    while (attempts < db_mod.default_test_wait_attempts) : (attempts += 1) {
        if (slow_asset.started.load(.acquire) and !slow_asset.finished.load(.acquire)) {
            caught_overlap = true;
            break;
        }
        db_mod.sleepPollInterval();
    }
    try std.testing.expect(caught_overlap);
    try std.testing.expect(!slow_asset.finished.load(.acquire));

    // The dense lane's quantum -- scanned strictly after the still-in-flight
    // asset quantum -- must already have been dispatched, run, published,
    // and checkpointed its own replay cursor scope, independent of the
    // sibling asset lane's own quantum still being in flight; see
    // "Two-Stream Execution Model" in ENRICHMENTS.md.
    var dense_cursor_seen = false;
    attempts = 0;
    while (attempts < db_mod.default_test_wait_attempts) : (attempts += 1) {
        if (try db_mod.enrichment_state.loadReplayCursor(alloc, db.enrichment.runtime.?.store, "generated.dense")) |cursor| {
            var owned = cursor;
            owned.deinit(alloc);
            dense_cursor_seen = true;
            break;
        }
        if (slow_asset.finished.load(.acquire)) break;
        db_mod.sleepPollInterval();
    }
    try std.testing.expect(dense_cursor_seen);
    // The overlap was real, not a lucky race after the asset call already
    // returned: it was still parked when the dense cursor appeared.
    try std.testing.expect(!slow_asset.finished.load(.acquire));

    slow_asset.allow();
    try db.runUntilIdle();

    // Both lanes' work is durable and correct once the whole pass has
    // drained -- the dense cursor observed above while asset was still
    // parked was real published progress, not a stale/partial artifact.
    var result = try db.search(alloc, .{
        .index_name = "title_dense",
        .dense = .{ .vector = &.{ 1.0, 0.0, 0.0 }, .k = 1 },
    });
    defer result.deinit();
    try std.testing.expectEqual(@as(u32, 1), result.total_hits);
    try std.testing.expectEqualStrings("doc:b", result.hits[0].id);

    const artifact_key = try db_mod.internal_keys.artifactNamedPrefixAlloc(alloc, "doc:a", "asset", "summary_v1");
    defer alloc.free(artifact_key);
    const artifact_value = try db.core.store.get(alloc, artifact_key);
    defer alloc.free(artifact_value);
    try std.testing.expectEqualStrings("asset:alpha summary source", artifact_value);
    try std.testing.expectEqual(@as(usize, 1), slow_asset.calls.load(.acquire));
}

test "synchronous window checkpoints never advance the asset cursor past queued or in-flight extractor work" {
    if (builtin.single_threaded or builtin.os.tag == .freestanding) return error.SkipZigTest;
    const alloc = std.testing.allocator;

    var path_tmp = try db_mod.TestDirectory.init("db");
    defer path_tmp.cleanup();
    const path = path_tmp.path().ptr;
    defer db_mod.cleanupTempDir(path);

    const c = struct {
        extern fn setenv(name: [*:0]const u8, value: [*:0]const u8, overwrite: c_int) c_int;
        extern fn unsetenv(name: [*:0]const u8) c_int;
    };
    // A preparation quantum wide enough to hold every document's extractor
    // request (so the asset lane is dispatched only once, at the end of the
    // scan, with the earlier documents' requests queued behind the scan the
    // whole time) and a one-item synchronous replay window (so each
    // document's copy asset takes the scanner's mid-scan flush-and-checkpoint
    // path while those extractor requests are still queued).
    const preparation_env = "ANTFLY_ENRICHMENT_PREPARATION_WINDOW_ITEMS";
    const window_env = "ANTFLY_ENRICHMENT_WINDOW_ITEMS";
    try std.testing.expectEqual(@as(c_int, 0), c.setenv(preparation_env, "8", 1));
    defer _ = c.unsetenv(preparation_env);
    try std.testing.expectEqual(@as(c_int, 0), c.setenv(window_env, "1", 1));
    defer _ = c.unsetenv(window_env);

    var slow_asset = db_mod.SlowGatedAssetProducer{};
    var db = try DB.open(alloc, std.mem.span(path), .{
        .enrichment = .{
            .owner_id = "worker-a",
            .asset_producer = slow_asset.interface(),
            .inline_retry_max_attempts = 1,
        },
        .ttl_cleanup = .{ .enabled = false },
    });
    defer db.close();
    defer slow_asset.allow();

    try db.enrichment.addEnrichment(.{
        .name = "summary_v1",
        .kind = .asset,
        .field = "body",
        .content_type = "text/plain",
        .producer_json = "{\"type\":\"generator\",\"config\":{\"provider\":\"antfly\",\"model\":\"test-generator\"}}",
    });
    // A copy asset is synchronous scanner work planned after the extractor:
    // it publishes through the shared window, never through an execution
    // lane, so it is what fills the window at the end of each group.
    try db.enrichment.addEnrichment(.{
        .name = "title_copy_v1",
        .kind = .asset,
        .field = "title",
        .content_type = "text/plain",
        .producer_json = "{\"type\":\"copy\"}",
    });

    // Every document queues an extractor request (deferred to the lane) and
    // a copy request (published synchronously by the scanner).
    try db.batch(.{
        .writes = &.{
            .{ .key = "doc:a", .value = "{\"title\":\"first title\",\"body\":\"alpha summary source\"}" },
            .{ .key = "doc:b", .value = "{\"title\":\"second title\",\"body\":\"beta summary source\"}" },
            .{ .key = "doc:c", .value = "{\"title\":\"third title\",\"body\":\"gamma summary source\"}" },
        },
        .sync_level = .write,
    });

    // The scan reaches the end and dispatches the asset lane, which parks in
    // the producer on its first call.
    var attempts: usize = 0;
    var parked = false;
    while (attempts < db_mod.slow_test_wait_attempts) : (attempts += 1) {
        if (slow_asset.started.load(.acquire) and !slow_asset.finished.load(.acquire)) {
            parked = true;
            break;
        }
        db_mod.sleepPollInterval();
    }
    try std.testing.expect(parked);

    // The copy work was published and checkpointed mid-scan: the dense
    // stream had nothing outstanding, so its cursor advanced...
    for ([_][]const u8{ "doc:a", "doc:b", "doc:c" }) |doc_key| {
        const copy_key = try db_mod.internal_keys.artifactNamedPrefixAlloc(alloc, doc_key, "asset", "title_copy_v1");
        defer alloc.free(copy_key);
        const copied = try db.core.store.get(alloc, copy_key);
        alloc.free(copied);
    }
    const dense_cursor = try db_mod.enrichment_state.loadReplayCursor(alloc, db.enrichment.runtime.?.store, "generated.dense");
    try std.testing.expect(dense_cursor != null);
    var owned_dense_cursor = dense_cursor.?;
    owned_dense_cursor.deinit(alloc);
    try std.testing.expect(!slow_asset.finished.load(.acquire));
    // ...but the asset stream's cursor must not exist yet: at each of those
    // checkpoints the documents' asset requests were still queued for the
    // lane, and now the lane's quantum is in flight. A cursor at doc:c would
    // tell a restart that all three extractions are durable, and the
    // historical mid-scan checkpoint wrote exactly that for both streams.
    if (try db_mod.enrichment_state.loadReplayCursor(alloc, db.enrichment.runtime.?.store, "generated.assets")) |cursor| {
        var owned = cursor;
        defer owned.deinit(alloc);
        std.debug.print("unexpected asset cursor while extraction is outstanding: sequence={d} doc_key={s}\n", .{ owned.sequence, owned.doc_key });
        return error.TestUnexpectedResult;
    }

    slow_asset.allow();
    try db.runUntilIdle();

    // The completed pass advanced the applied sequence and retired both
    // stream cursors; every extraction is durable.
    try std.testing.expect((try db_mod.enrichment_state.loadReplayCursor(alloc, db.enrichment.runtime.?.store, "generated.assets")) == null);
    try std.testing.expect((try db_mod.enrichment_state.loadReplayCursor(alloc, db.enrichment.runtime.?.store, "generated.dense")) == null);
    for ([_][]const u8{ "doc:a", "doc:b", "doc:c" }) |doc_key| {
        const artifact_key = try db_mod.internal_keys.artifactNamedPrefixAlloc(alloc, doc_key, "asset", "summary_v1");
        defer alloc.free(artifact_key);
        const artifact_value = try db.core.store.get(alloc, artifact_key);
        defer alloc.free(artifact_value);
        try std.testing.expect(std.mem.startsWith(u8, artifact_value, "asset:"));
    }
}

test "cross-quantum dispatch clones queued requests so later caller reuse cannot corrupt an in-flight asset quantum" {
    if (builtin.single_threaded or builtin.os.tag == .freestanding) return error.SkipZigTest;
    const alloc = std.testing.allocator;

    var path_tmp = try db_mod.TestDirectory.init("db");
    defer path_tmp.cleanup();
    const path = path_tmp.path().ptr;
    defer db_mod.cleanupTempDir(path);

    const c = struct {
        extern fn setenv(name: [*:0]const u8, value: [*:0]const u8, overwrite: c_int) c_int;
        extern fn unsetenv(name: [*:0]const u8) c_int;
    };
    const window_env = "ANTFLY_ENRICHMENT_PREPARATION_WINDOW_ITEMS";
    try std.testing.expectEqual(@as(c_int, 0), c.setenv(window_env, "1", 1));
    defer _ = c.unsetenv(window_env);

    var slow_asset = db_mod.SlowGatedAssetProducer{};
    var deterministic = db_mod.embedder_mod.DeterministicDenseEmbedder{};
    // Manual/hosted maintenance mode (Lite's synchronous `catchUpUntilGuarded`
    // drive, e.g. `runUntilIdle` called from the app thread), not the
    // background worker -- this test exercises that path specifically.
    var db = try DB.open(alloc, std.mem.span(path), .{
        .start_optional_runtime_workers = false,
        .enrichment = .{
            .owner_id = "worker-a",
            .asset_producer = slow_asset.interface(),
            .dense_embedder = deterministic.interface(),
            .inline_retry_max_attempts = 1,
        },
        .ttl_cleanup = .{ .enabled = false },
    });
    defer db.close();
    // Safety net, evaluated (LIFO) before the `db.close()` defer above: if
    // any assertion below fails before the unblocker thread (or its
    // explicit `allow()`) runs, this still releases the parked producer so
    // `db.close()`'s shutdown drain cannot hang the whole test binary
    // waiting on a gate nothing will ever open. Calling `allow()` twice is
    // harmless (it is a one-way latch).
    defer slow_asset.allow();

    try db.enrichment.addEnrichment(.{
        .name = "summary_v1",
        .kind = .asset,
        .field = "body",
        .content_type = "text/plain",
        .producer_json = "{\"type\":\"generator\",\"config\":{\"provider\":\"antfly\",\"model\":\"test-generator\"}}",
    });
    try db.addIndex(.{
        .name = "title_dense",
        .kind = .dense_vector,
        .config_json =
        \\{"field":"embedding","dims":3,"publication_policy":"progressive","generator":{"kind":"dense_embedding","source_field":"title","embedding_name":"title_dense"}}
        ,
    });

    // doc:a queues the (parked) asset quantum first. The 24 dense-only
    // documents that follow each become their own preparation quantum
    // (window = 1 item): every one of those quanta clones its own request,
    // dispatches, publishes, and repeatedly reallocates/frees
    // request_plan_cache and the deferred-request lists while doc:a's asset
    // quantum is still parked inside its producer call. If the asset lane's
    // request were still borrowing its doc_key/source_field strings from
    // request_plan_cache -- the confirmed root cause of the reverted first
    // pipelining attempt -- this churn would read freed/reused memory by
    // the time the producer call actually reads them, corrupting or
    // crashing instead of producing "doc:a"'s own artifact. Running under
    // `std.testing.allocator` also catches any use-after-free directly.
    const dense_doc_count = 24;
    var writes: [1 + dense_doc_count]db_mod.types.BatchWrite = undefined;
    writes[0] = .{ .key = "doc:a", .value = "{\"body\":\"alpha summary source\"}" };
    var dense_keys: [dense_doc_count][]u8 = undefined;
    var dense_values: [dense_doc_count][]u8 = undefined;
    for (0..dense_doc_count) |i| {
        dense_keys[i] = try std.fmt.allocPrint(alloc, "doc:b{d}", .{i});
        dense_values[i] = try std.fmt.allocPrint(alloc, "{{\"title\":\"independent title {d}\"}}", .{i});
        writes[1 + i] = .{ .key = dense_keys[i], .value = dense_values[i] };
    }
    defer for (0..dense_doc_count) |i| {
        alloc.free(dense_keys[i]);
        alloc.free(dense_values[i]);
    };

    try db.batch(.{ .writes = &writes, .sync_level = .write });

    const sequence = db.core.nextEnrichmentSequence();

    const Unblocker = struct {
        fn run(producer: *db_mod.SlowGatedAssetProducer) void {
            // Give the manual drive time to dispatch doc:a's asset quantum
            // and churn through every dense-only quantum before releasing
            // the parked producer call.
            db_mod.sleepNs(80 * std.time.ns_per_ms);
            producer.allow();
        }
    };
    const unblock_thread = try std.Thread.spawn(.{}, Unblocker.run, .{&slow_asset});
    defer unblock_thread.join();

    try db.enrichment.runUntil(sequence);

    try std.testing.expectEqual(@as(usize, 1), slow_asset.calls.load(.acquire));
    try std.testing.expect(slow_asset.finished.load(.acquire));

    const artifact_key = try db_mod.internal_keys.artifactNamedPrefixAlloc(alloc, "doc:a", "asset", "summary_v1");
    defer alloc.free(artifact_key);
    const artifact_value = try db.core.store.get(alloc, artifact_key);
    defer alloc.free(artifact_value);
    try std.testing.expectEqualStrings("asset:alpha summary source", artifact_value);

    // Durable coverage accounting (not approximate vector-search recall) is
    // the exact signal that every one of the 24 independently dispatched
    // dense quanta produced and published its own embedding correctly
    // despite the concurrent request_plan_cache/chunk_cache churn.
    const stats = try db.stats(alloc);
    defer db_mod.types.freeDBStats(alloc, stats);
    for (stats.indexes) |index_stats| {
        if (!std.mem.eql(u8, index_stats.name, "title_dense")) continue;
        try std.testing.expectEqual(@as(u64, dense_doc_count), index_stats.coverage_produced_count);
        try std.testing.expectEqual(@as(u64, 0), index_stats.coverage_terminal_failed_count);
    }
}

test "retired repair cleanup resumes marker retirement and removes corrupt sidecars after reopen" {
    const alloc = std.testing.allocator;

    var path_tmp = try db_mod.TestDirectory.init("db");
    defer path_tmp.cleanup();
    const path = path_tmp.path().ptr;
    defer db_mod.cleanupTempDir(path);

    const issue: db_mod.types.ArtifactRepairIssue = .{
        .artifact_kind = .embedding,
        .index_name = "retired_index",
        .doc_key = "doc:retired",
        .artifact_name = "retired_index",
        .artifact_key = "retired-physical-artifact",
        .reason = .enrichment_failed,
    };
    const issue_key = try db_mod.artifactRepairIssueKeyForIssueAlloc(alloc, issue);
    defer alloc.free(issue_key);
    const marker_prefix = try db_mod.internal_keys.enrichmentTerminalFailureIssuePrefixAlloc(alloc, issue_key);
    defer alloc.free(marker_prefix);
    const generation_key = try db_mod.internal_keys.enrichmentTerminalFailureGenerationKeyAlloc(alloc, issue_key);
    defer alloc.free(generation_key);
    const completion_key = try db_mod.internal_keys.artifactRepairCompletionKeyAlloc(
        alloc,
        @tagName(issue.artifact_kind),
        issue.artifact_key,
    );
    defer alloc.free(completion_key);
    const kind_key = try db_mod.artifactRepairIssueKindKeyForIssueAlloc(alloc, issue);
    defer alloc.free(kind_key);

    {
        var db = try DB.open(alloc, std.mem.span(path), .{
            .start_index_workers = false,
            .start_optional_runtime_workers = false,
            .ttl_cleanup = .{ .enabled = false },
        });
        defer db.close();

        for (0..db_mod.enrichment_terminal_failure_retirement_page_size + 7) |i| {
            var sequenced = issue;
            sequenced.sequence = 20_000 + @as(u64, @intCast(i));
            try db.recordArtifactRepairIssue(alloc, sequenced);
        }
        try db.enrichment.runArtifactRepairMetadataMaintenanceUntilIdle();
        // The primary key remains trustworthy even when the payload does not.
        // Cleanup must use it to drain marker, generation, secondary, and
        // single-flight sidecars without following corrupt value fields.
        try db.core.store.put(issue_key, "malformed-repair-issue");

        try std.testing.expect(try DB.drainRetiredIndexArtifactRepairIssuesPageContext(
            db.async_context,
            issue.index_name,
        ));
        const primary = try db.core.store.get(alloc, issue_key);
        alloc.free(primary);
        const remaining_markers = try db.core.store.scanPrefixPage(
            alloc,
            marker_prefix,
            null,
            db_mod.enrichment_terminal_failure_retirement_page_size,
        );
        defer db_mod.docstore_mod.DocStore.freeResults(alloc, remaining_markers);
        try std.testing.expectEqual(@as(usize, 7), remaining_markers.len);
    }

    var reopened = try DB.open(alloc, std.mem.span(path), .{
        .start_index_workers = false,
        .start_optional_runtime_workers = false,
        .ttl_cleanup = .{ .enabled = false },
    });
    defer reopened.close();

    var drained = false;
    for (0..8) |_| {
        if (!try DB.drainRetiredIndexArtifactRepairIssuesPageContext(
            reopened.async_context,
            issue.index_name,
        )) {
            drained = true;
            break;
        }
    }
    try std.testing.expect(drained);
    try std.testing.expectError(error.NotFound, reopened.core.store.get(alloc, issue_key));
    try std.testing.expectError(error.NotFound, reopened.core.store.get(alloc, kind_key));
    try std.testing.expectError(error.NotFound, reopened.core.store.get(alloc, generation_key));
    try std.testing.expectError(error.NotFound, reopened.core.store.get(alloc, completion_key));
    const remaining_markers = try reopened.core.store.scanPrefixPage(alloc, marker_prefix, null, 1);
    defer db_mod.docstore_mod.DocStore.freeResults(alloc, remaining_markers);
    try std.testing.expectEqual(@as(usize, 0), remaining_markers.len);

    try reopened.enrichment.runArtifactRepairMetadataMaintenanceUntilIdle();
    try std.testing.expect(try reopened.artifactRepairSummaryReady(alloc));
}

test "db generated enrichment recovery skips empty fields and advances coverage across reopen" {
    const alloc = std.testing.allocator;
    var path_tmp = try db_mod.TestDirectory.init("db");
    defer path_tmp.cleanup();
    const path = path_tmp.path().ptr;
    defer db_mod.cleanupTempDir(path);
    const indexes = [_]db_mod.types.IndexConfig{
        .{ .name = "dense", .kind = .dense_vector, .config_json = "{\"field\":\"embedding\",\"dims\":3,\"generator\":{\"kind\":\"dense_embedding\",\"source_field\":\"body\"}}" },
        .{ .name = "sparse", .kind = .sparse_vector, .config_json = "{\"field\":\"sparse_embedding\",\"generator\":{\"kind\":\"sparse_embedding\",\"source_field\":\"body\"}}" },
    };
    var target: u64 = 0;
    {
        // Leave durable replay for the next process: it must not interpret a
        // missing artifact as permission to send an empty source to inference.
        var db = try DB.open(alloc, std.mem.span(path), .{
            .start_index_workers = false,
            .ttl_cleanup = .{ .enabled = false },
        });
        defer db.close();
        try db.enrichment.addEnrichment(.{ .name = "body_chunks", .kind = .chunk, .field = "body", .chunk_size = 512 });
        try db.addIndex(.{ .name = "chunk_text", .kind = .full_text, .config_json = "{\"chunk_name\":\"body_chunks\"}" });
        for (indexes) |index| try db.addIndex(index);
        try db.batch(.{
            .writes = &.{
                .{ .key = "empty", .value = "{\"body\":\"\"}" },
                .{ .key = "missing", .value = "{}" },
                .{ .key = "null", .value = "{\"body\":null}" },
                .{ .key = "whitespace", .value = "{\"body\":\" \\t\\n\"}" },
                .{ .key = "full", .value = "{\"body\":\"portable databases\"}" },
            },
            .sync_level = .write,
        });
        target = db.pendingWorkStats().derived_target_sequence;
    }
    var dense: db_mod.CountingDenseEmbedder = .{};
    var sparse: db_mod.CountingSparseEmbedder = .{};
    // Repeat the reopen after recovery to prove the empty source stays covered
    // without inference, rather than being retried whenever a handle opens.
    for (0..2) |_| {
        var db = try DB.open(alloc, std.mem.span(path), .{
            .start_index_workers = false,
            .ttl_cleanup = .{ .enabled = false },
            .enrichment = .{ .owner_id = "recovery", .dense_embedder = dense.interface(), .sparse_embedder = sparse.interface() },
        });
        defer db.close();
        try db.runUntilIdle();
        try std.testing.expectEqual(@as(usize, 2), dense.calls);
        try std.testing.expectEqual(@as(usize, 2), sparse.calls);
        const pending = db.pendingWorkStats();
        try std.testing.expectEqual(pending.enrichment.target_sequence, pending.enrichment.applied_sequence);
        try std.testing.expect(pending.enrichment.applied_sequence >= target);
        for (indexes) |index| {
            try std.testing.expect((try db.core.loadAppliedSequence(alloc, index.name)) >= target);
            for ([_][]const u8{ "empty", "missing", "null", "whitespace", "full" }) |key| {
                const artifact = try db_mod.expectedDocumentEmbeddingArtifactKeyAlloc(alloc, key, index.name);
                defer alloc.free(artifact);
                const value = db.core.store.get(alloc, artifact) catch |err| switch (err) {
                    error.NotFound => {
                        try std.testing.expect(!std.mem.eql(u8, key, "whitespace") and !std.mem.eql(u8, key, "full"));
                        continue;
                    },
                    else => return err,
                };
                defer alloc.free(value);
                try std.testing.expect(std.mem.eql(u8, key, "whitespace") or std.mem.eql(u8, key, "full"));
            }
        }
    }
}

test "db leased enrichment worker materializes chunk artifacts" {
    const alloc = std.testing.allocator;

    var path_tmp = try db_mod.TestDirectory.init("db");
    defer path_tmp.cleanup();
    const path = path_tmp.path().ptr;
    defer db_mod.cleanupTempDir(path);

    var deterministic = db_mod.embedder_mod.DeterministicDenseEmbedder{};
    var db = try DB.open(alloc, std.mem.span(path), .{
        .enrichment = .{
            .owner_id = "worker-a",
            .dense_embedder = deterministic.interface(),
        },
    });
    defer db.close();

    try db.addIndex(.{
        .name = "dv_v1",
        .kind = .dense_vector,
        .config_json = "{\"field\":\"embedding\",\"dims\":3,\"generator\":{\"kind\":\"dense_embedding\",\"source_field\":\"body\",\"chunk_name\":\"body_chunks_v1\",\"chunk_size\":8,\"chunk_overlap\":2}}",
    });

    try db.batch(.{
        .writes = &.{
            .{ .key = "doc:a", .value = "{\"body\":\"abcdefghijklmno\"}" },
        },
        .sync_level = .write,
    });

    try db.enrichment.runtime.?.waitForApplied(1);
    try db_mod.waitForDerivedReplayTarget(&db);

    const chunk_prefix = try db_mod.internal_keys.artifactNamedPrefixAlloc(alloc, "doc:a", "chunk", "body_chunks_v1");
    defer alloc.free(chunk_prefix);
    const chunk_zero = try db_mod.expectedChunkArtifactKeyAlloc(alloc, "doc:a", "body_chunks_v1", 0);
    defer alloc.free(chunk_zero);
    const artifacts = try db.core.store.scanPrefix(alloc, chunk_prefix);
    defer db_mod.docstore_mod.DocStore.freeResults(alloc, artifacts);

    var chunk_count: usize = 0;
    var embedding_count: usize = 0;
    for (artifacts) |entry| {
        if (db_mod.internal_keys.isChunkArtifactRecordKey(entry.key)) {
            if (chunk_count == 0) {
                try std.testing.expectEqualStrings(chunk_zero, entry.key);
                try std.testing.expect(std.mem.indexOf(u8, entry.value, "\"abcdefgh\"") != null);
            }
            chunk_count += 1;
            continue;
        }
        if (db_mod.internal_keys.isDerivedEmbeddingArtifactKey(entry.key)) {
            embedding_count += 1;
            try db_mod.expectDenseEmbeddingArtifactWithSourceHash(alloc, entry.value, 3);
        }
    }
    try std.testing.expect(chunk_count > 0);
    try std.testing.expectEqual(@as(usize, 3), embedding_count);
}

test "db leased enrichment worker keeps chunk storage ephemeral when store_chunks is false" {
    const alloc = std.testing.allocator;

    var path_tmp = try db_mod.TestDirectory.init("db");
    defer path_tmp.cleanup();
    const path = path_tmp.path().ptr;
    defer db_mod.cleanupTempDir(path);

    var counting = db_mod.CountingDenseEmbedder{};
    var db = try DB.open(alloc, std.mem.span(path), .{
        .enrichment = .{
            .owner_id = "worker-a",
            .dense_embedder = counting.interface(),
        },
    });
    defer db.close();

    try db.addIndex(.{
        .name = "dv_v1",
        .kind = .dense_vector,
        .config_json = "{\"field\":\"embedding\",\"dims\":3,\"generator\":{\"kind\":\"dense_embedding\",\"source_field\":\"body\",\"chunk_name\":\"body_chunks_v1\",\"chunker\":{\"provider\":\"antfly\",\"store_chunks\":false,\"text\":{\"target_tokens\":8,\"overlap_tokens\":2}}}}",
    });

    try db.batch(.{
        .writes = &.{
            .{ .key = "doc:a", .value = "{\"body\":\"abcdefghijklmno\"}" },
        },
        .sync_level = .write,
    });

    try db.enrichment.runtime.?.waitForApplied(1);
    try db_mod.waitForDerivedReplayTarget(&db);

    const chunk_prefix = try db_mod.internal_keys.artifactNamedPrefixAlloc(alloc, "doc:a", "chunk", "body_chunks_v1");
    defer alloc.free(chunk_prefix);
    {
        const artifacts = try db.core.store.scanPrefix(alloc, chunk_prefix);
        defer db_mod.docstore_mod.DocStore.freeResults(alloc, artifacts);

        var chunk_count: usize = 0;
        var embedding_count: usize = 0;
        for (artifacts) |entry| {
            if (db_mod.internal_keys.isChunkArtifactRecordKey(entry.key)) chunk_count += 1;
            if (db_mod.internal_keys.isDerivedEmbeddingArtifactKey(entry.key)) embedding_count += 1;
        }

        try std.testing.expectEqual(@as(usize, 0), chunk_count);
        try std.testing.expectEqual(@as(usize, 3), embedding_count);
        try std.testing.expectEqual(@as(usize, 3), counting.calls);
    }

    // Replaying an unchanged source must retain the virtual chunks' derived
    // embeddings even though their intermediate chunk rows are intentionally
    // ephemeral. Deleting those embeddings before the source-hash check both
    // regresses serving counts and needlessly invokes the provider again.
    try db.batch(.{
        .writes = &.{
            .{ .key = "doc:a", .value = "{\"body\":\"abcdefghijklmno\",\"revision\":2}" },
        },
        .sync_level = .write,
    });
    try db.enrichment.runtime.?.waitForApplied(2);
    try db_mod.waitForDerivedReplayTarget(&db);

    const replayed_artifacts = try db.core.store.scanPrefix(alloc, chunk_prefix);
    defer db_mod.docstore_mod.DocStore.freeResults(alloc, replayed_artifacts);
    var chunk_count: usize = 0;
    var embedding_count: usize = 0;
    for (replayed_artifacts) |entry| {
        if (db_mod.internal_keys.isChunkArtifactRecordKey(entry.key)) chunk_count += 1;
        if (db_mod.internal_keys.isDerivedEmbeddingArtifactKey(entry.key)) embedding_count += 1;
    }
    try std.testing.expectEqual(@as(usize, 0), chunk_count);
    try std.testing.expectEqual(@as(usize, 3), embedding_count);
    try std.testing.expectEqual(@as(usize, 3), counting.calls);

    const stats = try db.stats(alloc);
    defer db_mod.types.freeDBStats(alloc, stats);
}

test "db leased enrichment worker persists chunk storage when full text consumes chunked text" {
    const alloc = std.testing.allocator;

    var path_tmp = try db_mod.TestDirectory.init("db");
    defer path_tmp.cleanup();
    const path = path_tmp.path().ptr;
    defer db_mod.cleanupTempDir(path);

    var deterministic = db_mod.embedder_mod.DeterministicDenseEmbedder{};
    var db = try DB.open(alloc, std.mem.span(path), .{
        .enrichment = .{
            .owner_id = "worker-a",
            .dense_embedder = deterministic.interface(),
        },
        .start_index_workers = false,
    });
    defer db.close();

    try db.addIndex(.{
        .name = "dv_v1",
        .kind = .dense_vector,
        .config_json = "{\"field\":\"embedding\",\"dims\":3,\"generator\":{\"kind\":\"dense_embedding\",\"source_field\":\"body\",\"chunk_name\":\"body_chunks_v1\",\"chunker\":{\"provider\":\"antfly\",\"store_chunks\":false,\"text\":{\"target_tokens\":8,\"overlap_tokens\":2}}}}",
    });
    try db.addIndex(.{
        .name = "ft_chunks",
        .kind = .full_text,
        .config_json = "{\"chunk_name\":\"body_chunks_v1\"}",
    });

    try db.batch(.{
        .writes = &.{
            .{ .key = "doc:a", .value = "{\"body\":\"abcdefghijklmno\"}" },
        },
        .sync_level = .write,
    });

    try db.enrichment.runtime.?.waitForApplied(1);

    const chunk_prefix = try db_mod.internal_keys.artifactNamedPrefixAlloc(alloc, "doc:a", "chunk", "body_chunks_v1");
    defer alloc.free(chunk_prefix);
    const artifacts = try db.core.store.scanPrefix(alloc, chunk_prefix);
    defer db_mod.docstore_mod.DocStore.freeResults(alloc, artifacts);

    var chunk_count: usize = 0;
    for (artifacts) |entry| {
        if (db_mod.internal_keys.isChunkArtifactRecordKey(entry.key)) chunk_count += 1;
    }

    try std.testing.expect(chunk_count > 0);
}

test "db leased enrichment worker persists chunk storage when chunker enables full text indexing" {
    const alloc = std.testing.allocator;

    var path_tmp = try db_mod.TestDirectory.init("db");
    defer path_tmp.cleanup();
    const path = path_tmp.path().ptr;
    defer db_mod.cleanupTempDir(path);

    var deterministic = db_mod.embedder_mod.DeterministicDenseEmbedder{};
    var db = try DB.open(alloc, std.mem.span(path), .{
        .enrichment = .{
            .owner_id = "worker-a",
            .dense_embedder = deterministic.interface(),
        },
        .start_index_workers = false,
    });
    defer db.close();

    try db.addIndex(.{
        .name = "dv_v1",
        .kind = .dense_vector,
        .config_json = "{\"field\":\"embedding\",\"dims\":3,\"generator\":{\"kind\":\"dense_embedding\",\"source_field\":\"body\",\"artifact_name\":\"body_chunks_v1\",\"chunker\":{\"provider\":\"antfly\",\"store_chunks\":false,\"full_text_index\":{},\"text\":{\"target_tokens\":8,\"overlap_tokens\":2}}}}",
    });

    try db.batch(.{
        .writes = &.{
            .{ .key = "doc:a", .value = "{\"body\":\"abcdefghijklmno\"}" },
        },
        .sync_level = .write,
    });

    try db.enrichment.runtime.?.waitForApplied(1);

    const chunk_prefix = try db_mod.internal_keys.artifactNamedPrefixAlloc(alloc, "doc:a", "chunk", "body_chunks_v1");
    defer alloc.free(chunk_prefix);
    const artifacts = try db.core.store.scanPrefix(alloc, chunk_prefix);
    defer db_mod.docstore_mod.DocStore.freeResults(alloc, artifacts);

    var chunk_count: usize = 0;
    for (artifacts) |entry| {
        if (db_mod.internal_keys.isChunkArtifactRecordKey(entry.key)) chunk_count += 1;
    }

    try std.testing.expect(chunk_count > 0);
}

test "db full_text sync level does not precompute template chunk full text routing" {
    const alloc = std.testing.allocator;

    var path_tmp = try db_mod.TestDirectory.init("db");
    defer path_tmp.cleanup();
    const path = path_tmp.path().ptr;
    defer db_mod.cleanupTempDir(path);

    var deterministic = db_mod.embedder_mod.DeterministicDenseEmbedder{};
    var db = try DB.open(alloc, std.mem.span(path), .{
        // Keep the asynchronous enrichment worker from racing the assertion
        // that .full_text returns before template-derived chunks are routed.
        // Drain the same work explicitly below once that boundary is proven.
        .start_optional_runtime_workers = false,
        .enrichment = .{
            .owner_id = "worker-a",
            .dense_embedder = deterministic.interface(),
        },
    });
    defer db.close();

    try db.addIndex(.{
        .name = "full_text_index_v0",
        .kind = .full_text,
        .config_json = "{}",
    });
    try db.addIndex(.{
        .name = "semantic_template_chunked_idx",
        .kind = .dense_vector,
        .config_json = "{\"field\":\"embedding\",\"dims\":3,\"generator\":{\"kind\":\"dense_embedding\",\"source_field\":\"body\",\"source_template\":\"{{title}}\",\"artifact_name\":\"semantic_template_chunked_idx_chunks\",\"chunker\":{\"provider\":\"antfly\",\"store_chunks\":false,\"full_text_index\":{},\"text\":{\"target_tokens\":4,\"overlap_tokens\":1,\"separator\":\" \"}}}}",
    });

    try db.batch(.{
        .writes = &.{
            .{ .key = "doc:a", .value = "{\"title\":\"Alpha routing only in template chunks\",\"body\":\"body text without the keyword\"}" },
        },
        .sync_level = .full_text,
    });

    const pending = db.pendingWorkStats();
    try std.testing.expect(pending.enrichment.target_sequence >= 1);

    var result = try db.search(alloc, .{
        .index_name = "full_text_index_v0",
        .full_text = .{ .match = .{ .field = "body", .text = "routing" } },
        .return_mode = .parent,
    });
    defer result.deinit();

    try std.testing.expectEqual(@as(u32, 0), result.total_hits);

    try db.enrichment.runUntil(1);

    const chunk_prefix = try db_mod.internal_keys.artifactNamedPrefixAlloc(alloc, "doc:a", "chunk", "semantic_template_chunked_idx_chunks");
    defer alloc.free(chunk_prefix);
    const artifacts = try db.core.store.scanPrefix(alloc, chunk_prefix);
    defer db_mod.docstore_mod.DocStore.freeResults(alloc, artifacts);
    var chunk_count: usize = 0;
    for (artifacts) |entry| {
        if (db_mod.internal_keys.isChunkArtifactRecordKey(entry.key)) chunk_count += 1;
    }
    try std.testing.expect(chunk_count > 0);
}

test "db leased enrichment worker materializes chunk artifacts with durable lsm primary backend" {
    const alloc = std.testing.allocator;

    var path_tmp = try db_mod.TestDirectory.init("db");
    defer path_tmp.cleanup();
    const path = path_tmp.path().ptr;
    defer db_mod.cleanupTempDir(path);

    var deterministic = db_mod.embedder_mod.DeterministicDenseEmbedder{};
    var db = try DB.open(alloc, std.mem.span(path), .{
        .primary_backend = .{ .lsm = .{ .flush_threshold = 1 } },
        .enrichment = .{
            .owner_id = "worker-a",
            .dense_embedder = deterministic.interface(),
        },
    });
    defer db.close();

    try db.addIndex(.{
        .name = "dv_v1",
        .kind = .dense_vector,
        .config_json = "{\"field\":\"embedding\",\"dims\":3,\"generator\":{\"kind\":\"dense_embedding\",\"source_field\":\"body\",\"chunk_name\":\"body_chunks_v1\",\"chunk_size\":8,\"chunk_overlap\":2}}",
    });

    try db.batch(.{
        .writes = &.{
            .{ .key = "doc:a", .value = "{\"body\":\"abcdefghijklmno\"}" },
        },
        .sync_level = .write,
    });

    try db.enrichment.runtime.?.waitForApplied(1);
    try db_mod.waitForDerivedReplayTarget(&db);

    const chunk_prefix = try db_mod.internal_keys.artifactNamedPrefixAlloc(alloc, "doc:a", "chunk", "body_chunks_v1");
    defer alloc.free(chunk_prefix);
    const chunk_zero = try db_mod.expectedChunkArtifactKeyAlloc(alloc, "doc:a", "body_chunks_v1", 0);
    defer alloc.free(chunk_zero);
    const artifacts = try db.core.store.scanPrefix(alloc, chunk_prefix);
    defer db_mod.docstore_mod.DocStore.freeResults(alloc, artifacts);

    var chunk_count: usize = 0;
    var embedding_count: usize = 0;
    for (artifacts) |entry| {
        if (db_mod.internal_keys.isChunkArtifactRecordKey(entry.key)) {
            if (chunk_count == 0) {
                try std.testing.expectEqualStrings(chunk_zero, entry.key);
                try std.testing.expect(std.mem.indexOf(u8, entry.value, "\"abcdefgh\"") != null);
            }
            chunk_count += 1;
            continue;
        }
        if (db_mod.internal_keys.isDerivedEmbeddingArtifactKey(entry.key)) {
            embedding_count += 1;
            try db_mod.expectDenseEmbeddingArtifactWithSourceHash(alloc, entry.value, 3);
        }
    }
    try std.testing.expectEqual(@as(usize, 3), chunk_count);
    try std.testing.expectEqual(@as(usize, 3), embedding_count);
}

test "db shared embedding enrichment feeds multiple dense indexes" {
    const alloc = std.testing.allocator;

    var path_tmp = try db_mod.TestDirectory.init("db");
    defer path_tmp.cleanup();
    const path = path_tmp.path().ptr;
    defer db_mod.cleanupTempDir(path);

    var deterministic = db_mod.embedder_mod.DeterministicDenseEmbedder{};
    var db = try DB.open(alloc, std.mem.span(path), .{
        .enrichment = .{
            .owner_id = "worker-a",
            .dense_embedder = deterministic.interface(),
        },
    });
    defer db.close();

    const shared_cfg = "{\"field\":\"embedding\",\"dims\":3,\"generator\":{\"kind\":\"dense_embedding\",\"source_field\":\"body\",\"embedding_name\":\"shared_dense_v1\"}}";
    try db.addIndex(.{
        .name = "dv_a",
        .kind = .dense_vector,
        .config_json = shared_cfg,
    });
    try db.addIndex(.{
        .name = "dv_b",
        .kind = .dense_vector,
        .config_json = shared_cfg,
    });

    try db.batch(.{
        .writes = &.{
            .{ .key = "doc:a", .value = "{\"body\":\"shared embedding text\"}" },
        },
        .sync_level = .write,
    });

    try db.enrichment.runtime.?.waitForApplied(1);
    try db_mod.waitForDerivedReplayTarget(&db);

    const shared_artifact_key = try db_mod.expectedDocumentEmbeddingArtifactKeyAlloc(alloc, "doc:a", "shared_dense_v1");
    defer alloc.free(shared_artifact_key);
    const artifacts = try db.core.store.scanPrefix(alloc, shared_artifact_key);
    defer db_mod.docstore_mod.DocStore.freeResults(alloc, artifacts);
    try std.testing.expectEqual(@as(usize, 1), artifacts.len);

    const query_vec = try deterministic.interface().embedDense(alloc, "", "shared embedding text", 3);
    defer alloc.free(query_vec);

    var result_a = try db.search(alloc, .{
        .index_name = "dv_a",
        .dense = .{ .vector = query_vec, .k = 1 },
    });
    defer result_a.deinit();
    try std.testing.expectEqual(@as(u32, 1), result_a.total_hits);
    try std.testing.expectEqualStrings("doc:a", result_a.hits[0].id);

    var result_b = try db.search(alloc, .{
        .index_name = "dv_b",
        .dense = .{ .vector = query_vec, .k = 1 },
    });
    defer result_b.deinit();
    try std.testing.expectEqual(@as(u32, 1), result_b.total_hits);
    try std.testing.expectEqualStrings("doc:a", result_b.hits[0].id);
}

test "db shared enrichment failure parks repair debt for every consumer" {
    const alloc = std.testing.allocator;

    var path_tmp = try db_mod.TestDirectory.init("db");
    defer path_tmp.cleanup();
    const path = path_tmp.path().ptr;
    defer db_mod.cleanupTempDir(path);

    var gated = db_mod.GateDenseEmbedder{};
    gated.allowed_successes.store(0, .release);
    var db = try DB.open(alloc, std.mem.span(path), .{
        .start_optional_runtime_workers = false,
        .enrichment = .{
            .owner_id = "worker-a",
            .dense_embedder = gated.interface(),
            .inline_retry_max_attempts = 1,
            .worker_retry_max_attempts = 2,
        },
    });
    defer db.close();

    const shared_cfg = "{\"field\":\"embedding\",\"dims\":3,\"generator\":{\"kind\":\"dense_embedding\",\"source_field\":\"body\",\"embedding_name\":\"shared_dense_v1\"}}";
    try db.addIndex(.{ .name = "dv_a", .kind = .dense_vector, .config_json = shared_cfg });
    try db.addIndex(.{ .name = "dv_b", .kind = .dense_vector, .config_json = shared_cfg });
    try db.batch(.{
        .writes = &.{.{ .key = "doc:a", .value = "{\"body\":\"shared failure\"}" }},
        .sync_level = .write,
    });
    const sequence = db.core.nextEnrichmentSequence();
    try std.testing.expectError(error.EnrichmentRetryInProgress, db.enrichment.runUntil(sequence));
    const retrying = db.enrichment.runtime.?.stats();
    try std.testing.expect(retrying.retrying);
    // Request-owned provider retries are tracked by durable request identity;
    // they must not consume the unrelated table-wide pipeline retry budget.
    try std.testing.expectEqual(@as(u32, 0), retrying.consecutive_retry_count);
    try std.testing.expectEqual(@as(u64, 1), retrying.retryable_error_count);
    db_mod.sleepNs(550 * std.time.ns_per_ms);
    try db.enrichment.runUntil(sequence);

    for ([_][]const u8{ "dv_a", "dv_b" }) |index_name| {
        const issues = try db.listArtifactRepairIssues(alloc, .embedding, index_name, 0);
        defer db_mod.types.freeArtifactRepairIssues(alloc, issues);
        try std.testing.expectEqual(@as(usize, 1), issues.len);
        try std.testing.expectEqualStrings(index_name, issues[0].index_name);
        try std.testing.expectEqualStrings("shared_dense_v1", issues[0].artifact_name);
        try std.testing.expectEqual(sequence, issues[0].sequence);
        try std.testing.expectEqual(@as(u64, 2), issues[0].generation_attempts);
        try std.testing.expectEqualStrings("EmbedRateLimited", issues[0].generation_error);
        try std.testing.expectEqual(@as(u64, 0), issues[0].attempts);
        try std.testing.expectEqualStrings("", issues[0].last_error);
    }
    try std.testing.expect(db.enrichment.runtime.?.indexHasIsolatedFailure("dv_a"));
    try std.testing.expect(db.enrichment.runtime.?.indexHasIsolatedFailure("dv_b"));

    // Model legacy debt discovered before generation failures were added to
    // the repair ledger. A forced generation rebuild may complete its work,
    // but settled-incomplete coverage must not be called repaired.
    for ([_][]const u8{ "dv_a", "dv_b" }) |index_name| {
        const issues = try db.listArtifactRepairIssues(alloc, .embedding, index_name, 0);
        defer db_mod.types.freeArtifactRepairIssues(alloc, issues);
        for (issues) |issue| try db.clearArtifactRepairIssue(alloc, issue);
    }
    gated.allowAll();
    const repair = try db.repairArtifactIssuesWithRequest(alloc, .{
        .target = .index,
        .index_name = "dv_a",
        .limit = 1,
        .force = true,
        .repair_job_id = 460,
        .repair_job_created_at_ms = 1_777_777,
    });
    try std.testing.expectEqual(@as(u64, 1), repair.indexes_rebuilt);
    try std.testing.expectEqual(@as(u64, 1), repair.indexes_degraded_after);
    try std.testing.expectEqual(@as(u64, 0), repair.repaired);
    try std.testing.expect(repair.debt_remaining);

    // A crash-idempotent redispatch must classify the committed generation,
    // not equate the durable completion marker with a healthy repair.
    const replayed = try db.repairArtifactIssuesWithRequest(alloc, .{
        .target = .index,
        .index_name = "dv_a",
        .limit = 1,
        .force = true,
        .repair_job_id = 460,
        .repair_job_created_at_ms = 1_777_777,
    });
    try std.testing.expectEqual(@as(u64, 1), replayed.indexes_rebuilt);
    try std.testing.expectEqual(@as(u64, 1), replayed.indexes_degraded_after);
    try std.testing.expectEqual(@as(u64, 0), replayed.repaired);
    try std.testing.expect(replayed.debt_remaining);
}

test "db upstream asset failure dominates downstream coverage in one replay sequence" {
    const alloc = std.testing.allocator;

    const FailingProducer = struct {
        fn producer(self: *@This()) db_mod.asset_producer_mod.Producer {
            return .{ .ptr = self, .vtable = &.{
                .produce = produce,
                .invocation_memory_for_requests = db_mod.TestAssetProducer.invocationMemory,
            } };
        }

        fn produce(_: *anyopaque, _: Allocator, _: db_mod.asset_producer_mod.Request) ![]u8 {
            return error.PermanentAssetFailure;
        }
    };

    var path_tmp = try db_mod.TestDirectory.init("db");
    defer path_tmp.cleanup();
    const path = path_tmp.path().ptr;
    defer db_mod.cleanupTempDir(path);

    var failing = FailingProducer{};
    var deterministic = db_mod.embedder_mod.DeterministicDenseEmbedder{};
    var db = try DB.open(alloc, std.mem.span(path), .{
        .start_optional_runtime_workers = false,
        .enrichment = .{
            .owner_id = "worker-a",
            .asset_producer = failing.producer(),
            .dense_embedder = deterministic.interface(),
            .inline_retry_max_attempts = 1,
            .worker_retry_max_attempts = 1,
        },
    });
    defer db.close();

    try db.enrichment.addEnrichment(.{
        .name = "summary_v1",
        .kind = .asset,
        .field = "body",
        .content_type = "text/plain",
        .producer_json = "{\"type\":\"generator\",\"config\":{\"provider\":\"antfly\",\"model\":\"test-generator\"}}",
    });
    try db.enrichment.addEnrichment(.{
        .name = "summary_chunks_v1",
        .kind = .chunk,
        .field = "value",
        .source_artifact_name = "summary_v1",
        .chunk_size = 8,
    });
    try db.enrichment.addEnrichment(.{
        .name = "summary_dense_v1",
        .kind = .embedding,
        .field = "value",
        .source_artifact_name = "summary_chunks_v1",
        .expected_dims = 3,
    });
    try db.addIndex(.{
        .name = "semantic",
        .kind = .dense_vector,
        .config_json = "{\"field\":\"embedding\",\"dims\":3,\"embedding_name\":\"summary_dense_v1\"}",
    });
    try db.batch(.{
        .writes = &.{.{ .key = "doc:a", .value = "{\"body\":\"alpha\"}" }},
        .sync_level = .write,
    });
    const sequence = db.core.nextEnrichmentSequence();
    try db.enrichment.runUntil(sequence);

    const generation = db.core.index_manager.coverageGenerationForIndex("semantic") orelse return error.TestUnexpectedResult;
    const marker_key = try db_mod.internal_keys.derivedCoverageOutcomeKeyAlloc(alloc, "semantic", generation, "doc:a");
    defer alloc.free(marker_key);
    const outcome = try db.core.store.get(alloc, marker_key);
    defer alloc.free(outcome);
    try std.testing.expectEqualStrings("terminal_failed", outcome);

    const issues = try db.listArtifactRepairIssues(alloc, .asset, "semantic", 0);
    defer db_mod.types.freeArtifactRepairIssues(alloc, issues);
    try std.testing.expectEqual(@as(usize, 1), issues.len);
    try std.testing.expectEqualStrings("summary_v1", issues[0].artifact_name);
}

test "db foreign inference provider failure releases enrichment waiter as terminal" {
    const alloc = std.testing.allocator;

    const ForeignFailingProducer = struct {
        calls: usize = 0,

        fn producer(self: *@This()) db_mod.asset_producer_mod.Producer {
            return .{ .ptr = self, .vtable = &.{
                .produce = produce,
                .invocation_memory_for_requests = invocationMemory,
            } };
        }

        fn invocationMemory(
            _: *anyopaque,
            _: Allocator,
            _: []const db_mod.asset_producer_mod.Request,
        ) !db_mod.inference_work.InvocationMemoryPlan {
            return .{
                .attachment_transport = .borrowed_binary,
                .fixed_bytes = 1,
                .allocator_limit_bytes = 1,
                .max_result_bytes_per_item = 1,
                .max_result_bytes = 1,
            };
        }

        fn produce(ptr: *anyopaque, _: Allocator, _: db_mod.asset_producer_mod.Request) ![]u8 {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            self.calls += 1;
            // Model a private error leaving the inference runtime artifact.
            // The stable ABI must normalize it to the provider-level terminal
            // identity instead of the generic RuntimeBoundaryFailure fallback.
            const status = db_mod.runtime_error_abi.statusFromErrorWithFallback(
                error.PrivateInferenceParserFailure,
                error.InferenceProviderFailure,
            );
            return db_mod.runtime_error_abi.errorFromStatus(status);
        }
    };

    var path_tmp = try db_mod.TestDirectory.init("db");
    defer path_tmp.cleanup();
    const path = path_tmp.path().ptr;
    defer db_mod.cleanupTempDir(path);

    var failing = ForeignFailingProducer{};
    var db = try DB.open(alloc, std.mem.span(path), .{
        .enrichment = .{
            .owner_id = "worker-a",
            .asset_producer = failing.producer(),
            .inline_retry_max_attempts = 1,
            .worker_retry_max_attempts = 3,
            .sync_wait_timeout_ms = 1_000,
        },
    });
    var db_open = true;
    defer if (db_open) db.close();

    try db.addIndex(.{
        .name = "relations_graph",
        .kind = .graph,
        .config_json =
        \\{
        \\  "source":{"artifact":"relations_v1","path":"$.relations[*]","format":"extraction_relation","edge":{}},
        \\  "artifact":{"name":"relations_v1","kind":"asset","source":{"type":"field","value":"body"},"content_type":"application/json","producer_json":{"type":"extractor","config":{"provider":"mock"}}}
        \\}
        ,
    });
    try db.batch(.{
        .writes = &.{.{ .key = "doc:a", .value = "{\"body\":\"Ada mention\"}" }},
        .sync_level = .write,
    });
    const sequence = db.core.nextEnrichmentSequence();

    try std.testing.expectError(
        error.EnrichmentWorkerFailed,
        db.enrichment.runtime.?.waitForApplied(sequence),
    );

    try std.testing.expectError(error.EnrichmentWorkerFailed, db.runUntilIdle());

    const stats = db.enrichment.runtime.?.stats();
    try std.testing.expectEqual(sequence, stats.applied_sequence);
    try std.testing.expectEqual(@as(usize, 1), failing.calls);
    try std.testing.expectEqual(@as(u64, 0), stats.retryable_error_count);
    try std.testing.expect(!stats.retrying);
    try std.testing.expect(!stats.worker_failed);

    const issues = try db.listArtifactRepairIssues(alloc, .asset, "relations_graph", 0);
    defer db_mod.types.freeArtifactRepairIssues(alloc, issues);
    try std.testing.expectEqual(@as(usize, 1), issues.len);
    try std.testing.expectEqualStrings("InferenceProviderFailure", issues[0].generation_error);
    try std.testing.expectEqual(@as(u64, 1), issues[0].generation_attempts);
    try std.testing.expect(try db_mod.enrichmentRequestFailureRangePending(
        db.enrichment.append_context.?,
        sequence - 1,
        sequence,
    ));
    try std.testing.expect(try db_mod.enrichmentRequestFailureRangePending(
        db.enrichment.append_context.?,
        sequence + 10,
        sequence,
    ));
    try std.testing.expect(!try db_mod.enrichmentRequestFailureRangePending(
        db.enrichment.append_context.?,
        sequence,
        sequence + 1,
    ));

    // Model a crash after publishing the durable terminal repair marker but
    // before the projection checkpoint reaches the request. Restart must use
    // the marker as the authoritative outcome and must not call the provider
    // again merely because the checkpoint is stale.
    try db_mod.enrichment_state.saveAppliedSequence(db.core.store, db_mod.enrichment_runtime_mod.scope_name, sequence - 1);
    // Also erase the advisory runtime envelope to model a crash after the
    // atomic repair marker write but before noteTerminalRequestFailure saves
    // runtime status. Runtime creation must reconstruct this gate from the
    // marker index itself.
    var persisted_status = try db_mod.enrichment_state.loadRuntimeStatus(alloc, db.core.store, db_mod.enrichment_runtime_mod.scope_name);
    persisted_status.terminal_failure_min_sequence = 0;
    persisted_status.terminal_failure_max_sequence = 0;
    try db_mod.enrichment_state.saveRuntimeStatus(db.core.store, db_mod.enrichment_runtime_mod.scope_name, persisted_status);
    db.close();
    db_open = false;

    var reopened = try DB.open(alloc, std.mem.span(path), .{
        .enrichment = .{
            .owner_id = "worker-b",
            .asset_producer = failing.producer(),
            .inline_retry_max_attempts = 1,
            .worker_retry_max_attempts = 3,
            .sync_wait_timeout_ms = 1_000,
        },
    });
    defer reopened.close();
    try std.testing.expectError(
        error.EnrichmentWorkerFailed,
        reopened.enrichment.runtime.?.waitForApplied(sequence),
    );
    try std.testing.expectEqual(@as(usize, 1), failing.calls);
    try std.testing.expectEqual(sequence, reopened.enrichment.runtime.?.stats().applied_sequence);
}

test "db terminal enrichment sequence index survives coalescing and retires with repair debt" {
    const alloc = std.testing.allocator;
    var path_tmp = try db_mod.TestDirectory.init("db");
    defer path_tmp.cleanup();
    const path = path_tmp.path().ptr;
    defer db_mod.cleanupTempDir(path);

    var deterministic = db_mod.embedder_mod.DeterministicDenseEmbedder{};
    var db = try DB.open(alloc, std.mem.span(path), .{
        .start_optional_runtime_workers = false,
        .enrichment = .{
            .owner_id = "worker-a",
            .dense_embedder = deterministic.interface(),
        },
    });
    defer db.close();
    const artifact_key = try db_mod.internal_keys.artifactNamedPrefixAlloc(alloc, "doc:a", "asset", "relations_v1");
    defer alloc.free(artifact_key);

    for ([_]u64{ 10, 20 }) |sequence| {
        try db_mod.recordArtifactRepairIssueContextDetailed(
            db.async_context,
            .asset,
            "relations_graph",
            "doc:a",
            "",
            "",
            "relations_v1",
            "",
            "relations_v1",
            artifact_key,
            null,
            sequence,
            .enrichment_failed,
            1,
            "UnexpectedToken",
        );
    }

    const issues = try db.listArtifactRepairIssues(alloc, .asset, "relations_graph", 0);
    defer db_mod.types.freeArtifactRepairIssues(alloc, issues);
    try std.testing.expectEqual(@as(usize, 1), issues.len);
    try std.testing.expectEqual(@as(u64, 20), issues[0].sequence);

    const append_ctx = db.enrichment.append_context.?;
    try std.testing.expect(try db_mod.enrichmentRequestFailureRangePending(append_ctx, 9, 10));
    try std.testing.expect(!try db_mod.enrichmentRequestFailureRangePending(append_ctx, 10, 15));
    try std.testing.expect(try db_mod.enrichmentRequestFailureRangePending(append_ctx, 15, 20));
    // Recovery entered after both failures but is completing the older write.
    try std.testing.expect(try db_mod.enrichmentRequestFailureRangePending(append_ctx, 20, 10));

    try db.clearArtifactRepairIssue(alloc, issues[0]);
    try std.testing.expect(!try db_mod.enrichmentRequestFailureRangePending(append_ctx, 9, 10));
    try std.testing.expect(!try db_mod.enrichmentRequestFailureRangePending(append_ctx, 15, 20));

    // A broad stable-session recovery range may contain many additive markers
    // left by an older binary. Put the authoritative marker beyond one scan
    // page: lookup must bound request memory, clean each stale page, and still
    // reach the live repair debt.
    var stale_sequence: u64 = 21;
    while (stale_sequence < 85) : (stale_sequence += 1) {
        const stale_issue_key = try std.fmt.allocPrint(alloc, "stale-repair-issue-{d}", .{stale_sequence});
        defer alloc.free(stale_issue_key);
        const stale_marker_key = try db_mod.internal_keys.enrichmentTerminalFailureSequenceKeyAlloc(alloc, stale_sequence, stale_issue_key);
        defer alloc.free(stale_marker_key);
        try db.core.store.put(stale_marker_key, stale_issue_key);
    }
    const live_artifact_key = try db_mod.internal_keys.artifactNamedPrefixAlloc(alloc, "doc:b", "asset", "relations_v1");
    defer alloc.free(live_artifact_key);
    try db_mod.recordArtifactRepairIssueContextDetailed(
        db.async_context,
        .asset,
        "relations_graph",
        "doc:b",
        "",
        "",
        "relations_v1",
        "",
        "relations_v1",
        live_artifact_key,
        null,
        85,
        .enrichment_failed,
        1,
        "UnexpectedToken",
    );
    const marker_envelope = try db_mod.enrichmentTerminalFailureSequenceEnvelope(db.core.store);
    try std.testing.expectEqual(@as(u64, 21), marker_envelope.min);
    try std.testing.expectEqual(@as(u64, 85), marker_envelope.max);
    try std.testing.expectEqual(@as(u8, 2), marker_envelope.entries_examined);
    try std.testing.expectError(
        error.EnrichmentRepairLookupBudgetExceeded,
        db_mod.enrichmentRequestFailureRangePending(append_ctx, 20, 85),
    );
    // The bounded probe cleaned one page and failed closed. A retry reaches
    // the live marker without allowing the first request to overrun its UX
    // deadline while processing rollback debris.
    try std.testing.expect(try db_mod.enrichmentRequestFailureRangePending(append_ctx, 20, 85));
    const marker_root = try db_mod.internal_keys.enrichmentTerminalFailureSequenceRootPrefixAlloc(alloc);
    defer alloc.free(marker_root);
    const remaining_markers = try db.core.store.scanPrefix(alloc, marker_root);
    defer db_mod.docstore_mod.DocStore.freeResults(alloc, remaining_markers);
    try std.testing.expectEqual(@as(usize, 1), remaining_markers.len);
}

test "db terminal enrichment marker cleanup isolates corrupt cross issue reverse entries" {
    const alloc = std.testing.allocator;
    var path_tmp = try db_mod.TestDirectory.init("db");
    defer path_tmp.cleanup();
    const path = path_tmp.path().ptr;
    defer db_mod.cleanupTempDir(path);

    var deterministic = db_mod.embedder_mod.DeterministicDenseEmbedder{};
    var db = try DB.open(alloc, std.mem.span(path), .{
        .start_optional_runtime_workers = false,
        .enrichment = .{
            .owner_id = "worker-a",
            .dense_embedder = deterministic.interface(),
        },
    });
    defer db.close();

    var issue_a = db_mod.types.ArtifactRepairIssue{
        .artifact_kind = .asset,
        .index_name = try alloc.dupe(u8, "relations_graph"),
        .doc_key = try alloc.dupe(u8, "doc:a"),
        .artifact_name = try alloc.dupe(u8, "relations_v1"),
        .reason = .enrichment_failed,
        .sequence = 10,
    };
    defer issue_a.deinit(alloc);
    var issue_b = db_mod.types.ArtifactRepairIssue{
        .artifact_kind = .asset,
        .index_name = try alloc.dupe(u8, "relations_graph"),
        .doc_key = try alloc.dupe(u8, "doc:b"),
        .artifact_name = try alloc.dupe(u8, "relations_v1"),
        .reason = .enrichment_failed,
        .sequence = 20,
    };
    defer issue_b.deinit(alloc);
    try db.recordArtifactRepairIssue(alloc, issue_a);
    try db.recordArtifactRepairIssue(alloc, issue_b);

    const issue_a_key = try db_mod.artifactRepairIssueKeyForIssueAlloc(alloc, issue_a);
    defer alloc.free(issue_a_key);
    const issue_b_key = try db_mod.artifactRepairIssueKeyForIssueAlloc(alloc, issue_b);
    defer alloc.free(issue_b_key);
    var marker_a = try db_mod.EnrichmentTerminalFailureMarkerPair.init(alloc, issue_a_key, issue_a.sequence);
    defer marker_a.deinit(alloc);
    var marker_b = try db_mod.EnrichmentTerminalFailureMarkerPair.init(alloc, issue_b_key, issue_b.sequence);
    defer marker_b.deinit(alloc);

    // Model a corrupt reverse record for A that names B's valid primary
    // marker. Retiring A must remove the corrupt reverse entry without
    // deleting B's authoritative visibility debt.
    const corrupt_reverse_key = try db_mod.internal_keys.enrichmentTerminalFailureIssueKeyAlloc(
        alloc,
        issue_a_key,
        issue_b.sequence,
    );
    defer alloc.free(corrupt_reverse_key);
    try db.core.store.put(corrupt_reverse_key, marker_b.sequence_key);
    const malformed_reverse_key = try db_mod.internal_keys.enrichmentTerminalFailureIssueKeyAlloc(
        alloc,
        issue_a_key,
        issue_b.sequence + 1,
    );
    defer alloc.free(malformed_reverse_key);
    try db.core.store.put(malformed_reverse_key, "not-a-sequence-marker");

    try db.clearArtifactRepairIssue(alloc, issue_a);
    try std.testing.expectError(error.NotFound, db.core.store.get(alloc, marker_a.sequence_key));
    try std.testing.expectError(error.NotFound, db.core.store.get(alloc, marker_a.issue_key));
    try std.testing.expectError(error.NotFound, db.core.store.get(alloc, corrupt_reverse_key));
    try std.testing.expectError(error.NotFound, db.core.store.get(alloc, malformed_reverse_key));

    const primary_value = try db.core.store.get(alloc, marker_b.sequence_key);
    defer alloc.free(primary_value);
    const decoded_primary = try db_mod.decodeEnrichmentTerminalFailureMarkerValue(primary_value);
    try std.testing.expectEqualStrings(issue_b_key, decoded_primary.issue_key);
    try std.testing.expectEqual(
        try db_mod.loadEnrichmentTerminalFailureGeneration(alloc, db.core.store, issue_b_key),
        decoded_primary.generation,
    );
    const reverse_value = try db.core.store.get(alloc, marker_b.issue_key);
    defer alloc.free(reverse_value);
    try std.testing.expectEqualStrings(marker_b.sequence_key, reverse_value);
    try std.testing.expect(try db_mod.enrichmentRequestFailureRangePending(
        db.enrichment.append_context.?,
        issue_b.sequence - 1,
        issue_b.sequence,
    ));
}

test "db terminal enrichment marker retirement is bounded and fences stale generations" {
    const alloc = std.testing.allocator;
    var path_tmp = try db_mod.TestDirectory.init("db");
    defer path_tmp.cleanup();
    const path = path_tmp.path().ptr;
    defer db_mod.cleanupTempDir(path);

    var deterministic = db_mod.embedder_mod.DeterministicDenseEmbedder{};
    var db = try DB.open(alloc, std.mem.span(path), .{
        .start_optional_runtime_workers = false,
        .enrichment = .{
            .owner_id = "worker-a",
            .dense_embedder = deterministic.interface(),
        },
    });
    defer db.close();

    var issue = db_mod.types.ArtifactRepairIssue{
        .artifact_kind = .asset,
        .index_name = try alloc.dupe(u8, "relations_graph"),
        .doc_key = try alloc.dupe(u8, "doc:hot"),
        .artifact_name = try alloc.dupe(u8, "relations_v1"),
        .reason = .enrichment_failed,
        .sequence = 1,
    };
    defer issue.deinit(alloc);
    try db.recordArtifactRepairIssue(alloc, issue);

    const issue_key = try db_mod.artifactRepairIssueKeyForIssueAlloc(alloc, issue);
    defer alloc.free(issue_key);
    const old_generation = try db_mod.loadEnrichmentTerminalFailureGeneration(alloc, db.core.store, issue_key);
    try std.testing.expect(old_generation != 0);

    // Build a hot-key backlog without repeatedly rewriting the same repair
    // summary. These are the exact pairs that ordinary coalesced failures add.
    var sequence: u64 = 2;
    while (sequence < 130) : (sequence += 1) {
        var marker = try db_mod.EnrichmentTerminalFailureMarkerPair.init(alloc, issue_key, sequence);
        defer marker.deinit(alloc);
        const marker_value = try db_mod.encodeEnrichmentTerminalFailureMarkerValueAlloc(alloc, issue_key, old_generation);
        defer alloc.free(marker_value);
        try db.core.store.putBatch(&.{
            .{ .key = marker.sequence_key, .value = marker_value },
            .{ .key = marker.issue_key, .value = marker.sequence_key },
        }, &.{});
    }
    issue.sequence = 130;
    try db.recordArtifactRepairIssue(alloc, issue);

    try db.clearArtifactRepairIssue(alloc, issue);
    const marker_root = try db_mod.internal_keys.enrichmentTerminalFailureSequenceRootPrefixAlloc(alloc);
    defer alloc.free(marker_root);
    const after_clear = try db.core.store.scanPrefix(alloc, marker_root);
    defer db_mod.docstore_mod.DocStore.freeResults(alloc, after_clear);
    try std.testing.expectEqual(@as(usize, 66), after_clear.len);
    const generation_key = try db_mod.internal_keys.enrichmentTerminalFailureGenerationKeyAlloc(alloc, issue_key);
    defer alloc.free(generation_key);
    try std.testing.expectError(error.NotFound, db.core.store.get(alloc, generation_key));
    try std.testing.expectEqual(
        old_generation,
        try db_mod.loadEnrichmentTerminalFailureGenerationCounter(alloc, db.core.store),
    );

    // Recreate at the exact original source sequence. Retirement removes only
    // one more page, while the store-wide high-water counter gives the new
    // incarnation a fresh token so the two remaining old markers cannot
    // become authoritative again.
    issue.sequence = 1;
    try db.recordArtifactRepairIssue(alloc, issue);
    const new_generation = try db_mod.loadEnrichmentTerminalFailureGeneration(alloc, db.core.store, issue_key);
    try std.testing.expect(new_generation > old_generation);
    try std.testing.expectEqual(
        new_generation,
        try db_mod.loadEnrichmentTerminalFailureGenerationCounter(alloc, db.core.store),
    );
    const after_recreate = try db.core.store.scanPrefix(alloc, marker_root);
    defer db_mod.docstore_mod.DocStore.freeResults(alloc, after_recreate);
    try std.testing.expectEqual(@as(usize, 3), after_recreate.len);
    try std.testing.expect(!try db_mod.enrichmentRequestFailureRangePending(db.enrichment.append_context.?, 128, 129));
    try std.testing.expect(try db_mod.enrichmentRequestFailureRangePending(db.enrichment.append_context.?, 0, 1));
}

test "db terminal enrichment markers do not resurrect across rollback recreation" {
    const alloc = std.testing.allocator;
    var path_tmp = try db_mod.TestDirectory.init("db");
    defer path_tmp.cleanup();
    const path = path_tmp.path().ptr;
    defer db_mod.cleanupTempDir(path);

    var deterministic = db_mod.embedder_mod.DeterministicDenseEmbedder{};
    var db = try DB.open(alloc, std.mem.span(path), .{
        .start_optional_runtime_workers = false,
        .enrichment = .{
            .owner_id = "worker-a",
            .dense_embedder = deterministic.interface(),
        },
    });
    defer db.close();
    var issue = db_mod.types.ArtifactRepairIssue{
        .artifact_kind = .asset,
        .index_name = try alloc.dupe(u8, "relations_graph"),
        .doc_key = try alloc.dupe(u8, "doc:a"),
        .artifact_name = try alloc.dupe(u8, "relations_v1"),
        .reason = .enrichment_failed,
        .sequence = 7,
    };
    defer issue.deinit(alloc);
    try db.recordArtifactRepairIssue(alloc, issue);

    const issue_key = try db_mod.artifactRepairIssueKeyForIssueAlloc(alloc, issue);
    defer alloc.free(issue_key);
    const kind_key = try db_mod.artifactRepairIssueKindKeyForIssueAlloc(alloc, issue);
    defer alloc.free(kind_key);
    // Model a rollback binary that knows the established repair ledger but not
    // the additive sequence indexes.
    try db.core.store.putBatch(&.{}, &.{ issue_key, kind_key });

    // Recreate the same logical issue before a marker-aware lookup gets a
    // chance to lazily clean sequence 7. The recreated issue deliberately has
    // no exact marker, just as it would when written by the rollback binary.
    issue.sequence = 11;
    try db.saveArtifactRepairIssueWithSummary(alloc, issue_key, issue, true, &.{}, &.{});

    try std.testing.expect(!try db_mod.enrichmentRequestFailureRangePending(db.enrichment.append_context.?, 6, 7));
    const marker_root = try db_mod.internal_keys.enrichmentTerminalFailureSequenceRootPrefixAlloc(alloc);
    defer alloc.free(marker_root);
    const after_lazy_cleanup = try db.core.store.scanPrefix(alloc, marker_root);
    defer db_mod.docstore_mod.DocStore.freeResults(alloc, after_lazy_cleanup);
    try std.testing.expectEqual(@as(usize, 0), after_lazy_cleanup.len);

    // Exercise the atomic creation path too: leave a second stale generation,
    // recreate it through the rollback shape, then let the current writer
    // coalesce a new failure before any range lookup runs.
    issue.sequence = 12;
    try db.recordArtifactRepairIssue(alloc, issue);
    try db.core.store.putBatch(&.{}, &.{ issue_key, kind_key });
    issue.sequence = 13;
    try db.saveArtifactRepairIssueWithSummary(alloc, issue_key, issue, true, &.{}, &.{});
    issue.sequence = 14;
    try db.recordArtifactRepairIssue(alloc, issue);

    try std.testing.expect(!try db_mod.enrichmentRequestFailureRangePending(db.enrichment.append_context.?, 11, 12));
    try std.testing.expect(try db_mod.enrichmentRequestFailureRangePending(db.enrichment.append_context.?, 13, 14));
    const remaining = try db.core.store.scanPrefix(alloc, marker_root);
    defer db_mod.docstore_mod.DocStore.freeResults(alloc, remaining);
    try std.testing.expectEqual(@as(usize, 1), remaining.len);
}

test "db enrichment retry makes monotonic progress across provider batches" {
    const alloc = std.testing.allocator;

    var path_tmp = try db_mod.TestDirectory.init("db");
    defer path_tmp.cleanup();
    const path = path_tmp.path().ptr;
    defer db_mod.cleanupTempDir(path);

    var gated = db_mod.GateDenseEmbedder{};
    gated.allowed_successes.store(0, .release);
    var db = try DB.open(alloc, std.mem.span(path), .{
        .start_optional_runtime_workers = false,
        .enrichment = .{
            .owner_id = "worker-a",
            .dense_embedder = gated.interface(),
            .inline_retry_max_attempts = 1,
            .worker_retry_max_attempts = 2,
        },
    });
    defer db.close();

    try db.addIndex(.{
        .name = "semantic",
        .kind = .dense_vector,
        .config_json = "{\"field\":\"embedding\",\"dims\":3,\"execution\":{\"embedding\":{\"batch_items\":1}},\"generator\":{\"kind\":\"dense_embedding\",\"source_field\":\"body\",\"embedding_name\":\"dense_v1\"}}",
    });
    try db.batch(.{
        .writes = &.{
            .{ .key = "doc:a", .value = "{\"body\":\"alpha\"}" },
            .{ .key = "doc:b", .value = "{\"body\":\"beta\"}" },
        },
        .sync_level = .write,
    });
    const sequence = db.core.nextEnrichmentSequence();

    try std.testing.expectError(
        error.EnrichmentRetryInProgress,
        db.runUntilIdleWithoutWaitingForEnrichmentRetries(),
    );
    db_mod.sleepNs(550 * std.time.ns_per_ms);
    try std.testing.expectError(error.EnrichmentRetryInProgress, db.enrichment.runUntil(sequence));
    db_mod.sleepNs(550 * std.time.ns_per_ms);
    try db.enrichment.runUntil(sequence);

    try std.testing.expectEqual(sequence, db.enrichment.runtime.?.stats().applied_sequence);
    const issues = try db.listArtifactRepairIssues(alloc, .embedding, "semantic", 0);
    defer db_mod.types.freeArtifactRepairIssues(alloc, issues);
    try std.testing.expectEqual(@as(usize, 2), issues.len);
    for (issues) |issue| {
        try std.testing.expectEqual(@as(u64, 2), issue.generation_attempts);
        try std.testing.expectEqualStrings("EmbedRateLimited", issue.generation_error);
    }
}

test "db shared enrichment repair regenerates one physical artifact once" {
    const alloc = std.testing.allocator;

    var path_tmp = try db_mod.TestDirectory.init("db");
    defer path_tmp.cleanup();
    const path = path_tmp.path().ptr;
    defer db_mod.cleanupTempDir(path);

    var gated = db_mod.GateDenseEmbedder{};
    gated.allowed_successes.store(0, .release);
    var db = try DB.open(alloc, std.mem.span(path), .{
        .start_optional_runtime_workers = false,
        .enrichment = .{
            .owner_id = "worker-a",
            .dense_embedder = gated.interface(),
            .inline_retry_max_attempts = 1,
            .worker_retry_max_attempts = 2,
        },
    });
    defer db.close();

    const shared_cfg = "{\"field\":\"embedding\",\"dims\":3,\"generator\":{\"kind\":\"dense_embedding\",\"source_field\":\"body\",\"embedding_name\":\"shared_dense_v1\"}}";
    try db.addIndex(.{ .name = "dv_a", .kind = .dense_vector, .config_json = shared_cfg });
    try db.addIndex(.{ .name = "dv_b", .kind = .dense_vector, .config_json = shared_cfg });
    try db.batch(.{
        .writes = &.{.{ .key = "doc:a", .value = "{\"body\":\"shared repair\"}" }},
        .sync_level = .write,
    });
    const sequence = db.core.nextEnrichmentSequence();
    try std.testing.expectError(error.EnrichmentRetryInProgress, db.enrichment.runUntil(sequence));
    db_mod.sleepNs(550 * std.time.ns_per_ms);
    try db.enrichment.runUntil(sequence);

    const before = gated.snapshot();
    gated.allowAll();
    var first = try db.repairArtifactIssuesWithRequest(alloc, .{
        .target = .artifact,
        .artifact_kind = .embedding,
        .limit = 1,
    });
    defer first.deinit(alloc);
    try std.testing.expect(first.has_more);
    try std.testing.expect(first.next_cursor != null);
    var second = try db.repairArtifactIssuesWithRequest(alloc, .{
        .target = .artifact,
        .artifact_kind = .embedding,
        .limit = 1,
        .cursor = first.next_cursor,
    });
    defer second.deinit(alloc);
    const after = gated.snapshot();

    try std.testing.expectEqual(@as(u64, 1), first.scanned);
    try std.testing.expectEqual(@as(u64, 1), first.reprocessed);
    try std.testing.expectEqual(@as(u64, 1), first.repaired);
    try std.testing.expect(first.debt_remaining);
    // The first replay publishes the shared artifact to every consumer and
    // atomically retires their debt. The cursor came from the pre-repair page,
    // so following it is valid but must not rediscover already-retired work.
    try std.testing.expectEqual(@as(u64, 0), second.scanned);
    try std.testing.expectEqual(@as(u64, 0), second.reprocessed);
    try std.testing.expectEqual(@as(u64, 0), second.repaired);
    try std.testing.expect(!second.debt_remaining);
    try std.testing.expectEqual(@as(usize, 1), after.total_requests - before.total_requests);
    try std.testing.expectEqual(@as(usize, 1), after.successful_requests - before.successful_requests);

    const remaining = try db.listArtifactRepairIssues(alloc, .embedding, null, 0);
    defer db_mod.types.freeArtifactRepairIssues(alloc, remaining);
    try std.testing.expectEqual(@as(usize, 0), remaining.len);

    const artifact_key = try db_mod.internal_keys.embeddingArtifactKeyForDocumentAlloc(alloc, "doc:a", "shared_dense_v1");
    defer alloc.free(artifact_key);
    const artifact_key_hex = try db_mod.bytesToHexAlloc(alloc, artifact_key);
    defer alloc.free(artifact_key_hex);
    const completion_key = try db_mod.internal_keys.artifactRepairCompletionKeyAlloc(alloc, "embedding", artifact_key_hex);
    defer alloc.free(completion_key);
    try std.testing.expectError(error.NotFound, db.core.store.get(alloc, completion_key));
}

test "db chunked enrichment failure repair completes from request coverage" {
    const alloc = std.testing.allocator;

    var path_tmp = try db_mod.TestDirectory.init("db");
    defer path_tmp.cleanup();
    const path = path_tmp.path().ptr;
    defer db_mod.cleanupTempDir(path);

    var gated = db_mod.GateDenseEmbedder{};
    gated.allowed_successes.store(0, .release);
    var db = try DB.open(alloc, std.mem.span(path), .{
        .start_optional_runtime_workers = false,
        .enrichment = .{
            .owner_id = "worker-a",
            .dense_embedder = gated.interface(),
            .inline_retry_max_attempts = 1,
            .worker_retry_max_attempts = 2,
        },
    });
    defer db.close();

    try db.addIndex(.{
        .name = "chunked_dense",
        .kind = .dense_vector,
        .config_json = "{\"field\":\"embedding\",\"dims\":3,\"generator\":{\"kind\":\"dense_embedding\",\"source_field\":\"body\",\"artifact_name\":\"body_chunks_v1\",\"chunk_size\":8,\"chunk_overlap\":2,\"embedding_name\":\"chunk_dense_v1\"}}",
    });
    try db.batch(.{
        .writes = &.{.{ .key = "doc:a", .value = "{\"body\":\"abcdefghijklmno\"}" }},
        .sync_level = .write,
    });
    const sequence = db.core.nextEnrichmentSequence();
    try std.testing.expectError(error.EnrichmentRetryInProgress, db.enrichment.runUntil(sequence));
    db_mod.sleepNs(550 * std.time.ns_per_ms);
    try db.enrichment.runUntil(sequence);

    const parked = try db.listArtifactRepairIssues(alloc, .embedding, "chunked_dense", 1);
    defer db_mod.types.freeArtifactRepairIssues(alloc, parked);
    try std.testing.expectEqual(@as(usize, 1), parked.len);
    try std.testing.expectEqualStrings("body_chunks_v1", parked[0].source_artifact_name);
    try std.testing.expectEqualStrings("chunk_dense_v1", parked[0].artifact_name);
    try std.testing.expectEqual(@as(usize, 0), parked[0].artifact_key.len);

    gated.allowAll();
    var repair = try db.repairArtifactIssuesWithRequest(alloc, .{
        .target = .artifact,
        .artifact_kind = .embedding,
        .index_name = "chunked_dense",
        .limit = 1,
    });
    defer repair.deinit(alloc);
    try std.testing.expectEqual(@as(u64, 1), repair.scanned);
    try std.testing.expectEqual(@as(u64, 1), repair.reprocessed);
    try std.testing.expectEqual(@as(u64, 1), repair.repaired);
    try std.testing.expectEqual(@as(u64, 0), repair.failed);
    try std.testing.expect(!repair.debt_remaining);

    const remaining = try db.listArtifactRepairIssues(alloc, .embedding, "chunked_dense", 1);
    defer db_mod.types.freeArtifactRepairIssues(alloc, remaining);
    try std.testing.expectEqual(@as(usize, 0), remaining.len);
}

test "db asset repair reports a missing source document precisely" {
    const alloc = std.testing.allocator;
    var path_tmp = try db_mod.TestDirectory.init("db");
    defer path_tmp.cleanup();
    const path = path_tmp.path().ptr;
    defer db_mod.cleanupTempDir(path);

    var db = try DB.open(alloc, std.mem.span(path), .{ .start_optional_runtime_workers = false });
    defer db.close();
    try db.enrichment.addEnrichment(.{
        .name = "body_copy_v1",
        .kind = .asset,
        .field = "body",
        .content_type = "text/plain",
        .producer_json = "{\"type\":\"copy\"}",
    });

    const artifact_key = try db_mod.internal_keys.artifactNamedPrefixAlloc(alloc, "doc:missing", "asset", "body_copy_v1");
    defer alloc.free(artifact_key);
    try db_mod.recordArtifactRepairIssueContextDetailed(
        db.async_context,
        .asset,
        "",
        "doc:missing",
        "",
        "",
        "body_copy_v1",
        "",
        "body_copy_v1",
        artifact_key,
        null,
        1,
        .enrichment_failed,
        1,
        "PermanentPromptFailure",
    );

    var repair = try db.repairArtifactIssuesWithRequest(alloc, .{
        .target = .artifact,
        .artifact_kind = .asset,
        .limit = 1,
    });
    defer repair.deinit(alloc);
    try std.testing.expectEqual(@as(u64, 1), repair.scanned);
    try std.testing.expectEqual(@as(u64, 1), repair.missing_source_docs);
    try std.testing.expectEqual(@as(u64, 0), repair.failed);
    try std.testing.expectEqual(@as(u64, 1), repair.unresolved);
    try std.testing.expect(repair.debt_remaining);

    const issues = try db.listArtifactRepairIssues(alloc, .asset, null, 1);
    defer db_mod.types.freeArtifactRepairIssues(alloc, issues);
    try std.testing.expectEqual(@as(usize, 1), issues.len);
    try std.testing.expectEqualStrings("source_document_missing", issues[0].last_error);
}

test "db shared embedding enrichment feeds multiple dense indexes with durable lsm primary backend" {
    const alloc = std.testing.allocator;

    var path_tmp = try db_mod.TestDirectory.init("db");
    defer path_tmp.cleanup();
    const path = path_tmp.path().ptr;
    defer db_mod.cleanupTempDir(path);

    var deterministic = db_mod.embedder_mod.DeterministicDenseEmbedder{};
    var db = try DB.open(alloc, std.mem.span(path), .{
        .primary_backend = .{ .lsm = .{ .flush_threshold = 1 } },
        .enrichment = .{
            .owner_id = "worker-a",
            .dense_embedder = deterministic.interface(),
        },
    });
    defer db.close();

    const shared_cfg = "{\"field\":\"embedding\",\"dims\":3,\"generator\":{\"kind\":\"dense_embedding\",\"source_field\":\"body\",\"embedding_name\":\"shared_dense_v1\"}}";
    try db.addIndex(.{
        .name = "dv_a",
        .kind = .dense_vector,
        .config_json = shared_cfg,
    });
    try db.addIndex(.{
        .name = "dv_b",
        .kind = .dense_vector,
        .config_json = shared_cfg,
    });

    try db.batch(.{
        .writes = &.{
            .{ .key = "doc:a", .value = "{\"body\":\"shared embedding text\"}" },
        },
        .sync_level = .write,
    });

    try db.enrichment.runtime.?.waitForApplied(1);
    try db_mod.waitForDerivedReplayTarget(&db);

    const shared_artifact_key = try db_mod.expectedDocumentEmbeddingArtifactKeyAlloc(alloc, "doc:a", "shared_dense_v1");
    defer alloc.free(shared_artifact_key);
    const artifacts = try db.core.store.scanPrefix(alloc, shared_artifact_key);
    defer db_mod.docstore_mod.DocStore.freeResults(alloc, artifacts);
    try std.testing.expectEqual(@as(usize, 1), artifacts.len);

    const query_vec = try deterministic.interface().embedDense(alloc, "", "shared embedding text", 3);
    defer alloc.free(query_vec);

    var result_a = try db.search(alloc, .{
        .index_name = "dv_a",
        .dense = .{ .vector = query_vec, .k = 1 },
    });
    defer result_a.deinit();
    try std.testing.expectEqual(@as(u32, 1), result_a.total_hits);
    try std.testing.expectEqualStrings("doc:a", result_a.hits[0].id);

    var result_b = try db.search(alloc, .{
        .index_name = "dv_b",
        .dense = .{ .vector = query_vec, .k = 1 },
    });
    defer result_b.deinit();
    try std.testing.expectEqual(@as(u32, 1), result_b.total_hits);
    try std.testing.expectEqualStrings("doc:a", result_b.hits[0].id);
}

test "db addEnrichment supports explicit shared definitions" {
    const alloc = std.testing.allocator;

    var path_tmp = try db_mod.TestDirectory.init("db");
    defer path_tmp.cleanup();
    const path = path_tmp.path().ptr;
    defer db_mod.cleanupTempDir(path);

    var validating = db_mod.ValidatingChunkDenseEmbedder{};
    var db = try DB.open(alloc, std.mem.span(path), .{
        .enrichment = .{
            .owner_id = "worker-a",
            .dense_embedder = validating.interface(),
        },
    });
    defer db.close();

    try db.enrichment.addEnrichment(.{
        .name = "body_chunks_v1",
        .kind = .chunk,
        .field = "body",
        .chunk_size = 8,
        .chunk_overlap = 2,
    });
    try db.enrichment.addEnrichment(.{
        .name = "chunk_dense_v1",
        .kind = .embedding,
        .field = "body",
        .source_artifact_name = "body_chunks_v1",
        .expected_dims = 3,
    });

    try db.addIndex(.{
        .name = "dv_ref",
        .kind = .dense_vector,
        .config_json = "{\"field\":\"embedding\",\"dims\":3,\"embedding_name\":\"chunk_dense_v1\"}",
    });

    try db.batch(.{
        .writes = &.{
            .{ .key = "doc:a", .value = "{\"body\":\"abcdefghijklmno\"}" },
        },
        .sync_level = .write,
    });

    try db.enrichment.runtime.?.waitForApplied(1);
    try db_mod.waitForDerivedReplayTarget(&db);

    const query_vec = try validating.deterministic.interface().embedDense(alloc, "chunk_dense_v1", "abcdefgh", 3);
    defer alloc.free(query_vec);

    const req: db_mod.types.SearchRequest = .{
        .index_name = "dv_ref",
        .dense = .{ .vector = query_vec, .k = 3 },
        .return_mode = .chunk,
    };
    var dense_result = try db_mod.waitForDenseSearchResult(alloc, &db, req, 1);
    dense_result.deinit();

    var result = try db_mod.waitForSearchResult(alloc, &db, req, 1);
    defer result.deinit();
    var chunk_ref = (try db_mod.artifact_ids.decodeArtifactPublicIdAlloc(alloc, result.hits[0].id)) orelse return error.TestUnexpectedResult;
    defer chunk_ref.deinit(alloc);
    try std.testing.expectEqual(db_mod.types.ArtifactKind.chunk, chunk_ref.kind);
    try std.testing.expectEqualStrings("doc:a", chunk_ref.document_id);
    try std.testing.expectEqualStrings("body_chunks_v1", chunk_ref.name);
    try std.testing.expect(chunk_ref.chunk_id != null);

    const chunk = (try db.enrichment.getEnrichment(alloc, .chunk, "body_chunks_v1")) orelse return error.TestUnexpectedResult;
    defer {
        var tmp = chunk;
        tmp.deinit(alloc);
    }
    try std.testing.expectEqualStrings("body", chunk.field);

    const embedding = (try db.enrichment.getEnrichment(alloc, .embedding, "chunk_dense_v1")) orelse return error.TestUnexpectedResult;
    defer {
        var tmp = embedding;
        tmp.deinit(alloc);
    }
    try std.testing.expectEqualStrings("body_chunks_v1", embedding.source_artifact_name);

    const enrichments = try db.enrichment.listEnrichments(alloc);
    defer db_mod.types.freeEnrichmentConfigs(alloc, enrichments);
    try std.testing.expectEqual(@as(usize, 2), enrichments.len);
}

test "db preflight classifies canonical artifact full-text sources consistently" {
    const alloc = std.testing.allocator;

    var path_tmp = try db_mod.TestDirectory.init("db");
    defer path_tmp.cleanup();
    const path = path_tmp.path().ptr;
    defer db_mod.cleanupTempDir(path);

    var db = try DB.open(alloc, std.mem.span(path), .{});
    defer db.close();

    try db.enrichment.addEnrichment(.{
        .name = "body_chunks_v1",
        .kind = .chunk,
        .field = "body",
        .chunk_size = 64,
    });
    try db.addIndex(.{
        .name = "artifact_text",
        .kind = .full_text,
        .config_json = "{\"sources\":[{\"artifact\":\"body_chunks_v1\"}]}",
    });

    var summary = try db.preflightSearchRequest(alloc, .{
        .index_name = "artifact_text",
        .full_text = .{ .match_all = {} },
    }, 0);
    defer summary.deinit(alloc);
    try std.testing.expectEqual(@as(usize, 1), summary.text_indexes.len);
    try std.testing.expect(summary.text_indexes[0].chunk_backed);
    try std.testing.expect(summary.text_indexes[0].group_chunk_parents);

    var publication = try db.core.index_manager.acquireTextPublicationContext(alloc, "artifact_text");
    defer publication.deinit();
    try std.testing.expect(publication.chunk_backed);
}

test "artifact text replay deletes only keys owned by its source projection" {
    const alloc = std.testing.allocator;

    var path_tmp = try db_mod.TestDirectory.init("db");
    defer path_tmp.cleanup();
    const path = path_tmp.path().ptr;
    defer db_mod.cleanupTempDir(path);

    var db = try DB.open(alloc, std.mem.span(path), .{});
    defer db.close();

    try db.enrichment.addEnrichment(.{
        .name = "body_chunks_v1",
        .kind = .chunk,
        .field = "body",
        .chunk_size = 64,
    });
    try db.enrichment.addEnrichment(.{
        .name = "notes_chunks_v1",
        .kind = .chunk,
        .field = "notes",
        .chunk_size = 64,
    });
    try db.addIndex(.{
        .name = "body_text",
        .kind = .full_text,
        .config_json = "{\"sources\":[{\"artifact\":\"body_chunks_v1\"}]}",
    });

    const body_key = try db_mod.internal_keys.chunkArtifactKeyAlloc(alloc, "doc-a", "body_chunks_v1", 0);
    defer alloc.free(body_key);
    const notes_key = try db_mod.internal_keys.chunkArtifactKeyAlloc(alloc, "doc-a", "notes_chunks_v1", 0);
    defer alloc.free(notes_key);

    const body_target = [_]db_mod.derived_types.DerivedTargetRef{.{
        .kind = .full_text,
        .index_name = "body_text",
    }};
    const other_target = [_]db_mod.derived_types.DerivedTargetRef{.{
        .kind = .full_text,
        .index_name = "notes_text",
    }};
    const replay_deletes = [_][]const u8{ body_key, notes_key, "doc-a" };
    const replay_overwrites = [_][]const u8{ notes_key, "doc-b" };
    const replay_documents = [_]db_mod.derived_types.DerivedDocument{
        .{ .key = body_key, .action = .upsert, .targets = &body_target },
        .{ .key = notes_key, .action = .upsert, .targets = &other_target },
    };

    var publication = try db.core.index_manager.acquireTextPublicationContext(alloc, "body_text");
    defer publication.deinit();
    var apply_guard = try db.core.index_manager.lockManagedIndexApply(.{
        .name = "body_text",
        .kind = .full_text,
    });
    defer apply_guard.unlock();
    const keys = try db_mod.collectTextReplayDeleteKeys(
        alloc,
        db.core.index_manager,
        .{
            .deleted_keys = &replay_deletes,
            .overwritten_doc_keys = &replay_overwrites,
            .documents = &replay_documents,
        },
        "body_text",
        publication,
    );
    defer alloc.free(keys);

    try std.testing.expectEqual(@as(usize, 1), keys.len);
    try std.testing.expectEqualStrings(body_key, keys[0]);
}

test "db chunk deletion replay targets only matching vector sources" {
    const alloc = std.testing.allocator;
    var path_tmp = try db_mod.TestDirectory.init("db");
    defer path_tmp.cleanup();
    const path = path_tmp.path().ptr;
    defer db_mod.cleanupTempDir(path);
    var db = try DB.open(alloc, std.mem.span(path), .{});
    defer db.close();

    for ([_][]const u8{ "body", "notes" }) |source| {
        const name = try std.fmt.allocPrint(alloc, "{s}_chunks", .{source});
        defer alloc.free(name);
        try db.enrichment.addEnrichment(.{ .name = name, .kind = .chunk, .field = source, .chunk_size = 64 });
    }
    for ([_]db_mod.types.IndexKind{ .dense_vector, .sparse_vector }) |kind| {
        for ([_][]const u8{ "body", "notes" }) |source| {
            const name = try std.fmt.allocPrint(alloc, "{s}_{s}", .{ @tagName(kind), source });
            defer alloc.free(name);
            const chunk_name = try std.fmt.allocPrint(alloc, "{s}_chunks", .{source});
            defer alloc.free(chunk_name);
            try db.enrichment.addEnrichment(.{
                .name = name,
                .kind = .embedding,
                .field = source,
                .source_artifact_name = chunk_name,
                .expected_dims = if (kind == .dense_vector) 3 else 0,
                .producer_json = if (kind == .dense_vector)
                    "{\"version\":1,\"provider\":\"antfly\",\"model\":\"dense-test\",\"dimensions\":3}"
                else
                    "{\"version\":1,\"provider\":\"antfly\",\"model\":\"sparse-test\"}",
            });
            const config = try std.fmt.allocPrint(
                alloc,
                "{{\"field\":\"embedding\",\"dims\":3,\"embedding_name\":\"{s}\"}}",
                .{name},
            );
            defer alloc.free(config);
            try db.addIndex(.{ .name = name, .kind = kind, .config_json = config });
        }
    }
    const body = try db_mod.internal_keys.chunkArtifactKeyAlloc(alloc, "doc-a", "body_chunks", 1);
    defer alloc.free(body);
    const notes = try db_mod.internal_keys.documentUnitChunkArtifactKeyAlloc(alloc, "doc-a", "notes_chunks", "page:1", 1);
    defer alloc.free(notes);
    const unrelated = try db_mod.internal_keys.chunkArtifactKeyAlloc(alloc, "doc-a", "unrelated", 1);
    defer alloc.free(unrelated);

    for ([_]db_mod.types.IndexKind{ .dense_vector, .sparse_vector }) |kind| {
        for ([_][]const u8{ "body", "notes" }, [_][]const u8{ body, notes }) |source, owned_key| {
            const name = try std.fmt.allocPrint(alloc, "{s}_{s}", .{ @tagName(kind), source });
            defer alloc.free(name);
            const ref = db_mod.index_manager_mod.ManagedIndexRef{ .name = name, .kind = kind };
            const sibling = if (std.mem.eql(u8, source, "body")) notes else body;
            for ([_][]const u8{ sibling, unrelated }) |foreign| {
                try std.testing.expectEqual(db_mod.ManagedIndexBatchApplicability.irrelevant, db_mod.managedIndexBatchApplicability(db.core.index_manager, .{ .deleted_keys = &.{foreign} }, ref));
                try std.testing.expectEqual(db_mod.ManagedIndexBatchApplicability.irrelevant, db_mod.managedIndexRecordApplicability(db.core.index_manager, .{ .sequence = 1, .deleted_doc_keys = &.{foreign} }, ref));
            }
            try std.testing.expectEqual(db_mod.ManagedIndexBatchApplicability.relevant, db_mod.managedIndexBatchApplicability(db.core.index_manager, .{ .deleted_keys = &.{owned_key} }, ref));
            try std.testing.expectEqual(db_mod.ManagedIndexBatchApplicability.relevant, db_mod.managedIndexRecordApplicability(db.core.index_manager, .{ .sequence = 1, .deleted_doc_keys = &.{owned_key} }, ref));
            const keys = try db_mod.collectVectorReplayDeleteKeys(alloc, db.core.index_manager, ref, &.{ sibling, owned_key, unrelated, owned_key });
            defer db_mod.freeOwnedKeySlice(alloc, keys);
            try std.testing.expectEqual(@as(usize, 1), keys.len);
            try std.testing.expectEqualStrings(owned_key, keys[0]);
        }
        const name = try std.fmt.allocPrint(alloc, "{s}_multi", .{@tagName(kind)});
        defer alloc.free(name);
        const config = try std.fmt.allocPrint(
            alloc,
            "{{\"field\":\"embedding\",\"dims\":3,\"sources\":[{{\"artifact\":\"{s}_body\"}},{{\"artifact\":\"{s}_notes\"}}]}}",
            .{ @tagName(kind), @tagName(kind) },
        );
        defer alloc.free(config);
        try db.addIndex(.{ .name = name, .kind = kind, .config_json = config });
        const ref = db_mod.index_manager_mod.ManagedIndexRef{ .name = name, .kind = kind };
        try std.testing.expect(db_mod.vectorIndexConsumesChunkKey(db.core.index_manager, ref, body));
        try std.testing.expect(db_mod.vectorIndexConsumesChunkKey(db.core.index_manager, ref, notes));
        try std.testing.expect(!db_mod.vectorIndexConsumesChunkKey(db.core.index_manager, ref, unrelated));
        const body_name = try std.fmt.allocPrint(alloc, "{s}_body", .{@tagName(kind)});
        defer alloc.free(body_name);
        const notes_name = try std.fmt.allocPrint(alloc, "{s}_notes", .{@tagName(kind)});
        defer alloc.free(notes_name);
        const body_member = try db_mod.internal_keys.derivedEmbeddingArtifactKeyAlloc(alloc, body, body_name);
        defer alloc.free(body_member);
        const notes_member = try db_mod.internal_keys.derivedEmbeddingArtifactKeyAlloc(alloc, notes, notes_name);
        defer alloc.free(notes_member);
        const keys = try db_mod.collectVectorReplayDeleteKeys(alloc, db.core.index_manager, ref, &.{ body, notes, unrelated, body_member, notes_member });
        defer db_mod.freeOwnedKeySlice(alloc, keys);
        try std.testing.expectEqual(@as(usize, 2), keys.len);
        try std.testing.expectEqualStrings(body_member, keys[0]);
        try std.testing.expectEqualStrings(notes_member, keys[1]);
    }
}

test "db deleteEnrichment rejects referenced definitions" {
    const alloc = std.testing.allocator;

    var path_tmp = try db_mod.TestDirectory.init("db");
    defer path_tmp.cleanup();
    const path = path_tmp.path().ptr;
    defer db_mod.cleanupTempDir(path);

    var db = try DB.open(alloc, std.mem.span(path), .{});
    defer db.close();

    try db.enrichment.addEnrichment(.{
        .name = "body_chunks_v1",
        .kind = .chunk,
        .field = "body",
        .chunk_size = 8,
        .chunk_overlap = 2,
    });
    try db.addIndex(.{
        .name = "ft_chunks",
        .kind = .full_text,
        .config_json = "{\"chunk_name\":\"body_chunks_v1\"}",
    });

    try std.testing.expectError(error.EnrichmentInUse, db.enrichment.deleteEnrichment(.chunk, "body_chunks_v1"));
}

test "db deleteEnrichment rejects asset referenced by chunk enrichment" {
    const alloc = std.testing.allocator;

    var path_tmp = try db_mod.TestDirectory.init("db");
    defer path_tmp.cleanup();
    const path = path_tmp.path().ptr;
    defer db_mod.cleanupTempDir(path);

    var db = try DB.open(alloc, std.mem.span(path), .{});
    defer db.close();

    try db.enrichment.addEnrichment(.{
        .name = "document_units_v1",
        .kind = .asset,
        .field = "url",
        .content_type = "application/json",
    });
    try db.enrichment.addEnrichment(.{
        .name = "document_chunks_v1",
        .kind = .chunk,
        .source_artifact_name = "document_units_v1",
        .field = "text",
        .chunk_size = 512,
    });

    try std.testing.expectError(error.EnrichmentInUse, db.enrichment.deleteEnrichment(.asset, "document_units_v1"));
}

test "db upsertEnrichment rejects replacing referenced asset with chunk" {
    const alloc = std.testing.allocator;

    var path_tmp = try db_mod.TestDirectory.init("db");
    defer path_tmp.cleanup();
    const path = path_tmp.path().ptr;
    defer db_mod.cleanupTempDir(path);

    var db = try DB.open(alloc, std.mem.span(path), .{});
    defer db.close();

    try db.enrichment.addEnrichment(.{
        .name = "document_units_v1",
        .kind = .asset,
        .field = "url",
        .content_type = "application/json",
    });
    try db.enrichment.addEnrichment(.{
        .name = "document_chunks_v1",
        .kind = .chunk,
        .source_artifact_name = "document_units_v1",
        .field = "text",
        .chunk_size = 512,
    });

    try std.testing.expectError(error.InvalidEnrichmentConfig, db.enrichment.upsertEnrichment(.{
        .name = "document_units_v1",
        .kind = .chunk,
        .source_artifact_name = "document_units_v1",
        .field = "text",
        .chunk_size = 512,
    }));

    var asset = (try db.enrichment.getEnrichment(alloc, .asset, "document_units_v1")).?;
    defer asset.deinit(alloc);
    try std.testing.expectEqual(db_mod.types.EnrichmentKind.asset, asset.kind);
}

test "db upsertEnrichment rejects replacing referenced chunk with asset" {
    const alloc = std.testing.allocator;

    var path_tmp = try db_mod.TestDirectory.init("db");
    defer path_tmp.cleanup();
    const path = path_tmp.path().ptr;
    defer db_mod.cleanupTempDir(path);

    var db = try DB.open(alloc, std.mem.span(path), .{});
    defer db.close();

    try db.enrichment.addEnrichment(.{
        .name = "document_chunks_v1",
        .kind = .chunk,
        .field = "body",
        .chunk_size = 512,
    });
    try db.enrichment.addEnrichment(.{
        .name = "document_dense_v1",
        .kind = .embedding,
        .source_artifact_name = "document_chunks_v1",
        .field = "text",
        .expected_dims = 3,
    });

    try std.testing.expectError(error.InvalidEnrichmentConfig, db.enrichment.upsertEnrichment(.{
        .name = "document_chunks_v1",
        .kind = .asset,
        .field = "url",
        .content_type = "application/json",
    }));

    var chunk = (try db.enrichment.getEnrichment(alloc, .chunk, "document_chunks_v1")).?;
    defer chunk.deinit(alloc);
    try std.testing.expectEqual(db_mod.types.EnrichmentKind.chunk, chunk.kind);
}

test "db upsertEnrichment rejects replacing indexed chunk with asset" {
    const alloc = std.testing.allocator;

    var path_tmp = try db_mod.TestDirectory.init("db");
    defer path_tmp.cleanup();
    const path = path_tmp.path().ptr;
    defer db_mod.cleanupTempDir(path);

    var db = try DB.open(alloc, std.mem.span(path), .{});
    defer db.close();

    try db.enrichment.addEnrichment(.{
        .name = "body_chunks_v1",
        .kind = .chunk,
        .field = "body",
        .chunk_size = 512,
    });
    try db.addIndex(.{
        .name = "body_text",
        .kind = .full_text,
        .config_json = "{\"chunk_name\":\"body_chunks_v1\"}",
    });

    try std.testing.expectError(error.InvalidEnrichmentConfig, db.enrichment.upsertEnrichment(.{
        .name = "body_chunks_v1",
        .kind = .asset,
        .field = "url",
        .content_type = "application/json",
    }));

    var chunk = (try db.enrichment.getEnrichment(alloc, .chunk, "body_chunks_v1")).?;
    defer chunk.deinit(alloc);
    try std.testing.expectEqual(db_mod.types.EnrichmentKind.chunk, chunk.kind);
}

test "db addEnrichment rejects duplicate names across enrichment kinds" {
    const alloc = std.testing.allocator;

    var path_tmp = try db_mod.TestDirectory.init("db");
    defer path_tmp.cleanup();
    const path = path_tmp.path().ptr;
    defer db_mod.cleanupTempDir(path);

    var db = try DB.open(alloc, std.mem.span(path), .{});
    defer db.close();

    try db.enrichment.addEnrichment(.{
        .name = "document_artifact_v1",
        .kind = .asset,
        .field = "url",
        .content_type = "application/json",
    });
    try std.testing.expectError(error.EnrichmentAlreadyExists, db.enrichment.addEnrichment(.{
        .name = "document_artifact_v1",
        .kind = .chunk,
        .field = "body",
        .chunk_size = 512,
    }));
}

test "db addEnrichment allows unrelated definitions after field sparse index" {
    const alloc = std.testing.allocator;

    var path_tmp = try db_mod.TestDirectory.init("db");
    defer path_tmp.cleanup();
    const path = path_tmp.path().ptr;
    defer db_mod.cleanupTempDir(path);

    var db = try DB.open(alloc, std.mem.span(path), .{});
    defer db.close();

    try db.addIndex(.{
        .name = "sp_v1",
        .kind = .sparse_vector,
        .config_json = "{\"field\":\"sparse\"}",
    });
    try db.enrichment.addEnrichment(.{
        .name = "document_units_v1",
        .kind = .asset,
        .field = "url",
        .content_type = "application/json",
    });
}

test "db enrichment artifact consumer resolution includes graph and default full text indexes" {
    const alloc = std.testing.allocator;

    var path_tmp = try db_mod.TestDirectory.init("db");
    defer path_tmp.cleanup();
    const path = path_tmp.path().ptr;
    defer db_mod.cleanupTempDir(path);

    var db = try DB.open(alloc, std.mem.span(path), .{});
    defer db.close();

    try db.addIndex(.{
        .name = "full_text_index_v0",
        .kind = .full_text,
        .config_json = "{}",
    });
    try db.enrichment.addEnrichment(.{
        .name = "relations_v1",
        .kind = .asset,
        .field = "body",
        .content_type = "application/json",
        .full_text_index = true,
    });
    try db.addIndex(.{
        .name = "knowledge_graph",
        .kind = .graph,
        .config_json = "{\"source\":{\"artifact\":\"relations_v1\",\"format\":\"extraction_relation\"}}",
    });

    const consumers = try db.core.index_manager.indexesDependingOnArtifact(alloc, "relations_v1");
    defer {
        for (consumers) |index_name| alloc.free(index_name);
        alloc.free(consumers);
    }
    try std.testing.expectEqual(@as(usize, 2), consumers.len);
    var found_text = false;
    var found_graph = false;
    for (consumers) |index_name| {
        found_text = found_text or std.mem.eql(u8, index_name, "full_text_index_v0");
        found_graph = found_graph or std.mem.eql(u8, index_name, "knowledge_graph");
    }
    try std.testing.expect(found_text and found_graph);
}

test "db asset enrichment full_text_index feeds default full text index after full_index sync" {
    const alloc = std.testing.allocator;

    var path_tmp = try db_mod.TestDirectory.init("db");
    defer path_tmp.cleanup();
    const path = path_tmp.path().ptr;
    defer db_mod.cleanupTempDir(path);

    var db = try DB.open(alloc, std.mem.span(path), .{});
    defer db.close();

    try db.addIndex(.{
        .name = "full_text_index_v0",
        .kind = .full_text,
        .config_json = "{}",
    });
    try db.enrichment.addEnrichment(.{
        .name = "image_caption_v1",
        .kind = .asset,
        .field = "caption_json",
        .content_type = "application/json",
        .full_text_index = true,
    });

    try db.batch(.{
        .writes = &.{
            .{
                .key = "image:1",
                .value = "{\"title\":\"row text\",\"caption_json\":{\"caption\":\"crimson sunset harbor\"}}",
            },
        },
        .sync_level = .full_index,
    });

    const asset_key = try db_mod.internal_keys.artifactNamedPrefixAlloc(alloc, "image:1", "asset", "image_caption_v1");
    defer alloc.free(asset_key);
    const asset_value = try db.core.store.get(alloc, asset_key);
    defer alloc.free(asset_value);
    try std.testing.expect(std.mem.indexOf(u8, asset_value, "crimson sunset harbor") != null);
    try std.testing.expect(db.core.index_manager.textIndex("full_text_index_v0").?.snapshot().liveDocCount() > 0);

    var results = try db.search(alloc, .{
        .index_name = "full_text_index_v0",
        .full_text = .{ .match = .{ .field = "_all", .text = "harbor" } },
        .limit = 10,
    });
    defer results.deinit();

    try std.testing.expectEqual(@as(u32, 1), results.total_hits);
    try std.testing.expectEqualStrings("image:1", results.hits[0].id);

    try db.batch(.{
        .writes = &.{
            .{
                .key = "image:1",
                .value = "{\"title\":\"row text\"}",
            },
        },
        .sync_level = .full_index,
    });

    var after_delete = try db.search(alloc, .{
        .index_name = "full_text_index_v0",
        .full_text = .{ .match = .{ .field = "_all", .text = "harbor" } },
        .limit = 10,
    });
    defer after_delete.deinit();

    try std.testing.expectEqual(@as(u32, 0), after_delete.total_hits);
}

test "db dense index can reference existing whole-doc embedding enrichment" {
    const alloc = std.testing.allocator;

    var path_tmp = try db_mod.TestDirectory.init("db");
    defer path_tmp.cleanup();
    const path = path_tmp.path().ptr;
    defer db_mod.cleanupTempDir(path);

    var deterministic = db_mod.embedder_mod.DeterministicDenseEmbedder{};
    var db = try DB.open(alloc, std.mem.span(path), .{
        .enrichment = .{
            .owner_id = "worker-a",
            .dense_embedder = deterministic.interface(),
        },
    });
    defer db.close();

    try db.addIndex(.{
        .name = "dv_gen",
        .kind = .dense_vector,
        .config_json = "{\"field\":\"embedding\",\"dims\":3,\"generator\":{\"kind\":\"dense_embedding\",\"source_field\":\"body\",\"embedding_name\":\"shared_dense_v1\"}}",
    });
    try db.addIndex(.{
        .name = "dv_ref",
        .kind = .dense_vector,
        .config_json = "{\"field\":\"embedding\",\"dims\":3,\"embedding_name\":\"shared_dense_v1\"}",
    });

    try db.batch(.{
        .writes = &.{
            .{ .key = "doc:a", .value = "{\"body\":\"reference embedding text\"}" },
        },
        .sync_level = .write,
    });

    try db.enrichment.runtime.?.waitForApplied(1);
    try db_mod.waitForDerivedReplayTarget(&db);

    const query_vec = try deterministic.interface().embedDense(alloc, "", "reference embedding text", 3);
    defer alloc.free(query_vec);

    var result = try db.search(alloc, .{
        .index_name = "dv_ref",
        .dense = .{ .vector = query_vec, .k = 1 },
    });
    defer result.deinit();
    try std.testing.expectEqual(@as(u32, 1), result.total_hits);
    try std.testing.expectEqualStrings("doc:a", result.hits[0].id);
}

test "db dense index can reference existing chunk embedding enrichment" {
    const alloc = std.testing.allocator;

    var path_tmp = try db_mod.TestDirectory.init("db");
    defer path_tmp.cleanup();
    const path = path_tmp.path().ptr;
    defer db_mod.cleanupTempDir(path);

    var deterministic = db_mod.embedder_mod.DeterministicDenseEmbedder{};
    var db = try DB.open(alloc, std.mem.span(path), .{
        .enrichment = .{
            .owner_id = "worker-a",
            .dense_embedder = deterministic.interface(),
        },
    });
    defer db.close();

    try db.addIndex(.{
        .name = "dv_gen",
        .kind = .dense_vector,
        .config_json = "{\"field\":\"embedding\",\"dims\":3,\"generator\":{\"kind\":\"dense_embedding\",\"source_field\":\"body\",\"chunk_name\":\"body_chunks_v1\",\"chunk_size\":8,\"chunk_overlap\":2,\"embedding_name\":\"chunk_dense_v1\"}}",
    });
    try db.addIndex(.{
        .name = "dv_ref",
        .kind = .dense_vector,
        .config_json = "{\"field\":\"embedding\",\"dims\":3,\"embedding_name\":\"chunk_dense_v1\"}",
    });

    try db.batch(.{
        .writes = &.{
            .{ .key = "doc:a", .value = "{\"body\":\"abcdefghijklmno\"}" },
        },
        .sync_level = .write,
    });

    try db.enrichment.runtime.?.waitForApplied(1);
    try db_mod.waitForDerivedReplayTarget(&db);
    _ = try db_mod.waitForAppliedSequenceAdvance(alloc, &db, "dv_ref", 0);

    const query_vec = try deterministic.interface().embedDense(alloc, "", "abcdefgh", 3);
    defer alloc.free(query_vec);
    const dv_ref = db.core.index_manager.denseIndex("dv_ref") orelse return error.IndexNotFound;
    var direct = try db_mod.waitForDenseIndexResultsWithAttempts(dv_ref.index, query_vec, 3, 2, db_mod.slow_test_wait_attempts);
    defer direct.deinit();

    const chunk_zero = try db_mod.expectedChunkArtifactPublicIdAlloc(alloc, "doc:a", "body_chunks_v1", 0);
    defer alloc.free(chunk_zero);
    var found_chunk_zero = false;
    for (direct.getHits(), 0..) |hit, idx| {
        const internal_id = if (direct.takeMetadata(idx)) |metadata|
            metadata
        else
            (try dv_ref.index.getMetadata(hit.vector_id)) orelse return error.TestUnexpectedResult;
        defer alloc.free(internal_id);
        var identity = try db_mod.artifact_ids.resolvePublicHitIdentityAlloc(alloc, internal_id);
        defer identity.deinit(alloc);
        if (std.mem.eql(u8, identity.id, chunk_zero)) found_chunk_zero = true;
    }
    try std.testing.expect(found_chunk_zero);
}

test "db persists shorthand chunk enrichment catalog across reopen" {
    const alloc = std.testing.allocator;

    var path_tmp = try db_mod.TestDirectory.init("db");
    defer path_tmp.cleanup();
    const path = path_tmp.path().ptr;
    defer db_mod.cleanupTempDir(path);

    {
        var db = try DB.open(alloc, std.mem.span(path), .{});
        defer db.close();

        try db.addIndex(.{
            .name = "dv_v1",
            .kind = .dense_vector,
            .config_json = "{\"field\":\"embedding\",\"dims\":3,\"generator\":{\"kind\":\"dense_embedding\",\"source_field\":\"body\",\"chunk_name\":\"body_chunks_v1\",\"chunk_size\":8,\"chunk_overlap\":2,\"embedding_name\":\"body_dense_v1\"}}",
        });

        const enrichment = (try db.enrichment.getEnrichment(alloc, .chunk, "body_chunks_v1")) orelse return error.TestUnexpectedResult;
        defer {
            var tmp = enrichment;
            tmp.deinit(alloc);
        }
        try std.testing.expectEqualStrings("body", enrichment.field);
        try std.testing.expectEqual(@as(u32, 8), enrichment.chunk_size);
        try std.testing.expectEqual(@as(u32, 2), enrichment.chunk_overlap);

        const embedding = (try db.enrichment.getEnrichment(alloc, .embedding, "body_dense_v1")) orelse return error.TestUnexpectedResult;
        defer {
            var tmp = embedding;
            tmp.deinit(alloc);
        }
        try std.testing.expectEqualStrings("body", embedding.field);
        try std.testing.expectEqualStrings("body_chunks_v1", embedding.source_artifact_name);
        try std.testing.expectEqual(@as(u32, 3), embedding.expected_dims);
    }

    var reopened = try DB.open(alloc, std.mem.span(path), .{});
    defer reopened.close();

    const enrichment = (try reopened.enrichment.getEnrichment(alloc, .chunk, "body_chunks_v1")) orelse return error.TestUnexpectedResult;
    defer {
        var tmp = enrichment;
        tmp.deinit(alloc);
    }
    try std.testing.expectEqualStrings("body", enrichment.field);
    try std.testing.expectEqual(@as(u32, 8), enrichment.chunk_size);
    try std.testing.expectEqual(@as(u32, 2), enrichment.chunk_overlap);

    const embedding = (try reopened.enrichment.getEnrichment(alloc, .embedding, "body_dense_v1")) orelse return error.TestUnexpectedResult;
    defer {
        var tmp = embedding;
        tmp.deinit(alloc);
    }
    try std.testing.expectEqualStrings("body", embedding.field);
    try std.testing.expectEqualStrings("body_chunks_v1", embedding.source_artifact_name);
    try std.testing.expectEqual(@as(u32, 3), embedding.expected_dims);
}

test "db persists shorthand chunk enrichment catalog across reopen with durable lsm primary backend" {
    const alloc = std.testing.allocator;

    var path_tmp = try db_mod.TestDirectory.init("db");
    defer path_tmp.cleanup();
    const path = path_tmp.path().ptr;
    defer db_mod.cleanupTempDir(path);

    const primary_backend: db_mod.PrimaryBackend = .{ .lsm = db_mod.db_config.primary_lsm_options_default };

    {
        var db = try DB.open(alloc, std.mem.span(path), .{
            .primary_backend = primary_backend,
        });
        defer db.close();

        try db.addIndex(.{
            .name = "dv_v1",
            .kind = .dense_vector,
            .config_json = "{\"field\":\"embedding\",\"dims\":3,\"generator\":{\"kind\":\"dense_embedding\",\"source_field\":\"body\",\"chunk_name\":\"body_chunks_v1\",\"chunk_size\":8,\"chunk_overlap\":2,\"embedding_name\":\"body_dense_v1\"}}",
        });

        const enrichment = (try db.enrichment.getEnrichment(alloc, .chunk, "body_chunks_v1")) orelse return error.TestUnexpectedResult;
        defer {
            var tmp = enrichment;
            tmp.deinit(alloc);
        }
        try std.testing.expectEqualStrings("body", enrichment.field);
        try std.testing.expectEqual(@as(u32, 8), enrichment.chunk_size);
        try std.testing.expectEqual(@as(u32, 2), enrichment.chunk_overlap);

        const embedding = (try db.enrichment.getEnrichment(alloc, .embedding, "body_dense_v1")) orelse return error.TestUnexpectedResult;
        defer {
            var tmp = embedding;
            tmp.deinit(alloc);
        }
        try std.testing.expectEqualStrings("body", embedding.field);
        try std.testing.expectEqualStrings("body_chunks_v1", embedding.source_artifact_name);
        try std.testing.expectEqual(@as(u32, 3), embedding.expected_dims);
    }

    var reopened = try DB.open(alloc, std.mem.span(path), .{
        .primary_backend = primary_backend,
    });
    defer reopened.close();

    const enrichment = (try reopened.enrichment.getEnrichment(alloc, .chunk, "body_chunks_v1")) orelse return error.TestUnexpectedResult;
    defer {
        var tmp = enrichment;
        tmp.deinit(alloc);
    }
    try std.testing.expectEqualStrings("body", enrichment.field);
    try std.testing.expectEqual(@as(u32, 8), enrichment.chunk_size);
    try std.testing.expectEqual(@as(u32, 2), enrichment.chunk_overlap);

    const embedding = (try reopened.enrichment.getEnrichment(alloc, .embedding, "body_dense_v1")) orelse return error.TestUnexpectedResult;
    defer {
        var tmp = embedding;
        tmp.deinit(alloc);
    }
    try std.testing.expectEqualStrings("body", embedding.field);
    try std.testing.expectEqualStrings("body_chunks_v1", embedding.source_artifact_name);
    try std.testing.expectEqual(@as(u32, 3), embedding.expected_dims);
}

test "db listEnrichments returns explicit definitions across reopen" {
    const alloc = std.testing.allocator;

    var path_tmp = try db_mod.TestDirectory.init("db");
    defer path_tmp.cleanup();
    const path = path_tmp.path().ptr;
    defer db_mod.cleanupTempDir(path);

    {
        var db = try DB.open(alloc, std.mem.span(path), .{});
        defer db.close();

        try db.enrichment.addEnrichment(.{
            .name = "body_chunks_v1",
            .kind = .chunk,
            .field = "body",
            .chunk_size = 8,
            .chunk_overlap = 2,
        });
        try db.enrichment.addEnrichment(.{
            .name = "body_dense_v1",
            .kind = .embedding,
            .field = "body",
            .source_artifact_name = "body_chunks_v1",
            .expected_dims = 3,
        });
    }

    var reopened = try DB.open(alloc, std.mem.span(path), .{});
    defer reopened.close();

    const enrichments = try reopened.enrichment.listEnrichments(alloc);
    defer db_mod.types.freeEnrichmentConfigs(alloc, enrichments);
    try std.testing.expectEqual(@as(usize, 2), enrichments.len);
    try std.testing.expectEqualStrings("body_chunks_v1", enrichments[0].name);
    try std.testing.expectEqual(.chunk, enrichments[0].kind);
    try std.testing.expectEqualStrings("body_dense_v1", enrichments[1].name);
    try std.testing.expectEqual(.embedding, enrichments[1].kind);
}

test "db listEnrichments returns explicit definitions across reopen with durable lsm primary backend" {
    const alloc = std.testing.allocator;

    var path_tmp = try db_mod.TestDirectory.init("db");
    defer path_tmp.cleanup();
    const path = path_tmp.path().ptr;
    defer db_mod.cleanupTempDir(path);

    const primary_backend: db_mod.PrimaryBackend = .{ .lsm = db_mod.db_config.primary_lsm_options_default };

    {
        var db = try DB.open(alloc, std.mem.span(path), .{
            .primary_backend = primary_backend,
        });
        defer db.close();

        try db.enrichment.addEnrichment(.{
            .name = "body_chunks_v1",
            .kind = .chunk,
            .field = "body",
            .chunk_size = 8,
            .chunk_overlap = 2,
        });
        try db.enrichment.addEnrichment(.{
            .name = "body_dense_v1",
            .kind = .embedding,
            .field = "body",
            .source_artifact_name = "body_chunks_v1",
            .expected_dims = 3,
        });
    }

    var reopened = try DB.open(alloc, std.mem.span(path), .{
        .primary_backend = primary_backend,
    });
    defer reopened.close();

    const enrichments = try reopened.enrichment.listEnrichments(alloc);
    defer db_mod.types.freeEnrichmentConfigs(alloc, enrichments);
    try std.testing.expectEqual(@as(usize, 2), enrichments.len);
    try std.testing.expectEqualStrings("body_chunks_v1", enrichments[0].name);
    try std.testing.expectEqual(.chunk, enrichments[0].kind);
    try std.testing.expectEqualStrings("body_dense_v1", enrichments[1].name);
    try std.testing.expectEqual(.embedding, enrichments[1].kind);
}

test "db full-text chunk consumer filters expired parents under ttl" {
    const alloc = std.testing.allocator;

    var path_tmp = try db_mod.TestDirectory.init("db");
    defer path_tmp.cleanup();
    const path = path_tmp.path().ptr;
    defer db_mod.cleanupTempDir(path);

    var deterministic = db_mod.embedder_mod.DeterministicDenseEmbedder{};
    var db = try DB.open(alloc, std.mem.span(path), .{
        .enrichment = .{
            .owner_id = "worker-a",
            .dense_embedder = deterministic.interface(),
        },
        .start_index_workers = false,
    });
    defer db.close();

    const ttl_duration_ns: u64 = 60 * std.time.ns_per_s;
    try db.setSchema(.{
        .version = 1,
        .default_type = "_default",
        .ttl_duration_ns = ttl_duration_ns,
    });

    try db.addIndex(.{
        .name = "ft_chunks",
        .kind = .full_text,
        .config_json = "{\"chunk_name\":\"body_chunks_v1\"}",
    });
    try db.addIndex(.{
        .name = "dv_v1",
        .kind = .dense_vector,
        .config_json = "{\"field\":\"embedding\",\"dims\":3,\"generator\":{\"kind\":\"dense_embedding\",\"source_field\":\"body\",\"chunk_name\":\"body_chunks_v1\",\"chunk_size\":8,\"chunk_overlap\":2}}",
    });

    const now_ns = db_mod.currentTimeNs();
    try db.batch(.{
        .writes = &.{.{ .key = "doc:old", .value = "{\"body\":\"abcdefghijklmno\"}" }},
        .timestamp_ns = now_ns - 2 * ttl_duration_ns,
        .sync_level = .full_index,
    });
    try db.batch(.{
        .writes = &.{.{ .key = "doc:fresh", .value = "{\"body\":\"abcdefghijklmno\"}" }},
        .timestamp_ns = now_ns,
        .sync_level = .full_index,
    });

    try db.enrichment.runtime.?.waitForApplied(2);
    try db_mod.waitForDerivedReplayTarget(&db);

    var chunk_result = try db_mod.waitForSearchResult(alloc, &db, .{
        .index_name = "ft_chunks",
        .full_text = .{ .match = .{ .field = "body", .text = "abcdefgh" } },
        .return_mode = .chunk,
    }, 1);
    defer chunk_result.deinit();
    try std.testing.expectEqual(@as(u32, 1), chunk_result.total_hits);
    const fresh_chunk_zero = try db_mod.expectedChunkArtifactPublicIdAlloc(alloc, "doc:fresh", "body_chunks_v1", 0);
    defer alloc.free(fresh_chunk_zero);
    try std.testing.expectEqualStrings(fresh_chunk_zero, chunk_result.hits[0].id);

    var parent_result = try db.search(alloc, .{
        .index_name = "ft_chunks",
        .full_text = .{ .match = .{ .field = "body", .text = "abcdefgh" } },
        .return_mode = .parent,
    });
    defer parent_result.deinit();
    try std.testing.expectEqual(@as(u32, 1), parent_result.total_hits);
    try std.testing.expectEqualStrings("doc:fresh", parent_result.hits[0].id);
}

test "db dense chunk consumer supports parent and parent_with_chunks modes" {
    const alloc = std.testing.allocator;

    var path_tmp = try db_mod.TestDirectory.init("db");
    defer path_tmp.cleanup();
    const path = path_tmp.path().ptr;
    defer db_mod.cleanupTempDir(path);

    var deterministic = db_mod.embedder_mod.DeterministicDenseEmbedder{};
    var db = try DB.open(alloc, std.mem.span(path), .{
        .enrichment = .{
            .owner_id = "worker-a",
            .dense_embedder = deterministic.interface(),
        },
    });
    defer db.close();

    try db.addIndex(.{
        .name = "dv_v1",
        .kind = .dense_vector,
        .config_json = "{\"field\":\"embedding\",\"dims\":3,\"generator\":{\"kind\":\"dense_embedding\",\"source_field\":\"body\",\"chunk_name\":\"body_chunks_v1\",\"chunk_size\":8,\"chunk_overlap\":2}}",
    });

    try db.batch(.{
        .writes = &.{
            .{ .key = "doc:a", .value = "{\"body\":\"abcdefghijklmno\"}" },
        },
        .sync_level = .write,
    });

    try db.enrichment.runtime.?.waitForApplied(1);
    try db_mod.waitForDerivedReplayTarget(&db);
    const chunk_prefix = try db_mod.internal_keys.artifactNamedPrefixAlloc(alloc, "doc:a", "chunk", "body_chunks_v1");
    defer alloc.free(chunk_prefix);
    const chunk_records = try db.core.store.scanPrefix(alloc, chunk_prefix);
    defer db_mod.docstore_mod.DocStore.freeResults(alloc, chunk_records);
    try std.testing.expect(chunk_records.len > 0);
    try std.testing.expect(db.core.index_manager.denseIndex("dv_v1").?.index.metadata.active_count > 0);

    const query_vec = try deterministic.interface().embedDense(alloc, "", "abcdefgh", 3);
    defer alloc.free(query_vec);

    var chunk_result = try db_mod.waitForSearchResult(alloc, &db, .{
        .index_name = "dv_v1",
        .dense = .{ .vector = query_vec, .k = 3 },
        .return_mode = .chunk,
    }, 1);
    defer chunk_result.deinit();
    const top_chunk_id = try alloc.dupe(u8, chunk_result.hits[0].id);
    defer alloc.free(top_chunk_id);
    var top_chunk_ref = (try db_mod.artifact_ids.decodeArtifactPublicIdAlloc(alloc, top_chunk_id)) orelse return error.TestUnexpectedResult;
    defer top_chunk_ref.deinit(alloc);
    try std.testing.expectEqual(db_mod.types.ArtifactKind.chunk, top_chunk_ref.kind);
    try std.testing.expectEqualStrings("doc:a", top_chunk_ref.document_id);
    try std.testing.expectEqualStrings("body_chunks_v1", top_chunk_ref.name);
    try std.testing.expect(top_chunk_ref.chunk_id != null);

    var include = try db.resolveDocSetForIdsAlloc(alloc, &.{"doc:a"});
    errdefer include.deinit(alloc);
    const ordinal = switch (include) {
        .ordinals => |ordinals| blk: {
            try std.testing.expectEqual(@as(usize, 1), ordinals.len);
            break :blk ordinals[0];
        },
        else => return error.ExpectedOrdinalDocSet,
    };
    const vector_ids = try db.core.index_manager.lookupDenseVectorIdsForOrdinalsAlloc(alloc, db.core.store, "dv_v1", &.{ordinal});
    defer alloc.free(vector_ids);
    try std.testing.expect(vector_ids.len >= 2);

    var filter = db_mod.doc_set.ResolvedDocFilter{
        .include = include,
        .exclude = .none,
    };
    include = .all;
    defer filter.deinit(alloc);

    var filtered_chunk_result = try db.searchDenseProfiled(alloc, .{
        .index_name = "dv_v1",
        .limit = 3,
        .include_stored = false,
        .return_mode = .parent,
        .resolved_doc_filter = &filter,
    }, .{ .vector = query_vec, .k = 3 });
    defer filtered_chunk_result.result.deinit();
    try std.testing.expect(filtered_chunk_result.profile.raw_hit_count >= 2);
    try std.testing.expectEqual(@as(u32, 1), filtered_chunk_result.result.total_hits);
    try std.testing.expectEqualStrings("doc:a", filtered_chunk_result.result.hits[0].id);

    var parent_result = try db.search(alloc, .{
        .index_name = "dv_v1",
        .dense = .{ .vector = query_vec, .k = 3 },
        .return_mode = .parent,
    });
    defer parent_result.deinit();
    try std.testing.expectEqual(@as(u32, 1), parent_result.total_hits);
    try std.testing.expectEqualStrings("doc:a", parent_result.hits[0].id);

    var parent_with_chunks = try db_mod.waitForSearchResult(alloc, &db, .{
        .index_name = "dv_v1",
        .dense = .{ .vector = query_vec, .k = 3 },
        .return_mode = .parent_with_chunks,
        .max_chunks_per_parent = 1,
    }, 1);
    defer parent_with_chunks.deinit();
    try std.testing.expectEqual(@as(u32, 1), parent_with_chunks.total_hits);
    try std.testing.expectEqualStrings("doc:a", parent_with_chunks.hits[0].id);
    try std.testing.expectEqual(@as(usize, 1), parent_with_chunks.hits[0].chunk_hits.len);
    try std.testing.expectEqualStrings(top_chunk_id, parent_with_chunks.hits[0].chunk_hits[0].id);

    var canonical_grouped = try db_mod.waitForSearchResult(alloc, &db, .{
        .index_name = "dv_v1",
        .dense = .{ .vector = query_vec, .k = 3 },
        .return_mode = .parent_with_chunks,
        .max_chunks_per_parent = 2,
        .hierarchy_grouped_matches = true,
        .hierarchy_match_fields = &.{},
        .hierarchy_match_include_all_fields = false,
        .include_stored = false,
    }, 1);
    defer canonical_grouped.deinit();
    try std.testing.expectEqual(@as(u32, 1), canonical_grouped.total_hits);
    try std.testing.expectEqualStrings("doc:a", canonical_grouped.hits[0].id);
    try std.testing.expectEqual(@as(usize, 2), canonical_grouped.hits[0].chunk_hits.len);
    try std.testing.expectEqualStrings(top_chunk_id, canonical_grouped.hits[0].chunk_hits[0].id);
    try std.testing.expect(!db.core.index_manager.denseIndex("dv_v1").?.supports_unit_grouping);
    try std.testing.expectError(error.UnsupportedHierarchyGrouping, db.search(alloc, .{
        .index_name = "dv_v1",
        .dense = .{ .vector = query_vec, .k = 3 },
        .return_mode = .unit,
    }));

    const doc_a_store_key = try db_mod.internal_keys.documentKeyAlloc(alloc, "doc:a");
    defer alloc.free(doc_a_store_key);
    {
        var txn = try db.core.store.beginWriteTxn();
        errdefer txn.abort();
        try txn.delete(doc_a_store_key);
        try db_mod.doc_identity.markDeletedTxn(alloc, &txn, 2, "doc:a");
        try txn.commit();
    }
    db.core.identity_visibility.summary = null;
    db.clearLiveDocSetCache();
    db.clearNonVisibleDocSetCache();

    var stale_chunk_result = try db.searchDenseProfiled(alloc, .{
        .index_name = "dv_v1",
        .limit = 3,
        .include_stored = false,
        .return_mode = .chunk,
    }, .{ .vector = query_vec, .k = 3 });
    defer stale_chunk_result.result.deinit();
    try std.testing.expectEqual(@as(u32, 0), stale_chunk_result.result.total_hits);
    try std.testing.expectEqual(@as(u32, 0), stale_chunk_result.profile.raw_hit_count);

    var stale_parent_result = try db.search(alloc, .{
        .index_name = "dv_v1",
        .dense = .{ .vector = query_vec, .k = 3 },
        .return_mode = .parent,
    });
    defer stale_parent_result.deinit();
    try std.testing.expectEqual(@as(u32, 0), stale_parent_result.total_hits);
}

test "db dense chunk consumer supports parent and parent_with_chunks modes with durable lsm primary backend" {
    const alloc = std.testing.allocator;

    var path_tmp = try db_mod.TestDirectory.init("db");
    defer path_tmp.cleanup();
    const path = path_tmp.path().ptr;
    defer db_mod.cleanupTempDir(path);

    var deterministic = db_mod.embedder_mod.DeterministicDenseEmbedder{};
    var db = try DB.open(alloc, std.mem.span(path), .{
        .primary_backend = .{ .lsm = .{ .flush_threshold = 1 } },
        .enrichment = .{
            .owner_id = "worker-a",
            .dense_embedder = deterministic.interface(),
        },
    });
    defer db.close();

    try db.addIndex(.{
        .name = "dv_v1",
        .kind = .dense_vector,
        .config_json = "{\"field\":\"embedding\",\"dims\":3,\"generator\":{\"kind\":\"dense_embedding\",\"source_field\":\"body\",\"chunk_name\":\"body_chunks_v1\",\"chunk_size\":8,\"chunk_overlap\":2}}",
    });

    try db.batch(.{
        .writes = &.{
            .{ .key = "doc:a", .value = "{\"body\":\"abcdefghijklmno\"}" },
        },
        .sync_level = .write,
    });

    try db.enrichment.runtime.?.waitForApplied(1);
    try db_mod.waitForDerivedReplayTarget(&db);
    const chunk_prefix = try db_mod.internal_keys.artifactNamedPrefixAlloc(alloc, "doc:a", "chunk", "body_chunks_v1");
    defer alloc.free(chunk_prefix);
    const chunk_records = try db.core.store.scanPrefix(alloc, chunk_prefix);
    defer db_mod.docstore_mod.DocStore.freeResults(alloc, chunk_records);
    try std.testing.expect(chunk_records.len > 0);
    try std.testing.expect(db.core.index_manager.denseIndex("dv_v1").?.index.metadata.active_count > 0);

    const query_vec = try deterministic.interface().embedDense(alloc, "", "abcdefgh", 3);
    defer alloc.free(query_vec);

    var chunk_result = try db_mod.waitForSearchResult(alloc, &db, .{
        .index_name = "dv_v1",
        .dense = .{ .vector = query_vec, .k = 3 },
        .return_mode = .chunk,
    }, 1);
    defer chunk_result.deinit();
    const top_chunk_id = try alloc.dupe(u8, chunk_result.hits[0].id);
    defer alloc.free(top_chunk_id);
    var top_chunk_ref = (try db_mod.artifact_ids.decodeArtifactPublicIdAlloc(alloc, top_chunk_id)) orelse return error.TestUnexpectedResult;
    defer top_chunk_ref.deinit(alloc);
    try std.testing.expectEqual(db_mod.types.ArtifactKind.chunk, top_chunk_ref.kind);
    try std.testing.expectEqualStrings("doc:a", top_chunk_ref.document_id);
    try std.testing.expectEqualStrings("body_chunks_v1", top_chunk_ref.name);
    try std.testing.expect(top_chunk_ref.chunk_id != null);

    var parent_result = try db.search(alloc, .{
        .index_name = "dv_v1",
        .dense = .{ .vector = query_vec, .k = 3 },
        .return_mode = .parent,
    });
    defer parent_result.deinit();
    try std.testing.expectEqual(@as(u32, 1), parent_result.total_hits);
    try std.testing.expectEqualStrings("doc:a", parent_result.hits[0].id);

    var parent_with_chunks = try db_mod.waitForSearchResult(alloc, &db, .{
        .index_name = "dv_v1",
        .dense = .{ .vector = query_vec, .k = 3 },
        .return_mode = .parent_with_chunks,
        .max_chunks_per_parent = 1,
    }, 1);
    defer parent_with_chunks.deinit();
    try std.testing.expectEqual(@as(u32, 1), parent_with_chunks.total_hits);
    try std.testing.expectEqualStrings("doc:a", parent_with_chunks.hits[0].id);
    try std.testing.expectEqual(@as(usize, 1), parent_with_chunks.hits[0].chunk_hits.len);
    try std.testing.expectEqualStrings(top_chunk_id, parent_with_chunks.hits[0].chunk_hits[0].id);
}

test "db restart after provider failure resumes enrichment from retained async replay" {
    const alloc = std.testing.allocator;

    var path_tmp = try db_mod.TestDirectory.init("db");
    defer path_tmp.cleanup();
    const path = path_tmp.path().ptr;
    defer db_mod.cleanupTempDir(path);

    var first_applied_sequence: u64 = 0;
    var failed_target_sequence: u64 = 0;
    {
        // Establish one durable enrichment watermark, then keep returning a
        // retryable provider error for the next document. Its source replay
        // must survive close/reopen until the missing embedding is generated.
        var gated = db_mod.GateDenseEmbedder{};
        var db = try DB.open(alloc, std.mem.span(path), .{
            .enrichment = .{
                .owner_id = "worker-a",
                .dense_embedder = gated.interface(),
            },
            .ttl_cleanup = .{ .enabled = false },
        });
        defer db.close();

        try db.addIndex(.{
            .name = "dv_v1",
            .kind = .dense_vector,
            .config_json = "{\"field\":\"embedding\",\"dims\":3,\"generator\":{\"kind\":\"dense_embedding\",\"source_field\":\"body\",\"embedding_name\":\"body_dense_v1\"}}",
        });
        try db.batch(.{
            .writes = &.{.{ .key = "doc:a", .value = "{\"body\":\"alpha\"}" }},
            .sync_level = .write,
        });
        // Generated-artifact commits also advance the shared journal tail.
        // Wait for the source obligation admitted by this write, rather than
        // racing an unrelated generated append that enrichment does not own.
        first_applied_sequence = db.enrichment.runtime.?.stats().target_sequence;
        try std.testing.expect(first_applied_sequence > 0);
        try db.enrichment.runtime.?.waitForApplied(first_applied_sequence);

        try db.batch(.{
            .writes = &.{.{ .key = "doc:b", .value = "{\"body\":\"beta\"}" }},
            .sync_level = .write,
        });
        failed_target_sequence = db.enrichment.runtime.?.stats().target_sequence;
        try std.testing.expect(failed_target_sequence > first_applied_sequence);

        var attempts: usize = 0;
        while (attempts < db_mod.default_test_wait_attempts and gated.snapshot().blocked_requests == 0) : (attempts += 1) {
            db_mod.sleepPollInterval();
        }
        try std.testing.expect(gated.snapshot().blocked_requests > 0);

        // Stop at the observed provider failure boundary and request truncation
        // through the source tail. Generated indexes cannot reach that tail
        // until the missing embedding exists; waiting for them here would
        // deadlock with the deliberately stopped enrichment worker. Calling
        // the truncation boundary directly exercises the worst-case consumer
        // request while the durable enrichment checkpoint still owns the debt.
        db.enrichment.runtime.?.stop();
        try db_mod.truncateReplaySequenceAsync(db.async_context, failed_target_sequence);

        const failed_stats = try db.stats(alloc);
        defer db_mod.types.freeDBStats(alloc, failed_stats);
        try std.testing.expectEqual(first_applied_sequence, failed_stats.enrichment.applied_sequence);
        try std.testing.expectEqual(failed_target_sequence, failed_stats.enrichment.target_sequence);

        const retained = try db_mod.replay_stream_mod.iterateFrom(alloc, db.core.store, first_applied_sequence + 1);
        defer {
            for (retained) |*entry| entry.deinit(alloc);
            alloc.free(retained);
        }
        try std.testing.expect(retained.len > 0);
        var retained_failed_target = false;
        for (retained) |entry| {
            if (entry.sequence == failed_target_sequence) retained_failed_target = true;
        }
        try std.testing.expect(retained_failed_target);
    }

    var deterministic = db_mod.embedder_mod.DeterministicDenseEmbedder{};
    var reopened = try DB.open(alloc, std.mem.span(path), .{
        .start_index_workers = false,
        .enrichment = .{
            .owner_id = "worker-a",
            .dense_embedder = deterministic.interface(),
        },
        .ttl_cleanup = .{ .enabled = false },
    });
    defer reopened.close();

    try reopened.runUntilIdle();

    const recovered_stats = try reopened.stats(alloc);
    defer db_mod.types.freeDBStats(alloc, recovered_stats);
    try std.testing.expect(recovered_stats.enrichment.applied_sequence >= failed_target_sequence);
    try std.testing.expectEqual(recovered_stats.enrichment.applied_sequence, recovered_stats.enrichment.target_sequence);

    const artifact_key = try db_mod.expectedDocumentEmbeddingArtifactKeyAlloc(alloc, "doc:b", "body_dense_v1");
    defer alloc.free(artifact_key);
    const artifacts = try reopened.core.store.scanPrefix(alloc, artifact_key);
    defer db_mod.docstore_mod.DocStore.freeResults(alloc, artifacts);
    try std.testing.expectEqual(@as(usize, 1), artifacts.len);

    const query_vec = try deterministic.interface().embedDense(alloc, "", "beta", 3);
    defer alloc.free(query_vec);
    var result = try reopened.search(alloc, .{
        .index_name = "dv_v1",
        .dense = .{
            .vector = query_vec,
            .k = 2,
        },
        .limit = 2,
    });
    defer result.deinit();
    try std.testing.expectEqual(@as(u32, 2), result.total_hits);
}

test "db async replay truncation retains journal behind generated enrichment" {
    const alloc = std.testing.allocator;

    var path_tmp = try db_mod.TestDirectory.init("db");
    defer path_tmp.cleanup();
    const path = path_tmp.path().ptr;
    defer db_mod.cleanupTempDir(path);

    var provider_failed: std.Io.Event = .unset;
    var gated = db_mod.GateDenseEmbedder{ .blocked_event = &provider_failed };
    gated.allowed_successes.store(0, .release);
    var db = try DB.open(alloc, std.mem.span(path), .{
        .executor = .{ .backend = .io_threaded },
        .enrichment = .{
            .owner_id = "worker-a",
            .dense_embedder = gated.interface(),
        },
    });
    defer db.close();

    try db.addIndex(.{
        .name = "ft_v1",
        .kind = .full_text,
        .config_json = "{}",
    });
    try db.addIndex(.{
        .name = "semantic_idx",
        .kind = .dense_vector,
        .config_json = "{\"field\":\"embedding\",\"dims\":3,\"metric\":\"cosine\",\"generator\":{\"kind\":\"dense_embedding\",\"source_field\":\"body\",\"embedding_name\":\"semantic_idx\"}}",
    });
    try db.batch(.{
        .writes = &.{
            .{ .key = "doc:a", .value = "{\"title\":\"alpha\",\"body\":\"alpha concept overview\"}" },
            .{ .key = "doc:b", .value = "{\"title\":\"beta\",\"body\":\"beta architecture notes\"}" },
        },
        .sync_level = .write,
    });

    const target_sequence = db.core.nextDerivedSequence();
    try std.testing.expect(target_sequence > 0);
    const deadline = std.Io.Clock.Timestamp.fromNow(std.testing.io, .{
        .raw = .fromSeconds(5),
        .clock = .awake,
    });
    while (!provider_failed.isSet()) {
        provider_failed.waitTimeout(std.testing.io, .{ .deadline = deadline }) catch |err| switch (err) {
            error.Timeout => {
                if (std.Io.Clock.Timestamp.now(std.testing.io, .awake).compare(.gte, deadline))
                    return error.ProviderFailureNotObserved;
            },
            error.Canceled => return err,
        };
    }
    try std.testing.expectEqual(
        @as(u64, 0),
        try db_mod.enrichment_state.loadAppliedSequence(alloc, db.core.store, db_mod.enrichment_runtime_mod.scope_name),
    );

    const expected = try db.core.store.iterateReplayFrom(alloc, 1);
    defer {
        for (expected) |*entry| entry.deinit(alloc);
        alloc.free(expected);
    }
    try std.testing.expect(expected.len > 0);
    try std.testing.expectEqual(target_sequence, expected[expected.len - 1].sequence);

    // Exercise the worst-case consumer ordering directly: request truncation
    // through the entire source tail while the real provider is failing.
    // A checkpoint appearing within a short wall-clock window is neither the
    // retention contract nor evidence that truncation has actually run.
    try db_mod.truncateReplaySequenceAsync(db.async_context, target_sequence);
    const retained = try db.core.store.iterateReplayFrom(alloc, 1);
    defer {
        for (retained) |*entry| entry.deinit(alloc);
        alloc.free(retained);
    }
    try std.testing.expectEqual(expected.len, retained.len);
    for (expected, retained) |before, after| {
        try std.testing.expectEqual(before.sequence, after.sequence);
        try std.testing.expectEqualStrings(before.payload, after.payload);
    }
    gated.allowAll();
    try db.runUntilIdle();
    const recovered = db.enrichment.runtime.?.stats();
    try std.testing.expect(recovered.applied_sequence >= target_sequence);
    try std.testing.expectEqual(recovered.target_sequence, recovered.applied_sequence);
    for ([_][]const u8{ "ft_v1", "semantic_idx" }) |index_name| {
        try std.testing.expect((try db.core.loadAppliedSequence(alloc, index_name)) >= target_sequence);
    }
    var result = try db.search(alloc, .{
        .index_name = "semantic_idx",
        .dense = .{ .vector = &.{ 1, 0, 0 }, .k = 2 },
        .limit = 2,
    });
    defer result.deinit();
    try std.testing.expectEqual(@as(u32, 2), result.total_hits);
}

test "enrichment worker bounds retries against a durable foreign lease" {
    const alloc = std.testing.allocator;

    var path_tmp = try db_mod.TestDirectory.init("db");
    defer path_tmp.cleanup();
    const path = path_tmp.path().ptr;
    defer db_mod.cleanupTempDir(path);

    const lease_ttl_ms: u64 = 5_000;
    var backend = db_mod.mem_backend_mod.Backend.init(alloc, .{});
    defer backend.close();
    var runtime_store = try backend.runtimeStore(alloc, .{ .name = "docs" });
    defer runtime_store.deinit();
    {
        var store = try db_mod.docstore_mod.DocStore.openRuntime(alloc, &runtime_store);
        defer store.close();
        var lease = try db_mod.lease_mod.Lease.init(alloc, &store, db_mod.enrichment_lease_mod.default_lease_key);
        defer lease.deinit();
        try std.testing.expect(try lease.tryAcquire(
            "crashed-owner",
            db_mod.platform_clock.Clock.real().nowRealtimeMs(),
            lease_ttl_ms,
        ));
    }

    var db = try DB.open(alloc, std.mem.span(path), .{
        .primary_backend = .{ .mem = .{} },
        .primary_runtime_store = &runtime_store,
        .physical_root_mode = .external_backend,
        .enrichment = .{
            .owner_id = "replacement-owner",
            .lease_ttl_ms = lease_ttl_ms,
            .enable_without_producers = true,
        },
    });
    defer db.close();

    const runtime = db.enrichment.runtime orelse return error.TestUnexpectedResult;
    runtime.notifySequence(1);

    var io_impl = std.Io.Threaded.init(alloc, .{});
    defer io_impl.deinit();
    io_impl.io().sleep(db_mod.Io.Duration.fromMilliseconds(50), .awake) catch {};

    const failures = runtime.stats().lease_acquire_failures;
    try std.testing.expect(failures >= 1);
    try std.testing.expect(failures <= 4);
}

test "db generated replay atomically promotes staged artifacts and deletes stale generation" {
    const alloc = std.testing.allocator;
    var test_dir = try db_mod.TestDirectory.init("db");
    defer test_dir.cleanup();

    var db = try DB.open(alloc, test_dir.path(), .{
        .start_index_workers = false,
        .ttl_cleanup = .{ .enabled = false },
        .enrichment = .{ .enable_without_producers = true },
    });
    defer db.close();
    const append_ctx = db.enrichment.append_context orelse return error.TestExpectedEnrichmentRuntime;

    const stage_key = "private:pdf-stage:1";
    const final_key = try db_mod.internal_keys.embeddingArtifactKeyForDocumentAlloc(alloc, "doc:new", "visual_v1");
    defer alloc.free(final_key);
    const stale_key = try db_mod.internal_keys.embeddingArtifactKeyForDocumentAlloc(alloc, "doc:stale", "visual_v1");
    defer alloc.free(stale_key);
    try db.core.store.putBatch(&.{
        .{ .key = stage_key, .value = "new-vector" },
        .{ .key = final_key, .value = "old-vector" },
        .{ .key = stale_key, .value = "stale-vector" },
    }, &.{});
    try db.core.shard_manager.prepareSplit("m");
    try db.core.shard_manager.split(99, "m");
    const transition_bytes_before = db.core.index_manager.resource_manager.?.sliceStats(
        .shard_transition_working_set,
    ).used_bytes;

    _ = try db_mod.appendGeneratedBatchFromEnrichment(
        append_ctx,
        .{},
        &.{.{ .staged_key = stage_key, .final_key = final_key }},
        &.{stale_key},
        &.{},
        null,
    );
    try std.testing.expectEqual(
        transition_bytes_before,
        db.core.index_manager.resource_manager.?.sliceStats(.shard_transition_working_set).used_bytes,
    );
    const promoted = try db.core.store.get(alloc, final_key);
    defer alloc.free(promoted);
    try std.testing.expectEqualStrings("new-vector", promoted);
    try std.testing.expectError(error.NotFound, db.core.store.get(alloc, stage_key));
    try std.testing.expectError(error.NotFound, db.core.store.get(alloc, stale_key));
    const split_deltas = try db.core.shard_manager.listDeltasAfter(alloc, 0);
    defer db_mod.shard_mod.freeDeltas(alloc, split_deltas);
    try std.testing.expectEqual(@as(usize, 1), split_deltas.len);
    var saw_promoted_split_value = false;
    for (split_deltas[0].writes) |write| {
        if (std.mem.eql(u8, write.key, final_key)) {
            try std.testing.expectEqualStrings("new-vector", write.value);
            saw_promoted_split_value = true;
        }
    }
    try std.testing.expect(saw_promoted_split_value);

    const missing_stage = "private:pdf-stage:missing";
    try db.core.store.putBatch(&.{
        .{ .key = final_key, .value = "stable-vector" },
        .{ .key = stale_key, .value = "stable-stale-vector" },
    }, &.{});
    try std.testing.expectError(
        error.InvalidGeneratedArtifactPromotion,
        db_mod.appendGeneratedBatchFromEnrichment(
            append_ctx,
            .{},
            &.{.{ .staged_key = missing_stage, .final_key = final_key }},
            &.{stale_key},
            &.{},
            null,
        ),
    );
    const stable_final = try db.core.store.get(alloc, final_key);
    defer alloc.free(stable_final);
    const stable_stale = try db.core.store.get(alloc, stale_key);
    defer alloc.free(stable_stale);
    try std.testing.expectEqualStrings("stable-vector", stable_final);
    try std.testing.expectEqualStrings("stable-stale-vector", stable_stale);
    const deltas_after_failed_promotion = try db.core.shard_manager.listDeltasAfter(alloc, 0);
    defer db_mod.shard_mod.freeDeltas(alloc, deltas_after_failed_promotion);
    try std.testing.expectEqual(@as(usize, 1), deltas_after_failed_promotion.len);

    const fence_key = "\x00\x00__metadata__:generated_replay_fence_test";
    var lease = try db_mod.lease_mod.Lease.init(alloc, db.core.store, fence_key);
    defer lease.deinit();
    const now_ms = db_mod.platform_clock.Clock.real().nowRealtimeMs();
    const acquired = try lease.tryAcquireFenced("worker-a", now_ms, 60_000);
    try std.testing.expect(acquired.acquired);
    try db.core.store.putBatch(&.{.{ .key = stage_key, .value = "fenced-vector" }}, &.{});

    try std.testing.expectError(
        error.EnrichmentLeaseFenceLost,
        db_mod.appendGeneratedBatchFromEnrichment(
            append_ctx,
            .{},
            &.{.{ .staged_key = stage_key, .final_key = final_key }},
            &.{stale_key},
            &.{},
            .{ .lease_key = fence_key, .owner_id = "worker-a", .epoch = acquired.epoch - 1 },
        ),
    );
    const staged_after_fence_rejection = try db.core.store.get(alloc, stage_key);
    defer alloc.free(staged_after_fence_rejection);
    try std.testing.expectEqualStrings("fenced-vector", staged_after_fence_rejection);
    const final_after_fence_rejection = try db.core.store.get(alloc, final_key);
    defer alloc.free(final_after_fence_rejection);
    try std.testing.expectEqualStrings("stable-vector", final_after_fence_rejection);
    const stale_after_fence_rejection = try db.core.store.get(alloc, stale_key);
    defer alloc.free(stale_after_fence_rejection);
    try std.testing.expectEqualStrings("stable-stale-vector", stale_after_fence_rejection);

    _ = try db_mod.appendGeneratedBatchFromEnrichment(
        append_ctx,
        .{},
        &.{.{ .staged_key = stage_key, .final_key = final_key }},
        &.{stale_key},
        &.{},
        .{ .lease_key = fence_key, .owner_id = "worker-a", .epoch = acquired.epoch },
    );
    const fenced_promoted = try db.core.store.get(alloc, final_key);
    defer alloc.free(fenced_promoted);
    try std.testing.expectEqualStrings("fenced-vector", fenced_promoted);
    try std.testing.expectError(error.NotFound, db.core.store.get(alloc, stage_key));
    try std.testing.expectError(error.NotFound, db.core.store.get(alloc, stale_key));

    const source_key = try db_mod.internal_keys.documentKeyAlloc(alloc, "doc:guarded");
    defer alloc.free(source_key);
    try db.core.store.putBatch(&.{
        .{ .key = source_key, .value = "old source" },
        .{ .key = stale_key, .value = "old embedding" },
    }, &.{});
    var source_digest: [32]u8 = undefined;
    var embedding_digest: [32]u8 = undefined;
    std.crypto.hash.sha2.Sha256.hash("old source", &source_digest, .{});
    std.crypto.hash.sha2.Sha256.hash("old embedding", &embedding_digest, .{});
    try db.core.store.putBatch(&.{.{ .key = source_key, .value = "new source" }}, &.{});
    try std.testing.expectError(error.EnrichmentSourceChanged, db_mod.appendGeneratedBatchFromEnrichment(
        append_ctx,
        .{},
        &.{},
        &.{stale_key},
        &.{.{ .key = source_key, .expected_digest = source_digest }},
        null,
    ));
    const retained_old_embedding = try db.core.store.get(alloc, stale_key);
    defer alloc.free(retained_old_embedding);
    try std.testing.expectEqualStrings("old embedding", retained_old_embedding);

    try db.core.store.putBatch(&.{
        .{ .key = source_key, .value = "old source" },
        .{ .key = stale_key, .value = "new embedding" },
    }, &.{});
    try std.testing.expectError(error.EnrichmentSourceChanged, db_mod.appendGeneratedBatchFromEnrichment(
        append_ctx,
        .{},
        &.{},
        &.{stale_key},
        &.{
            .{ .key = source_key, .expected_digest = source_digest },
            .{ .key = stale_key, .expected_digest = embedding_digest },
        },
        null,
    ));
    const retained_new_embedding = try db.core.store.get(alloc, stale_key);
    defer alloc.free(retained_new_embedding);
    try std.testing.expectEqualStrings("new embedding", retained_new_embedding);
}

test "db paged merge fences in-flight source reconciliation through primary commit" {
    if (builtin.single_threaded) return error.SkipZigTest;
    const alloc = std.testing.allocator;
    var donor_tmp = try db_mod.TestDirectory.init("db-paged-worker-donor");
    defer donor_tmp.cleanup();
    var receiver_tmp = try db_mod.TestDirectory.init("db-paged-worker-receiver");
    defer receiver_tmp.cleanup();
    var donor = try DB.open(alloc, donor_tmp.path(), .{ .start_optional_runtimes = false });
    defer donor.close();
    var receiver = try DB.open(alloc, receiver_tmp.path(), .{ .start_optional_runtimes = false });
    var receiver_open = true;
    defer if (receiver_open) receiver.close();
    for ([_]*DB{ &donor, &receiver }) |db| {
        try db.enrichment.addEnrichment(.{ .name = "relations_v1", .kind = .asset, .field = "relations", .content_type = "application/json" });
        try db.addIndex(.{ .name = "g", .kind = .graph, .config_json = "{\"ttl\":{\"duration\":\"1h\"},\"sources\":[{\"artifact\":\"relations_v1\"}]}" });
    }
    try donor.batch(.{ .writes = &.{.{ .key = "doc:a", .value = "{\"relations\":{\"type\":\"links\",\"target\":{\"document_id\":\"doc:b\"}}}" }}, .sync_level = .enrichments });
    try donor.runUntilIdle();
    const due = try donor.core.store.scanPrefix(alloc, &db_mod.internal_keys.graph_edge_expiration_index_prefix);
    defer db_mod.docstore_mod.DocStore.freeResults(alloc, due);
    try std.testing.expectEqual(@as(usize, 1), due.len);
    const candidate = (try db_mod.graph_edge_ttl_expiration.decodeDue(due[0].value)).source;
    var clock = db_mod.platform_clock.ManualClock{};
    clock.setRealtimeNs(candidate.deadline_ns + 1);
    var ttl = db_mod.TtlCleanupContext{ .batch = donor.batchContext(), .grace_period_ns = 0, .clock = clock.clock() };
    try std.testing.expect(try db_mod.expireGraphTtlCandidateContext(&ttl, candidate));
    const rows = try donor.mergeArtifactsPage(alloc, .{ .start = "", .end = "" }, null);
    defer db_mod.PagedGraphMergeTest.free(rows);
    var asset: ?db_mod.types.BatchWrite = null;
    var tomb: ?db_mod.types.BatchWrite = null;
    for (rows) |row| {
        if (db_mod.internal_keys.isAssetArtifactKey(row.key)) asset = row;
        if (db_mod.internal_keys.isGraphEdgeTtlTombstoneKey(row.key)) tomb = row;
    }
    try db_mod.PagedGraphMergeTest.accept(&receiver);
    // Leave a source input ready for a worker, with no materialized contender.
    try receiver.core.store.putBatch(&.{.{ .key = asset.?.key, .value = asset.?.value }}, &.{});
    const Pending = struct {
        db: *DB,
        artifact_key: []const u8,
        tombstone: db_mod.types.BatchWrite,
        entered: std.atomic.Value(bool) = .init(false),
        release: std.atomic.Value(bool) = .init(false),
        worker_done: std.atomic.Value(bool) = .init(false),
        import_done: std.atomic.Value(bool) = .init(false),
        worker_failure: ?anyerror = null,
        import_failure: ?anyerror = null,
        fn pause(ptr: *anyopaque) void {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            self.entered.store(true, .release);
            while (!self.release.load(.acquire)) db_mod.platform_time.yieldNow();
        }
        fn worker(self: *@This()) void {
            defer self.worker_done.store(true, .release);
            _ = db_mod.applyDerivedBatchToIndexAsync(self.db.async_context, .{ .changed_artifact_keys = &.{self.artifact_key} }, .{ .name = "g", .kind = .graph }, .{}) catch |err| {
                self.worker_failure = err;
            };
        }
        fn importer(self: *@This()) void {
            defer self.import_done.store(true, .release);
            // Raft apply uses write visibility too; correctness cannot depend
            // on a later full-index wait rebuilding the source projection.
            self.db.batch(.{ .merge_artifacts = &.{self.tombstone}, .merge_replication = .{
                .transition_id = 1,
                .donor_group_id = 2,
                .receiver_group_id = 3,
                .identity_namespace = self.db.core.identity_namespace,
            }, .sync_level = .write }) catch |err| {
                self.import_failure = err;
            };
        }
    };
    var pending = Pending{ .db = &receiver, .artifact_key = asset.?.key, .tombstone = tomb.? };
    db_mod.test_before_graph_contender_commit = .{ .ctx = &pending, .call = Pending.pause };
    defer db_mod.test_before_graph_contender_commit = null;
    const worker = try std.Thread.spawn(.{}, Pending.worker, .{&pending});
    var worker_joined = false;
    defer if (!worker_joined) {
        pending.release.store(true, .release);
        worker.join();
    };
    while (!pending.entered.load(.acquire) and !pending.worker_done.load(.acquire)) db_mod.platform_time.yieldNow();
    try std.testing.expect(pending.entered.load(.acquire));
    const importer = try std.Thread.spawn(.{}, Pending.importer, .{&pending});
    var import_joined = false;
    defer if (!import_joined) {
        pending.release.store(true, .release);
        importer.join();
    };
    while (receiver.core.index_manager.graph_primary_publication.exclusive_waiters.load(.acquire) == 0 and !pending.import_done.load(.acquire)) db_mod.platform_time.yieldNow();
    try std.testing.expect(!pending.import_done.load(.acquire));
    pending.release.store(true, .release);
    worker.join();
    worker_joined = true;
    importer.join();
    import_joined = true;
    db_mod.test_before_graph_contender_commit = null;
    if (pending.worker_failure) |err| return err;
    if (pending.import_failure) |err| return err;
    // Both an already-running worker and a subsequent retry respect the tomb.
    _ = try db_mod.applyDerivedBatchToIndexAsync(receiver.async_context, .{ .changed_artifact_keys = &.{asset.?.key} }, .{ .name = "g", .kind = .graph }, .{});
    try db_mod.PagedGraphMergeTest.page(&receiver, &.{tomb.?});
    const count_key = try db_mod.internal_keys.graphEdgeContenderCountKeyAlloc(alloc, "doc:a", "g");
    defer alloc.free(count_key);
    const count = try receiver.core.store.get(alloc, count_key);
    defer alloc.free(count);
    try std.testing.expectEqual(@as(usize, 0), (try db_mod.graph_edge_contender.decodeVisibleCount(count, receiver.core.index_manager.graphIndex("g").?.config.coverage_generation)).?);
    const remaining_due = try receiver.core.store.scanPrefix(alloc, &db_mod.internal_keys.graph_edge_expiration_index_prefix);
    defer db_mod.docstore_mod.DocStore.freeResults(alloc, remaining_due);
    try std.testing.expectEqual(@as(usize, 0), remaining_due.len);
    receiver.close();
    receiver_open = false;
    receiver = try DB.open(alloc, receiver_tmp.path(), .{ .start_optional_runtimes = false });
    receiver_open = true;
    const edges = try receiver.getEdges(alloc, "g", "doc:a", "links", .out);
    defer db_mod.graph_mod.GraphIndex.freeEdges(alloc, edges);
    try std.testing.expectEqual(@as(usize, 0), edges.len);
}

test "db paged merge late tombstone retires regenerated graph across restart and retry" {
    const alloc = std.testing.allocator;
    var donor_tmp = try db_mod.TestDirectory.init("db-paged-tombstone-donor");
    defer donor_tmp.cleanup();
    var receiver_tmp = try db_mod.TestDirectory.init("db-paged-tombstone-receiver");
    defer receiver_tmp.cleanup();
    var donor = try DB.open(alloc, donor_tmp.path(), .{ .start_optional_runtimes = false });
    defer donor.close();
    var receiver = try DB.open(alloc, receiver_tmp.path(), .{ .start_optional_runtimes = false });
    var receiver_open = true;
    defer if (receiver_open) receiver.close();
    for ([_]*DB{ &donor, &receiver }) |db| {
        try db.enrichment.addEnrichment(.{ .name = "relations_v1", .kind = .asset, .field = "relations", .content_type = "application/json" });
        try db.addIndex(.{ .name = "g", .kind = .graph, .config_json = "{\"ttl\":{\"duration\":\"1h\"},\"sources\":[{\"artifact\":\"relations_v1\"}]}" });
    }
    const padding = try alloc.alloc(u8, 1024 * 1024);
    defer alloc.free(padding);
    @memset(padding, 'x');
    const primary = try std.fmt.allocPrint(alloc, "{{\"relations\":{{\"type\":\"links\",\"target\":{{\"document_id\":\"doc:b\"}},\"padding\":\"{s}\"}}}}", .{padding});
    defer alloc.free(primary);
    try donor.batch(.{ .writes = &.{.{ .key = "doc:a", .value = primary }}, .sync_level = .enrichments });
    try donor.runUntilIdle();
    const donor_due = try donor.core.store.scanPrefix(alloc, &db_mod.internal_keys.graph_edge_expiration_index_prefix);
    defer db_mod.docstore_mod.DocStore.freeResults(alloc, donor_due);
    try std.testing.expectEqual(@as(usize, 1), donor_due.len);
    const expired = (try db_mod.graph_edge_ttl_expiration.decodeDue(donor_due[0].value)).source;
    var clock = db_mod.platform_clock.ManualClock{};
    clock.setRealtimeNs(expired.deadline_ns + 1);
    var donor_ttl = db_mod.TtlCleanupContext{ .batch = donor.batchContext(), .grace_period_ns = 0, .clock = clock.clock() };
    try std.testing.expect(try db_mod.expireGraphTtlCandidateContext(&donor_ttl, expired));
    try db_mod.PagedGraphMergeTest.accept(&receiver);
    const first = try donor.mergeArtifactsPage(alloc, .{ .start = "", .end = "" }, null);
    defer db_mod.PagedGraphMergeTest.free(first);
    try std.testing.expectEqual(@as(usize, 1), first.len);
    try std.testing.expect(db_mod.internal_keys.isAssetArtifactKey(first[0].key));
    try db_mod.PagedGraphMergeTest.page(&receiver, first);
    const regenerated = try receiver.core.store.scanPrefix(alloc, &db_mod.internal_keys.graph_edge_expiration_index_prefix);
    defer db_mod.docstore_mod.DocStore.freeResults(alloc, regenerated);
    try std.testing.expectEqual(@as(usize, 1), regenerated.len);
    const stale = (try db_mod.graph_edge_ttl_expiration.decodeDue(regenerated[0].value)).source;
    receiver.close();
    receiver_open = false;
    receiver = try DB.open(alloc, receiver_tmp.path(), .{ .start_optional_runtimes = false });
    receiver_open = true;
    const last = try donor.mergeArtifactsPage(alloc, .{ .start = "", .end = "" }, first[0].key);
    defer db_mod.PagedGraphMergeTest.free(last);
    try std.testing.expectEqual(@as(usize, 1), last.len);
    try std.testing.expect(db_mod.internal_keys.isGraphEdgeTtlTombstoneKey(last[0].key));
    for (0..2) |_| {
        try db_mod.PagedGraphMergeTest.page(&receiver, last);
        const edges = try receiver.getEdges(alloc, "g", "doc:a", "links", .out);
        defer db_mod.graph_mod.GraphIndex.freeEdges(alloc, edges);
        try std.testing.expectEqual(@as(usize, 0), edges.len);
        const due = try receiver.core.store.scanPrefix(alloc, &db_mod.internal_keys.graph_edge_expiration_index_prefix);
        defer db_mod.docstore_mod.DocStore.freeResults(alloc, due);
        try std.testing.expectEqual(@as(usize, 0), due.len);
        // A queued pre-import GC candidate cannot fail on the new tombstone.
        var receiver_ttl = db_mod.TtlCleanupContext{ .batch = receiver.batchContext(), .grace_period_ns = 0, .clock = clock.clock() };
        try std.testing.expect(!(try db_mod.expireGraphTtlCandidateContext(&receiver_ttl, stale)));
        try db_mod.PagedGraphMergeTest.page(&receiver, first);
    }
    receiver.close();
    receiver_open = false;
    receiver = try DB.open(alloc, receiver_tmp.path(), .{ .start_optional_runtimes = false });
    receiver_open = true;
    const suppressed = try receiver.getEdges(alloc, "g", "doc:a", "links", .out);
    defer db_mod.graph_mod.GraphIndex.freeEdges(alloc, suppressed);
    try std.testing.expectEqual(@as(usize, 0), suppressed.len);
    // A genuinely changed source revision remains eligible after transfer.
    try receiver.batch(.{ .writes = &.{.{ .key = "doc:a", .value = "{\"relations\":{\"type\":\"links\",\"target\":{\"document_id\":\"doc:b\"},\"weight\":4}}" }}, .sync_level = .enrichments });
    try receiver.runUntilIdle();
    const revived = try receiver.getEdges(alloc, "g", "doc:a", "links", .out);
    defer db_mod.graph_mod.GraphIndex.freeEdges(alloc, revived);
    try std.testing.expectEqual(@as(usize, 1), revived.len);
    try std.testing.expectEqual(@as(f64, 4), revived[0].weight);
}

test "db paged merge contributor accounting survives page order retry and source withdrawal" {
    const alloc = std.testing.allocator;
    for ([_]bool{ false, true }) |timed| {
        for ([_]bool{ false, true }) |reverse| {
            var donor_tmp = try db_mod.TestDirectory.init("db-paged-members-donor");
            defer donor_tmp.cleanup();
            var receiver_tmp = try db_mod.TestDirectory.init("db-paged-members-receiver");
            defer receiver_tmp.cleanup();
            var donor = try DB.open(alloc, donor_tmp.path(), .{ .start_optional_runtimes = false });
            defer donor.close();
            {
                var receiver = try DB.open(alloc, receiver_tmp.path(), .{ .start_optional_runtimes = false });
                defer receiver.close();
                for ([_]*DB{ &donor, &receiver }) |db| {
                    try db.enrichment.addEnrichment(.{ .name = "relations_v1", .kind = .asset, .field = "relations", .content_type = "application/json" });
                    try db.addIndex(.{ .name = "g", .kind = .graph, .config_json = if (timed) "{\"ttl\":{\"duration\":\"1h\"},\"max_edges_per_document\":1,\"sources\":[{\"artifact\":\"relations_v1\"}]}" else "{\"max_edges_per_document\":1,\"sources\":[{\"artifact\":\"relations_v1\"}]}" });
                }
                try donor.batch(.{ .graph_writes = &.{.{ .index_name = "g", .source = "doc:a", .target = "doc:b", .edge_type = "links", .weight = 4 }}, .sync_level = .full_index });
                try donor.batch(.{ .writes = &.{.{ .key = "doc:a", .value = "{\"relations\":{\"type\":\"links\",\"target\":{\"document_id\":\"doc:b\"},\"weight\":2}}" }}, .sync_level = .enrichments });
                try donor.runUntilIdle();
                try db_mod.PagedGraphMergeTest.accept(&receiver);
                const rows = try donor.mergeArtifactsPage(alloc, .{ .start = "", .end = "" }, null);
                defer db_mod.PagedGraphMergeTest.free(rows);
                // Split every row into its own page, including source lifetime
                // rows, and retry each page after its full index publication.
                for (0..rows.len) |i| {
                    const at = if (reverse) rows.len - 1 - i else i;
                    try db_mod.PagedGraphMergeTest.page(&receiver, rows[at .. at + 1]);
                    try db_mod.PagedGraphMergeTest.page(&receiver, rows[at .. at + 1]);
                }
                if (timed) {
                    const donor_due = try donor.core.store.scanPrefix(alloc, &db_mod.internal_keys.graph_edge_expiration_index_prefix);
                    defer db_mod.docstore_mod.DocStore.freeResults(alloc, donor_due);
                    const receiver_due = try receiver.core.store.scanPrefix(alloc, &db_mod.internal_keys.graph_edge_expiration_index_prefix);
                    defer db_mod.docstore_mod.DocStore.freeResults(alloc, receiver_due);
                    try std.testing.expectEqual(@as(usize, 2), receiver_due.len);
                    for (donor_due, receiver_due) |before, after| {
                        try std.testing.expectEqual(try db_mod.graph_edge_ttl_expiration.deadlineFromKey(before.key), try db_mod.graph_edge_ttl_expiration.deadlineFromKey(after.key));
                    }
                }
            }
            var receiver = try DB.open(alloc, receiver_tmp.path(), .{ .start_optional_runtimes = false });
            defer receiver.close();
            const generation = receiver.core.index_manager.graphIndex("g").?.config.coverage_generation;
            const count_key = try db_mod.internal_keys.graphEdgeContenderCountKeyAlloc(alloc, "doc:a", "g");
            defer alloc.free(count_key);
            const prefix = try db_mod.internal_keys.graphEdgeContenderIndexPrefixAlloc(alloc, "doc:a", "g");
            defer alloc.free(prefix);
            const members = try receiver.core.store.scanPrefix(alloc, prefix);
            defer db_mod.docstore_mod.DocStore.freeResults(alloc, members);
            var member_count: usize = 0;
            var source_state: ?[]const u8 = null;
            for (members) |row| {
                if (std.mem.eql(u8, row.key, count_key) or db_mod.internal_keys.isGraphEdgeTtlLifetimeKey(row.key) or db_mod.internal_keys.isGraphEdgeTtlTombstoneKey(row.key)) continue;
                const view = (try db_mod.graph_edge_contender.decode(row.value, generation)) orelse return error.TestUnexpectedResult;
                member_count += 1;
                if (view.source_priority != db_mod.graph_mod.direct_source_priority) source_state = view.state_key;
            }
            try std.testing.expectEqual(@as(usize, 2), member_count);
            try receiver.batch(.{ .writes = &.{.{ .key = "doc:a", .value = "{}" }}, .sync_level = .enrichments });
            try receiver.runUntilIdle();
            const remaining_count = try receiver.core.store.get(alloc, count_key);
            defer alloc.free(remaining_count);
            try std.testing.expectEqual(@as(?usize, 1), try db_mod.graph_edge_contender.decodeVisibleCount(remaining_count, generation));
            const direct = try receiver.getEdges(alloc, "g", "doc:a", "links", .out);
            defer db_mod.graph_mod.GraphIndex.freeEdges(alloc, direct);
            try std.testing.expectEqual(@as(usize, 1), direct.len);
            try std.testing.expectEqual(@as(f64, 4), direct[0].weight);
            var changed = std.ArrayListUnmanaged([]u8).empty;
            defer {
                for (changed.items) |key| alloc.free(key);
                changed.deinit(alloc);
            }
            var changed_set = std.StringHashMapUnmanaged(void).empty;
            defer changed_set.deinit(alloc);
            try std.testing.expectError(error.ResourceLimitExceeded, db_mod.materializeGraphArtifactValuePaged(alloc, receiver.core.store, receiver.core.index_manager, "g", .{ .document_id = @constCast("doc:a"), .name = @constCast("relations_v1"), .kind = .asset }, receiver.core.index_manager.graphArtifactSources("g")[0], source_state orelse return error.TestUnexpectedResult, "{\"type\":\"links\",\"target\":{\"document_id\":\"doc:new\"}}", .{}, &changed, &changed_set));
        }
    }
}

test "db managed admission drain preserves a generation requested during the pass" {
    const alloc = std.testing.allocator;
    var path_tmp = try db_mod.TestDirectory.init("db");
    defer path_tmp.cleanup();
    const path = path_tmp.path().ptr;
    defer db_mod.cleanupTempDir(path);

    var db = try DB.open(alloc, std.mem.span(path), .{
        .open_mode = .writer_no_replay,
        .start_index_workers = false,
        .ttl_cleanup = .{ .enabled = false },
    });
    defer db.close();

    const Hook = struct {
        passes: usize = 0,

        fn afterPass(ptr: *anyopaque, hooked_db: *DB) void {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            self.passes += 1;
            if (self.passes == 1) hooked_db.enrichment.requestManagedAdmissionMaterialization();
        }
    };
    var hook = Hook{};
    DB.test_managed_admission_materialization_hook = .{
        .ptr = &hook,
        .after_pass = Hook.afterPass,
    };
    defer DB.test_managed_admission_materialization_hook = null;

    db.enrichment.requestManagedAdmissionMaterialization();
    try db.drainManagedIndexAdmissions(alloc);
    try std.testing.expectEqual(@as(usize, 2), hook.passes);
    try std.testing.expect(!db.managedAdmissionMaterializationPending());
}

test "db managed admission cannot deadlock replay coverage deletion" {
    const a = std.testing.allocator;
    var directory = try db_mod.TestDirectory.init("coverage-catalog-admission");
    defer directory.cleanup();
    var db = try DB.open(a, directory.path(), .{
        .start_index_workers = false,
        .start_optional_runtimes = false,
        .ttl_cleanup = .{ .enabled = false },
    });
    defer db.close();
    try db.enrichment.addEnrichment(.{ .name = "copied", .kind = .asset, .field = "body", .content_type = "text/plain", .producer_json = "{\"type\":\"copy\"}" });
    try db.addIndex(.{ .name = "text", .kind = .full_text, .config_json = "{\"sources\":[{\"artifact\":\"copied\"}]}" });
    const manager = db.core.index_manager;
    // Retain the current plan without filling the single-flight cache. Replay
    // must use this plan even when a new admission makes the cache cold.
    manager.invalidateWritePlanSnapshot();
    var plan = try manager.writePlanSnapshotAlloc(a);
    defer plan.deinit();
    var guard = try manager.lockManagedIndexApply(.{ .name = "text", .kind = .full_text });
    defer guard.unlock();
    const State = struct {
        db: *DB,
        plan: *const db_mod.index_manager_mod.IndexManager.WritePlanSnapshot,
        cancel: std.atomic.Value(bool) = .init(false),
        done: std.atomic.Value(bool) = .init(false),
        deletion_error: ?anyerror = null,
        writer_error: ?anyerror = null,
        fn write(self: *@This()) void {
            self.db.core.index_manager.catalog_mutex.lockExclusiveIo(std.testing.io, @as(?db_mod.types.CancellationToken, db_mod.types.CancellationToken.fromAtomic(&self.cancel))) catch |err| {
                self.writer_error = err;
                return;
            };
            self.db.core.index_manager.catalog_mutex.unlockExclusive();
        }
        fn erase(self: *@This()) void {
            defer self.done.store(true, .release);
            db_mod.local_mutation.deleteDerivedCoverageForDocKeysWithPlan(std.testing.allocator, self.db.core.store, self.db.core.index_manager, "text", &.{"doc"}, self.plan) catch |err| {
                self.deletion_error = err;
            };
        }
    };
    var state = State{ .db = &db, .plan = &plan };
    const writer = try std.Thread.spawn(.{}, State.write, .{&state});
    var writer_joined = false;
    defer if (!writer_joined) {
        state.cancel.store(true, .release);
        writer.join();
    };
    const catalog = &manager.catalog_mutex;
    const deadline = db_mod.monotonicTimeNs() + 5 * std.time.ns_per_s;
    while (catalog.exclusive_waiters.load(.acquire) == 0 and db_mod.monotonicTimeNs() < deadline)
        @import("antfly_platform").time.yieldNow();
    try std.testing.expect(catalog.exclusive_waiters.load(.acquire) != 0);
    const deletion = try std.Thread.spawn(.{}, State.erase, .{&state});
    var deletion_joined = false;
    defer if (!deletion_joined) {
        state.cancel.store(true, .release);
        deletion.join();
    };
    while (!state.done.load(.acquire) and catalog.shared_waiters.load(.acquire) == 0 and db_mod.monotonicTimeNs() < deadline)
        @import("antfly_platform").time.yieldNow();
    const nested = catalog.shared_waiters.load(.acquire) != 0;
    const completed_without_canceling_writer = state.done.load(.acquire);
    // Rescue the old nested-acquisition path so a regression fails promptly
    // and joins both threads instead of hanging the unit process.
    state.cancel.store(true, .release);
    writer.join();
    writer_joined = true;
    deletion.join();
    deletion_joined = true;
    try std.testing.expect(!nested and completed_without_canceling_writer);
    try std.testing.expectEqual(@as(?anyerror, null), state.deletion_error);
    try std.testing.expectEqual(@as(?anyerror, error.Cancelled), state.writer_error);
}

test "db managed admission reconciliation retains same-name catalog mismatches" {
    const alloc = std.testing.allocator;
    var path_tmp = try db_mod.TestDirectory.init("db");
    defer path_tmp.cleanup();
    const path = path_tmp.path().ptr;
    defer db_mod.cleanupTempDir(path);

    var db = try DB.open(alloc, std.mem.span(path), .{
        .open_mode = .writer_no_replay,
        .start_index_workers = false,
        .ttl_cleanup = .{ .enabled = false },
    });
    defer db.close();
    const index_name = "dense_idx";
    try db.addIndex(.{
        .name = index_name,
        .kind = .dense_vector,
        .config_json = "{\"field\":\"embedding\",\"dims\":2,\"external\":true}",
    });

    const admission_key = try db_mod.internal_keys.managedIndexAdmissionKeyAlloc(alloc, index_name);
    defer alloc.free(admission_key);
    const marker = DB.encodeManagedIndexAdmissionMarker(.{
        .config_hash = 1,
        .source_doc_count = 1,
        .identity_generation = 0,
        .replay_target_sequence = db.core.nextDerivedSequence(),
    });
    try db.core.store.put(admission_key, &marker);
    db.enrichment.requestManagedAdmissionMaterialization();

    try std.testing.expectError(error.InvalidManagedIndexAdmission, db.drainManagedIndexAdmissions(alloc));
    const retained = try db.core.store.get(alloc, admission_key);
    defer alloc.free(retained);
    try std.testing.expectEqualSlices(u8, &marker, retained);
    try std.testing.expect(db.managedAdmissionMaterializationPending());
}

test "db artifact dense target prefers current incarnation outcomes over stale name counter" {
    const alloc = std.testing.allocator;

    var path_tmp = try db_mod.TestDirectory.init("db");
    defer path_tmp.cleanup();
    const path = path_tmp.path().ptr;
    defer db_mod.cleanupTempDir(path);

    var counting = db_mod.CountingDenseEmbedder{};
    var db = try DB.open(alloc, std.mem.span(path), .{
        .start_index_workers = false,
        .start_optional_runtime_workers = false,
        .ttl_cleanup = .{ .enabled = false },
        .enrichment = .{
            .owner_id = "worker-a",
            .dense_embedder = counting.interface(),
        },
    });
    defer db.close();

    const config: db_mod.types.IndexConfig = .{
        .name = "dense_idx",
        .kind = .dense_vector,
        .config_json = "{\"field\":\"body\",\"dims\":3,\"metric\":\"cosine\",\"generator\":{\"kind\":\"dense_embedding\",\"source_field\":\"body\",\"embedding_name\":\"dense_idx\"}}",
        .coverage_generation = 42,
    };
    try db.addIndex(config);
    try db.batch(.{
        .writes = &.{
            .{ .key = "doc:a", .value = "{\"body\":\"alpha concept overview\"}" },
            .{ .key = "doc:b", .value = "{\"body\":\"beta architecture notes\"}" },
        },
        .sync_level = .write,
    });
    try db.runUntilIdle();

    // Same-name recreation can leave this legacy counter stale while the
    // current generation's outcome accounting is complete.
    const counter_key = try DB.denseArtifactTargetCounterKeyAlloc(alloc, config.name);
    defer alloc.free(counter_key);
    var stale_value: [8]u8 = undefined;
    std.mem.writeInt(u64, &stale_value, 1, .little);
    try db.core.store.put(counter_key, &stale_value);

    try std.testing.expectEqual(
        @as(?u64, 2),
        try db_mod.denseTargetCountForIndexContext(db.async_context, config.name),
    );

    // A later source has no embedding yet. The certified generation must
    // still advance across source-only replay records; final source coverage
    // and shadow cutover must remain pending on that same durable state.
    db.async_context.enrichment_desired_running.store(false, .release);
    db.enrichment.runtime.?.stop();
    const applied = try db.core.loadAppliedSequence(alloc, config.name);
    try db.batch(.{
        .writes = &.{.{ .key = "doc:pending", .value = "{\"body\":\"provider pending\"}" }},
        .sync_level = .write,
    });
    const target = db.core.nextDerivedSequence();
    try std.testing.expect(target > applied);
    try std.testing.expectEqual(@as(?u64, null), try db_mod.denseTargetCountForIndexContext(db.async_context, config.name));
    try std.testing.expectEqual(@as(?u64, 2), try db_mod.denseTargetCountForIndexContextWithCoverage(db.async_context, config.name, .materialized));
    try std.testing.expect(try db_mod.canAdvanceDerivedToTargetAsync(db.async_context, .{
        .name = config.name,
        .kind = .dense_vector,
    }, applied, target));
}

test "db multi-source dense target uses physical artifact cardinality" {
    const alloc = std.testing.allocator;

    var path_tmp = try db_mod.TestDirectory.init("db");
    defer path_tmp.cleanup();
    const path = path_tmp.path().ptr;
    defer db_mod.cleanupTempDir(path);

    var db = try DB.open(alloc, std.mem.span(path), .{
        .start_index_workers = false,
        .start_optional_runtime_workers = false,
        .ttl_cleanup = .{ .enabled = false },
    });
    defer db.close();

    for ([_]db_mod.types.EnrichmentConfig{
        .{ .name = "title_dense", .kind = .embedding, .field = "title", .expected_dims = 3, .vector_space = "test:multi-source" },
        .{ .name = "summary_dense", .kind = .embedding, .field = "summary", .expected_dims = 3, .vector_space = "test:multi-source" },
    }) |enrichment| try db.enrichment.addEnrichment(enrichment);
    const config: db_mod.types.IndexConfig = .{
        .name = "dense_idx",
        .kind = .dense_vector,
        .config_json = "{\"field\":\"embedding\",\"dims\":3,\"metric\":\"cosine\",\"sources\":[{\"artifact\":\"title_dense\"},{\"artifact\":\"summary_dense\"}]}",
        .coverage_generation = 42,
    };
    try db.addIndex(config);

    // A single source document may contribute one vector from each stream.
    // Its generation-scoped outcome count is one, while publication must wait
    // for both physical vectors.
    const counter_key = try DB.denseArtifactTargetCounterKeyAlloc(alloc, config.name);
    defer alloc.free(counter_key);
    var counter_value: [8]u8 = undefined;
    std.mem.writeInt(u64, &counter_value, 2, .little);
    try db.core.store.put(counter_key, &counter_value);

    // Seed the complete per-source tuple too: without it the old implementation
    // takes its missing-outcomes artifact-counter fallback and passes this test.
    for ([_][]const u8{ "produced", "skipped", "terminal_failed" }, [_]u64{ 1, 0, 0 }) |outcome, count| {
        const key = try db_mod.internal_keys.derivedCoverageOutcomeCountKeyAlloc(alloc, config.name, 42, outcome);
        defer alloc.free(key);
        var value: [8]u8 = undefined;
        try db.core.store.put(key, db_mod.internal_keys.encodeDerivedCoverageOutcomeCount(&value, count));
    }

    try std.testing.expectEqual(
        @as(?u64, 2),
        try db_mod.denseTargetCountForIndexContext(db.async_context, config.name),
    );
}

test "db graph untimed migration restores source order and direct contributor precedence" {
    const alloc = std.testing.allocator;
    var path_tmp = try db_mod.TestDirectory.init("db-graph-untimed-order");
    defer path_tmp.cleanup();
    {
        var db = try DB.open(alloc, path_tmp.path(), .{ .start_optional_runtimes = false });
        defer db.close();
        try db.enrichment.addEnrichment(.{ .name = "primary_relations_v1", .kind = .asset, .field = "primary_relations", .content_type = "application/json" });
        try db.enrichment.addEnrichment(.{ .name = "fallback_relations_v1", .kind = .asset, .field = "fallback_relations", .content_type = "application/json" });
        try db.addIndex(.{ .name = "relations_graph", .kind = .graph, .config_json =
            \\{"sources":[{"artifact":"primary_relations_v1"},{"artifact":"fallback_relations_v1"}]}
        });
        try db.batch(.{ .writes = &.{.{ .key = "doc:a", .value =
            \\{"primary_relations":{"type":"mentions","target":{"document_id":"doc:b"},"weight":2},"fallback_relations":{"type":"mentions","target":{"document_id":"doc:b"},"weight":3}}
        }}, .sync_level = .enrichments });
        try db.runUntilIdle();
        const initial = try db.getEdges(alloc, "relations_graph", "doc:a", "mentions", .out);
        defer db_mod.graph_mod.GraphIndex.freeEdges(alloc, initial);
        try std.testing.expectEqual(@as(f64, 2), initial[0].weight);
        try std.testing.expectEqual(@as(u64, 1), initial[0].winner_rank);
        // Simulate an older private projection: adjacency is present, but
        // neither contributor snapshots nor the completion marker exist.
        try db.core.index_manager.resetGraphIndexForArtifactRebuild("relations_graph");
        const entry = db.core.index_manager.graphIndex("relations_graph").?;
        try entry.index.batchApply(&.{.{ .source = "doc:a", .target = "doc:b", .edge_type = "mentions", .weight = 99 }}, &.{});
        // A partially replayed owner snapshot must also be overwritten when
        // writable open resumes the migration without its completion marker.
        try entry.index.replaceContributionSnapshots(&.{.{ .source = "doc:a", .target = "doc:b", .edge_type = "mentions", .owner = "doc:a", .contributions = &.{
            .{ .state_key = "partial", .source_priority = 1, .weight = 99, .created_at = 0, .updated_at = 0, .ttl_created_ns = 0, .metadata_json = "" },
        } }});
        try std.testing.expect(!try entry.index.contributionOrderingReady());
        try entry.index.sync(true);
    }
    var db = try DB.open(alloc, path_tmp.path(), .{ .start_optional_runtimes = false });
    defer db.close();
    const entry = db.core.index_manager.graphIndex("relations_graph") orelse return error.TestExpectedGraphIndex;
    try std.testing.expect(try entry.index.contributionOrderingReady());
    const migrated = try db.getEdges(alloc, "relations_graph", "doc:a", "mentions", .out);
    defer db_mod.graph_mod.GraphIndex.freeEdges(alloc, migrated);
    try std.testing.expectEqual(@as(usize, 1), migrated.len);
    try std.testing.expectEqual(@as(f64, 2), migrated[0].weight);
    try std.testing.expectEqual(@as(u64, 1), migrated[0].winner_rank);
    try std.testing.expect(migrated[0].winner_key_hex.len > 0);
    try db.batch(.{ .graph_writes = &.{.{ .index_name = "relations_graph", .source = "doc:a", .target = "doc:b", .edge_type = "mentions", .weight = 4 }}, .sync_level = .full_index });
    // An asset revision cannot displace an explicit direct contribution.
    try db.batch(.{ .writes = &.{.{ .key = "doc:a", .value =
        \\{"primary_relations":{"type":"mentions","target":{"document_id":"doc:b"},"weight":5},"fallback_relations":{"type":"mentions","target":{"document_id":"doc:b"},"weight":3}}
    }}, .sync_level = .enrichments });
    try db.runUntilIdle();
    const direct = try db.getEdges(alloc, "relations_graph", "doc:a", "mentions", .out);
    defer db_mod.graph_mod.GraphIndex.freeEdges(alloc, direct);
    try std.testing.expectEqual(@as(f64, 4), direct[0].weight);
    try std.testing.expectEqual(@as(u64, 0), direct[0].winner_rank);
    try db.batch(.{ .graph_deletes = &.{.{ .index_name = "relations_graph", .source = "doc:a", .target = "doc:b", .edge_type = "mentions" }}, .sync_level = .full_index });
    // Explicit relationship deletion retires the complete assertion, including
    // retained source inputs. Only a later owner lifecycle may revive it.
    const retired = try db.getEdges(alloc, "relations_graph", "doc:a", "mentions", .out);
    defer db_mod.graph_mod.GraphIndex.freeEdges(alloc, retired);
    try std.testing.expectEqual(@as(usize, 0), retired.len);
    try db.batch(.{ .writes = &.{.{ .key = "doc:a", .value =
        \\{"primary_relations":{"type":"mentions","target":{"document_id":"doc:b"},"weight":5},"fallback_relations":{"type":"mentions","target":{"document_id":"doc:b"},"weight":3},"revived":true}
    }}, .sync_level = .full_index });
    const restored = try db.getEdges(alloc, "relations_graph", "doc:a", "mentions", .out);
    defer db_mod.graph_mod.GraphIndex.freeEdges(alloc, restored);
    try std.testing.expectEqual(@as(usize, 1), restored.len);
    try std.testing.expectEqual(@as(f64, 5), restored[0].weight);
    try std.testing.expectEqual(@as(u64, 1), restored[0].winner_rank);
    const due = try db.core.store.scanPrefix(alloc, &db_mod.internal_keys.graph_edge_expiration_index_prefix);
    defer db_mod.docstore_mod.DocStore.freeResults(alloc, due);
    try std.testing.expectEqual(@as(usize, 0), due.len);
}

test "db graph untimed migration restores source order and direct contributor precedence for legacy writes" {
    const alloc = std.testing.allocator;
    var tmp = try db_mod.TestDirectory.init("db-legacy-direct-order");
    defer tmp.cleanup();
    {
        var db = try DB.open(alloc, tmp.path(), .{ .start_optional_runtimes = false });
        defer db.close();
        try db.enrichment.addEnrichment(.{ .name = "relations_v1", .kind = .asset, .field = "relations", .content_type = "application/json" });
        try db.addIndex(.{ .name = "g", .kind = .graph, .config_json = "{\"sources\":[{\"artifact\":\"relations_v1\"}]}" });
        try db.batch(.{ .writes = &.{
            .{ .key = "doc:a", .value = "{\"relations\":{\"type\":\"mentions\",\"target\":{\"document_id\":\"doc:b\"},\"weight\":2}}" },
            .{ .key = "doc:c", .value = "{}" },
            .{ .key = "doc:e", .value = "{\"relations\":{\"type\":\"mentions\",\"target\":{\"document_id\":\"doc:b\"},\"weight\":3}}" },
        }, .sync_level = .enrichments });
        try db.runUntilIdle();
        for ([_][]const u8{ "doc:a", "doc:c" }) |owner| {
            try db.batch(.{ .graph_writes = &.{.{ .index_name = "g", .source = owner, .target = "doc:b", .edge_type = "mentions", .weight = 4 }}, .sync_level = .full_index });
            const generation = db.core.index_manager.graphIndex("g").?.config.coverage_generation;
            const edge_key = try db_mod.internal_keys.graphEdgeArtifactKeyWithSourceAlloc(alloc, owner, "g", "mentions", "doc:b", owner);
            defer alloc.free(edge_key);
            const state_key = try db_mod.internal_keys.graphDirectStateKeyAlloc(alloc, owner, "g");
            defer alloc.free(state_key);
            const global = try db_mod.internal_keys.graphGlobalEdgeContenderKeyAlloc(alloc, "g", generation, edge_key, @intCast(db_mod.graph_mod.direct_source_priority), state_key);
            defer alloc.free(global);
            const local = try db_mod.internal_keys.graphEdgeContenderKeyAlloc(alloc, owner, "g", edge_key, state_key);
            defer alloc.free(local);
            // Old direct writes persisted the artifact but no contributor.
            // The asset contender for doc:a remains unchanged.
            const count_key = try db_mod.internal_keys.graphEdgeContenderCountKeyAlloc(alloc, owner, "g");
            defer alloc.free(count_key);
            const count = try db_mod.graph_edge_contender.encodeVisibleCount(generation, if (std.mem.eql(u8, owner, "doc:a")) 1 else 0);
            try db.core.store.putBatch(&.{.{ .key = count_key, .value = &count }}, &.{ global, local });
        }
        try db.core.index_manager.resetGraphIndexForArtifactRebuild("g");
        const graph = &db.core.index_manager.graphIndex("g").?.index;
        for ([_][]const u8{ "doc:a", "doc:c", "doc:e" }) |owner| {
            try graph.batchApply(&.{.{ .source = owner, .target = "doc:b", .edge_type = "mentions", .weight = if (std.mem.eql(u8, owner, "doc:e")) 3 else 4 }}, &.{});
        }
        try db.core.store.sync(true);
        try graph.sync(true);
    }
    // Repeating migration after discarding the projection also exercises
    // durable recovery and idempotent membership/cardinality publication.
    for (0..2) |pass| {
        var db = try DB.open(alloc, tmp.path(), .{ .start_optional_runtimes = false });
        defer db.close();
        for ([_][]const u8{ "doc:a", "doc:c", "doc:e" }) |owner| {
            const edges = try db.getEdges(alloc, "g", owner, "mentions", .out);
            defer db_mod.graph_mod.GraphIndex.freeEdges(alloc, edges);
            try std.testing.expectEqual(@as(usize, 1), edges.len);
            const source_only = std.mem.eql(u8, owner, "doc:e");
            try std.testing.expectEqual(@as(f64, if (source_only) 3 else 4), edges[0].weight);
            try std.testing.expectEqual(@as(u64, if (source_only) 1 else 0), edges[0].winner_rank);
            const count_key = try db_mod.internal_keys.graphEdgeContenderCountKeyAlloc(alloc, owner, "g");
            defer alloc.free(count_key);
            const raw = try db.core.store.get(alloc, count_key);
            defer alloc.free(raw);
            try std.testing.expectEqual(@as(?usize, 1), try db_mod.graph_edge_contender.decodeVisibleCount(raw, db.core.index_manager.graphIndex("g").?.config.coverage_generation));
        }
        if (pass == 0) {
            try db.core.index_manager.resetGraphIndexForArtifactRebuild("g");
            const graph = &db.core.index_manager.graphIndex("g").?.index;
            for ([_][]const u8{ "doc:a", "doc:c", "doc:e" }) |owner| {
                try graph.batchApply(&.{.{ .source = owner, .target = "doc:b", .edge_type = "mentions", .weight = if (std.mem.eql(u8, owner, "doc:e")) 3 else 4 }}, &.{});
            }
            try graph.sync(true);
        } else {
            try db.batch(.{ .writes = &.{.{ .key = "doc:a", .value = "{\"relations\":{\"type\":\"mentions\",\"target\":{\"document_id\":\"doc:b\"},\"weight\":5}}" }}, .sync_level = .enrichments });
            try db.runUntilIdle();
            const direct = try db.getEdges(alloc, "g", "doc:a", "mentions", .out);
            defer db_mod.graph_mod.GraphIndex.freeEdges(alloc, direct);
            try std.testing.expectEqual(@as(f64, 4), direct[0].weight);
            try db.batch(.{ .graph_deletes = &.{.{ .index_name = "g", .source = "doc:a", .target = "doc:b", .edge_type = "mentions" }}, .sync_level = .full_index });
            const retired = try db.getEdges(alloc, "g", "doc:a", "mentions", .out);
            defer db_mod.graph_mod.GraphIndex.freeEdges(alloc, retired);
            try std.testing.expectEqual(@as(usize, 0), retired.len);
            // Historical direct-contributor migration preserves deletion
            // authority too; a new owner lifecycle can replay its asset input.
            try db.batch(.{ .writes = &.{.{ .key = "doc:a", .value = "{\"relations\":{\"type\":\"mentions\",\"target\":{\"document_id\":\"doc:b\"},\"weight\":5},\"revived\":true}" }}, .sync_level = .full_index });
            const revived = try db.getEdges(alloc, "g", "doc:a", "mentions", .out);
            defer db_mod.graph_mod.GraphIndex.freeEdges(alloc, revived);
            try std.testing.expectEqual(@as(usize, 1), revived.len);
            try std.testing.expectEqual(@as(f64, 5), revived[0].weight);
            try std.testing.expectEqual(@as(u64, 1), revived[0].winner_rank);
            try db.batch(.{ .graph_deletes = &.{.{ .index_name = "g", .source = "doc:c", .target = "doc:b", .edge_type = "mentions" }}, .sync_level = .full_index });
            const deleted = try db.getEdges(alloc, "g", "doc:c", "mentions", .out);
            defer db_mod.graph_mod.GraphIndex.freeEdges(alloc, deleted);
            try std.testing.expectEqual(@as(usize, 0), deleted.len);
        }
    }
}

test "db graph ttl projects source contenders across reopen" {
    const alloc = std.testing.allocator;
    var original_primary_timestamp: u64 = 0;
    var path_tmp = try db_mod.TestDirectory.init("db-source-edge-ttl");
    defer path_tmp.cleanup();
    const path = path_tmp.path().ptr;
    defer db_mod.cleanupTempDir(path);

    {
        var db = try DB.open(alloc, std.mem.span(path), .{});
        defer db.close();
        try db.enrichment.addEnrichment(.{ .name = "primary_relations_v1", .kind = .asset, .field = "primary_relations", .content_type = "application/json" });
        try db.enrichment.addEnrichment(.{ .name = "fallback_relations_v1", .kind = .asset, .field = "fallback_relations", .content_type = "application/json" });
        try db.addIndex(.{ .name = "relations_graph", .kind = .graph, .config_json =
            \\{"ttl":{"duration":"1h"},"sources":[{"artifact":"primary_relations_v1"},{"artifact":"fallback_relations_v1"}]}
        });
        try db.batch(.{ .writes = &.{.{ .key = "doc:a", .value =
            \\{"primary_relations":{"type":"mentions","target":{"document_id":"doc:b"},"weight":2}}
        }}, .sync_level = .enrichments });
        try db.runUntilIdle();
        const generation = db.core.index_manager.graphIndex("relations_graph").?.config.coverage_generation;
        const artifact_key = try db_mod.internal_keys.graphEdgeArtifactKeyWithSourceAlloc(alloc, "doc:a", "relations_graph", "mentions", "doc:b", "doc:a");
        defer alloc.free(artifact_key);
        const contender_prefix = try db_mod.internal_keys.graphGlobalEdgeContenderEdgePrefixAlloc(alloc, "relations_graph", generation, artifact_key);
        defer alloc.free(contender_prefix);
        const initial_contenders = try db.core.store.scanPrefix(alloc, contender_prefix);
        defer db_mod.docstore_mod.DocStore.freeResults(alloc, initial_contenders);
        try std.testing.expectEqual(@as(usize, 1), initial_contenders.len);
        const initial_view = (try db_mod.graph_edge_contender.decode(initial_contenders[0].value, generation)) orelse return error.TestExpectedGraphEdgeContender;
        var initial_edge = try db_mod.enrichment_artifact_codec.decodeGraphEdgeAlloc(alloc, initial_view.payload);
        defer initial_edge.deinit(alloc);
        try db.batch(.{ .writes = &.{.{ .key = "doc:a", .value =
            \\{"primary_relations":{"type":"mentions","target":{"document_id":"doc:b"},"weight":2},"fallback_relations":{"type":"mentions","target":{"document_id":"doc:b"},"weight":3}}
        }}, .sync_level = .enrichments });
        try db.runUntilIdle();
        const edges = try db.getEdges(alloc, "relations_graph", "doc:a", "mentions", .out);
        defer db_mod.graph_mod.GraphIndex.freeEdges(alloc, edges);
        try std.testing.expectEqual(@as(usize, 1), edges.len);
        try std.testing.expectEqual(@as(f64, 2), edges[0].weight);

        const contenders = try db.core.store.scanPrefix(alloc, contender_prefix);
        defer db_mod.docstore_mod.DocStore.freeResults(alloc, contenders);
        var primary_timestamp: u64 = 0;
        var fallback_timestamp: u64 = 0;
        for (contenders) |contender_row| {
            const contender = (try db_mod.graph_edge_contender.decode(contender_row.value, generation)) orelse continue;
            var edge = try db_mod.enrichment_artifact_codec.decodeGraphEdgeAlloc(alloc, contender.payload);
            defer edge.deinit(alloc);
            if (contender.source_priority == 0) {
                try std.testing.expectEqualStrings(initial_view.state_key, contender.state_key);
                primary_timestamp = edge.ttl_created_ns;
            }
            if (contender.source_priority == 1) fallback_timestamp = edge.ttl_created_ns;
        }
        try std.testing.expect(primary_timestamp != 0);
        original_primary_timestamp = primary_timestamp;
        try std.testing.expectEqual(initial_edge.ttl_created_ns, primary_timestamp);
        try std.testing.expect(fallback_timestamp > primary_timestamp);
        var changed_primary = false;
        for (contenders) |contender_row| {
            const contender = (try db_mod.graph_edge_contender.decode(contender_row.value, generation)) orelse continue;
            if (contender.source_priority != 0) continue;
            const header = try db_mod.enrichment_artifact_codec.decodeHeader(contender.payload);
            var edge = try db_mod.enrichment_artifact_codec.decodeGraphEdgeAlloc(alloc, contender.payload);
            defer edge.deinit(alloc);
            const expired_at = db_mod.currentTimeNs() - 2 * std.time.ns_per_hour;
            const expired_payload = try db_mod.enrichment_artifact_codec.encodeGraphEdgeWithTtlAlloc(alloc, if (header.flags.has_source_hash) header.source_hash else null, generation, edge.weight, edge.created_at, edge.updated_at, expired_at, edge.metadata_json);
            defer alloc.free(expired_payload);
            const changed = try db_mod.graph_edge_contender.encodeAlloc(alloc, generation, contender.source_priority, artifact_key, contender.state_key, expired_payload);
            defer alloc.free(changed);
            try db.core.store.putBatch(&.{.{ .key = contender_row.key, .value = changed }}, &.{});
            changed_primary = true;
            break;
        }
        try std.testing.expect(changed_primary);
        try db.core.index_manager.applyGraphMutationsByName("relations_graph", &.{.{
            .index_name = "relations_graph",
            .source = "doc:a",
            .target = "doc:b",
            .edge_type = "mentions",
            .weight = 2,
            .ttl_created_ns = db_mod.currentTimeNs() - 2 * std.time.ns_per_hour,
        }}, &.{});
        const fallback = try db.getEdges(alloc, "relations_graph", "doc:a", "mentions", .out);
        defer db_mod.graph_mod.GraphIndex.freeEdges(alloc, fallback);
        try std.testing.expectEqual(@as(usize, 1), fallback.len);
        try std.testing.expectEqual(@as(f64, 3), fallback[0].weight);
    }
    var reopened = try DB.open(alloc, std.mem.span(path), .{});
    defer reopened.close();
    const edges = try reopened.getEdges(alloc, "relations_graph", "doc:a", "mentions", .out);
    defer db_mod.graph_mod.GraphIndex.freeEdges(alloc, edges);
    try std.testing.expectEqual(@as(usize, 1), edges.len);
    try std.testing.expectEqual(@as(f64, 3), edges[0].weight);
    try reopened.batch(.{ .writes = &.{.{ .key = "doc:a", .value =
        \\{"fallback_relations":{"type":"mentions","target":{"document_id":"doc:b"},"weight":3}}
    }}, .sync_level = .enrichments });
    try reopened.runUntilIdle();
    try reopened.batch(.{ .writes = &.{.{ .key = "doc:a", .value =
        \\{"primary_relations":{"type":"mentions","target":{"document_id":"doc:b"},"weight":2},"fallback_relations":{"type":"mentions","target":{"document_id":"doc:b"},"weight":3}}
    }}, .sync_level = .enrichments });
    try reopened.runUntilIdle();
    const generation = reopened.core.index_manager.graphIndex("relations_graph").?.config.coverage_generation;
    const artifact_key = try db_mod.internal_keys.graphEdgeArtifactKeyWithSourceAlloc(alloc, "doc:a", "relations_graph", "mentions", "doc:b", "doc:a");
    defer alloc.free(artifact_key);
    const contender_prefix = try db_mod.internal_keys.graphGlobalEdgeContenderEdgePrefixAlloc(alloc, "relations_graph", generation, artifact_key);
    defer alloc.free(contender_prefix);
    const new_contenders = try reopened.core.store.scanPrefix(alloc, contender_prefix);
    defer db_mod.docstore_mod.DocStore.freeResults(alloc, new_contenders);
    var restarted_primary_timestamp: u64 = 0;
    for (new_contenders) |row| {
        const contender = (try db_mod.graph_edge_contender.decode(row.value, generation)) orelse continue;
        if (contender.source_priority != 0) continue;
        var edge = try db_mod.enrichment_artifact_codec.decodeGraphEdgeAlloc(alloc, contender.payload);
        defer edge.deinit(alloc);
        restarted_primary_timestamp = edge.ttl_created_ns;
    }
    try std.testing.expect(restarted_primary_timestamp > original_primary_timestamp);
}

test "db graph ttl tombstone suppresses unchanged source replay and admits changed revision" {
    const alloc = std.testing.allocator;
    var path_tmp = try db_mod.TestDirectory.init("db-source-edge-tombstone");
    defer path_tmp.cleanup();
    const path = path_tmp.path().ptr;
    defer db_mod.cleanupTempDir(path);

    var db = try DB.open(alloc, std.mem.span(path), .{});
    defer db.close();
    try db.enrichment.addEnrichment(.{ .name = "relations_v1", .kind = .asset, .field = "relations", .content_type = "application/json" });
    try db.addIndex(.{ .name = "relations_graph", .kind = .graph, .config_json =
        \\{"ttl":{"duration":"1h"},"sources":[{"artifact":"relations_v1"}]}
    });
    try db.batch(.{ .writes = &.{.{ .key = "doc:a", .value =
        \\{"relations":{"type":"mentions","target":{"document_id":"doc:b"},"weight":2}}
    }}, .sync_level = .enrichments });
    try db.runUntilIdle();

    const generation = db.core.index_manager.graphIndex("relations_graph").?.config.coverage_generation;
    const edge_key = try db_mod.internal_keys.graphEdgeArtifactKeyWithSourceAlloc(alloc, "doc:a", "relations_graph", "mentions", "doc:b", "doc:a");
    defer alloc.free(edge_key);
    const contender_prefix = try db_mod.internal_keys.graphGlobalEdgeContenderEdgePrefixAlloc(alloc, "relations_graph", generation, edge_key);
    defer alloc.free(contender_prefix);
    const initial = try db.core.store.scanPrefix(alloc, contender_prefix);
    defer db_mod.docstore_mod.DocStore.freeResults(alloc, initial);
    try std.testing.expectEqual(@as(usize, 1), initial.len);
    const view = (try db_mod.graph_edge_contender.decode(initial[0].value, generation)) orelse return error.TestExpectedGraphEdgeContender;
    const state_key = try alloc.dupe(u8, view.state_key);
    defer alloc.free(state_key);
    const original_payload = try alloc.dupe(u8, view.payload);
    defer alloc.free(original_payload);
    const digest = try db_mod.graph_edge_ttl_tombstone.sourceDigest(alloc, original_payload);
    const tombstone_key = try db_mod.internal_keys.graphEdgeTtlTombstoneKeyAlloc(alloc, edge_key, "relations_graph", generation, state_key);
    defer alloc.free(tombstone_key);
    const tombstone = (db_mod.graph_edge_ttl_tombstone.Tombstone{ .deadline_ns = db_mod.currentTimeNs(), .source_digest = digest }).encode();
    try db.core.store.put(tombstone_key, &tombstone);

    {
        var reconciled = try db_mod.reconcileSingleGraphStateContenders(
            alloc,
            db.core.store,
            "doc:a",
            "relations_graph",
            state_key,
            &.{edge_key},
            &.{.{ .key = edge_key, .value = original_payload }},
            db.core.index_manager.graphArtifactSources("relations_graph"),
            generation,
            std.time.ns_per_hour,
        );
        defer reconciled.deinit(alloc);
        try std.testing.expectEqual(@as(usize, 0), reconciled.visible_count);
        try std.testing.expect(reconciled.winners.map.get(edge_key) == null);
        try db_mod.commitGraphContenderReconcilePage(alloc, db.core.store, &.{edge_key}, &reconciled, &.{}, &.{});
    }
    try std.testing.expectError(error.NotFound, db.core.store.get(alloc, edge_key));
    const suppressed = try db.core.store.scanPrefix(alloc, contender_prefix);
    defer db_mod.docstore_mod.DocStore.freeResults(alloc, suppressed);
    try std.testing.expectEqual(@as(usize, 0), suppressed.len);

    const header = try db_mod.enrichment_artifact_codec.decodeHeader(original_payload);
    var original = try db_mod.enrichment_artifact_codec.decodeGraphEdgeAlloc(alloc, original_payload);
    defer original.deinit(alloc);
    const new_timestamp = db_mod.currentTimeNs();
    const changed_payload = try db_mod.enrichment_artifact_codec.encodeGraphEdgeWithTtlAlloc(
        alloc,
        if (header.flags.has_source_hash) header.source_hash else null,
        generation,
        3,
        original.created_at,
        original.updated_at,
        new_timestamp,
        original.metadata_json,
    );
    defer alloc.free(changed_payload);
    {
        var reconciled = try db_mod.reconcileSingleGraphStateContenders(
            alloc,
            db.core.store,
            "doc:a",
            "relations_graph",
            state_key,
            &.{edge_key},
            &.{.{ .key = edge_key, .value = changed_payload }},
            db.core.index_manager.graphArtifactSources("relations_graph"),
            generation,
            std.time.ns_per_hour,
        );
        defer reconciled.deinit(alloc);
        try std.testing.expectEqual(@as(usize, 1), reconciled.visible_count);
        try std.testing.expect(reconciled.winners.map.get(edge_key) != null);
        try db_mod.commitGraphContenderReconcilePage(alloc, db.core.store, &.{edge_key}, &reconciled, &.{}, &.{});
    }
    try std.testing.expectError(error.NotFound, db.core.store.get(alloc, tombstone_key));
    const restored_raw = try db.core.store.get(alloc, edge_key);
    defer alloc.free(restored_raw);
    var restored = try db_mod.enrichment_artifact_codec.decodeGraphEdgeAlloc(alloc, restored_raw);
    defer restored.deinit(alloc);
    try std.testing.expectEqual(@as(f64, 3), restored.weight);
    try std.testing.expectEqual(new_timestamp, restored.ttl_created_ns);
}

test "db enrichment graph ttl replay honors source tombstone" {
    const alloc = std.testing.allocator;
    var path_tmp = try db_mod.TestDirectory.init("db-enrichment-edge-tombstone");
    defer path_tmp.cleanup();
    const path = path_tmp.path().ptr;
    defer db_mod.cleanupTempDir(path);

    var db = try DB.open(alloc, std.mem.span(path), .{});
    defer db.close();
    try db.enrichment.addEnrichment(.{ .name = "relations_v1", .kind = .asset, .field = "relations", .content_type = "application/json" });
    try db.addIndex(.{ .name = "relations_graph", .kind = .graph, .config_json =
        \\{"ttl":{"duration":"1h"},"sources":[{"artifact":"relations_v1"}]}
    });
    try db.batch(.{ .writes = &.{.{ .key = "doc:a", .value =
        \\{"relations":{"type":"mentions","target":{"document_id":"doc:b"},"weight":2}}
    }}, .sync_level = .enrichments });
    try db.runUntilIdle();

    const generation = db.core.index_manager.graphIndex("relations_graph").?.config.coverage_generation;
    const edge_key = try db_mod.internal_keys.graphEdgeArtifactKeyWithSourceAlloc(alloc, "doc:a", "relations_graph", "mentions", "doc:b", "doc:a");
    defer alloc.free(edge_key);
    const contender_prefix = try db_mod.internal_keys.graphGlobalEdgeContenderEdgePrefixAlloc(alloc, "relations_graph", generation, edge_key);
    defer alloc.free(contender_prefix);
    const initial = try db.core.store.scanPrefix(alloc, contender_prefix);
    defer db_mod.docstore_mod.DocStore.freeResults(alloc, initial);
    try std.testing.expectEqual(@as(usize, 1), initial.len);
    const view = (try db_mod.graph_edge_contender.decode(initial[0].value, generation)) orelse return error.TestExpectedGraphEdgeContender;
    const state_key = try alloc.dupe(u8, view.state_key);
    defer alloc.free(state_key);
    const digest = try db_mod.graph_edge_ttl_tombstone.sourceDigest(alloc, view.payload);
    const tombstone_key = try db_mod.internal_keys.graphEdgeTtlTombstoneKeyAlloc(alloc, edge_key, "relations_graph", generation, state_key);
    defer alloc.free(tombstone_key);
    const tombstone = (db_mod.graph_edge_ttl_tombstone.Tombstone{ .deadline_ns = db_mod.currentTimeNs(), .source_digest = digest }).encode();
    try db.core.store.put(tombstone_key, &tombstone);

    try db.batch(.{ .writes = &.{.{ .key = "doc:a", .value =
        \\{"relations":{"type":"mentions","target":{"document_id":"doc:b"},"weight":2},"note":1}
    }}, .sync_level = .enrichments });
    try db.runUntilIdle();
    const suppressed = try db.core.store.scanPrefix(alloc, contender_prefix);
    defer db_mod.docstore_mod.DocStore.freeResults(alloc, suppressed);
    try std.testing.expectEqual(@as(usize, 0), suppressed.len);
    try std.testing.expectError(error.NotFound, db.core.store.get(alloc, edge_key));

    try db.batch(.{ .writes = &.{.{ .key = "doc:a", .value =
        \\{"relations":{"type":"mentions","target":{"document_id":"doc:b"},"weight":3},"note":1}
    }}, .sync_level = .enrichments });
    try db.runUntilIdle();
    const restored = try db.core.store.scanPrefix(alloc, contender_prefix);
    defer db_mod.docstore_mod.DocStore.freeResults(alloc, restored);
    try std.testing.expectEqual(@as(usize, 1), restored.len);
    try std.testing.expectError(error.NotFound, db.core.store.get(alloc, tombstone_key));
    const visible = try db.getEdges(alloc, "relations_graph", "doc:a", "mentions", .out);
    defer db_mod.graph_mod.GraphIndex.freeEdges(alloc, visible);
    try std.testing.expectEqual(@as(usize, 1), visible.len);
    try std.testing.expectEqual(@as(f64, 3), visible[0].weight);
}

test "db storage kernel graph edges retain typed filters and physical scan budgets" {
    const alloc = std.testing.allocator;
    var directory = try db_mod.TestDirectory.init("db-kernel-graph-edge-budgets");
    defer directory.cleanup();
    var db = try DB.open(alloc, directory.path(), .{ .start_index_workers = false, .start_optional_runtimes = false });
    defer db.close();
    try db.enrichment.addEnrichment(.{ .name = "relations", .kind = .asset, .field = "relations", .content_type = "application/json" });
    try db.addIndex(.{ .name = "relations_graph", .kind = .graph, .config_json =
        \\{"sources":[{"artifact":"relations"}]}
    });
    try db.batch(.{ .writes = &.{.{ .key = "doc:a", .value =
        \\{"relations":[{"type":"links","target":{"document_id":"doc:b"},"weight":2},{"type":"mentions","target":{"document_id":"doc:c"},"weight":3}]}
    }}, .sync_level = .enrichments });
    try db.runUntilIdle();
    const graph = @import("antfly_server_test_sources").local_test_sources.api_distributed_graph;
    const access = db_mod.algebraic_mod.ir.graphEdgeAccessPath("relations_graph");
    var request = graph.GraphEdgesRequest{
        .index_name = try alloc.dupe(u8, "relations_graph"),
        .key = try alloc.dupe(u8, "doc:a"),
        .direction = .out,
        .tensor_access_path = .{
            .owner = try alloc.dupe(u8, access.owner),
            .layout = access.layout,
            .fragments = try alloc.dupe(db_mod.algebraic_mod.ir.TensorFragment, access.fragments),
            .output_dims = try alloc.dupe(db_mod.algebraic_mod.ir.Dimension, access.output_dims),
            .law_ids = try alloc.dupe(@typeInfo(@TypeOf(access.law_ids)).pointer.child, access.law_ids),
        },
        .tensor_program = try graph.graphEdgesTensorProgramEnvelopeAlloc(alloc, "relations_graph"),
        .max_edges = 2,
        .max_owned_bytes = 4096,
        .max_scanned_rows = 4,
    };
    defer request.deinit(alloc);
    const local = @import("../../query.zig");
    {
        var result = try local.executeStorageKernelGraphEdges(alloc, &db, request);
        defer result.deinit(alloc);
        try std.testing.expectEqual(@as(usize, 2), result.edges.len);
        try std.testing.expectEqual(@as(u32, 4), result.scanned_rows);
        const wire = try graph.encodeGraphEdgesResponse(alloc, result);
        defer alloc.free(wire);
        var decoded = try graph.parseGraphEdgesResponse(alloc, wire);
        defer decoded.deinit(alloc);
        try std.testing.expectEqual(@as(u32, 4), decoded.scanned_rows);
    }
    request.max_scanned_rows = 1;
    try std.testing.expectError(error.GraphExploredEdgesBudgetExceeded, local.executeStorageKernelGraphEdges(alloc, &db, request));
    request.max_scanned_rows = 4;
    request.max_edges = 1;
    try std.testing.expectError(error.GraphExploredEdgesBudgetExceeded, local.executeStorageKernelGraphEdges(alloc, &db, request));
    request.max_edges = 2;
    request.max_owned_bytes = 1;
    try std.testing.expectError(error.GraphExploredEdgeBytesBudgetExceeded, local.executeStorageKernelGraphEdges(alloc, &db, request));
    request.max_owned_bytes = 4096;
    request.edge_types = try alloc.alloc([]const u8, 1);
    request.edge_types[0] = try alloc.dupe(u8, "mentions");
    request.max_edges = 1;
    var filtered = try local.executeStorageKernelGraphEdges(alloc, &db, request);
    defer filtered.deinit(alloc);
    try std.testing.expectEqual(@as(usize, 1), filtered.edges.len);
    try std.testing.expectEqualStrings("mentions", filtered.edges[0].edge_type);
    try std.testing.expectEqual(@as(u32, 2), filtered.scanned_rows);
}

test "db document deletion retires graph source contender and due entry" {
    const alloc = std.testing.allocator;
    var path_tmp = try db_mod.TestDirectory.init("db-graph-source-owner-delete");
    defer path_tmp.cleanup();
    var db = try DB.open(alloc, path_tmp.path(), .{ .start_optional_runtimes = false });
    defer db.close();
    try db.enrichment.addEnrichment(.{ .name = "relations_v1", .kind = .asset, .field = "relations", .content_type = "application/json" });
    try db.addIndex(.{ .name = "relations_graph", .kind = .graph, .config_json =
        \\{"ttl":{"duration":"1h"},"sources":[{"artifact":"relations_v1"}]}
    });
    try db.batch(.{ .writes = &.{.{ .key = "doc:a", .value =
        \\{"relations":{"type":"mentions","target":{"document_id":"doc:b"}}}
    }}, .sync_level = .enrichments });
    try db.runUntilIdle();
    const due_before = try db.core.store.scanPrefix(alloc, &db_mod.internal_keys.graph_edge_expiration_index_prefix);
    defer db_mod.docstore_mod.DocStore.freeResults(alloc, due_before);
    try std.testing.expectEqual(@as(usize, 1), due_before.len);
    const candidate = try db_mod.graph_edge_ttl_expiration.decode(due_before[0].value);
    const contender_key = try db_mod.internal_keys.graphGlobalEdgeContenderKeyAlloc(alloc, candidate.index_name, candidate.generation, candidate.edge_key, candidate.source_priority, candidate.state_key);
    defer alloc.free(contender_key);
    try db.batch(.{ .deletes = &.{"doc:a"} });
    const due_after = try db.core.store.scanPrefix(alloc, &db_mod.internal_keys.graph_edge_expiration_index_prefix);
    defer db_mod.docstore_mod.DocStore.freeResults(alloc, due_after);
    try std.testing.expectEqual(@as(usize, 0), due_after.len);
    try std.testing.expectError(error.NotFound, db.core.store.get(alloc, contender_key));
}

test "db graph ttl guarded cleanup removes one source and replays fallback" {
    const alloc = std.testing.allocator;
    var path_tmp = try db_mod.TestDirectory.init("db-graph-ttl-guarded-cleanup");
    defer path_tmp.cleanup();
    const path = path_tmp.path().ptr;
    defer db_mod.cleanupTempDir(path);

    var db = try DB.open(alloc, std.mem.span(path), .{});
    defer db.close();
    try db.enrichment.addEnrichment(.{ .name = "primary_v1", .kind = .asset, .field = "primary", .content_type = "application/json" });
    try db.enrichment.addEnrichment(.{ .name = "fallback_v1", .kind = .asset, .field = "fallback", .content_type = "application/json" });
    try db.addIndex(.{ .name = "relations_graph", .kind = .graph, .config_json =
        \\{"ttl":{"duration":"1h"},"sources":[{"artifact":"primary_v1"},{"artifact":"fallback_v1"}]}
    });
    try db.batch(.{ .writes = &.{.{ .key = "doc:a", .value =
        \\{"primary":{"type":"mentions","target":{"document_id":"doc:b"},"weight":2},"fallback":{"type":"mentions","target":{"document_id":"doc:b"},"weight":3}}
    }}, .sync_level = .enrichments });
    try db.runUntilIdle();

    const generation = db.core.index_manager.graphIndex("relations_graph").?.config.coverage_generation;
    const edge_key = try db_mod.internal_keys.graphEdgeArtifactKeyWithSourceAlloc(alloc, "doc:a", "relations_graph", "mentions", "doc:b", "doc:a");
    defer alloc.free(edge_key);
    const prefix = try db_mod.internal_keys.graphGlobalEdgeContenderEdgePrefixAlloc(alloc, "relations_graph", generation, edge_key);
    defer alloc.free(prefix);
    const contenders = try db.core.store.scanPrefix(alloc, prefix);
    defer db_mod.docstore_mod.DocStore.freeResults(alloc, contenders);
    try std.testing.expectEqual(@as(usize, 2), contenders.len);
    var candidate: ?db_mod.GraphTtlCandidate = null;
    for (contenders) |row| {
        const view = (try db_mod.graph_edge_contender.decode(row.value, generation)) orelse continue;
        if (view.source_priority != 0) continue;
        var edge = try db_mod.enrichment_artifact_codec.decodeGraphEdgeAlloc(alloc, view.payload);
        defer edge.deinit(alloc);
        var digest: [32]u8 = undefined;
        db_mod.GraphTtlSha256.hash(row.value, &digest, .{});
        candidate = .{
            .index_name = "relations_graph",
            .generation = generation,
            .edge_key = edge_key,
            .state_key = view.state_key,
            .source_priority = view.source_priority,
            .deadline_ns = edge.ttl_created_ns + std.time.ns_per_hour,
            .contender_digest = digest,
        };
    }
    const due = candidate orelse return error.TestExpectedGraphEdgeContender;
    var manual_clock = db_mod.platform_clock.ManualClock{};
    manual_clock.setRealtimeNs(due.deadline_ns + 1);
    var ttl_ctx = db_mod.TtlCleanupContext{ .batch = db.batchContext(), .grace_period_ns = 0, .clock = manual_clock.clock() };
    try std.testing.expect(try db_mod.expireGraphTtlCandidateContext(&ttl_ctx, due));
    try std.testing.expect(!(try db_mod.expireGraphTtlCandidateContext(&ttl_ctx, due)));

    const remaining = try db.core.store.scanPrefix(alloc, prefix);
    defer db_mod.docstore_mod.DocStore.freeResults(alloc, remaining);
    try std.testing.expectEqual(@as(usize, 1), remaining.len);
    const survivor = (try db_mod.graph_edge_contender.decode(remaining[0].value, generation)) orelse return error.TestExpectedGraphEdgeContender;
    try std.testing.expectEqual(@as(usize, 1), survivor.source_priority);
    const tombstone_key = try db_mod.internal_keys.graphEdgeTtlTombstoneKeyAlloc(alloc, edge_key, "relations_graph", generation, due.state_key);
    defer alloc.free(tombstone_key);
    const tombstone_raw = try db.core.store.get(alloc, tombstone_key);
    defer alloc.free(tombstone_raw);
    try std.testing.expectEqual(due.deadline_ns, (try db_mod.graph_edge_ttl_tombstone.Tombstone.decode(tombstone_raw)).deadline_ns);
    const fallback = try db.getEdges(alloc, "relations_graph", "doc:a", "mentions", .out);
    defer db_mod.graph_mod.GraphIndex.freeEdges(alloc, fallback);
    try std.testing.expectEqual(@as(usize, 1), fallback.len);
    try std.testing.expectEqual(@as(f64, 3), fallback[0].weight);

    try db.batch(.{ .writes = &.{.{ .key = "doc:a", .value =
        \\{"primary":{"type":"mentions","target":{"document_id":"doc:b"},"weight":2},"fallback":{"type":"mentions","target":{"document_id":"doc:b"},"weight":3},"note":1}
    }}, .sync_level = .enrichments });
    try db.runUntilIdle();
    const unchanged = try db.getEdges(alloc, "relations_graph", "doc:a", "mentions", .out);
    defer db_mod.graph_mod.GraphIndex.freeEdges(alloc, unchanged);
    try std.testing.expectEqual(@as(f64, 3), unchanged[0].weight);
    try db.batch(.{ .writes = &.{.{ .key = "doc:a", .value =
        \\{"primary":{"type":"mentions","target":{"document_id":"doc:b"},"weight":4},"fallback":{"type":"mentions","target":{"document_id":"doc:b"},"weight":3},"note":1}
    }}, .sync_level = .enrichments });
    try db.runUntilIdle();
    try std.testing.expectError(error.NotFound, db.core.store.get(alloc, tombstone_key));
    const revived = try db.getEdges(alloc, "relations_graph", "doc:a", "mentions", .out);
    defer db_mod.graph_mod.GraphIndex.freeEdges(alloc, revived);
    try std.testing.expectEqual(@as(f64, 4), revived[0].weight);
}

test "db graph ttl due index rebuild resumes after reopen" {
    const alloc = std.testing.allocator;
    var path_tmp = try db_mod.TestDirectory.init("db-graph-ttl-due-rebuild");
    defer path_tmp.cleanup();
    {
        var db = try DB.open(alloc, path_tmp.path(), .{ .start_optional_runtimes = false });
        defer db.close();
        try db.enrichment.addEnrichment(.{ .name = "relations_v1", .kind = .asset, .field = "relations", .content_type = "application/json" });
        try db.addIndex(.{ .name = "relations_graph", .kind = .graph, .config_json =
            \\{"ttl":{"duration":"1h"},"sources":[{"artifact":"relations_v1"}]}
        });
        try db.addIndex(.{ .name = "direct_graph", .kind = .graph, .config_json = "{\"ttl\":{\"duration\":\"1h\"}}" });
        try db.batch(.{ .writes = &.{.{ .key = "doc:a", .value =
            \\{"relations":{"type":"mentions","target":{"document_id":"doc:b"},"weight":2}}
        }}, .sync_level = .enrichments });
        try db.batch(.{ .writes = &.{.{ .key = "doc:c", .value =
            \\{"title":"direct","_edges":{"direct_graph":{"links":[{"target":"doc:d"}]}}}
        }}, .sync_level = .full_index });
        try db.runUntilIdle();
        const due_rows = try db.core.store.scanPrefix(alloc, &db_mod.internal_keys.graph_edge_expiration_index_prefix);
        defer db_mod.docstore_mod.DocStore.freeResults(alloc, due_rows);
        try std.testing.expectEqual(@as(usize, 2), due_rows.len);
        for (due_rows) |row| try db.core.store.delete(row.key);
        try db.core.store.put(db_mod.graph_edge_ttl_due_rebuild_marker, "1");
    }
    {
        var db = try DB.open(alloc, path_tmp.path(), .{ .start_optional_runtimes = false });
        defer db.close();
        const due_rows = try db.core.store.scanPrefix(alloc, &db_mod.internal_keys.graph_edge_expiration_index_prefix);
        defer db_mod.docstore_mod.DocStore.freeResults(alloc, due_rows);
        try std.testing.expectEqual(@as(usize, 2), due_rows.len);
        try std.testing.expectError(error.NotFound, db.core.store.get(alloc, db_mod.graph_edge_ttl_due_rebuild_marker));
        var saw_source = false;
        var saw_direct = false;
        for (due_rows) |row| switch (try db_mod.graph_edge_ttl_expiration.decodeDue(row.value)) {
            .source => |due| {
                saw_source = true;
                try std.testing.expectEqual(due.deadline_ns, try db_mod.graph_edge_ttl_expiration.deadlineFromKey(row.key));
            },
            .direct => |due| {
                saw_direct = true;
                try std.testing.expectEqual(due.deadline_ns, try db_mod.graph_edge_ttl_expiration.deadlineFromKey(row.key));
            },
        };
        try std.testing.expect(saw_source and saw_direct);
    }
}

test "db lookup includes unified artifact projection when _artifacts is requested" {
    const alloc = std.testing.allocator;

    var path_tmp = try db_mod.TestDirectory.init("db");
    defer path_tmp.cleanup();
    const path = path_tmp.path().ptr;
    defer db_mod.cleanupTempDir(path);

    var db = try DB.open(alloc, std.mem.span(path), .{});
    defer db.close();

    try db.batch(.{
        .writes = &.{.{ .key = "doc:a", .value = "{\"title\":\"alpha\",\"body\":\"hello world\"}" }},
    });
    try db.enrichment.addEnrichment(.{
        .name = "page_ocr_v1",
        .kind = .asset,
        .field = "body",
        .content_type = "text/plain",
    });
    try db.enrichment.addEnrichment(.{
        .name = "entities_v1",
        .kind = .asset,
        .field = "body",
        .content_type = "application/json",
    });

    const chunk_zero = try db_mod.expectedChunkArtifactKeyAlloc(alloc, "doc:a", "body_chunks_v1", 0);
    defer alloc.free(chunk_zero);
    const chunk_one = try db_mod.expectedChunkArtifactKeyAlloc(alloc, "doc:a", "body_chunks_v1", 1);
    defer alloc.free(chunk_one);
    try db.core.store.put(chunk_zero, "{\"body\":\"hello\",\"_artifact_name\":\"body_chunks_v1\",\"_chunk_id\":0,\"_start_offset\":0,\"_end_offset\":5,\"_artifact_unit_fingerprint\":\"private\"}");
    try db.core.store.put(chunk_one, "{\"body\":\"world\",\"_artifact_name\":\"body_chunks_v1\",\"_chunk_id\":1,\"_start_offset\":6,\"_end_offset\":11}");

    const dense_key = try db_mod.expectedDocumentEmbeddingArtifactKeyAlloc(alloc, "doc:a", "body_dense_v1");
    defer alloc.free(dense_key);
    try db_mod.putDenseEmbeddingArtifactForTest(&db, alloc, dense_key, null, &[_]f32{ 1, 2, 3 });

    var ocr_ref = db_mod.types.ArtifactRef{
        .document_id = try alloc.dupe(u8, "doc:a"),
        .name = try alloc.dupe(u8, "page_ocr_v1"),
        .kind = .asset,
    };
    defer ocr_ref.deinit(alloc);
    const ocr_key = try db_mod.artifact_ids.internalKeyForArtifactRefAlloc(alloc, ocr_ref);
    defer alloc.free(ocr_key);
    try db.core.store.put(ocr_key, "Revenue increased");

    var entities_ref = db_mod.types.ArtifactRef{
        .document_id = try alloc.dupe(u8, "doc:a"),
        .name = try alloc.dupe(u8, "entities_v1"),
        .kind = .asset,
    };
    defer entities_ref.deinit(alloc);
    const entities_key = try db_mod.artifact_ids.internalKeyForArtifactRefAlloc(alloc, entities_ref);
    defer alloc.free(entities_key);
    try db.core.store.put(entities_key, "{\"entities\":[{\"text\":\"Antfly\"}],\"relations\":[],\"_artifact_unit_fingerprint\":\"private\"}");

    var result = (try db.lookup(alloc, "doc:a", .{
        .fields = &.{ "title", "_artifacts" },
        .include_all_fields = false,
    })).?;
    defer result.deinit(alloc);

    const parsed = try std.json.parseFromSlice(std.json.Value, alloc, result.json, .{});
    defer parsed.deinit();

    try std.testing.expectEqualStrings("alpha", parsed.value.object.get("title").?.string);
    const artifacts = parsed.value.object.get("_artifacts").?.object;

    const chunks = artifacts.get("body_chunks_v1").?.object;
    try std.testing.expectEqualStrings("chunk_set", chunks.get("kind").?.string);
    const items = chunks.get("items").?.array.items;
    try std.testing.expectEqual(@as(usize, 2), items.len);
    try std.testing.expect(std.mem.startsWith(u8, items[0].object.get("artifact_id").?.string, "af1:chunk:"));
    try std.testing.expectEqualStrings("chunk", items[0].object.get("artifact_ref").?.object.get("kind").?.string);
    try std.testing.expectEqual(@as(i64, 0), items[0].object.get("artifact_ref").?.object.get("chunk_id").?.integer);
    try std.testing.expectEqualStrings("hello", items[0].object.get("value").?.object.get("_content").?.string);
    try std.testing.expect(items[0].object.get("value").?.object.get(db_mod.hierarchy_navigation_unit_fingerprint_field) == null);

    const embedding = artifacts.get("body_dense_v1").?.object;
    try std.testing.expect(std.mem.startsWith(u8, embedding.get("artifact_id").?.string, "af1:embedding:"));
    try std.testing.expectEqualStrings("embedding", embedding.get("kind").?.string);
    try std.testing.expectEqualStrings("application/vnd.antfly.embedding+binary", embedding.get("content_type").?.string);
    try std.testing.expectEqual(@as(i64, 3), embedding.get("dims").?.integer);
    try std.testing.expect(embedding.get("value").? == .null);

    const ocr = artifacts.get("page_ocr_v1").?.object;
    try std.testing.expect(std.mem.startsWith(u8, ocr.get("artifact_id").?.string, "af1:asset:"));
    try std.testing.expectEqualStrings("asset", ocr.get("artifact_ref").?.object.get("kind").?.string);
    try std.testing.expectEqualStrings("asset", ocr.get("kind").?.string);
    try std.testing.expectEqualStrings("text/plain", ocr.get("content_type").?.string);
    try std.testing.expectEqualStrings("Revenue increased", ocr.get("value").?.string);

    const entities = artifacts.get("entities_v1").?.object;
    try std.testing.expectEqualStrings("application/json", entities.get("content_type").?.string);
    try std.testing.expectEqualStrings("Antfly", entities.get("value").?.object.get("entities").?.array.items[0].object.get("text").?.string);
    try std.testing.expect(entities.get("value").?.object.get(db_mod.hierarchy_navigation_unit_fingerprint_field) == null);
}

test "db split preserves ttl graph source fallback ownership on destination" {
    const alloc = std.testing.allocator;

    var parent_tmp = try db_mod.TestDirectory.init("db");
    defer parent_tmp.cleanup();
    const parent_path = parent_tmp.path().ptr;
    defer db_mod.cleanupTempDir(parent_path);
    var child_tmp = try db_mod.TestDirectory.init("db");
    defer child_tmp.cleanup();
    const child_path = child_tmp.path().ptr;
    defer db_mod.cleanupTempDir(child_path);

    var parent = try DB.open(alloc, std.mem.span(parent_path), .{});
    defer parent.close();
    try parent.enrichment.addEnrichment(.{
        .name = "primary_relations_v1",
        .kind = .asset,
        .field = "primary_relations",
        .content_type = "application/json",
    });
    try parent.enrichment.addEnrichment(.{
        .name = "fallback_relations_v1",
        .kind = .asset,
        .field = "fallback_relations",
        .content_type = "application/json",
    });
    try parent.addIndex(.{
        .name = "relations_graph",
        .kind = .graph,
        .config_json =
        \\{"ttl":{"duration":"1h"},"sources":[{"artifact":"primary_relations_v1"},{"artifact":"fallback_relations_v1"}]}
        ,
    });
    try parent.batch(.{
        .writes = &.{.{
            .key = "doc:z",
            .value =
            \\{"primary_relations":{"type":"mentions","target":{"document_id":"doc:y"},"weight":2,"winner":"primary"},"fallback_relations":{"type":"mentions","target":{"document_id":"doc:y"},"weight":1,"winner":"fallback"}}
            ,
        }},
        .sync_level = .enrichments,
    });
    try parent.runUntilIdle();
    try std.testing.expect((try parent.core.index_manager.graphIndex("relations_graph").?.index.nextContributionExpirationAtOrAfter(0)) != null);
    try parent.split(parent.getRange(), "doc:m", "", std.mem.span(child_path), true);

    var child = try DB.open(alloc, std.mem.span(child_path), .{});
    defer child.close();
    try std.testing.expect((try child.core.index_manager.graphIndex("relations_graph").?.index.nextContributionExpirationAtOrAfter(0)) != null);
    const child_due_before = try child.core.store.scanPrefix(alloc, &db_mod.internal_keys.graph_edge_expiration_index_prefix);
    defer db_mod.docstore_mod.DocStore.freeResults(alloc, child_due_before);
    try std.testing.expectEqual(@as(usize, 2), child_due_before.len);
    try child.batch(.{
        .writes = &.{.{
            .key = "doc:z",
            .value =
            \\{"fallback_relations":{"type":"mentions","target":{"document_id":"doc:y"},"weight":1,"winner":"fallback"}}
            ,
        }},
        .sync_level = .enrichments,
    });
    try child.runUntilIdle();
    const child_due_after = try child.core.store.scanPrefix(alloc, &db_mod.internal_keys.graph_edge_expiration_index_prefix);
    defer db_mod.docstore_mod.DocStore.freeResults(alloc, child_due_after);
    try std.testing.expectEqual(@as(usize, 1), child_due_after.len);

    const child_edges = try child.getEdges(alloc, "relations_graph", "doc:z", "mentions", .out);
    defer db_mod.graph_mod.GraphIndex.freeEdges(alloc, child_edges);
    try std.testing.expectEqual(@as(usize, 1), child_edges.len);
    try std.testing.expectEqual(@as(f64, 1), child_edges[0].weight);
    try std.testing.expect(std.mem.indexOf(u8, child_edges[0].metadata, "\"winner\":\"fallback\"") != null);

    try parent.finalizeSplit(.{ .start = "", .end = "doc:m" });
    try parent.runUntilIdle();
    const parent_due = try parent.core.store.scanPrefix(alloc, &db_mod.internal_keys.graph_edge_expiration_index_prefix);
    defer db_mod.docstore_mod.DocStore.freeResults(alloc, parent_due);
    try std.testing.expectEqual(@as(usize, 0), parent_due.len);
    try std.testing.expectEqual(@as(?u64, null), try parent.core.index_manager.graphIndex("relations_graph").?.index.nextContributionExpirationAtOrAfter(0));
    const parent_edges = try parent.getEdges(alloc, "relations_graph", "doc:z", "mentions", .out);
    defer db_mod.graph_mod.GraphIndex.freeEdges(alloc, parent_edges);
    try std.testing.expectEqual(@as(usize, 0), parent_edges.len);
}

test "db split cutover fences enrichment to the owning range" {
    const alloc = std.testing.allocator;

    var path_tmp = try db_mod.TestDirectory.init("db");
    defer path_tmp.cleanup();
    const path = path_tmp.path().ptr;
    defer db_mod.cleanupTempDir(path);

    var dest_tmp = try db_mod.TestDirectory.init("db");
    defer dest_tmp.cleanup();
    const dest = dest_tmp.path().ptr;
    defer db_mod.cleanupTempDir(dest);

    var deterministic_parent = db_mod.embedder_mod.DeterministicDenseEmbedder{};
    var db = try DB.open(alloc, std.mem.span(path), .{
        .enrichment = .{
            .owner_id = "split-parent",
            .dense_embedder = deterministic_parent.interface(),
        },
    });
    defer db.close();

    try db.addIndex(.{
        .name = "dv_v1",
        .kind = .dense_vector,
        .config_json = "{\"field\":\"embedding\",\"dims\":3,\"generator\":{\"kind\":\"dense_embedding\",\"source_field\":\"body\",\"embedding_name\":\"body_dense_v1\"}}",
    });

    try db.batch(.{
        .writes = &.{
            .{ .key = "doc:a", .value = "{\"body\":\"left side\"}" },
            .{ .key = "doc:z", .value = "{\"body\":\"right side\"}" },
        },
        .sync_level = .write,
    });
    try db.enrichment.runtime.?.waitForApplied(1);
    try db.executor.waitForAll(2);

    try db.split(db.getRange(), "doc:m", "", std.mem.span(dest), true);
    try db.finalizeSplit(.{ .start = "", .end = "doc:m" });

    try std.testing.expectError(error.KeyOutOfRange, db.batch(.{
        .writes = &.{.{ .key = "doc:y", .value = "{\"body\":\"parent should reject\"}" }},
        .sync_level = .write,
    }));

    var deterministic_child = db_mod.embedder_mod.DeterministicDenseEmbedder{};
    var child = try DB.open(alloc, std.mem.span(dest), .{
        .enrichment = .{
            .owner_id = "split-child",
            .dense_embedder = deterministic_child.interface(),
        },
    });
    defer child.close();

    const child_before = try child.stats(alloc);
    defer db_mod.types.freeDBStats(alloc, child_before);
    const child_dense_before = try child.core.loadAppliedSequence(alloc, "dv_v1");

    try child.batch(.{
        .writes = &.{.{ .key = "doc:y", .value = "{\"body\":\"child owns this\"}" }},
        .sync_level = .write,
    });
    try child.runUntilIdle();
    const child_dense_after = try child.core.loadAppliedSequence(alloc, "dv_v1");
    try std.testing.expect(child_dense_after >= child_dense_before + 2);

    const query_vec = try deterministic_child.interface().embedDense(alloc, "", "child owns this", 3);
    defer alloc.free(query_vec);

    var child_result = try child.search(alloc, .{
        .index_name = "dv_v1",
        .dense = .{
            .vector = query_vec,
            .k = 1,
        },
    });
    defer child_result.deinit();
    try std.testing.expect(child_result.total_hits >= 1);
    try std.testing.expectEqualStrings("doc:y", child_result.hits[0].id);

    var parent_result = try db.search(alloc, .{
        .index_name = "dv_v1",
        .dense = .{
            .vector = query_vec,
            .k = 2,
        },
    });
    defer parent_result.deinit();
    for (parent_result.hits) |hit| {
        try std.testing.expect(!std.mem.eql(u8, hit.id, "doc:y"));
    }
}

test "db split cutover fences enrichment to the owning range with durable lsm primary backend" {
    const alloc = std.testing.allocator;

    var path_tmp = try db_mod.TestDirectory.init("db");
    defer path_tmp.cleanup();
    const path = path_tmp.path().ptr;
    defer db_mod.cleanupTempDir(path);

    var dest_tmp = try db_mod.TestDirectory.init("db");
    defer dest_tmp.cleanup();
    const dest = dest_tmp.path().ptr;
    defer db_mod.cleanupTempDir(dest);

    const primary_backend: db_mod.PrimaryBackend = .{ .lsm = db_mod.db_config.primary_lsm_options_default };

    var deterministic_parent = db_mod.embedder_mod.DeterministicDenseEmbedder{};
    var db = try DB.open(alloc, std.mem.span(path), .{
        .primary_backend = primary_backend,
        .enrichment = .{
            .owner_id = "split-parent",
            .dense_embedder = deterministic_parent.interface(),
        },
    });
    defer db.close();

    try db.addIndex(.{
        .name = "dv_v1",
        .kind = .dense_vector,
        .config_json = "{\"field\":\"embedding\",\"dims\":3,\"generator\":{\"kind\":\"dense_embedding\",\"source_field\":\"body\",\"embedding_name\":\"body_dense_v1\"}}",
    });

    try db.batch(.{
        .writes = &.{
            .{ .key = "doc:a", .value = "{\"body\":\"left side\"}" },
            .{ .key = "doc:z", .value = "{\"body\":\"right side\"}" },
        },
        .sync_level = .write,
    });
    try db.enrichment.runtime.?.waitForApplied(1);
    try db.executor.waitForAll(2);

    try db.split(db.getRange(), "doc:m", "", std.mem.span(dest), true);
    try db.finalizeSplit(.{ .start = "", .end = "doc:m" });

    try std.testing.expectError(error.KeyOutOfRange, db.batch(.{
        .writes = &.{.{ .key = "doc:y", .value = "{\"body\":\"parent should reject\"}" }},
        .sync_level = .write,
    }));

    var deterministic_child = db_mod.embedder_mod.DeterministicDenseEmbedder{};
    var child = try DB.open(alloc, std.mem.span(dest), .{
        .primary_backend = primary_backend,
        .enrichment = .{
            .owner_id = "split-child",
            .dense_embedder = deterministic_child.interface(),
        },
    });
    defer child.close();

    const child_before = try child.stats(alloc);
    defer db_mod.types.freeDBStats(alloc, child_before);
    const child_dense_before = try child.core.loadAppliedSequence(alloc, "dv_v1");

    try child.batch(.{
        .writes = &.{.{ .key = "doc:y", .value = "{\"body\":\"child owns this\"}" }},
        .sync_level = .write,
    });
    try child.runUntilIdle();
    const child_dense_after = try child.core.loadAppliedSequence(alloc, "dv_v1");
    try std.testing.expect(child_dense_after >= child_dense_before + 2);

    const query_vec = try deterministic_child.interface().embedDense(alloc, "", "child owns this", 3);
    defer alloc.free(query_vec);

    var child_result = try child.search(alloc, .{
        .index_name = "dv_v1",
        .dense = .{
            .vector = query_vec,
            .k = 1,
        },
    });
    defer child_result.deinit();
    try std.testing.expect(child_result.total_hits >= 1);
    try std.testing.expectEqualStrings("doc:y", child_result.hits[0].id);

    var parent_result = try db.search(alloc, .{
        .index_name = "dv_v1",
        .dense = .{
            .vector = query_vec,
            .k = 2,
        },
    });
    defer parent_result.deinit();
    for (parent_result.hits) |hit| {
        try std.testing.expect(!std.mem.eql(u8, hit.id, "doc:y"));
    }
}

test "db merge-style cutover fences enrichment to the merged receiver range" {
    const alloc = std.testing.allocator;

    var receiver_tmp = try db_mod.TestDirectory.init("db");
    defer receiver_tmp.cleanup();
    const receiver_path = receiver_tmp.path().ptr;
    defer db_mod.cleanupTempDir(receiver_path);

    var donor_tmp = try db_mod.TestDirectory.init("db");
    defer donor_tmp.cleanup();
    const donor_path = donor_tmp.path().ptr;
    defer db_mod.cleanupTempDir(donor_path);

    var deterministic_receiver = db_mod.embedder_mod.DeterministicDenseEmbedder{};
    var receiver = try DB.open(alloc, std.mem.span(receiver_path), .{
        .enrichment = .{
            .owner_id = "merge-receiver",
            .dense_embedder = deterministic_receiver.interface(),
        },
    });
    defer receiver.close();
    try receiver.updateRange(.{ .start = "doc:a", .end = "doc:m" });

    var deterministic_donor = db_mod.embedder_mod.DeterministicDenseEmbedder{};
    var donor = try DB.open(alloc, std.mem.span(donor_path), .{
        .enrichment = .{
            .owner_id = "merge-donor",
            .dense_embedder = deterministic_donor.interface(),
        },
    });
    defer donor.close();
    try donor.updateRange(.{ .start = "doc:m", .end = "" });

    try receiver.addIndex(.{
        .name = "dv_v1",
        .kind = .dense_vector,
        .config_json = "{\"field\":\"embedding\",\"dims\":3,\"generator\":{\"kind\":\"dense_embedding\",\"source_field\":\"body\",\"embedding_name\":\"body_dense_v1\"}}",
    });
    try donor.addIndex(.{
        .name = "dv_v1",
        .kind = .dense_vector,
        .config_json = "{\"field\":\"embedding\",\"dims\":3,\"generator\":{\"kind\":\"dense_embedding\",\"source_field\":\"body\",\"embedding_name\":\"body_dense_v1\"}}",
    });

    try donor.batch(.{
        .writes = &.{.{ .key = "doc:y", .value = "{\"body\":\"donor pre-cutover\"}" }},
        .sync_level = .write,
    });
    try donor.enrichment.runtime.?.waitForApplied(1);
    try donor.executor.waitForAll(2);

    try receiver.updateRange(.{ .start = "doc:a", .end = "" });
    try donor.updateRange(.{ .start = "doc:m", .end = "doc:m" });

    try std.testing.expectError(error.KeyOutOfRange, donor.batch(.{
        .writes = &.{.{ .key = "doc:z", .value = "{\"body\":\"donor should reject\"}" }},
        .sync_level = .write,
    }));

    try receiver.batch(.{
        .writes = &.{.{ .key = "doc:z", .value = "{\"body\":\"receiver merged this\"}" }},
        .sync_level = .write,
    });
    try receiver.enrichment.runtime.?.waitForApplied(1);
    try receiver.executor.waitForAll(2);

    const receiver_vec = try deterministic_receiver.interface().embedDense(alloc, "", "receiver merged this", 3);
    defer alloc.free(receiver_vec);

    var receiver_result = try receiver.search(alloc, .{
        .index_name = "dv_v1",
        .dense = .{
            .vector = receiver_vec,
            .k = 2,
        },
    });
    defer receiver_result.deinit();
    var found_receiver_doc = false;
    for (receiver_result.hits) |hit| {
        if (std.mem.eql(u8, hit.id, "doc:z")) found_receiver_doc = true;
    }
    try std.testing.expect(found_receiver_doc);

    var donor_result = try donor.search(alloc, .{
        .index_name = "dv_v1",
        .dense = .{
            .vector = receiver_vec,
            .k = 2,
        },
    });
    defer donor_result.deinit();
    for (donor_result.hits) |hit| {
        try std.testing.expect(!std.mem.eql(u8, hit.id, "doc:z"));
    }
}

test "db merge-style cutover fences enrichment to the merged receiver range with durable lsm primary backend" {
    const alloc = std.testing.allocator;

    var receiver_tmp = try db_mod.TestDirectory.init("db");
    defer receiver_tmp.cleanup();
    const receiver_path = receiver_tmp.path().ptr;
    defer db_mod.cleanupTempDir(receiver_path);

    var donor_tmp = try db_mod.TestDirectory.init("db");
    defer donor_tmp.cleanup();
    const donor_path = donor_tmp.path().ptr;
    defer db_mod.cleanupTempDir(donor_path);

    const primary_backend: db_mod.PrimaryBackend = .{ .lsm = .{ .flush_threshold = 1 } };

    var deterministic_receiver = db_mod.embedder_mod.DeterministicDenseEmbedder{};
    var receiver = try DB.open(alloc, std.mem.span(receiver_path), .{
        .primary_backend = primary_backend,
        .enrichment = .{
            .owner_id = "merge-receiver",
            .dense_embedder = deterministic_receiver.interface(),
        },
    });
    defer receiver.close();
    try receiver.updateRange(.{ .start = "doc:a", .end = "doc:m" });

    var deterministic_donor = db_mod.embedder_mod.DeterministicDenseEmbedder{};
    var donor = try DB.open(alloc, std.mem.span(donor_path), .{
        .primary_backend = primary_backend,
        .enrichment = .{
            .owner_id = "merge-donor",
            .dense_embedder = deterministic_donor.interface(),
        },
    });
    defer donor.close();
    try donor.updateRange(.{ .start = "doc:m", .end = "" });

    try receiver.addIndex(.{
        .name = "dv_v1",
        .kind = .dense_vector,
        .config_json = "{\"field\":\"embedding\",\"dims\":3,\"generator\":{\"kind\":\"dense_embedding\",\"source_field\":\"body\",\"embedding_name\":\"body_dense_v1\"}}",
    });
    try donor.addIndex(.{
        .name = "dv_v1",
        .kind = .dense_vector,
        .config_json = "{\"field\":\"embedding\",\"dims\":3,\"generator\":{\"kind\":\"dense_embedding\",\"source_field\":\"body\",\"embedding_name\":\"body_dense_v1\"}}",
    });

    try donor.batch(.{
        .writes = &.{.{ .key = "doc:y", .value = "{\"body\":\"donor pre-cutover\"}" }},
        .sync_level = .write,
    });
    try donor.enrichment.runtime.?.waitForApplied(1);
    try donor.executor.waitForAll(2);

    try receiver.updateRange(.{ .start = "doc:a", .end = "" });
    try donor.updateRange(.{ .start = "doc:m", .end = "doc:m" });

    try std.testing.expectError(error.KeyOutOfRange, donor.batch(.{
        .writes = &.{.{ .key = "doc:z", .value = "{\"body\":\"donor should reject\"}" }},
        .sync_level = .write,
    }));

    try receiver.batch(.{
        .writes = &.{.{ .key = "doc:z", .value = "{\"body\":\"receiver merged this\"}" }},
        .sync_level = .write,
    });
    try receiver.enrichment.runtime.?.waitForApplied(1);
    try receiver.executor.waitForAll(2);

    const receiver_vec = try deterministic_receiver.interface().embedDense(alloc, "", "receiver merged this", 3);
    defer alloc.free(receiver_vec);

    var receiver_result = try receiver.search(alloc, .{
        .index_name = "dv_v1",
        .dense = .{
            .vector = receiver_vec,
            .k = 2,
        },
    });
    defer receiver_result.deinit();
    var found_receiver_doc = false;
    for (receiver_result.hits) |hit| {
        if (std.mem.eql(u8, hit.id, "doc:z")) found_receiver_doc = true;
    }
    try std.testing.expect(found_receiver_doc);

    var donor_result = try donor.search(alloc, .{
        .index_name = "dv_v1",
        .dense = .{
            .vector = receiver_vec,
            .k = 2,
        },
    });
    defer donor_result.deinit();
    for (donor_result.hits) |hit| {
        try std.testing.expect(!std.mem.eql(u8, hit.id, "doc:z"));
    }
}

test "db direct merge import preserves graph source lifetime across generations" {
    const alloc = std.testing.allocator;
    var donor_tmp = try db_mod.TestDirectory.init("db-graph-source-direct-donor");
    defer donor_tmp.cleanup();
    var receiver_tmp = try db_mod.TestDirectory.init("db-graph-source-direct-receiver");
    defer receiver_tmp.cleanup();
    var donor = try DB.open(alloc, donor_tmp.path(), .{ .start_optional_runtimes = false });
    defer donor.close();
    var receiver = try DB.open(alloc, receiver_tmp.path(), .{ .start_optional_runtimes = false });
    defer receiver.close();
    const enrichment: db_mod.types.EnrichmentConfig = .{ .name = "relations_v1", .kind = .asset, .field = "relations", .content_type = "application/json" };
    const index: db_mod.types.IndexConfig = .{ .name = "relations_graph", .kind = .graph, .config_json =
        \\{"ttl":{"duration":"1h"},"sources":[{"artifact":"relations_v1"}]}
    };
    try donor.enrichment.addEnrichment(enrichment);
    try donor.addIndex(index);
    try receiver.enrichment.addEnrichment(enrichment);
    try receiver.addIndex(index);
    try donor.batch(.{ .writes = &.{.{ .key = "doc:a", .value =
        \\{"title":"owner","relations":{"type":"mentions","target":{"document_id":"doc:b"},"weight":2}}
    }}, .sync_level = .enrichments });
    try donor.runUntilIdle();
    const primary = (try donor.get(alloc, "doc:a")) orelse return error.TestExpectedDocument;
    defer alloc.free(primary);
    try receiver.batch(.{ .writes = &.{.{ .key = "doc:a", .value = primary }}, .sync_level = .enrichments });
    const donor_due = try donor.core.store.scanPrefix(alloc, &db_mod.internal_keys.graph_edge_expiration_index_prefix);
    defer db_mod.docstore_mod.DocStore.freeResults(alloc, donor_due);
    try std.testing.expectEqual(@as(usize, 1), donor_due.len);
    const donor_deadline = (try db_mod.graph_edge_ttl_expiration.decodeDue(donor_due[0].value)).source.deadline_ns;
    const pre_import_candidate = (try db_mod.graph_edge_ttl_expiration.decodeDue(donor_due[0].value)).source;
    const donor_lifetime_key = try db_mod.internal_keys.graphEdgeTtlLifetimeKeyAlloc(alloc, pre_import_candidate.edge_key, pre_import_candidate.index_name, pre_import_candidate.generation, pre_import_candidate.state_key);
    defer alloc.free(donor_lifetime_key);
    const donor_lifetime = try donor.core.store.get(alloc, donor_lifetime_key);
    defer alloc.free(donor_lifetime);
    try std.testing.expectEqual(donor_deadline - std.time.ns_per_hour, std.mem.readInt(u64, donor_lifetime[0..8], .big));
    try receiver.importMergeRangeFromTransitionDonor(&donor, .{ .start = "doc:a", .end = "doc:z" });
    const donor_after_import = try donor.core.store.scanPrefix(alloc, &db_mod.internal_keys.graph_edge_expiration_index_prefix);
    defer db_mod.docstore_mod.DocStore.freeResults(alloc, donor_after_import);
    try std.testing.expectEqual(@as(usize, 1), donor_after_import.len);
    try std.testing.expectEqual(donor_deadline, (try db_mod.graph_edge_ttl_expiration.decodeDue(donor_after_import[0].value)).source.deadline_ns);
    const receiver_due = try receiver.core.store.scanPrefix(alloc, &db_mod.internal_keys.graph_edge_expiration_index_prefix);
    defer db_mod.docstore_mod.DocStore.freeResults(alloc, receiver_due);
    try std.testing.expectEqual(@as(usize, 1), receiver_due.len);
    const candidate = (try db_mod.graph_edge_ttl_expiration.decodeDue(receiver_due[0].value)).source;
    const imported_lifetime_key = try db_mod.internal_keys.graphEdgeTtlLifetimeKeyAlloc(alloc, candidate.edge_key, candidate.index_name, candidate.generation, candidate.state_key);
    defer alloc.free(imported_lifetime_key);
    const imported_lifetime = try receiver.core.store.get(alloc, imported_lifetime_key);
    defer alloc.free(imported_lifetime);
    try std.testing.expectEqual(donor_deadline - std.time.ns_per_hour, std.mem.readInt(u64, imported_lifetime[0..8], .big));
    try std.testing.expectEqual(donor_deadline, candidate.deadline_ns);
    try std.testing.expectEqual(receiver.core.index_manager.graphIndex("relations_graph").?.config.coverage_generation, candidate.generation);
    const donor_candidate = (try db_mod.graph_edge_ttl_expiration.decodeDue(donor_due[0].value)).source;
    var manual_clock = db_mod.platform_clock.ManualClock{};
    manual_clock.setRealtimeNs(donor_candidate.deadline_ns + 1);
    var ttl_ctx = db_mod.TtlCleanupContext{ .batch = donor.batchContext(), .grace_period_ns = 0, .clock = manual_clock.clock() };
    try std.testing.expect(try db_mod.expireGraphTtlCandidateContext(&ttl_ctx, donor_candidate));
    try receiver.importMergeRangeFromTransitionDonor(&donor, .{ .start = "doc:a", .end = "doc:z" });
    const after_expiry = try receiver.core.store.scanPrefix(alloc, &db_mod.internal_keys.graph_edge_expiration_index_prefix);
    defer db_mod.docstore_mod.DocStore.freeResults(alloc, after_expiry);
    try std.testing.expectEqual(@as(usize, 0), after_expiry.len);
    const donor_tombstone = try db_mod.internal_keys.graphEdgeTtlTombstoneKeyAlloc(alloc, donor_candidate.edge_key, donor_candidate.index_name, donor_candidate.generation, donor_candidate.state_key);
    defer alloc.free(donor_tombstone);
    const receiver_tombstone = try db_mod.internal_keys.rebindGraphEdgeTtlStateKeyGenerationAlloc(alloc, donor_tombstone, candidate.generation);
    defer alloc.free(receiver_tombstone);
    const tombstone_raw = try receiver.core.store.get(alloc, receiver_tombstone);
    defer alloc.free(tombstone_raw);
    try std.testing.expectError(error.NotFound, receiver.core.store.get(alloc, candidate.edge_key));
}

test "db direct merge import retires replaced owners and preserves shared entity contributors" {
    const alloc = std.testing.allocator;
    for ([_]bool{ false, true }) |timed| {
        for ([_]bool{ false, true }) |sourced| {
            var donor_tmp = try db_mod.TestDirectory.init("db-merge-owner-donor");
            defer donor_tmp.cleanup();
            var receiver_tmp = try db_mod.TestDirectory.init("db-merge-owner-receiver");
            defer receiver_tmp.cleanup();
            const config_json = if (timed)
                (if (sourced) "{\"ttl\":{\"duration\":\"1h\"},\"sources\":[{\"artifact\":\"relations_v1\"}]}" else "{\"ttl\":{\"duration\":\"1h\"}}")
            else
                (if (sourced) "{\"sources\":[{\"artifact\":\"relations_v1\"}]}" else "{}");
            const Check = struct {
                fn edges(db: *DB) !void {
                    // Test source identities both inside and outside the
                    // imported document range, in both adjacency directions.
                    for ([_][]const u8{ "person/ada", "doc:b" }) |source| {
                        const outgoing = try db.getEdges(std.testing.allocator, "g", source, "links", .out);
                        defer db_mod.graph_mod.GraphIndex.freeEdges(std.testing.allocator, outgoing);
                        try std.testing.expectEqual(@as(usize, 2), outgoing.len);
                        var shared = false;
                        var replacement = false;
                        for (outgoing) |edge| {
                            if (std.mem.eql(u8, edge.target, "target/shared")) {
                                try std.testing.expectEqual(@as(f64, 7), edge.weight);
                                shared = true;
                            } else if (std.mem.eql(u8, edge.target, "target/new")) {
                                try std.testing.expectEqual(@as(f64, 4), edge.weight);
                                replacement = true;
                            } else return error.TestUnexpectedResult;
                        }
                        try std.testing.expect(shared and replacement);
                    }
                    const obsolete = try db.getEdges(std.testing.allocator, "g", "target/old", "links", .in);
                    defer db_mod.graph_mod.GraphIndex.freeEdges(std.testing.allocator, obsolete);
                    try std.testing.expectEqual(@as(usize, 0), obsolete.len);
                    const shared = try db.getEdges(std.testing.allocator, "g", "target/shared", "links", .in);
                    defer db_mod.graph_mod.GraphIndex.freeEdges(std.testing.allocator, shared);
                    try std.testing.expectEqual(@as(usize, 2), shared.len);
                    for (shared) |edge| try std.testing.expectEqual(@as(f64, 7), edge.weight);
                }
            };
            {
                var donor = try DB.open(alloc, donor_tmp.path(), .{ .start_optional_runtimes = false });
                defer donor.close();
                var receiver = try DB.open(alloc, receiver_tmp.path(), .{ .start_optional_runtimes = false });
                defer receiver.close();
                for ([_]*DB{ &donor, &receiver }) |db| {
                    if (sourced) try db.enrichment.addEnrichment(.{ .name = "relations_v1", .kind = .asset, .field = "relations", .content_type = "application/json" });
                    try db.addIndex(.{ .name = "g", .kind = .graph, .config_json = config_json });
                    try db.batch(.{ .writes = &.{ .{ .key = "doc:0", .value = "{}" }, .{ .key = "doc:a", .value = "{}" }, .{ .key = "doc:c", .value = "{}" } }, .sync_level = .full_index });
                }
                for ([_][]const u8{ "person/ada", "doc:b" }) |source| {
                    try receiver.batch(.{ .graph_writes = &.{
                        .{ .index_name = "g", .source = source, .owner = "doc:a", .target = "target/old", .edge_type = "links" },
                        .{ .index_name = "g", .source = source, .owner = "doc:a", .target = "target/shared", .edge_type = "links", .weight = 1 },
                        .{ .index_name = "g", .source = source, .owner = "doc:0", .target = "target/shared", .edge_type = "links", .weight = 7 },
                    }, .sync_level = .full_index });
                    try donor.batch(.{ .graph_writes = &.{.{ .index_name = "g", .source = source, .owner = "doc:c", .target = "target/new", .edge_type = "links", .weight = 4 }}, .sync_level = .full_index });
                }
                const retained_key = try db_mod.internal_keys.graphEdgeArtifactKeyWithSourceAlloc(alloc, "doc:0", "g", "links", "target/shared", "person/ada");
                defer alloc.free(retained_key);
                const retained_before = try receiver.core.store.get(alloc, retained_key);
                defer alloc.free(retained_before);
                for (0..2) |_| {
                    try receiver.importMergeRangeFromTransitionDonor(&donor, .{ .start = "doc:a", .end = "doc:z" });
                    try Check.edges(&receiver);
                    try std.testing.expectError(error.NotFound, receiver.core.store.get(alloc, db_mod.graph_merge_import_recovery_key));
                    const retained_after = try receiver.core.store.get(alloc, retained_key);
                    defer alloc.free(retained_after);
                    try std.testing.expectEqualStrings(retained_before, retained_after);
                    const due = try receiver.core.store.scanPrefix(alloc, &db_mod.internal_keys.graph_edge_expiration_index_prefix);
                    defer db_mod.docstore_mod.DocStore.freeResults(alloc, due);
                    try std.testing.expectEqual(@as(usize, if (timed) 4 else 0), due.len);
                }
            }
            {
                var readonly = try DB.open(alloc, receiver_tmp.path(), .{ .open_mode = .query_readonly });
                defer readonly.close();
                try Check.edges(&readonly);
            }
            var reopened = try DB.open(alloc, receiver_tmp.path(), .{ .start_optional_runtimes = false });
            defer reopened.close();
            try Check.edges(&reopened);
        }
    }
}

test "db direct merge import reconciles pending source inputs before snapshot coverage" {
    const alloc = std.testing.allocator;
    for ([_]bool{ false, true }) |timed| {
        for ([_]bool{ false, true }) |interrupted| {
            var donor_tmp = try db_mod.TestDirectory.init("db-merge-source-debt-donor");
            defer donor_tmp.cleanup();
            var receiver_tmp = try db_mod.TestDirectory.init("db-merge-source-debt-receiver");
            defer receiver_tmp.cleanup();
            const Check = struct {
                fn edges(db: *DB) !void {
                    for ([_][]const u8{ "doc:0", "doc:1", "doc:2", "doc:a" }, [_]?[]const u8{ "target/updated", null, "target/added", "target/donor" }) |owner, expected| {
                        const out = try db.getEdges(std.testing.allocator, "g", owner, "links", .out);
                        defer db_mod.graph_mod.GraphIndex.freeEdges(std.testing.allocator, out);
                        try std.testing.expectEqual(@as(usize, if (expected != null) 1 else 0), out.len);
                        if (expected) |target| try std.testing.expectEqualStrings(target, out[0].target);
                    }
                }
            };
            {
                var donor = try DB.open(alloc, donor_tmp.path(), .{ .start_optional_runtimes = false });
                defer donor.close();
                var receiver = try DB.open(alloc, receiver_tmp.path(), .{ .start_optional_runtimes = false });
                defer receiver.close();
                for ([_]*DB{ &donor, &receiver }) |db| {
                    try db.enrichment.addEnrichment(.{ .name = "relations_v1", .kind = .asset, .field = "relations", .content_type = "application/json" });
                    try db.addIndex(.{ .name = "g", .kind = .graph, .config_json = if (timed) "{\"ttl\":{\"duration\":\"1h\"},\"sources\":[{\"artifact\":\"relations_v1\"}]}" else "{\"sources\":[{\"artifact\":\"relations_v1\"}]}" });
                }
                try receiver.batch(.{ .writes = &.{
                    .{ .key = "doc:0", .value = "{\"relations\":{\"type\":\"links\",\"target\":{\"document_id\":\"target/old\"}}}" },
                    .{ .key = "doc:1", .value = "{\"relations\":{\"type\":\"links\",\"target\":{\"document_id\":\"target/withdrawn\"}}}" },
                    .{ .key = "doc:2", .value = "{}" },
                }, .sync_level = .enrichments });
                try receiver.runUntilIdle();
                try donor.batch(.{ .writes = &.{.{ .key = "doc:a", .value = "{}" }}, .sync_level = .full_index });
                // Commit source inputs and replay debt without running their
                // graph worker. Include donor debt as well as retained owners.
                for ([_][]const u8{ "doc:0", "doc:1", "doc:2", "doc:a" }, [_]?[]const u8{
                    "{\"type\":\"links\",\"target\":{\"document_id\":\"target/updated\"}}",
                    null,
                    "{\"type\":\"links\",\"target\":{\"document_id\":\"target/added\"}}",
                    "{\"type\":\"links\",\"target\":{\"document_id\":\"target/donor\"}}",
                }) |owner, raw| {
                    const db = if (std.mem.eql(u8, owner, "doc:a")) &donor else &receiver;
                    const key = try db_mod.artifact_ids.internalKeyForArtifactRefAlloc(alloc, .{ .document_id = @constCast(owner), .name = @constCast("relations_v1"), .kind = .asset });
                    defer alloc.free(key);
                    if (raw) |value| try db.core.store.put(key, value) else try db.core.store.delete(key);
                    _ = try db_mod.appendDerivedBatchRecord(db, .{ .changed_artifact_keys = &.{key} });
                }
                if (interrupted) {
                    db_mod.test_graph_merge_import_abort_after_primary.store(true, .monotonic);
                    defer db_mod.test_graph_merge_import_abort_after_primary.store(false, .monotonic);
                    try std.testing.expectError(error.TestInjectedGraphMergeImportFailure, receiver.importMergeRangeFromTransitionDonor(&donor, .{ .start = "doc:a", .end = "doc:z" }));
                    // Recovery must use its durable worklist even after source
                    // replay entries disappear and the donor handle is gone.
                    try receiver.core.store.truncateReplayUpTo(alloc, receiver.core.store.nextReplaySequence(1) -| 1);
                } else {
                    try receiver.importMergeRangeFromTransitionDonor(&donor, .{ .start = "doc:a", .end = "doc:z" });
                    try Check.edges(&receiver);
                    const retained = try db_mod.internal_keys.graphEdgeArtifactKeyAlloc(alloc, "doc:a", "g", "links", "target/donor");
                    defer alloc.free(retained);
                    const before = try receiver.core.store.get(alloc, retained);
                    defer alloc.free(before);
                    try receiver.importMergeRangeFromTransitionDonor(&donor, .{ .start = "doc:a", .end = "doc:z" });
                    try Check.edges(&receiver);
                    const after = try receiver.core.store.get(alloc, retained);
                    defer alloc.free(after);
                    try std.testing.expectEqualStrings(before, after);
                }
            }
            {
                var reopened = try DB.open(alloc, receiver_tmp.path(), .{ .start_optional_runtimes = false });
                defer reopened.close();
                try Check.edges(&reopened);
                const due = try reopened.core.store.scanPrefix(alloc, &db_mod.internal_keys.graph_edge_expiration_index_prefix);
                defer db_mod.docstore_mod.DocStore.freeResults(alloc, due);
                try std.testing.expectEqual(@as(usize, if (timed) 3 else 0), due.len);
            }
            var readonly = try DB.open(alloc, receiver_tmp.path(), .{ .open_mode = .query_readonly });
            defer readonly.close();
            try Check.edges(&readonly);
        }
    }
}

test "db direct merge import and paged export reject donors awaiting source recovery" {
    const alloc = std.testing.allocator;
    for ([_]bool{ false, true }) |timed| {
        var upstream_tmp = try db_mod.TestDirectory.init("db-merge-fenced-upstream");
        defer upstream_tmp.cleanup();
        var donor_tmp = try db_mod.TestDirectory.init("db-merge-fenced-donor");
        defer donor_tmp.cleanup();
        var destination_tmp = try db_mod.TestDirectory.init("db-merge-fenced-destination");
        defer destination_tmp.cleanup();
        var destination = try DB.open(alloc, destination_tmp.path(), .{ .start_optional_runtimes = false });
        defer destination.close();
        const Setup = struct {
            fn graph(db: *DB, has_ttl: bool) !void {
                try db.enrichment.addEnrichment(.{ .name = "relations_v1", .kind = .asset, .field = "relations", .content_type = "application/json" });
                try db.addIndex(.{ .name = "g", .kind = .graph, .config_json = if (has_ttl) "{\"ttl\":{\"duration\":\"1h\"},\"sources\":[{\"artifact\":\"relations_v1\"}]}" else "{\"sources\":[{\"artifact\":\"relations_v1\"}]}" });
            }
            fn paused(db: *DB, expected: bool) !void {
                db.core.index_manager.catalog_mutex.lockShared();
                defer db.core.index_manager.catalog_mutex.unlockShared();
                try std.testing.expectEqual(expected, db.core.index_manager.graph_artifact_rebuild_pending);
            }
        };
        try Setup.graph(&destination, timed);
        try destination.batch(.{ .graph_writes = &.{.{ .index_name = "g", .source = "doc:0", .target = "target/destination", .edge_type = "links" }}, .sync_level = .full_index });
        const moved: db_mod.types.ByteRange = .{ .start = "doc:a", .end = "doc:z" };
        const exported: db_mod.types.ByteRange = .{ .start = "doc:0", .end = "doc:z" };
        const updated_key = try db_mod.internal_keys.graphEdgeArtifactKeyAlloc(alloc, "doc:0", "g", "links", "target/updated");
        defer alloc.free(updated_key);
        {
            var upstream = try DB.open(alloc, upstream_tmp.path(), .{ .start_optional_runtimes = false });
            defer upstream.close();
            var donor = try DB.open(alloc, donor_tmp.path(), .{ .start_optional_runtimes = false });
            defer donor.close();
            try Setup.graph(&upstream, timed);
            try Setup.graph(&donor, timed);
            try donor.batch(.{ .writes = &.{.{ .key = "doc:0", .value = "{\"relations\":{\"type\":\"links\",\"target\":{\"document_id\":\"target/old\"}}}" }}, .sync_level = .enrichments });
            try donor.runUntilIdle();
            // Keep a real continuation from before failure. Each new page must
            // revalidate admission even if an earlier page was admitted.
            const initial_page = try donor.mergeArtifactsPage(alloc, exported, null);
            defer {
                for (initial_page) |row| {
                    alloc.free(row.key);
                    alloc.free(row.value);
                }
                alloc.free(initial_page);
            }
            try std.testing.expect(initial_page.len > 0);
            const source_key = try db_mod.artifact_ids.internalKeyForArtifactRefAlloc(alloc, .{ .document_id = @constCast("doc:0"), .name = @constCast("relations_v1"), .kind = .asset });
            defer alloc.free(source_key);
            try donor.core.store.put(source_key, "{\"type\":\"links\",\"target\":{\"document_id\":\"target/updated\"}}");
            _ = try db_mod.appendDerivedBatchRecord(&donor, .{ .changed_artifact_keys = &.{source_key} });
            db_mod.test_graph_merge_import_abort_after_primary.store(true, .monotonic);
            defer db_mod.test_graph_merge_import_abort_after_primary.store(false, .monotonic);
            try std.testing.expectError(error.TestInjectedGraphMergeImportFailure, donor.importMergeRangeFromTransitionDonor(&upstream, moved));
            try donor.core.store.truncateReplayUpTo(alloc, donor.core.store.nextReplaySequence(1) -| 1);
            try std.testing.expectError(error.NotFound, donor.core.store.get(alloc, updated_key));
            const recovery_before = try donor.core.store.get(alloc, db_mod.graph_merge_import_recovery_key);
            defer alloc.free(recovery_before);
            const record = try std.json.parseFromSlice(db_mod.GraphMergeImportRecovery, alloc, recovery_before, .{ .allocate = .alloc_always });
            defer record.deinit();
            try std.testing.expectEqual(@as(usize, 1), record.value.source_artifact_keys.len);
            try std.testing.expectEqualStrings(source_key, record.value.source_artifact_keys[0]);
            const destination_sequence = destination.core.store.nextReplaySequence(1);
            try Setup.paused(&donor, true);
            try std.testing.expectError(error.GraphMaintenanceInProgress, donor.mergeArtifactsPage(alloc, exported, null));
            try std.testing.expectError(error.GraphMaintenanceInProgress, donor.mergeArtifactsPage(alloc, exported, initial_page[initial_page.len - 1].key));
            try std.testing.expectError(error.GraphMaintenanceInProgress, destination.importMergeRangeFromTransitionDonor(&donor, exported));
            try Setup.paused(&donor, true);
            try Setup.paused(&destination, false);
            try std.testing.expectEqual(destination_sequence, destination.core.store.nextReplaySequence(1));
            try std.testing.expectError(error.NotFound, destination.core.store.get(alloc, db_mod.graph_merge_import_recovery_key));
            const recovery_after = try donor.core.store.get(alloc, db_mod.graph_merge_import_recovery_key);
            defer alloc.free(recovery_after);
            try std.testing.expectEqualStrings(recovery_before, recovery_after);
            const unchanged = try destination.getEdges(alloc, "g", "doc:0", "links", .out);
            defer db_mod.graph_mod.GraphIndex.freeEdges(alloc, unchanged);
            try std.testing.expectEqual(@as(usize, 1), unchanged.len);
            try std.testing.expectEqualStrings("target/destination", unchanged[0].target);
        }
        // The donor's own writable recovery reconciles the saved worklist.
        // Admission resumes only after that publication, without journal debt.
        var recovered = try DB.open(alloc, donor_tmp.path(), .{ .start_optional_runtimes = false });
        defer recovered.close();
        try Setup.paused(&recovered, false);
        const page = try recovered.mergeArtifactsPage(alloc, exported, null);
        defer {
            for (page) |row| {
                alloc.free(row.key);
                alloc.free(row.value);
            }
            alloc.free(page);
        }
        var found_updated = false;
        for (page) |row| if (std.mem.eql(u8, row.key, updated_key)) {
            found_updated = true;
        };
        try std.testing.expect(found_updated);
        try destination.importMergeRangeFromTransitionDonor(&recovered, exported);
        const updated = try destination.getEdges(alloc, "g", "doc:0", "links", .out);
        defer db_mod.graph_mod.GraphIndex.freeEdges(alloc, updated);
        try std.testing.expectEqual(@as(usize, 1), updated.len);
        try std.testing.expectEqualStrings("target/updated", updated[0].target);
        const due = try destination.core.store.scanPrefix(alloc, &db_mod.internal_keys.graph_edge_expiration_index_prefix);
        defer db_mod.docstore_mod.DocStore.freeResults(alloc, due);
        try std.testing.expectEqual(@as(usize, if (timed) 1 else 0), due.len);
    }
}

test "db direct merge import recovers interrupted primary and projection publication before reads" {
    const alloc = std.testing.allocator;
    for ([_]bool{ false, true }) |timed| {
        const Failure = enum { primary, projection, partial_clear };
        for ([_]Failure{ .primary, .projection, .partial_clear }) |failure| {
            const during_projection = failure == .projection;
            var donor_tmp = try db_mod.TestDirectory.init("db-merge-recovery-donor");
            defer donor_tmp.cleanup();
            var receiver_tmp = try db_mod.TestDirectory.init("db-merge-recovery-receiver");
            defer receiver_tmp.cleanup();
            const config: db_mod.types.IndexConfig = .{ .name = "g", .kind = .graph, .config_json = if (timed)
                "{\"ttl\":{\"duration\":\"1h\"},\"sources\":[{\"artifact\":\"relations_v1\"}],\"metrics\":{\"degree\":{\"enabled\":true,\"kind\":\"degree\",\"refresh\":\"manual\"}}}"
            else
                "{\"sources\":[{\"artifact\":\"relations_v1\"}],\"metrics\":{\"degree\":{\"enabled\":true,\"kind\":\"degree\",\"refresh\":\"manual\"}}}" };
            var replay_floor: u64 = 0;
            {
                var donor = try DB.open(alloc, donor_tmp.path(), .{ .start_optional_runtimes = false });
                defer donor.close();
                var receiver = try DB.open(alloc, receiver_tmp.path(), .{ .start_optional_runtimes = false });
                defer receiver.close();
                for ([_]*DB{ &donor, &receiver }) |db| {
                    try db.enrichment.addEnrichment(.{ .name = "relations_v1", .kind = .asset, .field = "relations", .content_type = "application/json" });
                    try db.addIndex(config);
                    try db.batch(.{ .writes = &.{ .{ .key = "doc:a", .value = "{}" }, .{ .key = "doc:c", .value = "{}" }, .{ .key = "doc:0", .value = "{}" } }, .sync_level = .full_index });
                }
                try donor.batch(.{ .graph_writes = &.{.{ .index_name = "g", .source = "doc:a", .target = "doc:b", .edge_type = "mentions", .weight = 4 }}, .sync_level = .full_index });
                try donor.batch(.{ .writes = &.{.{ .key = "doc:c", .value = "{\"relations\":{\"type\":\"mentions\",\"target\":{\"document_id\":\"doc:d\"},\"weight\":2}}" }}, .sync_level = .enrichments });
                try donor.runUntilIdle();
                try receiver.batch(.{ .graph_writes = &.{
                    .{ .index_name = "g", .source = "doc:a", .target = "doc:old", .edge_type = "mentions" },
                    .{ .index_name = "g", .source = "doc:c", .target = "doc:old", .edge_type = "mentions" },
                    .{ .index_name = "g", .source = "doc:0", .target = "doc:retained", .edge_type = "mentions" },
                }, .sync_level = .full_index });
                replay_floor = receiver.core.store.nextReplaySequence(1) -| 1;
                try receiver.core.saveAppliedSequence("g", 0);
                var existing_reader = try DB.open(alloc, receiver_tmp.path(), .{ .open_mode = .query_readonly });
                defer existing_reader.close();
                db_mod.test_graph_merge_import_abort_after_primary.store(failure == .primary, .monotonic);
                defer db_mod.test_graph_merge_import_abort_after_primary.store(false, .monotonic);
                db_mod.test_graph_merge_import_abort_during_projection.store(during_projection, .monotonic);
                defer db_mod.test_graph_merge_import_abort_during_projection.store(false, .monotonic);
                db_mod.graph_mod.test_artifact_rebuild_abort_after_outgoing.store(failure == .partial_clear, .monotonic);
                defer db_mod.graph_mod.test_artifact_rebuild_abort_after_outgoing.store(false, .monotonic);
                try std.testing.expectError(if (failure == .partial_clear) error.TestInjectedGraphArtifactRebuildFailure else error.TestInjectedGraphMergeImportFailure, receiver.importMergeRangeFromTransitionDonor(&donor, .{ .start = "doc:a", .end = "doc:z" }));
                if (failure != .primary) {
                    // Even an interrupted clear leaves live backend handles
                    // safe to sync/close, and scheduler work stays suspended.
                    try receiver.core.index_manager.syncGraphIndexes(true);
                    const worker = try receiver.core.index_manager.runGraphMetricPlannedWorkerSweep(.{ .worker_id = "recovery-test" });
                    try std.testing.expectEqual(@as(usize, 0), worker.worker_steps);
                    const coordinator = try receiver.core.index_manager.runGraphMetricPlannedCoordinatorSweep(.{});
                    try std.testing.expectEqual(@as(usize, 0), coordinator.metrics_scanned);
                }
                try std.testing.expectError(error.GraphMaintenanceInProgress, receiver.getEdges(alloc, "g", "doc:a", "mentions", .out));
                // A reader can retain its coherent pre-import snapshot. If
                // its primary view observes publication, it must be fenced.
                const pinned: ?[]db_mod.graph_mod.Edge = existing_reader.getEdges(alloc, "g", "doc:a", "mentions", .out) catch |err| switch (err) {
                    error.GraphMaintenanceInProgress => null,
                    else => return err,
                };
                if (pinned) |edges| {
                    defer db_mod.graph_mod.GraphIndex.freeEdges(alloc, edges);
                    try std.testing.expectEqual(@as(usize, 1), edges.len);
                    try std.testing.expectEqualStrings("doc:old", edges[0].target);
                }
                try std.testing.expectError(error.GraphMaintenanceInProgress, receiver.batch(.{ .writes = &.{.{ .key = "doc:a", .value = "{\"title\":\"blocked\"}" }} }));
            }
            try std.testing.expectError(error.GraphMaintenanceInProgress, DB.open(alloc, receiver_tmp.path(), .{ .open_mode = .query_readonly }));
            if (!timed and failure == .primary) {
                // Recovery itself may crash after resetting private stores.
                // The original primary snapshot and fence remain authoritative.
                db_mod.test_graph_merge_import_abort_during_recovery.store(true, .monotonic);
                defer db_mod.test_graph_merge_import_abort_during_recovery.store(false, .monotonic);
                try std.testing.expectError(error.TestInjectedGraphMergeImportFailure, DB.open(alloc, receiver_tmp.path(), .{ .start_optional_runtimes = false }));
                try std.testing.expectError(error.GraphMaintenanceInProgress, DB.open(alloc, receiver_tmp.path(), .{ .open_mode = .query_readonly }));
            }
            {
                // Recovery has no donor handle or donor files to consult.
                var recovered = try DB.open(alloc, receiver_tmp.path(), .{ .start_optional_runtimes = false });
                defer recovered.close();
                try std.testing.expectError(error.NotFound, recovered.core.store.get(alloc, db_mod.graph_merge_import_recovery_key));
                try std.testing.expect((try recovered.core.loadAppliedSequence(alloc, "g")) >= replay_floor);
                const metric_status = try recovered.core.index_manager.graphIndex("g").?.index.graphMetricSchedulerStatus("degree", null);
                try std.testing.expect(metric_status.state == .not_ready);
                for ([_][]const u8{ "doc:a", "doc:c", "doc:0" }, [_][]const u8{ "doc:b", "doc:d", "doc:retained" }) |source, target| {
                    const edges = try recovered.getEdges(alloc, "g", source, "mentions", .out);
                    defer db_mod.graph_mod.GraphIndex.freeEdges(alloc, edges);
                    try std.testing.expectEqual(@as(usize, 1), edges.len);
                    try std.testing.expectEqualStrings(target, edges[0].target);
                }
                const due = try recovered.core.store.scanPrefix(alloc, &db_mod.internal_keys.graph_edge_expiration_index_prefix);
                defer db_mod.docstore_mod.DocStore.freeResults(alloc, due);
                try std.testing.expectEqual(@as(usize, if (timed) 3 else 0), due.len);
                try recovered.batch(.{ .writes = &.{.{ .key = "doc:c", .value = "{\"relations\":{\"type\":\"mentions\",\"target\":{\"document_id\":\"doc:d\"},\"weight\":5}}" }}, .sync_level = .enrichments });
                try recovered.runUntilIdle();
                const revised = try recovered.getEdges(alloc, "g", "doc:c", "mentions", .out);
                defer db_mod.graph_mod.GraphIndex.freeEdges(alloc, revised);
                try std.testing.expectEqual(@as(usize, 1), revised.len);
                try std.testing.expectEqual(@as(f64, 5), revised[0].weight);
            }
            var readonly = try DB.open(alloc, receiver_tmp.path(), .{ .open_mode = .query_readonly });
            defer readonly.close();
            const visible = try readonly.getEdges(alloc, "g", "doc:a", "mentions", .out);
            defer db_mod.graph_mod.GraphIndex.freeEdges(alloc, visible);
            try std.testing.expectEqualStrings("doc:b", visible[0].target);
        }
    }
}

test "db direct merge import replaces contributor membership and enforces visible edge limits" {
    const alloc = std.testing.allocator;
    var donor_tmp = try db_mod.TestDirectory.init("db-merge-members-donor");
    defer donor_tmp.cleanup();
    var receiver_tmp = try db_mod.TestDirectory.init("db-merge-members-receiver");
    defer receiver_tmp.cleanup();
    var donor = try DB.open(alloc, donor_tmp.path(), .{ .start_optional_runtimes = false });
    defer donor.close();
    var receiver = try DB.open(alloc, receiver_tmp.path(), .{ .start_optional_runtimes = false });
    defer receiver.close();
    for ([_]*DB{ &donor, &receiver }) |db| {
        try db.enrichment.addEnrichment(.{ .name = "relations_v1", .kind = .asset, .field = "relations", .content_type = "application/json" });
        try db.addIndex(.{ .name = "g", .kind = .graph, .config_json = "{\"max_edges_per_document\":1,\"sources\":[{\"artifact\":\"relations_v1\"}]}" });
        try db.batch(.{ .writes = &.{.{ .key = "doc:a", .value = "{}" }}, .sync_level = .full_index });
    }
    try receiver.batch(.{ .graph_writes = &.{.{ .index_name = "g", .source = "doc:a", .target = "doc:stale", .edge_type = "mentions" }}, .sync_level = .full_index });
    try donor.batch(.{ .graph_writes = &.{.{ .index_name = "g", .source = "doc:a", .target = "doc:b", .edge_type = "mentions", .weight = 4 }}, .sync_level = .full_index });
    try donor.batch(.{ .writes = &.{.{ .key = "doc:a", .value = "{\"relations\":{\"type\":\"mentions\",\"target\":{\"document_id\":\"doc:b\"},\"weight\":2}}" }}, .sync_level = .enrichments });
    try donor.runUntilIdle();
    const generation = receiver.core.index_manager.graphIndex("g").?.config.coverage_generation;
    const count_key = try db_mod.internal_keys.graphEdgeContenderCountKeyAlloc(alloc, "doc:a", "g");
    defer alloc.free(count_key);
    const prefix = try db_mod.internal_keys.graphEdgeContenderIndexPrefixAlloc(alloc, "doc:a", "g");
    defer alloc.free(prefix);
    var source_state: ?[]u8 = null;
    defer if (source_state) |key| alloc.free(key);
    for (0..2) |_| {
        try receiver.importMergeRangeFromTransitionDonor(&donor, .{ .start = "doc:a", .end = "doc:z" });
        const raw_count = try receiver.core.store.get(alloc, count_key);
        defer alloc.free(raw_count);
        try std.testing.expectEqual(@as(?usize, 1), try db_mod.graph_edge_contender.decodeVisibleCount(raw_count, generation));
        const members = try receiver.core.store.scanPrefix(alloc, prefix);
        defer db_mod.docstore_mod.DocStore.freeResults(alloc, members);
        try std.testing.expectEqual(@as(usize, 3), members.len); // count plus two contributors
        for (members) |member| {
            if (std.mem.eql(u8, member.key, count_key)) continue;
            const view = (try db_mod.graph_edge_contender.decode(member.value, generation)) orelse return error.TestUnexpectedResult;
            try std.testing.expectEqual(@as(usize, 0), view.payload.len);
            if (view.source_priority != db_mod.graph_mod.direct_source_priority and source_state == null)
                source_state = try alloc.dupe(u8, view.state_key);
        }
    }
    // Withdrawing the asset cannot uncount the surviving direct edge.
    try receiver.batch(.{ .writes = &.{.{ .key = "doc:a", .value = "{\"title\":\"source removed\"}" }}, .sync_level = .enrichments });
    try receiver.runUntilIdle();
    const remaining_count = try receiver.core.store.get(alloc, count_key);
    defer alloc.free(remaining_count);
    try std.testing.expectEqual(@as(?usize, 1), try db_mod.graph_edge_contender.decodeVisibleCount(remaining_count, generation));
    const direct = try receiver.getEdges(alloc, "g", "doc:a", "mentions", .out);
    defer db_mod.graph_mod.GraphIndex.freeEdges(alloc, direct);
    try std.testing.expectEqual(@as(usize, 1), direct.len);
    try std.testing.expectEqual(@as(f64, 4), direct[0].weight);
    var changed = std.ArrayListUnmanaged([]u8).empty;
    defer {
        for (changed.items) |key| alloc.free(key);
        changed.deinit(alloc);
    }
    var changed_set = std.StringHashMapUnmanaged(void).empty;
    defer changed_set.deinit(alloc);
    const artifact_ref: db_mod.types.ArtifactRef = .{ .document_id = @constCast("doc:a"), .name = @constCast("relations_v1"), .kind = .asset };
    const source = receiver.core.index_manager.graphArtifactSources("g")[0];
    const new_relation = "{\"type\":\"mentions\",\"target\":{\"document_id\":\"doc:new\"}}";
    try std.testing.expectError(error.ResourceLimitExceeded, db_mod.materializeGraphArtifactValuePaged(alloc, receiver.core.store, receiver.core.index_manager, "g", artifact_ref, source, source_state orelse return error.TestUnexpectedResult, new_relation, .{}, &changed, &changed_set));
    // An empty replacement snapshot must remove stale local membership too.
    try donor.batch(.{ .writes = &.{.{ .key = "doc:a", .value = "{}" }}, .sync_level = .enrichments });
    try donor.runUntilIdle();
    try donor.batch(.{ .graph_deletes = &.{.{ .index_name = "g", .source = "doc:a", .target = "doc:b", .edge_type = "mentions" }}, .sync_level = .full_index });
    try receiver.importMergeRangeFromTransitionDonor(&donor, .{ .start = "doc:a", .end = "doc:z" });
    const empty = try receiver.core.store.scanPrefix(alloc, prefix);
    defer db_mod.docstore_mod.DocStore.freeResults(alloc, empty);
    try std.testing.expectEqual(@as(usize, 0), empty.len);
    try db_mod.materializeGraphArtifactValuePaged(alloc, receiver.core.store, receiver.core.index_manager, "g", artifact_ref, source, source_state.?, new_relation, .{}, &changed, &changed_set);
    const new_count = try receiver.core.store.get(alloc, count_key);
    defer alloc.free(new_count);
    try std.testing.expectEqual(@as(?usize, 1), try db_mod.graph_edge_contender.decodeVisibleCount(new_count, generation));
}

test "db direct merge import preserves mixed graph ttl contributor precedence" {
    const alloc = std.testing.allocator;
    var donor_tmp = try db_mod.TestDirectory.init("db-graph-mixed-merge-donor");
    defer donor_tmp.cleanup();
    var receiver_tmp = try db_mod.TestDirectory.init("db-graph-mixed-merge-receiver");
    defer receiver_tmp.cleanup();
    var donor = try DB.open(alloc, donor_tmp.path(), .{ .start_optional_runtimes = false });
    defer donor.close();
    var receiver = try DB.open(alloc, receiver_tmp.path(), .{ .start_optional_runtimes = false });
    defer receiver.close();
    const enrichment: db_mod.types.EnrichmentConfig = .{ .name = "relations_v1", .kind = .asset, .field = "relations", .content_type = "application/json" };
    const index: db_mod.types.IndexConfig = .{ .name = "relations_graph", .kind = .graph, .config_json =
        \\{"ttl":{"duration":"1h"},"sources":[{"artifact":"relations_v1"}]}
    };
    try donor.enrichment.addEnrichment(enrichment);
    try donor.addIndex(index);
    try receiver.enrichment.addEnrichment(enrichment);
    try receiver.addIndex(index);
    try donor.batch(.{ .writes = &.{.{ .key = "doc:a", .value = "{}" }}, .sync_level = .full_index });
    try donor.batch(.{ .graph_writes = &.{.{
        .index_name = "relations_graph",
        .source = "doc:a",
        .target = "doc:b",
        .edge_type = "mentions",
        .weight = 3,
    }}, .sync_level = .full_index });
    try donor.batch(.{ .writes = &.{.{ .key = "doc:a", .value =
        \\{"relations":{"type":"mentions","target":{"document_id":"doc:b"},"weight":2}}
    }}, .sync_level = .enrichments });
    try donor.runUntilIdle();
    const donor_due = try donor.core.store.scanPrefix(alloc, &db_mod.internal_keys.graph_edge_expiration_index_prefix);
    defer db_mod.docstore_mod.DocStore.freeResults(alloc, donor_due);
    try std.testing.expectEqual(@as(usize, 2), donor_due.len);
    var direct_candidate: ?db_mod.GraphTtlCandidate = null;
    for (donor_due) |row| switch (try db_mod.graph_edge_ttl_expiration.decodeDue(row.value)) {
        .source => |candidate| {
            if (candidate.source_priority == db_mod.graph_mod.direct_source_priority) direct_candidate = candidate;
        },
        .direct => return error.TestUnexpectedResult,
    };
    const direct = direct_candidate orelse return error.TestExpectedGraphEdgeContender;
    try receiver.importMergeRangeFromTransitionDonor(&donor, .{ .start = "doc:a", .end = "doc:z" });
    try receiver.runUntilIdle();
    const receiver_due = try receiver.core.store.scanPrefix(alloc, &db_mod.internal_keys.graph_edge_expiration_index_prefix);
    defer db_mod.docstore_mod.DocStore.freeResults(alloc, receiver_due);
    try std.testing.expectEqual(@as(usize, 2), receiver_due.len);
    var imported_direct = false;
    for (receiver_due) |row| switch (try db_mod.graph_edge_ttl_expiration.decodeDue(row.value)) {
        .source => |candidate| {
            if (candidate.source_priority == db_mod.graph_mod.direct_source_priority) {
                imported_direct = true;
                try std.testing.expectEqual(direct.deadline_ns, candidate.deadline_ns);
                try std.testing.expectEqual(receiver.core.index_manager.graphIndex("relations_graph").?.config.coverage_generation, candidate.generation);
            }
        },
        .direct => return error.TestUnexpectedResult,
    };
    try std.testing.expect(imported_direct);
    const before = try receiver.getEdges(alloc, "relations_graph", "doc:a", "mentions", .out);
    defer db_mod.graph_mod.GraphIndex.freeEdges(alloc, before);
    try std.testing.expectEqual(@as(usize, 1), before.len);
    try std.testing.expectEqual(@as(f64, 3), before[0].weight);
    var manual_clock = db_mod.platform_clock.ManualClock{};
    manual_clock.setRealtimeNs(direct.deadline_ns + 1);
    var ttl_ctx = db_mod.TtlCleanupContext{ .batch = donor.batchContext(), .grace_period_ns = 0, .clock = manual_clock.clock() };
    try std.testing.expect(try db_mod.expireGraphTtlCandidateContext(&ttl_ctx, direct));
    const donor_fallback = try donor.getEdges(alloc, "relations_graph", "doc:a", "mentions", .out);
    defer db_mod.graph_mod.GraphIndex.freeEdges(alloc, donor_fallback);
    try std.testing.expectEqual(@as(f64, 2), donor_fallback[0].weight);
    try receiver.importMergeRangeFromTransitionDonor(&donor, .{ .start = "doc:a", .end = "doc:z" });
    try receiver.runUntilIdle();
    const remaining_due = try receiver.core.store.scanPrefix(alloc, &db_mod.internal_keys.graph_edge_expiration_index_prefix);
    defer db_mod.docstore_mod.DocStore.freeResults(alloc, remaining_due);
    try std.testing.expectEqual(@as(usize, 1), remaining_due.len);
    const receiver_generation = receiver.core.index_manager.graphIndex("relations_graph").?.config.coverage_generation;
    const contender_prefix = try db_mod.internal_keys.graphGlobalEdgeContenderEdgePrefixAlloc(alloc, "relations_graph", receiver_generation, direct.edge_key);
    defer alloc.free(contender_prefix);
    const receiver_contenders = try receiver.core.store.scanPrefix(alloc, contender_prefix);
    defer db_mod.docstore_mod.DocStore.freeResults(alloc, receiver_contenders);
    try std.testing.expectEqual(@as(usize, 1), receiver_contenders.len);
    const remaining = (try db_mod.graph_edge_contender.decode(receiver_contenders[0].value, receiver_generation)) orelse return error.TestExpectedGraphEdgeContender;
    try std.testing.expectEqual(@as(usize, 0), remaining.source_priority);
    const imported_raw = try receiver.core.store.get(alloc, direct.edge_key);
    defer alloc.free(imported_raw);
    var imported_edge = try db_mod.enrichment_artifact_codec.decodeGraphEdgeAlloc(alloc, imported_raw);
    defer imported_edge.deinit(alloc);
    try std.testing.expectEqual(@as(f64, 2), imported_edge.weight);
    const after = try receiver.getEdges(alloc, "relations_graph", "doc:a", "mentions", .out);
    defer db_mod.graph_mod.GraphIndex.freeEdges(alloc, after);
    try std.testing.expectEqual(@as(usize, 1), after.len);
    try std.testing.expectEqual(@as(f64, 2), after[0].weight);

    // A later donor snapshot can retire the remaining asset source too.
    try donor.batch(.{ .writes = &.{.{ .key = "doc:a", .value = "{}" }}, .sync_level = .enrichments });
    try donor.runUntilIdle();
    try receiver.importMergeRangeFromTransitionDonor(&donor, .{ .start = "doc:a", .end = "doc:z" });
    try receiver.runUntilIdle();
    const retired = try receiver.getEdges(alloc, "relations_graph", "doc:a", "mentions", .out);
    defer db_mod.graph_mod.GraphIndex.freeEdges(alloc, retired);
    try std.testing.expectEqual(@as(usize, 0), retired.len);
    const retired_contenders = try receiver.core.store.scanPrefix(alloc, contender_prefix);
    defer db_mod.docstore_mod.DocStore.freeResults(alloc, retired_contenders);
    try std.testing.expectEqual(@as(usize, 0), retired_contenders.len);
    const retired_due = try receiver.core.store.scanPrefix(alloc, &db_mod.internal_keys.graph_edge_expiration_index_prefix);
    defer db_mod.docstore_mod.DocStore.freeResults(alloc, retired_due);
    try std.testing.expectEqual(@as(usize, 0), retired_due.len);
}

test "db split cutover preserves enrichment resume and fencing across reopen" {
    const alloc = std.testing.allocator;

    var parent_tmp = try db_mod.TestDirectory.init("db");
    defer parent_tmp.cleanup();
    const parent_path = parent_tmp.path().ptr;
    defer db_mod.cleanupTempDir(parent_path);

    var child_tmp = try db_mod.TestDirectory.init("db");
    defer child_tmp.cleanup();
    const child_path = child_tmp.path().ptr;
    defer db_mod.cleanupTempDir(child_path);

    {
        var deterministic_parent = db_mod.embedder_mod.DeterministicDenseEmbedder{};
        var parent = try DB.open(alloc, std.mem.span(parent_path), .{
            .enrichment = .{
                .owner_id = "split-parent",
                .dense_embedder = deterministic_parent.interface(),
            },
        });
        defer parent.close();

        try parent.addIndex(.{
            .name = "dv_v1",
            .kind = .dense_vector,
            .config_json = "{\"field\":\"embedding\",\"dims\":3,\"generator\":{\"kind\":\"dense_embedding\",\"source_field\":\"body\",\"embedding_name\":\"body_dense_v1\"}}",
        });

        try parent.batch(.{
            .writes = &.{
                .{ .key = "doc:a", .value = "{\"body\":\"left side\"}" },
                .{ .key = "doc:z", .value = "{\"body\":\"right side\"}" },
            },
            .sync_level = .write,
        });
        try parent.enrichment.runtime.?.waitForApplied(1);
        try db_mod.waitForDerivedReplayTarget(&parent);

        try parent.split(parent.getRange(), "doc:m", "", std.mem.span(child_path), true);
        try parent.finalizeSplit(.{ .start = "", .end = "doc:m" });

        var deterministic_child = db_mod.embedder_mod.DeterministicDenseEmbedder{};
        var child = try DB.open(alloc, std.mem.span(child_path), .{
            .enrichment = .{
                .owner_id = "split-child",
                .dense_embedder = deterministic_child.interface(),
            },
        });
        defer child.close();

        const child_before = try child.stats(alloc);
        defer db_mod.types.freeDBStats(alloc, child_before);
        const child_dense_before = try child.core.loadAppliedSequence(alloc, "dv_v1");
        try child.batch(.{
            .writes = &.{.{ .key = "doc:y", .value = "{\"body\":\"child pre-reopen\"}" }},
            .sync_level = .write,
        });
        try child.runUntilIdle();
        const child_dense_after = try child.core.loadAppliedSequence(alloc, "dv_v1");
        try std.testing.expect(child_dense_after > child_dense_before);
    }

    var deterministic_parent = db_mod.embedder_mod.DeterministicDenseEmbedder{};
    var reopened_parent = try DB.open(alloc, std.mem.span(parent_path), .{
        .enrichment = .{
            .owner_id = "split-parent",
            .dense_embedder = deterministic_parent.interface(),
        },
    });
    defer reopened_parent.close();

    try std.testing.expectError(error.KeyOutOfRange, reopened_parent.batch(.{
        .writes = &.{.{ .key = "doc:y", .value = "{\"body\":\"parent still fenced\"}" }},
        .sync_level = .write,
    }));

    var deterministic_child = db_mod.embedder_mod.DeterministicDenseEmbedder{};
    var reopened_child = try DB.open(alloc, std.mem.span(child_path), .{
        .enrichment = .{
            .owner_id = "split-child",
            .dense_embedder = deterministic_child.interface(),
        },
    });
    defer reopened_child.close();

    const child_before = try reopened_child.stats(alloc);
    defer db_mod.types.freeDBStats(alloc, child_before);

    try reopened_child.batch(.{
        .writes = &.{.{ .key = "doc:x", .value = "{\"body\":\"child post-reopen\"}" }},
        .sync_level = .write,
    });
    try reopened_child.runUntilIdle();

    const pre_reopen_vec = try deterministic_child.interface().embedDense(alloc, "", "child pre-reopen", 3);
    defer alloc.free(pre_reopen_vec);

    var pre_reopen_result = try db_mod.waitForSearchResult(alloc, &reopened_child, .{
        .index_name = "dv_v1",
        .dense = .{
            .vector = pre_reopen_vec,
            .k = 4,
        },
    }, 1);
    defer pre_reopen_result.deinit();
    var found_pre_reopen = false;
    for (pre_reopen_result.hits) |hit| {
        if (std.mem.eql(u8, hit.id, "doc:y")) found_pre_reopen = true;
    }
    try std.testing.expect(found_pre_reopen);

    const post_reopen_vec = try deterministic_child.interface().embedDense(alloc, "", "child post-reopen", 3);
    defer alloc.free(post_reopen_vec);

    var child_result = try db_mod.waitForSearchResult(alloc, &reopened_child, .{
        .index_name = "dv_v1",
        .dense = .{
            .vector = post_reopen_vec,
            .k = 4,
        },
    }, 1);
    defer child_result.deinit();
    var found_post_reopen = false;
    for (child_result.hits) |hit| {
        if (std.mem.eql(u8, hit.id, "doc:x")) found_post_reopen = true;
    }
    try std.testing.expect(found_post_reopen);
}

test "db split cutover preserves enrichment resume and fencing across reopen with durable lsm primary backend" {
    const alloc = std.testing.allocator;

    var parent_tmp = try db_mod.TestDirectory.init("db");
    defer parent_tmp.cleanup();
    const parent_path = parent_tmp.path().ptr;
    defer db_mod.cleanupTempDir(parent_path);

    var child_tmp = try db_mod.TestDirectory.init("db");
    defer child_tmp.cleanup();
    const child_path = child_tmp.path().ptr;
    defer db_mod.cleanupTempDir(child_path);

    const primary_backend: db_mod.PrimaryBackend = .{ .lsm = db_mod.db_config.primary_lsm_options_default };

    {
        var deterministic_parent = db_mod.embedder_mod.DeterministicDenseEmbedder{};
        var parent = try DB.open(alloc, std.mem.span(parent_path), .{
            .primary_backend = primary_backend,
            .enrichment = .{
                .owner_id = "split-parent",
                .dense_embedder = deterministic_parent.interface(),
            },
        });
        defer parent.close();

        try parent.addIndex(.{
            .name = "dv_v1",
            .kind = .dense_vector,
            .config_json = "{\"field\":\"embedding\",\"dims\":3,\"generator\":{\"kind\":\"dense_embedding\",\"source_field\":\"body\",\"embedding_name\":\"body_dense_v1\"}}",
        });

        try parent.batch(.{
            .writes = &.{
                .{ .key = "doc:a", .value = "{\"body\":\"left side\"}" },
                .{ .key = "doc:z", .value = "{\"body\":\"right side\"}" },
            },
            .sync_level = .write,
        });
        try parent.enrichment.runtime.?.waitForApplied(1);
        try db_mod.waitForDerivedReplayTarget(&parent);

        try parent.split(parent.getRange(), "doc:m", "", std.mem.span(child_path), true);
        try parent.finalizeSplit(.{ .start = "", .end = "doc:m" });

        var deterministic_child = db_mod.embedder_mod.DeterministicDenseEmbedder{};
        var child = try DB.open(alloc, std.mem.span(child_path), .{
            .primary_backend = primary_backend,
            .enrichment = .{
                .owner_id = "split-child",
                .dense_embedder = deterministic_child.interface(),
            },
        });
        defer child.close();

        const child_before = try child.stats(alloc);
        defer db_mod.types.freeDBStats(alloc, child_before);
        const child_dense_before = try child.core.loadAppliedSequence(alloc, "dv_v1");
        try child.batch(.{
            .writes = &.{.{ .key = "doc:y", .value = "{\"body\":\"child pre-reopen\"}" }},
            .sync_level = .write,
        });
        try child.runUntilIdle();
        const child_dense_after = try child.core.loadAppliedSequence(alloc, "dv_v1");
        try std.testing.expect(child_dense_after > child_dense_before);
        try parent.sync(true);
        try child.sync(true);
    }

    var deterministic_parent = db_mod.embedder_mod.DeterministicDenseEmbedder{};
    var reopened_parent = try DB.open(alloc, std.mem.span(parent_path), .{
        .primary_backend = primary_backend,
        .enrichment = .{
            .owner_id = "split-parent",
            .dense_embedder = deterministic_parent.interface(),
        },
    });
    defer reopened_parent.close();

    try std.testing.expectError(error.KeyOutOfRange, reopened_parent.batch(.{
        .writes = &.{.{ .key = "doc:y", .value = "{\"body\":\"parent still fenced\"}" }},
        .sync_level = .write,
    }));

    var deterministic_child = db_mod.embedder_mod.DeterministicDenseEmbedder{};
    var reopened_child = try DB.open(alloc, std.mem.span(child_path), .{
        .primary_backend = primary_backend,
        .enrichment = .{
            .owner_id = "split-child",
            .dense_embedder = deterministic_child.interface(),
        },
    });
    defer reopened_child.close();

    const child_before = try reopened_child.stats(alloc);
    defer db_mod.types.freeDBStats(alloc, child_before);

    try reopened_child.batch(.{
        .writes = &.{.{ .key = "doc:x", .value = "{\"body\":\"child post-reopen\"}" }},
        .sync_level = .write,
    });
    try reopened_child.runUntilIdle();

    const pre_reopen_vec = try deterministic_child.interface().embedDense(alloc, "", "child pre-reopen", 3);
    defer alloc.free(pre_reopen_vec);

    var pre_reopen_result = try db_mod.waitForSearchResult(alloc, &reopened_child, .{
        .index_name = "dv_v1",
        .dense = .{
            .vector = pre_reopen_vec,
            .k = 4,
        },
    }, 1);
    defer pre_reopen_result.deinit();
    var found_pre_reopen = false;
    for (pre_reopen_result.hits) |hit| {
        if (std.mem.eql(u8, hit.id, "doc:y")) found_pre_reopen = true;
    }
    try std.testing.expect(found_pre_reopen);

    const post_reopen_vec = try deterministic_child.interface().embedDense(alloc, "", "child post-reopen", 3);
    defer alloc.free(post_reopen_vec);

    var child_result = try db_mod.waitForSearchResult(alloc, &reopened_child, .{
        .index_name = "dv_v1",
        .dense = .{
            .vector = post_reopen_vec,
            .k = 4,
        },
    }, 1);
    defer child_result.deinit();
    var found_post_reopen = false;
    for (child_result.hits) |hit| {
        if (std.mem.eql(u8, hit.id, "doc:x")) found_post_reopen = true;
    }
    try std.testing.expect(found_post_reopen);
}

test "db merge-style cutover preserves enrichment resume and fencing across reopen" {
    const alloc = std.testing.allocator;

    var receiver_tmp = try db_mod.TestDirectory.init("db");
    defer receiver_tmp.cleanup();
    const receiver_path = receiver_tmp.path().ptr;
    defer db_mod.cleanupTempDir(receiver_path);

    var donor_tmp = try db_mod.TestDirectory.init("db");
    defer donor_tmp.cleanup();
    const donor_path = donor_tmp.path().ptr;
    defer db_mod.cleanupTempDir(donor_path);

    {
        var deterministic_receiver = db_mod.embedder_mod.DeterministicDenseEmbedder{};
        var receiver = try DB.open(alloc, std.mem.span(receiver_path), .{
            .enrichment = .{
                .owner_id = "merge-receiver",
                .dense_embedder = deterministic_receiver.interface(),
            },
        });
        defer receiver.close();
        try receiver.updateRange(.{ .start = "doc:a", .end = "doc:m" });

        var deterministic_donor = db_mod.embedder_mod.DeterministicDenseEmbedder{};
        var donor = try DB.open(alloc, std.mem.span(donor_path), .{
            .enrichment = .{
                .owner_id = "merge-donor",
                .dense_embedder = deterministic_donor.interface(),
            },
        });
        defer donor.close();
        try donor.updateRange(.{ .start = "doc:m", .end = "" });

        try receiver.addIndex(.{
            .name = "dv_v1",
            .kind = .dense_vector,
            .config_json = "{\"field\":\"embedding\",\"dims\":3,\"generator\":{\"kind\":\"dense_embedding\",\"source_field\":\"body\",\"embedding_name\":\"body_dense_v1\"}}",
        });
        try donor.addIndex(.{
            .name = "dv_v1",
            .kind = .dense_vector,
            .config_json = "{\"field\":\"embedding\",\"dims\":3,\"generator\":{\"kind\":\"dense_embedding\",\"source_field\":\"body\",\"embedding_name\":\"body_dense_v1\"}}",
        });

        try donor.batch(.{
            .writes = &.{.{ .key = "doc:y", .value = "{\"body\":\"donor pre-cutover\"}" }},
            .sync_level = .write,
        });
        try donor.enrichment.runtime.?.waitForApplied(1);
        try donor.executor.waitForAll(2);

        try receiver.updateRange(.{ .start = "doc:a", .end = "" });
        try donor.updateRange(.{ .start = "doc:m", .end = "doc:m" });

        try receiver.batch(.{
            .writes = &.{.{ .key = "doc:z", .value = "{\"body\":\"receiver pre-reopen\"}" }},
            .sync_level = .write,
        });
        try receiver.enrichment.runtime.?.waitForApplied(1);
        try receiver.executor.waitForAll(2);
    }

    var deterministic_donor = db_mod.embedder_mod.DeterministicDenseEmbedder{};
    var reopened_donor = try DB.open(alloc, std.mem.span(donor_path), .{
        .enrichment = .{
            .owner_id = "merge-donor",
            .dense_embedder = deterministic_donor.interface(),
        },
    });
    defer reopened_donor.close();

    try std.testing.expectError(error.KeyOutOfRange, reopened_donor.batch(.{
        .writes = &.{.{ .key = "doc:z", .value = "{\"body\":\"donor still fenced\"}" }},
        .sync_level = .write,
    }));

    var deterministic_receiver = db_mod.embedder_mod.DeterministicDenseEmbedder{};
    var reopened_receiver = try DB.open(alloc, std.mem.span(receiver_path), .{
        .enrichment = .{
            .owner_id = "merge-receiver",
            .dense_embedder = deterministic_receiver.interface(),
        },
    });
    defer reopened_receiver.close();

    const receiver_before = try reopened_receiver.stats(alloc);
    defer db_mod.types.freeDBStats(alloc, receiver_before);
    const receiver_dense_before = try reopened_receiver.core.loadAppliedSequence(alloc, "dv_v1");

    try reopened_receiver.batch(.{
        .writes = &.{.{ .key = "doc:w", .value = "{\"body\":\"receiver post-reopen\"}" }},
        .sync_level = .write,
    });
    try reopened_receiver.runUntilIdle();
    const receiver_dense_after = try db_mod.waitForAppliedSequenceAdvance(alloc, &reopened_receiver, "dv_v1", receiver_dense_before);
    try std.testing.expect(receiver_dense_after > receiver_dense_before);

    const pre_reopen_vec = try deterministic_receiver.interface().embedDense(alloc, "", "receiver pre-reopen", 3);
    defer alloc.free(pre_reopen_vec);

    var pre_reopen_result = try db_mod.waitForSearchResult(alloc, &reopened_receiver, .{
        .index_name = "dv_v1",
        .dense = .{
            .vector = pre_reopen_vec,
            .k = 3,
        },
    }, 1);
    defer pre_reopen_result.deinit();
    var found_pre_reopen = false;
    for (pre_reopen_result.hits) |hit| {
        if (std.mem.eql(u8, hit.id, "doc:z")) found_pre_reopen = true;
    }
    try std.testing.expect(found_pre_reopen);

    const receiver_vec = try deterministic_receiver.interface().embedDense(alloc, "", "receiver post-reopen", 3);
    defer alloc.free(receiver_vec);

    var receiver_result = try db_mod.waitForSearchResult(alloc, &reopened_receiver, .{
        .index_name = "dv_v1",
        .dense = .{
            .vector = receiver_vec,
            .k = 3,
        },
    }, 1);
    defer receiver_result.deinit();
    var found = false;
    for (receiver_result.hits) |hit| {
        if (std.mem.eql(u8, hit.id, "doc:w")) found = true;
    }
    try std.testing.expect(found);
}

test "db merge-style cutover preserves enrichment resume and fencing across reopen with durable lsm primary backend" {
    const alloc = std.testing.allocator;

    var receiver_tmp = try db_mod.TestDirectory.init("db");
    defer receiver_tmp.cleanup();
    const receiver_path = receiver_tmp.path().ptr;
    defer db_mod.cleanupTempDir(receiver_path);

    var donor_tmp = try db_mod.TestDirectory.init("db");
    defer donor_tmp.cleanup();
    const donor_path = donor_tmp.path().ptr;
    defer db_mod.cleanupTempDir(donor_path);

    const primary_backend: db_mod.PrimaryBackend = .{ .lsm = db_mod.db_config.primary_lsm_options_default };

    {
        var deterministic_receiver = db_mod.embedder_mod.DeterministicDenseEmbedder{};
        var receiver = try DB.open(alloc, std.mem.span(receiver_path), .{
            .primary_backend = primary_backend,
            .enrichment = .{
                .owner_id = "merge-receiver",
                .dense_embedder = deterministic_receiver.interface(),
            },
        });
        defer receiver.close();
        try receiver.updateRange(.{ .start = "doc:a", .end = "doc:m" });

        var deterministic_donor = db_mod.embedder_mod.DeterministicDenseEmbedder{};
        var donor = try DB.open(alloc, std.mem.span(donor_path), .{
            .primary_backend = primary_backend,
            .enrichment = .{
                .owner_id = "merge-donor",
                .dense_embedder = deterministic_donor.interface(),
            },
        });
        defer donor.close();
        try donor.updateRange(.{ .start = "doc:m", .end = "" });

        try receiver.addIndex(.{
            .name = "dv_v1",
            .kind = .dense_vector,
            .config_json = "{\"field\":\"embedding\",\"dims\":3,\"generator\":{\"kind\":\"dense_embedding\",\"source_field\":\"body\",\"embedding_name\":\"body_dense_v1\"}}",
        });
        try donor.addIndex(.{
            .name = "dv_v1",
            .kind = .dense_vector,
            .config_json = "{\"field\":\"embedding\",\"dims\":3,\"generator\":{\"kind\":\"dense_embedding\",\"source_field\":\"body\",\"embedding_name\":\"body_dense_v1\"}}",
        });

        try donor.batch(.{
            .writes = &.{.{ .key = "doc:y", .value = "{\"body\":\"donor pre-cutover\"}" }},
            .sync_level = .write,
        });
        try donor.enrichment.runtime.?.waitForApplied(1);
        try donor.executor.waitForAll(2);

        try receiver.updateRange(.{ .start = "doc:a", .end = "" });
        try donor.updateRange(.{ .start = "doc:m", .end = "doc:m" });

        try receiver.batch(.{
            .writes = &.{.{ .key = "doc:z", .value = "{\"body\":\"receiver pre-reopen\"}" }},
            .sync_level = .write,
        });
        try receiver.enrichment.runtime.?.waitForApplied(1);
        try receiver.executor.waitForAll(2);
        try receiver.sync(true);
        try donor.sync(true);
    }

    var deterministic_receiver = db_mod.embedder_mod.DeterministicDenseEmbedder{};
    var reopened_receiver = try DB.open(alloc, std.mem.span(receiver_path), .{
        .primary_backend = primary_backend,
        .enrichment = .{
            .owner_id = "merge-receiver",
            .dense_embedder = deterministic_receiver.interface(),
        },
    });
    defer reopened_receiver.close();

    var deterministic_donor = db_mod.embedder_mod.DeterministicDenseEmbedder{};
    var reopened_donor = try DB.open(alloc, std.mem.span(donor_path), .{
        .primary_backend = primary_backend,
        .enrichment = .{
            .owner_id = "merge-donor",
            .dense_embedder = deterministic_donor.interface(),
        },
    });
    defer reopened_donor.close();

    try std.testing.expectError(error.KeyOutOfRange, reopened_donor.batch(.{
        .writes = &.{.{ .key = "doc:x", .value = "{\"body\":\"donor still fenced\"}" }},
        .sync_level = .write,
    }));

    const receiver_before = try reopened_receiver.stats(alloc);
    defer db_mod.types.freeDBStats(alloc, receiver_before);
    const receiver_dense_before = try reopened_receiver.core.loadAppliedSequence(alloc, "dv_v1");

    try reopened_receiver.batch(.{
        .writes = &.{.{ .key = "doc:w", .value = "{\"body\":\"receiver post-reopen\"}" }},
        .sync_level = .write,
    });
    try reopened_receiver.runUntilIdle();
    const receiver_dense_after = try db_mod.waitForAppliedSequenceAdvance(alloc, &reopened_receiver, "dv_v1", receiver_dense_before);
    try std.testing.expect(receiver_dense_after > receiver_dense_before);

    const pre_reopen_vec = try deterministic_receiver.interface().embedDense(alloc, "", "receiver pre-reopen", 3);
    defer alloc.free(pre_reopen_vec);

    var pre_reopen_result = try db_mod.waitForSearchResult(alloc, &reopened_receiver, .{
        .index_name = "dv_v1",
        .dense = .{
            .vector = pre_reopen_vec,
            .k = 3,
        },
    }, 1);
    defer pre_reopen_result.deinit();
    var found_pre_reopen = false;
    for (pre_reopen_result.hits) |hit| {
        if (std.mem.eql(u8, hit.id, "doc:z")) found_pre_reopen = true;
    }
    try std.testing.expect(found_pre_reopen);

    const receiver_vec = try deterministic_receiver.interface().embedDense(alloc, "", "receiver post-reopen", 3);
    defer alloc.free(receiver_vec);

    var receiver_result = try db_mod.waitForSearchResult(alloc, &reopened_receiver, .{
        .index_name = "dv_v1",
        .dense = .{
            .vector = receiver_vec,
            .k = 3,
        },
    }, 1);
    defer receiver_result.deinit();
    var found = false;
    for (receiver_result.hits) |hit| {
        if (std.mem.eql(u8, hit.id, "doc:w")) found = true;
    }
    try std.testing.expect(found);
}

test "db restore snapshot replays managed chunked dense embeddings for durable lsm primary backend" {
    const alloc = std.testing.allocator;

    var src_tmp = try db_mod.TestDirectory.init("db");
    defer src_tmp.cleanup();
    const src_path = src_tmp.path().ptr;
    defer db_mod.cleanupTempDir(src_path);

    var restore_tmp = try db_mod.TestDirectory.init("db");
    defer restore_tmp.cleanup();
    const restore_path = restore_tmp.path().ptr;
    defer db_mod.cleanupTempDir(restore_path);

    const primary_backend: db_mod.PrimaryBackend = .{ .lsm = .{ .flush_threshold = 1 } };

    {
        var deterministic = db_mod.embedder_mod.DeterministicDenseEmbedder{};
        var db = try DB.open(alloc, std.mem.span(src_path), .{
            .primary_backend = primary_backend,
            .enrichment = .{
                .owner_id = "worker-a",
                .dense_embedder = deterministic.interface(),
            },
        });
        defer db.close();

        try db.addIndex(.{
            .name = "dv_v1",
            .kind = .dense_vector,
            .config_json = "{\"field\":\"embedding\",\"dims\":3,\"generator\":{\"kind\":\"dense_embedding\",\"source_field\":\"body\",\"chunk_name\":\"body_chunks_v1\",\"chunk_size\":8,\"chunk_overlap\":2}}",
        });

        try db.batch(.{
            .writes = &.{
                .{ .key = "doc:a", .value = "{\"body\":\"abcdefghijklmno\"}" },
            },
            .sync_level = .full_index,
        });

        const query_vec = try deterministic.interface().embedDense(alloc, "", "abcdefgh", 3);
        defer alloc.free(query_vec);
        var before = try db_mod.waitForSearchResult(alloc, &db, .{
            .index_name = "dv_v1",
            .dense = .{ .vector = query_vec, .k = 3 },
            .return_mode = .parent,
        }, 1);
        defer before.deinit();
        try std.testing.expectEqualStrings("doc:a", before.hits[0].id);

        _ = try db.snapshot("snap1");
    }

    const snapshot_root = try std.fmt.allocPrint(alloc, "{s}.snapshots/snap1", .{std.mem.span(src_path)});
    defer alloc.free(snapshot_root);
    defer {
        var snapshots_buf: [512]u8 = undefined;
        if (std.fmt.bufPrint(&snapshots_buf, "{s}.snapshots", .{std.mem.span(src_path)})) |snapshots| {
            var io_impl = db_mod.threadedIo();
            defer io_impl.deinit();
            std.Io.Dir.cwd().deleteTree(io_impl.io(), snapshots) catch {};
        } else |_| {}
    }

    try DB.restoreSnapshotTo(alloc, snapshot_root, std.mem.span(restore_path), .{
        .primary_backend = primary_backend,
    });

    var deterministic = db_mod.embedder_mod.DeterministicDenseEmbedder{};
    var restored = try DB.open(alloc, std.mem.span(restore_path), .{
        .primary_backend = primary_backend,
        .enrichment = .{
            .owner_id = "worker-a",
            .dense_embedder = deterministic.interface(),
        },
    });
    defer restored.close();

    const chunk_prefix = try db_mod.internal_keys.artifactNamedPrefixAlloc(alloc, "doc:a", "chunk", "body_chunks_v1");
    defer alloc.free(chunk_prefix);
    const chunk_records = try restored.core.store.scanPrefix(alloc, chunk_prefix);
    defer db_mod.docstore_mod.DocStore.freeResults(alloc, chunk_records);
    try std.testing.expect(chunk_records.len > 0);

    const rebuilt = try restored.rebuildDenseIndexesFromStoredEmbeddingArtifacts(alloc);
    try std.testing.expect(rebuilt > 0);
    _ = try restored.enrichment.replayGeneratedEnrichmentsFromStoredDocs(alloc);
    try restored.runUntilIdle();
    const pending = restored.pendingWorkStats();
    try std.testing.expectEqual(pending.enrichment.target_sequence, pending.enrichment.applied_sequence);
    try std.testing.expect(pending.enrichment.applied_sequence >= 1);

    const query_vec = try deterministic.interface().embedDense(alloc, "", "abcdefgh", 3);
    defer alloc.free(query_vec);
    var after = try db_mod.waitForSearchResult(alloc, &restored, .{
        .index_name = "dv_v1",
        .dense = .{ .vector = query_vec, .k = 3 },
        .return_mode = .parent,
    }, 1);
    defer after.deinit();
    try std.testing.expectEqualStrings("doc:a", after.hits[0].id);
}

test "db restore repair does not complete before regenerated chunk embeddings are query visible" {
    const alloc = std.testing.allocator;

    var src_tmp = try db_mod.TestDirectory.init("db");
    defer src_tmp.cleanup();
    const src_path = src_tmp.path().ptr;
    defer db_mod.cleanupTempDir(src_path);

    var restore_tmp = try db_mod.TestDirectory.init("db");
    defer restore_tmp.cleanup();
    const restore_path = restore_tmp.path().ptr;
    defer db_mod.cleanupTempDir(restore_path);

    const primary_backend: db_mod.PrimaryBackend = .{ .lsm = .{ .flush_threshold = 1 } };
    var portable = std.ArrayList(u8).empty;
    defer portable.deinit(alloc);

    {
        var deterministic = db_mod.embedder_mod.DeterministicDenseEmbedder{};
        var db = try DB.open(alloc, std.mem.span(src_path), .{
            .primary_backend = primary_backend,
            .enrichment = .{
                .owner_id = "worker-a",
                .dense_embedder = deterministic.interface(),
            },
        });
        defer db.close();

        try db.addIndex(.{
            .name = "dv_v1",
            .kind = .dense_vector,
            .config_json = "{\"field\":\"embedding\",\"dims\":3,\"generator\":{\"kind\":\"dense_embedding\",\"source_field\":\"body\",\"chunk_name\":\"body_chunks_v1\",\"chunk_size\":8,\"chunk_overlap\":2}}",
        });

        try db.batch(.{
            .writes = &.{
                .{ .key = "doc:a", .value = "{\"body\":\"abcdefghijklmno\"}" },
            },
            .sync_level = .full_index,
        });

        try db_mod.portable_backup.exportPortable(alloc, db.core.store, &portable);
    }

    // Portable import starts with an empty runtime-index directory and restores
    // only portable logical state. Remove imported embedding artifacts while
    // retaining chunks to reproduce the CI snapshot precisely.
    {
        var restored = try DB.open(alloc, std.mem.span(restore_path), .{
            .primary_backend = primary_backend,
            .open_mode = .writer_no_replay,
            .start_index_workers = false,
            .start_optional_runtimes = false,
        });
        defer restored.close();
        try db_mod.portable_backup.importPortable(alloc, restored.core.store, portable.items);

        const chunk_prefix = try db_mod.internal_keys.artifactNamedPrefixAlloc(alloc, "doc:a", "chunk", "body_chunks_v1");
        defer alloc.free(chunk_prefix);
        const chunk_records = try restored.core.store.scanPrefix(alloc, chunk_prefix);
        defer db_mod.docstore_mod.DocStore.freeResults(alloc, chunk_records);
        try std.testing.expect(chunk_records.len > 0);
        for (chunk_records) |chunk| {
            const embedding_key = try db_mod.internal_keys.derivedEmbeddingArtifactKeyAlloc(alloc, chunk.key, "dv_v1");
            defer alloc.free(embedding_key);
            try restored.core.store.delete(embedding_key);
        }

        // A restored source checkpoint can equal the destination's first
        // regenerated replay sequence even though the destination runtime
        // index is empty. Repair must reset this projection boundary or the
        // enrichment worker will skip sequence 1 as already applied.
        try db_mod.enrichment_state.saveProjectionCheckpoint(
            restored.core.store,
            db_mod.enrichment_runtime_mod.scope_name,
            .{ .applied_sequence = 1 },
        );
    }

    try DB.markRestorePrimaryRestoredForPathWithArtifact(
        alloc,
        std.mem.span(restore_path),
        "snap1",
        "file:///tmp/backups",
        "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855",
        "snapshots/snap1",
        7001,
    );
    try DB.markRestoreRuntimeRepairNeeded(alloc, std.mem.span(restore_path));
    try std.testing.expect(try DB.restoreRuntimeRepairNeededForPath(alloc, std.mem.span(restore_path)));

    // Run every phase on one writer-no-replay owner without background
    // workers so inline provider admission and completion proofs are required.
    {
        var deterministic = db_mod.embedder_mod.DeterministicDenseEmbedder{};
        var restored = try DB.open(alloc, std.mem.span(restore_path), .{
            .primary_backend = primary_backend,
            .open_mode = .writer_no_replay,
            .start_index_workers = false,
            .start_optional_runtime_workers = false,
            .enrichment = .{
                .owner_id = "worker-a",
                .dense_embedder = deterministic.interface(),
            },
        });
        defer restored.close();
        // Keep the same runtime across reset, source replay and inline drain.
        // Reopening between phases would hide a stale shutdown admission flag.
        while (try restored.restoreRuntimeRepairNeeded()) {
            try std.testing.expect(try restored.repairRestoreRuntimeStateStepIfNeeded(alloc));
            try std.testing.expect(!restored.enrichment.runtime.?.isStarted());
        }
    }

    {
        var deterministic = db_mod.embedder_mod.DeterministicDenseEmbedder{};
        var restored = try DB.open(alloc, std.mem.span(restore_path), .{
            .primary_backend = primary_backend,
            .open_mode = .query_readonly,
            .start_index_workers = false,
            .start_optional_runtimes = false,
            .enrichment = .{
                .owner_id = "worker-a",
                .dense_embedder = deterministic.interface(),
            },
        });
        defer restored.close();

        const query_vec = try deterministic.interface().embedDense(alloc, "", "abcdefgh", 3);
        defer alloc.free(query_vec);
        var after = try db_mod.waitForSearchResult(alloc, &restored, .{
            .index_name = "dv_v1",
            .dense = .{ .vector = query_vec, .k = 3 },
            .return_mode = .parent,
        }, 1);
        defer after.deinit();
        try std.testing.expectEqualStrings("doc:a", after.hits[0].id);
    }

    var restore_state = (try DB.readRestoreStateForPathWithIo(alloc, std.testing.io, std.mem.span(restore_path))).?;
    defer restore_state.deinit(alloc);
    try std.testing.expect(restore_state.runtime_repair_complete);
    try std.testing.expectEqualStrings("complete", restore_state.phase);
}

test "source vector table defers reopen GC and bounded maintenance preserves readers writes and cancellation" {
    const alloc = std.testing.allocator;
    const payload = @import("../../artifact_payload.zig");
    var path_tmp = try db_mod.TestDirectory.init("db");
    defer path_tmp.cleanup();
    const path = path_tmp.path().ptr;
    defer db_mod.cleanupTempDir(path);
    const key = try db_mod.internal_keys.embeddingArtifactKeyForDocumentAlloc(alloc, "doc", "model");
    defer alloc.free(key);
    const first = try db_mod.enrichment_artifact_codec.encodeDenseEmbeddingAlloc(alloc, 1, &.{ 1, 2, 3 });
    defer alloc.free(first);
    const second = try db_mod.enrichment_artifact_codec.encodeDenseEmbeddingAlloc(alloc, 2, &.{ 4, 5, 6 });
    defer alloc.free(second);
    const third = try db_mod.enrichment_artifact_codec.encodeDenseEmbeddingAlloc(alloc, 3, &.{ 7, 8, 9 });
    defer alloc.free(third);
    const opts: db_mod.OpenOptions = .{
        .start_index_workers = false,
        .start_optional_runtimes = false,
        .ttl_cleanup = .{ .enabled = false },
    };
    {
        var create_opts = opts;
        create_opts.table_storage = .{ .dense_embeddings = .vector_store };
        var db = try DB.open(alloc, std.mem.span(path), create_opts);
        defer db.close();
        try db.core.store.put(key, first);
        try db.core.store.put(key, second);
        const source = db.local_execution.source_vectors.load(.acquire).?;
        const iface = source.interface();
        try iface.vtable.prepare(iface.ptr, &.{.{ .reference = try payload.Reference.forArtifact("uncommitted", first), .artifact = first }});
        try db.core.store.sync(true);
        try source.checkpoint();
        try std.testing.expectEqual(@as(u64, 3), source.statsSnapshot().retained_payloads);
    }
    {
        var db = try DB.open(alloc, std.mem.span(path), opts);
        defer db.close();
        const source = db.local_execution.source_vectors.load(.acquire).?;
        try std.testing.expect(source.background_gc and source.mark_outside_lock and source.independent_scan);
        try std.testing.expectEqual(@as(u64, 0), source.statsSnapshot().collections);
        try std.testing.expectEqual(@as(u64, 0), source.statsSnapshot().collection_mark_rows);
        try std.testing.expectEqual(@as(u64, 3), source.statsSnapshot().retained_payloads);
        source.mark_step_rows = 1;
        {
            var old = try db.core.store.beginReadTxn();
            defer old.abort();
            try db.core.store.delete(key);
            _ = try db.enrichment.runArtifactRepairMetadataMaintenancePass();
            try std.testing.expect(!source.collectionPending());
            const retained = try old.get(key);
            try std.testing.expectEqualSlices(u8, second, retained);
        }
        try db.core.store.put(key, third);
        _ = try db.enrichment.runArtifactRepairMetadataMaintenancePass();
        try std.testing.expect(source.collectionPending());
        _ = try db.enrichment.runArtifactRepairMetadataMaintenancePass();
        try std.testing.expect(source.collectionPending());
        try std.testing.expectEqual(@as(u64, 1), source.statsSnapshot().collection_mark_max_step_rows);
        // Close cancels the partial mark before its primary owner is destroyed.
        // No uncommitted deletion/collection is published by this cancellation.
    }
    var db = try DB.open(alloc, std.mem.span(path), opts);
    defer db.close();
    const source = db.local_execution.source_vectors.load(.acquire).?;
    try std.testing.expectEqual(@as(u64, 0), source.statsSnapshot().collection_mark_rows);
    const current = try db.core.store.get(alloc, key);
    defer alloc.free(current);
    try std.testing.expectEqualSlices(u8, third, current);
    source.mark_step_rows = 1;
    var turns: usize = 0;
    // Recovery may still own a short source session even with optional
    // workers disabled. An idle pass can mean admission was deferred, not
    // that verification completed. Drive the same later wakeups as the worker.
    while (source.statsSnapshot().collections == 0 and source.statsSnapshot().collection_copy_deferrals == 0) : (turns += 1) {
        const pending = try db.enrichment.runArtifactRepairMetadataMaintenancePass();
        const stats = source.statsSnapshot();
        if (!pending and stats.collections == 0 and stats.collection_copy_deferrals == 0) {
            try std.testing.expect(stats.collection_deferrals != 0);
            db_mod.platform_time.yieldBriefly();
        }
        try std.testing.expect(turns < 1024);
    }
    try db.enrichment.runArtifactRepairMetadataMaintenanceUntilIdle();
    try std.testing.expect(turns > 1);
    if (source.cost_based_gc) {
        try std.testing.expect(source.statsSnapshot().collection_deferred_obsolete_bytes != 0);
        source.garbage_deadline_seconds = 0;
        try db.enrichment.runArtifactRepairMetadataMaintenanceUntilIdle();
    }
    try std.testing.expectEqual(@as(u64, 1), source.statsSnapshot().retained_payloads);
    try std.testing.expectEqual(@as(u64, 1), source.statsSnapshot().collection_mark_max_step_rows);
    try std.testing.expect(source.statsSnapshot().collection_mark_outside_lock_ns > 0);
    try db.core.store.delete(key);
    turns = 0;
    while (!try db.collectSourceVectorGarbage()) : (turns += 1)
        try std.testing.expect(turns < 1024);
    try std.testing.expectEqual(@as(u64, 0), source.statsSnapshot().retained_payloads);
}

test "db ordered artifact inventory producer coverage pages do not idle between productive turns" {
    const alloc = std.testing.allocator;
    const publication = @import("../artifact_publication.zig");
    var tmp = try db_mod.TestDirectory.init("producer-coverage-cadence");
    defer tmp.cleanup();
    var db = try DB.open(alloc, tmp.path(), .{ .identity_namespace = .{ .table_id = 1, .shard_id = 2, .range_id = 2 }, .online_source_authority = .raft, .primary_backend = .{ .lsm = .{} }, .start_index_workers = false, .start_optional_runtimes = false });
    defer db.close();
    try db.setSchemaJson(alloc, "{}");
    try db.enrichment.addEnrichment(.{ .name = "extraction", .kind = .asset, .field = "body", .content_type = "application/json" });
    for (0..140) |i| {
        const name = try std.fmt.allocPrint(alloc, "doc-{d:0>4}", .{i});
        defer alloc.free(name);
        const key = try db_mod.internal_keys.documentKeyAlloc(alloc, name);
        defer alloc.free(key);
        try db.core.store.put(key, "{\"body\":\"one\"}");
    }
    var ordered = try db.artifactInventoryCommand(alloc);
    defer ordered.catalogs.deinit(alloc);
    ordered.binding.effect_protocol = 14;
    var namespace: publication.Namespace = undefined;
    db_mod.doc_identity.encodeNamespace(&namespace, db.core.identity_namespace);
    {
        var txn = try db.core.store.beginWriteTxn();
        errdefer txn.abort();
        const catalogs = try @import("../artifact_inventory.zig").catalogs(&txn);
        try @import("../artifact_inventory.zig").stageOrdered(alloc, &txn, ordered, 1);
        try publication.stageAuthority(&txn, .{ .mode = .activate, .namespace = namespace, .authority_epoch = 1, .catalog_digest = catalogs.digest(), .producer_name = "", .producer_generation = 0, .sources = &.{}, .mutations = &.{}, .publication_digest = @splat(0) });
        try txn.commit();
    }
    // No protocol-15 work registry and no clock advancement. Both pages must
    // run immediately; otherwise a large census advances at 64 rows / 5 s.
    try std.testing.expect(try db.enrichment.advanceArtifactProducerWorkPage());
    try std.testing.expectEqual(@as(u64, 0), db.enrichment.artifacts.producer_scheduler.retry_after_ns.load(.acquire));
    try std.testing.expect(try db.enrichment.advanceArtifactProducerWorkPage());
    try std.testing.expectEqual(@as(u64, 0), db.enrichment.artifacts.producer_scheduler.retry_after_ns.load(.acquire));
    var turns: usize = 0;
    while (db.enrichment.artifacts.producer_scheduler.retry_after_ns.load(.acquire) == 0 and turns < 1000) : (turns += 1) {
        _ = try db.enrichment.advanceArtifactProducerWorkPage();
    }
    // Protocol-14 rows cannot fabricate protocol-15 receipts. The census can
    // finish, but an unresolved witness sweep must remain pending and back off.
    try std.testing.expect(turns < 1000);
    try std.testing.expect(db.enrichment.artifacts.producer_scheduler.pending.load(.acquire));
    try std.testing.expect(db.enrichment.artifacts.producer_scheduler.retry_after_ns.load(.acquire) > db.independentMaintenanceNowNs());
    try std.testing.expect(!try db.enrichment.advanceArtifactProducerWorkPage());
}

test "db ordered artifact inventory producer dispatch is durable and never completes work" {
    const alloc = std.testing.allocator;
    const publication = @import("../artifact_publication.zig");
    const obligations = @import("../artifact_producer_obligations.zig");
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    const path = try std.fmt.allocPrint(alloc, ".zig-cache/tmp/{s}/producer-dispatch", .{tmp.sub_path});
    defer alloc.free(path);
    const options: db_mod.OpenOptions = .{ .identity_namespace = .{ .table_id = 1, .shard_id = 2, .range_id = 2 }, .online_source_authority = .raft, .primary_backend = .{ .lsm = .{} }, .start_index_workers = false, .start_optional_runtimes = false };
    var expected_sequence: u64 = 0;
    var expected_dispatch_sequence: u64 = 0;
    for (0..2) |pass| {
        var db = try DB.open(alloc, path, options);
        defer db.close();
        // Keep the consumer paused so the wakeup can be inspected without
        // another write or a polling worker concealing a lost notification.
        try db.enrichment.reconfigureRuntimePaused(.{ .enable_without_producers = true });
        if (pass == 0) {
            try db.setSchemaJson(alloc, "{}");
            try db.enrichment.addEnrichment(.{ .name = "extraction", .kind = .asset, .field = "body", .content_type = "application/json" });
            var ordered = try db.artifactInventoryCommand(alloc);
            defer ordered.catalogs.deinit(alloc);
            ordered.binding.effect_protocol = 15;
            var namespace: publication.Namespace = undefined;
            db_mod.doc_identity.encodeNamespace(&namespace, db.core.identity_namespace);
            const authority = blk: {
                var txn = try db.core.store.beginWriteTxn();
                errdefer txn.abort();
                const catalogs = try @import("../artifact_inventory.zig").catalogs(&txn);
                const active: publication.Authority = .{ .namespace = namespace, .epoch = 1, .catalog_digest = catalogs.digest() };
                try @import("../artifact_inventory.zig").stageOrdered(alloc, &txn, ordered, 1);
                try publication.stageAuthority(&txn, .{ .mode = .activate, .namespace = namespace, .authority_epoch = active.epoch, .catalog_digest = active.catalog_digest, .producer_name = "", .producer_generation = 0, .sources = &.{}, .mutations = &.{}, .publication_digest = @splat(0) });
                try obligations.begin(alloc, &txn, active);
                try @import("../artifact_producer_validation.zig").begin(alloc, &txn, active);
                _ = try obligations.mark(alloc, &txn, active, "doc", null);
                try txn.commit();
                break :blk active;
            };
            const before = db.core.store.lastReplaySequence(0);
            {
                // A catalog race must not leave either a journal request or
                // a dispatch marker. The next valid pass must still admit it.
                var plan = try db.core.index_manager.acquireWritePlanSnapshot();
                defer plan.release();
                var ctx = db.batchContext();
                try std.testing.expectError(error.EnrichmentSourceChanged, db_mod.appendDerivedBatchRecordContextWithWork(&ctx, .{}, false, .{
                    .authority = authority,
                    .item = .{ .document = "doc", .position = null, .revision = 1 },
                    .plan_generation = plan.generation() + 1,
                    .page = .{ .next_template = 1, .complete = true },
                }));
                try std.testing.expectEqual(before, db.core.store.lastReplaySequence(0));
            }
            {
                var snapshot = try db.core.store.beginReadTxn();
                defer snapshot.abort();
                var old = try obligations.scanWork(alloc, &snapshot, authority, null);
                defer old.deinit();
                {
                    var changed = try db.core.store.beginWriteTxn();
                    errdefer changed.abort();
                    _ = try obligations.mark(alloc, &changed, authority, "doc", null);
                    try changed.commit();
                }
                var plan = try db.core.index_manager.acquireWritePlanSnapshot();
                defer plan.release();
                const template = plan.plan().generated_templates[0];
                var ctx = db.batchContext();
                // Same catalog and primary position, but a newer dependency
                // obligation: the journal and its dispatch marker both abort.
                try std.testing.expectError(error.EnrichmentSourceChanged, db_mod.appendDerivedBatchRecordContextWithWork(&ctx, .{
                    .generated_enrichment_refs = &.{.{ .kind = template.kind, .index_name = template.index_name, .artifact_name = template.artifact_name, .embedding_name = template.embedding_name, .doc_key = "doc" }},
                }, false, .{ .authority = authority, .item = old.items[0], .plan_generation = plan.generation(), .page = .{ .next_template = 1, .complete = true } }));
                try std.testing.expectEqual(before, db.core.store.lastReplaySequence(0));
            }
            try std.testing.expect(try db.enrichment.advanceArtifactProducerWorkPage());
            expected_sequence = db.core.store.lastReplaySequence(0);
            expected_dispatch_sequence = expected_sequence;
            try std.testing.expect(expected_sequence > before);
            try std.testing.expectEqual(expected_sequence, db.enrichment.runtime.?.stats().target_sequence);
        }
        _ = try db.enrichment.advanceArtifactProducerWorkPage();
        try std.testing.expectEqual(expected_sequence, db.core.store.lastReplaySequence(0));
        {
            var read = try db.core.store.beginReadTxn();
            defer read.abort();
            const authority = (try publication.authority(&read)).?;
            var page = try obligations.scanWork(alloc, &read, authority, null);
            defer page.deinit();
            try std.testing.expectEqual(@as(usize, 1), page.items.len);
            try std.testing.expectEqual(expected_dispatch_sequence, page.items[0].dispatch_sequence);
            try std.testing.expectEqual(@as(u64, 2), page.items[0].revision);
            try std.testing.expectEqual(@as(u64, 2), (try obligations.load(&read)).?.work_revision);
            try std.testing.expectEqual(@as(u64, 1), (try obligations.load(&read)).?.pending_documents);
            try std.testing.expectError(error.ArtifactCatalogDrift, obligations.requireDrained(&read, authority));
        }
        if (pass == 0) {
            // Lose the producer's queue/upload entirely: the durable original
            // dispatch is complete, but no accepted output exists. A bounded
            // retry sweep must append work without reopening that dispatch.
            db.enrichment.artifacts.producer_scheduler.producer_retry_after_ns = 0;
            db.enrichment.artifacts.producer_scheduler.retry_after_ns.store(0, .release);
            try std.testing.expect(try db.enrichment.advanceArtifactProducerWorkPage());
            const retried_sequence = db.core.store.lastReplaySequence(0);
            try std.testing.expect(retried_sequence > expected_sequence);
            try std.testing.expectEqual(retried_sequence, db.enrichment.runtime.?.stats().target_sequence);
            expected_sequence = retried_sequence;
            var read = try db.core.store.beginReadTxn();
            defer read.abort();
            const authority = (try publication.authority(&read)).?;
            const current = (try obligations.lookupWork(alloc, &read, authority, "doc")).?;
            try std.testing.expectEqual(@as(u64, 2), current.retry_round);
            try std.testing.expectEqual(retried_sequence, current.retry_sequence);
            try std.testing.expectEqual(expected_dispatch_sequence, current.dispatch_sequence);
            try std.testing.expect(current.dispatch_complete);
            try std.testing.expectEqual(@as(u64, 1), (try obligations.load(&read)).?.pending_documents);
        }
        if (pass == 1) {
            // Crash before the retry transaction commits: neither its round
            // allocation nor its per-document cursor may survive.
            {
                var txn = try db.core.store.beginWriteTxn();
                defer txn.abort();
                const authority = (try publication.authority(&txn)).?;
                const current = (try obligations.lookupWork(alloc, &txn, authority, "doc")).?;
                try std.testing.expectEqual(expected_sequence, current.retry_sequence);
                const round = try obligations.beginRetryRound(&txn, authority);
                try std.testing.expectEqual(@as(u64, 3), round);
                try std.testing.expect(try obligations.stageRetry(alloc, &txn, authority, current, round, expected_sequence + 1, .{ .next_template = 1, .complete = true }));
            }
            db.enrichment.artifacts.producer_scheduler.producer_retry_after_ns = 0;
            db.enrichment.artifacts.producer_scheduler.retry_after_ns.store(0, .release);
            try std.testing.expect(try db.enrichment.advanceArtifactProducerWorkPage());
            expected_sequence = db.core.store.lastReplaySequence(0);
            {
                var read = try db.core.store.beginReadTxn();
                defer read.abort();
                const authority = (try publication.authority(&read)).?;
                const current = (try obligations.lookupWork(alloc, &read, authority, "doc")).?;
                try std.testing.expectEqual(@as(u64, 3), current.retry_round);
                try std.testing.expectEqual(expected_sequence, current.retry_sequence);
                try std.testing.expectEqual(expected_dispatch_sequence, current.dispatch_sequence);
            }
            // A completed local retry round cannot spin while other work
            // keeps the maintenance scheduler on its active cadence.
            db.enrichment.artifacts.producer_scheduler.retry_after_ns.store(0, .release);
            _ = try db.enrichment.advanceArtifactProducerWorkPage();
            try std.testing.expectEqual(expected_sequence, db.core.store.lastReplaySequence(0));
            // A dependency repair can retain the primary position. It must
            // reopen durable dispatch even after a process restart.
            var txn = try db.core.store.beginWriteTxn();
            errdefer txn.abort();
            const authority = (try publication.authority(&txn)).?;
            _ = try obligations.mark(alloc, &txn, authority, "doc", null);
            try txn.commit();
            db.enrichment.artifacts.producer_scheduler.retry_after_ns.store(0, .release);
            try std.testing.expect(try db.enrichment.advanceArtifactProducerWorkPage());
            try std.testing.expect(db.core.store.lastReplaySequence(0) > expected_sequence);
        }
    }
}

test "db ordered artifact inventory producer pages resume after restart without completing obligations" {
    const alloc = std.testing.allocator;
    const publication = @import("../artifact_publication.zig");
    const obligations = @import("../artifact_producer_obligations.zig");
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    const path = try std.fmt.allocPrint(alloc, ".zig-cache/tmp/{s}/paged-producer-dispatch", .{tmp.sub_path});
    defer alloc.free(path);
    const options: db_mod.OpenOptions = .{ .identity_namespace = .{ .table_id = 1, .shard_id = 2, .range_id = 2 }, .online_source_authority = .raft, .primary_backend = .{ .lsm = .{} }, .start_index_workers = false, .start_optional_runtimes = false };
    const document: [12000]u8 = @splat('x');
    var previous_sequence: u64 = 0;
    for (0..4) |pass| {
        var db = try DB.open(alloc, path, options);
        defer db.close();
        if (pass == 0) {
            try db.setSchemaJson(alloc, "{}");
            for ([_][]const u8{ "first", "second", "third" }) |name| try db.enrichment.addEnrichment(.{ .name = name, .kind = .asset, .field = "body", .content_type = "application/json" });
            var ordered = try db.artifactInventoryCommand(alloc);
            defer ordered.catalogs.deinit(alloc);
            ordered.binding.effect_protocol = 15;
            var namespace: publication.Namespace = undefined;
            db_mod.doc_identity.encodeNamespace(&namespace, db.core.identity_namespace);
            var txn = try db.core.store.beginWriteTxn();
            errdefer txn.abort();
            const catalogs = try @import("../artifact_inventory.zig").catalogs(&txn);
            const authority: publication.Authority = .{ .namespace = namespace, .epoch = 1, .catalog_digest = catalogs.digest() };
            try @import("../artifact_inventory.zig").stageOrdered(alloc, &txn, ordered, 1);
            try publication.stageAuthority(&txn, .{ .mode = .activate, .namespace = namespace, .authority_epoch = 1, .catalog_digest = authority.catalog_digest, .producer_name = "", .producer_generation = 0, .sources = &.{}, .mutations = &.{}, .publication_digest = @splat(0) });
            try obligations.begin(alloc, &txn, authority);
            try @import("../artifact_producer_validation.zig").begin(alloc, &txn, authority);
            _ = try obligations.mark(alloc, &txn, authority, &document, null);
            try txn.commit();
        }
        _ = try db.enrichment.advanceArtifactProducerWorkPage();
        const sequence = db.core.store.lastReplaySequence(0);
        if (pass < 3) try std.testing.expect(sequence > previous_sequence) else try std.testing.expectEqual(previous_sequence, sequence);
        previous_sequence = sequence;
        var read = try db.core.store.beginReadTxn();
        defer read.abort();
        const authority = (try publication.authority(&read)).?;
        var work = try obligations.scanWork(alloc, &read, authority, null);
        defer work.deinit();
        try std.testing.expectEqual(@as(usize, 1), work.items.len);
        try std.testing.expectEqual(@as(u32, @intCast(@min(pass + 1, 3))), work.items[0].next_template);
        try std.testing.expectEqual(pass >= 2, work.items[0].dispatch_complete);
        try std.testing.expectEqual(@as(u64, 1), (try obligations.load(&read)).?.pending_documents);
        try std.testing.expectError(error.ArtifactCatalogDrift, obligations.requireDrained(&read, authority));
    }
}

test "db ordered artifact inventory graph TTL definitions retain reader envelope across mutations and reopen" {
    const alloc = std.testing.allocator;
    const inventory = @import("../artifact_inventory.zig");
    var tmp = try db_mod.TestDirectory.init("ttl-inventory-envelope");
    defer tmp.cleanup();
    const Check = struct {
        fn loaded(db: *DB) !void {
            var read = try db.core.store.beginReadTxn();
            defer read.abort();
            _ = try inventory.local(&read);
            const catalogs = try inventory.catalogs(&read);
            // Shard transition admission consumes both the exact binding and
            // its definition identity, not just loaded-index matching.
            const command: inventory.Command = .{
                .namespace = @splat(7),
                .binding = .{ .epoch = 1, .digest = catalogs.digest(), .semantic_digest = try catalogs.semanticDigest(std.testing.allocator) },
                .catalogs = catalogs,
            };
            try command.validate();
            try std.testing.expectEqual(@as(u32, 4), try db_mod.resolver_catalog_mod.catalogReaderVersion(std.testing.allocator, catalogs.resolvers));
            try std.testing.expect(try db.core.index_manager.matchesArtifactInventory(catalogs));
            var plan = try db.core.index_manager.acquireWritePlanSnapshot();
            defer plan.release();
            try std.testing.expect(plan.plan().matchesArtifactInventory(catalogs));
        }
    };
    {
        var db = try DB.open(alloc, tmp.path(), .{ .start_optional_runtimes = false });
        defer db.close();
        try db.addIndex(.{ .name = "g", .kind = .graph, .config_json = "{\"ttl\":{\"duration\":\"1h\"}}" });
        try Check.loaded(&db);
        try db.enrichment.addEnrichment(.{ .name = "relations", .kind = .asset, .field = "body" });
        try Check.loaded(&db);
        try std.testing.expect(try db.deleteIndex("g"));
        // Removing the last TTL index cannot erase a permanent format fence.
        try Check.loaded(&db);
    }
    {
        var db = try DB.open(alloc, tmp.path(), .{ .start_optional_runtimes = false });
        defer db.close();
        try Check.loaded(&db);
    }
    var readonly = try DB.open(alloc, tmp.path(), .{ .open_mode = .query_readonly });
    defer readonly.close();
    try Check.loaded(&readonly);
}

test "db ordered artifact inventory resolver commit invalidates pinned producer catalog" {
    const alloc = std.testing.allocator;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    const path = try std.fmt.allocPrint(alloc, ".zig-cache/tmp/{s}/producer-catalog-resolver", .{tmp.sub_path});
    defer alloc.free(path);
    var db = try DB.open(alloc, path, .{ .primary_backend = .{ .lsm = .{} }, .start_index_workers = false, .start_optional_runtimes = false });
    defer db.close();
    try db.setSchemaJson(alloc, "{}");
    try db.enrichment.addEnrichment(.{ .name = "extraction", .kind = .asset, .field = "body", .content_type = "application/json" });
    var before = try db.core.index_manager.acquireWritePlanSnapshot();
    defer before.release();
    try db.addResolver(.{ .name = "people", .table = "entities", .source_artifact = "extraction", .resolution_artifact = "resolved", .key_template = "{{canonical_name}}", .config_generation = 7 });
    var after = try db.core.index_manager.acquireWritePlanSnapshot();
    defer after.release();
    try std.testing.expect(after.generation() > before.generation());
    var read = try db.core.store.beginReadTxn();
    defer read.abort();
    const current = try @import("../artifact_inventory.zig").catalogs(&read);
    try std.testing.expect(!before.plan().matchesArtifactInventory(current));
    try std.testing.expect(after.plan().matchesArtifactInventory(current));
    try std.testing.expect(before.plan().matchesArtifactInventory(before.plan().artifact_catalogs.?));
}

test "db graph fact materialized scopes match journal" {
    const alloc = std.testing.allocator;
    var tmp = try db_mod.TestDirectory.init("review-scope");
    defer tmp.cleanup();
    var db = try DB.open(alloc, tmp.path(), .{ .start_optional_runtimes = false, .start_index_workers = false });
    defer db.close();
    try db.addIndex(.{ .name = "g", .kind = .graph, .config_json = "{}" });
    try db.enrichment.addEnrichment(.{ .name = "context", .kind = .asset, .field = "body", .producer_json = "{\"type\":\"generator\",\"config\":{\"provider\":\"antfly\",\"model\":\"test-generator\"}}", .neighbor_context = .{ .graph_index = "g", .direction = .out } });
    try db.addIndex(.{ .name = "h", .kind = .graph, .config_json = "{\"sources\":[{\"artifact\":\"context\"}]}" });
    try db.addIndex(.{ .name = "plain", .kind = .full_text, .config_json = "{}" });
    const req: db_mod.types.BatchRequest = .{ .graph_writes = &.{.{ .index_name = "g", .source = "a", .target = "b", .edge_type = "R", .edge_id = "f", .owner_document = "f" }} };
    const batch: db_mod.derived_types.DerivedBatch = .{ .graph_writes = req.graph_writes };
    const payload = try db_mod.encodeDirectChangeRecordPayload(&db.batchContext(), batch, 1, req, null);
    defer alloc.free(payload);
    var record = try db_mod.change_journal_mod.decodeRecord(alloc, payload);
    defer record.deinit();
    var planned = try db_mod.prepareDirectChangeRecord(&db.batchContext(), batch, 1, req, null);
    defer db_mod.change_journal_mod.deinitRecord(alloc, &planned);
    var from_batch = try db_mod.collectManagedSyncTargetsForRecordWithBatch(alloc, db.core.index_manager, planned, batch);
    defer from_batch.deinit(alloc);
    try std.testing.expectEqual(@as(usize, 0), from_batch.full_text_indexes.len);
    var from_record = try db_mod.collectManagedSyncTargetsForRecord(alloc, db.core.index_manager, record.record);
    defer from_record.deinit(alloc);
    var has_batch = false;
    for (from_batch.target_identities) |identity| try std.testing.expect(!std.mem.eql(u8, identity.index_name, "plain"));
    for (from_batch.target_identities) |i| {
        if (std.mem.eql(u8, i.index_name, "h")) {
            has_batch = true;
            try std.testing.expectEqual(db_mod.IndexTargetVisibility.ServingSetEffect.may_reduce, i.serving_set_effect);
        }
    }
    var has_record = false;
    for (from_record.target_identities) |i| {
        if (std.mem.eql(u8, i.index_name, "h")) has_record = true;
    }
    try std.testing.expect(has_record);
    try std.testing.expectEqual(has_record, has_batch);

    const Observer = struct {
        saw_generated_consumer: bool = false,
        scope_known: bool = false,
        fn changed(ptr: *anyopaque, event: db_mod.QueryVisibilityEvent) void {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            if (event.target_sequence == null) return;
            self.scope_known = event.target_scope_known;
            for (event.target_indexes) |identity| {
                if (std.mem.eql(u8, identity.index_name, "h") and identity.serving_set_effect == .may_reduce)
                    self.saw_generated_consumer = true;
            }
        }
    };
    var observer = Observer{};
    db.setQueryVisibilityHook(.{ .ptr = &observer, .on_change = Observer.changed });
    defer db.setQueryVisibilityHook(null);
    try db.batch(.{ .graph_writes = req.graph_writes, .sync_level = .write });
    try std.testing.expect(observer.scope_known);
    try std.testing.expect(observer.saw_generated_consumer);
}

test "db graph fact enrichment observes pending graph replay" {
    const alloc = std.testing.allocator;
    var tmp = try db_mod.TestDirectory.init("review-neighbor-order");
    defer tmp.cleanup();
    var producer = db_mod.GateAssetProducer{};
    producer.allowAll();
    var db = try DB.open(alloc, tmp.path(), .{ .start_optional_runtimes = false, .start_index_workers = false });
    defer db.close();
    try db.addIndex(.{ .name = "g", .kind = .graph, .config_json = "{}" });
    try db.enrichment.addEnrichment(.{ .name = "context", .kind = .asset, .field = "body", .content_type = "text/plain", .producer_json = "{\"type\":\"generator\",\"config\":{\"provider\":\"antfly\",\"model\":\"test-generator\"}}", .neighbor_context = .{ .graph_index = "g", .direction = .out } });
    var config: db_mod.enrichment_runtime_mod.Config = .{ .asset_producer = producer.interface() };
    try db.enrichment.initRuntime(&config);
    try db.batch(.{ .writes = &.{.{ .key = "a", .value = "{\"body\":\"Alice\"}" }}, .sync_level = .write });
    try db.enrichment.runtime.?.catchUpUntilForDrain(db.core.nextDerivedSequence());
    try db.runDerivedUntil(db.core.nextDerivedSequence());
    const previous = producer.successful_requests.load(.acquire);
    try db.batch(.{ .graph_writes = &.{.{ .index_name = "g", .source = "a", .target = "b", .edge_type = "R", .edge_id = "f", .owner_document = "f" }}, .sync_level = .write });
    const pending = db.core.nextDerivedSequence();
    const graph_checkpoint = try db.core.loadAppliedSequence(alloc, "g");
    try db.enrichment.runtime.?.catchUpUntilForDrain(pending);
    // Source refresh must not certify this consumer while its producer runs.
    try std.testing.expectEqual(graph_checkpoint, try db.core.loadAppliedSequence(alloc, "g"));
    try std.testing.expect(producer.successful_requests.load(.acquire) > previous);
    const RefreshAllocationFixture = struct {
        fn run(failing_alloc: Allocator, resident: *DB) !void {
            var ctx = resident.async_context.*;
            ctx.alloc = failing_alloc;
            var append_ctx = resident.enrichment.append_context.?.*;
            append_ctx.async_context = &ctx;
            resident.core.index_manager.graphIndex("g").?.invalidateNeighborSource();
            while (true) {
                const lease = db_mod.acquireNeighborContextGraphSource(&append_ctx, "g") catch |err| {
                    if (err == error.GraphSourceReplayPending) continue;
                    return err;
                };
                lease.release();
                break;
            }
        }
    };
    try @import("antfly_platform").allocator.checkAllAllocationFailures(alloc, RefreshAllocationFixture.run, .{&db});
    // Simulate an older consumer window withdrawing this owner after a source
    // refresh. Invalidation must make the next sample restore current primary
    // effects even though no new primary revision has arrived.
    try db.core.index_manager.deleteGraphDocsByName("g", &.{"f"});
    try std.testing.expectEqual(@as(u64, 0), db.core.index_manager.graphIndex("g").?.neighbor_source_sequence);
    {
        const source_cap = db.enrichment.runtime.?.config.neighbor_context_graph_source.?;
        const lease = try source_cap.acquire_fn(source_cap.ptr, "g");
        defer lease.release();
        const restored = try db.core.index_manager.graphIndex("g").?.index.getEdges(alloc, "a", "R", .out);
        defer db_mod.graph_mod.GraphIndex.freeEdges(alloc, restored);
        try std.testing.expectEqual(@as(usize, 1), restored.len);
    }
    try db.runDerivedUntil(db.core.nextDerivedSequence());
    try db.enrichment.runtime.?.catchUpUntilForDrain(db.core.nextDerivedSequence());
    const current = producer.successful_requests.load(.acquire);

    try std.testing.expect(current > previous);

    try db.batch(.{ .graph_deletes = &.{.{ .index_name = "g", .source = "a", .target = "b", .edge_type = "R", .edge_id = "f", .owner_document = "f" }}, .sync_level = .write });
    try db.enrichment.runtime.?.catchUpUntilForDrain(db.core.nextDerivedSequence());
    try std.testing.expect(producer.successful_requests.load(.acquire) > current);
    const edges = try db.core.index_manager.graphIndex("g").?.index.getEdges(alloc, "a", "R", .out);
    defer db_mod.graph_mod.GraphIndex.freeEdges(alloc, edges);
    try std.testing.expectEqual(@as(usize, 0), edges.len);
}

test "db graph fact inline mutation wakes incoming producers for additions replacement removal and deletion" {
    const Fixture = struct {
        fn produceBounded(ptr: *anyopaque, alloc: Allocator, request: db_mod.asset_producer_mod.Request, context: db_mod.asset_producer_mod.InvocationContext) ![]u8 {
            try context.check();
            return db_mod.GateAssetProducer.produce(ptr, alloc, request);
        }
        fn drain(db: *DB) !void {
            try db.enrichment.runtime.?.catchUpUntilForDrain(db.core.nextDerivedSequence());
            try db.runDerivedUntil(db.core.nextDerivedSequence());
            try db.enrichment.runtime.?.catchUpUntilForDrain(db.core.nextDerivedSequence());
        }
        fn hasNeighbor(db: *DB, doc: []const u8) !bool {
            const key = try db_mod.internal_keys.artifactNamedPrefixAlloc(db.alloc, doc, "asset", "context");
            defer db.alloc.free(key);
            const value = try db.core.store.get(db.alloc, key);
            defer db.alloc.free(value);
            return std.mem.indexOf(u8, value, "\"target\":\"a\"") != null;
        }
    };
    const alloc = std.testing.allocator;
    for ([_]db_mod.types.SyncLevel{ .write, .full_index }) |sync_level| {
        var tmp = try db_mod.TestDirectory.init("inline-neighbor-lifecycle");
        defer tmp.cleanup();
        var producer = db_mod.GateAssetProducer{};
        producer.allowAll();
        var db = try DB.open(alloc, tmp.path(), .{ .start_optional_runtimes = false, .start_index_workers = false });
        defer db.close();
        try db.addIndex(.{ .name = "g", .kind = .graph, .config_json = "{}" });
        try db.enrichment.addEnrichment(.{ .name = "context", .kind = .asset, .field = "body", .content_type = "text/plain", .producer_json = "{\"type\":\"generator\",\"config\":{\"provider\":\"antfly\",\"model\":\"test-generator\"}}", .neighbor_context = .{ .graph_index = "g", .direction = .in } });
        var config: db_mod.enrichment_runtime_mod.Config = .{ .asset_producer = .{ .ptr = &producer, .vtable = &.{
            .produce = db_mod.GateAssetProducer.produce,
            .produce_with_context = Fixture.produceBounded,
            .foreground_bounded = true,
            .invocation_memory_for_requests = db_mod.GateAssetProducer.invocationMemory,
        } } };
        try db.enrichment.initRuntime(&config);
        try db.batch(.{ .writes = &.{ .{ .key = "b", .value = "{\"body\":\"Bob\"}" }, .{ .key = "c", .value = "{\"body\":\"Carol\"}" } }, .sync_level = sync_level });
        try Fixture.drain(&db);
        try db.batch(.{ .writes = &.{.{ .key = "a", .value = "{\"_edges\":{\"g\":{\"R\":[{\"target\":\"b\",\"edge_id\":\"f\"}]}}}" }}, .sync_level = sync_level });
        try Fixture.drain(&db);
        try std.testing.expect(try Fixture.hasNeighbor(&db, "b"));
        // A synchronous source-field update must retain committed adjacency
        // in producer input and its skip hash, just like asynchronous replay.
        try db.batch(.{ .writes = &.{.{ .key = "b", .value = "{\"body\":\"Bob revised\"}" }}, .sync_level = sync_level });
        try Fixture.drain(&db);
        try std.testing.expect(try Fixture.hasNeighbor(&db, "b"));
        try db.batch(.{ .writes = &.{.{ .key = "a", .value = "{\"_edges\":{\"g\":{\"R\":[{\"target\":\"c\",\"edge_id\":\"f\"}]}}}" }}, .sync_level = sync_level });
        try Fixture.drain(&db);
        try std.testing.expect(!try Fixture.hasNeighbor(&db, "b"));
        try std.testing.expect(try Fixture.hasNeighbor(&db, "c"));
        try db.batch(.{ .writes = &.{.{ .key = "a", .value = "{\"_edges\":{\"g\":{\"R\":[]}}}" }}, .sync_level = sync_level });
        try Fixture.drain(&db);
        try std.testing.expect(!try Fixture.hasNeighbor(&db, "c"));
        try db.batch(.{ .writes = &.{.{ .key = "a", .value = "{\"_edges\":{\"g\":{\"R\":[{\"target\":\"b\",\"edge_id\":\"f\"}]}}}" }}, .sync_level = sync_level });
        try Fixture.drain(&db);
        try db.batch(.{ .deletes = &.{"a"}, .sync_level = sync_level });
        try Fixture.drain(&db);
        try std.testing.expect(!try Fixture.hasNeighbor(&db, "b"));
    }
}

test "db graph fact neighbor context excludes foreign source namespace" {
    const alloc = std.testing.allocator;
    var tmp = try db_mod.TestDirectory.init("review-foreign-neighbor");
    defer tmp.cleanup();
    var producer = db_mod.GateAssetProducer{};
    producer.allowAll();
    var db = try DB.open(alloc, tmp.path(), .{ .start_optional_runtimes = false, .start_index_workers = false });
    defer db.close();
    try db.core.store.put(db_mod.internal_keys.graph_owning_table_key, "here");
    db.local_execution.row_policy_table_name = "here";
    try db.addIndex(.{ .name = "g", .kind = .graph, .config_json = "{}" });
    try db.enrichment.addEnrichment(.{ .name = "context", .kind = .asset, .field = "body", .content_type = "text/plain", .producer_json = "{\"type\":\"generator\",\"config\":{\"provider\":\"antfly\",\"model\":\"test-generator\"}}", .neighbor_context = .{ .graph_index = "g", .direction = .out } });
    var config: db_mod.enrichment_runtime_mod.Config = .{ .asset_producer = producer.interface() };
    try db.enrichment.initRuntime(&config);
    try db.batch(.{ .writes = &.{.{ .key = "a", .value = "{\"body\":\"Alpha\"}" }}, .sync_level = .write });
    try db.enrichment.runtime.?.catchUpUntilForDrain(db.core.nextDerivedSequence());
    try db.runDerivedUntil(db.core.nextDerivedSequence());
    try db.batch(.{ .graph_writes = &.{.{ .index_name = "g", .source = "a", .target = "foreign-source-neighbor", .edge_type = "R", .edge_id = "f", .owner_document = "f", .metadata_json = "{\"source_table\":\"foreign\",\"target_table\":\"here\"}" }}, .sync_level = .write });
    try db.runDerivedUntil(db.core.nextDerivedSequence());
    const path = try db.findShortestPathWithOptions(alloc, "g", "a", "foreign-source-neighbor", .{ .owning_table = "here" });
    defer if (path) |value| db_mod.paths_mod.freePath(alloc, value);
    try std.testing.expect(path == null);
    try db.batch(.{ .writes = &.{.{ .key = "a", .value = "{\"body\":\"Alpha revised\"}" }}, .sync_level = .write });
    try db.enrichment.runtime.?.catchUpUntilForDrain(db.core.nextDerivedSequence());
    const key = try db_mod.internal_keys.artifactNamedPrefixAlloc(alloc, "a", "asset", "context");
    defer alloc.free(key);
    const output = try db.core.store.get(alloc, key);
    defer alloc.free(output);
    try std.testing.expect(std.mem.indexOf(u8, output, "foreign-source-neighbor") == null);
}

test "db graph fact neighbor refresh resumes bounded turns without certifying consumer coverage" {
    const alloc = std.testing.allocator;
    var tmp = try db_mod.TestDirectory.init("neighbor-refresh-turns");
    defer tmp.cleanup();
    var db = try DB.open(alloc, tmp.path(), .{ .start_optional_runtimes = false, .start_index_workers = false });
    defer db.close();
    try db.addIndex(.{ .name = "g", .kind = .graph, .config_json = "{}" });
    try db.enrichment.addEnrichment(.{ .name = "context", .kind = .asset, .field = "body", .producer_json = "{\"type\":\"generator\",\"config\":{\"provider\":\"antfly\",\"model\":\"test-generator\"}}", .neighbor_context = .{ .graph_index = "g", .direction = .out } });
    var producer = db_mod.GateAssetProducer{};
    producer.allowAll();
    var config: db_mod.enrichment_runtime_mod.Config = .{ .asset_producer = producer.interface() };
    try db.enrichment.initRuntime(&config);
    const record_count = 17;
    for (0..record_count) |i| {
        try db.batch(.{ .graph_writes = &.{.{ .index_name = "g", .source = "a", .target = "b", .edge_type = "R", .edge_id = "f", .owner_document = "f", .weight = @floatFromInt(i + 1) }}, .sync_level = .write });
    }
    const checkpoint = try db.core.loadAppliedSequence(alloc, "g");
    const source = db.enrichment.runtime.?.config.neighbor_context_graph_source.?;
    const entry = db.core.index_manager.graphIndex("g").?;
    var previous = entry.neighbor_source_sequence;
    var turns: usize = 0;
    while (true) {
        const lease = source.acquire_fn(source.ptr, "g") catch |err| {
            try std.testing.expectEqual(error.GraphSourceReplayPending, err);
            try std.testing.expect(entry.neighbor_source_sequence > previous);
            previous = entry.neighbor_source_sequence;
            turns += 1;
            try std.testing.expect(turns <= record_count);
            // The acquisition released publication/catalog/index locks on yield.
            var publication = db.core.index_manager.beginGraphPrimaryMutation();
            publication.release();
            try std.testing.expectEqual(checkpoint, try db.core.loadAppliedSequence(alloc, "g"));
            continue;
        };
        lease.release();
        break;
    }
    try std.testing.expect(turns >= 4);
    try std.testing.expectEqual(db.core.store.lastReplaySequence(0), entry.neighbor_source_sequence);
    try std.testing.expectEqual(checkpoint, try db.core.loadAppliedSequence(alloc, "g"));
    const edges = try entry.index.getEdges(alloc, "a", "R", .out);
    defer db_mod.graph_mod.GraphIndex.freeEdges(alloc, edges);
    try std.testing.expectEqual(@as(usize, 1), edges.len);
    try std.testing.expectEqual(@as(f64, record_count), edges[0].weight);
}

test "db graph fact neighbor context resolves equal-key qualified reverse orientation" {
    const alloc = std.testing.allocator;
    for ([_]db_mod.types.EnrichmentNeighborContextConfig{ .{ .graph_index = "g", .direction = .in }, .{ .graph_index = "g", .direction = .both } }) |neighbor_context| {
        var tmp = try db_mod.TestDirectory.init("qualified-reverse-neighbor");
        defer tmp.cleanup();
        var producer = db_mod.GateAssetProducer{};
        producer.allowAll();
        var db = try DB.open(alloc, tmp.path(), .{ .start_optional_runtimes = false, .start_index_workers = false });
        defer db.close();
        try db.core.store.put(db_mod.internal_keys.graph_owning_table_key, "here");
        db.local_execution.row_policy_table_name = "here";
        try db.addIndex(.{ .name = "g", .kind = .graph, .config_json = "{}" });
        try db.enrichment.addEnrichment(.{ .name = "context", .kind = .asset, .field = "body", .content_type = "text/plain", .producer_json = "{\"type\":\"generator\",\"config\":{\"provider\":\"antfly\",\"model\":\"test-generator\"}}", .neighbor_context = neighbor_context });
        var config: db_mod.enrichment_runtime_mod.Config = .{ .asset_producer = producer.interface() };
        try db.enrichment.initRuntime(&config);
        try db.batch(.{ .writes = &.{.{ .key = "a", .value = "{\"body\":\"Alpha\"}" }}, .sync_level = .write });
        try db.enrichment.runtime.?.catchUpUntilForDrain(db.core.nextDerivedSequence());
        try db.runDerivedUntil(db.core.nextDerivedSequence());
        try db.batch(.{ .graph_writes = &.{.{ .index_name = "g", .source = "a", .target = "a", .edge_type = "R", .edge_id = "f", .owner_document = "f", .metadata_json = "{\"source_table\":\"foreign\",\"target_table\":\"h\\u0065re\"}" }}, .sync_level = .write });
        try db.enrichment.runtime.?.catchUpUntilForDrain(db.core.nextDerivedSequence());
        const key = try db_mod.internal_keys.artifactNamedPrefixAlloc(alloc, "a", "asset", "context");
        defer alloc.free(key);
        const output = try db.core.store.get(alloc, key);
        defer alloc.free(output);
        try std.testing.expect(std.mem.indexOf(u8, output, "\"direction\":\"in\"") != null);
        try std.testing.expect(std.mem.indexOf(u8, output, "\"direction\":\"out\"") == null);
    }
}

test "db graph fact synchronous neighbor downstream chain" {
    const Fixture = struct {
        fn produceBounded(ptr: *anyopaque, alloc: Allocator, request: db_mod.asset_producer_mod.Request, context: db_mod.asset_producer_mod.InvocationContext) ![]u8 {
            try context.check();
            return db_mod.GateAssetProducer.produce(ptr, alloc, request);
        }
        fn drain(db: *DB) !void {
            try db.enrichment.runtime.?.catchUpUntilForDrain(db.core.nextDerivedSequence());
            try db.runDerivedUntil(db.core.nextDerivedSequence());
            try db.enrichment.runtime.?.catchUpUntilForDrain(db.core.nextDerivedSequence());
        }
        fn hasNeighbor(db: *DB, doc: []const u8) !bool {
            const key = try db_mod.internal_keys.artifactNamedPrefixAlloc(db.alloc, doc, "asset", "context");
            defer db.alloc.free(key);
            const value = try db.core.store.get(db.alloc, key);
            defer db.alloc.free(value);
            const downstream_key = try db_mod.internal_keys.artifactNamedPrefixAlloc(db.alloc, doc, "asset", "copied_context");
            defer db.alloc.free(downstream_key);
            const downstream = try db.core.store.get(db.alloc, downstream_key);
            defer db.alloc.free(downstream);
            try std.testing.expectEqualStrings(value, downstream);
            const mirror_key = try db_mod.internal_keys.artifactNamedPrefixAlloc(db.alloc, doc, "asset", "mirrored_context");
            defer db.alloc.free(mirror_key);
            const mirror = try db.core.store.get(db.alloc, mirror_key);
            defer db.alloc.free(mirror);
            try std.testing.expectEqualStrings(value, mirror);
            return std.mem.indexOf(u8, value, "\"target\":\"a\"") != null;
        }
    };
    const alloc = std.testing.allocator;
    for ([_]db_mod.types.SyncLevel{.full_index}) |sync_level| {
        var tmp = try db_mod.TestDirectory.init("inline-neighbor-lifecycle");
        defer tmp.cleanup();
        var producer = db_mod.GateAssetProducer{};
        producer.allowAll();
        var db = try DB.open(alloc, tmp.path(), .{ .start_optional_runtimes = false, .start_index_workers = false });
        defer db.close();
        try db.addIndex(.{ .name = "g", .kind = .graph, .config_json = "{}" });
        try db.enrichment.addEnrichment(.{ .name = "context", .kind = .asset, .field = "body", .content_type = "text/plain", .producer_json = "{\"type\":\"generator\",\"config\":{\"provider\":\"antfly\",\"model\":\"test-generator\"}}", .neighbor_context = .{ .graph_index = "g", .direction = .in } });
        try db.enrichment.addEnrichment(.{ .name = "copied_context", .kind = .asset, .source_artifact_name = "context", .producer_json = "{\"type\":\"copy\"}" });
        try db.enrichment.addEnrichment(.{ .name = "mirrored_context", .kind = .asset, .source_artifact_name = "copied_context", .producer_json = "{\"type\":\"copy\"}" });
        var config: db_mod.enrichment_runtime_mod.Config = .{ .asset_producer = .{ .ptr = &producer, .vtable = &.{
            .produce = db_mod.GateAssetProducer.produce,
            .produce_with_context = Fixture.produceBounded,
            .foreground_bounded = true,
            .invocation_memory_for_requests = db_mod.GateAssetProducer.invocationMemory,
        } } };
        try db.enrichment.initRuntime(&config);
        try db.batch(.{ .writes = &.{ .{ .key = "b", .value = "{\"body\":\"Bob\"}" }, .{ .key = "c", .value = "{\"body\":\"Carol\"}" } }, .sync_level = sync_level });
        try Fixture.drain(&db);
        try db.batch(.{ .writes = &.{.{ .key = "a", .value = "{\"_edges\":{\"g\":{\"R\":[{\"target\":\"b\",\"edge_id\":\"f\"}]}}}" }}, .sync_level = sync_level });
        try Fixture.drain(&db);
        try std.testing.expect(try Fixture.hasNeighbor(&db, "b"));

        // A synchronous source-field update must retain committed adjacency
        // in producer input and its skip hash, just like asynchronous replay.
        try db.batch(.{ .writes = &.{.{ .key = "b", .value = "{\"body\":\"Bob revised\"}" }}, .sync_level = sync_level });
        try Fixture.drain(&db);
        try std.testing.expect(try Fixture.hasNeighbor(&db, "b"));
        try db.batch(.{ .writes = &.{.{ .key = "a", .value = "{\"_edges\":{\"g\":{\"R\":[{\"target\":\"c\",\"edge_id\":\"f\"}]}}}" }}, .sync_level = sync_level });
        try Fixture.drain(&db);
        try std.testing.expect(!try Fixture.hasNeighbor(&db, "b"));
        try std.testing.expect(try Fixture.hasNeighbor(&db, "c"));
        try db.batch(.{ .writes = &.{.{ .key = "a", .value = "{\"_edges\":{\"g\":{\"R\":[]}}}" }}, .sync_level = sync_level });
        try Fixture.drain(&db);
        try std.testing.expect(!try Fixture.hasNeighbor(&db, "c"));
        try db.batch(.{ .writes = &.{.{ .key = "a", .value = "{\"_edges\":{\"g\":{\"R\":[{\"target\":\"b\",\"edge_id\":\"f\"}]}}}" }}, .sync_level = sync_level });
        try Fixture.drain(&db);
        try db.batch(.{ .deletes = &.{"a"}, .sync_level = sync_level });
        try Fixture.drain(&db);
        try std.testing.expect(!try Fixture.hasNeighbor(&db, "b"));
    }
}

test "db graph fact generated projection wakes endpoint context" {
    const alloc = std.testing.allocator;
    var tmp = try db_mod.TestDirectory.init("review-generated-fact-context");
    defer tmp.cleanup();
    var producer = db_mod.GateAssetProducer{};
    producer.allowAll();
    var db = try DB.open(alloc, tmp.path(), .{ .start_optional_runtimes = false, .start_index_workers = false });
    defer db.close();
    try db.addIndex(.{ .name = "facts", .kind = .graph, .config_json =
        \\{"source":{"artifact":"relations_v1","path":"$","nodes":{"source":"{{ _item.source }}","target":"{{ _item.target }}"},"edge":{"edge_id":"{{ _doc.key }}","type":"RELATES_TO"}},"artifact":{"name":"relations_v1","kind":"asset","source":{"type":"field","value":"relations"},"content_type":"application/json"}}
    });
    try db.enrichment.addEnrichment(.{ .name = "context", .kind = .asset, .field = "body", .producer_json = "{\"type\":\"generator\",\"config\":{\"provider\":\"antfly\",\"model\":\"test-generator\"}}", .neighbor_context = .{ .graph_index = "facts", .direction = .out } });
    var config: db_mod.enrichment_runtime_mod.Config = .{ .asset_producer = producer.interface() };
    try db.enrichment.initRuntime(&config);
    try db.batch(.{ .writes = &.{.{ .key = "alice", .value = "{\"body\":\"Alice\"}" }}, .sync_level = .write });
    try db.enrichment.runtime.?.catchUpUntilForDrain(db.core.nextDerivedSequence());
    try db.runDerivedUntil(db.core.nextDerivedSequence());
    try db.batch(.{ .writes = &.{.{ .key = "fact:works", .value = "{\"relations\":[{\"source\":\"alice\",\"target\":\"acme\"}]}" }}, .sync_level = .write });
    for (0..3) |_| {
        try db.enrichment.runtime.?.catchUpUntilForDrain(db.core.nextDerivedSequence());
        try db.runDerivedUntil(db.core.nextDerivedSequence());
    }
    const entry = db.core.index_manager.graphIndex("facts").?;
    const edges = try entry.index.getEdges(alloc, "alice", "RELATES_TO", .out);
    defer db_mod.graph_mod.GraphIndex.freeEdges(alloc, edges);
    try std.testing.expectEqual(@as(usize, 1), edges.len);
    const key = try db_mod.internal_keys.artifactNamedPrefixAlloc(alloc, "alice", "asset", "context");
    defer alloc.free(key);
    const output = try db.core.store.get(alloc, key);
    defer alloc.free(output);
    try std.testing.expect(std.mem.indexOf(u8, output, "acme") != null);
}

test "db graph fact refresh bounds artifact work within one large record" {
    var checked_allocator = try @import("antfly_platform").allocator.CheckedTestAllocator.init();
    defer checked_allocator.deinit();
    const alloc = checked_allocator.allocator();
    var tmp = try db_mod.TestDirectory.init("review-large-record-refresh");
    defer tmp.cleanup();
    var db = try DB.open(alloc, tmp.path(), .{ .start_optional_runtimes = false, .start_index_workers = false });
    defer db.close();
    try db.addIndex(.{ .name = "g", .kind = .graph, .config_json = "{}" });
    try db.enrichment.addEnrichment(.{ .name = "context", .kind = .asset, .field = "body", .producer_json = "{\"type\":\"generator\",\"config\":{\"provider\":\"antfly\",\"model\":\"test-generator\"}}", .neighbor_context = .{ .graph_index = "g", .direction = .out } });
    var producer = db_mod.GateAssetProducer{};
    producer.allowAll();
    var config: db_mod.enrichment_runtime_mod.Config = .{ .asset_producer = producer.interface() };
    try db.enrichment.initRuntime(&config);
    var arena = std.heap.ArenaAllocator.init(alloc);
    defer arena.deinit();
    const a = arena.allocator();
    const writes = try a.alloc(db_mod.types.GraphEdgeWrite, 1025);
    for (writes, 0..) |*write, i| write.* = .{ .index_name = "g", .source = "a", .target = "b", .edge_type = "R", .edge_id = try std.fmt.allocPrint(a, "f{d}", .{i}), .owner_document = "owner" };
    try db.batch(.{ .graph_writes = writes, .sync_level = .write });
    const before_entry = db.core.index_manager.graphIndex("g").?;
    const before_edges = try before_entry.index.getEdges(alloc, "a", "R", .out);
    defer db_mod.graph_mod.GraphIndex.freeEdges(alloc, before_edges);
    try std.testing.expectEqual(@as(usize, 0), before_edges.len);
    const tip = db.core.store.lastReplaySequence(0);
    try std.testing.expect((try db.core.loadAppliedSequence(alloc, "g")) < tip);
    try std.testing.expect((try before_entry.index.artifactRebuildSequence()) < tip);
    const source = db.enrichment.runtime.?.config.neighbor_context_graph_source.?;
    const checkpoint = try db.core.loadAppliedSequence(alloc, "g");
    try std.testing.expectError(error.GraphSourceReplayPending, source.acquire_fn(source.ptr, "g"));
    try std.testing.expect(before_entry.neighbor_refresh.sequence != 0);
    // An ordinary replay mutation discards the partial source view. A restart
    // must replay the record from its beginning rather than skipping lost rows.
    var stale = writes[0];
    stale.weight = 9;
    try db.core.index_manager.applyGraphMutationsByName("g", &.{stale}, &.{});
    try std.testing.expectEqual(@as(u64, 0), before_entry.neighbor_refresh.sequence);
    var turns: usize = 0;
    while (turns < 32) : (turns += 1) {
        const lease = source.acquire_fn(source.ptr, "g") catch |err| {
            if (err == error.GraphSourceReplayPending) continue;
            return err;
        };
        lease.release();
        break;
    }
    try std.testing.expect(turns < 32);
    try std.testing.expectEqual(checkpoint, try db.core.loadAppliedSequence(alloc, "g"));
    try std.testing.expectEqual(@as(u64, 0), before_entry.neighbor_refresh.sequence);
    const entry = db.core.index_manager.graphIndex("g").?;
    const edges = try entry.index.getEdges(alloc, "a", "R", .out);
    defer db_mod.graph_mod.GraphIndex.freeEdges(alloc, edges);
    try std.testing.expectEqual(@as(usize, 1025), edges.len);
}

test "db graph fact neighbor dependency admission rejects indirect feedback" {
    const alloc = std.testing.allocator;
    var tmp = try db_mod.TestDirectory.init("neighbor-feedback");
    defer tmp.cleanup();
    var db = try DB.open(alloc, tmp.path(), .{ .start_optional_runtimes = false, .start_index_workers = false });
    defer db.close();
    try db.enrichment.addEnrichment(.{ .name = "relations", .kind = .asset, .field = "body", .producer_json = "{\"type\":\"generator\",\"config\":{\"provider\":\"antfly\",\"model\":\"test-generator\"}}", .neighbor_context = .{ .graph_index = "facts", .direction = .out } });
    try db.enrichment.addEnrichment(.{ .name = "copied_relations", .kind = .asset, .source_artifact_name = "relations", .producer_json = "{\"type\":\"copy\"}" });
    try std.testing.expectError(error.InvalidEnrichmentConfig, db.addIndex(.{ .name = "facts", .kind = .graph, .config_json = "{\"source\":{\"artifact\":\"copied_relations\"}}" }));
    try std.testing.expect(db.core.index_manager.graphIndex("facts") == null);
}

test "db graph fact failed neighbor producer blocks downstream stale artifact coverage" {
    const Fixture = struct {
        fail: bool = false,
        fn produce(ptr: *anyopaque, alloc: Allocator, request: db_mod.asset_producer_mod.Request) ![]u8 {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            if (self.fail) return error.InvalidExtractorResponse;
            return std.fmt.allocPrint(alloc, "asset:{s}", .{request.source_text});
        }
    };
    const alloc = std.testing.allocator;
    var tmp = try db_mod.TestDirectory.init("neighbor-chain-failure");
    defer tmp.cleanup();
    var fixture: Fixture = .{};
    var db = try DB.open(alloc, tmp.path(), .{ .start_optional_runtimes = false, .start_index_workers = false });
    defer db.close();
    try db.addIndex(.{ .name = "g", .kind = .graph, .config_json = "{}" });
    try db.enrichment.addEnrichment(.{ .name = "context", .kind = .asset, .field = "body", .producer_json = "{\"type\":\"generator\",\"config\":{\"provider\":\"antfly\",\"model\":\"test-generator\"}}", .neighbor_context = .{ .graph_index = "g", .direction = .out } });
    try db.enrichment.addEnrichment(.{ .name = "copied_context", .kind = .asset, .source_artifact_name = "context", .producer_json = "{\"type\":\"copy\"}" });
    var config: db_mod.enrichment_runtime_mod.Config = .{ .asset_producer = .{ .ptr = &fixture, .vtable = &.{ .produce = Fixture.produce, .invocation_memory_for_requests = db_mod.GateAssetProducer.invocationMemory } } };
    try db.enrichment.initRuntime(&config);
    try db.batch(.{ .writes = &.{.{ .key = "a", .value = "{\"body\":\"old\"}" }}, .sync_level = .write });
    try db.enrichment.runtime.?.catchUpUntilForDrain(db.core.nextDerivedSequence());
    fixture.fail = true;
    try db.batch(.{ .writes = &.{.{ .key = "a", .value = "{\"body\":\"new\"}" }}, .sync_level = .write });
    try std.testing.expectError(error.EnrichmentWorkerFailed, db.enrichment.runtime.?.catchUpUntilForDrain(db.core.nextDerivedSequence()));
    const issues = try db.listArtifactRepairIssues(alloc, .asset, null, 0);
    defer db_mod.types.freeArtifactRepairIssues(alloc, issues);
    const downstream_failed = for (issues) |issue| {
        if (std.mem.eql(u8, issue.artifact_name, "copied_context") and issue.reason == .enrichment_failed and std.mem.eql(u8, issue.generation_error, "ArtifactDependencyFailed")) break true;
    } else false;
    try std.testing.expect(downstream_failed);
}

test "db graph fact orphan stages recover after reopen with no replay debt" {
    const alloc = std.testing.allocator;
    var tmp = try db_mod.TestDirectory.init("graph-stage-recovery");
    defer tmp.cleanup();
    const prefix = "\x00\x00__graph_stage__:v1:";
    {
        var db = try DB.open(alloc, tmp.path(), .{ .start_optional_runtimes = false, .start_index_workers = false });
        defer db.close();
        // More than two pages of abandoned preparations from earlier owners.
        // The legacy stage format has no manifest and remains recoverable.
        var batch = try db.core.store.beginWriteBatch();
        errdefer batch.abort();
        for (0..513) |i| {
            var buf: [80]u8 = undefined;
            const key = try std.fmt.bufPrint(&buf, "{s}{d:0>8}", .{ prefix, i });
            try batch.put(key, "unpublished graph payload");
        }
        try batch.put("unrelated", "keep");
        try batch.commit();
    }
    var db = try DB.open(alloc, tmp.path(), .{ .start_optional_runtimes = false, .start_index_workers = false });
    defer db.close();
    var config: db_mod.enrichment_runtime_mod.Config = .{ .enable_without_producers = true };
    try db.enrichment.initRuntime(&config);
    const runtime = db.enrichment.runtime.?;
    const before = runtime.stats();
    try std.testing.expect(runtime.graph_stage_recovery_pending);
    try std.testing.expectError(error.EnrichmentWaitTimeout, runtime.catchUpUntilWithVisibilityDeadline(0, .{}, 0));
    try std.testing.expect(runtime.graph_stage_recovery_pending);
    try runtime.catchUpUntil(0);
    try std.testing.expect(!runtime.graph_stage_recovery_pending);
    var page = try @import("../../backend_scan.zig").scanPrefixKeysPage(alloc, &runtime.store, prefix, 1);
    defer page.deinit(alloc);
    try std.testing.expectEqual(@as(usize, 0), page.keys.len);
    const kept = try db.core.store.get(alloc, "unrelated");
    defer alloc.free(kept);
    try std.testing.expectEqualStrings("keep", kept);
    const after = runtime.stats();
    try std.testing.expectEqual(before.applied_sequence, after.applied_sequence);
    try std.testing.expectEqual(before.processed_requests, after.processed_requests);
    try std.testing.expectEqual(before.error_count, after.error_count);
    try std.testing.expectEqual(before.retryable_error_count, after.retryable_error_count);
    try std.testing.expectEqual(@as(u64, 0), db.core.store.lastReplaySequence(0));
}

test "db graph fact orphan stage maintenance preserves terminal worker failure" {
    const alloc = std.testing.allocator;
    var tmp = try db_mod.TestDirectory.init("graph-stage-failed-owner");
    defer tmp.cleanup();
    var db = try DB.open(alloc, tmp.path(), .{ .start_optional_runtimes = false, .start_index_workers = false });
    defer db.close();
    const stage = "\x00\x00__graph_stage__:v1:old";
    try db.core.store.put(stage, "unpublished");
    var config: db_mod.enrichment_runtime_mod.Config = .{ .enable_without_producers = true };
    try db.enrichment.initRuntime(&config);
    const runtime = db.enrichment.runtime.?;
    runtime.worker_failed = true;
    runtime.last_error_name = "InvalidExtractorResponse";
    runtime.target_sequence = 1;
    try std.testing.expectError(error.EnrichmentWorkerFailed, runtime.catchUpUntil(1));
    try std.testing.expectError(error.NotFound, db.core.store.get(alloc, stage));
    try std.testing.expect(runtime.worker_failed);
    try std.testing.expectEqualStrings("InvalidExtractorResponse", runtime.last_error_name.?);
    try std.testing.expectEqual(@as(u64, 0), runtime.applied_sequence);
}

test "db graph fact generated replacement retains overlap and retires withdrawn edges" {
    const alloc = std.testing.allocator;
    var tmp = try db_mod.TestDirectory.init("graph-indexed-replacement");
    defer tmp.cleanup();
    var db = try DB.open(alloc, tmp.path(), .{ .start_optional_runtimes = false, .start_index_workers = false });
    defer db.close();
    try db.addIndex(.{ .name = "facts", .kind = .graph, .config_json =
        \\{"source":{"artifact":"relations_v1","path":"$","nodes":{"source":"{{ _item.source }}","target":"{{ _item.target }}"},"edge":{"edge_id":"{{ _doc.key }}","type":"RELATES_TO"}},"artifact":{"name":"relations_v1","kind":"asset","source":{"type":"field","value":"relations"},"content_type":"application/json"}}
    });
    var config: db_mod.enrichment_runtime_mod.Config = .{ .enable_without_producers = true };
    try db.enrichment.initRuntime(&config);
    var arena = std.heap.ArenaAllocator.init(alloc);
    defer arena.deinit();
    const a = arena.allocator();
    const rows = try a.alloc(struct { source: []const u8, target: []const u8 }, 96);
    for (0..2) |phase| {
        for (rows, 0..) |*row, i| row.* = .{ .source = "alice", .target = try std.fmt.allocPrint(a, "target:{d}", .{i + phase * 48}) };
        const raw = try std.json.Stringify.valueAlloc(a, .{ .relations = rows }, .{});
        try db.batch(.{ .writes = &.{.{ .key = "facts-owner", .value = raw }}, .sync_level = .write });
        try db.enrichment.runtime.?.catchUpUntilForDrain(db.core.nextDerivedSequence());
        try db.runDerivedUntil(db.core.nextDerivedSequence());
        const entry = db.core.index_manager.graphIndex("facts").?;
        const edges = try entry.index.getEdges(alloc, "alice", "RELATES_TO", .out);
        defer db_mod.graph_mod.GraphIndex.freeEdges(alloc, edges);
        try std.testing.expectEqual(rows.len, edges.len);
        var seen: [96]bool = @splat(false);
        for (edges) |edge| {
            const number = try std.fmt.parseInt(usize, edge.target["target:".len..], 10);
            try std.testing.expect(number >= phase * 48 and number < phase * 48 + rows.len);
            const offset = number - phase * 48;
            try std.testing.expect(!seen[offset]);
            seen[offset] = true;
        }
    }
    try db.batch(.{ .deletes = &.{"facts-owner"}, .sync_level = .write });
    try db.enrichment.runtime.?.catchUpUntilForDrain(db.core.nextDerivedSequence());
    try db.runDerivedUntil(db.core.nextDerivedSequence());
    const edges = try db.core.index_manager.graphIndex("facts").?.index.getEdges(alloc, "alice", "RELATES_TO", .out);
    defer db_mod.graph_mod.GraphIndex.freeEdges(alloc, edges);
    try std.testing.expectEqual(@as(usize, 0), edges.len);
}

test "db document extraction concurrent writes with unchanged TTL timestamps converge" {
    if (builtin.single_threaded or builtin.os.tag == .freestanding) return error.SkipZigTest;
    const alloc = std.testing.allocator;

    var path_tmp = try db_mod.TestDirectory.init("db");
    defer path_tmp.cleanup();
    const path = path_tmp.path().ptr;
    defer db_mod.cleanupTempDir(path);

    var deterministic_dense = db_mod.embedder_mod.DeterministicDenseEmbedder{};
    var deterministic_sparse = db_mod.embedder_mod.DeterministicSparseEmbedder{};
    var db = try DB.open(alloc, std.mem.span(path), .{
        .enrichment = .{
            .owner_id = "worker-a",
            .dense_embedder = deterministic_dense.interface(),
            .sparse_embedder = deterministic_sparse.interface(),
        },
    });
    defer db.close();

    try db.setSchema(.{ .version = 1, .ttl_duration_ns = 60 * std.time.ns_per_s, .ttl_field = "expires" });

    try db.enrichment.addEnrichment(.{
        .name = "document_units_v1",
        .kind = .asset,
        .field = "url",
        .content_type = "application/json",
        .producer_json = "{\"type\":\"document_extraction\",\"config\":{}}",
    });
    try db.enrichment.addEnrichment(.{
        .name = "document_chunks_v1",
        .kind = .chunk,
        .field = "text",
        .source_artifact_name = "document_units_v1",
        .chunk_size = 256,
    });
    try db.enrichment.addEnrichment(.{
        .name = "document_chunk_dense_v1",
        .kind = .embedding,
        .field = "text",
        .source_artifact_name = "document_chunks_v1",
        .expected_dims = 3,
    });
    try db.addIndex(.{
        .name = "document_vectors",
        .kind = .dense_vector,
        .config_json = "{\"field\":\"embedding\",\"dims\":3,\"embedding_name\":\"document_chunk_dense_v1\"}",
    });

    try db.batch(.{
        .writes = &.{.{ .key = "doc:a", .value = "{\"url\":\"data:text/plain;base64,YWxwaGEgYmV0YSBnYW1tYQ==\",\"text\":\"decoy\",\"expires\":4102444800000000000}" }},
        .sync_level = .full_index,
    });

    var hook = db_mod.PortableRuntimeBatchPrelockTestHook{};
    db_mod.test_portable_runtime_batch_prelock_hook = &hook;
    defer db_mod.test_portable_runtime_batch_prelock_hook = null;

    // Precompute against the original alpha artifacts, then park before
    // commit. Both concurrent writes retain the same TTL field value.
    const Writer = struct {
        db: *DB,
        value: []const u8,
        err: ?anyerror = null,

        fn run(self: *@This()) void {
            self.db.batch(.{
                .writes = &.{.{ .key = "doc:a", .value = self.value }},
                .sync_level = .enrichments,
            }) catch |err| {
                self.err = err;
            };
        }
    };
    var writer_a = Writer{
        .db = &db,
        .value = "{\"url\":\"data:text/plain;base64,YWxwaGEgYmV0YSBnYW1tYQ==\",\"text\":\"decoy\",\"expires\":4102444800000000000}",
    };
    const thread_a = try std.Thread.spawn(.{}, Writer.run, .{&writer_a});
    var joined_a = false;
    defer if (!joined_a) {
        hook.release.store(true, .release);
        thread_a.join();
    };

    const prelock_deadline = db_mod.monotonicTimeNs() + 5 * std.time.ns_per_s;
    while (!hook.entered.load(.acquire) and db_mod.monotonicTimeNs() < prelock_deadline) {
        @import("antfly_platform").time.yieldNow();
    }
    try std.testing.expect(hook.entered.load(.acquire));

    // Commit different content for the same key while thread_a is parked.
    // Clear the hook first so this write is not itself gated by it.
    db_mod.test_portable_runtime_batch_prelock_hook = null;
    try db.batch(.{
        .writes = &.{.{
            .key = "doc:a",
            .value = "{\"url\":\"data:text/plain;base64,YmV0YSBnYW1tYSBkZWx0YQ==\",\"text\":\"decoy\",\"expires\":4102444800000000000}",
        }},
        .sync_level = .full_index,
    });

    // Release thread_a. Its stale pre-lock precompute (built against the
    // original alpha document) must be detected and retried against the
    // now-current state, not silently committed on top of it -- which would
    // either corrupt the generation counter or leave orphaned chunk/embedding
    // artifacts from the intermediate commit.
    hook.release.store(true, .release);
    thread_a.join();
    joined_a = true;
    try std.testing.expectEqual(@as(?anyerror, null), writer_a.err);

    const manifest_key = try db_mod.internal_keys.artifactNamedPrefixAlloc(alloc, "doc:a", "asset", "document_units_v1");
    defer alloc.free(manifest_key);
    const manifest = try db.core.store.get(alloc, manifest_key);
    defer alloc.free(manifest);

    const chunk_prefix = try db_mod.internal_keys.artifactNamedPrefixAlloc(alloc, "doc:a", "chunk", "document_chunks_v1");
    defer alloc.free(chunk_prefix);
    const chunk_entries = try db.core.store.scanPrefix(alloc, chunk_prefix);
    defer db_mod.docstore_mod.DocStore.freeResults(alloc, chunk_entries);
    var chunk_count: usize = 0;
    var last_chunk_payload: []const u8 = "";
    for (chunk_entries) |entry| if (db_mod.internal_keys.isChunkArtifactRecordKey(entry.key)) {
        chunk_count += 1;
        last_chunk_payload = entry.value;
    };
    // Exactly one surviving chunk, matching thread_a's final ("alpha beta
    // gamma") content -- no orphaned chunk from the intermediate ("beta
    // gamma delta") generation this write raced against.
    const final_primary = (try db.get(alloc, "doc:a")) orelse return error.TestExpectedEqual;
    defer alloc.free(final_primary);
    try std.testing.expect(std.mem.indexOf(u8, final_primary, "YWxwaGEgYmV0YSBnYW1tYQ==") != null);
    try std.testing.expect(std.mem.indexOf(u8, last_chunk_payload, "alpha beta gamma") != null);
    try std.testing.expect(std.mem.indexOf(u8, manifest, "\"generation\":3") != null);
    try std.testing.expect(std.mem.indexOf(u8, manifest, "\"from_generation\":2") != null);
    try std.testing.expect(std.mem.indexOf(u8, manifest, "\"to_generation\":3") != null);
    try std.testing.expectEqual(@as(usize, 1), chunk_count);
    try std.testing.expect(std.mem.indexOf(u8, last_chunk_payload, "alpha beta gamma") != null);
}

test "db document lookup allocation benchmark" {
    const Counter = @import("../../../allocation_bench_support.zig").Counter;
    const Case = enum { short, long, missing, sparse, sparse_large, text_large, embedding_identity, embedding_identity_chunk, text, text_asset, text_asset_large, text_asset_escaped, text_mixed, text_missing, delete_set, relational, json_numbers, json_media };
    const case_name = if (std.c.getenv("ANTFLY_DOCUMENT_BENCH_CASE")) |raw| std.mem.span(raw) else "short";
    const fixture = std.meta.stringToEnum(Case, case_name) orelse return error.InvalidBenchmarkCase;
    // Setup is outside measurement. Use the C allocator here to avoid
    // debug-allocator bookkeeping dominating large fixture construction;
    // measured collectors use the counter or production smp allocator.
    const alloc = std.heap.c_allocator;
    const count = if (std.c.getenv("ANTFLY_DOCUMENT_BENCH_DOCUMENTS")) |raw| try std.fmt.parseInt(usize, std.mem.span(raw), 10) else 50_000;
    if (count == 0) return error.InvalidBenchmarkDocuments;
    const batch_size = if (std.c.getenv("ANTFLY_DOCUMENT_BENCH_BATCH")) |raw| try std.fmt.parseInt(usize, std.mem.span(raw), 10) else 256;
    if (batch_size == 0) return error.InvalidBenchmarkBatch;
    if (fixture == .json_numbers or fixture == .json_media)
        return db_mod.jsonOutputAllocationBenchmark(case_name, count, batch_size);
    if (fixture == .embedding_identity or fixture == .embedding_identity_chunk)
        return db_mod.embeddingIdentityAllocationBenchmark(case_name, count, batch_size);
    var backend = db_mod.mem_backend_mod.Backend.init(alloc, .{});
    defer backend.close();
    var store = try db_mod.docstore_mod.DocStore.openRuntime(alloc, try backend.runtimeStore(alloc, .{}));
    defer store.close();
    var directory = try db_mod.TestDirectory.init("collector-benchmark");
    defer directory.cleanup();
    const text_fixture = fixture == .text_large or fixture == .text or fixture == .text_asset or fixture == .text_asset_large or fixture == .text_asset_escaped or fixture == .text_mixed or fixture == .text_missing;
    var db: ?DB = if (text_fixture or fixture == .relational) try DB.open(alloc, directory.path(), .{ .start_index_workers = false }) else null;
    defer if (db) |*database| database.close();
    const sparse_fixture = fixture == .sparse or fixture == .sparse_large;
    const value_bytes = if (std.c.getenv("ANTFLY_DOCUMENT_BENCH_VALUE_BYTES")) |raw| try std.fmt.parseInt(usize, std.mem.span(raw), 10) else 4096;
    var large_value: ?[]u8 = null;
    defer if (large_value) |bytes| alloc.free(bytes);
    if (fixture == .text_large or fixture == .sparse_large) {
        const prefix = if (sparse_fixture) "{\"vec\":{\"indices\":[1,3],\"values\":[0.5,0.25]},\"padding\":\"" else "{\"title\":\"";
        const len = try std.math.add(usize, prefix.len + 2, value_bytes);
        const bytes = try alloc.alloc(u8, len);
        @memcpy(bytes[0..prefix.len], prefix);
        @memset(bytes[prefix.len..][0..value_bytes], 'x');
        @memcpy(bytes[len - 2 ..], "\"}");
        large_value = bytes;
    }
    const value = large_value orelse if (sparse_fixture) "{\"vec\":{\"indices\":[1,3],\"values\":[0.5,0.25]}}" else "{\"title\":\"document value\"}";
    const relational_schema = "{\"version\":1,\"storage_mode\":\"relational\",\"default_type\":\"row\",\"document_schemas\":{\"row\":{\"schema\":{\"type\":\"object\",\"properties\":{\"title\":{\"type\":\"text\"}},\"additionalProperties\":false}}}}";
    var packed_value: ?[]u8 = null;
    defer if (packed_value) |packed_bytes| alloc.free(packed_bytes);
    if (fixture == .relational) {
        try db.?.setSchemaJson(alloc, relational_schema);
        var parsed_schema = try db_mod.public_table_schema.parseValidatedTableSchema(alloc, relational_schema);
        defer parsed_schema.deinit(alloc);
        const runtime_schema = try db_mod.public_table_schema.deriveRuntimeTableSchema(alloc, parsed_schema);
        defer db_mod.schema_mod.freeSchema(alloc, runtime_schema);
        var parsed_value = try std.json.parseFromSlice(std.json.Value, alloc, value, .{});
        defer parsed_value.deinit();
        packed_value = try db_mod.mapper.buildRelationalRowValueForSchemaFromParsedAlloc(alloc, parsed_value.value, runtime_schema);
    }
    if (text_fixture) try db.?.addIndex(.{ .name = "text", .kind = .full_text, .config_json = "{}" });
    const asset_name = if (fixture == .text_asset_escaped) "cap\x00tion" else "caption";
    const large_asset_text = comptime blk: {
        const pattern = "quote\" newline\n null\x00 café 🚀";
        var text: [pattern.len * 128]u8 = undefined;
        for (0..128) |i| @memcpy(text[i * pattern.len ..][0..pattern.len], pattern);
        break :blk text;
    };
    const asset_raw: []const u8 = if (fixture == .text_asset_large) &large_asset_text else "projected value";
    const asset_projected = try std.fmt.allocPrint(alloc, "{{\"caption\":{f}}}", .{std.json.fmt(asset_raw, .{})});
    defer alloc.free(asset_projected);
    if (text_fixture) try db.?.enrichment.addEnrichment(.{ .name = asset_name, .kind = .asset, .field = "caption", .content_type = "text/plain" });
    const targets = [_]db_mod.derived_types.DerivedTargetRef{.{ .kind = .full_text, .index_name = "text" }};
    const names = try alloc.alloc([544]u8, count);
    defer alloc.free(names);
    const docs = try alloc.alloc(db_mod.derived_types.DerivedDocument, count);
    defer alloc.free(docs);
    const stored = try alloc.alloc(db_mod.docstore_mod.KVPair, count);
    defer alloc.free(stored);
    var initialized: usize = 0;
    defer for (stored[0..initialized]) |write| alloc.free(write.key);
    for (names, docs, stored, 0..) |*name, *doc, *write, i| {
        const prefix = try std.fmt.bufPrint(name, "document-{d:0>10}", .{i});
        const key = if (fixture == .long) blk: {
            @memset(name[prefix.len..512], 'x');
            // Exercise escaped component encoding and multiple key blocks.
            name[100] = 0;
            break :blk name[0..512];
        } else prefix;
        const is_asset = fixture == .text_asset or fixture == .text_asset_large or fixture == .text_asset_escaped or (fixture == .text_mixed and i % 8 == 0);
        const stored_key = if (is_asset) try db_mod.internal_keys.artifactNamedPrefixAlloc(alloc, key, "asset", asset_name) else try db_mod.replayDocumentStoreKeyAlloc(alloc, key, fixture == .relational);
        doc.* = .{ .key = if (is_asset) stored_key else key, .action = .upsert, .targets = &targets };
        write.* = .{ .key = stored_key, .value = if (is_asset) asset_raw else packed_value orelse value };
        initialized += 1;
    }
    const expected_count = if (fixture == .missing or fixture == .text_missing) count / 5 else count;
    try store.putBatch(stored[0..expected_count], &.{});
    for (0..2) |measurement| {
        for (0..2) |sample| {
            var counter: Counter = .{};
            const run_alloc = if (measurement == 0) counter.allocator() else std.heap.smp_allocator;
            var checksum: usize = 0;
            var output_count: usize = 0;
            const started = db_mod.monotonicTimeNs();
            var offset: usize = 0;
            while (offset < count) {
                const end = @min(count, offset + batch_size);
                if (fixture == .delete_set) {
                    var keys = std.ArrayListUnmanaged([]const u8).empty;
                    defer keys.deinit(run_alloc);
                    var seen = std.StringHashMapUnmanaged(void).empty;
                    defer seen.deinit(run_alloc);
                    // Exactly six keys fill the default map's first load limit.
                    const unique = @min(@as(usize, 6), end - offset);
                    for (docs[offset..][0..unique]) |doc| try db_mod.appendUniqueBorrowedKeyWithSet(run_alloc, &keys, &seen, doc.key);
                    for (0..128) |_| for (docs[offset..][0..unique]) |doc| try db_mod.appendUniqueBorrowedKeyWithSet(run_alloc, &keys, &seen, doc.key);
                    try std.testing.expectEqual(unique, keys.items.len);
                    for (keys.items) |key| for (key) |byte| {
                        checksum +%= byte;
                    };
                    output_count += end - offset;
                } else if (sparse_fixture) {
                    var writes = try db_mod.collectSparseFieldWritesProfiled(run_alloc, &store, null, docs[offset..end], .{ .start = "", .end = "" }, "vec", .{}, null);
                    defer writes.deinit();
                    try std.testing.expectEqual(end - offset, writes.items.len);
                    for (writes.items, docs[offset..end]) |write, doc| {
                        try std.testing.expectEqualStrings(doc.key, write.doc_id);
                        try std.testing.expectEqualSlices(u32, &.{ 1, 3 }, write.vec.indices);
                        try std.testing.expectEqualSlices(f32, &.{ 0.5, 0.25 }, write.vec.values);
                        for (write.doc_id) |byte| checksum +%= byte;
                    }
                    output_count += writes.items.len;
                } else if (text_fixture) {
                    var writes = try db_mod.collectTextDocumentWritesForIndex(run_alloc, &store, db.?.core.index_manager, docs[offset..end], "text", false, .{ .start = "", .end = "" }, .{});
                    defer writes.deinit();
                    const available = @min(end, expected_count) -| offset;
                    try std.testing.expectEqual(available, writes.docs.items.len);
                    for (writes.docs.items, docs[offset..][0..available]) |write, doc| {
                        try std.testing.expectEqualStrings(doc.key, write.key);
                        try std.testing.expectEqualStrings(if (db_mod.internal_keys.isAssetArtifactKey(doc.key)) asset_projected else value, write.value);
                        for (write.key) |byte| checksum +%= byte;
                    }
                    output_count += writes.docs.items.len;
                } else {
                    var writes = try db_mod.collectDocumentWritesProfiled(run_alloc, &store, if (db) |database| database.core.index_manager else null, docs[offset..end], .{ .start = "", .end = "" }, .{ .relational_base_rows = fixture == .relational }, null);
                    defer writes.deinit();
                    const available = @min(end, expected_count) -| offset;
                    try std.testing.expectEqual(available, writes.items.len);
                    for (writes.items, docs[offset .. offset + available]) |write, doc| {
                        try std.testing.expectEqualStrings(doc.key, write.key);
                        try std.testing.expectEqualStrings(if (db_mod.internal_keys.isAssetArtifactKey(doc.key)) asset_projected else value, write.value);
                        for (write.key) |byte| checksum +%= byte;
                    }
                    output_count += writes.items.len;
                }
                offset = end;
            }
            const elapsed = db_mod.monotonicTimeNs() - started;
            try std.testing.expectEqual(expected_count, output_count);
            try std.testing.expectEqual(@as(usize, 0), counter.live);
            if (sample != 0) std.debug.print("document_lookup_bench {{\"case\":\"{s}\",\"documents\":{d},\"batch\":{d},\"measurement\":\"{s}\",\"elapsed_ns\":{d},\"allocations\":{d},\"resize_calls\":{d},\"remap_calls\":{d},\"moving_remaps\":{d},\"moved_bytes\":{d},\"allocated_bytes\":{d},\"peak_live_bytes\":{d},\"checksum\":{d}}}\n", .{ @tagName(fixture), count, batch_size, if (measurement == 0) "counted" else "timing", elapsed, counter.calls, counter.resize_calls, counter.remap_calls, counter.moving_remaps, counter.moved_bytes, counter.bytes, counter.peak, checksum });
        }
    }
}

test "document collectors release text projections and materialized values on allocation failures" {
    const alloc = std.testing.allocator;
    var directory = try db_mod.TestDirectory.init("text-collector-failures");
    defer directory.cleanup();
    var db = try DB.open(alloc, directory.path(), .{});
    defer db.close();
    try db.addIndex(.{ .name = "text", .kind = .full_text, .config_json = "{}" });
    try db.enrichment.addEnrichment(.{ .name = "caption", .kind = .asset, .field = "caption", .content_type = "text/plain" });
    const asset_key = try db_mod.internal_keys.artifactNamedPrefixAlloc(alloc, "doc", "asset", "caption");
    defer alloc.free(asset_key);
    const key = try db_mod.replayDocumentStoreKeyAlloc(alloc, "normal", false);
    defer alloc.free(key);
    try db.core.store.putBatch(&.{ .{ .key = asset_key, .value = "projected value" }, .{ .key = key, .value = "{\"title\":\"normal\"}" } }, &.{});
    const targets = [_]db_mod.derived_types.DerivedTargetRef{.{ .kind = .full_text, .index_name = "text" }};
    const docs = [_]db_mod.derived_types.DerivedDocument{
        .{ .key = asset_key, .action = .upsert, .targets = &targets, .cleaned_value = "projected value" },
        .{ .key = "normal", .action = .upsert, .targets = &targets, .cleaned_value = "{\"title\":\"normal\"}" },
    };
    for ([_]bool{ false, true }) |inline_values| {
        // Mixed batches exercise per-value ownership; ordinary batches also
        // exhaust failures before and after the exact-size slab allocation.
        try @import("antfly_platform").allocator.checkAllAllocationFailures(alloc, db_mod.textCollectorFailureSweep, .{ db.core.store, db.core.index_manager, @as([]const db_mod.derived_types.DerivedDocument, &docs), inline_values });
        try @import("antfly_platform").allocator.checkAllAllocationFailures(alloc, db_mod.textCollectorFailureSweep, .{ db.core.store, db.core.index_manager, @as([]const db_mod.derived_types.DerivedDocument, docs[1..]), inline_values });
    }
}

test "document collectors asset projection borrows ordinary names and preserves escaped names" {
    const alloc = std.testing.allocator;
    var directory = try db_mod.TestDirectory.init("asset-name-projection");
    defer directory.cleanup();
    var db = try DB.open(alloc, directory.path(), .{ .start_index_workers = false });
    defer db.close();
    try db.enrichment.addEnrichment(.{ .name = "json", .kind = .asset, .field = "caption", .content_type = "application/json" });
    try db.enrichment.addEnrichment(.{ .name = "cap\x00tion", .kind = .asset, .field = "caption", .content_type = "text/plain" });
    const ordinary = try db_mod.internal_keys.artifactNamedPrefixAlloc(alloc, "doc\x00id", "asset", "json");
    defer alloc.free(ordinary);
    var failing = std.testing.FailingAllocator.init(alloc, .{ .fail_index = 0 });
    try std.testing.expectEqual(@as(?[]u8, null), try db_mod.textualAssetFullTextProjectionAlloc(failing.allocator(), db.core.index_manager, ordinary, "{}"));
    const escaped = try db_mod.internal_keys.artifactNamedPrefixAlloc(alloc, "doc\x00id", "asset", "cap\x00tion");
    defer alloc.free(escaped);
    var long_name = @as([512]u8, @splat('x'));
    long_name[1] = 0;
    const long_key = try db_mod.internal_keys.artifactNamedPrefixAlloc(alloc, "doc", "asset", &long_name);
    defer alloc.free(long_key);
    try std.testing.expectError(error.OutOfMemory, db_mod.textualAssetFullTextProjectionAlloc(failing.allocator(), db.core.index_manager, long_key, "value"));
    try std.testing.expectEqual(@as(?[]u8, null), try db_mod.textualAssetFullTextProjectionAlloc(alloc, db.core.index_manager, long_key, "value"));
    const Check = struct {
        fn run(a: Allocator, manager: *db_mod.index_manager_mod.IndexManager, key: []const u8) !void {
            const result = (try db_mod.textualAssetFullTextProjectionAlloc(a, manager, key, "projected value")).?;
            defer a.free(result);
            try std.testing.expectEqualStrings("{\"caption\":\"projected value\"}", result);
        }
    };
    try @import("antfly_platform").allocator.checkAllAllocationFailures(alloc, Check.run, .{ db.core.index_manager, @as([]const u8, escaped) });
}

test "document collectors ordinary slab excludes projected assets" {
    const alloc = std.testing.allocator;
    var directory = try db_mod.TestDirectory.init("text-materialization-hint");
    defer directory.cleanup();
    var db = try DB.open(alloc, directory.path(), .{ .start_index_workers = false });
    defer db.close();
    try db.addIndex(.{ .name = "text", .kind = .full_text, .config_json = "{}" });
    try db.enrichment.addEnrichment(.{ .name = "caption", .kind = .asset, .field = "caption", .content_type = "text/plain" });
    const asset = try db_mod.internal_keys.artifactNamedPrefixAlloc(alloc, "doc", "asset", "caption");
    defer alloc.free(asset);
    const normal = try db_mod.replayDocumentStoreKeyAlloc(alloc, "normal", false);
    defer alloc.free(normal);
    try db.core.store.putBatch(&.{ .{ .key = asset, .value = "projected value" }, .{ .key = normal, .value = "{\"title\":\"normal\"}" } }, &.{});
    const empty = try db_mod.replayDocumentStoreKeyAlloc(alloc, "empty", false);
    defer alloc.free(empty);
    const second = try db_mod.replayDocumentStoreKeyAlloc(alloc, "second", false);
    defer alloc.free(second);
    try db.core.store.putBatch(&.{ .{ .key = empty, .value = "" }, .{ .key = second, .value = "{\"title\":\"second document\"}" } }, &.{});
    const targets = [_]db_mod.derived_types.DerivedTargetRef{.{ .kind = .full_text, .index_name = "text" }};
    var docs = @as([32]db_mod.derived_types.DerivedDocument, @splat(.{ .key = asset, .action = .upsert, .targets = &targets }));
    docs[29].key = "normal";
    docs[30].key = "empty";
    docs[31].key = "second";
    var result = try db_mod.collectTextDocumentWritesForIndex(alloc, db.core.store, db.core.index_manager, docs[29..], "text", false, .{ .start = "", .end = "" }, .{});
    defer result.deinit();
    try std.testing.expectEqual(@as(usize, 3), result.docs.items.len);
    try std.testing.expectEqual(@as(usize, 0), result.materialized_documents.items.len);
    try std.testing.expectEqualStrings("{\"title\":\"normal\"}{\"title\":\"second document\"}", result.document_bytes);
    // The collector has already closed its read transaction. Replacing the
    // source document must not change the returned owned slice.
    try db.core.store.putBatch(&.{.{ .key = normal, .value = "{}" }}, &.{});
    try std.testing.expectEqualStrings("{\"title\":\"normal\"}", result.docs.items[1].value);
    try std.testing.expectEqualStrings("", result.docs.items[0].value);
    try std.testing.expectEqualStrings("{\"title\":\"second document\"}", result.docs.items[2].value);
    try std.testing.expectEqual(result.document_bytes.ptr, result.docs.items[1].value.ptr);
    try std.testing.expectEqual(result.document_bytes.ptr + result.docs.items[1].value.len, result.docs.items[2].value.ptr);
    var mixed = try db_mod.collectTextDocumentWritesForIndex(alloc, db.core.store, db.core.index_manager, &docs, "text", false, .{ .start = "", .end = "" }, .{});
    defer mixed.deinit();
    try std.testing.expectEqual(@as(usize, 32), mixed.docs.items.len);
    try std.testing.expectEqual(@as(usize, 0), mixed.document_bytes.len);
    try std.testing.expectEqual(@as(usize, 3), mixed.materialized_documents.items.len);
    try std.testing.expectEqualStrings("{}", mixed.docs.items[30].value);
    try std.testing.expectEqualStrings("{\"caption\":\"projected value\"}", mixed.docs.items[0].value);
}

test "db extractEnrichments field-index planning is allocation atomic" {
    const alloc = std.testing.allocator;
    var directory = try db_mod.TestDirectory.init("graph-field-plan-failures");
    defer directory.cleanup();
    const path = directory.path().ptr;
    defer db_mod.cleanupTempDir(path);
    var db = try DB.open(alloc, std.mem.span(path), .{});
    defer db.close();
    try db.addIndex(.{ .name = "graph", .kind = .graph, .config_json = "{\"edge_types\":[{\"name\":\"rel\",\"field\":\"links\"}]}" });
    const Check = struct {
        fn run(failure_alloc: Allocator, target: *DB, value: []const u8) !void {
            var result = try target.enrichment.extractEnrichments(failure_alloc, &.{.{ .key = "doc", .value = value }});
            defer result.deinit(failure_alloc);
            try std.testing.expectEqual(@as(usize, 1), result.cleaned_writes.len);
            if (std.mem.indexOf(u8, value, "target") != null) {
                try std.testing.expectEqual(@as(usize, 1), result.graph_writes.len);
                try std.testing.expectEqualStrings("target", result.graph_writes[0].target);
            } else try std.testing.expectEqual(@as(usize, 0), result.graph_writes.len);
        }
    };
    for ([_][]const u8{ "{\"links\":[\"target\"]}", "{\"links\":[]}" }) |value|
        try @import("antfly_platform").allocator.checkAllAllocationFailures(alloc, Check.run, .{ &db, value });
}

test "artifact projection construction releases failures with real local resources" {
    const alloc = std.testing.allocator;
    var directory = try db_mod.TestDirectory.init("projection-ownership");
    defer directory.cleanup();
    var db = try DB.open(alloc, std.mem.span(directory.path().ptr), .{});
    defer db.close();
    try db.enrichment.addEnrichment(.{ .name = "structured", .kind = .asset, .field = "body", .content_type = "application/json" });
    const Check = struct {
        fn run(failing: Allocator, source: db_mod.read_projection.Source, kind: db_mod.types.ArtifactKind, raw: []const u8) !void {
            var value = try db_mod.read_projection.artifactProjectionValue(source, failing, .{
                .document_id = @constCast("doc"),
                .name = @constCast("structured"),
                .kind = kind,
                .chunk_id = if (kind == .chunk) 0 else null,
            }, raw, null);
            defer db_mod.freeJsonValue(failing, &value);
            try std.testing.expectEqualStrings("ready", value.object.get("status").?.string);
            try std.testing.expectEqualStrings("doc", value.object.get("artifact_ref").?.object.get("document_id").?.string);
        }
    };
    const source = db_mod.read_projection.Source{ .core = db.core };
    try @import("antfly_platform").allocator.checkAllAllocationFailures(alloc, Check.run, .{ source, db_mod.types.ArtifactKind.asset, @as([]const u8, "{\"entities\":[{\"text\":\"Antfly\"}]}") });
    try @import("antfly_platform").allocator.checkAllAllocationFailures(alloc, Check.run, .{ source, db_mod.types.ArtifactKind.chunk, @as([]const u8, "{\"body\":\"hello\",\"_chunk_id\":0}") });
    try @import("antfly_platform").allocator.checkAllAllocationFailures(alloc, Check.run, .{ source, db_mod.types.ArtifactKind.chunk, @as([]const u8, "plain chunk") });
}

test "inline resolver replacement resolution deadline resumes a large record across reopen" {
    const a = std.testing.allocator;
    const Clock = struct {
        var now: u64 = 0;
        fn read() u64 {
            return now;
        }
        fn process(_: *anyopaque, _: Allocator, _: []const db_mod.resolution_runtime_mod.ResolverConfig, _: db_mod.resolver_lib.ArtifactStore, _: ?db_mod.resolver_lib.CandidateProvider, keys: []const []const u8, _: ?db_mod.resolution_runtime_mod.CandidateSource, _: ?db_mod.embedder_mod.DenseEmbedder) anyerror!bool {
            try std.testing.expectEqual(@as(usize, 1), keys.len);
            var parsed = (try db_mod.resolution_runtime_mod.parseSourceArtifactKeyAlloc(std.testing.allocator, keys[0])) orelse return error.TestUnexpectedResult;
            defer parsed.deinit(std.testing.allocator);
            now += 1;
            return false;
        }
    };
    var directory = try db_mod.TestDirectory.init("replacement-resolution-deadline");
    defer directory.cleanup();
    var host = try db_mod.background_runtime_mod.BackendRuntimeHandle.init(a, .{ .backend = .manual, .borrowed_io = .{ .general = std.testing.io } });
    defer host.deinit();
    const options: db_mod.OpenOptions = .{ .backend_runtime = host.ptr(), .executor = .{ .backend = .manual }, .start_optional_runtime_workers = false, .start_independent_maintenance = false };
    db_mod.resolution_runtime_mod.test_replay_now_ns = Clock.read;
    defer db_mod.resolution_runtime_mod.test_replay_now_ns = null;
    Clock.now = 0;
    const count = 40;
    var arena = std.heap.ArenaAllocator.init(a);
    defer arena.deinit();
    const scratch = arena.allocator();
    var source_keys: [count][]const u8 = undefined;
    for (&source_keys, 0..) |*key, i| key.* = try db_mod.internal_keys.artifactNamedPrefixAlloc(scratch, try std.fmt.allocPrint(scratch, "doc:{d:0>3}", .{i}), "asset", "kg");
    var sequence: u64 = 0;
    {
        var db = try DB.open(a, directory.path(), options);
        defer db.close();
        _ = try db.enrichment.upsertResolverWithResultOptions(.{ .name = "concepts", .table = "default", .source_artifact = "kg", .resolution_artifact = "resolved", .key_template = "entity/{{ uslug _entity.text }}", .promote = false }, .{ .drain_backfill = false });
        try db.runUntilIdle();
        for (source_keys) |key| try db.core.store.put(key, "{\"entities\":[{\"id\":\"a\",\"text\":\"Ada\",\"label\":\"person\"}]}");
        sequence = try db_mod.appendDerivedBatchRecord(&db, .{ .changed_artifact_keys = &source_keys });
        const runtime = db.enrichment.resolution.runtime.?;
        runtime.notifySequence(sequence);
        runtime.ordered_writer = .{ .ptr = &db, .process = Clock.process };
        try runtime.catchUpOnce(0);
        try std.testing.expect(runtime.replay_progress == null);
        try runtime.catchUpOnce(1);
        try std.testing.expectEqual(@as(usize, 1), runtime.replay_progress.?.next_key);
        try std.testing.expect(runtime.applied_sequence.load(.acquire) < sequence);
        const first = try db_mod.internal_keys.resolutionArtifactKeyAlloc(a, "doc:000", "resolved");
        defer a.free(first);
        const value = try db.core.store.get(a, first);
        defer a.free(value);
        const second = try db_mod.internal_keys.resolutionArtifactKeyAlloc(a, "doc:001", "resolved");
        defer a.free(second);
        try std.testing.expectError(error.NotFound, db.core.store.get(a, second));
    }
    {
        var db = try DB.open(a, directory.path(), options);
        defer db.close();
        const runtime = db.enrichment.resolution.runtime.?;
        try std.testing.expectEqual(@as(usize, 1), runtime.replay_progress.?.next_key);
        runtime.ordered_writer = .{ .ptr = &db, .process = Clock.process };
        try runtime.catchUpOnce(2);
        try std.testing.expectEqual(@as(usize, 2), runtime.replay_progress.?.next_key);
        try runtime.catchUpOnce(std.math.maxInt(u64));
        try std.testing.expectEqual(@as(usize, 34), runtime.replay_progress.?.next_key);
        // Ordinary drains/workers must honor a manual owner's partial cut.
        runtime.ordered_writer = .{ .ptr = db.enrichment.resolution.append_context.?, .process = db_mod.processOrderedResolutions };
        try db.runUntilIdle();
        try std.testing.expect(runtime.replay_progress == null);
        try std.testing.expect(runtime.applied_sequence.load(.acquire) >= sequence);
        for (0..count) |i| {
            const key = try db_mod.internal_keys.resolutionArtifactKeyAlloc(scratch, try std.fmt.allocPrint(scratch, "doc:{d:0>3}", .{i}), "resolved");
            const value = try db.core.store.get(a, key);
            defer a.free(value);
            try std.testing.expect(std.mem.indexOf(u8, value, "entity/ada") != null);
        }
    }
}

test "grouped sources production publishes private units incrementally across delete and reopen" {
    if (builtin.single_threaded or builtin.os.tag == .freestanding) return error.SkipZigTest;
    const a = std.testing.allocator;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    const path = try tmp.dir.realPathFileAlloc(std.testing.io, ".", a);
    defer a.free(path);
    const groups = @import("../group_sources.zig");
    const Snapshot = @import("../group_source_state.zig").Snapshot;
    const Harness = struct {
        const Status = struct { snapshot: ?Snapshot = null };
        fn snapshot(db: *DB, alloc: Allocator, definition: @import("../group_source_index.zig").Definition, segment: []const u8) !?std.json.Parsed(Status) {
            const request = try std.json.Stringify.valueAlloc(alloc, @import("../group_source_contract.zig").Query{ .action = .status, .artifact = "turns", .definition = definition, .group = "thread", .segment = segment, .cell = 0 }, .{});
            defer alloc.free(request);
            const response = (try db.lookup(alloc, "", .{ .group_source_query_json = request })) orelse return null;
            defer alloc.free(response.json);
            return try std.json.parseFromSlice(Status, alloc, response.json, .{ .allocate = .alloc_always, .ignore_unknown_fields = true });
        }
        fn waitSnapshot(db: *DB, alloc: Allocator, definition: @import("../group_source_index.zig").Definition, segment: []const u8, present: bool) !void {
            const io = db.backend_runtime.io().?;
            for (0..300) |_| {
                var observed = try snapshot(db, alloc, definition, segment);
                defer if (observed) |*value| value.deinit();
                if (observed) |value| if (value.value.snapshot) |current| {
                    if ((current.units.len > 0) == present) return;
                };
                try io.sleep(.fromMilliseconds(10), .awake);
            }
            return error.TestUnexpectedResult;
        }
    };
    const config = groups.Config{ .group_by = "thread_id", .order_by = "ordinal", .segment_by = "turn_id", .field = "text" };
    var definition: @import("../group_source_index.zig").Definition = undefined;
    var first_digest: groups.Digest = undefined;
    {
        const db = try DB.openOwned(a, path, .{ .enrichment = .{ .enable_without_producers = true } });
        defer db.closeOwned();
        try db.enrichment.addEnrichment(.{ .name = "turns", .kind = .group, .source = config });
        try std.testing.expectError(error.InvalidBatchRequest, @import("../../write.zig").validateGroupedSourceBatch(a, db, &.{.{ .key = "invalid", .value = "{\"thread_id\":\"thread\",\"turn_id\":\"a\",\"ordinal\":\"invalid\",\"text\":\"source\"}" }}, &.{}, &.{}));
        try std.testing.expect((try db.lookup(a, "invalid", .{})) == null);
        try db.batchInternal(.{ .writes = &.{ .{ .key = "one", .value = "{\"thread_id\":\"thread\",\"turn_id\":\"a\",\"ordinal\":1,\"text\":\"alpha\"}" }, .{ .key = "two", .value = "{\"thread_id\":\"thread\",\"turn_id\":\"a\",\"ordinal\":2,\"text\":\"beta\"}" } }, .sync_level = .enrichments }, null, .{});
        const definitions = (try db.lookup(a, "", .{ .group_source_query_json = "{\"action\":\"definitions\"}" })).?;
        defer a.free(definitions.json);
        var decoded = try std.json.parseFromSlice([]@import("../group_source_worker.zig").Definition, a, definitions.json, .{ .allocate = .alloc_always });
        defer decoded.deinit();
        try std.testing.expectEqual(@as(usize, 2), decoded.value.len);
        definition = decoded.value[0].definition;
        try Harness.waitSnapshot(db, a, definition, "a", true);
        var first = (try Harness.snapshot(db, a, definition, "a")).?;
        defer first.deinit();
        first_digest = first.value.snapshot.?.input_digest;
        try db.batchInternal(.{ .writes = &.{.{ .key = "three", .value = "{\"thread_id\":\"thread\",\"turn_id\":\"b\",\"ordinal\":3,\"text\":\"gamma\"}" }}, .sync_level = .enrichments }, null, .{});
        try Harness.waitSnapshot(db, a, definition, "b", true);
        var unchanged = (try Harness.snapshot(db, a, definition, "a")).?;
        defer unchanged.deinit();
        try std.testing.expectEqualSlices(u8, &first_digest, &unchanged.value.snapshot.?.input_digest);
        const unit = first.value.snapshot.?.units[0];
        try std.testing.expectEqual(@as(?db_mod.types.LookupResult, null), try db.lookup(a, unit, .{}));
        try std.testing.expectError(error.KeyOutOfRange, db.updateRange(.{ .start = unit, .end = "" }));
        const source = try db_mod.internal_keys.documentKeyAlloc(a, unit);
        defer a.free(source);
        try std.testing.expect(db_mod.internal_keys.isGroupSourceRowKey(source));
        try std.testing.expectError(error.InvalidBatchRequest, db.batchInternal(.{ .writes = &.{.{ .key = source, .value = "{}" }}, .sync_level = .write }, null, .{}));
        const private_timestamp = try db_mod.internal_keys.ttlKeyAlloc(a, unit);
        defer a.free(private_timestamp);
        try std.testing.expectError(error.InvalidBatchRequest, db.batchInternal(.{ .deletes = &.{private_timestamp}, .sync_level = .write }, null, .{}));
        try std.testing.expect(!db_mod.internal_keys.isStoredDocumentRowKey(source));
        const row = try db.core.store.get(a, source);
        defer a.free(row);
        try std.testing.expect(std.mem.indexOf(u8, row, "alpha\\nbeta") != null);
    }
    {
        const db = try DB.openOwned(a, path, .{ .enrichment = .{ .enable_without_producers = true } });
        defer db.closeOwned();
        var reopened = (try Harness.snapshot(db, a, definition, "a")).?;
        defer reopened.deinit();
        try std.testing.expectEqualSlices(u8, &first_digest, &reopened.value.snapshot.?.input_digest);
        try db.batchInternal(.{ .deletes = &.{ "one", "two" }, .sync_level = .enrichments }, null, .{});
        try Harness.waitSnapshot(db, a, definition, "a", false);
        var later = (try Harness.snapshot(db, a, definition, "b")).?;
        defer later.deinit();
        try std.testing.expect(later.value.snapshot.?.units.len > 0);
        const old_unit = try db_mod.internal_keys.documentKeyAlloc(a, later.value.snapshot.?.units[0]);
        defer a.free(old_unit);
        var replacement = config;
        replacement.max_bytes = 64;
        _ = try db.enrichment.upsertEnrichment(.{ .name = "turns", .kind = .group, .source = replacement });
        const next_definition = @import("../group_source_index.zig").Definition{ .table_incarnation = definition.table_incarnation, .configuration = groups.definitionDigest("turns", replacement) };
        try Harness.waitSnapshot(db, a, next_definition, "b", true);
        var retired = false;
        for (0..1000) |_| {
            const old = db.core.store.get(a, old_unit) catch |err| if (err == error.NotFound) {
                retired = true;
                break;
            } else return err;
            a.free(old);
            try db.backend_runtime.io().?.sleep(.fromMilliseconds(10), .awake);
        }
        try std.testing.expect(retired);
        // Re-add an identical retired definition after its private records and
        // baseline checkpoint have been collected. It must rebuild the source.
        _ = try db.enrichment.upsertEnrichment(.{ .name = "turns", .kind = .group, .source = config });
        try Harness.waitSnapshot(db, a, definition, "b", true);
        inline for (.{ @as(u32, 7), @as(u32, 8) }) |version| {
            const raw = try @import("../catalog/resolver_catalog.zig").readerVersionBytes(version);
            try std.testing.expectEqual(version, try std.fmt.parseInt(u32, raw, 10));
        }
    }
}

test "grouped sources production visibility retains its publication cut across later primary producer debt" {
    const Fixture = struct {
        calls: usize = 0,
        fn produce(raw: *anyopaque, alloc: Allocator, _: db_mod.asset_producer_mod.Request) ![]u8 {
            const self: *@This() = @ptrCast(@alignCast(raw));
            self.calls += 1;
            return alloc.dupe(u8, "{\"statement\":\"accepted\"}");
        }
    };
    const alloc = std.testing.allocator;
    var tmp = try db_mod.TestDirectory.init("grouped-publication-cut");
    defer tmp.cleanup();
    var db = try DB.open(alloc, tmp.path(), .{
        .start_optional_runtimes = false,
        .start_optional_runtime_workers = false,
        .start_index_workers = false,
        .ttl_cleanup = .{ .enabled = false },
    });
    defer db.close();
    try db.addIndex(.{ .name = "text", .kind = .full_text, .config_json = "{}" });
    try db.enrichment.addEnrichment(.{ .name = "turns", .kind = .group, .source = .{ .group_by = "thread", .order_by = "ordinal", .field = "text" } });
    try db.enrichment.addEnrichment(.{ .name = "notes", .kind = .asset, .field = "body", .producer_json = "{\"type\":\"generator\",\"config\":{\"generator\":{\"provider\":\"openai\",\"model\":\"fixture\",\"api_key\":\"fixture\"}}}" });
    var fixture: Fixture = .{};
    var config: db_mod.enrichment_runtime_mod.Config = .{ .asset_producer = .{ .ptr = &fixture, .vtable = &.{ .produce = Fixture.produce, .invocation_memory_for_requests = db_mod.GateAssetProducer.invocationMemory } } };
    try db.enrichment.initRuntime(&config);
    try db.batch(.{ .writes = &.{.{ .key = "earlier", .value = "{\"body\":\"first\"}" }}, .sync_level = .write });
    const runtime = db.enrichment.runtime.?;
    try runtime.catchUpUntilForDrain(db.core.nextDerivedSequence());
    var sequence = db.core.nextDerivedSequence();
    var phase: u8 = 1;
    try db.waitForResolvedTransactionSyncTargetWithCancellation(.full_index, &sequence, &phase, .none);
    try std.testing.expectEqual(@as(u8, 2), phase);
    const captured = sequence;
    const calls = fixture.calls;
    try std.testing.expect(calls > 0);

    runtime.retrying = true;
    runtime.next_retry_at_ms = runtime.clock.nowRealtimeMs() + 60_000;
    defer {
        runtime.retrying = false;
        runtime.next_retry_at_ms = 0;
    }
    try db.batch(.{ .writes = &.{.{ .key = "later", .value = "{\"body\":\"pending\"}" }}, .sync_level = .write });
    try std.testing.expect(runtime.stats().applied_sequence < db.core.nextDerivedSequence());
    // A resumed owner wait retains the accepted cut even when a new primary
    // producer is pending. It neither invokes that provider nor advances its
    // watermark to manufacture completion.
    try db.waitForResolvedTransactionSyncTargetWithCancellation(.full_index, &sequence, &phase, .none);
    try std.testing.expectEqual(captured, sequence);
    try std.testing.expectEqual(calls, fixture.calls);
    try std.testing.expect(runtime.stats().applied_sequence < db.core.nextDerivedSequence());
    try std.testing.expect(runtime.stats().retrying);
}

test "grouped sources production ordered transaction resolution never waits on derived publication" {
    const a = std.testing.allocator;
    var directory = try db_mod.TestDirectory.init("group-ordered-resolution");
    defer directory.cleanup();
    var db = try DB.open(a, directory.path(), .{ .executor = .{ .backend = .manual }, .start_optional_runtimes = false, .start_independent_maintenance = false });
    defer db.close();
    try db.enrichment.addEnrichment(.{ .name = "turns", .kind = .group, .source = .{ .group_by = "thread", .order_by = "ordinal", .segment_by = "turn", .field = "text" } });
    const txn = try db.beginTransaction(1);
    try db.writeTransaction(txn, .{ .writes = &.{.{ .key = "member", .value = "{\"thread\":\"t\",\"turn\":\"a\",\"ordinal\":1,\"text\":\"source\"}" }} });
    const Cancelled = struct {
        fn check(_: *const anyopaque) bool {
            return true;
        }
    };
    var context: u8 = 0;
    const cancelled: db_mod.types.CancellationToken = .{ .ptr = &context, .is_cancelled_fn = Cancelled.check };
    // Even a derived visibility request that is already cancelled cannot turn
    // a durable ordered commit into a failed apply or wait for its own replay.
    try db.resolveReplicatedTransactionAtOrderedReceipt(txn, .committed, 2, .full_index, cancelled, .{ .term = 1, .index = 1 }, null);
    const row = (try db.lookup(a, "member", .{})).?;
    defer a.free(row.json);
    try std.testing.expect(std.mem.indexOf(u8, row.json, "source") != null);
    try db.resolveReplicatedTransactionAtOrderedReceipt(txn, .committed, 2, .full_index, cancelled, .{ .term = 1, .index = 1 }, null);
}

test "db successful asset publication clears terminal debt but stale and failed publications do not" {
    const a = std.testing.allocator;
    var tmp = try db_mod.TestDirectory.init("asset-repair-publication");
    defer tmp.cleanup();
    var db = try DB.open(a, tmp.path(), .{ .start_optional_runtimes = false, .start_index_workers = false });
    defer db.close();
    try db.addIndex(.{ .name = "text", .kind = .full_text, .config_json = "{}" });
    const key = try db_mod.internal_keys.artifactNamedPrefixAlloc(a, "doc", "asset", "units");
    defer a.free(key);
    try db_mod.recordArtifactRepairIssueContextDetailed(db.async_context, .asset, "text", "doc", "", "", "units", "", "units", key, null, 5, .enrichment_failed, 1, "RemoteDocumentFetchFailed");
    try db.core.store.put(key, "{\"route_type\":\"error\",\"merge_plan\":{\"status\":\"failed\"},\"chunk_count\":0}");
    const batch: db_mod.derived_types.DerivedBatch = .{ .sequence = 6, .changed_artifact_keys = &.{key} };
    try db_mod.local_mutation.applyDerivedBatchToIndexContext(db.async_context, batch, .{ .name = "text", .kind = .full_text });
    {
        const issues = try db.listArtifactRepairIssues(a, .asset, "text", 0);
        defer db_mod.types.freeArtifactRepairIssues(a, issues);
        try std.testing.expectEqual(@as(usize, 1), issues.len);
    }
    try db.core.store.put(key, "{\"route_type\":\"text\",\"merge_plan\":{\"status\":\"in_progress\"},\"unit_count\":1}");
    try db_mod.local_mutation.applyDerivedBatchToIndexContext(db.async_context, batch, .{ .name = "text", .kind = .full_text });
    {
        const issues = try db.listArtifactRepairIssues(a, .asset, "text", 0);
        defer db_mod.types.freeArtifactRepairIssues(a, issues);
        try std.testing.expectEqual(@as(usize, 1), issues.len);
    }
    try db.core.store.put(key, "{\"route_type\":\"pdf\",\"merge_plan\":{\"status\":\"converged\"},\"unit_count\":1,\"chunk_count\":1,\"ocr_failed_count\":1}");
    try db_mod.local_mutation.applyDerivedBatchToIndexContext(db.async_context, batch, .{ .name = "text", .kind = .full_text });
    {
        const issues = try db.listArtifactRepairIssues(a, .asset, "text", 0);
        defer db_mod.types.freeArtifactRepairIssues(a, issues);
        try std.testing.expectEqual(@as(usize, 1), issues.len);
    }
    try db.core.store.put(key, "{\"route_type\":\"text\",\"merge_plan\":{\"status\":\"converged\"},\"unit_count\":1,\"chunk_count\":1}");
    var stale = batch;
    stale.sequence = 4;
    try db_mod.local_mutation.applyDerivedBatchToIndexContext(db.async_context, stale, .{ .name = "text", .kind = .full_text });
    {
        const issues = try db.listArtifactRepairIssues(a, .asset, "text", 0);
        defer db_mod.types.freeArtifactRepairIssues(a, issues);
        try std.testing.expectEqual(@as(usize, 1), issues.len);
    }
    try db_mod.local_mutation.applyDerivedBatchToIndexContext(db.async_context, batch, .{ .name = "text", .kind = .full_text });
    try db.enrichment.runArtifactRepairMetadataMaintenanceUntilIdle();
    const issues = try db.listArtifactRepairIssues(a, .asset, "text", 0);
    defer db_mod.types.freeArtifactRepairIssues(a, issues);
    try std.testing.expectEqual(@as(usize, 0), issues.len);
    try db.addIndex(.{ .name = "graph", .kind = .graph, .config_json = "{}" });
    try db_mod.recordArtifactRepairIssueContextDetailed(db.async_context, .asset, "graph", "doc", "", "", "units", "", "units", key, null, 5, .enrichment_failed, 1, "ConnectionRefused");
    // Root manifests are metadata, not extraction-relation rows. Replay must
    // settle their debt even when no graph projection work is applicable.
    try std.testing.expect(!try db_mod.applyDerivedBatchToIndexAsync(db.async_context, batch, .{ .name = "graph", .kind = .graph }, .{}));
    const graph_issues = try db.listArtifactRepairIssues(a, .asset, "graph", 0);
    defer db_mod.types.freeArtifactRepairIssues(a, graph_issues);
    try std.testing.expectEqual(@as(usize, 0), graph_issues.len);
}
