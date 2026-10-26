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

//! Enrichment operations of the local storage DB. Methods take `*DB` and
//! are re-exported from `DB` in db.zig, so callers keep writing `db.method()`.

const db_zig = @import("db.zig");
const DB = db_zig.DB;
const TestDb = db_zig.TestDb;
const enrichment_terminal_failure_retirement_page_size = @import("execution_resources.zig").enrichment_terminal_failure_retirement_page_size;
const TestDirectory = @import("../../common/test_directory.zig").TestDirectory;
const appendUniqueOwnedKey = @import("owned_keys.zig").appendUniqueOwnedKey;
const apply_state = @import("derived/apply_state.zig");
const artifact_producer_readiness = @import("artifact/producer_readiness.zig");
const artifact_publication = @import("artifact/publication.zig");
const artifact_reconcile_intent = @import("artifact/reconcile_intent.zig");
const background_runtime_mod = @import("../background_runtime.zig");
const builtin = @import("builtin");
const change_journal_mod = @import("derived/change_journal.zig");
const db_query_search = @import("query/search_exec.zig");
const derived_executor_mod = @import("derived/derived_executor.zig");
const derived_types = @import("derived/derived_types.zig");
const doc_identity = @import("doc_identity.zig");
const docstore_mod = @import("../docstore.zig");
const embedder_mod = @import("enrichment/embedder.zig");
const enrichment_artifact_codec = @import("enrichment/artifact_codec.zig");
const enrichment_runtime_mod = @import("enrichment/enrichment_runtime.zig");
const enrichment_state = @import("enrichment/enrichment_state.zig");
const enrichment_types = @import("enrichment/enrichment_types.zig");
const graph_edge_contender = @import("graph_edge_contender.zig");
const graph_edge_ttl_tombstone = @import("graph_edge_ttl_tombstone.zig");
const graph_mod = @import("../../graph/graph.zig");
const index_manager_mod = @import("catalog/index_manager.zig");
const index_repair_state = @import("derived/index_repair_state.zig");
const internal_keys = @import("../internal_keys.zig");
const lease_mod = @import("lease.zig");
const mapper = @import("document/mapper.zig");
const merge_page_chunks = @import("merge_page_chunks.zig");
const merge_state_mod = @import("merge_state.zig");
const persistent_mod = @import("../persistent.zig");
const range_cardinality = @import("range_cardinality.zig");
const replay_stream_mod = @import("derived/replay_stream.zig");
const resolver_catalog_mod = @import("catalog/resolver_catalog.zig");
const result_collectors = @import("result_collectors.zig");
const shard_mod = @import("../shard.zig");
const std = @import("std");
const test_enrichment = @import("test_support.zig").test_enrichment;
const test_index = @import("test_support.zig").test_index;
const storage_engine = @import("../engine.zig");
const types = @import("types.zig");
const Allocator = db_zig.Allocator;
const AsyncContext = db_zig.AsyncContext;
const ChunkCacheEntry = db_zig.ChunkCacheEntry;
const CountingDenseEmbedder = db_zig.CountingDenseEmbedder;
const CountingSparseEmbedder = db_zig.CountingSparseEmbedder;
const DerivedCoverageCounters = db_zig.DerivedCoverageCounters;
const EnrichmentAppendContext = db_zig.EnrichmentAppendContext;
const FakePromotionSink = db_zig.FakePromotionSink;
const GateDenseEmbedder = db_zig.GateDenseEmbedder;
const GateSparseEmbedder = db_zig.GateSparseEmbedder;
const ImageDecodingPartsEmbedder = db_zig.ImageDecodingPartsEmbedder;
const IndexLifecycleWorkClass = db_zig.IndexLifecycleWorkClass;
const IndexTargetVisibility = db_zig.IndexTargetVisibility;
const ManagedSyncTargets = db_zig.ManagedSyncTargets;
const QueryVisibilityChange = db_zig.QueryVisibilityChange;
const QueryVisibilityEvent = db_zig.QueryVisibilityEvent;
const accountDenseCoverage = db_zig.accountDenseCoverage;
const accountSparseCoverage = db_zig.accountSparseCoverage;
const acquireNeighborContextGraphSource = db_zig.acquireNeighborContextGraphSource;
const appendClonedExplicitDenseEmbedding = db_zig.appendClonedExplicitDenseEmbedding;
const appendClonedExplicitSparseEmbedding = db_zig.appendClonedExplicitSparseEmbedding;
const appendDerivedBatchRecord = db_zig.appendDerivedBatchRecord;
const appendGeneratedBatchFromEnrichment = db_zig.appendGeneratedBatchFromEnrichment;
const appendGeneratedEnrichments = db_zig.appendGeneratedEnrichments;
const artifactRepairIssueKeyForIssueAlloc = db_zig.artifactRepairIssueKeyForIssueAlloc;
const augmentExtractedWriteWithGraphFieldEdges = db_zig.augmentExtractedWriteWithGraphFieldEdges;
const chunker_mod = db_zig.chunker_mod;
const collectManagedSyncTargets = db_zig.collectManagedSyncTargets;
const collectManagedSyncTargetsForRecord = db_zig.collectManagedSyncTargetsForRecord;
const collectTextReplayDeleteKeys = db_zig.collectTextReplayDeleteKeys;
const commitArtifactUnitTurnFromEnrichment = db_zig.commitArtifactUnitTurnFromEnrichment;
const computeChunkRequest = db_zig.computeChunkRequest;
const computeDenseRequest = db_zig.computeDenseRequest;
const containsName = db_zig.containsName;
const currentTimeNs = db_zig.currentTimeNs;
const dataUriAlloc = db_zig.dataUriAlloc;
const default_test_wait_attempts = db_zig.default_test_wait_attempts;
const denseTargetCountForIndexContext = db_zig.denseTargetCountForIndexContext;
const drainManagedAdmissionSourceReplayForTest = db_zig.drainManagedAdmissionSourceReplayForTest;
const enrichmentRequestFailurePending = db_zig.enrichmentRequestFailurePending;
const enrichmentRequestFailureRangePending = db_zig.enrichmentRequestFailureRangePending;
const enrichmentTerminalFailureSequenceEnvelope = db_zig.enrichmentTerminalFailureSequenceEnvelope;
const expectDenseEmbeddingArtifactWithSourceHash = db_zig.expectDenseEmbeddingArtifactWithSourceHash;
const expectSparseEmbeddingArtifactValue = db_zig.expectSparseEmbeddingArtifactValue;
const expectedChunkArtifactPublicIdAlloc = db_zig.expectedChunkArtifactPublicIdAlloc;
const expectedDocumentEmbeddingArtifactKeyAlloc = db_zig.expectedDocumentEmbeddingArtifactKeyAlloc;
const journalRecordHasHint = db_zig.journalRecordHasHint;
const lockApplyShared = db_zig.lockApplyShared;
const lockAtomicWithBackoff = db_zig.lockAtomicWithBackoff;
const lockEnrichmentFailurePendingFence = db_zig.lockEnrichmentFailurePendingFence;
const missingAsNull = db_zig.missingAsNull;
const monotonicTimeNs = db_zig.monotonicTimeNs;
const no_background_test_open = db_zig.no_background_test_open;
const no_replay_no_background_test_open = db_zig.no_replay_no_background_test_open;
const notifyDerivedExecutorSequence = db_zig.notifyDerivedExecutorSequence;
const openModeRequiresReadOnlyBackends = db_zig.openModeRequiresReadOnlyBackends;
const platform_clock = db_zig.platform_clock;
const putDenseEmbeddingArtifactForTest = db_zig.putDenseEmbeddingArtifactForTest;
const recordArtifactRepairIssueContextDetailed = db_zig.recordArtifactRepairIssueContextDetailed;
const recordEnrichmentRequestFailure = db_zig.recordEnrichmentRequestFailure;
const relationalColumns = db_zig.relationalColumns;
const remote_media_bmp_24_2x2 = db_zig.remote_media_bmp_24_2x2;
const repair_completion_test_options = db_zig.repair_completion_test_options;
const resetDenseIndexForArtifactRebuildForTest = db_zig.resetDenseIndexForArtifactRebuildForTest;
const scraping = db_zig.scraping;
const sleepNs = db_zig.sleepNs;
const template_remote = db_zig.template_remote;
const testDenseChunkArtifactLifecycle = db_zig.testDenseChunkArtifactLifecycle;
const testManagedGenerationRepairAdmission = db_zig.testManagedGenerationRepairAdmission;
const testRemoteMediaHostRenderJsonToParts = db_zig.testRemoteMediaHostRenderJsonToParts;
const unlockEnrichmentFailurePendingFence = db_zig.unlockEnrichmentFailurePendingFence;
const waitForManagedIndexesApplied = db_zig.waitForManagedIndexesApplied;
const ChunkBoolFilterLowering = DB.ChunkBoolFilterLowering;
const ChunkBoolFilterLoweringBudget = DB.ChunkBoolFilterLoweringBudget;
const DetachedEnrichmentRuntime = DB.DetachedEnrichmentRuntime;
const EnrichmentCompletion = DB.EnrichmentCompletion;
const EnrichmentReconfigureOptions = DB.EnrichmentReconfigureOptions;
const GeneratedEnrichmentReplayAppendResult = DB.GeneratedEnrichmentReplayAppendResult;
const GeneratedEnrichmentReplayPageResult = DB.GeneratedEnrichmentReplayPageResult;
const GeneratedEnrichmentReplayScanOptions = DB.GeneratedEnrichmentReplayScanOptions;
const IndexAdmissionMode = DB.IndexAdmissionMode;
const InstalledIndex = DB.InstalledIndex;
const ReplayDrainOptions = DB.ReplayDrainOptions;
const combineStoredPatternJsonAlloc = DB.combineStoredPatternJsonAlloc;
const hasQueryVisibilityHook = DB.hasQueryVisibilityHook;
const initializeDerivedCoverageIdentity = DB.initializeDerivedCoverageIdentity;
const lowerTextQueryToStoredPatternValueAlloc = DB.lowerTextQueryToStoredPatternValueAlloc;
const notifyAsyncContextActivityHook = DB.notifyAsyncContextActivityHook;
const notifyAsyncContextVisibilityHook = DB.notifyAsyncContextVisibilityHook;
const notifyQueryVisibilityHook = DB.notifyQueryVisibilityHook;
const restoreEnrichmentPrevious = DB.restoreEnrichmentPrevious;

pub fn resumeGeneratedReplayFromJournalIfNeeded(self: *DB) !void {
    const runtime = self.enrichment_runtime orelse return;
    if (!self.core.hasGeneratedEnrichmentTargets()) return;

    const target_sequence = self.core.nextEnrichmentSequence();
    if (target_sequence == 0) return;
    runtime.resumeTargetPreservingRetryDebt(target_sequence);
}

pub fn createDetachedEnrichmentRuntime(self: *DB, enrichment_cfg: *enrichment_runtime_mod.Config) !?DetachedEnrichmentRuntime {
    if (comptime storage_engine.enabled) return storage_engine.call(@src().fn_name, .{ self, enrichment_cfg });
    if (enrichment_cfg.dense_embedder == null and enrichment_cfg.sparse_embedder == null and enrichment_cfg.asset_producer == null and !enrichment_cfg.enable_without_producers and !self.core.index_manager.hasGroupSources()) return null;
    // Producer ownership moves through reconfiguration, but execution and
    // security capabilities belong to the resident DB. Hydrate every
    // replacement at this single construction boundary so live index DDL
    // cannot silently fall back to default remote-content policy or an
    // unrelated executor when it supplies only a new producer set.
    var runtime_cfg = enrichment_cfg.*;
    if (self.core.index_manager.hasGroupSources()) runtime_cfg.enable_without_producers = true;
    runtime_cfg.root_incarnation = self.root_incarnation;
    if (runtime_cfg.secret_store == null) runtime_cfg.secret_store = self.secret_store;
    if (runtime_cfg.remote_content == null) runtime_cfg.remote_content = self.remote_content;
    if (runtime_cfg.resource_manager == null) runtime_cfg.resource_manager = self.core.index_manager.resource_manager;
    if (runtime_cfg.io == null) runtime_cfg.io = self.backend_runtime.inferenceIo();
    runtime_cfg.relational_base_rows = relationalColumns(self) != null;

    const resources = self.core.batchExecutionResources();
    const context: EnrichmentAppendContext = .{
        .alloc = self.runtime_alloc,
        .root_incarnation = self.root_incarnation,
        .read_only = openModeRequiresReadOnlyBackends(self.open_mode),
        .clock = runtime_cfg.clock orelse self.backend_runtime.clock(),
        .store = resources.store,
        .applied_sequence_checkpoint_path = resources.applied_sequence_checkpoint_path,
        .index_repair_checkpoint = resources.index_repair_checkpoint,
        .shard_manager = resources.shard_manager,
        .index_manager = resources.index_manager,
        .apply_mutex = resources.apply_mutex,
        .portable_runtime_activation_pending = &self.async_context.portable_runtime_activation_pending,
        .snapshot_admission = resources.snapshot_admission,
        .snapshot_replay_admission = resources.snapshot_replay_admission,
        .repair_replay_mutex = resources.repair_replay_mutex,
        .change_journal = resources.change_journal,
        .replay_source = resources.replay_source,
        .executor = self.executor,
        .async_context = self.async_context,
        .log_mutex = resources.log_mutex,
        .replication_async_effect_mirror = self.local_execution.replication_async_effect_mirror,
        .replication_async_batch_mirror = self.local_execution.replication_async_batch_mirror,
        .replication_async_metadata_mirror = self.local_execution.replication_async_metadata_mirror,
        .replication_write_gate = self.local_execution.replication_write_gate,
        .resolution_runtime = self.resolution_runtime,
        .promotion_runtime = self.promotion_runtime,
        .artifact_publication_dispatcher = self.local_execution.artifact_publication_dispatcher,
    };
    var bundle = try DetachedEnrichmentRuntime.create(self.runtime_alloc, enrichment_cfg, runtime_cfg, context, .{
        .backend_runtime = self.backend_runtime,
        .neighbor_acquire = acquireNeighborContextGraphSource,
        .write = appendGeneratedBatchFromEnrichment,
        .failure = recordEnrichmentRequestFailure,
        .failure_pending = enrichmentRequestFailurePending,
        .failure_range_pending = enrichmentRequestFailureRangePending,
        .failure_lock = lockEnrichmentFailurePendingFence,
        .failure_unlock = unlockEnrichmentFailurePendingFence,
        .notify_ctx = self.executor,
        .notify = notifyDerivedExecutorSequence,
        .commit = commitArtifactUnitTurnFromEnrichment,
    });
    errdefer bundle.deinit(self.runtime_alloc);
    try mergeEnrichmentTerminalFailureEnvelope(resources.store, bundle.runtime.?);
    return bundle;
}

pub fn mergeEnrichmentTerminalFailureEnvelope(
    store: *docstore_mod.DocStore,
    runtime: *enrichment_runtime_mod.EnrichmentRuntime,
) !void {
    const marker_envelope = try enrichmentTerminalFailureSequenceEnvelope(store);
    if (marker_envelope.min != 0) {
        runtime.terminal_failure_min_sequence = if (runtime.terminal_failure_min_sequence == 0)
            marker_envelope.min
        else
            @min(runtime.terminal_failure_min_sequence, marker_envelope.min);
        runtime.terminal_failure_max_sequence = @max(runtime.terminal_failure_max_sequence, marker_envelope.max);
    }
}

pub fn publishEnrichmentOwner(ptr: *anyopaque) void {
    const self: *DB = @ptrCast(@alignCast(ptr));
    self.enrichment_append_context = self.enrichment_owner.bundle.append_ctx;
    self.enrichment_runtime = self.enrichment_owner.bundle.runtime;
    self.async_context.enrichment_runtime = self.enrichment_owner.bundle.runtime;
}

pub fn initOptionalEnrichmentRuntime(self: *DB, enrichment_cfg: *enrichment_runtime_mod.Config) !void {
    self.enrichment_owner.bundle = (try self.createDetachedEnrichmentRuntime(enrichment_cfg)) orelse return;
    publishEnrichmentOwner(self);
}

pub fn reconfigureEnrichmentRuntime(self: *DB, cfg: enrichment_runtime_mod.Config) !void {
    if (comptime storage_engine.enabled) return storage_engine.call(@src().fn_name, .{ self, cfg });
    try reconfigureEnrichmentRuntimeWithOptions(self, cfg, .{});
}

/// Install a replacement producer set without starting it. Structural
/// activation uses this form to quiesce the old owner once, mutate the
/// index catalog, and then start the exact replacement after the durable
/// admission is established. Starting here would make addIndex stop the
/// new worker a second time and let corpus work delay control-plane DDL.
pub fn reconfigureEnrichmentRuntimePaused(self: *DB, cfg: enrichment_runtime_mod.Config) !void {
    if (comptime storage_engine.enabled) return storage_engine.call(@src().fn_name, .{ self, cfg });
    try reconfigureEnrichmentRuntimeWithOptions(self, cfg, .{ .start_replacement = false });
}

pub fn reconfigureEnrichmentRuntimeWithOptions(
    self: *DB,
    cfg: enrichment_runtime_mod.Config,
    options: EnrichmentReconfigureOptions,
) !void {
    if (openModeRequiresReadOnlyBackends(self.open_mode)) return error.ReadOnly;

    const recovery = self.transaction_recovery_local_context;
    const recovery_io = self.backend_runtime.io();
    if (recovery) |ctx| ctx.provider_mutex.lockUncancelable(recovery_io.?);
    defer if (recovery) |ctx| ctx.provider_mutex.unlock(recovery_io.?);

    const hook_present = hasQueryVisibilityHook(self.async_context);
    try self.enrichment_owner.replace(.{
        .ptr = self,
        .alloc = self.runtime_alloc,
        .mutex = &self.async_context.enrichment_lifecycle_mutex,
        .desired_running = &self.async_context.enrichment_desired_running,
        .allows_running = self.open_mode.allowsOptionalRuntimes(),
        .status_hook = if (hook_present) .{ .ptr = self.async_context, .on_change = notifyAsyncContextVisibilityHook, .on_activity = notifyAsyncContextActivityHook } else null,
        .create = createEnrichmentReplacement,
        .after_create = enrichmentReplacementCreated,
        .prepare = prepareEnrichmentReplacement,
        .restore = restoreEnrichmentPrevious,
        .publish = publishEnrichmentOwner,
    }, cfg, options.start_replacement and self.local_execution.optional_runtime_workers_enabled);
    if (!hook_present) self.setQueryVisibilityHook(null);
}

pub fn createEnrichmentReplacement(ptr: *anyopaque, cfg: *enrichment_runtime_mod.Config) !?DetachedEnrichmentRuntime {
    const self: *DB = @ptrCast(@alignCast(ptr));
    return self.createDetachedEnrichmentRuntime(cfg);
}

pub fn enrichmentReplacementCreated(ptr: *anyopaque) !void {
    const self: *DB = @ptrCast(@alignCast(ptr));
    if (builtin.is_test) if (DB.test_enrichment_reconfigure_after_detached_hook) |hook| try hook(self);
}

pub fn prepareEnrichmentReplacement(ptr: *anyopaque, runtime: *enrichment_runtime_mod.EnrichmentRuntime, telemetry: ?types.EnrichmentStats) !void {
    const self: *DB = @ptrCast(@alignCast(ptr));
    // The previous worker has joined. Rehydrate its final durable debt;
    // volatile telemetry never substitutes for those persisted markers.
    try runtime.reloadDurableState();
    if (telemetry) |previous| runtime.inheritProcessTelemetry(previous);
    try mergeEnrichmentTerminalFailureEnvelope(self.core.batchExecutionResources().store, runtime);
    if (self.core.hasGeneratedEnrichmentTargets()) {
        const target = self.core.nextEnrichmentSequence();
        if (target != 0) runtime.resumeTargetPreservingRetryDebt(target);
    }
}

pub fn resumeEnrichmentRuntimeAfterReconfigure(self: *DB, operation: []const u8, index_name: []const u8) !void {
    if (comptime storage_engine.enabled) return storage_engine.call(@src().fn_name, .{ self, operation, index_name });
    if (self.enrichment_runtime == null) return;
    try self.restartEnrichmentAfterStructuralMutation(operation, index_name);
}

pub fn clearEmbeddingActivityCache(self: *DB) void {
    self.embedding_activity.clear(self.alloc);
}

pub fn startEnrichmentRuntimeForLifecycle(runtime: *enrichment_runtime_mod.EnrichmentRuntime) !void {
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

pub fn scheduleEnrichmentRestartContext(ctx: *AsyncContext, lane: background_runtime_mod.DurableJobLane, owner_id: u64) void {
    ctx.enrichment_restart_owner.schedule(.{ .ptr = ctx, .lane = lane, .owner_id = owner_id, .io = ctx.io, .closing = &ctx.background_closing, .wanted = enrichmentRestartWanted, .attempt = attemptEnrichmentRestart, .name = "enrichment", .unavailable = error.EnrichmentRuntimeUnavailable });
}

pub fn enrichmentRestartWanted(ptr: *anyopaque) bool {
    const ctx: *AsyncContext = @ptrCast(@alignCast(ptr));
    return ctx.enrichment_desired_running.load(.acquire);
}

pub fn attemptEnrichmentRestart(ptr: *anyopaque) !bool {
    const ctx: *AsyncContext = @ptrCast(@alignCast(ptr));
    lockAtomicWithBackoff(&ctx.enrichment_lifecycle_mutex);
    const runtime = ctx.enrichment_runtime;
    const should_start = runtime != null and ctx.enrichment_desired_running.load(.acquire) and !ctx.background_closing.load(.acquire) and !runtime.?.isStarted();
    const start_error: ?anyerror = if (should_start) blk: {
        startEnrichmentRuntimeForLifecycle(runtime.?) catch |err| break :blk err;
        break :blk null;
    } else null;
    const started = runtime == null or runtime.?.isStarted();
    ctx.enrichment_lifecycle_mutex.unlock();
    if (started) {
        notifyQueryVisibilityHook(ctx, .status);
        return true;
    }
    if (start_error) |err| return err;
    return false;
}

pub fn quiesceEnrichmentForStructuralMutation(self: *DB) bool {
    const desired = self.async_context.enrichment_desired_running.swap(false, .acq_rel);
    lockAtomicWithBackoff(&self.async_context.enrichment_lifecycle_mutex);
    defer self.async_context.enrichment_lifecycle_mutex.unlock();
    const runtime = self.async_context.enrichment_runtime orelse return desired;
    const started = runtime.isStarted();
    if (started) runtime.stop();
    return desired or started;
}

pub fn enrichmentFailureCoverageMarkerKeyAlloc(
    self: *DB,
    alloc: Allocator,
    issue: types.ArtifactRepairIssue,
) !?[]u8 {
    if (issue.reason != .enrichment_failed or issue.index_name.len == 0) return null;
    if (self.core.index_manager.denseIndex(issue.index_name) == null and
        self.core.index_manager.sparseIndex(issue.index_name) == null)
    {
        return null;
    }
    const generation = self.core.index_manager.coverageGenerationForIndex(issue.index_name) orelse return null;
    return try internal_keys.derivedCoverageOutcomeKeyAlloc(alloc, issue.index_name, generation, issue.doc_key);
}

pub fn prepareMergePageChunk(self: *DB, alloc: Allocator, req: types.BatchRequest, cancellation: types.CancellationToken) !merge_page_chunks.Prepared {
    const chunks = merge_page_chunks;
    self.core.lockApplyShared();
    var locked = true;
    defer if (locked) self.core.unlockApplyShared();
    var txn = try self.core.store.beginReadTxnWithBlockCacheAdmission(.transient);
    defer txn.abort();
    const raw = try missingAsNull(txn.get(merge_state_mod.key));
    var state = if (raw) |value| try merge_state_mod.decodeAlloc(alloc, value) else null;
    defer if (state) |*value| value.deinit(alloc);
    const allowed = merge_state_mod.copyAllowed(state, req.merge_replication.?);
    self.core.unlockApplyShared();
    locked = false;
    // The regular apply fence retains its existing committed-stale-entry
    // no-op behavior, without trying to read a discarded attempt's spool.
    if (!allowed) return chunks.Prepared.init(alloc, req);
    var reader: chunks.NativeReader = .{ .txn = &txn };
    return chunks.prepare(alloc, &reader, req, cancellation);
}

pub fn installIndexWhileEnrichmentQuiesced(
    self: *DB,
    cfg: types.IndexConfig,
    admission_mode: IndexAdmissionMode,
) !InstalledIndex {
    return self.installIndexForReconciliation(cfg, admission_mode, null, null, null);
}

pub fn restartEnrichmentAfterStructuralMutation(self: *DB, operation: []const u8, index_name: []const u8) !void {
    // Manual hosts drive enrichment while holding their publication lease.
    // Catalog changes must preserve the worker policy chosen at open.
    if (!self.local_execution.optional_runtime_workers_enabled) return;
    self.async_context.enrichment_desired_running.store(true, .release);
    lockAtomicWithBackoff(&self.async_context.enrichment_lifecycle_mutex);
    const runtime = self.async_context.enrichment_runtime orelse {
        self.async_context.enrichment_lifecycle_mutex.unlock();
        return;
    };
    const start_result = if (!runtime.isStarted()) startEnrichmentRuntimeForLifecycle(runtime) else {};
    self.async_context.enrichment_lifecycle_mutex.unlock();
    start_result catch |err| {
        // The durable restart supervisor owns recovery from this point;
        // preserve the diagnostic without classifying a retryable outage
        // as an unrecoverable process failure.
        std.log.warn("failed to restart enrichment runtime after {s} index={s} err={s}", .{ operation, index_name, @errorName(err) });
        scheduleEnrichmentRestartContext(
            self.async_context,
            self.backend_runtime.durable_jobs,
            self.repair_cleanup_owner_id,
        );
        notifyQueryVisibilityHook(self.async_context, .status);
        return err;
    };
}

pub fn addEnrichment(self: *DB, cfg: types.EnrichmentConfig) !void {
    if (comptime storage_engine.enabled) return storage_engine.call(@src().fn_name, .{ self, cfg });
    if (openModeRequiresReadOnlyBackends(self.open_mode)) return error.ReadOnly;
    var write_fence = try self.beginReplicatedWrite(.gate_then_apply);
    defer write_fence.release();
    {
        var probe = try self.core.store.beginProbeTxn();
        defer probe.abort();
        try self.requireOnlineArtifactCatalogMutableLocked(&probe);
    }
    try self.enforceVectorMigrationConfigurationGate();
    try self.enforceReplicationWriteGate();
    try self.core.addEnrichment(cfg);
}

pub fn upsertEnrichment(self: *DB, cfg: types.EnrichmentConfig) !index_manager_mod.IndexManager.EnrichmentUpsertResult {
    if (comptime storage_engine.enabled) return storage_engine.call(@src().fn_name, .{ self, cfg });
    if (openModeRequiresReadOnlyBackends(self.open_mode)) return error.ReadOnly;
    var write_fence = try self.beginReplicatedWrite(.gate_then_apply);
    defer write_fence.release();
    {
        var probe = try self.core.store.beginProbeTxn();
        defer probe.abort();
        try self.requireOnlineArtifactCatalogMutableLocked(&probe);
    }
    try self.enforceVectorMigrationConfigurationGate();
    try self.enforceReplicationWriteGate();
    return try self.core.upsertEnrichment(cfg);
}

pub fn getEnrichment(self: *DB, alloc: Allocator, kind: types.EnrichmentKind, name: []const u8) !?types.EnrichmentConfig {
    if (comptime storage_engine.enabled) return storage_engine.call(@src().fn_name, .{ self, alloc, kind, name });
    try self.lockApplySharedForPortableRuntime();
    defer self.core.unlockApplyShared();
    return try self.core.getEnrichment(alloc, kind, name);
}

pub fn listEnrichments(self: *DB, alloc: Allocator) ![]types.EnrichmentConfig {
    if (comptime storage_engine.enabled) return storage_engine.call(@src().fn_name, .{ self, alloc });
    try self.lockApplySharedForPortableRuntime();
    defer self.core.unlockApplyShared();
    return try self.core.listEnrichments(alloc);
}

pub fn extractEnrichments(self: *DB, alloc: Allocator, writes: []const types.BatchWrite) !types.ExtractEnrichmentsResult {
    try self.lockApplySharedForPortableRuntime();
    defer self.core.unlockApplyShared();

    var collector: result_collectors.Extraction = .{ .alloc = alloc };
    defer collector.deinit();
    for (writes) |write| {
        var extracted = try mapper.extractWrite(alloc, write.key, write.value);
        defer extracted.deinit(alloc);
        try augmentExtractedWriteWithGraphFieldEdges(self, alloc, write.key, write.value, &extracted);
        try collector.append(write.key, extracted);
    }
    return collector.finish();
}

pub fn computeEnrichments(self: *DB, alloc: Allocator, writes: []const types.BatchWrite) !types.ComputeEnrichmentsResult {
    try self.lockApplySharedForPortableRuntime();
    defer self.core.unlockApplyShared();

    var collector: result_collectors.Enrichments = .{ .alloc = alloc };
    defer collector.deinit();

    for (writes) |write| {
        var extracted = try mapper.extractWrite(alloc, write.key, write.value);
        defer extracted.deinit(alloc);
        try augmentExtractedWriteWithGraphFieldEdges(self, alloc, write.key, write.value, &extracted);
        const cleaned = extracted.cleaned_value orelse continue;

        var explicit_dense = std.ArrayListUnmanaged(types.EnrichmentDenseEmbeddingWrite).empty;
        defer {
            for (explicit_dense.items) |*embedding| embedding.deinit(alloc);
            explicit_dense.deinit(alloc);
        }
        for (extracted.dense_embeddings) |embedding| {
            try appendClonedExplicitDenseEmbedding(
                alloc,
                &explicit_dense,
                embedding.index_name,
                embedding.doc_key,
                embedding.vector,
            );
        }
        var explicit_sparse = std.ArrayListUnmanaged(types.EnrichmentSparseEmbeddingWrite).empty;
        defer {
            for (explicit_sparse.items) |*embedding| embedding.deinit(alloc);
            explicit_sparse.deinit(alloc);
        }
        for (extracted.sparse_embeddings) |embedding| {
            try appendClonedExplicitSparseEmbedding(
                alloc,
                &explicit_sparse,
                embedding.index_name,
                embedding.doc_key,
                embedding.indices,
                embedding.values,
            );
        }

        const generated = try self.core.planGeneratedEnrichments(
            alloc,
            write.key,
            cleaned,
            explicit_dense.items,
            explicit_sparse.items,
        );
        defer enrichment_types.deinitGeneratedRequests(alloc, generated);

        var chunk_cache = std.ArrayListUnmanaged(ChunkCacheEntry).empty;
        defer {
            for (chunk_cache.items) |entry| {
                alloc.free(entry.key);
                chunker_mod.freeChunks(alloc, entry.chunks);
            }
            chunk_cache.deinit(alloc);
        }

        for (generated) |request| {
            switch (request.kind) {
                .chunk_text => try computeChunkRequest(alloc, self, cleaned, request, &collector.artifacts, &collector.documents, &chunk_cache),
                .dense_embedding => computeDenseRequest(alloc, self, cleaned, request, &collector.artifacts, &collector.dense, &chunk_cache) catch |err| switch (err) {
                    error.MissingDenseEmbedder => try appendUniqueOwnedKey(alloc, &collector.failed, write.key),
                    else => return err,
                },
                else => {},
            }
        }
    }

    return try collector.finish();
}

pub fn deleteIndexWhileEnrichmentQuiesced(self: *DB, name: []const u8) !bool {
    return self.deleteIndexWhileQuiescedForReconciliation(name, null);
}

pub fn deleteEnrichment(self: *DB, kind: types.EnrichmentKind, name: []const u8) !bool {
    if (comptime storage_engine.enabled) return storage_engine.call(@src().fn_name, .{ self, kind, name });
    return self.deleteEnrichmentForReconciliation(kind, name, null);
}

pub fn deleteEnrichmentForReconciliation(self: *DB, kind: types.EnrichmentKind, name: []const u8, context: ?artifact_reconcile_intent.Context) !bool {
    if (openModeRequiresReadOnlyBackends(self.open_mode)) return error.ReadOnly;
    var replication_mutation = self.acquireReplicationMutationShared();
    defer if (replication_mutation) |*lease| lease.release();
    if (context == null) try self.enforceReplicationWriteGate();
    try self.lockApplyForPortableRuntime();
    defer self.core.unlockApply();
    {
        var probe = try self.core.store.beginProbeTxn();
        defer probe.abort();
        try self.requireOnlineArtifactCatalogMutableWithContextLocked(&probe, context);
    }
    try self.enforceVectorMigrationConfigurationGate();
    return try self.core.deleteEnrichment(kind, name);
}

pub fn persistedEnrichmentStats(self: *DB) !types.EnrichmentStats {
    if (!self.core.hasGeneratedEnrichmentTargets() and self.core.index_manager.enrichments.items.len == 0) return .{};
    const resources = self.core.batchExecutionResources();
    const checkpoint = try enrichment_state.loadProjectionCheckpoint(self.alloc, resources.store, enrichment_runtime_mod.scope_name);
    const applied = checkpoint.applied_sequence;
    const status = try enrichment_state.loadRuntimeStatus(self.alloc, resources.store, enrichment_runtime_mod.scope_name);
    const target = @max(applied, status.target_sequence);
    return .{
        .enabled = target > 0 or status.error_count > 0 or status.retrying or status.worker_failed,
        .target_sequence = target,
        .applied_sequence = applied,
        .projection_checkpoint_status = enrichment_state.projectionStatusName(checkpoint.status),
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

pub fn runEnrichmentUntil(self: *DB, sequence: u64) !void {
    try runEnrichmentUntilWithCancellation(self, sequence, .none);
}

pub fn runEnrichmentUntilWithCancellation(self: *DB, sequence: u64, cancellation: types.CancellationToken) !void {
    try self.runEnrichmentUntilWithVisibilityDeadline(sequence, cancellation, null);
}

pub fn runEnrichmentUntilWithVisibilityDeadline(
    self: *DB,
    sequence: u64,
    cancellation: types.CancellationToken,
    deadline_ns: ?u64,
) !void {
    if (sequence == 0) return;
    if (self.enrichment_runtime) |runtime| {
        if (!self.local_execution.optional_runtime_workers_enabled) {
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
pub fn runEnrichmentUntilForDrainUnbounded(self: *DB, sequence: u64) !void {
    if (sequence == 0) return;
    const runtime = self.enrichment_runtime orelse return;
    runtime.notifySequence(sequence);
    try runtime.catchUpUntilForDrain(sequence);
}

pub fn runEnrichmentUntilForDrain(self: *DB, sequence: u64, options: ReplayDrainOptions) !void {
    while (true) {
        const outcome = if (options.unbounded_enrichment_wait and options.cancellation.ptr == null)
            runEnrichmentUntilForDrainUnbounded(self, sequence)
        else
            runEnrichmentUntilWithCancellation(self, sequence, options.cancellation);
        outcome catch |err| switch (err) {
            error.EnrichmentRetryInProgress => {
                if (!options.wait_for_enrichment_retries) return err;
                const io = self.backend_runtime.io() orelse return error.MissingBackendRuntimeIo;
                try io.sleep(.fromMilliseconds(25), .awake);
                continue;
            },
            else => return err,
        };
        return;
    }
}

pub fn markEnrichmentAppliedIfNoPendingThrough(self: *DB, sequence: u64) !void {
    if (sequence == 0) return;
    const runtime = self.enrichment_runtime orelse return;
    const runtime_stats = runtime.stats();
    if (runtime_stats.applied_sequence >= sequence) return;
    if (try self.noPendingEnrichmentReplayThrough(runtime_stats.applied_sequence, sequence)) {
        try runtime.markAppliedThrough(sequence);
    }
}

pub fn runMaintenanceUntilWithEnrichmentCompletion(
    self: *DB,
    sequence: u64,
    sync_targets: ManagedSyncTargets,
    cancellation: types.CancellationToken,
    deadline_ns: u64,
    completion: EnrichmentCompletion,
) !void {
    const wait = derived_executor_mod.VisibilityWait{
        .cancellation = cancellation,
        .deadline_ns = deadline_ns,
        .clock = self.backend_runtime.monotonicClock(),
    };
    var stable_target = sequence;
    while (true) {
        if (completion == .successful_requests) {
            try self.runEnrichmentUntilWithVisibilityDeadline(stable_target, cancellation, deadline_ns);
        } else if (self.enrichment_runtime) |runtime| {
            // A retained read captures committed primary and generated
            // publications. It must not run providers or wait for future
            // request retries before reading already-visible documents.
            // Replay/apply fences below exclude publication while pinning;
            // derived indexes still catch up to this exact primary cut.
            try runtime.requireHealthyForQueryCapture();
        }
        try self.runDerivedUntilWithVisibilityWait(stable_target, wait);

        const next_target = self.core.nextDerivedSequence();
        if (completion == .published_cut or next_target <= stable_target) {
            try waitForManagedIndexesApplied(self, sequence, sync_targets.all_indexes);
            _ = try self.runArtifactRepairMetadataMaintenancePass();
            return;
        }
        stable_target = next_target;
    }
}

/// Appends internal derived work without exposing the DB's batch execution
/// context. Runtime partitions use this boundary while retaining the normal
/// write gate, locking, backlog accounting, and hot-standby mirroring semantics.
/// Publish prepared producer output against this runtime's live coordinator.
/// The existing transactional guard checks source digests, incarnations,
/// and exact lease tenure before any artifact or replay record is committed.
pub fn publishPreparedGeneratedBatch(self: *DB, derived_batch: enrichment_runtime_mod.GeneratedRecordBatch, promotions: []const enrichment_runtime_mod.GeneratedArtifactPromotion, deletes: []const []const u8, guards: []const enrichment_runtime_mod.GeneratedSourceGuard, fence: ?enrichment_runtime_mod.GeneratedWriteFence) !enrichment_runtime_mod.GeneratedRecordCommit {
    if (self.enrichment_append_context == null) try self.reconfigureEnrichmentRuntimePaused(.{ .enable_without_producers = true });
    return appendGeneratedBatchFromEnrichment(self.enrichment_append_context orelse return error.EnrichmentWaitCanceled, derived_batch, promotions, deletes, guards, fence);
}

/// Resident managed writers already have an asynchronous enrichment owner.
/// Maintenance callers that only borrowed that writer must yield a bounded
/// quantum when its provider is retrying instead of becoming a second,
/// indefinitely waiting owner of the same replay tail.
pub fn runUntilIdleWithoutWaitingForEnrichmentRetries(self: *DB) !void {
    if (comptime storage_engine.enabled) return storage_engine.call(@src().fn_name, .{self});
    try self.runUntilIdleWithReplayDrainOptions(.{});
}

/// Borrowed maintenance owners must release their group quantum when a
/// higher-priority lifecycle operation arrives. This cancels only the
/// waiter's ownership; the resident enrichment worker retains durable work
/// and continues (or is explicitly reconfigured) under its own lifecycle.
pub fn runUntilIdleWithoutWaitingForEnrichmentRetriesWithCancellation(
    self: *DB,
    cancellation: types.CancellationToken,
) !void {
    if (comptime storage_engine.enabled) return storage_engine.call(@src().fn_name, .{ self, cancellation });
    try self.runUntilIdleWithReplayDrainOptions(.{ .cancellation = cancellation });
}

pub fn appendGeneratedEnrichmentsFromStoredDocsPage(
    self: *DB,
    alloc: Allocator,
    force_generated_artifact_names: []const []const u8,
    skip_terminally_covered: bool,
    resume_key: ?[]const u8,
    limit: usize,
    options: GeneratedEnrichmentReplayScanOptions,
) !GeneratedEnrichmentReplayPageResult {
    if (self.enrichment_runtime == null) return .{ .complete = true };
    if (limit == 0) return error.InvalidArgument;

    const initial_lower = if (resume_key == null) try self.core.documentRangeLowerAlloc("") else null;
    defer if (initial_lower) |key| self.core.alloc.free(key);
    const lower = resume_key orelse initial_lower.?;
    var replay_batch = try self.collectStoredGeneratedReplayBatch(alloc, lower, resume_key != null, limit, options);
    defer replay_batch.deinit(alloc);

    var pending_batch = derived_types.DerivedBatch{};
    defer derived_types.deinitDerivedBatch(self.alloc, &pending_batch);
    try appendGeneratedEnrichments(self, &pending_batch, .{
        .writes = replay_batch.writes,
        .sync_level = .write,
    }, replay_batch.extracted, force_generated_artifact_names, skip_terminally_covered);
    var target_sequence: u64 = 0;
    if (pending_batch.generated_enrichment_refs.len != 0) {
        target_sequence = try appendDerivedBatchRecord(self, pending_batch);
        self.executor.notifySequence(target_sequence);
        if (self.enrichment_runtime) |runtime| runtime.notifySequence(target_sequence);
        self.notifyResolverReplayRuntimes(target_sequence);
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

pub fn appendGeneratedEnrichmentsFromStoredDocs(
    self: *DB,
    alloc: Allocator,
    force_generated_artifact_names: []const []const u8,
    skip_terminally_covered: bool,
) !GeneratedEnrichmentReplayAppendResult {
    if (self.enrichment_runtime == null) return .{};

    const chunk_size: usize = 128;
    const initial_lower = try self.core.documentRangeLowerAlloc("");
    defer self.core.alloc.free(initial_lower);
    var lower = try alloc.dupe(u8, initial_lower);
    defer alloc.free(lower);
    var lower_exclusive = false;

    var generated_ref_count: usize = 0;
    var latest_sequence: u64 = 0;
    while (true) {
        var replay_batch = try self.collectStoredGeneratedReplayBatch(alloc, lower, lower_exclusive, chunk_size, .{
            .max_records = std.math.maxInt(usize),
            .max_bytes = std.math.maxInt(usize),
        });
        defer replay_batch.deinit(alloc);
        if (replay_batch.writes.len == 0) break;

        var pending_batch = derived_types.DerivedBatch{};
        defer derived_types.deinitDerivedBatch(self.alloc, &pending_batch);
        try appendGeneratedEnrichments(self, &pending_batch, .{
            .writes = replay_batch.writes,
            .sync_level = .write,
        }, replay_batch.extracted, force_generated_artifact_names, skip_terminally_covered);
        if (pending_batch.generated_enrichment_refs.len != 0) {
            generated_ref_count += pending_batch.generated_enrichment_refs.len;
            const sequence = try appendDerivedBatchRecord(self, pending_batch);
            latest_sequence = @max(latest_sequence, sequence);
        }

        const next_lower = replay_batch.next_lower orelse break;
        replay_batch.next_lower = null;
        alloc.free(lower);
        lower = next_lower;
        lower_exclusive = true;
    }
    if (latest_sequence != 0) {
        self.executor.notifySequence(latest_sequence);
        if (self.enrichment_runtime) |runtime| runtime.notifySequence(latest_sequence);
        self.notifyResolverReplayRuntimes(latest_sequence);
    }
    return .{
        .generated_ref_count = generated_ref_count,
        .target_sequence = latest_sequence,
    };
}

pub fn replayGeneratedEnrichmentsFromStoredDocs(self: *DB, alloc: Allocator) !usize {
    if (comptime storage_engine.enabled) return storage_engine.call(@src().fn_name, .{ self, alloc });
    return (try appendGeneratedEnrichmentsFromStoredDocs(self, alloc, &.{}, true)).generated_ref_count;
}

pub fn reprocessGeneratedEnrichmentFromStoredDocs(
    self: *DB,
    alloc: Allocator,
    artifact_name: ?[]const u8,
) !usize {
    return (try self.reprocessGeneratedEnrichmentFromStoredDocsWithProgress(alloc, artifact_name)).generated_ref_count;
}

pub fn reprocessGeneratedEnrichmentFromStoredDocsWithProgress(
    self: *DB,
    alloc: Allocator,
    artifact_name: ?[]const u8,
) !GeneratedEnrichmentReplayAppendResult {
    if (comptime storage_engine.enabled) return storage_engine.call(@src().fn_name, .{ self, alloc, artifact_name });
    if (artifact_name) |name| {
        const force_artifacts = [_][]const u8{name};
        return try appendGeneratedEnrichmentsFromStoredDocs(self, alloc, &force_artifacts, false);
    }
    return try appendGeneratedEnrichmentsFromStoredDocs(self, alloc, &.{}, false);
}

pub fn enrichmentStatsWithSupervisorState(self: *DB, fallback: types.EnrichmentStats) types.EnrichmentStats {
    // Status is called while the DB apply lock is held. Structural
    // mutations hold lifecycle while stopping and joining the enrichment
    // worker, and that worker can be waiting for apply. Never close the
    // lifecycle -> worker join -> apply -> lifecycle cycle here.
    const lifecycle_locked = self.async_context.enrichment_lifecycle_mutex.tryLock();
    defer if (lifecycle_locked) self.async_context.enrichment_lifecycle_mutex.unlock();
    var enrichment_status = if (lifecycle_locked)
        if (self.async_context.enrichment_runtime) |runtime|
            runtime.stats()
        else
            persistedEnrichmentStats(self) catch fallback
    else
        fallback;
    const desired = self.async_context.enrichment_desired_running.load(.acquire);
    // `desired` is the lock-free source of truth for whether this DB owns
    // an enrichment producer. A retained query-only snapshot may have
    // `enabled=false`, and lifecycle contention prevents us from replacing
    // it with runtime.stats(). Never let that observation gap turn a live
    // or restarting producer into a disabled one: public readiness treats
    // enabled + retrying as transient debt and remains fail-closed until a
    // later poll can observe the worker directly.
    if (desired) enrichment_status.enabled = true;
    const started = if (lifecycle_locked)
        if (self.async_context.enrichment_runtime) |runtime| runtime.isStarted() else false
    else
        false;
    if (desired and !started) {
        // While lifecycle owns the runtime pointer, expose the persisted
        // snapshot as a supervised transition rather than a fatal worker.
        const supervised = !lifecycle_locked or self.async_context.enrichment_restart_owner.state.load(.acquire) != 0;
        enrichment_status.retrying = supervised;
        enrichment_status.worker_failed = !supervised;
        enrichment_status.projection_checkpoint_status = if (supervised) "retrying" else "failed";
    }
    return enrichment_status;
}

pub fn clearEmbeddingActivityObservations(indexes: []types.DBIndexStats) void {
    for (indexes) |*item| {
        if (item.kind != .dense_vector and item.kind != .sparse_vector) continue;
        item.embedding_activity_observed = false;
        item.embedding_activity_sample_fresh = false;
        item.embedding_activity = .{};
    }
}

/// Cheap startup audit for the crash window where generated replay has no
/// remaining watermark debt but the current index incarnation still lacks
/// terminal outcomes for source documents. This deliberately avoids both
/// the full stats path and a primary corpus scan. Current writers maintain
/// the range cardinality transactionally; a missing cardinality (for
/// example, an older restored corpus) is conservatively handed to the
/// paged recovery owner instead of delaying availability to reconstruct it.
pub fn generatedCoverageRecoveryConfigsAlloc(self: *DB, alloc: Allocator) ![]types.IndexConfig {
    if (!self.core.hasGeneratedEnrichmentTargets()) return try alloc.alloc(types.IndexConfig, 0);
    lockApplyShared(self);
    defer self.core.unlockApplyShared();

    const primary_doc_count = try range_cardinality.loadOrProveEmpty(alloc, self.core.store);
    if (primary_doc_count != null and primary_doc_count.? == 0)
        return try alloc.alloc(types.IndexConfig, 0);
    const managed_indexes = try self.core.managedIndexes(alloc);
    defer {
        for (managed_indexes) |index_ref| alloc.free(@constCast(index_ref.name));
        alloc.free(managed_indexes);
    }
    var configs = std.ArrayListUnmanaged(types.IndexConfig).empty;
    errdefer {
        for (configs.items) |*cfg| cfg.deinit(alloc);
        configs.deinit(alloc);
    }
    var recovery_plan = try self.core.index_manager.acquireWritePlanSnapshot();
    defer recovery_plan.release();
    for (managed_indexes) |index_ref| {
        if (!try self.core.indexRequiresEnrichmentReplay(index_ref.name)) continue;
        // Local text/graph producers maintain direct-source completion
        // separately from vector outcomes. Missing historical receipts
        // belong to the existing durable paged recovery owner.
        if (index_ref.kind != .dense_vector and index_ref.kind != .sparse_vector) {
            if (index_ref.kind != .full_text and index_ref.kind != .graph) continue;
            const source_names = try self.core.index_manager.artifactSourceNamesForIndexAlloc(alloc, index_ref.name);
            defer {
                for (source_names) |source_name| alloc.free(source_name);
                alloc.free(source_names);
            }
            if (source_names.len == 0) continue;
            var read = try self.core.store.beginReadTxn();
            defer read.abort();
            if (try artifact_publication.authority(&read) != null) continue;
            const generation = self.core.index_manager.coverageGenerationForIndex(index_ref.name) orelse continue;
            var complete = primary_doc_count != null;
            for (source_names) |source_name| {
                const scope = try artifact_producer_readiness.localScopeAlloc(alloc, index_ref.name, source_name, recovery_plan.plan());
                defer alloc.free(scope);
                const counts = try DerivedCoverageCounters.load(alloc, &read, scope, generation);
                // A retained terminal failure is known producer debt, not
                // missing migration evidence. Its repair/retry owner keeps
                // readiness unhealthy without reinvoking it on every open.
                complete = complete and counts.produced != null and counts.skipped != null and
                    counts.produced.? +| counts.skipped.? +| (counts.terminal_failed orelse 0) == primary_doc_count.?;
            }
            if (complete) continue;
            const cfg = self.core.index_manager.get(index_ref.name) orelse continue;
            try configs.append(alloc, try types.IndexConfig.clone(alloc, cfg.*));
            continue;
        }
        if (!try self.core.indexRequiresEnrichmentReplay(index_ref.name)) continue;
        const cfg = self.core.index_manager.get(index_ref.name) orelse continue;
        var item = types.DBIndexStats{ .name = index_ref.name, .kind = index_ref.kind };
        initializeDerivedCoverageIdentity(cfg.*, &item);
        if (item.coverage_identity_ready) {
            _ = try self.populateDerivedCoverageCounts(
                index_ref.name,
                item.coverage_generation,
                item.coverage_config_hash,
                &item,
            );
        }
        const settled = item.coverage_produced_count +|
            item.coverage_skipped_count +|
            item.coverage_terminal_failed_count;
        if (primary_doc_count) |source_total| {
            if (item.coverage_identity_ready and item.coverage_summary_ready and settled >= source_total) continue;
        }
        try configs.append(alloc, try types.IndexConfig.clone(alloc, cfg.*));
    }
    return try configs.toOwnedSlice(alloc);
}

pub fn generatedCoverageReplayNeeded(self: *DB, alloc: Allocator) !bool {
    if (comptime storage_engine.enabled) return storage_engine.call(@src().fn_name, .{ self, alloc });
    const configs = try self.generatedCoverageRecoveryConfigsAlloc(alloc);
    defer {
        for (configs) |*cfg| cfg.deinit(alloc);
        alloc.free(configs);
    }
    return configs.len != 0;
}

/// Returns true when every incomplete generated projection is already
/// represented by an exact, live initial-build intent. This is the durable
/// ownership proof used by a resident writer: ordinary provider/backfill
/// latency must not be reclassified as broad startup repair merely because
/// the enrichment watermark is momentarily idle.
pub fn generatedCoverageRecoveryOwnedByInitialBuild(self: *DB, alloc: Allocator) !bool {
    if (comptime storage_engine.enabled) return storage_engine.call(@src().fn_name, .{ self, alloc });
    const configs = try self.generatedCoverageRecoveryConfigsAlloc(alloc);
    defer {
        for (configs) |*cfg| cfg.deinit(alloc);
        alloc.free(configs);
    }
    if (configs.len == 0) return false;

    var state = self.loadIndexRepairState(alloc) catch |err| switch (err) {
        error.FileNotFound, error.DurableIndexRepairStateUnavailable => return false,
        else => return err,
    };
    defer state.deinit(alloc);
    for (configs) |cfg| {
        if (!self.currentInitialBuildIntentMatches(&state, cfg)) return false;
    }
    return true;
}

/// Composed (fused/hybrid) search resolves filter_text/exclusion_text and
/// filter_query_json/exclusion_query_json exactly once, against a single
/// shared doc-number/ordinal space, before fanning out to the text/dense/
/// sparse arms (see `searchComposed`). Each per-arm search (searchTextQuery,
/// searchDense) already lifts these into applyStoredSearchPatternFilters'
/// parent-aware matcher for its OWN chunk-backed member/chunk-mode index,
/// but a single shared resolution can never be correct for more than one
/// doc-number space at once: whenever any arm in this composed request is
/// chunk-backed and returns member/chunk-level hits, the clauses must be
/// left unresolved here so each arm's own per-arm handling covers it
/// instead (issue #931 - fused/hybrid filter_query, exclusion_query, and
/// query.bool.filter/must_not).
pub fn composedRequestTouchesChunkBackedMemberModeArm(self: *DB, alloc: Allocator, req: types.SearchRequest) !bool {
    if (req.return_mode != .member and req.return_mode != .chunk) return false;

    if (req.full_text_queries.len > 0) {
        for (req.full_text_queries) |named| {
            if (try self.core.textIndexIsChunkBacked(alloc, named.index_name)) return true;
        }
    } else if (req.full_text != null or (!db_query_search.isDefaultMatchAll(req.query) and db_query_search.isTextQuery(req.query))) {
        if (try self.core.textIndexIsChunkBacked(alloc, req.index_name)) return true;
    }

    if (req.dense_queries.len > 0) {
        for (req.dense_queries) |named| {
            if (self.core.denseIndex(named.index_name)) |entry| {
                if (entry.chunk_name != null) return true;
            }
        }
    } else if (req.dense != null) {
        if (self.core.denseIndex(req.index_name)) |entry| {
            if (entry.chunk_name != null) return true;
        }
    }

    if (req.sparse_queries.len > 0) {
        for (req.sparse_queries) |named| {
            if (self.core.sparseIndex(named.index_name)) |entry| {
                if (entry.chunk_name != null) return true;
            }
        }
    } else if (req.sparse != null) {
        if (self.core.sparseIndex(req.index_name)) |entry| {
            if (entry.chunk_name != null) return true;
        }
    }

    return false;
}

/// Member/chunk-mode chunk hits are a separate full-text/vector document
/// from their parent row, so resolving `filter_text`/`exclusion_text`
/// (query.bool.filter/must_not) against the chunk-backed index's own
/// doc-number/ordinal space can never see a parent-only field (issue
/// #931). Convert the clauses that `applyStoredSearchPatternFilters`'
/// stored-pattern matcher (graph_exec.compilePatternFilter) can also
/// express, and fold them into filter_query_json/exclusion_query_json so
/// they reach that parent-aware matcher instead. Returns
/// error.UnsupportedQueryRequest for a clause the stored-pattern matcher
/// cannot represent (e.g. an analyzed `match`/`phrase`), rather than
/// silently dropping or misapplying it.
pub fn liftChunkBoolFilterClausesAlloc(
    alloc: Allocator,
    filter_text: ?types.TextQuery,
    exclusion_text: ?types.TextQuery,
    existing_filter_query_json: []const u8,
    existing_exclusion_query_json: []const u8,
) !ChunkBoolFilterLowering {
    var out = ChunkBoolFilterLowering{};
    errdefer out.deinit(alloc);
    if (filter_text == null and exclusion_text == null) return out;

    var arena = std.heap.ArenaAllocator.init(alloc);
    defer arena.deinit();
    const arena_alloc = arena.allocator();

    if (filter_text) |text_query| {
        var budget = ChunkBoolFilterLoweringBudget{};
        const lowered = try lowerTextQueryToStoredPatternValueAlloc(arena_alloc, text_query, 0, &budget);
        out.filter_query_json = try combineStoredPatternJsonAlloc(alloc, arena_alloc, existing_filter_query_json, lowered, .conjuncts);
    }
    if (exclusion_text) |text_query| {
        var budget = ChunkBoolFilterLoweringBudget{};
        const lowered = try lowerTextQueryToStoredPatternValueAlloc(arena_alloc, text_query, 0, &budget);
        out.exclusion_query_json = try combineStoredPatternJsonAlloc(alloc, arena_alloc, existing_exclusion_query_json, lowered, .disjuncts);
    }
    return out;
}

pub fn textIndexIsChunkBackedCallback(
    ctx: ?*anyopaque,
    alloc: Allocator,
    index_name: ?[]const u8,
) anyerror!bool {
    const self: *DB = @ptrCast(@alignCast(ctx orelse return error.InvalidArgument));
    return try self.core.textIndexIsChunkBacked(alloc, index_name);
}

test "db enrichment enabled requires backend runtime io" {
    const alloc = std.testing.allocator;

    var runtime = try background_runtime_mod.BackendRuntimeHandle.init(alloc, .{
        .backend = .manual,
        .filesystem_io = std.testing.io,
    });
    defer runtime.deinit();

    var deterministic = embedder_mod.DeterministicDenseEmbedder{};
    var path_tmp = try TestDirectory.init("db");
    defer path_tmp.cleanup();
    const path = path_tmp.path().ptr;

    try std.testing.expectError(error.MissingBackendRuntimeIo, DB.open(alloc, std.mem.span(path), .{
        .backend_runtime = runtime.ptr(),
        .executor = .{ .backend = .manual },
        .enrichment = .{ .dense_embedder = deterministic.interface() },
    }));
}

test "db enrichment reconfigure preserves active runtime when replacement cannot initialize" {
    const alloc = std.testing.allocator;

    var deterministic = embedder_mod.DeterministicDenseEmbedder{};
    const t = try TestDb.open(alloc, .{
        .enrichment = .{ .dense_embedder = deterministic.interface() },
    });
    defer t.deinit();
    const db = &t.db;
    const original_backend_runtime = db.backend_runtime;
    defer db.backend_runtime = original_backend_runtime;

    const original_runtime = db.enrichment_runtime orelse return error.TestUnexpectedResult;
    var manual_runtime = try background_runtime_mod.BackendRuntimeHandle.init(alloc, .{ .backend = .manual });
    defer manual_runtime.deinit();

    db.backend_runtime = manual_runtime.ptr();
    var replacement_embedder = embedder_mod.DeterministicDenseEmbedder{};
    try std.testing.expectError(error.MissingBackendRuntimeIo, db.reconfigureEnrichmentRuntime(.{
        .dense_embedder = replacement_embedder.interface(),
    }));
    try std.testing.expectEqual(original_runtime, db.enrichment_runtime.?);
}

test "db enrichment reconfigure inherits resident execution and security capabilities" {
    const alloc = std.testing.allocator;
    var remote_content = scraping.RemoteContentConfig{
        .security = .{ .block_private_ips = false },
    };
    const t = try TestDb.open(alloc, .{
        .remote_content = &remote_content,
    });
    defer t.deinit();
    const db = &t.db;

    var replacement_embedder = embedder_mod.DeterministicDenseEmbedder{};
    try db.reconfigureEnrichmentRuntime(.{
        .dense_embedder = replacement_embedder.interface(),
    });

    const runtime = db.enrichment_runtime orelse return error.TestUnexpectedResult;
    try std.testing.expectEqual(&remote_content, runtime.config.remote_content.?);
    try std.testing.expectEqual(db.core.index_manager.resource_manager, runtime.config.resource_manager);
    try std.testing.expect(runtime.config.io != null);
}

test "db enrichment reconfigure refreshes durable state after old worker joins" {
    const alloc = std.testing.allocator;
    var original_embedder = embedder_mod.DeterministicDenseEmbedder{};
    const t = try TestDb.open(alloc, .{
        .start_optional_runtime_workers = false,
        .enrichment = .{ .dense_embedder = original_embedder.interface() },
    });
    defer t.deinit();
    const db = &t.db;
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
    const original_runtime = db.enrichment_runtime.?;
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
            var status = try enrichment_state.loadRuntimeStatus(
                target_db.runtime_alloc,
                target_db.core.store,
                enrichment_runtime_mod.scope_name,
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
            try enrichment_state.saveRuntimeStatus(
                target_db.core.store,
                enrichment_runtime_mod.scope_name,
                status,
            );
        }
    };
    DB.test_enrichment_reconfigure_after_detached_hook = LateOldWorkerPublish.run;
    defer DB.test_enrichment_reconfigure_after_detached_hook = null;

    var replacement_embedder = embedder_mod.DeterministicDenseEmbedder{};
    try db.reconfigureEnrichmentRuntime(.{ .dense_embedder = replacement_embedder.interface() });
    const replacement = db.enrichment_runtime.?;
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

    const persisted = try enrichment_state.loadRuntimeStatus(
        alloc,
        db.core.store,
        enrichment_runtime_mod.scope_name,
    );
    try std.testing.expectEqual(@as(u32, 3), persisted.consecutive_retry_count);
    try std.testing.expectEqual(@as(u64, 0xfeed), persisted.retry_failure_fingerprint);
    try std.testing.expectEqual(@as(u32, 2), persisted.retry_failure_count);
    try std.testing.expect(persisted.worker_failed);
}

test "db enrichment restart supervisor recovers transient start failures" {
    const alloc = std.testing.allocator;
    var deterministic = embedder_mod.DeterministicDenseEmbedder{};
    const t = try TestDb.open(alloc, .{
        .enrichment = .{ .dense_embedder = deterministic.interface() },
    });
    defer t.deinit();
    const db = &t.db;
    const runtime = db.enrichment_runtime orelse return error.TestUnexpectedResult;
    try std.testing.expect(runtime.isStarted());
    try std.testing.expect(db.quiesceEnrichmentForStructuralMutation());
    try std.testing.expect(!runtime.isStarted());

    DB.test_enrichment_restart_failures_remaining.store(std.math.maxInt(u32), .release);
    defer DB.test_enrichment_restart_failures_remaining.store(0, .release);
    try std.testing.expectError(
        error.TestTransientEnrichmentRestart,
        db.restartEnrichmentAfterStructuralMutation("test", "semantic_idx"),
    );
    const degraded = db.enrichmentStatsWithSupervisorState(.{});
    try std.testing.expect(degraded.retrying);
    try std.testing.expect(!degraded.worker_failed);
    DB.test_enrichment_restart_failures_remaining.store(12, .release);
    db.backend_runtime.durable_jobs.drainOwner(db.repair_cleanup_owner_id);

    try std.testing.expectEqual(@as(u32, 0), DB.test_enrichment_restart_failures_remaining.load(.acquire));
    try std.testing.expect(db.async_context.enrichment_desired_running.load(.acquire));
    try std.testing.expect(runtime.isStarted());
}

test "db enrichment status does not wait for lifecycle mutation" {
    const alloc = std.testing.allocator;
    var deterministic = embedder_mod.DeterministicDenseEmbedder{};
    const t = try TestDb.open(alloc, .{
        .enrichment = .{ .dense_embedder = deterministic.interface() },
    });
    defer t.deinit();
    const db = &t.db;

    lockAtomicWithBackoff(&db.async_context.enrichment_lifecycle_mutex);
    const fallback = types.EnrichmentStats{
        .target_sequence = 41,
        .applied_sequence = 37,
    };
    const status = db.enrichmentStatsWithSupervisorState(fallback);
    db.async_context.enrichment_lifecycle_mutex.unlock();

    try std.testing.expect(status.enabled);
    try std.testing.expectEqual(@as(u64, 41), status.target_sequence);
    try std.testing.expectEqual(@as(u64, 37), status.applied_sequence);
    try std.testing.expect(status.retrying);
    try std.testing.expect(!status.worker_failed);
}

test "db embeddings index remoteMedia accepts bmp via shared image decode" {
    const alloc = std.testing.allocator;
    template_remote.setHostRenderer(.{
        .render_json_to_parts = testRemoteMediaHostRenderJsonToParts,
    });
    defer template_remote.setHostRenderer(null);

    var image_embedder = ImageDecodingPartsEmbedder{};
    const t = try TestDb.open(alloc, .{
        .enrichment = .{ .dense_embedder = image_embedder.interface() },
        .start_index_workers = false,
        .ttl_cleanup = .{ .enabled = false },
    });
    defer t.deinit();
    const db = &t.db;

    try db.addIndex(.{
        .name = "visual_idx",
        .kind = .dense_vector,
        .config_json = "{\"field\":\"embedding\",\"dims\":3,\"generator\":{\"kind\":\"dense_embedding\",\"source_field\":\"photo\",\"source_template\":\"{{remoteMedia url=photo}}\"}}",
    });

    const photo = try dataUriAlloc(alloc, "image/bmp", &remote_media_bmp_24_2x2);
    defer alloc.free(photo);
    const value = try std.fmt.allocPrint(alloc, "{{\"photo\":{f}}}", .{std.json.fmt(photo, .{})});
    defer alloc.free(value);

    try db.batch(.{
        .writes = &.{.{ .key = "doc:bmp", .value = value }},
        .sync_level = .full_index,
    });
    try std.testing.expectEqual(@as(usize, 1), image_embedder.calls);

    var result = try db.searchDenseProfiled(alloc, .{
        .index_name = "visual_idx",
        .limit = 1,
        .include_stored = false,
    }, .{ .vector = &.{ 1.0, 0.0, 0.0 }, .k = 1 });
    defer result.result.deinit();

    try std.testing.expectEqual(@as(u32, 1), result.result.total_hits);
    try std.testing.expectEqualStrings("doc:bmp", result.result.hits[0].id);
}

test "db embeddings index remoteMedia accepts webp via shared image decode" {
    const alloc = std.testing.allocator;
    template_remote.setHostRenderer(.{
        .render_json_to_parts = testRemoteMediaHostRenderJsonToParts,
    });
    defer template_remote.setHostRenderer(null);

    var image_embedder = ImageDecodingPartsEmbedder{};
    const t = try TestDb.open(alloc, .{
        .enrichment = .{ .dense_embedder = image_embedder.interface() },
        .start_index_workers = false,
        .ttl_cleanup = .{ .enabled = false },
    });
    defer t.deinit();
    const db = &t.db;

    try db.addIndex(.{
        .name = "visual_idx",
        .kind = .dense_vector,
        .config_json = "{\"field\":\"embedding\",\"dims\":3,\"generator\":{\"kind\":\"dense_embedding\",\"source_field\":\"photo\",\"source_template\":\"{{remoteMedia url=photo}}\"}}",
    });

    const webp_bytes = try std.Io.Dir.cwd().readFileAlloc(
        std.testing.io,
        "testdata/image/webp/lossy/minimal-vp8-1x1.webp",
        alloc,
        .limited(64 * 1024),
    );
    defer alloc.free(webp_bytes);

    const photo = try dataUriAlloc(alloc, "image/webp", webp_bytes);
    defer alloc.free(photo);
    const value = try std.fmt.allocPrint(alloc, "{{\"photo\":{f}}}", .{std.json.fmt(photo, .{})});
    defer alloc.free(value);

    try db.batch(.{
        .writes = &.{.{ .key = "doc:webp", .value = value }},
        .sync_level = .full_index,
    });
    try std.testing.expectEqual(@as(usize, 1), image_embedder.calls);

    var result = try db.searchDenseProfiled(alloc, .{
        .index_name = "visual_idx",
        .limit = 1,
        .include_stored = false,
    }, .{ .vector = &.{ 0.0, 1.0, 0.0 }, .k = 1 });
    defer result.result.deinit();

    try std.testing.expectEqual(@as(u32, 1), result.result.total_hits);
    try std.testing.expectEqualStrings("doc:webp", result.result.hits[0].id);
}

test "db enrichment status changes notify query visibility hook" {
    const alloc = std.testing.allocator;

    var deterministic = embedder_mod.DeterministicDenseEmbedder{};
    const t = try TestDb.open(alloc, .{
        .enrichment = .{ .dense_embedder = deterministic.interface() },
        // Assert the explicit status edge without racing startup/replay
        // notifications from independently scheduled workers.
        .start_index_workers = false,
        .start_optional_runtime_workers = false,
    });
    defer t.deinit();
    const db = &t.db;

    const HookCtx = struct {
        status_calls: u64 = 0,
        saw_db: bool = false,
        change: ?QueryVisibilityChange = null,

        fn onChange(ptr: *anyopaque, event: QueryVisibilityEvent) void {
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
    try db.enrichment_runtime.?.markAppliedThrough(1);

    try std.testing.expectEqual(@as(u64, 1), hook_ctx.status_calls);
    try std.testing.expectEqual(QueryVisibilityChange.status, hook_ctx.change.?);

    try db.enrichment_runtime.?.markAppliedThrough(2);
    try std.testing.expectEqual(@as(u64, 2), hook_ctx.status_calls);
    try std.testing.expectEqual(QueryVisibilityChange.status, hook_ctx.change.?);
}

test "db generated downstream indexes are exact convergence targets at source commit" {
    const alloc = std.testing.allocator;

    const t = try TestDb.open(alloc, .{
        .start_index_workers = false,
        .executor = .{ .backend = .manual },
    });
    defer t.deinit();
    const db = &t.db;
    try db.addIndex(test_index.semantic);
    try db.addIndex(test_index.relations_graph);

    var replay_targets = try collectManagedSyncTargetsForRecord(alloc, db.core.index_manager, .{
        .sequence = 1,
        .changed_doc_keys = &.{"doc:a"},
        .target_hints = &.{.enrichment},
    });
    defer replay_targets.deinit(alloc);
    try std.testing.expectEqual(@as(usize, 0), replay_targets.all_indexes.len);
    try std.testing.expect(replay_targets.target_scope_known);
    try std.testing.expectEqual(@as(usize, 2), replay_targets.target_identities.len);
    // Producer reruns can retire generated members, even when the source
    // commit itself adds a document.
    for (replay_targets.target_identities) |target|
        try std.testing.expectEqual(IndexTargetVisibility.ServingSetEffect.may_reduce, target.serving_set_effect);

    var materialized_targets = try collectManagedSyncTargets(alloc, db.core.index_manager, .{
        .documents = &.{.{ .key = "doc:a", .cleaned_value = "{}" }},
    });
    defer materialized_targets.deinit(alloc);
    // Whole-document dense generation can be applied directly by the
    // materialized path; graph production remains asynchronous. Both still
    // belong to the commit-time visibility target set.
    try std.testing.expectEqual(@as(usize, 1), materialized_targets.all_indexes.len);
    try std.testing.expectEqualStrings("semantic", materialized_targets.all_indexes[0]);
    try std.testing.expect(materialized_targets.target_scope_known);
    try std.testing.expectEqual(@as(usize, 2), materialized_targets.target_identities.len);
    for (materialized_targets.target_identities) |target|
        try std.testing.expectEqual(IndexTargetVisibility.ServingSetEffect.may_reduce, target.serving_set_effect);

    var deletion_targets = try collectManagedSyncTargets(alloc, db.core.index_manager, .{
        .deleted_keys = &.{"doc:a"},
    });
    defer deletion_targets.deinit(alloc);
    try std.testing.expect(deletion_targets.target_scope_known);
    try std.testing.expectEqual(@as(usize, 2), deletion_targets.target_identities.len);
    for (deletion_targets.target_identities) |target|
        try std.testing.expectEqual(IndexTargetVisibility.ServingSetEffect.may_reduce, target.serving_set_effect);

    // Internal replay/control cleanup is not a user deletion and must not
    // grant every index permission to replace a newer serving snapshot with a
    // lower one. An exact consumed artifact deletion remains reducing.
    var metadata_delete_targets = try collectManagedSyncTargets(alloc, db.core.index_manager, .{
        .deleted_keys = &.{internal_keys.replay_meta_next_sequence_key[0..]},
    });
    defer metadata_delete_targets.deinit(alloc);
    try std.testing.expectEqual(@as(usize, 0), metadata_delete_targets.target_identities.len);

    var storage_metadata_delete_targets = try collectManagedSyncTargets(alloc, db.core.index_manager, .{
        .deleted_keys = &.{"\x00\x00__metadata__:manifest"},
    });
    defer storage_metadata_delete_targets.deinit(alloc);
    try std.testing.expectEqual(@as(usize, 0), storage_metadata_delete_targets.target_identities.len);

    const unrelated_artifact = try internal_keys.embeddingArtifactKeyForDocumentAlloc(alloc, "doc:a", "unrelated");
    defer alloc.free(unrelated_artifact);
    var unrelated_delete_targets = try collectManagedSyncTargets(alloc, db.core.index_manager, .{
        .deleted_keys = &.{unrelated_artifact},
        .changed_artifact_keys = &.{unrelated_artifact},
    });
    defer unrelated_delete_targets.deinit(alloc);
    try std.testing.expectEqual(@as(usize, 0), unrelated_delete_targets.all_indexes.len);

    const consumed_artifact = try internal_keys.embeddingArtifactKeyForDocumentAlloc(alloc, "doc:a", "semantic");
    defer alloc.free(consumed_artifact);
    var consumed_delete_targets = try collectManagedSyncTargets(alloc, db.core.index_manager, .{
        .deleted_keys = &.{consumed_artifact},
        .changed_artifact_keys = &.{consumed_artifact},
    });
    defer consumed_delete_targets.deinit(alloc);
    try std.testing.expectEqual(@as(usize, 1), consumed_delete_targets.target_identities.len);
    try std.testing.expectEqualStrings("semantic", consumed_delete_targets.target_identities[0].index_name);
    try std.testing.expectEqual(
        IndexTargetVisibility.ServingSetEffect.may_reduce,
        consumed_delete_targets.target_identities[0].serving_set_effect,
    );

    const HookCtx = struct {
        calls: usize = 0,
        target_scope_known: bool = false,
        target_count: usize = 0,
        saw_dense: bool = false,
        saw_graph: bool = false,

        fn onChange(ptr: *anyopaque, event: QueryVisibilityEvent) void {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            if (event.change != .target_advanced) return;
            self.calls += 1;
            self.target_scope_known = event.target_scope_known;
            self.target_count = event.target_indexes.len;
            for (event.target_indexes) |target| {
                if (std.mem.eql(u8, target.index_name, "semantic")) {
                    self.saw_dense = target.kind == .dense_vector and
                        target.incarnation != 0 and target.config_hash != 0;
                }
                if (std.mem.eql(u8, target.index_name, "relations_graph")) {
                    self.saw_graph = target.kind == .graph and
                        target.incarnation != 0 and target.config_hash != 0;
                }
            }
        }
    };
    var hook_ctx = HookCtx{};
    db.setQueryVisibilityHook(.{
        .ptr = &hook_ctx,
        .on_change = HookCtx.onChange,
    });

    // Both indexes are complete for the empty source before this write. The
    // source commit must fence their exact convergence identities immediately;
    // waiting for the later generated artifact commit leaves a false-complete
    // window for status and backup consumers.
    try db.batch(.{
        .writes = &.{.{
            .key = "doc:a",
            .value = "{\"body\":\"alpha\",\"relations\":[]}",
        }},
        .sync_level = .write,
    });

    try std.testing.expectEqual(@as(usize, 1), hook_ctx.calls);
    try std.testing.expect(hook_ctx.target_scope_known);
    try std.testing.expectEqual(@as(usize, 2), hook_ctx.target_count);
    try std.testing.expect(hook_ctx.saw_dense);
    try std.testing.expect(hook_ctx.saw_graph);
}

test "db generated downstream indexes are exact convergence targets at source commit including implicit chunk deletion" {
    const alloc = std.testing.allocator;
    const t = try TestDb.open(alloc, .{
        .start_index_workers = false,
        .executor = .{ .backend = .manual },
    });
    defer t.deinit();
    const db = &t.db;
    try db.addIndex(.{ .name = "default", .kind = .full_text, .config_json = "{}" });
    try db.addEnrichment(.{
        .name = "body_chunks",
        .kind = .chunk,
        .field = "body",
        .chunk_size = 64,
        .full_text_index = true,
    });
    const key = try internal_keys.chunkArtifactKeyAlloc(alloc, "doc:a", "body_chunks", 0);
    defer alloc.free(key);
    var context = try db.core.index_manager.acquireTextPublicationContext(alloc, "default");
    defer context.deinit();
    try std.testing.expect(try db.core.index_manager.textPublicationContextConsumesKeyAssumeCatalogLocked("default", context, key));
    const batch: derived_types.DerivedBatch = .{ .deleted_keys = &.{key} };
    var targets = try collectManagedSyncTargets(alloc, db.core.index_manager, batch);
    defer targets.deinit(alloc);
    try std.testing.expectEqual(@as(usize, 1), targets.all_indexes.len);
    try std.testing.expectEqualStrings("default", targets.all_indexes[0]);
    try std.testing.expectEqual(IndexTargetVisibility.ServingSetEffect.may_reduce, targets.target_identities[0].serving_set_effect);
    var replay_targets = try collectManagedSyncTargetsForRecord(alloc, db.core.index_manager, .{ .sequence = 7, .deleted_doc_keys = &.{key} });
    defer replay_targets.deinit(alloc);
    try std.testing.expectEqual(@as(usize, 1), replay_targets.all_indexes.len);
    const deletes = try collectTextReplayDeleteKeys(alloc, db.core.index_manager, batch, "default", context);
    defer alloc.free(deletes);
    try std.testing.expectEqual(@as(usize, 1), deletes.len);
    try std.testing.expectEqualStrings(key, deletes[0]);
}

test "db managed resolver changes fence in-flight replay and reset durable cursors" {
    for ([_]bool{ false, true }) |follower| {
        const alloc = std.testing.allocator;

        var sink = FakePromotionSink{ .alloc = alloc };
        defer sink.deinit();
        const t = try TestDb.open(alloc, .{ .start_resolver_workers = false, .entity_sink = sink.sink() });
        defer t.deinit();
        const db = &t.db;

        try db.addIndex(test_index.relations_graph);

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

        const cfg: index_manager_mod.ResolverConfig = .{
            .name = "kg_background",
            .table = "entities",
            .source_artifact = "relations_v1",
            .resolution_artifact = "resolution_background_v1",
            .key_template = "{{ lower _entity.label }}/{{ slug _entity.text }}",
            .config_generation = 1,
        };
        _ = try db.upsertResolverWithResultOptions(cfg, .{ .drain_backfill = false });
        // Leave generation-one decisions queued for promotion across DDL.
        try db.resolution_runtime.?.catchUp();
        try std.testing.expectEqual(@as(usize, 0), sink.count());
        var replacement = cfg;
        replacement.config_generation = 2;
        for ([_]*std.atomic.Mutex{ &db.resolution_runtime.?.catch_up_mutex, &db.promotion_runtime.?.catch_up_mutex }) |mutex| {
            try std.testing.expect(mutex.tryLock());
            defer mutex.unlock();
            try std.testing.expectEqual(index_manager_mod.IndexManager.ResolverUpsertResult.updated_no_backfill, try db.upsertResolverWithResultOptions(cfg, .{ .drain_backfill = false }));
            try std.testing.expectError(error.WriterLocked, db.upsertResolverWithResultOptions(replacement, .{ .drain_backfill = false }));
            try std.testing.expectError(error.WriterLocked, db.removeResolverWithoutDrain(cfg.name));
            var saved = (try db.resolverConfigByNameAlloc(cfg.name)).?;
            defer saved.deinit(alloc);
            try std.testing.expectEqual(@as(u64, 1), saved.config_generation);
        }
        {
            var txn = try db.core.store.beginWriteTxn();
            errdefer txn.abort();
            try txn.put(resolver_catalog_mod.reresolve_resume_key, "past-doc-a");
            try txn.put(resolver_catalog_mod.reresolve_repair_resume_key, "past-doc-a");
            try txn.commit();
        }
        _ = try db.upsertResolverWithResultOptions(replacement, .{ .drain_backfill = false });
        for ([_][]const u8{ resolver_catalog_mod.reresolve_resume_key, resolver_catalog_mod.reresolve_repair_resume_key }) |key| {
            const cursor = try db.core.store.get(alloc, key);
            defer alloc.free(cursor);
            try std.testing.expectEqualStrings("", cursor);
        }
        try db.promotion_runtime.?.catchUp();
        try std.testing.expectEqual(@as(usize, 0), sink.count());
        try db.runUntilIdle();
        try std.testing.expectEqual(@as(usize, 1), sink.count());
        const resolution_key = try internal_keys.resolutionArtifactKeyAlloc(alloc, "doc:a", cfg.resolution_artifact);
        defer alloc.free(resolution_key);
        const raw = try db.core.store.get(alloc, resolution_key);
        defer alloc.free(raw);
        try std.testing.expect(std.mem.indexOf(u8, raw, "\"config_generation\":2") != null);
        const FollowerOwner = struct {
            fn isLocal(_: *anyopaque) bool {
                return false;
            }
        };
        if (follower) db.setPromotionOwner(.{ .ptr = undefined, .vtable = &.{ .is_local_owner = FollowerOwner.isLocal } });
        // Retirement is durable replay work; keep the catalog until it finishes.
        try std.testing.expectError(error.WriterLocked, db.removeResolverWithoutDrain(cfg.name));
        try db.runUntilIdle();
        try std.testing.expect(try db.removeResolverWithoutDrain(cfg.name));
        try std.testing.expectError(error.NotFound, db.core.store.get(alloc, resolution_key));
    }
}

test "db enrichments precomputes generated enrichments into the committed batch" {
    const alloc = std.testing.allocator;

    var deterministic_dense = embedder_mod.DeterministicDenseEmbedder{};
    var deterministic_sparse = embedder_mod.DeterministicSparseEmbedder{};
    const t = try TestDb.open(alloc, .{
        .enrichment = .{
            .owner_id = "worker-a",
            .dense_embedder = deterministic_dense.interface(),
            .sparse_embedder = deterministic_sparse.interface(),
        },
    });
    defer t.deinit();
    const db = &t.db;

    try db.addIndex(test_index.dv_v1_dims3_generated);
    try db.addIndex(.{
        .name = "sp_v1",
        .kind = .sparse_vector,
        .config_json = "{\"field\":\"sparse_embedding\",\"generator\":{\"kind\":\"sparse_embedding\",\"source_field\":\"body\",\"artifact_name\":\"body_chunks_v1\",\"chunk_size\":8,\"chunk_overlap\":2}}",
    });
    try db.addIndex(test_index.ft_chunks);

    try db.batch(.{
        .writes = &.{
            .{ .key = "doc:a", .value = "{\"body\":\"abcdefghijklmno\"}" },
        },
        .sync_level = .enrichments,
    });
    try std.testing.expectEqual(@as(u64, 1), db.enrichment_runtime.?.stats().applied_sequence);

    const journal_entries = try replay_stream_mod.iterateFrom(alloc, db.core.store, 1);
    defer {
        for (journal_entries) |*entry| entry.deinit(alloc);
        alloc.free(journal_entries);
    }
    try std.testing.expectEqual(@as(usize, 1), journal_entries.len);

    var journal_record = try change_journal_mod.decodeRecord(alloc, journal_entries[0].payload);
    defer journal_record.deinit();

    try std.testing.expect(journal_record.record.changed_doc_keys.len >= 1);
    try std.testing.expectEqualStrings("doc:a", journal_record.record.changed_doc_keys[0]);
    try std.testing.expect(journalRecordHasHint(journal_record.record, .full_text));
    try std.testing.expect(journalRecordHasHint(journal_record.record, .dense_vector));
    try std.testing.expect(journalRecordHasHint(journal_record.record, .sparse_vector));
    try std.testing.expect(!journalRecordHasHint(journal_record.record, .enrichment));
    try std.testing.expect(journal_record.record.changed_artifact_keys.len > 0);
}

test "db enrichments precomputed watermark advances across replay entries without enrichment debt" {
    const alloc = std.testing.allocator;

    var deterministic_dense = embedder_mod.DeterministicDenseEmbedder{};
    // This probes foreground precomputed advancement; a concurrent replay
    // worker could consume the inert entry before the initial observation.
    const t = try TestDb.open(alloc, .{
        .start_optional_runtime_workers = false,
        .enrichment = .{
            .owner_id = "worker-a",
            .dense_embedder = deterministic_dense.interface(),
        },
    });
    defer t.deinit();
    const db = &t.db;

    const inert_payload = try change_journal_mod.encodeRecord(alloc, .{
        .sequence = 1,
        .changed_doc_keys = &.{"doc:before"},
        .target_hints = &.{.full_text},
    });
    defer alloc.free(inert_payload);
    try db.core.store.appendReplayOpaque(alloc, 1, inert_payload);
    try db.enrichment_runtime.?.resumeFrom(0, 0);
    try std.testing.expectEqual(@as(u64, 0), db.enrichment_runtime.?.stats().applied_sequence);

    try db.addIndex(test_index.dv_v1);

    try db.batch(.{
        .writes = &.{
            .{ .key = "doc:a", .value = "{\"body\":\"precomputed dense text\"}" },
        },
        .sync_level = .enrichments,
    });

    try std.testing.expectEqual(db.core.nextDerivedSequence(), db.enrichment_runtime.?.stats().applied_sequence);
}

test "db managed operator repair persists intent without running reconstruction inline" {
    const alloc = std.testing.allocator;

    const t = try TestDb.open(alloc, no_background_test_open);
    defer t.deinit();
    const db = &t.db;
    try db.addIndex(.{ .name = "graph_v1", .kind = .graph, .config_json = "{}" });

    var queued = try db.repairArtifactIssuesWithRequestOptions(alloc, .{
        .target = .index,
        .index_name = "graph_v1",
        .limit = 1,
        .force = true,
    }, .{ .defer_durable_index_repair_execution = true });
    defer queued.deinit(alloc);
    try std.testing.expectEqual(@as(u64, 1), queued.scanned);
    try std.testing.expectEqual(@as(u64, 1), queued.in_progress);
    try std.testing.expectEqual(@as(u64, 0), queued.indexes_rebuilt);
    try std.testing.expect(queued.debt_remaining);

    const repair_id = (try db.indexRepairIdForIndex(alloc, "graph_v1")) orelse
        return error.TestUnexpectedResult;
    var pending = try db.loadIndexRepairEntryById(alloc, repair_id);
    defer pending.deinit(alloc);
    try std.testing.expectEqual(index_repair_state.Phase.detected, pending.intent.phase);
    try std.testing.expect(pending.intent.candidate_relative_path == null);

    const completed = try db.advanceIndexRepairIntent(alloc, repair_id, .{});
    try std.testing.expect(completed.attempted);
    try std.testing.expect(completed.repaired);
    try std.testing.expect(!try db.hasPendingIndexRepairIntents(alloc));
}

test "db managed dense enrichment remains searchable after transient rate limits" {
    const alloc = std.testing.allocator;

    var path_tmp = try TestDirectory.init("db");
    defer path_tmp.cleanup();
    const path = path_tmp.path().ptr;

    // Admit the first request, then force the worker through the real
    // retryable-provider path until this test explicitly releases the gate.
    // Keeping this at the gate's default is part of the test contract: an
    // always-open provider would bypass transient recovery entirely.
    var gated = GateDenseEmbedder{};
    var db = try DB.open(alloc, std.mem.span(path), .{
        .enrichment = .{
            .owner_id = "worker-a",
            .dense_embedder = gated.interface(),
        },
    });
    defer db.close();

    try db.addIndex(test_index.semantic_idx);

    try db.batch(.{
        .writes = &.{
            .{ .key = "doc:a", .value = "{\"title\":\"alpha\",\"body\":\"alpha concept overview\"}" },
            .{ .key = "doc:b", .value = "{\"title\":\"beta\",\"body\":\"beta architecture notes\"}" },
            .{ .key = "doc:c", .value = "{\"title\":\"gamma\",\"body\":\"gamma implementation details\"}" },
        },
        .sync_level = .write,
    });

    var attempts: usize = 0;
    while (attempts < 200) : (attempts += 1) {
        const snapshot = gated.snapshot();
        if (snapshot.blocked_requests > 0 and snapshot.successful_requests >= 1) break;
        sleepNs(10 * std.time.ns_per_ms);
    }

    const before_release = gated.snapshot();
    try std.testing.expect(before_release.blocked_requests > 0);
    try std.testing.expect(before_release.successful_requests >= 1);

    gated.allowAll();

    var ready = false;
    var attempts_after_release: usize = 0;
    while (attempts_after_release < 500) : (attempts_after_release += 1) {
        const stats = try db.stats(alloc);
        defer types.freeDBStats(alloc, stats);
        for (stats.indexes) |item| {
            if (!std.mem.eql(u8, item.name, "semantic_idx")) continue;
            if (item.doc_count == 3 and
                item.replay_applied_sequence >= 2 and
                item.replay_applied_sequence == item.replay_target_sequence and
                !item.backfill_active)
            {
                ready = true;
            }
            break;
        }
        if (ready) break;
        sleepNs(10 * std.time.ns_per_ms);
    }
    try std.testing.expect(ready);

    var result = try db.search(alloc, .{
        .index_name = "semantic_idx",
        .dense = .{
            .vector = &.{ 1.0, 0.0, 0.0 },
            .k = 3,
        },
        .limit = 3,
    });
    defer result.deinit();
    try std.testing.expectEqual(@as(u32, 3), result.total_hits);
    try std.testing.expectEqualStrings("doc:a", result.hits[0].id);

    try db.runUntilIdle();
}

test "db managed dense enrichment retries temporary model capacity without terminal coverage" {
    const alloc = std.testing.allocator;

    var gated = GateDenseEmbedder{
        .allowed_successes = .init(0),
        .blocked_error = error.ResourceTemporarilyUnavailable,
    };
    const t = try TestDb.open(alloc, .{
        .enrichment = .{
            .owner_id = "worker-a",
            .dense_embedder = gated.interface(),
            .inline_retry_max_attempts = 1,
        },
    });
    defer t.deinit();
    const db = &t.db;

    try db.addIndex(test_index.semantic_idx);
    try db.batch(.{
        .writes = &.{
            .{ .key = "doc:a", .value = "{\"body\":\"alpha concept overview\"}" },
            .{ .key = "doc:b", .value = "{\"body\":\"beta architecture notes\"}" },
            .{ .key = "doc:c", .value = "{\"body\":\"gamma implementation details\"}" },
        },
        .sync_level = .write,
    });

    var observed_retry = false;
    var attempts: usize = 0;
    while (attempts < default_test_wait_attempts) : (attempts += 1) {
        const stats = try db.stats(alloc);
        if (gated.snapshot().blocked_requests > 0 and stats.enrichment.retryable_error_count > 0) {
            try std.testing.expect(stats.enrichment.retrying);
            try std.testing.expect(!stats.enrichment.worker_failed);
            try std.testing.expectEqual(@as(u64, 0), stats.enrichment.fatal_error_count);
            for (stats.indexes) |index_stats| {
                if (!std.mem.eql(u8, index_stats.name, "semantic_idx")) continue;
                try std.testing.expect(!index_stats.enrichment_failed);
                try std.testing.expectEqual(@as(u64, 0), index_stats.coverage_terminal_failed_count);
            }
            observed_retry = true;
        }
        types.freeDBStats(alloc, stats);
        if (observed_retry) break;
        sleepNs(10 * std.time.ns_per_ms);
    }
    try std.testing.expect(observed_retry);

    gated.allowAll();
    try db.runUntilIdle();

    const final_stats = try db.stats(alloc);
    defer types.freeDBStats(alloc, final_stats);
    try std.testing.expectEqual(@as(u64, 0), final_stats.enrichment.fatal_error_count);
    try std.testing.expect(!final_stats.enrichment.worker_failed);
    for (final_stats.indexes) |index_stats| {
        if (!std.mem.eql(u8, index_stats.name, "semantic_idx")) continue;
        try std.testing.expectEqual(@as(u64, 3), index_stats.doc_count);
        try std.testing.expectEqual(@as(u64, 3), index_stats.coverage_produced_count);
        try std.testing.expectEqual(@as(u64, 0), index_stats.coverage_terminal_failed_count);
        try std.testing.expect(!index_stats.enrichment_failed);
    }

    var result = try db.search(alloc, .{
        .index_name = "semantic_idx",
        .dense = .{
            .vector = &.{ 1.0, 0.0, 0.0 },
            .k = 3,
        },
        .limit = 3,
    });
    defer result.deinit();
    try std.testing.expectEqual(@as(u32, 3), result.total_hits);
    try std.testing.expectEqualStrings("doc:a", result.hits[0].id);
}

test "db managed sparse enrichment retries temporary model capacity without terminal coverage" {
    const alloc = std.testing.allocator;

    var gated = GateSparseEmbedder{};
    const t = try TestDb.open(alloc, .{
        .enrichment = .{
            .owner_id = "worker-a",
            .sparse_embedder = gated.interface(),
            .inline_retry_max_attempts = 1,
        },
    });
    defer t.deinit();
    const db = &t.db;

    try db.addIndex(.{
        .name = "sp_v1",
        .kind = .sparse_vector,
        .config_json = "{\"field\":\"sparse_embedding\",\"generator\":{\"kind\":\"sparse_embedding\",\"source_field\":\"body\"}}",
    });
    try db.batch(.{
        .writes = &.{.{ .key = "doc:a", .value = "{\"body\":\"alpha concept overview\"}" }},
        .sync_level = .write,
    });

    var observed_retry = false;
    var attempts: usize = 0;
    while (attempts < default_test_wait_attempts) : (attempts += 1) {
        const stats = try db.stats(alloc);
        if (gated.blocked_requests.load(.acquire) > 0 and stats.enrichment.retryable_error_count > 0) {
            try std.testing.expect(stats.enrichment.retrying);
            try std.testing.expect(!stats.enrichment.worker_failed);
            try std.testing.expectEqual(@as(u64, 0), stats.enrichment.fatal_error_count);
            for (stats.indexes) |index_stats| {
                if (!std.mem.eql(u8, index_stats.name, "sp_v1")) continue;
                try std.testing.expect(!index_stats.enrichment_failed);
                try std.testing.expectEqual(@as(u64, 0), index_stats.coverage_terminal_failed_count);
            }
            observed_retry = true;
        }
        types.freeDBStats(alloc, stats);
        if (observed_retry) break;
        sleepNs(10 * std.time.ns_per_ms);
    }
    try std.testing.expect(observed_retry);

    gated.allowAll();
    try db.runUntilIdle();

    const final_stats = try db.stats(alloc);
    defer types.freeDBStats(alloc, final_stats);
    try std.testing.expectEqual(@as(u64, 0), final_stats.enrichment.fatal_error_count);
    try std.testing.expect(!final_stats.enrichment.worker_failed);
    for (final_stats.indexes) |index_stats| {
        if (!std.mem.eql(u8, index_stats.name, "sp_v1")) continue;
        try std.testing.expectEqual(@as(u64, 1), index_stats.doc_count);
        try std.testing.expectEqual(@as(u64, 1), index_stats.coverage_produced_count);
        try std.testing.expectEqual(@as(u64, 0), index_stats.coverage_terminal_failed_count);
        try std.testing.expect(!index_stats.enrichment_failed);
    }

    var query = try gated.deterministic.interface().embedSparse(alloc, "sp_v1", "alpha concept overview");
    defer query.deinit(alloc);
    var result = try db.search(alloc, .{
        .index_name = "sp_v1",
        .query = .{ .sparse_knn = .{
            .indices = query.indices,
            .values = query.values,
            .k = 1,
        } },
        .limit = 1,
    });
    defer result.deinit();
    try std.testing.expectEqual(@as(u32, 1), result.total_hits);
    try std.testing.expectEqualStrings("doc:a", result.hits[0].id);
}

test "db chunked dense enrichment retries temporary model capacity without terminal coverage" {
    const alloc = std.testing.allocator;

    var gated = GateDenseEmbedder{
        .allowed_successes = .init(0),
        .blocked_error = error.ResourceTemporarilyUnavailable,
    };
    const t = try TestDb.open(alloc, .{
        .enrichment = .{
            .owner_id = "worker-a",
            .dense_embedder = gated.interface(),
            .inline_retry_max_attempts = 1,
        },
    });
    defer t.deinit();
    const db = &t.db;

    try db.addIndex(test_index.dv_v1_chunked_named);
    try db.batch(.{
        .writes = &.{.{ .key = "doc:a", .value = "{\"body\":\"abcdefghijklmno\"}" }},
        .sync_level = .write,
    });

    var observed_retry = false;
    var attempts: usize = 0;
    while (attempts < default_test_wait_attempts) : (attempts += 1) {
        const stats = try db.stats(alloc);
        if (gated.snapshot().blocked_requests > 0 and stats.enrichment.retryable_error_count > 0) {
            try std.testing.expectEqual(@as(u64, 0), stats.enrichment.fatal_error_count);
            for (stats.indexes) |index_stats| {
                if (!std.mem.eql(u8, index_stats.name, "dv_v1")) continue;
                try std.testing.expect(!index_stats.enrichment_failed);
                try std.testing.expectEqual(@as(u64, 0), index_stats.coverage_terminal_failed_count);
            }
            observed_retry = true;
        }
        types.freeDBStats(alloc, stats);
        if (observed_retry) break;
        sleepNs(10 * std.time.ns_per_ms);
    }
    try std.testing.expect(observed_retry);

    gated.allowAll();
    try db.runUntilIdle();

    const final_stats = try db.stats(alloc);
    defer types.freeDBStats(alloc, final_stats);
    try std.testing.expectEqual(@as(u64, 0), final_stats.enrichment.fatal_error_count);
    for (final_stats.indexes) |index_stats| {
        if (!std.mem.eql(u8, index_stats.name, "dv_v1")) continue;
        try std.testing.expectEqual(@as(u64, 0), index_stats.coverage_terminal_failed_count);
        try std.testing.expect(!index_stats.enrichment_failed);
    }
    try std.testing.expectEqual(@as(u64, 3), db.core.index_manager.denseIndex("dv_v1").?.index.metadata.active_count);

    const query_vec = try gated.interface().embedDense(alloc, "chunk_dense_v1", "abcdefgh", 3);
    defer alloc.free(query_vec);
    var result = try db.search(alloc, .{
        .index_name = "dv_v1",
        .dense = .{ .vector = query_vec, .k = 3 },
        .return_mode = .parent,
    });
    defer result.deinit();
    try std.testing.expectEqual(@as(u32, 1), result.total_hits);
    try std.testing.expectEqualStrings("doc:a", result.hits[0].id);
}

test "db managed dense delete quiesces rate-limited enrichment and recreates cleanly" {
    const alloc = std.testing.allocator;

    var gated = GateDenseEmbedder{};
    const t = try TestDb.open(alloc, .{
        .enrichment = .{
            .owner_id = "worker-a",
            .dense_embedder = gated.interface(),
        },
    });
    defer t.deinit();
    const db = &t.db;

    const cfg: types.IndexConfig = .{
        .name = "semantic_idx",
        .kind = .dense_vector,
        .config_json = "{\"field\":\"embedding\",\"dims\":3,\"metric\":\"cosine\",\"generator\":{\"kind\":\"dense_embedding\",\"source_field\":\"body\",\"embedding_name\":\"semantic_idx\"}}",
    };
    try db.addIndex(cfg);
    try db.batch(.{
        .writes = &.{
            .{ .key = "doc:a", .value = "{\"body\":\"alpha concept overview\"}" },
            .{ .key = "doc:b", .value = "{\"body\":\"beta architecture notes\"}" },
            .{ .key = "doc:c", .value = "{\"body\":\"gamma implementation details\"}" },
        },
        .sync_level = .write,
    });

    var attempts: usize = 0;
    while (attempts < 200 and gated.snapshot().blocked_requests == 0) : (attempts += 1) {
        sleepNs(10 * std.time.ns_per_ms);
    }
    try std.testing.expect(gated.snapshot().blocked_requests > 0);

    // One coalesced issue may accumulate more terminal-failure history than a
    // single retirement page during a prolonged provider outage. Admission
    // must remain fenced until all of it is gone, rather than shifting cleanup
    // and fail-closed retries onto the replacement generation's first writes.
    const retired_issue: types.ArtifactRepairIssue = .{
        .artifact_kind = .embedding,
        .index_name = cfg.name,
        .doc_key = "doc:retired",
        .artifact_name = cfg.name,
        .artifact_key = "retired-generation-artifact",
        .reason = .enrichment_failed,
    };
    for (0..enrichment_terminal_failure_retirement_page_size + 7) |i| {
        var sequenced = retired_issue;
        sequenced.sequence = 10_000 + @as(u64, @intCast(i));
        try db.recordArtifactRepairIssue(alloc, sequenced);
    }
    const retired_issue_key = try artifactRepairIssueKeyForIssueAlloc(alloc, retired_issue);
    defer alloc.free(retired_issue_key);
    const retired_marker_prefix = try internal_keys.enrichmentTerminalFailureIssuePrefixAlloc(alloc, retired_issue_key);
    defer alloc.free(retired_marker_prefix);
    {
        const marker_rows = try db.core.store.scanPrefixPage(alloc, retired_marker_prefix, null, 1);
        defer docstore_mod.DocStore.freeResults(alloc, marker_rows);
        try std.testing.expectEqual(@as(usize, 1), marker_rows.len);
    }
    const retired_generation_key = try internal_keys.enrichmentTerminalFailureGenerationKeyAlloc(alloc, retired_issue_key);
    defer alloc.free(retired_generation_key);
    const retired_completion_key = try db.artifactRepairCompletionKeyForIssueAlloc(alloc, retired_issue);
    defer alloc.free(retired_completion_key);

    const delete_started_ns = monotonicTimeNs();
    try std.testing.expect(try db.deleteIndex("semantic_idx"));
    try std.testing.expect(monotonicTimeNs() - delete_started_ns < 2 * std.time.ns_per_s);

    gated.allowAll();
    db.backend_runtime.durable_jobs.drainOwner(db.repair_cleanup_owner_id);
    {
        const issues = try db.listArtifactRepairIssues(alloc, .embedding, cfg.name, 0);
        defer types.freeArtifactRepairIssues(alloc, issues);
        try std.testing.expectEqual(@as(usize, 0), issues.len);
        const marker_rows = try db.core.store.scanPrefixPage(alloc, retired_marker_prefix, null, 1);
        defer docstore_mod.DocStore.freeResults(alloc, marker_rows);
        try std.testing.expectEqual(@as(usize, 0), marker_rows.len);
        try std.testing.expectError(error.NotFound, db.core.store.get(alloc, retired_generation_key));
        try std.testing.expectError(error.NotFound, db.core.store.get(alloc, retired_completion_key));
    }
    try db.addIndex(cfg);
    try db.runUntilIdle();

    const stats = try db.stats(alloc);
    defer types.freeDBStats(alloc, stats);
    try std.testing.expectEqual(@as(usize, 1), stats.indexes.len);
    try std.testing.expect(!stats.enrichment.worker_failed);
    try std.testing.expectEqual(@as(u64, 3), stats.indexes[0].doc_count);
    try std.testing.expect(stats.indexes[0].replay_applied_sequence >= stats.indexes[0].replay_target_sequence);
    try std.testing.expect(stats.indexes[0].repair_summary_ready);
    try std.testing.expect(!stats.indexes[0].repair_degraded);
    try std.testing.expectEqual(@as(u64, 0), stats.indexes[0].repair_issue_count);
}

test "db managed dense enrichment delete recreate recovers after corrupt artifact" {
    const alloc = std.testing.allocator;

    var counting = CountingDenseEmbedder{};
    const t = try TestDb.open(alloc, .{
        .enrichment = .{
            .owner_id = "worker-a",
            .dense_embedder = counting.interface(),
        },
    });
    defer t.deinit();
    const db = &t.db;

    const cfg: types.IndexConfig = .{
        .name = "semantic_idx",
        .kind = .dense_vector,
        .config_json = "{\"field\":\"embedding\",\"dims\":3,\"metric\":\"cosine\",\"generator\":{\"kind\":\"dense_embedding\",\"source_field\":\"body\",\"embedding_name\":\"semantic_idx\"}}",
    };

    try db.addIndex(cfg);
    try db.batch(.{
        .writes = &.{
            .{ .key = "doc:a", .value = "{\"title\":\"alpha\",\"body\":\"alpha concept overview\"}" },
            .{ .key = "doc:b", .value = "{\"title\":\"beta\",\"body\":\"beta architecture notes\"}" },
        },
        .sync_level = .write,
    });
    try db.runUntilIdle();

    {
        const stats = try db.stats(alloc);
        defer types.freeDBStats(alloc, stats);
        try std.testing.expectEqual(@as(?u64, 2), stats.indexes[0].doc_count);
    }

    const artifact_key = try expectedDocumentEmbeddingArtifactKeyAlloc(alloc, "doc:a", "semantic_idx");
    defer alloc.free(artifact_key);
    try db.core.store.put(artifact_key, "bad-artifact");

    try std.testing.expect(try db.deleteIndex("semantic_idx"));
    db.backend_runtime.durable_jobs.drainOwner(db.repair_cleanup_owner_id);
    try db.addIndex(cfg);
    try db.runUntilIdle();

    {
        const stats = try db.stats(alloc);
        defer types.freeDBStats(alloc, stats);
        try std.testing.expectEqual(@as(?u64, 2), stats.indexes[0].doc_count);
        try std.testing.expect(stats.indexes[0].replay_applied_sequence >= 2);
        try std.testing.expectEqual(stats.indexes[0].replay_target_sequence, stats.indexes[0].replay_applied_sequence);
    }

    const query_vec = try counting.interface().embedDense(alloc, "semantic_idx", "alpha concept overview", 3);
    defer alloc.free(query_vec);
    var result = try db.search(alloc, .{
        .index_name = "semantic_idx",
        .dense = .{
            .vector = query_vec,
            .k = 2,
        },
        .limit = 2,
    });
    defer result.deinit();
    try std.testing.expectEqual(@as(u32, 2), result.total_hits);
    try std.testing.expectEqualStrings("doc:a", result.hits[0].id);
}

test "db managed dense enrichment delete recreate recovers after corrupt artifact across reopen" {
    const alloc = std.testing.allocator;

    var path_tmp = try TestDirectory.init("db");
    defer path_tmp.cleanup();
    const path = path_tmp.path().ptr;

    var counting = CountingDenseEmbedder{};
    const cfg: types.IndexConfig = .{
        .name = "semantic_idx",
        .kind = .dense_vector,
        .config_json = "{\"field\":\"embedding\",\"dims\":3,\"metric\":\"cosine\",\"generator\":{\"kind\":\"dense_embedding\",\"source_field\":\"body\",\"embedding_name\":\"semantic_idx\"}}",
    };

    {
        var db = try DB.open(alloc, std.mem.span(path), .{
            .enrichment = .{
                .owner_id = "worker-a",
                .dense_embedder = counting.interface(),
            },
        });
        defer db.close();

        try db.addIndex(cfg);
        try db.batch(.{
            .writes = &.{
                .{ .key = "doc:a", .value = "{\"title\":\"alpha\",\"body\":\"alpha concept overview\"}" },
                .{ .key = "doc:b", .value = "{\"title\":\"beta\",\"body\":\"beta architecture notes\"}" },
            },
            .sync_level = .write,
        });
        try db.runUntilIdle();

        const artifact_key = try expectedDocumentEmbeddingArtifactKeyAlloc(alloc, "doc:a", "semantic_idx");
        defer alloc.free(artifact_key);
        try db.core.store.put(artifact_key, "bad-artifact");
    }

    var reopened = try DB.open(alloc, std.mem.span(path), .{
        .enrichment = .{
            .owner_id = "worker-a",
            .dense_embedder = counting.interface(),
        },
    });
    defer reopened.close();

    try std.testing.expect(try reopened.deleteIndex("semantic_idx"));
    reopened.backend_runtime.durable_jobs.drainOwner(reopened.repair_cleanup_owner_id);
    try reopened.addIndex(cfg);
    try reopened.runUntilIdle();

    {
        const stats = try reopened.stats(alloc);
        defer types.freeDBStats(alloc, stats);
        try std.testing.expectEqual(@as(?u64, 2), stats.indexes[0].doc_count);
        try std.testing.expect(stats.indexes[0].replay_applied_sequence >= 2);
        try std.testing.expectEqual(stats.indexes[0].replay_target_sequence, stats.indexes[0].replay_applied_sequence);
    }

    const query_vec = try counting.interface().embedDense(alloc, "semantic_idx", "alpha concept overview", 3);
    defer alloc.free(query_vec);
    var result = try reopened.search(alloc, .{
        .index_name = "semantic_idx",
        .dense = .{
            .vector = query_vec,
            .k = 2,
        },
        .limit = 2,
    });
    defer result.deinit();
    try std.testing.expectEqual(@as(u32, 2), result.total_hits);
    try std.testing.expectEqualStrings("doc:a", result.hits[0].id);
}

test "db chunked dense enrichment skips unchanged chunks and deletes stale chunk artifacts" {
    try testDenseChunkArtifactLifecycle(.{}, false);
}

test "db chunked dense enrichment replays cached artifacts after dense reset without re-embedding" {
    const alloc = std.testing.allocator;

    var counting = CountingDenseEmbedder{};
    const t = try TestDb.open(alloc, .{
        .enrichment = .{
            .owner_id = "worker-a",
            .dense_embedder = counting.interface(),
        },
    });
    defer t.deinit();
    const db = &t.db;

    try db.addIndex(test_index.dv_v1_chunked_named);

    try db.batch(.{
        .writes = &.{.{ .key = "doc:a", .value = "{\"title\":\"alpha\",\"body\":\"abcdefghijklmno\"}" }},
        .sync_level = .write,
    });
    try db.runUntilIdle();

    const first_calls = counting.calls;
    try std.testing.expect(first_calls > 0);
    try std.testing.expectEqual(@as(u64, 3), db.core.index_manager.denseIndex("dv_v1").?.index.metadata.active_count);

    try resetDenseIndexForArtifactRebuildForTest(db, "dv_v1");
    try std.testing.expectEqual(@as(u64, 0), db.core.index_manager.denseIndex("dv_v1").?.index.metadata.active_count);

    try db.batch(.{
        .writes = &.{.{ .key = "doc:a", .value = "{\"title\":\"alpha\",\"body\":\"abcdefghijklmno\"}" }},
        .sync_level = .write,
    });
    try db.runUntilIdle();

    try std.testing.expectEqual(first_calls, counting.calls);
    try std.testing.expectEqual(@as(u64, 3), db.core.index_manager.denseIndex("dv_v1").?.index.metadata.active_count);

    const query_vec = try counting.interface().embedDense(alloc, "chunk_dense_v1", "abcdefgh", 3);
    defer alloc.free(query_vec);

    var result = try db.search(alloc, .{
        .index_name = "dv_v1",
        .dense = .{
            .vector = query_vec,
            .k = 3,
        },
        .return_mode = .parent,
    });
    defer result.deinit();
    try std.testing.expectEqual(@as(u32, 1), result.total_hits);
    try std.testing.expectEqualStrings("doc:a", result.hits[0].id);
}

test "db chunked dense retry preserves published vectors until replacements succeed" {
    const alloc = std.testing.allocator;

    var path_tmp = try TestDirectory.init("db");
    defer path_tmp.cleanup();
    const path = path_tmp.path().ptr;

    var gated = GateDenseEmbedder{ .allowed_successes = .init(3) };
    {
        var db = try DB.open(alloc, std.mem.span(path), .{
            .enrichment = .{
                .owner_id = "worker-a",
                .dense_embedder = gated.interface(),
            },
        });
        defer db.close();

        try db.addIndex(test_index.dv_v1_chunked_named);
        try db.batch(.{
            .writes = &.{.{ .key = "doc:a", .value = "{\"body\":\"abcdefghijklmno\"}" }},
            .sync_level = .write,
        });
        try db.runUntilIdle();
        try std.testing.expectEqual(@as(u64, 3), db.core.index_manager.denseIndex("dv_v1").?.index.metadata.active_count);

        try db.batch(.{
            .writes = &.{.{ .key = "doc:a", .value = "{\"body\":\"qrstuvwx\"}" }},
            .sync_level = .write,
        });
        var attempts: usize = 0;
        while (attempts < 200 and gated.snapshot().blocked_requests == 0) : (attempts += 1)
            sleepNs(10 * std.time.ns_per_ms);
        try std.testing.expect(gated.snapshot().blocked_requests > 0);
        try std.testing.expectEqual(@as(u64, 3), db.core.index_manager.denseIndex("dv_v1").?.index.metadata.active_count);
    }

    {
        var reopened = try DB.open(alloc, std.mem.span(path), .{
            .enrichment = .{
                .owner_id = "worker-a",
                .dense_embedder = gated.interface(),
            },
        });
        defer reopened.close();
        try std.testing.expectEqual(@as(u64, 3), reopened.core.index_manager.denseIndex("dv_v1").?.index.metadata.active_count);
        gated.allowAll();
        try reopened.runUntilIdle();
        try std.testing.expectEqual(@as(u64, 1), reopened.core.index_manager.denseIndex("dv_v1").?.index.metadata.active_count);
    }
}

test "db chunked generated dense and sparse embeddings search as parent results" {
    const alloc = std.testing.allocator;

    var deterministic_dense = embedder_mod.DeterministicDenseEmbedder{};
    var deterministic_sparse = embedder_mod.DeterministicSparseEmbedder{};
    const t = try TestDb.open(alloc, .{
        .enrichment = .{
            .owner_id = "worker-a",
            .dense_embedder = deterministic_dense.interface(),
            .sparse_embedder = deterministic_sparse.interface(),
        },
    });
    defer t.deinit();
    const db = &t.db;

    try db.addIndex(test_index.dv_v1_dims3_generated);
    try db.addIndex(.{
        .name = "sp_v1",
        .kind = .sparse_vector,
        .config_json = "{\"field\":\"sparse_embedding\",\"generator\":{\"kind\":\"sparse_embedding\",\"source_field\":\"body\",\"artifact_name\":\"sparse_body_chunks_v1\",\"chunk_size\":8,\"chunk_overlap\":2}}",
    });

    try db.batch(.{
        .writes = &.{.{ .key = "doc:a", .value = "{\"title\":\"alpha\",\"body\":\"abcdefghijklmno\"}" }},
        .sync_level = .write,
    });
    try db.runUntilIdle();

    const dense_vec = try deterministic_dense.interface().embedDense(alloc, "chunk_dense_v1", "abcdefgh", 3);
    defer alloc.free(dense_vec);
    var dense_result = try db.search(alloc, .{
        .index_name = "dv_v1",
        .dense = .{ .vector = dense_vec, .k = 3 },
        .limit = 1,
        .include_stored = true,
    });
    defer dense_result.deinit();
    try std.testing.expectEqual(@as(u32, 1), dense_result.total_hits);
    try std.testing.expectEqualStrings("doc:a", dense_result.hits[0].id);
    try std.testing.expect(dense_result.hits[0].stored_data != null);

    var sparse_query = try deterministic_sparse.interface().embedSparse(alloc, "sp_v1", "abcdefgh");
    defer sparse_query.deinit(alloc);
    var sparse_result = try db.search(alloc, .{
        .index_name = "sp_v1",
        .query = .{ .sparse_knn = .{
            .indices = sparse_query.indices,
            .values = sparse_query.values,
            .k = 3,
        } },
        .limit = 1,
        .include_stored = true,
    });
    defer sparse_result.deinit();
    try std.testing.expectEqual(@as(u32, 1), sparse_result.total_hits);
    try std.testing.expectEqualStrings("doc:a", sparse_result.hits[0].id);
    try std.testing.expect(sparse_result.hits[0].stored_data != null);
    {
        var txn = try db.core.store.beginProbeTxn();
        defer txn.abort();
        const ordinal = (try doc_identity.lookupOrdinalTxn(alloc, &txn, "doc:a")) orelse return error.TestUnexpectedResult;
        try std.testing.expectEqual(@as(?doc_identity.DocOrdinal, ordinal), sparse_result.hits[0].doc_ordinal);
    }

    var sparse_grouped = try db.search(alloc, .{
        .index_name = "sp_v1",
        .query = .{ .sparse_knn = .{
            .indices = sparse_query.indices,
            .values = sparse_query.values,
            .k = 3,
        } },
        .return_mode = .parent_with_chunks,
        .max_chunks_per_parent = 1,
        .hierarchy_grouped_matches = true,
        .hierarchy_match_fields = &.{},
        .hierarchy_match_include_all_fields = false,
        .limit = 1,
        .include_stored = false,
    });
    defer sparse_grouped.deinit();
    try std.testing.expectEqual(@as(usize, 1), sparse_grouped.hits.len);
    try std.testing.expectEqualStrings("doc:a", sparse_grouped.hits[0].id);
    try std.testing.expectEqual(@as(usize, 1), sparse_grouped.hits[0].chunk_hits.len);

    // Singleton selection must not borrow the other vector family's
    // embedding identity. These indexes deliberately own different chunks.
    for ([_]types.ReturnMode{ .parent, .chunk }) |mode| {
        var unnamed_sparse = try db.search(alloc, .{
            .query = .{ .sparse_knn = .{
                .indices = sparse_query.indices,
                .values = sparse_query.values,
                .k = 3,
            } },
            .return_mode = mode,
            .limit = 1,
            .include_stored = false,
        });
        defer unnamed_sparse.deinit();
        try std.testing.expectEqual(@as(usize, 1), unnamed_sparse.hits.len);
        if (mode == .parent) {
            try std.testing.expectEqualStrings("doc:a", unnamed_sparse.hits[0].id);
        } else {
            try std.testing.expectEqualStrings("sparse_body_chunks_v1", unnamed_sparse.hits[0].artifact_ref.?.name);
        }
    }

    const doc_a_store_key = try internal_keys.documentKeyAlloc(alloc, "doc:a");
    defer alloc.free(doc_a_store_key);
    {
        var txn = try db.core.store.beginWriteTxn();
        errdefer txn.abort();
        try txn.delete(doc_a_store_key);
        try doc_identity.markDeletedTxn(alloc, &txn, 2, "doc:a");
        try txn.commit();
    }
    db.core.identity_visibility.summary = null;
    db.clearLiveDocSetCache();
    db.clearNonVisibleDocSetCache();

    var stale_sparse_result = try db.search(alloc, .{
        .index_name = "sp_v1",
        .query = .{ .sparse_knn = .{
            .indices = sparse_query.indices,
            .values = sparse_query.values,
            .k = 3,
        } },
        .return_mode = .chunk,
        .limit = 3,
        .include_stored = false,
    });
    defer stale_sparse_result.deinit();
    try std.testing.expectEqual(@as(u32, 0), stale_sparse_result.total_hits);
    try std.testing.expectEqual(@as(usize, 0), stale_sparse_result.hits.len);
}

test "db chunked sparse enrichment skips unchanged chunks and deletes stale sparse artifacts" {
    const alloc = std.testing.allocator;

    var counting = CountingSparseEmbedder{};
    const t = try TestDb.open(alloc, .{
        .start_index_workers = false,
        .enrichment = .{
            .owner_id = "worker-a",
            .sparse_embedder = counting.interface(),
        },
    });
    defer t.deinit();
    const db = &t.db;

    try db.addIndex(.{
        .name = "sp_v1",
        .kind = .sparse_vector,
        .config_json = "{\"field\":\"sparse_embedding\",\"generator\":{\"kind\":\"sparse_embedding\",\"source_field\":\"body\",\"artifact_name\":\"body_chunks_v1\",\"chunk_size\":8,\"chunk_overlap\":2}}",
    });

    try db.batch(.{
        .writes = &.{.{ .key = "doc:a", .value = "{\"title\":\"alpha\",\"body\":\"abcdefghijklmno\"}" }},
        .sync_level = .write,
    });
    try db.runUntilIdle();
    const first_calls = counting.calls;
    try std.testing.expect(first_calls > 0);

    try db.batch(.{
        .writes = &.{.{ .key = "doc:a", .value = "{\"title\":\"changed\",\"body\":\"abcdefghijklmno\"}" }},
        .sync_level = .write,
    });
    try db.runUntilIdle();
    try std.testing.expectEqual(first_calls, counting.calls);

    try db.batch(.{
        .writes = &.{.{ .key = "doc:a", .value = "{\"title\":\"changed\",\"body\":\"abcdefgh\"}" }},
        .sync_level = .write,
    });
    try db.runUntilIdle();
    try std.testing.expectEqual(first_calls, counting.calls);

    const chunk_prefix = try internal_keys.artifactNamedPrefixAlloc(alloc, "doc:a", "chunk", "body_chunks_v1");
    defer alloc.free(chunk_prefix);
    const artifacts = try db.core.store.scanPrefix(alloc, chunk_prefix);
    defer docstore_mod.DocStore.freeResults(alloc, artifacts);

    var chunk_count: usize = 0;
    var sparse_artifact_count: usize = 0;
    for (artifacts) |entry| {
        if (internal_keys.isChunkArtifactRecordKey(entry.key)) chunk_count += 1;
        if (internal_keys.isDerivedEmbeddingArtifactKey(entry.key)) {
            sparse_artifact_count += 1;
            try expectSparseEmbeddingArtifactValue(alloc, entry.value, enrichment_artifact_codec.hashSource("abcdefgh"), 2);
        }
    }
    try std.testing.expectEqual(@as(usize, 1), chunk_count);
    try std.testing.expectEqual(@as(usize, 1), sparse_artifact_count);

    var stale_query = try counting.deterministic.interface().embedSparse(alloc, "sp_v1", "ghijklmn");
    defer stale_query.deinit(alloc);
    var stale_result = try db.search(alloc, .{
        .index_name = "sp_v1",
        .query = .{ .sparse_knn = .{
            .indices = stale_query.indices,
            .values = stale_query.values,
            .k = 10,
        } },
        .return_mode = .chunk,
        .limit = 10,
        .include_stored = false,
    });
    defer stale_result.deinit();

    const stale_chunk_id = try expectedChunkArtifactPublicIdAlloc(alloc, "doc:a", "body_chunks_v1", 1);
    defer alloc.free(stale_chunk_id);
    for (stale_result.hits) |hit| {
        try std.testing.expect(!std.mem.eql(u8, hit.id, stale_chunk_id));
    }
}

test "db generated enrichment empty backfill advances enrichment checkpoint" {
    const alloc = std.testing.allocator;

    var deterministic = embedder_mod.DeterministicDenseEmbedder{};
    const t = try TestDb.open(alloc, .{
        .enrichment = .{
            .owner_id = "embedded-worker",
            .dense_embedder = deterministic.interface(),
        },
    });
    defer t.deinit();
    const db = &t.db;

    try db.addIndex(test_index.dv_v1);

    const pending = db.pendingWorkStats();
    try std.testing.expectEqual(pending.enrichment.target_sequence, pending.enrichment.applied_sequence);
    try std.testing.expectEqual(pending.derived_target_sequence, try db.core.loadAppliedSequence(alloc, "dv_v1"));
}

test "db generated enrichment backfill drains stored docs beyond first replay chunk" {
    const alloc = std.testing.allocator;

    var deterministic = embedder_mod.DeterministicDenseEmbedder{};
    const t = try TestDb.open(alloc, .{
        .enrichment = .{
            .owner_id = "embedded-worker",
            .dense_embedder = deterministic.interface(),
        },
    });
    defer t.deinit();
    const db = &t.db;

    const total_docs: usize = 129;
    const writes = try alloc.alloc(types.BatchWrite, total_docs);
    defer {
        for (writes) |write| {
            alloc.free(@constCast(write.key));
            alloc.free(@constCast(write.value));
        }
        alloc.free(writes);
    }
    for (writes, 0..) |*write, i| {
        write.* = .{
            .key = try std.fmt.allocPrint(alloc, "doc:{d:0>3}", .{i}),
            .value = try std.fmt.allocPrint(alloc, "{{\"title\":\"doc {d}\",\"body\":\"generated vector text {d}\"}}", .{ i, i }),
        };
    }
    try db.batch(.{
        .writes = writes,
        .sync_level = .write,
    });

    try db.addIndex(test_index.dv_v1);
    try db.runUntilIdle();

    const pending_after = db.pendingWorkStats();
    try std.testing.expectEqual(pending_after.enrichment.target_sequence, pending_after.enrichment.applied_sequence);

    const last_artifact_key = try expectedDocumentEmbeddingArtifactKeyAlloc(alloc, "doc:128", "body_dense_v1");
    defer alloc.free(last_artifact_key);
    const last_artifact = try db.core.store.get(alloc, last_artifact_key);
    defer alloc.free(last_artifact);
    try expectDenseEmbeddingArtifactWithSourceHash(alloc, last_artifact, 3);

    const dense_entry = db.core.denseIndex("dv_v1") orelse return error.IndexNotFound;
    try std.testing.expectEqual(@as(u64, @intCast(total_docs)), dense_entry.index.stats().active_count);
}

test "db generated enrichment replay cannot infer terminal skip from an omitted partial artifact" {
    const alloc = std.testing.allocator;

    const t = try TestDb.open(alloc, .{ .start_index_workers = false });
    defer t.deinit();
    const db = &t.db;
    try db.addIndex(test_index.semantic);
    try db.addIndex(.{
        .name = "semantic_sparse",
        .kind = .sparse_vector,
        .config_json =
        \\{"field":"sparse_embedding","generator":{"kind":"sparse_embedding","source_field":"body","embedding_name":"semantic_sparse"}}
        ,
    });

    // Progressive generated batches may publish source documents separately
    // from their embeddings and without carrying the original request refs.
    // Only the enrichment producer can classify that absence as terminal.
    const documents = [_]derived_types.DerivedDocument{.{
        .key = "doc:pending",
        .cleaned_value = "{\"body\":\"still embedding\"}",
    }};
    try accountDenseCoverage(db.async_context, "semantic", .{ .documents = &documents }, &.{});
    try accountSparseCoverage(db.async_context, "semantic_sparse", .{ .documents = &documents }, &.{});

    const stats = try db.stats(alloc);
    defer types.freeDBStats(alloc, stats);
    var verified: usize = 0;
    for (stats.indexes) |index_stats| {
        if (!std.mem.eql(u8, index_stats.name, "semantic") and
            !std.mem.eql(u8, index_stats.name, "semantic_sparse")) continue;
        try std.testing.expectEqual(@as(u64, 0), index_stats.coverage_produced_count);
        try std.testing.expectEqual(@as(u64, 0), index_stats.coverage_skipped_count);
        try std.testing.expectEqual(@as(u64, 0), index_stats.coverage_terminal_failed_count);
        verified += 1;
    }
    try std.testing.expectEqual(@as(usize, 2), verified);
}

test "db managed conditional embeddings persist exact mixed corpus coverage across reopen" {
    const alloc = std.testing.allocator;

    var path_tmp = try TestDirectory.init("db");
    defer path_tmp.cleanup();
    const path = path_tmp.path().ptr;
    const config_json =
        "{\"field\":\"embedding\",\"dims\":3,\"generator\":{\"kind\":\"dense_embedding\",\"source_field\":\"image_url\",\"source_template\":\"{{#if image_url}}{{image_url}}{{/if}}\"}}";
    const expected_config_hash = try internal_keys.derivedCoverageConfigFingerprint(alloc, config_json);

    const ExpectCoverage = struct {
        fn run(db_value: *DB, config_hash: u64, produced: u64, skipped: u64) !void {
            const stats = try db_value.stats(std.testing.allocator);
            defer types.freeDBStats(std.testing.allocator, stats);
            try std.testing.expectEqual(@as(u64, 2), stats.source_doc_count);
            for (stats.indexes) |index_stats| {
                if (!std.mem.eql(u8, index_stats.name, "visual")) continue;
                try std.testing.expect(index_stats.coverage_summary_ready);
                try std.testing.expectEqual(config_hash, index_stats.coverage_config_hash);
                try std.testing.expectEqual(produced, index_stats.coverage_produced_count);
                try std.testing.expectEqual(skipped, index_stats.coverage_skipped_count);
                try std.testing.expectEqual(@as(u64, 0), index_stats.coverage_terminal_failed_count);
                try std.testing.expectEqual(produced, index_stats.doc_count);
                return;
            }
            return error.IndexNotFound;
        }
    };

    {
        var deterministic = embedder_mod.DeterministicDenseEmbedder{};
        var db = try DB.open(alloc, std.mem.span(path), .{
            .enrichment = .{
                .owner_id = "worker-a",
                .dense_embedder = deterministic.interface(),
            },
        });
        defer db.close();
        try db.addIndex(.{ .name = "visual", .kind = .dense_vector, .config_json = config_json });
        try db.batch(.{
            .writes = &.{
                .{ .key = "doc:image", .value = "{\"image_url\":\"https://example.invalid/image.png\"}" },
                .{ .key = "doc:text", .value = "{\"body\":\"not embeddable by this index\"}" },
            },
            .sync_level = .full_index,
        });
        try ExpectCoverage.run(&db, expected_config_hash, 1, 1);

        try db.batch(.{
            .writes = &.{.{ .key = "doc:text", .value = "{\"image_url\":\"https://example.invalid/second.png\"}" }},
            .sync_level = .full_index,
        });
        try ExpectCoverage.run(&db, expected_config_hash, 2, 0);

        try db.batch(.{
            .writes = &.{.{ .key = "doc:text", .value = "{\"body\":\"not embeddable again\"}" }},
            .sync_level = .full_index,
        });
        try ExpectCoverage.run(&db, expected_config_hash, 1, 1);

        // The asynchronous producer must retire the same artifact/member,
        // without a later write or synchronous enrichment doing the cleanup.
        try db.batch(.{
            .writes = &.{.{ .key = "doc:text", .value = "{\"image_url\":\"https://example.invalid/async.png\"}" }},
            .sync_level = .write,
        });
        try db.runUntilIdle();
        try ExpectCoverage.run(&db, expected_config_hash, 2, 0);
        try db.batch(.{
            .writes = &.{.{ .key = "doc:text", .value = "{\"body\":\"asynchronous skip\"}" }},
            .sync_level = .write,
        });
        try db.runUntilIdle();
        try ExpectCoverage.run(&db, expected_config_hash, 1, 1);
    }

    {
        var deterministic = embedder_mod.DeterministicDenseEmbedder{};
        var reopened = try DB.open(alloc, std.mem.span(path), .{
            .enrichment = .{
                .owner_id = "worker-b",
                .dense_embedder = deterministic.interface(),
            },
        });
        defer reopened.close();
        try ExpectCoverage.run(&reopened, expected_config_hash, 1, 1);
    }
}

test "db enrichment retry makes monotonic progress across provider batches" {
    const alloc = std.testing.allocator;

    var gated = GateDenseEmbedder{};
    gated.allowed_successes.store(0, .release);
    const t = try TestDb.open(alloc, .{
        .start_optional_runtime_workers = false,
        .enrichment = .{
            .owner_id = "worker-a",
            .dense_embedder = gated.interface(),
            .inline_retry_max_attempts = 1,
            .worker_retry_max_attempts = 2,
        },
    });
    defer t.deinit();
    const db = &t.db;

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
    sleepNs(550 * std.time.ns_per_ms);
    try std.testing.expectError(error.EnrichmentRetryInProgress, db.runEnrichmentUntil(sequence));
    sleepNs(550 * std.time.ns_per_ms);
    try db.runEnrichmentUntil(sequence);

    try std.testing.expectEqual(sequence, db.enrichment_runtime.?.stats().applied_sequence);
    const issues = try db.listArtifactRepairIssues(alloc, .embedding, "semantic", 0);
    defer types.freeArtifactRepairIssues(alloc, issues);
    try std.testing.expectEqual(@as(usize, 2), issues.len);
    for (issues) |issue| {
        try std.testing.expectEqual(@as(u64, 2), issue.generation_attempts);
        try std.testing.expectEqualStrings("EmbedRateLimited", issue.generation_error);
    }
}

test "db chunked enrichment failure repair completes from request coverage" {
    const alloc = std.testing.allocator;

    var gated = GateDenseEmbedder{};
    gated.allowed_successes.store(0, .release);
    const t = try TestDb.open(alloc, .{
        .start_optional_runtime_workers = false,
        .enrichment = .{
            .owner_id = "worker-a",
            .dense_embedder = gated.interface(),
            .inline_retry_max_attempts = 1,
            .worker_retry_max_attempts = 2,
        },
    });
    defer t.deinit();
    const db = &t.db;

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
    try std.testing.expectError(error.EnrichmentRetryInProgress, db.runEnrichmentUntil(sequence));
    sleepNs(550 * std.time.ns_per_ms);
    try db.runEnrichmentUntil(sequence);

    const parked = try db.listArtifactRepairIssues(alloc, .embedding, "chunked_dense", 1);
    defer types.freeArtifactRepairIssues(alloc, parked);
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
    defer types.freeArtifactRepairIssues(alloc, remaining);
    try std.testing.expectEqual(@as(usize, 0), remaining.len);
}

test "db enrichment repair never clears a newer failure revision" {
    const alloc = std.testing.allocator;
    const t = try TestDb.open(alloc, .{ .start_optional_runtime_workers = false });
    defer t.deinit();
    const db = &t.db;
    const artifact_key = try internal_keys.embeddingArtifactKeyForDocumentAlloc(alloc, "doc:race", "dense_v1");
    defer alloc.free(artifact_key);

    try recordArtifactRepairIssueContextDetailed(
        db.async_context,
        .embedding,
        "semantic",
        "doc:race",
        "",
        "",
        "dense_v1",
        "",
        "dense_v1",
        artifact_key,
        null,
        10,
        .enrichment_failed,
        3,
        "EmbedRateLimited",
    );
    const first_page = try db.listArtifactRepairIssues(alloc, .embedding, "semantic", 1);
    defer types.freeArtifactRepairIssues(alloc, first_page);
    try std.testing.expectEqual(@as(usize, 1), first_page.len);
    const stale_issue = first_page[0];
    const stale_revision = DB.ArtifactRepairIssueRevision.capture(stale_issue);
    const completion_key = try db.artifactRepairCompletionKeyForIssueAlloc(alloc, stale_issue);
    defer alloc.free(completion_key);
    const stale_completion = try db.artifactRepairCompletionSnapshot(alloc, completion_key);

    try recordArtifactRepairIssueContextDetailed(
        db.async_context,
        .embedding,
        "semantic",
        "doc:race",
        "",
        "",
        "dense_v1",
        "",
        "dense_v1",
        artifact_key,
        null,
        11,
        .enrichment_failed,
        1,
        "ProviderUnavailable",
    );
    try std.testing.expect(!try db.clearArtifactRepairIssueIfCurrent(alloc, stale_issue, stale_revision));
    try std.testing.expectEqual(
        DB.ArtifactRepairCompletionDisposition.stale,
        try db.completeArtifactRepairIssueIfCurrent(
            alloc,
            stale_issue,
            stale_revision,
            completion_key,
            stale_completion.epoch,
            false,
        ),
    );

    const current = try db.listArtifactRepairIssues(alloc, .embedding, "semantic", 1);
    defer types.freeArtifactRepairIssues(alloc, current);
    try std.testing.expectEqual(@as(usize, 1), current.len);
    try std.testing.expectEqual(@as(u64, 11), current[0].sequence);
    try std.testing.expectEqualStrings("ProviderUnavailable", current[0].generation_error);
}

test "db enrichment artifact consumer resolution includes graph and default full text indexes" {
    const alloc = std.testing.allocator;

    const t = try TestDb.open(alloc, .{});
    defer t.deinit();
    const db = &t.db;

    try db.addIndex(test_index.full_text_index_v0);
    try db.addEnrichment(.{
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

test "db managed projection checkpoints persist status and config identity" {
    const alloc = std.testing.allocator;

    var path_tmp = try TestDirectory.init("db");
    defer path_tmp.cleanup();
    const path = path_tmp.path().ptr;

    const configs = [_]types.IndexConfig{
        .{
            .name = "ft_v1",
            .kind = .full_text,
            .config_json = "{\"field\":\"title\"}",
        },
        .{
            .name = "dv_v1",
            .kind = .dense_vector,
            .config_json = "{\"field\":\"embedding\",\"dims\":2,\"metric\":\"l2_squared\"}",
        },
        .{
            .name = "sp_v1",
            .kind = .sparse_vector,
            .config_json = "{\"field\":\"sparse\"}",
        },
        .{
            .name = "gr_v1",
            .kind = .graph,
            .config_json = "{}",
        },
        .{
            .name = "alg_v1",
            .kind = .algebraic,
            .config_json =
            \\{
            \\  "version": 1,
            \\  "table": "docs",
            \\  "group_fields": [{"name":"category","path":"category","type":"string"}],
            \\  "measure_fields": [{"name":"score","path":"score","type":"number"}],
            \\  "materializations": [{"name":"count_by_category","op":"count","group_by":["category"]}]
            \\}
            ,
        },
    };

    var db = try DB.open(alloc, std.mem.span(path), .{});
    defer db.close();

    for (configs) |cfg| try db.addIndex(cfg);

    try db.batch(.{
        .writes = &.{
            .{
                .key = "doc:a",
                .value = "{\"title\":\"alpha\",\"category\":\"news\",\"score\":3,\"embedding\":[1,0],\"sparse\":{\"indices\":[1,3],\"values\":[0.5,0.75]}}",
            },
        },
        .graph_writes = &.{
            .{ .index_name = "gr_v1", .source = "doc:a", .target = "doc:b", .edge_type = "links", .weight = 1.0 },
        },
        .sync_level = .full_index,
    });

    for (configs) |cfg| {
        const checkpoint = try db.core.loadProjectionCheckpoint(alloc, cfg.name);
        try std.testing.expectEqual(apply_state.ProjectionStatus.clean, checkpoint.status);
        try std.testing.expectEqual(try db.core.loadAppliedSequence(alloc, cfg.name), checkpoint.applied_sequence);
        try std.testing.expect(checkpoint.applied_sequence > 0);
        try std.testing.expectEqual(types.indexConfigHash(cfg), checkpoint.config_hash);
    }
}

test "db generated replay atomically promotes staged artifacts and deletes stale generation" {
    const alloc = std.testing.allocator;
    var test_dir = try TestDirectory.init("db");
    defer test_dir.cleanup();

    var db = try DB.open(alloc, test_dir.path(), .{
        .start_index_workers = false,
        .ttl_cleanup = .{ .enabled = false },
        .enrichment = .{ .enable_without_producers = true },
    });
    defer db.close();
    const append_ctx = db.enrichment_append_context orelse return error.TestExpectedEnrichmentRuntime;

    const stage_key = "private:pdf-stage:1";
    const final_key = try internal_keys.embeddingArtifactKeyForDocumentAlloc(alloc, "doc:new", "visual_v1");
    defer alloc.free(final_key);
    const stale_key = try internal_keys.embeddingArtifactKeyForDocumentAlloc(alloc, "doc:stale", "visual_v1");
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

    _ = try appendGeneratedBatchFromEnrichment(
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
    defer shard_mod.freeDeltas(alloc, split_deltas);
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
        appendGeneratedBatchFromEnrichment(
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
    defer shard_mod.freeDeltas(alloc, deltas_after_failed_promotion);
    try std.testing.expectEqual(@as(usize, 1), deltas_after_failed_promotion.len);

    const fence_key = "\x00\x00__metadata__:generated_replay_fence_test";
    var lease = try lease_mod.Lease.init(alloc, db.core.store, fence_key);
    defer lease.deinit();
    const now_ms = platform_clock.Clock.real().nowRealtimeMs();
    const acquired = try lease.tryAcquireFenced("worker-a", now_ms, 60_000);
    try std.testing.expect(acquired.acquired);
    try db.core.store.putBatch(&.{.{ .key = stage_key, .value = "fenced-vector" }}, &.{});

    try std.testing.expectError(
        error.EnrichmentLeaseFenceLost,
        appendGeneratedBatchFromEnrichment(
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

    _ = try appendGeneratedBatchFromEnrichment(
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

    const source_key = try internal_keys.documentKeyAlloc(alloc, "doc:guarded");
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
    try std.testing.expectError(error.EnrichmentSourceChanged, appendGeneratedBatchFromEnrichment(
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
    try std.testing.expectError(error.EnrichmentSourceChanged, appendGeneratedBatchFromEnrichment(
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

test "db managed vector admission captures writes while durable repair is pending" {
    const alloc = std.testing.allocator;
    const cfg = types.IndexConfig{
        .name = "semantic_idx",
        .kind = .dense_vector,
        .config_json = "{\"field\":\"embedding\",\"dims\":3,\"metric\":\"l2_squared\"}",
    };
    const t = try TestDb.open(alloc, no_replay_no_background_test_open);
    defer t.deinit();
    const db = &t.db;
    try db.batch(.{
        .writes = &.{.{ .key = "doc:old", .value = "{\"body\":\"before admission\"}" }},
        .sync_level = .write,
    });

    _ = (try db.admitManagedIndex(cfg)) orelse return error.TestUnexpectedResult;
    try std.testing.expect(try db.hasPendingIndexRepairIntents(alloc));
    try std.testing.expectError(error.IndexRebuilding, db.search(alloc, .{
        .index_name = cfg.name,
        .query = .{ .dense_knn = .{ .vector = &.{ 1, 0, 0 }, .k = 1 } },
    }));

    // A post-admission write is allowed to commit while the corpus rebuild is
    // pending. With workers disabled, its target watermark is durable proof
    // that catch-up owns the write instead of silently dropping the vector.
    try db.batch(.{
        .writes = &.{.{
            .key = "doc:new",
            .value = "{\"body\":\"after admission\",\"embedding\":[1,0,0]}",
        }},
        .sync_level = .write,
    });
    const stats = try db.stats(alloc);
    defer types.freeDBStats(alloc, stats);
    const target = for (stats.indexes) |item| {
        if (std.mem.eql(u8, item.name, cfg.name)) break item;
    } else return error.TestUnexpectedResult;
    try std.testing.expectEqual(@as(u64, 0), target.doc_count);
    try std.testing.expect(target.replay_target_sequence > target.replay_applied_sequence);
}

test "db managed external dense admission ignores unrelated source rows" {
    const alloc = std.testing.allocator;
    const cfg = types.IndexConfig{
        .name = "semantic_idx",
        .kind = .dense_vector,
        .config_json = "{\"field\":\"embedding\",\"dims\":3,\"metric\":\"l2_squared\",\"external\":true}",
    };
    const t = try TestDb.open(alloc, no_replay_no_background_test_open);
    defer t.deinit();
    const db = &t.db;
    try db.batch(.{
        .writes = &.{.{ .key = "readiness", .value = "{\"ready\":true}" }},
        .sync_level = .write,
    });

    try std.testing.expect((try db.admitManagedIndex(cfg)) == null);
    try std.testing.expect(!try db.hasPendingIndexRepairIntents(alloc));
    try std.testing.expect(!db.core.index_manager.repairUnavailable(cfg.name));
    try std.testing.expectEqual(
        @as(?u64, 0),
        try DB.loadDenseArtifactTargetCounter(alloc, db.core.store, cfg.name),
    );
    const admission_key = try internal_keys.managedIndexAdmissionKeyAlloc(alloc, cfg.name);
    defer alloc.free(admission_key);
    try std.testing.expectError(error.NotFound, db.core.store.get(alloc, admission_key));

    // Subsequent caller-owned artifacts still enter through ordinary replay.
    try db.batch(.{
        .writes = &.{.{
            .key = "doc:new",
            .value = "{\"body\":\"after admission\",\"_embeddings\":{\"semantic_idx\":[1,0,0]}}",
        }},
        .sync_level = .full_index,
    });
    try std.testing.expectEqual(
        @as(?u64, 1),
        try DB.loadDenseArtifactTargetCounter(alloc, db.core.store, cfg.name),
    );
    var result = try db.search(alloc, .{
        .index_name = cfg.name,
        .query = .{ .dense_knn = .{ .vector = &.{ 1, 0, 0 }, .k = 1 } },
    });
    defer result.deinit();
    try std.testing.expectEqual(@as(u32, 1), result.total_hits);
    try std.testing.expectEqualStrings("doc:new", result.hits[0].id);
}

test "db managed full text admission replays transitive artifact producers" {
    const alloc = std.testing.allocator;
    var deterministic = embedder_mod.DeterministicDenseEmbedder{};
    const t = try TestDb.open(alloc, .{
        .start_index_workers = false,
        .start_optional_runtime_workers = false,
        .ttl_cleanup = .{ .enabled = false },
        .enrichment = .{ .dense_embedder = deterministic.interface() },
    });
    defer t.deinit();
    const db = &t.db;

    // Model the catalog-to-resident handoff: the source write commits through
    // the old schema immediately before the new generated index is admitted.
    try db.batch(.{
        .writes = &.{.{
            .key = "doc:a",
            .value = "{\"url\":\"data:text/plain;base64,YWxwaGEgYmV0YSBnYW1tYQ==\"}",
        }},
        .sync_level = .write,
    });
    try db.addEnrichment(test_enrichment.document_units_v1);
    try db.addEnrichment(test_enrichment.document_chunks_v1);

    const cfg = types.IndexConfig{
        .name = "document_text",
        .kind = .full_text,
        .config_json = "{\"field\":\"text\",\"sources\":[{\"artifact\":\"document_chunks_v1\"}]}",
    };
    const repair_id = (try db.admitManagedIndex(cfg)) orelse return error.TestUnexpectedResult;

    const direct_sources = try db.core.index_manager.artifactSourceNamesForIndexAlloc(alloc, cfg.name);
    defer {
        for (direct_sources) |name| alloc.free(name);
        if (direct_sources.len > 0) alloc.free(direct_sources);
    }
    try std.testing.expectEqual(@as(usize, 1), direct_sources.len);
    try std.testing.expectEqualStrings("document_chunks_v1", direct_sources[0]);

    const producer_path = try db.core.index_manager.artifactProducerNamesForIndexAlloc(alloc, cfg.name);
    defer {
        for (producer_path) |name| alloc.free(name);
        if (producer_path.len > 0) alloc.free(producer_path);
    }
    try std.testing.expectEqual(@as(usize, 2), producer_path.len);
    try std.testing.expect(containsName(producer_path, "document_chunks_v1"));
    try std.testing.expect(containsName(producer_path, "document_units_v1"));

    var admitted = try db.loadIndexRepairEntryById(alloc, repair_id);
    defer admitted.deinit(alloc);
    try std.testing.expectEqual(index_repair_state.SourceReplayState.pending, admitted.intent.source_replay_state);

    const replay = try db.advanceIndexRepairIntent(alloc, repair_id, .{});
    try std.testing.expect(replay.attempted);
    try std.testing.expect(replay.deferred);
    try std.testing.expectEqual(@as(u64, 1), replay.documents_reprocessed);
    try db.runUntilIdle();

    var repaired = false;
    for (0..16) |_| {
        const step = try db.advanceIndexRepairIntent(alloc, repair_id, repair_completion_test_options);
        if (step.repaired) {
            repaired = true;
            break;
        }
        try std.testing.expect(!step.terminal);
        try db.runUntilIdle();
    }
    try std.testing.expect(repaired);

    var result = try db.search(alloc, .{
        .index_name = cfg.name,
        .full_text = .{ .match = .{ .field = "text", .text = "gamma" } },
        .return_mode = .chunk,
    });
    defer result.deinit();
    try std.testing.expectEqual(@as(u32, 1), result.total_hits);
    try std.testing.expectEqualStrings("doc:a", result.hits[0].artifact_ref.?.document_id);
}

test "db managed vector admission discovers artifacts committed before resident activation" {
    const alloc = std.testing.allocator;
    const cfg = types.IndexConfig{
        .name = "late_external",
        .kind = .dense_vector,
        .config_json = "{\"field\":\"embedding\",\"dims\":3,\"metric\":\"l2_squared\",\"external\":true}",
    };
    const t = try TestDb.open(alloc, .{
        .start_index_workers = false,
        .start_optional_runtime_workers = false,
        .ttl_cleanup = .{ .enabled = false },
    });
    defer t.deinit();
    const db = &t.db;

    // Model a catalog create whose resident activation is queued while the
    // write path accepts the newly committed external artifact.
    try db.batch(.{
        .writes = &.{.{
            .key = "doc:a",
            .value = "{\"body\":\"arrived during activation\",\"_embeddings\":{\"late_external\":[1,0,0]}}",
        }},
        .sync_level = .write,
    });
    const artifact_key = try expectedDocumentEmbeddingArtifactKeyAlloc(alloc, "doc:a", cfg.name);
    defer alloc.free(artifact_key);
    const artifact = try db.core.store.get(alloc, artifact_key);
    defer alloc.free(artifact);

    const repair_id = (try db.admitManagedIndex(cfg)) orelse return error.TestUnexpectedResult;
    // A non-empty managed admission must bootstrap from the authoritative
    // artifact namespace. Zero here would incorrectly certify an empty target.
    try std.testing.expectEqual(
        @as(?u64, null),
        try DB.loadDenseArtifactTargetCounter(alloc, db.core.store, cfg.name),
    );

    var repaired = false;
    for (0..16) |_| {
        const step = try db.advanceIndexRepairIntent(alloc, repair_id, repair_completion_test_options);
        if (step.repaired) {
            repaired = true;
            break;
        }
        try std.testing.expect(!step.terminal);
    }
    try std.testing.expect(repaired);
    try std.testing.expectEqual(
        @as(?u64, 1),
        try DB.loadDenseArtifactTargetCounter(alloc, db.core.store, cfg.name),
    );
    try std.testing.expectEqual(
        @as(u64, 1),
        db.core.index_manager.denseIndex(cfg.name).?.index.stats().active_count,
    );
    var result = try db.search(alloc, .{
        .index_name = cfg.name,
        .query = .{ .dense_knn = .{ .vector = &.{ 1, 0, 0 }, .k = 1 } },
        .limit = 1,
    });
    defer result.deinit();
    try std.testing.expectEqual(@as(u32, 1), result.total_hits);
    try std.testing.expectEqualStrings("doc:a", result.hits[0].id);
}

test "db managed vector admission durably seeds missing enrichment artifacts" {
    const alloc = std.testing.allocator;
    var path_tmp = try TestDirectory.init("db");
    defer path_tmp.cleanup();
    const path = path_tmp.path().ptr;

    const document_count: usize = 129;
    const writes = try alloc.alloc(types.BatchWrite, document_count);
    defer {
        for (writes) |write| alloc.free(@constCast(write.key));
        alloc.free(writes);
    }
    for (writes, 0..) |*write, i| {
        write.* = .{
            .key = try std.fmt.allocPrint(alloc, "doc:{d:0>3}", .{i}),
            .value = "{\"body\":\"before admission\"}",
        };
    }

    var repair_id: u128 = 0;
    var first_page_target: u64 = 0;
    {
        var deterministic = embedder_mod.DeterministicDenseEmbedder{};
        var db = try DB.open(alloc, std.mem.span(path), .{
            .start_index_workers = false,
            .start_optional_runtime_workers = false,
            .ttl_cleanup = .{ .enabled = false },
            .enrichment = .{ .dense_embedder = deterministic.interface() },
        });
        defer db.close();
        try db.batch(.{ .writes = writes, .sync_level = .write });
        try db.addEnrichment(.{
            .name = "semantic_embedding",
            .kind = .embedding,
            .field = "body",
            .expected_dims = 3,
        });

        const before_admission = db.core.nextDerivedSequence();
        repair_id = (try db.admitManagedIndex(.{
            .name = "semantic_idx",
            .kind = .dense_vector,
            .config_json = "{\"field\":\"embedding\",\"dims\":3,\"embedding_name\":\"semantic_embedding\"}",
        })) orelse return error.TestUnexpectedResult;
        try std.testing.expect(repair_id != 0);

        {
            const driver = db.enrichment_runtime;
            db.enrichment_runtime = null;
            defer db.enrichment_runtime = driver;
            const unavailable = try db.advanceIndexRepairIntent(alloc, repair_id, .{ .source_replay_only = true });
            try std.testing.expect(!unavailable.attempted and !unavailable.repaired);
            var retained = try db.loadIndexRepairEntryById(alloc, repair_id);
            defer retained.deinit(alloc);
            try std.testing.expect(retained.intent.source_replay_state == .pending);
            try std.testing.expect(retained.intent.source_replay_resume_key == null);
        }

        // Admission installs the generator and durable repair owner, but does
        // no corpus-sized source scan while holding structural ownership.
        try std.testing.expectEqual(before_admission, db.core.nextDerivedSequence());
        try std.testing.expectEqual(
            before_admission,
            try db.core.loadAppliedSequence(alloc, "semantic_idx"),
        );
        var initial = try db.loadIndexRepairEntryById(alloc, repair_id);
        defer initial.deinit(alloc);
        try std.testing.expect(initial.intent.source_replay_resume_key == null);
        try std.testing.expectEqual(@as(u64, 0), initial.intent.source_replay_reprocessed);
        try std.testing.expectEqual(index_repair_state.SourceReplayState.pending, initial.intent.source_replay_state);

        // One owner quantum scans exactly one bounded page and publishes the
        // cursor only after its generated requests are durable.
        const first = try db.advanceIndexRepairIntent(alloc, repair_id, .{});
        try std.testing.expect(first.attempted);
        try std.testing.expect(first.deferred);
        try std.testing.expect(!first.repaired);
        try std.testing.expectEqual(@as(u64, 128), first.documents_reprocessed);
        var after_first = try db.loadIndexRepairEntryById(alloc, repair_id);
        defer after_first.deinit(alloc);
        try std.testing.expect(after_first.intent.source_replay_resume_key != null);
        try std.testing.expectEqual(@as(u64, 128), after_first.intent.source_replay_reprocessed);
        try std.testing.expectEqual(index_repair_state.SourceReplayState.pending, after_first.intent.source_replay_state);
        try std.testing.expect(after_first.intent.target_sequence > before_admission);
        first_page_target = after_first.intent.target_sequence;

        const canonical = db.core.denseIndex("semantic_idx") orelse return error.TestUnexpectedResult;
        try std.testing.expectEqual(@as(u64, 0), canonical.index.stats().active_count);
        try std.testing.expect(db.core.index_manager.repairUnavailable("semantic_idx"));
    }

    // Restart resumes after the durable primary-store cursor instead of
    // rescanning the first page or skipping the uncommitted suffix.
    var deterministic = embedder_mod.DeterministicDenseEmbedder{};
    var reopened = try DB.open(alloc, std.mem.span(path), .{
        .start_index_workers = false,
        .start_optional_runtime_workers = false,
        .ttl_cleanup = .{ .enabled = false },
        .enrichment = .{ .dense_embedder = deterministic.interface() },
    });
    defer reopened.close();
    try std.testing.expect(try DB.independentMaintenanceRun(&reopened, .repair));
    try std.testing.expect(reopened.local_source_recovery_pending);
    var seeded = try reopened.loadIndexRepairEntryById(alloc, repair_id);
    defer seeded.deinit(alloc);
    try std.testing.expect(seeded.intent.source_replay_resume_key == null);
    try std.testing.expectEqual(@as(u64, document_count), seeded.intent.source_replay_reprocessed);
    try std.testing.expectEqual(index_repair_state.SourceReplayState.complete, seeded.intent.source_replay_state);
    try std.testing.expect(seeded.intent.target_sequence > first_page_target);

    // With source discovery complete, provider/publication progress owns the
    // next edge. The repair must leave the immediate scheduler heap instead
    // of hot-polling while enrichment catches up.
    try reopened.ensureIndexRepairSchedulerDirectory(alloc);
    const waiting = try reopened.advanceIndexRepairIntent(alloc, repair_id, .{});
    try std.testing.expect(waiting.deferred);
    try std.testing.expect(!waiting.attempted);
    const summary = try reopened.indexRepairIntentSummary(alloc);
    switch (summary.wake) {
        .at_realtime_ms => |deadline| try std.testing.expect(deadline > currentTimeNs() / std.time.ns_per_ms),
        else => return error.TestUnexpectedResult,
    }
}

test "db managed repair scheduler defers canonical worker until shadow activation" {
    const alloc = std.testing.allocator;
    var deterministic = embedder_mod.DeterministicDenseEmbedder{};
    const t = try TestDb.open(alloc, .{
        .start_optional_runtime_workers = false,
        .ttl_cleanup = .{ .enabled = false },
        .enrichment = .{ .dense_embedder = deterministic.interface() },
    });
    defer t.deinit();
    const db = &t.db;
    try db.batch(.{
        .writes = &.{.{ .key = "doc:a", .value = "{\"body\":\"alpha\"}" }},
        .sync_level = .write,
    });

    const cfg = types.IndexConfig{
        .name = "semantic_idx",
        .kind = .dense_vector,
        .config_json =
        \\{"field":"body","dims":3,"metric":"cosine","embedding_name":"semantic_idx","publication_policy":"atomic","generator":{"kind":"dense_embedding","source_field":"body","embedding_name":"semantic_idx"}}
        ,
        .coverage_generation = 44,
    };
    // Model the provisioned runtime's durable repair owner without starting
    // unrelated optional test threads.
    db.local_execution.optional_runtime_workers_enabled = true;
    const repair_id = (try db.admitManagedIndex(cfg)) orelse return error.TestUnexpectedResult;
    db.local_execution.optional_runtime_workers_enabled = false;

    try std.testing.expect(db.executor.appliedSequence(cfg.name) == null);
    try drainManagedAdmissionSourceReplayForTest(db, alloc, repair_id);
    try std.testing.expect(db.executor.appliedSequence(cfg.name) == null);
    try std.testing.expectEqual(
        @as(u64, 1),
        (try DB.loadDenseArtifactTargetCounter(alloc, db.core.store, cfg.name)).?,
    );
    // Force the generation path even though the manual enrichment drain can
    // make the tiny canonical generation independently serviceable. This
    // isolates the activation contract under test: an absent admission worker
    // must be installed at the shadow's final replay fence.
    try db.updateIndexRepairIntent(alloc, repair_id, .{
        .trigger = .operator_generation_rebuild,
        .replace_last_error = true,
    });

    var repaired = false;
    for (0..16) |_| {
        const step = try db.advanceIndexRepairIntent(alloc, repair_id, repair_completion_test_options);
        if (step.repaired) {
            repaired = true;
            break;
        }
        try std.testing.expect(!step.terminal);
    }
    try std.testing.expect(repaired);
    const durable_applied = try db.core.loadAppliedSequence(alloc, cfg.name);
    try std.testing.expectEqual(durable_applied, db.executor.appliedSequence(cfg.name).?);
    try std.testing.expect(!db.core.index_manager.repairUnavailable(cfg.name));

    // Intent retirement must leave a live canonical consumer, not merely a
    // serviceable point-in-time generation. Publish one later derived record
    // and prove the newly attached worker advances from the durable cursor.
    const stored_key = try internal_keys.documentKeyAlloc(alloc, "doc:b");
    defer alloc.free(stored_key);
    try db.core.store.put(stored_key, "{\"body\":\"beta\"}");
    const artifact_key = try expectedDocumentEmbeddingArtifactKeyAlloc(alloc, "doc:b", cfg.name);
    defer alloc.free(artifact_key);
    try putDenseEmbeddingArtifactForTest(db, alloc, artifact_key, null, &[_]f32{ 0, 1, 0 });
    const later_sequence = try appendDerivedBatchRecord(db, .{
        .dense_embeddings = &.{.{
            .index_name = cfg.name,
            .doc_key = "doc:b",
            .artifact_key = artifact_key,
            .vector = &[_]f32{ 1, 0, 0 },
        }},
    });
    try db.runDerivedUntil(later_sequence);
    try std.testing.expect(db.executor.appliedSequence(cfg.name).? >= later_sequence);
}

test "db managed admission recovers legacy terminal source coverage lag" {
    try testManagedGenerationRepairAdmission(.coverage_lag);
}

test "db managed algebraic admission builds and reopens requires generation marker" {
    const alloc = std.testing.allocator;
    var path_tmp = try TestDirectory.init("db");
    defer path_tmp.cleanup();
    const path = path_tmp.path().ptr;

    const cfg = types.IndexConfig{
        .name = "analytics_idx",
        .kind = .algebraic,
        .config_json =
        \\{"version":1,"table":"docs","capability_lifecycle_status":"rebuild_required","dynamic_rules_backfill_pending":true,"group_fields":[{"name":"category","path":"category","type":"string"}],"dynamic_field_rules":[{"name":"ids","match":"*_id","type":"string"}],"materializations":[]}
        ,
    };
    {
        var db = try DB.open(alloc, std.mem.span(path), no_replay_no_background_test_open);
        defer db.close();
        try db.addIndex(cfg);
        try db.core.saveProjectionCheckpoint(cfg.name, .{
            .status = .clean,
            .generation = 9,
            .config_hash = types.indexConfigHash(cfg),
        });
    }

    var reopened = try DB.open(alloc, std.mem.span(path), no_replay_no_background_test_open);
    defer reopened.close();
    const active = reopened.core.index_manager.algebraicIndex(cfg.name) orelse return error.TestUnexpectedResult;
    try std.testing.expect(!active.index.projectionConfigReady());
    try std.testing.expect(!active.index.plannerLifecycleReady());
    try std.testing.expect(active.index.resolveField("user_id", .group) == null);
}

test "db managed algebraic admission builds and reopens an isolated generation" {
    const alloc = std.testing.allocator;
    var path_tmp = try TestDirectory.init("db");
    defer path_tmp.cleanup();
    const path = path_tmp.path().ptr;

    const cfg = types.IndexConfig{
        .name = "analytics_idx",
        .kind = .algebraic,
        .config_json =
        \\{"version":1,"table":"docs","capability_lifecycle_status":"rebuild_required","dynamic_rules_backfill_pending":true,"group_fields":[{"name":"category","path":"category","type":"string"}],"dynamic_field_rules":[{"name":"ids","match":"*_id","type":"string"}],"materializations":[]}
        ,
    };
    var active_namespace: []u8 = undefined;
    {
        var db = try DB.open(alloc, std.mem.span(path), no_replay_no_background_test_open);
        defer db.close();
        try db.batch(.{
            .writes = &.{.{ .key = "doc:old", .value = "{\"category\":\"tools\"}" }},
            .sync_level = .write,
        });

        const repair_id = (try db.admitManagedIndex(cfg)) orelse return error.TestUnexpectedResult;
        try std.testing.expect(db.core.index_manager.repairUnavailable(cfg.name));
        try std.testing.expect(try db.hasPendingIndexRepairIntents(alloc));
        const canonical = db.core.index_manager.algebraicIndex(cfg.name) orelse return error.TestUnexpectedResult;
        try std.testing.expectEqualStrings("canonical/analytics_idx", canonical.index.storageNamespace());
        try std.testing.expect(!canonical.index.plannerLifecycleReady());
        try std.testing.expect(canonical.index.resolveField("user_id", .group) == null);

        var repaired = false;
        for (0..16) |_| {
            const step = try db.advanceIndexRepairIntent(alloc, repair_id, repair_completion_test_options);
            if (step.repaired) {
                repaired = true;
                break;
            }
            try std.testing.expect(!step.terminal);
            try std.testing.expectEqual(@as(u64, 0), step.next_retry_at_ms);
        }
        try std.testing.expect(repaired);
        try std.testing.expect(!db.core.index_manager.repairUnavailable(cfg.name));
        try std.testing.expect(!try db.hasPendingIndexRepairIntents(alloc));

        const active = db.core.index_manager.algebraicIndex(cfg.name) orelse return error.TestUnexpectedResult;
        try std.testing.expect(std.mem.startsWith(u8, active.index.storageNamespace(), ".repair-shadow-"));
        try std.testing.expect(active.index.plannerLifecycleReady());
        try std.testing.expect(active.index.resolveField("user_id", .group) != null);
        active_namespace = try alloc.dupe(u8, active.index.storageNamespace());
        var resolved = (try active.index.resolvedDocFilterForFilterJsonAlloc(
            db.core.store,
            "{\"term\":{\"category\":\"tools\"}}",
        )) orelse return error.TestUnexpectedResult;
        defer resolved.deinit(alloc);
        switch (resolved.include) {
            .ordinals => |ordinals| try std.testing.expectEqual(@as(usize, 1), ordinals.len),
            else => return error.ExpectedOrdinalDocSet,
        }
    }
    defer alloc.free(active_namespace);

    var reopened = try DB.open(alloc, std.mem.span(path), no_replay_no_background_test_open);
    defer reopened.close();
    const active = reopened.core.index_manager.algebraicIndex(cfg.name) orelse return error.TestUnexpectedResult;
    try std.testing.expectEqualStrings(active_namespace, active.index.storageNamespace());
    try std.testing.expect(active.index.plannerLifecycleReady());
    try std.testing.expect(active.index.resolveField("user_id", .group) != null);
    var resolved = (try active.index.resolvedDocFilterForFilterJsonAlloc(
        reopened.core.store,
        "{\"term\":{\"category\":\"tools\"}}",
    )) orelse return error.TestUnexpectedResult;
    defer resolved.deinit(alloc);
    switch (resolved.include) {
        .ordinals => |ordinals| try std.testing.expectEqual(@as(usize, 1), ordinals.len),
        else => return error.ExpectedOrdinalDocSet,
    }
}

test "db managed full text admission survives restart without in-place backfill" {
    const alloc = std.testing.allocator;
    var path_tmp = try TestDirectory.init("db");
    defer path_tmp.cleanup();
    const path = path_tmp.path().ptr;

    const cfg = types.IndexConfig{
        .name = "full_text_index_v1",
        .kind = .full_text,
        .config_json = "{}",
    };
    var repair_checkpoint_path: ?[]u8 = null;
    defer if (repair_checkpoint_path) |value| alloc.free(value);
    {
        var db = try DB.open(alloc, std.mem.span(path), no_replay_no_background_test_open);
        defer db.close();
        try db.batch(.{
            .writes = &.{.{ .key = "doc:a", .value = "{\"body\":\"alpha\"}" }},
            .sync_level = .write,
        });

        const repair_id = (try db.admitManagedFullTextIndex(cfg)) orelse return error.TestUnexpectedResult;
        try std.testing.expectEqual(repair_id, (try db.materializeManagedIndexAdmission(alloc, cfg.name)).?);
        try std.testing.expect(try db.hasPendingIndexRepairIntents(alloc));
        repair_checkpoint_path = try alloc.dupe(u8, db.core.index_repair_checkpoint.?.path);

        const stats = try db.stats(alloc);
        defer types.freeDBStats(alloc, stats);
        const target = for (stats.indexes) |item| {
            if (std.mem.eql(u8, item.name, cfg.name)) break item;
        } else return error.TestUnexpectedResult;
        try std.testing.expectEqual(@as(u64, 0), target.doc_count);
    }

    // Model a crash after the atomic catalog/outbox commit but before the
    // repair checkpoint becomes durable. Reopen must reconstruct the intent
    // from the primary-store marker without running an in-place backfill.
    var io_impl = std.Io.Threaded.init(alloc, .{});
    defer io_impl.deinit();
    try std.Io.Dir.cwd().deleteFile(io_impl.io(), repair_checkpoint_path.?);

    // A read-only/status process cannot materialize the checkpoint, but the
    // primary marker must still close service during this crash window.
    var readonly = try DB.open(alloc, std.mem.span(path), .{
        .open_mode = .query_readonly,
        .start_index_workers = false,
        .ttl_cleanup = .{ .enabled = false },
    });
    defer readonly.close();
    try std.testing.expectError(error.IndexRebuilding, readonly.search(alloc, .{
        .index_name = cfg.name,
        .full_text = .{ .match = .{ .field = "body", .text = "alpha" } },
    }));

    var reopened = try DB.open(alloc, std.mem.span(path), no_replay_no_background_test_open);
    defer reopened.close();

    const reopened_stats = try reopened.stats(alloc);
    defer types.freeDBStats(alloc, reopened_stats);
    const reopened_target = for (reopened_stats.indexes) |item| {
        if (std.mem.eql(u8, item.name, cfg.name)) break item;
    } else return error.TestUnexpectedResult;
    try std.testing.expectEqual(@as(u64, 0), reopened_target.doc_count);
    const repair_id = (try reopened.materializeManagedIndexAdmission(alloc, cfg.name)) orelse
        return error.TestUnexpectedResult;
    const summary = try reopened.indexRepairIntentSummary(alloc);
    try std.testing.expectEqual(@as(usize, 1), summary.runnable);

    var repaired = false;
    for (0..16) |_| {
        const step = try reopened.advanceIndexRepairIntent(alloc, repair_id, repair_completion_test_options);
        if (step.repaired) {
            repaired = true;
            break;
        }
        try std.testing.expect(!step.terminal);
        try std.testing.expectEqual(@as(u64, 0), step.next_retry_at_ms);
    }
    try std.testing.expect(repaired);
    try std.testing.expect(!try reopened.hasPendingIndexRepairIntents(alloc));
    const admission_key = try internal_keys.managedIndexAdmissionKeyAlloc(alloc, cfg.name);
    defer alloc.free(admission_key);
    try std.testing.expectError(error.NotFound, reopened.core.store.get(alloc, admission_key));

    // This reader still owns the pre-activation root. Sidecar completion is
    // not permission to expose it; cache retirement and a fresh open publish
    // the repaired generation to subsequent requests.
    try std.testing.expectError(error.IndexRebuilding, readonly.search(alloc, .{
        .index_name = cfg.name,
        .full_text = .{ .match = .{ .field = "body", .text = "alpha" } },
    }));

    var result = try reopened.search(alloc, .{
        .index_name = cfg.name,
        .full_text = .{ .match = .{ .field = "body", .text = "alpha" } },
    });
    defer result.deinit();
    try std.testing.expectEqual(@as(u32, 1), result.total_hits);
}

test "db managed full text admission avoids debt for an empty source" {
    const alloc = std.testing.allocator;
    const t = try TestDb.open(alloc, no_replay_no_background_test_open);
    defer t.deinit();
    const db = &t.db;
    index_manager_mod.test_text_backfill_invocations = 0;
    defer index_manager_mod.test_text_backfill_invocations = 0;
    try std.testing.expect((try db.admitManagedFullTextIndex(.{
        .name = "full_text_index_v1",
        .kind = .full_text,
        .config_json = "{}",
    })) == null);
    try std.testing.expectEqual(@as(usize, 0), index_manager_mod.test_text_backfill_invocations);
    try std.testing.expect(!try db.hasPendingIndexRepairIntents(alloc));
}

test "db managed admission ignores stale zero identity cache" {
    const alloc = std.testing.allocator;
    const t = try TestDb.open(alloc, no_replay_no_background_test_open);
    defer t.deinit();
    const db = &t.db;
    try db.batch(.{
        .writes = &.{.{ .key = "doc:a", .value = "{\"body\":\"alpha\"}" }},
        .sync_level = .write,
    });

    // Model a primary commit followed by a hot-standby mirror failure before runtime
    // cache publication. Admission authority must remain the primary store.
    db.core.identity_visibility.summary = .{};
    try std.testing.expect((try db.admitManagedFullTextIndex(.{
        .name = "full_text_index_v1",
        .kind = .full_text,
        .config_json = "{}",
    })) != null);
    try std.testing.expect(try db.hasPendingIndexRepairIntents(alloc));
}

test "db managed index deletion commits catalog absence with marker removal" {
    const alloc = std.testing.allocator;
    var path_tmp = try TestDirectory.init("db");
    defer path_tmp.cleanup();
    const path = path_tmp.path().ptr;
    const index_name = "full_text_index_v1";

    {
        var db = try DB.open(alloc, std.mem.span(path), no_replay_no_background_test_open);
        defer db.close();
        try db.batch(.{
            .writes = &.{.{ .key = "doc:a", .value = "{\"body\":\"alpha\"}" }},
            .sync_level = .write,
        });
        try std.testing.expect((try db.admitManagedFullTextIndex(.{
            .name = index_name,
            .kind = .full_text,
            .config_json = "{}",
        })) != null);

        DB.test_fail_managed_index_delete_after_catalog_commit = true;
        defer DB.test_fail_managed_index_delete_after_catalog_commit = false;
        try std.testing.expectError(error.TestManagedIndexDeleteCrash, db.deleteIndex(index_name));
        DB.test_fail_managed_index_delete_after_catalog_commit = false;
    }

    // The simulated crash precedes checkpoint cleanup. Catalog and marker must
    // nevertheless agree, and writable startup reclaims the orphaned intent.
    var reopened = try DB.open(alloc, std.mem.span(path), no_replay_no_background_test_open);
    defer reopened.close();
    try std.testing.expect(reopened.core.index_manager.get(index_name) == null);
    const admission_key = try internal_keys.managedIndexAdmissionKeyAlloc(alloc, index_name);
    defer alloc.free(admission_key);
    try std.testing.expectError(error.NotFound, reopened.core.store.get(alloc, admission_key));
    try std.testing.expect(!try reopened.hasPendingIndexRepairIntents(alloc));
}

test "db managed index deletion remains successful after post-commit cleanup failure" {
    const alloc = std.testing.allocator;
    const index_name = "full_text_index_v1";

    const t = try TestDb.open(alloc, no_replay_no_background_test_open);
    defer t.deinit();
    const db = &t.db;
    try db.batch(.{
        .writes = &.{.{ .key = "doc:a", .value = "{\"body\":\"alpha\"}" }},
        .sync_level = .write,
    });
    const old_repair_id = (try db.admitManagedFullTextIndex(.{
        .name = index_name,
        .kind = .full_text,
        .config_json = "{}",
    })) orelse return error.TestUnexpectedResult;

    index_manager_mod.test_inject_index_removal_cleanup_error = error.TestPostCommitCleanup;
    defer index_manager_mod.test_inject_index_removal_cleanup_error = null;
    index_manager_mod.test_generated_artifact_cleanup_failures_remaining.store(std.math.maxInt(u32), .release);
    defer index_manager_mod.test_generated_artifact_cleanup_failures_remaining.store(0, .release);
    DB.test_fail_managed_index_repair_cleanup_after_catalog_commit = true;
    defer DB.test_fail_managed_index_repair_cleanup_after_catalog_commit = false;
    try std.testing.expect(try db.deleteIndex(index_name));
    index_manager_mod.test_inject_index_removal_cleanup_error = null;
    DB.test_fail_managed_index_repair_cleanup_after_catalog_commit = false;

    try std.testing.expect(db.core.index_manager.get(index_name) == null);
    const admission_key = try internal_keys.managedIndexAdmissionKeyAlloc(alloc, index_name);
    defer alloc.free(admission_key);
    try std.testing.expectError(error.NotFound, db.core.store.get(alloc, admission_key));
    try std.testing.expect(try db.hasPendingIndexRepairIntents(alloc));

    // Same-name recreation is bounded: it reports the durable retirement debt
    // instead of scanning the corpus on the request thread. The generation
    // owner drains that debt and reconciliation can then retry admission.
    try std.testing.expectError(error.IndexArtifactCleanupPending, db.admitManagedFullTextIndex(.{
        .name = index_name,
        .kind = .full_text,
        .config_json = "{}",
    }));
    index_manager_mod.test_generated_artifact_cleanup_failures_remaining.store(0, .release);
    db.backend_runtime.durable_jobs.drainOwner(db.repair_cleanup_owner_id);

    const new_repair_id = (try db.admitManagedFullTextIndex(.{
        .name = index_name,
        .kind = .full_text,
        .config_json = "{}",
    })) orelse return error.TestUnexpectedResult;
    try std.testing.expect(new_repair_id != old_repair_id);
    try std.testing.expect(db.core.index_manager.get(index_name) != null);
}

test "db generated artifact cleanup retries beyond the transient burst" {
    const alloc = std.testing.allocator;
    const cfg: types.IndexConfig = .{
        .name = "full_text_index_v1",
        .kind = .full_text,
        .config_json = "{}",
    };

    const t = try TestDb.open(alloc, no_replay_no_background_test_open);
    defer t.deinit();
    const db = &t.db;
    try db.addIndex(cfg);
    const old_provenance = (try db.core.textIndexEntry(cfg.name).?.persistent.readGenerationMetadataAlloc(
        alloc,
        persistent_mod.text_projection_provenance_meta_key,
    )) orelse return error.TestExpectedEqual;
    defer alloc.free(old_provenance);

    index_manager_mod.test_generated_artifact_cleanup_failures_remaining.store(12, .release);
    defer index_manager_mod.test_generated_artifact_cleanup_failures_remaining.store(0, .release);
    try std.testing.expect(try db.deleteIndex(cfg.name));
    db.backend_runtime.durable_jobs.drainOwner(db.repair_cleanup_owner_id);

    try std.testing.expectEqual(
        @as(u32, 0),
        index_manager_mod.test_generated_artifact_cleanup_failures_remaining.load(.acquire),
    );
    try db.addIndex(cfg);
    const new_provenance = (try db.core.textIndexEntry(cfg.name).?.persistent.readGenerationMetadataAlloc(
        alloc,
        persistent_mod.text_projection_provenance_meta_key,
    )) orelse return error.TestExpectedEqual;
    defer alloc.free(new_provenance);
    try std.testing.expect(!std.mem.eql(u8, old_provenance, new_provenance));
}

test "db generated artifact finalization releases page arbitration and preserves admission fence" {
    const alloc = std.testing.allocator;
    const first: types.IndexConfig = .{
        .name = "a_full_text",
        .kind = .full_text,
        .config_json = "{}",
    };
    const second: types.IndexConfig = .{
        .name = "b_full_text",
        .kind = .full_text,
        .config_json = "{}",
    };

    const t = try TestDb.open(alloc, no_replay_no_background_test_open);
    defer t.deinit();
    const db = &t.db;
    try db.addIndex(first);
    try db.addIndex(second);

    DB.test_generated_artifact_finalization_entered.store(false, .release);
    DB.test_release_generated_artifact_finalization.store(false, .release);
    DB.test_block_generated_artifact_finalization.store(true, .release);
    defer {
        DB.test_release_generated_artifact_finalization.store(true, .release);
        DB.test_block_generated_artifact_finalization.store(false, .release);
    }
    try std.testing.expect(try db.deleteIndex(first.name));
    var wait_attempts: usize = 0;
    while (!DB.test_generated_artifact_finalization_entered.load(.acquire)) : (wait_attempts += 1) {
        try std.testing.expect(wait_attempts < 100_000);
        std.testing.io.sleep(.fromNanoseconds(1), .awake) catch {};
    }

    try std.testing.expect(db.async_context.index_artifact_cleanup_mutex.tryLock());
    db.async_context.index_artifact_cleanup_mutex.unlock();
    try std.testing.expectError(error.IndexArtifactCleanupPending, db.addIndex(first));

    try std.testing.expect(try db.deleteIndex(second.name));
    try std.testing.expectEqual(
        DB.GeneratedArtifactCleanupAdvanceResult.progressed,
        try db.advanceGeneratedArtifactCleanupPage(second.name),
    );

    DB.test_release_generated_artifact_finalization.store(true, .release);
    db.backend_runtime.durable_jobs.drainOwner(db.repair_cleanup_owner_id);

    var second_drained = false;
    for (0..1_000) |_| {
        switch (try db.advanceGeneratedArtifactCleanupPage(second.name)) {
            .idle => {
                second_drained = true;
                break;
            },
            .progressed => {},
            .busy => std.testing.io.sleep(.fromNanoseconds(1), .awake) catch {},
        }
    }
    try std.testing.expect(second_drained);
    try db.addIndex(first);
    try db.addIndex(second);
}

test "db managed admission drain preserves a generation requested during the pass" {
    const alloc = std.testing.allocator;
    const t = try TestDb.open(alloc, no_replay_no_background_test_open);
    defer t.deinit();
    const db = &t.db;

    const Hook = struct {
        passes: usize = 0,

        fn afterPass(ptr: *anyopaque, hooked_db: *DB) void {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            self.passes += 1;
            if (self.passes == 1) hooked_db.requestManagedAdmissionMaterialization();
        }
    };
    var hook = Hook{};
    DB.test_managed_admission_materialization_hook = .{
        .ptr = &hook,
        .after_pass = Hook.afterPass,
    };
    defer DB.test_managed_admission_materialization_hook = null;

    db.requestManagedAdmissionMaterialization();
    try db.drainManagedIndexAdmissions(alloc);
    try std.testing.expectEqual(@as(usize, 2), hook.passes);
    try std.testing.expect(!db.managedAdmissionMaterializationPending());
}

test "db managed admission materialization serializes with index deletion" {
    const alloc = std.testing.allocator;
    var path_tmp = try TestDirectory.init("db");
    defer path_tmp.cleanup();
    const path = path_tmp.path().ptr;
    const index_name = "full_text_index_v1";

    var db = try DB.open(alloc, std.mem.span(path), no_replay_no_background_test_open);
    defer db.close();
    try db.batch(.{
        .writes = &.{.{ .key = "doc:a", .value = "{\"body\":\"alpha\"}" }},
        .sync_level = .write,
    });
    _ = try db.installIndexWhileEnrichmentQuiesced(.{
        .name = index_name,
        .kind = .full_text,
        .config_json = "{}",
    }, .managed);

    const Race = struct {
        db: *DB,
        entered: std.atomic.Value(bool) = .init(false),
        release: std.atomic.Value(bool) = .init(false),
        delete_started: std.atomic.Value(bool) = .init(false),
        delete_completed: std.atomic.Value(bool) = .init(false),
        materialize_result: ?u128 = null,
        materialize_err: ?anyerror = null,
        delete_result: bool = false,
        delete_err: ?anyerror = null,

        fn afterConfigLookup(ptr: *anyopaque, _: *DB, _: []const u8) void {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            self.entered.store(true, .release);
            while (!self.release.load(.acquire)) std.testing.io.sleep(.fromNanoseconds(1), .awake) catch {};
        }

        fn materialize(self: *@This()) void {
            self.materialize_result = self.db.materializeManagedIndexAdmission(self.db.alloc, index_name) catch |err| {
                self.materialize_err = err;
                return;
            };
        }

        fn delete(self: *@This()) void {
            self.delete_started.store(true, .release);
            self.delete_result = self.db.deleteIndex(index_name) catch |err| {
                self.delete_err = err;
                self.delete_completed.store(true, .release);
                return;
            };
            self.delete_completed.store(true, .release);
        }
    };

    var race = Race{ .db = &db };
    DB.test_managed_admission_materialization_hook = .{
        .ptr = &race,
        .after_config_lookup = Race.afterConfigLookup,
    };
    defer DB.test_managed_admission_materialization_hook = null;

    var materialize_thread = try std.testing.io.concurrent(Race.materialize, .{&race});
    var entered = false;
    for (0..100_000) |_| {
        if (race.entered.load(.acquire)) {
            entered = true;
            break;
        }
        std.testing.io.sleep(.fromNanoseconds(1), .awake) catch {};
    }
    if (!entered) {
        race.release.store(true, .release);
        materialize_thread.await(std.testing.io);
        return error.TestTimeout;
    }

    var delete_thread = std.testing.io.concurrent(Race.delete, .{&race}) catch |err| {
        race.release.store(true, .release);
        materialize_thread.await(std.testing.io);
        return err;
    };
    var delete_started = false;
    for (0..100_000) |_| {
        if (race.delete_started.load(.acquire)) {
            delete_started = true;
            break;
        }
        std.testing.io.sleep(.fromNanoseconds(1), .awake) catch {};
    }
    if (!delete_started) {
        race.release.store(true, .release);
        materialize_thread.await(std.testing.io);
        delete_thread.await(std.testing.io);
        return error.TestTimeout;
    }
    for (0..256) |_| std.testing.io.sleep(.fromNanoseconds(1), .awake) catch {};
    const deletion_crossed_materialization = race.delete_completed.load(.acquire);
    race.release.store(true, .release);
    materialize_thread.await(std.testing.io);
    delete_thread.await(std.testing.io);

    try std.testing.expect(!deletion_crossed_materialization);
    try std.testing.expect(race.materialize_err == null);
    try std.testing.expect(race.materialize_result != null);
    try std.testing.expect(race.delete_err == null);
    try std.testing.expect(race.delete_result);
    try std.testing.expect(db.core.index_manager.get(index_name) == null);
    try std.testing.expect(!try db.hasPendingIndexRepairIntents(alloc));
}

test "db managed admission materialization never infers debt from replay lag" {
    const alloc = std.testing.allocator;
    const t = try TestDb.open(alloc, no_replay_no_background_test_open);
    defer t.deinit();
    const db = &t.db;
    try db.addIndex(.{ .name = "ft", .kind = .full_text, .config_json = "{}" });
    try db.batch(.{
        .writes = &.{.{ .key = "doc:a", .value = "{\"body\":\"alpha\"}" }},
        .sync_level = .write,
    });

    try std.testing.expect((try db.materializeManagedIndexAdmission(alloc, "ft")) == null);
    try std.testing.expect(!try db.hasPendingIndexRepairIntents(alloc));
}

test "db managed visibility hook rehydrates exact durable initial build debt once" {
    const alloc = std.testing.allocator;
    var path_tmp = try TestDirectory.init("db");
    defer path_tmp.cleanup();
    const path = path_tmp.path().ptr;
    const index_name = "full_text_index_v1";

    const repair_id = repair: {
        var db = try DB.open(alloc, std.mem.span(path), no_replay_no_background_test_open);
        defer db.close();
        try db.batch(.{
            .writes = &.{.{ .key = "doc:a", .value = "{\"body\":\"alpha\"}" }},
            .sync_level = .write,
        });

        // Admission happens before the managed owner installs its hook. Its
        // process-local edge is deliberately lost across this close.
        break :repair (try db.admitManagedFullTextIndex(.{
            .name = index_name,
            .kind = .full_text,
            .config_json = "{}",
        })) orelse return error.TestUnexpectedResult;
    };

    var db = try DB.open(alloc, std.mem.span(path), no_replay_no_background_test_open);
    defer db.close();

    const Hook = struct {
        pending: usize = 0,
        pending_unknown: usize = 0,
        repair_id: u128 = 0,
        index_name_matches: bool = false,
        work_class: ?IndexLifecycleWorkClass = null,

        fn onChange(ptr: *anyopaque, event: QueryVisibilityEvent) void {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            if (event.change != .index_repair_pending) return;
            self.pending += 1;
            const repair = event.repair orelse {
                self.pending_unknown += 1;
                return;
            };
            self.repair_id = repair.repair_id;
            self.index_name_matches = std.mem.eql(u8, repair.index_name, index_name);
            self.work_class = repair.work_class;
        }
    };
    var hook = Hook{};
    // A clear for one intent cannot prove that all durable repair debt is
    // gone, so it must not consume a queued scheduler wakeup.
    DB.notifyQueryVisibilityHook(db.async_context, .index_repair_cleared);
    db.setQueryVisibilityHook(.{
        .ptr = &hook,
        .on_change = Hook.onChange,
    });
    try std.testing.expectEqual(@as(usize, 1), hook.pending);
    try std.testing.expectEqual(@as(usize, 0), hook.pending_unknown);
    try std.testing.expectEqual(repair_id, hook.repair_id);
    try std.testing.expect(hook.index_name_matches);
    try std.testing.expectEqual(IndexLifecycleWorkClass.initial_build, hook.work_class.?);

    try std.testing.expectEqual(
        repair_id,
        (try db.materializeManagedIndexAdmission(alloc, index_name)) orelse
            return error.TestUnexpectedResult,
    );
    // Reconciliation adopts the same durable intent without creating a hot
    // loop of cache invalidations and duplicate scheduler notifications.
    try std.testing.expectEqual(@as(usize, 1), hook.pending);
}

test "db managed admission rejects regressed identity evidence" {
    const alloc = std.testing.allocator;
    const t = try TestDb.open(alloc, no_replay_no_background_test_open);
    defer t.deinit();
    const db = &t.db;
    try db.batch(.{
        .writes = &.{.{ .key = "doc:a", .value = "{\"body\":\"alpha\"}" }},
        .sync_level = .write,
    });
    const cfg = types.IndexConfig{
        .name = "full_text_index_v1",
        .kind = .full_text,
        .config_json = "{}",
    };
    _ = (try db.admitManagedFullTextIndex(cfg)) orelse return error.TestUnexpectedResult;

    const admission_key = try internal_keys.managedIndexAdmissionKeyAlloc(alloc, cfg.name);
    defer alloc.free(admission_key);
    const raw = try db.core.store.get(alloc, admission_key);
    defer alloc.free(raw);
    var marker = try DB.decodeManagedIndexAdmissionMarker(raw);
    marker.identity_generation += 1;
    const regressed_marker = DB.encodeManagedIndexAdmissionMarker(marker);
    try db.core.store.put(admission_key, &regressed_marker);

    try std.testing.expectError(
        error.InvalidDocIdentity,
        db.materializeManagedIndexAdmission(alloc, cfg.name),
    );
    try std.testing.expect(try db.hasPendingIndexRepairIntents(alloc));
}

test "db managed admission reconciliation retains same-name catalog mismatches" {
    const alloc = std.testing.allocator;
    const t = try TestDb.open(alloc, no_replay_no_background_test_open);
    defer t.deinit();
    const db = &t.db;
    const index_name = "dense_idx";
    try db.addIndex(.{
        .name = index_name,
        .kind = .dense_vector,
        .config_json = "{\"field\":\"embedding\",\"dims\":2,\"external\":true}",
    });

    const admission_key = try internal_keys.managedIndexAdmissionKeyAlloc(alloc, index_name);
    defer alloc.free(admission_key);
    const marker = DB.encodeManagedIndexAdmissionMarker(.{
        .config_hash = 1,
        .source_doc_count = 1,
        .identity_generation = 0,
        .replay_target_sequence = db.core.nextDerivedSequence(),
    });
    try db.core.store.put(admission_key, &marker);
    db.requestManagedAdmissionMaterialization();

    try std.testing.expectError(error.InvalidManagedIndexAdmission, db.drainManagedIndexAdmissions(alloc));
    const retained = try db.core.store.get(alloc, admission_key);
    defer alloc.free(retained);
    try std.testing.expectEqualSlices(u8, &marker, retained);
    try std.testing.expect(db.managedAdmissionMaterializationPending());
}

test "db inline dense generation remains rebuilding until outcomes cover the live corpus" {
    const alloc = std.testing.allocator;
    var runtime = std.Io.Threaded.init(alloc, .{});
    defer runtime.deinit();
    const io = runtime.io();

    const t = try TestDb.open(alloc, no_background_test_open);
    defer t.deinit();
    const db = &t.db;

    const config: types.IndexConfig = .{
        .name = "dense_idx",
        .kind = .dense_vector,
        .config_json = "{\"field\":\"embedding\",\"dims\":3,\"metric\":\"l2_squared\"}",
    };
    try db.addIndex(config);
    try db.batch(.{
        .writes = &.{
            .{ .key = "doc:a", .value = "{\"embedding\":[1,0,0]}" },
            .{ .key = "doc:b", .value = "{\"title\":\"no vector\"}" },
        },
        .sync_level = .full_index,
    });

    const generation = db.core.index_manager.coverageGenerationForIndex(config.name) orelse
        return error.TestUnexpectedResult;
    const CounterFixture = struct {
        fn write(
            fixture_alloc: Allocator,
            store: *docstore_mod.DocStore,
            index_name: []const u8,
            coverage_generation: u64,
            counts: [3]u64,
        ) !void {
            const outcome_names = [_][]const u8{ "produced", "skipped", "terminal_failed" };
            var keys: [outcome_names.len][]u8 = undefined;
            var initialized: usize = 0;
            defer for (keys[0..initialized]) |key| fixture_alloc.free(key);
            var values: [outcome_names.len][8]u8 = undefined;
            var writes: [outcome_names.len]docstore_mod.KVPair = undefined;
            for (outcome_names, counts, 0..) |outcome, count, i| {
                keys[i] = try internal_keys.derivedCoverageOutcomeCountKeyAlloc(
                    fixture_alloc,
                    index_name,
                    coverage_generation,
                    outcome,
                );
                initialized += 1;
                writes[i] = .{
                    .key = keys[i],
                    .value = internal_keys.encodeDerivedCoverageOutcomeCount(&values[i], count),
                };
            }
            try store.putBatch(&writes, &.{});
        }
    };

    const applied = try db.core.loadAppliedSequence(alloc, config.name);
    try db.core.saveProjectionCheckpoint(config.name, .{
        .applied_sequence = applied,
        .status = .rebuilding,
        .generation = 9,
        .config_hash = types.indexConfigHash(config),
    });

    // One indexed source and one unaccounted source is a normal bounded-replay
    // intermediate state. It must not be confused with complete coverage.
    try CounterFixture.write(alloc, db.core.store, config.name, generation, .{ 1, 0, 0 });
    try std.testing.expect(!try db.finalizeDenseProjectionLifecycleForIdle());
    var checkpoint = try db.core.loadProjectionCheckpoint(alloc, config.name);
    try std.testing.expectEqual(apply_state.ProjectionStatus.rebuilding, checkpoint.status);
    try std.testing.expectEqual(@as(u64, 9), checkpoint.generation);

    // Corrupt counters fail explicitly instead of wrapping into a false proof.
    try CounterFixture.write(alloc, db.core.store, config.name, generation, .{ std.math.maxInt(u64), 1, 0 });
    try std.testing.expectError(
        error.InvalidDerivedCoverageCounter,
        denseTargetCountForIndexContext(db.async_context, config.name),
    );

    // A genuinely partial persisted tuple remains corruption. Snapshot reads
    // must prevent races without turning this state into a valid new epoch.
    const missing_key = try internal_keys.derivedCoverageOutcomeCountKeyAlloc(alloc, config.name, generation, "skipped");
    defer alloc.free(missing_key);
    try db.core.store.delete(missing_key);
    try std.testing.expectError(
        error.InvalidDerivedCoverageCounter,
        denseTargetCountForIndexContext(db.async_context, config.name),
    );

    // Once every live source has a terminal outcome, the recurring idle
    // finalizer must retry the same rebuilding checkpoint without requiring a
    // new source write or session-close callback.
    try CounterFixture.write(alloc, db.core.store, config.name, generation, .{ 1, 1, 0 });
    // A no-op replay callback may leave its last cut coalesced. The recurring
    // owner must flush that cut as well as retry the lifecycle certificate.
    // The coalescer owns its keys with the async context allocator, which
    // frees them on flush; seed it with the same allocator.
    try db.async_context.applied_sequence_coalescer.note(db.async_context.alloc, config.name, applied);
    try std.testing.expectEqual(@as(usize, 1), db.async_context.applied_sequence_coalescer.pending.count());
    const deadline = monotonicTimeNs() +| 10 * std.time.ns_per_s;
    while (true) {
        _ = try DB.independentMaintenanceRun(db, .repair);
        checkpoint = try db.core.loadProjectionCheckpoint(alloc, config.name);
        if (checkpoint.status == .clean) break;
        if (monotonicTimeNs() >= deadline) return error.TestUnexpectedResult;
        try io.sleep(std.Io.Duration.fromMilliseconds(1), .awake);
    }
    checkpoint = try db.core.loadProjectionCheckpoint(alloc, config.name);
    try std.testing.expectEqual(apply_state.ProjectionStatus.clean, checkpoint.status);
    try std.testing.expectEqual(@as(u64, 10), checkpoint.generation);
    try std.testing.expectEqual(@as(usize, 0), db.async_context.applied_sequence_coalescer.pending.count());
}

test "db inline dense coverage counters rebase with range ownership" {
    const alloc = std.testing.allocator;

    const t = try TestDb.open(alloc, no_background_test_open);
    defer t.deinit();
    const db = &t.db;

    const config: types.IndexConfig = .{
        .name = "dense_idx",
        .kind = .dense_vector,
        .config_json = "{\"field\":\"embedding\",\"dims\":3,\"metric\":\"l2_squared\"}",
    };
    try db.addIndex(config);
    try db.batch(.{
        .writes = &.{
            .{ .key = "doc:a", .value = "{\"embedding\":[1,0,0]}" },
            .{ .key = "doc:z", .value = "{\"embedding\":[0,1,0]}" },
        },
        .sync_level = .full_index,
    });

    try std.testing.expectEqual(@as(?u64, 2), try denseTargetCountForIndexContext(db.async_context, config.name));
    try db.updateRange(.{ .start = "", .end = "doc:m" });
    try std.testing.expectEqual(@as(?u64, 1), try denseTargetCountForIndexContext(db.async_context, config.name));
    try std.testing.expectEqual(@as(?u64, 1), try range_cardinality.load(alloc, db.core.store));
    try std.testing.expectEqual(@as(u64, 2), (try doc_identity.fastStatsFromStore(db.core.store)).live_ordinals);

    // Expanding ownership is the merge-side topology transition. Rebase sees
    // already-present donor rows and republishes the compact local summaries
    // in the same durable batch as the new range descriptor.
    try db.updateRange(.{ .start = "", .end = "" });
    try std.testing.expectEqual(@as(?u64, 2), try denseTargetCountForIndexContext(db.async_context, config.name));
    try std.testing.expectEqual(@as(?u64, 2), try range_cardinality.load(alloc, db.core.store));
}

test "db enrichment graph ttl replay honors source tombstone" {
    const alloc = std.testing.allocator;
    var path_tmp = try TestDirectory.init("db-enrichment-edge-tombstone");
    defer path_tmp.cleanup();
    const path = path_tmp.path().ptr;

    var db = try DB.open(alloc, std.mem.span(path), .{});
    defer db.close();
    try db.addEnrichment(.{ .name = "relations_v1", .kind = .asset, .field = "relations", .content_type = "application/json" });
    try db.addIndex(.{ .name = "relations_graph", .kind = .graph, .config_json =
        \\{"ttl":{"duration":"1h"},"sources":[{"artifact":"relations_v1"}]}
    });
    try db.batch(.{ .writes = &.{.{ .key = "doc:a", .value =
        \\{"relations":{"type":"mentions","target":{"document_id":"doc:b"},"weight":2}}
    }}, .sync_level = .enrichments });
    try db.runUntilIdle();

    const generation = db.core.index_manager.graphIndex("relations_graph").?.config.coverage_generation;
    const edge_key = try internal_keys.graphEdgeArtifactKeyWithSourceAlloc(alloc, "doc:a", "relations_graph", "mentions", "doc:b", "doc:a");
    defer alloc.free(edge_key);
    const contender_prefix = try internal_keys.graphGlobalEdgeContenderEdgePrefixAlloc(alloc, "relations_graph", generation, edge_key);
    defer alloc.free(contender_prefix);
    const initial = try db.core.store.scanPrefix(alloc, contender_prefix);
    defer docstore_mod.DocStore.freeResults(alloc, initial);
    try std.testing.expectEqual(@as(usize, 1), initial.len);
    const view = (try graph_edge_contender.decode(initial[0].value, generation)) orelse return error.TestExpectedGraphEdgeContender;
    const state_key = try alloc.dupe(u8, view.state_key);
    defer alloc.free(state_key);
    const digest = try graph_edge_ttl_tombstone.sourceDigest(alloc, view.payload);
    const tombstone_key = try internal_keys.graphEdgeTtlTombstoneKeyAlloc(alloc, edge_key, "relations_graph", generation, state_key);
    defer alloc.free(tombstone_key);
    const tombstone = (graph_edge_ttl_tombstone.Tombstone{ .deadline_ns = currentTimeNs(), .source_digest = digest }).encode();
    try db.core.store.put(tombstone_key, &tombstone);

    try db.batch(.{ .writes = &.{.{ .key = "doc:a", .value =
        \\{"relations":{"type":"mentions","target":{"document_id":"doc:b"},"weight":2},"note":1}
    }}, .sync_level = .enrichments });
    try db.runUntilIdle();
    const suppressed = try db.core.store.scanPrefix(alloc, contender_prefix);
    defer docstore_mod.DocStore.freeResults(alloc, suppressed);
    try std.testing.expectEqual(@as(usize, 0), suppressed.len);
    try std.testing.expectError(error.NotFound, db.core.store.get(alloc, edge_key));

    try db.batch(.{ .writes = &.{.{ .key = "doc:a", .value =
        \\{"relations":{"type":"mentions","target":{"document_id":"doc:b"},"weight":3},"note":1}
    }}, .sync_level = .enrichments });
    try db.runUntilIdle();
    const restored = try db.core.store.scanPrefix(alloc, contender_prefix);
    defer docstore_mod.DocStore.freeResults(alloc, restored);
    try std.testing.expectEqual(@as(usize, 1), restored.len);
    try std.testing.expectError(error.NotFound, db.core.store.get(alloc, tombstone_key));
    const visible = try db.getEdges(alloc, "relations_graph", "doc:a", "mentions", .out);
    defer graph_mod.GraphIndex.freeEdges(alloc, visible);
    try std.testing.expectEqual(@as(usize, 1), visible.len);
    try std.testing.expectEqual(@as(f64, 3), visible[0].weight);
}

test "db generated write read fences detect artifact mutations and ABA independently of TTL" {
    const alloc = std.testing.allocator;
    var directory = try TestDirectory.init("generated-read-fence");
    defer directory.cleanup();
    var db = try DB.open(alloc, directory.path(), .{ .start_optional_runtimes = false, .start_index_workers = false });
    defer db.close();
    const writes = &.{types.BatchWrite{ .key = "doc:a", .value = "{\"body\":\"alpha\"}" }};
    try db.batch(.{ .writes = writes, .timestamp_ns = 42 });
    var snapshot = try db.captureGeneratedWriteReadSnapshot(alloc, writes);
    defer snapshot.deinit(alloc);

    // A different document must not cause optimistic retries.
    try db.batch(.{ .writes = &.{.{ .key = "doc:b", .value = "{}" }}, .timestamp_ns = 42 });
    try db.validateGeneratedWriteReadSnapshot(snapshot);

    // Artifact-only commits leave both the primary bytes and TTL untouched.
    const artifact = try internal_keys.chunkArtifactKeyAlloc(alloc, "doc:a", "chunks", 0);
    defer alloc.free(artifact);
    try db.core.store.put(artifact, "{\"text\":\"beta\"}");
    try std.testing.expectError(error.PreparedReadSetChanged, db.validateGeneratedWriteReadSnapshot(snapshot));

    snapshot.deinit(alloc);
    snapshot = try db.captureGeneratedWriteReadSnapshot(alloc, writes);
    try db.core.store.delete(artifact);
    try std.testing.expectError(error.PreparedReadSetChanged, db.validateGeneratedWriteReadSnapshot(snapshot));

    snapshot.deinit(alloc);
    snapshot = try db.captureGeneratedWriteReadSnapshot(alloc, writes);
    try db.batch(.{ .deletes = &.{"doc:a"}, .timestamp_ns = 42 });
    try db.batch(.{ .writes = writes, .timestamp_ns = 42 });
    try std.testing.expectError(error.PreparedReadSetChanged, db.validateGeneratedWriteReadSnapshot(snapshot));
}
