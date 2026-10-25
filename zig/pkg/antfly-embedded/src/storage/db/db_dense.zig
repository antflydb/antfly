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

//! Dense operations of the local storage DB. Methods take `*DB` and
//! are re-exported from `DB` in db.zig, so callers keep writing `db.method()`.

const db_zig = @import("db.zig");
const DB = db_zig.DB;
const TestDirectory = @import("../../common/test_directory.zig").TestDirectory;
const apply_state = @import("derived/apply_state.zig");
const artifact_ids = @import("artifact/ids.zig");
const backend_types = @import("../backend_types.zig");
const backfill_state_mod = @import("backfill_state.zig");
const builtin = @import("builtin");
const change_journal_mod = @import("derived/change_journal.zig");
const db_query_metrics = @import("query_metrics.zig");
const db_query_search = @import("query/search_exec.zig");
const dense_exact = @import("dense_exact.zig");
const derived_types = @import("derived/derived_types.zig");
const doc_identity = @import("doc_identity.zig");
const doc_set = @import("doc_set.zig");
const docstore_mod = @import("../docstore.zig");
const embedder_mod = @import("enrichment/embedder.zig");
const enrichment_artifact_codec = @import("enrichment/artifact_codec.zig");
const hbc_mod = @import("../hbc_adapter.zig");
const index_manager_mod = @import("catalog/index_manager.zig");
const index_repair_state = @import("derived/index_repair_state.zig");
const internal_keys = @import("../internal_keys.zig");
const lsm_backend_mod = @import("../lsm_backend/mod.zig");
const mapper = @import("document/mapper.zig");
const platform = @import("antfly_platform");
const public_table_schema = @import("../../schema/mod.zig");
const replay_stream_mod = @import("derived/replay_stream.zig");
const resource_manager_mod = @import("../resource_manager.zig");
const schema_mod = @import("../schema.zig");
const std = @import("std");
const storage_engine = @import("../engine.zig");
const types = @import("types.zig");
const vector_mod = @import("antfly_vector").vector;
const vectorindex_mod = @import("antfly_vectorindex");
const Allocator = db_zig.Allocator;
const AsyncContext = db_zig.AsyncContext;
const CountingDenseEmbedder = db_zig.CountingDenseEmbedder;
const DerivedCoverageCounters = db_zig.DerivedCoverageCounters;
const ProfiledDenseSearchWithCapturedRequestResult = db_zig.ProfiledDenseSearchWithCapturedRequestResult;
const PublishedDenseCatalogLookupTestHook = db_zig.PublishedDenseCatalogLookupTestHook;
const QueryVisibilityEvent = db_zig.QueryVisibilityEvent;
const ReplayProgress = db_zig.ReplayProgress;
const ReplayProgressHook = db_zig.ReplayProgressHook;
const SearchWithDenseProfileResult = db_zig.SearchWithDenseProfileResult;
const asyncContextHasActiveDenseBulkWork = db_zig.asyncContextHasActiveDenseBulkWork;
const beginExternalDenseBulkSessionTrackedWait = db_zig.beginExternalDenseBulkSessionTrackedWait;
const benchMetricsEnabled = db_zig.benchMetricsEnabled;
const benchQueryProfileEnabled = db_zig.benchQueryProfileEnabled;
const cacheBlockHitsForBench = db_zig.cacheBlockHitsForBench;
const clearTargetAdvanceMaintenanceDebt = db_zig.clearTargetAdvanceMaintenanceDebt;
const currentTimeNs = db_zig.currentTimeNs;
const denseCoverageMatchesTarget = db_zig.denseCoverageMatchesTarget;
const denseIndexIsArtifactBacked = db_zig.denseIndexIsArtifactBacked;
const densePostingIdleMaxBoundaryReassignmentsPerIndex = db_zig.densePostingIdleMaxBoundaryReassignmentsPerIndex;
const densePostingIdleMaxLayoutChangesPerIndex = db_zig.densePostingIdleMaxLayoutChangesPerIndex;
const densePostingIdleMaxPostingsPerIndex = db_zig.densePostingIdleMaxPostingsPerIndex;
const densePublicationTargetUsesArtifactCounter = db_zig.densePublicationTargetUsesArtifactCounter;
const denseTargetCountForIndexContext = db_zig.denseTargetCountForIndexContext;
const denseTargetCountForIndexSnapshot = db_zig.denseTargetCountForIndexSnapshot;
const denseTestVectorId = db_zig.denseTestVectorId;
const drainClaimedDenseProjectionFinalizations = db_zig.drainClaimedDenseProjectionFinalizations;
const elapsedSince = db_zig.elapsedSince;
const expectDenseEmbeddingArtifactValue = db_zig.expectDenseEmbeddingArtifactValue;
const expectedChunkArtifactPublicIdAlloc = db_zig.expectedChunkArtifactPublicIdAlloc;
const expectedChunkEmbeddingArtifactKeyAlloc = db_zig.expectedChunkEmbeddingArtifactKeyAlloc;
const expectedDocumentEmbeddingArtifactKeyAlloc = db_zig.expectedDocumentEmbeddingArtifactKeyAlloc;
const externalizeSearchResultArtifactIds = db_zig.externalizeSearchResultArtifactIds;
const finalizeCoveredDenseProjectionCheckpoint = db_zig.finalizeCoveredDenseProjectionCheckpoint;
const finalizeCoveredDenseProjectionCheckpointsIfIdle = db_zig.finalizeCoveredDenseProjectionCheckpointsIfIdle;
const finishDenseProjectionFinalization = db_zig.finishDenseProjectionFinalization;
const finishExternalDenseBulkSessionTracked = db_zig.finishExternalDenseBulkSessionTracked;
const finishExternalDenseBulkSessionTrackedAndFinalize = db_zig.finishExternalDenseBulkSessionTrackedAndFinalize;
const finishExternalDenseBulkSessionTrackedBestEffort = db_zig.finishExternalDenseBulkSessionTrackedBestEffort;
const flushDeferredExternalBulkExecutorNotification = db_zig.flushDeferredExternalBulkExecutorNotification;
const flushDeferredExternalBulkExecutorNotificationOrTarget = db_zig.flushDeferredExternalBulkExecutorNotificationOrTarget;
const lockApply = db_zig.lockApply;
const lockApplyShared = db_zig.lockApplyShared;
const lockAtomicWithBackoffProfiled = db_zig.lockAtomicWithBackoffProfiled;
const monotonicTimeNs = db_zig.monotonicTimeNs;
const no_background_test_open = db_zig.no_background_test_open;
const no_replay_no_background_test_open = db_zig.no_replay_no_background_test_open;
const normalizedBenchSearchEffort = db_zig.normalizedBenchSearchEffort;
const observeSearchFailureMetric = db_zig.observeSearchFailureMetric;
const openLegacyDenseTestDB = db_zig.openLegacyDenseTestDB;
const openModeRequiresReadOnlyBackends = db_zig.openModeRequiresReadOnlyBackends;
const platform_time = db_zig.platform_time;
const printDenseStreamingQualificationDiagnostics = db_zig.printDenseStreamingQualificationDiagnostics;
const process_memory_mod = db_zig.process_memory_mod;
const profileBenchTestsEnabled = db_zig.profileBenchTestsEnabled;
const projectOwnedStoredBytesForSearch = db_zig.projectOwnedStoredBytesForSearch;
const putDenseEmbeddingArtifactForTest = db_zig.putDenseEmbeddingArtifactForTest;
const putDenseEmbeddingArtifactWithCounterForTest = db_zig.putDenseEmbeddingArtifactWithCounterForTest;
const readEnvU64 = db_zig.readEnvU64;
const readEnvUsize = db_zig.readEnvUsize;
const rebuildDenseIndexForTargetCoverageContext = db_zig.rebuildDenseIndexForTargetCoverageContext;
const repair_completion_test_options = db_zig.repair_completion_test_options;
const resetDenseIndexForArtifactRebuildForTest = db_zig.resetDenseIndexForArtifactRebuildForTest;
const resolveBenchSearchEpsilon = db_zig.resolveBenchSearchEpsilon;
const resolveBenchSearchWidth = db_zig.resolveBenchSearchWidth;
const resolveMultiSourceDenseHitKeyAlloc = db_zig.resolveMultiSourceDenseHitKeyAlloc;
const sleepNs = db_zig.sleepNs;
const slow_test_wait_attempts = db_zig.slow_test_wait_attempts;
const testDenseSourceHashReuse = db_zig.testDenseSourceHashReuse;
const test_support = db_zig.test_support;
const threadedIo = db_zig.threadedIo;
const tryClaimDenseProjectionFinalizationLocked = db_zig.tryClaimDenseProjectionFinalizationLocked;
const ttlDurationNs = db_zig.ttlDurationNs;
const waitForAppliedSequenceAdvance = db_zig.waitForAppliedSequenceAdvance;
const waitForDenseIndexResultsWithAttempts = db_zig.waitForDenseIndexResultsWithAttempts;
const waitForDerivedReplayTarget = db_zig.waitForDerivedReplayTarget;
const waitForSearchResult = db_zig.waitForSearchResult;
const writeRawProjectionCheckpointSidecarForTest = db_zig.writeRawProjectionCheckpointSidecarForTest;
const AlgebraicDocFilterRequest = DB.AlgebraicDocFilterRequest;
const AutomaticDenseGenerationRepairClassification = DB.AutomaticDenseGenerationRepairClassification;
const AutomaticDenseGenerationRepairIntentOutcome = DB.AutomaticDenseGenerationRepairIntentOutcome;
const ChunkBoolFilterLowering = DB.ChunkBoolFilterLowering;
const DenseArtifactCounterBootstrapSnapshot = DB.DenseArtifactCounterBootstrapSnapshot;
const DenseArtifactRebuildPlan = DB.DenseArtifactRebuildPlan;
const DenseArtifactRebuildPreparation = DB.DenseArtifactRebuildPreparation;
const DenseArtifactRebuildTarget = DB.DenseArtifactRebuildTarget;
const DenseArtifactTargetCounts = DB.DenseArtifactTargetCounts;
const DenseSearchAccess = DB.DenseSearchAccess;
const DenseServingCounts = DB.DenseServingCounts;
const DenseSnapshotExecutor = DB.DenseSnapshotExecutor;
const IndexAdmissionDisposition = DB.IndexAdmissionDisposition;
const NativePublicationResult = DB.NativePublicationResult;
const OperatorDenseGenerationValidation = DB.OperatorDenseGenerationValidation;
const PostingRefreshProgress = DB.PostingRefreshProgress;
const RestoreDenseArtifactCompletion = DB.RestoreDenseArtifactCompletion;
const RestoreDenseArtifactCoverageRepair = DB.RestoreDenseArtifactCoverageRepair;
const allDocsVisibleFastCallback = DB.allDocsVisibleFastCallback;
const denseArtifactCounterBootstrapKeyAlloc = DB.denseArtifactCounterBootstrapKeyAlloc;
const denseArtifactTargetCounterKeyAlloc = DB.denseArtifactTargetCounterKeyAlloc;
const dense_artifact_counter_bootstrap_encoded_len = DB.dense_artifact_counter_bootstrap_encoded_len;
const dense_counter_bootstrap_working_set_bytes = DB.dense_counter_bootstrap_working_set_bytes;
const directSingleVectorRequest = DB.directSingleVectorRequest;
const encodeDenseArtifactCounterBootstrap = DB.encodeDenseArtifactCounterBootstrap;
const hasNativeDocIdConstraints = DB.hasNativeDocIdConstraints;
const hbcSearchCallback = DB.hbcSearchCallback;
const hbcSearchProfiledCallback = DB.hbcSearchProfiledCallback;
const liftChunkBoolFilterClausesAlloc = DB.liftChunkBoolFilterClausesAlloc;
const liveFilterDocSetCallback = DB.liveFilterDocSetCallback;
const loadDenseArtifactCounterBootstrap = DB.loadDenseArtifactCounterBootstrap;
const loadDenseArtifactTargetCounter = DB.loadDenseArtifactTargetCounter;
const loadRequiredProjectedSearchDocumentCallback = DB.loadRequiredProjectedSearchDocumentCallback;
const lookupLiveDocOrdinalNoLockCallback = DB.lookupLiveDocOrdinalNoLockCallback;
const lookupLiveDocOrdinalsNoLockCallback = DB.lookupLiveDocOrdinalsNoLockCallback;
const nonVisibleDocSetCallback = DB.nonVisibleDocSetCallback;
const notifyQueryVisibilityHook = DB.notifyQueryVisibilityHook;
const postprocessVectorSearchResultCallback = DB.postprocessVectorSearchResultCallback;
const published_dense_reader_mask = DB.published_dense_reader_mask;
const resolveDocIdsToDocSetCallback = DB.resolveDocIdsToDocSetCallback;
const resolveDocSetDocIdsCallback = DB.resolveDocSetDocIdsCallback;
const textIndexEntryCallback = DB.textIndexEntryCallback;

pub fn beginDenseAutoBulkIngestSession(self: *DB) !void {
    if (comptime storage_engine.enabled) return storage_engine.call(@src().fn_name, .{self});
    var write_fence = try self.beginReplicatedWrite(.gate);
    defer write_fence.release();
    try self.enforcePortableRuntimeGate();
    try beginExternalDenseBulkSessionTrackedWait(self.async_context, self.backend_runtime.io());
    errdefer finishExternalDenseBulkSessionTrackedBestEffort(self.async_context);
    try self.lockApplyForPortableRuntime();
    defer self.core.unlockApply();
    const resources = self.core.batchExecutionResources();
    try resources.index_manager.beginDenseBulkIngestSessions();
    self.bulk_ingest_session.begin();
}

pub fn finishDenseAutoBulkIngestSessionWithOptions(self: *DB, options: backend_types.BulkIngestFinishOptions) !void {
    if (comptime storage_engine.enabled) return storage_engine.call(@src().fn_name, .{ self, options });
    try finishDenseAutoBulkIngestSessionWithOptionsInternal(self, options, true);
}

pub fn rollDenseAutoBulkIngestSessionWithOptions(self: *DB, options: backend_types.BulkIngestFinishOptions) !void {
    if (comptime storage_engine.enabled) return storage_engine.call(@src().fn_name, .{ self, options });
    try finishDenseAutoBulkIngestSessionWithOptionsInternal(self, options, true);
    try self.beginDenseAutoBulkIngestSession();
}

pub fn finishDenseAutoBulkIngestSessionWithOptionsInternal(self: *DB, options: backend_types.BulkIngestFinishOptions, notify_executor: bool) !void {
    var write_fence = try self.beginReplicatedWrite(.gate);
    defer write_fence.release();
    var external_session_tracked = true;
    defer if (external_session_tracked) finishExternalDenseBulkSessionTrackedBestEffort(self.async_context);
    {
        try self.lockApplyForPortableRuntime();
        defer self.core.unlockApply();
        const resources = self.core.batchExecutionResources();
        resources.store.flushBufferedWritesWithOptions(options) catch |err| {
            self.bulk_ingest_session.finish();
            return err;
        };
        resources.index_manager.finishDenseBulkIngestSessionsWithOptions(options) catch |err| {
            self.bulk_ingest_session.finish();
            self.clearBulkIngestIdentityAllNewLocked();
            return err;
        };
        self.bulk_ingest_session.finish();
        self.clearBulkIngestIdentityAllNewLocked();
    }
    external_session_tracked = false;
    _ = try finishExternalDenseBulkSessionTrackedAndFinalize(self.async_context);
    if (notify_executor) {
        flushDeferredExternalBulkExecutorNotificationOrTarget(
            self.async_context,
            self.executor,
            self.core.nextDerivedSequence(),
        );
    }
    notifyQueryVisibilityHook(self.async_context, .publish);
}

pub fn abortDenseAutoBulkIngestSession(self: *DB) void {
    var replication_mutation = self.acquireReplicationMutationShared();
    defer if (replication_mutation) |*lease| lease.release();
    lockApply(self);
    {
        defer self.core.unlockApply();
        const resources = self.core.batchExecutionResources();
        self.bulk_ingest_session.finish();
        resources.index_manager.abortDenseBulkIngestSessions();
    }
    finishExternalDenseBulkSessionTrackedBestEffort(self.async_context);
    flushDeferredExternalBulkExecutorNotification(self.async_context, self.executor);
}

pub fn hasActiveDenseBulkWork(self: *const DB) bool {
    if (comptime storage_engine.enabled) return storage_engine.call(@src().fn_name, .{self}) catch @panic("incompatible storage owner");
    return asyncContextHasActiveDenseBulkWork(self.async_context);
}

/// Bounded proof used by structural reconciliation after a managed dense
/// admission has retired its outbox/repair marker. A complete generation
/// must not be force-reprocessed from stored documents: deterministic
/// chunk artifact identities are already represented by the current
/// snapshot, and replaying their historical replacement stream over that
/// snapshot can remove the final values.
pub fn completedManagedDenseGenerationIsServiceable(
    self: *DB,
    alloc: Allocator,
    index_name: []const u8,
) !bool {
    if (comptime storage_engine.enabled) return storage_engine.call(@src().fn_name, .{ self, alloc, index_name });
    const cfg = self.core.index_manager.get(index_name) orelse return false;
    if (cfg.kind != .dense_vector) return false;
    return try self.managedGenerationIsServiceableAtLeast(
        alloc,
        index_name,
        types.indexConfigHash(cfg.*),
        0,
    );
}

pub fn classifyCurrentDenseGenerationRepair(
    self: *DB,
    alloc: Allocator,
    cfg: types.IndexConfig,
) !?AutomaticDenseGenerationRepairClassification {
    if (cfg.kind != .dense_vector) return null;
    const entry = self.core.index_manager.denseIndex(cfg.name) orelse return null;
    const physical_migration_required =
        try self.core.index_manager.denseNativePhysicalMigrationRequired(cfg.name);
    const checkpoint = try self.core.loadProjectionCheckpoint(alloc, cfg.name);
    if (checkpoint.config_hash != types.indexConfigHash(cfg)) return .{
        .trigger = .projection_generation_invalid,
        .last_error = "dense_projection_config_mismatch",
    };
    if (checkpoint.status == .repair_required) return .{
        .trigger = .projection_generation_invalid,
        .last_error = "dense_projection_checkpoint_repair_required",
    };
    if (!try index_manager_mod.denseConfigRequiresArtifactCoverage(alloc, cfg)) return if (physical_migration_required) .{
        .trigger = .storage_format_migration,
        .last_error = "dense_native_v2_migration",
    } else null;
    const expected = (try loadDenseArtifactTargetCounter(alloc, self.core.store, cfg.name)) orelse return .{
        .trigger = .artifact_counter_missing,
        .last_error = "dense_artifact_counter_missing",
    };
    if (entry.index.stats().active_count > expected) {
        const target_sequence = try self.probeDerivedReplayTargetSequence(
            alloc,
            self.core.replaySource(),
            .{ .name = cfg.name, .kind = .dense_vector },
            checkpoint.applied_sequence,
        );
        if (checkpoint.applied_sequence >= target_sequence) return .{
            .trigger = .artifact_coverage_mismatch,
            .last_error = "dense_artifact_coverage_surplus",
        };
    }
    return if (physical_migration_required) .{
        .trigger = .storage_format_migration,
        .last_error = "dense_native_v2_migration",
    } else null;
}

/// Performs the potentially linear structural proof only on the admitted
/// BackendRuntime repair worker, never on the HTTP/operator request path.
/// Cheap generation, replay, and cardinality checks run first so routine
/// lag avoids a tree walk. The validation read transaction is stable while
/// foreground writes continue through the ordinary derived stream.
pub fn validateOperatorDenseGenerationForOnlineRebuild(
    self: *DB,
    alloc: Allocator,
    cfg: types.IndexConfig,
    options: types.ArtifactRepairRunOptions,
) !OperatorDenseGenerationValidation {
    const checkpoint = try self.core.loadProjectionCheckpoint(alloc, cfg.name);
    if (checkpoint.config_hash != types.indexConfigHash(cfg)) return .{ .fail_closed = .{
        .trigger = .projection_generation_invalid,
        .last_error = "dense_projection_config_mismatch",
    } };
    if (checkpoint.status != .clean) return .{ .fail_closed = .{
        .trigger = .projection_generation_invalid,
        .last_error = "dense_projection_checkpoint_not_clean",
    } };
    const target_sequence = try self.probeDerivedReplayTargetSequence(
        alloc,
        self.core.replaySource(),
        .{ .name = cfg.name, .kind = .dense_vector },
        checkpoint.applied_sequence,
    );
    if (checkpoint.applied_sequence < target_sequence) return .replay_pending;

    // Pin both the catalog entry and its generation while inspecting the
    // HBC metadata/tree. The read transaction stabilizes backend pages;
    // this guard additionally prevents an apply or delete/recreate from
    // changing the in-memory generation around that snapshot.
    var apply_guard = self.core.index_manager.lockManagedIndexApply(.{
        .name = cfg.name,
        .kind = .dense_vector,
    }) catch return .{ .fail_closed = .{
        .trigger = .projection_generation_invalid,
        .last_error = "dense_projection_generation_unavailable",
    } };
    defer apply_guard.unlock();

    const entry = self.core.index_manager.denseIndex(cfg.name) orelse return .{ .fail_closed = .{
        .trigger = .projection_generation_invalid,
        .last_error = "dense_projection_generation_unavailable",
    } };

    if (try index_manager_mod.denseConfigRequiresArtifactCoverage(alloc, cfg)) {
        const expected = (try loadDenseArtifactTargetCounter(alloc, self.core.store, cfg.name)) orelse
            return .{ .fail_closed = .{
                .trigger = .artifact_counter_missing,
                .last_error = "dense_artifact_counter_missing",
            } };
        if (!denseCoverageMatchesTarget(entry.index.stats().active_count, expected)) {
            return .{ .fail_closed = .{
                .trigger = .artifact_coverage_mismatch,
                .last_error = "dense_artifact_coverage_mismatch",
            } };
        }
    }

    const cancel_ctx = if (options.cancel_check) |check| check.ptr else null;
    const cancel_fn = if (options.cancel_check) |check| check.is_requested else null;
    entry.index.validateStoredStructureWithCancellation(alloc, cancel_ctx, cancel_fn) catch |err| switch (err) {
        error.NotFound, error.FileNotFound, error.Corrupted => {
            entry.index.quarantineExperimentalPostingGeneration();
            return .{ .fail_closed = .{
                .trigger = .projection_generation_invalid,
                .last_error = "dense_projection_structure_invalid",
            } };
        },
        else => return err,
    };
    return .online_safe;
}

/// Replay has already identified the affected dense incarnation and
/// proved that ordinary catch-up cannot apply its next journal record.
/// Persist that stronger classification before returning to the repair
/// scheduler; otherwise an older operator-validation intent waits for the
/// same impossible replay forever.
pub fn markDenseReplayArtifactRepairRequired(
    self: *DB,
    alloc: Allocator,
    index_name: []const u8,
) !bool {
    if (comptime storage_engine.enabled) return storage_engine.call(@src().fn_name, .{ self, alloc, index_name });
    return try markDenseReplayRepairRequired(self, 
        alloc,
        index_name,
        .projection_generation_invalid,
        "dense_replay_artifact_missing",
    );
}

pub fn markDenseReplayArtifactCoverageRepairRequired(
    self: *DB,
    alloc: Allocator,
    index_name: []const u8,
) !void {
    _ = try markDenseReplayRepairRequired(self, 
        alloc,
        index_name,
        .artifact_coverage_mismatch,
        "dense_replay_artifact_unreadable",
    );
}

pub fn markDenseReplayRepairRequired(
    self: *DB,
    alloc: Allocator,
    index_name: []const u8,
    blocking_trigger: index_repair_state.Trigger,
    reason: []const u8,
) !bool {
    // Browser databases have no durable repair sidecar to publish.
    if (builtin.os.tag == .freestanding) return false;
    const cfg = self.core.index_manager.get(index_name) orelse return error.NotFound;
    if (cfg.kind != .dense_vector) return error.InvalidArgument;
    if (try self.indexRepairIdForIndex(alloc, index_name)) |repair_id| {
        var entry = try self.loadIndexRepairEntryById(alloc, repair_id);
        defer entry.deinit(alloc);
        // Replaying the same unavailable artifact must not revoke the
        // serviceability proof that created this shadow-repair intent.
        // Only an unrelated pre-existing intent is promoted to the
        // conservative blocking classification below.
        if (entry.intent.trigger == .replay_artifact_unavailable) return true;
        // Source replay for a newly admitted managed index can reach its
        // canonical consumer before the producer publishes the referenced
        // artifact. That is an expected dependency edge owned by the same
        // durable admission job, not evidence that a serving generation
        // is corrupt. Preserve the job class so the worker can retry via
        // ReplayDocumentNotVisible without manufacturing repair debt.
        if (entry.intent.work_class == .initial_build and
            entry.intent.phase != .terminal)
        {
            return false;
        }
        // A resumable shadow already owns recovery. Do not replace its
        // phase or candidate merely because the stale active generation
        // still cannot consume replay.
        const classifiable_phase = entry.intent.phase == .detected or entry.intent.phase == .preflight;
        if (!classifiable_phase or entry.intent.candidate_relative_path != null) return true;
        try self.updateIndexRepairIntent(alloc, repair_id, .{
            // Preserve the pre-existing intent's fail-closed contract.
            // Only debt created from this replay observation has proof
            // that the retained physical generation is serviceable.
            .trigger = blocking_trigger,
            .next_retry_at_ms = 0,
            .last_error = reason,
            .replace_last_error = true,
        });
        return true;
    }
    _ = try self.createGenerationRepairIntent(
        alloc,
        cfg.*,
        .replay_artifact_unavailable,
        0,
        0,
        reason,
    );
    return true;
}

pub fn automaticDenseGenerationRepairTriggerPriority(trigger: index_repair_state.Trigger) u8 {
    return switch (trigger) {
        .operator_generation_rebuild, .operator_generation_validation, .storage_format_migration, .artifact_baseline_adoption => 0,
        .artifact_coverage_mismatch, .replay_artifact_unavailable => 1,
        .artifact_counter_missing => 2,
        .incomplete_bulk_publish, .root_generation_rebuild, .projection_generation_invalid, .catalog_admission => 3,
    };
}

pub fn ensureAutomaticDenseGenerationRepairIntent(
    self: *DB,
    alloc: Allocator,
    cfg: types.IndexConfig,
    trigger: index_repair_state.Trigger,
    last_error: []const u8,
) !u128 {
    return (try ensureAutomaticDenseGenerationRepairIntentOutcome(self, 
        alloc,
        cfg,
        trigger,
        last_error,
    )).repair_id;
}

pub fn ensureAutomaticDenseGenerationRepairIntentOutcome(
    self: *DB,
    alloc: Allocator,
    cfg: types.IndexConfig,
    trigger: index_repair_state.Trigger,
    last_error: []const u8,
) !AutomaticDenseGenerationRepairIntentOutcome {
    std.debug.assert(trigger == .artifact_coverage_mismatch or
        trigger == .artifact_counter_missing or
        trigger == .projection_generation_invalid or
        trigger == .storage_format_migration);

    // Correctness repairs close the query gate before persistence. A
    // physical-format migration is different: v1 remains the proven
    // serving generation while its v2 shadow is built and caught up.
    const close_query_gate = trigger != .storage_format_migration;
    const was_unavailable = self.core.index_manager.repairUnavailable(cfg.name);
    if (close_query_gate) try self.core.index_manager.markUnscopedRepairUnavailable(cfg.name);

    const existing_repair_id = try self.indexRepairIdForIndex(alloc, cfg.name);
    const repair_id = existing_repair_id orelse try self.createGenerationRepairIntent(
        alloc,
        cfg,
        trigger,
        0,
        0,
        last_error,
    );
    if (existing_repair_id == null) {
        std.log.warn("dense generation repair intent created index={s} trigger={s} reason={s}", .{
            cfg.name,
            @tagName(trigger),
            last_error,
        });
    }

    // Creation races with an operator request are harmless: re-read the
    // winning intent and promote its safety classification when needed.
    var attempts: usize = 0;
    while (attempts < 4) : (attempts += 1) {
        var entry = try self.loadIndexRepairEntryById(alloc, repair_id);
        defer entry.deinit(alloc);
        if (automaticDenseGenerationRepairTriggerPriority(trigger) <=
            automaticDenseGenerationRepairTriggerPriority(entry.intent.trigger)) return .{
            .repair_id = repair_id,
            .made_progress = existing_repair_id == null or (close_query_gate and !was_unavailable),
        };
        self.updateIndexRepairIntent(alloc, repair_id, .{
            .trigger = trigger,
            .replace_last_error = true,
            .last_error = last_error,
        }) catch |err| switch (err) {
            error.RepairTransitionConflict => continue,
            else => return err,
        };
        return .{ .repair_id = repair_id, .made_progress = true };
    }
    return error.RepairTransitionConflict;
}

pub fn beginDenseArtifactCounterBootstrapSnapshot(
    self: *DB,
    alloc: Allocator,
    index_name: []const u8,
    repair_id: u128,
) !DenseArtifactCounterBootstrapSnapshot {
    try self.lockApplyForPortableRuntime();
    defer self.core.unlockApply();

    if (try loadDenseArtifactTargetCounter(alloc, self.core.store, index_name) != null) {
        return error.DenseArtifactCounterAlreadyInitialized;
    }
    const attempt_id = try index_repair_state.newRepairId(alloc);
    const marker_key = try denseArtifactCounterBootstrapKeyAlloc(alloc, index_name);
    defer alloc.free(marker_key);
    var marker_value: [dense_artifact_counter_bootstrap_encoded_len]u8 = undefined;
    encodeDenseArtifactCounterBootstrap(.{
        .repair_id = repair_id,
        .attempt_id = attempt_id,
    }, &marker_value);
    try self.core.store.putBatch(&.{.{ .key = marker_key, .value = &marker_value }}, &.{});

    // The apply lock orders marker publication before this snapshot and
    // before every later artifact mutation. Once it is released, those
    // mutations update the marker's signed delta in their own atomic store
    // batch while this stable snapshot is counted without blocking writes.
    return .{
        .txn = try self.core.store.beginReadTxn(),
        .attempt_id = attempt_id,
    };
}

pub fn finishDenseArtifactCounterBootstrap(
    self: *DB,
    alloc: Allocator,
    index_name: []const u8,
    repair_id: u128,
    attempt_id: u128,
    snapshot_count: u64,
) !void {
    try self.lockApplyForPortableRuntime();
    defer self.core.unlockApply();

    if (try loadDenseArtifactTargetCounter(alloc, self.core.store, index_name) != null) return;
    const marker = (try loadDenseArtifactCounterBootstrap(alloc, self.core.store, index_name)) orelse
        return error.DenseArtifactCounterBootstrapMissing;
    if (marker.repair_id != repair_id or marker.attempt_id != attempt_id) {
        return error.StaleDenseArtifactCounterBootstrap;
    }

    const final_count_i128 = @as(i128, @intCast(snapshot_count)) + @as(i128, marker.delta);
    if (final_count_i128 < 0 or final_count_i128 > std.math.maxInt(u64)) {
        return error.InvalidDenseArtifactCounterBootstrapDelta;
    }
    const counter_key = try denseArtifactTargetCounterKeyAlloc(alloc, index_name);
    defer alloc.free(counter_key);
    const marker_key = try denseArtifactCounterBootstrapKeyAlloc(alloc, index_name);
    defer alloc.free(marker_key);
    var counter_value: [8]u8 = undefined;
    std.mem.writeInt(u64, &counter_value, @intCast(final_count_i128), .little);
    try self.core.store.putBatch(
        &.{.{ .key = counter_key, .value = &counter_value }},
        &.{marker_key},
    );
}

pub fn ensureDenseArtifactTargetCounterForRepair(
    self: *DB,
    alloc: Allocator,
    cfg: types.IndexConfig,
    repair_id: u128,
    cancel_check: ?types.RepairCancelCheck,
) !void {
    if (!try index_manager_mod.denseConfigRequiresArtifactCoverage(alloc, cfg)) return;
    if (try loadDenseArtifactTargetCounter(alloc, self.core.store, cfg.name) != null) return;

    // Counter bootstrap is a background repair phase in its own right. It
    // can hold a stable store snapshot for a full artifact scan, so admit
    // it before publishing the marker rather than doing unaccounted work
    // that a later shadow-build admission would reject.
    var bootstrap_admission: ?resource_manager_mod.Reservation = if (self.core.index_manager.resource_manager) |manager|
        try reserveDenseCounterBootstrapWorkingSet(manager)
    else
        null;
    defer if (bootstrap_admission) |*reservation| reservation.release();

    var bootstrap_snapshot = self.beginDenseArtifactCounterBootstrapSnapshot(alloc, cfg.name, repair_id) catch |err| switch (err) {
        error.DenseArtifactCounterAlreadyInitialized => return,
        else => return err,
    };
    defer bootstrap_snapshot.deinit();

    const snapshot_count = try self.countDenseArtifactsForConfigFromReadTxn(
        alloc,
        cfg,
        &bootstrap_snapshot.txn,
        cancel_check,
    );
    if (cancel_check) |check| if (check.requested()) return error.Canceled;
    try self.finishDenseArtifactCounterBootstrap(
        alloc,
        cfg.name,
        repair_id,
        bootstrap_snapshot.attempt_id,
        snapshot_count,
    );
}

pub fn artifactBackedDenseRepairEstimate(self: *DB, alloc: Allocator, cfg: types.IndexConfig) !u64 {
    if (!try index_manager_mod.denseConfigRequiresArtifactCoverage(alloc, cfg)) return 0;
    const target_count = (try loadDenseArtifactTargetCounter(alloc, self.core.store, cfg.name)) orelse return 0;
    const dims = try index_manager_mod.denseConfigDimensions(alloc, cfg);
    // Candidate storage contains the source vector, HBC node/posting
    // state, quantized payloads, keys, and LSM metadata. Two times the raw
    // vector payload plus a per-vector structural allowance is a
    // deliberately conservative preflight; live boundary checks below
    // remain authoritative as bytes materialize.
    const per_vector = std.math.add(u64, @as(u64, dims) *| @sizeOf(f32), 512) catch std.math.maxInt(u64);
    const logical = std.math.mul(u64, target_count, per_vector) catch std.math.maxInt(u64);
    return std.math.mul(u64, logical, 2) catch std.math.maxInt(u64);
}

pub fn reserveDenseCounterBootstrapWorkingSet(
    manager: *resource_manager_mod.ResourceManager,
) !resource_manager_mod.Reservation {
    const slice_stats = manager.sliceStats(.dense_repair_working_set);
    const hard_available = if (slice_stats.hard_limit_bytes == 0)
        std.math.maxInt(u64)
    else
        slice_stats.hard_limit_bytes -| slice_stats.used_bytes;
    // Let ResourceManager perform an authoritative hard rejection so the
    // shared rejection metric remains accurate under pressure and races.
    if (hard_available < dense_counter_bootstrap_working_set_bytes) {
        return manager.reserve(.dense_repair_working_set, dense_counter_bootstrap_working_set_bytes) catch
            return error.RepairResourceUnavailable;
    }
    const soft_available = if (slice_stats.soft_limit_bytes == 0)
        hard_available
    else
        slice_stats.soft_limit_bytes -| slice_stats.used_bytes;
    if (soft_available < dense_counter_bootstrap_working_set_bytes) {
        return error.RepairResourceUnavailable;
    }
    return manager.reserve(.dense_repair_working_set, dense_counter_bootstrap_working_set_bytes) catch
        return error.RepairResourceUnavailable;
}

pub fn denseCoverageRegressionRepairRequired(self: *DB, alloc: Allocator, index_name: []const u8) !bool {
    const entry = self.core.index_manager.denseIndex(index_name) orelse return false;
    if (!denseIndexIsArtifactBacked(entry)) return false;
    const expected = (try loadDenseArtifactTargetCounter(alloc, self.core.store, index_name)) orelse
        return true;
    if (denseCoverageMatchesTarget(entry.index.stats().active_count, expected)) return false;

    // Source counters advance atomically with primary writes, before the
    // asynchronous projection applies the corresponding replay sequence.
    // A cardinality difference is repair debt only after replay converges;
    // otherwise every healthy asynchronous write would be misclassified.
    const checkpoint = try self.core.loadProjectionCheckpoint(alloc, index_name);
    if (checkpoint.status == .rebuilding) return false;
    const target_sequence = try self.probeDerivedReplayTargetSequence(
        alloc,
        self.core.replaySource(),
        .{ .name = index_name, .kind = .dense_vector },
        checkpoint.applied_sequence,
    );
    return checkpoint.applied_sequence >= target_sequence;
}

pub fn clearDenseHbcCaches(self: *DB) void {
    self.core.index_manager.clearDenseHbcCaches();
}

pub fn rebuildDenseIndexesForTargetCoverage(self: *DB, alloc: Allocator) !usize {
    if (comptime storage_engine.enabled) return storage_engine.call(@src().fn_name, .{ self, alloc });
    var snapshot_replay = try self.acquireSnapshotReplayMutation();
    defer snapshot_replay.release();
    var names = std.ArrayListUnmanaged([]u8).empty;
    defer {
        for (names.items) |name| alloc.free(name);
        names.deinit(alloc);
    }

    for (self.core.index_manager.dense_indexes.items) |*entry| {
        try names.append(alloc, try alloc.dupe(u8, entry.config.name));
    }

    var rebuilt: usize = 0;
    for (names.items) |name| {
        rebuilt += try rebuildDenseIndexForTargetCoverageContext(self.async_context, name, 2048);
    }
    return rebuilt;
}

pub fn runDensePostingMaintenanceForIdle(self: *DB) !usize {
    var snapshot_replay = try self.acquireSnapshotReplayMutation();
    defer snapshot_replay.release();
    try self.lockApplyForPortableRuntime();
    defer self.core.unlockApply();
    return try self.core.index_manager.runDensePostingMaintenance(.{
        .max_postings_per_index = densePostingIdleMaxPostingsPerIndex(),
        .max_layout_changes_per_index = densePostingIdleMaxLayoutChangesPerIndex(),
        .max_boundary_reassignments_per_index = densePostingIdleMaxBoundaryReassignmentsPerIndex(),
    });
}

pub fn refreshDensePostingPayloadPageBestEffort(self: *DB) !PostingRefreshProgress {
    if (comptime storage_engine.enabled) return storage_engine.call(@src().fn_name, .{self});
    // Clean scanning needs only the catalog lifetime and per-index
    // mutation owner. It must not close query admission or queue behind
    // a sustained stream of readers simply to discover an empty backlog.
    const scanned_page = blk: {
        var lease = self.tryAcquireIndexCatalogReadLease() orelse return .{ .pending = true };
        defer lease.release();
        break :blk try self.core.index_manager.refreshDensePostingPayloadPage(false);
    };
    if (!scanned_page.needs_write) return scanned_page;
    // Capture owns leaf/ancestor state and pins immutable input generations.
    // Native input loading and computation need no DB/catalog/index lock.
    // Unsupported input formats retain the coherent copied-input path.
    const capture = blk: {
        // Keep primary/source fallback reads at one revision while copying
        // inputs. This shared lease admits queries; no writer fence is
        // requested until the detached computation has completed.
        if (!self.core.tryLockApplyShared()) return .{ .pending = true, .scanned = scanned_page.scanned };
        defer self.core.unlockApplyShared();
        var lease = self.tryAcquireIndexCatalogReadLease() orelse return .{ .pending = true };
        defer lease.release();
        break :blk self.core.index_manager.captureDensePostingRefreshWithOutcome() catch |err| switch (err) {
            error.ResourceBudgetExceeded => return .{ .pending = true, .scanned = scanned_page.scanned },
            else => return err,
        };
    };
    const prepared: ?*hbc_mod.HBCIndex.PreparedPostingRefresh = switch (capture) {
        .prepared => |value| value,
        .deferred => return .{ .pending = true, .scanned = scanned_page.scanned },
        .synchronous_required => null,
    };
    defer if (prepared) |value| value.deinit();
    const preparation_started = monotonicTimeNs();
    defer if (prepared) |value| {
        if (benchMetricsEnabled()) std.log.info("dense posting prepared outcome leaf={} detached={} published={} elapsed_ns={} capture_ns={} input_ns={} metadata_ns={} read_ns={} input_rows={} binding_hits={}", .{ value.leaf_id, value.detached, value.published, elapsedSince(preparation_started), value.capture_ns, value.input_ns, value.metadata_ns, value.read_ns, value.input_rows, value.binding_hits });
    };
    if (prepared) |value| {
        const started = monotonicTimeNs();
        value.build() catch |err| switch (err) {
            error.PreparedInputUnavailable, error.ResourceBudgetExceeded => return .{ .pending = true, .scanned = scanned_page.scanned },
            else => return err,
        };
        value.build_ns = (monotonicTimeNs() -| started) -| value.input_ns;
    }

    // Upgrade only after finding actual debt, releasing the catalog lease
    // first to avoid a cycle with a structural writer draining that lease.
    // Writer-preferring admission makes progress under steady queries;
    // cancellation reopens admission if existing readers take too long.
    const admission_started = monotonicTimeNs();
    if (!self.core.tryLockApplyExclusive()) {
        const io = self.backend_runtime.io() orelse return .{ .pending = true, .scanned = scanned_page.scanned };
        self.core.apply_mutex.lockExclusiveDeadlineIo(io, .fromNow(io, .{
            .raw = .fromMilliseconds(50),
            .clock = .awake,
        })) catch |err| switch (err) {
            error.Timeout => return .{ .pending = true, .scanned = scanned_page.scanned },
            else => return err,
        };
    }
    defer self.core.unlockApply();
    if (prepared) |value| value.admission_ns = monotonicTimeNs() -| admission_started;
    // Recheck the current index state under the exclusive fence: a write
    // or another refresh may have run between scanning and admission.
    var repaired = if (prepared) |value|
        try self.core.index_manager.publishPreparedDensePostingRefresh(value)
    else
        try self.core.index_manager.refreshDensePostingPayloadPage(true);
    repaired.scanned += scanned_page.scanned;
    return repaired;
}

pub fn publishCompletedDensePostingCheckpoints(self: *DB) !NativePublicationResult {
    if (comptime storage_engine.enabled) return storage_engine.call(@src().fn_name, .{self});
    // Native backup closes and drains this same catalog barrier before
    // selecting/pinning generated files. It excludes publication until
    // hardlinks own the captured generation, without serializing ordinary
    // primary commits behind the checkpoint handoff.
    var catalog_lease = self.tryAcquireIndexCatalogReadLease() orelse return .{ .busy = true };
    defer catalog_lease.release();
    const result = try self.core.index_manager.publishCompletedDensePostingCheckpoints();
    return .{ .published = result.published, .deferred = result.deferred };
}

/// Cooperatively drain the posting dependency chain at a caller-proven
/// quiet boundary. Individual repair pages remain bounded by the runtime
/// policy and the drain stops when foreground dense work reappears.
pub fn drainDensePostingMaintenanceForIdle(self: *DB) !usize {
    lockApply(self);
    defer self.core.unlockApply();
    return try self.core.index_manager.drainDensePostingMaintenance(.{
        .max_postings_per_index = densePostingIdleMaxPostingsPerIndex(),
        .max_layout_changes_per_index = densePostingIdleMaxLayoutChangesPerIndex(),
        .max_boundary_reassignments_per_index = densePostingIdleMaxBoundaryReassignmentsPerIndex(),
    });
}

/// Retry readiness publication after all dense sessions have retired.
/// The last source callback can observe coverage counters just before
/// their final durable update and legitimately decline publication. The
/// recurring vector-maintenance lane must re-evaluate that lifecycle
/// proof; otherwise a finite load depends on unrelated future writes.
pub fn finalizeDenseProjectionLifecycleForIdle(self: *DB) !bool {
    if (comptime storage_engine.enabled) return storage_engine.call(@src().fn_name, .{self});
    // A final graph-only replay window may leave a coalesced dense no-op
    // watermark below the publication cut. Flush it when no dense session
    // is active; continuous ingestion retains its ordinary deferral.
    try self.flushAppliedSequences(false);
    // The recurring owner may construct files, but never while holding
    // the shared apply lease used by the small certification below.
    _ = try self.publishVectorBlockBasesOnline(.{});
    return try finalizeCoveredDenseProjectionCheckpointsIfIdle(self.async_context);
}

pub fn freeDenseArtifactRebuildWrites(alloc: Allocator, writes: *std.ArrayListUnmanaged(mapper.DenseEmbeddingWrite)) void {
    for (writes.items) |write| {
        alloc.free(write.index_name);
        alloc.free(write.doc_key);
        if (write.parent_doc_key) |parent_doc_key| alloc.free(@constCast(parent_doc_key));
        if (write.artifact_key) |artifact_key| alloc.free(artifact_key);
        if (write.vector.len > 0) alloc.free(write.vector);
    }
    writes.clearRetainingCapacity();
}

pub fn flushDenseArtifactRebuildChunk(
    self: *DB,
    alloc: Allocator,
    writes: *std.ArrayListUnmanaged(mapper.DenseEmbeddingWrite),
    mutated_indexes: *std.StringHashMapUnmanaged(void),
) !void {
    defer freeDenseArtifactRebuildWrites(alloc, writes);
    if (writes.items.len == 0) return;

    for (self.core.index_manager.dense_indexes.items) |entry| {
        var has_writes = false;
        for (writes.items) |write| {
            if (std.mem.eql(u8, write.index_name, entry.config.name)) {
                has_writes = true;
                break;
            }
        }
        if (!has_writes) continue;

        const posting_capture = try self.core.index_manager.beginDensePostingSidecarCaptureLeaseByNameWithOptions(entry.config.name, .{});
        var posting_capture_owned = if (posting_capture) |lease| lease.ownsLifecycle() else false;
        if (posting_capture == null and
            self.core.index_manager.densePostingSidecarCaptureRequiredByName(entry.config.name))
        {
            return error.PostingWalCaptureOwnershipConflict;
        }
        errdefer if (posting_capture_owned) if (posting_capture) |lease|
            self.core.index_manager.cancelDensePostingSidecarCaptureLeaseByName(entry.config.name, lease) catch {};
        try self.core.index_manager.applyDenseEmbeddingWritesByNameWithOptions(
            self.core.store,
            entry.config.name,
            writes.items,
            .{ .mode = .bulk_ingest },
        );
        if (posting_capture) |lease| if (posting_capture_owned) {
            // Repair chunks are independently recoverable. Keep their WAL
            // bounded at their existing capture boundary. Zero would regress
            // a reopened native generation; retaining the base does not
            // claim additional source coverage for an incomplete scan.
            try self.core.index_manager.finishDensePostingSidecarCaptureLeaseByName(entry.config.name, lease, lease.capture.base_coverage);
            posting_capture_owned = false;
        };
        try mutated_indexes.put(alloc, entry.config.name, {});
    }
}

pub fn denseIndexRebuildStatePathAlloc(self: *DB, alloc: Allocator, index_name: []const u8) ![]u8 {
    return try std.fmt.allocPrint(
        alloc,
        "{s}/indexes/{s}",
        .{ self.core.index_manager.base_path, index_name },
    );
}

pub fn loadDenseArtifactTargetCounterFromTxn(alloc: Allocator, txn: *docstore_mod.DocStore.Txn, index_name: []const u8) !?u64 {
    const key = try denseArtifactTargetCounterKeyAlloc(alloc, index_name);
    defer alloc.free(key);
    const raw = txn.get(key) catch |err| switch (err) {
        error.NotFound => return null,
        else => return err,
    };
    if (raw.len != 8) return error.InvalidDenseArtifactTargetCounter;
    return std.mem.readInt(u64, raw[0..8], .little);
}

pub fn initializeDenseArtifactTargetCounterForAdmission(
    self: *DB,
    cfg: types.IndexConfig,
    disposition: IndexAdmissionDisposition,
) !void {
    if (!try index_manager_mod.denseConfigRequiresArtifactCoverage(self.alloc, cfg)) return;

    // Every catalog add owns a fresh coverage generation. A managed
    // admission over a non-empty source can race writes that committed
    // artifacts after the catalog definition but before this resident
    // runtime was installed. Leave its counter deliberately absent: the
    // repair owner bootstraps the exact artifact count from a stable
    // snapshot while concurrent artifact mutations accumulate in the
    // bootstrap delta. Initializing it to zero would falsely certify that
    // those durable artifacts did not exist and let replay advance past
    // them without populating the new generation.
    // Table-owned sources can predate even an ordinary ANN admission,
    // including after its last consumer was dropped. Bootstrap them too.
    if (disposition == .managed_rebuild or try self.externalCoverageHasStoredArtifacts(cfg)) {
        try self.deleteDenseArtifactCounterMetadata(cfg.name);
        return;
    }

    // Empty-source and ordinary admissions have no pre-admission artifact
    // window to discover. Reset the name-scoped counter so debris from an
    // interrupted deletion cannot become coverage evidence for a
    // recreated generation.
    const key = try denseArtifactTargetCounterKeyAlloc(self.alloc, cfg.name);
    defer self.alloc.free(key);
    var value: [8]u8 = undefined;
    std.mem.writeInt(u64, &value, 0, .little);
    try self.core.store.putBatch(&.{.{ .key = key, .value = &value }}, &.{});
}

pub fn deleteDenseArtifactCounterMetadata(self: *DB, index_name: []const u8) !void {
    return try deleteDenseArtifactCounterMetadataContext(self.alloc, self.core.store, index_name);
}

pub fn deleteDenseArtifactCounterMetadataContext(alloc: Allocator, store: *docstore_mod.DocStore, index_name: []const u8) !void {
    const counter_key = try denseArtifactTargetCounterKeyAlloc(alloc, index_name);
    defer alloc.free(counter_key);
    const bootstrap_key = try denseArtifactCounterBootstrapKeyAlloc(alloc, index_name);
    defer alloc.free(bootstrap_key);
    try store.putBatch(&.{}, &.{ counter_key, bootstrap_key });
}

pub fn collectDenseArtifactTargetCountsFromCounters(
    self: *DB,
    alloc: Allocator,
    rebuild_targets: []const DenseArtifactRebuildTarget,
) !DenseArtifactTargetCounts {
    var counts: DenseArtifactTargetCounts = .{};
    errdefer counts.deinit(alloc);

    for (rebuild_targets) |target| {
        const entry = &self.core.index_manager.dense_indexes.items[target.dense_index_idx];
        // Startup planning and shadow activation must use the same
        // generation-fenced target after same-name recreation.
        const maybe_count = try denseTargetCountForIndexContext(self.async_context, entry.config.name);
        const count = maybe_count orelse 0;
        try counts.per_target_index.put(alloc, target.dense_index_idx, count);
        if (maybe_count != null) try counts.authoritative_targets.put(alloc, target.dense_index_idx, {});

        // Per-index counters drive coverage decisions below. Their sum is
        // only an upper bound when multiple indexes consume overlapping
        // artifact streams, so the final rebuild plan computes its exact
        // union after selecting the targets that actually need repair.
        counts.total_target_artifacts +|= count;
    }

    return counts;
}

pub fn prepareDenseArtifactRebuildPlan(
    self: *DB,
    plan: DenseArtifactRebuildPlan,
    dense_visibility_attempted: *bool,
) !DenseArtifactRebuildPreparation {
    var outcome: DenseArtifactRebuildPreparation = .{};
    for (plan.rebuild_state_cleanups) |dense_index_idx| {
        const entry = &self.core.index_manager.dense_indexes.items[dense_index_idx];
        const rebuild_root_path = try self.denseIndexRebuildStatePathAlloc(self.alloc, entry.config.name);
        defer self.alloc.free(rebuild_root_path);
        const rebuild_state = self.core.index_manager.rebuildState(.dense_vector, rebuild_root_path, entry.config);
        try rebuild_state.clearAllWithIo(self.core.index_manager.checkpointIo());
        outcome.made_progress = true;
    }
    for (plan.generation_repairs) |repair| {
        const entry = &self.core.index_manager.dense_indexes.items[repair.dense_index_idx];
        const repair_intent = ensureAutomaticDenseGenerationRepairIntentOutcome(self, 
            self.alloc,
            entry.config,
            repair.trigger,
            repair.last_error,
        ) catch |err| switch (err) {
            error.DurableIndexRepairStateUnavailable => {
                // Embedded/Lite runtimes intentionally have no durable
                // maintenance owner. They still use a shadow generation and
                // the same search/apply activation fences; only crash-resume
                // scheduling is omitted.
                var cfg = try types.IndexConfig.clone(self.alloc, entry.config);
                defer cfg.deinit(self.alloc);
                if (repair.trigger == .artifact_counter_missing) {
                    const bootstrap_id = try index_repair_state.newRepairId(self.alloc);
                    try self.ensureDenseArtifactTargetCounterForRepair(
                        self.alloc,
                        cfg,
                        bootstrap_id,
                        null,
                    );
                }
                // Shadow publication can complete before a later durable
                // bookkeeping failure escapes. Arm cache retirement before
                // entering the mutation boundary.
                dense_visibility_attempted.* = true;
                const rebuilt = try self.rebuildIndexWithShadowReplacement(self.alloc, cfg, .{}, null);
                if (rebuilt.yielded) return error.ShadowIndexCatchUpIncomplete;
                outcome.made_progress = true;
                outcome.dense_visibility_changed = true;
                clearTargetAdvanceMaintenanceDebt(self.async_context, entry.config.name);
                continue;
            },
            else => return err,
        };
        outcome.made_progress = outcome.made_progress or repair_intent.made_progress;
        // The durable generation intent is now the recovery authority.
        clearTargetAdvanceMaintenanceDebt(self.async_context, entry.config.name);
    }
    for (plan.targets) |target| {
        const entry = &self.core.index_manager.dense_indexes.items[target.dense_index_idx];
        const checkpoint = try self.core.loadProjectionCheckpoint(self.alloc, entry.config.name);
        const config_hash = types.indexConfigHash(entry.config);
        if (checkpoint.status != .rebuilding or checkpoint.config_hash != config_hash) {
            try self.core.saveProjectionCheckpoint(entry.config.name, .{
                .applied_sequence = checkpoint.applied_sequence,
                .status = .rebuilding,
                .generation = checkpoint.generation,
                .config_hash = config_hash,
            });
            outcome.made_progress = true;
        }
        // Only a cursor owned by this catalog generation is reusable.
        // Fresh targets and stale-generation restart markers are persisted
        // here so read-only readiness probes never perform migration I/O.
        if (!target.resume_state_current) {
            const rebuild_root_path = try self.denseIndexRebuildStatePathAlloc(self.alloc, entry.config.name);
            defer self.alloc.free(rebuild_root_path);
            const rebuild_state = self.core.index_manager.rebuildState(.dense_vector, rebuild_root_path, entry.config);
            try rebuild_state.restartWithIo(self.core.index_manager.checkpointIo());
            outcome.made_progress = true;
        }
        // The rebuilding checkpoint plus its generation-scoped cursor now
        // survive process loss. Retire the process-local discovery edge
        // only after both writes have succeeded.
        clearTargetAdvanceMaintenanceDebt(self.async_context, entry.config.name);
    }
    return outcome;
}

pub fn finalizeDenseArtifactRebuildPlan(self: *DB, alloc: Allocator, plan: DenseArtifactRebuildPlan) !bool {
    var made_progress = false;
    for (plan.targets) |target| {
        const entry = &self.core.index_manager.dense_indexes.items[target.dense_index_idx];
        const rebuild_root_path = try self.denseIndexRebuildStatePathAlloc(alloc, entry.config.name);
        defer alloc.free(rebuild_root_path);
        const rebuild_state = self.core.index_manager.rebuildState(.dense_vector, rebuild_root_path, entry.config);
        const applied_sequence = try self.core.loadAppliedSequence(alloc, entry.config.name);
        const target_sequence = try self.probeDerivedReplayTargetSequence(
            alloc,
            self.core.replaySource(),
            .{
                .name = entry.config.name,
                .kind = .dense_vector,
            },
            applied_sequence,
        );
        const repaired = denseCoverageMatchesTarget(entry.index.stats().active_count, target.artifact_target_count) and
            applied_sequence >= target_sequence;
        if (repaired) {
            try rebuild_state.clearAllWithIo(self.core.index_manager.checkpointIo());
            const checkpoint = try self.core.loadProjectionCheckpoint(alloc, entry.config.name);
            try self.core.saveProjectionCheckpoint(entry.config.name, .{
                .applied_sequence = applied_sequence,
                .status = .clean,
                .generation = checkpoint.generation +| 1,
                .config_hash = types.indexConfigHash(entry.config),
            });
            made_progress = true;
        }
    }
    return made_progress;
}

pub fn denseArtifactWatermarkRepairNeeded(self: *DB, alloc: Allocator) !bool {
    return (try repairDenseArtifactAppliedSequencesFromCoverage(self, alloc, false)) > 0;
}

pub fn repairRestoreDenseArtifactCoverageFromFinalArtifacts(self: *DB, alloc: Allocator) !RestoreDenseArtifactCoverageRepair {
    var target_counts = try self.collectDenseArtifactTargetCounts(alloc, null);
    defer target_counts.deinit(alloc);

    var deficit_names = std.ArrayListUnmanaged([]u8).empty;
    var reset_names = std.ArrayListUnmanaged([]u8).empty;
    defer {
        for (deficit_names.items) |name| alloc.free(name);
        deficit_names.deinit(alloc);
        for (reset_names.items) |name| alloc.free(name);
        reset_names.deinit(alloc);
    }
    var result = RestoreDenseArtifactCoverageRepair{};
    for (self.core.index_manager.dense_indexes.items, 0..) |*entry, dense_index_idx| {
        if (!try index_manager_mod.denseConfigRequiresArtifactCoverage(alloc, entry.config)) continue;
        const expected = target_counts.per_target_index.get(dense_index_idx) orelse 0;
        const current = try loadDenseArtifactTargetCounter(alloc, self.core.store, entry.config.name);
        if (current == null or current.? != expected) {
            var raw: [8]u8 = undefined;
            std.mem.writeInt(u64, raw[0..8], expected, .little);
            const counter_key = try denseArtifactTargetCounterKeyAlloc(alloc, entry.config.name);
            defer alloc.free(counter_key);
            try self.core.store.put(counter_key, &raw);
            result.made_progress = true;
        }
        const active_count = entry.index.stats().active_count;
        if (active_count == expected) continue;
        if (active_count < expected) {
            // A deficit is monotonic and can be filled in place from the
            // final restored artifact set. Do that even when the managed
            // worker is resident: the external mutation lease below
            // drains its source capture and prevents it from reopening.
            try deficit_names.append(alloc, try alloc.dupe(u8, entry.config.name));
        } else {
            // A surplus requires removing unknown members. Never reset a
            // live generation under a resident worker; leave it queryable
            // while the ordinary shadow-replacement repair takes over.
            try reset_names.append(alloc, try alloc.dupe(u8, entry.config.name));
        }
    }

    if (deficit_names.items.len > 0) {
        try beginExternalDenseBulkSessionTrackedWait(self.async_context, self.backend_runtime.io());
        var external_session_tracked = true;
        errdefer if (external_session_tracked)
            finishExternalDenseBulkSessionTracked(self.async_context);

        for (deficit_names.items) |name| {
            result.rebuilt += try rebuildDenseIndexForTargetCoverageContext(self.async_context, name, 2048);
        }

        external_session_tracked = false;
        // The restore state machine validates coverage after this bounded
        // quantum. Release admission without running the ordinary idle
        // finalizer early; exact-vector publication remains optional and
        // primary artifacts are the exact fallback until validation.
        finishExternalDenseBulkSessionTracked(self.async_context);
        self.clearDenseHbcCaches();
        result.made_progress = true;
    }

    if (reset_names.items.len > 0 and !self.start_index_workers) {
        // A pointer-selected native-v2 root is a published generation, not
        // a directory that can be cleared and reopened in place. Besides
        // invalidating its READY/authority proof, an in-place reset can
        // leave readers or storage implementations attached to the old
        // files. Reconstruct surplus coverage through the ordinary shadow
        // generation protocol even while restore owns a private staged DB:
        // build from the final primary artifacts, certify native authority,
        // then atomically replace the active-root pointer.
        for (reset_names.items) |name| {
            const cfg_ptr = self.core.index_manager.get(name) orelse return error.IndexNotFound;
            var cfg = try types.IndexConfig.clone(alloc, cfg_ptr.*);
            defer cfg.deinit(alloc);
            // Restore is the synchronous owner of this private generation.
            // Keep the detected durable intent unchanged until the final
            // counter/checkpoint proof removes it; scheduler phase leases
            // are for independently resumed background candidates.
            const rebuilt = try self.rebuildIndexWithShadowReplacement(alloc, cfg, .{}, null);
            if (rebuilt.yielded) return error.ShadowIndexCatchUpIncomplete;
            result.rebuilt +|= @intCast(rebuilt.reprocessed);
        }
        self.clearDenseHbcCaches();
        result.made_progress = true;
    }

    if (reset_names.items.len > 0 and self.start_index_workers) {
        result.waiting_for_workers = true;
    }
    return result;
}

pub fn restoreOwnsDenseArtifactIntent(trigger: index_repair_state.Trigger) bool {
    return trigger == .artifact_counter_missing or
        trigger == .artifact_coverage_mismatch or
        trigger == .projection_generation_invalid;
}

pub fn completeRestoreDenseArtifactRepairs(self: *DB, alloc: Allocator) !RestoreDenseArtifactCompletion {
    var state = self.loadIndexRepairState(alloc) catch |err| switch (err) {
        error.FileNotFound, error.DurableIndexRepairStateUnavailable => return .complete,
        else => return err,
    };
    defer state.deinit(alloc);

    // `rebuild_replayed_artifacts` reconstructed the private restore root
    // from its final logical artifacts. Recount them here so a restored
    // repair-required checkpoint can be retired only from exact source
    // and active-generation coverage, never from a stale counter alone.
    var exact_target_counts = try self.collectDenseArtifactTargetCounts(alloc, null);
    defer exact_target_counts.deinit(alloc);

    // Validate every durable intent before changing counters or removing
    // any intent. A pending proof error must be observably mutation-free.
    for (state.entries.items) |entry| {
        if (!restoreOwnsDenseArtifactIntent(entry.intent.trigger)) continue;
        const cfg = self.core.index_manager.get(entry.intent.index_name) orelse
            return error.RestoreDenseIndexProofIncomplete;
        if (cfg.kind != .dense_vector or
            types.indexConfigHash(cfg.*) != entry.intent.config_hash)
        {
            return error.RestoreDenseConfigProofIncomplete;
        }
    }

    // Counter bootstrap is required input for worker convergence. Publish
    // it as its own successful bounded quantum, then retry all proofs from
    // a stable state instead of returning a pending error after mutation.
    var bootstrapped = false;
    for (state.entries.items) |entry| {
        if (entry.intent.trigger != .artifact_counter_missing) continue;
        const cfg = self.core.index_manager.get(entry.intent.index_name) orelse unreachable;
        if (try loadDenseArtifactTargetCounter(alloc, self.core.store, cfg.name) != null) continue;
        try self.ensureDenseArtifactTargetCounterForRepair(
            alloc,
            cfg.*,
            entry.intent.repair_id,
            null,
        );
        bootstrapped = true;
    }
    if (bootstrapped) return .progressed;

    // Complete every proof before removing the first intent. This keeps
    // all pending exits side-effect free and avoids partially publishing a
    // multi-index restore proof.
    for (state.entries.items) |entry| {
        if (!restoreOwnsDenseArtifactIntent(entry.intent.trigger)) continue;
        const cfg = self.core.index_manager.get(entry.intent.index_name) orelse unreachable;
        const expected = (try loadDenseArtifactTargetCounter(alloc, self.core.store, cfg.name)) orelse
            return error.RestoreDenseCounterProofIncomplete;
        const dense = self.core.index_manager.denseIndex(cfg.name) orelse
            return error.RestoreDenseIndexProofIncomplete;
        if (!denseCoverageMatchesTarget(dense.index.stats().active_count, expected)) {
            return error.RestoreDenseCoverageProofIncomplete;
        }

        const applied = try self.core.loadAppliedSequence(alloc, cfg.name);
        const target = try self.probeDerivedReplayTargetSequence(
            alloc,
            self.core.replaySource(),
            .{ .name = cfg.name, .kind = .dense_vector },
            applied,
        );
        const checkpoint = try self.core.loadProjectionCheckpoint(alloc, cfg.name);
        if (applied < target or
            (entry.intent.trigger != .projection_generation_invalid and checkpoint.status != .clean) or
            checkpoint.applied_sequence < target or
            checkpoint.config_hash != entry.intent.config_hash)
        {
            return error.RestoreDenseCheckpointIncomplete;
        }

        if (entry.intent.trigger == .projection_generation_invalid) {
            var exact_expected: ?u64 = null;
            for (self.core.index_manager.dense_indexes.items, 0..) |*dense_entry, dense_index_idx| {
                if (!std.mem.eql(u8, dense_entry.config.name, cfg.name)) continue;
                exact_expected = exact_target_counts.per_target_index.get(dense_index_idx) orelse 0;
                break;
            }
            if (exact_expected == null or exact_expected.? != expected) {
                return error.RestoreDenseCounterProofIncomplete;
            }
            if (@hasDecl(@typeInfo(@TypeOf(dense.index)).pointer.child, "validateStoredStructure")) {
                dense.index.validateStoredStructure(alloc) catch
                    return error.RestoreDenseIndexProofIncomplete;
            }
            if (checkpoint.status == .degraded) return error.RestoreDenseCheckpointIncomplete;
            if (entry.intent.candidate_relative_path) |candidate| {
                if (try self.core.index_manager.isRepairCandidateActive(cfg.name, candidate)) {
                    return error.RestoreDenseIndexProofIncomplete;
                }
            }
        }
    }

    var removed_any = false;
    for (state.entries.items) |entry| {
        if (!restoreOwnsDenseArtifactIntent(entry.intent.trigger)) continue;
        if (entry.intent.trigger == .projection_generation_invalid) {
            const cfg = self.core.index_manager.get(entry.intent.index_name) orelse unreachable;
            const checkpoint = try self.core.loadProjectionCheckpoint(alloc, cfg.name);
            const applied = try self.core.loadAppliedSequence(alloc, cfg.name);
            if (checkpoint.status != .clean or checkpoint.applied_sequence != applied) {
                try self.core.saveProjectionCheckpoint(cfg.name, .{
                    .applied_sequence = applied,
                    .status = .clean,
                    .generation = checkpoint.generation +| @intFromBool(checkpoint.status != .clean),
                    .config_hash = entry.intent.config_hash,
                });
            }
            if (entry.intent.candidate_relative_path != null) {
                if (entry.intent.phase == .terminal) {
                    try self.updateIndexRepairIntent(alloc, entry.intent.repair_id, .{
                        .phase = .detected,
                        .failure_streak = 0,
                        .next_retry_at_ms = 0,
                        .replace_last_error = true,
                    });
                }
                try self.discardInactiveIndexRepairCandidate(alloc, entry.intent.repair_id);
            }
        }
        // This is restore-owned metadata debt, not a shadow-generation
        // attempt, so there is no candidate phase chain to advance. All
        // entries passed proof before the first durable removal.
        try self.removeIndexRepairIntentAndPin(alloc, entry.intent.repair_id);
        removed_any = true;
    }
    return if (removed_any) .completed_with_progress else .complete;
}

pub fn repairDenseArtifactAppliedSequencesIfCovered(self: *DB, alloc: Allocator) !usize {
    return try repairDenseArtifactAppliedSequencesFromCoverage(self, alloc, true);
}

pub fn repairDenseArtifactAppliedSequencesFromCoverage(self: *DB, alloc: Allocator, repair: bool) !usize {
    var target_counts = try self.collectDenseArtifactTargetCounts(alloc, null);
    defer target_counts.deinit(alloc);

    var repaired: usize = 0;
    for (self.core.index_manager.dense_indexes.items, 0..) |*entry, dense_index_idx| {
        const artifact_backed = entry.external or entry.chunk_name != null or entry.embedding_name != null or entry.embedding_names.len > 0;
        if (!artifact_backed) continue;

        const artifact_target_count = target_counts.per_target_index.get(dense_index_idx) orelse 0;
        if (artifact_target_count == 0) continue;
        if (entry.index.stats().active_count != artifact_target_count) continue;

        const applied_sequence = try self.core.loadAppliedSequence(alloc, entry.config.name);
        const checkpoint = try self.core.loadProjectionCheckpoint(alloc, entry.config.name);
        const target_sequence = try self.probeDerivedReplayTargetSequence(
            alloc,
            self.core.replaySource(),
            .{
                .name = entry.config.name,
                .kind = .dense_vector,
            },
            applied_sequence,
        );
        const checkpoint_needs_finalization = checkpoint.status == .rebuilding or
            checkpoint.applied_sequence < target_sequence;
        if (applied_sequence >= target_sequence and !checkpoint_needs_finalization) continue;

        repaired += 1;
        if (repair) {
            if (applied_sequence < target_sequence) try self.core.saveAppliedSequence(entry.config.name, target_sequence);
            const rebuild_root_path = try self.denseIndexRebuildStatePathAlloc(alloc, entry.config.name);
            defer alloc.free(rebuild_root_path);
            const rebuild_state = self.core.index_manager.rebuildState(.dense_vector, rebuild_root_path, entry.config);
            try rebuild_state.clearAllWithIo(self.core.index_manager.checkpointIo());
            try self.core.saveProjectionCheckpoint(entry.config.name, .{
                .applied_sequence = target_sequence,
                .status = .clean,
                .generation = checkpoint.generation + @intFromBool(checkpoint.status == .rebuilding),
                .config_hash = types.indexConfigHash(entry.config),
            });
        }
    }
    return repaired;
}

pub fn rebuildDenseIndexesFromStoredEmbeddingArtifacts(self: *DB, alloc: Allocator) !usize {
    return try rebuildDenseIndexesFromStoredEmbeddingArtifactsWithProgress(self, alloc, null, null);
}

pub fn rebuildDenseIndexesFromStoredEmbeddingArtifactsWithProgress(
    self: *DB,
    alloc: Allocator,
    progress_ctx: ?*anyopaque,
    progress_hook: ?ReplayProgressHook,
) !usize {
    var dense_visibility_attempted = false;
    errdefer if (dense_visibility_attempted) self.clearDenseHbcCaches();
    const rebuilt = try self.rebuildDenseIndexesFromStoredEmbeddingArtifactsResumeWithProgress(
        alloc,
        null,
        null,
        null,
        progress_ctx,
        progress_hook,
        null,
        null,
        &dense_visibility_attempted,
        2048,
        128,
    );
    if (dense_visibility_attempted) self.clearDenseHbcCaches();
    return rebuilt;
}

pub fn rebuildDenseIndexesFromStoredEmbeddingArtifactsIfNeeded(self: *DB, alloc: Allocator) !usize {
    return try self.rebuildDenseIndexesFromStoredEmbeddingArtifactsIfNeededWithProgress(alloc, null, null);
}

pub fn hasPendingDenseArtifactRebuild(self: *DB, alloc: Allocator) !bool {
    if (comptime storage_engine.enabled) return storage_engine.call(@src().fn_name, .{ self, alloc });
    var plan = try self.collectDenseArtifactRebuildPlan(alloc);
    defer plan.deinit(alloc);
    // Stale resume metadata without a logical target is maintenance-only:
    // the execution helper clears it, but readiness must not surface it as
    // rebuild debt (and this probe must remain mutation-free).
    return plan.targets.len > 0 or plan.generation_repairs.len > 0;
}

/// Internal scheduler predicate. Unlike logical readiness, maintenance
/// includes stale cursor cleanup, but excludes generation debt already
/// delegated to an equivalent durable repair intent. The probe is
/// read-only: cursor migration and query-gate changes happen in execution.
pub fn denseArtifactRebuildMaintenanceNeeded(self: *DB, alloc: Allocator) !bool {
    if (comptime storage_engine.enabled) return storage_engine.call(@src().fn_name, .{ self, alloc });
    var plan = try self.collectDenseArtifactRebuildPlan(alloc);
    defer plan.deinit(alloc);
    if (plan.targets.len > 0 or plan.rebuild_state_cleanups.len > 0) return true;
    for (plan.generation_repairs) |repair| {
        const entry = &self.core.index_manager.dense_indexes.items[repair.dense_index_idx];
        if (!self.core.index_manager.repairUnavailable(entry.config.name)) return true;
        const repair_id = (try self.indexRepairIdForIndex(alloc, entry.config.name)) orelse return true;
        var existing = self.loadIndexRepairEntryById(alloc, repair_id) catch |err| switch (err) {
            error.FileNotFound, error.IndexRepairIntentNotFound => return true,
            else => return err,
        };
        defer existing.deinit(alloc);
        if (automaticDenseGenerationRepairTriggerPriority(repair.trigger) >
            automaticDenseGenerationRepairTriggerPriority(existing.intent.trigger)) return true;
    }
    return false;
}

/// Executes one internal maintenance pass and reports only state mutation,
/// not the legacy public work count. Cache retirement is owned here so
/// managed callers cannot mistake rediscovered repair debt for visibility.
pub fn runDenseArtifactRebuildMaintenanceWithProgress(
    self: *DB,
    alloc: Allocator,
    progress_ctx: ?*anyopaque,
    progress_hook: ?ReplayProgressHook,
) !bool {
    if (comptime storage_engine.enabled) return storage_engine.call(@src().fn_name, .{ self, alloc, progress_ctx, progress_hook, storage_engine.ReplayBoundary.local_dispatch });
    const outcome = try self.rebuildDenseIndexesFromStoredEmbeddingArtifactsOutcomeWithProgress(
        alloc,
        progress_ctx,
        progress_hook,
    );
    if (outcome.dense_visibility_changed) self.clearDenseHbcCaches();
    return outcome.made_progress;
}

pub fn rebuildDenseIndexesFromStoredEmbeddingArtifactsIfNeededWithProgress(
    self: *DB,
    alloc: Allocator,
    progress_ctx: ?*anyopaque,
    progress_hook: ?ReplayProgressHook,
) !usize {
    return (try self.rebuildDenseIndexesFromStoredEmbeddingArtifactsOutcomeWithProgress(
        alloc,
        progress_ctx,
        progress_hook,
    )).reported_work;
}

/// Seed certificate validation before replay can advance live cardinality.
pub fn validateOpenedDenseServingCertificates(self: *DB) void {
    for (self.core.index_manager.dense_indexes.items) |*entry| {
        var checkpoint = self.core.loadProjectionCheckpoint(self.alloc, entry.config.name) catch continue;
        // A durable native source-only commit can advance its watermark
        // past the empty activation's count sidecar. Recover that explicit
        // zero publication only at open, with the same clean generation
        // and an empty durable native snapshot. Missing certificates and
        // unfinished generations still require ordinary materialization.
        if (checkpoint.published_count == null and checkpoint.status == .clean) {
            const previous = apply_state.loadProjectionCheckpointWithSidecar(self.alloc, self.core.index_manager.checkpointIo(), self.core.store, self.core.applied_sequence_checkpoint_path, entry.config.name) catch null;
            if (previous) |certificate| {
                const native_snapshot = entry.index.nativeServingSnapshot();
                if (certificate.status == .clean and certificate.published_count == @as(?u64, 0) and
                    certificate.generation == checkpoint.generation and certificate.config_hash == checkpoint.config_hash and
                    certificate.applied_sequence <= checkpoint.applied_sequence and native_snapshot != null and
                    native_snapshot.?.active_count == 0 and native_snapshot.?.source_sequence >= checkpoint.applied_sequence)
                {
                    checkpoint.published_count = 0;
                    if (!openModeRequiresReadOnlyBackends(self.open_mode))
                        self.core.saveProjectionCheckpoint(entry.config.name, checkpoint) catch continue;
                }
            }
        }
        if (!entry.validateServingCertificate(checkpoint)) {
            // An absent or newer sidecar may not describe the native
            // WAL's immutable generation after an in-flight source
            // window. On reopen it still proves a durable prefix. Record
            // this only at the open boundary. Status must never certify
            // a later coincidental live count.
            _ = entry.validateRecoveredNativeServingCertificate(checkpoint);
        }
    }
}

pub fn denseServingCounts(index: *hbc_mod.HBCIndex) DenseServingCounts {
    if (index.nativeServingSnapshot()) |serving_snapshot| return .{
        .doc_count = serving_snapshot.active_count,
        .node_count = serving_snapshot.node_count,
        .root_node = serving_snapshot.root_node,
        .revision = serving_snapshot.publish_generation,
    };
    if (index.experimentalPostingWalAuthoritative()) return .{
        .doc_count = 0,
        .node_count = 0,
        .root_node = 0,
        .revision = 0,
    };
    const published = index.publishedStats();
    return .{
        .doc_count = published.stats.active_count,
        .node_count = published.stats.node_count,
        .root_node = published.stats.root_node,
        .revision = published.generation,
    };
}

pub fn markDenseCoverageRegressionIfNeeded(self: *DB, alloc: Allocator, index_name: []const u8, item: *types.DBIndexStats) !void {
    const status_snapshot = (try self.loadIndexStatusSnapshot(alloc, index_name)) orelse return;
    if (status_snapshot.kind != .dense_vector) return;
    if (status_snapshot.doc_count <= item.doc_count) return;
    item.repair_degraded = true;
    item.repair_issue_count +|= 1;
}

/// Populate the exact physical cardinality expected from the current dense
/// incarnation without scanning documents or artifacts. External,
/// chunk-backed, and multi-source indexes use the write-maintained target
/// counter because document outcomes cannot express their multiplicity.
/// One-vector-per-document managed indexes use generation-scoped outcomes.
pub fn populateDensePublicationTarget(self: *DB, index_name: []const u8, coverage_counts: DerivedCoverageCounters, item: *types.DBIndexStats) !void {
    if (item.kind != .dense_vector) return;
    item.publication_target_count = 0;
    item.publication_target_ready = false;
    const entry = self.core.denseIndex(index_name) orelse return;
    if (densePublicationTargetUsesArtifactCounter(entry)) {
        item.publication_target_count = (try coverage_counts.artifactCount()) orelse return;
        item.publication_target_ready = true;
        return;
    }
    if (!item.coverage_summary_ready) return;
    item.publication_target_count = item.coverage_produced_count;
    item.publication_target_ready = true;
}

/// Execute a general query while retaining the single dense component's
/// profile when the request is composed (for example text+dense hybrid).
/// The public response currently has one dense profile slot; compositions
/// with multiple dense lanes intentionally leave it unset rather than
/// publishing an ambiguous aggregate.
pub fn searchWithDenseProfile(self: *DB, alloc: Allocator, req: types.SearchRequest) !SearchWithDenseProfileResult {
    if (comptime storage_engine.enabled) return storage_engine.call(@src().fn_name, .{ self, alloc, req });
    if (self.local_execution.initial_child_hidden.load(.acquire)) return error.InitialChildNotPublished;
    var row_policy_lease = try self.local_execution.row_policy_gate.enterUnsupportedSearch();
    defer row_policy_lease.release();
    try self.enforcePortableRuntimeGate();
    if (self.async_context.resource_manager) |manager| manager.beginForegroundQuery();
    defer if (self.async_context.resource_manager) |manager| manager.finishForegroundQuery();
    const snapshot_input = directSingleVectorRequest(req) orelse req;
    const dense = snapshot_input.dense orelse if (snapshot_input.query == .dense_knn) snapshot_input.query.dense_knn else null;
    if (dense) |query| if (try self.trySnapshotDenseSearch(alloc, snapshot_input, query, true)) |captured| {
        var result = captured;
        result.request = req;
        result.request.identity_read_generation = captured.request.identity_read_generation;
        return result;
    };
    const search_access = self.beginDenseSearchAccess(req);
    defer self.endDenseSearchAccess(search_access);
    try self.enforcePortableRuntimeGate();
    const snapshot_req = try self.searchRequestAtCurrentIdentityGeneration(req);
    var dense_profile: db_query_search.DenseSearchProfile = .{};
    const result = try self.searchLockedWithExecutionContextImpl(alloc, snapshot_req, .{}, true, &dense_profile);
    return .{
        .request = snapshot_req,
        .result = result,
        .dense_profile = if (dense_profile.search_route.len > 0) dense_profile else null,
    };
}

pub fn searchDenseComposedCallback(
    ctx: ?*anyopaque,
    alloc: Allocator,
    req: types.SearchRequest,
    dense: types.DenseKnnQuery,
) anyerror!types.SearchResult {
    const self: *DB = @ptrCast(@alignCast(ctx orelse return error.InvalidArgument));
    return try self.searchDense(alloc, req, dense);
}

pub fn searchDenseComposedProfiledCallback(
    ctx: ?*anyopaque,
    alloc: Allocator,
    req: types.SearchRequest,
    dense: types.DenseKnnQuery,
) anyerror!db_query_search.ProfiledDenseSearchResult {
    const self: *DB = @ptrCast(@alignCast(ctx orelse return error.InvalidArgument));
    return try searchDenseProfiledAtSnapshot(self, alloc, req, dense);
}

pub fn resolveDenseHitKeyCallback(
    ctx: ?*anyopaque,
    alloc: Allocator,
    entry: *index_manager_mod.IndexManager.DenseIndex,
    key: []const u8,
) anyerror![]u8 {
    _ = ctx;
    if (entry.embedding_names.len == 0) return try alloc.dupe(u8, key);
    return try resolveMultiSourceDenseHitKeyAlloc(alloc, key);
}

pub fn exactDenseSearchCallback(
    ctx: ?*anyopaque,
    entry: *index_manager_mod.IndexManager.DenseIndex,
    req: vectorindex_mod.SearchRequest,
) anyerror!dense_exact.SearchOutcome {
    const self: *DB = @ptrCast(@alignCast(ctx orelse return error.InvalidArgument));
    return try self.core.index_manager.exactScoreDenseEntryWithRequest(entry, req);
}

pub fn denseVectorIdsForOrdinalsCallback(
    ctx: ?*anyopaque,
    alloc: Allocator,
    index_name: []const u8,
    ordinals: []const u32,
) anyerror![]u64 {
    const self: *DB = @ptrCast(@alignCast(ctx orelse return error.InvalidArgument));
    return try self.core.index_manager.lookupDenseVectorIdsForOrdinalsAlloc(
        alloc,
        self.core.store,
        index_name,
        ordinals,
    );
}

pub fn denseOrdinalsForVectorIdsCallback(
    ctx: ?*anyopaque,
    alloc: Allocator,
    index_name: []const u8,
    vector_ids: []const u64,
    generation: ?u64,
) anyerror![]?doc_set.DocOrdinal {
    const self: *DB = @ptrCast(@alignCast(ctx orelse return error.InvalidArgument));
    const ordinals = try self.core.index_manager.lookupDenseOrdinalsForVectorIdsAlloc(
        alloc,
        self.core.store,
        index_name,
        vector_ids,
    );
    errdefer alloc.free(ordinals);
    const all_visible = try self.allDocsVisibleSummaryFast(generation);
    if (all_visible) return ordinals;

    var txn = try self.core.store.beginProbeTxn();
    defer txn.abort();
    for (ordinals) |*maybe_ordinal| {
        const ordinal = maybe_ordinal.* orelse continue;
        const state = (try doc_identity.lookupStateTxn(&txn, ordinal)) orelse {
            maybe_ordinal.* = null;
            continue;
        };
        const visible = if (generation) |at| state.isVisibleAt(at) else state.isLive();
        if (!visible) maybe_ordinal.* = null;
    }
    return ordinals;
}

pub fn searchDense(self: *DB, alloc: Allocator, req: types.SearchRequest, dense: types.DenseKnnQuery) !types.SearchResult {
    if (comptime storage_engine.enabled) return storage_engine.call(@src().fn_name, .{ self, alloc, req, dense });
    if (builtin.os.tag == .freestanding) return error.UnsupportedPlatform;
    const metric_name = denseQueryMetricIndexName(self, req);
    const start_ns = platform_time.monotonicNs();
    errdefer observeSearchFailureMetric(metric_name, .vector, platform_time.monotonicNs() -| start_ns);
    const bench_profile = benchQueryProfileEnabled();
    const total_start_ns = if (bench_profile) platform_time.monotonicNs() else 0;
    var algebraic_ns: u64 = 0;
    var prove_ns: u64 = 0;
    var inner_ns: u64 = 0;
    const algebraic_start_ns = if (bench_profile) platform_time.monotonicNs() else 0;
    // Chunk members are a separate vector member from their parent row
    // and share only its ordinal, not its own fields. Resolving
    // filter_query_json/exclusion_query_json (and filter_text/
    // exclusion_text, i.e. query.bool.filter/must_not) against the
    // algebraic/parent doc-number space here - same as against the
    // chunk-backed index's own doc set - can never see a parent-only
    // field: a positive clause would match nothing, a negative one would
    // exclude nothing. Skip the algebraic resolution entirely for
    // chunk-backed member/chunk-mode dense search (mirroring
    // searchTextQuery's `suppress_text_doc_num_filter` gate) so the JSON
    // filters survive unresolved into the lift step below, which folds
    // them into applyStoredSearchPatternFilters' parent-aware matcher
    // instead (issue #931). resolved_doc_filter - the generic
    // doc-identity form filter_prefix resolves into
    // (searchRequestWithIdentityPrefixFilterAlloc, run before this
    // wrapper is even reached) - is left intact: db_query_search.searchDense
    // itself skips applying it natively for a chunk-backed member/chunk-mode
    // query and instead carries it through to applyStoredSearchPatternFilters'
    // parent-aware matcher, same as the full-text arm (issue #931/#957).
    const chunk_backed_for_filter = if (self.core.denseIndex(req.index_name)) |entry|
        entry.chunk_name != null
    else
        false;
    const member_mode_for_filter = req.return_mode == .member or req.return_mode == .chunk;
    const suppress_dense_doc_num_filter = chunk_backed_for_filter and member_mode_for_filter;
    var algebraic_filter = if (suppress_dense_doc_num_filter)
        AlgebraicDocFilterRequest{ .req = req }
    else
        try self.searchRequestWithAlgebraicDocFilterAlloc(req);
    defer algebraic_filter.deinit();
    if (bench_profile) algebraic_ns = platform_time.monotonicNs() - algebraic_start_ns;
    // query.bool.filter/must_not (filter_text/exclusion_text) resolve the
    // same wrong way via native TextQuery resolution; lift the
    // representable clauses into filter_query_json/exclusion_query_json
    // so the same parent-aware matcher covers this arm too (issue #931).
    var lifted_dense_bool_filter = ChunkBoolFilterLowering{};
    defer lifted_dense_bool_filter.deinit(alloc);
    {
        if (suppress_dense_doc_num_filter) {
            lifted_dense_bool_filter = try liftChunkBoolFilterClausesAlloc(
                alloc,
                algebraic_filter.req.filter_text,
                algebraic_filter.req.exclusion_text,
                algebraic_filter.req.filter_query_json,
                algebraic_filter.req.exclusion_query_json,
            );
            if (lifted_dense_bool_filter.filter_query_json) |json| algebraic_filter.req.filter_query_json = json;
            if (lifted_dense_bool_filter.exclusion_query_json) |json| algebraic_filter.req.exclusion_query_json = json;
            algebraic_filter.req.filter_text = null;
            algebraic_filter.req.exclusion_text = null;
        }
    }
    const prove_start_ns = if (bench_profile) platform_time.monotonicNs() else 0;
    try self.proveVectorSearchAccessPath(algebraic_filter.req.index_name, .dense_vector, hasNativeDocIdConstraints(algebraic_filter.req));
    if (bench_profile) prove_ns = platform_time.monotonicNs() - prove_start_ns;
    const inner_start_ns = if (bench_profile) platform_time.monotonicNs() else 0;
    const result = try db_query_search.searchDense(alloc, algebraic_filter.req, dense, .{
        .filter_candidate_presence = true,
        .ctx = self,
        .text_index_entry = textIndexEntryCallback,
        .dense_index = denseIndexCallback,
        .lookup_doc_key = denseDocKeyCallback,
        .resolve_hit_key = resolveDenseHitKeyCallback,
        .lookup_vector_id = denseVectorIdCallback,
        .lookup_vector_ids_for_ordinals = denseVectorIdsForOrdinalsCallback,
        .all_docs_visible_fast = allDocsVisibleFastCallback,
        .lookup_doc_ordinal = lookupLiveDocOrdinalNoLockCallback,
        .lookup_doc_ordinals = lookupLiveDocOrdinalsNoLockCallback,
        .lookup_doc_ordinals_for_vector_ids = denseOrdinalsForVectorIdsCallback,
        .resolve_doc_set_doc_ids = resolveDocSetDocIdsCallback,
        .resolve_doc_ids_to_doc_set = resolveDocIdsToDocSetCallback,
        .live_filter_doc_set = liveFilterDocSetCallback,
        .nonvisible_doc_set = nonVisibleDocSetCallback,
        .load_projected_document = loadRequiredProjectedSearchDocumentCallback,
        .hbc_search = hbcSearchCallback,
        .hbc_search_profiled = hbcSearchProfiledCallback,
        .exact_dense_search = exactDenseSearchCallback,
        .postprocess = postprocessVectorSearchResultCallback,
    });
    if (bench_profile) {
        inner_ns = platform_time.monotonicNs() - inner_start_ns;
        std.log.info(
            "antfly_bench_db_dense_wrapper total_us={d} algebraic_us={d} prove_us={d} inner_us={d}",
            .{ (platform_time.monotonicNs() - total_start_ns) / 1000, algebraic_ns / 1000, prove_ns / 1000, inner_ns / 1000 },
        );
    }
    db_query_metrics.observeSortProfile(metric_name, .vector, platform_time.monotonicNs() -| start_ns, result.sort_profile);
    return result;
}

pub fn searchDenseWithProfileSink(self: *DB, alloc: Allocator, req: types.SearchRequest, dense: types.DenseKnnQuery, sink: ?*db_query_search.DenseSearchProfile) !types.SearchResult {
    const profile = sink orelse return self.searchDense(alloc, req, dense);
    // The general search already owns its read lease and identity token.
    // Direct dense lowering must retain telemetry just like a composed
    // dense lane, without reacquiring the outer search lock.
    const profiled = try searchDenseProfiledAtSnapshot(self, alloc, req, dense);
    profile.* = profiled.profile;
    return profiled.result;
}

pub fn searchDenseProfiled(self: *DB, alloc: Allocator, req: types.SearchRequest, dense: types.DenseKnnQuery) !db_query_search.ProfiledDenseSearchResult {
    if (comptime storage_engine.enabled) return storage_engine.call(@src().fn_name, .{ self, alloc, req, dense });
    return (try self.searchDenseProfiledWithCapturedRequest(alloc, req, dense)).profiled;
}

/// Profiled counterpart to `searchWithCapturedRequest`; the request token
/// and dense result are created under one DB search lease.
pub fn searchDenseProfiledWithCapturedRequest(
    self: *DB,
    alloc: Allocator,
    req: types.SearchRequest,
    dense: types.DenseKnnQuery,
) !ProfiledDenseSearchWithCapturedRequestResult {
    if (comptime storage_engine.enabled) return storage_engine.call(@src().fn_name, .{ self, alloc, req, dense });
    var row_policy_lease = try self.local_execution.row_policy_gate.enterUnsupportedSearch();
    defer row_policy_lease.release();
    if (builtin.os.tag == .freestanding) return error.UnsupportedPlatform;
    try self.enforcePortableRuntimeGate();
    if (self.async_context.resource_manager) |manager| manager.beginForegroundQuery();
    defer if (self.async_context.resource_manager) |manager| manager.finishForegroundQuery();
    if (try self.trySnapshotDenseSearch(alloc, req, dense, true)) |captured| return .{
        .request = captured.request,
        .profiled = .{ .result = captured.result, .profile = captured.dense_profile.? },
    };
    const search_access = self.beginDenseSearchAccess(req);
    defer self.endDenseSearchAccess(search_access);
    // Publication can begin after the optimistic retry above. Validate
    // only after owning apply-shared or the published-catalog reader lease
    // so the profiled API has the same generation guarantee as search().
    try self.enforcePortableRuntimeGate();
    const snapshot_req = try self.searchRequestAtCurrentIdentityGeneration(req);
    return .{
        .request = snapshot_req,
        .profiled = try searchDenseProfiledAtSnapshot(self, alloc, snapshot_req, dense),
    };
}

pub fn searchDenseProfiledAtSnapshot(self: *DB, alloc: Allocator, req: types.SearchRequest, dense: types.DenseKnnQuery) !db_query_search.ProfiledDenseSearchResult {
    var identity_prefix_filter = try self.searchRequestWithIdentityPrefixFilterAlloc(req);
    defer identity_prefix_filter.deinit();
    // See searchDense: the algebraic resolver would resolve filter_query_json/
    // exclusion_query_json/filter_text/exclusion_text against the parent
    // doc-number space, which never matches a chunk-backed member/chunk-mode
    // dense index's own doc set (issue #931). resolved_doc_filter - the
    // generic doc-identity form filter_prefix resolved into just above via
    // searchRequestWithIdentityPrefixFilterAlloc - is left intact for the
    // same reason as searchDense: db_query_search.searchDense carries it
    // through to applyStoredSearchPatternFilters' parent-aware matcher
    // instead of applying it natively (issue #931/#957).
    const chunk_backed_for_filter = if (self.core.denseIndex(identity_prefix_filter.req.index_name)) |entry|
        entry.chunk_name != null
    else
        false;
    const member_mode_for_filter = identity_prefix_filter.req.return_mode == .member or identity_prefix_filter.req.return_mode == .chunk;
    const suppress_dense_doc_num_filter = chunk_backed_for_filter and member_mode_for_filter;
    var algebraic_filter = if (suppress_dense_doc_num_filter)
        AlgebraicDocFilterRequest{ .req = identity_prefix_filter.req }
    else
        try self.searchRequestWithAlgebraicDocFilterAlloc(identity_prefix_filter.req);
    defer algebraic_filter.deinit();
    var lifted_dense_bool_filter = ChunkBoolFilterLowering{};
    defer lifted_dense_bool_filter.deinit(alloc);
    if (suppress_dense_doc_num_filter) {
        lifted_dense_bool_filter = try liftChunkBoolFilterClausesAlloc(
            alloc,
            algebraic_filter.req.filter_text,
            algebraic_filter.req.exclusion_text,
            algebraic_filter.req.filter_query_json,
            algebraic_filter.req.exclusion_query_json,
        );
        if (lifted_dense_bool_filter.filter_query_json) |json| algebraic_filter.req.filter_query_json = json;
        if (lifted_dense_bool_filter.exclusion_query_json) |json| algebraic_filter.req.exclusion_query_json = json;
        algebraic_filter.req.filter_text = null;
        algebraic_filter.req.exclusion_text = null;
    }
    try self.proveVectorSearchAccessPath(algebraic_filter.req.index_name, .dense_vector, hasNativeDocIdConstraints(algebraic_filter.req));
    const profiled = db_query_search.searchDenseProfiled(alloc, algebraic_filter.req, dense, .{
        .filter_candidate_presence = true,
        .ctx = self,
        .text_index_entry = textIndexEntryCallback,
        .dense_index = denseIndexCallback,
        .lookup_doc_key = denseDocKeyCallback,
        .resolve_hit_key = resolveDenseHitKeyCallback,
        .lookup_vector_id = denseVectorIdCallback,
        .lookup_vector_ids_for_ordinals = denseVectorIdsForOrdinalsCallback,
        .all_docs_visible_fast = allDocsVisibleFastCallback,
        .lookup_doc_ordinal = lookupLiveDocOrdinalNoLockCallback,
        .lookup_doc_ordinals = lookupLiveDocOrdinalsNoLockCallback,
        .lookup_doc_ordinals_for_vector_ids = denseOrdinalsForVectorIdsCallback,
        .resolve_doc_set_doc_ids = resolveDocSetDocIdsCallback,
        .resolve_doc_ids_to_doc_set = resolveDocIdsToDocSetCallback,
        .live_filter_doc_set = liveFilterDocSetCallback,
        .nonvisible_doc_set = nonVisibleDocSetCallback,
        .load_projected_document = loadRequiredProjectedSearchDocumentCallback,
        .hbc_search = hbcSearchCallback,
        .hbc_search_profiled = hbcSearchProfiledCallback,
        .exact_dense_search = exactDenseSearchCallback,
        .postprocess = postprocessVectorSearchResultCallback,
    });
    return profiled catch |err| {
        if (err == error.IndexNotFound) {
            const index_configs = self.listIndexes(alloc) catch |list_err| {
                std.log.err("dense profiled search missing index requested={s} list_err={s}", .{
                    req.index_name orelse "<null>",
                    @errorName(list_err),
                });
                return err;
            };
            defer types.freeIndexConfigs(alloc, index_configs);
            std.log.err("dense profiled search missing index requested={s} configured_index_count={d}", .{
                req.index_name orelse "<null>",
                index_configs.len,
            });
            for (index_configs) |cfg| {
                std.log.err("dense profiled search visible index name={s} kind={s}", .{
                    cfg.name,
                    @tagName(cfg.kind),
                });
            }
        }
        return err;
    };
}

pub fn denseQueryMetricIndexName(self: *DB, req: types.SearchRequest) ?[]const u8 {
    if (req.index_name) |name| return name;
    const entry = self.core.denseIndex(null) orelse return null;
    return entry.config.name;
}

pub fn denseIndexCallback(
    ctx: ?*anyopaque,
    index_name: ?[]const u8,
) anyerror!?*index_manager_mod.IndexManager.DenseIndex {
    const self: *DB = @ptrCast(@alignCast(ctx orelse return error.InvalidArgument));
    try self.failIfIndexQuarantined(index_name);
    return self.core.denseIndex(index_name);
}

pub fn snapshotDenseRequestEligible(req: types.SearchRequest) bool {
    if (req.document_authority_mode == .hydrate or (req.document_authority_mode == .candidates and req.document_authority_group == 0)) return false;
    if (req.include_stored or (req.query != .dense_knn and !db_query_search.isDefaultMatchAll(req.query))) return false;
    // Default-deny future request fields. This first snapshot path handles
    // plain document-member score pages; filters, hydration, grouping and
    // composed queries keep their existing synchronization.
    var remaining = req;
    const defaults: types.SearchRequest = .{};
    inline for (.{ "query", "dense", "index_name", "primary_text_index_name", "profile", "limit", "offset", "include_stored", "include_all_fields", "defer_stored_projection", "search_effort", "identity_read_generation", "execution_deadline_ns", "cancellation", "document_authority_mode", "document_authority_group" }) |field| {
        @field(remaining, field) = @field(defaults, field);
    }
    // Empty slices from a decoded wire envelope need not share the
    // static default's address. std.meta.eql compares slice pointers.
    inline for (@typeInfo(types.SearchRequest).@"struct".field_names, @typeInfo(types.SearchRequest).@"struct".field_types) |reflected_name, field_type| {
        switch (@typeInfo(field_type)) {
            .pointer => |pointer| if (pointer.size == .slice) {
                if (@field(remaining, reflected_name).len == 0 and @field(defaults, reflected_name).len == 0)
                    @field(remaining, reflected_name) = @field(defaults, reflected_name);
            },
            else => {},
        }
    }
    return std.meta.eql(remaining, defaults);
}

pub fn declineDenseSnapshot(reason: []const u8) ?SearchWithDenseProfileResult {
    if (benchQueryProfileEnabled()) std.log.info("antfly_bench_db_snapshot declined={s}", .{reason});
    return null;
}

pub fn trySnapshotDenseSearch(self: *DB, alloc: Allocator, req: types.SearchRequest, dense: types.DenseKnnQuery, include_profile: bool) !?SearchWithDenseProfileResult {
    if (comptime storage_engine.enabled) return storage_engine.call(@src().fn_name, .{ self, alloc, req, dense, include_profile });
    const manager = self.async_context.resource_manager orelse return null;
    if (!manager.dense_query_snapshot) return null;
    if (!snapshotDenseRequestEligible(req)) {
        if (benchQueryProfileEnabled()) {
            const defaults: types.SearchRequest = .{};
            inline for (comptime std.meta.fieldNames(types.SearchRequest)) |reflected_name| {
                if (!std.meta.eql(@field(req, reflected_name), @field(defaults, reflected_name)))
                    std.log.info("antfly_bench_db_snapshot nondefault_field={s}", .{reflected_name});
            }
        }
        return declineDenseSnapshot("request");
    }
    lockApplyShared(self);
    var apply_held = true;
    defer if (apply_held) self.core.unlockApplyShared();
    if (!self.beginPublishedDenseSearch()) return declineDenseSnapshot("catalog");
    defer self.endPublishedDenseSearch();
    // Publication can close admission while this query waits for apply.
    // Recheck under the same fence as the ordinary dense search path.
    try self.enforcePortableRuntimeGate();
    if (ttlDurationNs(self) != 0) return declineDenseSnapshot("ttl");
    const entry = (try denseIndexCallback(self, req.index_name)) orelse return error.IndexNotFound;
    if (entry.chunk_name != null or entry.embedding_names.len != 0) return declineDenseSnapshot("result_shape");
    const snapshot_req = try self.searchRequestAtCurrentIdentityGeneration(req);
    const summary = self.core.identity_visibility.summary orelse return declineDenseSnapshot("identity_summary");
    if (!doc_identity.allVisibleFromSummary(summary, snapshot_req.identity_read_generation)) return declineDenseSnapshot("visibility");
    try self.proveVectorSearchAccessPath(req.index_name, .dense_vector, false);
    var schema_view = if (req.document_authority_mode != .none) self.core.acquireSchemaView() else null;
    defer if (schema_view) |*view| view.release();
    var captured = (try self.core.index_manager.captureDenseReadSnapshotWithPrimary(entry, self.core.nextDerivedSequence(), req.document_authority_mode != .none)) orelse return declineDenseSnapshot("generation_bundle");
    defer captured.deinit();
    // Every mutable input is now captured; index lifetime remains protected
    // by the catalog lease until result metadata is fully materialized.
    self.core.unlockApplyShared();
    apply_held = false;
    captured.activate();
    defer captured.deactivate();
    var context: DenseSnapshotExecutor = .{ .snapshot = &captured, .identity_generation = snapshot_req.identity_read_generation.? };
    if (benchQueryProfileEnabled()) std.log.info("antfly_bench_db_snapshot captured=1 source_sequence={d} native_only={}", .{ captured.ann.sourceSequence(), captured.source.snapshot_native_only });
    const metric_name = entry.config.name;
    const started = platform_time.monotonicNs();
    errdefer observeSearchFailureMetric(metric_name, .vector, platform_time.monotonicNs() -| started);
    if (include_profile) {
        var result = db_query_search.searchDenseProfiled(alloc, snapshot_req, dense, context.executor()) catch |err| switch (err) {
            // Release the immutable bundle before retrying through the DB
            // path that owns durable repair and current-generation checks.
            error.IncompletePublishedSnapshot, error.DenseSnapshotNeedsPrimary => return null,
            else => return err,
        };
        errdefer result.result.deinit();
        if (snapshot_req.document_authority_mode != .none) {
            try externalizeSearchResultArtifactIds(alloc, &result.result);
            self.attachDistributedDocumentAuthorityAtSnapshot(alloc, snapshot_req, &result.result, try captured.documentReadTxn(), schema_view) catch |err| switch (err) {
                error.ReadSnapshotForkUnsupported => {
                    result.result.deinit();
                    return declineDenseSnapshot("primary_fork");
                },
                else => return err,
            };
        }
        result.profile.search_route = if (captured.source.snapshot_native_only) "hbc_snapshot_native" else "hbc_snapshot";
        return .{ .request = snapshot_req, .result = result.result, .dense_profile = result.profile };
    }
    var result = db_query_search.searchDense(alloc, snapshot_req, dense, context.executor()) catch |err| switch (err) {
        error.IncompletePublishedSnapshot, error.DenseSnapshotNeedsPrimary => return null,
        else => return err,
    };
    errdefer result.deinit();
    db_query_metrics.observeSortProfile(metric_name, .vector, platform_time.monotonicNs() -| started, result.sort_profile);
    try externalizeSearchResultArtifactIds(alloc, &result);
    if (snapshot_req.document_authority_mode != .none)
        self.attachDistributedDocumentAuthorityAtSnapshot(alloc, snapshot_req, &result, try captured.documentReadTxn(), schema_view) catch |err| switch (err) {
            error.ReadSnapshotForkUnsupported => {
                result.deinit();
                return declineDenseSnapshot("primary_fork");
            },
            else => return err,
        };
    return .{ .request = snapshot_req, .result = result, .dense_profile = null };
}

pub fn publishedDenseRequestEligible(req: types.SearchRequest) bool {
    if (req.document_authority_mode != .none) return false;
    // Graph execution must hold apply-shared from the retirement-gate
    // check through the entire read. The published-dense lease only pins
    // dense catalog entries and cannot fence a concurrent graph seal.
    if (req.query == .graph or req.graph_queries.len != 0 or
        req.graph_metric_queries.len != 0 or req.graph_metric_rerank != null or
        req.graph_query_transport != null) return false;
    if (req.full_text != null or req.sparse != null) return false;
    if (req.full_text_queries.len != 0 or req.dense_queries.len != 0 or req.sparse_queries.len != 0) return false;
    if (req.merge_config != null) return false;
    if (req.doc_filter_bindings.len != 0) return false;
    if (req.filter_query_json.len != 0 or req.exclusion_query_json.len != 0) return false;
    if (req.resolved_doc_filter != null) return false;
    if (req.filter_doc_ids_positive or req.filter_doc_ids.len != 0 or req.exclude_doc_ids.len != 0) return false;
    return req.dense != null or req.query == .dense_knn;
}

/// Selects and acquires the complete synchronization mode for one search.
/// When published-dense preflight discovers that the request needs the
/// apply lock, acquire that lock before releasing the catalog lease. This
/// closes the handoff gap in which a concurrent structural mutation could
/// delete the entry between the preflight lookup and the locked fallback.
pub fn beginDenseSearchAccess(self: *DB, req: types.SearchRequest) DenseSearchAccess {
    if (!publishedDenseRequestEligible(req) or !self.beginPublishedDenseSearch()) {
        lockApplyShared(self);
        return .apply_shared;
    }

    if (builtin.is_test) {
        if (db_zig.test_published_dense_catalog_lookup_hook) |hook| {
            hook.entered.set(hook.io);
            hook.release.waitUncancelable(hook.io);
        }
    }

    const entry = self.core.denseIndex(req.index_name) orelse {
        lockApplyShared(self);
        self.endPublishedDenseSearch();
        return .apply_shared;
    };
    if (entry.index.hasExternalVectorLoader()) {
        lockApplyShared(self);
        self.endPublishedDenseSearch();
        return .apply_shared;
    }
    return .published;
}

pub fn endDenseSearchAccess(self: *DB, access: DenseSearchAccess) void {
    switch (access) {
        .published => self.endPublishedDenseSearch(),
        .apply_shared => self.core.unlockApplyShared(),
    }
}

pub fn beginPublishedDenseSearch(self: *DB) bool {
    return self.core.index_manager.tryAcquireCatalogRead();
}

pub fn endPublishedDenseSearch(self: *DB) void {
    self.core.index_manager.releaseCatalogRead();
}

/// Nonblocking production-protocol microsteps used by VOPR. They expose
/// the lock-free reader/catalog-writer admission state machine without
/// copying the protocol into a test model or parking a deterministic
/// executor on native atomic waits.
pub fn beginPublishedDenseCaptureForVopr(self: *DB, index_name: []const u8) bool {
    if (!builtin.is_test) return false;
    if (!self.beginPublishedDenseSearch()) return false;
    if (self.core.denseIndex(index_name) == null) {
        self.endPublishedDenseSearch();
        return false;
    }
    return true;
}

pub fn endPublishedDenseCaptureForVopr(self: *DB) void {
    if (!builtin.is_test) return;
    self.endPublishedDenseSearch();
}

pub fn publishedDenseSearchCount(self: *const DB) u32 {
    return self.core.index_manager.published_dense_admission.load(.acquire) & published_dense_reader_mask;
}

pub fn denseDocKeyCallback(
    ctx: ?*anyopaque,
    index_name: []const u8,
    vector_id: u64,
) anyerror!?[]u8 {
    const self: *DB = @ptrCast(@alignCast(ctx orelse return error.InvalidArgument));
    return try self.core.index_manager.lookupDenseDocKey(self.core.store, index_name, vector_id);
}

pub fn denseVectorIdCallback(
    ctx: ?*anyopaque,
    index_name: []const u8,
    doc_key: []const u8,
) anyerror!?u64 {
    const self: *DB = @ptrCast(@alignCast(ctx orelse return error.InvalidArgument));
    return try self.core.index_manager.lookupDenseVectorId(self.core.store, index_name, doc_key);
}

test "db dense filter hydration only looks up required hit ordinals" {
    const alloc = std.testing.allocator;
    const Harness = struct {
        db: DB,
        lookup_calls: usize = 0,
        seen_generation: ?u64 = null,

        fn lookup(ctx: ?*anyopaque, allocator: Allocator, index_name: []const u8, vector_ids: []const u64, generation: ?u64) anyerror![]?doc_set.DocOrdinal {
            const db: *DB = @ptrCast(@alignCast(ctx.?));
            const self: *@This() = @fieldParentPtr("db", db);
            self.lookup_calls += 1;
            self.seen_generation = generation;
            return try denseOrdinalsForVectorIdsCallback(ctx, allocator, index_name, vector_ids, generation);
        }
    };
    var path_tmp = try TestDirectory.init("db");
    defer path_tmp.cleanup();
    const path = path_tmp.path().ptr;
    var harness = Harness{ .db = try DB.open(alloc, std.mem.span(path), .{ .start_index_workers = false }) };
    defer harness.db.close();
    const db = &harness.db;
    try db.addIndex(.{ .name = "ft_v1", .kind = .full_text, .config_json = "{}" });
    try db.addIndex(.{
        .name = "dv_v1",
        .kind = .dense_vector,
        .config_json = "{\"field\":\"embedding\",\"dims\":2,\"metric\":\"l2_squared\"}",
    });
    try db.batch(.{
        .writes = &.{
            .{ .key = "doc:a", .value = "{\"body\":\"alpha\",\"category\":\"keep\",\"embedding\":[0,0]}" },
            .{ .key = "doc:b", .value = "{\"body\":\"beta\",\"category\":\"reject\",\"embedding\":[1,0]}" },
            .{ .key = "doc:c", .value = "{\"body\":\"alpha\",\"category\":\"keep\",\"embedding\":[2,0]}" },
        },
        .sync_level = .full_index,
    });
    var filter = doc_set.ResolvedDocFilter{ .include = try db.resolveDocSetForIdsAlloc(alloc, &.{ "doc:a", "doc:c" }) };
    defer filter.deinit(alloc);
    const generation = try db.currentIdentityReadGenerationForRequest(null);
    // Exercise the real native collector and post-processor, counting the batch
    // identity read separately from the identity work that admits candidates.
    const executor = db_query_search.DenseSearchExecutor{
        .ctx = db,
        .text_index_entry = DB.textIndexEntryCallback,
        .dense_index = denseIndexCallback,
        .lookup_doc_key = denseDocKeyCallback,
        .resolve_hit_key = resolveDenseHitKeyCallback,
        .lookup_vector_id = denseVectorIdCallback,
        .lookup_vector_ids_for_ordinals = denseVectorIdsForOrdinalsCallback,
        .all_docs_visible_fast = DB.allDocsVisibleFastCallback,
        .lookup_doc_ordinal = DB.lookupLiveDocOrdinalNoLockCallback,
        .lookup_doc_ordinals = DB.lookupLiveDocOrdinalsNoLockCallback,
        .lookup_doc_ordinals_for_vector_ids = Harness.lookup,
        .resolve_doc_set_doc_ids = DB.resolveDocSetDocIdsCallback,
        .resolve_doc_ids_to_doc_set = DB.resolveDocIdsToDocSetCallback,
        .live_filter_doc_set = DB.liveFilterDocSetCallback,
        .nonvisible_doc_set = DB.nonVisibleDocSetCallback,
        .load_projected_document = DB.loadRequiredProjectedSearchDocumentCallback,
        .hbc_search = DB.hbcSearchCallback,
        .hbc_search_profiled = DB.hbcSearchProfiledCallback,
        .exact_dense_search = exactDenseSearchCallback,
        .postprocess = DB.postprocessVectorSearchResultCallback,
    };
    const Case = struct { req: types.SearchRequest, lookups: usize, without_text_index: bool = false };
    var removed_text_index = false;
    for ([_]Case{
        .{ .req = .{ .filter_text = .{ .match = .{ .field = "body", .text = "alpha" } } }, .lookups = 0 },
        .{ .req = .{ .exclusion_text = .{ .match = .{ .field = "body", .text = "beta" } } }, .lookups = 0 },
        .{ .req = .{ .filter_query_json = "{\"doc_id\":[\"doc:a\",\"doc:c\"]}" }, .lookups = 0 },
        .{ .req = .{ .exclusion_query_json = "{\"doc_id\":[\"doc:b\"]}" }, .lookups = 0 },
        .{ .req = .{ .filter_doc_ids = &.{ "doc:a", "doc:c" }, .filter_doc_ids_positive = true }, .lookups = 1 },
        .{ .req = .{ .exclude_doc_ids = &.{"doc:b"} }, .lookups = 1 },
        .{ .req = .{ .resolved_doc_filter = &filter }, .lookups = 1 },
        // Mix native ID constraints with residual stored predicates in both
        // positions. Removing resolved predicates must preserve hydration when
        // either remaining predicate still needs hit identity.
        .{ .req = .{ .filter_query_json = "{\"term\":{\"category\":\"keep\"}}", .exclusion_query_json = "{\"doc_id\":[\"doc:b\"]}" }, .lookups = 1, .without_text_index = true },
        .{ .req = .{ .filter_query_json = "{\"doc_id\":[\"doc:a\",\"doc:c\"]}", .exclusion_query_json = "{\"term\":{\"category\":\"reject\"}}" }, .lookups = 1, .without_text_index = true },
    }, 0..) |case, i| {
        errdefer std.debug.print("dense hydration case={d}\n", .{i});
        if (case.without_text_index and !removed_text_index) {
            try std.testing.expect(try db.deleteIndex("ft_v1"));
            removed_text_index = true;
        }
        var req = case.req;
        req.index_name = "dv_v1";
        req.primary_text_index_name = if (removed_text_index) null else "ft_v1";
        req.identity_read_generation = generation;
        req.include_stored = false;
        req.limit = 10;
        harness.lookup_calls = 0;
        harness.seen_generation = null;
        var result = try db_query_search.searchDense(alloc, req, .{ .vector = &.{ 0, 0 }, .k = 10 }, executor);
        defer result.deinit();
        try std.testing.expectEqual(case.lookups, harness.lookup_calls);
        try std.testing.expectEqual(@as(?u64, if (case.lookups == 0) null else generation), harness.seen_generation);
        try std.testing.expectEqual(@as(u32, 2), result.total_hits);
        try std.testing.expectEqual(types.TotalHitsRelation.exact, result.total_hits_relation);
        try std.testing.expectEqual(@as(usize, 2), result.hits.len);
        try std.testing.expectEqualStrings("doc:a", result.hits[0].id);
        try std.testing.expectEqualStrings("doc:c", result.hits[1].id);
        if (case.lookups > 0) {
            for (result.hits) |hit| try std.testing.expect(hit.doc_ordinal != null);
        }
    }
}

test "db dense fast path registers before catalog lookup during index deletion" {
    if (builtin.single_threaded or builtin.os.tag == .freestanding) return error.SkipZigTest;
    const alloc = std.testing.allocator;
    var io_impl = std.Io.Threaded.init(alloc, .{});
    defer io_impl.deinit();
    const io = io_impl.io();
    const wait_timeout_ns = 5 * std.time.ns_per_s;
    const EventWaiter = struct {
        fn wait(runtime_io: std.Io, event: *std.Io.Event, timeout_ns: u64) !void {
            const deadline_ns = monotonicTimeNs() +| timeout_ns;
            while (!event.isSet()) {
                const now_ns = monotonicTimeNs();
                if (now_ns >= deadline_ns) return error.TestTimeout;
                event.waitTimeout(runtime_io, .{ .duration = .{
                    .raw = std.Io.Duration.fromNanoseconds(@as(i96, deadline_ns - now_ns)),
                    .clock = .awake,
                } }) catch |err| switch (err) {
                    // Event waits permit spurious wakeups. Preserve the single
                    // absolute deadline rather than treating one as a failure.
                    error.Timeout => continue,
                    error.Canceled => return error.Canceled,
                };
            }
        }
    };
    var path_tmp = try TestDirectory.init("db");
    defer path_tmp.cleanup();
    const path = path_tmp.path().ptr;

    // Keep this fixture limited to the catalog-admission race under test.
    // Autonomous index and maintenance workers can legitimately retire an
    // empty index before the hook is reached, turning this into an unrelated
    // and intermittent IndexNotFound test failure.
    var db = try DB.open(alloc, std.mem.span(path), .{
        .start_index_workers = false,
        .start_optional_runtimes = false,
        .start_optional_runtime_workers = false,
        .ttl_cleanup = .{ .enabled = false },
    });
    defer db.close();
    try db.addIndex(.{
        .name = "dv_v1",
        .kind = .dense_vector,
        .config_json = "{\"field\":\"embedding\",\"dims\":2,\"metric\":\"l2_squared\"}",
    });

    var lookup_hook = PublishedDenseCatalogLookupTestHook{ .io = io };
    db_zig.test_published_dense_catalog_lookup_hook = &lookup_hook;
    defer {
        lookup_hook.release.set(io);
        db_zig.test_published_dense_catalog_lookup_hook = null;
    }

    const Searcher = struct {
        db: *DB,
        io: std.Io,
        completed: std.Io.Event = .unset,
        total_hits: u32 = 0,
        err: ?anyerror = null,

        fn run(searcher: *@This()) void {
            defer searcher.completed.set(searcher.io);
            var result = searcher.db.search(std.heap.smp_allocator, .{
                .index_name = "dv_v1",
                .limit = 1,
                .include_stored = false,
                .query = .{ .dense_knn = .{ .vector = &.{ 0.0, 0.0 }, .k = 1 } },
            }) catch |err| {
                searcher.err = err;
                return;
            };
            defer result.deinit();
            searcher.total_hits = result.total_hits;
        }
    };
    var searcher = Searcher{ .db = &db, .io = io };
    var search_future = try io.concurrent(Searcher.run, .{&searcher});
    var search_pending = true;
    defer if (search_pending) {
        lookup_hook.release.set(io);
        EventWaiter.wait(io, &searcher.completed, wait_timeout_ns) catch @panic("dense catalog lookup test search did not stop");
        _ = search_future.await(io);
    };

    try EventWaiter.wait(io, &lookup_hook.entered, wait_timeout_ns);
    try std.testing.expectEqual(@as(u32, 1), db.publishedDenseSearchCount());

    const Deleter = struct {
        db: *DB,
        io: std.Io,
        completed: std.Io.Event = .unset,
        removed: bool = false,
        err: ?anyerror = null,

        fn run(deleter: *@This()) void {
            defer deleter.completed.set(deleter.io);
            deleter.removed = deleter.db.deleteIndex("dv_v1") catch |err| {
                deleter.err = err;
                return;
            };
        }
    };
    var deleter = Deleter{ .db = &db, .io = io };
    var delete_future = try io.concurrent(Deleter.run, .{&deleter});
    var delete_pending = true;
    defer if (delete_pending) {
        lookup_hook.release.set(io);
        EventWaiter.wait(io, &deleter.completed, wait_timeout_ns) catch @panic("dense catalog lookup test deletion did not stop");
        _ = delete_future.await(io);
    };

    var barrier_entered = false;
    const barrier_deadline_ns = monotonicTimeNs() +| 5 * std.time.ns_per_s;
    while (monotonicTimeNs() < barrier_deadline_ns) {
        if (db.indexCatalogBarrierActive()) {
            barrier_entered = true;
            break;
        }
        try io.sleep(std.Io.Duration.fromMilliseconds(1), .awake);
    }
    if (!barrier_entered) return error.TestTimeout;
    // The deletion must wait for the already-registered lookup rather than
    // moving or freeing the dense entry underneath it.
    try std.testing.expect(!searcher.completed.isSet());
    try std.testing.expect(!deleter.completed.isSet());

    lookup_hook.release.set(io);
    try EventWaiter.wait(io, &searcher.completed, wait_timeout_ns);
    try EventWaiter.wait(io, &deleter.completed, wait_timeout_ns);
    _ = search_future.await(io);
    search_pending = false;
    _ = delete_future.await(io);
    delete_pending = false;

    if (searcher.err) |err| return err;
    if (deleter.err) |err| return err;
    try std.testing.expectEqual(@as(u32, 0), searcher.total_hits);
    try std.testing.expect(deleter.removed);
    try std.testing.expect(!db.hasIndex("dv_v1"));
}

test "db dense index stores stable vector ids with ordinal filter mappings" {
    const alloc = std.testing.allocator;
    var path_tmp = try TestDirectory.init("db");
    defer path_tmp.cleanup();
    const path = path_tmp.path().ptr;

    var db = try DB.open(alloc, std.mem.span(path), .{});
    defer db.close();

    try db.addIndex(.{
        .name = "dv_v1",
        .kind = .dense_vector,
        .config_json = "{\"field\":\"embedding\",\"dims\":2,\"metric\":\"l2_squared\"}",
    });

    try db.batch(.{
        .writes = &.{
            .{ .key = "doc:a", .value = "{\"embedding\":[0,0]}" },
            .{ .key = "doc:b", .value = "{\"embedding\":[10,0]}" },
        },
        .sync_level = .full_index,
    });

    var include = try db.resolveDocSetForIdsAlloc(alloc, &.{"doc:b"});
    errdefer include.deinit(alloc);
    const ordinal = switch (include) {
        .ordinals => |ordinals| blk: {
            try std.testing.expectEqual(@as(usize, 1), ordinals.len);
            break :blk ordinals[0];
        },
        else => return error.ExpectedOrdinalDocSet,
    };
    try std.testing.expectEqual(@as(doc_set.DocOrdinal, 2), ordinal);
    const expected_vector_id = denseTestVectorId("doc:b");
    try std.testing.expect(expected_vector_id != @as(u64, ordinal));
    try std.testing.expectEqual(@as(?u64, expected_vector_id), try db.core.index_manager.lookupDenseVectorId(db.core.store, "dv_v1", "doc:b"));

    var filter = doc_set.ResolvedDocFilter{
        .include = include,
        .exclude = .none,
    };
    include = .all;
    defer filter.deinit(alloc);

    try std.testing.expect(!DB.publishedDenseRequestEligible(.{
        .index_name = "dv_v1",
        .limit = 1,
        .include_stored = false,
        .resolved_doc_filter = &filter,
        .query = .{ .dense_knn = .{ .vector = &.{ 0.0, 0.0 }, .k = 1 } },
    }));

    var dense_filtered = try db.searchDenseProfiled(alloc, .{
        .index_name = "dv_v1",
        .limit = 1,
        .include_stored = false,
        .resolved_doc_filter = &filter,
    }, .{ .vector = &.{ 0.0, 0.0 }, .k = 1 });
    defer dense_filtered.result.deinit();
    try std.testing.expectEqual(@as(u32, 1), dense_filtered.result.total_hits);
    try std.testing.expectEqual(@as(usize, 1), dense_filtered.result.hits.len);
    try std.testing.expectEqualStrings("doc:b", dense_filtered.result.hits[0].id);
    try std.testing.expectEqual(@as(?doc_set.DocOrdinal, ordinal), dense_filtered.result.hits[0].doc_ordinal);
    try std.testing.expectEqual(@as(u32, 1), dense_filtered.profile.raw_hit_count);

    const reassigned_namespace = doc_identity.Namespace{ .table_id = 19, .shard_id = 1902, .range_id = 19002 };
    try db.reassignIdentityNamespaceForInternalTransition(reassigned_namespace);
    try std.testing.expectEqual(@as(?u64, expected_vector_id), try db.core.index_manager.lookupDenseVectorId(db.core.store, "dv_v1", "doc:b"));

    {
        var txn = try db.core.store.beginProbeTxn();
        defer txn.abort();
        const state = (try doc_identity.lookupStateTxn(&txn, ordinal)) orelse return error.TestUnexpectedResult;
        try std.testing.expectEqual(doc_identity.canonicalDocIdForNamespace(reassigned_namespace, "doc:b"), state.canonical_doc_id);
    }

    var dense_after_reassign = try db.searchDenseProfiled(alloc, .{
        .index_name = "dv_v1",
        .limit = 1,
        .include_stored = false,
        .resolved_doc_filter = &filter,
    }, .{ .vector = &.{ 0.0, 0.0 }, .k = 1 });
    defer dense_after_reassign.result.deinit();
    try std.testing.expectEqual(@as(u32, 1), dense_after_reassign.result.total_hits);
    try std.testing.expectEqual(@as(usize, 1), dense_after_reassign.result.hits.len);
    try std.testing.expectEqualStrings("doc:b", dense_after_reassign.result.hits[0].id);
    try std.testing.expectEqual(@as(?doc_set.DocOrdinal, ordinal), dense_after_reassign.result.hits[0].doc_ordinal);
    try std.testing.expectEqual(@as(u32, 1), dense_after_reassign.profile.raw_hit_count);
}

test "db dense default dynamic 0.2 percent numeric filter exact scores bounded candidates" {
    const alloc = std.testing.allocator;
    const table_schema_api = public_table_schema;

    var path_tmp = try TestDirectory.init("db");
    defer path_tmp.cleanup();
    const path = path_tmp.path().ptr;

    var db = try DB.open(alloc, std.mem.span(path), .{});
    defer db.close();

    const default_schema_json =
        \\{"version":0,"default_type":"doc","enforce_types":false,"document_schemas":{"doc":{"schema":{"type":"object","additionalProperties":true,"x-antfly-dynamic-indexing":{"mode":"infer_types"}}}}}
    ;
    var parsed_schema = try table_schema_api.parseValidatedTableSchema(alloc, default_schema_json);
    defer parsed_schema.deinit(alloc);
    const runtime_schema = try table_schema_api.deriveRuntimeTableSchema(alloc, parsed_schema);
    defer schema_mod.freeSchema(alloc, runtime_schema);
    try db.setSchema(runtime_schema);

    try db.addIndex(.{
        .name = "ft_v1",
        .kind = .full_text,
        .config_json = "{}",
    });
    try db.addIndex(.{
        .name = "dv_v1",
        .kind = .dense_vector,
        .config_json = "{\"field\":\"embedding\",\"dims\":2,\"metric\":\"l2_squared\"}",
    });

    const doc_count: usize = 10_000;
    // Reproduce the campaign's 0.2% selectivity cell. The public page still
    // asks for top-100, while native planning must score only the 20 eligible
    // vectors and return exact ordering instead of traversing filtered ANN.
    const eligible_count: usize = 20;
    const writes = try alloc.alloc(types.BatchWrite, doc_count);
    defer {
        for (writes) |write| {
            alloc.free(@constCast(write.key));
            alloc.free(@constCast(write.value));
        }
        alloc.free(writes);
    }
    for (writes, 0..) |*write, i| {
        write.* = .{
            .key = try std.fmt.allocPrint(alloc, "doc:{d:0>5}", .{i}),
            .value = try std.fmt.allocPrint(
                alloc,
                "{{\"id\":{d},\"bucket\":\"{s}\",\"embedding\":[{d},0]}}",
                .{ i, if (i >= doc_count - eligible_count) "selected" else "other", i },
            ),
        };
    }
    try db.batch(.{
        .writes = writes,
        .sync_level = .full_index,
    });

    const VectorLoadCounter = struct {
        count: usize = 0,

        fn onLoad(ctx_ptr: ?*anyopaque, _: *hbc_mod.HBCIndex, _: u64) void {
            const self: *@This() = @ptrCast(@alignCast(ctx_ptr.?));
            self.count += 1;
        }
    };
    var vector_load_counter = VectorLoadCounter{};
    hbc_mod.setTestGetVectorViewOrScratchHook(&vector_load_counter, VectorLoadCounter.onLoad);
    defer hbc_mod.setTestGetVectorViewOrScratchHook(null, null);

    const cases = [_]struct {
        filter_query_json: []const u8,
        exclusion_query_json: []const u8 = "",
        filter_prefix: []const u8 = "",
        expected_candidates: usize = eligible_count,
        expected_scored: usize = eligible_count,
        expected_route: []const u8 = "exact_native_filter",
        expected_route_reason: []const u8 = "",
    }{
        .{ .filter_query_json = "{\"numeric_range\":{\"field\":\"id\",\"min\":9980,\"inclusive_min\":true}}" },
        .{ .filter_query_json = "{\"term\":{\"bucket\":\"selected\"}}" },
        // Prefix metadata is resolved before vector IO. Only the ten matching
        // candidates should incur a distance evaluation.
        .{
            .filter_query_json = "{\"numeric_range\":{\"field\":\"id\",\"min\":9980,\"inclusive_min\":true}}",
            .filter_prefix = "doc:0999",
            .expected_candidates = 10,
            .expected_scored = 10,
        },
        // Exercise a highly selective positive filter together with a large,
        // disjoint native exclusion set. Exact scoring must subtract the two
        // ordered sets linearly instead of probing every exclusion for every
        // candidate.
        .{
            .filter_query_json = "{\"numeric_range\":{\"field\":\"id\",\"min\":9980,\"inclusive_min\":true}}",
            .exclusion_query_json = "{\"numeric_range\":{\"field\":\"id\",\"max\":9979,\"inclusive_max\":true}}",
        },
        // Overlap half of the positive set while retaining the same large
        // exclusion range. Telemetry must report actual distance evaluations,
        // not the pre-exclusion planner cardinality.
        .{
            .filter_query_json = "{\"numeric_range\":{\"field\":\"id\",\"min\":9980,\"inclusive_min\":true}}",
            .exclusion_query_json = "{\"numeric_range\":{\"field\":\"id\",\"max\":9989,\"inclusive_max\":true}}",
            .expected_scored = 10,
        },
        // A completely excluded native filter is authoritative and should
        // return immediately without entering either exact scoring or ANN.
        .{
            .filter_query_json = "{\"numeric_range\":{\"field\":\"id\",\"min\":9980,\"inclusive_min\":true}}",
            .exclusion_query_json = "{\"numeric_range\":{\"field\":\"id\",\"min\":9980,\"inclusive_min\":true}}",
            .expected_scored = 0,
            .expected_route = "empty_result",
            .expected_route_reason = "empty_native_filter",
        },
        // The positive set alone exceeds the exact-scoring budget, but the
        // ordered exclusion leaves only 20 effective candidates. Route on the
        // allocation-free set-difference cardinality instead of falling back
        // to filtered ANN for this highly selective result.
        .{
            .filter_query_json = "{\"numeric_range\":{\"field\":\"id\",\"min\":6000,\"inclusive_min\":true}}",
            .exclusion_query_json = "{\"numeric_range\":{\"field\":\"id\",\"max\":9979,\"inclusive_max\":true}}",
            .expected_candidates = 4000,
        },
    };
    for (cases, 0..) |case, case_index| {
        var profiled = try db.searchDenseProfiled(alloc, .{
            .index_name = "dv_v1",
            .primary_text_index_name = "ft_v1",
            .limit = 100,
            .include_stored = false,
            .filter_prefix = case.filter_prefix,
            .filter_query_json = case.filter_query_json,
            .exclusion_query_json = case.exclusion_query_json,
        }, .{ .vector = &.{ 9999.0, 0.0 }, .k = 100 });
        defer profiled.result.deinit();

        try std.testing.expectEqual(@as(u32, @intCast(case.expected_scored)), profiled.result.total_hits);
        try std.testing.expectEqual(case.expected_scored, profiled.result.hits.len);
        try std.testing.expectEqual(@as(u64, @intCast(case.expected_candidates)), profiled.profile.native_filter_candidate_count);
        try std.testing.expectEqual(@as(u64, @intCast(case.expected_scored)), profiled.profile.hbc_exact_vectors_scored);
        try std.testing.expectEqualStrings(case.expected_route, profiled.profile.search_route);
        const expected_route_reason = if (case.expected_route_reason.len > 0)
            case.expected_route_reason
        else if (case_index == 0)
            "exact_prior_work_lower"
        else
            "exact_measured_work_lower";
        try std.testing.expectEqualStrings(expected_route_reason, profiled.profile.route_reason);
        try std.testing.expectEqual(@as(u64, 0), profiled.profile.exact_raw_scalar_reads);
        try std.testing.expectEqual(@as(u64, 0), profiled.profile.exact_request_vector_cache_entries);
        try std.testing.expectEqual(@as(usize, 0), vector_load_counter.count);
        for (profiled.result.hits, 0..) |hit, rank| {
            const expected = try std.fmt.allocPrint(alloc, "doc:{d:0>5}", .{doc_count - 1 - rank});
            defer alloc.free(expected);
            try std.testing.expectEqualStrings(expected, hit.id);
        }
    }
    try std.testing.expectEqual(@as(usize, 0), vector_load_counter.count);
}

test "db dense and sparse vector searches apply stored symbolic filters before final paging" {
    const alloc = std.testing.allocator;

    var path_tmp = try TestDirectory.init("db");
    defer path_tmp.cleanup();
    const path = path_tmp.path().ptr;

    var db = try DB.open(alloc, std.mem.span(path), .{});
    defer db.close();

    try db.addIndex(.{
        .name = "dv_v1",
        .kind = .dense_vector,
        .config_json = "{\"field\":\"embedding\",\"dims\":2,\"metric\":\"l2_squared\"}",
    });
    try db.addIndex(.{
        .name = "sp_v1",
        .kind = .sparse_vector,
        .config_json = "{\"field\":\"sparse\"}",
    });

    try db.batch(.{
        .writes = &.{
            .{ .key = "doc:a", .value = "{\"category\":\"reject\",\"embedding\":[0,0],\"sparse\":{\"indices\":[1],\"values\":[1.0]}}" },
            .{ .key = "doc:b", .value = "{\"category\":\"keep\",\"embedding\":[10,0],\"sparse\":{\"indices\":[1],\"values\":[0.1]}}" },
        },
        .sync_level = .full_index,
    });

    var dense_result = try db.search(alloc, .{
        .index_name = "dv_v1",
        .dense = .{ .vector = &.{ 0.0, 0.0 }, .k = 1 },
        .limit = 1,
        .include_stored = false,
        .filter_query_json = "{\"term\":{\"category\":\"keep\"}}",
    });
    defer dense_result.deinit();
    try std.testing.expectEqual(@as(u32, 1), dense_result.total_hits);
    try std.testing.expectEqual(@as(usize, 1), dense_result.hits.len);
    try std.testing.expectEqualStrings("doc:b", dense_result.hits[0].id);
    {
        var txn = try db.core.store.beginProbeTxn();
        defer txn.abort();
        try std.testing.expectEqual(try doc_identity.lookupOrdinalTxn(alloc, &txn, "doc:b"), dense_result.hits[0].doc_ordinal);
    }

    var sparse_result = try db.search(alloc, .{
        .index_name = "sp_v1",
        .query = .{ .sparse_knn = .{
            .indices = &.{1},
            .values = &.{1.0},
            .k = 1,
        } },
        .limit = 1,
        .include_stored = false,
        .filter_query_json = "{\"term\":{\"category\":\"keep\"}}",
    });
    defer sparse_result.deinit();
    try std.testing.expectEqual(@as(u32, 1), sparse_result.total_hits);
    try std.testing.expectEqual(@as(usize, 1), sparse_result.hits.len);
    try std.testing.expectEqualStrings("doc:b", sparse_result.hits[0].id);
}

test "db dense stored symbolic filter candidate window covers offset pagination" {
    // Keep leak checks and failure injection; allocation backtraces are opt-in.
    var allocator_state: std.heap.SafeAllocator = .init(std.heap.page_allocator, .{ .stack_trace_frames = 0 });
    defer std.debug.assert(allocator_state.deinit() == 0);
    const alloc = if (platform.env.getenvBool("ANTFLY_TEST_ALLOCATOR_TRACES")) std.testing.allocator else allocator_state.allocator();

    var path_tmp = try TestDirectory.init("db");
    defer path_tmp.cleanup();
    const path = path_tmp.path().ptr;

    var db = try DB.open(alloc, std.mem.span(path), .{});
    defer db.close();

    try db.addIndex(.{
        .name = "dv_v1",
        .kind = .dense_vector,
        .config_json = "{\"field\":\"embedding\",\"dims\":2,\"metric\":\"l2_squared\"}",
    });

    var writes = std.ArrayListUnmanaged(types.BatchWrite).empty;
    defer {
        for (writes.items) |write| {
            alloc.free(write.key);
            alloc.free(write.value);
        }
        writes.deinit(alloc);
    }
    try writes.ensureTotalCapacity(alloc, 2200);
    for (0..2200) |i| {
        try writes.append(alloc, .{
            .key = try std.fmt.allocPrint(alloc, "doc:{d:0>4}", .{i}),
            .value = try std.fmt.allocPrint(alloc, "{{\"body\":\"keep\",\"embedding\":[{d},0]}}", .{i}),
        });
    }

    try db.batch(.{
        .writes = writes.items,
        .sync_level = .full_index,
    });

    const active_dense_count: u32 = @intCast(db.core.denseIndex("dv_v1").?.index.stats().active_count);
    try std.testing.expect(active_dense_count > 1024);

    var dense_result = try db.searchDenseProfiled(alloc, .{
        .index_name = "dv_v1",
        .limit = 1,
        .offset = 1024,
        .include_stored = false,
        .filter_query_json = "{\"term\":{\"field\":\"body\",\"term\":\"keep\"}}",
    }, .{ .vector = &.{ 0.0, 0.0 }, .k = 1 });
    defer dense_result.result.deinit();

    try std.testing.expectEqual(@as(usize, 1), dense_result.result.hits.len);
    try std.testing.expectEqual(@as(u32, 1025), dense_result.result.total_hits);
    try std.testing.expectEqual(types.TotalHitsRelation.gte, dense_result.result.total_hits_relation);
    try std.testing.expectEqual(@as(u32, 1025), dense_result.profile.raw_hit_count);
}

test "db dense algebraic doc facts feed native dense and sparse symbolic filters" {
    const alloc = std.testing.allocator;

    var path_tmp = try TestDirectory.init("db");
    defer path_tmp.cleanup();
    const path = path_tmp.path().ptr;

    var db = try DB.open(alloc, std.mem.span(path), .{});
    defer db.close();

    try db.addIndex(.{
        .name = "dv_v1",
        .kind = .dense_vector,
        .config_json = "{\"field\":\"embedding\",\"dims\":2,\"metric\":\"l2_squared\"}",
    });
    try db.addIndex(.{
        .name = "sp_v1",
        .kind = .sparse_vector,
        .config_json = "{\"field\":\"sparse\"}",
    });
    try db.addIndex(.{
        .name = "ft_v1",
        .kind = .full_text,
        .config_json = "{}",
    });
    try db.addIndex(.{
        .name = "alg",
        .kind = .algebraic,
        .config_json =
        \\{
        \\  "version": 1,
        \\  "table": "docs",
        \\  "group_fields": [
        \\    {"name":"category","path":"category","type":"string"},
        \\    {"name":"published","path":"published","type":"boolean"},
        \\    {"name":"ip","path":"ip","type":"string"},
        \\    {"name":"score","path":"score","type":"number"}
        \\  ],
        \\  "measure_fields": [
        \\    {"name":"code","path":"code","type":"string"}
        \\  ],
        \\  "materializations": [{"name":"count_by_category","op":"count","group_by":["category"]}]
        \\}
        ,
    });

    try db.batch(.{
        .writes = &.{
            .{ .key = "doc:a", .value = "{\"body\":\"shared token\",\"category\":\"reject\",\"published\":false,\"ip\":\"192.168.1.10\",\"score\":1.0,\"code\":\"drop\",\"meta\":{\"tier\":\"bronze\"},\"location\":{\"lat\":40.7128,\"lon\":-74.0060},\"embedding\":[0,0],\"sparse\":{\"indices\":[1],\"values\":[1.0]}}" },
            .{ .key = "doc:b", .value = "{\"body\":\"shared token\",\"category\":\"keep\",\"published\":true,\"ip\":\"10.1.2.3\",\"score\":5.0,\"code\":\"keep-code\",\"meta\":{\"tier\":\"gold\"},\"location\":{\"lat\":37.7749,\"lon\":-122.4194},\"embedding\":[10,0],\"sparse\":{\"indices\":[1],\"values\":[0.1]}}" },
        },
        .sync_level = .full_index,
    });

    const alg_entry = db.core.index_manager.algebraicIndex("alg") orelse return error.TestUnexpectedResult;
    const keep_doc_ids = (try alg_entry.index.docIdsForFilterJsonAlloc(db.core.store, "{\"term\":{\"category\":\"keep\"}}")) orelse return error.TestUnexpectedResult;
    defer alg_entry.index.freeDocIds(keep_doc_ids);
    try std.testing.expectEqual(@as(usize, 1), keep_doc_ids.len);
    try std.testing.expectEqualStrings("doc:b", keep_doc_ids[0]);
    try std.testing.expect((try db.core.index_manager.lookupDenseVectorId(db.core.store, "dv_v1", "doc:b")) != null);

    const location_lat_ids = (try alg_entry.index.docIdsForFilterJsonAlloc(db.core.store, "{\"numeric_range\":{\"path\":\"/location/lat\",\"min\":37.0,\"max\":38.0}}")) orelse return error.TestUnexpectedResult;
    defer alg_entry.index.freeDocIds(location_lat_ids);
    try std.testing.expectEqual(@as(usize, 1), location_lat_ids.len);
    try std.testing.expectEqualStrings("doc:b", location_lat_ids[0]);

    const geo_doc_ids = (try alg_entry.index.docIdsForFilterJsonAlloc(db.core.store, "{\"geo_distance\":{\"path\":\"/location\",\"lat\":37.7749,\"lon\":-122.4194,\"radius_meters\":2000}}")) orelse return error.TestUnexpectedResult;
    defer alg_entry.index.freeDocIds(geo_doc_ids);
    try std.testing.expectEqual(@as(usize, 1), geo_doc_ids.len);
    try std.testing.expectEqualStrings("doc:b", geo_doc_ids[0]);

    const geo_shape_doc_ids = (try alg_entry.index.docIdsForFilterJsonAlloc(db.core.store, "{\"geo_shape\":{\"path\":\"/location\",\"relation\":\"intersects\",\"polygons\":[[{\"lat\":37.0,\"lon\":-123.0},{\"lat\":38.0,\"lon\":-123.0},{\"lat\":38.0,\"lon\":-122.0},{\"lat\":37.0,\"lon\":-122.0}]]}}")) orelse return error.TestUnexpectedResult;
    defer alg_entry.index.freeDocIds(geo_shape_doc_ids);
    try std.testing.expectEqual(@as(usize, 1), geo_shape_doc_ids.len);
    try std.testing.expectEqualStrings("doc:b", geo_shape_doc_ids[0]);

    var dense_keep = try db.searchDenseProfiled(alloc, .{
        .index_name = "dv_v1",
        .limit = 1,
        .include_stored = false,
        .filter_query_json = "{\"term\":{\"category\":\"keep\"}}",
        .exclusion_query_json = "{\"term\":{\"path\":\"/meta/tier\",\"value\":\"bronze\"}}",
        .require_algebraic_filter_resolution = true,
    }, .{ .vector = &.{ 0.0, 0.0 }, .k = 1 });
    defer dense_keep.result.deinit();
    try std.testing.expectEqual(@as(u32, 1), dense_keep.result.total_hits);
    try std.testing.expectEqual(@as(usize, 1), dense_keep.result.hits.len);
    try std.testing.expectEqualStrings("doc:b", dense_keep.result.hits[0].id);
    try std.testing.expectEqual(@as(u32, 1), dense_keep.profile.raw_hit_count);
    {
        const status_value = alg_entry.index.status();
        try std.testing.expectEqual(@as(u64, 1), status_value.vector_filter_attempt_count);
        try std.testing.expectEqual(@as(u64, 1), status_value.vector_filter_resolved_count);
        try std.testing.expectEqual(@as(u64, 1), status_value.vector_filter_include_doc_id_count);
        try std.testing.expectEqual(@as(u64, 1), status_value.vector_filter_exclude_doc_id_count);
    }

    try std.testing.expectError(error.UnsupportedQueryRequest, db.searchDenseProfiled(alloc, .{
        .index_name = "dv_v1",
        .limit = 1,
        .include_stored = false,
        .exclusion_query_json = "{\"wildcard\":{\"/meta/tier\":\"*old\"}}",
        .require_algebraic_filter_resolution = true,
    }, .{ .vector = &.{ 0.0, 0.0 }, .k = 1 }));
    {
        const status_value = alg_entry.index.status();
        try std.testing.expectEqual(@as(u64, 2), status_value.vector_filter_attempt_count);
        try std.testing.expectEqual(@as(u64, 1), status_value.vector_filter_unsupported_count);
        try std.testing.expectEqual(@as(u64, 1), status_value.vector_filter_fail_closed_count);
    }
    {
        const stats = try db.stats(alloc);
        defer types.freeDBStats(alloc, stats);
        try std.testing.expect(stats.doc_set_planning.unsupported_filter_shape_count >= 1);
    }

    const binding_defs = [_]types.NamedDocFilterBinding{
        .{ .name = "kept", .filter_query_json = "{\"term\":{\"category\":\"keep\"}}" },
        .{ .name = "published", .filter_query_json = "{\"bool_field\":{\"field\":\"published\",\"value\":true}}" },
    };
    var sparse_with_binding = try db.search(alloc, .{
        .index_name = "sp_v1",
        .query = .{ .sparse_knn = .{
            .indices = &.{1},
            .values = &.{1.0},
            .k = 2,
        } },
        .limit = 2,
        .include_stored = false,
        .doc_filter_bindings = binding_defs[0..],
        .filter_query_json = "{\"bool\":{\"must\":[{\"ref\":\"kept\"},{\"ref\":\"published\"}]}}",
        .require_algebraic_filter_resolution = true,
    });
    defer sparse_with_binding.deinit();
    try std.testing.expectEqual(@as(u32, 1), sparse_with_binding.total_hits);
    try std.testing.expectEqual(@as(usize, 1), sparse_with_binding.hits.len);
    try std.testing.expectEqualStrings("doc:b", sparse_with_binding.hits[0].id);

    var full_text_with_binding = try db.search(alloc, .{
        .index_name = "ft_v1",
        .full_text = .{ .match = .{ .field = "body", .text = "shared" } },
        .limit = 2,
        .include_stored = false,
        .doc_filter_bindings = binding_defs[0..],
        .filter_query_json = "{\"bool\":{\"must\":[{\"ref\":\"kept\"},{\"ref\":\"published\"}]}}",
        .require_algebraic_filter_resolution = true,
    });
    defer full_text_with_binding.deinit();
    try std.testing.expectEqual(@as(u32, 1), full_text_with_binding.total_hits);
    try std.testing.expectEqual(@as(usize, 1), full_text_with_binding.hits.len);
    try std.testing.expectEqualStrings("doc:b", full_text_with_binding.hits[0].id);
    {
        const status_value = alg_entry.index.status();
        try std.testing.expectEqual(@as(u64, 4), status_value.vector_filter_attempt_count);
        try std.testing.expectEqual(@as(u64, 3), status_value.vector_filter_resolved_count);
    }

    var full_text_direct_algebraic = try db.search(alloc, .{
        .index_name = "ft_v1",
        .full_text = .{ .match = .{ .field = "body", .text = "shared" } },
        .limit = 2,
        .include_stored = false,
        .filter_query_json = "{\"term\":{\"category\":\"keep\"}}",
        .require_algebraic_filter_resolution = true,
    });
    defer full_text_direct_algebraic.deinit();
    try std.testing.expectEqual(@as(u32, 1), full_text_direct_algebraic.total_hits);
    try std.testing.expectEqual(@as(usize, 1), full_text_direct_algebraic.hits.len);
    try std.testing.expectEqualStrings("doc:b", full_text_direct_algebraic.hits[0].id);
    {
        const status_value = alg_entry.index.status();
        try std.testing.expectEqual(@as(u64, 5), status_value.vector_filter_attempt_count);
        try std.testing.expectEqual(@as(u64, 4), status_value.vector_filter_resolved_count);
        try std.testing.expectEqual(@as(u64, 1), status_value.vector_filter_unsupported_count);
    }

    var dense_terms = try db.searchDenseProfiled(alloc, .{
        .index_name = "dv_v1",
        .limit = 1,
        .include_stored = false,
        .filter_query_json = "{\"terms\":{\"field\":\"category\",\"values\":[\"missing\",\"keep\"]}}",
    }, .{ .vector = &.{ 0.0, 0.0 }, .k = 1 });
    defer dense_terms.result.deinit();
    try std.testing.expectEqual(@as(u32, 1), dense_terms.result.total_hits);
    try std.testing.expectEqual(@as(usize, 1), dense_terms.result.hits.len);
    try std.testing.expectEqualStrings("doc:b", dense_terms.result.hits[0].id);
    try std.testing.expectEqual(@as(u32, 1), dense_terms.profile.raw_hit_count);

    var dense_score_range = try db.searchDenseProfiled(alloc, .{
        .index_name = "dv_v1",
        .limit = 1,
        .include_stored = false,
        .filter_query_json = "{\"numeric_range\":{\"field\":\"score\",\"min\":2.0}}",
    }, .{ .vector = &.{ 0.0, 0.0 }, .k = 1 });
    defer dense_score_range.result.deinit();
    try std.testing.expectEqual(@as(u32, 1), dense_score_range.result.total_hits);
    try std.testing.expectEqual(@as(usize, 1), dense_score_range.result.hits.len);
    try std.testing.expectEqualStrings("doc:b", dense_score_range.result.hits[0].id);
    try std.testing.expectEqual(@as(u32, 1), dense_score_range.profile.raw_hit_count);

    var dense_standard_score_range = try db.searchDenseProfiled(alloc, .{
        .index_name = "dv_v1",
        .limit = 1,
        .include_stored = false,
        .filter_query_json = "{\"range\":{\"score\":{\"gte\":2.0}}}",
    }, .{ .vector = &.{ 0.0, 0.0 }, .k = 1 });
    defer dense_standard_score_range.result.deinit();
    try std.testing.expectEqual(@as(u32, 1), dense_standard_score_range.result.total_hits);
    try std.testing.expectEqual(@as(usize, 1), dense_standard_score_range.result.hits.len);
    try std.testing.expectEqualStrings("doc:b", dense_standard_score_range.result.hits[0].id);
    try std.testing.expectEqual(@as(u32, 1), dense_standard_score_range.profile.raw_hit_count);

    var dense_bool_field = try db.searchDenseProfiled(alloc, .{
        .index_name = "dv_v1",
        .limit = 1,
        .include_stored = false,
        .filter_query_json = "{\"bool_field\":{\"field\":\"published\",\"value\":true}}",
    }, .{ .vector = &.{ 0.0, 0.0 }, .k = 1 });
    defer dense_bool_field.result.deinit();
    try std.testing.expectEqual(@as(u32, 1), dense_bool_field.result.total_hits);
    try std.testing.expectEqual(@as(usize, 1), dense_bool_field.result.hits.len);
    try std.testing.expectEqualStrings("doc:b", dense_bool_field.result.hits[0].id);
    try std.testing.expectEqual(@as(u32, 1), dense_bool_field.profile.raw_hit_count);

    var dense_measure_prefix = try db.searchDenseProfiled(alloc, .{
        .index_name = "dv_v1",
        .limit = 1,
        .include_stored = false,
        .filter_query_json = "{\"prefix\":{\"field\":\"code\",\"role\":\"measure\",\"value\":\"keep\"}}",
    }, .{ .vector = &.{ 0.0, 0.0 }, .k = 1 });
    defer dense_measure_prefix.result.deinit();
    try std.testing.expectEqual(@as(u32, 1), dense_measure_prefix.result.total_hits);
    try std.testing.expectEqual(@as(usize, 1), dense_measure_prefix.result.hits.len);
    try std.testing.expectEqualStrings("doc:b", dense_measure_prefix.result.hits[0].id);
    try std.testing.expectEqual(@as(u32, 1), dense_measure_prefix.profile.raw_hit_count);

    var dense_measure_wildcard = try db.searchDenseProfiled(alloc, .{
        .index_name = "dv_v1",
        .limit = 1,
        .include_stored = false,
        .filter_query_json = "{\"wildcard\":{\"field\":\"code\",\"role\":\"measure\",\"pattern\":\"keep-*\"}}",
    }, .{ .vector = &.{ 0.0, 0.0 }, .k = 1 });
    defer dense_measure_wildcard.result.deinit();
    try std.testing.expectEqual(@as(u32, 1), dense_measure_wildcard.result.total_hits);
    try std.testing.expectEqual(@as(usize, 1), dense_measure_wildcard.result.hits.len);
    try std.testing.expectEqualStrings("doc:b", dense_measure_wildcard.result.hits[0].id);
    try std.testing.expectEqual(@as(u32, 1), dense_measure_wildcard.profile.raw_hit_count);

    var dense_measure_regexp = try db.searchDenseProfiled(alloc, .{
        .index_name = "dv_v1",
        .limit = 1,
        .include_stored = false,
        .filter_query_json = "{\"regexp\":{\"field\":\"code\",\"role\":\"measure\",\"pattern\":\"keep-.*\"}}",
    }, .{ .vector = &.{ 0.0, 0.0 }, .k = 1 });
    defer dense_measure_regexp.result.deinit();
    try std.testing.expectEqual(@as(u32, 1), dense_measure_regexp.result.total_hits);
    try std.testing.expectEqual(@as(usize, 1), dense_measure_regexp.result.hits.len);
    try std.testing.expectEqualStrings("doc:b", dense_measure_regexp.result.hits[0].id);
    try std.testing.expectEqual(@as(u32, 1), dense_measure_regexp.profile.raw_hit_count);

    var dense_fuzzy = try db.searchDenseProfiled(alloc, .{
        .index_name = "dv_v1",
        .limit = 1,
        .include_stored = false,
        .filter_query_json = "{\"fuzzy\":{\"field\":\"code\",\"role\":\"measure\",\"query\":\"keep-cide\",\"max_edits\":1,\"prefix_length\":5}}",
    }, .{ .vector = &.{ 0.0, 0.0 }, .k = 1 });
    defer dense_fuzzy.result.deinit();
    try std.testing.expectEqual(@as(u32, 1), dense_fuzzy.result.total_hits);
    try std.testing.expectEqual(@as(usize, 1), dense_fuzzy.result.hits.len);
    try std.testing.expectEqualStrings("doc:b", dense_fuzzy.result.hits[0].id);
    try std.testing.expectEqual(@as(u32, 1), dense_fuzzy.profile.raw_hit_count);

    var dense_match = try db.searchDenseProfiled(alloc, .{
        .index_name = "dv_v1",
        .limit = 1,
        .include_stored = false,
        .filter_query_json = "{\"match\":{\"field\":\"code\",\"role\":\"measure\",\"text\":\"KEEP\"}}",
    }, .{ .vector = &.{ 0.0, 0.0 }, .k = 1 });
    defer dense_match.result.deinit();
    try std.testing.expectEqual(@as(u32, 1), dense_match.result.total_hits);
    try std.testing.expectEqual(@as(usize, 1), dense_match.result.hits.len);
    try std.testing.expectEqualStrings("doc:b", dense_match.result.hits[0].id);
    try std.testing.expectEqual(@as(u32, 1), dense_match.profile.raw_hit_count);

    var dense_path_term = try db.searchDenseProfiled(alloc, .{
        .index_name = "dv_v1",
        .limit = 1,
        .include_stored = false,
        .filter_query_json = "{\"term\":{\"path\":\"/meta/tier\",\"value\":\"gold\"}}",
    }, .{ .vector = &.{ 0.0, 0.0 }, .k = 1 });
    defer dense_path_term.result.deinit();
    try std.testing.expectEqual(@as(u32, 1), dense_path_term.result.total_hits);
    try std.testing.expectEqual(@as(usize, 1), dense_path_term.result.hits.len);
    try std.testing.expectEqualStrings("doc:b", dense_path_term.result.hits[0].id);
    try std.testing.expectEqual(@as(u32, 1), dense_path_term.profile.raw_hit_count);

    var dense_path_prefix = try db.searchDenseProfiled(alloc, .{
        .index_name = "dv_v1",
        .limit = 1,
        .include_stored = false,
        .filter_query_json = "{\"prefix\":{\"path\":\"/meta/tier\",\"value\":\"go\"}}",
    }, .{ .vector = &.{ 0.0, 0.0 }, .k = 1 });
    defer dense_path_prefix.result.deinit();
    try std.testing.expectEqual(@as(u32, 1), dense_path_prefix.result.total_hits);
    try std.testing.expectEqual(@as(usize, 1), dense_path_prefix.result.hits.len);
    try std.testing.expectEqualStrings("doc:b", dense_path_prefix.result.hits[0].id);
    try std.testing.expectEqual(@as(u32, 1), dense_path_prefix.profile.raw_hit_count);

    var dense_ip_range = try db.searchDenseProfiled(alloc, .{
        .index_name = "dv_v1",
        .limit = 1,
        .include_stored = false,
        .filter_query_json = "{\"ip_range\":{\"field\":\"ip\",\"cidr\":\"10.0.0.0/8\"}}",
    }, .{ .vector = &.{ 0.0, 0.0 }, .k = 1 });
    defer dense_ip_range.result.deinit();
    try std.testing.expectEqual(@as(u32, 1), dense_ip_range.result.total_hits);
    try std.testing.expectEqual(@as(usize, 1), dense_ip_range.result.hits.len);
    try std.testing.expectEqualStrings("doc:b", dense_ip_range.result.hits[0].id);
    try std.testing.expectEqual(@as(u32, 1), dense_ip_range.profile.raw_hit_count);

    var dense_geo_distance = try db.searchDenseProfiled(alloc, .{
        .index_name = "dv_v1",
        .limit = 1,
        .include_stored = false,
        .filter_query_json = "{\"geo_distance\":{\"path\":\"/location\",\"lat\":37.7749,\"lon\":-122.4194,\"radius_meters\":2000}}",
    }, .{ .vector = &.{ 0.0, 0.0 }, .k = 1 });
    defer dense_geo_distance.result.deinit();
    try std.testing.expectEqual(@as(u32, 1), dense_geo_distance.result.total_hits);
    try std.testing.expectEqual(@as(usize, 1), dense_geo_distance.result.hits.len);
    try std.testing.expectEqualStrings("doc:b", dense_geo_distance.result.hits[0].id);
    try std.testing.expectEqual(@as(u32, 1), dense_geo_distance.profile.raw_hit_count);

    var dense_geo_shape = try db.searchDenseProfiled(alloc, .{
        .index_name = "dv_v1",
        .limit = 1,
        .include_stored = false,
        .filter_query_json = "{\"geo_shape\":{\"path\":\"/location\",\"relation\":\"intersects\",\"polygons\":[[{\"lat\":37.0,\"lon\":-123.0},{\"lat\":38.0,\"lon\":-123.0},{\"lat\":38.0,\"lon\":-122.0},{\"lat\":37.0,\"lon\":-122.0}]]}}",
    }, .{ .vector = &.{ 0.0, 0.0 }, .k = 1 });
    defer dense_geo_shape.result.deinit();
    try std.testing.expectEqual(@as(u32, 1), dense_geo_shape.result.total_hits);
    try std.testing.expectEqual(@as(usize, 1), dense_geo_shape.result.hits.len);
    try std.testing.expectEqualStrings("doc:b", dense_geo_shape.result.hits[0].id);
    try std.testing.expectEqual(@as(u32, 1), dense_geo_shape.profile.raw_hit_count);

    var dense_disjuncts = try db.searchDenseProfiled(alloc, .{
        .index_name = "dv_v1",
        .limit = 1,
        .include_stored = false,
        .filter_query_json = "{\"disjuncts\":[{\"term\":{\"category\":\"missing\"}},{\"bool_field\":{\"field\":\"published\",\"value\":true}}]}",
    }, .{ .vector = &.{ 0.0, 0.0 }, .k = 1 });
    defer dense_disjuncts.result.deinit();
    try std.testing.expectEqual(@as(u32, 1), dense_disjuncts.result.total_hits);
    try std.testing.expectEqual(@as(usize, 1), dense_disjuncts.result.hits.len);
    try std.testing.expectEqualStrings("doc:b", dense_disjuncts.result.hits[0].id);
    try std.testing.expectEqual(@as(u32, 1), dense_disjuncts.profile.raw_hit_count);

    var dense_required_plus_optional_should = try db.searchDenseProfiled(alloc, .{
        .index_name = "dv_v1",
        .limit = 1,
        .include_stored = false,
        .filter_query_json = "{\"bool\":{\"must\":[{\"term\":{\"category\":\"keep\"}}],\"should\":[{\"term\":{\"category\":\"reject\"}}]}}",
    }, .{ .vector = &.{ 0.0, 0.0 }, .k = 1 });
    defer dense_required_plus_optional_should.result.deinit();
    try std.testing.expectEqual(@as(u32, 1), dense_required_plus_optional_should.result.total_hits);
    try std.testing.expectEqual(@as(usize, 1), dense_required_plus_optional_should.result.hits.len);
    try std.testing.expectEqualStrings("doc:b", dense_required_plus_optional_should.result.hits[0].id);
    try std.testing.expectEqual(@as(u32, 1), dense_required_plus_optional_should.profile.raw_hit_count);

    var dense_missing = try db.searchDenseProfiled(alloc, .{
        .index_name = "dv_v1",
        .limit = 1,
        .include_stored = false,
        .filter_query_json = "{\"term\":{\"category\":\"missing\"}}",
    }, .{ .vector = &.{ 0.0, 0.0 }, .k = 1 });
    defer dense_missing.result.deinit();
    try std.testing.expectEqual(@as(u32, 0), dense_missing.result.total_hits);
    try std.testing.expectEqual(@as(usize, 0), dense_missing.result.hits.len);
    try std.testing.expectEqual(@as(u32, 0), dense_missing.profile.raw_hit_count);

    var dense_match_none = try db.searchDenseProfiled(alloc, .{
        .index_name = "dv_v1",
        .limit = 1,
        .include_stored = false,
        .filter_query_json = "{\"match_none\":{}}",
    }, .{ .vector = &.{ 0.0, 0.0 }, .k = 1 });
    defer dense_match_none.result.deinit();
    try std.testing.expectEqual(@as(u32, 0), dense_match_none.result.total_hits);
    try std.testing.expectEqual(@as(usize, 0), dense_match_none.result.hits.len);
    try std.testing.expectEqual(@as(u32, 0), dense_match_none.profile.raw_hit_count);

    var dense_intersect = try db.searchDenseProfiled(alloc, .{
        .index_name = "dv_v1",
        .limit = 1,
        .include_stored = false,
        .filter_doc_ids = &.{ "doc:a", "doc:b" },
        .filter_doc_ids_positive = true,
        .filter_query_json = "{\"term\":{\"category\":\"keep\"}}",
    }, .{ .vector = &.{ 0.0, 0.0 }, .k = 1 });
    defer dense_intersect.result.deinit();
    try std.testing.expectEqual(@as(u32, 1), dense_intersect.result.total_hits);
    try std.testing.expectEqual(@as(usize, 1), dense_intersect.result.hits.len);
    try std.testing.expectEqualStrings("doc:b", dense_intersect.result.hits[0].id);
    try std.testing.expectEqual(@as(u32, 1), dense_intersect.profile.raw_hit_count);

    var dense_doc_id_filter = try db.searchDenseProfiled(alloc, .{
        .index_name = "dv_v1",
        .limit = 1,
        .include_stored = false,
        .filter_query_json = "{\"doc_id\":{\"ids\":[\"doc:b\",\"missing\"]}}",
    }, .{ .vector = &.{ 0.0, 0.0 }, .k = 1 });
    defer dense_doc_id_filter.result.deinit();
    try std.testing.expectEqual(@as(u32, 1), dense_doc_id_filter.result.total_hits);
    try std.testing.expectEqual(@as(usize, 1), dense_doc_id_filter.result.hits.len);
    try std.testing.expectEqualStrings("doc:b", dense_doc_id_filter.result.hits[0].id);
    try std.testing.expectEqual(@as(u32, 1), dense_doc_id_filter.profile.raw_hit_count);

    var dense_must_not = try db.searchDenseProfiled(alloc, .{
        .index_name = "dv_v1",
        .limit = 1,
        .include_stored = false,
        .filter_query_json = "{\"bool\":{\"must_not\":[{\"term\":{\"category\":\"reject\"}}]}}",
    }, .{ .vector = &.{ 0.0, 0.0 }, .k = 2 });
    defer dense_must_not.result.deinit();
    try std.testing.expectEqual(@as(u32, 1), dense_must_not.result.total_hits);
    try std.testing.expectEqual(@as(usize, 1), dense_must_not.result.hits.len);
    try std.testing.expectEqualStrings("doc:b", dense_must_not.result.hits[0].id);

    var sparse_keep = try db.search(alloc, .{
        .index_name = "sp_v1",
        .query = .{ .sparse_knn = .{
            .indices = &.{1},
            .values = &.{1.0},
            .k = 1,
        } },
        .limit = 1,
        .include_stored = false,
        .filter_query_json = "{\"term\":{\"category\":\"keep\"}}",
    });
    defer sparse_keep.deinit();
    try std.testing.expectEqual(@as(u32, 1), sparse_keep.total_hits);
    try std.testing.expectEqual(@as(usize, 1), sparse_keep.hits.len);
    try std.testing.expectEqualStrings("doc:b", sparse_keep.hits[0].id);

    var sparse_terms = try db.search(alloc, .{
        .index_name = "sp_v1",
        .query = .{ .sparse_knn = .{
            .indices = &.{1},
            .values = &.{1.0},
            .k = 1,
        } },
        .limit = 1,
        .include_stored = false,
        .filter_query_json = "{\"terms\":{\"field\":\"category\",\"values\":[\"missing\",\"keep\"]}}",
    });
    defer sparse_terms.deinit();
    try std.testing.expectEqual(@as(u32, 1), sparse_terms.total_hits);
    try std.testing.expectEqual(@as(usize, 1), sparse_terms.hits.len);
    try std.testing.expectEqualStrings("doc:b", sparse_terms.hits[0].id);

    var sparse_missing = try db.search(alloc, .{
        .index_name = "sp_v1",
        .query = .{ .sparse_knn = .{
            .indices = &.{1},
            .values = &.{1.0},
            .k = 1,
        } },
        .limit = 1,
        .include_stored = false,
        .filter_query_json = "{\"term\":{\"category\":\"missing\"}}",
    });
    defer sparse_missing.deinit();
    try std.testing.expectEqual(@as(u32, 0), sparse_missing.total_hits);
    try std.testing.expectEqual(@as(usize, 0), sparse_missing.hits.len);

    var sparse_match_none = try db.search(alloc, .{
        .index_name = "sp_v1",
        .query = .{ .sparse_knn = .{
            .indices = &.{1},
            .values = &.{1.0},
            .k = 1,
        } },
        .limit = 1,
        .include_stored = false,
        .filter_query_json = "{\"match_none\":{}}",
    });
    defer sparse_match_none.deinit();
    try std.testing.expectEqual(@as(u32, 0), sparse_match_none.total_hits);
    try std.testing.expectEqual(@as(usize, 0), sparse_match_none.hits.len);

    var sparse_score_range = try db.search(alloc, .{
        .index_name = "sp_v1",
        .query = .{ .sparse_knn = .{
            .indices = &.{1},
            .values = &.{1.0},
            .k = 1,
        } },
        .limit = 1,
        .include_stored = false,
        .filter_query_json = "{\"numeric_range\":{\"field\":\"score\",\"min\":2.0}}",
    });
    defer sparse_score_range.deinit();
    try std.testing.expectEqual(@as(u32, 1), sparse_score_range.total_hits);
    try std.testing.expectEqual(@as(usize, 1), sparse_score_range.hits.len);
    try std.testing.expectEqualStrings("doc:b", sparse_score_range.hits[0].id);

    var sparse_standard_score_range = try db.search(alloc, .{
        .index_name = "sp_v1",
        .query = .{ .sparse_knn = .{
            .indices = &.{1},
            .values = &.{1.0},
            .k = 1,
        } },
        .limit = 1,
        .include_stored = false,
        .filter_query_json = "{\"range\":{\"score\":{\"gte\":2.0}}}",
    });
    defer sparse_standard_score_range.deinit();
    try std.testing.expectEqual(@as(u32, 1), sparse_standard_score_range.total_hits);
    try std.testing.expectEqual(@as(usize, 1), sparse_standard_score_range.hits.len);
    try std.testing.expectEqualStrings("doc:b", sparse_standard_score_range.hits[0].id);

    var sparse_bool_field = try db.search(alloc, .{
        .index_name = "sp_v1",
        .query = .{ .sparse_knn = .{
            .indices = &.{1},
            .values = &.{1.0},
            .k = 1,
        } },
        .limit = 1,
        .include_stored = false,
        .filter_query_json = "{\"bool_field\":{\"field\":\"published\",\"value\":true}}",
    });
    defer sparse_bool_field.deinit();
    try std.testing.expectEqual(@as(u32, 1), sparse_bool_field.total_hits);
    try std.testing.expectEqual(@as(usize, 1), sparse_bool_field.hits.len);
    try std.testing.expectEqualStrings("doc:b", sparse_bool_field.hits[0].id);

    var sparse_measure_prefix = try db.search(alloc, .{
        .index_name = "sp_v1",
        .query = .{ .sparse_knn = .{
            .indices = &.{1},
            .values = &.{1.0},
            .k = 1,
        } },
        .limit = 1,
        .include_stored = false,
        .filter_query_json = "{\"prefix\":{\"field\":\"code\",\"role\":\"measure\",\"value\":\"keep\"}}",
    });
    defer sparse_measure_prefix.deinit();
    try std.testing.expectEqual(@as(u32, 1), sparse_measure_prefix.total_hits);
    try std.testing.expectEqual(@as(usize, 1), sparse_measure_prefix.hits.len);
    try std.testing.expectEqualStrings("doc:b", sparse_measure_prefix.hits[0].id);

    var sparse_measure_wildcard = try db.search(alloc, .{
        .index_name = "sp_v1",
        .query = .{ .sparse_knn = .{
            .indices = &.{1},
            .values = &.{1.0},
            .k = 1,
        } },
        .limit = 1,
        .include_stored = false,
        .filter_query_json = "{\"wildcard\":{\"field\":\"code\",\"role\":\"measure\",\"pattern\":\"keep-*\"}}",
    });
    defer sparse_measure_wildcard.deinit();
    try std.testing.expectEqual(@as(u32, 1), sparse_measure_wildcard.total_hits);
    try std.testing.expectEqual(@as(usize, 1), sparse_measure_wildcard.hits.len);
    try std.testing.expectEqualStrings("doc:b", sparse_measure_wildcard.hits[0].id);

    var sparse_measure_regexp = try db.search(alloc, .{
        .index_name = "sp_v1",
        .query = .{ .sparse_knn = .{
            .indices = &.{1},
            .values = &.{1.0},
            .k = 1,
        } },
        .limit = 1,
        .include_stored = false,
        .filter_query_json = "{\"regexp\":{\"field\":\"code\",\"role\":\"measure\",\"pattern\":\"keep-.*\"}}",
    });
    defer sparse_measure_regexp.deinit();
    try std.testing.expectEqual(@as(u32, 1), sparse_measure_regexp.total_hits);
    try std.testing.expectEqual(@as(usize, 1), sparse_measure_regexp.hits.len);
    try std.testing.expectEqualStrings("doc:b", sparse_measure_regexp.hits[0].id);

    var sparse_fuzzy = try db.search(alloc, .{
        .index_name = "sp_v1",
        .query = .{ .sparse_knn = .{
            .indices = &.{1},
            .values = &.{1.0},
            .k = 1,
        } },
        .limit = 1,
        .include_stored = false,
        .filter_query_json = "{\"fuzzy\":{\"field\":\"code\",\"role\":\"measure\",\"query\":\"keep-cide\",\"max_edits\":1,\"prefix_length\":5}}",
    });
    defer sparse_fuzzy.deinit();
    try std.testing.expectEqual(@as(u32, 1), sparse_fuzzy.total_hits);
    try std.testing.expectEqual(@as(usize, 1), sparse_fuzzy.hits.len);
    try std.testing.expectEqualStrings("doc:b", sparse_fuzzy.hits[0].id);

    var sparse_match = try db.search(alloc, .{
        .index_name = "sp_v1",
        .query = .{ .sparse_knn = .{
            .indices = &.{1},
            .values = &.{1.0},
            .k = 1,
        } },
        .limit = 1,
        .include_stored = false,
        .filter_query_json = "{\"match\":{\"field\":\"code\",\"role\":\"measure\",\"text\":\"KEEP\"}}",
    });
    defer sparse_match.deinit();
    try std.testing.expectEqual(@as(u32, 1), sparse_match.total_hits);
    try std.testing.expectEqual(@as(usize, 1), sparse_match.hits.len);
    try std.testing.expectEqualStrings("doc:b", sparse_match.hits[0].id);

    var sparse_path_term = try db.search(alloc, .{
        .index_name = "sp_v1",
        .query = .{ .sparse_knn = .{
            .indices = &.{1},
            .values = &.{1.0},
            .k = 1,
        } },
        .limit = 1,
        .include_stored = false,
        .filter_query_json = "{\"term\":{\"path\":\"/meta/tier\",\"value\":\"gold\"}}",
    });
    defer sparse_path_term.deinit();
    try std.testing.expectEqual(@as(u32, 1), sparse_path_term.total_hits);
    try std.testing.expectEqual(@as(usize, 1), sparse_path_term.hits.len);
    try std.testing.expectEqualStrings("doc:b", sparse_path_term.hits[0].id);

    var sparse_path_prefix = try db.search(alloc, .{
        .index_name = "sp_v1",
        .query = .{ .sparse_knn = .{
            .indices = &.{1},
            .values = &.{1.0},
            .k = 1,
        } },
        .limit = 1,
        .include_stored = false,
        .filter_query_json = "{\"prefix\":{\"path\":\"/meta/tier\",\"value\":\"go\"}}",
    });
    defer sparse_path_prefix.deinit();
    try std.testing.expectEqual(@as(u32, 1), sparse_path_prefix.total_hits);
    try std.testing.expectEqual(@as(usize, 1), sparse_path_prefix.hits.len);
    try std.testing.expectEqualStrings("doc:b", sparse_path_prefix.hits[0].id);

    var sparse_ip_range = try db.search(alloc, .{
        .index_name = "sp_v1",
        .query = .{ .sparse_knn = .{
            .indices = &.{1},
            .values = &.{1.0},
            .k = 1,
        } },
        .limit = 1,
        .include_stored = false,
        .filter_query_json = "{\"ip_range\":{\"field\":\"ip\",\"cidr\":\"10.0.0.0/8\"}}",
    });
    defer sparse_ip_range.deinit();
    try std.testing.expectEqual(@as(u32, 1), sparse_ip_range.total_hits);
    try std.testing.expectEqual(@as(usize, 1), sparse_ip_range.hits.len);
    try std.testing.expectEqualStrings("doc:b", sparse_ip_range.hits[0].id);

    var sparse_geo_bbox = try db.search(alloc, .{
        .index_name = "sp_v1",
        .query = .{ .sparse_knn = .{
            .indices = &.{1},
            .values = &.{1.0},
            .k = 1,
        } },
        .limit = 1,
        .include_stored = false,
        .filter_query_json = "{\"geo_bbox\":{\"field\":\"/location\",\"min_lat\":37.70,\"min_lon\":-122.50,\"max_lat\":37.80,\"max_lon\":-122.30}}",
    });
    defer sparse_geo_bbox.deinit();
    try std.testing.expectEqual(@as(u32, 1), sparse_geo_bbox.total_hits);
    try std.testing.expectEqual(@as(usize, 1), sparse_geo_bbox.hits.len);
    try std.testing.expectEqualStrings("doc:b", sparse_geo_bbox.hits[0].id);

    var sparse_geo_shape = try db.search(alloc, .{
        .index_name = "sp_v1",
        .query = .{ .sparse_knn = .{
            .indices = &.{1},
            .values = &.{1.0},
            .k = 1,
        } },
        .limit = 1,
        .include_stored = false,
        .filter_query_json = "{\"geo_shape\":{\"path\":\"/location\",\"relation\":\"intersects\",\"polygons\":[[{\"lat\":37.0,\"lon\":-123.0},{\"lat\":38.0,\"lon\":-123.0},{\"lat\":38.0,\"lon\":-122.0},{\"lat\":37.0,\"lon\":-122.0}]]}}",
    });
    defer sparse_geo_shape.deinit();
    try std.testing.expectEqual(@as(u32, 1), sparse_geo_shape.total_hits);
    try std.testing.expectEqual(@as(usize, 1), sparse_geo_shape.hits.len);
    try std.testing.expectEqualStrings("doc:b", sparse_geo_shape.hits[0].id);

    var sparse_disjuncts = try db.search(alloc, .{
        .index_name = "sp_v1",
        .query = .{ .sparse_knn = .{
            .indices = &.{1},
            .values = &.{1.0},
            .k = 1,
        } },
        .limit = 1,
        .include_stored = false,
        .filter_query_json = "{\"disjuncts\":[{\"term\":{\"category\":\"missing\"}},{\"bool_field\":{\"field\":\"published\",\"value\":true}}]}",
    });
    defer sparse_disjuncts.deinit();
    try std.testing.expectEqual(@as(u32, 1), sparse_disjuncts.total_hits);
    try std.testing.expectEqual(@as(usize, 1), sparse_disjuncts.hits.len);
    try std.testing.expectEqualStrings("doc:b", sparse_disjuncts.hits[0].id);

    var sparse_required_plus_optional_should = try db.search(alloc, .{
        .index_name = "sp_v1",
        .query = .{ .sparse_knn = .{
            .indices = &.{1},
            .values = &.{1.0},
            .k = 1,
        } },
        .limit = 1,
        .include_stored = false,
        .filter_query_json = "{\"bool\":{\"filter\":[{\"term\":{\"category\":\"keep\"}}],\"should\":[{\"term\":{\"category\":\"reject\"}}],\"minimum_should_match\":0}}",
    });
    defer sparse_required_plus_optional_should.deinit();
    try std.testing.expectEqual(@as(u32, 1), sparse_required_plus_optional_should.total_hits);
    try std.testing.expectEqual(@as(usize, 1), sparse_required_plus_optional_should.hits.len);
    try std.testing.expectEqualStrings("doc:b", sparse_required_plus_optional_should.hits[0].id);
}

test "db dense index consuming a chunk-then-embed source via plural sources config converges its target counter" {
    try test_support.expectPluralSourceDenseConvergence(null);
}

test "db dense checkpoint persistence serializes with index apply" {
    const alloc = std.testing.allocator;

    var path_tmp = try TestDirectory.init("db");
    defer path_tmp.cleanup();
    const path = path_tmp.path().ptr;

    var db = try DB.open(alloc, std.mem.span(path), no_background_test_open);
    defer db.close();
    try db.addIndex(.{
        .name = "dense_checkpoint_lock",
        .kind = .dense_vector,
        .config_json = "{\"field\":\"embedding\",\"dims\":3}",
    });

    var apply_guard = try db.core.index_manager.lockManagedIndexApply(.{
        .name = "dense_checkpoint_lock",
        .kind = .dense_vector,
    });
    var apply_locked = true;
    defer if (apply_locked) apply_guard.unlock();

    const Persist = struct {
        db: *DB,
        started: std.atomic.Value(bool) = .init(false),
        done: std.atomic.Value(bool) = .init(false),
        err: ?anyerror = null,

        fn run(state: *@This()) void {
            state.started.store(true, .release);
            state.db.core.index_manager.saveDenseProjectionCheckpointMetadata(
                "dense_checkpoint_lock",
                .{ .applied_sequence = 1, .status = .clean },
            ) catch |err| {
                state.err = err;
            };
            state.done.store(true, .release);
        }
    };
    var persist = Persist{ .db = &db };
    var thread = try std.testing.io.concurrent(Persist.run, .{&persist});
    while (!persist.started.load(.acquire)) std.atomic.spinLoopHint();
    sleepNs(25 * std.time.ns_per_ms);
    const completed_while_apply_active = persist.done.load(.acquire);

    apply_guard.unlock();
    apply_locked = false;
    thread.await(std.testing.io);

    try std.testing.expect(!completed_while_apply_active);
    if (persist.err) |err| return err;
    try std.testing.expect(persist.done.load(.acquire));
}

test "db dense repair reprocesses corrupt managed source before rebuilding" {
    const alloc = std.testing.allocator;

    var path_tmp = try TestDirectory.init("db");
    defer path_tmp.cleanup();
    const path = path_tmp.path().ptr;

    var counting = CountingDenseEmbedder{};
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

    const artifact_key = try expectedDocumentEmbeddingArtifactKeyAlloc(alloc, "doc:a", "semantic_idx");
    defer alloc.free(artifact_key);
    try db.core.store.put(artifact_key, "bad-artifact");

    var queued = try db.repairArtifactIssuesWithRequestOptions(alloc, .{
        .target = .index,
        .artifact_kind = .embedding,
        .index_name = "semantic_idx",
        .limit = 1,
        .force = true,
    }, .{ .defer_durable_index_repair_execution = true });
    defer queued.deinit(alloc);
    const repair_id = (try db.indexRepairIdForIndex(alloc, "semantic_idx")) orelse
        return error.TestUnexpectedResult;

    const incomplete = try db.advanceIndexRepairIntent(alloc, repair_id, .{});
    try std.testing.expect(incomplete.attempted);
    try std.testing.expect(incomplete.deferred);
    try std.testing.expect(!incomplete.repaired);
    try std.testing.expect(db.enrichment_runtime != null);
    try std.testing.expect(!db.enrichment_runtime.?.isStarted());

    // The bounded workerless repair owner drains the re-armed producer
    // synchronously before yielding its turn.
    try db.runUntilIdle();
    const repaired_artifact = try db.core.store.get(alloc, artifact_key);
    defer alloc.free(repaired_artifact);
    try expectDenseEmbeddingArtifactValue(
        alloc,
        repaired_artifact,
        enrichment_artifact_codec.hashSource("alpha concept overview"),
        3,
    );

    const reset = try db.advanceIndexRepairIntent(alloc, repair_id, .{});
    try std.testing.expect(reset.attempted);
    try std.testing.expect(reset.deferred);
    const recovered = try db.advanceIndexRepairIntent(alloc, repair_id, .{});
    try std.testing.expect(recovered.has_repair_outcome);
    try std.testing.expectEqual(@as(u64, 1), recovered.indexes_rebuilt);
    try std.testing.expect(!recovered.repaired);
    try std.testing.expect(recovered.debt_remaining);
    try std.testing.expect(!try db.hasPendingIndexRepairIntents(alloc));

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

test "db dense enrichment skips unchanged source hash" {
    try testDenseSourceHashReuse(.{});
}

test "db dense enrichment republishes unchanged source hash from cached artifact after index reset" {
    const alloc = std.testing.allocator;

    var path_tmp = try TestDirectory.init("db");
    defer path_tmp.cleanup();
    const path = path_tmp.path().ptr;

    var counting = CountingDenseEmbedder{};
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
        .config_json = "{\"field\":\"embedding\",\"dims\":3,\"generator\":{\"kind\":\"dense_embedding\",\"source_field\":\"body\",\"embedding_name\":\"body_dense_v1\"}}",
    });

    try db.batch(.{
        .writes = &.{.{ .key = "doc:a", .value = "{\"title\":\"alpha\",\"body\":\"same source\"}" }},
        .sync_level = .write,
    });
    try db.runUntilIdle();
    try std.testing.expectEqual(@as(usize, 1), counting.calls);
    try std.testing.expectEqual(@as(u64, 1), db.core.index_manager.denseIndex("dv_v1").?.index.metadata.active_count);

    try resetDenseIndexForArtifactRebuildForTest(&db, "dv_v1");
    try std.testing.expectEqual(@as(u64, 0), db.core.index_manager.denseIndex("dv_v1").?.index.metadata.active_count);

    try db.batch(.{
        .writes = &.{.{ .key = "doc:a", .value = "{\"title\":\"changed\",\"body\":\"same source\"}" }},
        .sync_level = .write,
    });
    try db.runUntilIdle();

    try std.testing.expectEqual(@as(usize, 1), counting.calls);
    try std.testing.expectEqual(@as(u64, 1), db.core.index_manager.denseIndex("dv_v1").?.index.metadata.active_count);

    const query_vec = try counting.interface().embedDense(alloc, "", "same source", 3);
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
}

test "db dense index can reference existing whole-doc embedding enrichment" {
    const alloc = std.testing.allocator;

    var path_tmp = try TestDirectory.init("db");
    defer path_tmp.cleanup();
    const path = path_tmp.path().ptr;

    var deterministic = embedder_mod.DeterministicDenseEmbedder{};
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

    try db.enrichment_runtime.?.waitForApplied(1);
    try waitForDerivedReplayTarget(&db);

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

    var path_tmp = try TestDirectory.init("db");
    defer path_tmp.cleanup();
    const path = path_tmp.path().ptr;

    var deterministic = embedder_mod.DeterministicDenseEmbedder{};
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

    try db.enrichment_runtime.?.waitForApplied(1);
    try waitForDerivedReplayTarget(&db);
    _ = try waitForAppliedSequenceAdvance(alloc, &db, "dv_ref", 0);

    const query_vec = try deterministic.interface().embedDense(alloc, "", "abcdefgh", 3);
    defer alloc.free(query_vec);
    const dv_ref = db.core.index_manager.denseIndex("dv_ref") orelse return error.IndexNotFound;
    var direct = try waitForDenseIndexResultsWithAttempts(dv_ref.index, query_vec, 3, 2, slow_test_wait_attempts);
    defer direct.deinit();

    const chunk_zero = try expectedChunkArtifactPublicIdAlloc(alloc, "doc:a", "body_chunks_v1", 0);
    defer alloc.free(chunk_zero);
    var found_chunk_zero = false;
    for (direct.getHits(), 0..) |hit, idx| {
        const internal_id = if (direct.takeMetadata(idx)) |metadata|
            metadata
        else
            (try dv_ref.index.getMetadata(hit.vector_id)) orelse return error.TestUnexpectedResult;
        defer alloc.free(internal_id);
        var identity = try artifact_ids.resolvePublicHitIdentityAlloc(alloc, internal_id);
        defer identity.deinit(alloc);
        if (std.mem.eql(u8, identity.id, chunk_zero)) found_chunk_zero = true;
    }
    try std.testing.expect(found_chunk_zero);
}

test "db dense chunk consumer supports parent and parent_with_chunks modes" {
    const alloc = std.testing.allocator;

    var path_tmp = try TestDirectory.init("db");
    defer path_tmp.cleanup();
    const path = path_tmp.path().ptr;

    var deterministic = embedder_mod.DeterministicDenseEmbedder{};
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

    try db.enrichment_runtime.?.waitForApplied(1);
    try waitForDerivedReplayTarget(&db);
    const chunk_prefix = try internal_keys.artifactNamedPrefixAlloc(alloc, "doc:a", "chunk", "body_chunks_v1");
    defer alloc.free(chunk_prefix);
    const chunk_records = try db.core.store.scanPrefix(alloc, chunk_prefix);
    defer docstore_mod.DocStore.freeResults(alloc, chunk_records);
    try std.testing.expect(chunk_records.len > 0);
    try std.testing.expect(db.core.index_manager.denseIndex("dv_v1").?.index.metadata.active_count > 0);

    const query_vec = try deterministic.interface().embedDense(alloc, "", "abcdefgh", 3);
    defer alloc.free(query_vec);

    var chunk_result = try waitForSearchResult(alloc, &db, .{
        .index_name = "dv_v1",
        .dense = .{ .vector = query_vec, .k = 3 },
        .return_mode = .chunk,
    }, 1);
    defer chunk_result.deinit();
    const top_chunk_id = try alloc.dupe(u8, chunk_result.hits[0].id);
    defer alloc.free(top_chunk_id);
    var top_chunk_ref = (try artifact_ids.decodeArtifactPublicIdAlloc(alloc, top_chunk_id)) orelse return error.TestUnexpectedResult;
    defer top_chunk_ref.deinit(alloc);
    try std.testing.expectEqual(types.ArtifactKind.chunk, top_chunk_ref.kind);
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

    var filter = doc_set.ResolvedDocFilter{
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

    var parent_with_chunks = try waitForSearchResult(alloc, &db, .{
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

    var canonical_grouped = try waitForSearchResult(alloc, &db, .{
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

    var path_tmp = try TestDirectory.init("db");
    defer path_tmp.cleanup();
    const path = path_tmp.path().ptr;

    var deterministic = embedder_mod.DeterministicDenseEmbedder{};
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

    try db.enrichment_runtime.?.waitForApplied(1);
    try waitForDerivedReplayTarget(&db);
    const chunk_prefix = try internal_keys.artifactNamedPrefixAlloc(alloc, "doc:a", "chunk", "body_chunks_v1");
    defer alloc.free(chunk_prefix);
    const chunk_records = try db.core.store.scanPrefix(alloc, chunk_prefix);
    defer docstore_mod.DocStore.freeResults(alloc, chunk_records);
    try std.testing.expect(chunk_records.len > 0);
    try std.testing.expect(db.core.index_manager.denseIndex("dv_v1").?.index.metadata.active_count > 0);

    const query_vec = try deterministic.interface().embedDense(alloc, "", "abcdefgh", 3);
    defer alloc.free(query_vec);

    var chunk_result = try waitForSearchResult(alloc, &db, .{
        .index_name = "dv_v1",
        .dense = .{ .vector = query_vec, .k = 3 },
        .return_mode = .chunk,
    }, 1);
    defer chunk_result.deinit();
    const top_chunk_id = try alloc.dupe(u8, chunk_result.hits[0].id);
    defer alloc.free(top_chunk_id);
    var top_chunk_ref = (try artifact_ids.decodeArtifactPublicIdAlloc(alloc, top_chunk_id)) orelse return error.TestUnexpectedResult;
    defer top_chunk_ref.deinit(alloc);
    try std.testing.expectEqual(types.ArtifactKind.chunk, top_chunk_ref.kind);
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

    var parent_with_chunks = try waitForSearchResult(alloc, &db, .{
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

test "db dense parent paging fetches enough chunk hits before grouping" {
    const alloc = std.testing.allocator;

    var path_tmp = try TestDirectory.init("db");
    defer path_tmp.cleanup();
    const path = path_tmp.path().ptr;

    var deterministic = embedder_mod.DeterministicDenseEmbedder{};
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
            .{ .key = "doc:a", .value = "{\"body\":\"abcdefghabcdefghabcdefgh\"}" },
            .{ .key = "doc:b", .value = "{\"body\":\"abcdefzz\"}" },
        },
        .sync_level = .full_index,
    });

    const query_vec = try deterministic.interface().embedDense(alloc, "", "abcdefgh", 3);
    defer alloc.free(query_vec);

    var first_parent = try waitForSearchResult(alloc, &db, .{
        .index_name = "dv_v1",
        .dense = .{ .vector = query_vec, .k = 1 },
        .return_mode = .parent,
        .limit = 1,
    }, 1);
    defer first_parent.deinit();
    try std.testing.expectEqual(@as(u32, 2), first_parent.total_hits);
    try std.testing.expectEqual(@as(usize, 1), first_parent.hits.len);
    try std.testing.expectEqualStrings("doc:a", first_parent.hits[0].id);

    var second_parent = try waitForSearchResult(alloc, &db, .{
        .index_name = "dv_v1",
        .dense = .{ .vector = query_vec, .k = 1 },
        .return_mode = .parent,
        .limit = 1,
        .offset = 1,
    }, 1);
    defer second_parent.deinit();
    try std.testing.expectEqual(@as(u32, 2), second_parent.total_hits);
    try std.testing.expectEqual(@as(usize, 1), second_parent.hits.len);
    try std.testing.expectEqualStrings("doc:b", second_parent.hits[0].id);
}

test "db dense vector index routes knn search" {
    const alloc = std.testing.allocator;

    var path_tmp = try TestDirectory.init("db");
    defer path_tmp.cleanup();
    const path = path_tmp.path().ptr;

    var db = try DB.open(alloc, std.mem.span(path), .{});
    defer db.close();

    try db.batch(.{
        .writes = &.{
            .{ .key = "doc:a", .value = "{\"embedding\":[1,0,0],\"title\":\"alpha\"}" },
            .{ .key = "doc:b", .value = "{\"embedding\":[0,1,0],\"title\":\"beta\"}" },
        },
    });

    try db.addIndex(.{
        .name = "dv_v1",
        .kind = .dense_vector,
        .config_json = "{\"field\":\"embedding\",\"dims\":3,\"metric\":\"l2_squared\"}",
    });

    var result = try db.search(alloc, .{
        .index_name = "dv_v1",
        .query = .{ .dense_knn = .{
            .vector = &.{ 1.0, 0.0, 0.0 },
            .k = 2,
        } },
        .limit = 2,
    });
    defer result.deinit();

    try std.testing.expectEqual(@as(u32, 2), result.total_hits);
    try std.testing.expectEqualStrings("doc:a", result.hits[0].id);
    try std.testing.expect(result.hits[0].score.? >= result.hits[1].score.?);
    try std.testing.expect(result.hits[0].distance.? <= result.hits[1].distance.?);
}

test "db dense vector index routes knn search with durable lsm primary backend" {
    const alloc = std.testing.allocator;

    var path_tmp = try TestDirectory.init("db");
    defer path_tmp.cleanup();
    const path = path_tmp.path().ptr;

    var db = try DB.open(alloc, std.mem.span(path), .{
        .primary_backend = .{ .lsm = .{ .flush_threshold = 1 } },
    });
    defer db.close();

    try db.batch(.{
        .writes = &.{
            .{ .key = "doc:a", .value = "{\"embedding\":[1,0,0],\"title\":\"alpha\"}" },
            .{ .key = "doc:b", .value = "{\"embedding\":[0,1,0],\"title\":\"beta\"}" },
        },
    });

    try db.addIndex(.{
        .name = "dv_v1",
        .kind = .dense_vector,
        .config_json = "{\"field\":\"embedding\",\"dims\":3,\"metric\":\"l2_squared\"}",
    });

    var result = try db.search(alloc, .{
        .index_name = "dv_v1",
        .query = .{ .dense_knn = .{
            .vector = &.{ 1.0, 0.0, 0.0 },
            .k = 2,
        } },
        .limit = 2,
    });
    defer result.deinit();

    try std.testing.expectEqual(@as(u32, 2), result.total_hits);
    try std.testing.expectEqualStrings("doc:a", result.hits[0].id);
    try std.testing.expect(result.hits[0].score.? >= result.hits[1].score.?);
    try std.testing.expect(result.hits[0].distance.? <= result.hits[1].distance.?);
}

test "db dense and sparse field-backed vector indexes strip vector fields and persist embedding artifacts" {
    const alloc = std.testing.allocator;

    var path_tmp = try TestDirectory.init("db");
    defer path_tmp.cleanup();
    const path = path_tmp.path().ptr;

    var db = try DB.open(alloc, std.mem.span(path), .{});
    defer db.close();

    try db.addIndex(.{
        .name = "dv_v1",
        .kind = .dense_vector,
        .config_json = "{\"field\":\"embedding\",\"dims\":3,\"metric\":\"l2_squared\"}",
    });
    try db.addIndex(.{
        .name = "sp_v1",
        .kind = .sparse_vector,
        .config_json = "{\"field\":\"sparse\"}",
    });
    try db.addIndex(.{
        .name = "semantic_idx",
        .kind = .dense_vector,
        .config_json = "{\"field\":\"body\",\"dims\":3,\"metric\":\"cosine\",\"embedding_name\":\"semantic_idx\",\"generator\":{\"kind\":\"dense_embedding\",\"source_field\":\"body\",\"embedding_name\":\"semantic_idx\"}}",
    });
    try std.testing.expect(db.core.index_manager.denseIndexUsesManagedDirectField("dv_v1"));
    try std.testing.expect(db.core.index_manager.sparseIndexUsesManagedDirectField("sp_v1"));
    try std.testing.expect(!db.core.index_manager.denseIndexUsesManagedDirectField("semantic_idx"));

    try db.batch(.{
        .writes = &.{
            .{ .key = "doc:a", .value = "{\"title\":\"alpha\",\"embedding\":[1,0,0],\"sparse\":{\"indices\":[7],\"values\":[1.0]}}" },
            .{ .key = "doc:b", .value = "{\"title\":\"beta\",\"embedding\":[0,1,0],\"sparse\":{\"indices\":[3],\"values\":[1.0]}}" },
            .{ .key = "doc:c", .value = "{\"embedding\":[0,0,1],\"sparse\":{\"indices\":[11],\"values\":[1.0]}}" },
            .{ .key = "doc:missing", .value = "{\"title\":\"no vectors\"}" },
        },
        .sync_level = .full_index,
    });

    const coverage_stats = try db.stats(alloc);
    defer types.freeDBStats(alloc, coverage_stats);
    try std.testing.expectEqual(@as(u64, 4), coverage_stats.source_doc_count);
    var checked_direct_indexes: usize = 0;
    for (coverage_stats.indexes) |index_stats| {
        if (!std.mem.eql(u8, index_stats.name, "dv_v1") and !std.mem.eql(u8, index_stats.name, "sp_v1")) continue;
        try std.testing.expectEqual(@as(u64, 3), index_stats.coverage_produced_count);
        try std.testing.expectEqual(@as(u64, 1), index_stats.coverage_skipped_count);
        try std.testing.expectEqual(@as(u64, 0), index_stats.coverage_terminal_failed_count);
        checked_direct_indexes += 1;
    }
    try std.testing.expectEqual(@as(usize, 2), checked_direct_indexes);

    const stored = (try db.get(alloc, "doc:a")).?;
    defer alloc.free(stored);
    try std.testing.expect(std.mem.indexOf(u8, stored, "\"title\":\"alpha\"") != null);
    try std.testing.expect(std.mem.indexOf(u8, stored, "\"embedding\"") == null);
    try std.testing.expect(std.mem.indexOf(u8, stored, "\"sparse\"") == null);

    const vector_only_stored = (try db.get(alloc, "doc:c")).?;
    defer alloc.free(vector_only_stored);
    try std.testing.expectEqualStrings("{}", vector_only_stored);

    try db.batch(.{
        .writes = &.{
            .{ .key = "doc:managed", .value = "{\"body\":\"managed embedding source text\",\"embedding\":[1,0,0],\"sparse\":{\"indices\":[13],\"values\":[1.0]}}" },
        },
        .sync_level = .write,
    });
    const managed_stored = (try db.get(alloc, "doc:managed")).?;
    defer alloc.free(managed_stored);
    try std.testing.expect(std.mem.indexOf(u8, managed_stored, "\"body\":\"managed embedding source text\"") != null);
    try std.testing.expect(std.mem.indexOf(u8, managed_stored, "\"embedding\"") == null);
    try std.testing.expect(std.mem.indexOf(u8, managed_stored, "\"sparse\"") == null);

    const dense_artifact_key = try expectedDocumentEmbeddingArtifactKeyAlloc(alloc, "doc:a", "dv_v1");
    defer alloc.free(dense_artifact_key);
    const dense_artifact = try db.core.store.get(alloc, dense_artifact_key);
    defer alloc.free(dense_artifact);
    const sparse_artifact_key = try expectedDocumentEmbeddingArtifactKeyAlloc(alloc, "doc:a", "sp_v1");
    defer alloc.free(sparse_artifact_key);
    const sparse_artifact = try db.core.store.get(alloc, sparse_artifact_key);
    defer alloc.free(sparse_artifact);

    var dense = try db.search(alloc, .{
        .index_name = "dv_v1",
        .query = .{ .dense_knn = .{
            .vector = &.{ 1.0, 0.0, 0.0 },
            .k = 2,
        } },
        .limit = 2,
    });
    defer dense.deinit();
    try std.testing.expectEqualStrings("doc:a", dense.hits[0].id);

    var sparse = try db.search(alloc, .{
        .index_name = "sp_v1",
        .query = .{ .sparse_knn = .{
            .indices = &.{7},
            .values = &.{1.0},
            .k = 2,
        } },
        .limit = 2,
    });
    defer sparse.deinit();
    try std.testing.expectEqualStrings("doc:a", sparse.hits[0].id);

    try db.batch(.{
        .writes = &.{
            .{ .key = "doc:a", .value = "{\"title\":\"alpha\",\"embedding\":[0,0,1],\"sparse\":{\"indices\":[11],\"values\":[1.0]}}" },
        },
        .sync_level = .full_index,
    });

    const updated = (try db.get(alloc, "doc:a")).?;
    defer alloc.free(updated);
    try std.testing.expect(std.mem.indexOf(u8, updated, "\"title\":\"alpha\"") != null);
    try std.testing.expect(std.mem.indexOf(u8, updated, "\"embedding\"") == null);
    try std.testing.expect(std.mem.indexOf(u8, updated, "\"sparse\"") == null);
    const updated_dense_artifact = try db.core.store.get(alloc, dense_artifact_key);
    defer alloc.free(updated_dense_artifact);

    var dense_after = try db.search(alloc, .{
        .index_name = "dv_v1",
        .query = .{ .dense_knn = .{
            .vector = &.{ 0.0, 0.0, 1.0 },
            .k = 3,
        } },
        .limit = 3,
    });
    defer dense_after.deinit();
    try std.testing.expectEqualStrings("doc:a", dense_after.hits[0].id);

    const ExpectCoverage = struct {
        fn run(db_value: *DB, source: u64, produced: u64, skipped: u64) !void {
            const stats = try db_value.stats(std.testing.allocator);
            defer types.freeDBStats(std.testing.allocator, stats);
            try std.testing.expectEqual(source, stats.source_doc_count);
            var checked: usize = 0;
            for (stats.indexes) |index_stats| {
                if (!std.mem.eql(u8, index_stats.name, "dv_v1") and !std.mem.eql(u8, index_stats.name, "sp_v1")) continue;
                try std.testing.expectEqual(produced, index_stats.coverage_produced_count);
                try std.testing.expectEqual(skipped, index_stats.coverage_skipped_count);
                checked += 1;
            }
            try std.testing.expectEqual(@as(usize, 2), checked);
        }
    };
    try ExpectCoverage.run(&db, 5, 4, 1);

    try db.batch(.{ .writes = &.{.{ .key = "doc:a", .value = "{\"title\":\"now missing vectors\"}" }}, .sync_level = .full_index });
    try ExpectCoverage.run(&db, 5, 3, 2);

    try db.batch(.{ .writes = &.{.{ .key = "doc:missing", .value = "{\"embedding\":[1,1,0],\"sparse\":{\"indices\":[17],\"values\":[1.0]}}" }}, .sync_level = .full_index });
    try ExpectCoverage.run(&db, 5, 4, 1);

    try db.batch(.{ .deletes = &.{"doc:b"}, .sync_level = .full_index });
    try ExpectCoverage.run(&db, 4, 3, 1);
}

test "db dense artifact surplus uses quarantined generation replacement" {
    const alloc = std.testing.allocator;
    var path_tmp = try TestDirectory.init("db");
    defer path_tmp.cleanup();
    const path = path_tmp.path().ptr;

    var db = try DB.open(alloc, std.mem.span(path), no_background_test_open);
    defer db.close();
    try db.addIndex(.{
        .name = "dense_idx",
        .kind = .dense_vector,
        .config_json = "{\"field\":\"embedding\",\"dims\":3,\"metric\":\"l2_squared\",\"external\":true}",
    });
    try db.batch(.{
        .writes = &.{.{
            .key = "doc:a",
            .value = "{\"title\":\"alpha\",\"_embeddings\":{\"dense_idx\":[1,0,0]}}",
        }},
        .sync_level = .full_index,
    });
    try std.testing.expectEqual(
        @as(?u64, 1),
        try DB.loadDenseArtifactTargetCounter(alloc, db.core.store, "dense_idx"),
    );

    // Inject an index-only vector to model a stale key left by an interrupted
    // or defective replay. There is intentionally no matching source artifact
    // and therefore no target-counter increment.
    var ghost_vector = [_]f32{ 0, 1, 0 };
    try db.core.index_manager.denseIndex("dense_idx").?.index.insertVectorForTest(0xdead_beef, &ghost_vector);
    try std.testing.expectEqual(
        @as(u64, 2),
        db.core.index_manager.denseIndex("dense_idx").?.index.stats().active_count,
    );
    try std.testing.expect(try db.hasPendingDenseArtifactRebuild(alloc));

    try std.testing.expect((try db.rebuildDenseIndexesFromStoredEmbeddingArtifactsIfNeeded(alloc)) > 0);
    const first_repair_id = (try db.indexRepairIdForIndex(alloc, "dense_idx")) orelse
        return error.TestUnexpectedResult;
    var first_state = try db.loadIndexRepairState(alloc);
    try std.testing.expectEqual(
        index_repair_state.Trigger.artifact_coverage_mismatch,
        first_state.entries.items[0].intent.trigger,
    );
    first_state.deinit(alloc);
    // A source-counter surplus proves that this published generation is not a
    // healthy rollback anchor. Keep its files intact for replacement, but fail
    // queries closed throughout reconstruction.
    try std.testing.expectEqual(
        @as(u64, 2),
        db.core.index_manager.denseIndex("dense_idx").?.index.stats().active_count,
    );
    const blocked_stats = try db.stats(alloc);
    defer types.freeDBStats(alloc, blocked_stats);
    var observed_blocked_snapshot = false;
    for (blocked_stats.indexes) |index_stats| {
        if (!std.mem.eql(u8, index_stats.name, "dense_idx")) continue;
        observed_blocked_snapshot = true;
        try std.testing.expect(!index_stats.serving_snapshot_ready);
    }
    try std.testing.expect(observed_blocked_snapshot);
    try std.testing.expectError(error.IndexRebuilding, db.search(alloc, .{
        .index_name = "dense_idx",
        .query = .{ .dense_knn = .{ .vector = &.{ 1, 0, 0 }, .k = 1 } },
        .limit = 1,
    }));
    const QuarantinedGenerationHook = struct {
        fn afterSnapshot(_: *anyopaque, hooked_db: *DB, _: []const u8, _: u64) !void {
            try std.testing.expectEqual(
                @as(u64, 2),
                hooked_db.core.index_manager.denseIndex("dense_idx").?.index.stats().active_count,
            );
            try std.testing.expectError(error.IndexRebuilding, hooked_db.search(hooked_db.alloc, .{
                .index_name = "dense_idx",
                .query = .{ .dense_knn = .{ .vector = &.{ 1, 0, 0 }, .k = 1 } },
                .limit = 1,
            }));
        }
    };
    var hook_context: u8 = 0;
    db.shadow_index_repair_hook = .{
        .ptr = &hook_context,
        .after_snapshot_build = QuarantinedGenerationHook.afterSnapshot,
    };
    const first_repair = try db.advanceIndexRepairIntent(alloc, first_repair_id, .{});
    db.shadow_index_repair_hook = null;
    try std.testing.expect(first_repair.repaired);
    try std.testing.expectEqual(
        @as(u64, 1),
        db.core.index_manager.denseIndex("dense_idx").?.index.stats().active_count,
    );
    try std.testing.expect(!try db.hasPendingIndexRepairIntents(alloc));

    var result = try db.search(alloc, .{
        .index_name = "dense_idx",
        .query = .{ .dense_knn = .{ .vector = &.{ 0, 1, 0 }, .k = 2 } },
        .limit = 2,
    });
    defer result.deinit();
    try std.testing.expectEqual(@as(u32, 1), result.total_hits);
    try std.testing.expectEqualStrings("doc:a", result.hits[0].id);

    // The zero-target case must also reset a surplus, including after a crash
    // left a scan cursor belonging to the prior generation.
    try db.batch(.{
        .deletes = &.{"doc:a"},
        .sync_level = .full_index,
    });
    try std.testing.expectEqual(
        @as(?u64, 0),
        try DB.loadDenseArtifactTargetCounter(alloc, db.core.store, "dense_idx"),
    );
    try db.core.index_manager.denseIndex("dense_idx").?.index.insertVectorForTest(0xbeef_dead, &ghost_vector);
    const rebuild_state_path = try db.denseIndexRebuildStatePathAlloc(alloc, "dense_idx");
    defer alloc.free(rebuild_state_path);
    const rebuild_state = backfill_state_mod.RebuildState.init(rebuild_state_path);
    try rebuild_state.update("stale-generation-cursor");
    try std.testing.expect(try db.hasPendingDenseArtifactRebuild(alloc));
    _ = try db.rebuildDenseIndexesFromStoredEmbeddingArtifactsIfNeeded(alloc);
    const zero_repair_id = (try db.indexRepairIdForIndex(alloc, "dense_idx")) orelse
        return error.TestUnexpectedResult;
    try std.testing.expectEqual(
        @as(u64, 1),
        db.core.index_manager.denseIndex("dense_idx").?.index.stats().active_count,
    );
    const zero_repair = try db.advanceIndexRepairIntent(alloc, zero_repair_id, .{});
    try std.testing.expect(zero_repair.repaired);
    try std.testing.expectEqual(
        @as(u64, 0),
        db.core.index_manager.denseIndex("dense_idx").?.index.stats().active_count,
    );
    try std.testing.expect(!try db.hasPendingIndexRepairIntents(alloc));
}

test "db dense shadow activation rejects surplus candidate coverage" {
    const alloc = std.testing.allocator;
    var path_tmp = try TestDirectory.init("db");
    defer path_tmp.cleanup();
    const path = path_tmp.path().ptr;

    var db = try DB.open(alloc, std.mem.span(path), no_background_test_open);
    defer db.close();
    try db.addIndex(.{
        .name = "dense_idx",
        .kind = .dense_vector,
        .config_json = "{\"field\":\"embedding\",\"dims\":3,\"metric\":\"l2_squared\",\"external\":true}",
    });
    try db.batch(.{
        .writes = &.{
            .{ .key = "doc:a", .value = "{\"title\":\"alpha\",\"_embeddings\":{\"dense_idx\":[1,0,0]}}" },
            .{ .key = "doc:b", .value = "{\"title\":\"beta\",\"_embeddings\":{\"dense_idx\":[0,1,0]}}" },
        },
        .sync_level = .full_index,
    });
    const active_pointer_before = try db.core.index_manager.captureActiveIndexRootPointer("dense_idx");
    defer if (active_pointer_before) |pointer| alloc.free(pointer);

    var queued = try db.repairArtifactIssuesWithRequestOptions(alloc, .{
        .target = .index,
        .artifact_kind = .embedding,
        .index_name = "dense_idx",
        .limit = 1,
        .force = true,
    }, .{ .defer_durable_index_repair_execution = true });
    defer queued.deinit(alloc);
    const repair_id = (try db.indexRepairIdForIndex(alloc, "dense_idx")) orelse
        return error.TestUnexpectedResult;

    const CoverageHook = struct {
        fn afterSnapshot(_: *anyopaque, hooked_db: *DB, index_name: []const u8, _: u64) !void {
            const key = try DB.denseArtifactTargetCounterKeyAlloc(hooked_db.alloc, index_name);
            defer hooked_db.alloc.free(key);
            var reduced: [8]u8 = undefined;
            std.mem.writeInt(u64, &reduced, 1, .little);
            try hooked_db.core.store.put(key, &reduced);
        }
    };
    var hook_context: u8 = 0;
    db.shadow_index_repair_hook = .{
        .ptr = &hook_context,
        .after_snapshot_build = CoverageHook.afterSnapshot,
    };
    const outcome = try db.advanceIndexRepairIntent(alloc, repair_id, .{});
    db.shadow_index_repair_hook = null;
    try std.testing.expect(outcome.attempted);
    try std.testing.expect(outcome.deferred);
    try std.testing.expect(!outcome.terminal);
    try std.testing.expect(!outcome.repaired);
    try std.testing.expect(try db.hasPendingIndexRepairIntents(alloc));

    // Restore the deliberately altered source metadata. The failed candidate
    // must not have replaced the previous healthy two-vector generation.
    const counter_key = try DB.denseArtifactTargetCounterKeyAlloc(alloc, "dense_idx");
    defer alloc.free(counter_key);
    var restored: [8]u8 = undefined;
    std.mem.writeInt(u64, &restored, 2, .little);
    try db.core.store.put(counter_key, &restored);
    try std.testing.expectEqual(
        @as(u64, 2),
        db.core.index_manager.denseIndex("dense_idx").?.index.stats().active_count,
    );
    const active_pointer_after = try db.core.index_manager.captureActiveIndexRootPointer("dense_idx");
    defer if (active_pointer_after) |pointer| alloc.free(pointer);
    try std.testing.expectEqual(active_pointer_before == null, active_pointer_after == null);
    if (active_pointer_before) |before| {
        try std.testing.expectEqualStrings(before, active_pointer_after.?);
    }

    // Coverage failure makes this snapshot candidate insufficient. The next
    // turn discards it; the following turn rebuilds from authoritative source.
    const reset = try db.advanceIndexRepairIntent(alloc, repair_id, .{});
    try std.testing.expect(reset.attempted);
    try std.testing.expect(reset.deferred);
    var reset_entry = try db.loadIndexRepairEntryById(alloc, repair_id);
    try std.testing.expectEqual(index_repair_state.Phase.preflight, reset_entry.intent.phase);
    try std.testing.expect(reset_entry.intent.candidate_relative_path == null);
    try std.testing.expect(reset_entry.intent.last_error == null);
    try std.testing.expectEqual(@as(u32, 0), reset_entry.intent.failure_streak);
    reset_entry.deinit(alloc);

    const recovered = try db.advanceIndexRepairIntent(alloc, repair_id, .{});
    try std.testing.expect(recovered.attempted);
    try std.testing.expect(recovered.repaired);
    try std.testing.expect(!try db.hasPendingIndexRepairIntents(alloc));
}

test "db dense repair durably yields and resumes a reopenable building candidate" {
    const alloc = std.testing.allocator;
    var path_tmp = try TestDirectory.init("db");
    defer path_tmp.cleanup();
    const path = path_tmp.path().ptr;
    db_zig.test_dense_repair_rebuild_batch_size = 1;
    defer db_zig.test_dense_repair_rebuild_batch_size = null;
    db_zig.test_index_repair_catch_up_max_records_per_window = 1;
    defer db_zig.test_index_repair_catch_up_max_records_per_window = null;

    const cfg: types.IndexConfig = .{
        .name = "dense_idx",
        .kind = .dense_vector,
        .config_json = "{\"field\":\"embedding\",\"dims\":3,\"metric\":\"l2_squared\",\"external\":true}",
    };
    {
        var db = try openLegacyDenseTestDB(alloc, std.mem.span(path), .{
            .start_index_workers = false,
            .ttl_cleanup = .{ .enabled = false },
        });
        defer db.close();
        try db.addIndex(cfg);
        try db.batch(.{
            .writes = &.{
                .{ .key = "doc:a", .value = "{\"title\":\"alpha\",\"_embeddings\":{\"dense_idx\":[1,0,0]}}" },
                .{ .key = "doc:b", .value = "{\"title\":\"bravo\",\"_embeddings\":{\"dense_idx\":[0,1,0]}}" },
                .{ .key = "doc:c", .value = "{\"title\":\"charlie\",\"_embeddings\":{\"dense_idx\":[0,0,1]}}" },
                .{ .key = "doc:d", .value = "{\"title\":\"delta\",\"_embeddings\":{\"dense_idx\":[1,1,0]}}" },
            },
            .sync_level = .full_index,
        });
    }

    const active_path = try std.fmt.allocPrint(alloc, "{s}/indexes/{s}", .{ std.mem.span(path), cfg.name });
    defer alloc.free(active_path);
    const active_path_z = try alloc.dupeSentinel(u8, active_path, 0);
    defer alloc.free(active_path_z);
    {
        var hbc = try hbc_mod.HBCIndex.openWithLsmOptions(alloc, active_path_z, .{
            .dims = 3,
            .storage_backend = .lsm,
        }, .{});
        try hbc.markPublishedGenerationIncompleteForTest();
        hbc.close();
    }

    const Yield = struct {
        fn requested(_: *anyopaque) bool {
            return true;
        }
    };
    var yield_token: u8 = 0;
    const options = types.ArtifactRepairRunOptions{
        .yield_check = .{ .ptr = &yield_token, .is_requested = Yield.requested },
        // This test covers durable yield/resume checkpoints, not the default
        // activation pause SLA. Leave enough headroom for contended CI hosts.
        .max_activation_pause_ms = 5_000,
    };
    var repair_id: u128 = 0;
    var candidate_path: []u8 = undefined;
    {
        var db = try openLegacyDenseTestDB(alloc, std.mem.span(path), .{
            .open_mode = .writer_no_replay,
            .start_index_workers = false,
            .ttl_cleanup = .{ .enabled = false },
        });
        defer db.close();
        _ = try db.discoverRecoverableStartupIndexFailures(alloc, 1);
        repair_id = (try db.indexRepairIdForIndex(alloc, cfg.name)) orelse return error.TestUnexpectedResult;
        const first = try db.advanceIndexRepairIntent(alloc, repair_id, options);
        try std.testing.expect(first.attempted);
        try std.testing.expect(first.busy);
        try std.testing.expect(!first.repaired);
        try std.testing.expectEqual(@as(u64, 1), first.documents_reprocessed);

        var state = try db.loadIndexRepairState(alloc);
        defer state.deinit(alloc);
        const intent = state.entries.items[0].intent;
        try std.testing.expectEqual(index_repair_state.Phase.building, intent.phase);
        try std.testing.expect(intent.build_resume_key != null);
        try std.testing.expectEqual(@as(u64, 1), intent.build_reprocessed);
        try std.testing.expectEqual(@as(u32, 0), intent.failure_streak);
        candidate_path = try std.fmt.allocPrint(alloc, "{s}/{s}", .{ std.mem.span(path), intent.candidate_relative_path.? });

        // Mutate both sides of the saved source cursor. The new key sorts
        // before the first artifact slice and must arrive through replay; the
        // deleted key sorts after it and may disappear from a later snapshot.
        // Final fenced coverage must still converge without restarting the
        // candidate from zero.
        try db.batch(.{
            .writes = &.{.{ .key = "doc:0", .value = "{\"title\":\"zero\",\"_embeddings\":{\"dense_idx\":[1,0,1]}}" }},
            .sync_level = .write,
        });
        try db.batch(.{
            .writes = &.{.{ .key = "doc:b", .value = "{\"title\":\"bravo-updated\",\"_embeddings\":{\"dense_idx\":[0,1,1]}}" }},
            .sync_level = .write,
        });
        try db.batch(.{ .deletes = &.{"doc:d"}, .sync_level = .write });
    }
    defer alloc.free(candidate_path);

    // A yielded building candidate has no incomplete-publication marker and is
    // independently reopenable before the next process resumes its cursor.
    const candidate_path_z = try alloc.dupeSentinel(u8, candidate_path, 0);
    defer alloc.free(candidate_path_z);
    {
        var candidate = try hbc_mod.HBCIndex.openWithLsmOptions(alloc, candidate_path_z, .{
            .dims = 3,
            .storage_backend = .lsm,
        }, .{});
        candidate.close();
    }

    var documents_reprocessed: u64 = 1;
    var observed_catch_up_yield = false;
    {
        var reopened = try openLegacyDenseTestDB(alloc, std.mem.span(path), .{
            .open_mode = .writer_no_replay,
            .start_index_workers = false,
            .ttl_cleanup = .{ .enabled = false },
        });
        defer reopened.close();
        for (0..16) |_| {
            const step = try reopened.advanceIndexRepairIntent(alloc, repair_id, options);
            documents_reprocessed += step.documents_reprocessed;
            try std.testing.expect(!step.repaired);
            try std.testing.expect(step.busy);
            try std.testing.expect(!step.terminal);
            var state = try reopened.loadIndexRepairEntryById(alloc, repair_id);
            defer state.deinit(alloc);
            if (state.intent.phase == .catching_up) {
                try std.testing.expect(state.intent.candidate_applied_sequence > state.intent.build_floor_sequence);
                observed_catch_up_yield = true;
                break;
            }
        }
    }
    try std.testing.expect(observed_catch_up_yield);

    // Reopen from the durable replay checkpoint, proving a yielded catch-up
    // turn is independently restartable just like a yielded source scan.
    var final_reopened = try openLegacyDenseTestDB(alloc, std.mem.span(path), .{
        .open_mode = .writer_no_replay,
        .start_index_workers = false,
        .ttl_cleanup = .{ .enabled = false },
    });
    defer final_reopened.close();
    var repaired = false;
    for (0..16) |_| {
        const step = try final_reopened.advanceIndexRepairIntent(alloc, repair_id, options);
        documents_reprocessed += step.documents_reprocessed;
        if (step.repaired) {
            repaired = true;
            break;
        }
        try std.testing.expect(step.busy);
        try std.testing.expect(!step.terminal);
    }
    try std.testing.expect(repaired);
    // Three source artifacts were scanned (the fourth was concurrently
    // deleted); doc:0 arrived through replay and is intentionally not counted
    // as snapshot-reprocessed work.
    try std.testing.expectEqual(@as(u64, 3), documents_reprocessed);
    try std.testing.expect(!try final_reopened.hasPendingIndexRepairIntents(alloc));

    var result = try final_reopened.search(alloc, .{
        .index_name = cfg.name,
        .query = .{ .dense_knn = .{ .vector = &.{ 1, 0, 0 }, .k = 4 } },
        .limit = 4,
    });
    defer result.deinit();
    try std.testing.expectEqual(@as(u32, 4), result.total_hits);
    var found_replayed_insert = false;
    for (result.hits) |hit| {
        found_replayed_insert = found_replayed_insert or std.mem.eql(u8, hit.id, "doc:0");
        try std.testing.expect(!std.mem.eql(u8, hit.id, "doc:d"));
    }
    try std.testing.expect(found_replayed_insert);
}

test "db dense repair defers before candidate creation when node admission is exhausted" {
    const alloc = std.testing.allocator;
    var path_tmp = try TestDirectory.init("db");
    defer path_tmp.cleanup();
    const path = path_tmp.path().ptr;

    {
        var db = try openLegacyDenseTestDB(alloc, std.mem.span(path), .{
            .start_index_workers = false,
            .ttl_cleanup = .{ .enabled = false },
        });
        defer db.close();
        try db.addIndex(.{
            .name = "dense_idx",
            .kind = .dense_vector,
            .config_json = "{\"field\":\"embedding\",\"dims\":2,\"metric\":\"l2_squared\",\"external\":true}",
        });
        try db.batch(.{
            .writes = &.{.{ .key = "doc:a", .value = "{\"title\":\"alpha\",\"_embeddings\":{\"dense_idx\":[1,0]}}" }},
            .sync_level = .full_index,
        });
    }

    const dense_path = try std.fmt.allocPrint(alloc, "{s}/indexes/dense_idx", .{std.mem.span(path)});
    defer alloc.free(dense_path);
    const dense_path_z = try alloc.dupeSentinel(u8, dense_path, 0);
    defer alloc.free(dense_path_z);
    {
        var hbc = try hbc_mod.HBCIndex.openWithLsmOptions(alloc, dense_path_z, .{
            .dims = 2,
            .storage_backend = .lsm,
        }, .{});
        try hbc.markPublishedGenerationIncompleteForTest();
        hbc.close();
    }

    var budgets = resource_manager_mod.Options.defaultBudgets();
    budgets[@backingInt(resource_manager_mod.Slice.dense_repair_working_set)] = .{
        .soft_limit_bytes = 2 * 1024 * 1024,
        .hard_limit_bytes = 4 * 1024 * 1024,
    };
    var resources = resource_manager_mod.ResourceManager.init(.{ .budgets = budgets });
    defer resources.deinit(alloc);
    var reopened = try openLegacyDenseTestDB(alloc, std.mem.span(path), .{
        .open_mode = .writer_no_replay,
        .start_index_workers = false,
        .ttl_cleanup = .{ .enabled = false },
        .resource_manager = &resources,
    });
    defer reopened.close();
    _ = try reopened.discoverRecoverableStartupIndexFailures(alloc, 1);
    const repair_id = (try reopened.indexRepairIdForIndex(alloc, "dense_idx")) orelse return error.TestUnexpectedResult;
    const deferred = try reopened.advanceIndexRepairIntent(alloc, repair_id, .{});
    try std.testing.expect(deferred.attempted);
    try std.testing.expect(deferred.deferred);
    try std.testing.expect(!deferred.repaired);

    var entry = try reopened.loadIndexRepairEntryById(alloc, repair_id);
    defer entry.deinit(alloc);
    try std.testing.expect(entry.intent.candidate_relative_path == null);
    try std.testing.expectEqualStrings("RepairResourceUnavailable", entry.intent.last_error.?);
    try std.testing.expectEqual(@as(u32, 1), entry.intent.attempt_count);
    try std.testing.expectEqual(@as(u32, 1), entry.intent.failure_streak);
    const first_retry_at_ms = entry.intent.next_retry_at_ms;
    const admission = resources.sliceStats(.dense_repair_working_set);
    try std.testing.expectEqual(@as(u64, 0), admission.used_bytes);
    try std.testing.expectEqual(@as(u64, 1), admission.hard_limit_rejections);
    // This fixture is explicitly legacy-v1, so its injected interrupted
    // publication is represented by the durable LSM bulk marker. Native-v2
    // uses a missing CURRENT/checkpoint for the equivalent fail-closed state.
    try std.testing.expectError(error.IncompleteBulkPublish, hbc_mod.HBCIndex.openWithLsmOptions(alloc, dense_path_z, .{
        .dims = 2,
        .storage_backend = .lsm,
    }, .{}));

    // A persisted retry does not reset its consecutive-failure backoff across
    // executor passes or process restart.
    try reopened.updateIndexRepairIntent(alloc, repair_id, .{ .next_retry_at_ms = 0 });
    const second = try reopened.advanceIndexRepairIntent(alloc, repair_id, .{});
    try std.testing.expect(second.attempted);
    try std.testing.expect(second.deferred);
    var retried = try reopened.loadIndexRepairEntryById(alloc, repair_id);
    defer retried.deinit(alloc);
    try std.testing.expectEqual(@as(u32, 2), retried.intent.attempt_count);
    try std.testing.expectEqual(@as(u32, 2), retried.intent.failure_streak);
    try std.testing.expect(retried.intent.next_retry_at_ms > first_retry_at_ms);
}

test "db dense repair uses resource manager capacity admission before building" {
    const alloc = std.testing.allocator;
    var path_tmp = try TestDirectory.init("db");
    defer path_tmp.cleanup();
    const path = path_tmp.path().ptr;

    var db = try DB.open(alloc, std.mem.span(path), no_background_test_open);
    defer db.close();
    try db.addIndex(.{
        .name = "dense_idx",
        .kind = .dense_vector,
        .config_json = "{\"field\":\"embedding\",\"dims\":2,\"metric\":\"l2_squared\",\"external\":true}",
    });
    try db.batch(.{
        .writes = &.{.{ .key = "doc:a", .value = "{\"title\":\"alpha\",\"_embeddings\":{\"dense_idx\":[1,0]}}" }},
        .sync_level = .full_index,
    });

    const cfg = db.core.index_manager.get("dense_idx") orelse return error.TestUnexpectedResult;
    const repair_id = try db.createOperatorGenerationRepairIntent(alloc, cfg.*, 0, 0);
    const deferred = try db.advanceIndexRepairIntent(alloc, repair_id, .{
        .capacity_domain_id = 77,
        .capacity_observation = .{ .available_bytes = 1, .capacity_bytes = 1 },
    });
    try std.testing.expect(deferred.deferred);
    try std.testing.expect(deferred.disk_wait);
    try std.testing.expect(!deferred.attempted);

    var entry = try db.loadIndexRepairEntryById(alloc, repair_id);
    try std.testing.expect(entry.intent.candidate_relative_path == null);
    try std.testing.expectEqualStrings("disk_admission_unavailable", entry.intent.last_error.?);
    const first_estimate = entry.intent.estimated_candidate_bytes;
    const first_reservation = entry.intent.planned_disk_bytes;
    entry.deinit(alloc);
    const admission = db.core.index_manager.resource_manager.?.capacityStats();
    try std.testing.expectEqual(@as(u64, 0), admission.reserved_bytes);
    try std.testing.expectEqual(@as(u64, 1), admission.denials);

    // A durable intent must refresh upward when the live generation or the
    // scheduler's estimate grows between retries.
    try db.updateIndexRepairIntent(alloc, repair_id, .{ .next_retry_at_ms = 0 });
    const larger_estimate = first_estimate +| 1024 * 1024 * 1024;
    const retried = try db.advanceIndexRepairIntent(alloc, repair_id, .{
        .estimated_candidate_bytes = larger_estimate,
        .capacity_domain_id = 77,
        .capacity_observation = .{ .available_bytes = 1, .capacity_bytes = 1 },
    });
    try std.testing.expect(retried.disk_wait);
    var refreshed = try db.loadIndexRepairEntryById(alloc, repair_id);
    defer refreshed.deinit(alloc);
    try std.testing.expectEqual(larger_estimate, refreshed.intent.estimated_candidate_bytes);
    try std.testing.expect(refreshed.intent.planned_disk_bytes > first_reservation);
}

test "db dense replay failure upgrades a preflight validation intent" {
    const alloc = std.testing.allocator;
    var path_tmp = try TestDirectory.init("db");
    defer path_tmp.cleanup();
    const path = path_tmp.path().ptr;

    var db = try DB.open(alloc, std.mem.span(path), no_background_test_open);
    defer db.close();
    try db.addIndex(.{
        .name = "dense_idx",
        .kind = .dense_vector,
        .config_json = "{\"field\":\"embedding\",\"dims\":2,\"metric\":\"cosine\",\"external\":true}",
    });
    const cfg = db.core.index_manager.get("dense_idx") orelse return error.TestUnexpectedResult;
    const repair_id = try db.createOperatorGenerationRepairIntent(alloc, cfg.*, 0, 0);
    try db.updateIndexRepairIntent(alloc, repair_id, .{
        .phase = .preflight,
        .last_error = "OperatorGenerationValidationPending",
        .replace_last_error = true,
    });

    try std.testing.expect(try db.markDenseReplayArtifactRepairRequired(alloc, cfg.name));
    var upgraded = try db.loadIndexRepairEntryById(alloc, repair_id);
    try std.testing.expectEqual(index_repair_state.Trigger.projection_generation_invalid, upgraded.intent.trigger);
    try std.testing.expectEqual(index_repair_state.Phase.preflight, upgraded.intent.phase);
    try std.testing.expectEqualStrings("dense_replay_artifact_missing", upgraded.intent.last_error.?);
    try std.testing.expectEqual(@as(u64, 0), upgraded.intent.next_retry_at_ms);
    upgraded.deinit(alloc);

    try db.updateIndexRepairIntent(alloc, repair_id, .{
        .trigger = .operator_generation_validation,
        .last_error = "OperatorGenerationValidationPending",
        .replace_last_error = true,
    });
    try db.markDenseReplayArtifactCoverageRepairRequired(alloc, cfg.name);
    upgraded = try db.loadIndexRepairEntryById(alloc, repair_id);
    defer upgraded.deinit(alloc);
    try std.testing.expectEqual(index_repair_state.Trigger.artifact_coverage_mismatch, upgraded.intent.trigger);
    try std.testing.expectEqualStrings("dense_replay_artifact_unreadable", upgraded.intent.last_error.?);
}

test "db dense replay missing artifact remains a normal managed admission dependency" {
    const alloc = std.testing.allocator;
    var path_tmp = try TestDirectory.init("db");
    defer path_tmp.cleanup();
    const path = path_tmp.path().ptr;

    var db = try DB.open(alloc, std.mem.span(path), no_background_test_open);
    defer db.close();
    try db.batch(.{
        .writes = &.{.{ .key = "doc:a", .value = "{\"body\":\"alpha\"}" }},
        .sync_level = .write,
    });
    const cfg = types.IndexConfig{
        .name = "dense_idx",
        .kind = .dense_vector,
        .config_json = "{\"field\":\"embedding\",\"dims\":2,\"metric\":\"cosine\",\"generator\":{\"kind\":\"dense_embedding\",\"source_field\":\"body\"}}",
    };
    const repair_id = (try db.admitManagedIndex(cfg)) orelse return error.TestUnexpectedResult;

    try std.testing.expect(!try db.markDenseReplayArtifactRepairRequired(alloc, cfg.name));
    var retained = try db.loadIndexRepairEntryById(alloc, repair_id);
    defer retained.deinit(alloc);
    try std.testing.expectEqual(index_repair_state.WorkClass.initial_build, retained.intent.work_class);
    try std.testing.expectEqual(index_repair_state.Trigger.catalog_admission, retained.intent.trigger);
    try std.testing.expectEqual(index_repair_state.Phase.detected, retained.intent.phase);
    try std.testing.expectEqualStrings("managed_catalog_admission_rebuild", retained.intent.last_error.?);
}

test "db dense artifact rebuild preserves stable vector ids distinct from ordinals" {
    const alloc = std.testing.allocator;

    var path_tmp = try TestDirectory.init("db");
    defer path_tmp.cleanup();
    const path = path_tmp.path().ptr;

    {
        var db = try DB.open(alloc, std.mem.span(path), .{});
        defer db.close();

        try db.addIndex(.{
            .name = "dense_idx",
            .kind = .dense_vector,
            .config_json = "{\"field\":\"embedding\",\"dims\":3,\"metric\":\"l2_squared\",\"external\":true}",
        });

        try db.batch(.{
            .writes = &.{
                .{ .key = "doc:a", .value = "{\"title\":\"alpha\",\"_embeddings\":{\"dense_idx\":[1,0,0]}}" },
                .{ .key = "doc:b", .value = "{\"title\":\"beta\",\"_embeddings\":{\"dense_idx\":[0,1,0]}}" },
            },
            .sync_level = .full_index,
        });

        var txn = try db.core.store.beginProbeTxn();
        defer txn.abort();
        const ordinal = (try doc_identity.lookupOrdinalTxn(alloc, &txn, "doc:b")) orelse return error.TestUnexpectedResult;
        try std.testing.expectEqual(@as(doc_identity.DocOrdinal, 2), ordinal);
        const stable_vector_id = denseTestVectorId("doc:b");
        try std.testing.expect(stable_vector_id != @as(u64, ordinal));
        try std.testing.expectEqual(@as(?u64, stable_vector_id), try db.core.index_manager.lookupDenseVectorId(db.core.store, "dense_idx", "doc:b"));

        // Exercise the production reconstruction path. New dense indexes are
        // pointer-selected native generations, so deleting the legacy
        // canonical directory is no longer a meaningful rebuild trigger.
        const cfg = db.core.index_manager.get("dense_idx") orelse return error.TestUnexpectedResult;
        const repair_id = try db.createGenerationRepairIntent(
            alloc,
            cfg.*,
            .operator_generation_rebuild,
            0,
            0,
            null,
        );
        var repaired: DB.IndexRepairAdvanceResult = undefined;
        var documents_reprocessed: u64 = 0;
        for (0..4) |_| {
            repaired = try db.advanceIndexRepairIntent(alloc, repair_id, repair_completion_test_options);
            documents_reprocessed +|= repaired.documents_reprocessed;
            if (repaired.repaired) break;
            try std.testing.expect(repaired.deferred or repaired.busy);
            try std.testing.expect(!repaired.terminal);
            try std.testing.expectEqual(@as(u64, 0), repaired.next_retry_at_ms);
        }
        try std.testing.expect(repaired.repaired);
        try std.testing.expectEqual(@as(u64, 2), documents_reprocessed);
        try std.testing.expect(!try db.hasPendingIndexRepairIntents(alloc));
    }

    var reopened = try DB.open(alloc, std.mem.span(path), .{
        .open_mode = .query_readonly,
        .start_index_workers = false,
    });
    defer reopened.close();

    var txn = try reopened.core.store.beginProbeTxn();
    defer txn.abort();
    const ordinal = (try doc_identity.lookupOrdinalTxn(alloc, &txn, "doc:b")) orelse return error.TestUnexpectedResult;
    const stable_vector_id = denseTestVectorId("doc:b");
    try std.testing.expect(stable_vector_id != @as(u64, ordinal));
    try std.testing.expectEqual(@as(?u64, stable_vector_id), try reopened.core.index_manager.lookupDenseVectorId(reopened.core.store, "dense_idx", "doc:b"));

    const vector_ids = try reopened.core.index_manager.lookupDenseVectorIdsForOrdinalsAlloc(alloc, reopened.core.store, "dense_idx", &.{ordinal});
    defer alloc.free(vector_ids);
    try std.testing.expectEqual(@as(usize, 1), vector_ids.len);
    try std.testing.expectEqual(stable_vector_id, vector_ids[0]);

    const dense_entry = reopened.core.index_manager.denseIndex("dense_idx") orelse return error.TestUnexpectedResult;
    const stable_metadata = (try dense_entry.index.getMetadata(stable_vector_id)) orelse return error.TestUnexpectedResult;
    defer alloc.free(stable_metadata);
    try std.testing.expectEqualStrings("doc:b", stable_metadata);
    try std.testing.expectEqual(@as(?[]u8, null), try dense_entry.index.getMetadata(@as(u64, ordinal)));
}

test "db dense artifact rebuild trusts clean projection checkpoint without artifact recount" {
    const alloc = std.testing.allocator;

    var path_tmp = try TestDirectory.init("db");
    defer path_tmp.cleanup();
    const path = path_tmp.path().ptr;

    {
        var db = try DB.open(alloc, std.mem.span(path), no_background_test_open);
        defer db.close();

        const dense_cfg: types.IndexConfig = .{
            .name = "dense_idx",
            .kind = .dense_vector,
            .config_json = "{\"field\":\"embedding\",\"dims\":3,\"metric\":\"l2_squared\",\"external\":true}",
        };
        try db.addIndex(dense_cfg);

        try db.batch(.{
            .writes = &.{
                .{ .key = "doc:a", .value = "{\"title\":\"alpha\",\"_embeddings\":{\"dense_idx\":[1,0,0]}}" },
            },
            .sync_level = .full_index,
        });

        const target_sequence = db.core.nextDerivedSequence() -| 1;
        try db.core.saveProjectionCheckpoint("dense_idx", .{
            .applied_sequence = target_sequence,
            .status = .clean,
            .generation = 3,
        });
        const checkpoint_path = db.core.applied_sequence_checkpoint_path orelse return error.TestUnexpectedResult;
        const mirrored_ahead_sequence: u64 = 1000;
        try apply_state.saveProjectionCheckpointWithSidecar(alloc, db.core.index_manager.checkpointIo(), db.core.store, checkpoint_path, "dense_idx", .{
            .applied_sequence = mirrored_ahead_sequence,
            .status = .clean,
            .generation = 99,
            .config_hash = types.indexConfigHash(dense_cfg),
        });

        const stale_artifact_key = try expectedDocumentEmbeddingArtifactKeyAlloc(alloc, "doc:stale", "dense_idx");
        defer alloc.free(stale_artifact_key);
        try putDenseEmbeddingArtifactForTest(&db, alloc, stale_artifact_key, null, &[_]f32{ 0, 1, 0 });
    }

    var reopened = try DB.open(alloc, std.mem.span(path), no_replay_no_background_test_open);
    defer reopened.close();

    try std.testing.expect(!(try reopened.hasPendingDenseArtifactRebuild(alloc)));

    const checkpoint = try reopened.core.loadProjectionCheckpoint(alloc, "dense_idx");
    try std.testing.expectEqual(apply_state.ProjectionStatus.clean, checkpoint.status);
    try std.testing.expect(checkpoint.applied_sequence < 1000);
    try std.testing.expectEqual(@as(u64, 3), checkpoint.generation);
    try std.testing.expectEqual(types.indexConfigHash(.{
        .name = "dense_idx",
        .kind = .dense_vector,
        .config_json = "{\"field\":\"embedding\",\"dims\":3,\"metric\":\"l2_squared\",\"external\":true}",
    }), checkpoint.config_hash);

    const stats = try reopened.stats(alloc);
    defer types.freeDBStats(alloc, stats);
    try std.testing.expectEqual(@as(usize, 1), stats.indexes.len);
    try std.testing.expectEqualStrings("clean", stats.indexes[0].projection_checkpoint_status);
    try std.testing.expectEqual(checkpoint.applied_sequence, stats.indexes[0].projection_checkpoint_applied_sequence);
    try std.testing.expectEqual(@as(u64, 3), stats.indexes[0].projection_checkpoint_generation);
    try std.testing.expectEqual(types.indexConfigHash(.{
        .name = "dense_idx",
        .kind = .dense_vector,
        .config_json = "{\"field\":\"embedding\",\"dims\":3,\"metric\":\"l2_squared\",\"external\":true}",
    }), stats.indexes[0].projection_checkpoint_config_hash);
    try std.testing.expectEqual(stats.indexes[0].replay_target_sequence -| checkpoint.applied_sequence, stats.indexes[0].checkpoint_replay_tail_sequence_count);
}

test "db dense projection checkpoint prefers hbc metadata over corrupt sidecar" {
    const alloc = std.testing.allocator;

    var path_tmp = try TestDirectory.init("db");
    defer path_tmp.cleanup();
    const path = path_tmp.path().ptr;

    const dense_cfg: types.IndexConfig = .{
        .name = "dense_idx",
        .kind = .dense_vector,
        .config_json = "{\"field\":\"embedding\",\"dims\":3,\"metric\":\"l2_squared\",\"external\":true}",
    };
    var target_sequence: u64 = 0;
    var native_applied_sequence: u64 = 0;

    {
        var db = try DB.open(alloc, std.mem.span(path), no_background_test_open);
        defer db.close();

        try db.addIndex(dense_cfg);
        try db.batch(.{
            .writes = &.{
                .{ .key = "doc:a", .value = "{\"title\":\"alpha\",\"_embeddings\":{\"dense_idx\":[1,0,0]}}" },
            },
            .sync_level = .full_index,
        });

        target_sequence = db.core.nextDerivedSequence() -| 1;
        try db.core.saveProjectionCheckpoint("dense_idx", .{
            .applied_sequence = target_sequence,
            .status = .clean,
            .generation = 11,
            .config_hash = types.indexConfigHash(dense_cfg),
        });
        native_applied_sequence = db.core.index_manager
            .denseIndex("dense_idx").?.index.experimentalPostingDurableAppliedSequence() orelse
            return error.TestUnexpectedResult;

        const checkpoint_path = db.core.applied_sequence_checkpoint_path orelse return error.TestUnexpectedResult;
        try writeRawProjectionCheckpointSidecarForTest(checkpoint_path, "not-a-derived-apply-checkpoint");
        try std.testing.expectError(
            error.InvalidDerivedApplyState,
            apply_state.loadProjectionCheckpointWithSidecar(alloc, db.core.index_manager.checkpointIo(), db.core.store, checkpoint_path, "dense_idx"),
        );
    }

    var reopened = try DB.open(alloc, std.mem.span(path), no_replay_no_background_test_open);
    defer reopened.close();

    const checkpoint = try reopened.core.loadProjectionCheckpoint(alloc, "dense_idx");
    try std.testing.expectEqual(apply_state.ProjectionStatus.clean, checkpoint.status);
    try std.testing.expectEqual(@as(u64, 11), checkpoint.generation);
    try std.testing.expectEqual(types.indexConfigHash(dense_cfg), checkpoint.config_hash);
    // Lifecycle fields come from HBC metadata; native WAL coverage is the
    // authoritative applied sequence and may be ahead of that lifecycle write.
    try std.testing.expectEqual(native_applied_sequence, checkpoint.applied_sequence);

    const stats = try reopened.stats(alloc);
    defer types.freeDBStats(alloc, stats);
    try std.testing.expectEqual(@as(usize, 1), stats.indexes.len);
    try std.testing.expectEqual(checkpoint.applied_sequence, stats.indexes[0].projection_checkpoint_applied_sequence);
    try std.testing.expectEqual(@as(u64, 11), stats.indexes[0].projection_checkpoint_generation);
}

test "db dense artifact coverage finalizes a completed rebuilding checkpoint" {
    const alloc = std.testing.allocator;

    var path_tmp = try TestDirectory.init("db");
    defer path_tmp.cleanup();
    const path = path_tmp.path().ptr;

    var db = try DB.open(alloc, std.mem.span(path), no_background_test_open);
    defer db.close();

    const dense_cfg: types.IndexConfig = .{
        .name = "dense_idx",
        .kind = .dense_vector,
        .config_json = "{\"field\":\"embedding\",\"dims\":3,\"metric\":\"l2_squared\",\"external\":true}",
    };
    try db.addIndex(dense_cfg);
    try db.batch(.{
        .writes = &.{.{ .key = "doc:a", .value = "{\"_embeddings\":{\"dense_idx\":\"AACAPwAAAAAAAAAA\"}}" }},
        .sync_level = .full_index,
    });

    const target_sequence = db.core.nextDerivedSequence();
    try db.core.saveAppliedSequence("dense_idx", target_sequence -| 1);
    try db.core.saveProjectionCheckpoint("dense_idx", .{
        .applied_sequence = target_sequence -| 1,
        .status = .rebuilding,
        .generation = 7,
        .config_hash = types.indexConfigHash(dense_cfg),
    });

    try std.testing.expectEqual(@as(usize, 1), try db.rebuildDenseIndexesFromStoredEmbeddingArtifactsIfNeeded(alloc));
    try std.testing.expectEqual(target_sequence, try db.core.loadAppliedSequence(alloc, "dense_idx"));
    const checkpoint = try db.core.loadProjectionCheckpoint(alloc, "dense_idx");
    try std.testing.expectEqual(apply_state.ProjectionStatus.clean, checkpoint.status);
    try std.testing.expectEqual(target_sequence, checkpoint.applied_sequence);
    try std.testing.expectEqual(@as(u64, 8), checkpoint.generation);
    try std.testing.expectEqual(types.indexConfigHash(dense_cfg), checkpoint.config_hash);
}

test "db dense target reads atomic artifact and source coverage snapshot" {
    const alloc = std.testing.allocator;
    var path_tmp = try TestDirectory.init("db");
    defer path_tmp.cleanup();
    var db = try DB.open(alloc, path_tmp.path(), .{
        .start_index_workers = false,
        .start_optional_runtime_workers = false,
        .ttl_cleanup = .{ .enabled = false },
    });
    defer db.close();
    try db.addIndex(.{
        .name = "dense_idx",
        .kind = .dense_vector,
        .config_json = "{\"field\":\"embedding\",\"dims\":3,\"external\":true}",
        .coverage_generation = 42,
    });
    const Commit = struct {
        fn counters(ctx: *AsyncContext, count: u64) !void {
            const tags = [_][]const u8{ "produced", "skipped", "terminal_failed" };
            var keys: [4][]u8 = undefined;
            var initialized: usize = 0;
            defer for (keys[0..initialized]) |key| ctx.alloc.free(key);
            var value: [8]u8 = undefined;
            std.mem.writeInt(u64, &value, count, .little);
            const zero = @as([8]u8, @splat(0));
            var writes: [4]docstore_mod.KVPair = undefined;
            for (tags, 0..) |tag, i| {
                keys[i] = try internal_keys.derivedCoverageOutcomeCountKeyAlloc(ctx.alloc, "dense_idx", 42, tag);
                initialized += 1;
                writes[i] = .{ .key = keys[i], .value = if (i == 0) &value else &zero };
            }
            keys[3] = try DB.denseArtifactTargetCounterKeyAlloc(ctx.alloc, "dense_idx");
            initialized += 1;
            writes[3] = .{ .key = keys[3], .value = &value };
            try ctx.store.putBatch(&writes, &.{});
        }
        fn afterCapture(ctx: *AsyncContext) !void {
            db_zig.test_dense_target_after_coverage_capture = null;
            try counters(ctx, 2);
        }
    };
    try Commit.counters(db.async_context, 1);
    var item: types.DBIndexStats = .{
        .name = "dense_idx",
        .kind = .dense_vector,
        .coverage_identity_ready = true,
        .coverage_generation = 42,
    };
    // Commit a second source after status captures its coverage tuple. The
    // publication target must still describe the first revision, rather than
    // re-reading the new counter and reporting contradictory readiness facts.
    db_zig.test_dense_target_after_coverage_capture = Commit.afterCapture;
    defer db_zig.test_dense_target_after_coverage_capture = null;
    try db.populateConfiguredDerivedCoverageCounts("dense_idx", &item);
    try std.testing.expect(db_zig.test_dense_target_after_coverage_capture == null);
    try std.testing.expect(item.publication_target_ready);
    try std.testing.expectEqual(@as(u64, 1), item.coverage_produced_count);
    try std.testing.expectEqual(@as(u64, 1), item.publication_target_count);
    try db.populateConfiguredDerivedCoverageCounts("dense_idx", &item);
    try std.testing.expectEqual(@as(u64, 2), item.coverage_produced_count);
    try std.testing.expectEqual(@as(u64, 2), item.publication_target_count);

    // Missing/corrupt bootstrap metadata cannot be replaced with the source
    // count: a document may produce many vectors, or none.
    const counter_key = try DB.denseArtifactTargetCounterKeyAlloc(alloc, "dense_idx");
    defer alloc.free(counter_key);
    try db.core.store.putBatch(&.{}, &.{counter_key});
    try db.populateConfiguredDerivedCoverageCounts("dense_idx", &item);
    try std.testing.expect(!item.publication_target_ready);
    try std.testing.expectEqual(@as(u64, 0), item.publication_target_count);
    try db.core.store.put(counter_key, "invalid");
    db.populateConfiguredDerivedCoverageCountsBestEffort("dense_idx", &item);
    try std.testing.expect(!item.publication_target_ready);
    try std.testing.expect(item.repair_degraded);
}

test "db dense target reads atomic outcome and source coverage snapshot" {
    const alloc = std.testing.allocator;
    var path_tmp = try TestDirectory.init("db");
    defer path_tmp.cleanup();
    const path = path_tmp.path().ptr;
    var db = try DB.open(alloc, std.mem.span(path), .{
        .start_index_workers = false,
        .start_optional_runtime_workers = false,
        .ttl_cleanup = .{ .enabled = false },
    });
    defer db.close();
    const config: types.IndexConfig = .{
        .name = "dense_idx",
        .kind = .dense_vector,
        .config_json = "{\"field\":\"embedding\",\"dims\":3,\"metric\":\"cosine\"}",
        .coverage_generation = 42,
    };
    try db.addIndex(config);
    try db.core.store.put(&internal_keys.range_document_count_key, &(@as([8]u8, @splat(0))));
    const Commit = struct {
        fn afterProduced(ctx: *AsyncContext) !void {
            db_zig.test_dense_target_after_coverage_capture = null;
            const tags = [_][]const u8{ "produced", "skipped", "terminal_failed" };
            var keys: [3][]u8 = undefined;
            var initialized: usize = 0;
            defer for (keys[0..initialized]) |key| ctx.alloc.free(key);
            var one: [8]u8 = undefined;
            std.mem.writeInt(u64, &one, 1, .little);
            const zero = @as([8]u8, @splat(0));
            var writes: [4]docstore_mod.KVPair = undefined;
            for (tags, 0..) |tag, i| {
                keys[i] = try internal_keys.derivedCoverageOutcomeCountKeyAlloc(ctx.alloc, "dense_idx", 42, tag);
                initialized += 1;
                writes[i] = .{ .key = keys[i], .value = if (i == 0) &one else &zero };
            }
            writes[3] = .{ .key = &internal_keys.range_document_count_key, .value = &one };
            try ctx.store.putBatch(&writes, &.{});
        }
    };
    // Publish a complete first tuple after the counter batch is captured.
    // This call must observe the pre-commit empty table; the next sees all
    // four committed counters. No scheduling/sleep is needed to force the race.
    db_zig.test_dense_target_after_coverage_capture = Commit.afterProduced;
    defer db_zig.test_dense_target_after_coverage_capture = null;
    try std.testing.expectEqual(@as(?u64, 0), try denseTargetCountForIndexContext(db.async_context, config.name));
    try std.testing.expect(db_zig.test_dense_target_after_coverage_capture == null);
    try std.testing.expectEqual(@as(?u64, 1), try denseTargetCountForIndexContext(db.async_context, config.name));

    // Actual persisted corruption remains an error, rather than a retryable
    // absence that could certify an incomplete generation.
    const skipped_key = try internal_keys.derivedCoverageOutcomeCountKeyAlloc(alloc, config.name, 42, "skipped");
    defer alloc.free(skipped_key);
    try db.core.store.putBatch(&.{}, &.{skipped_key});
    try std.testing.expectError(error.InvalidDerivedCoverageCounter, denseTargetCountForIndexContext(db.async_context, config.name));
}

test "db dense finalization owner drains requests queued during publication" {
    const alloc = std.testing.allocator;

    var path_tmp = try TestDirectory.init("db");
    defer path_tmp.cleanup();
    const path = path_tmp.path().ptr;

    var db = try DB.open(alloc, std.mem.span(path), no_background_test_open);
    defer db.close();

    const config: types.IndexConfig = .{
        .name = "dense_idx",
        .kind = .dense_vector,
        .config_json = "{\"field\":\"embedding\",\"dims\":3,\"metric\":\"l2_squared\",\"external\":true}",
    };
    try db.addIndex(config);
    try db.batch(.{
        .writes = &.{.{
            .key = "doc:a",
            .value = "{\"_embeddings\":{\"dense_idx\":[1,0,0]}}",
        }},
        .sync_level = .full_index,
    });

    // This fixture exercises the certification handoff, whose prerequisite
    // is an already-published native generation. Online maintenance can return
    // busy/deferred while another owner is staging files; it is not a barrier.
    _ = try db.publishVectorBlockBasesAtStableTip();

    const counter_key = try DB.denseArtifactTargetCounterKeyAlloc(alloc, config.name);
    defer alloc.free(counter_key);
    var counter_value: [8]u8 = undefined;
    std.mem.writeInt(u64, &counter_value, 1, .little);
    try db.core.store.put(counter_key, &counter_value);
    const applied = try db.core.loadAppliedSequence(alloc, config.name);
    try db.core.saveProjectionCheckpoint(config.name, .{
        .applied_sequence = applied,
        .status = .rebuilding,
        .generation = 17,
        .config_hash = types.indexConfigHash(config),
    });

    db.async_context.apply_mutex.lockShared();
    defer db.async_context.apply_mutex.unlockShared();
    var claim_lock = lockAtomicWithBackoffProfiled(&db.async_context.dense_admission.mutex, &db.async_context.stats.dense_finish_mutex);
    const claimed = tryClaimDenseProjectionFinalizationLocked(db.async_context);
    claim_lock.unlock();
    try std.testing.expect(claimed);
    defer finishDenseProjectionFinalization(db.async_context);
    try std.testing.expect(db.snapshotAsyncIndexingStats().dense_projection_finalizing);
    try std.testing.expect(!try finalizeCoveredDenseProjectionCheckpoint(db.async_context, config.name, applied));
    try std.testing.expect(db.async_context.dense_admission.finalization_requested);
    try std.testing.expect(try drainClaimedDenseProjectionFinalizations(db.async_context));
    try std.testing.expect(!db.async_context.dense_admission.finalizing.load(.acquire));
    try std.testing.expect(!db.snapshotAsyncIndexingStats().dense_projection_finalizing);

    const checkpoint = try db.core.loadProjectionCheckpoint(alloc, config.name);
    try std.testing.expectEqual(apply_state.ProjectionStatus.clean, checkpoint.status);
    try std.testing.expectEqual(@as(u64, 18), checkpoint.generation);
}

test "db dense target coverage reads one immutable primary commit epoch" {
    const alloc = std.testing.allocator;
    var path_tmp = try TestDirectory.init("db");
    defer path_tmp.cleanup();
    const path = path_tmp.path().ptr;
    var db = try DB.open(alloc, std.mem.span(path), .{
        .primary_backend = .{ .lsm = .{ .flush_threshold = 1 } },
        .start_index_workers = false,
        .ttl_cleanup = .{ .enabled = false },
    });
    defer db.close();
    try db.addIndex(.{
        .name = "dense_idx",
        .kind = .dense_vector,
        .config_json = "{\"field\":\"embedding\",\"dims\":3}",
    });
    const generation = db.core.index_manager.coverageGenerationForIndex("dense_idx").?;
    var keys: [3][]u8 = undefined;
    var initialized: usize = 0;
    defer for (keys[0..initialized]) |key| alloc.free(key);
    inline for (.{ "produced", "skipped", "terminal_failed" }, 0..) |outcome, i| {
        keys[i] = try internal_keys.derivedCoverageOutcomeCountKeyAlloc(alloc, "dense_idx", generation, outcome);
        initialized += 1;
    }
    var empty = try db.core.store.beginReadTxnWithBlockCacheAdmission(.transient);
    defer empty.abort();
    var values: [4][8]u8 = undefined;
    var writes: [4]docstore_mod.KVPair = undefined;
    for (&writes, 0..) |*write, i| write.* = .{
        .key = if (i == 3) &internal_keys.range_document_count_key else keys[i],
        .value = internal_keys.encodeDerivedCoverageOutcomeCount(&values[i], if (i == 0 or i == 3) 1 else 0),
    };
    // Deterministically publish the first complete tuple after the target
    // reader has acquired its view. Three independent gets could see a
    // missing produced counter followed by newly present skipped/failed.
    try db.core.store.putBatch(&writes, &.{});
    try std.testing.expectEqual(@as(?u64, 0), try denseTargetCountForIndexSnapshot(db.async_context, "dense_idx", &empty));
    try std.testing.expectEqual(@as(?u64, 1), try denseTargetCountForIndexContext(db.async_context, "dense_idx"));

    var one = try db.core.store.beginReadTxnWithBlockCacheAdmission(.transient);
    defer one.abort();
    _ = internal_keys.encodeDerivedCoverageOutcomeCount(&values[0], 2);
    _ = internal_keys.encodeDerivedCoverageOutcomeCount(&values[3], 2);
    try db.core.store.putBatch(&writes, &.{});
    // Source cardinality is part of the same proof, not a second live read.
    try std.testing.expectEqual(@as(?u64, 1), try denseTargetCountForIndexSnapshot(db.async_context, "dense_idx", &one));
    try std.testing.expectEqual(@as(?u64, 2), try denseTargetCountForIndexContext(db.async_context, "dense_idx"));
    try db.core.store.putBatch(&.{}, &.{keys[1]});
    try std.testing.expectError(error.InvalidDerivedCoverageCounter, denseTargetCountForIndexContext(db.async_context, "dense_idx"));
}

test "db dense artifact rebuild rejects clean checkpoint for stale config identity" {
    const alloc = std.testing.allocator;

    var path_tmp = try TestDirectory.init("db");
    defer path_tmp.cleanup();
    const path = path_tmp.path().ptr;

    {
        var db = try DB.open(alloc, std.mem.span(path), no_background_test_open);
        defer db.close();

        try db.addIndex(.{
            .name = "dense_idx",
            .kind = .dense_vector,
            .config_json = "{\"field\":\"embedding\",\"dims\":3,\"metric\":\"l2_squared\",\"external\":true}",
        });

        try db.batch(.{
            .writes = &.{
                .{ .key = "doc:a", .value = "{\"title\":\"alpha\",\"_embeddings\":{\"dense_idx\":[1,0,0]}}" },
            },
            .sync_level = .full_index,
        });

        const target_sequence = db.core.nextDerivedSequence() -| 1;
        try db.core.saveProjectionCheckpoint("dense_idx", .{
            .applied_sequence = target_sequence,
            .status = .clean,
            .generation = 4,
            .config_hash = types.indexConfigHash(.{
                .name = "dense_idx",
                .kind = .dense_vector,
                .config_json = "{\"field\":\"embedding\",\"dims\":3,\"metric\":\"cosine\",\"external\":true}",
            }),
        });

        const stale_artifact_key = try expectedDocumentEmbeddingArtifactKeyAlloc(alloc, "doc:stale", "dense_idx");
        defer alloc.free(stale_artifact_key);
        try putDenseEmbeddingArtifactForTest(&db, alloc, stale_artifact_key, null, &[_]f32{ 0, 1, 0 });
    }

    var reopened = try DB.open(alloc, std.mem.span(path), no_replay_no_background_test_open);
    defer reopened.close();

    try std.testing.expect(try reopened.hasPendingDenseArtifactRebuild(alloc));
    try std.testing.expectEqual(@as(usize, 1), try reopened.rebuildDenseIndexesFromStoredEmbeddingArtifactsIfNeeded(alloc));
    const repair_id = (try reopened.indexRepairIdForIndex(alloc, "dense_idx")) orelse return error.TestUnexpectedResult;
    var intent = try reopened.loadIndexRepairEntryById(alloc, repair_id);
    try std.testing.expectEqual(index_repair_state.Trigger.projection_generation_invalid, intent.intent.trigger);
    intent.deinit(alloc);
    try std.testing.expect(reopened.core.index_manager.repairUnavailable("dense_idx"));
    try std.testing.expectError(error.IndexRebuilding, reopened.search(alloc, .{
        .index_name = "dense_idx",
        .query = .{ .dense_knn = .{ .vector = &.{ 1, 0, 0 }, .k = 1 } },
        .limit = 1,
    }));
}

test "db dense artifact rebuild checks artifact counters before clean checkpoint skip" {
    const alloc = std.testing.allocator;

    var path_tmp = try TestDirectory.init("db");
    defer path_tmp.cleanup();
    const path = path_tmp.path().ptr;

    const dense_cfg: types.IndexConfig = .{
        .name = "dense_idx",
        .kind = .dense_vector,
        .config_json = "{\"field\":\"embedding\",\"dims\":3,\"metric\":\"l2_squared\",\"external\":true}",
    };

    {
        var db = try openLegacyDenseTestDB(alloc, std.mem.span(path), .{
            .start_index_workers = false,
            .ttl_cleanup = .{ .enabled = false },
        });
        defer db.close();

        try db.addIndex(dense_cfg);
        const stored_key = try internal_keys.documentKeyAlloc(alloc, "doc:a");
        defer alloc.free(stored_key);
        try db.core.store.putBatch(&.{
            .{ .key = stored_key, .value = "{\"title\":\"alpha\"}" },
        }, &.{});
        const artifact_key = try expectedDocumentEmbeddingArtifactKeyAlloc(alloc, "doc:a", "dense_idx");
        defer alloc.free(artifact_key);
        try putDenseEmbeddingArtifactWithCounterForTest(&db, alloc, artifact_key, null, &[_]f32{ 1, 0, 0 });

        const target_sequence = db.core.nextDerivedSequence() -| 1;
        try db.core.saveProjectionCheckpoint("dense_idx", .{
            .applied_sequence = target_sequence,
            .status = .clean,
            .generation = 7,
            .config_hash = types.indexConfigHash(dense_cfg),
        });
    }

    var io_impl = threadedIo();
    defer io_impl.deinit();
    const dense_index_path = try std.fmt.allocPrint(alloc, "{s}/indexes/dense_idx", .{std.mem.span(path)});
    defer alloc.free(dense_index_path);
    try std.Io.Dir.cwd().deleteTree(io_impl.io(), dense_index_path);

    var reopened = try openLegacyDenseTestDB(alloc, std.mem.span(path), .{
        .open_mode = .writer_no_replay,
        .start_index_workers = false,
        .ttl_cleanup = .{ .enabled = false },
    });
    defer reopened.close();

    try std.testing.expect(try reopened.hasPendingDenseArtifactRebuild(alloc));
    try std.testing.expectEqual(@as(usize, 1), try reopened.rebuildDenseIndexesFromStoredEmbeddingArtifactsIfNeeded(alloc));
    try std.testing.expect(!(try reopened.hasPendingDenseArtifactRebuild(alloc)));

    const stats = try reopened.stats(alloc);
    defer types.freeDBStats(alloc, stats);
    try std.testing.expectEqual(@as(usize, 1), stats.indexes.len);
    try std.testing.expectEqual(@as(u64, 1), stats.indexes[0].doc_count);
}

test "db dense artifact planner does not let stale status override authoritative counter" {
    const alloc = std.testing.allocator;
    var path_tmp = try TestDirectory.init("db");
    defer path_tmp.cleanup();
    const path = path_tmp.path().ptr;

    var db = try DB.open(alloc, std.mem.span(path), no_background_test_open);
    defer db.close();
    try db.addIndex(.{
        .name = "dense_idx",
        .kind = .dense_vector,
        .config_json = "{\"field\":\"embedding\",\"dims\":3,\"metric\":\"l2_squared\",\"external\":true}",
    });
    try db.batch(.{
        .writes = &.{.{
            .key = "doc:a",
            .value = "{\"_embeddings\":{\"dense_idx\":[1,0,0]}}",
        }},
        .sync_level = .full_index,
    });
    try std.testing.expectEqual(
        @as(?u64, 1),
        try DB.loadDenseArtifactTargetCounter(alloc, db.core.store, "dense_idx"),
    );

    const status_key = try DB.indexStatusKeyAlloc(alloc, "dense_idx");
    defer alloc.free(status_key);
    var stale_status: [DB.index_status_encoded_len]u8 = undefined;
    DB.encodeIndexStatusSnapshot(.{
        .kind = .dense_vector,
        .doc_count = 99,
        .node_count = 99,
        .root_node = 99,
    }, &stale_status);
    try db.core.store.put(status_key, &stale_status);

    try std.testing.expect(!(try db.hasPendingDenseArtifactRebuild(alloc)));
    try std.testing.expect(!(try db.indexGenerationRepairRequired(alloc, "dense_idx")));
    try std.testing.expectEqual(
        @as(usize, 0),
        try db.rebuildDenseIndexesFromStoredEmbeddingArtifactsIfNeeded(alloc),
    );
}

test "db dense artifact rebuild bootstraps missing counter metadata" {
    const alloc = std.testing.allocator;

    var path_tmp = try TestDirectory.init("db");
    defer path_tmp.cleanup();
    const path = path_tmp.path().ptr;

    const dense_cfg: types.IndexConfig = .{
        .name = "dense_idx",
        .kind = .dense_vector,
        .config_json = "{\"field\":\"embedding\",\"dims\":3,\"metric\":\"l2_squared\",\"external\":true}",
    };

    {
        var db = try openLegacyDenseTestDB(alloc, std.mem.span(path), .{
            .start_index_workers = false,
            .ttl_cleanup = .{ .enabled = false },
        });
        defer db.close();

        try db.addIndex(dense_cfg);
        const counter_key = try DB.denseArtifactTargetCounterKeyAlloc(alloc, "dense_idx");
        defer alloc.free(counter_key);
        try db.core.store.putBatch(&.{}, &.{counter_key});
        const stored_key = try internal_keys.documentKeyAlloc(alloc, "doc:a");
        defer alloc.free(stored_key);
        try db.core.store.putBatch(&.{
            .{ .key = stored_key, .value = "{\"title\":\"alpha\"}" },
        }, &.{});
        const artifact_key = try expectedDocumentEmbeddingArtifactKeyAlloc(alloc, "doc:a", "dense_idx");
        defer alloc.free(artifact_key);
        try putDenseEmbeddingArtifactForTest(&db, alloc, artifact_key, null, &[_]f32{ 1, 0, 0 });

        const target_sequence = db.core.nextDerivedSequence() -| 1;
        try db.core.saveProjectionCheckpoint("dense_idx", .{
            .applied_sequence = target_sequence,
            .status = .clean,
            .generation = 9,
            .config_hash = types.indexConfigHash(dense_cfg),
        });
    }

    var io_impl = threadedIo();
    defer io_impl.deinit();
    const dense_index_path = try std.fmt.allocPrint(alloc, "{s}/indexes/dense_idx", .{std.mem.span(path)});
    defer alloc.free(dense_index_path);
    try std.Io.Dir.cwd().deleteTree(io_impl.io(), dense_index_path);

    var repair_id: u128 = 0;
    {
        var reopened = try openLegacyDenseTestDB(alloc, std.mem.span(path), .{
            .open_mode = .writer_no_replay,
            .start_index_workers = false,
            .ttl_cleanup = .{ .enabled = false },
        });
        defer reopened.close();

        try std.testing.expect(try reopened.hasPendingDenseArtifactRebuild(alloc));
        try std.testing.expectEqual(@as(usize, 1), try reopened.rebuildDenseIndexesFromStoredEmbeddingArtifactsIfNeeded(alloc));
        repair_id = (try reopened.indexRepairIdForIndex(alloc, "dense_idx")) orelse return error.TestUnexpectedResult;
        var intent = try reopened.loadIndexRepairEntryById(alloc, repair_id);
        try std.testing.expectEqual(index_repair_state.Trigger.artifact_counter_missing, intent.intent.trigger);
        intent.deinit(alloc);
        try std.testing.expectError(error.IndexRebuilding, reopened.search(alloc, .{
            .index_name = "dense_idx",
            .query = .{ .dense_knn = .{ .vector = &.{ 1, 0, 0 }, .k = 1 } },
            .limit = 1,
        }));
    }

    // The missing-proof gate is reconstructed from the durable intent before
    // any repair owner resumes work after process restart.
    {
        var resumed = try openLegacyDenseTestDB(alloc, std.mem.span(path), .{
            .open_mode = .writer_no_replay,
            .start_index_workers = false,
            .ttl_cleanup = .{ .enabled = false },
        });
        defer resumed.close();
        try std.testing.expectEqual(
            repair_id,
            (try resumed.indexRepairIdForIndex(alloc, "dense_idx")) orelse return error.TestUnexpectedResult,
        );
        try std.testing.expectError(error.IndexRebuilding, resumed.search(alloc, .{
            .index_name = "dense_idx",
            .query = .{ .dense_knn = .{ .vector = &.{ 1, 0, 0 }, .k = 1 } },
            .limit = 1,
        }));
        const repaired = try resumed.advanceIndexRepairIntent(alloc, repair_id, .{});
        try std.testing.expect(repaired.repaired);
        try std.testing.expectEqual(
            @as(?u64, 1),
            try DB.loadDenseArtifactTargetCounter(alloc, resumed.core.store, "dense_idx"),
        );
        try std.testing.expect(!(try resumed.hasPendingDenseArtifactRebuild(alloc)));
    }
}

test "db dense artifact counter bootstrap combines snapshot with concurrent write delta" {
    const alloc = std.testing.allocator;

    var path_tmp = try TestDirectory.init("db");
    defer path_tmp.cleanup();
    const path = path_tmp.path().ptr;

    var db = try DB.open(alloc, std.mem.span(path), no_background_test_open);
    defer db.close();

    try db.addIndex(.{
        .name = "dense_idx",
        .kind = .dense_vector,
        .config_json = "{\"field\":\"embedding\",\"dims\":3,\"external\":true}",
    });
    const counter_key = try DB.denseArtifactTargetCounterKeyAlloc(alloc, "dense_idx");
    defer alloc.free(counter_key);
    try db.core.store.putBatch(&.{}, &.{counter_key});

    const artifact_a = try expectedDocumentEmbeddingArtifactKeyAlloc(alloc, "doc:a", "dense_idx");
    defer alloc.free(artifact_a);
    try putDenseEmbeddingArtifactForTest(&db, alloc, artifact_a, null, &[_]f32{ 1, 0, 0 });

    const repair_id: u128 = 0x1234;
    var bootstrap_snapshot = try db.beginDenseArtifactCounterBootstrapSnapshot(alloc, "dense_idx", repair_id);
    defer bootstrap_snapshot.deinit();

    try db.batch(.{
        .writes = &.{.{
            .key = "doc:b",
            .value = "{\"title\":\"beta\",\"_embeddings\":{\"dense_idx\":[0,1,0]}}",
        }},
        .sync_level = .write,
    });
    const marker = (try DB.loadDenseArtifactCounterBootstrap(alloc, db.core.store, "dense_idx")).?;
    try std.testing.expectEqual(repair_id, marker.repair_id);
    try std.testing.expectEqual(@as(i64, 1), marker.delta);

    const snapshot_count = try db.countDenseArtifactsForConfigFromReadTxn(
        alloc,
        .{
            .name = "dense_idx",
            .kind = .dense_vector,
            .config_json = "{\"field\":\"embedding\",\"dims\":3,\"external\":true}",
        },
        &bootstrap_snapshot.txn,
        null,
    );
    try std.testing.expectEqual(@as(u64, 1), snapshot_count);
    try db.finishDenseArtifactCounterBootstrap(
        alloc,
        "dense_idx",
        repair_id,
        bootstrap_snapshot.attempt_id,
        snapshot_count,
    );

    try std.testing.expectEqual(
        @as(?u64, 2),
        try DB.loadDenseArtifactTargetCounter(alloc, db.core.store, "dense_idx"),
    );
    try std.testing.expectEqual(
        @as(?DB.DenseArtifactCounterBootstrap, null),
        try DB.loadDenseArtifactCounterBootstrap(alloc, db.core.store, "dense_idx"),
    );
}

test "db dense artifact counter bootstrap restarts from a fresh snapshot" {
    const alloc = std.testing.allocator;
    var path_tmp = try TestDirectory.init("db");
    defer path_tmp.cleanup();
    const path = path_tmp.path().ptr;
    const cfg: types.IndexConfig = .{
        .name = "dense_idx",
        .kind = .dense_vector,
        .config_json = "{\"field\":\"embedding\",\"dims\":3,\"external\":true}",
    };
    const repair_id: u128 = 0x5678;

    {
        var db = try DB.open(alloc, std.mem.span(path), no_background_test_open);
        defer db.close();
        try db.addIndex(cfg);
        const counter_key = try DB.denseArtifactTargetCounterKeyAlloc(alloc, cfg.name);
        defer alloc.free(counter_key);
        try db.core.store.putBatch(&.{}, &.{counter_key});

        const artifact_a = try expectedDocumentEmbeddingArtifactKeyAlloc(alloc, "doc:a", cfg.name);
        defer alloc.free(artifact_a);
        try putDenseEmbeddingArtifactForTest(&db, alloc, artifact_a, null, &[_]f32{ 1, 0, 0 });
        var abandoned_snapshot = try db.beginDenseArtifactCounterBootstrapSnapshot(alloc, cfg.name, repair_id);
        try db.batch(.{
            .writes = &.{.{
                .key = "doc:b",
                .value = "{\"title\":\"beta\",\"_embeddings\":{\"dense_idx\":[0,1,0]}}",
            }},
            .sync_level = .write,
        });
        abandoned_snapshot.deinit();
        try db.core.syncStore(true);
    }

    {
        var reopened = try DB.open(alloc, std.mem.span(path), no_background_test_open);
        defer reopened.close();
        var fresh_snapshot = try reopened.beginDenseArtifactCounterBootstrapSnapshot(alloc, cfg.name, repair_id);
        defer fresh_snapshot.deinit();
        const count = try reopened.countDenseArtifactsForConfigFromReadTxn(
            alloc,
            cfg,
            &fresh_snapshot.txn,
            null,
        );
        try std.testing.expectEqual(@as(u64, 2), count);
        try reopened.finishDenseArtifactCounterBootstrap(
            alloc,
            cfg.name,
            repair_id,
            fresh_snapshot.attempt_id,
            count,
        );
        try std.testing.expectEqual(
            @as(?u64, 2),
            try DB.loadDenseArtifactTargetCounter(alloc, reopened.core.store, cfg.name),
        );
        try std.testing.expect(try DB.loadDenseArtifactCounterBootstrap(alloc, reopened.core.store, cfg.name) == null);
    }
}

test "db dense artifact counter bootstrap fences stale concurrent attempt" {
    const alloc = std.testing.allocator;
    var path_tmp = try TestDirectory.init("db");
    defer path_tmp.cleanup();
    const path = path_tmp.path().ptr;

    var db = try DB.open(alloc, std.mem.span(path), no_background_test_open);
    defer db.close();
    try db.addIndex(.{
        .name = "dense_idx",
        .kind = .dense_vector,
        .config_json = "{\"field\":\"embedding\",\"dims\":3,\"external\":true}",
    });
    const counter_key = try DB.denseArtifactTargetCounterKeyAlloc(alloc, "dense_idx");
    defer alloc.free(counter_key);
    try db.core.store.putBatch(&.{}, &.{counter_key});

    const repair_id: u128 = 0x9abc;
    var stale_snapshot = try db.beginDenseArtifactCounterBootstrapSnapshot(alloc, "dense_idx", repair_id);
    defer stale_snapshot.deinit();
    var current_snapshot = try db.beginDenseArtifactCounterBootstrapSnapshot(alloc, "dense_idx", repair_id);
    defer current_snapshot.deinit();
    try std.testing.expect(stale_snapshot.attempt_id != current_snapshot.attempt_id);
    try std.testing.expectError(
        error.StaleDenseArtifactCounterBootstrap,
        db.finishDenseArtifactCounterBootstrap(
            alloc,
            "dense_idx",
            repair_id,
            stale_snapshot.attempt_id,
            0,
        ),
    );
    try db.finishDenseArtifactCounterBootstrap(
        alloc,
        "dense_idx",
        repair_id,
        current_snapshot.attempt_id,
        0,
    );
}

test "db dense repair working set scales batch to resource budget" {
    const alloc = std.testing.allocator;
    var budgets = resource_manager_mod.Options.defaultBudgets();
    budgets[@backingInt(resource_manager_mod.Slice.dense_repair_working_set)] = .{
        .soft_limit_bytes = 12 * 1024 * 1024,
        .hard_limit_bytes = 16 * 1024 * 1024,
    };
    var manager = resource_manager_mod.ResourceManager.init(.{ .budgets = budgets });
    defer manager.deinit(alloc);

    const cfg: types.IndexConfig = .{
        .name = "dense_idx",
        .kind = .dense_vector,
        .config_json = "{\"field\":\"embedding\",\"dims\":3072,\"external\":true}",
    };
    const plan = try DB.repairWorkingSetPlan(alloc, &manager, cfg);
    try std.testing.expect(plan.dense_rebuild_batch_items > 0);
    try std.testing.expect(plan.dense_rebuild_batch_items < 2048);
    try std.testing.expect(plan.reservation_bytes <= 12 * 1024 * 1024);
    var reservation = try manager.reserve(.dense_repair_working_set, plan.reservation_bytes);
    defer reservation.release();
}

test "db dense counter bootstrap admission respects soft background budget" {
    const alloc = std.testing.allocator;
    var budgets = resource_manager_mod.Options.defaultBudgets();
    budgets[@backingInt(resource_manager_mod.Slice.dense_repair_working_set)] = .{
        .soft_limit_bytes = 12 * 1024 * 1024,
        .hard_limit_bytes = 16 * 1024 * 1024,
    };
    var manager = resource_manager_mod.ResourceManager.init(.{ .budgets = budgets });
    defer manager.deinit(alloc);

    var existing = try manager.reserve(.dense_repair_working_set, 5 * 1024 * 1024);
    defer existing.release();
    try std.testing.expectError(
        error.RepairResourceUnavailable,
        reserveDenseCounterBootstrapWorkingSet(&manager),
    );
    const under_soft_pressure = manager.sliceStats(.dense_repair_working_set);
    try std.testing.expectEqual(@as(u64, 5 * 1024 * 1024), under_soft_pressure.used_bytes);
    try std.testing.expectEqual(@as(u64, 0), under_soft_pressure.hard_limit_rejections);

    existing.release();
    var admitted = try reserveDenseCounterBootstrapWorkingSet(&manager);
    defer admitted.release();
    try std.testing.expectEqual(
        @as(u64, DB.dense_counter_bootstrap_working_set_bytes),
        manager.sliceStats(.dense_repair_working_set).used_bytes,
    );
}

test "db dense artifact rebuild uses durable artifact counters instead of recount" {
    const alloc = std.testing.allocator;

    var path_tmp = try TestDirectory.init("db");
    defer path_tmp.cleanup();
    const path = path_tmp.path().ptr;

    {
        var db = try DB.open(alloc, std.mem.span(path), no_background_test_open);
        defer db.close();

        const dense_cfg: types.IndexConfig = .{
            .name = "dense_idx",
            .kind = .dense_vector,
            .config_json = "{\"field\":\"embedding\",\"dims\":3,\"metric\":\"l2_squared\",\"external\":true}",
        };
        try db.addIndex(dense_cfg);

        try db.batch(.{
            .writes = &.{
                .{ .key = "doc:a", .value = "{\"title\":\"alpha\",\"_embeddings\":{\"dense_idx\":[1,0,0]}}" },
            },
            .sync_level = .full_index,
        });

        const target_sequence = db.core.nextDerivedSequence() -| 1;
        try db.core.saveProjectionCheckpoint("dense_idx", .{
            .applied_sequence = target_sequence,
            .status = .rebuilding,
            .generation = 5,
            .config_hash = types.indexConfigHash(dense_cfg),
        });

        const stale_artifact_key = try expectedDocumentEmbeddingArtifactKeyAlloc(alloc, "doc:stale", "dense_idx");
        defer alloc.free(stale_artifact_key);
        try putDenseEmbeddingArtifactForTest(&db, alloc, stale_artifact_key, null, &[_]f32{ 0, 1, 0 });
    }

    var reopened = try DB.open(alloc, std.mem.span(path), no_replay_no_background_test_open);
    defer reopened.close();

    try std.testing.expect(!(try reopened.hasPendingDenseArtifactRebuild(alloc)));
}

test "db dense artifact counters include derived chunk embedding artifacts" {
    const alloc = std.testing.allocator;

    var path_tmp = try TestDirectory.init("db");
    defer path_tmp.cleanup();
    const path = path_tmp.path().ptr;

    var db = try DB.open(alloc, std.mem.span(path), no_background_test_open);
    defer db.close();

    try db.addIndex(.{
        .name = "semantic_idx",
        .kind = .dense_vector,
        .config_json = "{\"field\":\"embedding\",\"dims\":3,\"generator\":{\"kind\":\"dense_embedding\",\"source_field\":\"body\",\"chunk_name\":\"body_chunks_v1\",\"chunk_size\":8,\"chunk_overlap\":2,\"embedding_name\":\"chunk_dense_v1\"}}",
    });

    const artifact_key = try expectedChunkEmbeddingArtifactKeyAlloc(alloc, "doc:a", "body_chunks_v1", 0, "chunk_dense_v1");
    defer alloc.free(artifact_key);
    const payload = try enrichment_artifact_codec.encodeDenseEmbeddingAlloc(alloc, null, &[_]f32{ 1, 0, 0 });
    defer alloc.free(payload);

    var writes = std.ArrayListUnmanaged(docstore_mod.KVPair).empty;
    defer writes.deinit(alloc);
    try writes.append(alloc, .{ .key = artifact_key, .value = payload });

    var owned_keys = std.ArrayListUnmanaged([]u8).empty;
    defer {
        for (owned_keys.items) |key| alloc.free(key);
        owned_keys.deinit(alloc);
    }
    var owned_values = std.ArrayListUnmanaged([]u8).empty;
    defer {
        for (owned_values.items) |value| alloc.free(value);
        owned_values.deinit(alloc);
    }

    try DB.appendDenseArtifactCounterMutations(
        alloc,
        db.core.store,
        db.core.index_manager,
        &writes,
        &.{},
        &owned_keys,
        &owned_values,
    );
    try db.core.store.putBatch(writes.items, &.{});

    try std.testing.expectEqual(
        @as(?u64, 1),
        try DB.loadDenseArtifactTargetCounter(alloc, db.core.store, "semantic_idx"),
    );
}

test "db dense artifact rebuild force-resets corrupt external dense structure" {
    const alloc = std.testing.allocator;

    var path_tmp = try TestDirectory.init("db");
    defer path_tmp.cleanup();
    const path = path_tmp.path().ptr;

    {
        var db = try DB.open(alloc, std.mem.span(path), no_background_test_open);
        defer db.close();

        try db.addIndex(.{
            .name = "dense_idx",
            .kind = .dense_vector,
            .config_json = "{\"field\":\"embedding\",\"dims\":3,\"metric\":\"l2_squared\",\"external\":true,\"leaf_size\":2}",
        });

        try db.batch(.{
            .writes = &.{
                .{ .key = "doc:a", .value = "{\"title\":\"alpha\",\"_embeddings\":{\"dense_idx\":[1,0,0]}}" },
                .{ .key = "doc:b", .value = "{\"title\":\"beta\",\"_embeddings\":{\"dense_idx\":[0,1,0]}}" },
                .{ .key = "doc:c", .value = "{\"title\":\"gamma\",\"_embeddings\":{\"dense_idx\":[0.9,0.1,0]}}" },
                .{ .key = "doc:d", .value = "{\"title\":\"delta\",\"_embeddings\":{\"dense_idx\":[0,0,1]}}" },
                .{ .key = "doc:e", .value = "{\"title\":\"epsilon\",\"_embeddings\":{\"dense_idx\":[0.8,0.2,0]}}" },
                .{ .key = "doc:f", .value = "{\"title\":\"zeta\",\"_embeddings\":{\"dense_idx\":[0.7,0.3,0]}}" },
            },
            .sync_level = .full_index,
        });

        const dense_entry = db.core.index_manager.denseIndex("dense_idx").?;
        try std.testing.expect(dense_entry.index.stats().node_count > 1);

        var read_txn = try dense_entry.index.beginReadTxn();
        defer read_txn.abort();
        var root = try dense_entry.index.loadNode(&read_txn, dense_entry.index.metadata.root_node);
        defer root.deinit(alloc);
        try std.testing.expect(!root.is_leaf);
        try dense_entry.index.deleteNodeHeaderForTest(root.children[0]);
    }

    var reopened = try DB.open(alloc, std.mem.span(path), no_replay_no_background_test_open);
    defer reopened.close();

    // Native startup may validate lazily. Exercise the real query detection
    // path before asking the durable repair owner to replace this generation.
    try std.testing.expectError(error.IndexRebuilding, reopened.search(alloc, .{
        .index_name = "dense_idx",
        .dense = .{ .vector = &.{ 1, 0, 0 }, .k = 6 },
        .search_effort = 1,
    }));

    // The query or startup persisted the corrupt generation repair intent.
    // Broad discovery must not compete with that durable owner.
    try std.testing.expect(try reopened.hasPendingIndexRepairIntents(alloc));

    const rebuilt = try reopened.rebuildDenseIndexesFromStoredEmbeddingArtifactsIfNeeded(alloc);
    try std.testing.expectEqual(@as(usize, 0), rebuilt);
    try std.testing.expect(!try reopened.hasPendingDenseArtifactRebuild(alloc));
    try std.testing.expect(try reopened.hasPendingIndexRepairIntents(alloc));
    const repair_id = (try reopened.indexRepairIdForIndex(alloc, "dense_idx")) orelse return error.TestUnexpectedResult;
    const advanced = try reopened.advanceIndexRepairIntent(alloc, repair_id, .{});
    try std.testing.expect(advanced.repaired);
    try std.testing.expect(!(try reopened.hasPendingDenseArtifactRebuild(alloc)));

    const stats = try reopened.stats(alloc);
    defer types.freeDBStats(alloc, stats);
    var dense_doc_count: ?u64 = null;
    for (stats.indexes) |index| {
        if (std.mem.eql(u8, index.name, "dense_idx")) dense_doc_count = index.doc_count;
    }
    try std.testing.expectEqual(@as(?u64, 6), dense_doc_count);

    var result = try reopened.search(alloc, .{
        .index_name = "dense_idx",
        .query = .{ .dense_knn = .{
            .vector = &.{ 1.0, 0.0, 0.0 },
            .k = 3,
        } },
        .limit = 3,
    });
    defer result.deinit();
    try std.testing.expectEqual(@as(u32, 3), result.total_hits);
    try std.testing.expectEqualStrings("doc:a", result.hits[0].id);
}

test "db dense artifact rebuild resumes from persisted state" {
    const alloc = std.testing.allocator;
    const doc_count: usize = 8;

    var path_tmp = try TestDirectory.init("db");
    defer path_tmp.cleanup();
    const path = path_tmp.path().ptr;

    {
        var db = try openLegacyDenseTestDB(alloc, std.mem.span(path), .{
            .start_index_workers = false,
            .ttl_cleanup = .{ .enabled = false },
        });
        defer db.close();

        try db.addIndex(.{
            .name = "dense_idx",
            .kind = .dense_vector,
            .config_json = "{\"field\":\"embedding\",\"dims\":3,\"metric\":\"l2_squared\",\"external\":true}",
        });

        for (0..doc_count) |i| {
            const doc_id = try std.fmt.allocPrint(alloc, "doc:{d:0>5}", .{i});
            defer alloc.free(doc_id);
            const stored_key = try internal_keys.documentKeyAlloc(alloc, doc_id);
            defer alloc.free(stored_key);
            const stored_value = try std.fmt.allocPrint(alloc, "{{\"title\":\"doc-{d}\"}}", .{i});
            defer alloc.free(stored_value);
            try db.core.store.putBatch(&.{
                .{ .key = stored_key, .value = stored_value },
            }, &.{});

            const artifact_key = try expectedDocumentEmbeddingArtifactKeyAlloc(alloc, doc_id, "dense_idx");
            defer alloc.free(artifact_key);
            try putDenseEmbeddingArtifactWithCounterForTest(&db, alloc, artifact_key, null, &[_]f32{
                @floatFromInt(i + 1),
                0,
                0,
            });
        }
    }

    const dense_index_path = try std.fmt.allocPrint(alloc, "{s}/indexes/dense_idx", .{std.mem.span(path)});
    defer alloc.free(dense_index_path);
    var io_impl = threadedIo();
    defer io_impl.deinit();
    try std.Io.Dir.cwd().deleteTree(io_impl.io(), dense_index_path);

    {
        var interrupted = try openLegacyDenseTestDB(alloc, std.mem.span(path), .{
            .open_mode = .writer_no_replay,
            .start_index_workers = false,
            .ttl_cleanup = .{ .enabled = false },
        });
        defer interrupted.close();

        const dense_entry = interrupted.core.index_manager.denseIndex("dense_idx") orelse return error.IndexNotFound;
        const rebuild_state = interrupted.core.index_manager.rebuildState(.dense_vector, dense_index_path, dense_entry.config);
        const ResumeFailureCtx = struct {
            db: *DB,
            rebuild_state: backfill_state_mod.RebuildState,
            persisted_calls: usize = 0,
            cache_bytes_before_failure: usize = 0,

            fn persistAndFail(ctx: *anyopaque, last_key: []const u8) !void {
                const failure: *@This() = @ptrCast(@alignCast(ctx));
                try failure.rebuild_state.update(last_key);
                failure.persisted_calls += 1;
                const index = failure.db.core.index_manager.denseIndex("dense_idx") orelse return error.IndexNotFound;
                var search = try index.index.searchProfiledRequest(.{
                    .query = &.{ 1, 0, 0 },
                    .k = 1,
                });
                defer search.results.deinit();
                failure.cache_bytes_before_failure = index.index.hbcCacheStats().total_bytes;
                return error.TestInjectedFailure;
            }
        };
        var failure_ctx = ResumeFailureCtx{
            .db = &interrupted,
            .rebuild_state = rebuild_state,
        };
        var dense_visibility_attempted = false;

        try std.testing.expectError(
            error.TestInjectedFailure,
            interrupted.rebuildDenseIndexesFromStoredEmbeddingArtifactsResumeWithProgress(
                alloc,
                null,
                null,
                null,
                null,
                null,
                &failure_ctx,
                ResumeFailureCtx.persistAndFail,
                &dense_visibility_attempted,
                4,
                1,
            ),
        );
        try std.testing.expectEqual(@as(usize, 1), failure_ctx.persisted_calls);
        try std.testing.expect(failure_ctx.cache_bytes_before_failure > 0);
        try std.testing.expectEqual(@as(usize, 0), dense_entry.index.hbcCacheStats().total_bytes);

        const partial_stats = try interrupted.stats(alloc);
        defer types.freeDBStats(alloc, partial_stats);
        var partial_doc_count: ?u64 = null;
        for (partial_stats.indexes) |index| {
            if (!std.mem.eql(u8, index.name, "dense_idx")) continue;
            partial_doc_count = index.doc_count;
        }
        try std.testing.expectEqual(@as(?u64, 4), partial_doc_count);
        const persisted_resume = try rebuild_state.check(alloc);
        defer if (persisted_resume) |buf| alloc.free(buf);
        try std.testing.expect(persisted_resume != null);
    }

    {
        var resumed = try openLegacyDenseTestDB(alloc, std.mem.span(path), .{
            .open_mode = .writer_no_replay,
            .start_index_workers = false,
            .ttl_cleanup = .{ .enabled = false },
        });
        defer resumed.close();

        const dense_entry = resumed.core.index_manager.denseIndex("dense_idx") orelse return error.IndexNotFound;
        const rebuild_state = resumed.core.index_manager.rebuildState(.dense_vector, dense_index_path, dense_entry.config);
        const rebuilt = try resumed.rebuildDenseIndexesFromStoredEmbeddingArtifactsIfNeeded(alloc);
        try std.testing.expectEqual(@as(usize, 4), rebuilt);

        try std.testing.expect((try rebuild_state.check(alloc)) == null);

        const stats = try resumed.stats(alloc);
        defer types.freeDBStats(alloc, stats);
        var dense_doc_count: ?u64 = null;
        for (stats.indexes) |index| {
            if (!std.mem.eql(u8, index.name, "dense_idx")) continue;
            dense_doc_count = index.doc_count;
        }
        try std.testing.expectEqual(@as(?u64, doc_count), dense_doc_count);
    }
}

test "db dense artifact rebuild persists state through external index storage" {
    const alloc = std.testing.allocator;

    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    const path = try std.fmt.allocPrint(alloc, ".zig-cache/tmp/{s}/table.aflite", .{tmp.sub_path});
    defer alloc.free(path);
    const index_base_path = try std.fmt.allocPrint(alloc, ".zig-cache/tmp/{s}/index-namespace", .{tmp.sub_path});
    defer alloc.free(index_base_path);
    try std.Io.Dir.cwd().writeFile(std.testing.io, .{
        .sub_path = path,
        .data = "single-file-container",
    });

    var index_storage = lsm_backend_mod.MemoryStorage.init(alloc);
    defer index_storage.deinit();
    const external_storage = index_storage.storage();

    var db = try DB.open(alloc, path, .{
        .primary_backend = .{ .mem = .{} },
        .index_backends = .{
            .dense_storage_backend = .lsm,
            .dense_lsm_storage = external_storage,
        },
        .physical_root_mode = .external_backend,
        .index_base_path = index_base_path,
        .external_derived_checkpoints = false,
        .start_index_workers = false,
        .start_optional_runtimes = false,
        .ttl_cleanup = .{ .enabled = false },
    });
    defer db.close();

    try db.addIndex(.{
        .name = "dense_idx",
        .kind = .dense_vector,
        .config_json = "{\"field\":\"embedding\",\"dims\":3,\"metric\":\"l2_squared\",\"external\":true}",
    });

    try db.batch(.{
        .writes = &.{.{
            .key = "doc:a",
            .value = "{\"title\":\"alpha\"}",
        }},
        .sync_level = .write,
    });
    const artifact_key = try expectedDocumentEmbeddingArtifactKeyAlloc(alloc, "doc:a", "dense_idx");
    defer alloc.free(artifact_key);
    try putDenseEmbeddingArtifactWithCounterForTest(&db, alloc, artifact_key, null, &[_]f32{ 1, 0, 0 });

    const rebuild_root_path = try db.denseIndexRebuildStatePathAlloc(alloc, "dense_idx");
    defer alloc.free(rebuild_root_path);
    const rebuild_state = db.core.index_manager.rebuildState(
        .dense_vector,
        rebuild_root_path,
        db.core.index_manager.denseIndex("dense_idx").?.config,
    );
    try rebuild_state.updateWithIo(db.core.index_manager.checkpointIo(), "");
    const persisted_cursor = (try rebuild_state.checkWithIo(alloc, db.core.index_manager.checkpointIo())) orelse
        return error.MissingExternalRebuildState;
    defer alloc.free(persisted_cursor);
    const rebuilding_stats = try db.diagnosticStats(alloc);
    defer types.freeDBStats(alloc, rebuilding_stats);
    var saw_dense_index = false;
    var saw_active_backfill = false;
    for (rebuilding_stats.indexes) |index| {
        if (!std.mem.eql(u8, index.name, "dense_idx")) continue;
        saw_dense_index = true;
        saw_active_backfill = index.backfill_active;
        try std.testing.expectEqual(@as(u64, 0), index.doc_count);
    }
    try std.testing.expect(saw_dense_index);
    try std.testing.expect(saw_active_backfill);

    try std.testing.expectEqual(
        @as(usize, 1),
        try db.rebuildDenseIndexesFromStoredEmbeddingArtifactsIfNeeded(alloc),
    );
    try std.testing.expect((try rebuild_state.checkWithIo(alloc, db.core.index_manager.checkpointIo())) == null);

    var result = try db.search(alloc, .{
        .index_name = "dense_idx",
        .query = .{ .dense_knn = .{ .vector = &.{ 1, 0, 0 }, .k = 1 } },
        .limit = 1,
    });
    defer result.deinit();
    try std.testing.expectEqual(@as(u32, 1), result.total_hits);
    try std.testing.expectEqualStrings("doc:a", result.hits[0].id);
}

test "db dense artifact rebuild resume keys are owned by plan allocator" {
    const alloc = std.testing.allocator;
    const db_alloc = std.heap.page_allocator;
    const doc_count: usize = 3;

    var path_tmp = try TestDirectory.init("db");
    defer path_tmp.cleanup();
    const path = path_tmp.path().ptr;

    {
        var db = try openLegacyDenseTestDB(db_alloc, std.mem.span(path), .{
            .start_index_workers = false,
            .ttl_cleanup = .{ .enabled = false },
        });
        defer db.close();

        try db.addIndex(.{
            .name = "dense_idx",
            .kind = .dense_vector,
            .config_json = "{\"field\":\"embedding\",\"dims\":3,\"metric\":\"l2_squared\",\"external\":true}",
        });

        for (0..doc_count) |i| {
            const doc_id = try std.fmt.allocPrint(alloc, "doc:{d:0>5}", .{i});
            defer alloc.free(doc_id);
            const stored_key = try internal_keys.documentKeyAlloc(alloc, doc_id);
            defer alloc.free(stored_key);
            const stored_value = try std.fmt.allocPrint(alloc, "{{\"title\":\"doc-{d}\"}}", .{i});
            defer alloc.free(stored_value);
            try db.core.store.putBatch(&.{
                .{ .key = stored_key, .value = stored_value },
            }, &.{});

            const artifact_key = try expectedDocumentEmbeddingArtifactKeyAlloc(alloc, doc_id, "dense_idx");
            defer alloc.free(artifact_key);
            try putDenseEmbeddingArtifactWithCounterForTest(&db, alloc, artifact_key, null, &[_]f32{
                @floatFromInt(i + 1),
                0,
                0,
            });
        }
    }

    var io_impl = threadedIo();
    defer io_impl.deinit();
    const dense_index_path = try std.fmt.allocPrint(alloc, "{s}/indexes/dense_idx", .{std.mem.span(path)});
    defer alloc.free(dense_index_path);
    try std.Io.Dir.cwd().deleteTree(io_impl.io(), dense_index_path);

    var reopened = try openLegacyDenseTestDB(db_alloc, std.mem.span(path), .{
        .open_mode = .writer_no_replay,
        .start_index_workers = false,
        .ttl_cleanup = .{ .enabled = false },
    });
    defer reopened.close();

    const rebuilt = try reopened.rebuildDenseIndexesFromStoredEmbeddingArtifactsIfNeeded(alloc);
    try std.testing.expectEqual(doc_count, rebuilt);
    try std.testing.expectEqual(@as(u64, doc_count), reopened.core.index_manager.denseIndex("dense_idx").?.index.metadata.active_count);
}

test "db dense artifact rebuild progress counts source artifacts across multiple consumer indexes" {
    const alloc = std.testing.allocator;
    const doc_count: usize = 3;
    const shared_cfg = "{\"field\":\"embedding\",\"dims\":3,\"metric\":\"l2_squared\",\"external\":true,\"embedding_name\":\"shared_dense_v1\"}";

    var path_tmp = try TestDirectory.init("db");
    defer path_tmp.cleanup();
    const path = path_tmp.path().ptr;

    {
        var db = try openLegacyDenseTestDB(alloc, std.mem.span(path), .{
            .start_index_workers = false,
            .ttl_cleanup = .{ .enabled = false },
        });
        defer db.close();

        try db.addIndex(.{
            .name = "dense_a",
            .kind = .dense_vector,
            .config_json = shared_cfg,
        });
        try db.addIndex(.{
            .name = "dense_b",
            .kind = .dense_vector,
            .config_json = shared_cfg,
        });

        for (0..doc_count) |i| {
            const doc_id = try std.fmt.allocPrint(alloc, "doc:{d:0>5}", .{i});
            defer alloc.free(doc_id);
            const stored_key = try internal_keys.documentKeyAlloc(alloc, doc_id);
            defer alloc.free(stored_key);
            const stored_value = try std.fmt.allocPrint(alloc, "{{\"title\":\"doc-{d}\"}}", .{i});
            defer alloc.free(stored_value);
            try db.core.store.putBatch(&.{
                .{ .key = stored_key, .value = stored_value },
            }, &.{});

            const artifact_key = try expectedDocumentEmbeddingArtifactKeyAlloc(alloc, doc_id, "shared_dense_v1");
            defer alloc.free(artifact_key);
            try putDenseEmbeddingArtifactWithCounterForTest(&db, alloc, artifact_key, null, &[_]f32{
                @floatFromInt(i + 1),
                0,
                0,
            });
        }
    }

    var io_impl = threadedIo();
    defer io_impl.deinit();
    const dense_a_path = try std.fmt.allocPrint(alloc, "{s}/indexes/dense_a", .{std.mem.span(path)});
    defer alloc.free(dense_a_path);
    try std.Io.Dir.cwd().deleteTree(io_impl.io(), dense_a_path);
    const dense_b_path = try std.fmt.allocPrint(alloc, "{s}/indexes/dense_b", .{std.mem.span(path)});
    defer alloc.free(dense_b_path);
    try std.Io.Dir.cwd().deleteTree(io_impl.io(), dense_b_path);

    var reopened = try openLegacyDenseTestDB(alloc, std.mem.span(path), .{
        .open_mode = .writer_no_replay,
        .start_index_workers = false,
        .ttl_cleanup = .{ .enabled = false },
    });
    defer reopened.close();

    const Capture = struct {
        seen: usize = 0,
        last: ReplayProgress = .{},

        fn run(ptr: *anyopaque, _: []const u8, progress: ReplayProgress) !void {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            self.seen += 1;
            self.last = progress;
        }
    };
    var capture = Capture{};

    const rebuilt = try reopened.rebuildDenseIndexesFromStoredEmbeddingArtifactsIfNeededWithProgress(alloc, &capture, Capture.run);
    try std.testing.expectEqual(doc_count, rebuilt);
    try std.testing.expect(capture.seen > 0);
    try std.testing.expectEqual(@as(u64, doc_count), capture.last.target_sequence);
    try std.testing.expectEqual(@as(u64, doc_count), capture.last.applied_entries);
    try std.testing.expectEqual(@as(u64, doc_count), capture.last.sequence);
    try reopened.runUntilIdle();

    const stats = try reopened.stats(alloc);
    defer types.freeDBStats(alloc, stats);
    var dense_a_count: ?u64 = null;
    var dense_b_count: ?u64 = null;
    for (stats.indexes) |index| {
        if (std.mem.eql(u8, index.name, "dense_a")) dense_a_count = index.doc_count;
        if (std.mem.eql(u8, index.name, "dense_b")) dense_b_count = index.doc_count;
    }
    try std.testing.expectEqual(@as(?u64, doc_count), dense_a_count);
    try std.testing.expectEqual(@as(?u64, doc_count), dense_b_count);
}

test "db dense artifact rebuild keeps resume keys owned by caller allocator" {
    const alloc = std.testing.allocator;
    const rebuild_alloc = std.heap.page_allocator;

    var path_tmp = try TestDirectory.init("db");
    defer path_tmp.cleanup();
    const path = path_tmp.path().ptr;

    {
        var db = try openLegacyDenseTestDB(alloc, std.mem.span(path), .{
            .start_index_workers = false,
            .ttl_cleanup = .{ .enabled = false },
        });
        defer db.close();

        try db.addIndex(.{
            .name = "dense_idx",
            .kind = .dense_vector,
            .config_json = "{\"field\":\"embedding\",\"dims\":3,\"metric\":\"l2_squared\",\"external\":true}",
        });

        const stored_key = try internal_keys.documentKeyAlloc(alloc, "doc:a");
        defer alloc.free(stored_key);
        try db.core.store.putBatch(&.{
            .{ .key = stored_key, .value = "{\"title\":\"alpha\"}" },
        }, &.{});

        const artifact_key = try expectedDocumentEmbeddingArtifactKeyAlloc(alloc, "doc:a", "dense_idx");
        defer alloc.free(artifact_key);
        try putDenseEmbeddingArtifactWithCounterForTest(&db, alloc, artifact_key, null, &[_]f32{ 1, 0, 0 });
    }

    var io_impl = threadedIo();
    defer io_impl.deinit();
    const dense_index_path = try std.fmt.allocPrint(alloc, "{s}/indexes/dense_idx", .{std.mem.span(path)});
    defer alloc.free(dense_index_path);
    try std.Io.Dir.cwd().deleteTree(io_impl.io(), dense_index_path);

    var reopened = try openLegacyDenseTestDB(alloc, std.mem.span(path), .{
        .open_mode = .writer_no_replay,
        .start_index_workers = false,
        .ttl_cleanup = .{ .enabled = false },
    });
    defer reopened.close();

    const rebuilt = try reopened.rebuildDenseIndexesFromStoredEmbeddingArtifactsIfNeeded(rebuild_alloc);
    try std.testing.expectEqual(@as(usize, 1), rebuilt);
    try std.testing.expect(!(try reopened.hasPendingDenseArtifactRebuild(alloc)));
}

test "db dense artifact rebuild does not let resumed targets skip fresh targets" {
    const alloc = std.testing.allocator;
    const doc_count: usize = 4;
    const shared_cfg = "{\"field\":\"embedding\",\"dims\":3,\"metric\":\"l2_squared\",\"external\":true,\"embedding_name\":\"shared_dense_v1\"}";

    var path_tmp = try TestDirectory.init("db");
    defer path_tmp.cleanup();
    const path = path_tmp.path().ptr;

    {
        var db = try openLegacyDenseTestDB(alloc, std.mem.span(path), .{
            .start_index_workers = false,
            .ttl_cleanup = .{ .enabled = false },
        });
        defer db.close();

        try db.addIndex(.{
            .name = "dense_a",
            .kind = .dense_vector,
            .config_json = shared_cfg,
        });
        try db.addIndex(.{
            .name = "dense_b",
            .kind = .dense_vector,
            .config_json = shared_cfg,
        });

        for (0..doc_count) |i| {
            const doc_id = try std.fmt.allocPrint(alloc, "doc:{d:0>5}", .{i});
            defer alloc.free(doc_id);
            const stored_key = try internal_keys.documentKeyAlloc(alloc, doc_id);
            defer alloc.free(stored_key);
            const stored_value = try std.fmt.allocPrint(alloc, "{{\"title\":\"doc-{d}\"}}", .{i});
            defer alloc.free(stored_value);
            try db.core.store.putBatch(&.{
                .{ .key = stored_key, .value = stored_value },
            }, &.{});

            const artifact_key = try expectedDocumentEmbeddingArtifactKeyAlloc(alloc, doc_id, "shared_dense_v1");
            defer alloc.free(artifact_key);
            try putDenseEmbeddingArtifactWithCounterForTest(&db, alloc, artifact_key, null, &[_]f32{
                @floatFromInt(i + 1),
                0,
                0,
            });
        }
    }

    var io_impl = threadedIo();
    defer io_impl.deinit();
    const dense_a_path = try std.fmt.allocPrint(alloc, "{s}/indexes/dense_a", .{std.mem.span(path)});
    defer alloc.free(dense_a_path);
    try std.Io.Dir.cwd().deleteTree(io_impl.io(), dense_a_path);
    const dense_b_path = try std.fmt.allocPrint(alloc, "{s}/indexes/dense_b", .{std.mem.span(path)});
    defer alloc.free(dense_b_path);
    try std.Io.Dir.cwd().deleteTree(io_impl.io(), dense_b_path);
    const rebuild_state = backfill_state_mod.RebuildState.init(dense_a_path);

    {
        var interrupted = try openLegacyDenseTestDB(alloc, std.mem.span(path), .{
            .open_mode = .writer_no_replay,
            .start_index_workers = false,
            .ttl_cleanup = .{ .enabled = false },
        });
        defer interrupted.close();

        const ResumeFailureCtx = struct {
            rebuild_state: backfill_state_mod.RebuildState,
            persisted_calls: usize = 0,

            fn persistAndFail(ctx: *anyopaque, last_key: []const u8) !void {
                const failure: *@This() = @ptrCast(@alignCast(ctx));
                try failure.rebuild_state.update(last_key);
                failure.persisted_calls += 1;
                return error.TestInjectedFailure;
            }
        };
        var failure_ctx = ResumeFailureCtx{
            .rebuild_state = rebuild_state,
        };
        var dense_visibility_attempted = false;

        const targets = [_]DB.DenseArtifactRebuildTarget{
            .{ .dense_index_idx = 0 },
        };

        try std.testing.expectError(
            error.TestInjectedFailure,
            interrupted.rebuildDenseIndexesFromStoredEmbeddingArtifactsResumeWithProgress(
                alloc,
                null,
                &targets,
                null,
                null,
                null,
                &failure_ctx,
                ResumeFailureCtx.persistAndFail,
                &dense_visibility_attempted,
                2,
                1,
            ),
        );
        try std.testing.expectEqual(@as(usize, 1), failure_ctx.persisted_calls);
    }

    {
        var resumed = try openLegacyDenseTestDB(alloc, std.mem.span(path), .{
            .open_mode = .writer_no_replay,
            .start_index_workers = false,
            .ttl_cleanup = .{ .enabled = false },
        });
        defer resumed.close();

        const rebuilt = try resumed.rebuildDenseIndexesFromStoredEmbeddingArtifactsIfNeeded(alloc);
        try std.testing.expectEqual(doc_count, rebuilt);

        const stats = try resumed.stats(alloc);
        defer types.freeDBStats(alloc, stats);
        var dense_a_count: ?u64 = null;
        var dense_b_count: ?u64 = null;
        for (stats.indexes) |index| {
            if (std.mem.eql(u8, index.name, "dense_a")) dense_a_count = index.doc_count;
            if (std.mem.eql(u8, index.name, "dense_b")) dense_b_count = index.doc_count;
        }
        try std.testing.expectEqual(@as(?u64, doc_count), dense_a_count);
        try std.testing.expectEqual(@as(?u64, doc_count), dense_b_count);
    }
}

test "db dense artifact rebuild ignores stale wrong-dimension artifacts when counting progress" {
    const alloc = std.testing.allocator;

    var path_tmp = try TestDirectory.init("db");
    defer path_tmp.cleanup();
    const path = path_tmp.path().ptr;

    {
        var db = try openLegacyDenseTestDB(alloc, std.mem.span(path), .{
            .start_index_workers = false,
            .ttl_cleanup = .{ .enabled = false },
        });
        defer db.close();

        try db.addIndex(.{
            .name = "dense_idx",
            .kind = .dense_vector,
            .config_json = "{\"field\":\"embedding\",\"dims\":3,\"metric\":\"l2_squared\",\"external\":true,\"embedding_name\":\"shared_dense_v1\"}",
        });

        const stored_key = try internal_keys.documentKeyAlloc(alloc, "doc:a");
        defer alloc.free(stored_key);
        try db.core.store.putBatch(&.{
            .{ .key = stored_key, .value = "{\"title\":\"alpha\"}" },
        }, &.{});

        const valid_artifact_key = try expectedDocumentEmbeddingArtifactKeyAlloc(alloc, "doc:a", "shared_dense_v1");
        defer alloc.free(valid_artifact_key);
        try putDenseEmbeddingArtifactWithCounterForTest(&db, alloc, valid_artifact_key, null, &[_]f32{ 1, 0, 0 });

        const stale_artifact_key = try expectedDocumentEmbeddingArtifactKeyAlloc(alloc, "doc:stale", "shared_dense_v1");
        defer alloc.free(stale_artifact_key);
        try putDenseEmbeddingArtifactWithCounterForTest(&db, alloc, stale_artifact_key, null, &[_]f32{ 1, 0 });
    }

    var io_impl = threadedIo();
    defer io_impl.deinit();
    const dense_index_path = try std.fmt.allocPrint(alloc, "{s}/indexes/dense_idx", .{std.mem.span(path)});
    defer alloc.free(dense_index_path);
    try std.Io.Dir.cwd().deleteTree(io_impl.io(), dense_index_path);

    var reopened = try openLegacyDenseTestDB(alloc, std.mem.span(path), .{
        .open_mode = .writer_no_replay,
        .start_index_workers = false,
        .ttl_cleanup = .{ .enabled = false },
    });
    defer reopened.close();

    const Capture = struct {
        seen: usize = 0,
        last: ReplayProgress = .{},

        fn run(ptr: *anyopaque, _: []const u8, progress: ReplayProgress) !void {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            self.seen += 1;
            self.last = progress;
        }
    };
    var capture = Capture{};

    const rebuilt = try reopened.rebuildDenseIndexesFromStoredEmbeddingArtifactsIfNeededWithProgress(alloc, &capture, Capture.run);
    try std.testing.expectEqual(@as(usize, 1), rebuilt);
    try std.testing.expect(capture.seen > 0);
    try std.testing.expectEqual(@as(u64, 1), capture.last.target_sequence);
    try std.testing.expect(!(try reopened.hasPendingDenseArtifactRebuild(alloc)));

    const stats = try reopened.stats(alloc);
    defer types.freeDBStats(alloc, stats);
    var dense_doc_count: ?u64 = null;
    for (stats.indexes) |index| {
        if (std.mem.eql(u8, index.name, "dense_idx")) dense_doc_count = index.doc_count;
    }
    try std.testing.expectEqual(@as(?u64, 1), dense_doc_count);
}

test "db dense artifact rebuild clears stale persisted state when no valid artifacts remain" {
    const alloc = std.testing.allocator;

    var path_tmp = try TestDirectory.init("db");
    defer path_tmp.cleanup();
    const path = path_tmp.path().ptr;

    {
        var db = try openLegacyDenseTestDB(alloc, std.mem.span(path), .{
            .start_index_workers = false,
            .ttl_cleanup = .{ .enabled = false },
        });
        defer db.close();

        try db.addIndex(.{
            .name = "dense_idx",
            .kind = .dense_vector,
            .config_json = "{\"field\":\"embedding\",\"dims\":3,\"metric\":\"l2_squared\",\"external\":true,\"embedding_name\":\"shared_dense_v1\"}",
        });

        const rebuild_root_path = try db.denseIndexRebuildStatePathAlloc(alloc, "dense_idx");
        defer alloc.free(rebuild_root_path);
        const rebuild_state = backfill_state_mod.RebuildState.init(rebuild_root_path);
        try rebuild_state.update("doc:z");
    }

    {
        var reopened = try openLegacyDenseTestDB(alloc, std.mem.span(path), .{
            .open_mode = .writer_no_replay,
            .start_index_workers = false,
            .ttl_cleanup = .{ .enabled = false },
        });
        defer reopened.close();

        const rebuild_root_path = try reopened.denseIndexRebuildStatePathAlloc(alloc, "dense_idx");
        defer alloc.free(rebuild_root_path);
        const legacy_rebuild_state = backfill_state_mod.RebuildState.init(rebuild_root_path);
        var before_probe = try legacy_rebuild_state.loadWithIo(alloc, std.testing.io);
        defer before_probe.deinit(alloc);
        try std.testing.expectEqualStrings("doc:z", before_probe.valid);

        try std.testing.expect(!(try reopened.hasPendingDenseArtifactRebuild(alloc)));
        // Logical readiness is read-only and does not surface internal cursor
        // cleanup as user-visible rebuild debt.
        var after_probe = try legacy_rebuild_state.loadWithIo(alloc, std.testing.io);
        defer after_probe.deinit(alloc);
        try std.testing.expectEqualStrings("doc:z", after_probe.valid);

        try std.testing.expect(try reopened.denseArtifactRebuildMaintenanceNeeded(alloc));
        try std.testing.expect(try reopened.runDenseArtifactRebuildMaintenanceWithProgress(alloc, null, null));
        try std.testing.expect(!(try reopened.denseArtifactRebuildMaintenanceNeeded(alloc)));
        var cleared = try legacy_rebuild_state.loadWithIo(alloc, std.testing.io);
        defer cleared.deinit(alloc);
        try std.testing.expect(cleared == .absent);
    }
}

test "db dense artifact rebuild waits for replay debt instead of raw doc count alone" {
    const alloc = std.testing.allocator;

    var path_tmp = try TestDirectory.init("db");
    defer path_tmp.cleanup();
    const path = path_tmp.path().ptr;

    var appended_sequence: u64 = 0;

    {
        var db = try openLegacyDenseTestDB(alloc, std.mem.span(path), .{
            .start_index_workers = false,
            .ttl_cleanup = .{ .enabled = false },
        });
        defer db.close();

        try db.addIndex(.{
            .name = "dv_v1",
            .kind = .dense_vector,
            .config_json = "{\"field\":\"embedding\",\"dims\":2,\"external\":true}",
        });

        const stored_key = try internal_keys.documentKeyAlloc(alloc, "doc:a");
        defer alloc.free(stored_key);
        try db.core.store.putBatch(&.{
            .{ .key = stored_key, .value = "{\"title\":\"alpha\"}" },
        }, &.{});

        const artifact_key = try expectedDocumentEmbeddingArtifactKeyAlloc(alloc, "doc:a", "dv_v1");
        defer alloc.free(artifact_key);
        try putDenseEmbeddingArtifactForTest(&db, alloc, artifact_key, null, &[_]f32{ 1, 0 });

        var dense_embeddings = try alloc.alloc(derived_types.DerivedDenseEmbeddingWrite, 1);
        var batch = derived_types.DerivedBatch{
            .dense_embeddings = dense_embeddings,
        };
        defer derived_types.deinitDerivedBatch(alloc, &batch);
        dense_embeddings[0] = .{
            .index_name = try alloc.dupe(u8, "dv_v1"),
            .doc_key = try alloc.dupe(u8, "doc:a"),
            .artifact_key = try alloc.dupe(u8, artifact_key),
            .vector = try alloc.dupe(f32, &[_]f32{ 1, 0 }),
        };

        appended_sequence = db.core.store.reserveNextReplaySequence(1);
        var record = try change_journal_mod.recordFromDerivedBatch(alloc, batch, appended_sequence);
        defer change_journal_mod.deinitRecord(alloc, &record);
        const encoded = try change_journal_mod.encodeRecord(alloc, record);
        defer alloc.free(encoded);
        try replay_stream_mod.appendOpaque(alloc, db.core.store, appended_sequence, encoded);
    }

    var reopened = try openLegacyDenseTestDB(alloc, std.mem.span(path), .{
        .open_mode = .writer_no_replay,
        .start_index_workers = false,
        .ttl_cleanup = .{ .enabled = false },
    });
    defer reopened.close();

    const rebuilt = try reopened.rebuildDenseIndexesFromStoredEmbeddingArtifactsIfNeeded(alloc);
    try std.testing.expectEqual(@as(usize, 0), rebuilt);

    const rebuild_state_path = try std.fmt.allocPrint(alloc, "{s}/indexes/dv_v1", .{std.mem.span(path)});
    defer alloc.free(rebuild_state_path);
    const rebuild_state = backfill_state_mod.RebuildState.init(rebuild_state_path);
    try std.testing.expect((try rebuild_state.check(alloc)) == null);

    const replay_debt = try reopened.listDerivedReplayDebt(alloc);
    defer {
        for (replay_debt) |*status| status.deinit(alloc);
        alloc.free(replay_debt);
    }
    try std.testing.expectEqual(@as(usize, 1), replay_debt.len);
    try std.testing.expectEqual(@as(u64, 0), replay_debt[0].applied_sequence);
    try std.testing.expectEqual(appended_sequence, replay_debt[0].target_sequence);
    try std.testing.expect(replay_debt[0].catch_up_required);
}

test "db dense broad visibility filtering does not bypass ttl" {
    const alloc = std.testing.allocator;
    const ttl_duration_ns: u64 = 60 * std.time.ns_per_s;

    var path_tmp = try TestDirectory.init("db");
    defer path_tmp.cleanup();
    const path = path_tmp.path().ptr;

    var db = try DB.open(alloc, std.mem.span(path), .{});
    defer db.close();

    try db.setSchema(.{
        .version = 1,
        .default_type = "_default",
        .ttl_duration_ns = ttl_duration_ns,
    });

    const now_ns = currentTimeNs();
    try db.batch(.{
        .writes = &.{.{ .key = "doc:expired", .value = "{\"embedding\":[1,0]}" }},
        .timestamp_ns = now_ns - 2 * ttl_duration_ns,
    });
    try db.batch(.{
        .writes = &.{
            .{ .key = "doc:fresh", .value = "{\"embedding\":[0,1]}" },
            .{ .key = "doc:deleted", .value = "{\"embedding\":[0.5,0.5]}" },
        },
        .timestamp_ns = now_ns,
    });
    try db.addIndex(.{
        .name = "dv_v1",
        .kind = .dense_vector,
        .config_json = "{\"field\":\"embedding\",\"dims\":2,\"metric\":\"l2_squared\"}",
    });
    // Force broad identity visibility to have a cheap complement. TTL is a
    // second visibility dimension and must still select the positive live set.
    try db.batch(.{
        .deletes = &.{"doc:deleted"},
        .timestamp_ns = now_ns,
        .sync_level = .full_index,
    });

    var result = try db.search(alloc, .{
        .index_name = "dv_v1",
        .dense = .{ .vector = &.{ 1.0, 0.0 }, .k = 1 },
        .limit = 1,
    });
    defer result.deinit();

    try std.testing.expectEqual(@as(u32, 1), result.total_hits);
    try std.testing.expectEqual(@as(usize, 1), result.hits.len);
    try std.testing.expectEqualStrings("doc:fresh", result.hits[0].id);
}

test "db dense streaming replay qualification survives full ingest and reopen" {
    if (platform.env.getenv("ANTFLY_RUN_DENSE_STREAMING_QUALIFICATION") == null) return error.SkipZigTest;
    const alloc = if (comptime builtin.link_libc) std.heap.c_allocator else std.testing.allocator;
    const doc_count = @max(@as(usize, 1), readEnvUsize("ANTFLY_DENSE_STREAMING_QUALIFICATION_DOCS", 100_000));
    const batch_docs = @max(@as(usize, 1), readEnvUsize("ANTFLY_DENSE_STREAMING_QUALIFICATION_BATCH_DOCS", 1_000));
    const max_rss_bytes = readEnvU64("ANTFLY_DENSE_STREAMING_QUALIFICATION_MAX_RSS_BYTES", 0);
    const max_working_set_bytes = readEnvU64("ANTFLY_DENSE_STREAMING_QUALIFICATION_MAX_WORKING_SET_BYTES", 2 * 1024 * 1024 * 1024);
    var peak_rss_bytes: u64 = 0;
    var peak_working_set_bytes: u64 = 0;
    var path_tmp = try TestDirectory.init("db");
    defer path_tmp.cleanup();
    const path = path_tmp.path().ptr;

    const started_ns = monotonicTimeNs();
    {
        var db = try DB.open(alloc, std.mem.span(path), .{
            .ttl_cleanup = .{ .enabled = false },
        });
        defer db.close();
        try db.addIndex(.{
            .name = "dense_idx",
            .kind = .dense_vector,
            .config_json = "{\"field\":\"embedding\",\"dims\":8,\"metric\":\"l2_squared\",\"external\":true}",
        });

        var first: usize = 0;
        while (first < doc_count) : (first += batch_docs) {
            const count = @min(batch_docs, doc_count - first);
            const writes = try alloc.alloc(types.BatchWrite, count);
            var initialized: usize = 0;
            errdefer {
                for (writes[0..initialized]) |write| {
                    alloc.free(write.key);
                    alloc.free(write.value);
                }
                alloc.free(writes);
            }
            for (writes, 0..) |*write, offset| {
                const doc_id = first + offset;
                const key = try std.fmt.allocPrint(alloc, "doc:{d}", .{doc_id});
                errdefer alloc.free(key);
                const value = try std.fmt.allocPrint(
                    alloc,
                    "{{\"title\":\"doc-{d}\",\"_embeddings\":{{\"dense_idx\":[{d},{d},{d},{d},{d},{d},{d},{d}]}}}}",
                    .{
                        doc_id,
                        (doc_id *% 17 +% 3) % 997,
                        (doc_id *% 31 +% 5) % 991,
                        (doc_id *% 43 +% 7) % 983,
                        (doc_id *% 59 +% 11) % 977,
                        (doc_id *% 71 +% 13) % 971,
                        (doc_id *% 83 +% 17) % 967,
                        (doc_id *% 97 +% 19) % 953,
                        (doc_id *% 109 +% 23) % 947,
                    },
                );
                write.* = .{
                    .key = key,
                    .value = value,
                };
                initialized += 1;
            }
            try db.batch(.{ .writes = writes, .sync_level = .write });
            for (writes) |write| {
                alloc.free(write.key);
                alloc.free(write.value);
            }
            alloc.free(writes);
            const memory = process_memory_mod.pressureSnapshot();
            peak_rss_bytes = @max(peak_rss_bytes, memory.resident_bytes);
            const working_set_bytes = process_memory_mod.pressureWorkingSetBytes(memory);
            peak_working_set_bytes = @max(peak_working_set_bytes, working_set_bytes);
            if (memory.available and max_rss_bytes > 0 and memory.resident_bytes > max_rss_bytes) {
                printDenseStreamingQualificationDiagnostics(&db, first + count, memory);
                std.debug.print(
                    "dense_streaming_qualification exceeded_rss docs_written={d} rss_bytes={d} working_set_bytes={d} max_rss_bytes={d}\n",
                    .{ first + count, memory.resident_bytes, working_set_bytes, max_rss_bytes },
                );
                return error.ResourceBudgetExceeded;
            }
            if (memory.available and max_working_set_bytes > 0 and working_set_bytes > max_working_set_bytes) {
                printDenseStreamingQualificationDiagnostics(&db, first + count, memory);
                std.debug.print(
                    "dense_streaming_qualification exceeded_working_set docs_written={d} rss_bytes={d} working_set_bytes={d} max_working_set_bytes={d}\n",
                    .{ first + count, memory.resident_bytes, working_set_bytes, max_working_set_bytes },
                );
                return error.ResourceBudgetExceeded;
            }
        }

        const target_sequence = db.core.nextDerivedSequence();
        try db.executor.waitForAll(target_sequence);
        const replay_memory = process_memory_mod.pressureSnapshot();
        peak_rss_bytes = @max(peak_rss_bytes, replay_memory.resident_bytes);
        const replay_working_set_bytes = process_memory_mod.pressureWorkingSetBytes(replay_memory);
        peak_working_set_bytes = @max(peak_working_set_bytes, replay_working_set_bytes);
        if (replay_memory.available and max_rss_bytes > 0 and replay_memory.resident_bytes > max_rss_bytes) {
            printDenseStreamingQualificationDiagnostics(&db, doc_count, replay_memory);
            return error.ResourceBudgetExceeded;
        }
        if (replay_memory.available and max_working_set_bytes > 0 and replay_working_set_bytes > max_working_set_bytes) {
            printDenseStreamingQualificationDiagnostics(&db, doc_count, replay_memory);
            return error.ResourceBudgetExceeded;
        }
        const stats = try db.stats(alloc);
        defer types.freeDBStats(alloc, stats);
        try std.testing.expectEqual(@as(u64, @intCast(doc_count)), stats.indexes[0].doc_count);
        try std.testing.expectEqual(target_sequence, stats.indexes[0].replay_applied_sequence);
        try db.core.index_manager.syncAll(true);
    }

    var reopened = try DB.open(alloc, std.mem.span(path), no_replay_no_background_test_open);
    defer reopened.close();
    try std.testing.expect(reopened.core.index_manager.loadFailure("dense_idx") == null);
    const reopened_stats = try reopened.stats(alloc);
    defer types.freeDBStats(alloc, reopened_stats);
    try std.testing.expectEqual(@as(u64, @intCast(doc_count)), reopened_stats.indexes[0].doc_count);
    var result = try reopened.search(alloc, .{
        .index_name = "dense_idx",
        .query = .{ .dense_knn = .{
            .vector = &.{ 3, 5, 7, 11, 13, 17, 19, 23 },
            .k = 1,
        } },
        .limit = 1,
    });
    defer result.deinit();
    try std.testing.expectEqual(@as(u32, 1), result.total_hits);
    try std.testing.expectEqualStrings("doc:0", result.hits[0].id);
    std.debug.print(
        "dense_streaming_qualification docs={d} batch_docs={d} elapsed_ms={d} peak_rss_bytes={d} peak_working_set_bytes={d}\n",
        .{ doc_count, batch_docs, @divTrunc(monotonicTimeNs() - started_ns, std.time.ns_per_ms), peak_rss_bytes, peak_working_set_bytes },
    );
}

test "db dense auto bulk finish wakes weak-sync replay and publishes visibility after catch-up" {
    const alloc = std.testing.allocator;

    var path_tmp = try TestDirectory.init("db");
    defer path_tmp.cleanup();
    const path = path_tmp.path().ptr;

    var db = try DB.open(alloc, std.mem.span(path), .{});
    defer db.close();

    try db.addIndex(.{
        .name = "dense_idx",
        .kind = .dense_vector,
        .config_json = "{\"field\":\"embedding\",\"dims\":3,\"metric\":\"l2_squared\",\"external\":true}",
    });
    try db.core.saveProjectionCheckpoint("dense_idx", .{ .status = .rebuilding });

    const HookCtx = struct {
        mutex: *std.Io.Mutex,
        changed_db: ?*DB,
        publish_calls: u64 = 0,
        status_calls: u64 = 0,
        status_calls_after_wal_checkpoint: u64 = 0,
        publish_blocking_calls: u64 = 0,
        publish_blocking_while_applied_sequence_locked: u64 = 0,
        publish_blocking_with_clean_checkpoint: u64 = 0,
        publish_blocking_checkpoint_applied: u64 = 0,
        publish_blocking_checkpoint_clean: bool = false,
        invalidate_calls: u64 = 0,

        fn onChange(ptr: *anyopaque, event: QueryVisibilityEvent) void {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            self.mutex.lockUncancelable(std.testing.io);
            defer self.mutex.unlock(std.testing.io);
            switch (event.change) {
                .status => {
                    self.publish_calls += 1;
                    self.status_calls += 1;
                    if (self.changed_db) |hook_db| {
                        if (hook_db.core.denseIndex("dense_idx")) |entry| if (entry.index.snapshotLsmMaintenanceStats()) |stats| {
                            const checkpoint = hook_db.core.index_manager.denseProjectionCheckpointMetadata("dense_idx") orelse return;
                            if (checkpoint.applied_sequence >= 4 and
                                stats.mutable_entries == 0 and
                                stats.immutable_memtables == 0 and
                                stats.wal_checkpoint_current_segment > 0 and
                                stats.wal_checkpoint_lag_segments == 0)
                            {
                                self.status_calls_after_wal_checkpoint += 1;
                            }
                        };
                    }
                },
                .publish, .publish_consistent => self.publish_calls += 1,
                .publish_blocking => {
                    self.publish_calls += 1;
                    self.publish_blocking_calls += 1;
                    if (self.changed_db) |hook_db| {
                        if (hook_db.async_context.applied_sequence_mutex.tryLock()) {
                            hook_db.async_context.applied_sequence_mutex.unlock();
                        } else {
                            self.publish_blocking_while_applied_sequence_locked += 1;
                        }
                        if (hook_db.core.index_manager.denseProjectionCheckpointMetadata("dense_idx")) |checkpoint| {
                            self.publish_blocking_checkpoint_applied = checkpoint.applied_sequence;
                            self.publish_blocking_checkpoint_clean = checkpoint.status == .clean;
                            if (checkpoint.status == .clean and checkpoint.applied_sequence >= 4) {
                                self.publish_blocking_with_clean_checkpoint += 1;
                            }
                        }
                    }
                },
                .invalidate => self.invalidate_calls += 1,
                .activity, .target_advanced, .index_repair_pending, .index_repair_cleared, .index_repair_progress => {},
            }
        }
    };
    var hook_mutex: std.Io.Mutex = .init;
    var hook_ctx = HookCtx{ .mutex = &hook_mutex, .changed_db = &db };
    db.setQueryVisibilityHook(.{
        .ptr = &hook_ctx,
        .on_change = HookCtx.onChange,
    });

    try db.beginDenseAutoBulkIngestSession();
    errdefer abortDenseAutoBulkIngestSession(&db);

    inline for (.{ "write", "propose", "write", "propose" }, 0..) |level, i| {
        const key = try std.fmt.allocPrint(alloc, "doc:{d}", .{i});
        defer alloc.free(key);
        const value = try std.fmt.allocPrint(
            alloc,
            "{{\"_embeddings\":{{\"dense_idx\":[{d}.0,0.0,0.0]}}}}",
            .{i + 1},
        );
        defer alloc.free(value);
        try db.batch(.{
            .writes = &.{.{ .key = key, .value = value }},
            .sync_level = types.parsePublicSyncLevelText(level).?,
        });
    }

    // Finish the implicit bulk publish, but hold the deferred executor wake so
    // the test can prove the replay catch-up itself publishes fresh visibility.
    try finishDenseAutoBulkIngestSessionWithOptionsInternal(&db, .{ .compact = false }, false);
    hook_mutex.lockUncancelable(std.testing.io);
    hook_ctx = .{ .mutex = &hook_mutex, .changed_db = &db };
    hook_mutex.unlock(std.testing.io);

    flushDeferredExternalBulkExecutorNotification(db.async_context, db.executor);
    try db.executor.waitForAll(4);
    // Replay completion schedules native publication; its clean visibility
    // edge is delivered by maintenance, not synchronously by the executor.
    const deadline = monotonicTimeNs() +| 10 * std.time.ns_per_s;
    var observed: HookCtx = undefined;
    while (true) {
        hook_mutex.lockUncancelable(std.testing.io);
        observed = hook_ctx;
        hook_mutex.unlock(std.testing.io);
        if (observed.publish_blocking_with_clean_checkpoint > 0) break;
        if (monotonicTimeNs() >= deadline) return error.TestUnexpectedResult;
        try std.testing.io.sleep(std.Io.Duration.fromMilliseconds(1), .awake);
    }

    try std.testing.expect(observed.publish_calls > 0);
    try std.testing.expect(observed.publish_blocking_calls > 0);
    try std.testing.expectEqual(@as(u64, 0), observed.publish_blocking_while_applied_sequence_locked);
    try std.testing.expectEqual(@as(u64, 4), observed.publish_blocking_checkpoint_applied);
    try std.testing.expect(observed.publish_blocking_checkpoint_clean);
    try std.testing.expect(observed.publish_blocking_with_clean_checkpoint > 0);
    try std.testing.expectEqual(@as(u64, 0), observed.invalidate_calls);
    const stats = try db.stats(alloc);
    defer types.freeDBStats(alloc, stats);
    try std.testing.expectEqual(@as(u64, 4), stats.indexes[0].replay_applied_sequence);
    try std.testing.expectEqual(@as(u64, 4), stats.indexes[0].replay_target_sequence);
    try std.testing.expectEqualStrings("clean", stats.indexes[0].projection_checkpoint_status);

    const persisted_status = (try db.loadIndexStatusSnapshot(alloc, "dense_idx")).?;
    try std.testing.expectEqual(@as(u64, 4), persisted_status.doc_count);
    try std.testing.expectEqual(@as(u64, 4), try db.core.loadAppliedSequence(alloc, "dense_idx"));
}

test "db dense auto bulk finish wakes current replay target if deferred wake is absent" {
    const alloc = std.testing.allocator;

    var path_tmp = try TestDirectory.init("db");
    defer path_tmp.cleanup();
    const path = path_tmp.path().ptr;

    var db = try DB.open(alloc, std.mem.span(path), .{});
    defer db.close();

    try db.addIndex(.{
        .name = "dense_idx",
        .kind = .dense_vector,
        .config_json = "{\"field\":\"embedding\",\"dims\":3,\"metric\":\"l2_squared\",\"external\":true}",
    });

    try db.beginDenseAutoBulkIngestSession();
    errdefer abortDenseAutoBulkIngestSession(&db);

    try db.batch(.{
        .writes = &.{
            .{ .key = "doc:a", .value = "{\"_embeddings\":{\"dense_idx\":[1.0,0.0,0.0]}}" },
        },
        .sync_level = .write,
    });

    const target_sequence = db.core.nextDerivedSequence();
    try std.testing.expect(target_sequence > 0);
    db.async_context.dense_admission.deferred_sequence.store(0, .release);

    try db.finishDenseAutoBulkIngestSessionWithOptions(.{ .compact = false });
    try db.executor.waitForAll(target_sequence);

    const stats = try db.stats(alloc);
    defer types.freeDBStats(alloc, stats);
    try std.testing.expectEqual(target_sequence, stats.indexes[0].replay_applied_sequence);
    try std.testing.expectEqual(@as(u64, 1), stats.indexes[0].doc_count);
}

test "db dense auto bulk replays packed external embedding strings" {
    const alloc = std.testing.allocator;

    var path_tmp = try TestDirectory.init("db");
    defer path_tmp.cleanup();
    const path = path_tmp.path().ptr;

    var db = try DB.open(alloc, std.mem.span(path), .{});
    defer db.close();

    try db.addIndex(.{
        .name = "dense_idx",
        .kind = .dense_vector,
        .config_json = "{\"field\":\"embedding\",\"dims\":3,\"metric\":\"l2_squared\",\"external\":true}",
    });

    try db.beginDenseAutoBulkIngestSession();
    errdefer abortDenseAutoBulkIngestSession(&db);

    try db.batch(.{
        .writes = &.{
            .{ .key = "doc:a", .value = "{\"_embeddings\":{\"dense_idx\":\"AACAPwAAAAAAAAAA\"}}" },
            .{ .key = "doc:b", .value = "{\"_embeddings\":{\"dense_idx\":\"AAAAAAAAAAAAAIA/\"}}" },
        },
        .sync_level = .write,
    });

    const target_sequence = db.core.nextDerivedSequence();
    try db.finishDenseAutoBulkIngestSessionWithOptions(.{ .compact = false });
    try db.executor.waitForAll(target_sequence);

    const stats = try db.stats(alloc);
    defer types.freeDBStats(alloc, stats);
    try std.testing.expectEqual(@as(u64, 2), stats.indexes[0].doc_count);
}

test "db dense lsm cache profile benchmark" {
    if (!profileBenchTestsEnabled()) return error.SkipZigTest;
    const alloc = std.testing.allocator;

    var path_tmp = try TestDirectory.init("db");
    defer path_tmp.cleanup();
    const path = path_tmp.path().ptr;

    var lsm_cache = lsm_backend_mod.Cache.init(alloc, 64 * 1024 * 1024);
    defer lsm_cache.deinit();

    var db = try DB.open(alloc, std.mem.span(path), .{
        .primary_backend = .{ .lsm = .{ .flush_threshold = 1 } },
        .lsm_cache = &lsm_cache,
    });
    defer db.close();

    try db.addIndex(.{
        .name = "dv_v1",
        .kind = .dense_vector,
        .config_json = "{\"field\":\"embedding\",\"dims\":32,\"metric\":\"cosine\"}",
    });

    const doc_count: usize = 8192;
    const dims: usize = 32;
    const reps: usize = 20;

    const writes = try alloc.alloc(types.BatchWrite, doc_count);
    defer {
        for (writes) |write| {
            alloc.free(write.key);
            alloc.free(write.value);
        }
        alloc.free(writes);
    }

    const vector_buf = try alloc.alloc(f32, dims);
    defer alloc.free(vector_buf);

    for (writes, 0..) |*write, doc_id| {
        var norm_sq: f32 = 0;
        for (vector_buf, 0..) |*slot, dim| {
            const raw: u32 = @intCast((doc_id * 1315423911 + dim * 2654435761 + 17) % 1000);
            const centered = (@as(f32, @floatFromInt(raw)) / 500.0) - 1.0;
            slot.* = centered;
            norm_sq += centered * centered;
        }
        const inv_norm: f32 = 1.0 / @sqrt(norm_sq);
        for (vector_buf) |*slot| slot.* *= inv_norm;

        write.* = .{
            .key = try std.fmt.allocPrint(alloc, "doc:{d}", .{doc_id}),
            .value = try std.fmt.allocPrint(
                alloc,
                "{{\"title\":\"doc-{d}\",\"embedding\":{f}}}",
                .{ doc_id, std.json.fmt(vector_buf, .{}) },
            ),
        };
    }

    try db.batch(.{
        .writes = writes,
        .sync_level = .full_index,
    });

    const dense_entry = db.core.denseIndex("dv_v1").?;
    const query = vector_buf[0..dims];
    @memcpy(query, &[_]f32{
        0.31,  -0.09, 0.27,  -0.41, 0.12,  0.05,  -0.33, 0.44,
        -0.11, 0.22,  -0.18, 0.39,  -0.28, 0.07,  0.14,  -0.36,
        0.25,  -0.19, 0.08,  0.17,  -0.45, 0.29,  -0.04, 0.35,
        -0.23, 0.16,  0.03,  -0.27, 0.41,  -0.15, 0.21,  -0.32,
    });
    var query_norm_sq: f32 = 0;
    for (query) |value| query_norm_sq += value * value;
    const query_inv_norm: f32 = 1.0 / @sqrt(query_norm_sq);
    for (query) |*value| value.* *= query_inv_norm;

    const dense_query: types.DenseKnnQuery = .{
        .vector = query,
        .k = 100,
    };

    const req: types.SearchRequest = .{
        .index_name = "dv_v1",
        .query = .{ .dense_knn = dense_query },
        .dense = dense_query,
        .limit = 100,
        .include_stored = false,
    };

    const cache_before = lsm_cache.snapshotStats();
    const block_hits_before = cacheBlockHitsForBench(cache_before);
    const index_hits_before = cache_before.run_table_index.hits;

    const cold_hbc_start = monotonicTimeNs();
    var cold_profiled = try dense_entry.index.searchProfiledRequest(.{
        .query = query,
        .k = 100,
    });
    const cold_hbc_ns: u64 = monotonicTimeNs() - cold_hbc_start;
    defer cold_profiled.results.deinit();

    const cold_db_start = monotonicTimeNs();
    var cold_result = try db.search(alloc, req);
    const cold_db_ns: u64 = monotonicTimeNs() - cold_db_start;
    defer cold_result.deinit();

    var warm_hbc_total_ns: u64 = 0;
    var warm_hbc_rerank_load_ns: u64 = 0;
    var warm_hbc_rerank_ns: u64 = 0;
    var warm_hbc_total_reranked: u64 = 0;
    var warm_dense_total_ns: u64 = 0;
    const warm_dense_index_lookup_ns: u64 = 0;
    var warm_dense_hbc_search_ns: u64 = 0;
    var warm_dense_doc_key_resolve_ns: u64 = 0;
    var warm_dense_load_projected_ns: u64 = 0;
    var warm_dense_postprocess_ns: u64 = 0;
    var warm_dense_inline_metadata_hits: u64 = 0;
    var warm_dense_fetched_metadata_hits: u64 = 0;
    var warm_dense_lookup_doc_key_hits: u64 = 0;
    for (0..reps) |_| {
        const start = monotonicTimeNs();
        var profiled = try dense_entry.index.searchProfiledRequest(.{
            .query = query,
            .k = 100,
        });
        warm_hbc_total_ns += monotonicTimeNs() - start;
        warm_hbc_rerank_load_ns += profiled.profile.rerank_vector_load_ns;
        warm_hbc_rerank_ns += profiled.profile.rerank_ns;
        warm_hbc_total_reranked += profiled.profile.reranked_vectors;
        profiled.results.deinit();
    }

    for (0..reps) |_| {
        const total_start = monotonicTimeNs();
        const profiled_entry = dense_entry;
        const chunk_backed = profiled_entry.chunk_name != null;
        const group_chunk_parents = db_query_search.shouldGroupChunkParents(req, chunk_backed);
        const effective_k: u32 = if (group_chunk_parents)
            @intCast(profiled_entry.index.metadata.active_count)
        else
            req.dense.?.k;
        const effort = normalizedBenchSearchEffort(req.search_effort);

        const hbc_search_start = monotonicTimeNs();
        var dense_results = try profiled_entry.index.searchWithRequest(.{
            .query = req.dense.?.vector,
            .k = effective_k,
            .search_width = resolveBenchSearchWidth(req.dense.?.k, effort, profiled_entry.index.stats()),
            .epsilon = resolveBenchSearchEpsilon(effort),
            .filter_prefix = req.filter_prefix,
            .distance_over = req.distance_over,
            .distance_under = req.distance_under,
            .filter_ids = req.filter_ids,
            .exclude_ids = req.exclude_ids,
        });
        warm_dense_hbc_search_ns += monotonicTimeNs() - hbc_search_start;
        defer dense_results.deinit();

        const raw_hits = dense_results.getHits();
        const start: u32 = if (group_chunk_parents) 0 else @min(req.offset, @as(u32, @intCast(raw_hits.len)));
        const end: u32 = if (group_chunk_parents) @intCast(raw_hits.len) else @min(start + req.limit, @as(u32, @intCast(raw_hits.len)));

        var hits = std.ArrayListUnmanaged(types.SearchHit).empty;
        errdefer {
            for (hits.items) |*hit| hit.deinit(alloc);
            hits.deinit(alloc);
        }

        for (raw_hits[@intCast(start)..@intCast(end)], 0..) |hit, i| {
            const result_index: usize = @as(usize, @intCast(start)) + i;
            const resolve_start = monotonicTimeNs();
            const doc_key = if (dense_results.takeMetadata(result_index)) |metadata| blk: {
                warm_dense_inline_metadata_hits += 1;
                break :blk metadata;
            } else blk: {
                if (try profiled_entry.index.getMetadata(hit.vector_id)) |metadata| {
                    warm_dense_fetched_metadata_hits += 1;
                    break :blk metadata;
                }
                const looked_up = (try db.core.index_manager.lookupDenseDocKey(
                    db.core.store,
                    profiled_entry.config.name,
                    hit.vector_id,
                )) orelse {
                    warm_dense_doc_key_resolve_ns += monotonicTimeNs() - resolve_start;
                    continue;
                };
                warm_dense_lookup_doc_key_hits += 1;
                break :blk looked_up;
            };
            warm_dense_doc_key_resolve_ns += monotonicTimeNs() - resolve_start;

            const stored_data = if (req.include_stored and !(chunk_backed and group_chunk_parents)) blk: {
                const load_start = monotonicTimeNs();
                const loaded = try projectOwnedStoredBytesForSearch(
                    &db,
                    alloc,
                    req,
                    doc_key,
                    (try db.get(alloc, doc_key)) orelse return error.StoredDocMissing,
                );
                warm_dense_load_projected_ns += monotonicTimeNs() - load_start;
                break :blk loaded;
            } else null;

            try hits.append(alloc, .{
                .id = doc_key,
                .score = vector_mod.similarityFromDistance(hit.distance, profiled_entry.metric),
                .distance = hit.distance,
                .stored_data = stored_data,
            });
        }

        const postprocess_start = monotonicTimeNs();
        var profiled_result = try DB.postprocessVectorSearchResultCallback(
            @ptrCast(&db),
            alloc,
            req,
            .{
                .alloc = alloc,
                .hits = try hits.toOwnedSlice(alloc),
                .total_hits = @intCast(hits.items.len),
                .graph_results = &.{},
            },
            chunk_backed,
        );
        warm_dense_postprocess_ns += monotonicTimeNs() - postprocess_start;
        warm_dense_total_ns += monotonicTimeNs() - total_start;
        profiled_result.deinit();
    }

    var warm_db_total_ns: u64 = 0;
    for (0..reps) |_| {
        const start = monotonicTimeNs();
        var result = try db.search(alloc, req);
        warm_db_total_ns += monotonicTimeNs() - start;
        result.deinit();
    }

    const cache_after = lsm_cache.snapshotStats();
    const block_hits_after = cacheBlockHitsForBench(cache_after);
    const index_hits_after = cache_after.run_table_index.hits;
    const warm_db_overhead_total_ns = if (warm_db_total_ns > warm_dense_total_ns)
        warm_db_total_ns - warm_dense_total_ns
    else
        0;

    std.debug.print(
        "dense_lsm_cache_profile docs={d} reps={d} cold_hbc_ms={d} cold_db_ms={d} warm_hbc_avg_ms={d} warm_db_avg_ms={d} warm_dense_total_avg_ms={d} warm_dense_index_lookup_avg_ms={d} warm_dense_hbc_search_avg_ms={d} warm_dense_doc_key_avg_ms={d} warm_dense_load_projected_avg_ms={d} warm_dense_postprocess_avg_ms={d} warm_db_overhead_avg_ms={d} warm_rerank_load_avg_ms={d} warm_rerank_avg_ms={d} avg_reranked={d} avg_inline_metadata_hits={d} avg_fetched_metadata_hits={d} avg_lookup_doc_key_hits={d} cache_index_hits_delta={d} cache_block_hits_delta={d}\n",
        .{
            doc_count,
            reps,
            @divTrunc(cold_hbc_ns, std.time.ns_per_ms),
            @divTrunc(cold_db_ns, std.time.ns_per_ms),
            @divTrunc(@divTrunc(warm_hbc_total_ns, reps), std.time.ns_per_ms),
            @divTrunc(@divTrunc(warm_db_total_ns, reps), std.time.ns_per_ms),
            @divTrunc(@divTrunc(warm_dense_total_ns, reps), std.time.ns_per_ms),
            @divTrunc(@divTrunc(warm_dense_index_lookup_ns, reps), std.time.ns_per_ms),
            @divTrunc(@divTrunc(warm_dense_hbc_search_ns, reps), std.time.ns_per_ms),
            @divTrunc(@divTrunc(warm_dense_doc_key_resolve_ns, reps), std.time.ns_per_ms),
            @divTrunc(@divTrunc(warm_dense_load_projected_ns, reps), std.time.ns_per_ms),
            @divTrunc(@divTrunc(warm_dense_postprocess_ns, reps), std.time.ns_per_ms),
            @divTrunc(@divTrunc(warm_db_overhead_total_ns, reps), std.time.ns_per_ms),
            @divTrunc(@divTrunc(warm_hbc_rerank_load_ns, reps), std.time.ns_per_ms),
            @divTrunc(@divTrunc(warm_hbc_rerank_ns, reps), std.time.ns_per_ms),
            @divTrunc(warm_hbc_total_reranked, reps),
            @divTrunc(warm_dense_inline_metadata_hits, reps),
            @divTrunc(warm_dense_fetched_metadata_hits, reps),
            @divTrunc(warm_dense_lookup_doc_key_hits, reps),
            index_hits_after - index_hits_before,
            block_hits_after - block_hits_before,
        },
    );

    try std.testing.expectEqual(@as(u32, 100), cold_result.total_hits);
}

test "db dense artifact rebuild write cleanup tolerates artifact-backed empty vectors" {
    const alloc = std.testing.allocator;

    var writes = std.ArrayListUnmanaged(mapper.DenseEmbeddingWrite).empty;
    defer writes.deinit(alloc);

    try writes.append(alloc, .{
        .index_name = try alloc.dupe(u8, "dv_v1"),
        .doc_key = try alloc.dupe(u8, "doc:a"),
        .artifact_key = try alloc.dupe(u8, "artifact:dense:doc:a"),
        .vector = &.{},
    });

    DB.freeDenseArtifactRebuildWrites(alloc, &writes);
    try std.testing.expectEqual(@as(usize, 0), writes.items.len);
}

test "db dense artifact rebuild chunks retain nonzero posting capture coverage" {
    const alloc = std.testing.allocator;
    var path_tmp = try TestDirectory.init("db");
    defer path_tmp.cleanup();
    const path = path_tmp.path().ptr;
    var db = try DB.open(alloc, std.mem.span(path), .{
        .start_index_workers = false,
        .start_optional_runtime_workers = false,
        .ttl_cleanup = .{ .enabled = false },
    });
    defer db.close();
    const name = "dense_idx";
    try db.addIndex(.{
        .name = name,
        .kind = .dense_vector,
        .config_json = "{\"field\":\"embedding\",\"dims\":3,\"metric\":\"l2_squared\",\"external\":true}",
    });
    try db.batch(.{
        .writes = &.{.{ .key = "doc:a", .value = "{\"_embeddings\":{\"dense_idx\":[1,0,0]}}" }},
        .sync_level = .full_index,
    });
    const dense = db.core.index_manager.denseIndex(name).?;
    const covered = dense.index.experimentalPostingDurableAppliedSequence() orelse return error.TestUnexpectedResult;
    try std.testing.expect(covered > 0);
    const artifact_key = try expectedDocumentEmbeddingArtifactKeyAlloc(alloc, "doc:a", name);
    defer alloc.free(artifact_key);
    try putDenseEmbeddingArtifactWithCounterForTest(&db, alloc, artifact_key, null, &.{ 0, 1, 0 });
    try std.testing.expectEqual(@as(usize, 1), try db.rebuildDenseIndexesFromStoredEmbeddingArtifacts(alloc));
    try std.testing.expectEqual(@as(?u64, covered), dense.index.experimentalPostingDurableAppliedSequence());
    try std.testing.expectEqual(covered, try db.core.loadAppliedSequence(alloc, name));
    try std.testing.expectEqual(@as(u64, 1), dense.index.stats().active_count);
}
