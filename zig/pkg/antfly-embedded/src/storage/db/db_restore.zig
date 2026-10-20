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

//! Restore operations of the local storage DB. Methods take `*DB` and
//! are re-exported from `DB` in db.zig, so callers keep writing `db.method()`.

const db_zig = @import("db.zig");
const DB = db_zig.DB;
const RelationalProjectionPlan = @import("query/relational_projection.zig").Plan;
const TestDirectory = @import("../../common/test_directory.zig").TestDirectory;
const antfly_test_error_logs = @import("antfly_test_error_logs");
const WriterPreferringRwLock = @import("antfly_platform").sync.WriterPreferringRwLock;
const apply_state = @import("derived/apply_state.zig");
const artifact_ids = @import("artifact/ids.zig");
const artifact_reconcile_intent = @import("artifact/reconcile_intent.zig");
const artifact_source_gap = @import("artifact/source_gap.zig");
const background_runtime_mod = @import("../background_runtime.zig");
const builtin = @import("builtin");
const db_core = @import("core.zig");
const db_query_projection = @import("query/projection.zig");
const doc_identity = @import("doc_identity.zig");
const docstore_mod = @import("../docstore.zig");
const document_mutation_revision_mod = @import("../document_mutation_revision.zig");
const embedder_mod = @import("enrichment/embedder.zig");
const enrichment_artifact_codec = @import("enrichment/artifact_codec.zig");
const enrichment_runtime_mod = @import("enrichment/enrichment_runtime.zig");
const enrichment_state = @import("enrichment/enrichment_state.zig");
const generation_lifecycle = @import("generation_lifecycle.zig");
const graph_edge_ttl_expiration = @import("graph_edge_ttl_expiration.zig");
const graph_mod = @import("../../graph/graph.zig");
const hbc_mod = @import("../hbc_adapter.zig");
const index_manager_mod = @import("catalog/index_manager.zig");
const index_repair_state = @import("derived/index_repair_state.zig");
const internal_keys = @import("../internal_keys.zig");
const lsm_backend_mod = @import("../lsm_backend/mod.zig");
const mem_backend_mod = @import("../mem_backend.zig");
const native_backup = @import("native/backup.zig");
const native_backup_seal = @import("native/backup_seal.zig");
const native_query_cut = @import("native/query_cut.zig");
const online_source = @import("online_source.zig");
const platform = @import("antfly_platform");
const portable_activation_recovery = @import("portable_activation_recovery.zig");
const portable_backup = @import("../portable_backup.zig");
const public_table_schema = @import("../../schema/mod.zig");
const range_cardinality = @import("range_cardinality.zig");
const range_state_mod = @import("range_state.zig");
const relational_constraint_jobs = @import("relational_constraint_jobs.zig");
const relational_index_catalog = @import("relational_index_catalog.zig");
const relational_index_jobs = @import("relational_index_jobs.zig");
const relational_index_records = @import("relational_index_records.zig");
const relational_integrity_activation = @import("relational_integrity_activation.zig");
const relational_integrity_catalog = @import("relational_integrity_catalog.zig");
const relational_integrity_retirement = @import("relational_integrity_retirement.zig");
const relational_integrity_topology = @import("relational_integrity_topology.zig");
const relational_rewrite_program = @import("relational_rewrite_program.zig");
const relational_rewrite_staging = @import("relational_rewrite_staging.zig");
const relational_row_codec = @import("algebraic/relational_row_codec.zig");
const relational_store = @import("relational_store.zig");
const replication_effects_mod = @import("replication_effects.zig");
const restore_generation_admissions = @import("restore_generation_admissions.zig");
const restore_staging = @import("restore_staging.zig");
const restore_staging_contract = @import("restore_staging_contract.zig");
const root_identity = @import("root_identity.zig");
const row_policy_authority_mod = @import("../../usermgr/row_policy_authority.zig");
const schema_mod = @import("../schema.zig");
const schema_registry_mod = @import("schema_registry.zig");
const snapshot_admission_mod = @import("snapshot_admission.zig");
const snapshot_staging = @import("snapshot_staging.zig");
const source_pin_state_mod = @import("../source_pin_state.zig");
const std = @import("std");
const storage_engine = @import("../engine.zig");
const storage_io_mod = @import("../lsm_backend/storage_io.zig");
const ttl_mod = @import("../ttl.zig");
const types = @import("types.zig");
const vector_migration = @import("../vector_migration.zig");
const Allocator = db_zig.Allocator;
const ArtifactRepairCompletionState = db_zig.ArtifactRepairCompletionState;
const AsyncContext = db_zig.AsyncContext;
const BatchExecutionOptions = db_zig.BatchExecutionOptions;
const DocumentPrimaryReadWindow = db_zig.DocumentPrimaryReadWindow;
const Io = db_zig.Io;
const LookupProjectionPlans = db_zig.LookupProjectionPlans;
const NativeRestoreOpenPlan = db_zig.NativeRestoreOpenPlan;
const OpenOptions = db_zig.OpenOptions;
const OrderedApplyReceipt = db_zig.OrderedApplyReceipt;
const PortableImportStage = db_zig.PortableImportStage;
const PortableRuntimeBatchPrelockTestHook = db_zig.PortableRuntimeBatchPrelockTestHook;
const PrimaryBackend = db_zig.PrimaryBackend;
const PublishedDenseCatalogLookupTestHook = db_zig.PublishedDenseCatalogLookupTestHook;
const ReplicationAsyncEffectMirror = db_zig.ReplicationAsyncEffectMirror;
const RestoreIdentity = db_zig.RestoreIdentity;
const RestoreImportDisk = db_zig.RestoreImportDisk;
const RestoreState = db_zig.RestoreState;
const RowPolicyOutputCheckpoint = db_zig.RowPolicyOutputCheckpoint;
const acquirePortableImportStageLease = db_zig.acquirePortableImportStageLease;
const appendDerivedBatchRecord = db_zig.appendDerivedBatchRecord;
const asyncContextHasActiveDenseBulkWork = db_zig.asyncContextHasActiveDenseBulkWork;
const backendConfigurationPathFingerprint = db_zig.backendConfigurationPathFingerprint;
const beginDenseCatchUpSessionTracked = db_zig.beginDenseCatchUpSessionTracked;
const beginExternalDenseBulkSessionTracked = db_zig.beginExternalDenseBulkSessionTracked;
const bytesToHexAlloc = db_zig.bytesToHexAlloc;
const checkLookupOptionsActive = db_zig.checkLookupOptionsActive;
const cleanupTempDir = db_zig.cleanupTempDir;
const createSnapshotStagingRoot = db_zig.createSnapshotStagingRoot;
const createUniquePortableImportStageBase = db_zig.createUniquePortableImportStageBase;
const createUniqueRepairShadowBase = db_zig.createUniqueRepairShadowBase;
const currentTimeNs = db_zig.currentTimeNs;
const deleteFileIfExists = db_zig.deleteFileIfExists;
const deletePortableImportStageLease = db_zig.deletePortableImportStageLease;
const durableReplicationOutboxKeyAlloc = db_zig.durableReplicationOutboxKeyAlloc;
const encodeDurableReplicationOutboxAlloc = db_zig.encodeDurableReplicationOutboxAlloc;
const encodeStoreLookupKeyWithPinnedSchemaAlloc = db_zig.encodeStoreLookupKeyWithPinnedSchemaAlloc;
const ensureSnapshotActive = db_zig.ensureSnapshotActive;
const failNextPortableImportCommittedSyncForTest = db_zig.failNextPortableImportCommittedSyncForTest;
const failNextPortableImportPublishForTest = db_zig.failNextPortableImportPublishForTest;
const failNextPortablePromotionRuntimeStartForTest = db_zig.failNextPortablePromotionRuntimeStartForTest;
const finishDenseCatchUpSessionTrackedAndFinalize = db_zig.finishDenseCatchUpSessionTrackedAndFinalize;
const finishExternalDenseBulkSessionTrackedAndFinalize = db_zig.finishExternalDenseBulkSessionTrackedAndFinalize;
const fs_paths = db_zig.fs_paths;
const graph_split_retire_cursor_key = db_zig.graph_split_retire_cursor_key;
const hexToBytesAlloc = db_zig.hexToBytesAlloc;
const isMergeArtifactKey = db_zig.isMergeArtifactKey;
const isRestoreArtifactKey = db_zig.isRestoreArtifactKey;
const loadArtifactFieldValueCallback = db_zig.loadArtifactFieldValueCallback;
const loadChunkFieldValueCallback = db_zig.loadChunkFieldValueCallback;
const loadEmbeddingFieldValueCallback = db_zig.loadEmbeddingFieldValueCallback;
const loadEmbeddingFieldsCallback = db_zig.loadEmbeddingFieldsCallback;
const loadOrCreateDurableRootIdentity = db_zig.loadOrCreateDurableRootIdentity;
const lockApply = db_zig.lockApply;
const lockApplyForPortableRuntimeAsync = db_zig.lockApplyForPortableRuntimeAsync;
const lockApplyForPortableRuntimeContext = db_zig.lockApplyForPortableRuntimeContext;
const lockApplyShared = db_zig.lockApplyShared;
const lockApplySharedForPortableRuntimeAsync = db_zig.lockApplySharedForPortableRuntimeAsync;
const lockAtomic = db_zig.lockAtomic;
const lockAtomicWithBackoff = db_zig.lockAtomicWithBackoff;
const lockAtomicWithCancellation = db_zig.lockAtomicWithCancellation;
const missingAsNull = db_zig.missingAsNull;
const monotonicTimeNs = db_zig.monotonicTimeNs;
const no_background_test_open = db_zig.no_background_test_open;
const openEmbeddingStreamCallback = db_zig.openEmbeddingStreamCallback;
const openModeRequiresReadOnlyBackends = db_zig.openModeRequiresReadOnlyBackends;
const openPrimaryStore = db_zig.openPrimaryStore;
const orderedApplyDisposition = db_zig.orderedApplyDisposition;
const orderedApplyReceiptWrite = db_zig.orderedApplyReceiptWrite;
const ordered_apply_receipt_value_len = db_zig.ordered_apply_receipt_value_len;
const ordinalProjectionFields = db_zig.ordinalProjectionFields;
const platform_time = db_zig.platform_time;
const portableImportStageLeasePathAlloc = db_zig.portableImportStageLeasePathAlloc;
const portable_import_identity_baseline_key = db_zig.portable_import_identity_baseline_key;
const portable_import_in_progress_key = db_zig.portable_import_in_progress_key;
const public_schema_json_key = db_zig.public_schema_json_key;
const publishSnapshotStaging = db_zig.publishSnapshotStaging;
const readOrderedApplyReceipt = db_zig.readOrderedApplyReceipt;
const readRestoreImportStateAllocWithIo = db_zig.readRestoreImportStateAllocWithIo;
const readRestoreStateForPathAlloc = db_zig.readRestoreStateForPathAlloc;
const readRestoreStateForPathAllocWithIo = db_zig.readRestoreStateForPathAllocWithIo;
const replayPendingDerivedBatches = db_zig.replayPendingDerivedBatches;
const replicationAppliedSequenceWrite = db_zig.replicationAppliedSequenceWrite;
const replication_applied_lsn_value_len = db_zig.replication_applied_lsn_value_len;
const requestEmbeddingName = db_zig.requestEmbeddingName;
const resetDenseIndexForArtifactRebuildForTest = db_zig.resetDenseIndexForArtifactRebuildForTest;
const restoreImportMarkerPathAlloc = db_zig.restoreImportMarkerPathAlloc;
const restoreIntentMarkerPathAlloc = db_zig.restoreIntentMarkerPathAlloc;
const restoreStateAlloc = db_zig.restoreStateAlloc;
const restoreStateMarkerPathAlloc = db_zig.restoreStateMarkerPathAlloc;
const resumeDeferredBackgroundMaintenanceIfIdle = db_zig.resumeDeferredBackgroundMaintenanceIfIdle;
const sleepNs = db_zig.sleepNs;
const snapshotPathExists = db_zig.snapshotPathExists;
const threadedIo = db_zig.threadedIo;
const validRestoreIdentity = db_zig.validRestoreIdentity;
const validateSnapshotId = db_zig.validateSnapshotId;
const waitForSearchResult = db_zig.waitForSearchResult;
const writeRestoreMarkerAtomicWithIo = db_zig.writeRestoreMarkerAtomicWithIo;
const writeRestoreStateForPath = db_zig.writeRestoreStateForPath;
const writeRestoreStateForPathWithIo = db_zig.writeRestoreStateForPathWithIo;
const ArtifactRepairIndexFallbackCounts = DB.ArtifactRepairIndexFallbackCounts;
const ArtifactRepairSummarySnapshot = DB.ArtifactRepairSummarySnapshot;
const EnrichmentThroughputSnapshot = DB.EnrichmentThroughputSnapshot;
const IndexStatusSnapshot = DB.IndexStatusSnapshot;
const IndexStructuralMutationGuard = DB.IndexStructuralMutationGuard;
const LsmOwnerStats = DB.LsmOwnerStats;
const PinnedIndexRepairSnapshot = DB.PinnedIndexRepairSnapshot;
const PreparedPortableRuntimeMetadata = DB.PreparedPortableRuntimeMetadata;
const RestorePurpose = DB.RestorePurpose;
const StatementReadFence = DB.StatementReadFence;
const TransferablePrimarySnapshot = DB.TransferablePrimarySnapshot;
const classifyIncompatibleNativeProjections = DB.classifyIncompatibleNativeProjections;
const decodeIndexStatusSnapshot = DB.decodeIndexStatusSnapshot;
const denseServingCounts = DB.denseServingCounts;
const durableGenerationBuildActive = DB.durableGenerationBuildActive;
const encodeIndexStatusSnapshot = DB.encodeIndexStatusSnapshot;
const finishMaterializedNativeGenerationWithIo = DB.finishMaterializedNativeGenerationWithIo;
const indexRepairIntentBlocksService = DB.indexRepairIntentBlocksService;
const indexStatusKeyAlloc = DB.indexStatusKeyAlloc;
const index_status_encoded_len = DB.index_status_encoded_len;
const normalizeReplayStatusFromDurableCheckpoint = DB.normalizeReplayStatusFromDurableCheckpoint;
const noteIndexRepairSchedulerUpsert = DB.noteIndexRepairSchedulerUpsert;
const repairAdmissionVersion = DB.repairAdmissionVersion;

pub fn lockApplySharedForPortableRuntime(self: *DB) !void {
    try self.enforcePortableRuntimeGate();
    lockApplyShared(self);
    errdefer self.core.unlockApplyShared();
    try self.enforcePortableRuntimeGate();
    // A failed split may have withdrawn only some graph owners. Keep the
    // live handle closed to reads until the durable retirement is repaired.
    const pending = try missingAsNull(self.core.store.get(self.alloc, graph_split_retire_cursor_key));
    if (pending) |cursor| {
        self.alloc.free(cursor);
        return error.GraphMaintenanceInProgress;
    }
    if (self.core.index_manager.graphOwnershipCleanupPending())
        return error.GraphMaintenanceInProgress;
}

pub fn acquireSnapshotReplayMutation(self: *DB) !snapshot_admission_mod.SnapshotAdmission.MutationLease {
    if (self.backend_runtime.filesystemIo()) |io| {
        return try self.core.snapshot_replay_admission.acquireMutationIo(
            io,
            @as(?types.CancellationToken, null),
        );
    }
    return self.core.snapshot_replay_admission.acquireMutation();
}

/// Resolves the exact storage configuration used by native restore before
/// any corpus bytes are fetched. The returned options are safe to pass to
/// DB.open without invoking a path-sensitive configurator a second time.
pub fn resolveNativeRestoreOpenPlan(
    target_path: []const u8,
    requested_options: OpenOptions,
) !NativeRestoreOpenPlan {
    if (comptime storage_engine.enabled) return storage_engine.call(@src().fn_name, .{ target_path, requested_options });
    var opts = requested_options;
    if (opts.backend_runtime) |runtime| {
        if (runtime.db_open_configurator) |configurator| {
            try configurator.configure(target_path, &opts);
        }
    }
    // A restore plan is intentionally copyable across candidate,
    // recovery, and published opens. Storage configurators may install
    // borrowed backend capabilities, but may not smuggle move-only
    // enrichment ownership into it.
    if (opts.enrichment != null) return error.NativeRestoreConfiguratorOwnedStateUnsupported;
    // Host-directory publication may reuse only path-independent policy
    // and storage capabilities which explicitly operate on the host paths
    // supplied by each open. A target-bound store or derived namespace
    // would make candidate validation touch storage outside the
    // lifecycle-owned sibling. Such providers must implement backend-owned
    // stage/promote and advertise `external_backend` instead.
    if (opts.physical_root_mode == .filesystem_managed and
        (opts.primary_runtime_store != null or
            !nativeRestorePrimaryStoragePublicationCompatible(opts.primary_backend) or
            !nativeRestoreStoragePublicationCompatible(opts.storage) or
            !nativeRestoreStoragePublicationCompatible(opts.change_journal_storage) or
            !nativeRestoreStoragePublicationCompatible(opts.index_backends.text_lsm_storage) or
            !nativeRestoreStoragePublicationCompatible(opts.index_backends.dense_lsm_storage) or
            !nativeRestoreStoragePublicationCompatible(opts.index_backends.sparse_lsm_storage) or
            !nativeRestoreStoragePublicationCompatible(opts.index_backends.graph_lsm_storage) or
            !nativeRestoreStoragePublicationCompatible(opts.index_backends.text_main_lsm_options.storage) or
            !nativeRestoreStoragePublicationCompatible(opts.index_backends.text_wal_lsm_options.storage) or
            !nativeRestoreStoragePublicationCompatible(opts.index_backends.dense_lsm_options.storage) or
            !nativeRestoreStoragePublicationCompatible(opts.index_backends.sparse_lsm_options.storage) or
            !nativeRestoreStoragePublicationCompatible(opts.index_backends.graph_reverse_lsm_options.storage) or
            opts.index_base_path != null or
            !nativeRestoreStoragePublicationCompatible(opts.index_repair_checkpoint_storage)))
    {
        return error.NativeBackupStorageBackendUnsupported;
    }
    return .{
        ._options = opts,
        ._target_path_fingerprint = backendConfigurationPathFingerprint(target_path),
    };
}

pub fn nativeRestoreStoragePublicationCompatible(storage: ?lsm_backend_mod.Storage) bool {
    return storage == null or storage.?.supportsHostPathGenerationPublication();
}

pub fn nativeRestorePrimaryStoragePublicationCompatible(primary_backend: PrimaryBackend) bool {
    return switch (primary_backend) {
        .lsm, .lsm_memory => |options| nativeRestoreStoragePublicationCompatible(options.storage),
        .mem => true,
    };
}

/// A connection retired this generation after another owner committed.
/// Its cached indexes must never flush over the newer durable generation.
pub fn closeImmutableSnapshot(self: *DB) void {
    if (self.closed) return;
    self.open_mode = .query_readonly;
    self.core.discard_storage_writes = true;
    self.close();
}

pub fn restoreEnrichmentPrevious(ptr: *anyopaque) !void {
    const self: *DB = @ptrCast(@alignCast(ptr));
    try self.restartEnrichmentAfterStructuralMutation("failed enrichment reconfiguration", "");
}

pub fn snapshotAsyncIndexingStats(self: *DB) types.AsyncIndexingStats {
    if (comptime storage_engine.enabled) return storage_engine.call(@src().fn_name, .{self}) catch @panic("incompatible storage owner");
    var async_stats = self.async_context.stats.snapshot();
    // Runtime status must carry the writer-owned native-vector lifecycle
    // fact. A cached materialization snapshot can have a stale HBC count
    // and cannot reconstruct this safely after aggregation. Treat queued
    // native publication as part of the finalization fence: public status
    // exposes it through dense_vector_projection_pending and remains
    // closed until a complete generation is installed.
    async_stats.dense_projection_finalizing =
        self.async_context.dense_admission.finalizing.load(.acquire) or
        self.core.index_manager.vectorBlockProjectionPending();
    async_stats.bulk_coalescing = self.bulk_ingest_session.snapshot();
    async_stats.derived_workers = self.executor.snapshotStats();
    return async_stats;
}

pub fn snapshotApplyLockStats(self: *DB) WriterPreferringRwLock.Stats {
    return self.core.apply_mutex.snapshot();
}

pub fn snapshotLsmMaintenanceStats(self: *DB) lsm_backend_mod.Backend.MaintenanceStats {
    if (comptime storage_engine.enabled) return storage_engine.call(@src().fn_name, .{self}) catch @panic("incompatible storage owner");
    lockApplyShared(self);
    defer self.core.unlockApplyShared();
    return self.snapshotLsmMaintenanceStatsLocked();
}

pub fn snapshotLsmOwnerStatsLockedAlloc(self: *DB, alloc: Allocator) ![]LsmOwnerStats {
    var owners = std.ArrayListUnmanaged(LsmOwnerStats).empty;
    errdefer {
        for (owners.items) |*owner| owner.deinit(alloc);
        owners.deinit(alloc);
    }
    if (self.core.primary_store_owner.snapshotLsmMaintenanceStats()) |maintenance| {
        try owners.ensureUnusedCapacity(alloc, 1);
        const primary_name = try alloc.dupe(u8, "primary");
        owners.appendAssumeCapacity(.{
            .kind = .primary,
            .name = primary_name,
            .maintenance = maintenance,
        });
    }
    var index_owners = try self.core.index_manager.snapshotLsmOwnerStatsAlloc(alloc);
    var transferred: usize = 0;
    defer {
        for (index_owners[transferred..]) |*owner| owner.deinit(alloc);
        alloc.free(index_owners);
    }
    for (index_owners) |owner| {
        try owners.append(alloc, .{
            .kind = switch (owner.kind) {
                .full_text => .full_text,
                .dense_vector => .dense_vector,
                else => unreachable,
            },
            .name = owner.name,
            .maintenance = owner.maintenance,
            .owner_overflow = owner.owner_overflow,
            .retired_labels_collapsed_total = owner.retired_labels_collapsed_total,
        });
        transferred += 1;
    }
    return try owners.toOwnedSlice(alloc);
}

pub fn snapshotLsmOwnerStatsAlloc(self: *DB, alloc: Allocator) ![]LsmOwnerStats {
    if (comptime storage_engine.enabled) return storage_engine.call(@src().fn_name, .{ self, alloc });
    lockApplyShared(self);
    defer self.core.unlockApplyShared();
    return try self.snapshotLsmOwnerStatsLockedAlloc(alloc);
}

pub fn trySnapshotLsmOwnerStatsAlloc(self: *DB, alloc: Allocator) !?[]LsmOwnerStats {
    if (comptime storage_engine.enabled) return storage_engine.call(@src().fn_name, .{ self, alloc });
    if (!self.core.tryLockApplyShared()) return null;
    defer self.core.unlockApplyShared();
    return try self.snapshotLsmOwnerStatsLockedAlloc(alloc);
}

pub fn trySnapshotLsmMaintenanceStats(self: *DB) ?lsm_backend_mod.Backend.MaintenanceStats {
    if (comptime storage_engine.enabled) return storage_engine.call(@src().fn_name, .{self}) catch @panic("incompatible storage owner");
    if (!self.core.tryLockApplyShared()) return null;
    defer self.core.unlockApplyShared();
    return self.snapshotLsmMaintenanceStatsLocked();
}

pub fn snapshotLsmMaintenanceStatsLocked(self: *DB) lsm_backend_mod.Backend.MaintenanceStats {
    var maintenance_stats = lsm_backend_mod.Backend.MaintenanceStats{};
    if (self.core.primary_store_owner.snapshotLsmMaintenanceStats()) |primary_stats| {
        lsm_backend_mod.Backend.accumulateMaintenanceStats(&maintenance_stats, primary_stats);
    }
    lsm_backend_mod.Backend.accumulateMaintenanceStats(&maintenance_stats, self.core.index_manager.snapshotLsmMaintenanceStats());
    return maintenance_stats;
}

pub fn snapshotLsmWriteStats(self: *DB) lsm_backend_mod.Backend.WriteStats {
    if (comptime storage_engine.enabled) return storage_engine.call(@src().fn_name, .{self}) catch @panic("incompatible storage owner");
    lockApplyShared(self);
    defer self.core.unlockApplyShared();
    return self.snapshotLsmWriteStatsLocked();
}

pub fn trySnapshotLsmWriteStats(self: *DB) ?lsm_backend_mod.Backend.WriteStats {
    if (comptime storage_engine.enabled) return storage_engine.call(@src().fn_name, .{self}) catch @panic("incompatible storage owner");
    if (!self.core.tryLockApplyShared()) return null;
    defer self.core.unlockApplyShared();
    return self.snapshotLsmWriteStatsLocked();
}

pub fn snapshotLsmWriteStatsLocked(self: *DB) lsm_backend_mod.Backend.WriteStats {
    var write_stats = lsm_backend_mod.Backend.WriteStats{};
    if (self.core.primary_store_owner.snapshotLsmWriteStats()) |primary_stats| {
        lsm_backend_mod.Backend.accumulateWriteStats(&write_stats, primary_stats);
    }
    lsm_backend_mod.Backend.accumulateWriteStats(&write_stats, self.core.index_manager.snapshotLsmWriteStats());
    return write_stats;
}

pub fn snapshotTextMemoryAttributionStats(self: *DB) index_manager_mod.TextMemoryAttributionStats {
    lockApplyShared(self);
    defer self.core.unlockApplyShared();
    return self.core.index_manager.snapshotTextMemoryAttribution();
}

pub fn trySnapshotTextMemoryAttributionStats(self: *DB) ?index_manager_mod.TextMemoryAttributionStats {
    if (comptime storage_engine.enabled) return storage_engine.call(@src().fn_name, .{self}) catch @panic("incompatible storage owner");
    if (!self.core.tryLockApplyShared()) return null;
    defer self.core.unlockApplyShared();
    return self.core.index_manager.snapshotTextMemoryAttribution();
}

pub fn snapshotTextMergeStats(self: *DB) types.TextMergeStats {
    lockApplyShared(self);
    defer self.core.unlockApplyShared();
    var merge_stats = self.core.index_manager.textMergeStatsSnapshot();
    if (self.text_merge_runtime) |runtime| runtime.overlayRuntimeStats(&merge_stats);
    return merge_stats;
}

pub fn trySnapshotTextMergeStats(self: *DB) ?types.TextMergeStats {
    if (comptime storage_engine.enabled) return storage_engine.call(@src().fn_name, .{self}) catch @panic("incompatible storage owner");
    if (!self.core.tryLockApplyShared()) return null;
    defer self.core.unlockApplyShared();
    var merge_stats = self.core.index_manager.textMergeStatsSnapshot();
    if (self.text_merge_runtime) |runtime| runtime.overlayRuntimeStats(&merge_stats);
    return merge_stats;
}

pub fn snapshotPrimaryLsmWriteStatsForTest(self: *DB) ?lsm_backend_mod.Backend.WriteStats {
    lockApplyShared(self);
    defer self.core.unlockApplyShared();
    return self.core.primary_store_owner.snapshotLsmWriteStats();
}

pub fn snapshotLsmNativeStorageStats(self: *DB) lsm_backend_mod.NativeStorageStats {
    return self.backend_runtime.snapshotNativeStorageStats();
}

pub fn trySnapshotLsmNativeStorageStats(self: *DB) ?lsm_backend_mod.NativeStorageStats {
    return self.backend_runtime.snapshotNativeStorageStats();
}

pub fn failNextRestoreProjectionApplyForTest() void {
    std.debug.assert(builtin.is_test);
    db_zig.test_fail_restore_projection_apply = true;
}

/// Pin primary data and its ordered receipt under the local apply lock.
/// A server adapter supplies any surrounding protocol completion fence.
pub fn pinTransferablePrimarySnapshot(self: *DB) !TransferablePrimarySnapshot {
    if (comptime storage_engine.enabled) return storage_engine.call(@src().fn_name, .{self});
    try self.lockApplyForPortableRuntime();
    defer self.core.unlockApply();
    var namespace: [24]u8 = undefined;
    doc_identity.encodeNamespace(&namespace, self.core.identity_namespace);
    {
        var read = try self.core.store.beginProbeTxn();
        defer read.abort();
        try source_pin_state_mod.requireNoPrepared(&read, namespace);
        try online_source.requireTransferableSnapshots(&read, namespace);
    }
    const marker = try readOrderedApplyReceipt(self.alloc, self.core.store);
    var primary = try self.core.pinNativeSnapshot();
    errdefer primary.deinit();
    switch (primary) {
        .logical => return error.UnsupportedSnapshotFormat,
        .lsm => |checkpoint| if (!checkpoint.storage.supportsHostPathGenerationPublication()) return error.UnsupportedSnapshotFormat,
    }
    return .{ .alloc = self.alloc, .io = self.backend_runtime.filesystemIo() orelse return error.BackendRuntimeIoUnavailable, .namespace = namespace, .receipt = marker, .primary = primary };
}

pub fn verifyTransferablePrimarySnapshot(self: *DB, namespace: [24]u8, receipt: ?OrderedApplyReceipt) !void {
    if (comptime storage_engine.enabled) return storage_engine.call(@src().fn_name, .{ self, namespace, receipt });
    var actual_namespace: [24]u8 = undefined;
    doc_identity.encodeNamespace(&actual_namespace, self.core.identity_namespace);
    if (!std.mem.eql(u8, &actual_namespace, &namespace)) return error.InvalidSnapshot;
    const marker = try self.orderedApplyReceipt();
    if ((if (marker) |value| value.index else 0) != (if (receipt) |value| value.index else 0) or
        (if (marker) |value| value.term else 0) != (if (receipt) |value| value.term else 0)) return error.InvalidSnapshot;
    var read = try self.core.store.beginProbeTxn();
    defer read.abort();
    try source_pin_state_mod.requireNoPrepared(&read, namespace);
    try online_source.requireTransferableSnapshots(&read, namespace);
}

pub fn recoverReplicatedRestoreFinish(self: *DB, finish: @FieldType(restore_staging_contract.Control, "finish")) !void {
    if (comptime storage_engine.enabled) return storage_engine.call(@src().fn_name, .{ self, finish });
    try self.prepareAppliedRestoreFinishReplay(.{ .restore_staging = .{ .finish = finish } });
}

pub fn artifactRepairCompletionSnapshot(
    self: *DB,
    alloc: Allocator,
    completion_key: []const u8,
) !ArtifactRepairCompletionState {
    lockAtomicWithBackoff(&self.async_context.artifact_repair_issue_mutex);
    defer self.async_context.artifact_repair_issue_mutex.unlock();
    return (try self.loadArtifactRepairCompletionState(alloc, completion_key)) orelse .{};
}

pub fn artifactRepairSummaryRootSnapshot(self: *DB, alloc: Allocator) !ArtifactRepairSummarySnapshot {
    const ready = try self.artifactRepairSummaryReady(alloc);
    const key = if (ready)
        try internal_keys.artifactRepairSummaryRootKeyAlloc(alloc)
    else
        try internal_keys.artifactRepairSummaryRebuildRootKeyAlloc(alloc);
    defer alloc.free(key);
    return .{
        .ready = ready,
        .count = (try self.loadArtifactRepairSummaryCountByKey(alloc, key)) orelse if (ready) 0 else try self.scanArtifactRepairIssueCountBounded(alloc, null),
    };
}

pub fn artifactRepairSummaryIndexSnapshot(self: *DB, alloc: Allocator, index_name: []const u8, ready: bool) !ArtifactRepairSummarySnapshot {
    const key = if (ready)
        try internal_keys.artifactRepairSummaryIndexKeyAlloc(alloc, index_name)
    else
        try internal_keys.artifactRepairSummaryRebuildIndexKeyAlloc(alloc, index_name);
    defer alloc.free(key);
    if (try self.loadArtifactRepairSummaryCountByKey(alloc, key)) |count| {
        return .{ .ready = ready, .count = count };
    }
    if (ready) return .{ .ready = true, .count = 0 };
    const scanned_count = try self.scanArtifactRepairIssueCountBounded(alloc, index_name);
    return .{ .ready = false, .count = scanned_count, .repair_scan_count = scanned_count };
}

pub fn artifactRepairSummarySourceSnapshot(
    self: *DB,
    alloc: Allocator,
    index_name: []const u8,
    artifact_name: []const u8,
    ready: bool,
) !ArtifactRepairSummarySnapshot {
    // During a rebuild there is no safe bounded fallback that can prove
    // absence for an arbitrary source. Return the partial shadow count for
    // diagnostics and keep readiness explicitly false until publication.
    const key = if (ready)
        try internal_keys.artifactRepairSummarySourceKeyAlloc(alloc, index_name, artifact_name)
    else
        try internal_keys.artifactRepairSummaryRebuildSourceKeyAlloc(alloc, index_name, artifact_name);
    defer alloc.free(key);
    return .{
        .ready = ready,
        .count = (try self.loadArtifactRepairSummaryCountByKey(alloc, key)) orelse 0,
    };
}

pub fn artifactRepairSummaryIndexSnapshotForStats(
    self: *DB,
    alloc: Allocator,
    index_name: []const u8,
    ready: bool,
    fallback: *ArtifactRepairIndexFallbackCounts,
) !ArtifactRepairSummarySnapshot {
    if (ready) return try self.artifactRepairSummaryIndexSnapshot(alloc, index_name, true);

    const key = try internal_keys.artifactRepairSummaryRebuildIndexKeyAlloc(alloc, index_name);
    defer alloc.free(key);
    if (try self.loadArtifactRepairSummaryCountByKey(alloc, key)) |count| {
        return .{ .ready = false, .count = count };
    }

    try self.ensureArtifactRepairIndexFallbackCounts(alloc, fallback);
    const scanned_count = fallback.counts.get(index_name) orelse 0;
    return .{ .ready = false, .count = scanned_count, .repair_scan_count = scanned_count };
}

/// Atomically protects replay before exposing a primary snapshot. The
/// provisional zero pin and truncation share repair_replay_mutex, closing
/// the floor-capture race even if the process stops between either durable
/// checkpoint update.
pub fn beginPinnedIndexRepairSnapshot(
    self: *DB,
    alloc: Allocator,
    repair_id: u128,
) !PinnedIndexRepairSnapshot {
    self.core.repair_replay_mutex.lockUncancelable(self.core.index_manager.checkpointIo());
    defer self.core.repair_replay_mutex.unlock(self.core.index_manager.checkpointIo());
    const location = try self.indexRepairStateLocation();
    var state = try index_repair_state.loadAt(alloc, location);
    defer state.deinit(alloc);
    const entry_index = blk: {
        for (state.entries.items, 0..) |entry, i| {
            if (entry.intent.repair_id == repair_id) break :blk i;
        }
        return error.IndexRepairIntentNotFound;
    };
    var entry = try state.entries.items[entry_index].clone(alloc);
    defer entry.deinit(alloc);
    if (entry.intent.phase == .terminal or entry.intent.automation == .paused) return error.IndexRepairNotRunnable;
    var expected = index_repair_state.ExpectedTransition{
        .repair_id = entry.intent.repair_id,
        .revision = entry.intent.revision,
        .phase = entry.intent.phase,
        .config_hash = entry.intent.config_hash,
        .root_generation = entry.intent.root_generation,
        .owner_epoch = entry.intent.owner_epoch,
    };
    if (entry.pin == null) {
        entry.pin = .{
            .repair_id = entry.intent.repair_id,
            .db_identity = entry.intent.db_identity,
            .replica_id = entry.intent.replica_id,
            .root_generation = entry.intent.root_generation,
            .index_name = try alloc.dupe(u8, entry.intent.index_name),
            .retain_after_sequence = 0,
        };
        const control_revision = try index_repair_state.putEntryAt(alloc, location, state.identity, expected, entry);
        self.core.index_manager.publishRepairAdmission(
            entry.intent.index_name,
            repairAdmissionVersion(entry.intent, expected.revision +| 1, control_revision),
            indexRepairIntentBlocksService(entry.intent),
            true,
        );
        noteIndexRepairSchedulerUpsert(self.async_context, entry.intent, expected.revision +| 1, control_revision);
    } else {
        // Re-entering snapshot acquisition must become conservative before
        // replacing a previously finalized build floor.
        entry.pin.?.retain_after_sequence = 0;
        const control_revision = try index_repair_state.putEntryAt(alloc, location, state.identity, expected, entry);
        self.core.index_manager.publishRepairAdmission(
            entry.intent.index_name,
            repairAdmissionVersion(entry.intent, expected.revision +| 1, control_revision),
            indexRepairIntentBlocksService(entry.intent),
            true,
        );
        noteIndexRepairSchedulerUpsert(self.async_context, entry.intent, expected.revision +| 1, control_revision);
    }
    expected.revision +|= 1;
    self.async_context.index_repair_replay_pinned.store(true, .release);

    var txn = try self.core.store.beginReadTxn();
    errdefer txn.abort();
    const build_floor = try self.core.store.lastReplaySequenceFromTxn(&txn, 0);
    entry.intent.build_floor_sequence = build_floor;
    entry.intent.build_source_guard = try artifact_source_gap.Guard.capture(&txn);
    entry.intent.updated_at_ms = self.indexRepairNowMs();
    entry.pin.?.retain_after_sequence = build_floor;
    const control_revision = try index_repair_state.putEntryAt(alloc, location, state.identity, expected, entry);
    self.core.index_manager.publishRepairAdmission(
        entry.intent.index_name,
        repairAdmissionVersion(entry.intent, expected.revision +| 1, control_revision),
        indexRepairIntentBlocksService(entry.intent),
        true,
    );
    noteIndexRepairSchedulerUpsert(self.async_context, entry.intent, expected.revision +| 1, control_revision);
    return .{
        .txn = txn,
        .repair_id = repair_id,
        .build_floor_sequence = build_floor,
    };
}

/// Advances one durable projection-repair intent created while validating
/// a native restore. `true` means the staged generation has no remaining
/// repair debt and is eligible for sealing/publication. Existing healthy
/// projections are never reset or replayed by this path.
pub fn repairNativeRestoreProjectionIntentsStep(
    self: *DB,
    alloc: Allocator,
    options: types.ArtifactRepairRunOptions,
) !bool {
    const before = try self.indexRepairIntentSummary(alloc);
    if (before.terminal != 0) return error.NativeBackupProjectionRepairFailed;
    if (before.paused != 0) return error.NativeBackupProjectionRepairPaused;
    if (before.runnable == 0) return true;

    const result = try self.repairRecoverableStartupIndexFailures(alloc, 1, options);
    const after = try self.indexRepairIntentSummary(alloc);
    if (after.terminal != 0) return error.NativeBackupProjectionRepairFailed;
    if (after.paused != 0) return error.NativeBackupProjectionRepairPaused;
    return result.remaining == 0 and after.runnable == 0;
}

pub fn lookupPrimarySnapshot(self: *DB, alloc: Allocator, key: []const u8, opts: types.LookupOptions, probe: *docstore_mod.DocStore.Txn) !?types.LookupResult {
    var schema_view = self.core.acquireSchemaView();
    defer if (schema_view) |*view| view.release();
    return self.lookupPrimarySnapshotWithSchema(alloc, key, opts, probe, schema_view);
}

pub fn lookupPrimarySnapshotWithSchema(self: *DB, alloc: Allocator, key: []const u8, opts: types.LookupOptions, probe: *docstore_mod.DocStore.Txn, schema_view: ?schema_registry_mod.SchemaView) !?types.LookupResult {
    return self.lookupPrimarySnapshotWithSchemaAndOutput(alloc, alloc, key, opts, probe, schema_view, null, null);
}

pub fn lookupPrimarySnapshotWithSchemaAndOutput(self: *DB, alloc: Allocator, output: Allocator, key: []const u8, opts: types.LookupOptions, probe: *docstore_mod.DocStore.Txn, schema_view: ?schema_registry_mod.SchemaView, primary: ?[]const u8, primary_timestamp: ?u64) !?types.LookupResult {
    return self.lookupPrimarySnapshotWithPreparedProjection(alloc, output, key, opts, probe, schema_view, primary, primary_timestamp, null);
}

pub fn lookupPrimarySnapshotWithPreparedProjection(self: *DB, alloc: Allocator, output: Allocator, key: []const u8, opts: types.LookupOptions, probe: *docstore_mod.DocStore.Txn, schema_view: ?schema_registry_mod.SchemaView, primary: ?[]const u8, primary_timestamp: ?u64, prepared: ?*LookupProjectionPlans) !?types.LookupResult {
    // Virtual group sources have no independently authorized public row.
    // Owner control queries use the dedicated private lookup path above;
    // producers read their physical source through authenticated snapshots.
    if (@import("group_source_identity.zig").isAnchor(key)) return null;
    var verified_principal: ?std.json.Parsed(row_policy_authority_mod.Payload) = null;
    defer if (verified_principal) |*principal| principal.deinit();
    var row_policy_lease = if (opts.row_policy_principal_proof.len != 0) bound: {
        verified_principal = try self.verifyRowPolicyPrincipal(alloc, opts.row_policy_principal_proof, opts.row_policy_database, .read, @intCast(@divFloor(platform_time.realtimeNs(), std.time.ns_per_s)));
        break :bound try self.local_execution.row_policy_gate.enterVerifiedPrincipal(&verified_principal.?.value, @intCast(@divFloor(platform_time.realtimeNs(), std.time.ns_per_s)));
    } else try self.local_execution.row_policy_gate.enterRawRead();
    defer row_policy_lease.release();
    // Projection borrows the caller's immutable schema and primary snapshot.
    const store_key = try encodeStoreLookupKeyWithPinnedSchemaAlloc(self, alloc, key, schema_view);
    defer alloc.free(store_key);
    const raw = primary orelse (probe.getLeased(store_key) catch |err| switch (err) {
        error.NotFound => return null,
        else => return err,
    });
    const relational = internal_keys.isRelationalRowKey(store_key);
    var historical_schema_view: ?schema_registry_mod.SchemaView = null;
    defer if (historical_schema_view) |*view| view.release();
    const ordinal_row = if (relational) blk: {
        const version = try relational_store.rowSchemaVersion(raw);
        const row_schema = if (schema_view) |view|
            if (view.version() == version)
                view
            else historical: {
                historical_schema_view = (try self.core.acquireSchemaVersionView(version)) orelse
                    return error.UnknownSchemaVersion;
                break :historical historical_schema_view.?;
            }
        else historical: {
            historical_schema_view = (try self.core.acquireSchemaVersionView(version)) orelse
                return error.UnknownSchemaVersion;
            break :historical historical_schema_view.?;
        };
        break :blk if (self.core.store.valuesAreAuthenticated())
            try relational_row_codec.ordinalRowViewTrusted(
                raw,
                row_schema.tableSchema().*,
                row_schema.physicalLayout(),
            )
        else
            try relational_row_codec.ordinalRowViewSelective(
                raw,
                row_schema.tableSchema().*,
                row_schema.physicalLayout(),
            );
    } else null;
    if (verified_principal) |*principal| {
        const row = ordinal_row orelse return error.RowPolicyCatalogChanged;
        const bundle = if (self.local_execution.row_policy_bundle) |*installed| installed else return error.RowPolicyCatalogChanged;
        var evaluation = try bundle.captureReadEvaluation(alloc, row.table_schema, &principal.value);
        defer evaluation.deinit();
        if (!try evaluation.permits(row)) return null;
        try row_policy_lease.checkAt(@intCast(@divFloor(platform_time.realtimeNs(), std.time.ns_per_s)));
    }
    if (!internal_keys.isInternalUserKey(key)) {
        const ttl_duration_ns = if (schema_view) |view| view.visibilityTtlDurationNs() else 0;
        if (ttl_duration_ns != 0) {
            const timestamp_ns = if (ordinal_row) |row|
                row.writeTimestampNs()
            else
                primary_timestamp orelse try lookupSnapshotTimestamp(alloc, key, probe);
            if (timestamp_ns != 0 and ttl_mod.isExpired(timestamp_ns, ttl_duration_ns, currentTimeNs())) return null;
        }
    }
    try checkLookupOptionsActive(opts);
    const direct_fields = if (relational)
        ordinalProjectionFields(opts.fields, opts.include_all_fields)
    else
        null;
    const stored = if (direct_fields) |fields| blk: {
        const row = ordinal_row.?;
        if (prepared) |plans| break :blk try (try plans.forRow(row, fields)).projectWithOutputPolicy(alloc, output, row, opts.output_exact);
        var projection = try RelationalProjectionPlan.init(
            alloc,
            row.table_schema,
            row.layout,
            fields,
        );
        defer projection.deinit();
        break :blk try projection.projectWithOutputPolicy(alloc, output, row, opts.output_exact);
    } else blk: {
        if (opts.fields.len == 0 and opts.include_all_fields) {
            if (ordinal_row) |row| {
                const bytes = try row.reconstructValueAlloc(alloc);
                defer alloc.free(bytes);
                break :blk try output.dupe(u8, bytes);
            }
            break :blk try output.dupe(u8, raw);
        }
        const materialized = if (ordinal_row) |row| try row.reconstructValueAlloc(alloc) else null;
        defer if (materialized) |bytes| alloc.free(bytes);
        break :blk try db_query_projection.projectLookupStoredBytesWithPlanAndOutput(alloc, output, key, materialized orelse raw, opts, .{
            .ctx = self,
            .load_chunks = loadChunkFieldValueCallback,
            .load_embeddings = loadEmbeddingFieldValueCallback,
            .open_embeddings = openEmbeddingStreamCallback,
            .load_embeddings_compact = loadEmbeddingFieldsCallback,
            .load_artifacts = loadArtifactFieldValueCallback,
        }, if (prepared) |plans| try plans.getPlain() else null);
    };
    errdefer output.free(stored);
    try checkLookupOptionsActive(opts);
    var version: ?u64 = if (ordinal_row) |row| row.writeTimestampNs() else null;
    var digest: ?[32]u8 = null;
    if (opts.include_primary_digest) {
        var primary_digest: [32]u8 = undefined;
        std.crypto.hash.sha2.Sha256.hash(raw, &primary_digest, .{});
        digest = primary_digest;
        if (version == null) {
            version = primary_timestamp orelse try lookupSnapshotTimestamp(alloc, key, probe);
        }
    }
    try row_policy_lease.checkAt(@intCast(@divFloor(platform_time.realtimeNs(), std.time.ns_per_s)));
    return .{ .json = stored, .version = version, .expected_content_digest = digest };
}

pub fn lookupSnapshotTimestamp(alloc: Allocator, key: []const u8, probe: *docstore_mod.DocStore.Txn) !u64 {
    const timestamp_key = try internal_keys.ttlKeyAlloc(alloc, key);
    defer alloc.free(timestamp_key);
    const timestamp = try missingAsNull(probe.get(timestamp_key));
    const bytes = timestamp orelse return 0;
    if (bytes.len != 8) return error.InvalidTimestamp;
    return std.mem.readInt(u64, bytes[0..8], .little);
}

/// Appends a bounded document chunk into an isolated snapshot generation.
/// The caller owns generation publication and must never call this against
/// a live generation because chunks are independently durable.
pub fn appendStagedSnapshotDocuments(
    self: *DB,
    staged_generation: *const generation_lifecycle.StagedGeneration,
    byte_range: types.ByteRange,
    writes: []const types.BatchWrite,
) !void {
    if (comptime storage_engine.enabled) return storage_engine.call(@src().fn_name, .{ self, staged_generation, byte_range, writes });
    try staged_generation.validatePath(self.core.path);
    if (byte_range.start.len > 0 and byte_range.end.len > 0 and
        std.mem.order(u8, byte_range.start, byte_range.end) != .lt)
    {
        return error.InvalidAppliedDataRange;
    }
    for (writes) |write| if (!byte_range.contains(write.key)) return error.KeyOutOfRange;
    try self.batchInternal(.{
        .writes = writes,
        .sync_level = .write,
    }, null, .{
        .validate_range_ownership = false,
        .wait_for_sync_level = false,
        .bypass_replication_write_gate = true,
    });
}

/// Finalizes the range metadata after all chunks have been imported.
pub fn finishStagedSnapshotRange(
    self: *DB,
    staged_generation: *const generation_lifecycle.StagedGeneration,
    byte_range: types.ByteRange,
) !void {
    if (comptime storage_engine.enabled) return storage_engine.call(@src().fn_name, .{ self, staged_generation, byte_range });
    try staged_generation.validatePath(self.core.path);
    const range_value = try range_state_mod.encodeRangeAlloc(self.alloc, byte_range);
    defer self.alloc.free(range_value);
    const range_write: docstore_mod.KVPair = .{ .key = range_state_mod.range_key, .value = range_value };
    try self.batchInternal(.{ .sync_level = .write }, null, .{
        .validate_range_ownership = false,
        .wait_for_sync_level = false,
        .bypass_replication_write_gate = true,
        .extra_store_writes = &.{range_write},
    });
    try self.refreshSplitBootstrapRangeInMemory(byte_range);
}

pub fn snapshot(self: *DB, id: []const u8) !u64 {
    if (comptime storage_engine.enabled) return storage_engine.call(@src().fn_name, .{ self, id });
    var row_policy_lease = try self.local_execution.row_policy_gate.enterRaw();
    defer row_policy_lease.release();
    return try self.snapshotInternal(id, false, .none, null, null, false, null);
}

/// Drain durable replay/enrichment effects before a caller freezes writes
/// for snapshot capture. Background completion can itself mutate storage,
/// so callers must drain before taking an exclusive mutation barrier.
/// The deadline is in this DB's BackendRuntime.monotonicClock domain.
pub fn drainSnapshotMaintenance(self: *DB, deadline_ns: u64) !void {
    if (comptime storage_engine.enabled) return storage_engine.call(@src().fn_name, .{ self, deadline_ns });
    while (true) {
        self.runMaintenanceUntilWithCancellation(
            self.currentMaintenanceTargetSequence(),
            .{},
            .none,
            deadline_ns,
        ) catch |err| switch (err) {
            // A retryable provider failure has already committed its
            // source mutation. Seed preflight is the safe place to absorb
            // that finite retry: returning it to the operator adds a full
            // reconciliation round trip, while waiting after the global
            // barrier closes would deadlock its eventual publication.
            error.EnrichmentRetryInProgress => {
                const wait_clock = self.backend_runtime.monotonicClock();
                const now_ns = wait_clock.nowRealtimeNs();
                if (now_ns >= deadline_ns) return error.EnrichmentWaitTimeout;
                const sleep_ns = @min(25 * std.time.ns_per_ms, deadline_ns - now_ns);
                wait_clock.sleepMs(@max(1, @divTrunc(sleep_ns +| std.time.ns_per_ms - 1, std.time.ns_per_ms)));
                continue;
            },
            else => return err,
        };
        return;
    }
}

/// Capture a portable snapshot with bounded maintenance verification.
/// Callers drain maintenance before freezing writes; this deadline bounds
/// any follow-up work that raced with their capture barrier.
/// The deadline is in this DB's BackendRuntime.monotonicClock domain.
pub fn snapshotWithMaintenanceDeadline(self: *DB, id: []const u8, maintenance_deadline_ns: u64) !u64 {
    if (comptime storage_engine.enabled) return storage_engine.call(@src().fn_name, .{ self, id, maintenance_deadline_ns });
    var row_policy_lease = try self.local_execution.row_policy_gate.enterRaw();
    defer row_policy_lease.release();
    return try self.snapshotInternal(id, false, .none, maintenance_deadline_ns, null, false, null);
}

/// Captures the primary store and its validated generated projections as
/// one self-contained generation for the native table-backup format.
pub fn snapshotNative(self: *DB, id: []const u8) !u64 {
    return try self.snapshotNativeWithCancellation(id, .none);
}

pub fn snapshotNativeWithCancellation(self: *DB, id: []const u8, cancellation: types.CancellationToken) !u64 {
    if (comptime storage_engine.enabled) return storage_engine.call(@src().fn_name, .{ self, id, cancellation });
    var row_policy_lease = try self.local_execution.row_policy_gate.enterRaw();
    defer row_policy_lease.release();
    return try self.snapshotInternal(id, true, cancellation, null, null, false, null);
}

/// Seal an exact common-cut owner generation durably. No corpus copy or
/// hashing is performed while the write fence is held.
pub fn sealBackupCohort(self: *DB, id: []const u8, expected: relational_integrity_topology.Fence, cancellation: types.CancellationToken) !native_backup_seal.Handle {
    if (comptime storage_engine.enabled) return storage_engine.call(@src().fn_name, .{ self, id, expected, cancellation });
    var row_policy_lease = try self.local_execution.row_policy_gate.enterRaw();
    defer row_policy_lease.release();
    try validateSnapshotId(id);
    if (expected.role != .backup_snapshot or !expected.namespace.eql(self.core.identity_namespace)) return error.InvalidTopologyFence;
    const seal = native_backup_seal;
    const name = try seal.nameAlloc(self.alloc, expected);
    defer self.alloc.free(name);
    _ = try self.snapshotInternal(name, true, cancellation, null, expected, true, null);
    const root = try seal.pathAlloc(self.alloc, self.core.path, expected);
    defer self.alloc.free(root);
    const io = self.backend_runtime.filesystemIo() orelse return error.BackendRuntimeIoUnavailable;
    return seal.readHandle(self.alloc, io, root, expected);
}

pub fn exportBackupCohort(self: *DB, handle: native_backup_seal.Handle, id: []const u8, cancellation: types.CancellationToken) !u64 {
    if (comptime storage_engine.enabled) return storage_engine.call(@src().fn_name, .{ self, handle, id, cancellation });
    var row_policy_lease = try self.local_execution.row_policy_gate.enterRawRead();
    defer row_policy_lease.release();
    const checkpoint: RowPolicyOutputCheckpoint = .{ .upstream = cancellation, .lease = &row_policy_lease };
    const output_cancellation = checkpoint.token();
    if (!handle.fence.namespace.eql(self.core.identity_namespace)) return error.IdentityNamespaceMismatch;
    try validateSnapshotId(id);
    try lockAtomicWithCancellation(&self.snapshot_publication_mutex, cancellation);
    defer self.snapshot_publication_mutex.unlock();
    const seal = native_backup_seal;
    const io = self.backend_runtime.filesystemIo() orelse return error.BackendRuntimeIoUnavailable;
    var pin_lock = try seal.StoreLock.acquire(self.alloc, io, self.core.path, cancellation);
    defer pin_lock.deinit();
    const root = try seal.pathAlloc(self.alloc, self.core.path, handle.fence);
    defer self.alloc.free(root);
    const tombstone = try std.fmt.allocPrint(self.alloc, "{s}.released", .{root});
    defer self.alloc.free(tombstone);
    if (try snapshotPathExists(io, tombstone)) return error.BackupSealReleased;
    const parent = try std.fmt.allocPrint(self.alloc, "{s}.snapshots", .{self.core.path});
    defer self.alloc.free(parent);
    try fs_paths.createDirPathPortable(io, parent);
    const target = try std.fmt.allocPrint(self.alloc, "{s}/{s}", .{ parent, id });
    defer self.alloc.free(target);
    if (try snapshotPathExists(io, target)) {
        const bytes = try seal.exportedBytes(self.alloc, io, target, handle);
        try output_cancellation.check();
        return bytes;
    }
    const staging = try createSnapshotStagingRoot(self.alloc, io, parent, id);
    defer self.alloc.free(staging);
    var published = false;
    defer if (!published) snapshot_staging.cleanup(io, staging);
    const total = try seal.exportTo(self.alloc, io, root, handle, staging, output_cancellation);
    try seal.recordExport(self.alloc, io, staging, handle, total);
    try output_cancellation.check();
    try publishSnapshotStaging(io, parent, staging, target);
    published = true;
    return total;
}

/// Portable logical rows from the identical immutable cut used by native
/// cohorts. The live root may already have resumed writes at this point.
pub fn exportBackupCohortPortable(self: *DB, handle: native_backup_seal.Handle, writer: *std.Io.Writer, options: portable_backup.ExportOptions, cancellation: types.CancellationToken) !void {
    if (comptime storage_engine.enabled) return storage_engine.call(@src().fn_name, .{ self, handle, writer, options, cancellation });
    if (options.source_generation_summary_output) |summary| summary.output.* = null;
    errdefer if (options.source_generation_summary_output) |summary| if (summary.output.*) |entries| {
        portable_backup.freeSourceGenerationAdmissionSummary(summary.alloc, entries);
        summary.output.* = null;
    };
    var row_policy_lease = try self.local_execution.row_policy_gate.enterRawRead();
    defer row_policy_lease.release();
    const checkpoint: RowPolicyOutputCheckpoint = .{ .upstream = cancellation, .lease = &row_policy_lease };
    const output_cancellation = checkpoint.token();
    if (options.source_certificate) |certificate| certificate.output.* = null;
    errdefer if (options.source_certificate) |certificate| {
        certificate.output.* = null;
    };
    if (!handle.fence.namespace.eql(self.core.identity_namespace)) return error.IdentityNamespaceMismatch;
    try lockAtomicWithCancellation(&self.snapshot_publication_mutex, cancellation);
    defer self.snapshot_publication_mutex.unlock();
    const seal = native_backup_seal;
    const io = self.backend_runtime.filesystemIo() orelse return error.BackendRuntimeIoUnavailable;
    var pin_lock = try seal.StoreLock.acquire(self.alloc, io, self.core.path, cancellation);
    defer pin_lock.deinit();
    const root = try seal.pathAlloc(self.alloc, self.core.path, handle.fence);
    defer self.alloc.free(root);
    const tombstone = try std.fmt.allocPrint(self.alloc, "{s}.released", .{root});
    defer self.alloc.free(tombstone);
    if (try snapshotPathExists(io, tombstone)) return error.BackupSealReleased;
    var opened = try seal.open(self.alloc, io, root, handle);
    defer opened.deinit();
    for (opened.parsed.value.files) |file| {
        try cancellation.check();
        const path = try std.fmt.allocPrint(self.alloc, "{s}/{s}", .{ root, file.path });
        defer self.alloc.free(path);
        const stat = try native_backup.statRegularFile(io, path);
        if (stat.inode != file.inode or stat.size != file.size or stat.mtime.toNanoseconds() != file.mtime_ns) return error.BackupSealSourceChanged;
    }
    var export_options = options;
    export_options.cancellation = output_cancellation;
    export_options.cohort = .{ .seal = handle, .namespace = handle.fence.namespace };
    if (std.mem.eql(u8, opened.parsed.value.primary.artifact_format, "antfly-lsm-checkpoint")) {
        const primary = try std.fmt.allocPrint(self.alloc, "{s}/{s}", .{ root, db_core.primary_lsm_checkpoint_directory_name });
        defer self.alloc.free(primary);
        var backend = try @import("../lsm_backend.zig").Backend.open(self.alloc, primary, .{ .backend = .{ .read_only = true }, .read_runtime = storage_io_mod.ReadRuntime.init(io) });
        defer backend.close();
        var store = try docstore_mod.DocStore.openRuntime(self.alloc, try backend.runtimeStore(self.alloc, .{ .name = "docs" }));
        defer store.close();
        try portable_backup.exportPortableToWriterWithOptions(self.alloc, &store, writer, export_options);
    } else if (std.mem.eql(u8, opened.parsed.value.primary.artifact_format, "antfly-kv-stream")) {
        // Logical-backend pins are already a bounded KV stream. Decode
        // into disposable LSM storage, never an in-memory corpus buffer.
        const parent = std.fs.path.dirname(root) orelse return error.InvalidBackupSeal;
        const temporary = try createSnapshotStagingRoot(self.alloc, io, parent, "portable-decoder");
        defer self.alloc.free(temporary);
        defer snapshot_staging.cleanup(io, temporary);
        var backend = try @import("../lsm_backend.zig").Backend.open(self.alloc, temporary, .{ .read_runtime = storage_io_mod.ReadRuntime.init(io) });
        defer backend.close();
        var store = try docstore_mod.DocStore.openRuntime(self.alloc, try backend.runtimeStore(self.alloc, .{ .name = "docs" }));
        defer store.close();
        _ = try db_core.importStoreSnapshotWithIo(self.alloc, io, &store, root, cancellation);
        try portable_backup.exportPortableToWriterWithOptions(self.alloc, &store, writer, export_options);
    } else return error.UnsupportedBackupFormat;
    try output_cancellation.check();
}

/// Tombstone before unlink: retries after a crash finish bounded inventory
/// cleanup, and a delayed seal can never recreate an acknowledged release.
pub fn releaseBackupCohort(self: *DB, handle: native_backup_seal.Handle) !void {
    if (!handle.fence.namespace.eql(self.core.identity_namespace)) return error.IdentityNamespaceMismatch;
    lockAtomicWithBackoff(&self.snapshot_publication_mutex);
    defer self.snapshot_publication_mutex.unlock();
    const seal = native_backup_seal;
    const io = self.backend_runtime.filesystemIo() orelse return error.BackendRuntimeIoUnavailable;
    try seal.reclaim(self.alloc, io, self.core.path, .{ .release = handle }, .none);
}

/// Cancellation consumes a planned seal even when its response was lost
/// or delivery is delayed. Caller authority is the exact cohort fence.
pub fn cancelBackupCohort(self: *DB, fence: relational_integrity_topology.Fence) !void {
    if (fence.role != .backup_snapshot or !fence.namespace.eql(self.core.identity_namespace)) return error.IdentityNamespaceMismatch;
    lockAtomicWithBackoff(&self.snapshot_publication_mutex);
    defer self.snapshot_publication_mutex.unlock();
    const seal = native_backup_seal;
    const io = self.backend_runtime.filesystemIo() orelse return error.BackendRuntimeIoUnavailable;
    try seal.reclaim(self.alloc, io, self.core.path, .{ .cancel = fence }, .none);
}

pub fn primaryOnlySnapshotSupportedLocked(self: *DB) !bool {
    if (self.local_execution.table_storage.dense_embeddings != .primary_lsm) return false;
    if (self.local_execution.source_vectors.load(.acquire) == null) return true;
    // Cancellation retains the source object for already-admitted readers
    // and background retirement. Its presence is not published authority:
    // a terminal cancelled job has removed all candidate roots and kept
    // every live artifact inline in the primary store.
    var job = (try vector_migration.load(self.alloc, self.core.store)) orelse return false;
    defer job.deinit();
    try self.validateVectorMigrationIdentity(job.value);
    return job.value.phase == .cancelled;
}

pub fn ensurePrimaryOnlySnapshotLocked(self: *DB) !void {
    var read = try self.core.store.beginReadTxn();
    defer read.abort();
    try artifact_reconcile_intent.requireAbsent(&read);
    if (!try self.primaryOnlySnapshotSupportedLocked()) return error.VectorStoreLifecycleUnsupported;
}

pub fn ensurePrimaryOnlySnapshot(self: *DB) !void {
    lockApplyShared(self);
    defer self.core.unlockApplyShared();
    try self.ensurePrimaryOnlySnapshotLocked();
}

pub fn snapshotInternal(
    self: *DB,
    id: []const u8,
    include_generated: bool,
    cancellation: types.CancellationToken,
    maintenance_deadline_ns: ?u64,
    expected_topology_fence: ?relational_integrity_topology.Fence,
    backup_seal_only: bool,
    query_cut: ?native_query_cut.Request,
) !u64 {
    if (comptime storage_engine.enabled) return storage_engine.call(@src().fn_name, .{ self, id, include_generated, cancellation, maintenance_deadline_ns, expected_topology_fence, backup_seal_only, query_cut });
    const seal_only = backup_seal_only or query_cut != null;
    if (query_cut == null) try self.ensurePrimaryOnlySnapshot();
    // Serialize only snapshot construction/publication. Normal writes can
    // resume before native manifest hashing, while same-ID captures cannot
    // race the fresh-directory check or atomic rename.
    try lockAtomicWithCancellation(&self.snapshot_publication_mutex, cancellation);
    defer self.snapshot_publication_mutex.unlock();
    try ensureSnapshotActive(cancellation);
    try validateSnapshotId(id);
    if (include_generated) {
        // Host-path exchange is the only native publication adapter
        // currently implemented. External stores need a backend-owned
        // staged namespace and atomic generation pointer; until that
        // capability exists, fail before creating or syncing artifacts.
        if (self.physical_root_mode != .filesystem_managed)
            return error.NativeBackupStorageBackendUnsupported;
        if (self.primary_lsm_storage) |storage| {
            if (!storage.supportsHostPathGenerationPublication()) {
                std.log.err(
                    "native backup requires atomic host-path generation publication for primary storage",
                    .{},
                );
                return error.NativeBackupStorageBackendUnsupported;
            }
        }
    }
    const io = self.backend_runtime.filesystemIo() orelse return error.BackendRuntimeIoUnavailable;
    const snapshot_parent = try std.fmt.allocPrint(self.alloc, "{s}.{s}", .{ self.core.path, if (query_cut != null) "query-pins" else if (seal_only) "backup-pins" else "snapshots" });
    defer self.alloc.free(snapshot_parent);
    var pin_lock: ?native_backup_seal.StoreLock = if (backup_seal_only) try native_backup_seal.StoreLock.acquire(self.alloc, io, self.core.path, cancellation) else null;
    defer if (pin_lock) |*held| held.deinit();
    const snapshot_root = try std.fmt.allocPrint(self.alloc, "{s}/{s}", .{ snapshot_parent, id });
    defer self.alloc.free(snapshot_root);
    try fs_paths.createDirPathPortable(io, snapshot_parent);
    if (seal_only) {
        const tombstone = try std.fmt.allocPrint(self.alloc, "{s}.released", .{snapshot_root});
        defer self.alloc.free(tombstone);
        if (try snapshotPathExists(io, tombstone)) return error.BackupSealReleased;
        if (try snapshotPathExists(io, snapshot_root)) {
            if (query_cut) |request| try native_query_cut.validate(self.alloc, io, snapshot_root, request, self.core.identity_namespace, cancellation) else _ = try native_backup_seal.readHandle(self.alloc, io, snapshot_root, expected_topology_fence.?);
            return 0;
        }
    } else if (try snapshotPathExists(io, snapshot_root)) return error.SnapshotAlreadyExists;
    if (query_cut != null) try self.requireQueryCutStorage();
    const staging_root = if (seal_only) blk: {
        // One deterministic unpublished namespace per exact attempt lets
        // restart/retry reclaim abandoned pins without a directory scan.
        const candidate = try std.fmt.allocPrint(self.alloc, "{s}.staging", .{snapshot_root});
        errdefer self.alloc.free(candidate);
        try std.Io.Dir.cwd().deleteTree(io, candidate);
        try fs_paths.createDirPathPortable(io, candidate);
        try fs_paths.syncDirPortable(io, snapshot_parent);
        break :blk candidate;
    } else try createSnapshotStagingRoot(self.alloc, io, snapshot_parent, id);
    defer self.alloc.free(staging_root);
    var published = false;
    defer if (!published) snapshot_staging.cleanup(io, staging_root);

    if (!include_generated) {
        // Portable snapshots omit generated files, but their replay log and
        // durable watermarks must still describe a converged generation so
        // restore can deterministically rebuild every managed projection.
        if (maintenance_deadline_ns) |deadline_ns| {
            try self.runMaintenanceUntilWithCancellation(
                self.currentMaintenanceTargetSequence(),
                .{},
                cancellation,
                deadline_ns,
            );
        } else {
            try self.runMaintenanceUntil(self.currentMaintenanceTargetSequence(), .{});
        }
        try self.lockApplyForPortableRuntime();
        var apply_held = true;
        defer if (apply_held) self.core.unlockApply();
        // Migration may have started while portable maintenance drained.
        try self.ensurePrimaryOnlySnapshotLocked();
        try self.core.syncStore(true);
        try self.core.index_manager.syncAll(true);
        var primary_snapshot = try self.core.pinPortableSnapshot();
        defer primary_snapshot.deinit();
        self.core.unlockApply();
        apply_held = false;
        var total = try primary_snapshot.materialize(
            self.alloc,
            io,
            staging_root,
            cancellation,
        );
        total = std.math.add(
            u64,
            total,
            try db_core.writeLogicalSnapshotManifest(self.alloc, io, staging_root),
        ) catch return error.SnapshotTooLarge;
        try publishSnapshotStaging(io, snapshot_parent, staging_root, snapshot_root);
        published = true;
        return total;
    }

    // Close primary/catalog admission before selecting the target. The
    // barrier is writer preferring, so continuous client traffic cannot
    // starve capture. Internal enrichment and resolution callbacks remain
    // admitted while maintenance drains already-committed work.
    var capture = self.core.snapshot_admission.acquireCaptureIo(io, @as(?types.CancellationToken, cancellation)) catch |err| switch (err) {
        error.Cancelled => return error.Canceled,
        else => return err,
    };
    defer capture.release();
    // Migration may have won admission after the optimistic entry check.
    // Its structural mutation uses this same snapshot fence.
    if (query_cut == null) try self.ensurePrimaryOnlySnapshot() else try self.requireQueryCutStorage();
    if (builtin.is_test) {
        if (DB.test_snapshot_fence_hook) |hook| hook.after_capture_admission(hook.ptr);
    }

    var structural: ?IndexStructuralMutationGuard = null;
    defer if (structural) |*guard| guard.release();
    var replay_capture: ?snapshot_admission_mod.SnapshotAdmission.CaptureLease = null;
    defer if (replay_capture) |*lease| lease.release();
    var apply_held = false;
    defer if (apply_held) self.core.unlockApply();
    var capture_target_sequence: u64 = undefined;

    // An internal callback can append a finite follow-up replay record
    // during the drain. Recheck under the final apply fence; unlike the old
    // optimistic loop, primary writes and catalog churn are already barred,
    // so an external write stream cannot keep this loop alive forever.
    while (true) {
        const target_sequence = self.currentMaintenanceTargetSequence();
        self.runMaintenanceUntilWithEnrichmentCompletion(
            target_sequence,
            .{},
            cancellation,
            maintenance_deadline_ns orelse std.math.maxInt(u64),
            if (query_cut != null) .published_requests else .successful_requests,
        ) catch |err| switch (err) {
            error.Cancelled => return error.Canceled,
            else => return err,
        };
        try ensureSnapshotActive(cancellation);
        try self.flushAppliedSequencesForIdle();

        if (query_cut != null) {
            try lockAtomicWithCancellation(&self.local_execution.index_structural_mutation_mutex, cancellation);
            structural = .{
                .db = self,
                .snapshot_mutation = capture.borrowMutation(),
                .operation = "native query cut",
                .index_name = "*",
                .restart_text_merge = self.quiesceTextMergeForStructuralMutation(),
                .restart_sparse_compaction = self.quiesceSparseCompactionForStructuralMutation(),
            };
            try structural.?.acquireCatalogBarrierWithCancellation(io, cancellation);
        } else {
            structural = self.beginDrainedIndexStructuralMutationWithLease("native snapshot", "*", capture.borrowMutation());
            if (!structural.?.acquireCatalogBarrierUntil(std.math.maxInt(u64))) unreachable;
        }
        replay_capture = self.core.snapshot_replay_admission.acquireCaptureIo(
            io,
            @as(?types.CancellationToken, cancellation),
        ) catch |err| switch (err) {
            error.Cancelled => return error.Canceled,
            else => return err,
        };
        if (query_cut != null) {
            try self.enforcePortableRuntimeGate();
            while (!self.core.tryLockApplyExclusive()) {
                try cancellation.check();
                try io.sleep(.fromMilliseconds(1), .awake);
            }
            apply_held = true;
            try self.enforcePortableRuntimeGate();
            try cancellation.check();
        } else {
            try self.lockApplyForPortableRuntime();
            apply_held = true;
        }
        if (self.currentMaintenanceTargetSequence() == target_sequence) {
            capture_target_sequence = self.core.nextDerivedSequence();
            break;
        }
        self.core.unlockApply();
        apply_held = false;
        replay_capture.?.release();
        replay_capture = null;
        structural.?.release();
        structural = null;
    }

    if (query_cut == null) try self.ensurePrimaryOnlySnapshotLocked() else try self.requireQueryCutStorage();
    if (expected_topology_fence) |expected| {
        var read = try self.core.store.beginProbeTxn();
        defer read.abort();
        var manager = try self.core.initTxnManager();
        defer manager.deinit();
        try relational_integrity_topology.requireDrained(&read, &manager, expected);
    }
    try self.core.syncStore(true);
    try self.core.index_manager.syncAll(true);
    if (self.loadIndexRepairState(self.alloc)) |repair_state_value| {
        var repair_state = repair_state_value;
        defer repair_state.deinit(self.alloc);
        if (repair_state.entries.items.len != 0)
            return error.NativeBackupRepairStateNotQuiescent;
    } else |err| switch (err) {
        error.FileNotFound, error.DurableIndexRepairStateUnavailable => {},
        else => return err,
    }

    // Pin the primary MVCC/read-snapshot while the selected revision is
    // fenced. Its corpus-sized serialization happens only after writers
    // resume and therefore does not extend write unavailability.
    const query_reuse_digest = if (query_cut != null) try self.nativeQueryReuseDigestLocked(self.alloc) else null;
    var primary_snapshot = try self.core.pinNativeSnapshot();
    defer primary_snapshot.deinit();
    if (comptime builtin.os.tag == .freestanding) {
        self.core.unlockApply();
        apply_held = false;
        replay_capture.?.release();
        replay_capture = null;
        structural.?.release();
        structural = null;
        capture.release();
        const total = try primary_snapshot.materialize(
            self.alloc,
            io,
            staging_root,
            cancellation,
        );
        try publishSnapshotStaging(io, snapshot_parent, staging_root, snapshot_root);
        published = true;
        return total;
    }

    const configs = try self.core.listIndexes(self.alloc);
    defer types.freeIndexConfigs(self.alloc, configs);
    const projections = try self.alloc.alloc(native_backup.Projection, configs.len);
    defer self.alloc.free(projections);
    for (configs, 0..) |cfg, i| {
        const checkpoint = try self.core.loadProjectionCheckpoint(self.alloc, cfg.name);
        if (checkpoint.status != .clean or
            checkpoint.config_hash != types.indexConfigHash(cfg) or
            checkpoint.applied_sequence != capture_target_sequence)
        {
            std.log.warn(
                "native backup projection is not quiescent index={s} status={s} checkpoint_sequence={} target_sequence={} checkpoint_config_hash={x} expected_config_hash={x}",
                .{
                    cfg.name,
                    @tagName(checkpoint.status),
                    checkpoint.applied_sequence,
                    capture_target_sequence,
                    checkpoint.config_hash,
                    types.indexConfigHash(cfg),
                },
            );
            return error.NativeBackupProjectionNotQuiescent;
        }
        const backend_id = self.core.index_manager.nativeBackupBackendId(cfg.name, cfg.kind);
        const has_immutable_checkpoint = self.core.index_manager.nativeBackupSupportsImmutableCheckpoint(cfg.name, cfg.kind);
        if (!has_immutable_checkpoint) {
            std.log.err(
                "native backup requires an immutable projection checkpoint index={s} kind={s} backend={s}",
                .{ cfg.name, @tagName(cfg.kind), backend_id },
            );
            return error.NativeBackupProjectionBackendUnsupported;
        }
        projections[i] = .{
            .name = cfg.name,
            .kind = @tagName(cfg.kind),
            .config_hash = types.indexConfigHash(cfg),
            .coverage_generation = cfg.coverage_generation,
            .checkpoint_generation = checkpoint.generation,
            .applied_sequence = checkpoint.applied_sequence,
            .target_sequence = capture_target_sequence,
            .artifact_format = "antfly-managed-index-tree",
            .artifact_version = 1,
            .backend_id = backend_id,
            .codec_version = 1,
            .artifact_state = .complete,
            .repair_reason = "",
        };
    }
    std.mem.sort(native_backup.Projection, projections, {}, struct {
        fn lessThan(_: void, lhs: native_backup.Projection, rhs: native_backup.Projection) bool {
            return std.mem.order(u8, lhs.name, rhs.name) == .lt;
        }
    }.lessThan);

    // Ask each backend to retain its exact manifested generation. Pinning
    // is bounded by backend/run metadata: it hardlinks manifest-named
    // immutable files and opens stable committed-WAL descriptor leases
    // without reading corpus or WAL payloads under the mutation fence.
    const generated_native_pin_root = try std.fmt.allocPrint(self.alloc, "{s}/.generated-native-pin", .{staging_root});
    defer self.alloc.free(generated_native_pin_root);
    var generated_checkpoints = try self.core.index_manager.pinNativeBackupCheckpoints(
        io,
        generated_native_pin_root,
        capture_target_sequence,
        cancellation,
    );
    defer generated_checkpoints.deinit();
    const generated_metadata_pin_root = try std.fmt.allocPrint(self.alloc, "{s}/.generated-metadata-pin", .{staging_root});
    defer self.alloc.free(generated_metadata_pin_root);
    var generated_metadata = try native_backup.pinGeneratedCheckpointMetadata(
        self.alloc,
        io,
        self.core.path,
        generated_metadata_pin_root,
        cancellation,
    );
    defer generated_metadata.deinit();

    if (seal_only) {
        const seal = native_backup_seal;
        const wal_bytes = try generated_checkpoints.walPrefixBytes();
        if (wal_bytes > seal.wal_budget_bytes) return error.BackupSealWalBudgetExceeded;
        var vector_bytes: u64 = 0;
        if (query_cut != null) if (self.local_execution.source_vectors.load(.acquire)) |source| {
            const vector_root = try std.fs.path.join(self.alloc, &.{ staging_root, "source-vectors" });
            defer self.alloc.free(vector_root);
            var remaining_wal = seal.wal_budget_bytes - wal_bytes;
            vector_bytes = try source.sealQuerySnapshot(io, vector_root, &remaining_wal, cancellation);
        };
        const primary_root = if (query_cut != null) try self.alloc.dupe(u8, staging_root) else try std.fmt.allocPrint(self.alloc, "{s}/primary-lsm", .{staging_root});
        defer self.alloc.free(primary_root);
        var total: u64 = switch (primary_snapshot) {
            .lsm => |*checkpoint| try checkpoint.seal(io, primary_root, cancellation),
            .logical => return error.BackupSealBackendUnsupported,
        };
        total = std.math.add(u64, total, vector_bytes) catch return error.FileTooBig;
        total = std.math.add(u64, total, try generated_checkpoints.seal(io, staging_root, cancellation, seal.wal_budget_bytes)) catch return error.FileTooBig;
        total = std.math.add(u64, total, try generated_metadata.seal(staging_root, cancellation, 0)) catch return error.FileTooBig;
        // The durable tree has its own links; discard temporary lease
        // trees before its inventory and atomic publication.
        try std.Io.Dir.cwd().deleteTree(io, generated_native_pin_root);
        try std.Io.Dir.cwd().deleteTree(io, generated_metadata_pin_root);
        if (query_cut) |request| {
            const final_digest = try self.nativeQueryReuseDigestLocked(self.alloc);
            const reuse_digest = if (query_reuse_digest != null and final_digest != null and std.mem.eql(u8, &query_reuse_digest.?, &final_digest.?)) query_reuse_digest else null;
            try native_query_cut.finishWithReuseDigest(self.alloc, io, staging_root, request, self.core.identity_namespace, capture_target_sequence, reuse_digest, cancellation);
        } else _ = try seal.finish(self.alloc, io, staging_root, expected_topology_fence.?, capture_target_sequence, .{
            .artifact_format = primary_snapshot.artifactFormat(),
            .artifact_version = primary_snapshot.artifactVersion(),
            .source_backend = @tagName(self.primary_backend),
        }, projections, cancellation);
        try publishSnapshotStaging(io, snapshot_parent, staging_root, snapshot_root);
        published = true;
        return total;
    }

    // The primary store and hardlink-pinned generated files now form one
    // stable generation. Release the latency-sensitive apply lock first,
    // then all mutation admission before reading corpus bytes.
    self.core.unlockApply();
    apply_held = false;
    if (builtin.is_test) {
        if (DB.test_snapshot_fence_hook) |hook| {
            if (hook.after_apply_release) |run| run(hook.ptr);
        }
    }

    replay_capture.?.release();
    replay_capture = null;
    structural.?.release();
    structural = null;
    capture.release();

    // Both primary and generated artifacts remain pinned. Expensive file
    // copies, primary serialization, hashing, and manifest encoding stay
    // off both the apply lock and mutation-admission paths.
    if (builtin.is_test) {
        if (DB.test_snapshot_fence_hook) |hook| {
            if (hook.before_artifact_materialize) |run| run(hook.ptr);
        }
    }
    var artifact_receipts = native_backup.ArtifactReceiptCollector.init(
        self.alloc,
        staging_root,
        projections,
    );
    defer artifact_receipts.deinit();
    const receipt_sink = artifact_receipts.sink();
    var total = try generated_checkpoints.materializeWithSink(io, staging_root, cancellation, receipt_sink);
    total = std.math.add(
        u64,
        total,
        try generated_metadata.materializeWithSink(staging_root, cancellation, receipt_sink),
    ) catch return error.FileTooBig;
    total = std.math.add(u64, total, try primary_snapshot.materializeWithSink(
        self.alloc,
        io,
        staging_root,
        cancellation,
        receipt_sink,
    )) catch return error.FileTooBig;
    total = std.math.add(u64, total, try native_backup.finalizeCaptureGenerationFromReceiptsWithCancellation(
        self.alloc,
        io,
        &artifact_receipts,
        capture_target_sequence,
        .{
            .artifact_format = primary_snapshot.artifactFormat(),
            .artifact_version = primary_snapshot.artifactVersion(),
            .source_backend = @tagName(self.primary_backend),
        },
        cancellation,
    )) catch return error.FileTooBig;
    try ensureSnapshotActive(cancellation);
    try publishSnapshotStaging(io, snapshot_parent, staging_root, snapshot_root);
    published = true;
    return total;
}

pub fn restoreSnapshotStoreTo(
    alloc: Allocator,
    snapshot_root: []const u8,
    path: []const u8,
    opts: OpenOptions,
    restore_identity: ?RestoreIdentity,
    restore_io: ?Io,
    cancellation: types.CancellationToken,
    native_manifest: ?*const native_backup.Manifest,
    purpose: RestorePurpose,
) !void {
    try cancellation.check();
    const shared_io = restore_io orelse if (opts.backend_runtime) |runtime| runtime.filesystemIo() else null;
    const identity_io = if (restore_identity != null)
        shared_io orelse return error.BackendRuntimeIoUnavailable
    else
        null;
    if (restore_identity) |identity| {
        try beginRestoreImportWithIo(alloc, identity_io.?, path, snapshot_root, identity);
    }
    const physical_primary = if (native_manifest) |manifest|
        std.mem.eql(u8, manifest.primary.artifact_format, "antfly-lsm-checkpoint")
    else
        false;
    if (physical_primary) switch (opts.primary_backend) {
        .lsm => {},
        else => return error.UnsupportedBackupFormat,
    };

    var opened_primary = try openPrimaryStore(alloc, path, .{
        .map_size = opts.map_size,
        .no_sync = opts.no_sync,
        .primary_backend = opts.primary_backend,
        .primary_runtime_store = opts.primary_runtime_store,
        .storage = opts.storage,
        .index_backends = opts.index_backends,
    });
    defer {
        opened_primary.store.close();
        opened_primary.owner.close(alloc);
    }

    const streaming_store = if (physical_primary)
        true
    else blk: {
        try db_core.clearAllKeysFromStore(alloc, &opened_primary.store);
        break :blk if (shared_io) |io|
            try db_core.importStoreSnapshotWithIo(alloc, io, &opened_primary.store, snapshot_root, cancellation)
        else fallback: {
            try db_core.importStoreSnapshot(alloc, &opened_primary.store, snapshot_root);
            break :fallback false;
        };
    };
    try cancellation.check();
    if (purpose == .historical_table)
        try doc_identity.upgradeImportedLiveDirectoryAlloc(alloc, &opened_primary.store, cancellation);
    try doc_identity.validateStoreAlloc(alloc, &opened_primary.store);
    try validateRestoredIdentityNamespace(&opened_primary.store, opts);
    // A table-local historical image cannot certify references owned by
    // other tables at a later cut. Keep the candidate unpublished until a
    // cluster-coordinated restore/activation barrier is available.
    if (purpose == .historical_table) {
        const topology_fence = try missingAsNull(opened_primary.store.get(alloc, relational_integrity_topology.fence_key));
        if (topology_fence) |bytes| {
            alloc.free(bytes);
            return error.CoordinatedConstraintRestoreRequired;
        }
        const retirement = try missingAsNull(opened_primary.store.get(alloc, relational_integrity_retirement.key));
        if (retirement) |bytes| {
            alloc.free(bytes);
            return error.CoordinatedConstraintRestoreRequired;
        }
    }
    const restored_integrity = try missingAsNull(opened_primary.store.get(alloc, relational_integrity_catalog.key));
    if (restored_integrity) |bytes| {
        defer alloc.free(bytes);
        var catalog = try relational_integrity_catalog.decode(alloc, bytes);
        defer catalog.deinit();
        if (relational_integrity_activation.hasActive(catalog)) switch (purpose) {
            .historical_table => return error.CoordinatedConstraintRestoreRequired,
            .identity_preserving => |expected_namespace| {
                const stored_namespace = (try doc_identity.loadNamespaceFromStore(&opened_primary.store)) orelse return error.IdentityNamespaceMismatch;
                if (!stored_namespace.eql(expected_namespace)) return error.IdentityNamespaceMismatch;
                const incarnation = try relational_integrity_catalog.incarnationFromTableId(expected_namespace.table_id);
                if (!std.mem.eql(u8, &incarnation, &catalog.incarnation)) return error.IntegrityCatalogIncarnationMismatch;
            },
        };
    }
    if (!streaming_store)
        try db_core.importChangeJournalSnapshot(alloc, &opened_primary.store, snapshot_root);
    // The candidate root owns its identity. Creating it while the staged
    // capability is still held makes identity part of the durable tree
    // that is sealed and atomically published, rather than an open-time
    // side effect after the generation becomes visible.
    if (opts.physical_root_mode == .filesystem_managed) {
        _ = if (shared_io) |io|
            try root_identity.loadOrCreate(alloc, io, path)
        else
            try loadOrCreateDurableRootIdentity(alloc, opts.backend_runtime, path);
    }
    if (restore_identity) |identity| {
        try markRestorePrimaryRestoredForPathWithIdentityWithIo(alloc, identity_io.?, path, identity);
    }
}

pub fn validateRestoredIdentityNamespace(store: *docstore_mod.DocStore, opts: OpenOptions) !void {
    if (opts.identity_namespace == null or opts.prefer_existing_identity_namespace) return;
    const stored = (try doc_identity.loadNamespaceFromStore(store)) orelse return;
    if (!stored.eql(opts.identity_namespace.?)) return error.IdentityNamespaceMismatch;
}

pub fn restoreSnapshotTo(alloc: Allocator, snapshot_root: []const u8, path: []const u8, opts: OpenOptions) !void {
    try restoreSnapshotStoreTo(alloc, snapshot_root, path, opts, null, null, .none, null, .historical_table);
    try finishSnapshotRuntimeRepair(alloc, path, opts);
}

pub fn finishSnapshotRuntimeRepair(alloc: Allocator, path: []const u8, opts: OpenOptions) !void {
    // Graph reverse indexes are derived from stored outgoing edge keys, so
    // restore them after the logical store and derived log are rehydrated.
    var restored = try DB.open(alloc, path, opts);
    defer restored.close();
    _ = try restored.rebuildDenseIndexesForTargetCoverage(alloc);
    _ = try restored.rebuildSparseIndexesForTargetCoverage(alloc);
    try restored.rebuildGraphIndexesForTargetCoverage(alloc);
    try restored.core.index_manager.syncAll(true);
}

/// A verified primary is complete. Reconstruct only
/// derived projections in its unpublished generation; never run optional
/// transaction recovery or advance the replicated mutation watermark.
pub fn repairVerifiedPrimarySnapshot(
    alloc: Allocator,
    staged: *const generation_lifecycle.StagedGeneration,
    namespace: [24]u8,
    receipt: ?OrderedApplyReceipt,
    root_generation: u64,
) !void {
    if (comptime storage_engine.enabled) return storage_engine.call(@src().fn_name, .{ alloc, staged, namespace, receipt, root_generation });
    try staged.validatePath(staged.path());
    const options: OpenOptions = .{
        .staged_generation = staged,
        .open_mode = .writer_no_replay,
        .start_optional_runtimes = false,
        .start_index_workers = false,
        .lsm_root_generation = root_generation,
        .identity_namespace = .{
            .table_id = std.mem.readInt(u64, namespace[0..8], .big),
            .shard_id = std.mem.readInt(u64, namespace[8..16], .big),
            .range_id = std.mem.readInt(u64, namespace[16..24], .big),
        },
    };
    try finishSnapshotRuntimeRepair(alloc, staged.path(), options);
    // Prove a fresh owner can use the files, not merely the rebuilding
    // owner's transient in-memory structures.
    var verify = try DB.open(alloc, staged.path(), options);
    defer verify.close();
    try verify.verifyTransferablePrimarySnapshot(namespace, receipt);
    if (verify.core.index_manager.hasLoadFailures() or verify.core.index_manager.hasRepairUnavailableIndexes()) return error.RestoreIndexLoadIncomplete;
    try verify.core.index_manager.syncAll(true);
    try verify.core.syncStore(true);
}

pub fn restoreSnapshotToStagedGeneration(
    staged_generation: *const generation_lifecycle.StagedGeneration,
    alloc: Allocator,
    snapshot_root: []const u8,
    path: []const u8,
    opts: OpenOptions,
) !void {
    if (comptime storage_engine.enabled) return storage_engine.call(@src().fn_name, .{ staged_generation, alloc, snapshot_root, path, opts });
    try staged_generation.validatePath(path);
    var staged_opts = opts;
    staged_opts.staged_generation = staged_generation;
    try restoreSnapshotTo(alloc, snapshot_root, path, staged_opts);
}

/// Install a verified snapshot while preserving its namespace and catalog
/// incarnation. The staged capability keeps it unpublished during repair.
pub fn restoreIdentityPreservingSnapshotToStagedGeneration(
    staged_generation: *const generation_lifecycle.StagedGeneration,
    alloc: Allocator,
    snapshot_root: []const u8,
    path: []const u8,
    opts: OpenOptions,
    expected_namespace: doc_identity.Namespace,
) !void {
    if (comptime storage_engine.enabled) return storage_engine.call(@src().fn_name, .{ staged_generation, alloc, snapshot_root, path, opts, expected_namespace });
    try staged_generation.validatePath(path);
    if (expected_namespace.table_id == 0 or opts.prefer_existing_identity_namespace or
        !(opts.identity_namespace orelse return error.IdentityNamespaceMismatch).eql(expected_namespace))
        return error.IdentityNamespaceMismatch;
    var staged_opts = opts;
    staged_opts.staged_generation = staged_generation;
    try restoreSnapshotStoreTo(alloc, snapshot_root, path, staged_opts, null, null, .none, null, .{ .identity_preserving = expected_namespace });
    try finishSnapshotRuntimeRepair(alloc, path, staged_opts);
}

/// Materialize an authenticated native backup into a disposable source
/// generation, retaining SOURCE identities only in this read-only decoder.
/// No source catalog/index/claim bytes are ever installed into the target.
/// The artifact digest is verified by the private restore RPC before this
/// filesystem operation; it is then bound into the target owner's Scope.
pub fn openVerifiedRestoreSource(staged_generation: *const generation_lifecycle.StagedGeneration, alloc: Allocator, snapshot_root: []const u8, path: []const u8, opts: OpenOptions, expected_namespace: doc_identity.Namespace) !DB {
    if (comptime storage_engine.enabled) return storage_engine.call(@src().fn_name, .{ staged_generation, alloc, snapshot_root, path, opts, expected_namespace });
    return openVerifiedRestoreSourceWithCancellation(staged_generation, alloc, snapshot_root, path, opts, expected_namespace, .none);
}

pub fn openVerifiedRestoreSourceWithCancellation(staged_generation: *const generation_lifecycle.StagedGeneration, alloc: Allocator, snapshot_root: []const u8, path: []const u8, opts: OpenOptions, expected_namespace: doc_identity.Namespace, cancellation: types.CancellationToken) !DB {
    if (comptime storage_engine.enabled) return storage_engine.call(@src().fn_name, .{ staged_generation, alloc, snapshot_root, path, opts, expected_namespace, cancellation });
    try cancellation.check();
    try staged_generation.validatePath(path);
    if (expected_namespace.table_id == 0) return error.IdentityNamespaceMismatch;
    var source_opts = opts;
    source_opts.identity_namespace = expected_namespace;
    source_opts.prefer_existing_identity_namespace = false;
    source_opts.staged_generation = staged_generation;
    source_opts.start_index_workers = false;
    source_opts.start_optional_runtimes = false;
    const io = if (opts.backend_runtime) |runtime| runtime.filesystemIo() orelse return error.BackendRuntimeIoUnavailable else return error.BackendRuntimeIoUnavailable;
    var native_generation = try native_backup.validateAndMaterializeWithCancellation(alloc, io, snapshot_root, path, cancellation);
    defer if (native_generation) |*manifest| manifest.deinit();
    if (native_generation) |*manifest| {
        if (std.mem.eql(u8, manifest.value().primary.artifact_format, "antfly-lsm-checkpoint")) source_opts.primary_backend = .{ .lsm = .{} };
    }
    try restoreSnapshotStoreTo(alloc, snapshot_root, path, source_opts, null, io, cancellation, if (native_generation) |*manifest| manifest.value() else null, .{ .identity_preserving = expected_namespace });
    source_opts.open_mode = .query_readonly;
    source_opts.primary_only_readonly = true;
    return try DB.open(alloc, path, source_opts);
}

pub fn restoreSnapshotToDeferredRuntimeRepair(
    staged_generation: *const generation_lifecycle.StagedGeneration,
    alloc: Allocator,
    snapshot_root: []const u8,
    path: []const u8,
    opts: OpenOptions,
    identity: RestoreIdentity,
) !void {
    const runtime = opts.backend_runtime orelse return error.BackendRuntimeIoUnavailable;
    const io = runtime.filesystemIo() orelse return error.BackendRuntimeIoUnavailable;
    return try restoreSnapshotToDeferredRuntimeRepairWithIo(
        staged_generation,
        alloc,
        io,
        snapshot_root,
        path,
        opts,
        identity,
    );
}

pub fn restoreSnapshotToDeferredRuntimeRepairWithIo(
    staged_generation: *const generation_lifecycle.StagedGeneration,
    alloc: Allocator,
    io: Io,
    snapshot_root: []const u8,
    path: []const u8,
    opts: OpenOptions,
    identity: RestoreIdentity,
) !void {
    return try restoreSnapshotToDeferredRuntimeRepairWithIoAndCancellation(
        staged_generation,
        alloc,
        io,
        snapshot_root,
        path,
        opts,
        identity,
        .none,
    );
}

pub fn restoreSnapshotToDeferredRuntimeRepairWithIoAndCancellation(
    staged_generation: *const generation_lifecycle.StagedGeneration,
    alloc: Allocator,
    io: Io,
    snapshot_root: []const u8,
    path: []const u8,
    opts: OpenOptions,
    identity: RestoreIdentity,
    cancellation: types.CancellationToken,
) !void {
    if (comptime storage_engine.enabled) return storage_engine.call(@src().fn_name, .{ staged_generation, alloc, io, snapshot_root, path, opts, identity, cancellation });
    try staged_generation.validatePath(path);
    var native_generation = try native_backup.validateAndMaterializeWithCancellation(
        alloc,
        io,
        snapshot_root,
        path,
        cancellation,
    );
    defer if (native_generation) |*generation| generation.deinit();
    if (native_generation) |*generation| return try restoreMaterializedNativeGenerationToDeferredRuntimeRepairWithIoAndCancellation(
        staged_generation,
        alloc,
        io,
        snapshot_root,
        path,
        opts,
        identity,
        generation,
        cancellation,
    );
    try restoreSnapshotStoreTo(alloc, snapshot_root, path, opts, identity, io, cancellation, null, .historical_table);
}

/// Installs a generation whose declared files were already authenticated
/// and copied directly into the unpublished destination. This is the
/// manifest-first remote restore path; it deliberately performs no source
/// tree enumeration or second corpus-sized validation pass.
pub fn restoreMaterializedNativeGenerationToDeferredRuntimeRepairWithIoAndCancellation(
    staged_generation: *const generation_lifecycle.StagedGeneration,
    alloc: Allocator,
    io: Io,
    primary_snapshot_root: []const u8,
    path: []const u8,
    opts: OpenOptions,
    identity: RestoreIdentity,
    native_generation: *native_backup.LoadedManifest,
    cancellation: types.CancellationToken,
) !void {
    if (comptime storage_engine.enabled) return storage_engine.call(@src().fn_name, .{ staged_generation, alloc, io, primary_snapshot_root, path, opts, identity, native_generation, cancellation });
    // Tree-materialized callers may already contain every projection.
    // Remove statically incompatible physical codecs before opening the
    // staged catalog; the manifest-first remote planner calls the primary
    // phase directly and has not downloaded these groups yet.
    try classifyIncompatibleNativeProjections(native_generation, opts);
    try native_generation.discardInvalidProjectionArtifacts(io, path);
    try restoreMaterializedNativePrimaryAndPlanProjectionsWithIoAndCancellation(
        staged_generation,
        alloc,
        io,
        primary_snapshot_root,
        path,
        opts,
        identity,
        native_generation,
        cancellation,
    );
    try finishMaterializedNativeGenerationWithIo(
        staged_generation,
        alloc,
        io,
        path,
        opts,
        native_generation,
        true,
    );
}

/// Restores only the primary and small generation metadata, then uses the
/// restored catalog to decide which projection artifact groups are worth
/// transferring. No physical projection corpus is required by this phase.
pub fn restoreMaterializedNativePrimaryAndPlanProjectionsWithIoAndCancellation(
    staged_generation: *const generation_lifecycle.StagedGeneration,
    alloc: Allocator,
    io: Io,
    primary_snapshot_root: []const u8,
    path: []const u8,
    opts: OpenOptions,
    identity: RestoreIdentity,
    native_generation: *native_backup.LoadedManifest,
    cancellation: types.CancellationToken,
) !void {
    if (comptime storage_engine.enabled) return storage_engine.call(@src().fn_name, .{ staged_generation, alloc, io, primary_snapshot_root, path, opts, identity, native_generation, cancellation });
    try staged_generation.validatePath(path);
    try classifyIncompatibleNativeProjections(native_generation, opts);
    try restoreSnapshotStoreTo(
        alloc,
        primary_snapshot_root,
        path,
        opts,
        identity,
        io,
        cancellation,
        native_generation.value(),
        .historical_table,
    );
    var validation_opts = opts;
    validation_opts.open_mode = .query_readonly;
    validation_opts.staged_generation = staged_generation;
    validation_opts.start_index_workers = false;
    validation_opts.start_optional_runtimes = false;
    validation_opts.start_optional_runtime_workers = false;
    var validation = try DB.open(alloc, path, validation_opts);
    defer validation.close();
    try validation.classifyNativeProjectionCatalogCompatibility(alloc, native_generation);
}

/// Local bound databases do not have a non-zero Raft group identity. Return
/// whether a complete native generation was installed so the caller can
/// run the legacy logical fallback without manufacturing replica provenance.
pub fn restoreSnapshotToLocalDeferredRuntimeRepairWithIo(
    staged_generation: *const generation_lifecycle.StagedGeneration,
    alloc: Allocator,
    io: Io,
    snapshot_root: []const u8,
    path: []const u8,
    opts: OpenOptions,
) !bool {
    return try restoreSnapshotToLocalDeferredRuntimeRepairWithIoAndCancellation(
        staged_generation,
        alloc,
        io,
        snapshot_root,
        path,
        opts,
        .none,
    );
}

pub fn restoreSnapshotToLocalDeferredRuntimeRepairWithIoAndCancellation(
    staged_generation: *const generation_lifecycle.StagedGeneration,
    alloc: Allocator,
    io: Io,
    snapshot_root: []const u8,
    path: []const u8,
    opts: OpenOptions,
    cancellation: types.CancellationToken,
) !bool {
    if (comptime storage_engine.enabled) return storage_engine.call(@src().fn_name, .{ staged_generation, alloc, io, snapshot_root, path, opts, cancellation });
    try staged_generation.validatePath(path);
    var native_generation = try native_backup.validateAndMaterializeWithCancellation(
        alloc,
        io,
        snapshot_root,
        path,
        cancellation,
    );
    defer if (native_generation) |*generation| generation.deinit();
    if (native_generation) |*generation| {
        try classifyIncompatibleNativeProjections(generation, opts);
        try generation.discardInvalidProjectionArtifacts(io, path);
    }
    try restoreSnapshotStoreTo(
        alloc,
        snapshot_root,
        path,
        opts,
        null,
        io,
        cancellation,
        if (native_generation) |*generation| generation.value() else null,
        .historical_table,
    );
    if (native_generation) |*generation| {
        try validateInstalledNativeBackupGeneration(staged_generation, alloc, io, path, opts, generation, false);
        return true;
    }
    return false;
}

pub fn validateInstalledNativeBackupGeneration(
    staged_generation: *const generation_lifecycle.StagedGeneration,
    alloc: Allocator,
    io: Io,
    path: []const u8,
    opts: OpenOptions,
    native_generation: *native_backup.LoadedManifest,
    complete_runtime_repair: bool,
) !void {
    // Repair identities are replica-local capabilities. The checkpoint is
    // part of the checksummed generation so pending debt cannot disappear,
    // but a quiesced fast-path snapshot is required to carry no entries.
    // Rebind that empty state before any staged open so two restored roots
    // never share repair authority.
    const repair_path = try index_repair_state.checkpointPathAlloc(alloc, path);
    defer alloc.free(repair_path);
    if (index_repair_state.load(alloc, repair_path)) |repair_state_value| {
        var repair_state = repair_state_value;
        defer repair_state.deinit(alloc);
        if (repair_state.entries.items.len != 0)
            return error.NativeBackupRepairStateNotQuiescent;
        var rebound = try index_repair_state.resetForRootGeneration(
            alloc,
            repair_path,
            repair_state.identity,
            opts.lsm_root_generation,
        );
        rebound.deinit(alloc);
    } else |err| switch (err) {
        error.FileNotFound => {},
        else => return err,
    }

    // Native backup artifacts are authenticated into their portable
    // canonical paths. Before the staged root can become visible, rehome
    // every valid native dense projection beneath an explicit physical
    // generation and publish the same incompatible v2 pointer used by an
    // online migration. This is an O(index-count) rename/publication step;
    // restoring native data must never manufacture a corpus-wide rebuild.
    for (native_generation.value().projections) |projection| {
        if (native_generation.projectionInvalid(projection.name) or
            !std.mem.eql(u8, projection.kind, @tagName(types.IndexKind.dense_vector)) or
            !std.mem.eql(
                u8,
                projection.backend_id,
                index_manager_mod.IndexManager.dense_native_backup_backend_id,
            )) continue;
        try index_manager_mod.IndexManager.publishRestoredNativePhysicalGeneration(
            alloc,
            path,
            projection.name,
            projection.config_hash,
            projection.applied_sequence,
            native_generation.value().capture_target_sequence,
        );
    }

    var validation_opts = opts;
    validation_opts.open_mode = if (native_generation.invalid_projections.items.len == 0)
        .query_readonly
    else
        .writer_no_replay;
    validation_opts.staged_generation = staged_generation;
    validation_opts.start_index_workers = false;
    validation_opts.start_optional_runtimes = false;
    validation_opts.start_optional_runtime_workers = false;
    {
        var validation = try DB.open(alloc, path, validation_opts);
        defer validation.close();
        try validation.validateNativeBackupGeneration(alloc, native_generation);
    }
    if (native_generation.invalid_projections.items.len != 0) {
        try native_generation.discardInvalidProjectionArtifacts(io, path);
        validation_opts.open_mode = .writer_no_replay;
        var restored = try DB.open(alloc, path, validation_opts);
        defer restored.close();
        for (native_generation.invalid_projections.items) |invalid| {
            // A projection present only in the native manifest is stale
            // relative to the restored catalog. Its files were discarded,
            // but there is no configured runtime generation to repair.
            const cfg = restored.core.index_manager.get(invalid.name) orelse continue;
            const previous = try restored.core.loadProjectionCheckpoint(alloc, invalid.name);
            try restored.core.saveProjectionCheckpoint(invalid.name, .{
                .applied_sequence = 0,
                .status = .repair_required,
                .generation = previous.generation +| 1,
                .config_hash = types.indexConfigHash(cfg.*),
            });
            _ = try restored.createGenerationRepairIntentAtTarget(
                alloc,
                cfg.*,
                .projection_generation_invalid,
                0,
                0,
                @tagName(invalid.reason),
                native_generation.value().capture_target_sequence,
                .not_required,
                .repair,
            );
        }
        try restored.core.index_manager.syncAll(true);
        if (complete_runtime_repair) {
            try restored.updateRestoreRuntimeRepairPhaseWithIo(alloc, io, "repair_indexes", false);
        }
    } else if (complete_runtime_repair) {
        try markRestoreRuntimeRepairCompleteWithIo(alloc, io, path);
    }
}

pub fn validateNativeBackupGeneration(self: *DB, alloc: Allocator, native_generation: *native_backup.LoadedManifest) !void {
    try self.classifyNativeProjectionCatalogCompatibility(alloc, native_generation);
    const configs = try self.core.listIndexes(alloc);
    defer types.freeIndexConfigs(alloc, configs);
    var configs_by_name = std.StringHashMapUnmanaged(*const types.IndexConfig).empty;
    defer configs_by_name.deinit(alloc);
    try configs_by_name.ensureTotalCapacity(alloc, @intCast(configs.len));
    for (configs) |*cfg| {
        const entry = configs_by_name.getOrPutAssumeCapacity(cfg.name);
        if (entry.found_existing) return error.InvalidIndexCatalog;
        entry.value_ptr.* = cfg;
    }
    for (native_generation.value().projections) |projection| {
        if (native_generation.projectionInvalid(projection.name)) continue;
        const cfg = configs_by_name.get(projection.name) orelse continue;
        if (self.core.index_manager.loadFailure(projection.name)) |err_name| {
            if (nativeBackupProjectionLoadFailureIsRepairable(err_name)) {
                try native_generation.invalidateProjection(projection.name, .unreadable);
                continue;
            }
            // Unknown, resource, and transient I/O failures are not proof
            // that a checksummed generation is corrupt. Leave the staged
            // tree intact so the asynchronous restore job can retry.
            std.log.warn("native backup projection validation deferred index={s} err={s}", .{ projection.name, err_name });
            return error.NativeBackupProjectionValidationIndeterminate;
        }
        // The primary-only transfer plan cannot validate a dense checkpoint:
        // its authoritative sequence lives in the still-unrestored posting
        // generation. Validate physical coverage only after installation.
        const checkpoint = try self.core.loadProjectionCheckpoint(alloc, projection.name);
        if (checkpoint.status != .clean or
            checkpoint.config_hash != projection.config_hash or
            checkpoint.generation != projection.checkpoint_generation or
            checkpoint.applied_sequence != projection.applied_sequence or
            checkpoint.applied_sequence != projection.target_sequence)
        {
            std.log.err("native backup projection checkpoint mismatch index={s} status={s} checkpoint_hash={d} manifest_hash={d} checkpoint_generation={d} manifest_generation={d} checkpoint_applied={d} manifest_applied={d} manifest_target={d}", .{
                projection.name,
                @tagName(checkpoint.status),
                checkpoint.config_hash,
                projection.config_hash,
                checkpoint.generation,
                projection.checkpoint_generation,
                checkpoint.applied_sequence,
                projection.applied_sequence,
                projection.target_sequence,
            });
            try native_generation.invalidateProjection(projection.name, .checkpoint_mismatch);
            continue;
        }
        if (cfg.kind == .dense_vector) {
            const dense = self.core.index_manager.denseIndex(projection.name) orelse {
                try native_generation.invalidateProjection(projection.name, .unreadable);
                continue;
            };
            if (@hasDecl(@TypeOf(dense.index.*), "validateStoredStructure")) {
                dense.index.validateStoredStructure(alloc) catch |err| switch (err) {
                    error.NotFound, error.FileNotFound, error.Corrupted => {
                        try native_generation.invalidateProjection(projection.name, .unreadable);
                    },
                    else => {
                        // A transient resource or I/O failure is not
                        // evidence that the checksummed artifact is bad.
                        // Preserve the staged generation and let the
                        // durable restore job retry validation.
                        std.log.warn("native backup dense projection validation deferred index={s} err={s}", .{ projection.name, @errorName(err) });
                        return error.NativeBackupProjectionValidationIndeterminate;
                    },
                };
            }
        }
    }
}

pub fn nativeBackupProjectionLoadFailureIsRepairable(err_name: []const u8) bool {
    const repairable = [_][]const u8{
        "Corrupted",
        "FileNotFound",
        "NotFound",
        "UnsupportedVersion",
        "IncompleteBulkPublish",
    };
    for (repairable) |candidate| {
        if (std.mem.eql(u8, err_name, candidate)) return true;
    }
    return false;
}

pub fn beginRestoreImport(alloc: Allocator, path: []const u8, snapshot_root: []const u8, identity: RestoreIdentity) !void {
    var io_impl = threadedIo();
    defer io_impl.deinit();
    return try beginRestoreImportWithIo(alloc, io_impl.io(), path, snapshot_root, identity);
}

pub fn beginRestoreImportWithIo(alloc: Allocator, io: Io, path: []const u8, snapshot_root: []const u8, identity: RestoreIdentity) !void {
    if (snapshot_root.len == 0 or !validRestoreIdentity(identity))
        return error.InvalidRestoreImportMarker;
    try fs_paths.createDirPathPortable(io, path);
    const restore_intent_path = try restoreIntentMarkerPathAlloc(alloc, path);
    defer alloc.free(restore_intent_path);
    const restore_state_path = try restoreStateMarkerPathAlloc(alloc, path);
    defer alloc.free(restore_state_path);
    const import_marker_path = try restoreImportMarkerPathAlloc(alloc, path);
    defer alloc.free(import_marker_path);

    try deleteFileIfExists(io, restore_intent_path);
    try deleteFileIfExists(io, restore_state_path);
    const marker = try std.json.Stringify.valueAlloc(alloc, RestoreImportDisk{
        .snapshot_root = snapshot_root,
        .backup_id = identity.backup_id,
        .location = identity.location,
        .artifact_sha256 = identity.artifact_sha256,
        .native_manifest_size_bytes = identity.native_manifest_size_bytes,
        .native_manifest_sha256 = identity.native_manifest_sha256,
        .snapshot_path = identity.snapshot_path,
        .group_id = identity.group_id,
    }, .{});
    defer alloc.free(marker);
    try writeRestoreMarkerAtomicWithIo(alloc, io, path, import_marker_path, marker);
}

pub fn recoverIncompleteRestoreImportIfNeeded(alloc: Allocator, path: []const u8, opts: OpenOptions) !bool {
    var io_impl = threadedIo();
    defer io_impl.deinit();
    return try recoverIncompleteRestoreImportIfNeededWithIo(alloc, io_impl.io(), path, opts);
}

pub fn recoverIncompleteRestoreImportIfNeededWithIo(
    alloc: Allocator,
    io: Io,
    path: []const u8,
    opts: OpenOptions,
) !bool {
    if (comptime storage_engine.enabled) return storage_engine.call(@src().fn_name, .{ alloc, io, path, opts });
    if (try readRestoreStateForPathAllocWithIo(alloc, io, path)) |state_value| {
        var state = state_value;
        state.deinit(alloc);
        return false;
    }

    var import_state = (try readRestoreImportStateAllocWithIo(alloc, io, path)) orelse return false;
    defer import_state.deinit(alloc);
    const identity_state = import_state.identity;
    const identity: RestoreIdentity = .{
        .backup_id = identity_state.backup_id,
        .location = identity_state.location,
        .artifact_sha256 = identity_state.artifact_sha256,
        .native_manifest_size_bytes = identity_state.native_manifest_size_bytes,
        .native_manifest_sha256 = identity_state.native_manifest_sha256,
        .snapshot_path = identity_state.snapshot_path,
        .group_id = identity_state.group_id,
    };
    std.log.warn("recovering incomplete restore import phase=startup", .{});
    try restoreSnapshotStoreTo(alloc, import_state.snapshot_root, path, opts, identity, io, .none, null, .historical_table);
    return true;
}

pub fn readRestoreStateForPath(alloc: Allocator, path: []const u8) !?RestoreState {
    if (comptime storage_engine.enabled) return storage_engine.call(@src().fn_name, .{ alloc, path });
    return try readRestoreStateForPathAlloc(alloc, path);
}

pub fn readRestoreStateForPathWithIo(alloc: Allocator, io: Io, path: []const u8) !?RestoreState {
    if (comptime storage_engine.enabled) return storage_engine.call(@src().fn_name, .{ alloc, io, path });
    return try readRestoreStateForPathAllocWithIo(alloc, io, path);
}

pub fn markRestorePrimaryRestoredForPathWithArtifact(
    alloc: Allocator,
    path: []const u8,
    backup_id: []const u8,
    location: []const u8,
    artifact_sha256: []const u8,
    snapshot_path: []const u8,
    group_id: u64,
) !void {
    if (comptime storage_engine.enabled) return storage_engine.call(@src().fn_name, .{ alloc, path, backup_id, location, artifact_sha256, snapshot_path, group_id });
    var io_impl = threadedIo();
    defer io_impl.deinit();
    return try markRestorePrimaryRestoredForPathWithArtifactWithIo(
        alloc,
        io_impl.io(),
        path,
        backup_id,
        location,
        artifact_sha256,
        snapshot_path,
        group_id,
    );
}

pub fn markRestorePrimaryRestoredForPathWithArtifactWithIo(
    alloc: Allocator,
    io: Io,
    path: []const u8,
    backup_id: []const u8,
    location: []const u8,
    artifact_sha256: []const u8,
    snapshot_path: []const u8,
    group_id: u64,
) !void {
    if (comptime storage_engine.enabled) return storage_engine.call(@src().fn_name, .{ alloc, io, path, backup_id, location, artifact_sha256, snapshot_path, group_id });
    return try markRestorePrimaryRestoredForPathWithIdentityWithIo(alloc, io, path, .{
        .backup_id = backup_id,
        .location = location,
        .artifact_sha256 = artifact_sha256,
        .snapshot_path = snapshot_path,
        .group_id = group_id,
    });
}

pub fn markRestorePrimaryRestoredForPathWithIdentityWithIo(
    alloc: Allocator,
    io: Io,
    path: []const u8,
    identity: RestoreIdentity,
) !void {
    if (comptime storage_engine.enabled) return storage_engine.call(@src().fn_name, .{ alloc, io, path, identity });
    var state = try restoreStateAlloc(
        alloc,
        identity.backup_id,
        identity.location,
        identity.artifact_sha256,
        identity.native_manifest_size_bytes,
        identity.native_manifest_sha256,
        identity.snapshot_path,
        identity.group_id,
        "runtime_repair",
        true,
        false,
        "",
        "",
    );
    defer state.deinit(alloc);
    try writeRestoreStateForPathWithIo(alloc, io, path, state);
    const import_marker_path = try restoreImportMarkerPathAlloc(alloc, path);
    defer alloc.free(import_marker_path);
    try deleteFileIfExists(io, import_marker_path);
}

pub fn restoreRuntimeRepairNeededForPath(alloc: Allocator, path: []const u8) !bool {
    if (comptime storage_engine.enabled) return storage_engine.call(@src().fn_name, .{ alloc, path });
    var io_impl = threadedIo();
    defer io_impl.deinit();
    return try restoreRuntimeRepairNeededForPathWithIo(alloc, io_impl.io(), path);
}

pub fn restoreRuntimeRepairNeededForPathWithIo(alloc: Allocator, io: Io, path: []const u8) !bool {
    if (comptime storage_engine.enabled) return storage_engine.call(@src().fn_name, .{ alloc, io, path });
    var state = (try readRestoreStateForPathAllocWithIo(alloc, io, path)) orelse return false;
    defer state.deinit(alloc);
    return state.primary_restored and !state.runtime_repair_complete;
}

pub fn markRestoreRuntimeRepairNeeded(alloc: Allocator, path: []const u8) !void {
    var io_impl = threadedIo();
    defer io_impl.deinit();
    return try markRestoreRuntimeRepairNeededWithIo(alloc, io_impl.io(), path);
}

pub fn markRestoreRuntimeRepairNeededWithIo(alloc: Allocator, io: Io, path: []const u8) !void {
    const import_marker_path = try restoreImportMarkerPathAlloc(alloc, path);
    defer alloc.free(import_marker_path);

    var state = (try readRestoreStateForPathAllocWithIo(alloc, io, path)) orelse return error.InvalidRestoreState;
    defer state.deinit(alloc);
    const new_phase = try alloc.dupe(u8, "runtime_repair");
    alloc.free(state.phase);
    state.phase = new_phase;
    state.primary_restored = true;
    state.runtime_repair_complete = false;
    const empty_cursor = try alloc.dupe(u8, "");
    alloc.free(state.graph_ownership_cursor);
    state.graph_ownership_cursor = empty_cursor;
    try writeRestoreStateForPathWithIo(alloc, io, path, state);
    try deleteFileIfExists(io, import_marker_path);
}

pub fn markRestoreRuntimeRepairCompleteWithIo(alloc: Allocator, io: Io, path: []const u8) !void {
    var state = (try readRestoreStateForPathAllocWithIo(alloc, io, path)) orelse return error.InvalidRestoreState;
    defer state.deinit(alloc);
    const new_phase = try alloc.dupe(u8, "complete");
    alloc.free(state.phase);
    state.phase = new_phase;
    state.primary_restored = true;
    state.runtime_repair_complete = true;
    const empty_cursor = try alloc.dupe(u8, "");
    alloc.free(state.graph_ownership_cursor);
    state.graph_ownership_cursor = empty_cursor;
    try writeRestoreStateForPathWithIo(alloc, io, path, state);
}

pub fn restoreRuntimeRepairNeeded(self: *DB) !bool {
    if (comptime storage_engine.enabled) return storage_engine.call(@src().fn_name, .{self});
    if (self.backend_runtime.filesystemIo()) |io| {
        return try restoreRuntimeRepairNeededForPathWithIo(self.alloc, io, self.core.path);
    }
    return try restoreRuntimeRepairNeededForPath(self.alloc, self.core.path);
}

/// Restore's first repair owner can prove a generation only while its
/// index objects remain open. Publication needs the stronger proof that a
/// fresh owner can load the files it left behind. When that reopen
/// rediscovers dense generation or watermark debt, reset the durable
/// restore state so the fresh owner reconstructs the candidate before it
/// can be sealed.
pub fn prepareRestoreDurabilityRetryIfNeeded(self: *DB, alloc: Allocator) !bool {
    if (comptime storage_engine.enabled) return storage_engine.call(@src().fn_name, .{ self, alloc });
    if (self.core.index_manager.hasLoadFailures()) return error.RestoreIndexLoadIncomplete;
    const retry_needed = try self.hasPendingDenseArtifactRebuild(alloc) or
        try self.denseArtifactWatermarkRepairNeeded(alloc) or
        self.core.index_manager.hasRepairUnavailableIndexes();
    if (!retry_needed) return false;
    if (self.backend_runtime.io()) |io| {
        try markRestoreRuntimeRepairNeededWithIo(alloc, io, self.core.path);
    } else {
        try markRestoreRuntimeRepairNeeded(alloc, self.core.path);
    }
    return true;
}

pub fn updateRestoreRuntimeRepairPhaseWithIo(
    self: *DB,
    alloc: Allocator,
    io: Io,
    phase: []const u8,
    complete: bool,
) !void {
    var state = (try readRestoreStateForPathAllocWithIo(alloc, io, self.core.path)) orelse return error.InvalidRestoreState;
    defer state.deinit(alloc);
    const new_phase = try alloc.dupe(u8, phase);
    alloc.free(state.phase);
    state.phase = new_phase;
    state.primary_restored = true;
    state.runtime_repair_complete = complete;
    const empty_cursor = try alloc.dupe(u8, "");
    alloc.free(state.graph_ownership_cursor);
    state.graph_ownership_cursor = empty_cursor;
    const new_last_error = try alloc.dupe(u8, "");
    alloc.free(state.last_error);
    state.last_error = new_last_error;
    try writeRestoreStateForPathWithIo(alloc, io, self.core.path, state);
}

pub fn repairRestoreRuntimeStateStepIfNeeded(self: *DB, alloc: Allocator) !bool {
    if (comptime storage_engine.enabled) return storage_engine.call(@src().fn_name, .{ self, alloc });
    if (self.backend_runtime.filesystemIo()) |io| {
        return try self.repairRestoreRuntimeStateStepIfNeededWithIo(alloc, io);
    }
    var io_impl = threadedIo();
    defer io_impl.deinit();
    return try self.repairRestoreRuntimeStateStepIfNeededWithIo(alloc, io_impl.io());
}

pub fn repairRestoreRuntimeStateStepIfNeededWithIo(self: *DB, alloc: Allocator, io: Io) !bool {
    return try self.repairRestoreRuntimeStateStepIfNeededWithIoAndRepairOptions(alloc, io, .{});
}

pub fn repairRestoreRuntimeStateStepIfNeededWithIoAndRepairOptions(
    self: *DB,
    alloc: Allocator,
    io: Io,
    repair_options: types.ArtifactRepairRunOptions,
) !bool {
    if (comptime storage_engine.enabled) return storage_engine.call(@src().fn_name, .{ self, alloc, io, repair_options });
    var state = (try readRestoreStateForPathAllocWithIo(alloc, io, self.core.path)) orelse return false;
    defer state.deinit(alloc);
    if (!state.primary_restored or state.runtime_repair_complete) return false;
    const phase = state.phase;

    if (std.mem.eql(u8, phase, "repair_indexes")) {
        std.log.info("restore runtime repair phase=repair_indexes", .{});
        if (!try self.repairNativeRestoreProjectionIntentsStep(alloc, repair_options))
            return error.RestoreRuntimeRepairIncomplete;
        try self.updateRestoreRuntimeRepairPhaseWithIo(alloc, io, "complete", true);
        return true;
    }

    if (std.mem.eql(u8, phase, "runtime_repair") or std.mem.eql(u8, phase, "reset_watermarks")) {
        std.log.info("restore runtime repair phase=reset_index_watermarks", .{});
        try self.resetManagedIndexAppliedSequencesForRestoreRepair(alloc);
        try self.refreshManagedIndexWorkersLocked();
        try self.updateRestoreRuntimeRepairPhaseWithIo(alloc, io, "rebuild_graph", false);
        return true;
    }
    if (std.mem.eql(u8, phase, "rebuild_graph")) {
        std.log.info("restore runtime repair phase=rebuild_graph_ownership", .{});
        const cursor = if (state.graph_ownership_cursor.len > 0)
            try hexToBytesAlloc(alloc, state.graph_ownership_cursor)
        else
            null;
        defer if (cursor) |key| alloc.free(key);
        var ownership_page = try self.rebuildGraphArtifactOwnershipForRestorePage(alloc, cursor);
        defer ownership_page.deinit(alloc);
        if (!ownership_page.complete) {
            try self.updateRestoreGraphOwnershipCursorWithIo(
                alloc,
                io,
                ownership_page.next_cursor.?,
            );
            std.log.info("restore runtime repair rebuilt graph ownership page sources={d}", .{ownership_page.replayed});
            return true;
        }
        const graph_count = try self.rebuildGraphDerivedState();
        std.log.info("restore runtime repair rebuilt final graph ownership page sources={d} mutations={d}", .{ ownership_page.replayed, graph_count });
        try self.updateRestoreRuntimeRepairPhaseWithIo(alloc, io, "rebuild_artifacts", false);
        return true;
    }
    if (std.mem.eql(u8, phase, "rebuild_artifacts")) {
        std.log.info("restore runtime repair rebuild stored embedding artifacts path={s}", .{self.core.path});
        const dense_rebuild = try self.rebuildDenseIndexesFromStoredEmbeddingArtifactsOutcomeWithProgress(alloc, null, null);
        if (dense_rebuild.dense_visibility_changed) self.clearDenseHbcCaches();
        std.log.info("restore runtime repair rebuilt stored embedding artifacts path={s} count={d}", .{ self.core.path, dense_rebuild.reported_work });
        try self.updateRestoreRuntimeRepairPhaseWithIo(alloc, io, "replay_enrichments", false);
        return true;
    }
    if (std.mem.eql(u8, phase, "replay_enrichments")) {
        if (self.core.hasGeneratedEnrichmentTargets()) {
            const runtime = self.enrichment_runtime orelse return error.RestoreRepairRequiresEnrichmentRuntime;
            std.log.info("restore runtime repair replay generated enrichments path={s}", .{self.core.path});
            // A restored enrichment checkpoint can be equal to the first
            // destination replay sequence even when the destination has
            // no generated artifacts. Quiesce the worker before resetting
            // and appending so it cannot advance the checkpoint across an
            // empty replay window between those two operations.
            runtime.stop();
            errdefer if (self.local_execution.optional_runtime_workers_enabled) {
                runtime.start() catch |start_err| {
                    std.log.warn("restore runtime repair enrichment restart failed path={s} err={}", .{ self.core.path, start_err });
                };
            };
            try runtime.resumeFrom(0, self.core.nextDerivedSequence());
            const generated_refs = try self.replayGeneratedEnrichmentsFromStoredDocs(alloc);
            // Restore opens with optional workers disabled and drains them
            // synchronously. Starting a background worker here races the
            // foreground catch-up in the next phase over index mutation.
            if (self.local_execution.optional_runtime_workers_enabled)
                try runtime.start()
            else
                try runtime.resumeInlineAfterStop();
            std.log.info("restore runtime repair replayed generated enrichments path={s} refs={d}", .{ self.core.path, generated_refs });
        }
        try self.updateRestoreRuntimeRepairPhaseWithIo(alloc, io, "drain_async", false);
        return true;
    }
    if (std.mem.eql(u8, phase, "drain_async")) {
        std.log.info("restore runtime repair phase=drain_async", .{});
        if (self.core.hasGeneratedEnrichmentTargets()) {
            try self.runRestoreRepairDrainAsync();
            // This is the only asynchronous restore phase that can apply
            // newly generated dense vectors. Retire caches at that exact
            // mutation boundary instead of after every restore quantum.
            self.clearDenseHbcCaches();
        } else {
            std.log.info("restore runtime repair phase=skip_async_drain", .{});
        }
        try self.updateRestoreRuntimeRepairPhaseWithIo(alloc, io, "rebuild_replayed_artifacts", false);
        return true;
    }
    if (std.mem.eql(u8, phase, "rebuild_replayed_artifacts")) {
        std.log.info("restore runtime repair phase=rebuild_replayed_embeddings", .{});
        const dense_rebuild = try self.rebuildDenseIndexesFromStoredEmbeddingArtifactsOutcomeWithProgress(alloc, null, null);
        if (dense_rebuild.dense_visibility_changed) self.clearDenseHbcCaches();
        const coverage_repair = try self.repairRestoreDenseArtifactCoverageFromFinalArtifacts(alloc);
        if (coverage_repair.waiting_for_workers) {
            if (dense_rebuild.made_progress or coverage_repair.made_progress) return true;
            return error.RestoreRuntimeRepairIncomplete;
        }
        _ = try self.rebuildSparseIndexesForTargetCoverage(alloc);
        try self.updateRestoreRuntimeRepairPhaseWithIo(alloc, io, "sync_indexes", false);
        return true;
    }
    if (std.mem.eql(u8, phase, "sync_indexes")) {
        std.log.info("restore runtime repair phase=complete_indexes", .{});
        // Re-evaluate after every replay/rebuild phase. Those phases can
        // advance the durable replay target without rebuilding vectors,
        // leaving a completed artifact projection with an older applied
        // watermark or a rebuilding checkpoint. A restore must not publish
        // its completion marker until both forms of debt converge.
        // Resolve durable restore-owned intents first. In particular, a
        // missing-counter intent is itself reported as rebuild debt, so
        // checking the plan before bootstrapping its counter would keep
        // rediscovering the same intent forever.
        const artifact_completion = try self.completeRestoreDenseArtifactRepairs(alloc);
        if (artifact_completion == .progressed) return true;

        const dense_rebuild = try self.rebuildDenseIndexesFromStoredEmbeddingArtifactsOutcomeWithProgress(alloc, null, null);
        if (dense_rebuild.dense_visibility_changed) self.clearDenseHbcCaches();
        // Portable restores can legitimately rediscover a non-artifact
        // projection intent here. In particular, opening a restored v1
        // dense index under a v2-capable catalog creates a durable storage
        // migration intent only after the artifact repair pass has
        // established complete source coverage. The staged DB is not yet
        // published and therefore has no managed scheduler owner which can
        // advance that intent. Drive one bounded quantum from the restore
        // owner itself, using the same cancellation and capacity fences as
        // native selective restore repair. Otherwise sync_indexes waits
        // forever for an external owner that cannot open the staged root.
        if (!try self.repairNativeRestoreProjectionIntentsStep(alloc, repair_options)) {
            return error.RestoreRuntimeRepairIncomplete;
        }
        if (try self.hasPendingDenseArtifactRebuild(alloc) or
            try self.denseArtifactWatermarkRepairNeeded(alloc))
        {
            // A successful bounded rebuild is real visibility progress.
            // Publish/invalidate it before a later proof-only retry.
            if (dense_rebuild.made_progress) return true;
            return error.RestoreDenseArtifactRebuildIncomplete;
        }
        if (self.core.index_manager.hasRepairUnavailableIndexes()) {
            if (dense_rebuild.made_progress or artifact_completion == .completed_with_progress) return true;
            return error.RestoreIndexAvailabilityIncomplete;
        }
        try self.saveAllLiveIndexStatusSnapshots(alloc);
        try self.core.index_manager.syncAll(true);
        try self.core.syncStore(true);
        try markRestoreRuntimeRepairCompleteWithIo(alloc, io, self.core.path);
        std.log.info("restore runtime repair phase=complete", .{});
        return true;
    }
    return error.InvalidRestoreState;
}

pub fn repairRestoreRuntimeStateIfNeeded(self: *DB, alloc: Allocator) !bool {
    if (self.backend_runtime.filesystemIo()) |io| {
        return try self.repairRestoreRuntimeStateIfNeededWithIo(alloc, io);
    }
    var io_impl = threadedIo();
    defer io_impl.deinit();
    return try self.repairRestoreRuntimeStateIfNeededWithIo(alloc, io_impl.io());
}

pub fn repairRestoreRuntimeStateIfNeededWithIo(self: *DB, alloc: Allocator, io: Io) !bool {
    var repaired = false;
    while (try self.repairRestoreRuntimeStateStepIfNeededWithIo(alloc, io)) {
        repaired = true;
    }
    return repaired;
}

pub fn restoreResolverIdentityWithoutDrain(self: *DB, name: []const u8, cfg: ?index_manager_mod.ResolverConfig) !void {
    if (openModeRequiresReadOnlyBackends(self.open_mode)) return error.ReadOnly;
    var write_fence = try self.beginReplicatedWrite(.gate);
    defer write_fence.release();
    var activity = try self.acquireResolverCatalogActivity(true);
    defer activity.deinit();
    try self.lockApplyForPortableRuntime();
    defer self.core.unlockApply();
    var probe = try self.core.store.beginProbeTxn();
    defer probe.abort();
    try self.requireOnlineArtifactCatalogMutableLocked(&probe);
    if (cfg) |previous| {
        try self.core.index_manager.restoreResolverIdentity(self.core.store, previous);
    } else {
        _ = try self.core.index_manager.removeResolver(self.core.store, name);
    }
    if (self.resolution_runtime) |runtime| runtime.notifyBackfill();
}

pub fn resetManagedIndexAppliedSequencesForRestoreRepair(self: *DB, alloc: Allocator) !void {
    const managed_indexes = try self.core.managedIndexes(alloc);
    defer {
        for (managed_indexes) |index_ref| alloc.free(@constCast(index_ref.name));
        alloc.free(managed_indexes);
    }

    for (managed_indexes) |index_ref| {
        const sequence: u64 = switch (index_ref.kind) {
            .full_text => try self.probeDerivedReplayTargetSequence(
                alloc,
                self.core.replaySource(),
                index_ref,
                0,
            ),
            else => 0,
        };
        try self.core.saveAppliedSequence(index_ref.name, sequence);
    }
}

/// Retries a top-level portable catalog activation that failed after the
/// durable generation commit. The restore loader rolls partial runtime
/// state back on error. Recovery is single-flight: callers never queue a
/// second full catalog reload behind one already in progress.
pub fn retryPortableRuntimeActivationIfNeeded(self: *DB) !bool {
    if (!self.async_context.portable_runtime_activation_pending.load(.acquire)) return false;
    if (!self.portable_runtime_activation_mutex.tryLock()) return error.PortableRuntimeActivationPending;
    defer self.portable_runtime_activation_mutex.unlock();
    if (!self.async_context.portable_runtime_activation_pending.load(.acquire)) return false;

    _ = self.portable_runtime_activation_attempts.fetchAdd(1, .monotonic);
    self.loadPortableRuntimeAfterPublication() catch |err| {
        self.async_context.portable_runtime_activation_pending.store(true, .release);
        return err;
    };
    self.async_context.portable_runtime_activation_pending.store(false, .release);
    // Session cleanup intentionally leaves maintenance deferred while the
    // catalog is degraded. Release that deferral only after the restored
    // runtime is fully usable.
    resumeDeferredBackgroundMaintenanceIfIdle(self.async_context);
    self.startQuarantineRetryWorkerIfNeeded();
    std.log.info("portable runtime activation recovered path={s}", .{self.core.path});
    return true;
}

pub fn portableActivationRecoveryPort(self: *DB) portable_activation_recovery.Owner.Port {
    return .{
        .ptr = self,
        .path = self.core.path,
        .runtime = self.backend_runtime,
        .owner_id = self.backend_owner_id,
        .pending = &self.async_context.portable_runtime_activation_pending,
        .retry = retryPortableActivationFromOwner,
        .before_start = if (builtin.is_test) portableActivationBeforeStartForTest else null,
        .refuse_launch = if (builtin.is_test) portableActivationRefuseLaunchForTest else null,
    };
}

pub fn retryPortableActivationFromOwner(ptr: *anyopaque) !bool {
    const self: *DB = @ptrCast(@alignCast(ptr));
    return self.retryPortableRuntimeActivationIfNeeded();
}

pub fn portableActivationBeforeStartForTest(ptr: *anyopaque) void {
    const self: *DB = @ptrCast(@alignCast(ptr));
    if (db_zig.test_pause_portable_activation_retry_probe_before_lifecycle_lock.load(.acquire)) {
        db_zig.test_portable_activation_retry_probe_paused.store(true, .release);
        const io = self.backend_runtime.io() orelse std.Options.debug_io;
        while (!db_zig.test_release_portable_activation_retry_probe.load(.acquire))
            io.sleep(.fromMilliseconds(1), .awake) catch {};
    }
}

pub fn portableActivationRefuseLaunchForTest(_: *anyopaque) bool {
    return db_zig.test_fail_portable_activation_retry_fallback_submit.swap(false, .acq_rel);
}

pub fn startPortableActivationRetryWorkerIfNeeded(self: *DB) void {
    self.startPortableActivationRetryWorkerIfNeededInner(false);
}

pub fn startPortableActivationRetryWorkerIfNeededInner(self: *DB, allow_test_background: bool) void {
    if (comptime builtin.single_threaded or builtin.os.tag == .freestanding) return;
    if (comptime builtin.is_test) {
        if (!allow_test_background) return;
    }
    self.portable_activation_recovery.start(self.portableActivationRecoveryPort());
}

pub fn launchPortableActivationRetryWorkerLocked(self: *DB) !portable_activation_recovery.Owner.PortableActivationRetryLaunch {
    self.portable_activation_recovery.port = self.portableActivationRecoveryPort();
    self.portable_activation_recovery.bound = true;
    return self.portable_activation_recovery.launchPortableActivationRetryWorkerLocked();
}

pub fn stopPortableActivationRetryWorker(self: *DB) void {
    self.portable_activation_recovery.stop(self.portableActivationRecoveryPort());
}

pub fn runRestoreRepairDrainAsync(self: *DB) !void {
    // Generated enrichment replay is asynchronous with respect to the
    // replay append performed by the preceding repair phase. The restore
    // completion marker is also the query-readiness barrier, so drain all
    // replay stages to a stable sequence before publishing it. Keep
    // posting, text-merge, artifact-metadata, and LSM maintenance outside
    // this barrier; those can continue in the background without hiding
    // logically restored query results.
    if (self.enrichment_runtime) |runtime| {
        const before = runtime.stats();
        std.log.info("restore runtime repair enrichment drain begin path={s} applied={d} target={d} processed={d} skipped={d}", .{
            self.core.path,
            before.applied_sequence,
            before.target_sequence,
            before.processed_requests,
            before.skip_by_hash_count,
        });
    }
    // Do not truncate the replay journal here: subsequent restore repair
    // phases still use its durable target to prove derived-state coverage.
    try self.drainReplayStagesUntilStableWithOptions(.{
        .truncate_replay = false,
        .wait_for_enrichment_retries = true,
    });
    if (self.enrichment_runtime) |runtime| {
        const after = runtime.stats();
        std.log.info("restore runtime repair enrichment drain complete path={s} applied={d} target={d} processed={d} skipped={d} errors={d}", .{
            self.core.path,
            after.applied_sequence,
            after.target_sequence,
            after.processed_requests,
            after.skip_by_hash_count,
            after.error_count,
        });
    }
    try self.flushAppliedSequencesForIdle();
}

pub fn enrichmentThroughputSnapshot(self: *DB) EnrichmentThroughputSnapshot {
    const runtime = self.enrichment_runtime orelse return .{};
    return .{
        .embed_batches = runtime.embed_batches_completed,
        .embed_items = runtime.embed_items_completed,
        .embed_ns = runtime.total_embed_ns,
        .extract_batches = runtime.extract_batches_completed,
        .extract_items = runtime.extract_items_completed,
        .extract_ns = runtime.total_extract_ns,
    };
}

/// Project exact resident query admission. Presence alone is insufficient
/// because managed admission installs a gated empty generation before its
/// first safe snapshot; cardinality is insufficient because a published
/// empty snapshot is valid and searchable.
pub fn vectorServingSnapshotReady(
    self: *DB,
    alloc: Allocator,
    item: *const types.DBIndexStats,
    preloaded_repair_state: ?*const index_repair_state.State,
) bool {
    const installed = switch (item.kind) {
        .dense_vector => self.core.denseIndex(item.name) != null,
        .sparse_vector => self.core.sparseIndex(item.name) != null,
        else => false,
    };
    const admission = self.observeResidentIndexAdmission(alloc, item.name, preloaded_repair_state);
    if (!installed or admission != .admitted) return false;
    if (item.kind == .dense_vector) {
        const entry = self.core.denseIndex(item.name) orelse return false;
        if (item.projection_checkpoint_published_count) |certified| {
            const checkpoint: apply_state.ProjectionCheckpoint = .{
                .applied_sequence = item.projection_checkpoint_applied_sequence,
                .generation = item.projection_checkpoint_generation,
                .config_hash = item.projection_checkpoint_config_hash,
                .published_count = certified,
            };
            if (!entry.hasValidatedServingCertificate(checkpoint) and
                !entry.hasRecoveredNativeServingCertificate(checkpoint))
            {
                return false;
            }
        } else if (item.coverage_produced_count != 0) {
            const checkpoint: apply_state.ProjectionCheckpoint = .{
                .applied_sequence = item.projection_checkpoint_applied_sequence,
                .generation = item.projection_checkpoint_generation,
                .config_hash = item.projection_checkpoint_config_hash,
            };
            if (entry.hasRecoveredNativeServingCertificate(checkpoint)) return true;
            if (self.core.index_manager.get(item.name)) |cfg| {
                // Older checkpoints have no count certificate. For managed
                // progressive indexes use the existing durable coverage
                // proof instead of treating an empty HBC as ready.
                const proof = self.observeProgressiveManagedGenerationQueryabilityAtLeast(
                    alloc,
                    item.name,
                    types.indexConfigHash(cfg.*),
                    0,
                ) catch return false;
                switch (proof) {
                    .queryable, .atomic_publication, .external_coverage => {},
                    else => return false,
                }
            }
        }
    }
    return true;
}

pub fn collectLiveIndexStatusSnapshot(index_manager: *index_manager_mod.IndexManager, index_name: []const u8) ?IndexStatusSnapshot {
    // These bytes enter compressed durable tables, so even a diagnostic
    // timestamp can change disk usage and subsequent placement decisions.
    const now = platform_time.awakeNs(index_manager.checkpointIo());
    if (index_manager.textIndex(index_name)) |entry| {
        // Applied-sequence persistence runs outside the DB apply lock; keep this
        // snapshot cheap and avoid walking full-text segment internals here.
        const text_snapshot = entry.acquireSnapshot();
        defer text_snapshot.release();
        return .{
            .kind = .full_text,
            .doc_count = text_snapshot.liveDocCount(),
            .updated_at_ns = now,
        };
    }
    if (index_manager.denseIndex(index_name)) |entry| {
        const dense_stats = denseServingCounts(entry.index);
        return .{
            .kind = .dense_vector,
            .doc_count = dense_stats.doc_count,
            .node_count = dense_stats.node_count,
            .root_node = dense_stats.root_node,
            .updated_at_ns = now,
        };
    }
    if (index_manager.sparseIndex(index_name)) |entry| {
        const sparse_stats = entry.index.stats();
        return .{
            .kind = .sparse_vector,
            .doc_count = sparse_stats.doc_count,
            .term_count = sparse_stats.term_count,
            .updated_at_ns = now,
        };
    }
    if (index_manager.graphIndex(index_name)) |entry| {
        const graph_stats = entry.index.operationalStats();
        return .{
            .kind = .graph,
            .doc_count = graph_stats.node_count,
            .edge_count = graph_stats.edge_count,
            .graph_counts_pending = graph_stats.counts_pending,
            .node_count = graph_stats.node_count,
            .updated_at_ns = now,
        };
    }
    return null;
}

pub fn textSnapshotTermCount(snap: anytype) u64 {
    var terms: u64 = 0;
    for (snap.segments) |*seg| {
        terms +|= seg.layoutStats(false).inverted_term_count;
    }
    return terms;
}

pub fn saveIndexStatusSnapshots(
    alloc: Allocator,
    store: *docstore_mod.DocStore,
    index_manager: *index_manager_mod.IndexManager,
    updates: []const apply_state.AppliedSequenceUpdate,
) !void {
    if (updates.len == 0) return;

    const PendingStatusWrite = struct {
        key: []u8,
        value: [index_status_encoded_len]u8,
    };
    var pending = std.ArrayListUnmanaged(PendingStatusWrite).empty;
    defer {
        for (pending.items) |item| alloc.free(item.key);
        pending.deinit(alloc);
    }

    for (updates) |update| {
        const status_snapshot = collectLiveIndexStatusSnapshot(index_manager, update.index_name) orelse continue;
        const key = try indexStatusKeyAlloc(alloc, update.index_name);
        var encoded: [index_status_encoded_len]u8 = undefined;
        encodeIndexStatusSnapshot(status_snapshot, &encoded);
        errdefer alloc.free(key);
        try pending.append(alloc, .{
            .key = key,
            .value = encoded,
        });
    }

    if (pending.items.len == 0) return;
    var status_batch = try store.beginWriteBatch();
    errdefer status_batch.abort();
    for (pending.items) |item| try status_batch.put(item.key, &item.value);
    try status_batch.commit();
}

pub fn saveAllLiveIndexStatusSnapshots(self: *DB, alloc: Allocator) !void {
    const configs = try self.core.listIndexes(alloc);
    defer types.freeIndexConfigs(alloc, configs);
    if (configs.len == 0) return;

    var updates = std.ArrayListUnmanaged(apply_state.AppliedSequenceUpdate).empty;
    defer updates.deinit(alloc);
    for (configs) |cfg| {
        try updates.append(alloc, .{
            .index_name = cfg.name,
            .sequence = try self.core.loadAppliedSequence(alloc, cfg.name),
        });
    }
    try saveIndexStatusSnapshots(alloc, self.core.store, self.core.index_manager, updates.items);
}

pub fn loadIndexStatusSnapshot(self: *DB, alloc: Allocator, index_name: []const u8) !?IndexStatusSnapshot {
    const key = try indexStatusKeyAlloc(alloc, index_name);
    defer alloc.free(key);
    const raw = self.core.store.get(alloc, key) catch |err| switch (err) {
        error.NotFound => return null,
        else => return err,
    };
    defer alloc.free(raw);
    return decodeIndexStatusSnapshot(raw) catch null;
}

/// Apply lifecycle facts only when the caller also owns a consistent
/// artifact/coverage refresh boundary.
pub fn overlayRuntimeStatusLifecycleFromSnapshot(self: *DB, runtime_stats: *types.DBStats) void {
    for (runtime_stats.indexes) |*item| {
        const dense_catch_up = item.kind == .dense_vector and runtime_stats.async_indexing.dense_catch_up.active;
        // This overlay can run over a cached materialization snapshot.
        // Read the live immutable HBC generation's cardinality for the
        // exact-vector certificate instead of pairing a current replay
        // watermark with that snapshot's potentially stale doc_count.
        // The public aggregation layer may later contribute the current
        // count, but it cannot safely reconstruct this generation-local
        // consistency check after the fact.
        const dense_vector_active_count = if (item.kind == .dense_vector)
            if (self.core.denseIndex(item.name)) |entry|
                entry.index.stats().active_count
            else
                item.doc_count
        else
            0;
        if (!dense_catch_up) if (self.executor.appliedSequence(item.name)) |live_applied| {
            item.replay_applied_sequence = @max(item.replay_applied_sequence, live_applied);
            item.replay_target_sequence = @max(item.replay_target_sequence, live_applied);
        };
        item.catch_up_active = dense_catch_up;
        if (item.kind == .dense_vector) {
            const progress = runtime_stats.async_indexing.dense_catch_up;
            if (dense_catch_up) {
                if (progress.current_sequence != 0) {
                    item.replay_applied_sequence = if (item.replay_applied_sequence == 0)
                        progress.current_sequence
                    else
                        @min(item.replay_applied_sequence, progress.current_sequence);
                    item.catch_up_applied_sequence = progress.current_sequence;
                } else {
                    item.catch_up_applied_sequence = item.replay_applied_sequence;
                }
                item.catch_up_target_sequence = @max(item.replay_target_sequence, progress.current_target_sequence);
                item.replay_target_sequence = item.catch_up_target_sequence;
                item.replay_catch_up_required = true;
                item.backfill_active = true;
            } else {
                item.replay_catch_up_required = item.replay_applied_sequence < item.replay_target_sequence;
                item.catch_up_applied_sequence = item.replay_applied_sequence;
                item.catch_up_target_sequence = item.replay_target_sequence;
            }
            item.catch_up_phase = progress.phase;
        } else {
            item.replay_catch_up_required = item.replay_applied_sequence < item.replay_target_sequence;
            item.catch_up_applied_sequence = item.replay_applied_sequence;
            item.catch_up_target_sequence = item.replay_target_sequence;
            item.catch_up_phase = .idle;
        }
        if (item.catch_up_active or item.replay_catch_up_required) {
            item.backfill_active = true;
            if (item.replay_target_sequence > 0) {
                item.backfill_progress = @min(
                    1.0,
                    @as(f64, @floatFromInt(item.replay_applied_sequence)) /
                        @as(f64, @floatFromInt(item.replay_target_sequence)),
                );
            }
        } else {
            item.backfill_active = durableGenerationBuildActive(item);
            if (!item.backfill_active and item.replay_target_sequence > 0) item.backfill_progress = 1.0;
        }
        normalizeReplayStatusFromDurableCheckpoint(item);
        if (item.kind == .dense_vector and
            self.core.index_manager.vectorBlockProjectionRequiredForDenseIndex(item.name) and
            !self.core.index_manager.vectorBlockReadyForDenseIndexAtSequence(
                item.name,
                item.replay_applied_sequence,
                dense_vector_active_count,
            ))
        {
            // When the native exact-vector projection is enabled it is
            // part of query readiness, not best-effort maintenance. Apply
            // this after replay normalization so it cannot be overwritten
            // by an otherwise-clean durable generation.
            item.backfill_active = true;
            item.backfill_progress = @min(item.backfill_progress, 0.999);
            item.dense_vector_projection_pending = true;
        }
        for (item.source_replay) |*source| {
            source.published_sequence = @max(source.published_sequence, item.replay_applied_sequence);
            source.target_sequence = @max(source.target_sequence, source.published_sequence);
        }
        item.checkpoint_replay_tail_sequence_count = item.replay_target_sequence -| item.projection_checkpoint_applied_sequence;
    }
}

/// A contended apply lock skips the live per-index refresh below, but the
/// durable per-index status snapshot (`saveIndexStatusSnapshots`) is
/// written on every applied update as part of the ordinary replay/apply
/// commit path, independent of this lock. Read it as a lock-free floor so
/// a sustained backfill or schema migration, which keeps the writer-
/// preferring apply lock busy, does not leave doc_count frozen at
/// whatever the last successful live sample happened to observe. Only
/// raise the value: a durable snapshot can itself lag a live sample that
/// did succeed, and this is strictly a fallback for when the live sample
/// could not run at all.
pub fn refreshIndexDocCountsFromDurableSnapshotBestEffort(self: *DB, alloc: Allocator, runtime_stats: *types.DBStats) void {
    for (runtime_stats.indexes) |*item| {
        const status_snapshot = (self.loadIndexStatusSnapshot(alloc, item.name) catch continue) orelse continue;
        if (status_snapshot.kind != item.kind) continue;
        if (status_snapshot.doc_count > item.doc_count) item.doc_count = status_snapshot.doc_count;
    }
}

/// A table restore reassigns canonical document identities after native
/// validation creates selective repair debt. That debt is still unstarted
/// and belongs to this unpublished candidate, so bind fresh replica-local
/// repair identities to the destination group before running any quantum.
/// Calling again after a crash between namespace publication and this
/// checkpoint write completes the same transition without losing debt.
pub fn rebindUnstartedNativeRestoreRepairIdentity(
    self: *DB,
    staged_generation: *const generation_lifecycle.StagedGeneration,
) !void {
    try staged_generation.validatePath(self.core.path);
    if (self.open_mode != .writer_no_replay) return error.InvalidNativeRestoreOpenMode;
    const location = try self.indexRepairStateLocation();
    var state = index_repair_state.loadAt(self.alloc, location) catch |err| switch (err) {
        error.FileNotFound => return,
        else => return err,
    };
    defer state.deinit(self.alloc);
    const group_id = self.localRepairGroupId();
    const needs_rebind = for (state.entries.items) |entry| {
        if (entry.intent.group_id != group_id) break true;
    } else false;
    if (!needs_rebind) return;
    var restore = (try readRestoreStateForPathAlloc(self.alloc, self.core.path)) orelse
        return error.InvalidRestoreImportMarker;
    defer restore.deinit(self.alloc);
    if (!restore.primary_restored or !std.mem.eql(u8, restore.phase, "repair_indexes"))
        return error.InvalidRestoreImportMarker;
    const intents = try self.alloc.alloc(index_repair_state.IndexRepairIntent, state.entries.items.len);
    defer self.alloc.free(intents);
    for (state.entries.items, intents) |entry, *intent| {
        if (entry.intent.trigger != .projection_generation_invalid or
            entry.intent.phase != .detected or entry.intent.owner_epoch != 0 or
            entry.intent.source_replay_state != .not_required or
            entry.intent.candidate_relative_path != null or entry.pin != null or
            entry.intent.root_generation != self.core.root_generation)
            return error.InvalidIndexRepairTransition;
        intent.* = entry.intent;
        intent.group_id = group_id;
    }
    var rebound = try index_repair_state.resetForRootGenerationWithIntentsAt(
        self.alloc,
        location,
        state.identity,
        self.core.root_generation,
        intents,
    );
    rebound.deinit(self.alloc);
}

/// Returns whether every durable catalog that participates in a portable
/// restore is pristine. Keep this predicate in DB so CLI, C ABI, and
/// embedded callers cannot drift into weaker definitions of "empty".
pub fn isPortableImportTargetEmpty(self: *DB, alloc: Allocator) !bool {
    if (self.open_mode == .status_only) {
        return try self.portableImportTargetEmptyLocked(alloc);
    }
    lockApplyShared(self);
    defer self.core.unlockApplyShared();
    return try self.portableImportTargetEmptyLocked(alloc);
}

pub fn portableImportRecoveryRequired(self: *const DB) bool {
    return self.core.store.portableImportRecoveryRequired();
}

/// Allocates every schema-dependent runtime object before a durable
/// generation swap. Applying this object is deliberately infallible.
pub fn preparePortableRuntimeMetadata(
    self: *DB,
    source_store: *docstore_mod.DocStore,
    target_identity: doc_identity.Namespace,
) !PreparedPortableRuntimeMetadata {
    var row_policy_lease = try self.local_execution.row_policy_gate.enterRaw();
    errdefer row_policy_lease.release();
    const identity_summary = try doc_identity.visibilitySummaryFromStore(source_store);
    const restored_schema = try schema_mod.loadSchema(source_store, self.alloc);
    var restored_schema_owned = true;
    defer if (restored_schema_owned) if (restored_schema) |schema| schema_mod.freeSchema(self.alloc, schema);

    var prepared_epoch: ?*schema_registry_mod.Epoch = null;
    if (restored_schema) |runtime_schema| {
        var schema_json: ?[]u8 = null;
        defer if (schema_json) |value| self.alloc.free(value);
        if (runtime_schema.storage_mode == .relational) {
            schema_json = try missingAsNull(source_store.get(self.alloc, public_schema_json_key));
        }
        prepared_epoch = try self.core.prepareSchemaEpoch(runtime_schema, schema_json);
    }
    var prepared_epoch_owned = prepared_epoch != null;
    defer if (prepared_epoch_owned) prepared_epoch.?.release();
    var schema_replacement = try self.core.prepareSchemaRegistryReplacement(prepared_epoch);
    prepared_epoch_owned = false;
    errdefer schema_replacement.deinit();
    var restored_indexes = try relational_index_catalog.Controller.loadSnapshot(
        self.alloc,
        source_store,
        if (prepared_epoch) |epoch| .{ .epoch = epoch } else null,
    );
    errdefer if (restored_indexes) |*indexes| indexes.deinit();
    // Historical layouts were validated while importing the unpublished
    // image and remain durable in the restored store. Match normal startup:
    // publish only the active epoch and fault historical versions into the
    // registry when a row first references them. This keeps publication
    // latency and resident memory independent of schema-history length.
    const restored_catalog = try db_core.loadTableCatalogForSchema(self.alloc, source_store, restored_schema);
    // Portable publication replaces the owner catalog without replaying
    // policy commands. Until the staged image carries a verified compiled
    // policy epoch and an authenticated principal binding, importing an
    // active policy (or replacing one) would create an unguarded window.
    if (restored_catalog.row_policy_phase != .disabled or self.local_execution.row_policy_gate.currentPhase() != .disabled)
        return error.RowPolicyAuthenticationRequired;
    var prepared_recovery = if (self.transaction_recovery_identity_context != null)
        try db_core.PreparedRecoveryRelationalState.init(
            self.runtime_alloc,
            if (restored_schema) |schema| if (schema.storage_mode == .relational) schema.relational_columns else null else null,
            if (restored_schema) |schema| if (schema.storage_mode == .relational) schema.version else 0 else 0,
        )
    else
        null;
    defer if (prepared_recovery) |*prepared| prepared.deinit();

    const result = PreparedPortableRuntimeMetadata{
        .alloc = self.alloc,
        .runtime_alloc = self.runtime_alloc,
        .identity_summary = identity_summary,
        .schema = restored_schema,
        .schema_replacement = schema_replacement,
        .relational_indexes = restored_indexes,
        .table_catalog = restored_catalog,
        .row_policy_lease = row_policy_lease,
        .recovery = prepared_recovery,
        .target_identity = target_identity,
    };
    restored_schema_owned = false;
    prepared_recovery = null;
    return result;
}

pub fn applyPreparedPortableRuntimeMetadataLocked(self: *DB, prepared: *PreparedPortableRuntimeMetadata) void {
    std.debug.assert(prepared.alloc.ptr == self.alloc.ptr and prepared.alloc.vtable == self.alloc.vtable);
    std.debug.assert(prepared.runtime_alloc.ptr == self.runtime_alloc.ptr and prepared.runtime_alloc.vtable == self.runtime_alloc.vtable);
    self.clearBulkIngestIdentityAllNewLocked();
    self.core.identity_visibility.summary = prepared.identity_summary;
    self.clearLiveDocSetCache();
    self.clearNonVisibleDocSetCache();
    self.core.replaceSchemaOwnedPrepared(prepared.schema, &prepared.schema_replacement, &prepared.relational_indexes);
    prepared.schema = null;
    self.core.table_catalog = prepared.table_catalog;
    self.core.identity_namespace = prepared.target_identity;
    if (self.transaction_recovery_identity_context) |ctx| ctx.updateIdentityNamespace(prepared.target_identity);
    self.publishRelationalRuntimeModePrepared(if (prepared.recovery) |*recovery| recovery else null);
}

pub fn loadPortableRuntimeAfterPublicationLocked(self: *DB) !void {
    try self.core.loadIndexesForRestore();
    if (builtin.is_test and db_zig.test_fail_portable_index_activation) {
        db_zig.test_fail_portable_index_activation = false;
        return error.InjectedPortableIndexActivationFailure;
    }
}

/// Rebind the catalog while writes are excluded, but perform blocking
/// worker joins and starts outside that lock. Resolver append callbacks
/// take the apply lock, so joining under it can deadlock shutdown.
pub fn loadPortableRuntimeAfterPublication(self: *DB) !void {
    // Reuse the live-catalog publication barrier so lock-free published
    // dense readers and every structural mutation retire before reset.
    // Acquire it before apply: its runtime drains may themselves need apply.
    var structural_guard = self.beginIndexStructuralMutation("portable runtime activation", "*");
    defer structural_guard.deinit();
    self.stopResolverReplayRuntimes();
    {
        lockApply(self);
        defer self.core.unlockApply();
        try self.loadPortableRuntimeAfterPublicationLocked();
    }
    try self.startResolverReplayRuntimesIfConfigured();
}

pub fn activatePortableIndexesAfterPublication(self: *DB) void {
    lockAtomic(&self.portable_runtime_activation_mutex);
    defer self.portable_runtime_activation_mutex.unlock();
    _ = self.portable_runtime_activation_attempts.fetchAdd(1, .monotonic);
    const activation_result: anyerror!void = self.loadPortableRuntimeAfterPublication();
    activation_result catch |err| {
        self.async_context.portable_runtime_activation_pending.store(true, .release);
        std.log.err("portable generation published but live index activation is degraded path={s} class={s}", .{ self.core.path, @errorName(err) });
        self.startPortableActivationRetryWorkerIfNeeded();
        return;
    };
    self.async_context.portable_runtime_activation_pending.store(false, .release);
    resumeDeferredBackgroundMaintenanceIfIdle(self.async_context);
    self.startQuarantineRetryWorkerIfNeeded();
}

pub fn refreshPortableImportedGenerationLocked(
    self: *DB,
    target_identity: doc_identity.Namespace,
) !void {
    var prepared = try self.preparePortableRuntimeMetadata(self.core.store, target_identity);
    defer prepared.deinit();
    self.applyPreparedPortableRuntimeMetadataLocked(&prepared);
    try self.core.loadIndexesForRestore();
}

/// Imports directly into a disposable, unpublished DB generation. Portable
/// blocks are already bounded to small store transactions, so this avoids
/// both an archive-sized pending transaction and an equally large scratch
/// LSM. On any failure the owner must discard the generation.
pub fn importPortableIntoUnpublishedEmpty(
    self: *DB,
    alloc: Allocator,
    backup: []const u8,
    target_identity: doc_identity.Namespace,
) !void {
    if (self.open_mode == .status_only or self.open_mode == .query_readonly) return error.UnsupportedOperation;

    lockApply(self);
    defer self.core.unlockApply();
    if (!(try self.portableImportTargetEmptyLocked(alloc))) return error.LiteImportTargetNotEmpty;
    try portable_backup.importPortableWithOptions(alloc, self.core.store, backup, .{
        .identity_namespace = target_identity,
        .prefer_existing_identity_namespace = true,
        .unpublished_staging = true,
    });
    portable_backup.validateCompleteDatabaseImageAlloc(alloc, self.core.store) catch |err| switch (err) {
        error.OutOfMemory => return err,
        else => return error.InvalidBackupRequest,
    };
    try doc_identity.reassignNamespaceAlloc(alloc, self.core.store, target_identity);
    try self.refreshPortableImportedGenerationLocked(target_identity);
}

pub fn restoreGenerationAdmissionReceipt(self: *DB) !?restore_staging_contract.GenerationAdmissionReceipt {
    if (comptime storage_engine.enabled) return storage_engine.call(@src().fn_name, .{self});
    var read = try self.core.store.beginReadTxn();
    defer read.abort();
    return restore_generation_admissions.loadReceipt(&read);
}

/// Private provisioners call this before cache adoption, after installing
/// the durable reservation. Initial empty schema/index admission must not
/// emit a hot-standby mutation before its owner authorization exists.
pub fn attachRestoreStagingReplicationMirror(self: *DB, mirror: ?ReplicationAsyncEffectMirror) !void {
    if (comptime storage_engine.enabled) return storage_engine.call(@src().fn_name, .{ self, mirror });
    if (!self.local_execution.restore_staging_required.load(.acquire)) return error.RestoreStagingScopeChanged;
    if (mirror != null and self.core.table_catalog.row_policy_phase != .disabled)
        return error.RowPolicyUnsupported;
    self.local_execution.replication_async_batch_mirror = mirror;
    self.local_execution.replication_async_effect_mirror = mirror;
    self.local_execution.replication_async_metadata_mirror = mirror;
}

/// Retryable owner-side repair for a crash after an import page committed
/// its primary checkpoint but before projection replay became durable.
/// Processes one bounded apply window per index, with no external producers.
pub fn prepareRestoreStagingIndexesStep(self: *DB, alloc: Allocator, scope: [32]u8) !bool {
    if (comptime storage_engine.enabled) return storage_engine.call(@src().fn_name, .{ self, alloc, scope });
    return self.prepareRestoreStagingIndexesStepLocal(alloc, scope, false);
}

pub fn prepareRestoreStagingIndexesStepLocal(self: *DB, alloc: Allocator, scope: [32]u8, replicated: bool) !bool {
    var progress = (try self.restoreStagingStatus(alloc)) orelse return error.RestoreStagingScopeChanged;
    defer progress.deinit();
    if (!std.mem.eql(u8, &progress.value.scope.digest(), &scope)) return error.RestoreStagingScopeChanged;
    if (progress.value.phase != .imported and progress.value.phase != .validated and progress.value.phase != .published) return error.RestoreStagingInProgress;
    _ = try self.validateConstraintsStepLocal(.{}, replicated);
    // A persisted receipt is not replica-local relational index coverage.
    // Advance one bounded page independently on every importing/recovering
    // owner, including after a lost validated/published acknowledgement.
    if (self.core.relational_indexes.acquire()) |index_snapshot| {
        var pinned = index_snapshot;
        defer pinned.deinit();
        const indexes = pinned.plan.boundIndexes();
        if (indexes.len != 0) for (0..@min(indexes.len, 16)) |_| {
            const ordinal = self.local_execution.relational_index_maintenance_cursor.fetchAdd(1, .monotonic) % indexes.len;
            const index = indexes[ordinal];
            switch ((try self.relationalIndexBuildStatus(index.name)).state) {
                .ready => {},
                .failed => return error.RestoreProjectionCorrupt,
                .building => {
                    try self.buildRelationalIndexStepLocal(index.name, .{}, replicated);
                    break;
                },
            }
        };
    }
    replayPendingDerivedBatches(self, null, null, .{
        .truncate_replay = false,
        .max_windows_per_index = 1,
        .deadline_ns = monotonicTimeNs() +| 250 * std.time.ns_per_ms,
    }) catch |err| switch (err) {
        error.CatchUpDeadlineExceeded => return false,
        else => return err,
    };
    try self.lockApplyForPortableRuntime();
    defer self.core.unlockApply();
    if (self.core.acquireSchemaView()) |active| {
        var view = active;
        defer view.release();
        if (view.validator()) |validator| if (validator.execution.checks != null) {
            var txn = try self.core.store.beginReadTxn();
            defer txn.abort();
            switch ((try relational_constraint_jobs.status(&txn, view)).state) {
                .enforced => {},
                .validating => return false,
                .invalid => return if (replicated) error.RestoreProjectionCorrupt else error.RelationalCheckViolation,
            }
        };
    }
    if (!(try self.restoreStagingIndexesReadyAssumeApply(alloc))) return false;
    try self.core.index_manager.syncAll(true);
    try self.core.store.sync(true);
    return true;
}

pub fn restoreStagingIndexesReadyAssumeApply(self: *DB, alloc: Allocator) !bool {
    if (self.core.relational_indexes.acquire()) |index_snapshot| {
        var pinned = index_snapshot;
        defer pinned.deinit();
        var read = try self.core.store.beginReadTxn();
        defer read.abort();
        for (pinned.plan.boundIndexes()) |index| {
            switch ((try relational_index_jobs.status(&read, index)).state) {
                .ready => {},
                .building => return false,
                .failed => return error.RestoreProjectionCorrupt,
            }
        }
    }
    const debt = try self.listDerivedReplayDebtAssumeApplyLockHeld(alloc);
    defer {
        for (debt) |*entry| entry.deinit(alloc);
        alloc.free(debt);
    }
    for (debt) |entry| if (entry.catch_up_required) return false;
    return !self.core.index_manager.hasRepairUnavailableIndexes() and !(try self.hasPendingIndexRepairIntents(alloc));
}

/// Called by private provisioning before a hidden owner can be hosted.
/// The artifact is not available yet: this durable reservation admits no
/// reads or writes, including scoped ones, until authenticated import binds it.
pub fn reserveRestoreStaging(self: *DB, alloc: Allocator, plan_id: [16]u8, plan_digest: [32]u8, namespace: doc_identity.Namespace) !void {
    if (comptime storage_engine.enabled) return storage_engine.call(@src().fn_name, .{ self, alloc, plan_id, plan_digest, namespace });
    const staging = restore_staging;
    const scope: staging.Scope = .{ .plan_id = plan_id, .plan_digest = plan_digest, .target_namespace = namespace, .source_namespace = .{ .table_id = 0, .shard_id = 0, .range_id = 0 }, .source_artifact_digest = @splat(0), .target_schema_digest = @splat(0) };
    return self.reserveRestoreStagingScoped(alloc, scope);
}

pub fn reserveRestoreStagingScoped(self: *DB, alloc: Allocator, scope: restore_staging.Scope) !void {
    if (comptime storage_engine.enabled) return storage_engine.call(@src().fn_name, .{ self, alloc, scope });
    const staging = restore_staging;
    const namespace = scope.target_namespace;
    try scope.validateReservation();
    if (scope.source_namespace.table_id != 0) try scope.validate();
    if (!namespace.eql(self.core.identity_namespace)) return error.RestoreStagingScopeChanged;
    try self.lockApplyForPortableRuntime();
    defer self.core.unlockApply();
    var txn = try self.core.store.beginWriteTxn();
    var reservation_txn_open = true;
    defer if (reservation_txn_open) txn.abort();
    if (try staging.optional(&txn)) |raw| {
        var existing = try staging.Progress.decode(alloc, raw);
        defer existing.deinit();
        if (!std.mem.eql(u8, &existing.value.scope.plan_id, &scope.plan_id) or !std.mem.eql(u8, &existing.value.scope.plan_digest, &scope.plan_digest) or !namespace.eql(existing.value.scope.target_namespace)) return error.RestoreStagingScopeChanged;
        if (scope.source_namespace.table_id != 0 and !std.mem.eql(u8, &scope.digest(), &existing.value.scope.digest())) return error.RestoreStagingScopeChanged;
        if (existing.value.phase == .canceled or existing.value.phase == .published) return error.RestoreStagingScopeChanged;
        return;
    }
    if (self.core.table_catalog.row_count != 0) return error.RestoreStagingTargetNotEmpty;
    const raw = try (staging.Progress{ .scope = scope, .phase = .reserved }).encode(alloc);
    defer alloc.free(raw);
    if (try doc_identity.loadNamespaceTxn(&txn)) |stored| {
        if (!stored.eql(namespace)) return error.RestoreStagingScopeChanged;
    } else {
        // Empty document owners may never have written a schema or row.
        // Their reservation itself must make the fresh identity durable.
        try doc_identity.writeNamespaceTxn(&txn, namespace);
    }
    try txn.put(staging.key, raw);
    try txn.commit();
    reservation_txn_open = false;
    self.local_execution.restore_staging_required.store(true, .release);
    try self.core.store.sync(true);
}

/// Receiver-side authority check. A capability never substitutes for the
/// durable owner marker and its fresh namespace; callers hold a resident DB lease.
pub fn validateRestoreStagingScope(self: *DB, alloc: Allocator, scope: [32]u8) !void {
    if (comptime storage_engine.enabled) return storage_engine.call(@src().fn_name, .{ self, alloc, scope });
    var txn = try self.core.store.beginReadTxn();
    defer txn.abort();
    try restore_staging.requireScope(alloc, &txn, scope, false);
}

pub fn beginRestoreStaging(self: *DB, alloc: Allocator, scope: restore_staging.Scope) !void {
    if (comptime storage_engine.enabled) return storage_engine.call(@src().fn_name, .{ self, alloc, scope });
    return self.beginRestoreStagingWithMarker(alloc, scope, null, null, null);
}

/// Install the immutable historical read mapping before constructing any
/// versioned projection. This does not switch the active row schema or
/// import source-local migration progress. hot-standby begin carries the same mapping
/// so followers reconstruct it before accepting row effects.
pub fn installRestoreStagingReadSchema(self: *DB, alloc: Allocator, scope: restore_staging.Scope, read_json: []const u8) !void {
    if (comptime storage_engine.enabled) return storage_engine.call(@src().fn_name, .{ self, alloc, scope, read_json });
    if (read_json.len == 0) return;
    try scope.validate();
    var previous = try public_table_schema.parseValidatedTableSchema(alloc, read_json);
    defer previous.deinit(alloc);
    const historical = try public_table_schema.deriveRuntimeTableSchema(alloc, previous);
    defer schema_mod.freeSchema(alloc, historical);
    const encoded = try schema_mod.serializeSchema(alloc, historical);
    defer alloc.free(encoded);
    const runtime_key = try schema_mod.schemaVersionKeyAlloc(alloc, historical.version);
    defer alloc.free(runtime_key);
    const public_key = try public_table_schema.versionedSchemaKeyAlloc(alloc, historical.version);
    defer alloc.free(public_key);
    try self.lockApplyForPortableRuntime();
    defer self.core.unlockApply();
    if (!scope.target_namespace.eql(self.core.identity_namespace)) return error.RestoreStagingScopeChanged;
    const active = self.core.schema orelse return error.RestoreStagingScopeChanged;
    if (historical.version >= active.version or historical.storage_mode != active.storage_mode) return error.InvalidRestoreMigrationState;
    const active_encoded = try schema_mod.serializeSchema(alloc, active);
    defer alloc.free(active_encoded);
    if (!std.mem.eql(u8, &restore_staging.digest(active_encoded), &scope.target_schema_digest)) return error.RestoreStagingScopeChanged;
    var txn = try self.core.store.beginWriteTxn();
    var history_txn_open = true;
    defer if (history_txn_open) txn.abort();
    var history_mutable = true;
    if (try restore_staging.optional(&txn)) |raw| {
        var status = try restore_staging.Progress.decode(alloc, raw);
        defer status.deinit();
        if (!std.mem.eql(u8, &status.value.scope.digest(), &scope.digest())) return error.RestoreStagingScopeChanged;
        history_mutable = status.value.phase == .reserved;
    } else if (self.core.table_catalog.row_count != 0) return error.RestoreStagingTargetNotEmpty;
    const prior_bootstrap = try missingAsNull(txn.get(restore_staging.bootstrap_key));
    if (prior_bootstrap) |bytes| {
        var prior = try restore_staging.OwnerBootstrap.decode(alloc, bytes);
        defer prior.deinit();
        if (!std.mem.eql(u8, prior.value.read_schema_json, read_json)) return error.RestoreStagingScopeChanged;
    }
    const existing = try missingAsNull(txn.get(runtime_key));
    if (existing) |bytes| if (!std.mem.eql(u8, bytes, encoded)) return error.RestoreStagingScopeChanged;
    const existing_public = try missingAsNull(txn.get(public_key));
    if (existing_public) |bytes| if (!std.mem.eql(u8, bytes, read_json)) return error.RestoreStagingScopeChanged;
    if (existing != null and existing_public != null) return;
    if (!history_mutable) return error.RestoreStagingScopeChanged;
    try txn.put(runtime_key, encoded);
    try txn.put(public_key, read_json);
    try txn.commit();
    history_txn_open = false;
    try self.core.store.sync(true);
}

pub fn installRestoreStagingBootstrap(self: *DB, alloc: Allocator, descriptor: restore_staging.OwnerBootstrap) !void {
    if (comptime storage_engine.enabled) return storage_engine.call(@src().fn_name, .{ self, alloc, descriptor });
    const staging = restore_staging;
    try descriptor.validate();
    try @import("../../schema/restore_migration.zig").validate(alloc, descriptor.schema_json, descriptor.read_schema_json);
    try self.installRestoreStagingReadSchema(alloc, descriptor.scope, descriptor.read_schema_json);
    const encoded = try descriptor.encode(alloc);
    defer alloc.free(encoded);
    try self.lockApplyForPortableRuntime();
    defer self.core.unlockApply();
    if (!descriptor.scope.target_namespace.eql(self.core.identity_namespace) or !std.mem.eql(u8, descriptor.byte_range.start, self.core.byteRange().start) or !std.mem.eql(u8, descriptor.byte_range.end, self.core.byteRange().end)) return error.RestoreStagingScopeChanged;
    var txn = try self.core.store.beginWriteTxn();
    var txn_open = true;
    defer if (txn_open) txn.abort();
    const marker = (try staging.optional(&txn)) orelse return error.RestoreStagingScopeChanged;
    var progress = try staging.Progress.decode(alloc, marker);
    defer progress.deinit();
    if (!std.mem.eql(u8, &progress.value.scope.digest(), &descriptor.scope.digest())) return error.RestoreStagingScopeChanged;
    const previous = try missingAsNull(txn.get(staging.bootstrap_key));
    if (previous) |bytes| {
        if (!std.mem.eql(u8, bytes, encoded)) return error.RestoreStagingScopeChanged;
        return;
    }
    if (progress.value.phase != .reserved) return error.RestoreStagingScopeChanged;
    const public_json = txn.get(public_schema_json_key) catch |err| switch (err) {
        error.NotFound => "",
        else => return err,
    };
    if (!std.mem.eql(u8, public_json, descriptor.schema_json)) return error.RestoreStagingScopeChanged;
    const native_schema = try schema_mod.serializeSchema(alloc, self.core.schema orelse .{});
    defer alloc.free(native_schema);
    if (!std.mem.eql(u8, &staging.digest(native_schema), &descriptor.scope.target_schema_digest)) return error.RestoreStagingScopeChanged;
    try txn.put(staging.bootstrap_key, encoded);
    try txn.commit();
    txn_open = false;
    try self.core.store.sync(true);
}

pub fn readRestoreStagingBootstrap(self: *DB, alloc: Allocator) !?std.json.Parsed(restore_staging.OwnerBootstrap) {
    if (comptime storage_engine.enabled) return storage_engine.call(@src().fn_name, .{ self, alloc });
    const staging = restore_staging;
    const stored = try self.core.getStoreValue(alloc, staging.bootstrap_key);
    defer if (stored) |bytes| alloc.free(bytes);
    if (stored) |bytes| return try staging.OwnerBootstrap.decode(alloc, bytes);
    return null;
}

pub fn encodeRestoreStagingReplicationPayload(self: *DB, req: types.BatchRequest, ordered_receipt: ?OrderedApplyReceipt) ![]u8 {
    const staging = restore_staging;
    if (req.restore_staging.? == .install_generation_admissions)
        return replication_effects_mod.encodeRestoreGenerationAdmissionMutationRequestAlloc(self.alloc, req, ordered_receipt orelse return error.InvalidRestoreStagingCommand);
    if (req.restore_staging.? == .begin) {
        const stored = try self.core.getStoreValue(self.alloc, staging.bootstrap_key);
        defer if (stored) |bytes| self.alloc.free(bytes);
        if (stored) |bytes| {
            var descriptor = try staging.OwnerBootstrap.decode(self.alloc, bytes);
            defer descriptor.deinit();
            return replication_effects_mod.encodeBatchMutationWithRestoreBootstrapAlloc(self.alloc, req, descriptor.value);
        }
    }
    return replication_effects_mod.encodeBatchMutationRequestAlloc(self.alloc, req);
}

/// An exact replicated completion receipt may survive a crash before its
/// local index coverage does. Repair only the still-current scope/phase;
/// old entries and canceled/replaced attempts retain no-op replay semantics.
/// This never reapplies logical controls or advances replication markers.
pub fn prepareAppliedRestoreFinishReplay(self: *DB, req: types.BatchRequest) !void {
    const command = req.restore_staging orelse return;
    if (command != .finish or (command.finish.phase != .validated and command.finish.phase != .published)) return;
    var progress = (try self.restoreStagingStatus(self.alloc)) orelse return;
    defer progress.deinit();
    if (progress.value.phase != command.finish.phase or !std.mem.eql(u8, &progress.value.scope.digest(), &command.finish.scope)) return;
    const ready = self.prepareRestoreStagingIndexesStepLocal(self.alloc, command.finish.scope, true) catch |err| switch (err) {
        // A concurrently completed cancellation/replacement must not turn
        // an already-applied entry into a stale logical command failure.
        error.RestoreStagingScopeChanged, error.RestoreStagingInProgress => return,
        else => return err,
    };
    if (!ready) return error.RestoreProjectionCatchUpPending;
}

pub fn installRestoreGenerationAdmissionsWithMarker(
    self: *DB,
    alloc: Allocator,
    command: restore_staging_contract.InstallGenerationAdmissions,
    marker: ?OrderedApplyReceipt,
    replication_lsn: ?u64,
    replication_payload: ?[]const u8,
) !restore_staging_contract.GenerationAdmissionReceipt {
    const contract = restore_staging_contract;
    const entry = marker orelse return error.InvalidRestoreStagingCommand;
    const expected = try contract.admissionReceiptDigest(command);
    try self.lockApplyForPortableRuntime();
    defer self.core.unlockApply();
    var txn = try self.core.store.beginWriteTxn();
    var txn_open = true;
    defer if (txn_open) txn.abort();
    const bootstrap_bytes = txn.get(contract.bootstrap_key) catch |err| switch (err) {
        error.NotFound => return error.RestoreStagingScopeChanged,
        else => return err,
    };
    var bootstrap = try contract.OwnerBootstrap.decode(alloc, bootstrap_bytes);
    defer bootstrap.deinit();
    const binding = bootstrap.value.generation_admission orelse return error.RestoreSourceProofMissing;
    const source_proof_digest = bootstrap.value.source_generation_proof_digest orelse return error.RestoreSourceProofMissing;
    if (!std.mem.eql(u8, &bootstrap.value.scope.digest(), &command.scope) or
        !std.mem.eql(u8, &source_proof_digest, &binding.source_summary_digest) or
        !std.mem.eql(u8, &binding.source_summary_digest, &command.source_summary_digest) or
        !std.mem.eql(u8, &binding.expected_receipt_digest, &expected))
        return error.RestoreStagingScopeChanged;
    switch (try orderedApplyDisposition(try readOrderedApplyReceipt(alloc, self.core.store), entry)) {
        .already_applied => {
            const receipt = (try restore_generation_admissions.loadReceipt(&txn)) orelse
                return error.RestoreSourceProofMissing;
            if (!std.mem.eql(u8, &receipt.logical_digest, &expected) or
                !std.mem.eql(u8, &receipt.scope, &command.scope)) return error.RestoreStagingScopeChanged;
            return receipt;
        },
        .apply => {},
    }
    const receipt = try restore_generation_admissions.stageInstall(alloc, &txn, command, entry.term, entry.index);
    var marker_bytes: [ordered_apply_receipt_value_len]u8 = undefined;
    const applied = orderedApplyReceiptWrite(entry, &marker_bytes);
    try txn.put(applied.key, applied.value);
    if (replication_lsn) |lsn| {
        var lsn_bytes: [replication_applied_lsn_value_len]u8 = undefined;
        const replicated = replicationAppliedSequenceWrite(lsn, &lsn_bytes);
        try txn.put(replicated.key, replicated.value);
    }
    try self.stageRestoreStagingReplicationOutbox(&txn, replication_payload);
    try txn.commit();
    txn_open = false;
    try self.core.store.sync(true);
    return receipt;
}

/// Control payload and owner marker are committed together. Every hot-standby mode
/// retains a local append obligation; remote acknowledgement still follows
/// the configured policy. Retrying an applied Raft entry drains this record.
pub fn applyRestoreStagingControl(self: *DB, req: types.BatchRequest, opts: BatchExecutionOptions) !void {
    // Local index/CHECK progress is not a deterministic command result.
    // A follower may have committed the previous import's primary/Raft
    // receipt before its projection watermark was durable. Repair one
    // bounded local slice on EVERY committed-control retry; no optional
    // worker or leader RPC is required, and no apply lock is held here.
    if (opts.ordered_apply_receipt != null or opts.replication_applied_lsn_marker != null) {
        if (req.restore_staging.? == .finish and (req.restore_staging.?.finish.phase == .validated or req.restore_staging.?.finish.phase == .published)) {
            if (!try self.prepareRestoreStagingIndexesStepLocal(self.alloc, req.restore_staging.?.finish.scope, true)) return error.RestoreProjectionCatchUpPending;
        }
    }
    var mutation = if (opts.bypass_replication_write_gate) null else self.acquireReplicationMutationShared();
    defer if (mutation) |*lease| lease.release();
    if (!opts.bypass_replication_write_gate) {
        try self.enforceReplicationWriteGate();
        try self.ensureDurableReplicationStartupBarrier();
        try self.flushDurableReplicationOutboxes();
        try self.preflightReplicationBatchSyncCommit();
    }
    const payload = if (!opts.bypass_replication_write_gate and self.local_execution.replication_async_batch_mirror != null)
        try self.encodeRestoreStagingReplicationPayload(req, opts.ordered_apply_receipt)
    else
        null;
    defer if (payload) |bytes| self.alloc.free(bytes);
    switch (req.restore_staging.?) {
        .begin => |scope| try self.beginRestoreStagingWithMarker(self.alloc, scope, opts.ordered_apply_receipt, opts.replication_applied_lsn_marker, payload),
        .finish => |finish| _ = try self.finishRestoreStagingWithMarker(self.alloc, finish.scope, finish.phase, opts.ordered_apply_receipt, opts.replication_applied_lsn_marker, payload),
        .install_generation_admissions => |install| _ = try self.installRestoreGenerationAdmissionsWithMarker(self.alloc, install, opts.ordered_apply_receipt, opts.replication_applied_lsn_marker, payload),
        .import_page, .rewrite_page => unreachable,
    }
    if (payload != null) try self.flushDurableReplicationOutboxes();
}

pub fn stageRestoreStagingReplicationOutbox(self: *DB, txn: anytype, payload: ?[]const u8) !void {
    const bytes = payload orelse return;
    const mirror = self.local_execution.replication_async_batch_mirror orelse return error.ReplicationPublisherUnavailable;
    const from_lsn = mirror.publisher.nextLsn();
    const encoded = try encodeDurableReplicationOutboxAlloc(self.alloc, from_lsn, bytes);
    defer self.alloc.free(encoded);
    const outbox_key = try durableReplicationOutboxKeyAlloc(self.alloc, .restore_batch, from_lsn, self.core.root_generation, bytes);
    defer self.alloc.free(outbox_key);
    try txn.put(outbox_key, encoded);
    // A failed transaction may leave a false positive, which the bounded
    // outbox scan clears. Never expose a committed obligation as absent.
    self.local_execution.durable_replication_outbox_maybe.store(true, .release);
}

pub fn beginRestoreStagingWithMarker(self: *DB, alloc: Allocator, scope: restore_staging.Scope, marker: ?OrderedApplyReceipt, replication_lsn: ?u64, replication_payload: ?[]const u8) !void {
    const staging = restore_staging;
    try scope.validate();
    if (!scope.target_namespace.eql(self.core.identity_namespace)) return error.RestoreStagingScopeChanged;
    try self.lockApplyForPortableRuntime();
    defer self.core.unlockApply();
    if (marker) |entry| switch (try orderedApplyDisposition(try readOrderedApplyReceipt(self.alloc, self.core.store), entry)) {
        .already_applied => return,
        .apply => {},
    };
    var manager = try self.core.initTxnManager();
    defer manager.deinit();
    if (try manager.hasSchemaLeases()) return error.SchemaInUse;
    var txn = try self.core.store.beginWriteTxn();
    var txn_open = true;
    defer if (txn_open) txn.abort();
    if (try staging.optional(&txn)) |raw| {
        var existing = try staging.Progress.decode(alloc, raw);
        defer existing.deinit();
        if (existing.value.phase == .reserved) {
            if (!std.mem.eql(u8, &scope.plan_id, &existing.value.scope.plan_id) or !std.mem.eql(u8, &scope.plan_digest, &existing.value.scope.plan_digest) or !scope.target_namespace.eql(existing.value.scope.target_namespace)) return error.RestoreStagingScopeChanged;
            if (existing.value.scope.source_namespace.table_id != 0 and !std.mem.eql(u8, &scope.digest(), &existing.value.scope.digest())) return error.RestoreStagingScopeChanged;
        } else {
            if (!std.mem.eql(u8, &scope.digest(), &existing.value.scope.digest())) return error.RestoreStagingScopeChanged;
            if (existing.value.phase == .canceled) return error.RestoreStagingCanceled;
            if (marker != null or replication_lsn != null or replication_payload != null) {
                if (marker) |entry| {
                    var buffer: [ordered_apply_receipt_value_len]u8 = undefined;
                    const write = orderedApplyReceiptWrite(entry, &buffer);
                    try txn.put(write.key, write.value);
                }
                if (replication_lsn) |lsn| {
                    var buffer: [replication_applied_lsn_value_len]u8 = undefined;
                    const write = replicationAppliedSequenceWrite(lsn, &buffer);
                    try txn.put(write.key, write.value);
                }
                try self.stageRestoreStagingReplicationOutbox(&txn, replication_payload);
                try txn.commit();
                txn_open = false;
            }
            return;
        }
    }
    if (self.core.table_catalog.row_count != 0) return error.RestoreStagingTargetNotEmpty;
    if (scope.empty_generation) {
        if ((scope.graph_retirement_digest != null) != self.core.index_manager.hasGraphIndexes()) return error.RestoreStagingScopeChanged;
        if (scope.graph_retirement_digest != null) for (self.core.index_manager.graph_indexes.items) |*entry| {
            try entry.index.requirePristineEmptyGeneration();
        };
        if (scope.graph_retirement_digest != null) {
            // Source-owned graph edge/state/contender rows live in the
            // primary store, outside the graph index's private stores.
            // An empty row count cannot prove these orphan artifacts
            // absent. The new owner normally has no user-key band, so
            // this is one bounded early-exit cursor walk at admission.
            var graph_cursor = try txn.openPhysicalCursorAdapter();
            defer graph_cursor.close();
            var entry = try graph_cursor.seekAtOrAfter(&.{internal_keys.replay_namespace});
            while (entry) |record| {
                if (record.key.len == 0 or record.key[0] != internal_keys.replay_namespace or
                    (record.key.len > 1 and record.key[1] == 0xff)) break;
                if (internal_keys.isGraphEdgeArtifactKey(record.key) or
                    internal_keys.isGraphAssetStateKey(record.key) or
                    internal_keys.isGraphEdgeContenderKey(record.key)) return error.RestoreStagingTargetNotEmpty;
                entry = try graph_cursor.next();
            }
            const global_prefix = [_]u8{ internal_keys.replay_namespace, 0xff, internal_keys.graph_global_edge_contender_kind };
            if (try graph_cursor.seekAtOrAfter(&global_prefix)) |record| if (std.mem.startsWith(u8, record.key, &global_prefix)) return error.RestoreStagingTargetNotEmpty;
        }
        // These global secondary records lie outside document identity's
        // user-key proof. A fresh generation must not inherit claims or
        // ordered-index entries even if a corrupt counter says zero.
        var cursor = try txn.openPhysicalCursorAdapter();
        defer cursor.close();
        for ([_][]const u8{ @import("relational_integrity_contract.zig").namespace, relational_index_records.forward_namespace, relational_index_records.ownership_namespace }) |prefix| {
            if (try cursor.seekAtOrAfter(prefix)) |entry| if (std.mem.startsWith(u8, entry.key, prefix)) return error.RestoreStagingTargetNotEmpty;
        }
    }
    const schema_bytes = try schema_mod.serializeSchema(alloc, self.core.schema orelse .{});
    defer alloc.free(schema_bytes);
    if (!std.mem.eql(u8, &staging.digest(schema_bytes), &scope.target_schema_digest)) return error.RestoreStagingScopeChanged;
    var initial_identity_summary: ?doc_identity.VisibilitySummary = null;
    if (scope.preserve_artifacts or scope.empty_generation) {
        // Native cache pages precede primary rows. Bind a proven-pristine
        // identity root and exact zero cardinality before those artifacts
        // make an absent visibility summary ambiguous. Keep these facts
        // atomic with scope admission, Raft/standby markers, and outbox.
        initial_identity_summary = (try doc_identity.initializePristineVisibilitySummaryTxn(&txn, scope.target_namespace)) orelse return error.RestoreStagingTargetNotEmpty;
        if (try range_cardinality.loadFromTxn(&txn)) |count| {
            if (count != 0) return error.RestoreStagingTargetNotEmpty;
        } else {
            const zero = @as([8]u8, @splat(0));
            try txn.put(&internal_keys.range_document_count_key, &zero);
        }
    }
    const raw_catalog = try missingAsNull(txn.get(relational_integrity_catalog.key));
    if (raw_catalog) |bytes| {
        var catalog = try relational_integrity_catalog.decode(alloc, bytes);
        defer catalog.deinit();
        if (try staging.initialCoverage(alloc, &txn, catalog)) |coverage| {
            defer alloc.free(coverage);
            try txn.put(relational_integrity_activation.key, coverage);
        }
    }
    const bootstrap_raw = try missingAsNull(txn.get(staging.bootstrap_key));
    var source_proofs_required = false;
    if (bootstrap_raw) |bytes| {
        var bootstrap = try staging.OwnerBootstrap.decode(alloc, bytes);
        defer bootstrap.deinit();
        if (!std.mem.eql(u8, &bootstrap.value.scope.digest(), &scope.digest())) return error.RestoreStagingScopeChanged;
        source_proofs_required = bootstrap.value.source_generation_proof_digest != null;
    }
    const initial = try (staging.Progress{ .scope = scope, .phase = if (scope.empty_generation) .imported else .importing, .source_generation_proofs_complete = !source_proofs_required, .rewrite = if (scope.rewrite) |rewrite| .{ .sequence = rewrite.retained_start } else null }).encode(alloc);
    defer alloc.free(initial);
    try txn.put(staging.key, initial);
    try self.stageRestoreStagingReplicationOutbox(&txn, replication_payload);
    if (replication_lsn) |lsn| {
        var buffer: [replication_applied_lsn_value_len]u8 = undefined;
        const write = replicationAppliedSequenceWrite(lsn, &buffer);
        try txn.put(write.key, write.value);
    }
    if (marker) |entry| {
        var buffer: [ordered_apply_receipt_value_len]u8 = undefined;
        const write = orderedApplyReceiptWrite(entry, &buffer);
        try txn.put(write.key, write.value);
    }
    try txn.commit();
    txn_open = false;
    if (initial_identity_summary) |summary| self.core.identity_visibility.summary = summary;
    self.local_execution.restore_staging_required.store(true, .release);
    try self.core.store.sync(true);
}

/// Source is a verified immutable artifact opened read-only by the shared
/// restore driver. Each page decodes its declared historical layout once
/// and feeds logical rows through the target's normal preparation pipeline.
pub fn importRestoreStagingPage(self: *DB, alloc: Allocator, scope: restore_staging.Scope, source: *DB, max_rows: usize, cancellation: types.CancellationToken) !restore_staging.Phase {
    if (comptime storage_engine.enabled) return storage_engine.call(@src().fn_name, .{ self, alloc, scope, source, max_rows, cancellation });
    var prepared = try self.prepareRestoreStagingPage(alloc, scope, source, max_rows, cancellation);
    defer prepared.deinit();
    if (prepared.batch) |req| try self.batchReplicatedApply(req);
    try self.core.store.sync(true);
    return prepared.phase;
}

pub fn prepareRestoreStagingPage(self: *DB, alloc: Allocator, scope: restore_staging.Scope, source: *DB, max_rows: usize, cancellation: types.CancellationToken) !restore_staging.PreparedPage {
    if (comptime storage_engine.enabled) return storage_engine.call(@src().fn_name, .{ self, alloc, scope, source, max_rows, cancellation });
    if (scope.rewrite != null or scope.empty_generation) return error.InvalidRestoreStagingCommand;
    return self.prepareRestoreStagingPageInternal(alloc, scope, source, max_rows, cancellation, null);
}

pub fn prepareRestoreStagingPageInternal(self: *DB, alloc: Allocator, scope: restore_staging.Scope, source: *DB, max_rows: usize, cancellation: types.CancellationToken, program: ?*const relational_rewrite_staging.ProgramSet) !restore_staging.PreparedPage {
    const staging = restore_staging;
    if (max_rows == 0 or max_rows > 128 or source.open_mode != .query_readonly or !source.core.identity_namespace.eql(scope.source_namespace)) return error.InvalidRestoreStagingCommand;
    if (!std.mem.eql(u8, source.core.byteRange().start, self.core.byteRange().start) or !std.mem.eql(u8, source.core.byteRange().end, self.core.byteRange().end)) return error.RestoreStagingScopeChanged;
    var arena = std.heap.ArenaAllocator.init(alloc);
    errdefer arena.deinit();
    const owned = arena.allocator();
    const raw = (try self.core.getStoreValue(owned, staging.key)) orelse return error.RestoreStagingScopeChanged;
    const parsed = try staging.Progress.decode(owned, raw);
    var next = parsed.value;
    if (!std.mem.eql(u8, &next.scope.digest(), &scope.digest())) return error.RestoreStagingScopeChanged;
    if (next.phase == .canceled) return error.RestoreStagingCanceled;
    if (next.phase == .reserved) return error.RestoreStagingInProgress;
    if (next.phase != .importing) return .{ .arena = arena, .phase = next.phase, .batch = null };
    if (!next.source_generation_proofs_complete) {
        const portable = portable_backup;
        var bootstrap = (try self.readRestoreStagingBootstrap(alloc)) orelse return error.RestoreSourceProofMissing;
        defer bootstrap.deinit();
        const expected_digest = bootstrap.value.source_generation_proof_digest orelse return error.RestoreSourceProofMissing;
        if (!std.mem.eql(u8, &bootstrap.value.scope.digest(), &scope.digest())) return error.RestoreStagingScopeChanged;
        var source_read = try source.core.store.beginReadTxn();
        defer source_read.abort();
        var proofs = try portable.sourceGenerationAdmissionProofPageAlloc(alloc, &source_read, scope.source_namespace);
        defer proofs.deinit();
        if (!std.mem.eql(u8, &expected_digest, &try portable.sourceGenerationAdmissionSummaryDigest(scope.source_namespace, proofs.entries)))
            return error.RestoreSourceProofMissing;
        const artifacts = try owned.alloc(staging.Artifact, proofs.artifacts.len);
        for (proofs.artifacts, artifacts) |proof, *artifact| artifact.* = .{
            .key = try owned.dupe(u8, proof.key),
            .value = try owned.dupe(u8, proof.value),
        };
        next.source_generation_proofs_complete = true;
        const encoded_progress = try next.encode(owned);
        return .{ .arena = arena, .phase = next.phase, .batch = .{ .sync_level = .write, .restore_staging = .{ .import_page = .{
            .expected = staging.digest(raw),
            .next = encoded_progress,
            .scope = scope.digest(),
            .timestamps = &.{},
            .source_generation_proof_page = true,
            .artifacts = artifacts,
        } } } };
    }
    if (next.rewrite) |rewrite| if (rewrite.snapshot_complete) return .{ .arena = arena, .phase = next.phase, .batch = null };
    if (scope.preserve_artifacts and (!next.artifacts_complete or next.rows_complete)) {
        // Import immutable generated inputs before primary rows. Normal
        // row preparation can then rebuild target-local index identities
        // using cached provider results, without issuing remote requests.
        var read = try source.core.store.beginReadTxn();
        defer read.abort();
        var artifacts_cursor = try read.openCursor();
        defer artifacts_cursor.close();
        const projections = next.rows_complete;
        var write_plan = if (!projections) try self.core.index_manager.acquireWritePlanSnapshot() else null;
        defer if (write_plan) |*view| view.release();
        var generated_embeddings: std.StringHashMapUnmanaged(void) = .empty;
        if (write_plan) |view| for (view.plan().generated_templates) |request| {
            if (request.kind == .dense_embedding or request.kind == .sparse_embedding)
                try generated_embeddings.put(owned, requestEmbeddingName(request), {});
        };
        const continuation = if (projections) &next.projection_cursor else &next.artifact_cursor;
        // Skipped records retain only one continuation and one record's
        // scratch data, never one allocation per scan step in the page.
        var continuation_buffer: std.ArrayList(u8) = .empty;
        defer continuation_buffer.deinit(alloc);
        var record_arena = std.heap.ArenaAllocator.init(alloc);
        defer record_arena.deinit();
        var artifact_entry = try artifacts_cursor.seekAtOrAfter(if (continuation.*.len == 0) &.{internal_keys.user_namespace} else continuation.*);
        if (artifact_entry) |row| if (std.mem.eql(u8, row.key, continuation.*)) {
            artifact_entry = try artifacts_cursor.next();
        };
        var artifacts: std.ArrayList(staging.Artifact) = .empty;
        var artifact_bytes: usize = 0;
        var examined_artifacts: usize = 0;
        // Artifact and projection passes only scan the immutable source;
        // they often emit no writes. Give a bounded cursor turn enough
        // time to amortize reopening the decoder and committing its
        // continuation, while retaining per-record cancellation and the
        // 1024-record/128-artifact/16 MiB page ceilings below.
        const artifact_deadline = monotonicTimeNs() +| 100 * std.time.ns_per_ms;
        while (artifact_entry) |row| : (artifact_entry = try artifacts_cursor.next()) {
            _ = record_arena.reset(.retain_capacity);
            const scratch = record_arena.allocator();
            if (cancellation.isCancelled()) return error.Canceled;
            if (row.key.len == 0 or row.key[0] != internal_keys.user_namespace) {
                artifact_entry = null;
                break;
            }
            if (artifacts.items.len == max_rows or examined_artifacts == 1024 or
                (examined_artifacts != 0 and monotonicTimeNs() >= artifact_deadline)) break;
            if (if (projections) isMergeArtifactKey(row.key) else isRestoreArtifactKey(row.key)) {
                if (row.key.len > 16 * 1024 * 1024 or row.value.len > 16 * 1024 * 1024 - row.key.len) return error.TransactionTooLarge;
                const owner = (try internal_keys.decodeDocumentComponentAlloc(scratch, row.key)) orelse return error.InvalidRestoreStagingCommand;
                if (!self.core.byteRange().contains(owner)) return error.RestoreStagingScopeChanged;
                if (!projections and (internal_keys.isEmbeddingArtifactKey(row.key) or internal_keys.isDerivedEmbeddingArtifactKey(row.key)) and
                    !(try enrichment_artifact_codec.decodeHeader(row.value)).flags.has_source_hash)
                {
                    const embedding = (try artifact_ids.decodeEmbeddingArtifactIdentityAlloc(scratch, row.key)) orelse return error.InvalidRestoreStagingCommand;
                    if (generated_embeddings.contains(embedding.embedding_name)) {
                        // Portable vectors lack producer provenance. A
                        // managed producer must regenerate them, even when
                        // this hidden owner's optional runtime is absent.
                        // External vectors have no producer and remain data.
                        continuation_buffer.clearRetainingCapacity();
                        try continuation_buffer.appendSlice(alloc, row.key);
                        continuation.* = continuation_buffer.items;
                        examined_artifacts += 1;
                        continue;
                    }
                }
                const value = if (internal_keys.isGraphEdgeArtifactKey(row.key)) graph: {
                    const edge = (try internal_keys.parseGraphEdgeArtifactKeyAlloc(scratch, row.key)) orelse return error.InvalidRestoreStagingCommand;
                    const index = self.core.index_manager.graphIndex(edge.index_name);
                    if (index == null or enrichment_artifact_codec.isLegacyUnboundGraphEdge(row.value)) {
                        continuation_buffer.clearRetainingCapacity();
                        try continuation_buffer.appendSlice(alloc, row.key);
                        continuation.* = continuation_buffer.items;
                        examined_artifacts += 1;
                        continue;
                    }
                    const decoded = try enrichment_artifact_codec.decodeGraphEdgeAlloc(scratch, row.value);
                    if (decoded.generation != index.?.config.coverage_generation and !enrichment_artifact_codec.isPortableUnboundGraphEdge(row.value)) {
                        continuation_buffer.clearRetainingCapacity();
                        try continuation_buffer.appendSlice(alloc, row.key);
                        continuation.* = continuation_buffer.items;
                        examined_artifacts += 1;
                        continue;
                    }
                    break :graph try enrichment_artifact_codec.encodePortableUnboundGraphEdgeWithTtlAlloc(scratch, decoded.weight, decoded.created_at, decoded.updated_at, decoded.ttl_created_ns, decoded.metadata_json);
                } else if (projections) current: {
                    // Row preparation may regenerate (or remove) a portable
                    // cache entry. Project the target's current artifact,
                    // never overwrite it with an archived generated value.
                    break :current self.core.store.get(scratch, row.key) catch |err| switch (err) {
                        error.NotFound => {
                            continuation_buffer.clearRetainingCapacity();
                            try continuation_buffer.appendSlice(alloc, row.key);
                            continuation.* = continuation_buffer.items;
                            examined_artifacts += 1;
                            continue;
                        },
                        else => return err,
                    };
                } else row.value;
                const size = std.math.add(usize, row.key.len, value.len) catch return error.TransactionTooLarge;
                if (size > 16 * 1024 * 1024) return error.TransactionTooLarge;
                if (artifacts.items.len != 0 and artifact_bytes + size > 16 * 1024 * 1024) break;
                try artifacts.append(owned, .{ .key = try owned.dupe(u8, row.key), .value = try owned.dupe(u8, value) });
                artifact_bytes += size;
            }
            continuation_buffer.clearRetainingCapacity();
            try continuation_buffer.appendSlice(alloc, row.key);
            continuation.* = continuation_buffer.items;
            examined_artifacts += 1;
        }
        if (artifact_entry == null) {
            // Only the active phase retains a physical continuation. This
            // keeps the existing bounded record size even for maximum-size
            // binary document keys, and commits phase + cursor atomically.
            continuation.* = "";
            if (projections) next.phase = .imported else next.artifacts_complete = true;
        } else continuation.* = try owned.dupe(u8, continuation.*);
        // Finish every arena allocation before copying its ownership into
        // PreparedPage; allocating while evaluating a later field would
        // update only the old ArenaAllocator state and leak its new block.
        const encoded_progress = try next.encode(owned);
        return .{ .arena = arena, .phase = next.phase, .batch = .{ .sync_level = .write, .restore_staging = .{ .import_page = .{
            .expected = staging.digest(raw),
            .next = encoded_progress,
            .scope = scope.digest(),
            .timestamps = &.{},
            .artifact_page = !projections,
            .projection_page = projections,
            .artifacts = artifacts.items,
        } } } };
    }
    var source_read = try source.core.store.beginReadTxn();
    defer source_read.abort();
    var cursor = try source_read.openCursor();
    defer cursor.close();
    var continuation_buffer: std.ArrayList(u8) = .empty;
    defer continuation_buffer.deinit(alloc);
    var entry = try cursor.seekAtOrAfter(if (next.cursor.len == 0) &.{internal_keys.user_namespace} else next.cursor);
    if (entry) |row| if (std.mem.eql(u8, row.key, next.cursor)) {
        entry = try cursor.next();
    };
    var writes: std.ArrayList(types.BatchWrite) = .empty;
    var timestamps: std.ArrayList(staging.Timestamp) = .empty;
    var historical: ?schema_registry_mod.SchemaView = null;
    defer if (historical) |*view| view.release();
    var bytes: usize = 0;
    var examined: usize = 0;
    const row_started = monotonicTimeNs();
    const row_soft_deadline = row_started +| 5 * std.time.ns_per_ms;
    const row_hard_deadline = row_started +| 100 * std.time.ns_per_ms;
    while (entry) |row| : (entry = try cursor.next()) {
        if (cancellation.isCancelled()) return error.Canceled;
        if (row.key.len == 0 or row.key[0] != internal_keys.user_namespace) {
            entry = null;
            break;
        }
        if (writes.items.len == max_rows or examined == 1024) break;
        if (examined != 0) {
            const now = monotonicTimeNs();
            // Restore pages pay a durable apply and index update each
            // time. Amortize those costs over at least 32 ordinary rows
            // when possible, but keep the 100 ms hard slice, row/byte/
            // examined-record ceilings and per-record cancellation.
            if (now >= row_hard_deadline or (writes.items.len >= 32 and now >= row_soft_deadline)) break;
        }
        const document_key = (try internal_keys.decodeStoredDocumentRowKeyAlloc(owned, row.key)) orelse {
            continuation_buffer.clearRetainingCapacity();
            try continuation_buffer.appendSlice(alloc, row.key);
            next.cursor = continuation_buffer.items;
            examined += 1;
            continue;
        };
        if (!self.core.byteRange().contains(document_key)) return error.RestoreStagingScopeChanged;
        var timestamp: u64 = 0;
        var json_null_fields: []const []const u8 = &.{};
        const json = if (internal_keys.isRelationalRowKey(row.key)) logical: {
            if (program) |rewrite| {
                const transformed = try rewrite.transformJson(owned, row.value);
                timestamp = transformed.timestamp;
                json_null_fields = transformed.json_null_fields;
                break :logical transformed.json;
            }
            const version = try relational_store.rowSchemaVersion(row.value);
            if (historical == null or historical.?.version() != version) {
                if (historical) |*view| view.release();
                historical = (try source.core.acquireSchemaVersionView(version)) orelse return error.UnknownSchemaVersion;
            }
            const view = historical.?;
            const typed = try relational_row_codec.ordinalRowViewSelective(row.value, view.tableSchema().*, view.physicalLayout());
            timestamp = typed.writeTimestampNs();
            json_null_fields = try typed.jsonNullFieldsAlloc(owned);
            break :logical try typed.reconstructValueAlloc(owned);
        } else logical: {
            timestamp = try source.getTimestamp(owned, document_key);
            if (program) |rewrite| break :logical try rewrite.preserveDocument(owned, row.value);
            break :logical try owned.dupe(u8, row.value);
        };
        const row_bytes = std.math.add(usize, document_key.len, json.len) catch return error.TransactionTooLarge;
        if (row_bytes > 16 * 1024 * 1024) return error.RelationalRowResultTooLarge;
        if (writes.items.len != 0 and bytes + row_bytes > 16 * 1024 * 1024) break;
        bytes += row_bytes;
        // The import-page admission below selects preserved-value row
        // preparation. These historical logical values must be validated,
        // not treated as new API input that fills today's defaults or
        // recomputes (and thereby repairs) stored generated results.
        try writes.append(owned, .{ .key = document_key, .value = json, .json_null_fields = json_null_fields });
        try timestamps.append(owned, .{ .key = document_key, .timestamp = timestamp });
        var hash = std.crypto.hash.Blake3.init(.{});
        hash.update(&next.logical_digest);
        var length: [8]u8 = undefined;
        std.mem.writeInt(u64, &length, document_key.len, .little);
        hash.update(&length);
        hash.update(document_key);
        hash.update(json);
        relational_rewrite_program.hashJsonNullFields(&hash, json_null_fields);
        hash.final(&next.logical_digest);
        next.rows = std.math.add(u64, next.rows, 1) catch return error.InvalidRestoreStagingCommand;
        continuation_buffer.clearRetainingCapacity();
        try continuation_buffer.appendSlice(alloc, row.key);
        next.cursor = continuation_buffer.items;
        examined += 1;
    }
    if (entry == null) {
        if (next.rewrite) |*rewrite| rewrite.snapshot_complete = true else if (scope.preserve_artifacts) {
            next.rows_complete = true;
            next.cursor = "";
        } else next.phase = .imported;
    }
    next.cursor = try owned.dupe(u8, next.cursor);
    const encoded = try next.encode(owned);
    const page: staging.ImportPage = .{ .expected = staging.digest(raw), .next = encoded, .scope = scope.digest(), .timestamps = timestamps.items };
    return .{
        .arena = arena,
        .phase = next.phase,
        .batch = .{
            .writes = writes.items,
            // The scoped native import synchronizes derived writes internally.
            // Request only durable apply acknowledgement here: the outer
            // serving-table visibility lookup cannot resolve this hidden owner.
            // Validation rechecks all local coverage before sealing its receipt.
            .sync_level = .write,
            .restore_staging = if (program != null) .{ .rewrite_page = page } else .{ .import_page = page },
        },
    };
}

/// Scope is retained as a tombstone after publication/cancellation. Late
/// import RPCs can never initialize or mutate a recycled owner generation.
pub fn finishRestoreStaging(self: *DB, alloc: Allocator, scope: restore_staging.Digest, phase: restore_staging.Phase) !restore_staging.Digest {
    if (comptime storage_engine.enabled) return storage_engine.call(@src().fn_name, .{ self, alloc, scope, phase });
    return self.finishRestoreStagingWithMarker(alloc, scope, phase, null, null, null);
}

pub fn finishRestoreStagingWithMarker(self: *DB, alloc: Allocator, scope: restore_staging.Digest, phase: restore_staging.Phase, marker: ?OrderedApplyReceipt, replication_lsn: ?u64, replication_payload: ?[]const u8) !restore_staging.Digest {
    const staging = restore_staging;
    if (phase != .validated and phase != .published and phase != .canceled) return error.InvalidRestoreStagingCommand;
    try self.lockApplyForPortableRuntime();
    defer self.core.unlockApply();
    var manager = try self.core.initTxnManager();
    defer manager.deinit();
    if (try manager.hasSchemaLeases()) return error.SchemaInUse;
    var txn = try self.core.store.beginWriteTxn();
    var txn_open = true;
    defer if (txn_open) txn.abort();
    const raw = (try staging.optional(&txn)) orelse return error.RestoreStagingScopeChanged;
    var parsed = try staging.Progress.decode(alloc, raw);
    defer parsed.deinit();
    var next = parsed.value;
    if (!std.mem.eql(u8, &next.scope.digest(), &scope)) return error.RestoreStagingScopeChanged;
    if (marker) |entry| switch (try orderedApplyDisposition(try readOrderedApplyReceipt(self.alloc, self.core.store), entry)) {
        .already_applied => return next.receipt(),
        .apply => {},
    };
    if (next.phase != phase) {
        if (next.phase == .canceled or next.phase == .published) return error.RestoreStagingScopeChanged;
        if (phase == .validated and next.phase != .imported) return error.RestoreStagingInProgress;
        if (phase == .published and next.phase != .validated) return error.RestoreStagingInProgress;
    }
    if ((phase == .validated or phase == .published) and
        (next.phase == .imported or next.phase == .validated or next.phase == .published) and
        !try self.restoreStagingIndexesReadyAssumeApply(alloc))
        return if (marker != null or replication_lsn != null) error.RestoreProjectionCatchUpPending else error.IndexRebuilding;
    if (next.phase == phase) {
        if (marker != null or replication_lsn != null or replication_payload != null) {
            if (marker) |entry| {
                var buffer: [ordered_apply_receipt_value_len]u8 = undefined;
                const write = orderedApplyReceiptWrite(entry, &buffer);
                try txn.put(write.key, write.value);
            }
            if (replication_lsn) |lsn| {
                var buffer: [replication_applied_lsn_value_len]u8 = undefined;
                const write = replicationAppliedSequenceWrite(lsn, &buffer);
                try txn.put(write.key, write.value);
            }
            try self.stageRestoreStagingReplicationOutbox(&txn, replication_payload);
            try txn.commit();
            txn_open = false;
        }
        return next.receipt();
    }
    if (phase == .validated) {
        const bootstrap_raw = try missingAsNull(txn.get(staging.bootstrap_key));
        if (bootstrap_raw) |bytes| {
            var bootstrap = try staging.OwnerBootstrap.decode(alloc, bytes);
            defer bootstrap.deinit();
            if (!std.mem.eql(u8, &bootstrap.value.scope.digest(), &scope)) return error.RestoreStagingScopeChanged;
            if (bootstrap.value.source_generation_proof_digest) |expected_digest| {
                if (!next.source_generation_proofs_complete) return error.RestoreSourceProofMissing;
                try restore_generation_admissions.verifyImportedProofSummary(alloc, &txn, next.scope.source_namespace, expected_digest);
            }
        }
        if (self.core.acquireSchemaView()) |active| {
            var view = active;
            defer view.release();
            if (view.validator()) |validator| if (validator.execution.checks != null) switch ((try relational_constraint_jobs.status(&txn, view)).state) {
                .enforced => {},
                .validating => return if (marker != null or replication_lsn != null) error.RestoreProjectionCatchUpPending else error.ConstraintActivationInProgress,
                .invalid => return if (marker != null or replication_lsn != null) error.RestoreProjectionCorrupt else error.RelationalCheckViolation,
            };
        }
        if (!try self.restoreStagingIndexesReadyAssumeApply(alloc)) return if (marker != null or replication_lsn != null) error.RestoreProjectionCatchUpPending else error.IndexRebuilding;
        // Hidden physical indexes were created empty before import and
        // receive the same PreparedRow writes. Seal their durable state
        // before acknowledging this owner's completion receipt.
        try self.core.index_manager.syncAll(true);
        const raw_catalog = try missingAsNull(txn.get(relational_integrity_catalog.key));
        if (raw_catalog) |bytes| {
            var catalog = try relational_integrity_catalog.decode(alloc, bytes);
            defer catalog.deinit();
            try relational_integrity_activation.requireReady(&txn, catalog);
        }
    }
    next.phase = phase;
    const encoded = try next.encode(alloc);
    defer alloc.free(encoded);
    try txn.put(staging.key, encoded);
    try self.stageRestoreStagingReplicationOutbox(&txn, replication_payload);
    if (replication_lsn) |lsn| {
        var buffer: [replication_applied_lsn_value_len]u8 = undefined;
        const write = replicationAppliedSequenceWrite(lsn, &buffer);
        try txn.put(write.key, write.value);
    }
    if (marker) |entry| {
        var buffer: [ordered_apply_receipt_value_len]u8 = undefined;
        const write = orderedApplyReceiptWrite(entry, &buffer);
        try txn.put(write.key, write.value);
    }
    try txn.commit();
    txn_open = false;
    self.local_execution.restore_staging_required.store(next.phase != .published, .release);
    try self.core.store.sync(true);
    return next.receipt();
}

/// File-backed variant for CLI restore paths. Portable decoding uses
/// bounded positional reads over a locked source generation, so archive
/// size does not become resident memory.
/// Restore decoding and complete-image validation must use the store's
/// compilation-local callbacks. A foreign caller borrows only its executor.
pub fn importPortableRestoreFileIntoUnpublishedEmpty(
    self: *DB,
    alloc: Allocator,
    io: Io,
    file: std.Io.File,
    file_size: u64,
    options: storage_engine.PortableRestoreImportOptions,
) !void {
    if (comptime storage_engine.enabled) return storage_engine.call(@src().fn_name, .{ self, alloc, io, file, file_size, options });
    if (self.open_mode == .status_only or self.open_mode == .query_readonly) return error.UnsupportedOperation;
    lockApply(self);
    defer self.core.unlockApply();
    if (!(try self.portableImportTargetEmptyLocked(alloc))) return error.LiteImportTargetNotEmpty;
    try portable_backup.importPortableFileWithOptions(alloc, self.core.store, io, file, file_size, .{
        .identity_namespace = options.identity_namespace,
        .prefer_existing_identity_namespace = true,
        .import_derived_indexes = true,
        .embedding_source_fields = options.embedding_source_fields,
        .unpublished_staging = true,
        .cancellation = options.cancellation,
    });
    portable_backup.validateCompleteDatabaseImageAlloc(alloc, self.core.store) catch |err| switch (err) {
        error.OutOfMemory => return err,
        else => return error.InvalidBackupRequest,
    };
}

pub fn importPortableFileIntoUnpublishedEmpty(
    self: *DB,
    alloc: Allocator,
    io: Io,
    file: std.Io.File,
    file_size: u64,
    target_identity: doc_identity.Namespace,
) !void {
    if (comptime storage_engine.enabled) return storage_engine.call(@src().fn_name, .{ self, alloc, io, file, file_size, target_identity });
    return self.importPortableLogicalSourceFile(alloc, io, file, file_size, target_identity, null, .none);
}

pub fn importCohortPortableFileIntoUnpublishedEmpty(self: *DB, alloc: Allocator, io: Io, file: std.Io.File, file_size: u64, proof: portable_backup.CohortProof, cancellation: types.CancellationToken) !void {
    if (comptime storage_engine.enabled) return storage_engine.call(@src().fn_name, .{ self, alloc, io, file, file_size, proof, cancellation });
    return self.importPortableLogicalSourceFile(alloc, io, file, file_size, proof.namespace, proof, cancellation);
}

pub fn importPortableLogicalSourceFile(self: *DB, alloc: Allocator, io: Io, file: std.Io.File, file_size: u64, target_identity: doc_identity.Namespace, cohort: ?portable_backup.CohortProof, cancellation: types.CancellationToken) !void {
    if (self.open_mode == .status_only or self.open_mode == .query_readonly) return error.UnsupportedOperation;

    lockApply(self);
    defer self.core.unlockApply();
    if (!(try self.portableImportTargetEmptyLocked(alloc))) return error.LiteImportTargetNotEmpty;
    try portable_backup.importPortableFileWithOptions(alloc, self.core.store, io, file, file_size, .{
        .cohort = cohort,
        .cancellation = cancellation,
        .identity_namespace = target_identity,
        .prefer_existing_identity_namespace = true,
        .unpublished_staging = true,
    });
    portable_backup.validateCompleteDatabaseImageWithCohort(alloc, self.core.store, cohort) catch |err| switch (err) {
        error.OutOfMemory => return err,
        else => return error.InvalidBackupRequest,
    };
    try doc_identity.reassignNamespaceAlloc(alloc, self.core.store, target_identity);
    try self.refreshPortableImportedGenerationLocked(target_identity);
}

/// Atomically adopts a prepared portable generation into a pristine live
/// DB. The caller-provided publication step runs under the same exclusive
/// apply fence as the final emptiness check, closing the build-time race
/// with concurrent writers before runtime metadata is refreshed.
pub fn adoptPreparedPortableGenerationIfEmpty(
    self: *DB,
    alloc: Allocator,
    prepared_runtime: *PreparedPortableRuntimeMetadata,
    context: anytype,
    comptime adopt: anytype,
) !void {
    if (self.open_mode == .status_only or self.open_mode == .query_readonly) return error.UnsupportedOperation;
    lockApply(self);
    var apply_locked = true;
    defer if (apply_locked) self.core.unlockApply();
    if (!(try self.portableImportTargetEmptyLocked(alloc))) return error.LiteImportTargetNotEmpty;
    try self.core.store.beginPortableImportPublication(
        self.backend_runtime.io() orelse self.backend_runtime.filesystemIo() orelse std.Options.debug_io,
    );
    var publication_fenced = true;
    defer if (publication_fenced) self.core.store.finishPortableImportPublication();
    const publication = try adopt(context);
    self.applyPreparedPortableRuntimeMetadataLocked(prepared_runtime);
    // The durable generation and its schema now agree. Lock-free point
    // reads may resume; index-bearing operations remain behind the
    // activation gate while catalog workers are rebound.
    self.core.store.finishPortableImportPublication();
    publication_fenced = false;
    // Keep index-bearing operations closed while activation temporarily
    // releases the apply lock to join and restart resolver workers.
    self.async_context.portable_runtime_activation_pending.store(true, .release);
    self.core.unlockApply();
    apply_locked = false;
    // The rename above is the commit point. Index artifacts were fully
    // finalized in the disposable generation; a host-level allocation or
    // descriptor failure while rebinding the live catalog is degraded
    // runtime state, not a retryable import failure.
    self.activatePortableIndexesAfterPublication();
    if (publication == .durability_unknown) return error.DurabilityOutcomeUnknown;
}

/// Publish a portable archive into a pristine live DB as one store
/// generation. Expensive decoding and validation happen in an isolated
/// stage before the apply lock is acquired; the locked target check closes
/// the race with concurrent writers.
pub fn importPortableIntoEmpty(
    self: *DB,
    alloc: Allocator,
    backup: []const u8,
    target_identity: doc_identity.Namespace,
) !void {
    if (comptime storage_engine.enabled) return storage_engine.call(@src().fn_name, .{ self, alloc, backup, target_identity });
    if (self.open_mode == .status_only or self.open_mode == .query_readonly) return error.UnsupportedOperation;
    var stage = try PortableImportStage.init(alloc, self.core.path, backup, .{});
    defer stage.deinit();
    portable_backup.validateCompleteDatabaseImageAlloc(alloc, &stage.store) catch |err| switch (err) {
        error.OutOfMemory => return err,
        else => return error.InvalidBackupRequest,
    };
    try stage.reassignIdentityNamespace(alloc, target_identity);
    var prepared = try self.preparePortableRuntimeMetadata(&stage.store, target_identity);
    defer prepared.deinit();

    lockApply(self);
    var apply_locked = true;
    defer if (apply_locked) self.core.unlockApply();
    if (!(try self.portableImportTargetEmptyLocked(alloc))) return error.LiteImportTargetNotEmpty;
    try self.core.store.beginPortableImportPublication(
        self.backend_runtime.io() orelse self.backend_runtime.filesystemIo() orelse std.Options.debug_io,
    );
    var publication_fenced = true;
    defer if (publication_fenced) self.core.store.finishPortableImportPublication();
    try stage.publish(self.core.store);

    self.applyPreparedPortableRuntimeMetadataLocked(&prepared);
    self.core.store.finishPortableImportPublication();
    publication_fenced = false;
    self.async_context.portable_runtime_activation_pending.store(true, .release);
    self.core.unlockApply();
    apply_locked = false;
    self.activatePortableIndexesAfterPublication();
}

/// Refresh every schema-dependent runtime view after a portable restore
/// has replaced durable metadata beneath this already-open DB.
pub fn reloadSchemaForInternalRestore(self: *DB) !void {
    if (self.open_mode == .status_only) return error.UnsupportedOperation;
    lockApply(self);
    defer self.core.unlockApply();
    try self.core.reloadSchemaFromStore();
    try self.refreshRelationalRuntimeMode();
}

pub fn snapshotDocSetPlanningStats(self: *DB) types.DocSetPlanningStats {
    return self.local_execution.doc_set_planning_stats.snapshot();
}

pub fn snapshotVisibilityStats(self: *DB) types.VisibilityStats {
    return self.local_execution.visibility_runtime_stats.snapshot(self.core.identity_visibility.nonvisible_entries.load(.monotonic));
}

/// A complete embedded backup excludes native intents and captures only
/// the committed primary state. It need not wait for an open transaction
/// to finish, while still excluding primary and replay mutation races.
pub fn tryEmbeddedBackupReadFence(self: *DB) !?StatementReadFence {
    if (comptime storage_engine.enabled) return storage_engine.call(@src().fn_name, .{self});
    return self.tryPrimaryReadFence(true);
}

pub fn attachDistributedDocumentAuthorityAtSnapshot(self: *DB, alloc: Allocator, req: types.SearchRequest, result: *types.SearchResult, pinned: ?*docstore_mod.DocStore.Txn, schema_view: ?schema_registry_mod.SchemaView) !void {
    if (req.document_authority_mode == .none) return;
    if (req.document_output_allocator != null and req.document_authority_mode != .hydrate) return error.InvalidQueryRequest;
    const lookup_opts: types.LookupOptions = .{ .output_exact = req.document_output_exact, .fields = if (req.defer_stored_projection) &.{} else req.fields, .include_all_fields = req.defer_stored_projection or req.include_all_fields };
    var plans = try LookupProjectionPlans.init(alloc, lookup_opts);
    defer plans.deinit();
    var read: ?docstore_mod.DocStore.Txn = if (pinned) |captured_read| try captured_read.forkRead() else try self.core.store.beginReadTxn();
    defer if (read) |*owned| owned.abort();
    var metadata_scratch = std.heap.ArenaAllocator.init(platform.allocator.reclaimingScratch(alloc));
    defer metadata_scratch.deinit();
    var projection_scratch = std.heap.ArenaAllocator.init(platform.allocator.reclaimingScratch(alloc));
    defer projection_scratch.deinit();
    var primary_window = DocumentPrimaryReadWindow.init(alloc);
    defer primary_window.deinit();
    var physical: [64][]const u8 = undefined;
    var ids: [64][]const u8 = undefined;
    var contents: [64]?document_mutation_revision_mod.Content = undefined;
    var versions: [64]u64 = undefined;
    var mutation_revisions: [64]u64 = undefined;
    const body_allocator = req.document_output_allocator orelse alloc;
    if (req.document_output_allocator != null) result.stored_data_allocator = body_allocator;
    var eager_bytes: usize = 0;
    var primary_batch_end: usize = 0;
    var primary_values: [64]?[]const u8 = undefined;
    for (result.hits, 0..) |*hit, index| {
        if (req.cancellation) |token| try token.check();
        if (req.execution_deadline_ns) |deadline| if (platform_time.monotonicNs() >= deadline) return error.Timeout;
        // Apply-shared fences ordinary reads; captured dense reads fork
        // the original visibility cut. Retire page scratch every 64 hits.
        if (index != 0 and index % 64 == 0) {
            primary_window.retire();
            read.?.abort();
            read = null;
            read = if (pinned) |captured_read| try captured_read.forkRead() else try self.core.store.beginReadTxn();
        }
        if (index % 64 == 0) {
            if (!metadata_scratch.reset(.{ .retain_with_limit = 64 * 1024 })) _ = metadata_scratch.reset(.free_all);
            const m = metadata_scratch.allocator();
            const count = @min(@as(usize, 64), result.hits.len - index);
            for (result.hits[index..][0..count], 0..) |candidate, row| {
                ids[row] = candidate.id;
                physical[row] = try encodeStoreLookupKeyWithPinnedSchemaAlloc(self, m, candidate.id, schema_view);
            }
            var scope = try read.?.openReadScope(platform.allocator.reclaimingScratch(alloc));
            defer scope.close();
            try document_mutation_revision_mod.loadWindowWithRevisions(&scope, m, physical[0..count], ids[0..count], contents[0..count], versions[0..count], mutation_revisions[0..count]);
        }
        if (req.cancellation) |token| try token.check();
        if (req.execution_deadline_ns) |deadline| if (platform_time.monotonicNs() >= deadline) return error.Timeout;
        const store_key = physical[index % 64];
        const cached = contents[index % 64];
        var raw_primary: ?[]const u8 = null;
        defer {
            // Prefetched spans stay live until their last consumer retires.
            if (index >= primary_batch_end or index + 1 == primary_batch_end)
                primary_window.finishRow(cached == null);
        }
        var digest: [32]u8 = undefined;
        const primary_bytes = if (cached) |content| cached: {
            digest = content.digest;
            break :cached content.bytes;
        } else legacy: {
            const raw = primary_window.get(&read.?, store_key, null) catch |err| return if (err == error.NotFound) error.StorageReadTemporarilyUnavailable else err;
            raw_primary = raw;
            std.crypto.hash.sha2.Sha256.hash(raw, &digest, .{});
            const revision = mutation_revisions[index % 64];
            if (!self.document_authority_repairs.enqueue(self.alloc, store_key, .{ .revision = revision, .bytes = raw.len, .kind = store_key[store_key.len - 1], .digest = digest }))
                self.document_authority_backfill_requested.store(true, .release);
            break :legacy raw.len;
        };
        const version = versions[index % 64];
        const authority: types.document_authority.Authority = .{ .group = req.document_authority_group, .version = version, .digest = digest };
        // Small pages stay in one round trip; large candidates travel
        // as proofs until their global window has been selected.
        const eager = req.document_authority_mode == .candidates and primary_bytes <= 8 * 1024 and primary_bytes <= 64 * 1024 -| eager_bytes;
        if (raw_primary == null and (req.document_authority_mode == .hydrate or eager)) {
            if (index >= primary_batch_end) {
                const offset = index % 64;
                const count = @min(@as(usize, 64) - offset, result.hits.len - index);
                const byte_limit = if (req.document_authority_mode == .hydrate) DocumentPrimaryReadWindow.max_bytes else 64 * 1024 -| eager_bytes;
                var batch_count: usize = 0;
                var bytes: usize = 0;
                while (batch_count < count) : (batch_count += 1) {
                    const next = contents[offset + batch_count] orelse break;
                    if (req.document_authority_mode == .candidates and next.bytes > 8 * 1024) break;
                    if (next.bytes > byte_limit -| bytes) break;
                    bytes += @intCast(next.bytes);
                }
                if (batch_count > 1) {
                    try primary_window.getMany(&read.?, physical[offset..][0..batch_count], primary_values[offset..][0..batch_count]);
                    primary_batch_end = index + batch_count;
                }
            }
            raw_primary = if (index < primary_batch_end)
                primary_values[index % 64] orelse return error.StorageReadTemporarilyUnavailable
            else
                primary_window.get(&read.?, store_key, primary_bytes) catch |err| return if (err == error.NotFound) error.StorageReadTemporarilyUnavailable else err;
        }
        if (!projection_scratch.reset(.{ .retain_with_limit = 64 * 1024 })) _ = projection_scratch.reset(.free_all);
        var projected: ?types.LookupResult = if (req.document_authority_mode == .hydrate or eager)
            try self.lookupPrimarySnapshotWithPreparedProjection(projection_scratch.allocator(), body_allocator, hit.id, lookup_opts, &read.?, schema_view, raw_primary, version, &plans)
        else
            null;
        if (req.document_authority_mode == .candidates) if (projected) |*document| {
            // Charge actual projected JSON, including native rows whose
            // compact storage bytes can expand during serialization.
            if (document.json.len > 8 * 1024 or document.json.len > 64 * 1024 -| eager_bytes) {
                document.deinit(body_allocator);
                projected = null;
            } else eager_bytes += document.json.len;
        };
        defer if (projected) |*document| document.deinit(body_allocator);
        if (req.document_authority_mode == .hydrate and projected == null) return error.StorageReadTemporarilyUnavailable;
        const owned_authority = try alloc.create(types.document_authority.Authority);
        owned_authority.* = authority;
        errdefer alloc.destroy(owned_authority);
        // Projection already has final body ownership. No arena copy.
        const document_bytes = if (projected) |document| document.json else null;
        projected = null;
        if (hit.stored_data) |bytes| alloc.free(bytes);
        if (hit.source_value) |*value| types.deinitJsonValue(alloc, value);
        if (hit.column_source) |source| source.deinit();
        hit.source_value = null;
        hit.column_source = null;
        if (hit.document_authority) |proof| alloc.destroy(proof);
        hit.document_authority = owned_authority;
        hit.stored_data = document_bytes;
    }
    result.document_authority_version = 1;
}

/// Persist the fail-closed repair decision before reporting that the index
/// is rebuilding. The HBC marker is generation-scoped, so a failure from a
/// snapshot superseded before this callback arrived cannot quarantine the
/// current serving generation.
pub fn persistIncompleteHbcSnapshotRepair(
    self: *DB,
    entry: *index_manager_mod.IndexManager.DenseIndex,
) !bool {
    if (!entry.index.generationRepairPending()) return false;
    if (comptime builtin.os.tag != .freestanding) {
        _ = self.ensureAutomaticDenseGenerationRepairIntent(
            self.alloc,
            entry.config,
            .projection_generation_invalid,
            "dense_hbc_published_snapshot_incomplete",
        ) catch |err| switch (err) {
            error.DurableIndexRepairStateUnavailable => {
                // Embedded/Lite runtimes intentionally have no durable repair
                // owner. The query gate is already closed; their maintenance
                // pass performs the same synchronous shadow replacement used
                // for other automatic generation repairs.
                std.log.warn("incomplete dense index quarantined without durable repair owner name={s}", .{entry.config.name});
            },
            else => return err,
        };
    }
    return true;
}

test "portable activation failure still releases dense session bookkeeping" {
    var apply_mutex: WriterPreferringRwLock = .{};
    var ctx = AsyncContext{
        .alloc = std.testing.allocator,
        .store = undefined,
        .index_manager = undefined,
        .apply_mutex = &apply_mutex,
    };
    defer ctx.deinit(std.testing.allocator);

    try beginDenseCatchUpSessionTracked(&ctx, "vec");
    ctx.portable_runtime_activation_pending.store(true, .release);
    try std.testing.expectError(
        error.PortableRuntimeActivationPending,
        finishDenseCatchUpSessionTrackedAndFinalize(&ctx, "vec"),
    );
    try std.testing.expectEqual(@as(u32, 0), ctx.dense_admission.sessions.active.load(.acquire));
    try std.testing.expect(ctx.stats.dense_catch_up.active.load(.monotonic) == 0);
    try std.testing.expect(ctx.text_merge_deferred.load(.acquire));

    ctx.portable_runtime_activation_pending.store(false, .release);
    resumeDeferredBackgroundMaintenanceIfIdle(&ctx);
    try std.testing.expect(!ctx.text_merge_deferred.load(.acquire));

    try beginExternalDenseBulkSessionTracked(&ctx);
    ctx.portable_runtime_activation_pending.store(true, .release);
    try std.testing.expectError(
        error.PortableRuntimeActivationPending,
        finishExternalDenseBulkSessionTrackedAndFinalize(&ctx),
    );
    try std.testing.expectEqual(@as(u32, 0), ctx.dense_admission.external_sessions.load(.acquire));
    try std.testing.expect(ctx.text_merge_deferred.load(.acquire));

    ctx.portable_runtime_activation_pending.store(false, .release);
    resumeDeferredBackgroundMaintenanceIfIdle(&ctx);
    try std.testing.expect(!asyncContextHasActiveDenseBulkWork(&ctx));
    try std.testing.expect(!ctx.text_merge_deferred.load(.acquire));
}

test "db portable relational restore refreshes open runtime before returning" {
    const alloc = std.testing.allocator;
    var source_path_tmp = try TestDirectory.init("db");
    defer source_path_tmp.cleanup();
    const source_path = source_path_tmp.path().ptr;
    var target_path_tmp = try TestDirectory.init("db");
    defer target_path_tmp.cleanup();
    const target_path = target_path_tmp.path().ptr;
    const schema_json =
        \\{"version":1,"storage_mode":"relational","default_type":"row","enforce_types":true,"document_schemas":{"row":{"schema":{"type":"object","properties":{"id":{"type":"keyword"},"status":{"type":"keyword","enum":["active"]}},"required":["id","status"],"additionalProperties":false}}}}
    ;
    var portable = std.ArrayList(u8).empty;
    defer portable.deinit(alloc);

    {
        var source = try DB.open(alloc, std.mem.span(source_path), .{ .start_index_workers = false });
        defer source.close();
        try source.setSchemaJson(alloc, schema_json);
        try source.batch(.{ .writes = &.{.{
            .key = "row:a",
            .value = "{\"id\":\"a\",\"status\":\"active\"}",
        }} });
        try portable_backup.exportPortable(alloc, source.core.store, &portable);
    }

    var target = try DB.open(alloc, std.mem.span(target_path), .{ .start_index_workers = false });
    defer target.close();
    target.core.identity_visibility.summary = .{
        .live_ordinals = 99,
        .tombstone_ordinals = 7,
        .max_created_generation = 999,
    };
    target.core.identity_visibility.live_generation = 999;
    target.core.identity_visibility.nonvisible_generation = 999;
    target.core.identity_visibility.nonvisible_overflow = true;
    target.core.identity_visibility.nonvisible_entries.store(7, .monotonic);
    try target.importPortableIntoEmpty(alloc, portable.items, doc_identity.default_namespace);

    const restored = (try target.get(alloc, "row:a")) orelse return error.TestExpectedEqual;
    defer alloc.free(restored);
    try std.testing.expectEqualStrings("{\"id\":\"a\",\"status\":\"active\"}", restored);
    const restored_summary = target.core.identity_visibility.summary orelse return error.TestExpectedEqual;
    try std.testing.expectEqual(@as(u64, 1), restored_summary.live_ordinals);
    try std.testing.expectEqual(@as(u64, 0), restored_summary.tombstone_ordinals);
    try std.testing.expectEqual(@as(?u64, null), target.core.identity_visibility.live_generation);
    try std.testing.expectEqual(@as(?u64, null), target.core.identity_visibility.nonvisible_generation);
    try std.testing.expect(!target.core.identity_visibility.nonvisible_overflow);
    try std.testing.expectEqual(@as(u64, 0), target.core.identity_visibility.nonvisible_entries.load(.monotonic));
    try std.testing.expectError(error.InvalidBatchRequest, target.batch(.{ .writes = &.{.{
        .key = "row:invalid",
        .value = "{\"id\":\"invalid\",\"status\":\"inactive\"}",
    }} }));
}

test "db portable import target rejects active in-memory bulk leases" {
    const alloc = std.testing.allocator;
    var path_tmp = try TestDirectory.init("db");
    defer path_tmp.cleanup();
    const path = path_tmp.path().ptr;

    var db = try DB.open(alloc, std.mem.span(path), .{
        .start_index_workers = false,
        .start_optional_runtimes = false,
        .start_optional_runtime_workers = false,
        .ttl_cleanup = .{ .enabled = false },
    });
    defer db.close();
    try std.testing.expect(try db.isPortableImportTargetEmpty(alloc));

    try db.beginPrimaryStoreAutoBulkIngestSession();
    try std.testing.expectError(error.WriterLocked, db.isPortableImportTargetEmpty(alloc));
    db.abortPrimaryStoreAutoBulkIngestSession();
    try std.testing.expect(try db.isPortableImportTargetEmpty(alloc));

    db.async_context.portable_runtime_activation_pending.store(true, .release);
    try std.testing.expectError(error.PortableRuntimeActivationPending, db.beginBulkIngestSession());
    try std.testing.expectEqual(@as(u32, 0), db.async_context.dense_admission.external_sessions.load(.acquire));
    try std.testing.expect(!db.bulk_ingest_session.active);
    db.async_context.portable_runtime_activation_pending.store(false, .release);

    try db.beginBulkIngestSession();
    try std.testing.expectError(error.WriterLocked, db.isPortableImportTargetEmpty(alloc));
    db.abortBulkIngestSession();
    try std.testing.expect(try db.isPortableImportTargetEmpty(alloc));

    try db.batch(.{ .writes = &.{.{
        .key = "doc:durable-target-state",
        .value = "{\"title\":\"permanently nonempty\"}",
    }} });
    try db.beginPrimaryStoreAutoBulkIngestSession();
    defer db.abortPrimaryStoreAutoBulkIngestSession();
    // Permanent target state takes precedence over transient activity so API
    // clients receive invalid_argument rather than entering a futile retry loop.
    try std.testing.expect(!(try db.isPortableImportTargetEmpty(alloc)));
}

test "db portable publication rollback preserves target routing metadata" {
    const alloc = std.testing.allocator;
    var source_path_tmp = try TestDirectory.init("db");
    defer source_path_tmp.cleanup();
    const source_path = source_path_tmp.path().ptr;
    var target_path_tmp = try TestDirectory.init("db");
    defer target_path_tmp.cleanup();
    const target_path = target_path_tmp.path().ptr;
    var portable = std.ArrayList(u8).empty;
    defer portable.deinit(alloc);

    {
        var source = try DB.open(alloc, std.mem.span(source_path), .{ .start_optional_runtimes = false });
        defer source.close();
        try source.batch(.{ .writes = &.{.{
            .key = "doc:portable-rollback",
            .value = "{\"title\":\"must be rolled back\"}",
        }} });
        try portable_backup.exportPortable(alloc, source.core.store, &portable);
    }

    var target = try DB.open(alloc, std.mem.span(target_path), .{ .start_optional_runtimes = false });
    defer target.close();
    const target_identity: doc_identity.Namespace = .{ .table_id = 41, .shard_id = 7, .range_id = 3 };
    try target.reassignIdentityNamespaceForInternalTransition(target_identity);
    try target.updateRange(.{ .start = "doc:a", .end = "doc:z" });
    const range_before = try target.core.store.get(alloc, range_state_mod.range_key);
    defer alloc.free(range_before);

    failNextPortableImportPublishForTest();
    try std.testing.expectError(
        error.InjectedPortableImportPublishFailure,
        target.importPortableIntoEmpty(alloc, portable.items, target_identity),
    );
    try std.testing.expect(!target.portableImportRecoveryRequired());
    try std.testing.expect(!target.pendingWorkStats().portable_import_publication_in_progress);
    try std.testing.expect((try target.get(alloc, "doc:portable-rollback")) == null);
    const range_after = try target.core.store.get(alloc, range_state_mod.range_key);
    defer alloc.free(range_after);
    try std.testing.expectEqualSlices(u8, range_before, range_after);
    const identity_after = (try doc_identity.loadNamespaceFromStore(target.core.store)) orelse return error.TestExpectedEqual;
    try std.testing.expect(identity_after.eql(target_identity));
}

test "db portable committed sync failure retains rollback authority" {
    const alloc = std.testing.allocator;
    var source_path_tmp = try TestDirectory.init("db");
    defer source_path_tmp.cleanup();
    const source_path = source_path_tmp.path().ptr;
    var target_path_tmp = try TestDirectory.init("db");
    defer target_path_tmp.cleanup();
    const target_path = target_path_tmp.path().ptr;
    var portable = std.ArrayList(u8).empty;
    defer portable.deinit(alloc);

    {
        var source = try DB.open(alloc, std.mem.span(source_path), .{ .start_optional_runtimes = false });
        defer source.close();
        try source.batch(.{ .writes = &.{.{
            .key = "doc:portable-commit-sync",
            .value = "{\"title\":\"must be rolled back\"}",
        }} });
        try portable_backup.exportPortable(alloc, source.core.store, &portable);
    }

    var target = try DB.open(alloc, std.mem.span(target_path), .{ .start_optional_runtimes = false });
    defer target.close();
    const target_identity: doc_identity.Namespace = .{ .table_id = 43, .shard_id = 11, .range_id = 5 };
    try target.reassignIdentityNamespaceForInternalTransition(target_identity);
    try target.updateRange(.{ .start = "doc:b", .end = "doc:y" });
    const range_before = try target.core.store.get(alloc, range_state_mod.range_key);
    defer alloc.free(range_before);

    failNextPortableImportCommittedSyncForTest();
    try std.testing.expectError(
        error.InjectedPortableImportCommittedSyncFailure,
        target.importPortableIntoEmpty(alloc, portable.items, target_identity),
    );
    try std.testing.expect(!target.portableImportRecoveryRequired());
    try std.testing.expect((try target.get(alloc, "doc:portable-commit-sync")) == null);
    const range_after = try target.core.store.get(alloc, range_state_mod.range_key);
    defer alloc.free(range_after);
    try std.testing.expectEqualSlices(u8, range_before, range_after);
    const identity_after = (try doc_identity.loadNamespaceFromStore(target.core.store)) orelse return error.TestExpectedEqual;
    try std.testing.expect(identity_after.eql(target_identity));
    try std.testing.expectError(error.NotFound, target.core.store.get(alloc, portable_import_in_progress_key));
    try std.testing.expectError(error.NotFound, target.core.store.get(alloc, portable_import_identity_baseline_key));
}

test "db portable publication fence blocks lock-free point reads" {
    const alloc = std.testing.allocator;
    var path_tmp = try TestDirectory.init("db");
    defer path_tmp.cleanup();
    const path = path_tmp.path().ptr;

    var db = try DB.open(alloc, std.mem.span(path), .{ .start_optional_runtimes = false });
    defer db.close();
    try db.core.store.beginPortableImportPublication(std.Options.debug_io);
    var fenced = true;
    defer if (fenced) db.core.store.finishPortableImportPublication();

    try std.testing.expect(db.pendingWorkStats().portable_import_publication_in_progress);
    try std.testing.expectError(error.PortableImportPublicationInProgress, db.get(alloc, "doc:any"));
    try std.testing.expectError(error.PortableImportPublicationInProgress, db.core.store.beginReadTxn());

    db.core.store.finishPortableImportPublication();
    fenced = false;
    try std.testing.expect(!db.pendingWorkStats().portable_import_publication_in_progress);
    try std.testing.expect((try db.get(alloc, "doc:any")) == null);
}

test "portable runtime lock helpers reject degraded work before lock admission" {
    const alloc = std.testing.allocator;
    var path_tmp = try TestDirectory.init("db");
    defer path_tmp.cleanup();
    const path = path_tmp.path().ptr;

    var db = try DB.open(alloc, std.mem.span(path), .{
        .start_index_workers = false,
        .start_optional_runtimes = false,
        .start_optional_runtime_workers = false,
        .ttl_cleanup = .{ .enabled = false },
    });
    defer db.close();

    db.async_context.portable_runtime_activation_pending.store(true, .release);
    defer db.async_context.portable_runtime_activation_pending.store(false, .release);
    const before = db.snapshotApplyLockStats();

    try std.testing.expectError(error.PortableRuntimeActivationPending, db.lockApplyForPortableRuntime());
    try std.testing.expectError(error.PortableRuntimeActivationPending, db.lockApplySharedForPortableRuntime());
    var batch_ctx = db.batchContext();
    try std.testing.expectError(error.PortableRuntimeActivationPending, lockApplyForPortableRuntimeContext(&batch_ctx));
    try std.testing.expectError(error.PortableRuntimeActivationPending, lockApplySharedForPortableRuntimeAsync(db.async_context));
    try std.testing.expectError(error.PortableRuntimeActivationPending, lockApplyForPortableRuntimeAsync(db.async_context));

    const after = db.snapshotApplyLockStats();
    try std.testing.expectEqual(before.exclusive_lock_calls, after.exclusive_lock_calls);
    try std.testing.expectEqual(before.shared_lock_calls, after.shared_lock_calls);
}

test "db portable publication waits for admitted readers and rejects new ones" {
    if (builtin.single_threaded or builtin.os.tag == .freestanding) return error.SkipZigTest;

    const alloc = std.testing.allocator;
    var path_tmp = try TestDirectory.init("db");
    defer path_tmp.cleanup();
    const path = path_tmp.path().ptr;

    var db = try DB.open(alloc, std.mem.span(path), .{ .start_optional_runtimes = false });
    defer db.close();
    var admitted = try db.core.store.beginProbeTxn();
    var admitted_open = true;
    defer if (admitted_open) admitted.abort();

    const Publisher = struct {
        store: *docstore_mod.DocStore,
        io: std.Io,
        returned: std.atomic.Value(bool) = .init(false),
        release: std.atomic.Value(bool) = .init(false),
        err: ?anyerror = null,

        fn run(self: *@This()) void {
            self.store.beginPortableImportPublication(self.io) catch |err| {
                self.err = err;
                self.returned.store(true, .release);
                return;
            };
            self.returned.store(true, .release);
            while (!self.release.load(.acquire)) platform.time.yieldNow();
            self.store.finishPortableImportPublication();
        }
    };
    var publisher = Publisher{ .store = db.core.store, .io = std.Options.debug_io };
    const thread = try std.Thread.spawn(.{}, Publisher.run, .{&publisher});
    var joined = false;
    defer if (!joined) {
        if (admitted_open) {
            admitted.abort();
            admitted_open = false;
        }
        publisher.release.store(true, .release);
        thread.join();
    };

    const close_deadline = monotonicTimeNs() + std.time.ns_per_s;
    while (!db.core.store.portableImportPublicationInProgress() and monotonicTimeNs() < close_deadline) {
        platform.time.yieldNow();
    }
    try std.testing.expect(db.core.store.portableImportPublicationInProgress());
    try std.testing.expect(!publisher.returned.load(.acquire));
    try std.testing.expectError(error.PortableImportPublicationInProgress, db.core.store.beginProbeTxn());

    admitted.abort();
    admitted_open = false;
    const drain_deadline = monotonicTimeNs() + std.time.ns_per_s;
    while (!publisher.returned.load(.acquire) and monotonicTimeNs() < drain_deadline) {
        platform.time.yieldNow();
    }
    try std.testing.expect(publisher.returned.load(.acquire));
    publisher.release.store(true, .release);
    thread.join();
    joined = true;
    try std.testing.expectEqual(@as(?anyerror, null), publisher.err);
    try std.testing.expect(!db.core.store.portableImportPublicationInProgress());
}

test "db portable activation gate revalidates queued and replicated writes" {
    if (builtin.single_threaded or builtin.os.tag == .freestanding) return error.SkipZigTest;

    const alloc = std.testing.allocator;
    var path_tmp = try TestDirectory.init("db");
    defer path_tmp.cleanup();
    const path = path_tmp.path().ptr;

    var db = try DB.open(alloc, std.mem.span(path), .{ .start_optional_runtimes = false });
    defer db.close();

    var hook = PortableRuntimeBatchPrelockTestHook{};
    db_zig.test_portable_runtime_batch_prelock_hook = &hook;
    defer {
        hook.release.store(true, .release);
        db_zig.test_portable_runtime_batch_prelock_hook = null;
        db.async_context.portable_runtime_activation_pending.store(false, .release);
    }

    const Writer = struct {
        db: *DB,
        err: ?anyerror = null,

        fn run(self: *@This()) void {
            self.db.batch(.{ .writes = &.{.{
                .key = "doc:queued-before-portable-publication",
                .value = "{\"title\":\"must remain blocked\"}",
            }} }) catch |err| {
                self.err = err;
            };
        }
    };
    var writer = Writer{ .db = &db };
    const writer_thread = try std.Thread.spawn(.{}, Writer.run, .{&writer});
    var writer_joined = false;
    defer if (!writer_joined) writer_thread.join();

    const prelock_deadline = monotonicTimeNs() + std.time.ns_per_s;
    while (!hook.entered.load(.acquire) and monotonicTimeNs() < prelock_deadline) {
        platform.time.yieldNow();
    }
    try std.testing.expect(hook.entered.load(.acquire));

    // Model the publication transition after the fast preflight but before the
    // queued writer owns apply. The under-lock check must reject it.
    db.async_context.portable_runtime_activation_pending.store(true, .release);
    hook.release.store(true, .release);
    writer_thread.join();
    writer_joined = true;
    try std.testing.expectEqual(@as(?anyerror, error.PortableRuntimeActivationPending), writer.err);
    try std.testing.expect((try db.lookup(alloc, "doc:queued-before-portable-publication", .{})) == null);

    // Replicated apply bypasses hot-standby role admission only; portable activation is
    // a generation-safety fence and therefore remains mandatory.
    try std.testing.expectError(error.PortableRuntimeActivationPending, db.batchReplicatedApply(.{
        .writes = &.{.{
            .key = "doc:replicated-during-portable-activation",
            .value = "{\"title\":\"must remain blocked\"}",
        }},
    }));

    // Stable runtime callback contexts share the same gate rather than keeping
    // a pointer into DB.open's movable wrapper value.
    try std.testing.expectError(
        error.PortableRuntimeActivationPending,
        appendDerivedBatchRecord(&db, .{}),
    );
}

test "db portable activation gate revalidates queued graph reads" {
    if (builtin.single_threaded or builtin.os.tag == .freestanding) return error.SkipZigTest;

    const alloc = std.testing.allocator;
    var path_tmp = try TestDirectory.init("db");
    defer path_tmp.cleanup();
    const path = path_tmp.path().ptr;

    var db = try DB.open(alloc, std.mem.span(path), .{ .start_optional_runtimes = false });
    defer db.close();
    db.core.lockApplyExclusive();
    var apply_locked = true;
    defer if (apply_locked) db.core.unlockApplyExclusive();

    const Reader = struct {
        db: *DB,
        entered: std.atomic.Value(bool) = .init(false),
        err: ?anyerror = null,

        fn run(self: *@This()) void {
            self.entered.store(true, .release);
            const edges = self.db.getEdges(std.heap.page_allocator, "restored_graph", "doc:a", "rel", .out) catch |err| {
                self.err = err;
                return;
            };
            graph_mod.GraphIndex.freeEdges(std.heap.page_allocator, edges);
        }
    };
    var reader = Reader{ .db = &db };
    const reader_thread = try std.Thread.spawn(.{}, Reader.run, .{&reader});
    var reader_joined = false;
    defer if (!reader_joined) reader_thread.join();

    const entered_deadline = monotonicTimeNs() + std.time.ns_per_s;
    while (!reader.entered.load(.acquire) and monotonicTimeNs() < entered_deadline) {
        platform.time.yieldNow();
    }
    try std.testing.expect(reader.entered.load(.acquire));
    db.async_context.portable_runtime_activation_pending.store(true, .release);
    defer db.async_context.portable_runtime_activation_pending.store(false, .release);
    db.core.unlockApplyExclusive();
    apply_locked = false;

    reader_thread.join();
    reader_joined = true;
    try std.testing.expectEqual(@as(?anyerror, error.PortableRuntimeActivationPending), reader.err);
}

test "db portable activation gate revalidates profiled dense search after catalog admission" {
    if (builtin.single_threaded or builtin.os.tag == .freestanding) return error.SkipZigTest;

    const alloc = std.testing.allocator;
    var io_impl = std.Io.Threaded.init(alloc, .{});
    defer io_impl.deinit();
    const io = io_impl.io();
    var path_tmp = try TestDirectory.init("db");
    defer path_tmp.cleanup();
    const path = path_tmp.path().ptr;

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

    var lookup_hook = PublishedDenseCatalogLookupTestHook{ .io = io };
    db_zig.test_published_dense_catalog_lookup_hook = &lookup_hook;
    defer {
        lookup_hook.release.set(io);
        db_zig.test_published_dense_catalog_lookup_hook = null;
        db.async_context.portable_runtime_activation_pending.store(false, .release);
    }

    const Searcher = struct {
        db: *DB,
        err: ?anyerror = null,

        fn run(searcher: *@This()) void {
            var profiled = searcher.db.searchDenseProfiled(std.heap.page_allocator, .{
                .index_name = "dense_idx",
                .limit = 1,
                .include_stored = false,
                .query = .{ .dense_knn = .{ .vector = &.{ 0.0, 0.0 }, .k = 1 } },
            }, .{ .vector = &.{ 0.0, 0.0 }, .k = 1 }) catch |err| {
                searcher.err = err;
                return;
            };
            profiled.result.deinit();
        }
    };
    var searcher = Searcher{ .db = &db };
    const search_thread = try std.Thread.spawn(.{}, Searcher.run, .{&searcher});
    var search_joined = false;
    defer if (!search_joined) {
        lookup_hook.release.set(io);
        search_thread.join();
    };

    const entered_deadline = monotonicTimeNs() + 5 * std.time.ns_per_s;
    while (!lookup_hook.entered.isSet() and monotonicTimeNs() < entered_deadline) {
        try io.sleep(std.Io.Duration.fromMilliseconds(1), .awake);
    }
    try std.testing.expect(lookup_hook.entered.isSet());
    try std.testing.expectEqual(@as(u32, 1), db.publishedDenseSearchCount());

    // Model publication after the optimistic retry but before the catalog
    // lease is returned to the caller. The post-admission check must reject
    // the mixed generation rather than executing against it.
    db.async_context.portable_runtime_activation_pending.store(true, .release);
    lookup_hook.release.set(io);
    search_thread.join();
    search_joined = true;
    try std.testing.expectEqual(@as(?anyerror, error.PortableRuntimeActivationPending), searcher.err);
    try std.testing.expectEqual(@as(u32, 0), db.publishedDenseSearchCount());
}

test "portable activation retry backoff is bounded stable and exponential" {
    const first = DB.portableActivationRetryDelayNs("/tmp/db-a", 0x1234, 0);
    const second = DB.portableActivationRetryDelayNs("/tmp/db-a", 0x1234, 1);
    const capped = DB.portableActivationRetryDelayNs("/tmp/db-a", 0x1234, 100);

    try std.testing.expect(first >= DB.portable_activation_retry_base_ns * 4 / 5);
    try std.testing.expect(first <= DB.portable_activation_retry_base_ns);
    try std.testing.expect(second >= DB.portable_activation_retry_base_ns * 2 * 4 / 5);
    try std.testing.expect(second <= DB.portable_activation_retry_base_ns * 2);
    try std.testing.expect(second > first);
    try std.testing.expect(capped >= DB.portable_activation_retry_max_ns * 4 / 5);
    try std.testing.expect(capped <= DB.portable_activation_retry_max_ns);
    try std.testing.expectEqual(capped, DB.portableActivationRetryDelayNs("/tmp/db-a", 0x1234, 100));
    try std.testing.expect(capped != DB.portableActivationRetryDelayNs("/tmp/db-a", 0x5678, 100));
}

test "portable activation retry runtime job can be restarted" {
    if (builtin.single_threaded or builtin.os.tag == .freestanding) return error.SkipZigTest;

    const alloc = std.testing.allocator;
    var path_tmp = try TestDirectory.init("db");
    defer path_tmp.cleanup();
    const path = path_tmp.path().ptr;

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
        _ = lockAtomic(&db.portable_activation_recovery.lifecycle_mutex);
        db.portable_activation_recovery.stopping.store(false, .release);
        db.portable_activation_recovery.worker_running.store(true, .release);
        const launch = db.launchPortableActivationRetryWorkerLocked() catch |err| {
            db.portable_activation_recovery.worker_running.store(false, .release);
            db.portable_activation_recovery.lifecycle_mutex.unlock();
            return err;
        };
        db.portable_activation_recovery.lifecycle_mutex.unlock();
        try std.testing.expectEqual(portable_activation_recovery.Owner.PortableActivationRetryLaunch.backend_runtime, launch);

        const exit_deadline = monotonicTimeNs() + std.time.ns_per_s;
        while (db.portable_activation_recovery.worker_running.load(.acquire) and monotonicTimeNs() < exit_deadline) {
            platform.time.yieldNow();
        }
        try std.testing.expect(!db.portable_activation_recovery.worker_running.load(.acquire));
    }
}

test "portable activation retry stop joins the runtime worker final handshake" {
    if (builtin.single_threaded or builtin.os.tag == .freestanding) return error.SkipZigTest;

    const alloc = std.testing.allocator;
    var path_tmp = try TestDirectory.init("db");
    defer path_tmp.cleanup();
    const path = path_tmp.path().ptr;

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
            while (!ctx.db.portable_activation_recovery.stopping.load(.acquire)) platform.time.yieldNow();
            _ = lockAtomic(&ctx.db.portable_activation_recovery.lifecycle_mutex);
            ctx.db.portable_activation_recovery.worker_running.store(false, .release);
            ctx.holding_final_lock.store(true, .release);
            while (!ctx.release_final_lock.load(.acquire)) platform.time.yieldNow();
            ctx.db.portable_activation_recovery.lifecycle_mutex.unlock();
        }
    };
    const StopCtx = struct {
        db: *DB,
        returned: *std.atomic.Value(bool),

        fn run(ctx: @This()) void {
            ctx.db.stopPortableActivationRetryWorker();
            ctx.returned.store(true, .release);
        }
    };

    var holding_final_lock = std.atomic.Value(bool).init(false);
    var release_final_lock = std.atomic.Value(bool).init(false);
    var stop_returned = std.atomic.Value(bool).init(false);
    db.portable_activation_recovery.stopping.store(false, .release);
    db.portable_activation_recovery.worker_running.store(true, .release);

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

    const hold_deadline = monotonicTimeNs() + std.time.ns_per_s;
    while (!holding_final_lock.load(.acquire) and monotonicTimeNs() < hold_deadline) {
        platform.time.yieldNow();
    }
    try std.testing.expect(holding_final_lock.load(.acquire));
    // Give the stopper ample time to observe the published false state. It
    // must still wait for the lifecycle lock that fences the worker's return.
    sleepNs(10 * std.time.ns_per_ms);
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
    var path_tmp = try TestDirectory.init("db");
    defer path_tmp.cleanup();
    const path = path_tmp.path().ptr;

    var db = try DB.open(alloc, std.mem.span(path), .{
        .start_index_workers = false,
        .start_optional_runtimes = false,
        .start_optional_runtime_workers = false,
        .ttl_cleanup = .{ .enabled = false },
    });

    db_zig.test_fail_portable_activation_retry_fallback_submit.store(true, .release);
    defer db_zig.test_fail_portable_activation_retry_fallback_submit.store(false, .release);
    defer db_zig.test_pause_portable_activation_retry_probe_before_lifecycle_lock.store(false, .release);
    defer db_zig.test_portable_activation_retry_probe_paused.store(false, .release);
    defer db_zig.test_release_portable_activation_retry_probe.store(true, .release);

    db.async_context.portable_runtime_activation_pending.store(true, .release);
    db.startPortableActivationRetryWorkerIfNeededInner(true);
    try std.testing.expect(db.portable_activation_recovery.next_launch_ns > 0);

    // Rearm the injected launch failures. The claimed probe must leave these
    // untouched after close publishes the permanent stop state.
    db_zig.test_fail_portable_activation_retry_fallback_submit.store(true, .release);
    db.portable_activation_recovery.next_launch_ns = 0;
    db_zig.test_release_portable_activation_retry_probe.store(false, .release);
    db_zig.test_portable_activation_retry_probe_paused.store(false, .release);
    db_zig.test_pause_portable_activation_retry_probe_before_lifecycle_lock.store(true, .release);

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
        db_zig.test_release_portable_activation_retry_probe.store(true, .release);
        poller.join();
    };

    const pause_deadline = monotonicTimeNs() + std.time.ns_per_s;
    while (!db_zig.test_portable_activation_retry_probe_paused.load(.acquire) and monotonicTimeNs() < pause_deadline) {
        platform.time.yieldNow();
    }
    try std.testing.expect(db_zig.test_portable_activation_retry_probe_paused.load(.acquire));

    var closer = try std.Thread.spawn(.{}, CloseCtx.run, .{CloseCtx{
        .db = &db,
        .returned = &close_returned,
    }});
    var closer_joined = false;
    defer if (!closer_joined) {
        db_zig.test_release_portable_activation_retry_probe.store(true, .release);
        closer.join();
    };

    const stop_deadline = monotonicTimeNs() + std.time.ns_per_s;
    while (!db.portable_activation_recovery.stopping.load(.acquire) and monotonicTimeNs() < stop_deadline) {
        platform.time.yieldNow();
    }
    try std.testing.expect(db.portable_activation_recovery.stopping.load(.acquire));
    try std.testing.expect(!close_returned.load(.acquire));

    db_zig.test_release_portable_activation_retry_probe.store(true, .release);
    poller.join();
    poller_joined = true;
    closer.join();
    closer_joined = true;

    try std.testing.expect(poll_completed.load(.acquire));
    try std.testing.expect(close_returned.load(.acquire));
    try std.testing.expect(db_zig.test_fail_portable_activation_retry_fallback_submit.load(.acquire));
}

test "portable activation retry uses owner scoped runtime" {
    if (builtin.single_threaded or builtin.os.tag == .freestanding) return error.SkipZigTest;

    const alloc = std.testing.allocator;
    var path_tmp = try TestDirectory.init("db");
    defer path_tmp.cleanup();
    const path = path_tmp.path().ptr;

    var db = try DB.open(alloc, std.mem.span(path), .{
        .start_index_workers = false,
        .start_optional_runtimes = false,
        .start_optional_runtime_workers = false,
        .ttl_cleanup = .{ .enabled = false },
    });
    defer db.close();

    db.async_context.portable_runtime_activation_pending.store(true, .release);
    _ = lockAtomic(&db.portable_activation_recovery.lifecycle_mutex);
    db.portable_activation_recovery.stopping.store(false, .release);
    db.portable_activation_recovery.worker_running.store(true, .release);
    const launch = db.launchPortableActivationRetryWorkerLocked() catch |err| {
        db.portable_activation_recovery.worker_running.store(false, .release);
        db.portable_activation_recovery.lifecycle_mutex.unlock();
        return err;
    };
    db.portable_activation_recovery.lifecycle_mutex.unlock();
    try std.testing.expectEqual(portable_activation_recovery.Owner.PortableActivationRetryLaunch.backend_runtime, launch);

    const recovery_deadline = monotonicTimeNs() + 5 * std.time.ns_per_s;
    while ((db.async_context.portable_runtime_activation_pending.load(.acquire) or
        db.portable_activation_recovery.worker_running.load(.acquire)) and
        monotonicTimeNs() < recovery_deadline)
    {
        platform.time.yieldNow();
    }
    try std.testing.expect(!db.async_context.portable_runtime_activation_pending.load(.acquire));
    try std.testing.expect(!db.portable_activation_recovery.worker_running.load(.acquire));
    try std.testing.expectEqual(@as(u64, 1), db.portable_runtime_activation_attempts.load(.monotonic));
}

test "portable activation retry automatically rearms after runtime submission fails" {
    if (builtin.single_threaded or builtin.os.tag == .freestanding) return error.SkipZigTest;

    const alloc = std.testing.allocator;
    var path_tmp = try TestDirectory.init("db");
    defer path_tmp.cleanup();
    const path = path_tmp.path().ptr;

    var db = try DB.open(alloc, std.mem.span(path), .{
        .start_index_workers = false,
        .start_optional_runtimes = false,
        .start_optional_runtime_workers = false,
        .ttl_cleanup = .{ .enabled = false },
    });
    defer db.close();

    db.async_context.portable_runtime_activation_pending.store(true, .release);
    db_zig.test_fail_portable_activation_retry_fallback_submit.store(true, .release);
    defer db_zig.test_fail_portable_activation_retry_fallback_submit.store(false, .release);

    db.startPortableActivationRetryWorkerIfNeededInner(true);
    try std.testing.expect(!db.portable_activation_recovery.worker_running.load(.acquire));
    try std.testing.expectEqual(@as(u32, 1), db.portable_activation_recovery.launch_failure_streak);
    try std.testing.expect(db.portable_activation_recovery.next_launch_ns > 0);

    const recovery_deadline = monotonicTimeNs() + 5 * std.time.ns_per_s;
    while ((db.async_context.portable_runtime_activation_pending.load(.acquire) or
        db.portable_activation_recovery.worker_running.load(.acquire)) and
        monotonicTimeNs() < recovery_deadline)
    {
        sleepNs(std.time.ns_per_ms);
    }
    try std.testing.expect(!db.async_context.portable_runtime_activation_pending.load(.acquire));
    try std.testing.expect(!db.portable_activation_recovery.worker_running.load(.acquire));
    try std.testing.expectEqual(@as(u32, 0), db.portable_activation_recovery.launch_failure_streak);
    try std.testing.expectEqual(@as(u64, 0), db.portable_activation_recovery.next_launch_ns);
    try std.testing.expectEqual(@as(u64, 1), db.portable_runtime_activation_attempts.load(.monotonic));
}

test "db portable resolver activation stops partial startup before retry reload" {
    antfly_test_error_logs.expectErrorLogs(1);
    const alloc = std.testing.allocator;
    var source_path_tmp = try TestDirectory.init("db");
    defer source_path_tmp.cleanup();
    const source_path = source_path_tmp.path().ptr;
    var target_path_tmp = try TestDirectory.init("db");
    defer target_path_tmp.cleanup();
    const target_path = target_path_tmp.path().ptr;
    var portable = std.ArrayList(u8).empty;
    defer portable.deinit(alloc);

    {
        var source = try DB.open(alloc, std.mem.span(source_path), .{});
        defer source.close();
        try source.addIndex(.{
            .name = "relations_graph",
            .kind = .graph,
            .config_json =
            \\{
            \\  "source":{"artifact":"relations_v1","path":"$.relations[*]","format":"extraction_relation"},
            \\  "artifact":{"name":"relations_v1","kind":"asset","source":{"type":"field","value":"relations"},"content_type":"application/json"}
            \\}
            ,
        });
        try source.addResolver(.{
            .name = "kg_restore",
            .table = "entities",
            .source_artifact = "relations_v1",
            .resolution_artifact = "resolution_restore_v1",
            .key_template = "{{ lower _entity.label }}/{{ slug _entity.text }}",
            .config_generation = 1,
        });
        try portable_backup.exportPortable(alloc, source.core.store, &portable);
    }

    var target = try DB.open(alloc, std.mem.span(target_path), .{});
    defer target.close();
    failNextPortablePromotionRuntimeStartForTest();
    try target.importPortableIntoEmpty(alloc, portable.items, doc_identity.default_namespace);

    try std.testing.expect(target.pendingWorkStats().portable_runtime_activation_pending);
    try std.testing.expect(!target.resolution_runtime.?.worker_started.load(.acquire));
    try std.testing.expect(!target.promotion_runtime.?.worker_started.load(.acquire));

    try std.testing.expect(try target.retryPortableRuntimeActivationIfNeeded());
    try std.testing.expect(!target.pendingWorkStats().portable_runtime_activation_pending);
    try std.testing.expect(target.resolution_runtime.?.worker_started.load(.acquire));
    try std.testing.expect(target.promotion_runtime.?.worker_started.load(.acquire));
}

test "db portable restore rejects documents without identity coverage" {
    const alloc = std.testing.allocator;
    var backend = mem_backend_mod.Backend.init(alloc, .{});
    defer backend.close();
    var runtime_store = try backend.runtimeStore(alloc, .{ .name = "identity-incomplete-portable" });
    defer runtime_store.deinit();
    var source = try docstore_mod.DocStore.openRuntime(alloc, &runtime_store);
    defer source.close();
    const source_key = try internal_keys.documentKeyAlloc(alloc, "doc:missing-identity");
    defer alloc.free(source_key);
    try source.put(source_key, "{\"title\":\"must reject\"}");

    var portable = std.ArrayList(u8).empty;
    defer portable.deinit(alloc);
    try portable_backup.exportPortable(alloc, &source, &portable);

    var target_path_tmp = try TestDirectory.init("db");
    defer target_path_tmp.cleanup();
    const target_path = target_path_tmp.path().ptr;
    var target = try DB.open(alloc, std.mem.span(target_path), .{ .start_optional_runtimes = false });
    defer target.close();

    try std.testing.expectError(
        error.InvalidBackupRequest,
        target.importPortableIntoEmpty(alloc, portable.items, doc_identity.default_namespace),
    );
    try std.testing.expectEqual(@as(u64, 0), try target.primaryDocCount(alloc));
    try std.testing.expect(target.core.schema == null);
}

test "db portable restore rejects target with residual identity tombstones" {
    const alloc = std.testing.allocator;
    var source_path_tmp = try TestDirectory.init("db");
    defer source_path_tmp.cleanup();
    const source_path = source_path_tmp.path().ptr;
    var target_path_tmp = try TestDirectory.init("db");
    defer target_path_tmp.cleanup();
    const target_path = target_path_tmp.path().ptr;
    var portable = std.ArrayList(u8).empty;
    defer portable.deinit(alloc);

    {
        var source = try DB.open(alloc, std.mem.span(source_path), .{ .start_optional_runtimes = false });
        defer source.close();
        try source.batch(.{ .writes = &.{.{
            .key = "doc:restored",
            .value = "{\"title\":\"restored\"}",
        }} });
        try portable_backup.exportPortable(alloc, source.core.store, &portable);
    }

    var target = try DB.open(alloc, std.mem.span(target_path), .{ .start_optional_runtimes = false });
    defer target.close();
    try target.batch(.{ .writes = &.{.{
        .key = "doc:deleted",
        .value = "{\"title\":\"deleted\"}",
    }} });
    try target.batch(.{ .deletes = &.{"doc:deleted"} });
    try std.testing.expectEqual(@as(u64, 0), try target.primaryDocCount(alloc));
    const identity_before = try doc_identity.fullStatsFromStore(target.core.store);
    try std.testing.expectEqual(@as(u64, 1), identity_before.tombstone_ordinals);

    try std.testing.expectError(
        error.LiteImportTargetNotEmpty,
        target.importPortableIntoEmpty(alloc, portable.items, doc_identity.default_namespace),
    );
    try std.testing.expect((try target.get(alloc, "doc:restored")) == null);
    const identity_after = try doc_identity.fullStatsFromStore(target.core.store);
    try std.testing.expectEqual(identity_before.next_ordinal, identity_after.next_ordinal);
    try std.testing.expectEqual(identity_before.tombstone_ordinals, identity_after.tombstone_ordinals);
}

test "portable import scratch scavenging removes abandoned stages and preserves active leases" {
    if (comptime builtin.os.tag == .freestanding) return error.SkipZigTest;

    const alloc = std.testing.allocator;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    const parent = try std.fmt.allocPrint(alloc, ".zig-cache/tmp/{s}", .{tmp.sub_path});
    defer alloc.free(parent);
    const target = try std.fmt.allocPrint(alloc, "{s}/target", .{parent});
    defer alloc.free(target);
    const abandoned = try std.fmt.allocPrint(alloc, "{s}/.target.portable-import-abandoned", .{parent});
    defer alloc.free(abandoned);
    const active = try std.fmt.allocPrint(alloc, "{s}/.target.portable-import-active", .{parent});
    defer alloc.free(active);
    const lease_only = try std.fmt.allocPrint(alloc, "{s}/.target.portable-import-lease-only", .{parent});
    defer alloc.free(lease_only);
    const abandoned_lease_path = try portableImportStageLeasePathAlloc(alloc, abandoned);
    defer alloc.free(abandoned_lease_path);
    const active_lease_path = try portableImportStageLeasePathAlloc(alloc, active);
    defer alloc.free(active_lease_path);
    const lease_only_path = try portableImportStageLeasePathAlloc(alloc, lease_only);
    defer alloc.free(lease_only_path);

    db_zig.test_fail_portable_import_stage_directory_creation = true;
    try std.testing.expectError(
        error.InjectedPortableImportStageDirectoryCreationFailure,
        createUniquePortableImportStageBase(alloc, target),
    );
    var parent_dir = try std.Io.Dir.cwd().openDir(std.testing.io, parent, .{ .iterate = true });
    defer parent_dir.close(std.testing.io);
    var entries = parent_dir.iterate();
    while (try entries.next(std.testing.io)) |entry| {
        try std.testing.expect(!std.mem.startsWith(u8, entry.name, ".target.portable-import-"));
    }

    var abandoned_lease = try acquirePortableImportStageLease(alloc, abandoned);
    try std.Io.Dir.cwd().createDir(std.testing.io, abandoned, .default_dir);
    abandoned_lease.close(std.testing.io);

    var active_lease = try acquirePortableImportStageLease(alloc, active);
    defer active_lease.close(std.testing.io);
    try std.Io.Dir.cwd().createDir(std.testing.io, active, .default_dir);

    var orphaned_lease = try acquirePortableImportStageLease(alloc, lease_only);
    orphaned_lease.close(std.testing.io);

    const created = try createUniquePortableImportStageBase(alloc, target);
    defer {
        created.lease_file.close(std.testing.io);
        std.Io.Dir.cwd().deleteTree(std.testing.io, created.path) catch {};
        deletePortableImportStageLease(alloc, std.testing.io, created.path) catch {};
        alloc.free(created.path);
    }

    try std.testing.expectError(error.FileNotFound, std.Io.Dir.cwd().access(std.testing.io, abandoned, .{}));
    try std.testing.expectError(error.FileNotFound, std.Io.Dir.cwd().access(std.testing.io, abandoned_lease_path, .{}));
    try std.testing.expectError(error.FileNotFound, std.Io.Dir.cwd().access(std.testing.io, lease_only_path, .{}));
    try std.Io.Dir.cwd().access(std.testing.io, active, .{});
    try std.Io.Dir.cwd().access(std.testing.io, active_lease_path, .{});
}

test "db portable restore rebuilds multi-source graph contender provenance" {
    const alloc = std.testing.allocator;

    var source_tmp = try TestDirectory.init("db");
    defer source_tmp.cleanup();
    const source_path = source_tmp.path().ptr;
    var restore_tmp = try TestDirectory.init("db");
    defer restore_tmp.cleanup();
    const restore_path = restore_tmp.path().ptr;

    const primary_backend: PrimaryBackend = .{ .lsm = .{ .flush_threshold = 1 } };
    var portable = std.ArrayList(u8).empty;
    defer portable.deinit(alloc);

    {
        var source = try DB.open(alloc, std.mem.span(source_path), .{ .primary_backend = primary_backend });
        defer source.close();
        try source.addEnrichment(.{
            .name = "primary_relations_v1",
            .kind = .asset,
            .field = "primary_relations",
            .content_type = "application/json",
        });
        try source.addEnrichment(.{
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
        try portable_backup.exportPortable(alloc, source.core.store, &portable);
    }

    {
        var restored = try DB.open(alloc, std.mem.span(restore_path), .{
            .primary_backend = primary_backend,
            .open_mode = .writer_no_replay,
            .start_index_workers = false,
            .start_optional_runtimes = false,
        });
        defer restored.close();
        try portable_backup.importPortable(alloc, restored.core.store, portable.items);
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
    defer graph_mod.GraphIndex.freeEdges(alloc, edges);
    try std.testing.expectEqual(@as(usize, 1), edges.len);
    try std.testing.expectEqual(@as(f64, 1), edges[0].weight);
    try std.testing.expect(std.mem.indexOf(u8, edges[0].metadata, "\"winner\":\"fallback\"") != null);
    const due_rows = try restored.core.store.scanPrefix(alloc, &internal_keys.graph_edge_expiration_index_prefix);
    defer docstore_mod.DocStore.freeResults(alloc, due_rows);
    try std.testing.expectEqual(@as(usize, 1), due_rows.len);
    const due = (try graph_edge_ttl_expiration.decodeDue(due_rows[0].value)).source;
    try std.testing.expectEqualStrings("relations_graph", due.index_name);
}

test "db native snapshot exports self-contained generation" {
    const alloc = std.testing.allocator;

    var path_tmp = try TestDirectory.init("db");
    defer path_tmp.cleanup();
    const path = path_tmp.path().ptr;

    var runtime = try background_runtime_mod.BackendRuntimeHandle.init(alloc, .{
        .backend = .manual,
        .filesystem_io = std.testing.io,
    });
    defer runtime.deinit();
    var db = try DB.open(alloc, std.mem.span(path), .{
        .backend_runtime = runtime.ptr(),
    });
    defer db.close();

    try db.batch(.{
        .writes = &.{.{ .key = "doc:snap", .value = "{\"title\":\"snap\"}" }},
    });

    var canceled = std.atomic.Value(bool).init(true);
    try std.testing.expectError(
        error.Canceled,
        db.snapshotNativeWithCancellation("cancelled", types.CancellationToken.fromAtomic(&canceled)),
    );
    const snapshot_size = try db.snapshotNative("snap1");
    try std.testing.expect(snapshot_size > 0);
    try std.testing.expectError(error.SnapshotAlreadyExists, db.snapshotNative("snap1"));

    const store_snapshot_path = try std.fmt.allocPrint(alloc, "{s}.snapshots/snap1/{s}/manifest.bin", .{
        std.mem.span(path),
        db_core.primary_lsm_checkpoint_directory_name,
    });
    defer alloc.free(store_snapshot_path);
    defer {
        var snapshots_buf: [512]u8 = undefined;
        if (std.fmt.bufPrint(&snapshots_buf, "{s}.snapshots", .{std.mem.span(path)})) |snapshots| {
            var io_impl = threadedIo();
            defer io_impl.deinit();
            std.Io.Dir.cwd().deleteTree(io_impl.io(), snapshots) catch {};
        } else |_| {}
    }
    var io_impl = threadedIo();
    defer io_impl.deinit();
    try std.Io.Dir.accessAbsolute(io_impl.io(), store_snapshot_path, .{});
    const generation_manifest_path = try std.fmt.allocPrint(alloc, "{s}.snapshots/snap1/{s}", .{ std.mem.span(path), native_backup.manifest_file_name });
    defer alloc.free(generation_manifest_path);
    try std.Io.Dir.accessAbsolute(io_impl.io(), generation_manifest_path, .{});
    const manifest_raw = try std.Io.Dir.cwd().readFileAlloc(
        io_impl.io(),
        generation_manifest_path,
        alloc,
        .limited(1024 * 1024),
    );
    defer alloc.free(manifest_raw);
    var parsed_manifest = try std.json.parseFromSlice(native_backup.Manifest, alloc, manifest_raw, .{});
    defer parsed_manifest.deinit();
    try std.testing.expectEqual(native_backup.format_version, parsed_manifest.value.format_version);
    try std.testing.expectEqualStrings("antfly-lsm-checkpoint", parsed_manifest.value.primary.artifact_format);
    try std.testing.expectEqual(@as(u32, 1), parsed_manifest.value.primary.artifact_version);
}

test "db native snapshot rejects storage without atomic host generation publication" {
    antfly_test_error_logs.expectErrorLogs(1);

    const alloc = std.testing.allocator;
    var path_tmp = try TestDirectory.init("db");
    defer path_tmp.cleanup();
    const path = path_tmp.path().ptr;

    var memory_storage = lsm_backend_mod.MemoryStorage.init(alloc);
    defer memory_storage.deinit();
    var db = try DB.open(alloc, std.mem.span(path), .{
        .primary_backend = .{ .lsm = .{ .flush_threshold = 1 } },
        .storage = memory_storage.storage(),
    });
    defer db.close();
    try db.batch(.{ .writes = &.{.{ .key = "doc:one", .value = "{\"title\":\"one\"}" }} });
    try std.testing.expectError(
        error.NativeBackupStorageBackendUnsupported,
        db.snapshotNative("unsupported-storage-publication"),
    );
}

test "db native snapshot rejects an external physical root before creating artifacts" {
    const alloc = std.testing.allocator;
    var path_tmp = try TestDirectory.init("db");
    defer path_tmp.cleanup();
    const path = std.mem.span(path_tmp.path().ptr);
    defer cleanupTempDir(path);

    var backend = mem_backend_mod.Backend.init(alloc, .{});
    defer backend.close();
    var runtime_store = try backend.runtimeStore(alloc, .{ .name = "native-capture-external" });
    defer runtime_store.deinit();
    var db = try DB.open(alloc, path, .{
        .primary_backend = .{ .mem = .{} },
        .primary_runtime_store = &runtime_store,
        .physical_root_mode = .external_backend,
        .start_index_workers = false,
        .start_optional_runtimes = false,
        .ttl_cleanup = .{ .enabled = false },
    });
    defer db.close();
    try db.batch(.{ .writes = &.{.{ .key = "doc:one", .value = "{\"title\":\"one\"}" }} });

    try std.testing.expectError(
        error.NativeBackupStorageBackendUnsupported,
        db.snapshotNative("unsupported-external-root"),
    );
    const snapshot_parent = try std.fmt.allocPrint(alloc, "{s}.snapshots", .{path});
    defer alloc.free(snapshot_parent);
    try std.testing.expect(!try snapshotPathExists(std.testing.io, snapshot_parent));
}

test "db native snapshot admission bounds capture under concurrent writes" {
    const alloc = std.heap.page_allocator;
    var path_tmp = try TestDirectory.init("db");
    defer path_tmp.cleanup();
    const path = path_tmp.path().ptr;
    defer {
        var snapshots_buf: [512]u8 = undefined;
        if (std.fmt.bufPrint(&snapshots_buf, "{s}.snapshots", .{std.mem.span(path)})) |snapshots| {
            var cleanup_io = threadedIo();
            defer cleanup_io.deinit();
            std.Io.Dir.cwd().deleteTree(cleanup_io.io(), snapshots) catch {};
        } else |_| {}
    }

    var db = try DB.open(alloc, std.mem.span(path), .{
        .primary_backend = .{ .lsm = .{ .flush_threshold = 1 } },
    });
    defer db.close();
    try db.addIndex(.{
        .name = "dense_idx",
        .kind = .dense_vector,
        .config_json = "{\"field\":\"embedding\",\"dims\":3}",
    });
    try db.batch(.{
        .writes = &.{.{ .key = "doc:before", .value = "{\"embedding\":[1,0,0]}" }},
        .sync_level = .full_index,
    });

    const Fence = struct {
        db: *DB,
        entered: std.atomic.Value(bool) = .init(false),
        release: std.atomic.Value(bool) = .init(false),
        apply_released: std.atomic.Value(bool) = .init(false),
        copy_entered: std.atomic.Value(bool) = .init(false),
        release_copy: std.atomic.Value(bool) = .init(false),
        materialize_entered: std.atomic.Value(bool) = .init(false),
        release_materialize: std.atomic.Value(bool) = .init(false),

        fn afterCaptureAdmission(ptr: *anyopaque) void {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            self.entered.store(true, .release);
            while (!self.release.load(.acquire)) std.testing.io.sleep(.fromNanoseconds(1), .awake) catch {};
        }

        fn afterApplyRelease(ptr: *anyopaque) void {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            if (self.db.core.apply_mutex.tryLockExclusive()) {
                self.db.core.apply_mutex.unlockExclusive();
                self.apply_released.store(true, .release);
            }
            self.copy_entered.store(true, .release);
            while (!self.release_copy.load(.acquire)) std.testing.io.sleep(.fromNanoseconds(1), .awake) catch {};
        }

        fn beforeArtifactMaterialize(ptr: *anyopaque) void {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            self.materialize_entered.store(true, .release);
            while (!self.release_materialize.load(.acquire)) std.testing.io.sleep(.fromNanoseconds(1), .awake) catch {};
        }
    };
    const SnapshotWorker = struct {
        db: *DB,
        canceled: std.atomic.Value(bool) = .init(false),
        done: std.atomic.Value(bool) = .init(false),
        err: ?anyerror = null,

        fn run(self: *@This()) void {
            defer self.done.store(true, .release);
            _ = self.db.snapshotNativeWithCancellation(
                "fenced-native",
                types.CancellationToken.fromAtomic(&self.canceled),
            ) catch |err| {
                self.err = err;
                return;
            };
        }
    };
    const Writer = struct {
        db: *DB,
        started: std.atomic.Value(bool) = .init(false),
        done: std.atomic.Value(bool) = .init(false),
        err: ?anyerror = null,

        fn run(self: *@This()) void {
            self.started.store(true, .release);
            self.db.batch(.{
                .writes = &.{.{ .key = "doc:during", .value = "{\"embedding\":[0,1,0]}" }},
                // Exercise the post-fence generated-index write path. Native
                // capture must never pin an appendable LSM WAL payload inode.
                .sync_level = .full_index,
            }) catch |err| {
                self.err = err;
            };
            self.done.store(true, .release);
        }
    };
    const Maintenance = struct {
        db: *DB,
        started: std.atomic.Value(bool) = .init(false),
        done: std.atomic.Value(bool) = .init(false),
        err: ?anyerror = null,

        fn run(self: *@This()) void {
            self.started.store(true, .release);
            _ = self.db.runLsmMaintenanceStepBestEffort() catch |err| {
                self.err = err;
                self.done.store(true, .release);
                return;
            };
            self.done.store(true, .release);
        }
    };

    var fence = Fence{ .db = &db };
    DB.test_snapshot_fence_hook = .{
        .ptr = &fence,
        .after_capture_admission = Fence.afterCaptureAdmission,
        .after_apply_release = Fence.afterApplyRelease,
        .before_artifact_materialize = Fence.beforeArtifactMaterialize,
    };
    defer DB.test_snapshot_fence_hook = null;
    var worker = SnapshotWorker{ .db = &db };
    var snapshot_thread = try std.testing.io.concurrent(SnapshotWorker.run, .{&worker});
    defer {
        fence.release.store(true, .release);
        fence.release_copy.store(true, .release);
        fence.release_materialize.store(true, .release);
        snapshot_thread.await(std.testing.io);
    }
    const capture_deadline = monotonicTimeNs() +| 5 * std.time.ns_per_s;
    while (!fence.entered.load(.acquire) and
        !worker.done.load(.acquire) and
        monotonicTimeNs() < capture_deadline) std.testing.io.sleep(.fromNanoseconds(1), .awake) catch {};
    if (!fence.entered.load(.acquire)) {
        worker.canceled.store(true, .release);
        fence.release.store(true, .release);
        fence.release_copy.store(true, .release);
        fence.release_materialize.store(true, .release);
        snapshot_thread.await(std.testing.io);
        if (worker.err) |err| return err;
        return error.SnapshotCaptureFenceTimeout;
    }

    // Maintenance-owned replay production is not a client mutation and must
    // remain able to finish after primary admission closes. The capture drain
    // includes this advance before selecting its final checkpoint.
    const internal_sequence = try appendDerivedBatchRecord(&db, .{});
    try std.testing.expect(internal_sequence > 0);

    // A writer arriving after capture intent is published cannot advance the
    // selected revision. It resumes immediately after staging, before manifest
    // hashing and publication complete.
    var writer = Writer{ .db = &db };
    var writer_thread = try std.testing.io.concurrent(Writer.run, .{&writer});
    defer {
        fence.release.store(true, .release);
        fence.release_copy.store(true, .release);
        fence.release_materialize.store(true, .release);
        writer_thread.await(std.testing.io);
    }
    const writer_start_deadline = monotonicTimeNs() +| 5 * std.time.ns_per_s;
    while (!writer.started.load(.acquire) and monotonicTimeNs() < writer_start_deadline)
        std.testing.io.sleep(.fromNanoseconds(1), .awake) catch {};
    try std.testing.expect(writer.started.load(.acquire));
    for (0..1024) |_| std.testing.io.sleep(.fromNanoseconds(1), .awake) catch {};
    try std.testing.expect(!writer.done.load(.acquire));
    fence.release.store(true, .release);
    const copy_deadline = monotonicTimeNs() +| 5 * std.time.ns_per_s;
    while (!fence.copy_entered.load(.acquire) and
        !worker.done.load(.acquire) and
        monotonicTimeNs() < copy_deadline) std.testing.io.sleep(.fromNanoseconds(1), .awake) catch {};
    if (!fence.copy_entered.load(.acquire)) {
        worker.canceled.store(true, .release);
        fence.release_copy.store(true, .release);
        fence.release_materialize.store(true, .release);
        snapshot_thread.await(std.testing.io);
        writer_thread.await(std.testing.io);
        if (worker.err) |err| return err;
        if (writer.err) |err| return err;
        return error.SnapshotCopyFenceTimeout;
    }

    // Apply release alone is not the publication boundary: the short metadata
    // pin still excludes physical index maintenance.
    var maintenance = Maintenance{ .db = &db };
    var maintenance_thread = try std.testing.io.concurrent(Maintenance.run, .{&maintenance});
    defer {
        fence.release.store(true, .release);
        fence.release_copy.store(true, .release);
        fence.release_materialize.store(true, .release);
        maintenance_thread.await(std.testing.io);
    }
    const maintenance_start_deadline = monotonicTimeNs() +| 5 * std.time.ns_per_s;
    while (!maintenance.started.load(.acquire) and monotonicTimeNs() < maintenance_start_deadline)
        std.testing.io.sleep(.fromNanoseconds(1), .awake) catch {};
    try std.testing.expect(maintenance.started.load(.acquire));
    for (0..1024) |_| std.testing.io.sleep(.fromNanoseconds(1), .awake) catch {};
    try std.testing.expect(!maintenance.done.load(.acquire));
    fence.release_copy.store(true, .release);
    const materialize_deadline = monotonicTimeNs() +| 5 * std.time.ns_per_s;
    while (!fence.materialize_entered.load(.acquire) and
        !worker.done.load(.acquire) and
        monotonicTimeNs() < materialize_deadline) std.testing.io.sleep(.fromNanoseconds(1), .awake) catch {};
    if (!fence.materialize_entered.load(.acquire)) {
        worker.canceled.store(true, .release);
        fence.release_materialize.store(true, .release);
        snapshot_thread.await(std.testing.io);
        maintenance_thread.await(std.testing.io);
        writer_thread.await(std.testing.io);
        if (worker.err) |err| return err;
        if (maintenance.err) |err| return err;
        if (writer.err) |err| return err;
        return error.SnapshotMaterializeFenceTimeout;
    }
    // Every corpus-sized generated and primary read is outside both mutation
    // fences. Foreground writes and index maintenance must finish even while
    // artifact materialization is deliberately held.
    const outside_fence_deadline = monotonicTimeNs() +| 5 * std.time.ns_per_s;
    while (monotonicTimeNs() < outside_fence_deadline) {
        if (writer.done.load(.acquire) and maintenance.done.load(.acquire)) break;
        std.testing.io.sleep(.fromNanoseconds(1), .awake) catch {};
    }
    const writer_finished_outside_fence = writer.done.load(.acquire);
    const maintenance_finished_outside_fence = maintenance.done.load(.acquire);
    fence.release_materialize.store(true, .release);
    snapshot_thread.await(std.testing.io);
    maintenance_thread.await(std.testing.io);
    writer_thread.await(std.testing.io);
    if (worker.err) |err| return err;
    if (maintenance.err) |err| return err;
    if (writer.err) |err| return err;
    try std.testing.expect(writer_finished_outside_fence);
    try std.testing.expect(maintenance_finished_outside_fence);
    try std.testing.expect(writer.done.load(.acquire));
    try std.testing.expect(fence.apply_released.load(.acquire));

    const snapshot_root = try std.fmt.allocPrint(alloc, "{s}.snapshots/fenced-native", .{std.mem.span(path)});
    defer alloc.free(snapshot_root);
    var destination_tmp = std.testing.tmpDir(.{});
    defer destination_tmp.cleanup();
    const destination = try std.fmt.allocPrint(alloc, ".zig-cache/tmp/{s}", .{destination_tmp.sub_path});
    defer alloc.free(destination);
    var loaded = (try native_backup.validateAndMaterialize(
        alloc,
        std.testing.io,
        snapshot_root,
        destination,
    )).?;
    defer loaded.deinit();
    try std.testing.expectEqual(@as(usize, 1), loaded.value().projections.len);
    try std.testing.expectEqual(
        loaded.value().capture_target_sequence,
        loaded.value().projections[0].applied_sequence,
    );
    try std.testing.expectEqual(
        loaded.value().projections[0].applied_sequence,
        loaded.value().projections[0].target_sequence,
    );
}

test "db snapshot exports logical store only for durable lsm primary backend" {
    const alloc = std.testing.allocator;

    var path_tmp = try TestDirectory.init("db");
    defer path_tmp.cleanup();
    const path = path_tmp.path().ptr;

    var db = try DB.open(alloc, std.mem.span(path), .{
        .primary_backend = .{ .lsm = .{ .flush_threshold = 1 } },
    });
    defer db.close();

    try db.batch(.{
        .writes = &.{.{ .key = "doc:snap", .value = "{\"title\":\"snap\"}" }},
    });

    const snapshot_size = try db.snapshot("snap1");
    try std.testing.expect(snapshot_size > 0);

    const store_snapshot_path = try std.fmt.allocPrint(alloc, "{s}.snapshots/snap1/store.bin", .{std.mem.span(path)});
    defer alloc.free(store_snapshot_path);
    const snapshot_manifest_path = try std.fmt.allocPrint(
        alloc,
        "{s}.snapshots/snap1/{s}",
        .{ std.mem.span(path), db_core.logical_snapshot_manifest_file_name },
    );
    defer alloc.free(snapshot_manifest_path);
    defer {
        var snapshots_buf: [512]u8 = undefined;
        if (std.fmt.bufPrint(&snapshots_buf, "{s}.snapshots", .{std.mem.span(path)})) |snapshots| {
            var io_impl = threadedIo();
            defer io_impl.deinit();
            std.Io.Dir.cwd().deleteTree(io_impl.io(), snapshots) catch {};
        } else |_| {}
    }
    var io_impl = threadedIo();
    defer io_impl.deinit();
    try std.Io.Dir.accessAbsolute(io_impl.io(), store_snapshot_path, .{});
    try std.Io.Dir.accessAbsolute(io_impl.io(), snapshot_manifest_path, .{});
}

test "db native deferred restore preserves generated dense generation without embedder" {
    const alloc = std.testing.allocator;
    var source_tmp = try TestDirectory.init("db");
    defer source_tmp.cleanup();
    const source_path = source_tmp.path().ptr;
    var restore_tmp = try TestDirectory.init("db");
    defer restore_tmp.cleanup();
    const restore_path = restore_tmp.path().ptr;
    const primary_backend: PrimaryBackend = .{ .lsm = .{ .flush_threshold = 1 } };

    var source_applied: u64 = 0;
    var source_generation: u64 = 0;
    var source_count: u64 = 0;
    {
        var deterministic = embedder_mod.DeterministicDenseEmbedder{};
        var source = try DB.open(alloc, std.mem.span(source_path), .{
            .primary_backend = primary_backend,
            .enrichment = .{
                .owner_id = "native-backup-source",
                .dense_embedder = deterministic.interface(),
            },
        });
        defer source.close();
        try source.addIndex(.{
            .name = "semantic_idx",
            .kind = .dense_vector,
            .config_json = "{\"field\":\"embedding\",\"dims\":3,\"generator\":{\"kind\":\"dense_embedding\",\"source_field\":\"body\",\"chunk_name\":\"body_chunks_v1\",\"chunk_size\":8,\"chunk_overlap\":2}}",
        });
        try source.addIndex(.{
            .name = "text_idx",
            .kind = .full_text,
            .config_json = "{\"field\":\"title\"}",
        });
        try source.batch(.{
            .writes = &.{.{
                .key = "doc:native",
                .value = "{\"body\":\"abcdefghijklmno\",\"title\":\"portable native generation\"}",
            }},
            .sync_level = .full_index,
        });
        try source.runUntilIdle();
        // A fresh native-v2 index can now publish its clean projection during
        // the normal full-index barrier. Older roots still converge through
        // the durable repair lane. The backup invariant is the same in both
        // cases, so exercise repair only when authoritative debt remains
        // instead of requiring a particular publication schedule.
        if (try source.denseArtifactRebuildMaintenanceNeeded(alloc)) {
            try std.testing.expect(try source.runDenseArtifactRebuildMaintenanceWithProgress(alloc, null, null));
            _ = try source.repairRecoverableStartupIndexFailures(alloc, 1, .{});
        }
        try source.runUntilIdle();
        const cfg = source.core.index_manager.get("semantic_idx") orelse return error.TestUnexpectedResult;
        const checkpoint = try source.core.loadProjectionCheckpoint(alloc, cfg.name);
        source_applied = checkpoint.applied_sequence;
        source_generation = checkpoint.generation;
        source_count = source.core.index_manager.denseIndex("semantic_idx").?.index.stats().active_count;
        try std.testing.expect(source_count > 0);
        _ = try source.core.index_manager.cleanupInactiveRepairShadowRootsPage();
        const active_relative = (try source.core.index_manager.captureActiveIndexRootPointer("semantic_idx")) orelse
            return error.TestUnexpectedResult;
        defer alloc.free(active_relative);
        const active_root = try std.fmt.allocPrint(alloc, "{s}/{s}", .{ std.mem.span(source_path), active_relative });
        defer alloc.free(active_root);
        for ([_][]const u8{ "runs", "wal", "manifest.bin" }) |legacy_name| {
            const legacy_path = try std.fmt.allocPrint(alloc, "{s}/{s}", .{ active_root, legacy_name });
            defer alloc.free(legacy_path);
            try std.testing.expectError(
                error.FileNotFound,
                std.Io.Dir.cwd().access(std.testing.io, legacy_path, .{}),
            );
        }
        _ = try source.snapshotNative("native-fast");
    }

    const snapshot_root = try std.fmt.allocPrint(alloc, "{s}.snapshots/native-fast", .{std.mem.span(source_path)});
    defer alloc.free(snapshot_root);
    defer {
        var snapshots_buf: [512]u8 = undefined;
        if (std.fmt.bufPrint(&snapshots_buf, "{s}.snapshots", .{std.mem.span(source_path)})) |snapshots| {
            var cleanup_io = threadedIo();
            defer cleanup_io.deinit();
            std.Io.Dir.cwd().deleteTree(cleanup_io.io(), snapshots) catch {};
        } else |_| {}
    }
    const native_manifest_path = try std.fmt.allocPrint(alloc, "{s}/{s}", .{ snapshot_root, native_backup.manifest_file_name });
    defer alloc.free(native_manifest_path);
    const native_manifest_raw = try std.Io.Dir.cwd().readFileAlloc(
        std.testing.io,
        native_manifest_path,
        alloc,
        .limited(native_backup.max_manifest_bytes),
    );
    defer alloc.free(native_manifest_raw);
    var native_manifest = try std.json.parseFromSlice(native_backup.Manifest, alloc, native_manifest_raw, .{});
    defer native_manifest.deinit();
    var saw_native_dense = false;
    for (native_manifest.value.projections) |projection| {
        if (!std.mem.eql(u8, projection.name, "semantic_idx")) continue;
        try std.testing.expectEqualStrings(
            index_manager_mod.IndexManager.dense_native_backup_backend_id,
            projection.backend_id,
        );
        saw_native_dense = true;
    }
    try std.testing.expect(saw_native_dense);
    const native_posting_current = try std.fmt.allocPrint(
        alloc,
        "{s}/indexes/semantic_idx/posting-segments/CURRENT",
        .{snapshot_root},
    );
    defer alloc.free(native_posting_current);
    try std.Io.Dir.accessAbsolute(std.testing.io, native_posting_current, .{});

    // Exact-vector blocks are shared acceleration rather than posting
    // authority. Simulate losing one after capture: restore must retain the
    // native posting generation and serve exact results from primary vectors,
    // even though this fixture intentionally has no embedder for rebuilding.
    var removed_shared_acceleration = false;
    for (native_manifest.value.artifacts) |artifact| {
        if (artifact.role != .shared_acceleration or !std.mem.endsWith(u8, artifact.path, ".afvb")) continue;
        const artifact_path = try std.fmt.allocPrint(alloc, "{s}/{s}", .{ snapshot_root, artifact.path });
        defer alloc.free(artifact_path);
        try std.Io.Dir.cwd().deleteFile(std.testing.io, artifact_path);
        removed_shared_acceleration = true;
        break;
    }
    try std.testing.expect(removed_shared_acceleration);

    var transition = try generation_lifecycle.beginProcessExclusive(std.mem.span(restore_path));
    defer transition.deinit();
    var staged = try transition.beginStaging();
    defer staged.deinit();
    var restore_runtime = try background_runtime_mod.BackendRuntimeHandle.init(alloc, .{
        .backend = .manual,
        .filesystem_io = std.testing.io,
    });
    defer restore_runtime.deinit();
    try DB.restoreSnapshotToDeferredRuntimeRepair(
        &staged,
        alloc,
        snapshot_root,
        staged.path(),
        .{
            .primary_backend = primary_backend,
            .backend_runtime = restore_runtime.ptr(),
        },
        .{
            .backup_id = "native-fast",
            .location = "local",
            .artifact_sha256 = "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855",
            .snapshot_path = "native-fast",
            .group_id = 7001,
        },
    );
    try std.testing.expect(!(try DB.restoreRuntimeRepairNeededForPathWithIo(alloc, std.testing.io, staged.path())));
    _ = try staged.publish();
    staged.deinit();
    transition.deinit();

    var restored = try DB.open(alloc, std.mem.span(restore_path), .{
        .primary_backend = primary_backend,
        .open_mode = .query_readonly,
        .start_index_workers = false,
        .start_optional_runtimes = false,
    });
    defer restored.close();
    const cfg = restored.core.index_manager.get("semantic_idx") orelse return error.TestUnexpectedResult;
    const checkpoint = try restored.core.loadProjectionCheckpoint(alloc, cfg.name);
    try std.testing.expectEqual(source_applied, checkpoint.applied_sequence);
    try std.testing.expectEqual(source_generation, checkpoint.generation);
    try std.testing.expectEqual(source_count, restored.core.index_manager.denseIndex("semantic_idx").?.index.stats().active_count);
    try std.testing.expect(!try restored.core.index_manager.denseNativePhysicalMigrationRequired("semantic_idx"));
    try std.testing.expect((try restored.indexRepairIdForIndex(alloc, "semantic_idx")) == null);

    var deterministic = embedder_mod.DeterministicDenseEmbedder{};
    const vector = try deterministic.interface().embedDense(alloc, "", "abcdefgh", 3);
    defer alloc.free(vector);
    var result = try restored.search(alloc, .{
        .index_name = "semantic_idx",
        .dense = .{ .vector = vector, .k = 3 },
        .return_mode = .parent,
    });
    defer result.deinit();
    try std.testing.expect(result.hits.len > 0);
    try std.testing.expectEqualStrings("doc:native", result.hits[0].id);

    var text_result = try restored.search(alloc, .{
        .index_name = "text_idx",
        .full_text = .{ .match = .{ .field = "title", .text = "generation" } },
    });
    defer text_result.deinit();
    try std.testing.expectEqual(@as(u32, 1), text_result.total_hits);
    try std.testing.expectEqualStrings("doc:native", text_result.hits[0].id);
}

test "db restore snapshot recreates logical store for durable lsm primary backend" {
    const alloc = std.testing.allocator;

    var src_tmp = try TestDirectory.init("db");
    defer src_tmp.cleanup();
    const src_path = src_tmp.path().ptr;

    var restore_tmp = try TestDirectory.init("db");
    defer restore_tmp.cleanup();
    const restore_path = restore_tmp.path().ptr;

    const primary_backend: PrimaryBackend = .{ .lsm = .{ .flush_threshold = 1 } };

    {
        var db = try DB.open(alloc, std.mem.span(src_path), .{
            .primary_backend = primary_backend,
        });
        defer db.close();

        try db.addIndex(.{
            .name = "ft_v1",
            .kind = .full_text,
            .config_json = "{\"field\":\"title\"}",
        });
        try db.batch(.{
            .writes = &.{.{ .key = "doc:snap", .value = "{\"title\":\"snap\"}" }},
        });
        _ = try db.snapshot("snap1");
    }

    const snapshot_root = try std.fmt.allocPrint(alloc, "{s}.snapshots/snap1", .{std.mem.span(src_path)});
    defer alloc.free(snapshot_root);
    defer {
        var snapshots_buf: [512]u8 = undefined;
        if (std.fmt.bufPrint(&snapshots_buf, "{s}.snapshots", .{std.mem.span(src_path)})) |snapshots| {
            var io_impl = threadedIo();
            defer io_impl.deinit();
            std.Io.Dir.cwd().deleteTree(io_impl.io(), snapshots) catch {};
        } else |_| {}
    }

    try DB.restoreSnapshotTo(alloc, snapshot_root, std.mem.span(restore_path), .{
        .primary_backend = primary_backend,
    });

    var restored = try DB.open(alloc, std.mem.span(restore_path), .{
        .primary_backend = primary_backend,
    });
    defer restored.close();

    const snap_doc = (try restored.get(alloc, "doc:snap")) orelse return error.TestExpectedEqual;
    defer alloc.free(snap_doc);
    try std.testing.expectEqualStrings("{\"title\":\"snap\"}", snap_doc);

    var result = try restored.search(alloc, .{
        .index_name = "ft_v1",
        .full_text = .{ .match = .{ .field = "title", .text = "snap" } },
    });
    defer result.deinit();
    try std.testing.expectEqual(@as(u32, 1), result.total_hits);
}

test "db restore snapshot repeatedly validates run-backed doc identity metadata" {
    // Exercise the allocator used by the server process. Darwin's malloc
    // diagnostics only poison and guard allocations that cross this boundary.
    const alloc = platform.allocator.processAllocator(std.testing.allocator);

    var src_tmp = try TestDirectory.initFast("db");
    defer src_tmp.cleanup();
    const src_path = src_tmp.path().ptr;

    const primary_backend: PrimaryBackend = .{ .lsm = .{ .flush_threshold = 1 } };
    const identity_namespace = doc_identity.Namespace{
        .table_id = 21,
        .shard_id = 22,
        .range_id = 23,
    };

    {
        var db = try DB.open(alloc, std.mem.span(src_path), .{
            .primary_backend = primary_backend,
            .identity_namespace = identity_namespace,
        });
        defer db.close();

        for (0..96) |i| {
            const key = try std.fmt.allocPrint(alloc, "doc:restore-chaos-{d:0>3}", .{i});
            defer alloc.free(key);
            const value = try std.fmt.allocPrint(alloc, "{{\"title\":\"restore chaos {d}\",\"ordinal\":{d}}}", .{ i, i });
            defer alloc.free(value);
            try db.batch(.{
                .writes = &.{.{ .key = key, .value = value }},
                .sync_level = .full_index,
            });
        }

        _ = try db.snapshot("snap1");
    }

    const snapshot_root = try std.fmt.allocPrint(alloc, "{s}.snapshots/snap1", .{std.mem.span(src_path)});
    defer alloc.free(snapshot_root);
    defer {
        var snapshots_buf: [512]u8 = undefined;
        if (std.fmt.bufPrint(&snapshots_buf, "{s}.snapshots", .{std.mem.span(src_path)})) |snapshots| {
            var io_impl = threadedIo();
            defer io_impl.deinit();
            std.Io.Dir.cwd().deleteTree(io_impl.io(), snapshots) catch {};
        } else |_| {}
    }

    for (0..32) |i| {
        var restore_tmp = try TestDirectory.initFast("db");
        defer restore_tmp.cleanup();
        const restore_path = restore_tmp.path().ptr;

        var transition = try generation_lifecycle.beginProcessExclusive(std.mem.span(restore_path));
        defer transition.deinit();
        var staged_generation = try transition.beginStaging();
        defer staged_generation.deinit();
        try DB.restoreSnapshotToDeferredRuntimeRepairWithIo(
            &staged_generation,
            alloc,
            std.testing.io,
            snapshot_root,
            staged_generation.path(),
            .{
                .primary_backend = primary_backend,
                .identity_namespace = identity_namespace,
            },
            .{
                .backup_id = "restore-chaos",
                .location = "local",
                .artifact_sha256 = "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855",
                .snapshot_path = "snap1",
                .group_id = @intCast(i + 1),
            },
        );
        _ = try staged_generation.publish();
        // Release the staged generation and the exclusive transition before
        // reopening the live path: published-generation reads fail closed with
        // GenerationTransitionActive while an exclusive transition is active.
        staged_generation.deinit();
        transition.deinit();

        var restored = try DB.open(alloc, std.mem.span(restore_path), .{
            .primary_backend = primary_backend,
            .identity_namespace = identity_namespace,
            .prefer_existing_identity_namespace = true,
        });
        defer restored.close();

        const doc = (try restored.get(alloc, "doc:restore-chaos-095")) orelse return error.TestExpectedEqual;
        defer alloc.free(doc);
        try std.testing.expect(std.mem.indexOf(u8, doc, "restore chaos 95") != null);
    }
}

test "db restore snapshot rejects invalid doc identity metadata" {
    const alloc = std.testing.allocator;

    var src_tmp = try TestDirectory.init("db");
    defer src_tmp.cleanup();
    const src_path = src_tmp.path().ptr;

    var restore_tmp = try TestDirectory.init("db");
    defer restore_tmp.cleanup();
    const restore_path = restore_tmp.path().ptr;

    const primary_backend: PrimaryBackend = .{ .lsm = .{ .flush_threshold = 1 } };

    {
        var db = try DB.open(alloc, std.mem.span(src_path), .{
            .primary_backend = primary_backend,
        });
        defer db.close();

        try db.batch(.{
            .writes = &.{.{ .key = "doc:bad-identity", .value = "{\"title\":\"bad\"}" }},
        });

        const state_key = internal_keys.identityOrdinalStateKey(1);
        var corrupt_state: [25]u8 = undefined;
        std.mem.writeInt(u64, corrupt_state[0..8], 0xdead_beef, .big);
        std.mem.writeInt(u64, corrupt_state[8..16], 1, .big);
        corrupt_state[16] = 0;
        @memset(corrupt_state[17..25], 0);
        try db.core.store.putBatch(&.{.{ .key = state_key[0..], .value = corrupt_state[0..] }}, &.{});

        _ = try db.snapshot("snap1");
    }

    const snapshot_root = try std.fmt.allocPrint(alloc, "{s}.snapshots/snap1", .{std.mem.span(src_path)});
    defer alloc.free(snapshot_root);
    defer {
        var snapshots_buf: [512]u8 = undefined;
        if (std.fmt.bufPrint(&snapshots_buf, "{s}.snapshots", .{std.mem.span(src_path)})) |snapshots| {
            var io_impl = threadedIo();
            defer io_impl.deinit();
            std.Io.Dir.cwd().deleteTree(io_impl.io(), snapshots) catch {};
        } else |_| {}
    }

    try std.testing.expectError(error.InvalidDocIdentity, DB.restoreSnapshotTo(alloc, snapshot_root, std.mem.span(restore_path), .{
        .primary_backend = primary_backend,
    }));
}

test "db restore snapshot recreates text sparse and graph indexes for durable lsm primary backend" {
    const alloc = std.testing.allocator;

    var src_tmp = try TestDirectory.init("db");
    defer src_tmp.cleanup();
    const src_path = src_tmp.path().ptr;

    var restore_tmp = try TestDirectory.init("db");
    defer restore_tmp.cleanup();
    const restore_path = restore_tmp.path().ptr;

    const primary_backend: PrimaryBackend = .{ .lsm = .{ .flush_threshold = 1 } };

    {
        var db = try DB.open(alloc, std.mem.span(src_path), .{
            .primary_backend = primary_backend,
        });
        defer db.close();

        const index_cfgs = [_]types.IndexConfig{
            .{
                .name = "ft_v1",
                .kind = .full_text,
                .config_json = "{\"field\":\"title\"}",
            },
            .{
                .name = "sp_v1",
                .kind = .sparse_vector,
                .config_json = "{\"field\":\"sparse\"}",
            },
            .{
                .name = "graph_v1",
                .kind = .graph,
                .config_json = "{}",
            },
        };
        for (index_cfgs) |cfg| try db.addIndex(cfg);

        try db.batch(.{
            .writes = &.{
                .{ .key = "doc:a", .value = "{\"title\":\"alpha\",\"sparse\":{\"indices\":[1],\"values\":[1.0]}}" },
                .{ .key = "doc:z", .value = "{\"title\":\"zeta\",\"sparse\":{\"indices\":[2],\"values\":[1.0]}}" },
            },
            .graph_writes = &.{
                .{ .index_name = "graph_v1", .source = "doc:z", .target = "doc:y", .edge_type = "cites", .weight = 1.0 },
            },
        });
        _ = try db.snapshot("snap1");
    }

    const snapshot_root = try std.fmt.allocPrint(alloc, "{s}.snapshots/snap1", .{std.mem.span(src_path)});
    defer alloc.free(snapshot_root);
    defer {
        var snapshots_buf: [512]u8 = undefined;
        if (std.fmt.bufPrint(&snapshots_buf, "{s}.snapshots", .{std.mem.span(src_path)})) |snapshots| {
            var io_impl = threadedIo();
            defer io_impl.deinit();
            std.Io.Dir.cwd().deleteTree(io_impl.io(), snapshots) catch {};
        } else |_| {}
    }

    try DB.restoreSnapshotTo(alloc, snapshot_root, std.mem.span(restore_path), .{
        .primary_backend = primary_backend,
    });

    var restored = try DB.open(alloc, std.mem.span(restore_path), .{
        .primary_backend = primary_backend,
    });
    defer restored.close();

    var text = try restored.search(alloc, .{
        .index_name = "ft_v1",
        .full_text = .{ .match = .{ .field = "title", .text = "zeta" } },
    });
    defer text.deinit();
    try std.testing.expectEqual(@as(u32, 1), text.total_hits);
    try std.testing.expectEqualStrings("doc:z", text.hits[0].id);

    var sparse = try restored.search(alloc, .{
        .index_name = "sp_v1",
        .query = .{ .sparse_knn = .{
            .indices = &.{2},
            .values = &.{1.0},
            .k = 2,
        } },
        .limit = 2,
    });
    defer sparse.deinit();
    try std.testing.expectEqual(@as(u32, 1), sparse.total_hits);
    try std.testing.expectEqualStrings("doc:z", sparse.hits[0].id);

    const incoming = try restored.getEdges(alloc, "graph_v1", "doc:y", "cites", .in);
    defer graph_mod.GraphIndex.freeEdges(alloc, incoming);
    try std.testing.expectEqual(@as(usize, 1), incoming.len);
    try std.testing.expectEqualStrings("doc:z", incoming[0].source);
}

test "db restore snapshot replays managed chunked dense embeddings for durable lsm primary backend" {
    const alloc = std.testing.allocator;

    var src_tmp = try TestDirectory.init("db");
    defer src_tmp.cleanup();
    const src_path = src_tmp.path().ptr;

    var restore_tmp = try TestDirectory.init("db");
    defer restore_tmp.cleanup();
    const restore_path = restore_tmp.path().ptr;

    const primary_backend: PrimaryBackend = .{ .lsm = .{ .flush_threshold = 1 } };

    {
        var deterministic = embedder_mod.DeterministicDenseEmbedder{};
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
        var before = try waitForSearchResult(alloc, &db, .{
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
            var io_impl = threadedIo();
            defer io_impl.deinit();
            std.Io.Dir.cwd().deleteTree(io_impl.io(), snapshots) catch {};
        } else |_| {}
    }

    try DB.restoreSnapshotTo(alloc, snapshot_root, std.mem.span(restore_path), .{
        .primary_backend = primary_backend,
    });

    var deterministic = embedder_mod.DeterministicDenseEmbedder{};
    var restored = try DB.open(alloc, std.mem.span(restore_path), .{
        .primary_backend = primary_backend,
        .enrichment = .{
            .owner_id = "worker-a",
            .dense_embedder = deterministic.interface(),
        },
    });
    defer restored.close();

    const chunk_prefix = try internal_keys.artifactNamedPrefixAlloc(alloc, "doc:a", "chunk", "body_chunks_v1");
    defer alloc.free(chunk_prefix);
    const chunk_records = try restored.core.store.scanPrefix(alloc, chunk_prefix);
    defer docstore_mod.DocStore.freeResults(alloc, chunk_records);
    try std.testing.expect(chunk_records.len > 0);

    const rebuilt = try restored.rebuildDenseIndexesFromStoredEmbeddingArtifacts(alloc);
    try std.testing.expect(rebuilt > 0);
    _ = try restored.replayGeneratedEnrichmentsFromStoredDocs(alloc);
    try restored.runUntilIdle();
    const pending = restored.pendingWorkStats();
    try std.testing.expectEqual(pending.enrichment.target_sequence, pending.enrichment.applied_sequence);
    try std.testing.expect(pending.enrichment.applied_sequence >= 1);

    const query_vec = try deterministic.interface().embedDense(alloc, "", "abcdefgh", 3);
    defer alloc.free(query_vec);
    var after = try waitForSearchResult(alloc, &restored, .{
        .index_name = "dv_v1",
        .dense = .{ .vector = query_vec, .k = 3 },
        .return_mode = .parent,
    }, 1);
    defer after.deinit();
    try std.testing.expectEqualStrings("doc:a", after.hits[0].id);
}

test "db restore dense artifact completion publishes counter bootstrap as a bounded quantum" {
    const alloc = std.testing.allocator;
    var path_tmp = try TestDirectory.init("db");
    defer path_tmp.cleanup();
    const path = path_tmp.path().ptr;

    var db = try DB.open(alloc, std.mem.span(path), no_background_test_open);
    defer db.close();
    try db.addIndex(.{
        .name = "dense_idx",
        .kind = .dense_vector,
        .config_json = "{\"field\":\"embedding\",\"dims\":2,\"external\":true}",
    });
    try db.batch(.{
        .writes = &.{.{
            .key = "doc:a",
            .value = "{\"_embeddings\":{\"dense_idx\":[1,0]}}",
        }},
        .sync_level = .full_index,
    });

    const counter_key = try DB.denseArtifactTargetCounterKeyAlloc(alloc, "dense_idx");
    defer alloc.free(counter_key);
    try db.core.store.delete(counter_key);
    const cfg = db.core.index_manager.get("dense_idx") orelse return error.TestUnexpectedResult;
    const repair_id = try db.ensureAutomaticDenseGenerationRepairIntent(
        alloc,
        cfg.*,
        .artifact_counter_missing,
        "dense_artifact_counter_missing",
    );

    try std.testing.expectEqual(
        DB.RestoreDenseArtifactCompletion.progressed,
        try db.completeRestoreDenseArtifactRepairs(alloc),
    );
    try std.testing.expectEqual(
        @as(?u64, 1),
        try DB.loadDenseArtifactTargetCounter(alloc, db.core.store, "dense_idx"),
    );
    try std.testing.expectEqual(
        repair_id,
        (try db.indexRepairIdForIndex(alloc, "dense_idx")) orelse return error.TestUnexpectedResult,
    );

    try std.testing.expectEqual(
        DB.RestoreDenseArtifactCompletion.completed_with_progress,
        try db.completeRestoreDenseArtifactRepairs(alloc),
    );
    try std.testing.expect((try db.indexRepairIdForIndex(alloc, "dense_idx")) == null);
}

test "db restore dense artifact completion keeps every intent when one proof is pending" {
    const alloc = std.testing.allocator;
    var path_tmp = try TestDirectory.init("db");
    defer path_tmp.cleanup();
    const path = path_tmp.path().ptr;

    var db = try DB.open(alloc, std.mem.span(path), no_background_test_open);
    defer db.close();
    inline for (.{ "dense_a", "dense_b" }) |name| {
        try db.addIndex(.{
            .name = name,
            .kind = .dense_vector,
            .config_json = "{\"field\":\"embedding\",\"dims\":2,\"external\":true}",
        });
    }
    try db.batch(.{
        .writes = &.{.{
            .key = "doc:a",
            .value = "{\"_embeddings\":{\"dense_a\":[1,0],\"dense_b\":[0,1]}}",
        }},
        .sync_level = .full_index,
    });

    const cfg_a = db.core.index_manager.get("dense_a") orelse return error.TestUnexpectedResult;
    const repair_a = try db.ensureAutomaticDenseGenerationRepairIntent(
        alloc,
        cfg_a.*,
        .artifact_coverage_mismatch,
        "dense_artifact_coverage_mismatch",
    );
    const cfg_b = db.core.index_manager.get("dense_b") orelse return error.TestUnexpectedResult;
    const repair_b = try db.ensureAutomaticDenseGenerationRepairIntent(
        alloc,
        cfg_b.*,
        .artifact_coverage_mismatch,
        "dense_artifact_coverage_mismatch",
    );
    const counter_b = try DB.denseArtifactTargetCounterKeyAlloc(alloc, "dense_b");
    defer alloc.free(counter_b);
    try db.core.store.delete(counter_b);

    try std.testing.expectError(
        error.RestoreDenseCounterProofIncomplete,
        db.completeRestoreDenseArtifactRepairs(alloc),
    );
    try std.testing.expectEqual(
        repair_a,
        (try db.indexRepairIdForIndex(alloc, "dense_a")) orelse return error.TestUnexpectedResult,
    );
    try std.testing.expectEqual(
        repair_b,
        (try db.indexRepairIdForIndex(alloc, "dense_b")) orelse return error.TestUnexpectedResult,
    );
}

test "db restore dense artifact completion retires an obsolete invalid-generation shadow" {
    const alloc = std.testing.allocator;
    var test_directory = try TestDirectory.init("db");
    defer test_directory.cleanup();
    const path = test_directory.path().ptr;

    var db = try DB.open(alloc, std.mem.span(path), no_background_test_open);
    defer db.close();
    try db.addIndex(.{
        .name = "dense_idx",
        .kind = .dense_vector,
        .config_json = "{\"field\":\"embedding\",\"dims\":2,\"external\":true}",
    });
    try db.batch(.{
        .writes = &.{.{
            .key = "doc:a",
            .value = "{\"_embeddings\":{\"dense_idx\":[1,0]}}",
        }},
        .sync_level = .full_index,
    });

    const cfg = db.core.index_manager.get("dense_idx") orelse return error.TestUnexpectedResult;
    const repair_id = try db.ensureAutomaticDenseGenerationRepairIntent(
        alloc,
        cfg.*,
        .projection_generation_invalid,
        "dense_projection_checkpoint_repair_required",
    );
    const checkpoint = try db.core.loadProjectionCheckpoint(alloc, cfg.name);
    try db.core.saveProjectionCheckpoint(cfg.name, .{
        .applied_sequence = checkpoint.applied_sequence,
        .status = .repair_required,
        .generation = checkpoint.generation,
        .config_hash = types.indexConfigHash(cfg.*),
    });

    const shadow_base = try createUniqueRepairShadowBase(alloc, std.mem.span(path));
    defer alloc.free(shadow_base);
    const candidate_relative_path = try std.fmt.allocPrint(
        alloc,
        "{s}/indexes/{s}",
        .{ std.fs.path.basename(shadow_base), cfg.name },
    );
    defer alloc.free(candidate_relative_path);
    try db.updateIndexRepairIntent(alloc, repair_id, .{ .phase = .preflight });
    try db.updateIndexRepairIntent(alloc, repair_id, .{
        .phase = .building,
        .candidate_relative_path = candidate_relative_path,
        .replace_candidate_path = true,
    });
    try db.recordIndexRepairAttemptFailure(
        alloc,
        repair_id,
        @errorName(error.RepairSourceCoverageIncomplete),
        true,
    );

    try std.testing.expectEqual(
        DB.RestoreDenseArtifactCompletion.completed_with_progress,
        try db.completeRestoreDenseArtifactRepairs(alloc),
    );
    try std.testing.expect((try db.indexRepairIdForIndex(alloc, cfg.name)) == null);
    const completed_checkpoint = try db.core.loadProjectionCheckpoint(alloc, cfg.name);
    try std.testing.expectEqual(apply_state.ProjectionStatus.clean, completed_checkpoint.status);
    try std.testing.expectError(error.FileNotFound, std.Io.Dir.cwd().access(std.testing.io, shadow_base, .{}));
}

test "db restore final artifact recount preserves an exact native generation" {
    const alloc = std.testing.allocator;
    var test_directory = try TestDirectory.init("db");
    defer test_directory.cleanup();
    const path = test_directory.path().ptr;
    const primary_backend: PrimaryBackend = .{ .lsm = .{ .flush_threshold = 1 } };
    var lsm_cache = lsm_backend_mod.Cache.init(alloc, 8 * 1024 * 1024);
    defer lsm_cache.deinit();
    var hbc_cache = hbc_mod.Cache.init(alloc);
    defer hbc_cache.deinit();
    var resident_runtime = try background_runtime_mod.BackendRuntimeHandle.init(alloc, .{});
    defer resident_runtime.deinit();
    {
        var old_live = try DB.open(alloc, std.mem.span(path), .{
            .primary_backend = primary_backend,
            .lsm_cache = &lsm_cache,
            .hbc_cache = &hbc_cache,
            .backend_runtime = resident_runtime.ptr(),
            .lsm_root_generation = 41,
            .ttl_cleanup = .{ .enabled = false },
        });
        defer old_live.close();
        try old_live.addIndex(.{
            .name = "dense_idx",
            .kind = .dense_vector,
            .config_json = "{\"field\":\"embedding\",\"dims\":3,\"generator\":{\"kind\":\"dense_embedding\",\"source_field\":\"body\",\"chunk_name\":\"body_chunks_v1\",\"chunk_size\":8,\"chunk_overlap\":2}}",
        });
    }
    var transition = try generation_lifecycle.beginProcessExclusive(std.mem.span(path));
    defer transition.deinit();
    var staged = try transition.beginStaging();
    defer staged.deinit();

    {
        var deterministic = embedder_mod.DeterministicDenseEmbedder{};
        var db = try DB.open(alloc, staged.path(), .{
            .primary_backend = primary_backend,
            .lsm_root_generation = 41,
            .staged_generation = &staged,
            .enrichment = .{
                .owner_id = "worker-a",
                .dense_embedder = deterministic.interface(),
            },
            .ttl_cleanup = .{ .enabled = false },
        });
        defer db.close();
        try db.addIndex(.{
            .name = "dense_idx",
            .kind = .dense_vector,
            .config_json = "{\"field\":\"embedding\",\"dims\":3,\"generator\":{\"kind\":\"dense_embedding\",\"source_field\":\"body\",\"chunk_name\":\"body_chunks_v1\",\"chunk_size\":8,\"chunk_overlap\":2}}",
        });
        try db.batch(.{
            .writes = &.{.{
                .key = "doc:a",
                .value = "{\"body\":\"abcdefghijklmno\"}",
            }},
            .sync_level = .full_index,
        });
    }

    var expected_active: u64 = 0;
    {
        var db = try DB.open(alloc, staged.path(), .{
            .primary_backend = primary_backend,
            .lsm_root_generation = 41,
            .staged_generation = &staged,
            .start_index_workers = false,
            .start_optional_runtimes = false,
            .ttl_cleanup = .{ .enabled = false },
        });
        defer db.close();
        const cfg = db.core.index_manager.get("dense_idx") orelse return error.TestUnexpectedResult;
        const counter_key = try DB.denseArtifactTargetCounterKeyAlloc(alloc, cfg.name);
        defer alloc.free(counter_key);
        try db.core.store.delete(counter_key);
        _ = try db.ensureAutomaticDenseGenerationRepairIntent(
            alloc,
            cfg.*,
            .artifact_counter_missing,
            "dense_artifact_counter_missing",
        );
        const dense = db.core.index_manager.denseIndex("dense_idx") orelse return error.TestUnexpectedResult;
        expected_active = dense.index.stats().active_count;
        try std.testing.expect(expected_active > 0);
        // Native-v2 already has exact published coverage. Repair the missing
        // durable counter without inventing an unowned bulk session or
        // rebuilding the active root; the fresh owner below proves durability.
        const repaired = try db.repairRestoreDenseArtifactCoverageFromFinalArtifacts(alloc);
        try std.testing.expectEqual(@as(usize, 0), repaired.rebuilt);
        try std.testing.expect(repaired.made_progress);
        const completed_checkpoint = try db.core.loadProjectionCheckpoint(alloc, "dense_idx");
        try std.testing.expectEqual(apply_state.ProjectionStatus.clean, completed_checkpoint.status);
        try std.testing.expectEqual(
            DB.RestoreDenseArtifactCompletion.completed_with_progress,
            try db.completeRestoreDenseArtifactRepairs(alloc),
        );
        _ = try db.rebuildDenseIndexesFromStoredEmbeddingArtifactsOutcomeWithProgress(alloc, null, null);
        try db.sync(true);
        try db.syncIndexes(true);
    }
    try staged.seal();
    _ = try staged.publish();
    staged.deinit();
    transition.deinit();

    var reopened = try DB.open(alloc, std.mem.span(path), .{
        .primary_backend = primary_backend,
        .lsm_root_generation = 41,
        .lsm_cache = &lsm_cache,
        .hbc_cache = &hbc_cache,
        .backend_runtime = resident_runtime.ptr(),
        .open_mode = .query_readonly,
        .start_index_workers = false,
        .ttl_cleanup = .{ .enabled = false },
    });
    defer reopened.close();
    const dense = reopened.core.index_manager.denseIndex("dense_idx") orelse return error.TestUnexpectedResult;
    try std.testing.expectEqual(expected_active, dense.index.stats().active_count);
}

test "db restore durability proof retries reopen-only dense debt" {
    const alloc = std.testing.allocator;
    var test_directory = try TestDirectory.init("db");
    defer test_directory.cleanup();
    const path = test_directory.path().ptr;

    {
        var db = try DB.open(alloc, std.mem.span(path), no_background_test_open);
        defer db.close();
        try db.addIndex(.{
            .name = "dense_idx",
            .kind = .dense_vector,
            .config_json = "{\"field\":\"embedding\",\"dims\":2,\"external\":true}",
        });
        try db.batch(.{
            .writes = &.{.{
                .key = "doc:a",
                .value = "{\"_embeddings\":{\"dense_idx\":[1,0]}}",
            }},
            .sync_level = .full_index,
        });
        try DB.markRestorePrimaryRestoredForPathWithArtifact(
            alloc,
            std.mem.span(path),
            "snap1",
            "file:///tmp/backups",
            "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855",
            "snapshots/snap1",
            7001,
        );
        try DB.markRestoreRuntimeRepairCompleteWithIo(alloc, std.testing.io, std.mem.span(path));
        // Leave valid native authority with a stale completion watermark.
        // Resetting its selected directory would manufacture a corrupt root,
        // which restore must reject rather than treat as recoverable debt.
        const checkpoint = try db.core.loadProjectionCheckpoint(alloc, "dense_idx");
        try db.core.saveProjectionCheckpoint("dense_idx", .{
            .applied_sequence = 0,
            .status = .rebuilding,
            .generation = checkpoint.generation,
            .config_hash = checkpoint.config_hash,
        });
        try db.sync(true);
        try db.syncIndexes(true);
    }

    {
        var db = try DB.open(alloc, std.mem.span(path), .{
            .open_mode = .writer_no_replay,
            .start_index_workers = false,
            .start_optional_runtimes = false,
            .ttl_cleanup = .{ .enabled = false },
        });
        defer db.close();
        try std.testing.expect(try db.prepareRestoreDurabilityRetryIfNeeded(alloc));
        var attempts: usize = 0;
        while (try db.restoreRuntimeRepairNeeded()) : (attempts += 1) {
            try std.testing.expect(try db.repairRestoreRuntimeStateStepIfNeeded(alloc));
            try std.testing.expect(attempts < 16);
        }
    }

    var reopened = try DB.open(alloc, std.mem.span(path), .{
        .open_mode = .query_readonly,
        .start_index_workers = false,
        .ttl_cleanup = .{ .enabled = false },
    });
    defer reopened.close();
    const dense = reopened.core.index_manager.denseIndex("dense_idx") orelse return error.TestUnexpectedResult;
    try std.testing.expectEqual(@as(u64, 1), dense.index.stats().active_count);
    try std.testing.expect(!(try reopened.hasPendingDenseArtifactRebuild(alloc)));
}

test "db restore owner converges rediscovered dense projection intent" {
    const alloc = std.testing.allocator;
    var path_tmp = try TestDirectory.init("db");
    defer path_tmp.cleanup();
    const path = path_tmp.path().ptr;

    var db = try DB.open(alloc, std.mem.span(path), no_background_test_open);
    defer db.close();
    try db.addIndex(.{
        .name = "dense_idx",
        .kind = .dense_vector,
        .config_json = "{\"field\":\"embedding\",\"dims\":2,\"external\":true}",
    });
    try db.batch(.{
        .writes = &.{.{
            .key = "doc:a",
            .value = "{\"_embeddings\":{\"dense_idx\":[1,0]}}",
        }},
        .sync_level = .full_index,
    });

    const cfg = db.core.index_manager.get("dense_idx") orelse return error.TestUnexpectedResult;
    const checkpoint = try db.core.loadProjectionCheckpoint(alloc, cfg.name);
    try db.core.saveProjectionCheckpoint(cfg.name, .{
        .applied_sequence = checkpoint.applied_sequence,
        .status = .repair_required,
        .generation = checkpoint.generation,
        .config_hash = types.indexConfigHash(cfg.*),
    });

    const discovered = try db.rebuildDenseIndexesFromStoredEmbeddingArtifactsOutcomeWithProgress(alloc, null, null);
    try std.testing.expectEqual(@as(usize, 1), discovered.reported_work);
    try std.testing.expect(discovered.made_progress);
    try std.testing.expect(!discovered.dense_visibility_changed);

    const rediscovered = try db.rebuildDenseIndexesFromStoredEmbeddingArtifactsOutcomeWithProgress(alloc, null, null);
    // Durable repair ownership excludes duplicate in-place discovery work.
    try std.testing.expectEqual(@as(usize, 0), rediscovered.reported_work);
    try std.testing.expect((try db.indexRepairIdForIndex(alloc, "dense_idx")) != null);
    try std.testing.expect(!rediscovered.made_progress);
    try std.testing.expect(!rediscovered.dense_visibility_changed);

    try DB.markRestorePrimaryRestoredForPathWithArtifact(
        alloc,
        std.mem.span(path),
        "snap1",
        "file:///tmp/backups",
        "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855",
        "snapshots/snap1",
        7001,
    );
    try db.updateRestoreRuntimeRepairPhaseWithIo(alloc, std.testing.io, "sync_indexes", false);
    try std.testing.expect(try db.repairRestoreRuntimeStateStepIfNeeded(alloc));
    try std.testing.expect((try db.indexRepairIdForIndex(alloc, "dense_idx")) == null);
    var restore_state = (try DB.readRestoreStateForPathWithIo(alloc, std.testing.io, std.mem.span(path))).?;
    defer restore_state.deinit(alloc);
    try std.testing.expect(restore_state.runtime_repair_complete);
    try std.testing.expectEqualStrings("complete", restore_state.phase);
}

test "db restore dense rebuild publishes mixed progress before worker wait" {
    const alloc = std.testing.allocator;
    var path_tmp = try TestDirectory.init("db");
    defer path_tmp.cleanup();
    const path = path_tmp.path().ptr;

    var db = try DB.open(alloc, std.mem.span(path), no_background_test_open);
    defer db.close();
    inline for (.{ "dense_a", "dense_b" }) |name| {
        try db.addIndex(.{
            .name = name,
            .kind = .dense_vector,
            .config_json = "{\"field\":\"embedding\",\"dims\":2,\"external\":true}",
        });
    }
    try db.batch(.{
        .writes = &.{.{
            .key = "doc:a",
            .value = "{\"_embeddings\":{\"dense_a\":[1,0],\"dense_b\":[0,1]}}",
        }},
        .sync_level = .full_index,
    });

    try resetDenseIndexForArtifactRebuildForTest(&db, "dense_a");
    var ghost_vector = [_]f32{ 1, 1 };
    try db.core.index_manager.denseIndex("dense_b").?.index.insertVectorForTest(0xdead_beef, &ghost_vector);
    try DB.markRestorePrimaryRestoredForPathWithArtifact(
        alloc,
        std.mem.span(path),
        "snap1",
        "file:///tmp/backups",
        "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855",
        "snapshots/snap1",
        7001,
    );
    try db.updateRestoreRuntimeRepairPhaseWithIo(alloc, std.testing.io, "rebuild_replayed_artifacts", false);

    // Simulate a resident managed writer without racing a real worker against
    // this deterministic step test.
    db.start_index_workers = true;
    try std.testing.expect(try db.repairRestoreRuntimeStateStepIfNeeded(alloc));
    try std.testing.expectEqual(
        @as(u64, 1),
        db.core.index_manager.denseIndex("dense_a").?.index.stats().active_count,
    );
    try std.testing.expect((try db.indexRepairIdForIndex(alloc, "dense_b")) != null);

    var restore_state = (try DB.readRestoreStateForPathWithIo(alloc, std.testing.io, std.mem.span(path))).?;
    defer restore_state.deinit(alloc);
    try std.testing.expectEqualStrings("rebuild_replayed_artifacts", restore_state.phase);
    // Watermark repair can publish one more bounded progress quantum after
    // rebuilding. Once drained, the pending owner must yield, not spin.
    try std.testing.expect(try db.repairRestoreRuntimeStateStepIfNeeded(alloc));
    var waiting = (try DB.readRestoreStateForPathWithIo(alloc, std.testing.io, std.mem.span(path))).?;
    defer waiting.deinit(alloc);
    try std.testing.expectEqualStrings("rebuild_replayed_artifacts", waiting.phase);
    try std.testing.expectError(error.RestoreRuntimeRepairIncomplete, db.repairRestoreRuntimeStateStepIfNeeded(alloc));
    try std.testing.expect(!waiting.runtime_repair_complete);
    try std.testing.expect((try db.indexRepairIdForIndex(alloc, "dense_b")) != null);
}

test "db restore repair does not complete before regenerated chunk embeddings are query visible" {
    const alloc = std.testing.allocator;

    var src_tmp = try TestDirectory.init("db");
    defer src_tmp.cleanup();
    const src_path = src_tmp.path().ptr;

    var restore_tmp = try TestDirectory.init("db");
    defer restore_tmp.cleanup();
    const restore_path = restore_tmp.path().ptr;

    const primary_backend: PrimaryBackend = .{ .lsm = .{ .flush_threshold = 1 } };
    var portable = std.ArrayList(u8).empty;
    defer portable.deinit(alloc);

    {
        var deterministic = embedder_mod.DeterministicDenseEmbedder{};
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

        try portable_backup.exportPortable(alloc, db.core.store, &portable);
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
        try portable_backup.importPortable(alloc, restored.core.store, portable.items);

        const chunk_prefix = try internal_keys.artifactNamedPrefixAlloc(alloc, "doc:a", "chunk", "body_chunks_v1");
        defer alloc.free(chunk_prefix);
        const chunk_records = try restored.core.store.scanPrefix(alloc, chunk_prefix);
        defer docstore_mod.DocStore.freeResults(alloc, chunk_records);
        try std.testing.expect(chunk_records.len > 0);
        for (chunk_records) |chunk| {
            const embedding_key = try internal_keys.derivedEmbeddingArtifactKeyAlloc(alloc, chunk.key, "dv_v1");
            defer alloc.free(embedding_key);
            try restored.core.store.delete(embedding_key);
        }

        // A restored source checkpoint can equal the destination's first
        // regenerated replay sequence even though the destination runtime
        // index is empty. Repair must reset this projection boundary or the
        // enrichment worker will skip sequence 1 as already applied.
        try enrichment_state.saveProjectionCheckpoint(
            restored.core.store,
            enrichment_runtime_mod.scope_name,
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
        var deterministic = embedder_mod.DeterministicDenseEmbedder{};
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
            try std.testing.expect(!restored.enrichment_runtime.?.isStarted());
        }
    }

    {
        var deterministic = embedder_mod.DeterministicDenseEmbedder{};
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
        var after = try waitForSearchResult(alloc, &restored, .{
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

test "db restore state uses strict structured content identity markers" {
    const alloc = std.testing.allocator;
    var path_tmp = try TestDirectory.init("db");
    defer path_tmp.cleanup();
    const path = path_tmp.path().ptr;
    const location = "file:///tmp/backups?note=line\nphase=complete";
    const artifact_sha256 = "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855";

    try DB.markRestorePrimaryRestoredForPathWithArtifact(
        alloc,
        std.mem.span(path),
        "snap1",
        location,
        artifact_sha256,
        "snapshots/snap1",
        7001,
    );
    {
        var state = (try DB.readRestoreStateForPath(alloc, std.mem.span(path))).?;
        defer state.deinit(alloc);
        try std.testing.expectEqualStrings(location, state.location);
        try std.testing.expectEqualStrings(artifact_sha256, state.artifact_sha256);
        try std.testing.expectEqualStrings("runtime_repair", state.phase);
        try std.testing.expectEqualStrings("", state.graph_ownership_cursor);
        const cursor = try bytesToHexAlloc(alloc, "\x01artifact:cursor");
        alloc.free(state.graph_ownership_cursor);
        state.graph_ownership_cursor = cursor;
        try writeRestoreStateForPath(alloc, std.mem.span(path), state);
    }
    {
        var state = (try DB.readRestoreStateForPath(alloc, std.mem.span(path))).?;
        defer state.deinit(alloc);
        const cursor = try hexToBytesAlloc(alloc, state.graph_ownership_cursor);
        defer alloc.free(cursor);
        try std.testing.expectEqualStrings("\x01artifact:cursor", cursor);
    }

    const state_path = try restoreStateMarkerPathAlloc(alloc, std.mem.span(path));
    defer alloc.free(state_path);
    var io_impl = threadedIo();
    defer io_impl.deinit();
    try std.Io.Dir.cwd().writeFile(io_impl.io(), .{
        .sub_path = state_path,
        .data = "restore_state_v2\nbackup_id=snap1\nlocation=file:///tmp/backups\n",
    });
    try std.testing.expectError(
        error.InvalidRestoreState,
        DB.readRestoreStateForPath(alloc, std.mem.span(path)),
    );
}

test "db restore graph ownership replay persists a bounded page cursor" {
    const alloc = std.testing.allocator;
    var path_tmp = try TestDirectory.init("db");
    defer path_tmp.cleanup();
    const path = path_tmp.path().ptr;

    var db = try DB.open(alloc, std.mem.span(path), .{ .start_index_workers = false });
    defer db.close();
    try db.addIndex(.{
        .name = "relations_graph",
        .kind = .graph,
        .config_json = "{\"edge_types\":[{\"name\":\"mentions\"}]}",
    });

    const document_count = 513;
    const writes = try alloc.alloc(types.BatchWrite, document_count);
    defer alloc.free(writes);
    var initialized: usize = 0;
    defer for (writes[0..initialized]) |write| alloc.free(write.key);
    for (writes, 0..) |*write, i| {
        write.* = .{
            .key = try std.fmt.allocPrint(alloc, "doc:{d:0>4}", .{i}),
            .value = "{}",
        };
        initialized += 1;
    }
    try db.batch(.{ .writes = writes });

    try DB.markRestorePrimaryRestoredForPathWithArtifact(
        alloc,
        std.mem.span(path),
        "snap1",
        "file:///tmp/backups",
        "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855",
        "snapshots/snap1",
        7001,
    );
    try db.updateRestoreRuntimeRepairPhaseWithIo(alloc, std.testing.io, "rebuild_graph", false);

    try std.testing.expect(try db.repairRestoreRuntimeStateStepIfNeeded(alloc));
    {
        var state = (try DB.readRestoreStateForPath(alloc, std.mem.span(path))).?;
        defer state.deinit(alloc);
        try std.testing.expectEqualStrings("rebuild_graph", state.phase);
        try std.testing.expect(state.graph_ownership_cursor.len > 0);
    }

    var page_steps: usize = 1;
    while (true) {
        var before = (try DB.readRestoreStateForPath(alloc, std.mem.span(path))).?;
        defer before.deinit(alloc);
        if (!std.mem.eql(u8, before.phase, "rebuild_graph")) break;
        try std.testing.expect(page_steps < 16);
        const cursor_before = try alloc.dupe(u8, before.graph_ownership_cursor);
        defer alloc.free(cursor_before);

        try std.testing.expect(try db.repairRestoreRuntimeStateStepIfNeeded(alloc));
        page_steps += 1;
        var after = (try DB.readRestoreStateForPath(alloc, std.mem.span(path))).?;
        defer after.deinit(alloc);
        if (std.mem.eql(u8, after.phase, "rebuild_graph")) {
            try std.testing.expect(!std.mem.eql(u8, cursor_before, after.graph_ownership_cursor));
        } else {
            try std.testing.expectEqualStrings("rebuild_artifacts", after.phase);
            try std.testing.expectEqualStrings("", after.graph_ownership_cursor);
        }
    }
    try std.testing.expect(page_steps > 1);
}
