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

//! Private compiled local storage owner. Calls cover whole lifecycle, mutation,
//! query, schema, index and maintenance operations. Record loops stay in owner.
const std = @import("std");
pub const enabled = @import("engine_selection.zig").enabled;
const db = @import("db/db.zig");
const DB = db.DB;
const Allocator = std.mem.Allocator;
const types = @import("db/types.zig");
const BatchProfile = db.BatchProfile;
const BatchExecutionOptions = @import("db/execution_resources.zig").BatchExecutionOptions;
const SearchWithDenseProfileResult = db.SearchWithDenseProfileResult;
const SearchWithCapturedRequestResult = db.SearchWithCapturedRequestResult;
const schema_mod = @import("schema.zig");
const OpenOptions = db.OpenOptions;
const native = @import("antfly_runtime_abi").native_abi;
const callback = @import("antfly_runtime_abi").callback_abi;
pub const Failure = @import("native_outcome.zig").Failure;
pub const Outcome = @import("native_outcome.zig").Outcome;
pub const ReplayCallbacks = struct { progress: db.ReplayProgressHook };
pub const ReplayBoundary = @import("named_callback.zig").Boundary(ReplayCallbacks);
pub const BackupPlan = struct { value: @import("../api/backup_contract.zig").TableBackupPlan, io: ?native.IoBorrow };
/// Restore owns a disposable generation; keep its record loops and errors in
/// the physical storage unit. Callbacks here return no Zig error values.
pub const PortableRestoreImportOptions = struct {
    identity_namespace: ?@import("db/doc_identity.zig").Namespace = null,
    embedding_source_fields: []const @import("portable_backup.zig").ImportOptions.EmbeddingSourceField = &.{},
    cancellation: types.CancellationToken = .none,
};
pub const WriterBorrow = @import("native_writer.zig").Borrow;
pub const PortableExportOptions = struct {
    value: @import("portable_backup.zig").ExportOptions,
    spool: ?struct { io: native.IoBorrow, file: std.Io.File },
};

pub const VTable = struct {
    portableExportToWriterWithOptions: *const fn (Allocator, *@import("docstore.zig").DocStore, WriterBorrow, PortableExportOptions) Outcome(void),
    openVerifiedRestoreSource: *const fn (staged_generation: *const @import("db/generation_lifecycle.zig").StagedGeneration, alloc: std.mem.Allocator, snapshot_root: []const u8, path: []const u8, opts: db.OpenOptions, expected_namespace: @import("db/doc_identity.zig").Namespace) Outcome(DB),
    restoreSnapshotToDeferredRuntimeRepairWithIoAndCancellation: *const fn (staged_generation: *const @import("db/generation_lifecycle.zig").StagedGeneration, alloc: std.mem.Allocator, io: native.IoBorrow, snapshot_root: []const u8, path: []const u8, opts: db.OpenOptions, identity: db.RestoreIdentity, cancellation: types.CancellationToken) Outcome(void),
    restoreMaterializedNativeGenerationToDeferredRuntimeRepairWithIoAndCancellation: *const fn (staged_generation: *const @import("db/generation_lifecycle.zig").StagedGeneration, alloc: std.mem.Allocator, io: native.IoBorrow, primary_snapshot_root: []const u8, path: []const u8, opts: db.OpenOptions, identity: db.RestoreIdentity, native_generation: *@import("db/native_backup.zig").LoadedManifest, cancellation: types.CancellationToken) Outcome(void),
    restoreMaterializedNativePrimaryAndPlanProjectionsWithIoAndCancellation: *const fn (staged_generation: *const @import("db/generation_lifecycle.zig").StagedGeneration, alloc: std.mem.Allocator, io: native.IoBorrow, primary_snapshot_root: []const u8, path: []const u8, opts: db.OpenOptions, identity: db.RestoreIdentity, native_generation: *@import("db/native_backup.zig").LoadedManifest, cancellation: types.CancellationToken) Outcome(void),
    finishMaterializedNativeGenerationWithIo: *const fn (staged_generation: *const @import("db/generation_lifecycle.zig").StagedGeneration, alloc: std.mem.Allocator, io: native.IoBorrow, path: []const u8, opts: db.OpenOptions, native_generation: *@import("db/native_backup.zig").LoadedManifest, complete_runtime_repair: bool) Outcome(void),
    restoreSnapshotToLocalDeferredRuntimeRepairWithIoAndCancellation: *const fn (staged_generation: *const @import("db/generation_lifecycle.zig").StagedGeneration, alloc: std.mem.Allocator, io: native.IoBorrow, snapshot_root: []const u8, path: []const u8, opts: db.OpenOptions, cancellation: types.CancellationToken) Outcome(bool),
    markRestorePrimaryRestoredForPathWithArtifactWithIo: *const fn (alloc: std.mem.Allocator, io: native.IoBorrow, path: []const u8, backup_id: []const u8, location: []const u8, artifact_sha256: []const u8, snapshot_path: []const u8, group_id: u64) Outcome(void),
    portableExport: *const fn (std.mem.Allocator, *@import("docstore.zig").DocStore, *std.ArrayList(u8)) Outcome(void),
    resolutionDeinit: *const fn (self: *@import("db/resolution_runtime.zig").ResolutionRuntime) Outcome(void),
    resolutionStart: *const fn (self: *@import("db/resolution_runtime.zig").ResolutionRuntime) Outcome(void),
    resolutionStop: *const fn (self: *@import("db/resolution_runtime.zig").ResolutionRuntime) Outcome(void),
    resolutionCatchUp: *const fn (self: *@import("db/resolution_runtime.zig").ResolutionRuntime) Outcome(void),
    resolutionCatchUpOnce: *const fn (self: *@import("db/resolution_runtime.zig").ResolutionRuntime, deadline_ns: u64) Outcome(void),
    resolutionRequestReresolveBacklog: *const fn (self: *@import("db/resolution_runtime.zig").ResolutionRuntime) Outcome(void),
    resolutionHasReresolveBacklog: *const fn (self: *@import("db/resolution_runtime.zig").ResolutionRuntime) Outcome(bool),
    resolutionRunReresolveBacklogWindow: *const fn (self: *@import("db/resolution_runtime.zig").ResolutionRuntime) Outcome(@import("db/resolution_runtime.zig").ReresolveEnqueueResult),
    resolutionPendingReviews: *const fn (self: *@import("db/resolution_runtime.zig").ResolutionRuntime, alloc: std.mem.Allocator) Outcome([]@import("db/resolution_runtime.zig").PendingReview),
    promotionDeinit: *const fn (self: *@import("db/promotion_runtime.zig").PromotionRuntime) Outcome(void),
    promotionStart: *const fn (self: *@import("db/promotion_runtime.zig").PromotionRuntime) Outcome(void),
    promotionStop: *const fn (self: *@import("db/promotion_runtime.zig").PromotionRuntime) Outcome(void),
    promotionCatchUp: *const fn (self: *@import("db/promotion_runtime.zig").PromotionRuntime) Outcome(void),
    pinnedSnapshotDeinit: *const fn (self: *@import("db/core.zig").PinnedStoreSnapshot) Outcome(void),
    pinnedSnapshotMaterializeWithSink: *const fn (self: *@import("db/core.zig").PinnedStoreSnapshot, alloc: Allocator, io: native.IoBorrow, snapshot_root: []const u8, cancellation: types.CancellationToken, sink: ?@import("native_artifact_sink.zig").Sink) Outcome(u64),
    sourcePinLocate: *const fn (db: *DB, scope: @import("db/online_source.zig").Scope) Outcome(@import("db/online_source.zig").Located),
    sourcePinEnsureAssumeApply: *const fn (db: *DB, scope: @import("db/online_source.zig").Scope) Outcome(void),
    sourcePinPublicationCertificateIfPresent: *const fn (db: *DB, scope: @import("db/online_source.zig").Scope, cancellation: types.CancellationToken) Outcome(?@import("source_snapshot.zig").Certificate),
    sourcePinPreparePublication: *const fn (db: *DB, scope: @import("db/online_source.zig").Scope, cancellation: types.CancellationToken) Outcome(@import("source_snapshot.zig").Certificate),
    sourcePinReclaimReleased: *const fn (db: *DB, scope: @import("db/online_source.zig").Scope) Outcome(void),
    sourcePinReconcileReleasedWithBudget: *const fn (db: *DB, budget: @import("db/source_pin_gc.zig").Budget) Outcome(@import("db/source_pin_gc.zig").Work),
    writeCorruptEmbeddingArtifactInDb: *const fn (alloc: std.mem.Allocator, db: *@import("db/mod.zig").DB, doc_key: []const u8, index_name: []const u8) Outcome(bool),
    writeConfigureRestoreOwnerDb: *const fn (alloc: std.mem.Allocator, db: *@import("db/mod.zig").DB, bootstrap: @import("db/restore_staging_contract.zig").OwnerBootstrap, allow_canceled: bool, hot_standby_replay: bool) Outcome(void),
    writeCreateManagedDbEnrichments: *const fn (allocator: std.mem.Allocator, raw_indexes_json: []const u8, runtime: ?*@import("db/mod.zig").background_runtime.BackendRuntime, local_provider: ?@import("../inference/managed_embedder.zig").AntflyProvider, remote_capability_cache: ?*@import("antfly_inference_remote_capabilities").Cache, inference_api_url: ?[]const u8, source_table: []const u8, store: ?*@import("../common/secrets.zig").FileStore, remote: ?*const @import("write.zig").scraping.RemoteContentConfig) Outcome(@import("write.zig").ManagedDbEnrichmentSet),
    writeReconfigureManagedDbEnrichmentRuntime: *const fn (ignored_allocator: std.mem.Allocator, db: *@import("db/mod.zig").DB, indexes_json: []const u8, backend_runtime: ?*@import("db/mod.zig").background_runtime.BackendRuntime, antfly_provider: ?@import("../inference/managed_embedder.zig").AntflyProvider, remote_capability_cache: ?*@import("antfly_inference_remote_capabilities").Cache, inference_api_url: ?[]const u8, source_table: []const u8, secret_store: ?*@import("../common/secrets.zig").FileStore, remote_content: ?*const @import("write.zig").scraping.RemoteContentConfig) Outcome(void),
    writePrepareManagedSchemaBeforeIndexLoad: *const fn (alloc: std.mem.Allocator, mode: @import("../api/write_contract.zig").ManagedDbOpenMode, schema_json: ?[]const u8) Outcome(?@import("db/mod.zig").SchemaBeforeIndexLoad),
    writeOpenManagedDbWithIndexesJsonAndCacheModeWithRuntimeAndLocalAntflyAndIdentityWithOptions: *const fn (alloc: std.mem.Allocator, path: []const u8, indexes_json: []const u8, lsm_cache: ?*@import("lsm_backend/mod.zig").Cache, hbc_cache: ?*@import("hbc_adapter.zig").Cache, lsm_root_generation: u64, resource_manager: ?*@import("resource_manager.zig").ResourceManager, mode: @import("../api/write_contract.zig").ManagedDbOpenMode, backend_runtime: ?*@import("db/mod.zig").background_runtime.BackendRuntime, antfly_provider: ?@import("../inference/managed_embedder.zig").AntflyProvider, secret_store: ?*@import("../common/secrets.zig").FileStore, remote_content: ?*const @import("write.zig").scraping.RemoteContentConfig, identity_namespace: ?@import("db/doc_identity.zig").Namespace, options: @import("write.zig").ManagedDbOpenOptions) Outcome(@import("db/mod.zig").DB),
    writeLoadLocalTableSchemaJson: *const fn (alloc: std.mem.Allocator, db: *@import("db/mod.zig").DB) Outcome(?[]u8),
    writeExportPortableBackupShard: *const fn (alloc: std.mem.Allocator, db: *@import("db/mod.zig").DB, backup_root: []const u8, backup_id: []const u8, group_id: u64, shared_io: ?native.IoBorrow) Outcome([]@import("../api/backups.zig").ShardSnapshot),
    writeExportPortableBackupShardWithSeal: *const fn (alloc: std.mem.Allocator, db: *@import("db/mod.zig").DB, backup_root: []const u8, backup_id: []const u8, group_id: u64, shared_io: ?native.IoBorrow, sealed: ?@import("../api/backup_contract.zig").SealedHandle, cancellation: @import("db/mod.zig").types.CancellationToken) Outcome([]@import("../api/backups.zig").ShardSnapshot),
    writeCopyPreparedNativeBackupShardSnapshot: *const fn (alloc: std.mem.Allocator, native_snapshot: *@import("write.zig").NativeBackupShardSnapshot) Outcome([]@import("../api/backups.zig").ShardSnapshot),
    writeBackupStorageKernelOwnerDb: *const fn (alloc: std.mem.Allocator, db: *@import("db/mod.zig").DB, db_path: []const u8, group_id: u64, backup_root: []const u8, backup_id: []const u8, format: @import("../api/backups.zig").BackupFormat) Outcome([]@import("../api/backups.zig").ShardSnapshot),
    writeBackupStorageKernelOwnerDbWithControl: *const fn (alloc: std.mem.Allocator, db: *@import("db/mod.zig").DB, db_path: []const u8, group_id: u64, backup_root: []const u8, backup_id: []const u8, format: @import("../api/backups.zig").BackupFormat, cohort: ?@import("db/relational_integrity_topology_contract.zig").Fence, sealed: ?@import("db/native_backup_seal_contract.zig").Handle, control: @import("../api/backups.zig").BackupOperationControl) Outcome([]@import("../api/backups.zig").ShardSnapshot),
    writeExportPortableBackupFile: *const fn (alloc: std.mem.Allocator, store: *@import("db/mod.zig").docstore.DocStore, path: []const u8, shared_io: ?native.IoBorrow) Outcome(void),
    writeExportPortableBackupFileWithIo: *const fn (alloc: std.mem.Allocator, store: *@import("db/mod.zig").docstore.DocStore, path: []const u8, io: native.IoBorrow) Outcome(void),
    writeResolveWritesForSchemaValidation: *const fn (alloc: std.mem.Allocator, db: *@import("db/mod.zig").DB, base_writes: []const @import("db/mod.zig").types.BatchWrite, deletes: []const []const u8, transforms: []const @import("db/mod.zig").types.DocumentTransform) Outcome([]@import("db/mod.zig").types.BatchWrite),
    writeValidateTableBatchAgainstLocalSchema: *const fn (alloc: std.mem.Allocator, db: *@import("db/mod.zig").DB, writes: []const @import("db/mod.zig").types.BatchWrite, deletes: []const []const u8, transforms: []const @import("db/mod.zig").types.DocumentTransform) Outcome(void),
    writeApplyLocalTableSchemaJson: *const fn (alloc: std.mem.Allocator, db: *@import("db/mod.zig").DB, schema_json: []const u8) Outcome(void),
    writePrepareOwnerSchemaBeforeIndexLoad: *const fn (alloc: std.mem.Allocator, schema_json: []const u8) Outcome(?@import("db/mod.zig").SchemaBeforeIndexLoad),
    writeConfigureStorageKernelOwnerDb: *const fn (alloc: std.mem.Allocator, db: *@import("db/mod.zig").DB, table_name: []const u8, schema_json: []const u8, indexes_json: []const u8, backend_runtime: ?*@import("db/mod.zig").background_runtime.BackendRuntime, antfly_provider: ?@import("../inference/managed_embedder.zig").AntflyProvider, secret_store: ?*@import("../common/secrets.zig").FileStore, remote_content: ?*const @import("write.zig").scraping.RemoteContentConfig, installed: ?*@import("write.zig").OwnerManagedConfig) Outcome(void),
    writeConfigureStorageKernelOwnerDbAtOpen: *const fn (alloc: std.mem.Allocator, db: *@import("db/mod.zig").DB, table_name: []const u8, schema_json: []const u8, indexes_json: []const u8, backend_runtime: ?*@import("db/mod.zig").background_runtime.BackendRuntime, antfly_provider: ?@import("../inference/managed_embedder.zig").AntflyProvider, secret_store: ?*@import("../common/secrets.zig").FileStore, remote_content: ?*const @import("write.zig").scraping.RemoteContentConfig, installed: ?*@import("write.zig").OwnerManagedConfig, historical_ordered_apply: bool) Outcome(bool),
    writeOpenStorageKernelRestoreDb: *const fn (alloc: std.mem.Allocator, path: []const u8, indexes_json: []const u8, lsm_root_generation: u64, backend_runtime: *@import("db/mod.zig").background_runtime.BackendRuntime, identity_namespace: ?@import("db/doc_identity.zig").Namespace, prepared_restore: *const @import("backup_restore.zig").PreparedRestore) Outcome(@import("db/mod.zig").DB),
    writeRepairStorageKernelRestoreDb: *const fn (alloc: std.mem.Allocator, db: *@import("db/mod.zig").DB, group_id: u64, schema_json: []const u8, indexes_json: []const u8, cancellation: @import("db/mod.zig").types.CancellationToken) Outcome(void),
    writeReconcileStorageKernelOwnerDb: *const fn (alloc: std.mem.Allocator, db: *@import("db/mod.zig").DB, table_name: []const u8, schema_json: []const u8, indexes_json: []const u8, target_index_name: ?[]const u8, advance_index_repair: bool, backend_runtime: ?*@import("db/mod.zig").background_runtime.BackendRuntime, antfly_provider: ?@import("../inference/managed_embedder.zig").AntflyProvider, installed: ?*@import("write.zig").OwnerManagedConfig) Outcome(@import("../api/write_contract.zig").StorageKernelReconcileResult),
    writeRepairStorageKernelOwnerDb: *const fn (alloc: std.mem.Allocator, db: *@import("db/mod.zig").DB, target_index_name: ?[]const u8, advance_index_repair: bool, options: @import("db/mod.zig").types.ArtifactRepairRunOptions) Outcome(@import("../api/write_contract.zig").StorageKernelReconcileResult),
    writeValidateProvisionedDbIdentityNamespaceWithPolicy: *const fn (expected: ?@import("db/doc_identity.zig").Namespace, validation: @import("../api/write_contract.zig").StartupCatchUpMetadata.IdentityValidation, db: *const @import("db/mod.zig").DB) Outcome(void),
    writeBatchUsesDurableTransactionContract: *const fn (alloc: std.mem.Allocator, db: *@import("db/mod.zig").DB, req: @import("db/mod.zig").types.BatchRequest) Outcome(bool),
    writeTransactionUsesDurableContract: *const fn (alloc: std.mem.Allocator, db: *@import("db/mod.zig").DB, txn_id: @import("db/mod.zig").types.TxnId) Outcome(bool),
    writeApplyGraphMetricActionToDb: *const fn (alloc: std.mem.Allocator, db: *@import("db/mod.zig").DB, index_name: []const u8, metric_name: []const u8, action: []const u8) Outcome(@import("db/mod.zig").types.GraphMetricStatus),
    writeRunGraphMetricMaintenanceOrActionJsonAlloc: *const fn (alloc: std.mem.Allocator, db: *@import("db/mod.zig").DB, body: []const u8) Outcome([]u8),
    writeReconfigureManagedDbEnrichmentRuntimePaused: *const fn (ignored_allocator: std.mem.Allocator, db: *@import("db/mod.zig").DB, indexes_json: []const u8, backend_runtime: ?*@import("db/mod.zig").background_runtime.BackendRuntime, antfly_provider: ?@import("../inference/managed_embedder.zig").AntflyProvider, remote_capability_cache: ?*@import("antfly_inference_remote_capabilities").Cache, inference_api_url: ?[]const u8, source_table: []const u8, secret_store: ?*@import("../common/secrets.zig").FileStore, remote_content: ?*const @import("write.zig").scraping.RemoteContentConfig) Outcome(void),
    writeExportPortableBackupFileWithSource: *const fn (alloc: std.mem.Allocator, store: *@import("db/mod.zig").docstore.DocStore, path: []const u8, io: native.IoBorrow, sealed: ?@import("write.zig").PortableCohortSource) Outcome(void),
    managedDbEnrichmentSetDeinit: *const fn (*@import("write.zig").ManagedDbEnrichmentSet, Allocator) Outcome(void),
    nativeBackupSnapshotDeinit: *const fn (*@import("write.zig").NativeBackupShardSnapshot, Allocator) Outcome(void),
    writePrepareNativeBackupShardSnapshot: *const fn (Allocator, *DB, []const u8, u64, BackupPlan) Outcome(@import("write.zig").NativeBackupShardSnapshot),
    portableDecoderStep: *const fn (Allocator, native.IoBorrow, std.Io.File, @import("portable_decoder.zig").Plan, types.CancellationToken) Outcome(bool),
    portableDecoderBindRange: *const fn (Allocator, *@import("docstore.zig").DocStore, types.ByteRange) Outcome(void),
    requireRewriteSourceManifest: *const fn (*DB, Allocator, *const @import("db/relational_rewrite_program.zig").ProgramSet, types.CancellationToken) Outcome(void),
    verifyColdInitialChildSeed: *const fn (*DB, @import("db/relational_initial_child_publication.zig").Bootstrap) Outcome(void),
    syncPrimaryStore: *const fn (*DB) Outcome(void),

    queryValidateGraphHydrateResolvedDocFilterForDb: *const fn (req: @import("../api/graph.zig").GraphHydrateRequest, db: *@import("db/mod.zig").DB) Outcome(void),
    queryExecuteStorageKernelGraphExpand: *const fn (alloc: std.mem.Allocator, db: *@import("db/mod.zig").DB, table_name: []const u8, req: @import("../api/graph.zig").GraphExpandRequest) Outcome(@import("../api/graph.zig").GraphExpandResponse),
    queryExecuteStorageKernelGraphHydrate: *const fn (alloc: std.mem.Allocator, db: *@import("db/mod.zig").DB, req: @import("../api/graph.zig").GraphHydrateRequest) Outcome(@import("../api/graph.zig").GraphHydrateResponse),
    queryExecuteStorageKernelGraphEdges: *const fn (alloc: std.mem.Allocator, db: *@import("db/mod.zig").DB, req: @import("../api/graph.zig").GraphEdgesRequest) Outcome(@import("../api/graph.zig").GraphEdgesResponse),
    queryValidateGraphHydrateIncomingIndexIdentity: *const fn (req: @import("../api/graph.zig").GraphHydrateRequest, db: *@import("db/mod.zig").DB) Outcome(void),
    queryCurrentIdentityReadGenerationForDb: *const fn (requested: ?u64, db: *@import("db/mod.zig").DB) Outcome(u64),
    queryAlgebraicIndexFreshEnoughForName: *const fn (alloc: std.mem.Allocator, index_name_opt: ?[]const u8, db: *@import("db/mod.zig").DB) Outcome(bool),
    queryCollectTextStatsFromDbForRequest: *const fn (alloc: std.mem.Allocator, db: *@import("db/mod.zig").DB, request: @import("../api/query_execution_contract.zig").ParsedTextStatsRequest) Outcome([]const @import("../search/distributed_stats.zig").TextFieldStats),
    queryCollectBackgroundTextStatsFromDbForRequest: *const fn (alloc: std.mem.Allocator, db: *@import("db/mod.zig").DB, request: @import("../api/query_execution_contract.zig").ParsedTextStatsRequest) Outcome([]const @import("db/mod.zig").aggregations.DistributedBackgroundTextStats),
    queryCollectAlgebraicPartialsFromDbForRequest: *const fn (alloc: std.mem.Allocator, db: *@import("db/mod.zig").DB, request: @import("../api/query_execution_contract.zig").ParsedAlgebraicPartialsRequest) Outcome([]@import("db/mod.zig").algebraic.distributed.Partial),
    queryAlgebraicTensorProgramOutputExpressionsForIndexAlloc: *const fn (alloc: std.mem.Allocator, index: ?*const @import("db/mod.zig").algebraic.index.Index, access_paths: []@import("../api/query_execution_contract.zig").OwnedAlgebraicTensorAccessPath, program: *const @import("../api/query_execution_contract.zig").OwnedAlgebraicTensorProgram) Outcome([]@import("../api/query_execution_contract.zig").algebraic_ir.TensorExpr),
    queryAlgebraicTensorProgramOutputExpressionForStep: *const fn (alloc: std.mem.Allocator, index: ?*const @import("db/mod.zig").algebraic.index.Index, path_values: []const @import("../api/query_execution_contract.zig").algebraic_ir.PhysicalAccessPath, expr: @import("../api/query_execution_contract.zig").algebraic_ir.TensorExpr) Outcome(@import("../api/query_execution_contract.zig").algebraic_ir.TensorExpr),
    queryFindAlgebraicMaterialization: *const fn (index: *const @import("db/mod.zig").algebraic.index.Index, name: []const u8) Outcome(?@import("db/mod.zig").algebraic.index.MaterializationConfig),
    queryExecuteStorageKernelTextStats: *const fn (alloc: std.mem.Allocator, db: *@import("db/mod.zig").DB, table_name: []const u8, body: []const u8) Outcome([]u8),
    queryExecuteStorageKernelAlgebraicPartials: *const fn (alloc: std.mem.Allocator, db: *@import("db/mod.zig").DB, body: []const u8) Outcome([]u8),
    queryExecuteStorageKernelPreflight: *const fn (alloc: std.mem.Allocator, db: *@import("db/mod.zig").DB, table_name: []const u8, request_json: []const u8) Outcome([]u8),
    computeSearchAggregations: *const fn (Allocator, []const @import("db/aggregations.zig").SearchAggregationRequest, types.SearchResult, @import("db/aggregations.zig").Context) Outcome([]@import("db/aggregations.zig").SearchAggregationResult),
    batchReplicatedApply: *const fn (self: *DB, req: types.BatchRequest) Outcome(void),
    applyReplicatedRowPolicyPublication: *const fn (self: *DB, bundle_bytes: []const u8, request: @import("../system_catalog/policies.zig").InstallRequest, identity: db.OrderedApplyReceipt) Outcome(?@import("db/row_policy_bundle.zig").Receipt),
    recoverReplicatedRestoreFinish: *const fn (self: *DB, finish: @FieldType(@import("db/restore_staging_contract.zig").Control, "finish")) Outcome(void),
    reprocessDocumentArtifact: *const fn (self: *DB, alloc: std.mem.Allocator, doc_key: []const u8, artifact_name: []const u8) Outcome(bool),
    indexRepairIntentSummary: *const fn (self: *DB, alloc: std.mem.Allocator) Outcome(DB.IndexRepairIntentSummary),
    repairArtifactIssuesWithRequest: *const fn (self: *DB, alloc: std.mem.Allocator, req: types.ArtifactRepairRunRequest) Outcome(types.ArtifactRepairResult),
    beginTransactionWithId: *const fn (self: *DB, txn_id: @import("transactions.zig").TxnId, timestamp_ns: u64) Outcome(@import("transactions.zig").TxnId),
    beginReplicatedTransactionAtOrderedReceipt: *const fn (self: *DB, txn_id: @import("transactions.zig").TxnId, timestamp_ns: u64, created_at_ns: u64, participants: []const []const u8, coordinator: bool, retain_terminal: bool, identity: db.OrderedApplyReceipt) Outcome(@import("transactions.zig").TxnId),
    commitTransaction: *const fn (self: *DB, txn_id: @import("transactions.zig").TxnId, timestamp_ns: u64) Outcome(void),
    resolveTransactionIntents: *const fn (self: *DB, txn_id: @import("transactions.zig").TxnId, status: @import("transactions.zig").TxnStatus, commit_version: u64) Outcome(void),
    applyRelationalTopologyControl: *const fn (self: *DB, command: @import("db/relational_integrity_topology.zig").Command, ordered_receipt: ?db.OrderedApplyReceipt) Outcome(void),
    runUntilIdleWithoutWaitingForEnrichmentRetries: *const fn (self: *DB) Outcome(void),
    runVectorBlockMaintenanceForIdle: *const fn (self: *DB) Outcome(usize),
    prepareRestoreStagingIndexesStep: *const fn (self: *DB, alloc: std.mem.Allocator, scope: [32]u8) Outcome(bool),
    beginRestoreStaging: *const fn (self: *DB, alloc: std.mem.Allocator, scope: @import("db/restore_staging.zig").Scope) Outcome(void),
    finishRestoreStaging: *const fn (self: *DB, alloc: std.mem.Allocator, scope: @import("db/restore_staging.zig").Digest, phase: @import("db/restore_staging.zig").Phase) Outcome(@import("db/restore_staging.zig").Digest),
    importPortableRestoreFileIntoUnpublishedEmpty: *const fn (self: *DB, alloc: std.mem.Allocator, io: native.IoBorrow, file: std.Io.File, file_size: u64, options: PortableRestoreImportOptions) Outcome(void),
    importPortableFileIntoUnpublishedEmpty: *const fn (self: *DB, alloc: std.mem.Allocator, io: native.IoBorrow, file: std.Io.File, file_size: u64, target_identity: @import("db/doc_identity.zig").Namespace) Outcome(void),
    importCohortPortableFileIntoUnpublishedEmpty: *const fn (self: *DB, alloc: std.mem.Allocator, io: native.IoBorrow, file: std.Io.File, file_size: u64, proof: @import("portable_backup.zig").CohortProof, cancellation: types.CancellationToken) Outcome(void),
    openRelationalReadSession: *const fn (self: *DB, alloc: std.mem.Allocator, from_key: []const u8, to_key: []const u8, opts: types.ScanOptions) Outcome(*DB.RelationalReadSession),
    search: *const fn (self: *DB, alloc: std.mem.Allocator, req: types.SearchRequest) Outcome(types.SearchResult),
    searchWithCapturedRequest: *const fn (self: *DB, alloc: std.mem.Allocator, req: types.SearchRequest) Outcome(db.SearchWithCapturedRequestResult),
    preflightSearchRequest: *const fn (self: *DB, alloc: std.mem.Allocator, req: types.SearchRequest, max_work: u32) Outcome(@import("db/query/search_exec.zig").RuntimePreflightSummary),
    sourceVectorStats: *const fn (*DB) Outcome(?@import("vector_payload_store.zig").Stats),
    relationalTopologyStatus: *const fn (*DB) Outcome(DB.RelationalTopologyStatus),
    failGraphMetricPlannedBuild: *const fn (*DB, Allocator, []const u8, []const u8, []const u8) Outcome(types.GraphMetricStatus),
    runDenseArtifactRebuildMaintenanceWithProgress: *const fn (*DB, Allocator, ?*anyopaque, ?db.ReplayProgressHook, @import("named_callback.zig").Dispatch) Outcome(bool),
    queryReadLeaseRelease: *const fn (*DB.QueryReadLease) Outcome(void),

    authorizeOfflineVectorMigrationCandidate: *const fn (self: *DB, stage: *const @import("db/generation_lifecycle.zig").StagedGeneration) Outcome(void),
    setQueryVisibilityHook: *const fn (self: *DB, hook: ?db.QueryVisibilityHook) Outcome(void),
    setCoordinatedTtl: *const fn (self: *DB, port: ?db.coordinated_ttl.Port) Outcome(void),
    beginTeardown: *const fn (self: *DB) Outcome(void),
    recoverLocalTransactionRecords: *const fn (self: *DB, cutoff_ns: u64, now_ns: u64) Outcome(@import("transactions.zig").RecoveryStats),
    abortPrimaryStoreAutoBulkIngestSession: *const fn (self: *DB) Outcome(void),
    abortBulkIngestSession: *const fn (self: *DB) Outcome(void),
    lsmMaintenanceScore: *const fn (self: *DB) Outcome(u64),
    lsmMaintenanceDebtHint: *const fn (self: *DB) Outcome(u64),
    primaryLsmMaintenanceDebtHint: *const fn (self: *DB) Outcome(u64),
    nextLsmMaintenanceWakeDelayNsBestEffort: *const fn (self: *DB) Outcome(?u64),
    nextPrimaryLsmMaintenanceWakeDelayNsBestEffort: *const fn (self: *DB) Outcome(?u64),
    snapshotAsyncIndexingStats: *const fn (self: *DB) Outcome(types.AsyncIndexingStats),
    snapshotLsmMaintenanceStats: *const fn (self: *DB) Outcome(@import("lsm_backend/mod.zig").Backend.MaintenanceStats),
    snapshotLsmOwnerStatsAlloc: *const fn (self: *DB, alloc: std.mem.Allocator) Outcome([]DB.LsmOwnerStats),
    trySnapshotLsmOwnerStatsAlloc: *const fn (self: *DB, alloc: std.mem.Allocator) Outcome(?[]DB.LsmOwnerStats),
    trySnapshotLsmMaintenanceStats: *const fn (self: *DB) Outcome(?@import("lsm_backend/mod.zig").Backend.MaintenanceStats),
    snapshotLsmWriteStats: *const fn (self: *DB) Outcome(@import("lsm_backend/mod.zig").Backend.WriteStats),
    trySnapshotLsmWriteStats: *const fn (self: *DB) Outcome(?@import("lsm_backend/mod.zig").Backend.WriteStats),
    trySnapshotTextMemoryAttributionStats: *const fn (self: *DB) Outcome(?@import("db/catalog/index_manager.zig").TextMemoryAttributionStats),
    trySnapshotTextMergeStats: *const fn (self: *DB) Outcome(?types.TextMergeStats),
    hasActiveDenseBulkWork: *const fn (self: *const DB) Outcome(bool),
    orderedApplyReceipt: *const fn (self: *DB) Outcome(?db.OrderedApplyReceipt),
    clearOrderedApplyReceipt: *const fn (self: *DB) Outcome(void),
    setResolutionCandidateSource: *const fn (self: *DB, src: ?@import("db/resolution_runtime.zig").CandidateSource) Outcome(void),
    setEntitySink: *const fn (self: *DB, sink: ?@import("db/promotion_runtime.zig").EntitySink) Outcome(void),
    setPromotionOwner: *const fn (self: *DB, owner: ?@import("db/promotion_runtime.zig").PromotionOwner) Outcome(void),
    advanceGeneratedArtifactCleanupPage: *const fn (self: *DB, index_name: ?[]const u8) Outcome(DB.GeneratedArtifactCleanupAdvanceResult),
    loadIndexRepairState: *const fn (self: *const DB, alloc: std.mem.Allocator) Outcome(@import("db/derived/index_repair_state.zig").State),
    getRange: *const fn (self: *DB) Outcome(types.ByteRange),
    getSplitBootstrapMarker: *const fn (self: *DB, alloc: std.mem.Allocator) Outcome(?@import("db/range_state.zig").SplitBootstrapMarker),
    setSplitBootstrapMarker: *const fn (self: *DB, marker: @import("db/range_state.zig").SplitBootstrapMarker) Outcome(void),
    readRestoreStateForPath: *const fn (alloc: std.mem.Allocator, path: []const u8) Outcome(?db.RestoreState),
    readRestoreStateForPathWithIo: *const fn (alloc: std.mem.Allocator, io: native.IoBorrow, path: []const u8) Outcome(?db.RestoreState),
    restoreRuntimeRepairNeededForPath: *const fn (alloc: std.mem.Allocator, path: []const u8) Outcome(bool),
    restoreRuntimeRepairNeededForPathWithIo: *const fn (alloc: std.mem.Allocator, io: native.IoBorrow, path: []const u8) Outcome(bool),
    beginTransaction: *const fn (self: *DB, timestamp_ns: u64) Outcome(@import("transactions.zig").TxnId),
    beginTransactionWithParticipants: *const fn (self: *DB, timestamp_ns: u64, participants: []const []const u8) Outcome(@import("transactions.zig").TxnId),
    writeReplicatedTransactionAtOrderedReceipt: *const fn (self: *DB, txn_id: types.TxnId, req: types.TransactionIntentRequest, identity: db.OrderedApplyReceipt) Outcome(void),
    getTransactionStatus: *const fn (self: *DB, txn_id: @import("transactions.zig").TxnId) Outcome(@import("transactions.zig").TxnStatus),
    transactionDefersCoordinatorAcknowledgement: *const fn (self: *DB, txn_id: @import("transactions.zig").TxnId) Outcome(bool),
    transactionRetainsCoordinatorAcknowledgement: *const fn (self: *DB, txn_id: @import("transactions.zig").TxnId) Outcome(bool),
    hasTopologySensitiveTransactions: *const fn (self: *DB) Outcome(bool),
    onlineSourceStatus: *const fn (self: *DB, scope: @import("db/online_source_contract.zig").Scope) Outcome(@import("db/online_source.zig").Progress),
    prepareOnlineSourcePublication: *const fn (self: *DB, scope: @import("db/online_source_contract.zig").Scope, cancellation: types.CancellationToken) Outcome(@import("source_snapshot.zig").Certificate),
    cleanupTransactionMetadataIfEligible: *const fn (self: *DB, txn_id: @import("transactions.zig").TxnId, cutoff_timestamp: u64, retained_cutoff_timestamp: u64) Outcome(bool),
    getUnresolvedTransactionParticipants: *const fn (self: *DB, alloc: std.mem.Allocator, txn_id: @import("transactions.zig").TxnId) Outcome([][]u8),
    admitManagedFullTextIndex: *const fn (self: *DB, cfg: types.IndexConfig) Outcome(?u128),
    listIndexes: *const fn (self: *DB, alloc: std.mem.Allocator) Outcome([]types.IndexConfig),
    listEnrichments: *const fn (self: *DB, alloc: std.mem.Allocator) Outcome([]types.EnrichmentConfig),
    listResolvers: *const fn (self: *DB, alloc: std.mem.Allocator) Outcome([]@import("db/catalog/index_manager.zig").ResolverConfig),
    curateResolverJsonAlloc: *const fn (self: *DB, alloc: std.mem.Allocator, name: []const u8, request_json: []const u8) Outcome([]u8),
    resolverCurationJsonAlloc: *const fn (self: *DB, alloc: std.mem.Allocator, name: []const u8) Outcome([]u8),
    resolverReviewsJsonAlloc: *const fn (self: *DB, alloc: std.mem.Allocator, request_json: []const u8) Outcome([]u8),
    listResolverIdentities: *const fn (self: *DB, alloc: std.mem.Allocator) Outcome([]@import("db/catalog/resolver_catalog.zig").ResolverConfig),
    consumeResolverMetadataDrop: *const fn (self: *DB, alloc: std.mem.Allocator, identity: @import("db/resolver_curation.zig").MetadataIdentity) Outcome(void),
    pendingWorkStats: *const fn (self: *DB) Outcome(@import("db/core.zig").PendingWorkStats),
    runArtifactRepairMetadataMaintenanceUntilIdle: *const fn (self: *DB) Outcome(void),
    publishVectorBlockBasesOnline: *const fn (self: *DB, options: @import("db/catalog/index_manager.zig").IndexManager.OnlineVectorBlockPublicationOptions) Outcome(usize),
    nativeVectorProjectionMaintenanceNeeded: *const fn (self: *DB) Outcome(bool),
    reprocessGeneratedEnrichmentFromStoredDocsWithProgress: *const fn (self: *DB, alloc: std.mem.Allocator, artifact_name: ?[]const u8) Outcome(DB.GeneratedEnrichmentReplayAppendResult),
    overlayRuntimeStatusBestEffort: *const fn (self: *DB, stats_alloc: std.mem.Allocator, runtime_stats: *types.DBStats) Outcome(bool),
    restoreGenerationAdmissionReceipt: *const fn (self: *DB) Outcome(?@import("db/restore_staging_contract.zig").GenerationAdmissionReceipt),
    reserveRestoreStaging: *const fn (self: *DB, alloc: std.mem.Allocator, plan_id: [16]u8, plan_digest: [32]u8, namespace: @import("db/doc_identity.zig").Namespace) Outcome(void),
    validateRestoreStagingScope: *const fn (self: *DB, alloc: std.mem.Allocator, scope: [32]u8) Outcome(void),
    readInitialChildPublicationRecord: *const fn (self: *DB) Outcome(?@import("db/relational_initial_child_publication.zig").Record),
    prepareRestoreStagingPage: *const fn (self: *DB, alloc: std.mem.Allocator, scope: @import("db/restore_staging.zig").Scope, source: *DB, max_rows: usize, cancellation: types.CancellationToken) Outcome(@import("db/restore_staging.zig").PreparedPage),
    prepareRewriteStagingPage: *const fn (self: *DB, alloc: std.mem.Allocator, scope: @import("db/restore_staging.zig").Scope, source: *DB, max_rows: usize, cancellation: types.CancellationToken, program: *const @import("db/relational_rewrite_staging.zig").ProgramSet) Outcome(@import("db/restore_staging.zig").PreparedPage),
    resolveNativeRestoreOpenPlan: *const fn (target_path: []const u8, requested_options: db.OpenOptions) Outcome(db.NativeRestoreOpenPlan),
    vectorMigrationCommand: *const fn (self: *DB, alloc: std.mem.Allocator, command: db.vector_migration.contract.Command) Outcome([]u8),
    vectorMigrationStatus: *const fn (self: *DB, alloc: std.mem.Allocator) Outcome(?[]u8),
    admitTransactionRecovery: *const fn (self: *DB) Outcome(?db.MutationBarrier.SharedLease),
    runPrimaryLsmMaintenanceStep: *const fn (self: *DB) Outcome(bool),
    runPrimaryLsmMaintenanceStepBestEffort: *const fn (self: *DB) Outcome(bool),
    runDueLsmObsoleteReclaimUntilIdle: *const fn (self: *DB, max_steps: usize) Outcome(usize),
    applyOrderedCommittedMutation: *const fn (self: *DB, request: types.BatchRequest, identity: db.OrderedApplyReceipt, bypass_write_gate: bool) Outcome(void),
    recoverOrderedSourceAdmission: *const fn (self: *DB, request: types.BatchRequest, identity: db.OrderedApplyReceipt, bypass_write_gate: bool) Outcome(void),
    batchNativeFkGenerationApply: *const fn (self: *DB, req: types.BatchRequest) Outcome(void),
    pinTransferablePrimarySnapshot: *const fn (self: *DB) Outcome(DB.TransferablePrimarySnapshot),
    verifyTransferablePrimarySnapshot: *const fn (self: *DB, namespace: [24]u8, receipt: ?db.OrderedApplyReceipt) Outcome(void),
    orderedMutationAlreadyApplied: *const fn (self: *DB, identity: db.OrderedApplyReceipt) Outcome(bool),
    loadRowPolicyReceipt: *const fn (self: *DB, generation: u64, phase: @import("../system_catalog/policies.zig").Publication.Phase) Outcome(@import("db/row_policy_bundle.zig").Receipt),
    listArtifactRepairIssues: *const fn (self: *DB, alloc: std.mem.Allocator, artifact_kind: ?types.ArtifactRepairKind, index_name: ?[]const u8, limit: usize) Outcome([]types.ArtifactRepairIssue),
    hasPendingIndexRepairIntents: *const fn (self: *const DB, alloc: std.mem.Allocator) Outcome(bool),
    completedManagedDenseGenerationIsServiceable: *const fn (self: *DB, alloc: std.mem.Allocator, index_name: []const u8) Outcome(bool),
    markDenseReplayArtifactRepairRequired: *const fn (self: *DB, alloc: std.mem.Allocator, index_name: []const u8) Outcome(bool),
    materializeManagedIndexAdmission: *const fn (self: *DB, alloc: std.mem.Allocator, index_name: []const u8) Outcome(?u128),
    repairArtifactIssuesWithRequestOptions: *const fn (self: *DB, alloc: std.mem.Allocator, req: types.ArtifactRepairRunRequest, options: types.ArtifactRepairRunOptions) Outcome(types.ArtifactRepairResult),
    repairArtifactIssues: *const fn (self: *DB, alloc: std.mem.Allocator, artifact_kind: ?types.ArtifactRepairKind, limit: usize) Outcome(types.ArtifactRepairResult),
    getTimestamp: *const fn (self: *DB, alloc: std.mem.Allocator, key: []const u8) Outcome(u64),
    findMedianKey: *const fn (self: *DB, alloc: std.mem.Allocator) Outcome([]u8),
    clearSplitDeltaFinalSeq: *const fn (self: *DB) Outcome(void),
    appendStagedSnapshotDocuments: *const fn (self: *DB, staged_generation: *const @import("db/generation_lifecycle.zig").StagedGeneration, byte_range: types.ByteRange, writes: []const types.BatchWrite) Outcome(void),
    finishStagedSnapshotRange: *const fn (self: *DB, staged_generation: *const @import("db/generation_lifecycle.zig").StagedGeneration, byte_range: types.ByteRange) Outcome(void),
    replaceSplitBootstrap: *const fn (self: *DB, alloc: std.mem.Allocator, byte_range: types.ByteRange, writes: []const types.BatchWrite, base_delta_sequence: u64, marker: @import("db/range_state.zig").SplitBootstrapMarker) Outcome(bool),
    completeSplitBootstrap: *const fn (self: *DB, alloc: std.mem.Allocator, byte_range: types.ByteRange, base_delta_sequence: u64, marker: @import("db/range_state.zig").SplitBootstrapMarker) Outcome(bool),
    split: *const fn (self: *DB, curr_range: types.ByteRange, split_key: []const u8, dest_dir1: []const u8, dest_dir2: []const u8, prepare_only: bool) Outcome(void),
    drainSnapshotMaintenance: *const fn (self: *DB, deadline_ns: u64) Outcome(void),
    sealBackupCohort: *const fn (self: *DB, id: []const u8, expected: @import("db/relational_integrity_topology.zig").Fence, cancellation: types.CancellationToken) Outcome(@import("db/native_backup_seal.zig").Handle),
    exportBackupCohort: *const fn (self: *DB, handle: @import("db/native_backup_seal.zig").Handle, id: []const u8, cancellation: types.CancellationToken) Outcome(u64),
    exportBackupCohortPortable: *const fn (self: *DB, handle: @import("db/native_backup_seal.zig").Handle, writer: WriterBorrow, options: PortableExportOptions, cancellation: types.CancellationToken) Outcome(void),
    mergeDocumentsPage: *const fn (self: *DB, alloc: std.mem.Allocator, byte_range: types.ByteRange, after_key: ?[]const u8) Outcome([]types.BatchWrite),
    mergeCleanupKeysPage: *const fn (self: *DB, alloc: std.mem.Allocator, byte_range: types.ByteRange, after_key: ?[]const u8) Outcome([]types.BatchWrite),
    repairVerifiedPrimarySnapshot: *const fn (alloc: std.mem.Allocator, staged: *const @import("db/generation_lifecycle.zig").StagedGeneration, namespace: [24]u8, receipt: ?db.OrderedApplyReceipt, root_generation: u64) Outcome(void),
    restoreSnapshotToStagedGeneration: *const fn (staged_generation: *const @import("db/generation_lifecycle.zig").StagedGeneration, alloc: std.mem.Allocator, snapshot_root: []const u8, path: []const u8, opts: db.OpenOptions) Outcome(void),
    restoreIdentityPreservingSnapshotToStagedGeneration: *const fn (staged_generation: *const @import("db/generation_lifecycle.zig").StagedGeneration, alloc: std.mem.Allocator, snapshot_root: []const u8, path: []const u8, opts: db.OpenOptions, expected_namespace: @import("db/doc_identity.zig").Namespace) Outcome(void),
    openVerifiedRestoreSourceWithCancellation: *const fn (staged_generation: *const @import("db/generation_lifecycle.zig").StagedGeneration, alloc: std.mem.Allocator, snapshot_root: []const u8, path: []const u8, opts: db.OpenOptions, expected_namespace: @import("db/doc_identity.zig").Namespace, cancellation: types.CancellationToken) Outcome(DB),
    recoverIncompleteRestoreImportIfNeededWithIo: *const fn (alloc: std.mem.Allocator, io: native.IoBorrow, path: []const u8, opts: db.OpenOptions) Outcome(bool),
    markRestorePrimaryRestoredForPathWithArtifact: *const fn (alloc: std.mem.Allocator, path: []const u8, backup_id: []const u8, location: []const u8, artifact_sha256: []const u8, snapshot_path: []const u8, group_id: u64) Outcome(void),
    markRestorePrimaryRestoredForPathWithIdentityWithIo: *const fn (alloc: std.mem.Allocator, io: native.IoBorrow, path: []const u8, identity: db.RestoreIdentity) Outcome(void),
    restoreRuntimeRepairNeeded: *const fn (self: *DB) Outcome(bool),
    prepareRestoreDurabilityRetryIfNeeded: *const fn (self: *DB, alloc: std.mem.Allocator) Outcome(bool),
    repairRestoreRuntimeStateStepIfNeeded: *const fn (self: *DB, alloc: std.mem.Allocator) Outcome(bool),
    repairRestoreRuntimeStateStepIfNeededWithIoAndRepairOptions: *const fn (self: *DB, alloc: std.mem.Allocator, io: native.IoBorrow, repair_options: types.ArtifactRepairRunOptions) Outcome(bool),
    installPublishedChildSchema: *const fn (self: *DB, alloc: std.mem.Allocator, schema_json: []const u8, publication: DB.PublishedChildSchema) Outcome(void),
    beginTransactionWithIdAndParticipantsCreatedAtAndRole: *const fn (self: *DB, txn_id: @import("transactions.zig").TxnId, timestamp_ns: u64, created_at_ns: u64, participants: []const []const u8, coordinator: bool) Outcome(@import("transactions.zig").TxnId),
    beginTransactionScoped: *const fn (self: *DB, txn_id: @import("transactions.zig").TxnId, timestamp_ns: u64, created_at_ns: u64, participants: []const []const u8, coordinator: bool, retain_terminal: bool, scope: ?[32]u8) Outcome(@import("transactions.zig").TxnId),
    beginReplicatedTransactionScoped: *const fn (self: *DB, txn_id: @import("transactions.zig").TxnId, timestamp_ns: u64, created_at_ns: u64, participants: []const []const u8, coordinator: bool, retain_terminal: bool, identity: db.OrderedApplyReceipt, scope: ?[32]u8) Outcome(@import("transactions.zig").TxnId),
    writeIntents: *const fn (self: *DB, txn_id: @import("transactions.zig").TxnId, intents: []const @import("transactions.zig").WriteIntent, predicates: []const @import("transactions.zig").VersionPredicate) Outcome(void),
    writeTransaction: *const fn (self: *DB, txn_id: types.TxnId, req: types.TransactionIntentRequest) Outcome(void),
    resolveTransactionIntentsWithSyncLevelAndCancellation: *const fn (self: *DB, txn_id: @import("transactions.zig").TxnId, status: @import("transactions.zig").TxnStatus, commit_version: u64, sync_level: types.SyncLevel, visibility_cancellation: types.CancellationToken) Outcome(void),
    resolveReplicatedTransactionAtOrderedReceipt: *const fn (self: *DB, txn_id: @import("transactions.zig").TxnId, status: @import("transactions.zig").TxnStatus, commit_version: u64, sync_level: types.SyncLevel, visibility_cancellation: types.CancellationToken, identity: db.OrderedApplyReceipt, resolved_participant: ?[]const u8) Outcome(void),
    relationalTopologyIdentity: *const fn (self: *DB) Outcome(DB.RelationalTopologyIdentity),
    relationalHandoffManifest: *const fn (self: *DB, alloc: std.mem.Allocator, source: @import("db/relational_integrity_topology.zig").Fence, destination: @import("db/relational_integrity_topology.zig").Fence, lower: []const u8, upper: []const u8, primary_sequence: u64) Outcome(@import("db/relational_integrity_handoff.zig").Manifest),
    relationalHandoffPage: *const fn (self: *DB, alloc: std.mem.Allocator, manifest: @import("db/relational_integrity_handoff.zig").Manifest, progress: @import("db/relational_integrity_handoff.zig").Progress) Outcome(@import("db/relational_integrity_handoff.zig").Page),
    markTransactionParticipantResolved: *const fn (self: *DB, txn_id: @import("transactions.zig").TxnId, participant: []const u8) Outcome(void),
    markTransactionParticipantsResolved: *const fn (self: *DB, txn_id: @import("transactions.zig").TxnId, participants: []const []const u8) Outcome(void),
    markReplicatedTransactionParticipantResolvedAtOrderedReceipt: *const fn (self: *DB, txn_id: @import("transactions.zig").TxnId, participant: []const u8, identity: db.OrderedApplyReceipt) Outcome(void),
    markReplicatedTransactionParticipantsResolvedAtOrderedReceipt: *const fn (self: *DB, txn_id: @import("transactions.zig").TxnId, participants: []const []const u8, identity: db.OrderedApplyReceipt) Outcome(void),
    cleanupReplicatedTransactionAtOrderedReceipt: *const fn (self: *DB, txn_id: @import("transactions.zig").TxnId, cutoff_timestamp: u64, retained_cutoff_timestamp: u64, identity: db.OrderedApplyReceipt) Outcome(bool),
    getEdges: *const fn (self: *DB, alloc: std.mem.Allocator, index_name: []const u8, key: []const u8, edge_type: []const u8, direction: @import("../graph/graph.zig").EdgeDirection) Outcome([]@import("../graph/graph.zig").Edge),
    addEnrichment: *const fn (self: *DB, cfg: types.EnrichmentConfig) Outcome(void),
    upsertEnrichment: *const fn (self: *DB, cfg: types.EnrichmentConfig) Outcome(@import("db/catalog/index_manager.zig").IndexManager.EnrichmentUpsertResult),
    listDerivedReplayDebt: *const fn (self: *DB, alloc: std.mem.Allocator) Outcome([]db.DerivedReplayDebtStatus),
    waitForCurrentSyncLevelWithCancellation: *const fn (self: *DB, sync_level: types.SyncLevel, cancellation: types.CancellationToken) Outcome(void),
    refreshGraphMetric: *const fn (self: *DB, alloc: std.mem.Allocator, index_name: []const u8, metric_name: []const u8) Outcome(types.GraphMetricStatus),
    rebuildGraphMetric: *const fn (self: *DB, alloc: std.mem.Allocator, index_name: []const u8, metric_name: []const u8) Outcome(types.GraphMetricStatus),
    pauseGraphMetricMaintenance: *const fn (self: *DB, alloc: std.mem.Allocator, index_name: []const u8, metric_name: []const u8) Outcome(types.GraphMetricStatus),
    resumeGraphMetricMaintenance: *const fn (self: *DB, alloc: std.mem.Allocator, index_name: []const u8, metric_name: []const u8) Outcome(types.GraphMetricStatus),
    ensureGraphMetricPlannedBuild: *const fn (self: *DB, alloc: std.mem.Allocator, index_name: []const u8, metric_name: []const u8, target_generation: u64) Outcome(types.GraphMetricStatus),
    runGraphMetricPlannedWorkerPageStepAt: *const fn (self: *DB, index_name: []const u8, metric_name: []const u8, worker_id: []const u8, now_ms: u64) Outcome(@import("../graph/graph.zig").GraphIndex.GraphMetricBuildWorkerStepResult),
    runGraphMetricPlannedCoordinatorStepAt: *const fn (self: *DB, index_name: []const u8, metric_name: []const u8, now_ms: u64) Outcome(@import("../graph/graph.zig").GraphIndex.GraphMetricBuildWorkerStepResult),
    runGraphMetricPlannedDrain: *const fn (self: *DB, alloc: std.mem.Allocator, index_name: []const u8, metric_name: []const u8, target_generation: u64, options: @import("../graph/graph.zig").GraphIndex.GraphMetricPlannedDrainOptions) Outcome(types.GraphMetricStatus),
    refreshDensePostingPayloadPageBestEffort: *const fn (self: *DB) Outcome(DB.PostingRefreshProgress),
    publishCompletedDensePostingCheckpoints: *const fn (self: *DB) Outcome(DB.NativePublicationResult),
    publishVectorBlockBasesOnlineReported: *const fn (self: *DB, options: @import("db/catalog/index_manager.zig").IndexManager.OnlineVectorBlockPublicationOptions) Outcome(DB.NativePublicationResult),
    finalizeDenseProjectionLifecycleForIdle: *const fn (self: *DB) Outcome(bool),
    publishVectorBlockBasesAtStableTip: *const fn (self: *DB) Outcome(usize),
    hasPendingDenseArtifactRebuild: *const fn (self: *DB, alloc: std.mem.Allocator) Outcome(bool),
    denseArtifactRebuildMaintenanceNeeded: *const fn (self: *DB, alloc: std.mem.Allocator) Outcome(bool),
    stats: *const fn (self: *DB, alloc: std.mem.Allocator) Outcome(types.DBStats),
    runtimeStatusStatsConsistent: *const fn (self: *DB, alloc: std.mem.Allocator) Outcome(types.DBStats),
    runtimeStatusStatsConsistentIfAvailable: *const fn (self: *DB, alloc: std.mem.Allocator) Outcome(?types.DBStats),
    reassignIdentityNamespaceForInternalTransition: *const fn (self: *DB, namespace: @import("db/doc_identity.zig").Namespace) Outcome(void),
    attachRestoreStagingReplicationMirror: *const fn (self: *DB, mirror: ?db.ReplicationAsyncEffectMirror) Outcome(void),
    reserveRestoreStagingScoped: *const fn (self: *DB, alloc: std.mem.Allocator, scope: @import("db/restore_staging.zig").Scope) Outcome(void),
    installRestoreStagingReadSchema: *const fn (self: *DB, alloc: std.mem.Allocator, scope: @import("db/restore_staging.zig").Scope, read_json: []const u8) Outcome(void),
    installRestoreStagingBootstrap: *const fn (self: *DB, alloc: std.mem.Allocator, descriptor: @import("db/restore_staging.zig").OwnerBootstrap) Outcome(void),
    readRestoreStagingBootstrap: *const fn (self: *DB, alloc: std.mem.Allocator) Outcome(?std.json.Parsed(@import("db/restore_staging.zig").OwnerBootstrap)),
    cancelColdInitialChildForRetirement: *const fn (self: *DB, expected: @import("db/relational_initial_child_publication.zig").Record, cancel_revision: u64) Outcome(void),
    importRestoreStagingPage: *const fn (self: *DB, alloc: std.mem.Allocator, scope: @import("db/restore_staging.zig").Scope, source: *DB, max_rows: usize, cancellation: types.CancellationToken) Outcome(@import("db/restore_staging.zig").Phase),
    importPortableIntoEmpty: *const fn (self: *DB, alloc: std.mem.Allocator, backup: []const u8, target_identity: @import("db/doc_identity.zig").Namespace) Outcome(void),
    currentIdentityReadGenerationForRequest: *const fn (self: *DB, requested: ?u64) Outcome(u64),
    generatedCoverageReplayNeeded: *const fn (self: *DB, alloc: std.mem.Allocator) Outcome(bool),
    generatedCoverageRecoveryOwnedByInitialBuild: *const fn (self: *DB, alloc: std.mem.Allocator) Outcome(bool),
    ensureGeneratedCoverageRecoveryIntents: *const fn (self: *DB, alloc: std.mem.Allocator) Outcome(usize),
    beginQueryReadLease: *const fn (self: *DB) Outcome(DB.QueryReadLease),
    searchRequestAtCurrentIdentityGeneration: *const fn (self: *DB, req: types.SearchRequest) Outcome(types.SearchRequest),
    graphHasIncomingEdgesForInternalReadBoundedAt: *const fn (self: *DB, alloc: std.mem.Allocator, index_name: []const u8, keys: []const []const u8, expected: @import("db/catalog/index_manager.zig").IndexManager.CoverageIdentity, identity_read_generation: ?u64, ttl_now_ns: u64, max_scanned_rows: usize) Outcome(@import("../graph/graph.zig").GraphIndex.IncomingProbeResult),
    catchUpPendingDerivedReplayWithProgress: *const fn (*DB, *anyopaque, db.ReplayProgressHook, @import("named_callback.zig").Dispatch) Outcome(void),
    relationalSnapshotDeinit: *const fn (*DB.RelationalStatementSnapshot) Outcome(void),
    statementFenceRelease: *const fn (*DB.StatementReadFence) Outcome(void),
    relationalRowsUnderLease: *const fn (*const DB.QueryReadLease, Allocator, []const []const u8, u32) Outcome(@import("db/relational_rows.zig").Reader),
    relationalReaderDeinit: *const fn (self: *@import("db/relational_rows.zig").Reader) Outcome(void),
    relationalReaderNextPage: *const fn (self: *@import("db/relational_rows.zig").Reader, alloc: Allocator, io: ?native.IoBorrow, budget: @import("db/relational_rows.zig").Budget) Outcome(@import("db/relational_rows.zig").Page),
    relationalReaderNextTypedPage: *const fn (self: *@import("db/relational_rows.zig").Reader, alloc: Allocator, io: ?native.IoBorrow, budget: @import("db/relational_rows.zig").Budget) Outcome(@import("db/relational_rows.zig").Page),
    relationalReaderLookupSqlBatch: *const fn (self: *@import("db/relational_rows.zig").Reader, alloc: Allocator, documents: []const []const u8, layout: *const @import("../sql/catalog.zig").Row.TypedLayout, max_bytes: usize) Outcome(@import("db/relational_rows.zig").Reader.Hydrated),
    relationalReaderLookupTypedRow: *const fn (self: *@import("db/relational_rows.zig").Reader, alloc: Allocator, document: []const u8, native_cells: bool, shared: ?*?*const @import("../sql/catalog.zig").Row.TypedLayout) Outcome(?@import("db/relational_rows.zig").Row),
    relationalSessionDeinit: *const fn (session: *@import("db/relational_read_session.zig").Session) Outcome(void),
    relationalSessionCheckpoint: *const fn (session: *const @import("db/relational_read_session.zig").Session) Outcome(void),
    relationalSessionRangeProofs: *const fn (session: *@import("db/relational_read_session.zig").Session, alloc: Allocator) Outcome([]@import("range_protection.zig").Proof),
    relationalSessionNextTypedPage: *const fn (session: *@import("db/relational_read_session.zig").Session, alloc: Allocator, io: ?native.IoBorrow, budget: @import("db/relational_rows.zig").Budget) Outcome(@import("db/relational_rows.zig").Page),
    relationalSessionNormalizeRows: *const fn (session: *@import("db/relational_read_session.zig").Session, alloc: Allocator, writes: []const types.BatchWrite) Outcome([]types.BatchWrite),
    documentSessionDeinit: *const fn (self: *@import("db/document_rows.zig").Session) Outcome(void),
    documentSessionRangeProofs: *const fn (self: *@import("db/document_rows.zig").Session, alloc: std.mem.Allocator) Outcome([]@import("range_protection.zig").Proof),
    documentSessionNormalizeRows: *const fn (self: *@import("db/document_rows.zig").Session, alloc: std.mem.Allocator, writes: []const types.BatchWrite) Outcome([]types.BatchWrite),
    documentSessionNext: *const fn (self: *@import("db/document_rows.zig").Session, alloc: std.mem.Allocator, limit: u32) Outcome(@import("relational_read_view.zig").View.Page),
    captureRelationalStatementSnapshot: *const fn (self: *DB) Outcome(DB.RelationalStatementSnapshot),
    beginRelationalRows: *const fn (self: *DB, alloc: std.mem.Allocator, request: @import("db/relational_rows.zig").Request) Outcome(@import("db/relational_rows.zig").Reader),
    openDocumentReadSession: *const fn (self: *DB, alloc: std.mem.Allocator, from_key: []const u8, to_key: []const u8, opts: types.ScanOptions) Outcome(*DB.DocumentReadSession),
    openRelationalReadSessionAtSnapshot: *const fn (self: *DB, alloc: std.mem.Allocator, from_key: []const u8, to_key: []const u8, opts: types.ScanOptions, statement: ?*DB.RelationalStatementSnapshot) Outcome(*DB.RelationalReadSession),
    tryStatementReadFence: *const fn (self: *DB) Outcome(?DB.StatementReadFence),
    tryEmbeddedBackupReadFence: *const fn (self: *DB) Outcome(?DB.StatementReadFence),
    catchUpPendingDerivedReplay: *const fn (self: *DB) Outcome(void),
    derivedAsyncAppendDerivedBatchRecord: *const fn (self: *DB, derived_batch: @import("db/derived/derived_types.zig").DerivedBatch) Outcome(u64),
    runDerivedUntil: *const fn (self: *DB, sequence: u64) Outcome(void),
    resolvedFilterTextIndexNameAlloc: *const fn (self: *DB, alloc: Allocator, primary: ?[]const u8, singleton: ?[]const u8) Outcome(?[]u8),
    waitForCurrentIndexTargets: *const fn (self: *DB, index_names: []const []const u8, deadline_ns: u64) Outcome(u64),
    runDerivedUntilTargets: *const fn (self: *DB, sequence: u64, index_names: []const []const u8) Outcome(void),
    runRelationalIndexMaintenancePass: *const fn (self: *DB) Outcome(bool),
    runRelationalIndexMaintenanceUntilIdle: *const fn (self: *DB, cancellation: types.CancellationToken, deadline_ns: ?u64) Outcome(void),
    ensureRelationalIndexesReady: *const fn (self: *DB, names: []const []const u8, cancellation: types.CancellationToken, deadline_ns: ?u64) Outcome(void),
    runRelationalColumnMaintenancePass: *const fn (self: *DB) Outcome(usize),
    rebuildRelationalColumns: *const fn (self: *DB) Outcome(bool),
    replayGeneratedEnrichmentsFromStoredDocs: *const fn (self: *DB, alloc: std.mem.Allocator) Outcome(usize),
    advanceGraphReplacement: *const fn (self: *DB) Outcome(bool),
    replaceGraphIndex: *const fn (self: *DB, old: []const u8, cfg: types.IndexConfig) Outcome(u64),
    rebuildGraphIndexesForTargetCoverage: *const fn (self: *DB, alloc: std.mem.Allocator) Outcome(void),
    beginBulkIngestSession: *const fn (self: *DB) Outcome(void),
    finishBulkIngestSessionWithOptions: *const fn (self: *DB, options: @import("backend_types.zig").BulkIngestFinishOptions) Outcome(void),
    beginDenseAutoBulkIngestSession: *const fn (self: *DB) Outcome(void),
    beginPrimaryStoreAutoBulkIngestSession: *const fn (self: *DB) Outcome(void),
    finishPrimaryStoreAutoBulkIngestSessionWithOptions: *const fn (self: *DB, options: @import("backend_types.zig").BulkIngestFinishOptions) Outcome(void),
    rollPrimaryStoreAutoBulkIngestSessionWithOptions: *const fn (self: *DB, options: @import("backend_types.zig").BulkIngestFinishOptions) Outcome(void),
    finishDenseAutoBulkIngestSessionWithOptions: *const fn (self: *DB, options: @import("backend_types.zig").BulkIngestFinishOptions) Outcome(void),
    rollDenseAutoBulkIngestSessionWithOptions: *const fn (self: *DB, options: @import("backend_types.zig").BulkIngestFinishOptions) Outcome(void),
    admitManagedIndex: *const fn (self: *DB, cfg: types.IndexConfig) Outcome(?u128),
    waitForSyncLevelWithCancellation: *const fn (self: *DB, sync_level: types.SyncLevel, sequence: u64, sync_targets: @import("db/execution_resources.zig").ManagedSyncTargets, cancellation: types.CancellationToken, skip_enrichment_runtime_wait: bool) Outcome(void),
    waitForResolvedTransactionSyncWithCancellation: *const fn (self: *DB, sync_level: types.SyncLevel, sequence: u64, cancellation: types.CancellationToken) Outcome(void),
    enrichmentStart: *const fn (self: *@import("db/enrichment/enrichment_runtime.zig").EnrichmentRuntime) Outcome(void),
    enrichmentReloadDurableState: *const fn (self: *@import("db/enrichment/enrichment_runtime.zig").EnrichmentRuntime) Outcome(void),
    enrichmentResumeFrom: *const fn (self: *@import("db/enrichment/enrichment_runtime.zig").EnrichmentRuntime, sequence: u64, target_sequence: u64) Outcome(void),
    enrichmentWaitForAppliedWithVisibilityDeadline: *const fn (self: *@import("db/enrichment/enrichment_runtime.zig").EnrichmentRuntime, sequence: u64, cancellation: types.CancellationToken, deadline_ns: ?u64) Outcome(void),
    enrichmentCatchUpUntil: *const fn (self: *@import("db/enrichment/enrichment_runtime.zig").EnrichmentRuntime, sequence: u64) Outcome(void),
    enrichmentCatchUpUntilWithVisibilityDeadline: *const fn (self: *@import("db/enrichment/enrichment_runtime.zig").EnrichmentRuntime, sequence: u64, cancellation: types.CancellationToken, deadline_ns: ?u64) Outcome(void),
    applyDerivedBatchToIndexAsync: *const fn (*@import("db/execution_resources.zig").AsyncContext, @import("db/derived/derived_types.zig").DerivedBatch, @import("db/catalog/index_manager.zig").ManagedIndexRef, @import("db/derived/derived_executor.zig").CatchUpSessionToken) Outcome(bool),
    createDetachedEnrichmentRuntime: *const fn (self: *DB, enrichment_cfg: *@import("db/enrichment/enrichment_runtime.zig").Config) Outcome(?DB.DetachedEnrichmentRuntime),
    initResolutionRuntime: *const fn (self: *DB) Outcome(void),
    initPromotionRuntime: *const fn (self: *DB) Outcome(void),
    initOptionalTtlRuntime: *const fn (self: *DB, cfg: @import("db/maintenance/ttl_runtime.zig").Config) Outcome(void),
    initOptionalTransactionRuntime: *const fn (self: *DB, cfg: @import("db/maintenance/transaction_runtime.zig").Config) Outcome(void),
    initOptionalTextMergeRuntime: *const fn (self: *DB, cfg: @import("db/maintenance/text_merge_runtime.zig").Config) Outcome(void),
    initOptionalSparseCompactionRuntime: *const fn (self: *DB, cfg: @import("db/maintenance/sparse_compaction_runtime.zig").Config) Outcome(void),
    initOptionalGraphMetricRuntime: *const fn (self: *DB, cfg: @import("db/maintenance/graph_metric_runtime.zig").Config) Outcome(void),
    initOptionalRuntimes: *const fn (self: *DB, opts: *db.OpenOptions) Outcome(void),
    startOptionalRuntimes: *const fn (self: *DB) Outcome(void),
    rebuildIndexWithShadowReplacementOwned: *const fn (self: *DB, alloc: std.mem.Allocator, cfg: types.IndexConfig, options: types.ArtifactRepairRunOptions, durable_repair_id: ?u128, yield_progress: *DB.ShadowIndexReplacementResult) Outcome(DB.ShadowIndexReplacementResult),
    advanceIndexRepairIntent: *const fn (self: *DB, alloc: std.mem.Allocator, repair_id: u128, options: types.ArtifactRepairRunOptions) Outcome(DB.IndexRepairAdvanceResult),
    rehydrateSchemaJson: *const fn (self: *DB, schema_json: []const u8) Outcome(bool),
    removeResolverWithoutDrain: *const fn (self: *DB, name: []const u8) Outcome(bool),
    deleteEnrichment: *const fn (self: *DB, kind: types.EnrichmentKind, name: []const u8) Outcome(bool),
    startVectorMigration: *const fn (self: *DB, request: db.vector_migration.contract.Request) Outcome(void),
    advanceVectorMigration: *const fn (self: *DB, job_id: []const u8) Outcome(void),
    publishVectorMigration: *const fn (self: *DB, job_id: []const u8) Outcome(void),
    observedDynamicFieldCapabilitySetsAlloc: *const fn (self: *DB, alloc: std.mem.Allocator, observation: @import("db/catalog/index_manager.zig").IndexManager.DynamicFieldObservationQuery) Outcome([]@import("db/catalog/index_manager.zig").IndexManager.ObservedDynamicFieldCapabilitySet),
    graphHydrateKeysForInternalRead: *const fn (self: *DB, alloc: std.mem.Allocator, req: types.SearchRequest, keys: []const []const u8) Outcome([]types.SearchHit),
    scanVisit: *const fn (*DB, Allocator, []const u8, []const u8, types.ScanOptions, types.ScanVisitor) Outcome(void),
    trySnapshotDenseSearch: *const fn (self: *DB, alloc: std.mem.Allocator, req: types.SearchRequest, dense: types.DenseKnnQuery, include_profile: bool) Outcome(?db.SearchWithDenseProfileResult),
    snapshotInternal: *const fn (self: *DB, id: []const u8, include_generated: bool, cancellation: types.CancellationToken, maintenance_deadline_ns: ?u64, expected_topology_fence: ?@import("db/relational_integrity_topology.zig").Fence, backup_seal_only: bool, query_cut: ?@import("db/native_query_cut.zig").Request) Outcome(u64),
    navigateHierarchyChildren: *const fn (self: *DB, alloc: std.mem.Allocator, req: types.SearchRequest) Outcome(types.SearchResult),
    searchDense: *const fn (self: *DB, alloc: std.mem.Allocator, req: types.SearchRequest, dense: types.DenseKnnQuery) Outcome(types.SearchResult),
    searchSparse: *const fn (self: *DB, alloc: std.mem.Allocator, req: types.SearchRequest, sparse: types.SparseKnnQuery) Outcome(types.SearchResult),
    searchGraph: *const fn (self: *DB, alloc: std.mem.Allocator, req: types.SearchRequest, graph_query: @import("../graph/query.zig").GraphQuery, base_hits: ?[]const types.SearchHit) Outcome(types.SearchResult),
    applyReplicatedBatch: *const fn (self: *DB, mutation: @import("db/replicated_mutation.zig").Batch, sequence: u64) Outcome(void),
    reconcileReplicatedArtifactAdmission: *const fn (self: *DB, request: types.BatchRequest, entry: db.OrderedApplyReceipt) Outcome(void),
    applyReplicatedSchema: *const fn (self: *DB, mutation: @import("db/replication_effects.zig").SchemaMutation, sequence: u64) Outcome(void),
    applyReplicatedRowPolicy: *const fn (self: *DB, mutation: @import("db/replication_effects.zig").RowPolicyMutation, sequence: u64) Outcome(void),
    applyReplicatedDerivedEffect: *const fn (self: *DB, change: @import("db/derived/change_journal.zig").Record, primary: ?@import("db/replication_effects.zig").primary_effect.View, sequence: u64) Outcome(u64),
    applyDocumentArtifactChildRangeBatch: *const fn (self: *DB, child_batch: db.DocumentArtifactChildRangeApplyBatch) Outcome(u64),
    batchNativeInitialChildApply: *const fn (self: *DB, req: types.BatchRequest, receipt: db.OrderedApplyReceipt) Outcome(void),
    captureQueryCut: *const fn (self: *DB, request: @import("db/native_query_cut.zig").Request, cancellation: types.CancellationToken) Outcome(void),
    openQueryCut: *const fn (self: *DB, request: @import("db/native_query_cut.zig").Request, cancellation: types.CancellationToken) Outcome(DB),
    runSourcePinCleanupStep: *const fn (self: *DB) Outcome(bool),
    runLsmMaintenanceStepBestEffort: *const fn (self: *DB) Outcome(bool),
    getDocumentArtifactManifest: *const fn (self: *DB, alloc: std.mem.Allocator, doc_key: []const u8, artifact_name: []const u8) Outcome(?types.DocumentArtifactManifest),
    listDocumentArtifactManifests: *const fn (self: *DB, alloc: std.mem.Allocator, doc_key: []const u8) Outcome(types.DocumentArtifactManifestList),
    updateDocumentArtifactChildRangePlacement: *const fn (self: *DB, alloc: std.mem.Allocator, doc_key: []const u8, artifact_name: []const u8, update: types.DocumentArtifactChildRangePlacementUpdate) Outcome(bool),
    listArtifactRepairIssuesPage: *const fn (self: *DB, alloc: std.mem.Allocator, req: types.ArtifactRepairListRequest) Outcome(types.ArtifactRepairListResult),
    discoverRecoverableStartupIndexFailures: *const fn (self: *DB, alloc: std.mem.Allocator, limit: usize) Outcome(DB.StartupIndexRepairDiscovery),
    repairRecoverableStartupIndexFailures: *const fn (self: *DB, alloc: std.mem.Allocator, limit: usize, options: types.ArtifactRepairRunOptions) Outcome(DB.StartupIndexRepairResult),
    reprocessDocumentArtifactRange: *const fn (self: *DB, alloc: std.mem.Allocator, artifact_name: []const u8, req: types.DocumentArtifactTableReprocessRequest) Outcome(types.DocumentArtifactTableReprocessResult),
    updateRange: *const fn (self: *DB, byte_range: types.ByteRange) Outcome(void),
    setSplitDeltaFinalSeq: *const fn (self: *DB, seq: u64) Outcome(void),
    mergeArtifactsPage: *const fn (self: *DB, alloc: std.mem.Allocator, byte_range: types.ByteRange, after_key: ?[]const u8) Outcome([]types.BatchWrite),
    alignOrderedArtifactCatalog: *const fn (self: *DB, command: @import("db/artifact_inventory.zig").Command, configs: []const types.IndexConfig, context: @import("db/artifact_reconcile_intent.zig").Context) Outcome(void),
    advanceOrderedArtifactInitialBuild: *const fn (self: *DB, configs: []const types.IndexConfig, context: @import("db/artifact_reconcile_intent.zig").Context) Outcome(void),
    reconcileArtifactDeleteIndex: *const fn (self: *DB, name: []const u8, context: @import("db/artifact_reconcile_intent.zig").Context) Outcome(bool),
    reconcileArtifactAddEnrichment: *const fn (self: *DB, cfg: types.EnrichmentConfig, context: @import("db/artifact_reconcile_intent.zig").Context) Outcome(void),
    artifactInventoryCommand: *const fn (self: *DB, alloc: std.mem.Allocator) Outcome(@import("db/artifact_inventory.zig").Command),
    advanceArtifactFootprintPage: *const fn (self: *DB) Outcome(bool),
    runGraphMetricServiceMaintenanceJsonAlloc: *const fn (self: *DB, alloc: std.mem.Allocator, body: []const u8) Outcome([]u8),
    scheduleGraphMetricBuild: *const fn (self: *DB, alloc: std.mem.Allocator, index_name: []const u8, metric_name: []const u8, force: bool) Outcome(types.GraphMetricStatus),
    rebuildDenseIndexesForTargetCoverage: *const fn (self: *DB, alloc: std.mem.Allocator) Outcome(usize),
    rebuildSparseIndexesForTargetCoverage: *const fn (self: *DB, alloc: std.mem.Allocator) Outcome(usize),
    searchDenseProfiled: *const fn (self: *DB, alloc: Allocator, req: types.SearchRequest, dense: types.DenseKnnQuery) Outcome(@import("db/query/search_exec.zig").ProfiledDenseSearchResult),
    searchDenseProfiledWithCapturedRequest: *const fn (self: *DB, alloc: Allocator, req: types.SearchRequest, dense: types.DenseKnnQuery) Outcome(db.ProfiledDenseSearchWithCapturedRequestResult),
    searchTextKernel: *const fn (self: *DB, alloc: Allocator, index_name: []const u8, text_query: types.TextQuery, options: types.TextKernelSearchOptions) Outcome(types.TextKernelResult),
    preflightSearchRequestWithExecutionContext: *const fn (self: *DB, alloc: Allocator, req: types.SearchRequest, max_work: u32, exec_ctx: types.ExecutionContext) Outcome(@import("db/query/search_exec.zig").RuntimePreflightSummary),
    collectPlanningStatsWithExecutionContext: *const fn (self: *DB, alloc: Allocator, req: types.SearchRequest, max_work: u32, exec_ctx: types.ExecutionContext) Outcome(@import("db/planning_stats.zig").PlanningStatsSummary),
    collectSearchRequestTextStats: *const fn (self: *DB, alloc: Allocator, req: types.SearchRequest) Outcome([]const @import("../search/distributed_stats.zig").TextFieldStats),
    configureTableStorage: *const fn (self: *DB, settings: @import("../common/table_storage.zig").Settings) Outcome(void),
    batchPrepared: *const fn (*DB.PreparedBatchOwner, types.BatchRequest, ?*BatchProfile, BatchExecutionOptions, *@import("db/execution_resources.zig").GeneratedEmbeddingMemo, *@import("db/execution_resources.zig").PreparedRowAllocator) Outcome(void),
    reconfigureEnrichmentRuntime: *const fn (self: *DB, cfg: @import("db/enrichment/enrichment_runtime.zig").Config) Outcome(void),
    reconfigureEnrichmentRuntimePaused: *const fn (self: *DB, cfg: @import("db/enrichment/enrichment_runtime.zig").Config) Outcome(void),
    resumeEnrichmentRuntimeAfterReconfigure: *const fn (self: *DB, operation: []const u8, index_name: []const u8) Outcome(void),
    ensureTransactionRecoveryRuntime: *const fn (self: *DB, cfg: @import("db/maintenance/transaction_runtime.zig").Config) Outcome(void),
    activateResolverReplayRuntimes: *const fn (self: *DB) Outcome(void),
    get: *const fn (self: *DB, alloc: Allocator, key: []const u8) Outcome(?[]u8),
    getDocument: *const fn (self: *DB, alloc: Allocator, key: []const u8, opts: types.LookupOptions) Outcome(?types.LookupResult),
    lookup: *const fn (self: *DB, alloc: Allocator, key: []const u8, opts: types.LookupOptions) Outcome(?types.LookupResult),
    scan: *const fn (self: *DB, alloc: Allocator, from_key: []const u8, to_key: []const u8, opts: types.ScanOptions) Outcome(types.ScanResult),
    diagnosticStats: *const fn (self: *DB, alloc: Allocator) Outcome(types.DBStats),
    getSchemaJson: *const fn (self: *DB, alloc: Allocator) Outcome(?[]u8),
    primaryDocCount: *const fn (self: *DB, alloc: Allocator) Outcome(u64),
    getEnrichment: *const fn (self: *DB, alloc: Allocator, kind: types.EnrichmentKind, name: []const u8) Outcome(?types.EnrichmentConfig),
    runLsmMaintenanceStep: *const fn (self: *DB) Outcome(bool),
    constraintValidationStatus: *const fn (self: *DB) Outcome(DB.ConstraintValidationStatus),
    validateConstraintsStep: *const fn (self: *DB, budget: @import("db/relational_constraint_jobs.zig").Budget) Outcome(bool),
    retryConstraintValidation: *const fn (self: *DB, schema_version: u32) Outcome(bool),
    buildRelationalIndexStep: *const fn (self: *DB, name: []const u8, budget: DB.RelationalIndexBuildBudget) Outcome(void),
    collectRelationalIndexGarbageStep: *const fn (self: *DB) Outcome(bool),
    retryRelationalIndexBuild: *const fn (self: *DB, name: []const u8, generation: u64) Outcome(bool),
    relationalIndexBuildStatus: *const fn (self: *DB, name: []const u8) Outcome(DB.RelationalIndexBuildStatus),
    open: *const fn (Allocator, []const u8, OpenOptions) Outcome(DB),
    openOwned: *const fn (Allocator, []const u8, OpenOptions) Outcome(*DB),
    close: *const fn (*DB) Outcome(void),
    closeOwned: *const fn (*DB) Outcome(void),
    startResidentBackgroundWorkersIfNeeded: *const fn (*DB) Outcome(void),
    batchInternal: *const fn (*DB, types.BatchRequest, ?*BatchProfile, BatchExecutionOptions) Outcome(void),
    searchWithDenseProfile: *const fn (*DB, Allocator, types.SearchRequest) Outcome(SearchWithDenseProfileResult),
    searchWithCapturedRequestAndExecutionContext: *const fn (*DB, Allocator, types.SearchRequest, types.ExecutionContext) Outcome(SearchWithCapturedRequestResult),
    searchLease: *const fn (DB.QueryReadLease, Allocator, types.SearchRequest) Outcome(SearchWithDenseProfileResult),
    sync: *const fn (*DB, bool) Outcome(void),
    syncIndexes: *const fn (*DB, bool) Outcome(void),
    snapshot: *const fn (*DB, []const u8) Outcome(u64),
    snapshotWithMaintenanceDeadline: *const fn (*DB, []const u8, u64) Outcome(u64),
    snapshotNativeWithCancellation: *const fn (*DB, []const u8, types.CancellationToken) Outcome(u64),
    snapshotRelationalCohort: *const fn (*DB, []const u8, @import("db/relational_integrity_topology.zig").Fence, types.CancellationToken) Outcome(u64),
    setSchema: *const fn (*DB, schema_mod.TableSchema) Outcome(void),
    setSchemaJson: *const fn (*DB, Allocator, []const u8) Outcome(void),
    addIndex: *const fn (*DB, types.IndexConfig) Outcome(void),
    deleteIndex: *const fn (*DB, []const u8) Outcome(bool),
    runUntilIdle: *const fn (*DB) Outcome(void),
    runUntilIdleWithoutWaitingForEnrichmentRetriesWithCancellation: *const fn (*DB, types.CancellationToken) Outcome(void),
    finishBackgroundGraphReplacement: *const fn (*DB, []const u8, u64) Outcome(bool),
    runBackgroundTurnWithCancellation: *const fn (*DB, types.CancellationToken, bool) Outcome(bool),
    runBackgroundMaintenanceWithCancellation: *const fn (*DB, types.CancellationToken) Outcome(bool),
    registry: *const fn () Outcome(*anyopaque),
};
pub const Boundary = callback.Boundary(VTable);
pub const FieldLayout = extern struct { name: [*:0]const u8, type_name: [*:0]const u8, offset: usize, size: usize, alignment: usize };
pub fn fieldsFor(comptime T: type) []const FieldLayout {
    const Holder = struct {
        const fields = blk: {
            const info = @typeInfo(T).@"struct";
            var value: [info.field_names.len]FieldLayout = undefined;
            for (info.field_names, info.field_types, 0..) |name, Field, index|
                value[index] = .{ .name = name ++ "", .type_name = @typeName(Field) ++ "", .offset = @offsetOf(T, name), .size = @sizeOf(Field), .alignment = @alignOf(Field) };
            break :blk value;
        };
    };
    return &Holder.fields;
}
pub const Descriptor = extern struct {
    version: u32 = 25,
    table_layout: native.TypeContract,
    db_layout: native.TypeContract,
    core_layout: native.TypeContract,
    handle_layout: native.TypeContract,
    registry_layout: native.TypeContract,
    backend_runtime_layout: native.TypeContract,
    resource_manager_layout: native.TypeContract,
    prepared_owner_layout: native.TypeContract,
    prepared_allocator_layout: native.TypeContract,
    embedding_memo_layout: native.TypeContract,
    async_context_layout: native.TypeContract,
    enrichment_runtime_layout: native.TypeContract,
    relationalReader_layout: native.TypeContract,
    relationalSession_layout: native.TypeContract,
    documentSession_layout: native.TypeContract,

    index_manager_layout: native.TypeContract,
    index_manager_fields: [*]const FieldLayout,
    index_manager_field_count: usize,
    enrichment_set_layout: native.TypeContract,
    backup_snapshot_layout: native.TypeContract,
    algebraic_index_layout: native.TypeContract,
    docstore_layout: native.TypeContract,
    rewrite_program_layout: native.TypeContract,
    resolution_runtime_layout: native.TypeContract,
    promotion_runtime_layout: native.TypeContract,
    pinned_snapshot_layout: native.TypeContract,
    native_manifest_layout: native.TypeContract,
    writer_layout: native.TypeContract,
    generation_manager_layout: native.TypeContract,
    staged_generation_layout: native.TypeContract,
    table_fields: [*]const FieldLayout,
    table_field_count: usize,
    backend_fields: [*]const FieldLayout,
    backend_field_count: usize,
    resource_fields: [*]const FieldLayout,
    resource_field_count: usize,
    db_fields: [*]const FieldLayout,
    db_field_count: usize,
    core_fields: [*]const FieldLayout,
    core_field_count: usize,
    dispatch: callback.CallbackDispatch,
};
pub const table_layout = blk: {
    @setEvalBranchQuota(1_000_000);
    break :blk native.TypeContract.of(VTable);
};
pub const db_layout = blk: {
    @setEvalBranchQuota(100_000);
    break :blk native.TypeContract.of(DB);
};
pub const core_layout = blk: {
    @setEvalBranchQuota(100_000);
    break :blk native.TypeContract.of(@typeInfo(@FieldType(DB, "core")).pointer.child);
};
pub const handle_layout = blk: {
    @setEvalBranchQuota(100_000);
    break :blk native.TypeContract.of(@import("../capi/handles.zig").Handle);
};
pub const registry_layout = blk: {
    @setEvalBranchQuota(100_000);
    break :blk native.TypeContract.of(@import("../capi/handles.zig").HandleRegistry);
};
pub const backend_runtime_layout = blk: {
    @setEvalBranchQuota(100_000);
    break :blk native.TypeContract.of(@import("background_runtime.zig").BackendRuntime);
};
pub const resource_manager_layout = blk: {
    @setEvalBranchQuota(100_000);
    break :blk native.TypeContract.of(@import("resource_manager.zig").ResourceManager);
};
extern fn antfly_storage_engine_get_descriptor_v25() callconv(.c) *const Descriptor;
fn Result(comptime name: []const u8) type {
    const Function = @typeInfo(@FieldType(VTable, name)).pointer.child;
    const Wire = @typeInfo(Function).@"fn".return_type.?;
    return anyerror!@FieldType(Wire, "value");
}
pub fn call(comptime name: []const u8, args: anytype) Result(name) {
    const descriptor = antfly_storage_engine_get_descriptor_v25();
    try validateDescriptor(descriptor);
    // Individual hidden accessors let final links discard unused operations.
    // A global table would retain every physical operation in every consumer.
    const getter = @extern(*const fn () callconv(.c) *const anyopaque, .{ .name = "antfly_storage_engine_" ++ name ++ "_v25" });
    const method: @FieldType(VTable, name) = @ptrCast(@alignCast(getter()));
    const Function = @typeInfo(@FieldType(VTable, name)).pointer.child;
    const Args = std.meta.ArgsTuple(Function);
    var wire_args: Args = undefined;
    inline for (@typeInfo(Args).@"struct".field_names, @typeInfo(Args).@"struct".field_types) |field, T| {
        const value = @field(args, field);
        @field(wire_args, field) = if (comptime T == native.IoBorrow and @TypeOf(value) == std.Io)
            native.IoBorrow.init(&value)
        else if (comptime T == ?native.IoBorrow and @TypeOf(value) == ?std.Io)
            if (value) |io| native.IoBorrow.init(&io) else null
        else if (comptime T == ?native.IoBorrow and @TypeOf(value) == std.Io)
            native.IoBorrow.init(&value)
        else if (comptime T == WriterBorrow and @TypeOf(value) == *std.Io.Writer)
            WriterBorrow.init(value)
        else if (comptime T == PortableExportOptions and @TypeOf(value) != PortableExportOptions) blk: {
            var plan = value;
            const spool: @FieldType(PortableExportOptions, "spool") = if (plan.spool) |entry| .{ .io = native.IoBorrow.init(&entry.io), .file = entry.file } else null;
            plan.spool = null;
            break :blk PortableExportOptions{ .value = plan, .spool = spool };
        } else if (comptime T == BackupPlan and @TypeOf(value) != BackupPlan) blk: {
            var plan = value;
            const io: ?native.IoBorrow = if (plan.io) |borrowed| native.IoBorrow.init(&borrowed) else null;
            plan.io = null;
            break :blk BackupPlan{ .value = plan, .io = io };
        } else value;
    }
    const outcome = try Boundary.call(name, descriptor.dispatch, method, wire_args);
    if (outcome.failure.ptr) |ptr| return @import("storage_engine_errors").fromName(ptr[0..outcome.failure.len]);
    return outcome.value;
}

pub fn descriptorForValidation() *const Descriptor {
    return antfly_storage_engine_get_descriptor_v25();
}
pub fn validateDescriptor(descriptor: *const Descriptor) !void {
    if (descriptor.version != 25 or !descriptor.table_layout.matches(table_layout) or
        !descriptor.db_layout.matches(db_layout) or !descriptor.core_layout.matches(core_layout) or
        !descriptor.handle_layout.matches(handle_layout) or !descriptor.registry_layout.matches(registry_layout) or
        !descriptor.backend_runtime_layout.matches(backend_runtime_layout) or
        !descriptor.resource_manager_layout.matches(resource_manager_layout) or
        !descriptor.prepared_owner_layout.matches(prepared_owner_layout) or
        !descriptor.prepared_allocator_layout.matches(prepared_allocator_layout) or
        !descriptor.embedding_memo_layout.matches(embedding_memo_layout) or
        !descriptor.async_context_layout.matches(async_context_layout) or
        !descriptor.enrichment_runtime_layout.matches(enrichment_runtime_layout) or
        !descriptor.relationalReader_layout.matches(relationalReader_layout) or
        !descriptor.relationalSession_layout.matches(relationalSession_layout) or
        !descriptor.documentSession_layout.matches(documentSession_layout) or
        !descriptor.index_manager_layout.matches(index_manager_layout) or
        !descriptor.algebraic_index_layout.matches(algebraic_index_layout) or
        !descriptor.docstore_layout.matches(docstore_layout) or
        !descriptor.rewrite_program_layout.matches(rewrite_program_layout) or
        !descriptor.enrichment_set_layout.matches(enrichment_set_layout) or
        !descriptor.backup_snapshot_layout.matches(backup_snapshot_layout) or
        !descriptor.resolution_runtime_layout.matches(resolution_runtime_layout) or
        !descriptor.promotion_runtime_layout.matches(promotion_runtime_layout) or
        !descriptor.staged_generation_layout.matches(staged_generation_layout) or !descriptor.generation_manager_layout.matches(generation_manager_layout) or !descriptor.writer_layout.matches(writer_layout) or !descriptor.native_manifest_layout.matches(native_manifest_layout) or !descriptor.pinned_snapshot_layout.matches(pinned_snapshot_layout))
        return error.UnsupportedVersion;
}

pub const table_fields = fieldsFor(VTable);
pub const db_fields = fieldsFor(DB);
pub const core_fields = fieldsFor(@typeInfo(@FieldType(DB, "core")).pointer.child);

pub const backend_fields = fieldsFor(@import("background_runtime.zig").BackendRuntime);
pub const resource_fields = fieldsFor(@import("resource_manager.zig").ResourceManager);

pub const prepared_owner_layout = blk: {
    @setEvalBranchQuota(100_000);
    break :blk native.TypeContract.of(DB.PreparedBatchOwner);
};
pub const prepared_allocator_layout = native.TypeContract.of(@import("db/execution_resources.zig").PreparedRowAllocator);
pub const embedding_memo_layout = native.TypeContract.of(@import("db/execution_resources.zig").GeneratedEmbeddingMemo);

pub const async_context_layout = blk: {
    @setEvalBranchQuota(100_000);
    break :blk native.TypeContract.of(@import("db/execution_resources.zig").AsyncContext);
};

pub const enrichment_runtime_layout = blk: {
    @setEvalBranchQuota(100_000);
    break :blk native.TypeContract.of(@import("db/enrichment/enrichment_runtime.zig").EnrichmentRuntime);
};

pub const relationalReader_layout = blk: {
    @setEvalBranchQuota(100_000);
    break :blk native.TypeContract.of(@import("db/relational_rows.zig").Reader);
};
pub const relationalSession_layout = blk: {
    @setEvalBranchQuota(100_000);
    break :blk native.TypeContract.of(@import("db/relational_read_session.zig").Session);
};
pub const documentSession_layout = blk: {
    @setEvalBranchQuota(100_000);
    break :blk native.TypeContract.of(@import("db/document_rows.zig").Session);
};

pub const index_manager_layout = blk: {
    @setEvalBranchQuota(1_000_000);
    break :blk native.TypeContract.of(@import("db/catalog/index_manager.zig").IndexManager);
};
pub const algebraic_index_layout = blk: {
    @setEvalBranchQuota(1_000_000);
    break :blk native.TypeContract.of(@import("db/algebraic/index.zig").Index);
};
pub const docstore_layout = blk: {
    @setEvalBranchQuota(1_000_000);
    break :blk native.TypeContract.of(@import("docstore.zig").DocStore);
};
pub const rewrite_program_layout = blk: {
    @setEvalBranchQuota(1_000_000);
    break :blk native.TypeContract.of(@import("db/relational_rewrite_program.zig").ProgramSet);
};

pub const index_manager_fields = fieldsFor(@import("db/catalog/index_manager.zig").IndexManager);
pub const enrichment_set_layout = blk: {
    @setEvalBranchQuota(1_000_000);
    break :blk native.TypeContract.of(@import("write.zig").ManagedDbEnrichmentSet);
};
pub const backup_snapshot_layout = blk: {
    @setEvalBranchQuota(1_000_000);
    break :blk native.TypeContract.of(@import("write.zig").NativeBackupShardSnapshot);
};

pub const resolution_runtime_layout = blk: {
    @setEvalBranchQuota(1_000_000);
    break :blk native.TypeContract.of(@import("db/resolution_runtime.zig").ResolutionRuntime);
};
pub const promotion_runtime_layout = blk: {
    @setEvalBranchQuota(1_000_000);
    break :blk native.TypeContract.of(@import("db/promotion_runtime.zig").PromotionRuntime);
};
pub const pinned_snapshot_layout = blk: {
    @setEvalBranchQuota(1_000_000);
    break :blk native.TypeContract.of(@import("db/core.zig").PinnedStoreSnapshot);
};

pub const staged_generation_layout = blk: {
    @setEvalBranchQuota(1_000_000);
    break :blk native.TypeContract.of(@import("db/generation_lifecycle.zig").StagedGeneration);
};

pub const generation_manager_layout = blk: {
    @setEvalBranchQuota(1_000_000);
    break :blk native.TypeContract.of(@typeInfo(@FieldType(@import("db/generation_lifecycle.zig").StagedGeneration, "manager")).pointer.child);
};

pub const native_manifest_layout = blk: {
    @setEvalBranchQuota(1_000_000);
    break :blk native.TypeContract.of(@import("db/native_backup.zig").LoadedManifest);
};

pub const writer_layout = native.TypeContract.of(std.Io.Writer);
