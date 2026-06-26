// Copyright 2026 Antfly, Inc.
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
const builtin = @import("builtin");
const build_options = @import("build_options");
const platform = @import("antfly_platform");
const Allocator = std.mem.Allocator;
const common_secrets = @import("../../common/secrets.zig");
const backend_types = @import("../backend_types.zig");
const docstore_mod = @import("../docstore.zig");
const db_config = @import("config.zig");
const apply_rw_lock_mod = @import("apply_rw_lock.zig");
const db_core = @import("core.zig");
const split_restore = @import("split_restore.zig");
const db_internal = @import("internal.zig");
const artifact_replay = @import("artifact_replay.zig");
const lifecycle_mod = @import("lifecycle.zig");
const ha_replication = @import("ha_replication.zig");
const ha_types = @import("ha_types.zig");
const write_path = @import("write_path.zig");
const db_transactions = @import("transactions.zig");
const schema_runtime = @import("schema_runtime.zig");
const relational_integrity = @import("relational_integrity.zig");
const relational_rows = @import("relational_rows.zig");
const search_runtime = @import("search_runtime.zig");
const doc_identity = @import("doc_identity.zig");
const doc_set = @import("doc_set.zig");
const index_manager_mod = @import("catalog/index_manager.zig");
const resolution_runtime_mod = @import("resolution_runtime.zig");
const promotion_runtime_mod = @import("promotion_runtime.zig");
const resolver_lib = @import("antfly_resolver");
const types = @import("types.zig");
const aggregations_mod = @import("aggregations.zig");
const ha_replication_record_mod = @import("../ha/replication_record.zig");
const derived_types = @import("derived/derived_types.zig");
const derived_executor_mod = @import("derived/derived_executor.zig");
const derived_async = @import("derived_async.zig");
const background_runtime_mod = @import("../background_runtime.zig");
const vectorindex_mod = @import("antfly_vectorindex");
const embedder_mod = @import("enrichment/embedder.zig");
const enrichment_runtime_mod = @import("enrichment/enrichment_runtime.zig");
const lsm_backend_mod = @import("../lsm_backend/mod.zig");
const schema_mod = @import("../schema.zig");
const transactions_mod = @import("../transactions.zig");
const scraping = if (builtin.os.tag == .freestanding or build_options.bench_minimal_deps)
    @import("scraping_stub.zig")
else
    @import("antfly_scraping");
const mapper = @import("document_mapper.zig");
const metadata_table_manager = @import("../../metadata/table_manager.zig");
const relational_store_mod = @import("relational_store.zig");
const planning_stats_mod = @import("planning_stats.zig");
const ttl_runtime_mod = @import("maintenance/ttl_runtime.zig");
const transaction_runtime_mod = @import("maintenance/transaction_runtime.zig");
const text_merge_runtime_mod = @import("maintenance/text_merge_runtime.zig");
const sparse_compaction_runtime_mod = @import("maintenance/sparse_compaction_runtime.zig");
const graph_metric_runtime_mod = @import("maintenance/graph_metric_runtime.zig");

pub const OpenOptions = lifecycle_mod.OpenOptions;
pub const OpenMode = lifecycle_mod.OpenOptions.OpenMode;
pub const ForeignKeyIntegrityReport = relational_store_mod.ForeignKeyIntegrityReport;
pub const ForeignKeyIntegrityViolation = relational_store_mod.ForeignKeyIntegrityViolation;
pub const ForeignKeyDeletePlan = relational_store_mod.ForeignKeyDeletePlan;
pub const UniqueConstraintIntegrityReport = relational_store_mod.UniqueConstraintIntegrityReport;
pub const local_schema_json_key = schema_runtime.local_schema_json_key;
pub const local_lite_sql_table_record_json_key = schema_runtime.local_lite_sql_table_record_json_key;
pub const HAAsyncEffectMirror = ha_types.AsyncEffectMirror;
pub const HAAsyncBatchMirror = ha_types.AsyncBatchMirror;
pub const HAAsyncMetadataMirror = ha_types.AsyncMetadataMirror;
pub const HASyncWaitFn = ha_types.SyncWaitFn;
pub const HAProgressPollFn = ha_types.ProgressPollFn;
pub const HAPrimaryProgressSyncWait = ha_types.PrimaryProgressSyncWait;
pub const HASessionSyncWait = ha_types.SessionSyncWait;
pub const HAWriteGate = ha_types.WriteGate;

pub const ReplayProgress = db_internal.ReplayProgress;
pub const ReplayProgressHook = db_internal.ReplayProgressHook;

pub const DocumentArtifactChildRangeApplyBatch = write_path.DocumentArtifactChildRangeApplyBatch;
pub const DocumentArtifactChildRangeDispatch = write_path.DocumentArtifactChildRangeDispatch;
pub const DocumentArtifactChildRangeOutboxDrainResult = write_path.DocumentArtifactChildRangeOutboxDrainResult;
pub const DocumentArtifactChildRangeDispatcher = write_path.DocumentArtifactChildRangeDispatcher;

pub const QueryVisibilityChange = db_internal.QueryVisibilityChange;
pub const QueryVisibilityHook = db_internal.QueryVisibilityHook(DB);

pub const PrimaryBackend = db_config.PrimaryBackend;

pub const DerivedReplayDebtStatus = lifecycle_mod.DerivedReplayDebtStatus;

const DocSetPlanningRuntimeStats = db_internal.DocSetPlanningRuntimeStats;
const ForeignKeyRuntimeStats = db_internal.ForeignKeyRuntimeStats;
const ManagedSyncTargets = db_internal.ManagedSyncTargets;

const AsyncContext = db_internal.AsyncContext(DB);

const denseCatchUpFinishOptions = derived_async.denseCatchUpFinishOptions;
const EnrichmentAppendContext = db_internal.EnrichmentAppendContext(DB);

const BatchExecutionContext = db_internal.BatchExecutionContext(DB);

const TtlCleanupContext = db_internal.TtlCleanupContext(DB);

const AlgebraicDocFilterRequest = search_runtime.AlgebraicDocFilterRequest;

pub const BatchProfile = write_path.BatchProfile;
const BatchExecutionOptions = write_path.BatchExecutionOptions;

pub const OpenProfile = lifecycle_mod.OpenProfile;

const logBatchProfile = write_path.logBatchProfile;

const monotonicTimeNs = platform.time.monotonicNs;

pub const RestoreState = split_restore.RestoreState;
pub const RestoreIdentity = split_restore.RestoreIdentity;

pub const DB = struct {
    alloc: Allocator,
    runtime_alloc: Allocator,
    open_mode: OpenOptions.OpenMode,
    primary_backend: PrimaryBackend,
    primary_lsm_storage: ?lsm_backend_mod.Storage,
    index_backends: db_config.IndexBackendOptions,
    core: db_core.DBCore,
    async_context: *AsyncContext,
    backend_runtime: *background_runtime_mod.BackendRuntime,
    backend_owner_id: u64,
    owned_backend_runtime: ?background_runtime_mod.BackendRuntimeHandle,
    executor: *derived_executor_mod.Executor,
    start_index_workers: bool,
    graph_metric_idle_maintenance: OpenOptions.GraphMetricIdleMaintenanceMode,
    graph_metric_idle_planned_options: index_manager_mod.IndexManager.GraphMetricPlannedMaintenanceOptions,
    graph_metric_idle_auto_options: index_manager_mod.IndexManager.GraphMetricPlannedAutoIdleOptions,
    graph_metric_idle_degree_canary_options: index_manager_mod.IndexManager.GraphMetricDegreeCanaryOptions,
    secret_store: ?*common_secrets.FileStore,
    remote_content: ?*const scraping.RemoteContentConfig,
    enrichment_append_context: ?*EnrichmentAppendContext,
    enrichment_runtime: ?*enrichment_runtime_mod.EnrichmentRuntime,
    resolution_append_context: ?*EnrichmentAppendContext = null,
    resolution_runtime: ?*resolution_runtime_mod.ResolutionRuntime = null,
    resolution_candidate_source: ?resolution_runtime_mod.CandidateSource = null,
    resolution_embedder: ?embedder_mod.DenseEmbedder = null,
    promotion_runtime: ?*promotion_runtime_mod.PromotionRuntime = null,
    entity_sink: ?promotion_runtime_mod.EntitySink = null,
    promotion_owner: ?promotion_runtime_mod.PromotionOwner = null,
    entity_sink_missing_policy: promotion_runtime_mod.MissingSinkPolicy = .wait,
    ha_async_effect_mirror: ?HAAsyncEffectMirror = null,
    ha_async_batch_mirror: ?HAAsyncBatchMirror = null,
    ha_async_metadata_mirror: ?HAAsyncMetadataMirror = null,
    ha_write_gate: ?HAWriteGate = null,
    ttl_cleanup_context: ?*TtlCleanupContext,
    ttl_runtime: ?*ttl_runtime_mod.TtlRuntime,
    transaction_recovery_identity_context: ?*db_core.TransactionRecoveryIdentityContext,
    transaction_runtime: ?*transaction_runtime_mod.Runtime,
    text_merge_runtime: ?*text_merge_runtime_mod.TextMergeRuntime,
    sparse_compaction_runtime: ?*sparse_compaction_runtime_mod.SparseCompactionRuntime,
    graph_metric_runtime: ?*graph_metric_runtime_mod.GraphMetricRuntime,
    shadow: ?split_restore.ShadowState,
    bulk_ingest_coalescer: write_path.BulkIngestCoalescer(@This()) = .{},
    flushing_bulk_ingest_coalescer: bool = false,
    bulk_ingest_identity_all_new: bool = false,
    bulk_ingest_identity_state: doc_identity.AllNewTrustedState = .{},
    identity_visibility_summary_cache: ?doc_identity.VisibilitySummary = null,
    bulk_ingest_seen_doc_keys: std.StringHashMapUnmanaged(void) = .{},
    doc_set_planning_stats: DocSetPlanningRuntimeStats = .{},
    foreign_key_stats: ForeignKeyRuntimeStats = .{},

    const split_restore_impl = split_restore.Impl(@This());
    const internal_impl = db_internal.Impl(@This());
    const lifecycle_impl = lifecycle_mod.Impl(@This());
    const ha_replication_impl = ha_replication.Impl(@This());
    const write_path_impl = write_path.Impl(@This());
    const db_transactions_impl = db_transactions.Impl(@This());
    const schema_runtime_impl = schema_runtime.Impl(@This());
    const relational_integrity_impl = relational_integrity.Impl(@This());
    const relational_rows_impl = relational_rows.Impl(@This());
    const search_runtime_impl = search_runtime.Impl(@This());
    const derived_async_impl = derived_async.Impl(@This());
    pub const LifecycleCallbacks = struct {
        pub const apply_derived_batch_to_index_async = derived_async_impl.applyDerivedBatchToIndexAsync;
        pub const persist_applied_sequence_async = derived_async_impl.persistAppliedSequenceAsync;
        pub const truncate_replay_sequence_async = derived_async_impl.truncateReplaySequenceAsync;
        pub const begin_derived_catch_up_session_async = derived_async_impl.beginDerivedCatchUpSessionAsync;
        pub const finish_derived_catch_up_session_async = derived_async_impl.finishDerivedCatchUpSessionAsync;
        pub const can_advance_derived_to_target_async = derived_async_impl.canAdvanceDerivedToTargetAsync;
        pub const append_derived_batch_from_enrichment = derived_async_impl.appendDerivedBatchFromEnrichment;
        pub const notify_derived_executor_sequence = derived_async_impl.notifyDerivedExecutorSequence;
        pub const delete_expired_documents_from_candidates = write_path_impl.deleteExpiredDocumentsFromCandidates;
        pub const notify_async_context_visibility_hook = internal_impl.notifyAsyncContextVisibilityHook;
        pub const clear_bulk_ingest_seen_doc_keys_locked = write_path_impl.clearBulkIngestSeenDocKeysLocked;
        pub const deinit_bulk_ingest_coalescer = write_path_impl.deinitBulkIngestCoalescer;
        pub const replay_pending_derived_batches = derived_async_impl.replayPendingDerivedBatches;
        pub const start_async_workers = lifecycle_impl.startAsyncWorkers;
        pub const flush_applied_sequences_for_idle = derived_async_impl.flushAppliedSequencesForIdle;
        pub const wait_for_sync_level = derived_async_impl.waitForSyncLevel;
        pub const dense_index_rebuild_state_path_alloc = derived_async_impl.denseIndexRebuildStatePathAlloc;
        pub const set_dense_catch_up_progress = derived_async_impl.setDenseCatchUpProgress;
        pub const probe_derived_replay_target_sequence = lifecycle_mod.probeDerivedReplayTargetSequence;
        pub const lock_apply = internal_impl.lockApply;
        pub const populate_algebraic_index_stats = lifecycle_impl.populateAlgebraicIndexStats;
        pub const open_mode_requires_read_only_backends = db_config.openModeRequiresReadOnlyBackends;
    };
    pub const DerivedAsyncCallbacks = struct {
        pub const dense_catch_up_finish_options = denseCatchUpFinishOptions;
        pub const enforce_ha_write_gate_optional = ha_replication.enforceWriteGateOptional;
        pub const mirror_ha_replay_payload_best_effort_context = ha_replication.mirrorReplayPayloadBestEffort;
        pub const apply_derived_batch_to_index_context = derived_async_impl.applyDerivedBatchToIndexContext;
        pub const apply_derived_batch_to_index_context_profiled = derived_async_impl.applyDerivedBatchToIndexContextProfiled;
        pub const save_index_status_snapshots = db_internal.saveIndexStatusSnapshots;
        pub const async_index_profile_enabled = db_internal.asyncIndexProfileEnabled;
        pub const replay_pending_derived_batches_context = derived_async_impl.replayPendingDerivedBatchesContext;
        pub const open_profile_enabled = lifecycle_mod.openProfileEnabled;
        pub const log_replay_catch_up_profile = derived_async.logReplayCatchUpProfile;
        pub const log_derived_worker_profile = derived_async.logDerivedWorkerProfile;
    };
    pub const WritePathCallbacks = struct {
        pub const Profile = BatchProfile;
        pub const Options = BatchExecutionOptions;
        pub const batch_internal = write_path_impl.batchInternal;
        pub const open_mode_requires_read_only_backends = db_config.openModeRequiresReadOnlyBackends;
        pub const enforce_ha_write_gate = ha_replication_impl.enforceDBWriteGate;
        pub const enforce_ha_write_gate_optional = ha_replication.enforceWriteGateOptional;
        pub const preflight_ha_batch_sync_commit = ha_replication_impl.preflightDBBatchSyncCommit;
        pub const bench_metrics_enabled = db_internal.benchMetricsEnabled;
        pub const log_batch_profile = logBatchProfile;
        pub const monotonic_time_ns = monotonicTimeNs;
        pub const record_profile_ns = write_path.recordProfileNs;
        pub const lock_apply = internal_impl.lockApply;
        pub const append_row_claim_predicates_for_mutation_keys = db_transactions.appendRowClaimPredicatesForMutationKeys;
        pub const append_row_claim_predicates_for_identity_rewrites = db_transactions.appendRowClaimPredicatesForIdentityRewrites;
        pub const reclaim_expired_row_claim_intents_for_mutation_keys = db_transactions.reclaimExpiredRowClaimIntentsForMutationKeys;
        pub const reclaim_expired_row_claim_intents_for_identity_rewrites = db_transactions.reclaimExpiredRowClaimIntentsForIdentityRewrites;
        pub const relational_column_index_policy_for_store = schema_runtime_impl.relationalColumnIndexPolicyForStore;
        pub const relational_columns_for_store = relationalColumnsForStore;
        pub const record_foreign_key_child_write_reject = relational_integrity_impl.recordForeignKeyChildWriteReject;
        pub const record_foreign_key_parent_delete_reject = relational_integrity_impl.recordForeignKeyParentDeleteReject;
        pub const is_metadata_key = db_internal.isMetadataKey;
        pub const augment_extracted_write_with_graph_field_edges = write_path.augmentExtractedWriteWithGraphFieldEdges;
        pub const should_write_timestamp = db_internal.shouldWriteTimestamp;
        pub const resolve_write_timestamp_ns = internal_impl.resolveWriteTimestampNs;
        pub const make_timestamp_key = db_internal.makeTimestampKey;
        pub const encode_timestamp_value = db_internal.encodeTimestampValue;
        pub const append_system_versioned_history_for_batch = db_transactions.appendSystemVersionedHistoryForBatch;
        pub const split_shadow_requires_materialized_derived_batch = split_restore_impl.splitShadowRequiresMaterializedDerivedBatch;
        pub const encode_thin_replay_record_payload = write_path.encodeThinReplayRecordPayload;
        pub const append_precomputed_graph_source_artifacts = write_path_impl.appendPrecomputedGraphSourceArtifacts;
        pub const graph_writes_from_artifact_value_alloc = artifact_replay.graphWritesFromArtifactValueAlloc;
        pub const free_graph_writes = artifact_replay.freeGraphWrites;
        pub const resolution_mention_state_keys_for_graph_source_alloc = artifact_replay.resolutionMentionStateKeysForGraphSourceAlloc;
        pub const attach_inline_upsert_document_values = derived_async.attachInlineUpsertDocumentValues;
        pub const apply_derived_batch_to_shadow_if_needed = derived_async_impl.applyDerivedBatchToShadowIfNeeded;
        pub const collect_managed_sync_targets = derived_async_impl.collectManagedSyncTargets;
        pub const encode_change_record_payload = derived_async_impl.encodeChangeRecordPayloadForDB;
        pub const encode_change_record_payload_context = derived_async_impl.encodeChangeRecordPayload;
        pub const mirror_ha_batch_mutation_commit = ha_replication_impl.mirrorDBBatchMutationCommit;
        pub const mirror_ha_replay_payload_commit = ha_replication_impl.mirrorDBReplayPayloadCommit;
        pub const mirror_ha_replay_payload_best_effort = ha_replication_impl.mirrorDBReplayPayloadBestEffort;
        pub const mirror_ha_replay_payload_best_effort_context = ha_replication.mirrorReplayPayloadBestEffort;
        pub const should_append_split_delta = split_restore_impl.shouldAppendSplitDelta;
        pub const current_time_ns = db_internal.currentTimeNs;
        pub const mark_precomputed_enrichment_applied_for_sync = lifecycle_impl.markPrecomputedEnrichmentAppliedForSync;
        pub const mark_precomputed_enrichment_applied_for_sync_context = derived_async_impl.markPrecomputedEnrichmentAppliedForSyncContext;
        pub const apply_derived_backlog_pressure = derived_async_impl.applyDerivedBacklogPressure;
        pub const apply_derived_backlog_pressure_context = derived_async_impl.applyDerivedBacklogPressureContext;
        pub const notify_executor_for_sync_level_with_dense_bulk_deferral = db_internal.notifyExecutorForSyncLevelWithDenseBulkDeferral;
        pub const wait_for_sync_level = derived_async_impl.waitForSyncLevel;
        pub const wait_for_sync_level_context = derived_async_impl.waitForSyncLevelContext;
        pub const sync_level_requires_derived_visibility = derived_async_impl.syncLevelRequiresDerivedVisibility;
        pub const apply_derived_batch = derived_async_impl.applyDerivedBatch;
        pub const apply_derived_batch_targets = derived_async_impl.applyDerivedBatchTargets;
        pub const apply_derived_batch_context = derived_async_impl.applyDerivedBatchContext;
        pub const apply_derived_batch_targets_context = derived_async_impl.applyDerivedBatchTargetsContext;
        pub const apply_derived_batch_profiled = derived_async_impl.applyDerivedBatchProfiled;
        pub const apply_derived_batch_targets_profiled = derived_async_impl.applyDerivedBatchTargetsProfiled;
        pub const notify_resolver_replay_runtimes = lifecycle_impl.notifyResolverReplayRuntimes;
        pub const notify_resolver_replay_runtimes_for_catalog = derived_async_impl.notifyResolverReplayRuntimesForCatalog;
    };
    pub const SchemaRuntimeCallbacks = struct {
        pub const hydrate_algebraic_observation_status_for_index_best_effort = lifecycle_impl.hydrateAlgebraicObservationStatusForIndexBestEffort;
        pub const replay_generated_enrichments_from_stored_docs = replayGeneratedEnrichmentsFromStoredDocs;
        pub const append_generated_enrichments = write_path_impl.appendGeneratedEnrichments;
        pub const append_derived_batch_record = derivedAsyncAppendDerivedBatchRecord;
        pub const save_index_status_snapshot = schema_runtime_impl.saveIndexStatusSnapshot;
        pub const notify_resolver_replay_runtimes = lifecycle_impl.notifyResolverReplayRuntimes;
        pub const mirror_ha_schema_metadata_commit = ha_replication_impl.mirrorDBSchemaMetadataCommit;
        pub const mirror_ha_lite_sql_table_metadata_commit = ha_replication_impl.mirrorDBLiteSqlTableMetadataCommit;
    };
    pub const HAReplicationCallbacks = struct {
        pub const batch_replicated_apply_with_marker = write_path_impl.batchReplicatedApplyWithMarker;
        pub const apply_ha_derived_effect_record = applyHADerivedEffectRecord;
        pub const set_schema_with_local_lite_sql_table_record_json_replicated_apply = schema_runtime_impl.setSchemaWithLocalLiteSqlTableRecordJsonReplicatedApply;
    };

    pub fn batchContext(self: *DB) BatchExecutionContext {
        return internal_impl.batchContext(self);
    }

    pub fn open(alloc: Allocator, path: []const u8, opts: OpenOptions) !DB {
        return try lifecycle_impl.open(alloc, path, opts);
    }

    pub fn close(self: *DB) void {
        lifecycle_impl.close(self);
    }

    pub fn maintenanceDriver(self: *DB) db_core.MaintenanceDriver {
        return lifecycle_impl.maintenanceDriver(self);
    }

    pub fn engine(self: *DB) db_core.Engine {
        return lifecycle_impl.engine(self);
    }

    pub fn services(self: *DB) db_core.Services {
        return lifecycle_impl.services(self);
    }

    pub fn setQueryVisibilityHook(self: *DB, hook: ?QueryVisibilityHook) void {
        lifecycle_impl.setQueryVisibilityHook(self, hook);
    }

    pub fn runTransactionRecoveryOnce(self: *DB, config: transaction_runtime_mod.Config) !types.TransactionRecoveryStats {
        try ha_replication_impl.enforceDurableMutationGate(self);
        return try lifecycle_impl.runTransactionRecoveryOnce(self, config);
    }

    pub fn beginBulkIngestSession(self: *DB) !void {
        try ha_replication_impl.enforceDurableMutationGate(self);
        return try write_path_impl.beginBulkIngestSessionAfterGate(self);
    }

    pub fn finishBulkIngestSessionWithOptions(self: *DB, options: backend_types.BulkIngestFinishOptions) !void {
        try ha_replication_impl.enforceDurableMutationGate(self);
        return try write_path_impl.finishBulkIngestSessionWithOptionsAfterGate(self, options);
    }

    pub fn beginDenseAutoBulkIngestSession(self: *DB) !void {
        try ha_replication_impl.enforceDurableMutationGate(self);
        return try write_path_impl.beginDenseAutoBulkIngestSessionAfterGate(self);
    }

    pub fn beginPrimaryStoreAutoBulkIngestSession(self: *DB) !void {
        try ha_replication_impl.enforceDurableMutationGate(self);
        return try write_path_impl.beginPrimaryStoreAutoBulkIngestSessionAfterGate(self);
    }

    pub fn finishPrimaryStoreAutoBulkIngestSessionWithOptions(self: *DB, options: backend_types.BulkIngestFinishOptions) !void {
        try ha_replication_impl.enforceDurableMutationGate(self);
        return try write_path_impl.finishPrimaryStoreAutoBulkIngestSessionWithOptionsAfterGate(self, options);
    }

    pub fn rollPrimaryStoreAutoBulkIngestSessionWithOptions(self: *DB, options: backend_types.BulkIngestFinishOptions) !void {
        try ha_replication_impl.enforceDurableMutationGate(self);
        try write_path_impl.finishPrimaryStoreAutoBulkIngestSessionWithOptionsAfterGate(self, options);
        try write_path_impl.beginPrimaryStoreAutoBulkIngestSessionAfterGate(self);
    }

    pub fn finishDenseAutoBulkIngestSessionWithOptionsAndNotifyExecutor(
        self: *DB,
        options: backend_types.BulkIngestFinishOptions,
        notify_executor: bool,
    ) !void {
        try ha_replication_impl.enforceDurableMutationGate(self);
        return try write_path_impl.finishDenseAutoBulkIngestSessionWithOptionsAfterGate(self, options, notify_executor);
    }

    pub fn finishDenseAutoBulkIngestSessionWithOptions(self: *DB, options: backend_types.BulkIngestFinishOptions) !void {
        try ha_replication_impl.enforceDurableMutationGate(self);
        return try write_path_impl.finishDenseAutoBulkIngestSessionWithOptionsAfterGate(self, options, true);
    }

    pub fn rollDenseAutoBulkIngestSessionWithOptions(self: *DB, options: backend_types.BulkIngestFinishOptions) !void {
        try ha_replication_impl.enforceDurableMutationGate(self);
        try write_path_impl.finishDenseAutoBulkIngestSessionWithOptionsAfterGate(self, options, true);
        try write_path_impl.beginDenseAutoBulkIngestSessionAfterGate(self);
    }

    pub fn abortDenseAutoBulkIngestSession(self: *DB) void {
        write_path_impl.abortDenseAutoBulkIngestSession(self);
    }

    pub fn abortPrimaryStoreAutoBulkIngestSession(self: *DB) void {
        write_path_impl.abortPrimaryStoreAutoBulkIngestSession(self);
    }

    pub fn abortBulkIngestSession(self: *DB) void {
        write_path_impl.abortBulkIngestSession(self);
    }

    pub fn lsmMaintenanceScore(self: *DB) u64 {
        return lifecycle_impl.lsmMaintenanceScore(self);
    }

    pub fn lsmMaintenanceDebtHint(self: *DB) u64 {
        return lifecycle_impl.lsmMaintenanceDebtHint(self);
    }

    pub fn nextLsmMaintenanceWakeDelayNsBestEffort(self: *DB) ?u64 {
        return lifecycle_impl.nextLsmMaintenanceWakeDelayNsBestEffort(self);
    }

    pub fn snapshotAsyncIndexingStats(self: *DB) types.AsyncIndexingStats {
        return lifecycle_impl.snapshotAsyncIndexingStats(self);
    }

    pub fn snapshotApplyLockStats(self: *DB) apply_rw_lock_mod.ApplyRwLock.Stats {
        return lifecycle_impl.snapshotApplyLockStats(self);
    }

    pub fn snapshotLsmMaintenanceStats(self: *DB) lsm_backend_mod.Backend.MaintenanceStats {
        return lifecycle_impl.snapshotLsmMaintenanceStats(self);
    }

    pub fn trySnapshotLsmMaintenanceStats(self: *DB) ?lsm_backend_mod.Backend.MaintenanceStats {
        return lifecycle_impl.trySnapshotLsmMaintenanceStats(self);
    }

    pub fn snapshotLsmWriteStats(self: *DB) lsm_backend_mod.Backend.WriteStats {
        return lifecycle_impl.snapshotLsmWriteStats(self);
    }

    pub fn trySnapshotLsmWriteStats(self: *DB) ?lsm_backend_mod.Backend.WriteStats {
        return lifecycle_impl.trySnapshotLsmWriteStats(self);
    }

    pub fn snapshotTextMemoryAttributionStats(self: *DB) index_manager_mod.TextMemoryAttributionStats {
        return lifecycle_impl.snapshotTextMemoryAttributionStats(self);
    }

    pub fn trySnapshotTextMemoryAttributionStats(self: *DB) ?index_manager_mod.TextMemoryAttributionStats {
        return lifecycle_impl.trySnapshotTextMemoryAttributionStats(self);
    }

    pub fn snapshotTextMergeStats(self: *DB) types.TextMergeStats {
        return lifecycle_impl.snapshotTextMergeStats(self);
    }

    pub fn trySnapshotTextMergeStats(self: *DB) ?types.TextMergeStats {
        return lifecycle_impl.trySnapshotTextMergeStats(self);
    }

    pub fn snapshotPrimaryLsmWriteStatsForTest(self: *DB) ?lsm_backend_mod.Backend.WriteStats {
        return lifecycle_impl.snapshotPrimaryLsmWriteStatsForTest(self);
    }

    pub fn snapshotLsmNativeStorageStats(self: *DB) lsm_backend_mod.NativeStorageStats {
        return lifecycle_impl.snapshotLsmNativeStorageStats(self);
    }

    pub fn trySnapshotLsmNativeStorageStats(self: *DB) ?lsm_backend_mod.NativeStorageStats {
        return lifecycle_impl.trySnapshotLsmNativeStorageStats(self);
    }

    pub fn runLsmMaintenanceStep(self: *DB) !bool {
        try ha_replication_impl.enforceDurableMutationGate(self);
        return try lifecycle_impl.runLsmMaintenanceStep(self);
    }

    pub fn runPrimaryLsmMaintenanceStep(self: *DB) !bool {
        try ha_replication_impl.enforceDurableMutationGate(self);
        return try lifecycle_impl.runPrimaryLsmMaintenanceStep(self);
    }

    pub fn runLsmMaintenanceStepBestEffort(self: *DB) !bool {
        try ha_replication_impl.enforceDurableMutationGate(self);
        return try lifecycle_impl.runLsmMaintenanceStepBestEffort(self);
    }

    pub fn hasActiveDenseBulkWork(self: *const DB) bool {
        return db_internal.asyncContextHasActiveDenseBulkWork(self.async_context);
    }

    pub fn runLsmMaintenanceUntilIdle(self: *DB) !usize {
        try ha_replication_impl.enforceDurableMutationGate(self);
        return try lifecycle_impl.runLsmMaintenanceUntilIdle(self);
    }

    pub fn retryQuarantinedIndexLoads(self: *DB, force: bool) !index_manager_mod.IndexManager.QuarantineRetryResult {
        try ha_replication_impl.enforceDurableMutationGate(self);
        return try lifecycle_impl.retryQuarantinedIndexLoads(self, force);
    }

    pub fn runDueLsmObsoleteReclaimUntilIdle(self: *DB, max_steps: usize) !usize {
        try ha_replication_impl.enforceDurableMutationGate(self);
        return try lifecycle_impl.runDueLsmObsoleteReclaimUntilIdle(self, max_steps);
    }

    pub fn batch(self: *DB, req: types.BatchRequest) anyerror!void {
        try ha_replication_impl.enforceDurableMutationGate(self);
        return try write_path_impl.batchAfterGate(self, req);
    }

    pub fn batchProfiled(self: *DB, req: types.BatchRequest, profile: *BatchProfile) anyerror!void {
        try ha_replication_impl.enforceDurableMutationGate(self);
        return try write_path_impl.batchProfiledAfterGate(self, req, profile);
    }

    pub fn batchWithDocumentArtifactChildRangeDispatcher(
        self: *DB,
        req: types.BatchRequest,
        dispatcher: DocumentArtifactChildRangeDispatcher,
    ) anyerror!void {
        try ha_replication_impl.enforceDurableMutationGate(self);
        return try write_path_impl.batchWithDocumentArtifactChildRangeDispatcherAfterGate(self, req, dispatcher);
    }

    pub fn coalesceKeyValueRequest(
        self: *DB,
        comptime T: type,
        writes: []const T,
        deletes: []const []const u8,
        transforms: []const types.DocumentTransform,
    ) !write_path.CoalescedKeyValueRequest(T) {
        return try write_path_impl.coalesceKeyValueRequest(self, T, writes, deletes, transforms);
    }

    pub fn drainDocumentArtifactChildRangeOutbox(
        self: *DB,
        dispatcher: DocumentArtifactChildRangeDispatcher,
        limit: usize,
    ) anyerror!DocumentArtifactChildRangeOutboxDrainResult {
        try ha_replication_impl.enforceDurableMutationGate(self);
        return try write_path_impl.drainDocumentArtifactChildRangeOutboxAfterGate(self, dispatcher, limit);
    }

    pub fn batchWithoutRangeValidation(self: *DB, req: types.BatchRequest) anyerror!void {
        try ha_replication_impl.enforceDurableMutationGate(self);
        return try write_path_impl.batchWithoutRangeValidationAfterGate(self, req);
    }

    pub fn batchReplicatedApply(self: *DB, req: types.BatchRequest) anyerror!void {
        return try write_path_impl.batchReplicatedApply(self, req);
    }

    pub fn applyHAReplicationRecord(self: *DB, record: ha_replication_record_mod.RecordView) anyerror!void {
        return try ha_replication_impl.applyReplicationRecord(self, record);
    }

    pub fn applyHADerivedEffectRecord(self: *DB, record: ha_replication_record_mod.RecordView) anyerror!u64 {
        var ctx = self.batchContext();
        return try derived_async_impl.appendReplicatedHADerivedEffectContext(&ctx, record);
    }

    pub fn haAppliedReplicationLsn(self: *DB) anyerror!u64 {
        return try ha_replication_impl.appliedReplicationLsn(self);
    }

    pub fn applyHAReplicationRecordCallback(ctx: *anyopaque, record: ha_replication_record_mod.RecordView) anyerror!void {
        return try ha_replication_impl.applyReplicationRecordCallback(ctx, record);
    }

    pub fn applyDocumentArtifactChildRangeBatch(self: *DB, child_batch: DocumentArtifactChildRangeApplyBatch) anyerror!u64 {
        try ha_replication_impl.enforceDurableMutationGate(self);
        return try write_path_impl.applyDocumentArtifactChildRangeBatchAfterGate(self, child_batch);
    }

    /// Inject (or clear) the cross-shard entity-resolution candidate source on
    /// an already-open DB. The serving layer (managed write cache) calls this
    /// right after a DB is opened, since managed DBs open lazily and cannot
    /// thread the source through `OpenOptions`. No-op when this DB has no
    /// resolution runtime (e.g. read-only / status opens).
    pub fn setResolutionCandidateSource(self: *DB, src: ?resolution_runtime_mod.CandidateSource) void {
        lifecycle_impl.setResolutionCandidateSource(self, src);
    }

    /// Inject (or clear) the cross-shard entity sink on an already-open DB. The
    /// serving layer (managed write cache) calls this right after a DB is opened,
    /// since managed DBs open lazily and cannot thread the sink through
    /// `OpenOptions`. No-op when this DB has no promotion runtime.
    pub fn setEntitySink(self: *DB, sink: ?promotion_runtime_mod.EntitySink) void {
        lifecycle_impl.setEntitySink(self, sink);
    }

    /// Inject (or clear) the source-shard promotion owner on an already-open DB.
    /// Serving-layer managed DBs use this to keep follower raft apply from
    /// turning replay into public entity-table writes.
    pub fn setPromotionOwner(self: *DB, owner: ?promotion_runtime_mod.PromotionOwner) void {
        lifecycle_impl.setPromotionOwner(self, owner);
    }

    pub fn get(self: *DB, alloc: Allocator, key: []const u8) !?[]u8 {
        return try search_runtime_impl.get(self, alloc, key);
    }

    pub fn getRawStoreValue(self: *DB, alloc: Allocator, key: []const u8) !?[]u8 {
        return try search_runtime_impl.getRawStoreValue(self, alloc, key);
    }

    pub fn scanSystemVersionedHistoryForDocKeyAlloc(self: *DB, alloc: Allocator, key: []const u8) ![]docstore_mod.OwnedKVPair {
        return try relational_rows_impl.scanSystemVersionedHistoryForDocKeyAlloc(self, alloc, key);
    }

    pub fn scanSystemVersionedHistoryAlloc(self: *DB, alloc: Allocator) ![]docstore_mod.OwnedKVPair {
        return try relational_rows_impl.scanSystemVersionedHistoryAlloc(self, alloc);
    }

    pub fn querySystemVersionedRelationalRowsAsOfSequence(
        self: *DB,
        alloc: Allocator,
        runtime_schema: schema_mod.TableSchema,
        commit_sequence: u64,
        req: types.RelationalRowsQueryRequest,
    ) !types.RelationalRowsQueryResult {
        return try relational_rows_impl.querySystemVersionedRelationalRowsAsOfSequence(self, alloc, runtime_schema, commit_sequence, req);
    }

    pub fn lookupRelationalTemporalUniqueOwner(
        self: *DB,
        alloc: Allocator,
        constraint_name: []const u8,
        encoded_value: []const u8,
        encoded_point: []const u8,
    ) !?[]u8 {
        return try relational_rows_impl.lookupRelationalTemporalUniqueOwner(self, alloc, constraint_name, encoded_value, encoded_point);
    }

    pub fn lookupRelationalTemporalUniqueOverlapOwner(
        self: *DB,
        alloc: Allocator,
        constraint_name: []const u8,
        encoded_value: []const u8,
        encoded_start: []const u8,
        encoded_end: []const u8,
    ) !?[]u8 {
        return try relational_rows_impl.lookupRelationalTemporalUniqueOverlapOwner(self, alloc, constraint_name, encoded_value, encoded_start, encoded_end);
    }

    pub fn getGroupCreatedAtMillis(self: *DB, alloc: Allocator, group_id: u64) !?u64 {
        return try lifecycle_impl.getGroupCreatedAtMillis(self, alloc, group_id);
    }

    pub fn ensureGroupCreatedAtMillis(self: *DB, alloc: Allocator, group_id: u64, now_ms: u64) !u64 {
        try ha_replication_impl.enforceDurableMutationGate(self);
        return try lifecycle_impl.ensureGroupCreatedAtMillis(self, alloc, group_id, now_ms);
    }

    pub fn getArtifact(self: *DB, alloc: Allocator, artifact_id: []const u8) !?types.ArtifactRecord {
        return try search_runtime_impl.getArtifact(self, alloc, artifact_id);
    }

    pub fn getDocumentArtifactManifest(
        self: *DB,
        alloc: Allocator,
        doc_key: []const u8,
        artifact_name: []const u8,
    ) !?types.DocumentArtifactManifest {
        return try write_path_impl.getDocumentArtifactManifest(self, alloc, doc_key, artifact_name);
    }

    pub fn listDocumentArtifactManifests(
        self: *DB,
        alloc: Allocator,
        doc_key: []const u8,
    ) !types.DocumentArtifactManifestList {
        return try write_path_impl.listDocumentArtifactManifests(self, alloc, doc_key);
    }

    pub fn updateDocumentArtifactChildRangePlacement(
        self: *DB,
        alloc: Allocator,
        doc_key: []const u8,
        artifact_name: []const u8,
        update: types.DocumentArtifactChildRangePlacementUpdate,
    ) !bool {
        try ha_replication_impl.enforceDurableMutationGate(self);
        return try write_path_impl.updateDocumentArtifactChildRangePlacementAfterGate(self, alloc, doc_key, artifact_name, update);
    }

    pub fn reprocessDocumentArtifact(
        self: *DB,
        alloc: Allocator,
        doc_key: []const u8,
        artifact_name: []const u8,
    ) !bool {
        try ha_replication_impl.enforceDurableMutationGate(self);
        return try write_path_impl.reprocessDocumentArtifactAfterGate(self, alloc, doc_key, artifact_name);
    }

    pub fn reprocessDocumentArtifactRange(
        self: *DB,
        alloc: Allocator,
        artifact_name: []const u8,
        req: types.DocumentArtifactTableReprocessRequest,
    ) !types.DocumentArtifactTableReprocessResult {
        try ha_replication_impl.enforceDurableMutationGate(self);
        return try write_path_impl.reprocessDocumentArtifactRangeAfterGate(self, alloc, artifact_name, req);
    }

    pub fn getDocument(self: *DB, alloc: Allocator, key: []const u8, opts: types.LookupOptions) !?types.LookupResult {
        return try search_runtime_impl.getDocument(self, alloc, key, opts);
    }

    pub fn lookup(self: *DB, alloc: Allocator, key: []const u8, opts: types.LookupOptions) !?types.LookupResult {
        return try search_runtime_impl.lookup(self, alloc, key, opts);
    }

    pub fn getTimestamp(self: *DB, alloc: Allocator, key: []const u8) !u64 {
        return try search_runtime_impl.getTimestamp(self, alloc, key);
    }

    pub fn updateRange(self: *DB, byte_range: types.ByteRange) !void {
        try ha_replication_impl.enforceDurableMutationGate(self);
        try split_restore_impl.updateRangeAfterGate(self, byte_range);
    }

    pub fn getRange(self: *DB) types.ByteRange {
        return split_restore_impl.getRange(self);
    }

    pub fn findMedianKey(self: *DB, alloc: Allocator) ![]u8 {
        return try split_restore_impl.findMedianKey(self, alloc);
    }

    pub fn getSplitState(self: *DB, alloc: Allocator) !?types.SplitState {
        return try split_restore_impl.getSplitState(self, alloc);
    }

    pub fn setSplitState(self: *DB, state: ?types.SplitState) !void {
        try ha_replication_impl.enforceDurableMutationGate(self);
        try split_restore_impl.setSplitState(self, state);
    }

    pub fn clearSplitState(self: *DB) !void {
        try ha_replication_impl.enforceDurableMutationGate(self);
        try split_restore_impl.clearSplitState(self);
    }

    pub fn getSplitDeltaSeq(self: *DB) u64 {
        return split_restore_impl.getSplitDeltaSeq(self);
    }

    pub fn getSplitDeltaFinalSeq(self: *DB, alloc: Allocator) !u64 {
        return try split_restore_impl.getSplitDeltaFinalSeq(self, alloc);
    }

    pub fn setSplitDeltaFinalSeq(self: *DB, seq: u64) !void {
        try ha_replication_impl.enforceDurableMutationGate(self);
        try split_restore_impl.setSplitDeltaFinalSeq(self, seq);
    }

    pub fn clearSplitDeltaFinalSeq(self: *DB) !void {
        try ha_replication_impl.enforceDurableMutationGate(self);
        try split_restore_impl.clearSplitDeltaFinalSeq(self);
    }

    pub fn listSplitDeltaEntriesAfter(self: *DB, alloc: Allocator, after_seq: u64) ![]types.SplitDeltaEntry {
        return try split_restore_impl.listSplitDeltaEntriesAfter(self, alloc, after_seq);
    }

    pub fn clearSplitDeltaEntries(self: *DB) !void {
        try ha_replication_impl.enforceDurableMutationGate(self);
        try split_restore_impl.clearSplitDeltaEntries(self);
    }

    pub fn createShadowIndexManager(self: *DB, split_key: []const u8, original_range_end: []const u8) !void {
        try ha_replication_impl.enforceDurableMutationGate(self);
        try split_restore_impl.createShadowIndexManager(self, split_key, original_range_end);
    }

    pub fn closeShadowIndexManager(self: *DB) !void {
        try ha_replication_impl.enforceDurableMutationGate(self);
        try split_restore_impl.closeShadowIndexManager(self);
    }

    pub fn getShadowIndexDir(self: *DB) []const u8 {
        return split_restore_impl.getShadowIndexDir(self);
    }

    pub fn split(
        self: *DB,
        curr_range: types.ByteRange,
        split_key: []const u8,
        dest_dir1: []const u8,
        dest_dir2: []const u8,
        prepare_only: bool,
    ) !void {
        try ha_replication_impl.enforceDurableMutationGate(self);
        try split_restore_impl.split(self, curr_range, split_key, dest_dir1, dest_dir2, prepare_only);
    }

    pub fn finalizeSplit(self: *DB, new_range: types.ByteRange) !void {
        try ha_replication_impl.enforceDurableMutationGate(self);
        try split_restore_impl.finalizeSplit(self, new_range);
    }

    pub fn snapshot(self: *DB, id: []const u8) !u64 {
        try ha_replication_impl.enforceDurableMutationGate(self);
        return try split_restore_impl.snapshot(self, id);
    }

    pub fn sync(self: *DB, full: bool) !void {
        try ha_replication_impl.enforceDurableMutationGate(self);
        try lifecycle_impl.sync(self, full);
    }

    pub fn syncIndexes(self: *DB, force: bool) !void {
        try ha_replication_impl.enforceDurableMutationGate(self);
        try lifecycle_impl.syncIndexes(self, force);
    }

    pub fn restoreSnapshotTo(alloc: Allocator, snapshot_root: []const u8, path: []const u8, opts: OpenOptions) !void {
        try split_restore_impl.restoreSnapshotTo(alloc, snapshot_root, path, opts);
    }

    pub fn restoreSnapshotToDeferredRuntimeRepair(
        alloc: Allocator,
        snapshot_root: []const u8,
        path: []const u8,
        opts: OpenOptions,
        identity: RestoreIdentity,
    ) !void {
        try split_restore_impl.restoreSnapshotToDeferredRuntimeRepair(alloc, snapshot_root, path, opts, identity);
    }

    pub fn recoverIncompleteRestoreImportIfNeeded(alloc: Allocator, path: []const u8, opts: OpenOptions) !bool {
        return try split_restore_impl.recoverIncompleteRestoreImportIfNeeded(alloc, path, opts);
    }

    pub fn beginRestoreImport(alloc: Allocator, path: []const u8, snapshot_root: []const u8, identity: RestoreIdentity) !void {
        try split_restore_impl.beginRestoreImport(alloc, path, snapshot_root, identity);
    }

    pub fn readRestoreStateForPath(alloc: Allocator, path: []const u8) !?RestoreState {
        return try split_restore_impl.readRestoreStateForPath(alloc, path);
    }

    pub fn markRestorePrimaryRestoredForPath(
        alloc: Allocator,
        path: []const u8,
        backup_id: []const u8,
        location: []const u8,
        snapshot_path: []const u8,
        group_id: u64,
    ) !void {
        try split_restore_impl.markRestorePrimaryRestoredForPath(alloc, path, backup_id, location, snapshot_path, group_id);
    }

    pub fn restoreRuntimeRepairNeededForPath(alloc: Allocator, path: []const u8) !bool {
        return try split_restore_impl.restoreRuntimeRepairNeededForPath(alloc, path);
    }

    pub fn markRestoreRuntimeRepairNeeded(alloc: Allocator, path: []const u8) !void {
        try split_restore_impl.markRestoreRuntimeRepairNeeded(alloc, path);
    }

    pub fn restoreRuntimeRepairNeeded(self: *DB) !bool {
        return try split_restore_impl.restoreRuntimeRepairNeeded(self);
    }

    pub fn repairRestoreRuntimeStateStepIfNeeded(self: *DB, alloc: Allocator) !bool {
        try ha_replication_impl.enforceDurableMutationGate(self);
        return try split_restore_impl.repairRestoreRuntimeStateStepIfNeeded(self, alloc);
    }

    pub fn repairRestoreRuntimeStateIfNeeded(self: *DB, alloc: Allocator) !bool {
        try ha_replication_impl.enforceDurableMutationGate(self);
        return try split_restore_impl.repairRestoreRuntimeStateIfNeeded(self, alloc);
    }

    pub fn clearDenseHbcCaches(self: *DB) void {
        schema_runtime_impl.clearDenseHbcCaches(self);
    }

    pub fn setSchema(self: *DB, table_schema: schema_mod.TableSchema) !void {
        try ha_replication_impl.enforceDurableMutationGate(self);
        try ha_replication_impl.preflightDBMetadataSyncCommit(self);
        try schema_runtime_impl.setSchemaAfterGate(self, table_schema);
    }

    pub const ApplyTableSchemaOptions = schema_runtime.ApplyTableSchemaOptions;

    /// Apply table metadata schema JSON to the DB runtime and all schema-derived
    /// local artifacts. This is the single production entry point for table
    /// schema application so write-cache reconciliation, metadata provisioning,
    /// and crash recovery keep algebraic sidecars in the same lifecycle state.
    pub fn applyTableSchemaJson(
        self: *DB,
        alloc: Allocator,
        schema_json: []const u8,
        options: ApplyTableSchemaOptions,
    ) !void {
        if (schema_json.len == 0) return;
        try ha_replication_impl.enforceDurableMutationGate(self);
        try ha_replication_impl.preflightDBMetadataSyncCommit(self);
        try schema_runtime_impl.applyTableSchemaJsonAfterGate(self, alloc, schema_json, options);
    }

    pub const SchemaRewriteJobExecutionResult = schema_runtime.SchemaRewriteJobExecutionResult;
    pub const SchemaRewriteJobDrainOptions = schema_runtime.SchemaRewriteJobDrainOptions;

    pub fn drainSchemaRewriteJobsForIdle(
        self: *DB,
        alloc: Allocator,
        service: anytype,
        options: SchemaRewriteJobDrainOptions,
    ) !usize {
        try ha_replication_impl.enforceDurableMutationGate(self);
        return try schema_runtime_impl.drainSchemaRewriteJobsForIdle(self, alloc, service, options);
    }

    pub fn executeClaimedSchemaRewriteJob(
        self: *DB,
        alloc: Allocator,
        job: metadata_table_manager.SchemaRewriteJobRecord,
    ) !SchemaRewriteJobExecutionResult {
        try ha_replication_impl.enforceDurableMutationGate(self);
        return try schema_runtime_impl.executeClaimedSchemaRewriteJob(self, alloc, job);
    }

    /// Returns the relational column catalog when the table is in relational
    /// storage mode (so document writes store a dedicated relational base row),
    /// or null for document-mode tables (which keep the JSON blob).
    pub fn relationalColumnsForStore(self: *DB) ?[]const schema_mod.RelationalColumn {
        return schema_runtime_impl.relationalColumnsForStore(self);
    }

    pub fn rebuildRelationalSecondaryIndexInRange(
        self: *DB,
        index_name: []const u8,
        index_generation: u64,
        lower_doc_key: []const u8,
        upper_doc_key: []const u8,
    ) !relational_store_mod.SecondaryIndexRebuildReport {
        try ha_replication_impl.enforceDurableMutationGate(self);
        return try schema_runtime_impl.rebuildRelationalSecondaryIndexInRange(self, index_name, index_generation, lower_doc_key, upper_doc_key);
    }

    pub fn validateForeignKeyRefsInRange(
        self: *DB,
        lower_doc_key: []const u8,
        upper_doc_key: []const u8,
    ) !ForeignKeyIntegrityReport {
        return try relational_integrity_impl.validateForeignKeyRefsInRange(self, lower_doc_key, upper_doc_key);
    }

    pub fn validateForeignKeyRefsInRangeForConstraint(
        self: *DB,
        constraint_name: ?[]const u8,
        lower_doc_key: []const u8,
        upper_doc_key: []const u8,
    ) !ForeignKeyIntegrityReport {
        return try relational_integrity_impl.validateForeignKeyRefsInRangeForConstraint(self, constraint_name, lower_doc_key, upper_doc_key);
    }

    pub fn repairForeignKeyRefsInRange(
        self: *DB,
        lower_doc_key: []const u8,
        upper_doc_key: []const u8,
    ) !ForeignKeyIntegrityReport {
        try ha_replication_impl.enforceDurableMutationGate(self);
        return try relational_integrity_impl.repairForeignKeyRefsInRange(self, lower_doc_key, upper_doc_key);
    }

    pub fn repairForeignKeyRefsInRangeForConstraint(
        self: *DB,
        constraint_name: ?[]const u8,
        lower_doc_key: []const u8,
        upper_doc_key: []const u8,
    ) !ForeignKeyIntegrityReport {
        try ha_replication_impl.enforceDurableMutationGate(self);
        return try relational_integrity_impl.repairForeignKeyRefsInRangeForConstraint(self, constraint_name, lower_doc_key, upper_doc_key);
    }

    pub fn dryRunRepairForeignKeyRefsInRange(
        self: *DB,
        lower_doc_key: []const u8,
        upper_doc_key: []const u8,
    ) !ForeignKeyIntegrityReport {
        return try relational_integrity_impl.dryRunRepairForeignKeyRefsInRange(self, lower_doc_key, upper_doc_key);
    }

    pub fn dryRunRepairForeignKeyRefsInRangeForConstraint(
        self: *DB,
        constraint_name: ?[]const u8,
        lower_doc_key: []const u8,
        upper_doc_key: []const u8,
    ) !ForeignKeyIntegrityReport {
        return try relational_integrity_impl.dryRunRepairForeignKeyRefsInRangeForConstraint(self, constraint_name, lower_doc_key, upper_doc_key);
    }

    pub fn validateUniqueConstraintRowsInRange(
        self: *DB,
        lower_doc_key: []const u8,
        upper_doc_key: []const u8,
    ) !UniqueConstraintIntegrityReport {
        return try relational_integrity_impl.validateUniqueConstraintRowsInRange(self, lower_doc_key, upper_doc_key);
    }

    pub fn dryRunRepairUniqueConstraintRowsInRange(
        self: *DB,
        lower_doc_key: []const u8,
        upper_doc_key: []const u8,
    ) !UniqueConstraintIntegrityReport {
        return try relational_integrity_impl.dryRunRepairUniqueConstraintRowsInRange(self, lower_doc_key, upper_doc_key);
    }

    pub fn repairUniqueConstraintRowsInRange(
        self: *DB,
        lower_doc_key: []const u8,
        upper_doc_key: []const u8,
    ) !UniqueConstraintIntegrityReport {
        try ha_replication_impl.enforceDurableMutationGate(self);
        return try relational_integrity_impl.repairUniqueConstraintRowsInRange(self, lower_doc_key, upper_doc_key);
    }

    pub fn validateForeignKeyRefOwnerForParent(
        self: *DB,
        constraint_name: []const u8,
        parent_table: []const u8,
        parent_key: []const u8,
    ) !ForeignKeyIntegrityReport {
        return try relational_integrity_impl.validateForeignKeyRefOwnerForParent(self, constraint_name, parent_table, parent_key);
    }

    pub fn dryRunRepairForeignKeyRefOwnerForParent(
        self: *DB,
        constraint_name: []const u8,
        parent_table: []const u8,
        parent_key: []const u8,
    ) !ForeignKeyIntegrityReport {
        return try relational_integrity_impl.dryRunRepairForeignKeyRefOwnerForParent(self, constraint_name, parent_table, parent_key);
    }

    pub fn repairForeignKeyRefOwnerForParent(
        self: *DB,
        constraint_name: []const u8,
        parent_table: []const u8,
        parent_key: []const u8,
    ) !ForeignKeyIntegrityReport {
        try ha_replication_impl.enforceDurableMutationGate(self);
        return try relational_integrity_impl.repairForeignKeyRefOwnerForParent(self, constraint_name, parent_table, parent_key);
    }

    pub fn validateForeignKeyRefOwnerRange(
        self: *DB,
        constraint_name: []const u8,
        parent_table: []const u8,
        start_parent_key: []const u8,
        end_parent_key: []const u8,
    ) !ForeignKeyIntegrityReport {
        return try relational_integrity_impl.validateForeignKeyRefOwnerRange(self, constraint_name, parent_table, start_parent_key, end_parent_key);
    }

    pub fn dryRunRepairForeignKeyRefOwnerRange(
        self: *DB,
        constraint_name: []const u8,
        parent_table: []const u8,
        start_parent_key: []const u8,
        end_parent_key: []const u8,
    ) !ForeignKeyIntegrityReport {
        return try relational_integrity_impl.dryRunRepairForeignKeyRefOwnerRange(self, constraint_name, parent_table, start_parent_key, end_parent_key);
    }

    pub fn repairForeignKeyRefOwnerRange(
        self: *DB,
        constraint_name: []const u8,
        parent_table: []const u8,
        start_parent_key: []const u8,
        end_parent_key: []const u8,
    ) !ForeignKeyIntegrityReport {
        try ha_replication_impl.enforceDurableMutationGate(self);
        return try relational_integrity_impl.repairForeignKeyRefOwnerRange(self, constraint_name, parent_table, start_parent_key, end_parent_key);
    }

    pub fn explainForeignKeyDelete(self: *DB, doc_key: []const u8) !ForeignKeyDeletePlan {
        return try relational_integrity_impl.explainForeignKeyDelete(self, doc_key);
    }

    pub fn explainForeignKeyDeleteForConstraint(self: *DB, constraint_name: ?[]const u8, doc_key: []const u8) !ForeignKeyDeletePlan {
        return try relational_integrity_impl.explainForeignKeyDeleteForConstraint(self, constraint_name, doc_key);
    }

    pub fn listForeignKeyViolationsInRange(
        self: *DB,
        lower_doc_key: []const u8,
        upper_doc_key: []const u8,
    ) ![]ForeignKeyIntegrityViolation {
        return try relational_integrity_impl.listForeignKeyViolationsInRange(self, lower_doc_key, upper_doc_key);
    }

    pub fn listForeignKeyViolationsInRangeForConstraint(
        self: *DB,
        constraint_name: ?[]const u8,
        lower_doc_key: []const u8,
        upper_doc_key: []const u8,
    ) ![]ForeignKeyIntegrityViolation {
        return try relational_integrity_impl.listForeignKeyViolationsInRangeForConstraint(self, constraint_name, lower_doc_key, upper_doc_key);
    }

    pub fn freeForeignKeyIntegrityViolations(self: *DB, violations: []ForeignKeyIntegrityViolation) void {
        relational_integrity_impl.freeForeignKeyIntegrityViolations(self, violations);
    }

    pub fn listForeignKeyRefChildrenForParent(
        self: *DB,
        alloc: Allocator,
        constraint_name: []const u8,
        parent_table: []const u8,
        parent_key: []const u8,
        limit: usize,
    ) ![]types.ForeignKeyRefChild {
        return try relational_integrity_impl.listForeignKeyRefChildrenForParent(self, alloc, constraint_name, parent_table, parent_key, limit);
    }

    pub fn listForeignKeyRefChildrenPageForParent(
        self: *DB,
        alloc: Allocator,
        constraint_name: []const u8,
        parent_table: []const u8,
        parent_key: []const u8,
        start_after_child_table: ?[]const u8,
        start_after_child_key: ?[]const u8,
        limit: usize,
    ) !types.ForeignKeyRefChildrenPage {
        return try relational_integrity_impl.listForeignKeyRefChildrenPageForParent(self, alloc, constraint_name, parent_table, parent_key, start_after_child_table, start_after_child_key, limit);
    }

    pub fn freeForeignKeyRefChildren(self: *DB, alloc: Allocator, children: []types.ForeignKeyRefChild) void {
        relational_integrity_impl.freeForeignKeyRefChildren(self, alloc, children);
    }

    pub fn freeForeignKeyRefChildrenPage(self: *DB, alloc: Allocator, page: *types.ForeignKeyRefChildrenPage) void {
        relational_integrity_impl.freeForeignKeyRefChildrenPage(self, alloc, page);
    }

    pub fn relationalIntegrityReconcileForeignKeyRefsInRangeLocked(
        self: *DB,
        constraint_name: ?[]const u8,
        lower_doc_key: []const u8,
        upper_doc_key: []const u8,
        mode: relational_store_mod.ForeignKeyIntegrityMode,
    ) !ForeignKeyIntegrityReport {
        return try relational_integrity_impl.reconcileForeignKeyRefsInRangeLocked(self, constraint_name, lower_doc_key, upper_doc_key, mode);
    }

    pub fn relationalIntegrityReconcileUniqueConstraintRowsInRangeLocked(
        self: *DB,
        lower_doc_key: []const u8,
        upper_doc_key: []const u8,
        mode: relational_store_mod.ForeignKeyIntegrityMode,
    ) !UniqueConstraintIntegrityReport {
        return try relational_integrity_impl.reconcileUniqueConstraintRowsInRangeLocked(self, lower_doc_key, upper_doc_key, mode);
    }

    pub fn relationalIntegrityReconcileForeignKeyRefOwnerForParentLocked(
        self: *DB,
        constraint_name: []const u8,
        parent_table: []const u8,
        parent_key: []const u8,
        mode: relational_store_mod.ForeignKeyIntegrityMode,
    ) !ForeignKeyIntegrityReport {
        return try relational_integrity_impl.reconcileForeignKeyRefOwnerForParentLocked(self, constraint_name, parent_table, parent_key, mode);
    }

    pub fn relationalIntegrityReconcileForeignKeyRefOwnerRangeLocked(
        self: *DB,
        constraint_name: []const u8,
        parent_table: []const u8,
        start_parent_key: []const u8,
        end_parent_key: []const u8,
        mode: relational_store_mod.ForeignKeyIntegrityMode,
    ) !ForeignKeyIntegrityReport {
        return try relational_integrity_impl.reconcileForeignKeyRefOwnerRangeLocked(self, constraint_name, parent_table, start_parent_key, end_parent_key, mode);
    }

    pub fn relationalIntegrityReconcileForeignKeyRefOwnerRangeForConstraintLocked(
        self: *DB,
        constraint_name: []const u8,
        start_parent_key: []const u8,
        end_parent_key: []const u8,
        mode: relational_store_mod.ForeignKeyIntegrityMode,
    ) !ForeignKeyIntegrityReport {
        return try relational_integrity_impl.reconcileForeignKeyRefOwnerRangeForConstraintLocked(self, constraint_name, start_parent_key, end_parent_key, mode);
    }

    /// Refresh schema-derived algebraic index configs for callers that have
    /// already applied the runtime schema. This remains a structural mutation:
    /// config swaps, sidecar clears, and rebuild replay are serialized with
    /// normal apply work just like `applyTableSchemaJson`.
    pub fn reloadAlgebraicSchemaConfigs(self: *DB, schema_json: []const u8) !void {
        if (schema_json.len == 0) return;
        try ha_replication_impl.enforceDurableMutationGate(self);
        try schema_runtime_impl.reloadAlgebraicSchemaConfigsAfterGate(self, schema_json);
    }

    pub fn schemaRuntimeStageAlgebraicSchemaConfigsPending(self: *DB, schema_json: []const u8) !void {
        if (schema_json.len == 0) return;
        try ha_replication_impl.enforceDurableMutationGate(self);
        try schema_runtime_impl.stageAlgebraicSchemaConfigsPending(self, schema_json);
    }

    pub fn setSchemaJson(self: *DB, alloc: Allocator, schema_json: []const u8) !void {
        try self.applyTableSchemaJson(alloc, schema_json, .{});
    }

    pub fn getSchemaJson(self: *DB, alloc: Allocator) !?[]u8 {
        return try schema_runtime_impl.getSchemaJson(self, alloc);
    }

    pub fn applyLiteSqlTableRecord(self: *DB, alloc: Allocator, table: metadata_table_manager.TableRecord) !void {
        if (table.schema_json.len == 0) return error.InvalidSchemaUpdateRequest;
        try ha_replication_impl.enforceDurableMutationGate(self);
        try ha_replication_impl.preflightDBMetadataSyncCommit(self);
        try schema_runtime_impl.applyLiteSqlTableRecordAfterGate(self, alloc, table);
    }

    pub fn getLiteSqlTableRecordAlloc(self: *DB, alloc: Allocator) !?metadata_table_manager.TableRecord {
        return try schema_runtime_impl.getLiteSqlTableRecordAlloc(self, alloc);
    }

    pub fn beginTransaction(self: *DB, timestamp_ns: u64) !transactions_mod.TxnId {
        try ha_replication_impl.enforceDurableMutationGate(self);
        return try db_transactions_impl.beginTransaction(self, timestamp_ns);
    }

    pub fn beginTransactionWithId(self: *DB, txn_id: transactions_mod.TxnId, timestamp_ns: u64) !transactions_mod.TxnId {
        try ha_replication_impl.enforceDurableMutationGate(self);
        return try db_transactions_impl.beginTransactionWithId(self, txn_id, timestamp_ns);
    }

    pub fn beginTransactionWithParticipants(self: *DB, timestamp_ns: u64, participants: []const []const u8) !transactions_mod.TxnId {
        try ha_replication_impl.enforceDurableMutationGate(self);
        return try db_transactions_impl.beginTransactionWithParticipants(self, timestamp_ns, participants);
    }

    pub fn beginTransactionWithIdAndParticipants(self: *DB, txn_id: transactions_mod.TxnId, timestamp_ns: u64, participants: []const []const u8) !transactions_mod.TxnId {
        try ha_replication_impl.enforceDurableMutationGate(self);
        return try db_transactions_impl.beginTransactionWithIdAndParticipants(self, txn_id, timestamp_ns, participants);
    }

    pub fn writeIntents(
        self: *DB,
        txn_id: transactions_mod.TxnId,
        intents: []const transactions_mod.WriteIntent,
        predicates: []const transactions_mod.VersionPredicate,
    ) !void {
        try ha_replication_impl.enforceDurableMutationGate(self);
        return try db_transactions_impl.writeIntents(self, txn_id, intents, predicates);
    }

    pub fn writeTransaction(self: *DB, txn_id: types.TxnId, req: types.TransactionIntentRequest) !void {
        try ha_replication_impl.enforceDurableMutationGate(self);
        return try db_transactions_impl.writeTransaction(self, txn_id, req);
    }

    pub fn claimRowsForTransaction(
        self: *DB,
        txn_id: types.TxnId,
        row_keys: []const []const u8,
        claim: types.RowClaimRequest,
    ) !void {
        try ha_replication_impl.enforceDurableMutationGate(self);
        return try db_transactions_impl.claimRowsForTransaction(self, txn_id, row_keys, claim);
    }

    pub fn commitTransaction(self: *DB, txn_id: transactions_mod.TxnId, timestamp_ns: u64) !void {
        try ha_replication_impl.enforceDurableMutationGate(self);
        return try db_transactions_impl.commitTransaction(self, txn_id, timestamp_ns);
    }

    pub fn resolveTransactionIntents(self: *DB, txn_id: transactions_mod.TxnId, status: transactions_mod.TxnStatus, commit_version: u64) !void {
        try ha_replication_impl.enforceDurableMutationGate(self);
        return try db_transactions_impl.resolveTransactionIntents(self, txn_id, status, commit_version);
    }

    pub fn abortTransaction(self: *DB, txn_id: transactions_mod.TxnId, timestamp_ns: u64) !void {
        try ha_replication_impl.enforceDurableMutationGate(self);
        return try db_transactions_impl.abortTransaction(self, txn_id, timestamp_ns);
    }

    pub fn getTransactionStatus(self: *DB, txn_id: transactions_mod.TxnId) !transactions_mod.TxnStatus {
        return try db_transactions_impl.getTransactionStatus(self, txn_id);
    }

    pub fn getCommitVersion(self: *DB, txn_id: transactions_mod.TxnId) !u64 {
        return try db_transactions_impl.getCommitVersion(self, txn_id);
    }

    pub fn markTransactionParticipantResolved(self: *DB, txn_id: transactions_mod.TxnId, participant: []const u8) !void {
        try ha_replication_impl.enforceDurableMutationGate(self);
        return try db_transactions_impl.markTransactionParticipantResolved(self, txn_id, participant);
    }

    pub fn getTransactionParticipants(self: *DB, alloc: Allocator, txn_id: transactions_mod.TxnId) ![][]u8 {
        return try db_transactions_impl.getTransactionParticipants(self, alloc, txn_id);
    }

    pub fn getUnresolvedTransactionParticipants(self: *DB, alloc: Allocator, txn_id: transactions_mod.TxnId) ![][]u8 {
        return try db_transactions_impl.getUnresolvedTransactionParticipants(self, alloc, txn_id);
    }

    pub fn recoverTransactions(self: *DB, cutoff_timestamp: u64, resolution_timestamp: u64) !transactions_mod.RecoveryStats {
        try ha_replication_impl.enforceDurableMutationGate(self);
        return try db_transactions_impl.recoverTransactions(self, cutoff_timestamp, resolution_timestamp);
    }

    pub fn getEdges(
        self: *DB,
        alloc: Allocator,
        index_name: []const u8,
        key: []const u8,
        edge_type: []const u8,
        direction: search_runtime.EdgeDirection,
    ) ![]search_runtime.Edge {
        return try search_runtime_impl.getEdges(self, alloc, index_name, key, edge_type, direction);
    }

    pub fn traverseEdges(
        self: *DB,
        alloc: Allocator,
        index_name: []const u8,
        start_key: []const u8,
        rules: search_runtime.TraversalRules,
    ) ![]search_runtime.TraversalResult {
        return try search_runtime_impl.traverseEdges(self, alloc, index_name, start_key, rules);
    }

    pub fn getNeighbors(
        self: *DB,
        alloc: Allocator,
        index_name: []const u8,
        key: []const u8,
        edge_type: []const u8,
        direction: search_runtime.EdgeDirection,
    ) ![]search_runtime.TraversalResult {
        return try search_runtime_impl.getNeighbors(self, alloc, index_name, key, edge_type, direction);
    }

    pub fn findShortestPath(
        self: *DB,
        alloc: Allocator,
        index_name: []const u8,
        source: []const u8,
        target: []const u8,
        edge_types: []const []const u8,
        direction: search_runtime.EdgeDirection,
        weight_mode: search_runtime.PathWeightMode,
        max_depth: u32,
        min_weight: f64,
        max_weight: f64,
    ) !?search_runtime.Path {
        return try search_runtime_impl.findShortestPath(self, alloc, index_name, source, target, edge_types, direction, weight_mode, max_depth, min_weight, max_weight);
    }

    pub fn findKShortestPaths(
        self: *DB,
        alloc: Allocator,
        index_name: []const u8,
        source: []const u8,
        target: []const u8,
        k: u32,
        edge_types: []const []const u8,
        direction: search_runtime.EdgeDirection,
        weight_mode: search_runtime.PathWeightMode,
        max_depth: u32,
        min_weight: f64,
        max_weight: f64,
    ) ![]search_runtime.Path {
        return try search_runtime_impl.findKShortestPaths(self, alloc, index_name, source, target, k, edge_types, direction, weight_mode, max_depth, min_weight, max_weight);
    }

    pub fn matchPattern(
        self: *DB,
        alloc: Allocator,
        index_name: []const u8,
        start_keys: []const []const u8,
        pattern: []const search_runtime.PatternStep,
        max_results: u32,
        return_aliases: []const []const u8,
    ) ![]search_runtime.PatternMatch {
        return try search_runtime_impl.matchPattern(self, alloc, index_name, start_keys, pattern, max_results, return_aliases);
    }

    pub fn executeNamedGraphQueries(
        self: *DB,
        alloc: Allocator,
        req: types.SearchRequest,
        graph_queries: []const types.NamedGraphQuery,
        input_sets: []const types.NamedGraphInputSet,
    ) ![]types.GraphSearchResult {
        return try search_runtime_impl.executeNamedGraphQueries(self, alloc, req, graph_queries, input_sets);
    }

    pub fn addIndex(self: *DB, cfg: types.IndexConfig) !void {
        try ha_replication_impl.enforceDurableMutationGate(self);
        return try schema_runtime_impl.addIndex(self, cfg);
    }

    pub fn addEnrichment(self: *DB, cfg: types.EnrichmentConfig) !void {
        try ha_replication_impl.enforceDurableMutationGate(self);
        return try schema_runtime_impl.addEnrichment(self, cfg);
    }

    pub fn upsertEnrichment(self: *DB, cfg: types.EnrichmentConfig) !index_manager_mod.IndexManager.EnrichmentUpsertResult {
        try ha_replication_impl.enforceDurableMutationGate(self);
        return try schema_runtime_impl.upsertEnrichment(self, cfg);
    }

    pub fn addResolver(self: *DB, cfg: index_manager_mod.ResolverConfig) !void {
        try ha_replication_impl.enforceDurableMutationGate(self);
        return try lifecycle_impl.addResolver(self, cfg);
    }

    pub const ResolverUpsertOptions = lifecycle_mod.ResolverUpsertOptions;

    /// Add or replace a resolver. Inserts and material config changes re-resolve
    /// the existing corpus so the new resolver/scorer behavior applies to
    /// documents already ingested (the extraction artifacts did not change, so
    /// the incremental hint would not fire on its own).
    pub fn upsertResolverWithResultOptions(
        self: *DB,
        cfg: index_manager_mod.ResolverConfig,
        options: ResolverUpsertOptions,
    ) !index_manager_mod.IndexManager.ResolverUpsertResult {
        try ha_replication_impl.enforceDurableMutationGate(self);
        return try lifecycle_impl.upsertResolverWithResultOptions(self, cfg, options);
    }

    pub fn upsertResolverWithResult(self: *DB, cfg: index_manager_mod.ResolverConfig) !index_manager_mod.IndexManager.ResolverUpsertResult {
        try ha_replication_impl.enforceDurableMutationGate(self);
        return try lifecycle_impl.upsertResolverWithResult(self, cfg);
    }

    pub fn upsertResolver(self: *DB, cfg: index_manager_mod.ResolverConfig) !void {
        try ha_replication_impl.enforceDurableMutationGate(self);
        return try lifecycle_impl.upsertResolver(self, cfg);
    }

    pub fn drainResolverBackfill(self: *DB) !void {
        try ha_replication_impl.enforceDurableMutationGate(self);
        return try lifecycle_impl.drainResolverBackfill(self);
    }

    pub fn removeResolver(self: *DB, name: []const u8) !bool {
        try ha_replication_impl.enforceDurableMutationGate(self);
        return try lifecycle_impl.removeResolver(self, name);
    }

    /// The review queue: review-band mentions awaiting human curation. Empty
    /// when no resolution runtime is active. Caller owns the result
    /// (`resolution_runtime_mod.freePendingReviews`).
    pub fn listPendingReviews(self: *DB, alloc: Allocator) ![]resolution_runtime_mod.PendingReview {
        return try lifecycle_impl.listPendingReviews(self, alloc);
    }

    /// Persist a human curation decision for one review-band mention and enqueue
    /// the document for ordinary replay-driven resolution/promotion.
    pub fn recordReviewDecision(
        self: *DB,
        doc_key: []const u8,
        source_artifact: []const u8,
        resolution_artifact: []const u8,
        local_id: []const u8,
        decision: resolver_lib.Decision,
        table: []const u8,
        key: []const u8,
    ) !u64 {
        try ha_replication_impl.enforceDurableMutationGate(self);
        return try lifecycle_impl.recordReviewDecisionAfterGate(self, doc_key, source_artifact, resolution_artifact, local_id, decision, table, key);
    }

    /// Eager edge rewrite for an entity merge: repoint every inbound edge of
    /// `old_key` (the merged-away entity) at `new_key` (the survivor) in the
    /// given graph index, preserving edge type, weight, and metadata. Already
    /// materialized provenance mention edges do not follow `merged_into` on their
    /// own; this brings the graph in line with a merge.
    /// Returns the number of edges rewritten.
    pub fn rewriteEntityEdges(
        self: *DB,
        alloc: Allocator,
        index_name: []const u8,
        old_key: []const u8,
        new_key: []const u8,
    ) !usize {
        try ha_replication_impl.enforceDurableMutationGate(self);
        return try search_runtime_impl.rewriteEntityEdges(self, alloc, index_name, old_key, new_key);
    }

    pub fn hasIndex(self: *DB, name: []const u8) bool {
        return schema_runtime_impl.hasIndex(self, name);
    }

    pub fn listIndexes(self: *DB, alloc: Allocator) ![]types.IndexConfig {
        return try schema_runtime_impl.listIndexes(self, alloc);
    }

    pub fn listAlgebraicMaterializationStates(self: *DB, alloc: Allocator, index_name: ?[]const u8) ![]types.AlgebraicMaterializationState {
        return try schema_runtime_impl.listAlgebraicMaterializationStates(self, alloc, index_name);
    }

    pub fn listAlgebraicQueryObservations(self: *DB, alloc: Allocator, index_name: ?[]const u8) ![]types.AlgebraicQueryObservation {
        return try schema_runtime_impl.listAlgebraicQueryObservations(self, alloc, index_name);
    }

    pub fn evaluateAlgebraicAdaptiveCandidates(self: *DB) !u64 {
        try ha_replication_impl.enforceDurableMutationGate(self);
        return try schema_runtime_impl.evaluateAlgebraicAdaptiveCandidates(self);
    }

    pub fn runAlgebraicAdaptiveWork(self: *DB) !u64 {
        try ha_replication_impl.enforceDurableMutationGate(self);
        return try schema_runtime_impl.runAlgebraicAdaptiveWork(self);
    }

    pub fn listAlgebraicAdaptiveCandidates(self: *DB, alloc: Allocator, index_name: ?[]const u8) ![]types.AlgebraicAdaptiveCandidate {
        return try schema_runtime_impl.listAlgebraicAdaptiveCandidates(self, alloc, index_name);
    }

    pub fn listAlgebraicAdaptiveProgress(self: *DB, alloc: Allocator, index_name: ?[]const u8) ![]types.AlgebraicAdaptiveProgress {
        return try schema_runtime_impl.listAlgebraicAdaptiveProgress(self, alloc, index_name);
    }

    pub fn listDerivedReplayDebt(self: *DB, alloc: Allocator) ![]DerivedReplayDebtStatus {
        return try lifecycle_impl.listDerivedReplayDebt(self, alloc);
    }

    pub fn promotionStageStats(self: *DB) types.ReplayStageStats {
        return lifecycle_impl.promotionStageStats(self);
    }

    pub fn graphMetricRuntimeStats(self: *DB) types.GraphMetricRuntimeStats {
        return lifecycle_impl.graphMetricRuntimeStats(self);
    }

    pub fn compactTextIndexes(self: *DB) !void {
        try ha_replication_impl.enforceDurableMutationGate(self);
        return try lifecycle_impl.compactTextIndexes(self);
    }

    pub fn drainScheduledTextMerges(self: *DB) !void {
        try ha_replication_impl.enforceDurableMutationGate(self);
        return try lifecycle_impl.drainScheduledTextMerges(self);
    }

    pub fn forceCompactTextIndexes(self: *DB) !void {
        try ha_replication_impl.enforceDurableMutationGate(self);
        return try lifecycle_impl.forceCompactTextIndexes(self);
    }

    pub fn bestEffortForceCompactTextIndexes(self: *DB) !void {
        try ha_replication_impl.enforceDurableMutationGate(self);
        return try lifecycle_impl.bestEffortForceCompactTextIndexes(self);
    }

    pub fn getEnrichment(self: *DB, alloc: Allocator, kind: types.EnrichmentKind, name: []const u8) !?types.EnrichmentConfig {
        return try schema_runtime_impl.getEnrichment(self, alloc, kind, name);
    }

    pub fn listEnrichments(self: *DB, alloc: Allocator) ![]types.EnrichmentConfig {
        return try schema_runtime_impl.listEnrichments(self, alloc);
    }

    pub fn listResolvers(self: *DB, alloc: Allocator) ![]index_manager_mod.ResolverConfig {
        return try lifecycle_impl.listResolvers(self, alloc);
    }

    pub fn extractEnrichments(self: *DB, alloc: Allocator, writes: []const types.BatchWrite) !types.ExtractEnrichmentsResult {
        return try write_path_impl.extractEnrichments(self, alloc, writes);
    }

    pub fn computeEnrichments(self: *DB, alloc: Allocator, writes: []const types.BatchWrite) !types.ComputeEnrichmentsResult {
        return try write_path_impl.computeEnrichments(self, alloc, writes);
    }

    pub fn prepareGeneratedEnrichments(
        self: *DB,
        req: types.BatchRequest,
        extracted: []const mapper.ExtractedWrite,
        precompute_mode: write_path.GeneratedPrecomputeMode,
        force_generated_artifact_names: []const []const u8,
    ) !write_path.PrecomputedGeneratedBatch {
        return try write_path_impl.prepareGeneratedEnrichments(self, req, extracted, precompute_mode, force_generated_artifact_names);
    }

    pub fn deleteIndex(self: *DB, name: []const u8) !bool {
        try ha_replication_impl.enforceDurableMutationGate(self);
        return try schema_runtime_impl.deleteIndex(self, name);
    }

    pub fn deleteEnrichment(self: *DB, kind: types.EnrichmentKind, name: []const u8) !bool {
        try ha_replication_impl.enforceDurableMutationGate(self);
        return try schema_runtime_impl.deleteEnrichment(self, kind, name);
    }

    pub fn pendingWorkStats(self: *DB) db_core.PendingWorkStats {
        return lifecycle_impl.pendingWorkStats(self);
    }

    pub fn runDerivedUntil(self: *DB, sequence: u64) !void {
        try ha_replication_impl.enforceDurableMutationGate(self);
        return try lifecycle_impl.runDerivedUntil(self, sequence);
    }

    pub fn runDerivedUntilTargets(self: *DB, sequence: u64, index_names: []const []const u8) !void {
        try ha_replication_impl.enforceDurableMutationGate(self);
        return try lifecycle_impl.runDerivedUntilTargets(self, sequence, index_names);
    }

    pub fn runEnrichmentUntil(self: *DB, sequence: u64) !void {
        try ha_replication_impl.enforceDurableMutationGate(self);
        return try lifecycle_impl.runEnrichmentUntil(self, sequence);
    }

    pub fn runMaintenanceUntil(self: *DB, sequence: u64, sync_targets: ManagedSyncTargets) !void {
        try ha_replication_impl.enforceDurableMutationGate(self);
        return try lifecycle_impl.runMaintenanceUntil(self, sequence, sync_targets);
    }

    pub fn runMaintenanceUntilTargets(self: *DB, sequence: u64, index_names: []const []const u8) !void {
        try ha_replication_impl.enforceDurableMutationGate(self);
        return try lifecycle_impl.runMaintenanceUntilTargets(self, sequence, index_names);
    }

    pub fn waitForCurrentSyncLevel(self: *DB, sync_level: types.SyncLevel) !void {
        return try lifecycle_impl.waitForCurrentSyncLevel(self, sync_level);
    }

    pub fn catchUpPendingDerivedReplay(self: *DB) !void {
        try ha_replication_impl.enforceDurableMutationGate(self);
        try derived_async_impl.replayPendingDerivedBatches(self, null, null);
    }

    pub fn catchUpPendingDerivedReplayWithProgress(
        self: *DB,
        progress_ctx: *anyopaque,
        progress_hook: ReplayProgressHook,
    ) !void {
        try ha_replication_impl.enforceDurableMutationGate(self);
        try derived_async_impl.replayPendingDerivedBatches(self, progress_ctx, progress_hook);
    }

    pub fn derivedAsyncCanAdvanceDerivedToTargetAsync(
        ctx_ptr: *anyopaque,
        index_ref: index_manager_mod.ManagedIndexRef,
        from_sequence: u64,
        target_sequence: u64,
    ) !bool {
        return try derived_async_impl.canAdvanceDerivedToTargetAsync(ctx_ptr, index_ref, from_sequence, target_sequence);
    }

    pub fn derivedAsyncAppendDerivedBatchRecord(self: *DB, derived_batch: derived_types.DerivedBatch) !u64 {
        try ha_replication_impl.enforceDurableMutationGate(self);
        return try derived_async_impl.appendDerivedBatchRecord(self, derived_batch);
    }

    pub fn derivedAsyncEncodeChangeRecordPayloadForContext(
        ctx: *const BatchExecutionContext,
        derived_batch: derived_types.DerivedBatch,
        sequence: u64,
    ) ![]u8 {
        return try derived_async_impl.encodeChangeRecordPayload(ctx, derived_batch, sequence);
    }

    pub fn derivedAsyncCollectManagedSyncTargets(
        self: *DB,
        alloc: Allocator,
        derived_batch: derived_types.DerivedBatch,
    ) !ManagedSyncTargets {
        return try derived_async_impl.collectManagedSyncTargets(alloc, self.core.index_manager, derived_batch);
    }

    pub fn derivedAsyncBatchAffectsManagedIndex(
        self: *DB,
        derived_batch: derived_types.DerivedBatch,
        index_ref: index_manager_mod.ManagedIndexRef,
    ) bool {
        return derived_async_impl.batchAffectsManagedIndex(self.core.index_manager, derived_batch, index_ref);
    }

    pub fn derivedAsyncBatchAffectsManagedIndexForReplay(
        self: *DB,
        derived_batch: derived_types.DerivedBatch,
        index_ref: index_manager_mod.ManagedIndexRef,
    ) !bool {
        return try derived_async_impl.batchAffectsManagedIndexForReplay(self.core.index_manager, derived_batch, index_ref);
    }

    pub fn derivedAsyncBatchAdvancesManagedIndexApplyStateForReplay(
        self: *DB,
        derived_batch: derived_types.DerivedBatch,
        index_ref: index_manager_mod.ManagedIndexRef,
    ) !bool {
        return try derived_async_impl.batchAdvancesManagedIndexApplyStateForReplay(self.core.index_manager, derived_batch, index_ref);
    }

    pub fn runUntilIdle(self: *DB) !void {
        try ha_replication_impl.enforceDurableMutationGate(self);
        return try lifecycle_impl.runUntilIdle(self);
    }

    pub fn runGraphMetricMaintenanceForIdle(self: *DB) !usize {
        try ha_replication_impl.enforceDurableMutationGate(self);
        return try search_runtime_impl.runGraphMetricMaintenanceForIdle(self);
    }

    pub fn runGraphMetricPlannedMaintenanceForIdle(
        self: *DB,
        options: index_manager_mod.IndexManager.GraphMetricPlannedMaintenanceOptions,
    ) !index_manager_mod.IndexManager.GraphMetricPlannedSchedulerSweepResult {
        try ha_replication_impl.enforceDurableMutationGate(self);
        return try search_runtime_impl.runGraphMetricPlannedMaintenanceForIdle(self, options);
    }

    pub fn runGraphMetricServiceMaintenanceJsonAlloc(self: *DB, alloc: Allocator, body: []const u8) ![]u8 {
        try ha_replication_impl.enforceDurableMutationGate(self);
        return try search_runtime_impl.runGraphMetricServiceMaintenanceJsonAlloc(self, alloc, body);
    }

    pub fn refreshGraphMetric(self: *DB, alloc: Allocator, index_name: []const u8, metric_name: []const u8) !types.GraphMetricStatus {
        try ha_replication_impl.enforceDurableMutationGate(self);
        return try search_runtime_impl.refreshGraphMetric(self, alloc, index_name, metric_name);
    }

    pub fn rebuildGraphMetric(self: *DB, alloc: Allocator, index_name: []const u8, metric_name: []const u8) !types.GraphMetricStatus {
        try ha_replication_impl.enforceDurableMutationGate(self);
        return try search_runtime_impl.rebuildGraphMetric(self, alloc, index_name, metric_name);
    }

    pub fn deleteGraphMetricMaterialization(self: *DB, alloc: Allocator, index_name: []const u8, metric_name: []const u8) !types.GraphMetricStatus {
        try ha_replication_impl.enforceDurableMutationGate(self);
        return try search_runtime_impl.deleteGraphMetricMaterialization(self, alloc, index_name, metric_name);
    }

    pub fn pauseGraphMetricMaintenance(self: *DB, alloc: Allocator, index_name: []const u8, metric_name: []const u8) !types.GraphMetricStatus {
        try ha_replication_impl.enforceDurableMutationGate(self);
        return try search_runtime_impl.pauseGraphMetricMaintenance(self, alloc, index_name, metric_name);
    }

    pub fn resumeGraphMetricMaintenance(self: *DB, alloc: Allocator, index_name: []const u8, metric_name: []const u8) !types.GraphMetricStatus {
        try ha_replication_impl.enforceDurableMutationGate(self);
        return try search_runtime_impl.resumeGraphMetricMaintenance(self, alloc, index_name, metric_name);
    }

    pub fn ensureGraphMetricPlannedBuild(
        self: *DB,
        alloc: Allocator,
        index_name: []const u8,
        metric_name: []const u8,
        target_generation: u64,
    ) !types.GraphMetricStatus {
        try ha_replication_impl.enforceDurableMutationGate(self);
        return try search_runtime_impl.ensureGraphMetricPlannedBuild(self, alloc, index_name, metric_name, target_generation);
    }

    pub fn runGraphMetricPlannedWorkerPageStep(
        self: *DB,
        index_name: []const u8,
        metric_name: []const u8,
        worker_id: []const u8,
    ) !search_runtime.GraphMetricBuildWorkerStepResult {
        try ha_replication_impl.enforceDurableMutationGate(self);
        return try search_runtime_impl.runGraphMetricPlannedWorkerPageStep(self, index_name, metric_name, worker_id);
    }

    pub fn runGraphMetricPlannedWorkerPageStepAt(
        self: *DB,
        index_name: []const u8,
        metric_name: []const u8,
        worker_id: []const u8,
        now_ms: u64,
    ) !search_runtime.GraphMetricBuildWorkerStepResult {
        try ha_replication_impl.enforceDurableMutationGate(self);
        return try search_runtime_impl.runGraphMetricPlannedWorkerPageStepAt(self, index_name, metric_name, worker_id, now_ms);
    }

    pub fn runGraphMetricPlannedCoordinatorStep(
        self: *DB,
        index_name: []const u8,
        metric_name: []const u8,
    ) !search_runtime.GraphMetricBuildWorkerStepResult {
        try ha_replication_impl.enforceDurableMutationGate(self);
        return try search_runtime_impl.runGraphMetricPlannedCoordinatorStep(self, index_name, metric_name);
    }

    pub fn runGraphMetricPlannedCoordinatorStepAt(
        self: *DB,
        index_name: []const u8,
        metric_name: []const u8,
        now_ms: u64,
    ) !search_runtime.GraphMetricBuildWorkerStepResult {
        try ha_replication_impl.enforceDurableMutationGate(self);
        return try search_runtime_impl.runGraphMetricPlannedCoordinatorStepAt(self, index_name, metric_name, now_ms);
    }

    pub fn failGraphMetricPlannedBuild(
        self: *DB,
        alloc: Allocator,
        index_name: []const u8,
        metric_name: []const u8,
        err: anyerror,
    ) !types.GraphMetricStatus {
        try ha_replication_impl.enforceDurableMutationGate(self);
        return try search_runtime_impl.failGraphMetricPlannedBuild(self, alloc, index_name, metric_name, err);
    }

    pub fn runGraphMetricPlannedDrain(
        self: *DB,
        alloc: Allocator,
        index_name: []const u8,
        metric_name: []const u8,
        target_generation: u64,
        options: search_runtime.GraphMetricPlannedDrainOptions,
    ) !types.GraphMetricStatus {
        try ha_replication_impl.enforceDurableMutationGate(self);
        return try search_runtime_impl.runGraphMetricPlannedDrain(self, alloc, index_name, metric_name, target_generation, options);
    }

    pub fn runGraphMetricPlannedCoordinatorSweep(
        self: *DB,
        options: index_manager_mod.IndexManager.GraphMetricPlannedSchedulerSweepOptions,
    ) !index_manager_mod.IndexManager.GraphMetricPlannedSchedulerSweepResult {
        try ha_replication_impl.enforceDurableMutationGate(self);
        return try search_runtime_impl.runGraphMetricPlannedCoordinatorSweep(self, options);
    }

    pub fn runGraphMetricPlannedWorkerSweep(
        self: *DB,
        options: index_manager_mod.IndexManager.GraphMetricPlannedWorkerSweepOptions,
    ) !index_manager_mod.IndexManager.GraphMetricPlannedSchedulerSweepResult {
        try ha_replication_impl.enforceDurableMutationGate(self);
        return try search_runtime_impl.runGraphMetricPlannedWorkerSweep(self, options);
    }

    pub const DenseArtifactRebuildTarget = derived_async.DenseArtifactRebuildTarget;

    pub fn rebuildDenseIndexesForTargetCoverage(self: *DB, alloc: Allocator) !usize {
        try ha_replication_impl.enforceDurableMutationGate(self);
        return try derived_async_impl.rebuildDenseIndexesForTargetCoverage(self, alloc);
    }

    pub fn rebuildSparseIndexesForTargetCoverage(self: *DB, alloc: Allocator) !usize {
        try ha_replication_impl.enforceDurableMutationGate(self);
        return try derived_async_impl.rebuildSparseIndexesForTargetCoverage(self, alloc);
    }

    pub fn rebuildGraphIndexesForTargetCoverage(self: *DB, alloc: Allocator) !void {
        try ha_replication_impl.enforceDurableMutationGate(self);
        return try split_restore_impl.rebuildGraphIndexesForTargetCoverage(self, alloc);
    }

    pub fn runDensePostingMaintenanceForIdle(self: *DB) !usize {
        try ha_replication_impl.enforceDurableMutationGate(self);
        return try derived_async_impl.runDensePostingMaintenanceForIdle(self);
    }

    pub fn runDensePostingMaintenanceForIdleBestEffort(self: *DB) !usize {
        try ha_replication_impl.enforceDurableMutationGate(self);
        return try derived_async_impl.runDensePostingMaintenanceForIdleBestEffort(self);
    }

    pub fn rebuildDenseIndexesFromStoredEmbeddingArtifacts(self: *DB, alloc: Allocator) !usize {
        try ha_replication_impl.enforceDurableMutationGate(self);
        return try derived_async_impl.rebuildDenseIndexesFromStoredEmbeddingArtifacts(self, alloc);
    }

    pub fn rebuildDenseIndexesFromStoredEmbeddingArtifactsIfNeeded(self: *DB, alloc: Allocator) !usize {
        try ha_replication_impl.enforceDurableMutationGate(self);
        return try derived_async_impl.rebuildDenseIndexesFromStoredEmbeddingArtifactsIfNeeded(self, alloc);
    }

    pub fn hasPendingDenseArtifactRebuild(self: *DB, alloc: Allocator) !bool {
        return try derived_async_impl.hasPendingDenseArtifactRebuild(self, alloc);
    }

    pub fn rebuildDenseIndexesFromStoredEmbeddingArtifactsIfNeededWithProgress(
        self: *DB,
        alloc: Allocator,
        progress_ctx: ?*anyopaque,
        progress_hook: ?ReplayProgressHook,
    ) !usize {
        try ha_replication_impl.enforceDurableMutationGate(self);
        return try derived_async_impl.rebuildDenseIndexesFromStoredEmbeddingArtifactsIfNeededWithProgress(self, alloc, progress_ctx, progress_hook);
    }

    pub fn derivedAsyncRebuildDenseIndexesFromStoredEmbeddingArtifactsResumeWithProgress(
        self: *DB,
        alloc: Allocator,
        resume_from: ?[]const u8,
        rebuild_targets: ?[]const DenseArtifactRebuildTarget,
        target_sequence_override: ?u64,
        progress_ctx: ?*anyopaque,
        progress_hook: ?ReplayProgressHook,
        resume_ctx: ?*anyopaque,
        resume_hook: ?derived_async.DenseArtifactRebuildResumeHook,
        rebuild_chunk_size: usize,
        rebuild_progress_interval: usize,
    ) !usize {
        try ha_replication_impl.enforceDurableMutationGate(self);
        return try derived_async_impl.rebuildDenseIndexesFromStoredEmbeddingArtifactsResumeWithProgress(
            self,
            alloc,
            resume_from,
            rebuild_targets,
            target_sequence_override,
            progress_ctx,
            progress_hook,
            resume_ctx,
            resume_hook,
            rebuild_chunk_size,
            rebuild_progress_interval,
        );
    }

    pub fn derivedAsyncDenseIndexRebuildStatePathAlloc(self: *DB, alloc: Allocator, index_name: []const u8) ![]u8 {
        return try derived_async_impl.denseIndexRebuildStatePathAlloc(self, alloc, index_name);
    }

    pub fn derivedAsyncFreeDenseArtifactRebuildWrites(
        alloc: Allocator,
        writes: *std.ArrayListUnmanaged(mapper.DenseEmbeddingWrite),
    ) void {
        derived_async_impl.freeDenseArtifactRebuildWrites(alloc, writes);
    }

    pub fn derivedAsyncApplyDerivedBatchToIndexReplay(
        ctx_ptr: *anyopaque,
        derived_batch: derived_types.DerivedBatch,
        index_ref: index_manager_mod.ManagedIndexRef,
    ) !bool {
        return try derived_async_impl.applyDerivedBatchToIndexReplay(ctx_ptr, derived_batch, index_ref);
    }

    pub fn replayGeneratedEnrichmentsFromStoredDocs(self: *DB, alloc: Allocator) !usize {
        try ha_replication_impl.enforceDurableMutationGate(self);
        return try schema_runtime_impl.replayGeneratedEnrichmentsFromStoredDocs(self, alloc);
    }

    pub fn overlayRuntimeStatusBestEffort(self: *DB, stats_alloc: Allocator, runtime_stats: *types.DBStats) void {
        return lifecycle_impl.overlayRuntimeStatusBestEffort(self, stats_alloc, runtime_stats);
    }

    pub fn overlayRuntimeStatusConsistent(self: *DB, stats_alloc: Allocator, runtime_stats: *types.DBStats) void {
        return lifecycle_impl.overlayRuntimeStatusConsistent(self, stats_alloc, runtime_stats);
    }

    pub fn stats(self: *DB, alloc: Allocator) !types.DBStats {
        return try lifecycle_impl.stats(self, alloc);
    }

    pub fn runtimeStatusStatsConsistent(self: *DB, alloc: Allocator) !types.DBStats {
        return try lifecycle_impl.runtimeStatusStatsConsistent(self, alloc);
    }

    pub fn tryRuntimeStatusStatsConsistent(self: *DB, alloc: Allocator) !?types.DBStats {
        return try lifecycle_impl.tryRuntimeStatusStatsConsistent(self, alloc);
    }

    pub fn reassignIdentityNamespaceForInternalTransition(self: *DB, namespace: doc_identity.Namespace) !void {
        try ha_replication_impl.enforceDurableMutationGate(self);
        return try lifecycle_impl.reassignIdentityNamespaceForInternalTransition(self, namespace);
    }

    pub fn currentIdentityReadGenerationForRequest(self: *DB, requested: ?u64) !u64 {
        return try lifecycle_impl.currentIdentityReadGenerationForRequest(self, requested);
    }

    pub fn relationalIntegrityRecordForeignKeyIntegrityReport(
        self: *DB,
        mode: relational_store_mod.ForeignKeyIntegrityMode,
        report: relational_store_mod.ForeignKeyIntegrityReport,
    ) void {
        relational_integrity_impl.recordForeignKeyIntegrityReport(self, mode, report);
    }

    pub fn relationalIntegrityRecordForeignKeyIntegrityProgressLocked(
        self: *DB,
        alloc: Allocator,
        mode: relational_store_mod.ForeignKeyIntegrityMode,
        constraint_name: ?[]const u8,
        lower_doc_key: []const u8,
        upper_doc_key: []const u8,
        report: relational_store_mod.ForeignKeyIntegrityReport,
    ) !void {
        return try relational_integrity_impl.recordForeignKeyIntegrityProgressLocked(self, alloc, mode, constraint_name, lower_doc_key, upper_doc_key, report);
    }

    pub fn appendForeignKeyExternalizedParentCheckIntents(
        self: *DB,
        txn_id: types.TxnId,
        intents: *std.ArrayListUnmanaged(transactions_mod.WriteIntent),
        owned_keys: *std.ArrayListUnmanaged([]u8),
        owned_values: *std.ArrayListUnmanaged([]u8),
        checks: []const types.ForeignKeyParentCheck,
    ) !void {
        return try relational_integrity_impl.appendForeignKeyExternalizedParentCheckIntents(self, txn_id, intents, owned_keys, owned_values, checks);
    }

    pub fn appendForeignKeyConstraintTimingOverrideIntents(
        self: *DB,
        txn_id: types.TxnId,
        intents: *std.ArrayListUnmanaged(transactions_mod.WriteIntent),
        owned_keys: *std.ArrayListUnmanaged([]u8),
        owned_values: *std.ArrayListUnmanaged([]u8),
        overrides: []const types.ForeignKeyConstraintTimingOverride,
    ) !void {
        return try relational_integrity_impl.appendForeignKeyConstraintTimingOverrideIntents(self, txn_id, intents, owned_keys, owned_values, overrides);
    }

    pub fn validateUniqueConstraintMutations(
        self: *DB,
        unique_writes: []const types.UniqueConstraintMutation,
        unique_deletes: []const types.UniqueConstraintMutation,
    ) !void {
        return try relational_integrity_impl.validateUniqueConstraintMutations(self, unique_writes, unique_deletes);
    }

    pub fn appendUniqueConstraintMutationIntents(
        self: *DB,
        intents: *std.ArrayListUnmanaged(transactions_mod.WriteIntent),
        owned_keys: *std.ArrayListUnmanaged([]u8),
        owned_values: *std.ArrayListUnmanaged([]u8),
        unique_writes: []const types.UniqueConstraintMutation,
        unique_deletes: []const types.UniqueConstraintMutation,
    ) !void {
        return try relational_integrity_impl.appendUniqueConstraintMutationIntents(self, intents, owned_keys, owned_values, unique_writes, unique_deletes);
    }

    pub fn appendForeignKeyRefMutationIntents(
        self: *DB,
        intents: *std.ArrayListUnmanaged(transactions_mod.WriteIntent),
        owned_keys: *std.ArrayListUnmanaged([]u8),
        ref_writes: []const types.ForeignKeyRefMutation,
        ref_deletes: []const types.ForeignKeyRefMutation,
    ) !void {
        return try relational_integrity_impl.appendForeignKeyRefMutationIntents(self, intents, owned_keys, ref_writes, ref_deletes);
    }

    pub fn applyForeignKeyParentDeleteActions(
        self: *DB,
        intents: *std.ArrayListUnmanaged(transactions_mod.WriteIntent),
        owned_keys: *std.ArrayListUnmanaged([]u8),
        owned_values: *std.ArrayListUnmanaged([]u8),
        checks: []const types.ForeignKeyParentDeleteCheck,
    ) !void {
        return try relational_integrity_impl.applyForeignKeyParentDeleteActions(self, intents, owned_keys, owned_values, checks);
    }

    pub fn applyForeignKeySetNullChildActions(
        self: *DB,
        intents: *std.ArrayListUnmanaged(transactions_mod.WriteIntent),
        owned_keys: *std.ArrayListUnmanaged([]u8),
        owned_values: *std.ArrayListUnmanaged([]u8),
        actions: []const types.ForeignKeySetNullChildAction,
    ) !void {
        return try relational_integrity_impl.applyForeignKeySetNullChildActions(self, intents, owned_keys, owned_values, actions);
    }

    pub fn applyForeignKeyCascadeChildActions(
        self: *DB,
        intents: *std.ArrayListUnmanaged(transactions_mod.WriteIntent),
        owned_keys: *std.ArrayListUnmanaged([]u8),
        owned_values: *std.ArrayListUnmanaged([]u8),
        actions: []const types.ForeignKeyCascadeChildAction,
    ) !void {
        return try relational_integrity_impl.applyForeignKeyCascadeChildActions(self, intents, owned_keys, owned_values, actions);
    }

    pub fn appendForeignKeyConflictIntents(
        self: *DB,
        intents: *std.ArrayListUnmanaged(transactions_mod.WriteIntent),
        owned_keys: *std.ArrayListUnmanaged([]u8),
        writes: []const types.TransactionWrite,
        parent_delete_checks: []const types.ForeignKeyParentDeleteCheck,
        conflict_checks: []const types.ForeignKeyConflictCheck,
        ref_writes: []const types.ForeignKeyRefMutation,
    ) !void {
        return try relational_integrity_impl.appendForeignKeyConflictIntents(self, intents, owned_keys, writes, parent_delete_checks, conflict_checks, ref_writes);
    }

    pub fn validateForeignKeyParentChecks(
        self: *DB,
        checks: []const types.ForeignKeyParentCheck,
        writes: []const types.TransactionWrite,
        deletes: []const []const u8,
        unique_writes: []const types.UniqueConstraintMutation,
        unique_deletes: []const types.UniqueConstraintMutation,
    ) !void {
        return try relational_integrity_impl.validateForeignKeyParentChecks(self, checks, writes, deletes, unique_writes, unique_deletes);
    }

    pub fn validateForeignKeyReferenceShapes(
        self: *DB,
        writes: []const types.TransactionWrite,
    ) !void {
        return try relational_integrity_impl.validateForeignKeyReferenceShapes(self, writes);
    }

    pub fn validateExternalizedForeignKeyParentChecks(
        self: *DB,
        checks: []const types.ForeignKeyParentCheck,
        constraint_timing_overrides: []const types.ForeignKeyConstraintTimingOverride,
        writes: []const types.TransactionWrite,
    ) !void {
        return try relational_integrity_impl.validateExternalizedForeignKeyParentChecks(self, checks, constraint_timing_overrides, writes);
    }

    pub fn validateForeignKeyConstraintTimingOverrides(
        self: *DB,
        overrides: []const types.ForeignKeyConstraintTimingOverride,
    ) !void {
        return try relational_integrity_impl.validateForeignKeyConstraintTimingOverrides(self, overrides);
    }

    pub fn validateForeignKeyParentDeleteChecks(
        self: *DB,
        checks: []const types.ForeignKeyParentDeleteCheck,
        constraint_timing_overrides: []const types.ForeignKeyConstraintTimingOverride,
        writes: []const types.TransactionWrite,
        deletes: []const []const u8,
        ref_writes: []const types.ForeignKeyRefMutation,
        ref_deletes: []const types.ForeignKeyRefMutation,
    ) !void {
        return try relational_integrity_impl.validateForeignKeyParentDeleteChecks(self, checks, constraint_timing_overrides, writes, deletes, ref_writes, ref_deletes);
    }

    pub fn validateForeignKeyRefMutations(self: *DB, mutations: []const types.ForeignKeyRefMutation) !void {
        return try relational_integrity_impl.validateForeignKeyRefMutations(self, mutations);
    }

    pub fn foreignKeyActionJobCanonicalAction(action: []const u8) ?[]const u8 {
        return relational_integrity_impl.foreignKeyActionJobCanonicalAction(action);
    }

    pub fn foreignKeyActionScheduleKeyAlloc(alloc: Allocator, schedule_id: []const u8) ![]u8 {
        return try relational_integrity_impl.foreignKeyActionScheduleKeyAlloc(alloc, schedule_id);
    }

    pub fn validateForeignKeyActionLineage(cascade_depth: u32, cascade_max_depth: u32) !void {
        return try relational_integrity_impl.validateForeignKeyActionLineage(cascade_depth, cascade_max_depth);
    }

    pub fn validateForeignKeyActionJobIdentity(
        job_id: []const u8,
        action: []const u8,
        worker_id: []const u8,
        constraint_name: []const u8,
        parent_table: []const u8,
        parent_key: []const u8,
        updated_parent_key: ?[]const u8,
    ) !void {
        return try relational_integrity_impl.validateForeignKeyActionJobIdentity(job_id, action, worker_id, constraint_name, parent_table, parent_key, updated_parent_key);
    }

    pub fn validateForeignKeyActionScheduleMatches(
        existing: ForeignKeyActionScheduleRecord,
        action_job_id: []const u8,
        action: []const u8,
        constraint_name: []const u8,
        parent_table: []const u8,
        parent_key: []const u8,
        updated_parent_key: ?[]const u8,
    ) !void {
        return try relational_integrity_impl.validateForeignKeyActionScheduleMatches(existing, action_job_id, action, constraint_name, parent_table, parent_key, updated_parent_key);
    }

    pub fn isForeignKeyActionScheduleMetadataKey(key: []const u8) bool {
        return relational_integrity.isForeignKeyActionScheduleMetadataKey(key);
    }

    pub const ForeignKeyIntegrityProgressRecord = relational_integrity.ForeignKeyIntegrityProgressRecord;
    pub const ForeignKeyIntegrityClaimRecord = relational_integrity.ForeignKeyIntegrityClaimRecord;
    pub const ForeignKeyIntegrityJobRecord = relational_integrity.ForeignKeyIntegrityJobRecord;
    pub const ForeignKeyActionJobRecord = relational_integrity.ForeignKeyActionJobRecord;
    pub const ForeignKeyActionScheduleRecord = relational_integrity.ForeignKeyActionScheduleRecord;
    pub const UniqueConstraintIntegrityProgressRecord = relational_integrity.UniqueConstraintIntegrityProgressRecord;

    pub fn freeForeignKeyIntegrityProgressRecord(self: *DB, record: ForeignKeyIntegrityProgressRecord) void {
        relational_integrity_impl.freeForeignKeyIntegrityProgressRecord(self, record);
    }

    pub fn freeForeignKeyIntegrityProgressRecords(self: *DB, records: []ForeignKeyIntegrityProgressRecord) void {
        relational_integrity_impl.freeForeignKeyIntegrityProgressRecords(self, records);
    }

    pub fn freeForeignKeyIntegrityClaimRecord(self: *DB, record: ForeignKeyIntegrityClaimRecord) void {
        relational_integrity_impl.freeForeignKeyIntegrityClaimRecord(self, record);
    }

    pub fn freeForeignKeyIntegrityClaimRecords(self: *DB, records: []ForeignKeyIntegrityClaimRecord) void {
        relational_integrity_impl.freeForeignKeyIntegrityClaimRecords(self, records);
    }

    pub fn freeForeignKeyIntegrityJobRecord(self: *DB, record: ForeignKeyIntegrityJobRecord) void {
        relational_integrity_impl.freeForeignKeyIntegrityJobRecord(self, record);
    }

    pub fn freeForeignKeyIntegrityJobRecords(self: *DB, records: []ForeignKeyIntegrityJobRecord) void {
        relational_integrity_impl.freeForeignKeyIntegrityJobRecords(self, records);
    }

    pub fn freeForeignKeyActionJobRecord(self: *DB, record: ForeignKeyActionJobRecord) void {
        relational_integrity_impl.freeForeignKeyActionJobRecord(self, record);
    }

    pub fn freeForeignKeyActionJobRecords(self: *DB, records: []ForeignKeyActionJobRecord) void {
        relational_integrity_impl.freeForeignKeyActionJobRecords(self, records);
    }

    pub fn freeForeignKeyActionScheduleRecord(self: *DB, record: ForeignKeyActionScheduleRecord) void {
        relational_integrity_impl.freeForeignKeyActionScheduleRecord(self, record);
    }

    pub fn freeForeignKeyActionScheduleRecords(self: *DB, records: []ForeignKeyActionScheduleRecord) void {
        relational_integrity_impl.freeForeignKeyActionScheduleRecords(self, records);
    }

    pub fn freeUniqueConstraintIntegrityProgressRecord(self: *DB, record: UniqueConstraintIntegrityProgressRecord) void {
        relational_integrity_impl.freeUniqueConstraintIntegrityProgressRecord(self, record);
    }

    pub fn freeUniqueConstraintIntegrityProgressRecords(self: *DB, records: []UniqueConstraintIntegrityProgressRecord) void {
        relational_integrity_impl.freeUniqueConstraintIntegrityProgressRecords(self, records);
    }

    pub fn listForeignKeyIntegrityProgressRecords(self: *DB) ![]ForeignKeyIntegrityProgressRecord {
        return try relational_integrity_impl.listForeignKeyIntegrityProgressRecords(self);
    }

    pub fn listForeignKeyIntegrityClaimRecords(self: *DB) ![]ForeignKeyIntegrityClaimRecord {
        return try relational_integrity_impl.listForeignKeyIntegrityClaimRecords(self);
    }

    pub fn listForeignKeyIntegrityJobRecords(self: *DB) ![]ForeignKeyIntegrityJobRecord {
        return try relational_integrity_impl.listForeignKeyIntegrityJobRecords(self);
    }

    pub fn loadForeignKeyActionJobRecord(self: *DB, job_id: []const u8) !?ForeignKeyActionJobRecord {
        return try relational_integrity_impl.loadForeignKeyActionJobRecord(self, job_id);
    }

    pub fn listForeignKeyActionJobRecords(self: *DB) ![]ForeignKeyActionJobRecord {
        return try relational_integrity_impl.listForeignKeyActionJobRecords(self);
    }

    pub fn loadForeignKeyActionScheduleRecord(self: *DB, schedule_id: []const u8) !?ForeignKeyActionScheduleRecord {
        return try relational_integrity_impl.loadForeignKeyActionScheduleRecord(self, schedule_id);
    }

    pub fn listForeignKeyActionScheduleRecords(self: *DB) ![]ForeignKeyActionScheduleRecord {
        return try relational_integrity_impl.listForeignKeyActionScheduleRecords(self);
    }

    pub fn listUniqueConstraintIntegrityProgressRecords(self: *DB) ![]UniqueConstraintIntegrityProgressRecord {
        return try relational_integrity_impl.listUniqueConstraintIntegrityProgressRecords(self);
    }

    pub fn loadForeignKeyIntegrityProgressRecord(
        self: *DB,
        mode: relational_store_mod.ForeignKeyIntegrityMode,
        constraint_name: ?[]const u8,
        lower_doc_key: []const u8,
        upper_doc_key: []const u8,
    ) !?ForeignKeyIntegrityProgressRecord {
        return try relational_integrity_impl.loadForeignKeyIntegrityProgressRecord(self, mode, constraint_name, lower_doc_key, upper_doc_key);
    }

    pub fn loadForeignKeyIntegrityProgressRecordForPhase(
        self: *DB,
        phase: []const u8,
        mode: relational_store_mod.ForeignKeyIntegrityMode,
        constraint_name: ?[]const u8,
        lower_doc_key: []const u8,
        upper_doc_key: []const u8,
    ) !?ForeignKeyIntegrityProgressRecord {
        return try relational_integrity_impl.loadForeignKeyIntegrityProgressRecordForPhase(self, phase, mode, constraint_name, lower_doc_key, upper_doc_key);
    }

    pub fn loadForeignKeyIntegrityClaimRecord(
        self: *DB,
        claim_key: []const u8,
    ) !?ForeignKeyIntegrityClaimRecord {
        return try relational_integrity_impl.loadForeignKeyIntegrityClaimRecord(self, claim_key);
    }

    pub fn loadForeignKeyIntegrityJobRecord(
        self: *DB,
        job_id: []const u8,
    ) !?ForeignKeyIntegrityJobRecord {
        return try relational_integrity_impl.loadForeignKeyIntegrityJobRecord(self, job_id);
    }

    pub fn loadUniqueConstraintIntegrityProgressRecord(
        self: *DB,
        mode: relational_store_mod.ForeignKeyIntegrityMode,
        lower_doc_key: []const u8,
        upper_doc_key: []const u8,
    ) !?UniqueConstraintIntegrityProgressRecord {
        return try relational_integrity_impl.loadUniqueConstraintIntegrityProgressRecord(self, mode, lower_doc_key, upper_doc_key);
    }

    pub fn claimForeignKeyIntegrityWorkUnit(
        self: *DB,
        claim_key: []const u8,
        worker_id: []const u8,
        group_id: u64,
        phase: []const u8,
        planned_action: []const u8,
        constraint_name: ?[]const u8,
        lower_doc_key: []const u8,
        upper_doc_key: []const u8,
        lease_ms: u64,
    ) !ForeignKeyIntegrityClaimRecord {
        try ha_replication_impl.enforceDurableMutationGate(self);
        return try relational_integrity_impl.claimForeignKeyIntegrityWorkUnit(self, claim_key, worker_id, group_id, phase, planned_action, constraint_name, lower_doc_key, upper_doc_key, lease_ms);
    }

    pub fn claimForeignKeyIntegrityWorkUnitAt(
        self: *DB,
        claim_key: []const u8,
        worker_id: []const u8,
        group_id: u64,
        phase: []const u8,
        planned_action: []const u8,
        constraint_name: ?[]const u8,
        lower_doc_key: []const u8,
        upper_doc_key: []const u8,
        lease_ms: u64,
        now_ns: u64,
    ) !ForeignKeyIntegrityClaimRecord {
        try ha_replication_impl.enforceDurableMutationGate(self);
        return try relational_integrity_impl.claimForeignKeyIntegrityWorkUnitAt(self, claim_key, worker_id, group_id, phase, planned_action, constraint_name, lower_doc_key, upper_doc_key, lease_ms, now_ns);
    }

    pub fn upsertForeignKeyIntegrityJobRecord(
        self: *DB,
        job_id: []const u8,
        table_name: []const u8,
        action: []const u8,
        worker_id: []const u8,
        constraint_name: ?[]const u8,
        lower_doc_key: []const u8,
        upper_doc_key: []const u8,
        lease_ms: u64,
        max_work_units: usize,
        status: []const u8,
    ) !ForeignKeyIntegrityJobRecord {
        try ha_replication_impl.enforceDurableMutationGate(self);
        return try relational_integrity_impl.upsertForeignKeyIntegrityJobRecord(self, job_id, table_name, action, worker_id, constraint_name, lower_doc_key, upper_doc_key, lease_ms, max_work_units, status);
    }

    pub fn upsertForeignKeyIntegrityJobRecordAt(
        self: *DB,
        job_id: []const u8,
        table_name: []const u8,
        action: []const u8,
        worker_id: []const u8,
        constraint_name: ?[]const u8,
        lower_doc_key: []const u8,
        upper_doc_key: []const u8,
        lease_ms: u64,
        max_work_units: usize,
        status: []const u8,
        now_ns: u64,
    ) !ForeignKeyIntegrityJobRecord {
        try ha_replication_impl.enforceDurableMutationGate(self);
        return try relational_integrity_impl.upsertForeignKeyIntegrityJobRecordAt(self, job_id, table_name, action, worker_id, constraint_name, lower_doc_key, upper_doc_key, lease_ms, max_work_units, status, now_ns);
    }

    pub fn completeForeignKeyIntegrityJobRecord(
        self: *DB,
        job_id: []const u8,
        status: []const u8,
        valid: bool,
        report: relational_store_mod.ForeignKeyIntegrityReport,
    ) !ForeignKeyIntegrityJobRecord {
        try ha_replication_impl.enforceDurableMutationGate(self);
        return try relational_integrity_impl.completeForeignKeyIntegrityJobRecord(self, job_id, status, valid, report);
    }

    pub fn completeForeignKeyIntegrityJobRecordWithDiagnostics(
        self: *DB,
        job_id: []const u8,
        status: []const u8,
        valid: bool,
        report: relational_store_mod.ForeignKeyIntegrityReport,
        violation_samples_json: []const u8,
        violation_sample_count: usize,
        violations_truncated: bool,
    ) !ForeignKeyIntegrityJobRecord {
        try ha_replication_impl.enforceDurableMutationGate(self);
        return try relational_integrity_impl.completeForeignKeyIntegrityJobRecordWithDiagnostics(self, job_id, status, valid, report, violation_samples_json, violation_sample_count, violations_truncated);
    }

    pub fn completeForeignKeyIntegrityJobRecordAt(
        self: *DB,
        job_id: []const u8,
        status: []const u8,
        valid: bool,
        report: relational_store_mod.ForeignKeyIntegrityReport,
        now_ns: u64,
    ) !ForeignKeyIntegrityJobRecord {
        try ha_replication_impl.enforceDurableMutationGate(self);
        return try relational_integrity_impl.completeForeignKeyIntegrityJobRecordAt(self, job_id, status, valid, report, now_ns);
    }

    pub fn completeForeignKeyIntegrityJobRecordWithDiagnosticsAt(
        self: *DB,
        job_id: []const u8,
        status: []const u8,
        valid: bool,
        report: relational_store_mod.ForeignKeyIntegrityReport,
        violation_samples_json: ?[]const u8,
        violation_sample_count: ?usize,
        violations_truncated: ?bool,
        now_ns: u64,
    ) !ForeignKeyIntegrityJobRecord {
        try ha_replication_impl.enforceDurableMutationGate(self);
        return try relational_integrity_impl.completeForeignKeyIntegrityJobRecordWithDiagnosticsAt(self, job_id, status, valid, report, violation_samples_json, violation_sample_count, violations_truncated, now_ns);
    }

    pub fn updateForeignKeyIntegrityJobDiagnostics(
        self: *DB,
        job_id: []const u8,
        violation_samples_json: []const u8,
        violation_sample_count: usize,
        violations_truncated: bool,
    ) !ForeignKeyIntegrityJobRecord {
        try ha_replication_impl.enforceDurableMutationGate(self);
        return try relational_integrity_impl.updateForeignKeyIntegrityJobDiagnostics(self, job_id, violation_samples_json, violation_sample_count, violations_truncated);
    }

    pub fn updateForeignKeyIntegrityJobDiagnosticsWithReport(
        self: *DB,
        job_id: []const u8,
        report: relational_store_mod.ForeignKeyIntegrityReport,
        violation_samples_json: []const u8,
        violation_sample_count: usize,
        violations_truncated: bool,
    ) !ForeignKeyIntegrityJobRecord {
        try ha_replication_impl.enforceDurableMutationGate(self);
        return try relational_integrity_impl.updateForeignKeyIntegrityJobDiagnosticsWithReport(self, job_id, report, violation_samples_json, violation_sample_count, violations_truncated);
    }

    pub fn updateForeignKeyIntegrityJobDiagnosticsAt(
        self: *DB,
        job_id: []const u8,
        violation_samples_json: []const u8,
        violation_sample_count: usize,
        violations_truncated: bool,
        now_ns: u64,
    ) !ForeignKeyIntegrityJobRecord {
        try ha_replication_impl.enforceDurableMutationGate(self);
        return try relational_integrity_impl.updateForeignKeyIntegrityJobDiagnosticsAt(self, job_id, violation_samples_json, violation_sample_count, violations_truncated, now_ns);
    }

    pub fn updateForeignKeyIntegrityJobDiagnosticsWithReportAt(
        self: *DB,
        job_id: []const u8,
        report: relational_store_mod.ForeignKeyIntegrityReport,
        violation_samples_json: []const u8,
        violation_sample_count: usize,
        violations_truncated: bool,
        now_ns: u64,
    ) !ForeignKeyIntegrityJobRecord {
        try ha_replication_impl.enforceDurableMutationGate(self);
        return try relational_integrity_impl.updateForeignKeyIntegrityJobDiagnosticsWithReportAt(self, job_id, report, violation_samples_json, violation_sample_count, violations_truncated, now_ns);
    }

    pub fn claimAndRunForeignKeyActionJobPage(self: *DB, job_id: []const u8, action: []const u8, worker_id: []const u8, constraint_name: []const u8, parent_table: []const u8, parent_key: []const u8, page_limit: usize, lease_ms: u64) !ForeignKeyActionJobRecord {
        try ha_replication_impl.enforceDurableMutationGate(self);
        return try relational_integrity_impl.claimAndRunForeignKeyActionJobPage(self, job_id, action, worker_id, constraint_name, parent_table, parent_key, page_limit, lease_ms);
    }

    pub fn scheduleForeignKeyActionJob(self: *DB, job_id: []const u8, action: []const u8, worker_id: []const u8, constraint_name: []const u8, parent_table: []const u8, parent_key: []const u8, page_limit: usize) !ForeignKeyActionJobRecord {
        try ha_replication_impl.enforceDurableMutationGate(self);
        return try relational_integrity_impl.scheduleForeignKeyActionJob(self, job_id, action, worker_id, constraint_name, parent_table, parent_key, page_limit);
    }

    pub fn scheduleForeignKeyActionJobWithUpdatedParentKey(self: *DB, job_id: []const u8, action: []const u8, worker_id: []const u8, constraint_name: []const u8, parent_table: []const u8, parent_key: []const u8, updated_parent_key: ?[]const u8, page_limit: usize) !ForeignKeyActionJobRecord {
        try ha_replication_impl.enforceDurableMutationGate(self);
        return try relational_integrity_impl.scheduleForeignKeyActionJobWithUpdatedParentKey(self, job_id, action, worker_id, constraint_name, parent_table, parent_key, updated_parent_key, page_limit);
    }

    pub fn scheduleForeignKeyActionJobAt(self: *DB, job_id: []const u8, action: []const u8, worker_id: []const u8, constraint_name: []const u8, parent_table: []const u8, parent_key: []const u8, page_limit: usize, now_ns: u64) !ForeignKeyActionJobRecord {
        try ha_replication_impl.enforceDurableMutationGate(self);
        return try relational_integrity_impl.scheduleForeignKeyActionJobAt(self, job_id, action, worker_id, constraint_name, parent_table, parent_key, page_limit, now_ns);
    }

    pub fn scheduleForeignKeyActionJobWithUpdatedParentKeyAt(self: *DB, job_id: []const u8, action: []const u8, worker_id: []const u8, constraint_name: []const u8, parent_table: []const u8, parent_key: []const u8, updated_parent_key: ?[]const u8, page_limit: usize, now_ns: u64) !ForeignKeyActionJobRecord {
        try ha_replication_impl.enforceDurableMutationGate(self);
        return try relational_integrity_impl.scheduleForeignKeyActionJobWithUpdatedParentKeyAt(self, job_id, action, worker_id, constraint_name, parent_table, parent_key, updated_parent_key, page_limit, now_ns);
    }

    pub fn scheduleForeignKeyActionJobWithUpdatedParentKeyAndCascadeLineageAt(self: *DB, job_id: []const u8, action: []const u8, worker_id: []const u8, constraint_name: []const u8, parent_table: []const u8, parent_key: []const u8, updated_parent_key: ?[]const u8, page_limit: usize, cascade_depth: u32, cascade_max_depth: u32, now_ns: u64) !ForeignKeyActionJobRecord {
        try ha_replication_impl.enforceDurableMutationGate(self);
        return try relational_integrity_impl.scheduleForeignKeyActionJobWithUpdatedParentKeyAndCascadeLineageAt(self, job_id, action, worker_id, constraint_name, parent_table, parent_key, updated_parent_key, page_limit, cascade_depth, cascade_max_depth, now_ns);
    }

    pub fn requeueForeignKeyActionJob(self: *DB, job_id: []const u8, action: []const u8, worker_id: []const u8, constraint_name: []const u8, parent_table: []const u8, parent_key: []const u8, page_limit: usize) !ForeignKeyActionJobRecord {
        try ha_replication_impl.enforceDurableMutationGate(self);
        return try relational_integrity_impl.requeueForeignKeyActionJob(self, job_id, action, worker_id, constraint_name, parent_table, parent_key, page_limit);
    }

    pub fn requeueForeignKeyActionJobWithUpdatedParentKey(self: *DB, job_id: []const u8, action: []const u8, worker_id: []const u8, constraint_name: []const u8, parent_table: []const u8, parent_key: []const u8, updated_parent_key: ?[]const u8, page_limit: usize) !ForeignKeyActionJobRecord {
        try ha_replication_impl.enforceDurableMutationGate(self);
        return try relational_integrity_impl.requeueForeignKeyActionJobWithUpdatedParentKey(self, job_id, action, worker_id, constraint_name, parent_table, parent_key, updated_parent_key, page_limit);
    }

    pub fn requeueForeignKeyActionJobAt(self: *DB, job_id: []const u8, action: []const u8, worker_id: []const u8, constraint_name: []const u8, parent_table: []const u8, parent_key: []const u8, page_limit: usize, now_ns: u64) !ForeignKeyActionJobRecord {
        try ha_replication_impl.enforceDurableMutationGate(self);
        return try relational_integrity_impl.requeueForeignKeyActionJobAt(self, job_id, action, worker_id, constraint_name, parent_table, parent_key, page_limit, now_ns);
    }

    pub fn requeueForeignKeyActionJobWithUpdatedParentKeyAt(self: *DB, job_id: []const u8, action: []const u8, worker_id: []const u8, constraint_name: []const u8, parent_table: []const u8, parent_key: []const u8, updated_parent_key: ?[]const u8, page_limit: usize, now_ns: u64) !ForeignKeyActionJobRecord {
        try ha_replication_impl.enforceDurableMutationGate(self);
        return try relational_integrity_impl.requeueForeignKeyActionJobWithUpdatedParentKeyAt(self, job_id, action, worker_id, constraint_name, parent_table, parent_key, updated_parent_key, page_limit, now_ns);
    }

    pub fn scheduleForeignKeyActionSchedule(self: *DB, schedule_id: []const u8, action_job_id: []const u8, action: []const u8, worker_id: []const u8, constraint_name: []const u8, parent_table: []const u8, parent_key: []const u8, page_limit: usize) !ForeignKeyActionScheduleRecord {
        try ha_replication_impl.enforceDurableMutationGate(self);
        return try relational_integrity_impl.scheduleForeignKeyActionSchedule(self, schedule_id, action_job_id, action, worker_id, constraint_name, parent_table, parent_key, page_limit);
    }

    pub fn scheduleForeignKeyActionScheduleWithUpdatedParentKey(self: *DB, schedule_id: []const u8, action_job_id: []const u8, action: []const u8, worker_id: []const u8, constraint_name: []const u8, parent_table: []const u8, parent_key: []const u8, updated_parent_key: ?[]const u8, page_limit: usize) !ForeignKeyActionScheduleRecord {
        try ha_replication_impl.enforceDurableMutationGate(self);
        return try relational_integrity_impl.scheduleForeignKeyActionScheduleWithUpdatedParentKey(self, schedule_id, action_job_id, action, worker_id, constraint_name, parent_table, parent_key, updated_parent_key, page_limit);
    }

    pub fn scheduleForeignKeyActionScheduleAt(self: *DB, schedule_id: []const u8, action_job_id: []const u8, action: []const u8, worker_id: []const u8, constraint_name: []const u8, parent_table: []const u8, parent_key: []const u8, page_limit: usize, now_ns: u64) !ForeignKeyActionScheduleRecord {
        try ha_replication_impl.enforceDurableMutationGate(self);
        return try relational_integrity_impl.scheduleForeignKeyActionScheduleAt(self, schedule_id, action_job_id, action, worker_id, constraint_name, parent_table, parent_key, page_limit, now_ns);
    }

    pub fn scheduleForeignKeyActionScheduleWithUpdatedParentKeyAt(self: *DB, schedule_id: []const u8, action_job_id: []const u8, action: []const u8, worker_id: []const u8, constraint_name: []const u8, parent_table: []const u8, parent_key: []const u8, updated_parent_key: ?[]const u8, page_limit: usize, now_ns: u64) !ForeignKeyActionScheduleRecord {
        try ha_replication_impl.enforceDurableMutationGate(self);
        return try relational_integrity_impl.scheduleForeignKeyActionScheduleWithUpdatedParentKeyAt(self, schedule_id, action_job_id, action, worker_id, constraint_name, parent_table, parent_key, updated_parent_key, page_limit, now_ns);
    }

    pub fn requeueForeignKeyActionSchedule(self: *DB, schedule_id: []const u8, action_job_id: []const u8, action: []const u8, worker_id: []const u8, constraint_name: []const u8, parent_table: []const u8, parent_key: []const u8, page_limit: usize) !ForeignKeyActionScheduleRecord {
        try ha_replication_impl.enforceDurableMutationGate(self);
        return try relational_integrity_impl.requeueForeignKeyActionSchedule(self, schedule_id, action_job_id, action, worker_id, constraint_name, parent_table, parent_key, page_limit);
    }

    pub fn requeueForeignKeyActionScheduleAt(self: *DB, schedule_id: []const u8, action_job_id: []const u8, action: []const u8, worker_id: []const u8, constraint_name: []const u8, parent_table: []const u8, parent_key: []const u8, page_limit: usize, now_ns: u64) !ForeignKeyActionScheduleRecord {
        try ha_replication_impl.enforceDurableMutationGate(self);
        return try relational_integrity_impl.requeueForeignKeyActionScheduleAt(self, schedule_id, action_job_id, action, worker_id, constraint_name, parent_table, parent_key, page_limit, now_ns);
    }

    pub fn requeueForeignKeyActionScheduleWithUpdatedParentKeyAt(self: *DB, schedule_id: []const u8, action_job_id: []const u8, action: []const u8, worker_id: []const u8, constraint_name: []const u8, parent_table: []const u8, parent_key: []const u8, updated_parent_key: ?[]const u8, page_limit: usize, now_ns: u64) !ForeignKeyActionScheduleRecord {
        try ha_replication_impl.enforceDurableMutationGate(self);
        return try relational_integrity_impl.requeueForeignKeyActionScheduleWithUpdatedParentKeyAt(self, schedule_id, action_job_id, action, worker_id, constraint_name, parent_table, parent_key, updated_parent_key, page_limit, now_ns);
    }

    pub fn markForeignKeyActionScheduleSeeded(self: *DB, schedule_id: []const u8, scheduled_groups: u64) !ForeignKeyActionScheduleRecord {
        try ha_replication_impl.enforceDurableMutationGate(self);
        return try relational_integrity_impl.markForeignKeyActionScheduleSeeded(self, schedule_id, scheduled_groups);
    }

    pub fn markForeignKeyActionScheduleSeededAt(self: *DB, schedule_id: []const u8, scheduled_groups: u64, now_ns: u64) !ForeignKeyActionScheduleRecord {
        try ha_replication_impl.enforceDurableMutationGate(self);
        return try relational_integrity_impl.markForeignKeyActionScheduleSeededAt(self, schedule_id, scheduled_groups, now_ns);
    }

    pub fn claimAndRunForeignKeyActionJobPageAt(self: *DB, job_id: []const u8, action: []const u8, worker_id: []const u8, constraint_name: []const u8, parent_table: []const u8, parent_key: []const u8, page_limit: usize, lease_ms: u64, now_ns: u64) !ForeignKeyActionJobRecord {
        try ha_replication_impl.enforceDurableMutationGate(self);
        return try relational_integrity_impl.claimAndRunForeignKeyActionJobPageAt(self, job_id, action, worker_id, constraint_name, parent_table, parent_key, page_limit, lease_ms, now_ns);
    }

    pub fn claimAndRunForeignKeyActionJobPageWithUpdatedParentKeyAt(self: *DB, job_id: []const u8, action: []const u8, worker_id: []const u8, constraint_name: []const u8, parent_table: []const u8, parent_key: []const u8, updated_parent_key: ?[]const u8, page_limit: usize, lease_ms: u64, now_ns: u64) !ForeignKeyActionJobRecord {
        try ha_replication_impl.enforceDurableMutationGate(self);
        return try relational_integrity_impl.claimAndRunForeignKeyActionJobPageWithUpdatedParentKeyAt(self, job_id, action, worker_id, constraint_name, parent_table, parent_key, updated_parent_key, page_limit, lease_ms, now_ns);
    }

    pub fn claimAndRunForeignKeyActionJobPageWithUpdatedParentKeyAndCascadeLineageAt(self: *DB, job_id: []const u8, action: []const u8, worker_id: []const u8, constraint_name: []const u8, parent_table: []const u8, parent_key: []const u8, updated_parent_key: ?[]const u8, page_limit: usize, lease_ms: u64, cascade_depth: u32, cascade_max_depth: u32, now_ns: u64) !ForeignKeyActionJobRecord {
        try ha_replication_impl.enforceDurableMutationGate(self);
        return try relational_integrity_impl.claimAndRunForeignKeyActionJobPageWithUpdatedParentKeyAndCascadeLineageAt(self, job_id, action, worker_id, constraint_name, parent_table, parent_key, updated_parent_key, page_limit, lease_ms, cascade_depth, cascade_max_depth, now_ns);
    }

    pub fn claimForeignKeyActionJobPage(self: *DB, job_id: []const u8, action: []const u8, worker_id: []const u8, constraint_name: []const u8, parent_table: []const u8, parent_key: []const u8, page_limit: usize, lease_ms: u64) !ForeignKeyActionJobRecord {
        try ha_replication_impl.enforceDurableMutationGate(self);
        return try relational_integrity_impl.claimForeignKeyActionJobPage(self, job_id, action, worker_id, constraint_name, parent_table, parent_key, page_limit, lease_ms);
    }

    pub fn claimForeignKeyActionJobPageAt(self: *DB, job_id: []const u8, action: []const u8, worker_id: []const u8, constraint_name: []const u8, parent_table: []const u8, parent_key: []const u8, page_limit: usize, lease_ms: u64, now_ns: u64) !ForeignKeyActionJobRecord {
        try ha_replication_impl.enforceDurableMutationGate(self);
        return try relational_integrity_impl.claimForeignKeyActionJobPageAt(self, job_id, action, worker_id, constraint_name, parent_table, parent_key, page_limit, lease_ms, now_ns);
    }

    pub fn claimForeignKeyActionJobPageWithUpdatedParentKeyAt(self: *DB, job_id: []const u8, action: []const u8, worker_id: []const u8, constraint_name: []const u8, parent_table: []const u8, parent_key: []const u8, updated_parent_key: ?[]const u8, page_limit: usize, lease_ms: u64, now_ns: u64) !ForeignKeyActionJobRecord {
        try ha_replication_impl.enforceDurableMutationGate(self);
        return try relational_integrity_impl.claimForeignKeyActionJobPageWithUpdatedParentKeyAt(self, job_id, action, worker_id, constraint_name, parent_table, parent_key, updated_parent_key, page_limit, lease_ms, now_ns);
    }

    pub fn claimForeignKeyActionJobPageWithUpdatedParentKeyAndCascadeLineageAt(self: *DB, job_id: []const u8, action: []const u8, worker_id: []const u8, constraint_name: []const u8, parent_table: []const u8, parent_key: []const u8, updated_parent_key: ?[]const u8, page_limit: usize, lease_ms: u64, cascade_depth: u32, cascade_max_depth: u32, now_ns: u64) !ForeignKeyActionJobRecord {
        try ha_replication_impl.enforceDurableMutationGate(self);
        return try relational_integrity_impl.claimForeignKeyActionJobPageWithUpdatedParentKeyAndCascadeLineageAt(self, job_id, action, worker_id, constraint_name, parent_table, parent_key, updated_parent_key, page_limit, lease_ms, cascade_depth, cascade_max_depth, now_ns);
    }

    pub fn finishClaimedForeignKeyActionJobPage(self: *DB, claimed: ForeignKeyActionJobRecord, applied_count: usize, complete: bool, next_child_table: ?[]const u8, next_child_key: ?[]const u8, last_error: ?[]const u8) !ForeignKeyActionJobRecord {
        try ha_replication_impl.enforceDurableMutationGate(self);
        return try relational_integrity_impl.finishClaimedForeignKeyActionJobPage(self, claimed, applied_count, complete, next_child_table, next_child_key, last_error);
    }

    pub fn finishClaimedForeignKeyActionJobPageAt(self: *DB, claimed: ForeignKeyActionJobRecord, applied_count: usize, complete: bool, next_child_table: ?[]const u8, next_child_key: ?[]const u8, last_error: ?[]const u8, now_ns: u64) !ForeignKeyActionJobRecord {
        try ha_replication_impl.enforceDurableMutationGate(self);
        return try relational_integrity_impl.finishClaimedForeignKeyActionJobPageAt(self, claimed, applied_count, complete, next_child_table, next_child_key, last_error, now_ns);
    }

    pub fn claimAndRunForeignKeyIntegrityWorkUnit(self: *DB, claim_key: []const u8, worker_id: []const u8, group_id: u64, phase: []const u8, mode: relational_store_mod.ForeignKeyIntegrityMode, constraint_name: ?[]const u8, lower_doc_key: []const u8, upper_doc_key: []const u8, lease_ms: u64) !ForeignKeyIntegrityReport {
        try ha_replication_impl.enforceDurableMutationGate(self);
        return try relational_integrity_impl.claimAndRunForeignKeyIntegrityWorkUnit(self, claim_key, worker_id, group_id, phase, mode, constraint_name, lower_doc_key, upper_doc_key, lease_ms);
    }

    pub fn claimAndRunForeignKeyIntegrityWorkUnitAt(self: *DB, claim_key: []const u8, worker_id: []const u8, group_id: u64, phase: []const u8, mode: relational_store_mod.ForeignKeyIntegrityMode, constraint_name: ?[]const u8, lower_doc_key: []const u8, upper_doc_key: []const u8, lease_ms: u64, now_ns: u64) !ForeignKeyIntegrityReport {
        try ha_replication_impl.enforceDurableMutationGate(self);
        return try relational_integrity_impl.claimAndRunForeignKeyIntegrityWorkUnitAt(self, claim_key, worker_id, group_id, phase, mode, constraint_name, lower_doc_key, upper_doc_key, lease_ms, now_ns);
    }

    pub fn cloneForeignKeyActionScheduleRecordFromJson(self: *DB, raw: []const u8) !ForeignKeyActionScheduleRecord {
        return try relational_integrity_impl.cloneForeignKeyActionScheduleRecordFromJson(self, raw);
    }

    pub fn cloneForeignKeyActionJobRecordFromJson(self: *DB, raw: []const u8) !ForeignKeyActionJobRecord {
        return try relational_integrity_impl.cloneForeignKeyActionJobRecordFromJson(self, raw);
    }

    pub fn diagnosticStats(self: *DB, alloc: Allocator) !types.DBStats {
        return try lifecycle_impl.diagnosticStats(self, alloc);
    }

    pub fn primaryDocCount(self: *DB, alloc: Allocator) !u64 {
        _ = alloc;
        return try lifecycle_impl.primaryDocCount(self);
    }

    pub fn scan(self: *DB, alloc: Allocator, from_key: []const u8, to_key: []const u8, opts: types.ScanOptions) !types.ScanResult {
        return try search_runtime_impl.scan(self, alloc, from_key, to_key, opts);
    }

    pub fn queryRelationalRows(
        self: *DB,
        alloc: Allocator,
        runtime_schema: schema_mod.TableSchema,
        req: types.RelationalRowsQueryRequest,
    ) !types.RelationalRowsQueryResult {
        return try relational_rows_impl.queryRelationalRows(self, alloc, runtime_schema, req);
    }

    pub fn collectRelationalRowsPreimagesAlloc(
        self: *DB,
        alloc: Allocator,
        runtime_schema: schema_mod.TableSchema,
        req: types.RelationalRowsQueryRequest,
    ) ![]const types.RelationalRowsCollectedRow {
        return try relational_rows_impl.collectRelationalRowsPreimagesAlloc(self, alloc, runtime_schema, req);
    }

    pub fn collectRelationalRowsPreimagesAcrossRangesAlloc(
        self: *DB,
        alloc: Allocator,
        runtime_schema: schema_mod.TableSchema,
        req: types.RelationalRowsQueryRequest,
        ranges: []const types.RelationalRowsDocKeyRange,
    ) ![]const types.RelationalRowsCollectedRow {
        return try relational_rows_impl.collectRelationalRowsPreimagesAcrossRangesAlloc(self, alloc, runtime_schema, req, ranges);
    }

    pub fn queryRelationalRowsPlan(
        self: *DB,
        alloc: Allocator,
        runtime_schema: schema_mod.TableSchema,
        plan: types.RelationalRowsQueryPlan,
    ) !types.RelationalRowsQueryResult {
        return try relational_rows_impl.queryRelationalRowsPlan(self, alloc, runtime_schema, plan);
    }

    pub fn queryRelationalRowsSetOperationPlan(
        self: *DB,
        alloc: Allocator,
        runtime_schema: schema_mod.TableSchema,
        plan: types.RelationalRowsSetOperationPlan,
    ) !types.RelationalRowsQueryResult {
        return try relational_rows_impl.queryRelationalRowsSetOperationPlan(self, alloc, runtime_schema, plan);
    }

    pub fn mutateRelationalRowsFromSource(
        self: *DB,
        alloc: Allocator,
        runtime_schema: schema_mod.TableSchema,
        req: types.RelationalRowsMutationSourceRequest,
    ) !types.RelationalRowsMutationSourceResult {
        try ha_replication_impl.enforceDurableMutationGate(self);
        return try relational_rows_impl.mutateRelationalRowsFromSource(self, alloc, runtime_schema, req);
    }

    pub fn validateRelationalRowsInsertSourceRequest(
        runtime_schema: schema_mod.TableSchema,
        req: types.RelationalRowsInsertSourceRequest,
    ) !void {
        return relational_rows.validateInsertSourceRequest(runtime_schema, req);
    }

    pub fn validateRelationalRowsInsertSourceRequestWithSchemas(
        target_schema: schema_mod.TableSchema,
        source_schema: schema_mod.TableSchema,
        req: types.RelationalRowsInsertSourceRequest,
    ) !void {
        return relational_rows.validateInsertSourceRequestWithSchemas(target_schema, source_schema, req);
    }

    pub fn planRelationalRowsMutationSourceAlloc(
        self: *DB,
        alloc: Allocator,
        runtime_schema: schema_mod.TableSchema,
        req: types.RelationalRowsMutationSourceRequest,
    ) !RelationalRowsMutationSourcePlan {
        return try relational_rows_impl.planRelationalRowsMutationSourceAlloc(self, alloc, runtime_schema, req);
    }

    pub fn planRelationalRowsMutationSourceAcrossRangesAlloc(
        self: *DB,
        alloc: Allocator,
        runtime_schema: schema_mod.TableSchema,
        req: types.RelationalRowsMutationSourceRequest,
        ranges: []const types.RelationalRowsDocKeyRange,
    ) !RelationalRowsMutationSourcePlan {
        return try relational_rows_impl.planRelationalRowsMutationSourceAcrossRangesAlloc(self, alloc, runtime_schema, req, ranges);
    }

    pub fn collectRelationalRowsMutationSourceCandidatesAlloc(
        self: *DB,
        alloc: Allocator,
        runtime_schema: schema_mod.TableSchema,
        req: types.RelationalRowsMutationSourceRequest,
        doc_key_range: ?types.RelationalRowsDocKeyRange,
    ) ![]RelationalRowsMutationSourceCandidate {
        return try relational_rows_impl.collectRelationalRowsMutationSourceCandidatesAlloc(self, alloc, runtime_schema, req, doc_key_range);
    }

    pub fn collectRelationalRowsMutationSourceCandidatesAcrossRangesAlloc(
        self: *DB,
        alloc: Allocator,
        runtime_schema: schema_mod.TableSchema,
        req: types.RelationalRowsMutationSourceRequest,
        ranges: []const types.RelationalRowsDocKeyRange,
    ) ![]RelationalRowsMutationSourceCandidate {
        return try relational_rows_impl.collectRelationalRowsMutationSourceCandidatesAcrossRangesAlloc(self, alloc, runtime_schema, req, ranges);
    }

    pub fn selectPlannedRelationalRowsMutationSourceCandidatesAlloc(
        alloc: Allocator,
        req: types.RelationalRowsMutationSourceRequest,
        candidates: *[]RelationalRowsMutationSourceCandidate,
    ) !RelationalRowsMutationSourcePlan {
        return try relational_rows_impl.selectPlannedRelationalRowsMutationSourceCandidatesAlloc(alloc, req, candidates);
    }

    pub fn stagePlannedRelationalRowsMutationSourceAlloc(
        self: *DB,
        alloc: Allocator,
        runtime_schema: schema_mod.TableSchema,
        req: types.RelationalRowsMutationSourceRequest,
        matched: u32,
        candidates: []const RelationalRowsMutationSourceCandidate,
    ) !types.RelationalRowsMutationSourceResult {
        try ha_replication_impl.enforceDurableMutationGate(self);
        return try relational_rows_impl.stagePlannedRelationalRowsMutationSourceAlloc(self, alloc, runtime_schema, req, matched, candidates);
    }

    pub fn mutateRelationalRowsJoinedSourceAlloc(
        self: *DB,
        alloc: Allocator,
        runtime_schema: schema_mod.TableSchema,
        req: types.RelationalRowsJoinedMutationSourceRequest,
    ) !types.RelationalRowsMutationSourceResult {
        try ha_replication_impl.enforceDurableMutationGate(self);
        return try relational_rows_impl.mutateRelationalRowsJoinedSourceAlloc(self, alloc, runtime_schema, req);
    }

    pub fn planRelationalRowsJoinedMutationSourceAlloc(
        self: *DB,
        alloc: Allocator,
        runtime_schema: schema_mod.TableSchema,
        req: types.RelationalRowsJoinedMutationSourceRequest,
    ) !RelationalRowsJoinedMutationSourcePlan {
        return try relational_rows_impl.planRelationalRowsJoinedMutationSourceAlloc(self, alloc, runtime_schema, req);
    }

    pub fn planRelationalRowsJoinedMutationSourceAcrossRangesAlloc(
        self: *DB,
        alloc: Allocator,
        runtime_schema: schema_mod.TableSchema,
        req: types.RelationalRowsJoinedMutationSourceRequest,
        target_ranges: []const types.RelationalRowsDocKeyRange,
        source_ranges: []const types.RelationalRowsDocKeyRange,
    ) !RelationalRowsJoinedMutationSourcePlan {
        return try relational_rows_impl.planRelationalRowsJoinedMutationSourceAcrossRangesAlloc(self, alloc, runtime_schema, req, target_ranges, source_ranges);
    }

    pub fn planRelationalRowsJoinedMutationSourceAcrossRangesWithSchemasAlloc(
        self: *DB,
        alloc: Allocator,
        target_schema: schema_mod.TableSchema,
        source_schema: schema_mod.TableSchema,
        req: types.RelationalRowsJoinedMutationSourceRequest,
        target_ranges: []const types.RelationalRowsDocKeyRange,
        source_ranges: []const types.RelationalRowsDocKeyRange,
    ) !RelationalRowsJoinedMutationSourcePlan {
        return try relational_rows_impl.planRelationalRowsJoinedMutationSourceAcrossRangesWithSchemasAlloc(self, alloc, target_schema, source_schema, req, target_ranges, source_ranges);
    }

    pub fn collectRelationalRowsJoinedMutationSourceCandidatesAlloc(
        self: *DB,
        alloc: Allocator,
        runtime_schema: schema_mod.TableSchema,
        req: types.RelationalRowsJoinedMutationSourceRequest,
    ) ![]RelationalRowsJoinedMutationSourceCandidate {
        return try relational_rows_impl.collectRelationalRowsJoinedMutationSourceCandidatesAlloc(self, alloc, runtime_schema, req);
    }

    pub fn collectRelationalRowsJoinedMutationSourceCandidatesAcrossRangesAlloc(
        self: *DB,
        alloc: Allocator,
        runtime_schema: schema_mod.TableSchema,
        req: types.RelationalRowsJoinedMutationSourceRequest,
        target_ranges: []const types.RelationalRowsDocKeyRange,
        source_ranges: []const types.RelationalRowsDocKeyRange,
    ) ![]RelationalRowsJoinedMutationSourceCandidate {
        return try relational_rows_impl.collectRelationalRowsJoinedMutationSourceCandidatesAcrossRangesAlloc(self, alloc, runtime_schema, req, target_ranges, source_ranges);
    }

    pub fn collectRelationalRowsJoinedMutationSourceCandidatesAcrossRangesWithSchemasAlloc(
        self: *DB,
        alloc: Allocator,
        target_schema: schema_mod.TableSchema,
        source_schema: schema_mod.TableSchema,
        req: types.RelationalRowsJoinedMutationSourceRequest,
        target_ranges: []const types.RelationalRowsDocKeyRange,
        source_ranges: []const types.RelationalRowsDocKeyRange,
    ) ![]RelationalRowsJoinedMutationSourceCandidate {
        return try relational_rows_impl.collectRelationalRowsJoinedMutationSourceCandidatesAcrossRangesWithSchemasAlloc(self, alloc, target_schema, source_schema, req, target_ranges, source_ranges);
    }

    pub fn collectRelationalRowsJoinedMutationSourceCandidatesForTargetRangeAlloc(
        self: *DB,
        alloc: Allocator,
        runtime_schema: schema_mod.TableSchema,
        req: types.RelationalRowsJoinedMutationSourceRequest,
        target_doc_key_range: ?types.RelationalRowsDocKeyRange,
    ) ![]RelationalRowsJoinedMutationSourceCandidate {
        return try relational_rows_impl.collectRelationalRowsJoinedMutationSourceCandidatesForTargetRangeAlloc(self, alloc, runtime_schema, req, target_doc_key_range);
    }

    pub fn collectRelationalRowsJoinedMutationTargetCandidatesForTargetRangeAlloc(
        self: *DB,
        alloc: Allocator,
        runtime_schema: schema_mod.TableSchema,
        req: types.RelationalRowsJoinedMutationSourceRequest,
        target_doc_key_range: ?types.RelationalRowsDocKeyRange,
    ) ![]RelationalRowsMutationSourceCandidate {
        return try relational_rows_impl.collectRelationalRowsJoinedMutationTargetCandidatesForTargetRangeAlloc(self, alloc, runtime_schema, req, target_doc_key_range);
    }

    pub fn queryRelationalRowsJoinedMutationSourceSideForRangeAlloc(
        self: *DB,
        alloc: Allocator,
        runtime_schema: schema_mod.TableSchema,
        req: types.RelationalRowsJoinedMutationSourceRequest,
        source_doc_key_range: ?types.RelationalRowsDocKeyRange,
    ) !types.RelationalRowsQueryResult {
        return try relational_rows_impl.queryRelationalRowsJoinedMutationSourceSideForRangeAlloc(self, alloc, runtime_schema, req, source_doc_key_range);
    }

    pub fn queryRelationalRowsJoinedMutationSourceSideOnlyForRangeAlloc(
        self: *DB,
        alloc: Allocator,
        source_schema: schema_mod.TableSchema,
        req: types.RelationalRowsJoinedMutationSourceRequest,
        source_doc_key_range: ?types.RelationalRowsDocKeyRange,
    ) !types.RelationalRowsQueryResult {
        return try relational_rows_impl.queryRelationalRowsJoinedMutationSourceSideOnlyForRangeAlloc(self, alloc, source_schema, req, source_doc_key_range);
    }

    pub fn buildRelationalRowsJoinedMutationSourceCandidatesFromCollectedRowsAlloc(
        alloc: Allocator,
        req: types.RelationalRowsJoinedMutationSourceRequest,
        target_candidates: *[]RelationalRowsMutationSourceCandidate,
        source_rows: []const []const u8,
    ) ![]RelationalRowsJoinedMutationSourceCandidate {
        return try relational_rows_impl.buildRelationalRowsJoinedMutationSourceCandidatesFromCollectedRowsAlloc(alloc, req, target_candidates, source_rows);
    }

    pub fn selectPlannedRelationalRowsJoinedMutationSourceCandidatesAlloc(
        alloc: Allocator,
        req: types.RelationalRowsJoinedMutationSourceRequest,
        candidates: *[]RelationalRowsJoinedMutationSourceCandidate,
    ) !RelationalRowsJoinedMutationSourcePlan {
        return try relational_rows_impl.selectPlannedRelationalRowsJoinedMutationSourceCandidatesAlloc(alloc, req, candidates);
    }

    pub fn stagePlannedRelationalRowsJoinedMutationSourceAlloc(
        self: *DB,
        alloc: Allocator,
        runtime_schema: schema_mod.TableSchema,
        req: types.RelationalRowsJoinedMutationSourceRequest,
        matched: u32,
        candidates: []const RelationalRowsJoinedMutationSourceCandidate,
    ) !types.RelationalRowsMutationSourceResult {
        try ha_replication_impl.enforceDurableMutationGate(self);
        return try relational_rows_impl.stagePlannedRelationalRowsJoinedMutationSourceAlloc(self, alloc, runtime_schema, req, matched, candidates);
    }

    pub fn stagePlannedRelationalRowsJoinedMutationSourceWithSourceSchemaAlloc(
        self: *DB,
        alloc: Allocator,
        target_schema: schema_mod.TableSchema,
        source_schema: schema_mod.TableSchema,
        req: types.RelationalRowsJoinedMutationSourceRequest,
        matched: u32,
        candidates: []const RelationalRowsJoinedMutationSourceCandidate,
    ) !types.RelationalRowsMutationSourceResult {
        try ha_replication_impl.enforceDurableMutationGate(self);
        return try relational_rows_impl.stagePlannedRelationalRowsJoinedMutationSourceWithSourceSchemaAlloc(self, alloc, target_schema, source_schema, req, matched, candidates);
    }

    pub fn windowRelationalRowsPlan(
        self: *DB,
        alloc: Allocator,
        runtime_schema: schema_mod.TableSchema,
        plan: types.RelationalRowsWindowPlan,
    ) !types.RelationalRowsWindowResult {
        return try relational_rows_impl.windowRelationalRowsPlan(self, alloc, runtime_schema, plan);
    }

    pub fn windowRelationalRowsAcrossRanges(
        self: *DB,
        alloc: Allocator,
        runtime_schema: schema_mod.TableSchema,
        req: types.RelationalRowsWindowRequest,
        ranges: []const types.RelationalRowsDocKeyRange,
    ) !types.RelationalRowsWindowResult {
        return try relational_rows_impl.windowRelationalRowsAcrossRanges(self, alloc, runtime_schema, req, ranges);
    }

    pub fn admitRelationalRowsSetOperationRows(
        plan: types.RelationalRowsSetOperationPlan,
        rows: []const []const u8,
    ) !void {
        try relational_rows.admitRelationalRowsSetOperationRows(plan, rows);
    }

    pub fn admitRelationalRowsSetOperationRowsAllowSpill(
        plan: types.RelationalRowsSetOperationPlan,
        rows: []const []const u8,
    ) !void {
        try relational_rows.admitRelationalRowsSetOperationRowsAllowSpill(plan, rows);
    }

    pub fn admitRelationalRowsCteMaterialization(
        cte: types.RelationalRowsCte,
        observed_rows: usize,
        observed_bytes: u64,
    ) !void {
        try relational_rows.admitRelationalRowsCteMaterialization(cte, observed_rows, observed_bytes);
    }

    pub fn admitRelationalRowsCteMaterializationAllowSpill(
        cte: types.RelationalRowsCte,
        observed_rows: usize,
        observed_bytes: u64,
    ) !void {
        try relational_rows.admitRelationalRowsCteMaterializationAllowSpill(cte, observed_rows, observed_bytes);
    }

    pub fn relationalRowsSetOperationRowsAlloc(
        alloc: Allocator,
        operation: types.RelationalRowsSetOperation,
        left: []const []const u8,
        right: []const []const u8,
    ) ![]const []const u8 {
        return try relational_rows.relationalRowsSetOperationRowsAlloc(alloc, operation, left, right);
    }

    pub fn queryRelationalRowsAcrossRanges(
        self: *DB,
        alloc: Allocator,
        runtime_schema: schema_mod.TableSchema,
        req: types.RelationalRowsQueryRequest,
        ranges: []const types.RelationalRowsDocKeyRange,
    ) !types.RelationalRowsQueryResult {
        return try relational_rows_impl.queryRelationalRowsAcrossRanges(self, alloc, runtime_schema, req, ranges);
    }

    pub fn queryRelationalRowsFromSourceRowsAlloc(
        self: *DB,
        alloc: Allocator,
        source_name: []const u8,
        source_rows: []const []const u8,
        req: types.RelationalRowsQueryRequest,
    ) !types.RelationalRowsQueryResult {
        return try relational_rows_impl.queryRelationalRowsFromSourceRowsAlloc(self, alloc, source_name, source_rows, req);
    }

    pub fn queryRelationalRowsFromSourceRowsStaticAlloc(
        alloc: Allocator,
        source_name: []const u8,
        source_rows: []const []const u8,
        req: types.RelationalRowsQueryRequest,
    ) !types.RelationalRowsQueryResult {
        return try relational_rows_impl.queryRelationalRowsFromSourceRowsStaticAlloc(alloc, source_name, source_rows, req);
    }

    pub fn windowRelationalRowsFromUnorderedSourceRowsAlloc(
        self: *DB,
        alloc: Allocator,
        source_name: []const u8,
        source_rows: []const []const u8,
        req: types.RelationalRowsWindowRequest,
    ) !types.RelationalRowsWindowResult {
        return try relational_rows_impl.windowRelationalRowsFromUnorderedSourceRowsAlloc(self, alloc, source_name, source_rows, req);
    }

    pub fn windowRelationalRowsFromUnorderedSourceRowsStaticAlloc(
        alloc: Allocator,
        source_name: []const u8,
        source_rows: []const []const u8,
        req: types.RelationalRowsWindowRequest,
    ) !types.RelationalRowsWindowResult {
        return try relational_rows_impl.windowRelationalRowsFromUnorderedSourceRowsStaticAlloc(alloc, source_name, source_rows, req);
    }

    pub fn applyRelationalRowsClaimToSelectedCandidatesAlloc(
        self: *DB,
        alloc: Allocator,
        rows: []const relational_rows.QueryCandidate,
        selected_indexes: *std.ArrayListUnmanaged(usize),
        claim: types.RowClaimRequest,
        limit: ?u32,
    ) !u32 {
        return try relational_rows_impl.applyRelationalRowsClaimToSelectedCandidatesAlloc(self, alloc, rows, selected_indexes, claim, limit);
    }

    pub fn aggregateRelationalRows(
        self: *DB,
        alloc: Allocator,
        runtime_schema: schema_mod.TableSchema,
        req: types.RelationalRowsAggregateRequest,
    ) !types.RelationalRowsAggregateResult {
        return try relational_rows_impl.aggregateRelationalRows(self, alloc, runtime_schema, req);
    }

    pub fn aggregateRelationalRowsPlan(
        self: *DB,
        alloc: Allocator,
        runtime_schema: schema_mod.TableSchema,
        plan: types.RelationalRowsAggregatePlan,
    ) !types.RelationalRowsAggregateResult {
        return try relational_rows_impl.aggregateRelationalRowsPlan(self, alloc, runtime_schema, plan);
    }

    pub fn aggregateRelationalRowsAcrossRanges(
        self: *DB,
        alloc: Allocator,
        runtime_schema: schema_mod.TableSchema,
        req: types.RelationalRowsAggregateRequest,
        ranges: []const types.RelationalRowsDocKeyRange,
    ) !types.RelationalRowsAggregateResult {
        return try relational_rows_impl.aggregateRelationalRowsAcrossRanges(self, alloc, runtime_schema, req, ranges);
    }

    pub fn aggregateRelationalRowsFromSourceRowsAlloc(
        alloc: Allocator,
        req: types.RelationalRowsAggregateRequest,
        source_rows: []const []const u8,
    ) !types.RelationalRowsAggregateResult {
        return try relational_rows_impl.aggregateRelationalRowsFromSourceRowsAlloc(alloc, req, source_rows);
    }

    pub fn joinRelationalRows(
        self: *DB,
        alloc: Allocator,
        runtime_schema: schema_mod.TableSchema,
        req: types.RelationalRowsJoinRequest,
    ) !types.RelationalRowsJoinResult {
        return try relational_rows_impl.joinRelationalRows(self, alloc, runtime_schema, req);
    }

    pub fn joinRelationalRowsPlan(
        self: *DB,
        alloc: Allocator,
        runtime_schema: schema_mod.TableSchema,
        plan: types.RelationalRowsJoinPlan,
    ) !types.RelationalRowsJoinResult {
        return try relational_rows_impl.joinRelationalRowsPlan(self, alloc, runtime_schema, plan);
    }

    pub fn joinRelationalRowsAcrossRanges(
        self: *DB,
        alloc: Allocator,
        runtime_schema: schema_mod.TableSchema,
        req: types.RelationalRowsJoinRequest,
        left_ranges: []const types.RelationalRowsDocKeyRange,
        right_ranges: []const types.RelationalRowsDocKeyRange,
    ) !types.RelationalRowsJoinResult {
        return try relational_rows_impl.joinRelationalRowsAcrossRanges(self, alloc, runtime_schema, req, left_ranges, right_ranges);
    }

    pub fn lateralRelationalRowsPlan(
        self: *DB,
        alloc: Allocator,
        runtime_schema: schema_mod.TableSchema,
        plan: types.RelationalRowsLateralPlan,
    ) !types.RelationalRowsJoinResult {
        return try relational_rows_impl.lateralRelationalRowsPlan(self, alloc, runtime_schema, plan);
    }

    pub fn lateralRelationalRowsAcrossRanges(
        self: *DB,
        alloc: Allocator,
        runtime_schema: schema_mod.TableSchema,
        req: types.RelationalRowsLateralRequest,
        left_ranges: []const types.RelationalRowsDocKeyRange,
        right_ranges: []const types.RelationalRowsDocKeyRange,
    ) !types.RelationalRowsJoinResult {
        return try relational_rows_impl.lateralRelationalRowsAcrossRanges(self, alloc, runtime_schema, req, left_ranges, right_ranges);
    }

    pub fn joinRelationalRowsFromSourceRowsAlloc(
        alloc: Allocator,
        req: types.RelationalRowsJoinRequest,
        left_rows: []const []const u8,
        right_rows: []const []const u8,
    ) !types.RelationalRowsJoinResult {
        return try relational_rows_impl.joinRelationalRowsFromSourceRowsAlloc(alloc, req, left_rows, right_rows);
    }

    pub fn lateralRelationalRowsFromSourceRowsAlloc(
        self: *DB,
        alloc: Allocator,
        runtime_schema: schema_mod.TableSchema,
        req: types.RelationalRowsLateralRequest,
        left_rows: []const []const u8,
        right_rows: []const []const u8,
    ) !types.RelationalRowsJoinResult {
        return try relational_rows_impl.lateralRelationalRowsFromSourceRowsAlloc(self, alloc, runtime_schema, req, left_rows, right_rows);
    }

    pub fn lateralRelationalRowsFromSourceRowsStaticAlloc(
        alloc: Allocator,
        req: types.RelationalRowsLateralRequest,
        left_rows: []const []const u8,
        right_rows: []const []const u8,
    ) !types.RelationalRowsJoinResult {
        return try relational_rows_impl.lateralRelationalRowsFromSourceRowsStaticAlloc(alloc, req, left_rows, right_rows);
    }

    pub fn reclaimExpiredRowClaimIntentsForRows(
        self: *DB,
        claiming_txn_id: types.TxnId,
        row_keys: []const []const u8,
        now_ns: u64,
    ) !usize {
        return try relational_rows_impl.reclaimExpiredRowClaimIntentsForRows(self, claiming_txn_id, row_keys, now_ns);
    }

    pub fn reclaimExpiredRowClaimIntentsForMutationKeys(
        self: *DB,
        writes: anytype,
        deletes: []const []const u8,
        exclude_txn_id: ?types.TxnId,
        now_ns: u64,
        comptime already_locked: bool,
        comptime is_user_row_mutation_key: fn ([]const u8) bool,
    ) !usize {
        return try relational_rows_impl.reclaimExpiredRowClaimIntentsForMutationKeys(self, writes, deletes, exclude_txn_id, now_ns, already_locked, is_user_row_mutation_key);
    }

    pub fn reclaimExpiredRowClaimIntentsForIdentityRewrites(
        self: *DB,
        rewrites: []const types.RelationalIdentityRewrite,
        exclude_txn_id: ?types.TxnId,
        now_ns: u64,
        comptime already_locked: bool,
        comptime is_user_row_mutation_key: fn ([]const u8) bool,
    ) !usize {
        return try relational_rows_impl.reclaimExpiredRowClaimIntentsForIdentityRewrites(self, rewrites, exclude_txn_id, now_ns, already_locked, is_user_row_mutation_key);
    }

    pub fn appendSystemVersionedHistoryForBatch(
        self: *DB,
        req: types.BatchRequest,
        sequence: u64,
        timestamp_ns: u64,
        writes: *std.ArrayListUnmanaged(docstore_mod.KVPair),
        owned_keys: *std.ArrayListUnmanaged([]u8),
        owned_values: *std.ArrayListUnmanaged([]u8),
        comptime is_user_row_mutation_key: fn ([]const u8) bool,
    ) !void {
        return try relational_rows_impl.appendSystemVersionedHistoryForBatch(self, req, sequence, timestamp_ns, writes, owned_keys, owned_values, is_user_row_mutation_key);
    }

    pub fn appendSystemVersionedHistoryForTransactionMutations(
        self: *DB,
        mutations: []const transactions_mod.OwnedIntentMutation,
        rewrites: []const types.RelationalIdentityRewrite,
        commit_version: u64,
        writes: *std.ArrayListUnmanaged(docstore_mod.KVPair),
        owned_keys: *std.ArrayListUnmanaged([]u8),
        owned_values: *std.ArrayListUnmanaged([]u8),
        comptime is_user_row_mutation_key: fn ([]const u8) bool,
    ) !void {
        return try relational_rows_impl.appendSystemVersionedHistoryForTransactionMutations(self, mutations, rewrites, commit_version, writes, owned_keys, owned_values, is_user_row_mutation_key);
    }

    pub const RelationalRowsMutationSourceCandidate = relational_rows.MutationSourceCandidate;

    pub fn cloneRelationalRowsMutationSourceCandidateAlloc(
        alloc: Allocator,
        candidate: RelationalRowsMutationSourceCandidate,
    ) !RelationalRowsMutationSourceCandidate {
        return try relational_rows.cloneMutationSourceCandidateAlloc(alloc, candidate);
    }

    pub const RelationalRowsMutationSourcePlan = relational_rows.MutationSourcePlan;

    pub const RelationalRowsJoinedMutationSourceCandidate = relational_rows.JoinedMutationSourceCandidate;

    pub const RelationalRowsJoinedMutationSourcePlan = relational_rows.JoinedMutationSourcePlan;

    pub const RelationalRowsQueryOrderKey = relational_rows.QueryOrderKey;

    pub fn validateRelationalRowsExpressionAgainstSchema(
        self: *DB,
        runtime_schema: schema_mod.TableSchema,
        expression: schema_mod.RelationalRowsExpression,
    ) !void {
        _ = self;
        return try relational_rows.validateExpressionAgainstSchema(runtime_schema, expression);
    }

    pub fn relationalRowsExpressionValueJsonAlloc(
        self: *DB,
        alloc: Allocator,
        row: std.json.Value,
        expression: types.RelationalRowsExpression,
    ) anyerror![]u8 {
        _ = self;
        return try relational_rows_impl.relationalRowsExpressionValueJsonAlloc(alloc, row, expression);
    }

    pub fn relationalRowsGeneratedColumnValueJsonAlloc(
        self: *DB,
        alloc: Allocator,
        row: std.json.Value,
        generated: schema_mod.RelationalGeneratedValue,
    ) ![]u8 {
        _ = self;
        return try relational_rows_impl.schemaRuntimeRelationalRowsGeneratedColumnValueJsonAlloc(alloc, row, generated);
    }

    pub fn relationalRowsExpressionConditionMatches(
        self: *DB,
        alloc: Allocator,
        row: std.json.Value,
        condition: types.RelationalRowsExpressionCondition,
    ) anyerror!bool {
        _ = self;
        return try relational_rows_impl.relationalRowsExpressionConditionMatches(alloc, row, condition);
    }

    pub fn relationalRowsQueryPredicatePasses(
        self: *DB,
        alloc: Allocator,
        row: std.json.Value,
        predicate: schema_mod.RelationalCheck,
    ) !bool {
        _ = self;
        return try relational_rows.queryPredicatePasses(alloc, row, predicate);
    }

    pub fn relationalRowsExpressionConditionsImpliedByEqualityPredicatesAlloc(
        alloc: Allocator,
        predicates: []const schema_mod.RelationalCheck,
        required: []const types.RelationalRowsExpressionCondition,
    ) !bool {
        return try relational_rows_impl.relationalRowsExpressionConditionsImpliedByEqualityPredicatesAlloc(alloc, predicates, required);
    }

    pub fn relationalRowsExpressionPredicatesImply(
        self: *DB,
        alloc: Allocator,
        implications: relational_store_mod.PredicateImplications,
        required: []const types.RelationalRowsExpressionCondition,
    ) !bool {
        _ = self;
        return try relational_rows_impl.relationalRowsExpressionPredicatesImply(alloc, implications, required);
    }

    pub fn search(self: *DB, alloc: Allocator, req: types.SearchRequest) !types.SearchResult {
        return try search_runtime_impl.search(self, alloc, req);
    }

    pub fn searchWithExecutionContext(
        self: *DB,
        alloc: Allocator,
        req: types.SearchRequest,
        exec_ctx: types.ExecutionContext,
    ) !types.SearchResult {
        return try search_runtime_impl.searchWithExecutionContext(self, alloc, req, exec_ctx);
    }

    pub fn searchRuntimeApplyRowClaimToSearchResult(
        self: *DB,
        result: *types.SearchResult,
        txn_id: types.TxnId,
        claim: types.RowClaimRequest,
    ) !void {
        return try search_runtime_impl.applyRowClaimToSearchResult(self, result, txn_id, claim);
    }

    pub fn searchRequestAtCurrentIdentityGeneration(self: *DB, req: types.SearchRequest) !types.SearchRequest {
        return try search_runtime_impl.searchRequestAtCurrentIdentityGeneration(self, req);
    }

    pub fn searchRuntimeCanUsePublishedDenseSearch(self: *DB, req: types.SearchRequest) bool {
        return search_runtime_impl.canUsePublishedDenseSearch(self, req);
    }

    pub fn searchRuntimeSearchRequestWithTextAlgebraicDocFilterAlloc(self: *DB, req: types.SearchRequest) !AlgebraicDocFilterRequest {
        return try search_runtime_impl.searchRequestWithTextAlgebraicDocFilterAlloc(self, req);
    }

    pub fn searchRuntimeSearchRequestWithAlgebraicDocFilterAlloc(self: *DB, req: types.SearchRequest) !AlgebraicDocFilterRequest {
        return try search_runtime_impl.searchRequestWithAlgebraicDocFilterAlloc(self, req);
    }

    pub fn searchRuntimeApplyGraphExpandStrategy(
        self: *DB,
        alloc: Allocator,
        result: *types.SearchResult,
        strategy: ?search_runtime.ExpandStrategy,
    ) !void {
        try search_runtime_impl.applyGraphExpandStrategy(self, alloc, result, strategy);
    }

    pub fn searchRuntimeCloneNamedSetAsResult(
        self: *DB,
        alloc: Allocator,
        set: search_runtime.GraphNamedResultSet,
        include_stored: bool,
    ) !types.SearchResult {
        return try search_runtime_impl.cloneNamedSetAsResult(self, alloc, set, include_stored);
    }

    pub fn searchRuntimeResolveSearchHitsToDocSet(
        self: *DB,
        alloc: Allocator,
        req: types.SearchRequest,
        hits: []const types.SearchHit,
    ) !doc_set.ResolvedDocSet {
        return try search_runtime_impl.resolveSearchHitsToDocSet(self, alloc, req, hits);
    }

    pub fn searchRuntimeResolveGraphNodesToDocSet(
        self: *DB,
        alloc: Allocator,
        req: types.SearchRequest,
        nodes: []const search_runtime.GraphResultNode,
    ) !doc_set.ResolvedDocSet {
        return try search_runtime_impl.resolveGraphNodesToDocSet(self, alloc, req, nodes);
    }

    pub fn internalRecordResolvedDocSet(
        self: *DB,
        set: *const doc_set.ResolvedDocSet,
        missing_ordinal_coverage: bool,
    ) void {
        internal_impl.recordResolvedDocSet(self, set, missing_ordinal_coverage);
    }

    pub fn internalResolveDocSetForIdsNoLockAtGenerationAlloc(
        self: *DB,
        alloc: Allocator,
        doc_ids: []const []const u8,
        generation: ?u64,
    ) !doc_set.ResolvedDocSet {
        return try internal_impl.resolveDocSetForIdsNoLockAtGenerationAlloc(self, alloc, doc_ids, generation);
    }

    pub fn internalResolveDocSetForIdsAlloc(
        self: *DB,
        alloc: Allocator,
        doc_ids: []const []const u8,
    ) !doc_set.ResolvedDocSet {
        return try internal_impl.resolveDocSetForIdsAlloc(self, alloc, doc_ids);
    }

    pub fn internalDocIdsForResolvedDocSetNoLockAtGenerationAlloc(
        self: *DB,
        alloc: Allocator,
        set: *const doc_set.ResolvedDocSet,
        generation: ?u64,
    ) !?[]const []const u8 {
        return try internal_impl.docIdsForResolvedDocSetNoLockAtGenerationAlloc(self, alloc, set, generation);
    }

    pub fn internalDocIdsForResolvedDocSetAlloc(
        self: *DB,
        alloc: Allocator,
        set: *const doc_set.ResolvedDocSet,
    ) !?[]const []const u8 {
        return try internal_impl.docIdsForResolvedDocSetAlloc(self, alloc, set);
    }

    pub fn internalResolvedDocFilterForIdsAlloc(
        self: *DB,
        include_positive: bool,
        include_doc_ids: []const []const u8,
        exclude_doc_ids: []const []const u8,
        generation: ?u64,
    ) !doc_set.ResolvedDocFilter {
        return try internal_impl.resolvedDocFilterForIdsAlloc(self, include_positive, include_doc_ids, exclude_doc_ids, generation);
    }

    pub fn internalRecordUnsupportedDocSetFilterShape(self: *DB) void {
        internal_impl.recordUnsupportedDocSetFilterShape(self);
    }

    pub fn internalAllDocsVisibleAtGeneration(self: *DB, generation: ?u64) !bool {
        return try internal_impl.allDocsVisibleAtGeneration(self, generation);
    }

    pub fn internalAllDocsVisibleSummaryFast(self: *DB, generation: ?u64) !bool {
        return try internal_impl.allDocsVisibleSummaryFast(self, generation);
    }

    pub fn internalLookupLiveDocOrdinalNoLock(
        self: *DB,
        alloc: Allocator,
        doc_id: []const u8,
        generation: ?u64,
    ) !?doc_set.DocOrdinal {
        return try internal_impl.lookupLiveDocOrdinalNoLock(self, alloc, doc_id, generation);
    }

    pub fn internalLookupLiveDocOrdinalsNoLock(
        self: *DB,
        alloc: Allocator,
        doc_ids: []const []const u8,
        generation: ?u64,
    ) ![]?doc_set.DocOrdinal {
        return try internal_impl.lookupLiveDocOrdinalsNoLock(self, alloc, doc_ids, generation);
    }

    pub fn internalAnnotateSearchHitOrdinalsNoLock(
        self: *DB,
        alloc: Allocator,
        req: types.SearchRequest,
        hits: []types.SearchHit,
    ) !void {
        try internal_impl.annotateSearchHitOrdinalsNoLock(self, alloc, req, hits);
    }

    pub fn searchRuntimeProjectLookupStoredBytes(self: *DB, alloc: Allocator, doc_key: []const u8, raw: []const u8, opts: types.LookupOptions) ![]u8 {
        return try search_runtime_impl.projectLookupStoredBytes(self, alloc, doc_key, raw, opts);
    }

    pub fn searchRuntimeMatchPattern(
        self: *DB,
        alloc: Allocator,
        named: *const types.NamedGraphQuery,
        start_key_refs: []const []const u8,
    ) ![]search_runtime.PatternMatch {
        return try search_runtime_impl.matchNamedPattern(self, alloc, named, start_key_refs);
    }

    pub fn searchRuntimeLoadPatternProjectedDocument(
        self: *DB,
        alloc: Allocator,
        query: search_runtime.GraphQuery,
        key: []const u8,
    ) !?[]u8 {
        return try search_runtime_impl.loadPatternProjectedDocument(self, alloc, query, key);
    }

    pub fn searchRuntimeResolveDocSetDocIds(
        self: *DB,
        alloc: Allocator,
        set: *const doc_set.ResolvedDocSet,
        generation: ?u64,
    ) !?[]const []const u8 {
        return try search_runtime_impl.resolveDocSetDocIds(self, alloc, set, generation);
    }

    pub fn searchRuntimeResolveDocIdsToDocSet(
        self: *DB,
        alloc: Allocator,
        doc_ids: []const []const u8,
        generation: ?u64,
    ) !doc_set.ResolvedDocSet {
        return try search_runtime_impl.resolveDocIdsToDocSet(self, alloc, doc_ids, generation);
    }

    pub fn searchRuntimeResolvedDocFilterForIdsAlloc(
        self: *DB,
        include_positive: bool,
        include_doc_ids: []const []const u8,
        exclude_doc_ids: []const []const u8,
        generation: ?u64,
    ) !doc_set.ResolvedDocFilter {
        return try search_runtime_impl.resolvedDocFilterForIdsAlloc(self, include_positive, include_doc_ids, exclude_doc_ids, generation);
    }

    pub fn searchRuntimeResolvedDocFilterForRequestNativeConstraintsAlloc(
        self: *DB,
        alloc: Allocator,
        req: types.SearchRequest,
    ) !?doc_set.ResolvedDocFilter {
        return try search_runtime_impl.resolvedDocFilterForRequestNativeConstraintsAlloc(self, alloc, req);
    }

    pub fn searchRuntimeRecordUnsupportedDocSetFilterShape(self: *DB) void {
        search_runtime_impl.recordUnsupportedDocSetFilterShape(self);
    }

    pub fn searchRuntimeResolveRelationalFilterDocSet(
        self: *DB,
        alloc: Allocator,
        runtime_schema: schema_mod.TableSchema,
        query: search_runtime.SearchQuery,
        generation: ?u64,
    ) !?doc_set.ResolvedDocSet {
        return try search_runtime_impl.resolveRelationalFilterDocSet(self, alloc, runtime_schema, query, generation);
    }

    pub fn searchRuntimeResolveRelationalFilterQueryDocSetAlloc(
        self: *DB,
        alloc: Allocator,
        runtime_schema: schema_mod.TableSchema,
        query: search_runtime.SearchQuery,
        generation: ?u64,
    ) !?doc_set.ResolvedDocSet {
        return try search_runtime_impl.resolveRelationalFilterQueryDocSetAlloc(self, alloc, runtime_schema, query, generation);
    }

    pub fn searchRuntimeResolveRelationalFilterQueryDocSetWithImplicationsAlloc(
        self: *DB,
        alloc: Allocator,
        runtime_schema: schema_mod.TableSchema,
        query: search_runtime.SearchQuery,
        implications: relational_store_mod.PredicateImplications,
        generation: ?u64,
    ) !?doc_set.ResolvedDocSet {
        return try search_runtime_impl.resolveRelationalFilterQueryDocSetWithImplicationsAlloc(self, alloc, runtime_schema, query, implications, generation);
    }

    pub fn searchRuntimeCombineRelationalFilterSetAlloc(
        self: *DB,
        alloc: Allocator,
        current: *?doc_set.ResolvedDocSet,
        child: *const doc_set.ResolvedDocSet,
        generation: ?u64,
        mode: relational_store_mod.FilterCombineMode,
    ) !void {
        try search_runtime_impl.combineRelationalFilterSetAlloc(self, alloc, current, child, generation, mode);
    }

    pub fn searchRuntimeRelationalFilterGenerationCanUseCurrentRows(self: *DB, generation: ?u64) bool {
        return search_runtime_impl.relationalFilterGenerationCanUseCurrentRows(self, generation);
    }

    pub fn searchRuntimeRelationalColumnIndexUsableForQuery(
        self: *DB,
        alloc: Allocator,
        column: schema_mod.RelationalColumn,
        implications: relational_store_mod.PredicateImplications,
    ) !bool {
        return try search_runtime_impl.relationalColumnIndexUsableForQuery(self, alloc, column, implications);
    }

    pub fn resolveRelationalRowsQueryCandidateSetAlloc(
        self: *DB,
        alloc: Allocator,
        runtime_schema: schema_mod.TableSchema,
        req: types.RelationalRowsQueryRequest,
        generation: ?u64,
    ) !?doc_set.ResolvedDocSet {
        return try relational_rows_impl.resolveRelationalRowsQueryCandidateSetAlloc(self, alloc, runtime_schema, req, generation);
    }

    pub fn searchRuntimeLiveFilterDocSet(
        self: *DB,
        alloc: Allocator,
        set: *const doc_set.ResolvedDocSet,
        generation: ?u64,
    ) !doc_set.ResolvedDocSet {
        return try search_runtime_impl.liveFilterDocSet(self, alloc, set, generation);
    }

    pub fn searchRuntimeAllDocsVisible(self: *DB, generation: ?u64) !bool {
        return try search_runtime_impl.allDocsVisible(self, generation);
    }

    pub fn searchRuntimeTextIndexIsChunkBacked(self: *DB, alloc: Allocator, index_name: ?[]const u8) !bool {
        return try self.core.textIndexIsChunkBacked(alloc, index_name);
    }

    pub fn searchRuntimeLoadChunkFieldValue(self: *DB, alloc: Allocator, doc_key: []const u8) !?std.json.Value {
        return try search_runtime_impl.loadChunkFieldValue(self, alloc, doc_key);
    }

    pub fn searchRuntimeLoadEmbeddingFieldValue(self: *DB, alloc: Allocator, doc_key: []const u8) !?std.json.Value {
        return try search_runtime_impl.loadEmbeddingFieldValue(self, alloc, doc_key);
    }

    pub fn searchRuntimeLoadArtifactFieldValue(self: *DB, alloc: Allocator, doc_key: []const u8) !?std.json.Value {
        return try search_runtime_impl.loadArtifactFieldValue(self, alloc, doc_key);
    }

    pub fn searchRuntimeProjectStoredBytesForSearch(
        self: *DB,
        alloc: Allocator,
        req: types.SearchRequest,
        doc_key: []const u8,
        raw: []const u8,
    ) ![]u8 {
        return try search_runtime_impl.projectStoredBytesForSearch(self, alloc, req, doc_key, raw);
    }

    pub fn searchRuntimeProjectOwnedStoredBytesForSearch(
        self: *DB,
        alloc: Allocator,
        req: types.SearchRequest,
        doc_key: []const u8,
        raw: []u8,
    ) ![]u8 {
        return try search_runtime_impl.projectOwnedStoredBytesForSearch(self, alloc, req, doc_key, raw);
    }

    pub fn searchRuntimeLoadProjectedSearchDocument(
        self: *DB,
        alloc: Allocator,
        req: types.SearchRequest,
        key: []const u8,
    ) !?[]u8 {
        return try search_runtime_impl.loadProjectedSearchDocument(self, alloc, req, key);
    }

    pub fn searchRuntimePostprocessTextSearchResult(
        self: *DB,
        alloc: Allocator,
        req: types.SearchRequest,
        raw: types.SearchResult,
        chunk_backed: bool,
    ) !types.SearchResult {
        return try search_runtime_impl.postprocessTextSearchResult(self, alloc, req, raw, chunk_backed);
    }

    pub fn searchRuntimeDenseIndex(self: *DB, index_name: ?[]const u8) ?*index_manager_mod.IndexManager.DenseIndex {
        return search_runtime_impl.denseIndex(self, index_name);
    }

    pub fn searchRuntimeSparseIndex(self: *DB, index_name: ?[]const u8) ?*index_manager_mod.IndexManager.SparseIndex {
        return search_runtime_impl.sparseIndex(self, index_name);
    }

    pub fn searchRuntimeDenseDocKey(self: *DB, index_name: []const u8, vector_id: u64) !?[]u8 {
        return try search_runtime_impl.denseDocKey(self, index_name, vector_id);
    }

    pub fn searchRuntimeDenseVectorId(self: *DB, index_name: []const u8, doc_key: []const u8) !?u64 {
        return try search_runtime_impl.denseVectorId(self, index_name, doc_key);
    }

    pub fn searchRuntimeDenseVectorIdsForOrdinals(
        self: *DB,
        alloc: Allocator,
        index_name: []const u8,
        ordinals: []const u32,
    ) ![]u64 {
        return try search_runtime_impl.denseVectorIdsForOrdinals(self, alloc, index_name, ordinals);
    }

    pub fn searchRuntimeAllDocsVisibleFast(self: *DB, generation: ?u64) !bool {
        return try search_runtime_impl.allDocsVisibleFast(self, generation);
    }

    pub fn searchRuntimeLookupLiveDocOrdinalNoLock(
        self: *DB,
        alloc: Allocator,
        doc_id: []const u8,
        generation: ?u64,
    ) !?doc_set.DocOrdinal {
        return try search_runtime_impl.lookupLiveDocOrdinalNoLock(self, alloc, doc_id, generation);
    }

    pub fn searchRuntimeLookupLiveDocOrdinalsNoLock(
        self: *DB,
        alloc: Allocator,
        doc_ids: []const []const u8,
        generation: ?u64,
    ) ![]?doc_set.DocOrdinal {
        return try search_runtime_impl.lookupLiveDocOrdinalsNoLock(self, alloc, doc_ids, generation);
    }

    pub fn searchRuntimeDenseOrdinalsForVectorIds(
        self: *DB,
        alloc: Allocator,
        index_name: []const u8,
        vector_ids: []const u64,
        generation: ?u64,
    ) ![]?doc_set.DocOrdinal {
        return try search_runtime_impl.denseOrdinalsForVectorIds(self, alloc, index_name, vector_ids, generation);
    }

    pub fn searchRuntimeSparseDocNumsForOrdinals(
        self: *DB,
        alloc: Allocator,
        index_name: []const u8,
        ordinals: []const u32,
    ) ![]const u32 {
        return try search_runtime_impl.sparseDocNumsForOrdinals(self, alloc, index_name, ordinals);
    }

    pub fn searchRuntimeLoadRequiredProjectedSearchDocument(
        self: *DB,
        alloc: Allocator,
        req: types.SearchRequest,
        key: []const u8,
    ) ![]u8 {
        return try search_runtime_impl.loadRequiredProjectedSearchDocument(self, alloc, req, key);
    }

    pub fn searchRuntimeLoadProjectedSearchDocumentMany(
        self: *DB,
        alloc: Allocator,
        req: types.SearchRequest,
        keys: []const []const u8,
    ) ![]?[]u8 {
        return try search_runtime_impl.loadProjectedSearchDocumentMany(self, alloc, req, keys);
    }

    pub fn searchRuntimeIsVisibleSearchHit(self: *DB, alloc: Allocator, hit: types.SearchHit) !bool {
        return try search_runtime_impl.isVisibleSearchHit(self, alloc, hit);
    }

    pub fn searchRuntimeFilterVisibleSearchHitsMany(
        self: *DB,
        alloc: Allocator,
        hits: []const types.SearchHit,
    ) ![]bool {
        return try search_runtime_impl.filterVisibleSearchHitsMany(self, alloc, hits);
    }

    pub fn searchRuntimeIsVisibleNonChunkSearchHit(self: *DB, alloc: Allocator, hit: types.SearchHit) !bool {
        return try search_runtime_impl.isVisibleNonChunkSearchHit(self, alloc, hit);
    }

    pub fn searchRuntimeResolveChunkParentId(self: *DB, alloc: Allocator, hit: types.SearchHit) ![]u8 {
        return try search_runtime_impl.resolveChunkParentId(self, alloc, hit);
    }

    pub fn searchRuntimeLoadParentStoredForSearch(
        self: *DB,
        alloc: Allocator,
        req: types.SearchRequest,
        parent_id: []const u8,
    ) !?[]u8 {
        return try search_runtime_impl.loadParentStoredForSearch(self, alloc, req, parent_id);
    }

    pub fn searchRuntimeLoadParentStoredForSearchMany(
        self: *DB,
        alloc: Allocator,
        req: types.SearchRequest,
        parent_ids: []const []const u8,
    ) ![]?[]u8 {
        return try search_runtime_impl.loadParentStoredForSearchMany(self, alloc, req, parent_ids);
    }

    pub fn searchRuntimePostprocessVectorSearchResult(
        self: *DB,
        alloc: Allocator,
        req: types.SearchRequest,
        raw: types.SearchResult,
        chunk_backed: bool,
    ) !types.SearchResult {
        return try search_runtime_impl.postprocessVectorSearchResult(self, alloc, req, raw, chunk_backed);
    }

    pub fn searchRuntimeHbcSearch(
        self: *DB,
        entry: *index_manager_mod.IndexManager.DenseIndex,
        req: vectorindex_mod.SearchRequest,
    ) !vectorindex_mod.SearchResults {
        return try search_runtime_impl.hbcSearch(self, entry, req);
    }

    pub fn searchRuntimeHbcSearchProfiled(
        self: *DB,
        entry: *index_manager_mod.IndexManager.DenseIndex,
        req: vectorindex_mod.SearchRequest,
    ) !vectorindex_mod.ProfiledSearchResults {
        return try search_runtime_impl.hbcSearchProfiled(self, entry, req);
    }

    pub fn searchRuntimeHasRelationalBaseRows(self: *DB) bool {
        return search_runtime_impl.hasRelationalBaseRows(self);
    }

    pub fn searchRuntimeIsMetadataKey(self: *DB, key: []const u8) bool {
        return search_runtime_impl.isMetadataKey(self, key);
    }

    pub fn searchRuntimeScanStoreRange(
        self: *DB,
        alloc: Allocator,
        lower: []const u8,
        upper: []const u8,
    ) ![]docstore_mod.OwnedKVPair {
        return try search_runtime_impl.scanStoreRange(self, alloc, lower, upper);
    }

    pub fn searchRuntimeIsExpiredDocumentKey(self: *DB, alloc: Allocator, key: []const u8) !bool {
        return try search_runtime_impl.isExpiredDocumentKey(self, alloc, key);
    }

    pub fn searchRuntimeTtlDurationNs(self: *DB) u64 {
        return search_runtime_impl.ttlDurationNs(self);
    }

    pub fn searchRuntimeLoadDocumentTimestampsMany(self: *DB, alloc: Allocator, keys: []const []const u8) ![]u64 {
        return try search_runtime_impl.loadDocumentTimestampsMany(self, alloc, keys);
    }

    pub fn searchRuntimeLookupLiveDocOrdinal(
        self: *DB,
        alloc: Allocator,
        doc_id: []const u8,
        generation: ?u64,
    ) !?doc_set.DocOrdinal {
        return try search_runtime_impl.lookupLiveDocOrdinal(self, alloc, doc_id, generation);
    }

    pub fn searchRuntimeLoadStoredSearchDocument(self: *DB, alloc: Allocator, key: []const u8) !?[]u8 {
        return try search_runtime_impl.loadStoredSearchDocument(self, alloc, key);
    }

    pub fn searchRuntimeLoadStoredSearchDocumentMany(self: *DB, alloc: Allocator, keys: []const []const u8) ![]?[]u8 {
        return try search_runtime_impl.loadStoredSearchDocumentMany(self, alloc, keys);
    }

    pub fn searchRuntimeExecuteSearchGraphQuery(
        self: *DB,
        alloc: Allocator,
        graph_query: search_runtime.GraphQuery,
        start_key_refs: []const []const u8,
        target_keys: [][]u8,
    ) !search_runtime.GraphQueryResult {
        return try search_runtime_impl.executeSearchGraphQuery(self, alloc, graph_query, start_key_refs, target_keys);
    }

    pub fn searchRuntimeAnnotateSearchHitOrdinalsNoLock(
        self: *DB,
        alloc: Allocator,
        req: types.SearchRequest,
        hits: []types.SearchHit,
    ) !void {
        try search_runtime_impl.annotateSearchHitOrdinalsNoLock(self, alloc, req, hits);
    }

    pub fn searchRuntimeFilterExpiredSearchResult(
        self: *DB,
        alloc: Allocator,
        raw: types.SearchResult,
    ) !types.SearchResult {
        return try search_runtime_impl.filterExpiredSearchResult(self, alloc, raw);
    }

    pub fn collectSearchRequestTextStats(self: *DB, alloc: Allocator, req: types.SearchRequest) ![]const search_runtime.TextFieldStats {
        return try search_runtime_impl.collectSearchRequestTextStats(self, alloc, req);
    }

    pub fn preflightSearchRequest(self: *DB, alloc: Allocator, req: types.SearchRequest, max_work: u32) !search_runtime.RuntimePreflightSummary {
        return try search_runtime_impl.preflightSearchRequest(self, alloc, req, max_work);
    }

    pub fn preflightSearchRequestWithExecutionContext(
        self: *DB,
        alloc: Allocator,
        req: types.SearchRequest,
        max_work: u32,
        exec_ctx: types.ExecutionContext,
    ) !search_runtime.RuntimePreflightSummary {
        return try search_runtime_impl.preflightSearchRequestWithExecutionContext(self, alloc, req, max_work, exec_ctx);
    }

    pub fn collectPlanningStats(
        self: *DB,
        alloc: Allocator,
        req: types.SearchRequest,
        max_work: u32,
    ) !planning_stats_mod.PlanningStatsSummary {
        return try search_runtime_impl.collectPlanningStats(self, alloc, req, max_work);
    }

    pub fn collectPlanningStatsWithExecutionContext(
        self: *DB,
        alloc: Allocator,
        req: types.SearchRequest,
        max_work: u32,
        exec_ctx: types.ExecutionContext,
    ) !planning_stats_mod.PlanningStatsSummary {
        return try search_runtime_impl.collectPlanningStatsWithExecutionContext(self, alloc, req, max_work, exec_ctx);
    }

    pub fn planningStatsProvider(self: *DB) planning_stats_mod.PlanningStatsProvider {
        return search_runtime_impl.planningStatsProvider(self);
    }

    pub fn collectExplicitTextStats(self: *DB, alloc: Allocator, requests: []const search_runtime.ExplicitTextStatRequest) ![]const search_runtime.TextFieldStats {
        return try search_runtime_impl.collectExplicitTextStats(self, alloc, requests);
    }

    pub fn collectExplicitBackgroundTextStats(
        self: *DB,
        alloc: Allocator,
        requests: []const search_runtime.ExplicitBackgroundTextStatRequest,
    ) ![]const aggregations_mod.DistributedBackgroundTextStats {
        return try search_runtime_impl.collectExplicitBackgroundTextStats(self, alloc, requests);
    }

    pub fn searchDenseProfiled(self: *DB, alloc: Allocator, req: types.SearchRequest, dense: types.DenseKnnQuery) !search_runtime.ProfiledDenseSearchResult {
        return try search_runtime_impl.searchDenseProfiled(self, alloc, req, dense);
    }

    pub fn lookupLiveDocOrdinalForInternalRead(
        self: *DB,
        alloc: Allocator,
        doc_id: []const u8,
        generation: ?u64,
    ) !?doc_set.DocOrdinal {
        return try internal_impl.lookupLiveDocOrdinalNoLock(self, alloc, doc_id, generation);
    }
};
