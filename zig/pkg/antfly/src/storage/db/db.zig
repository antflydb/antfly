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
const Io = std.Io;
const common_secrets = @import("../../common/secrets.zig");
const backend_types = @import("../backend_types.zig");
const docstore_mod = @import("../docstore.zig");
const db_config = @import("config.zig");
const apply_rw_lock_mod = @import("apply_rw_lock.zig");
const db_core = @import("core.zig");
const split_restore = @import("split_restore.zig");
const db_internal = @import("internal.zig");
const lifecycle_mod = @import("lifecycle.zig");
const ha_replication = @import("ha_replication.zig");
const write_path = @import("write_path.zig");
const db_transactions = @import("transactions.zig");
const schema_runtime = @import("schema_runtime.zig");
const relational_integrity = @import("relational_integrity.zig");
const relational_rows = @import("relational_rows.zig");
const search_runtime = @import("search_runtime.zig");
const internal_keys = @import("../internal_keys.zig");
const doc_identity = @import("doc_identity.zig");
const doc_set = @import("doc_set.zig");
const index_manager_mod = @import("catalog/index_manager.zig");
const resolver_catalog_mod = @import("catalog/resolver_catalog.zig");
const resolution_runtime_mod = @import("resolution_runtime.zig");
const promotion_runtime_mod = @import("promotion_runtime.zig");
const resolver_lib = @import("antfly_resolver");
const backfill_state_mod = @import("backfill_state.zig");
const types = @import("types.zig");
const aggregations_mod = @import("aggregations.zig");
const algebraic_mod = @import("algebraic/mod.zig");
const artifact_ids = @import("artifact_ids.zig");
const apply_state = @import("derived/apply_state.zig");
const change_journal_mod = @import("derived/change_journal.zig");
const ha_replication_record_mod = @import("../ha/replication_record.zig");
const replay_stream_mod = @import("derived/replay_stream.zig");
const derived_types = @import("derived/derived_types.zig");
const derived_executor_mod = @import("derived/derived_executor.zig");
const derived_async = @import("derived_async.zig");
const background_runtime_mod = @import("../background_runtime.zig");
const hbc_mod = @import("../hbc_adapter.zig");
const vectorindex_mod = @import("antfly_vectorindex");
const embedder_mod = @import("enrichment/embedder.zig");
const asset_producer_mod = @import("enrichment/asset_producer.zig");
const document_extraction_mod = @import("enrichment/document_extraction.zig");
const enrichment_runtime_mod = @import("enrichment/enrichment_runtime.zig");
const enrichment_types = @import("enrichment/enrichment_types.zig");
const enrichment_artifact_codec = @import("enrichment/artifact_codec.zig");
const lsm_backend_mod = @import("../lsm_backend/mod.zig");
const resource_manager_mod = @import("../resource_manager.zig");
const schema_mod = @import("../schema.zig");
const schema_api_mod = @import("../../schema/mod.zig");
const transactions_mod = @import("../transactions.zig");
const scraping = if (builtin.os.tag == .freestanding or build_options.bench_minimal_deps)
    @import("scraping_stub.zig")
else
    @import("antfly_scraping");
const graph_mod = @import("../../graph/graph.zig");
const traversal_mod = @import("../../graph/traversal.zig");
const paths_mod = @import("../../graph/paths.zig");
const graph_query_mod = @import("../../graph/query.zig");
const graph_pattern_mod = @import("../../graph/pattern.zig");
const search_mod = @import("../../search/search.zig");
const mapper = @import("document_mapper.zig");
const metadata_table_manager = @import("../../metadata/table_manager.zig");
const relational_store_mod = @import("relational_store.zig");
const planning_stats_mod = @import("planning_stats.zig");
const db_query_graph = @import("query/graph_exec.zig");
const db_query_projection = @import("query/projection.zig");
const db_query_search = @import("query/search_exec.zig");
const ttl_runtime_mod = @import("maintenance/ttl_runtime.zig");
const transaction_runtime_mod = @import("maintenance/transaction_runtime.zig");
const text_merge_runtime_mod = @import("maintenance/text_merge_runtime.zig");
const sparse_compaction_runtime_mod = @import("maintenance/sparse_compaction_runtime.zig");
const graph_metric_runtime_mod = @import("maintenance/graph_metric_runtime.zig");
const platform_clock = @import("../../platform/clock.zig");
const getenv = db_internal.getenv;
const readEnvUsize = db_internal.readEnvUsize;

pub const OpenOptions = lifecycle_mod.OpenOptions;
pub const OpenMode = lifecycle_mod.OpenOptions.OpenMode;
pub const ForeignKeyIntegrityReport = relational_store_mod.ForeignKeyIntegrityReport;
pub const ForeignKeyIntegrityViolation = relational_store_mod.ForeignKeyIntegrityViolation;
pub const ForeignKeyDeletePlan = relational_store_mod.ForeignKeyDeletePlan;
pub const UniqueConstraintIntegrityReport = relational_store_mod.UniqueConstraintIntegrityReport;
pub const local_schema_json_key = schema_runtime.local_schema_json_key;
pub const local_lite_sql_table_record_json_key = schema_runtime.local_lite_sql_table_record_json_key;
pub const HAAsyncEffectMirror = ha_replication.AsyncEffectMirror;
pub const HAAsyncBatchMirror = ha_replication.AsyncBatchMirror;
pub const HAAsyncMetadataMirror = ha_replication.AsyncMetadataMirror;
pub const HASyncWaitFn = ha_replication.SyncWaitFn;
pub const HAProgressPollFn = ha_replication.ProgressPollFn;
pub const HAPrimaryProgressSyncWait = ha_replication.PrimaryProgressSyncWait;
pub const HASessionSyncWait = ha_replication.SessionSyncWait;
pub const HAWriteGate = ha_replication.WriteGate;

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

fn notifyAsyncContextVisibilityHook(ptr: *anyopaque) void {
    const ctx: *AsyncContext = @ptrCast(@alignCast(ptr));
    if (ctx.query_visibility_hook) |hook| hook.notify(.invalidate);
}

const denseCatchUpFinishOptions = derived_async.denseCatchUpFinishOptions;
const denseCatchUpStartupCacheNodes = derived_async.denseCatchUpStartupCacheNodes;
const denseCatchUpStartupCacheVectors = derived_async.denseCatchUpStartupCacheVectors;
const EnrichmentAppendContext = db_internal.EnrichmentAppendContext(DB);

const BatchExecutionContext = db_internal.BatchExecutionContext(DB);

const ReplayApplyContext = db_internal.ReplayApplyContext(DB);

const TtlCleanupContext = db_internal.TtlCleanupContext(DB);

const AlgebraicDocFilterRequest = search_runtime.AlgebraicDocFilterRequest;

pub const BatchProfile = write_path.BatchProfile;
const BatchExecutionOptions = write_path.BatchExecutionOptions;

pub const OpenProfile = lifecycle_mod.OpenProfile;

const addBatchProfile = write_path.addBatchProfile;
const profileDelta = db_internal.profileDelta;
const logBatchProfile = write_path.logBatchProfile;

const threadedIo = db_internal.threadedIo;
const monotonicTimeNs = platform.time.monotonicNs;
const sleepNs = db_internal.sleepNs;
const sleepPollInterval = db_internal.sleepPollInterval;
const yieldToBackground = db_internal.yieldToBackground;

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
        pub const apply_derived_batch_to_index_async = lifecycleApplyDerivedBatchToIndexAsync;
        pub const persist_applied_sequence_async = lifecyclePersistAppliedSequenceAsync;
        pub const truncate_replay_sequence_async = lifecycleTruncateReplaySequenceAsync;
        pub const begin_derived_catch_up_session_async = lifecycleBeginDerivedCatchUpSessionAsync;
        pub const finish_derived_catch_up_session_async = lifecycleFinishDerivedCatchUpSessionAsync;
        pub const can_advance_derived_to_target_async = derivedAsyncCanAdvanceDerivedToTargetAsync;
        pub const append_derived_batch_from_enrichment = lifecycleAppendDerivedBatchFromEnrichment;
        pub const notify_derived_executor_sequence = lifecycleNotifyDerivedExecutorSequence;
        pub const delete_expired_documents_from_candidates = lifecycleDeleteExpiredDocumentsFromCandidates;
        pub const notify_async_context_visibility_hook = notifyAsyncContextVisibilityHook;
        pub const clear_bulk_ingest_seen_doc_keys_locked = lifecycleClearBulkIngestSeenDocKeysLocked;
        pub const deinit_bulk_ingest_coalescer = lifecycleDeinitBulkIngestCoalescer;
        pub const replay_pending_derived_batches = lifecycleReplayPendingDerivedBatches;
        pub const start_async_workers = lifecycleStartAsyncWorkers;
        pub const flush_applied_sequences_for_idle = lifecycleFlushAppliedSequencesForIdle;
        pub const wait_for_sync_level = lifecycleWaitForSyncLevel;
        pub const dense_index_rebuild_state_path_alloc = lifecycleDenseIndexRebuildStatePathAlloc;
        pub const set_dense_catch_up_progress = lifecycleSetDenseCatchUpProgress;
        pub const probe_derived_replay_target_sequence = lifecycle_mod.probeDerivedReplayTargetSequence;
        pub const lock_apply = lockApply;
        pub const populate_algebraic_index_stats = lifecyclePopulateAlgebraicIndexStats;
        pub const open_mode_requires_read_only_backends = lifecycle_mod.openModeRequiresReadOnlyBackends;
    };
    pub const DerivedAsyncCallbacks = struct {
        pub const dense_catch_up_finish_options = denseCatchUpFinishOptions;
        pub const apply_derived_batch_to_index_context = derivedAsyncApplyDerivedBatchToIndexContext;
        pub const apply_derived_batch_to_index_context_profiled = derivedAsyncApplyDerivedBatchToIndexContextProfiled;
        pub const save_index_status_snapshots = lifecycle_mod.saveIndexStatusSnapshots;
        pub const async_index_profile_enabled = db_internal.asyncIndexProfileEnabled;
        pub const replay_pending_derived_batches_context = derivedAsyncReplayPendingDerivedBatchesContext;
        pub const open_profile_enabled = lifecycle_mod.openProfileEnabled;
        pub const log_replay_catch_up_profile = derived_async.logReplayCatchUpProfile;
        pub const log_derived_worker_profile = derived_async.logDerivedWorkerProfile;
    };
    pub const WritePathCallbacks = struct {
        pub const Profile = BatchProfile;
        pub const Options = BatchExecutionOptions;
        pub const batch_internal = writePathBatchInternal;
        pub const open_mode_requires_read_only_backends = lifecycle_mod.openModeRequiresReadOnlyBackends;
        pub const enforce_ha_write_gate = writePathEnforceHAWriteGate;
        pub const preflight_ha_batch_sync_commit = writePathPreflightHABatchSyncCommit;
        pub const bench_metrics_enabled = db_internal.benchMetricsEnabled;
        pub const log_batch_profile = logBatchProfile;
        pub const monotonic_time_ns = monotonicTimeNs;
        pub const record_profile_ns = write_path.recordProfileNs;
        pub const lock_apply = lockApply;
        pub const append_row_claim_predicates_for_mutation_keys = db_transactions.appendRowClaimPredicatesForMutationKeys;
        pub const append_row_claim_predicates_for_identity_rewrites = db_transactions.appendRowClaimPredicatesForIdentityRewrites;
        pub const reclaim_expired_row_claim_intents_for_mutation_keys = db_transactions.reclaimExpiredRowClaimIntentsForMutationKeys;
        pub const reclaim_expired_row_claim_intents_for_identity_rewrites = db_transactions.reclaimExpiredRowClaimIntentsForIdentityRewrites;
        pub const relational_column_index_policy_for_store = writePathRelationalColumnIndexPolicyForStore;
        pub const relational_columns_for_store = relationalColumnsForStore;
        pub const record_foreign_key_child_write_reject = writePathRecordForeignKeyChildWriteReject;
        pub const record_foreign_key_parent_delete_reject = writePathRecordForeignKeyParentDeleteReject;
        pub const is_metadata_key = db_internal.isMetadataKey;
        pub const augment_extracted_write_with_graph_field_edges = write_path.augmentExtractedWriteWithGraphFieldEdges;
        pub const should_write_timestamp = db_internal.shouldWriteTimestamp;
        pub const resolve_write_timestamp_ns = writePathResolveWriteTimestampNs;
        pub const make_timestamp_key = db_internal.makeTimestampKey;
        pub const encode_timestamp_value = db_internal.encodeTimestampValue;
        pub const append_system_versioned_history_for_batch = db_transactions.appendSystemVersionedHistoryForBatch;
        pub const split_shadow_requires_materialized_derived_batch = writePathSplitShadowRequiresMaterializedDerivedBatch;
        pub const encode_thin_replay_record_payload = write_path.encodeThinReplayRecordPayload;
        pub const append_precomputed_graph_source_artifacts = writePathAppendPrecomputedGraphSourceArtifacts;
        pub const graph_writes_from_artifact_value_alloc = derived_async.graphWritesFromArtifactValueAlloc;
        pub const free_graph_writes = derived_async.freeGraphWrites;
        pub const resolution_mention_state_keys_for_graph_source_alloc = derived_async.resolutionMentionStateKeysForGraphSourceAlloc;
        pub const attach_inline_upsert_document_values = derived_async.attachInlineUpsertDocumentValues;
        pub const apply_derived_batch_to_shadow_if_needed = writePathApplyDerivedBatchToShadowIfNeeded;
        pub const collect_managed_sync_targets = writePathCollectManagedSyncTargets;
        pub const encode_change_record_payload = writePathEncodeChangeRecordPayload;
        pub const encode_change_record_payload_context = writePathEncodeChangeRecordPayloadContext;
        pub const mirror_ha_batch_mutation_commit = writePathMirrorHABatchMutationCommit;
        pub const mirror_ha_replay_payload_commit = writePathMirrorHAReplayPayloadCommit;
        pub const mirror_ha_replay_payload_best_effort = writePathMirrorHAReplayPayloadBestEffort;
        pub const should_append_split_delta = writePathShouldAppendSplitDelta;
        pub const current_time_ns = db_internal.currentTimeNs;
        pub const mark_precomputed_enrichment_applied_for_sync = writePathMarkPrecomputedEnrichmentAppliedForSync;
        pub const mark_precomputed_enrichment_applied_for_sync_context = writePathMarkPrecomputedEnrichmentAppliedForSyncContext;
        pub const apply_derived_backlog_pressure = writePathApplyDerivedBacklogPressure;
        pub const apply_derived_backlog_pressure_context = writePathApplyDerivedBacklogPressureContext;
        pub const notify_executor_for_sync_level_with_dense_bulk_deferral = db_internal.notifyExecutorForSyncLevelWithDenseBulkDeferral;
        pub const wait_for_sync_level = writePathWaitForSyncLevel;
        pub const wait_for_sync_level_context = writePathWaitForSyncLevelContext;
        pub const sync_level_requires_derived_visibility = writePathSyncLevelRequiresDerivedVisibility;
        pub const apply_derived_batch = writePathApplyDerivedBatch;
        pub const apply_derived_batch_targets = writePathApplyDerivedBatchTargets;
        pub const apply_derived_batch_context = writePathApplyDerivedBatchContext;
        pub const apply_derived_batch_targets_context = writePathApplyDerivedBatchTargetsContext;
        pub const apply_derived_batch_profiled = writePathApplyDerivedBatchProfiled;
        pub const apply_derived_batch_targets_profiled = writePathApplyDerivedBatchTargetsProfiled;
        pub const notify_resolver_replay_runtimes = writePathNotifyResolverReplayRuntimes;
        pub const notify_resolver_replay_runtimes_for_catalog = writePathNotifyResolverReplayRuntimesForCatalog;
    };
    pub const SchemaRuntimeCallbacks = struct {
        pub const hydrate_algebraic_observation_status_for_index_best_effort = schemaRuntimeHydrateAlgebraicObservationStatusForIndexBestEffort;
        pub const replay_generated_enrichments_from_stored_docs = replayGeneratedEnrichmentsFromStoredDocs;
        pub const append_generated_enrichments = schemaRuntimeAppendGeneratedEnrichments;
        pub const append_derived_batch_record = derivedAsyncAppendDerivedBatchRecord;
        pub const save_index_status_snapshot = schemaRuntimeSaveIndexStatusSnapshot;
        pub const notify_resolver_replay_runtimes = schemaRuntimeNotifyResolverReplayRuntimes;
        pub const mirror_ha_schema_metadata_commit = schemaRuntimeMirrorHASchemaMetadataCommit;
    };
    pub const HAReplicationCallbacks = struct {
        pub const batch_replicated_apply_with_marker = haReplicationBatchReplicatedApplyWithMarker;
        pub const apply_ha_derived_effect_record = applyHADerivedEffectRecord;
    };

    fn lifecycleApplyDerivedBatchToIndexAsync(
        ctx_ptr: *anyopaque,
        derived_batch: derived_types.DerivedBatch,
        index_ref: index_manager_mod.ManagedIndexRef,
    ) !bool {
        return try derived_async_impl.applyDerivedBatchToIndexAsync(ctx_ptr, derived_batch, index_ref);
    }

    fn lifecyclePersistAppliedSequenceAsync(ctx_ptr: *anyopaque, index_name: []const u8, sequence: u64, force: bool) !bool {
        return try derived_async_impl.persistAppliedSequenceAsync(ctx_ptr, index_name, sequence, force);
    }

    fn lifecycleTruncateReplaySequenceAsync(ctx_ptr: *anyopaque, sequence: u64) !void {
        return try derived_async_impl.truncateReplaySequenceAsync(ctx_ptr, sequence);
    }

    fn lifecycleBeginDerivedCatchUpSessionAsync(ctx_ptr: *anyopaque, index_ref: index_manager_mod.ManagedIndexRef) !void {
        return try derived_async_impl.beginDerivedCatchUpSessionAsync(ctx_ptr, index_ref);
    }

    fn lifecycleFinishDerivedCatchUpSessionAsync(ctx_ptr: *anyopaque, index_ref: index_manager_mod.ManagedIndexRef, success: bool) !void {
        return try derived_async_impl.finishDerivedCatchUpSessionAsync(ctx_ptr, index_ref, success);
    }

    fn lifecycleAppendDerivedBatchFromEnrichment(ctx_ptr: *anyopaque, derived_batch: derived_types.DerivedBatch) !u64 {
        return try derived_async_impl.appendDerivedBatchFromEnrichment(ctx_ptr, derived_batch);
    }

    fn lifecycleNotifyDerivedExecutorSequence(ctx_ptr: *anyopaque, sequence: u64) void {
        derived_async_impl.notifyDerivedExecutorSequence(ctx_ptr, sequence);
    }

    fn lifecycleDeleteExpiredDocumentsFromCandidates(ctx_ptr: *anyopaque, candidates: []const ttl_runtime_mod.DeleteCandidate) !u32 {
        return try write_path_impl.deleteExpiredDocumentsFromCandidates(ctx_ptr, candidates);
    }

    fn lifecycleClearBulkIngestSeenDocKeysLocked(self: *DB) void {
        write_path_impl.clearBulkIngestSeenDocKeysLocked(self);
    }

    fn lifecycleDeinitBulkIngestCoalescer(self: *DB) void {
        write_path_impl.deinitBulkIngestCoalescer(self);
    }

    fn lifecycleReplayPendingDerivedBatches(
        self: *DB,
        progress_ctx: ?*anyopaque,
        progress_hook: ?ReplayProgressHook,
    ) !void {
        return try derived_async_impl.replayPendingDerivedBatches(self, progress_ctx, progress_hook);
    }

    fn lifecycleStartAsyncWorkers(self: *DB) !void {
        return try lifecycle_impl.startAsyncWorkers(self);
    }

    fn lifecycleFlushAppliedSequencesForIdle(self: *DB) !void {
        return try derived_async_impl.flushAppliedSequencesForIdle(self);
    }

    fn lifecycleWaitForSyncLevel(
        self: *DB,
        sync_level: types.SyncLevel,
        sequence: u64,
        sync_targets: ManagedSyncTargets,
    ) !void {
        return try derived_async_impl.waitForSyncLevel(self, sync_level, sequence, sync_targets);
    }

    fn lifecycleDenseIndexRebuildStatePathAlloc(self: *DB, alloc: Allocator, index_name: []const u8) ![]u8 {
        return try derived_async_impl.denseIndexRebuildStatePathAlloc(self, alloc, index_name);
    }

    fn lifecycleSetDenseCatchUpProgress(ctx: *AsyncContext, progress: ReplayProgress) void {
        derived_async_impl.setDenseCatchUpProgress(ctx, progress);
    }

    fn lifecyclePopulateAlgebraicIndexStats(
        self: *DB,
        alloc: Allocator,
        index_name: []const u8,
        item: *types.DBIndexStats,
        include_adaptive_scans: bool,
    ) !void {
        return try lifecycle_impl.populateAlgebraicIndexStats(self, alloc, index_name, item, include_adaptive_scans);
    }

    fn derivedAsyncApplyDerivedBatchToIndexContext(
        ctx: *const AsyncContext,
        derived_batch: derived_types.DerivedBatch,
        index_ref: index_manager_mod.ManagedIndexRef,
    ) !void {
        return try derived_async_impl.applyDerivedBatchToIndexContext(ctx, derived_batch, index_ref);
    }

    fn derivedAsyncApplyDerivedBatchToIndexContextProfiled(
        ctx: *const AsyncContext,
        derived_batch: derived_types.DerivedBatch,
        index_ref: index_manager_mod.ManagedIndexRef,
        profile: ?*BatchProfile,
    ) !void {
        return try derived_async_impl.applyDerivedBatchToIndexContextProfiled(ctx, derived_batch, index_ref, profile);
    }

    fn derivedAsyncReplayPendingDerivedBatchesContext(ctx: *const BatchExecutionContext) !void {
        return try derived_async_impl.replayPendingDerivedBatchesContext(ctx);
    }

    fn schemaRuntimeHydrateAlgebraicObservationStatusForIndexBestEffort(self: *DB, index_name: []const u8) void {
        lifecycle_impl.hydrateAlgebraicObservationStatusForIndexBestEffort(self, index_name);
    }

    fn schemaRuntimeAppendGeneratedEnrichments(
        self: *DB,
        derived_batch_out: *derived_types.DerivedBatch,
        req: types.BatchRequest,
        extracted: []const mapper.ExtractedWrite,
    ) !void {
        return try write_path_impl.appendGeneratedEnrichments(self, derived_batch_out, req, extracted);
    }

    fn schemaRuntimeNotifyResolverReplayRuntimes(self: *DB, sequence: u64) void {
        lifecycle_impl.notifyResolverReplayRuntimes(self, sequence);
    }

    fn schemaRuntimeSaveIndexStatusSnapshot(self: *DB, index_name: []const u8, sequence: u64) !void {
        return try lifecycle_mod.saveIndexStatusSnapshots(self.alloc, self.core.store, self.core.index_manager, &[_]apply_state.AppliedSequenceUpdate{.{
            .index_name = index_name,
            .sequence = sequence,
        }});
    }

    fn schemaRuntimeMirrorHASchemaMetadataCommit(self: *DB, table_schema: schema_mod.TableSchema) !void {
        return try ha_replication_impl.mirrorDBSchemaMetadataCommit(self, table_schema);
    }

    fn writePathBatchInternal(
        self: *DB,
        req: types.BatchRequest,
        profile: ?*BatchProfile,
        opts: BatchExecutionOptions,
    ) anyerror!void {
        return try write_path_impl.batchInternal(self, req, profile, opts);
    }

    fn writePathResolveWriteTimestampNs(self: *DB, fallback_timestamp_ns: u64, value_json: []const u8) !u64 {
        return try internal_impl.resolveWriteTimestampNs(self, fallback_timestamp_ns, value_json);
    }

    fn writePathEnforceHAWriteGate(self: *DB) !void {
        return try ha_replication_impl.enforceDBWriteGate(self);
    }

    fn writePathPreflightHABatchSyncCommit(self: *DB) !void {
        return try ha_replication_impl.preflightDBBatchSyncCommit(self);
    }

    fn writePathMirrorHABatchMutationCommit(self: *DB, request: types.BatchRequest) !void {
        return try ha_replication_impl.mirrorDBBatchMutationCommit(self, request);
    }

    fn writePathMirrorHAReplayPayloadCommit(self: *DB, payload: []const u8) !void {
        return try ha_replication_impl.mirrorDBReplayPayloadCommit(self, payload);
    }

    fn writePathMirrorHAReplayPayloadBestEffort(self: *DB, payload: []const u8) void {
        ha_replication_impl.mirrorDBReplayPayloadBestEffort(self, payload);
    }

    fn writePathApplyDerivedBatchToShadowIfNeeded(self: *DB, derived_batch: derived_types.DerivedBatch) !void {
        return try derived_async_impl.applyDerivedBatchToShadowIfNeeded(self, derived_batch);
    }

    fn writePathCollectManagedSyncTargets(
        alloc: Allocator,
        index_manager: *index_manager_mod.IndexManager,
        derived_batch: derived_types.DerivedBatch,
    ) !ManagedSyncTargets {
        return try derived_async_impl.collectManagedSyncTargets(alloc, index_manager, derived_batch);
    }

    fn writePathEncodeChangeRecordPayload(
        self: *DB,
        derived_batch: derived_types.DerivedBatch,
        sequence: u64,
    ) ![]u8 {
        return try derived_async_impl.encodeChangeRecordPayloadForDB(self, derived_batch, sequence);
    }

    fn writePathEncodeChangeRecordPayloadContext(
        ctx: *const BatchExecutionContext,
        derived_batch: derived_types.DerivedBatch,
        sequence: u64,
    ) ![]u8 {
        return try derived_async_impl.encodeChangeRecordPayload(ctx, derived_batch, sequence);
    }

    fn writePathShouldAppendSplitDelta(self: *DB) bool {
        return split_restore_impl.shouldAppendSplitDelta(self);
    }

    fn writePathMarkPrecomputedEnrichmentAppliedForSync(
        self: *DB,
        sync_level: types.SyncLevel,
        sequence: u64,
    ) !void {
        return try lifecycle_impl.markPrecomputedEnrichmentAppliedForSync(self, sync_level, sequence);
    }

    fn writePathMarkPrecomputedEnrichmentAppliedForSyncContext(
        ctx: *const BatchExecutionContext,
        sync_level: types.SyncLevel,
        sequence: u64,
    ) !void {
        return try derived_async_impl.markPrecomputedEnrichmentAppliedForSyncContext(ctx, sync_level, sequence);
    }

    fn writePathApplyDerivedBacklogPressure(
        self: *DB,
        sequence: u64,
        sync_level: types.SyncLevel,
        sync_targets: ManagedSyncTargets,
    ) !void {
        return try derived_async_impl.applyDerivedBacklogPressure(self, sequence, sync_level, sync_targets);
    }

    fn writePathApplyDerivedBacklogPressureContext(
        ctx: *const BatchExecutionContext,
        sequence: u64,
        sync_level: types.SyncLevel,
        sync_targets: ManagedSyncTargets,
    ) !void {
        return try derived_async_impl.applyDerivedBacklogPressureContext(ctx, sequence, sync_level, sync_targets);
    }

    fn writePathWaitForSyncLevel(
        self: *DB,
        sync_level: types.SyncLevel,
        sequence: u64,
        sync_targets: ManagedSyncTargets,
    ) !void {
        return try derived_async_impl.waitForSyncLevel(self, sync_level, sequence, sync_targets);
    }

    fn writePathWaitForSyncLevelContext(
        ctx: *const BatchExecutionContext,
        sync_level: types.SyncLevel,
        sequence: u64,
        sync_targets: ManagedSyncTargets,
    ) !void {
        return try derived_async_impl.waitForSyncLevelContext(ctx, sync_level, sequence, sync_targets);
    }

    fn writePathSyncLevelRequiresDerivedVisibility(sync_level: types.SyncLevel) bool {
        return derived_async_impl.syncLevelRequiresDerivedVisibility(sync_level);
    }

    fn writePathApplyDerivedBatch(self: *DB, derived_batch: derived_types.DerivedBatch) !void {
        return try derived_async_impl.applyDerivedBatch(self, derived_batch);
    }

    fn writePathApplyDerivedBatchTargets(
        self: *DB,
        derived_batch: derived_types.DerivedBatch,
        index_names: []const []const u8,
    ) !void {
        return try derived_async_impl.applyDerivedBatchTargets(self, derived_batch, index_names);
    }

    fn writePathApplyDerivedBatchContext(
        ctx: *const BatchExecutionContext,
        derived_batch: derived_types.DerivedBatch,
    ) !void {
        return try derived_async_impl.applyDerivedBatchContext(ctx, derived_batch);
    }

    fn writePathApplyDerivedBatchTargetsContext(
        ctx: *const BatchExecutionContext,
        derived_batch: derived_types.DerivedBatch,
        index_names: []const []const u8,
    ) !void {
        return try derived_async_impl.applyDerivedBatchTargetsContext(ctx, derived_batch, index_names);
    }

    fn writePathApplyDerivedBatchProfiled(
        self: *DB,
        derived_batch: derived_types.DerivedBatch,
        profile: ?*BatchProfile,
    ) !void {
        return try derived_async_impl.applyDerivedBatchProfiled(self, derived_batch, profile);
    }

    fn writePathApplyDerivedBatchTargetsProfiled(
        self: *DB,
        derived_batch: derived_types.DerivedBatch,
        index_names: []const []const u8,
        profile: ?*BatchProfile,
    ) !void {
        return try derived_async_impl.applyDerivedBatchTargetsProfiled(self, derived_batch, index_names, profile);
    }

    fn writePathNotifyResolverReplayRuntimes(self: *DB, sequence: u64) void {
        lifecycle_impl.notifyResolverReplayRuntimes(self, sequence);
    }

    fn writePathNotifyResolverReplayRuntimesForCatalog(
        index_manager: *const index_manager_mod.IndexManager,
        resolution_runtime: ?*resolution_runtime_mod.ResolutionRuntime,
        promotion_runtime: ?*promotion_runtime_mod.PromotionRuntime,
        sequence: u64,
    ) void {
        derived_async_impl.notifyResolverReplayRuntimesForCatalog(
            index_manager,
            resolution_runtime,
            promotion_runtime,
            sequence,
        );
    }

    fn writePathRelationalColumnIndexPolicyForStore(self: *DB) relational_store_mod.ColumnIndexPolicy {
        return schema_runtime_impl.relationalColumnIndexPolicyForStore(self);
    }

    fn writePathRecordForeignKeyChildWriteReject(self: *DB) void {
        relational_integrity_impl.recordForeignKeyChildWriteReject(self);
    }

    fn writePathRecordForeignKeyParentDeleteReject(self: *DB) void {
        relational_integrity_impl.recordForeignKeyParentDeleteReject(self);
    }

    fn writePathSplitShadowRequiresMaterializedDerivedBatch(self: *DB) bool {
        return split_restore_impl.splitShadowRequiresMaterializedDerivedBatch(self);
    }

    fn writePathAppendPrecomputedGraphSourceArtifacts(
        self: *DB,
        artifact_writes: []const types.BatchWrite,
        artifact_delete_keys: []const []const u8,
        owned_graph_artifact_writes: *std.ArrayListUnmanaged(types.BatchWrite),
        store_writes: *std.ArrayListUnmanaged(docstore_mod.KVPair),
        delete_keys: *std.ArrayListUnmanaged([]const u8),
        owned_delete_keys: *std.ArrayListUnmanaged([]u8),
        changed_artifact_keys: *std.ArrayListUnmanaged([]u8),
    ) !void {
        return try write_path_impl.appendPrecomputedGraphSourceArtifacts(
            self,
            artifact_writes,
            artifact_delete_keys,
            owned_graph_artifact_writes,
            store_writes,
            delete_keys,
            owned_delete_keys,
            changed_artifact_keys,
        );
    }

    fn haReplicationBatchReplicatedApplyWithMarker(
        self: *DB,
        req: types.BatchRequest,
        applied_lsn_marker: ?u64,
    ) anyerror!void {
        return try write_path_impl.batchReplicatedApplyWithMarker(self, req, applied_lsn_marker);
    }

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

    fn enforceDurableMutationGate(self: *DB) !void {
        if (lifecycle_mod.openModeRequiresReadOnlyBackends(self.open_mode)) return error.ReadOnly;
        try ha_replication_impl.enforceDBWriteGate(self);
    }

    pub fn runTransactionRecoveryOnce(self: *DB, config: transaction_runtime_mod.Config) !types.TransactionRecoveryStats {
        try self.enforceDurableMutationGate();
        return try lifecycle_impl.runTransactionRecoveryOnce(self, config);
    }

    pub fn beginBulkIngestSession(self: *DB) !void {
        try self.enforceDurableMutationGate();
        return try write_path_impl.beginBulkIngestSessionAfterGate(self);
    }

    pub fn finishBulkIngestSessionWithOptions(self: *DB, options: backend_types.BulkIngestFinishOptions) !void {
        try self.enforceDurableMutationGate();
        return try write_path_impl.finishBulkIngestSessionWithOptionsAfterGate(self, options);
    }

    pub fn beginDenseAutoBulkIngestSession(self: *DB) !void {
        try self.enforceDurableMutationGate();
        return try write_path_impl.beginDenseAutoBulkIngestSessionAfterGate(self);
    }

    pub fn beginPrimaryStoreAutoBulkIngestSession(self: *DB) !void {
        try self.enforceDurableMutationGate();
        return try write_path_impl.beginPrimaryStoreAutoBulkIngestSessionAfterGate(self);
    }

    pub fn finishPrimaryStoreAutoBulkIngestSessionWithOptions(self: *DB, options: backend_types.BulkIngestFinishOptions) !void {
        try self.enforceDurableMutationGate();
        return try write_path_impl.finishPrimaryStoreAutoBulkIngestSessionWithOptionsAfterGate(self, options);
    }

    pub fn rollPrimaryStoreAutoBulkIngestSessionWithOptions(self: *DB, options: backend_types.BulkIngestFinishOptions) !void {
        try self.enforceDurableMutationGate();
        try write_path_impl.finishPrimaryStoreAutoBulkIngestSessionWithOptionsAfterGate(self, options);
        try write_path_impl.beginPrimaryStoreAutoBulkIngestSessionAfterGate(self);
    }

    pub fn finishDenseAutoBulkIngestSessionWithOptionsAndNotifyExecutor(
        self: *DB,
        options: backend_types.BulkIngestFinishOptions,
        notify_executor: bool,
    ) !void {
        try self.enforceDurableMutationGate();
        return try write_path_impl.finishDenseAutoBulkIngestSessionWithOptionsAfterGate(self, options, notify_executor);
    }

    pub fn finishDenseAutoBulkIngestSessionWithOptions(self: *DB, options: backend_types.BulkIngestFinishOptions) !void {
        try self.enforceDurableMutationGate();
        return try write_path_impl.finishDenseAutoBulkIngestSessionWithOptionsAfterGate(self, options, true);
    }

    pub fn rollDenseAutoBulkIngestSessionWithOptions(self: *DB, options: backend_types.BulkIngestFinishOptions) !void {
        try self.enforceDurableMutationGate();
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
        try self.enforceDurableMutationGate();
        return try lifecycle_impl.runLsmMaintenanceStep(self);
    }

    pub fn runPrimaryLsmMaintenanceStep(self: *DB) !bool {
        try self.enforceDurableMutationGate();
        return try lifecycle_impl.runPrimaryLsmMaintenanceStep(self);
    }

    pub fn runLsmMaintenanceStepBestEffort(self: *DB) !bool {
        try self.enforceDurableMutationGate();
        return try lifecycle_impl.runLsmMaintenanceStepBestEffort(self);
    }

    pub fn hasActiveDenseBulkWork(self: *const DB) bool {
        return asyncContextHasActiveDenseBulkWork(self.async_context);
    }

    pub fn runLsmMaintenanceUntilIdle(self: *DB) !usize {
        try self.enforceDurableMutationGate();
        return try lifecycle_impl.runLsmMaintenanceUntilIdle(self);
    }

    pub fn retryQuarantinedIndexLoads(self: *DB, force: bool) !index_manager_mod.IndexManager.QuarantineRetryResult {
        try self.enforceDurableMutationGate();
        return try lifecycle_impl.retryQuarantinedIndexLoads(self, force);
    }

    pub fn runDueLsmObsoleteReclaimUntilIdle(self: *DB, max_steps: usize) !usize {
        try self.enforceDurableMutationGate();
        return try lifecycle_impl.runDueLsmObsoleteReclaimUntilIdle(self, max_steps);
    }

    pub fn batch(self: *DB, req: types.BatchRequest) anyerror!void {
        try self.enforceDurableMutationGate();
        return try write_path_impl.batchAfterGate(self, req);
    }

    pub fn batchProfiled(self: *DB, req: types.BatchRequest, profile: *BatchProfile) anyerror!void {
        try self.enforceDurableMutationGate();
        return try write_path_impl.batchProfiledAfterGate(self, req, profile);
    }

    pub fn batchWithDocumentArtifactChildRangeDispatcher(
        self: *DB,
        req: types.BatchRequest,
        dispatcher: DocumentArtifactChildRangeDispatcher,
    ) anyerror!void {
        try self.enforceDurableMutationGate();
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
        try self.enforceDurableMutationGate();
        return try write_path_impl.drainDocumentArtifactChildRangeOutboxAfterGate(self, dispatcher, limit);
    }

    pub fn batchWithoutRangeValidation(self: *DB, req: types.BatchRequest) anyerror!void {
        try self.enforceDurableMutationGate();
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
        try self.enforceDurableMutationGate();
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
        try self.enforceDurableMutationGate();
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
        try self.enforceDurableMutationGate();
        return try write_path_impl.updateDocumentArtifactChildRangePlacementAfterGate(self, alloc, doc_key, artifact_name, update);
    }

    pub fn reprocessDocumentArtifact(
        self: *DB,
        alloc: Allocator,
        doc_key: []const u8,
        artifact_name: []const u8,
    ) !bool {
        try self.enforceDurableMutationGate();
        return try write_path_impl.reprocessDocumentArtifactAfterGate(self, alloc, doc_key, artifact_name);
    }

    pub fn reprocessDocumentArtifactRange(
        self: *DB,
        alloc: Allocator,
        artifact_name: []const u8,
        req: types.DocumentArtifactTableReprocessRequest,
    ) !types.DocumentArtifactTableReprocessResult {
        try self.enforceDurableMutationGate();
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
        try self.enforceDurableMutationGate();
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
        try self.enforceDurableMutationGate();
        try split_restore_impl.setSplitState(self, state);
    }

    pub fn clearSplitState(self: *DB) !void {
        try self.enforceDurableMutationGate();
        try split_restore_impl.clearSplitState(self);
    }

    pub fn getSplitDeltaSeq(self: *DB) u64 {
        return split_restore_impl.getSplitDeltaSeq(self);
    }

    pub fn getSplitDeltaFinalSeq(self: *DB, alloc: Allocator) !u64 {
        return try split_restore_impl.getSplitDeltaFinalSeq(self, alloc);
    }

    pub fn setSplitDeltaFinalSeq(self: *DB, seq: u64) !void {
        try self.enforceDurableMutationGate();
        try split_restore_impl.setSplitDeltaFinalSeq(self, seq);
    }

    pub fn clearSplitDeltaFinalSeq(self: *DB) !void {
        try self.enforceDurableMutationGate();
        try split_restore_impl.clearSplitDeltaFinalSeq(self);
    }

    pub fn listSplitDeltaEntriesAfter(self: *DB, alloc: Allocator, after_seq: u64) ![]types.SplitDeltaEntry {
        return try split_restore_impl.listSplitDeltaEntriesAfter(self, alloc, after_seq);
    }

    pub fn clearSplitDeltaEntries(self: *DB) !void {
        try self.enforceDurableMutationGate();
        try split_restore_impl.clearSplitDeltaEntries(self);
    }

    pub fn createShadowIndexManager(self: *DB, split_key: []const u8, original_range_end: []const u8) !void {
        try self.enforceDurableMutationGate();
        try split_restore_impl.createShadowIndexManager(self, split_key, original_range_end);
    }

    pub fn closeShadowIndexManager(self: *DB) !void {
        try self.enforceDurableMutationGate();
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
        try self.enforceDurableMutationGate();
        try split_restore_impl.split(self, curr_range, split_key, dest_dir1, dest_dir2, prepare_only);
    }

    pub fn finalizeSplit(self: *DB, new_range: types.ByteRange) !void {
        try self.enforceDurableMutationGate();
        try split_restore_impl.finalizeSplit(self, new_range);
    }

    pub fn snapshot(self: *DB, id: []const u8) !u64 {
        try self.enforceDurableMutationGate();
        return try split_restore_impl.snapshot(self, id);
    }

    pub fn sync(self: *DB, full: bool) !void {
        try self.enforceDurableMutationGate();
        try lifecycle_impl.sync(self, full);
    }

    pub fn syncIndexes(self: *DB, force: bool) !void {
        try self.enforceDurableMutationGate();
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
        try self.enforceDurableMutationGate();
        return try split_restore_impl.repairRestoreRuntimeStateStepIfNeeded(self, alloc);
    }

    pub fn repairRestoreRuntimeStateIfNeeded(self: *DB, alloc: Allocator) !bool {
        try self.enforceDurableMutationGate();
        return try split_restore_impl.repairRestoreRuntimeStateIfNeeded(self, alloc);
    }

    pub fn clearDenseHbcCaches(self: *DB) void {
        schema_runtime_impl.clearDenseHbcCaches(self);
    }

    pub fn setSchema(self: *DB, table_schema: schema_mod.TableSchema) !void {
        try self.enforceDurableMutationGate();
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
        try self.enforceDurableMutationGate();
        try ha_replication_impl.preflightDBMetadataSyncCommit(self);
        try schema_runtime_impl.applyTableSchemaJsonAfterGate(self, alloc, schema_json, options);
    }

    pub const SchemaRewriteJobExecutionResult = schema_runtime.SchemaRewriteJobExecutionResult;
    pub const SchemaRewriteJobDrainOptions = schema_runtime.SchemaRewriteJobDrainOptions;

    fn enforceSchemaRewriteMutationGate(self: *DB) !void {
        try self.enforceDurableMutationGate();
    }

    pub fn drainSchemaRewriteJobsForIdle(
        self: *DB,
        alloc: Allocator,
        service: anytype,
        options: SchemaRewriteJobDrainOptions,
    ) !usize {
        try self.enforceSchemaRewriteMutationGate();
        return try schema_runtime_impl.drainSchemaRewriteJobsForIdle(self, alloc, service, options);
    }

    pub fn executeClaimedSchemaRewriteJob(
        self: *DB,
        alloc: Allocator,
        job: metadata_table_manager.SchemaRewriteJobRecord,
    ) !SchemaRewriteJobExecutionResult {
        try self.enforceSchemaRewriteMutationGate();
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
        try self.enforceDurableMutationGate();
        return try schema_runtime_impl.rebuildRelationalSecondaryIndexInRange(self, index_name, index_generation, lower_doc_key, upper_doc_key);
    }

    fn enforceRelationalIntegrityMutationGate(self: *DB) !void {
        try self.enforceDurableMutationGate();
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
        try self.enforceRelationalIntegrityMutationGate();
        return try relational_integrity_impl.repairForeignKeyRefsInRange(self, lower_doc_key, upper_doc_key);
    }

    pub fn repairForeignKeyRefsInRangeForConstraint(
        self: *DB,
        constraint_name: ?[]const u8,
        lower_doc_key: []const u8,
        upper_doc_key: []const u8,
    ) !ForeignKeyIntegrityReport {
        try self.enforceRelationalIntegrityMutationGate();
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
        try self.enforceRelationalIntegrityMutationGate();
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
        try self.enforceRelationalIntegrityMutationGate();
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
        try self.enforceRelationalIntegrityMutationGate();
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
        try self.enforceDurableMutationGate();
        try schema_runtime_impl.reloadAlgebraicSchemaConfigsAfterGate(self, schema_json);
    }

    pub fn schemaRuntimeStageAlgebraicSchemaConfigsPending(self: *DB, schema_json: []const u8) !void {
        if (schema_json.len == 0) return;
        try self.enforceDurableMutationGate();
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
        try self.enforceDurableMutationGate();
        try ha_replication_impl.preflightDBMetadataSyncCommit(self);
        try schema_runtime_impl.applyLiteSqlTableRecordAfterGate(self, alloc, table);
    }

    pub fn getLiteSqlTableRecordAlloc(self: *DB, alloc: Allocator) !?metadata_table_manager.TableRecord {
        return try schema_runtime_impl.getLiteSqlTableRecordAlloc(self, alloc);
    }

    fn enforceTransactionMutationGate(self: *DB) !void {
        try self.enforceDurableMutationGate();
    }

    pub fn beginTransaction(self: *DB, timestamp_ns: u64) !transactions_mod.TxnId {
        try self.enforceTransactionMutationGate();
        return try db_transactions_impl.beginTransaction(self, timestamp_ns);
    }

    pub fn beginTransactionWithId(self: *DB, txn_id: transactions_mod.TxnId, timestamp_ns: u64) !transactions_mod.TxnId {
        try self.enforceTransactionMutationGate();
        return try db_transactions_impl.beginTransactionWithId(self, txn_id, timestamp_ns);
    }

    pub fn beginTransactionWithParticipants(self: *DB, timestamp_ns: u64, participants: []const []const u8) !transactions_mod.TxnId {
        try self.enforceTransactionMutationGate();
        return try db_transactions_impl.beginTransactionWithParticipants(self, timestamp_ns, participants);
    }

    pub fn beginTransactionWithIdAndParticipants(self: *DB, txn_id: transactions_mod.TxnId, timestamp_ns: u64, participants: []const []const u8) !transactions_mod.TxnId {
        try self.enforceTransactionMutationGate();
        return try db_transactions_impl.beginTransactionWithIdAndParticipants(self, txn_id, timestamp_ns, participants);
    }

    pub fn writeIntents(
        self: *DB,
        txn_id: transactions_mod.TxnId,
        intents: []const transactions_mod.WriteIntent,
        predicates: []const transactions_mod.VersionPredicate,
    ) !void {
        try self.enforceTransactionMutationGate();
        return try db_transactions_impl.writeIntents(self, txn_id, intents, predicates);
    }

    pub fn writeTransaction(self: *DB, txn_id: types.TxnId, req: types.TransactionIntentRequest) !void {
        try self.enforceTransactionMutationGate();
        return try db_transactions_impl.writeTransaction(self, txn_id, req);
    }

    pub fn claimRowsForTransaction(
        self: *DB,
        txn_id: types.TxnId,
        row_keys: []const []const u8,
        claim: types.RowClaimRequest,
    ) !void {
        try self.enforceTransactionMutationGate();
        return try db_transactions_impl.claimRowsForTransaction(self, txn_id, row_keys, claim);
    }

    pub fn commitTransaction(self: *DB, txn_id: transactions_mod.TxnId, timestamp_ns: u64) !void {
        try self.enforceTransactionMutationGate();
        return try db_transactions_impl.commitTransaction(self, txn_id, timestamp_ns);
    }

    pub fn resolveTransactionIntents(self: *DB, txn_id: transactions_mod.TxnId, status: transactions_mod.TxnStatus, commit_version: u64) !void {
        try self.enforceTransactionMutationGate();
        return try db_transactions_impl.resolveTransactionIntents(self, txn_id, status, commit_version);
    }

    pub fn abortTransaction(self: *DB, txn_id: transactions_mod.TxnId, timestamp_ns: u64) !void {
        try self.enforceTransactionMutationGate();
        return try db_transactions_impl.abortTransaction(self, txn_id, timestamp_ns);
    }

    pub fn getTransactionStatus(self: *DB, txn_id: transactions_mod.TxnId) !transactions_mod.TxnStatus {
        return try db_transactions_impl.getTransactionStatus(self, txn_id);
    }

    pub fn getCommitVersion(self: *DB, txn_id: transactions_mod.TxnId) !u64 {
        return try db_transactions_impl.getCommitVersion(self, txn_id);
    }

    pub fn markTransactionParticipantResolved(self: *DB, txn_id: transactions_mod.TxnId, participant: []const u8) !void {
        try self.enforceTransactionMutationGate();
        return try db_transactions_impl.markTransactionParticipantResolved(self, txn_id, participant);
    }

    pub fn getTransactionParticipants(self: *DB, alloc: Allocator, txn_id: transactions_mod.TxnId) ![][]u8 {
        return try db_transactions_impl.getTransactionParticipants(self, alloc, txn_id);
    }

    pub fn getUnresolvedTransactionParticipants(self: *DB, alloc: Allocator, txn_id: transactions_mod.TxnId) ![][]u8 {
        return try db_transactions_impl.getUnresolvedTransactionParticipants(self, alloc, txn_id);
    }

    pub fn recoverTransactions(self: *DB, cutoff_timestamp: u64, resolution_timestamp: u64) !transactions_mod.RecoveryStats {
        try self.enforceTransactionMutationGate();
        return try db_transactions_impl.recoverTransactions(self, cutoff_timestamp, resolution_timestamp);
    }

    pub fn getEdges(
        self: *DB,
        alloc: Allocator,
        index_name: []const u8,
        key: []const u8,
        edge_type: []const u8,
        direction: graph_mod.EdgeDirection,
    ) ![]graph_mod.Edge {
        return try search_runtime_impl.getEdges(self, alloc, index_name, key, edge_type, direction);
    }

    pub fn traverseEdges(
        self: *DB,
        alloc: Allocator,
        index_name: []const u8,
        start_key: []const u8,
        rules: traversal_mod.TraversalRules,
    ) ![]traversal_mod.TraversalResult {
        return try search_runtime_impl.traverseEdges(self, alloc, index_name, start_key, rules);
    }

    pub fn getNeighbors(
        self: *DB,
        alloc: Allocator,
        index_name: []const u8,
        key: []const u8,
        edge_type: []const u8,
        direction: graph_mod.EdgeDirection,
    ) ![]traversal_mod.TraversalResult {
        return try search_runtime_impl.getNeighbors(self, alloc, index_name, key, edge_type, direction);
    }

    pub fn findShortestPath(
        self: *DB,
        alloc: Allocator,
        index_name: []const u8,
        source: []const u8,
        target: []const u8,
        edge_types: []const []const u8,
        direction: graph_mod.EdgeDirection,
        weight_mode: paths_mod.PathWeightMode,
        max_depth: u32,
        min_weight: f64,
        max_weight: f64,
    ) !?paths_mod.Path {
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
        direction: graph_mod.EdgeDirection,
        weight_mode: paths_mod.PathWeightMode,
        max_depth: u32,
        min_weight: f64,
        max_weight: f64,
    ) ![]paths_mod.Path {
        return try search_runtime_impl.findKShortestPaths(self, alloc, index_name, source, target, k, edge_types, direction, weight_mode, max_depth, min_weight, max_weight);
    }

    pub fn matchPattern(
        self: *DB,
        alloc: Allocator,
        index_name: []const u8,
        start_keys: []const []const u8,
        pattern: []const graph_pattern_mod.PatternStep,
        max_results: u32,
        return_aliases: []const []const u8,
    ) ![]graph_pattern_mod.PatternMatch {
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
        try self.enforceDurableMutationGate();
        return try schema_runtime_impl.addIndex(self, cfg);
    }

    pub fn addEnrichment(self: *DB, cfg: types.EnrichmentConfig) !void {
        try self.enforceDurableMutationGate();
        return try schema_runtime_impl.addEnrichment(self, cfg);
    }

    pub fn upsertEnrichment(self: *DB, cfg: types.EnrichmentConfig) !index_manager_mod.IndexManager.EnrichmentUpsertResult {
        try self.enforceDurableMutationGate();
        return try schema_runtime_impl.upsertEnrichment(self, cfg);
    }

    pub fn addResolver(self: *DB, cfg: index_manager_mod.ResolverConfig) !void {
        try self.enforceDurableMutationGate();
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
        try self.enforceDurableMutationGate();
        return try lifecycle_impl.upsertResolverWithResultOptions(self, cfg, options);
    }

    pub fn upsertResolverWithResult(self: *DB, cfg: index_manager_mod.ResolverConfig) !index_manager_mod.IndexManager.ResolverUpsertResult {
        try self.enforceDurableMutationGate();
        return try lifecycle_impl.upsertResolverWithResult(self, cfg);
    }

    pub fn upsertResolver(self: *DB, cfg: index_manager_mod.ResolverConfig) !void {
        try self.enforceDurableMutationGate();
        return try lifecycle_impl.upsertResolver(self, cfg);
    }

    pub fn drainResolverBackfill(self: *DB) !void {
        try self.enforceDurableMutationGate();
        return try lifecycle_impl.drainResolverBackfill(self);
    }

    pub fn removeResolver(self: *DB, name: []const u8) !bool {
        try self.enforceDurableMutationGate();
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
        try self.enforceDurableMutationGate();
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
        try self.enforceDurableMutationGate();
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
        try self.enforceDurableMutationGate();
        return try schema_runtime_impl.evaluateAlgebraicAdaptiveCandidates(self);
    }

    pub fn runAlgebraicAdaptiveWork(self: *DB) !u64 {
        try self.enforceDurableMutationGate();
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
        try self.enforceDurableMutationGate();
        return try lifecycle_impl.compactTextIndexes(self);
    }

    pub fn drainScheduledTextMerges(self: *DB) !void {
        try self.enforceDurableMutationGate();
        return try lifecycle_impl.drainScheduledTextMerges(self);
    }

    pub fn forceCompactTextIndexes(self: *DB) !void {
        try self.enforceDurableMutationGate();
        return try lifecycle_impl.forceCompactTextIndexes(self);
    }

    pub fn bestEffortForceCompactTextIndexes(self: *DB) !void {
        try self.enforceDurableMutationGate();
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
        try self.enforceDurableMutationGate();
        return try schema_runtime_impl.deleteIndex(self, name);
    }

    pub fn deleteEnrichment(self: *DB, kind: types.EnrichmentKind, name: []const u8) !bool {
        try self.enforceDurableMutationGate();
        return try schema_runtime_impl.deleteEnrichment(self, kind, name);
    }

    pub fn pendingWorkStats(self: *DB) db_core.PendingWorkStats {
        return lifecycle_impl.pendingWorkStats(self);
    }

    pub fn runDerivedUntil(self: *DB, sequence: u64) !void {
        try self.enforceDurableMutationGate();
        return try lifecycle_impl.runDerivedUntil(self, sequence);
    }

    pub fn runDerivedUntilTargets(self: *DB, sequence: u64, index_names: []const []const u8) !void {
        try self.enforceDurableMutationGate();
        return try lifecycle_impl.runDerivedUntilTargets(self, sequence, index_names);
    }

    pub fn runEnrichmentUntil(self: *DB, sequence: u64) !void {
        try self.enforceDurableMutationGate();
        return try lifecycle_impl.runEnrichmentUntil(self, sequence);
    }

    pub fn runMaintenanceUntil(self: *DB, sequence: u64, sync_targets: ManagedSyncTargets) !void {
        try self.enforceDurableMutationGate();
        return try lifecycle_impl.runMaintenanceUntil(self, sequence, sync_targets);
    }

    pub fn runMaintenanceUntilTargets(self: *DB, sequence: u64, index_names: []const []const u8) !void {
        try self.enforceDurableMutationGate();
        return try lifecycle_impl.runMaintenanceUntilTargets(self, sequence, index_names);
    }

    pub fn waitForCurrentSyncLevel(self: *DB, sync_level: types.SyncLevel) !void {
        return try lifecycle_impl.waitForCurrentSyncLevel(self, sync_level);
    }

    pub fn catchUpPendingDerivedReplay(self: *DB) !void {
        try self.enforceDurableMutationGate();
        try derived_async_impl.replayPendingDerivedBatches(self, null, null);
    }

    pub fn catchUpPendingDerivedReplayWithProgress(
        self: *DB,
        progress_ctx: *anyopaque,
        progress_hook: ReplayProgressHook,
    ) !void {
        try self.enforceDurableMutationGate();
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
        try self.enforceDurableMutationGate();
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
        try self.enforceDurableMutationGate();
        return try lifecycle_impl.runUntilIdle(self);
    }

    fn enforceGraphMetricMutationGate(self: *DB) !void {
        try self.enforceDurableMutationGate();
    }

    pub fn runGraphMetricMaintenanceForIdle(self: *DB) !usize {
        try self.enforceGraphMetricMutationGate();
        return try search_runtime_impl.runGraphMetricMaintenanceForIdle(self);
    }

    pub fn runGraphMetricPlannedMaintenanceForIdle(
        self: *DB,
        options: index_manager_mod.IndexManager.GraphMetricPlannedMaintenanceOptions,
    ) !index_manager_mod.IndexManager.GraphMetricPlannedSchedulerSweepResult {
        try self.enforceGraphMetricMutationGate();
        return try search_runtime_impl.runGraphMetricPlannedMaintenanceForIdle(self, options);
    }

    pub fn runGraphMetricServiceMaintenanceJsonAlloc(self: *DB, alloc: Allocator, body: []const u8) ![]u8 {
        try self.enforceGraphMetricMutationGate();
        return try search_runtime_impl.runGraphMetricServiceMaintenanceJsonAlloc(self, alloc, body);
    }

    pub fn refreshGraphMetric(self: *DB, alloc: Allocator, index_name: []const u8, metric_name: []const u8) !types.GraphMetricStatus {
        try self.enforceGraphMetricMutationGate();
        return try search_runtime_impl.refreshGraphMetric(self, alloc, index_name, metric_name);
    }

    pub fn rebuildGraphMetric(self: *DB, alloc: Allocator, index_name: []const u8, metric_name: []const u8) !types.GraphMetricStatus {
        try self.enforceGraphMetricMutationGate();
        return try search_runtime_impl.rebuildGraphMetric(self, alloc, index_name, metric_name);
    }

    pub fn deleteGraphMetricMaterialization(self: *DB, alloc: Allocator, index_name: []const u8, metric_name: []const u8) !types.GraphMetricStatus {
        try self.enforceGraphMetricMutationGate();
        return try search_runtime_impl.deleteGraphMetricMaterialization(self, alloc, index_name, metric_name);
    }

    pub fn pauseGraphMetricMaintenance(self: *DB, alloc: Allocator, index_name: []const u8, metric_name: []const u8) !types.GraphMetricStatus {
        try self.enforceGraphMetricMutationGate();
        return try search_runtime_impl.pauseGraphMetricMaintenance(self, alloc, index_name, metric_name);
    }

    pub fn resumeGraphMetricMaintenance(self: *DB, alloc: Allocator, index_name: []const u8, metric_name: []const u8) !types.GraphMetricStatus {
        try self.enforceGraphMetricMutationGate();
        return try search_runtime_impl.resumeGraphMetricMaintenance(self, alloc, index_name, metric_name);
    }

    pub fn ensureGraphMetricPlannedBuild(
        self: *DB,
        alloc: Allocator,
        index_name: []const u8,
        metric_name: []const u8,
        target_generation: u64,
    ) !types.GraphMetricStatus {
        try self.enforceGraphMetricMutationGate();
        return try search_runtime_impl.ensureGraphMetricPlannedBuild(self, alloc, index_name, metric_name, target_generation);
    }

    pub fn runGraphMetricPlannedWorkerPageStep(
        self: *DB,
        index_name: []const u8,
        metric_name: []const u8,
        worker_id: []const u8,
    ) !graph_mod.GraphIndex.GraphMetricBuildWorkerStepResult {
        try self.enforceGraphMetricMutationGate();
        return try search_runtime_impl.runGraphMetricPlannedWorkerPageStep(self, index_name, metric_name, worker_id);
    }

    pub fn runGraphMetricPlannedWorkerPageStepAt(
        self: *DB,
        index_name: []const u8,
        metric_name: []const u8,
        worker_id: []const u8,
        now_ms: u64,
    ) !graph_mod.GraphIndex.GraphMetricBuildWorkerStepResult {
        try self.enforceGraphMetricMutationGate();
        return try search_runtime_impl.runGraphMetricPlannedWorkerPageStepAt(self, index_name, metric_name, worker_id, now_ms);
    }

    pub fn runGraphMetricPlannedCoordinatorStep(
        self: *DB,
        index_name: []const u8,
        metric_name: []const u8,
    ) !graph_mod.GraphIndex.GraphMetricBuildWorkerStepResult {
        try self.enforceGraphMetricMutationGate();
        return try search_runtime_impl.runGraphMetricPlannedCoordinatorStep(self, index_name, metric_name);
    }

    pub fn runGraphMetricPlannedCoordinatorStepAt(
        self: *DB,
        index_name: []const u8,
        metric_name: []const u8,
        now_ms: u64,
    ) !graph_mod.GraphIndex.GraphMetricBuildWorkerStepResult {
        try self.enforceGraphMetricMutationGate();
        return try search_runtime_impl.runGraphMetricPlannedCoordinatorStepAt(self, index_name, metric_name, now_ms);
    }

    pub fn failGraphMetricPlannedBuild(
        self: *DB,
        alloc: Allocator,
        index_name: []const u8,
        metric_name: []const u8,
        err: anyerror,
    ) !types.GraphMetricStatus {
        try self.enforceGraphMetricMutationGate();
        return try search_runtime_impl.failGraphMetricPlannedBuild(self, alloc, index_name, metric_name, err);
    }

    pub fn runGraphMetricPlannedDrain(
        self: *DB,
        alloc: Allocator,
        index_name: []const u8,
        metric_name: []const u8,
        target_generation: u64,
        options: graph_mod.GraphIndex.GraphMetricPlannedDrainOptions,
    ) !types.GraphMetricStatus {
        try self.enforceGraphMetricMutationGate();
        return try search_runtime_impl.runGraphMetricPlannedDrain(self, alloc, index_name, metric_name, target_generation, options);
    }

    pub fn runGraphMetricPlannedCoordinatorSweep(
        self: *DB,
        options: index_manager_mod.IndexManager.GraphMetricPlannedSchedulerSweepOptions,
    ) !index_manager_mod.IndexManager.GraphMetricPlannedSchedulerSweepResult {
        try self.enforceGraphMetricMutationGate();
        return try search_runtime_impl.runGraphMetricPlannedCoordinatorSweep(self, options);
    }

    pub fn runGraphMetricPlannedWorkerSweep(
        self: *DB,
        options: index_manager_mod.IndexManager.GraphMetricPlannedWorkerSweepOptions,
    ) !index_manager_mod.IndexManager.GraphMetricPlannedSchedulerSweepResult {
        try self.enforceGraphMetricMutationGate();
        return try search_runtime_impl.runGraphMetricPlannedWorkerSweep(self, options);
    }

    pub const DenseArtifactRebuildTarget = derived_async.DenseArtifactRebuildTarget;

    pub fn rebuildDenseIndexesForTargetCoverage(self: *DB, alloc: Allocator) !usize {
        try self.enforceDurableMutationGate();
        return try derived_async_impl.rebuildDenseIndexesForTargetCoverage(self, alloc);
    }

    pub fn rebuildSparseIndexesForTargetCoverage(self: *DB, alloc: Allocator) !usize {
        try self.enforceDurableMutationGate();
        return try derived_async_impl.rebuildSparseIndexesForTargetCoverage(self, alloc);
    }

    pub fn rebuildGraphIndexesForTargetCoverage(self: *DB, alloc: Allocator) !void {
        try self.enforceDurableMutationGate();
        return try split_restore_impl.rebuildGraphIndexesForTargetCoverage(self, alloc);
    }

    pub fn runDensePostingMaintenanceForIdle(self: *DB) !usize {
        try self.enforceDurableMutationGate();
        return try derived_async_impl.runDensePostingMaintenanceForIdle(self);
    }

    pub fn runDensePostingMaintenanceForIdleBestEffort(self: *DB) !usize {
        try self.enforceDurableMutationGate();
        return try derived_async_impl.runDensePostingMaintenanceForIdleBestEffort(self);
    }

    pub fn rebuildDenseIndexesFromStoredEmbeddingArtifacts(self: *DB, alloc: Allocator) !usize {
        try self.enforceDurableMutationGate();
        return try derived_async_impl.rebuildDenseIndexesFromStoredEmbeddingArtifacts(self, alloc);
    }

    pub fn rebuildDenseIndexesFromStoredEmbeddingArtifactsIfNeeded(self: *DB, alloc: Allocator) !usize {
        try self.enforceDurableMutationGate();
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
        try self.enforceDurableMutationGate();
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
        try self.enforceDurableMutationGate();
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
        try self.enforceDurableMutationGate();
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
        try self.enforceDurableMutationGate();
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
        try self.enforceRelationalIntegrityMutationGate();
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
        try self.enforceRelationalIntegrityMutationGate();
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
        try self.enforceRelationalIntegrityMutationGate();
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
        try self.enforceRelationalIntegrityMutationGate();
        return try relational_integrity_impl.upsertForeignKeyIntegrityJobRecordAt(self, job_id, table_name, action, worker_id, constraint_name, lower_doc_key, upper_doc_key, lease_ms, max_work_units, status, now_ns);
    }

    pub fn completeForeignKeyIntegrityJobRecord(
        self: *DB,
        job_id: []const u8,
        status: []const u8,
        valid: bool,
        report: relational_store_mod.ForeignKeyIntegrityReport,
    ) !ForeignKeyIntegrityJobRecord {
        try self.enforceRelationalIntegrityMutationGate();
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
        try self.enforceRelationalIntegrityMutationGate();
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
        try self.enforceRelationalIntegrityMutationGate();
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
        try self.enforceRelationalIntegrityMutationGate();
        return try relational_integrity_impl.completeForeignKeyIntegrityJobRecordWithDiagnosticsAt(self, job_id, status, valid, report, violation_samples_json, violation_sample_count, violations_truncated, now_ns);
    }

    pub fn updateForeignKeyIntegrityJobDiagnostics(
        self: *DB,
        job_id: []const u8,
        violation_samples_json: []const u8,
        violation_sample_count: usize,
        violations_truncated: bool,
    ) !ForeignKeyIntegrityJobRecord {
        try self.enforceRelationalIntegrityMutationGate();
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
        try self.enforceRelationalIntegrityMutationGate();
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
        try self.enforceRelationalIntegrityMutationGate();
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
        try self.enforceRelationalIntegrityMutationGate();
        return try relational_integrity_impl.updateForeignKeyIntegrityJobDiagnosticsWithReportAt(self, job_id, report, violation_samples_json, violation_sample_count, violations_truncated, now_ns);
    }

    pub fn claimAndRunForeignKeyActionJobPage(self: *DB, job_id: []const u8, action: []const u8, worker_id: []const u8, constraint_name: []const u8, parent_table: []const u8, parent_key: []const u8, page_limit: usize, lease_ms: u64) !ForeignKeyActionJobRecord {
        try self.enforceRelationalIntegrityMutationGate();
        return try relational_integrity_impl.claimAndRunForeignKeyActionJobPage(self, job_id, action, worker_id, constraint_name, parent_table, parent_key, page_limit, lease_ms);
    }

    pub fn scheduleForeignKeyActionJob(self: *DB, job_id: []const u8, action: []const u8, worker_id: []const u8, constraint_name: []const u8, parent_table: []const u8, parent_key: []const u8, page_limit: usize) !ForeignKeyActionJobRecord {
        try self.enforceRelationalIntegrityMutationGate();
        return try relational_integrity_impl.scheduleForeignKeyActionJob(self, job_id, action, worker_id, constraint_name, parent_table, parent_key, page_limit);
    }

    pub fn scheduleForeignKeyActionJobWithUpdatedParentKey(self: *DB, job_id: []const u8, action: []const u8, worker_id: []const u8, constraint_name: []const u8, parent_table: []const u8, parent_key: []const u8, updated_parent_key: ?[]const u8, page_limit: usize) !ForeignKeyActionJobRecord {
        try self.enforceRelationalIntegrityMutationGate();
        return try relational_integrity_impl.scheduleForeignKeyActionJobWithUpdatedParentKey(self, job_id, action, worker_id, constraint_name, parent_table, parent_key, updated_parent_key, page_limit);
    }

    pub fn scheduleForeignKeyActionJobAt(self: *DB, job_id: []const u8, action: []const u8, worker_id: []const u8, constraint_name: []const u8, parent_table: []const u8, parent_key: []const u8, page_limit: usize, now_ns: u64) !ForeignKeyActionJobRecord {
        try self.enforceRelationalIntegrityMutationGate();
        return try relational_integrity_impl.scheduleForeignKeyActionJobAt(self, job_id, action, worker_id, constraint_name, parent_table, parent_key, page_limit, now_ns);
    }

    pub fn scheduleForeignKeyActionJobWithUpdatedParentKeyAt(self: *DB, job_id: []const u8, action: []const u8, worker_id: []const u8, constraint_name: []const u8, parent_table: []const u8, parent_key: []const u8, updated_parent_key: ?[]const u8, page_limit: usize, now_ns: u64) !ForeignKeyActionJobRecord {
        try self.enforceRelationalIntegrityMutationGate();
        return try relational_integrity_impl.scheduleForeignKeyActionJobWithUpdatedParentKeyAt(self, job_id, action, worker_id, constraint_name, parent_table, parent_key, updated_parent_key, page_limit, now_ns);
    }

    pub fn scheduleForeignKeyActionJobWithUpdatedParentKeyAndCascadeLineageAt(self: *DB, job_id: []const u8, action: []const u8, worker_id: []const u8, constraint_name: []const u8, parent_table: []const u8, parent_key: []const u8, updated_parent_key: ?[]const u8, page_limit: usize, cascade_depth: u32, cascade_max_depth: u32, now_ns: u64) !ForeignKeyActionJobRecord {
        try self.enforceRelationalIntegrityMutationGate();
        return try relational_integrity_impl.scheduleForeignKeyActionJobWithUpdatedParentKeyAndCascadeLineageAt(self, job_id, action, worker_id, constraint_name, parent_table, parent_key, updated_parent_key, page_limit, cascade_depth, cascade_max_depth, now_ns);
    }

    pub fn requeueForeignKeyActionJob(self: *DB, job_id: []const u8, action: []const u8, worker_id: []const u8, constraint_name: []const u8, parent_table: []const u8, parent_key: []const u8, page_limit: usize) !ForeignKeyActionJobRecord {
        try self.enforceRelationalIntegrityMutationGate();
        return try relational_integrity_impl.requeueForeignKeyActionJob(self, job_id, action, worker_id, constraint_name, parent_table, parent_key, page_limit);
    }

    pub fn requeueForeignKeyActionJobWithUpdatedParentKey(self: *DB, job_id: []const u8, action: []const u8, worker_id: []const u8, constraint_name: []const u8, parent_table: []const u8, parent_key: []const u8, updated_parent_key: ?[]const u8, page_limit: usize) !ForeignKeyActionJobRecord {
        try self.enforceRelationalIntegrityMutationGate();
        return try relational_integrity_impl.requeueForeignKeyActionJobWithUpdatedParentKey(self, job_id, action, worker_id, constraint_name, parent_table, parent_key, updated_parent_key, page_limit);
    }

    pub fn requeueForeignKeyActionJobAt(self: *DB, job_id: []const u8, action: []const u8, worker_id: []const u8, constraint_name: []const u8, parent_table: []const u8, parent_key: []const u8, page_limit: usize, now_ns: u64) !ForeignKeyActionJobRecord {
        try self.enforceRelationalIntegrityMutationGate();
        return try relational_integrity_impl.requeueForeignKeyActionJobAt(self, job_id, action, worker_id, constraint_name, parent_table, parent_key, page_limit, now_ns);
    }

    pub fn requeueForeignKeyActionJobWithUpdatedParentKeyAt(self: *DB, job_id: []const u8, action: []const u8, worker_id: []const u8, constraint_name: []const u8, parent_table: []const u8, parent_key: []const u8, updated_parent_key: ?[]const u8, page_limit: usize, now_ns: u64) !ForeignKeyActionJobRecord {
        try self.enforceRelationalIntegrityMutationGate();
        return try relational_integrity_impl.requeueForeignKeyActionJobWithUpdatedParentKeyAt(self, job_id, action, worker_id, constraint_name, parent_table, parent_key, updated_parent_key, page_limit, now_ns);
    }

    pub fn scheduleForeignKeyActionSchedule(self: *DB, schedule_id: []const u8, action_job_id: []const u8, action: []const u8, worker_id: []const u8, constraint_name: []const u8, parent_table: []const u8, parent_key: []const u8, page_limit: usize) !ForeignKeyActionScheduleRecord {
        try self.enforceRelationalIntegrityMutationGate();
        return try relational_integrity_impl.scheduleForeignKeyActionSchedule(self, schedule_id, action_job_id, action, worker_id, constraint_name, parent_table, parent_key, page_limit);
    }

    pub fn scheduleForeignKeyActionScheduleWithUpdatedParentKey(self: *DB, schedule_id: []const u8, action_job_id: []const u8, action: []const u8, worker_id: []const u8, constraint_name: []const u8, parent_table: []const u8, parent_key: []const u8, updated_parent_key: ?[]const u8, page_limit: usize) !ForeignKeyActionScheduleRecord {
        try self.enforceRelationalIntegrityMutationGate();
        return try relational_integrity_impl.scheduleForeignKeyActionScheduleWithUpdatedParentKey(self, schedule_id, action_job_id, action, worker_id, constraint_name, parent_table, parent_key, updated_parent_key, page_limit);
    }

    pub fn scheduleForeignKeyActionScheduleAt(self: *DB, schedule_id: []const u8, action_job_id: []const u8, action: []const u8, worker_id: []const u8, constraint_name: []const u8, parent_table: []const u8, parent_key: []const u8, page_limit: usize, now_ns: u64) !ForeignKeyActionScheduleRecord {
        try self.enforceRelationalIntegrityMutationGate();
        return try relational_integrity_impl.scheduleForeignKeyActionScheduleAt(self, schedule_id, action_job_id, action, worker_id, constraint_name, parent_table, parent_key, page_limit, now_ns);
    }

    pub fn scheduleForeignKeyActionScheduleWithUpdatedParentKeyAt(self: *DB, schedule_id: []const u8, action_job_id: []const u8, action: []const u8, worker_id: []const u8, constraint_name: []const u8, parent_table: []const u8, parent_key: []const u8, updated_parent_key: ?[]const u8, page_limit: usize, now_ns: u64) !ForeignKeyActionScheduleRecord {
        try self.enforceRelationalIntegrityMutationGate();
        return try relational_integrity_impl.scheduleForeignKeyActionScheduleWithUpdatedParentKeyAt(self, schedule_id, action_job_id, action, worker_id, constraint_name, parent_table, parent_key, updated_parent_key, page_limit, now_ns);
    }

    pub fn requeueForeignKeyActionSchedule(self: *DB, schedule_id: []const u8, action_job_id: []const u8, action: []const u8, worker_id: []const u8, constraint_name: []const u8, parent_table: []const u8, parent_key: []const u8, page_limit: usize) !ForeignKeyActionScheduleRecord {
        try self.enforceRelationalIntegrityMutationGate();
        return try relational_integrity_impl.requeueForeignKeyActionSchedule(self, schedule_id, action_job_id, action, worker_id, constraint_name, parent_table, parent_key, page_limit);
    }

    pub fn requeueForeignKeyActionScheduleAt(self: *DB, schedule_id: []const u8, action_job_id: []const u8, action: []const u8, worker_id: []const u8, constraint_name: []const u8, parent_table: []const u8, parent_key: []const u8, page_limit: usize, now_ns: u64) !ForeignKeyActionScheduleRecord {
        try self.enforceRelationalIntegrityMutationGate();
        return try relational_integrity_impl.requeueForeignKeyActionScheduleAt(self, schedule_id, action_job_id, action, worker_id, constraint_name, parent_table, parent_key, page_limit, now_ns);
    }

    pub fn requeueForeignKeyActionScheduleWithUpdatedParentKeyAt(self: *DB, schedule_id: []const u8, action_job_id: []const u8, action: []const u8, worker_id: []const u8, constraint_name: []const u8, parent_table: []const u8, parent_key: []const u8, updated_parent_key: ?[]const u8, page_limit: usize, now_ns: u64) !ForeignKeyActionScheduleRecord {
        try self.enforceRelationalIntegrityMutationGate();
        return try relational_integrity_impl.requeueForeignKeyActionScheduleWithUpdatedParentKeyAt(self, schedule_id, action_job_id, action, worker_id, constraint_name, parent_table, parent_key, updated_parent_key, page_limit, now_ns);
    }

    pub fn markForeignKeyActionScheduleSeeded(self: *DB, schedule_id: []const u8, scheduled_groups: u64) !ForeignKeyActionScheduleRecord {
        try self.enforceRelationalIntegrityMutationGate();
        return try relational_integrity_impl.markForeignKeyActionScheduleSeeded(self, schedule_id, scheduled_groups);
    }

    pub fn markForeignKeyActionScheduleSeededAt(self: *DB, schedule_id: []const u8, scheduled_groups: u64, now_ns: u64) !ForeignKeyActionScheduleRecord {
        try self.enforceRelationalIntegrityMutationGate();
        return try relational_integrity_impl.markForeignKeyActionScheduleSeededAt(self, schedule_id, scheduled_groups, now_ns);
    }

    pub fn claimAndRunForeignKeyActionJobPageAt(self: *DB, job_id: []const u8, action: []const u8, worker_id: []const u8, constraint_name: []const u8, parent_table: []const u8, parent_key: []const u8, page_limit: usize, lease_ms: u64, now_ns: u64) !ForeignKeyActionJobRecord {
        try self.enforceRelationalIntegrityMutationGate();
        return try relational_integrity_impl.claimAndRunForeignKeyActionJobPageAt(self, job_id, action, worker_id, constraint_name, parent_table, parent_key, page_limit, lease_ms, now_ns);
    }

    pub fn claimAndRunForeignKeyActionJobPageWithUpdatedParentKeyAt(self: *DB, job_id: []const u8, action: []const u8, worker_id: []const u8, constraint_name: []const u8, parent_table: []const u8, parent_key: []const u8, updated_parent_key: ?[]const u8, page_limit: usize, lease_ms: u64, now_ns: u64) !ForeignKeyActionJobRecord {
        try self.enforceRelationalIntegrityMutationGate();
        return try relational_integrity_impl.claimAndRunForeignKeyActionJobPageWithUpdatedParentKeyAt(self, job_id, action, worker_id, constraint_name, parent_table, parent_key, updated_parent_key, page_limit, lease_ms, now_ns);
    }

    pub fn claimAndRunForeignKeyActionJobPageWithUpdatedParentKeyAndCascadeLineageAt(self: *DB, job_id: []const u8, action: []const u8, worker_id: []const u8, constraint_name: []const u8, parent_table: []const u8, parent_key: []const u8, updated_parent_key: ?[]const u8, page_limit: usize, lease_ms: u64, cascade_depth: u32, cascade_max_depth: u32, now_ns: u64) !ForeignKeyActionJobRecord {
        try self.enforceRelationalIntegrityMutationGate();
        return try relational_integrity_impl.claimAndRunForeignKeyActionJobPageWithUpdatedParentKeyAndCascadeLineageAt(self, job_id, action, worker_id, constraint_name, parent_table, parent_key, updated_parent_key, page_limit, lease_ms, cascade_depth, cascade_max_depth, now_ns);
    }

    pub fn claimForeignKeyActionJobPage(self: *DB, job_id: []const u8, action: []const u8, worker_id: []const u8, constraint_name: []const u8, parent_table: []const u8, parent_key: []const u8, page_limit: usize, lease_ms: u64) !ForeignKeyActionJobRecord {
        try self.enforceRelationalIntegrityMutationGate();
        return try relational_integrity_impl.claimForeignKeyActionJobPage(self, job_id, action, worker_id, constraint_name, parent_table, parent_key, page_limit, lease_ms);
    }

    pub fn claimForeignKeyActionJobPageAt(self: *DB, job_id: []const u8, action: []const u8, worker_id: []const u8, constraint_name: []const u8, parent_table: []const u8, parent_key: []const u8, page_limit: usize, lease_ms: u64, now_ns: u64) !ForeignKeyActionJobRecord {
        try self.enforceRelationalIntegrityMutationGate();
        return try relational_integrity_impl.claimForeignKeyActionJobPageAt(self, job_id, action, worker_id, constraint_name, parent_table, parent_key, page_limit, lease_ms, now_ns);
    }

    pub fn claimForeignKeyActionJobPageWithUpdatedParentKeyAt(self: *DB, job_id: []const u8, action: []const u8, worker_id: []const u8, constraint_name: []const u8, parent_table: []const u8, parent_key: []const u8, updated_parent_key: ?[]const u8, page_limit: usize, lease_ms: u64, now_ns: u64) !ForeignKeyActionJobRecord {
        try self.enforceRelationalIntegrityMutationGate();
        return try relational_integrity_impl.claimForeignKeyActionJobPageWithUpdatedParentKeyAt(self, job_id, action, worker_id, constraint_name, parent_table, parent_key, updated_parent_key, page_limit, lease_ms, now_ns);
    }

    pub fn claimForeignKeyActionJobPageWithUpdatedParentKeyAndCascadeLineageAt(self: *DB, job_id: []const u8, action: []const u8, worker_id: []const u8, constraint_name: []const u8, parent_table: []const u8, parent_key: []const u8, updated_parent_key: ?[]const u8, page_limit: usize, lease_ms: u64, cascade_depth: u32, cascade_max_depth: u32, now_ns: u64) !ForeignKeyActionJobRecord {
        try self.enforceRelationalIntegrityMutationGate();
        return try relational_integrity_impl.claimForeignKeyActionJobPageWithUpdatedParentKeyAndCascadeLineageAt(self, job_id, action, worker_id, constraint_name, parent_table, parent_key, updated_parent_key, page_limit, lease_ms, cascade_depth, cascade_max_depth, now_ns);
    }

    pub fn finishClaimedForeignKeyActionJobPage(self: *DB, claimed: ForeignKeyActionJobRecord, applied_count: usize, complete: bool, next_child_table: ?[]const u8, next_child_key: ?[]const u8, last_error: ?[]const u8) !ForeignKeyActionJobRecord {
        try self.enforceRelationalIntegrityMutationGate();
        return try relational_integrity_impl.finishClaimedForeignKeyActionJobPage(self, claimed, applied_count, complete, next_child_table, next_child_key, last_error);
    }

    pub fn finishClaimedForeignKeyActionJobPageAt(self: *DB, claimed: ForeignKeyActionJobRecord, applied_count: usize, complete: bool, next_child_table: ?[]const u8, next_child_key: ?[]const u8, last_error: ?[]const u8, now_ns: u64) !ForeignKeyActionJobRecord {
        try self.enforceRelationalIntegrityMutationGate();
        return try relational_integrity_impl.finishClaimedForeignKeyActionJobPageAt(self, claimed, applied_count, complete, next_child_table, next_child_key, last_error, now_ns);
    }

    pub fn claimAndRunForeignKeyIntegrityWorkUnit(self: *DB, claim_key: []const u8, worker_id: []const u8, group_id: u64, phase: []const u8, mode: relational_store_mod.ForeignKeyIntegrityMode, constraint_name: ?[]const u8, lower_doc_key: []const u8, upper_doc_key: []const u8, lease_ms: u64) !ForeignKeyIntegrityReport {
        try self.enforceRelationalIntegrityMutationGate();
        return try relational_integrity_impl.claimAndRunForeignKeyIntegrityWorkUnit(self, claim_key, worker_id, group_id, phase, mode, constraint_name, lower_doc_key, upper_doc_key, lease_ms);
    }

    pub fn claimAndRunForeignKeyIntegrityWorkUnitAt(self: *DB, claim_key: []const u8, worker_id: []const u8, group_id: u64, phase: []const u8, mode: relational_store_mod.ForeignKeyIntegrityMode, constraint_name: ?[]const u8, lower_doc_key: []const u8, upper_doc_key: []const u8, lease_ms: u64, now_ns: u64) !ForeignKeyIntegrityReport {
        try self.enforceRelationalIntegrityMutationGate();
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
        try self.enforceDurableMutationGate();
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
        try self.enforceDurableMutationGate();
        return try relational_rows_impl.stagePlannedRelationalRowsMutationSourceAlloc(self, alloc, runtime_schema, req, matched, candidates);
    }

    pub fn mutateRelationalRowsJoinedSourceAlloc(
        self: *DB,
        alloc: Allocator,
        runtime_schema: schema_mod.TableSchema,
        req: types.RelationalRowsJoinedMutationSourceRequest,
    ) !types.RelationalRowsMutationSourceResult {
        try self.enforceDurableMutationGate();
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
        try self.enforceDurableMutationGate();
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
        try self.enforceDurableMutationGate();
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

    pub fn relationalRowsExpressionConditionsImpliedByEqualityPredicatesAlloc(
        alloc: Allocator,
        predicates: []const schema_mod.RelationalCheck,
        required: []const types.RelationalRowsExpressionCondition,
    ) !bool {
        return try relational_rows_impl.relationalRowsExpressionConditionsImpliedByEqualityPredicatesAlloc(alloc, predicates, required);
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
        strategy: ?graph_query_mod.ExpandStrategy,
    ) !void {
        try search_runtime_impl.applyGraphExpandStrategy(self, alloc, result, strategy);
    }

    pub fn searchRuntimeCloneNamedSetAsResult(
        self: *DB,
        alloc: Allocator,
        set: NamedResultSet,
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
        nodes: []const graph_query_mod.GraphResultNode,
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
    ) ![]graph_pattern_mod.PatternMatch {
        return try search_runtime_impl.matchNamedPattern(self, alloc, named, start_key_refs);
    }

    pub fn searchRuntimeLoadPatternProjectedDocument(
        self: *DB,
        alloc: Allocator,
        query: graph_query_mod.GraphQuery,
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
        query: search_mod.SearchQuery,
        generation: ?u64,
    ) !?doc_set.ResolvedDocSet {
        return try search_runtime_impl.resolveRelationalFilterDocSet(self, alloc, runtime_schema, query, generation);
    }

    pub fn searchRuntimeResolveRelationalFilterQueryDocSetAlloc(
        self: *DB,
        alloc: Allocator,
        runtime_schema: schema_mod.TableSchema,
        query: search_mod.SearchQuery,
        generation: ?u64,
    ) !?doc_set.ResolvedDocSet {
        return try search_runtime_impl.resolveRelationalFilterQueryDocSetAlloc(self, alloc, runtime_schema, query, generation);
    }

    pub fn searchRuntimeResolveRelationalFilterQueryDocSetWithImplicationsAlloc(
        self: *DB,
        alloc: Allocator,
        runtime_schema: schema_mod.TableSchema,
        query: search_mod.SearchQuery,
        implications: relational_rows.PredicateImplications,
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
        mode: relational_rows.FilterCombineMode,
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
        implications: relational_rows.PredicateImplications,
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
        graph_query: graph_query_mod.GraphQuery,
        start_key_refs: []const []const u8,
        target_keys: [][]u8,
    ) !graph_query_mod.GraphQueryResult {
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

    pub fn collectSearchRequestTextStats(self: *DB, alloc: Allocator, req: types.SearchRequest) ![]const @import("../../search/distributed_stats.zig").TextFieldStats {
        return try search_runtime_impl.collectSearchRequestTextStats(self, alloc, req);
    }

    pub fn preflightSearchRequest(self: *DB, alloc: Allocator, req: types.SearchRequest, max_work: u32) !db_query_search.RuntimePreflightSummary {
        return try search_runtime_impl.preflightSearchRequest(self, alloc, req, max_work);
    }

    pub fn preflightSearchRequestWithExecutionContext(
        self: *DB,
        alloc: Allocator,
        req: types.SearchRequest,
        max_work: u32,
        exec_ctx: types.ExecutionContext,
    ) !db_query_search.RuntimePreflightSummary {
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

    pub fn collectExplicitTextStats(self: *DB, alloc: Allocator, requests: []const db_query_search.ExplicitTextStatRequest) ![]const @import("../../search/distributed_stats.zig").TextFieldStats {
        return try search_runtime_impl.collectExplicitTextStats(self, alloc, requests);
    }

    pub fn collectExplicitBackgroundTextStats(
        self: *DB,
        alloc: Allocator,
        requests: []const db_query_search.ExplicitBackgroundTextStatRequest,
    ) ![]const aggregations_mod.DistributedBackgroundTextStats {
        return try search_runtime_impl.collectExplicitBackgroundTextStats(self, alloc, requests);
    }

    pub fn searchDenseProfiled(self: *DB, alloc: Allocator, req: types.SearchRequest, dense: types.DenseKnnQuery) !db_query_search.ProfiledDenseSearchResult {
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

test "document extraction templated inline source size is rejected before persistence" {
    const alloc = std.testing.allocator;

    var path_buf: [256]u8 = undefined;
    const path = tempPath(&path_buf);
    defer cleanupTempDir(path);

    const security = scraping.ContentSecurityConfig{ .max_download_size_bytes = 4 };
    var remote_content = scraping.RemoteContentConfig{ .security = security };
    var db = try DB.open(alloc, std.mem.span(path), .{
        .remote_content = &remote_content,
        .start_index_workers = false,
        .ttl_cleanup = .{ .enabled = false },
    });
    defer db.close();

    try db.addEnrichment(.{
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

    const doc_key = try internal_keys.documentKeyAlloc(alloc, "doc:templated-too-large");
    defer alloc.free(doc_key);
    try std.testing.expectError(error.NotFound, db.core.store.get(alloc, doc_key));
}

const systemVersionedHistoryRecordCommitSequence = relational_rows.systemVersionedHistoryRecordCommitSequence;

const rowClaimIntentKeyAlloc = relational_rows.rowClaimIntentKeyAlloc;

const assetStateKeyAlloc = db_internal.assetStateKeyAlloc;

const graphAssetStateKeyAlloc = db_internal.graphAssetStateKeyAlloc;

const DocumentExtractionUnitDescriptor = write_path.DocumentExtractionUnitDescriptor;
const documentUnitPayloadAlloc = write_path.documentUnitPayloadAlloc;
const documentExtractionUnitFingerprintAlloc = write_path.documentExtractionUnitFingerprintAlloc;
const documentExtractionUnitRangeCount = write_path.documentExtractionUnitRangeCount;
const documentExtractionUnitRangeIndex = write_path.documentExtractionUnitRangeIndex;
const documentExtractionManifestPayloadAlloc = write_path.documentExtractionManifestPayloadAlloc;

const NamedResultSet = db_query_graph.NamedResultSet;

const applyGraphUnion = db_query_graph.applyGraphUnion;

const applyGraphIntersection = db_query_graph.applyGraphIntersection;
const cloneNamedSetAsResult = db_query_graph.cloneNamedSetAsResult;

const currentTimeNs = db_internal.currentTimeNs;

const parsePatternRfc3339ToNs = db_internal.parseRfc3339ToNs;

const beginDenseCatchUpSessionTracked = db_internal.beginDenseCatchUpSessionTracked;
const finishDenseCatchUpSessionTracked = db_internal.finishDenseCatchUpSessionTracked;
const asyncContextHasActiveDenseBulkWork = db_internal.asyncContextHasActiveDenseBulkWork;
const flushDeferredExternalBulkExecutorNotification = db_internal.flushDeferredExternalBulkExecutorNotification;

test "db lsm maintenance reclaims due index obsolete paths before primary compaction" {
    const alloc = std.testing.allocator;
    var path_buf: [256]u8 = undefined;
    const path = tempPath(&path_buf);
    defer cleanupTempDir(path);

    var db = try DB.open(alloc, std.mem.span(path), .{
        .start_index_workers = false,
        .primary_backend = .{ .lsm = .{
            .flush_threshold = 1,
            .defer_flush_on_commit = true,
            .l0_soft_limit_runs = 100,
            .obsolete_retention_ns = 0,
        } },
        .index_backends = .{
            .dense_lsm_options = .{
                .flush_threshold = 1,
                .defer_flush_on_commit = true,
                .l0_soft_limit_runs = 100,
                .obsolete_retention_ns = 0,
            },
        },
    });
    defer db.close();

    try db.addIndex(.{
        .name = "dv_v1",
        .kind = .dense_vector,
        .config_json = "{\"field\":\"embedding\",\"dims\":2,\"metric\":\"l2_squared\"}",
    });

    var key_buf: [16]u8 = undefined;
    for (0..4) |i| {
        const key = try std.fmt.bufPrint(&key_buf, "doc:{d}", .{i});
        try db.batch(.{
            .writes = &.{.{ .key = key, .value = "{\"embedding\":[1,2]}" }},
            .sync_level = .write,
        });
        switch (db.core.primary_store_owner) {
            .lsm => |*owner| try owner.handle.backend.sync(true),
            else => return error.SkipZigTest,
        }
    }

    switch (db.core.primary_store_owner) {
        .lsm => |*owner| try owner.handle.backend.flushBufferedWritesWithOptions(.{ .compact = false, .flush = true }),
        else => return error.SkipZigTest,
    }

    switch (db.core.primary_store_owner) {
        .lsm => |*owner| owner.handle.backend.options.l0_soft_limit_runs = 1,
        else => return error.SkipZigTest,
    }

    const dense_entry = db.core.index_manager.denseIndex("dv_v1") orelse return error.TestUnexpectedResult;
    const dense_backend = switch (dense_entry.index.env_owner) {
        .lsm => |*handle| handle.backend,
        else => return error.SkipZigTest,
    };
    const dense_root = dense_backend.root_dir orelse return error.TestUnexpectedResult;
    const obsolete_path = try lsm_backend_mod.repository.runPath(alloc, dense_root, 999_999);
    defer alloc.free(obsolete_path);

    try lsm_backend_mod.repository.writeFileAbsoluteWithStorage(dense_backend.storage.?, obsolete_path, "obsolete");
    {
        const locked = lsm_backend_mod.runtime.lockBackend(lsm_backend_mod.Backend, dense_backend);
        defer lsm_backend_mod.runtime.unlockBackend(lsm_backend_mod.Backend, dense_backend, locked);
        try dense_backend.queueObsoleteFilePath(try alloc.dupe(u8, obsolete_path));
        dense_backend.manifest_dirty = false;
    }

    try expectObsoletePathsReclaimable(dense_backend, 1);
    try std.testing.expect(try db.runLsmMaintenanceStepBestEffort());
    try std.testing.expectEqual(@as(u64, 0), dense_backend.snapshotMaintenanceStats().obsolete_paths);
    try std.testing.expectError(error.FileNotFound, dense_backend.storage.?.readFileAlloc(alloc, obsolete_path, 1024));
}

test "db primary lsm maintenance step does not reclaim index obsolete paths" {
    const alloc = std.testing.allocator;
    var path_buf: [256]u8 = undefined;
    const path = tempPath(&path_buf);
    defer cleanupTempDir(path);

    var db = try DB.open(alloc, std.mem.span(path), .{
        .start_index_workers = false,
        .primary_backend = .{ .lsm = .{
            .flush_threshold = 1,
            .defer_flush_on_commit = true,
            .l0_soft_limit_runs = 100,
            .obsolete_retention_ns = 0,
        } },
        .index_backends = .{
            .dense_lsm_options = .{
                .flush_threshold = 1,
                .defer_flush_on_commit = true,
                .l0_soft_limit_runs = 100,
                .obsolete_retention_ns = 0,
            },
        },
    });
    defer db.close();

    try db.addIndex(.{
        .name = "dv_v1",
        .kind = .dense_vector,
        .config_json = "{\"field\":\"embedding\",\"dims\":2,\"metric\":\"l2_squared\"}",
    });

    const primary_backend = switch (db.core.primary_store_owner) {
        .lsm => |*owner| owner.handle.backend,
        else => return error.SkipZigTest,
    };
    const primary_root = primary_backend.root_dir orelse return error.TestUnexpectedResult;
    const primary_obsolete_path = try lsm_backend_mod.repository.runPath(alloc, primary_root, 888_888);
    defer alloc.free(primary_obsolete_path);

    const dense_entry = db.core.index_manager.denseIndex("dv_v1") orelse return error.TestUnexpectedResult;
    const dense_backend = switch (dense_entry.index.env_owner) {
        .lsm => |*handle| handle.backend,
        else => return error.SkipZigTest,
    };
    const dense_root = dense_backend.root_dir orelse return error.TestUnexpectedResult;
    const dense_obsolete_path = try lsm_backend_mod.repository.runPath(alloc, dense_root, 999_999);
    defer alloc.free(dense_obsolete_path);

    try lsm_backend_mod.repository.writeFileAbsoluteWithStorage(primary_backend.storage.?, primary_obsolete_path, "primary obsolete");
    {
        const locked = lsm_backend_mod.runtime.lockBackend(lsm_backend_mod.Backend, primary_backend);
        defer lsm_backend_mod.runtime.unlockBackend(lsm_backend_mod.Backend, primary_backend, locked);
        try primary_backend.queueObsoleteFilePath(try alloc.dupe(u8, primary_obsolete_path));
        primary_backend.manifest_dirty = false;
    }

    try lsm_backend_mod.repository.writeFileAbsoluteWithStorage(dense_backend.storage.?, dense_obsolete_path, "dense obsolete");
    {
        const locked = lsm_backend_mod.runtime.lockBackend(lsm_backend_mod.Backend, dense_backend);
        defer lsm_backend_mod.runtime.unlockBackend(lsm_backend_mod.Backend, dense_backend, locked);
        try dense_backend.queueObsoleteFilePath(try alloc.dupe(u8, dense_obsolete_path));
        dense_backend.manifest_dirty = false;
    }

    try expectObsoletePathsReclaimable(primary_backend, 1);
    try expectObsoletePathsReclaimable(dense_backend, 1);

    _ = try db.runPrimaryLsmMaintenanceStep();

    try std.testing.expectEqual(@as(u64, 0), primary_backend.snapshotMaintenanceStats().obsolete_paths);
    try std.testing.expectError(error.FileNotFound, primary_backend.storage.?.readFileAlloc(alloc, primary_obsolete_path, 1024));
    try std.testing.expectEqual(@as(u64, 1), dense_backend.snapshotMaintenanceStats().obsolete_paths);
    const dense_bytes = try dense_backend.storage.?.readFileAlloc(alloc, dense_obsolete_path, 1024);
    alloc.free(dense_bytes);
}

test "dense target advance is blocked while catch-up bulk session is active" {
    const alloc = std.testing.allocator;

    var path_buf: [256]u8 = undefined;
    const path = tempPath(&path_buf);
    defer cleanupTempDir(path);

    var db = try DB.open(alloc, std.mem.span(path), .{
        .start_index_workers = false,
        .ttl_cleanup = .{ .enabled = false },
        .enrichment = .{
            .owner_id = "worker-a",
            .enable_without_producers = true,
        },
    });
    defer db.close();

    try db.addIndex(.{
        .name = "semantic_idx",
        .kind = .dense_vector,
        .config_json = "{\"field\":\"embedding\",\"dims\":3,\"metric\":\"cosine\",\"generator\":{\"kind\":\"dense_embedding\",\"source_field\":\"body\",\"embedding_name\":\"semantic_idx\"}}",
    });

    const resources = db.core.asyncResources();
    var ctx = AsyncContext{
        .alloc = alloc,
        .store = resources.store,
        .index_manager = resources.index_manager,
        .apply_mutex = resources.apply_mutex,
    };
    defer ctx.deinit(alloc);

    try beginDenseCatchUpSessionTracked(&ctx, "semantic_idx");
    defer finishDenseCatchUpSessionTracked(&ctx, "semantic_idx");

    const can_advance = try DB.derivedAsyncCanAdvanceDerivedToTargetAsync(&ctx, .{
        .name = "semantic_idx",
        .kind = .dense_vector,
    }, 1, 2);
    try std.testing.expect(!can_advance);
}

const db_test_support = @import("test_support.zig");
const profileBenchTestsEnabled = db_test_support.profileBenchTestsEnabled;

const freeOwnedKeySlice = derived_async.freeOwnedKeySlice;

const lockAtomic = platform.sync.lockYielding;
const lockApply = db_test_support.lockApply;
const stressDenseBackend = db_test_support.stressDenseBackend;
const allocStressDenseDocJson = db_test_support.allocStressDenseDocJson;
const tempPath = db_test_support.tempPath;
const cleanupTempDir = db_test_support.cleanupTempDir;
const corruptNonEmptyFilesUnderDir = db_test_support.corruptNonEmptyFilesUnderDir;
const cacheBlockHitsForBench = db_test_support.cacheBlockHitsForBench;
const expectObsoletePathsReclaimable = db_test_support.expectObsoletePathsReclaimable;
const expectRelationalTemporalPriceRow = db_test_support.expectRelationalTemporalPriceRow;
const expectRelationalTemporalPrimarySelectorPriceRow = db_test_support.expectRelationalTemporalPrimarySelectorPriceRow;
const verifyDbSingleVectorFailedPlannedRebuildPreservesPublishedPublicReads = db_test_support.verifyDbSingleVectorFailedPlannedRebuildPreservesPublishedPublicReads;
const default_test_wait_attempts = db_test_support.default_test_wait_attempts;
const slow_test_wait_attempts = db_test_support.slow_test_wait_attempts;
const waitForSearchResult = db_test_support.waitForSearchResult;
const waitForSearchResultWithAttempts = db_test_support.waitForSearchResultWithAttempts;
const waitForDenseSearchResult = db_test_support.waitForDenseSearchResult;
const waitForDenseSearchResultWithAttempts = db_test_support.waitForDenseSearchResultWithAttempts;
const waitForDenseIndexResultsWithAttempts = db_test_support.waitForDenseIndexResultsWithAttempts;
const waitForAppliedSequenceAdvance = db_test_support.waitForAppliedSequenceAdvance;
const waitForRawDelete = db_test_support.waitForRawDelete;
const putDenseEmbeddingArtifactForTest = db_test_support.putDenseEmbeddingArtifactForTest;
const CountingDenseEmbedder = db_test_support.CountingDenseEmbedder;
const GateDenseEmbedder = db_test_support.GateDenseEmbedder;
const CountingSparseEmbedder = db_test_support.CountingSparseEmbedder;
const TestTransactionRecoveryResolver = db_test_support.TestTransactionRecoveryResolver;
const TxnResolverRecorder = db_test_support.TxnResolverRecorder;
const FixedVectorEmbedder = db_test_support.FixedVectorEmbedder;
const FakePromotionSink = db_test_support.FakePromotionSink;
const TestAssetProducer = db_test_support.TestAssetProducer;
const SharedReadLockHold = db_test_support.SharedReadLockHold(DB);
const ConcurrentReadProbe = db_test_support.ConcurrentReadProbe(DB);
const ConcurrentWriteProbe = db_test_support.ConcurrentWriteProbe(DB);

test "db batch get match_all search and index registry" {
    const alloc = std.testing.allocator;

    var path_buf: [256]u8 = undefined;
    const path = tempPath(&path_buf);
    defer cleanupTempDir(path);

    var db = try DB.open(alloc, std.mem.span(path), .{});
    defer db.close();

    try db.batch(.{
        .writes = &.{
            .{ .key = "doc:a", .value = "{\"title\":\"alpha\"}" },
            .{ .key = "doc:b", .value = "{\"title\":\"beta\"}" },
        },
    });

    const value = (try db.get(alloc, "doc:a")).?;
    defer alloc.free(value);
    try std.testing.expectEqualStrings("{\"title\":\"alpha\"}", value);

    try db.addIndex(.{
        .name = "ft_v1",
        .kind = .full_text,
        .config_json = "{}",
    });
    try std.testing.expectEqual(@as(u32, 1), db.core.index_manager.count());
    try std.testing.expect(try db.deleteIndex("ft_v1"));
    try std.testing.expectEqual(@as(u32, 0), db.core.index_manager.count());

    var result = try db.search(alloc, .{
        .query = .{ .match_all = {} },
        .limit = 10,
    });
    defer result.deinit();

    try std.testing.expectEqual(@as(u32, 2), result.total_hits);
    try std.testing.expectEqual(@as(usize, 2), result.hits.len);

    const stats = try db.diagnosticStats(alloc);
    defer types.freeDBStats(alloc, stats);
    try std.testing.expectEqual(@as(u64, 2), stats.doc_count);
}

test "db stats report engine-owned algebraic adaptive observation status" {
    const alloc = std.testing.allocator;
    const algebraic_ir = @import("algebraic/ir.zig");

    var path_buf: [256]u8 = undefined;
    const path = tempPath(&path_buf);
    defer cleanupTempDir(path);

    const cfg =
        \\{
        \\  "version": 1,
        \\  "table": "orders",
        \\  "schema_version": 42,
        \\  "capability_fingerprint": "cap:v1",
        \\  "capability_lifecycle_status": "stale",
        \\  "capability_change_added_fields": 1,
        \\  "capability_change_removed_fields": 2,
        \\  "capability_change_changed_type_fields": 3,
        \\  "skipped_dynamic_fields": 4,
        \\  "skipped_complex_fields": 5,
        \\  "skipped_unbounded_fields": 6,
        \\  "group_fields": [{"name":"customer","path":"customer","type":"string"},{"name":"tenant","path":"tenant","type":"string"}],
        \\  "measure_fields": [{"name":"amount","path":"amount","type":"number"}],
        \\  "adaptive": {"observe": true, "lazy_materialization": true, "min_observations": 1, "min_estimated_scan_rows_saved": 1},
        \\  "materializations": []
        \\}
    ;

    {
        var db = try DB.open(alloc, std.mem.span(path), .{ .start_index_workers = false });
        defer db.close();

        try db.addIndex(.{
            .name = "alg",
            .kind = .algebraic,
            .config_json = cfg,
        });

        const entry = db.core.index_manager.algebraicIndex("alg").?;
        const constraints = [_]algebraic_ir.Constraint{.{ .field = "tenant", .value = "t1" }};
        entry.index.recordObservedQueryShapeWithStore(db.core.store, .{
            .kind = .terms,
            .aggregation_name = "amount_by_customer",
            .bucket_field = "customer",
            .constraints = constraints[0..],
            .metric = .{ .name = "amount", .op = .sum, .field = "amount" },
        }, "no_materialization");

        const stats = try db.stats(alloc);
        defer types.freeDBStats(alloc, stats);
        try std.testing.expectEqual(@as(usize, 1), stats.indexes.len);
        try std.testing.expectEqual(@as(u64, 1), stats.indexes[0].algebraic_observed_query_shape_count);
        try std.testing.expectEqual(@as(u64, 1), stats.indexes[0].algebraic_recommendation_count);
        try std.testing.expect(stats.indexes[0].algebraic_last_observed_query_shape != null);
        try std.testing.expect(stats.indexes[0].algebraic_last_recommended_materialization != null);
        try std.testing.expect(std.mem.indexOf(u8, stats.indexes[0].algebraic_last_recommended_materialization.?, "recommendation:v1") != null);

        const materialization_states = try db.listAlgebraicMaterializationStates(alloc, "alg");
        defer types.freeAlgebraicMaterializationStates(alloc, materialization_states);
        try std.testing.expectEqual(@as(usize, 1), materialization_states.len);
        try std.testing.expectEqualStrings("alg", materialization_states[0].index_name);
        try std.testing.expectEqualStrings("recommended", materialization_states[0].lifecycle);
        try std.testing.expectEqual(@as(u64, 1), materialization_states[0].observation_count);
        try std.testing.expect(std.mem.indexOf(u8, materialization_states[0].recommendation, "recommendation:v1") != null);

        const observations = try db.listAlgebraicQueryObservations(alloc, "alg");
        defer types.freeAlgebraicQueryObservations(alloc, observations);
        try std.testing.expectEqual(@as(usize, 1), observations.len);
        try std.testing.expectEqualStrings("alg", observations[0].index_name);
        try std.testing.expectEqual(@as(u64, 1), observations[0].count);
        try std.testing.expectEqualStrings("no_materialization", observations[0].reason);
        try std.testing.expectEqualStrings("recommended", observations[0].lifecycle);
        try std.testing.expect(observations[0].recommendation != null);
        try std.testing.expect(std.mem.indexOf(u8, observations[0].shape, "shape:v1") != null);

        try std.testing.expectEqual(@as(u64, 1), try db.evaluateAlgebraicAdaptiveCandidates());
        const backfilling_states = try db.listAlgebraicMaterializationStates(alloc, "alg");
        defer types.freeAlgebraicMaterializationStates(alloc, backfilling_states);
        try std.testing.expectEqual(@as(usize, 1), backfilling_states.len);
        try std.testing.expectEqualStrings("backfilling", backfilling_states[0].lifecycle);
        try std.testing.expectEqual(@as(u64, 1), backfilling_states[0].observation_count);

        const backfilling_observations = try db.listAlgebraicQueryObservations(alloc, "alg");
        defer types.freeAlgebraicQueryObservations(alloc, backfilling_observations);
        try std.testing.expectEqual(@as(usize, 1), backfilling_observations.len);
        try std.testing.expectEqualStrings("backfilling", backfilling_observations[0].lifecycle);

        const missing_states = try db.listAlgebraicMaterializationStates(alloc, "missing");
        defer types.freeAlgebraicMaterializationStates(alloc, missing_states);
        try std.testing.expectEqual(@as(usize, 0), missing_states.len);
    }

    {
        var reopened = try DB.open(alloc, std.mem.span(path), .{ .start_index_workers = false });
        defer reopened.close();

        const stats = try reopened.stats(alloc);
        defer types.freeDBStats(alloc, stats);
        try std.testing.expectEqual(@as(usize, 1), stats.indexes.len);
        try std.testing.expectEqual(@as(u64, 1), stats.indexes[0].algebraic_observed_query_shape_count);
        try std.testing.expectEqual(@as(u64, 1), stats.indexes[0].algebraic_recommendation_count);
        try std.testing.expect(stats.indexes[0].algebraic_last_observed_query_shape != null);
        try std.testing.expect(stats.indexes[0].algebraic_last_recommended_materialization != null);

        const materialization_states = try reopened.listAlgebraicMaterializationStates(alloc, null);
        defer types.freeAlgebraicMaterializationStates(alloc, materialization_states);
        try std.testing.expectEqual(@as(usize, 1), materialization_states.len);
        try std.testing.expectEqualStrings("alg", materialization_states[0].index_name);
        try std.testing.expectEqualStrings("backfilling", materialization_states[0].lifecycle);
        try std.testing.expectEqual(@as(u64, 1), materialization_states[0].observation_count);

        const observations = try reopened.listAlgebraicQueryObservations(alloc, null);
        defer types.freeAlgebraicQueryObservations(alloc, observations);
        try std.testing.expectEqual(@as(usize, 1), observations.len);
        try std.testing.expectEqualStrings("alg", observations[0].index_name);
        try std.testing.expectEqualStrings("backfilling", observations[0].lifecycle);
        try std.testing.expectEqual(@as(u64, 1), observations[0].count);
    }
}

test "db evaluates policy-gated algebraic adaptive candidates" {
    const alloc = std.testing.allocator;
    const algebraic_ir = @import("algebraic/ir.zig");

    var path_buf: [256]u8 = undefined;
    const path = tempPath(&path_buf);
    defer cleanupTempDir(path);

    var db = try DB.open(alloc, std.mem.span(path), .{ .start_index_workers = false });
    defer db.close();

    const cfg =
        \\{
        \\  "version": 1,
        \\  "table": "orders",
        \\  "schema_version": 42,
        \\  "capability_fingerprint": "cap:v1",
        \\  "capability_lifecycle_status": "stale",
        \\  "capability_change_added_fields": 1,
        \\  "capability_change_removed_fields": 2,
        \\  "capability_change_changed_type_fields": 3,
        \\  "skipped_dynamic_fields": 4,
        \\  "skipped_complex_fields": 5,
        \\  "skipped_unbounded_fields": 6,
        \\  "group_fields": [{"name":"customer","path":"customer","type":"string"},{"name":"tenant","path":"tenant","type":"string"}],
        \\  "measure_fields": [{"name":"amount","path":"amount","type":"number"}],
        \\  "adaptive": {"observe": true, "lazy_materialization": true, "dematerialization": true, "min_observations": 1, "min_estimated_scan_rows_saved": 1, "dematerialize_after_observation_misses": 2},
        \\  "materializations": []
        \\}
    ;
    try db.addIndex(.{
        .name = "alg",
        .kind = .algebraic,
        .config_json = cfg,
    });

    const entry = db.core.index_manager.algebraicIndex("alg").?;
    const constraints = [_]algebraic_ir.Constraint{.{ .field = "tenant", .value = "t1" }};
    entry.index.recordObservedQueryShapeWithStore(db.core.store, .{
        .kind = .terms,
        .aggregation_name = "amount_by_customer",
        .bucket_field = "customer",
        .constraints = constraints[0..],
        .metric = .{ .name = "amount", .op = .sum, .field = "amount" },
    }, "no_materialization");

    try std.testing.expectEqual(@as(u64, 1), try db.evaluateAlgebraicAdaptiveCandidates());
    const candidates = try db.listAlgebraicAdaptiveCandidates(alloc, "alg");
    defer types.freeAlgebraicAdaptiveCandidates(alloc, candidates);
    try std.testing.expectEqual(@as(usize, 1), candidates.len);
    try std.testing.expect(std.mem.startsWith(u8, candidates[0].materialization_id, "am_"));
    try std.testing.expectEqualStrings("backfilling", candidates[0].lifecycle);
    try std.testing.expectEqualStrings("auto_backfill_started", candidates[0].decision);
    try std.testing.expect(candidates[0].estimated_scan_rows_saved >= 1);
    try std.testing.expect(candidates[0].estimated_write_cost >= 1);
    try std.testing.expect(candidates[0].estimated_write_amplification > 1);

    const progress = try db.listAlgebraicAdaptiveProgress(alloc, "alg");
    defer types.freeAlgebraicAdaptiveProgress(alloc, progress);
    try std.testing.expectEqual(@as(usize, 1), progress.len);
    try std.testing.expectEqualStrings(candidates[0].materialization_id, progress[0].materialization_id);
    try std.testing.expectEqualStrings("backfilling", progress[0].lifecycle);

    const stats = try db.diagnosticStats(alloc);
    defer types.freeDBStats(alloc, stats);
    try std.testing.expectEqual(@as(usize, 1), stats.indexes.len);
    try std.testing.expectEqual(@as(u32, 42), stats.indexes[0].algebraic_schema_version);
    try std.testing.expectEqualStrings("cap:v1", stats.indexes[0].algebraic_capability_fingerprint.?);
    try std.testing.expectEqualStrings("stale", stats.indexes[0].algebraic_capability_lifecycle_status.?);
    try std.testing.expect(!stats.indexes[0].algebraic_planner_lifecycle_ready);
    try std.testing.expectEqualStrings("capability_lifecycle_not_ready", stats.indexes[0].algebraic_planner_lifecycle_blocking_reason.?);
    try std.testing.expectEqual(@as(u32, 1), stats.indexes[0].algebraic_capability_change_added_fields);
    try std.testing.expectEqual(@as(u32, 2), stats.indexes[0].algebraic_capability_change_removed_fields);
    try std.testing.expectEqual(@as(u32, 3), stats.indexes[0].algebraic_capability_change_changed_type_fields);
    try std.testing.expectEqual(@as(u32, 4), stats.indexes[0].algebraic_skipped_dynamic_fields);
    try std.testing.expectEqual(@as(u32, 5), stats.indexes[0].algebraic_skipped_complex_fields);
    try std.testing.expectEqual(@as(u32, 6), stats.indexes[0].algebraic_skipped_unbounded_fields);
    try std.testing.expectEqual(@as(u64, 1), stats.indexes[0].algebraic_adaptive_candidate_count);
    try std.testing.expectEqual(@as(u64, 1), stats.indexes[0].algebraic_adaptive_progress_count);
    try std.testing.expectEqual(@as(u64, 1), stats.indexes[0].algebraic_adaptive_backfilling_count);
    try std.testing.expectEqual(@as(u64, 1), stats.indexes[0].algebraic_adaptive_decision_history_count);
    try std.testing.expectEqual(@as(u64, 1), stats.indexes[0].algebraic_adaptive_policy_drift_count);
    const top_candidate = stats.indexes[0].algebraic_top_candidate orelse return error.TestUnexpectedResult;
    try std.testing.expectEqualStrings(candidates[0].recommendation, top_candidate.recommendation);
    try std.testing.expectEqualStrings(candidates[0].materialization_id, top_candidate.materialization_id);
    try std.testing.expectEqualStrings("backfilling", top_candidate.lifecycle);
    try std.testing.expectEqualStrings("auto_backfill_started", top_candidate.decision);
    try std.testing.expectEqual(candidates[0].score, top_candidate.score);
    try std.testing.expectEqual(@as(usize, 1), stats.indexes[0].algebraic_candidate_decision_history.len);
    try std.testing.expectEqualStrings("auto_backfill_started", stats.indexes[0].algebraic_candidate_decision_history[0].decision);
    const active_progress = stats.indexes[0].algebraic_active_progress orelse return error.TestUnexpectedResult;
    try std.testing.expectEqualStrings(progress[0].recommendation, active_progress.recommendation);
    try std.testing.expectEqualStrings(progress[0].materialization_id, active_progress.materialization_id);
    try std.testing.expectEqualStrings("backfilling", active_progress.lifecycle);
    try std.testing.expectEqual(progress[0].target_sequence, active_progress.target_sequence);
}

test "db basic batch/get works with memory primary backend" {
    const alloc = std.testing.allocator;

    var path_buf: [256]u8 = undefined;
    const path = tempPath(&path_buf);
    defer cleanupTempDir(path);

    var db = try DB.open(alloc, std.mem.span(path), .{
        .primary_backend = .{ .mem = .{} },
    });
    defer db.close();

    try db.batch(.{
        .writes = &.{
            .{ .key = "doc:a", .value = "{\"name\":\"alpha\"}" },
            .{ .key = "doc:b", .value = "{\"name\":\"beta\"}" },
        },
    });

    const raw = try db.get(alloc, "doc:a");
    defer if (raw) |value| alloc.free(value);
    try std.testing.expect(raw != null);
    try std.testing.expect(std.mem.indexOf(u8, raw.?, "\"alpha\"") != null);
}

test "db batch treats reserved namespace bytes as user document ids" {
    const alloc = std.testing.allocator;

    var path_buf: [256]u8 = undefined;
    const path = tempPath(&path_buf);
    defer cleanupTempDir(path);

    var db = try DB.open(alloc, std.mem.span(path), .{
        .primary_backend = .{ .mem = .{} },
    });
    defer db.close();

    const raw_id = "\x03raw\x00doc";
    try db.batch(.{
        .writes = &.{
            .{ .key = raw_id, .value = "{\"name\":\"binary\"}" },
        },
    });

    const raw = try db.get(alloc, raw_id);
    defer if (raw) |value| alloc.free(value);
    try std.testing.expect(raw != null);
    try std.testing.expect(std.mem.indexOf(u8, raw.?, "\"binary\"") != null);

    var resolved = try db.internalResolveDocSetForIdsAlloc(alloc, &.{raw_id});
    defer resolved.deinit(alloc);
    switch (resolved) {
        .ordinals => |ordinals| {
            try std.testing.expectEqual(@as(usize, 1), ordinals.len);
            try std.testing.expectEqual(@as(doc_set.DocOrdinal, 1), ordinals[0]);
        },
        else => return error.ExpectedOrdinalDocSet,
    }

    const resolved_doc_ids = (try db.internalDocIdsForResolvedDocSetAlloc(alloc, &resolved)).?;
    defer {
        for (resolved_doc_ids) |doc_id| alloc.free(@constCast(doc_id));
        alloc.free(resolved_doc_ids);
    }
    try std.testing.expectEqual(@as(usize, 1), resolved_doc_ids.len);
    try std.testing.expectEqualSlices(u8, raw_id, resolved_doc_ids[0]);
}

test "db batch and scan round trip adversarial document ids" {
    const alloc = std.testing.allocator;

    var path_buf: [256]u8 = undefined;
    const path = tempPath(&path_buf);
    defer cleanupTempDir(path);

    var db = try DB.open(alloc, std.mem.span(path), .{
        .primary_backend = .{ .mem = .{} },
    });
    defer db.close();

    const raw_ids = [_][]const u8{
        "",
        "\x00",
        "\x00\x00",
        "\x00a",
        ":",
        ":e:",
        ":i:",
        ":t",
        "abc\x00def",
        "abc:",
        "abc\xffdef",
        "\xff",
    };

    var writes: [raw_ids.len]types.BatchWrite = undefined;
    for (raw_ids, 0..) |raw_id, i| {
        writes[i] = .{
            .key = raw_id,
            .value = try std.fmt.allocPrint(alloc, "{{\"ordinal\":{d}}}", .{i}),
        };
    }
    defer for (writes) |write| alloc.free(@constCast(write.value));

    try db.batch(.{ .writes = writes[0..] });

    for (raw_ids, 0..) |raw_id, i| {
        const raw = (try db.get(alloc, raw_id)) orelse return error.TestExpectedEqual;
        defer alloc.free(raw);
        const expected = try std.fmt.allocPrint(alloc, "{{\"ordinal\":{d}}}", .{i});
        defer alloc.free(expected);
        try std.testing.expectEqualStrings(expected, raw);
    }

    var resolved = try db.internalResolveDocSetForIdsAlloc(alloc, raw_ids[0..]);
    defer resolved.deinit(alloc);
    switch (resolved) {
        .ordinals => |ordinals| {
            try std.testing.expectEqual(raw_ids.len, ordinals.len);
            for (ordinals, 0..) |ordinal, i| {
                try std.testing.expectEqual(@as(doc_set.DocOrdinal, @intCast(i + 1)), ordinal);
            }
        },
        else => return error.ExpectedOrdinalDocSet,
    }

    const resolved_doc_ids = (try db.internalDocIdsForResolvedDocSetAlloc(alloc, &resolved)).?;
    defer {
        for (resolved_doc_ids) |doc_id| alloc.free(@constCast(doc_id));
        alloc.free(resolved_doc_ids);
    }
    try std.testing.expectEqual(raw_ids.len, resolved_doc_ids.len);
    for (raw_ids, 0..) |raw_id, i| {
        try std.testing.expectEqualSlices(u8, raw_id, resolved_doc_ids[i]);
    }

    var scanned = try db.scan(alloc, "", "", .{ .include_documents = true });
    defer scanned.deinit(alloc);
    try std.testing.expectEqual(raw_ids.len, scanned.hashes.len);
    try std.testing.expectEqual(raw_ids.len, scanned.documents.len);
    for (raw_ids, 0..) |raw_id, i| {
        try std.testing.expectEqualSlices(u8, raw_id, scanned.hashes[i].id);
        try std.testing.expectEqualSlices(u8, raw_id, scanned.documents[i].id);
    }
}

test "db persists configured doc identity namespace for batch writes" {
    const alloc = std.testing.allocator;

    var path_buf: [256]u8 = undefined;
    const path = tempPath(&path_buf);
    defer cleanupTempDir(path);

    const namespace = doc_identity.Namespace{ .table_id = 7, .shard_id = 11, .range_id = 13 };
    {
        var db = try DB.open(alloc, std.mem.span(path), .{
            .start_index_workers = false,
            .identity_namespace = namespace,
        });
        defer db.close();

        try db.batch(.{
            .writes = &.{
                .{ .key = "doc:a", .value = "{\"name\":\"alpha\"}" },
            },
        });

        var txn = try db.core.store.beginProbeTxn();
        defer txn.abort();
        const ordinal = (try doc_identity.lookupOrdinalTxn(alloc, &txn, "doc:a")).?;
        const state = (try doc_identity.lookupStateTxn(&txn, ordinal)).?;
        try std.testing.expectEqual(doc_identity.canonicalDocIdForNamespace(namespace, "doc:a"), state.canonical_doc_id);

        const stats = try db.stats(alloc);
        defer types.freeDBStats(alloc, stats);
        try std.testing.expectEqual(namespace.table_id, stats.doc_identity.namespace_table_id);
        try std.testing.expectEqual(namespace.shard_id, stats.doc_identity.namespace_shard_id);
        try std.testing.expectEqual(namespace.range_id, stats.doc_identity.namespace_range_id);
    }

    {
        var reopened = try DB.open(alloc, std.mem.span(path), .{
            .start_index_workers = false,
        });
        defer reopened.close();
        try std.testing.expect(reopened.core.identity_namespace.eql(namespace));
    }

    try std.testing.expectError(error.IdentityNamespaceMismatch, DB.open(alloc, std.mem.span(path), .{
        .start_index_workers = false,
        .identity_namespace = .{ .table_id = 7, .shard_id = 11, .range_id = 14 },
    }));
}

test "db preferred identity namespace seeds new stores but preserves existing namespace" {
    const alloc = std.testing.allocator;

    var managed_path_buf: [256]u8 = undefined;
    const managed_path = tempPath(&managed_path_buf);
    defer cleanupTempDir(managed_path);

    const managed_namespace = doc_identity.Namespace{ .table_id = 7, .shard_id = 7001, .range_id = 7001 };
    {
        var db = try DB.open(alloc, std.mem.span(managed_path), .{
            .start_index_workers = false,
            .identity_namespace = managed_namespace,
            .prefer_existing_identity_namespace = true,
        });
        defer db.close();
        try db.batch(.{
            .writes = &.{.{ .key = "doc:a", .value = "{\"name\":\"alpha\"}" }},
        });
        try std.testing.expect(db.core.identity_namespace.eql(managed_namespace));
    }

    {
        var reopened = try DB.open(alloc, std.mem.span(managed_path), .{
            .start_index_workers = false,
            .identity_namespace = .{ .table_id = 7, .shard_id = 7002, .range_id = 7002 },
            .prefer_existing_identity_namespace = true,
        });
        defer reopened.close();
        try std.testing.expect(reopened.core.identity_namespace.eql(managed_namespace));
    }

    var legacy_path_buf: [256]u8 = undefined;
    const legacy_path = tempPath(&legacy_path_buf);
    defer cleanupTempDir(legacy_path);

    {
        var legacy = try DB.open(alloc, std.mem.span(legacy_path), .{
            .start_index_workers = false,
        });
        defer legacy.close();
        try legacy.batch(.{
            .writes = &.{.{ .key = "doc:legacy", .value = "{\"name\":\"legacy\"}" }},
        });
    }

    {
        var opened = try DB.open(alloc, std.mem.span(legacy_path), .{
            .start_index_workers = false,
            .identity_namespace = managed_namespace,
            .prefer_existing_identity_namespace = true,
        });
        defer opened.close();
        try std.testing.expect(opened.core.identity_namespace.eql(doc_identity.default_namespace));
    }
}

test "db can reassign identity namespace for rebuild" {
    const alloc = std.testing.allocator;

    var path_buf: [256]u8 = undefined;
    const path = tempPath(&path_buf);
    defer cleanupTempDir(path);

    const old_namespace = doc_identity.Namespace{ .table_id = 8, .shard_id = 801, .range_id = 8001 };
    const new_namespace = doc_identity.Namespace{ .table_id = 8, .shard_id = 802, .range_id = 8002 };
    var db = try DB.open(alloc, std.mem.span(path), .{
        .start_index_workers = false,
        .identity_namespace = old_namespace,
    });
    defer db.close();

    try db.batch(.{
        .writes = &.{.{ .key = "doc:a", .value = "{\"name\":\"alpha\"}" }},
    });

    {
        var txn = try db.core.store.beginProbeTxn();
        defer txn.abort();
        const ordinal = (try doc_identity.lookupOrdinalTxn(alloc, &txn, "doc:a")).?;
        const state = (try doc_identity.lookupStateTxn(&txn, ordinal)).?;
        try std.testing.expectEqual(doc_identity.canonicalDocIdForNamespace(old_namespace, "doc:a"), state.canonical_doc_id);
    }

    try db.reassignIdentityNamespaceForInternalTransition(new_namespace);
    try std.testing.expect(db.core.identity_namespace.eql(new_namespace));

    {
        var txn = try db.core.store.beginProbeTxn();
        defer txn.abort();
        const ordinal = (try doc_identity.lookupOrdinalTxn(alloc, &txn, "doc:a")).?;
        const state = (try doc_identity.lookupStateTxn(&txn, ordinal)).?;
        try std.testing.expectEqual(doc_identity.canonicalDocIdForNamespace(new_namespace, "doc:a"), state.canonical_doc_id);
        try std.testing.expectEqual(@as(u32, 1), ordinal);
        try std.testing.expectEqual(@as(u64, 1), state.created_generation);
    }

    const stats = try db.diagnosticStats(alloc);
    defer types.freeDBStats(alloc, stats);
    try std.testing.expectEqual(new_namespace.table_id, stats.doc_identity.namespace_table_id);
    try std.testing.expectEqual(new_namespace.shard_id, stats.doc_identity.namespace_shard_id);
    try std.testing.expectEqual(new_namespace.range_id, stats.doc_identity.namespace_range_id);
    try std.testing.expect(!stats.doc_identity.rebuild_required);

    const generation = try db.currentIdentityReadGenerationForRequest(null);
    var filter = doc_set.ResolvedDocFilter{
        .include = try doc_set.fromOrdinalsAlloc(alloc, &.{1}),
    };
    defer filter.deinit(alloc);

    try std.testing.expectError(error.DocIdentityNamespaceMismatch, db.search(alloc, .{
        .limit = 10,
        .identity_read_generation = generation,
        .resolved_doc_filter = &filter,
        .resolved_doc_filter_wire_context = .{
            .namespace = old_namespace,
            .identity_read_generation = generation,
        },
    }));

    var ok = try db.search(alloc, .{
        .limit = 10,
        .identity_read_generation = generation,
        .resolved_doc_filter = &filter,
        .resolved_doc_filter_wire_context = .{
            .namespace = new_namespace,
            .identity_read_generation = generation,
        },
    });
    defer ok.deinit();
    try std.testing.expectEqual(@as(u32, 1), ok.total_hits);
}

test "db strict namespace reopen recovers after identity reassignment repair" {
    const alloc = std.testing.allocator;

    var path_buf: [256]u8 = undefined;
    const path = tempPath(&path_buf);
    defer cleanupTempDir(path);

    const old_namespace = doc_identity.Namespace{ .table_id = 38, .shard_id = 3801, .range_id = 38001 };
    const new_namespace = doc_identity.Namespace{ .table_id = 38, .shard_id = 3802, .range_id = 38002 };

    {
        var db = try DB.open(alloc, std.mem.span(path), .{
            .start_index_workers = false,
            .identity_namespace = old_namespace,
        });
        defer db.close();

        try db.batch(.{
            .writes = &.{.{ .key = "doc:a", .value = "{\"name\":\"alpha\"}" }},
        });
    }

    try std.testing.expectError(error.IdentityNamespaceMismatch, DB.open(alloc, std.mem.span(path), .{
        .start_index_workers = false,
        .identity_namespace = new_namespace,
    }));

    {
        var db = try DB.open(alloc, std.mem.span(path), .{
            .start_index_workers = false,
            .identity_namespace = old_namespace,
        });
        defer db.close();
        try db.reassignIdentityNamespaceForInternalTransition(new_namespace);
    }

    {
        var repaired = try DB.open(alloc, std.mem.span(path), .{
            .start_index_workers = false,
            .identity_namespace = new_namespace,
        });
        defer repaired.close();

        var filtered = try repaired.search(alloc, .{
            .query = .{ .match_all = {} },
            .filter_doc_ids = &.{"doc:a"},
            .filter_doc_ids_positive = true,
            .limit = 10,
        });
        defer filtered.deinit();
        try std.testing.expectEqual(@as(u32, 1), filtered.total_hits);
        try std.testing.expectEqualStrings("doc:a", filtered.hits[0].id);
        try std.testing.expectEqual(@as(?doc_set.DocOrdinal, 1), filtered.hits[0].doc_ordinal);

        var txn = try repaired.core.store.beginProbeTxn();
        defer txn.abort();
        const ordinal = (try doc_identity.lookupOrdinalTxn(alloc, &txn, "doc:a")).?;
        const state = (try doc_identity.lookupStateTxn(&txn, ordinal)).?;
        try std.testing.expectEqual(doc_identity.canonicalDocIdForNamespace(new_namespace, "doc:a"), state.canonical_doc_id);
        try std.testing.expectEqual(@as(?doc_identity.DocOrdinal, null), try doc_identity.lookupCanonicalOrdinalTxn(&txn, doc_identity.canonicalDocIdForNamespace(old_namespace, "doc:a")));
    }
}

test "db identity namespace reassignment refreshes transaction recovery hook context" {
    const alloc = std.testing.allocator;

    var path_buf: [256]u8 = undefined;
    const path = tempPath(&path_buf);
    defer cleanupTempDir(path);

    const old_namespace = doc_identity.Namespace{ .table_id = 28, .shard_id = 2801, .range_id = 28001 };
    const new_namespace = doc_identity.Namespace{ .table_id = 28, .shard_id = 2802, .range_id = 28002 };
    var recorder = TxnResolverRecorder{};
    var db = try DB.open(alloc, std.mem.span(path), .{
        .start_index_workers = false,
        .identity_namespace = old_namespace,
        .transaction_recovery = .{
            .enabled = true,
            .interval_ms = 60_000,
            .resolver_ctx = &recorder,
            .resolve_participant_fn = TxnResolverRecorder.resolve,
        },
    });
    defer db.close();

    try db.batch(.{
        .writes = &.{.{ .key = "doc:a", .value = "{\"name\":\"alpha\"}" }},
    });

    const identity_ctx = db.transaction_recovery_identity_context orelse return error.TestExpectedEqual;
    try std.testing.expect(identity_ctx.identity_namespace.eql(old_namespace));
    try std.testing.expect(db.transaction_runtime.?.config.resolution_extra_hooks.build != null);
    try std.testing.expectEqual(
        @intFromPtr(identity_ctx),
        @intFromPtr(db.transaction_runtime.?.config.resolution_extra_hooks.ctx.?),
    );

    try db.reassignIdentityNamespaceForInternalTransition(new_namespace);
    try std.testing.expect(db.core.identity_namespace.eql(new_namespace));
    try std.testing.expect(identity_ctx.identity_namespace.eql(new_namespace));

    var txn = try db.core.store.beginProbeTxn();
    defer txn.abort();
    const ordinal = (try doc_identity.lookupOrdinalTxn(alloc, &txn, "doc:a")).?;
    const state = (try doc_identity.lookupStateTxn(&txn, ordinal)).?;
    try std.testing.expectEqual(doc_identity.canonicalDocIdForNamespace(new_namespace, "doc:a"), state.canonical_doc_id);
}

test "db setSchema refreshes transaction recovery relational mode context" {
    const alloc = std.testing.allocator;

    var path_buf: [256]u8 = undefined;
    const path = tempPath(&path_buf);
    defer cleanupTempDir(path);

    var recorder = TxnResolverRecorder{};
    var db = try DB.open(alloc, std.mem.span(path), .{
        .start_index_workers = false,
        .transaction_recovery = .{
            .enabled = true,
            .interval_ms = 60_000,
            .resolver_ctx = &recorder,
            .resolve_participant_fn = TxnResolverRecorder.resolve,
        },
    });
    defer db.close();

    const identity_ctx = db.transaction_recovery_identity_context orelse return error.TestExpectedEqual;
    try std.testing.expect(!identity_ctx.relational_base_rows);

    const schema_json =
        \\{"version":1,"storage_mode":"relational","default_type":"row","enforce_types":true,"document_schemas":{"row":{"schema":{"type":"object","properties":{"title":{"type":"text"}},"required":["title"],"additionalProperties":false}}}}
    ;
    var parsed_schema = try schema_api_mod.parseValidatedTableSchema(alloc, schema_json);
    defer parsed_schema.deinit(alloc);
    const runtime_schema = try schema_api_mod.deriveRuntimeTableSchema(alloc, parsed_schema);
    defer schema_mod.freeSchema(alloc, runtime_schema);
    try db.setSchema(runtime_schema);

    try std.testing.expect(identity_ctx.relational_base_rows);
}

test "db identity namespace reassignment is unavailable on status-only handles" {
    const alloc = std.testing.allocator;

    var path_buf: [256]u8 = undefined;
    const path = tempPath(&path_buf);
    defer cleanupTempDir(path);

    const old_namespace = doc_identity.Namespace{ .table_id = 18, .shard_id = 1801, .range_id = 18001 };
    const new_namespace = doc_identity.Namespace{ .table_id = 18, .shard_id = 1802, .range_id = 18002 };
    {
        var db = try DB.open(alloc, std.mem.span(path), .{
            .start_index_workers = false,
            .identity_namespace = old_namespace,
        });
        defer db.close();
        try db.batch(.{
            .writes = &.{.{ .key = "doc:a", .value = "{\"name\":\"alpha\"}" }},
        });
    }

    {
        var status_db = try DB.open(alloc, std.mem.span(path), .{
            .open_mode = .status_only,
            .start_index_workers = false,
            .identity_namespace = old_namespace,
        });
        defer status_db.close();
        try std.testing.expectError(error.ReadOnly, status_db.reassignIdentityNamespaceForInternalTransition(new_namespace));
        const stats = try status_db.runtimeStatusStatsConsistent(alloc);
        defer types.freeDBStats(alloc, stats);
        try std.testing.expectEqual(old_namespace.table_id, stats.doc_identity.namespace_table_id);
        try std.testing.expectEqual(old_namespace.shard_id, stats.doc_identity.namespace_shard_id);
        try std.testing.expectEqual(old_namespace.range_id, stats.doc_identity.namespace_range_id);
    }

    {
        var reopened = try DB.open(alloc, std.mem.span(path), .{
            .start_index_workers = false,
            .identity_namespace = old_namespace,
        });
        defer reopened.close();
        try std.testing.expect(reopened.core.identity_namespace.eql(old_namespace));
    }
}

test "db stats expose document identity coverage and tombstones" {
    const alloc = std.testing.allocator;

    var path_buf: [256]u8 = undefined;
    const path = tempPath(&path_buf);
    defer cleanupTempDir(path);

    var db = try DB.open(alloc, std.mem.span(path), .{
        .primary_backend = .{ .mem = .{} },
    });
    defer db.close();

    try db.batch(.{
        .writes = &.{
            .{ .key = "doc:a", .value = "{\"name\":\"alpha\"}" },
            .{ .key = "doc:b", .value = "{\"name\":\"beta\"}" },
        },
    });

    var resolved_existing = try db.internalResolveDocSetForIdsAlloc(alloc, &.{"doc:a"});
    defer resolved_existing.deinit(alloc);
    var resolved_missing = try db.internalResolveDocSetForIdsAlloc(alloc, &.{"doc:missing"});
    defer resolved_missing.deinit(alloc);

    const fast = try db.stats(alloc);
    defer types.freeDBStats(alloc, fast);
    try std.testing.expectEqual(@as(u32, 3), fast.doc_identity.next_ordinal);
    try std.testing.expectEqual(@as(u64, 2), fast.doc_identity.allocated_ordinals);
    try std.testing.expectEqual(@as(u64, std.math.maxInt(doc_identity.DocOrdinal) - 3), fast.doc_identity.ordinal_capacity_remaining);
    try std.testing.expect(!fast.doc_identity.ordinal_capacity_exhausted);
    try std.testing.expect(!fast.doc_identity.rebuild_required);
    try std.testing.expect(!fast.doc_identity.complete);
    try std.testing.expectEqual(@as(u64, 2), fast.doc_set_planning.resolved_set_count);
    try std.testing.expectEqual(@as(u64, 1), fast.doc_set_planning.ordinal_list_count);
    try std.testing.expectEqual(@as(u64, 1), fast.doc_set_planning.ordinal_list_docs);
    try std.testing.expectEqual(@as(u64, 1), fast.doc_set_planning.doc_key_list_count);
    try std.testing.expectEqual(@as(u64, 1), fast.doc_set_planning.doc_key_list_docs);
    try std.testing.expectEqual(@as(u64, 1), fast.doc_set_planning.missing_ordinal_coverage_count);

    try db.batch(.{
        .deletes = &.{"doc:b"},
    });

    const diagnostic = try db.diagnosticStats(alloc);
    defer types.freeDBStats(alloc, diagnostic);
    try std.testing.expect(diagnostic.doc_identity.complete);
    try std.testing.expectEqual(@as(u32, 3), diagnostic.doc_identity.next_ordinal);
    try std.testing.expectEqual(@as(u64, 2), diagnostic.doc_identity.allocated_ordinals);
    try std.testing.expectEqual(@as(u64, std.math.maxInt(doc_identity.DocOrdinal) - 3), diagnostic.doc_identity.ordinal_capacity_remaining);
    try std.testing.expect(!diagnostic.doc_identity.ordinal_capacity_exhausted);
    try std.testing.expect(!diagnostic.doc_identity.rebuild_required);
    try std.testing.expectEqual(@as(u64, 2), diagnostic.doc_identity.state_rows);
    try std.testing.expectEqual(@as(u64, 1), diagnostic.doc_identity.live_ordinals);
    try std.testing.expectEqual(@as(u64, 1), diagnostic.doc_identity.tombstone_ordinals);
    try std.testing.expect(diagnostic.doc_identity.min_created_generation > 0);
    try std.testing.expectEqual(diagnostic.doc_identity.min_created_generation, diagnostic.doc_identity.max_created_generation);
    try std.testing.expect(diagnostic.doc_identity.min_deleted_generation > diagnostic.doc_identity.min_created_generation);
    try std.testing.expectEqual(diagnostic.doc_identity.min_deleted_generation, diagnostic.doc_identity.max_deleted_generation);
    try std.testing.expectEqual(@as(u64, 1), diagnostic.doc_identity.scanned_primary_docs);
    try std.testing.expectEqual(@as(u64, 0), diagnostic.doc_identity.primary_docs_missing_ordinals);
    try std.testing.expectEqual(@as(u64, 0), diagnostic.doc_identity.primary_docs_missing_identity_state);
    try std.testing.expectEqual(@as(u64, 0), diagnostic.doc_identity.primary_docs_with_tombstone_ordinals);

    const legacy_key = try internal_keys.documentKeyAlloc(alloc, "doc:legacy");
    defer alloc.free(legacy_key);
    try db.core.store.put(legacy_key, "{\"name\":\"legacy\"}");

    const resurrected_key = try internal_keys.documentKeyAlloc(alloc, "doc:b");
    defer alloc.free(resurrected_key);
    try db.core.store.put(resurrected_key, "{\"name\":\"resurrected\"}");

    const repaired_diagnostic = try db.diagnosticStats(alloc);
    defer types.freeDBStats(alloc, repaired_diagnostic);
    try std.testing.expectEqual(@as(u64, 3), repaired_diagnostic.doc_identity.scanned_primary_docs);
    try std.testing.expectEqual(@as(u64, 1), repaired_diagnostic.doc_identity.primary_docs_missing_ordinals);
    try std.testing.expectEqual(@as(u64, 0), repaired_diagnostic.doc_identity.primary_docs_missing_identity_state);
    try std.testing.expectEqual(@as(u64, 1), repaired_diagnostic.doc_identity.primary_docs_with_tombstone_ordinals);
    try std.testing.expect(repaired_diagnostic.doc_identity.rebuild_required);
}

test "db stats flag document identity ordinal capacity exhaustion" {
    const alloc = std.testing.allocator;

    var path_buf: [256]u8 = undefined;
    const path = tempPath(&path_buf);
    defer cleanupTempDir(path);

    var db = try DB.open(alloc, std.mem.span(path), .{
        .primary_backend = .{ .mem = .{} },
    });
    defer db.close();

    var value: [4]u8 = undefined;
    std.mem.writeInt(u32, &value, std.math.maxInt(doc_identity.DocOrdinal), .big);
    try db.core.store.put(internal_keys.identity_next_ordinal_key[0..], &value);

    const stats = try db.stats(alloc);
    defer types.freeDBStats(alloc, stats);
    try std.testing.expectEqual(std.math.maxInt(doc_identity.DocOrdinal), stats.doc_identity.next_ordinal);
    try std.testing.expectEqual(@as(u64, 0), stats.doc_identity.ordinal_capacity_remaining);
    try std.testing.expect(stats.doc_identity.ordinal_capacity_exhausted);
    try std.testing.expect(stats.doc_identity.rebuild_required);

    try std.testing.expectError(error.DocOrdinalExhausted, db.batch(.{
        .writes = &.{.{ .key = "doc:overflow", .value = "{\"name\":\"overflow\"}" }},
    }));
}

test "db allocates final document ordinal then rejects new documents" {
    const alloc = std.testing.allocator;

    var path_buf: [256]u8 = undefined;
    const path = tempPath(&path_buf);
    defer cleanupTempDir(path);

    var db = try DB.open(alloc, std.mem.span(path), .{
        .primary_backend = .{ .mem = .{} },
    });
    defer db.close();

    const last_allocatable = std.math.maxInt(doc_identity.DocOrdinal) - 1;
    var value: [4]u8 = undefined;
    std.mem.writeInt(u32, &value, last_allocatable, .big);
    try db.core.store.put(internal_keys.identity_next_ordinal_key[0..], &value);

    try db.batch(.{
        .writes = &.{.{ .key = "doc:last", .value = "{\"name\":\"last\"}" }},
        .sync_level = .write,
    });

    {
        var txn = try db.core.store.beginProbeTxn();
        defer txn.abort();
        try std.testing.expectEqual(@as(?doc_identity.DocOrdinal, last_allocatable), try doc_identity.lookupOrdinalTxn(alloc, &txn, "doc:last"));
    }

    const exhausted = try db.stats(alloc);
    defer types.freeDBStats(alloc, exhausted);
    try std.testing.expectEqual(std.math.maxInt(doc_identity.DocOrdinal), exhausted.doc_identity.next_ordinal);
    try std.testing.expectEqual(@as(u64, 0), exhausted.doc_identity.ordinal_capacity_remaining);
    try std.testing.expect(exhausted.doc_identity.ordinal_capacity_exhausted);
    try std.testing.expect(exhausted.doc_identity.rebuild_required);

    try db.batch(.{
        .writes = &.{.{ .key = "doc:last", .value = "{\"name\":\"updated-last\"}" }},
        .sync_level = .full_index,
    });
    const updated = (try db.get(alloc, "doc:last")) orelse return error.TestExpectedEqual;
    defer alloc.free(updated);
    try std.testing.expectEqualStrings("{\"name\":\"updated-last\"}", updated);

    try std.testing.expectError(error.DocOrdinalExhausted, db.batch(.{
        .writes = &.{.{ .key = "doc:overflow", .value = "{\"name\":\"overflow\"}" }},
        .sync_level = .write,
    }));
}

test "db allocates final document ordinal with all index families present" {
    const alloc = std.testing.allocator;

    var path_buf: [256]u8 = undefined;
    const path = tempPath(&path_buf);
    defer cleanupTempDir(path);

    var db = try DB.open(alloc, std.mem.span(path), .{
        .primary_backend = .{ .mem = .{} },
    });
    defer db.close();

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
    try db.addIndex(.{
        .name = "sp_v1",
        .kind = .sparse_vector,
        .config_json = "{\"field\":\"sparse\"}",
    });
    try db.addIndex(.{
        .name = "graph_v1",
        .kind = .graph,
        .config_json = "{}",
    });
    try db.addIndex(.{
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
    });

    const last_allocatable = std.math.maxInt(doc_identity.DocOrdinal) - 1;
    var value: [4]u8 = undefined;
    std.mem.writeInt(u32, &value, last_allocatable, .big);
    try db.core.store.put(internal_keys.identity_next_ordinal_key[0..], &value);

    try db.batch(.{
        .writes = &.{.{
            .key = "doc:last",
            .value = "{\"body\":\"final ordinal token\",\"category\":\"keep\",\"score\":1.0,\"embedding\":[0.0,1.0],\"sparse\":{\"indices\":[1],\"values\":[1.0]}}",
        }},
        .graph_writes = &.{.{
            .index_name = "graph_v1",
            .source = "doc:last",
            .target = "doc:last",
            .edge_type = "self",
            .weight = 1.0,
        }},
        .sync_level = .full_index,
    });

    {
        var txn = try db.core.store.beginProbeTxn();
        defer txn.abort();
        try std.testing.expectEqual(@as(?doc_identity.DocOrdinal, last_allocatable), try doc_identity.lookupOrdinalTxn(alloc, &txn, "doc:last"));
    }

    const exhausted = try db.stats(alloc);
    defer types.freeDBStats(alloc, exhausted);
    try std.testing.expectEqual(std.math.maxInt(doc_identity.DocOrdinal), exhausted.doc_identity.next_ordinal);
    try std.testing.expect(exhausted.doc_identity.ordinal_capacity_exhausted);
    try std.testing.expect(exhausted.doc_identity.rebuild_required);

    try std.testing.expectError(error.DocOrdinalExhausted, db.batch(.{
        .writes = &.{.{ .key = "doc:overflow", .value = "{\"body\":\"overflow\"}" }},
        .sync_level = .full_index,
    }));
}

test "db rejects new document writes at ordinal exhaustion for every sync level" {
    const alloc = std.testing.allocator;

    var path_buf: [256]u8 = undefined;
    const path = tempPath(&path_buf);
    defer cleanupTempDir(path);

    var db = try DB.open(alloc, std.mem.span(path), .{
        .primary_backend = .{ .mem = .{} },
    });
    defer db.close();

    try db.batch(.{
        .writes = &.{.{ .key = "doc:existing", .value = "{\"name\":\"existing\"}" }},
        .sync_level = .write,
    });

    var value: [4]u8 = undefined;
    std.mem.writeInt(u32, &value, std.math.maxInt(doc_identity.DocOrdinal), .big);
    try db.core.store.put(internal_keys.identity_next_ordinal_key[0..], &value);

    try db.batch(.{
        .writes = &.{.{ .key = "doc:existing", .value = "{\"name\":\"updated\"}" }},
        .sync_level = .full_index,
    });
    const existing = (try db.get(alloc, "doc:existing")) orelse return error.TestExpectedEqual;
    defer alloc.free(existing);
    try std.testing.expectEqualStrings("{\"name\":\"updated\"}", existing);

    const levels = [_]types.SyncLevel{
        .propose,
        .write,
        .full_text,
        .enrichments,
        .aknn,
        .full_index,
    };
    for (levels, 0..) |level, i| {
        var key_buf: [32]u8 = undefined;
        const key = try std.fmt.bufPrint(&key_buf, "doc:new:{d}", .{i});
        try std.testing.expectError(error.DocOrdinalExhausted, db.batch(.{
            .writes = &.{.{ .key = key, .value = "{\"name\":\"new\"}" }},
            .sync_level = level,
        }));
        try std.testing.expect((try db.get(alloc, key)) == null);
    }
}

test "db caches identity visibility summary after local writes" {
    const alloc = std.testing.allocator;

    var path_buf: [256]u8 = undefined;
    const path = tempPath(&path_buf);
    defer cleanupTempDir(path);

    var db = try DB.open(alloc, std.mem.span(path), .{
        .primary_backend = .{ .mem = .{} },
    });
    defer db.close();

    try db.batch(.{
        .writes = &.{.{ .key = "doc:a", .value = "{\"name\":\"alpha\"}" }},
    });
    try std.testing.expect(db.identity_visibility_summary_cache != null);
    try std.testing.expect(try db.internalAllDocsVisibleAtGeneration(db.core.nextDerivedSequence()));

    try db.batch(.{
        .deletes = &.{"doc:a"},
    });
    try std.testing.expect(db.identity_visibility_summary_cache != null);
    try std.testing.expect(!(try db.internalAllDocsVisibleAtGeneration(db.core.nextDerivedSequence())));
}

test "db lsm primary compaction preserves doc identity ordinals" {
    const alloc = std.testing.allocator;

    var path_buf: [256]u8 = undefined;
    const path = tempPath(&path_buf);
    defer cleanupTempDir(path);

    const primary_backend: PrimaryBackend = .{ .lsm = .{ .flush_threshold = 1 } };
    {
        var db = try DB.open(alloc, std.mem.span(path), .{
            .primary_backend = primary_backend,
        });
        defer db.close();

        try db.batch(.{
            .writes = &.{
                .{ .key = "doc:a", .value = "{\"body\":\"alpha\"}" },
                .{ .key = "doc:b", .value = "{\"body\":\"beta\"}" },
            },
            .sync_level = .write,
        });
        try db.core.store.flushBufferedWritesWithOptions(.{ .compact = true });

        {
            var txn = try db.core.store.beginProbeTxn();
            defer txn.abort();
            try std.testing.expectEqual(@as(?doc_identity.DocOrdinal, 1), try doc_identity.lookupOrdinalTxn(alloc, &txn, "doc:a"));
            try std.testing.expectEqual(@as(?doc_identity.DocOrdinal, 2), try doc_identity.lookupOrdinalTxn(alloc, &txn, "doc:b"));
            const doc_b = (try doc_identity.lookupDocIdTxn(alloc, &txn, 2)) orelse return error.TestUnexpectedResult;
            defer alloc.free(doc_b);
            try std.testing.expectEqualStrings("doc:b", doc_b);
        }

        try db.batch(.{
            .writes = &.{
                .{ .key = "doc:c", .value = "{\"body\":\"gamma\"}" },
                .{ .key = "doc:a", .value = "{\"body\":\"alpha updated\"}" },
            },
            .deletes = &.{"doc:b"},
            .sync_level = .write,
        });
        try db.core.store.flushBufferedWritesWithOptions(.{ .compact = true });

        var compacted_txn = try db.core.store.beginProbeTxn();
        defer compacted_txn.abort();
        try std.testing.expectEqual(@as(?doc_identity.DocOrdinal, 1), try doc_identity.lookupOrdinalTxn(alloc, &compacted_txn, "doc:a"));
        try std.testing.expectEqual(@as(?doc_identity.DocOrdinal, 2), try doc_identity.lookupOrdinalTxn(alloc, &compacted_txn, "doc:b"));
        try std.testing.expectEqual(@as(?doc_identity.DocOrdinal, 3), try doc_identity.lookupOrdinalTxn(alloc, &compacted_txn, "doc:c"));
        const doc_c = (try doc_identity.lookupDocIdTxn(alloc, &compacted_txn, 3)) orelse return error.TestUnexpectedResult;
        defer alloc.free(doc_c);
        try std.testing.expectEqualStrings("doc:c", doc_c);
    }

    {
        var reopened = try DB.open(alloc, std.mem.span(path), .{
            .primary_backend = primary_backend,
        });
        defer reopened.close();

        var txn = try reopened.core.store.beginProbeTxn();
        defer txn.abort();
        try std.testing.expectEqual(@as(?doc_identity.DocOrdinal, 1), try doc_identity.lookupOrdinalTxn(alloc, &txn, "doc:a"));
        try std.testing.expectEqual(@as(?doc_identity.DocOrdinal, 2), try doc_identity.lookupOrdinalTxn(alloc, &txn, "doc:b"));
        try std.testing.expectEqual(@as(?doc_identity.DocOrdinal, 3), try doc_identity.lookupOrdinalTxn(alloc, &txn, "doc:c"));
        const doc_a = (try doc_identity.lookupDocIdTxn(alloc, &txn, 1)) orelse return error.TestUnexpectedResult;
        defer alloc.free(doc_a);
        try std.testing.expectEqualStrings("doc:a", doc_a);
    }
}

test "db dense and sparse vector searches apply stored symbolic filters before final paging" {
    const alloc = std.testing.allocator;

    var path_buf: [256]u8 = undefined;
    const path = tempPath(&path_buf);
    defer cleanupTempDir(path);

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

test "db dense algebraic doc facts feed native dense and sparse symbolic filters" {
    const alloc = std.testing.allocator;

    var path_buf: [256]u8 = undefined;
    const path = tempPath(&path_buf);
    defer cleanupTempDir(path);

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

test "db vector symbolic filters fail closed when algebraic lifecycle is stale" {
    const alloc = std.testing.allocator;

    var path_buf: [256]u8 = undefined;
    const path = tempPath(&path_buf);
    defer cleanupTempDir(path);

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
        .name = "alg",
        .kind = .algebraic,
        .config_json =
        \\{
        \\  "version": 1,
        \\  "table": "docs",
        \\  "capability_lifecycle_status": "rebuild_required",
        \\  "group_fields": [{"name":"category","path":"category","type":"string"}],
        \\  "materializations": [{"name":"count_by_category","op":"count","group_by":["category"]}]
        \\}
        ,
    });

    try db.batch(.{
        .writes = &.{
            .{ .key = "doc:a", .value = "{\"category\":\"reject\",\"embedding\":[0,0],\"sparse\":{\"indices\":[1],\"values\":[1.0]}}" },
            .{ .key = "doc:b", .value = "{\"category\":\"keep\",\"embedding\":[10,0],\"sparse\":{\"indices\":[1],\"values\":[0.1]}}" },
        },
        .sync_level = .full_index,
    });

    const alg_entry = db.core.index_manager.algebraicIndex("alg") orelse return error.TestUnexpectedResult;
    try std.testing.expect(!alg_entry.index.plannerLifecycleReady());

    try std.testing.expectError(error.UnsupportedQueryRequest, db.searchDenseProfiled(alloc, .{
        .index_name = "dv_v1",
        .limit = 1,
        .include_stored = false,
        .filter_query_json = "{\"term\":{\"category\":\"keep\"}}",
        .require_algebraic_filter_resolution = true,
    }, .{ .vector = &.{ 0.0, 0.0 }, .k = 1 }));
    {
        const status_value = alg_entry.index.status();
        try std.testing.expectEqual(@as(u64, 1), status_value.vector_filter_attempt_count);
        try std.testing.expectEqual(@as(u64, 0), status_value.vector_filter_resolved_count);
        try std.testing.expectEqual(@as(u64, 1), status_value.vector_filter_unsupported_count);
        try std.testing.expectEqual(@as(u64, 1), status_value.vector_filter_fail_closed_count);
        try std.testing.expectEqualStrings("capability_lifecycle_not_ready", status_value.planner_lifecycle_blocking_reason.?);
    }

    try std.testing.expectError(error.UnsupportedQueryRequest, db.search(alloc, .{
        .index_name = "sp_v1",
        .query = .{ .sparse_knn = .{
            .indices = &.{1},
            .values = &.{1.0},
            .k = 1,
        } },
        .limit = 1,
        .include_stored = false,
        .filter_query_json = "{\"term\":{\"category\":\"keep\"}}",
        .require_algebraic_filter_resolution = true,
    }));
    {
        const status_value = alg_entry.index.status();
        try std.testing.expectEqual(@as(u64, 2), status_value.vector_filter_attempt_count);
        try std.testing.expectEqual(@as(u64, 0), status_value.vector_filter_resolved_count);
        try std.testing.expectEqual(@as(u64, 2), status_value.vector_filter_unsupported_count);
        try std.testing.expectEqual(@as(u64, 2), status_value.vector_filter_fail_closed_count);
    }
}

test "db basic batch/get works with in-memory lsm primary backend" {
    const alloc = std.testing.allocator;

    var path_buf: [256]u8 = undefined;
    const path = tempPath(&path_buf);
    defer cleanupTempDir(path);

    var db = try DB.open(alloc, std.mem.span(path), .{
        .primary_backend = .{ .lsm_memory = .{} },
    });
    defer db.close();

    try db.batch(.{
        .writes = &.{
            .{ .key = "doc:a", .value = "{\"name\":\"alpha\"}" },
            .{ .key = "doc:b", .value = "{\"name\":\"beta\"}" },
        },
    });

    const raw = try db.get(alloc, "doc:b");
    defer if (raw) |value| alloc.free(value);
    try std.testing.expect(raw != null);
    try std.testing.expect(std.mem.indexOf(u8, raw.?, "\"beta\"") != null);
}

test "db in-memory primary backends keep derived log off disk" {
    const alloc = std.testing.allocator;

    inline for ([_]PrimaryBackend{
        .{ .mem = .{} },
        .{ .lsm_memory = .{} },
    }) |primary_backend| {
        var path_buf: [256]u8 = undefined;
        const path = tempPath(&path_buf);
        defer cleanupTempDir(path);

        {
            var db = try DB.open(alloc, std.mem.span(path), .{
                .primary_backend = primary_backend,
            });
            defer db.close();

            try db.batch(.{
                .writes = &.{
                    .{ .key = "doc:a", .value = "{\"name\":\"alpha\"}" },
                },
            });
        }

        const derived_log_path = try std.fmt.allocPrint(alloc, "{s}/derived_log", .{std.mem.span(path)});
        defer alloc.free(derived_log_path);
        try std.testing.expectError(error.FileNotFound, std.Io.Dir.cwd().openDir(std.testing.io, derived_log_path, .{}));
    }
}

test "db can override change journal backend to lmdb" {
    const alloc = std.testing.allocator;

    var path_buf: [256]u8 = undefined;
    const path = tempPath(&path_buf);
    defer cleanupTempDir(path);

    {
        var db = try DB.open(alloc, std.mem.span(path), .{
            .primary_backend = .{ .lsm_memory = .{} },
            .change_journal_backend = .lmdb,
        });
        defer db.close();

        try db.batch(.{
            .writes = &.{
                .{ .key = "doc:a", .value = "{\"name\":\"alpha\"}" },
            },
        });
    }

    const change_journal_path = try std.fmt.allocPrint(alloc, "{s}/change_journal", .{std.mem.span(path)});
    defer alloc.free(change_journal_path);
    var dir = try std.Io.Dir.cwd().openDir(std.testing.io, change_journal_path, .{});
    dir.close(std.testing.io);
}

test "db basic batch/get survives reopen with durable lsm primary backend" {
    const alloc = std.testing.allocator;

    var path_buf: [256]u8 = undefined;
    const path = tempPath(&path_buf);
    defer cleanupTempDir(path);

    {
        var db = try DB.open(alloc, std.mem.span(path), .{
            .primary_backend = .{ .lsm = db_config.primary_lsm_options_default },
        });
        defer db.close();

        try db.batch(.{
            .writes = &.{
                .{ .key = "doc:a", .value = "{\"name\":\"alpha\"}" },
                .{ .key = "doc:b", .value = "{\"name\":\"beta\"}" },
            },
        });
    }

    {
        var reopened = try DB.open(alloc, std.mem.span(path), .{
            .primary_backend = .{ .lsm = .{ .flush_threshold = 1 } },
        });
        defer reopened.close();

        const raw = try reopened.get(alloc, "doc:a");
        defer if (raw) |value| alloc.free(value);
        try std.testing.expect(raw != null);
        try std.testing.expect(std.mem.indexOf(u8, raw.?, "\"alpha\"") != null);
    }
}

test "db lsm match-all query sees same latest value as point lookup" {
    const alloc = std.testing.allocator;

    var path_buf: [256]u8 = undefined;
    const path = tempPath(&path_buf);
    defer cleanupTempDir(path);

    var db = try DB.open(alloc, std.mem.span(path), .{
        .primary_backend = .{ .lsm = .{ .flush_threshold = 1 } },
    });
    defer db.close();

    try db.batch(.{
        .writes = &.{
            .{ .key = "user-1", .value = "{\"name\":\"Alice\",\"tier\":\"gold\"}" },
            .{ .key = "user-2", .value = "{\"name\":\"Bob\",\"tier\":\"silver\"}" },
        },
        .sync_level = .write,
    });

    try db.batch(.{
        .writes = &.{
            .{ .key = "user-1", .value = "{\"name\":\"Alice\",\"tier\":\"platinum\"}" },
        },
        .sync_level = .write,
    });

    const raw = try db.get(alloc, "user-1");
    defer if (raw) |value| alloc.free(value);
    try std.testing.expect(raw != null);
    try std.testing.expect(std.mem.indexOf(u8, raw.?, "\"platinum\"") != null);

    var result = try db.search(alloc, .{
        .query = .match_all,
        .fields = &.{ "name", "tier" },
        .limit = 10,
    });
    defer result.deinit();

    var saw_user_1 = false;
    for (result.hits) |hit| {
        if (!std.mem.eql(u8, hit.id, "user-1")) continue;
        saw_user_1 = true;
        try std.testing.expect(hit.stored_data != null);
        try std.testing.expect(std.mem.indexOf(u8, hit.stored_data.?, "\"platinum\"") != null);
        try std.testing.expect(std.mem.indexOf(u8, hit.stored_data.?, "\"gold\"") == null);
    }
    try std.testing.expect(saw_user_1);
}

test "db enrichment status changes notify query visibility hook" {
    const alloc = std.testing.allocator;

    var path_buf: [256]u8 = undefined;
    const path = tempPath(&path_buf);
    defer cleanupTempDir(path);

    var deterministic = embedder_mod.DeterministicDenseEmbedder{};
    var db = try DB.open(alloc, std.mem.span(path), .{
        .enrichment = .{ .dense_embedder = deterministic.interface() },
    });
    defer db.close();

    const HookCtx = struct {
        calls: u64 = 0,
        table_name: ?[]const u8 = null,
        group_id: u64 = 0,
        saw_db: bool = false,
        change: ?QueryVisibilityChange = null,

        fn onChange(ptr: *anyopaque, table_name: []const u8, group_id: u64, changed_db: ?*DB, change: QueryVisibilityChange) void {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            self.calls += 1;
            self.table_name = table_name;
            self.group_id = group_id;
            self.saw_db = changed_db != null;
            self.change = change;
        }
    };
    var hook_ctx = HookCtx{};
    db.setQueryVisibilityHook(.{
        .ptr = &hook_ctx,
        .table_name = "docs",
        .group_id = 7001,
        .db = &db,
        .on_change = HookCtx.onChange,
    });

    try db.enrichment_runtime.?.markAppliedThrough(1);

    try std.testing.expectEqual(@as(u64, 1), hook_ctx.calls);
    try std.testing.expectEqualStrings("docs", hook_ctx.table_name.?);
    try std.testing.expectEqual(@as(u64, 7001), hook_ctx.group_id);
    try std.testing.expect(hook_ctx.saw_db);
    try std.testing.expectEqual(QueryVisibilityChange.invalidate, hook_ctx.change.?);
}

test "db full-text index and search survive reopen with durable lsm primary backend" {
    const alloc = std.testing.allocator;

    var path_buf: [256]u8 = undefined;
    const path = tempPath(&path_buf);
    defer cleanupTempDir(path);

    {
        var db = try DB.open(alloc, std.mem.span(path), .{
            .primary_backend = .{ .lsm = .{ .flush_threshold = 1 } },
        });
        defer db.close();

        try db.batch(.{
            .writes = &.{
                .{ .key = "doc:a", .value = "{\"title\":\"alpha\",\"body\":\"first alpha\"}" },
                .{ .key = "doc:b", .value = "{\"title\":\"beta\",\"body\":\"second body\"}" },
            },
        });

        try db.addIndex(.{
            .name = "ft_v1",
            .kind = .full_text,
            .config_json = "{}",
        });

        var result = try db.search(alloc, .{
            .index_name = "ft_v1",
            .query = .{ .match = .{ .field = "body", .text = "alpha" } },
            .limit = 10,
        });
        defer result.deinit();

        try std.testing.expectEqual(@as(u32, 1), result.total_hits);
        try std.testing.expectEqualStrings("doc:a", result.hits[0].id);
    }

    {
        var reopened = try DB.open(alloc, std.mem.span(path), .{
            .primary_backend = .{ .lsm = .{ .flush_threshold = 1 } },
        });
        defer reopened.close();

        try std.testing.expectEqual(@as(u32, 1), reopened.core.index_manager.count());

        var result = try reopened.search(alloc, .{
            .index_name = "ft_v1",
            .query = .{ .match = .{ .field = "body", .text = "alpha" } },
            .limit = 10,
        });
        defer result.deinit();

        try std.testing.expectEqual(@as(u32, 1), result.total_hits);
        try std.testing.expectEqualStrings("doc:a", result.hits[0].id);
    }
}

test "db algebraic bulk ingest survives reopen with durable lsm primary backend" {
    const alloc = std.testing.allocator;

    var path_buf: [256]u8 = undefined;
    const path = tempPath(&path_buf);
    defer cleanupTempDir(path);

    const cfg =
        \\{
        \\  "version": 1,
        \\  "table": "orders",
        \\  "group_fields": [{"name":"customer","path":"customer","type":"string"}],
        \\  "measure_fields": [{"name":"amount","path":"amount","type":"number"}],
        \\  "materializations": [{"name":"sum_by_customer","op":"sum","group_by":["customer"],"measure":"amount"}]
        \\}
    ;

    {
        var db = try DB.open(alloc, std.mem.span(path), .{
            .primary_backend = .{ .lsm = .{ .flush_threshold = 2 } },
            .start_index_workers = false,
        });
        defer db.close();

        try db.addIndex(.{
            .name = "alg",
            .kind = .algebraic,
            .config_json = cfg,
        });

        try db.beginBulkIngestSession();
        errdefer db.abortBulkIngestSession();

        try db.batch(.{
            .writes = &.{
                .{ .key = "o1", .value = "{\"customer\":\"alice\",\"amount\":10}" },
                .{ .key = "o2", .value = "{\"customer\":\"alice\",\"amount\":20}" },
                .{ .key = "o3", .value = "{\"customer\":\"bob\",\"amount\":7}" },
            },
            .sync_level = .write,
        });

        try db.finishBulkIngestSessionWithOptions(.{
            .compact = false,
            .flush = true,
            .max_deferred_l0_runs = 2,
            .max_foreground_compaction_steps = 1,
        });

        const entry = db.core.index_manager.algebraicIndex("alg") orelse return error.TestUnexpectedResult;
        const alice_token = try entry.index.constraintTokenAlloc(alloc, "customer", "alice");
        defer alloc.free(alice_token);
        const alice_group = try algebraic_mod.token.canonicalTupleAlloc(alloc, &.{alice_token});
        defer alloc.free(alice_group);
        try std.testing.expectEqual(@as(f64, 30), (try entry.index.numericValue(db.core.store, "sum_by_customer", alice_group)).?);
    }

    {
        var reopened = try DB.open(alloc, std.mem.span(path), .{
            .primary_backend = .{ .lsm = .{ .flush_threshold = 2 } },
            .start_index_workers = false,
        });
        defer reopened.close();

        try std.testing.expectEqual(@as(u32, 1), reopened.core.index_manager.count());
        const entry = reopened.core.index_manager.algebraicIndex("alg") orelse return error.TestUnexpectedResult;

        const alice_token = try entry.index.constraintTokenAlloc(alloc, "customer", "alice");
        defer alloc.free(alice_token);
        const alice_group = try algebraic_mod.token.canonicalTupleAlloc(alloc, &.{alice_token});
        defer alloc.free(alice_group);
        try std.testing.expectEqual(@as(f64, 30), (try entry.index.numericValue(reopened.core.store, "sum_by_customer", alice_group)).?);

        const bob_token = try entry.index.constraintTokenAlloc(alloc, "customer", "bob");
        defer alloc.free(bob_token);
        const bob_group = try algebraic_mod.token.canonicalTupleAlloc(alloc, &.{bob_token});
        defer alloc.free(bob_group);
        try std.testing.expectEqual(@as(f64, 7), (try entry.index.numericValue(reopened.core.store, "sum_by_customer", bob_group)).?);
    }
}

test "db batch appends only thin replay stream records" {
    const alloc = std.testing.allocator;

    var path_buf: [256]u8 = undefined;
    const path = tempPath(&path_buf);
    defer cleanupTempDir(path);

    var db = try DB.open(alloc, std.mem.span(path), .{});
    defer db.close();

    try db.batch(.{
        .writes = &.{
            .{ .key = "doc:a", .value = "{\"title\":\"alpha\",\"body\":\"machine learning\"}" },
        },
    });

    const entries = try replay_stream_mod.iterateFrom(alloc, db.core.store, 1);
    defer {
        for (entries) |*entry| entry.deinit(alloc);
        alloc.free(entries);
    }
    try std.testing.expectEqual(@as(usize, 1), entries.len);
}

test "db batch writes thin change journal record" {
    const alloc = std.testing.allocator;

    var path_buf: [256]u8 = undefined;
    const path = tempPath(&path_buf);
    defer cleanupTempDir(path);

    var db = try DB.open(alloc, std.mem.span(path), .{});
    defer db.close();

    try db.batch(.{
        .writes = &.{
            .{ .key = "doc:a", .value = "{\"title\":\"alpha\",\"body\":\"machine learning\"}" },
        },
    });

    const entries = try replay_stream_mod.iterateFrom(alloc, db.core.store, 1);
    defer {
        for (entries) |*entry| entry.deinit(alloc);
        alloc.free(entries);
    }

    try std.testing.expectEqual(@as(usize, 1), entries.len);
    var record = try change_journal_mod.decodeRecord(alloc, entries[0].payload);
    defer record.deinit();

    try std.testing.expectEqual(@as(u64, 1), record.record.sequence);
    try std.testing.expectEqual(@as(usize, 1), record.record.changed_doc_keys.len);
    try std.testing.expectEqualStrings("doc:a", record.record.changed_doc_keys[0]);
    try std.testing.expectEqual(@as(usize, 0), record.record.changed_artifact_keys.len);
}

test "db batch uses change journal as the replay authority" {
    const alloc = std.testing.allocator;

    var path_buf: [256]u8 = undefined;
    const path = tempPath(&path_buf);
    defer cleanupTempDir(path);

    var db = try DB.open(alloc, std.mem.span(path), .{});
    defer db.close();

    try db.batch(.{
        .writes = &.{
            .{ .key = "doc:a", .value = "{\"title\":\"alpha\",\"body\":\"machine learning\"}" },
        },
    });

    const entries = try replay_stream_mod.iterateFrom(alloc, db.core.store, 1);
    defer {
        for (entries) |*entry| entry.deinit(alloc);
        alloc.free(entries);
    }

    try std.testing.expectEqual(@as(usize, 1), entries.len);
}

test "db pending work stats track replay stream sequence" {
    const alloc = std.testing.allocator;

    var path_buf: [256]u8 = undefined;
    const path = tempPath(&path_buf);
    defer cleanupTempDir(path);

    var db = try DB.open(alloc, std.mem.span(path), .{});
    defer db.close();

    try std.testing.expectEqual(@as(u64, 0), replay_stream_mod.lastSequence(db.core.store, 0));
    try std.testing.expectEqual(@as(u64, 0), db.pendingWorkStats().derived_target_sequence);

    const record = change_journal_mod.Record{
        .sequence = 1,
        .changed_doc_keys = &.{"doc:a"},
        .target_hints = &.{.full_text},
    };
    const payload = try change_journal_mod.encodeRecord(alloc, record);
    defer alloc.free(payload);
    try replay_stream_mod.appendOpaque(alloc, db.core.store, 1, payload);

    try std.testing.expectEqual(@as(u64, 1), replay_stream_mod.lastSequence(db.core.store, 0));
    try std.testing.expectEqual(@as(u64, 1), db.pendingWorkStats().derived_target_sequence);
}

test "db open preserves existing change journal records" {
    const alloc = std.testing.allocator;

    var path_buf: [256]u8 = undefined;
    const path = tempPath(&path_buf);
    defer cleanupTempDir(path);

    {
        var db = try DB.open(alloc, std.mem.span(path), .{});
        defer db.close();

        const payload = try change_journal_mod.encodeRecord(alloc, .{
            .sequence = 1,
            .changed_doc_keys = &.{"doc:legacy"},
            .target_hints = &.{.full_text},
        });
        defer alloc.free(payload);
        try replay_stream_mod.appendOpaque(alloc, db.core.store, 1, payload);
        try std.testing.expectEqual(@as(u64, 1), replay_stream_mod.lastSequence(db.core.store, 0));
    }

    var reopened = try DB.open(alloc, std.mem.span(path), .{});
    defer reopened.close();

    const entries = try replay_stream_mod.iterateFrom(alloc, reopened.core.store, 1);
    defer {
        for (entries) |*entry| entry.deinit(alloc);
        alloc.free(entries);
    }
    try std.testing.expectEqual(@as(usize, 1), entries.len);
    try std.testing.expectEqual(@as(u64, 1), reopened.pendingWorkStats().derived_target_sequence);
}

test "db lsm primary reopens explicit dense replay stream state" {
    const alloc = std.testing.allocator;

    var path_buf: [256]u8 = undefined;
    const path = tempPath(&path_buf);
    defer cleanupTempDir(path);

    {
        var db = try DB.open(alloc, std.mem.span(path), .{
            .primary_backend = .{ .lsm = .{ .flush_threshold = 1 } },
            .start_index_workers = false,
        });
        defer db.close();

        try db.addIndex(.{
            .name = "dense_idx",
            .kind = .dense_vector,
            .config_json = "{\"field\":\"embedding\",\"dims\":8,\"metric\":\"l2_squared\"}",
        });

        try db.batch(.{
            .writes = &.{
                .{ .key = "doc:1", .value = "{\"embedding\":[1,0,0,0,0,0,0,0]}" },
                .{ .key = "doc:2", .value = "{\"embedding\":[0,1,0,0,0,0,0,0]}" },
            },
            .sync_level = .write,
        });
        try db.sync(true);

        try std.testing.expectEqual(@as(u64, 1), replay_stream_mod.lastSequence(db.core.store, 0));
        const live_entries = try replay_stream_mod.iterateFrom(alloc, db.core.store, 1);
        defer {
            for (live_entries) |*entry| entry.deinit(alloc);
            alloc.free(live_entries);
        }
        try std.testing.expectEqual(@as(usize, 1), live_entries.len);
    }

    var reopened = try DB.open(alloc, std.mem.span(path), .{
        .primary_backend = .{ .lsm = db_config.primary_lsm_options_default },
        .open_mode = .query_readonly,
    });
    defer reopened.close();

    const entries = try replay_stream_mod.iterateFrom(alloc, reopened.core.store, 1);
    defer {
        for (entries) |*entry| entry.deinit(alloc);
        alloc.free(entries);
    }
    try std.testing.expectEqual(@as(usize, 1), entries.len);
    try std.testing.expectEqual(@as(u64, 1), entries[0].sequence);
}

test "db lsm generated chunked enrichment publishes replay stream state" {
    const alloc = std.testing.allocator;

    var path_buf: [256]u8 = undefined;
    const path = tempPath(&path_buf);
    defer cleanupTempDir(path);

    var deterministic = embedder_mod.DeterministicDenseEmbedder{};
    var db = try DB.open(alloc, std.mem.span(path), .{
        .primary_backend = .{ .lsm = db_config.primary_lsm_options_default },
        .enrichment = .{
            .owner_id = "worker-a",
            .dense_embedder = deterministic.interface(),
        },
    });
    defer db.close();

    try db.addIndex(.{
        .name = "dense_idx",
        .kind = .dense_vector,
        .config_json = "{\"field\":\"embedding\",\"dims\":3,\"metric\":\"l2_squared\",\"generator\":{\"kind\":\"dense_embedding\",\"source_field\":\"body\",\"artifact_name\":\"body_chunks_v1\",\"chunk_size\":8,\"chunk_overlap\":2,\"embedding_name\":\"dense_idx\"}}",
    });

    var doc_idx: usize = 0;
    while (doc_idx < 40) : (doc_idx += 4) {
        var writes: [4]types.BatchWrite = undefined;
        var owned_keys: [4][]u8 = undefined;
        var owned_values: [4][]u8 = undefined;
        defer {
            for (owned_keys, owned_values) |key, value| {
                alloc.free(key);
                alloc.free(value);
            }
        }
        for (0..4) |offset| {
            owned_keys[offset] = try std.fmt.allocPrint(alloc, "doc:{d}", .{doc_idx + offset});
            owned_values[offset] = try std.fmt.allocPrint(
                alloc,
                "{{\"title\":\"doc-{d}\",\"body\":\"abcdefghijklmno {d}\"}}",
                .{ doc_idx + offset, doc_idx + offset },
            );
            writes[offset] = .{
                .key = owned_keys[offset],
                .value = owned_values[offset],
            };
        }

        try db.batch(.{
            .writes = writes[0..],
            .sync_level = .write,
        });
    }

    try db.runUntilIdle();
    try db.sync(true);

    const stats = try db.stats(alloc);
    defer types.freeDBStats(alloc, stats);
    try std.testing.expectEqual(@as(u64, 0), stats.enrichment.error_count);
    try std.testing.expect(stats.enrichment.applied_sequence > 0);

    const chunk_prefix = try internal_keys.artifactNamedPrefixAlloc(alloc, "doc:0", "chunk", "body_chunks_v1");
    defer alloc.free(chunk_prefix);
    const artifacts = try db.core.store.scanPrefix(alloc, chunk_prefix);
    defer docstore_mod.DocStore.freeResults(alloc, artifacts);
    try std.testing.expect(artifacts.len > 0);

    const query_vec = try deterministic.interface().embedDense(alloc, "dense_idx", "abcdefgh", 3);
    defer alloc.free(query_vec);
    var result = try db.search(alloc, .{
        .index_name = "dense_idx",
        .dense = .{
            .vector = query_vec,
            .k = 5,
        },
        .limit = 1,
        .include_stored = true,
    });
    defer result.deinit();
    try std.testing.expect(result.total_hits > 0);
}

test "db direct graph writes record graph artifacts in the replay stream instead of graph payload replay" {
    const alloc = std.testing.allocator;

    var path_buf: [256]u8 = undefined;
    const path = tempPath(&path_buf);
    defer cleanupTempDir(path);

    var db = try DB.open(alloc, std.mem.span(path), .{});
    defer db.close();

    try db.addIndex(.{
        .name = "gr_v1",
        .kind = .graph,
        .config_json = "{}",
    });

    try db.batch(.{
        .graph_writes = &.{
            .{ .index_name = "gr_v1", .source = "doc:a", .target = "doc:b", .edge_type = "links", .weight = 1.0 },
        },
        .sync_level = .write,
    });

    const journal_entries = try replay_stream_mod.iterateFrom(alloc, db.core.store, 1);
    defer {
        for (journal_entries) |*entry| entry.deinit(alloc);
        alloc.free(journal_entries);
    }

    try std.testing.expectEqual(@as(usize, 1), journal_entries.len);
    var journal_record = try change_journal_mod.decodeRecord(alloc, journal_entries[0].payload);
    defer journal_record.deinit();

    try std.testing.expectEqual(@as(usize, 1), journal_record.record.changed_artifact_keys.len);
    try std.testing.expect(internal_keys.isGraphEdgeArtifactKey(journal_record.record.changed_artifact_keys[0]));
}

test "db _edges writes record graph artifacts in the replay stream instead of graph payload replay" {
    const alloc = std.testing.allocator;

    var path_buf: [256]u8 = undefined;
    const path = tempPath(&path_buf);
    defer cleanupTempDir(path);

    var db = try DB.open(alloc, std.mem.span(path), .{});
    defer db.close();

    try db.addIndex(.{
        .name = "gr_v1",
        .kind = .graph,
        .config_json = "{}",
    });

    try db.batch(.{
        .writes = &.{
            .{ .key = "doc:a", .value = "{\"title\":\"alpha\",\"_edges\":{\"gr_v1\":{\"links\":[{\"target\":\"doc:b\"},{\"target\":\"doc:c\"}]}}}" },
        },
        .sync_level = .write,
    });

    const journal_entries = try replay_stream_mod.iterateFrom(alloc, db.core.store, 1);
    defer {
        for (journal_entries) |*entry| entry.deinit(alloc);
        alloc.free(journal_entries);
    }

    try std.testing.expectEqual(@as(usize, 1), journal_entries.len);
    var journal_record = try change_journal_mod.decodeRecord(alloc, journal_entries[0].payload);
    defer journal_record.deinit();

    try std.testing.expectEqual(@as(usize, 2), journal_record.record.changed_artifact_keys.len);
    for (journal_record.record.changed_artifact_keys) |artifact_key| {
        try std.testing.expect(internal_keys.isGraphEdgeArtifactKey(artifact_key));
    }
}

test "db starts resolver replay workers only while resolver catalog is configured" {
    const alloc = std.testing.allocator;

    var path_buf: [256]u8 = undefined;
    const path = tempPath(&path_buf);
    defer cleanupTempDir(path);

    var db = try DB.open(alloc, std.mem.span(path), .{});
    defer db.close();

    try std.testing.expect(db.resolution_runtime != null);
    try std.testing.expect(db.promotion_runtime != null);
    try std.testing.expect(!db.resolution_runtime.?.worker_started.load(.acquire));
    try std.testing.expect(!db.promotion_runtime.?.worker_started.load(.acquire));

    try db.addIndex(.{
        .name = "relations_graph",
        .kind = .graph,
        .config_json =
        \\{
        \\  "source":{"kind":"artifact","artifact":"relations_v1","path":"$.relations[*]","format":"extraction_relation"},
        \\  "artifact":{"name":"relations_v1","kind":"asset","field":"relations","content_type":"application/json"}
        \\}
        ,
    });
    try std.testing.expect(!db.resolution_runtime.?.worker_started.load(.acquire));
    try std.testing.expect(!db.promotion_runtime.?.worker_started.load(.acquire));

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
    try std.testing.expect(!db.resolution_runtime.?.worker_started.load(.acquire));
    try std.testing.expect(!db.promotion_runtime.?.worker_started.load(.acquire));
    try db.drainResolverBackfill();
    try std.testing.expectError(error.NotFound, db.core.store.get(alloc, resolver_catalog_mod.reresolve_resume_key));
    try std.testing.expectError(error.NotFound, db.core.store.get(alloc, resolver_catalog_mod.reresolve_repair_resume_key));

    try db.addResolver(.{
        .name = "kg_lifecycle",
        .table = "entities",
        .source_artifact = "relations_v1",
        .resolution_artifact = "resolution_lifecycle_v1",
        .key_template = "{{ lower _entity.label }}/{{ slug _entity.text }}",
        .config_generation = 1,
    });
    try std.testing.expect(db.resolution_runtime.?.worker_started.load(.acquire));
    try std.testing.expect(db.promotion_runtime.?.worker_started.load(.acquire));

    try std.testing.expect(try db.removeResolver("kg_lifecycle"));
    try std.testing.expect(!db.resolution_runtime.?.worker_started.load(.acquire));
    try std.testing.expect(!db.promotion_runtime.?.worker_started.load(.acquire));
}

test "db backfills a mention name embedding so ann/cosine resolution links end-to-end" {
    const alloc = std.testing.allocator;

    var path_buf: [256]u8 = undefined;
    const path = tempPath(&path_buf);
    defer cleanupTempDir(path);

    var embedder = FixedVectorEmbedder{};
    var db = try DB.open(alloc, std.mem.span(path), .{ .resolution_embedder = embedder.interface() });
    defer db.close();

    try db.addIndex(.{
        .name = "relations_graph",
        .kind = .graph,
        .config_json =
        \\{
        \\  "source":{"kind":"artifact","artifact":"relations_v1","path":"$.relations[*]",
        \\    "format":"extraction_relation","mention_edge_type":"mentions"},
        \\  "artifact":{"name":"relations_v1","kind":"asset","field":"relations","content_type":"application/json"}
        \\}
        ,
    });
    // Resolver backfills a name embedding for each mention (no embedding in the
    // extraction artifact), then prefix-blocks + cosine-scores against the entity.
    try db.addResolver(.{
        .name = "kg",
        .table = "entities",
        .source_artifact = "relations_v1",
        .resolution_artifact = "resolution_v1",
        .key_template = "{{ lower _entity.label }}/{{ slug _entity.text }}",
        .candidate_search = "prefix",
        .name_embedding = "name",
        .name_embedding_dims = 4,
        .scorer_json =
        \\{ "comparisons": [ { "name": "emb", "left": "name_embedding", "right": "name_embedding",
        \\  "levels": [ { "when": "cosine > 0.9", "weight": 8.0 }, { "else": true, "weight": -6.0 } ] } ],
        \\  "combine": { "bias": -3.0 }, "decision": { "match": 0.9 } }
        ,
        .config_generation = 1,
    });

    // An existing entity under a different key, carrying the matching vector.
    const entity_doc_key = try internal_keys.documentKeyAlloc(alloc, "person/ada_lovelace");
    defer alloc.free(entity_doc_key);
    try db.core.store.put(entity_doc_key,
        \\{ "canonical_name": "Ada Lovelace", "label": "person", "name_embedding": [1.0, 0.0, 0.0, 0.0] }
    );

    try db.batch(.{
        .writes = &.{.{
            .key = "doc:a",
            .value =
            \\{"relations":{"entities":[{"id":"e0","label":"person","text":"A. Lovelace"}]}}
            ,
        }},
        .sync_level = .enrichments,
    });
    try db.runUntilIdle();

    const resolution_key = try internal_keys.resolutionArtifactKeyAlloc(alloc, "doc:a", "resolution_v1");
    defer alloc.free(resolution_key);
    const raw = try db.core.store.get(alloc, resolution_key);
    defer alloc.free(raw);
    var parsed = try std.json.parseFromSlice(std.json.Value, alloc, raw, .{});
    defer parsed.deinit();
    const ent = parsed.value.object.get("entities").?.array.items[0].object;
    // The mention had no embedding; the backfilled vector cosine-matched the
    // entity, so it linked instead of minting a new key.
    try std.testing.expectEqualStrings("match", ent.get("decision").?.string);
    try std.testing.expectEqualStrings("person/ada_lovelace", ent.get("doc_ref").?.object.get("key").?.string);

    const edges = try db.getEdges(alloc, "relations_graph", "doc:a", "mentions", .out);
    defer graph_mod.GraphIndex.freeEdges(alloc, edges);
    try std.testing.expectEqual(@as(usize, 1), edges.len);
    try std.testing.expectEqualStrings("person/ada_lovelace", edges[0].target);
}

test "db resolves extracted entities into a resolution artifact end-to-end" {
    const alloc = std.testing.allocator;

    var path_buf: [256]u8 = undefined;
    const path = tempPath(&path_buf);
    defer cleanupTempDir(path);

    var db = try DB.open(alloc, std.mem.span(path), .{});
    defer db.close();

    // A graph index drives production of the relations_v1 asset (extraction)
    // artifact; the resolver consumes the same artifact.
    try db.addIndex(.{
        .name = "relations_graph",
        .kind = .graph,
        .config_json =
        \\{
        \\  "source":{"kind":"artifact","artifact":"relations_v1","path":"$.relations[*]","format":"extraction_relation"},
        \\  "artifact":{"name":"relations_v1","kind":"asset","field":"relations","content_type":"application/json"}
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
    try db.runUntilIdle();

    const resolution_key = try internal_keys.resolutionArtifactKeyAlloc(alloc, "doc:a", "resolution_v1");
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

test "db re-resolves the corpus when upsertResolver bumps the config generation" {
    const alloc = std.testing.allocator;

    var path_buf: [256]u8 = undefined;
    const path = tempPath(&path_buf);
    defer cleanupTempDir(path);

    var db = try DB.open(alloc, std.mem.span(path), .{});
    defer db.close();

    try db.addIndex(.{
        .name = "relations_graph",
        .kind = .graph,
        .config_json =
        \\{
        \\  "source":{"kind":"artifact","artifact":"relations_v1","path":"$.relations[*]","format":"extraction_relation"},
        \\  "artifact":{"name":"relations_v1","kind":"asset","field":"relations","content_type":"application/json"}
        \\}
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
        .writes = &.{.{
            .key = "doc:a",
            .value =
            \\{"relations":{"entities":[{"id":"e0","label":"person","text":"Ada Lovelace"}]}}
            ,
        }},
        .sync_level = .enrichments,
    });
    try db.runUntilIdle();

    const resolution_key = try internal_keys.resolutionArtifactKeyAlloc(alloc, "doc:a", "resolution_v1");
    defer alloc.free(resolution_key);
    {
        const raw = try db.core.store.get(alloc, resolution_key);
        defer alloc.free(raw);
        try std.testing.expect(std.mem.indexOf(u8, raw, "\"config_generation\":1") != null);
    }

    // Bump the resolver's config generation: the document was already ingested,
    // so only the corpus backfill (triggered by upsertResolver) re-resolves it.
    try db.upsertResolver(.{
        .name = "kg",
        .table = "entities",
        .source_artifact = "relations_v1",
        .resolution_artifact = "resolution_v1",
        .key_template = "{{ lower _entity.label }}/{{ slug _entity.text }}",
        .config_generation = 2,
    });

    const raw = try db.core.store.get(alloc, resolution_key);
    defer alloc.free(raw);
    try std.testing.expect(std.mem.indexOf(u8, raw, "\"config_generation\":2") != null);
}

test "db re-resolves existing corpus when upsertResolver inserts a new resolver" {
    const alloc = std.testing.allocator;

    var path_buf: [256]u8 = undefined;
    const path = tempPath(&path_buf);
    defer cleanupTempDir(path);

    var db = try DB.open(alloc, std.mem.span(path), .{});
    defer db.close();

    try db.addIndex(.{
        .name = "relations_graph",
        .kind = .graph,
        .config_json =
        \\{
        \\  "source":{"kind":"artifact","artifact":"relations_v1","path":"$.relations[*]","format":"extraction_relation"},
        \\  "artifact":{"name":"relations_v1","kind":"asset","field":"relations","content_type":"application/json"}
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

    const asset_key = try internal_keys.artifactNamedPrefixAlloc(alloc, "doc:a", "asset", "relations_v1");
    defer alloc.free(asset_key);
    {
        const asset_raw = try db.core.store.get(alloc, asset_key);
        defer alloc.free(asset_raw);
        try std.testing.expect(std.mem.indexOf(u8, asset_raw, "Ada Lovelace") != null);
    }
    const marker_key = try internal_keys.assetArtifactSourceIndexKeyAlloc(alloc, "relations_v1", "doc:a");
    defer alloc.free(marker_key);
    {
        const marker_raw = try db.core.store.get(alloc, marker_key);
        defer alloc.free(marker_raw);
        try std.testing.expectEqualStrings(asset_key, marker_raw);
    }

    const resolution_key = try internal_keys.resolutionArtifactKeyAlloc(alloc, "doc:a", "resolution_inserted_v1");
    defer alloc.free(resolution_key);
    try std.testing.expectError(error.NotFound, db.core.store.get(alloc, resolution_key));

    try db.upsertResolver(.{
        .name = "kg_inserted",
        .table = "entities",
        .source_artifact = "relations_v1",
        .resolution_artifact = "resolution_inserted_v1",
        .key_template = "{{ lower _entity.label }}/{{ slug _entity.text }}",
        .config_generation = 1,
    });

    const raw = try db.core.store.get(alloc, resolution_key);
    defer alloc.free(raw);
    try std.testing.expect(std.mem.indexOf(u8, raw, "\"config_generation\":1") != null);
    try std.testing.expect(std.mem.indexOf(u8, raw, "person/ada_lovelace") != null);
}

test "db drains pending resolver backfill when retrying a no-op upsertResolver" {
    const alloc = std.testing.allocator;

    var path_buf: [256]u8 = undefined;
    const path = tempPath(&path_buf);
    defer cleanupTempDir(path);

    var db = try DB.open(alloc, std.mem.span(path), .{});
    defer db.close();

    try db.addIndex(.{
        .name = "relations_graph",
        .kind = .graph,
        .config_json =
        \\{
        \\  "source":{"kind":"artifact","artifact":"relations_v1","path":"$.relations[*]","format":"extraction_relation"},
        \\  "artifact":{"name":"relations_v1","kind":"asset","field":"relations","content_type":"application/json"}
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

    const resolution_key = try internal_keys.resolutionArtifactKeyAlloc(alloc, "doc:a", "resolution_retry_v1");
    defer alloc.free(resolution_key);
    try std.testing.expectError(error.NotFound, db.core.store.get(alloc, resolution_key));

    const cfg: index_manager_mod.ResolverConfig = .{
        .name = "kg_retry",
        .table = "entities",
        .source_artifact = "relations_v1",
        .resolution_artifact = "resolution_retry_v1",
        .key_template = "{{ lower _entity.label }}/{{ slug _entity.text }}",
        .config_generation = 1,
    };

    {
        lockApply(&db);
        defer db.core.unlockApply();
        try std.testing.expectEqual(index_manager_mod.IndexManager.ResolverUpsertResult.inserted, try db.core.upsertResolver(cfg));
    }
    try std.testing.expectError(error.NotFound, db.core.store.get(alloc, resolution_key));
    try std.testing.expect(try db.resolution_runtime.?.hasReresolveBacklog());

    // Retrying the same catalog config is a material no-op, but the durable
    // dirty cursor from the first attempt must still be drained.
    try db.upsertResolver(cfg);

    const raw = try db.core.store.get(alloc, resolution_key);
    defer alloc.free(raw);
    try std.testing.expect(std.mem.indexOf(u8, raw, "\"config_generation\":1") != null);
    try std.testing.expect(std.mem.indexOf(u8, raw, "person/ada_lovelace") != null);
    try std.testing.expect(!try db.resolution_runtime.?.hasReresolveBacklog());
}

test "db refuses resolver removal while resolution or promotion replay is pending" {
    const alloc = std.testing.allocator;

    var path_buf: [256]u8 = undefined;
    const path = tempPath(&path_buf);
    defer cleanupTempDir(path);

    var db = try DB.open(alloc, std.mem.span(path), .{});
    defer db.close();

    try db.addIndex(.{
        .name = "relations_graph",
        .kind = .graph,
        .config_json =
        \\{
        \\  "source":{"kind":"artifact","artifact":"relations_v1","path":"$.relations[*]","format":"extraction_relation","mention_edge_type":"mentions"},
        \\  "artifact":{"name":"relations_v1","kind":"asset","field":"relations","content_type":"application/json"}
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
    try std.testing.expect(db.promotionStageStats().catch_up_required);
}

test "db promotes resolved entities into entity-document upserts end-to-end" {
    const alloc = std.testing.allocator;

    var path_buf: [256]u8 = undefined;
    const path = tempPath(&path_buf);
    defer cleanupTempDir(path);

    var sink = FakePromotionSink{ .alloc = alloc };
    defer sink.deinit();

    var db = try DB.open(alloc, std.mem.span(path), .{ .entity_sink = sink.sink() });
    defer db.close();

    try db.addIndex(.{
        .name = "relations_graph",
        .kind = .graph,
        .config_json =
        \\{
        \\  "source":{"kind":"artifact","artifact":"relations_v1","path":"$.relations[*]","format":"extraction_relation"},
        \\  "artifact":{"name":"relations_v1","kind":"asset","field":"relations","content_type":"application/json"}
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
    try db.runUntilIdle();

    const stats = try db.stats(alloc);
    defer types.freeDBStats(alloc, stats);
    try std.testing.expect(stats.resolution.enabled);
    try std.testing.expect(stats.promotion.enabled);
    try std.testing.expectEqual(stats.resolution.target_sequence, stats.resolution.applied_sequence);
    try std.testing.expectEqual(stats.promotion.target_sequence, stats.promotion.applied_sequence);
    try std.testing.expect(!stats.promotion.blocked);

    // The promoter upserted a canonical entity document per resolved mention,
    // keyed by the rendered canonical key, into the entity table.
    const ada = sink.findKey("person/ada_lovelace") orelse return error.MissingAdaUpsert;
    try std.testing.expect(std.mem.indexOf(u8, ada, "\"canonical_name\":\"Ada Lovelace\"") != null);
    try std.testing.expect(std.mem.indexOf(u8, ada, "\"entity_type\":\"person\"") != null);
    const antfly = sink.findKey("org/antfly") orelse return error.MissingAntflyUpsert;
    try std.testing.expect(std.mem.indexOf(u8, antfly, "\"canonical_name\":\"Antfly\"") != null);

    // Replay is idempotent: draining again promotes nothing new.
    const count_before = sink.upserts.items.len;
    try db.runUntilIdle();
    try std.testing.expectEqual(count_before, sink.upserts.items.len);
}

test "db graph index materializes relation asset artifacts into graph edge artifacts" {
    const alloc = std.testing.allocator;

    var path_buf: [256]u8 = undefined;
    const path = tempPath(&path_buf);
    defer cleanupTempDir(path);

    var db = try DB.open(alloc, std.mem.span(path), .{});
    defer db.close();

    try db.addIndex(.{
        .name = "relations_graph",
        .kind = .graph,
        .config_json =
        \\{
        \\  "source":{"kind":"artifact","artifact":"relations_v1","path":"$.relations[*]","format":"extraction_relation"},
        \\  "artifact":{"name":"relations_v1","kind":"asset","field":"relations","content_type":"application/json"}
        \\}
        ,
    });

    try db.batch(.{
        .writes = &.{
            .{
                .key = "doc:a",
                .value =
                \\{"relations":{"relations":[{"type":"mentions","target":{"document_id":"doc:b"},"confidence":0.75}]}}
                ,
            },
        },
        .sync_level = .enrichments,
    });
    try db.runUntilIdle();

    const artifact_key = try internal_keys.graphEdgeArtifactKeyAlloc(alloc, "doc:a", "relations_graph", "mentions", "doc:b");
    defer alloc.free(artifact_key);
    const raw_artifact = try db.core.store.get(alloc, artifact_key);
    defer alloc.free(raw_artifact);

    const edges = try db.getEdges(alloc, "relations_graph", "doc:a", "mentions", .out);
    defer graph_mod.GraphIndex.freeEdges(alloc, edges);
    try std.testing.expectEqual(@as(usize, 1), edges.len);
    try std.testing.expectEqualStrings("doc:b", edges[0].target);
    try std.testing.expectApproxEqAbs(@as(f64, 0.75), edges[0].weight, 0.0001);
}

test "db graph replay blocks resolution artifact without resolver contract" {
    const alloc = std.testing.allocator;

    var path_buf: [256]u8 = undefined;
    const path = tempPath(&path_buf);
    defer cleanupTempDir(path);

    var db = try DB.open(alloc, std.mem.span(path), .{ .start_index_workers = false });
    defer db.close();

    try db.addIndex(.{
        .name = "prov_graph",
        .kind = .graph,
        .config_json =
        \\{
        \\  "source":{"kind":"artifact","artifact":"relations_v1","mention_edge_type":"mentions",
        \\    "format":"extraction_relation","path":"$.relations[*]"},
        \\  "artifact":{"name":"relations_v1","kind":"asset","field":"relations","content_type":"application/json"}
        \\}
        ,
    });

    const resolution_key = try internal_keys.resolutionArtifactKeyAlloc(alloc, "doc:a", "resolution_v1");
    defer alloc.free(resolution_key);
    const batch = derived_types.DerivedBatch{
        .sequence = 1,
        .changed_artifact_keys = &.{resolution_key},
    };
    const index_ref = index_manager_mod.ManagedIndexRef{
        .name = "prov_graph",
        .kind = .graph,
    };

    try db.core.store.put(resolution_key, "{}");

    try std.testing.expect(!db.derivedAsyncBatchAffectsManagedIndex(batch, index_ref));
    try std.testing.expect(try db.derivedAsyncBatchAffectsManagedIndexForReplay(batch, index_ref));
    try std.testing.expect(try db.derivedAsyncBatchAdvancesManagedIndexApplyStateForReplay(batch, index_ref));
    try std.testing.expectError(
        error.MissingResolverArtifactContract,
        derived_async.materializeGraphSourceArtifactsForIndex(
            alloc,
            db.core.store,
            db.core.index_manager,
            &.{resolution_key},
            "prov_graph",
            .{ .require_resolution_contract = true },
        ),
    );

    try db.core.store.delete(resolution_key);
    try std.testing.expectError(
        error.MissingResolverArtifactContract,
        derived_async.materializeGraphSourceArtifactsForIndex(
            alloc,
            db.core.store,
            db.core.index_manager,
            &.{resolution_key},
            "prov_graph",
            .{ .require_resolution_contract = true },
        ),
    );

    try db.addResolver(.{
        .name = "kg",
        .table = "entities",
        .source_artifact = "relations_v1",
        .resolution_artifact = "resolution_v1",
        .key_template = "{{ lower _entity.label }}/{{ slug _entity.text }}",
        .config_generation = 1,
    });

    try std.testing.expect(db.derivedAsyncBatchAffectsManagedIndex(batch, index_ref));
    try std.testing.expect(try db.derivedAsyncBatchAdvancesManagedIndexApplyStateForReplay(batch, index_ref));
}

test "db graph replay ignores resolution artifacts bound to another source contract" {
    const alloc = std.testing.allocator;

    var path_buf: [256]u8 = undefined;
    const path = tempPath(&path_buf);
    defer cleanupTempDir(path);

    var db = try DB.open(alloc, std.mem.span(path), .{ .start_index_workers = false });
    defer db.close();

    try db.addIndex(.{
        .name = "graph_a",
        .kind = .graph,
        .config_json =
        \\{
        \\  "source":{"kind":"artifact","artifact":"relations_a","mention_edge_type":"mentions",
        \\    "format":"extraction_relation","path":"$.relations[*]"},
        \\  "artifact":{"name":"relations_a","kind":"asset","field":"relations_a","content_type":"application/json"}
        \\}
        ,
    });
    try db.addIndex(.{
        .name = "graph_b",
        .kind = .graph,
        .config_json =
        \\{
        \\  "source":{"kind":"artifact","artifact":"relations_b","mention_edge_type":"mentions",
        \\    "format":"extraction_relation","path":"$.relations[*]"},
        \\  "artifact":{"name":"relations_b","kind":"asset","field":"relations_b","content_type":"application/json"}
        \\}
        ,
    });
    try db.addResolver(.{
        .name = "kg_b",
        .table = "entities",
        .source_artifact = "relations_b",
        .resolution_artifact = "resolution_b",
        .key_template = "{{ lower _entity.label }}/{{ slug _entity.text }}",
        .config_generation = 1,
    });

    const resolution_key = try internal_keys.resolutionArtifactKeyAlloc(alloc, "doc:a", "resolution_b");
    defer alloc.free(resolution_key);
    const batch = derived_types.DerivedBatch{
        .sequence = 1,
        .changed_artifact_keys = &.{resolution_key},
    };

    const graph_a = index_manager_mod.ManagedIndexRef{ .name = "graph_a", .kind = .graph };
    try std.testing.expect(!db.derivedAsyncBatchAffectsManagedIndex(batch, graph_a));
    try std.testing.expect(!try db.derivedAsyncBatchAffectsManagedIndexForReplay(batch, graph_a));
    try std.testing.expect(!try db.derivedAsyncBatchAdvancesManagedIndexApplyStateForReplay(batch, graph_a));

    const graph_b = index_manager_mod.ManagedIndexRef{ .name = "graph_b", .kind = .graph };
    try std.testing.expect(db.derivedAsyncBatchAffectsManagedIndex(batch, graph_b));
    try std.testing.expect(try db.derivedAsyncBatchAffectsManagedIndexForReplay(batch, graph_b));
    try std.testing.expect(try db.derivedAsyncBatchAdvancesManagedIndexApplyStateForReplay(batch, graph_b));
}

test "db materializes doc->entity mention edges as provenance and clears them on delete" {
    const alloc = std.testing.allocator;

    var path_buf: [256]u8 = undefined;
    const path = tempPath(&path_buf);
    defer cleanupTempDir(path);

    var db = try DB.open(alloc, std.mem.span(path), .{});
    defer db.close();

    // The graph index materializes the relations_v1 extraction asset and, with
    // mention_edge_type set, emits doc->entity provenance edges from the durable
    // resolution artifact.
    try db.addIndex(.{
        .name = "prov_graph",
        .kind = .graph,
        .config_json =
        \\{
        \\  "source":{"kind":"artifact","artifact":"relations_v1","mention_edge_type":"mentions",
        \\    "format":"extraction_relation","path":"$.relations[*]"},
        \\  "artifact":{"name":"relations_v1","kind":"asset","field":"relations","content_type":"application/json"}
        \\}
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
        .writes = &.{.{
            .key = "doc:a",
            .value =
            \\{"relations":{"entities":[{"id":"e0","label":"person","text":"Ada Lovelace"},{"id":"e1","label":"org","text":"Antfly"}]}}
            ,
        }},
        .sync_level = .enrichments,
    });
    try db.runUntilIdle();

    const resolution_key = try internal_keys.resolutionArtifactKeyAlloc(alloc, "doc:a", "resolution_v1");
    defer alloc.free(resolution_key);
    {
        const raw = try db.core.store.get(alloc, resolution_key);
        defer alloc.free(raw);
        try std.testing.expect(std.mem.indexOf(u8, raw, "\"decision\":\"new\"") != null);
        try std.testing.expect(std.mem.indexOf(u8, raw, "person/ada_lovelace") != null);
        try std.testing.expect(std.mem.indexOf(u8, raw, "org/antfly") != null);
    }

    const ada_edge_key = try internal_keys.graphEdgeArtifactKeyAlloc(alloc, "doc:a", "prov_graph", "mentions", "person/ada_lovelace");
    defer alloc.free(ada_edge_key);
    {
        const raw = try db.core.store.get(alloc, ada_edge_key);
        defer alloc.free(raw);
        try std.testing.expect(std.mem.indexOf(u8, raw, "\"target_table\":\"entities\"") != null);
    }

    // Outbound: doc:a mentions both canonical entities.
    {
        const out = try db.getEdges(alloc, "prov_graph", "doc:a", "mentions", .out);
        defer graph_mod.GraphIndex.freeEdges(alloc, out);
        try std.testing.expectEqual(@as(usize, 2), out.len);
        // Each mention edge records the resolved DocRef target table so the
        // endpoint can be hydrated cross-table.
        for (out) |edge| {
            try std.testing.expect(std.mem.indexOf(u8, edge.metadata, "\"target_table\":\"entities\"") != null);
        }
    }
    // Inbound provenance: "which documents mention this entity" == inbound edges.
    {
        const inbound = try db.getEdges(alloc, "prov_graph", "person/ada_lovelace", "mentions", .in);
        defer graph_mod.GraphIndex.freeEdges(alloc, inbound);
        try std.testing.expectEqual(@as(usize, 1), inbound.len);
        try std.testing.expectEqualStrings("doc:a", inbound[0].source);
        try std.testing.expectEqualStrings("person/ada_lovelace", inbound[0].target);
    }

    // Deleting the source document clears its mention edges (delete-on-source-delete).
    try db.batch(.{ .deletes = &.{"doc:a"}, .sync_level = .enrichments });
    try db.runUntilIdle();
    {
        const out = try db.getEdges(alloc, "prov_graph", "doc:a", "mentions", .out);
        defer graph_mod.GraphIndex.freeEdges(alloc, out);
        try std.testing.expectEqual(@as(usize, 0), out.len);
        const inbound = try db.getEdges(alloc, "prov_graph", "person/ada_lovelace", "mentions", .in);
        defer graph_mod.GraphIndex.freeEdges(alloc, inbound);
        try std.testing.expectEqual(@as(usize, 0), inbound.len);
    }
}

test "db resolver removal retires resolution artifacts and mention graph state" {
    const alloc = std.testing.allocator;

    var path_buf: [256]u8 = undefined;
    const path = tempPath(&path_buf);
    defer cleanupTempDir(path);

    var sink = FakePromotionSink{ .alloc = alloc };
    defer sink.deinit();

    var db = try DB.open(alloc, std.mem.span(path), .{
        .executor = .{ .backend = .manual },
        .start_index_workers = false,
        .entity_sink = sink.sink(),
    });
    defer db.close();

    try db.addIndex(.{
        .name = "prov_graph",
        .kind = .graph,
        .config_json =
        \\{
        \\  "source":{"kind":"artifact","artifact":"relations_v1","mention_edge_type":"mentions",
        \\    "format":"extraction_relation","path":"$.relations[*]"},
        \\  "artifact":{"name":"relations_v1","kind":"asset","field":"relations","content_type":"application/json"}
        \\}
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
        .writes = &.{.{
            .key = "doc:a",
            .value =
            \\{"relations":{"entities":[{"id":"e0","label":"person","text":"Ada Lovelace"}]}}
            ,
        }},
        .sync_level = .enrichments,
    });
    try db.runUntilIdle();

    const asset_key = try internal_keys.artifactNamedPrefixAlloc(alloc, "doc:a", "asset", "relations_v1");
    defer alloc.free(asset_key);
    {
        const raw = try db.core.store.get(alloc, asset_key);
        defer alloc.free(raw);
        try std.testing.expect(std.mem.indexOf(u8, raw, "Ada Lovelace") != null);
    }

    const resolution_key = try internal_keys.resolutionArtifactKeyAlloc(alloc, "doc:a", "resolution_v1");
    defer alloc.free(resolution_key);
    {
        const raw = try db.core.store.get(alloc, resolution_key);
        defer alloc.free(raw);
        try std.testing.expect(std.mem.indexOf(u8, raw, "person/ada_lovelace") != null);
    }

    const graph_edge_key = try internal_keys.graphEdgeArtifactKeyAlloc(alloc, "doc:a", "prov_graph", "mentions", "person/ada_lovelace");
    defer alloc.free(graph_edge_key);
    {
        const raw = try db.core.store.get(alloc, graph_edge_key);
        defer alloc.free(raw);
        try std.testing.expect(raw.len > 0);
    }
    {
        const inbound = try db.getEdges(alloc, "prov_graph", "person/ada_lovelace", "mentions", .in);
        defer graph_mod.GraphIndex.freeEdges(alloc, inbound);
        try std.testing.expectEqual(@as(usize, 1), inbound.len);
    }

    try std.testing.expect(try db.removeResolver("kg"));

    const resolvers = try db.listResolvers(alloc);
    defer {
        for (resolvers) |*cfg| cfg.deinit(alloc);
        alloc.free(resolvers);
    }
    try std.testing.expectEqual(@as(usize, 0), resolvers.len);

    try std.testing.expectError(error.NotFound, db.core.store.get(alloc, resolution_key));
    try std.testing.expectError(error.NotFound, db.core.store.get(alloc, graph_edge_key));
    {
        const raw = try db.core.store.get(alloc, asset_key);
        defer alloc.free(raw);
        try std.testing.expect(std.mem.indexOf(u8, raw, "Ada Lovelace") != null);
    }
    {
        const inbound = try db.getEdges(alloc, "prov_graph", "person/ada_lovelace", "mentions", .in);
        defer graph_mod.GraphIndex.freeEdges(alloc, inbound);
        try std.testing.expectEqual(@as(usize, 0), inbound.len);
        const out = try db.getEdges(alloc, "prov_graph", "doc:a", "mentions", .out);
        defer graph_mod.GraphIndex.freeEdges(alloc, out);
        try std.testing.expectEqual(@as(usize, 0), out.len);
    }
}

test "db does not materialize review-band resolution as canonical mention edges" {
    const alloc = std.testing.allocator;

    var path_buf: [256]u8 = undefined;
    const path = tempPath(&path_buf);
    defer cleanupTempDir(path);

    var db = try DB.open(alloc, std.mem.span(path), .{});
    defer db.close();

    try db.addIndex(.{
        .name = "prov_graph",
        .kind = .graph,
        .config_json =
        \\{
        \\  "source":{"kind":"artifact","artifact":"relations_v1","mention_edge_type":"mentions",
        \\    "format":"extraction_relation","path":"$.relations[*]"},
        \\  "artifact":{"name":"relations_v1","kind":"asset","field":"relations","content_type":"application/json"}
        \\}
        ,
    });
    try db.addResolver(.{
        .name = "kg",
        .table = "entities",
        .source_artifact = "relations_v1",
        .resolution_artifact = "resolution_v1",
        .key_template = "{{ lower _entity.label }}/{{ slug _entity.text }}",
        .candidate_search = "prefix",
        .scorer_json =
        \\{ "comparisons": [ { "name": "name", "left": "canonical_text", "right": "canonical_name",
        \\  "levels": [
        \\    { "when": "exact", "weight": 8.0 },
        \\    { "when": "jaro_winkler > 0.92", "weight": 5.0 },
        \\    { "when": "jaro_winkler > 0.85", "weight": 2.0 },
        \\    { "else": true, "weight": -6.0 }
        \\  ] } ],
        \\  "combine": { "bias": -3.0 }, "decision": { "match": 0.9, "review": 0.6 } }
        ,
        .config_generation = 1,
    });

    const existing_key = try internal_keys.documentKeyAlloc(alloc, "person/ada_lovlace");
    defer alloc.free(existing_key);
    try db.core.store.put(existing_key,
        \\{"canonical_name":"Ada Lovlace","label":"person"}
    );

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

    const resolution_key = try internal_keys.resolutionArtifactKeyAlloc(alloc, "doc:a", "resolution_v1");
    defer alloc.free(resolution_key);
    const raw = try db.core.store.get(alloc, resolution_key);
    defer alloc.free(raw);
    var parsed = try std.json.parseFromSlice(std.json.Value, alloc, raw, .{});
    defer parsed.deinit();
    const ent = parsed.value.object.get("entities").?.array.items[0].object;
    try std.testing.expectEqualStrings("review", ent.get("decision").?.string);
    try std.testing.expectEqualStrings("person/ada_lovelace", ent.get("doc_ref").?.object.get("key").?.string);

    const out = try db.getEdges(alloc, "prov_graph", "doc:a", "mentions", .out);
    defer graph_mod.GraphIndex.freeEdges(alloc, out);
    try std.testing.expectEqual(@as(usize, 0), out.len);

    _ = try db.recordReviewDecision("doc:a", "relations_v1", "resolution_v1", "e0", .match, "entities", "person/ada_lovelace");
    try db.runUntilIdle();

    const curated_raw = try db.core.store.get(alloc, resolution_key);
    defer alloc.free(curated_raw);
    var curated = try std.json.parseFromSlice(std.json.Value, alloc, curated_raw, .{});
    defer curated.deinit();
    const curated_ent = curated.value.object.get("entities").?.array.items[0].object;
    try std.testing.expectEqualStrings("match", curated_ent.get("decision").?.string);
    try std.testing.expectEqualStrings("person/ada_lovelace", curated_ent.get("doc_ref").?.object.get("key").?.string);

    const curated_edges = try db.getEdges(alloc, "prov_graph", "doc:a", "mentions", .out);
    defer graph_mod.GraphIndex.freeEdges(alloc, curated_edges);
    try std.testing.expectEqual(@as(usize, 1), curated_edges.len);
    try std.testing.expectEqualStrings("person/ada_lovelace", curated_edges[0].target);
}

test "db mention edge weight is fused from extractor trust and mention confidence" {
    const alloc = std.testing.allocator;

    var path_buf: [256]u8 = undefined;
    const path = tempPath(&path_buf);
    defer cleanupTempDir(path);

    var db = try DB.open(alloc, std.mem.span(path), .{});
    defer db.close();

    try db.addIndex(.{
        .name = "prov_graph",
        .kind = .graph,
        .config_json =
        \\{
        \\  "source":{"kind":"artifact","artifact":"relations_v1","mention_edge_type":"mentions",
        \\    "format":"extraction_relation","path":"$.relations[*]"},
        \\  "artifact":{"name":"relations_v1","kind":"asset","field":"relations","content_type":"application/json"}
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

    const out = try db.getEdges(alloc, "prov_graph", "doc:a", "mentions", .out);
    defer graph_mod.GraphIndex.freeEdges(alloc, out);
    try std.testing.expectEqual(@as(usize, 1), out.len);
    try std.testing.expectEqualStrings("person/ada_lovelace", out[0].target);
    // Calibrated weight, not the legacy 1.0.
    try std.testing.expectApproxEqAbs(@as(f64, 0.72), out[0].weight, 1e-9);
}

test "db rewriteEntityEdges repoints provenance edges to a merge survivor" {
    const alloc = std.testing.allocator;

    var path_buf: [256]u8 = undefined;
    const path = tempPath(&path_buf);
    defer cleanupTempDir(path);

    var db = try DB.open(alloc, std.mem.span(path), .{});
    defer db.close();

    try db.addIndex(.{
        .name = "prov_graph",
        .kind = .graph,
        .config_json =
        \\{
        \\  "source":{"kind":"artifact","artifact":"relations_v1","mention_edge_type":"mentions",
        \\    "format":"extraction_relation","path":"$.relations[*]"},
        \\  "artifact":{"name":"relations_v1","kind":"asset","field":"relations","content_type":"application/json"}
        \\}
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
        .writes = &.{.{
            .key = "doc:a",
            .value =
            \\{"relations":{"entities":[{"id":"e0","label":"person","text":"Ada Lovelace"}]}}
            ,
        }},
        .sync_level = .enrichments,
    });
    try db.runUntilIdle();

    // The mention edge points at the resolved DocRef from the resolution artifact.
    {
        const inbound = try db.getEdges(alloc, "prov_graph", "person/ada_lovelace", "mentions", .in);
        defer graph_mod.GraphIndex.freeEdges(alloc, inbound);
        try std.testing.expectEqual(@as(usize, 1), inbound.len);
    }

    // Merge person/ada_lovelace into a canonical survivor: rewrite its edges.
    const rewritten = try db.rewriteEntityEdges(alloc, "prov_graph", "person/ada_lovelace", "person/ada_canonical");
    try std.testing.expectEqual(@as(usize, 1), rewritten);
    try db.runUntilIdle();

    // Inbound edges moved from the merged-away key to the survivor.
    {
        const old_inbound = try db.getEdges(alloc, "prov_graph", "person/ada_lovelace", "mentions", .in);
        defer graph_mod.GraphIndex.freeEdges(alloc, old_inbound);
        try std.testing.expectEqual(@as(usize, 0), old_inbound.len);

        const new_inbound = try db.getEdges(alloc, "prov_graph", "person/ada_canonical", "mentions", .in);
        defer graph_mod.GraphIndex.freeEdges(alloc, new_inbound);
        try std.testing.expectEqual(@as(usize, 1), new_inbound.len);
        try std.testing.expectEqualStrings("doc:a", new_inbound[0].source);
        try std.testing.expectEqualStrings("person/ada_canonical", new_inbound[0].target);
    }
}

test "db graph hydration fails closed for a not-yet-promoted entity node" {
    const alloc = std.testing.allocator;

    var path_buf: [256]u8 = undefined;
    const path = tempPath(&path_buf);
    defer cleanupTempDir(path);

    var db = try DB.open(alloc, std.mem.span(path), .{});
    defer db.close();

    try db.addIndex(.{
        .name = "prov_graph",
        .kind = .graph,
        .config_json =
        \\{
        \\  "source":{"kind":"artifact","artifact":"relations_v1","mention_edge_type":"mentions",
        \\    "format":"extraction_relation","path":"$.relations[*]"},
        \\  "artifact":{"name":"relations_v1","kind":"asset","field":"relations","content_type":"application/json"}
        \\}
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
        .writes = &.{.{
            .key = "doc:a",
            .value =
            \\{"relations":{"entities":[{"id":"e0","label":"person","text":"Ada Lovelace"}]}}
            ,
        }},
        .sync_level = .enrichments,
    });
    try db.runUntilIdle();

    const mention_query = graph_query_mod.GraphQuery{
        .query_type = .neighbors,
        .index_name = "prov_graph",
        .start_nodes = .{ .keys = &.{"doc:a"} },
        .params = .{ .edge_types = &.{"mentions"}, .direction = .out, .max_depth = 1 },
        .include_documents = true,
    };

    // The mention edge points at person/ada_lovelace, but that entity document
    // has not been promoted into this store: the node is returned as a graph
    // result with its key, hydrated to nothing (fail closed), never fabricated.
    {
        var result = try db.search(alloc, .{ .graph_queries = &.{.{ .name = "m", .query = mention_query }} });
        defer result.deinit();
        try std.testing.expectEqual(@as(usize, 1), result.graph_results.len);
        const hits = result.graph_results[0].hits;
        try std.testing.expectEqual(@as(usize, 1), hits.len);
        try std.testing.expectEqualStrings("person/ada_lovelace", hits[0].id);
        try std.testing.expect(hits[0].stored_data == null);

        // The reached node records its home table (from the mention edge's
        // `target_table` metadata) so the api can route hydration to the
        // entities table instead of failing closed against the query table.
        const nodes = result.graph_results[0].nodes;
        try std.testing.expectEqual(@as(usize, 1), nodes.len);
        try std.testing.expectEqualStrings("person/ada_lovelace", nodes[0].key);
        try std.testing.expect(nodes[0].table != null);
        try std.testing.expectEqualStrings("entities", nodes[0].table.?);
    }

    // Once the entity document exists (promoter wrote it; here co-located for the
    // single-store test), the same node hydrates instead of failing closed.
    try db.batch(.{
        .writes = &.{.{ .key = "person/ada_lovelace", .value =
        \\{"entity_type":"person","canonical_name":"Ada Lovelace"}
        }},
        .sync_level = .write,
    });
    try db.runUntilIdle();
    {
        var result = try db.search(alloc, .{ .graph_queries = &.{.{ .name = "m", .query = mention_query }} });
        defer result.deinit();
        const hits = result.graph_results[0].hits;
        try std.testing.expectEqual(@as(usize, 1), hits.len);
        try std.testing.expectEqualStrings("person/ada_lovelace", hits[0].id);
        try std.testing.expect(hits[0].stored_data != null);
        try std.testing.expect(std.mem.indexOf(u8, hits[0].stored_data.?, "Ada Lovelace") != null);
    }
}

test "db graph relation artifact materializer uses mapping templates" {
    const alloc = std.testing.allocator;

    var path_buf: [256]u8 = undefined;
    const path = tempPath(&path_buf);
    defer cleanupTempDir(path);

    var db = try DB.open(alloc, std.mem.span(path), .{});
    defer db.close();

    try db.addIndex(.{
        .name = "relations_graph",
        .kind = .graph,
        .config_json =
        \\{
        \\  "source":{"kind":"artifact","artifact":"relations_v1","path":"$.items[*]","format":"extraction_relation"},
        \\  "nodes":{"source":"{{ _doc.key }}","target":"{{ _item.to }}"},
        \\  "edge":{"type":"{{ _item.rel }}","weight":"{{ default _item.score 1.0 }}","metadata":{"evidence":"{{ _item.evidence }}","ordinal":"{{ _item_index }}","tenant":"{{ _doc.value.tenant_id }}"}},
        \\  "context":{"doc_fields":["tenant_id"]},
        \\  "artifact":{"name":"relations_v1","kind":"asset","field":"relations","content_type":"application/json"}
        \\}
        ,
    });

    try db.batch(.{
        .writes = &.{
            .{
                .key = "doc:a",
                .value =
                \\{"tenant_id":"tenant-a","relations":{"items":[{"rel":"cites","to":"doc:b","score":0.5,"evidence":"see section 2"}]}}
                ,
            },
        },
        .sync_level = .enrichments,
    });
    try db.runUntilIdle();

    const edges = try db.getEdges(alloc, "relations_graph", "doc:a", "cites", .out);
    defer graph_mod.GraphIndex.freeEdges(alloc, edges);
    try std.testing.expectEqual(@as(usize, 1), edges.len);
    try std.testing.expectEqualStrings("doc:b", edges[0].target);
    try std.testing.expectApproxEqAbs(@as(f64, 0.5), edges[0].weight, 0.0001);
    try std.testing.expect(std.mem.indexOf(u8, edges[0].metadata, "\"evidence\":\"see section 2\"") != null);
    try std.testing.expect(std.mem.indexOf(u8, edges[0].metadata, "\"ordinal\":\"0\"") != null);
    try std.testing.expect(std.mem.indexOf(u8, edges[0].metadata, "\"tenant\":\"tenant-a\"") != null);
}

test "db graph relation artifact materializer resolves entity refs and artifact template values" {
    const alloc = std.testing.allocator;

    var path_buf: [256]u8 = undefined;
    const path = tempPath(&path_buf);
    defer cleanupTempDir(path);

    var db = try DB.open(alloc, std.mem.span(path), .{});
    defer db.close();

    try db.addIndex(.{
        .name = "relations_graph",
        .kind = .graph,
        .config_json =
        \\{
        \\  "source":{"kind":"artifact","artifact":"relations_v1","path":"$.relations[*]","format":"extraction_graph"},
        \\  "nodes":{"source":"{{ _doc.key }}","target":"{{ _item.target.doc_ref.key }}"},
        \\  "edge":{"type":"{{ _item.type }}","metadata":{"artifact":"{{ _artifact.name }}","content_type":"{{ _artifact.content_type }}","source_text":"{{ _item.source.text }}","target_text":"{{ _item.target.text }}"}},
        \\  "artifact":{"name":"relations_v1","kind":"asset","field":"relations","content_type":"application/json"}
        \\}
        ,
    });

    try db.batch(.{
        .writes = &.{
            .{
                .key = "doc:a",
                .value =
                \\{"relations":{"entities":[{"id":"e0","text":"Ada Lovelace","document_id":"doc:a"},{"id":"e1","text":"Analytical Engine","doc_ref":{"key":"doc:b"}}],"relations":[{"type":"mentions","source":{"entity_id":"e0"},"target":{"entity_id":"e1"}}]}}
                ,
            },
        },
        .sync_level = .enrichments,
    });
    try db.runUntilIdle();

    const edges = try db.getEdges(alloc, "relations_graph", "doc:a", "mentions", .out);
    defer graph_mod.GraphIndex.freeEdges(alloc, edges);
    try std.testing.expectEqual(@as(usize, 1), edges.len);
    try std.testing.expectEqualStrings("doc:b", edges[0].target);
    try std.testing.expect(std.mem.indexOf(u8, edges[0].metadata, "\"artifact\":\"relations_v1\"") != null);
    try std.testing.expect(std.mem.indexOf(u8, edges[0].metadata, "\"content_type\":\"application/json\"") != null);
    try std.testing.expect(std.mem.indexOf(u8, edges[0].metadata, "\"source_text\":\"Ada Lovelace\"") != null);
    try std.testing.expect(std.mem.indexOf(u8, edges[0].metadata, "\"target_text\":\"Analytical Engine\"") != null);
}

test "db graph relation artifact materializer replaces stale document edges" {
    const alloc = std.testing.allocator;

    var path_buf: [256]u8 = undefined;
    const path = tempPath(&path_buf);
    defer cleanupTempDir(path);

    var db = try DB.open(alloc, std.mem.span(path), .{});
    defer db.close();

    try db.addIndex(.{
        .name = "relations_graph",
        .kind = .graph,
        .config_json =
        \\{
        \\  "source":{"kind":"artifact","artifact":"relations_v1","path":"$.relations[*]","format":"extraction_relation"},
        \\  "artifact":{"name":"relations_v1","kind":"asset","field":"relations","content_type":"application/json"}
        \\}
        ,
    });

    try db.batch(.{
        .writes = &.{
            .{ .key = "doc:a", .value = "{\"relations\":{\"relations\":[{\"type\":\"mentions\",\"target\":{\"document_id\":\"doc:b\"}}]}}" },
        },
        .sync_level = .enrichments,
    });
    try db.runUntilIdle();
    try db.batch(.{
        .writes = &.{
            .{ .key = "doc:a", .value = "{\"relations\":{\"relations\":[{\"type\":\"mentions\",\"target\":{\"document_id\":\"doc:c\"}}]}}" },
        },
        .sync_level = .enrichments,
    });
    try db.runUntilIdle();

    const edges = try db.getEdges(alloc, "relations_graph", "doc:a", "mentions", .out);
    defer graph_mod.GraphIndex.freeEdges(alloc, edges);
    try std.testing.expectEqual(@as(usize, 1), edges.len);
    try std.testing.expectEqualStrings("doc:c", edges[0].target);

    const stale_key = try internal_keys.graphEdgeArtifactKeyAlloc(alloc, "doc:a", "relations_graph", "mentions", "doc:b");
    defer alloc.free(stale_key);
    try std.testing.expectError(error.NotFound, db.core.store.get(alloc, stale_key));
}

test "db graph relation artifact materializer deletes edges when asset source disappears" {
    const alloc = std.testing.allocator;

    var path_buf: [256]u8 = undefined;
    const path = tempPath(&path_buf);
    defer cleanupTempDir(path);

    var db = try DB.open(alloc, std.mem.span(path), .{});
    defer db.close();

    try db.addIndex(.{
        .name = "relations_graph",
        .kind = .graph,
        .config_json =
        \\{
        \\  "source":{"kind":"artifact","artifact":"relations_v1","path":"$.relations[*]","format":"extraction_relation"},
        \\  "artifact":{"name":"relations_v1","kind":"asset","field":"relations","content_type":"application/json"}
        \\}
        ,
    });

    try db.batch(.{
        .writes = &.{
            .{ .key = "doc:a", .value = "{\"relations\":{\"relations\":[{\"type\":\"mentions\",\"target\":{\"document_id\":\"doc:b\"}}]}}" },
        },
        .sync_level = .enrichments,
    });
    try db.runUntilIdle();

    try db.batch(.{
        .writes = &.{
            .{ .key = "doc:a", .value = "{\"title\":\"relations removed\"}" },
        },
        .sync_level = .enrichments,
    });
    try db.runUntilIdle();

    const edges = try db.getEdges(alloc, "relations_graph", "doc:a", "mentions", .out);
    defer graph_mod.GraphIndex.freeEdges(alloc, edges);
    try std.testing.expectEqual(@as(usize, 0), edges.len);

    const asset_key = try internal_keys.artifactNamedPrefixAlloc(alloc, "doc:a", "asset", "relations_v1");
    defer alloc.free(asset_key);
    try std.testing.expectError(error.NotFound, db.core.store.get(alloc, asset_key));

    const stale_key = try internal_keys.graphEdgeArtifactKeyAlloc(alloc, "doc:a", "relations_graph", "mentions", "doc:b");
    defer alloc.free(stale_key);
    try std.testing.expectError(error.NotFound, db.core.store.get(alloc, stale_key));
}

test "db graph artifact source lifecycle reuses and protects asset enrichments" {
    const alloc = std.testing.allocator;

    var path_buf: [256]u8 = undefined;
    const path = tempPath(&path_buf);
    defer cleanupTempDir(path);

    var db = try DB.open(alloc, std.mem.span(path), .{});
    defer db.close();

    try db.addIndex(.{
        .name = "relations_graph_a",
        .kind = .graph,
        .config_json =
        \\{
        \\  "source":{"kind":"artifact","artifact":"relations_v1","path":"$.relations[*]","format":"extraction_relation"},
        \\  "artifact":{"name":"relations_v1","kind":"asset","field":"relations","content_type":"application/json"}
        \\}
        ,
    });

    const created = (try db.getEnrichment(alloc, .asset, "relations_v1")) orelse return error.TestUnexpectedResult;
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
        \\  "source":{"kind":"artifact","artifact":"relations_v1","path":"$.relations[*]","format":"extraction_relation"},
        \\  "artifact":{"name":"relations_v1","kind":"asset","field":"relations","content_type":"application/json"}
        \\}
        ,
    });

    try std.testing.expectError(error.EnrichmentInUse, db.deleteEnrichment(.asset, "relations_v1"));
    try std.testing.expect(try db.deleteIndex("relations_graph_a"));
    try std.testing.expectError(error.EnrichmentInUse, db.deleteEnrichment(.asset, "relations_v1"));

    const still_present = (try db.getEnrichment(alloc, .asset, "relations_v1")) orelse return error.TestUnexpectedResult;
    defer {
        var tmp = still_present;
        tmp.deinit(alloc);
    }
    try std.testing.expectEqualStrings("relations", still_present.field);
}

test "db resolver catalog persists across reopen" {
    const alloc = std.testing.allocator;

    var path_buf: [256]u8 = undefined;
    const path = tempPath(&path_buf);
    defer cleanupTempDir(path);

    {
        var db = try DB.open(alloc, std.mem.span(path), .{});
        defer db.close();
        try db.addResolver(.{
            .name = "knowledge_graph",
            .table = "entities",
            .source_artifact = "relations_v1",
            .resolution_artifact = "resolution_v1",
            .key_template = "{{ lower _entity.label }}/{{ slug _entity.canonical_text }}",
            .config_generation = 3,
        });
        try std.testing.expectError(error.ResolverAlreadyExists, db.addResolver(.{
            .name = "knowledge_graph",
            .table = "entities",
            .source_artifact = "relations_v1",
            .resolution_artifact = "resolution_v1",
            .key_template = "x",
        }));
        try std.testing.expectError(error.ResolverArtifactAlreadyExists, db.addResolver(.{
            .name = "duplicate_output",
            .table = "entities",
            .source_artifact = "other_v1",
            .resolution_artifact = "resolution_v1",
            .key_template = "x",
        }));
        try std.testing.expectError(error.ResolverSourceArtifactImmutable, db.upsertResolver(.{
            .name = "knowledge_graph",
            .table = "entities",
            .source_artifact = "other_v1",
            .resolution_artifact = "resolution_v1",
            .key_template = "x",
        }));
        try std.testing.expectError(error.ResolverArtifactImmutable, db.upsertResolver(.{
            .name = "knowledge_graph",
            .table = "entities",
            .source_artifact = "relations_v1",
            .resolution_artifact = "other_resolution_v1",
            .key_template = "x",
        }));
    }

    var db = try DB.open(alloc, std.mem.span(path), .{});
    defer db.close();

    const resolvers = try db.listResolvers(alloc);
    defer {
        for (resolvers) |*r| r.deinit(alloc);
        alloc.free(resolvers);
    }
    try std.testing.expectEqual(@as(usize, 1), resolvers.len);
    try std.testing.expectEqualStrings("knowledge_graph", resolvers[0].name);
    try std.testing.expectEqualStrings("entities", resolvers[0].table);
    try std.testing.expectEqual(@as(u64, 3), resolvers[0].config_generation);

    try std.testing.expect(try db.removeResolver("knowledge_graph"));
    const after = try db.listResolvers(alloc);
    defer alloc.free(after);
    try std.testing.expectEqual(@as(usize, 0), after.len);
}

test "db graph artifact source reuses user enrichment and rejects incompatible shorthand" {
    const alloc = std.testing.allocator;

    var path_buf: [256]u8 = undefined;
    const path = tempPath(&path_buf);
    defer cleanupTempDir(path);

    var db = try DB.open(alloc, std.mem.span(path), .{});
    defer db.close();

    try db.addEnrichment(.{
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
        \\  "source":{"kind":"artifact","artifact":"relations_v1","path":"$.relations[*]","format":"extraction_relation"}
        \\}
        ,
    });

    try std.testing.expectError(error.ConflictingEnrichmentConfig, db.addIndex(.{
        .name = "conflicting_graph",
        .kind = .graph,
        .config_json =
        \\{
        \\  "source":{"kind":"artifact","artifact":"relations_v1","path":"$.relations[*]","format":"extraction_relation"},
        \\  "artifact":{"name":"relations_v1","kind":"asset","field":"body","content_type":"application/json"}
        \\}
        ,
    }));
}

test "db graph source artifact deletion clears materialized graph edges" {
    const alloc = std.testing.allocator;

    var path_buf: [256]u8 = undefined;
    const path = tempPath(&path_buf);
    defer cleanupTempDir(path);

    var db = try DB.open(alloc, std.mem.span(path), .{});
    defer db.close();

    try db.addIndex(.{
        .name = "relations_graph",
        .kind = .graph,
        .config_json =
        \\{
        \\  "source":{"kind":"artifact","artifact":"relations_v1","path":"$.relations[*]","format":"extraction_relation"},
        \\  "artifact":{"name":"relations_v1","kind":"asset","field":"relations","content_type":"application/json"}
        \\}
        ,
    });

    try db.batch(.{
        .writes = &.{
            .{ .key = "doc:a", .value = "{\"relations\":{\"relations\":[{\"type\":\"mentions\",\"target\":{\"document_id\":\"doc:b\"}}]}}" },
        },
        .sync_level = .enrichments,
    });
    try db.runUntilIdle();

    {
        const edges = try db.getEdges(alloc, "relations_graph", "doc:a", "mentions", .out);
        defer graph_mod.GraphIndex.freeEdges(alloc, edges);
        try std.testing.expectEqual(@as(usize, 1), edges.len);
    }

    try db.batch(.{
        .deletes = &.{"doc:a"},
        .sync_level = .enrichments,
    });
    try db.runUntilIdle();

    const edges = try db.getEdges(alloc, "relations_graph", "doc:a", "mentions", .out);
    defer graph_mod.GraphIndex.freeEdges(alloc, edges);
    try std.testing.expectEqual(@as(usize, 0), edges.len);

    const graph_artifact_key = try internal_keys.graphEdgeArtifactKeyAlloc(alloc, "doc:a", "relations_graph", "mentions", "doc:b");
    defer alloc.free(graph_artifact_key);
    try std.testing.expectError(error.NotFound, db.core.store.get(alloc, graph_artifact_key));
}

test "db graph artifact edges are visible to graph search queries" {
    const alloc = std.testing.allocator;

    var path_buf: [256]u8 = undefined;
    const path = tempPath(&path_buf);
    defer cleanupTempDir(path);

    var db = try DB.open(alloc, std.mem.span(path), .{});
    defer db.close();

    try db.addIndex(.{
        .name = "relations_graph",
        .kind = .graph,
        .config_json =
        \\{
        \\  "source":{"kind":"artifact","artifact":"relations_v1","path":"$.relations[*]","format":"extraction_relation"},
        \\  "artifact":{"name":"relations_v1","kind":"asset","field":"relations","content_type":"application/json"}
        \\}
        ,
    });

    try db.batch(.{
        .writes = &.{
            .{ .key = "doc:a", .value = "{\"relations\":{\"relations\":[{\"type\":\"mentions\",\"target\":{\"document_id\":\"doc:b\"}}]}}" },
            .{ .key = "doc:b", .value = "{\"title\":\"Beta\"}" },
        },
        .sync_level = .enrichments,
    });
    try db.runUntilIdle();

    var result = try db.search(alloc, .{
        .graph_queries = &.{
            .{
                .name = "mentions",
                .query = .{
                    .query_type = .neighbors,
                    .index_name = "relations_graph",
                    .start_nodes = .{ .keys = &.{"doc:a"} },
                    .params = .{ .direction = .out, .edge_types = &.{"mentions"} },
                    .include_documents = true,
                },
            },
        },
        .limit = 10,
    });
    defer result.deinit();

    try std.testing.expectEqual(@as(usize, 1), result.graph_results.len);
    try std.testing.expectEqual(@as(u32, 1), result.graph_results[0].total_hits);
    try std.testing.expectEqualStrings("doc:b", result.graph_results[0].hits[0].id);
    try std.testing.expect(result.graph_results[0].hits[0].stored_data != null);
}

test "db graph artifact external node targets return ids without document hydration" {
    const alloc = std.testing.allocator;

    var path_buf: [256]u8 = undefined;
    const path = tempPath(&path_buf);
    defer cleanupTempDir(path);

    var db = try DB.open(alloc, std.mem.span(path), .{});
    defer db.close();

    try db.addIndex(.{
        .name = "relations_graph",
        .kind = .graph,
        .config_json =
        \\{
        \\  "source":{"kind":"artifact","artifact":"relations_v1","path":"$.relations[*]","format":"extraction_relation"},
        \\  "nodes":{"model":"external","target":"{{ _item.target.entity_id }}"},
        \\  "artifact":{"name":"relations_v1","kind":"asset","field":"relations","content_type":"application/json"}
        \\}
        ,
    });

    try db.batch(.{
        .writes = &.{
            .{ .key = "doc:a", .value = "{\"relations\":{\"relations\":[{\"type\":\"mentions\",\"target\":{\"entity_id\":\"entity:person:ada_lovelace\"}}]}}" },
        },
        .sync_level = .enrichments,
    });
    try db.runUntilIdle();

    var result = try db.search(alloc, .{
        .graph_queries = &.{
            .{
                .name = "mentions",
                .query = .{
                    .query_type = .neighbors,
                    .index_name = "relations_graph",
                    .start_nodes = .{ .keys = &.{"doc:a"} },
                    .params = .{ .direction = .out, .edge_types = &.{"mentions"} },
                    .include_documents = true,
                },
            },
        },
        .limit = 10,
    });
    defer result.deinit();

    try std.testing.expectEqual(@as(usize, 1), result.graph_results.len);
    try std.testing.expectEqual(@as(u32, 1), result.graph_results[0].total_hits);
    try std.testing.expectEqualStrings("entity:person:ada_lovelace", result.graph_results[0].hits[0].id);
    try std.testing.expect(result.graph_results[0].hits[0].stored_data == null);
    try std.testing.expect(result.graph_results[0].hits[0].doc_ordinal == null);
}

test "db async asset producer graph source materializes through replay" {
    const alloc = std.testing.allocator;

    var path_buf: [256]u8 = undefined;
    const path = tempPath(&path_buf);
    defer cleanupTempDir(path);

    var fake = TestAssetProducer{};
    var db = try DB.open(alloc, std.mem.span(path), .{
        .enrichment = .{
            .owner_id = "worker-a",
            .asset_producer = fake.producer(),
        },
    });
    defer db.close();

    try db.addIndex(.{
        .name = "relations_graph",
        .kind = .graph,
        .config_json =
        \\{
        \\  "source":{"kind":"artifact","artifact":"relations_v1","path":"$.relations[*]","format":"extraction_relation"},
        \\  "edge":{"metadata":{"artifact":"{{ _artifact.name }}","content_type":"{{ _artifact.content_type }}"}},
        \\  "artifact":{"name":"relations_v1","kind":"asset","field":"target_doc","content_type":"application/json","producer_json":{"type":"extractor","config":{"provider":"mock"}}}
        \\}
        ,
    });

    try db.batch(.{
        .writes = &.{
            .{ .key = "doc:a", .value = "{\"target_doc\":\"doc:b\"}" },
        },
        .sync_level = .write,
    });
    try db.runUntilIdle();
    try db.runUntilIdle();

    try std.testing.expectEqual(@as(usize, 1), fake.extractor_calls);
    const edges = try db.getEdges(alloc, "relations_graph", "doc:a", "mentions", .out);
    defer graph_mod.GraphIndex.freeEdges(alloc, edges);
    try std.testing.expectEqual(@as(usize, 1), edges.len);
    try std.testing.expectEqualStrings("doc:b", edges[0].target);
    try std.testing.expect(std.mem.indexOf(u8, edges[0].metadata, "\"artifact\":\"relations_v1\"") != null);
    try std.testing.expect(std.mem.indexOf(u8, edges[0].metadata, "\"content_type\":\"application/json\"") != null);
}

test "db async asset producer mention edges come from resolution artifacts" {
    const alloc = std.testing.allocator;

    var path_buf: [256]u8 = undefined;
    const path = tempPath(&path_buf);
    defer cleanupTempDir(path);

    var fake = TestAssetProducer{
        .extractor_output =
        \\{"entities":[{"id":"e0","label":"person","text":"A. Lovelace"}],"relations":[]}
        ,
    };
    var embedder = FixedVectorEmbedder{};
    var db = try DB.open(alloc, std.mem.span(path), .{
        .enrichment = .{
            .owner_id = "worker-a",
            .asset_producer = fake.producer(),
        },
        .resolution_embedder = embedder.interface(),
    });
    defer db.close();

    try db.addIndex(.{
        .name = "prov_graph",
        .kind = .graph,
        .config_json =
        \\{
        \\  "source":{"kind":"artifact","artifact":"relations_v1","path":"$.relations[*]",
        \\    "format":"extraction_relation","mention_edge_type":"mentions"},
        \\  "artifact":{"name":"relations_v1","kind":"asset","field":"body","content_type":"application/json","producer_json":{"type":"extractor","config":{"provider":"mock"}}}
        \\}
        ,
    });
    try db.addResolver(.{
        .name = "kg",
        .table = "entities",
        .source_artifact = "relations_v1",
        .resolution_artifact = "resolution_v1",
        .key_template = "{{ lower _entity.label }}/{{ slug _entity.text }}",
        .candidate_search = "prefix",
        .name_embedding = "name",
        .name_embedding_dims = 4,
        .scorer_json =
        \\{ "comparisons": [ { "name": "emb", "left": "name_embedding", "right": "name_embedding",
        \\  "levels": [ { "when": "cosine > 0.9", "weight": 8.0 }, { "else": true, "weight": -6.0 } ] } ],
        \\  "combine": { "bias": -3.0 }, "decision": { "match": 0.9 } }
        ,
        .config_generation = 1,
    });

    const entity_doc_key = try internal_keys.documentKeyAlloc(alloc, "person/ada_lovelace");
    defer alloc.free(entity_doc_key);
    try db.core.store.put(entity_doc_key,
        \\{ "canonical_name": "Ada Lovelace", "label": "person", "name_embedding": [1.0, 0.0, 0.0, 0.0] }
    );

    try db.batch(.{
        .writes = &.{.{
            .key = "doc:a",
            .value = "{\"body\":\"Ada mention\"}",
        }},
        .sync_level = .enrichments,
    });
    try db.runUntilIdle();
    try db.runUntilIdle();

    try std.testing.expectEqual(@as(usize, 1), fake.extractor_calls);

    const resolution_key = try internal_keys.resolutionArtifactKeyAlloc(alloc, "doc:a", "resolution_v1");
    defer alloc.free(resolution_key);
    const raw = try db.core.store.get(alloc, resolution_key);
    defer alloc.free(raw);
    var parsed = try std.json.parseFromSlice(std.json.Value, alloc, raw, .{});
    defer parsed.deinit();
    const ent = parsed.value.object.get("entities").?.array.items[0].object;
    try std.testing.expectEqualStrings("match", ent.get("decision").?.string);
    try std.testing.expectEqualStrings("person/ada_lovelace", ent.get("doc_ref").?.object.get("key").?.string);

    const edges = try db.getEdges(alloc, "prov_graph", "doc:a", "mentions", .out);
    defer graph_mod.GraphIndex.freeEdges(alloc, edges);
    try std.testing.expectEqual(@as(usize, 1), edges.len);
    try std.testing.expectEqualStrings("person/ada_lovelace", edges[0].target);
    try std.testing.expect(std.mem.indexOf(u8, edges[0].metadata, "\"target_table\":\"entities\"") != null);

    const deterministic_edges = try db.getEdges(alloc, "prov_graph", "person/a_lovelace", "mentions", .in);
    defer graph_mod.GraphIndex.freeEdges(alloc, deterministic_edges);
    try std.testing.expectEqual(@as(usize, 0), deterministic_edges.len);

    const relation_state_key = try graphAssetStateKeyAlloc(alloc, "doc:a", "prov_graph", "relations_v1");
    defer alloc.free(relation_state_key);
    try db.core.store.delete(relation_state_key);

    const asset_key = try internal_keys.artifactNamedPrefixAlloc(alloc, "doc:a", "asset", "relations_v1");
    defer alloc.free(asset_key);
    const changed = try derived_async.materializeGraphSourceArtifactsForIndex(alloc, db.core.store, db.core.index_manager, &.{asset_key}, "prov_graph", .{});
    defer freeOwnedKeySlice(alloc, changed);

    const graph_edge_key = try internal_keys.graphEdgeArtifactKeyAlloc(alloc, "doc:a", "prov_graph", "mentions", "person/ada_lovelace");
    defer alloc.free(graph_edge_key);
    const edge_raw = try db.core.store.get(alloc, graph_edge_key);
    defer alloc.free(edge_raw);
}

test "db graph artifact source replay catches up after reopen" {
    const alloc = std.testing.allocator;

    var path_buf: [256]u8 = undefined;
    const path = tempPath(&path_buf);
    defer cleanupTempDir(path);

    {
        var db = try DB.open(alloc, std.mem.span(path), .{ .start_index_workers = false });
        defer db.close();

        try db.addIndex(.{
            .name = "relations_graph",
            .kind = .graph,
            .config_json =
            \\{
            \\  "source":{"kind":"artifact","artifact":"relations_v1","path":"$.relations[*]","format":"extraction_relation"},
            \\  "artifact":{"name":"relations_v1","kind":"asset","field":"relations","content_type":"application/json"}
            \\}
            ,
        });

        const doc_key = try internal_keys.documentKeyAlloc(alloc, "doc:a");
        defer alloc.free(doc_key);
        const artifact_key = try internal_keys.artifactNamedPrefixAlloc(alloc, "doc:a", "asset", "relations_v1");
        defer alloc.free(artifact_key);
        var ctx = db.batchContext();
        const sequence = db.core.store.reserveNextReplaySequence(1);
        const replay_payload = try DB.derivedAsyncEncodeChangeRecordPayloadForContext(&ctx, .{
            .changed_artifact_keys = &.{artifact_key},
        }, sequence);
        defer alloc.free(replay_payload);

        try db.core.store.putBatchWithReplay(null, &.{
            .{ .key = doc_key, .value = "{\"title\":\"alpha\"}" },
            .{ .key = artifact_key, .value = "{\"relations\":[{\"type\":\"mentions\",\"target\":{\"document_id\":\"doc:b\"}}]}" },
        }, &.{}, .{
            .sequence = sequence,
            .payload = replay_payload,
        });
    }

    var reopened = try DB.open(alloc, std.mem.span(path), .{});
    defer reopened.close();
    try reopened.runUntilIdle();

    const edges = try reopened.getEdges(alloc, "relations_graph", "doc:a", "mentions", .out);
    defer graph_mod.GraphIndex.freeEdges(alloc, edges);
    try std.testing.expectEqual(@as(usize, 1), edges.len);
    try std.testing.expectEqualStrings("doc:b", edges[0].target);
}

test "db graph edge artifact replay catches up after reopen" {
    const alloc = std.testing.allocator;

    var path_buf: [256]u8 = undefined;
    const path = tempPath(&path_buf);
    defer cleanupTempDir(path);

    {
        var db = try DB.open(alloc, std.mem.span(path), .{ .start_index_workers = false });
        defer db.close();

        try db.addIndex(.{
            .name = "relations_graph",
            .kind = .graph,
            .config_json = "{}",
        });

        const graph_key = try internal_keys.graphEdgeArtifactKeyAlloc(alloc, "doc:a", "relations_graph", "mentions", "doc:b");
        defer alloc.free(graph_key);
        const graph_value = try enrichment_artifact_codec.encodeGraphEdgeAlloc(alloc, null, 0.8, 0, 0, "");
        defer alloc.free(graph_value);
        var ctx = db.batchContext();
        const sequence = db.core.store.reserveNextReplaySequence(1);
        const replay_payload = try DB.derivedAsyncEncodeChangeRecordPayloadForContext(&ctx, .{
            .changed_artifact_keys = &.{graph_key},
        }, sequence);
        defer alloc.free(replay_payload);

        try db.core.store.putBatchWithReplay(null, &.{
            .{ .key = graph_key, .value = graph_value },
        }, &.{}, .{
            .sequence = sequence,
            .payload = replay_payload,
        });
    }

    var reopened = try DB.open(alloc, std.mem.span(path), .{});
    defer reopened.close();
    try reopened.runUntilIdle();

    const edges = try reopened.getEdges(alloc, "relations_graph", "doc:a", "mentions", .out);
    defer graph_mod.GraphIndex.freeEdges(alloc, edges);
    try std.testing.expectEqual(@as(usize, 1), edges.len);
    try std.testing.expectEqualStrings("doc:b", edges[0].target);
    try std.testing.expectApproxEqAbs(@as(f64, 0.8), edges[0].weight, 0.0001);
}

test "db full_index precomputes generated enrichments into the committed batch" {
    const alloc = std.testing.allocator;

    var path_buf: [256]u8 = undefined;
    const path = tempPath(&path_buf);
    defer cleanupTempDir(path);

    var deterministic_dense = embedder_mod.DeterministicDenseEmbedder{};
    var deterministic_sparse = embedder_mod.DeterministicSparseEmbedder{};
    var db = try DB.open(alloc, std.mem.span(path), .{
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
}

test "db full_text sync level does not wait for dense hbc visibility" {
    const alloc = std.testing.allocator;

    var path_buf: [256]u8 = undefined;
    const path = tempPath(&path_buf);
    defer cleanupTempDir(path);

    var db = try DB.open(alloc, std.mem.span(path), .{});
    defer db.close();

    try db.addIndex(.{
        .name = "ft_v1",
        .kind = .full_text,
        .config_json = "{\"field\":\"title\"}",
    });
    try db.addIndex(.{
        .name = "dv_v1",
        .kind = .dense_vector,
        .config_json = "{\"field\":\"embedding\",\"dims\":3,\"metric\":\"l2_squared\"}",
    });

    try db.batch(.{
        .writes = &.{
            .{ .key = "doc:a", .value = "{\"title\":\"alpha routing\",\"embedding\":[1,0,0]}" },
        },
        .sync_level = .full_text,
    });

    const full_text_applied = try db.core.loadAppliedSequence(alloc, "ft_v1");
    const dense_applied = try db.core.loadAppliedSequence(alloc, "dv_v1");
    try std.testing.expectEqual(@as(u64, 1), full_text_applied);
    try std.testing.expectEqual(@as(u64, 0), dense_applied);
    try std.testing.expectEqual(@as(u64, 0), db.core.index_manager.denseIndex("dv_v1").?.index.metadata.active_count);

    var text_result = try db.search(alloc, .{
        .index_name = "ft_v1",
        .full_text = .{ .match = .{ .field = "title", .text = "alpha" } },
    });
    defer text_result.deinit();
    try std.testing.expectEqual(@as(u32, 1), text_result.total_hits);
}

test "db enrichments precomputes generated enrichments into the committed batch" {
    const alloc = std.testing.allocator;

    var path_buf: [256]u8 = undefined;
    const path = tempPath(&path_buf);
    defer cleanupTempDir(path);

    var deterministic_dense = embedder_mod.DeterministicDenseEmbedder{};
    var deterministic_sparse = embedder_mod.DeterministicSparseEmbedder{};
    var db = try DB.open(alloc, std.mem.span(path), .{
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
    try std.testing.expect(change_journal_mod.recordHasHint(journal_record.record, .full_text));
    try std.testing.expect(change_journal_mod.recordHasHint(journal_record.record, .dense_vector));
    try std.testing.expect(change_journal_mod.recordHasHint(journal_record.record, .sparse_vector));
    try std.testing.expect(!change_journal_mod.recordHasHint(journal_record.record, .enrichment));
    try std.testing.expect(journal_record.record.changed_artifact_keys.len > 0);
}

test "db replicated apply decouples client enrichment sync from raft apply execution" {
    const alloc = std.testing.allocator;

    var path_buf: [256]u8 = undefined;
    const path = tempPath(&path_buf);
    defer cleanupTempDir(path);

    var deterministic_dense = embedder_mod.DeterministicDenseEmbedder{};
    var db = try DB.open(alloc, std.mem.span(path), .{
        .start_index_workers = false,
        .enrichment = .{
            .owner_id = "worker-a",
            .dense_embedder = deterministic_dense.interface(),
        },
    });
    defer db.close();

    try db.addIndex(.{
        .name = "dv_v1",
        .kind = .dense_vector,
        .config_json = "{\"field\":\"embedding\",\"dims\":3,\"generator\":{\"kind\":\"dense_embedding\",\"source_field\":\"body\",\"artifact_name\":\"body_chunks_v1\",\"chunk_size\":8,\"chunk_overlap\":2,\"embedding_name\":\"chunk_dense_v1\"}}",
    });

    try db.batchReplicatedApply(.{
        .writes = &.{
            .{ .key = "doc:a", .value = "{\"body\":\"abcdefghijklmno\"}" },
        },
        .sync_level = .enrichments,
    });

    const journal_entries = try replay_stream_mod.iterateFrom(alloc, db.core.store, 1);
    defer {
        for (journal_entries) |*entry| entry.deinit(alloc);
        alloc.free(journal_entries);
    }
    try std.testing.expectEqual(@as(usize, 1), journal_entries.len);

    var journal_record = try change_journal_mod.decodeRecord(alloc, journal_entries[0].payload);
    defer journal_record.deinit();

    try std.testing.expect(change_journal_mod.recordHasHint(journal_record.record, .enrichment));
    try std.testing.expect(!change_journal_mod.recordHasHint(journal_record.record, .dense_vector));
    try std.testing.expectEqual(@as(usize, 0), journal_record.record.changed_artifact_keys.len);
}

test "db enrichments precomputed watermark advances across replay entries without enrichment debt" {
    const alloc = std.testing.allocator;

    var path_buf: [256]u8 = undefined;
    const path = tempPath(&path_buf);
    defer cleanupTempDir(path);

    var deterministic_dense = embedder_mod.DeterministicDenseEmbedder{};
    var db = try DB.open(alloc, std.mem.span(path), .{
        .enrichment = .{
            .owner_id = "worker-a",
            .dense_embedder = deterministic_dense.interface(),
        },
    });
    defer db.close();

    const inert_payload = try change_journal_mod.encodeRecord(alloc, .{
        .sequence = 1,
        .changed_doc_keys = &.{"doc:before"},
        .target_hints = &.{.full_text},
    });
    defer alloc.free(inert_payload);
    try db.core.store.appendReplayOpaque(alloc, 1, inert_payload);
    try db.enrichment_runtime.?.resumeFrom(0, 0);
    try std.testing.expectEqual(@as(u64, 0), db.enrichment_runtime.?.stats().applied_sequence);

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

    try std.testing.expectEqual(db.core.nextDerivedSequence(), db.enrichment_runtime.?.stats().applied_sequence);
}

test "db asset producer enrichments execute fake providers and skip unchanged state" {
    const alloc = std.testing.allocator;

    var path_buf: [256]u8 = undefined;
    const path = tempPath(&path_buf);
    defer cleanupTempDir(path);

    var fake = TestAssetProducer{};
    var db = try DB.open(alloc, std.mem.span(path), .{
        .enrichment = .{
            .owner_id = "worker-a",
            .asset_producer = fake.producer(),
        },
    });
    defer db.close();

    try db.addEnrichment(.{
        .name = "generated_title_v1",
        .kind = .asset,
        .field = "body",
        .content_type = "text/plain",
        .producer_json = "{\"type\":\"generator\",\"config\":{\"provider\":\"mock\"}}",
    });
    try db.addEnrichment(.{
        .name = "image_text_v1",
        .kind = .asset,
        .field = "image",
        .content_type = "text/plain",
        .producer_json = "{\"type\":\"reader\",\"config\":{\"provider\":\"mock\"}}",
    });
    try db.addEnrichment(.{
        .name = "audio_text_v1",
        .kind = .asset,
        .field = "audio",
        .content_type = "text/plain",
        .producer_json = "{\"type\":\"transcriber\",\"config\":{\"provider\":\"mock\"}}",
    });

    try db.batch(.{
        .writes = &.{.{
            .key = "doc:a",
            .value = "{\"body\":\"hello\",\"image\":\"data:image/png;base64,aaa\",\"audio\":\"https://example.test/a.wav\"}",
        }},
        .sync_level = .enrichments,
    });

    try std.testing.expectEqual(@as(usize, 3), fake.calls);
    try std.testing.expectEqual(@as(usize, 1), fake.generator_calls);
    try std.testing.expectEqual(@as(usize, 1), fake.reader_calls);
    try std.testing.expectEqual(@as(usize, 1), fake.transcriber_calls);

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
    try std.testing.expectEqual(@as(usize, 3), fake.calls);

    try db.batch(.{
        .writes = &.{.{
            .key = "doc:a",
            .value = "{\"body\":\"goodbye\",\"image\":\"data:image/png;base64,aaa\",\"audio\":\"https://example.test/a.wav\"}",
        }},
        .sync_level = .enrichments,
    });
    try std.testing.expectEqual(@as(usize, 4), fake.calls);
    try std.testing.expectEqual(@as(usize, 2), fake.generator_calls);
    try std.testing.expectEqual(@as(usize, 1), fake.reader_calls);
    try std.testing.expectEqual(@as(usize, 1), fake.transcriber_calls);

    const asset_key = try internal_keys.artifactNamedPrefixAlloc(alloc, "doc:a", "asset", "generated_title_v1");
    defer alloc.free(asset_key);
    const marker_key = try internal_keys.assetArtifactSourceIndexKeyAlloc(alloc, "generated_title_v1", "doc:a");
    defer alloc.free(marker_key);
    const state_key = try assetStateKeyAlloc(alloc, "doc:a", "generated_title_v1");
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

test "db document unit payload preserves pdf page provenance" {
    const alloc = std.testing.allocator;
    var text_regions = [_]document_extraction_mod.TextRegion{.{
        .span = .{ 0, 5 },
        .bbox = .{ 72, 700, 120, 712 },
    }};
    const unit = document_extraction_mod.Unit{
        .unit_id = @constCast("page:000001"),
        .unit_type = @constCast("page"),
        .text = @constCast("hello"),
        .method = @constCast("pdf_text"),
        .page_number = 1,
        .page_label = @constCast("i"),
        .page_bbox = .{ 0, 0, 612, 792 },
        .page_rotation = 90,
        .text_regions = text_regions[0..],
        .char_start = 0,
        .char_end = 5,
    };

    const payload = try documentUnitPayloadAlloc(alloc, "doc:a", "document_units_v1", unit, "data:application/pdf;base64,AA==", "application/pdf", .{ .range_id = "range:000000" });
    defer alloc.free(payload);

    var parsed = try std.json.parseFromSlice(std.json.Value, alloc, payload, .{});
    defer parsed.deinit();
    const provenance = parsed.value.object.get("provenance").?.object;
    try std.testing.expectEqualStrings("range:000000", parsed.value.object.get("_artifact_range_id").?.string);
    try std.testing.expectEqualStrings("unit", parsed.value.object.get("_artifact_range_kind").?.string);
    try std.testing.expectEqualStrings("local_committed", parsed.value.object.get("_artifact_route_status").?.string);
    try std.testing.expectEqual(@as(i64, 0), parsed.value.object.get("_artifact_owner_group_id").?.integer);
    try std.testing.expectEqual(@as(i64, 1), provenance.get("page_number").?.integer);
    try std.testing.expectEqualStrings("i", provenance.get("page_label").?.string);
    const page_bbox = provenance.get("page_bbox").?.array.items;
    try std.testing.expectEqual(@as(usize, 4), page_bbox.len);
    try std.testing.expectEqual(@as(i64, 612), page_bbox[2].integer);
    try std.testing.expectEqual(@as(i64, 90), provenance.get("page_rotation").?.integer);
    const format_provenance = provenance.get("format_provenance").?.object;
    try std.testing.expectEqualStrings("antfly.document_format_provenance.v1", format_provenance.get("schema").?.string);
    try std.testing.expectEqualStrings("application/pdf", format_provenance.get("source_content_type").?.string);
    try std.testing.expectEqualStrings("source_page_points", format_provenance.get("coordinate_system").?.string);
    try std.testing.expectEqualStrings("pdf_text", format_provenance.get("extraction_method").?.string);
    try std.testing.expect(!format_provenance.get("ocr_used").?.bool);
    const regions = format_provenance.get("text_regions").?.array.items;
    try std.testing.expectEqual(@as(usize, 1), regions.len);
    const region = regions[0].object;
    try std.testing.expectEqual(@as(i64, 0), region.get("span").?.array.items[0].integer);
    try std.testing.expectEqual(@as(i64, 5), region.get("span").?.array.items[1].integer);
    try std.testing.expectEqual(@as(i64, 72), region.get("bbox").?.array.items[0].integer);
    try std.testing.expectEqual(@as(i64, 712), region.get("bbox").?.array.items[3].integer);
}

test "db document unit payload marks scanned pdf pages as pending OCR" {
    const alloc = std.testing.allocator;
    const unit = document_extraction_mod.Unit{
        .unit_id = @constCast("page:000002"),
        .unit_type = @constCast("page"),
        .text = @constCast(""),
        .method = @constCast("pdf_ocr_pending"),
        .extraction_status = @constCast("pending_ocr"),
        .ocr_used = false,
        .page_number = 2,
        .page_label = @constCast("2"),
        .page_bbox = .{ 0, 0, 612, 792 },
        .char_start = 5,
        .char_end = 5,
    };

    const payload = try documentUnitPayloadAlloc(alloc, "doc:a", "document_units_v1", unit, "data:application/pdf;base64,AA==", "application/pdf", .{ .range_id = "range:000000" });
    defer alloc.free(payload);
    const fingerprint = try documentExtractionUnitFingerprintAlloc(alloc, unit);
    defer alloc.free(fingerprint);
    try std.testing.expectEqual(@as(usize, 64), fingerprint.len);

    var parsed = try std.json.parseFromSlice(std.json.Value, alloc, payload, .{});
    defer parsed.deinit();
    const provenance = parsed.value.object.get("provenance").?.object;
    try std.testing.expectEqualStrings("pending_ocr", parsed.value.object.get("extraction_status").?.string);
    try std.testing.expectEqualStrings("pdf_ocr_pending", provenance.get("method").?.string);
    try std.testing.expectEqualStrings("pending_ocr", provenance.get("extraction_status").?.string);
    try std.testing.expect(!provenance.get("ocr_used").?.bool);
    try std.testing.expectEqual(@as(i64, 2), provenance.get("page_number").?.integer);
    try std.testing.expectEqual(@as(i64, 5), provenance.get("char_start").?.integer);
    try std.testing.expectEqual(@as(i64, 5), provenance.get("char_end").?.integer);
    const format_provenance = provenance.get("format_provenance").?.object;
    try std.testing.expectEqualStrings("pdf_ocr_pending", format_provenance.get("extraction_method").?.string);
    try std.testing.expectEqualStrings("pending_ocr", format_provenance.get("extraction_status").?.string);
    try std.testing.expect(!format_provenance.get("ocr_used").?.bool);
}

test "db document extraction asset materializes unit artifacts from data url" {
    const alloc = std.testing.allocator;

    var path_buf: [256]u8 = undefined;
    const path = tempPath(&path_buf);
    defer cleanupTempDir(path);

    var fake = TestAssetProducer{};
    var db = try DB.open(alloc, std.mem.span(path), .{
        .enrichment = .{
            .owner_id = "worker-a",
            .asset_producer = fake.producer(),
        },
    });
    defer db.close();

    try db.addEnrichment(.{
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

    try std.testing.expectEqual(@as(usize, 0), fake.calls);

    const manifest_key = try internal_keys.artifactNamedPrefixAlloc(alloc, "doc:a", "asset", "document_units_v1");
    defer alloc.free(manifest_key);
    const unit_key = try internal_keys.documentUnitArtifactKeyAlloc(alloc, "doc:a", "document_units_v1", "document:000001");
    defer alloc.free(unit_key);
    const state_key = try assetStateKeyAlloc(alloc, "doc:a", "document_units_v1");
    defer alloc.free(state_key);

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

    const state = try db.core.store.get(alloc, state_key);
    defer alloc.free(state);
    try std.testing.expect(std.mem.indexOf(u8, state, "\"kind\":\"document_extraction_state_v1\"") != null);
    try std.testing.expect(std.mem.indexOf(u8, state, "\"unit_descriptors\"") != null);
    try std.testing.expect(std.mem.indexOf(u8, state, "document:000001") != null);

    try db.batch(.{
        .writes = &.{.{
            .key = "doc:a",
            .value = "{\"url\":\"\"}",
        }},
        .sync_level = .write,
    });
    try db.runUntilIdle();
    try std.testing.expectError(error.NotFound, db.core.store.get(alloc, manifest_key));
    try std.testing.expectError(error.NotFound, db.core.store.get(alloc, unit_key));
    try std.testing.expectError(error.NotFound, db.core.store.get(alloc, state_key));
}

test "db async document extraction accounts resource manager working set" {
    const alloc = std.testing.allocator;

    var path_buf: [256]u8 = undefined;
    const path = tempPath(&path_buf);
    defer cleanupTempDir(path);

    var resource_manager = resource_manager_mod.ResourceManager.init(.{});
    var db = try DB.open(alloc, std.mem.span(path), .{
        .resource_manager = &resource_manager,
        .enrichment = .{
            .owner_id = "worker-a",
            .enable_without_producers = true,
        },
    });
    defer db.close();

    try db.addEnrichment(.{
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

    const manifest_key = try internal_keys.artifactNamedPrefixAlloc(alloc, "doc:a", "asset", "document_units_v1");
    defer alloc.free(manifest_key);
    const manifest = try db.core.store.get(alloc, manifest_key);
    defer alloc.free(manifest);
    try std.testing.expect(std.mem.indexOf(u8, manifest, "\"status\":\"converged\"") != null);

    const stats = resource_manager.snapshot().slices[@intFromEnum(resource_manager_mod.Slice.document_extraction_working_set)];
    try std.testing.expect(stats.peak_bytes > 0);
    try std.testing.expectEqual(@as(u64, 0), stats.used_bytes);
}

test "db document extraction routes mixed files using source metadata fields" {
    const alloc = std.testing.allocator;

    var path_buf: [256]u8 = undefined;
    const path = tempPath(&path_buf);
    defer cleanupTempDir(path);

    var db = try DB.open(alloc, std.mem.span(path), .{
        .enrichment = .{
            .owner_id = "worker-a",
            .enable_without_producers = true,
        },
    });
    defer db.close();

    try db.addEnrichment(.{
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
        .sync_level = .write,
    });
    try db.runUntilIdle();

    const manifest_key = try internal_keys.artifactNamedPrefixAlloc(alloc, "doc:a", "asset", "document_units_v1");
    defer alloc.free(manifest_key);
    const unit_key = try internal_keys.documentUnitArtifactKeyAlloc(alloc, "doc:a", "document_units_v1", "note:000001");
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

    var path_buf: [256]u8 = undefined;
    const path = tempPath(&path_buf);
    defer cleanupTempDir(path);

    var db = try DB.open(alloc, std.mem.span(path), .{
        .start_index_workers = false,
        .ttl_cleanup = .{ .enabled = false },
        .enrichment = .{
            .owner_id = "worker-a",
            .enable_without_producers = true,
        },
    });
    defer db.close();

    try db.addEnrichment(.{
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
        .sync_level = .write,
    });
    try db.runUntilIdle();

    const manifest_key = try internal_keys.artifactNamedPrefixAlloc(alloc, "doc:docx", "asset", "document_units_v1");
    defer alloc.free(manifest_key);
    const section_key = try internal_keys.documentUnitArtifactKeyAlloc(alloc, "doc:docx", "document_units_v1", "section:000001");
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

    var path_buf: [256]u8 = undefined;
    const path = tempPath(&path_buf);
    defer cleanupTempDir(path);

    var db = try DB.open(alloc, std.mem.span(path), .{
        .start_index_workers = false,
        .ttl_cleanup = .{ .enabled = false },
        .enrichment = .{
            .owner_id = "worker-a",
            .enable_without_producers = true,
        },
    });
    defer db.close();

    try db.addEnrichment(.{
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
        .sync_level = .write,
    });
    try db.runUntilIdle();

    const manifest_key = try internal_keys.artifactNamedPrefixAlloc(alloc, "doc:archive", "asset", "document_units_v1");
    defer alloc.free(manifest_key);
    const entry_key = try internal_keys.documentUnitArtifactKeyAlloc(alloc, "doc:archive", "document_units_v1", "archive:entry:000001");
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

test "db document extraction stores image pending OCR unit" {
    const alloc = std.testing.allocator;

    var path_buf: [256]u8 = undefined;
    const path = tempPath(&path_buf);
    defer cleanupTempDir(path);

    var db = try DB.open(alloc, std.mem.span(path), .{
        .start_index_workers = false,
        .ttl_cleanup = .{ .enabled = false },
        .enrichment = .{
            .owner_id = "worker-a",
            .enable_without_producers = true,
        },
    });
    defer db.close();

    try db.addEnrichment(.{
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
        .sync_level = .write,
    });
    try db.runUntilIdle();

    const manifest_key = try internal_keys.artifactNamedPrefixAlloc(alloc, "doc:image", "asset", "document_units_v1");
    defer alloc.free(manifest_key);
    const unit_key = try internal_keys.documentUnitArtifactKeyAlloc(alloc, "doc:image", "document_units_v1", "image:000001");
    defer alloc.free(unit_key);

    const manifest = try db.core.store.get(alloc, manifest_key);
    defer alloc.free(manifest);
    try std.testing.expect(std.mem.indexOf(u8, manifest, "\"route_type\":\"image\"") != null);
    try std.testing.expect(std.mem.indexOf(u8, manifest, "\"unit_count\":1") != null);

    const unit_payload = try db.core.store.get(alloc, unit_key);
    defer alloc.free(unit_payload);
    try std.testing.expect(std.mem.indexOf(u8, unit_payload, "\"unit_type\":\"image\"") != null);
    try std.testing.expect(std.mem.indexOf(u8, unit_payload, "\"method\":\"ocr_pending\"") != null);
    try std.testing.expect(std.mem.indexOf(u8, unit_payload, "\"extraction_status\":\"pending_ocr\"") != null);
    try std.testing.expect(std.mem.indexOf(u8, unit_payload, "\"byte_length\":19") != null);
    try std.testing.expect(std.mem.indexOf(u8, unit_payload, "\"source_sha256\"") != null);
    try std.testing.expect(std.mem.indexOf(u8, unit_payload, "\"ocr_used\":false") != null);
    try std.testing.expect(std.mem.indexOf(u8, unit_payload, "\"transcript_used\":false") != null);
    try std.testing.expect(std.mem.indexOf(u8, unit_payload, "\"source_content_type\":\"image/png\"") != null);
}

test "db document extraction completes image OCR with reader producer" {
    const alloc = std.testing.allocator;

    var path_buf: [256]u8 = undefined;
    const path = tempPath(&path_buf);
    defer cleanupTempDir(path);

    var fake = TestAssetProducer{};
    var db = try DB.open(alloc, std.mem.span(path), .{
        .start_index_workers = false,
        .ttl_cleanup = .{ .enabled = false },
        .enrichment = .{
            .owner_id = "worker-a",
            .asset_producer = fake.producer(),
        },
    });
    defer db.close();

    try db.addEnrichment(.{
        .name = "document_units_v1",
        .kind = .asset,
        .field = "url",
        .content_type = "application/json",
        .producer_json = "{\"type\":\"document_extraction\",\"config\":{\"source\":{\"filename_field\":\"filename\",\"content_type_field\":\"mime_type\"},\"ocr\":{\"enabled\":true,\"config\":{\"provider\":\"mock-reader\"}}}}",
    });

    try db.batch(.{
        .writes = &.{.{
            .key = "doc:image-ocr",
            .value = "{\"filename\":\"scan.png\",\"mime_type\":\"image/png\",\"url\":\"data:image/png;base64,iVBORw0KGgppbWFnZSBieXRlcw==\"}",
        }},
        .sync_level = .write,
    });
    try db.runUntilIdle();

    try std.testing.expectEqual(@as(usize, 1), fake.calls);
    try std.testing.expectEqual(@as(usize, 1), fake.reader_calls);

    const unit_key = try internal_keys.documentUnitArtifactKeyAlloc(alloc, "doc:image-ocr", "document_units_v1", "image:000001");
    defer alloc.free(unit_key);
    const unit_payload = try db.core.store.get(alloc, unit_key);
    defer alloc.free(unit_payload);
    try std.testing.expect(std.mem.indexOf(u8, unit_payload, "\"method\":\"ocr_text\"") != null);
    try std.testing.expect(std.mem.indexOf(u8, unit_payload, "\"extraction_status\":\"completed\"") != null);
    try std.testing.expect(std.mem.indexOf(u8, unit_payload, "\"ocr_used\":true") != null);
    try std.testing.expect(std.mem.indexOf(u8, unit_payload, "\"text\":\"reader:data:image/png;base64,iVBORw0KGgppbWFnZSBieXRlcw==\"") != null);
}

test "db async document extraction reuses generated OCR text across streaming passes" {
    const alloc = std.testing.allocator;

    var path_buf: [256]u8 = undefined;
    const path = tempPath(&path_buf);
    defer cleanupTempDir(path);

    var fake = TestAssetProducer{};
    var db = try DB.open(alloc, std.mem.span(path), .{
        .ttl_cleanup = .{ .enabled = false },
        .enrichment = .{
            .owner_id = "worker-a",
            .asset_producer = fake.producer(),
        },
    });
    defer db.close();

    try db.addEnrichment(.{
        .name = "document_units_v1",
        .kind = .asset,
        .field = "url",
        .content_type = "application/json",
        .producer_json = "{\"type\":\"document_extraction\",\"config\":{\"source\":{\"filename_field\":\"filename\",\"content_type_field\":\"mime_type\"},\"ocr\":{\"enabled\":true,\"config\":{\"provider\":\"mock-reader\"}}}}",
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

    const unit_key = try internal_keys.documentUnitArtifactKeyAlloc(alloc, "doc:image-ocr-async", "document_units_v1", "image:000001");
    defer alloc.free(unit_key);
    const unit_payload = try db.core.store.get(alloc, unit_key);
    defer alloc.free(unit_payload);
    try std.testing.expect(std.mem.indexOf(u8, unit_payload, "\"method\":\"ocr_text\"") != null);
    try std.testing.expect(std.mem.indexOf(u8, unit_payload, "\"extraction_status\":\"completed\"") != null);
    try std.testing.expect(std.mem.indexOf(u8, unit_payload, "\"text\":\"reader:data:image/png;base64,iVBORw0KGgppbWFnZSBieXRlcw==\"") != null);
}

test "db document extraction stores structured OCR confidence and coordinates" {
    const alloc = std.testing.allocator;

    var path_buf: [256]u8 = undefined;
    const path = tempPath(&path_buf);
    defer cleanupTempDir(path);

    var fake = TestAssetProducer{
        .reader_output = "{\"text\":\"invoice total\",\"confidence\":0.92,\"bbox\":[1,2,101,42],\"warning\":\"low contrast\"}",
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

    try db.addEnrichment(.{
        .name = "document_units_v1",
        .kind = .asset,
        .field = "url",
        .content_type = "application/json",
        .producer_json = "{\"type\":\"document_extraction\",\"config\":{\"source\":{\"filename_field\":\"filename\",\"content_type_field\":\"mime_type\"},\"ocr\":{\"enabled\":true,\"config\":{\"provider\":\"mock-reader\"}}}}",
    });

    try db.batch(.{
        .writes = &.{.{
            .key = "doc:image-ocr-structured",
            .value = "{\"filename\":\"scan.png\",\"mime_type\":\"image/png\",\"url\":\"data:image/png;base64,iVBORw0KGgppbWFnZSBieXRlcw==\"}",
        }},
        .sync_level = .write,
    });
    try db.runUntilIdle();

    const unit_key = try internal_keys.documentUnitArtifactKeyAlloc(alloc, "doc:image-ocr-structured", "document_units_v1", "image:000001");
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
    try std.testing.expectApproxEqAbs(@as(f64, 1), relational_rows.jsonNumberAsF64(bbox[0]) orelse return error.TestUnexpectedResult, 0.0001);
    try std.testing.expectApproxEqAbs(@as(f64, 42), relational_rows.jsonNumberAsF64(bbox[3]) orelse return error.TestUnexpectedResult, 0.0001);
    try std.testing.expectEqualStrings("low contrast", parsed.value.object.get("extraction_warning").?.string);
    const provenance = parsed.value.object.get("provenance").?.object;
    try std.testing.expectApproxEqAbs(@as(f64, 0.92), provenance.get("confidence").?.float, 0.0001);
    try std.testing.expectApproxEqAbs(@as(f64, 0.92), provenance.get("ocr_confidence").?.float, 0.0001);
    try std.testing.expectEqualStrings("low contrast", provenance.get("extraction_warning").?.string);
}

test "db document extraction completes audio transcription with transcriber producer" {
    const alloc = std.testing.allocator;

    var path_buf: [256]u8 = undefined;
    const path = tempPath(&path_buf);
    defer cleanupTempDir(path);

    var fake = TestAssetProducer{
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

    try db.addEnrichment(.{
        .name = "document_units_v1",
        .kind = .asset,
        .field = "url",
        .content_type = "application/json",
        .producer_json = "{\"type\":\"document_extraction\",\"config\":{\"source\":{\"filename_field\":\"filename\",\"content_type_field\":\"mime_type\"},\"transcription\":{\"enabled\":true,\"config\":{\"provider\":\"mock-transcriber\"}}}}",
    });

    try db.batch(.{
        .writes = &.{.{
            .key = "doc:audio-transcript",
            .value = "{\"filename\":\"audio.mp3\",\"mime_type\":\"audio/mpeg\",\"url\":\"data:audio/mpeg;base64,SUQzYXVkaW8gYnl0ZXM=\"}",
        }},
        .sync_level = .write,
    });
    try db.runUntilIdle();

    try std.testing.expectEqual(@as(usize, 1), fake.calls);
    try std.testing.expectEqual(@as(usize, 1), fake.transcriber_calls);

    const unit_key = try internal_keys.documentUnitArtifactKeyAlloc(alloc, "doc:audio-transcript", "document_units_v1", "audio:000001");
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

test "db document extraction stores rfc822 email units" {
    const alloc = std.testing.allocator;

    var path_buf: [256]u8 = undefined;
    const path = tempPath(&path_buf);
    defer cleanupTempDir(path);

    var db = try DB.open(alloc, std.mem.span(path), .{
        .start_index_workers = false,
        .ttl_cleanup = .{ .enabled = false },
        .enrichment = .{
            .owner_id = "worker-a",
            .enable_without_producers = true,
        },
    });
    defer db.close();

    try db.addEnrichment(.{
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
        .sync_level = .write,
    });
    try db.runUntilIdle();

    const manifest_key = try internal_keys.artifactNamedPrefixAlloc(alloc, "doc:email", "asset", "document_units_v1");
    defer alloc.free(manifest_key);
    const headers_key = try internal_keys.documentUnitArtifactKeyAlloc(alloc, "doc:email", "document_units_v1", "email:headers");
    defer alloc.free(headers_key);
    const body_key = try internal_keys.documentUnitArtifactKeyAlloc(alloc, "doc:email", "document_units_v1", "email:body");
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

    var path_buf: [256]u8 = undefined;
    const path = tempPath(&path_buf);
    defer cleanupTempDir(path);

    var db = try DB.open(alloc, std.mem.span(path), .{
        .start_index_workers = false,
        .ttl_cleanup = .{ .enabled = false },
        .enrichment = .{
            .owner_id = "worker-a",
            .enable_without_producers = true,
        },
    });
    defer db.close();

    try db.addEnrichment(.{
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
        .sync_level = .write,
    });
    try db.runUntilIdle();

    const manifest_key = try internal_keys.artifactNamedPrefixAlloc(alloc, "doc:multipart-email", "asset", "document_units_v1");
    defer alloc.free(manifest_key);
    const plain_key = try internal_keys.documentUnitArtifactKeyAlloc(alloc, "doc:multipart-email", "document_units_v1", "email:part:000001");
    defer alloc.free(plain_key);
    const html_key = try internal_keys.documentUnitArtifactKeyAlloc(alloc, "doc:multipart-email", "document_units_v1", "email:part:000002");
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

    var path_buf: [256]u8 = undefined;
    const path = tempPath(&path_buf);
    defer cleanupTempDir(path);

    var db = try DB.open(alloc, std.mem.span(path), .{
        .enrichment = .{
            .owner_id = "worker-a",
            .enable_without_producers = true,
        },
    });
    defer db.close();

    try db.addEnrichment(.{
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
        .sync_level = .write,
    });
    try db.runUntilIdle();

    const manifest_key = try internal_keys.artifactNamedPrefixAlloc(alloc, "doc:bin", "asset", "document_units_v1");
    defer alloc.free(manifest_key);
    const state_key = try assetStateKeyAlloc(alloc, "doc:bin", "document_units_v1");
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

test "db document extraction manifest classifies unit fingerprint keeps" {
    const alloc = std.testing.allocator;
    const unit_keys = [_][]const u8{ "unit:a", "unit:b" };
    const chunk_keys = [_][]const u8{};
    const previous_unit_keys = [_][]const u8{ "unit:a", "unit:b" };
    const previous_chunk_keys = [_][]const u8{};
    const units = [_]document_extraction_mod.Unit{
        .{
            .unit_id = @constCast("unit:a"),
            .unit_type = @constCast("document"),
            .text = @constCast("same"),
            .method = @constCast("text"),
        },
        .{
            .unit_id = @constCast("unit:b"),
            .unit_type = @constCast("document"),
            .text = @constCast("changed"),
            .method = @constCast("text"),
        },
    };
    const extraction = document_extraction_mod.Result{
        .content_type = @constCast("text/plain"),
        .route_type = @constCast("text"),
        .units = @constCast(units[0..]),
    };
    const desired_descriptors = [_]DocumentExtractionUnitDescriptor{
        .{ .key = "unit:a", .fingerprint = "same-fingerprint" },
        .{ .key = "unit:b", .fingerprint = "new-fingerprint" },
    };
    const previous_descriptors = [_]DocumentExtractionUnitDescriptor{
        .{ .key = "unit:a", .fingerprint = "same-fingerprint" },
        .{ .key = "unit:b", .fingerprint = "old-fingerprint" },
    };

    const manifest = try documentExtractionManifestPayloadAlloc(
        alloc,
        "doc:a",
        "document_units_v1",
        "data:text/plain,same",
        "source-fingerprint",
        extraction,
        &unit_keys,
        &desired_descriptors,
        &chunk_keys,
        &.{},
        &previous_unit_keys,
        &previous_descriptors,
        &previous_chunk_keys,
        2,
        1,
        2,
        "converged",
    );
    defer alloc.free(manifest);

    try std.testing.expect(std.mem.indexOf(u8, manifest, "\"operation_granularity\":\"unit_fingerprint\"") != null);
    try std.testing.expect(std.mem.indexOf(u8, manifest, "\"status\":\"converged\"") != null);
    try std.testing.expect(std.mem.indexOf(u8, manifest, "\"owner_group_id\":0") != null);
    try std.testing.expect(std.mem.indexOf(u8, manifest, "\"placement_generation\":0") != null);
    try std.testing.expect(std.mem.indexOf(u8, manifest, "\"route_status\":\"local_committed\"") != null);
    try std.testing.expect(std.mem.indexOf(u8, manifest, "\"split_eligible\":true") != null);
    try std.testing.expect(std.mem.indexOf(u8, manifest, "\"coverage_plan\"") != null);
    try std.testing.expect(std.mem.indexOf(u8, manifest, "\"full_text_replay\":\"stored_artifact_required\"") != null);
    try std.testing.expect(std.mem.indexOf(u8, manifest, "\"watermark_required_before_suppression\":true") != null);
    try std.testing.expect(std.mem.indexOf(u8, manifest, "\"op\":\"keep\"") != null);
    try std.testing.expect(std.mem.indexOf(u8, manifest, "\"op\":\"upsert\"") != null);
    try std.testing.expect(std.mem.indexOf(u8, manifest, "\"fingerprint_match\":true") != null);
    try std.testing.expect(std.mem.indexOf(u8, manifest, "\"fingerprint_match\":false") != null);
    try std.testing.expect(std.mem.indexOf(u8, manifest, "\"first_key\":\"unit:a\"") != null);
    try std.testing.expect(std.mem.indexOf(u8, manifest, "\"first_key\":\"unit:b\"") != null);
}

test "db document extraction skips stable unit local rewrites without text consumers" {
    const alloc = std.testing.allocator;

    var path_buf: [256]u8 = undefined;
    const path = tempPath(&path_buf);
    defer cleanupTempDir(path);

    var counting = CountingDenseEmbedder{};
    var fake = TestAssetProducer{};
    var db = try DB.open(alloc, std.mem.span(path), .{
        .enrichment = .{
            .owner_id = "worker-a",
            .dense_embedder = counting.interface(),
            .asset_producer = fake.producer(),
        },
    });
    defer db.close();

    try db.addEnrichment(.{
        .name = "document_units_v1",
        .kind = .asset,
        .field = "url",
        .content_type = "application/json",
        .producer_json = "{\"type\":\"document_extraction\",\"config\":{}}",
    });
    try db.addEnrichment(.{
        .name = "document_chunks_v1",
        .kind = .chunk,
        .field = "text",
        .source_artifact_name = "document_units_v1",
        .chunk_size = 256,
    });
    try db.addEnrichment(.{
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
        .sync_level = .write,
    });
    try db.runUntilIdle();
    const first_calls = counting.calls;
    try std.testing.expectEqual(@as(usize, 1), first_calls);

    const manifest_key = try internal_keys.artifactNamedPrefixAlloc(alloc, "doc:a", "asset", "document_units_v1");
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

test "db applies document artifact child range batch without source row write" {
    const alloc = std.testing.allocator;

    var path_buf: [256]u8 = undefined;
    const path = tempPath(&path_buf);
    defer cleanupTempDir(path);

    var db = try DB.open(alloc, std.mem.span(path), .{
        .start_index_workers = false,
        .ttl_cleanup = .{ .enabled = false },
    });
    defer db.close();

    const artifact_key = try internal_keys.documentUnitArtifactKeyAlloc(alloc, "doc:a", "document_units_v1", "page:000001");
    defer alloc.free(artifact_key);
    const artifact_value =
        "{\"_parent_doc_key\":\"doc:a\",\"_artifact_name\":\"document_units_v1\",\"_artifact_range_id\":\"range:000000\",\"_artifact_range_kind\":\"unit\",\"_artifact_route_status\":\"remote_committed\",\"_artifact_owner_group_id\":7002,\"unit_id\":\"page:000001\",\"text\":\"alpha\"}";
    const writes = [_]types.BatchWrite{.{
        .key = artifact_key,
        .value = artifact_value,
    }};

    const sequence = try db.applyDocumentArtifactChildRangeBatch(.{
        .artifact_writes = writes[0..],
        .sync_level = .write,
    });
    try std.testing.expect(sequence > 0);

    const stored = try db.core.store.get(alloc, artifact_key);
    defer alloc.free(stored);
    try std.testing.expectEqualStrings(artifact_value, stored);

    const source_store_key = try internal_keys.documentKeyAlloc(alloc, "doc:a");
    defer alloc.free(source_store_key);
    try std.testing.expectError(error.NotFound, db.core.store.get(alloc, source_store_key));

    const deletes = [_][]const u8{artifact_key};
    const delete_sequence = try db.applyDocumentArtifactChildRangeBatch(.{
        .artifact_delete_keys = deletes[0..],
        .sync_level = .write,
    });
    try std.testing.expect(delete_sequence > sequence);
    try std.testing.expectError(error.NotFound, db.core.store.get(alloc, artifact_key));
}

test "db dispatches generated document child range artifacts to remote owner" {
    const alloc = std.testing.allocator;

    var path_buf: [256]u8 = undefined;
    const path = tempPath(&path_buf);
    defer cleanupTempDir(path);

    var db = try DB.open(alloc, std.mem.span(path), .{
        .start_index_workers = false,
        .ttl_cleanup = .{ .enabled = false },
    });
    defer db.close();

    try db.addEnrichment(.{
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

        fn deinit(self: *@This(), allocator: Allocator) void {
            if (self.first_key) |key| allocator.free(key);
            if (self.first_value) |value| allocator.free(value);
        }

        fn dispatcher(self: *@This()) DocumentArtifactChildRangeDispatcher {
            return .{ .ptr = self, .apply = apply };
        }

        fn apply(ptr: *anyopaque, allocator: Allocator, dispatch: DocumentArtifactChildRangeDispatch) !void {
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

    var path_buf: [256]u8 = undefined;
    const path = tempPath(&path_buf);
    defer cleanupTempDir(path);

    var db = try DB.open(alloc, std.mem.span(path), .{
        .start_index_workers = false,
        .ttl_cleanup = .{ .enabled = false },
    });
    defer db.close();

    try db.addEnrichment(.{
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

        fn deinit(self: *@This(), allocator: Allocator) void {
            if (self.first_key) |key| allocator.free(key);
        }

        fn dispatcher(self: *@This()) DocumentArtifactChildRangeDispatcher {
            return .{ .ptr = self, .apply = apply };
        }

        fn apply(ptr: *anyopaque, allocator: Allocator, dispatch: DocumentArtifactChildRangeDispatch) !void {
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

    const outbox_prefix = try internal_keys.documentChildRangeOutboxRootPrefixAlloc(alloc);
    defer alloc.free(outbox_prefix);
    const pending = try db.core.scanStorePrefix(alloc, outbox_prefix);
    defer docstore_mod.DocStore.freeResults(alloc, pending);
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
    defer docstore_mod.DocStore.freeResults(alloc, after);
    try std.testing.expectEqual(@as(usize, 0), after.len);
}

test "db document extraction manifest inspection and reprocess API" {
    const alloc = std.testing.allocator;

    var path_buf: [256]u8 = undefined;
    const path = tempPath(&path_buf);
    defer cleanupTempDir(path);

    var db = try DB.open(alloc, std.mem.span(path), .{
        .enrichment = .{
            .owner_id = "worker-a",
            .enable_without_producers = true,
        },
    });
    defer db.close();

    try db.addEnrichment(.{
        .name = "document_units_v1",
        .kind = .asset,
        .field = "url",
        .content_type = "application/json",
        .producer_json = "{\"type\":\"document_extraction\",\"config\":{}}",
    });
    try db.addEnrichment(.{
        .name = "document_chunks_v1",
        .kind = .chunk,
        .field = "text",
        .source_artifact_name = "document_units_v1",
        .chunk_size = 256,
    });
    try db.batch(.{
        .writes = &.{.{
            .key = "doc:a",
            .value = "{\"url\":\"data:text/plain;base64,YWxwaGEgYmV0YSBnYW1tYQ==\"}",
        }},
        .sync_level = .write,
    });
    try db.runUntilIdle();

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

    var after = (try db.getDocumentArtifactManifest(alloc, "doc:a", "document_units_v1")) orelse return error.TestUnexpectedResult;
    defer after.deinit(alloc);
    try std.testing.expectEqual(@as(u64, 2), after.generation);
    try std.testing.expectEqualStrings("remote", after.child_ranges[0].placement);
    try std.testing.expectEqual(@as(?u64, 7001), after.child_ranges[0].owner_group_id);
    try std.testing.expectEqual(@as(?u64, 2), after.child_ranges[0].placement_generation);
    try std.testing.expectEqualStrings("remote_committed", after.child_ranges[0].route_status.?);
    try std.testing.expect(std.mem.indexOf(u8, after.manifest_json, "\"op\":\"upsert\"") != null);
    const routed_unit_payload = try db.core.store.get(alloc, after.child_ranges[0].start_key);
    defer alloc.free(routed_unit_payload);
    try std.testing.expect(std.mem.indexOf(u8, routed_unit_payload, "\"_artifact_route_status\":\"remote_committed\"") != null);
    try std.testing.expect(std.mem.indexOf(u8, routed_unit_payload, "\"_artifact_owner_group_id\":7001") != null);

    try std.testing.expect(!try db.reprocessDocumentArtifact(alloc, "doc:missing", "document_units_v1"));
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
        .sync_level = .write,
    });
    try db.runUntilIdle();

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
    try std.testing.expect(range_after_a.generation >= 2);

    var range_second = try db.reprocessDocumentArtifactRange(alloc, "document_units_v1", .{ .from_key = range_first.next_key.? });
    defer range_second.deinit(alloc);
    try std.testing.expectEqual(@as(usize, 1), range_second.scanned);
    try std.testing.expectEqual(@as(usize, 1), range_second.reprocessed);
    try std.testing.expect(range_second.next_key == null);

    var range_after_b = (try db.getDocumentArtifactManifest(alloc, "doc:b", "document_units_v1")) orelse return error.TestUnexpectedResult;
    defer range_after_b.deinit(alloc);
    try std.testing.expectEqual(@as(u64, 2), range_after_b.generation);
}

test "db document extraction unit ranges split by text bytes" {
    const alloc = std.testing.allocator;
    const text_a = try alloc.alloc(u8, 600 * 1024);
    defer alloc.free(text_a);
    const text_b = try alloc.alloc(u8, 400 * 1024);
    defer alloc.free(text_b);
    const text_c = try alloc.alloc(u8, 100 * 1024);
    defer alloc.free(text_c);
    @memset(text_a, 'a');
    @memset(text_b, 'b');
    @memset(text_c, 'c');

    const units = [_]document_extraction_mod.Unit{
        .{
            .unit_id = @constCast("unit:a"),
            .unit_type = @constCast("document"),
            .text = text_a,
            .method = @constCast("text"),
        },
        .{
            .unit_id = @constCast("unit:b"),
            .unit_type = @constCast("document"),
            .text = text_b,
            .method = @constCast("text"),
        },
        .{
            .unit_id = @constCast("unit:c"),
            .unit_type = @constCast("document"),
            .text = text_c,
            .method = @constCast("text"),
        },
    };

    try std.testing.expectEqual(@as(usize, 2), documentExtractionUnitRangeCount(&units));
    try std.testing.expectEqual(@as(usize, 0), documentExtractionUnitRangeIndex(&units, 0));
    try std.testing.expectEqual(@as(usize, 0), documentExtractionUnitRangeIndex(&units, 1));
    try std.testing.expectEqual(@as(usize, 1), documentExtractionUnitRangeIndex(&units, 2));
}

test "db document extraction failure records last error and clears stale children" {
    const alloc = std.testing.allocator;

    var path_buf: [256]u8 = undefined;
    const path = tempPath(&path_buf);
    defer cleanupTempDir(path);

    var db = try DB.open(alloc, std.mem.span(path), .{});
    defer db.close();

    try db.addEnrichment(.{
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
    try std.testing.expectEqual(@as(usize, 0), after.unit_count);
    try std.testing.expectEqual(@as(usize, 0), after.child_range_count);
    try std.testing.expect(after.state_json == null);
    try std.testing.expect(after.last_error_code != null);
    try std.testing.expectEqualStrings("InvalidDataUri", after.last_error_code.?);
    try std.testing.expect(after.last_error_message != null);
    try std.testing.expectEqualStrings("remote content download failed", after.last_error_message.?);
    try std.testing.expect(std.mem.indexOf(u8, after.manifest_json, "\"last_error\"") != null);
    try std.testing.expectError(error.NotFound, db.core.store.get(alloc, stale_unit_key));

    var list = try db.listDocumentArtifactManifests(alloc, "doc:a");
    defer list.deinit(alloc);
    try std.testing.expectEqual(@as(usize, 1), list.artifacts.len);
    try std.testing.expectEqualStrings("InvalidDataUri", list.artifacts[0].last_error_code.?);
}

test "db document extraction skips stable unit local rewrites while replaying full text from stored artifacts" {
    const alloc = std.testing.allocator;

    var path_buf: [256]u8 = undefined;
    const path = tempPath(&path_buf);
    defer cleanupTempDir(path);

    var counting = CountingDenseEmbedder{};
    var db = try DB.open(alloc, std.mem.span(path), .{
        .enrichment = .{
            .owner_id = "worker-a",
            .dense_embedder = counting.interface(),
        },
    });
    defer db.close();

    try db.addEnrichment(.{
        .name = "document_units_v1",
        .kind = .asset,
        .field = "url",
        .content_type = "application/json",
        .producer_json = "{\"type\":\"document_extraction\",\"config\":{}}",
    });
    try db.addEnrichment(.{
        .name = "document_chunks_v1",
        .kind = .chunk,
        .field = "text",
        .source_artifact_name = "document_units_v1",
        .chunk_size = 256,
    });
    try db.addEnrichment(.{
        .name = "document_chunk_dense_v1",
        .kind = .embedding,
        .field = "text",
        .source_artifact_name = "document_chunks_v1",
        .expected_dims = 3,
    });
    try db.addIndex(.{
        .name = "ft_document_units",
        .kind = .full_text,
        .config_json = "{\"artifact_name\":\"document_units_v1\"}",
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

    const first_value = "{\"url\":\"data:text/plain;base64,YWxwaGEgYmV0YSBnYW1tYQ==\"}";
    try db.batch(.{
        .writes = &.{.{ .key = "doc:a", .value = first_value }},
        .sync_level = .full_index,
    });
    try db.runUntilIdle();
    const first_calls = counting.calls;
    try std.testing.expectEqual(@as(usize, 1), first_calls);

    const second_value = "{\"url\":\"data:text/plain,alpha%20beta%20gamma\"}";
    var extracted = [_]mapper.ExtractedWrite{try mapper.extractWrite(alloc, "doc:a", second_value)};
    defer extracted[0].deinit(alloc);
    var precomputed = try db.prepareGeneratedEnrichments(.{
        .writes = &.{.{ .key = "doc:a", .value = second_value }},
    }, &extracted, .all, &.{});
    defer precomputed.deinit(alloc);

    const unit_key = try internal_keys.documentUnitArtifactKeyAlloc(alloc, "doc:a", "document_units_v1", "document:000001");
    defer alloc.free(unit_key);
    const chunk_key = try internal_keys.documentUnitChunkArtifactKeyAlloc(alloc, "doc:a", "document_chunks_v1", "document:000001", 0);
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

    var unit_result = try db.search(alloc, .{
        .index_name = "ft_document_units",
        .full_text = .{ .match = .{ .field = "text", .text = "gamma" } },
        .return_mode = .chunk,
    });
    defer unit_result.deinit();
    try std.testing.expectEqual(@as(u32, 1), unit_result.total_hits);

    var chunk_result = try db.search(alloc, .{
        .index_name = "ft_document_chunks",
        .full_text = .{ .match = .{ .field = "text", .text = "gamma" } },
        .return_mode = .chunk,
    });
    defer chunk_result.deinit();
    try std.testing.expectEqual(@as(u32, 1), chunk_result.total_hits);
}

test "db document extraction chunks units through source artifact enrichment" {
    const alloc = std.testing.allocator;

    var path_buf: [256]u8 = undefined;
    const path = tempPath(&path_buf);
    defer cleanupTempDir(path);

    var deterministic_dense = embedder_mod.DeterministicDenseEmbedder{};
    var deterministic_sparse = embedder_mod.DeterministicSparseEmbedder{};
    var db = try DB.open(alloc, std.mem.span(path), .{
        .enrichment = .{
            .owner_id = "worker-a",
            .dense_embedder = deterministic_dense.interface(),
            .sparse_embedder = deterministic_sparse.interface(),
        },
    });
    defer db.close();

    try db.addEnrichment(.{
        .name = "document_units_v1",
        .kind = .asset,
        .field = "url",
        .content_type = "application/json",
        .producer_json = "{\"type\":\"document_extraction\",\"config\":{}}",
    });
    try db.addEnrichment(.{
        .name = "document_chunks_v1",
        .kind = .chunk,
        .field = "text",
        .source_artifact_name = "document_units_v1",
        .chunk_size = 256,
    });
    try db.addEnrichment(.{
        .name = "document_chunk_dense_v1",
        .kind = .embedding,
        .field = "text",
        .source_artifact_name = "document_chunks_v1",
        .expected_dims = 3,
    });
    try db.addEnrichment(.{
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
        .name = "dv_document_chunks",
        .kind = .dense_vector,
        .config_json = "{\"field\":\"embedding\",\"dims\":3,\"embedding_name\":\"document_chunk_dense_v1\"}",
    });
    try db.addIndex(.{
        .name = "document_chunk_sparse_v1",
        .kind = .sparse_vector,
        .config_json = "{\"field\":\"sparse_embedding\"}",
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
    try std.testing.expectEqualStrings("dv_document_chunks", dense_consumers[0]);
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
        "{\"url\":\"data:text/plain;base64,YWxwaGEgYmV0YSBnYW1tYQ==\"}",
        &.{},
        &.{},
    );
    defer enrichment_types.deinitGeneratedRequests(alloc, planned);
    var saw_document_asset = false;
    var saw_document_chunk_dense = false;
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
        if (request.kind == .sparse_embedding and
            std.mem.eql(u8, request.artifact_name, "document_chunks_v1") and
            std.mem.eql(u8, request.embedding_name, "document_chunk_sparse_v1") and
            request.chunk_size == 256)
        {
            saw_document_chunk_sparse = true;
        }
    }
    try std.testing.expect(saw_document_asset);
    try std.testing.expect(!saw_document_chunk_dense);
    try std.testing.expect(!saw_document_chunk_sparse);

    try db.batch(.{
        .writes = &.{.{
            .key = "doc:a",
            .value = "{\"url\":\"data:text/plain;base64,YWxwaGEgYmV0YSBnYW1tYQ==\"}",
        }},
        .sync_level = .full_index,
    });

    const unit_key = try internal_keys.documentUnitArtifactKeyAlloc(alloc, "doc:a", "document_units_v1", "document:000001");
    defer alloc.free(unit_key);
    const chunk_key = try internal_keys.documentUnitChunkArtifactKeyAlloc(alloc, "doc:a", "document_chunks_v1", "document:000001", 0);
    defer alloc.free(chunk_key);
    const state_key = try assetStateKeyAlloc(alloc, "doc:a", "document_units_v1");
    defer alloc.free(state_key);
    const manifest_key = try internal_keys.artifactNamedPrefixAlloc(alloc, "doc:a", "asset", "document_units_v1");
    defer alloc.free(manifest_key);

    const chunk_payload = try db.core.store.get(alloc, chunk_key);
    defer alloc.free(chunk_payload);
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
    const chunk_provenance = parsed_chunk_payload.value.object.get("provenance").?.object;
    try std.testing.expectEqualStrings("unit", chunk_provenance.get("offset_basis").?.string);
    try std.testing.expectEqualStrings("document:000001", chunk_provenance.get("parent_unit_id").?.string);
    try std.testing.expectEqualStrings("document_units_v1", chunk_provenance.get("source_artifact_name").?.string);
    try std.testing.expectEqual(@as(i64, 0), chunk_provenance.get("unit_char_start").?.integer);
    try std.testing.expectEqual(@as(i64, 16), chunk_provenance.get("unit_char_end").?.integer);
    try std.testing.expectEqual(@as(i64, 0), chunk_provenance.get("document_char_start").?.integer);
    try std.testing.expectEqual(@as(i64, 16), chunk_provenance.get("document_char_end").?.integer);

    const dense_artifact_key = try internal_keys.derivedEmbeddingArtifactKeyAlloc(alloc, chunk_key, "document_chunk_dense_v1");
    defer alloc.free(dense_artifact_key);
    const dense_artifact_payload = try db.core.store.get(alloc, dense_artifact_key);
    defer alloc.free(dense_artifact_payload);
    const sparse_artifact_key = try internal_keys.derivedEmbeddingArtifactKeyAlloc(alloc, chunk_key, "document_chunk_sparse_v1");
    defer alloc.free(sparse_artifact_key);
    const sparse_artifact_payload = try db.core.store.get(alloc, sparse_artifact_key);
    defer alloc.free(sparse_artifact_payload);

    const state = try db.core.store.get(alloc, state_key);
    defer alloc.free(state);
    try std.testing.expect(std.mem.indexOf(u8, state, "\"chunk_keys\"") != null);
    try std.testing.expect(std.mem.indexOf(u8, state, "\"unit_descriptors\"") != null);
    try std.testing.expect(std.mem.indexOf(u8, state, "\"fingerprint\"") != null);

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
    var chunk_public_id_for_rollup = try artifact_ids.resolvePublicHitIdentityAlloc(alloc, chunk_key);
    defer chunk_public_id_for_rollup.deinit(alloc);
    try std.testing.expectEqualStrings(chunk_public_id_for_rollup.id, parent_with_chunks.hits[0].chunk_hits[0].id);
    const rollup_chunk_ref = parent_with_chunks.hits[0].chunk_hits[0].artifact_ref orelse return error.TestUnexpectedResult;
    try std.testing.expectEqualStrings("doc:a", rollup_chunk_ref.document_id);
    try std.testing.expectEqualStrings("document_chunks_v1", rollup_chunk_ref.name);
    try std.testing.expectEqual(types.ArtifactKind.chunk, rollup_chunk_ref.kind);
    try std.testing.expectEqual(@as(?u32, 0), rollup_chunk_ref.chunk_id);
    try std.testing.expectEqualStrings("document:000001", rollup_chunk_ref.unit_id.?);

    const query_vec = try deterministic_dense.interface().embedDense(alloc, "document_chunk_dense_v1", "alpha beta gamma", 3);
    defer alloc.free(query_vec);
    const dense_index = db.core.index_manager.denseIndex("dv_document_chunks") orelse return error.IndexNotFound;
    var direct = try waitForDenseIndexResultsWithAttempts(&dense_index.index, query_vec, 3, 1, slow_test_wait_attempts);
    defer direct.deinit();
    const dense_internal_id = if (direct.takeMetadata(0)) |metadata|
        metadata
    else blk: {
        const hit = direct.getHits()[0];
        break :blk (try dense_index.index.getMetadata(hit.vector_id)) orelse return error.TestUnexpectedResult;
    };
    defer alloc.free(dense_internal_id);
    try std.testing.expectEqualStrings(chunk_key, dense_internal_id);

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
    var chunk_public_id = try artifact_ids.resolvePublicHitIdentityAlloc(alloc, chunk_key);
    defer chunk_public_id.deinit(alloc);
    try std.testing.expectEqualStrings(chunk_public_id.id, sparse_result.hits[0].id);
    try std.testing.expect(sparse_result.hits[0].stored_data == null);
    try std.testing.expect(sparse_result.hits[0].ancestor_source_data != null);
    try std.testing.expect(std.mem.indexOf(u8, sparse_result.hits[0].ancestor_source_data.?, "\"url\"") != null);
    try std.testing.expect(sparse_result.hits[0].ancestor_unit_data != null);
    try std.testing.expect(std.mem.indexOf(u8, sparse_result.hits[0].ancestor_unit_data.?, "\"unit_id\":\"document:000001\"") != null);
    try std.testing.expect(std.mem.indexOf(u8, sparse_result.hits[0].ancestor_unit_data.?, "\"text\":\"alpha beta gamma\"") != null);

    try db.batch(.{
        .writes = &.{.{
            .key = "doc:a",
            .value = "{\"url\":\"data:text/plain;base64,YWxwaGEgYmV0YSBkZWx0YQ==\"}",
        }},
        .sync_level = .full_index,
    });
    const updated_manifest = try db.core.store.get(alloc, manifest_key);
    defer alloc.free(updated_manifest);
    try std.testing.expect(std.mem.indexOf(u8, updated_manifest, "\"generation\":2") != null);
    try std.testing.expect(std.mem.indexOf(u8, updated_manifest, "\"from_generation\":1") != null);
    try std.testing.expect(std.mem.indexOf(u8, updated_manifest, "\"to_generation\":2") != null);
    const updated_chunk_payload = try db.core.store.get(alloc, chunk_key);
    defer alloc.free(updated_chunk_payload);
    try std.testing.expect(std.mem.indexOf(u8, updated_chunk_payload, "\"text\":\"alpha beta delta\"") != null);

    try db.batch(.{
        .writes = &.{.{
            .key = "doc:a",
            .value = "{\"url\":\"\"}",
        }},
        .sync_level = .full_index,
    });
    try std.testing.expectError(error.NotFound, db.core.store.get(alloc, unit_key));
    try std.testing.expectError(error.NotFound, db.core.store.get(alloc, chunk_key));
}

test "db extractEnrichments exposes cleaned writes and special fields" {
    const alloc = std.testing.allocator;

    var path_buf: [256]u8 = undefined;
    const path = tempPath(&path_buf);
    defer cleanupTempDir(path);

    var db = try DB.open(alloc, std.mem.span(path), .{});
    defer db.close();

    var result = try db.extractEnrichments(alloc, &.{
        .{
            .key = "doc:a",
            .value = "{\"title\":\"alpha\",\"_embeddings\":{\"dense_idx\":[1,2,3],\"sparse_idx\":{\"indices\":[1,5],\"values\":[0.5,0.75]}},\"_edges\":{\"graph_v1\":{\"cites\":[{\"target\":\"doc:b\",\"weight\":2.0}]}}}",
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
}

test "db extractEnrichments projects configured embedded json vector and graph fields" {
    const alloc = std.testing.allocator;

    var path_buf: [256]u8 = undefined;
    const path = tempPath(&path_buf);
    defer cleanupTempDir(path);

    var db = try DB.open(alloc, std.mem.span(path), .{});
    defer db.close();

    try db.addIndex(.{
        .name = "attrs_dense",
        .kind = .dense_vector,
        .config_json = "{\"field\":\"attrs.embedding\",\"dims\":3,\"metric\":\"cosine\"}",
    });
    try db.addIndex(.{
        .name = "attrs_sparse",
        .kind = .sparse_vector,
        .config_json = "{\"field\":\"attrs.sparse\"}",
    });
    try db.addIndex(.{
        .name = "attrs_graph",
        .kind = .graph,
        .config_json = "{\"edge_types\":[{\"name\":\"cites\",\"field\":\"attrs.links\"}]}",
    });

    var result = try db.extractEnrichments(alloc, &.{
        .{
            .key = "doc:a",
            .value =
            \\{"title":"alpha","attrs":{"embedding":[1,0,0],"sparse":{"indices":[7,42],"values":[1.5,0.5]},"links":["doc:b","doc:c"]}}
            ,
        },
    });
    defer result.deinit(alloc);

    try std.testing.expectEqual(@as(usize, 1), result.cleaned_writes.len);
    try std.testing.expectEqual(@as(usize, 1), result.dense_embeddings.len);
    try std.testing.expectEqualStrings("attrs_dense", result.dense_embeddings[0].index_name);
    try std.testing.expectEqual(@as(usize, 3), result.dense_embeddings[0].vector.len);
    try std.testing.expectApproxEqAbs(@as(f32, 1.0), result.dense_embeddings[0].vector[0], 0.0001);

    try std.testing.expectEqual(@as(usize, 1), result.sparse_embeddings.len);
    try std.testing.expectEqualStrings("attrs_sparse", result.sparse_embeddings[0].index_name);
    try std.testing.expectEqual(@as(usize, 2), result.sparse_embeddings[0].indices.len);
    try std.testing.expectEqual(@as(u32, 7), result.sparse_embeddings[0].indices[0]);

    try std.testing.expectEqual(@as(usize, 2), result.graph_writes.len);
    try std.testing.expectEqualStrings("attrs_graph", result.graph_writes[0].index_name);
    try std.testing.expectEqualStrings("cites", result.graph_writes[0].edge_type);
    try std.testing.expectEqualStrings("doc:b", result.graph_writes[0].target);
    try std.testing.expectEqualStrings("doc:c", result.graph_writes[1].target);
}

test "db extractEnrichments rejects unsupported legacy summaries field" {
    const alloc = std.testing.allocator;

    var path_buf: [256]u8 = undefined;
    const path = tempPath(&path_buf);
    defer cleanupTempDir(path);

    var db = try DB.open(alloc, std.mem.span(path), .{});
    defer db.close();

    try std.testing.expectError(error.UnsupportedReservedField, db.extractEnrichments(alloc, &.{
        .{
            .key = "doc:a",
            .value = "{\"title\":\"alpha\",\"_summaries\":{\"sum_idx\":\"brief\"}}",
        },
    }));
}

test "db relational enrichment sources read committed base rows" {
    const alloc = std.testing.allocator;

    var path_buf: [256]u8 = undefined;
    const path = tempPath(&path_buf);
    defer cleanupTempDir(path);

    var deterministic = embedder_mod.DeterministicDenseEmbedder{};
    var db = try DB.open(alloc, std.mem.span(path), .{
        .enrichment = .{
            .owner_id = "worker-a",
            .dense_embedder = deterministic.interface(),
        },
    });
    defer db.close();

    const schema_json =
        \\{"version":1,"storage_mode":"relational","default_type":"row","enforce_types":true,"document_schemas":{"row":{"schema":{"type":"object","properties":{"title":{"type":"text"},"body":{"type":"text"}},"required":["title","body"],"additionalProperties":false}}}}
    ;
    var parsed_schema = try schema_api_mod.parseValidatedTableSchema(alloc, schema_json);
    defer parsed_schema.deinit(alloc);
    const runtime_schema = try schema_api_mod.deriveRuntimeTableSchema(alloc, parsed_schema);
    defer schema_mod.freeSchema(alloc, runtime_schema);
    try db.setSchema(runtime_schema);

    try db.addIndex(.{
        .name = "dv_v1",
        .kind = .dense_vector,
        .config_json = "{\"field\":\"embedding\",\"dims\":3,\"generator\":{\"kind\":\"dense_embedding\",\"source_field\":\"body\",\"embedding_name\":\"body_dense_v1\"}}",
    });

    try db.batch(.{
        .writes = &.{.{
            .key = "row:a",
            .value = "{\"title\":\"alpha\",\"body\":\"generated relational vector text\"}",
        }},
        .sync_level = .write,
    });
    try db.enrichment_runtime.?.waitForApplied(1);
    try db.executor.waitForAll(2);

    const primary_key = try internal_keys.documentKeyAlloc(alloc, "row:a");
    defer alloc.free(primary_key);
    const maybe_primary = db.core.store.get(alloc, primary_key) catch |err| switch (err) {
        error.NotFound => null,
        else => return err,
    };
    if (maybe_primary) |primary_value| {
        defer alloc.free(primary_value);
        return error.TestExpectedEqual;
    }

    const relational_key = try relational_store_mod.rowKeyAlloc(alloc, "row:a");
    defer alloc.free(relational_key);
    const raw_row = try db.core.store.get(alloc, relational_key);
    defer alloc.free(raw_row);
    try std.testing.expect(mapper.isRelationalRowValue(raw_row));

    const query_vec = try deterministic.interface().embedDense(alloc, "", "generated relational vector text", 3);
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
    try std.testing.expectEqualStrings("row:a", result.hits[0].id);
}

test "db relational dense HBC loader reads committed base rows" {
    const alloc = std.testing.allocator;

    var path_buf: [256]u8 = undefined;
    const path = tempPath(&path_buf);
    defer cleanupTempDir(path);

    var db = try DB.open(alloc, std.mem.span(path), .{});
    defer db.close();

    const schema_json =
        \\{"version":1,"storage_mode":"relational","default_type":"row","enforce_types":true,"document_schemas":{"row":{"schema":{"type":"object","properties":{"title":{"type":"text"},"embedding":{"type":"array"}},"required":["title","embedding"],"additionalProperties":false}}}}
    ;
    var parsed_schema = try schema_api_mod.parseValidatedTableSchema(alloc, schema_json);
    defer parsed_schema.deinit(alloc);
    const runtime_schema = try schema_api_mod.deriveRuntimeTableSchema(alloc, parsed_schema);
    defer schema_mod.freeSchema(alloc, runtime_schema);
    try db.setSchema(runtime_schema);

    try db.addIndex(.{
        .name = "dv_v1",
        .kind = .dense_vector,
        .config_json = "{\"field\":\"embedding\",\"dims\":3,\"metric\":\"l2_squared\"}",
    });

    try db.batch(.{
        .writes = &.{.{
            .key = "row:a",
            .value = "{\"title\":\"alpha\",\"embedding\":[1,0,0]}",
        }},
        .sync_level = .full_index,
    });

    const artifact_key = try internal_keys.embeddingArtifactKeyForDocumentAlloc(alloc, "row:a", "dv_v1");
    defer alloc.free(artifact_key);
    try db.core.store.delete(artifact_key);
    db.clearDenseHbcCaches();

    const primary_key = try internal_keys.documentKeyAlloc(alloc, "row:a");
    defer alloc.free(primary_key);
    const maybe_primary = db.core.store.get(alloc, primary_key) catch |err| switch (err) {
        error.NotFound => null,
        else => return err,
    };
    if (maybe_primary) |primary_value| {
        defer alloc.free(primary_value);
        return error.TestExpectedEqual;
    }

    var result = try db.search(alloc, .{
        .index_name = "dv_v1",
        .dense = .{
            .vector = &[_]f32{ 1, 0, 0 },
            .k = 1,
        },
    });
    defer result.deinit();
    try std.testing.expectEqual(@as(u32, 1), result.total_hits);
    try std.testing.expectEqualStrings("row:a", result.hits[0].id);
}

test "db managed dense enrichment remains searchable after transient rate limits" {
    const alloc = std.testing.allocator;

    var path_buf: [256]u8 = undefined;
    const path = tempPath(&path_buf);
    defer cleanupTempDir(path);

    var gated = GateDenseEmbedder{};
    var db = try DB.open(alloc, std.mem.span(path), .{
        .enrichment = .{
            .owner_id = "worker-a",
            .dense_embedder = gated.interface(),
        },
    });
    defer db.close();

    try db.addIndex(.{
        .name = "semantic_idx",
        .kind = .dense_vector,
        .config_json = "{\"field\":\"embedding\",\"dims\":3,\"metric\":\"cosine\",\"generator\":{\"kind\":\"dense_embedding\",\"source_field\":\"body\",\"embedding_name\":\"semantic_idx\"}}",
    });

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
        if (snapshot.rate_limited_requests > 0 and snapshot.successful_requests >= 1) break;
        sleepNs(10 * std.time.ns_per_ms);
    }

    const before_release = gated.snapshot();
    try std.testing.expect(before_release.rate_limited_requests > 0);
    try std.testing.expect(before_release.successful_requests >= 1);

    gated.allowAll();

    var ready = false;
    var attempts_after_release: usize = 0;
    while (attempts_after_release < 500) : (attempts_after_release += 1) {
        const stats = try db.stats(alloc);
        defer types.freeDBStats(alloc, stats);
        if (stats.indexes[0].doc_count == 3 and
            stats.indexes[0].replay_applied_sequence >= 2 and
            stats.indexes[0].replay_applied_sequence == stats.indexes[0].replay_target_sequence and
            !stats.indexes[0].backfill_active)
        {
            ready = true;
            break;
        }
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

test "db open quarantines dense index with unsupported artifact version" {
    const alloc = std.testing.allocator;

    var path_buf: [256]u8 = undefined;
    const path = tempPath(&path_buf);
    defer cleanupTempDir(path);

    const dense_cfg: types.IndexConfig = .{
        .name = "dv_quarantine",
        .kind = .dense_vector,
        .config_json = "{\"field\":\"embedding\",\"dims\":3,\"generator\":{\"kind\":\"dense_embedding\",\"source_field\":\"body\",\"embedding_name\":\"dv_quarantine\"}}",
    };

    var deterministic = embedder_mod.DeterministicDenseEmbedder{};
    const open_options: OpenOptions = .{
        .enrichment = .{
            .owner_id = "quarantine-worker",
            .dense_embedder = deterministic.interface(),
        },
    };

    {
        var db = try DB.open(alloc, std.mem.span(path), open_options);
        defer db.close();

        try db.addIndex(.{ .name = "ft_v1", .kind = .full_text, .config_json = "{}" });
        try db.addIndex(dense_cfg);
        try db.batch(.{
            .writes = &.{
                .{ .key = "doc:a", .value = "{\"title\":\"alpha\",\"body\":\"alpha concept overview\"}" },
            },
            .sync_level = .write,
        });
        try db.runUntilIdle();

        // Persist an HBC metadata record with a version this build does not
        // support, simulating an artifact written by an incompatible build.
        const entry = db.core.denseIndex("dv_quarantine") orelse return error.TestUnexpectedResult;
        var bad_meta = entry.index.metadata;
        bad_meta.version = 99;
        var meta_buf: [vectorindex_mod.hbc.IndexMetadata.encoded_size]u8 = undefined;
        const encoded = bad_meta.encode(&meta_buf);
        var txn = try entry.index.beginWriteTxn();
        errdefer txn.abort();
        try txn.put(.meta, vectorindex_mod.hbc.meta_key, encoded);
        try txn.commit();
    }

    var db = try DB.open(alloc, std.mem.span(path), open_options);
    defer db.close();

    // The broken dense index is quarantined: absent from the runtime, its
    // load error recorded, and the rest of the table fully usable.
    try std.testing.expect(db.core.denseIndex("dv_quarantine") == null);
    const recorded = db.core.index_manager.loadFailure("dv_quarantine") orelse return error.TestUnexpectedResult;
    try std.testing.expectEqualStrings("UnsupportedVersion", recorded);

    var ft_result = try db.search(alloc, .{
        .index_name = "ft_v1",
        .full_text = .{ .match = .{ .field = "title", .text = "alpha" } },
    });
    defer ft_result.deinit();
    try std.testing.expectEqual(@as(u32, 1), ft_result.total_hits);

    {
        const stats = try db.stats(alloc);
        defer types.freeDBStats(alloc, stats);
        var found = false;
        for (stats.indexes) |item| {
            if (!std.mem.eql(u8, item.name, "dv_quarantine")) continue;
            found = true;
            const load_error = item.load_error orelse return error.TestUnexpectedResult;
            try std.testing.expectEqualStrings("UnsupportedVersion", load_error);
            try std.testing.expect(!item.backfill_active);
        }
        try std.testing.expect(found);
    }

    // Queries that explicitly reference the quarantined index get a distinct
    // error instead of IndexNotFound or a stall.
    const query_vec = [_]f32{ 0.1, 0.2, 0.3 };
    try std.testing.expectError(error.IndexUnavailable, db.search(alloc, .{
        .index_name = "dv_quarantine",
        .dense = .{
            .vector = &query_vec,
            .k = 1,
        },
        .limit = 1,
    }));

    // Drop + recreate recovers and clears the recorded failure.
    try std.testing.expect(try db.deleteIndex("dv_quarantine"));
    try std.testing.expect(db.core.index_manager.loadFailure("dv_quarantine") == null);
    try db.addIndex(dense_cfg);
    try db.runUntilIdle();
    try std.testing.expect(db.core.denseIndex("dv_quarantine") != null);

    const recovered_vec = try deterministic.interface().embedDense(alloc, "dv_quarantine", "alpha concept overview", 3);
    defer alloc.free(recovered_vec);
    var recovered = try waitForSearchResult(alloc, &db, .{
        .index_name = "dv_quarantine",
        .dense = .{
            .vector = recovered_vec,
            .k = 1,
        },
        .limit = 1,
    }, 1);
    defer recovered.deinit();
    try std.testing.expect(recovered.total_hits >= 1);
}

test "db drops quarantined dense index after persisted index directory corruption" {
    const alloc = std.testing.allocator;

    var path_buf: [256]u8 = undefined;
    const path = tempPath(&path_buf);
    defer cleanupTempDir(path);
    const path_slice = std.mem.span(path);

    const dense_cfg: types.IndexConfig = .{
        .name = "dv_corrupt",
        .kind = .dense_vector,
        .config_json = "{\"field\":\"embedding\",\"dims\":3,\"metric\":\"cosine\",\"generator\":{\"kind\":\"dense_embedding\",\"source_field\":\"body\",\"embedding_name\":\"dv_corrupt\"}}",
    };

    var deterministic = embedder_mod.DeterministicDenseEmbedder{};
    const open_options: OpenOptions = .{
        .enrichment = .{
            .owner_id = "corrupt-worker",
            .dense_embedder = deterministic.interface(),
        },
    };

    {
        var db = try DB.open(alloc, path_slice, open_options);
        defer db.close();

        try db.addIndex(dense_cfg);
        try db.batch(.{
            .writes = &.{
                .{ .key = "doc:a", .value = "{\"body\":\"alpha concept overview\"}" },
            },
            .sync_level = .write,
        });
        try db.runUntilIdle();
        try std.testing.expect(db.core.denseIndex("dv_corrupt") != null);
    }

    const index_path = try std.fmt.allocPrint(alloc, "{s}/indexes/dv_corrupt", .{path_slice});
    defer alloc.free(index_path);
    try std.testing.expect((try corruptNonEmptyFilesUnderDir(alloc, index_path)) > 0);

    var db = try DB.open(alloc, path_slice, open_options);
    defer db.close();

    try std.testing.expect(db.core.denseIndex("dv_corrupt") == null);
    _ = db.core.index_manager.loadFailure("dv_corrupt") orelse return error.TestUnexpectedResult;

    try std.testing.expect(try db.deleteIndex("dv_corrupt"));
    try std.testing.expect(db.core.index_manager.loadFailure("dv_corrupt") == null);
    try std.testing.expect(db.core.index_manager.get("dv_corrupt") == null);

    var io_impl = threadedIo();
    defer io_impl.deinit();
    try std.testing.expectError(error.FileNotFound, std.Io.Dir.cwd().statFile(io_impl.io(), index_path, .{}));
}

test "db quarantined index self-heals via retryQuarantinedIndexLoads" {
    const alloc = std.testing.allocator;

    var path_buf: [256]u8 = undefined;
    const path = tempPath(&path_buf);
    defer cleanupTempDir(path);

    {
        var db = try DB.open(alloc, std.mem.span(path), .{});
        defer db.close();

        var writes = std.ArrayListUnmanaged(types.BatchWrite).empty;
        defer {
            for (writes.items) |item| {
                alloc.free(@constCast(item.key));
                alloc.free(@constCast(item.value));
            }
            writes.deinit(alloc);
        }
        for (0..300) |i| {
            try writes.append(alloc, .{
                .key = try std.fmt.allocPrint(alloc, "doc:{d:0>4}", .{i}),
                .value = try std.fmt.allocPrint(alloc, "{{\"title\":\"alpha\",\"n\":{d}}}", .{i}),
            });
        }
        try db.batch(.{ .writes = writes.items });
        try db.core.index_manager.addAllNoBackfill(db.core.store, &.{.{
            .name = "ft_v1",
            .kind = .full_text,
            .config_json = "{}",
        }});
    }

    index_manager_mod.test_abort_text_backfill_after_batches = 1;
    defer index_manager_mod.test_abort_text_backfill_after_batches = null;

    var db = try DB.open(alloc, std.mem.span(path), .{});
    defer db.close();
    try std.testing.expect(db.core.index_manager.loadFailure("ft_v1") != null);
    try std.testing.expect(db.core.textIndexEntry("ft_v1") == null);

    // Writes keep flowing while the index is quarantined; the heal's resumed
    // backfill must pick them up. They also guarantee the next backfill
    // attempt flushes at least once, so the still-set abort knob fires.
    {
        var writes = std.ArrayListUnmanaged(types.BatchWrite).empty;
        defer {
            for (writes.items) |item| {
                alloc.free(@constCast(item.key));
                alloc.free(@constCast(item.value));
            }
            writes.deinit(alloc);
        }
        for (300..400) |i| {
            try writes.append(alloc, .{
                .key = try std.fmt.allocPrint(alloc, "doc:{d:0>4}", .{i}),
                .value = try std.fmt.allocPrint(alloc, "{{\"title\":\"alpha\",\"n\":{d}}}", .{i}),
            });
        }
        try db.batch(.{ .writes = writes.items });
    }

    // While the cause persists, a forced retry fails and applies backoff.
    {
        const result = try db.retryQuarantinedIndexLoads(true);
        try std.testing.expectEqual(@as(usize, 0), result.recovered);
        try std.testing.expectEqual(@as(usize, 1), result.remaining);
    }
    // Cause fixed — but the failed attempt set a backoff deadline, so a
    // non-forced retry is skipped without re-attempting the open (it would
    // recover if it attempted).
    index_manager_mod.test_abort_text_backfill_after_batches = null;
    {
        const result = try db.retryQuarantinedIndexLoads(false);
        try std.testing.expectEqual(@as(usize, 0), result.recovered);
        try std.testing.expectEqual(@as(usize, 1), result.remaining);
    }

    // Forced retry recovers the index in-process — no reopen, no
    // drop+recreate — resuming the persisted rebuild state.
    const result = try db.retryQuarantinedIndexLoads(true);
    try std.testing.expectEqual(@as(usize, 1), result.recovered);
    try std.testing.expectEqual(@as(usize, 0), result.remaining);
    try std.testing.expect(db.core.index_manager.loadFailure("ft_v1") == null);
    try std.testing.expect(db.core.textIndexEntry("ft_v1") != null);

    try db.runUntilIdle();
    var search_result = try db.search(alloc, .{
        .index_name = "ft_v1",
        .query = .{ .match_all = {} },
        .limit = 500,
    });
    defer search_result.deinit();
    try std.testing.expectEqual(@as(u32, 400), search_result.total_hits);
}

test "db read-only open propagates transient index load errors instead of quarantining" {
    const alloc = std.testing.allocator;

    var path_buf: [256]u8 = undefined;
    const path = tempPath(&path_buf);
    defer cleanupTempDir(path);

    {
        var db = try DB.open(alloc, std.mem.span(path), .{});
        defer db.close();
        try db.addIndex(.{ .name = "ft_v1", .kind = .full_text, .config_json = "{}" });
    }

    index_manager_mod.test_inject_index_open_error = error.FileNotFound;
    defer index_manager_mod.test_inject_index_open_error = null;

    // A read-only (replica) open must NOT quarantine a transient read race
    // (e.g. the writer reclaiming obsolete LSM runs mid-open): the error
    // propagates so the query layer reopens against a fresh manifest and
    // retries. A quarantined replica would instead serve IndexUnavailable
    // until the read cache next invalidates it — the quarantine retry
    // worker only runs on the writer.
    try std.testing.expectError(error.FileNotFound, DB.open(alloc, std.mem.span(path), .{
        .open_mode = .query_readonly,
    }));

    // The writer open quarantines the same error: the writer cannot race
    // its own (not yet started) reclaim, so a missing artifact there is
    // persistent damage — and the in-process retry recovers it once the
    // cause clears.
    var db = try DB.open(alloc, std.mem.span(path), .{});
    defer db.close();
    const recorded = db.core.index_manager.loadFailure("ft_v1") orelse return error.TestUnexpectedResult;
    try std.testing.expectEqualStrings("FileNotFound", recorded);

    index_manager_mod.test_inject_index_open_error = null;
    const result = try db.retryQuarantinedIndexLoads(true);
    try std.testing.expectEqual(@as(usize, 1), result.recovered);
    try std.testing.expectEqual(@as(usize, 0), result.remaining);
    try std.testing.expect(db.core.index_manager.loadFailure("ft_v1") == null);
}

test "db managed dense enrichment delete recreate recovers after corrupt artifact" {
    const alloc = std.testing.allocator;

    var path_buf: [256]u8 = undefined;
    const path = tempPath(&path_buf);
    defer cleanupTempDir(path);

    var counting = CountingDenseEmbedder{};
    var db = try DB.open(alloc, std.mem.span(path), .{
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

    {
        const stats = try db.stats(alloc);
        defer types.freeDBStats(alloc, stats);
        try std.testing.expectEqual(@as(?u64, 2), stats.indexes[0].doc_count);
    }

    const artifact_key = try internal_keys.embeddingArtifactKeyForDocumentAlloc(alloc, "doc:a", "semantic_idx");
    defer alloc.free(artifact_key);
    try db.core.store.put(artifact_key, "bad-artifact");

    try std.testing.expect(try db.deleteIndex("semantic_idx"));
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

    var path_buf: [256]u8 = undefined;
    const path = tempPath(&path_buf);
    defer cleanupTempDir(path);

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

        const artifact_key = try internal_keys.embeddingArtifactKeyForDocumentAlloc(alloc, "doc:a", "semantic_idx");
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

test "db dense enrichment skips unchanged source hash" {
    const alloc = std.testing.allocator;

    var path_buf: [256]u8 = undefined;
    const path = tempPath(&path_buf);
    defer cleanupTempDir(path);

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

    try db.batch(.{
        .writes = &.{.{ .key = "doc:a", .value = "{\"title\":\"changed\",\"body\":\"same source\"}" }},
        .sync_level = .write,
    });
    try db.runUntilIdle();
    try std.testing.expectEqual(@as(usize, 1), counting.calls);

    try db.batch(.{
        .writes = &.{.{ .key = "doc:a", .value = "{\"title\":\"changed\",\"body\":\"new source\"}" }},
        .sync_level = .write,
    });
    try db.runUntilIdle();
    try std.testing.expectEqual(@as(usize, 2), counting.calls);

    const artifact_key = try internal_keys.embeddingArtifactKeyForDocumentAlloc(alloc, "doc:a", "body_dense_v1");
    defer alloc.free(artifact_key);
    const artifacts = try db.core.store.scanPrefix(alloc, artifact_key);
    defer docstore_mod.DocStore.freeResults(alloc, artifacts);
    try std.testing.expectEqual(@as(usize, 1), artifacts.len);
    try enrichment_artifact_codec.expectDenseEmbeddingValue(alloc, artifacts[0].value, enrichment_artifact_codec.hashSource("new source"), 3);

    const stats = try db.stats(alloc);
    defer types.freeDBStats(alloc, stats);
    try std.testing.expectEqual(@as(u64, 1), stats.enrichment.skip_by_hash_count);
    try std.testing.expectEqual(@as(u64, 0), stats.enrichment.codec_decode_failures);
    try std.testing.expect(stats.enrichment.dense_artifact_bytes_written >= @as(u64, @intCast(artifacts[0].value.len * 2)));
    try std.testing.expectEqual(stats.enrichment.dense_artifact_bytes_written, stats.enrichment.artifact_bytes_written);
}

test "db dense enrichment republishes unchanged source hash from cached artifact after index reset" {
    const alloc = std.testing.allocator;

    var path_buf: [256]u8 = undefined;
    const path = tempPath(&path_buf);
    defer cleanupTempDir(path);

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

    try db.core.index_manager.resetDenseIndexForArtifactRebuild("dv_v1");
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

test "db chunked dense enrichment skips unchanged chunks and deletes stale chunk artifacts" {
    const alloc = std.testing.allocator;

    var path_buf: [256]u8 = undefined;
    const path = tempPath(&path_buf);
    defer cleanupTempDir(path);

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
        .config_json = "{\"field\":\"embedding\",\"dims\":3,\"generator\":{\"kind\":\"dense_embedding\",\"source_field\":\"body\",\"chunk_name\":\"body_chunks_v1\",\"chunk_size\":8,\"chunk_overlap\":2,\"embedding_name\":\"chunk_dense_v1\"}}",
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

    {
        const chunk_prefix = try internal_keys.artifactNamedPrefixAlloc(alloc, "doc:a", "chunk", "body_chunks_v1");
        defer alloc.free(chunk_prefix);
        const artifacts = try db.core.store.scanPrefix(alloc, chunk_prefix);
        defer docstore_mod.DocStore.freeResults(alloc, artifacts);

        var chunk_count: usize = 0;
        var embedding_count: usize = 0;
        for (artifacts) |entry| {
            if (internal_keys.isChunkArtifactRecordKey(entry.key)) chunk_count += 1;
            if (internal_keys.isDerivedEmbeddingArtifactKey(entry.key)) embedding_count += 1;
        }
        try std.testing.expectEqual(@as(usize, 1), chunk_count);
        try std.testing.expectEqual(@as(usize, 1), embedding_count);
        try std.testing.expectEqual(@as(u64, 1), db.core.index_manager.denseIndex("dv_v1").?.index.metadata.active_count);
    }

    try db.batch(.{
        .writes = &.{.{ .key = "doc:a", .value = "{\"title\":\"changed\",\"body\":\"xyzuvw\"}" }},
        .sync_level = .write,
    });
    try db.runUntilIdle();
    try std.testing.expect(counting.calls > first_calls);

    const chunk_prefix = try internal_keys.artifactNamedPrefixAlloc(alloc, "doc:a", "chunk", "body_chunks_v1");
    defer alloc.free(chunk_prefix);
    const artifacts = try db.core.store.scanPrefix(alloc, chunk_prefix);
    defer docstore_mod.DocStore.freeResults(alloc, artifacts);

    var chunk_count: usize = 0;
    var embedding_count: usize = 0;
    for (artifacts) |entry| {
        if (internal_keys.isChunkArtifactRecordKey(entry.key)) {
            chunk_count += 1;
            try std.testing.expect(std.mem.indexOf(u8, entry.value, "\"xyzuvw\"") != null);
        } else if (internal_keys.isDerivedEmbeddingArtifactKey(entry.key)) {
            embedding_count += 1;
            try enrichment_artifact_codec.expectDenseEmbeddingValue(alloc, entry.value, enrichment_artifact_codec.hashSource("xyzuvw"), 3);
        }
    }
    try std.testing.expectEqual(@as(usize, 1), chunk_count);
    try std.testing.expectEqual(@as(usize, 1), embedding_count);
    try std.testing.expectEqual(@as(u64, 1), db.core.index_manager.denseIndex("dv_v1").?.index.metadata.active_count);
}

test "db chunked dense enrichment replays cached artifacts after dense reset without re-embedding" {
    const alloc = std.testing.allocator;

    var path_buf: [256]u8 = undefined;
    const path = tempPath(&path_buf);
    defer cleanupTempDir(path);

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
        .config_json = "{\"field\":\"embedding\",\"dims\":3,\"generator\":{\"kind\":\"dense_embedding\",\"source_field\":\"body\",\"chunk_name\":\"body_chunks_v1\",\"chunk_size\":8,\"chunk_overlap\":2,\"embedding_name\":\"chunk_dense_v1\"}}",
    });

    try db.batch(.{
        .writes = &.{.{ .key = "doc:a", .value = "{\"title\":\"alpha\",\"body\":\"abcdefghijklmno\"}" }},
        .sync_level = .write,
    });
    try db.runUntilIdle();

    const first_calls = counting.calls;
    try std.testing.expect(first_calls > 0);
    try std.testing.expectEqual(@as(u64, 3), db.core.index_manager.denseIndex("dv_v1").?.index.metadata.active_count);

    try db.core.index_manager.resetDenseIndexForArtifactRebuild("dv_v1");
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

test "db reopened chunked dense HBC deletes stale vectors through artifact loader" {
    const alloc = std.testing.allocator;

    var path_buf: [256]u8 = undefined;
    const path = tempPath(&path_buf);
    defer cleanupTempDir(path);

    var counting = CountingDenseEmbedder{};
    {
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
            .config_json = "{\"field\":\"embedding\",\"dims\":3,\"generator\":{\"kind\":\"dense_embedding\",\"source_field\":\"body\",\"chunk_name\":\"body_chunks_v1\",\"chunk_size\":8,\"chunk_overlap\":2,\"embedding_name\":\"chunk_dense_v1\"}}",
        });

        try db.batch(.{
            .writes = &.{.{ .key = "doc:a", .value = "{\"title\":\"alpha\",\"body\":\"abcdefghijklmno\"}" }},
            .sync_level = .write,
        });
        try db.runUntilIdle();
        try std.testing.expectEqual(@as(u64, 3), db.core.index_manager.denseIndex("dv_v1").?.index.metadata.active_count);
    }

    const calls_after_first_open = counting.calls;
    try std.testing.expect(calls_after_first_open > 0);

    {
        var reopened = try DB.open(alloc, std.mem.span(path), .{
            .enrichment = .{
                .owner_id = "worker-a",
                .dense_embedder = counting.interface(),
            },
        });
        defer reopened.close();

        const entry = reopened.core.index_manager.denseIndex("dv_v1") orelse return error.TestUnexpectedResult;
        try std.testing.expect(entry.index.hasExternalVectorLoader());
        try std.testing.expectEqual(@as(u64, 3), entry.index.metadata.active_count);

        try reopened.batch(.{
            .writes = &.{.{ .key = "doc:a", .value = "{\"title\":\"changed\",\"body\":\"abcdefgh\"}" }},
            .sync_level = .write,
        });
        try reopened.runUntilIdle();

        try std.testing.expectEqual(calls_after_first_open, counting.calls);
        try std.testing.expectEqual(@as(u64, 1), reopened.core.index_manager.denseIndex("dv_v1").?.index.metadata.active_count);
    }
}

test "db chunked generated dense and sparse embeddings search as parent results" {
    const alloc = std.testing.allocator;

    var path_buf: [256]u8 = undefined;
    const path = tempPath(&path_buf);
    defer cleanupTempDir(path);

    var deterministic_dense = embedder_mod.DeterministicDenseEmbedder{};
    var deterministic_sparse = embedder_mod.DeterministicSparseEmbedder{};
    var db = try DB.open(alloc, std.mem.span(path), .{
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

    const doc_a_store_key = try internal_keys.documentKeyAlloc(alloc, "doc:a");
    defer alloc.free(doc_a_store_key);
    {
        var txn = try db.core.store.beginWriteTxn();
        errdefer txn.abort();
        try txn.delete(doc_a_store_key);
        try doc_identity.markDeletedTxn(alloc, &txn, 2, "doc:a");
        try txn.commit();
    }
    db.identity_visibility_summary_cache = null;

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

test "db sparse enrichment skips unchanged source hash" {
    const alloc = std.testing.allocator;

    var path_buf: [256]u8 = undefined;
    const path = tempPath(&path_buf);
    defer cleanupTempDir(path);

    var counting = CountingSparseEmbedder{};
    var db = try DB.open(alloc, std.mem.span(path), .{
        .start_index_workers = false,
        .enrichment = .{
            .owner_id = "worker-a",
            .sparse_embedder = counting.interface(),
        },
    });
    defer db.close();

    try db.addIndex(.{
        .name = "sp_v1",
        .kind = .sparse_vector,
        .config_json = "{\"field\":\"sparse_embedding\",\"generator\":{\"kind\":\"sparse_embedding\",\"source_field\":\"body\"}}",
    });

    try db.batch(.{
        .writes = &.{.{ .key = "doc:a", .value = "{\"title\":\"alpha\",\"body\":\"same source\"}" }},
        .sync_level = .write,
    });
    try db.runUntilIdle();
    try std.testing.expectEqual(@as(usize, 1), counting.calls);

    try db.batch(.{
        .writes = &.{.{ .key = "doc:a", .value = "{\"title\":\"changed\",\"body\":\"same source\"}" }},
        .sync_level = .write,
    });
    try db.runUntilIdle();
    try std.testing.expectEqual(@as(usize, 1), counting.calls);

    try db.batch(.{
        .writes = &.{.{ .key = "doc:a", .value = "{\"title\":\"changed\",\"body\":\"new source\"}" }},
        .sync_level = .write,
    });
    try db.runUntilIdle();
    try std.testing.expectEqual(@as(usize, 2), counting.calls);

    const artifact_key = try internal_keys.embeddingArtifactKeyForDocumentAlloc(alloc, "doc:a", "sp_v1");
    defer alloc.free(artifact_key);
    const artifacts = try db.core.store.scanPrefix(alloc, artifact_key);
    defer docstore_mod.DocStore.freeResults(alloc, artifacts);
    try std.testing.expectEqual(@as(usize, 1), artifacts.len);
    try enrichment_artifact_codec.expectSparseEmbeddingValue(alloc, artifacts[0].value, enrichment_artifact_codec.hashSource("new source"), 2);

    const stats = try db.stats(alloc);
    defer types.freeDBStats(alloc, stats);
    try std.testing.expectEqual(@as(u64, 1), stats.enrichment.skip_by_hash_count);
    try std.testing.expectEqual(@as(u64, 0), stats.enrichment.codec_decode_failures);
    try std.testing.expect(stats.enrichment.sparse_artifact_bytes_written >= @as(u64, @intCast(artifacts[0].value.len * 2)));
    try std.testing.expectEqual(stats.enrichment.sparse_artifact_bytes_written, stats.enrichment.artifact_bytes_written);
}

test "db chunked sparse enrichment skips unchanged chunks and deletes stale sparse artifacts" {
    const alloc = std.testing.allocator;

    var path_buf: [256]u8 = undefined;
    const path = tempPath(&path_buf);
    defer cleanupTempDir(path);

    var counting = CountingSparseEmbedder{};
    var db = try DB.open(alloc, std.mem.span(path), .{
        .start_index_workers = false,
        .enrichment = .{
            .owner_id = "worker-a",
            .sparse_embedder = counting.interface(),
        },
    });
    defer db.close();

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
            try enrichment_artifact_codec.expectSparseEmbeddingValue(alloc, entry.value, enrichment_artifact_codec.hashSource("abcdefgh"), 2);
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

    const stale_chunk_id = try artifact_ids.chunkArtifactPublicIdAlloc(alloc, "doc:a", "body_chunks_v1", 1);
    defer alloc.free(stale_chunk_id);
    for (stale_result.hits) |hit| {
        try std.testing.expect(!std.mem.eql(u8, hit.id, stale_chunk_id));
    }
}

test "db runUntilIdle drains enrichment and derived indexing" {
    const alloc = std.testing.allocator;

    var path_buf: [256]u8 = undefined;
    const path = tempPath(&path_buf);
    defer cleanupTempDir(path);

    var deterministic = embedder_mod.DeterministicDenseEmbedder{};
    var db = try DB.open(alloc, std.mem.span(path), .{
        .enrichment = .{
            .owner_id = "embedded-worker",
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

    const pending_before = db.pendingWorkStats();
    try std.testing.expect(pending_before.derived_target_sequence >= 1);

    try db.runUntilIdle();

    const pending_after = db.pendingWorkStats();
    try std.testing.expectEqual(pending_after.derived_target_sequence, pending_after.enrichment.applied_sequence);

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
}

test "db runUntilIdle publishes configured graph pagerank metrics" {
    const alloc = std.testing.allocator;

    var path_buf: [256]u8 = undefined;
    const path = tempPath(&path_buf);
    defer cleanupTempDir(path);

    var db = try DB.open(alloc, std.mem.span(path), .{
        .start_index_workers = false,
        .ttl_cleanup = .{ .enabled = false },
    });
    defer db.close();

    try db.addIndex(.{
        .name = "graph_idx",
        .kind = .graph,
        .config_json = "{\"metrics\":{\"pagerank\":{\"enabled\":true,\"max_iterations\":40,\"tolerance\":0.000001,\"refresh\":\"background\",\"edge_filter\":{\"types\":[\"cites\"]}},\"degree\":{\"enabled\":true,\"kind\":\"degree\",\"refresh\":\"background\",\"edge_filter\":{\"types\":[\"cites\"]}},\"eigenvector\":{\"enabled\":true,\"kind\":\"eigenvector\",\"max_iterations\":20,\"tolerance\":0.000001,\"refresh\":\"background\",\"edge_filter\":{\"types\":[\"cites\"]}},\"hits_authority\":{\"enabled\":true,\"kind\":\"hits_authority\",\"max_iterations\":20,\"tolerance\":0.000001,\"refresh\":\"background\",\"edge_filter\":{\"types\":[\"cites\"]}},\"hits_hub\":{\"enabled\":true,\"kind\":\"hits_hub\",\"max_iterations\":20,\"tolerance\":0.000001,\"refresh\":\"background\",\"edge_filter\":{\"types\":[\"cites\"]}}}}",
    });

    try db.batch(.{
        .writes = &.{
            .{ .key = "doc:a", .value = "{\"title\":\"alpha\",\"_edges\":{\"graph_idx\":{\"cites\":[{\"target\":\"doc:b\",\"weight\":1.0}],\"related\":[{\"target\":\"doc:x\",\"weight\":1.0}]}}}" },
            .{ .key = "doc:c", .value = "{\"title\":\"gamma\",\"_edges\":{\"graph_idx\":{\"cites\":[{\"target\":\"doc:b\",\"weight\":1.0}]}}}" },
            .{ .key = "doc:b", .value = "{\"title\":\"beta\",\"_edges\":{\"graph_idx\":{\"cites\":[{\"target\":\"doc:d\",\"weight\":1.0}]}}}" },
            .{ .key = "doc:d", .value = "{\"title\":\"delta\"}" },
            .{ .key = "doc:x", .value = "{\"title\":\"excluded\"}" },
        },
        .sync_level = .write,
    });

    {
        const graph_entry = db.core.graphIndex("graph_idx") orelse return error.IndexNotFound;
        var status = try graph_entry.index.graphMetricStatus("pagerank");
        defer status.deinit(alloc);
        try std.testing.expectEqual(graph_mod.GraphIndex.GraphMetricState.not_ready, status.state);
        var degree_status = try graph_entry.index.graphMetricStatus("degree");
        defer degree_status.deinit(alloc);
        try std.testing.expectEqual(graph_mod.GraphIndex.GraphMetricState.not_ready, degree_status.state);
        var eigenvector_status = try graph_entry.index.graphMetricStatus("eigenvector");
        defer eigenvector_status.deinit(alloc);
        try std.testing.expectEqual(graph_mod.GraphIndex.GraphMetricState.not_ready, eigenvector_status.state);
        var authority_status = try graph_entry.index.graphMetricStatus("hits_authority");
        defer authority_status.deinit(alloc);
        try std.testing.expectEqual(graph_mod.GraphIndex.GraphMetricState.not_ready, authority_status.state);
        var hub_status = try graph_entry.index.graphMetricStatus("hits_hub");
        defer hub_status.deinit(alloc);
        try std.testing.expectEqual(graph_mod.GraphIndex.GraphMetricState.not_ready, hub_status.state);
    }

    try db.runUntilIdle();

    {
        const graph_entry = db.core.graphIndex("graph_idx") orelse return error.IndexNotFound;
        var status = try graph_entry.index.graphMetricStatus("pagerank");
        defer status.deinit(alloc);
        try std.testing.expectEqual(graph_mod.GraphIndex.GraphMetricState.fresh, status.state);
        try std.testing.expect(status.published_generation > 0);
        try std.testing.expect(status.converged or status.iterations_completed == 40);
        var degree_status = try graph_entry.index.graphMetricStatus("degree");
        defer degree_status.deinit(alloc);
        try std.testing.expectEqual(graph_mod.GraphIndex.GraphMetricState.fresh, degree_status.state);
        try std.testing.expect(degree_status.converged);
        try std.testing.expectEqual(@as(u32, 1), degree_status.iterations_completed);
        var eigenvector_status = try graph_entry.index.graphMetricStatus("eigenvector");
        defer eigenvector_status.deinit(alloc);
        try std.testing.expectEqual(graph_mod.GraphIndex.GraphMetricState.fresh, eigenvector_status.state);
        try std.testing.expect(eigenvector_status.converged or eigenvector_status.iterations_completed == 20);
        var authority_status = try graph_entry.index.graphMetricStatus("hits_authority");
        defer authority_status.deinit(alloc);
        try std.testing.expectEqual(graph_mod.GraphIndex.GraphMetricState.fresh, authority_status.state);
        try std.testing.expect(authority_status.converged or authority_status.iterations_completed == 20);
        var hub_status = try graph_entry.index.graphMetricStatus("hits_hub");
        defer hub_status.deinit(alloc);
        try std.testing.expectEqual(graph_mod.GraphIndex.GraphMetricState.fresh, hub_status.state);
        try std.testing.expect(hub_status.converged or hub_status.iterations_completed == 20);

        const top = try graph_entry.index.graphMetricTopK("pagerank", 10);
        defer {
            for (top) |*score| score.deinit(alloc);
            alloc.free(top);
        }
        try std.testing.expectEqual(@as(usize, 4), top.len);
        for (top) |score| try std.testing.expect(!std.mem.eql(u8, score.node, "doc:x"));

        const degree_top = try graph_entry.index.graphMetricTopK("degree", 10);
        defer {
            for (degree_top) |*score| score.deinit(alloc);
            alloc.free(degree_top);
        }
        try std.testing.expectEqual(@as(usize, 4), degree_top.len);
        try std.testing.expectEqualStrings("doc:b", degree_top[0].node);
        try std.testing.expectApproxEqAbs(@as(f64, 3.0), degree_top[0].score, 0.001);
        for (degree_top) |score| try std.testing.expect(!std.mem.eql(u8, score.node, "doc:x"));

        const eigenvector_top = try graph_entry.index.graphMetricTopK("eigenvector", 10);
        defer {
            for (eigenvector_top) |*score| score.deinit(alloc);
            alloc.free(eigenvector_top);
        }
        try std.testing.expectEqual(@as(usize, 4), eigenvector_top.len);
        for (eigenvector_top) |score| try std.testing.expect(!std.mem.eql(u8, score.node, "doc:x"));

        const authority_top = try graph_entry.index.graphMetricTopK("hits_authority", 10);
        defer {
            for (authority_top) |*score| score.deinit(alloc);
            alloc.free(authority_top);
        }
        try std.testing.expectEqual(@as(usize, 4), authority_top.len);
        for (authority_top) |score| try std.testing.expect(!std.mem.eql(u8, score.node, "doc:x"));

        const hub_top = try graph_entry.index.graphMetricTopK("hits_hub", 10);
        defer {
            for (hub_top) |*score| score.deinit(alloc);
            alloc.free(hub_top);
        }
        try std.testing.expectEqual(@as(usize, 4), hub_top.len);
        for (hub_top) |score| try std.testing.expect(!std.mem.eql(u8, score.node, "doc:x"));
    }

    var metric_result = try db.search(alloc, .{
        .graph_metric_queries = &.{.{
            .name = "pagerank",
            .query = .{
                .index_name = "graph_idx",
                .metric_name = "pagerank",
                .top_k = 2,
                .freshness = .fresh,
            },
        }},
        .limit = 0,
    });
    defer metric_result.deinit();
    try std.testing.expectEqual(@as(usize, 0), metric_result.hits.len);
    try std.testing.expectEqual(@as(usize, 1), metric_result.graph_metric_results.len);
    try std.testing.expectEqual(@as(usize, 2), metric_result.graph_metric_results[0].scores.len);
    try std.testing.expectEqual(graph_mod.GraphIndex.GraphMetricState.fresh, metric_result.graph_metric_results[0].status.state);

    var degree_metric_result = try db.search(alloc, .{
        .graph_metric_queries = &.{.{
            .name = "degree",
            .query = .{
                .index_name = "graph_idx",
                .metric_name = "degree",
                .top_k = 1,
                .freshness = .fresh,
            },
        }},
        .limit = 0,
    });
    defer degree_metric_result.deinit();
    try std.testing.expectEqual(@as(usize, 1), degree_metric_result.graph_metric_results.len);
    try std.testing.expectEqual(@as(usize, 1), degree_metric_result.graph_metric_results[0].scores.len);
    try std.testing.expectEqualStrings("doc:b", degree_metric_result.graph_metric_results[0].scores[0].node);
    try std.testing.expectApproxEqAbs(@as(f64, 3.0), degree_metric_result.graph_metric_results[0].scores[0].score, 0.001);
}

test "db planned scheduler does not auto retry failed graph metric generation" {
    const alloc = std.testing.allocator;

    var path_buf: [256]u8 = undefined;
    const path = tempPath(&path_buf);
    defer cleanupTempDir(path);

    var db = try DB.open(alloc, std.mem.span(path), .{
        .start_index_workers = false,
        .ttl_cleanup = .{ .enabled = false },
        .graph_metric_idle_maintenance = .planned,
    });
    defer db.close();

    try db.addIndex(.{
        .name = "graph_idx",
        .kind = .graph,
        .config_json = "{\"metrics\":{\"pagerank\":{\"enabled\":true,\"kind\":\"pagerank\",\"refresh\":\"background\",\"max_iterations\":2,\"tolerance\":0.000000001,\"edge_filter\":{\"types\":[\"cites\"]}}}}",
    });

    try db.batch(.{
        .writes = &.{
            .{ .key = "doc:a", .value = "{\"title\":\"alpha\",\"_edges\":{\"graph_idx\":{\"cites\":[{\"target\":\"doc:b\",\"weight\":1.0}]}}}" },
            .{ .key = "doc:b", .value = "{\"title\":\"beta\",\"_edges\":{\"graph_idx\":{\"cites\":[{\"target\":\"doc:d\",\"weight\":1.0}]}}}" },
            .{ .key = "doc:c", .value = "{\"title\":\"gamma\",\"_edges\":{\"graph_idx\":{\"cites\":[{\"target\":\"doc:b\",\"weight\":1.0}]}}}" },
            .{ .key = "doc:d", .value = "{\"title\":\"delta\"}" },
        },
        .sync_level = .write,
    });
    try db.runDerivedUntil(db.core.nextDerivedSequence());

    try graph_metric_runtime_mod.expectPlannedAutoIdleDecision(db.core.index_manager, db.graph_metric_idle_auto_options, true, 0, 1, 0, 0);
    {
        const pending = db.pendingWorkStats().graph_metric;
        try std.testing.expect(pending.hasWork());
        try std.testing.expectEqual(@as(usize, 1), pending.queued_builds);
        try std.testing.expectEqual(@as(usize, 0), pending.active_builds);
    }

    const start = try db.runGraphMetricPlannedCoordinatorSweep(.{
        .max_metrics = 8,
        .start_background_builds = true,
    });
    try std.testing.expectEqual(@as(usize, 1), start.builds_started);
    try std.testing.expectEqual(@as(usize, 1), start.active_builds);

    var failed = try db.failGraphMetricPlannedBuild(alloc, "graph_idx", "pagerank", error.InvalidGraphMetricBuildManifest);
    defer failed.deinit(alloc);
    try std.testing.expectEqual(graph_mod.GraphIndex.GraphMetricState.failed, failed.state);
    try std.testing.expect(failed.build_queued);
    try std.testing.expectEqualStrings("InvalidGraphMetricBuildManifest", failed.last_error);
    const failed_target_generation = failed.target_edge_generation;

    try graph_metric_runtime_mod.expectPlannedAutoIdleDecision(db.core.index_manager, db.graph_metric_idle_auto_options, false, 0, 0, 0, 0);
    {
        const pending = db.pendingWorkStats().graph_metric;
        try std.testing.expect(!pending.hasWork());
        try std.testing.expectEqual(@as(usize, 0), pending.queued_builds);
        try std.testing.expectEqual(@as(usize, 0), pending.active_builds);
    }

    const duplicate = try db.runGraphMetricPlannedCoordinatorSweep(.{
        .max_metrics = 8,
        .start_background_builds = true,
    });
    try std.testing.expectEqual(@as(usize, 0), duplicate.builds_started);
    try std.testing.expectEqual(@as(usize, 0), duplicate.active_builds);
    try std.testing.expectEqual(@as(usize, 0), duplicate.coordinator_steps);
    try std.testing.expect(!duplicate.durableProgressed());

    {
        const graph_entry = db.core.graphIndex("graph_idx") orelse return error.IndexNotFound;
        var status = try graph_entry.index.graphMetricStatus("pagerank");
        defer status.deinit(alloc);
        try std.testing.expectEqual(graph_mod.GraphIndex.GraphMetricState.failed, status.state);
        try std.testing.expectEqual(failed_target_generation, status.target_edge_generation);
        try std.testing.expectEqual(@as(u64, 0), status.build_job_id);
        var failed_events: usize = 0;
        for (status.recent_events) |event| {
            if (event.kind == .failed) failed_events += 1;
        }
        try std.testing.expectEqual(@as(usize, 1), failed_events);
    }

    try db.batch(.{
        .writes = &.{.{
            .key = "doc:e",
            .value = "{\"title\":\"epsilon\",\"_edges\":{\"graph_idx\":{\"cites\":[{\"target\":\"doc:b\",\"weight\":1.0}]}}}",
        }},
        .sync_level = .write,
    });
    try db.runDerivedUntil(db.core.nextDerivedSequence());

    try graph_metric_runtime_mod.expectPlannedAutoIdleDecision(db.core.index_manager, db.graph_metric_idle_auto_options, true, 0, 1, 0, 0);
    const retry_new_generation = try db.runGraphMetricPlannedCoordinatorSweep(.{
        .max_metrics = 8,
        .start_background_builds = true,
    });
    try std.testing.expectEqual(@as(usize, 1), retry_new_generation.builds_started);
    {
        const graph_entry = db.core.graphIndex("graph_idx") orelse return error.IndexNotFound;
        var status = try graph_entry.index.graphMetricStatus("pagerank");
        defer status.deinit(alloc);
        try std.testing.expectEqual(graph_mod.GraphIndex.GraphMetricState.building, status.state);
        try std.testing.expect(status.building_generation > failed_target_generation);
    }
}

test "db graph metric manual refresh rebuild and delete operate on configured metric" {
    const alloc = std.testing.allocator;

    var path_buf: [256]u8 = undefined;
    const path = tempPath(&path_buf);
    defer cleanupTempDir(path);

    var db = try DB.open(alloc, std.mem.span(path), .{
        .start_index_workers = false,
        .ttl_cleanup = .{ .enabled = false },
    });
    defer db.close();

    try db.addIndex(.{
        .name = "graph_idx",
        .kind = .graph,
        .config_json = "{\"metrics\":{\"manual_degree\":{\"enabled\":true,\"kind\":\"degree\",\"refresh\":\"manual\",\"edge_filter\":{\"types\":[\"cites\"]}}}}",
    });

    try db.batch(.{
        .writes = &.{
            .{ .key = "doc:a", .value = "{\"title\":\"alpha\",\"_edges\":{\"graph_idx\":{\"cites\":[{\"target\":\"doc:b\",\"weight\":1.0}]}}}" },
            .{ .key = "doc:b", .value = "{\"title\":\"beta\"}" },
        },
        .sync_level = .write,
    });

    try db.runUntilIdle();
    {
        const graph_entry = db.core.graphIndex("graph_idx") orelse return error.IndexNotFound;
        var status = try graph_entry.index.graphMetricStatus("manual_degree");
        defer status.deinit(alloc);
        try std.testing.expectEqual(graph_mod.GraphIndex.GraphMetricState.not_ready, status.state);
    }

    var refreshed = try db.refreshGraphMetric(alloc, "graph_idx", "manual_degree");
    defer refreshed.deinit(alloc);
    try std.testing.expectEqual(graph_mod.GraphIndex.GraphMetricState.fresh, refreshed.state);
    try std.testing.expect(refreshed.published_generation > 0);

    try db.batch(.{
        .writes = &.{
            .{ .key = "doc:c", .value = "{\"title\":\"gamma\",\"_edges\":{\"graph_idx\":{\"cites\":[{\"target\":\"doc:b\",\"weight\":1.0}]}}}" },
        },
        .sync_level = .write,
    });
    try db.runUntilIdle();
    {
        const graph_entry = db.core.graphIndex("graph_idx") orelse return error.IndexNotFound;
        var status = try graph_entry.index.graphMetricStatus("manual_degree");
        defer status.deinit(alloc);
        try std.testing.expectEqual(graph_mod.GraphIndex.GraphMetricState.stale, status.state);
    }

    var rebuilt = try db.rebuildGraphMetric(alloc, "graph_idx", "manual_degree");
    defer rebuilt.deinit(alloc);
    try std.testing.expectEqual(graph_mod.GraphIndex.GraphMetricState.fresh, rebuilt.state);
    try std.testing.expect(rebuilt.published_generation >= refreshed.published_generation);

    const graph_entry = db.core.graphIndex("graph_idx") orelse return error.IndexNotFound;
    const top = try graph_entry.index.graphMetricTopK("manual_degree", 1);
    defer {
        for (top) |*score| score.deinit(alloc);
        alloc.free(top);
    }
    try std.testing.expectEqual(@as(usize, 1), top.len);
    try std.testing.expectEqualStrings("doc:b", top[0].node);
    try std.testing.expectApproxEqAbs(@as(f64, 2.0), top[0].score, 0.001);

    var deleted = try db.deleteGraphMetricMaterialization(alloc, "graph_idx", "manual_degree");
    defer deleted.deinit(alloc);
    try std.testing.expectEqual(graph_mod.GraphIndex.GraphMetricState.not_ready, deleted.state);
    try std.testing.expectEqual(@as(u64, 0), deleted.published_generation);
    try std.testing.expect(!deleted.maintenance_paused);

    try std.testing.expectError(error.MetricNotReady, graph_entry.index.graphMetricTopK("manual_degree", 1));

    var refreshed_after_delete = try db.refreshGraphMetric(alloc, "graph_idx", "manual_degree");
    defer refreshed_after_delete.deinit(alloc);
    try std.testing.expectEqual(graph_mod.GraphIndex.GraphMetricState.fresh, refreshed_after_delete.state);
}

test "db graph metric planned scheduler boundary completes degree by name" {
    const alloc = std.testing.allocator;

    var path_buf: [256]u8 = undefined;
    const path = tempPath(&path_buf);
    defer cleanupTempDir(path);

    var db = try DB.open(alloc, std.mem.span(path), .{
        .start_index_workers = false,
        .ttl_cleanup = .{ .enabled = false },
    });
    defer db.close();

    try db.addIndex(.{
        .name = "graph_idx",
        .kind = .graph,
        .config_json = "{\"metrics\":{\"manual_degree\":{\"enabled\":true,\"kind\":\"degree\",\"refresh\":\"manual\",\"edge_filter\":{\"types\":[\"cites\"]}}}}",
    });

    try db.batch(.{
        .writes = &.{
            .{ .key = "doc:a", .value = "{\"title\":\"alpha\",\"_edges\":{\"graph_idx\":{\"cites\":[{\"target\":\"doc:b\",\"weight\":1.0}]}}}" },
            .{ .key = "doc:b", .value = "{\"title\":\"beta\"}" },
            .{ .key = "doc:c", .value = "{\"title\":\"gamma\",\"_edges\":{\"graph_idx\":{\"cites\":[{\"target\":\"doc:b\",\"weight\":1.0}]}}}" },
        },
        .sync_level = .write,
    });
    try db.runUntilIdle();

    const target_generation = blk: {
        const graph_entry = db.core.graphIndex("graph_idx") orelse return error.IndexNotFound;
        break :blk graph_entry.index.edge_generation;
    };

    var started = try db.ensureGraphMetricPlannedBuild(alloc, "graph_idx", "manual_degree", target_generation);
    defer started.deinit(alloc);
    try std.testing.expectEqual(graph_mod.GraphIndex.GraphMetricState.building, started.state);
    try std.testing.expectEqual(target_generation, started.building_generation);

    const workers = [_][]const u8{ "worker-a", "worker-b" };
    var step_index: usize = 0;
    var published = false;
    while (step_index < 1000) : (step_index += 1) {
        const now_ms: u64 = 1000 + @as(u64, @intCast(step_index)) * 2;
        const worker_step = try db.runGraphMetricPlannedWorkerPageStepAt("graph_idx", "manual_degree", workers[step_index % workers.len], now_ms);
        try std.testing.expect(!worker_step.advanced_phase);
        if (worker_step.completed_build) {
            try std.testing.expect(worker_step.phase == .complete or worker_step.phase == .cleanup_old_generations);
            published = true;
            break;
        }

        const coordinator_step = try db.runGraphMetricPlannedCoordinatorStepAt("graph_idx", "manual_degree", now_ms + 1);
        if (coordinator_step.completed_build) {
            published = true;
            break;
        }

        const progressed =
            worker_step.claimed_page or
            worker_step.completed_page or
            coordinator_step.advanced_phase;
        if (!progressed and worker_step.phase != .cleanup_old_generations) return error.GraphMetricBuildNoEligiblePage;
    }
    try std.testing.expect(published);

    var published_result = try db.search(alloc, .{
        .graph_metric_queries = &.{.{
            .name = "central",
            .query = .{
                .index_name = "graph_idx",
                .metric_name = "manual_degree",
                .top_k = 3,
                .freshness = .fresh,
            },
        }},
        .limit = 0,
    });
    defer published_result.deinit();
    try std.testing.expectEqual(@as(usize, 1), published_result.graph_metric_results.len);
    try std.testing.expectEqual(graph_mod.GraphIndex.GraphMetricState.fresh, published_result.graph_metric_results[0].status.state);
    try std.testing.expectEqual(target_generation, published_result.graph_metric_results[0].status.published_generation);
    try std.testing.expectEqual(@as(usize, 3), published_result.graph_metric_results[0].scores.len);
    try std.testing.expectEqualStrings("doc:b", published_result.graph_metric_results[0].scores[0].node);
    try std.testing.expectApproxEqAbs(@as(f64, 2.0), published_result.graph_metric_results[0].scores[0].score, 0.001);
}

test "db graph metric planned scheduler sweeps active degree work" {
    const alloc = std.testing.allocator;

    var path_buf: [256]u8 = undefined;
    const path = tempPath(&path_buf);
    defer cleanupTempDir(path);

    var db = try DB.open(alloc, std.mem.span(path), .{
        .start_index_workers = false,
        .ttl_cleanup = .{ .enabled = false },
    });
    defer db.close();

    try db.addIndex(.{
        .name = "graph_idx",
        .kind = .graph,
        .config_json = "{\"metrics\":{\"degree\":{\"enabled\":true,\"kind\":\"degree\",\"refresh\":\"manual\",\"edge_filter\":{\"types\":[\"cites\"]}}}}",
    });

    try db.batch(.{
        .writes = &.{
            .{ .key = "doc:a", .value = "{\"title\":\"alpha\",\"_edges\":{\"graph_idx\":{\"cites\":[{\"target\":\"doc:b\",\"weight\":1.0}]}}}" },
            .{ .key = "doc:b", .value = "{\"title\":\"beta\"}" },
            .{ .key = "doc:c", .value = "{\"title\":\"gamma\",\"_edges\":{\"graph_idx\":{\"cites\":[{\"target\":\"doc:b\",\"weight\":1.0}]}}}" },
        },
        .sync_level = .write,
    });
    try db.runUntilIdle();

    const target_generation = blk: {
        const graph_entry = db.core.graphIndex("graph_idx") orelse return error.IndexNotFound;
        break :blk graph_entry.index.edge_generation;
    };
    var started = try db.ensureGraphMetricPlannedBuild(alloc, "graph_idx", "degree", target_generation);
    defer started.deinit(alloc);
    try std.testing.expectEqual(graph_mod.GraphIndex.GraphMetricState.building, started.state);
    try std.testing.expectEqual(target_generation, started.building_generation);

    const start = try db.runGraphMetricPlannedCoordinatorSweep(.{
        .max_metrics = 8,
        .start_background_builds = false,
    });
    try std.testing.expectEqual(@as(usize, 1), start.metrics_scanned);
    try std.testing.expectEqual(@as(usize, 0), start.builds_started);
    try std.testing.expect(start.active_builds > 0);

    const workers = [_][]const u8{ "sweep-worker-a", "sweep-worker-b" };
    var finished = false;
    var saw_coordinator_publish = false;
    var step_index: usize = 0;
    while (step_index < 1000) : (step_index += 1) {
        const worker = try db.runGraphMetricPlannedWorkerSweep(.{
            .worker_id = workers[step_index % workers.len],
            .max_pages = 1,
        });
        try std.testing.expectEqual(@as(usize, 0), worker.published);
        const coordinator = try db.runGraphMetricPlannedCoordinatorSweep(.{
            .max_metrics = 8,
            .start_background_builds = false,
        });
        if (coordinator.published > 0) saw_coordinator_publish = true;
        {
            const graph_entry = db.core.graphIndex("graph_idx") orelse return error.IndexNotFound;
            var status = try graph_entry.index.graphMetricStatus("degree");
            defer status.deinit(alloc);
            if (status.state == .fresh and status.published_generation != 0 and status.phase == .complete) {
                finished = true;
                break;
            }
        }
        if (!worker.progressed() and !coordinator.progressed()) {
            return error.GraphMetricBuildNoEligiblePage;
        }
    }
    try std.testing.expect(finished);
    try std.testing.expect(saw_coordinator_publish);

    var published_result = try db.search(alloc, .{
        .graph_metric_queries = &.{.{
            .name = "central",
            .query = .{
                .index_name = "graph_idx",
                .metric_name = "degree",
                .top_k = 3,
                .freshness = .fresh,
            },
        }},
        .limit = 0,
    });
    defer published_result.deinit();
    try std.testing.expectEqual(@as(usize, 1), published_result.graph_metric_results.len);
    try std.testing.expectEqual(graph_mod.GraphIndex.GraphMetricState.fresh, published_result.graph_metric_results[0].status.state);
    try std.testing.expectEqual(@as(usize, 3), published_result.graph_metric_results[0].scores.len);
    try std.testing.expectEqualStrings("doc:b", published_result.graph_metric_results[0].scores[0].node);
    try std.testing.expectApproxEqAbs(@as(f64, 2.0), published_result.graph_metric_results[0].scores[0].score, 0.001);
}

test "db graph metric planned scheduler sweeps active pagerank work" {
    const alloc = std.testing.allocator;

    var path_buf: [256]u8 = undefined;
    const path = tempPath(&path_buf);
    defer cleanupTempDir(path);

    var db = try DB.open(alloc, std.mem.span(path), .{
        .start_index_workers = false,
        .ttl_cleanup = .{ .enabled = false },
    });
    defer db.close();

    try db.addIndex(.{
        .name = "graph_idx",
        .kind = .graph,
        .config_json = "{\"metrics\":{\"pagerank\":{\"enabled\":true,\"kind\":\"pagerank\",\"refresh\":\"manual\",\"max_iterations\":20,\"tolerance\":0.000001,\"edge_filter\":{\"types\":[\"cites\"]}}}}",
    });

    try db.batch(.{
        .writes = &.{
            .{ .key = "doc:a", .value = "{\"title\":\"alpha\",\"_edges\":{\"graph_idx\":{\"cites\":[{\"target\":\"doc:b\",\"weight\":1.0}]}}}" },
            .{ .key = "doc:b", .value = "{\"title\":\"beta\",\"_edges\":{\"graph_idx\":{\"cites\":[{\"target\":\"doc:d\",\"weight\":1.0}]}}}" },
            .{ .key = "doc:c", .value = "{\"title\":\"gamma\",\"_edges\":{\"graph_idx\":{\"cites\":[{\"target\":\"doc:b\",\"weight\":1.0}]}}}" },
            .{ .key = "doc:d", .value = "{\"title\":\"delta\"}" },
        },
        .sync_level = .write,
    });
    try db.runUntilIdle();

    const target_generation = blk: {
        const graph_entry = db.core.graphIndex("graph_idx") orelse return error.IndexNotFound;
        break :blk graph_entry.index.edge_generation;
    };
    var started = try db.ensureGraphMetricPlannedBuild(alloc, "graph_idx", "pagerank", target_generation);
    defer started.deinit(alloc);
    try std.testing.expectEqual(graph_mod.GraphIndex.GraphMetricState.building, started.state);
    try std.testing.expectEqual(target_generation, started.building_generation);

    const workers = [_][]const u8{ "pagerank-sweep-a", "pagerank-sweep-b", "pagerank-sweep-c" };
    var finished = false;
    var step_index: usize = 0;
    while (step_index < 2000) : (step_index += 1) {
        const worker = try db.runGraphMetricPlannedWorkerSweep(.{
            .worker_id = workers[step_index % workers.len],
            .max_pages = 1,
        });
        const coordinator = try db.runGraphMetricPlannedCoordinatorSweep(.{
            .max_metrics = 8,
            .start_background_builds = false,
        });
        {
            const graph_entry = db.core.graphIndex("graph_idx") orelse return error.IndexNotFound;
            var status = try graph_entry.index.graphMetricStatus("pagerank");
            defer status.deinit(alloc);
            if (status.state == .fresh and status.published_generation != 0 and status.phase == .complete) {
                try std.testing.expect(status.iterations_completed > 0);
                finished = true;
                break;
            }
        }
        if (!worker.progressed() and !coordinator.progressed()) {
            return error.GraphMetricBuildNoEligiblePage;
        }
    }
    try std.testing.expect(finished);

    var published_result = try db.search(alloc, .{
        .graph_metric_queries = &.{.{
            .name = "central",
            .query = .{
                .index_name = "graph_idx",
                .metric_name = "pagerank",
                .top_k = 2,
                .freshness = .fresh,
            },
        }},
        .limit = 0,
    });
    defer published_result.deinit();
    try std.testing.expectEqual(@as(usize, 1), published_result.graph_metric_results.len);
    try std.testing.expectEqual(graph_mod.GraphIndex.GraphMetricState.fresh, published_result.graph_metric_results[0].status.state);
    try std.testing.expectEqual(target_generation, published_result.graph_metric_results[0].status.published_generation);
    try std.testing.expectEqual(@as(usize, 2), published_result.graph_metric_results[0].scores.len);
    try std.testing.expectEqualStrings("doc:d", published_result.graph_metric_results[0].scores[0].node);
    try std.testing.expect(published_result.graph_metric_results[0].scores[0].score >= published_result.graph_metric_results[0].scores[1].score);
}

test "db graph metric planned scheduler sweeps pagerank across reopened handles" {
    const alloc = std.testing.allocator;

    var path_buf: [256]u8 = undefined;
    const path = tempPath(&path_buf);
    defer cleanupTempDir(path);

    var target_generation: u64 = 0;
    {
        var db = try DB.open(alloc, std.mem.span(path), .{
            .start_index_workers = false,
            .ttl_cleanup = .{ .enabled = false },
        });
        defer db.close();

        try db.addIndex(.{
            .name = "graph_idx",
            .kind = .graph,
            .config_json = "{\"metrics\":{\"pagerank\":{\"enabled\":true,\"kind\":\"pagerank\",\"refresh\":\"manual\",\"max_iterations\":20,\"tolerance\":0.000001,\"edge_filter\":{\"types\":[\"cites\"]}}}}",
        });

        try db.batch(.{
            .writes = &.{
                .{ .key = "doc:a", .value = "{\"title\":\"alpha\",\"_edges\":{\"graph_idx\":{\"cites\":[{\"target\":\"doc:b\",\"weight\":1.0}]}}}" },
                .{ .key = "doc:b", .value = "{\"title\":\"beta\",\"_edges\":{\"graph_idx\":{\"cites\":[{\"target\":\"doc:d\",\"weight\":1.0}]}}}" },
                .{ .key = "doc:c", .value = "{\"title\":\"gamma\",\"_edges\":{\"graph_idx\":{\"cites\":[{\"target\":\"doc:b\",\"weight\":1.0}]}}}" },
                .{ .key = "doc:d", .value = "{\"title\":\"delta\"}" },
            },
            .sync_level = .write,
        });
        try db.runUntilIdle();

        const graph_entry = db.core.graphIndex("graph_idx") orelse return error.IndexNotFound;
        target_generation = graph_entry.index.edge_generation;
    }

    {
        var coordinator = try DB.open(alloc, std.mem.span(path), .{
            .start_index_workers = false,
            .ttl_cleanup = .{ .enabled = false },
        });
        defer coordinator.close();

        var started = try coordinator.ensureGraphMetricPlannedBuild(alloc, "graph_idx", "pagerank", target_generation);
        defer started.deinit(alloc);
        try std.testing.expectEqual(graph_mod.GraphIndex.GraphMetricState.building, started.state);
        try std.testing.expectEqual(target_generation, started.building_generation);

        const initial_tick = try coordinator.runGraphMetricPlannedCoordinatorSweep(.{
            .max_metrics = 8,
            .start_background_builds = false,
        });
        try std.testing.expect(initial_tick.active_builds > 0);
    }

    const workers = [_][]const u8{ "reopened-pagerank-a", "reopened-pagerank-b", "reopened-pagerank-c" };
    var finished = false;
    var step_index: usize = 0;
    while (step_index < 2000) : (step_index += 1) {
        const worker = blk: {
            var worker_db = try DB.open(alloc, std.mem.span(path), .{
                .start_index_workers = false,
                .ttl_cleanup = .{ .enabled = false },
            });
            defer worker_db.close();
            break :blk try worker_db.runGraphMetricPlannedWorkerSweep(.{
                .worker_id = workers[step_index % workers.len],
                .max_pages = 1,
            });
        };

        const coordinator = blk: {
            var coordinator_db = try DB.open(alloc, std.mem.span(path), .{
                .start_index_workers = false,
                .ttl_cleanup = .{ .enabled = false },
            });
            defer coordinator_db.close();
            const sweep = try coordinator_db.runGraphMetricPlannedCoordinatorSweep(.{
                .max_metrics = 8,
                .start_background_builds = false,
            });
            {
                const graph_entry = coordinator_db.core.graphIndex("graph_idx") orelse return error.IndexNotFound;
                var status = try graph_entry.index.graphMetricStatus("pagerank");
                defer status.deinit(alloc);
                if (status.state == .fresh and status.published_generation != 0 and status.phase == .complete) {
                    try std.testing.expectEqual(target_generation, status.published_generation);
                    try std.testing.expect(status.iterations_completed > 0);
                    finished = true;
                }
            }
            break :blk sweep;
        };
        if (finished) break;
        if (!worker.progressed() and !coordinator.progressed()) {
            return error.GraphMetricBuildNoEligiblePage;
        }
    }
    try std.testing.expect(finished);

    {
        var reader = try DB.open(alloc, std.mem.span(path), .{
            .start_index_workers = false,
            .ttl_cleanup = .{ .enabled = false },
        });
        defer reader.close();

        var published_result = try reader.search(alloc, .{
            .graph_metric_queries = &.{.{
                .name = "central",
                .query = .{
                    .index_name = "graph_idx",
                    .metric_name = "pagerank",
                    .top_k = 2,
                    .freshness = .fresh,
                },
            }},
            .limit = 0,
        });
        defer published_result.deinit();
        try std.testing.expectEqual(@as(usize, 1), published_result.graph_metric_results.len);
        try std.testing.expectEqual(graph_mod.GraphIndex.GraphMetricState.fresh, published_result.graph_metric_results[0].status.state);
        try std.testing.expectEqual(target_generation, published_result.graph_metric_results[0].status.published_generation);
        try std.testing.expectEqual(@as(usize, 2), published_result.graph_metric_results[0].scores.len);
        try std.testing.expectEqualStrings("doc:d", published_result.graph_metric_results[0].scores[0].node);
        try std.testing.expect(published_result.graph_metric_results[0].scores[0].score >= published_result.graph_metric_results[0].scores[1].score);
    }
}

test "db graph metric planned scheduler reopened coordinators do not duplicate pagerank publish" {
    const alloc = std.testing.allocator;

    var path_buf: [256]u8 = undefined;
    const path = tempPath(&path_buf);
    defer cleanupTempDir(path);

    var target_generation: u64 = 0;
    {
        var db = try DB.open(alloc, std.mem.span(path), .{
            .start_index_workers = false,
            .ttl_cleanup = .{ .enabled = false },
        });
        defer db.close();

        try db.addIndex(.{
            .name = "graph_idx",
            .kind = .graph,
            .config_json = "{\"metrics\":{\"pagerank\":{\"enabled\":true,\"kind\":\"pagerank\",\"refresh\":\"manual\",\"max_iterations\":1,\"tolerance\":0.000001,\"edge_filter\":{\"types\":[\"cites\"]}}}}",
        });

        try db.batch(.{
            .writes = &.{
                .{ .key = "doc:a", .value = "{\"title\":\"alpha\",\"_edges\":{\"graph_idx\":{\"cites\":[{\"target\":\"doc:b\",\"weight\":1.0}]}}}" },
                .{ .key = "doc:b", .value = "{\"title\":\"beta\",\"_edges\":{\"graph_idx\":{\"cites\":[{\"target\":\"doc:d\",\"weight\":1.0}]}}}" },
                .{ .key = "doc:c", .value = "{\"title\":\"gamma\",\"_edges\":{\"graph_idx\":{\"cites\":[{\"target\":\"doc:b\",\"weight\":1.0}]}}}" },
                .{ .key = "doc:d", .value = "{\"title\":\"delta\"}" },
            },
            .sync_level = .write,
        });
        try db.runUntilIdle();

        const graph_entry = db.core.graphIndex("graph_idx") orelse return error.IndexNotFound;
        target_generation = graph_entry.index.edge_generation;
    }

    {
        var coordinator = try DB.open(alloc, std.mem.span(path), .{
            .start_index_workers = false,
            .ttl_cleanup = .{ .enabled = false },
        });
        defer coordinator.close();

        var started = try coordinator.ensureGraphMetricPlannedBuild(alloc, "graph_idx", "pagerank", target_generation);
        defer started.deinit(alloc);
        try std.testing.expectEqual(graph_mod.GraphIndex.GraphMetricState.building, started.state);
        try std.testing.expectEqual(target_generation, started.building_generation);
    }

    const workers = [_][]const u8{ "db-publish-race-worker-a", "db-publish-race-worker-b" };
    var reached_publish = false;
    var step_index: usize = 0;
    while (step_index < 400) : (step_index += 1) {
        const worker = blk: {
            var worker_db = try DB.open(alloc, std.mem.span(path), .{
                .start_index_workers = false,
                .ttl_cleanup = .{ .enabled = false },
            });
            defer worker_db.close();
            break :blk try worker_db.runGraphMetricPlannedWorkerPageStep("graph_idx", "pagerank", workers[step_index % workers.len]);
        };

        {
            var reader = try DB.open(alloc, std.mem.span(path), .{
                .start_index_workers = false,
                .ttl_cleanup = .{ .enabled = false },
            });
            defer reader.close();
            const graph_entry = reader.core.graphIndex("graph_idx") orelse return error.IndexNotFound;
            var status = try graph_entry.index.graphMetricStatus("pagerank");
            defer status.deinit(alloc);
            if (status.phase == .publish_generation) {
                reached_publish = true;
                break;
            }
        }

        const coordinator = blk: {
            var coordinator_db = try DB.open(alloc, std.mem.span(path), .{
                .start_index_workers = false,
                .ttl_cleanup = .{ .enabled = false },
            });
            defer coordinator_db.close();
            break :blk try coordinator_db.runGraphMetricPlannedCoordinatorStep("graph_idx", "pagerank");
        };

        {
            var reader = try DB.open(alloc, std.mem.span(path), .{
                .start_index_workers = false,
                .ttl_cleanup = .{ .enabled = false },
            });
            defer reader.close();
            const graph_entry = reader.core.graphIndex("graph_idx") orelse return error.IndexNotFound;
            var status = try graph_entry.index.graphMetricStatus("pagerank");
            defer status.deinit(alloc);
            if (status.phase == .publish_generation) {
                reached_publish = true;
                break;
            }
        }

        if (!worker.claimed_page and !worker.completed_page and !coordinator.advanced_phase) {
            return error.GraphMetricBuildNoEligiblePage;
        }
    }
    try std.testing.expect(reached_publish);

    {
        var coordinator = try DB.open(alloc, std.mem.span(path), .{
            .start_index_workers = false,
            .ttl_cleanup = .{ .enabled = false },
        });
        defer coordinator.close();

        const publish = try coordinator.runGraphMetricPlannedCoordinatorStep("graph_idx", "pagerank");
        try std.testing.expectEqual(graph_mod.GraphIndex.GraphMetricBuildPhase.publish_generation, publish.phase);
        try std.testing.expect(publish.advanced_phase);

        const graph_entry = coordinator.core.graphIndex("graph_idx") orelse return error.IndexNotFound;
        var status = try graph_entry.index.graphMetricStatus("pagerank");
        defer status.deinit(alloc);
        try std.testing.expectEqual(graph_mod.GraphIndex.GraphMetricState.building, status.state);
        try std.testing.expectEqual(graph_mod.GraphIndex.GraphMetricBuildPhase.cleanup_old_generations, status.phase);
        try std.testing.expectEqual(target_generation, status.published_generation);
        try std.testing.expectEqual(@as(usize, 1), status.recent_events.len);
        try std.testing.expectEqual(graph_mod.GraphIndex.GraphMetricEventKind.publish, status.recent_events[0].kind);
    }

    {
        var duplicate_coordinator = try DB.open(alloc, std.mem.span(path), .{
            .start_index_workers = false,
            .ttl_cleanup = .{ .enabled = false },
        });
        defer duplicate_coordinator.close();

        const duplicate = try duplicate_coordinator.runGraphMetricPlannedCoordinatorStep("graph_idx", "pagerank");
        try std.testing.expectEqual(graph_mod.GraphIndex.GraphMetricBuildPhase.cleanup_old_generations, duplicate.phase);
        try std.testing.expect(!duplicate.advanced_phase);
        try std.testing.expect(!duplicate.published);

        const graph_entry = duplicate_coordinator.core.graphIndex("graph_idx") orelse return error.IndexNotFound;
        var status = try graph_entry.index.graphMetricStatus("pagerank");
        defer status.deinit(alloc);
        try std.testing.expectEqual(graph_mod.GraphIndex.GraphMetricState.building, status.state);
        try std.testing.expectEqual(graph_mod.GraphIndex.GraphMetricBuildPhase.cleanup_old_generations, status.phase);
        try std.testing.expectEqual(target_generation, status.published_generation);
        try std.testing.expectEqual(@as(usize, 1), status.recent_events.len);
        try std.testing.expectEqual(graph_mod.GraphIndex.GraphMetricEventKind.publish, status.recent_events[0].kind);
    }

    {
        var worker_db = try DB.open(alloc, std.mem.span(path), .{
            .start_index_workers = false,
            .ttl_cleanup = .{ .enabled = false },
        });
        defer worker_db.close();

        var cleanup_finished = false;
        for (0..12) |_| {
            const cleanup = try worker_db.runGraphMetricPlannedWorkerPageStep("graph_idx", "pagerank", "db-publish-race-cleaner");
            try std.testing.expectEqual(graph_mod.GraphIndex.GraphMetricBuildPhase.cleanup_old_generations, cleanup.phase);
            try std.testing.expect(!cleanup.advanced_phase);
            if (cleanup.published) {
                cleanup_finished = true;
                break;
            }
        }
        try std.testing.expect(cleanup_finished);
    }

    {
        var reader = try DB.open(alloc, std.mem.span(path), .{
            .start_index_workers = false,
            .ttl_cleanup = .{ .enabled = false },
        });
        defer reader.close();

        const graph_entry = reader.core.graphIndex("graph_idx") orelse return error.IndexNotFound;
        var status = try graph_entry.index.graphMetricStatus("pagerank");
        defer status.deinit(alloc);
        try std.testing.expectEqual(graph_mod.GraphIndex.GraphMetricState.fresh, status.state);
        try std.testing.expectEqual(graph_mod.GraphIndex.GraphMetricBuildPhase.complete, status.phase);
        try std.testing.expectEqual(target_generation, status.published_generation);
        try std.testing.expectEqual(@as(usize, 1), status.recent_events.len);
        try std.testing.expectEqual(graph_mod.GraphIndex.GraphMetricEventKind.publish, status.recent_events[0].kind);

        var published_result = try reader.search(alloc, .{
            .graph_metric_queries = &.{.{
                .name = "central",
                .query = .{
                    .index_name = "graph_idx",
                    .metric_name = "pagerank",
                    .top_k = 2,
                    .freshness = .fresh,
                },
            }},
            .limit = 0,
        });
        defer published_result.deinit();
        try std.testing.expectEqual(@as(usize, 1), published_result.graph_metric_results.len);
        try std.testing.expectEqual(graph_mod.GraphIndex.GraphMetricState.fresh, published_result.graph_metric_results[0].status.state);
        try std.testing.expectEqual(target_generation, published_result.graph_metric_results[0].status.published_generation);
        try std.testing.expectEqual(@as(usize, 2), published_result.graph_metric_results[0].scores.len);
    }
}

test "db graph metric planned scheduler reopened coordinators do not duplicate eigenvector publish" {
    const alloc = std.testing.allocator;

    var path_buf: [256]u8 = undefined;
    const path = tempPath(&path_buf);
    defer cleanupTempDir(path);

    var target_generation: u64 = 0;
    {
        var db = try DB.open(alloc, std.mem.span(path), .{
            .start_index_workers = false,
            .ttl_cleanup = .{ .enabled = false },
        });
        defer db.close();

        try db.addIndex(.{
            .name = "graph_idx",
            .kind = .graph,
            .config_json = "{\"metrics\":{\"eigenvector\":{\"enabled\":true,\"kind\":\"eigenvector\",\"refresh\":\"manual\",\"max_iterations\":1,\"tolerance\":0.000001,\"edge_filter\":{\"types\":[\"cites\"]}}}}",
        });

        try db.batch(.{
            .writes = &.{
                .{ .key = "doc:a", .value = "{\"title\":\"alpha\",\"_edges\":{\"graph_idx\":{\"cites\":[{\"target\":\"doc:b\",\"weight\":1.0}]}}}" },
                .{ .key = "doc:b", .value = "{\"title\":\"beta\",\"_edges\":{\"graph_idx\":{\"cites\":[{\"target\":\"doc:d\",\"weight\":1.0}]}}}" },
                .{ .key = "doc:c", .value = "{\"title\":\"gamma\",\"_edges\":{\"graph_idx\":{\"cites\":[{\"target\":\"doc:b\",\"weight\":1.0}]}}}" },
                .{ .key = "doc:d", .value = "{\"title\":\"delta\"}" },
            },
            .sync_level = .write,
        });
        try db.runUntilIdle();

        const graph_entry = db.core.graphIndex("graph_idx") orelse return error.IndexNotFound;
        target_generation = graph_entry.index.edge_generation;
    }

    {
        var coordinator = try DB.open(alloc, std.mem.span(path), .{
            .start_index_workers = false,
            .ttl_cleanup = .{ .enabled = false },
        });
        defer coordinator.close();

        var started = try coordinator.ensureGraphMetricPlannedBuild(alloc, "graph_idx", "eigenvector", target_generation);
        defer started.deinit(alloc);
        try std.testing.expectEqual(graph_mod.GraphIndex.GraphMetricState.building, started.state);
        try std.testing.expectEqual(target_generation, started.building_generation);
    }

    const workers = [_][]const u8{ "db-eigenvector-publish-race-worker-a", "db-eigenvector-publish-race-worker-b" };
    var reached_publish = false;
    var step_index: usize = 0;
    while (step_index < 400) : (step_index += 1) {
        const worker = blk: {
            var worker_db = try DB.open(alloc, std.mem.span(path), .{
                .start_index_workers = false,
                .ttl_cleanup = .{ .enabled = false },
            });
            defer worker_db.close();
            break :blk try worker_db.runGraphMetricPlannedWorkerPageStep("graph_idx", "eigenvector", workers[step_index % workers.len]);
        };

        {
            var reader = try DB.open(alloc, std.mem.span(path), .{
                .start_index_workers = false,
                .ttl_cleanup = .{ .enabled = false },
            });
            defer reader.close();
            const graph_entry = reader.core.graphIndex("graph_idx") orelse return error.IndexNotFound;
            var status = try graph_entry.index.graphMetricStatus("eigenvector");
            defer status.deinit(alloc);
            if (status.phase == .publish_generation) {
                reached_publish = true;
                break;
            }
        }

        const coordinator = blk: {
            var coordinator_db = try DB.open(alloc, std.mem.span(path), .{
                .start_index_workers = false,
                .ttl_cleanup = .{ .enabled = false },
            });
            defer coordinator_db.close();
            break :blk try coordinator_db.runGraphMetricPlannedCoordinatorStep("graph_idx", "eigenvector");
        };

        {
            var reader = try DB.open(alloc, std.mem.span(path), .{
                .start_index_workers = false,
                .ttl_cleanup = .{ .enabled = false },
            });
            defer reader.close();
            const graph_entry = reader.core.graphIndex("graph_idx") orelse return error.IndexNotFound;
            var status = try graph_entry.index.graphMetricStatus("eigenvector");
            defer status.deinit(alloc);
            if (status.phase == .publish_generation) {
                reached_publish = true;
                break;
            }
        }

        if (!worker.claimed_page and !worker.completed_page and !coordinator.advanced_phase) {
            return error.GraphMetricBuildNoEligiblePage;
        }
    }
    try std.testing.expect(reached_publish);

    {
        var coordinator = try DB.open(alloc, std.mem.span(path), .{
            .start_index_workers = false,
            .ttl_cleanup = .{ .enabled = false },
        });
        defer coordinator.close();

        const publish = try coordinator.runGraphMetricPlannedCoordinatorStep("graph_idx", "eigenvector");
        try std.testing.expectEqual(graph_mod.GraphIndex.GraphMetricBuildPhase.publish_generation, publish.phase);
        try std.testing.expect(publish.advanced_phase);

        const graph_entry = coordinator.core.graphIndex("graph_idx") orelse return error.IndexNotFound;
        var status = try graph_entry.index.graphMetricStatus("eigenvector");
        defer status.deinit(alloc);
        try std.testing.expectEqual(graph_mod.GraphIndex.GraphMetricState.building, status.state);
        try std.testing.expectEqual(graph_mod.GraphIndex.GraphMetricBuildPhase.cleanup_old_generations, status.phase);
        try std.testing.expectEqual(target_generation, status.published_generation);
        try std.testing.expectEqual(@as(usize, 1), status.recent_events.len);
        try std.testing.expectEqual(graph_mod.GraphIndex.GraphMetricEventKind.publish, status.recent_events[0].kind);
    }

    {
        var duplicate_coordinator = try DB.open(alloc, std.mem.span(path), .{
            .start_index_workers = false,
            .ttl_cleanup = .{ .enabled = false },
        });
        defer duplicate_coordinator.close();

        const duplicate = try duplicate_coordinator.runGraphMetricPlannedCoordinatorStep("graph_idx", "eigenvector");
        try std.testing.expectEqual(graph_mod.GraphIndex.GraphMetricBuildPhase.cleanup_old_generations, duplicate.phase);
        try std.testing.expect(!duplicate.advanced_phase);
        try std.testing.expect(!duplicate.published);

        const graph_entry = duplicate_coordinator.core.graphIndex("graph_idx") orelse return error.IndexNotFound;
        var status = try graph_entry.index.graphMetricStatus("eigenvector");
        defer status.deinit(alloc);
        try std.testing.expectEqual(graph_mod.GraphIndex.GraphMetricState.building, status.state);
        try std.testing.expectEqual(graph_mod.GraphIndex.GraphMetricBuildPhase.cleanup_old_generations, status.phase);
        try std.testing.expectEqual(target_generation, status.published_generation);
        try std.testing.expectEqual(@as(usize, 1), status.recent_events.len);
        try std.testing.expectEqual(graph_mod.GraphIndex.GraphMetricEventKind.publish, status.recent_events[0].kind);
    }

    {
        var worker_db = try DB.open(alloc, std.mem.span(path), .{
            .start_index_workers = false,
            .ttl_cleanup = .{ .enabled = false },
        });
        defer worker_db.close();

        var cleanup_finished = false;
        for (0..12) |_| {
            const cleanup = try worker_db.runGraphMetricPlannedWorkerPageStep("graph_idx", "eigenvector", "db-eigenvector-publish-race-cleaner");
            try std.testing.expectEqual(graph_mod.GraphIndex.GraphMetricBuildPhase.cleanup_old_generations, cleanup.phase);
            try std.testing.expect(!cleanup.advanced_phase);
            if (cleanup.published) {
                cleanup_finished = true;
                break;
            }
        }
        try std.testing.expect(cleanup_finished);
    }

    {
        var reader = try DB.open(alloc, std.mem.span(path), .{
            .start_index_workers = false,
            .ttl_cleanup = .{ .enabled = false },
        });
        defer reader.close();

        const graph_entry = reader.core.graphIndex("graph_idx") orelse return error.IndexNotFound;
        var status = try graph_entry.index.graphMetricStatus("eigenvector");
        defer status.deinit(alloc);
        try std.testing.expectEqual(graph_mod.GraphIndex.GraphMetricState.fresh, status.state);
        try std.testing.expectEqual(graph_mod.GraphIndex.GraphMetricBuildPhase.complete, status.phase);
        try std.testing.expectEqual(target_generation, status.published_generation);
        try std.testing.expectEqual(@as(usize, 1), status.recent_events.len);
        try std.testing.expectEqual(graph_mod.GraphIndex.GraphMetricEventKind.publish, status.recent_events[0].kind);

        var published_result = try reader.search(alloc, .{
            .graph_metric_queries = &.{.{
                .name = "central",
                .query = .{
                    .index_name = "graph_idx",
                    .metric_name = "eigenvector",
                    .top_k = 2,
                    .freshness = .fresh,
                },
            }},
            .limit = 0,
        });
        defer published_result.deinit();
        try std.testing.expectEqual(@as(usize, 1), published_result.graph_metric_results.len);
        try std.testing.expectEqual(graph_mod.GraphIndex.GraphMetricState.fresh, published_result.graph_metric_results[0].status.state);
        try std.testing.expectEqual(target_generation, published_result.graph_metric_results[0].status.published_generation);
        try std.testing.expectEqual(@as(usize, 2), published_result.graph_metric_results[0].scores.len);
    }
}

test "db graph metric planned scheduler reopened coordinators do not duplicate hits publish" {
    const alloc = std.testing.allocator;

    var path_buf: [256]u8 = undefined;
    const path = tempPath(&path_buf);
    defer cleanupTempDir(path);

    var target_generation: u64 = 0;
    {
        var db = try DB.open(alloc, std.mem.span(path), .{
            .start_index_workers = false,
            .ttl_cleanup = .{ .enabled = false },
        });
        defer db.close();

        try db.addIndex(.{
            .name = "graph_idx",
            .kind = .graph,
            .config_json = "{\"metrics\":{\"hits_authority\":{\"enabled\":true,\"kind\":\"hits_authority\",\"refresh\":\"manual\",\"max_iterations\":1,\"tolerance\":0.000001,\"edge_filter\":{\"types\":[\"cites\"]}},\"hits_hub\":{\"enabled\":true,\"kind\":\"hits_hub\",\"refresh\":\"manual\",\"max_iterations\":1,\"tolerance\":0.000001,\"edge_filter\":{\"types\":[\"cites\"]}}}}",
        });

        try db.batch(.{
            .writes = &.{
                .{ .key = "doc:hub_a", .value = "{\"title\":\"hub a\",\"_edges\":{\"graph_idx\":{\"cites\":[{\"target\":\"doc:authority\",\"weight\":1.0}]}}}" },
                .{ .key = "doc:hub_b", .value = "{\"title\":\"hub b\",\"_edges\":{\"graph_idx\":{\"cites\":[{\"target\":\"doc:authority\",\"weight\":1.0}]}}}" },
                .{ .key = "doc:authority", .value = "{\"title\":\"authority\",\"_edges\":{\"graph_idx\":{\"cites\":[{\"target\":\"doc:authority\",\"weight\":1.0}]}}}" },
            },
            .sync_level = .write,
        });
        try db.runUntilIdle();

        const graph_entry = db.core.graphIndex("graph_idx") orelse return error.IndexNotFound;
        target_generation = graph_entry.index.edge_generation;
    }

    {
        var coordinator = try DB.open(alloc, std.mem.span(path), .{
            .start_index_workers = false,
            .ttl_cleanup = .{ .enabled = false },
        });
        defer coordinator.close();

        var started = try coordinator.ensureGraphMetricPlannedBuild(alloc, "graph_idx", "hits_authority", target_generation);
        defer started.deinit(alloc);
        try std.testing.expectEqual(graph_mod.GraphIndex.GraphMetricState.building, started.state);
        try std.testing.expectEqual(target_generation, started.building_generation);
    }

    const workers = [_][]const u8{ "db-hits-publish-race-worker-a", "db-hits-publish-race-worker-b" };
    var reached_publish = false;
    var step_index: usize = 0;
    while (step_index < 400) : (step_index += 1) {
        const worker = blk: {
            var worker_db = try DB.open(alloc, std.mem.span(path), .{
                .start_index_workers = false,
                .ttl_cleanup = .{ .enabled = false },
            });
            defer worker_db.close();
            break :blk try worker_db.runGraphMetricPlannedWorkerPageStep("graph_idx", "hits_authority", workers[step_index % workers.len]);
        };

        {
            var reader = try DB.open(alloc, std.mem.span(path), .{
                .start_index_workers = false,
                .ttl_cleanup = .{ .enabled = false },
            });
            defer reader.close();
            const graph_entry = reader.core.graphIndex("graph_idx") orelse return error.IndexNotFound;
            var status = try graph_entry.index.graphMetricStatus("hits_authority");
            defer status.deinit(alloc);
            if (status.phase == .publish_generation) {
                reached_publish = true;
                break;
            }
        }

        const coordinator = blk: {
            var coordinator_db = try DB.open(alloc, std.mem.span(path), .{
                .start_index_workers = false,
                .ttl_cleanup = .{ .enabled = false },
            });
            defer coordinator_db.close();
            break :blk try coordinator_db.runGraphMetricPlannedCoordinatorStep("graph_idx", "hits_authority");
        };

        {
            var reader = try DB.open(alloc, std.mem.span(path), .{
                .start_index_workers = false,
                .ttl_cleanup = .{ .enabled = false },
            });
            defer reader.close();
            const graph_entry = reader.core.graphIndex("graph_idx") orelse return error.IndexNotFound;
            var status = try graph_entry.index.graphMetricStatus("hits_authority");
            defer status.deinit(alloc);
            if (status.phase == .publish_generation) {
                reached_publish = true;
                break;
            }
        }

        if (!worker.claimed_page and !worker.completed_page and !coordinator.advanced_phase) {
            return error.GraphMetricBuildNoEligiblePage;
        }
    }
    try std.testing.expect(reached_publish);

    {
        var coordinator = try DB.open(alloc, std.mem.span(path), .{
            .start_index_workers = false,
            .ttl_cleanup = .{ .enabled = false },
        });
        defer coordinator.close();

        const publish = try coordinator.runGraphMetricPlannedCoordinatorStep("graph_idx", "hits_authority");
        try std.testing.expectEqual(graph_mod.GraphIndex.GraphMetricBuildPhase.publish_generation, publish.phase);
        try std.testing.expect(publish.advanced_phase);

        const graph_entry = coordinator.core.graphIndex("graph_idx") orelse return error.IndexNotFound;
        var authority = try graph_entry.index.graphMetricStatus("hits_authority");
        defer authority.deinit(alloc);
        var hub = try graph_entry.index.graphMetricStatus("hits_hub");
        defer hub.deinit(alloc);
        try std.testing.expectEqual(graph_mod.GraphIndex.GraphMetricState.building, authority.state);
        try std.testing.expectEqual(graph_mod.GraphIndex.GraphMetricState.fresh, hub.state);
        try std.testing.expectEqual(graph_mod.GraphIndex.GraphMetricBuildPhase.cleanup_old_generations, authority.phase);
        try std.testing.expectEqual(graph_mod.GraphIndex.GraphMetricBuildPhase.complete, hub.phase);
        try std.testing.expectEqual(target_generation, authority.published_generation);
        try std.testing.expectEqual(authority.published_generation, hub.published_generation);
        try std.testing.expectEqual(@as(usize, 1), authority.recent_events.len);
        try std.testing.expectEqual(@as(usize, 1), hub.recent_events.len);
        try std.testing.expectEqual(graph_mod.GraphIndex.GraphMetricEventKind.publish, authority.recent_events[0].kind);
        try std.testing.expectEqual(graph_mod.GraphIndex.GraphMetricEventKind.publish, hub.recent_events[0].kind);
    }

    {
        var duplicate_coordinator = try DB.open(alloc, std.mem.span(path), .{
            .start_index_workers = false,
            .ttl_cleanup = .{ .enabled = false },
        });
        defer duplicate_coordinator.close();

        const duplicate = try duplicate_coordinator.runGraphMetricPlannedCoordinatorStep("graph_idx", "hits_authority");
        try std.testing.expectEqual(graph_mod.GraphIndex.GraphMetricBuildPhase.cleanup_old_generations, duplicate.phase);
        try std.testing.expect(!duplicate.advanced_phase);
        try std.testing.expect(!duplicate.published);

        const graph_entry = duplicate_coordinator.core.graphIndex("graph_idx") orelse return error.IndexNotFound;
        var authority = try graph_entry.index.graphMetricStatus("hits_authority");
        defer authority.deinit(alloc);
        var hub = try graph_entry.index.graphMetricStatus("hits_hub");
        defer hub.deinit(alloc);
        try std.testing.expectEqual(graph_mod.GraphIndex.GraphMetricState.building, authority.state);
        try std.testing.expectEqual(graph_mod.GraphIndex.GraphMetricState.fresh, hub.state);
        try std.testing.expectEqual(graph_mod.GraphIndex.GraphMetricBuildPhase.cleanup_old_generations, authority.phase);
        try std.testing.expectEqual(graph_mod.GraphIndex.GraphMetricBuildPhase.complete, hub.phase);
        try std.testing.expectEqual(target_generation, authority.published_generation);
        try std.testing.expectEqual(authority.published_generation, hub.published_generation);
        try std.testing.expectEqual(@as(usize, 1), authority.recent_events.len);
        try std.testing.expectEqual(@as(usize, 1), hub.recent_events.len);
        try std.testing.expectEqual(graph_mod.GraphIndex.GraphMetricEventKind.publish, authority.recent_events[0].kind);
        try std.testing.expectEqual(graph_mod.GraphIndex.GraphMetricEventKind.publish, hub.recent_events[0].kind);
    }

    {
        var worker_db = try DB.open(alloc, std.mem.span(path), .{
            .start_index_workers = false,
            .ttl_cleanup = .{ .enabled = false },
        });
        defer worker_db.close();

        var cleanup_finished = false;
        for (0..12) |_| {
            const cleanup = try worker_db.runGraphMetricPlannedWorkerPageStep("graph_idx", "hits_authority", "db-hits-publish-race-cleaner");
            try std.testing.expectEqual(graph_mod.GraphIndex.GraphMetricBuildPhase.cleanup_old_generations, cleanup.phase);
            try std.testing.expect(!cleanup.advanced_phase);
            if (cleanup.published) {
                cleanup_finished = true;
                break;
            }
        }
        try std.testing.expect(cleanup_finished);
    }

    {
        var reader = try DB.open(alloc, std.mem.span(path), .{
            .start_index_workers = false,
            .ttl_cleanup = .{ .enabled = false },
        });
        defer reader.close();

        const graph_entry = reader.core.graphIndex("graph_idx") orelse return error.IndexNotFound;
        var authority = try graph_entry.index.graphMetricStatus("hits_authority");
        defer authority.deinit(alloc);
        var hub = try graph_entry.index.graphMetricStatus("hits_hub");
        defer hub.deinit(alloc);
        try std.testing.expectEqual(graph_mod.GraphIndex.GraphMetricState.fresh, authority.state);
        try std.testing.expectEqual(graph_mod.GraphIndex.GraphMetricState.fresh, hub.state);
        try std.testing.expectEqual(graph_mod.GraphIndex.GraphMetricBuildPhase.complete, authority.phase);
        try std.testing.expectEqual(graph_mod.GraphIndex.GraphMetricBuildPhase.complete, hub.phase);
        try std.testing.expectEqual(target_generation, authority.published_generation);
        try std.testing.expectEqual(authority.published_generation, hub.published_generation);
        try std.testing.expectEqual(@as(usize, 1), authority.recent_events.len);
        try std.testing.expectEqual(@as(usize, 1), hub.recent_events.len);
        try std.testing.expectEqual(graph_mod.GraphIndex.GraphMetricEventKind.publish, authority.recent_events[0].kind);
        try std.testing.expectEqual(graph_mod.GraphIndex.GraphMetricEventKind.publish, hub.recent_events[0].kind);

        var authority_result = try reader.search(alloc, .{
            .graph_metric_queries = &.{.{
                .name = "authority",
                .query = .{
                    .index_name = "graph_idx",
                    .metric_name = "hits_authority",
                    .top_k = 2,
                    .freshness = .fresh,
                },
            }},
            .limit = 0,
        });
        defer authority_result.deinit();
        try std.testing.expectEqual(@as(usize, 1), authority_result.graph_metric_results.len);
        try std.testing.expectEqual(graph_mod.GraphIndex.GraphMetricState.fresh, authority_result.graph_metric_results[0].status.state);
        try std.testing.expectEqual(target_generation, authority_result.graph_metric_results[0].status.published_generation);
        try std.testing.expectEqual(@as(usize, 2), authority_result.graph_metric_results[0].scores.len);

        var hub_result = try reader.search(alloc, .{
            .graph_metric_queries = &.{.{
                .name = "hub",
                .query = .{
                    .index_name = "graph_idx",
                    .metric_name = "hits_hub",
                    .top_k = 2,
                    .freshness = .fresh,
                },
            }},
            .limit = 0,
        });
        defer hub_result.deinit();
        try std.testing.expectEqual(@as(usize, 1), hub_result.graph_metric_results.len);
        try std.testing.expectEqual(graph_mod.GraphIndex.GraphMetricState.fresh, hub_result.graph_metric_results[0].status.state);
        try std.testing.expectEqual(target_generation, hub_result.graph_metric_results[0].status.published_generation);
        try std.testing.expectEqual(@as(usize, 2), hub_result.graph_metric_results[0].scores.len);
    }
}

test "db graph metric planned maintenance drains background pagerank work" {
    const alloc = std.testing.allocator;

    var path_buf: [256]u8 = undefined;
    const path = tempPath(&path_buf);
    defer cleanupTempDir(path);

    var db = try DB.open(alloc, std.mem.span(path), .{
        .start_index_workers = false,
        .ttl_cleanup = .{ .enabled = false },
    });
    defer db.close();

    try db.addIndex(.{
        .name = "graph_idx",
        .kind = .graph,
        .config_json = "{\"metrics\":{\"pagerank\":{\"enabled\":true,\"kind\":\"pagerank\",\"refresh\":\"background\",\"max_iterations\":2,\"tolerance\":0.000000001,\"edge_filter\":{\"types\":[\"cites\"]}}}}",
    });

    try db.batch(.{
        .writes = &.{
            .{ .key = "doc:a", .value = "{\"title\":\"alpha\",\"_edges\":{\"graph_idx\":{\"cites\":[{\"target\":\"doc:b\",\"weight\":1.0}]}}}" },
            .{ .key = "doc:b", .value = "{\"title\":\"beta\",\"_edges\":{\"graph_idx\":{\"cites\":[{\"target\":\"doc:d\",\"weight\":1.0}]}}}" },
            .{ .key = "doc:c", .value = "{\"title\":\"gamma\",\"_edges\":{\"graph_idx\":{\"cites\":[{\"target\":\"doc:b\",\"weight\":1.0}]}}}" },
            .{ .key = "doc:d", .value = "{\"title\":\"delta\"}" },
        },
        .sync_level = .write,
    });

    try db.runDerivedUntil(db.core.nextDerivedSequence());

    const target_generation = blk: {
        const graph_entry = db.core.graphIndex("graph_idx") orelse return error.IndexNotFound;
        var status = try graph_entry.index.graphMetricStatus("pagerank");
        defer status.deinit(alloc);
        try std.testing.expectEqual(graph_mod.GraphIndex.GraphMetricState.not_ready, status.state);
        break :blk graph_entry.index.edge_generation;
    };

    const maintenance = try db.runGraphMetricPlannedMaintenanceForIdle(.{
        .worker_id = "planned-maintenance-pagerank",
        .max_rounds = 200,
        .max_metrics_per_round = 8,
        .max_pages_per_round = 1,
    });
    try std.testing.expectEqual(@as(usize, 1), maintenance.builds_started);
    try std.testing.expect(maintenance.pages_completed > 0);
    try std.testing.expect(maintenance.phases_advanced > 0);

    var published_result = try db.search(alloc, .{
        .graph_metric_queries = &.{.{
            .name = "central",
            .query = .{
                .index_name = "graph_idx",
                .metric_name = "pagerank",
                .top_k = 2,
                .freshness = .fresh,
            },
        }},
        .limit = 0,
    });
    defer published_result.deinit();
    try std.testing.expectEqual(@as(usize, 1), published_result.graph_metric_results.len);
    try std.testing.expectEqual(graph_mod.GraphIndex.GraphMetricState.fresh, published_result.graph_metric_results[0].status.state);
    try std.testing.expectEqual(target_generation, published_result.graph_metric_results[0].status.published_generation);
    try std.testing.expectEqual(@as(usize, 2), published_result.graph_metric_results[0].scores.len);
    try std.testing.expectEqualStrings("doc:d", published_result.graph_metric_results[0].scores[0].node);
    try std.testing.expect(published_result.graph_metric_results[0].scores[0].score >= published_result.graph_metric_results[0].scores[1].score);
}

test "db graph metric runtime drains background pagerank through planned maintenance" {
    const alloc = std.testing.allocator;

    var path_buf: [256]u8 = undefined;
    const path = tempPath(&path_buf);
    defer cleanupTempDir(path);

    var db = try DB.open(alloc, std.mem.span(path), .{
        .start_index_workers = false,
        .ttl_cleanup = .{ .enabled = false },
    });
    defer db.close();

    try db.addIndex(.{
        .name = "graph_idx",
        .kind = .graph,
        .config_json = "{\"metrics\":{\"pagerank\":{\"enabled\":true,\"kind\":\"pagerank\",\"refresh\":\"background\",\"max_iterations\":2,\"tolerance\":0.000000001,\"edge_filter\":{\"types\":[\"cites\"]}}}}",
    });

    try db.batch(.{
        .writes = &.{
            .{ .key = "doc:a", .value = "{\"title\":\"alpha\",\"_edges\":{\"graph_idx\":{\"cites\":[{\"target\":\"doc:b\",\"weight\":1.0}]}}}" },
            .{ .key = "doc:b", .value = "{\"title\":\"beta\",\"_edges\":{\"graph_idx\":{\"cites\":[{\"target\":\"doc:d\",\"weight\":1.0}]}}}" },
            .{ .key = "doc:c", .value = "{\"title\":\"gamma\",\"_edges\":{\"graph_idx\":{\"cites\":[{\"target\":\"doc:b\",\"weight\":1.0}]}}}" },
            .{ .key = "doc:d", .value = "{\"title\":\"delta\"}" },
        },
        .sync_level = .write,
    });

    try db.runDerivedUntil(db.core.nextDerivedSequence());

    const target_generation = blk: {
        const graph_entry = db.core.graphIndex("graph_idx") orelse return error.IndexNotFound;
        var status = try graph_entry.index.graphMetricStatus("pagerank");
        defer status.deinit(alloc);
        try std.testing.expectEqual(graph_mod.GraphIndex.GraphMetricState.not_ready, status.state);
        break :blk graph_entry.index.edge_generation;
    };

    const resources = db.core.asyncResources();
    var runtime = try graph_metric_runtime_mod.GraphMetricRuntime.init(
        alloc,
        resources.store,
        resources.index_manager,
        resources.apply_mutex,
        db.backend_runtime,
        .{
            .enabled = true,
            .runtime_id = "runtime-pagerank-combined",
            .planned_options = .{
                .worker_id = "runtime-pagerank-worker",
                .max_rounds = 1,
                .max_metrics_per_round = 8,
                .max_pages_per_round = 1,
            },
        },
    );
    defer runtime.deinit();

    {
        const initial_stats = runtime.stats();
        try std.testing.expect(initial_stats.enabled);
        try std.testing.expectEqual(graph_metric_runtime_mod.Role.combined, initial_stats.role);
        try std.testing.expectEqual(std.hash.Wyhash.hash(0, "runtime-pagerank-combined"), initial_stats.runtime_id_hash);
        try std.testing.expectEqual(std.hash.Wyhash.hash(0, "local"), initial_stats.owner_id_hash);
        try std.testing.expect(initial_stats.lease_key_hash != 0);
        try std.testing.expectEqual(std.hash.Wyhash.hash(0, "runtime-pagerank-worker"), initial_stats.worker_id_hash);
        try std.testing.expectEqual(@as(usize, 1), initial_stats.worker_count);
        try std.testing.expectEqual(@as(u64, 0), initial_stats.ticks_started);
        try std.testing.expectEqual(@as(u64, 0), initial_stats.ticks_completed);
        try std.testing.expectEqual(@as(u64, 0), initial_stats.durable_progress_ticks);
        try std.testing.expectEqual(@as(?[]const u8, null), initial_stats.last_error_name);
    }

    var steps: usize = 0;
    while (try runtime.runOnce()) {
        steps += 1;
        if (steps > 200) return error.TestUnexpectedResult;
    }
    try std.testing.expect(steps > 0);

    {
        const drained_stats = runtime.stats();
        try std.testing.expectEqual(@as(u64, @intCast(steps + 1)), drained_stats.ticks_started);
        try std.testing.expectEqual(drained_stats.ticks_started, drained_stats.ticks_completed);
        try std.testing.expectEqual(@as(u64, @intCast(steps)), drained_stats.durable_progress_ticks);
        try std.testing.expectEqual(@as(u64, 1), drained_stats.idle_ticks);
        try std.testing.expectEqual(@as(u64, 0), drained_stats.error_ticks);
        try std.testing.expectEqual(@as(?[]const u8, null), drained_stats.last_error_name);
        try std.testing.expect(!drained_stats.last_result.durableProgressed());
    }

    db.graph_metric_runtime = &runtime;
    defer db.graph_metric_runtime = null;
    {
        const mapped_stats = try db.stats(alloc);
        defer types.freeDBStats(alloc, mapped_stats);
        try std.testing.expect(mapped_stats.graph_metric_runtime.enabled);
        try std.testing.expectEqual(types.GraphMetricRuntimeRole.combined, mapped_stats.graph_metric_runtime.role.?);
        try std.testing.expectEqual(std.hash.Wyhash.hash(0, "runtime-pagerank-combined"), mapped_stats.graph_metric_runtime.runtime_id_hash);
        try std.testing.expectEqual(std.hash.Wyhash.hash(0, "local"), mapped_stats.graph_metric_runtime.owner_id_hash);
        try std.testing.expectEqual(runtime.stats().lease_key_hash, mapped_stats.graph_metric_runtime.lease_key_hash);
        try std.testing.expectEqual(std.hash.Wyhash.hash(0, "runtime-pagerank-worker"), mapped_stats.graph_metric_runtime.worker_id_hash);
        try std.testing.expectEqual(@as(u64, 1), mapped_stats.graph_metric_runtime.worker_count);
        try std.testing.expectEqual(@as(u64, @intCast(steps + 1)), mapped_stats.graph_metric_runtime.ticks_started);
        try std.testing.expectEqual(mapped_stats.graph_metric_runtime.ticks_started, mapped_stats.graph_metric_runtime.ticks_completed);
        try std.testing.expectEqual(@as(u64, @intCast(steps)), mapped_stats.graph_metric_runtime.durable_progress_ticks);
        try std.testing.expectEqual(@as(u64, 1), mapped_stats.graph_metric_runtime.idle_ticks);
        try std.testing.expectEqual(@as(u64, 0), mapped_stats.graph_metric_runtime.error_ticks);
        try std.testing.expectEqual(@as(u64, 0), mapped_stats.graph_metric_runtime.last_builds_started);
        try std.testing.expect(!mapped_stats.graph_metric_runtime.last_budget_exhausted);
    }

    {
        const pending = db.pendingWorkStats().graph_metric;
        try std.testing.expect(!pending.hasWork());
    }

    var published_result = try db.search(alloc, .{
        .graph_metric_queries = &.{.{
            .name = "central",
            .query = .{
                .index_name = "graph_idx",
                .metric_name = "pagerank",
                .top_k = 2,
                .freshness = .fresh,
            },
        }},
        .limit = 0,
    });
    defer published_result.deinit();
    try std.testing.expectEqual(@as(usize, 1), published_result.graph_metric_results.len);
    try std.testing.expectEqual(graph_mod.GraphIndex.GraphMetricState.fresh, published_result.graph_metric_results[0].status.state);
    try std.testing.expectEqual(target_generation, published_result.graph_metric_results[0].status.published_generation);
    try std.testing.expectEqual(@as(usize, 2), published_result.graph_metric_results[0].scores.len);
    try std.testing.expectEqualStrings("doc:d", published_result.graph_metric_results[0].scores[0].node);
}

test "db graph metric runtime roles separate automatic coordinator and worker loops" {
    const alloc = std.testing.allocator;

    var path_buf: [256]u8 = undefined;
    const path = tempPath(&path_buf);
    defer cleanupTempDir(path);

    var db = try DB.open(alloc, std.mem.span(path), .{
        .start_index_workers = false,
        .ttl_cleanup = .{ .enabled = false },
    });
    defer db.close();

    try db.addIndex(.{
        .name = "graph_idx",
        .kind = .graph,
        .config_json = "{\"metrics\":{\"degree\":{\"enabled\":true,\"kind\":\"degree\",\"refresh\":\"background\",\"edge_filter\":{\"types\":[\"cites\"]}}}}",
    });

    try db.batch(.{
        .writes = &.{
            .{ .key = "doc:a", .value = "{\"title\":\"alpha\",\"_edges\":{\"graph_idx\":{\"cites\":[{\"target\":\"doc:b\",\"weight\":1.0}]}}}" },
            .{ .key = "doc:b", .value = "{\"title\":\"beta\"}" },
        },
        .sync_level = .write,
    });

    try db.runDerivedUntil(db.core.nextDerivedSequence());

    const resources = db.core.asyncResources();
    var coordinator_runtime = try graph_metric_runtime_mod.GraphMetricRuntime.init(
        alloc,
        resources.store,
        resources.index_manager,
        resources.apply_mutex,
        db.backend_runtime,
        .{
            .enabled = true,
            .role = .coordinator,
            .runtime_id = "runtime-role-coordinator-owner",
            .planned_options = .{
                .worker_id = "runtime-role-coordinator",
                .max_rounds = 1,
                .max_metrics_per_round = 8,
                .max_pages_per_round = 1,
            },
        },
    );
    defer coordinator_runtime.deinit();

    var worker_runtime = try graph_metric_runtime_mod.GraphMetricRuntime.init(
        alloc,
        resources.store,
        resources.index_manager,
        resources.apply_mutex,
        db.backend_runtime,
        .{
            .enabled = true,
            .role = .worker,
            .runtime_id = "runtime-role-worker-owner",
            .planned_options = .{
                .worker_id = "runtime-role-worker",
                .max_rounds = 1,
                .max_metrics_per_round = 8,
                .max_pages_per_round = 1,
            },
        },
    );
    defer worker_runtime.deinit();

    const early_worker = try worker_runtime.runOnceDetailed();
    try std.testing.expect(!early_worker.durableProgressed());
    try std.testing.expectEqual(@as(usize, 0), early_worker.builds_started);
    try std.testing.expectEqual(@as(usize, 0), early_worker.worker_steps);

    const coordinator_start = try coordinator_runtime.runOnceDetailed();
    try std.testing.expectEqual(@as(usize, 1), coordinator_start.builds_started);
    try std.testing.expectEqual(@as(usize, 0), coordinator_start.worker_steps);
    try std.testing.expectEqual(@as(usize, 0), coordinator_start.pages_completed);

    const worker_prepare = try worker_runtime.runOnceDetailed();
    try std.testing.expectEqual(@as(usize, 1), worker_prepare.worker_steps);
    try std.testing.expectEqual(@as(usize, 1), worker_prepare.pages_completed);
    try std.testing.expectEqual(@as(usize, 0), worker_prepare.phases_advanced);
    try std.testing.expectEqual(@as(usize, 0), worker_prepare.published);

    {
        const graph_entry = db.core.graphIndex("graph_idx") orelse return error.IndexNotFound;
        var status = try graph_entry.index.graphMetricStatus("degree");
        defer status.deinit(alloc);
        try std.testing.expectEqual(graph_mod.GraphIndex.GraphMetricState.building, status.state);
        try std.testing.expectEqual(graph_mod.GraphIndex.GraphMetricBuildPhase.prepare_generation, status.phase);
    }

    const coordinator_advance = try coordinator_runtime.runOnceDetailed();
    try std.testing.expectEqual(@as(usize, 0), coordinator_advance.builds_started);
    try std.testing.expect(coordinator_advance.phases_advanced > 0);

    var finished = false;
    var steps: usize = 0;
    while (steps < 200) : (steps += 1) {
        const worker_tick = try worker_runtime.runOnceDetailed();
        try std.testing.expectEqual(@as(usize, 0), worker_tick.phases_advanced);

        const coordinator_tick = try coordinator_runtime.runOnceDetailed();
        const graph_entry = db.core.graphIndex("graph_idx") orelse return error.IndexNotFound;
        var status = try graph_entry.index.graphMetricStatus("degree");
        defer status.deinit(alloc);
        if (status.state == .fresh and status.phase == .complete) {
            finished = true;
            break;
        }
        if (!worker_tick.durableProgressed() and !coordinator_tick.durableProgressed()) {
            return error.GraphMetricBuildNoEligiblePage;
        }
    }
    try std.testing.expect(finished);

    {
        const coordinator_stats = coordinator_runtime.stats();
        try std.testing.expectEqual(graph_metric_runtime_mod.Role.coordinator, coordinator_stats.role);
        try std.testing.expectEqual(std.hash.Wyhash.hash(0, "runtime-role-coordinator-owner"), coordinator_stats.runtime_id_hash);
        try std.testing.expectEqual(@as(u64, 0), coordinator_stats.worker_id_hash);
        try std.testing.expectEqual(@as(usize, 0), coordinator_stats.worker_count);
        try std.testing.expect(coordinator_stats.durable_progress_ticks > 0);
        try std.testing.expect(coordinator_stats.total_result.builds_started > 0);
        try std.testing.expect(coordinator_stats.total_result.coordinator_steps > 0);
        try std.testing.expect(coordinator_stats.total_result.phases_advanced > 0);
        try std.testing.expectEqual(@as(usize, 0), coordinator_stats.total_result.worker_steps);
        try std.testing.expect(coordinator_stats.last_result.worker_steps == 0);
    }
    {
        const worker_stats = worker_runtime.stats();
        try std.testing.expectEqual(graph_metric_runtime_mod.Role.worker, worker_stats.role);
        try std.testing.expectEqual(std.hash.Wyhash.hash(0, "runtime-role-worker-owner"), worker_stats.runtime_id_hash);
        try std.testing.expectEqual(std.hash.Wyhash.hash(0, "runtime-role-worker"), worker_stats.worker_id_hash);
        try std.testing.expectEqual(@as(usize, 1), worker_stats.worker_count);
        try std.testing.expect(worker_stats.durable_progress_ticks > 0);
        try std.testing.expect(worker_stats.total_result.worker_steps > 0);
        try std.testing.expect(worker_stats.total_result.pages_completed > 0);
        try std.testing.expectEqual(@as(usize, 0), worker_stats.total_result.coordinator_steps);
        try std.testing.expect(worker_stats.last_result.coordinator_steps == 0);
    }

    var metric_result = try db.search(alloc, .{
        .graph_metric_queries = &.{.{
            .name = "degree",
            .query = .{
                .index_name = "graph_idx",
                .metric_name = "degree",
                .top_k = 1,
                .freshness = .fresh,
            },
        }},
        .limit = 0,
    });
    defer metric_result.deinit();
    try std.testing.expectEqual(@as(usize, 1), metric_result.graph_metric_results.len);
    try std.testing.expectEqual(graph_mod.GraphIndex.GraphMetricState.fresh, metric_result.graph_metric_results[0].status.state);
    try std.testing.expectEqualStrings("doc:a", metric_result.graph_metric_results[0].scores[0].node);
}

test "db graph metric runtime distinct worker owners complete separate active pages" {
    const alloc = std.testing.allocator;

    var path_buf: [256]u8 = undefined;
    const path = tempPath(&path_buf);
    defer cleanupTempDir(path);

    var db = try DB.open(alloc, std.mem.span(path), .{
        .start_index_workers = false,
        .ttl_cleanup = .{ .enabled = false },
    });
    defer db.close();

    try db.addIndex(.{
        .name = "graph_idx",
        .kind = .graph,
        .config_json = "{\"metrics\":{\"degree\":{\"enabled\":true,\"kind\":\"degree\",\"refresh\":\"background\",\"edge_filter\":{\"types\":[\"cites\"]}}}}",
    });

    try db.batch(.{
        .writes = &.{.{ .key = "doc:hub", .value = "{\"title\":\"hub\"}" }},
        .sync_level = .write,
    });

    for (0..130) |i| {
        const key = try std.fmt.allocPrint(alloc, "doc:{d:0>3}", .{i});
        defer alloc.free(key);
        const value = try std.fmt.allocPrint(
            alloc,
            "{{\"title\":\"source {d}\",\"_edges\":{{\"graph_idx\":{{\"cites\":[{{\"target\":\"doc:hub\",\"weight\":1.0}}]}}}}}}",
            .{i},
        );
        defer alloc.free(value);
        try db.batch(.{
            .writes = &.{.{ .key = key, .value = value }},
            .sync_level = .write,
        });
    }

    try db.runDerivedUntil(db.core.nextDerivedSequence());

    const target_generation = blk: {
        const graph_entry = db.core.graphIndex("graph_idx") orelse return error.IndexNotFound;
        break :blk graph_entry.index.edge_generation;
    };

    const resources = db.core.asyncResources();
    var manual_clock = platform_clock.ManualClock{};
    manual_clock.setRealtimeNs(6_000 * std.time.ns_per_ms);
    var coordinator_runtime = try graph_metric_runtime_mod.GraphMetricRuntime.init(
        alloc,
        resources.store,
        resources.index_manager,
        resources.apply_mutex,
        db.backend_runtime,
        .{
            .enabled = true,
            .role = .coordinator,
            .runtime_id = "runtime-distinct-workers-coordinator",
            .lease_owned = true,
            .owner_id = "runtime-distinct-workers-coordinator",
            .lease_ttl_ms = 100,
            .clock = manual_clock.clock(),
            .planned_options = .{
                .worker_id = "runtime-distinct-workers-coordinator-unused",
                .max_rounds = 1,
                .max_metrics_per_round = 8,
                .max_pages_per_round = 1,
            },
        },
    );
    defer coordinator_runtime.deinit();

    var worker_a_runtime = try graph_metric_runtime_mod.GraphMetricRuntime.init(
        alloc,
        resources.store,
        resources.index_manager,
        resources.apply_mutex,
        db.backend_runtime,
        .{
            .enabled = true,
            .role = .worker,
            .runtime_id = "runtime-distinct-worker-a",
            .lease_owned = true,
            .owner_id = "runtime-distinct-worker-a",
            .lease_ttl_ms = 100,
            .clock = manual_clock.clock(),
            .planned_options = .{
                .worker_id = "runtime-distinct-worker-a",
                .max_rounds = 1,
                .max_metrics_per_round = 8,
                .max_pages_per_round = 1,
            },
        },
    );
    defer worker_a_runtime.deinit();

    var worker_b_runtime = try graph_metric_runtime_mod.GraphMetricRuntime.init(
        alloc,
        resources.store,
        resources.index_manager,
        resources.apply_mutex,
        db.backend_runtime,
        .{
            .enabled = true,
            .role = .worker,
            .runtime_id = "runtime-distinct-worker-b",
            .lease_owned = true,
            .owner_id = "runtime-distinct-worker-b",
            .lease_ttl_ms = 100,
            .clock = manual_clock.clock(),
            .planned_options = .{
                .worker_id = "runtime-distinct-worker-b",
                .max_rounds = 1,
                .max_metrics_per_round = 8,
                .max_pages_per_round = 1,
            },
        },
    );
    defer worker_b_runtime.deinit();

    var replacement_worker_a_runtime = try graph_metric_runtime_mod.GraphMetricRuntime.init(
        alloc,
        resources.store,
        resources.index_manager,
        resources.apply_mutex,
        db.backend_runtime,
        .{
            .enabled = true,
            .role = .worker,
            .runtime_id = "runtime-distinct-worker-a-replacement",
            .lease_owned = true,
            .owner_id = "runtime-distinct-worker-a-replacement",
            .lease_ttl_ms = 100,
            .clock = manual_clock.clock(),
            .planned_options = .{
                .worker_id = "runtime-distinct-worker-a",
                .max_rounds = 1,
                .max_metrics_per_round = 8,
                .max_pages_per_round = 1,
            },
        },
    );
    defer replacement_worker_a_runtime.deinit();

    const coordinator_start = try coordinator_runtime.runOnceDetailed();
    try std.testing.expectEqual(@as(usize, 1), coordinator_start.builds_started);
    try std.testing.expectEqual(@as(usize, 0), coordinator_start.worker_steps);

    const worker_a_prepare = try worker_a_runtime.runOnceDetailed();
    try std.testing.expectEqual(@as(usize, 1), worker_a_prepare.worker_steps);
    try std.testing.expectEqual(@as(usize, 1), worker_a_prepare.pages_completed);
    try std.testing.expectEqual(@as(usize, 0), worker_a_prepare.phases_advanced);

    const coordinator_scan = try coordinator_runtime.runOnceDetailed();
    try std.testing.expect(coordinator_scan.phases_advanced > 0);
    {
        const graph_entry = db.core.graphIndex("graph_idx") orelse return error.IndexNotFound;
        var status = try graph_entry.index.graphMetricStatus("degree");
        defer status.deinit(alloc);
        try std.testing.expectEqual(graph_mod.GraphIndex.GraphMetricBuildPhase.scan_edges_and_out_degree, status.phase);
    }

    const replacement_blocked = try replacement_worker_a_runtime.runOnceDetailed();
    try std.testing.expect(!replacement_blocked.durableProgressed());
    try std.testing.expectEqual(@as(usize, 0), replacement_blocked.worker_steps);
    try std.testing.expectEqual(@as(usize, 0), replacement_blocked.pages_completed);
    {
        const stats = replacement_worker_a_runtime.stats();
        try std.testing.expect(stats.lease_owned);
        try std.testing.expect(!stats.has_lease);
        try std.testing.expectEqual(worker_a_runtime.stats().lease_key_hash, stats.lease_key_hash);
        try std.testing.expectEqual(worker_a_runtime.stats().worker_id_hash, stats.worker_id_hash);
        try std.testing.expectEqual(@as(u64, 1), stats.lease_acquire_failures);
    }

    manual_clock.advanceMs(101);
    const replacement_scan = try replacement_worker_a_runtime.runOnceDetailed();
    try std.testing.expectEqual(@as(usize, 1), replacement_scan.worker_steps);
    try std.testing.expectEqual(@as(usize, 1), replacement_scan.pages_completed);
    try std.testing.expectEqual(@as(usize, 0), replacement_scan.phases_advanced);
    try std.testing.expect(replacement_scan.budget_exhausted);
    {
        const stats = replacement_worker_a_runtime.stats();
        try std.testing.expect(stats.has_lease);
        try std.testing.expectEqual(@as(u64, 1), stats.acquisition_count);
        try std.testing.expectEqual(@as(u64, 6_101), stats.last_acquired_ms);
        try std.testing.expect(stats.last_result.budget_exhausted);
    }

    const worker_a_lost = try worker_a_runtime.runOnceDetailed();
    try std.testing.expect(!worker_a_lost.durableProgressed());
    try std.testing.expectEqual(@as(usize, 0), worker_a_lost.worker_steps);
    {
        const stats = worker_a_runtime.stats();
        try std.testing.expect(!stats.has_lease);
        try std.testing.expectEqual(@as(u64, 1), stats.lost_leases);
        try std.testing.expectEqual(@as(u64, 1), stats.lease_acquire_failures);
    }

    const worker_b_scan = try worker_b_runtime.runOnceDetailed();
    try std.testing.expectEqual(@as(usize, 1), worker_b_scan.worker_steps);
    try std.testing.expectEqual(@as(usize, 1), worker_b_scan.pages_completed);
    try std.testing.expectEqual(@as(usize, 0), worker_b_scan.phases_advanced);

    {
        const worker_a_stats = worker_a_runtime.stats();
        const worker_b_stats = worker_b_runtime.stats();
        try std.testing.expect(worker_a_stats.lease_owned);
        try std.testing.expect(worker_b_stats.lease_owned);
        try std.testing.expect(!worker_a_stats.has_lease);
        try std.testing.expect(worker_b_stats.has_lease);
        try std.testing.expect(worker_a_stats.lease_key_hash != 0);
        try std.testing.expect(worker_b_stats.lease_key_hash != 0);
        try std.testing.expect(worker_a_stats.lease_key_hash != worker_b_stats.lease_key_hash);
        try std.testing.expect(worker_a_stats.worker_id_hash != worker_b_stats.worker_id_hash);
        try std.testing.expectEqual(@as(u64, 1), worker_a_stats.lease_acquire_failures);
        try std.testing.expectEqual(@as(u64, 0), worker_b_stats.lease_acquire_failures);
        try std.testing.expect(worker_a_stats.total_result.pages_completed >= 1);
        try std.testing.expect(worker_b_stats.total_result.pages_completed >= 1);
        const replacement_stats = replacement_worker_a_runtime.stats();
        try std.testing.expect(replacement_stats.has_lease);
        try std.testing.expectEqual(worker_a_stats.lease_key_hash, replacement_stats.lease_key_hash);
        try std.testing.expectEqual(worker_a_stats.worker_id_hash, replacement_stats.worker_id_hash);
        try std.testing.expect(replacement_stats.total_result.pages_completed >= 1);
    }

    var finished = false;
    var steps: usize = 0;
    while (steps < 200) : (steps += 1) {
        const worker_tick = if (steps % 2 == 0)
            try replacement_worker_a_runtime.runOnceDetailed()
        else
            try worker_b_runtime.runOnceDetailed();
        try std.testing.expectEqual(@as(usize, 0), worker_tick.phases_advanced);

        const coordinator_tick = try coordinator_runtime.runOnceDetailed();
        const graph_entry = db.core.graphIndex("graph_idx") orelse return error.IndexNotFound;
        var status = try graph_entry.index.graphMetricStatus("degree");
        defer status.deinit(alloc);
        if (status.state == .fresh and status.published_generation == target_generation and status.phase == .complete) {
            finished = true;
            break;
        }
        if (!worker_tick.durableProgressed() and !coordinator_tick.durableProgressed()) {
            return error.GraphMetricBuildNoEligiblePage;
        }
    }
    try std.testing.expect(finished);

    var metric_result = try db.search(alloc, .{
        .graph_metric_queries = &.{.{
            .name = "degree",
            .query = .{
                .index_name = "graph_idx",
                .metric_name = "degree",
                .top_k = 1,
                .freshness = .fresh,
            },
        }},
        .limit = 0,
    });
    defer metric_result.deinit();
    try std.testing.expectEqual(@as(usize, 1), metric_result.graph_metric_results.len);
    try std.testing.expectEqual(graph_mod.GraphIndex.GraphMetricState.fresh, metric_result.graph_metric_results[0].status.state);
    try std.testing.expectEqual(target_generation, metric_result.graph_metric_results[0].status.published_generation);
    try std.testing.expectEqualStrings("doc:hub", metric_result.graph_metric_results[0].scores[0].node);
}

test "db graph metric runtime background coordinator and worker loops publish degree" {
    const alloc = std.testing.allocator;

    var path_buf: [256]u8 = undefined;
    const path = tempPath(&path_buf);
    defer cleanupTempDir(path);

    var db = try DB.open(alloc, std.mem.span(path), .{
        .start_index_workers = false,
        .ttl_cleanup = .{ .enabled = false },
    });
    defer db.close();

    try db.addIndex(.{
        .name = "graph_idx",
        .kind = .graph,
        .config_json = "{\"metrics\":{\"degree\":{\"enabled\":true,\"kind\":\"degree\",\"refresh\":\"background\",\"edge_filter\":{\"types\":[\"cites\"]}}}}",
    });

    try db.batch(.{
        .writes = &.{
            .{ .key = "doc:a", .value = "{\"title\":\"alpha\",\"_edges\":{\"graph_idx\":{\"cites\":[{\"target\":\"doc:b\",\"weight\":1.0}]}}}" },
            .{ .key = "doc:b", .value = "{\"title\":\"beta\"}" },
        },
        .sync_level = .write,
    });

    try db.runDerivedUntil(db.core.nextDerivedSequence());

    const target_generation = blk: {
        const graph_entry = db.core.graphIndex("graph_idx") orelse return error.IndexNotFound;
        break :blk graph_entry.index.edge_generation;
    };

    const resources = db.core.asyncResources();
    var coordinator_runtime = try graph_metric_runtime_mod.GraphMetricRuntime.init(
        alloc,
        resources.store,
        resources.index_manager,
        resources.apply_mutex,
        db.backend_runtime,
        .{
            .enabled = true,
            .role = .coordinator,
            .runtime_id = "runtime-bg-coordinator-owner",
            .idle_interval_ms = 1,
            .error_interval_ms = 1,
            .planned_options = .{
                .worker_id = "runtime-bg-coordinator-unused",
                .max_rounds = 1,
                .max_metrics_per_round = 8,
                .max_pages_per_round = 1,
            },
        },
    );
    defer coordinator_runtime.deinit();

    var worker_runtime = try graph_metric_runtime_mod.GraphMetricRuntime.init(
        alloc,
        resources.store,
        resources.index_manager,
        resources.apply_mutex,
        db.backend_runtime,
        .{
            .enabled = true,
            .role = .worker,
            .runtime_id = "runtime-bg-worker-owner",
            .idle_interval_ms = 1,
            .error_interval_ms = 1,
            .planned_options = .{
                .worker_id = "runtime-bg-worker",
                .max_rounds = 1,
                .max_metrics_per_round = 8,
                .max_pages_per_round = 1,
            },
        },
    );
    defer worker_runtime.deinit();

    try coordinator_runtime.start();
    try worker_runtime.start();
    coordinator_runtime.notify();
    worker_runtime.notify();

    var fresh = false;
    for (0..300) |_| {
        yieldToBackground();
        const graph_entry = db.core.graphIndex("graph_idx") orelse return error.IndexNotFound;
        var status = try graph_entry.index.graphMetricStatus("degree");
        defer status.deinit(alloc);
        if (status.state == .fresh and status.published_generation == target_generation) {
            fresh = true;
            break;
        }
    }
    try std.testing.expect(fresh);

    {
        const coordinator_stats = coordinator_runtime.stats();
        try std.testing.expect(coordinator_stats.started);
        try std.testing.expectEqual(graph_metric_runtime_mod.Role.coordinator, coordinator_stats.role);
        try std.testing.expectEqual(std.hash.Wyhash.hash(0, "runtime-bg-coordinator-owner"), coordinator_stats.runtime_id_hash);
        try std.testing.expectEqual(@as(u64, 0), coordinator_stats.worker_id_hash);
        try std.testing.expectEqual(@as(usize, 0), coordinator_stats.worker_count);
        try std.testing.expect(coordinator_stats.ticks_started > 0);
        try std.testing.expect(coordinator_stats.durable_progress_ticks > 0);
        try std.testing.expectEqual(@as(u64, 0), coordinator_stats.error_ticks);
        try std.testing.expect(coordinator_stats.total_result.builds_started > 0);
        try std.testing.expect(coordinator_stats.total_result.coordinator_steps > 0);
        try std.testing.expect(coordinator_stats.total_result.phases_advanced > 0);
        try std.testing.expectEqual(@as(usize, 0), coordinator_stats.total_result.worker_steps);
        try std.testing.expect(coordinator_stats.last_result.worker_steps == 0);
    }
    {
        const worker_stats = worker_runtime.stats();
        try std.testing.expect(worker_stats.started);
        try std.testing.expectEqual(graph_metric_runtime_mod.Role.worker, worker_stats.role);
        try std.testing.expectEqual(std.hash.Wyhash.hash(0, "runtime-bg-worker-owner"), worker_stats.runtime_id_hash);
        try std.testing.expectEqual(std.hash.Wyhash.hash(0, "runtime-bg-worker"), worker_stats.worker_id_hash);
        try std.testing.expectEqual(@as(usize, 1), worker_stats.worker_count);
        try std.testing.expect(worker_stats.ticks_started > 0);
        try std.testing.expect(worker_stats.durable_progress_ticks > 0);
        try std.testing.expectEqual(@as(u64, 0), worker_stats.error_ticks);
        try std.testing.expect(worker_stats.total_result.worker_steps > 0);
        try std.testing.expect(worker_stats.total_result.pages_completed > 0);
        try std.testing.expectEqual(@as(usize, 0), worker_stats.total_result.coordinator_steps);
        try std.testing.expect(worker_stats.last_result.coordinator_steps == 0);
    }

    {
        const pending = db.pendingWorkStats().graph_metric;
        try std.testing.expect(!pending.hasWork());
    }

    var metric_result = try db.search(alloc, .{
        .graph_metric_queries = &.{.{
            .name = "degree",
            .query = .{
                .index_name = "graph_idx",
                .metric_name = "degree",
                .top_k = 1,
                .freshness = .fresh,
            },
        }},
        .limit = 0,
    });
    defer metric_result.deinit();
    try std.testing.expectEqual(@as(usize, 1), metric_result.graph_metric_results.len);
    try std.testing.expectEqual(graph_mod.GraphIndex.GraphMetricState.fresh, metric_result.graph_metric_results[0].status.state);
    try std.testing.expectEqual(target_generation, metric_result.graph_metric_results[0].status.published_generation);
    try std.testing.expectEqualStrings("doc:a", metric_result.graph_metric_results[0].scores[0].node);
    try std.testing.expectApproxEqAbs(@as(f64, 1.0), metric_result.graph_metric_results[0].scores[0].score, 0.001);
}

test "db graph metric runtime background coordinator and worker pool loops publish degree" {
    const alloc = std.testing.allocator;

    var path_buf: [256]u8 = undefined;
    const path = tempPath(&path_buf);
    defer cleanupTempDir(path);

    var db = try DB.open(alloc, std.mem.span(path), .{
        .start_index_workers = false,
        .ttl_cleanup = .{ .enabled = false },
    });
    defer db.close();

    try db.addIndex(.{
        .name = "graph_idx",
        .kind = .graph,
        .config_json = "{\"metrics\":{\"degree\":{\"enabled\":true,\"kind\":\"degree\",\"refresh\":\"background\",\"edge_filter\":{\"types\":[\"cites\"]}}}}",
    });

    try db.batch(.{
        .writes = &.{.{ .key = "doc:hub", .value = "{\"title\":\"hub\"}" }},
        .sync_level = .write,
    });

    for (0..130) |i| {
        const key = try std.fmt.allocPrint(alloc, "doc:{d:0>3}", .{i});
        defer alloc.free(key);
        const value = try std.fmt.allocPrint(
            alloc,
            "{{\"title\":\"source {d}\",\"_edges\":{{\"graph_idx\":{{\"cites\":[{{\"target\":\"doc:hub\",\"weight\":1.0}}]}}}}}}",
            .{i},
        );
        defer alloc.free(value);
        try db.batch(.{
            .writes = &.{.{ .key = key, .value = value }},
            .sync_level = .write,
        });
    }

    try db.runDerivedUntil(db.core.nextDerivedSequence());

    const target_generation = blk: {
        const graph_entry = db.core.graphIndex("graph_idx") orelse return error.IndexNotFound;
        break :blk graph_entry.index.edge_generation;
    };

    const resources = db.core.asyncResources();
    var coordinator_runtime = try graph_metric_runtime_mod.GraphMetricRuntime.init(
        alloc,
        resources.store,
        resources.index_manager,
        resources.apply_mutex,
        db.backend_runtime,
        .{
            .enabled = true,
            .role = .coordinator,
            .runtime_id = "runtime-bg-pool-coordinator-owner",
            .idle_interval_ms = 1,
            .error_interval_ms = 1,
            .planned_options = .{
                .worker_id = "runtime-bg-pool-coordinator-unused",
                .max_rounds = 1,
                .max_metrics_per_round = 8,
                .max_pages_per_round = 1,
            },
        },
    );
    defer coordinator_runtime.deinit();

    const workers = [_][]const u8{ "runtime-bg-pool-worker-a", "runtime-bg-pool-worker-b" };
    var worker_pool_runtime = try graph_metric_runtime_mod.GraphMetricRuntime.init(
        alloc,
        resources.store,
        resources.index_manager,
        resources.apply_mutex,
        db.backend_runtime,
        .{
            .enabled = true,
            .role = .worker_pool,
            .runtime_id = "runtime-bg-pool-worker-owner",
            .idle_interval_ms = 1,
            .error_interval_ms = 1,
            .planned_options = .{
                .worker_ids = &workers,
                .max_rounds = 1,
                .max_metrics_per_round = 8,
                .max_pages_per_round = 2,
            },
        },
    );
    defer worker_pool_runtime.deinit();

    try coordinator_runtime.start();
    try worker_pool_runtime.start();
    coordinator_runtime.notify();
    worker_pool_runtime.notify();

    var fresh = false;
    for (0..500) |_| {
        yieldToBackground();
        const graph_entry = db.core.graphIndex("graph_idx") orelse return error.IndexNotFound;
        var status = try graph_entry.index.graphMetricStatus("degree");
        defer status.deinit(alloc);
        if (status.state == .fresh and status.published_generation == target_generation) {
            fresh = true;
            break;
        }
    }
    try std.testing.expect(fresh);

    {
        const coordinator_stats = coordinator_runtime.stats();
        try std.testing.expect(coordinator_stats.started);
        try std.testing.expectEqual(graph_metric_runtime_mod.Role.coordinator, coordinator_stats.role);
        try std.testing.expectEqual(std.hash.Wyhash.hash(0, "runtime-bg-pool-coordinator-owner"), coordinator_stats.runtime_id_hash);
        try std.testing.expectEqual(@as(u64, 0), coordinator_stats.worker_id_hash);
        try std.testing.expectEqual(@as(usize, 0), coordinator_stats.worker_count);
        try std.testing.expect(coordinator_stats.ticks_started > 0);
        try std.testing.expect(coordinator_stats.durable_progress_ticks > 0);
        try std.testing.expectEqual(@as(u64, 0), coordinator_stats.error_ticks);
        try std.testing.expect(coordinator_stats.total_result.builds_started > 0);
        try std.testing.expect(coordinator_stats.total_result.coordinator_steps > 0);
        try std.testing.expect(coordinator_stats.total_result.phases_advanced > 0);
        try std.testing.expectEqual(@as(usize, 0), coordinator_stats.total_result.worker_steps);
        try std.testing.expect(coordinator_stats.last_result.worker_steps == 0);
    }
    {
        const worker_stats = worker_pool_runtime.stats();
        try std.testing.expect(worker_stats.started);
        try std.testing.expectEqual(graph_metric_runtime_mod.Role.worker_pool, worker_stats.role);
        try std.testing.expectEqual(std.hash.Wyhash.hash(0, "runtime-bg-pool-worker-owner"), worker_stats.runtime_id_hash);
        const expected_worker_hash = graph_metric_runtime_mod.workerSetIdentityHash(workers[0..]);
        try std.testing.expectEqual(expected_worker_hash, worker_stats.worker_id_hash);
        try std.testing.expectEqual(@as(usize, 2), worker_stats.worker_count);
        try std.testing.expect(worker_stats.ticks_started > 0);
        try std.testing.expect(worker_stats.durable_progress_ticks > 0);
        try std.testing.expectEqual(@as(u64, 0), worker_stats.error_ticks);
        try std.testing.expect(worker_stats.total_result.worker_steps >= 2);
        try std.testing.expect(worker_stats.total_result.pages_completed >= 2);
        try std.testing.expectEqual(@as(usize, 0), worker_stats.total_result.coordinator_steps);
        try std.testing.expect(worker_stats.last_result.coordinator_steps == 0);
    }

    {
        const pending = db.pendingWorkStats().graph_metric;
        try std.testing.expect(!pending.hasWork());
    }

    var metric_result = try db.search(alloc, .{
        .graph_metric_queries = &.{.{
            .name = "degree",
            .query = .{
                .index_name = "graph_idx",
                .metric_name = "degree",
                .top_k = 1,
                .freshness = .fresh,
            },
        }},
        .limit = 0,
    });
    defer metric_result.deinit();
    try std.testing.expectEqual(@as(usize, 1), metric_result.graph_metric_results.len);
    try std.testing.expectEqual(graph_mod.GraphIndex.GraphMetricState.fresh, metric_result.graph_metric_results[0].status.state);
    try std.testing.expectEqual(target_generation, metric_result.graph_metric_results[0].status.published_generation);
    try std.testing.expectEqual(@as(usize, 1), metric_result.graph_metric_results[0].scores.len);
    try std.testing.expectEqualStrings("doc:hub", metric_result.graph_metric_results[0].scores[0].node);
    try std.testing.expectApproxEqAbs(@as(f64, 130.0), metric_result.graph_metric_results[0].scores[0].score, 0.001);
}

test "db graph metric runtime background coordinator and worker pool loops publish pagerank" {
    const alloc = std.testing.allocator;

    var path_buf: [256]u8 = undefined;
    const path = tempPath(&path_buf);
    defer cleanupTempDir(path);

    var db = try DB.open(alloc, std.mem.span(path), .{
        .start_index_workers = false,
        .ttl_cleanup = .{ .enabled = false },
    });
    defer db.close();

    try db.addIndex(.{
        .name = "graph_idx",
        .kind = .graph,
        .config_json = "{\"metrics\":{\"pagerank\":{\"enabled\":true,\"kind\":\"pagerank\",\"refresh\":\"background\",\"max_iterations\":2,\"tolerance\":0.000000001,\"edge_filter\":{\"types\":[\"cites\"]}}}}",
    });

    try db.batch(.{
        .writes = &.{
            .{ .key = "doc:a", .value = "{\"title\":\"alpha\",\"_edges\":{\"graph_idx\":{\"cites\":[{\"target\":\"doc:b\",\"weight\":1.0}]}}}" },
            .{ .key = "doc:b", .value = "{\"title\":\"beta\",\"_edges\":{\"graph_idx\":{\"cites\":[{\"target\":\"doc:d\",\"weight\":1.0}]}}}" },
            .{ .key = "doc:c", .value = "{\"title\":\"gamma\",\"_edges\":{\"graph_idx\":{\"cites\":[{\"target\":\"doc:b\",\"weight\":1.0}]}}}" },
            .{ .key = "doc:d", .value = "{\"title\":\"delta\"}" },
        },
        .sync_level = .write,
    });

    try db.runDerivedUntil(db.core.nextDerivedSequence());

    const target_generation = blk: {
        const graph_entry = db.core.graphIndex("graph_idx") orelse return error.IndexNotFound;
        break :blk graph_entry.index.edge_generation;
    };

    const resources = db.core.asyncResources();
    var coordinator_runtime = try graph_metric_runtime_mod.GraphMetricRuntime.init(
        alloc,
        resources.store,
        resources.index_manager,
        resources.apply_mutex,
        db.backend_runtime,
        .{
            .enabled = true,
            .role = .coordinator,
            .runtime_id = "runtime-bg-pagerank-pool-coordinator-owner",
            .idle_interval_ms = 1,
            .error_interval_ms = 1,
            .planned_options = .{
                .worker_id = "runtime-bg-pagerank-pool-coordinator-unused",
                .max_rounds = 1,
                .max_metrics_per_round = 8,
                .max_pages_per_round = 1,
            },
        },
    );
    defer coordinator_runtime.deinit();

    const workers = [_][]const u8{ "runtime-bg-pagerank-pool-worker-a", "runtime-bg-pagerank-pool-worker-b" };
    var worker_pool_runtime = try graph_metric_runtime_mod.GraphMetricRuntime.init(
        alloc,
        resources.store,
        resources.index_manager,
        resources.apply_mutex,
        db.backend_runtime,
        .{
            .enabled = true,
            .role = .worker_pool,
            .runtime_id = "runtime-bg-pagerank-pool-worker-owner",
            .idle_interval_ms = 1,
            .error_interval_ms = 1,
            .planned_options = .{
                .worker_ids = &workers,
                .max_rounds = 1,
                .max_metrics_per_round = 8,
                .max_pages_per_round = 2,
            },
        },
    );
    defer worker_pool_runtime.deinit();

    try coordinator_runtime.start();
    try worker_pool_runtime.start();
    coordinator_runtime.notify();
    worker_pool_runtime.notify();

    var fresh = false;
    for (0..700) |_| {
        yieldToBackground();
        const graph_entry = db.core.graphIndex("graph_idx") orelse return error.IndexNotFound;
        var status = try graph_entry.index.graphMetricStatus("pagerank");
        defer status.deinit(alloc);
        if (status.state == .fresh and status.published_generation == target_generation) {
            fresh = true;
            break;
        }
    }
    try std.testing.expect(fresh);

    {
        const coordinator_stats = coordinator_runtime.stats();
        try std.testing.expect(coordinator_stats.started);
        try std.testing.expectEqual(graph_metric_runtime_mod.Role.coordinator, coordinator_stats.role);
        try std.testing.expectEqual(std.hash.Wyhash.hash(0, "runtime-bg-pagerank-pool-coordinator-owner"), coordinator_stats.runtime_id_hash);
        try std.testing.expectEqual(@as(u64, 0), coordinator_stats.worker_id_hash);
        try std.testing.expectEqual(@as(usize, 0), coordinator_stats.worker_count);
        try std.testing.expect(coordinator_stats.ticks_started > 0);
        try std.testing.expect(coordinator_stats.durable_progress_ticks > 0);
        try std.testing.expectEqual(@as(u64, 0), coordinator_stats.error_ticks);
        try std.testing.expect(coordinator_stats.total_result.builds_started > 0);
        try std.testing.expect(coordinator_stats.total_result.coordinator_steps > 0);
        try std.testing.expect(coordinator_stats.total_result.phases_advanced > 0);
        try std.testing.expect(coordinator_stats.total_result.published > 0);
        try std.testing.expectEqual(@as(usize, 0), coordinator_stats.total_result.worker_steps);
        try std.testing.expect(coordinator_stats.last_result.worker_steps == 0);
    }
    {
        const worker_stats = worker_pool_runtime.stats();
        try std.testing.expect(worker_stats.started);
        try std.testing.expectEqual(graph_metric_runtime_mod.Role.worker_pool, worker_stats.role);
        try std.testing.expectEqual(std.hash.Wyhash.hash(0, "runtime-bg-pagerank-pool-worker-owner"), worker_stats.runtime_id_hash);
        const expected_worker_hash = graph_metric_runtime_mod.workerSetIdentityHash(workers[0..]);
        try std.testing.expectEqual(expected_worker_hash, worker_stats.worker_id_hash);
        try std.testing.expectEqual(@as(usize, 2), worker_stats.worker_count);
        try std.testing.expect(worker_stats.ticks_started > 0);
        try std.testing.expect(worker_stats.durable_progress_ticks > 0);
        try std.testing.expectEqual(@as(u64, 0), worker_stats.error_ticks);
        try std.testing.expect(worker_stats.total_result.worker_steps > 0);
        try std.testing.expect(worker_stats.total_result.pages_completed > 0);
        try std.testing.expectEqual(@as(usize, 0), worker_stats.total_result.coordinator_steps);
        try std.testing.expect(worker_stats.last_result.coordinator_steps == 0);
    }

    {
        const pending = db.pendingWorkStats().graph_metric;
        try std.testing.expect(!pending.hasWork());
    }

    var metric_result = try db.search(alloc, .{
        .graph_metric_queries = &.{.{
            .name = "pagerank",
            .query = .{
                .index_name = "graph_idx",
                .metric_name = "pagerank",
                .top_k = 2,
                .freshness = .fresh,
            },
        }},
        .limit = 0,
    });
    defer metric_result.deinit();
    try std.testing.expectEqual(@as(usize, 1), metric_result.graph_metric_results.len);
    try std.testing.expectEqual(graph_mod.GraphIndex.GraphMetricState.fresh, metric_result.graph_metric_results[0].status.state);
    try std.testing.expectEqual(target_generation, metric_result.graph_metric_results[0].status.published_generation);
    try std.testing.expectEqualStrings("doc:d", metric_result.graph_metric_results[0].scores[0].node);
}

test "db graph metric runtime background coordinator and worker pool loops publish eigenvector" {
    const alloc = std.testing.allocator;

    var path_buf: [256]u8 = undefined;
    const path = tempPath(&path_buf);
    defer cleanupTempDir(path);

    var db = try DB.open(alloc, std.mem.span(path), .{
        .start_index_workers = false,
        .ttl_cleanup = .{ .enabled = false },
    });
    defer db.close();

    try db.addIndex(.{
        .name = "graph_idx",
        .kind = .graph,
        .config_json = "{\"metrics\":{\"eigenvector\":{\"enabled\":true,\"kind\":\"eigenvector\",\"refresh\":\"background\",\"max_iterations\":1,\"tolerance\":0.000001,\"edge_filter\":{\"types\":[\"cites\"]}}}}",
    });

    try db.batch(.{
        .writes = &.{
            .{ .key = "doc:a", .value = "{\"title\":\"alpha\",\"_edges\":{\"graph_idx\":{\"cites\":[{\"target\":\"doc:b\",\"weight\":1.0}]}}}" },
            .{ .key = "doc:b", .value = "{\"title\":\"beta\",\"_edges\":{\"graph_idx\":{\"cites\":[{\"target\":\"doc:d\",\"weight\":1.0}]}}}" },
            .{ .key = "doc:c", .value = "{\"title\":\"gamma\",\"_edges\":{\"graph_idx\":{\"cites\":[{\"target\":\"doc:b\",\"weight\":1.0}]}}}" },
            .{ .key = "doc:d", .value = "{\"title\":\"delta\"}" },
        },
        .sync_level = .write,
    });

    try db.runDerivedUntil(db.core.nextDerivedSequence());

    const target_generation = blk: {
        const graph_entry = db.core.graphIndex("graph_idx") orelse return error.IndexNotFound;
        break :blk graph_entry.index.edge_generation;
    };

    const resources = db.core.asyncResources();
    var coordinator_runtime = try graph_metric_runtime_mod.GraphMetricRuntime.init(
        alloc,
        resources.store,
        resources.index_manager,
        resources.apply_mutex,
        db.backend_runtime,
        .{
            .enabled = true,
            .role = .coordinator,
            .runtime_id = "runtime-bg-eigenvector-pool-coordinator-owner",
            .idle_interval_ms = 1,
            .error_interval_ms = 1,
            .planned_options = .{
                .worker_id = "runtime-bg-eigenvector-pool-coordinator-unused",
                .max_rounds = 1,
                .max_metrics_per_round = 8,
                .max_pages_per_round = 1,
            },
        },
    );
    defer coordinator_runtime.deinit();

    const workers = [_][]const u8{ "runtime-bg-eigenvector-pool-worker-a", "runtime-bg-eigenvector-pool-worker-b" };
    var worker_pool_runtime = try graph_metric_runtime_mod.GraphMetricRuntime.init(
        alloc,
        resources.store,
        resources.index_manager,
        resources.apply_mutex,
        db.backend_runtime,
        .{
            .enabled = true,
            .role = .worker_pool,
            .runtime_id = "runtime-bg-eigenvector-pool-worker-owner",
            .idle_interval_ms = 1,
            .error_interval_ms = 1,
            .planned_options = .{
                .worker_ids = &workers,
                .max_rounds = 1,
                .max_metrics_per_round = 8,
                .max_pages_per_round = 2,
            },
        },
    );
    defer worker_pool_runtime.deinit();

    try coordinator_runtime.start();
    try worker_pool_runtime.start();
    coordinator_runtime.notify();
    worker_pool_runtime.notify();

    var fresh = false;
    for (0..700) |_| {
        yieldToBackground();
        const graph_entry = db.core.graphIndex("graph_idx") orelse return error.IndexNotFound;
        var status = try graph_entry.index.graphMetricStatus("eigenvector");
        defer status.deinit(alloc);
        if (status.state == .fresh and status.published_generation == target_generation) {
            fresh = true;
            break;
        }
    }
    try std.testing.expect(fresh);

    {
        const coordinator_stats = coordinator_runtime.stats();
        try std.testing.expect(coordinator_stats.started);
        try std.testing.expectEqual(graph_metric_runtime_mod.Role.coordinator, coordinator_stats.role);
        try std.testing.expectEqual(std.hash.Wyhash.hash(0, "runtime-bg-eigenvector-pool-coordinator-owner"), coordinator_stats.runtime_id_hash);
        try std.testing.expectEqual(@as(u64, 0), coordinator_stats.worker_id_hash);
        try std.testing.expectEqual(@as(usize, 0), coordinator_stats.worker_count);
        try std.testing.expect(coordinator_stats.ticks_started > 0);
        try std.testing.expect(coordinator_stats.durable_progress_ticks > 0);
        try std.testing.expectEqual(@as(u64, 0), coordinator_stats.error_ticks);
        try std.testing.expect(coordinator_stats.total_result.builds_started > 0);
        try std.testing.expect(coordinator_stats.total_result.coordinator_steps > 0);
        try std.testing.expect(coordinator_stats.total_result.phases_advanced > 0);
        try std.testing.expect(coordinator_stats.total_result.published > 0);
        try std.testing.expectEqual(@as(usize, 0), coordinator_stats.total_result.worker_steps);
        try std.testing.expect(coordinator_stats.last_result.worker_steps == 0);
    }
    {
        const worker_stats = worker_pool_runtime.stats();
        try std.testing.expect(worker_stats.started);
        try std.testing.expectEqual(graph_metric_runtime_mod.Role.worker_pool, worker_stats.role);
        try std.testing.expectEqual(std.hash.Wyhash.hash(0, "runtime-bg-eigenvector-pool-worker-owner"), worker_stats.runtime_id_hash);
        const expected_worker_hash = graph_metric_runtime_mod.workerSetIdentityHash(workers[0..]);
        try std.testing.expectEqual(expected_worker_hash, worker_stats.worker_id_hash);
        try std.testing.expectEqual(@as(usize, 2), worker_stats.worker_count);
        try std.testing.expect(worker_stats.ticks_started > 0);
        try std.testing.expect(worker_stats.durable_progress_ticks > 0);
        try std.testing.expectEqual(@as(u64, 0), worker_stats.error_ticks);
        try std.testing.expect(worker_stats.total_result.worker_steps > 0);
        try std.testing.expect(worker_stats.total_result.pages_completed > 0);
        try std.testing.expectEqual(@as(usize, 0), worker_stats.total_result.coordinator_steps);
        try std.testing.expect(worker_stats.last_result.coordinator_steps == 0);
    }

    {
        const pending = db.pendingWorkStats().graph_metric;
        try std.testing.expect(!pending.hasWork());
    }

    var metric_result = try db.search(alloc, .{
        .graph_metric_queries = &.{.{
            .name = "eigenvector",
            .query = .{
                .index_name = "graph_idx",
                .metric_name = "eigenvector",
                .top_k = 2,
                .freshness = .fresh,
            },
        }},
        .limit = 0,
    });
    defer metric_result.deinit();
    try std.testing.expectEqual(@as(usize, 1), metric_result.graph_metric_results.len);
    try std.testing.expectEqual(graph_mod.GraphIndex.GraphMetricState.fresh, metric_result.graph_metric_results[0].status.state);
    try std.testing.expectEqual(target_generation, metric_result.graph_metric_results[0].status.published_generation);
    try std.testing.expectEqual(@as(usize, 2), metric_result.graph_metric_results[0].scores.len);
}

test "db graph metric runtime background coordinator and worker pool loops publish hits pair" {
    const alloc = std.testing.allocator;

    var path_buf: [256]u8 = undefined;
    const path = tempPath(&path_buf);
    defer cleanupTempDir(path);

    var db = try DB.open(alloc, std.mem.span(path), .{
        .start_index_workers = false,
        .ttl_cleanup = .{ .enabled = false },
    });
    defer db.close();

    try db.addIndex(.{
        .name = "graph_idx",
        .kind = .graph,
        .config_json = "{\"metrics\":{\"hits_authority\":{\"enabled\":true,\"kind\":\"hits_authority\",\"refresh\":\"background\",\"max_iterations\":1,\"tolerance\":0.000001,\"edge_filter\":{\"types\":[\"cites\"]}},\"hits_hub\":{\"enabled\":true,\"kind\":\"hits_hub\",\"refresh\":\"background\",\"max_iterations\":1,\"tolerance\":0.000001,\"edge_filter\":{\"types\":[\"cites\"]}}}}",
    });

    try db.batch(.{
        .writes = &.{
            .{ .key = "doc:hub-a", .value = "{\"title\":\"hub a\",\"_edges\":{\"graph_idx\":{\"cites\":[{\"target\":\"doc:authority\",\"weight\":1.0}]}}}" },
            .{ .key = "doc:hub-b", .value = "{\"title\":\"hub b\",\"_edges\":{\"graph_idx\":{\"cites\":[{\"target\":\"doc:authority\",\"weight\":1.0}]}}}" },
            .{ .key = "doc:authority", .value = "{\"title\":\"authority\"}" },
        },
        .sync_level = .write,
    });

    try db.runDerivedUntil(db.core.nextDerivedSequence());

    const target_generation = blk: {
        const graph_entry = db.core.graphIndex("graph_idx") orelse return error.IndexNotFound;
        break :blk graph_entry.index.edge_generation;
    };

    const resources = db.core.asyncResources();
    var coordinator_runtime = try graph_metric_runtime_mod.GraphMetricRuntime.init(
        alloc,
        resources.store,
        resources.index_manager,
        resources.apply_mutex,
        db.backend_runtime,
        .{
            .enabled = true,
            .role = .coordinator,
            .runtime_id = "runtime-bg-hits-pool-coordinator-owner",
            .idle_interval_ms = 1,
            .error_interval_ms = 1,
            .planned_options = .{
                .worker_id = "runtime-bg-hits-pool-coordinator-unused",
                .max_rounds = 1,
                .max_metrics_per_round = 8,
                .max_pages_per_round = 1,
            },
        },
    );
    defer coordinator_runtime.deinit();

    const workers = [_][]const u8{ "runtime-bg-hits-pool-worker-a", "runtime-bg-hits-pool-worker-b" };
    var worker_pool_runtime = try graph_metric_runtime_mod.GraphMetricRuntime.init(
        alloc,
        resources.store,
        resources.index_manager,
        resources.apply_mutex,
        db.backend_runtime,
        .{
            .enabled = true,
            .role = .worker_pool,
            .runtime_id = "runtime-bg-hits-pool-worker-owner",
            .idle_interval_ms = 1,
            .error_interval_ms = 1,
            .planned_options = .{
                .worker_ids = &workers,
                .max_rounds = 1,
                .max_metrics_per_round = 8,
                .max_pages_per_round = 2,
            },
        },
    );
    defer worker_pool_runtime.deinit();

    try coordinator_runtime.start();
    try worker_pool_runtime.start();
    coordinator_runtime.notify();
    worker_pool_runtime.notify();

    var fresh = false;
    for (0..700) |_| {
        yieldToBackground();
        const graph_entry = db.core.graphIndex("graph_idx") orelse return error.IndexNotFound;
        var authority_status = try graph_entry.index.graphMetricStatus("hits_authority");
        defer authority_status.deinit(alloc);
        var hub_status = try graph_entry.index.graphMetricStatus("hits_hub");
        defer hub_status.deinit(alloc);
        if (authority_status.state == .fresh and
            hub_status.state == .fresh and
            authority_status.published_generation == target_generation and
            hub_status.published_generation == target_generation)
        {
            fresh = true;
            break;
        }
    }
    try std.testing.expect(fresh);

    {
        const coordinator_stats = coordinator_runtime.stats();
        try std.testing.expect(coordinator_stats.started);
        try std.testing.expectEqual(graph_metric_runtime_mod.Role.coordinator, coordinator_stats.role);
        try std.testing.expectEqual(std.hash.Wyhash.hash(0, "runtime-bg-hits-pool-coordinator-owner"), coordinator_stats.runtime_id_hash);
        try std.testing.expectEqual(@as(u64, 0), coordinator_stats.worker_id_hash);
        try std.testing.expectEqual(@as(usize, 0), coordinator_stats.worker_count);
        try std.testing.expect(coordinator_stats.ticks_started > 0);
        try std.testing.expect(coordinator_stats.durable_progress_ticks > 0);
        try std.testing.expectEqual(@as(u64, 0), coordinator_stats.error_ticks);
        try std.testing.expect(coordinator_stats.total_result.builds_started > 0);
        try std.testing.expect(coordinator_stats.total_result.coordinator_steps > 0);
        try std.testing.expect(coordinator_stats.total_result.phases_advanced > 0);
        try std.testing.expect(coordinator_stats.total_result.published > 0);
        try std.testing.expectEqual(@as(usize, 0), coordinator_stats.total_result.worker_steps);
        try std.testing.expect(coordinator_stats.last_result.worker_steps == 0);
    }
    {
        const worker_stats = worker_pool_runtime.stats();
        try std.testing.expect(worker_stats.started);
        try std.testing.expectEqual(graph_metric_runtime_mod.Role.worker_pool, worker_stats.role);
        try std.testing.expectEqual(std.hash.Wyhash.hash(0, "runtime-bg-hits-pool-worker-owner"), worker_stats.runtime_id_hash);
        const expected_worker_hash = graph_metric_runtime_mod.workerSetIdentityHash(workers[0..]);
        try std.testing.expectEqual(expected_worker_hash, worker_stats.worker_id_hash);
        try std.testing.expectEqual(@as(usize, 2), worker_stats.worker_count);
        try std.testing.expect(worker_stats.ticks_started > 0);
        try std.testing.expect(worker_stats.durable_progress_ticks > 0);
        try std.testing.expectEqual(@as(u64, 0), worker_stats.error_ticks);
        try std.testing.expect(worker_stats.total_result.worker_steps > 0);
        try std.testing.expect(worker_stats.total_result.pages_completed > 0);
        try std.testing.expectEqual(@as(usize, 0), worker_stats.total_result.coordinator_steps);
        try std.testing.expect(worker_stats.last_result.coordinator_steps == 0);
    }

    {
        const pending = db.pendingWorkStats().graph_metric;
        try std.testing.expect(!pending.hasWork());
    }

    var metric_result = try db.search(alloc, .{
        .graph_metric_queries = &.{
            .{
                .name = "authority",
                .query = .{
                    .index_name = "graph_idx",
                    .metric_name = "hits_authority",
                    .top_k = 3,
                    .freshness = .fresh,
                },
            },
            .{
                .name = "hub",
                .query = .{
                    .index_name = "graph_idx",
                    .metric_name = "hits_hub",
                    .top_k = 3,
                    .freshness = .fresh,
                },
            },
        },
        .limit = 0,
    });
    defer metric_result.deinit();
    try std.testing.expectEqual(@as(usize, 2), metric_result.graph_metric_results.len);
    try std.testing.expectEqual(graph_mod.GraphIndex.GraphMetricState.fresh, metric_result.graph_metric_results[0].status.state);
    try std.testing.expectEqual(target_generation, metric_result.graph_metric_results[0].status.published_generation);
    try std.testing.expectEqual(graph_mod.GraphIndex.GraphMetricState.fresh, metric_result.graph_metric_results[1].status.state);
    try std.testing.expectEqual(metric_result.graph_metric_results[0].status.published_generation, metric_result.graph_metric_results[1].status.published_generation);
    try std.testing.expectEqualStrings("doc:authority", metric_result.graph_metric_results[0].scores[0].node);
    try std.testing.expectApproxEqAbs(@as(f64, 1.0), metric_result.graph_metric_results[0].scores[0].score, 0.001);
}

test "db graph metric runtime background worker pool survives separate reopened handles" {
    const alloc = std.testing.allocator;

    var path_buf: [256]u8 = undefined;
    const path = tempPath(&path_buf);
    defer cleanupTempDir(path);

    var target_generation: u64 = 0;
    {
        var db = try DB.open(alloc, std.mem.span(path), .{
            .start_index_workers = false,
            .ttl_cleanup = .{ .enabled = false },
        });
        defer db.close();

        try db.addIndex(.{
            .name = "graph_idx",
            .kind = .graph,
            .config_json = "{\"metrics\":{\"degree\":{\"enabled\":true,\"kind\":\"degree\",\"refresh\":\"background\",\"edge_filter\":{\"types\":[\"cites\"]}}}}",
        });

        try db.batch(.{
            .writes = &.{.{ .key = "doc:hub", .value = "{\"title\":\"hub\"}" }},
            .sync_level = .write,
        });

        for (0..130) |i| {
            const key = try std.fmt.allocPrint(alloc, "doc:{d:0>3}", .{i});
            defer alloc.free(key);
            const value = try std.fmt.allocPrint(
                alloc,
                "{{\"title\":\"source {d}\",\"_edges\":{{\"graph_idx\":{{\"cites\":[{{\"target\":\"doc:hub\",\"weight\":1.0}}]}}}}}}",
                .{i},
            );
            defer alloc.free(value);
            try db.batch(.{
                .writes = &.{.{ .key = key, .value = value }},
                .sync_level = .write,
            });
        }

        try db.runDerivedUntil(db.core.nextDerivedSequence());

        const graph_entry = db.core.graphIndex("graph_idx") orelse return error.IndexNotFound;
        target_generation = graph_entry.index.edge_generation;
    }

    const workers = [_][]const u8{ "runtime-reopened-pool-worker-a", "runtime-reopened-pool-worker-b" };
    const reversed_workers = [_][]const u8{ "runtime-reopened-pool-worker-b", "runtime-reopened-pool-worker-a" };
    var coordinator_total = index_manager_mod.IndexManager.GraphMetricPlannedSchedulerSweepResult{};
    var worker_total = index_manager_mod.IndexManager.GraphMetricPlannedSchedulerSweepResult{};
    var saw_worker_pool_role = false;
    var saw_two_page_worker_pool_tick = false;
    var saw_live_duplicate_worker_pool_fenced = false;
    var fresh = false;
    for (0..400) |_| {
        const worker_tick = blk: {
            var worker_pool = try DB.open(alloc, std.mem.span(path), .{
                .open_mode = .writer_no_replay,
                .start_index_workers = false,
                .ttl_cleanup = .{ .enabled = false },
            });
            defer worker_pool.close();

            const worker_resources = worker_pool.core.asyncResources();
            var runtime = try graph_metric_runtime_mod.GraphMetricRuntime.init(
                alloc,
                worker_resources.store,
                worker_resources.index_manager,
                worker_resources.apply_mutex,
                worker_pool.backend_runtime,
                .{
                    .enabled = true,
                    .role = .worker_pool,
                    .runtime_id = "runtime-reopened-pool-worker-owner",
                    .lease_owned = true,
                    .owner_id = "runtime-reopened-pool-worker-owner",
                    .planned_options = .{
                        .worker_ids = &workers,
                        .max_rounds = 1,
                        .max_metrics_per_round = 8,
                        .max_pages_per_round = 2,
                    },
                },
            );
            defer runtime.deinit();

            const tick = try runtime.runOnceDetailed();
            const stats = runtime.stats();
            try std.testing.expectEqual(graph_metric_runtime_mod.Role.worker_pool, stats.role);
            try std.testing.expectEqual(std.hash.Wyhash.hash(0, "runtime-reopened-pool-worker-owner"), stats.runtime_id_hash);
            try std.testing.expect(stats.lease_owned);
            try std.testing.expect(stats.has_lease);
            const expected_worker_hash = graph_metric_runtime_mod.workerSetIdentityHash(workers[0..]);
            try std.testing.expectEqual(expected_worker_hash, stats.worker_id_hash);
            try std.testing.expectEqual(@as(usize, 2), stats.worker_count);
            try std.testing.expectEqual(@as(usize, 0), stats.total_result.coordinator_steps);
            saw_worker_pool_role = true;
            if (tick.worker_steps >= 2 and tick.pages_completed >= 2) saw_two_page_worker_pool_tick = true;
            if (tick.worker_steps > 0) {
                var duplicate_runtime = try graph_metric_runtime_mod.GraphMetricRuntime.init(
                    alloc,
                    worker_resources.store,
                    worker_resources.index_manager,
                    worker_resources.apply_mutex,
                    worker_pool.backend_runtime,
                    .{
                        .enabled = true,
                        .role = .worker_pool,
                        .runtime_id = "runtime-reopened-pool-worker-owner-duplicate",
                        .lease_owned = true,
                        .owner_id = "runtime-reopened-pool-worker-owner-duplicate",
                        .planned_options = .{
                            .worker_ids = &reversed_workers,
                            .max_rounds = 1,
                            .max_metrics_per_round = 8,
                            .max_pages_per_round = 2,
                        },
                    },
                );
                defer duplicate_runtime.deinit();

                const duplicate_tick = try duplicate_runtime.runOnceDetailed();
                try std.testing.expect(!duplicate_tick.durableProgressed());
                try std.testing.expectEqual(@as(usize, 0), duplicate_tick.worker_steps);
                try std.testing.expectEqual(@as(usize, 0), duplicate_tick.pages_completed);
                const duplicate_stats = duplicate_runtime.stats();
                try std.testing.expect(duplicate_stats.lease_owned);
                try std.testing.expect(!duplicate_stats.has_lease);
                try std.testing.expectEqual(stats.worker_id_hash, duplicate_stats.worker_id_hash);
                try std.testing.expectEqual(stats.lease_key_hash, duplicate_stats.lease_key_hash);
                try std.testing.expectEqual(@as(u64, 0), duplicate_stats.acquisition_count);
                try std.testing.expectEqual(@as(u64, 1), duplicate_stats.lease_acquire_failures);
                saw_live_duplicate_worker_pool_fenced = true;
            }
            break :blk tick;
        };
        worker_total.add(worker_tick);

        const coordinator_tick = blk: {
            var coordinator = try DB.open(alloc, std.mem.span(path), .{
                .open_mode = .writer_no_replay,
                .start_index_workers = false,
                .ttl_cleanup = .{ .enabled = false },
            });
            defer coordinator.close();

            const coordinator_resources = coordinator.core.asyncResources();
            var runtime = try graph_metric_runtime_mod.GraphMetricRuntime.init(
                alloc,
                coordinator_resources.store,
                coordinator_resources.index_manager,
                coordinator_resources.apply_mutex,
                coordinator.backend_runtime,
                .{
                    .enabled = true,
                    .role = .coordinator,
                    .runtime_id = "runtime-reopened-pool-coordinator-owner",
                    .lease_owned = true,
                    .owner_id = "runtime-reopened-pool-coordinator-owner",
                    .planned_options = .{
                        .worker_id = "runtime-reopened-pool-coordinator-unused",
                        .max_rounds = 1,
                        .max_metrics_per_round = 8,
                        .max_pages_per_round = 1,
                    },
                },
            );
            defer runtime.deinit();

            const tick = try runtime.runOnceDetailed();
            const stats = runtime.stats();
            try std.testing.expectEqual(graph_metric_runtime_mod.Role.coordinator, stats.role);
            try std.testing.expectEqual(std.hash.Wyhash.hash(0, "runtime-reopened-pool-coordinator-owner"), stats.runtime_id_hash);
            try std.testing.expect(stats.lease_owned);
            try std.testing.expect(stats.has_lease);
            try std.testing.expectEqual(@as(u64, 0), stats.worker_id_hash);
            try std.testing.expectEqual(@as(usize, 0), stats.worker_count);
            try std.testing.expectEqual(@as(usize, 0), stats.total_result.worker_steps);
            break :blk tick;
        };
        coordinator_total.add(coordinator_tick);

        {
            var reader = try DB.open(alloc, std.mem.span(path), .{
                .open_mode = .query_readonly,
                .start_index_workers = false,
                .ttl_cleanup = .{ .enabled = false },
            });
            defer reader.close();

            const graph_entry = reader.core.graphIndex("graph_idx") orelse return error.IndexNotFound;
            var status = try graph_entry.index.graphMetricStatus("degree");
            defer status.deinit(alloc);
            if (status.state == .fresh and status.published_generation == target_generation) {
                fresh = true;
                break;
            }
        }

        if (!worker_tick.durableProgressed() and !coordinator_tick.durableProgressed()) {
            return error.GraphMetricBuildNoEligiblePage;
        }
    }
    try std.testing.expect(fresh);
    try std.testing.expect(saw_worker_pool_role);
    try std.testing.expect(saw_two_page_worker_pool_tick);
    try std.testing.expect(saw_live_duplicate_worker_pool_fenced);
    try std.testing.expect(coordinator_total.builds_started > 0);
    try std.testing.expect(coordinator_total.coordinator_steps > 0);
    try std.testing.expect(coordinator_total.phases_advanced > 0);
    try std.testing.expectEqual(@as(usize, 0), coordinator_total.worker_steps);
    try std.testing.expect(worker_total.worker_steps >= 2);
    try std.testing.expect(worker_total.pages_completed >= 2);
    try std.testing.expectEqual(@as(usize, 0), worker_total.coordinator_steps);

    {
        var reader = try DB.open(alloc, std.mem.span(path), .{
            .open_mode = .query_readonly,
            .start_index_workers = false,
            .ttl_cleanup = .{ .enabled = false },
        });
        defer reader.close();

        var metric_result = try reader.search(alloc, .{
            .graph_metric_queries = &.{.{
                .name = "degree",
                .query = .{
                    .index_name = "graph_idx",
                    .metric_name = "degree",
                    .top_k = 1,
                    .freshness = .fresh,
                },
            }},
            .limit = 0,
        });
        defer metric_result.deinit();
        try std.testing.expectEqual(@as(usize, 1), metric_result.graph_metric_results.len);
        try std.testing.expectEqual(graph_mod.GraphIndex.GraphMetricState.fresh, metric_result.graph_metric_results[0].status.state);
        try std.testing.expectEqual(target_generation, metric_result.graph_metric_results[0].status.published_generation);
        try std.testing.expectEqual(@as(usize, 1), metric_result.graph_metric_results[0].scores.len);
        try std.testing.expectEqualStrings("doc:hub", metric_result.graph_metric_results[0].scores[0].node);
        try std.testing.expectApproxEqAbs(@as(f64, 130.0), metric_result.graph_metric_results[0].scores[0].score, 0.001);
    }
}

test "db graph metric runtime open-configured pagerank worker pool survives separate reopened handles" {
    const alloc = std.testing.allocator;

    var path_buf: [256]u8 = undefined;
    const path = tempPath(&path_buf);
    defer cleanupTempDir(path);

    var target_generation: u64 = 0;
    {
        var db = try DB.open(alloc, std.mem.span(path), .{
            .start_index_workers = false,
            .ttl_cleanup = .{ .enabled = false },
        });
        defer db.close();

        try db.addIndex(.{
            .name = "graph_idx",
            .kind = .graph,
            .config_json = "{\"metrics\":{\"pagerank\":{\"enabled\":true,\"kind\":\"pagerank\",\"refresh\":\"background\",\"max_iterations\":2,\"tolerance\":0.000000001,\"edge_filter\":{\"types\":[\"cites\"]}}}}",
        });

        try db.batch(.{
            .writes = &.{
                .{ .key = "doc:a", .value = "{\"title\":\"alpha\",\"_edges\":{\"graph_idx\":{\"cites\":[{\"target\":\"doc:b\",\"weight\":1.0}]}}}" },
                .{ .key = "doc:b", .value = "{\"title\":\"beta\",\"_edges\":{\"graph_idx\":{\"cites\":[{\"target\":\"doc:d\",\"weight\":1.0}]}}}" },
                .{ .key = "doc:c", .value = "{\"title\":\"gamma\",\"_edges\":{\"graph_idx\":{\"cites\":[{\"target\":\"doc:b\",\"weight\":1.0}]}}}" },
                .{ .key = "doc:d", .value = "{\"title\":\"delta\"}" },
            },
            .sync_level = .write,
        });

        try db.runDerivedUntil(db.core.nextDerivedSequence());

        const graph_entry = db.core.graphIndex("graph_idx") orelse return error.IndexNotFound;
        target_generation = graph_entry.index.edge_generation;
    }

    const workers = [_][]const u8{ "runtime-reopened-pagerank-pool-worker-a", "runtime-reopened-pagerank-pool-worker-b" };
    const reversed_workers = [_][]const u8{ "runtime-reopened-pagerank-pool-worker-b", "runtime-reopened-pagerank-pool-worker-a" };
    var coordinator_total = index_manager_mod.IndexManager.GraphMetricPlannedSchedulerSweepResult{};
    var worker_total = index_manager_mod.IndexManager.GraphMetricPlannedSchedulerSweepResult{};
    var saw_worker_pool_role = false;
    var saw_live_duplicate_worker_pool_fenced = false;
    var fresh = false;
    for (0..500) |_| {
        const worker_tick = blk: {
            var worker_pool = try DB.open(alloc, std.mem.span(path), .{
                .open_mode = .writer_no_replay,
                .ttl_cleanup = .{ .enabled = false },
                .graph_metric_maintenance = .{
                    .enabled = true,
                    .start_background_loop = false,
                    .role = .worker_pool,
                    .runtime_id = "runtime-reopened-pagerank-pool-worker-owner",
                    .lease_owned = true,
                    .owner_id = "runtime-reopened-pagerank-pool-worker-owner",
                    .planned_options = .{
                        .worker_ids = &workers,
                        .max_rounds = 1,
                        .max_metrics_per_round = 8,
                        .max_pages_per_round = 2,
                    },
                },
            });
            defer worker_pool.close();

            const tick = try worker_pool.graph_metric_runtime.?.runOnceDetailed();
            const stats = worker_pool.graphMetricRuntimeStats();
            try std.testing.expect(stats.enabled);
            try std.testing.expectEqual(types.GraphMetricRuntimeRole.worker_pool, stats.role.?);
            try std.testing.expectEqual(std.hash.Wyhash.hash(0, "runtime-reopened-pagerank-pool-worker-owner"), stats.runtime_id_hash);
            try std.testing.expectEqual(std.hash.Wyhash.hash(0, "runtime-reopened-pagerank-pool-worker-owner"), stats.owner_id_hash);
            try std.testing.expect(stats.lease_owned);
            try std.testing.expect(stats.has_lease);
            try std.testing.expect(!stats.started);
            const expected_worker_hash = graph_metric_runtime_mod.workerSetIdentityHash(workers[0..]);
            try std.testing.expectEqual(expected_worker_hash, stats.worker_id_hash);
            try std.testing.expectEqual(@as(u64, 2), stats.worker_count);
            try std.testing.expectEqual(@as(u64, 0), stats.total_coordinator_steps);
            saw_worker_pool_role = true;
            if (tick.worker_steps > 0) {
                var duplicate_worker_pool = try DB.open(alloc, std.mem.span(path), .{
                    .open_mode = .writer_no_replay,
                    .ttl_cleanup = .{ .enabled = false },
                    .graph_metric_maintenance = .{
                        .enabled = true,
                        .start_background_loop = false,
                        .role = .worker_pool,
                        .runtime_id = "runtime-reopened-pagerank-pool-worker-owner-duplicate",
                        .lease_owned = true,
                        .owner_id = "runtime-reopened-pagerank-pool-worker-owner-duplicate",
                        .planned_options = .{
                            .worker_ids = &reversed_workers,
                            .max_rounds = 1,
                            .max_metrics_per_round = 8,
                            .max_pages_per_round = 2,
                        },
                    },
                });
                defer duplicate_worker_pool.close();

                const duplicate_tick = try duplicate_worker_pool.graph_metric_runtime.?.runOnceDetailed();
                try std.testing.expect(!duplicate_tick.durableProgressed());
                try std.testing.expectEqual(@as(usize, 0), duplicate_tick.worker_steps);
                try std.testing.expectEqual(@as(usize, 0), duplicate_tick.pages_completed);
                const duplicate_stats = duplicate_worker_pool.graphMetricRuntimeStats();
                try std.testing.expect(duplicate_stats.enabled);
                try std.testing.expectEqual(types.GraphMetricRuntimeRole.worker_pool, duplicate_stats.role.?);
                try std.testing.expect(duplicate_stats.lease_owned);
                try std.testing.expect(!duplicate_stats.has_lease);
                try std.testing.expectEqual(stats.worker_id_hash, duplicate_stats.worker_id_hash);
                try std.testing.expectEqual(stats.lease_key_hash, duplicate_stats.lease_key_hash);
                try std.testing.expectEqual(@as(u64, 0), duplicate_stats.acquisition_count);
                try std.testing.expectEqual(@as(u64, 1), duplicate_stats.lease_acquire_failures);
                saw_live_duplicate_worker_pool_fenced = true;
            }
            break :blk tick;
        };
        worker_total.add(worker_tick);

        const coordinator_tick = blk: {
            var coordinator = try DB.open(alloc, std.mem.span(path), .{
                .open_mode = .writer_no_replay,
                .ttl_cleanup = .{ .enabled = false },
                .graph_metric_maintenance = .{
                    .enabled = true,
                    .start_background_loop = false,
                    .role = .coordinator,
                    .runtime_id = "runtime-reopened-pagerank-pool-coordinator-owner",
                    .lease_owned = true,
                    .owner_id = "runtime-reopened-pagerank-pool-coordinator-owner",
                    .planned_options = .{
                        .worker_id = "runtime-reopened-pagerank-pool-coordinator-unused",
                        .max_rounds = 1,
                        .max_metrics_per_round = 8,
                        .max_pages_per_round = 1,
                    },
                },
            });
            defer coordinator.close();

            const tick = try coordinator.graph_metric_runtime.?.runOnceDetailed();
            const stats = coordinator.graphMetricRuntimeStats();
            try std.testing.expect(stats.enabled);
            try std.testing.expectEqual(types.GraphMetricRuntimeRole.coordinator, stats.role.?);
            try std.testing.expectEqual(std.hash.Wyhash.hash(0, "runtime-reopened-pagerank-pool-coordinator-owner"), stats.runtime_id_hash);
            try std.testing.expectEqual(std.hash.Wyhash.hash(0, "runtime-reopened-pagerank-pool-coordinator-owner"), stats.owner_id_hash);
            try std.testing.expect(stats.lease_owned);
            try std.testing.expect(stats.has_lease);
            try std.testing.expect(!stats.started);
            try std.testing.expectEqual(@as(u64, 0), stats.worker_id_hash);
            try std.testing.expectEqual(@as(u64, 0), stats.worker_count);
            try std.testing.expectEqual(@as(u64, 0), stats.total_worker_steps);
            break :blk tick;
        };
        coordinator_total.add(coordinator_tick);

        {
            var reader = try DB.open(alloc, std.mem.span(path), .{
                .open_mode = .query_readonly,
                .start_index_workers = false,
                .ttl_cleanup = .{ .enabled = false },
            });
            defer reader.close();

            const graph_entry = reader.core.graphIndex("graph_idx") orelse return error.IndexNotFound;
            var status = try graph_entry.index.graphMetricStatus("pagerank");
            defer status.deinit(alloc);
            if (status.state == .fresh and status.published_generation == target_generation) {
                fresh = true;
                break;
            }
        }

        if (!worker_tick.durableProgressed() and !coordinator_tick.durableProgressed()) {
            return error.GraphMetricBuildNoEligiblePage;
        }
    }
    try std.testing.expect(fresh);
    try std.testing.expect(saw_worker_pool_role);
    try std.testing.expect(saw_live_duplicate_worker_pool_fenced);
    try std.testing.expect(coordinator_total.builds_started > 0);
    try std.testing.expect(coordinator_total.coordinator_steps > 0);
    try std.testing.expect(coordinator_total.phases_advanced > 0);
    try std.testing.expect(coordinator_total.published > 0);
    try std.testing.expectEqual(@as(usize, 0), coordinator_total.worker_steps);
    try std.testing.expect(worker_total.worker_steps > 0);
    try std.testing.expect(worker_total.pages_completed > 0);
    try std.testing.expectEqual(@as(usize, 0), worker_total.coordinator_steps);

    {
        var reader = try DB.open(alloc, std.mem.span(path), .{
            .open_mode = .query_readonly,
            .start_index_workers = false,
            .ttl_cleanup = .{ .enabled = false },
        });
        defer reader.close();

        var metric_result = try reader.search(alloc, .{
            .graph_metric_queries = &.{.{
                .name = "pagerank",
                .query = .{
                    .index_name = "graph_idx",
                    .metric_name = "pagerank",
                    .top_k = 2,
                    .freshness = .fresh,
                },
            }},
            .limit = 0,
        });
        defer metric_result.deinit();
        try std.testing.expectEqual(@as(usize, 1), metric_result.graph_metric_results.len);
        try std.testing.expectEqual(graph_mod.GraphIndex.GraphMetricState.fresh, metric_result.graph_metric_results[0].status.state);
        try std.testing.expectEqual(target_generation, metric_result.graph_metric_results[0].status.published_generation);
        try std.testing.expectEqual(@as(usize, 2), metric_result.graph_metric_results[0].scores.len);
        try std.testing.expectEqualStrings("doc:d", metric_result.graph_metric_results[0].scores[0].node);
        try std.testing.expect(metric_result.graph_metric_results[0].scores[0].score >= metric_result.graph_metric_results[0].scores[1].score);
    }
}

test "db graph metric runtime open-configured eigenvector worker pool survives separate reopened handles" {
    const alloc = std.testing.allocator;

    var path_buf: [256]u8 = undefined;
    const path = tempPath(&path_buf);
    defer cleanupTempDir(path);

    var target_generation: u64 = 0;
    {
        var db = try DB.open(alloc, std.mem.span(path), .{
            .start_index_workers = false,
            .ttl_cleanup = .{ .enabled = false },
        });
        defer db.close();

        try db.addIndex(.{
            .name = "graph_idx",
            .kind = .graph,
            .config_json = "{\"metrics\":{\"eigenvector\":{\"enabled\":true,\"kind\":\"eigenvector\",\"refresh\":\"background\",\"max_iterations\":1,\"tolerance\":0.000001,\"edge_filter\":{\"types\":[\"cites\"]}}}}",
        });

        try db.batch(.{
            .writes = &.{
                .{ .key = "doc:a", .value = "{\"title\":\"alpha\",\"_edges\":{\"graph_idx\":{\"cites\":[{\"target\":\"doc:b\",\"weight\":1.0}]}}}" },
                .{ .key = "doc:b", .value = "{\"title\":\"beta\",\"_edges\":{\"graph_idx\":{\"cites\":[{\"target\":\"doc:d\",\"weight\":1.0}]}}}" },
                .{ .key = "doc:c", .value = "{\"title\":\"gamma\",\"_edges\":{\"graph_idx\":{\"cites\":[{\"target\":\"doc:b\",\"weight\":1.0}]}}}" },
                .{ .key = "doc:d", .value = "{\"title\":\"delta\"}" },
            },
            .sync_level = .write,
        });

        try db.runDerivedUntil(db.core.nextDerivedSequence());

        const graph_entry = db.core.graphIndex("graph_idx") orelse return error.IndexNotFound;
        target_generation = graph_entry.index.edge_generation;
    }

    const workers = [_][]const u8{ "runtime-reopened-eigenvector-pool-worker-a", "runtime-reopened-eigenvector-pool-worker-b" };
    const reversed_workers = [_][]const u8{ "runtime-reopened-eigenvector-pool-worker-b", "runtime-reopened-eigenvector-pool-worker-a" };
    var coordinator_total = index_manager_mod.IndexManager.GraphMetricPlannedSchedulerSweepResult{};
    var worker_total = index_manager_mod.IndexManager.GraphMetricPlannedSchedulerSweepResult{};
    var saw_worker_pool_role = false;
    var saw_live_duplicate_worker_pool_fenced = false;
    var fresh = false;
    for (0..500) |_| {
        const worker_tick = blk: {
            var worker_pool = try DB.open(alloc, std.mem.span(path), .{
                .open_mode = .writer_no_replay,
                .ttl_cleanup = .{ .enabled = false },
                .graph_metric_maintenance = .{
                    .enabled = true,
                    .start_background_loop = false,
                    .role = .worker_pool,
                    .runtime_id = "runtime-reopened-eigenvector-pool-worker-owner",
                    .lease_owned = true,
                    .owner_id = "runtime-reopened-eigenvector-pool-worker-owner",
                    .planned_options = .{
                        .worker_ids = &workers,
                        .max_rounds = 1,
                        .max_metrics_per_round = 8,
                        .max_pages_per_round = 2,
                    },
                },
            });
            defer worker_pool.close();

            const tick = try worker_pool.graph_metric_runtime.?.runOnceDetailed();
            const stats = worker_pool.graphMetricRuntimeStats();
            try std.testing.expect(stats.enabled);
            try std.testing.expectEqual(types.GraphMetricRuntimeRole.worker_pool, stats.role.?);
            try std.testing.expectEqual(std.hash.Wyhash.hash(0, "runtime-reopened-eigenvector-pool-worker-owner"), stats.runtime_id_hash);
            try std.testing.expectEqual(std.hash.Wyhash.hash(0, "runtime-reopened-eigenvector-pool-worker-owner"), stats.owner_id_hash);
            try std.testing.expect(stats.lease_owned);
            try std.testing.expect(stats.has_lease);
            try std.testing.expect(!stats.started);
            const expected_worker_hash = graph_metric_runtime_mod.workerSetIdentityHash(workers[0..]);
            try std.testing.expectEqual(expected_worker_hash, stats.worker_id_hash);
            try std.testing.expectEqual(@as(u64, 2), stats.worker_count);
            try std.testing.expectEqual(@as(u64, 0), stats.total_coordinator_steps);
            saw_worker_pool_role = true;
            if (tick.worker_steps > 0) {
                var duplicate_worker_pool = try DB.open(alloc, std.mem.span(path), .{
                    .open_mode = .writer_no_replay,
                    .ttl_cleanup = .{ .enabled = false },
                    .graph_metric_maintenance = .{
                        .enabled = true,
                        .start_background_loop = false,
                        .role = .worker_pool,
                        .runtime_id = "runtime-reopened-eigenvector-pool-worker-owner-duplicate",
                        .lease_owned = true,
                        .owner_id = "runtime-reopened-eigenvector-pool-worker-owner-duplicate",
                        .planned_options = .{
                            .worker_ids = &reversed_workers,
                            .max_rounds = 1,
                            .max_metrics_per_round = 8,
                            .max_pages_per_round = 2,
                        },
                    },
                });
                defer duplicate_worker_pool.close();

                const duplicate_tick = try duplicate_worker_pool.graph_metric_runtime.?.runOnceDetailed();
                try std.testing.expect(!duplicate_tick.durableProgressed());
                try std.testing.expectEqual(@as(usize, 0), duplicate_tick.worker_steps);
                try std.testing.expectEqual(@as(usize, 0), duplicate_tick.pages_completed);
                const duplicate_stats = duplicate_worker_pool.graphMetricRuntimeStats();
                try std.testing.expect(duplicate_stats.enabled);
                try std.testing.expectEqual(types.GraphMetricRuntimeRole.worker_pool, duplicate_stats.role.?);
                try std.testing.expect(duplicate_stats.lease_owned);
                try std.testing.expect(!duplicate_stats.has_lease);
                try std.testing.expectEqual(stats.worker_id_hash, duplicate_stats.worker_id_hash);
                try std.testing.expectEqual(stats.lease_key_hash, duplicate_stats.lease_key_hash);
                try std.testing.expectEqual(@as(u64, 0), duplicate_stats.acquisition_count);
                try std.testing.expectEqual(@as(u64, 1), duplicate_stats.lease_acquire_failures);
                saw_live_duplicate_worker_pool_fenced = true;
            }
            break :blk tick;
        };
        worker_total.add(worker_tick);

        const coordinator_tick = blk: {
            var coordinator = try DB.open(alloc, std.mem.span(path), .{
                .open_mode = .writer_no_replay,
                .ttl_cleanup = .{ .enabled = false },
                .graph_metric_maintenance = .{
                    .enabled = true,
                    .start_background_loop = false,
                    .role = .coordinator,
                    .runtime_id = "runtime-reopened-eigenvector-pool-coordinator-owner",
                    .lease_owned = true,
                    .owner_id = "runtime-reopened-eigenvector-pool-coordinator-owner",
                    .planned_options = .{
                        .worker_id = "runtime-reopened-eigenvector-pool-coordinator-unused",
                        .max_rounds = 1,
                        .max_metrics_per_round = 8,
                        .max_pages_per_round = 1,
                    },
                },
            });
            defer coordinator.close();

            const tick = try coordinator.graph_metric_runtime.?.runOnceDetailed();
            const stats = coordinator.graphMetricRuntimeStats();
            try std.testing.expect(stats.enabled);
            try std.testing.expectEqual(types.GraphMetricRuntimeRole.coordinator, stats.role.?);
            try std.testing.expectEqual(std.hash.Wyhash.hash(0, "runtime-reopened-eigenvector-pool-coordinator-owner"), stats.runtime_id_hash);
            try std.testing.expectEqual(std.hash.Wyhash.hash(0, "runtime-reopened-eigenvector-pool-coordinator-owner"), stats.owner_id_hash);
            try std.testing.expect(stats.lease_owned);
            try std.testing.expect(stats.has_lease);
            try std.testing.expect(!stats.started);
            try std.testing.expectEqual(@as(u64, 0), stats.worker_id_hash);
            try std.testing.expectEqual(@as(u64, 0), stats.worker_count);
            try std.testing.expectEqual(@as(u64, 0), stats.total_worker_steps);
            break :blk tick;
        };
        coordinator_total.add(coordinator_tick);

        {
            var reader = try DB.open(alloc, std.mem.span(path), .{
                .open_mode = .query_readonly,
                .start_index_workers = false,
                .ttl_cleanup = .{ .enabled = false },
            });
            defer reader.close();

            const graph_entry = reader.core.graphIndex("graph_idx") orelse return error.IndexNotFound;
            var status = try graph_entry.index.graphMetricStatus("eigenvector");
            defer status.deinit(alloc);
            if (status.state == .fresh and status.published_generation == target_generation) {
                fresh = true;
                break;
            }
        }

        if (!worker_tick.durableProgressed() and !coordinator_tick.durableProgressed()) {
            return error.GraphMetricBuildNoEligiblePage;
        }
    }
    try std.testing.expect(fresh);
    try std.testing.expect(saw_worker_pool_role);
    try std.testing.expect(saw_live_duplicate_worker_pool_fenced);
    try std.testing.expect(coordinator_total.builds_started > 0);
    try std.testing.expect(coordinator_total.coordinator_steps > 0);
    try std.testing.expect(coordinator_total.phases_advanced > 0);
    try std.testing.expect(coordinator_total.published > 0);
    try std.testing.expectEqual(@as(usize, 0), coordinator_total.worker_steps);
    try std.testing.expect(worker_total.worker_steps > 0);
    try std.testing.expect(worker_total.pages_completed > 0);
    try std.testing.expectEqual(@as(usize, 0), worker_total.coordinator_steps);

    {
        var reader = try DB.open(alloc, std.mem.span(path), .{
            .open_mode = .query_readonly,
            .start_index_workers = false,
            .ttl_cleanup = .{ .enabled = false },
        });
        defer reader.close();

        var metric_result = try reader.search(alloc, .{
            .graph_metric_queries = &.{.{
                .name = "eigenvector",
                .query = .{
                    .index_name = "graph_idx",
                    .metric_name = "eigenvector",
                    .top_k = 2,
                    .freshness = .fresh,
                },
            }},
            .limit = 0,
        });
        defer metric_result.deinit();
        try std.testing.expectEqual(@as(usize, 1), metric_result.graph_metric_results.len);
        try std.testing.expectEqual(graph_mod.GraphIndex.GraphMetricState.fresh, metric_result.graph_metric_results[0].status.state);
        try std.testing.expectEqual(target_generation, metric_result.graph_metric_results[0].status.published_generation);
        try std.testing.expectEqual(@as(usize, 2), metric_result.graph_metric_results[0].scores.len);
    }
}

test "db graph metric runtime open-configured hits worker pool survives separate reopened handles" {
    const alloc = std.testing.allocator;

    var path_buf: [256]u8 = undefined;
    const path = tempPath(&path_buf);
    defer cleanupTempDir(path);

    var target_generation: u64 = 0;
    {
        var db = try DB.open(alloc, std.mem.span(path), .{
            .start_index_workers = false,
            .ttl_cleanup = .{ .enabled = false },
        });
        defer db.close();

        try db.addIndex(.{
            .name = "graph_idx",
            .kind = .graph,
            .config_json = "{\"metrics\":{\"hits_authority\":{\"enabled\":true,\"kind\":\"hits_authority\",\"refresh\":\"background\",\"max_iterations\":1,\"tolerance\":0.000001,\"edge_filter\":{\"types\":[\"cites\"]}},\"hits_hub\":{\"enabled\":true,\"kind\":\"hits_hub\",\"refresh\":\"background\",\"max_iterations\":1,\"tolerance\":0.000001,\"edge_filter\":{\"types\":[\"cites\"]}}}}",
        });

        try db.batch(.{
            .writes = &.{
                .{ .key = "doc:hub-a", .value = "{\"title\":\"hub a\",\"_edges\":{\"graph_idx\":{\"cites\":[{\"target\":\"doc:authority\",\"weight\":1.0}]}}}" },
                .{ .key = "doc:hub-b", .value = "{\"title\":\"hub b\",\"_edges\":{\"graph_idx\":{\"cites\":[{\"target\":\"doc:authority\",\"weight\":1.0}]}}}" },
                .{ .key = "doc:authority", .value = "{\"title\":\"authority\"}" },
            },
            .sync_level = .write,
        });

        try db.runDerivedUntil(db.core.nextDerivedSequence());

        const graph_entry = db.core.graphIndex("graph_idx") orelse return error.IndexNotFound;
        target_generation = graph_entry.index.edge_generation;
    }

    const workers = [_][]const u8{ "runtime-reopened-hits-pool-worker-a", "runtime-reopened-hits-pool-worker-b" };
    const reversed_workers = [_][]const u8{ "runtime-reopened-hits-pool-worker-b", "runtime-reopened-hits-pool-worker-a" };
    var coordinator_total = index_manager_mod.IndexManager.GraphMetricPlannedSchedulerSweepResult{};
    var worker_total = index_manager_mod.IndexManager.GraphMetricPlannedSchedulerSweepResult{};
    var saw_worker_pool_role = false;
    var saw_live_duplicate_worker_pool_fenced = false;
    var fresh = false;
    for (0..500) |_| {
        const worker_tick = blk: {
            var worker_pool = try DB.open(alloc, std.mem.span(path), .{
                .open_mode = .writer_no_replay,
                .ttl_cleanup = .{ .enabled = false },
                .graph_metric_maintenance = .{
                    .enabled = true,
                    .start_background_loop = false,
                    .role = .worker_pool,
                    .runtime_id = "runtime-reopened-hits-pool-worker-owner",
                    .lease_owned = true,
                    .owner_id = "runtime-reopened-hits-pool-worker-owner",
                    .planned_options = .{
                        .worker_ids = &workers,
                        .max_rounds = 1,
                        .max_metrics_per_round = 8,
                        .max_pages_per_round = 2,
                    },
                },
            });
            defer worker_pool.close();

            const tick = try worker_pool.graph_metric_runtime.?.runOnceDetailed();
            const stats = worker_pool.graphMetricRuntimeStats();
            try std.testing.expect(stats.enabled);
            try std.testing.expectEqual(types.GraphMetricRuntimeRole.worker_pool, stats.role.?);
            try std.testing.expectEqual(std.hash.Wyhash.hash(0, "runtime-reopened-hits-pool-worker-owner"), stats.runtime_id_hash);
            try std.testing.expectEqual(std.hash.Wyhash.hash(0, "runtime-reopened-hits-pool-worker-owner"), stats.owner_id_hash);
            try std.testing.expect(stats.lease_owned);
            try std.testing.expect(stats.has_lease);
            try std.testing.expect(!stats.started);
            const expected_worker_hash = graph_metric_runtime_mod.workerSetIdentityHash(workers[0..]);
            try std.testing.expectEqual(expected_worker_hash, stats.worker_id_hash);
            try std.testing.expectEqual(@as(u64, 2), stats.worker_count);
            try std.testing.expectEqual(@as(u64, 0), stats.total_coordinator_steps);
            saw_worker_pool_role = true;
            if (tick.worker_steps > 0) {
                var duplicate_worker_pool = try DB.open(alloc, std.mem.span(path), .{
                    .open_mode = .writer_no_replay,
                    .ttl_cleanup = .{ .enabled = false },
                    .graph_metric_maintenance = .{
                        .enabled = true,
                        .start_background_loop = false,
                        .role = .worker_pool,
                        .runtime_id = "runtime-reopened-hits-pool-worker-owner-duplicate",
                        .lease_owned = true,
                        .owner_id = "runtime-reopened-hits-pool-worker-owner-duplicate",
                        .planned_options = .{
                            .worker_ids = &reversed_workers,
                            .max_rounds = 1,
                            .max_metrics_per_round = 8,
                            .max_pages_per_round = 2,
                        },
                    },
                });
                defer duplicate_worker_pool.close();

                const duplicate_tick = try duplicate_worker_pool.graph_metric_runtime.?.runOnceDetailed();
                try std.testing.expect(!duplicate_tick.durableProgressed());
                try std.testing.expectEqual(@as(usize, 0), duplicate_tick.worker_steps);
                try std.testing.expectEqual(@as(usize, 0), duplicate_tick.pages_completed);
                const duplicate_stats = duplicate_worker_pool.graphMetricRuntimeStats();
                try std.testing.expect(duplicate_stats.enabled);
                try std.testing.expectEqual(types.GraphMetricRuntimeRole.worker_pool, duplicate_stats.role.?);
                try std.testing.expect(duplicate_stats.lease_owned);
                try std.testing.expect(!duplicate_stats.has_lease);
                try std.testing.expectEqual(stats.worker_id_hash, duplicate_stats.worker_id_hash);
                try std.testing.expectEqual(stats.lease_key_hash, duplicate_stats.lease_key_hash);
                try std.testing.expectEqual(@as(u64, 0), duplicate_stats.acquisition_count);
                try std.testing.expectEqual(@as(u64, 1), duplicate_stats.lease_acquire_failures);
                saw_live_duplicate_worker_pool_fenced = true;
            }
            break :blk tick;
        };
        worker_total.add(worker_tick);

        const coordinator_tick = blk: {
            var coordinator = try DB.open(alloc, std.mem.span(path), .{
                .open_mode = .writer_no_replay,
                .ttl_cleanup = .{ .enabled = false },
                .graph_metric_maintenance = .{
                    .enabled = true,
                    .start_background_loop = false,
                    .role = .coordinator,
                    .runtime_id = "runtime-reopened-hits-pool-coordinator-owner",
                    .lease_owned = true,
                    .owner_id = "runtime-reopened-hits-pool-coordinator-owner",
                    .planned_options = .{
                        .worker_id = "runtime-reopened-hits-pool-coordinator-unused",
                        .max_rounds = 1,
                        .max_metrics_per_round = 8,
                        .max_pages_per_round = 1,
                    },
                },
            });
            defer coordinator.close();

            const tick = try coordinator.graph_metric_runtime.?.runOnceDetailed();
            const stats = coordinator.graphMetricRuntimeStats();
            try std.testing.expect(stats.enabled);
            try std.testing.expectEqual(types.GraphMetricRuntimeRole.coordinator, stats.role.?);
            try std.testing.expectEqual(std.hash.Wyhash.hash(0, "runtime-reopened-hits-pool-coordinator-owner"), stats.runtime_id_hash);
            try std.testing.expectEqual(std.hash.Wyhash.hash(0, "runtime-reopened-hits-pool-coordinator-owner"), stats.owner_id_hash);
            try std.testing.expect(stats.lease_owned);
            try std.testing.expect(stats.has_lease);
            try std.testing.expect(!stats.started);
            try std.testing.expectEqual(@as(u64, 0), stats.worker_id_hash);
            try std.testing.expectEqual(@as(u64, 0), stats.worker_count);
            try std.testing.expectEqual(@as(u64, 0), stats.total_worker_steps);
            break :blk tick;
        };
        coordinator_total.add(coordinator_tick);

        {
            var reader = try DB.open(alloc, std.mem.span(path), .{
                .open_mode = .query_readonly,
                .start_index_workers = false,
                .ttl_cleanup = .{ .enabled = false },
            });
            defer reader.close();

            const graph_entry = reader.core.graphIndex("graph_idx") orelse return error.IndexNotFound;
            var authority_status = try graph_entry.index.graphMetricStatus("hits_authority");
            defer authority_status.deinit(alloc);
            var hub_status = try graph_entry.index.graphMetricStatus("hits_hub");
            defer hub_status.deinit(alloc);
            if (authority_status.state == .fresh and
                hub_status.state == .fresh and
                authority_status.published_generation == target_generation and
                hub_status.published_generation == target_generation)
            {
                fresh = true;
                break;
            }
        }

        if (!worker_tick.durableProgressed() and !coordinator_tick.durableProgressed()) {
            return error.GraphMetricBuildNoEligiblePage;
        }
    }
    try std.testing.expect(fresh);
    try std.testing.expect(saw_worker_pool_role);
    try std.testing.expect(saw_live_duplicate_worker_pool_fenced);
    try std.testing.expect(coordinator_total.builds_started > 0);
    try std.testing.expect(coordinator_total.coordinator_steps > 0);
    try std.testing.expect(coordinator_total.phases_advanced > 0);
    try std.testing.expect(coordinator_total.published > 0);
    try std.testing.expectEqual(@as(usize, 0), coordinator_total.worker_steps);
    try std.testing.expect(worker_total.worker_steps > 0);
    try std.testing.expect(worker_total.pages_completed > 0);
    try std.testing.expectEqual(@as(usize, 0), worker_total.coordinator_steps);

    {
        var reader = try DB.open(alloc, std.mem.span(path), .{
            .open_mode = .query_readonly,
            .start_index_workers = false,
            .ttl_cleanup = .{ .enabled = false },
        });
        defer reader.close();

        var metric_result = try reader.search(alloc, .{
            .graph_metric_queries = &.{
                .{
                    .name = "authority",
                    .query = .{
                        .index_name = "graph_idx",
                        .metric_name = "hits_authority",
                        .top_k = 3,
                        .freshness = .fresh,
                    },
                },
                .{
                    .name = "hub",
                    .query = .{
                        .index_name = "graph_idx",
                        .metric_name = "hits_hub",
                        .top_k = 3,
                        .freshness = .fresh,
                    },
                },
            },
            .limit = 0,
        });
        defer metric_result.deinit();
        try std.testing.expectEqual(@as(usize, 2), metric_result.graph_metric_results.len);
        try std.testing.expectEqual(graph_mod.GraphIndex.GraphMetricState.fresh, metric_result.graph_metric_results[0].status.state);
        try std.testing.expectEqual(target_generation, metric_result.graph_metric_results[0].status.published_generation);
        try std.testing.expectEqual(graph_mod.GraphIndex.GraphMetricState.fresh, metric_result.graph_metric_results[1].status.state);
        try std.testing.expectEqual(metric_result.graph_metric_results[0].status.published_generation, metric_result.graph_metric_results[1].status.published_generation);
        try std.testing.expectEqualStrings("doc:authority", metric_result.graph_metric_results[0].scores[0].node);
        try std.testing.expectApproxEqAbs(@as(f64, 1.0), metric_result.graph_metric_results[0].scores[0].score, 0.001);
    }
}

test "db graph metric runtime split ticks survive reopened pagerank handles" {
    const alloc = std.testing.allocator;

    var path_buf: [256]u8 = undefined;
    const path = tempPath(&path_buf);
    defer cleanupTempDir(path);

    var target_generation: u64 = 0;
    {
        var db = try DB.open(alloc, std.mem.span(path), .{
            .start_index_workers = false,
            .ttl_cleanup = .{ .enabled = false },
        });
        defer db.close();

        try db.addIndex(.{
            .name = "graph_idx",
            .kind = .graph,
            .config_json = "{\"metrics\":{\"pagerank\":{\"enabled\":true,\"kind\":\"pagerank\",\"refresh\":\"background\",\"max_iterations\":2,\"tolerance\":0.000000001,\"edge_filter\":{\"types\":[\"cites\"]}}}}",
        });

        try db.batch(.{
            .writes = &.{
                .{ .key = "doc:a", .value = "{\"title\":\"alpha\",\"_edges\":{\"graph_idx\":{\"cites\":[{\"target\":\"doc:b\",\"weight\":1.0}]}}}" },
                .{ .key = "doc:b", .value = "{\"title\":\"beta\",\"_edges\":{\"graph_idx\":{\"cites\":[{\"target\":\"doc:d\",\"weight\":1.0}]}}}" },
                .{ .key = "doc:c", .value = "{\"title\":\"gamma\",\"_edges\":{\"graph_idx\":{\"cites\":[{\"target\":\"doc:b\",\"weight\":1.0}]}}}" },
                .{ .key = "doc:d", .value = "{\"title\":\"delta\"}" },
            },
            .sync_level = .write,
        });

        try db.runDerivedUntil(db.core.nextDerivedSequence());

        const graph_entry = db.core.graphIndex("graph_idx") orelse return error.IndexNotFound;
        var status = try graph_entry.index.graphMetricStatus("pagerank");
        defer status.deinit(alloc);
        try std.testing.expectEqual(graph_mod.GraphIndex.GraphMetricState.not_ready, status.state);
        target_generation = graph_entry.index.edge_generation;
    }

    {
        var coordinator = try DB.open(alloc, std.mem.span(path), .{
            .start_index_workers = false,
            .ttl_cleanup = .{ .enabled = false },
        });
        defer coordinator.close();

        const resources = coordinator.core.asyncResources();
        var runtime = try graph_metric_runtime_mod.GraphMetricRuntime.init(
            alloc,
            resources.store,
            resources.index_manager,
            resources.apply_mutex,
            coordinator.backend_runtime,
            .{
                .enabled = true,
                .role = .coordinator,
                .runtime_id = "runtime-reopened-coordinator",
                .lease_owned = true,
                .owner_id = "runtime-reopened-coordinator",
                .planned_options = .{
                    .worker_id = "runtime-reopened-coordinator",
                    .max_rounds = 1,
                    .max_metrics_per_round = 8,
                    .max_pages_per_round = 1,
                },
            },
        );
        defer runtime.deinit();

        const started = try runtime.runCoordinatorOnce(true);
        try std.testing.expectEqual(@as(usize, 1), started.builds_started);
        try std.testing.expectEqual(@as(usize, 0), started.worker_steps);
        try std.testing.expectEqual(@as(usize, 0), started.pages_completed);
        {
            const stats = runtime.stats();
            try std.testing.expect(stats.lease_owned);
            try std.testing.expect(stats.has_lease);
            try std.testing.expectEqual(graph_metric_runtime_mod.Role.coordinator, stats.role);
        }
    }

    {
        var worker = try DB.open(alloc, std.mem.span(path), .{
            .start_index_workers = false,
            .ttl_cleanup = .{ .enabled = false },
        });
        defer worker.close();

        const resources = worker.core.asyncResources();
        var runtime = try graph_metric_runtime_mod.GraphMetricRuntime.init(
            alloc,
            resources.store,
            resources.index_manager,
            resources.apply_mutex,
            worker.backend_runtime,
            .{
                .enabled = true,
                .role = .worker,
                .runtime_id = "runtime-reopened-worker-a",
                .lease_owned = true,
                .owner_id = "runtime-reopened-worker-a",
                .planned_options = .{
                    .worker_id = "runtime-reopened-worker-a",
                    .max_rounds = 1,
                    .max_metrics_per_round = 8,
                    .max_pages_per_round = 1,
                },
            },
        );
        defer runtime.deinit();

        const prepared = try runtime.runWorkerOnce("runtime-reopened-worker-a");
        try std.testing.expectEqual(@as(usize, 1), prepared.worker_steps);
        try std.testing.expectEqual(@as(usize, 1), prepared.pages_completed);
        try std.testing.expectEqual(@as(usize, 0), prepared.phases_advanced);
        try std.testing.expectEqual(@as(usize, 0), prepared.published);
        {
            const stats = runtime.stats();
            try std.testing.expect(stats.lease_owned);
            try std.testing.expect(stats.has_lease);
            try std.testing.expectEqual(graph_metric_runtime_mod.Role.worker, stats.role);
        }
    }

    {
        var reader = try DB.open(alloc, std.mem.span(path), .{
            .start_index_workers = false,
            .ttl_cleanup = .{ .enabled = false },
        });
        defer reader.close();

        const graph_entry = reader.core.graphIndex("graph_idx") orelse return error.IndexNotFound;
        var status = try graph_entry.index.graphMetricStatus("pagerank");
        defer status.deinit(alloc);
        try std.testing.expectEqual(graph_mod.GraphIndex.GraphMetricState.building, status.state);
        try std.testing.expectEqual(graph_mod.GraphIndex.GraphMetricBuildPhase.prepare_generation, status.phase);
    }

    {
        var coordinator = try DB.open(alloc, std.mem.span(path), .{
            .start_index_workers = false,
            .ttl_cleanup = .{ .enabled = false },
        });
        defer coordinator.close();

        const resources = coordinator.core.asyncResources();
        var runtime = try graph_metric_runtime_mod.GraphMetricRuntime.init(
            alloc,
            resources.store,
            resources.index_manager,
            resources.apply_mutex,
            coordinator.backend_runtime,
            .{
                .enabled = true,
                .role = .coordinator,
                .runtime_id = "runtime-reopened-coordinator",
                .lease_owned = true,
                .owner_id = "runtime-reopened-coordinator",
                .planned_options = .{
                    .worker_id = "runtime-reopened-coordinator",
                    .max_rounds = 1,
                    .max_metrics_per_round = 8,
                    .max_pages_per_round = 1,
                },
            },
        );
        defer runtime.deinit();

        const advanced = try runtime.runCoordinatorOnce(false);
        try std.testing.expectEqual(@as(usize, 0), advanced.builds_started);
        try std.testing.expect(advanced.phases_advanced > 0);
        {
            const stats = runtime.stats();
            try std.testing.expect(stats.lease_owned);
            try std.testing.expect(stats.has_lease);
            try std.testing.expectEqual(graph_metric_runtime_mod.Role.coordinator, stats.role);
        }
    }

    const workers = [_][]const u8{ "runtime-reopened-worker-a", "runtime-reopened-worker-b" };
    var finished = false;
    var step_index: usize = 0;
    while (step_index < 400) : (step_index += 1) {
        const worker_tick = blk: {
            var worker = try DB.open(alloc, std.mem.span(path), .{
                .start_index_workers = false,
                .ttl_cleanup = .{ .enabled = false },
            });
            defer worker.close();

            const resources = worker.core.asyncResources();
            var runtime = try graph_metric_runtime_mod.GraphMetricRuntime.init(
                alloc,
                resources.store,
                resources.index_manager,
                resources.apply_mutex,
                worker.backend_runtime,
                .{
                    .enabled = true,
                    .role = .worker,
                    .runtime_id = workers[step_index % workers.len],
                    .lease_owned = true,
                    .owner_id = workers[step_index % workers.len],
                    .planned_options = .{
                        .worker_id = workers[step_index % workers.len],
                        .max_rounds = 1,
                        .max_metrics_per_round = 8,
                        .max_pages_per_round = 1,
                    },
                },
            );
            defer runtime.deinit();
            break :blk try runtime.runWorkerOnce(workers[step_index % workers.len]);
        };

        const coordinator_tick = blk: {
            var coordinator = try DB.open(alloc, std.mem.span(path), .{
                .start_index_workers = false,
                .ttl_cleanup = .{ .enabled = false },
            });
            defer coordinator.close();

            const resources = coordinator.core.asyncResources();
            var runtime = try graph_metric_runtime_mod.GraphMetricRuntime.init(
                alloc,
                resources.store,
                resources.index_manager,
                resources.apply_mutex,
                coordinator.backend_runtime,
                .{
                    .enabled = true,
                    .role = .coordinator,
                    .runtime_id = "runtime-reopened-coordinator",
                    .lease_owned = true,
                    .owner_id = "runtime-reopened-coordinator",
                    .planned_options = .{
                        .worker_id = "runtime-reopened-coordinator",
                        .max_rounds = 1,
                        .max_metrics_per_round = 8,
                        .max_pages_per_round = 1,
                    },
                },
            );
            defer runtime.deinit();
            const tick = try runtime.runCoordinatorOnce(false);
            const graph_entry = coordinator.core.graphIndex("graph_idx") orelse return error.IndexNotFound;
            var status = try graph_entry.index.graphMetricStatus("pagerank");
            defer status.deinit(alloc);
            if (status.state == .fresh and status.published_generation == target_generation and status.phase == .complete) {
                try std.testing.expect(status.iterations_completed > 0);
                finished = true;
            }
            break :blk tick;
        };

        if (finished) break;
        if (!worker_tick.durableProgressed() and !coordinator_tick.durableProgressed()) {
            return error.GraphMetricBuildNoEligiblePage;
        }
    }
    try std.testing.expect(finished);

    {
        var reader = try DB.open(alloc, std.mem.span(path), .{
            .start_index_workers = false,
            .ttl_cleanup = .{ .enabled = false },
        });
        defer reader.close();

        var published_result = try reader.search(alloc, .{
            .graph_metric_queries = &.{.{
                .name = "central",
                .query = .{
                    .index_name = "graph_idx",
                    .metric_name = "pagerank",
                    .top_k = 2,
                    .freshness = .fresh,
                },
            }},
            .limit = 0,
        });
        defer published_result.deinit();
        try std.testing.expectEqual(@as(usize, 1), published_result.graph_metric_results.len);
        try std.testing.expectEqual(graph_mod.GraphIndex.GraphMetricState.fresh, published_result.graph_metric_results[0].status.state);
        try std.testing.expectEqual(target_generation, published_result.graph_metric_results[0].status.published_generation);
        try std.testing.expectEqual(@as(usize, 2), published_result.graph_metric_results[0].scores.len);
        try std.testing.expectEqualStrings("doc:d", published_result.graph_metric_results[0].scores[0].node);
        try std.testing.expect(published_result.graph_metric_results[0].scores[0].score >= published_result.graph_metric_results[0].scores[1].score);
    }
}

test "db graph metric runtime reopened coordinators do not duplicate pagerank publish" {
    const alloc = std.testing.allocator;

    var path_buf: [256]u8 = undefined;
    const path = tempPath(&path_buf);
    defer cleanupTempDir(path);

    var target_generation: u64 = 0;
    {
        var db = try DB.open(alloc, std.mem.span(path), .{
            .start_index_workers = false,
            .ttl_cleanup = .{ .enabled = false },
        });
        defer db.close();

        try db.addIndex(.{
            .name = "graph_idx",
            .kind = .graph,
            .config_json = "{\"metrics\":{\"pagerank\":{\"enabled\":true,\"kind\":\"pagerank\",\"refresh\":\"manual\",\"max_iterations\":1,\"tolerance\":0.000001,\"edge_filter\":{\"types\":[\"cites\"]}}}}",
        });

        try db.batch(.{
            .writes = &.{
                .{ .key = "doc:a", .value = "{\"title\":\"alpha\",\"_edges\":{\"graph_idx\":{\"cites\":[{\"target\":\"doc:b\",\"weight\":1.0}]}}}" },
                .{ .key = "doc:b", .value = "{\"title\":\"beta\",\"_edges\":{\"graph_idx\":{\"cites\":[{\"target\":\"doc:d\",\"weight\":1.0}]}}}" },
                .{ .key = "doc:c", .value = "{\"title\":\"gamma\",\"_edges\":{\"graph_idx\":{\"cites\":[{\"target\":\"doc:b\",\"weight\":1.0}]}}}" },
                .{ .key = "doc:d", .value = "{\"title\":\"delta\"}" },
            },
            .sync_level = .write,
        });
        try db.runUntilIdle();

        const graph_entry = db.core.graphIndex("graph_idx") orelse return error.IndexNotFound;
        target_generation = graph_entry.index.edge_generation;
    }

    {
        var coordinator = try DB.open(alloc, std.mem.span(path), .{
            .start_index_workers = false,
            .ttl_cleanup = .{ .enabled = false },
        });
        defer coordinator.close();

        var started = try coordinator.ensureGraphMetricPlannedBuild(alloc, "graph_idx", "pagerank", target_generation);
        defer started.deinit(alloc);
        try std.testing.expectEqual(graph_mod.GraphIndex.GraphMetricState.building, started.state);
        try std.testing.expectEqual(target_generation, started.building_generation);
    }

    const workers = [_][]const u8{ "runtime-publish-race-worker-a", "runtime-publish-race-worker-b" };
    var reached_publish = false;
    var step_index: usize = 0;
    while (step_index < 400) : (step_index += 1) {
        const worker_tick = blk: {
            var worker = try DB.open(alloc, std.mem.span(path), .{
                .start_index_workers = false,
                .ttl_cleanup = .{ .enabled = false },
            });
            defer worker.close();

            const resources = worker.core.asyncResources();
            var runtime = try graph_metric_runtime_mod.GraphMetricRuntime.init(
                alloc,
                resources.store,
                resources.index_manager,
                resources.apply_mutex,
                worker.backend_runtime,
                .{
                    .enabled = true,
                    .role = .worker,
                    .lease_owned = true,
                    .owner_id = workers[step_index % workers.len],
                    .planned_options = .{
                        .worker_id = workers[step_index % workers.len],
                        .max_rounds = 1,
                        .max_metrics_per_round = 8,
                        .max_pages_per_round = 1,
                    },
                },
            );
            defer runtime.deinit();
            break :blk try runtime.runWorkerOnce(workers[step_index % workers.len]);
        };

        {
            var reader = try DB.open(alloc, std.mem.span(path), .{
                .start_index_workers = false,
                .ttl_cleanup = .{ .enabled = false },
            });
            defer reader.close();
            const graph_entry = reader.core.graphIndex("graph_idx") orelse return error.IndexNotFound;
            var status = try graph_entry.index.graphMetricStatus("pagerank");
            defer status.deinit(alloc);
            if (status.phase == .publish_generation) {
                reached_publish = true;
                break;
            }
        }

        const coordinator_tick = blk: {
            var coordinator = try DB.open(alloc, std.mem.span(path), .{
                .start_index_workers = false,
                .ttl_cleanup = .{ .enabled = false },
            });
            defer coordinator.close();

            const resources = coordinator.core.asyncResources();
            var runtime = try graph_metric_runtime_mod.GraphMetricRuntime.init(
                alloc,
                resources.store,
                resources.index_manager,
                resources.apply_mutex,
                coordinator.backend_runtime,
                .{
                    .enabled = true,
                    .role = .coordinator,
                    .lease_owned = true,
                    .owner_id = "runtime-publish-race-coordinator",
                    .planned_options = .{
                        .worker_id = "runtime-publish-race-coordinator",
                        .max_rounds = 1,
                        .max_metrics_per_round = 8,
                        .max_pages_per_round = 1,
                    },
                },
            );
            defer runtime.deinit();
            break :blk try runtime.runCoordinatorOnce(false);
        };

        {
            var reader = try DB.open(alloc, std.mem.span(path), .{
                .start_index_workers = false,
                .ttl_cleanup = .{ .enabled = false },
            });
            defer reader.close();
            const graph_entry = reader.core.graphIndex("graph_idx") orelse return error.IndexNotFound;
            var status = try graph_entry.index.graphMetricStatus("pagerank");
            defer status.deinit(alloc);
            if (status.phase == .publish_generation) {
                reached_publish = true;
                break;
            }
        }

        if (!worker_tick.durableProgressed() and !coordinator_tick.durableProgressed()) {
            return error.GraphMetricBuildNoEligiblePage;
        }
    }
    try std.testing.expect(reached_publish);

    {
        var coordinator = try DB.open(alloc, std.mem.span(path), .{
            .start_index_workers = false,
            .ttl_cleanup = .{ .enabled = false },
        });
        defer coordinator.close();

        const resources = coordinator.core.asyncResources();
        var runtime = try graph_metric_runtime_mod.GraphMetricRuntime.init(
            alloc,
            resources.store,
            resources.index_manager,
            resources.apply_mutex,
            coordinator.backend_runtime,
            .{
                .enabled = true,
                .role = .coordinator,
                .runtime_id = "runtime-publish-race-coordinator-a",
                .lease_owned = true,
                .owner_id = "runtime-publish-race-coordinator-a",
                .planned_options = .{
                    .worker_id = "runtime-publish-race-coordinator-a",
                    .max_rounds = 1,
                    .max_metrics_per_round = 8,
                    .max_pages_per_round = 1,
                },
            },
        );
        defer runtime.deinit();

        const publish = try runtime.runCoordinatorOnce(false);
        try std.testing.expect(publish.phases_advanced > 0);
        try std.testing.expectEqual(@as(usize, 0), publish.worker_steps);
        const publish_stats = runtime.stats();
        try std.testing.expect(publish_stats.lease_owned);
        try std.testing.expect(publish_stats.has_lease);
        try std.testing.expectEqual(graph_metric_runtime_mod.Role.coordinator, publish_stats.role);

        {
            var live_duplicate_coordinator = try DB.open(alloc, std.mem.span(path), .{
                .start_index_workers = false,
                .ttl_cleanup = .{ .enabled = false },
            });
            defer live_duplicate_coordinator.close();

            const duplicate_resources = live_duplicate_coordinator.core.asyncResources();
            var duplicate_runtime = try graph_metric_runtime_mod.GraphMetricRuntime.init(
                alloc,
                duplicate_resources.store,
                duplicate_resources.index_manager,
                duplicate_resources.apply_mutex,
                live_duplicate_coordinator.backend_runtime,
                .{
                    .enabled = true,
                    .role = .coordinator,
                    .runtime_id = "runtime-publish-race-coordinator-b",
                    .lease_owned = true,
                    .owner_id = "runtime-publish-race-coordinator-b",
                    .planned_options = .{
                        .worker_id = "runtime-publish-race-coordinator-b",
                        .max_rounds = 1,
                        .max_metrics_per_round = 8,
                        .max_pages_per_round = 1,
                    },
                },
            );
            defer duplicate_runtime.deinit();

            const live_duplicate = try duplicate_runtime.runCoordinatorOnce(false);
            try std.testing.expectEqual(@as(usize, 0), live_duplicate.phases_advanced);
            try std.testing.expectEqual(@as(usize, 0), live_duplicate.published);
            try std.testing.expectEqual(@as(usize, 0), live_duplicate.worker_steps);
            const live_duplicate_stats = duplicate_runtime.stats();
            try std.testing.expect(live_duplicate_stats.lease_owned);
            try std.testing.expect(!live_duplicate_stats.has_lease);
            try std.testing.expectEqual(@as(u64, 1), live_duplicate_stats.lease_acquire_failures);
            try std.testing.expectEqual(graph_metric_runtime_mod.Role.coordinator, live_duplicate_stats.role);
        }

        const graph_entry = coordinator.core.graphIndex("graph_idx") orelse return error.IndexNotFound;
        var status = try graph_entry.index.graphMetricStatus("pagerank");
        defer status.deinit(alloc);
        try std.testing.expectEqual(graph_mod.GraphIndex.GraphMetricState.building, status.state);
        try std.testing.expectEqual(graph_mod.GraphIndex.GraphMetricBuildPhase.cleanup_old_generations, status.phase);
        try std.testing.expectEqual(target_generation, status.published_generation);
        try std.testing.expectEqual(@as(usize, 1), status.recent_events.len);
        try std.testing.expectEqual(graph_mod.GraphIndex.GraphMetricEventKind.publish, status.recent_events[0].kind);
    }

    {
        var duplicate_coordinator = try DB.open(alloc, std.mem.span(path), .{
            .start_index_workers = false,
            .ttl_cleanup = .{ .enabled = false },
        });
        defer duplicate_coordinator.close();

        const resources = duplicate_coordinator.core.asyncResources();
        var runtime = try graph_metric_runtime_mod.GraphMetricRuntime.init(
            alloc,
            resources.store,
            resources.index_manager,
            resources.apply_mutex,
            duplicate_coordinator.backend_runtime,
            .{
                .enabled = true,
                .role = .coordinator,
                .runtime_id = "runtime-publish-race-coordinator-b",
                .lease_owned = true,
                .owner_id = "runtime-publish-race-coordinator-b",
                .planned_options = .{
                    .worker_id = "runtime-publish-race-coordinator-b",
                    .max_rounds = 1,
                    .max_metrics_per_round = 8,
                    .max_pages_per_round = 1,
                },
            },
        );
        defer runtime.deinit();

        const duplicate = try runtime.runCoordinatorOnce(false);
        try std.testing.expectEqual(@as(usize, 0), duplicate.phases_advanced);
        try std.testing.expectEqual(@as(usize, 0), duplicate.published);
        try std.testing.expectEqual(@as(usize, 0), duplicate.worker_steps);
        const duplicate_stats = runtime.stats();
        try std.testing.expect(duplicate_stats.lease_owned);
        try std.testing.expect(duplicate_stats.has_lease);
        try std.testing.expectEqual(graph_metric_runtime_mod.Role.coordinator, duplicate_stats.role);

        const graph_entry = duplicate_coordinator.core.graphIndex("graph_idx") orelse return error.IndexNotFound;
        var status = try graph_entry.index.graphMetricStatus("pagerank");
        defer status.deinit(alloc);
        try std.testing.expectEqual(graph_mod.GraphIndex.GraphMetricState.building, status.state);
        try std.testing.expectEqual(graph_mod.GraphIndex.GraphMetricBuildPhase.cleanup_old_generations, status.phase);
        try std.testing.expectEqual(target_generation, status.published_generation);
        try std.testing.expectEqual(@as(usize, 1), status.recent_events.len);
        try std.testing.expectEqual(graph_mod.GraphIndex.GraphMetricEventKind.publish, status.recent_events[0].kind);
    }

    {
        var worker = try DB.open(alloc, std.mem.span(path), .{
            .start_index_workers = false,
            .ttl_cleanup = .{ .enabled = false },
        });
        defer worker.close();

        var cleanup_finished = false;
        for (0..12) |_| {
            const resources = worker.core.asyncResources();
            var runtime = try graph_metric_runtime_mod.GraphMetricRuntime.init(
                alloc,
                resources.store,
                resources.index_manager,
                resources.apply_mutex,
                worker.backend_runtime,
                .{
                    .enabled = true,
                    .role = .worker,
                    .lease_owned = true,
                    .owner_id = "runtime-publish-race-cleaner",
                    .planned_options = .{
                        .worker_id = "runtime-publish-race-cleaner",
                        .max_rounds = 1,
                        .max_metrics_per_round = 8,
                        .max_pages_per_round = 1,
                    },
                },
            );
            defer runtime.deinit();

            const cleanup = try runtime.runWorkerOnce("runtime-publish-race-cleaner");
            try std.testing.expectEqual(@as(usize, 0), cleanup.phases_advanced);
            try std.testing.expectEqual(@as(usize, 0), cleanup.published);
            const graph_entry = worker.core.graphIndex("graph_idx") orelse return error.IndexNotFound;
            var status = try graph_entry.index.graphMetricStatus("pagerank");
            defer status.deinit(alloc);
            if (status.state == .fresh and status.phase == .complete) {
                cleanup_finished = true;
                break;
            }
        }
        try std.testing.expect(cleanup_finished);
    }

    {
        var reader = try DB.open(alloc, std.mem.span(path), .{
            .start_index_workers = false,
            .ttl_cleanup = .{ .enabled = false },
        });
        defer reader.close();

        const graph_entry = reader.core.graphIndex("graph_idx") orelse return error.IndexNotFound;
        var status = try graph_entry.index.graphMetricStatus("pagerank");
        defer status.deinit(alloc);
        try std.testing.expectEqual(graph_mod.GraphIndex.GraphMetricState.fresh, status.state);
        try std.testing.expectEqual(graph_mod.GraphIndex.GraphMetricBuildPhase.complete, status.phase);
        try std.testing.expectEqual(target_generation, status.published_generation);
        try std.testing.expectEqual(@as(usize, 1), status.recent_events.len);
        try std.testing.expectEqual(graph_mod.GraphIndex.GraphMetricEventKind.publish, status.recent_events[0].kind);
    }
}

test "db graph metric runtime reopened coordinators do not duplicate eigenvector publish" {
    const alloc = std.testing.allocator;

    var path_buf: [256]u8 = undefined;
    const path = tempPath(&path_buf);
    defer cleanupTempDir(path);

    var target_generation: u64 = 0;
    {
        var db = try DB.open(alloc, std.mem.span(path), .{
            .start_index_workers = false,
            .ttl_cleanup = .{ .enabled = false },
        });
        defer db.close();

        try db.addIndex(.{
            .name = "graph_idx",
            .kind = .graph,
            .config_json = "{\"metrics\":{\"eigenvector\":{\"enabled\":true,\"kind\":\"eigenvector\",\"refresh\":\"manual\",\"max_iterations\":1,\"tolerance\":0.000001,\"edge_filter\":{\"types\":[\"cites\"]}}}}",
        });

        try db.batch(.{
            .writes = &.{
                .{ .key = "doc:a", .value = "{\"title\":\"alpha\",\"_edges\":{\"graph_idx\":{\"cites\":[{\"target\":\"doc:b\",\"weight\":1.0}]}}}" },
                .{ .key = "doc:b", .value = "{\"title\":\"beta\",\"_edges\":{\"graph_idx\":{\"cites\":[{\"target\":\"doc:d\",\"weight\":1.0}]}}}" },
                .{ .key = "doc:c", .value = "{\"title\":\"gamma\",\"_edges\":{\"graph_idx\":{\"cites\":[{\"target\":\"doc:b\",\"weight\":1.0}]}}}" },
                .{ .key = "doc:d", .value = "{\"title\":\"delta\"}" },
            },
            .sync_level = .write,
        });
        try db.runUntilIdle();

        const graph_entry = db.core.graphIndex("graph_idx") orelse return error.IndexNotFound;
        target_generation = graph_entry.index.edge_generation;
    }

    {
        var coordinator = try DB.open(alloc, std.mem.span(path), .{
            .start_index_workers = false,
            .ttl_cleanup = .{ .enabled = false },
        });
        defer coordinator.close();

        var started = try coordinator.ensureGraphMetricPlannedBuild(alloc, "graph_idx", "eigenvector", target_generation);
        defer started.deinit(alloc);
        try std.testing.expectEqual(graph_mod.GraphIndex.GraphMetricState.building, started.state);
        try std.testing.expectEqual(target_generation, started.building_generation);
    }

    const workers = [_][]const u8{ "runtime-eigenvector-publish-race-worker-a", "runtime-eigenvector-publish-race-worker-b" };
    var reached_publish = false;
    var step_index: usize = 0;
    while (step_index < 400) : (step_index += 1) {
        const worker_tick = blk: {
            var worker = try DB.open(alloc, std.mem.span(path), .{
                .start_index_workers = false,
                .ttl_cleanup = .{ .enabled = false },
            });
            defer worker.close();

            const resources = worker.core.asyncResources();
            var runtime = try graph_metric_runtime_mod.GraphMetricRuntime.init(
                alloc,
                resources.store,
                resources.index_manager,
                resources.apply_mutex,
                worker.backend_runtime,
                .{
                    .enabled = true,
                    .role = .worker,
                    .lease_owned = true,
                    .owner_id = workers[step_index % workers.len],
                    .planned_options = .{
                        .worker_id = workers[step_index % workers.len],
                        .max_rounds = 1,
                        .max_metrics_per_round = 8,
                        .max_pages_per_round = 1,
                    },
                },
            );
            defer runtime.deinit();
            break :blk try runtime.runWorkerOnce(workers[step_index % workers.len]);
        };

        {
            var reader = try DB.open(alloc, std.mem.span(path), .{
                .start_index_workers = false,
                .ttl_cleanup = .{ .enabled = false },
            });
            defer reader.close();
            const graph_entry = reader.core.graphIndex("graph_idx") orelse return error.IndexNotFound;
            var status = try graph_entry.index.graphMetricStatus("eigenvector");
            defer status.deinit(alloc);
            if (status.phase == .publish_generation) {
                reached_publish = true;
                break;
            }
        }

        const coordinator_tick = blk: {
            var coordinator = try DB.open(alloc, std.mem.span(path), .{
                .start_index_workers = false,
                .ttl_cleanup = .{ .enabled = false },
            });
            defer coordinator.close();

            const resources = coordinator.core.asyncResources();
            var runtime = try graph_metric_runtime_mod.GraphMetricRuntime.init(
                alloc,
                resources.store,
                resources.index_manager,
                resources.apply_mutex,
                coordinator.backend_runtime,
                .{
                    .enabled = true,
                    .role = .coordinator,
                    .lease_owned = true,
                    .owner_id = "runtime-eigenvector-publish-race-coordinator",
                    .planned_options = .{
                        .worker_id = "runtime-eigenvector-publish-race-coordinator",
                        .max_rounds = 1,
                        .max_metrics_per_round = 8,
                        .max_pages_per_round = 1,
                    },
                },
            );
            defer runtime.deinit();
            break :blk try runtime.runCoordinatorOnce(false);
        };

        {
            var reader = try DB.open(alloc, std.mem.span(path), .{
                .start_index_workers = false,
                .ttl_cleanup = .{ .enabled = false },
            });
            defer reader.close();
            const graph_entry = reader.core.graphIndex("graph_idx") orelse return error.IndexNotFound;
            var status = try graph_entry.index.graphMetricStatus("eigenvector");
            defer status.deinit(alloc);
            if (status.phase == .publish_generation) {
                reached_publish = true;
                break;
            }
        }

        if (!worker_tick.durableProgressed() and !coordinator_tick.durableProgressed()) {
            return error.GraphMetricBuildNoEligiblePage;
        }
    }
    try std.testing.expect(reached_publish);

    {
        var coordinator = try DB.open(alloc, std.mem.span(path), .{
            .start_index_workers = false,
            .ttl_cleanup = .{ .enabled = false },
        });
        defer coordinator.close();

        const resources = coordinator.core.asyncResources();
        var runtime = try graph_metric_runtime_mod.GraphMetricRuntime.init(
            alloc,
            resources.store,
            resources.index_manager,
            resources.apply_mutex,
            coordinator.backend_runtime,
            .{
                .enabled = true,
                .role = .coordinator,
                .runtime_id = "runtime-eigenvector-publish-race-coordinator-a",
                .lease_owned = true,
                .owner_id = "runtime-eigenvector-publish-race-coordinator-a",
                .planned_options = .{
                    .worker_id = "runtime-eigenvector-publish-race-coordinator-a",
                    .max_rounds = 1,
                    .max_metrics_per_round = 8,
                    .max_pages_per_round = 1,
                },
            },
        );
        defer runtime.deinit();

        const publish = try runtime.runCoordinatorOnce(false);
        try std.testing.expect(publish.phases_advanced > 0);
        try std.testing.expectEqual(@as(usize, 0), publish.worker_steps);
        const publish_stats = runtime.stats();
        try std.testing.expect(publish_stats.lease_owned);
        try std.testing.expect(publish_stats.has_lease);
        try std.testing.expectEqual(graph_metric_runtime_mod.Role.coordinator, publish_stats.role);

        {
            var live_duplicate_coordinator = try DB.open(alloc, std.mem.span(path), .{
                .start_index_workers = false,
                .ttl_cleanup = .{ .enabled = false },
            });
            defer live_duplicate_coordinator.close();

            const duplicate_resources = live_duplicate_coordinator.core.asyncResources();
            var duplicate_runtime = try graph_metric_runtime_mod.GraphMetricRuntime.init(
                alloc,
                duplicate_resources.store,
                duplicate_resources.index_manager,
                duplicate_resources.apply_mutex,
                live_duplicate_coordinator.backend_runtime,
                .{
                    .enabled = true,
                    .role = .coordinator,
                    .runtime_id = "runtime-eigenvector-publish-race-coordinator-b",
                    .lease_owned = true,
                    .owner_id = "runtime-eigenvector-publish-race-coordinator-b",
                    .planned_options = .{
                        .worker_id = "runtime-eigenvector-publish-race-coordinator-b",
                        .max_rounds = 1,
                        .max_metrics_per_round = 8,
                        .max_pages_per_round = 1,
                    },
                },
            );
            defer duplicate_runtime.deinit();

            const live_duplicate = try duplicate_runtime.runCoordinatorOnce(false);
            try std.testing.expectEqual(@as(usize, 0), live_duplicate.phases_advanced);
            try std.testing.expectEqual(@as(usize, 0), live_duplicate.published);
            try std.testing.expectEqual(@as(usize, 0), live_duplicate.worker_steps);
            const live_duplicate_stats = duplicate_runtime.stats();
            try std.testing.expect(live_duplicate_stats.lease_owned);
            try std.testing.expect(!live_duplicate_stats.has_lease);
            try std.testing.expectEqual(@as(u64, 1), live_duplicate_stats.lease_acquire_failures);
            try std.testing.expectEqual(graph_metric_runtime_mod.Role.coordinator, live_duplicate_stats.role);
        }

        const graph_entry = coordinator.core.graphIndex("graph_idx") orelse return error.IndexNotFound;
        var status = try graph_entry.index.graphMetricStatus("eigenvector");
        defer status.deinit(alloc);
        try std.testing.expectEqual(graph_mod.GraphIndex.GraphMetricState.building, status.state);
        try std.testing.expectEqual(graph_mod.GraphIndex.GraphMetricBuildPhase.cleanup_old_generations, status.phase);
        try std.testing.expectEqual(target_generation, status.published_generation);
        try std.testing.expectEqual(@as(usize, 1), status.recent_events.len);
        try std.testing.expectEqual(graph_mod.GraphIndex.GraphMetricEventKind.publish, status.recent_events[0].kind);
    }

    {
        var duplicate_coordinator = try DB.open(alloc, std.mem.span(path), .{
            .start_index_workers = false,
            .ttl_cleanup = .{ .enabled = false },
        });
        defer duplicate_coordinator.close();

        const resources = duplicate_coordinator.core.asyncResources();
        var runtime = try graph_metric_runtime_mod.GraphMetricRuntime.init(
            alloc,
            resources.store,
            resources.index_manager,
            resources.apply_mutex,
            duplicate_coordinator.backend_runtime,
            .{
                .enabled = true,
                .role = .coordinator,
                .runtime_id = "runtime-eigenvector-publish-race-coordinator-b",
                .lease_owned = true,
                .owner_id = "runtime-eigenvector-publish-race-coordinator-b",
                .planned_options = .{
                    .worker_id = "runtime-eigenvector-publish-race-coordinator-b",
                    .max_rounds = 1,
                    .max_metrics_per_round = 8,
                    .max_pages_per_round = 1,
                },
            },
        );
        defer runtime.deinit();

        const duplicate = try runtime.runCoordinatorOnce(false);
        try std.testing.expectEqual(@as(usize, 0), duplicate.phases_advanced);
        try std.testing.expectEqual(@as(usize, 0), duplicate.published);
        try std.testing.expectEqual(@as(usize, 0), duplicate.worker_steps);
        const duplicate_stats = runtime.stats();
        try std.testing.expect(duplicate_stats.lease_owned);
        try std.testing.expect(duplicate_stats.has_lease);
        try std.testing.expectEqual(graph_metric_runtime_mod.Role.coordinator, duplicate_stats.role);

        const graph_entry = duplicate_coordinator.core.graphIndex("graph_idx") orelse return error.IndexNotFound;
        var status = try graph_entry.index.graphMetricStatus("eigenvector");
        defer status.deinit(alloc);
        try std.testing.expectEqual(graph_mod.GraphIndex.GraphMetricState.building, status.state);
        try std.testing.expectEqual(graph_mod.GraphIndex.GraphMetricBuildPhase.cleanup_old_generations, status.phase);
        try std.testing.expectEqual(target_generation, status.published_generation);
        try std.testing.expectEqual(@as(usize, 1), status.recent_events.len);
        try std.testing.expectEqual(graph_mod.GraphIndex.GraphMetricEventKind.publish, status.recent_events[0].kind);
    }

    {
        var worker = try DB.open(alloc, std.mem.span(path), .{
            .start_index_workers = false,
            .ttl_cleanup = .{ .enabled = false },
        });
        defer worker.close();

        var cleanup_finished = false;
        for (0..12) |_| {
            const resources = worker.core.asyncResources();
            var runtime = try graph_metric_runtime_mod.GraphMetricRuntime.init(
                alloc,
                resources.store,
                resources.index_manager,
                resources.apply_mutex,
                worker.backend_runtime,
                .{
                    .enabled = true,
                    .role = .worker,
                    .lease_owned = true,
                    .owner_id = "runtime-eigenvector-publish-race-cleaner",
                    .planned_options = .{
                        .worker_id = "runtime-eigenvector-publish-race-cleaner",
                        .max_rounds = 1,
                        .max_metrics_per_round = 8,
                        .max_pages_per_round = 1,
                    },
                },
            );
            defer runtime.deinit();

            const cleanup = try runtime.runWorkerOnce("runtime-eigenvector-publish-race-cleaner");
            try std.testing.expectEqual(@as(usize, 0), cleanup.phases_advanced);
            try std.testing.expectEqual(@as(usize, 0), cleanup.published);
            const graph_entry = worker.core.graphIndex("graph_idx") orelse return error.IndexNotFound;
            var status = try graph_entry.index.graphMetricStatus("eigenvector");
            defer status.deinit(alloc);
            if (status.state == .fresh and status.phase == .complete) {
                cleanup_finished = true;
                break;
            }
        }
        try std.testing.expect(cleanup_finished);
    }

    {
        var reader = try DB.open(alloc, std.mem.span(path), .{
            .start_index_workers = false,
            .ttl_cleanup = .{ .enabled = false },
        });
        defer reader.close();

        const graph_entry = reader.core.graphIndex("graph_idx") orelse return error.IndexNotFound;
        var status = try graph_entry.index.graphMetricStatus("eigenvector");
        defer status.deinit(alloc);
        try std.testing.expectEqual(graph_mod.GraphIndex.GraphMetricState.fresh, status.state);
        try std.testing.expectEqual(graph_mod.GraphIndex.GraphMetricBuildPhase.complete, status.phase);
        try std.testing.expectEqual(target_generation, status.published_generation);
        try std.testing.expectEqual(@as(usize, 1), status.recent_events.len);
        try std.testing.expectEqual(graph_mod.GraphIndex.GraphMetricEventKind.publish, status.recent_events[0].kind);
    }
}

test "db graph metric runtime reopened coordinators do not duplicate hits publish" {
    const alloc = std.testing.allocator;

    var path_buf: [256]u8 = undefined;
    const path = tempPath(&path_buf);
    defer cleanupTempDir(path);

    var target_generation: u64 = 0;
    {
        var db = try DB.open(alloc, std.mem.span(path), .{
            .start_index_workers = false,
            .ttl_cleanup = .{ .enabled = false },
        });
        defer db.close();

        try db.addIndex(.{
            .name = "graph_idx",
            .kind = .graph,
            .config_json = "{\"metrics\":{\"hits_authority\":{\"enabled\":true,\"kind\":\"hits_authority\",\"refresh\":\"manual\",\"max_iterations\":1,\"tolerance\":0.000001,\"edge_filter\":{\"types\":[\"cites\"]}},\"hits_hub\":{\"enabled\":true,\"kind\":\"hits_hub\",\"refresh\":\"manual\",\"max_iterations\":1,\"tolerance\":0.000001,\"edge_filter\":{\"types\":[\"cites\"]}}}}",
        });

        try db.batch(.{
            .writes = &.{
                .{ .key = "doc:hub_a", .value = "{\"title\":\"hub a\",\"_edges\":{\"graph_idx\":{\"cites\":[{\"target\":\"doc:authority\",\"weight\":1.0}]}}}" },
                .{ .key = "doc:hub_b", .value = "{\"title\":\"hub b\",\"_edges\":{\"graph_idx\":{\"cites\":[{\"target\":\"doc:authority\",\"weight\":1.0}]}}}" },
                .{ .key = "doc:authority", .value = "{\"title\":\"authority\",\"_edges\":{\"graph_idx\":{\"cites\":[{\"target\":\"doc:authority\",\"weight\":1.0}]}}}" },
            },
            .sync_level = .write,
        });
        try db.runUntilIdle();

        const graph_entry = db.core.graphIndex("graph_idx") orelse return error.IndexNotFound;
        target_generation = graph_entry.index.edge_generation;
    }

    {
        var coordinator = try DB.open(alloc, std.mem.span(path), .{
            .start_index_workers = false,
            .ttl_cleanup = .{ .enabled = false },
        });
        defer coordinator.close();

        var started = try coordinator.ensureGraphMetricPlannedBuild(alloc, "graph_idx", "hits_authority", target_generation);
        defer started.deinit(alloc);
        try std.testing.expectEqual(graph_mod.GraphIndex.GraphMetricState.building, started.state);
        try std.testing.expectEqual(target_generation, started.building_generation);
    }

    const workers = [_][]const u8{ "runtime-hits-publish-race-worker-a", "runtime-hits-publish-race-worker-b" };
    var reached_publish = false;
    var step_index: usize = 0;
    while (step_index < 400) : (step_index += 1) {
        const worker_tick = blk: {
            var worker = try DB.open(alloc, std.mem.span(path), .{
                .start_index_workers = false,
                .ttl_cleanup = .{ .enabled = false },
            });
            defer worker.close();

            const resources = worker.core.asyncResources();
            var runtime = try graph_metric_runtime_mod.GraphMetricRuntime.init(
                alloc,
                resources.store,
                resources.index_manager,
                resources.apply_mutex,
                worker.backend_runtime,
                .{
                    .enabled = true,
                    .role = .worker,
                    .lease_owned = true,
                    .owner_id = workers[step_index % workers.len],
                    .planned_options = .{
                        .worker_id = workers[step_index % workers.len],
                        .max_rounds = 1,
                        .max_metrics_per_round = 8,
                        .max_pages_per_round = 1,
                    },
                },
            );
            defer runtime.deinit();
            break :blk try runtime.runWorkerOnce(workers[step_index % workers.len]);
        };

        {
            var reader = try DB.open(alloc, std.mem.span(path), .{
                .start_index_workers = false,
                .ttl_cleanup = .{ .enabled = false },
            });
            defer reader.close();
            const graph_entry = reader.core.graphIndex("graph_idx") orelse return error.IndexNotFound;
            var status = try graph_entry.index.graphMetricStatus("hits_authority");
            defer status.deinit(alloc);
            if (status.phase == .publish_generation) {
                reached_publish = true;
                break;
            }
        }

        const coordinator_tick = blk: {
            var coordinator = try DB.open(alloc, std.mem.span(path), .{
                .start_index_workers = false,
                .ttl_cleanup = .{ .enabled = false },
            });
            defer coordinator.close();

            const resources = coordinator.core.asyncResources();
            var runtime = try graph_metric_runtime_mod.GraphMetricRuntime.init(
                alloc,
                resources.store,
                resources.index_manager,
                resources.apply_mutex,
                coordinator.backend_runtime,
                .{
                    .enabled = true,
                    .role = .coordinator,
                    .lease_owned = true,
                    .owner_id = "runtime-hits-publish-race-coordinator",
                    .planned_options = .{
                        .worker_id = "runtime-hits-publish-race-coordinator",
                        .max_rounds = 1,
                        .max_metrics_per_round = 8,
                        .max_pages_per_round = 1,
                    },
                },
            );
            defer runtime.deinit();
            break :blk try runtime.runCoordinatorOnce(false);
        };

        {
            var reader = try DB.open(alloc, std.mem.span(path), .{
                .start_index_workers = false,
                .ttl_cleanup = .{ .enabled = false },
            });
            defer reader.close();
            const graph_entry = reader.core.graphIndex("graph_idx") orelse return error.IndexNotFound;
            var status = try graph_entry.index.graphMetricStatus("hits_authority");
            defer status.deinit(alloc);
            if (status.phase == .publish_generation) {
                reached_publish = true;
                break;
            }
        }

        if (!worker_tick.durableProgressed() and !coordinator_tick.durableProgressed()) {
            return error.GraphMetricBuildNoEligiblePage;
        }
    }
    try std.testing.expect(reached_publish);

    {
        var coordinator = try DB.open(alloc, std.mem.span(path), .{
            .start_index_workers = false,
            .ttl_cleanup = .{ .enabled = false },
        });
        defer coordinator.close();

        const resources = coordinator.core.asyncResources();
        var runtime = try graph_metric_runtime_mod.GraphMetricRuntime.init(
            alloc,
            resources.store,
            resources.index_manager,
            resources.apply_mutex,
            coordinator.backend_runtime,
            .{
                .enabled = true,
                .role = .coordinator,
                .runtime_id = "runtime-hits-publish-race-coordinator-a",
                .lease_owned = true,
                .owner_id = "runtime-hits-publish-race-coordinator-a",
                .planned_options = .{
                    .worker_id = "runtime-hits-publish-race-coordinator-a",
                    .max_rounds = 1,
                    .max_metrics_per_round = 8,
                    .max_pages_per_round = 1,
                },
            },
        );
        defer runtime.deinit();

        const publish = try runtime.runCoordinatorOnce(false);
        try std.testing.expect(publish.phases_advanced > 0);
        try std.testing.expectEqual(@as(usize, 0), publish.worker_steps);
        const publish_stats = runtime.stats();
        try std.testing.expect(publish_stats.lease_owned);
        try std.testing.expect(publish_stats.has_lease);
        try std.testing.expectEqual(graph_metric_runtime_mod.Role.coordinator, publish_stats.role);

        {
            var live_duplicate_coordinator = try DB.open(alloc, std.mem.span(path), .{
                .start_index_workers = false,
                .ttl_cleanup = .{ .enabled = false },
            });
            defer live_duplicate_coordinator.close();

            const duplicate_resources = live_duplicate_coordinator.core.asyncResources();
            var duplicate_runtime = try graph_metric_runtime_mod.GraphMetricRuntime.init(
                alloc,
                duplicate_resources.store,
                duplicate_resources.index_manager,
                duplicate_resources.apply_mutex,
                live_duplicate_coordinator.backend_runtime,
                .{
                    .enabled = true,
                    .role = .coordinator,
                    .runtime_id = "runtime-hits-publish-race-coordinator-b",
                    .lease_owned = true,
                    .owner_id = "runtime-hits-publish-race-coordinator-b",
                    .planned_options = .{
                        .worker_id = "runtime-hits-publish-race-coordinator-b",
                        .max_rounds = 1,
                        .max_metrics_per_round = 8,
                        .max_pages_per_round = 1,
                    },
                },
            );
            defer duplicate_runtime.deinit();

            const live_duplicate = try duplicate_runtime.runCoordinatorOnce(false);
            try std.testing.expectEqual(@as(usize, 0), live_duplicate.phases_advanced);
            try std.testing.expectEqual(@as(usize, 0), live_duplicate.published);
            try std.testing.expectEqual(@as(usize, 0), live_duplicate.worker_steps);
            const live_duplicate_stats = duplicate_runtime.stats();
            try std.testing.expect(live_duplicate_stats.lease_owned);
            try std.testing.expect(!live_duplicate_stats.has_lease);
            try std.testing.expectEqual(@as(u64, 1), live_duplicate_stats.lease_acquire_failures);
            try std.testing.expectEqual(graph_metric_runtime_mod.Role.coordinator, live_duplicate_stats.role);
        }

        const graph_entry = coordinator.core.graphIndex("graph_idx") orelse return error.IndexNotFound;
        var authority = try graph_entry.index.graphMetricStatus("hits_authority");
        defer authority.deinit(alloc);
        var hub = try graph_entry.index.graphMetricStatus("hits_hub");
        defer hub.deinit(alloc);
        try std.testing.expectEqual(graph_mod.GraphIndex.GraphMetricState.building, authority.state);
        try std.testing.expectEqual(graph_mod.GraphIndex.GraphMetricState.fresh, hub.state);
        try std.testing.expectEqual(graph_mod.GraphIndex.GraphMetricBuildPhase.cleanup_old_generations, authority.phase);
        try std.testing.expectEqual(graph_mod.GraphIndex.GraphMetricBuildPhase.complete, hub.phase);
        try std.testing.expectEqual(target_generation, authority.published_generation);
        try std.testing.expectEqual(authority.published_generation, hub.published_generation);
        try std.testing.expectEqual(@as(usize, 1), authority.recent_events.len);
        try std.testing.expectEqual(@as(usize, 1), hub.recent_events.len);
        try std.testing.expectEqual(graph_mod.GraphIndex.GraphMetricEventKind.publish, authority.recent_events[0].kind);
        try std.testing.expectEqual(graph_mod.GraphIndex.GraphMetricEventKind.publish, hub.recent_events[0].kind);
    }

    {
        var duplicate_coordinator = try DB.open(alloc, std.mem.span(path), .{
            .start_index_workers = false,
            .ttl_cleanup = .{ .enabled = false },
        });
        defer duplicate_coordinator.close();

        const resources = duplicate_coordinator.core.asyncResources();
        var runtime = try graph_metric_runtime_mod.GraphMetricRuntime.init(
            alloc,
            resources.store,
            resources.index_manager,
            resources.apply_mutex,
            duplicate_coordinator.backend_runtime,
            .{
                .enabled = true,
                .role = .coordinator,
                .runtime_id = "runtime-hits-publish-race-coordinator-b",
                .lease_owned = true,
                .owner_id = "runtime-hits-publish-race-coordinator-b",
                .planned_options = .{
                    .worker_id = "runtime-hits-publish-race-coordinator-b",
                    .max_rounds = 1,
                    .max_metrics_per_round = 8,
                    .max_pages_per_round = 1,
                },
            },
        );
        defer runtime.deinit();

        const duplicate = try runtime.runCoordinatorOnce(false);
        try std.testing.expectEqual(@as(usize, 0), duplicate.phases_advanced);
        try std.testing.expectEqual(@as(usize, 0), duplicate.published);
        try std.testing.expectEqual(@as(usize, 0), duplicate.worker_steps);
        const duplicate_stats = runtime.stats();
        try std.testing.expect(duplicate_stats.lease_owned);
        try std.testing.expect(duplicate_stats.has_lease);
        try std.testing.expectEqual(graph_metric_runtime_mod.Role.coordinator, duplicate_stats.role);

        const graph_entry = duplicate_coordinator.core.graphIndex("graph_idx") orelse return error.IndexNotFound;
        var authority = try graph_entry.index.graphMetricStatus("hits_authority");
        defer authority.deinit(alloc);
        var hub = try graph_entry.index.graphMetricStatus("hits_hub");
        defer hub.deinit(alloc);
        try std.testing.expectEqual(graph_mod.GraphIndex.GraphMetricState.building, authority.state);
        try std.testing.expectEqual(graph_mod.GraphIndex.GraphMetricState.fresh, hub.state);
        try std.testing.expectEqual(graph_mod.GraphIndex.GraphMetricBuildPhase.cleanup_old_generations, authority.phase);
        try std.testing.expectEqual(graph_mod.GraphIndex.GraphMetricBuildPhase.complete, hub.phase);
        try std.testing.expectEqual(target_generation, authority.published_generation);
        try std.testing.expectEqual(authority.published_generation, hub.published_generation);
        try std.testing.expectEqual(@as(usize, 1), authority.recent_events.len);
        try std.testing.expectEqual(@as(usize, 1), hub.recent_events.len);
        try std.testing.expectEqual(graph_mod.GraphIndex.GraphMetricEventKind.publish, authority.recent_events[0].kind);
        try std.testing.expectEqual(graph_mod.GraphIndex.GraphMetricEventKind.publish, hub.recent_events[0].kind);
    }

    {
        var worker = try DB.open(alloc, std.mem.span(path), .{
            .start_index_workers = false,
            .ttl_cleanup = .{ .enabled = false },
        });
        defer worker.close();

        var cleanup_finished = false;
        for (0..12) |_| {
            const resources = worker.core.asyncResources();
            var runtime = try graph_metric_runtime_mod.GraphMetricRuntime.init(
                alloc,
                resources.store,
                resources.index_manager,
                resources.apply_mutex,
                worker.backend_runtime,
                .{
                    .enabled = true,
                    .role = .worker,
                    .lease_owned = true,
                    .owner_id = "runtime-hits-publish-race-cleaner",
                    .planned_options = .{
                        .worker_id = "runtime-hits-publish-race-cleaner",
                        .max_rounds = 1,
                        .max_metrics_per_round = 8,
                        .max_pages_per_round = 1,
                    },
                },
            );
            defer runtime.deinit();

            const cleanup = try runtime.runWorkerOnce("runtime-hits-publish-race-cleaner");
            try std.testing.expectEqual(@as(usize, 0), cleanup.phases_advanced);
            try std.testing.expectEqual(@as(usize, 0), cleanup.published);
            const graph_entry = worker.core.graphIndex("graph_idx") orelse return error.IndexNotFound;
            var authority = try graph_entry.index.graphMetricStatus("hits_authority");
            defer authority.deinit(alloc);
            var hub = try graph_entry.index.graphMetricStatus("hits_hub");
            defer hub.deinit(alloc);
            if (authority.state == .fresh and authority.phase == .complete and hub.state == .fresh and hub.phase == .complete) {
                cleanup_finished = true;
                break;
            }
        }
        try std.testing.expect(cleanup_finished);
    }

    {
        var reader = try DB.open(alloc, std.mem.span(path), .{
            .start_index_workers = false,
            .ttl_cleanup = .{ .enabled = false },
        });
        defer reader.close();

        const graph_entry = reader.core.graphIndex("graph_idx") orelse return error.IndexNotFound;
        var authority = try graph_entry.index.graphMetricStatus("hits_authority");
        defer authority.deinit(alloc);
        var hub = try graph_entry.index.graphMetricStatus("hits_hub");
        defer hub.deinit(alloc);
        try std.testing.expectEqual(graph_mod.GraphIndex.GraphMetricState.fresh, authority.state);
        try std.testing.expectEqual(graph_mod.GraphIndex.GraphMetricState.fresh, hub.state);
        try std.testing.expectEqual(graph_mod.GraphIndex.GraphMetricBuildPhase.complete, authority.phase);
        try std.testing.expectEqual(graph_mod.GraphIndex.GraphMetricBuildPhase.complete, hub.phase);
        try std.testing.expectEqual(target_generation, authority.published_generation);
        try std.testing.expectEqual(authority.published_generation, hub.published_generation);
        try std.testing.expectEqual(@as(usize, 1), authority.recent_events.len);
        try std.testing.expectEqual(@as(usize, 1), hub.recent_events.len);
        try std.testing.expectEqual(graph_mod.GraphIndex.GraphMetricEventKind.publish, authority.recent_events[0].kind);
        try std.testing.expectEqual(graph_mod.GraphIndex.GraphMetricEventKind.publish, hub.recent_events[0].kind);
    }
}

test "db graph metric runtime cycles multiple worker ids across planned pages" {
    const alloc = std.testing.allocator;

    var path_buf: [256]u8 = undefined;
    const path = tempPath(&path_buf);
    defer cleanupTempDir(path);

    var db = try DB.open(alloc, std.mem.span(path), .{
        .start_index_workers = false,
        .ttl_cleanup = .{ .enabled = false },
    });
    defer db.close();

    try db.addIndex(.{
        .name = "graph_idx",
        .kind = .graph,
        .config_json = "{\"metrics\":{\"degree\":{\"enabled\":true,\"kind\":\"degree\",\"refresh\":\"background\",\"edge_filter\":{\"types\":[\"cites\"]}}}}",
    });

    try db.batch(.{
        .writes = &.{.{ .key = "doc:hub", .value = "{\"title\":\"hub\"}" }},
        .sync_level = .write,
    });

    for (0..130) |i| {
        const key = try std.fmt.allocPrint(alloc, "doc:{d:0>3}", .{i});
        defer alloc.free(key);
        const value = try std.fmt.allocPrint(
            alloc,
            "{{\"title\":\"source {d}\",\"_edges\":{{\"graph_idx\":{{\"cites\":[{{\"target\":\"doc:hub\",\"weight\":1.0}}]}}}}}}",
            .{i},
        );
        defer alloc.free(value);
        try db.batch(.{
            .writes = &.{.{ .key = key, .value = value }},
            .sync_level = .write,
        });
    }

    try db.runDerivedUntil(db.core.nextDerivedSequence());

    const workers = [_][]const u8{ "runtime-worker-a", "runtime-worker-b" };
    const resources = db.core.asyncResources();
    var runtime = try graph_metric_runtime_mod.GraphMetricRuntime.init(
        alloc,
        resources.store,
        resources.index_manager,
        resources.apply_mutex,
        db.backend_runtime,
        .{
            .enabled = true,
            .planned_options = .{
                .worker_ids = &workers,
                .max_rounds = 1,
                .max_metrics_per_round = 8,
                .max_pages_per_round = 2,
            },
        },
    );
    defer runtime.deinit();

    const prepare_tick = try runtime.runOnceDetailed();
    try std.testing.expectEqual(@as(usize, 1), prepare_tick.builds_started);
    try std.testing.expectEqual(@as(usize, 2), prepare_tick.worker_steps);
    try std.testing.expectEqual(@as(usize, 1), prepare_tick.pages_completed);
    try std.testing.expect(prepare_tick.phases_advanced > 0);

    const scan_tick = try runtime.runOnceDetailed();
    try std.testing.expectEqual(@as(usize, 0), scan_tick.builds_started);
    try std.testing.expectEqual(@as(usize, 2), scan_tick.worker_steps);
    try std.testing.expectEqual(@as(usize, 2), scan_tick.pages_completed);
    try std.testing.expectEqual(@as(usize, 0), scan_tick.phases_advanced);

    var steps: usize = 0;
    while (try runtime.runOnce()) {
        steps += 1;
        if (steps > 200) return error.TestUnexpectedResult;
    }

    {
        const pending = db.pendingWorkStats().graph_metric;
        try std.testing.expect(!pending.hasWork());
    }

    var metric_result = try db.search(alloc, .{
        .graph_metric_queries = &.{.{
            .name = "degree",
            .query = .{
                .index_name = "graph_idx",
                .metric_name = "degree",
                .top_k = 1,
                .freshness = .fresh,
            },
        }},
        .limit = 0,
    });
    defer metric_result.deinit();
    try std.testing.expectEqual(@as(usize, 1), metric_result.graph_metric_results.len);
    try std.testing.expectEqual(graph_mod.GraphIndex.GraphMetricState.fresh, metric_result.graph_metric_results[0].status.state);
    try std.testing.expectEqual(@as(usize, 1), metric_result.graph_metric_results[0].scores.len);
    try std.testing.expectEqualStrings("doc:hub", metric_result.graph_metric_results[0].scores[0].node);
    try std.testing.expectApproxEqAbs(@as(f64, 130.0), metric_result.graph_metric_results[0].scores[0].score, 0.001);
}

test "db graph metric planned maintenance reports budget exhaustion and resumes" {
    const alloc = std.testing.allocator;

    var path_buf: [256]u8 = undefined;
    const path = tempPath(&path_buf);
    defer cleanupTempDir(path);

    var db = try DB.open(alloc, std.mem.span(path), .{
        .start_index_workers = false,
        .ttl_cleanup = .{ .enabled = false },
    });
    defer db.close();

    try db.addIndex(.{
        .name = "graph_idx",
        .kind = .graph,
        .config_json = "{\"metrics\":{\"pagerank\":{\"enabled\":true,\"kind\":\"pagerank\",\"refresh\":\"background\",\"max_iterations\":3,\"tolerance\":0.000000001,\"edge_filter\":{\"types\":[\"cites\"]}}}}",
    });

    try db.batch(.{
        .writes = &.{
            .{ .key = "doc:a", .value = "{\"title\":\"alpha\",\"_edges\":{\"graph_idx\":{\"cites\":[{\"target\":\"doc:b\",\"weight\":1.0}]}}}" },
            .{ .key = "doc:b", .value = "{\"title\":\"beta\",\"_edges\":{\"graph_idx\":{\"cites\":[{\"target\":\"doc:d\",\"weight\":1.0}]}}}" },
            .{ .key = "doc:c", .value = "{\"title\":\"gamma\",\"_edges\":{\"graph_idx\":{\"cites\":[{\"target\":\"doc:b\",\"weight\":1.0}]}}}" },
            .{ .key = "doc:d", .value = "{\"title\":\"delta\"}" },
        },
        .sync_level = .write,
    });

    try db.runDerivedUntil(db.core.nextDerivedSequence());

    const target_generation = blk: {
        const graph_entry = db.core.graphIndex("graph_idx") orelse return error.IndexNotFound;
        break :blk graph_entry.index.edge_generation;
    };
    {
        const pending = db.pendingWorkStats().graph_metric;
        try std.testing.expect(pending.hasWork());
        try std.testing.expectEqual(@as(usize, 1), pending.queued_builds);
        try std.testing.expectEqual(@as(usize, 0), pending.active_builds);
    }

    const first_tick = try db.runGraphMetricPlannedMaintenanceForIdle(.{
        .worker_id = "planned-maintenance-budgeted",
        .max_rounds = 1,
        .max_metrics_per_round = 8,
        .max_pages_per_round = 1,
    });
    try std.testing.expect(first_tick.budget_exhausted);
    try std.testing.expect(first_tick.durableProgressed());
    {
        const graph_entry = db.core.graphIndex("graph_idx") orelse return error.IndexNotFound;
        var status = try graph_entry.index.graphMetricStatus("pagerank");
        defer status.deinit(alloc);
        try std.testing.expect(status.state != .fresh or status.phase != .complete);
    }
    {
        const pending = db.pendingWorkStats().graph_metric;
        try std.testing.expect(pending.hasWork());
        try std.testing.expectEqual(@as(usize, 0), pending.queued_builds);
        try std.testing.expect(pending.active_builds > 0);
    }

    var finished = false;
    var saw_budget_exhausted = first_tick.budget_exhausted;
    var tick_index: usize = 0;
    while (tick_index < 200) : (tick_index += 1) {
        const tick = try db.runGraphMetricPlannedMaintenanceForIdle(.{
            .worker_id = "planned-maintenance-budgeted",
            .max_rounds = 1,
            .max_metrics_per_round = 8,
            .max_pages_per_round = 1,
        });
        saw_budget_exhausted = saw_budget_exhausted or tick.budget_exhausted;
        const graph_entry = db.core.graphIndex("graph_idx") orelse return error.IndexNotFound;
        var status = try graph_entry.index.graphMetricStatus("pagerank");
        defer status.deinit(alloc);
        if (status.state == .fresh and status.phase == .complete) {
            try std.testing.expectEqual(target_generation, status.published_generation);
            try std.testing.expect(status.iterations_completed > 0);
            finished = true;
            break;
        }
        if (!tick.progressed()) return error.GraphMetricBuildNoEligiblePage;
    }
    try std.testing.expect(saw_budget_exhausted);
    try std.testing.expect(finished);

    const no_work = try db.runGraphMetricPlannedMaintenanceForIdle(.{
        .worker_id = "planned-maintenance-budgeted",
        .max_rounds = 1,
        .max_metrics_per_round = 8,
        .max_pages_per_round = 1,
    });
    try std.testing.expect(!no_work.budget_exhausted);
    try std.testing.expect(!no_work.durableProgressed());
    {
        const pending = db.pendingWorkStats().graph_metric;
        try std.testing.expect(!pending.hasWork());
        try std.testing.expectEqual(@as(usize, 0), pending.queued_builds);
        try std.testing.expectEqual(@as(usize, 0), pending.active_builds);
    }

    var published_result = try db.search(alloc, .{
        .graph_metric_queries = &.{.{
            .name = "central",
            .query = .{
                .index_name = "graph_idx",
                .metric_name = "pagerank",
                .top_k = 2,
                .freshness = .fresh,
            },
        }},
        .limit = 0,
    });
    defer published_result.deinit();
    try std.testing.expectEqual(@as(usize, 1), published_result.graph_metric_results.len);
    try std.testing.expectEqual(graph_mod.GraphIndex.GraphMetricState.fresh, published_result.graph_metric_results[0].status.state);
    try std.testing.expectEqual(target_generation, published_result.graph_metric_results[0].status.published_generation);
    try std.testing.expectEqualStrings("doc:d", published_result.graph_metric_results[0].scores[0].node);
}

test "db graph metric planned pagerank production budget matches local oracle" {
    const alloc = std.testing.allocator;

    var path_buf: [256]u8 = undefined;
    const path = tempPath(&path_buf);
    defer cleanupTempDir(path);

    var db = try DB.open(alloc, std.mem.span(path), .{
        .start_index_workers = false,
        .ttl_cleanup = .{ .enabled = false },
    });
    defer db.close();

    try db.addIndex(.{
        .name = "graph_idx",
        .kind = .graph,
        .config_json = "{\"metrics\":{\"pagerank_local\":{\"enabled\":true,\"kind\":\"pagerank\",\"refresh\":\"manual\",\"max_iterations\":2,\"tolerance\":0.000000001,\"edge_filter\":{\"types\":[\"cites\"]}},\"pagerank_planned\":{\"enabled\":true,\"kind\":\"pagerank\",\"refresh\":\"background\",\"max_iterations\":2,\"tolerance\":0.000000001,\"edge_filter\":{\"types\":[\"cites\"]}}}}",
    });

    var writes = std.ArrayListUnmanaged(types.BatchWrite).empty;
    defer {
        for (writes.items) |write| {
            alloc.free(write.key);
            alloc.free(write.value);
        }
        writes.deinit(alloc);
    }
    for (0..130) |i| {
        const key = try std.fmt.allocPrint(alloc, "doc:src-{d:0>3}", .{i});
        errdefer alloc.free(key);
        const value = try std.fmt.allocPrint(
            alloc,
            "{{\"title\":\"source {d}\",\"_edges\":{{\"graph_idx\":{{\"cites\":[{{\"target\":\"doc:hub\",\"weight\":1.0}}]}}}}}}",
            .{i},
        );
        errdefer alloc.free(value);
        try writes.append(alloc, .{ .key = key, .value = value });
    }
    const hub_key = try alloc.dupe(u8, "doc:hub");
    errdefer alloc.free(hub_key);
    const hub_value = try alloc.dupe(u8, "{\"title\":\"hub\"}");
    errdefer alloc.free(hub_value);
    try writes.append(alloc, .{ .key = hub_key, .value = hub_value });

    try db.batch(.{
        .writes = writes.items,
        .sync_level = .write,
    });
    try db.runDerivedUntil(db.core.nextDerivedSequence());

    const target_generation = blk: {
        const graph_entry = db.core.graphIndex("graph_idx") orelse return error.IndexNotFound;
        break :blk graph_entry.index.edge_generation;
    };

    var local_refreshed = try db.refreshGraphMetric(alloc, "graph_idx", "pagerank_local");
    defer local_refreshed.deinit(alloc);
    try std.testing.expectEqual(graph_mod.GraphIndex.GraphMetricState.fresh, local_refreshed.state);
    try std.testing.expectEqual(target_generation, local_refreshed.published_generation);

    {
        const pending = db.pendingWorkStats().graph_metric;
        try std.testing.expect(pending.hasWork());
        try std.testing.expectEqual(@as(usize, 1), pending.queued_builds);
        try std.testing.expectEqual(@as(usize, 0), pending.active_builds);
    }

    const first_tick = try db.runGraphMetricPlannedMaintenanceForIdle(.{
        .worker_ids = &.{ "budget-worker-a", "budget-worker-b" },
        .max_rounds = 1,
        .max_metrics_per_round = 8,
        .max_pages_per_round = 1,
    });
    try std.testing.expect(first_tick.budget_exhausted);
    try std.testing.expect(first_tick.durableProgressed());
    try std.testing.expect(first_tick.rounds_executed <= 1);

    var total = first_tick;
    var finished = false;
    var saw_budget_exhausted = first_tick.budget_exhausted;
    var tick_index: usize = 0;
    while (tick_index < 400) : (tick_index += 1) {
        const tick = try db.runGraphMetricPlannedMaintenanceForIdle(.{
            .worker_ids = &.{ "budget-worker-a", "budget-worker-b" },
            .max_rounds = 1,
            .max_metrics_per_round = 8,
            .max_pages_per_round = 1,
        });
        total.add(tick);
        saw_budget_exhausted = saw_budget_exhausted or tick.budget_exhausted;
        const graph_entry = db.core.graphIndex("graph_idx") orelse return error.IndexNotFound;
        var status = try graph_entry.index.graphMetricStatus("pagerank_planned");
        defer status.deinit(alloc);
        if (status.state == .fresh and status.phase == .complete) {
            try std.testing.expectEqual(target_generation, status.published_generation);
            finished = true;
            break;
        }
        if (!tick.progressed()) return error.GraphMetricBuildNoEligiblePage;
    }
    try std.testing.expect(finished);
    try std.testing.expect(saw_budget_exhausted);
    try std.testing.expect(total.rounds_executed > 1);
    try std.testing.expect(total.pages_completed > 1);
    try std.testing.expect(total.phases_advanced > 0);
    try std.testing.expect(total.published > 0);

    const graph_entry = db.core.graphIndex("graph_idx") orelse return error.IndexNotFound;
    var local_status = try graph_entry.index.graphMetricStatus("pagerank_local");
    defer local_status.deinit(alloc);
    var planned_status = try graph_entry.index.graphMetricStatus("pagerank_planned");
    defer planned_status.deinit(alloc);
    try std.testing.expectEqual(local_status.published_generation, planned_status.published_generation);
    try std.testing.expectEqual(local_status.iterations_completed, planned_status.iterations_completed);
    try std.testing.expectEqual(local_status.converged, planned_status.converged);
    try std.testing.expectApproxEqAbs(local_status.delta, planned_status.delta, 0.0000001);

    const top_limit: usize = 32;
    const local_top = try graph_entry.index.graphMetricTopK("pagerank_local", top_limit);
    defer {
        for (local_top) |*score| score.deinit(alloc);
        alloc.free(local_top);
    }
    const planned_top = try graph_entry.index.graphMetricTopK("pagerank_planned", top_limit);
    defer {
        for (planned_top) |*score| score.deinit(alloc);
        alloc.free(planned_top);
    }
    try std.testing.expectEqual(local_top.len, planned_top.len);
    try std.testing.expectEqualStrings("doc:hub", planned_top[0].node);
    for (local_top, planned_top) |local, planned| {
        try std.testing.expectEqualStrings(local.node, planned.node);
        try std.testing.expectApproxEqAbs(local.score, planned.score, 0.0000001);
    }
}

test "db graph metric planned eigenvector production budget matches local oracle" {
    const alloc = std.testing.allocator;

    var path_buf: [256]u8 = undefined;
    const path = tempPath(&path_buf);
    defer cleanupTempDir(path);

    var db = try DB.open(alloc, std.mem.span(path), .{
        .start_index_workers = false,
        .ttl_cleanup = .{ .enabled = false },
    });
    defer db.close();

    try db.addIndex(.{
        .name = "graph_idx",
        .kind = .graph,
        .config_json = "{\"metrics\":{\"eigenvector_local\":{\"enabled\":true,\"kind\":\"eigenvector\",\"refresh\":\"manual\",\"max_iterations\":3,\"tolerance\":0.000000001,\"edge_filter\":{\"types\":[\"cites\"]}},\"eigenvector_planned\":{\"enabled\":true,\"kind\":\"eigenvector\",\"refresh\":\"background\",\"max_iterations\":3,\"tolerance\":0.000000001,\"edge_filter\":{\"types\":[\"cites\"]}}}}",
    });

    var writes = std.ArrayListUnmanaged(types.BatchWrite).empty;
    defer {
        for (writes.items) |write| {
            alloc.free(write.key);
            alloc.free(write.value);
        }
        writes.deinit(alloc);
    }
    for (0..130) |i| {
        const key = try std.fmt.allocPrint(alloc, "doc:src-{d:0>3}", .{i});
        errdefer alloc.free(key);
        const value = try std.fmt.allocPrint(
            alloc,
            "{{\"title\":\"source {d}\",\"_edges\":{{\"graph_idx\":{{\"cites\":[{{\"target\":\"doc:hub\",\"weight\":1.0}}]}}}}}}",
            .{i},
        );
        errdefer alloc.free(value);
        try writes.append(alloc, .{ .key = key, .value = value });
    }
    const hub_key = try alloc.dupe(u8, "doc:hub");
    errdefer alloc.free(hub_key);
    const hub_value = try alloc.dupe(u8, "{\"title\":\"hub\"}");
    errdefer alloc.free(hub_value);
    try writes.append(alloc, .{ .key = hub_key, .value = hub_value });

    try db.batch(.{
        .writes = writes.items,
        .sync_level = .write,
    });
    try db.runDerivedUntil(db.core.nextDerivedSequence());

    const target_generation = blk: {
        const graph_entry = db.core.graphIndex("graph_idx") orelse return error.IndexNotFound;
        break :blk graph_entry.index.edge_generation;
    };

    var local_refreshed = try db.refreshGraphMetric(alloc, "graph_idx", "eigenvector_local");
    defer local_refreshed.deinit(alloc);
    try std.testing.expectEqual(graph_mod.GraphIndex.GraphMetricState.fresh, local_refreshed.state);
    try std.testing.expectEqual(target_generation, local_refreshed.published_generation);

    {
        const pending = db.pendingWorkStats().graph_metric;
        try std.testing.expect(pending.hasWork());
        try std.testing.expectEqual(@as(usize, 1), pending.queued_builds);
        try std.testing.expectEqual(@as(usize, 0), pending.active_builds);
    }

    const first_tick = try db.runGraphMetricPlannedMaintenanceForIdle(.{
        .worker_ids = &.{ "budget-worker-a", "budget-worker-b" },
        .max_rounds = 1,
        .max_metrics_per_round = 8,
        .max_pages_per_round = 1,
    });
    try std.testing.expect(first_tick.budget_exhausted);
    try std.testing.expect(first_tick.durableProgressed());
    try std.testing.expect(first_tick.rounds_executed <= 1);

    var total = first_tick;
    var finished = false;
    var saw_budget_exhausted = first_tick.budget_exhausted;
    var tick_index: usize = 0;
    while (tick_index < 600) : (tick_index += 1) {
        const tick = try db.runGraphMetricPlannedMaintenanceForIdle(.{
            .worker_ids = &.{ "budget-worker-a", "budget-worker-b" },
            .max_rounds = 1,
            .max_metrics_per_round = 8,
            .max_pages_per_round = 1,
        });
        total.add(tick);
        saw_budget_exhausted = saw_budget_exhausted or tick.budget_exhausted;
        const graph_entry = db.core.graphIndex("graph_idx") orelse return error.IndexNotFound;
        var status = try graph_entry.index.graphMetricStatus("eigenvector_planned");
        defer status.deinit(alloc);
        if (status.state == .fresh and status.phase == .complete) {
            try std.testing.expectEqual(target_generation, status.published_generation);
            finished = true;
            break;
        }
        if (!tick.progressed()) return error.GraphMetricBuildNoEligiblePage;
    }
    try std.testing.expect(finished);
    try std.testing.expect(saw_budget_exhausted);
    try std.testing.expect(total.rounds_executed > 1);
    try std.testing.expect(total.pages_completed > 1);
    try std.testing.expect(total.phases_advanced > 0);
    try std.testing.expect(total.published > 0);

    const graph_entry = db.core.graphIndex("graph_idx") orelse return error.IndexNotFound;
    var local_status = try graph_entry.index.graphMetricStatus("eigenvector_local");
    defer local_status.deinit(alloc);
    var planned_status = try graph_entry.index.graphMetricStatus("eigenvector_planned");
    defer planned_status.deinit(alloc);
    try std.testing.expectEqual(local_status.published_generation, planned_status.published_generation);
    try std.testing.expectEqual(local_status.iterations_completed, planned_status.iterations_completed);
    try std.testing.expectEqual(local_status.converged, planned_status.converged);
    try std.testing.expectApproxEqAbs(local_status.delta, planned_status.delta, 0.0000001);

    const top_limit: usize = 32;
    const local_top = try graph_entry.index.graphMetricTopK("eigenvector_local", top_limit);
    defer {
        for (local_top) |*score| score.deinit(alloc);
        alloc.free(local_top);
    }
    const planned_top = try graph_entry.index.graphMetricTopK("eigenvector_planned", top_limit);
    defer {
        for (planned_top) |*score| score.deinit(alloc);
        alloc.free(planned_top);
    }
    try std.testing.expectEqual(local_top.len, planned_top.len);
    for (local_top, planned_top) |local, planned| {
        try std.testing.expectEqualStrings(local.node, planned.node);
        try std.testing.expect(std.math.isFinite(local.score));
        try std.testing.expect(std.math.isFinite(planned.score));
        try std.testing.expectApproxEqAbs(local.score, planned.score, 0.0000001);
    }
}

test "db graph metric planned hits production budget matches local oracle" {
    const alloc = std.testing.allocator;

    var local_path_buf: [256]u8 = undefined;
    const local_path = tempPath(&local_path_buf);
    defer cleanupTempDir(local_path);
    var planned_path_buf: [256]u8 = undefined;
    const planned_path = tempPath(&planned_path_buf);
    defer cleanupTempDir(planned_path);

    var local_db = try DB.open(alloc, std.mem.span(local_path), .{
        .start_index_workers = false,
        .ttl_cleanup = .{ .enabled = false },
    });
    defer local_db.close();
    var planned_db = try DB.open(alloc, std.mem.span(planned_path), .{
        .start_index_workers = false,
        .ttl_cleanup = .{ .enabled = false },
    });
    defer planned_db.close();

    const config_json =
        "{\"metrics\":{\"hits_authority\":{\"enabled\":true,\"kind\":\"hits_authority\",\"refresh\":\"background\",\"max_iterations\":2,\"tolerance\":0.000000001,\"edge_filter\":{\"types\":[\"cites\"]}},\"hits_hub\":{\"enabled\":true,\"kind\":\"hits_hub\",\"refresh\":\"background\",\"max_iterations\":2,\"tolerance\":0.000000001,\"edge_filter\":{\"types\":[\"cites\"]}}}}";
    try local_db.addIndex(.{
        .name = "graph_idx",
        .kind = .graph,
        .config_json = config_json,
    });
    try planned_db.addIndex(.{
        .name = "graph_idx",
        .kind = .graph,
        .config_json = config_json,
    });

    var writes = std.ArrayListUnmanaged(types.BatchWrite).empty;
    defer {
        for (writes.items) |write| {
            alloc.free(write.key);
            alloc.free(write.value);
        }
        writes.deinit(alloc);
    }
    for (0..130) |i| {
        const key = try std.fmt.allocPrint(alloc, "doc:hub-{d:0>3}", .{i});
        errdefer alloc.free(key);
        const value = try std.fmt.allocPrint(
            alloc,
            "{{\"title\":\"hub {d}\",\"_edges\":{{\"graph_idx\":{{\"cites\":[{{\"target\":\"doc:authority\",\"weight\":1.0}}]}}}}}}",
            .{i},
        );
        errdefer alloc.free(value);
        try writes.append(alloc, .{ .key = key, .value = value });
    }
    const authority_key = try alloc.dupe(u8, "doc:authority");
    errdefer alloc.free(authority_key);
    const authority_value = try alloc.dupe(u8, "{\"title\":\"authority\",\"_edges\":{\"graph_idx\":{\"cites\":[{\"target\":\"doc:authority\",\"weight\":1.0}]}}}");
    errdefer alloc.free(authority_value);
    try writes.append(alloc, .{ .key = authority_key, .value = authority_value });

    try local_db.batch(.{
        .writes = writes.items,
        .sync_level = .write,
    });
    try planned_db.batch(.{
        .writes = writes.items,
        .sync_level = .write,
    });
    try local_db.runDerivedUntil(local_db.core.nextDerivedSequence());
    try planned_db.runDerivedUntil(planned_db.core.nextDerivedSequence());

    const target_generation = blk: {
        const graph_entry = planned_db.core.graphIndex("graph_idx") orelse return error.IndexNotFound;
        break :blk graph_entry.index.edge_generation;
    };

    var local_authority_refreshed = try local_db.refreshGraphMetric(alloc, "graph_idx", "hits_authority");
    defer local_authority_refreshed.deinit(alloc);
    try std.testing.expectEqual(graph_mod.GraphIndex.GraphMetricState.fresh, local_authority_refreshed.state);
    try std.testing.expectEqual(target_generation, local_authority_refreshed.published_generation);

    {
        const pending = planned_db.pendingWorkStats().graph_metric;
        try std.testing.expect(pending.hasWork());
        try std.testing.expectEqual(@as(usize, 1), pending.queued_builds);
        try std.testing.expectEqual(@as(usize, 0), pending.active_builds);
    }

    const first_tick = try planned_db.runGraphMetricPlannedMaintenanceForIdle(.{
        .worker_ids = &.{ "budget-worker-a", "budget-worker-b" },
        .max_rounds = 1,
        .max_metrics_per_round = 8,
        .max_pages_per_round = 1,
    });
    try std.testing.expect(first_tick.budget_exhausted);
    try std.testing.expect(first_tick.durableProgressed());
    try std.testing.expect(first_tick.rounds_executed <= 1);

    var total = first_tick;
    var finished = false;
    var saw_budget_exhausted = first_tick.budget_exhausted;
    var tick_index: usize = 0;
    while (tick_index < 800) : (tick_index += 1) {
        const tick = try planned_db.runGraphMetricPlannedMaintenanceForIdle(.{
            .worker_ids = &.{ "budget-worker-a", "budget-worker-b" },
            .max_rounds = 1,
            .max_metrics_per_round = 8,
            .max_pages_per_round = 1,
        });
        total.add(tick);
        saw_budget_exhausted = saw_budget_exhausted or tick.budget_exhausted;
        const graph_entry = planned_db.core.graphIndex("graph_idx") orelse return error.IndexNotFound;
        var authority_status = try graph_entry.index.graphMetricStatus("hits_authority");
        defer authority_status.deinit(alloc);
        var hub_status = try graph_entry.index.graphMetricStatus("hits_hub");
        defer hub_status.deinit(alloc);
        if (authority_status.state == .fresh and authority_status.phase == .complete and hub_status.state == .fresh and hub_status.phase == .complete) {
            try std.testing.expectEqual(target_generation, authority_status.published_generation);
            try std.testing.expectEqual(authority_status.published_generation, hub_status.published_generation);
            finished = true;
            break;
        }
        if (!tick.progressed()) return error.GraphMetricBuildNoEligiblePage;
    }
    try std.testing.expect(finished);
    try std.testing.expect(saw_budget_exhausted);
    try std.testing.expect(total.rounds_executed > 1);
    try std.testing.expect(total.pages_completed > 1);
    try std.testing.expect(total.phases_advanced > 0);
    try std.testing.expect(total.published > 0);

    const local_graph = local_db.core.graphIndex("graph_idx") orelse return error.IndexNotFound;
    const planned_graph = planned_db.core.graphIndex("graph_idx") orelse return error.IndexNotFound;
    var local_authority_status = try local_graph.index.graphMetricStatus("hits_authority");
    defer local_authority_status.deinit(alloc);
    var local_hub_status = try local_graph.index.graphMetricStatus("hits_hub");
    defer local_hub_status.deinit(alloc);
    var planned_authority_status = try planned_graph.index.graphMetricStatus("hits_authority");
    defer planned_authority_status.deinit(alloc);
    var planned_hub_status = try planned_graph.index.graphMetricStatus("hits_hub");
    defer planned_hub_status.deinit(alloc);
    try std.testing.expectEqual(local_authority_status.published_generation, planned_authority_status.published_generation);
    try std.testing.expectEqual(local_hub_status.published_generation, planned_hub_status.published_generation);
    try std.testing.expectEqual(planned_authority_status.published_generation, planned_hub_status.published_generation);
    try std.testing.expectEqual(local_authority_status.iterations_completed, planned_authority_status.iterations_completed);
    try std.testing.expectEqual(local_hub_status.iterations_completed, planned_hub_status.iterations_completed);
    try std.testing.expectEqual(local_authority_status.converged, planned_authority_status.converged);
    try std.testing.expectEqual(local_hub_status.converged, planned_hub_status.converged);
    try std.testing.expectApproxEqAbs(local_authority_status.delta, planned_authority_status.delta, 0.0000001);
    try std.testing.expectApproxEqAbs(local_hub_status.delta, planned_hub_status.delta, 0.0000001);

    const local_authorities = try local_graph.index.graphMetricTopK("hits_authority", 32);
    defer {
        for (local_authorities) |*score| score.deinit(alloc);
        alloc.free(local_authorities);
    }
    const planned_authorities = try planned_graph.index.graphMetricTopK("hits_authority", 32);
    defer {
        for (planned_authorities) |*score| score.deinit(alloc);
        alloc.free(planned_authorities);
    }
    try std.testing.expectEqual(local_authorities.len, planned_authorities.len);
    try std.testing.expectEqualStrings("doc:authority", planned_authorities[0].node);
    for (local_authorities, planned_authorities) |local, planned| {
        try std.testing.expectEqualStrings(local.node, planned.node);
        try std.testing.expect(std.math.isFinite(local.score));
        try std.testing.expect(std.math.isFinite(planned.score));
        try std.testing.expectApproxEqAbs(local.score, planned.score, 0.0000001);
    }

    const local_hubs = try local_graph.index.graphMetricTopK("hits_hub", 32);
    defer {
        for (local_hubs) |*score| score.deinit(alloc);
        alloc.free(local_hubs);
    }
    const planned_hubs = try planned_graph.index.graphMetricTopK("hits_hub", 32);
    defer {
        for (planned_hubs) |*score| score.deinit(alloc);
        alloc.free(planned_hubs);
    }
    try std.testing.expectEqual(local_hubs.len, planned_hubs.len);
    for (local_hubs, planned_hubs) |local, planned| {
        try std.testing.expectEqualStrings(local.node, planned.node);
        try std.testing.expect(std.math.isFinite(local.score));
        try std.testing.expect(std.math.isFinite(planned.score));
        try std.testing.expectApproxEqAbs(local.score, planned.score, 0.0000001);
    }
}

test "db graph metric planned maintenance drains background centrality family work" {
    const alloc = std.testing.allocator;

    var path_buf: [256]u8 = undefined;
    const path = tempPath(&path_buf);
    defer cleanupTempDir(path);

    var db = try DB.open(alloc, std.mem.span(path), .{
        .start_index_workers = false,
        .ttl_cleanup = .{ .enabled = false },
    });
    defer db.close();

    try db.addIndex(.{
        .name = "graph_idx",
        .kind = .graph,
        .config_json = "{\"metrics\":{\"degree\":{\"enabled\":true,\"kind\":\"degree\",\"refresh\":\"background\",\"edge_filter\":{\"types\":[\"cites\"]}},\"eigenvector\":{\"enabled\":true,\"kind\":\"eigenvector\",\"refresh\":\"background\",\"max_iterations\":1,\"tolerance\":0.000001,\"edge_filter\":{\"types\":[\"cites\"]}},\"hits_authority\":{\"enabled\":true,\"kind\":\"hits_authority\",\"refresh\":\"background\",\"max_iterations\":1,\"tolerance\":0.000001,\"edge_filter\":{\"types\":[\"cites\"]}},\"hits_hub\":{\"enabled\":true,\"kind\":\"hits_hub\",\"refresh\":\"background\",\"max_iterations\":1,\"tolerance\":0.000001,\"edge_filter\":{\"types\":[\"cites\"]}}}}",
    });

    try db.batch(.{
        .writes = &.{
            .{ .key = "doc:hub-a", .value = "{\"title\":\"hub a\",\"_edges\":{\"graph_idx\":{\"cites\":[{\"target\":\"doc:authority\",\"weight\":1.0}]}}}" },
            .{ .key = "doc:hub-b", .value = "{\"title\":\"hub b\",\"_edges\":{\"graph_idx\":{\"cites\":[{\"target\":\"doc:authority\",\"weight\":1.0}]}}}" },
            .{ .key = "doc:authority", .value = "{\"title\":\"authority\"}" },
        },
        .sync_level = .write,
    });

    try db.runDerivedUntil(db.core.nextDerivedSequence());

    const target_generation = blk: {
        const graph_entry = db.core.graphIndex("graph_idx") orelse return error.IndexNotFound;
        const metric_names = [_][]const u8{ "degree", "eigenvector", "hits_authority", "hits_hub" };
        for (metric_names) |metric_name| {
            var status = try graph_entry.index.graphMetricStatus(metric_name);
            defer status.deinit(alloc);
            try std.testing.expectEqual(graph_mod.GraphIndex.GraphMetricState.not_ready, status.state);
        }
        break :blk graph_entry.index.edge_generation;
    };

    const maintenance = try db.runGraphMetricPlannedMaintenanceForIdle(.{
        .worker_id = "planned-maintenance-centrality",
        .max_rounds = 400,
        .max_metrics_per_round = 8,
        .max_pages_per_round = 1,
    });
    try std.testing.expectEqual(@as(usize, 3), maintenance.builds_started);
    try std.testing.expect(maintenance.pages_completed > 0);
    try std.testing.expect(maintenance.phases_advanced > 0);

    {
        const graph_entry = db.core.graphIndex("graph_idx") orelse return error.IndexNotFound;
        const metric_names = [_][]const u8{ "degree", "eigenvector", "hits_authority", "hits_hub" };
        var authority_generation: u64 = 0;
        for (metric_names) |metric_name| {
            var status = try graph_entry.index.graphMetricStatus(metric_name);
            defer status.deinit(alloc);
            try std.testing.expectEqual(graph_mod.GraphIndex.GraphMetricState.fresh, status.state);
            try std.testing.expectEqual(target_generation, status.published_generation);
            try std.testing.expectEqual(graph_mod.GraphIndex.GraphMetricBuildPhase.complete, status.phase);
            try std.testing.expect(status.iterations_completed > 0);
            if (std.mem.eql(u8, metric_name, "hits_authority")) {
                authority_generation = status.published_generation;
            } else if (std.mem.eql(u8, metric_name, "hits_hub")) {
                try std.testing.expectEqual(authority_generation, status.published_generation);
            }
        }
    }

    var published_result = try db.search(alloc, .{
        .graph_metric_queries = &.{
            .{
                .name = "degree",
                .query = .{
                    .index_name = "graph_idx",
                    .metric_name = "degree",
                    .top_k = 3,
                    .freshness = .fresh,
                },
            },
            .{
                .name = "authority",
                .query = .{
                    .index_name = "graph_idx",
                    .metric_name = "hits_authority",
                    .top_k = 3,
                    .freshness = .fresh,
                },
            },
            .{
                .name = "hub",
                .query = .{
                    .index_name = "graph_idx",
                    .metric_name = "hits_hub",
                    .top_k = 3,
                    .freshness = .fresh,
                },
            },
        },
        .limit = 0,
    });
    defer published_result.deinit();
    try std.testing.expectEqual(@as(usize, 3), published_result.graph_metric_results.len);
    try std.testing.expectEqualStrings("doc:authority", published_result.graph_metric_results[0].scores[0].node);
    try std.testing.expectEqualStrings("doc:authority", published_result.graph_metric_results[1].scores[0].node);
    try std.testing.expect(published_result.graph_metric_results[2].scores[0].score >= published_result.graph_metric_results[2].scores[1].score);
}

test "db graph metric planned scheduler sweeps active eigenvector work" {
    const alloc = std.testing.allocator;

    var path_buf: [256]u8 = undefined;
    const path = tempPath(&path_buf);
    defer cleanupTempDir(path);

    var db = try DB.open(alloc, std.mem.span(path), .{
        .start_index_workers = false,
        .ttl_cleanup = .{ .enabled = false },
    });
    defer db.close();

    try db.addIndex(.{
        .name = "graph_idx",
        .kind = .graph,
        .config_json = "{\"metrics\":{\"eigenvector\":{\"enabled\":true,\"kind\":\"eigenvector\",\"refresh\":\"manual\",\"max_iterations\":20,\"tolerance\":0.000001,\"edge_filter\":{\"types\":[\"cites\"]}}}}",
    });

    try db.batch(.{
        .writes = &.{
            .{ .key = "doc:a", .value = "{\"title\":\"alpha\",\"_edges\":{\"graph_idx\":{\"cites\":[{\"target\":\"doc:b\",\"weight\":1.0}]}}}" },
            .{ .key = "doc:b", .value = "{\"title\":\"beta\",\"_edges\":{\"graph_idx\":{\"cites\":[{\"target\":\"doc:b\",\"weight\":1.0}]}}}" },
            .{ .key = "doc:c", .value = "{\"title\":\"gamma\",\"_edges\":{\"graph_idx\":{\"cites\":[{\"target\":\"doc:b\",\"weight\":1.0}]}}}" },
        },
        .sync_level = .write,
    });
    try db.runUntilIdle();

    const target_generation = blk: {
        const graph_entry = db.core.graphIndex("graph_idx") orelse return error.IndexNotFound;
        break :blk graph_entry.index.edge_generation;
    };
    var started = try db.ensureGraphMetricPlannedBuild(alloc, "graph_idx", "eigenvector", target_generation);
    defer started.deinit(alloc);
    try std.testing.expectEqual(graph_mod.GraphIndex.GraphMetricState.building, started.state);
    try std.testing.expectEqual(target_generation, started.building_generation);

    const workers = [_][]const u8{ "eigenvector-sweep-a", "eigenvector-sweep-b", "eigenvector-sweep-c" };
    var finished = false;
    var step_index: usize = 0;
    while (step_index < 2000) : (step_index += 1) {
        const worker = try db.runGraphMetricPlannedWorkerSweep(.{
            .worker_id = workers[step_index % workers.len],
            .max_pages = 1,
        });
        const coordinator = try db.runGraphMetricPlannedCoordinatorSweep(.{
            .max_metrics = 8,
            .start_background_builds = false,
        });
        {
            const graph_entry = db.core.graphIndex("graph_idx") orelse return error.IndexNotFound;
            var status = try graph_entry.index.graphMetricStatus("eigenvector");
            defer status.deinit(alloc);
            if (status.state == .fresh and status.published_generation != 0 and status.phase == .complete) {
                try std.testing.expect(status.iterations_completed > 0);
                finished = true;
                break;
            }
        }
        if (!worker.progressed() and !coordinator.progressed()) {
            return error.GraphMetricBuildNoEligiblePage;
        }
    }
    try std.testing.expect(finished);

    var published_result = try db.search(alloc, .{
        .graph_metric_queries = &.{.{
            .name = "central",
            .query = .{
                .index_name = "graph_idx",
                .metric_name = "eigenvector",
                .top_k = 3,
                .freshness = .fresh,
            },
        }},
        .limit = 0,
    });
    defer published_result.deinit();
    try std.testing.expectEqual(@as(usize, 1), published_result.graph_metric_results.len);
    try std.testing.expectEqual(graph_mod.GraphIndex.GraphMetricState.fresh, published_result.graph_metric_results[0].status.state);
    try std.testing.expectEqual(target_generation, published_result.graph_metric_results[0].status.published_generation);
    try std.testing.expectEqual(@as(usize, 3), published_result.graph_metric_results[0].scores.len);
    try std.testing.expectEqualStrings("doc:b", published_result.graph_metric_results[0].scores[0].node);
    try std.testing.expect(published_result.graph_metric_results[0].scores[0].score > published_result.graph_metric_results[0].scores[1].score);
    try std.testing.expectApproxEqAbs(@as(f64, 1.0), published_result.graph_metric_results[0].scores[0].score, 0.001);
}

test "db graph metric planned scheduler sweeps active hits work" {
    const alloc = std.testing.allocator;

    var path_buf: [256]u8 = undefined;
    const path = tempPath(&path_buf);
    defer cleanupTempDir(path);

    var db = try DB.open(alloc, std.mem.span(path), .{
        .start_index_workers = false,
        .ttl_cleanup = .{ .enabled = false },
    });
    defer db.close();

    try db.addIndex(.{
        .name = "graph_idx",
        .kind = .graph,
        .config_json = "{\"metrics\":{\"hits_authority\":{\"enabled\":true,\"kind\":\"hits_authority\",\"refresh\":\"manual\",\"max_iterations\":1,\"tolerance\":0.000001,\"edge_filter\":{\"types\":[\"cites\"]}},\"hits_hub\":{\"enabled\":true,\"kind\":\"hits_hub\",\"refresh\":\"manual\",\"max_iterations\":1,\"tolerance\":0.000001,\"edge_filter\":{\"types\":[\"cites\"]}}}}",
    });

    try db.batch(.{
        .writes = &.{
            .{ .key = "doc:hub-a", .value = "{\"title\":\"hub a\",\"_edges\":{\"graph_idx\":{\"cites\":[{\"target\":\"doc:authority\",\"weight\":1.0}]}}}" },
            .{ .key = "doc:hub-b", .value = "{\"title\":\"hub b\",\"_edges\":{\"graph_idx\":{\"cites\":[{\"target\":\"doc:authority\",\"weight\":1.0}]}}}" },
            .{ .key = "doc:authority", .value = "{\"title\":\"authority\"}" },
        },
        .sync_level = .write,
    });
    try db.runUntilIdle();

    const target_generation = blk: {
        const graph_entry = db.core.graphIndex("graph_idx") orelse return error.IndexNotFound;
        break :blk graph_entry.index.edge_generation;
    };
    var started = try db.ensureGraphMetricPlannedBuild(alloc, "graph_idx", "hits_authority", target_generation);
    defer started.deinit(alloc);
    try std.testing.expectEqual(graph_mod.GraphIndex.GraphMetricState.building, started.state);
    try std.testing.expectEqual(target_generation, started.building_generation);

    const workers = [_][]const u8{ "hits-sweep-a", "hits-sweep-b", "hits-sweep-c" };
    var finished = false;
    var step_index: usize = 0;
    while (step_index < 2000) : (step_index += 1) {
        const worker = try db.runGraphMetricPlannedWorkerSweep(.{
            .worker_id = workers[step_index % workers.len],
            .max_pages = 1,
        });
        const coordinator = try db.runGraphMetricPlannedCoordinatorSweep(.{
            .max_metrics = 8,
            .start_background_builds = false,
        });
        {
            const graph_entry = db.core.graphIndex("graph_idx") orelse return error.IndexNotFound;
            var authority_status = try graph_entry.index.graphMetricStatus("hits_authority");
            defer authority_status.deinit(alloc);
            var hub_status = try graph_entry.index.graphMetricStatus("hits_hub");
            defer hub_status.deinit(alloc);
            if (authority_status.state == .fresh and authority_status.published_generation != 0 and authority_status.phase == .complete) {
                try std.testing.expectEqual(graph_mod.GraphIndex.GraphMetricState.fresh, hub_status.state);
                try std.testing.expectEqual(authority_status.published_generation, hub_status.published_generation);
                try std.testing.expectEqual(authority_status.iterations_completed, hub_status.iterations_completed);
                try std.testing.expect(authority_status.iterations_completed > 0);
                finished = true;
                break;
            }
        }
        if (!worker.progressed() and !coordinator.progressed()) {
            return error.GraphMetricBuildNoEligiblePage;
        }
    }
    try std.testing.expect(finished);

    var published_result = try db.search(alloc, .{
        .graph_metric_queries = &.{
            .{
                .name = "authority",
                .query = .{
                    .index_name = "graph_idx",
                    .metric_name = "hits_authority",
                    .top_k = 3,
                    .freshness = .fresh,
                },
            },
            .{
                .name = "hub",
                .query = .{
                    .index_name = "graph_idx",
                    .metric_name = "hits_hub",
                    .top_k = 3,
                    .freshness = .fresh,
                },
            },
        },
        .limit = 0,
    });
    defer published_result.deinit();
    try std.testing.expectEqual(@as(usize, 2), published_result.graph_metric_results.len);
    try std.testing.expectEqual(graph_mod.GraphIndex.GraphMetricState.fresh, published_result.graph_metric_results[0].status.state);
    try std.testing.expectEqual(target_generation, published_result.graph_metric_results[0].status.published_generation);
    try std.testing.expectEqual(graph_mod.GraphIndex.GraphMetricState.fresh, published_result.graph_metric_results[1].status.state);
    try std.testing.expectEqual(published_result.graph_metric_results[0].status.published_generation, published_result.graph_metric_results[1].status.published_generation);
    try std.testing.expectEqual(@as(usize, 3), published_result.graph_metric_results[0].scores.len);
    try std.testing.expectEqual(@as(usize, 3), published_result.graph_metric_results[1].scores.len);
    try std.testing.expectEqualStrings("doc:authority", published_result.graph_metric_results[0].scores[0].node);
    try std.testing.expectApproxEqAbs(@as(f64, 1.0), published_result.graph_metric_results[0].scores[0].score, 0.001);
    try std.testing.expect(published_result.graph_metric_results[1].scores[0].score >= published_result.graph_metric_results[1].scores[1].score);
    try std.testing.expect(published_result.graph_metric_results[1].scores[1].score > published_result.graph_metric_results[1].scores[2].score);
}

test "db single-vector failed planned rebuild preserves published public reads" {
    const alloc = std.testing.allocator;
    try verifyDbSingleVectorFailedPlannedRebuildPreservesPublishedPublicReads(DB, alloc, "pagerank", "pagerank");
    try verifyDbSingleVectorFailedPlannedRebuildPreservesPublishedPublicReads(DB, alloc, "eigenvector", "eigenvector");
}

test "db paired hits failed planned rebuild preserves published public reads" {
    const alloc = std.testing.allocator;

    var path_buf: [256]u8 = undefined;
    const path = tempPath(&path_buf);
    defer cleanupTempDir(path);

    var db = try DB.open(alloc, std.mem.span(path), .{
        .start_index_workers = false,
        .ttl_cleanup = .{ .enabled = false },
    });
    defer db.close();

    try db.addIndex(.{
        .name = "ft_v1",
        .kind = .full_text,
        .config_json = "{\"store\":true}",
    });
    try db.addIndex(.{
        .name = "graph_idx",
        .kind = .graph,
        .config_json = "{\"metrics\":{\"hits_authority\":{\"enabled\":true,\"kind\":\"hits_authority\",\"refresh\":\"manual\",\"max_iterations\":1,\"tolerance\":0.000001,\"edge_filter\":{\"types\":[\"cites\"]}},\"hits_hub\":{\"enabled\":true,\"kind\":\"hits_hub\",\"refresh\":\"manual\",\"max_iterations\":1,\"tolerance\":0.000001,\"edge_filter\":{\"types\":[\"cites\"]}}}}",
    });

    try db.batch(.{
        .writes = &.{
            .{ .key = "doc:hub-a", .value = "{\"title\":\"hub a\",\"_edges\":{\"graph_idx\":{\"cites\":[{\"target\":\"doc:authority\",\"weight\":1.0}]}}}" },
            .{ .key = "doc:hub-b", .value = "{\"title\":\"hub b\",\"_edges\":{\"graph_idx\":{\"cites\":[{\"target\":\"doc:authority\",\"weight\":1.0}]}}}" },
            .{ .key = "doc:authority", .value = "{\"title\":\"authority\"}" },
        },
        .sync_level = .full_index,
    });
    try db.runDerivedUntil(db.core.nextDerivedSequence());

    var refreshed = try db.refreshGraphMetric(alloc, "graph_idx", "hits_authority");
    defer refreshed.deinit(alloc);
    try std.testing.expectEqual(graph_mod.GraphIndex.GraphMetricState.fresh, refreshed.state);
    const published_generation = refreshed.published_generation;
    {
        const graph_entry = db.core.graphIndex("graph_idx") orelse return error.IndexNotFound;
        var hub_status = try graph_entry.index.graphMetricStatus("hits_hub");
        defer hub_status.deinit(alloc);
        try std.testing.expectEqual(graph_mod.GraphIndex.GraphMetricState.fresh, hub_status.state);
        try std.testing.expectEqual(published_generation, hub_status.published_generation);
    }

    var initial = try db.search(alloc, .{
        .graph_metric_queries = &.{
            .{
                .name = "authority",
                .query = .{
                    .index_name = "graph_idx",
                    .metric_name = "hits_authority",
                    .top_k = 3,
                    .freshness = .fresh,
                },
            },
            .{
                .name = "hub",
                .query = .{
                    .index_name = "graph_idx",
                    .metric_name = "hits_hub",
                    .top_k = 3,
                    .freshness = .fresh,
                },
            },
        },
        .limit = 0,
    });
    defer initial.deinit();
    try std.testing.expectEqual(@as(usize, 2), initial.graph_metric_results.len);
    try std.testing.expectEqualStrings("doc:authority", initial.graph_metric_results[0].scores[0].node);
    try std.testing.expectEqual(graph_mod.GraphIndex.GraphMetricState.fresh, initial.graph_metric_results[0].status.state);
    try std.testing.expectEqual(graph_mod.GraphIndex.GraphMetricState.fresh, initial.graph_metric_results[1].status.state);
    try std.testing.expectEqual(published_generation, initial.graph_metric_results[0].status.published_generation);
    try std.testing.expectEqual(published_generation, initial.graph_metric_results[1].status.published_generation);

    try db.batch(.{
        .writes = &.{
            .{ .key = "doc:new-hub", .value = "{\"title\":\"new hub\",\"_edges\":{\"graph_idx\":{\"cites\":[{\"target\":\"doc:authority\",\"weight\":1.0}]}}}" },
        },
        .sync_level = .full_index,
    });
    try db.runDerivedUntil(db.core.nextDerivedSequence());

    const rebuilding_generation = blk: {
        const graph_entry = db.core.graphIndex("graph_idx") orelse return error.IndexNotFound;
        const target_generation = graph_entry.index.edge_generation;
        try std.testing.expect(target_generation > published_generation);
        var building = try db.ensureGraphMetricPlannedBuild(alloc, "graph_idx", "hits_authority", target_generation);
        defer building.deinit(alloc);
        try std.testing.expectEqual(graph_mod.GraphIndex.GraphMetricState.building, building.state);
        try std.testing.expectEqual(target_generation, building.building_generation);
        break :blk target_generation;
    };

    var failed = try db.failGraphMetricPlannedBuild(alloc, "graph_idx", "hits_authority", error.InvalidGraphMetricScore);
    defer failed.deinit(alloc);
    try std.testing.expectEqual(graph_mod.GraphIndex.GraphMetricState.failed, failed.state);
    try std.testing.expectEqual(published_generation, failed.published_generation);
    {
        const graph_entry = db.core.graphIndex("graph_idx") orelse return error.IndexNotFound;
        var authority_status = try graph_entry.index.graphMetricStatus("hits_authority");
        defer authority_status.deinit(alloc);
        var hub_status = try graph_entry.index.graphMetricStatus("hits_hub");
        defer hub_status.deinit(alloc);
        try std.testing.expectEqual(graph_mod.GraphIndex.GraphMetricState.failed, authority_status.state);
        try std.testing.expectEqual(graph_mod.GraphIndex.GraphMetricState.failed, hub_status.state);
        try std.testing.expectEqual(published_generation, authority_status.published_generation);
        try std.testing.expectEqual(published_generation, hub_status.published_generation);
        try std.testing.expectEqual(rebuilding_generation, authority_status.target_edge_generation);
        try std.testing.expectEqual(rebuilding_generation, hub_status.target_edge_generation);
    }

    var published_after_failure = try db.search(alloc, .{
        .graph_metric_queries = &.{
            .{
                .name = "authority",
                .query = .{
                    .index_name = "graph_idx",
                    .metric_name = "hits_authority",
                    .top_k = 4,
                    .freshness = .published,
                },
            },
            .{
                .name = "hub",
                .query = .{
                    .index_name = "graph_idx",
                    .metric_name = "hits_hub",
                    .top_k = 4,
                    .freshness = .published,
                },
            },
        },
        .limit = 0,
    });
    defer published_after_failure.deinit();
    try std.testing.expectEqual(@as(usize, 2), published_after_failure.graph_metric_results.len);
    for (published_after_failure.graph_metric_results) |result| {
        try std.testing.expectEqual(graph_mod.GraphIndex.GraphMetricState.failed, result.status.state);
        try std.testing.expectEqual(published_generation, result.status.published_generation);
        try std.testing.expect(result.scores.len > 0);
        for (result.scores) |score| {
            try std.testing.expect(!std.mem.eql(u8, score.node, "doc:new-hub"));
        }
    }
    try std.testing.expectEqualStrings("doc:authority", published_after_failure.graph_metric_results[0].scores[0].node);

    const published_metric_reads = [_]graph_query_mod.GraphMetricRead{
        .{ .name = "hits_authority", .freshness = .published },
        .{ .name = "hits_hub", .freshness = .published },
    };
    const published_graph_query = graph_query_mod.GraphQuery{
        .query_type = .neighbors,
        .index_name = "graph_idx",
        .start_nodes = .{ .keys = &.{"doc:hub-a"} },
        .params = .{ .edge_types = &.{"cites"}, .direction = .out, .max_depth = 1 },
        .metrics = &published_metric_reads,
        .include_metric_status = true,
    };
    var traversal_after_failure = try db.search(alloc, .{
        .graph_queries = &.{.{ .name = "neighbors", .query = published_graph_query }},
        .limit = 0,
    });
    defer traversal_after_failure.deinit();
    try std.testing.expectEqual(@as(usize, 1), traversal_after_failure.graph_results.len);
    try std.testing.expectEqual(@as(usize, 1), traversal_after_failure.graph_results[0].nodes.len);
    try std.testing.expectEqualStrings("doc:authority", traversal_after_failure.graph_results[0].nodes[0].key);
    try std.testing.expectEqual(@as(usize, 2), traversal_after_failure.graph_results[0].nodes[0].metrics.len);
    try std.testing.expectEqualStrings("hits_authority", traversal_after_failure.graph_results[0].nodes[0].metrics[0].name);
    try std.testing.expect(traversal_after_failure.graph_results[0].nodes[0].metrics[0].score != null);
    try std.testing.expectApproxEqAbs(@as(f64, 1.0), traversal_after_failure.graph_results[0].nodes[0].metrics[0].score.?, 0.001);
    try std.testing.expectEqual(@as(usize, 2), traversal_after_failure.graph_results[0].metric_status.len);
    for (traversal_after_failure.graph_results[0].metric_status) |status| {
        try std.testing.expectEqual(graph_mod.GraphIndex.GraphMetricState.failed, status.state);
        try std.testing.expectEqual(published_generation, status.published_generation);
    }

    const published_metric_orders = [_]graph_query_mod.GraphMetricOrder{.{
        .name = "hits_authority",
        .freshness = .published,
    }};
    var published_order_query = published_graph_query;
    published_order_query.order_by = &published_metric_orders;
    var traversal_order_after_failure = try db.search(alloc, .{
        .graph_queries = &.{.{ .name = "neighbors", .query = published_order_query }},
        .limit = 0,
    });
    defer traversal_order_after_failure.deinit();
    try std.testing.expectEqual(@as(usize, 1), traversal_order_after_failure.graph_results.len);
    try std.testing.expectEqualStrings("doc:authority", traversal_order_after_failure.graph_results[0].nodes[0].key);
    try std.testing.expectEqual(graph_mod.GraphIndex.GraphMetricState.failed, traversal_order_after_failure.graph_results[0].metric_status[0].state);

    const published_metric_filters = [_]graph_query_mod.GraphMetricFilter{.{
        .name = "hits_authority",
        .op = .gte,
        .value = 0.5,
        .freshness = .published,
    }};
    var published_filter_query = published_graph_query;
    published_filter_query.where_metric = &published_metric_filters;
    var traversal_filter_after_failure = try db.search(alloc, .{
        .graph_queries = &.{.{ .name = "neighbors", .query = published_filter_query }},
        .limit = 0,
    });
    defer traversal_filter_after_failure.deinit();
    try std.testing.expectEqual(@as(usize, 1), traversal_filter_after_failure.graph_results.len);
    try std.testing.expectEqualStrings("doc:authority", traversal_filter_after_failure.graph_results[0].nodes[0].key);
    try std.testing.expectEqual(graph_mod.GraphIndex.GraphMetricState.failed, traversal_filter_after_failure.graph_results[0].metric_status[0].state);

    var rerank_after_failure = try db.search(alloc, .{
        .index_name = "ft_v1",
        .full_text = .{ .match_all = {} },
        .graph_metric_rerank = .{
            .index_name = "graph_idx",
            .metric_name = "hits_authority",
            .freshness = .published,
            .weight = 1.0,
        },
        .limit = 4,
        .include_stored = false,
    });
    defer rerank_after_failure.deinit();
    try std.testing.expectEqual(@as(u32, 4), rerank_after_failure.total_hits);
    const rerank_status = rerank_after_failure.graph_metric_rerank_status orelse return error.TestUnexpectedResult;
    try std.testing.expectEqual(graph_mod.GraphIndex.GraphMetricState.failed, rerank_status.state);
    try std.testing.expectEqual(published_generation, rerank_status.published_generation);
    var saw_authority_score = false;
    var saw_new_hub_missing_score = false;
    for (rerank_after_failure.hits) |hit| {
        const details = hit.score_details orelse return error.TestUnexpectedResult;
        try std.testing.expectEqual(published_generation, details.published_generation);
        if (std.mem.eql(u8, hit.id, "doc:authority")) {
            saw_authority_score = details.metric_score != null;
        } else if (std.mem.eql(u8, hit.id, "doc:new-hub")) {
            saw_new_hub_missing_score = details.metric_score == null;
        }
    }
    try std.testing.expect(saw_authority_score);
    try std.testing.expect(saw_new_hub_missing_score);

    try std.testing.expectError(error.MetricStale, db.search(alloc, .{
        .graph_metric_queries = &.{.{
            .name = "authority",
            .query = .{
                .index_name = "graph_idx",
                .metric_name = "hits_authority",
                .top_k = 1,
                .freshness = .fresh,
            },
        }},
        .limit = 0,
    }));
    try std.testing.expectError(error.MetricStale, db.search(alloc, .{
        .graph_metric_queries = &.{.{
            .name = "hub",
            .query = .{
                .index_name = "graph_idx",
                .metric_name = "hits_hub",
                .top_k = 1,
                .freshness = .fresh,
            },
        }},
        .limit = 0,
    }));

    const fresh_metric_reads = [_]graph_query_mod.GraphMetricRead{
        .{ .name = "hits_authority", .freshness = .fresh },
        .{ .name = "hits_hub", .freshness = .fresh },
    };
    var fresh_projection_query = published_graph_query;
    fresh_projection_query.metrics = &fresh_metric_reads;
    try std.testing.expectError(error.MetricStale, db.search(alloc, .{
        .graph_queries = &.{.{ .name = "neighbors", .query = fresh_projection_query }},
        .limit = 0,
    }));

    const fresh_metric_orders = [_]graph_query_mod.GraphMetricOrder{.{
        .name = "hits_authority",
        .freshness = .fresh,
    }};
    var fresh_order_query = published_graph_query;
    fresh_order_query.order_by = &fresh_metric_orders;
    try std.testing.expectError(error.MetricStale, db.search(alloc, .{
        .graph_queries = &.{.{ .name = "neighbors", .query = fresh_order_query }},
        .limit = 0,
    }));

    const fresh_metric_filters = [_]graph_query_mod.GraphMetricFilter{.{
        .name = "hits_authority",
        .op = .gte,
        .value = 0.5,
        .freshness = .fresh,
    }};
    var fresh_filter_query = published_graph_query;
    fresh_filter_query.where_metric = &fresh_metric_filters;
    try std.testing.expectError(error.MetricStale, db.search(alloc, .{
        .graph_queries = &.{.{ .name = "neighbors", .query = fresh_filter_query }},
        .limit = 0,
    }));

    try std.testing.expectError(error.MetricStale, db.search(alloc, .{
        .index_name = "ft_v1",
        .full_text = .{ .match_all = {} },
        .graph_metric_rerank = .{
            .index_name = "graph_idx",
            .metric_name = "hits_authority",
            .freshness = .fresh,
            .weight = 1.0,
        },
        .limit = 4,
        .include_stored = false,
    }));
}

test "db graph metric pause and resume controls background maintenance" {
    const alloc = std.testing.allocator;

    var path_buf: [256]u8 = undefined;
    const path = tempPath(&path_buf);
    defer cleanupTempDir(path);

    var db = try DB.open(alloc, std.mem.span(path), .{
        .start_index_workers = false,
        .ttl_cleanup = .{ .enabled = false },
    });
    defer db.close();

    try db.addIndex(.{
        .name = "graph_idx",
        .kind = .graph,
        .config_json = "{\"metrics\":{\"degree\":{\"enabled\":true,\"kind\":\"degree\",\"refresh\":\"background\",\"edge_filter\":{\"types\":[\"cites\"]}}}}",
    });

    try db.batch(.{
        .writes = &.{
            .{ .key = "doc:a", .value = "{\"title\":\"alpha\",\"_edges\":{\"graph_idx\":{\"cites\":[{\"target\":\"doc:b\",\"weight\":1.0}]}}}" },
            .{ .key = "doc:b", .value = "{\"title\":\"beta\"}" },
        },
        .sync_level = .write,
    });

    try db.runUntilIdle();

    var initial = try db.refreshGraphMetric(alloc, "graph_idx", "degree");
    defer initial.deinit(alloc);
    try std.testing.expectEqual(graph_mod.GraphIndex.GraphMetricState.fresh, initial.state);
    try std.testing.expect(!initial.maintenance_paused);
    const first_generation = initial.published_generation;
    try std.testing.expect(first_generation > 0);

    var paused = try db.pauseGraphMetricMaintenance(alloc, "graph_idx", "degree");
    defer paused.deinit(alloc);
    try std.testing.expectEqual(graph_mod.GraphIndex.GraphMetricState.fresh, paused.state);
    try std.testing.expect(paused.maintenance_paused);
    try std.testing.expectEqual(first_generation, paused.published_generation);

    try db.batch(.{
        .writes = &.{
            .{ .key = "doc:c", .value = "{\"title\":\"gamma\",\"_edges\":{\"graph_idx\":{\"cites\":[{\"target\":\"doc:b\",\"weight\":1.0}]}}}" },
        },
        .sync_level = .write,
    });

    {
        const pending = db.pendingWorkStats().graph_metric;
        try std.testing.expectEqual(@as(usize, 1), pending.metrics_scanned);
        try std.testing.expectEqual(@as(usize, 1), pending.paused_metrics);
        try std.testing.expectEqual(@as(usize, 0), pending.queued_builds);
        try std.testing.expectEqual(@as(usize, 0), pending.active_builds);
        try std.testing.expectEqual(@as(usize, 0), pending.active_pages);
        try std.testing.expect(!pending.hasWork());
    }

    const planned_while_paused = try db.runGraphMetricPlannedMaintenanceForIdle(.{
        .worker_id = "paused-degree-worker",
        .max_rounds = 4,
        .max_metrics_per_round = 8,
        .max_pages_per_round = 4,
    });
    try std.testing.expect(!planned_while_paused.durableProgressed());
    try std.testing.expectEqual(@as(usize, 0), planned_while_paused.builds_started);
    try std.testing.expectEqual(@as(usize, 0), planned_while_paused.worker_steps);
    try std.testing.expectEqual(@as(usize, 0), planned_while_paused.coordinator_steps);
    try std.testing.expectEqual(@as(usize, 0), planned_while_paused.pages_completed);
    try std.testing.expectEqual(@as(usize, 0), planned_while_paused.published);

    try db.runUntilIdle();

    {
        const graph_entry = db.core.graphIndex("graph_idx") orelse return error.IndexNotFound;
        var status = try graph_entry.index.graphMetricStatus("degree");
        defer status.deinit(alloc);
        try std.testing.expect(status.maintenance_paused);
        try std.testing.expectEqual(graph_mod.GraphIndex.GraphMetricState.stale, status.state);
        try std.testing.expectEqual(first_generation, status.published_generation);

        const top = try graph_entry.index.graphMetricTopK("degree", 10);
        defer {
            for (top) |*score| score.deinit(alloc);
            alloc.free(top);
        }
        try std.testing.expectEqual(@as(usize, 2), top.len);
        for (top) |score| try std.testing.expect(!std.mem.eql(u8, score.node, "doc:c"));
    }

    var refreshed_while_paused = try db.refreshGraphMetric(alloc, "graph_idx", "degree");
    defer refreshed_while_paused.deinit(alloc);
    try std.testing.expectEqual(graph_mod.GraphIndex.GraphMetricState.fresh, refreshed_while_paused.state);
    try std.testing.expect(refreshed_while_paused.maintenance_paused);
    try std.testing.expect(refreshed_while_paused.published_generation > first_generation);

    {
        const graph_entry = db.core.graphIndex("graph_idx") orelse return error.IndexNotFound;
        const top = try graph_entry.index.graphMetricTopK("degree", 1);
        defer {
            for (top) |*score| score.deinit(alloc);
            alloc.free(top);
        }
        try std.testing.expectEqual(@as(usize, 1), top.len);
        try std.testing.expectEqualStrings("doc:b", top[0].node);
        try std.testing.expectApproxEqAbs(@as(f64, 2.0), top[0].score, 0.001);
    }

    var resumed = try db.resumeGraphMetricMaintenance(alloc, "graph_idx", "degree");
    defer resumed.deinit(alloc);
    try std.testing.expectEqual(graph_mod.GraphIndex.GraphMetricState.fresh, resumed.state);
    try std.testing.expect(!resumed.maintenance_paused);

    const resumed_generation = resumed.published_generation;
    try db.batch(.{
        .writes = &.{
            .{ .key = "doc:d", .value = "{\"title\":\"delta\",\"_edges\":{\"graph_idx\":{\"cites\":[{\"target\":\"doc:b\",\"weight\":1.0}]}}}" },
        },
        .sync_level = .write,
    });

    try db.runUntilIdle();

    {
        const graph_entry = db.core.graphIndex("graph_idx") orelse return error.IndexNotFound;
        var status = try graph_entry.index.graphMetricStatus("degree");
        defer status.deinit(alloc);
        try std.testing.expect(!status.maintenance_paused);
        try std.testing.expectEqual(graph_mod.GraphIndex.GraphMetricState.fresh, status.state);
        try std.testing.expect(status.published_generation > resumed_generation);

        const top = try graph_entry.index.graphMetricTopK("degree", 1);
        defer {
            for (top) |*score| score.deinit(alloc);
            alloc.free(top);
        }
        try std.testing.expectEqual(@as(usize, 1), top.len);
        try std.testing.expectEqualStrings("doc:b", top[0].node);
        try std.testing.expectApproxEqAbs(@as(f64, 3.0), top[0].score, 0.001);
    }
}

test "db runUntilIdle drains lazy dense posting maintenance" {
    const alloc = std.testing.allocator;

    var path_buf: [256]u8 = undefined;
    const path = tempPath(&path_buf);
    defer cleanupTempDir(path);

    var db = try DB.open(alloc, std.mem.span(path), .{});
    defer db.close();

    try db.addIndex(.{
        .name = "dv_v1",
        .kind = .dense_vector,
        .config_json = "{\"field\":\"embedding\",\"dims\":2,\"metric\":\"l2_squared\",\"use_quantization\":false,\"lazy_posting_maintenance\":true,\"auto_posting_maintenance_max_postings\":0}",
    });

    try db.batch(.{
        .writes = &.{
            .{ .key = "doc:a", .value = "{\"embedding\":[1.0,0.0]}" },
            .{ .key = "doc:b", .value = "{\"embedding\":[3.0,0.0]}" },
        },
        .sync_level = .full_index,
    });

    {
        const entry = db.core.denseIndex("dv_v1") orelse return error.IndexNotFound;
        var txn = try entry.index.beginWriteTxn();
        errdefer txn.abort();
        var root = try entry.index.loadNode(&txn, entry.index.metadata.root_node);
        defer root.deinit(alloc);
        try root.ensureUnbacked(alloc);
        root.posting_state.noteMembersChanged(root.members.len);
        try entry.index.saveNode(&txn, &root);
        try entry.index.finishWriteTxn(&txn);
        entry.index.invalidateNodeCache(root.id);
    }

    {
        const stats = try db.diagnosticStats(alloc);
        defer types.freeDBStats(alloc, stats);
        try std.testing.expectEqual(@as(u64, 1), stats.indexes[0].hbc_posting.dirty_postings);
    }

    try db.runUntilIdle();

    {
        const stats = try db.diagnosticStats(alloc);
        defer types.freeDBStats(alloc, stats);
        try std.testing.expectEqual(@as(u64, 0), stats.indexes[0].hbc_posting.dirty_postings);
        try std.testing.expect(stats.indexes[0].hbc_posting.maintenance_repaired_postings > 0);
    }
}

test "db default full text index searches template chunk text when chunker full text indexing is enabled" {
    const alloc = std.testing.allocator;

    var path_buf: [256]u8 = undefined;
    const path = tempPath(&path_buf);
    defer cleanupTempDir(path);

    var deterministic = embedder_mod.DeterministicDenseEmbedder{};
    var db = try DB.open(alloc, std.mem.span(path), .{
        .enrichment = .{
            .owner_id = "worker-a",
            .dense_embedder = deterministic.interface(),
        },
        .start_index_workers = false,
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

    var result = try waitForSearchResult(alloc, &db, .{
        .index_name = "full_text_index_v0",
        .full_text = .{ .match = .{ .field = "body", .text = "routing" } },
        .return_mode = .parent,
    }, 1);
    defer result.deinit();

    try std.testing.expectEqual(@as(u32, 1), result.total_hits);
    try std.testing.expectEqualStrings("doc:a", result.hits[0].id);
}

test "db full_text sync level covers template chunk full text routing" {
    const alloc = std.testing.allocator;

    var path_buf: [256]u8 = undefined;
    const path = tempPath(&path_buf);
    defer cleanupTempDir(path);

    var deterministic = embedder_mod.DeterministicDenseEmbedder{};
    var db = try DB.open(alloc, std.mem.span(path), .{
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
    try std.testing.expectEqual(pending.enrichment.target_sequence, pending.enrichment.applied_sequence);
    try std.testing.expect(pending.enrichment.applied_sequence >= 1);

    var result = try db.search(alloc, .{
        .index_name = "full_text_index_v0",
        .full_text = .{ .match = .{ .field = "body", .text = "routing" } },
        .return_mode = .parent,
    });
    defer result.deinit();

    try std.testing.expectEqual(@as(u32, 1), result.total_hits);
    try std.testing.expectEqualStrings("doc:a", result.hits[0].id);
}

test "collectManagedSyncTargets includes graph index for graph artifact journal changes" {
    const alloc = std.testing.allocator;

    var path_buf: [256]u8 = undefined;
    const path = tempPath(&path_buf);
    defer cleanupTempDir(path);

    var db = try DB.open(alloc, std.mem.span(path), .{});
    defer db.close();

    try db.addIndex(.{
        .name = "gr_v1",
        .kind = .graph,
        .config_json = "{}",
    });

    const artifact_key = try internal_keys.graphEdgeArtifactKeyAlloc(alloc, "doc:a", "gr_v1", "links", "doc:b");

    var batch = derived_types.DerivedBatch{
        .changed_artifact_keys = try alloc.dupe([]const u8, &.{artifact_key}),
    };
    defer derived_types.deinitDerivedBatch(alloc, &batch);

    var sync_targets = try db.derivedAsyncCollectManagedSyncTargets(alloc, batch);
    defer sync_targets.deinit(alloc);

    try std.testing.expectEqual(@as(usize, 1), sync_targets.all_indexes.len);
    try std.testing.expectEqualStrings("gr_v1", sync_targets.all_indexes[0]);
}

test "db full_index supports dense parent search for template chunked embeddings" {
    const alloc = std.testing.allocator;

    var path_buf: [256]u8 = undefined;
    const path = tempPath(&path_buf);
    defer cleanupTempDir(path);

    var deterministic = embedder_mod.DeterministicDenseEmbedder{};
    var db = try DB.open(alloc, std.mem.span(path), .{
        .enrichment = .{
            .owner_id = "worker-a",
            .dense_embedder = deterministic.interface(),
        },
    });
    defer db.close();

    try db.addIndex(.{
        .name = "semantic_template_chunked_idx",
        .kind = .dense_vector,
        .config_json = "{\"field\":\"embedding\",\"dims\":3,\"generator\":{\"kind\":\"dense_embedding\",\"source_field\":\"body\",\"source_template\":\"{{title}}\",\"artifact_name\":\"semantic_template_chunked_idx_chunks\",\"chunker\":{\"provider\":\"antfly\",\"store_chunks\":false,\"full_text_index\":{},\"text\":{\"target_tokens\":4,\"overlap_tokens\":1,\"separator\":\" \"}}}}",
    });

    try db.batch(.{
        .writes = &.{
            .{ .key = "doc:a", .value = "{\"title\":\"alpha routing only in template chunks\",\"body\":\"body text without the keyword\"}" },
        },
        .sync_level = .full_index,
    });

    const query_vec = try deterministic.interface().embedDense(alloc, "", "alpha routing only in template chunks", 3);
    defer alloc.free(query_vec);

    var result = try db.search(alloc, .{
        .index_name = "semantic_template_chunked_idx",
        .dense = .{ .vector = query_vec, .k = 5 },
        .return_mode = .parent,
    });
    defer result.deinit();

    try std.testing.expectEqual(@as(u32, 1), result.total_hits);
    try std.testing.expectEqualStrings("doc:a", result.hits[0].id);
}

test "db full_index supports dense parent search when chunk artifacts are ephemeral" {
    const alloc = std.testing.allocator;

    var path_buf: [256]u8 = undefined;
    const path = tempPath(&path_buf);
    defer cleanupTempDir(path);

    var deterministic = embedder_mod.DeterministicDenseEmbedder{};
    var db = try DB.open(alloc, std.mem.span(path), .{
        .enrichment = .{
            .owner_id = "worker-a",
            .dense_embedder = deterministic.interface(),
        },
    });
    defer db.close();

    try db.addIndex(.{
        .name = "semantic_template_chunked_idx",
        .kind = .dense_vector,
        .config_json = "{\"field\":\"embedding\",\"dims\":3,\"generator\":{\"kind\":\"dense_embedding\",\"source_field\":\"body\",\"source_template\":\"{{title}}\",\"artifact_name\":\"semantic_template_chunked_idx_chunks\",\"chunker\":{\"provider\":\"antfly\",\"store_chunks\":false,\"text\":{\"target_tokens\":4,\"overlap_tokens\":1,\"separator\":\" \"}}}}",
    });

    try db.batch(.{
        .writes = &.{
            .{ .key = "doc:a", .value = "{\"title\":\"alpha routing only in template chunks\",\"body\":\"body text without the keyword\"}" },
        },
        .sync_level = .full_index,
    });

    const chunk_prefix = try internal_keys.artifactNamedPrefixAlloc(alloc, "doc:a", "chunk", "semantic_template_chunked_idx_chunks");
    defer alloc.free(chunk_prefix);
    const chunk_records = try db.core.store.scanPrefix(alloc, chunk_prefix);
    defer docstore_mod.DocStore.freeResults(alloc, chunk_records);
    var chunk_count: usize = 0;
    for (chunk_records) |entry| {
        if (internal_keys.isChunkArtifactRecordKey(entry.key)) chunk_count += 1;
    }
    try std.testing.expectEqual(@as(usize, 0), chunk_count);

    const query_vec = try deterministic.interface().embedDense(alloc, "", "alpha routing only in template chunks", 3);
    defer alloc.free(query_vec);

    var result = try db.search(alloc, .{
        .index_name = "semantic_template_chunked_idx",
        .dense = .{ .vector = query_vec, .k = 5 },
        .return_mode = .parent,
    });
    defer result.deinit();

    try std.testing.expectEqual(@as(u32, 1), result.total_hits);
    try std.testing.expectEqualStrings("doc:a", result.hits[0].id);
}

test "db reopened full_index supports dense parent search when chunk artifacts are ephemeral" {
    const alloc = std.testing.allocator;

    var path_buf: [256]u8 = undefined;
    const path = tempPath(&path_buf);
    defer cleanupTempDir(path);

    var deterministic = embedder_mod.DeterministicDenseEmbedder{};
    {
        var db = try DB.open(alloc, std.mem.span(path), .{
            .enrichment = .{
                .owner_id = "worker-a",
                .dense_embedder = deterministic.interface(),
            },
        });
        defer db.close();

        try db.addIndex(.{
            .name = "semantic_template_chunked_idx",
            .kind = .dense_vector,
            .config_json = "{\"field\":\"embedding\",\"dims\":3,\"generator\":{\"kind\":\"dense_embedding\",\"source_field\":\"body\",\"source_template\":\"{{title}}\",\"artifact_name\":\"semantic_template_chunked_idx_chunks\",\"chunker\":{\"provider\":\"antfly\",\"store_chunks\":false,\"text\":{\"target_tokens\":4,\"overlap_tokens\":1,\"separator\":\" \"}}}}",
        });

        try db.batch(.{
            .writes = &.{
                .{ .key = "doc:a", .value = "{\"title\":\"alpha routing only in template chunks\",\"body\":\"body text without the keyword\"}" },
            },
            .sync_level = .full_index,
        });
    }

    var reopened = try DB.open(alloc, std.mem.span(path), .{});
    defer reopened.close();

    const query_vec = try deterministic.interface().embedDense(alloc, "", "alpha routing only in template chunks", 3);
    defer alloc.free(query_vec);

    var result = try reopened.search(alloc, .{
        .index_name = "semantic_template_chunked_idx",
        .dense = .{ .vector = query_vec, .k = 5 },
        .return_mode = .parent,
    });
    defer result.deinit();

    try std.testing.expectEqual(@as(u32, 1), result.total_hits);
    try std.testing.expectEqualStrings("doc:a", result.hits[0].id);
}

test "db addEnrichment supports explicit shared definitions" {
    const alloc = std.testing.allocator;

    var path_buf: [256]u8 = undefined;
    const path = tempPath(&path_buf);
    defer cleanupTempDir(path);

    var deterministic = embedder_mod.DeterministicDenseEmbedder{};
    var db = try DB.open(alloc, std.mem.span(path), .{
        .enrichment = .{
            .owner_id = "worker-a",
            .dense_embedder = deterministic.interface(),
        },
    });
    defer db.close();

    try db.addEnrichment(.{
        .name = "body_chunks_v1",
        .kind = .chunk,
        .field = "body",
        .chunk_size = 8,
        .chunk_overlap = 2,
    });
    try db.addEnrichment(.{
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

    try db.enrichment_runtime.?.waitForApplied(1);
    try db.runDerivedUntil(db.core.nextDerivedSequence());

    const query_vec = try deterministic.interface().embedDense(alloc, "", "abcdefgh", 3);
    defer alloc.free(query_vec);

    const req: types.SearchRequest = .{
        .index_name = "dv_ref",
        .dense = .{ .vector = query_vec, .k = 3 },
        .return_mode = .chunk,
    };
    var dense_result = try waitForDenseSearchResult(alloc, &db, req, 1);
    dense_result.deinit();

    var result = try waitForSearchResult(alloc, &db, req, 1);
    defer result.deinit();
    const chunk_zero = try artifact_ids.chunkArtifactPublicIdAlloc(alloc, "doc:a", "body_chunks_v1", 0);
    defer alloc.free(chunk_zero);
    try std.testing.expectEqualStrings(chunk_zero, result.hits[0].id);

    const chunk = (try db.getEnrichment(alloc, .chunk, "body_chunks_v1")) orelse return error.TestUnexpectedResult;
    defer {
        var tmp = chunk;
        tmp.deinit(alloc);
    }
    try std.testing.expectEqualStrings("body", chunk.field);

    const embedding = (try db.getEnrichment(alloc, .embedding, "chunk_dense_v1")) orelse return error.TestUnexpectedResult;
    defer {
        var tmp = embedding;
        tmp.deinit(alloc);
    }
    try std.testing.expectEqualStrings("body_chunks_v1", embedding.source_artifact_name);

    const enrichments = try db.listEnrichments(alloc);
    defer types.freeEnrichmentConfigs(alloc, enrichments);
    try std.testing.expectEqual(@as(usize, 2), enrichments.len);
}

test "db index inspection lists graph indexes" {
    const alloc = std.testing.allocator;

    var path_buf: [256]u8 = undefined;
    const path = tempPath(&path_buf);
    defer cleanupTempDir(path);

    var db = try DB.open(alloc, std.mem.span(path), .{});
    defer db.close();

    try db.addIndex(.{
        .name = "graph_v1",
        .kind = .graph,
        .config_json = "{}",
    });

    try std.testing.expect(db.hasIndex("graph_v1"));
    try std.testing.expect(!db.hasIndex("missing_graph"));

    const indexes = try db.listIndexes(alloc);
    defer types.freeIndexConfigs(alloc, indexes);
    try std.testing.expectEqual(@as(usize, 1), indexes.len);
    try std.testing.expectEqualStrings("graph_v1", indexes[0].name);
    try std.testing.expectEqual(types.IndexKind.graph, indexes[0].kind);
}

test "db deleteEnrichment rejects referenced definitions" {
    const alloc = std.testing.allocator;

    var path_buf: [256]u8 = undefined;
    const path = tempPath(&path_buf);
    defer cleanupTempDir(path);

    var db = try DB.open(alloc, std.mem.span(path), .{});
    defer db.close();

    try db.addEnrichment(.{
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

    try std.testing.expectError(error.EnrichmentInUse, db.deleteEnrichment(.chunk, "body_chunks_v1"));
}

test "db deleteEnrichment rejects asset referenced by chunk enrichment" {
    const alloc = std.testing.allocator;

    var path_buf: [256]u8 = undefined;
    const path = tempPath(&path_buf);
    defer cleanupTempDir(path);

    var db = try DB.open(alloc, std.mem.span(path), .{});
    defer db.close();

    try db.addEnrichment(.{
        .name = "document_units_v1",
        .kind = .asset,
        .field = "url",
        .content_type = "application/json",
    });
    try db.addEnrichment(.{
        .name = "document_chunks_v1",
        .kind = .chunk,
        .source_artifact_name = "document_units_v1",
        .field = "text",
        .chunk_size = 512,
    });

    try std.testing.expectError(error.EnrichmentInUse, db.deleteEnrichment(.asset, "document_units_v1"));
}

test "db upsertEnrichment rejects replacing referenced asset with chunk" {
    const alloc = std.testing.allocator;

    var path_buf: [256]u8 = undefined;
    const path = tempPath(&path_buf);
    defer cleanupTempDir(path);

    var db = try DB.open(alloc, std.mem.span(path), .{});
    defer db.close();

    try db.addEnrichment(.{
        .name = "document_units_v1",
        .kind = .asset,
        .field = "url",
        .content_type = "application/json",
    });
    try db.addEnrichment(.{
        .name = "document_chunks_v1",
        .kind = .chunk,
        .source_artifact_name = "document_units_v1",
        .field = "text",
        .chunk_size = 512,
    });

    try std.testing.expectError(error.InvalidEnrichmentConfig, db.upsertEnrichment(.{
        .name = "document_units_v1",
        .kind = .chunk,
        .source_artifact_name = "document_units_v1",
        .field = "text",
        .chunk_size = 512,
    }));

    var asset = (try db.getEnrichment(alloc, .asset, "document_units_v1")).?;
    defer asset.deinit(alloc);
    try std.testing.expectEqual(types.EnrichmentKind.asset, asset.kind);
}

test "db upsertEnrichment rejects replacing referenced chunk with asset" {
    const alloc = std.testing.allocator;

    var path_buf: [256]u8 = undefined;
    const path = tempPath(&path_buf);
    defer cleanupTempDir(path);

    var db = try DB.open(alloc, std.mem.span(path), .{});
    defer db.close();

    try db.addEnrichment(.{
        .name = "document_chunks_v1",
        .kind = .chunk,
        .field = "body",
        .chunk_size = 512,
    });
    try db.addEnrichment(.{
        .name = "document_dense_v1",
        .kind = .embedding,
        .source_artifact_name = "document_chunks_v1",
        .field = "text",
        .expected_dims = 3,
    });

    try std.testing.expectError(error.InvalidEnrichmentConfig, db.upsertEnrichment(.{
        .name = "document_chunks_v1",
        .kind = .asset,
        .field = "url",
        .content_type = "application/json",
    }));

    var chunk = (try db.getEnrichment(alloc, .chunk, "document_chunks_v1")).?;
    defer chunk.deinit(alloc);
    try std.testing.expectEqual(types.EnrichmentKind.chunk, chunk.kind);
}

test "db upsertEnrichment rejects replacing indexed chunk with asset" {
    const alloc = std.testing.allocator;

    var path_buf: [256]u8 = undefined;
    const path = tempPath(&path_buf);
    defer cleanupTempDir(path);

    var db = try DB.open(alloc, std.mem.span(path), .{});
    defer db.close();

    try db.addEnrichment(.{
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

    try std.testing.expectError(error.InvalidEnrichmentConfig, db.upsertEnrichment(.{
        .name = "body_chunks_v1",
        .kind = .asset,
        .field = "url",
        .content_type = "application/json",
    }));

    var chunk = (try db.getEnrichment(alloc, .chunk, "body_chunks_v1")).?;
    defer chunk.deinit(alloc);
    try std.testing.expectEqual(types.EnrichmentKind.chunk, chunk.kind);
}

test "db addEnrichment rejects duplicate names across enrichment kinds" {
    const alloc = std.testing.allocator;

    var path_buf: [256]u8 = undefined;
    const path = tempPath(&path_buf);
    defer cleanupTempDir(path);

    var db = try DB.open(alloc, std.mem.span(path), .{});
    defer db.close();

    try db.addEnrichment(.{
        .name = "document_artifact_v1",
        .kind = .asset,
        .field = "url",
        .content_type = "application/json",
    });
    try std.testing.expectError(error.EnrichmentAlreadyExists, db.addEnrichment(.{
        .name = "document_artifact_v1",
        .kind = .chunk,
        .field = "body",
        .chunk_size = 512,
    }));
}

test "db addEnrichment allows unrelated definitions after field sparse index" {
    const alloc = std.testing.allocator;

    var path_buf: [256]u8 = undefined;
    const path = tempPath(&path_buf);
    defer cleanupTempDir(path);

    var db = try DB.open(alloc, std.mem.span(path), .{});
    defer db.close();

    try db.addIndex(.{
        .name = "sp_v1",
        .kind = .sparse_vector,
        .config_json = "{\"field\":\"sparse\"}",
    });
    try db.addEnrichment(.{
        .name = "document_units_v1",
        .kind = .asset,
        .field = "url",
        .content_type = "application/json",
    });
}

test "db full-text chunk consumer returns parent and chunk modes" {
    const alloc = std.testing.allocator;

    var path_buf: [256]u8 = undefined;
    const path = tempPath(&path_buf);
    defer cleanupTempDir(path);

    var deterministic = embedder_mod.DeterministicDenseEmbedder{};
    var db = try DB.open(alloc, std.mem.span(path), .{
        .enrichment = .{
            .owner_id = "worker-a",
            .dense_embedder = deterministic.interface(),
        },
        .start_index_workers = false,
    });
    defer db.close();

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

    try db.batch(.{
        .writes = &.{
            .{ .key = "doc:a", .value = "{\"body\":\"abcdefghijklmno\"}" },
        },
        .sync_level = .full_text,
    });

    const chunk_prefix = try internal_keys.artifactNamedPrefixAlloc(alloc, "doc:a", "chunk", "body_chunks_v1");
    defer alloc.free(chunk_prefix);
    const chunk_records = try db.core.store.scanPrefix(alloc, chunk_prefix);
    defer docstore_mod.DocStore.freeResults(alloc, chunk_records);
    try std.testing.expect(chunk_records.len > 0);
    try std.testing.expect(db.core.index_manager.textIndex("ft_chunks").?.snapshot().global_doc_count > 0);

    var chunk_result = try waitForSearchResult(alloc, &db, .{
        .index_name = "ft_chunks",
        .full_text = .{ .match = .{ .field = "body", .text = "abcdefgh" } },
        .return_mode = .chunk,
    }, 1);
    defer chunk_result.deinit();

    try std.testing.expectEqual(@as(u32, 1), chunk_result.total_hits);
    const chunk_zero = try artifact_ids.chunkArtifactPublicIdAlloc(alloc, "doc:a", "body_chunks_v1", 0);
    defer alloc.free(chunk_zero);
    try std.testing.expectEqualStrings(chunk_zero, chunk_result.hits[0].id);
    const chunk_ref = chunk_result.hits[0].artifact_ref orelse return error.TestUnexpectedResult;
    try std.testing.expectEqual(types.ArtifactKind.chunk, chunk_ref.kind);
    try std.testing.expectEqualStrings("doc:a", chunk_ref.document_id);
    try std.testing.expectEqualStrings("body_chunks_v1", chunk_ref.name);
    try std.testing.expectEqual(@as(?u32, 0), chunk_ref.chunk_id);

    var parent_result = try db.search(alloc, .{
        .index_name = "ft_chunks",
        .full_text = .{ .match = .{ .field = "body", .text = "abcdefgh" } },
        .return_mode = .parent,
    });
    defer parent_result.deinit();

    try std.testing.expectEqual(@as(u32, 1), parent_result.total_hits);
    try std.testing.expectEqualStrings("doc:a", parent_result.hits[0].id);
}

test "db full-text chunk consumer filters expired parents under ttl" {
    const alloc = std.testing.allocator;

    var path_buf: [256]u8 = undefined;
    const path = tempPath(&path_buf);
    defer cleanupTempDir(path);

    var deterministic = embedder_mod.DeterministicDenseEmbedder{};
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

    const now_ns = currentTimeNs();
    try db.batch(.{
        .writes = &.{.{ .key = "doc:old", .value = "{\"body\":\"abcdefghijklmno\"}" }},
        .timestamp_ns = now_ns - 2 * ttl_duration_ns,
        .sync_level = .full_text,
    });
    try db.batch(.{
        .writes = &.{.{ .key = "doc:fresh", .value = "{\"body\":\"abcdefghijklmno\"}" }},
        .timestamp_ns = now_ns,
        .sync_level = .full_text,
    });

    var chunk_result = try waitForSearchResult(alloc, &db, .{
        .index_name = "ft_chunks",
        .full_text = .{ .match = .{ .field = "body", .text = "abcdefgh" } },
        .return_mode = .chunk,
    }, 1);
    defer chunk_result.deinit();
    try std.testing.expectEqual(@as(u32, 1), chunk_result.total_hits);
    const fresh_chunk_zero = try artifact_ids.chunkArtifactPublicIdAlloc(alloc, "doc:fresh", "body_chunks_v1", 0);
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

test "db getArtifact loads stored chunk artifacts by public id" {
    const alloc = std.testing.allocator;

    var path_buf: [256]u8 = undefined;
    const path = tempPath(&path_buf);
    defer cleanupTempDir(path);

    var db = try DB.open(alloc, std.mem.span(path), .{});
    defer db.close();

    const chunk_zero = try artifact_ids.chunkArtifactPublicIdAlloc(alloc, "doc:a", "body_chunks_v1", 0);
    defer alloc.free(chunk_zero);
    const internal_key = try internal_keys.chunkArtifactKeyAlloc(alloc, "doc:a", "body_chunks_v1", 0);
    defer alloc.free(internal_key);
    try db.core.store.put(internal_key, "{\"body\":\"abcdefgh\",\"_artifact_name\":\"body_chunks_v1\",\"_chunk_id\":0}");

    var artifact = (try db.getArtifact(alloc, chunk_zero)) orelse return error.TestUnexpectedResult;
    defer artifact.deinit(alloc);

    try std.testing.expectEqualStrings(chunk_zero, artifact.id);
    try std.testing.expectEqual(types.ArtifactKind.chunk, artifact.artifact_ref.kind);
    try std.testing.expectEqualStrings("doc:a", artifact.artifact_ref.document_id);
    try std.testing.expectEqualStrings("body_chunks_v1", artifact.artifact_ref.name);
    try std.testing.expectEqual(@as(?u32, 0), artifact.artifact_ref.chunk_id);
    try std.testing.expect(std.mem.indexOf(u8, artifact.value, "\"_chunk_id\":0") != null);
    try std.testing.expect(std.mem.indexOf(u8, artifact.value, "\"body\":\"abcdefgh\"") != null);
}

test "db group created-at metadata is written once and readable" {
    const alloc = std.testing.allocator;

    var path_buf: [256]u8 = undefined;
    const path = tempPath(&path_buf);
    defer cleanupTempDir(path);

    var db = try DB.open(alloc, std.mem.span(path), .{});
    defer db.close();

    try std.testing.expect((try db.getGroupCreatedAtMillis(alloc, 42)) == null);
    try std.testing.expectEqual(@as(u64, 1234), try db.ensureGroupCreatedAtMillis(alloc, 42, 1234));
    try std.testing.expectEqual(@as(u64, 1234), (try db.getGroupCreatedAtMillis(alloc, 42)).?);
    try std.testing.expectEqual(@as(u64, 1234), try db.ensureGroupCreatedAtMillis(alloc, 42, 5678));
}

test "db full-text chunk parent paging applies after grouping" {
    const alloc = std.testing.allocator;

    var path_buf: [256]u8 = undefined;
    const path = tempPath(&path_buf);
    defer cleanupTempDir(path);

    var deterministic = embedder_mod.DeterministicDenseEmbedder{};
    var db = try DB.open(alloc, std.mem.span(path), .{
        .enrichment = .{
            .owner_id = "worker-a",
            .dense_embedder = deterministic.interface(),
        },
        .start_index_workers = false,
    });
    defer db.close();

    try db.addIndex(.{
        .name = "ft_chunks",
        .kind = .full_text,
        .config_json = "{\"chunk_name\":\"body_chunks_v1\"}",
    });
    try db.addIndex(.{
        .name = "dv_v1",
        .kind = .dense_vector,
        .config_json = "{\"field\":\"embedding\",\"dims\":3,\"generator\":{\"kind\":\"dense_embedding\",\"source_field\":\"body\",\"chunk_name\":\"body_chunks_v1\",\"chunk_size\":32,\"chunk_overlap\":0}}",
    });

    try db.batch(.{
        .writes = &.{
            .{ .key = "doc:a", .value = "{\"body\":\"alpha alpha alpha alpha\"}" },
            .{ .key = "doc:b", .value = "{\"body\":\"alpha\"}" },
        },
        .sync_level = .full_text,
    });

    var result = try waitForSearchResult(alloc, &db, .{
        .index_name = "ft_chunks",
        .full_text = .{ .term = .{ .field = "body", .term = "alpha" } },
        .return_mode = .parent,
        .limit = 1,
        .offset = 1,
    }, 1);
    defer result.deinit();

    try std.testing.expectEqual(@as(u32, 2), result.total_hits);
    try std.testing.expectEqual(@as(usize, 1), result.hits.len);
    try std.testing.expectEqualStrings("doc:b", result.hits[0].id);
}

test "db dense chunk consumer supports parent and parent_with_chunks modes" {
    const alloc = std.testing.allocator;

    var path_buf: [256]u8 = undefined;
    const path = tempPath(&path_buf);
    defer cleanupTempDir(path);

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
    try db.runDerivedUntil(db.core.nextDerivedSequence());
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
        .search_effort = 1.0,
    }, 1);
    defer chunk_result.deinit();
    const chunk_zero = try artifact_ids.chunkArtifactPublicIdAlloc(alloc, "doc:a", "body_chunks_v1", 0);
    defer alloc.free(chunk_zero);
    const chunk_one = try artifact_ids.chunkArtifactPublicIdAlloc(alloc, "doc:a", "body_chunks_v1", 1);
    defer alloc.free(chunk_one);
    const chunk_two = try artifact_ids.chunkArtifactPublicIdAlloc(alloc, "doc:a", "body_chunks_v1", 2);
    defer alloc.free(chunk_two);
    try std.testing.expectEqual(@as(u32, 3), chunk_result.total_hits);
    try std.testing.expect(
        std.mem.eql(u8, chunk_result.hits[0].id, chunk_zero) or
            std.mem.eql(u8, chunk_result.hits[0].id, chunk_one) or
            std.mem.eql(u8, chunk_result.hits[0].id, chunk_two),
    );

    var include = try db.internalResolveDocSetForIdsAlloc(alloc, &.{"doc:a"});
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
        .search_effort = 1.0,
    }, .{ .vector = query_vec, .k = 3 });
    defer filtered_chunk_result.result.deinit();
    try std.testing.expect(filtered_chunk_result.profile.raw_hit_count >= 2);
    try std.testing.expectEqual(@as(u32, 1), filtered_chunk_result.result.total_hits);
    try std.testing.expectEqualStrings("doc:a", filtered_chunk_result.result.hits[0].id);

    var parent_result = try db.search(alloc, .{
        .index_name = "dv_v1",
        .dense = .{ .vector = query_vec, .k = 3 },
        .return_mode = .parent,
        .search_effort = 1.0,
    });
    defer parent_result.deinit();
    try std.testing.expectEqual(@as(u32, 1), parent_result.total_hits);
    try std.testing.expectEqualStrings("doc:a", parent_result.hits[0].id);

    var parent_with_chunks = try waitForSearchResult(alloc, &db, .{
        .index_name = "dv_v1",
        .dense = .{ .vector = query_vec, .k = 3 },
        .return_mode = .parent_with_chunks,
        .max_chunks_per_parent = 1,
        .search_effort = 1.0,
    }, 1);
    defer parent_with_chunks.deinit();
    try std.testing.expectEqual(@as(u32, 1), parent_with_chunks.total_hits);
    try std.testing.expectEqualStrings("doc:a", parent_with_chunks.hits[0].id);
    try std.testing.expectEqual(@as(usize, 1), parent_with_chunks.hits[0].chunk_hits.len);
    try std.testing.expect(
        std.mem.eql(u8, parent_with_chunks.hits[0].chunk_hits[0].id, chunk_zero) or
            std.mem.eql(u8, parent_with_chunks.hits[0].chunk_hits[0].id, chunk_one) or
            std.mem.eql(u8, parent_with_chunks.hits[0].chunk_hits[0].id, chunk_two),
    );

    const doc_a_store_key = try internal_keys.documentKeyAlloc(alloc, "doc:a");
    defer alloc.free(doc_a_store_key);
    {
        var txn = try db.core.store.beginWriteTxn();
        errdefer txn.abort();
        try txn.delete(doc_a_store_key);
        try doc_identity.markDeletedTxn(alloc, &txn, 2, "doc:a");
        try txn.commit();
    }
    db.identity_visibility_summary_cache = null;

    var stale_chunk_result = try db.searchDenseProfiled(alloc, .{
        .index_name = "dv_v1",
        .limit = 3,
        .include_stored = false,
        .return_mode = .chunk,
        .search_effort = 1.0,
    }, .{ .vector = query_vec, .k = 3 });
    defer stale_chunk_result.result.deinit();
    try std.testing.expectEqual(@as(u32, 0), stale_chunk_result.result.total_hits);
    try std.testing.expectEqual(@as(u32, 0), stale_chunk_result.profile.raw_hit_count);

    var stale_parent_result = try db.search(alloc, .{
        .index_name = "dv_v1",
        .dense = .{ .vector = query_vec, .k = 3 },
        .return_mode = .parent,
        .search_effort = 1.0,
    });
    defer stale_parent_result.deinit();
    try std.testing.expectEqual(@as(u32, 0), stale_parent_result.total_hits);
}

test "db dense chunk consumer supports parent and parent_with_chunks modes with durable lsm primary backend" {
    const alloc = std.testing.allocator;

    var path_buf: [256]u8 = undefined;
    const path = tempPath(&path_buf);
    defer cleanupTempDir(path);

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
        .config_json = "{\"field\":\"embedding\",\"dims\":3,\"metric\":\"l2_squared\",\"generator\":{\"kind\":\"dense_embedding\",\"source_field\":\"body\",\"chunk_name\":\"body_chunks_v1\",\"chunk_size\":8,\"chunk_overlap\":2}}",
    });

    try db.batch(.{
        .writes = &.{
            .{ .key = "doc:a", .value = "{\"body\":\"abcdefghijklmno\"}" },
        },
        .sync_level = .write,
    });

    try db.enrichment_runtime.?.waitForApplied(1);
    try db.runDerivedUntil(db.core.nextDerivedSequence());
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
        .search_effort = 1.0,
    }, 1);
    defer chunk_result.deinit();
    const chunk_zero = try artifact_ids.chunkArtifactPublicIdAlloc(alloc, "doc:a", "body_chunks_v1", 0);
    defer alloc.free(chunk_zero);
    const chunk_one = try artifact_ids.chunkArtifactPublicIdAlloc(alloc, "doc:a", "body_chunks_v1", 1);
    defer alloc.free(chunk_one);
    const chunk_two = try artifact_ids.chunkArtifactPublicIdAlloc(alloc, "doc:a", "body_chunks_v1", 2);
    defer alloc.free(chunk_two);
    try std.testing.expectEqual(@as(u32, 3), chunk_result.total_hits);
    try std.testing.expect(
        std.mem.eql(u8, chunk_result.hits[0].id, chunk_zero) or
            std.mem.eql(u8, chunk_result.hits[0].id, chunk_one) or
            std.mem.eql(u8, chunk_result.hits[0].id, chunk_two),
    );

    var parent_result = try db.search(alloc, .{
        .index_name = "dv_v1",
        .dense = .{ .vector = query_vec, .k = 3 },
        .return_mode = .parent,
        .search_effort = 1.0,
    });
    defer parent_result.deinit();
    try std.testing.expectEqual(@as(u32, 1), parent_result.total_hits);
    try std.testing.expectEqualStrings("doc:a", parent_result.hits[0].id);

    var parent_with_chunks = try waitForSearchResult(alloc, &db, .{
        .index_name = "dv_v1",
        .dense = .{ .vector = query_vec, .k = 3 },
        .return_mode = .parent_with_chunks,
        .max_chunks_per_parent = 1,
        .search_effort = 1.0,
    }, 1);
    defer parent_with_chunks.deinit();
    try std.testing.expectEqual(@as(u32, 1), parent_with_chunks.total_hits);
    try std.testing.expectEqualStrings("doc:a", parent_with_chunks.hits[0].id);
    try std.testing.expectEqual(@as(usize, 1), parent_with_chunks.hits[0].chunk_hits.len);
    try std.testing.expect(
        std.mem.eql(u8, parent_with_chunks.hits[0].chunk_hits[0].id, chunk_zero) or
            std.mem.eql(u8, parent_with_chunks.hits[0].chunk_hits[0].id, chunk_one) or
            std.mem.eql(u8, parent_with_chunks.hits[0].chunk_hits[0].id, chunk_two),
    );
}

test "db io_threaded executor stress applies explicit dense embeddings on lsm backend" {
    if (getenv("ANTFLY_STRESS_DB_DENSE_REPRO") == null) return error.SkipZigTest;

    const alloc = std.testing.allocator;
    const dims = index_manager_mod.stressEnvUsize("ANTFLY_STRESS_DENSE_DIMS", 256);
    const total_docs = index_manager_mod.stressEnvUsize("ANTFLY_STRESS_DENSE_DOCS", 4096);
    const batch_size = @max(@as(usize, 1), index_manager_mod.stressEnvUsize("ANTFLY_STRESS_DENSE_BATCH", 256));
    const progress_interval = @max(batch_size, index_manager_mod.stressEnvUsize("ANTFLY_STRESS_DENSE_PROGRESS", batch_size * 8));
    const dense_backend = stressDenseBackend();

    var path_buf: [256]u8 = undefined;
    const path = tempPath(&path_buf);
    defer cleanupTempDir(path);

    var db = try DB.open(alloc, std.mem.span(path), .{
        .primary_backend = .{ .lsm = .{ .flush_threshold = 1 } },
        .executor = .{ .backend = .io_threaded },
        .index_backends = .{
            .dense_storage_backend = dense_backend,
        },
    });
    defer db.close();

    const config_json = try std.fmt.allocPrint(alloc, "{{\"field\":\"embedding\",\"dims\":{d},\"metric\":\"l2_squared\"}}", .{dims});
    defer alloc.free(config_json);
    try db.addIndex(.{
        .name = "dv_v1",
        .kind = .dense_vector,
        .config_json = config_json,
    });

    var queued_docs: usize = 0;
    while (queued_docs < total_docs) {
        const end = @min(queued_docs + batch_size, total_docs);

        var writes = std.ArrayListUnmanaged(types.BatchWrite).empty;
        defer {
            for (writes.items) |write| {
                alloc.free(@constCast(write.key));
                alloc.free(@constCast(write.value));
            }
            writes.deinit(alloc);
        }

        for (queued_docs..end) |doc_index| {
            const doc_key = try std.fmt.allocPrint(alloc, "doc:{d:0>8}", .{doc_index});
            const doc_json = try allocStressDenseDocJson(alloc, dims, doc_index);
            try writes.append(alloc, .{
                .key = doc_key,
                .value = doc_json,
            });
        }

        try db.batch(.{
            .writes = writes.items,
            .sync_level = .write,
        });
        queued_docs = end;

        if (queued_docs % progress_interval == 0 or queued_docs == total_docs) {
            try db.runDerivedUntil(db.core.nextDerivedSequence());
            const entry = db.core.index_manager.denseIndex("dv_v1") orelse return error.IndexNotFound;
            try std.testing.expectEqual(@as(u64, @intCast(queued_docs)), entry.index.stats().active_count);
        }
    }

    try db.runDerivedUntil(db.core.nextDerivedSequence());
    try db.runUntilIdle();

    const entry = db.core.index_manager.denseIndex("dv_v1") orelse return error.IndexNotFound;
    try std.testing.expectEqual(@as(u64, @intCast(total_docs)), entry.index.stats().active_count);

    const first_vector_id = (try db.core.index_manager.lookupDenseVectorId(db.core.store, "dv_v1", "doc:00000000")) orelse return error.TestUnexpectedResult;
    try std.testing.expectEqual(index_manager_mod.deterministicDenseVectorId("doc:00000000"), first_vector_id);
    const first_doc = (try db.core.index_manager.lookupDenseDocKey(db.core.store, "dv_v1", first_vector_id)) orelse return error.TestUnexpectedResult;
    defer alloc.free(first_doc);
    try std.testing.expectEqualStrings("doc:00000000", first_doc);

    const expected_last_doc = try std.fmt.allocPrint(alloc, "doc:{d:0>8}", .{total_docs - 1});
    defer alloc.free(expected_last_doc);
    const last_vector_id = (try db.core.index_manager.lookupDenseVectorId(db.core.store, "dv_v1", expected_last_doc)) orelse return error.TestUnexpectedResult;
    try std.testing.expectEqual(index_manager_mod.deterministicDenseVectorId(expected_last_doc), last_vector_id);
    const last_doc = (try db.core.index_manager.lookupDenseDocKey(db.core.store, "dv_v1", last_vector_id)) orelse return error.TestUnexpectedResult;
    defer alloc.free(last_doc);
    try std.testing.expectEqualStrings(expected_last_doc, last_doc);

    var read_txn = try entry.index.beginReadTxn();
    defer read_txn.abort();
    const last_vector = try entry.index.getVector(&read_txn, last_vector_id);
    defer alloc.free(last_vector);

    const expected_last_vector = try alloc.alloc(f32, dims);
    defer alloc.free(expected_last_vector);
    index_manager_mod.fillStressDenseVector(expected_last_vector, total_docs - 1);
    try std.testing.expectEqualSlices(f32, expected_last_vector, last_vector);
}

test "db writes and reads timestamp metadata" {
    const alloc = std.testing.allocator;

    var path_buf: [256]u8 = undefined;
    const path = tempPath(&path_buf);
    defer cleanupTempDir(path);

    var db = try DB.open(alloc, std.mem.span(path), .{});
    defer db.close();

    try db.batch(.{
        .writes = &.{.{ .key = "doc:a", .value = "{\"title\":\"alpha\"}" }},
        .timestamp_ns = 1_700_000_000_000_000_000,
    });

    const ts = try db.getTimestamp(alloc, "doc:a");
    try std.testing.expectEqual(@as(u64, 1_700_000_000_000_000_000), ts);

    const first_doc = "doc\x00ttl";
    const second_doc = "doc\x00ttl:child";
    try db.batch(.{
        .writes = &.{
            .{ .key = first_doc, .value = "{\"title\":\"first\"}" },
            .{ .key = second_doc, .value = "{\"title\":\"second\"}" },
        },
        .timestamp_ns = 1_700_000_000_000_000_101,
    });

    try std.testing.expectEqual(@as(u64, 1_700_000_000_000_000_101), try db.getTimestamp(alloc, first_doc));
    try std.testing.expectEqual(@as(u64, 1_700_000_000_000_000_101), try db.getTimestamp(alloc, second_doc));

    try db.batch(.{ .deletes = &.{first_doc} });

    try std.testing.expectEqual(@as(u64, 0), try db.getTimestamp(alloc, first_doc));
    try std.testing.expectEqual(@as(u64, 1_700_000_000_000_000_101), try db.getTimestamp(alloc, second_doc));
}

test "db dense lsm cache profile benchmark" {
    if (!profileBenchTestsEnabled()) return error.SkipZigTest;
    const alloc = std.testing.allocator;

    var path_buf: [256]u8 = undefined;
    const path = tempPath(&path_buf);
    defer cleanupTempDir(path);

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
        const effort = db_query_search.resolvedSearchEffort(req.search_effort);

        const hbc_search_start = monotonicTimeNs();
        var dense_results = try profiled_entry.index.searchWithRequest(.{
            .query = req.dense.?.vector,
            .k = effective_k,
            .search_width = db_query_search.resolveSearchWidth(req.dense.?.k, effort, profiled_entry.index.stats()),
            .epsilon = db_query_search.resolveSearchEpsilon(effort),
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
                const loaded = try db.searchRuntimeProjectOwnedStoredBytesForSearch(
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
                .score = hit.distance,
                .stored_data = stored_data,
            });
        }

        const postprocess_start = monotonicTimeNs();
        var profiled_result = try db.searchRuntimePostprocessVectorSearchResult(
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

test "db batch load profile benchmark" {
    if (!profileBenchTestsEnabled()) return error.SkipZigTest;
    const alloc = std.testing.allocator;

    var path_buf: [256]u8 = undefined;
    const path = tempPath(&path_buf);
    defer cleanupTempDir(path);

    var db = try DB.open(alloc, std.mem.span(path), .{
        .primary_backend = .{ .lsm = .{ .flush_threshold = 1 } },
    });
    defer db.close();

    const dims: usize = 16;
    const batch_docs: usize = 32;
    const batch_count: usize = 1;
    const total_docs: usize = batch_docs * batch_count;

    try db.addIndex(.{
        .name = "dv_v1",
        .kind = .dense_vector,
        .config_json = "{\"field\":\"embedding\",\"dims\":16,\"metric\":\"cosine\",\"external\":true}",
    });

    const vector_buf = try alloc.alloc(f32, dims);
    defer alloc.free(vector_buf);

    var total_profile = BatchProfile{};
    for (0..batch_count) |batch_index| {
        const writes = try alloc.alloc(types.BatchWrite, batch_docs);
        defer {
            for (writes) |write| {
                alloc.free(write.key);
                alloc.free(write.value);
            }
            alloc.free(writes);
        }
        for (writes, 0..) |*write, doc_offset| {
            const doc_id = batch_index * batch_docs + doc_offset;
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
                    "{{\"title\":\"doc-{d}\",\"_embeddings\":{{\"dv_v1\":{f}}}}}",
                    .{ doc_id, std.json.fmt(vector_buf, .{}) },
                ),
            };
        }

        var batch_profile = BatchProfile{};
        try db.batchProfiled(.{
            .writes = writes,
            .sync_level = .full_index,
        }, &batch_profile);
        addBatchProfile(&total_profile, batch_profile);
    }

    const stats = try db.stats(alloc);
    defer types.freeDBStats(alloc, stats);
    try std.testing.expectEqual(@as(u64, total_docs), stats.indexes[0].doc_count);

    std.debug.print(
        "batch_load_profile docs={d} batch_docs={d} batches={d} dims={d} avg_total_ms={d} avg_resolve_transforms_ms={d} avg_merge_req_ms={d} avg_predicates_ms={d} avg_validate_range_ms={d} avg_extract_writes_ms={d} avg_delete_artifacts_ms={d} avg_precompute_generated_ms={d} avg_store_write_ms={d} avg_split_delta_ms={d} avg_build_derived_ms={d} avg_apply_shadow_ms={d} avg_collect_sync_targets_ms={d} avg_append_replay_journal_ms={d} avg_wait_sync_ms={d} avg_notify_enrichment_ms={d}\n",
        .{
            total_docs,
            batch_docs,
            batch_count,
            dims,
            @divTrunc(@divTrunc(total_profile.total_ns, batch_count), std.time.ns_per_ms),
            @divTrunc(@divTrunc(total_profile.resolve_transforms_ns, batch_count), std.time.ns_per_ms),
            @divTrunc(@divTrunc(total_profile.merge_effective_req_ns, batch_count), std.time.ns_per_ms),
            @divTrunc(@divTrunc(total_profile.predicates_ns, batch_count), std.time.ns_per_ms),
            @divTrunc(@divTrunc(total_profile.validate_range_ns, batch_count), std.time.ns_per_ms),
            @divTrunc(@divTrunc(total_profile.extract_writes_ns, batch_count), std.time.ns_per_ms),
            @divTrunc(@divTrunc(total_profile.delete_artifacts_ns, batch_count), std.time.ns_per_ms),
            @divTrunc(@divTrunc(total_profile.precompute_generated_ns, batch_count), std.time.ns_per_ms),
            @divTrunc(@divTrunc(total_profile.store_write_ns, batch_count), std.time.ns_per_ms),
            @divTrunc(@divTrunc(total_profile.split_delta_ns, batch_count), std.time.ns_per_ms),
            @divTrunc(@divTrunc(total_profile.build_derived_ns, batch_count), std.time.ns_per_ms),
            @divTrunc(@divTrunc(total_profile.apply_shadow_ns, batch_count), std.time.ns_per_ms),
            @divTrunc(@divTrunc(total_profile.collect_sync_targets_ns, batch_count), std.time.ns_per_ms),
            @divTrunc(@divTrunc(total_profile.append_replay_journal_ns, batch_count), std.time.ns_per_ms),
            @divTrunc(@divTrunc(total_profile.wait_sync_ns, batch_count), std.time.ns_per_ms),
            @divTrunc(@divTrunc(total_profile.notify_enrichment_ns, batch_count), std.time.ns_per_ms),
        },
    );
}

test "db hbc posting lazy versus eager profile benchmark" {
    if (!profileBenchTestsEnabled()) return error.SkipZigTest;
    const alloc = std.testing.allocator;

    const dims: usize = @max(@as(usize, 1), readEnvUsize("ANTFLY_HBC_POSTING_BENCH_DIMS", 16));
    const doc_count: usize = @max(@as(usize, 1), readEnvUsize("ANTFLY_HBC_POSTING_BENCH_DOCS", 1024));
    const Mode = struct {
        name: []const u8,
        lazy: bool,
    };
    const modes = [_]Mode{
        .{
            .name = "eager",
            .lazy = false,
        },
        .{
            .name = "lazy",
            .lazy = true,
        },
    };

    const BenchWrites = struct {
        fn fill(
            allocator: Allocator,
            writes: []types.BatchWrite,
            vector_buf: []f32,
            salt: usize,
        ) !void {
            for (writes, 0..) |*write, doc_id| {
                var norm_sq: f32 = 0;
                for (vector_buf, 0..) |*slot, dim| {
                    const raw: u32 = @intCast(((doc_id + salt) * 1103515245 + dim * 2654435761 + 19) % 1000);
                    const centered = (@as(f32, @floatFromInt(raw)) / 500.0) - 1.0;
                    slot.* = centered;
                    norm_sq += centered * centered;
                }
                const inv_norm: f32 = 1.0 / @sqrt(norm_sq);
                for (vector_buf) |*slot| slot.* *= inv_norm;

                const key = try std.fmt.allocPrint(allocator, "doc:{d}", .{doc_id});
                errdefer allocator.free(key);
                const value = try std.fmt.allocPrint(
                    allocator,
                    "{{\"embedding\":{f}}}",
                    .{std.json.fmt(vector_buf, .{})},
                );
                write.* = .{
                    .key = key,
                    .value = value,
                };
            }
        }

        fn free(allocator: Allocator, writes: []types.BatchWrite) void {
            for (writes) |*write| {
                allocator.free(write.key);
                allocator.free(write.value);
                write.* = .{ .key = &.{}, .value = &.{} };
            }
        }
    };

    for (modes) |mode| {
        var path_buf: [256]u8 = undefined;
        const path = tempPath(&path_buf);
        defer cleanupTempDir(path);

        var db = try DB.open(alloc, std.mem.span(path), .{});
        defer db.close();

        const config_json = try std.fmt.allocPrint(
            alloc,
            "{{\"field\":\"embedding\",\"dims\":{d},\"metric\":\"cosine\",\"use_quantization\":true,\"lazy_posting_maintenance\":{s},\"auto_posting_maintenance_max_postings\":0}}",
            .{ dims, if (mode.lazy) "true" else "false" },
        );
        defer alloc.free(config_json);

        try db.addIndex(.{
            .name = "dv_v1",
            .kind = .dense_vector,
            .config_json = config_json,
        });

        const writes = try alloc.alloc(types.BatchWrite, doc_count);
        @memset(writes, .{ .key = &.{}, .value = &.{} });
        defer {
            BenchWrites.free(alloc, writes);
            alloc.free(writes);
        }

        const vector_buf = try alloc.alloc(f32, dims);
        defer alloc.free(vector_buf);

        try BenchWrites.fill(alloc, writes, vector_buf, 0);
        const seed_start = monotonicTimeNs();
        try db.batch(.{
            .writes = writes,
            .sync_level = .full_index,
        });
        const seed_ns = monotonicTimeNs() - seed_start;

        const seed_stats = try db.stats(alloc);
        defer types.freeDBStats(alloc, seed_stats);

        BenchWrites.free(alloc, writes);
        try BenchWrites.fill(alloc, writes, vector_buf, 17);

        var batch_profile = BatchProfile{};
        const write_start = monotonicTimeNs();
        try db.batchProfiled(.{
            .writes = writes,
            .sync_level = .full_index,
        }, &batch_profile);
        const write_ns = monotonicTimeNs() - write_start;

        const before_idle_stats = try db.stats(alloc);
        defer types.freeDBStats(alloc, before_idle_stats);

        const idle_start = monotonicTimeNs();
        const idle_steps = try db.runDensePostingMaintenanceForIdle();
        const idle_ns = monotonicTimeNs() - idle_start;

        const stats = try db.stats(alloc);
        defer types.freeDBStats(alloc, stats);

        std.debug.print(
            "hbc_posting_lazy_vs_eager mode={s} docs={d} seed_ms={d} update_ms={d} idle_ms={d} idle_steps={d} dirty_before_idle={d} dirty_after_idle={d} update_lazy_centroid_deferrals={d} update_repaired_postings={d} total_lazy_centroid_deferrals={d} total_repaired_postings={d} split_postings={d} merged_postings={d} boundary_reassigned={d}\n",
            .{
                mode.name,
                doc_count,
                @divTrunc(seed_ns, std.time.ns_per_ms),
                @divTrunc(write_ns, std.time.ns_per_ms),
                @divTrunc(idle_ns, std.time.ns_per_ms),
                idle_steps,
                before_idle_stats.indexes[0].hbc_posting.dirty_postings,
                stats.indexes[0].hbc_posting.dirty_postings,
                profileDelta(stats.indexes[0].hbc_posting.lazy_centroid_deferrals, seed_stats.indexes[0].hbc_posting.lazy_centroid_deferrals),
                profileDelta(stats.indexes[0].hbc_posting.maintenance_repaired_postings, seed_stats.indexes[0].hbc_posting.maintenance_repaired_postings),
                stats.indexes[0].hbc_posting.lazy_centroid_deferrals,
                stats.indexes[0].hbc_posting.maintenance_repaired_postings,
                stats.indexes[0].hbc_posting.maintenance_split_postings,
                stats.indexes[0].hbc_posting.maintenance_merged_postings,
                stats.indexes[0].hbc_posting.maintenance_boundary_reassigned_vectors,
            },
        );
    }
}

test "db rebuild dense indexes deletes corrupt stored embedding artifacts" {
    const alloc = std.testing.allocator;

    var path_buf: [256]u8 = undefined;
    const path = tempPath(&path_buf);
    defer cleanupTempDir(path);

    var db = try DB.open(alloc, std.mem.span(path), .{});
    defer db.close();

    try db.addIndex(.{
        .name = "dv_v1",
        .kind = .dense_vector,
        .config_json = "{\"field\":\"embedding\",\"dims\":2}",
    });

    const artifact_key = try internal_keys.embeddingArtifactKeyForDocumentAlloc(alloc, "doc:a", "dv_v1");
    defer alloc.free(artifact_key);
    try db.core.store.put(artifact_key, "bad-artifact");

    const rebuilt = try db.rebuildDenseIndexesFromStoredEmbeddingArtifacts(alloc);
    try std.testing.expectEqual(@as(usize, 0), rebuilt);
    try std.testing.expectError(error.NotFound, db.core.store.get(alloc, artifact_key));
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

    DB.derivedAsyncFreeDenseArtifactRebuildWrites(alloc, &writes);
    try std.testing.expectEqual(@as(usize, 0), writes.items.len);
}

test "db rw lock allows search and scan while shared read lock is held" {
    const alloc = std.heap.c_allocator;

    var path_buf: [256]u8 = undefined;
    const path = tempPath(&path_buf);
    defer cleanupTempDir(path);

    var db = try DB.open(alloc, std.mem.span(path), .{});
    defer db.close();

    try db.addIndex(.{
        .name = "ft_v1",
        .kind = .full_text,
        .config_json = "{\"field\":\"title\"}",
    });
    try db.batch(.{
        .writes = &.{
            .{ .key = "doc:a", .value = "{\"title\":\"alpha\"}" },
        },
        .sync_level = .full_text,
    });

    var held = SharedReadLockHold{ .db = &db };
    const held_thread = try std.Thread.spawn(.{}, SharedReadLockHold.run, .{&held});
    try std.testing.expect(db_internal.waitForAtomicU8(&held.acquired, 1, 10_000));

    var search_probe = ConcurrentReadProbe{ .db = &db };
    const search_thread = try std.Thread.spawn(.{}, ConcurrentReadProbe.runSearch, .{&search_probe});

    try std.testing.expect(db_internal.waitForAtomicU8(&search_probe.started, 1, 10_000));
    try std.testing.expect(db_internal.waitForAtomicU8(&search_probe.done, 1, 10_000));
    try std.testing.expectEqual(@as(u8, 0), search_probe.failed.load(.monotonic));
    search_thread.join();

    var scan_probe = ConcurrentReadProbe{ .db = &db };
    const scan_thread = try std.Thread.spawn(.{}, ConcurrentReadProbe.runScan, .{&scan_probe});
    try std.testing.expect(db_internal.waitForAtomicU8(&scan_probe.started, 1, 10_000));
    try std.testing.expect(db_internal.waitForAtomicU8(&scan_probe.done, 1, 10_000));
    try std.testing.expectEqual(@as(u8, 0), scan_probe.failed.load(.monotonic));
    scan_thread.join();

    held.release.store(1, .monotonic);
    held_thread.join();
}

test "db rw lock keeps batch writes blocked behind shared read lock" {
    const alloc = std.heap.c_allocator;

    var path_buf: [256]u8 = undefined;
    const path = tempPath(&path_buf);
    defer cleanupTempDir(path);

    var db = try DB.open(alloc, std.mem.span(path), .{});
    defer db.close();

    try db.batch(.{
        .writes = &.{
            .{ .key = "doc:a", .value = "{\"title\":\"alpha\"}" },
        },
    });

    var held = SharedReadLockHold{ .db = &db };
    const held_thread = try std.Thread.spawn(.{}, SharedReadLockHold.run, .{&held});
    try std.testing.expect(db_internal.waitForAtomicU8(&held.acquired, 1, 10_000));

    var write_probe = ConcurrentWriteProbe{ .db = &db };
    const write_thread = try std.Thread.spawn(.{}, ConcurrentWriteProbe.runBatch, .{&write_probe});

    try std.testing.expect(db_internal.waitForAtomicU8(&write_probe.started, 1, 10_000));

    var still_blocked = true;
    var attempts: usize = 0;
    while (attempts < 10_000) : (attempts += 1) {
        if (write_probe.done.load(.monotonic) != 0 or write_probe.failed.load(.monotonic) != 0) {
            still_blocked = false;
            break;
        }
        db_internal.spinOrYield();
    }
    try std.testing.expect(still_blocked);

    held.release.store(1, .monotonic);
    held_thread.join();
    write_thread.join();
    try std.testing.expectEqual(@as(u8, 0), write_probe.failed.load(.monotonic));
    try std.testing.expectEqual(@as(u8, 1), write_probe.done.load(.monotonic));

    const doc = (try db.get(alloc, "doc:b")) orelse return error.TestExpectedEqual;
    defer alloc.free(doc);
    try std.testing.expectEqualStrings("{\"title\":\"bravo\"}", doc);
}

test "db scan returns hashes and projected documents" {
    const alloc = std.testing.allocator;

    var path_buf: [256]u8 = undefined;
    const path = tempPath(&path_buf);
    defer cleanupTempDir(path);

    var db = try DB.open(alloc, std.mem.span(path), .{});
    defer db.close();

    try db.batch(.{
        .writes = &.{
            .{ .key = "doc:a", .value = "{\"title\":\"alpha\",\"body\":\"x\"}" },
            .{ .key = "doc:b", .value = "{\"title\":\"beta\",\"body\":\"y\"}" },
            .{ .key = "doc:c", .value = "{\"title\":\"gamma\",\"body\":\"z\"}" },
        },
    });

    var result = try db.scan(alloc, "doc:a", "doc:c", .{
        .include_documents = true,
        .fields = &.{"title"},
        .include_all_fields = false,
    });
    defer result.deinit(alloc);

    try std.testing.expectEqual(@as(usize, 2), result.hashes.len);
    try std.testing.expectEqualStrings("doc:b", result.hashes[0].id);
    try std.testing.expectEqualStrings("doc:c", result.hashes[1].id);
    try std.testing.expectEqual(@as(usize, 2), result.documents.len);
    try std.testing.expect(std.mem.indexOf(u8, result.documents[0].json, "\"title\"") != null);
    try std.testing.expect(std.mem.indexOf(u8, result.documents[0].json, "\"body\"") == null);
}

test "relational table point reads use only the relational base store" {
    const alloc = std.testing.allocator;
    const table_schema_api = @import("../../schema/mod.zig");

    var path_buf: [256]u8 = undefined;
    const path = tempPath(&path_buf);
    defer cleanupTempDir(path);

    var db = try DB.open(alloc, std.mem.span(path), .{});
    defer db.close();

    const schema_json =
        \\{"version":1,"storage_mode":"relational","default_type":"row","enforce_types":true,"document_schemas":{"row":{"schema":{"type":"object","properties":{"title":{"type":"text"},"amount":{"type":"numeric"},"attrs":{"type":"json"}},"required":["title"],"additionalProperties":false}}}}
    ;
    var parsed_schema = try table_schema_api.parseValidatedTableSchema(alloc, schema_json);
    defer parsed_schema.deinit(alloc);
    const runtime_schema = try table_schema_api.deriveRuntimeTableSchema(alloc, parsed_schema);
    defer schema_mod.freeSchema(alloc, runtime_schema);
    try db.setSchema(runtime_schema);

    try db.batch(.{
        .writes = &.{.{
            .key = "row:base",
            .value =
            \\{"title":"base row","amount":12.5,"attrs":{"tier":"gold"}}
            ,
        }},
    });

    const relational_key = try relational_store_mod.rowKeyAlloc(alloc, "row:base");
    defer alloc.free(relational_key);
    const raw_row = try db.core.store.get(alloc, relational_key);
    defer alloc.free(raw_row);
    try std.testing.expect(mapper.isRelationalRowValue(raw_row));

    const primary_key = try internal_keys.documentKeyAlloc(alloc, "row:base");
    defer alloc.free(primary_key);
    const maybe_primary = db.core.store.get(alloc, primary_key) catch |err| switch (err) {
        error.NotFound => null,
        else => return err,
    };
    if (maybe_primary) |primary_value| {
        defer alloc.free(primary_value);
        return error.TestExpectedEqual;
    }
    try db.core.store.put(primary_key, "{\"title\":\"stale primary\",\"amount\":999}");

    const doc = (try db.get(alloc, "row:base")).?;
    defer alloc.free(doc);
    try std.testing.expect(std.mem.indexOf(u8, doc, "\"title\":\"base row\"") != null);
    try std.testing.expect(std.mem.indexOf(u8, doc, "stale primary") == null);
    try std.testing.expect(std.mem.indexOf(u8, doc, "\"amount\":12.5") != null);
    try std.testing.expect(std.mem.indexOf(u8, doc, "\"attrs\":{\"tier\":\"gold\"}") != null);

    const median = try db.findMedianKey(alloc);
    defer alloc.free(median);
    try std.testing.expectEqualStrings("row:base", median);

    var scan_result = try db.scan(alloc, "row:", "row:\xff", .{
        .include_documents = true,
        .fields = &.{"title"},
        .include_all_fields = false,
    });
    defer scan_result.deinit(alloc);
    try std.testing.expectEqual(@as(usize, 1), scan_result.hashes.len);
    try std.testing.expectEqualStrings("row:base", scan_result.hashes[0].id);
    try std.testing.expectEqual(@as(usize, 1), scan_result.documents.len);
    try std.testing.expectEqualStrings("row:base", scan_result.documents[0].id);
    try std.testing.expect(std.mem.indexOf(u8, scan_result.documents[0].json, "\"title\":\"base row\"") != null);
    try std.testing.expect(std.mem.indexOf(u8, scan_result.documents[0].json, "stale primary") == null);
    try std.testing.expect(std.mem.indexOf(u8, scan_result.documents[0].json, "\"amount\"") == null);

    try db.batch(.{
        .writes = &.{.{
            .key = "row:base",
            .value =
            \\{"title":"updated row","amount":99.25,"attrs":{"tier":"platinum"}}
            ,
        }},
    });
    try std.testing.expectError(error.NotFound, db.core.store.get(alloc, primary_key));

    const updated_doc = (try db.get(alloc, "row:base")).?;
    defer alloc.free(updated_doc);
    try std.testing.expect(std.mem.indexOf(u8, updated_doc, "\"title\":\"updated row\"") != null);
    try std.testing.expect(std.mem.indexOf(u8, updated_doc, "stale primary") == null);

    try db.batch(.{
        .deletes = &.{"row:base"},
    });
    try std.testing.expect((try relational_store_mod.getRawAlloc(alloc, db.core.store, "row:base")) == null);
    try std.testing.expectError(error.NotFound, db.core.store.get(alloc, primary_key));
    try std.testing.expect((try db.get(alloc, "row:base")) == null);
}

test "db dense startup catch-up defaults keep more than one cache entry" {
    try std.testing.expect(denseCatchUpStartupCacheNodes() > 1);
    try std.testing.expect(denseCatchUpStartupCacheVectors() > 1);
}
