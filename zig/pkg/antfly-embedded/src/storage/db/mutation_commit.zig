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

//! Authoritative local commit, durable receipts and ordered publication.
//! Receivers borrow local resources; lifetime and scheduling remain with DB.

const DurableReplicationOutboxKind = @import("durable_outbox.zig").Kind;
pub const IndexTargetVisibility = @import("query_visibility.zig").IndexTargetVisibility;
pub const QueryVisibilityEvent = @import("query_visibility.zig").QueryVisibilityEvent;
const apply_state = @import("derived/apply_state.zig");
const backend_types = @import("../backend_types.zig");
const builtin = @import("builtin");
const change_journal_mod = @import("derived/change_journal.zig");
const derived_executor_mod = @import("derived/derived_executor.zig");
const derived_types = @import("derived/derived_types.zig");
const doc_identity = @import("doc_identity.zig");
const docstore_mod = @import("../docstore.zig");
const document_child_range_effects = @import("document_child_range_effects.zig");
const durable_outbox_store = @import("durable_outbox_store.zig");
const enrichment_artifact_codec = @import("enrichment/artifact_codec.zig");
const enrichment_runtime_mod = @import("enrichment/enrichment_runtime.zig");
const enrichment_state = @import("enrichment/enrichment_state.zig");
const enrichment_worker = @import("enrichment/enrichment_worker.zig");
const execution_resources = @import("execution_resources.zig");
const graph_edge_contender = @import("graph_edge_contender.zig");
const graph_edge_ttl_tombstone = @import("graph_edge_ttl_tombstone.zig");
const index_manager_mod = @import("catalog/index_manager.zig");
const index_repair_state = @import("derived/index_repair_state.zig");
const internal_keys = @import("../internal_keys.zig");
const mapper = @import("document_mapper.zig");
const merge_state_mod = @import("merge_state.zig");
const platform = @import("antfly_platform");
const platform_clock = @import("antfly_platform").clock;
const platform_time = @import("antfly_platform").time;
const range_cardinality = @import("range_cardinality.zig");
const range_state_mod = @import("range_state.zig");
const relational_index_catalog = @import("relational_index_catalog.zig");
const relational_index_jobs = @import("relational_index_jobs.zig");
const relational_index_plans = @import("relational_index_plan.zig");
const relational_index_records = @import("relational_index_records.zig");
const relational_row_codec = @import("algebraic/relational_row_codec.zig");
const relational_store = @import("relational_store.zig");
const replication_commit = @import("commit_integration.zig");
const replication_contract = @import("replication_contract.zig");
const replication_effects_mod = @import("replication_effects.zig");
const resource_manager_mod = @import("../resource_manager.zig");
const row_policy_authority_mod = @import("../../usermgr/row_policy_authority.zig");
const row_policy_bundle_mod = @import("row_policy_bundle.zig");
const runtime_failure_abi = @import("runtime_failure_abi");
const schema_registry_mod = @import("schema_registry.zig");
const std = @import("std");
const table_catalog_mod = @import("table_catalog.zig");
const transactions_mod = @import("../transactions.zig");
const types = @import("types.zig");

pub fn ImplementationFor(comptime S: type, comptime D: type) type {
    return struct {
        const Implementation = S;
        const Allocator = execution_resources.Allocator;
        const AsyncContext = execution_resources.AsyncContext;
        const BatchExecutionContext = execution_resources.BatchExecutionContext;
        const BatchExecutionOptions = execution_resources.BatchExecutionOptions;
        const BatchProfile = execution_resources.BatchProfile;
        const DocumentChildRangeDispatchGroup = execution_resources.DocumentChildRangeDispatchGroup;
        const DurableReplicationOutbox = execution_resources.DurableReplicationOutbox;
        const GeneratedBatchWritePlan = execution_resources.GeneratedBatchWritePlan;
        const GeneratedEmbeddingMemo = execution_resources.GeneratedEmbeddingMemo;
        const GraphArtifactClear = execution_resources.GraphArtifactClear;
        const ManagedSyncTargets = execution_resources.ManagedSyncTargets;
        const MutationBarrier = execution_resources.MutationBarrier;
        const MutexContentionStats = execution_resources.MutexContentionStats;
        const NeighborContextReplayHints = execution_resources.NeighborContextReplayHints;
        const OpenOptions = D.OpenOptions;
        const OrderedApplyReceipt = execution_resources.OrderedApplyReceipt;
        const OverwriteProbeEntry = execution_resources.OverwriteProbeEntry;
        const PrecomputedGeneratedBatch = execution_resources.PrecomputedGeneratedBatch;
        const PreparedRowAllocator = execution_resources.PreparedRowAllocator;
        const PreparedRowEffects = execution_resources.PreparedRowEffects;
        const ProfiledLock = execution_resources.ProfiledLock;
        const RelationalPriorMembership = execution_resources.RelationalPriorMembership;
        const ReplicationAsyncEffectMirror = execution_resources.ReplicationAsyncEffectMirror;
        const ReplicationDeferredCommitGate = execution_resources.ReplicationDeferredCommitGate;
        const ReplicationDeferredCommitGates = execution_resources.ReplicationDeferredCommitGates;
        const RequestPreparationContext = execution_resources.RequestPreparationContext;
        const ShadowState = execution_resources.ShadowState;
        const appendDocumentChildRangeOutboxWrites = D.appendDocumentChildRangeOutboxWrites;
        const decodeDurableReplicationOutbox = D.decodeDurableReplicationOutbox;
        const durableReplicationOutboxKeyAlloc = D.durableReplicationOutboxKeyAlloc;
        const durableReplicationOutboxKindFromKey = D.durableReplicationOutboxKindFromKey;
        const encodeDurableReplicationOutboxAlloc = D.encodeDurableReplicationOutboxAlloc;
        const enforceReplicationWriteGateOptional = D.enforceReplicationWriteGateOptional;
        const graph_merge_import_recovery_key = D.graph_merge_import_recovery_key;
        const orderedApplyDisposition = D.orderedApplyDisposition;
        const orderedApplyReceiptWrite = D.orderedApplyReceiptWrite;
        const ordered_apply_receipt_value_len = D.ordered_apply_receipt_value_len;
        const readOrderedApplyReceipt = D.readOrderedApplyReceipt;
        const replicationAppliedSequenceWrite = D.replicationAppliedSequenceWrite;
        const replicationMirrorRequiresDurableOutbox = D.replicationMirrorRequiresDurableOutbox;
        const replicationMirrorSyncEnabled = D.replicationMirrorSyncEnabled;
        const replication_applied_lsn_value_len = D.replication_applied_lsn_value_len;
        const replication_batch_outbox_key = D.replication_batch_outbox_key;
        const replication_outbox_checksum_len = D.replication_outbox_checksum_len;
        const replication_outbox_header_len = D.replication_outbox_header_len;
        const replication_replay_outbox_key = D.replication_replay_outbox_key;
        const replication_schema_outbox_key = D.replication_schema_outbox_key;
        const saveAppliedSequencesBatchLockedContext = D.saveAppliedSequencesBatchLockedContext;
        const GeneratedWriteReadSnapshot = S.GeneratedWriteReadSnapshot;
        const appendArtifactSourceRevisionWritesFromReplay = S.appendArtifactSourceRevisionWritesFromReplay;
        const appendAssetArtifactSourceIndexMutations = S.appendAssetArtifactSourceIndexMutations;
        const appendDenseArtifactCounterMutations = S.appendDenseArtifactCounterMutations;
        const appendDirectGraphTtlDueWrite = S.appendDirectGraphTtlDueWrite;
        const appendEmbeddingArtifactWrite = S.appendEmbeddingArtifactWrite;
        const appendGraphEdgeArtifactWrite = S.appendGraphEdgeArtifactWrite;
        const appendGraphEndpointRetirements = S.appendGraphEndpointRetirements;
        const appendGraphLifecycleGeneration = S.appendGraphLifecycleGeneration;
        const appendImportedGraphContenderMutations = S.appendImportedGraphContenderMutations;
        const appendMixedDirectGraphContenderMutations = S.appendMixedDirectGraphContenderMutations;
        const appendPrecomputedCoverageOutcomeMutations = S.appendPrecomputedCoverageOutcomeMutations;
        const appendPreparedGraphEdgeArtifactWrite = S.appendPreparedGraphEdgeArtifactWrite;
        const appendRetiredDirectGraphTtlDueDeletes = S.appendRetiredDirectGraphTtlDueDeletes;
        const appendSparseEmbeddingArtifactWrite = S.appendSparseEmbeddingArtifactWrite;
        const appendUniqueBorrowedKeyWithSet = S.appendUniqueBorrowedKeyWithSet;
        const appendUniqueOwnedKeyIndexed = S.appendUniqueOwnedKeyIndexed;
        const appendUniqueReplayRecordHint = S.appendUniqueReplayRecordHint;
        const appendUniqueReplayRecordKeyWithSet = S.appendUniqueReplayRecordKeyWithSet;
        const applyDerivedBacklogPressureContext = S.applyDerivedBacklogPressureContext;
        const applyDerivedBatchProfiled = S.applyDerivedBatchProfiled;
        const applyDerivedBatchTargetsProfiled = S.applyDerivedBatchTargetsProfiled;
        const applyDerivedBatchToIndexContext = S.applyDerivedBatchToIndexContext;
        const attachPreparedUpsertDocumentProjections = S.attachPreparedUpsertDocumentProjections;
        const augmentExtractedWriteWithGraphFieldEdges = S.augmentExtractedWriteWithGraphFieldEdges;
        const augmentExtractedWriteWithGraphFieldEdgesFromSnapshotParsed = S.augmentExtractedWriteWithGraphFieldEdgesFromSnapshotParsed;
        const augmentExtractedWriteWithGraphFieldEdgesParsed = S.augmentExtractedWriteWithGraphFieldEdgesParsed;
        const batchAffectsManagedIndex = S.batchAffectsManagedIndex;
        const buildDerivedBatch = S.buildDerivedBatch;
        const buildOverwrittenDocKeys = S.buildOverwrittenDocKeys;
        const childRangeManifestReader = S.childRangeManifestReader;
        const coalesceKeyValueRequest = S.coalesceKeyValueRequest;
        const collectEnrichmentArtifactDeletesForBatch = S.collectEnrichmentArtifactDeletesForBatch;
        const collectGraphArtifactsForDocIndex = S.collectGraphArtifactsForDocIndex;
        const collectManagedSyncTargetsForRecord = S.collectManagedSyncTargetsForRecord;
        const collectManagedSyncTargetsForRecordWithBatch = S.collectManagedSyncTargetsForRecordWithBatch;
        const directGraphNeighborContextHintsAlloc = S.directGraphNeighborContextHintsAlloc;
        const documentRangeLowerAlloc = S.documentRangeLowerAlloc;
        const encodeTimestampValue = S.encodeTimestampValue;
        const freeOwnedKeySlice = S.freeOwnedKeySlice;
        const generatedPrecomputeModeForSyncLevel = S.generatedPrecomputeModeForSyncLevel;
        const graphCleanupReplayWritesAlloc = S.graphCleanupReplayWritesAlloc;
        const graphEndpointCleanupDeletesAlloc = S.graphEndpointCleanupDeletesAlloc;
        const isMergeArtifactKey = S.isMergeArtifactKey;
        const loadDenseArtifactTargetCounter = S.loadDenseArtifactTargetCounter;
        const makeTimestampKey = S.makeTimestampKey;
        const overwriteProbeLessThan = S.overwriteProbeLessThan;
        const planGeneratedEnrichmentsForRows = S.planGeneratedEnrichmentsForRows;
        const prepareGeneratedEnrichments = S.prepareGeneratedEnrichments;
        const prepareRelationalRows = S.prepareRelationalRows;
        const projectedBatchLsmAdmissionBytes = S.projectedBatchLsmAdmissionBytes;
        const relationalColumns = S.relationalColumns;
        const requireRelationalConsumerFields = S.requireRelationalConsumerFields;
        const resolveWriteTimestampFromValue = S.resolveWriteTimestampFromValue;
        const resolveWriteTimestampNs = S.resolveWriteTimestampNs;
        const resolverReplayRetentionRequired = S.resolverReplayRetentionRequired;
        const retainPreparedTextRoots = S.retainPreparedTextRoots;
        const shouldWriteTimestamp = S.shouldWriteTimestamp;
        const storeHasUserDataBounded = S.storeHasUserDataBounded;
        const storedDocumentValuesEqual = S.storedDocumentValuesEqual;
        const strippedStoredDocumentValueAlloc = S.strippedStoredDocumentValueAlloc;
        const takeOwnedSlice = S.takeOwnedSlice;
        const validateDocumentExtractionInlineSources = S.validateDocumentExtractionInlineSources;
        const validateDocumentExtractionInlineSourcesSnapshotParsed = S.validateDocumentExtractionInlineSourcesSnapshotParsed;
        pub fn acquireReplicationMutationShared(self: anytype) ?MutationBarrier.SharedLease {
            const barrier = self.replicationMutationBarrier() orelse return null;
            return barrier.acquireShared();
        }

        pub fn batchContext(self: anytype) BatchExecutionContext {
            const resources = self.core.batchExecutionResources();
            return .{
                .alloc = self.alloc,
                .store = resources.store,
                .applied_sequence_checkpoint_path = resources.applied_sequence_checkpoint_path,
                .index_repair_checkpoint = resources.index_repair_checkpoint,
                .shard_manager = resources.shard_manager,
                .change_journal = resources.change_journal,
                .replay_source = resources.replay_source,
                .index_manager = resources.index_manager,
                .apply_mutex = resources.apply_mutex,
                .portable_runtime_activation_pending = &self.async_context.portable_runtime_activation_pending,
                .snapshot_admission = resources.snapshot_admission,
                .snapshot_replay_admission = resources.snapshot_replay_admission,
                .repair_replay_mutex = resources.repair_replay_mutex,
                .log_mutex = resources.log_mutex,
                .identity_namespace = resources.identity_namespace,
                .root_generation = self.core.root_generation,
                .identity_visibility = &self.core.identity_visibility,
                .artifact_cleanup_maybe = resources.artifact_cleanup_maybe,
                .executor = self.executor,
                .io = self.backend_runtime.io(),
                .enrichment_runtime = self.enrichment_runtime,
                .resolution_runtime = self.resolution_runtime,
                .promotion_runtime = self.promotion_runtime,
                .async_context = self.async_context,
                .relational_base_rows = relationalColumns(self) != null,
                .table_catalog = &self.core.table_catalog,
                .replication_async_effect_mirror = self.local_execution.replication_async_effect_mirror,
                .replication_async_batch_mirror = self.local_execution.replication_async_batch_mirror,
                .replication_async_metadata_mirror = self.local_execution.replication_async_metadata_mirror,
                .replication_write_gate = self.local_execution.replication_write_gate,
            };
        }

        pub fn batchInternalPrepared(
            self: anytype,
            req: types.BatchRequest,
            profile: ?*BatchProfile,
            opts: BatchExecutionOptions,
            generated_memo: *GeneratedEmbeddingMemo,
            prepared_row_allocator: *PreparedRowAllocator,
        ) anyerror!void {
            const maintenance_contract = @import("relational_index_maintenance_contract.zig");
            var maintenance_attempted = false;
            defer if (maintenance_attempted) {
                // Only a scheduling hint: the worker re-reads durable desired state.
                // Also wake on an ambiguous result after the store committed.
                self.local_execution.relational_index_maintenance_sweep.request();
                self.local_execution.relational_index_retry_after_ns.store(0, .release);
            };
            for (req.writes) |write| if (relational_index_catalog.Controller.isReservedMetadataKey(write.key)) {
                const trusted = opts.transaction_resolution != null or opts.replication_applied_lsn_marker != null;
                if (!trusted or !maintenance_contract.isControlKey(write.key)) return error.ReservedRelationalIndexMetadataKey;
                _ = try maintenance_contract.Control.decode(write.value);
                maintenance_attempted = true;
            };
            for (req.deletes) |key| if (relational_index_catalog.Controller.isReservedMetadataKey(key)) return error.ReservedRelationalIndexMetadataKey;
            for (req.transforms) |transform| if (relational_index_catalog.Controller.isReservedMetadataKey(transform.key)) return error.ReservedRelationalIndexMetadataKey;
            const schema_namespace = if (opts.transaction_resolution) |resolution|
                resolution.schema_namespace_generation orelse self.core.schemaNamespaceGeneration()
            else
                self.core.schemaNamespaceGeneration();
            if (schema_namespace != self.core.schemaNamespaceGeneration()) return error.PreparedGenerationChanged;
            if (openModeRequiresReadOnlyBackends(self.open_mode)) return error.ReadOnly;
            if (!opts.bypass_replication_write_gate and self.denseRepairWriteBackpressured()) return error.DenseRepairBackpressure;
            var replication_mutation = if (opts.bypass_replication_write_gate) null else self.acquireReplicationMutationShared();
            defer if (replication_mutation) |*lease| lease.release();
            if (!opts.bypass_replication_write_gate) try self.ensureDurableReplicationStartupBarrier();
            if (!opts.bypass_replication_write_gate) try self.enforceReplicationWriteGate();
            if (!opts.bypass_replication_write_gate) try self.preflightReplicationBatchSyncCommit();
            const total_start_ns = monotonicTimeNs();
            var schedule_replication_recovery_on_exit = false;
            defer {
                if (schedule_replication_recovery_on_exit) self.scheduleDurableReplicationOutboxRecovery();
                if (profile) |active_profile| {
                    active_profile.total_ns += monotonicTimeNs() - total_start_ns;
                }
            }

            try self.executor.checkSyncLevelHealth(req.sync_level);

            var foreground_write = resource_manager_mod.ResourceManager.ForegroundWriteLease{};
            defer foreground_write.release();

            // Admit projected LSM pressure before spending CPU, memory, or provider
            // calls on a PreparedRow that cannot yet commit. Maintenance workers
            // remain able to drain the pressure because no DB apply lock is held.
            if (self.core.index_manager.resource_manager) |manager| {
                try manager.awaitAdmission(.lsm_in_memory_state, projectedBatchLsmAdmissionBytes(req));
            }
            const preparation_timestamp_ns = if (req.timestamp_ns != 0) req.timestamp_ns else self.backend_runtime.clock().nowRealtimeNs();
            const preparation_alloc = prepared_row_allocator.allocator();

            // Resolve request ordering and transforms before row preparation. A
            // transform that reads durable state carries an optimistic version
            // predicate into the commit fence; retries rebuild both the effective
            // JSON and every derived artifact from a fresh base.
            const resolve_transforms_start_ns = monotonicTimeNs();
            var transform_snapshot = try self.captureTransformReadSnapshot(
                preparation_alloc,
                types.BatchWrite,
                req.writes,
                req.deletes,
                req.transforms,
            );
            defer transform_snapshot.deinit(preparation_alloc);
            var effective_ops = try coalesceKeyValueRequest(
                self,
                preparation_alloc,
                types.BatchWrite,
                req.writes,
                req.deletes,
                req.transforms,
                &transform_snapshot,
            );
            defer effective_ops.deinit(preparation_alloc);
            if (profile) |active_profile| recordProfileNs(profile, &active_profile.resolve_transforms_ns, resolve_transforms_start_ns);

            if (req.reject_graph_transform_projections and
                (effective_ops.graph_writes.items.len != 0 or effective_ops.graph_deletes.items.len != 0))
            {
                return error.UnsupportedTransformOperation;
            }

            const merge_effective_req_start_ns = monotonicTimeNs();
            var owned_effective_graph_writes: ?[]types.GraphEdgeWrite = null;
            defer if (owned_effective_graph_writes) |owned| preparation_alloc.free(owned);
            const effective_graph_writes = if (effective_ops.graph_writes.items.len == 0)
                req.graph_writes
            else blk: {
                const combined = try preparation_alloc.alloc(types.GraphEdgeWrite, req.graph_writes.len + effective_ops.graph_writes.items.len);
                @memcpy(combined[0..req.graph_writes.len], req.graph_writes);
                @memcpy(combined[req.graph_writes.len..], effective_ops.graph_writes.items);
                owned_effective_graph_writes = combined;
                break :blk combined;
            };

            var owned_effective_graph_deletes: ?[]types.GraphEdgeDelete = null;
            defer if (owned_effective_graph_deletes) |owned| preparation_alloc.free(owned);
            const effective_graph_deletes = if (effective_ops.graph_deletes.items.len == 0)
                req.graph_deletes
            else blk: {
                const combined = try preparation_alloc.alloc(types.GraphEdgeDelete, req.graph_deletes.len + effective_ops.graph_deletes.items.len);
                @memcpy(combined[0..req.graph_deletes.len], req.graph_deletes);
                @memcpy(combined[req.graph_deletes.len..], effective_ops.graph_deletes.items);
                owned_effective_graph_deletes = combined;
                break :blk combined;
            };

            var effective_req: types.BatchRequest = .{
                .artifact_catalog = req.artifact_catalog,
                .row_policy_principal_proof = req.row_policy_principal_proof,
                .row_policy_database = req.row_policy_database,
                .row_policy_admitted_at_seconds = req.row_policy_admitted_at_seconds,
                .range_guards = req.range_guards,
                .restore_staging_scope = req.restore_staging_scope,
                .restore_staging_plan_id = req.restore_staging_plan_id,
                .schema_version = req.schema_version,
                .relational_schema_version = req.relational_schema_version,
                .relational_integrity_generation_set = req.relational_integrity_generation_set,
                .relational_repair = req.relational_repair,
                .activate_range_tracking = req.activate_range_tracking,
                .integrity = req.integrity,
                .integrity_commands = req.integrity_commands,
                .relational_activation = req.relational_activation,
                .relational_retirement = req.relational_retirement,
                .relational_index_maintenance = req.relational_index_maintenance,
                .writes = effective_ops.writes,
                .deletes = effective_ops.deletes,
                .graph_endpoint_cleanup = req.graph_endpoint_cleanup,
                .graph_endpoint_cleanup_planned = req.graph_endpoint_cleanup_planned,
                .graph_endpoint_cleanup_guards = req.graph_endpoint_cleanup_guards,
                .graph_writes = effective_graph_writes,
                .graph_deletes = effective_graph_deletes,
                .transforms = &.{},
                .predicates = req.predicates,
                .timestamp_ns = req.timestamp_ns,
                .sync_level = req.sync_level,
                .reject_graph_transform_projections = req.reject_graph_transform_projections,
                .split_checkpoint = req.split_checkpoint,
                .split_replication = req.split_replication,
                .split_transition = req.split_transition,
                .merge_source_transition = req.merge_source_transition,
                .merge_checkpoint = req.merge_checkpoint,
                .merge_replication = req.merge_replication,
                .merge_page = req.merge_page,
                .merge_artifacts = req.merge_artifacts,
                .transaction = req.transaction,
            };
            if (profile) |active_profile| recordProfileNs(profile, &active_profile.merge_effective_req_ns, merge_effective_req_start_ns);

            // Pin one immutable layout for the request and do the allocation-heavy
            // parse/extract/hash/encode work before entering the serialized apply
            // section. A concurrent schema/index publication or transform-base
            // mutation is detected after admission by comparing pinned epochs.
            // A committed entry retains the schema version used when its write
            // was admitted. Metadata may have published a newer active schema
            // before this replica applies the entry, just as a prepared durable
            // transaction may finish after publication. Use the immutable
            // historical write view for both cases without changing the active
            // schema or its durable catalog.
            const request_schema_binding: ?transactions_mod.SchemaBinding = if (opts.transaction_resolution) |resolution|
                resolution.schema_binding
            else if (opts.ordered_apply_receipt != null and req.relational_schema_version != null)
                .{ .version = req.relational_schema_version }
            else
                null;
            var request_schema_view = try self.acquireTransactionSchemaView(preparation_alloc, request_schema_binding);
            defer if (request_schema_view) |*view| view.release();
            if (req.schema_version) |version| {
                const view = request_schema_view orelse return error.PreparedGenerationChanged;
                if (view.version() != version) return error.PreparedGenerationChanged;
            }
            // Only authenticated hot-standby replay may supply final scoped metadata effects
            // without local participant intents. Scope, key kinds, owner range and
            // generation are revalidated below under the apply fence.
            const scoped_restore_replication_apply = opts.replication_applied_lsn_marker != null and effective_req.restore_staging_scope != null and opts.restore_staging == null;
            const live_replication_apply = opts.replication_applied_lsn_marker != null and effective_req.restore_staging_scope == null and opts.restore_staging == null;
            if (opts.transaction_resolution == null and !scoped_restore_replication_apply and !live_replication_apply) {
                for (effective_ops.writes) |write| if (isProtectedIntegrityKey(write.key) or isProtectedRangeWriteKey(write.key)) return error.InvalidIntegrityOperation;
                for (effective_ops.deletes) |key| if (isProtectedIntegrityKey(key) or isProtectedRangeWriteKey(key)) return error.InvalidIntegrityOperation;
                if (!req.graph_endpoint_cleanup_planned and hasCoordinatedConstraints(request_schema_view) and opts.restore_staging == null and req.split_replication == null and req.merge_replication == null and (effective_ops.writes.len != 0 or effective_ops.deletes.len != 0))
                    return error.ForeignKeyCoordinationRequired;
            }
            if (req.relational_schema_version) |version| {
                const view = request_schema_view orelse return error.InvalidRelationalRowsRequest;
                if (view.storageMode() != .relational) return error.InvalidRelationalRowsRequest;
                if (view.version() != version) return error.PreparedGenerationChanged;
            }
            // Integrity effects may only enter through durable transaction prepare.
            if (req.integrity.len != 0 or req.integrity_commands.len != 0 or req.relational_activation != null or req.relational_retirement != null or req.relational_index_maintenance != null) return error.InvalidIntegrityOperation;
            var relational_index_snapshot = self.core.relational_indexes.acquire();
            defer if (relational_index_snapshot) |*index_snapshot| index_snapshot.deinit();
            const index_ready = try preparation_alloc.alloc(bool, if (relational_index_snapshot) |index_snapshot| index_snapshot.plan.boundIndexes().len else 0);
            defer preparation_alloc.free(index_ready);
            var prepared_index_keys: ?relational_index_plans.Batch = if (relational_index_snapshot) |index_snapshot|
                relational_index_plans.Batch.init(preparation_alloc, index_snapshot.plan)
            else
                null;
            defer if (prepared_index_keys) |*keys| keys.deinit();
            if (relational_index_snapshot) |index_snapshot| {
                const view = request_schema_view orelse return error.PreparedGenerationChanged;
                if (index_snapshot.plan.schemaView().epoch != view.epoch and opts.ordered_apply_receipt == null)
                    return error.PreparedGenerationChanged;
            }
            var index_writer = relational_index_records.Writer.init(preparation_alloc);
            defer index_writer.deinit();
            // Row side effects outlive speculative preparation but are consumed as
            // borrowed slices by the serialized commit. A batch arena turns their
            // many small keys/payloads into bump allocations and one release.
            var prepared_effects_arena = std.heap.ArenaAllocator.init(preparation_alloc);
            defer prepared_effects_arena.deinit();
            var preprepared_rows: ?[]?mapper.PreparedRelationalWrite = null;
            var preprepared_effects: ?[]PreparedRowEffects = null;
            var preprepared_generated: ?PrecomputedGeneratedBatch = null;
            var preprepared_generated_plan: ?GeneratedBatchWritePlan = null;
            var prepared_write_plan_generation: ?u64 = null;
            var write_plan_snapshot: ?index_manager_mod.IndexManager.WritePlanSnapshotView = null;
            defer if (write_plan_snapshot) |*plan_snapshot| plan_snapshot.release();
            defer if (preprepared_rows) |rows| {
                for (rows) |*maybe_row| if (maybe_row.*) |*row| row.deinit(preparation_alloc);
                preparation_alloc.free(rows);
            };
            defer if (preprepared_effects) |effects| {
                preparation_alloc.free(effects);
            };
            defer if (preprepared_generated) |*generated| generated.deinit(preparation_alloc);
            defer if (preprepared_generated_plan) |*plan| plan.deinit();
            // Document-mode analogue of the relational hoist below: extraction and
            // generated-enrichment precompute (asset/chunk/embedding requests) run
            // here, before the exclusive apply lock, instead of unconditionally
            // in-lock. `preprepared_document_generated` is reused under the lock
            // only if the catalog generation this precompute observed is still
            // current (`document_generated_write_plan_generation`) and this
            // batch's own write keys have not been touched by another commit
            // since (`generated_precompute_snapshot`, validated unconditionally
            // alongside `transform_snapshot` below). Either check failing forces
            // `PreparedGenerationChanged`/`PreparedReadSetChanged`, which the
            // existing bounded retry in `batchInternalWithPreparationAllocator`
            // already handles by recomputing from a fresh snapshot -- never by
            // silently reusing stale precompute.
            var preprepared_document_generated: ?PrecomputedGeneratedBatch = null;
            defer if (preprepared_document_generated) |*generated| generated.deinit(preparation_alloc);
            var document_generated_write_plan_generation: ?u64 = null;
            var generated_precompute_snapshot: GeneratedWriteReadSnapshot = .{};
            defer generated_precompute_snapshot.deinit(preparation_alloc);
            if (request_schema_view) |view| if (view.storageMode() == .relational) {
                var stable_keys = std.StringHashMapUnmanaged(void).empty;
                defer stable_keys.deinit(preparation_alloc);
                try stable_keys.ensureTotalCapacity(preparation_alloc, std.math.cast(u32, effective_req.writes.len) orelse
                    return error.InvalidBatchRequest);
                var eligible = true;
                for (effective_req.writes) |write| if (isMetadataKey(write.key)) {
                    eligible = false;
                    break;
                } else {
                    const result = stable_keys.getOrPutAssumeCapacity(write.key);
                    if (result.found_existing) {
                        eligible = false;
                        break;
                    }
                };
                if (eligible) for (effective_req.deletes) |key| if (stable_keys.contains(key)) {
                    eligible = false;
                    break;
                };
                if (eligible) {
                    const requires_inline_generated =
                        (req.sync_level == .full_text or req.sync_level == .enrichments or req.sync_level == .full_index) or
                        splitShadowRequiresMaterializedDerivedBatch(self);
                    const rows = try preparation_alloc.alloc(?mapper.PreparedRelationalWrite, effective_req.writes.len);
                    @memset(rows, null);
                    preprepared_rows = rows;
                    const effects = try preparation_alloc.alloc(PreparedRowEffects, effective_req.writes.len);
                    for (effects) |*effect| effect.* = .{};
                    preprepared_effects = effects;
                    const prepare_start_ns = monotonicTimeNs();
                    write_plan_snapshot = try self.core.index_manager.acquireWritePlanSnapshot();
                    prepared_write_plan_generation = write_plan_snapshot.?.generation();
                    try prepareRelationalRows(
                        prepared_row_allocator,
                        self.backend_runtime.io(),
                        self,
                        effective_req.writes,
                        view.validator(),
                        view.tableSchema().*,
                        view.physicalLayout(),
                        write_plan_snapshot.?.plan().*,
                        retainPreparedTextRoots(req.sync_level, write_plan_snapshot.?.plan().has_text_consumers, splitShadowRequiresMaterializedDerivedBatch(self)),
                        preparation_timestamp_ns,
                        rows,
                        opts.durable_rows,
                        opts.restore_timestamps,
                        opts.restore_staging != null or opts.preserve_logical_values,
                    );
                    if (prepared_index_keys) |*keys| {
                        var largest_document: usize = 0;
                        var largest_tuple: usize = 0;
                        var largest_payload: usize = 0;
                        for (rows, effective_req.writes) |*maybe_row, write| {
                            const index_row = if (relational_index_snapshot.?.plan.schemaView().epoch == view.epoch)
                                try keys.appendPrepared(&maybe_row.*.?)
                            else
                                try keys.appendPreparedFromSchema(&maybe_row.*.?, view);
                            largest_document = @max(largest_document, internal_keys.encodedComponentLen(write.key));
                            for (keys.view.boundIndexes(), 0..) |_, index| {
                                const key = try keys.key(index_row, index);
                                largest_tuple = @max(largest_tuple, key.bytes.len);
                                largest_payload = @max(largest_payload, key.payload.len);
                            }
                        }
                        try index_writer.reserve(largest_document, largest_tuple);
                        try index_writer.reservePayload(largest_payload);
                    }
                    // Copy every borrowed catalog input into request-owned effects
                    // while holding only the catalog read lease. Schema/index
                    // publication may proceed after this short phase; the commit
                    // fence validates both generations before consuming the plan.
                    if ((request_schema_binding == null and !self.core.isSchemaViewCurrent(view)) or
                        self.core.index_manager.writePlanGeneration() != prepared_write_plan_generation.?)
                        return error.PreparedGenerationChanged;
                    {
                        const effects_alloc = prepared_effects_arena.allocator();
                        for (effective_req.writes, rows, effects) |write, *maybe_row, *effect| {
                            if (maybe_row.*) |*row| {
                                row.extracted.artifact_keys_owned_individually = false;
                                if (row.extracted.hasDocument()) {
                                    effect.store_key = try encodeStoreLookupKeyAlloc(self, effects_alloc, write.key);
                                    if (shouldWriteTimestamp(write.key)) {
                                        const write_timestamp_ns = try relational_row_codec.rowWriteTimestampNsTrusted(row.packed_row);
                                        effect.timestamp_key = try makeTimestampKey(effects_alloc, write.key);
                                        effect.timestamp_value = try encodeTimestampValue(effects_alloc, write_timestamp_ns);
                                    }
                                }
                                for (row.extracted.dense_embeddings) |*embedding| {
                                    if (embedding.artifact_key != null) continue;
                                    embedding.artifact_key = try appendEmbeddingArtifactWrite(
                                        effects_alloc,
                                        &effect.embedding_writes,
                                        write.key,
                                        write.key,
                                        embedding.index_name,
                                        "_embeddings",
                                        null,
                                        .authored,
                                        embedding.vector,
                                    );
                                }
                                for (row.extracted.sparse_embeddings) |*embedding| {
                                    if (embedding.artifact_key != null) continue;
                                    embedding.artifact_key = try appendSparseEmbeddingArtifactWrite(
                                        effects_alloc,
                                        &effect.embedding_writes,
                                        write.key,
                                        embedding.index_name,
                                        .authored,
                                        embedding.indices,
                                        embedding.values,
                                    );
                                }
                                for (row.extracted.graph_writes) |graph_write| {
                                    const generation = write_plan_snapshot.?.plan().graphCoverageGeneration(graph_write.index_name) orelse return error.IndexNotFound;
                                    const graph_entry = self.core.index_manager.graphIndex(graph_write.index_name) orelse return error.IndexNotFound;
                                    try appendPreparedGraphEdgeArtifactWrite(effects_alloc, self.core.store, &effect.graph_writes, graph_write, generation, graph_entry.ttl_duration_ns, preparation_timestamp_ns);
                                }
                                for (row.extracted.mentioned_graph_indexes) |index_name| {
                                    const clear = try GraphArtifactClear.initAlloc(effects_alloc, write.key, index_name);
                                    effect.graph_clears.append(effects_alloc, clear) catch |err| {
                                        var owned_clear = clear;
                                        owned_clear.deinit(effects_alloc);
                                        return err;
                                    };
                                }
                            }
                        }
                        if (requires_inline_generated) {
                            const extracted_rows = try preparation_alloc.alloc(mapper.ExtractedWrite, rows.len);
                            defer preparation_alloc.free(extracted_rows);
                            for (rows, 0..) |maybe_row, index| extracted_rows[index] = maybe_row.?.extracted;
                            preprepared_generated_plan = try planGeneratedEnrichmentsForRows(
                                preparation_alloc,
                                effective_req,
                                extracted_rows,
                                write_plan_snapshot.?.plan().*,
                            );
                        }
                    }
                    if (preprepared_generated_plan != null) {
                        var extracted_rows = try preparation_alloc.alloc(mapper.ExtractedWrite, rows.len);
                        defer preparation_alloc.free(extracted_rows);
                        for (rows, 0..) |maybe_row, index| extracted_rows[index] = maybe_row.?.extracted;
                        preprepared_generated = try prepareGeneratedEnrichments(
                            self,
                            preparation_alloc,
                            effective_req,
                            extracted_rows,
                            generatedPrecomputeModeForSyncLevel(req.sync_level),
                            opts.force_generated_artifact_names,
                            &preprepared_generated_plan.?,
                            generated_memo,
                        );
                    }
                    if (profile) |active_profile| {
                        recordProfileNs(profile, &active_profile.relational_prepare_ns, prepare_start_ns);
                        active_profile.relational_rows_prepared += @intCast(rows.len);
                        for (effective_req.writes, rows) |write, maybe_row| {
                            active_profile.relational_logical_bytes += @intCast(write.value.len);
                            active_profile.relational_encoded_bytes += @intCast(maybe_row.?.packed_row.len);
                        }
                    }
                }
            };
            // Document-mode tables have no `PreparedRelationalWrite`/write-plan
            // row encoding to hoist, but generated-enrichment precompute (asset
            // extraction, chunking, embedding) for `.enrichments`/`.full_index`/
            // `.full_text` sync levels is exactly as safe to run before the lock:
            // it only needs this write's own submitted value, the enrichment
            // catalog, and a handful of read-only store lookups (existing
            // asset/chunk state used to skip unchanged work). The catalog-epoch
            // and per-document mutation revisions below make stale precompute retry
            // instead of silently committing.
            // Transaction resolution publishes keys that still carry their
            // prepared intents; those reads are only valid under the apply lock,
            // so resolution keeps the in-lock precompute path.
            if (opts.transaction_resolution == null and
                !(if (request_schema_view) |view| view.storageMode() == .relational else false))
            {
                const requires_inline_generated =
                    (effective_req.sync_level == .full_text or effective_req.sync_level == .enrichments or effective_req.sync_level == .full_index) or
                    splitShadowRequiresMaterializedDerivedBatch(self);
                if (requires_inline_generated and effective_req.writes.len > 0) {
                    const precompute_generated_start_ns = monotonicTimeNs();
                    generated_precompute_snapshot = try self.captureGeneratedWriteReadSnapshot(preparation_alloc, effective_req.writes);
                    // Every catalog-derived input this loop needs (dense/sparse
                    // direct-field vectors, graph field-edge configs) is read
                    // from an owned, refcounted WritePlanSnapshot acquired here
                    // -- the same mechanism the relational-row hoist above uses
                    // via `write_plan_snapshot` -- instead of touching
                    // `self.core.index_manager`'s live catalog arrays directly.
                    // The snapshot is cloned under `catalog_mutex.lockShared()`
                    // once per generation and stays valid after that lock is
                    // released, so a concurrent catalog publish can replace or
                    // free the live entries without this precompute observing
                    // freed memory. Pinned before prepareGeneratedEnrichments
                    // runs (which may take slightly longer than this read), so
                    // any catalog publication racing with this precompute is
                    // also caught as a generation change under the lock, never
                    // silently used.
                    write_plan_snapshot = try self.core.index_manager.acquireWritePlanSnapshot();
                    document_generated_write_plan_generation = write_plan_snapshot.?.generation();
                    const extracted_rows = try preparation_alloc.alloc(mapper.ExtractedWrite, effective_req.writes.len);
                    var extracted_rows_initialized: usize = 0;
                    defer {
                        for (extracted_rows[0..extracted_rows_initialized]) |*item| item.deinit(preparation_alloc);
                        preparation_alloc.free(extracted_rows);
                    }
                    const doc_plan = write_plan_snapshot.?.plan().*;
                    const doc_plan_needs_parse = doc_plan.graph_fields.len != 0 or doc_plan.dense_fields.len != 0 or doc_plan.sparse_fields.len != 0;
                    for (effective_req.writes, 0..) |write, i| {
                        if (isMetadataKey(write.key)) {
                            extracted_rows[i] = .{
                                .cleaned_value = null,
                                .graph_writes = &.{},
                                .mentioned_graph_indexes = &.{},
                                .dense_embeddings = &.{},
                                .sparse_embeddings = &.{},
                            };
                            extracted_rows_initialized += 1;
                            continue;
                        }
                        extracted_rows[i] = try mapper.extractWrite(preparation_alloc, write.key, write.value);
                        extracted_rows_initialized += 1;
                        if (doc_plan_needs_parse) {
                            var parsed = try std.json.parseFromSlice(std.json.Value, preparation_alloc, write.value, .{});
                            defer parsed.deinit();
                            try augmentExtractedWriteWithGraphFieldEdgesFromSnapshotParsed(doc_plan, preparation_alloc, write.key, parsed.value, &extracted_rows[i]);
                            try doc_plan.appendIndexFieldEmbeddingsFromParsedToExtractedWrite(preparation_alloc, write.key, parsed.value, &extracted_rows[i]);
                        }
                    }
                    preprepared_document_generated = try prepareGeneratedEnrichments(
                        self,
                        preparation_alloc,
                        effective_req,
                        extracted_rows[0..extracted_rows_initialized],
                        generatedPrecomputeModeForSyncLevel(effective_req.sync_level),
                        opts.force_generated_artifact_names,
                        null,
                        generated_memo,
                    );
                    if (profile) |active_profile| recordProfileNs(profile, &active_profile.precompute_generated_ns, precompute_generated_start_ns);
                }
            }

            // hot-standby encoding is a pure function of the admitted request and can be
            // proportional to the entire batch. Prepare it before the serialized
            // apply section, then reuse the exact bytes for durable outboxes and
            // the stream append. This also avoids holding the hot-standby log mutex while
            // walking and encoding every document in a large request.
            var preencoded_replication_batch_payload: ?[]u8 = null;
            defer if (preencoded_replication_batch_payload) |payload| preparation_alloc.free(payload);
            const scoped_replication = opts.restore_staging != null or replication_contract.requiresDurableLifecycleReplication(effective_req);
            const local_graph_cleanup = req.graph_endpoint_cleanup and !req.graph_endpoint_cleanup_planned;
            // Cleanup identities depend on the current incoming directory. Encode
            // that bounded page under apply after selection, never the planner command.
            if (!opts.bypass_replication_write_gate and !local_graph_cleanup) if (self.local_execution.replication_async_batch_mirror) |mirror| {
                preencoded_replication_batch_payload = (if (req.artifact_catalog != null)
                    replication_effects_mod.encodeArtifactCatalogMutationRequestAlloc(preparation_alloc, req, opts.ordered_apply_receipt orelse return error.InvalidArtifactCatalogCommand)
                else if (opts.ordered_apply_receipt) |entry|
                    replication_effects_mod.encodeRaftBatchMutationRequestAlloc(preparation_alloc, opts.restore_replication_request orelse effective_req, entry)
                else
                    replication_effects_mod.encodeBatchMutationRequestAlloc(preparation_alloc, opts.restore_replication_request orelse effective_req)) catch |err| blk: {
                    if (err == error.OutOfMemory) return err;
                    // Non-resource encoding failures retain best-effort async
                    // behavior. Admission failures must never retry allocation
                    // uncharged after commit.
                    if (scoped_replication or replicationMirrorSyncEnabled(mirror)) return err;
                    break :blk null;
                };
            };

            // Reserve the fixed-size apply workspace before admission to the
            // serialized section. These buffers are request-owned and contain no
            // mutable store state; allocating them while holding apply only extends
            // tail latency for every writer queued behind this batch.
            const extracted = try preparation_alloc.alloc(mapper.ExtractedWrite, effective_req.writes.len);
            var extracted_initialized: usize = 0;
            defer {
                for (extracted[0..extracted_initialized]) |*item| item.deinit(self.alloc);
                preparation_alloc.free(extracted);
            }
            const overwritten_flags = try preparation_alloc.alloc(bool, effective_req.writes.len);
            defer preparation_alloc.free(overwritten_flags);
            @memset(overwritten_flags, false);
            const derived_changed_flags = try preparation_alloc.alloc(bool, effective_req.writes.len);
            defer preparation_alloc.free(derived_changed_flags);
            @memset(derived_changed_flags, true);
            const overwrite_probe_keys = try preparation_alloc.alloc([]const u8, effective_req.writes.len);
            defer preparation_alloc.free(overwrite_probe_keys);
            const overwrite_probe_values = try preparation_alloc.alloc(?[]const u8, effective_req.writes.len);
            defer preparation_alloc.free(overwrite_probe_values);
            var semantic_noop_store_keys = std.StringHashMapUnmanaged(void).empty;
            defer semantic_noop_store_keys.deinit(preparation_alloc);
            const can_elide_primary_noops = if (request_schema_view) |view|
                view.storageMode() == .document
            else
                true;
            if (can_elide_primary_noops) {
                try semantic_noop_store_keys.ensureTotalCapacity(
                    preparation_alloc,
                    std.math.cast(u32, effective_req.writes.len) orelse return error.InvalidBatchRequest,
                );
            }

            var snapshot_mutation = if (opts.snapshot_mutation) |lease| lease.retain() else self.core.snapshot_admission.acquireMutation();
            defer snapshot_mutation.release();
            std.debug.assert(snapshot_mutation.admission == self.core.snapshot_admission);
            if (builtin.is_test) {
                if (D.test_portable_runtime_batch_prelock_hook.*) |hook| {
                    hook.entered.store(true, .release);
                    while (!hook.release.load(.acquire)) @import("antfly_platform").time.yieldNow();
                }
            }

            // hot-standby replay may bypass the primary-role gate, but it must never bypass
            // portable runtime activation. Rechecking under apply also closes the
            // interval between the fast preflight above and lock acquisition.
            var prepared_artifacts = try self.prepareMergeArtifactEffects(preparation_alloc, req);
            defer prepared_artifacts.deinit();
            const apply_lock_wait_start_ns = monotonicTimeNs();
            try self.lockApplyForPortableRuntime();
            if (profile) |active_profile| active_profile.apply_lock_wait_ns += monotonicTimeNs() - apply_lock_wait_start_ns;
            if (self.local_execution.source_vectors.load(.acquire)) |source| source.recordBatchLockWait(monotonicTimeNs() -| apply_lock_wait_start_ns);
            var filtered_cleanup_edges: ?[]types.GraphEdgeDelete = null;
            defer if (filtered_cleanup_edges) |items| preparation_alloc.free(items);
            var filtered_cleanup_jobs: ?[][]const u8 = null;
            defer if (filtered_cleanup_jobs) |items| preparation_alloc.free(items);
            var cleanup_replay_writes: ?[]types.BatchWrite = null;
            defer if (cleanup_replay_writes) |items| preparation_alloc.free(items);
            var filtered_cleanup_replay_writes: ?[]types.BatchWrite = null;
            defer if (filtered_cleanup_replay_writes) |items| preparation_alloc.free(items);
            var graph_cleanup_page: ?docstore_mod.DocStore.GraphEndpointCleanupPage = null;
            defer if (graph_cleanup_page) |*page| page.deinit();
            var cleanup_graph_deletes: ?[]types.GraphEdgeDelete = null;
            var initialized_cleanup_deletes: usize = 0;
            defer if (cleanup_graph_deletes) |items| {
                for (items[0..initialized_cleanup_deletes]) |*item| item.deinit(preparation_alloc);
                preparation_alloc.free(items);
            };
            var apply_mutex_held = true;
            const apply_lock_acquired_ns = monotonicTimeNs();
            errdefer if (apply_mutex_held) unlockProfiledApply(self, profile, &apply_mutex_held, apply_lock_acquired_ns);
            // A durable epoch survives active-schema publication, not replacement
            // of the entire database namespace with reused version/transaction IDs.
            if (schema_namespace != self.core.schemaNamespaceGeneration()) return error.PreparedGenerationChanged;
            // A document batch has no AROW preparation to perform this fence on
            // its behalf. Check the pinned epoch again under exclusive admission.
            if (self.local_execution.initial_child_hidden.load(.acquire) and
                (effective_req.relational_topology == null or
                    (effective_req.relational_topology.?.action != .provision_initial_child and
                        effective_req.relational_topology.?.action != .release_initial_child and
                        effective_req.relational_topology.?.action != .cancel_initial_child)))
                return error.InitialChildNotPublished;
            if (req.schema_version != null) try self.validatePreparedSchemaViewLocked(request_schema_view);
            if (!self.core.relational_indexes.isCurrent(relational_index_snapshot)) return error.PreparedGenerationChanged;
            if (live_replication_apply) {
                var integrity_read = try self.core.store.beginProbeTxn();
                defer integrity_read.abort();
                try self.validateLiveReplicationIntegrityEffects(preparation_alloc, &integrity_read, effective_req);
            }
            if (self.local_execution.restore_staging_required.load(.acquire) or opts.restore_staging != null or effective_req.restore_staging_scope != null) {
                var restore_read = try self.core.store.beginProbeTxn();
                defer restore_read.abort();
                if (opts.restore_staging) |admission|
                    try @import("restore_staging.zig").validateImport(preparation_alloc, &restore_read, admission, effective_req.writes.len, effective_req.deletes.len)
                else if (opts.transaction_resolution == null) {
                    if (opts.replication_applied_lsn_marker != null and effective_req.restore_staging_scope != null) {
                        try @import("restore_staging.zig").requireMutableScope(preparation_alloc, &restore_read, effective_req.restore_staging_scope);
                        try self.validateRestoreStagingReplicationEffects(preparation_alloc, &restore_read, effective_req);
                    } else try @import("restore_staging.zig").requireScope(preparation_alloc, &restore_read, null, false);
                }
            }

            if (opts.ordered_apply_receipt) |identity| {
                switch (try orderedApplyDisposition(
                    try readOrderedApplyReceipt(self.alloc, self.core.store),
                    identity,
                )) {
                    .apply => {},
                    .already_applied => {
                        unlockProfiledApply(self, profile, &apply_mutex_held, apply_lock_acquired_ns);
                        return;
                    },
                }
            }

            var merge_page_value: ?[]u8 = null;
            defer if (merge_page_value) |value| self.alloc.free(value);
            var merge_source_catalog_value: ?[]u8 = null;
            defer if (merge_source_catalog_value) |value| self.alloc.free(value);
            // Checkpoints certify a copy; payloads must use a separately fenced
            // command so a stale checkpoint cannot smuggle destructive mutations.
            if (req.merge_checkpoint != null and (req.writes.len != 0 or req.deletes.len != 0 or
                effective_req.merge_artifacts.len != 0 or req.transforms.len != 0 or
                req.graph_writes.len != 0 or req.graph_deletes.len != 0))
                return error.InvalidBatchRequest;
            if (req.merge_replication) |replication| if (req.merge_checkpoint == null) {
                const raw = try merge_state_mod.loadRawAlloc(self.alloc, self.core.store);
                defer if (raw) |value| self.alloc.free(value);
                var state = if (raw) |value| try merge_state_mod.decodeAlloc(self.alloc, value) else null;
                defer if (state) |*value| value.deinit(self.alloc);
                if (!merge_state_mod.copyAllowed(state, replication)) {
                    if (!opts.bypass_replication_write_gate) return error.MergeCopyFenced;
                    // A delayed committed command must advance the receipt without
                    // touching documents, artifacts, indexes or visibility state.
                    if (opts.ordered_apply_receipt) |identity| {
                        var marker_buf: [ordered_apply_receipt_value_len]u8 = undefined;
                        try self.core.store.putBatch(&.{orderedApplyReceiptWrite(identity, &marker_buf)}, &.{});
                    }
                    if (opts.replication_applied_lsn_marker) |lsn| {
                        var marker_buf: [replication_applied_lsn_value_len]u8 = undefined;
                        try self.core.store.putBatch(&.{replicationAppliedSequenceWrite(lsn, &marker_buf)}, &.{});
                    }
                    self.core.unlockApply();
                    apply_mutex_held = false;
                    return;
                }
                const pages = @import("merge_page_contract.zig");
                const page_raw = try self.core.getStoreValue(self.alloc, pages.key);
                defer if (page_raw) |value| self.alloc.free(value);
                if (req.merge_page != null or page_raw != null) {
                    var progress = if (page_raw) |value| try pages.decode(self.alloc, value) else return error.MergePageSourceMissing;
                    defer progress.deinit();
                    if (progress.value.matches(replication)) {
                        if (req.merge_page == null) return error.MergePageRequired;
                        if (!replication.identity_namespace.eql(self.core.identity_namespace)) return error.DocIdentityNamespaceMismatch;
                        try pages.validateRange(self.alloc, state.?, req);
                        switch (try pages.plan(progress.value, req)) {
                            .replay => {
                                var marker_writes: [2]docstore_mod.KVPair = undefined;
                                var count: usize = 0;
                                var raft_buffer: [ordered_apply_receipt_value_len]u8 = undefined;
                                var standby_buffer: [replication_applied_lsn_value_len]u8 = undefined;
                                if (opts.ordered_apply_receipt) |identity| {
                                    marker_writes[count] = orderedApplyReceiptWrite(identity, &raft_buffer);
                                    count += 1;
                                }
                                if (opts.replication_applied_lsn_marker) |lsn| {
                                    marker_writes[count] = replicationAppliedSequenceWrite(lsn, &standby_buffer);
                                    count += 1;
                                }
                                if (count != 0) try self.core.store.putBatch(marker_writes[0..count], &.{});
                                self.core.unlockApply();
                                apply_mutex_held = false;
                                return;
                            },
                            .apply => |next| {
                                if (req.merge_page.?.chunk != null) {
                                    const expected = opts.merge_chunk_expected_progress orelse return error.InvalidMergePage;
                                    if (!std.mem.eql(u8, &expected, &@import("merge_page_chunks.zig").checksum(page_raw.?))) return error.MergePageSequenceGap;
                                }
                                if (req.merge_page.?.phase == .cleanup) try self.validateMergeCleanupPageLocked(state.?, req);
                                if (req.merge_page.?.phase == .cleanup_integrity) {
                                    const merged = state.?.merged_range orelse return error.InvalidMergeState;
                                    const base = state.?.receiver_base_range;
                                    const donor: types.ByteRange = if (!std.mem.eql(u8, merged.start, base.start)) .{ .start = merged.start, .end = base.start } else .{ .start = base.end, .end = merged.end };
                                    var cleanup_scan = try self.core.store.beginCurrentScanTxn();
                                    defer cleanup_scan.abort();
                                    var cleanup_arena = std.heap.ArenaAllocator.init(self.alloc);
                                    defer cleanup_arena.deinit();
                                    const expected = try @import("online_integrity_shadow.zig").cleanup(cleanup_arena.allocator(), &cleanup_scan, donor, req.merge_page.?.after);
                                    if (expected.effects.len != req.merge_page.?.integrity.len or expected.exhausted != req.merge_page.?.exhausted) return error.InvalidMergePage;
                                    for (expected.effects, req.merge_page.?.integrity) |left, right| if (!std.mem.eql(u8, left.key, right.key) or right.value != null) return error.InvalidMergePage;
                                }
                                merge_page_value = try pages.encode(self.alloc, next);
                            },
                        }
                    } else if (req.merge_page != null) return error.MergePageSourceMissing;
                }
            };

            // A fenced or already committed page has no artifact effects left to
            // validate. Retire its committed marker above even if local catalog
            // reconciliation has since advanced or removed the old projection.
            const transferred_artifacts = prepared_artifacts.effects;
            const artifact_source = if (req.merge_page) |page| @as(?@import("merge_page_contract.zig").Source, page.source) else if (req.merge_checkpoint) |checkpoint| checkpoint.page_source else null;
            if (artifact_source) |source| if (source.artifact_catalog) |binding| {
                var read = try self.core.store.beginReadTxn();
                defer read.abort();
                var namespace: [24]u8 = undefined;
                doc_identity.encodeNamespace(&namespace, self.core.identity_namespace);
                if (req.artifact_catalog) |install| {
                    if (!install.binding.compatible(binding) or !std.mem.eql(u8, &install.binding.digest, &(try @import("artifact_inventory.zig").local(&read)).digest)) return error.ArtifactCatalogDrift;
                } else try @import("artifact_inventory.zig").requireCompatibleReady(self.alloc, &read, namespace, binding);
                if (!try self.artifactMaterializationsReady(&read, try @import("artifact_inventory.zig").catalogs(&read))) return error.ArtifactCatalogDrift;
                if (prepared_artifacts.receiver_binding) |prepared_binding| {
                    const inventory = @import("artifact_inventory.zig");
                    const observed = try inventory.status(self.alloc, &read, namespace);
                    if (!observed.ready or !std.meta.eql(observed.ordered, @as(?inventory.Binding, prepared_binding))) return error.ArtifactCatalogDrift;
                }
                if (req.merge_page) |page| for (page.artifact_effects) |effect| {
                    if (@import("online_graph_artifacts.zig").isKey(effect.key)) {
                        if (prepared_artifacts.receiver_binding == null) return error.PreparedGenerationChanged;
                        continue;
                    }
                    try @import("online_vector_artifacts.zig").validate(effect.key, effect.value);
                    const identity = (try internal_keys.parseEmbeddingArtifactKeyAlloc(self.alloc, effect.key)) orelse return error.InvalidMergePage;
                    defer self.alloc.free(identity.doc_key);
                    defer self.alloc.free(identity.artifact_name);
                    const dense = self.core.index_manager.denseIndex(identity.artifact_name);
                    const sparse = self.core.index_manager.sparseIndex(identity.artifact_name);
                    if (dense) |entry| {
                        if (!entry.managed_direct_field) return error.InvalidMergePage;
                        if (effect.value) |value| {
                            const header = try enrichment_artifact_codec.decodeHeader(value);
                            if (header.kind != .dense_embedding or std.mem.readInt(u32, value[enrichment_artifact_codec.header_len..][0..4], .little) != entry.dims) return error.InvalidMergePage;
                        }
                    } else if (sparse) |entry| {
                        if (!entry.managed_direct_field) return error.InvalidMergePage;
                        if (effect.value) |value| if ((try enrichment_artifact_codec.decodeHeader(value)).kind != .sparse_embedding) return error.InvalidMergePage;
                    }
                    // Historical base values have no current projection but are
                    // still document-owned data. Preserve their bytes, with the
                    // same range, effect-language and Raft ordering proof; never
                    // synthesize an index from an unknown artifact name.
                };
            };

            if (effective_req.merge_artifacts.len > 0 and !req.graph_endpoint_cleanup) {
                if (!req.merge_replication.?.identity_namespace.eql(self.core.identity_namespace))
                    return error.DocIdentityNamespaceMismatch;
                for (effective_req.merge_artifacts) |row| {
                    const owner = (try internal_keys.decodeDocumentComponentAlloc(self.alloc, row.key)) orelse
                        return error.InvalidBatchRequest;
                    defer self.alloc.free(owner);
                    if (!self.core.byteRange().contains(owner)) return error.KeyOutOfRange;
                }
            }

            if (opts.transaction_resolution) |resolution| {
                const intent_snapshot = try self.core.validateTransactionIntentSnapshot(
                    resolution.txn_id,
                    resolution.expected_intent_revision,
                );
                if (!intent_snapshot.has_intents) {
                    // Validate the requested decision while still avoiding mapper
                    // and index preparation on an idempotent resolve retry. Intent
                    // resolution does not advance the prepare revision, so another
                    // resolver can legitimately remove the intents between the
                    // optimistic collection and this apply fence. The replicated
                    // receipt and coordinator acknowledgement must still commit in
                    // the same terminal batch before Raft advances this entry.
                    var raft_marker_value_buf: [ordered_apply_receipt_value_len]u8 = undefined;
                    const completion_writes: []const docstore_mod.KVPair = if (opts.ordered_apply_receipt) |identity|
                        &.{orderedApplyReceiptWrite(identity, &raft_marker_value_buf)}
                    else
                        &.{};
                    const outcome = try self.core.resolveTransactionIntentsWithExtraBatch(
                        resolution.txn_id,
                        resolution.status,
                        resolution.commit_version,
                        .{
                            .completion_writes = completion_writes,
                            .resolved_participant = resolution.resolved_participant,
                            .expected_intent_revision = resolution.expected_intent_revision,
                            .known_intent_keys = resolution.intent_keys,
                        },
                    );
                    unlockProfiledApply(self, profile, &apply_mutex_held, apply_lock_acquired_ns);
                    if (!opts.bypass_replication_write_gate) try self.flushTransactionReplicationOutbox(resolution.txn_id);
                    try self.waitForResolvedTransactionSync(req.sync_level, outcome.replay_sequence);
                    return;
                }
            }

            // Every batch on a graph database can change source inputs or remove
            // contributor state. Fence preparation through primary/replay commit,
            // including ordinary graph writes and document/relational deletes.
            // Acquire without retaining a catalog lease.
            if (!opts.bypass_replication_write_gate and self.async_context.primary_replication_append_pending.load(.acquire)) return error.ReplicationPublisherUnavailable;
            var graph_publication = if (self.core.index_manager.hasGraphIndexes())
                self.core.index_manager.beginGraphPrimaryMutation()
            else
                null;
            defer if (graph_publication) |*lease| lease.release();

            var coordinated_handoff = false;
            if (req.split_replication != null or req.split_checkpoint != null) {
                var topology_read = try self.core.store.beginProbeTxn();
                defer topology_read.abort();
                coordinated_handoff = try @import("relational_integrity_handoff.zig").admitSplitRequest(self.alloc, &topology_read, req);
                if (hasCoordinatedConstraints(request_schema_view) and !coordinated_handoff) return error.CoordinatedConstraintTopologyUnsupported;
            }
            if (req.merge_replication != null or req.merge_checkpoint != null or req.merge_page != null) {
                var topology_read = try self.core.store.beginProbeTxn();
                defer topology_read.abort();
                // Source retention remains authoritative after its brief pin
                // fence is released. Receiving another merge would replace that
                // source's range state while its immutable cut/tail are in use.
                if (try @import("../retained_effects.zig").load(&topology_read)) |retained| {
                    const namespace = @import("online_source_contract.zig").namespaceBytes(self.core.identity_namespace);
                    if (retained.active() and std.mem.eql(u8, &retained.namespace, &namespace)) return error.IntegrityTopologyBusy;
                }
                coordinated_handoff = try @import("online_integrity_shadow.zig").admit(self.alloc, &topology_read, req, self.core.identity_namespace);
                if (!coordinated_handoff) coordinated_handoff = try @import("relational_integrity_handoff.zig").admitMergeRequest(self.alloc, &topology_read, req);
                if (hasCoordinatedConstraints(request_schema_view) and !coordinated_handoff) return error.CoordinatedConstraintTopologyUnsupported;
            }
            // hot-standby carries already committed effects, including prepared decisions
            // drained after a topology fence. It must not rerun fresh-write
            // admission on the replica and strand an authoritative commit.
            if (!coordinated_handoff and opts.transaction_resolution == null and !live_replication_apply and !scoped_restore_replication_apply and (effective_req.writes.len != 0 or effective_req.deletes.len != 0 or
                effective_req.graph_writes.len != 0 or effective_req.graph_deletes.len != 0))
            {
                var topology_read = try self.core.store.beginProbeTxn();
                defer topology_read.abort();
                try @import("relational_integrity_topology.zig").requireUnfenced(&topology_read);
            }

            // A pending coalescer flush may itself run a complete nested batch and
            // visibility wait. Enter foreground scheduling only for this batch's
            // own primary mutation so nested `.full_index` work cannot be hidden
            // behind an outer maintenance-deferral lease.
            if (self.core.index_manager.resource_manager) |manager|
                foreground_write = manager.beginForegroundWrite();

            if (!try self.shouldApplySplitReplicationLocked(req)) {
                unlockProfiledApply(self, profile, &apply_mutex_held, apply_lock_acquired_ns);
                return;
            }

            // Validate the final post-transform rows at the storage boundary. API
            // preflight remains useful for early UX feedback, but every embedded,
            // replicated, recovery, and direct DB caller gets the same contract.
            if (schema_namespace != self.core.schemaNamespaceGeneration()) return error.PreparedGenerationChanged;
            // Coalescer draining above can release/reacquire apply. An index-only
            // publication need not change the schema or managed-index generation.
            if (!self.core.relational_indexes.isCurrent(relational_index_snapshot)) return error.PreparedGenerationChanged;
            const use_preprepared_rows = blk: {
                const rows = preprepared_rows orelse break :blk false;
                const pinned = request_schema_view orelse break :blk false;
                if ((request_schema_binding == null and !self.core.isSchemaViewCurrent(pinned)) or
                    rows.len != effective_req.writes.len) break :blk false;
                const expected_plan_generation = prepared_write_plan_generation orelse break :blk false;
                if (self.core.index_manager.writePlanGeneration() != expected_plan_generation) break :blk false;
                break :blk true;
            };
            // Never convert an optimistic generation race into allocation-heavy
            // work under the exclusive apply lock. batchInternal retries this
            // request from a freshly pinned immutable schema/catalog generation.
            if (preprepared_rows != null and !use_preprepared_rows)
                return error.PreparedGenerationChanged;
            // Same fence for document-mode's pre-lock generated-enrichment
            // precompute: a catalog publication (new/changed enrichment, index)
            // between the pre-lock read and this lock acquisition invalidates it.
            const use_preprepared_document_generated = preprepared_document_generated != null and
                document_generated_write_plan_generation != null and
                self.core.index_manager.writePlanGeneration() == document_generated_write_plan_generation.?;
            if (preprepared_document_generated != null and !use_preprepared_document_generated)
                return error.PreparedGenerationChanged;
            // Transform expansion or an epoch/index-plan race invalidates the
            // speculative rows. Pin the currently published epoch once for the
            // serialized fallback; AROW v2 never encodes without its compiled
            // physical layout.
            var apply_schema_view: ?schema_registry_mod.SchemaView = if (!use_preprepared_rows and relationalColumns(self) != null)
                if (request_schema_binding != null)
                    if (request_schema_view) |view| view.clone() else null
                else
                    self.core.acquireSchemaView()
            else
                null;
            defer if (apply_schema_view) |*view| view.release();
            if (apply_schema_view) |view| if (view.tableSchema().external_base_source != null and
                (effective_req.writes.len != 0 or effective_req.deletes.len != 0 or effective_req.graph_writes.len != 0 or effective_req.graph_deletes.len != 0)) return error.ExternalLakeReadOnly;
            if (relationalColumns(self) == null) for (effective_req.writes) |write| if (write.json_null_fields.len != 0) return error.InvalidBatchRequest;
            if (!use_preprepared_rows and relationalColumns(self) != null and apply_schema_view == null)
                return error.InvalidSchemaUpdateRequest;
            const batch_timestamp_ns = if (use_preprepared_rows)
                preparation_timestamp_ns
            else if (effective_req.timestamp_ns != 0)
                effective_req.timestamp_ns
            else
                preparation_timestamp_ns;

            if (opts.row_policy_principal) |principal| {
                const policy_schema_view = if (use_preprepared_rows)
                    request_schema_view orelse return error.RowPolicyCatalogChanged
                else
                    apply_schema_view orelse return error.RowPolicyCatalogChanged;
                try self.enforceRowPolicyMutationLocked(
                    policy_schema_view,
                    effective_req,
                    if (use_preprepared_rows) preprepared_rows else null,
                    principal,
                );
            }

            // Prepared transaction intents fence the ordinary single-group fast
            // path too. The key-oriented intent index keeps this O(touched keys)
            // instead of scanning every outstanding transaction.
            if (opts.transaction_resolution == null) {
                try self.core.checkOrdinaryWriteConflicts(effective_ops.writes, effective_ops.deletes);
            }
            try self.validateTransformReadSnapshot(transform_snapshot);
            // Catches a concurrent commit to one of this batch's own write keys
            // between the pre-lock precompute read and this lock acquisition --
            // the race the catalog-generation check above cannot see, since it
            // only tracks schema/enrichment catalog changes, not per-document
            // data changes. use_preprepared_document_generated already forced a
            // retry on a stale catalog; this call does the same for a stale key.
            if (use_preprepared_document_generated) try self.validateGeneratedWriteReadSnapshot(generated_precompute_snapshot);

            if (effective_req.predicates.len > 0) {
                const predicates_start_ns = monotonicTimeNs();
                var predicates = std.ArrayListUnmanaged(transactions_mod.VersionPredicate).empty;
                defer predicates.deinit(self.alloc);
                for (effective_req.predicates) |predicate| {
                    try predicates.append(self.alloc, .{
                        .key = predicate.key,
                        .expected_version = predicate.expected_version,
                        .expected_content_digest = predicate.expected_content_digest,
                        .unique_absence = predicate.unique_absence,
                    });
                }
                try self.core.checkVersionPredicates(predicates.items, null);
                if (profile) |active_profile| recordProfileNs(profile, &active_profile.predicates_ns, predicates_start_ns);
            }

            if (opts.validate_range_ownership) {
                const validate_range_start_ns = monotonicTimeNs();
                if (opts.transaction_resolution != null or scoped_restore_replication_apply or live_replication_apply) {
                    // Participant preparation routes private claims/references by
                    // their logical address, never by their NUL-prefixed physical
                    // key. Repeat that same ownership check at resolution. Other
                    // prepared catalog/checkpoint records are owner-local state.
                    for (effective_req.writes) |write| try self.validateResolvedKeyOwnership(write.key);
                    for (effective_req.deletes) |key| try self.validateResolvedKeyOwnership(key);
                    var graph_request = effective_req;
                    graph_request.writes = &.{};
                    graph_request.deletes = &.{};
                    try self.core.validateBatchRangeOwnership(graph_request);
                } else try self.core.validateBatchRangeOwnership(effective_req);
                if (profile) |active_profile| recordProfileNs(profile, &active_profile.validate_range_ns, validate_range_start_ns);
            }

            const extract_writes_start_ns = monotonicTimeNs();
            var index_read: ?docstore_mod.DocStore.Txn = if (relational_index_snapshot != null) try self.core.store.beginReadTxn() else null;
            defer if (index_read) |*read| read.abort();
            if (relational_index_snapshot) |index_snapshot| for (index_snapshot.plan.boundIndexes(), index_ready) |index, *ready| {
                ready.* = (try relational_index_jobs.status(&index_read.?, index)).state == .ready;
            };
            var index_stage: ?relational_index_records.Staged = if (index_read) |*read| relational_index_records.Staged.init(preparation_alloc, read) else null;
            defer if (index_stage) |*stage| stage.deinit();
            var prior_membership = try RelationalPriorMembership.init(preparation_alloc, if (relational_index_snapshot) |index_snapshot| index_snapshot.plan else null);
            defer prior_membership.deinit();
            // Consume the coalesced logical request, not raw input ordering. API
            // deletes win over writes; transforms may subsequently recreate a row.
            // Its effective write/delete sets are disjoint.
            if (index_stage) |*stage| for (effective_req.deletes) |key| {
                if (isMetadataKey(key)) continue;
                const plan = relational_index_snapshot.?.plan;
                const members = try prior_membership.read(self, &index_read.?, plan, index_ready, key);
                if (members.len != 0) for (plan.boundIndexes(), members) |index, member| {
                    if (member) _ = try stage.deleteIndex(&index_writer, index.id(), key, .indexed);
                };
                try stage.deleteDocument(&index_writer, key);
            };
            var store_writes = std.ArrayListUnmanaged(docstore_mod.KVPair).empty;
            if (self.local_execution.row_policy_table_name) |table_name| {
                try store_writes.append(self.alloc, .{ .key = internal_keys.graph_owning_table_key, .value = table_name });
            }
            defer store_writes.deinit(self.alloc);
            if (req.activate_range_tracking) {
                const ranges = @import("../range_protection.zig");
                try ranges.validateRequest(req);
                var probe = try self.core.store.beginReadTxn();
                defer probe.abort();
                if (!try ranges.isActive(&probe)) {
                    if (try self.core.hasTopologySensitiveTransactions()) return error.IntentConflict;
                    try store_writes.append(self.alloc, .{ .key = ranges.activation_key, .value = ranges.activation_value });
                }
            }
            var owned_store_keys = std.ArrayListUnmanaged([]u8).empty;
            defer {
                for (owned_store_keys.items) |key| self.alloc.free(key);
                owned_store_keys.deinit(self.alloc);
            }
            var owned_store_values = std.ArrayListUnmanaged([]u8).empty;
            defer {
                for (owned_store_values.items) |value| self.alloc.free(value);
                owned_store_values.deinit(self.alloc);
            }
            const vector_field_names_start_ns = monotonicTimeNs();
            const vector_store_field_names = if (relationalColumns(self) == null)
                try self.core.index_manager.vectorStoreFieldNamesAlloc(self.alloc)
            else
                &.{};
            if (profile) |active_profile| recordProfileNs(profile, &active_profile.extract_vector_field_names_ns, vector_field_names_start_ns);
            defer {
                for (vector_store_field_names) |field| self.alloc.free(field);
                if (vector_store_field_names.len > 0) self.alloc.free(vector_store_field_names);
            }
            var timestamp_writes = std.ArrayListUnmanaged(docstore_mod.KVPair).empty;
            defer {
                if (!use_preprepared_rows) {
                    for (timestamp_writes.items) |item| {
                        self.alloc.free(@constCast(item.key));
                        self.alloc.free(@constCast(item.value));
                    }
                }
                timestamp_writes.deinit(self.alloc);
            }
            var prepared_embedding_artifact_count: usize = 0;
            var explicit_embedding_artifact_writes = std.ArrayListUnmanaged(types.BatchWrite).empty;
            defer {
                for (explicit_embedding_artifact_writes.items[prepared_embedding_artifact_count..]) |item| {
                    self.alloc.free(@constCast(item.key));
                    self.alloc.free(@constCast(item.value));
                }
                explicit_embedding_artifact_writes.deinit(self.alloc);
            }
            var prepared_graph_artifact_count: usize = 0;
            var explicit_graph_artifact_writes = std.ArrayListUnmanaged(types.BatchWrite).empty;
            defer {
                for (explicit_graph_artifact_writes.items[prepared_graph_artifact_count..]) |item| {
                    self.alloc.free(@constCast(item.key));
                    self.alloc.free(@constCast(item.value));
                }
                explicit_graph_artifact_writes.deinit(self.alloc);
            }
            var delete_keys = std.ArrayListUnmanaged([]const u8).empty;
            defer delete_keys.deinit(self.alloc);
            var owned_delete_keys = std.ArrayListUnmanaged([]u8).empty;
            defer {
                for (owned_delete_keys.items) |key| self.alloc.free(key);
                owned_delete_keys.deinit(self.alloc);
            }
            var graph_artifact_clears = std.ArrayListUnmanaged(GraphArtifactClear).empty;
            defer {
                if (!use_preprepared_rows)
                    for (graph_artifact_clears.items) |*item| item.deinit(self.alloc);
                graph_artifact_clears.deinit(self.alloc);
            }
            var timestamp_delete_keys = std.ArrayListUnmanaged([]const u8).empty;
            defer {
                for (timestamp_delete_keys.items) |key| self.alloc.free(@constCast(key));
                timestamp_delete_keys.deinit(self.alloc);
            }
            var overwrite_probe_entries = std.ArrayListUnmanaged(OverwriteProbeEntry).empty;
            defer overwrite_probe_entries.deinit(self.alloc);
            var identity_upsert_keys = std.ArrayListUnmanaged([]const u8).empty;
            defer identity_upsert_keys.deinit(self.alloc);
            var identity_upsert_write_indexes = std.ArrayListUnmanaged(usize).empty;
            defer identity_upsert_write_indexes.deinit(self.alloc);
            var identity_writes = std.ArrayListUnmanaged(docstore_mod.KVPair).empty;
            defer {
                for (identity_writes.items) |item| {
                    self.alloc.free(@constCast(item.key));
                    self.alloc.free(@constCast(item.value));
                }
                identity_writes.deinit(self.alloc);
            }
            var identity_visibility_deletes = std.ArrayListUnmanaged([]u8).empty;
            defer {
                for (identity_visibility_deletes.items) |key| self.alloc.free(key);
                identity_visibility_deletes.deinit(self.alloc);
            }

            if (req.graph_endpoint_cleanup and !req.graph_endpoint_cleanup_planned) {
                // Recheck under the apply fence: ownership may have changed since
                // the scheduler or foreground drain probed it.
                if (try self.maintenanceRequiresOrderedApply()) return error.InvalidBatchRequest;
                effective_req.graph_endpoint_cleanup_planned = true;
                graph_cleanup_page = try self.core.store.prepareGraphEndpointCleanupPage(self.alloc);
                if (graph_cleanup_page) |page| {
                    const graph_deletes = try graphEndpointCleanupDeletesAlloc(preparation_alloc, page);
                    cleanup_graph_deletes = graph_deletes;
                    initialized_cleanup_deletes = graph_deletes.len;
                    effective_req.graph_deletes = graph_deletes;
                    effective_req.graph_endpoint_cleanup_guards = page.guards;
                    effective_req.deletes = page.deletes;
                    cleanup_replay_writes = try graphCleanupReplayWritesAlloc(preparation_alloc, page);
                    effective_req.merge_artifacts = cleanup_replay_writes.?;
                }
                // The selected identities and job removals share the primary apply
                // fence. Both the durable hot-standby outbox and stream reuse these bytes.
                // Encoding failures abort before commit even for async mirroring:
                // falling back to a planner command could fork authoritative state.
                if (!opts.bypass_replication_write_gate and self.local_execution.replication_async_batch_mirror != null) {
                    try types.validateGraphEndpointCleanupCommand(effective_req);
                    preencoded_replication_batch_payload = try replication_effects_mod.encodeBatchMutationRequestAlloc(preparation_alloc, effective_req);
                }
            }

            if (effective_req.graph_endpoint_cleanup_planned) {
                const contract = @import("../graph_cleanup_contract.zig");
                var live_endpoints = std.StringHashMapUnmanaged(void).empty;
                defer live_endpoints.deinit(preparation_alloc);
                var live_owners = std.StringHashMapUnmanaged(contract.Guard).empty;
                defer live_owners.deinit(preparation_alloc);
                var live_jobs = std.StringHashMapUnmanaged(void).empty;
                defer live_jobs.deinit(preparation_alloc);
                defer {
                    var iterator = live_jobs.keyIterator();
                    while (iterator.next()) |job| preparation_alloc.free(job.*);
                }
                var read = try self.core.store.beginReadTxn();
                defer read.abort();
                for (effective_req.graph_endpoint_cleanup_guards) |guard| {
                    const job = try contract.guardKeyAlloc(preparation_alloc, guard);
                    defer preparation_alloc.free(job);
                    const value = read.get(job) catch |err| switch (err) {
                        error.NotFound => continue,
                        else => return err,
                    };
                    if (!try contract.guardMatches(guard, job, value)) continue;
                    if (guard.kind == .endpoint) try live_endpoints.put(preparation_alloc, guard.endpoint, {}) else try live_owners.put(preparation_alloc, guard.endpoint, guard);
                    const retained_job = try preparation_alloc.dupe(u8, job);
                    live_jobs.put(preparation_alloc, retained_job, {}) catch |err| {
                        preparation_alloc.free(retained_job);
                        return err;
                    };
                }
                var edges = std.ArrayListUnmanaged(types.GraphEdgeDelete).empty;
                defer edges.deinit(preparation_alloc);
                for (effective_req.graph_deletes) |edge| if (live_endpoints.contains(edge.target)) {
                    const artifact = try internal_keys.graphRelationshipArtifactKeyAlloc(preparation_alloc, if (edge.owner_document.len > 0) edge.owner_document else if (edge.owner.len > 0) edge.owner else edge.source, edge.index_name, edge.edge_type, edge.target, edge.source, edge.edge_id);
                    defer preparation_alloc.free(artifact);
                    if (!try docstore_mod.DocStore.graphRelationshipLocallyOwned(&read, preparation_alloc, artifact)) continue;
                    try edges.append(preparation_alloc, edge);
                };
                var jobs = std.ArrayListUnmanaged([]const u8).empty;
                defer jobs.deinit(preparation_alloc);
                for (effective_req.deletes) |key| {
                    if (live_jobs.contains(key)) {
                        try jobs.append(preparation_alloc, key);
                        continue;
                    }
                    if (!internal_keys.isGraphRetirementKey(key)) continue;
                    var owners = live_owners.iterator();
                    while (owners.next()) |owner| {
                        if (!try contract.ownedBy(preparation_alloc, key, owner.key_ptr.*)) continue;
                        const current = read.get(key) catch |err| switch (err) {
                            error.NotFound => break,
                            else => return err,
                        };
                        // A new deletion after planning must retain its suppression.
                        if (try contract.retirementGeneration(current) < owner.value_ptr.generation) try jobs.append(preparation_alloc, key);
                        break;
                    }
                }
                var replay = std.ArrayListUnmanaged(types.BatchWrite).empty;
                defer replay.deinit(preparation_alloc);
                for (effective_req.merge_artifacts) |row| {
                    if (live_jobs.contains(row.key)) {
                        try replay.append(preparation_alloc, row);
                        continue;
                    }
                    if (!contract.isReplayInput(row.key)) continue;
                    var owners = live_owners.keyIterator();
                    while (owners.next()) |owner| {
                        if (!try contract.ownedBy(preparation_alloc, row.key, owner.*)) continue;
                        const current = read.get(row.key) catch |err| switch (err) {
                            error.NotFound => break,
                            else => return err,
                        };
                        // Replay an exact durable beforeimage only. An input update
                        // publishes its own graph effects and cannot be overwritten
                        // by an older queued page.
                        if (std.mem.eql(u8, current, row.value)) try replay.append(preparation_alloc, row);
                        break;
                    }
                }
                const selected_edges = try preparation_alloc.dupe(types.GraphEdgeDelete, edges.items);
                filtered_cleanup_edges = selected_edges;
                const selected_jobs = try preparation_alloc.dupe([]const u8, jobs.items);
                filtered_cleanup_jobs = selected_jobs;
                effective_req.graph_deletes = selected_edges;
                effective_req.deletes = selected_jobs;
                filtered_cleanup_replay_writes = try preparation_alloc.dupe(types.BatchWrite, replay.items);
                effective_req.merge_artifacts = filtered_cleanup_replay_writes.?;
            }

            for (effective_req.writes, 0..) |write, i| {
                if (isMetadataKey(write.key)) {
                    extracted[i] = .{
                        .cleaned_value = null,
                        .graph_writes = &.{},
                        .mentioned_graph_indexes = &.{},
                        .dense_embeddings = &.{},
                        .sparse_embeddings = &.{},
                    };
                    extracted_initialized += 1;
                    try store_writes.append(self.alloc, .{
                        .key = write.key,
                        .value = write.value,
                    });
                    continue;
                }
                const relational_prepare_start_ns = monotonicTimeNs();
                var prepared_relational: ?mapper.PreparedRelationalWrite = if (use_preprepared_rows) blk: {
                    const prepared = preprepared_rows.?[i].?;
                    preprepared_rows.?[i] = null;
                    break :blk prepared;
                } else if (apply_schema_view) |view|
                    if (write.json_null_fields.len != 0)
                        try mapper.PreparedRelationalWrite.initTyped(self.alloc, self.alloc, self.alloc, false, write.key, write.value, view.validator(), view.tableSchema().*, view.physicalLayout(), write.json_null_fields, opts.restore_staging != null or opts.preserve_logical_values)
                    else if (opts.restore_staging != null or opts.preserve_logical_values)
                        try mapper.PreparedRelationalWrite.initPreserved(self.alloc, write.key, write.value, view.validator(), view.tableSchema().*, view.physicalLayout())
                    else
                        try mapper.PreparedRelationalWrite.initFromIntent(
                            self.alloc,
                            write.key,
                            write.value,
                            view.validator(),
                            view.tableSchema().*,
                            view.physicalLayout(),
                            if (opts.durable_rows) |durable| durable.get(write.key) else null,
                        )
                else
                    null;
                defer if (prepared_relational) |*prepared| prepared.deinit(self.alloc);
                var fallback_consumer_plan = if (!use_preprepared_rows and prepared_relational != null) try self.core.index_manager.acquireWritePlanSnapshot() else null;
                defer if (fallback_consumer_plan) |*plan| plan.release();
                if (fallback_consumer_plan) |plan| try requireRelationalConsumerFields(&prepared_relational.?, apply_schema_view.?.tableSchema().*, plan.plan().*, false);
                if (!use_preprepared_rows) if (prepared_relational) |prepared| if (profile) |active_profile| {
                    recordProfileNs(profile, &active_profile.relational_prepare_ns, relational_prepare_start_ns);
                    active_profile.relational_rows_prepared += 1;
                    active_profile.relational_logical_bytes += @intCast(write.value.len);
                    active_profile.relational_encoded_bytes += @intCast(prepared.packed_row.len);
                };
                const mapper_extract_start_ns = monotonicTimeNs();
                const semantic_hash = if (prepared_relational) |*prepared| prepared.semantic_hash else null;
                extracted[i] = if (prepared_relational) |*prepared|
                    prepared.takeExtracted()
                else
                    try mapper.extractWrite(self.alloc, write.key, write.value);
                // Ownership has transferred even if a later graph/vector consumer
                // rejects this row. Include it in error cleanup immediately.
                extracted_initialized += 1;
                if (profile) |active_profile| recordProfileNs(profile, &active_profile.extract_mapper_ns, mapper_extract_start_ns);
                const graph_field_extract_start_ns = monotonicTimeNs();
                if (!use_preprepared_rows) {
                    if (prepared_relational) |*prepared|
                        try augmentExtractedWriteWithGraphFieldEdgesParsed(self, self.alloc, write.key, prepared.parsedValue(), &extracted[i])
                    else
                        try augmentExtractedWriteWithGraphFieldEdges(self, self.alloc, write.key, write.value, &extracted[i]);
                }
                if (profile) |active_profile| recordProfileNs(profile, &active_profile.extract_graph_fields_ns, graph_field_extract_start_ns);
                const index_field_embeddings_start_ns = monotonicTimeNs();
                if (!use_preprepared_rows) {
                    if (prepared_relational) |*prepared|
                        try self.core.index_manager.appendIndexFieldEmbeddingsFromPreparedToExtractedWrite(self.alloc, write.key, prepared.parsedValue(), try prepared.typedView(apply_schema_view.?.tableSchema().*, apply_schema_view.?.physicalLayout()), &extracted[i])
                    else
                        try self.core.index_manager.appendIndexFieldEmbeddingsToExtractedWrite(self.alloc, write.key, write.value, &extracted[i]);
                }
                if (profile) |active_profile| recordProfileNs(profile, &active_profile.extract_index_field_embeddings_ns, index_field_embeddings_start_ns);
                const embedding_artifacts_start_ns = monotonicTimeNs();
                if (use_preprepared_rows) {
                    const effects = &preprepared_effects.?[i];
                    try explicit_embedding_artifact_writes.appendSlice(self.alloc, effects.embedding_writes.items);
                    prepared_embedding_artifact_count = explicit_embedding_artifact_writes.items.len;
                    effects.embedding_writes.clearRetainingCapacity();
                } else {
                    for (extracted[i].dense_embeddings) |*embedding| {
                        if (embedding.artifact_key != null) continue;
                        embedding.artifact_key = try appendEmbeddingArtifactWrite(
                            self.alloc,
                            &explicit_embedding_artifact_writes,
                            write.key,
                            write.key,
                            embedding.index_name,
                            "_embeddings",
                            null,
                            .authored,
                            embedding.vector,
                        );
                    }
                    for (extracted[i].sparse_embeddings) |*embedding| {
                        if (embedding.artifact_key != null) continue;
                        embedding.artifact_key = try appendSparseEmbeddingArtifactWrite(
                            self.alloc,
                            &explicit_embedding_artifact_writes,
                            write.key,
                            embedding.index_name,
                            .authored,
                            embedding.indices,
                            embedding.values,
                        );
                    }
                }
                if (profile) |active_profile| recordProfileNs(profile, &active_profile.extract_embedding_artifacts_ns, embedding_artifacts_start_ns);
                const graph_artifacts_start_ns = monotonicTimeNs();
                if (use_preprepared_rows) {
                    const effects = &preprepared_effects.?[i];
                    try explicit_graph_artifact_writes.appendSlice(self.alloc, effects.graph_writes.items);
                    prepared_graph_artifact_count = explicit_graph_artifact_writes.items.len;
                    effects.graph_writes.clearRetainingCapacity();
                    try graph_artifact_clears.appendSlice(self.alloc, effects.graph_clears.items);
                    effects.graph_clears.clearRetainingCapacity();
                } else {
                    for (extracted[i].graph_writes) |graph_write| {
                        const generation = (self.core.index_manager.graphIndex(graph_write.index_name) orelse return error.IndexNotFound).config.coverage_generation;
                        const graph_entry = self.core.index_manager.graphIndex(graph_write.index_name) orelse return error.IndexNotFound;
                        try appendGraphEdgeArtifactWrite(self.alloc, self.core.store, &explicit_graph_artifact_writes, graph_write, generation, graph_entry.ttl_duration_ns, batch_timestamp_ns);
                    }
                    for (extracted[i].mentioned_graph_indexes) |index_name| {
                        const clear = try GraphArtifactClear.initAlloc(self.alloc, write.key, index_name);
                        graph_artifact_clears.append(self.alloc, clear) catch |err| {
                            var owned_clear = clear;
                            owned_clear.deinit(self.alloc);
                            return err;
                        };
                    }
                }
                if (profile) |active_profile| recordProfileNs(profile, &active_profile.extract_graph_artifacts_ns, graph_artifacts_start_ns);
                if (extracted[i].hasDocument()) {
                    // Only the document-mode branch consumes JSON here. Relational
                    // persistence and timestamps use the prepared typed authority.
                    const cleaned = if (prepared_relational == null) extracted[i].cleaned_value.? else "";
                    const resolved_write_timestamp_ns: ?u64 = if (!use_preprepared_rows and shouldWriteTimestamp(write.key))
                        (if (opts.restore_timestamps) |timestamps| timestamps.get(write.key) else null) orelse if (prepared_relational) |*prepared|
                            try resolveWriteTimestampFromValue(self, batch_timestamp_ns, prepared.parsedValue())
                        else
                            try resolveWriteTimestampNs(self, batch_timestamp_ns, write.value)
                    else
                        null;
                    if (!use_preprepared_rows) if (prepared_relational) |*prepared|
                        try prepared.finalizeMetadata(resolved_write_timestamp_ns orelse 0);
                    // Covering values carry the authoritative full-row timestamp
                    // and semantic hash. Prepare/stage fallback index effects only
                    // after those metadata fields have been finalized.
                    if (index_stage) |*stage| if (prepared_relational) |*prepared| {
                        const keys = &prepared_index_keys.?;
                        const row = if (use_preprepared_rows) i else if (apply_schema_view) |view|
                            if (relational_index_snapshot.?.plan.schemaView().epoch == view.epoch)
                                try keys.appendPrepared(prepared)
                            else
                                try keys.appendPreparedFromSchema(prepared, view)
                        else
                            return error.PreparedGenerationChanged;
                        const plan = relational_index_snapshot.?.plan;
                        // Effective writes/deletes are coalesced before this loop,
                        // so the pinned base is the exact prior row for each key.
                        const members = try prior_membership.read(self, &index_read.?, plan, index_ready, write.key);
                        try stage.upsertPreparedWithMembership(&index_writer, plan, keys, row, write.key, index_ready, members);
                    };
                    if (!use_preprepared_rows) {
                        if (prepared_relational) |*prepared|
                            try validateDocumentExtractionInlineSourcesSnapshotParsed(self.alloc, self, fallback_consumer_plan.?.plan().*, prepared.parsedValue(), extracted[i])
                        else
                            try validateDocumentExtractionInlineSources(self, cleaned);
                    }
                    const strip_store_value_start_ns = monotonicTimeNs();
                    const store_value = if (prepared_relational) |*prepared| blk: {
                        const packed_row = prepared.takePackedRow();
                        // A preprepared row stays alive through the ExtractedWrite's
                        // row region. Fallback rows use the DB allocator and retain
                        // the established individual ownership list.
                        if (!use_preprepared_rows) {
                            errdefer self.alloc.free(packed_row);
                            try owned_store_values.append(self.alloc, packed_row);
                        }
                        break :blk packed_row;
                    } else try strippedStoredDocumentValueAlloc(
                        self.alloc,
                        cleaned,
                        vector_store_field_names,
                        &owned_store_values,
                    );
                    if (profile) |active_profile| recordProfileNs(profile, &active_profile.extract_strip_store_value_ns, strip_store_value_start_ns);
                    const store_key = if (use_preprepared_rows) preprepared_effects.?[i].store_key.? else try encodeStoreLookupKeyAlloc(self, self.alloc, write.key);
                    if (!use_preprepared_rows) try owned_store_keys.append(self.alloc, store_key);
                    if (use_preprepared_rows) preprepared_effects.?[i].store_key = null;
                    try overwrite_probe_entries.append(self.alloc, .{
                        .key = store_key,
                        .value = store_value,
                        .write_index = i,
                        .semantic_hash = semantic_hash,
                    });
                    try store_writes.append(self.alloc, .{
                        .key = store_key,
                        .value = store_value,
                    });
                    try identity_upsert_keys.append(self.alloc, write.key);
                    try identity_upsert_write_indexes.append(self.alloc, i);
                    if (shouldWriteTimestamp(write.key)) {
                        const timestamp_start_ns = monotonicTimeNs();
                        const timestamp_key = if (use_preprepared_rows) preprepared_effects.?[i].timestamp_key.? else try makeTimestampKey(self.alloc, write.key);
                        const timestamp_value = if (use_preprepared_rows) preprepared_effects.?[i].timestamp_value.? else blk: {
                            break :blk try encodeTimestampValue(self.alloc, resolved_write_timestamp_ns.?);
                        };
                        try timestamp_writes.append(self.alloc, .{
                            .key = timestamp_key,
                            .value = timestamp_value,
                        });
                        if (use_preprepared_rows) {
                            preprepared_effects.?[i].timestamp_key = null;
                            preprepared_effects.?[i].timestamp_value = null;
                        }
                        if (profile) |active_profile| recordProfileNs(profile, &active_profile.extract_timestamp_ns, timestamp_start_ns);
                    }
                }
            }
            if (profile) |active_profile| {
                active_profile.identity_upsert_keys += @intCast(identity_upsert_keys.items.len);
                active_profile.identity_delete_keys += @intCast(effective_req.deletes.len);
            }
            const identity_capacity_start_ns = monotonicTimeNs();
            if (!self.bulk_identity.enabled or effective_req.deletes.len != 0) {
                try self.failIfIdentityOrdinalExhaustedForNewUpserts(identity_upsert_keys.items);
            }
            if (profile) |active_profile| recordProfileNs(profile, &active_profile.identity_capacity_check_ns, identity_capacity_start_ns);

            var assume_all_new_identity_upserts = false;
            if (self.bulk_identity.enabled and effective_req.deletes.len == 0 and identity_upsert_keys.items.len > 0) {
                assume_all_new_identity_upserts = try self.rememberBulkIngestAllNewIdentityUpserts(identity_upsert_keys.items);
                if (!assume_all_new_identity_upserts) self.clearBulkIngestIdentityAllNewLocked();
            }

            if (!assume_all_new_identity_upserts and overwrite_probe_entries.items.len > 0) {
                const overwrite_probe_start_ns = monotonicTimeNs();
                const unchanged_derived_targets_serviceable = try self.unchangedDerivedReplayTargetsServiceable(self.alloc);
                std.sort.pdq(OverwriteProbeEntry, overwrite_probe_entries.items, {}, overwriteProbeLessThan);
                const probe_keys = overwrite_probe_keys[0..overwrite_probe_entries.items.len];
                const probe_values = overwrite_probe_values[0..overwrite_probe_entries.items.len];
                for (overwrite_probe_entries.items, 0..) |entry, i| {
                    probe_keys[i] = entry.key;
                }
                const overwrite_probe_admission: backend_types.Namespace.BlockCacheAdmission = if (opts.store_batch_options.mode == .bulk_ingest or self.bulkSessionActive()) .transient else .retain;
                var overwrite_probe_txn = try self.core.store.beginProbeTxnWithBlockCacheAdmission(overwrite_probe_admission);
                defer overwrite_probe_txn.abort();
                try overwrite_probe_txn.getManySorted(probe_keys, probe_values);
                for (overwrite_probe_entries.items, 0..) |entry, i| {
                    const existing = probe_values[i] orelse continue;
                    overwritten_flags[entry.write_index] = true;
                    const extracted_write = extracted[entry.write_index];
                    const values_equal = if (entry.semantic_hash) |expected_hash| blk: {
                        const stored_hash = try relational_store.rowSemanticHash(existing);
                        break :blk std.mem.eql(u8, &expected_hash, &stored_hash);
                    } else storedDocumentValuesEqual(self.alloc, existing, entry.value);
                    if (unchanged_derived_targets_serviceable and
                        values_equal and
                        extracted_write.dense_embeddings.len == 0 and
                        extracted_write.sparse_embeddings.len == 0 and
                        extracted_write.graph_writes.len == 0 and
                        extracted_write.mentioned_graph_indexes.len == 0)
                    {
                        derived_changed_flags[entry.write_index] = false;
                    }
                }
                if (profile) |active_profile| recordProfileNs(profile, &active_profile.overwrite_probe_ns, overwrite_probe_start_ns);
            }

            // A document-mode semantic no-op may avoid rewriting the primary value
            // while retaining its timestamp sidecar. AROW carries the authoritative
            // TTL timestamp in its checksummed header, so relational rows must still
            // publish the newly prepared physical row even when derived content is
            // unchanged. The derived journal remains safely elided in both modes.
            for (overwrite_probe_entries.items) |entry| {
                // Prepared retention credits bind the new physical afterimage.
                // Keeping a semantically equal but arbitrarily larger old JSON
                // value while updating TTL would violate that durable bound.
                if (opts.transaction_resolution != null) continue;
                if (derived_changed_flags[entry.write_index]) continue;
                if (internal_keys.isRelationalRowKey(entry.key)) continue;
                semantic_noop_store_keys.putAssumeCapacity(entry.key, {});
            }
            if (semantic_noop_store_keys.count() != 0) {
                var retained: usize = 0;
                for (store_writes.items) |write| {
                    if (semantic_noop_store_keys.contains(write.key)) continue;
                    store_writes.items[retained] = write;
                    retained += 1;
                }
                store_writes.shrinkRetainingCapacity(retained);
            }

            for (effective_req.graph_writes) |graph_write| {
                const generation = (self.core.index_manager.graphIndex(graph_write.index_name) orelse return error.IndexNotFound).config.coverage_generation;
                const graph_entry = self.core.index_manager.graphIndex(graph_write.index_name) orelse return error.IndexNotFound;
                try appendGraphEdgeArtifactWrite(self.alloc, self.core.store, &explicit_graph_artifact_writes, graph_write, generation, graph_entry.ttl_duration_ns, batch_timestamp_ns);
            }

            const cleanup_contract = @import("../graph_cleanup_contract.zig");
            var graph_lifecycle_generation: u64 = if (effective_req.graph_endpoint_cleanup or effective_req.graph_deletes.len != 0 or effective_req.deletes.len != 0 or
                (std.mem.indexOfScalar(bool, derived_changed_flags, true) != null))
                try appendGraphLifecycleGeneration(self.alloc, self.core.store, if (opts.ordered_apply_receipt) |entry| entry.index else 0, &store_writes, &owned_store_values)
            else
                0;

            var changed_graph_artifact_keys = std.ArrayListUnmanaged([]u8).empty;
            defer {
                for (changed_graph_artifact_keys.items) |key| self.alloc.free(key);
                changed_graph_artifact_keys.deinit(self.alloc);
            }
            var changed_graph_artifact_key_set = std.StringHashMapUnmanaged(void).empty;
            defer changed_graph_artifact_key_set.deinit(self.alloc);
            if (opts.restore_staging) |admission| if (admission.projection_page) {
                for (opts.restore_artifacts) |write| if (isMergeArtifactKey(write.key)) {
                    try appendUniqueOwnedKeyIndexed(self.alloc, &changed_graph_artifact_keys, &changed_graph_artifact_key_set, write.key);
                };
            };
            var explicit_graph_write_key_set = std.StringHashMapUnmanaged(void).empty;
            defer explicit_graph_write_key_set.deinit(self.alloc);
            try explicit_graph_write_key_set.ensureTotalCapacity(
                self.alloc,
                std.math.cast(u32, explicit_graph_artifact_writes.items.len) orelse return error.OutOfMemory,
            );
            for (explicit_graph_artifact_writes.items) |write|
                explicit_graph_write_key_set.putAssumeCapacity(write.key, {});
            var graph_delete_key_set = std.StringHashMapUnmanaged(void).empty;
            defer graph_delete_key_set.deinit(self.alloc);
            try graph_delete_key_set.ensureTotalCapacity(
                self.alloc,
                std.math.cast(u32, owned_delete_keys.items.len) orelse return error.OutOfMemory,
            );
            for (owned_delete_keys.items) |key| graph_delete_key_set.putAssumeCapacity(key, {});
            for (graph_artifact_clears.items) |clear| {
                const existing = try collectGraphArtifactsForDocIndex(self.alloc, self.core.store, clear.doc_key, clear.index_name);
                defer docstore_mod.DocStore.freeResults(self.alloc, existing);
                for (existing) |entry| {
                    if (explicit_graph_write_key_set.contains(entry.key)) continue;
                    if (graph_delete_key_set.contains(entry.key)) continue;
                    const owned_key = try self.alloc.dupe(u8, entry.key);
                    try owned_delete_keys.append(self.alloc, owned_key);
                    try graph_delete_key_set.put(self.alloc, owned_key, {});
                    try delete_keys.append(self.alloc, owned_key);
                    try appendUniqueOwnedKeyIndexed(self.alloc, &changed_graph_artifact_keys, &changed_graph_artifact_key_set, entry.key);
                }
            }

            try store_writes.appendSlice(self.alloc, timestamp_writes.items);
            // Scoped restore artifacts share the normal presence, source-index,
            // target-cardinality and journal transaction; extra control writes are
            // appended too late in the pipeline for those derived invariants.
            for (opts.restore_artifacts) |artifact| {
                try store_writes.append(self.alloc, .{ .key = artifact.key, .value = artifact.value });
            }
            for (effective_req.merge_artifacts) |row| {
                if (internal_keys.isGraphEdgeTtlLifetimeKey(row.key) or internal_keys.isGraphEdgeTtlTombstoneKey(row.key)) {
                    var receiver_entry: ?*index_manager_mod.IndexManager.GraphIndex = null;
                    for (self.core.index_manager.graph_indexes.items) |*entry| {
                        if (internal_keys.matchesGraphEdgeContenderIndexName(row.key, entry.config.name)) {
                            receiver_entry = entry;
                            break;
                        }
                    }
                    const entry = receiver_entry orelse return error.IndexNotFound;
                    const key = try internal_keys.rebindGraphEdgeTtlStateKeyGenerationAlloc(self.alloc, row.key, entry.config.coverage_generation);
                    var key_unowned = true;
                    errdefer if (key_unowned) self.alloc.free(key);
                    if (internal_keys.isGraphEdgeTtlLifetimeKey(row.key)) {
                        if (row.value.len != 8) return error.InvalidGraphEdgeTtlLifetime;
                    } else _ = try graph_edge_ttl_tombstone.Tombstone.decode(row.value);
                    try owned_store_keys.append(self.alloc, key);
                    key_unowned = false;
                    try store_writes.append(self.alloc, .{ .key = key, .value = row.value });
                    continue;
                }
                if (internal_keys.isGraphGlobalEdgeContenderKey(row.key)) {
                    const donor_generation = try graph_edge_contender.coverageGeneration(row.value);
                    const contender = (try graph_edge_contender.decode(row.value, donor_generation)) orelse return error.InvalidGraphEdgeContender;
                    var receiver_entry: ?*index_manager_mod.IndexManager.GraphIndex = null;
                    for (self.core.index_manager.graph_indexes.items) |*entry| {
                        if (internal_keys.matchesGraphGlobalEdgeContenderIndexName(row.key, entry.config.name)) {
                            receiver_entry = entry;
                            break;
                        }
                    }
                    const entry = receiver_entry orelse return error.IndexNotFound;
                    const donor_key = try internal_keys.graphGlobalEdgeContenderKeyAlloc(self.alloc, entry.config.name, donor_generation, contender.edge_key, contender.source_priority, contender.state_key);
                    defer self.alloc.free(donor_key);
                    if (!std.mem.eql(u8, donor_key, row.key)) return error.InvalidGraphEdgeContender;
                    var donor_edge = try enrichment_artifact_codec.decodeGraphEdgeAlloc(self.alloc, contender.payload);
                    defer donor_edge.deinit(self.alloc);
                    if (donor_edge.generation != donor_generation) return error.InvalidGraphEdgeContender;
                    const rebound_payload = try enrichment_artifact_codec.encodeGraphEdgeWithTtlAlloc(
                        self.alloc,
                        null,
                        entry.config.coverage_generation,
                        donor_edge.weight,
                        donor_edge.created_at,
                        donor_edge.updated_at,
                        donor_edge.ttl_created_ns,
                        donor_edge.metadata_json,
                    );
                    defer self.alloc.free(rebound_payload);
                    const key = try internal_keys.graphGlobalEdgeContenderKeyAlloc(self.alloc, entry.config.name, entry.config.coverage_generation, contender.edge_key, contender.source_priority, contender.state_key);
                    var key_unowned = true;
                    errdefer if (key_unowned) self.alloc.free(key);
                    const value = try graph_edge_contender.encodeAlloc(self.alloc, entry.config.coverage_generation, contender.source_priority, contender.edge_key, contender.state_key, rebound_payload);
                    var value_unowned = true;
                    errdefer if (value_unowned) self.alloc.free(value);
                    try owned_store_keys.append(self.alloc, key);
                    key_unowned = false;
                    try owned_store_values.append(self.alloc, value);
                    value_unowned = false;
                    try store_writes.append(self.alloc, .{ .key = key, .value = value });
                    // Shared reconciliation below publishes membership, lifetime,
                    // canonical payload and deadline together, including suppression.
                    continue;
                }
                var value = row.value;
                if (internal_keys.isGraphEdgeArtifactKey(row.key)) {
                    const parsed = (try internal_keys.parseGraphEdgeArtifactKeyAlloc(self.alloc, row.key)) orelse return error.InvalidGraphEdgeArtifact;
                    defer {
                        self.alloc.free(parsed.doc_key);
                        self.alloc.free(parsed.index_name);
                        self.alloc.free(parsed.edge_type);
                        self.alloc.free(parsed.target_doc_key);
                        self.alloc.free(parsed.edge_id);
                        self.alloc.free(parsed.logical_source);
                    }
                    if (self.core.index_manager.graphIndex(parsed.index_name)) |entry| {
                        if (entry.ttl_duration_ns != 0 and
                            self.core.index_manager.graphArtifactSources(parsed.index_name).len == 0 and
                            enrichment_artifact_codec.isPortableUnboundGraphEdge(row.value))
                        {
                            const bound = try enrichment_artifact_codec.bindGraphEdgeGenerationAlloc(self.alloc, row.value, entry.config.coverage_generation);
                            var bound_unowned = true;
                            errdefer if (bound_unowned) self.alloc.free(bound);
                            try owned_store_values.append(self.alloc, bound);
                            bound_unowned = false;
                            value = bound;
                        }
                    }
                }
                try store_writes.append(self.alloc, .{ .key = row.key, .value = value });
                try appendDirectGraphTtlDueWrite(self.alloc, self.core.index_manager, .{ .key = row.key, .value = value }, &store_writes, &owned_store_keys, &owned_store_values);
                if (internal_keys.isGraphRetirementKey(row.key)) {
                    const artifact = try internal_keys.graphRetirementArtifactKeyAlloc(self.alloc, row.key);
                    defer self.alloc.free(artifact);
                    try appendUniqueOwnedKeyIndexed(self.alloc, &changed_graph_artifact_keys, &changed_graph_artifact_key_set, artifact);
                } else try appendUniqueOwnedKeyIndexed(self.alloc, &changed_graph_artifact_keys, &changed_graph_artifact_key_set, row.key);
            }
            // Typed online artifact afterimages participate in the same primary,
            // derived replay and page-receipt transaction as ordinary artifacts.
            var transferred_artifact_position_bytes: [@import("artifact_publication.zig").Position.encoded_len]u8 = undefined;
            if (transferred_artifacts.len != 0) {
                const entry = opts.ordered_apply_receipt orelse return error.InvalidMergePage;
                transferred_artifact_position_bytes = try (@import("artifact_publication.zig").Position{ .raft = .{ .term = entry.term, .index = entry.index } }).encode();
            }
            var receiver_artifact_namespace: @import("artifact_publication.zig").Namespace = undefined;
            doc_identity.encodeNamespace(&receiver_artifact_namespace, self.core.identity_namespace);
            for (transferred_artifacts) |effect| {
                const revision_key = @import("artifact_publication.zig").artifactRevisionKey(receiver_artifact_namespace, effect.key);
                const owned_revision_key = try self.alloc.dupe(u8, &revision_key);
                owned_store_keys.append(self.alloc, owned_revision_key) catch |err| {
                    self.alloc.free(owned_revision_key);
                    return err;
                };
                try store_writes.append(self.alloc, .{ .key = owned_revision_key, .value = &transferred_artifact_position_bytes });
                if (effect.value) |value| {
                    try store_writes.append(self.alloc, .{ .key = effect.key, .value = value });
                    // Both thin and materialized journals route these keys to
                    // configured vector consumers. Storing bytes alone would
                    // acknowledge the page without advancing its projections.
                    try appendUniqueOwnedKeyIndexed(self.alloc, &changed_graph_artifact_keys, &changed_graph_artifact_key_set, effect.key);
                }
            }
            for (explicit_embedding_artifact_writes.items) |write| {
                try store_writes.append(self.alloc, .{
                    .key = write.key,
                    .value = write.value,
                });
            }
            for (explicit_graph_artifact_writes.items) |write| {
                const retired = try internal_keys.graphRetirementKeyAlloc(self.alloc, write.key);
                owned_delete_keys.append(self.alloc, retired) catch |err| {
                    self.alloc.free(retired);
                    return err;
                };
                try delete_keys.append(self.alloc, retired);
                try store_writes.append(self.alloc, .{
                    .key = write.key,
                    .value = write.value,
                });
                try appendDirectGraphTtlDueWrite(self.alloc, self.core.index_manager, write, &store_writes, &owned_store_keys, &owned_store_values);
                try appendUniqueOwnedKeyIndexed(self.alloc, &changed_graph_artifact_keys, &changed_graph_artifact_key_set, write.key);
            }
            var deleted_graph_owners = std.StringHashMapUnmanaged(void).empty;
            defer deleted_graph_owners.deinit(self.alloc);
            for (effective_req.deletes) |owner| try deleted_graph_owners.put(self.alloc, owner, {});
            for (effective_req.graph_deletes) |delete| {
                const artifact_key = try internal_keys.graphRelationshipArtifactKeyAlloc(self.alloc, if (delete.owner_document.len > 0) delete.owner_document else if (delete.owner.len > 0) delete.owner else delete.source, delete.index_name, delete.edge_type, delete.target, delete.source, delete.edge_id);
                defer self.alloc.free(artifact_key);
                if (explicit_graph_write_key_set.contains(artifact_key)) continue;
                // Explicit removal overrides retained projection inputs until a
                // new owner lifecycle or exact relationship write revives it.
                const owner = if (delete.owner_document.len > 0) delete.owner_document else if (delete.owner.len > 0) delete.owner else delete.source;
                if (!deleted_graph_owners.contains(owner)) {
                    const retired = try internal_keys.graphRetirementKeyAlloc(self.alloc, artifact_key);
                    owned_store_keys.append(self.alloc, retired) catch |err| {
                        self.alloc.free(retired);
                        return err;
                    };
                    const retirement_generation = if (effective_req.graph_endpoint_cleanup) blk: {
                        for (effective_req.graph_endpoint_cleanup_guards) |guard| if (guard.kind == .endpoint and std.mem.eql(u8, guard.endpoint, delete.target)) break :blk guard.generation;
                        break :blk graph_lifecycle_generation;
                    } else graph_lifecycle_generation;
                    const stamp = if (retirement_generation != 0) cleanup_contract.retirementValue(retirement_generation) else undefined;
                    const value = try self.alloc.dupe(u8, if (retirement_generation != 0) &stamp else "1");
                    owned_store_values.append(self.alloc, value) catch |err| {
                        self.alloc.free(value);
                        return err;
                    };
                    try store_writes.append(self.alloc, .{ .key = retired, .value = value });
                }
                if (graph_delete_key_set.contains(artifact_key)) continue;
                const owned_key = try self.alloc.dupe(u8, artifact_key);
                try owned_delete_keys.append(self.alloc, owned_key);
                try graph_delete_key_set.put(self.alloc, owned_key, {});
                try delete_keys.append(self.alloc, owned_key);
                try appendUniqueOwnedKeyIndexed(self.alloc, &changed_graph_artifact_keys, &changed_graph_artifact_key_set, artifact_key);
            }
            for (effective_req.deletes) |key| {
                const store_key = try encodeStoreLookupKeyAlloc(self, self.alloc, key);
                try owned_delete_keys.append(self.alloc, store_key);
                try delete_keys.append(self.alloc, store_key);
                if (shouldWriteTimestamp(key)) {
                    const timestamp_key = try makeTimestampKey(self.alloc, key);
                    try timestamp_delete_keys.append(self.alloc, timestamp_key);
                    try delete_keys.append(self.alloc, timestamp_key);
                }
            }
            if (profile) |active_profile| recordProfileNs(profile, &active_profile.extract_writes_ns, extract_writes_start_ns);

            var owner_job_writes = std.ArrayListUnmanaged(docstore_mod.KVPair).empty;
            defer owner_job_writes.deinit(self.alloc);
            if (self.core.index_manager.hasGraphIndexes()) for (effective_req.writes, 0..) |write, write_index| {
                if (!derived_changed_flags[write_index] or isMetadataKey(write.key)) continue;
                const job_key = try cleanup_contract.ownerJobKeyAlloc(self.alloc, write.key);
                var key_owned = true;
                defer if (key_owned) self.alloc.free(job_key);
                const pending = self.core.store.get(self.alloc, job_key) catch |err| switch (err) {
                    error.NotFound => null,
                    else => return err,
                };
                defer if (pending) |value| self.alloc.free(value);
                var needs_revival = pending != null;
                if (!needs_revival and try self.core.store.mayHaveGraphRetirements()) {
                    const prefix = try internal_keys.graphRetirementPrefixAlloc(self.alloc, write.key);
                    defer self.alloc.free(prefix);
                    // One prefix seek proves presence; never enumerate owner history
                    // or retained artifact inputs in the foreground mutation.
                    const markers = try self.core.store.scanPrefixKeysPage(self.alloc, prefix, null, 1);
                    defer freeOwnedKeySlice(self.alloc, markers);
                    needs_revival = markers.len != 0;
                }
                if (!needs_revival) continue;
                try owned_store_keys.append(self.alloc, job_key);
                key_owned = false;
                const value = try cleanup_contract.encodeOwnerJobAlloc(self.alloc, .{ .owner = write.key, .generation = graph_lifecycle_generation });
                owned_store_values.append(self.alloc, value) catch |err| {
                    self.alloc.free(value);
                    return err;
                };
                // Publish the new lifecycle before fresh graph artifact writes in
                // this transaction; older retirement stamps then cease suppression.
                try owner_job_writes.append(self.alloc, .{ .key = job_key, .value = value });
            };
            // Prepend all lifecycle changes once so large owner batches remain
            // linear in their write count rather than repeatedly moving the tail.
            try store_writes.insertSlice(self.alloc, 0, owner_job_writes.items);

            const delete_artifacts_start_ns = monotonicTimeNs();
            const deleted_artifact_keys = try collectEnrichmentArtifactDeletesForBatch(
                self,
                effective_req,
                transferred_artifacts,
                extracted[0..extracted_initialized],
                &delete_keys,
                &owned_delete_keys,
            );
            defer self.alloc.free(deleted_artifact_keys);
            // Native transfer pages can delete relationship artifacts without a
            // document or tuple delete. Classify their prepared effects too, so
            // every retirement receives this commit's nonzero lifecycle stamp.
            if (graph_lifecycle_generation == 0) for (deleted_artifact_keys) |artifact| {
                if (internal_keys.graphInlineTargetComponent(artifact) == null) continue;
                graph_lifecycle_generation = try appendGraphLifecycleGeneration(self.alloc, self.core.store, if (opts.ordered_apply_receipt) |entry| entry.index else 0, &store_writes, &owned_store_values);
                break;
            };
            try appendGraphEndpointRetirements(self.alloc, self.core.store, graph_lifecycle_generation, effective_req.deletes, self.core.index_manager.hasGraphIndexes(), deleted_artifact_keys, &store_writes, &owned_store_keys, &owned_store_values);
            if (profile) |active_profile| recordProfileNs(profile, &active_profile.delete_artifacts_ns, delete_artifacts_start_ns);

            const use_thin_replay_fast_path =
                effective_req.sync_level != .full_text and
                effective_req.sync_level != .enrichments and
                effective_req.sync_level != .full_index and
                !splitShadowRequiresMaterializedDerivedBatch(self);
            const include_generated_enrichment_hint = use_thin_replay_fast_path and
                self.core.hasGeneratedEnrichmentTargets();

            var precomputed_generated: PrecomputedGeneratedBatch = .{};
            defer precomputed_generated.deinit(preparation_alloc);
            // Whether this batch's own rows needed nothing further from the
            // enrichment runtime: every generated-enrichment request for them was
            // precomputed and folded into store_writes above, under this same
            // apply-lock hold. Set after the precompute block below, before
            // precomputed_generated.generated_enrichment_refs is transferred into
            // materialized_derived_batch (which zeroes it). A sync_level
            // .enrichments write with this true does not owe the enrichment
            // runtime anything from its own commit, so it can skip waiting on the
            // table-wide applied watermark below -- unrelated backlog queued
            // ahead of this write's sequence is not this write's debt to drain.
            var enrichment_fully_precomputed = false;
            var remote_child_range_dispatches = std.ArrayListUnmanaged(DocumentChildRangeDispatchGroup).empty;
            defer {
                for (remote_child_range_dispatches.items) |*dispatch| dispatch.deinit(preparation_alloc);
                remote_child_range_dispatches.deinit(preparation_alloc);
            }
            var owned_child_range_outbox_keys = std.ArrayListUnmanaged([]u8).empty;
            defer {
                for (owned_child_range_outbox_keys.items) |key| self.alloc.free(key);
                owned_child_range_outbox_keys.deinit(self.alloc);
            }
            var owned_child_range_outbox_values = std.ArrayListUnmanaged([]u8).empty;
            defer {
                for (owned_child_range_outbox_values.items) |value| self.alloc.free(value);
                owned_child_range_outbox_values.deinit(self.alloc);
            }
            if (!use_thin_replay_fast_path) {
                const precompute_generated_start_ns = monotonicTimeNs();
                if (use_preprepared_rows and preprepared_generated != null) {
                    precomputed_generated = preprepared_generated.?;
                    preprepared_generated = null;
                } else if (use_preprepared_document_generated) {
                    precomputed_generated = preprepared_document_generated.?;
                    preprepared_document_generated = null;
                } else {
                    precomputed_generated = try prepareGeneratedEnrichments(
                        self,
                        preparation_alloc,
                        effective_req,
                        extracted[0..extracted_initialized],
                        generatedPrecomputeModeForSyncLevel(effective_req.sync_level),
                        opts.force_generated_artifact_names,
                        null,
                        generated_memo,
                    );
                }

                if (opts.document_child_range_dispatcher) |dispatcher| {
                    try document_child_range_effects.partitionRemoteDocumentChildRangeGeneratedBatch(childRangeManifestReader(self), preparation_alloc, &precomputed_generated, &remote_child_range_dispatches, .{ .ptr = dispatcher.ptr, .select = dispatcher.select_destination });
                }

                for (precomputed_generated.artifact_writes) |write| {
                    try store_writes.append(self.alloc, .{
                        .key = write.key,
                        .value = write.value,
                    });
                    try appendUniqueOwnedKeyIndexed(self.alloc, &changed_graph_artifact_keys, &changed_graph_artifact_key_set, write.key);
                }
                for (precomputed_generated.artifact_delete_keys) |key| {
                    try delete_keys.append(self.alloc, key);
                    if (internal_keys.isAssetArtifactKey(key)) {
                        try appendUniqueOwnedKeyIndexed(self.alloc, &changed_graph_artifact_keys, &changed_graph_artifact_key_set, key);
                    }
                }
                if (profile) |active_profile| recordProfileNs(profile, &active_profile.precompute_generated_ns, precompute_generated_start_ns);
                enrichment_fully_precomputed = effective_req.sync_level == .enrichments and
                    precomputed_generated.generated_enrichment_refs.len == 0;
            }
            if (effective_req.merge_artifacts.len > 0 or (if (req.merge_page) |page| page.artifact_effects.len != 0 else false) or explicit_embedding_artifact_writes.items.len > 0 or
                explicit_graph_artifact_writes.items.len > 0 or
                precomputed_generated.artifact_writes.len > 0)
            {
                try self.core.appendArtifactPresenceMarker(&store_writes);
            }
            try appendAssetArtifactSourceIndexMutations(
                self.alloc,
                &store_writes,
                deleted_artifact_keys,
                &delete_keys,
                &owned_store_keys,
                &owned_store_values,
                &owned_delete_keys,
            );
            try appendMixedDirectGraphContenderMutations(
                self.alloc,
                self.core.store,
                self.core.index_manager,
                explicit_graph_artifact_writes.items,
                &store_writes,
                &delete_keys,
                &owned_store_keys,
                &owned_store_values,
                &owned_delete_keys,
            );
            if (effective_req.merge_artifacts.len != 0) try appendImportedGraphContenderMutations(
                self.alloc,
                self.core.store,
                self.core.index_manager,
                effective_req.merge_artifacts,
                &store_writes,
                &delete_keys,
                &owned_store_keys,
                &owned_store_values,
                &owned_delete_keys,
                &changed_graph_artifact_keys,
                &changed_graph_artifact_key_set,
            );
            try appendRetiredDirectGraphTtlDueDeletes(
                self.alloc,
                self.core.store,
                self.core.index_manager,
                store_writes.items,
                delete_keys.items,
                &delete_keys,
                &owned_delete_keys,
            );

            var sync_targets: ManagedSyncTargets = .{};
            defer sync_targets.deinit(self.alloc);
            var split_shadow_ticket: ?u64 = null;
            var materialized_derived_batch: ?derived_types.DerivedBatch = null;
            defer if (materialized_derived_batch) |*materialized_batch| derived_types.deinitDerivedBatch(preparation_alloc, materialized_batch);
            var materialized_deleted_artifact_keys = std.ArrayListUnmanaged([]const u8).empty;
            defer materialized_deleted_artifact_keys.deinit(self.alloc);
            if (!use_thin_replay_fast_path) {
                var seen_deleted_artifact_keys = std.StringHashMapUnmanaged(void).empty;
                defer seen_deleted_artifact_keys.deinit(self.alloc);
                for (deleted_artifact_keys) |key| try appendUniqueBorrowedKeyWithSet(self.alloc, &materialized_deleted_artifact_keys, &seen_deleted_artifact_keys, key);
                for (precomputed_generated.artifact_delete_keys) |key| try appendUniqueBorrowedKeyWithSet(self.alloc, &materialized_deleted_artifact_keys, &seen_deleted_artifact_keys, key);
            }
            // An exact source retry that only refreshes timestamp metadata has no
            // derived visibility transition. Reuse the current committed fence
            // instead of reserving a sequence that would either be lost (and make
            // the in-memory generation move backwards after restart) or require a
            // synthetic replay item that every managed consumer must drain.
            const elide_semantic_noop_replay = use_thin_replay_fast_path and
                opts.extra_store_writes.len == 0 and
                opts.transaction_resolution == null and
                opts.replication_applied_lsn_marker == null and
                opts.ordered_apply_receipt == null and
                !thinReplayInputsHaveDerivedWork(
                    effective_req,
                    deleted_artifact_keys,
                    changed_graph_artifact_keys.items,
                    derived_changed_flags,
                );
            const sequence = if (elide_semantic_noop_replay)
                self.core.nextDerivedSequence()
            else
                self.core.reserveDerivedAppendSequence();
            if (profile) |active_profile| active_profile.source_sequence = sequence;
            const identity_metadata_start_ns = monotonicTimeNs();
            const identity_live_before = if (identity_upsert_keys.items.len != 0 or effective_req.deletes.len != 0)
                (try doc_identity.fastStatsFromStore(self.core.store)).live_ordinals
            else
                0;
            var used_trusted_identity_path = false;
            if (effective_req.deletes.len != 0) {
                self.clearBulkIngestIdentityAllNewLocked();
            }
            if (self.bulk_identity.enabled and
                effective_req.deletes.len == 0 and
                identity_upsert_keys.items.len > 0 and
                (assume_all_new_identity_upserts or identityUpsertStoreWritesAreNew(identity_upsert_write_indexes.items, overwritten_flags)))
            {
                used_trusted_identity_path = try doc_identity.appendBatchIdentityMetadataAllNewTrustedStateWithVisibilityDeletesForNamespaceAlloc(
                    self.alloc,
                    self.core.identity_namespace,
                    sequence,
                    &identity_writes,
                    &identity_visibility_deletes,
                    identity_upsert_keys.items,
                    &self.bulk_identity.trusted,
                );
                if (!used_trusted_identity_path) self.clearBulkIngestIdentityAllNewLocked();
            }
            if (!used_trusted_identity_path) {
                try doc_identity.appendBatchIdentityMetadataForNamespaceWithVisibilityDeletesAlloc(
                    self.alloc,
                    self.core.store,
                    self.core.identity_namespace,
                    sequence,
                    &identity_writes,
                    &identity_visibility_deletes,
                    identity_upsert_keys.items,
                    effective_req.deletes,
                );
            }
            if (profile) |active_profile| {
                recordProfileNs(profile, &active_profile.identity_metadata_ns, identity_metadata_start_ns);
                active_profile.identity_metadata_writes += @intCast(identity_writes.items.len);
            }
            const pending_identity_visibility_summary = try doc_identity.visibilitySummaryFromWrites(identity_writes.items);
            // Plan once under the apply lock: counts, page state, and the durable
            // transition must agree on whether a delayed checkpoint has authority.
            const merge_existing_raw = if (req.merge_checkpoint != null) try merge_state_mod.loadRawAlloc(self.alloc, self.core.store) else null;
            defer if (merge_existing_raw) |value| self.alloc.free(value);
            var merge_existing_state: ?merge_state_mod.State = if (merge_existing_raw) |value| try merge_state_mod.decodeAlloc(self.alloc, value) else null;
            defer if (merge_existing_state) |*state| state.deinit(self.alloc);
            const merge_plan: ?merge_state_mod.ApplyPlan = if (req.merge_checkpoint) |checkpoint| try merge_state_mod.planCheckpointApply(self.alloc, if (merge_existing_state) |*state| state else null, self.core.byteRange(), checkpoint) else null;
            defer if (merge_plan) |plan| plan.deinit(self.alloc);
            const merge_count_prepared = try @import("merge_cardinality.zig").prepare(self.alloc, self.core.store, effective_req, if (merge_plan) |plan| plan.applies_checkpoint else false, identity_upsert_keys.items, effective_req.deletes, identity_live_before, if (pending_identity_visibility_summary) |summary| summary.live_ordinals else identity_live_before, &identity_writes);
            if (!merge_count_prepared) if (pending_identity_visibility_summary) |summary| {
                try range_cardinality.appendIdentityTransitionAlloc(
                    self.alloc,
                    self.core.store,
                    self.core.byteRange(),
                    identity_live_before,
                    summary.live_ordinals,
                    &identity_writes,
                );
            };
            var next_table_catalog: ?table_catalog_mod.Catalog = null;
            var table_catalog_value: [table_catalog_mod.encoded_len]u8 = undefined;
            if (pending_identity_visibility_summary != null or merge_count_prepared) {
                var catalog = self.core.table_catalog;
                const previous = catalog;
                catalog.mode_initialized = true;
                if (self.core.schema) |table_schema| {
                    catalog.storage_mode = table_schema.storage_mode;
                    catalog.active_schema_version = table_schema.version;
                } else {
                    catalog.storage_mode = .document;
                    catalog.active_schema_version = 0;
                }
                // Persist only the empty/non-empty transition. Exact cardinality
                // is already transactional in the range-local counter. A split
                // intentionally retains foreign identities in the shared namespace.
                catalog.row_count = @intFromBool(try range_cardinality.afterIdentityTransition(self.alloc, self.core.store, identity_writes.items) != 0);
                catalog.reconciled = true;
                if (catalog.mode_initialized != previous.mode_initialized or
                    catalog.storage_mode != previous.storage_mode or
                    catalog.active_schema_version != previous.active_schema_version or
                    catalog.row_count != previous.row_count or
                    catalog.reconciled != previous.reconciled)
                {
                    catalog.generation +|= 1;
                    table_catalog_value = catalog.encode();
                    next_table_catalog = catalog;
                }
            } else if (self.core.table_catalog.row_count == 0 and (effective_req.writes.len != 0 or effective_req.deletes.len != 0)) {
                // An older/unreconciled owner can contain primary rows but lack a
                // visibility summary. Absence of a counter is not an empty-table
                // proof: a write proves presence, while a delete keeps it unknown
                // unless one bounded user-namespace probe proves the prior root
                // empty. Never grant empty-table CHECK/rewrite admission from zero
                // metadata on a populated root. A later explicit reconciliation
                // may clear this conservative presence bit after delete-only work.
                const has_user_data = effective_req.writes.len != 0 or try storeHasUserDataBounded(self.core.store);
                if (has_user_data) {
                    var catalog = self.core.table_catalog;
                    catalog.mode_initialized = true;
                    catalog.storage_mode = if (self.core.schema) |table_schema| table_schema.storage_mode else .document;
                    catalog.active_schema_version = if (self.core.schema) |table_schema| table_schema.version else 0;
                    catalog.row_count = 1;
                    catalog.generation +|= 1;
                    table_catalog_value = catalog.encode();
                    next_table_catalog = catalog;
                }
            }
            try store_writes.appendSlice(self.alloc, identity_writes.items);
            if (next_table_catalog != null) try store_writes.append(self.alloc, .{
                .key = table_catalog_mod.key,
                .value = &table_catalog_value,
            });
            try appendDocumentChildRangeOutboxWrites(
                self.alloc,
                sequence,
                remote_child_range_dispatches.items,
                effective_req.sync_level,
                &store_writes,
                &owned_child_range_outbox_keys,
                &owned_child_range_outbox_values,
            );
            const build_derived_start_ns = monotonicTimeNs();
            const replay_payload = if (use_thin_replay_fast_path)
                try encodeThinReplayRecordPayload(
                    self.alloc,
                    effective_req,
                    extracted[0..extracted_initialized],
                    deleted_artifact_keys,
                    changed_graph_artifact_keys.items,
                    overwritten_flags,
                    derived_changed_flags,
                    sequence,
                    include_generated_enrichment_hint,
                    self.core.index_manager,
                    &sync_targets,
                    self.local_execution.row_policy_table_name,
                )
            else blk: {
                materialized_derived_batch = try buildDerivedBatch(
                    preparation_alloc,
                    effective_req,
                    extracted[0..extracted_initialized],
                    materialized_deleted_artifact_keys.items,
                    changed_graph_artifact_keys.items,
                );
                for (materialized_derived_batch.?.overwritten_doc_keys) |key| preparation_alloc.free(@constCast(key));
                if (materialized_derived_batch.?.overwritten_doc_keys.len > 0) preparation_alloc.free(materialized_derived_batch.?.overwritten_doc_keys);
                materialized_derived_batch.?.overwritten_doc_keys = &.{};
                materialized_derived_batch.?.overwritten_doc_keys = try buildOverwrittenDocKeys(preparation_alloc, effective_req.writes, overwritten_flags);
                materialized_derived_batch.?.documents = try takeOwnedSlice(derived_types.DerivedDocument, preparation_alloc, materialized_derived_batch.?.documents, &precomputed_generated.documents);
                materialized_derived_batch.?.dense_embeddings = try takeOwnedSlice(derived_types.DerivedDenseEmbeddingWrite, preparation_alloc, materialized_derived_batch.?.dense_embeddings, &precomputed_generated.dense_embeddings);
                materialized_derived_batch.?.sparse_embeddings = try takeOwnedSlice(derived_types.DerivedSparseEmbeddingWrite, preparation_alloc, materialized_derived_batch.?.sparse_embeddings, &precomputed_generated.sparse_embeddings);
                materialized_derived_batch.?.generated_enrichment_refs = precomputed_generated.generated_enrichment_refs;
                precomputed_generated.generated_enrichment_refs = &.{};
                materialized_derived_batch.?.sequence = sequence;
                var record = try prepareDirectChangeRecord(&self.batchContext(), materialized_derived_batch.?, sequence, effective_req, self.local_execution.row_policy_table_name);
                defer change_journal_mod.deinitRecord(self.alloc, &record);
                const payload = try change_journal_mod.encodeRecord(self.alloc, record);
                errdefer self.alloc.free(payload);
                if (write_plan_snapshot == null or write_plan_snapshot.?.plan().has_text_consumers or splitShadowRequiresMaterializedDerivedBatch(self))
                    try attachPreparedUpsertDocumentProjections(preparation_alloc, &materialized_derived_batch.?, effective_req, extracted[0..extracted_initialized]);

                const collect_sync_targets_start_ns = monotonicTimeNs();
                sync_targets = try collectManagedSyncTargetsForRecordWithBatch(self.alloc, self.core.index_manager, record, materialized_derived_batch.?);
                if (profile) |active_profile| recordProfileNs(profile, &active_profile.collect_sync_targets_ns, collect_sync_targets_start_ns);
                break :blk payload;
            };
            defer self.alloc.free(replay_payload);
            const append_derived_replay = !opts.suppress_derived_replay_append and
                !elide_semantic_noop_replay;
            if (append_derived_replay) {
                try appendArtifactSourceRevisionWritesFromReplay(
                    self.alloc,
                    replay_payload,
                    sequence,
                    &store_writes,
                    &owned_store_keys,
                    &owned_store_values,
                );
            }
            var durable_replication_batch_payload: ?[]u8 = null;
            var durable_replication_replay_payload: ?[]u8 = null;
            var durable_replication_batch_outbox_key: ?[]const u8 = null;
            var durable_replication_replay_outbox_key: ?[]const u8 = null;
            if (!opts.bypass_replication_write_gate and (opts.transaction_resolution == null or scoped_replication)) {
                if (self.local_execution.replication_async_batch_mirror) |mirror| if (scoped_replication or replicationMirrorRequiresDurableOutbox(mirror)) {
                    const payload = preencoded_replication_batch_payload orelse return error.ReplicationPublisherUnavailable;
                    const from_lsn = mirror.publisher.nextLsn();
                    const outbox = try encodeDurableReplicationOutboxAlloc(self.alloc, from_lsn, payload);
                    owned_store_values.append(self.alloc, outbox) catch |err| {
                        self.alloc.free(outbox);
                        return err;
                    };
                    const outbox_key = try durableReplicationOutboxKeyAlloc(
                        self.alloc,
                        if (scoped_replication) .restore_batch else .batch,
                        from_lsn,
                        self.core.root_generation,
                        outbox[replication_outbox_header_len .. outbox.len - replication_outbox_checksum_len],
                    );
                    var outbox_key_owned = true;
                    errdefer if (outbox_key_owned) self.alloc.free(outbox_key);
                    try owned_store_keys.append(self.alloc, outbox_key);
                    outbox_key_owned = false;
                    try store_writes.append(self.alloc, .{ .key = outbox_key, .value = outbox });
                    durable_replication_batch_payload = outbox[replication_outbox_header_len .. outbox.len - replication_outbox_checksum_len];
                    durable_replication_batch_outbox_key = outbox_key;
                };
                if (append_derived_replay and !scoped_replication) if (self.local_execution.replication_async_effect_mirror) |mirror| if (replicationMirrorRequiresDurableOutbox(mirror)) {
                    const from_lsn = mirror.publisher.nextLsn();
                    const outbox = try encodeDurableReplicationOutboxAlloc(self.alloc, from_lsn, replay_payload);
                    owned_store_values.append(self.alloc, outbox) catch |err| {
                        self.alloc.free(outbox);
                        return err;
                    };
                    const outbox_key = try durableReplicationOutboxKeyAlloc(
                        self.alloc,
                        .replay,
                        from_lsn,
                        self.core.root_generation,
                        outbox[replication_outbox_header_len .. outbox.len - replication_outbox_checksum_len],
                    );
                    var outbox_key_owned = true;
                    errdefer if (outbox_key_owned) self.alloc.free(outbox_key);
                    try owned_store_keys.append(self.alloc, outbox_key);
                    outbox_key_owned = false;
                    try store_writes.append(self.alloc, .{ .key = outbox_key, .value = outbox });
                    durable_replication_replay_payload = outbox[replication_outbox_header_len .. outbox.len - replication_outbox_checksum_len];
                    durable_replication_replay_outbox_key = outbox_key;
                };
            }
            if (profile) |active_profile| recordProfileNs(profile, &active_profile.build_derived_ns, build_derived_start_ns);

            const store_write_start_ns = monotonicTimeNs();
            if (opts.row_policy_lease) |lease| try lease.checkAt(@intCast(@divFloor(platform_time.realtimeNs(), std.time.ns_per_s)));
            const store_batch_options: backend_types.BatchOptions = if (opts.store_batch_options.mode != .default)
                opts.store_batch_options
            else if (self.bulkSessionActive())
                .{ .mode = .bulk_ingest, .defer_commit_flush = true }
            else
                .{};
            var replication_applied_lsn_value_buf: [replication_applied_lsn_value_len]u8 = undefined;
            if (opts.replication_applied_lsn_marker) |lsn| {
                if (lsn != 0) {
                    try store_writes.append(self.alloc, replicationAppliedSequenceWrite(lsn, &replication_applied_lsn_value_buf));
                }
            }
            var ordered_apply_receipt_value_buf: [ordered_apply_receipt_value_len]u8 = undefined;
            const ordered_apply_receipt_write: ?docstore_mod.KVPair = if (opts.ordered_apply_receipt) |identity|
                orderedApplyReceiptWrite(identity, &ordered_apply_receipt_value_buf)
            else
                null;
            // A transaction resolution owns a second idempotency boundary: normal
            // document writes must not be replayed once its decision is terminal,
            // while the command receipt and participant acknowledgement still
            // must be completed. Keep those completion writes separate.
            if (ordered_apply_receipt_write) |write| {
                if (opts.transaction_resolution == null) try store_writes.append(self.alloc, write);
            }
            var split_range_value: ?[]u8 = null;
            defer if (split_range_value) |value| self.alloc.free(value);
            var integrity_coverage_value: ?[]u8 = null;
            defer if (integrity_coverage_value) |value| self.alloc.free(value);
            var merge_range_value: ?[]u8 = null;
            defer if (merge_range_value) |value| self.alloc.free(value);
            var merge_state_value = std.ArrayListUnmanaged(u8).empty;
            defer merge_state_value.deinit(self.alloc);
            if (merge_page_value) |value| try store_writes.append(self.alloc, .{ .key = @import("merge_page_contract.zig").key, .value = value });
            var ordered_artifact_value: ?[]u8 = null;
            defer if (ordered_artifact_value) |value| self.alloc.free(value);
            if (req.artifact_catalog) |command| {
                const entry = opts.ordered_apply_receipt orelse return error.InvalidArtifactCatalogCommand;
                var read = try self.core.store.beginReadTxn();
                defer read.abort();
                if (!std.mem.eql(u8, &command.namespace, &@import("online_source_contract.zig").namespaceBytes(self.core.identity_namespace))) return error.IdentityNamespaceMismatch;
                if (!std.mem.eql(u8, &command.binding.digest, &(try @import("artifact_inventory.zig").local(&read)).digest)) return error.ArtifactCatalogDrift;
                ordered_artifact_value = try @import("artifact_inventory.zig").prepareOrdered(self.alloc, &read, command, entry.index);
                if (ordered_artifact_value) |value| try store_writes.append(self.alloc, .{ .key = @import("artifact_inventory.zig").ordered_key, .value = value });
                if (try @import("artifact_reconcile_intent.zig").validateCompletion(self.alloc, &read, command, entry.index)) try delete_keys.append(self.alloc, @import("artifact_reconcile_intent.zig").key);
                try delete_keys.append(self.alloc, @import("artifact_reconcile_intent.zig").resolver_cursor_key);
            }
            var split_sequence_buf: [8]u8 = undefined;
            var split_marker_buf: [4 * @sizeOf(u64) + 1]u8 = undefined;
            var persisted_range: ?types.ByteRange = null;
            var persisted_range_start_owned: ?[]u8 = null;
            defer if (persisted_range_start_owned) |value| self.alloc.free(value);
            var persisted_range_end_owned: ?[]u8 = null;
            defer if (persisted_range_end_owned) |value| self.alloc.free(value);
            if (req.split_replication) |replication| {
                if (replication.operation == .delta) {
                    try store_writes.append(self.alloc, .{
                        .key = range_state_mod.split_delta_final_seq_key,
                        .value = range_state_mod.encodeSplitDeltaFinalSeq(replication.sequence, &split_sequence_buf),
                    });
                }
            }
            if (req.split_checkpoint) |checkpoint| {
                if (coordinated_handoff and checkpoint.kind == .destination_complete) {
                    const raw_catalog = (try self.core.getStoreValue(self.alloc, @import("relational_integrity_catalog.zig").key)) orelse return error.IntegrityCatalogChanged;
                    defer self.alloc.free(raw_catalog);
                    integrity_coverage_value = try @import("relational_integrity_handoff.zig").coverageForRange(self.alloc, raw_catalog, self.core.identity_namespace, .{ .start = checkpoint.range_start, .end = checkpoint.range_end });
                    try store_writes.append(self.alloc, .{ .key = @import("relational_integrity_activation.zig").key, .value = integrity_coverage_value.? });
                }
                if (checkpoint.kind != .source_ack) {
                    persisted_range = .{ .start = checkpoint.range_start, .end = checkpoint.range_end };
                    persisted_range_start_owned = try self.alloc.dupe(u8, checkpoint.range_start);
                    persisted_range_end_owned = try self.alloc.dupe(u8, checkpoint.range_end);
                    split_range_value = try range_state_mod.encodeRangeAlloc(self.alloc, persisted_range.?);
                    try store_writes.append(self.alloc, .{
                        .key = range_state_mod.range_key,
                        .value = split_range_value.?,
                    });
                }
                try store_writes.append(self.alloc, .{
                    .key = range_state_mod.split_delta_final_seq_key,
                    .value = range_state_mod.encodeSplitDeltaFinalSeq(checkpoint.delta_sequence, &split_sequence_buf),
                });
                try store_writes.append(self.alloc, .{
                    .key = range_state_mod.split_bootstrap_marker_key,
                    .value = range_state_mod.encodeSplitBootstrapMarker(.{
                        .transition_id = checkpoint.transition_id,
                        .attempt_epoch = checkpoint.attempt_epoch,
                        .source_group_id = checkpoint.source_group_id,
                        .destination_group_id = checkpoint.destination_group_id,
                        .bootstrap_complete = checkpoint.kind == .destination_complete,
                    }, &split_marker_buf),
                });
            }
            if (req.merge_checkpoint) |checkpoint| {
                if (checkpoint.receiver_identity_reassignment_namespace) |namespace| {
                    if (!checkpoint.allow_doc_identity_reassignment or
                        !self.core.identity_namespace.eql(namespace))
                        return error.DocIdentityNamespaceMismatch;
                } else if (checkpoint.allow_doc_identity_reassignment) {
                    return error.InvalidBatchRequest;
                }
                const plan = merge_plan.?;
                const pages = @import("merge_page_contract.zig");
                if ((checkpoint.page_source != null) != (checkpoint.page_receiver_namespace != null) or
                    (checkpoint.page_source != null and checkpoint.kind != .begin_copy and !(checkpoint.kind == .accept and (checkpoint.page_source.?.integrity != null or checkpoint.page_source.?.artifact_catalog != null)))) return error.InvalidMergeCheckpoint;
                const page_raw = try self.core.getStoreValue(self.alloc, pages.key);
                defer if (page_raw) |value| self.alloc.free(value);
                var page_progress = if (page_raw) |value| try pages.decode(self.alloc, value) else null;
                defer if (page_progress) |*value| value.deinit();
                const page_plan = try pages.checkpointPlan(merge_existing_state, plan.state, checkpoint, if (page_progress) |value| value.value else null);
                switch (page_plan) {
                    .unchanged => {},
                    .clear => {
                        try delete_keys.append(self.alloc, pages.key);
                        try delete_keys.append(self.alloc, @import("merge_artifact_catalog.zig").key);
                    },
                    .bind => |progress| {
                        if (!progress.receiver_namespace.eql(self.core.identity_namespace)) return error.DocIdentityNamespaceMismatch;
                        merge_page_value = try pages.encode(self.alloc, progress);
                        try store_writes.append(self.alloc, .{ .key = pages.key, .value = merge_page_value.? });
                        merge_source_catalog_value = try @import("merge_artifact_catalog.zig").encode(self.alloc, checkpoint, progress);
                        if (merge_source_catalog_value) |value| {
                            try store_writes.append(self.alloc, .{ .key = @import("merge_artifact_catalog.zig").key, .value = value });
                        } else try delete_keys.append(self.alloc, @import("merge_artifact_catalog.zig").key);
                    },
                }
                if (page_plan != .unchanged) {
                    const chunks = @import("merge_page_chunks.zig");
                    const manifest = self.core.store.get(self.alloc, chunks.manifest_key) catch |err| switch (err) {
                        error.NotFound => null,
                        else => return err,
                    };
                    defer if (manifest) |value| self.alloc.free(value);
                    if (manifest) |value| {
                        const count = try chunks.slotCount(value);
                        for (0..count) |i| {
                            const slot_key = try chunks.slotKey(self.alloc, @intCast(i));
                            owned_store_keys.append(self.alloc, slot_key) catch |err| {
                                self.alloc.free(slot_key);
                                return err;
                            };
                            try delete_keys.append(self.alloc, slot_key);
                        }
                        try delete_keys.append(self.alloc, chunks.manifest_key);
                    }
                }
                persisted_range = plan.range;
                persisted_range_start_owned = try self.alloc.dupe(u8, plan.range.start);
                persisted_range_end_owned = try self.alloc.dupe(u8, plan.range.end);
                merge_range_value = try range_state_mod.encodeRangeAlloc(self.alloc, plan.range);
                try store_writes.append(self.alloc, .{
                    .key = range_state_mod.range_key,
                    .value = merge_range_value.?,
                });
                try merge_state_mod.encode(&merge_state_value, self.alloc, plan.state);
                try store_writes.append(self.alloc, .{
                    .key = merge_state_mod.key,
                    .value = merge_state_value.items,
                });
                try delete_keys.append(self.alloc, merge_state_mod.legacy_key);
                const shadow = @import("online_integrity_shadow.zig");
                if (plan.applies_checkpoint and (checkpoint.kind == .begin_copy or (checkpoint.kind == .accept and plan.state.copy_attempt.sequence == 0)) and plan.state.phase == .accepting and checkpoint.page_source != null and checkpoint.page_source.?.integrity != null) {
                    const shadow_state = try std.json.Stringify.valueAlloc(self.alloc, shadow.State{ .source = checkpoint.page_source.?, .context = .{ .transition_id = checkpoint.transition_id, .donor_group_id = checkpoint.donor_group_id, .receiver_group_id = checkpoint.receiver_group_id, .identity_namespace = self.core.identity_namespace, .copy_attempt = checkpoint.copy_attempt } }, .{});
                    try owned_store_values.append(self.alloc, shadow_state);
                    try store_writes.append(self.alloc, .{ .key = shadow.key, .value = shadow_state });
                    const serving_range = try range_state_mod.encodeRangeAlloc(self.alloc, plan.state.receiver_base_range);
                    try owned_store_values.append(self.alloc, serving_range);
                    try store_writes.append(self.alloc, .{ .key = shadow.range_key, .value = serving_range });
                }
                if (plan.applies_checkpoint and coordinated_handoff and (checkpoint.kind == .finalize or checkpoint.kind == .rollback)) {
                    // Keep one bounded exact-source terminal receipt for lost
                    // checkpoint responses. A new transition replaces it; only
                    // the serving-range override controls live shadow admission.
                    try delete_keys.append(self.alloc, shadow.range_key);
                }
                if (plan.applies_checkpoint and coordinated_handoff and (checkpoint.kind == .finalize or checkpoint.kind == .rollback)) {
                    const catalog = @import("relational_integrity_catalog.zig");
                    const raw_catalog = try self.core.store.get(self.alloc, catalog.key);
                    defer self.alloc.free(raw_catalog);
                    const coverage = try @import("relational_integrity_handoff.zig").coverageForRange(self.alloc, raw_catalog, self.core.identity_namespace, plan.range);
                    try owned_store_values.append(self.alloc, coverage);
                    try store_writes.append(self.alloc, .{ .key = @import("relational_integrity_activation.zig").key, .value = coverage });
                }
            }
            if (persisted_range) |range| try self.core.index_manager.validateRangeTransition(range);
            try appendDenseArtifactCounterMutations(
                self.alloc,
                self.core.store,
                self.core.index_manager,
                &store_writes,
                delete_keys.items,
                &owned_store_keys,
                &owned_store_values,
            );
            // Synchronous enrichment is the producer for every request it consumes.
            // Commit its terminal coverage decisions in the same backend batch as
            // the source document, artifacts, and replay append. This makes
            // `.full_index` an exact coverage fence and leaves asynchronous replay
            // responsible only for requests retained in generated_enrichment_refs.
            // Local producer receipts are fenced by the latest primary mutation,
            // not by the immutable document identity. Stamp in the primary batch.
            if (self.core.hasGeneratedEnrichmentTargets() and !try orderedCoverageActive(self.core.store)) {
                const readiness = @import("artifact_producer_readiness.zig");
                for (identity_upsert_keys.items, identity_upsert_write_indexes.items) |document, write_index| {
                    // An identical logical row creates no producer replay debt.
                    if (!derived_changed_flags[write_index]) continue;
                    const key = try readiness.localPrimaryRevisionKeyAlloc(self.alloc, document);
                    owned_store_keys.append(self.alloc, key) catch |err| {
                        self.alloc.free(key);
                        return err;
                    };
                    const value = try self.alloc.alloc(u8, 8);
                    owned_store_values.append(self.alloc, value) catch |err| {
                        self.alloc.free(value);
                        return err;
                    };
                    std.mem.writeInt(u64, value[0..8], sequence, .little);
                    try store_writes.append(self.alloc, .{ .key = key, .value = value });
                }
                for (effective_req.deletes) |document| {
                    const key = try readiness.localPrimaryRevisionKeyAlloc(self.alloc, document);
                    owned_store_keys.append(self.alloc, key) catch |err| {
                        self.alloc.free(key);
                        return err;
                    };
                    try delete_keys.append(self.alloc, key);
                }
            }
            var local_coverage_guard = if (precomputed_generated.coverage_outcomes.len != 0)
                self.core.index_manager.lockLocalCoverage()
            else
                index_manager_mod.IndexManager.LocalCoverageGuard{};
            defer local_coverage_guard.release();
            try appendPrecomputedCoverageOutcomeMutations(
                self.alloc,
                self.core.store,
                self.core.index_manager,
                precomputed_generated.coverage_outcomes,
                sequence,
                &store_writes,
                &owned_store_keys,
                &owned_store_values,
            );
            try store_writes.appendSlice(self.alloc, opts.extra_store_writes);
            if (req.merge_page) |page| if (page.integrity.len != 0) {
                // Private page shape, exact generation/ownership and replay cursor
                // have been checked under this same apply fence. The physical
                // shadow records commit with primary/index effects and receipt.
                if (!coordinated_handoff) return error.CoordinatedConstraintTopologyUnsupported;
                const catalog_mod = @import("relational_integrity_catalog.zig");
                const raw = try self.core.store.get(self.alloc, catalog_mod.key);
                defer self.alloc.free(raw);
                var compiled = try catalog_mod.decode(self.alloc, raw);
                defer compiled.deinit();
                for (page.integrity) |effect| {
                    const address = (try @import("relational_integrity_contract.zig").parseKey(effect.key)).address;
                    if (page.phase != .cleanup_integrity and compiled.findGeneration(address.generation) == null) return error.IntegrityCatalogChanged;
                    if (effect.value) |value| try store_writes.append(self.alloc, .{ .key = effect.key, .value = value }) else try delete_keys.append(self.alloc, effect.key);
                }
            };
            if (req.merge_page) |page| for (page.provenance_effects) |effect| {
                // Inert source evidence commits with the page cursor, never with
                // donor receipt or authority records. Adoption is a separate
                // receiver-local validation and transaction.
                try store_writes.append(self.alloc, .{ .key = effect.key, .value = effect.value orelse return error.InvalidMergePage });
            };
            try delete_keys.appendSlice(self.alloc, opts.extra_store_deletes);
            if (index_stage) |*stage| {
                const effects = try stage.seal();
                if (opts.transaction_resolution == null and !live_replication_apply and !scoped_restore_replication_apply) {
                    var forward_keys = std.ArrayListUnmanaged([]const u8).empty;
                    defer forward_keys.deinit(self.alloc);
                    for (effects.writes) |effect| if (relational_index_records.isForwardKey(effect.key))
                        try forward_keys.append(self.alloc, effect.key);
                    for (effects.deletes) |key| if (relational_index_records.isForwardKey(key))
                        try forward_keys.append(self.alloc, key);
                    if (forward_keys.items.len != 0) {
                        var manager = try self.core.initTxnManager();
                        defer manager.deinit();
                        try manager.checkIndexForwardWriteConflicts(forward_keys.items);
                    }
                }
                try store_writes.appendSlice(self.alloc, effects.writes);
                try delete_keys.appendSlice(self.alloc, effects.deletes);
            }
            try delete_keys.appendSlice(self.alloc, identity_visibility_deletes.items);

            // A synchronous hot-standby append can fail after the local transaction commit.
            // Persist the exact committed payloads in the same backend batch so an
            // idempotent resolve retry can finish mirroring without reconstructing
            // data from already-deleted intents.
            var transaction_replication_batch_payload: ?[]const u8 = null;
            var transaction_replication_replay_payload: ?[]const u8 = null;
            if (opts.transaction_resolution) |resolution| if (!opts.bypass_replication_write_gate and !scoped_replication) {
                if (self.local_execution.replication_async_batch_mirror) |mirror| if (replicationMirrorSyncEnabled(mirror)) {
                    const payload = preencoded_replication_batch_payload orelse return error.ReplicationPublisherUnavailable;
                    // The outbox borrows the request-owned buffer through commit
                    // and the hot-standby wait; keep its original budgeted owner intact.
                    const key_array = transactions_mod.makeTransactionReplicationBatchOutboxKey(resolution.txn_id);
                    const key = try self.alloc.dupe(u8, &key_array);
                    try owned_store_keys.append(self.alloc, key);
                    try store_writes.append(self.alloc, .{ .key = key, .value = payload });
                    transaction_replication_batch_payload = payload;
                };
                if (append_derived_replay) if (self.local_execution.replication_async_effect_mirror) |mirror| if (replicationMirrorSyncEnabled(mirror)) {
                    const payload = try self.alloc.dupe(u8, replay_payload);
                    try owned_store_values.append(self.alloc, payload);
                    const key_array = transactions_mod.makeTransactionReplicationReplayOutboxKey(resolution.txn_id);
                    const key = try self.alloc.dupe(u8, &key_array);
                    try owned_store_keys.append(self.alloc, key);
                    try store_writes.append(self.alloc, .{ .key = key, .value = payload });
                    transaction_replication_replay_payload = payload;
                };
            };
            const replay_append: ?docstore_mod.DocStore.ReplayAppend = if (append_derived_replay)
                .{
                    .sequence = sequence,
                    .payload = replay_payload,
                }
            else
                null;
            // This is the final fallible replay-retention boundary. It is an
            // immediate node-wide reservation: no reclamation, disk I/O, or wait
            // occurs while the apply fence is held. A hard-limit failure is
            // returned before the primary mutation and replay intent commit.
            const backlog_admission_start_ns = monotonicTimeNs();
            var backlog_admission = if (append_derived_replay)
                try self.executor.admitBacklogBytes(@intCast(replay_payload.len))
            else
                derived_executor_mod.BacklogAdmission{};
            if (profile) |active_profile| recordProfileNs(profile, &active_profile.backlog_admission_ns, backlog_admission_start_ns);
            defer backlog_admission.cancel();
            // Direct admissions may have spent time preparing derived effects.
            // Recheck the original signed statement deadline at the irreversible
            // primary commit boundary; Raft replay leases have no wall-time limit.
            if (opts.row_policy_lease) |lease| try lease.checkAt(@intCast(@divFloor(platform_time.realtimeNs(), std.time.ns_per_s)));
            const authored_root = blk: {
                if (self.root_incarnation == 0 or explicit_embedding_artifact_writes.items.len == 0) break :blk 0;
                var probe = try self.core.store.beginProbeTxn();
                defer probe.abort();
                if (try @import("artifact_publication.zig").authority(&probe) == null) break :blk 0;
                break :blk self.root_incarnation;
            };
            var authored_acceptance = try @import("artifact_authored_acceptance.zig").Prepared.init(
                self.alloc,
                authored_root,
                explicit_embedding_artifact_writes.items,
                store_writes.items,
            );
            defer authored_acceptance.deinit();
            if (opts.row_policy_lease) |lease| try lease.checkAt(@intCast(@divFloor(platform_time.realtimeNs(), std.time.ns_per_s)));
            const transaction_applied = if (opts.transaction_resolution) |resolution| blk: {
                const outcome = try self.core.resolveTransactionIntentsWithExtraBatch(
                    resolution.txn_id,
                    resolution.status,
                    resolution.commit_version,
                    .{
                        .commit_participant = if (authored_acceptance.vectors.len != 0) authored_acceptance.participant() else null,
                        .writes = store_writes.items,
                        .deletes = delete_keys.items,
                        .replay = if (replay_append) |entry| .{ .sequence = entry.sequence, .payload = entry.payload } else null,
                        .expected_intent_revision = resolution.expected_intent_revision,
                        .known_intent_keys = resolution.intent_keys,
                        .skip_all_intent_application = relationalColumns(self) != null,
                        .completion_writes = if (ordered_apply_receipt_write) |write| &.{write} else &.{},
                        .resolved_participant = resolution.resolved_participant,
                    },
                );
                if (outcome.applied) {
                    if (replay_append) |entry| self.core.store.observeExternalReplayCommit(entry.sequence);
                }
                break :blk outcome;
            } else blk: {
                if (durable_replication_batch_outbox_key != null or durable_replication_replay_outbox_key != null)
                    self.local_execution.durable_replication_outbox_maybe.store(true, .release);
                try self.core.store.putBatchWithReplayAndParticipant(
                    self.backend_runtime.io(),
                    store_writes.items,
                    delete_keys.items,
                    replay_append,
                    store_batch_options,
                    if (authored_acceptance.vectors.len != 0) authored_acceptance.participant() else null,
                );
                schedule_replication_recovery_on_exit = durable_replication_batch_outbox_key != null or durable_replication_replay_outbox_key != null;
                break :blk transactions_mod.ResolutionOutcome{ .applied = true, .replay_sequence = sequence };
            };
            local_coverage_guard.release();
            if (graph_publication) |*lease| lease.release();
            if (!transaction_applied.applied) {
                unlockProfiledApply(self, profile, &apply_mutex_held, apply_lock_acquired_ns);
                try self.waitForResolvedTransactionSync(effective_req.sync_level, transaction_applied.replay_sequence);
                return;
            }
            if (next_table_catalog) |catalog| self.core.table_catalog = catalog;
            if (persisted_range != null) {
                self.core.adoptPersistedRangeOwned(persisted_range_start_owned.?, persisted_range_end_owned.?);
                persisted_range_start_owned = null;
                persisted_range_end_owned = null;
            }
            var deferred_replication_gates = ReplicationDeferredCommitGates{};
            defer deferred_replication_gates.releaseTransition();
            if (!opts.bypass_replication_write_gate) {
                var replication_ctx = self.batchContext();
                deferred_replication_gates = ReplicationDeferredCommitGates.begin(replicationTransitionMutexFromContext(&replication_ctx));
                if (transaction_replication_batch_payload) |payload| {
                    deferred_replication_gates.append(try appendReplicationEncodedBatchMutationCommitLockedContext(&replication_ctx, payload));
                } else if (durable_replication_batch_payload) |payload| {
                    deferred_replication_gates.append(try appendReplicationEncodedBatchMutationCommitLockedContextStrict(&replication_ctx, payload, scoped_replication));
                } else if (preencoded_replication_batch_payload) |payload| {
                    deferred_replication_gates.append(try appendReplicationEncodedBatchMutationCommitLockedContext(&replication_ctx, payload));
                } else deferred_replication_gates.append(try appendReplicationBatchMutationCommitLockedContext(&replication_ctx, effective_req));
                if (transaction_replication_replay_payload) |payload| {
                    deferred_replication_gates.append(try appendReplicationReplayPayloadCommitLockedContext(&replication_ctx, payload));
                } else if (durable_replication_replay_payload) |payload| {
                    deferred_replication_gates.append(try appendReplicationReplayPayloadCommitLockedContext(&replication_ctx, payload));
                } else if (append_derived_replay and !scoped_replication) deferred_replication_gates.append(try appendReplicationReplayPayloadCommitLockedContext(&replication_ctx, replay_payload));
            }
            if (opts.committed_batch_effects_observer) |observer| {
                try observer.observe(if (append_derived_replay) replay_payload else "");
            }
            if (pending_identity_visibility_summary) |summary| {
                self.core.identity_visibility.summary = summary;
                self.clearLiveDocSetCache();
                self.clearNonVisibleDocSetCache();
            }
            if (profile) |active_profile| {
                recordProfileNs(profile, &active_profile.store_write_ns, store_write_start_ns);
                active_profile.store_write_count += @intCast(store_writes.items.len);
                active_profile.store_delete_count += @intCast(delete_keys.items.len);
            }
            const is_split_progress_checkpoint = if (req.split_checkpoint) |checkpoint|
                checkpoint.kind == .source_ack
            else
                false;
            if (shouldAppendSplitDelta(self) and !is_split_progress_checkpoint) {
                const split_delta_start_ns = monotonicTimeNs();
                try self.core.appendSplitDelta(batch_timestamp_ns, store_writes.items, delete_keys.items);
                if (profile) |active_profile| recordProfileNs(profile, &active_profile.split_delta_ns, split_delta_start_ns);
            }

            if (append_derived_replay) {
                const append_replay_journal_start_ns = monotonicTimeNs();
                self.executor.commitBacklogAdmission(sequence, &backlog_admission);
                if (profile) |active_profile| recordProfileNs(profile, &active_profile.append_replay_journal_ns, append_replay_journal_start_ns);
            }
            if (materialized_derived_batch != null) {
                split_shadow_ticket = reserveSplitShadowApplyTicket(self);
            }
            unlockProfiledApply(self, profile, &apply_mutex_held, apply_lock_acquired_ns);
            snapshot_mutation.release();
            if (split_shadow_ticket) |ticket| {
                const apply_shadow_start_ns = monotonicTimeNs();
                applyCommittedBatchToShadowOrdered(self, materialized_derived_batch.?, ticket);
                if (profile) |active_profile| recordProfileNs(profile, &active_profile.apply_shadow_ns, apply_shadow_start_ns);
            }
            // Explicit derived-visibility waits are not part of foreground write
            // admission. Releasing here lets optional maintenance make progress
            // while `.full_index` deliberately waits below.
            foreground_write.release();
            if (append_derived_replay)
                notifyQueryVisibilityTargetAdvancedScoped(
                    self.async_context,
                    sequence,
                    sync_targets.target_identities,
                    sync_targets.target_scope_known,
                );
            releaseReplicationMutationShared(&replication_mutation);
            if (builtin.is_test and opts.restore_staging != null and D.test_fail_restore_projection_apply.*) {
                D.test_fail_restore_projection_apply.* = false;
                return error.InjectedRestoreProjectionApplyFailure;
            }
            // Replay intent is locally durable at this point. Wake derived workers
            // before any remote hot-standby acknowledgment so HBC/full-text progress is
            // independent of response durability latency; explicit visibility
            // waits still occur only after the hot-standby gate below.
            if (append_derived_replay and self.executor.hasWorkers()) {
                const notify_executor_start_ns = monotonicTimeNs();
                notifyExecutorForSyncLevelWithDenseBulkDeferral(self.async_context, self.executor, effective_req.sync_level, sequence, sync_targets);
                if (profile) |active_profile| recordProfileNs(profile, &active_profile.executor_notify_ns, notify_executor_start_ns);
            }
            if (!opts.bypass_replication_write_gate) {
                const replication_ctx = self.batchContext();
                try deferred_replication_gates.waitForDurabilityAndAuthority(replication_ctx.replication_write_gate);
                if (opts.transaction_resolution != null or durable_replication_batch_payload != null or durable_replication_replay_payload != null) {
                    // The outbox is cleared only after every appended record has
                    // satisfied durability and the final authority recheck. Keep
                    // the cleanup atomic with other local DB mutations without
                    // retaining the apply lock across the remote wait above.
                    lockApply(self);
                    defer self.core.unlockApply();
                    if (opts.transaction_resolution) |resolution| {
                        if (transaction_replication_batch_payload != null)
                            try self.core.clearTransactionReplicationOutbox(resolution.txn_id, .batch);
                        if (transaction_replication_replay_payload != null)
                            try self.core.clearTransactionReplicationOutbox(resolution.txn_id, .replay);
                    }
                    var durable_outbox_deletes: [2][]const u8 = undefined;
                    var durable_outbox_delete_count: usize = 0;
                    if (durable_replication_batch_outbox_key) |key| {
                        durable_outbox_deletes[durable_outbox_delete_count] = key;
                        durable_outbox_delete_count += 1;
                    }
                    if (durable_replication_replay_outbox_key) |key| {
                        durable_outbox_deletes[durable_outbox_delete_count] = key;
                        durable_outbox_delete_count += 1;
                    }
                    if (durable_outbox_delete_count != 0)
                        try self.core.store.putBatch(&.{}, durable_outbox_deletes[0..durable_outbox_delete_count]);
                }
            }
            if (opts.document_child_range_dispatcher) |dispatcher| {
                _ = try self.drainDocumentArtifactChildRangeOutbox(dispatcher, 0);
            }
            if (append_derived_replay) {
                var pressure_ctx = self.batchContext();
                const backlog_pressure_start_ns = monotonicTimeNs();
                try self.markPrecomputedEnrichmentAppliedForSync(effective_req.sync_level, sequence);
                try applyDerivedBacklogPressureContext(&pressure_ctx, sequence, effective_req.sync_level, sync_targets);
                if (profile) |active_profile| recordProfileNs(profile, &active_profile.backlog_pressure_ns, backlog_pressure_start_ns);
            }
            const wait_sync_start_ns = monotonicTimeNs();
            if (append_derived_replay and self.executor.hasWorkers()) {
                if (opts.wait_for_sync_level) {
                    const sync_wait_start_ns = monotonicTimeNs();
                    try self.waitForSyncLevelWithCancellation(effective_req.sync_level, sequence, sync_targets, opts.visibility_cancellation, enrichment_fully_precomputed);
                    if (profile) |active_profile| recordProfileNs(profile, &active_profile.sync_wait_ns, sync_wait_start_ns);
                }
            } else if (append_derived_replay) {
                if (opts.wait_for_sync_level and syncLevelRequiresDerivedVisibility(effective_req.sync_level)) {
                    const derived_apply_start_ns = monotonicTimeNs();
                    if (effective_req.sync_level == .full_text) {
                        try applyDerivedBatchTargetsProfiled(self, materialized_derived_batch.?, sync_targets.full_text_indexes, profile);
                    } else {
                        try applyDerivedBatchProfiled(self, materialized_derived_batch.?, profile);
                    }
                    if (profile) |active_profile| recordProfileNs(profile, &active_profile.derived_apply_ns, derived_apply_start_ns);
                }
                if (opts.wait_for_sync_level) {
                    const sync_wait_start_ns = monotonicTimeNs();
                    try self.waitForSyncLevelWithCancellation(effective_req.sync_level, sequence, sync_targets, opts.visibility_cancellation, enrichment_fully_precomputed);
                    if (profile) |active_profile| recordProfileNs(profile, &active_profile.sync_wait_ns, sync_wait_start_ns);
                }
            }
            if (profile) |active_profile| recordProfileNs(profile, &active_profile.wait_sync_ns, wait_sync_start_ns);
            if (append_derived_replay) {
                if (self.enrichment_runtime) |runtime| {
                    const notify_enrichment_start_ns = monotonicTimeNs();
                    runtime.notifySequence(sequence);
                    if (profile) |active_profile| recordProfileNs(profile, &active_profile.notify_enrichment_ns, notify_enrichment_start_ns);
                }
                self.notifyResolverReplayRuntimes(sequence);
            }
        }

        pub fn batchInternalWithPreparationAllocator(
            self: anytype,
            req: types.BatchRequest,
            profile: ?*BatchProfile,
            opts: BatchExecutionOptions,
            allocator_guard: *PreparedRowAllocator,
        ) anyerror!void {
            try types.validateMergeArtifacts(req);
            const max_prepared_generation_retries = 2;
            var retry_count: usize = 0;
            // Provider results are semantic request data, not schema/index-plan
            // data. Keep them across optimistic publication retries so catalog
            // churn cannot multiply expensive external work.
            var generated_memo = GeneratedEmbeddingMemo.init(allocator_guard.allocator());
            // Cache restoration is an internal preservation operation. Ordinary
            // writes and explicit regeneration retain their provider semantics.
            generated_memo.reuse_stored_artifacts = if (opts.restore_staging) |admission| !admission.rewrite else false;
            defer generated_memo.deinit();
            while (true) {
                self.batchInternalPrepared(req, profile, opts, &generated_memo, allocator_guard) catch |err| switch (err) {
                    error.PreparedGenerationChanged, error.PreparedReadSetChanged => {
                        if (retry_count >= max_prepared_generation_retries) return err;
                        retry_count += 1;
                        continue;
                    },
                    else => return err,
                };
                // Completion errors happen after commit. Keep this outside the
                // preparation retry handler so they cannot replay the user batch.
                try self.finishBatchGraphEndpointCleanup(req, opts);
                return;
            }
        }

        pub fn clearActiveIndexRepairsLocked(self: anytype) void {
            var it = self.active_index_repairs.keyIterator();
            while (it.next()) |key_ptr| self.alloc.free(@constCast(key_ptr.*));
            self.active_index_repairs.clearRetainingCapacity();
        }

        pub fn clearBulkIngestIdentityAllNewLocked(self: anytype) void {
            self.bulk_identity.reset(self.alloc);
        }

        pub fn clearDurableReplicationOutbox(self: anytype, key: []const u8) !void {
            try self.lockApplyForPortableRuntime();
            defer self.core.unlockApply();
            try durable_outbox_store.clearPublished(self.core.store, key);
        }

        pub fn clearLiveDocSetCache(self: anytype) void {
            self.core.identity_visibility.clearLive();
        }

        pub fn clearNonVisibleDocSetCache(self: anytype) void {
            self.core.identity_visibility.clearNonvisible();
        }

        pub fn enforcePortableRuntimeGate(self: anytype) !void {
            if (self.local_execution.vector_migration_reopen_required.load(.acquire)) return error.VectorMigrationRecoveryRequired;
            try enforcePortableRuntimeGateOptional(&self.async_context.portable_runtime_activation_pending);
            if (self.local_execution.graph_merge_import_recovery_pending.load(.acquire)) return error.GraphMaintenanceInProgress;
            // A read-only handle can predate the writer's import. Its local flag
            // cannot observe publication, so consult the shared durable fence.
            if (openModeRequiresReadOnlyBackends(self.open_mode)) {
                const pending = self.core.store.get(self.alloc, graph_merge_import_recovery_key) catch |err| switch (err) {
                    error.NotFound => null,
                    else => return err,
                };
                if (pending) |record| {
                    self.alloc.free(record);
                    return error.GraphMaintenanceInProgress;
                }
            }
        }

        pub fn enforceReplicationWriteGate(self: anytype) !void {
            try self.enforcePortableRuntimeGate();
            try enforceReplicationWriteGateOptional(self.local_execution.replication_write_gate);
            if (self.async_context.primary_replication_append_pending.load(.acquire)) return error.ReplicationPublisherUnavailable;
        }

        pub fn enforceRowPolicyMutationLocked(
            self: anytype,
            schema_view: schema_registry_mod.SchemaView,
            req: types.BatchRequest,
            prepared_rows: ?[]?mapper.PreparedRelationalWrite,
            principal: *const row_policy_authority_mod.Payload,
        ) !void {
            if (principal.access != .write or req.graph_writes.len != 0 or req.graph_deletes.len != 0 or
                req.merge_artifacts.len != 0 or req.online_source != null or req.restore_staging != null)
                return error.RowPolicyAuthenticationRequired;
            if (req.writes.len != 0 and (prepared_rows == null or prepared_rows.?.len != req.writes.len))
                return error.RowPolicyMutationUnsupported;
            const bundle = if (self.local_execution.row_policy_bundle) |*installed| installed else return error.RowPolicyCatalogChanged;
            const schema = schema_view.tableSchema().*;
            var insert_check = try bundle.captureEvaluation(self.alloc, schema, principal, .insert);
            defer insert_check.deinit();
            var update_old = try bundle.captureEvaluation(self.alloc, schema, principal, .update_old);
            defer update_old.deinit();
            var update_new = try bundle.captureEvaluation(self.alloc, schema, principal, .update_new);
            defer update_new.deinit();
            var delete_old = try bundle.captureEvaluation(self.alloc, schema, principal, .delete);
            defer delete_old.deinit();
            var probe = try self.core.store.beginProbeTxn();
            defer probe.abort();
            for (req.writes, 0..) |write, i| {
                if (isMetadataKey(write.key)) return error.RowPolicyMutationUnsupported;
                const store_key = try encodeStoreLookupKeyAlloc(self, self.alloc, write.key);
                defer self.alloc.free(store_key);
                const old_bytes: ?[]const u8 = probe.getLeased(store_key) catch |err| switch (err) {
                    error.NotFound => null,
                    else => return err,
                };
                if (old_bytes) |bytes| {
                    if (try relational_store.rowSchemaVersion(bytes) != schema.version) return error.RowPolicyCatalogChanged;
                    const old = if (self.core.store.valuesAreAuthenticated())
                        try relational_row_codec.ordinalRowViewTrusted(bytes, schema, schema_view.physicalLayout())
                    else
                        try relational_row_codec.ordinalRowViewSelective(bytes, schema, schema_view.physicalLayout());
                    if (!try update_old.permits(old)) return error.RowPolicyDenied;
                }
                const prepared = prepared_rows.?[i] orelse return error.RowPolicyMutationUnsupported;
                const new = try relational_row_codec.ordinalRowViewTrusted(prepared.packed_row, schema, schema_view.physicalLayout());
                if (!try (if (old_bytes != null) update_new else insert_check).permits(new))
                    return error.RowPolicyDenied;
            }
            for (req.deletes) |key| {
                if (isMetadataKey(key)) return error.RowPolicyMutationUnsupported;
                const store_key = try encodeStoreLookupKeyAlloc(self, self.alloc, key);
                defer self.alloc.free(store_key);
                const bytes = probe.getLeased(store_key) catch |err| switch (err) {
                    error.NotFound => continue,
                    else => return err,
                };
                if (try relational_store.rowSchemaVersion(bytes) != schema.version) return error.RowPolicyCatalogChanged;
                const old = if (self.core.store.valuesAreAuthenticated())
                    try relational_row_codec.ordinalRowViewTrusted(bytes, schema, schema_view.physicalLayout())
                else
                    try relational_row_codec.ordinalRowViewSelective(bytes, schema, schema_view.physicalLayout());
                if (!try delete_old.permits(old)) return error.RowPolicyDenied;
            }
        }

        pub fn ensureDurableReplicationStartupBarrier(self: anytype) !void {
            // A failed primary append must complete before a later mutation can
            // overtake its afterimage in the hot-standby tail.
            while (self.async_context.primary_replication_append_pending.load(.acquire)) try self.flushDurableReplicationOutboxes();
            if (!self.local_execution.durable_replication_startup_barrier_pending.load(.acquire) and
                !self.local_execution.row_policy_replication_outbox_pending.load(.acquire)) return;
            const io = self.backend_runtime.io() orelse std.Options.debug_io;
            self.local_execution.durable_replication_flush_mutex.lockUncancelable(io);
            defer self.local_execution.durable_replication_flush_mutex.unlock(io);
            if (!self.local_execution.durable_replication_startup_barrier_pending.load(.acquire) and
                !self.local_execution.row_policy_replication_outbox_pending.load(.acquire)) return;
            while (true) {
                while (self.local_execution.durable_replication_outbox_maybe.load(.acquire))
                    try self.flushDurableReplicationOutboxesLocked();
                // Close the race with a policy commit that published its flag just
                // after the final scan. The same apply fence orders that commit
                // and the flag clear against subsequent row writers.
                try self.lockApplyForPortableRuntime();
                const empty = !self.local_execution.durable_replication_outbox_maybe.load(.acquire);
                if (empty) {
                    self.local_execution.durable_replication_startup_barrier_pending.store(false, .release);
                    self.local_execution.row_policy_replication_outbox_pending.store(false, .release);
                }
                self.core.unlockApply();
                if (empty) return;
            }
        }

        pub fn failIfIdentityOrdinalExhaustedForNewUpserts(self: anytype, doc_ids: []const []const u8) !void {
            if (doc_ids.len == 0) return;

            var txn = try self.core.store.beginProbeTxn();
            defer txn.abort();

            const raw_next = txn.get(internal_keys.identity_next_ordinal_key[0..]) catch |err| switch (err) {
                error.NotFound => return,
                else => return err,
            };
            if (raw_next.len != @sizeOf(doc_identity.DocOrdinal)) return error.InvalidDocIdentity;
            const next_ordinal = std.mem.readInt(doc_identity.DocOrdinal, raw_next[0..4], .big);
            if (next_ordinal != 0 and next_ordinal < std.math.maxInt(doc_identity.DocOrdinal)) return;

            var seen = std.StringHashMapUnmanaged(void).empty;
            defer seen.deinit(self.alloc);
            for (doc_ids) |doc_id| {
                if (seen.contains(doc_id)) continue;
                try seen.put(self.alloc, doc_id, {});
                if (try doc_identity.lookupOrdinalTxn(self.alloc, &txn, doc_id) != null) continue;
                return error.DocOrdinalExhausted;
            }
        }

        pub fn finalizePendingRowPolicyReceiptLocked(self: anytype, expected: row_policy_bundle_mod.Receipt) !row_policy_bundle_mod.Receipt {
            const pending = (try self.pendingRowPolicyReceipt()) orelse return error.NotFound;
            if (!std.meta.eql(pending, expected)) return error.RowPolicyCatalogChanged;
            if (!self.local_execution.row_policy_gate.quiesced()) return error.RowPolicyReadersActive;
            var manager = try self.core.initTxnManager();
            defer manager.deinit();
            if (try manager.hasSchemaLeases()) return error.RowPolicyReadersActive;
            const schema = self.core.schema orelse return error.RowPolicyCatalogChanged;
            const bundle_bytes = try self.core.store.get(self.alloc, row_policy_bundle_mod.key);
            defer self.alloc.free(bundle_bytes);
            var installed = try row_policy_bundle_mod.Installed.init(self.alloc, bundle_bytes, self.core.identity_namespace.table_id, schema);
            var installed_owned = true;
            defer if (installed_owned) installed.deinit();
            const publication = installed.parsed.value;
            var digest: [32]u8 = undefined;
            std.crypto.hash.sha2.Sha256.hash(bundle_bytes, &digest, .{});
            if (publication.table_id != expected.table_id or
                publication.policy_generation != expected.generation or
                publication.catalog_epoch != expected.catalog_epoch or
                publication.phase != expected.phase or
                !std.mem.eql(u8, &digest, &expected.bundle_digest)) return error.RowPolicyCatalogChanged;
            const previous = self.core.table_catalog;
            if (previous.row_policy_phase != .preparing or
                previous.row_policy_generation != expected.generation or
                previous.row_policy_catalog_epoch != expected.catalog_epoch or
                previous.active_schema_version != publication.schema_version) return error.RowPolicyCatalogChanged;
            var next = previous;
            next.row_policy_phase = switch (publication.phase) {
                .pending_install, .pending_disable, .serving_disable => .preparing,
                .serving_install, .active => .active,
                .disabled => .disabled,
            };
            next.generation +|= 1;
            const catalog_bytes = next.encode();
            const encoded_receipt = expected.encode();
            var receipt_key_buf: [128]u8 = undefined;
            const receipt_key = try expected.key(&receipt_key_buf);
            var txn = try self.core.store.beginWriteTxn();
            errdefer txn.abort();
            try txn.put(receipt_key, &encoded_receipt);
            try txn.put(table_catalog_mod.key, &catalog_bytes);
            try txn.delete(row_policy_bundle_mod.pending_key);
            try txn.commit();
            self.core.table_catalog = next;
            if (next.row_policy_phase != .preparing) {
                if (self.local_execution.row_policy_bundle) |*old| old.deinit();
                self.local_execution.row_policy_bundle = if (next.row_policy_phase == .active) installed else null;
                if (next.row_policy_phase == .active) installed_owned = false;
                try self.local_execution.row_policy_gate.publishCommitted(next);
            }
            return expected;
        }

        pub fn flushDurableReplicationOutboxes(self: anytype) !void {
            if (self.async_context.primary_replication_outbox_pending.load(.acquire)) self.local_execution.durable_replication_outbox_maybe.store(true, .release);
            if (!self.local_execution.durable_replication_outbox_maybe.load(.acquire)) return;
            const io = self.backend_runtime.io() orelse std.Options.debug_io;
            self.local_execution.durable_replication_flush_mutex.lockUncancelable(io);
            defer self.local_execution.durable_replication_flush_mutex.unlock(io);
            try self.flushDurableReplicationOutboxesLocked();
        }

        pub fn flushDurableReplicationOutboxesLocked(self: anytype) !void {
            if (!self.local_execution.durable_replication_outbox_maybe.load(.acquire)) return;
            try self.lockApplyForPortableRuntime();
            var apply_held = true;
            errdefer if (apply_held) self.core.unlockApply();
            var page = try durable_outbox_store.readPending(self.alloc, self.core.store);
            defer page.deinit(self.alloc);
            const batch_raw = page.legacy_batch;
            const replay_raw = page.legacy_replay;
            const schema_raw = page.legacy_schema;
            const pending = page.entries;
            const any_pending = !page.isEmpty();
            if (!any_pending) {
                self.local_execution.durable_replication_outbox_maybe.store(false, .release);
                self.async_context.primary_replication_outbox_pending.store(false, .release);
                self.async_context.primary_replication_append_pending.store(false, .release);
            }
            self.core.unlockApply();
            apply_held = false;
            if (!any_pending) return;

            const batch_outbox = if (batch_raw) |raw| try decodeDurableReplicationOutbox(raw) else null;
            const replay_outbox = if (replay_raw) |raw| try decodeDurableReplicationOutbox(raw) else null;
            const schema_outbox = if (schema_raw) |raw| try decodeDurableReplicationOutbox(raw) else null;

            if (batch_outbox != null) {
                const mirror = self.local_execution.replication_async_batch_mirror orelse return error.ReplicationPublisherUnavailable;
                if (!replicationMirrorRequiresDurableOutbox(mirror)) return error.ReplicationPublisherUnavailable;
            }
            if (replay_outbox != null) {
                const mirror = self.local_execution.replication_async_effect_mirror orelse return error.ReplicationPublisherUnavailable;
                if (!replicationMirrorRequiresDurableOutbox(mirror)) return error.ReplicationPublisherUnavailable;
            }
            if (schema_outbox != null) {
                const mirror = self.local_execution.replication_async_metadata_mirror orelse return error.ReplicationPublisherUnavailable;
                if (!replicationMirrorRequiresDurableOutbox(mirror)) return error.ReplicationPublisherUnavailable;
            }
            // Recovery delivers the unlogged effect that closes foreground writes.
            try self.enforcePortableRuntimeGate();
            try enforceReplicationWriteGateOptional(self.local_execution.replication_write_gate);

            if (batch_outbox) |outbox| {
                var ctx = self.batchContext();
                try recoverDurableReplicationOutboxContext(&ctx, self.local_execution.replication_async_batch_mirror.?, outbox, .batch);
                try self.clearDurableReplicationOutbox(replication_batch_outbox_key);
            }
            if (replay_outbox) |outbox| {
                var ctx = self.batchContext();
                try recoverDurableReplicationOutboxContext(&ctx, self.local_execution.replication_async_effect_mirror.?, outbox, .replay);
                try self.clearDurableReplicationOutbox(replication_replay_outbox_key);
            }
            if (schema_outbox) |outbox| {
                var ctx = self.batchContext();
                try recoverDurableReplicationOutboxContext(&ctx, self.local_execution.replication_async_metadata_mirror.?, outbox, .schema);
                try self.clearDurableReplicationOutbox(replication_schema_outbox_key);
            }

            for (pending) |entry| {
                const kind = try durableReplicationOutboxKindFromKey(entry.key);
                const outbox = try decodeDurableReplicationOutbox(entry.value);
                const mirror = switch (kind) {
                    .batch, .restore_batch => self.local_execution.replication_async_batch_mirror,
                    .replay, .primary_effect => self.local_execution.replication_async_effect_mirror,
                    .schema, .row_policy => self.local_execution.replication_async_metadata_mirror,
                } orelse return error.ReplicationPublisherUnavailable;
                if (kind != .restore_batch and kind != .primary_effect and kind != .row_policy and !replicationMirrorRequiresDurableOutbox(mirror)) return error.ReplicationPublisherUnavailable;
                var ctx = self.batchContext();
                try recoverDurableReplicationOutboxContext(&ctx, mirror, outbox, kind);
                // The key names this exact mutation, so concurrent publishers cannot
                // replace the record which this recovery pass is about to remove.
                try self.clearDurableReplicationOutbox(entry.key);
            }
        }

        pub fn flushTransactionReplicationOutbox(self: anytype, txn_id: transactions_mod.TxnId) !void {
            var outbox = try self.core.loadTransactionReplicationOutbox(self.alloc, txn_id);
            defer outbox.deinit(self.alloc);
            if (outbox.batch_payload == null and outbox.replay_payload == null) return;

            // An outbox entry is a durable synchronous-replication obligation. Do
            // not silently discard it if a restart temporarily removes or
            // downgrades the corresponding mirror configuration.
            if (outbox.batch_payload != null) {
                const mirror = self.local_execution.replication_async_batch_mirror orelse return error.ReplicationPublisherUnavailable;
                if (!replicationMirrorSyncEnabled(mirror)) return error.ReplicationPublisherUnavailable;
            }
            if (outbox.replay_payload != null) {
                const mirror = self.local_execution.replication_async_effect_mirror orelse return error.ReplicationPublisherUnavailable;
                if (!replicationMirrorSyncEnabled(mirror)) return error.ReplicationPublisherUnavailable;
            }
            try self.enforceReplicationWriteGate();
            var ctx = self.batchContext();
            if (outbox.batch_payload != null) try preflightReplicationMirrorSyncCommitContext(&ctx, ctx.replication_async_batch_mirror);
            if (outbox.replay_payload != null) try preflightReplicationMirrorSyncCommitContext(&ctx, ctx.replication_async_effect_mirror);

            if (outbox.batch_payload) |payload| {
                try self.mirrorReplicationEncodedBatchMutationCommit(payload);
                try self.lockApplyForPortableRuntime();
                defer self.core.unlockApply();
                try self.core.clearTransactionReplicationOutbox(txn_id, .batch);
            }
            if (outbox.replay_payload) |payload| {
                try self.mirrorReplicationReplayPayloadCommit(payload);
                try self.lockApplyForPortableRuntime();
                defer self.core.unlockApply();
                try self.core.clearTransactionReplicationOutbox(txn_id, .replay);
            }
        }

        pub fn hasCoordinatedConstraints(view: ?schema_registry_mod.SchemaView) bool {
            const validator = (view orelse return false).validator() orelse return false;
            return (if (validator.schema.unique_constraints) |constraints| constraints.value.len != 0 else false) or
                (if (validator.schema.foreign_keys) |constraints| constraints.value.len != 0 else false);
        }

        pub fn identityUpsertStoreWritesAreNew(write_indexes: []const usize, overwritten_flags: []const bool) bool {
            for (write_indexes) |write_index| {
                if (write_index >= overwritten_flags.len) return false;
                if (overwritten_flags[write_index]) return false;
            }
            return true;
        }

        pub fn isProtectedIntegrityKey(key: []const u8) bool {
            if (@import("../artifact_footprint.zig").isKey(key)) return true;
            if (@import("relational_index_catalog.zig").Controller.isReservedMetadataKey(key)) return true;
            if (@import("retirement_set_summary.zig").isKey(key)) return true;
            if (@import("native_topology_receipt.zig").isKey(key)) return true;
            const generation_admission = @import("relational_integrity_generation_admission.zig");
            return std.mem.eql(u8, key, @import("restore_staging.zig").key) or std.mem.eql(u8, key, @import("restore_staging.zig").bootstrap_key) or @import("relational_integrity.zig").isKey(key) or std.mem.eql(u8, key, @import("relational_integrity_catalog.zig").key) or
                std.mem.eql(u8, key, @import("relational_integrity_activation.zig").key) or
                std.mem.eql(u8, key, @import("relational_integrity_topology.zig").fence_key) or
                std.mem.eql(u8, key, @import("relational_integrity_topology.zig").receipt_key) or
                std.mem.startsWith(u8, key, @import("relational_integrity_topology.zig").abort_prefix) or
                std.mem.eql(u8, key, @import("relational_integrity_retirement.zig").key) or
                std.mem.eql(u8, key, @import("relational_integrity_generation_retirement.zig").key) or
                std.mem.startsWith(u8, key, @import("relational_integrity_generation_retirement.zig").active_prefix) or
                std.mem.eql(u8, key, @import("relational_integrity_generation_retirement.zig").gc_progress_key) or
                std.mem.eql(u8, key, @import("relational_integrity_generation_retirement.zig").activation_receipt_key) or
                std.mem.eql(u8, key, @import("relational_integrity_generation_retirement.zig").completed_pending_key) or
                std.mem.eql(u8, key, @import("relational_integrity_generation_retirement.zig").acknowledged_receipt_key) or
                std.mem.startsWith(u8, key, generation_admission.prefix) or
                std.mem.eql(u8, key, generation_admission.staged_receipt_key) or
                std.mem.eql(u8, key, generation_admission.activation_receipt_key) or
                std.mem.eql(u8, key, generation_admission.acknowledged_receipt_key) or
                std.mem.eql(u8, key, generation_admission.dual_acknowledged_fence_key) or
                std.mem.eql(u8, key, generation_admission.dual_canceled_fence_key) or
                std.mem.eql(u8, key, generation_admission.cancel_receipt_key) or
                std.mem.eql(u8, key, generation_admission.source_cancel_receipt_key) or
                std.mem.eql(u8, key, generation_admission.source_fence_receipt_key) or
                std.mem.eql(u8, key, generation_admission.source_install_receipt_key) or
                std.mem.eql(u8, key, @import("relational_initial_child_publication.zig").key) or
                std.mem.eql(u8, key, @import("relational_integrity_handoff.zig").manifest_key) or
                std.mem.eql(u8, key, @import("relational_integrity_handoff.zig").progress_key) or
                std.mem.eql(u8, key, @import("relational_integrity_handoff.zig").prune_key);
        }

        pub fn isProtectedRangeWriteKey(key: []const u8) bool {
            const protection = @import("../range_protection.zig");
            return std.mem.eql(u8, key, protection.activation_key) or
                std.mem.startsWith(u8, key, protection.counter_prefix) or
                std.mem.startsWith(u8, key, protection.writer_prefix) or
                std.mem.startsWith(u8, key, protection.index_counter_prefix) or
                std.mem.startsWith(u8, key, protection.index_writer_prefix);
        }

        pub fn lockApplyForPortableRuntime(self: anytype) !void {
            try self.enforcePortableRuntimeGate();
            lockApply(self);
            errdefer self.core.unlockApply();
            try self.enforcePortableRuntimeGate();
        }

        pub fn maintenanceRequiresOrderedApply(self: anytype) !bool {
            var read = try self.core.store.beginReadTxn();
            defer read.abort();
            return try @import("../source_authority.zig").maintenanceMode(&read) == .ordered;
        }

        pub fn markPrecomputedEnrichmentAppliedForSync(self: anytype, sync_level: types.SyncLevel, sequence: u64) !void {
            if (sync_level != .enrichments or sequence == 0) return;
            const runtime = self.enrichment_runtime orelse return;
            const runtime_stats = runtime.stats();
            if (runtime_stats.applied_sequence >= sequence -| 1 or
                try self.noPendingEnrichmentReplayThrough(runtime_stats.applied_sequence, sequence))
            {
                try runtime.markAppliedThrough(sequence);
            }
        }

        pub fn maybeFinalizePendingRowPolicyPublication(self: anytype) !void {
            if (self.local_execution.row_policy_gate.currentPhase() != .preparing) return;
            try self.lockApplyForPortableRuntime();
            defer self.core.unlockApply();
            const pending = (try self.pendingRowPolicyReceipt()) orelse return;
            _ = self.finalizePendingRowPolicyReceiptLocked(pending) catch |err| switch (err) {
                error.RowPolicyReadersActive => return,
                else => return err,
            };
        }

        pub fn mirrorReplicationEncodedBatchMutationCommit(self: anytype, payload: []const u8) !void {
            var ctx = self.batchContext();
            try mirrorReplicationEncodedBatchMutationCommitContext(&ctx, payload);
        }

        pub fn mirrorReplicationReplayPayloadCommit(self: anytype, payload: []const u8) !void {
            var ctx = self.batchContext();
            try mirrorReplicationReplayPayloadCommitContext(&ctx, payload);
        }

        pub fn noPendingEnrichmentReplayThrough(self: anytype, applied_sequence: u64, sequence: u64) !bool {
            const pending = try enrichment_worker.collectPendingDocumentGroups(self.alloc, self.core.replaySource(), applied_sequence);
            defer enrichment_worker.freePendingDocumentGroups(self.alloc, pending);
            for (pending) |group| {
                if (group.sequence <= sequence) return false;
            }
            return true;
        }

        pub fn notifyQueryVisibilityEvent(ctx: *AsyncContext, event: QueryVisibilityEvent) void {
            ctx.visibility_observer.notify(event);
        }

        pub fn notifyQueryVisibilityTargetAdvancedScoped(
            ctx: *AsyncContext,
            target_sequence: u64,
            target_indexes: []const IndexTargetVisibility,
            target_scope_known: bool,
        ) void {
            notifyQueryVisibilityEvent(ctx, .{
                .change = .target_advanced,
                .target_sequence = target_sequence,
                .target_indexes = target_indexes,
                .target_scope_known = target_scope_known,
            });
        }

        pub fn notifyResolverReplayRuntimes(self: anytype, sequence: u64) void {
            if (!self.hasConfiguredResolvers()) return;
            self.notifyResolverReplayRuntimesForced(sequence);
        }

        pub fn notifyResolverReplayRuntimesForced(self: anytype, sequence: u64) void {
            if (self.resolution_runtime) |runtime| runtime.notifySequence(sequence);
            if (self.promotion_runtime) |runtime| runtime.notifySequence(sequence);
        }

        pub fn pendingRowPolicyReceipt(self: anytype) !?row_policy_bundle_mod.Receipt {
            const bytes = self.core.store.get(self.alloc, row_policy_bundle_mod.pending_key) catch |err| switch (err) {
                error.NotFound => return null,
                else => return err,
            };
            defer self.alloc.free(bytes);
            return try row_policy_bundle_mod.Receipt.decode(bytes);
        }

        pub fn preflightReplicationBatchSyncCommit(self: anytype) !void {
            var ctx = self.batchContext();
            try preflightReplicationMirrorSyncCommitContext(&ctx, ctx.replication_async_batch_mirror);
            try preflightReplicationMirrorSyncCommitContext(&ctx, ctx.replication_async_effect_mirror);
        }

        pub fn rememberBulkIngestAllNewIdentityUpserts(self: anytype, doc_ids: []const []const u8) !bool {
            return self.bulk_identity.remember(self.alloc, doc_ids);
        }

        pub fn replicationMutationBarrier(self: anytype) ?*MutationBarrier {
            var barrier: ?*MutationBarrier = null;
            const mirrors = .{
                self.local_execution.replication_async_effect_mirror,
                self.local_execution.replication_async_batch_mirror,
                self.local_execution.replication_async_metadata_mirror,
            };
            inline for (mirrors) |maybe_mirror| {
                if (maybe_mirror) |mirror| {
                    if (mirror.mutation_barrier) |candidate| {
                        if (barrier) |configured| {
                            std.debug.assert(configured == candidate);
                        } else {
                            barrier = candidate;
                        }
                    }
                }
            }
            return barrier;
        }

        pub fn resolveTransactionIntentsInternal(
            self: anytype,
            txn_id: transactions_mod.TxnId,
            status: transactions_mod.TxnStatus,
            commit_version: u64,
            sync_level: types.SyncLevel,
            visibility_cancellation: types.CancellationToken,
            ordered_receipt: ?OrderedApplyReceipt,
            resolved_participant: ?[]const u8,
        ) !void {
            var preparation: RequestPreparationContext = undefined;
            preparation.init(self);
            defer preparation.deinit();
            self.resolveTransactionIntentsPrepared(
                txn_id,
                status,
                commit_version,
                sync_level,
                visibility_cancellation,
                ordered_receipt,
                resolved_participant,
                &preparation.guard,
            ) catch |err| return preparation.mapError(err);
        }

        pub fn resolveTransactionIntentsPrepared(
            self: anytype,
            txn_id: transactions_mod.TxnId,
            status: transactions_mod.TxnStatus,
            commit_version: u64,
            sync_level: types.SyncLevel,
            visibility_cancellation: types.CancellationToken,
            ordered_receipt: ?OrderedApplyReceipt,
            resolved_participant: ?[]const u8,
            preparation: *PreparedRowAllocator,
        ) !void {
            const alloc = preparation.allocator();
            var staging_progress = if (self.local_execution.restore_staging_required.load(.acquire)) try self.restoreStagingStatus(alloc) else null;
            defer if (staging_progress) |*progress| progress.deinit();
            const restore_scope: ?[32]u8 = if (staging_progress) |progress| progress.value.scope.digest() else null;
            const mirror_scoped_restore = restore_scope != null and self.local_execution.replication_async_batch_mirror != null;
            const bypass_replication_write_gate = ordered_receipt != null and !mirror_scoped_restore;
            var replication_mutation = if (!bypass_replication_write_gate) self.acquireReplicationMutationShared() else null;
            defer if (replication_mutation) |*lease| lease.release();
            if (!bypass_replication_write_gate) try self.enforceReplicationWriteGate();
            if (mirror_scoped_restore) try self.flushDurableReplicationOutboxes();
            if (status != .committed) {
                try self.lockApplyForPortableRuntime();
                defer self.core.unlockApply();
                var marker_value_buf: [ordered_apply_receipt_value_len]u8 = undefined;
                const marker_writes: []const docstore_mod.KVPair = if (ordered_receipt) |identity| blk: {
                    switch (try orderedApplyDisposition(try readOrderedApplyReceipt(self.alloc, self.core.store), identity)) {
                        .already_applied => return,
                        .apply => {},
                    }
                    break :blk &.{orderedApplyReceiptWrite(identity, &marker_value_buf)};
                } else &.{};
                _ = self.core.resolveTransactionIntentsWithExtraBatch(txn_id, status, commit_version, .{
                    .completion_writes = marker_writes,
                    .resolved_participant = resolved_participant,
                }) catch |err| switch (err) {
                    // An abort decision for a participant that was declared but
                    // never enlisted is already satisfied. Treat it as an
                    // idempotent no-op so coordinator recovery can acknowledge it.
                    transactions_mod.TxnError.TxnNotFound => {
                        if (marker_writes.len != 0) try self.core.store.putBatch(marker_writes, &.{});
                        return;
                    },
                    else => return err,
                };
                return;
            }

            var attempts: usize = 0;
            while (attempts < 8) : (attempts += 1) {
                const schema_namespace = self.core.schemaNamespaceGeneration();
                var intents = try self.core.collectTransactionIntentBatch(alloc, txn_id);
                defer intents.deinit(alloc);
                const intent_keys = try alloc.alloc([]const u8, intents.writes.len + intents.deletes.len);
                defer alloc.free(intent_keys);
                for (intents.writes, 0..) |write, i| intent_keys[i] = write.key;
                for (intents.deletes, 0..) |key, i| intent_keys[intents.writes.len + i] = key;
                if (intents.writes.len == 0 and intents.deletes.len == 0) {
                    try self.lockApplyForPortableRuntime();
                    if (schema_namespace != self.core.schemaNamespaceGeneration()) {
                        self.core.unlockApply();
                        return error.PreparedGenerationChanged;
                    }
                    var marker_value_buf: [ordered_apply_receipt_value_len]u8 = undefined;
                    const marker_writes: []const docstore_mod.KVPair = if (ordered_receipt) |identity| blk: {
                        switch (try orderedApplyDisposition(try readOrderedApplyReceipt(self.alloc, self.core.store), identity)) {
                            .already_applied => {
                                self.core.unlockApply();
                                return;
                            },
                            .apply => {},
                        }
                        break :blk &.{orderedApplyReceiptWrite(identity, &marker_value_buf)};
                    } else &.{};
                    const outcome = self.core.resolveTransactionIntentsWithExtraBatch(
                        txn_id,
                        status,
                        commit_version,
                        .{
                            .completion_writes = marker_writes,
                            .resolved_participant = resolved_participant,
                            .expected_intent_revision = intents.revision,
                            .known_intent_keys = intent_keys,
                        },
                    ) catch |err| {
                        self.core.unlockApply();
                        if (err == error.IntentSnapshotChanged) continue;
                        return err;
                    };
                    self.core.unlockApply();
                    try self.flushTransactionReplicationOutbox(txn_id);
                    try self.waitForResolvedTransactionSyncWithCancellation(sync_level, outcome.replay_sequence, visibility_cancellation);
                    return;
                }

                const writes = try alloc.alloc(types.BatchWrite, intents.writes.len);
                defer alloc.free(writes);
                for (intents.writes, 0..) |write, i| writes[i] = .{ .key = write.key, .value = write.value };
                var durable_rows = std.StringHashMapUnmanaged([]const u8).empty;
                defer durable_rows.deinit(alloc);
                for (intents.prepared_rows, 0..) |maybe_row, i| if (maybe_row) |row| {
                    try durable_rows.put(alloc, intents.writes[i].key, row);
                };
                self.batchInternalWithPreparationAllocator(.{
                    .writes = writes,
                    .deletes = intents.deletes,
                    .timestamp_ns = commit_version,
                    .sync_level = sync_level,
                    .restore_staging_scope = restore_scope,
                }, null, .{
                    .visibility_cancellation = visibility_cancellation,
                    .bypass_replication_write_gate = bypass_replication_write_gate,
                    .ordered_apply_receipt = ordered_receipt,
                    .durable_rows = &durable_rows,
                    .transaction_resolution = .{
                        .txn_id = txn_id,
                        .status = status,
                        .commit_version = commit_version,
                        .expected_intent_revision = intents.revision,
                        .intent_keys = intent_keys,
                        .resolved_participant = resolved_participant,
                        .schema_binding = intents.schema_binding,
                        .schema_namespace_generation = schema_namespace,
                    },
                }, preparation) catch |err| {
                    if (err == error.IntentSnapshotChanged) continue;
                    return err;
                };
                return;
            }
            return error.TransactionPrepareContention;
        }

        pub fn restoreStagingStatus(self: anytype, alloc: Allocator) !?std.json.Parsed(@import("restore_staging.zig").Progress) {
            const staging = @import("restore_staging.zig");
            const raw = (try self.core.getStoreValue(alloc, staging.key)) orelse return null;
            defer alloc.free(raw);
            return try staging.Progress.decode(alloc, raw);
        }

        pub fn shouldApplySplitReplicationLocked(self: anytype, req: types.BatchRequest) !bool {
            if (req.split_replication) |replication| {
                if (replication.transition_id == 0 or
                    replication.attempt_epoch == 0 or
                    replication.source_group_id == replication.destination_group_id)
                {
                    return error.InvalidBatchRequest;
                }
                const marker = try self.core.loadSplitBootstrapMarker(self.alloc);
                switch (replication.operation) {
                    .bootstrap_chunk => {
                        const bootstrap_sequence = replication.bootstrap_sequence orelse
                            return error.InvalidBatchRequest;
                        const existing = marker orelse return error.SplitBootstrapIncomplete;
                        if (!splitMarkerMatches(existing, replication.transition_id, replication.attempt_epoch, replication.source_group_id, replication.destination_group_id)) {
                            return error.ConflictingSplitTransition;
                        }
                        if (existing.bootstrap_complete) return false;
                        if (bootstrap_sequence != try self.core.loadSplitDeltaFinalSeq(self.alloc))
                            return error.StaleSplitBootstrap;
                    },
                    .delta => {
                        const complete = marker orelse return error.SplitBootstrapIncomplete;
                        if (!splitMarkerMatches(complete, replication.transition_id, replication.attempt_epoch, replication.source_group_id, replication.destination_group_id)) {
                            return error.ConflictingSplitTransition;
                        }
                        if (!complete.bootstrap_complete) return error.SplitBootstrapIncomplete;
                        if (replication.sequence == 0) return error.InvalidBatchRequest;
                        const applied = try self.core.loadSplitDeltaFinalSeq(self.alloc);
                        if (replication.sequence <= applied) return false;
                        if (replication.previous_sequence) |previous| {
                            if (previous >= replication.sequence) return error.InvalidBatchRequest;
                            if (previous != applied) return error.SplitReplicationSequenceGap;
                        } else if (replication.sequence != applied + 1) {
                            return error.SplitReplicationSequenceGap;
                        }
                    },
                    .checkpoint => {
                        const checkpoint = req.split_checkpoint orelse return error.MissingSplitReplicationCheckpoint;
                        if (checkpoint.kind == .source_ack or
                            checkpoint.transition_id != replication.transition_id or
                            checkpoint.attempt_epoch != replication.attempt_epoch or
                            checkpoint.source_group_id != replication.source_group_id or
                            checkpoint.destination_group_id != replication.destination_group_id or
                            checkpoint.delta_sequence != replication.sequence)
                        {
                            return error.InvalidBatchRequest;
                        }
                        if (replication.bootstrap_sequence) |bootstrap_sequence| {
                            if (bootstrap_sequence != checkpoint.delta_sequence) return error.StaleSplitBootstrap;
                        } else if (checkpoint.kind == .destination_begin) {
                            return error.InvalidBatchRequest;
                        }
                        if (marker) |existing| {
                            if (!splitMarkerMatches(existing, replication.transition_id, replication.attempt_epoch, replication.source_group_id, replication.destination_group_id)) {
                                return error.ConflictingSplitTransition;
                            }
                            const applied = try self.core.loadSplitDeltaFinalSeq(self.alloc);
                            if (checkpoint.delta_sequence > applied) return error.SplitReplicationSequenceGap;
                            if (checkpoint.kind == .destination_begin or existing.bootstrap_complete) return false;
                        } else if (checkpoint.kind == .destination_complete) {
                            return error.SplitBootstrapIncomplete;
                        }
                    },
                }
            } else if (req.split_checkpoint) |checkpoint| {
                if (checkpoint.kind != .source_ack or checkpoint.transition_id == 0) return error.MissingSplitReplicationContext;
                if (try self.core.loadSplitBootstrapMarker(self.alloc)) |marker| {
                    if (!splitMarkerMatches(marker, checkpoint.transition_id, checkpoint.attempt_epoch, checkpoint.source_group_id, checkpoint.destination_group_id)) {
                        return error.ConflictingSplitTransition;
                    }
                    if (checkpoint.delta_sequence <= try self.core.loadSplitDeltaFinalSeq(self.alloc)) return false;
                }
            }
            return true;
        }

        pub fn splitMarkerMatches(marker: range_state_mod.SplitBootstrapMarker, transition_id: u64, attempt_epoch: u64, source_group_id: u64, destination_group_id: u64) bool {
            return marker.transition_id == transition_id and
                marker.attempt_epoch == attempt_epoch and
                marker.source_group_id == source_group_id and
                marker.destination_group_id == destination_group_id;
        }

        pub fn thinReplayInputsHaveDerivedWork(
            req: types.BatchRequest,
            deleted_artifact_keys: []const []u8,
            changed_artifact_keys: []const []u8,
            derived_changed_flags: []const bool,
        ) bool {
            if (req.deletes.len != 0 or
                req.graph_writes.len != 0 or
                req.graph_deletes.len != 0 or
                deleted_artifact_keys.len != 0 or
                changed_artifact_keys.len != 0 or
                req.split_checkpoint != null or
                req.split_replication != null or
                req.split_transition != null or
                req.transaction != null)
            {
                return true;
            }
            for (derived_changed_flags) |changed| {
                if (changed) return true;
            }
            return false;
        }

        pub fn unchangedDerivedReplayTargetsServiceable(self: anytype, alloc: Allocator) !bool {
            for (self.core.index_manager.dense_indexes.items) |*entry| {
                if (!try index_manager_mod.denseConfigRequiresArtifactCoverage(alloc, entry.config)) continue;
                const expected = (try loadDenseArtifactTargetCounter(alloc, self.core.store, entry.config.name)) orelse return false;
                if (expected != 0 and entry.index.stats().active_count == 0) return false;
            }
            return true;
        }

        pub fn validateLiveReplicationIntegrityEffects(self: anytype, alloc: Allocator, txn: anytype, req: types.BatchRequest) !void {
            const integrity = @import("relational_integrity.zig");
            const activation = @import("relational_integrity_activation.zig");
            const retirement = @import("relational_integrity_retirement.zig");
            const catalog_mod = @import("relational_integrity_catalog.zig");
            var loaded: ?catalog_mod.Catalog = null;
            defer if (loaded) |*catalog| catalog.deinit();
            for (req.writes) |write| {
                if (!isProtectedIntegrityKey(write.key)) continue;
                const maintenance = @import("relational_index_maintenance_contract.zig");
                if (maintenance.isControlKey(write.key)) {
                    // Index maintenance does not require an FK/unique catalog.
                    // The replicated desired state is fenced by its index identity,
                    // never by this replica's independent coverage progress.
                    const id = try @import("relational_index_records.zig").Id.decode(write.key[maintenance.control_prefix.len..]);
                    var pinned = self.core.relational_indexes.acquire() orelse return error.PreparedGenerationChanged;
                    defer pinned.deinit();
                    const index = for (pinned.plan.boundIndexes()) |candidate| {
                        if (candidate.id().mapKey() == id.mapKey()) break candidate;
                    } else return error.PreparedGenerationChanged;
                    const next = try maintenance.Control.decode(write.value);
                    const before = try maintenance.readControl(txn, index.id());
                    const identical = next.epoch == before.epoch and std.mem.eql(u8, &next.last_request, &before.last_request);
                    if (!identical and (before.epoch == std.math.maxInt(u64) or next.epoch != before.epoch + 1))
                        return error.PreparedGenerationChanged;
                    continue;
                }
                if (loaded == null) loaded = try catalog_mod.decode(alloc, txn.get(catalog_mod.key) catch |err| switch (err) {
                    error.NotFound => return error.IntegrityCatalogChanged,
                    else => return err,
                });
                const catalog = loaded.?;
                if (std.mem.eql(u8, write.key, activation.key)) {
                    const next = try activation.Progress.decode(write.value);
                    if (!next.matches(catalog, try activation.ownership(txn)) or next.schema_version != catalog.schema_version)
                        return error.IntegrityCatalogChanged;
                } else if (std.mem.eql(u8, write.key, retirement.key)) {
                    const next = try retirement.Progress.decode(write.value);
                    if (!std.mem.eql(u8, &next.owner, &try activation.ownership(txn)) or
                        !std.mem.eql(u8, &next.generation_set, &activation.generationSet(catalog)) or
                        next.schema_version != catalog.schema_version) return error.IntegrityCatalogChanged;
                    for (next.generations) |generation| if (catalog.findGeneration(generation) == null) return error.IntegrityCatalogChanged;
                } else {
                    const parsed = try integrity.parseKey(write.key);
                    const binding = catalog.findGeneration(parsed.address.generation) orelse return error.IntegrityCatalogChanged;
                    if (binding.retired or binding.definition.kind != .unique) return error.IntegrityCatalogChanged;
                    if (!self.core.byteRange().contains(&parsed.address.routing)) return error.KeyOutOfRange;
                    try integrity.validateOperation(.{ .kind = .put, .key = write.key, .routing_key = &parsed.address.routing, .value = write.value });
                }
            }
            for (req.deletes) |key| {
                if (!isProtectedIntegrityKey(key)) continue;
                if (loaded == null) loaded = try catalog_mod.decode(alloc, txn.get(catalog_mod.key) catch |err| switch (err) {
                    error.NotFound => return error.IntegrityCatalogChanged,
                    else => return err,
                });
                const parsed = try integrity.parseKey(key);
                const binding = loaded.?.findGeneration(parsed.address.generation) orelse return error.IntegrityCatalogChanged;
                if (binding.definition.kind != .unique) return error.IntegrityCatalogChanged;
                if (!self.core.byteRange().contains(&parsed.address.routing)) return error.KeyOutOfRange;
            }
        }

        pub fn validateMergeCleanupPageLocked(self: anytype, state: merge_state_mod.State, req: types.BatchRequest) !void {
            const page = req.merge_page.?;
            const merged = state.merged_range.?;
            const donor: types.ByteRange = if (!std.mem.eql(u8, merged.start, state.receiver_base_range.start))
                .{ .start = merged.start, .end = state.receiver_base_range.start }
            else
                .{ .start = state.receiver_base_range.end, .end = merged.end };
            const lower = if (page.after.len != 0) try internal_keys.documentKeyAlloc(self.alloc, page.after) else try documentRangeLowerAlloc(self.alloc, donor.start);
            defer self.alloc.free(lower);
            const upper = if (donor.end.len != 0) try documentRangeLowerAlloc(self.alloc, donor.end) else null;
            defer if (upper) |value| self.alloc.free(value);
            var txn = try self.core.store.beginReadTxn();
            defer txn.abort();
            // Cleanup does not need row payload hydration or JSON reconstruction.
            var cursor = try txn.openPhysicalCursorAdapter();
            defer cursor.close();
            cursor.setUpperBound(upper);
            var index: usize = 0;
            var item = try cursor.seekAtOrAfter(lower);
            while (item) |row| : (item = try cursor.next()) {
                if (upper) |bound| if (std.mem.order(u8, row.key, bound) != .lt) break;
                const logical = (try internal_keys.decodeDocumentComponentAlloc(self.alloc, row.key)) orelse continue;
                defer self.alloc.free(logical);
                if (internal_keys.isInternalUserKey(logical)) continue;
                if (page.after.len != 0 and std.mem.order(u8, logical, page.after) != .gt) continue;
                if (index != 0 and std.mem.eql(u8, logical, req.deletes[index - 1])) continue;
                if (index == req.deletes.len or !std.mem.eql(u8, logical, req.deletes[index])) return error.InvalidMergePage;
                index += 1;
                if (index == req.deletes.len and !page.exhausted) return;
            }
            if (index != req.deletes.len) return error.InvalidMergePage;
        }

        pub fn validateResolvedKeyOwnership(self: anytype, key: []const u8) !void {
            const integrity = @import("relational_integrity.zig");
            if (integrity.isKey(key)) {
                const parsed = try integrity.parseKey(key);
                return self.core.validateKeyOwnership(&parsed.address.routing);
            }
            if (!isMetadataKey(key)) try self.core.validateKeyOwnership(key);
        }

        pub fn validateRestoreStagingReplicationEffects(self: anytype, alloc: Allocator, txn: anytype, req: types.BatchRequest) !void {
            if (req.relational_index_maintenance != null) return error.InvalidRestoreStagingCommand;
            const integrity = @import("relational_integrity.zig");
            const activation = @import("relational_integrity_activation.zig");
            const catalog_mod = @import("relational_integrity_catalog.zig");
            if (req.transforms.len != 0 or req.graph_writes.len != 0 or req.graph_deletes.len != 0 or req.relational_repair or req.relational_activation != null or req.relational_retirement != null or req.integrity.len != 0 or req.integrity_commands.len != 0) return error.InvalidRestoreStagingCommand;
            const raw_catalog = txn.get(catalog_mod.key) catch |err| switch (err) {
                error.NotFound => return error.IntegrityCatalogChanged,
                else => return err,
            };
            var catalog = try catalog_mod.decode(alloc, raw_catalog);
            defer catalog.deinit();
            for (req.writes) |write| {
                if (std.mem.eql(u8, write.key, activation.key)) {
                    const before = try activation.status(txn, catalog);
                    const after = try activation.Progress.decode(write.value);
                    if (!after.matches(catalog, before.owner) or after.schema_version != catalog.schema_version) return error.IntegrityCatalogChanged;
                } else {
                    const parsed = try integrity.parseKey(write.key);
                    if (!self.core.byteRange().contains(&parsed.address.routing)) return error.KeyOutOfRange;
                    const binding = catalog.findGeneration(parsed.address.generation) orelse return error.IntegrityCatalogChanged;
                    if (binding.retired or binding.definition.kind != .unique) return error.IntegrityCatalogChanged;
                    try integrity.validateOperation(.{ .kind = .put, .key = write.key, .routing_key = &parsed.address.routing, .value = write.value });
                }
            }
            for (req.deletes) |key| {
                const parsed = try integrity.parseKey(key);
                if (!self.core.byteRange().contains(&parsed.address.routing)) return error.KeyOutOfRange;
                const binding = catalog.findGeneration(parsed.address.generation) orelse return error.IntegrityCatalogChanged;
                if (binding.retired or binding.definition.kind != .unique) return error.IntegrityCatalogChanged;
            }
        }

        pub fn waitForResolvedTransactionSync(self: anytype, sync_level: types.SyncLevel, sequence: u64) !void {
            try self.waitForResolvedTransactionSyncWithCancellation(sync_level, sequence, .none);
        }

        pub fn activeSplitShadow(self: anytype) ?*ShadowState {
            // Recovery and the serving wrapper share the same ticket/mutex owner.
            // The context is updated under the primary apply fence during split setup
            // and teardown; a copied wrapper must not capture stale split ownership.
            if (self.transaction_recovery_local_context) |ctx| return ctx.split_shadow;
            return self.shadow;
        }

        pub fn appendNeighborContextHintsToRecord(ctx: *const BatchExecutionContext, record: *change_journal_mod.Record, keys_to_add: []const []const u8) !void {
            if (keys_to_add.len > 0) {
                const merged = merge: {
                    var keys = std.ArrayListUnmanaged([]const u8).empty;
                    defer keys.deinit(ctx.alloc);
                    try keys.appendSlice(ctx.alloc, record.changed_doc_keys);
                    errdefer for (keys.items[record.changed_doc_keys.len..]) |key| ctx.alloc.free(key);
                    var seen = std.StringHashMapUnmanaged(void).empty;
                    defer seen.deinit(ctx.alloc);
                    for (record.changed_doc_keys) |key| try seen.put(ctx.alloc, key, {});
                    for (keys_to_add) |key| try appendUniqueReplayRecordKeyWithSet(ctx.alloc, &keys, &seen, key);
                    break :merge try keys.toOwnedSlice(ctx.alloc);
                };
                ctx.alloc.free(record.changed_doc_keys);
                record.changed_doc_keys = merged;
                if (!journalRecordHasHint(record.*, .enrichment)) {
                    const targets = try std.mem.concat(ctx.alloc, change_journal_mod.TargetHint, &.{ record.target_hints, &.{.enrichment} });
                    ctx.alloc.free(record.target_hints);
                    record.target_hints = targets;
                }
            }
        }

        pub fn appendReplicationBatchMutationCommitLockedContext(ctx: *const BatchExecutionContext, request: types.BatchRequest) !?ReplicationDeferredCommitGate {
            const projected = replicationCommitContext(ctx);
            return try replication_commit.appendReplicationBatchMutationCommitLockedContext(&projected, request);
        }

        pub fn appendReplicationEncodedBatchMutationCommitLockedContext(ctx: *const BatchExecutionContext, payload: []const u8) !?ReplicationDeferredCommitGate {
            const projected = replicationCommitContext(ctx);
            return try replication_commit.appendReplicationEncodedBatchMutationCommitLockedContext(&projected, payload);
        }

        pub fn appendReplicationEncodedBatchMutationCommitLockedContextStrict(ctx: *const BatchExecutionContext, payload: []const u8, strict_append: bool) !?ReplicationDeferredCommitGate {
            const projected = replicationCommitContext(ctx);
            return try replication_commit.appendReplicationEncodedBatchMutationCommitLockedContextStrict(&projected, payload, strict_append);
        }

        pub fn appendReplicationReplayPayloadCommitLockedContext(ctx: *const BatchExecutionContext, payload: []const u8) !?ReplicationDeferredCommitGate {
            const projected = replicationCommitContext(ctx);
            return try replication_commit.appendReplicationReplayPayloadCommitLockedContext(&projected, payload);
        }

        pub fn appliedSequenceUpdatesWithConfigHashes(
            alloc: Allocator,
            index_manager: *index_manager_mod.IndexManager,
            updates: []const apply_state.AppliedSequenceUpdate,
        ) ![]apply_state.AppliedSequenceUpdate {
            const enriched = try alloc.alloc(apply_state.AppliedSequenceUpdate, updates.len);
            for (updates, 0..) |update, i| {
                enriched[i] = update;
                if (enriched[i].config_hash == 0) {
                    if (index_manager.get(update.index_name)) |cfg| {
                        enriched[i].config_hash = types.indexConfigHash(cfg.*);
                    }
                }
                // Native batches publish their WAL watermark directly; their generic
                // update never writes a count sidecar. Avoid pinning a read generation
                // on every ingest batch just to discard that count below.
                if (enriched[i].published_count == null and
                    !index_manager.densePostingWalAuthoritativeByName(update.index_name))
                {
                    if (index_manager.denseIndex(update.index_name)) |entry| {
                        enriched[i].published_count = entry.index.servingActiveCountForCheckpoint();
                    }
                }
            }
            return enriched;
        }

        pub fn applyCommittedBatchToShadow(self: anytype, shadow: *ShadowState, batch: derived_types.DerivedBatch) !void {
            const managed_indexes = try shadow.manager.managedIndexes(self.alloc);
            defer {
                for (managed_indexes) |index_ref| self.alloc.free(@constCast(index_ref.name));
                self.alloc.free(managed_indexes);
            }

            const async_resources = self.core.asyncResources();
            const ctx = AsyncContext{
                .alloc = self.alloc,
                .io = self.backend_runtime.io(),
                .store = async_resources.store,
                .relational_base_rows = relationalColumns(self) != null,
                .applied_sequence_checkpoint_path = async_resources.applied_sequence_checkpoint_path,
                .index_manager = shadow.manager,
                .apply_mutex = async_resources.apply_mutex,
                .snapshot_replay_admission = async_resources.snapshot_replay_admission,
                .allow_graph_materialization = false,
                .projection_only = true,
            };

            for (managed_indexes) |index_ref| {
                if (!batchAffectsManagedIndex(shadow.manager, batch, index_ref)) continue;
                try applyDerivedBatchToIndexContext(&ctx, batch, index_ref);
            }
        }

        pub fn applyCommittedBatchToShadowOrdered(self: anytype, batch: derived_types.DerivedBatch, ticket: u64) void {
            const shadow = activeSplitShadow(self) orelse return;
            const io = self.backend_runtime.io() orelse self.backend_runtime.filesystemIo() orelse std.Options.debug_io;
            shadow.apply_mutex.lockUncancelable(io);
            while (shadow.applied_ticket != ticket) {
                shadow.apply_advanced.waitUncancelable(io, &shadow.apply_mutex);
            }
            defer {
                shadow.applied_ticket = ticket + 1;
                shadow.apply_advanced.broadcast(io);
                shadow.apply_mutex.unlock(io);
            }
            if (shadow.repair_required) return;

            applyCommittedBatchToShadow(self, shadow, batch) catch |err| {
                // The primary row and its durable replay record are already committed.
                // Do not report a false write failure. Instead fail split publication
                // closed; the durable replay remains the repair source of truth.
                shadow.repair_required = true;
                std.log.err(
                    "split shadow apply failed after primary commit ticket={} sequence={} err={}",
                    .{ ticket, batch.sequence, err },
                );
            };
        }

        pub fn asyncIndexProfileEnabled() bool {
            const cached = D.async_index_profile_enabled_cache.*.load(.monotonic);
            if (cached == 1 or cached == 2) return cached == 2;
            if (cached == 3) return waitForCachedBool(&D.async_index_profile_enabled_cache.*);
            if (D.async_index_profile_enabled_cache.*.cmpxchgStrong(0, 3, .acq_rel, .monotonic) != null) {
                return waitForCachedBool(&D.async_index_profile_enabled_cache.*);
            }
            if (comptime builtin.os.tag == .freestanding) {
                D.async_index_profile_enabled_cache.*.store(1, .release);
                return false;
            }
            const enabled = if (getenv("ANTFLY_ASYNC_INDEX_PROFILE")) |raw_z|
                envBoolEnabled(raw_z)
            else
                benchMetricsEnabled();
            D.async_index_profile_enabled_cache.*.store(if (enabled) 2 else 1, .release);
            return enabled;
        }

        pub const atomicMaxU64 = execution_resources.atomicMaxU64;

        pub fn benchMetricsEnabled() bool {
            const cached = D.bench_metrics_enabled_cache.*.load(.monotonic);
            if (cached == 1 or cached == 2) return cached == 2;
            if (cached == 3) return waitForCachedBool(&D.bench_metrics_enabled_cache.*);
            if (D.bench_metrics_enabled_cache.*.cmpxchgStrong(0, 3, .acq_rel, .monotonic) != null) {
                return waitForCachedBool(&D.bench_metrics_enabled_cache.*);
            }
            if (comptime builtin.os.tag == .freestanding) {
                D.bench_metrics_enabled_cache.*.store(1, .release);
                return false;
            }
            const raw_z = getenv("ANTFLY_BENCH_METRICS") orelse
                getenv("ANTFLY_BENCH_BATCH_PROFILE") orelse {
                D.bench_metrics_enabled_cache.*.store(1, .release);
                return false;
            };
            const enabled = envBoolEnabled(raw_z);
            D.bench_metrics_enabled_cache.*.store(if (enabled) 2 else 1, .release);
            return enabled;
        }

        pub fn boundaryFailureErrorName(failure: *const runtime_failure_abi.FailureIdentity, fallback: anyerror) []const u8 {
            return if (failure.error_name_len > 0) failure.errorName() else @errorName(fallback);
        }

        pub fn cachedEnvUsize(cache: *std.atomic.Value(usize), name: [:0]const u8, default_value: usize) usize {
            const cached = cache.load(.acquire);
            if (cached != 0) return cached - 1;

            const value = readEnvUsize(name, default_value);
            const encoded = value +% 1;
            _ = cache.cmpxchgWeak(0, encoded, .acq_rel, .acquire);
            return cache.load(.acquire) - 1;
        }

        pub fn cachedOptionalEnvUsize(cache: *std.atomic.Value(usize), name: [:0]const u8) ?usize {
            const cached = cache.load(.acquire);
            if (cached != 0) return if (cached == 1) null else cached - 2;

            if (comptime builtin.os.tag == .freestanding) {
                _ = cache.cmpxchgWeak(0, 1, .acq_rel, .acquire);
            } else {
                const encoded: usize = blk: {
                    const raw = getenv(name) orelse break :blk 1;
                    if (raw.len == 0) break :blk 1;
                    const value = std.fmt.parseUnsigned(usize, raw, 10) catch break :blk 1;
                    break :blk value +% 2;
                };
                _ = cache.cmpxchgWeak(0, encoded, .acq_rel, .acquire);
            }

            const loaded = cache.load(.acquire);
            return if (loaded == 1) null else loaded - 2;
        }

        pub fn clampReplayTruncationForRepairPins(
            alloc: Allocator,
            checkpoint: ?index_repair_state.Location,
            effective: u64,
        ) !u64 {
            if (comptime builtin.os.tag == .freestanding) {
                return effective;
            } else {
                const location = checkpoint orelse return effective;
                var state = index_repair_state.loadAt(alloc, location) catch |err| switch (err) {
                    error.FileNotFound => return effective,
                    // A malformed local repair checkpoint may have contained a zero or
                    // finalized replay pin. Retain everything until an operator repairs
                    // the checkpoint; never convert corruption into replay loss.
                    error.InvalidIndexRepairState => return 0,
                    else => return err,
                };
                defer state.deinit(alloc);
                const pin = state.minimumRetainAfterSequence() orelse return effective;
                return @min(effective, pin);
            }
        }

        pub fn currentTimeNs() u64 {
            return platform_clock.Clock.real().nowRealtimeNs();
        }

        pub fn deferExternalBulkExecutorNotification(ctx: *AsyncContext, sync_level: types.SyncLevel, sequence: u64) bool {
            switch (sync_level) {
                .propose, .write, .enrichments => {},
                .full_text, .full_index => return false,
            }
            if (ctx.dense_admission.external_sessions.load(.acquire) == 0) return false;
            ctx.dense_admission.deferSequence(sequence);
            return true;
        }

        pub const elapsedSince = execution_resources.elapsedSince;

        pub fn encodeStoreLookupKeyAlloc(self: anytype, alloc: Allocator, key: []const u8) ![]u8 {
            // Point reads deliberately avoid the global apply lock. Fence before
            // consulting the runtime schema so a native generation swap cannot encode
            // a lookup with the old keyspace and execute it against the new store.
            if (self.core.store.portableImportPublicationInProgress()) {
                return error.PortableImportPublicationInProgress;
            }
            if (internal_keys.isInternalUserKey(key) or std.mem.startsWith(u8, key, "\x00\x00__metadata__:") or isSplitMetadataKey(key)) {
                return try alloc.dupe(u8, key);
            }
            var schema_view = self.core.acquireSchemaView();
            defer if (schema_view) |*view| view.release();
            return try encodeStoreLookupKeyWithPinnedSchemaAlloc(self, alloc, key, schema_view);
        }

        pub const encodeStoreLookupKeyWithPinnedSchemaAlloc = execution_resources.encodeStoreLookupKeyWithPinnedSchemaAlloc;

        pub fn encodeThinReplayRecordPayload(
            alloc: Allocator,
            req: types.BatchRequest,
            extracted: []const mapper.ExtractedWrite,
            deleted_artifact_keys: []const []u8,
            changed_artifact_keys: []const []u8,
            overwritten_flags: []const bool,
            derived_changed_flags: []const bool,
            sequence: u64,
            include_generated_enrichment_hint: bool,
            index_manager: ?*index_manager_mod.IndexManager,
            sync_targets_out: ?*ManagedSyncTargets,
            owning_table: ?[]const u8,
        ) ![]u8 {
            if ((index_manager == null) != (sync_targets_out == null)) return error.InvalidArgument;
            if (derived_changed_flags.len != req.writes.len) return error.InvalidArgument;
            var changed_doc_keys = std.ArrayListUnmanaged([]const u8).empty;
            var changed_doc_key_set = std.StringHashMapUnmanaged(void).empty;
            errdefer {
                for (changed_doc_keys.items) |key| alloc.free(@constCast(key));
                changed_doc_keys.deinit(alloc);
            }
            var deleted_doc_keys = std.ArrayListUnmanaged([]const u8).empty;
            var deleted_doc_key_set = std.StringHashMapUnmanaged(void).empty;
            errdefer {
                for (deleted_doc_keys.items) |key| alloc.free(@constCast(key));
                deleted_doc_keys.deinit(alloc);
            }
            var overwritten_doc_keys = std.ArrayListUnmanaged([]const u8).empty;
            var overwritten_doc_key_set = std.StringHashMapUnmanaged(void).empty;
            errdefer {
                for (overwritten_doc_keys.items) |key| alloc.free(@constCast(key));
                overwritten_doc_keys.deinit(alloc);
            }
            var thin_changed_artifact_keys = std.ArrayListUnmanaged([]const u8).empty;
            var thin_changed_artifact_key_set = std.StringHashMapUnmanaged(void).empty;
            errdefer {
                for (thin_changed_artifact_keys.items) |key| alloc.free(@constCast(key));
                thin_changed_artifact_keys.deinit(alloc);
            }
            var target_hints = std.ArrayListUnmanaged(change_journal_mod.TargetHint).empty;
            errdefer target_hints.deinit(alloc);
            defer {
                changed_doc_key_set.deinit(alloc);
                deleted_doc_key_set.deinit(alloc);
                overwritten_doc_key_set.deinit(alloc);
                thin_changed_artifact_key_set.deinit(alloc);
            }

            var saw_overwritten = false;

            for (req.writes, 0..) |write, i| {
                if (!derived_changed_flags[i]) continue;
                const extracted_write = extracted[i];
                // PR #957 review, blocker 8: a write with no content (an
                // `_edges`-only document is one shape; an `_embeddings`-only write
                // is another) can never match a generated enrichment request --
                // those are declared against document FIELDS, and a content-less
                // write has none to chunk or embed. Gating this whole block on
                // hasDocument() (rather than waking the enrichment worker for
                // every write whenever any generated-enrichment target exists)
                // keeps that content-less write's key out of changed_doc_keys
                // entirely. Recording it there would otherwise put the #938 fix
                // right back where it started: full-text/algebraic replay also
                // consumes this list, and once this batch's OTHER writes give the
                // record a full_text hint, it would wait forever for a primary row
                // this key will never have.
                if (extracted_write.hasDocument()) {
                    try appendUniqueReplayRecordKeyWithSet(alloc, &changed_doc_keys, &changed_doc_key_set, write.key);
                    try appendUniqueReplayRecordHint(alloc, &target_hints, .full_text);
                    try appendUniqueReplayRecordHint(alloc, &target_hints, .algebraic);
                    if (include_generated_enrichment_hint) {
                        try appendUniqueReplayRecordHint(alloc, &target_hints, .enrichment);
                    }
                }

                for (extracted_write.dense_embeddings) |embedding| {
                    if (embedding.artifact_key) |artifact_key| try appendUniqueReplayRecordKeyWithSet(alloc, &thin_changed_artifact_keys, &thin_changed_artifact_key_set, artifact_key);
                    try appendUniqueReplayRecordHint(alloc, &target_hints, .dense_vector);
                }
                for (extracted_write.sparse_embeddings) |embedding| {
                    if (embedding.artifact_key) |artifact_key| try appendUniqueReplayRecordKeyWithSet(alloc, &thin_changed_artifact_keys, &thin_changed_artifact_key_set, artifact_key);
                    try appendUniqueReplayRecordHint(alloc, &target_hints, .sparse_vector);
                }
                if (extracted_write.mentioned_graph_indexes.len > 0 or extracted_write.graph_writes.len > 0) {
                    // Graph-only content (an `_edges`-only document, for example) has
                    // no full-text/dense/sparse/algebraic payload. changed_doc_keys
                    // feeds those replay workers, which reconstruct a synthetic
                    // upsert for every key it contains and expect to find a primary
                    // row. A document whose only content is edges never has one, so
                    // recording its key here would make full-text replay retry
                    // ReplayDocumentNotVisible forever (issue #938). The graph
                    // worker replays this change through changed_artifact_keys
                    // instead, so only the hint belongs here.
                    try appendUniqueReplayRecordHint(alloc, &target_hints, .graph);
                }
                if (overwritten_flags[i] and extracted_write.hasDocument()) {
                    try appendUniqueReplayRecordKeyWithSet(alloc, &overwritten_doc_keys, &overwritten_doc_key_set, write.key);
                    saw_overwritten = true;
                }
            }

            if (saw_overwritten) {
                try appendUniqueReplayRecordHint(alloc, &target_hints, .full_text);
                try appendUniqueReplayRecordHint(alloc, &target_hints, .dense_vector);
                try appendUniqueReplayRecordHint(alloc, &target_hints, .sparse_vector);
                try appendUniqueReplayRecordHint(alloc, &target_hints, .algebraic);
            }

            for (changed_artifact_keys) |key| {
                try appendUniqueReplayRecordKeyWithSet(alloc, &thin_changed_artifact_keys, &thin_changed_artifact_key_set, key);
                if (internal_keys.isEmbeddingArtifactKey(key) or internal_keys.isDerivedEmbeddingArtifactKey(key)) {
                    try appendUniqueReplayRecordHint(alloc, &target_hints, .dense_vector);
                    try appendUniqueReplayRecordHint(alloc, &target_hints, .sparse_vector);
                }
                if (internal_keys.isChunkArtifactRecordKey(key)) try appendUniqueReplayRecordHint(alloc, &target_hints, .full_text);
                if (internal_keys.isGraphEdgeArtifactKey(key) or internal_keys.isAssetArtifactKey(key) or internal_keys.isChunkArtifactRecordKey(key)) try appendUniqueReplayRecordHint(alloc, &target_hints, .graph);
                if (internal_keys.isAssetArtifactKey(key)) {
                    try appendUniqueReplayRecordHint(alloc, &target_hints, .resolution);
                }
                // A committed resolution artifact re-drives sibling resolvers over
                // the same source (compositional event identity composes sibling
                // canonical keys). The runtime maps the key back to its source
                // extraction; byte-stable recomputes emit no further resolution
                // record, so the fan-back terminates.
                if (internal_keys.isResolutionArtifactKey(key)) {
                    try appendUniqueReplayRecordHint(alloc, &target_hints, .resolution);
                }
            }

            for (req.graph_writes) |write| {
                try appendUniqueReplayRecordKeyWithSet(alloc, &changed_doc_keys, &changed_doc_key_set, if (write.owner_document.len > 0) write.owner_document else if (write.owner.len > 0) write.owner else write.source);
                const artifact_key = try internal_keys.graphRelationshipArtifactKeyAlloc(alloc, if (write.owner_document.len > 0) write.owner_document else if (write.owner.len > 0) write.owner else write.source, write.index_name, write.edge_type, write.target, write.source, write.edge_id);
                defer alloc.free(artifact_key);
                try appendUniqueReplayRecordKeyWithSet(alloc, &thin_changed_artifact_keys, &thin_changed_artifact_key_set, artifact_key);
                try appendUniqueReplayRecordHint(alloc, &target_hints, .graph);
            }
            for (req.graph_deletes) |delete| {
                // An edge deletion changes the source document's graph projection; it
                // does not delete the source document. Classifying it as a document
                // deletion makes graph replay clear every source edge before applying
                // the targeted artifact delta.
                try appendUniqueReplayRecordKeyWithSet(alloc, &changed_doc_keys, &changed_doc_key_set, if (delete.owner_document.len > 0) delete.owner_document else if (delete.owner.len > 0) delete.owner else delete.source);
                const artifact_key = try internal_keys.graphRelationshipArtifactKeyAlloc(alloc, if (delete.owner_document.len > 0) delete.owner_document else if (delete.owner.len > 0) delete.owner else delete.source, delete.index_name, delete.edge_type, delete.target, delete.source, delete.edge_id);
                defer alloc.free(artifact_key);
                try appendUniqueReplayRecordKeyWithSet(alloc, &thin_changed_artifact_keys, &thin_changed_artifact_key_set, artifact_key);
                try appendUniqueReplayRecordHint(alloc, &target_hints, .graph);
            }

            var neighbor_hints = hints: {
                const active = index_manager orelse break :hints NeighborContextReplayHints{};
                if (!active.hasAssetNeighborContext()) break :hints NeighborContextReplayHints{};
                var direct_writes = std.ArrayListUnmanaged(types.GraphEdgeWrite).empty;
                defer direct_writes.deinit(alloc);
                try direct_writes.appendSlice(alloc, req.graph_writes);
                for (extracted, 0..) |write, i| {
                    if (derived_changed_flags[i]) try direct_writes.appendSlice(alloc, write.graph_writes);
                }
                var notification_artifacts = std.ArrayListUnmanaged([]const u8).empty;
                defer notification_artifacts.deinit(alloc);
                try notification_artifacts.appendSlice(alloc, thin_changed_artifact_keys.items);
                for (deleted_artifact_keys) |key| try notification_artifacts.append(alloc, key);
                break :hints try directGraphNeighborContextHintsAlloc(alloc, req, direct_writes.items, notification_artifacts.items, active, owning_table);
            };
            defer neighbor_hints.deinit(alloc);
            if (neighbor_hints.keys.len > 0) try appendUniqueReplayRecordHint(alloc, &target_hints, .enrichment);
            for (neighbor_hints.keys) |key| try appendUniqueReplayRecordKeyWithSet(alloc, &changed_doc_keys, &changed_doc_key_set, key);

            for (req.deletes) |key| {
                try appendUniqueReplayRecordKeyWithSet(alloc, &deleted_doc_keys, &deleted_doc_key_set, key);
                try appendUniqueReplayRecordHint(alloc, &target_hints, .full_text);
                try appendUniqueReplayRecordHint(alloc, &target_hints, .dense_vector);
                try appendUniqueReplayRecordHint(alloc, &target_hints, .sparse_vector);
                try appendUniqueReplayRecordHint(alloc, &target_hints, .algebraic);
                try appendUniqueReplayRecordHint(alloc, &target_hints, .graph);
            }
            for (deleted_artifact_keys) |key| {
                try appendUniqueReplayRecordKeyWithSet(alloc, &deleted_doc_keys, &deleted_doc_key_set, key);
                // Artifact removals advance the same source stream as upserts. Keep
                // them in the artifact-key lane so source-specific readiness cannot
                // report complete while a consumer still owes a delete.
                try appendUniqueReplayRecordKeyWithSet(alloc, &thin_changed_artifact_keys, &thin_changed_artifact_key_set, key);
                try appendUniqueReplayRecordHint(alloc, &target_hints, .full_text);
                try appendUniqueReplayRecordHint(alloc, &target_hints, .dense_vector);
                try appendUniqueReplayRecordHint(alloc, &target_hints, .sparse_vector);
                try appendUniqueReplayRecordHint(alloc, &target_hints, .algebraic);
                try appendUniqueReplayRecordHint(alloc, &target_hints, .graph);
                if (internal_keys.isAssetArtifactKey(key) or internal_keys.isChunkArtifactRecordKey(key) or internal_keys.isGraphEdgeArtifactKey(key)) {
                    try appendUniqueReplayRecordKeyWithSet(alloc, &thin_changed_artifact_keys, &thin_changed_artifact_key_set, key);
                    if (internal_keys.isChunkArtifactRecordKey(key)) try appendUniqueReplayRecordHint(alloc, &target_hints, .full_text);
                    try appendUniqueReplayRecordHint(alloc, &target_hints, .graph);
                    if (internal_keys.isAssetArtifactKey(key)) try appendUniqueReplayRecordHint(alloc, &target_hints, .resolution);
                }
            }

            var record: change_journal_mod.Record = .{ .sequence = sequence };
            defer change_journal_mod.deinitRecord(alloc, &record);
            record.changed_doc_keys = try changed_doc_keys.toOwnedSlice(alloc);
            record.deleted_doc_keys = try deleted_doc_keys.toOwnedSlice(alloc);
            record.overwritten_doc_keys = try overwritten_doc_keys.toOwnedSlice(alloc);
            record.changed_artifact_keys = try thin_changed_artifact_keys.toOwnedSlice(alloc);
            record.target_hints = try target_hints.toOwnedSlice(alloc);
            if (sync_targets_out) |out|
                out.* = try collectManagedSyncTargetsForRecord(alloc, index_manager.?, record);
            return try change_journal_mod.encodeRecord(alloc, record);
        }

        pub fn enforcePortableRuntimeGateOptional(pending: ?*const std.atomic.Value(bool)) !void {
            const configured = pending orelse return;
            if (configured.load(.acquire)) return error.PortableRuntimeActivationPending;
        }

        pub fn envBoolEnabled(raw: []const u8) bool {
            return !(std.mem.eql(u8, raw, "0") or
                std.ascii.eqlIgnoreCase(raw, "false") or
                std.ascii.eqlIgnoreCase(raw, "no"));
        }

        pub fn getenv(name: [*:0]const u8) ?[]const u8 {
            return platform.env.getenv(name);
        }

        pub fn isMetadataKey(key: []const u8) bool {
            return internal_keys.isGraphRetirementKey(key) or std.mem.startsWith(u8, key, "\x00\x00__metadata__:") or
                isSplitMetadataKey(key) or
                internal_keys.isTtlKey(key);
        }

        pub const isSplitMetadataKey = execution_resources.isSplitMetadataKey;

        pub fn journalRecordHasHint(record: change_journal_mod.Record, hint: change_journal_mod.TargetHint) bool {
            for (record.target_hints) |candidate| {
                if (candidate == hint) return true;
            }
            return false;
        }

        pub fn loadManagedAppliedSequenceContext(
            alloc: Allocator,
            index_manager: *const index_manager_mod.IndexManager,
            store: anytype,
            applied_sequence_checkpoint_path: ?[]const u8,
            index_ref: index_manager_mod.ManagedIndexRef,
        ) !u64 {
            // A WAL-authoritative dense index carries its exact durable replay
            // boundary in the posting checkpoint/WAL commit stream. The projection
            // metadata accessor overlays that boundary on the rarer lifecycle fields.
            if (index_ref.kind == .dense_vector and
                index_manager.densePostingWalAuthoritativeByName(index_ref.name))
            {
                const checkpoint = index_manager.denseProjectionCheckpointMetadata(index_ref.name) orelse
                    return error.InvalidDerivedApplyState;
                return checkpoint.applied_sequence;
            }
            return try apply_state.loadAppliedSequenceWithCheckpoint(
                alloc,
                index_manager.checkpointIo(),
                store,
                applied_sequence_checkpoint_path,
                index_ref.name,
            );
        }

        pub fn lockApply(self: anytype) void {
            self.core.lockApply();
        }

        pub fn lockAtomicWithBackoff(mutex: *std.atomic.Mutex) void {
            var attempts: usize = 0;
            while (!mutex.tryLock()) : (attempts += 1) {
                if (builtin.os.tag == .freestanding or builtin.single_threaded) {
                    std.atomic.spinLoopHint();
                    continue;
                }
                if (attempts < 64) {
                    std.atomic.spinLoopHint();
                    continue;
                }
                if (attempts < 128) {
                    @import("antfly_platform").time.yieldNow();
                    continue;
                }
                const backoff_step = @min(attempts - 128, 5);
                const sleep_ns = @min(@as(u64, 50_000) << @intCast(backoff_step), @as(u64, 1_000_000));
                sleepNs(sleep_ns);
            }
        }

        pub fn lockAtomicWithBackoffProfiled(mutex: *std.atomic.Mutex, stats: *MutexContentionStats) ProfiledLock {
            if (!asyncIndexProfileEnabled()) {
                lockAtomicWithBackoff(mutex);
                return .{
                    .mutex = mutex,
                    .stats = stats,
                    .hold_start_ns = 0,
                    .profiled = false,
                };
            }
            _ = stats.lock_calls.fetchAdd(1, .monotonic);
            if (mutex.tryLock()) {
                return .{
                    .mutex = mutex,
                    .stats = stats,
                    .hold_start_ns = monotonicTimeNs(),
                    .profiled = true,
                };
            }

            _ = stats.contended_calls.fetchAdd(1, .monotonic);
            const current_waiters = stats.current_waiters.fetchAdd(1, .monotonic) + 1;
            atomicMaxU64(&stats.max_waiters, current_waiters);
            defer _ = stats.current_waiters.fetchSub(1, .monotonic);

            const wait_start_ns = monotonicTimeNs();
            var attempts: usize = 0;
            var spin_loops: u64 = 0;
            var yield_loops: u64 = 0;
            var sleep_loops: u64 = 0;
            while (!mutex.tryLock()) : (attempts += 1) {
                if (builtin.os.tag == .freestanding or builtin.single_threaded) {
                    std.atomic.spinLoopHint();
                    spin_loops += 1;
                    continue;
                }
                if (attempts < 64) {
                    std.atomic.spinLoopHint();
                    spin_loops += 1;
                    continue;
                }
                if (attempts < 128) {
                    @import("antfly_platform").time.yieldNow();
                    yield_loops += 1;
                    continue;
                }
                const backoff_step = @min(attempts - 128, 5);
                const sleep_ns = @min(@as(u64, 50_000) << @intCast(backoff_step), @as(u64, 1_000_000));
                sleepNs(sleep_ns);
                sleep_loops += 1;
            }

            const wait_ns = elapsedSince(wait_start_ns);
            _ = stats.spin_loops.fetchAdd(spin_loops, .monotonic);
            _ = stats.yield_loops.fetchAdd(yield_loops, .monotonic);
            _ = stats.sleep_loops.fetchAdd(sleep_loops, .monotonic);
            _ = stats.wait_ns.fetchAdd(wait_ns, .monotonic);
            atomicMaxU64(&stats.max_wait_ns, wait_ns);
            return .{
                .mutex = mutex,
                .stats = stats,
                .hold_start_ns = monotonicTimeNs(),
                .profiled = true,
            };
        }

        pub fn mirrorReplicationEncodedBatchMutationCommitContext(ctx: *const BatchExecutionContext, payload: []const u8) !void {
            const projected = replicationCommitContext(ctx);
            return try replication_commit.mirrorReplicationEncodedBatchMutationCommitContext(&projected, payload);
        }

        pub fn mirrorReplicationReplayPayloadCommitContext(ctx: *const BatchExecutionContext, payload: []const u8) !void {
            const projected = replicationCommitContext(ctx);
            return try replication_commit.mirrorReplicationReplayPayloadCommitContext(&projected, payload);
        }

        pub const monotonicTimeNs = execution_resources.monotonicTimeNs;

        pub fn notifyExecutorForSyncLevel(
            executor: *derived_executor_mod.Executor,
            sync_level: types.SyncLevel,
            sequence: u64,
            sync_targets: ManagedSyncTargets,
        ) void {
            switch (sync_level) {
                .full_text => executor.notifyIndexes(sequence, sync_targets.full_text_indexes),
                .propose, .write, .enrichments, .full_index => executor.notifySequence(sequence),
            }
        }

        pub fn notifyExecutorForSyncLevelWithDenseBulkDeferral(
            async_context: ?*AsyncContext,
            executor: *derived_executor_mod.Executor,
            sync_level: types.SyncLevel,
            sequence: u64,
            sync_targets: ManagedSyncTargets,
        ) void {
            if (async_context) |ctx| {
                if (deferExternalBulkExecutorNotification(ctx, sync_level, sequence)) {
                    executor.notifyExceptKind(sequence, .dense_vector);
                    return;
                }
            }
            notifyExecutorForSyncLevel(executor, sync_level, sequence, sync_targets);
        }

        pub fn nsToMs(ns: u64) u64 {
            return ns / std.time.ns_per_ms;
        }

        pub fn openModeRequiresReadOnlyBackends(open_mode: OpenOptions.OpenMode) bool {
            return open_mode == .query_readonly or open_mode == .status_only;
        }

        pub fn orderedCoverageActive(store: *docstore_mod.DocStore) !bool {
            var probe = try store.beginProbeTxnWithBlockCacheAdmission(.transient);
            defer probe.abort();
            return try @import("artifact_publication.zig").authority(&probe) != null;
        }

        pub fn preflightReplicationMirrorSyncCommitContext(ctx: *const BatchExecutionContext, mirror: ?ReplicationAsyncEffectMirror) !void {
            try replication_commit.preflight(mirror, ctx.log_mutex);
        }

        pub fn prepareDirectChangeRecord(
            ctx: *const BatchExecutionContext,
            batch: derived_types.DerivedBatch,
            sequence: u64,
            req: types.BatchRequest,
            owning_table: ?[]const u8,
        ) !change_journal_mod.Record {
            var record = try change_journal_mod.recordFromDerivedBatch(ctx.alloc, batch, sequence);
            errdefer change_journal_mod.deinitRecord(ctx.alloc, &record);
            var hints = try directGraphNeighborContextHintsAlloc(ctx.alloc, req, batch.graph_writes, record.changed_artifact_keys, ctx.index_manager, owning_table);
            defer hints.deinit(ctx.alloc);
            try appendNeighborContextHintsToRecord(ctx, &record, hints.keys);
            return record;
        }

        pub fn profileDelta(after: u64, before: u64) u64 {
            return after -| before;
        }

        pub fn readEnvUsize(name: [:0]const u8, default_value: usize) usize {
            if (comptime builtin.os.tag == .freestanding) return default_value;
            const raw = getenv(name) orelse return default_value;
            if (raw.len == 0) return default_value;
            return std.fmt.parseUnsigned(usize, raw, 10) catch default_value;
        }

        pub fn recordProfileNs(profile: ?*BatchProfile, field: *u64, start_ns: u64) void {
            if (profile == null) return;
            field.* += monotonicTimeNs() - start_ns;
        }

        pub fn recoverDurableReplicationOutboxContext(
            ctx: *const BatchExecutionContext,
            mirror: ReplicationAsyncEffectMirror,
            outbox: DurableReplicationOutbox,
            kind: DurableReplicationOutboxKind,
        ) !void {
            try replication_commit.recoverDurableOutbox(.{
                .transition_mutex = replicationTransitionMutexFromContext(ctx),
                .log_mutex = ctx.log_mutex,
                .namespace = ctx.identity_namespace,
                .write_gate = ctx.replication_write_gate,
            }, mirror, outbox, kind);
        }

        pub fn releaseReplicationMutationShared(lease: *?MutationBarrier.SharedLease) void {
            if (lease.*) |*held| held.release();
            lease.* = null;
        }

        pub fn replayRecordHasTargetHint(
            record: change_journal_mod.Record,
            hint: change_journal_mod.TargetHint,
        ) bool {
            for (record.target_hints) |candidate| {
                if (candidate == hint) return true;
            }
            return false;
        }

        pub fn replicationCommitContext(ctx: *const BatchExecutionContext) replication_commit.CommitContext {
            return .{
                .alloc = ctx.alloc,
                .identity_namespace = ctx.identity_namespace,
                .transition_mutex = replicationTransitionMutexFromContext(ctx),
                .log_mutex = ctx.log_mutex,
                .replication_write_gate = ctx.replication_write_gate,
                .replication_async_effect_mirror = ctx.replication_async_effect_mirror,
                .replication_async_batch_mirror = ctx.replication_async_batch_mirror,
                .replication_async_metadata_mirror = ctx.replication_async_metadata_mirror,
                .append_pending = if (ctx.async_context) |async_ctx| &async_ctx.primary_replication_append_pending else null,
            };
        }

        pub fn replicationTransitionMutexFromContext(ctx: *const BatchExecutionContext) ?*std.atomic.Mutex {
            var transition_mutex: ?*std.atomic.Mutex = null;
            const mirrors = .{
                ctx.replication_async_effect_mirror,
                ctx.replication_async_batch_mirror,
                ctx.replication_async_metadata_mirror,
            };
            inline for (mirrors) |maybe_mirror| {
                if (maybe_mirror) |mirror| {
                    if (mirror.transition_mutex) |candidate| {
                        if (transition_mutex) |configured| {
                            std.debug.assert(configured == candidate);
                        } else {
                            transition_mutex = candidate;
                        }
                    }
                }
            }
            return transition_mutex;
        }

        pub fn reserveSplitShadowApplyTicket(self: anytype) ?u64 {
            const shadow = activeSplitShadow(self) orelse return null;
            const state = self.core.splitState() orelse return null;
            if (state.phase != .splitting) return null;
            const ticket = shadow.next_ticket;
            shadow.next_ticket = std.math.add(u64, ticket, 1) catch @panic("split shadow ticket overflow");
            return ticket;
        }

        pub fn saveAppliedSequencesBatchContext(
            ctx: *const BatchExecutionContext,
            updates: []const apply_state.AppliedSequenceUpdate,
        ) !void {
            if (updates.len == 0) return;
            const enriched_updates = try appliedSequenceUpdatesWithConfigHashes(ctx.alloc, ctx.index_manager, updates);
            defer ctx.alloc.free(enriched_updates);
            if (ctx.async_context) |async_ctx| {
                var seq_lock = lockAtomicWithBackoffProfiled(
                    &async_ctx.applied_sequence_mutex,
                    &async_ctx.stats.applied_sequence_mutex,
                );
                defer seq_lock.unlock();
                return try saveAppliedSequencesBatchLockedContext(ctx, enriched_updates, async_ctx);
            }
            return try saveAppliedSequencesBatchLockedContext(ctx, enriched_updates, null);
        }

        pub fn shouldAppendSplitDelta(self: anytype) bool {
            const state = self.core.splitState() orelse return false;
            return state.phase == .splitting;
        }

        pub fn shouldDeferBacklogPressureForExternalDenseBulk(ctx: *const BatchExecutionContext, sync_level: types.SyncLevel) bool {
            switch (sync_level) {
                .propose, .write, .enrichments => {},
                .full_text, .full_index => return false,
            }
            const async_context = ctx.async_context orelse return false;
            return async_context.dense_admission.external_sessions.load(.acquire) != 0;
        }

        pub fn sleepNs(duration_ns: u64) void {
            if (comptime builtin.os.tag == .freestanding) {
                return;
            }

            var req = std.posix.timespec{
                .sec = @intCast(duration_ns / std.time.ns_per_s),
                .nsec = @intCast(duration_ns % std.time.ns_per_s),
            };
            while (true) switch (std.posix.errno(std.posix.system.nanosleep(&req, &req))) {
                .SUCCESS => return,
                .INTR => continue,
                else => return,
            };
        }

        pub fn splitShadowRequiresMaterializedDerivedBatch(self: anytype) bool {
            if (activeSplitShadow(self) == null) return false;
            const state = self.core.splitState() orelse return false;
            return state.phase == .splitting;
        }

        pub fn syncLevelParticipatesInDerivedBacklogPressure(sync_level: types.SyncLevel) bool {
            return switch (sync_level) {
                .propose => false,
                .write, .enrichments, .full_text, .full_index => true,
            };
        }

        pub fn syncLevelRequiresDerivedVisibility(sync_level: types.SyncLevel) bool {
            return switch (sync_level) {
                .propose, .write, .enrichments => false,
                .full_text, .full_index => true,
            };
        }

        pub fn truncateReplayJournalIfSafeContext(ctx: *const BatchExecutionContext) !void {
            if (ctx.repair_replay_mutex) |mutex| mutex.lockUncancelable(ctx.index_manager.checkpointIo());
            defer if (ctx.repair_replay_mutex) |mutex| mutex.unlock(ctx.index_manager.checkpointIo());
            if (!ctx.index_manager.hasManagedIndexes()) return;

            const managed_indexes = try ctx.index_manager.managedIndexes(ctx.alloc);
            defer {
                for (managed_indexes) |index_ref| ctx.alloc.free(@constCast(index_ref.name));
                ctx.alloc.free(managed_indexes);
            }
            if (managed_indexes.len == 0) return;

            var min_applied: u64 = std.math.maxInt(u64);
            for (managed_indexes) |index_ref| {
                const applied = try loadManagedAppliedSequenceContext(
                    ctx.alloc,
                    ctx.index_manager,
                    ctx.store,
                    ctx.applied_sequence_checkpoint_path,
                    index_ref,
                );
                min_applied = @min(min_applied, applied);
            }
            if (ctx.index_manager.hasGeneratedEnrichmentTargets()) {
                const enrichment_applied = try enrichment_state.loadAppliedSequence(
                    ctx.alloc,
                    ctx.store,
                    enrichment_runtime_mod.scope_name,
                );
                min_applied = @min(min_applied, enrichment_applied);
            }
            if (ctx.resolution_runtime) |runtime| {
                const stats = runtime.stats();
                if (resolverReplayRetentionRequired(ctx.index_manager, stats)) {
                    min_applied = @min(min_applied, stats.applied_sequence);
                }
            }
            if (ctx.promotion_runtime) |runtime| {
                const stats = runtime.stats();
                if (resolverReplayRetentionRequired(ctx.index_manager, stats)) {
                    min_applied = @min(min_applied, stats.applied_sequence);
                }
            }
            min_applied = try clampReplayTruncationForRepairPins(ctx.alloc, ctx.index_repair_checkpoint, min_applied);
            if (min_applied == 0 or min_applied == std.math.maxInt(u64)) return;
            try truncateReplayLogs(ctx, min_applied);
        }

        pub fn truncateReplayLogs(ctx: *const BatchExecutionContext, up_to_sequence: u64) !void {
            try ctx.store.truncateReplayUpTo(ctx.alloc, up_to_sequence);
            ctx.executor.releaseBacklogThrough(up_to_sequence);
        }

        pub fn unlockProfiledApply(
            self: anytype,
            profile: ?*BatchProfile,
            held: *bool,
            acquired_ns: u64,
        ) void {
            std.debug.assert(held.*);
            if (profile) |active_profile| active_profile.apply_lock_held_ns += monotonicTimeNs() - acquired_ns;
            self.core.unlockApply();
            held.* = false;
        }

        pub fn waitForCachedBool(cache: *std.atomic.Value(u8)) bool {
            while (true) {
                switch (cache.load(.acquire)) {
                    1 => return false,
                    2 => return true,
                    else => std.atomic.spinLoopHint(),
                }
            }
        }
    };
}
