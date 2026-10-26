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

//! Apply operations of the local storage DB. Methods take `*DB` and
//! are re-exported from `DB` in db.zig, so callers keep writing `db.method()`.

const db_zig = @import("db.zig");
const DB = db_zig.DB;
const TestDb = db_zig.TestDb;
const execution_resources = @import("execution_resources.zig");
const monotonicTimeNs = db_zig.monotonicTimeNs;
const resolution_runtime_mod = @import("resolution_runtime.zig");
const localGroupSourceSettled = DB.localGroupSourceSettled;
const TestDirectory = @import("../../common/test_directory.zig").TestDirectory;
const apply_state = @import("derived/apply_state.zig");
const artifact_ids = @import("artifact/ids.zig");
const artifact_inventory = @import("artifact/inventory.zig");
const artifact_producer_provenance = @import("artifact/producer_provenance.zig");
const artifact_producer_readiness = @import("artifact/producer_readiness.zig");
const artifact_publication = @import("artifact/publication.zig");
const artifact_reconcile_intent = @import("artifact/reconcile_intent.zig");
const backfill_state_mod = @import("backfill_state.zig");
const background_runtime_mod = @import("../background_runtime.zig");
const builtin = @import("builtin");
const change_journal_mod = @import("derived/change_journal.zig");
const derived_types = @import("derived/derived_types.zig");
const doc_identity = @import("doc_identity.zig");
const doc_set = @import("doc_set.zig");
const docstore_mod = @import("../docstore.zig");
const enrichment_artifact_codec = @import("enrichment/artifact_codec.zig");
const enrichment_state = @import("enrichment/enrichment_state.zig");
const graph_edge_contender = @import("graph_edge_contender.zig");
const graph_edge_ttl_expiration = @import("graph_edge_ttl_expiration.zig");
const graph_mod = @import("../../graph/graph.zig");
const index_manager_mod = @import("catalog/index_manager.zig");
const index_repair_state = @import("derived/index_repair_state.zig");
const internal_keys = @import("../internal_keys.zig");
const mapper = @import("document/mapper.zig");
const merge_proof_adoption = @import("merge_proof_adoption.zig");
const merge_state_mod = @import("merge_state.zig");
const online_merge_io = @import("online_merge_io.zig");
const online_source = @import("online_source.zig");
const online_source_contract = @import("online_source_contract.zig");
const platform = @import("antfly_platform");
const policies_mod = @import("../../system_catalog/policies.zig");
const public_table_schema = @import("../../schema/mod.zig");
const range_state_mod = @import("range_state.zig");
const relational_initial_child_publication = @import("relational_initial_child_publication.zig");
const relational_integrity_activation = @import("relational_integrity_activation.zig");
const relational_integrity_catalog = @import("relational_integrity_catalog.zig");
const relational_integrity_generation_admission = @import("relational_integrity_generation_admission.zig");
const relational_integrity_generation_retirement = @import("relational_integrity_generation_retirement.zig");
const relational_integrity_handoff = @import("relational_integrity_handoff.zig");
const relational_integrity_topology = @import("relational_integrity_topology.zig");
const relational_integrity_topology_contract = @import("relational_integrity_topology_contract.zig");
const replay_source_mod = @import("derived/replay_source.zig");
const replay_stream_mod = @import("derived/replay_stream.zig");
const replicated_mutation = @import("replicated_mutation.zig");
const replication_effects_mod = @import("replication_effects.zig");
const resource_manager_mod = @import("../resource_manager.zig");
const restore_staging = @import("restore_staging.zig");
const row_policy_bundle_mod = @import("row_policy_bundle.zig");
const source_authority_mod = @import("../source_authority.zig");
const source_pin = @import("source_pin.zig");
const source_pin_state_mod = @import("../source_pin_state.zig");
const source_proof_batch = @import("source_proof_batch.zig");
const std = @import("std");
const test_index = @import("test_support.zig").test_index;
const storage_engine = @import("../engine.zig");
const table_catalog_mod = @import("table_catalog.zig");
const transactions_mod = @import("../transactions.zig");
const types = @import("types.zig");
const vopr_mod = @import("vopr");
const Allocator = db_zig.Allocator;
const AsyncContext = db_zig.AsyncContext;
const BatchExecutionOptions = db_zig.BatchExecutionOptions;
const BatchProfile = db_zig.BatchProfile;
const DerivedCoverageCounters = db_zig.DerivedCoverageCounters;
const DerivedReplayDebtStatus = db_zig.DerivedReplayDebtStatus;
const GraphMergeImportRecovery = db_zig.GraphMergeImportRecovery;
const GraphTtlCandidate = db_zig.GraphTtlCandidate;
const MutationBarrier = db_zig.MutationBarrier;
const OrderedApplyReceipt = db_zig.OrderedApplyReceipt;
const ReplayProgressHook = db_zig.ReplayProgressHook;
const RequestPreparationContext = db_zig.RequestPreparationContext;
const TtlCleanupContext = db_zig.TtlCleanupContext;
const addBatchProfile = db_zig.addBatchProfile;
const appendDerivedBatchRecord = db_zig.appendDerivedBatchRecord;
const appendReplicatedDerivedEffectContext = db_zig.appendReplicatedDerivedEffectContext;
const applyDerivedBatchToIndexAsync = db_zig.applyDerivedBatchToIndexAsync;
const benchMetricsEnabled = db_zig.benchMetricsEnabled;
const buildDerivedBatch = db_zig.buildDerivedBatch;
const default_test_wait_attempts = db_zig.default_test_wait_attempts;
const durableReplicationOutboxKeyAlloc = db_zig.durableReplicationOutboxKeyAlloc;
const encodeDurableReplicationOutboxAlloc = db_zig.encodeDurableReplicationOutboxAlloc;
const ensureSnapshotActive = db_zig.ensureSnapshotActive;
const expectedDocumentEmbeddingArtifactKeyAlloc = db_zig.expectedDocumentEmbeddingArtifactKeyAlloc;
const expireGraphTtlCandidateContext = db_zig.expireGraphTtlCandidateContext;
const finalizeCoveredDenseProjectionCheckpointsIfIdle = db_zig.finalizeCoveredDenseProjectionCheckpointsIfIdle;
const graph_merge_import_recovery_key = db_zig.graph_merge_import_recovery_key;
const isMergeArtifactKey = db_zig.isMergeArtifactKey;
const isRestoreArtifactKey = db_zig.isRestoreArtifactKey;
const journalRecordHasHint = db_zig.journalRecordHasHint;
const lockApply = db_zig.lockApply;
const logBatchProfile = db_zig.logBatchProfile;
const managedIndexReplayHint = db_zig.managedIndexReplayHint;
const materializeGraphArtifactValuePaged = db_zig.materializeGraphArtifactValuePaged;
const mirrorReplicationReplayPayloadBestEffortContext = db_zig.mirrorReplicationReplayPayloadBestEffortContext;
const missingAsNull = db_zig.missingAsNull;
const no_background_test_open = db_zig.no_background_test_open;
const openModeRequiresReadOnlyBackends = db_zig.openModeRequiresReadOnlyBackends;
const orderedApplyDisposition = db_zig.orderedApplyDisposition;
const orderedApplyReceiptWrite = db_zig.orderedApplyReceiptWrite;
const ordered_apply_receipt_value_len = db_zig.ordered_apply_receipt_value_len;
const persistAppliedSequenceAsync = db_zig.persistAppliedSequenceAsync;
const platform_clock = db_zig.platform_clock;
const platform_time = db_zig.platform_time;
const profileBenchTestsEnabled = db_zig.profileBenchTestsEnabled;
const public_schema_json_key = db_zig.public_schema_json_key;
const putDenseEmbeddingArtifactForTest = db_zig.putDenseEmbeddingArtifactForTest;
const putSparseEmbeddingArtifactForTest = db_zig.putSparseEmbeddingArtifactForTest;
const readOrderedApplyReceipt = db_zig.readOrderedApplyReceipt;
const rebaseRangeCoverageMetadata = db_zig.rebaseRangeCoverageMetadata;
const replayPendingDerivedBatches = db_zig.replayPendingDerivedBatches;
const replicationAppliedSequenceWrite = db_zig.replicationAppliedSequenceWrite;
const replication_applied_lsn_value_len = db_zig.replication_applied_lsn_value_len;
const sleepPollInterval = db_zig.sleepPollInterval;
const stageApplyMarkers = db_zig.stageApplyMarkers;
const truncateReplayJournalIfSafeContext = db_zig.truncateReplayJournalIfSafeContext;
const truncateReplaySequenceAsync = db_zig.truncateReplaySequenceAsync;
const waitForAppliedSequenceAdvance = db_zig.waitForAppliedSequenceAdvance;
const ReplayDrainOptions = DB.ReplayDrainOptions;
const decodeManagedIndexAdmissionMarker = DB.decodeManagedIndexAdmissionMarker;
const durableGenerationBuildActive = DB.durableGenerationBuildActive;
const hasCoordinatedConstraints = DB.hasCoordinatedConstraints;
const projectionCheckpointStatusName = DB.projectionCheckpointStatusName;
const run_until_idle_max_replay_rounds = DB.run_until_idle_max_replay_rounds;

pub fn mirrorReplicationReplayPayloadBestEffort(self: *DB, payload: []const u8) void {
    var ctx = self.batchContext();
    mirrorReplicationReplayPayloadBestEffortContext(&ctx, payload);
}

pub fn resumeAsyncReplayFromJournalIfNeeded(self: *DB) void {
    const target_sequence = self.core.nextDerivedSequence();
    if (target_sequence == 0) return;
    self.executor.notifySequence(target_sequence);
}

pub fn resolverReplayNeedsDrain(self: *DB) bool {
    if (self.hasConfiguredResolvers()) return true;
    if (self.resolution_runtime) |runtime| {
        const replay_stats = runtime.stats();
        if (replay_stats.catch_up_required or replay_stats.blocked) return true;
    }
    if (self.promotion_runtime) |runtime| {
        const replay_stats = runtime.stats();
        if (replay_stats.catch_up_required or replay_stats.blocked) return true;
    }
    return false;
}

pub fn startResolverReplayRuntimesIfConfigured(self: *DB) !void {
    if (!self.hasConfiguredResolvers()) return;
    try self.startResolverReplayRuntimes();
}

/// Activate only after the serving owner installs candidate/sink hooks.
pub fn activateResolverReplayRuntimes(self: *DB) !void {
    if (comptime storage_engine.enabled) return storage_engine.call(@src().fn_name, .{self});
    self.resolver_workers_enabled = true;
    if (self.local_execution.optional_runtime_workers_enabled) try self.startResolverReplayRuntimesIfConfigured();
}

pub fn startResolverReplayRuntimes(self: *DB) !void {
    if (!self.resolver_workers_enabled or !self.local_execution.optional_runtime_workers_enabled) return;
    var resolution_started = false;
    errdefer if (resolution_started) if (self.resolution_runtime) |runtime| runtime.stop();
    if (self.resolution_runtime) |runtime| {
        try runtime.start();
        resolution_started = true;
    }
    if (builtin.is_test and db_zig.test_fail_portable_promotion_runtime_start) {
        db_zig.test_fail_portable_promotion_runtime_start = false;
        return error.InjectedPortablePromotionRuntimeStartFailure;
    }
    if (self.promotion_runtime) |runtime| try runtime.start();
}

pub fn stopResolverReplayRuntimes(self: *DB) void {
    if (self.resolution_runtime) |runtime| runtime.stop();
    if (self.promotion_runtime) |runtime| runtime.stop();
}

pub fn stopResolverReplayRuntimesIfUnconfigured(self: *DB) void {
    if (self.hasConfiguredResolvers()) return;
    if (self.resolution_runtime) |runtime| runtime.stop();
    if (self.promotion_runtime) |runtime| runtime.stop();
}

pub fn batch(self: *DB, req: types.BatchRequest) anyerror!void {
    if (benchMetricsEnabled()) {
        var profile = BatchProfile{};
        try self.batchInternal(req, &profile, .{});
        logBatchProfile(req, profile);
    } else {
        try self.batchInternal(req, null, .{});
    }
}

pub fn batchWithVisibilityCancellation(self: *DB, req: types.BatchRequest, cancellation: types.CancellationToken) anyerror!void {
    if (benchMetricsEnabled()) {
        var profile = BatchProfile{};
        try self.batchInternal(req, &profile, .{ .visibility_cancellation = cancellation });
        logBatchProfile(req, profile);
    } else {
        try self.batchInternal(req, null, .{ .visibility_cancellation = cancellation });
    }
}

pub fn batchProfiled(self: *DB, req: types.BatchRequest, profile: *BatchProfile) anyerror!void {
    try self.batchInternal(req, profile, .{});
}

pub fn batchWithoutRangeValidation(self: *DB, req: types.BatchRequest) anyerror!void {
    try self.batchInternal(req, null, .{ .validate_range_ownership = false });
}

pub fn batchReplicatedApply(self: *DB, req: types.BatchRequest) anyerror!void {
    if (comptime storage_engine.enabled) return storage_engine.call(@src().fn_name, .{ self, req });
    try batchReplicatedApplyWithMarker(self, req, null);
}

/// Applies one exact data-Raft log entry and persists its identity in the
/// same primary-store batch. Replaying the entry after a crash becomes an
/// allocation-light no-op before transforms or derived work execute.
/// Execute a committed ordered mutation. The receipt and primary effects
/// share a store transaction; server replay policy is supplied by the caller.
pub fn applyOrderedCommittedMutation(self: *DB, request: types.BatchRequest, identity: OrderedApplyReceipt, bypass_write_gate: bool) anyerror!void {
    if (comptime storage_engine.enabled) return storage_engine.call(@src().fn_name, .{ self, request, identity, bypass_write_gate });
    if (request.graph_endpoint_cleanup and !request.graph_endpoint_cleanup_planned) return error.InvalidBatchRequest;
    try self.batchInternal(request, null, .{
        .validate_range_ownership = false,
        .wait_for_sync_level = false,
        .bypass_replication_write_gate = bypass_write_gate,
        .ordered_apply_receipt = identity,
    });
}

/// A hidden initial child in native standalone has no data-Raft log. The
/// metadata decision names one immutable plan and owner, while the
/// hidden Record commits the operation receipt with the schema/phase.
/// Keep this distinct from the generic Raft path: native source roots
/// must never acquire a fabricated Raft applied-entry watermark.
pub fn batchNativeInitialChildApply(self: *DB, req: types.BatchRequest, receipt: OrderedApplyReceipt) !void {
    if (comptime storage_engine.enabled) return storage_engine.call(@src().fn_name, .{ self, req, receipt });
    const command = req.relational_topology orelse return error.InvalidInitialChildPublication;
    if (command.action != .provision_initial_child and command.action != .release_initial_child and command.action != .cancel_initial_child)
        return error.InvalidInitialChildPublication;
    if (receipt.term != 1 or receipt.index != switch (command.action) {
        .provision_initial_child => @as(u64, 1),
        .release_initial_child => @as(u64, 2),
        .cancel_initial_child => @as(u64, 3),
        else => unreachable,
    }) return error.InvalidInitialChildPublication;
    if (req.writes.len != 0 or req.deletes.len != 0 or req.transforms.len != 0 or
        req.graph_writes.len != 0 or req.graph_deletes.len != 0 or req.predicates.len != 0 or
        req.integrity.len != 0 or req.integrity_commands.len != 0 or req.range_guards.len != 0 or
        req.online_source != null or req.restore_staging != null or req.restore_staging_scope != null or
        req.restore_staging_plan_id != null or req.relational_generation_gc != null or
        req.relational_activation != null or req.relational_retirement != null or
        req.relational_index_maintenance != null or req.row_policy_publication != null or
        req.row_policy_install_bundle.len != 0 or req.row_policy_principal_proof.len != 0 or
        req.row_policy_database.len != 0 or req.row_policy_admitted_at_seconds != 0 or
        req.split_checkpoint != null or req.split_replication != null or req.split_transition != null or
        req.merge_source_transition != null or req.merge_checkpoint != null or req.merge_replication != null or
        req.merge_page != null or req.merge_artifacts.len != 0 or req.transaction != null or
        req.relational_repair or req.activate_range_tracking or req.reject_graph_transform_projections or
        req.schema_version != null or req.relational_schema_version != null or
        req.relational_integrity_generation_set != null or req.timestamp_ns != 0)
        return error.InvalidInitialChildPublication;
    const bootstrap = self.local_execution.initial_child_bootstrap orelse return error.InvalidInitialChildPublication;
    if (self.local_execution.replication_async_batch_mirror != null or self.local_execution.replication_async_metadata_mirror != null or
        self.local_execution.replication_async_effect_mirror != null or self.local_execution.replication_write_gate != null or
        openModeRequiresReadOnlyBackends(self.open_mode))
        return error.InvalidInitialChildPublication;
    if (!bootstrap.namespace.eql(command.fence.namespace) or
        !std.mem.eql(u8, &bootstrap.catalog_digest, &command.fence.catalog_digest))
        return error.InitialChildPublicationChanged;
    switch (command.action) {
        .provision_initial_child => {
            const provision = command.initial_child_provision orelse return error.InvalidInitialChildPublication;
            if (!std.mem.eql(u8, &bootstrap.plan_id, &provision.plan_id) or
                !std.mem.eql(u8, &bootstrap.plan_digest, &provision.plan_digest) or
                !std.mem.eql(u8, &bootstrap.schema_digest, &provision.schema_digest) or
                !std.mem.eql(u8, &bootstrap.public_schema_json_digest, &provision.public_schema_json_digest) or
                !std.mem.eql(u8, &bootstrap.catalog_digest, &provision.catalog_digest))
                return error.InitialChildPublicationChanged;
        },
        .release_initial_child, .cancel_initial_child => {
            const control = command.initial_child_control orelse return error.InvalidInitialChildPublication;
            if (!std.mem.eql(u8, &bootstrap.plan_id, &control.plan_id) or
                !std.mem.eql(u8, &bootstrap.plan_digest, &control.plan_digest) or
                bootstrap.schema_version != control.schema_version or
                !std.mem.eql(u8, &bootstrap.schema_digest, &control.schema_digest) or
                !std.mem.eql(u8, &bootstrap.public_schema_json_digest, &control.public_schema_json_digest) or
                !std.mem.eql(u8, &bootstrap.catalog_digest, &control.catalog_digest))
                return error.InitialChildPublicationChanged;
        },
        else => unreachable,
    }
    var authority = try self.core.store.beginReadTxn();
    defer authority.abort();
    _ = try source_authority_mod.require(&authority, .native, online_source_contract.namespaceBytes(self.core.identity_namespace));
    return self.batchInternal(req, null, .{
        .validate_range_ownership = false,
        .wait_for_sync_level = false,
        .bypass_replication_write_gate = true,
        .native_initial_child_entry = receipt,
    });
}

/// A native owner has no Raft log. Metadata still authorizes the exact
/// publication fence, and its operation receipt commits with the effect,
/// without advancing the unrelated Raft apply watermark.
pub fn batchNativeFkGenerationApply(self: *DB, req: types.BatchRequest) !void {
    if (comptime storage_engine.enabled) return storage_engine.call(@src().fn_name, .{ self, req });
    const command = req.relational_topology orelse return error.InvalidBatchRequest;
    if (command.fence.role != .child_generation_source and command.fence.role != .child_generation_parent and
        command.fence.role != .child_generation_dual) return error.InvalidBatchRequest;
    const ordinal: u64 = switch (command.action) {
        .begin => 1,
        .stage_child_generation => 2,
        .activate_child_generation => 3,
        .acknowledge_child_generation => 4,
        .install_child_schema => 5,
        .cancel => 6,
        .cancel_child_generation_source => 7,
        else => return error.InvalidBatchRequest,
    };
    // This boundary accepts a control only, never a mixed user mutation.
    const defaults: types.BatchRequest = .{};
    inline for (@typeInfo(types.BatchRequest).@"struct".field_names, @typeInfo(types.BatchRequest).@"struct".field_types) |reflected_name, field_type| {
        if (comptime !std.mem.eql(u8, reflected_name, "relational_topology") and !std.mem.eql(u8, reflected_name, "sync_level")) {
            const value = @field(req, reflected_name);
            if (comptime @typeInfo(field_type) == .pointer and @typeInfo(field_type).pointer.size == .slice) {
                if (value.len != 0) return error.InvalidBatchRequest;
            } else if (!std.meta.eql(value, @field(defaults, reflected_name))) return error.InvalidBatchRequest;
        }
    }
    if (command.transfer != null or command.parent_retirement != null or command.parent_activation != null or
        command.initial_child_provision != null or command.initial_child_control != null or command.graph_retirement != null or
        command.generation_handoff != null or command.generation_handoff_seal != null or command.generation_handoff_install != null)
        return error.InvalidBatchRequest;
    if (self.local_execution.replication_async_batch_mirror != null or self.local_execution.replication_async_metadata_mirror != null or
        self.local_execution.replication_async_effect_mirror != null or self.local_execution.replication_write_gate != null or openModeRequiresReadOnlyBackends(self.open_mode))
        return error.UnsupportedOperation;
    _ = try command.fence.encode();
    var authority = try self.core.store.beginReadTxn();
    defer authority.abort();
    _ = try source_authority_mod.require(&authority, .native, online_source_contract.namespaceBytes(self.core.identity_namespace));
    return self.batchInternal(req, null, .{
        .validate_range_ownership = false,
        .wait_for_sync_level = false,
        .native_fk_generation_entry = .{ .term = command.fence.admission_epoch, .index = ordinal },
    });
}

/// The durable Raft projection validates the split lifecycle before the
/// document delegate runs. Mirror its finalized range on every replica,
/// without sending an empty source command through document/index work.
/// The range and entry receipt share a batch so restart replay cannot
/// narrow a range again after a later merge has expanded it.
pub fn applyRangeFinalization(
    self: *DB,
    transition: types.SplitTransitionMutation,
    identity: ?OrderedApplyReceipt,
    replication_lsn: ?u64,
    replication_payload: ?[]const u8,
) !void {
    var snapshot_mutation = self.core.snapshot_admission.acquireMutation();
    defer snapshot_mutation.release();
    lockApply(self);
    defer self.core.unlockApply();
    var transaction_manager = try self.core.initTxnManager();
    defer transaction_manager.deinit();
    if (try transaction_manager.hasSchemaLeases()) return error.SchemaInUse;
    if (identity) |entry| switch (try orderedApplyDisposition(try readOrderedApplyReceipt(self.alloc, self.core.store), entry)) {
        .already_applied => return,
        .apply => {},
    };
    const current = self.core.byteRange();
    if (transition.transition_id == 0 or transition.attempt_epoch == 0 or
        transition.destination_group_id == 0 or transition.split_key.len == 0 or
        !std.mem.lessThan(u8, current.start, transition.split_key) or
        (current.end.len != 0 and std.mem.lessThan(u8, current.end, transition.split_key)))
        return error.InvalidSplitRange;
    const start = try self.alloc.dupe(u8, current.start);
    errdefer self.alloc.free(start);
    const end = try self.alloc.dupe(u8, transition.split_key);
    errdefer self.alloc.free(end);
    const range: types.ByteRange = .{ .start = start, .end = end };
    const range_value = try range_state_mod.encodeRangeAlloc(self.alloc, range);
    defer self.alloc.free(range_value);
    // Publish graph source ownership before acknowledging the transition.
    // Physical retirement is a durable maintenance task, not Raft apply
    // work. Reverse reads use the same source fence; old metric jobs are
    // invalidated before they can publish against the narrowed range.
    self.core.index_manager.fenceGraphSplitRange(transition.split_key, current.end) catch |err| switch (err) {
        // A previous split's cleanup is bounded background work. Keep
        // this committed entry pending, not a fatal Raft apply failure.
        error.GraphMaintenanceInProgress => return error.RaftApplyWriterUnavailable,
        else => return err,
    };
    if (builtin.is_test and graph_mod.test_abort_ownership_before_range_commit) return error.TestInjectedBackfillFailure;
    var marker_buf: [ordered_apply_receipt_value_len]u8 = undefined;
    var replication_marker_buf: [replication_applied_lsn_value_len]u8 = undefined;
    var outbox_value: ?[]u8 = null;
    defer if (outbox_value) |bytes| self.alloc.free(bytes);
    var outbox_key: ?[]u8 = null;
    defer if (outbox_key) |bytes| self.alloc.free(bytes);
    var integrity_coverage: ?[]u8 = null;
    defer if (integrity_coverage) |bytes| self.alloc.free(bytes);
    var receipt: [136]u8 = undefined;
    var metadata_writes: std.ArrayList(docstore_mod.KVPair) = .empty;
    defer metadata_writes.deinit(self.alloc);
    try metadata_writes.append(self.alloc, .{ .key = range_state_mod.range_key, .value = range_value });
    if (identity) |entry| try metadata_writes.append(self.alloc, orderedApplyReceiptWrite(entry, &marker_buf));
    if (replication_lsn) |lsn| try metadata_writes.append(self.alloc, replicationAppliedSequenceWrite(lsn, &replication_marker_buf));
    if (replication_payload) |payload| {
        const mirror = self.local_execution.replication_async_batch_mirror orelse return error.ReplicationPublisherUnavailable;
        const from_lsn = mirror.publisher.nextLsn();
        outbox_value = try encodeDurableReplicationOutboxAlloc(self.alloc, from_lsn, payload);
        outbox_key = try durableReplicationOutboxKeyAlloc(self.alloc, .restore_batch, from_lsn, self.core.root_generation, payload);
        try metadata_writes.append(self.alloc, .{ .key = outbox_key.?, .value = outbox_value.? });
        self.local_execution.durable_replication_outbox_maybe.store(true, .release);
    }
    {
        var view = self.core.acquireSchemaView();
        defer if (view) |*pinned| pinned.release();
        if (hasCoordinatedConstraints(view)) {
            var read = try self.core.store.beginProbeTxn();
            defer read.abort();
            const topology = relational_integrity_topology;
            const fence = (try topology.current(&read)) orelse return error.CoordinatedConstraintTopologyUnsupported;
            if (fence.role != .split_source or fence.transition_id != transition.transition_id or fence.attempt != transition.attempt_epoch or fence.peer_group_id != transition.destination_group_id) return error.IntegrityTopologyChanged;
            try topology.requireDrained(&read, &transaction_manager, fence);
            const raw_catalog = try read.get(relational_integrity_catalog.key);
            var compiled = try relational_integrity_catalog.decode(self.alloc, raw_catalog);
            defer compiled.deinit();
            try relational_integrity_activation.requireReady(&read, compiled);
            integrity_coverage = try relational_integrity_handoff.coverageForRange(self.alloc, raw_catalog, self.core.identity_namespace, range);
            receipt = try fence.encode();
            try metadata_writes.appendSlice(self.alloc, &.{ .{ .key = relational_integrity_activation.key, .value = integrity_coverage.? }, .{ .key = topology.receipt_key, .value = &receipt } });
        } else {
            var read = try self.core.store.beginProbeTxn();
            defer read.abort();
            try relational_integrity_topology.requireUnfenced(&read);
        }
    }
    try rebaseRangeCoverageMetadata(self.alloc, self.core.store, self.core.index_manager, range, metadata_writes.items);
    self.core.adoptRangeInMemoryOwned(start, end);
}

pub fn orderedApplyReceipt(self: *DB) !?OrderedApplyReceipt {
    if (comptime storage_engine.enabled) return storage_engine.call(@src().fn_name, .{self});
    lockApply(self);
    defer self.core.unlockApply();
    return try readOrderedApplyReceipt(self.alloc, self.core.store);
}

pub fn orderedMutationAlreadyApplied(self: *DB, identity: OrderedApplyReceipt) !bool {
    if (comptime storage_engine.enabled) return storage_engine.call(@src().fn_name, .{ self, identity });
    lockApply(self);
    defer self.core.unlockApply();
    return switch (try orderedApplyDisposition(
        try readOrderedApplyReceipt(self.alloc, self.core.store),
        identity,
    )) {
        .apply => false,
        .already_applied => true,
    };
}

pub fn recordOrderedApplyReceipt(self: *DB, identity: OrderedApplyReceipt) !void {
    lockApply(self);
    defer self.core.unlockApply();
    switch (try orderedApplyDisposition(
        try readOrderedApplyReceipt(self.alloc, self.core.store),
        identity,
    )) {
        .already_applied => return,
        .apply => {},
    }
    var value_buf: [ordered_apply_receipt_value_len]u8 = undefined;
    const marker = orderedApplyReceiptWrite(identity, &value_buf);
    try self.core.store.putBatch(&.{marker}, &.{});
}

pub fn applyReplicatedRowPolicyPublication(self: *DB, bundle_bytes: []const u8, request: policies_mod.InstallRequest, identity: OrderedApplyReceipt) !?row_policy_bundle_mod.Receipt {
    if (comptime storage_engine.enabled) return storage_engine.call(@src().fn_name, .{ self, bundle_bytes, request, identity });
    return applyRowPolicyPublicationInternal(self, bundle_bytes, request, identity, null);
}

pub fn applyRowPolicyPublicationInternal(self: *DB, bundle_bytes: []const u8, request: policies_mod.InstallRequest, identity: OrderedApplyReceipt, replication_lsn: ?u64) !?row_policy_bundle_mod.Receipt {
    if (openModeRequiresReadOnlyBackends(self.open_mode)) return error.ReadOnly;
    if (identity.term == 0 or identity.index == 0 or (replication_lsn != null and replication_lsn.? == 0)) return error.InvalidRowPolicyPublication;
    if (replication_lsn == null and self.local_execution.replication_async_metadata_mirror == null and
        (self.local_execution.replication_async_batch_mirror != null or self.local_execution.replication_write_gate != null)) return error.ReplicationPublisherUnavailable;
    // The policy cut must be serialized with all in-flight primary writes,
    // including writers that passed their fast outbox preflight already.
    // Production hot-standby mirrors share this capture barrier with every mutation.
    if (replication_lsn == null) if (self.local_execution.replication_async_metadata_mirror) |mirror|
        if (mirror.mutation_barrier == null) return error.ReplicationPublisherUnavailable;
    var replication_mutation: ?MutationBarrier.ExclusiveLease = null;
    defer if (replication_mutation) |*lease| lease.release();
    if (replication_lsn == null) {
        try self.enforceReplicationWriteGate();
        try self.ensureDurableReplicationStartupBarrier();
        try self.preflightReplicationMetadataSyncCommit();
        if (self.local_execution.replication_async_metadata_mirror) |mirror|
            replication_mutation = mirror.mutation_barrier.?.acquireExclusive();
    }
    var schedule_replication_recovery_on_exit = false;
    defer if (schedule_replication_recovery_on_exit) self.scheduleDurableReplicationOutboxRecovery();
    const replication_payload = if (replication_lsn == null and self.local_execution.replication_async_metadata_mirror != null)
        try replication_effects_mod.encodeRowPolicyMetadataMutationAlloc(self.alloc, bundle_bytes, request, identity)
    else
        null;
    defer if (replication_payload) |payload| self.alloc.free(payload);
    const replication_outbox = if (replication_payload) |payload| blk: {
        // This is a WAL search lower bound, not an LSN reservation.
        // Recovery matches the exact payload and owner identity under the
        // transition/log locks even if another publisher appended first.
        const from_lsn = self.local_execution.replication_async_metadata_mirror.?.publisher.nextLsn();
        const encoded = try encodeDurableReplicationOutboxAlloc(self.alloc, from_lsn, payload);
        errdefer self.alloc.free(encoded);
        const key = try durableReplicationOutboxKeyAlloc(self.alloc, .row_policy, from_lsn, self.core.root_generation, payload);
        break :blk .{ .key = key, .value = encoded };
    } else null;
    defer if (replication_outbox) |outbox| {
        self.alloc.free(outbox.key);
        self.alloc.free(outbox.value);
    };
    const schema = self.core.schema orelse return error.RowPolicyCatalogChanged;
    var installed = try row_policy_bundle_mod.Installed.init(self.alloc, bundle_bytes, self.core.identity_namespace.table_id, schema);
    defer installed.deinit();
    if (installed.parsed.value.table_id != request.table_id or
        installed.parsed.value.policy_generation != request.expected_generation or
        installed.parsed.value.catalog_epoch != request.expected_catalog_epoch or
        installed.parsed.value.phase != request.expected_phase)
        return error.RowPolicyCatalogChanged;
    var bundle_digest: [32]u8 = undefined;
    std.crypto.hash.sha2.Sha256.hash(bundle_bytes, &bundle_digest, .{});
    const expected_receipt: row_policy_bundle_mod.Receipt = .{
        .table_id = request.table_id,
        .generation = request.expected_generation,
        .catalog_epoch = request.expected_catalog_epoch,
        .phase = request.expected_phase,
        .applied_term = identity.term,
        .applied_index = identity.index,
        .bundle_digest = bundle_digest,
        .descriptor_digest = request.expected_descriptor_digest,
    };
    if (try self.orderedMutationAlreadyApplied(identity)) {
        lockApply(self);
        const already = self.readRowPolicyApplyResultLocked(expected_receipt) catch |err| {
            self.core.unlockApply();
            return err;
        };
        self.core.unlockApply();
        if (replication_lsn) |lsn| try self.recordReplicationApplied(lsn);
        return already;
    }
    const phase = installed.parsed.value.phase;
    const pending_phase = phase == .pending_install or phase == .pending_disable;
    try self.lockApplyForPortableRuntime();
    var apply_held = true;
    defer if (apply_held) self.core.unlockApply();
    if (switch (try orderedApplyDisposition(try readOrderedApplyReceipt(self.alloc, self.core.store), identity)) {
        .already_applied => true,
        .apply => false,
    }) {
        const already = try self.readRowPolicyApplyResultLocked(expected_receipt);
        self.core.unlockApply();
        apply_held = false;
        if (replication_lsn) |lsn| try self.recordReplicationApplied(lsn);
        return already;
    }
    {
        var read = try self.core.store.beginReadTxn();
        defer read.abort();
        try self.requireOnlineArtifactCatalogMutableLocked(&read);
        try relational_integrity_topology.requireUnfenced(&read);
    }
    const stage_with_serving = pending_phase and self.local_execution.row_policy_gate.currentPhase() == .active;
    if (!stage_with_serving and pending_phase) {
        if (self.local_execution.row_policy_gate.currentPhase() != .preparing)
            try self.local_execution.row_policy_gate.beginPreparing(self.local_execution.row_policy_gate.currentPhase());
    } else if (!pending_phase and self.local_execution.row_policy_gate.currentPhase() == .active and
        (phase == .serving_install or phase == .serving_disable))
    {
        try self.local_execution.row_policy_gate.beginPreparing(.active);
    } else if (!stage_with_serving and self.local_execution.row_policy_gate.currentPhase() != .preparing) {
        return error.RowPolicyCatalogChanged;
    }
    if (try self.pendingRowPolicyReceipt()) |prior| {
        // A lagging follower need not have received a coordinator receipt
        // probe for the earlier phase. A later metadata-authorized Raft
        // phase may supersede that fail-closed intent without admitting
        // readers or claiming the earlier local ACK. The latest phase
        // still waits for all old leases before it can serve.
        const next_phase = installed.parsed.value.phase;
        const follows = if (request.expected_generation > prior.generation)
            next_phase == .pending_install or next_phase == .pending_disable
        else if (request.expected_generation == prior.generation)
            switch (prior.phase) {
                .pending_install => next_phase == .serving_install,
                .serving_install => next_phase == .active or next_phase == .pending_disable,
                .pending_disable => next_phase == .serving_disable,
                .serving_disable => next_phase == .disabled,
                .active, .disabled => false,
            }
        else
            false;
        if (!follows or prior.table_id != request.table_id or
            !std.mem.eql(u8, &prior.descriptor_digest, &request.expected_descriptor_digest) or
            identity.index <= prior.applied_index) return error.RowPolicyCatalogChanged;
    }
    const previous = self.core.table_catalog;
    const publication = installed.parsed.value;
    const current_range = self.core.byteRange();
    const actual_descriptor_digest = try (policies_mod.OwnerDescriptor{
        .table_id = self.core.identity_namespace.table_id,
        .group_id = request.owner_group_id,
        .shard_id = self.core.identity_namespace.shard_id,
        .range_id = self.core.identity_namespace.range_id,
        .schema_version = publication.schema_version,
        .schema_digest = publication.schema_digest,
        .range_start = current_range.start,
        .range_end = current_range.end,
    }).digest();
    if (!std.mem.eql(u8, &actual_descriptor_digest, &request.expected_descriptor_digest))
        return error.RowPolicyCatalogChanged;
    if (previous.storage_mode != .relational or previous.active_schema_version != publication.schema_version or
        publication.policy_generation < previous.row_policy_generation or
        (publication.policy_generation == previous.row_policy_generation and !stage_with_serving and
            previous.row_policy_phase != .preparing)) return error.RowPolicyCatalogChanged;
    var receipt_bytes: [ordered_apply_receipt_value_len]u8 = undefined;
    const receipt = orderedApplyReceiptWrite(identity, &receipt_bytes);
    if (stage_with_serving) {
        if (self.local_execution.row_policy_gate.currentPhase() != .active or previous.row_policy_phase != .active or
            publication.policy_generation <= previous.row_policy_generation or
            (try self.pendingRowPolicyReceipt()) != null) return error.RowPolicyCatalogChanged;
        const existing_candidate = try missingAsNull(self.core.store.get(self.alloc, row_policy_bundle_mod.candidate_key));
        defer if (existing_candidate) |value| self.alloc.free(value);
        if (existing_candidate != null) return error.RowPolicyCatalogChanged;
        const encoded_receipt = expected_receipt.encode();
        var receipt_key_buf: [128]u8 = undefined;
        const receipt_key = try expected_receipt.key(&receipt_key_buf);
        var stage_txn = try self.core.store.beginWriteTxn();
        errdefer stage_txn.abort();
        try stage_txn.put(row_policy_bundle_mod.candidate_key, bundle_bytes);
        try stage_txn.put(receipt_key, &encoded_receipt);
        try stage_txn.put(receipt.key, receipt.value);
        var replication_lsn_buf: [replication_applied_lsn_value_len]u8 = undefined;
        if (replication_lsn) |lsn| {
            const marker = replicationAppliedSequenceWrite(lsn, &replication_lsn_buf);
            try stage_txn.put(marker.key, marker.value);
        }
        if (replication_outbox) |outbox| try stage_txn.put(outbox.key, outbox.value);
        if (replication_outbox != null) {
            // Conservative on abort: the next admission performs an
            // apply-fenced empty scan and clears both fast-negative flags.
            self.local_execution.durable_replication_outbox_maybe.store(true, .release);
            self.local_execution.row_policy_replication_outbox_pending.store(true, .release);
        }
        try stage_txn.commit();
        if (replication_outbox != null) {
            schedule_replication_recovery_on_exit = true;
        }
        try self.finishRowPolicyReplicationCommitLocked(replication_payload, if (replication_outbox) |outbox| outbox.key else null, &apply_held, &replication_mutation);
        return expected_receipt;
    }
    if (phase == .serving_install or phase == .serving_disable) {
        const candidate = try self.core.store.get(self.alloc, row_policy_bundle_mod.candidate_key);
        defer self.alloc.free(candidate);
        var parsed_candidate = try row_policy_bundle_mod.Installed.init(self.alloc, candidate, self.core.identity_namespace.table_id, schema);
        defer parsed_candidate.deinit();
        if (parsed_candidate.parsed.value.table_id != publication.table_id or
            parsed_candidate.parsed.value.policy_generation != publication.policy_generation or
            parsed_candidate.parsed.value.catalog_epoch != publication.catalog_epoch or
            !std.mem.eql(u8, &parsed_candidate.parsed.value.schema_digest, &publication.schema_digest) or
            parsed_candidate.parsed.value.phase != (if (phase == .serving_install)
                @as(policies_mod.Publication.Phase, .pending_install)
            else
                .pending_disable))
            return error.RowPolicyCatalogChanged;
        const candidate_definitions = try std.json.Stringify.valueAlloc(self.alloc, .{ .records = parsed_candidate.parsed.value.records, .settings = parsed_candidate.parsed.value.settings }, .{});
        defer self.alloc.free(candidate_definitions);
        const serving_definitions = try std.json.Stringify.valueAlloc(self.alloc, .{ .records = publication.records, .settings = publication.settings }, .{});
        defer self.alloc.free(serving_definitions);
        if (!std.mem.eql(u8, candidate_definitions, serving_definitions)) return error.RowPolicyCatalogChanged;
    }
    var next = previous;
    next.row_policy_generation = publication.policy_generation;
    next.row_policy_catalog_epoch = publication.catalog_epoch;
    next.row_policy_phase = .preparing;
    next.generation +|= 1;
    const catalog_bytes = next.encode();
    const encoded_pending = expected_receipt.encode();
    var txn = try self.core.store.beginWriteTxn();
    errdefer txn.abort();
    try txn.put(row_policy_bundle_mod.key, bundle_bytes);
    if (pending_phase) try txn.put(row_policy_bundle_mod.candidate_key, bundle_bytes);
    if (phase == .serving_install or phase == .serving_disable) try txn.delete(row_policy_bundle_mod.candidate_key);
    try txn.put(row_policy_bundle_mod.pending_key, &encoded_pending);
    try txn.put(table_catalog_mod.key, &catalog_bytes);
    try txn.put(receipt.key, receipt.value);
    var replication_lsn_buf: [replication_applied_lsn_value_len]u8 = undefined;
    if (replication_lsn) |lsn| {
        const marker = replicationAppliedSequenceWrite(lsn, &replication_lsn_buf);
        try txn.put(marker.key, marker.value);
    }
    if (replication_outbox) |outbox| try txn.put(outbox.key, outbox.value);
    if (replication_outbox != null) {
        self.local_execution.durable_replication_outbox_maybe.store(true, .release);
        self.local_execution.row_policy_replication_outbox_pending.store(true, .release);
    }
    try txn.commit();
    if (replication_outbox != null) {
        schedule_replication_recovery_on_exit = true;
    }
    self.core.table_catalog = next;
    // Keep the old immutable bundle alive for previously admitted bound
    // readers. The receipt probe swaps it only after those leases drain.
    try self.finishRowPolicyReplicationCommitLocked(replication_payload, if (replication_outbox) |outbox| outbox.key else null, &apply_held, &replication_mutation);
    return null;
}

/// Removes group-local apply history when this document generation is
/// materialized for a different Raft history (for example restore or the
/// destination side of a split). The caller must hold structural ownership
/// of the generation; ordinary Raft apply must never clear this fence.
pub fn clearOrderedApplyReceipt(self: *DB) !void {
    if (comptime storage_engine.enabled) return storage_engine.call(@src().fn_name, .{self});
    lockApply(self);
    defer self.core.unlockApply();
    try self.core.store.putBatch(&.{}, &.{internal_keys.ordered_document_applied_entry_key[0..]});
}

pub fn batchReplicatedApplyWithMarker(self: *DB, req: types.BatchRequest, applied_lsn_marker: ?u64) anyerror!void {
    var apply_req = req;
    apply_req.sync_level = .write;
    try self.batchInternal(apply_req, null, .{
        .validate_range_ownership = false,
        .wait_for_sync_level = false,
        .bypass_replication_write_gate = true,
        .replication_applied_lsn_marker = applied_lsn_marker,
    });
}

/// Apply an already decoded committed batch. The mutation and its replay
/// receipt remain in the same primary-store transaction.
pub fn applyReplicatedBatch(self: *DB, mutation: replicated_mutation.Batch, sequence: u64) !void {
    if (comptime storage_engine.enabled) return storage_engine.call(@src().fn_name, .{ self, mutation, sequence });
    var request = mutation.request;
    var options: BatchExecutionOptions = .{
        .validate_range_ownership = false,
        .wait_for_sync_level = false,
        .bypass_replication_write_gate = true,
        .replication_applied_lsn_marker = sequence,
    };
    switch (mutation.receipt) {
        .native => |position| options.native_topology_position = position,
        .ordered => |entry| options.ordered_apply_receipt = entry,
        .online_source => |index| {
            request.sync_level = .write;
            options.online_source_applied_index = index;
        },
        .none => request.sync_level = .write,
    }
    try self.batchInternal(request, null, options);
}

pub fn applyReplicatedSchema(self: *DB, mutation: replication_effects_mod.SchemaMutation, sequence: u64) !void {
    if (comptime storage_engine.enabled) return storage_engine.call(@src().fn_name, .{ self, mutation, sequence });
    if (mutation.published_child) |published|
        try self.setPublishedChildSchemaReplicatedApplyWithMarker(mutation.schema, mutation.public_schema_json orelse return error.InvalidGenerationPublication, published, sequence)
    else
        try self.setSchemaReplicatedApplyWithMarker(mutation.schema, mutation.public_schema_json, sequence);
}

pub fn applyReplicatedRowPolicy(self: *DB, mutation: replication_effects_mod.RowPolicyMutation, sequence: u64) !void {
    if (comptime storage_engine.enabled) return storage_engine.call(@src().fn_name, .{ self, mutation, sequence });
    _ = try applyRowPolicyPublicationInternal(self, 
        mutation.bundle,
        mutation.request,
        mutation.entry,
        sequence,
    );
}

pub fn applyReplicatedDerivedEffect(self: *DB, change: change_journal_mod.Record, primary: ?replication_effects_mod.primary_effect.View, sequence: u64) !u64 {
    if (comptime storage_engine.enabled) return storage_engine.call(@src().fn_name, .{ self, change, primary, sequence });
    var ctx = self.batchContext();
    return try appendReplicatedDerivedEffectContext(&ctx, change, primary, sequence);
}

/// Prepared relational work is optimistic: schema or index publication may
/// invalidate it before commit. Retry the complete preparation outside the
/// apply lock instead of falling back to parse/extract/encode while holding
/// the global serialization fence. The bound prevents catalog churn from
/// turning one request into unbounded CPU/provider work.
pub fn batchInternal(self: *DB, req: types.BatchRequest, profile: ?*BatchProfile, opts: BatchExecutionOptions) anyerror!void {
    if (comptime storage_engine.enabled) return storage_engine.call("batchInternal", .{ self, req, profile, opts });
    if (opts.visibility_completion != null or !opts.wait_for_sync_level or
        (req.sync_level != .enrichments and req.sync_level != .full_index) or !self.core.index_manager.hasGroupSources())
        return self.batchInternalCommit(req, profile, opts);
    var completion: execution_resources.BatchVisibilityCompletion = .{};
    defer completion.deinit(self.alloc);
    var commit_opts = opts;
    commit_opts.visibility_completion = &completion;
    try self.batchInternalCommit(req, profile, commit_opts);
    if (completion.sequence) |sequence| {
        // Group visibility can publish more control mutations. Release the
        // large preparation/commit frame before driving those writes; small
        // host stacks must never contain two nested commit frames.
        const started = monotonicTimeNs();
        defer if (profile) |active| {
            const elapsed = monotonicTimeNs() - started;
            active.total_ns += elapsed;
            active.sync_wait_ns += elapsed;
            active.wait_sync_ns += elapsed;
        };
        try self.waitForSyncLevelWithCancellation(completion.sync_level, sequence, completion.targets, opts.visibility_cancellation, completion.enrichment_fully_precomputed);
    }
}

/// Bind a completion/query proof to this replica's initial materialization.
/// Generated-coverage recovery has no catalog-admission marker: its durable
/// source-replay state supplies that boundary instead. Ordinary repairs
/// must never inherit this exception.
pub fn managedInitialBuildMinimumReplay(
    self: *DB,
    alloc: Allocator,
    intent: index_repair_state.IndexRepairIntent,
) !?u64 {
    if ((intent.kind != .dense_vector and intent.kind != .sparse_vector) or
        intent.work_class != .initial_build or
        intent.root_generation != self.core.root_generation or
        intent.group_id != self.localRepairGroupId()) return null;

    if (intent.trigger == .replay_artifact_unavailable) {
        if (intent.source_replay_state == .not_required) return null;
        return intent.target_sequence;
    }
    if (intent.trigger != .catalog_admission) return null;
    const admission_key = try internal_keys.managedIndexAdmissionKeyAlloc(alloc, intent.index_name);
    defer alloc.free(admission_key);
    const admission_raw = self.core.store.get(alloc, admission_key) catch |err| switch (err) {
        error.NotFound => return null,
        else => return err,
    };
    defer alloc.free(admission_raw);
    const marker = try decodeManagedIndexAdmissionMarker(admission_raw);
    if (marker.disposition != .managed_rebuild or marker.config_hash != intent.config_hash) return null;
    return @max(marker.replay_target_sequence, intent.target_sequence);
}

pub fn applyInitialChildPhase(self: *DB, fence: relational_integrity_topology_contract.Fence, control: relational_integrity_topology_contract.InitialChildControl, phase: relational_initial_child_publication.Phase, entry: OrderedApplyReceipt, native: bool, replication_lsn: ?u64, replication_payload: ?[]const u8) !void {
    const hidden = relational_initial_child_publication;
    if (fence.role != .child_generation_source or !fence.namespace.eql(self.core.identity_namespace) or
        std.mem.allEqual(u8, &control.plan_id, 0) or std.mem.allEqual(u8, &control.plan_digest, 0) or
        !std.mem.eql(u8, &control.catalog_digest, &fence.catalog_digest) or
        phase == .hidden or (replication_lsn != null and replication_payload != null)) return error.InvalidInitialChildPublication;
    const replay = if (native)
        try self.core.getStoreValue(self.alloc, hidden.key)
    else if (try self.orderedMutationAlreadyApplied(entry))
        (try self.core.getStoreValue(self.alloc, hidden.key)) orelse return error.InitialChildPublicationChanged
    else
        null;
    if (replay) |raw| {
        defer self.alloc.free(raw);
        const prior = try hidden.Record.decode(raw);
        if (!std.mem.eql(u8, &prior.plan_id, &control.plan_id) or
            !std.mem.eql(u8, &prior.plan_digest, &control.plan_digest) or
            !prior.namespace.eql(fence.namespace) or prior.schema_version != control.schema_version or
            !std.mem.eql(u8, &prior.schema_digest, &control.schema_digest) or
            !std.mem.eql(u8, &prior.public_schema_json_digest, &control.public_schema_json_digest) or
            !std.mem.eql(u8, &prior.catalog_digest, &control.catalog_digest))
            return error.InitialChildPublicationChanged;
        if (prior.phase == phase) {
            if (prior.phase_term != entry.term or prior.phase_index != entry.index) return error.InitialChildPublicationChanged;
            if (replication_lsn) |lsn| try self.recordReplicationApplied(lsn);
            return;
        }
        if (!native or prior.phase != .hidden) return error.InitialChildPublicationChanged;
    }
    var mutation = self.core.snapshot_admission.acquireMutation();
    defer mutation.release();
    try self.lockApplyForPortableRuntime();
    defer self.core.unlockApply();
    if (!native) switch (try orderedApplyDisposition(try readOrderedApplyReceipt(self.alloc, self.core.store), entry)) {
        .already_applied => return,
        .apply => {},
    };
    var txn = try self.core.store.beginWriteTxn();
    errdefer txn.abort();
    const before = try hidden.load(&txn);
    if (before == null and phase == .canceled) {
        const canceled: hidden.Record = .{
            .phase = .canceled,
            .plan_id = control.plan_id,
            .plan_digest = control.plan_digest,
            .namespace = fence.namespace,
            .schema_version = control.schema_version,
            .row_count = 0,
            .schema_digest = control.schema_digest,
            .public_schema_json_digest = control.public_schema_json_digest,
            .catalog_digest = control.catalog_digest,
            .provision_term = 0,
            .provision_index = 0,
            .phase_term = entry.term,
            .phase_index = entry.index,
        };
        _ = try hidden.stageUnprovisionedCancel(&txn, canceled);
        try stageApplyMarkers(&txn, if (native) null else entry, replication_lsn);
        try self.stageRestoreStagingReplicationOutbox(&txn, replication_payload);
        try txn.commit();
        self.local_execution.initial_child_hidden.store(true, .release);
        return;
    }
    const current = before orelse return error.InitialChildPublicationMissing;
    if (!current.namespace.eql(fence.namespace) or
        !std.mem.eql(u8, &current.catalog_digest, &fence.catalog_digest) or
        !std.mem.eql(u8, &current.plan_id, &control.plan_id) or
        !std.mem.eql(u8, &current.plan_digest, &control.plan_digest) or
        current.schema_version != control.schema_version or
        !std.mem.eql(u8, &current.schema_digest, &control.schema_digest) or
        !std.mem.eql(u8, &current.public_schema_json_digest, &control.public_schema_json_digest)) return error.InitialChildPublicationChanged;
    if (native and current.phase == phase) {
        if (current.phase_term != entry.term or current.phase_index != entry.index) return error.InitialChildPublicationChanged;
        return;
    }
    if (native and current.phase != .hidden) return error.InitialChildPublicationChanged;
    if (current.provision_term == 0) {
        if (phase != .canceled) return error.InitialChildPublicationChanged;
        try stageApplyMarkers(&txn, if (native) null else entry, replication_lsn);
        try self.stageRestoreStagingReplicationOutbox(&txn, replication_payload);
        try txn.commit();
        return;
    }
    const stored_catalog = txn.get(relational_integrity_catalog.key) catch return error.IntegrityCatalogChanged;
    var catalog_digest: [32]u8 = undefined;
    std.crypto.hash.Blake3.hash(stored_catalog, &catalog_digest, .{});
    if (!std.mem.eql(u8, &catalog_digest, &current.catalog_digest)) return error.IntegrityCatalogChanged;
    const stored_schema = txn.get(public_schema_json_key) catch return error.IntegrityCatalogChanged;
    var schema_digest: [32]u8 = undefined;
    std.crypto.hash.Blake3.hash(stored_schema, &schema_digest, .{});
    if (!std.mem.eql(u8, &schema_digest, &current.public_schema_json_digest)) return error.IntegrityCatalogChanged;
    if (phase == .released) {
        var parsed_schema = try public_table_schema.parseValidatedTableSchema(self.alloc, stored_schema);
        defer parsed_schema.deinit(self.alloc);
        var parsed_catalog = try relational_integrity_catalog.decode(self.alloc, stored_catalog);
        defer parsed_catalog.deinit();
        const fks = try parsed_schema.relationalForeignKeyDefinitions(self.alloc);
        defer if (fks.len > 0) self.alloc.free(fks);
        for (fks) |fk| {
            // Physical table identity is hash-derived by metadata. Match
            // the hidden child without relying on a public route, then
            // require its own accepted parent generation to be durable.
            if (!std.mem.startsWith(u8, fk.parent_table, "table:")) continue;
            const physical_id = std.hash.Wyhash.hash(0x54424c45, fk.parent_table);
            if ((if (physical_id == 0) @as(u64, 1) else physical_id) != current.namespace.table_id) continue;
            const scope = (try relational_integrity_generation_admission.load(&txn, fk.parent_table, fk.name)) orelse return error.InitialChildPublicationChanged;
            const binding = parsed_catalog.find(.foreign_key, fk.name) orelse return error.IntegrityCatalogChanged;
            if (scope.phase != .active or scope.active_generation == null or
                !std.mem.eql(u8, &scope.active_generation.?, &binding.generation) or
                !std.mem.eql(u8, &scope.plan_id, &current.plan_id) or
                !std.mem.eql(u8, &scope.decision_digest, &current.plan_digest))
                return error.InitialChildPublicationChanged;
        }
    }
    const table_raw = txn.get(table_catalog_mod.key) catch return error.InitialChildPublicationChanged;
    if ((try table_catalog_mod.Catalog.decode(table_raw)).row_count != 0) return error.InitialChildPublicationChanged;
    _ = try hidden.stagePhase(&txn, current, phase, entry.term, entry.index);
    try stageApplyMarkers(&txn, if (native) null else entry, replication_lsn);
    try self.stageRestoreStagingReplicationOutbox(&txn, replication_payload);
    try txn.commit();
    if (phase == .released) self.local_execution.initial_child_hidden.store(false, .release);
}

/// Internal source-retention controls share the lifecycle transaction path:
/// source progress, retained log, replication marker and durable outbox are
/// one commit. The public API has no admission route for these commands.
pub fn applyOnlineSourceBatch(self: *DB, req: types.BatchRequest, opts: BatchExecutionOptions) !void {
    try online_source_contract.validateRequest(req);
    const command = req.online_source orelse return error.InvalidOnlineSourceCommand;
    if (openModeRequiresReadOnlyBackends(self.open_mode)) return error.ReadOnly;
    // A committed admission retry must not need a later Raft GC command to
    // free its slot. Try the bounded local cleanup catalog without waiting
    // on exports and before taking the apply fence.
    if (command == .admit) source_pin.reconcileReleased(self) catch |err| switch (err) {
        error.OnlineSourcePinPending => {}, // Repair this exact prepared cut below.
        else => return err,
    };
    var replication_mutation = if (opts.bypass_replication_write_gate) null else self.acquireReplicationMutationShared();
    defer if (replication_mutation) |*lease| lease.release();
    if (!opts.bypass_replication_write_gate) {
        try self.enforceReplicationWriteGate();
        try self.ensureDurableReplicationStartupBarrier();
        try self.flushDurableReplicationOutboxes();
        try self.preflightReplicationBatchSyncCommit();
    }
    var applied_index = if (opts.ordered_apply_receipt) |entry| entry.index else opts.online_source_applied_index orelse 0;
    var payload: ?[]u8 = null;
    defer if (payload) |bytes| self.alloc.free(bytes);
    apply_source: {
        var admission = self.core.snapshot_admission.acquireMutation();
        defer admission.release();
        // Index deletion must join its derived worker before taking apply
        // exclusive. Serialize that join and the final catalog mutation
        // with source admission without blocking the Raft apply lane on a
        // worker: a busy try-lock leaves this committed entry unapplied and
        // the progress driver retries it after the index transition.
        const catalog_admission = command == .admit;
        if (catalog_admission and !self.local_execution.index_structural_mutation_mutex.tryLock())
            return error.StorageBusy;
        defer if (catalog_admission) self.local_execution.index_structural_mutation_mutex.unlock();
        try self.lockApplyForPortableRuntime();
        defer self.core.unlockApply();
        if (opts.ordered_apply_receipt) |entry| switch (try orderedApplyDisposition(try readOrderedApplyReceipt(self.alloc, self.core.store), entry)) {
            .already_applied => {
                if (command == .admit) {
                    const repair = blk: {
                        var read = try self.core.store.beginReadTxn();
                        defer read.abort();
                        const pending = try source_pin_state_mod.load(&read) orelse break :blk false;
                        break :blk std.mem.eql(u8, &pending.namespace, &command.scope().namespace()) and std.mem.eql(u8, &pending.pin, &command.scope().pin());
                    };
                    if (repair) try source_pin.ensureAssumeApply(self, command.scope());
                }
                break :apply_source;
            },
            .apply => {},
        };
        if (!command.scope().fence.namespace.eql(self.core.identity_namespace)) return error.IdentityNamespaceMismatch;
        if (command.scope().authority == .native and opts.ordered_apply_receipt != null) return error.OnlineSourceScopeChanged;
        // Complete any committed native admission before a retry can
        // advance the owner clock. It must never recapture a later cut.
        if (command.scope().authority == .native and command == .admit) {
            const pending_scope = blk: {
                var read = try self.core.store.beginReadTxn();
                defer read.abort();
                const pending = try source_pin_state_mod.load(&read) orelse break :blk null;
                break :blk try online_source_contract.Scope.decode(&pending.scope_bytes);
            };
            if (pending_scope) |scope| {
                if (!std.meta.eql(scope, command.scope())) return error.OnlineSourcePinPending;
                try source_pin.ensureAssumeApply(self, scope);
            }
        }
        // Reject unsupported backends before publishing a durable write
        // fence which they could never turn into an immutable pin.
        if (command == .admit) {
            if (try self.core.store.hasGraphEndpointCleanup()) return error.IntegrityTopologyBusy;
            const lsm = self.core.primary_store_owner.lsmBackend() orelse return error.BackupSealBackendUnsupported;
            if (lsm.storage) |storage| {
                if (!storage.supportsHostPathGenerationPublication()) return error.NativeBackupStorageBackendUnsupported;
            }
            // The catalog predicate is local to this DB, not Raft state.
            // Rechecking it while applying a committed entry could reject
            // on a follower after its leader succeeded. Distributed online
            // merge is rejected before proposal until catalog/membership
            // barriers are Raft-ordered; retain the local recheck for
            // native standalone admission only.
            if (command.scope().authority == .native)
                try online_merge_io.requireAdmissibleSourceAtCommitAssumeApply(self, self.alloc);
        }
        var txn = try self.core.store.beginWriteTxn();
        var txn_open = true;
        defer if (txn_open) txn.abort();
        if (command == .admit) {
            if (req.artifact_catalog) |install| try artifact_inventory.stageOrdered(self.alloc, &txn, install, applied_index);
            if (command.admit.artifact_catalog) |binding| {
                try artifact_inventory.requireReady(self.alloc, &txn, command.scope().namespace(), binding);
                if (!try self.artifactMaterializationsReady(&txn, try artifact_inventory.catalogs(&txn))) return error.ArtifactCatalogDrift;
            } else if (command.scope().authority == .raft) {
                const ordered = try artifact_inventory.load(self.alloc, &txn);
                defer if (ordered) |value| value.deinit();
                if (ordered != null) return error.ArtifactCatalogDrift;
            }
        } else if (command != .release) {
            const progress = try online_source.status(&txn, command.scope());
            if (progress.artifact_catalog) |binding|
                try artifact_inventory.requireReady(self.alloc, &txn, command.scope().namespace(), binding);
        }
        try restore_staging.requireScope(self.alloc, &txn, null, false);
        if (command != .admit and command != .release) try source_pin_state_mod.requireNoPrepared(&txn, command.scope().namespace());
        if (try doc_identity.loadNamespaceTxn(&txn)) |stored| {
            if (!stored.eql(command.scope().fence.namespace)) return error.IdentityNamespaceMismatch;
        } else {
            try doc_identity.writeNamespaceTxn(&txn, command.scope().fence.namespace);
        }
        const source_authority = source_authority_mod;
        if (command.scope().authority == .native) {
            applied_index = try source_authority.advance(&txn, command.scope().namespace(), opts.online_source_applied_index);
        } else if (try source_authority.load(&txn) != null) {
            _ = try source_authority.require(&txn, .raft, command.scope().namespace());
        }
        if (command == .final_fence) {
            var manager = try self.core.initTxnManager();
            defer manager.deinit();
            try relational_integrity_topology.requireDrained(&txn, &manager, command.scope().fence);
        }
        if (command == .admit) {
            // Candidate discovery is advisory. Recheck the durable local
            // receiver transition in this admission transaction so an
            // intervening ordinary merge cannot acquire both roles.
            const merge_raw = txn.get(merge_state_mod.key) catch |err| switch (err) {
                // Older receiver checkpoints remain authoritative until
                // protectForSplit atomically upgrades their key. Do not
                // admit a source role over an active legacy receiver.
                error.NotFound => txn.get(merge_state_mod.legacy_key) catch |legacy_err| switch (legacy_err) {
                    error.NotFound => null,
                    else => return legacy_err,
                },
                else => return err,
            };
            if (merge_raw) |raw| {
                var state = try merge_state_mod.decodeAlloc(self.alloc, raw);
                defer state.deinit(self.alloc);
                if (state.phase != .finalized and state.phase != .rolled_back and state.phase != .none) return error.IntegrityTopologyBusy;
            }
        }
        try online_source.stage(&txn, command, applied_index);
        if (req.artifact_catalog) |install| try artifact_reconcile_intent.clear(self.alloc, &txn, install, applied_index);
        if (!opts.bypass_replication_write_gate and self.local_execution.replication_async_batch_mirror != null)
            payload = if (req.artifact_catalog != null) try replication_effects_mod.encodeArtifactCatalogMutationRequestAlloc(self.alloc, req, opts.ordered_apply_receipt orelse return error.InvalidArtifactCatalogCommand) else try replication_effects_mod.encodeOnlineSourceMutationRequestAlloc(self.alloc, req, applied_index);
        for (opts.extra_store_writes) |write| try txn.put(write.key, write.value);
        if (opts.ordered_apply_receipt) |entry| {
            var bytes: [ordered_apply_receipt_value_len]u8 = undefined;
            const marker = orderedApplyReceiptWrite(entry, &bytes);
            try txn.put(marker.key, marker.value);
        }
        if (opts.replication_applied_lsn_marker) |lsn| {
            var bytes: [replication_applied_lsn_value_len]u8 = undefined;
            const marker = replicationAppliedSequenceWrite(lsn, &bytes);
            try txn.put(marker.key, marker.value);
        }
        try self.stageRestoreStagingReplicationOutbox(&txn, payload);
        try txn.commit();
        txn_open = false;
        if (command == .admit) {
            try source_pin.afterPrepare();
            try source_pin.ensureAssumeApply(self, command.scope());
        }
    }
    if (command == .release) {
        const io = self.backend_runtime.io() orelse std.Options.debug_io;
        self.local_execution.source_publication.cancelScope(io, command.scope());
        self.local_execution.online_merge_reader.retire(io, command.scope());
    }
    if (command == .release) source_pin.reclaimReleased(self, command.scope()) catch |err| {
        // Release is already durable. Replica-local cleanup failures keep
        // the bounded slot reserved, but cannot suppress its outbox flush
        // or misreport the replicated terminal decision as rejected.
        if (builtin.is_test and err == error.InjectedSourcePinFailure) return err;
    };
    if (payload != null) try self.flushDurableReplicationOutboxes();
}

pub fn applyMergeProofAdoptionBatch(self: *DB, req: types.BatchRequest, opts: BatchExecutionOptions) !void {
    const adoption = merge_proof_adoption;
    const proof_batch = source_proof_batch;
    const provenance = artifact_producer_provenance;
    try adoption.validateRequest(req);
    const command = req.merge_proof_adoption orelse return error.InvalidBatchRequest;
    const entry = opts.ordered_apply_receipt orelse return error.InvalidBatchRequest;
    if (entry.term == 0 or entry.index == 0 or opts.artifact_upload_finalize != null or
        opts.extra_store_writes.len != 0 or opts.extra_store_deletes.len != 0) return error.InvalidBatchRequest;
    if (openModeRequiresReadOnlyBackends(self.open_mode)) return error.ReadOnly;

    var replication_mutation = if (opts.bypass_replication_write_gate) null else self.acquireReplicationMutationShared();
    defer if (replication_mutation) |*lease| lease.release();
    if (!opts.bypass_replication_write_gate) {
        try self.enforceReplicationWriteGate();
        try self.ensureDurableReplicationStartupBarrier();
        try self.flushDurableReplicationOutboxes();
        try self.preflightReplicationBatchSyncCommit();
    }
    const replication_payload = if (!opts.bypass_replication_write_gate and self.local_execution.replication_async_batch_mirror != null)
        try replication_effects_mod.encodeMergeProofAdoptionMutationRequestAlloc(self.alloc, req, entry)
    else
        null;
    defer if (replication_payload) |value| self.alloc.free(value);

    var preparation_budget = if (self.core.index_manager.resource_manager) |manager|
        resource_manager_mod.BudgetedAllocator.init(manager, .shard_transition_working_set, self.alloc, 1)
    else
        null;
    defer if (preparation_budget) |*budget| budget.deinit();
    const preparation_alloc = if (preparation_budget) |*budget| budget.allocator() else self.alloc;
    var fence: ?adoption.Fence = null;
    defer if (fence) |*value| value.deinit(preparation_alloc);
    var prepared: ?proof_batch.PreparedAdoption = null;
    defer if (prepared) |*value| value.deinit(preparation_alloc);
    {
        var read = try self.core.store.beginReadTxnWithBlockCacheAdmission(.transient);
        defer read.abort();
        fence = adoption.loadFence(preparation_alloc, &read, self.core.identity_namespace, command) catch |err| {
            if (err == error.OutOfMemory) if (preparation_budget) |*budget| if (budget.denied()) return error.ResourceBudgetExceeded;
            return err;
        };
        if (fence) |*current| {
            var plan = proof_batch.ReceiverProducerPlan.initForMerge(preparation_alloc, &read, current.progress.value) catch |err| switch (err) {
                error.ArtifactCatalogDrift => null,
                error.OutOfMemory => {
                    if (preparation_budget) |*budget| if (budget.denied()) return error.ResourceBudgetExceeded;
                    return err;
                },
                else => return err,
            };
            defer if (plan) |*value| value.deinit();
            if (plan) |*mapping| {
                var receiver_namespace: [24]u8 = undefined;
                var donor_namespace: [24]u8 = undefined;
                doc_identity.encodeNamespace(&receiver_namespace, current.progress.value.receiver_namespace);
                doc_identity.encodeNamespace(&donor_namespace, current.progress.value.source.namespace);
                prepared = proof_batch.prepareAdoption(preparation_alloc, &read, receiver_namespace, try current.donorRange(), command.source_pin, donor_namespace, command.proof_digest, mapping) catch |err| switch (err) {
                    error.ArtifactAdoptionUnsupported, error.ArtifactCatalogDrift => null,
                    error.OutOfMemory => {
                        if (preparation_budget) |*budget| if (budget.denied()) return error.ResourceBudgetExceeded;
                        return err;
                    },
                    else => return err,
                };
            }
        }
    }

    var mutation = self.core.snapshot_admission.acquireMutation();
    defer mutation.release();
    try self.lockApplyForPortableRuntime();
    var apply_held = true;
    defer if (apply_held) self.core.unlockApply();
    if (try orderedApplyDisposition(try readOrderedApplyReceipt(self.alloc, self.core.store), entry) == .already_applied) return;
    var txn = try self.core.store.beginWriteTxn();
    errdefer txn.abort();
    if (fence) |*current| if (prepared) |*candidate| {
        if (std.mem.eql(u8, &candidate.candidate.donor.record_digest, &command.record_digest) and
            try current.matchesStored(&txn) and
            try proof_batch.revalidateReceiverCandidate(preparation_alloc, &txn, candidate.candidate.receiver.namespace, candidate.candidate))
        {
            // No donor receipt, sequence or physical position is trusted.
            // The ordered Raft index is this metadata-only receipt's
            // monotonic publication sequence; copied values were already
            // installed and journaled by merge pages.
            try provenance.stageAdoptedIndexed(&txn, candidate.adopted.proof, candidate.adopted.encoded, candidate.positions, .{ .raft = .{ .term = entry.term, .index = entry.index } }, &candidate.references, entry.index);
        }
    };
    try stageApplyMarkers(&txn, entry, opts.replication_applied_lsn_marker);
    try self.stageRestoreStagingReplicationOutbox(&txn, replication_payload);
    try txn.commit();
    self.core.unlockApply();
    apply_held = false;
    if (replication_payload != null) try self.flushDurableReplicationOutboxes();
}

pub fn applyGenerationGcBatch(self: *DB, req: types.BatchRequest, opts: BatchExecutionOptions) !void {
    try req.relational_generation_gc.?.validate();
    if (req.relational_topology != null or req.row_policy_publication != null or req.online_source != null or req.restore_staging != null or
        req.restore_staging_scope != null or req.restore_staging_plan_id != null or req.merge_page != null or
        req.transaction != null or req.writes.len != 0 or req.deletes.len != 0 or req.transforms.len != 0 or
        req.graph_writes.len != 0 or req.graph_deletes.len != 0 or req.integrity.len != 0 or
        req.integrity_commands.len != 0 or req.predicates.len != 0 or req.relational_activation != null or
        req.relational_retirement != null or req.relational_index_maintenance != null or req.relational_repair or
        req.split_checkpoint != null or req.split_replication != null or req.split_transition != null or
        req.merge_checkpoint != null or req.merge_replication != null or req.merge_source_transition != null or
        req.merge_artifacts.len != 0 or req.range_guards.len != 0 or req.activate_range_tracking or
        req.schema_version != null or req.relational_schema_version != null or req.relational_integrity_generation_set != null) return error.InvalidBatchRequest;
    var mutation = if (opts.bypass_replication_write_gate) null else self.acquireReplicationMutationShared();
    defer if (mutation) |*lease| lease.release();
    if (!opts.bypass_replication_write_gate) {
        try self.enforceReplicationWriteGate();
        try self.ensureDurableReplicationStartupBarrier();
        try self.flushDurableReplicationOutboxes();
        try self.preflightReplicationBatchSyncCommit();
    }
    const payload = if (!opts.bypass_replication_write_gate and self.local_execution.replication_async_batch_mirror != null)
        try replication_effects_mod.encodeBatchMutationRequestAlloc(self.alloc, req)
    else
        null;
    defer if (payload) |bytes| self.alloc.free(bytes);
    var admission = self.core.snapshot_admission.acquireMutation();
    defer admission.release();
    try self.lockApplyForPortableRuntime();
    defer self.core.unlockApply();
    if (opts.ordered_apply_receipt) |entry| switch (try orderedApplyDisposition(try readOrderedApplyReceipt(self.alloc, self.core.store), entry)) {
        .already_applied => return,
        .apply => {},
    };
    var txn = try self.core.store.beginWriteTxn();
    errdefer txn.abort();
    if (req.relational_generation_gc.?.owner_group_id == 0 or !req.relational_generation_gc.?.namespace.eql(self.core.identity_namespace)) return error.IdentityNamespaceMismatch;
    // A page prepared at read-index can lose a race to a new reference,
    // activation or topology fence before its Raft entry applies. Such an
    // admitted entry must still advance the applied marker on every
    // replica; the next bounded sweep will reprepare from durable state.
    if (try relational_integrity_topology.current(&txn) == null) {
        relational_integrity_generation_retirement.applyGcPage(&txn, req.relational_generation_gc.?) catch |err| switch (err) {
            error.GenerationRetirementChanged => {},
            else => return err,
        };
    }
    try stageApplyMarkers(&txn, opts.ordered_apply_receipt, opts.replication_applied_lsn_marker);
    try self.stageRestoreStagingReplicationOutbox(&txn, payload);
    try txn.commit();
    if (payload != null) try self.flushDurableReplicationOutboxes();
}

pub fn retireResolverReplayBeforeCatalogRemoval(self: *DB) !void {
    try self.runUntilIdle();
    const resolution_stats = self.resolutionStageStats();
    if (resolution_stats.catch_up_required or resolution_stats.blocked) return error.ResolverReplayPending;
    const promotion_stats = self.promotionStageStats();
    if (promotion_stats.catch_up_required or promotion_stats.blocked) return error.ResolverReplayPending;
}

pub fn probeDerivedReplayTargetSequence(
    self: *DB,
    alloc: Allocator,
    replay_source: replay_source_mod.Source,
    index_ref: index_manager_mod.ManagedIndexRef,
    applied_sequence: u64,
) !u64 {
    _ = self;
    return try replay_source.latestMatchingSequence(
        alloc,
        applied_sequence,
        managedIndexReplayHint(index_ref.kind),
    );
}

pub fn listDerivedReplayDebt(self: *DB, alloc: Allocator) ![]DerivedReplayDebtStatus {
    if (comptime storage_engine.enabled) return storage_engine.call(@src().fn_name, .{ self, alloc });
    try self.lockApplySharedForPortableRuntime();
    defer self.core.unlockApplyShared();

    return try self.listDerivedReplayDebtAssumeApplyLockHeld(alloc);
}

pub fn listDerivedReplayDebtAssumeApplyLockHeld(self: *DB, alloc: Allocator) ![]DerivedReplayDebtStatus {
    const managed_indexes = try self.core.managedIndexes(alloc);
    var transferred_names: usize = 0;
    errdefer {
        for (managed_indexes[transferred_names..]) |index_ref| alloc.free(@constCast(index_ref.name));
        alloc.free(managed_indexes);
    }

    const out = try alloc.alloc(DerivedReplayDebtStatus, managed_indexes.len);
    var initialized: usize = 0;
    errdefer {
        for (out[0..initialized]) |*status| status.deinit(alloc);
        alloc.free(out);
    }

    for (managed_indexes) |index_ref| {
        const applied_sequence = try self.managedIndexAppliedSequence(alloc, index_ref.name);
        const target_sequence = try self.probeDerivedReplayTargetSequence(
            alloc,
            self.core.replaySource(),
            index_ref,
            applied_sequence,
        );
        out[initialized] = .{
            .index_name = index_ref.name,
            .kind = index_ref.kind,
            .applied_sequence = applied_sequence,
            .target_sequence = target_sequence,
            .catch_up_required = applied_sequence < target_sequence,
            .replay_document_not_visible_skipped = if (self.core.index_manager.resource_manager) |manager|
                manager.replayDocumentNotVisibleSkippedTotal(resource_manager_mod.replayOwnerIdFromPtr(self.core.index_manager), index_ref.name)
            else
                0,
        };
        initialized += 1;
        transferred_names += 1;
    }
    alloc.free(managed_indexes);
    return out;
}

pub fn persistedReplayStageStats(self: *DB, scope_name: []const u8, force_enabled: bool) !types.ReplayStageStats {
    const resources = self.core.batchExecutionResources();
    const applied = try enrichment_state.loadAppliedSequence(self.alloc, resources.store, scope_name);
    const target = @max(applied, self.core.nextDerivedSequence());
    return .{
        .enabled = force_enabled or target > 0 or applied < target,
        .target_sequence = target,
        .applied_sequence = applied,
        .catch_up_required = applied < target,
    };
}

pub fn resolverReplayDiagnosticsLocked(self: *DB, alloc: Allocator) !types.ResolverReplayDiagnostics {
    const catalog = self.core.index_manager.resolvers.items;
    var resolvers = try alloc.alloc(types.ResolverReplayDiagnostic, catalog.len);
    var initialized: usize = 0;
    errdefer {
        for (resolvers[0..initialized]) |resolver| {
            alloc.free(resolver.name);
            alloc.free(resolver.table);
            alloc.free(resolver.source_artifact);
            alloc.free(resolver.resolution_artifact);
        }
        if (resolvers.len > 0) alloc.free(resolvers);
    }

    for (catalog) |cfg| {
        const name = try alloc.dupe(u8, cfg.name);
        errdefer alloc.free(name);
        const table = try alloc.dupe(u8, cfg.table);
        errdefer alloc.free(table);
        const source_artifact = try alloc.dupe(u8, cfg.source_artifact);
        errdefer alloc.free(source_artifact);
        const resolution_artifact = try alloc.dupe(u8, cfg.resolution_artifact);
        errdefer alloc.free(resolution_artifact);
        resolvers[initialized] = .{
            .name = name,
            .config_generation = cfg.config_generation,
            .effective_generation = cfg.effectiveGeneration(),
            .curation_revision = cfg.curation_revision,
            .namespace_curation_generation = cfg.namespace_curation_generation,
            .namespace_curation_table_id = cfg.namespace_curation_table_id,
            .namespace_curation_missing = cfg.namespace_curation_missing,
            .table = table,
            .source_artifact = source_artifact,
            .resolution_artifact = resolution_artifact,
        };
        initialized += 1;
    }

    return .{
        .reresolve_pending = try resolution_runtime_mod.hasDurableReresolveBacklog(self.core.store),
        .resolver_count = @intCast(catalog.len),
        .resolution_runtime_present = self.resolution_runtime != null,
        .resolution_worker_started = if (self.resolution_runtime) |runtime| runtime.worker_started.load(.acquire) else false,
        .promotion_runtime_present = self.promotion_runtime != null,
        .promotion_worker_started = if (self.promotion_runtime) |runtime| runtime.worker_started.load(.acquire) else false,
        .resolvers = resolvers[0..initialized],
    };
}

pub fn catchUpPendingDerivedReplay(self: *DB) !void {
    if (comptime storage_engine.enabled) return storage_engine.call(@src().fn_name, .{self});
    try replayPendingDerivedBatches(self, null, null, .{});
}

pub fn catchUpPendingDerivedReplayWithProgress(
    self: *DB,
    progress_ctx: *anyopaque,
    progress_hook: ReplayProgressHook,
) !void {
    if (comptime storage_engine.enabled) return storage_engine.call(@src().fn_name, .{ self, progress_ctx, progress_hook, storage_engine.ReplayBoundary.local_dispatch });
    try replayPendingDerivedBatches(self, progress_ctx, progress_hook, .{});
}

pub fn resolverReplayBlockedAfterRunnableDrain(self: *DB) bool {
    const resolution_stats = self.resolutionStageStats();
    if (resolution_stats.catch_up_required and !resolution_stats.blocked) return false;
    const promotion_stats = self.promotionStageStats();
    return promotion_stats.blocked;
}

pub fn drainReplayStagesUntilStableWithOptions(self: *DB, options: ReplayDrainOptions) !void {
    var rounds: usize = 0;
    while (rounds < run_until_idle_max_replay_rounds) : (rounds += 1) {
        const starting_sequence = self.core.nextDerivedSequence();
        const starting_resolution_applied = if (self.resolution_runtime) |runtime| runtime.stats().applied_sequence else 0;
        const starting_promotion_applied = if (self.promotion_runtime) |runtime| runtime.stats().applied_sequence else 0;
        try self.runMaintenanceUntilWithOptions(self.currentMaintenanceTargetSequence(), .{}, options);
        const drained_sequence = self.core.nextDerivedSequence();

        const drain_resolver_replay = resolverReplayNeedsDrain(self);
        if (drain_resolver_replay) {
            if (self.resolution_runtime) |runtime| {
                // nextDerivedSequence() is the highest committed sequence,
                // not the next append sequence. Include that record when
                // recovering a missed runtime notification.
                if (drained_sequence > 0) runtime.notifySequence(drained_sequence);
                try runtime.catchUp();
            }

            if (self.promotion_runtime) |runtime| {
                const latest = self.core.nextDerivedSequence();
                if (latest > 0) runtime.notifySequence(latest);
                try runtime.catchUp();
            }

            // Resolution can append graph/promotion replay before
            // promotion discovers that its external sink is unavailable.
            // Drain those independent derived consumers before reporting
            // the blocked stage; otherwise runUntilIdle can return with a
            // durable resolution artifact that is absent from the graph.
            if (self.core.nextDerivedSequence() > drained_sequence) {
                try self.runMaintenanceUntilWithOptions(self.currentMaintenanceTargetSequence(), .{}, options);
            }
            if (resolverReplayBlockedAfterRunnableDrain(self)) return;
        }

        const resolution_advanced = if (self.resolution_runtime) |runtime|
            runtime.stats().applied_sequence > starting_resolution_applied
        else
            false;
        const promotion_advanced = if (self.promotion_runtime) |runtime|
            runtime.stats().applied_sequence > starting_promotion_applied
        else
            false;
        if (options.bounded_replay or (self.core.nextDerivedSequence() <= starting_sequence and !resolution_advanced and !promotion_advanced)) return;
    }
    return error.RunUntilIdleDidNotConverge;
}

pub fn runUntilIdleWithReplayDrainOptions(self: *DB, options: ReplayDrainOptions) !void {
    _ = try self.retryPortableRuntimeActivationIfNeeded();
    // Manual owners need synchronous resolver drivers both during shadow
    // construction and after a crash between activation and promotion.
    // Creating these drivers does not start background workers.
    if (!openModeRequiresReadOnlyBackends(self.open_mode) and self.hasConfiguredResolvers()) {
        if (self.resolution_runtime == null) try self.initResolutionRuntime();
        if (self.promotion_runtime == null) try self.initPromotionRuntime();
    }
    {
        const group_state = options.group_source_state orelse self.borrowed_group_source_state orelse &self.group_source_owner.state;
        const drive_groups = self.async_context.replay_publication == null;
        if (drive_groups) {
            // A manual drain owns this producer lane until its fair round
            // and downstream replay settle. A busy resident slice must not
            // be mistaken for proof of idle, nor race the idle-round count.
            while (!group_state.slice_mutex.tryLock()) {
                try ensureSnapshotActive(options.cancellation);
                platform_time.yieldNow();
            }
            group_state.idle_definitions = 0;
        }
        defer if (drive_groups) group_state.slice_mutex.unlock();
        while (true) {
            try ensureSnapshotActive(options.cancellation);
            if (options.deadline_ns) |deadline| if (monotonicTimeNs() >= deadline) return error.RunUntilIdleDidNotConverge;
            // Detached snapshots compute providers; their connection owner
            // issues group commands on the current coordinator instead.
            const group_pending = if (drive_groups) try self.runGroupSourceSliceLocked(group_state) else false;
            try self.drainReplayStagesUntilStableWithOptions(options);
            if (!group_pending or options.bounded_replay) break;
        }
    }
    if (options.drain_graph_replacements) {
        const replacement_catalog_generation = self.core.index_manager.write_plan_generation.load(.acquire);
        if (options.drain_relational_indexes) {
            while (try self.advanceGraphReplacement()) {
                try ensureSnapshotActive(options.cancellation);
                try self.drainReplayStagesUntilStableWithOptions(options);
            }
        } else _ = try self.advanceGraphReplacement();
        // The final swap can return false while releasing durable resolver
        // replay. Drain that handoff before reporting idle, including manual
        // owners with no worker to consume it after this call returns.
        if (self.core.index_manager.write_plan_generation.load(.acquire) != replacement_catalog_generation)
            try self.drainReplayStagesUntilStableWithOptions(options);
    }
    _ = try self.evaluateAlgebraicAdaptiveCandidates();
    while (try self.runAlgebraicAdaptiveWork() != 0) {}
    try self.flushAppliedSequencesForIdle();
    try self.drainScheduledTextMerges();
    try self.runArtifactRepairMetadataMaintenanceUntilIdle();
    if (options.drain_relational_indexes) {
        try self.runRelationalIndexMaintenanceUntilIdle(options.cancellation, options.deadline_ns);
    } else {
        try ensureSnapshotActive(options.cancellation);
        _ = try self.runRelationalIndexMaintenancePass();
    }
    // Preserve the ordinary bounded maintenance pass at the lifecycle
    // boundary: besides posting repair it advances tree-link repair,
    // posting checkpoints, and quiescent vector-block publication.
    _ = try self.runDensePostingMaintenanceForIdle();
    _ = try self.runGraphMetricMaintenanceForIdle();
    _ = try self.drainDensePostingMaintenanceForIdle();
    // This is a caller-proven stable writer boundary. Publish the native
    // exact-vector generation here rather than depending on a later live
    // write-cache maintenance tick; read-only restarts otherwise have no
    // resident writer after startup catch-up retires its short-lived DB.
    _ = try self.publishVectorBlockBasesAtStableTip();
    // Publishing the shared vector generation changes the exact-vector
    // residency policy. Recheck posting debt before advertising idle so
    // no deferred centroid/payload work escapes into the first query.
    _ = try self.drainDensePostingMaintenanceForIdle();
    // Dense WAL coverage is the hot-path applied watermark. Persist the
    // human-facing catalog snapshot once at an explicit idle boundary,
    // then let the normal LSM idle drain include this small control write.
    _ = try finalizeCoveredDenseProjectionCheckpointsIfIdle(self.async_context);
    try self.saveAllLiveIndexStatusSnapshots(self.alloc);
    _ = try self.runLsmMaintenanceUntilIdle();
}

pub fn populateIndexSourceReplayStats(self: *DB, alloc: Allocator, index_name: []const u8, published_sequence: u64, aggregate_target_sequence: u64, item: *types.DBIndexStats) !void {
    const names = try self.core.index_manager.artifactSourceNamesForIndexAlloc(alloc, index_name);
    defer if (names.len > 0) alloc.free(names);
    if (names.len == 0) return;

    const statuses = try alloc.alloc(types.IndexSourceReplayStatus, names.len);
    var initialized: usize = 0;
    errdefer {
        for (statuses[0..initialized]) |status| alloc.free(status.artifact_name);
        for (names[initialized..]) |name| alloc.free(name);
        alloc.free(statuses);
    }
    var producer_plan = try self.core.index_manager.acquireWritePlanSnapshot();
    defer producer_plan.release();
    var producer_read = try self.core.store.beginReadTxn();
    defer producer_read.abort();
    const legacy = try artifact_publication.authority(&producer_read) == null;
    const repair_summary_ready = try self.artifactRepairSummaryReady(alloc);
    for (names) |name| {
        const source_repair = try self.artifactRepairSummarySourceSnapshot(
            alloc,
            index_name,
            name,
            repair_summary_ready,
        );
        const local_scope = try artifact_producer_readiness.localScopeAlloc(alloc, index_name, name, producer_plan.plan());
        defer alloc.free(local_scope);
        const legacy_complete = if (legacy) blk: {
            const generation = self.core.index_manager.coverageGenerationForIndex(index_name) orelse break :blk false;
            const counts = try DerivedCoverageCounters.load(alloc, &producer_read, if (item.kind == .dense_vector or item.kind == .sparse_vector) index_name else local_scope, generation);
            const total = counts.source_total orelse break :blk false;
            break :blk if (total == 0) true else counts.produced != null and counts.skipped != null and counts.terminal_failed == 0 and
                counts.produced.? +| counts.skipped.? == total;
        } else false;
        statuses[initialized] = .{
            .artifact_name = name,
            .published_sequence = published_sequence,
            .target_sequence = try self.artifactSourceTargetSequence(alloc, name, published_sequence, aggregate_target_sequence),
            .failed = source_repair.ready and source_repair.count != 0,
            .repair_issue_count = source_repair.count,
            .repair_summary_ready = source_repair.ready,
            .producer_complete = legacy_complete or try artifact_producer_readiness.sourceComplete(alloc, &producer_read, producer_plan.plan(), self.root_incarnation, name),
        };
        initialized += 1;
    }
    item.source_replay = statuses;
}

pub fn applyProjectionCheckpointStats(item: *types.DBIndexStats, checkpoint: apply_state.ProjectionCheckpoint, target_sequence: u64) void {
    item.projection_checkpoint_status = projectionCheckpointStatusName(checkpoint.status);
    item.projection_checkpoint_applied_sequence = checkpoint.applied_sequence;
    item.projection_checkpoint_generation = checkpoint.generation;
    item.projection_checkpoint_config_hash = checkpoint.config_hash;
    item.projection_checkpoint_published_count = checkpoint.published_count;
    item.checkpoint_replay_tail_sequence_count = target_sequence -| checkpoint.applied_sequence;
    switch (checkpoint.status) {
        .clean => {},
        .rebuilding => item.backfill_active = true,
        .degraded, .repair_required => item.repair_degraded = true,
    }
}

pub fn managedIndexReplayTargetSequence(
    self: *DB,
    alloc: Allocator,
    index_name: []const u8,
    kind: types.IndexKind,
    applied_sequence: u64,
) !u64 {
    return try self.probeDerivedReplayTargetSequence(
        alloc,
        self.core.replaySource(),
        .{
            .name = index_name,
            .kind = kind,
        },
        applied_sequence,
    );
}

pub fn normalizeReplayStatusFromDurableCheckpoint(item: *types.DBIndexStats) void {
    const durable_applied = item.projection_checkpoint_applied_sequence;
    if (durable_applied <= item.replay_applied_sequence) return;

    // Projection checkpoints are persisted only after the index publication
    // boundary. A worker snapshot can briefly lag that durable write while
    // it updates its in-memory sequence, but status must not expose the
    // transient ordering as durable replay debt.
    item.replay_applied_sequence = durable_applied;
    item.replay_target_sequence = @max(item.replay_target_sequence, durable_applied);
    item.catch_up_applied_sequence = @max(item.catch_up_applied_sequence, durable_applied);
    item.catch_up_target_sequence = @max(item.catch_up_target_sequence, item.replay_target_sequence);
    item.replay_catch_up_required = item.replay_applied_sequence < item.replay_target_sequence;
    if (!item.catch_up_active and !item.replay_catch_up_required) {
        item.backfill_active = durableGenerationBuildActive(item);
        if (!item.backfill_active and item.replay_target_sequence > 0) item.backfill_progress = 1.0;
    }
}

pub fn applyCachedIdentityVisibilitySummary(identity_stats: *doc_identity.Stats, summary: ?doc_identity.VisibilitySummary) void {
    const cached = summary orelse return;
    identity_stats.live_ordinals = cached.live_ordinals;
    identity_stats.tombstone_ordinals = cached.tombstone_ordinals;
    identity_stats.max_created_generation = cached.max_created_generation;
    identity_stats.min_deleted_generation = cached.min_deleted_generation;
    identity_stats.max_deleted_generation = cached.max_deleted_generation;
}

pub fn applyStatusOnlyRebuildStateStats(
    self: *DB,
    alloc: Allocator,
    cfg: types.IndexConfig,
    item: *types.DBIndexStats,
) !void {
    if (cfg.kind == .algebraic) return;
    const rebuild_root_path = try self.denseIndexRebuildStatePathAlloc(alloc, cfg.name);
    defer alloc.free(rebuild_root_path);
    const rebuild_state = self.core.index_manager.rebuildState(cfg.kind, rebuild_root_path, cfg);
    var loaded = try rebuild_state.loadWithIo(
        alloc,
        self.core.index_manager.checkpointIo(),
    );
    defer loaded.deinit(alloc);
    const key = switch (loaded) {
        .absent => return,
        .valid => |resume_key| resume_key,
        .legacy => {
            item.backfill_active = true;
            item.backfill_progress = 0;
            return;
        },
        .corrupt => {
            item.load_error = try alloc.dupe(u8, @errorName(error.InvalidRebuildState));
            item.repair_degraded = true;
            item.backfill_active = false;
            item.backfill_progress = 0;
            return;
        },
    };
    const byte_range = self.core.byteRange();
    item.backfill_active = true;
    item.backfill_progress = backfill_state_mod.estimateProgressForKey(
        byte_range.start,
        byte_range.end,
        key,
    );
}

pub fn applyResolvedFilterIncludeAlloc(
    alloc: Allocator,
    target: *doc_set.ResolvedDocFilter,
    include_filter: *const doc_set.ResolvedDocFilter,
) !bool {
    var merged_include = (try doc_set.intersectAlloc(alloc, &target.include, &include_filter.include)) orelse return false;
    errdefer merged_include.deinit(alloc);
    var merged_exclude = (try doc_set.unionAlloc(alloc, &target.exclude, &include_filter.exclude)) orelse return false;
    errdefer merged_exclude.deinit(alloc);

    target.include.deinit(alloc);
    target.exclude.deinit(alloc);
    target.include = merged_include;
    target.exclude = merged_exclude;
    return true;
}

test "db batch get match_all search and index registry" {
    const alloc = std.testing.allocator;

    const t = try TestDb.open(alloc, .{});
    defer t.deinit();
    const db = &t.db;

    try db.batch(.{
        .writes = &.{
            .{ .key = "doc:a", .value = "{\"title\":\"alpha\"}" },
            .{ .key = "doc:b", .value = "{\"title\":\"beta\"}" },
        },
    });

    const value = (try db.get(alloc, "doc:a")).?;
    defer alloc.free(value);
    try std.testing.expectEqualStrings("{\"title\":\"alpha\"}", value);

    try db.addIndex(test_index.ft_v1);
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

test "db batch projected admission includes payload and internal record overhead" {
    const writes = [_]types.BatchWrite{.{ .key = "doc:a", .value = "1234567890" }};
    const deletes = [_][]const u8{"doc:b"};
    const estimated = DB.projectedBatchLsmAdmissionBytes(.{
        .writes = writes[0..],
        .deletes = deletes[0..],
    });
    const payload_bytes = "doc:a".len + "1234567890".len + "doc:b".len;
    try std.testing.expectEqual(@as(u64, payload_bytes * 2 + 2 * 512), estimated);
}

test "db replay truncation waits for repair pins through borrowed VoprIo" {
    const vopr = vopr_mod;
    const alloc = std.testing.allocator;
    var vopr_io = try vopr.vopr_io.VoprIo.init(.{});
    defer vopr_io.deinit();
    const io = vopr_io.io();
    var runtime = try background_runtime_mod.BackendRuntimeHandle.init(alloc, .{
        .backend = .manual,
        .borrowed_io = .{ .general = io },
    });
    defer runtime.deinit();
    var db = try DB.open(alloc, "/replay-pin-lock-vopr", .{
        .backend_runtime = runtime.ptr(),
        .executor = .{ .backend = .manual },
        .primary_backend = .{ .mem = .{} },
        .physical_root_mode = .external_backend,
        .start_index_workers = false,
        .start_optional_runtimes = false,
        .ttl_cleanup = .{ .enabled = false },
    });
    defer db.close();
    // The owner is authoritative even if this operation has no Io override.
    // Do not substitute an incomplete fake vtable or switch a live mutex's Io.
    db.async_context.io = null;
    const Work = struct {
        fn run(database: *DB, asynchronous: bool, completed: *bool) !void {
            if (asynchronous) {
                try truncateReplaySequenceAsync(database.async_context, 0);
            } else {
                var ctx = database.batchContext();
                try truncateReplayJournalIfSafeContext(&ctx);
            }
            completed.* = true;
        }
    };
    var enabled: vopr.transition.List = .{};
    defer enabled.deinit(alloc);
    var events: vopr.event.Sink = .{};
    defer events.deinit(alloc);
    for ([_]bool{ false, true }) |asynchronous| {
        const mutex = db.core.repair_replay_mutex;
        mutex.lockUncancelable(io);
        var locked = true;
        var completed = false;
        var future = io.async(Work.run, .{ &db, asynchronous, &completed });
        defer {
            if (locked) mutex.unlock(io);
            _ = vopr_io.cancelAndDrainTasksForTeardown(alloc, 64) catch @panic("replay pin test cleanup failed");
            _ = future.cancel(io) catch {};
        }
        const scheduler = vopr_io.scheduler();
        for (0..16) |_| {
            if (vopr_io.futureTaskSnapshot(future.any_future.?).?.waiting_on_futex) break;
            enabled.items.clearRetainingCapacity();
            try scheduler.enumerateReady(&enabled, alloc);
            try enabled.canonicalize();
            try std.testing.expect(enabled.items.items.len != 0);
            try scheduler.executeReady(enabled.items.items[0].id, &events, alloc);
        }
        try std.testing.expect(!completed);
        try std.testing.expect(vopr_io.futureTaskSnapshot(future.any_future.?).?.waiting_on_futex);
        mutex.unlock(io);
        locked = false;
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
    }
    try vopr_io.ensureNoCapabilityViolation();
}

test "db batch treats reserved namespace bytes as user document ids" {
    const alloc = std.testing.allocator;

    const t = try TestDb.open(alloc, .{
        .primary_backend = .{ .mem = .{} },
    });
    defer t.deinit();
    const db = &t.db;

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

    var resolved = try db.resolveDocSetForIdsAlloc(alloc, &.{raw_id});
    defer resolved.deinit(alloc);
    switch (resolved) {
        .ordinals => |ordinals| {
            try std.testing.expectEqual(@as(usize, 1), ordinals.len);
            try std.testing.expectEqual(@as(doc_set.DocOrdinal, 1), ordinals[0]);
        },
        else => return error.ExpectedOrdinalDocSet,
    }

    const resolved_doc_ids = (try db.docIdsForResolvedDocSetAlloc(alloc, &resolved)).?;
    defer {
        for (resolved_doc_ids) |doc_id| alloc.free(@constCast(doc_id));
        alloc.free(resolved_doc_ids);
    }
    try std.testing.expectEqual(@as(usize, 1), resolved_doc_ids.len);
    try std.testing.expectEqualSlices(u8, raw_id, resolved_doc_ids[0]);
}

test "db batch and scan round trip adversarial document ids" {
    const alloc = std.testing.allocator;

    var path_tmp = try TestDirectory.init("db");
    defer path_tmp.cleanup();
    const path = path_tmp.path().ptr;

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

    var resolved = try db.resolveDocSetForIdsAlloc(alloc, raw_ids[0..]);
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

    const resolved_doc_ids = (try db.docIdsForResolvedDocSetAlloc(alloc, &resolved)).?;
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

test "db batch appends only thin replay stream records" {
    const alloc = std.testing.allocator;

    const t = try TestDb.open(alloc, .{});
    defer t.deinit();
    const db = &t.db;

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

    const t = try TestDb.open(alloc, .{});
    defer t.deinit();
    const db = &t.db;

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

test "db batch delete replay record wakes managed index workers" {
    const alloc = std.testing.allocator;

    const t = try TestDb.open(alloc, .{
        .start_index_workers = false,
    });
    defer t.deinit();
    const db = &t.db;

    try db.addIndex(.{
        .name = "semantic_idx",
        .kind = .dense_vector,
        .config_json = "{\"field\":\"embedding\",\"dims\":3}",
    });

    try db.batch(.{
        .writes = &.{.{ .key = "doc:a", .value = "{\"title\":\"alpha\"}" }},
        .sync_level = .write,
    });
    try db.batch(.{
        .deletes = &.{"doc:a"},
        .sync_level = .write,
    });

    const entries = try replay_stream_mod.iterateFrom(alloc, db.core.store, 1);
    defer {
        for (entries) |*entry| entry.deinit(alloc);
        alloc.free(entries);
    }

    try std.testing.expectEqual(@as(usize, 2), entries.len);
    var record = try change_journal_mod.decodeRecord(alloc, entries[1].payload);
    defer record.deinit();

    try std.testing.expectEqual(@as(u64, 2), record.record.sequence);
    try std.testing.expectEqual(@as(usize, 1), record.record.deleted_doc_keys.len);
    try std.testing.expectEqualStrings("doc:a", record.record.deleted_doc_keys[0]);
    try std.testing.expect(journalRecordHasHint(record.record, .full_text));
    try std.testing.expect(journalRecordHasHint(record.record, .dense_vector));
    try std.testing.expect(journalRecordHasHint(record.record, .sparse_vector));
    try std.testing.expect(journalRecordHasHint(record.record, .algebraic));
    try std.testing.expect(journalRecordHasHint(record.record, .graph));
}

test "db batch uses change journal as the replay authority" {
    const alloc = std.testing.allocator;

    const t = try TestDb.open(alloc, .{});
    defer t.deinit();
    const db = &t.db;

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

test "db direct graph writes record graph artifacts in the replay stream instead of graph payload replay" {
    const alloc = std.testing.allocator;

    const t = try TestDb.open(alloc, .{});
    defer t.deinit();
    const db = &t.db;

    try db.addIndex(test_index.gr_v1);

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

test "db direct generated chunks feed multi-source text and graph indexes" {
    const alloc = std.testing.allocator;

    const t = try TestDb.open(alloc, .{
        .enrichment = .{
            .owner_id = "worker-a",
            .enable_without_producers = true,
        },
    });
    defer t.deinit();
    const db = &t.db;

    const ephemeral_chunker =
        "{\"provider\":\"antfly\",\"store_chunks\":false,\"text\":{\"target_tokens\":32,\"overlap_tokens\":0}}";
    try db.addEnrichment(.{
        .name = "search_chunks_v1",
        .kind = .chunk,
        .field = "body",
        .chunker_json = ephemeral_chunker,
    });
    try db.addEnrichment(.{
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

    const graph_chunk_key = try internal_keys.chunkArtifactKeyAlloc(alloc, "doc:a", "graph_chunks_v1", 0);
    defer alloc.free(graph_chunk_key);
    const graph_chunk = try db.core.store.get(alloc, graph_chunk_key);
    defer alloc.free(graph_chunk);

    const edges = try db.getEdges(alloc, "selected_graph", "doc:a", "mentions", .out);
    defer graph_mod.GraphIndex.freeEdges(alloc, edges);
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
    defer graph_mod.GraphIndex.freeEdges(alloc, after_delete);
    try std.testing.expectEqual(@as(usize, 0), after_delete.len);
}

test "db batch marks generated enrichment replay for generator-enabled dense index" {
    const alloc = std.testing.allocator;

    const t = try TestDb.open(alloc, .{});
    defer t.deinit();
    const db = &t.db;

    try db.addIndex(.{
        .name = "dv_v1",
        .kind = .dense_vector,
        .config_json = "{\"field\":\"embedding\",\"dims\":3,\"generator\":{\"kind\":\"dense_embedding\",\"source_field\":\"body\",\"chunk_size\":256,\"chunk_overlap\":32}}",
    });

    try db.batch(.{
        .writes = &.{
            .{ .key = "doc:a", .value = "{\"title\":\"alpha\",\"body\":\"needs generated embedding\"}" },
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

    try std.testing.expect(journal_record.record.changed_doc_keys.len >= 1);
    try std.testing.expectEqualStrings("doc:a", journal_record.record.changed_doc_keys[0]);
    try std.testing.expect(journalRecordHasHint(journal_record.record, .enrichment));
    try std.testing.expect(!journalRecordHasHint(journal_record.record, .dense_vector));
}

test "db batch persists per-index applied sequence watermark" {
    const alloc = std.testing.allocator;

    const t = try TestDb.open(alloc, .{});
    defer t.deinit();
    const db = &t.db;

    try db.addIndex(test_index.ft_v1);

    try db.batch(.{
        .writes = &.{
            .{ .key = "doc:a", .value = "{\"title\":\"alpha\"}" },
        },
        .sync_level = .full_index,
    });
    const applied = try db.core.loadAppliedSequence(alloc, "ft_v1");
    try std.testing.expect(applied > 0);
    const index_lsm = db.core.index_manager.snapshotLsmMaintenanceStats();
    try std.testing.expectEqual(@as(u64, 0), index_lsm.mutable_entries);
    try std.testing.expectEqual(@as(u64, 0), index_lsm.immutable_memtables);
    try std.testing.expect(index_lsm.wal_checkpoint_current_segment > 0);
    try std.testing.expectEqual(@as(u64, 0), index_lsm.wal_checkpoint_lag_segments);

    const owner_stats = (try db.trySnapshotLsmOwnerStatsAlloc(alloc)) orelse
        return error.UnexpectedLsmOwnerStatsContention;
    defer {
        for (owner_stats) |*owner| owner.deinit(alloc);
        alloc.free(owner_stats);
    }
    try std.testing.expectEqual(@as(usize, 2), owner_stats.len);
    try std.testing.expectEqual(DB.LsmOwnerKind.primary, owner_stats[0].kind);
    try std.testing.expectEqualStrings("primary", owner_stats[0].name);
    try std.testing.expectEqual(DB.LsmOwnerKind.full_text, owner_stats[1].kind);
    try std.testing.expectEqualStrings("ft_v1", owner_stats[1].name);
}

test "db batch truncates replay logs after managed indexes catch up" {
    const alloc = std.testing.allocator;

    const t = try TestDb.open(alloc, .{});
    defer t.deinit();
    const db = &t.db;

    try db.addIndex(test_index.ft_v1);

    try db.batch(.{
        .writes = &.{
            .{ .key = "doc:a", .value = "{\"title\":\"alpha\"}" },
        },
    });

    _ = try waitForAppliedSequenceAdvance(alloc, db, "ft_v1", 0);

    var remaining_journal_entries: usize = std.math.maxInt(usize);
    var attempts: usize = 0;
    while (attempts < default_test_wait_attempts) : (attempts += 1) {
        const journal_entries = try db.core.store.iterateReplayFrom(alloc, 1);
        defer {
            for (journal_entries) |*entry| entry.deinit(alloc);
            alloc.free(journal_entries);
        }
        remaining_journal_entries = journal_entries.len;
        if (remaining_journal_entries == 0) break;
        sleepPollInterval();
    }
    try std.testing.expectEqual(@as(usize, 0), remaining_journal_entries);
}

test "db replay respects per-index applied watermarks" {
    const alloc = std.testing.allocator;

    var path_tmp = try TestDirectory.init("db");
    defer path_tmp.cleanup();
    const path = path_tmp.path().ptr;

    var appended_sequence: u64 = 0;

    {
        var db = try DB.open(alloc, std.mem.span(path), .{});
        defer db.close();

        try db.addIndex(test_index.dv_v1_dims2);
        try db.addIndex(.{
            .name = "gr_v1",
            .kind = .graph,
            .config_json = "{\"edge_types\":[{\"name\":\"related\"}]}",
        });

        const req = types.BatchRequest{
            .writes = &.{
                .{ .key = "doc:a", .value = "{\"title\":\"alpha\",\"_embeddings\":{\"dv_v1\":[1,0]}}" },
            },
        };

        var extracted = try mapper.extractWrite(alloc, req.writes[0].key, req.writes[0].value);
        defer extracted.deinit(alloc);

        const stored_key = try internal_keys.documentKeyAlloc(alloc, req.writes[0].key);
        defer alloc.free(stored_key);
        try db.core.store.putBatch(&.{
            .{ .key = stored_key, .value = extracted.cleaned_value.? },
        }, &.{});
        const artifact_key = try expectedDocumentEmbeddingArtifactKeyAlloc(alloc, "doc:a", "dv_v1");
        defer alloc.free(artifact_key);
        try putDenseEmbeddingArtifactForTest(&db, alloc, artifact_key, null, &[_]f32{ 1, 0 });
        extracted.dense_embeddings[0].artifact_key = try alloc.dupe(u8, artifact_key);

        var derived_batch = try buildDerivedBatch(alloc, req, &.{extracted}, &.{}, &.{});
        defer derived_types.deinitDerivedBatch(alloc, &derived_batch);

        appended_sequence = try appendDerivedBatchRecord(&db, derived_batch);

        try db.core.saveAppliedSequence("dv_v1", appended_sequence);
    }

    var reopened = try DB.open(alloc, std.mem.span(path), .{ .start_index_workers = false });
    defer reopened.close();

    var result = try reopened.search(alloc, .{
        .index_name = "dv_v1",
        .dense = .{
            .vector = &[_]f32{ 1, 0 },
            .k = 1,
        },
        .limit = 1,
    });
    defer result.deinit();

    try std.testing.expectEqual(@as(u32, 0), result.total_hits);

    const dense_applied = try reopened.core.loadAppliedSequence(alloc, "dv_v1");
    try std.testing.expectEqual(appended_sequence, dense_applied);

    const graph_applied = try reopened.core.loadAppliedSequence(alloc, "gr_v1");
    try std.testing.expectEqual(@as(u64, 0), graph_applied);
}

test "db replay applies dense embeddings from artifact payloads" {
    const alloc = std.testing.allocator;

    var path_tmp = try TestDirectory.init("db");
    defer path_tmp.cleanup();
    const path = path_tmp.path().ptr;

    {
        var db = try DB.open(alloc, std.mem.span(path), .{});
        defer db.close();

        try db.addIndex(test_index.dv_v1_dims2);

        const stored_key = try internal_keys.documentKeyAlloc(alloc, "doc:a");
        defer alloc.free(stored_key);
        try db.core.store.put(stored_key, "{\"title\":\"alpha\"}");

        const artifact_key = try expectedDocumentEmbeddingArtifactKeyAlloc(alloc, "doc:a", "dv_v1");
        defer alloc.free(artifact_key);
        try putDenseEmbeddingArtifactForTest(&db, alloc, artifact_key, null, &[_]f32{ 0, 1 });

        const derived_batch = derived_types.DerivedBatch{
            .dense_embeddings = &.{
                .{
                    .index_name = "dv_v1",
                    .doc_key = "doc:a",
                    .artifact_key = artifact_key,
                    .vector = &[_]f32{ 1, 0 },
                },
            },
        };

        _ = try appendDerivedBatchRecord(&db, derived_batch);
    }

    var reopened = try DB.open(alloc, std.mem.span(path), .{});
    defer reopened.close();

    var result = try reopened.search(alloc, .{
        .index_name = "dv_v1",
        .dense = .{
            .vector = &[_]f32{ 0, 1 },
            .k = 1,
        },
        .limit = 1,
    });
    defer result.deinit();

    try std.testing.expectEqual(@as(u32, 1), result.total_hits);
    try std.testing.expectEqualStrings("doc:a", result.hits[0].id);
}

test "db replay blocks dense embedding writes when artifact payload is missing" {
    const alloc = std.testing.allocator;

    var path_tmp = try TestDirectory.init("db");
    defer path_tmp.cleanup();
    const path = path_tmp.path().ptr;

    var appended_sequence: u64 = 0;
    {
        var db = try DB.open(alloc, std.mem.span(path), .{});
        defer db.close();

        try db.addIndex(test_index.dv_v1_dims2);

        const stored_key = try internal_keys.documentKeyAlloc(alloc, "doc:a");
        defer alloc.free(stored_key);
        try db.core.store.put(stored_key, "{\"title\":\"alpha\"}");

        const artifact_key = try expectedDocumentEmbeddingArtifactKeyAlloc(alloc, "doc:a", "dv_v1");
        defer alloc.free(artifact_key);

        const derived_batch = derived_types.DerivedBatch{
            .dense_embeddings = &.{
                .{
                    .index_name = "dv_v1",
                    .doc_key = "doc:a",
                    .artifact_key = artifact_key,
                    .vector = &[_]f32{ 1, 0 },
                },
            },
        };

        appended_sequence = try appendDerivedBatchRecord(&db, derived_batch);
    }

    var reopened = try DB.open(alloc, std.mem.span(path), no_background_test_open);
    defer reopened.close();

    const issues = try reopened.listEmbeddingArtifactRepairIssues(alloc, "dv_v1", 0);
    defer types.freeEmbeddingArtifactRepairIssues(alloc, issues);
    try std.testing.expectEqual(@as(usize, 1), issues.len);
    try std.testing.expectEqual(.missing_embedding_artifact, issues[0].reason);
    try std.testing.expectEqual(.embedding, issues[0].artifact_kind);
    try std.testing.expectEqualStrings("doc:a", issues[0].doc_key);
    try std.testing.expectEqualStrings("dv_v1", issues[0].artifact_name);

    const degraded_stats = try reopened.stats(alloc);
    defer types.freeDBStats(alloc, degraded_stats);
    try std.testing.expect(degraded_stats.repair_degraded);
    try std.testing.expectEqual(@as(u64, 1), degraded_stats.repair_issue_count);
    try std.testing.expect(degraded_stats.indexes[0].repair_degraded);
    try std.testing.expectEqual(@as(u64, 1), degraded_stats.indexes[0].repair_issue_count);

    var result = try reopened.search(alloc, .{
        .index_name = "dv_v1",
        .dense = .{
            .vector = &[_]f32{ 1, 0 },
            .k = 1,
        },
        .limit = 1,
    });
    defer result.deinit();

    try std.testing.expectEqual(@as(u32, 0), result.total_hits);

    const dense_applied = try reopened.core.loadAppliedSequence(alloc, "dv_v1");
    try std.testing.expectEqual(@as(u64, 0), dense_applied);
    try std.testing.expect(appended_sequence > dense_applied);
}

test "db replay skips a missing dense artifact after its source document was deleted" {
    const alloc = std.testing.allocator;

    var path_tmp = try TestDirectory.init("db");
    defer path_tmp.cleanup();
    const path = path_tmp.path().ptr;

    var appended_sequence: u64 = 0;
    {
        var db = try DB.open(alloc, std.mem.span(path), .{});
        defer db.close();

        try db.addIndex(test_index.dv_v1_dims2);

        const artifact_key = try expectedDocumentEmbeddingArtifactKeyAlloc(alloc, "doc:deleted", "dv_v1");
        defer alloc.free(artifact_key);
        appended_sequence = try appendDerivedBatchRecord(&db, .{
            .dense_embeddings = &.{.{
                .index_name = "dv_v1",
                .doc_key = "doc:deleted",
                .artifact_key = artifact_key,
                .vector = &[_]f32{ 1, 0 },
            }},
        });
    }

    var reopened = try DB.open(alloc, std.mem.span(path), no_background_test_open);
    defer reopened.close();

    const issues = try reopened.listEmbeddingArtifactRepairIssues(alloc, "dv_v1", 0);
    defer types.freeEmbeddingArtifactRepairIssues(alloc, issues);
    try std.testing.expectEqual(@as(usize, 0), issues.len);
    try std.testing.expectEqual(appended_sequence, try reopened.core.loadAppliedSequence(alloc, "dv_v1"));
}

test "db replay blocks and preserves corrupt dense embedding artifacts" {
    const alloc = std.testing.allocator;

    var path_tmp = try TestDirectory.init("db");
    defer path_tmp.cleanup();
    const path = path_tmp.path().ptr;

    var appended_sequence: u64 = 0;
    {
        var db = try DB.open(alloc, std.mem.span(path), .{});
        defer db.close();

        try db.addIndex(test_index.dv_v1_dims2);

        const stored_key = try internal_keys.documentKeyAlloc(alloc, "doc:a");
        defer alloc.free(stored_key);
        try db.core.store.put(stored_key, "{\"title\":\"alpha\"}");

        const artifact_key = try expectedDocumentEmbeddingArtifactKeyAlloc(alloc, "doc:a", "dv_v1");
        defer alloc.free(artifact_key);
        try db.core.store.put(artifact_key, "bad-artifact");

        const derived_batch = derived_types.DerivedBatch{
            .dense_embeddings = &.{
                .{
                    .index_name = "dv_v1",
                    .doc_key = "doc:a",
                    .artifact_key = artifact_key,
                    .vector = &[_]f32{ 1, 0 },
                },
            },
        };

        appended_sequence = try appendDerivedBatchRecord(&db, derived_batch);
    }

    var reopened = try DB.open(alloc, std.mem.span(path), no_background_test_open);
    defer reopened.close();

    const issues = try reopened.listEmbeddingArtifactRepairIssues(alloc, "dv_v1", 0);
    defer types.freeEmbeddingArtifactRepairIssues(alloc, issues);
    try std.testing.expectEqual(@as(usize, 1), issues.len);
    try std.testing.expectEqual(.corrupt_embedding_artifact, issues[0].reason);
    try std.testing.expectEqual(.embedding, issues[0].artifact_kind);
    try std.testing.expectEqualStrings("doc:a", issues[0].doc_key);
    try std.testing.expectEqualStrings("dv_v1", issues[0].artifact_name);
    const repair_id = (try reopened.indexRepairIdForIndex(alloc, "dv_v1")) orelse
        return error.TestUnexpectedResult;
    var replay_repair = try reopened.loadIndexRepairEntryById(alloc, repair_id);
    defer replay_repair.deinit(alloc);
    try std.testing.expectEqual(
        index_repair_state.Trigger.replay_artifact_unavailable,
        replay_repair.intent.trigger,
    );
    try std.testing.expect(!reopened.core.index_manager.repairUnavailable("dv_v1"));

    // A terminal replacement failure is actionable completeness debt, but it
    // cannot revoke the explicitly retained active generation. Status and the
    // API query coordinator consume this same scoped serviceability proof.
    try reopened.updateIndexRepairIntent(alloc, repair_id, .{
        .phase = .terminal,
        .last_error = "IndexRepairConfigurationChanged",
        .replace_last_error = true,
    });
    const terminal_stats = try reopened.stats(alloc);
    defer types.freeDBStats(alloc, terminal_stats);
    var observed_terminal_repair = false;
    for (terminal_stats.indexes) |index_stats| {
        if (!std.mem.eql(u8, index_stats.name, "dv_v1")) continue;
        observed_terminal_repair = true;
        try std.testing.expectEqual(types.IndexRepairStatus.failed, index_stats.index_repair_status.?);
        try std.testing.expect(index_stats.index_repair_action_required);
        try std.testing.expect(index_stats.index_repair_active_generation_serviceable);
        try std.testing.expect(index_stats.serving_snapshot_ready);
    }
    try std.testing.expect(observed_terminal_repair);
    try std.testing.expect(!reopened.core.index_manager.repairUnavailable("dv_v1"));

    var result = try reopened.search(alloc, .{
        .index_name = "dv_v1",
        .dense = .{
            .vector = &[_]f32{ 1, 0 },
            .k = 1,
        },
        .limit = 1,
    });
    defer result.deinit();

    try std.testing.expectEqual(@as(u32, 0), result.total_hits);

    const artifact_key = try expectedDocumentEmbeddingArtifactKeyAlloc(alloc, "doc:a", "dv_v1");
    defer alloc.free(artifact_key);
    const artifact_value = try reopened.core.store.get(alloc, artifact_key);
    defer alloc.free(artifact_value);
    try std.testing.expectEqualStrings("bad-artifact", artifact_value);

    const dense_applied = try reopened.core.loadAppliedSequence(alloc, "dv_v1");
    try std.testing.expectEqual(@as(u64, 0), dense_applied);
    try std.testing.expect(appended_sequence > dense_applied);

    reopened.runDerivedUntil(appended_sequence) catch |err| switch (err) {
        error.ArtifactRepairRequired => {},
        else => return err,
    };
    const issues_after_replay = try reopened.listEmbeddingArtifactRepairIssues(alloc, "dv_v1", 0);
    defer types.freeEmbeddingArtifactRepairIssues(alloc, issues_after_replay);
    try std.testing.expectEqual(@as(usize, 1), issues_after_replay.len);
    try std.testing.expectEqual(issues[0].first_seen_ns, issues_after_replay[0].first_seen_ns);
    try std.testing.expectEqual(issues[0].attempts, issues_after_replay[0].attempts);
    try std.testing.expectEqualStrings(issues[0].last_error, issues_after_replay[0].last_error);

    const repair = try reopened.repairEmbeddingArtifactIssues(alloc, 10);
    try std.testing.expectEqual(@as(u64, 1), repair.scanned);
    try std.testing.expectEqual(@as(u64, 0), repair.repaired);
    try std.testing.expectEqual(@as(u64, 1), repair.failed);
    const issues_after_repair = try reopened.listEmbeddingArtifactRepairIssues(alloc, "dv_v1", 0);
    defer types.freeEmbeddingArtifactRepairIssues(alloc, issues_after_repair);
    try std.testing.expectEqual(@as(usize, 1), issues_after_repair.len);
    try std.testing.expectEqual(@as(u64, 1), issues_after_repair[0].attempts);
    try std.testing.expectEqualStrings("embedding_enrichment_unavailable", issues_after_repair[0].last_error);
}

test "db replay records wrong-dimension dense embedding artifact as repair debt" {
    const alloc = std.testing.allocator;

    var path_tmp = try TestDirectory.init("db");
    defer path_tmp.cleanup();
    const path = path_tmp.path().ptr;

    var appended_sequence: u64 = 0;
    {
        var db = try DB.open(alloc, std.mem.span(path), .{});
        defer db.close();

        try db.addIndex(test_index.dv_v1_dims2);

        const stored_key = try internal_keys.documentKeyAlloc(alloc, "doc:a");
        defer alloc.free(stored_key);
        try db.core.store.put(stored_key, "{\"title\":\"alpha\"}");

        const artifact_key = try expectedDocumentEmbeddingArtifactKeyAlloc(alloc, "doc:a", "dv_v1");
        defer alloc.free(artifact_key);
        try putDenseEmbeddingArtifactForTest(&db, alloc, artifact_key, null, &[_]f32{ 1, 0, 0 });

        const derived_batch = derived_types.DerivedBatch{
            .dense_embeddings = &.{
                .{
                    .index_name = "dv_v1",
                    .doc_key = "doc:a",
                    .artifact_key = artifact_key,
                    .vector = &[_]f32{ 1, 0 },
                },
            },
        };
        appended_sequence = try appendDerivedBatchRecord(&db, derived_batch);
    }

    var reopened = try DB.open(alloc, std.mem.span(path), no_background_test_open);
    defer reopened.close();

    const issues = try reopened.listEmbeddingArtifactRepairIssues(alloc, "dv_v1", 0);
    defer types.freeEmbeddingArtifactRepairIssues(alloc, issues);
    try std.testing.expectEqual(@as(usize, 1), issues.len);
    try std.testing.expectEqual(.corrupt_embedding_artifact, issues[0].reason);
    try std.testing.expectEqualStrings("doc:a", issues[0].doc_key);
    try std.testing.expectEqualStrings("dv_v1", issues[0].artifact_name);

    const degraded_stats = try reopened.stats(alloc);
    defer types.freeDBStats(alloc, degraded_stats);
    try std.testing.expect(degraded_stats.repair_degraded);
    try std.testing.expectEqual(@as(u64, 1), degraded_stats.repair_issue_count);
    try std.testing.expect(degraded_stats.indexes[0].repair_degraded);
    try std.testing.expectEqual(@as(u64, 1), degraded_stats.indexes[0].repair_issue_count);

    const dense_applied = try reopened.core.loadAppliedSequence(alloc, "dv_v1");
    try std.testing.expectEqual(@as(u64, 0), dense_applied);
    try std.testing.expect(appended_sequence > dense_applied);
}

test "db replay applies sparse embeddings from artifact payloads" {
    const alloc = std.testing.allocator;

    var path_tmp = try TestDirectory.init("db");
    defer path_tmp.cleanup();
    const path = path_tmp.path().ptr;

    {
        var db = try DB.open(alloc, std.mem.span(path), .{});
        defer db.close();

        try db.addIndex(.{
            .name = "sp_v1",
            .kind = .sparse_vector,
            .config_json = "{\"field\":\"sparse_embedding\"}",
        });

        const stored_key = try internal_keys.documentKeyAlloc(alloc, "doc:a");
        defer alloc.free(stored_key);
        try db.core.store.put(stored_key, "{\"title\":\"alpha\"}");

        const artifact_key = try expectedDocumentEmbeddingArtifactKeyAlloc(alloc, "doc:a", "sp_v1");
        defer alloc.free(artifact_key);
        try putSparseEmbeddingArtifactForTest(&db, alloc, artifact_key, null, &.{ 1, 5 }, &.{ 0.5, 0.75 });

        const derived_batch = derived_types.DerivedBatch{
            .sparse_embeddings = &.{
                .{
                    .index_name = "sp_v1",
                    .doc_key = "doc:a",
                    .artifact_key = artifact_key,
                    .indices = &.{9},
                    .values = &.{9.0},
                },
            },
        };

        _ = try appendDerivedBatchRecord(&db, derived_batch);
    }

    var reopened = try DB.open(alloc, std.mem.span(path), .{});
    defer reopened.close();

    var sparse_index = reopened.core.index_manager.sparseIndex("sp_v1").?.index;
    const stats = sparse_index.stats();
    try std.testing.expectEqual(@as(u64, 1), stats.doc_count);
}

test "db replay skips a missing sparse artifact after its source document was deleted" {
    const alloc = std.testing.allocator;

    var path_tmp = try TestDirectory.init("db");
    defer path_tmp.cleanup();
    const path = path_tmp.path().ptr;

    var appended_sequence: u64 = 0;
    {
        var db = try DB.open(alloc, std.mem.span(path), .{});
        defer db.close();

        try db.addIndex(.{
            .name = "sp_v1",
            .kind = .sparse_vector,
            .config_json = "{\"field\":\"sparse_embedding\"}",
        });

        const artifact_key = try expectedDocumentEmbeddingArtifactKeyAlloc(alloc, "doc:deleted", "sp_v1");
        defer alloc.free(artifact_key);
        appended_sequence = try appendDerivedBatchRecord(&db, .{
            .sparse_embeddings = &.{.{
                .index_name = "sp_v1",
                .doc_key = "doc:deleted",
                .artifact_key = artifact_key,
                .indices = &.{1},
                .values = &.{1.0},
            }},
        });
    }

    var reopened = try DB.open(alloc, std.mem.span(path), no_background_test_open);
    defer reopened.close();

    const issues = try reopened.listEmbeddingArtifactRepairIssues(alloc, "sp_v1", 0);
    defer types.freeEmbeddingArtifactRepairIssues(alloc, issues);
    try std.testing.expectEqual(@as(usize, 0), issues.len);
    try std.testing.expectEqual(appended_sequence, try reopened.core.loadAppliedSequence(alloc, "sp_v1"));
    try std.testing.expectEqual(@as(u64, 0), reopened.core.index_manager.sparseIndex("sp_v1").?.index.stats().doc_count);
}

test "db replay blocks and preserves corrupt sparse embedding artifacts" {
    const alloc = std.testing.allocator;

    var path_tmp = try TestDirectory.init("db");
    defer path_tmp.cleanup();
    const path = path_tmp.path().ptr;

    var appended_sequence: u64 = 0;
    {
        var db = try DB.open(alloc, std.mem.span(path), .{});
        defer db.close();

        try db.addIndex(.{
            .name = "sp_v1",
            .kind = .sparse_vector,
            .config_json = "{\"field\":\"sparse_embedding\"}",
        });

        const stored_key = try internal_keys.documentKeyAlloc(alloc, "doc:a");
        defer alloc.free(stored_key);
        try db.core.store.put(stored_key, "{\"title\":\"alpha\"}");

        const artifact_key = try expectedDocumentEmbeddingArtifactKeyAlloc(alloc, "doc:a", "sp_v1");
        defer alloc.free(artifact_key);
        try db.core.store.put(artifact_key, "bad-artifact");

        const derived_batch = derived_types.DerivedBatch{
            .sparse_embeddings = &.{
                .{
                    .index_name = "sp_v1",
                    .doc_key = "doc:a",
                    .artifact_key = artifact_key,
                    .indices = &.{9},
                    .values = &.{9.0},
                },
            },
        };

        appended_sequence = try appendDerivedBatchRecord(&db, derived_batch);
    }

    var reopened = try DB.open(alloc, std.mem.span(path), no_background_test_open);
    defer reopened.close();

    const issues = try reopened.listEmbeddingArtifactRepairIssues(alloc, "sp_v1", 0);
    defer types.freeEmbeddingArtifactRepairIssues(alloc, issues);
    try std.testing.expectEqual(@as(usize, 1), issues.len);
    try std.testing.expectEqual(.corrupt_embedding_artifact, issues[0].reason);
    try std.testing.expectEqual(.embedding, issues[0].artifact_kind);
    try std.testing.expectEqualStrings("doc:a", issues[0].doc_key);
    try std.testing.expectEqualStrings("sp_v1", issues[0].artifact_name);

    var sparse_index = reopened.core.index_manager.sparseIndex("sp_v1").?.index;
    const stats = sparse_index.stats();
    try std.testing.expectEqual(@as(u64, 0), stats.doc_count);

    const artifact_key = try expectedDocumentEmbeddingArtifactKeyAlloc(alloc, "doc:a", "sp_v1");
    defer alloc.free(artifact_key);
    const artifact_value = try reopened.core.store.get(alloc, artifact_key);
    defer alloc.free(artifact_value);
    try std.testing.expectEqualStrings("bad-artifact", artifact_value);

    const sparse_applied = try reopened.core.loadAppliedSequence(alloc, "sp_v1");
    try std.testing.expectEqual(@as(u64, 0), sparse_applied);
    try std.testing.expect(appended_sequence > sparse_applied);
}

test "db direct graph writes persist graph artifacts and deletes remove them" {
    const alloc = std.testing.allocator;

    const t = try TestDb.open(alloc, .{});
    defer t.deinit();
    const db = &t.db;

    try db.batch(.{
        .writes = &.{
            .{ .key = "doc:a", .value = "{\"title\":\"alpha\"}" },
            .{ .key = "doc:b", .value = "{\"title\":\"beta\"}" },
        },
    });

    try db.addIndex(test_index.gr_v1);

    try db.batch(.{
        .graph_writes = &.{
            .{ .index_name = "gr_v1", .source = "doc:a", .target = "doc:b", .edge_type = "links", .weight = 1.25, .metadata_json = "{\"kind\":\"cite\"}" },
        },
        .sync_level = .full_index,
    });

    const prefix = try internal_keys.graphArtifactIndexPrefixAlloc(alloc, "doc:a", "gr_v1");
    defer alloc.free(prefix);
    const after_write = try db.core.scanStorePrefix(alloc, prefix);
    defer docstore_mod.DocStore.freeResults(alloc, after_write);
    try std.testing.expectEqual(@as(usize, 1), after_write.len);
    try std.testing.expect(internal_keys.isGraphEdgeArtifactKey(after_write[0].key));
    var decoded = try enrichment_artifact_codec.decodeGraphEdgeAlloc(alloc, after_write[0].value);
    defer decoded.deinit(alloc);
    try std.testing.expectEqual(@as(f64, 1.25), decoded.weight);
    try std.testing.expectEqualStrings("{\"kind\":\"cite\"}", decoded.metadata_json);

    try db.batch(.{
        .graph_deletes = &.{
            .{ .index_name = "gr_v1", .source = "doc:a", .target = "doc:b", .edge_type = "links" },
        },
        .sync_level = .full_index,
    });

    const after_delete = try db.core.scanStorePrefix(alloc, prefix);
    defer docstore_mod.DocStore.freeResults(alloc, after_delete);
    try std.testing.expectEqual(@as(usize, 0), after_delete.len);
}

test "db batch resolves transforms against pending same-batch writes" {
    const alloc = std.testing.allocator;

    const t = try TestDb.open(alloc, .{});
    defer t.deinit();
    const db = &t.db;

    try db.batch(.{
        .writes = &.{
            .{ .key = "doc:coalesce", .value = "{\"count\":1,\"tags\":[\"db\"]}" },
        },
        .transforms = &.{
            .{
                .key = "doc:coalesce",
                .operations = &.{
                    .{ .op = .inc, .path = "count", .value_json = "2" },
                    .{ .op = .add_to_set, .path = "tags", .value_json = "\"zig\"" },
                },
            },
        },
    });

    const raw = (try db.get(alloc, "doc:coalesce")) orelse return error.TestExpectedEqual;
    defer alloc.free(raw);

    var parsed = try std.json.parseFromSlice(std.json.Value, alloc, raw, .{});
    defer parsed.deinit();
    const count_value = parsed.value.object.get("count").?;
    switch (count_value) {
        .integer => |value| try std.testing.expectEqual(@as(i64, 3), value),
        .float => |value| try std.testing.expectEqual(@as(f64, 3), value),
        else => return error.TestExpectedEqual,
    }
    try std.testing.expectEqual(@as(usize, 2), parsed.value.object.get("tags").?.array.items.len);
}

test "db batch keeps delete when same-batch transform targets deleted key" {
    const alloc = std.testing.allocator;

    const t = try TestDb.open(alloc, .{});
    defer t.deinit();
    const db = &t.db;

    try db.batch(.{
        .writes = &.{
            .{ .key = "doc:delete_transform", .value = "{\"status\":\"old\"}" },
        },
    });

    try db.batch(.{
        .deletes = &.{"doc:delete_transform"},
        .transforms = &.{
            .{
                .key = "doc:delete_transform",
                .operations = &.{
                    .{ .op = .set, .path = "status", .value_json = "\"new\"" },
                },
            },
        },
    });

    try std.testing.expect((try db.get(alloc, "doc:delete_transform")) == null);
}

test "db batch enforces optimistic version predicates" {
    const alloc = std.testing.allocator;

    const t = try TestDb.open(alloc, .{});
    defer t.deinit();
    const db = &t.db;

    try db.batch(.{
        .writes = &.{.{ .key = "doc:cas", .value = "{\"title\":\"v1\"}" }},
        .timestamp_ns = 10_000,
    });

    try std.testing.expectError(transactions_mod.TxnError.VersionConflict, db.batch(.{
        .writes = &.{.{ .key = "doc:cas", .value = "{\"title\":\"v2\"}" }},
        .predicates = &.{.{ .key = "doc:cas", .expected_version = 0 }},
        .timestamp_ns = 11_000,
    }));

    try db.batch(.{
        .writes = &.{.{ .key = "doc:cas", .value = "{\"title\":\"v2\"}" }},
        .predicates = &.{.{ .key = "doc:cas", .expected_version = 10_000 }},
        .timestamp_ns = 11_000,
    });

    const raw = (try db.get(alloc, "doc:cas")) orelse return error.TestExpectedEqual;
    defer alloc.free(raw);
    try std.testing.expectEqualStrings("{\"title\":\"v2\"}", raw);
    try std.testing.expectEqual(@as(u64, 11_000), try db.getTimestamp(alloc, "doc:cas"));
}

test "db direct merge import rebinds graph ttl artifact and rebuilds deadline" {
    const alloc = std.testing.allocator;
    var donor_tmp = try TestDirectory.init("db-graph-ttl-donor");
    defer donor_tmp.cleanup();
    var receiver_tmp = try TestDirectory.init("db-graph-ttl-receiver");
    defer receiver_tmp.cleanup();
    var donor = try DB.open(alloc, donor_tmp.path(), .{ .start_optional_runtimes = false });
    defer donor.close();
    var receiver = try DB.open(alloc, receiver_tmp.path(), .{ .start_optional_runtimes = false });
    defer receiver.close();
    const config: types.IndexConfig = .{ .name = "direct_graph", .kind = .graph, .config_json = "{\"ttl\":{\"duration\":\"1h\"}}" };
    try donor.addIndex(config);
    try receiver.addIndex(config);
    try std.testing.expect(donor.core.index_manager.graphIndex("direct_graph").?.config.coverage_generation !=
        receiver.core.index_manager.graphIndex("direct_graph").?.config.coverage_generation);
    try donor.batch(.{ .writes = &.{.{ .key = "doc:a", .value =
        \\{"title":"owner","_edges":{"direct_graph":{"links":[{"target":"doc:b"}]}}}
    }}, .sync_level = .full_index });
    try receiver.importMergeRangeFromTransitionDonor(&donor, .{ .start = "doc:a", .end = "doc:z" });
    const due_rows = try receiver.core.store.scanPrefix(alloc, &internal_keys.graph_edge_expiration_index_prefix);
    defer docstore_mod.DocStore.freeResults(alloc, due_rows);
    try std.testing.expectEqual(@as(usize, 1), due_rows.len);
    const due = (try graph_edge_ttl_expiration.decodeDue(due_rows[0].value)).direct;
    try std.testing.expectEqual(receiver.core.index_manager.graphIndex("direct_graph").?.config.coverage_generation, due.generation);
    const raw = try receiver.core.store.get(alloc, due.artifact_key);
    defer alloc.free(raw);
    var edge = try enrichment_artifact_codec.decodeGraphEdgeAlloc(alloc, raw);
    defer edge.deinit(alloc);
    try std.testing.expectEqual(due.generation, edge.generation);
    try std.testing.expectEqual(due.deadline_ns, edge.ttl_created_ns + std.time.ns_per_hour);
}

test "db direct merge import preserves graph source lifetime across generations" {
    const alloc = std.testing.allocator;
    var donor_tmp = try TestDirectory.init("db-graph-source-direct-donor");
    defer donor_tmp.cleanup();
    var receiver_tmp = try TestDirectory.init("db-graph-source-direct-receiver");
    defer receiver_tmp.cleanup();
    var donor = try DB.open(alloc, donor_tmp.path(), .{ .start_optional_runtimes = false });
    defer donor.close();
    var receiver = try DB.open(alloc, receiver_tmp.path(), .{ .start_optional_runtimes = false });
    defer receiver.close();
    const enrichment: types.EnrichmentConfig = .{ .name = "relations_v1", .kind = .asset, .field = "relations", .content_type = "application/json" };
    const index: types.IndexConfig = .{ .name = "relations_graph", .kind = .graph, .config_json =
        \\{"ttl":{"duration":"1h"},"sources":[{"artifact":"relations_v1"}]}
    };
    try donor.addEnrichment(enrichment);
    try donor.addIndex(index);
    try receiver.addEnrichment(enrichment);
    try receiver.addIndex(index);
    try donor.batch(.{ .writes = &.{.{ .key = "doc:a", .value =
        \\{"title":"owner","relations":{"type":"mentions","target":{"document_id":"doc:b"},"weight":2}}
    }}, .sync_level = .enrichments });
    try donor.runUntilIdle();
    const primary = (try donor.get(alloc, "doc:a")) orelse return error.TestExpectedDocument;
    defer alloc.free(primary);
    try receiver.batch(.{ .writes = &.{.{ .key = "doc:a", .value = primary }}, .sync_level = .enrichments });
    const donor_due = try donor.core.store.scanPrefix(alloc, &internal_keys.graph_edge_expiration_index_prefix);
    defer docstore_mod.DocStore.freeResults(alloc, donor_due);
    try std.testing.expectEqual(@as(usize, 1), donor_due.len);
    const donor_deadline = (try graph_edge_ttl_expiration.decodeDue(donor_due[0].value)).source.deadline_ns;
    const pre_import_candidate = (try graph_edge_ttl_expiration.decodeDue(donor_due[0].value)).source;
    const donor_lifetime_key = try internal_keys.graphEdgeTtlLifetimeKeyAlloc(alloc, pre_import_candidate.edge_key, pre_import_candidate.index_name, pre_import_candidate.generation, pre_import_candidate.state_key);
    defer alloc.free(donor_lifetime_key);
    const donor_lifetime = try donor.core.store.get(alloc, donor_lifetime_key);
    defer alloc.free(donor_lifetime);
    try std.testing.expectEqual(donor_deadline - std.time.ns_per_hour, std.mem.readInt(u64, donor_lifetime[0..8], .big));
    try receiver.importMergeRangeFromTransitionDonor(&donor, .{ .start = "doc:a", .end = "doc:z" });
    const donor_after_import = try donor.core.store.scanPrefix(alloc, &internal_keys.graph_edge_expiration_index_prefix);
    defer docstore_mod.DocStore.freeResults(alloc, donor_after_import);
    try std.testing.expectEqual(@as(usize, 1), donor_after_import.len);
    try std.testing.expectEqual(donor_deadline, (try graph_edge_ttl_expiration.decodeDue(donor_after_import[0].value)).source.deadline_ns);
    const receiver_due = try receiver.core.store.scanPrefix(alloc, &internal_keys.graph_edge_expiration_index_prefix);
    defer docstore_mod.DocStore.freeResults(alloc, receiver_due);
    try std.testing.expectEqual(@as(usize, 1), receiver_due.len);
    const candidate = (try graph_edge_ttl_expiration.decodeDue(receiver_due[0].value)).source;
    const imported_lifetime_key = try internal_keys.graphEdgeTtlLifetimeKeyAlloc(alloc, candidate.edge_key, candidate.index_name, candidate.generation, candidate.state_key);
    defer alloc.free(imported_lifetime_key);
    const imported_lifetime = try receiver.core.store.get(alloc, imported_lifetime_key);
    defer alloc.free(imported_lifetime);
    try std.testing.expectEqual(donor_deadline - std.time.ns_per_hour, std.mem.readInt(u64, imported_lifetime[0..8], .big));
    try std.testing.expectEqual(donor_deadline, candidate.deadline_ns);
    try std.testing.expectEqual(receiver.core.index_manager.graphIndex("relations_graph").?.config.coverage_generation, candidate.generation);
    const donor_candidate = (try graph_edge_ttl_expiration.decodeDue(donor_due[0].value)).source;
    var manual_clock = platform_clock.ManualClock{};
    manual_clock.setRealtimeNs(donor_candidate.deadline_ns + 1);
    var ttl_ctx = TtlCleanupContext{ .batch = donor.batchContext(), .grace_period_ns = 0, .clock = manual_clock.clock() };
    try std.testing.expect(try expireGraphTtlCandidateContext(&ttl_ctx, donor_candidate));
    try receiver.importMergeRangeFromTransitionDonor(&donor, .{ .start = "doc:a", .end = "doc:z" });
    const after_expiry = try receiver.core.store.scanPrefix(alloc, &internal_keys.graph_edge_expiration_index_prefix);
    defer docstore_mod.DocStore.freeResults(alloc, after_expiry);
    try std.testing.expectEqual(@as(usize, 0), after_expiry.len);
    const donor_tombstone = try internal_keys.graphEdgeTtlTombstoneKeyAlloc(alloc, donor_candidate.edge_key, donor_candidate.index_name, donor_candidate.generation, donor_candidate.state_key);
    defer alloc.free(donor_tombstone);
    const receiver_tombstone = try internal_keys.rebindGraphEdgeTtlStateKeyGenerationAlloc(alloc, donor_tombstone, candidate.generation);
    defer alloc.free(receiver_tombstone);
    const tombstone_raw = try receiver.core.store.get(alloc, receiver_tombstone);
    defer alloc.free(tombstone_raw);
    try std.testing.expectError(error.NotFound, receiver.core.store.get(alloc, candidate.edge_key));
}

test "db direct merge import retires replaced owners and preserves shared entity contributors" {
    const alloc = std.testing.allocator;
    for ([_]bool{ false, true }) |timed| {
        for ([_]bool{ false, true }) |sourced| {
            var donor_tmp = try TestDirectory.init("db-merge-owner-donor");
            defer donor_tmp.cleanup();
            var receiver_tmp = try TestDirectory.init("db-merge-owner-receiver");
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
                        defer graph_mod.GraphIndex.freeEdges(std.testing.allocator, outgoing);
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
                    defer graph_mod.GraphIndex.freeEdges(std.testing.allocator, obsolete);
                    try std.testing.expectEqual(@as(usize, 0), obsolete.len);
                    const shared = try db.getEdges(std.testing.allocator, "g", "target/shared", "links", .in);
                    defer graph_mod.GraphIndex.freeEdges(std.testing.allocator, shared);
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
                    if (sourced) try db.addEnrichment(.{ .name = "relations_v1", .kind = .asset, .field = "relations", .content_type = "application/json" });
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
                const retained_key = try internal_keys.graphEdgeArtifactKeyWithSourceAlloc(alloc, "doc:0", "g", "links", "target/shared", "person/ada");
                defer alloc.free(retained_key);
                const retained_before = try receiver.core.store.get(alloc, retained_key);
                defer alloc.free(retained_before);
                for (0..2) |_| {
                    try receiver.importMergeRangeFromTransitionDonor(&donor, .{ .start = "doc:a", .end = "doc:z" });
                    try Check.edges(&receiver);
                    try std.testing.expectError(error.NotFound, receiver.core.store.get(alloc, graph_merge_import_recovery_key));
                    const retained_after = try receiver.core.store.get(alloc, retained_key);
                    defer alloc.free(retained_after);
                    try std.testing.expectEqualStrings(retained_before, retained_after);
                    const due = try receiver.core.store.scanPrefix(alloc, &internal_keys.graph_edge_expiration_index_prefix);
                    defer docstore_mod.DocStore.freeResults(alloc, due);
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
            var donor_tmp = try TestDirectory.init("db-merge-source-debt-donor");
            defer donor_tmp.cleanup();
            var receiver_tmp = try TestDirectory.init("db-merge-source-debt-receiver");
            defer receiver_tmp.cleanup();
            const Check = struct {
                fn edges(db: *DB) !void {
                    for ([_][]const u8{ "doc:0", "doc:1", "doc:2", "doc:a" }, [_]?[]const u8{ "target/updated", null, "target/added", "target/donor" }) |owner, expected| {
                        const out = try db.getEdges(std.testing.allocator, "g", owner, "links", .out);
                        defer graph_mod.GraphIndex.freeEdges(std.testing.allocator, out);
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
                    try db.addEnrichment(.{ .name = "relations_v1", .kind = .asset, .field = "relations", .content_type = "application/json" });
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
                    const key = try artifact_ids.internalKeyForArtifactRefAlloc(alloc, .{ .document_id = @constCast(owner), .name = @constCast("relations_v1"), .kind = .asset });
                    defer alloc.free(key);
                    if (raw) |value| try db.core.store.put(key, value) else try db.core.store.delete(key);
                    _ = try appendDerivedBatchRecord(db, .{ .changed_artifact_keys = &.{key} });
                }
                if (interrupted) {
                    db_zig.test_graph_merge_import_abort_after_primary.store(true, .monotonic);
                    defer db_zig.test_graph_merge_import_abort_after_primary.store(false, .monotonic);
                    try std.testing.expectError(error.TestInjectedGraphMergeImportFailure, receiver.importMergeRangeFromTransitionDonor(&donor, .{ .start = "doc:a", .end = "doc:z" }));
                    // Recovery must use its durable worklist even after source
                    // replay entries disappear and the donor handle is gone.
                    try receiver.core.store.truncateReplayUpTo(alloc, receiver.core.store.nextReplaySequence(1) -| 1);
                } else {
                    try receiver.importMergeRangeFromTransitionDonor(&donor, .{ .start = "doc:a", .end = "doc:z" });
                    try Check.edges(&receiver);
                    const retained = try internal_keys.graphEdgeArtifactKeyAlloc(alloc, "doc:a", "g", "links", "target/donor");
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
                const due = try reopened.core.store.scanPrefix(alloc, &internal_keys.graph_edge_expiration_index_prefix);
                defer docstore_mod.DocStore.freeResults(alloc, due);
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
        var upstream_tmp = try TestDirectory.init("db-merge-fenced-upstream");
        defer upstream_tmp.cleanup();
        var donor_tmp = try TestDirectory.init("db-merge-fenced-donor");
        defer donor_tmp.cleanup();
        var destination_tmp = try TestDirectory.init("db-merge-fenced-destination");
        defer destination_tmp.cleanup();
        var destination = try DB.open(alloc, destination_tmp.path(), .{ .start_optional_runtimes = false });
        defer destination.close();
        const Setup = struct {
            fn graph(db: *DB, has_ttl: bool) !void {
                try db.addEnrichment(.{ .name = "relations_v1", .kind = .asset, .field = "relations", .content_type = "application/json" });
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
        const moved: types.ByteRange = .{ .start = "doc:a", .end = "doc:z" };
        const exported: types.ByteRange = .{ .start = "doc:0", .end = "doc:z" };
        const updated_key = try internal_keys.graphEdgeArtifactKeyAlloc(alloc, "doc:0", "g", "links", "target/updated");
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
            const source_key = try artifact_ids.internalKeyForArtifactRefAlloc(alloc, .{ .document_id = @constCast("doc:0"), .name = @constCast("relations_v1"), .kind = .asset });
            defer alloc.free(source_key);
            try donor.core.store.put(source_key, "{\"type\":\"links\",\"target\":{\"document_id\":\"target/updated\"}}");
            _ = try appendDerivedBatchRecord(&donor, .{ .changed_artifact_keys = &.{source_key} });
            db_zig.test_graph_merge_import_abort_after_primary.store(true, .monotonic);
            defer db_zig.test_graph_merge_import_abort_after_primary.store(false, .monotonic);
            try std.testing.expectError(error.TestInjectedGraphMergeImportFailure, donor.importMergeRangeFromTransitionDonor(&upstream, moved));
            try donor.core.store.truncateReplayUpTo(alloc, donor.core.store.nextReplaySequence(1) -| 1);
            try std.testing.expectError(error.NotFound, donor.core.store.get(alloc, updated_key));
            const recovery_before = try donor.core.store.get(alloc, graph_merge_import_recovery_key);
            defer alloc.free(recovery_before);
            const record = try std.json.parseFromSlice(GraphMergeImportRecovery, alloc, recovery_before, .{ .allocate = .alloc_always });
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
            try std.testing.expectError(error.NotFound, destination.core.store.get(alloc, graph_merge_import_recovery_key));
            const recovery_after = try donor.core.store.get(alloc, graph_merge_import_recovery_key);
            defer alloc.free(recovery_after);
            try std.testing.expectEqualStrings(recovery_before, recovery_after);
            const unchanged = try destination.getEdges(alloc, "g", "doc:0", "links", .out);
            defer graph_mod.GraphIndex.freeEdges(alloc, unchanged);
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
        defer graph_mod.GraphIndex.freeEdges(alloc, updated);
        try std.testing.expectEqual(@as(usize, 1), updated.len);
        try std.testing.expectEqualStrings("target/updated", updated[0].target);
        const due = try destination.core.store.scanPrefix(alloc, &internal_keys.graph_edge_expiration_index_prefix);
        defer docstore_mod.DocStore.freeResults(alloc, due);
        try std.testing.expectEqual(@as(usize, if (timed) 1 else 0), due.len);
    }
}

test "db direct merge import supersedes queued graph replay across publication and reopen" {
    if (builtin.single_threaded) return error.SkipZigTest;
    const alloc = std.testing.allocator;
    for ([_]bool{ false, true }) |timed| {
        var donor_tmp = try TestDirectory.init("db-merge-replay-donor");
        defer donor_tmp.cleanup();
        var receiver_tmp = try TestDirectory.init("db-merge-replay-receiver");
        defer receiver_tmp.cleanup();
        const config: types.IndexConfig = .{ .name = "g", .kind = .graph, .config_json = if (timed) "{\"ttl\":{\"duration\":\"1h\"}}" else "{}" };
        const deletion: types.GraphEdgeDelete = .{ .index_name = "g", .source = "doc:a", .target = "doc:b", .edge_type = "links" };
        var old_sequence: u64 = 0;
        {
            var donor = try DB.open(alloc, donor_tmp.path(), .{ .start_optional_runtimes = false });
            defer donor.close();
            var receiver = try DB.open(alloc, receiver_tmp.path(), .{ .start_optional_runtimes = false });
            defer receiver.close();
            for ([_]*DB{ &donor, &receiver }) |db| {
                try db.addIndex(config);
                try db.batch(.{ .graph_writes = &.{.{ .index_name = "g", .source = "doc:a", .target = "doc:b", .edge_type = "links", .weight = 4 }}, .sync_level = .full_index });
            }
            try receiver.batch(.{ .graph_deletes = &.{deletion}, .sync_level = .full_index });
            old_sequence = receiver.core.store.nextReplaySequence(1) -| 1;
            try std.testing.expect(old_sequence != 0);
            const Pending = struct {
                ctx: *AsyncContext,
                batch: derived_types.DerivedBatch,
                entered: std.atomic.Value(bool) = .init(false),
                release: std.atomic.Value(bool) = .init(false),
                failure: ?anyerror = null,
                fn pause(ptr: *anyopaque) void {
                    const self: *@This() = @ptrCast(@alignCast(ptr));
                    self.entered.store(true, .release);
                    while (!self.release.load(.acquire)) platform.time.yieldNow();
                }
                fn run(self: *@This()) void {
                    _ = applyDerivedBatchToIndexAsync(self.ctx, self.batch, .{ .name = "g", .kind = .graph }, .{}) catch |err| {
                        self.failure = err;
                    };
                }
            };
            var pending = Pending{ .ctx = receiver.async_context, .batch = .{ .sequence = old_sequence, .graph_deletes = &.{deletion} } };
            db_zig.test_before_graph_replay_apply = .{ .ctx = &pending, .call = Pending.pause };
            defer db_zig.test_before_graph_replay_apply = null;
            const thread = try std.Thread.spawn(.{}, Pending.run, .{&pending});
            var joined = false;
            defer if (!joined) {
                pending.release.store(true, .release);
                thread.join();
            };
            while (!pending.entered.load(.acquire)) platform.time.yieldNow();
            try receiver.importMergeRangeFromTransitionDonor(&donor, .{ .start = "doc:a", .end = "doc:z" });
            pending.release.store(true, .release);
            thread.join();
            joined = true;
            db_zig.test_before_graph_replay_apply = null;
            if (pending.failure) |err| return err;
            // A delayed worker can also report old progress after publication.
            _ = try persistAppliedSequenceAsync(receiver.async_context, "g", old_sequence, true);
            const edges = try receiver.getEdges(alloc, "g", "doc:a", "links", .out);
            defer graph_mod.GraphIndex.freeEdges(alloc, edges);
            try std.testing.expectEqual(@as(usize, 1), edges.len);
            try std.testing.expectEqual(@as(f64, 4), edges[0].weight);
            try std.testing.expect((try receiver.core.index_manager.graphIndex("g").?.index.artifactRebuildSequence()) >= old_sequence);
        }
        var reopened = try DB.open(alloc, receiver_tmp.path(), .{ .start_optional_runtimes = false });
        defer reopened.close();
        // Snapshot coverage is durable and independent of worker watermarks.
        // Even explicitly lowering generic progress cannot authorize old work.
        try reopened.core.saveAppliedSequence("g", 0);
        _ = try applyDerivedBatchToIndexAsync(reopened.async_context, .{ .sequence = old_sequence, .graph_deletes = &.{deletion} }, .{ .name = "g", .kind = .graph }, .{});
        {
            const edges = try reopened.getEdges(alloc, "g", "doc:a", "links", .out);
            defer graph_mod.GraphIndex.freeEdges(alloc, edges);
            try std.testing.expectEqual(@as(usize, 1), edges.len);
        }
        // A legitimate mutation committed after the snapshot still applies.
        try reopened.batch(.{ .graph_deletes = &.{deletion}, .sync_level = .full_index });
        const removed = try reopened.getEdges(alloc, "g", "doc:a", "links", .out);
        defer graph_mod.GraphIndex.freeEdges(alloc, removed);
        try std.testing.expectEqual(@as(usize, 0), removed.len);
    }
}

test "db direct merge import recovers interrupted primary and projection publication before reads" {
    const alloc = std.testing.allocator;
    for ([_]bool{ false, true }) |timed| {
        const Failure = enum { primary, projection, partial_clear };
        for ([_]Failure{ .primary, .projection, .partial_clear }) |failure| {
            const during_projection = failure == .projection;
            var donor_tmp = try TestDirectory.init("db-merge-recovery-donor");
            defer donor_tmp.cleanup();
            var receiver_tmp = try TestDirectory.init("db-merge-recovery-receiver");
            defer receiver_tmp.cleanup();
            const config: types.IndexConfig = .{ .name = "g", .kind = .graph, .config_json = if (timed)
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
                    try db.addEnrichment(.{ .name = "relations_v1", .kind = .asset, .field = "relations", .content_type = "application/json" });
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
                db_zig.test_graph_merge_import_abort_after_primary.store(failure == .primary, .monotonic);
                defer db_zig.test_graph_merge_import_abort_after_primary.store(false, .monotonic);
                db_zig.test_graph_merge_import_abort_during_projection.store(during_projection, .monotonic);
                defer db_zig.test_graph_merge_import_abort_during_projection.store(false, .monotonic);
                graph_mod.test_artifact_rebuild_abort_after_outgoing.store(failure == .partial_clear, .monotonic);
                defer graph_mod.test_artifact_rebuild_abort_after_outgoing.store(false, .monotonic);
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
                const pinned: ?[]graph_mod.Edge = existing_reader.getEdges(alloc, "g", "doc:a", "mentions", .out) catch |err| switch (err) {
                    error.GraphMaintenanceInProgress => null,
                    else => return err,
                };
                if (pinned) |edges| {
                    defer graph_mod.GraphIndex.freeEdges(alloc, edges);
                    try std.testing.expectEqual(@as(usize, 1), edges.len);
                    try std.testing.expectEqualStrings("doc:old", edges[0].target);
                }
                try std.testing.expectError(error.GraphMaintenanceInProgress, receiver.batch(.{ .writes = &.{.{ .key = "doc:a", .value = "{\"title\":\"blocked\"}" }} }));
            }
            try std.testing.expectError(error.GraphMaintenanceInProgress, DB.open(alloc, receiver_tmp.path(), .{ .open_mode = .query_readonly }));
            if (!timed and failure == .primary) {
                // Recovery itself may crash after resetting private stores.
                // The original primary snapshot and fence remain authoritative.
                db_zig.test_graph_merge_import_abort_during_recovery.store(true, .monotonic);
                defer db_zig.test_graph_merge_import_abort_during_recovery.store(false, .monotonic);
                try std.testing.expectError(error.TestInjectedGraphMergeImportFailure, DB.open(alloc, receiver_tmp.path(), .{ .start_optional_runtimes = false }));
                try std.testing.expectError(error.GraphMaintenanceInProgress, DB.open(alloc, receiver_tmp.path(), .{ .open_mode = .query_readonly }));
            }
            {
                // Recovery has no donor handle or donor files to consult.
                var recovered = try DB.open(alloc, receiver_tmp.path(), .{ .start_optional_runtimes = false });
                defer recovered.close();
                try std.testing.expectError(error.NotFound, recovered.core.store.get(alloc, graph_merge_import_recovery_key));
                try std.testing.expect((try recovered.core.loadAppliedSequence(alloc, "g")) >= replay_floor);
                const metric_status = try recovered.core.index_manager.graphIndex("g").?.index.graphMetricSchedulerStatus("degree", null);
                try std.testing.expect(metric_status.state == .not_ready);
                for ([_][]const u8{ "doc:a", "doc:c", "doc:0" }, [_][]const u8{ "doc:b", "doc:d", "doc:retained" }) |source, target| {
                    const edges = try recovered.getEdges(alloc, "g", source, "mentions", .out);
                    defer graph_mod.GraphIndex.freeEdges(alloc, edges);
                    try std.testing.expectEqual(@as(usize, 1), edges.len);
                    try std.testing.expectEqualStrings(target, edges[0].target);
                }
                const due = try recovered.core.store.scanPrefix(alloc, &internal_keys.graph_edge_expiration_index_prefix);
                defer docstore_mod.DocStore.freeResults(alloc, due);
                try std.testing.expectEqual(@as(usize, if (timed) 3 else 0), due.len);
                try recovered.batch(.{ .writes = &.{.{ .key = "doc:c", .value = "{\"relations\":{\"type\":\"mentions\",\"target\":{\"document_id\":\"doc:d\"},\"weight\":5}}" }}, .sync_level = .enrichments });
                try recovered.runUntilIdle();
                const revised = try recovered.getEdges(alloc, "g", "doc:c", "mentions", .out);
                defer graph_mod.GraphIndex.freeEdges(alloc, revised);
                try std.testing.expectEqual(@as(usize, 1), revised.len);
                try std.testing.expectEqual(@as(f64, 5), revised[0].weight);
            }
            var readonly = try DB.open(alloc, receiver_tmp.path(), .{ .open_mode = .query_readonly });
            defer readonly.close();
            const visible = try readonly.getEdges(alloc, "g", "doc:a", "mentions", .out);
            defer graph_mod.GraphIndex.freeEdges(alloc, visible);
            try std.testing.expectEqualStrings("doc:b", visible[0].target);
        }
    }
}

test "db direct merge import replaces contributor membership and enforces visible edge limits" {
    const alloc = std.testing.allocator;
    var donor_tmp = try TestDirectory.init("db-merge-members-donor");
    defer donor_tmp.cleanup();
    var receiver_tmp = try TestDirectory.init("db-merge-members-receiver");
    defer receiver_tmp.cleanup();
    var donor = try DB.open(alloc, donor_tmp.path(), .{ .start_optional_runtimes = false });
    defer donor.close();
    var receiver = try DB.open(alloc, receiver_tmp.path(), .{ .start_optional_runtimes = false });
    defer receiver.close();
    for ([_]*DB{ &donor, &receiver }) |db| {
        try db.addEnrichment(.{ .name = "relations_v1", .kind = .asset, .field = "relations", .content_type = "application/json" });
        try db.addIndex(.{ .name = "g", .kind = .graph, .config_json = "{\"max_edges_per_document\":1,\"sources\":[{\"artifact\":\"relations_v1\"}]}" });
        try db.batch(.{ .writes = &.{.{ .key = "doc:a", .value = "{}" }}, .sync_level = .full_index });
    }
    try receiver.batch(.{ .graph_writes = &.{.{ .index_name = "g", .source = "doc:a", .target = "doc:stale", .edge_type = "mentions" }}, .sync_level = .full_index });
    try donor.batch(.{ .graph_writes = &.{.{ .index_name = "g", .source = "doc:a", .target = "doc:b", .edge_type = "mentions", .weight = 4 }}, .sync_level = .full_index });
    try donor.batch(.{ .writes = &.{.{ .key = "doc:a", .value = "{\"relations\":{\"type\":\"mentions\",\"target\":{\"document_id\":\"doc:b\"},\"weight\":2}}" }}, .sync_level = .enrichments });
    try donor.runUntilIdle();
    const generation = receiver.core.index_manager.graphIndex("g").?.config.coverage_generation;
    const count_key = try internal_keys.graphEdgeContenderCountKeyAlloc(alloc, "doc:a", "g");
    defer alloc.free(count_key);
    const prefix = try internal_keys.graphEdgeContenderIndexPrefixAlloc(alloc, "doc:a", "g");
    defer alloc.free(prefix);
    var source_state: ?[]u8 = null;
    defer if (source_state) |key| alloc.free(key);
    for (0..2) |_| {
        try receiver.importMergeRangeFromTransitionDonor(&donor, .{ .start = "doc:a", .end = "doc:z" });
        const raw_count = try receiver.core.store.get(alloc, count_key);
        defer alloc.free(raw_count);
        try std.testing.expectEqual(@as(?usize, 1), try graph_edge_contender.decodeVisibleCount(raw_count, generation));
        const members = try receiver.core.store.scanPrefix(alloc, prefix);
        defer docstore_mod.DocStore.freeResults(alloc, members);
        try std.testing.expectEqual(@as(usize, 3), members.len); // count plus two contributors
        for (members) |member| {
            if (std.mem.eql(u8, member.key, count_key)) continue;
            const view = (try graph_edge_contender.decode(member.value, generation)) orelse return error.TestUnexpectedResult;
            try std.testing.expectEqual(@as(usize, 0), view.payload.len);
            if (view.source_priority != graph_mod.direct_source_priority and source_state == null)
                source_state = try alloc.dupe(u8, view.state_key);
        }
    }
    // Withdrawing the asset cannot uncount the surviving direct edge.
    try receiver.batch(.{ .writes = &.{.{ .key = "doc:a", .value = "{\"title\":\"source removed\"}" }}, .sync_level = .enrichments });
    try receiver.runUntilIdle();
    const remaining_count = try receiver.core.store.get(alloc, count_key);
    defer alloc.free(remaining_count);
    try std.testing.expectEqual(@as(?usize, 1), try graph_edge_contender.decodeVisibleCount(remaining_count, generation));
    const direct = try receiver.getEdges(alloc, "g", "doc:a", "mentions", .out);
    defer graph_mod.GraphIndex.freeEdges(alloc, direct);
    try std.testing.expectEqual(@as(usize, 1), direct.len);
    try std.testing.expectEqual(@as(f64, 4), direct[0].weight);
    var changed = std.ArrayListUnmanaged([]u8).empty;
    defer {
        for (changed.items) |key| alloc.free(key);
        changed.deinit(alloc);
    }
    var changed_set = std.StringHashMapUnmanaged(void).empty;
    defer changed_set.deinit(alloc);
    const artifact_ref: types.ArtifactRef = .{ .document_id = @constCast("doc:a"), .name = @constCast("relations_v1"), .kind = .asset };
    const source = receiver.core.index_manager.graphArtifactSources("g")[0];
    const new_relation = "{\"type\":\"mentions\",\"target\":{\"document_id\":\"doc:new\"}}";
    try std.testing.expectError(error.ResourceLimitExceeded, materializeGraphArtifactValuePaged(alloc, receiver.core.store, receiver.core.index_manager, "g", artifact_ref, source, source_state orelse return error.TestUnexpectedResult, new_relation, .{}, &changed, &changed_set));
    // An empty replacement snapshot must remove stale local membership too.
    try donor.batch(.{ .writes = &.{.{ .key = "doc:a", .value = "{}" }}, .sync_level = .enrichments });
    try donor.runUntilIdle();
    try donor.batch(.{ .graph_deletes = &.{.{ .index_name = "g", .source = "doc:a", .target = "doc:b", .edge_type = "mentions" }}, .sync_level = .full_index });
    try receiver.importMergeRangeFromTransitionDonor(&donor, .{ .start = "doc:a", .end = "doc:z" });
    const empty = try receiver.core.store.scanPrefix(alloc, prefix);
    defer docstore_mod.DocStore.freeResults(alloc, empty);
    try std.testing.expectEqual(@as(usize, 0), empty.len);
    try materializeGraphArtifactValuePaged(alloc, receiver.core.store, receiver.core.index_manager, "g", artifact_ref, source, source_state.?, new_relation, .{}, &changed, &changed_set);
    const new_count = try receiver.core.store.get(alloc, count_key);
    defer alloc.free(new_count);
    try std.testing.expectEqual(@as(?usize, 1), try graph_edge_contender.decodeVisibleCount(new_count, generation));
}

test "db direct merge import preserves mixed graph ttl contributor precedence" {
    const alloc = std.testing.allocator;
    var donor_tmp = try TestDirectory.init("db-graph-mixed-merge-donor");
    defer donor_tmp.cleanup();
    var receiver_tmp = try TestDirectory.init("db-graph-mixed-merge-receiver");
    defer receiver_tmp.cleanup();
    var donor = try DB.open(alloc, donor_tmp.path(), .{ .start_optional_runtimes = false });
    defer donor.close();
    var receiver = try DB.open(alloc, receiver_tmp.path(), .{ .start_optional_runtimes = false });
    defer receiver.close();
    const enrichment: types.EnrichmentConfig = .{ .name = "relations_v1", .kind = .asset, .field = "relations", .content_type = "application/json" };
    const index: types.IndexConfig = .{ .name = "relations_graph", .kind = .graph, .config_json =
        \\{"ttl":{"duration":"1h"},"sources":[{"artifact":"relations_v1"}]}
    };
    try donor.addEnrichment(enrichment);
    try donor.addIndex(index);
    try receiver.addEnrichment(enrichment);
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
    const donor_due = try donor.core.store.scanPrefix(alloc, &internal_keys.graph_edge_expiration_index_prefix);
    defer docstore_mod.DocStore.freeResults(alloc, donor_due);
    try std.testing.expectEqual(@as(usize, 2), donor_due.len);
    var direct_candidate: ?GraphTtlCandidate = null;
    for (donor_due) |row| switch (try graph_edge_ttl_expiration.decodeDue(row.value)) {
        .source => |candidate| {
            if (candidate.source_priority == graph_mod.direct_source_priority) direct_candidate = candidate;
        },
        .direct => return error.TestUnexpectedResult,
    };
    const direct = direct_candidate orelse return error.TestExpectedGraphEdgeContender;
    try receiver.importMergeRangeFromTransitionDonor(&donor, .{ .start = "doc:a", .end = "doc:z" });
    try receiver.runUntilIdle();
    const receiver_due = try receiver.core.store.scanPrefix(alloc, &internal_keys.graph_edge_expiration_index_prefix);
    defer docstore_mod.DocStore.freeResults(alloc, receiver_due);
    try std.testing.expectEqual(@as(usize, 2), receiver_due.len);
    var imported_direct = false;
    for (receiver_due) |row| switch (try graph_edge_ttl_expiration.decodeDue(row.value)) {
        .source => |candidate| {
            if (candidate.source_priority == graph_mod.direct_source_priority) {
                imported_direct = true;
                try std.testing.expectEqual(direct.deadline_ns, candidate.deadline_ns);
                try std.testing.expectEqual(receiver.core.index_manager.graphIndex("relations_graph").?.config.coverage_generation, candidate.generation);
            }
        },
        .direct => return error.TestUnexpectedResult,
    };
    try std.testing.expect(imported_direct);
    const before = try receiver.getEdges(alloc, "relations_graph", "doc:a", "mentions", .out);
    defer graph_mod.GraphIndex.freeEdges(alloc, before);
    try std.testing.expectEqual(@as(usize, 1), before.len);
    try std.testing.expectEqual(@as(f64, 3), before[0].weight);
    var manual_clock = platform_clock.ManualClock{};
    manual_clock.setRealtimeNs(direct.deadline_ns + 1);
    var ttl_ctx = TtlCleanupContext{ .batch = donor.batchContext(), .grace_period_ns = 0, .clock = manual_clock.clock() };
    try std.testing.expect(try expireGraphTtlCandidateContext(&ttl_ctx, direct));
    const donor_fallback = try donor.getEdges(alloc, "relations_graph", "doc:a", "mentions", .out);
    defer graph_mod.GraphIndex.freeEdges(alloc, donor_fallback);
    try std.testing.expectEqual(@as(f64, 2), donor_fallback[0].weight);
    try receiver.importMergeRangeFromTransitionDonor(&donor, .{ .start = "doc:a", .end = "doc:z" });
    try receiver.runUntilIdle();
    const remaining_due = try receiver.core.store.scanPrefix(alloc, &internal_keys.graph_edge_expiration_index_prefix);
    defer docstore_mod.DocStore.freeResults(alloc, remaining_due);
    try std.testing.expectEqual(@as(usize, 1), remaining_due.len);
    const receiver_generation = receiver.core.index_manager.graphIndex("relations_graph").?.config.coverage_generation;
    const contender_prefix = try internal_keys.graphGlobalEdgeContenderEdgePrefixAlloc(alloc, "relations_graph", receiver_generation, direct.edge_key);
    defer alloc.free(contender_prefix);
    const receiver_contenders = try receiver.core.store.scanPrefix(alloc, contender_prefix);
    defer docstore_mod.DocStore.freeResults(alloc, receiver_contenders);
    try std.testing.expectEqual(@as(usize, 1), receiver_contenders.len);
    const remaining = (try graph_edge_contender.decode(receiver_contenders[0].value, receiver_generation)) orelse return error.TestExpectedGraphEdgeContender;
    try std.testing.expectEqual(@as(usize, 0), remaining.source_priority);
    const imported_raw = try receiver.core.store.get(alloc, direct.edge_key);
    defer alloc.free(imported_raw);
    var imported_edge = try enrichment_artifact_codec.decodeGraphEdgeAlloc(alloc, imported_raw);
    defer imported_edge.deinit(alloc);
    try std.testing.expectEqual(@as(f64, 2), imported_edge.weight);
    const after = try receiver.getEdges(alloc, "relations_graph", "doc:a", "mentions", .out);
    defer graph_mod.GraphIndex.freeEdges(alloc, after);
    try std.testing.expectEqual(@as(usize, 1), after.len);
    try std.testing.expectEqual(@as(f64, 2), after[0].weight);

    // A later donor snapshot can retire the remaining asset source too.
    try donor.batch(.{ .writes = &.{.{ .key = "doc:a", .value = "{}" }}, .sync_level = .enrichments });
    try donor.runUntilIdle();
    try receiver.importMergeRangeFromTransitionDonor(&donor, .{ .start = "doc:a", .end = "doc:z" });
    try receiver.runUntilIdle();
    const retired = try receiver.getEdges(alloc, "relations_graph", "doc:a", "mentions", .out);
    defer graph_mod.GraphIndex.freeEdges(alloc, retired);
    try std.testing.expectEqual(@as(usize, 0), retired.len);
    const retired_contenders = try receiver.core.store.scanPrefix(alloc, contender_prefix);
    defer docstore_mod.DocStore.freeResults(alloc, retired_contenders);
    try std.testing.expectEqual(@as(usize, 0), retired_contenders.len);
    const retired_due = try receiver.core.store.scanPrefix(alloc, &internal_keys.graph_edge_expiration_index_prefix);
    defer docstore_mod.DocStore.freeResults(alloc, retired_due);
    try std.testing.expectEqual(@as(usize, 0), retired_due.len);
}

test "db batch load profile benchmark" {
    if (!profileBenchTestsEnabled()) return error.SkipZigTest;
    const alloc = std.testing.allocator;

    const t = try TestDb.open(alloc, .{
        .primary_backend = .{ .lsm = .{ .flush_threshold = 1 } },
    });
    defer t.deinit();
    const db = &t.db;

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
