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

//! Transaction operations of the local storage DB. Methods take `*DB` and
//! are re-exported from `DB` in db.zig, so callers keep writing `db.method()`.

const db_zig = @import("db.zig");
const DB = db_zig.DB;
const IndexRepairSchedulerDirectory = db_zig.IndexRepairSchedulerDirectory;
const TestDirectory = @import("../../common/test_directory.zig").TestDirectory;
const builtin = @import("builtin");
const change_journal_mod = @import("derived/change_journal.zig");
const db_core = @import("core.zig");
const doc_identity = @import("doc_identity.zig");
const docstore_mod = @import("../docstore.zig");
const enrichment_artifact_codec = @import("enrichment/artifact_codec.zig");
const index_manager_mod = @import("catalog/index_manager.zig");
const index_repair_state = @import("derived/index_repair_state.zig");
const internal_keys = @import("../internal_keys.zig");
const mapper = @import("document/mapper.zig");
const mem_backend_mod = @import("../mem_backend.zig");
const online_integrity_shadow = @import("online_integrity_shadow.zig");
const range_protection_mod = @import("../range_protection.zig");
const relational_index_catalog = @import("relational_index_catalog.zig");
const relational_index_jobs = @import("relational_index_jobs.zig");
const relational_index_keys = @import("relational_index_keys.zig");
const relational_index_maintenance = @import("relational_index_maintenance.zig");
const relational_index_maintenance_contract = @import("relational_index_maintenance_contract.zig");
const relational_integrity = @import("relational_integrity.zig");
const relational_integrity_activation = @import("relational_integrity_activation.zig");
const relational_integrity_catalog = @import("relational_integrity_catalog.zig");
const relational_integrity_retirement = @import("relational_integrity_retirement.zig");
const relational_integrity_topology = @import("relational_integrity_topology.zig");
const relational_row_codec = @import("algebraic/relational_row_codec.zig");
const relational_store = @import("relational_store.zig");
const restore_staging = @import("restore_staging.zig");
const retained_effects_mod = @import("../retained_effects.zig");
const row_policy_authority_mod = @import("../../usermgr/row_policy_authority.zig");
const row_policy_gate_mod = @import("row_policy_gate.zig");
const schema_mod = @import("../schema.zig");
const schema_registry_mod = @import("schema_registry.zig");
const std = @import("std");
const storage_engine = @import("../engine.zig");
const transaction_runtime_mod = @import("maintenance/transaction_runtime.zig");
const transactions_mod = @import("../transactions.zig");
const types = @import("types.zig");
const Allocator = db_zig.Allocator;
const AsyncContext = db_zig.AsyncContext;
const BatchProfile = db_zig.BatchProfile;
const LocalExecutionContext = db_zig.LocalExecutionContext;
const MutationBarrier = db_zig.MutationBarrier;
const OpenOptions = db_zig.OpenOptions;
const OrderedApplyReceipt = db_zig.OrderedApplyReceipt;
const RequestPreparationContext = db_zig.RequestPreparationContext;
const TransactionRecoveryContexts = db_zig.TransactionRecoveryContexts;
const benchMetricsEnabled = db_zig.benchMetricsEnabled;
const checkArtifactRepairActivationOwner = db_zig.checkArtifactRepairActivationOwner;
const directoryUsageBytes = db_zig.directoryUsageBytes;
const encodeStoreLookupKeyAlloc = db_zig.encodeStoreLookupKeyAlloc;
const isMetadataKey = db_zig.isMetadataKey;
const lockApply = db_zig.lockApply;
const lockAtomic = db_zig.lockAtomic;
const logBatchProfile = db_zig.logBatchProfile;
const makeTxnId = db_zig.makeTxnId;
const missingAsNull = db_zig.missingAsNull;
const monotonicTimeNs = db_zig.monotonicTimeNs;
const notifyExecutorForSyncLevelWithDenseBulkDeferral = db_zig.notifyExecutorForSyncLevelWithDenseBulkDeferral;
const openModeRequiresReadOnlyBackends = db_zig.openModeRequiresReadOnlyBackends;
const orderedApplyDisposition = db_zig.orderedApplyDisposition;
const orderedApplyReceiptWrite = db_zig.orderedApplyReceiptWrite;
const ordered_apply_receipt_value_len = db_zig.ordered_apply_receipt_value_len;
const platform_time = db_zig.platform_time;
const quarantineClaimMismatchReason = db_zig.quarantineClaimMismatchReason;
const readOrderedApplyReceipt = db_zig.readOrderedApplyReceipt;
const relationalColumns = db_zig.relationalColumns;
const resolveRecoveredLocalTransaction = db_zig.resolveRecoveredLocalTransaction;
const retryableIndexRepairTerminalPhase = db_zig.retryableIndexRepairTerminalPhase;
const ActivatedIndexRepairCertificationFailure = DB.ActivatedIndexRepairCertificationFailure;
const DurableIndexRepairRunControl = DB.DurableIndexRepairRunControl;
const IndexRepairAdvanceResult = DB.IndexRepairAdvanceResult;
const IndexRepairBindingEffect = DB.IndexRepairBindingEffect;
const IndexRepairIntentSummary = DB.IndexRepairIntentSummary;
const IndexRepairIntentUpdate = DB.IndexRepairIntentUpdate;
const RepairCapacityGuard = DB.RepairCapacityGuard;
const applyCommittedRepairOutcomeToAdvance = DB.applyCommittedRepairOutcomeToAdvance;
const artifactRepairKindForIndexKind = DB.artifactRepairKindForIndexKind;
const coalesceKeyValueRequest = DB.coalesceKeyValueRequest;
const hasCoordinatedConstraints = DB.hasCoordinatedConstraints;
const indexRepairActionRequired = DB.indexRepairActionRequired;
const indexRepairAdmission = DB.indexRepairAdmission;
const indexRepairFailureIsTerminal = DB.indexRepairFailureIsTerminal;
const indexRepairPhaseHasActivationCertification = DB.indexRepairPhaseHasActivationCertification;
const indexRepairRetryDelayMs = DB.indexRepairRetryDelayMs;
const indexRepairStateLocationContext = DB.indexRepairStateLocationContext;
const isProtectedIntegrityKey = DB.isProtectedIntegrityKey;
const isProtectedRangeWriteKey = DB.isProtectedRangeWriteKey;
const noteIndexRepairSchedulerRemove = DB.noteIndexRepairSchedulerRemove;
const noteIndexRepairSchedulerUpsert = DB.noteIndexRepairSchedulerUpsert;
const notifyIndexRepairVisibilityHook = DB.notifyIndexRepairVisibilityHook;
const persistOperatorRepairCompletionContext = DB.persistOperatorRepairCompletionContext;
const repairAdmissionVersion = DB.repairAdmissionVersion;
const repairCapacityDomain = DB.repairCapacityDomain;
const repairCapacityObservation = DB.repairCapacityObservation;

pub fn prepareTransactionRecoveryOwner(self: *DB) !void {
    const ctx = self.transaction_recovery_local_context orelse return;
    if (ctx.execution != null) return;
    ctx.split_shadow = self.shadow;
    ctx.execution = LocalExecutionContext.borrow(self);
}

pub fn constructTransactionRuntime(alloc: Allocator, contexts: *TransactionRecoveryContexts, self: *DB, cfg: transaction_runtime_mod.Config) !transaction_runtime_mod.Runtime {
    var effective_cfg = cfg;
    effective_cfg.resolution_extra_hooks = db_core.transactionRecoveryIdentityHooks(&contexts.identity);
    effective_cfg.local_resolution_ctx = &contexts.local;
    effective_cfg.resolve_local_fn = resolveRecoveredLocalTransaction;
    return try transaction_runtime_mod.Runtime.init(
        alloc,
        self.core.batchExecutionResources().store,
        self.backend_runtime,
        effective_cfg,
    );
}

pub fn initOptionalTransactionRuntime(self: *DB, cfg: transaction_runtime_mod.Config) !void {
    if (comptime storage_engine.enabled) return storage_engine.call(@src().fn_name, .{ self, cfg });
    var contexts: TransactionRecoveryContexts = .{
        .identity = try db_core.TransactionRecoveryIdentityContext.init(
            self.runtime_alloc,
            self.core.store,
            self.core.identity_namespace,
            relationalColumns(self),
            if (self.core.schema) |schema| schema.version else 0,
        ),
    };
    errdefer contexts.release();
    contexts.identity.resource_manager = self.core.index_manager.resource_manager;
    contexts.identity.io = self.backend_runtime.io() orelse std.Options.debug_io;
    self.transaction_owner = try @TypeOf(self.transaction_owner).create(self.runtime_alloc, contexts, .{ self, cfg }, constructTransactionRuntime);
    self.transaction_recovery_identity_context = &self.transaction_owner.context.?.identity;
    self.transaction_recovery_local_context = &self.transaction_owner.context.?.local;
    self.transaction_runtime = self.transaction_owner.runtime.?;
}

pub fn ensureTransactionRecoveryRuntime(self: *DB, cfg: transaction_runtime_mod.Config) !void {
    if (comptime storage_engine.enabled) return storage_engine.call(@src().fn_name, .{ self, cfg });
    if (!cfg.enabled or self.transaction_runtime != null) return;
    try self.lockApplyForPortableRuntime();
    defer self.core.unlockApply();
    if (self.transaction_runtime != null) return;
    try self.initOptionalTransactionRuntime(cfg);
    if (self.local_execution.optional_runtime_workers_enabled) {
        const runtime = self.transaction_runtime.?;
        try self.prepareTransactionRecoveryOwner();
        try runtime.start();
    }
}

/// Admit an external recovery pass without exposing write-gate policy.
pub fn admitTransactionRecovery(self: *DB) !?MutationBarrier.SharedLease {
    if (comptime storage_engine.enabled) return storage_engine.call(@src().fn_name, .{self});
    var lease = self.acquireReplicationMutationShared();
    errdefer if (lease) |*held| held.release();
    try self.enforceReplicationWriteGate();
    return lease;
}

pub fn recoverFinalizedTransactionIntents(self: *DB, now_ns: u64) !u64 {
    return try resolveFinalizedTransactionIntentsForRecovery(self, now_ns);
}

pub fn recoverLocalTransactionRecords(self: *DB, cutoff_ns: u64, now_ns: u64) !transactions_mod.RecoveryStats {
    if (comptime storage_engine.enabled) return storage_engine.call(@src().fn_name, .{ self, cutoff_ns, now_ns });
    try self.lockApplyForPortableRuntime();
    defer self.core.unlockApply();
    return try self.core.recoverTransactions(cutoff_ns, now_ns);
}

/// Applies a one-participant transaction through the ordinary atomic DB
/// batch while preserving the distributed transaction's representability
/// contract for transforms. This avoids durable 2PC bookkeeping without
/// making graph-transform behavior depend on the current shard count.
pub fn batchTransactionCompatible(self: *DB, req: types.BatchRequest) anyerror!void {
    try self.batchTransactionCompatibleWithVisibilityCancellation(req, .none);
}

pub fn batchTransactionCompatibleWithVisibilityCancellation(self: *DB, req: types.BatchRequest, cancellation: types.CancellationToken) anyerror!void {
    var compatible_req = req;
    compatible_req.reject_graph_transform_projections = true;
    if (benchMetricsEnabled()) {
        var profile = BatchProfile{};
        try self.batchInternal(compatible_req, &profile, .{ .visibility_cancellation = cancellation });
        logBatchProfile(compatible_req, profile);
    } else {
        try self.batchInternal(compatible_req, null, .{ .visibility_cancellation = cancellation });
    }
}

pub fn hasPendingIndexRepairIntents(self: *const DB, alloc: Allocator) !bool {
    if (comptime storage_engine.enabled) return storage_engine.call(@src().fn_name, .{ self, alloc });
    if (builtin.os.tag == .freestanding) return self.hasManagedIndexAdmissionMarker(alloc);
    const location = try self.indexRepairStateLocation();
    var state = index_repair_state.loadAt(alloc, location) catch |err| switch (err) {
        error.FileNotFound => return self.hasManagedIndexAdmissionMarker(alloc),
        else => return err,
    };
    defer state.deinit(alloc);
    if (state.entries.items.len != 0) return true;
    return self.hasManagedIndexAdmissionMarker(alloc);
}

pub fn indexRepairIntentSummary(self: *DB, alloc: Allocator) !IndexRepairIntentSummary {
    if (comptime storage_engine.enabled) return storage_engine.call(@src().fn_name, .{ self, alloc });
    return self.indexRepairIntentSummaryForIndex(alloc, null);
}

pub fn indexRepairIntentSummaryForIndex(self: *DB, alloc: Allocator, target_index_name: ?[]const u8) !IndexRepairIntentSummary {
    if (self.managedAdmissionMaterializationPending()) try self.drainManagedIndexAdmissions(alloc);
    try self.ensureIndexRepairSchedulerDirectory(alloc);
    lockAtomic(&self.async_context.index_repair_scheduler_mutex);
    defer self.async_context.index_repair_scheduler_mutex.unlock();
    return self.async_context.index_repair_scheduler.summaryForIndex(target_index_name);
}

pub fn indexRepairIntentBlocksService(intent: index_repair_state.IndexRepairIntent) bool {
    // Initial materialization has no validated predecessor to retain.
    // Replay can reclassify its trigger while source artifacts arrive,
    // but only the scoped canonical proof or validated cleanup may open
    // admission; the ordinary replay-repair exception is not authority.
    if (intent.work_class == .initial_build) return intent.phase != .cleanup;
    return switch (intent.trigger) {
        .incomplete_bulk_publish => intent.phase != .cleanup,
        // Only an operator-requested rebuild starts from a generation that
        // is still proven healthy. Coverage mismatch or missing proof is
        // fail-closed until replacement validation publishes cleanup.
        .operator_generation_rebuild, .storage_format_migration, .artifact_baseline_adoption => intent.phase == .activating or
            intent.phase == .validating or
            intent.phase == .rolling_back,
        .replay_artifact_unavailable => intent.phase == .activating or
            intent.phase == .validating or
            intent.phase == .rolling_back,
        .artifact_coverage_mismatch,
        .artifact_counter_missing,
        .root_generation_rebuild,
        .projection_generation_invalid,
        .operator_generation_validation,
        .catalog_admission,
        => intent.phase != .cleanup,
    };
}

/// Resolve the phase produced by the complete update transaction. Failure
/// accounting can move an intent to `terminal` without populating the
/// explicit phase field, while an explicit phase retains the historical
/// last-writer precedence of the mutation block below. Safety invariants
/// must reason about this effective phase, not the update's encoding.
pub fn indexRepairIntentEffectivePhase(
    current: index_repair_state.Phase,
    update: IndexRepairIntentUpdate,
) index_repair_state.Phase {
    if (update.phase) |phase| return phase;
    if (update.attempt_failure) |failure| {
        if (failure.terminal) return .terminal;
    }
    return current;
}

pub fn updateIndexRepairIntent(
    self: *DB,
    alloc: Allocator,
    repair_id: u128,
    update: IndexRepairIntentUpdate,
) !void {
    return try self.updateIndexRepairIntentWithBindingEffect(
        alloc,
        repair_id,
        update,
        .retain,
        null,
    );
}

pub fn updateIndexRepairIntentWithBindingEffect(
    self: *DB,
    alloc: Allocator,
    repair_id: u128,
    update: IndexRepairIntentUpdate,
    binding_effect: IndexRepairBindingEffect,
    required_revision: ?u64,
) !void {
    lockAtomic(&self.async_context.index_repair_control_mutex);
    var control_locked = true;
    defer if (control_locked) self.async_context.index_repair_control_mutex.unlock();
    const location = try self.indexRepairStateLocation();
    var state = try index_repair_state.loadAt(alloc, location);
    defer state.deinit(alloc);
    const i = blk: {
        for (state.entries.items, 0..) |entry, entry_i| {
            if (entry.intent.repair_id == repair_id) break :blk entry_i;
        }
        return error.IndexRepairIntentNotFound;
    };
    if (required_revision) |revision| {
        if (state.entries.items[i].intent.revision != revision) {
            return error.RepairTransitionConflict;
        }
    }
    var entry = try state.entries.items[i].clone(alloc);
    defer entry.deinit(alloc);
    const effective_phase = indexRepairIntentEffectivePhase(entry.intent.phase, update);
    if (update.candidate_applied_sequence) |candidate_sequence| {
        const remains_activation_certified = indexRepairPhaseHasActivationCertification(entry.intent.phase) and
            indexRepairPhaseHasActivationCertification(effective_phase);
        if (remains_activation_certified and candidate_sequence != entry.intent.candidate_applied_sequence) {
            return error.InvalidIndexRepairState;
        }
    }
    const previous_admission = indexRepairAdmission(entry.intent);
    const previous_action_required = indexRepairActionRequired(entry.intent);
    const previous_trigger = entry.intent.trigger;
    const previous_phase = entry.intent.phase;
    const expected = index_repair_state.ExpectedTransition{
        .repair_id = entry.intent.repair_id,
        .revision = entry.intent.revision,
        .phase = entry.intent.phase,
        .config_hash = entry.intent.config_hash,
        .root_generation = entry.intent.root_generation,
        .owner_epoch = entry.intent.owner_epoch,
    };
    if (update.attempt_failure != null and update.replace_last_error) {
        return error.InvalidArgument;
    }

    // Stage every fallible owned-string replacement before mutating the
    // cloned intent. Once staging succeeds, the swaps below are infallible;
    // on a later checkpoint failure, entry.deinit owns the replacements.
    const replacement_candidate_path = if (update.replace_candidate_path)
        if (update.candidate_relative_path) |value| try alloc.dupe(u8, value) else null
    else
        null;
    var replacement_candidate_path_owned = replacement_candidate_path != null;
    errdefer if (replacement_candidate_path_owned) alloc.free(replacement_candidate_path.?);

    const replacement_previous_active_path = if (update.replace_previous_active_path)
        if (update.previous_active_relative_path) |value| try alloc.dupe(u8, value) else null
    else
        null;
    var replacement_previous_active_path_owned = replacement_previous_active_path != null;
    errdefer if (replacement_previous_active_path_owned) alloc.free(replacement_previous_active_path.?);

    const replacement_build_resume_key = if (update.replace_build_resume_key)
        if (update.build_resume_key) |value| try alloc.dupe(u8, value) else null
    else
        null;
    var replacement_build_resume_key_owned = replacement_build_resume_key != null;
    errdefer if (replacement_build_resume_key_owned) alloc.free(replacement_build_resume_key.?);

    const replacement_source_replay_resume_key = if (update.replace_source_replay_resume_key)
        if (update.source_replay_resume_key) |value| try alloc.dupe(u8, value) else null
    else
        null;
    var replacement_source_replay_resume_key_owned = replacement_source_replay_resume_key != null;
    errdefer if (replacement_source_replay_resume_key_owned) alloc.free(replacement_source_replay_resume_key.?);

    const replaces_last_error = update.attempt_failure != null or update.replace_last_error;
    const replacement_last_error_source = if (update.attempt_failure) |failure|
        @as(?[]const u8, failure.err_name)
    else
        update.last_error;
    const replacement_last_error = if (replaces_last_error)
        if (replacement_last_error_source) |value| try alloc.dupe(u8, value) else null
    else
        null;
    var replacement_last_error_owned = replacement_last_error != null;
    errdefer if (replacement_last_error_owned) alloc.free(replacement_last_error.?);

    const now_ms = self.indexRepairNowMs();
    if (update.attempt_failure) |failure| {
        entry.intent.attempt_count = @max(entry.intent.attempt_count, 1);
        entry.intent.failure_streak +|= 1;
        entry.intent.next_retry_at_ms = if (failure.terminal)
            0
        else
            now_ms +| indexRepairRetryDelayMs(repair_id, entry.intent.failure_streak);
    }
    if (update.trigger) |value| entry.intent.trigger = value;
    if (update.operator_job_id) |value| entry.intent.operator_job_id = value;
    if (update.operator_job_created_at_ms) |value| entry.intent.operator_job_created_at_ms = value;
    if (update.phase != null or update.attempt_failure != null) entry.intent.phase = effective_phase;
    if (update.replace_candidate_path) {
        if (entry.intent.candidate_relative_path) |value| alloc.free(value);
        entry.intent.candidate_relative_path = replacement_candidate_path;
        replacement_candidate_path_owned = false;
    }
    if (update.previous_pointer_captured) |value| entry.intent.previous_pointer_captured = value;
    if (update.replace_previous_active_path) {
        if (entry.intent.previous_active_relative_path) |value| alloc.free(value);
        entry.intent.previous_active_relative_path = replacement_previous_active_path;
        replacement_previous_active_path_owned = false;
    }
    if (update.candidate_applied_sequence) |value| entry.intent.candidate_applied_sequence = value;
    if (update.replace_build_resume_key) {
        if (entry.intent.build_resume_key) |value| alloc.free(value);
        entry.intent.build_resume_key = replacement_build_resume_key;
        replacement_build_resume_key_owned = false;
    }
    if (update.build_reprocessed) |value| entry.intent.build_reprocessed = value;
    if (update.replace_source_replay_resume_key) {
        if (entry.intent.source_replay_resume_key) |value| alloc.free(value);
        entry.intent.source_replay_resume_key = replacement_source_replay_resume_key;
        replacement_source_replay_resume_key_owned = false;
    }
    if (update.source_replay_reprocessed) |value| entry.intent.source_replay_reprocessed = value;
    if (update.source_replay_state) |value| entry.intent.source_replay_state = value;
    if (update.estimated_candidate_bytes) |value| entry.intent.estimated_candidate_bytes = value;
    if (update.planned_disk_bytes) |value| entry.intent.planned_disk_bytes = value;
    if (update.target_sequence) |value| entry.intent.target_sequence = value;
    if (update.attempt_count) |value| entry.intent.attempt_count = value;
    if (update.failure_streak) |value| entry.intent.failure_streak = value;
    if (update.next_retry_at_ms) |value| entry.intent.next_retry_at_ms = value;
    if (update.automation) |value| entry.intent.automation = value;
    if (update.owner_epoch) |value| entry.intent.owner_epoch = value;
    if (replaces_last_error) {
        if (entry.intent.last_error) |value| alloc.free(value);
        entry.intent.last_error = replacement_last_error;
        replacement_last_error_owned = false;
    }
    entry.intent.updated_at_ms = now_ms;
    const blocks_service = indexRepairIntentBlocksService(entry.intent);
    const preserve_serviceability = blocks_service and
        previous_admission == .blocked and
        previous_trigger == entry.intent.trigger and
        previous_phase == entry.intent.phase;
    var admission_fence: ?index_manager_mod.IndexManager.RepairAdmissionFence = if (blocks_service and
        !preserve_serviceability)
        try self.core.index_manager.beginRepairAdmissionFence(entry.intent.index_name)
    else
        null;
    defer if (admission_fence) |*fence| {
        self.core.index_manager.cancelRepairAdmissionFence(fence, entry.intent.index_name);
    };
    const control_revision = try index_repair_state.putEntryAt(alloc, location, state.identity, expected, entry);
    const persisted_revision = entry.intent.revision +| 1;
    if (admission_fence) |*fence| {
        self.core.index_manager.finishRepairAdmissionFence(
            fence,
            entry.intent.index_name,
            repairAdmissionVersion(entry.intent, persisted_revision, control_revision),
            true,
            false,
        );
    } else {
        self.core.index_manager.publishRepairAdmission(
            entry.intent.index_name,
            repairAdmissionVersion(entry.intent, persisted_revision, control_revision),
            blocks_service,
            preserve_serviceability,
        );
    }
    noteIndexRepairSchedulerUpsert(self.async_context, entry.intent, persisted_revision, control_revision);
    if (binding_effect == .release) {
        self.core.index_manager.releaseFailedIndexLoadRepairClaim(
            entry.intent.index_name,
            entry.intent.repair_id,
        );
    }
    // Hooks may enqueue work or inspect the DB. Publish them only after the
    // durable transition, scheduler projection, and binding effect are one
    // completed control transaction.
    self.async_context.index_repair_control_mutex.unlock();
    control_locked = false;
    const admission = indexRepairAdmission(entry.intent);
    const action_required = indexRepairActionRequired(entry.intent);
    if (previous_admission != admission or
        previous_action_required != action_required)
    {
        notifyIndexRepairVisibilityHook(
            self.async_context,
            .index_repair_pending,
            entry.intent,
            persisted_revision,
            previous_admission,
            previous_action_required,
            false,
        );
    }
    const persisted_phase = update.phase orelse
        if (update.attempt_failure != null) entry.intent.phase else null;
    if (persisted_phase) |phase| {
        if (self.shadow_index_repair_hook) |hook| {
            if (hook.after_phase_persisted) |after_phase| {
                try after_phase(hook.ptr, self, repair_id, phase);
            }
        }
    }
}

pub fn createGenerationRepairIntent(
    self: *DB,
    alloc: Allocator,
    cfg: types.IndexConfig,
    trigger: index_repair_state.Trigger,
    operator_job_id: u64,
    operator_job_created_at_ms: u64,
    last_error: ?[]const u8,
) !u128 {
    return try self.createGenerationRepairIntentAtTarget(
        alloc,
        cfg,
        trigger,
        operator_job_id,
        operator_job_created_at_ms,
        last_error,
        null,
        .not_required,
        .repair,
    );
}

pub fn createGenerationRepairIntentAtTarget(
    self: *DB,
    alloc: Allocator,
    cfg: types.IndexConfig,
    trigger: index_repair_state.Trigger,
    operator_job_id: u64,
    operator_job_created_at_ms: u64,
    last_error: ?[]const u8,
    minimum_target_sequence: ?u64,
    source_replay_state: index_repair_state.SourceReplayState,
    work_class: index_repair_state.WorkClass,
) !u128 {
    lockAtomic(&self.async_context.index_repair_control_mutex);
    var control_locked = true;
    defer if (control_locked) self.async_context.index_repair_control_mutex.unlock();
    var state = try self.loadOrCreateCurrentIndexRepairState(alloc);
    defer state.deinit(alloc);
    if (state.findIndex(cfg.name)) |i| return state.entries.items[i].intent.repair_id;
    const location = try self.indexRepairStateLocation();
    const now_ms = self.indexRepairNowMs();
    const target_sequence = @max(
        self.core.nextDerivedSequence(),
        minimum_target_sequence orelse 0,
    );
    const repair_id = try index_repair_state.newRepairId(alloc);
    const index_name = try alloc.dupe(u8, cfg.name);
    var intent_allocations_transferred = false;
    errdefer if (!intent_allocations_transferred) alloc.free(index_name);
    const owned_last_error = if (last_error) |value| try alloc.dupe(u8, value) else null;
    errdefer if (!intent_allocations_transferred) if (owned_last_error) |value| alloc.free(value);
    var intent = index_repair_state.IndexRepairIntent{
        .repair_id = repair_id,
        .db_identity = state.identity.db_identity,
        .group_id = self.localRepairGroupId(),
        .replica_id = state.identity.replica_id,
        .root_generation = state.identity.root_generation,
        .index_name = index_name,
        .kind = cfg.kind,
        .config_hash = types.indexConfigHash(cfg),
        .trigger = trigger,
        .work_class = work_class,
        .operator_job_id = operator_job_id,
        .operator_job_created_at_ms = operator_job_created_at_ms,
        .detected_sequence = target_sequence,
        .target_sequence = target_sequence,
        .started_at_ms = now_ms,
        .updated_at_ms = now_ms,
        .owner_epoch = 0,
        .last_error = owned_last_error,
        .source_replay_state = source_replay_state,
    };
    intent_allocations_transferred = true;
    defer intent.deinit(alloc);
    const blocks_service = indexRepairIntentBlocksService(intent);
    var admission_fence: ?index_manager_mod.IndexManager.RepairAdmissionFence = if (blocks_service)
        try self.core.index_manager.beginRepairAdmissionFence(intent.index_name)
    else
        null;
    defer if (admission_fence) |*fence| {
        self.core.index_manager.cancelRepairAdmissionFence(fence, intent.index_name);
    };
    const control_revision = index_repair_state.putEntryAt(alloc, location, state.identity, null, .{ .intent = intent }) catch |err| switch (err) {
        // Another operator/maintenance caller may win creation between the
        // optimistic state read and the checkpoint CAS. Adopt that durable
        // intent; callers subsequently attach their job identity or
        // promote its safety classification with revision-fenced retries.
        error.RepairTransitionConflict => return (try self.indexRepairIdForIndex(alloc, cfg.name)) orelse return err,
        else => return err,
    };
    const persisted_revision: u64 = 1;
    if (admission_fence) |*fence| {
        self.core.index_manager.finishRepairAdmissionFence(
            fence,
            intent.index_name,
            repairAdmissionVersion(intent, persisted_revision, control_revision),
            true,
            false,
        );
    }
    noteIndexRepairSchedulerUpsert(self.async_context, intent, persisted_revision, control_revision);
    self.async_context.index_repair_control_mutex.unlock();
    control_locked = false;
    notifyIndexRepairVisibilityHook(
        self.async_context,
        .index_repair_pending,
        intent,
        persisted_revision,
        .serviceable,
        false,
        false,
    );
    return repair_id;
}

pub fn createOperatorGenerationRepairIntent(
    self: *DB,
    alloc: Allocator,
    cfg: types.IndexConfig,
    operator_job_id: u64,
    operator_job_created_at_ms: u64,
) !u128 {
    const repair_id = try self.createGenerationRepairIntent(
        alloc,
        cfg,
        if (cfg.kind == .dense_vector) .operator_generation_validation else .operator_generation_rebuild,
        operator_job_id,
        operator_job_created_at_ms,
        null,
    );
    try self.attachOperatorRepairRequest(
        alloc,
        repair_id,
        operator_job_id,
        operator_job_created_at_ms,
    );
    return repair_id;
}

pub fn removeOrphanedIndexRepairIntents(self: *DB, alloc: Allocator) !void {
    var state = self.loadIndexRepairState(alloc) catch |err| switch (err) {
        error.FileNotFound => return,
        else => return err,
    };
    defer state.deinit(alloc);
    var orphan_ids = std.ArrayListUnmanaged(u128).empty;
    defer orphan_ids.deinit(alloc);
    for (state.entries.items) |entry| {
        if (self.core.index_manager.get(entry.intent.index_name) == null) {
            try orphan_ids.append(alloc, entry.intent.repair_id);
        }
    }
    for (orphan_ids.items) |repair_id| try self.removeIndexRepairIntentAndPin(alloc, repair_id);
}

/// Requires the DB apply lock. Fresh admission must not adopt repair state
/// from a catalog generation whose deletion already committed.
pub fn removeOrphanedIndexRepairIntentForFreshAdmission(self: *DB, alloc: Allocator, index_name: []const u8) !void {
    if (self.core.index_manager.get(index_name) != null) return;
    const repair_id = (try self.indexRepairIdForIndex(alloc, index_name)) orelse return;
    try self.removeIndexRepairIntentAndPin(alloc, repair_id);
}

pub fn removeIndexRepairIntentAndPin(self: *DB, alloc: Allocator, repair_id: u128) !void {
    return try removeIndexRepairIntentAndPinContext(self.async_context, alloc, repair_id);
}

pub fn removeIndexRepairIntentAndPinContext(ctx: *AsyncContext, alloc: Allocator, repair_id: u128) !void {
    lockAtomic(&ctx.index_repair_control_mutex);
    var control_locked = true;
    defer if (control_locked) ctx.index_repair_control_mutex.unlock();
    // Pin creation, pin removal, the in-memory pressure gate, and replay
    // truncation must observe one serialization order. Per-index repair
    // leases are insufficient because two different indexes may complete
    // concurrently.
    const repair_replay_mutex = ctx.repair_replay_mutex orelse return error.DurableIndexRepairStateUnavailable;
    repair_replay_mutex.lockUncancelable(ctx.index_manager.checkpointIo());
    defer repair_replay_mutex.unlock(ctx.index_manager.checkpointIo());
    const location = try indexRepairStateLocationContext(ctx);
    var state = try index_repair_state.loadAt(alloc, location);
    defer state.deinit(alloc);
    const entry = blk: {
        for (state.entries.items) |candidate| {
            if (candidate.intent.repair_id == repair_id) break :blk candidate;
        }
        return error.IndexRepairIntentNotFound;
    };
    if (entry.intent.phase == .cleanup) try persistOperatorRepairCompletionContext(ctx, alloc, entry.intent);
    var remaining_replay_pin = false;
    for (state.entries.items) |candidate| {
        if (candidate.intent.repair_id != repair_id and candidate.pin != null) {
            remaining_replay_pin = true;
            break;
        }
    }
    // For managed admission, clear the primary-store lifecycle marker only
    // after activation published a clean generation. Do this before
    // removing the checkpoint intent: a crash between the two leaves
    // harmless resumable checkpoint debt, never an admitted generation
    // with no proof.
    const admission_key = try internal_keys.managedIndexAdmissionKeyAlloc(alloc, entry.intent.index_name);
    defer alloc.free(admission_key);
    ctx.store.delete(admission_key) catch |err| switch (err) {
        error.NotFound => {},
        else => return err,
    };
    const control_revision = try index_repair_state.removeEntryAndPinAt(alloc, location, state.identity, .{
        .repair_id = repair_id,
        .revision = entry.intent.revision,
        .phase = entry.intent.phase,
        .config_hash = entry.intent.config_hash,
        .root_generation = entry.intent.root_generation,
        .owner_epoch = entry.intent.owner_epoch,
    });
    noteIndexRepairSchedulerRemove(ctx, state.identity, repair_id, control_revision);
    ctx.index_repair_replay_pinned.store(remaining_replay_pin, .release);
    ctx.index_manager.publishRepairAdmission(
        entry.intent.index_name,
        repairAdmissionVersion(entry.intent, entry.intent.revision +| 1, control_revision),
        false,
        false,
    );
    ctx.index_manager.releaseFailedIndexLoadRepairClaim(
        entry.intent.index_name,
        entry.intent.repair_id,
    );
    ctx.index_repair_control_mutex.unlock();
    control_locked = false;
    notifyIndexRepairVisibilityHook(
        ctx,
        .index_repair_cleared,
        entry.intent,
        entry.intent.revision,
        indexRepairAdmission(entry.intent),
        indexRepairActionRequired(entry.intent),
        remaining_replay_pin,
    );
}

pub fn advanceIndexRepairIntent(
    self: *DB,
    alloc: Allocator,
    repair_id: u128,
    options: types.ArtifactRepairRunOptions,
) anyerror!IndexRepairAdvanceResult {
    if (comptime storage_engine.enabled) return storage_engine.call(@src().fn_name, .{ self, alloc, repair_id, options });
    return try self.advanceScheduledIndexRepairIntent(alloc, repair_id, options, null, false);
}

pub fn advanceScheduledIndexRepairIntent(
    self: *DB,
    alloc: Allocator,
    repair_id: u128,
    options: types.ArtifactRepairRunOptions,
    scheduled_revision: ?u64,
    audit_due: bool,
) anyerror!IndexRepairAdvanceResult {
    // Resolve the durable name before taking the process-local lease, then
    // reload after acquisition. A pause or terminal transition which wins
    // between those operations must be observed by this owner; using the
    // locator snapshot would let an acknowledged pause start one more
    // repair quantum. The checkpoint is already read repeatedly by a
    // material state-machine turn, while deferred intents remain on the
    // O(1) scheduler path and never pay this ownership handoff cost.
    var locator = try self.loadIndexRepairEntryById(alloc, repair_id);
    defer locator.deinit(alloc);
    if (!(try self.beginIndexRepairLease(locator.intent.index_name))) {
        return .{
            .repair_id = repair_id,
            .busy = true,
        };
    }
    defer self.endIndexRepairLease(locator.intent.index_name);

    if (comptime builtin.is_test) {
        if (DB.test_index_repair_advance_lease_hook) |hook| {
            if (hook.after_acquire) |after_acquire| try after_acquire(hook.ptr, self, repair_id);
        }
    }
    var entry = try self.loadIndexRepairEntryById(alloc, repair_id);
    defer entry.deinit(alloc);
    if (!std.mem.eql(u8, locator.intent.index_name, entry.intent.index_name)) {
        return error.InvalidIndexRepairState;
    }
    if (comptime builtin.is_test) {
        if (DB.test_index_repair_advance_lease_hook) |hook| {
            if (hook.after_reload) |after_reload| try after_reload(hook.ptr, self, repair_id);
        }
    }
    // An audit grant is a process-local acceleration token for one exact
    // durable revision. Selection releases the scheduler mutex before the
    // repair lease is acquired, so a source-replay, retry, pause/resume, or
    // concurrent owner transition may supersede it in that interval. The
    // reloaded durable entry is authoritative; never let a stale token skip
    // the newly revised generation's bounded convergence wait.
    const audit_revision = if (audit_due and
        scheduled_revision != null and
        entry.intent.revision == scheduled_revision.?)
        scheduled_revision
    else
        null;
    var run_control = DurableIndexRepairRunControl{
        .db = self,
        .index_name = entry.intent.index_name,
        .upstream = options.cancel_check,
        .ownership = options.activation_check,
    };
    var owned_options = options;
    owned_options.cancel_check = .{
        .ptr = &run_control,
        .is_requested = DurableIndexRepairRunControl.cancelled,
    };
    return try advanceIndexRepairIntentOwned(self, 
        alloc,
        repair_id,
        owned_options,
        &entry,
        audit_revision,
    );
}

/// Executes one repair quantum while `active_index_repairs[index_name]` is
/// held by the caller. Keeping the lease outside every phase makes pause,
/// cancel, deletion, rollback, and activation share one ownership domain.
pub fn advanceIndexRepairIntentOwned(
    self: *DB,
    alloc: Allocator,
    repair_id: u128,
    options: types.ArtifactRepairRunOptions,
    entry: *index_repair_state.Entry,
    audit_revision: ?u64,
) anyerror!IndexRepairAdvanceResult {
    var result = IndexRepairAdvanceResult{ .repair_id = repair_id };
    var effective_options = options;
    if (effective_options.capacity_source == null) effective_options.capacity_source = self.capacity_source;

    if (effective_options.source_replay_only and !IndexRepairSchedulerDirectory.sourceRecoveryIntent(entry.intent)) {
        result.deferred = true;
        return result;
    }

    if (entry.intent.root_generation != self.core.root_generation or
        entry.intent.group_id != self.localRepairGroupId())
    {
        try self.recordIndexRepairAttemptFailure(alloc, repair_id, "repair_identity_mismatch", true);
        result.terminal = true;
        return result;
    }
    if (entry.intent.phase == .terminal) {
        if (retryableIndexRepairTerminalPhase(
            entry.intent.last_error,
            entry.intent.trigger,
        )) |retry_phase| {
            try self.updateIndexRepairIntent(alloc, repair_id, .{
                .phase = retry_phase,
                .next_retry_at_ms = 0,
                .replace_last_error = true,
            });
            entry.intent.phase = retry_phase;
            entry.intent.revision +|= 1;
        } else {
            result.terminal = true;
            return result;
        }
    }
    // Only startup discovery creates incomplete_bulk_publish intents, so
    // this durable provenance is the authority to reclaim quarantine
    // ownership. Operator and generation-health intents may coexist with
    // retry/manual load failures but must not be reclassified as automatic
    // destructive recovery merely because they share an index name.
    if (entry.intent.trigger == .incomplete_bulk_publish and
        entry.intent.phase != .rolling_back)
    {
        // Retryable terminal phases release their quarantine claim so an
        // operator may intervene. Reclaiming classifies action/config and
        // claims the current incarnation in one catalog-locked operation.
        // Policy or identity drift is terminal for this durable intent: a
        // fresh audit must authorize any different action.
        switch (self.core.index_manager.claimFailedIndexLoadRepair(
            entry.intent.index_name,
            .{ .current_policy = .{
                .action = .rebuild_from_artifacts,
                .config_hash = entry.intent.config_hash,
            } },
            repair_id,
        )) {
            .claimed, .already_claimed, .absent => {},
            .claimed_by_other => {
                result.deferred = true;
                return result;
            },
            .mismatch => |mismatch| {
                try self.recordIndexRepairAttemptFailure(
                    alloc,
                    repair_id,
                    quarantineClaimMismatchReason(mismatch.differences),
                    true,
                );
                result.terminal = true;
                return result;
            },
        }
    }
    // A candidate which already failed coverage cannot manufacture an
    // artifact missing below its snapshot floor. Discard only that
    // inactive candidate and rebuild from a current pinned snapshot.
    if (entry.intent.candidate_relative_path != null and
        retryableIndexRepairTerminalPhase(
            entry.intent.last_error,
            entry.intent.trigger,
        ) != null)
    {
        try self.discardInactiveIndexRepairCandidate(alloc, repair_id);
        result.attempted = true;
        result.deferred = true;
        return result;
    }
    if (entry.intent.automation == .paused) {
        result.deferred = true;
        return result;
    }
    const now_ms = self.indexRepairNowMs();
    if (entry.intent.next_retry_at_ms > now_ms) {
        result.deferred = true;
        result.next_retry_at_ms = entry.intent.next_retry_at_ms;
        return result;
    }
    checkArtifactRepairActivationOwner(options) catch |err| switch (err) {
        error.RepairOwnershipLost => {
            result.deferred = true;
            return result;
        },
        else => return err,
    };

    // Claim the durable state machine with the same epoch captured by the
    // owner-side activation fence. A later term/root owner may safely
    // reclaim the intent, but every subsequent transition and the final
    // activation in this attempt are CAS-fenced by the new epoch.
    if (options.owner_epoch != 0 and entry.intent.owner_epoch != options.owner_epoch) {
        try self.updateIndexRepairIntent(alloc, repair_id, .{ .owner_epoch = options.owner_epoch });
        entry.intent.owner_epoch = options.owner_epoch;
        entry.intent.revision +|= 1;
    }

    const cfg_ptr = self.core.index_manager.get(entry.intent.index_name) orelse {
        try self.recordIndexRepairAttemptFailure(alloc, repair_id, "index_configuration_missing", true);
        result.terminal = true;
        return result;
    };
    if (cfg_ptr.kind != entry.intent.kind or types.indexConfigHash(cfg_ptr.*) != entry.intent.config_hash) {
        try self.recordIndexRepairAttemptFailure(alloc, repair_id, "index_configuration_changed", true);
        result.terminal = true;
        return result;
    }

    if (effective_options.source_replay_only and entry.intent.source_replay_state == .pending and self.enrichment_runtime == null) {
        // An unconstructed optional driver is not evidence that source
        // discovery is complete. Retain its durable cursor for a live owner.
        result.deferred = true;
        return result;
    }

    if (entry.intent.phase == .detected and entry.intent.source_replay_state == .pending) {
        // Managed DDL installs the generator for future writes under the
        // synchronous structural fence, but existing-row discovery belongs
        // to this durable bounded owner. Append one primary-store page,
        // then publish its cursor. A crash before the cursor repeats an
        // idempotent page; a crash after it can never skip source rows.
        const artifact_names = try self.core.index_manager.artifactProducerNamesForIndexAlloc(
            alloc,
            entry.intent.index_name,
        );
        defer {
            for (artifact_names) |name| alloc.free(name);
            alloc.free(artifact_names);
        }
        var page = try self.appendGeneratedEnrichmentsFromStoredDocsPage(
            alloc,
            artifact_names,
            true,
            entry.intent.source_replay_resume_key,
            128,
            .{
                .deadline_ns = if (effective_options.source_replay_only) self.graphReplacementWorkNow() +| 25 * std.time.ns_per_ms else null,
                .min_records_before_deadline = if (effective_options.source_replay_only) 1 else 0,
            },
        );
        defer page.deinit(alloc);
        try self.updateIndexRepairIntent(alloc, repair_id, .{
            .source_replay_resume_key = page.next_resume_key,
            .replace_source_replay_resume_key = true,
            .source_replay_reprocessed = entry.intent.source_replay_reprocessed +| @as(u64, @intCast(page.documents_scanned)),
            .source_replay_state = if (page.complete) .complete else .pending,
            .target_sequence = @max(entry.intent.target_sequence, page.target_sequence),
            .failure_streak = 0,
            .next_retry_at_ms = 0,
        });
        result.attempted = true;
        result.documents_reprocessed = @intCast(page.documents_scanned);
        result.deferred = true;
        return result;
    }

    // Activation recovery is structural O(1) work and precedes corpus
    // validation or capacity admission. `rolling_back` is an explicit
    // durable decision: if restart finds the candidate healthy before the
    // pointer moved, reconciliation may still commit it; otherwise the
    // rollback owner must finish retiring it and must never resume it as a
    // convergence candidate.
    if (entry.intent.phase == .activating or
        entry.intent.phase == .validating or
        entry.intent.phase == .cleanup or
        entry.intent.phase == .rolling_back)
    {
        if (entry.intent.candidate_relative_path == null) {
            try self.recordIndexRepairAttemptFailure(alloc, repair_id, "activated_candidate_missing", true);
            result.terminal = true;
            return result;
        }
        const reconciliation = try self.reconcileActivatedIndexRepair(alloc, repair_id);
        var certification_failure: ?ActivatedIndexRepairCertificationFailure = null;
        switch (reconciliation) {
            .committed => {
                result.attempted = true;
                var repair = types.ArtifactRepairResult{ .indexes_rebuilt = 1 };
                try self.finalizeCommittedIndexRepairOutcome(alloc, entry.intent.index_name, 0, &repair);
                applyCommittedRepairOutcomeToAdvance(&result, repair);
                result.repaired = repair.indexes_degraded_after == 0 and !repair.debt_remaining;
                return result;
            },
            .not_active => {},
            .certification_failed => |failure| certification_failure = failure,
        }
        const rolled_back = try self.rollbackUnavailableActivatedIndexRepair(
            alloc,
            repair_id,
            certification_failure,
        );
        switch (rolled_back) {
            .not_active => {
                // `activating` is durable before pointer publication. If
                // that publication never happened, retain the ready shadow
                // and resume its bounded convergence path.
                try self.updateIndexRepairIntent(alloc, repair_id, .{
                    .phase = .waiting_for_convergence,
                });
                entry.intent.phase = .waiting_for_convergence;
                entry.intent.revision +|= 1;
            },
            .prior_generation_restored => {
                // Yield after the rollback quantum. The restored generation
                // is immediately available while the original durable
                // repair resumes from preflight on the next owner turn.
                result.attempted = true;
                result.deferred = true;
                return result;
            },
            .retry_deferred => {
                var deferred = try self.loadIndexRepairEntryById(alloc, repair_id);
                defer deferred.deinit(alloc);
                result.attempted = true;
                result.deferred = true;
                result.next_retry_at_ms = deferred.intent.next_retry_at_ms;
                return result;
            },
            .action_required => {
                result.attempted = true;
                result.terminal = true;
                return result;
            },
            .rebuild_required => {},
        }
    }

    // Activation/rollback recovery is a committed-generation safety
    // boundary and therefore wins a concurrent cancellation. Everything
    // below may start new scan/build work and must honor the cancellation
    // bit installed by pause/cancel while this lease is visible.
    if (effective_options.cancelled()) {
        result.deferred = true;
        return result;
    }

    if (effective_options.source_replay_only) {
        if (try self.retireServiceableInitialBuild(alloc, repair_id, entry.intent)) {
            result.attempted = true;
            result.repaired = true;
        } else result.deferred = true;
        return result;
    }

    // A managed admission can become queryable before its exact physical
    // cardinality is known. Publish that identity proof before either
    // queryability exit below: the snapshot is ordered with foreground
    // artifact mutations by the apply lock, and later mutations contribute
    // to its signed delta. Deferring first used to park a healthy recreated
    // index forever because no subsequent path owned counter bootstrap.
    self.ensureDenseArtifactTargetCounterForRepair(
        alloc,
        cfg_ptr.*,
        repair_id,
        effective_options.cancel_check,
    ) catch |err| switch (err) {
        error.Canceled, error.StaleDenseArtifactCounterBootstrap => {
            result.attempted = true;
            result.deferred = true;
            return result;
        },
        error.RepairResourceUnavailable => {
            const attempt_count = entry.intent.attempt_count +| 1;
            const failure_streak = entry.intent.failure_streak +| 1;
            const next_retry_at_ms = now_ms +| indexRepairRetryDelayMs(repair_id, failure_streak);
            try self.updateIndexRepairIntent(alloc, repair_id, .{
                .attempt_count = attempt_count,
                .failure_streak = failure_streak,
                .next_retry_at_ms = next_retry_at_ms,
                .last_error = "RepairResourceUnavailable",
                .replace_last_error = true,
            });
            result.attempted = true;
            result.deferred = true;
            result.next_retry_at_ms = next_retry_at_ms;
            return result;
        },
        else => return err,
    };

    // Check the exact index generation before the shared enrichment
    // producer fence. Other indexes can legitimately advance that global
    // runtime while this managed admission is already complete. Letting
    // unrelated producer work win here keeps a healthy durable intent at
    // the head of a one-slot repair queue forever.
    if (try self.retireServiceableInitialBuild(alloc, repair_id, entry.intent)) {
        result.attempted = true;
        result.repaired = true;
        return result;
    }

    // Progressive admission has already published a trustworthy canonical
    // generation. Keep serving and advancing that generation instead of
    // allocating a shadow candidate, which would otherwise make query
    // admission regress from queryable_partial back to fail-closed. The
    // durable intent remains pending and the stronger proof above retires
    // it once coverage and replay converge.
    if (try self.managedAdmissionGenerationIsQueryable(alloc, entry.intent)) {
        const observed_sequence = try self.managedIndexAppliedSequence(alloc, entry.intent.index_name);
        // Selection's slow-audit grant is authority for one exact durable
        // revision, not for the whole owned turn. Claim it with a durable
        // CAS immediately before using the queryable shortcut. Besides
        // closing the selection/reload and reload/use races, the revision
        // advance visibly consumes the process-local audit token so a
        // retry must receive a fresh scheduler grant.
        const consumed_audit_revision = if (audit_revision) |expected_revision|
            try self.consumeIndexRepairAuditGrant(alloc, repair_id, expected_revision)
        else
            null;
        if (consumed_audit_revision) |revision| entry.intent.revision = revision;
        // The audit rechecks the publication proof; its deadline is not
        // evidence of damaged artifacts. A certified partial generation
        // must continue through its canonical worker even when provider
        // failures or new writes keep source coverage incomplete.
        try self.ensureManagedAdmissionCanonicalWorker(alloc, entry.intent);
        if (!try self.deferIndexRepairUntilTargetOrAudit(
            alloc,
            repair_id,
            entry.intent.revision,
            entry.intent.index_name,
            observed_sequence,
            entry.intent.target_sequence,
        )) {
            result.busy = true;
            return result;
        }
        result.deferred = true;
        return result;
    }

    if (entry.intent.phase == .detected and entry.intent.source_replay_state == .complete) {
        // The bounded source-replay phase advances the intent target after
        // every durable page, so its final value is the producer-completion
        // fence. Starting a shadow from artifacts
        // below that fence can snapshot a later artifact value and then
        // replay the earlier delete which produced it, permanently
        // removing deterministic chunk identities from the candidate.
        if (self.enrichment_runtime) |runtime| {
            const enrichment = runtime.stats();
            if (enrichment.enabled and enrichment.applied_sequence < entry.intent.target_sequence) {
                // The source-discovery owner has durably queued all work;
                // provider execution and derived publication now own the
                // next progress edge. Park this exact intent instead of
                // polling it on every repair scheduler turn. A derived
                // publication wakes it only when the durable target has
                // not yet been reached; after that, the bounded audit owns
                // coverage completion, including terminal/skipped output
                // which produces no index watermark edge.
                const observed_sequence = try self.managedIndexAppliedSequence(alloc, entry.intent.index_name);
                if (!try self.deferIndexRepairUntilTargetOrAudit(
                    alloc,
                    repair_id,
                    entry.intent.revision,
                    entry.intent.index_name,
                    observed_sequence,
                    entry.intent.target_sequence,
                )) {
                    result.busy = true;
                    return result;
                }
                result.deferred = true;
                return result;
            }
        }

        // The producer fence is sufficient for shadow construction: all
        // artifacts and their derived records are now durable, and the
        // candidate captures the current replay floor after that output.
        // Do not wait for the ordinary canonical worker to apply each
        // generated record individually. Large chunked admissions are
        // intentionally completed by the bounded bulk snapshot path.
    }

    // Automatic reconstruction is destructive only at pointer activation,
    // but creating an apparently valid empty candidate from missing source
    // metadata is still unsafe. Full per-artifact validation remains part
    // of the shadow build.
    self.validateIndexRepairSourcePreflight(alloc, cfg_ptr.*) catch |err| {
        const err_name = if (err == error.RepairSourceCoverageIncomplete)
            "dense_artifact_coverage_missing"
        else
            @errorName(err);
        try self.recordIndexRepairAttemptFailure(alloc, repair_id, err_name, true);
        result.attempted = true;
        result.terminal = true;
        return result;
    };

    const active_index_bytes = self.core.index_manager.activeIndexStorageBytes(entry.intent.index_name) catch 0;
    const source_estimate = self.artifactBackedDenseRepairEstimate(alloc, cfg_ptr.*) catch 0;
    var estimated_candidate_bytes = @max(
        entry.intent.estimated_candidate_bytes,
        @max(options.estimated_candidate_bytes, @max(active_index_bytes, source_estimate)),
    );
    if (estimated_candidate_bytes == 0) {
        estimated_candidate_bytes = 64 * 1024 * 1024;
    }
    const cleanup_and_replay_headroom = @max(estimated_candidate_bytes / 4, 256 * 1024 * 1024);
    const required_disk_bytes = estimated_candidate_bytes +| cleanup_and_replay_headroom;
    const planned_disk_bytes = @max(
        entry.intent.planned_disk_bytes,
        @max(options.planned_disk_bytes, required_disk_bytes),
    );
    if (entry.intent.estimated_candidate_bytes != estimated_candidate_bytes or
        entry.intent.planned_disk_bytes != planned_disk_bytes)
    {
        try self.updateIndexRepairIntent(alloc, repair_id, .{
            .estimated_candidate_bytes = estimated_candidate_bytes,
            .planned_disk_bytes = planned_disk_bytes,
        });
        entry.intent.estimated_candidate_bytes = estimated_candidate_bytes;
        entry.intent.planned_disk_bytes = planned_disk_bytes;
    }

    // Capacity is checked and claimed through the shared ResourceManager
    // so every DB on the node participates in one admission domain. Existing
    // candidate bytes are already reflected in filesystem availability;
    // reserve only the remaining growth and cleanup headroom on resume.
    // Reconciliation above is intentionally admission-free because it
    // releases space and must remain possible on an already-full volume.
    var candidate_bytes: u64 = 0;
    if (entry.intent.candidate_relative_path) |candidate| {
        try index_repair_state.validateCandidateRelativePath(entry.intent.index_name, candidate);
        const separator = std.mem.indexOfScalar(u8, candidate, '/') orelse return error.InvalidRepairCandidatePath;
        if (entry.intent.kind == .algebraic) {
            const storage_namespace = try std.fmt.allocPrint(
                alloc,
                "{s}/{s}",
                .{ candidate[0..separator], entry.intent.index_name },
            );
            defer alloc.free(storage_namespace);
            candidate_bytes = self.core.index_manager.algebraicStorageNamespaceBytes(storage_namespace) catch 0;
        } else {
            const candidate_root = try std.fmt.allocPrint(alloc, "{s}/{s}", .{ self.core.index_manager.base_path, candidate[0..separator] });
            defer alloc.free(candidate_root);
            candidate_bytes = self.core.index_manager.generationStorageBytes(candidate_root) catch 0;
        }
    }
    const remaining_disk_claim = planned_disk_bytes -| @min(planned_disk_bytes, candidate_bytes);
    const manager = self.core.index_manager.resource_manager orelse return error.ResourceManagerUnavailable;
    const first_capacity = repairCapacityObservation(effective_options) catch |err| {
        try self.recordIndexRepairAttemptFailure(alloc, repair_id, @errorName(err), false);
        result.deferred = true;
        result.disk_wait = true;
        return result;
    };
    var disk_reservation = manager.reserveCapacity(
        alloc,
        repairCapacityDomain(effective_options),
        remaining_disk_claim,
        first_capacity,
        monotonicTimeNs(),
    ) catch |err| switch (err) {
        error.CapacityUnavailable, error.CapacityObservationStale => {
            try self.recordIndexRepairAttemptFailure(alloc, repair_id, "disk_admission_unavailable", false);
            result.deferred = true;
            result.disk_wait = true;
            var updated = try self.loadIndexRepairEntryById(alloc, repair_id);
            defer updated.deinit(alloc);
            result.next_retry_at_ms = updated.intent.next_retry_at_ms;
            if (entry.intent.last_error == null or
                !std.mem.eql(u8, entry.intent.last_error.?, "disk_admission_unavailable"))
            {
                std.log.info("dense generation repair waiting for disk capacity index={s} available_bytes={?} requested_growth_bytes={} retry_at_ms={} reason={s}", .{
                    entry.intent.index_name,
                    first_capacity.available_bytes,
                    remaining_disk_claim,
                    result.next_retry_at_ms,
                    @errorName(err),
                });
            }
            return result;
        },
        else => return err,
    };
    defer disk_reservation.release();
    var capacity_guard = RepairCapacityGuard{
        .reservation = &disk_reservation,
        .db = self,
        .alloc = alloc,
        .repair_id = repair_id,
        .options = effective_options,
        .admitted_total_bytes = planned_disk_bytes,
        .headroom_bytes = cleanup_and_replay_headroom,
        .algebraic_index_name = if (entry.intent.kind == .algebraic) entry.intent.index_name else null,
    };

    const attempt_count = entry.intent.attempt_count +| 1;
    try self.updateIndexRepairIntent(alloc, repair_id, .{
        .attempt_count = attempt_count,
        .next_retry_at_ms = 0,
    });
    entry.intent.attempt_count = attempt_count;
    result.attempted = true;
    var run_options = effective_options;
    run_options.capacity_check = .{
        .ptr = &capacity_guard,
        .reconcile = RepairCapacityGuard.reconcile,
        .revalidate = RepairCapacityGuard.revalidate,
        .bind_candidate_root = RepairCapacityGuard.bindCandidateRoot,
    };
    const artifact_kind = artifactRepairKindForIndexKind(entry.intent.kind) orelse {
        try self.recordIndexRepairAttemptFailure(alloc, repair_id, "unsupported_index_kind", true);
        result.terminal = true;
        return result;
    };
    const repair = self.repairArtifactIssuesWithRequestOptionsInternal(alloc, .{
        .target = .index,
        .artifact_kind = artifact_kind,
        .index_name = entry.intent.index_name,
        .limit = 1,
        .force = true,
    }, run_options, .already_held) catch |err| {
        // Fault-injection hooks model process loss, not an ordinary
        // returned repair failure. Let the test harness observe the
        // injected stop without persisting retry/backoff state that a
        // crashed process could never have written.
        if (comptime builtin.is_test) {
            if (self.shadow_index_repair_hook != null and std.mem.startsWith(u8, @errorName(err), "Test")) {
                return err;
            }
        }
        if (err == error.Canceled and run_options.cancelled()) {
            result.deferred = true;
            return result;
        }
        var current_trigger = entry.intent.trigger;
        if (err == error.RepairSourceCoverageIncomplete) {
            var current = try self.loadIndexRepairEntryById(alloc, repair_id);
            defer current.deinit(alloc);
            current_trigger = current.intent.trigger;
        }
        if (err == error.RepairSourceCoverageIncomplete and
            entry.intent.kind == .dense_vector and
            self.enrichment_runtime != null and
            (current_trigger == .operator_generation_rebuild or
                current_trigger == .storage_format_migration or
                current_trigger == .artifact_coverage_mismatch or
                current_trigger == .replay_artifact_unavailable))
        {
            // A shadow rebuild consumes stored embedding artifacts and
            // cannot repair one missing below its snapshot floor. Re-arm
            // the managed producer from primary documents first.
            const artifact_names = try index_manager_mod.denseConfigArtifactNamesAlloc(alloc, cfg_ptr.*);
            defer {
                for (artifact_names) |name| alloc.free(name);
                alloc.free(artifact_names);
            }
            var queued_refs: usize = 0;
            for (artifact_names) |artifact_name| {
                queued_refs +|= self.reprocessGeneratedEnrichmentFromStoredDocs(alloc, artifact_name) catch |reprocess_err| blk: {
                    std.log.warn(
                        "dense repair source recovery deferred index={s} artifact={s} err={s}",
                        .{ entry.intent.index_name, artifact_name, @errorName(reprocess_err) },
                    );
                    break :blk 0;
                };
            }
            if (queued_refs != 0) {
                const recovery_target = self.core.nextDerivedSequence();
                const runtime = self.enrichment_runtime.?;
                if (runtime.isStarted()) {
                    runtime.notifySequence(recovery_target);
                    runtime.waitForApplied(recovery_target) catch |drain_err| {
                        std.log.warn(
                            "dense repair source worker recovery deferred index={s} err={s}",
                            .{ entry.intent.index_name, @errorName(drain_err) },
                        );
                    };
                } else {
                    // Bounded startup/repair owners must not start a
                    // long-lived worker which can outlive and pin them.
                    runtime.catchUpUntil(recovery_target) catch |drain_err| {
                        std.log.warn(
                            "dense repair source foreground recovery deferred index={s} err={s}",
                            .{ entry.intent.index_name, @errorName(drain_err) },
                        );
                    };
                }
            }
        }
        const capacity_wait = err == error.CapacityUnavailable or err == error.CapacityObservationStale;
        // Use the same classification for live failures and persisted
        // terminal states from older binaries. Catalog admission retains
        // its existing producer outcomes; it does not re-arm failed work.
        const retryable_replacement_coverage =
            retryableIndexRepairTerminalPhase(@errorName(err), current_trigger) != null;
        const terminal_failure =
            !retryable_replacement_coverage and indexRepairFailureIsTerminal(err);
        try self.recordIndexRepairAttemptFailure(
            alloc,
            repair_id,
            if (capacity_wait) "disk_admission_unavailable" else @errorName(err),
            terminal_failure,
        );
        result.disk_wait = capacity_wait;
        if (terminal_failure) {
            result.terminal = true;
        } else {
            result.deferred = true;
            var updated = try self.loadIndexRepairEntryById(alloc, repair_id);
            defer updated.deinit(alloc);
            result.next_retry_at_ms = updated.intent.next_retry_at_ms;
        }
        return result;
    };
    result.documents_reprocessed = repair.reprocessed;
    applyCommittedRepairOutcomeToAdvance(&result, repair);

    // A committed replacement wins a cancellation observed after the
    // final pointer/checkpoint publication. Artifact debt discovered
    // during the rebuild remains in its own durable queue and must not
    // resurrect the already-completed generation intent.
    if (repair.indexes_rebuilt != 0 or repair.repaired != 0) {
        result.repaired = repair.indexes_degraded_after == 0 and !repair.debt_remaining;
        return result;
    }

    // Cooperative owner shutdown is not a failed repair attempt. Durable
    // snapshot/catch-up boundaries have already left the candidate in a
    // resumable phase, so preserve its retry schedule and let the next
    // owner continue immediately.
    if (run_options.cancelled()) {
        result.deferred = true;
        return result;
    }
    if (repair.in_progress != 0) {
        try self.updateIndexRepairIntent(alloc, repair_id, .{
            .failure_streak = 0,
            .next_retry_at_ms = 0,
            .replace_last_error = true,
        });
        result.busy = true;
        return result;
    }
    try self.recordIndexRepairAttemptFailure(alloc, repair_id, "repair_attempt_incomplete", false);
    result.deferred = true;
    var updated = try self.loadIndexRepairEntryById(alloc, repair_id);
    defer updated.deinit(alloc);
    result.next_retry_at_ms = updated.intent.next_retry_at_ms;
    return result;
}

pub fn enforceRowPolicyIntentsLocked(self: *DB, schema_view: schema_registry_mod.SchemaView, intents: []const transactions_mod.WriteIntent, principal: *const row_policy_authority_mod.Payload) !void {
    if (principal.access != .write) return error.RowPolicyAuthenticationRequired;
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
    for (intents) |intent| {
        if (isMetadataKey(intent.key)) return error.RowPolicyMutationUnsupported;
        const store_key = try encodeStoreLookupKeyAlloc(self, self.alloc, intent.key);
        defer self.alloc.free(store_key);
        const old_bytes: ?[]const u8 = try missingAsNull(probe.getLeased(store_key));
        if (old_bytes) |bytes| {
            if (try relational_store.rowSchemaVersion(bytes) != schema.version) return error.RowPolicyCatalogChanged;
            const old = if (self.core.store.valuesAreAuthenticated())
                try relational_row_codec.ordinalRowViewTrusted(bytes, schema, schema_view.physicalLayout())
            else
                try relational_row_codec.ordinalRowViewSelective(bytes, schema, schema_view.physicalLayout());
            if (!try (if (intent.value == null) delete_old else update_old).permits(old)) return error.RowPolicyDenied;
        }
        if (intent.value != null) {
            const encoded = intent.prepared_row orelse return error.RowPolicyMutationUnsupported;
            const new = try relational_row_codec.ordinalRowViewTrusted(encoded, schema, schema_view.physicalLayout());
            if (!try (if (old_bytes == null) insert_check else update_new).permits(new)) return error.RowPolicyDenied;
        }
    }
}

pub fn beginTransaction(self: *DB, timestamp_ns: u64) !transactions_mod.TxnId {
    if (comptime storage_engine.enabled) return storage_engine.call(@src().fn_name, .{ self, timestamp_ns });
    const txn_id = makeTxnId(self);
    return try self.beginTransactionWithIdAndParticipants(txn_id, timestamp_ns, &.{});
}

pub fn beginTransactionWithId(self: *DB, txn_id: transactions_mod.TxnId, timestamp_ns: u64) !transactions_mod.TxnId {
    if (comptime storage_engine.enabled) return storage_engine.call(@src().fn_name, .{ self, txn_id, timestamp_ns });
    return try self.beginTransactionWithIdAndParticipants(txn_id, timestamp_ns, &.{});
}

pub fn beginTransactionWithParticipants(self: *DB, timestamp_ns: u64, participants: []const []const u8) !transactions_mod.TxnId {
    if (comptime storage_engine.enabled) return storage_engine.call(@src().fn_name, .{ self, timestamp_ns, participants });
    const txn_id = makeTxnId(self);
    return try self.beginTransactionWithIdAndParticipants(txn_id, timestamp_ns, participants);
}

pub fn beginTransactionWithIdAndParticipants(self: *DB, txn_id: transactions_mod.TxnId, timestamp_ns: u64, participants: []const []const u8) !transactions_mod.TxnId {
    return try beginTransactionWithIdAndParticipantsCreatedAt(self, txn_id, timestamp_ns, timestamp_ns, participants);
}

pub fn beginTransactionWithIdAndParticipantsCreatedAt(
    self: *DB,
    txn_id: transactions_mod.TxnId,
    timestamp_ns: u64,
    created_at_ns: u64,
    participants: []const []const u8,
) !transactions_mod.TxnId {
    var row_policy_lease = try self.local_execution.row_policy_gate.enterRaw();
    defer row_policy_lease.release();
    var write_fence = try self.beginReplicatedWrite(.gate_then_apply);
    defer write_fence.release();
    if (self.local_execution.initial_child_hidden.load(.acquire)) return error.InitialChildNotPublished;
    if (self.local_execution.restore_staging_required.load(.acquire)) {
        var read = try self.core.store.beginReadTxn();
        defer read.abort();
        try restore_staging.requireScope(self.alloc, &read, null, false);
    }
    return try self.core.beginTransactionWithParticipantsCreatedAt(txn_id, timestamp_ns, created_at_ns, participants);
}

pub fn beginTransactionWithIdAndParticipantsCreatedAtAndRole(
    self: *DB,
    txn_id: transactions_mod.TxnId,
    timestamp_ns: u64,
    created_at_ns: u64,
    participants: []const []const u8,
    coordinator: bool,
) !transactions_mod.TxnId {
    if (comptime storage_engine.enabled) return storage_engine.call(@src().fn_name, .{ self, txn_id, timestamp_ns, created_at_ns, participants, coordinator });
    return try self.beginTransactionWithIdAndParticipantsCreatedAtRoleAndRetention(
        txn_id,
        timestamp_ns,
        created_at_ns,
        participants,
        coordinator,
        false,
    );
}

pub fn beginTransactionWithIdAndParticipantsCreatedAtRoleAndRetention(
    self: *DB,
    txn_id: transactions_mod.TxnId,
    timestamp_ns: u64,
    created_at_ns: u64,
    participants: []const []const u8,
    coordinator: bool,
    retain_terminal: bool,
) !transactions_mod.TxnId {
    return self.beginTransactionScoped(txn_id, timestamp_ns, created_at_ns, participants, coordinator, retain_terminal, null);
}

pub fn beginTransactionScoped(self: *DB, txn_id: transactions_mod.TxnId, timestamp_ns: u64, created_at_ns: u64, participants: []const []const u8, coordinator: bool, retain_terminal: bool, scope: ?[32]u8) !transactions_mod.TxnId {
    if (comptime storage_engine.enabled) return storage_engine.call(@src().fn_name, .{ self, txn_id, timestamp_ns, created_at_ns, participants, coordinator, retain_terminal, scope });
    var row_policy_lease = try self.local_execution.row_policy_gate.enterRaw();
    defer row_policy_lease.release();
    var write_fence = try self.beginReplicatedWrite(.gate_then_apply);
    defer write_fence.release();
    if (self.local_execution.initial_child_hidden.load(.acquire)) return error.InitialChildNotPublished;
    if (self.local_execution.restore_staging_required.load(.acquire) or scope != null) {
        var read = try self.core.store.beginReadTxn();
        defer read.abort();
        try restore_staging.requireMutableScope(self.alloc, &read, scope);
    }
    return try self.core.beginTransactionWithParticipantsCreatedAtRoleAndRetention(
        txn_id,
        timestamp_ns,
        created_at_ns,
        participants,
        coordinator,
        retain_terminal,
    );
}

pub fn beginReplicatedTransactionAtOrderedReceipt(
    self: *DB,
    txn_id: transactions_mod.TxnId,
    timestamp_ns: u64,
    created_at_ns: u64,
    participants: []const []const u8,
    coordinator: bool,
    retain_terminal: bool,
    identity: OrderedApplyReceipt,
) !transactions_mod.TxnId {
    if (comptime storage_engine.enabled) return storage_engine.call(@src().fn_name, .{ self, txn_id, timestamp_ns, created_at_ns, participants, coordinator, retain_terminal, identity });
    return self.beginReplicatedTransactionScoped(txn_id, timestamp_ns, created_at_ns, participants, coordinator, retain_terminal, identity, null);
}

pub fn beginReplicatedTransactionScoped(self: *DB, txn_id: transactions_mod.TxnId, timestamp_ns: u64, created_at_ns: u64, participants: []const []const u8, coordinator: bool, retain_terminal: bool, identity: OrderedApplyReceipt, scope: ?[32]u8) !transactions_mod.TxnId {
    if (comptime storage_engine.enabled) return storage_engine.call(@src().fn_name, .{ self, txn_id, timestamp_ns, created_at_ns, participants, coordinator, retain_terminal, identity, scope });
    lockApply(self);
    defer self.core.unlockApply();
    if (self.local_execution.initial_child_hidden.load(.acquire)) return error.InitialChildNotPublished;
    switch (try orderedApplyDisposition(try readOrderedApplyReceipt(self.alloc, self.core.store), identity)) {
        .already_applied => return txn_id,
        .apply => {},
    }
    if (self.local_execution.restore_staging_required.load(.acquire) or scope != null) {
        var read = try self.core.store.beginReadTxn();
        defer read.abort();
        try restore_staging.requireMutableScope(self.alloc, &read, scope);
    }
    var value_buf: [ordered_apply_receipt_value_len]u8 = undefined;
    const marker = orderedApplyReceiptWrite(identity, &value_buf);
    return try self.core.beginTransactionWithParticipantsCreatedAtRoleAndRetentionExtraBatch(
        txn_id,
        timestamp_ns,
        created_at_ns,
        participants,
        coordinator,
        retain_terminal,
        .{ .writes = &.{marker} },
    );
}

pub fn writeIntents(
    self: *DB,
    txn_id: transactions_mod.TxnId,
    intents: []const transactions_mod.WriteIntent,
    predicates: []const transactions_mod.VersionPredicate,
) !void {
    if (comptime storage_engine.enabled) return storage_engine.call(@src().fn_name, .{ self, txn_id, intents, predicates });
    if (openModeRequiresReadOnlyBackends(self.open_mode)) return error.ReadOnly;
    var write_fence = try self.beginReplicatedWrite(.gate);
    defer write_fence.release();
    var preparation: RequestPreparationContext = undefined;
    preparation.init(self);
    defer preparation.deinit();
    var retries: usize = 0;
    while (true) {
        writeIntentsPreparedOnce(self, txn_id, intents, predicates, preparation.guard.allocator()) catch |err| switch (err) {
            error.PreparedGenerationChanged => {
                if (retries >= 2) return err;
                retries += 1;
                continue;
            },
            else => return preparation.mapError(err),
        };
        return;
    }
}

pub fn writeIntentsPreparedOnce(
    self: *DB,
    txn_id: transactions_mod.TxnId,
    intents: []const transactions_mod.WriteIntent,
    predicates: []const transactions_mod.VersionPredicate,
    preparation_alloc: Allocator,
) !void {
    const schema_namespace = self.core.schemaNamespaceGeneration();
    const prepared_intents = try preparation_alloc.dupe(transactions_mod.WriteIntent, intents);
    defer preparation_alloc.free(prepared_intents);
    for (prepared_intents) |*intent| intent.prepared_row = null;
    defer freePreparedIntentRows(preparation_alloc, prepared_intents);
    var identity_upsert_keys = std.ArrayListUnmanaged([]const u8).empty;
    defer identity_upsert_keys.deinit(preparation_alloc);
    for (intents) |intent| {
        if (intent.value == null or isMetadataKey(intent.key)) continue;
        try identity_upsert_keys.append(preparation_alloc, intent.key);
    }
    const binding = try self.core.transactionSchemaBinding(preparation_alloc, txn_id);
    var view = try self.acquireTransactionSchemaView(preparation_alloc, binding);
    defer if (view) |*pinned| pinned.release();
    for (intents) |intent| {
        if (isProtectedIntegrityKey(intent.key) or isProtectedRangeWriteKey(intent.key)) return error.InvalidIntegrityOperation;
        if (hasCoordinatedConstraints(view) and !isMetadataKey(intent.key)) return error.ForeignKeyCoordinationRequired;
    }
    for (predicates) |predicate| if (isProtectedIntegrityKey(predicate.key)) return error.InvalidIntegrityOperation;
    try self.prepareTransactionRows(preparation_alloc, prepared_intents, view);

    try self.lockApplyForPortableRuntime();
    defer self.core.unlockApply();
    if (self.local_execution.initial_child_hidden.load(.acquire)) return error.InitialChildNotPublished;
    if (schema_namespace != self.core.schemaNamespaceGeneration()) return error.PreparedGenerationChanged;
    if (binding == null) try self.validatePreparedSchemaViewLocked(view);
    {
        var topology_manager = try self.core.initTxnManager();
        defer topology_manager.deinit();
        var topology_read = try self.core.store.beginProbeTxn();
        defer topology_read.abort();
        if (self.local_execution.restore_staging_required.load(.acquire)) try restore_staging.requireScope(preparation_alloc, &topology_read, null, false);
        try relational_integrity_topology.admitPrepare(&topology_read, &topology_manager, preparation_alloc, txn_id);
    }
    try self.failIfIdentityOrdinalExhaustedForNewUpserts(identity_upsert_keys.items);
    const index_spans = try self.collectIndexSpanReservations(preparation_alloc, prepared_intents, view);
    defer if (index_spans.len != 0) preparation_alloc.free(index_spans);
    try self.prepareOnlineVectorIntentBoundsLocked(preparation_alloc, prepared_intents, view);
    var guarded_predicates = std.ArrayListUnmanaged(transactions_mod.VersionPredicate).empty;
    defer guarded_predicates.deinit(preparation_alloc);
    try guarded_predicates.appendSlice(preparation_alloc, predicates);
    var endpoint_dependencies = std.heap.ArenaAllocator.init(preparation_alloc);
    defer endpoint_dependencies.deinit();
    try self.prepareGraphEndpointDependenciesLocked(preparation_alloc, endpoint_dependencies.allocator(), txn_id, prepared_intents, view, &guarded_predicates);
    try self.core.writeIntentsExtraBatch(txn_id, prepared_intents, guarded_predicates.items, .{
        .preparation_allocator = preparation_alloc,
        .schema_binding = .{ .version = if (view) |pinned| pinned.version() else null },
        .index_span_digests = index_spans,
    });
}

pub fn writeTransaction(self: *DB, txn_id: types.TxnId, req: types.TransactionIntentRequest) !void {
    if (comptime storage_engine.enabled) return storage_engine.call(@src().fn_name, .{ self, txn_id, req });
    if (self.local_execution.initial_child_hidden.load(.acquire)) return error.InitialChildNotPublished;
    if (openModeRequiresReadOnlyBackends(self.open_mode)) return error.ReadOnly;
    var write_fence = try self.beginReplicatedWrite(.gate);
    defer write_fence.release();
    try writeTransactionInternal(self, txn_id, req, null);
}

pub fn writeReplicatedTransactionAtOrderedReceipt(
    self: *DB,
    txn_id: types.TxnId,
    req: types.TransactionIntentRequest,
    identity: OrderedApplyReceipt,
) !void {
    if (comptime storage_engine.enabled) return storage_engine.call(@src().fn_name, .{ self, txn_id, req, identity });
    if (openModeRequiresReadOnlyBackends(self.open_mode)) return error.ReadOnly;
    try writeTransactionInternal(self, txn_id, req, identity);
}

pub fn writeTransactionInternal(
    self: *DB,
    txn_id: types.TxnId,
    req: types.TransactionIntentRequest,
    ordered_receipt: ?OrderedApplyReceipt,
) !void {
    if (ordered_receipt == null) try self.maybeFinalizePendingRowPolicyPublication();
    var verified_principal: ?std.json.Parsed(row_policy_authority_mod.Payload) = null;
    defer if (verified_principal) |*principal| principal.deinit();
    var row_policy_lease: ?row_policy_gate_mod.Gate.Lease = null;
    if (req.row_policy_principal_proof.len != 0) {
        if (req.row_policy_database.len == 0 or req.row_policy_admitted_at_seconds <= 0 or
            req.restore_staging_scope != null) return error.RowPolicyAuthenticationRequired;
        const now_seconds: i64 = @intCast(@divFloor(platform_time.realtimeNs(), std.time.ns_per_s));
        verified_principal = try self.verifyRowPolicyPrincipal(self.alloc, req.row_policy_principal_proof, req.row_policy_database, .write, if (ordered_receipt != null) req.row_policy_admitted_at_seconds else now_seconds);
        row_policy_lease = if (ordered_receipt != null)
            try self.local_execution.row_policy_gate.enterReplicatedPrincipal(&verified_principal.?.value)
        else
            try self.local_execution.row_policy_gate.enterVerifiedPrincipal(&verified_principal.?.value, now_seconds);
    } else if (req.row_policy_database.len != 0 or req.row_policy_admitted_at_seconds != 0) {
        return error.RowPolicyAuthenticationRequired;
    } else if (ordered_receipt == null) {
        row_policy_lease = try self.local_execution.row_policy_gate.enterRaw();
    } else if (self.local_execution.row_policy_gate.currentPhase() != .disabled and
        (req.writes.len != 0 or req.deletes.len != 0 or req.transforms.len != 0))
    {
        return error.RowPolicyAuthenticationRequired;
    }
    defer if (row_policy_lease) |*lease| lease.release();
    var preparation: RequestPreparationContext = undefined;
    preparation.init(self);
    defer preparation.deinit();
    const max_prepared_retries = 2;
    var retries: usize = 0;
    while (true) {
        writeTransactionInternalOnce(self, txn_id, req, ordered_receipt, preparation.guard.allocator(), if (verified_principal) |*principal| &principal.value else null, if (row_policy_lease) |*lease| lease else null) catch |err| switch (err) {
            error.PreparedGenerationChanged, error.PreparedReadSetChanged => {
                if (retries >= max_prepared_retries) return err;
                retries += 1;
                continue;
            },
            else => return preparation.mapError(err),
        };
        return;
    }
}

pub fn writeTransactionInternalOnce(
    self: *DB,
    txn_id: types.TxnId,
    req: types.TransactionIntentRequest,
    ordered_receipt: ?OrderedApplyReceipt,
    preparation_alloc: Allocator,
    row_policy_principal: ?*const row_policy_authority_mod.Payload,
    row_policy_lease: ?*const row_policy_gate_mod.Gate.Lease,
) !void {
    if (req.relational_index_maintenance != null and
        (req.writes.len != 0 or req.deletes.len != 0 or req.transforms.len != 0 or
            req.predicates.len != 0 or req.integrity.len != 0 or req.integrity_commands.len != 0 or
            req.relational_activation != null or req.relational_retirement != null or
            req.relational_repair or req.restore_staging_scope != null)) return error.InvalidBatchRequest;
    const schema_namespace = self.core.schemaNamespaceGeneration();
    // Parse and expand transforms from one optimistic read snapshot before
    // entering the serialized commit section. The commit fence below
    // validates both that read set and the immutable schema epoch.
    var transform_snapshot = try self.captureTransformReadSnapshot(
        preparation_alloc,
        types.TransactionWrite,
        req.writes,
        req.deletes,
        req.transforms,
    );
    defer transform_snapshot.deinit(preparation_alloc);

    var effective_ops = try coalesceKeyValueRequest(
        self,
        preparation_alloc,
        types.TransactionWrite,
        req.writes,
        req.deletes,
        req.transforms,
        &transform_snapshot,
    );
    defer effective_ops.deinit(preparation_alloc);
    // Graph projections are maintained as artifact deltas, while the
    // transaction intent format currently carries only primary key/value
    // rows. Refuse this combination rather than acknowledging a transform
    // whose graph half cannot participate in the transaction commit.
    if (effective_ops.graph_writes.items.len != 0 or effective_ops.graph_deletes.items.len != 0)
        return error.UnsupportedTransformOperation;

    var intents = std.ArrayListUnmanaged(transactions_mod.WriteIntent).empty;
    defer intents.deinit(preparation_alloc);
    var predicates = std.ArrayListUnmanaged(transactions_mod.VersionPredicate).empty;
    defer predicates.deinit(preparation_alloc);

    for (effective_ops.writes) |write| {
        if (isProtectedIntegrityKey(write.key) or isProtectedRangeWriteKey(write.key)) return error.InvalidIntegrityOperation;
        try intents.append(preparation_alloc, .{
            .key = write.key,
            .value = write.value,
            .json_null_fields = write.json_null_fields,
        });
    }
    for (effective_ops.deletes) |key| {
        if (isProtectedIntegrityKey(key) or isProtectedRangeWriteKey(key)) return error.InvalidIntegrityOperation;
        try intents.append(preparation_alloc, .{
            .key = key,
            .value = null,
        });
    }
    for (req.predicates) |predicate| {
        if (isProtectedIntegrityKey(predicate.key)) return error.InvalidIntegrityOperation;
        try predicates.append(preparation_alloc, .{
            .key = predicate.key,
            .expected_version = predicate.expected_version,
            .expected_content_digest = predicate.expected_content_digest,
            .unique_absence = predicate.unique_absence,
        });
    }

    // These internal observations address this exact owner's metadata,
    // never document keyspace. The coordinator routes them by owner fence.
    const tracking = range_protection_mod;
    if (req.range_guards.len > range_protection_mod.max_proofs) return error.TransactionTooLarge;
    const range_keys = try preparation_alloc.alloc([tracking.counter_prefix.len + 2]u8, req.range_guards.len);
    defer preparation_alloc.free(range_keys);
    const index_range_keys = try preparation_alloc.alloc([tracking.index_counter_prefix.len + tracking.index_span_digest_bytes]u8, req.range_guards.len);
    defer preparation_alloc.free(index_range_keys);
    const range_values = try preparation_alloc.alloc([8]u8, req.range_guards.len);
    defer preparation_alloc.free(range_values);
    var observed_buckets = std.StaticBitSet(tracking.bucket_count).empty;
    for (req.range_guards, 0..) |guard, i| {
        tracking.validateProof(guard) catch return error.InvalidBatchRequest;
        const key: []const u8 = if (guard.index) |span| blk: {
            index_range_keys[i] = tracking.indexCounterKey(span.digest);
            break :blk &index_range_keys[i];
        } else blk: {
            if (observed_buckets.isSet(guard.bucket)) return error.InvalidBatchRequest;
            observed_buckets.set(guard.bucket);
            range_keys[i] = tracking.counterKey(guard.bucket);
            break :blk &range_keys[i];
        };
        if (guard.generation) |generation| std.mem.writeInt(u64, &range_values[i], generation, .little);
        try predicates.append(preparation_alloc, .{
            .key = key,
            .expected_version = 0,
            .comparison = .exact_value,
            .expected_value = if (guard.generation != null) &range_values[i] else null,
        });
    }

    var identity_upsert_keys = std.ArrayListUnmanaged([]const u8).empty;
    defer identity_upsert_keys.deinit(preparation_alloc);
    for (intents.items) |intent| {
        if (intent.value == null or isMetadataKey(intent.key)) continue;
        try identity_upsert_keys.append(preparation_alloc, intent.key);
    }

    const binding = try self.core.transactionSchemaBinding(preparation_alloc, txn_id);
    var prepared_schema_view = try self.acquireTransactionSchemaView(preparation_alloc, binding);
    defer if (prepared_schema_view) |*view| view.release();
    if (req.schema_version) |version| {
        const view = prepared_schema_view orelse return error.PreparedGenerationChanged;
        if (view.version() != version) return error.PreparedGenerationChanged;
    }
    if ((hasCoordinatedConstraints(prepared_schema_view) or req.relational_repair) and
        (req.relational_schema_version == null or req.relational_integrity_generation_set == null) and
        (effective_ops.writes.len != 0 or effective_ops.deletes.len != 0)) return error.ForeignKeyCoordinationRequired;
    if (req.relational_schema_version) |version| {
        const view = prepared_schema_view orelse return error.InvalidRelationalRowsRequest;
        if (view.storageMode() != .relational) return error.InvalidRelationalRowsRequest;
        if (view.version() != version) return error.PreparedGenerationChanged;
    }
    // Exact index-span proofs remain valid only while the observed READY
    // generation is the current catalog generation. Fence all three
    // pieces of that state in the same prepare as the span counter.
    var range_metadata_buffers: std.ArrayList([]u8) = .empty;
    defer {
        for (range_metadata_buffers.items) |bytes| preparation_alloc.free(bytes);
        range_metadata_buffers.deinit(preparation_alloc);
    }
    const CaptureMetadata = struct {
        fn add(alloc: Allocator, buffers: *std.ArrayList([]u8), output: *std.ArrayListUnmanaged(transactions_mod.VersionPredicate), read: anytype, key: []const u8) !void {
            try buffers.ensureUnusedCapacity(alloc, 2);
            const owned_key = try alloc.dupe(u8, key);
            buffers.appendAssumeCapacity(owned_key);
            const raw = try missingAsNull(read.get(key));
            const value = if (raw) |bytes| try alloc.dupe(u8, bytes) else null;
            if (value) |bytes| buffers.appendAssumeCapacity(bytes);
            try output.append(alloc, .{ .key = owned_key, .comparison = .exact_value, .expected_value = value });
        }
    };
    var guarded_indexes: std.AutoHashMapUnmanaged([tracking.index_id_bytes]u8, void) = .empty;
    defer guarded_indexes.deinit(preparation_alloc);
    var index_plan: ?relational_index_catalog.WriteSnapshot = null;
    defer if (index_plan) |*plan| plan.deinit();
    var index_read: ?docstore_mod.DocStore.Txn = null;
    defer if (index_read) |*read| read.abort();
    for (req.range_guards) |guard| {
        const span = guard.index orelse continue;
        const seen = try guarded_indexes.getOrPut(preparation_alloc, span.id);
        if (seen.found_existing) continue;
        if (index_plan == null) {
            index_plan = self.core.relational_indexes.acquire() orelse return error.PreparedGenerationChanged;
            index_read = try self.core.store.beginProbeTxn();
            const head_raw = try index_read.?.get(relational_index_catalog.head_key);
            if (!(try relational_index_catalog.Head.decode(head_raw)).eql(index_plan.?.head)) return error.PreparedGenerationChanged;
            try CaptureMetadata.add(preparation_alloc, &range_metadata_buffers, &predicates, &index_read.?, relational_index_catalog.head_key);
        }
        const index = for (index_plan.?.plan.boundIndexes()) |candidate| {
            const candidate_id = candidate.id().encode();
            if (std.mem.eql(u8, &candidate_id, &span.id)) break candidate;
        } else return error.PreparedGenerationChanged;
        if ((try relational_index_jobs.status(&index_read.?, index)).state != .ready) return error.RelationalIndexNotReady;
        const progress_key = relational_index_jobs.progressKey(index.id());
        const control_key = relational_index_maintenance_contract.controlKey(index.id());
        try CaptureMetadata.add(preparation_alloc, &range_metadata_buffers, &predicates, &index_read.?, &progress_key);
        try CaptureMetadata.add(preparation_alloc, &range_metadata_buffers, &predicates, &index_read.?, &control_key);
    }
    defer freePreparedIntentRows(preparation_alloc, intents.items);
    try self.prepareTransactionRows(preparation_alloc, intents.items, prepared_schema_view);

    const integrity_catalog_mod = relational_integrity_catalog;
    var integrity_catalog: ?integrity_catalog_mod.Catalog = null;
    var integrity_catalog_digest: ?[32]u8 = null;
    defer if (integrity_catalog) |*catalog| catalog.deinit();
    if (req.integrity.len != 0 or req.integrity_commands.len != 0 or req.relational_activation != null or req.relational_retirement != null or req.relational_repair or hasCoordinatedConstraints(prepared_schema_view)) {
        const raw_catalog = (try self.core.getStoreValue(preparation_alloc, integrity_catalog_mod.key)) orelse return error.IntegrityCatalogChanged;
        defer preparation_alloc.free(raw_catalog);
        integrity_catalog = try integrity_catalog_mod.decode(preparation_alloc, raw_catalog);
        var digest: [32]u8 = undefined;
        std.crypto.hash.Blake3.hash(raw_catalog, &digest, .{});
        integrity_catalog_digest = digest;
        if (req.relational_integrity_generation_set) |expected| {
            const actual = relational_integrity_activation.generationSet(integrity_catalog.?);
            if (!std.mem.eql(u8, &expected, &actual)) return error.IntegrityCatalogChanged;
        }
        const view = prepared_schema_view orelse return error.IntegrityCatalogChanged;
        if (integrity_catalog.?.schema_version != view.version()) return error.PreparedGenerationChanged;
        const incarnation = try integrity_catalog_mod.incarnationFromTableId(self.core.identity_namespace.table_id);
        if (!std.mem.eql(u8, &incarnation, &integrity_catalog.?.incarnation)) return error.IntegrityCatalogIncarnationMismatch;
        for (req.integrity_commands) |command| {
            const generation = integrity_catalog.?.findGeneration(command.address.generation) orelse return error.IntegrityCatalogChanged;
            if (generation.definition.kind != .unique) return error.InvalidIntegrityOperation;
            if (generation.retired and (command.operation == .establish or command.operation == .attach)) return error.IntegrityCatalogChanged;
        }
    }

    try self.lockApplyForPortableRuntime();
    defer self.core.unlockApply();
    if (self.local_execution.initial_child_hidden.load(.acquire)) return error.InitialChildNotPublished;
    if (schema_namespace != self.core.schemaNamespaceGeneration()) return error.PreparedGenerationChanged;
    if (binding == null) try self.validatePreparedSchemaViewLocked(prepared_schema_view);
    if (row_policy_principal) |principal| {
        const view = prepared_schema_view orelse return error.RowPolicyCatalogChanged;
        try enforceRowPolicyIntentsLocked(self, view, intents.items, principal);
    }
    const integrity_mod = relational_integrity;
    const activation_mod = relational_integrity_activation;
    var repair_checkpoint: ?[]const u8 = null;
    defer if (repair_checkpoint) |bytes| preparation_alloc.free(bytes);
    var restore_checkpoint: ?[]const u8 = null;
    defer if (restore_checkpoint) |bytes| preparation_alloc.free(bytes);
    var topology_manager = try self.core.initTxnManager();
    defer topology_manager.deinit();
    {
        var topology_read = try self.core.store.beginProbeTxn();
        defer topology_read.abort();
        try restore_staging.requireMutableScope(preparation_alloc, &topology_read, req.restore_staging_scope);
        if (req.restore_staging_scope != null and (effective_ops.writes.len != 0 or effective_ops.deletes.len != 0 or req.relational_repair or req.relational_retirement != null)) return error.InvalidRestoreStagingCommand;
        if (req.restore_staging_scope != null) {
            const checkpoint = (try restore_staging.optional(&topology_read)) orelse return error.RestoreStagingScopeChanged;
            restore_checkpoint = try preparation_alloc.dupe(u8, checkpoint);
            try predicates.append(preparation_alloc, .{ .key = restore_staging.key, .comparison = .exact_value, .expected_value = restore_checkpoint });
        }
        try relational_integrity_topology.admitPrepare(&topology_read, &topology_manager, preparation_alloc, txn_id);
    }
    var maintenance_preparation: ?relational_index_maintenance.Prepared = null;
    defer if (maintenance_preparation) |*value| value.deinit();
    if (req.relational_index_maintenance) |command| {
        const view = prepared_schema_view orelse return error.RelationalTableRequired;
        if (view.version() != command.schema_version) return error.PreparedGenerationChanged;
        var plan = self.core.relational_indexes.acquire() orelse return error.PreparedGenerationChanged;
        defer plan.deinit();
        if (plan.plan.schemaView().epoch != view.epoch) return error.PreparedGenerationChanged;
        var maintenance_read = try self.core.store.beginProbeTxn();
        defer maintenance_read.abort();
        maintenance_preparation = try relational_index_maintenance.prepareCommand(
            preparation_alloc,
            &maintenance_read,
            plan.plan,
            self.core.identity_namespace.table_id,
            command,
        );
        try intents.append(preparation_alloc, maintenance_preparation.?.intent);
        try predicates.append(preparation_alloc, maintenance_preparation.?.predicate);
    }
    var integrity_effects: ?integrity_mod.Effects = null;
    defer if (integrity_effects) |*effects| effects.deinit();
    if (req.integrity.len != 0 or req.integrity_commands.len != 0 or req.relational_activation != null or req.relational_retirement != null or integrity_catalog != null) {
        var integrity_read = try integrity_mod.CurrentView.initForTransaction(self.core.store, preparation_alloc, txn_id);
        defer integrity_read.deinit();
        // Shadow claims must remain unobservable even to a participant
        // whose stale route happens to name this receiver. Check before
        // any claim/reference lookup or semantic preparation occurs.
        for (req.integrity_commands) |command| try online_integrity_shadow.requireServing(&integrity_read, self.core.byteRange(), &command.address.routing);
        for (req.integrity) |operation| try online_integrity_shadow.requireServing(&integrity_read, self.core.byteRange(), operation.routing_key);
        // Layout epochs alone do not fence changes to UNIQUE/FK declarations.
        // Compare the exact catalog observed during preparation under apply.
        const current_catalog = integrity_read.get(integrity_catalog_mod.key) catch |err| switch (err) {
            error.NotFound => return error.IntegrityCatalogChanged,
            else => return err,
        };
        var current_digest: [32]u8 = undefined;
        std.crypto.hash.Blake3.hash(current_catalog, &current_digest, .{});
        if (!std.mem.eql(u8, &current_digest, &integrity_catalog_digest.?)) return error.IntegrityCatalogChanged;
        if (req.relational_retirement) |command| {
            if (effective_ops.writes.len != 0 or effective_ops.deletes.len != 0 or req.relational_activation != null or req.relational_repair) return error.InvalidConstraintRetirementCommand;
            try online_integrity_shadow.requireCatalogMutable(&integrity_read);
            try online_integrity_shadow.requireServing(&integrity_read, self.core.byteRange(), command.routing_key);
            const retirement = try relational_integrity_retirement.prepareCommand(preparation_alloc, &integrity_read, integrity_catalog.?, command);
            const after = try relational_integrity_retirement.Progress.decode(command.next);
            if (command.expected) |expected| {
                const before = try relational_integrity_retirement.Progress.decode(expected);
                if (binding == null and before.phase == .fenced and after.phase != .fenced and try topology_manager.hasSchemaLeases()) return error.SchemaInUse;
            }
            try intents.append(preparation_alloc, retirement.intent);
            try predicates.append(preparation_alloc, retirement.predicate);
        }
        if (req.relational_activation) |command| {
            try relational_integrity_retirement.requireMutable(&integrity_read);
            try online_integrity_shadow.requireCatalogMutable(&integrity_read);
            // A backfill page changes claims and its durable cursor, never
            // primary rows. Its observed rows are retained read guards.
            if (effective_ops.writes.len != 0 or effective_ops.deletes.len != 0) return error.InvalidConstraintActivationCommand;
            try online_integrity_shadow.requireServing(&integrity_read, self.core.byteRange(), command.routing_key);
            const activation = try activation_mod.prepareCommand(preparation_alloc, &integrity_read, integrity_catalog.?, command);
            try intents.append(preparation_alloc, activation.intent);
            try predicates.append(preparation_alloc, activation.predicate);
        } else if (effective_ops.writes.len != 0 or effective_ops.deletes.len != 0) {
            try relational_integrity_retirement.requireMutable(&integrity_read);
            if (req.relational_repair) {
                const repair_guard = try activation_mod.repairPredicate(preparation_alloc, &integrity_read, integrity_catalog.?);
                repair_checkpoint = repair_guard.expected_value;
                try predicates.append(preparation_alloc, repair_guard);
            } else try activation_mod.requireReady(&integrity_read, integrity_catalog.?);
        }
        if (req.integrity.len != 0 and req.integrity_commands.len != 0) return error.InvalidIntegrityOperation;
        if (req.integrity.len != 0) try relational_integrity_retirement.requireMutable(&integrity_read);
        try relational_integrity_retirement.admitCommands(&integrity_read, req.integrity_commands);
        const integrity_operations = if (req.integrity_commands.len != 0) operations: {
            integrity_effects = try integrity_mod.prepare(preparation_alloc, &integrity_read, req.integrity_commands);
            break :operations integrity_effects.?.operations;
        } else req.integrity;
        try integrity_mod.validatePreparedEffects(preparation_alloc, &integrity_read, integrity_operations);
        for (integrity_operations) |operation| {
            try online_integrity_shadow.requireServing(&integrity_read, self.core.byteRange(), operation.routing_key);
            const parsed_key = try integrity_mod.parseKey(operation.key);
            const generation = integrity_catalog.?.findGeneration(parsed_key.address.generation) orelse return error.IntegrityCatalogChanged;
            if (generation.definition.kind != .unique) return error.InvalidIntegrityOperation;
            if (req.integrity_commands.len == 0 and generation.retired and operation.kind != .delete) return error.IntegrityCatalogChanged;
            try predicates.append(preparation_alloc, integrity_mod.predicate(operation));
            if (operation.kind != .guard) try intents.append(preparation_alloc, .{ .key = operation.key, .value = operation.value });
        }
    }
    try self.validateTransformReadSnapshot(transform_snapshot);
    try self.failIfIdentityOrdinalExhaustedForNewUpserts(identity_upsert_keys.items);
    const index_spans = try self.collectIndexSpanReservations(preparation_alloc, intents.items, prepared_schema_view);
    defer if (index_spans.len != 0) preparation_alloc.free(index_spans);
    try self.prepareOnlineVectorIntentBoundsLocked(preparation_alloc, intents.items, prepared_schema_view);
    var endpoint_dependencies = std.heap.ArenaAllocator.init(preparation_alloc);
    defer endpoint_dependencies.deinit();
    try self.prepareGraphEndpointDependenciesLocked(preparation_alloc, endpoint_dependencies.allocator(), txn_id, intents.items, prepared_schema_view, &predicates);
    if (row_policy_lease) |lease| try lease.checkAt(@intCast(@divFloor(platform_time.realtimeNs(), std.time.ns_per_s)));
    if (ordered_receipt) |identity| {
        switch (try orderedApplyDisposition(try readOrderedApplyReceipt(preparation_alloc, self.core.store), identity)) {
            .already_applied => return,
            .apply => {},
        }
        var value_buf: [ordered_apply_receipt_value_len]u8 = undefined;
        const marker = orderedApplyReceiptWrite(identity, &value_buf);
        try self.core.writeIntentsExtraBatch(
            txn_id,
            intents.items,
            predicates.items,
            .{ .writes = &.{marker}, .schema_binding = .{ .version = if (prepared_schema_view) |view| view.version() else null }, .preparation_allocator = preparation_alloc, .index_span_digests = index_spans },
        );
    } else {
        try self.core.writeIntentsExtraBatch(txn_id, intents.items, predicates.items, .{
            .preparation_allocator = preparation_alloc,
            .schema_binding = .{ .version = if (prepared_schema_view) |view| view.version() else null },
            .index_span_digests = index_spans,
        });
    }
}

pub fn freePreparedIntentRows(alloc: Allocator, intents: []const transactions_mod.WriteIntent) void {
    for (intents) |intent| if (intent.prepared_row) |row| {
        alloc.free(@constCast(row));
        alloc.free(@constCast(intent.value.?));
    };
}

/// Called under apply after row preparation. Prior reverse companions are
/// the authoritative old tuples, including retired generations; the pinned
/// plan produces candidate tuples from canonical AROW without reparsing JSON.
/// Reserve both sides before publishing a primary intent so an empty index
/// equality read cannot pass between prepare and index-effect commit.
/// The catalog cannot change while direct-vector retention is active.
/// Compute server-owned conservative bounds before voting prepared; the
/// persisted replacement-aware ledger also counts artifact keys so commit
/// cannot exceed the retained frame's independent key-count limit.
pub fn prepareOnlineVectorIntentBoundsLocked(self: *DB, alloc: Allocator, intents: []transactions_mod.WriteIntent, schema_view: ?schema_registry_mod.SchemaView) !void {
    for (intents) |*intent| {
        intent.retained_artifact_bytes = 0;
        intent.retained_artifact_keys = 0;
    }
    var read = try self.core.store.beginReadTxn();
    defer read.abort();
    const retention = (try retained_effects_mod.load(&read)) orelse return;
    if (!retention.active() or !retention.direct_vectors) return;
    for (intents) |*intent| {
        if (isMetadataKey(intent.key)) continue;
        if (intent.value) |value| {
            var prepared: ?mapper.PreparedRelationalWrite = if (intent.prepared_row != null) prepared_row: {
                const view = schema_view orelse return error.PreparedGenerationChanged;
                break :prepared_row try mapper.PreparedRelationalWrite.initFromIntent(alloc, intent.key, value, view.validator(), view.tableSchema().*, view.physicalLayout(), intent.prepared_row);
            } else null;
            defer if (prepared) |*row| row.deinit(alloc);
            var extracted = if (prepared) |*row| row.takeExtracted() else try mapper.extractWrite(alloc, intent.key, value);
            defer extracted.deinit(alloc);
            if (prepared) |*row| {
                const view = schema_view.?;
                try self.core.index_manager.appendIndexFieldEmbeddingsFromPreparedToExtractedWrite(alloc, intent.key, row.parsedValue(), try row.typedView(view.tableSchema().*, view.physicalLayout()), &extracted);
            } else try self.core.index_manager.appendIndexFieldEmbeddingsToExtractedWrite(alloc, intent.key, value, &extracted);
            inline for (.{ extracted.dense_embeddings, extracted.sparse_embeddings }) |embeddings| {
                for (embeddings) |embedding| {
                    const numbers = if (@hasField(@TypeOf(embedding), "vector")) embedding.vector else embedding.values;
                    for (numbers) |number| if (!std.math.isFinite(number)) return error.InvalidBatchRequest;
                    const key = try internal_keys.embeddingArtifactKeyForDocumentAlloc(alloc, intent.key, embedding.index_name);
                    defer alloc.free(key);
                    const vector_bytes = if (@hasField(@TypeOf(embedding), "vector"))
                        std.math.mul(u64, embedding.vector.len, 4) catch return error.TransactionTooLarge
                    else
                        std.math.mul(u64, embedding.indices.len, 8) catch return error.TransactionTooLarge;
                    const bytes = std.math.add(u64, 16 + key.len + enrichment_artifact_codec.header_len + 4, vector_bytes) catch return error.TransactionTooLarge;
                    intent.retained_artifact_bytes = std.math.add(u64, intent.retained_artifact_bytes, bytes) catch return error.TransactionTooLarge;
                    intent.retained_artifact_keys += 1;
                }
            }
            // Explicit nulls publish vector tombstones in the retained
            // source frame just like a document delete. Reserve both their
            // encoded keys and bytes before voting prepared, even when the
            // current artifact is absent (it may be replaced before apply).
            for (extracted.deleted_embedding_indexes) |name| {
                const key = try internal_keys.embeddingArtifactKeyForDocumentAlloc(alloc, intent.key, name);
                defer alloc.free(key);
                intent.retained_artifact_bytes = std.math.add(u64, intent.retained_artifact_bytes, 16 + key.len) catch return error.TransactionTooLarge;
                intent.retained_artifact_keys += 1;
            }
        } else {
            const prefix = try internal_keys.artifactRootPrefixAlloc(alloc, intent.key);
            defer alloc.free(prefix);
            var cursor = try read.openCursor();
            defer cursor.close();
            var entry = try cursor.seekAtOrAfter(prefix);
            var inspected: usize = 0;
            while (entry) |record| : (entry = try cursor.next()) {
                if (!std.mem.startsWith(u8, record.key, prefix)) break;
                inspected += 1;
                if (inspected > retained_effects_mod.max_keys) return error.TransactionTooLarge;
                if (!internal_keys.isEmbeddingArtifactKey(record.key)) continue;
                intent.retained_artifact_bytes = std.math.add(u64, intent.retained_artifact_bytes, 16 + record.key.len) catch return error.TransactionTooLarge;
                intent.retained_artifact_keys += 1;
            }
        }
    }
}

pub fn commitTransaction(self: *DB, txn_id: transactions_mod.TxnId, timestamp_ns: u64) !void {
    if (comptime storage_engine.enabled) return storage_engine.call(@src().fn_name, .{ self, txn_id, timestamp_ns });
    try self.resolveTransactionIntents(txn_id, .committed, timestamp_ns);
}

pub fn resolveTransactionIntents(self: *DB, txn_id: transactions_mod.TxnId, status: transactions_mod.TxnStatus, commit_version: u64) !void {
    if (comptime storage_engine.enabled) return storage_engine.call(@src().fn_name, .{ self, txn_id, status, commit_version });
    try self.resolveTransactionIntentsWithSyncLevel(txn_id, status, commit_version, .propose);
}

pub fn resolveTransactionIntentsWithSyncLevel(
    self: *DB,
    txn_id: transactions_mod.TxnId,
    status: transactions_mod.TxnStatus,
    commit_version: u64,
    sync_level: types.SyncLevel,
) !void {
    try self.resolveTransactionIntentsWithSyncLevelAndCancellation(txn_id, status, commit_version, sync_level, .none);
}

/// Cancellation applies only to the derived-visibility barrier after the
/// transaction decision and primary mutation are durable. It must never
/// turn an in-flight commit into a client-visible abort.
pub fn resolveTransactionIntentsWithSyncLevelAndCancellation(
    self: *DB,
    txn_id: transactions_mod.TxnId,
    status: transactions_mod.TxnStatus,
    commit_version: u64,
    sync_level: types.SyncLevel,
    visibility_cancellation: types.CancellationToken,
) !void {
    if (comptime storage_engine.enabled) return storage_engine.call(@src().fn_name, .{ self, txn_id, status, commit_version, sync_level, visibility_cancellation });
    // Resolution applies only a previously prepared durable decision.
    // Policy publication fences on outstanding schema/transaction leases,
    // so a preparing policy must not deadlock the very resolution needed
    // to drain those leases. User-visible writes are checked at prepare.
    try self.resolveTransactionIntentsInternal(
        txn_id,
        status,
        commit_version,
        sync_level,
        visibility_cancellation,
        null,
        null,
    );
}

pub fn resolveReplicatedTransactionAtOrderedReceipt(
    self: *DB,
    txn_id: transactions_mod.TxnId,
    status: transactions_mod.TxnStatus,
    commit_version: u64,
    sync_level: types.SyncLevel,
    visibility_cancellation: types.CancellationToken,
    identity: OrderedApplyReceipt,
    resolved_participant: ?[]const u8,
) !void {
    if (comptime storage_engine.enabled) return storage_engine.call(@src().fn_name, .{ self, txn_id, status, commit_version, sync_level, visibility_cancellation, identity, resolved_participant });
    try self.resolveTransactionIntentsInternal(
        txn_id,
        status,
        commit_version,
        sync_level,
        visibility_cancellation,
        identity,
        resolved_participant,
    );
}

pub fn abortTransaction(self: *DB, txn_id: transactions_mod.TxnId, timestamp_ns: u64) !void {
    try self.resolveTransactionIntents(txn_id, .aborted, timestamp_ns);
}

pub fn getTransactionStatus(self: *DB, txn_id: transactions_mod.TxnId) !transactions_mod.TxnStatus {
    if (comptime storage_engine.enabled) return storage_engine.call(@src().fn_name, .{ self, txn_id });
    return try self.core.getTransactionStatus(txn_id);
}

pub fn transactionDefersCoordinatorAcknowledgement(self: *DB, txn_id: transactions_mod.TxnId) !bool {
    if (comptime storage_engine.enabled) return storage_engine.call(@src().fn_name, .{ self, txn_id });
    return try self.core.transactionDefersCoordinatorAcknowledgement(txn_id);
}

pub fn transactionRetainsCoordinatorAcknowledgement(self: *DB, txn_id: transactions_mod.TxnId) !bool {
    if (comptime storage_engine.enabled) return storage_engine.call(@src().fn_name, .{ self, txn_id });
    return try self.core.transactionRetainsCoordinatorAcknowledgement(txn_id);
}

/// Returns whether split or merge handoff would strand a pending decision,
/// unresolved participant, intent, or hot-standby recovery outbox.
pub fn hasTopologySensitiveTransactions(self: *DB) !bool {
    if (comptime storage_engine.enabled) return storage_engine.call(@src().fn_name, .{self});
    return try self.core.hasTopologySensitiveTransactions();
}

pub fn markTransactionParticipantResolved(self: *DB, txn_id: transactions_mod.TxnId, participant: []const u8) !void {
    if (comptime storage_engine.enabled) return storage_engine.call(@src().fn_name, .{ self, txn_id, participant });
    var write_fence = try self.beginReplicatedWrite(.gate_then_apply);
    defer write_fence.release();
    try self.core.markTransactionParticipantResolved(txn_id, participant);
}

pub fn markTransactionParticipantsResolved(self: *DB, txn_id: transactions_mod.TxnId, participants: []const []const u8) !void {
    if (comptime storage_engine.enabled) return storage_engine.call(@src().fn_name, .{ self, txn_id, participants });
    var write_fence = try self.beginReplicatedWrite(.gate_then_apply);
    defer write_fence.release();
    try self.core.markTransactionParticipantsResolvedExtraBatch(txn_id, participants, .{});
}

pub fn markReplicatedTransactionParticipantResolvedAtOrderedReceipt(
    self: *DB,
    txn_id: transactions_mod.TxnId,
    participant: []const u8,
    identity: OrderedApplyReceipt,
) !void {
    if (comptime storage_engine.enabled) return storage_engine.call(@src().fn_name, .{ self, txn_id, participant, identity });
    lockApply(self);
    defer self.core.unlockApply();
    switch (try orderedApplyDisposition(try readOrderedApplyReceipt(self.alloc, self.core.store), identity)) {
        .already_applied => return,
        .apply => {},
    }
    var value_buf: [ordered_apply_receipt_value_len]u8 = undefined;
    const marker = orderedApplyReceiptWrite(identity, &value_buf);
    self.core.markTransactionParticipantResolvedExtraBatch(
        txn_id,
        participant,
        .{ .writes = &.{marker} },
    ) catch |err| switch (err) {
        transactions_mod.TxnError.TxnNotFound => try self.core.store.putBatch(&.{marker}, &.{}),
        else => return err,
    };
}

pub fn markReplicatedTransactionParticipantsResolvedAtOrderedReceipt(self: *DB, txn_id: transactions_mod.TxnId, participants: []const []const u8, identity: OrderedApplyReceipt) !void {
    if (comptime storage_engine.enabled) return storage_engine.call(@src().fn_name, .{ self, txn_id, participants, identity });
    lockApply(self);
    defer self.core.unlockApply();
    switch (try orderedApplyDisposition(try readOrderedApplyReceipt(self.alloc, self.core.store), identity)) {
        .already_applied => return,
        .apply => {},
    }
    var value_buf: [ordered_apply_receipt_value_len]u8 = undefined;
    const marker = orderedApplyReceiptWrite(identity, &value_buf);
    self.core.markTransactionParticipantsResolvedExtraBatch(
        txn_id,
        participants,
        .{ .writes = &.{marker} },
    ) catch |err| switch (err) {
        transactions_mod.TxnError.TxnNotFound => try self.core.store.putBatch(&.{marker}, &.{}),
        else => return err,
    };
}

pub fn cleanupTransactionMetadataIfEligible(
    self: *DB,
    txn_id: transactions_mod.TxnId,
    cutoff_timestamp: u64,
    retained_cutoff_timestamp: u64,
) !bool {
    if (comptime storage_engine.enabled) return storage_engine.call(@src().fn_name, .{ self, txn_id, cutoff_timestamp, retained_cutoff_timestamp });
    try self.lockApplyForPortableRuntime();
    defer self.core.unlockApply();
    return try self.core.cleanupTransactionMetadataIfEligible(txn_id, cutoff_timestamp, retained_cutoff_timestamp);
}

pub fn cleanupReplicatedTransactionAtOrderedReceipt(
    self: *DB,
    txn_id: transactions_mod.TxnId,
    cutoff_timestamp: u64,
    retained_cutoff_timestamp: u64,
    identity: OrderedApplyReceipt,
) !bool {
    if (comptime storage_engine.enabled) return storage_engine.call(@src().fn_name, .{ self, txn_id, cutoff_timestamp, retained_cutoff_timestamp, identity });
    lockApply(self);
    defer self.core.unlockApply();
    switch (try orderedApplyDisposition(try readOrderedApplyReceipt(self.alloc, self.core.store), identity)) {
        .already_applied => return false,
        .apply => {},
    }
    var value_buf: [ordered_apply_receipt_value_len]u8 = undefined;
    const marker = orderedApplyReceiptWrite(identity, &value_buf);
    return try self.core.cleanupTransactionMetadataIfEligibleExtraBatch(
        txn_id,
        cutoff_timestamp,
        retained_cutoff_timestamp,
        .{ .writes = &.{marker} },
    );
}

pub fn getTransactionParticipants(self: *DB, alloc: Allocator, txn_id: transactions_mod.TxnId) ![][]u8 {
    return try self.core.getTransactionParticipants(alloc, txn_id);
}

pub fn getUnresolvedTransactionParticipants(self: *DB, alloc: Allocator, txn_id: transactions_mod.TxnId) ![][]u8 {
    if (comptime storage_engine.enabled) return storage_engine.call(@src().fn_name, .{ self, alloc, txn_id });
    return try self.core.getUnresolvedTransactionParticipants(alloc, txn_id);
}

pub fn recoverTransactions(self: *DB, cutoff_timestamp: u64, resolution_timestamp: u64) !transactions_mod.RecoveryStats {
    var write_fence = try self.beginReplicatedWrite(.gate);
    defer write_fence.release();
    const resolved_finalized = try resolveFinalizedTransactionIntentsForRecovery(self, resolution_timestamp);
    try self.lockApplyForPortableRuntime();
    defer self.core.unlockApply();
    var recovery_stats = try self.core.recoverTransactions(cutoff_timestamp, resolution_timestamp);
    recovery_stats.resolved_finalized += resolved_finalized;
    return recovery_stats;
}

pub fn resolveFinalizedTransactionIntentsForRecovery(self: *DB, resolution_timestamp: u64) !u64 {
    const txns = try self.core.listTransactions(self.alloc);
    defer self.alloc.free(txns);
    var resolved_finalized: u64 = 0;
    for (txns) |txn| {
        if (txn.status == .pending or
            (!(try self.core.transactionHasIntents(txn.txn_id)) and !(try self.core.transactionHasReplicationOutbox(txn.txn_id)))) continue;
        const resolve_version = if (txn.status == .committed and txn.commit_version != 0)
            txn.commit_version
        else
            resolution_timestamp;
        try self.resolveTransactionIntentsWithSyncLevel(txn.txn_id, txn.status, resolve_version, .propose);
        resolved_finalized += 1;
    }
    return resolved_finalized;
}

pub fn waitForResolvedTransactionSyncWithCancellation(self: *DB, sync_level: types.SyncLevel, sequence: u64, cancellation: types.CancellationToken) !void {
    if (comptime storage_engine.enabled) return storage_engine.call(@src().fn_name, .{ self, sync_level, sequence, cancellation });
    return self.waitForResolvedTransactionSyncImpl(sync_level, sequence, cancellation, null);
}

pub fn currentInitialBuildIntentMatches(
    self: *const DB,
    state: *const index_repair_state.State,
    cfg: types.IndexConfig,
) bool {
    const index = state.findIndex(cfg.name) orelse return false;
    const intent = state.entries.items[index].intent;
    return intent.work_class == .initial_build and
        intent.phase != .terminal and
        intent.automation != .paused and
        intent.root_generation == self.core.root_generation and
        intent.group_id == self.localRepairGroupId() and
        intent.config_hash == types.indexConfigHash(cfg);
}

pub fn currentNonInitialRepairIntentMatches(
    self: *const DB,
    state: *const index_repair_state.State,
    cfg: types.IndexConfig,
) bool {
    const index = state.findIndex(cfg.name) orelse return false;
    const intent = state.entries.items[index].intent;
    return intent.work_class != .initial_build and
        intent.phase != .terminal and
        intent.root_generation == self.core.root_generation and
        intent.group_id == self.localRepairGroupId() and
        intent.config_hash == types.indexConfigHash(cfg);
}

/// Converts an idle-watermark/source-coverage mismatch into the existing
/// durable, bounded generation-materialization state machine. Source discovery is
/// paged at 128 documents per owner quantum and crash-resumes from its
/// fenced cursor; the currently published partial generation remains
/// serviceable until the replacement is atomically activated.
pub fn ensureGeneratedCoverageRecoveryIntents(self: *DB, alloc: Allocator) !usize {
    if (comptime storage_engine.enabled) return storage_engine.call(@src().fn_name, .{ self, alloc });
    const configs = try self.generatedCoverageRecoveryConfigsAlloc(alloc);
    defer {
        for (configs) |*cfg| cfg.deinit(alloc);
        alloc.free(configs);
    }
    var ensured: usize = 0;
    for (configs) |cfg| {
        _ = try self.createGenerationRepairIntentAtTarget(
            alloc,
            cfg,
            .replay_artifact_unavailable,
            0,
            0,
            "generated_source_coverage_incomplete_after_replay",
            null,
            .pending,
            // A missing terminal source outcome after replay is unfinished
            // materialization, not evidence that the published generation
            // is corrupt. It shares the bounded generation scheduler with
            // repair, while retaining its non-repair lifecycle class.
            .initial_build,
        );
        ensured += 1;
    }
    return ensured;
}

test "db transaction recovery identity context owns schema generations" {
    const alloc = std.testing.allocator;
    var backend = mem_backend_mod.Backend.init(alloc, .{});
    defer backend.close();
    var runtime_store = try backend.runtimeStore(alloc, .{ .name = "recovery-schema-context" });
    defer runtime_store.deinit();
    var store = try docstore_mod.DocStore.openRuntime(alloc, &runtime_store);
    defer store.close();

    const first = [_]schema_mod.RelationalColumn{.{
        .name = "status",
        .path = "status",
        .column_type = .string,
    }};
    var ctx = try db_core.TransactionRecoveryIdentityContext.init(alloc, &store, .{}, &first, 1);
    defer ctx.deinit();
    try std.testing.expect(ctx.relational_columns.ptr != first[0..].ptr);
    try std.testing.expectEqualStrings("status", ctx.relational_columns[0].name);

    const second = [_]schema_mod.RelationalColumn{.{
        .name = "amount",
        .path = "nested.amount",
        .column_type = .number,
        .required = true,
    }};
    var prepared = try db_core.PreparedRecoveryRelationalState.init(alloc, &second, 2);
    defer prepared.deinit();
    ctx.installPreparedRelationalState(&prepared);
    try std.testing.expect(ctx.relational_columns.ptr != second[0..].ptr);
    try std.testing.expectEqualStrings("amount", ctx.relational_columns[0].name);
    try std.testing.expectEqualStrings("nested.amount", ctx.relational_columns[0].path);
    try std.testing.expect(ctx.relational_columns[0].required);
    try std.testing.expectEqual(@as(u32, 2), ctx.relational_schema_version);
}

test "db transaction intent writes reject new documents at ordinal exhaustion" {
    const alloc = std.testing.allocator;

    var path_tmp = try TestDirectory.init("db");
    defer path_tmp.cleanup();
    const path = path_tmp.path().ptr;

    var db = try DB.open(alloc, std.mem.span(path), .{
        .primary_backend = .{ .mem = .{} },
    });
    defer db.close();

    try db.batch(.{
        .writes = &.{.{ .key = "doc:existing_txn", .value = "{\"name\":\"existing\"}" }},
        .sync_level = .write,
    });

    var value: [4]u8 = undefined;
    std.mem.writeInt(u32, &value, std.math.maxInt(doc_identity.DocOrdinal), .big);
    try db.core.store.put(internal_keys.identity_next_ordinal_key[0..], &value);

    const existing_txn = try db.beginTransaction(11_000);
    try db.writeIntents(existing_txn, &.{
        .{ .key = "doc:existing_txn", .value = "{\"name\":\"updated\"}" },
    }, &.{});
    try db.commitTransaction(existing_txn, 11_001);

    const existing = (try db.get(alloc, "doc:existing_txn")) orelse return error.TestExpectedEqual;
    defer alloc.free(existing);
    try std.testing.expectEqualStrings("{\"name\":\"updated\"}", existing);

    const direct_txn = try db.beginTransaction(12_000);
    try std.testing.expectError(error.DocOrdinalExhausted, db.writeIntents(direct_txn, &.{
        .{ .key = "doc:new_direct_txn", .value = "{\"name\":\"new\"}" },
    }, &.{}));
    try db.abortTransaction(direct_txn, 12_001);
    try std.testing.expect((try db.get(alloc, "doc:new_direct_txn")) == null);

    const request_txn = try db.beginTransaction(13_000);
    try std.testing.expectError(error.DocOrdinalExhausted, db.writeTransaction(request_txn, .{
        .writes = &.{.{ .key = "doc:new_request_txn", .value = "{\"name\":\"new\"}" }},
    }));
    try db.abortTransaction(request_txn, 13_001);
    try std.testing.expect((try db.get(alloc, "doc:new_request_txn")) == null);
}

test "db transaction-created identity rows remain visible after reopen" {
    const alloc = std.testing.allocator;

    var path_tmp = try TestDirectory.init("db");
    defer path_tmp.cleanup();
    const path = path_tmp.path().ptr;

    {
        var db = try DB.open(alloc, std.mem.span(path), .{});
        defer db.close();

        const txn_id = try db.beginTransaction(21_000);
        try db.writeTransaction(txn_id, .{
            .writes = &.{.{ .key = "doc:txn_reopen", .value = "{\"title\":\"txn\"}" }},
        });
        try db.commitTransaction(txn_id, 21_001);
    }

    {
        var db = try DB.open(alloc, std.mem.span(path), .{});
        defer db.close();

        const raw = (try db.get(alloc, "doc:txn_reopen")) orelse return error.TestExpectedEqual;
        defer alloc.free(raw);
        try std.testing.expectEqualStrings("{\"title\":\"txn\"}", raw);

        const current = try db.searchRequestAtCurrentIdentityGeneration(.{});
        var resolved = try db.resolveDocSetForIdsNoLockAtGenerationAlloc(alloc, &.{"doc:txn_reopen"}, current.identity_read_generation);
        defer resolved.deinit(alloc);
        try std.testing.expect(resolved.containsOrdinal(1));

        const stats = try db.diagnosticStats(alloc);
        defer types.freeDBStats(alloc, stats);
        try std.testing.expectEqual(@as(u64, 1), stats.doc_identity.state_rows);
        try std.testing.expectEqual(@as(u64, 1), stats.doc_identity.live_ordinals);
        try std.testing.expectEqual(@as(u64, 0), stats.doc_identity.primary_docs_missing_ordinals);
        try std.testing.expectEqual(@as(u64, 0), stats.doc_identity.primary_docs_missing_identity_state);
    }
}

test "db transaction abort leaves no visible document" {
    const alloc = std.testing.allocator;

    var path_tmp = try TestDirectory.init("db");
    defer path_tmp.cleanup();
    const path = path_tmp.path().ptr;

    var db = try DB.open(alloc, std.mem.span(path), .{});
    defer db.close();

    const txn_id = try db.beginTransaction(1_700_000_000_000_000_000);
    try db.writeIntents(txn_id, &.{
        .{ .key = "doc:txn_abort", .value = "{\"title\":\"alpha\"}" },
    }, &.{});
    try db.abortTransaction(txn_id, 1_700_000_000_000_000_001);

    try std.testing.expectEqual(transactions_mod.TxnStatus.aborted, try db.getTransactionStatus(txn_id));
    try std.testing.expect((try db.get(alloc, "doc:txn_abort")) == null);
}

test "db transaction resolves transforms against pending same-transaction writes" {
    const alloc = std.testing.allocator;

    var path_tmp = try TestDirectory.init("db");
    defer path_tmp.cleanup();
    const path = path_tmp.path().ptr;

    var db = try DB.open(alloc, std.mem.span(path), .{});
    defer db.close();

    const txn_id = try db.beginTransaction(9_000);
    try db.writeTransaction(txn_id, .{
        .writes = &.{
            .{ .key = "doc:txn_transform", .value = "{\"count\":1}" },
        },
        .transforms = &.{
            .{
                .key = "doc:txn_transform",
                .operations = &.{
                    .{ .op = .inc, .path = "count", .value_json = "4" },
                },
            },
        },
    });
    try db.commitTransaction(txn_id, 9_001);

    const raw = (try db.get(alloc, "doc:txn_transform")) orelse return error.TestExpectedEqual;
    defer alloc.free(raw);

    var parsed = try std.json.parseFromSlice(std.json.Value, alloc, raw, .{});
    defer parsed.deinit();
    const count_value = parsed.value.object.get("count").?;
    switch (count_value) {
        .integer => |value| try std.testing.expectEqual(@as(i64, 5), value),
        .float => |value| try std.testing.expectEqual(@as(f64, 5), value),
        else => return error.TestExpectedEqual,
    }
}

test "db transaction intents fence ordinary batch transforms" {
    const alloc = std.testing.allocator;
    var path_tmp = try TestDirectory.init("db");
    defer path_tmp.cleanup();
    const path = path_tmp.path().ptr;

    var db = try DB.open(alloc, std.mem.span(path), .{});
    defer db.close();
    try db.batch(.{ .writes = &.{.{ .key = "doc:counter", .value = "{\"count\":0}" }} });

    const txn_id = try db.beginTransaction(10_000);
    try db.writeTransaction(txn_id, .{ .transforms = &.{.{
        .key = "doc:counter",
        .operations = &.{.{ .op = .inc, .path = "count", .value_json = "1" }},
    }} });

    try std.testing.expectError(error.IntentConflict, db.batch(.{ .transforms = &.{.{
        .key = "doc:counter",
        .operations = &.{.{ .op = .inc, .path = "count", .value_json = "1" }},
    }} }));

    try db.commitTransaction(txn_id, 10_001);
    try db.batch(.{ .transforms = &.{.{
        .key = "doc:counter",
        .operations = &.{.{ .op = .inc, .path = "count", .value_json = "1" }},
    }} });
    const raw = (try db.get(alloc, "doc:counter")) orelse return error.TestExpectedEqual;
    defer alloc.free(raw);
    try std.testing.expect(std.mem.indexOf(u8, raw, "\"count\":2") != null);
}

test "db transaction write request enforces version predicates" {
    const alloc = std.testing.allocator;

    var path_tmp = try TestDirectory.init("db");
    defer path_tmp.cleanup();
    const path = path_tmp.path().ptr;

    var db = try DB.open(alloc, std.mem.span(path), .{});
    defer db.close();

    try db.batch(.{
        .writes = &.{.{ .key = "doc:pred", .value = "{\"title\":\"v1\"}" }},
        .timestamp_ns = 5_000,
    });

    const txn_id = try db.beginTransaction(6_000);
    try std.testing.expectError(transactions_mod.TxnError.VersionConflict, db.writeTransaction(txn_id, .{
        .writes = &.{.{ .key = "doc:pred", .value = "{\"title\":\"v2\"}" }},
        .predicates = &.{.{ .key = "doc:pred", .expected_version = 0 }},
    }));

    try db.writeTransaction(txn_id, .{
        .writes = &.{.{ .key = "doc:pred", .value = "{\"title\":\"v2\"}" }},
        .predicates = &.{.{ .key = "doc:pred", .expected_version = 5_000 }},
    });
    try db.commitTransaction(txn_id, 6_001);

    const raw = (try db.get(alloc, "doc:pred")) orelse return error.TestExpectedEqual;
    defer alloc.free(raw);
    try std.testing.expectEqualStrings("{\"title\":\"v2\"}", raw);
}

test "db transaction write request detects concurrent intent conflicts" {
    const alloc = std.testing.allocator;

    var path_tmp = try TestDirectory.init("db");
    defer path_tmp.cleanup();
    const path = path_tmp.path().ptr;

    var db = try DB.open(alloc, std.mem.span(path), .{});
    defer db.close();

    const txn1 = try db.beginTransaction(1_000);
    const txn2 = try db.beginTransaction(1_001);

    try db.writeTransaction(txn1, .{
        .writes = &.{.{ .key = "doc:shared", .value = "{\"title\":\"from1\"}" }},
    });

    try std.testing.expectError(transactions_mod.TxnError.IntentConflict, db.writeTransaction(txn2, .{
        .writes = &.{.{ .key = "doc:shared", .value = "{\"title\":\"from2\"}" }},
    }));
}

test "db transaction retained credits bind compact no-op and prepared generated AROW after reopen" {
    const alloc = std.testing.allocator;
    const retention = retained_effects_mod;
    for ([_]bool{ false, true }) |typed| {
        var directory = try TestDirectory.init("reserved-db-rows");
        defer directory.cleanup();
        const options: OpenOptions = .{ .start_optional_runtimes = false, .primary_backend = .{ .lsm = .{} } };
        var db = try DB.open(alloc, directory.path(), options);
        defer db.close();
        if (typed) try db.setSchemaJson(alloc,
            \\{"version":1,"storage_mode":"relational","default_type":"row","column_defaults":[{"column":"b","expression":{"op":"literal","type":"integer","value":"2"}}],"generated_columns":[{"column":"total","expression":{"op":"add","args":[{"op":"column","column":"a"},{"op":"column","column":"b"}]}}],"document_schemas":{"row":{"schema":{"type":"object","properties":{"a":{"type":"integer"},"b":{"type":"integer"},"total":{"type":"integer"}},"required":["a","b","total"],"additionalProperties":false}}}}
        );
        const spaces = try alloc.alloc(u8, 64 * 1024);
        defer alloc.free(spaces);
        @memset(spaces, ' ');
        const prior = try std.fmt.allocPrint(alloc, "{{{s}\"a\":5}}", .{spaces});
        defer alloc.free(prior);
        try db.batch(.{ .writes = &.{.{ .key = "row", .value = if (typed) "{\"a\":5}" else prior }}, .timestamp_ns = 10 });
        const id = try db.beginTransaction(20);
        try db.writeTransaction(id, .{ .writes = &.{.{ .key = "row", .value = "{\"a\":5}" }} });
        var reserved: u64 = 0;
        var ns: retention.Namespace = undefined;
        {
            var txn = try db.core.store.beginWriteTxn();
            errdefer txn.abort();
            ns = (try txn.get(&internal_keys.identity_namespace_key))[0..24].*;
            reserved = (try retention.loadReservations(&txn)).?.bytes;
            try std.testing.expect(reserved < prior.len);
            // A newly admitted source includes already prepared obligations.
            _ = try retention.admit(&txn, ns, 1, @splat(9), retention.default_limit);
            try txn.commit();
        }
        db.close();
        db = try DB.open(alloc, directory.path(), options);
        try db.resolveTransactionIntents(id, .committed, 30);
        {
            var txn = try db.core.store.beginReadTxn();
            defer txn.abort();
            const state = (try retention.load(&txn)).?;
            try std.testing.expectEqual(reserved, state.retained_bytes);
            try std.testing.expectEqual(@as(u64, 0), (try retention.loadReservations(&txn)).?.bytes);
            var frame = (try retention.read(&txn, ns, 1, @splat(9), 0)).?;
            const effect = (try frame.next()).?;
            try std.testing.expectEqual(@as(u64, 30), effect.timestamp);
            try std.testing.expect(try frame.next() == null);
            if (!typed) try std.testing.expectEqualStrings("{\"a\":5}", effect.value.?);
        }
        const logical = (try db.get(alloc, "row")).?;
        defer alloc.free(logical);
        if (typed) {
            var parsed = try std.json.parseFromSlice(std.json.Value, alloc, logical, .{});
            defer parsed.deinit();
            try std.testing.expectEqual(@as(i64, 2), parsed.value.object.get("b").?.integer);
            try std.testing.expectEqual(@as(i64, 7), parsed.value.object.get("total").?.integer);
        }
    }
}

test "db transaction repeated committed resolve cannot overwrite a newer write" {
    const alloc = std.testing.allocator;
    var path_tmp = try TestDirectory.init("db");
    defer path_tmp.cleanup();
    const path = path_tmp.path().ptr;

    var db = try DB.open(alloc, std.mem.span(path), .{ .start_index_workers = false });
    defer db.close();

    const txn_id = try db.beginTransaction(12_000);
    try db.writeTransaction(txn_id, .{
        .writes = &.{.{ .key = "doc:fenced", .value = "{\"title\":\"transaction\"}" }},
    });
    try db.resolveTransactionIntents(txn_id, .committed, 15_000);
    try db.batch(.{
        .writes = &.{.{ .key = "doc:fenced", .value = "{\"title\":\"newer\"}" }},
        .timestamp_ns = 16_000,
    });

    try db.resolveTransactionIntents(txn_id, .committed, 15_000);
    const raw = (try db.get(alloc, "doc:fenced")) orelse return error.TestExpectedEqual;
    defer alloc.free(raw);
    try std.testing.expectEqualStrings("{\"title\":\"newer\"}", raw);
    try std.testing.expectEqual(@as(u64, 16_000), try db.getTimestamp(alloc, "doc:fenced"));
    try std.testing.expectEqual(@as(u64, 2), db.core.store.lastReplaySequence(0));
}

test "db transaction committed transform appends derived replay from final value" {
    const alloc = std.testing.allocator;
    var path_tmp = try TestDirectory.init("db");
    defer path_tmp.cleanup();
    const path = path_tmp.path().ptr;

    var db = try DB.open(alloc, std.mem.span(path), .{ .start_index_workers = false });
    defer db.close();
    try db.batch(.{ .writes = &.{.{ .key = "doc:transform", .value = "{\"count\":1}" }} });

    const txn_id = try db.beginTransaction(12_000);
    try db.writeTransaction(txn_id, .{
        .transforms = &.{.{
            .key = "doc:transform",
            .operations = &.{.{ .op = .inc, .path = "count", .value_json = "2" }},
        }},
    });
    try db.resolveTransactionIntents(txn_id, .committed, 15_000);

    const raw = (try db.get(alloc, "doc:transform")) orelse return error.TestExpectedEqual;
    defer alloc.free(raw);
    try std.testing.expectEqualStrings("{\"count\":3}", raw);

    const entries = try db.core.store.iterateReplayFrom(alloc, 2);
    defer {
        for (entries) |*entry| entry.deinit(alloc);
        alloc.free(entries);
    }
    try std.testing.expectEqual(@as(usize, 1), entries.len);
    var record = try change_journal_mod.decodeRecord(alloc, entries[0].payload);
    defer record.deinit();
    try std.testing.expectEqual(@as(usize, 1), record.record.changed_doc_keys.len);
    try std.testing.expectEqualStrings("doc:transform", record.record.changed_doc_keys[0]);
}

test "db transaction committed transform reaches full text visibility" {
    const alloc = std.testing.allocator;
    var path_tmp = try TestDirectory.init("db");
    defer path_tmp.cleanup();
    const path = path_tmp.path().ptr;

    var db = try DB.open(alloc, std.mem.span(path), .{});
    defer db.close();
    try db.addIndex(.{ .name = "ft_txn", .kind = .full_text, .config_json = "{}" });
    try db.batch(.{
        .writes = &.{.{ .key = "doc:transform-index", .value = "{\"title\":\"alpha\"}" }},
        .sync_level = .full_index,
    });

    const txn_id = try db.beginTransaction(12_000);
    try db.writeTransaction(txn_id, .{
        .transforms = &.{.{
            .key = "doc:transform-index",
            .operations = &.{.{ .op = .set, .path = "title", .value_json = "\"omega\"" }},
        }},
    });
    try db.resolveTransactionIntentsWithSyncLevel(txn_id, .committed, 15_000, .full_index);

    var result = try db.search(alloc, .{
        .index_name = "ft_txn",
        .query = .{ .match = .{ .field = "_all", .text = "omega" } },
        .limit = 10,
    });
    defer result.deinit();
    try std.testing.expectEqual(@as(u32, 1), result.total_hits);
    try std.testing.expectEqualStrings("doc:transform-index", result.hits[0].id);
}

test "db transaction idempotent resolve honors stronger sync level" {
    const alloc = std.testing.allocator;
    var path_tmp = try TestDirectory.init("db");
    defer path_tmp.cleanup();
    const path = path_tmp.path().ptr;

    var db = try DB.open(alloc, std.mem.span(path), .{});
    defer db.close();
    try db.addIndex(.{ .name = "ft_txn_retry", .kind = .full_text, .config_json = "{}" });

    const txn_id = try db.beginTransaction(12_000);
    try db.writeTransaction(txn_id, .{
        .writes = &.{.{ .key = "doc:sync-retry", .value = "{\"title\":\"durable replay\"}" }},
    });
    try db.resolveTransactionIntentsWithSyncLevel(txn_id, .committed, 15_000, .propose);
    try db.resolveTransactionIntentsWithSyncLevel(txn_id, .committed, 15_000, .full_index);

    var result = try db.search(alloc, .{
        .index_name = "ft_txn_retry",
        .query = .{ .match = .{ .field = "_all", .text = "durable" } },
        .limit = 10,
    });
    defer result.deinit();
    try std.testing.expectEqual(@as(u32, 1), result.total_hits);
    try std.testing.expectEqualStrings("doc:sync-retry", result.hits[0].id);
}

test "db transaction integrity contention precedes stale claim semantics" {
    const alloc = std.testing.allocator;
    for ([_]transactions_mod.TxnStatus{ .committed, .aborted }) |decision| {
        var directory = try TestDirectory.init("claim-contention");
        defer directory.cleanup();
        var db = try DB.open(alloc, directory.path(), .{
            .start_optional_runtimes = false,
            .start_index_workers = false,
            .identity_namespace = .{ .table_id = 100, .shard_id = 101 },
        });
        defer db.close();
        try db.setSchemaJson(alloc,
            \\{"version":1,"storage_mode":"relational","default_type":"row","unique_constraints":[{"name":"pk","columns":["id"]}],"document_schemas":{"row":{"schema":{"type":"object","properties":{"id":{"type":"integer"}},"additionalProperties":false}}}}
        );
        const integrity = relational_integrity;
        const catalog_mod = relational_integrity_catalog;
        const raw = (try db.core.getStoreValue(alloc, catalog_mod.key)).?;
        defer alloc.free(raw);
        var catalog = try catalog_mod.decode(alloc, raw);
        defer catalog.deinit();
        const generation_set = relational_integrity_activation.generationSet(catalog);
        var view = db.core.acquireSchemaView().?;
        defer view.release();
        var plan = try relational_index_keys.TuplePlan.init(alloc, view.tableSchema().*, view.physicalLayout(), &.{.{ .column = "id" }});
        defer plan.deinit();
        var tuple: std.ArrayList(u8) = .empty;
        defer tuple.deinit(alloc);
        _ = try plan.appendValues(alloc, &tuple, &.{.{ .integer = 17 }});
        const address = try integrity.Address.init(catalog.find(.unique, "pk").?.generation, tuple.items);
        const seed = try db.beginTransactionWithId(@splat(71), 10);
        try db.writeTransaction(seed, .{
            .relational_schema_version = 1,
            .relational_integrity_generation_set = generation_set,
            .writes = &.{.{ .key = "parent", .value = "{\"id\":17}" }},
            .integrity_commands = &.{.{ .address = address, .operation = .{ .establish = .{ .tuple = tuple.items, .parent_table = "parents", .parent_key = "parent", .schema_version = 1 } } }},
        });
        try db.commitTransaction(seed, 11);
        const release = try db.beginTransactionWithId(@splat(72), 12);
        const release_request: types.TransactionIntentRequest = .{
            .relational_schema_version = 1,
            .relational_integrity_generation_set = generation_set,
            .deletes = &.{"parent"},
            .integrity_commands = &.{.{ .address = address, .operation = .{ .release = .{ .parent_table = "parents", .parent_key = "parent" } } }},
        };
        try db.writeTransaction(release, release_request);
        // Own intents remain idempotently preparable. Another transaction must
        // see recoverable contention before decoding the old physical claim.
        try db.writeTransaction(release, release_request);
        const candidate = try db.beginTransactionWithId(@splat(73), 13);
        const replacement: types.TransactionIntentRequest = .{
            .relational_schema_version = 1,
            .relational_integrity_generation_set = generation_set,
            .writes = &.{.{ .key = "replacement", .value = "{\"id\":17}" }},
            .integrity_commands = &.{.{ .address = address, .operation = .{ .establish = .{ .tuple = tuple.items, .parent_table = "parents", .parent_key = "replacement", .schema_version = 1 } } }},
        };
        try std.testing.expectError(error.IntentConflict, db.writeTransaction(candidate, replacement));
        try db.resolveTransactionIntents(release, decision, 20);
        if (decision == .committed) {
            try db.writeTransaction(candidate, replacement);
            try db.commitTransaction(candidate, 21);
        } else {
            try std.testing.expectError(error.UniqueConstraintViolation, db.writeTransaction(candidate, replacement));
        }
    }
}
